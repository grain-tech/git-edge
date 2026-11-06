# Bundle-URI support

> Second pass · Idea #12 · verdict: **risky** · feasibility 4/5 · reliability 3/5 · correctness 4/5 (first pass 4/3/3)
> First pass: [proof](./archived/bundle-uri.md) · [review](../reviews/archived/bundle-uri.md) · Second pass: [review](../reviews/bundle-uri.md) · Contract: [CONTRACTS.md](../../CONTRACTS.md)


## Mechanism
CONTRACTS.md section 12 lists `bundle-uri` as not advertised by the foundation and section 2.2 forbids any R2 key beyond `packs/` and `pending/`, so this idea is neither subsumed nor foundation: it is the first post-foundation module, and it never writes the foundation bucket. A new job kind `BundleCut` (registered per section 4.5 in `jobs`) takes the clone plan that precomputed-clone-pack already keeps in `RepoDo` (`clone_plan` bitmaps plus the `clone.refs` snapshot captured with them) and materializes it as one R2 object in a second bucket binding `BUNDLES`: the bundle v2 text header built from that snapshot, then `PACK` + count + every marked entry copied verbatim in offset order with 8 MiB range reads and 8 MiB multipart parts, then a SHA-1 trailer; a `bundles` table (`cutting | live | dead`) records it and the same job deletes dead objects GRACE later. `wire` gains `V2Command::BundleUri` and one advertisement line, `RepoDo` gains the sync route `/_do/bundle-uri` that lists the newest live bundle as `bundle.<id>.uri=` / `creationToken=` pkt-lines, and `edge` gains `GET /_bundles/<repo_id>/<id>.bundle` for private deployments (public deployments point the URI at the bucket's custom domain, which Cloudflare caches). After the client unbundles into `refs/bundles/*`, its ordinary `fetch` carries those tips as `have` lines and the section 9 negotiation answers with only the objects pushed since the plan.

## Primitives
- DO SQLite through `worker::SqlStorage::exec` (sync; `to_array`), no CAS needed: `exec` helper shape **unverified**, as in the sibling proofs. BLOB column read for `clone_plan.full`: `SqlStorageValue` blob variant **unverified**.
- Jobs dispatcher (section 4): `JobKind::BundleCut`, `run_slice` arm, `SliceOutcome::{Done, Reschedule}`, `enqueue` dedup; no `set_alarm` here (4.1; second `setAlarm` cancels the first, **measured** platform-facts #5).
- Second R2 binding `BUNDLES`: `worker::Bucket::create_multipart_upload(key).execute()`, `MultipartUpload::{upload_part(u16, Data::Bytes), complete(Vec<UploadedPart>), abort, upload_id}`, `resume_multipart_upload(key, id)`, `Bucket::delete(key)`, `Bucket::get(key).execute()`, `ObjectBody::stream()`: names from memo section 1 / docs.rs, multipart **verified on the local simulator only** (platform-facts #6: the 5 MiB minimum and equal-size rules are not enforced locally); `upload_id()` accessor and `http_metadata` on the multipart builder **unverified**.
- Foundation bucket via `store::Bucket::read_range` (1.2, `Range::OffsetWithLength` verified in streaming-pack-parser); real R2 range reads on multi-GB objects **local only**.
- `gix_hash::hasher(Kind::Sha1)`, `Hasher::update`, `Hasher::try_finalize -> ObjectId` (gix-hash 0.26.2): crate linked and ran in the spike; the `Hasher` method names are from docs, **not run in the spike**.
- `wire::PktWriter::{text, flush}` over `gix_packetline::blocking_io::encode` (correction 1): **measured** against git 2.43 for ls-refs and the advertisement.
- `worker::Response::from_stream` over `ByteStream` (private serve route): memo-listed, **unverified at runtime**. `auth::authenticate` (section 12): contract signature.
- `store::random_hex32()` (`web_sys::Crypto::get_random_values_with_u8_array`, 8.2): binding path **unverified**.
- R2 custom domain in front of the `BUNDLES` bucket, Cloudflare cache for objects up to 512 MB off Enterprise, object `cacheControl` honoured: Cloudflare R2 docs, **not measured**.
- git facts used: `command=bundle-uri` has no delim and no arguments; the reply is `bundle.version=1`, `bundle.mode=all`, optional `bundle.heuristic=creationToken`, `bundle.<id>.uri=`, `bundle.<id>.creationToken=` lines then flush; ids must be dot-free; `clone.c` skips the download when the list has no bundles and when `--depth` or `--filter` is given; unbundle writes `refs/bundles/<name minus refs/>` and refuses a ref whose target is not in the pack; `mark_tips` sends every local ref, including `refs/bundles/*`, as `have`. From git 2.43 source and gitformat-bundle, **not yet a scenario run**.

## Proof code
```rust
// src/jobs/bundle_cut.rs, src/repo_do/bundle.rs, src/edge/bundles.rs -- CONTRACTS.md 1.1, 1.3, 2.1, 2.3, 4, 5, 7, 8, 9, 10, 12
use gix_hash::{Hasher, Kind as HashKind};
use serde::Deserialize;
use worker::{Data, Env, MultipartUpload, Request, Response, SqlStorage, UploadedPart};
use crate::{auth, error::Error, jobs::{self, JobKind, SliceBudget, SliceOutcome}, store::{self, keys, PackId},
            wire::PktWriter, ReqBudget, RepoDo};
// Helpers exec(), meta_i64(), meta_str(), meta_opt(), store::random_hex32(): as in refs-sqlite-objects-r2 / precomputed-clone-pack.
// Schema at schema_version 3 (8.2), only this module touches it: CREATE TABLE bundles (id TEXT PRIMARY KEY, state TEXT NOT NULL
//   /* 'cutting'|'live'|'dead' */, refs_version INTEGER NOT NULL, bytes INTEGER NOT NULL DEFAULT 0, upload_id TEXT,
//   created_at INTEGER NOT NULL, dead_at INTEGER) WITHOUT ROWID;
// R2: second binding BUNDLES, key `<repo_id>/<bundle_id>.bundle`, never reused; the 2.2 bucket is not written. Env GE_BUNDLE_BASE =
//   "https://bundles.example.com" (public bucket + custom domain = CDN) or "https://<host>/_bundles" (private: serve_bundle). Unset: not advertised.
// Inputs from precomputed-clone-pack: clone_plan(pack_id, full, noblob), meta clone.refs_version, clone.refs = JSON [{name,target}]
//   captured in its start() next to clone.tips (write-back there). wire write-backs: a `bundle-uri` advertisement line (rule 6) and
//   `Some(b"bundle-uri") => Ok(V2Command::BundleUri)` (no delim, no arguments: already accepted); edge routes it to `/_do/bundle-uri`.
const WINDOW: u64 = 8 << 20;                 // 7.2 / 6.4: one range read and one part per 8 MiB
const BUNDLE_MAX: u64 = 512 << 20;           // one-slice cut bound (Known limits); also the CDN cacheable-object cap off Enterprise
const GRACE_MS: i64 = 60 * 60 * 1000;        // section 5 GRACE
const QUIET_MS: i64 = 10 * 60 * 1000;        // GC_QUIET
#[derive(Deserialize)] struct Row { offset: u64, len: u32, idx: u32 }
struct Cut { id: String, header: Vec<u8>, count: u32, gc_epoch: i64, packs: Vec<(PackId, Vec<u8>, u32, u64)> }   // bitmap, count, bytes
/// jobs::run_slice arm for JobKind::BundleCut. One firing: sweep old objects, then at most one cut, never resumed.
pub async fn run_bundle_cut(d: &RepoDo, budget: &mut SliceBudget) -> Result<SliceOutcome, Error> {
    let (sql, now) = (d.state.storage().sql(), js_sys::Date::now() as i64);
    let r2 = d.env.bucket("BUNDLES").map_err(|e| Error::Storage(e.to_string()))?;
    let repo = d.meta_str("repo_id")?;                                       // 8.2: never ctx.id.name, never the URL
    sweep(&sql, &r2, &repo, now, &mut budget.req).await?;
    let Some(cut) = plan_cut(d, &sql, now)? else { return Ok(SliceOutcome::Done) };
    let key = format!("{repo}/{}.bundle", cut.id); budget.req.charge(1)?;
    let mpu = r2.create_multipart_upload(&key).execute().await.map_err(|e| Error::Storage(e.to_string()))?;   // .http_metadata(content-type, immutable cache-control): builder shape unverified
    exec(&sql, "UPDATE bundles SET upload_id=? WHERE id=?", vec![mpu.upload_id().into(), cut.id.clone().into()])?;   // from here an eviction leaves a row sweep() can abort
    let mut w = BundleWriter { mpu, part: cut.header.clone(), parts: vec![], n: 1, hasher: gix_hash::hasher(HashKind::Sha1), bytes: 0 };
    w.pack_header(cut.count);                                                // text header is not hashed; PACK header and entries are
    let bucket = d.bucket()?;                                                // store::Bucket over the foundation bucket (1.2)
    for (pack, bits, count, pack_bytes) in &cut.packs {
        let (pkey, mut lo) = (keys::pack(&bucket.repo, pack), 0u32);
        while lo < *count {                                                  // <= 50,000 rows per sync span keeps memory bounded
            let hi = lo.saturating_add(50_000).min(*count);
            let rows = marked_rows(d, &sql, pack, bits, lo, hi, cut.gc_epoch)?;
            let mut i = 0usize;
            while let Some(first) = rows.get(i) {                            // window starts at the next uncopied entry: one read per 8 MiB of pack, whatever the marking density
                let len = WINDOW.max(u64::from(first.len)).min(pack_bytes.saturating_sub(first.offset));
                if u64::from(first.len) > len { return Err(Error::Storage("entry past pack end".into())); }
                let win = bucket.read_range(&pkey, first.offset, len, &mut budget.req).await?;   // async, one subrequest (7.1)
                while let Some(r) = rows.get(i) {                            // sync: copy every marked entry that lies inside the window
                    let (s, e) = (r.offset.saturating_sub(first.offset), r.offset.saturating_sub(first.offset).saturating_add(u64::from(r.len)));
                    if e > len { break; }
                    let entry = usize::try_from(s).ok().zip(usize::try_from(e).ok()).and_then(|(s, e)| win.get(s..e))
                        .ok_or_else(|| Error::Storage("entry outside window".into()))?;
                    w.append(entry);                                         // 2.1: the bytes are a complete full-object entry, valid in any pack
                    i = i.saturating_add(1);
                }
                w.upload_full(&mut budget.req).await?;
            }
            lo = hi;
        }
    }
    let bytes = w.finish(&mut budget.req).await?;
    publish(&sql, &cut.id, bytes, now)
}
/// One sync span. Header refs and bitmaps come from one captured snapshot (clone.refs, clone_plan), never the live refs table (blocker 1).
fn plan_cut(d: &RepoDo, sql: &SqlStorage, now: i64) -> Result<Option<Cut>, Error> {
    #[derive(Deserialize)] struct R { name: String, target: String }
    #[derive(Deserialize)] struct P { pack_id: String, full: Vec<u8>, state: String, count: u32, bytes: u64 }
    #[derive(Deserialize)] struct V { refs_version: i64 }
    let (Some(refs), Some(rv)) = (d.meta_opt("clone.refs")?, d.meta_opt("clone.refs_version")?) else { return Ok(None) };   // no plan yet: the next clone miss enqueues ClonePlan
    let rv: i64 = rv.parse().map_err(|_| Error::Internal("clone.refs_version".into()))?;
    let live = exec(sql, "SELECT refs_version FROM bundles WHERE state='live' ORDER BY created_at DESC LIMIT 1", vec![])?.to_array::<V>()?;
    if live.first().is_some_and(|b| b.refs_version >= rv) { return Ok(None); }   // the live bundle already covers this plan
    let refs: Vec<R> = serde_json::from_str(&refs).map_err(|e| Error::Internal(e.to_string()))?;
    let packs = exec(sql, "SELECT c.pack_id, c.full, p.state, p.count, p.bytes FROM clone_plan c JOIN packs p ON p.id = c.pack_id", vec![])?.to_array::<P>()?;
    if refs.is_empty() || packs.is_empty() || packs.iter().any(|p| p.state != "live") { return Ok(None); }   // empty repo, or GcSweep since the plan
    if packs.iter().try_fold(0u64, |a, p| a.checked_add(p.bytes)).is_none_or(|t| t > BUNDLE_MAX) { return Ok(None); }   // pack bytes bound object size and read count
    let count = packs.iter().flat_map(|p| p.full.iter()).try_fold(0u32, |n, b| n.checked_add(b.count_ones()))
        .ok_or_else(|| Error::Limit("bundle over 2^32 objects".into()))?;
    let mut header = b"# v2 git bundle\n".to_vec();                          // gitformat-bundle v2: header lines, blank line, then a plain pack
    for r in &refs { header.extend_from_slice(format!("{} {}\n", r.target, r.name).as_bytes()); }   // <40 hex> <full ref name>, validated at push (section 3)
    header.push(b'\n');
    let id = store::random_hex32()?;                                         // same generator as PackId at push begin (8.2, binding unverified)
    exec(sql, "INSERT INTO bundles(id, state, refs_version, created_at) VALUES(?, 'cutting', ?, ?)", vec![id.clone().into(), rv.into(), now.into()])?;
    Ok(Some(Cut { id, header, count, gc_epoch: d.meta_i64("gc_epoch")?, packs: packs.into_iter().map(|p| (PackId(p.pack_id), p.full, p.count, p.bytes)).collect() }))
}
/// Sync span: rows for the marked bits of one idx range in offset order (2.4), guarded by gc_epoch (3.2, 5.3): after a GcSweep the cut aborts, 4.4 retries.
fn marked_rows(d: &RepoDo, sql: &SqlStorage, pack: &PackId, bits: &[u8], lo: u32, hi: u32, epoch: i64) -> Result<Vec<Row>, Error> {
    if d.meta_i64("gc_epoch")? != epoch { return Err(Error::Conflict("gc ran during bundle cut".into())); }
    let rows = exec(sql, "SELECT offset, len, idx FROM objects WHERE pack_id=? AND idx>=? AND idx<? ORDER BY idx",
                    vec![pack.0.clone().into(), lo.into(), hi.into()])?.to_array::<Row>()?;
    Ok(rows.into_iter().filter(|r| bits.get((r.idx / 8) as usize).is_some_and(|b| b & (1u8 << (r.idx % 8)) != 0)).collect())
}
/// One sync span: the new bundle is the only live one; the old one is dead and sweep() deletes its key GRACE later (blocker 2).
fn publish(sql: &SqlStorage, id: &str, bytes: u64, now: i64) -> Result<SliceOutcome, Error> {
    exec(sql, "UPDATE bundles SET state='dead', dead_at=? WHERE state='live'", vec![now.into()])?;
    exec(sql, "UPDATE bundles SET state='live', bytes=? WHERE id=? AND state='cutting'", vec![i64::try_from(bytes).unwrap_or(i64::MAX).into(), id.into()])?;
    Ok(SliceOutcome::Reschedule { run_at: now.saturating_add(GRACE_MS).saturating_add(60_000) })   // come back to delete the dead object
}
/// Section 5 rule: delete only rows an earlier slice marked dead >= GRACE ago, plus 'cutting' rows older than GRACE (DO evicted mid-cut).
async fn sweep(sql: &SqlStorage, r2: &worker::Bucket, repo: &str, now: i64, b: &mut ReqBudget) -> Result<(), Error> {
    #[derive(Deserialize)] struct B { id: String, upload_id: Option<String> }
    let rows = exec(sql, "SELECT id, upload_id FROM bundles WHERE (state='dead' AND dead_at < ?) OR (state='cutting' AND created_at < ?) LIMIT 100",
                    vec![(now - GRACE_MS).into(), (now - GRACE_MS).into()])?.to_array::<B>()?;
    for r in rows {
        let key = format!("{repo}/{}.bundle", r.id);
        if let Some(u) = r.upload_id { b.charge(1)?; if let Ok(m) = r2.resume_multipart_upload(&key, &u) { let _ = m.abort().await; } }
        b.charge(1)?; r2.delete(&key).await.map_err(|e| Error::Storage(e.to_string()))?;
        exec(sql, "DELETE FROM bundles WHERE id=?", vec![r.id.into()])?;
    }
    Ok(())
}
struct BundleWriter { mpu: MultipartUpload, part: Vec<u8>, parts: Vec<UploadedPart>, n: u16, hasher: Hasher, bytes: u64 }
impl BundleWriter {
    fn pack_header(&mut self, count: u32) { let mut h = b"PACK\0\0\0\x02".to_vec(); h.extend_from_slice(&count.to_be_bytes()); self.append(&h); }
    fn append(&mut self, b: &[u8]) { self.hasher.update(b); self.part.extend_from_slice(b); }   // hasher covers pack bytes only, never the text header
    async fn upload_full(&mut self, b: &mut ReqBudget) -> Result<(), Error> {   // parts of exactly 8 MiB (6.4, R2 equal-size rule)
        while self.part.len() >= WINDOW as usize { let rest = self.part.split_off(WINDOW as usize); let p = std::mem::replace(&mut self.part, rest); self.put(p, b).await?; }
        Ok(())
    }
    async fn put(&mut self, chunk: Vec<u8>, b: &mut ReqBudget) -> Result<(), Error> {
        b.charge(1)?; self.bytes = self.bytes.saturating_add(chunk.len() as u64);
        self.parts.push(self.mpu.upload_part(self.n, Data::Bytes(chunk)).await.map_err(|e| Error::Storage(e.to_string()))?);
        self.n = self.n.checked_add(1).ok_or_else(|| Error::Limit("bundle over 10,000 parts".into()))?;
        Ok(())
    }
    async fn finish(mut self, b: &mut ReqBudget) -> Result<u64, Error> {
        let trailer = self.hasher.try_finalize().map_err(|e| Error::Internal(e.to_string()))?;   // gix-hash 0.26: name per docs, not run in the spike
        self.part.extend_from_slice(trailer.as_bytes());
        let last = std::mem::take(&mut self.part); self.put(last, b).await?;   // last part may be any size
        b.charge(1)?;
        self.mpu.complete(self.parts).await.map_err(|e| Error::Storage(e.to_string()))?;
        Ok(self.bytes)
    }
}
impl RepoDo {
    /// /_do/bundle-uri: one sync span (1.3). Only the newest live bundle is listed (2.38/2.39 download every listed one); an empty list is legal.
    pub fn bundle_uri(&self, base: &str) -> Result<Vec<u8>, Error> {
        #[derive(Deserialize)] struct B { id: String, refs_version: i64, created_at: i64 }
        let (sql, now, rv) = (self.state.storage().sql(), js_sys::Date::now() as i64, self.meta_i64("refs_version")?);
        let newest = exec(&sql, "SELECT id, refs_version, created_at FROM bundles WHERE state='live' ORDER BY created_at DESC LIMIT 1", vec![])?.to_array::<B>()?;
        let mut w = PktWriter::default();
        for l in ["bundle.version=1", "bundle.mode=all", "bundle.heuristic=creationToken"] { w.text(l)?; }
        if let Some(b) = newest.first() {
            w.text(&format!("bundle.{0}.uri={base}/{1}/{0}.bundle", b.id, self.meta_str("repo_id")?))?;   // id is 32 hex: dot-free, as bundle-uri.txt requires
            w.text(&format!("bundle.{}.creationToken={}", b.id, b.created_at))?;
        }
        if newest.first().is_none_or(|b| b.refs_version < rv) { jobs::enqueue(&sql, JobKind::BundleCut, now.saturating_add(QUIET_MS), "{}")?; }   // demand-driven, dedups (4.5)
        w.flush();
        Ok(w.out)
    }
}
/// GET /_bundles/<repo_id>/<id>.bundle, private mode only: section 12 auth, then R2 streamed without the DO. Public mode never hits the Worker.
pub async fn serve_bundle(req: Request, env: &Env, repo_id: &str, id: &str) -> Result<Response, Error> {
    auth::authenticate(&req, env)?;
    let hex32 = |s: &str| s.len() == 32 && s.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b));
    if !(hex32(repo_id) && hex32(id)) { return Err(Error::NotFound); }         // path segments are client bytes: validated before use
    let obj = env.bucket("BUNDLES").map_err(|e| Error::Storage(e.to_string()))?
        .get(format!("{repo_id}/{id}.bundle")).execute().await.map_err(|e| Error::Storage(e.to_string()))?.ok_or(Error::NotFound)?;
    let body = obj.body().ok_or(Error::NotFound)?.stream().map_err(|e| Error::Storage(e.to_string()))?;   // ByteStream: Stream<Item = Result<Vec<u8>>> (memo section 1)
    let mut resp = Response::from_stream(body).map_err(|e| Error::Storage(e.to_string()))?;              // unverified at runtime, like every from_stream user
    resp.headers_mut().set("content-type", "application/x-git-bundle").map_err(|e| Error::Internal(e.to_string()))?;
    Ok(resp)
}
```

## Why it works
- **A bundle is a text header over a plain pack, and every entry at rest is already a valid pack entry.** gitformat-bundle v2 is `# v2 git bundle\n`, `<oid> <refname>\n` lines, a blank line, then a pack that `unbundle` hands to `index-pack --fix-thin --stdin`. Section 2.1 stores every object as `varint(kind,size) + zlib(data)` with no `ofs-delta` or `ref-delta`, so `BundleWriter::append` copies entry bytes verbatim in any order (the same argument precomputed-clone-pack makes for bitmap slices), the count is the plan's popcount (`plan_cut`), and the trailer is `gix_hash::Hasher` over exactly the bytes after the text header (`finish`). No inflate, no delta, no base.
- **Header and pack describe one snapshot, by construction.** The plan job of precomputed-clone-pack captures its refs in `start()` and walks the closure of those tips into `clone_plan.full`; `plan_cut` reads the header lines from that same `clone.refs` row and the bitmaps from `clone_plan`, and never reads the live `refs` table. So every `<oid>` in the header is a captured tip whose closure is marked, and `update_ref` on the client finds the object; the first-pass "nonexistent object" failure cannot occur. Refs that moved after the plan make the bundle stale, not wrong, and stale is the design: the client's `fetch` then negotiates from the bundle tips (section 9 steps 1-2: known haves are ACKed, `ready`, small pack; scenario 12's path).
- **The cut cannot read a hole.** A pack is immutable once `live` (2.2) and its key survives GRACE after `dead_at` (5, Janitor step 3), longer than an alarm firing lives. `plan_cut` checks `p.state = 'live'` in the same sync span that inserts the `cutting` row; `marked_rows` re-checks `gc_epoch` in every sync span before it queries `objects`, the mechanism section 3 step 2 uses for pushes: a `GcSweep` (one sync span, 5.3) cannot interleave with the check-plus-query, so the job either sees all rows or aborts with `Error::Conflict` and 4.4 retries from scratch with a fresh id.
- **Storage is bounded and R2 deletion follows the section 5 rule.** `publish` is one sync span: the new row becomes `live`, the old `live` row becomes `dead` with `dead_at`, and the job reschedules itself GRACE + 1 min later so `sweep` deletes the old key in a later slice; a crash between `complete` and `publish` leaves a `cutting` row whose key and upload `sweep` removes after GRACE. R2 deletion therefore only ever targets rows a previous slice marked at least GRACE earlier, the wording of section 5, and there is at most one live bundle per repo plus one dead one for an hour.
- **The job obeys sections 4, 7 and 9.** Only `jobs::enqueue` is called (4.1, from `bundle_uri` on demand, dedup 4.5); the arm returns `Done`, or `Reschedule` after a cut. Reads are one `read_range` per 8 MiB window starting at the next uncopied marked entry, so a partially marked pack costs the same subrequests as a full one; parts are exactly 8 MiB (6.4) and every R2 call goes through `ReqBudget::charge` (7.1). `plan_cut` refuses plans over 512 MiB of pack bytes, which bounds a cut at about 64 reads + 65 parts + 3 calls, under the 320 a slice may spend (4.3). Async functions only await R2; `plan_cut`, `marked_rows`, `publish`, `pack_header`, `append` and `bundle_uri` are `fn`.
- **Wire shape matches git's client.** `remote-curl` in stateless-connect mode sends `command=bundle-uri` as one POST to `git-upload-pack` with capability lines and a flush and no `0001`; `parse_v2_command` (protocol-v2-only) never requires a delim, so `V2Command::BundleUri` is a one-arm addition. The reply from `bundle_uri` is the key-value list of bundle-uri.txt, flush-terminated, ids dot-free (32 hex), URIs absolute, `bundle.mode=all` with a single bundle so 2.38/2.39 (no `creationToken` heuristic, every listed bundle downloaded) and 2.40+ behave the same. `response-end` is never written (rule 3).
- **Failure degrades to the foundation path.** A missing or truncated object (bad trailer at `index-pack`), a 401 on the private route, or a repo with no bundle (empty list) makes `clone.c` warn and continue with an ordinary `fetch`; nothing this module does can make a clone fail that would otherwise succeed. `--depth` and `--filter` clones never ask for bundles, matching the plan gate of precomputed-clone-pack that also excludes them.
- **Identity and auth are the contract's.** Keys use `meta.repo_id` (8.2, 8.4), so a rename changes nothing in R2. The private route runs `auth::authenticate` with the same two tokens as `git-upload-pack` (section 12); git downloads the bundle through `git remote-https`, whose `get` command retries a 401 with the credential helper.
- Conformance (section 11): this proof must pass 1 (empty repo: `bundle-uri` answers an empty list, clone proceeds), 3 (tag refs in the header land as `refs/bundles/tags/v1.0`; annotated tag objects are in the plan) and 12 (fetch after bundle negotiates with `ACK`). Added scenario 17: push 3 commits, fire the alarm until `bundles` has a `live` row, `git -c transfer.bundleURI=true -c credential.helper=store clone` with `GIT_TRACE_PACKET=1`; assert the trace shows `command=bundle-uri` and `bundle.<id>.uri=`, that `git bundle verify` on the downloaded file passes, and that no `packfile` section was sent; then push one more commit and clone again: assert the `fetch` carried `have <old tip>`, got `ACK`, and the pack count equals the new objects. Added scenario 18: force-push away 100 commits, advance the clock past GC, fire alarms; assert the old bundle row is `dead`, its key is gone after GRACE, and a new clone with `transfer.bundleURI=true` passes `fsck`.

## Changes from the first pass
| First-pass finding (quoted) | How addressed |
|---|---|
| Blocker 1: "Bundle header refs are read from live `refs` at alarm time instead of the tips snapshotted with the full pack; any push between repack and bundle alarm produces a bundle whose ref points at an object not in the pack" | `plan_cut` builds the header from `clone.refs`, the snapshot precomputed-clone-pack captures in the same sync span that starts its walk, and the pack from that walk's `clone_plan` bitmaps; the live `refs` table is not read anywhere in the cut. `bundles.refs_version` records which snapshot the bundle covers. |
| Blocker 2: "Row pruning deletes SQL rows but never deletes the R2 bundle objects; every rebuild leaks a full-pack-sized object indefinitely" | `publish` marks the previous bundle `dead` with `dead_at` and reschedules the job GRACE + 1 min later; `sweep` deletes keys of `dead` rows older than GRACE and of `cutting` rows older than GRACE (aborting their upload), then the rows. At most one live and one dead object per repo. |
| Blocker 3: "Hard dependency on a self-contained full pack from gc-and-repack-alarm / precomputed-clone-pack; nothing correct exists to wrap without it" | Not a wrap any more. The cut assembles its own pack from the plan bitmaps over any number of live packs (`run_bundle_cut` window loop, `BundleWriter`), valid because every entry at rest is a full object (2.1). It depends on the clone plan for the closure, not on `GcConsolidate` having produced one pack; GC only makes the cut cheaper. |
| Caveat: "Only opt-in clients (transfer.bundleURI=true ... or --bundle-uri) use it, so the 'bulk bytes never pass through the DO' benefit reaches near-zero default clients" | Not addressed, because it is git's default and no server can change it. Known limits states it; default clients get precomputed-clone-pack's in-band fast path, which reads the same bitmaps. |
| Caveat: "On git 2.38/2.39 the creationToken heuristic is unknown and mode=all downloads every listed bundle; listing the newest two doubles clone bytes there. List only the newest row" | `bundle_uri` lists exactly one row (`WHERE state='live' ... LIMIT 1`); the previous object stays in R2 for GRACE for in-flight downloads but is never listed. |
| Caveat: "CDN cacheable-object limit (512 MB non-Enterprise) and ~5 GiB single R2 put (multipart not shown) bound large repos" | Multipart is the only write path (`BundleWriter`, 8 MiB parts, up to 10,000). `BUNDLE_MAX = 512 MiB` is now an explicit gate in `plan_cut`; above it no bundle is cut (Known limits: the one-slice bound coincides with the CDN cap). |
| Caveat: "command=bundle-uri request carries no delim/argument section; the Worker parser must accept that shape. Refs land as refs/bundles/heads/main, not refs/bundles/<id>/<refname>" | `parse_v2_command` (protocol-v2-only) treats delim as optional, noted in the code header comment; the proof text uses `refs/bundles/<name minus refs/>` throughout and nothing server-side depends on the client's ref layout. |
| Caveat: "Private repos fall back to per-colo Cache API behind an auth Worker; presigned S3 URLs bypass the CDN cache. Incremental bundles are out of scope" | Private mode is `serve_bundle`: `auth::authenticate` then a streamed R2 read, no DO. No Cache API is used (Known limits: `worker::Cache` binding unverified, so the private path is uncached R2 reads). Public mode is a deployment-wide switch (`GE_BUNDLE_BASE` on the bucket's custom domain), because the foundation's auth is two global tokens, not per repo. Incremental bundles remain out of scope. |
| Review crash walk-through: "`BUCKET.put(...)` completes, DO evicted before the `INSERT INTO bundles`. Result: a complete ... object that nothing references" | The row is inserted `cutting` before the upload starts and `upload_id` is stored right after `create_multipart_upload`, so every object or upload has a row; `sweep` reaps `cutting` rows after GRACE. R2 put and SQL row are still not atomic, but the non-atomic window now produces a tracked row, not an untracked object. |
| Review concurrency: "`fullClonePack()` deleting-while-streaming is the same unspecified hazard as idea #7" | The bundle object is never served by the DO. The foundation packs it reads are immutable and kept GRACE after death (2.2, 5); the bundle object itself is kept GRACE after it stops being listed. |
| Review interop: "`have`s from `refs/bundles/*` do reach the server ... negotiation must tolerate haves it does not know" | Section 9 step 1 drops unknown haves and ACKs known ones; a force-pushed-away bundle tip that GC swept is dropped, and the client falls back to more haves or `done`. |
| Review: "Alarm copies the pack through the DO (R2 get -> R2 put) ... wall-clock scales with pack size" | Still a copy through the DO (there is no server-side R2 copy), now bounded: 512 MiB per cut, 8 MiB in flight per read and per part, and the cut is refused rather than attempted when the plan is larger. |

## Known limits
- **One slice, 512 MiB.** The cut cannot resume: `gix_hash::Hasher` has no serializable state, so a multipart upload cannot be continued in a later alarm firing with the trailer still correct. `BUNDLE_MAX` keeps a cut at about 130 subrequests, but a 512 MiB copy can exceed the 20 s wall guideline of 4.3 while staying inside the platform's alarm limits (30 s CPU, `limits.cpu_ms` raised to 300 s, minutes of wall). Write-back to 4.3: a slice may declare itself non-resumable and run to its own byte cap. Section 5.2's `GcConsolidate` has the same unstated hasher-resume problem; a vendored 60-line SHA-1 with `[u32; 5]` state in the cursor fixes both and lifts this cap.
- **The size gate over-estimates.** `plan_cut` sums `packs.bytes`, not marked bytes; a repo with 600 MiB of live packs and 300 MiB reachable gets no bundle until GC consolidates. Summing `objects.len` over marked bits costs a full scan and was left out.
- **Opt-in only, clone only.** `transfer.bundleURI` defaults to false in every released git; `--depth` and `--filter` clones never ask. `bundle.heuristic=creationToken` needs 2.40+; older clients ignore the key and, with one bundle listed, behave the same. Whether a protocol-advertised list makes clone record `fetch.bundleURI` for later fetches is not relied on; incremental bundles with prerequisite lines are out of scope.
- **Private mode is uncached and may prompt twice.** `serve_bundle` streams from R2 on every request (one subrequest, `Response::from_stream` unverified at runtime); `worker::Cache` is not in the memo's verified list and is not used. `git remote-https` runs as a separate process for the bundle download, so without a credential helper the user is asked for the password a second time; scenario 17 sets `credential.helper=store`.
- **Public mode is all-or-nothing.** The foundation has no per-repo visibility (section 12), so `GE_BUNDLE_BASE` on a public custom domain exposes every repo's history at an unguessable but unauthenticated URL; only a deployment where every repo is public may set it. Cloudflare's cache serves objects up to 512 MB on non-Enterprise plans; `cacheControl` metadata on the object (`immutable`, keys are never reused) is set through the multipart builder, shape unverified.
- **Staleness follows the plan.** A cut needs a plan; a plan needs a clone miss and a `ClonePlan` run (precomputed-clone-pack). A demand enqueue from `bundle_uri` while the job row is already queued for its GRACE + 1 min sweep is a dedup no-op (4.5), so a new cut can wait up to an hour after a plan change. A DO evicted mid-cut leaves the job row `running`; 4.2 has no stale-`running` reset, which every job kind needs and this proof does not add.
- **Memory.** Per cut: one 8 MiB window (or one entry up to 32 MiB, 2.4), the part buffer up to 16 MiB around `split_off`, 50,000 `Row`s (about 1 MB), the bitmaps (125 KB per million objects per pack), the `clone.refs` JSON and header (about 120 bytes per ref: 100,000 refs is 12 MB). Under 80 MB worst case in a 128 MB isolate.
- **Unverified, day-1 list.** `MultipartUpload::upload_id()`, `CreateMultipartUploadOptionsBuilder::http_metadata`, `resume_multipart_upload` sync signature, `Bucket::delete(key)` on the `worker` crate; `Hasher::{update, try_finalize}` names in gix-hash 0.26.2; BLOB reads through `SqlStorageValue`; `Option::is_none_or` (Rust 1.82+, fine on the spike's 1.94 toolchain); real R2 multipart part-size enforcement and the DO subrequest limit on a deployed Worker (platform-facts #6, #7); that `clone.c` skips an empty bundle list (read in source, not run).
- **Write-backs this proof needs.** `JobKind::BundleCut` and its `run_slice` arm (1.4, 4.5); `V2Command::BundleUri`, a conditional `bundle-uri` line in rule 6 and the `/_do/bundle-uri` route in 1.3 (sync, no awaits); `schema_version = 3` for `bundles`; the `BUNDLES` binding and `GE_BUNDLE_BASE` var in `wrangler.jsonc`; `clone.refs` next to `clone.tips` in precomputed-clone-pack's `start()`; `SliceBudget.req: ReqBudget`, `RepoDo::{bucket, meta_opt}` and the `store::Bucket.repo` field as the sibling proofs assume; `store::random_hex32` shared with push begin; section 12's `bundle-uri` line pointing at this module.

## Depends on
- precomputed-clone-pack
- protocol-v2-only
- want-have-negotiation
- refs-sqlite-objects-r2
- gc-and-repack-alarm
- info-refs-endpoint
