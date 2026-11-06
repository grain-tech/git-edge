# Per-blob presigned direct upload for giant pushes

> Second pass · Idea #24 · verdict: **lands with caveats** · feasibility 3/5 · reliability 3/5 · correctness 3/5 (first pass 4/2/3)
> First pass: [proof](./archived/presigned-direct-upload.md) · [review](../reviews/archived/presigned-direct-upload.md) · Second pass: [review](../reviews/presigned-direct-upload.md) · Contract: [CONTRACTS.md](../../CONTRACTS.md)

## Mechanism

```
REGISTRY (amendment A9)
edge route    POST /:owner/:repo.git/blobs/batch                              auth: can_write
DO routes     POST /_do/blobs/batch (awaits: none) · POST /_do/blobs/claims (awaits: none)
JobKind       BlobSweep
table         blob_claims(sha TEXT PRIMARY KEY, size INTEGER NOT NULL, claimed_at INTEGER NOT NULL) WITHOUT ROWID
R2 prefix     r/<repo_id>/blobs/<oid>          scratch, like pending/ (2.2); no reader ever resolves it
write-backs   CommitRequest.pack_id -> pack_ids: Vec<String> · PackWriter::{begin_entry, append_encoded, end_entry}
              · store::keys::blob · store::Bucket.inner pub(crate) (as native-lfs) · extract_links skips mode 160000
```

The first pass kept loose objects at rest; CONTRACTS.md 2.1 forbids that ("There are no loose objects"), 2.2 allows no key besides `packs/` and `pending/`, and section 12 lists presigned uploads as out of scope, so the idea is reshaped, not weakened. The bespoke client (`git-remote-edge`; stock `git push` can never do this — the wire protocol has no "blob X is already there") POSTs `{objects:[{oid,size}]}` to the new edge route; `/_do/blobs/batch` answers `have` for oids already live (the 2.3 query) and otherwise upserts a `blob_claims` row plus a presigned PUT for `r/<repo_id>/blobs/<oid>` whose signed `x-amz-checksum-sha1` equals the oid, so the body must be `blob <size>\0<content>` and SHA-1 of the body is the oid by construction. The client PUTs straight to `r2.cloudflarestorage.com`, then pushes normally with a blob-filtered pack (`git pack-objects --filter=blob:limit`). `pack::ingest::run` (two-phase-push) is unchanged until the section 2.5 connectivity step: links that are neither entries of this push's pack nor live are matched against `blob_claims`, and each claimed oid is streamed out of R2, rehashed, and appended — an ordinary full-object entry — to a **second** normalized pack that flips `live` in the same commit span (`pack_ids`). The loose key is scratch: readers still resolve only through 2.3, every stored object still lives in a pack, so fetch, `GcMark`/`GcConsolidate`/`GcSweep` and the Janitor are untouched. `BlobSweep` removes claim rows and their scratch keys only for claims older than TTL + GRACE, the mark-then-delete rule of 5.3.

## Primitives

- R2 S3 presigned PUT with a signed `x-amz-checksum-sha1`: changelog-documented per the first-pass review (2023-06-16, 2022-09-19); that a mismatched body is rejected and that `head().checksums.sha1` is populated are **unverified** — day-1 gate, Known limits. Signing is `lfs::sigv4::{presign, S3Creds}` (native-lfs): `hmac`/`hex` already in the spike tree, `sha2`/`base64` not in the memo and not built.
- `worker::Bucket::{head, get, delete, delete_multiple}`, `Object::{size, checksum}`, `R2Checksums.sha1`, `ObjectBody::stream() -> Stream<Item = Result<Vec<u8>>>`: verified in `worker` 0.8.5 source (`r2/mod.rs`, `r2/checksums.rs`; the same list native-lfs verified).
- `gix_hash::{hasher, Hasher::{update, try_finalize}}`, incremental SHA-1: measured on workerd (spike).
- `gix_zlib::stream::deflate::Write` (0.1.0): source-read; deflate on wasm32 was not exercised by the spike; `get_mut`/`try_finish` method names **unverified**.
- `gix_object::Kind`, `gix_pack::data::entry::Header::write_to`: verified in 0.64.1 / 0.74.2 source (content-addressed-r2-keys).
- `SqlStorage::exec` synchronous, `SELECT changes()`: measured 1 / 0 / 1 (platform-facts #1); sync-span atomicity after an R2 await measured (#4).
- `json_each(?)` for the claims IN-list: the 100-bound-parameter rule of A6.
- `Index::lookup` (live packs only, 2.3), `IndexSink`, `lookup` (with the `pack` presence parameter), `stub_json`, `PackWriter::{create, flush_if_full, finish, abort}`: two-phase-push signatures as amended by A1.
- `jobs::enqueue` dedup by kind, `jobs::rearm` as the only `set_alarm` caller (4.1, A3); a second `setAlarm` cancels the first: measured (#5).
- `Stub::fetch_with_request` + `Request::new_with_init` JSON stub path: **unverified at runtime** (two-phase-push). `PackId::random` needs `web_sys::Crypto`: binding path unverified (8.2).
- `ObjectId::from_hex` as the 40-hex gate; `js_sys::Date::now()`; `Env::{var, secret}` for the R2 credentials: standard / verified in 0.8.5 (`env.rs`).

## Proof code

```rust
// src/repo_do/blobs.rs + src/pack/ingest/stage.rs (edge) + src/jobs/blob_sweep.rs + src/edge/blobs.rs.
// worker 0.8.5, gix-hash 0.26.2, gix-zlib 0.1.0. `q`, `changes`, `oid`, `json`, `now_ms`, `sql`, `bucket`,
// `repo_id`: RepoDo helpers (repo-do-ref-authority, native-lfs); `stub_json`, `lookup`, `unpack`, `IndexSink`:
// two-phase-push; `presign`, `S3Creds`: crate::lfs::sigv4 (native-lfs). From<worker::Error> -> Error::Storage.
use std::{collections::HashMap, io::Write as _};
use futures_util::TryStreamExt;
use gix_hash::{Kind as H, ObjectId};
use gix_object::Kind;
use gix_zlib::stream::deflate;
use serde_json::json;
use worker::{Env, Request, Response, SqlStorageValue as V, Stub};
use crate::{auth::Principal, edge::RepoRoute, error::Error, jobs::{self, Job, JobKind, SliceBudget, SliceOutcome},
            lfs::sigv4::{presign, S3Creds}, repo_do::RepoDo,
            store::{keys, Bucket, Index, ObjRow, PackId, PackMeta, PackWriter}, ReqBudget};
pub const TTL_S: u32 = 3_600;                                            // inside lfs::sigv4::presign
const CLAIM_MS: i64 = 7_200_000;                                         // URL TTL (1 h) + GRACE (1 h), section 5
const MAX_BATCH: usize = 1_000; const MAX_BLOB: u64 = 5 << 30;             // S3 single-PUT cap
const HEADS_PER_SLICE: usize = 150;                                      // heads + 1 delete_multiple <= 80% of 400 (4.3)
// store::keys write-back: pub fn blob(repo: &RepoId, oid: &str) -> String { format!("r/{}/blobs/{oid}", repo.0) }
// boot migration (schema_version bump): the blob_claims table of the REGISTRY block.

// ---- src/repo_do/blobs.rs. Both routes are "Awaits inside: none" (1.3): one sync span each. ----
#[derive(serde::Deserialize)] pub struct BatchDto { pub objects: Vec<BlobIn> }
#[derive(serde::Deserialize)] pub struct BlobIn { pub oid: String, pub size: u64 }
#[derive(serde::Deserialize)] pub struct ClaimsDto { pub ids: Vec<String> }
#[derive(serde::Deserialize)] struct ShaRow { sha: String, size: i64 }
impl RepoDo {
    /// One presigned PUT per oid the index does not already have. The signed `x-amz-checksum-sha1` pins the
    /// body to its name; content-length is deliberately unsigned (review): size is enforced at copy time.
    pub fn blobs_batch(&self, b: &BatchDto) -> Result<Response, Error> {
        if b.objects.len() > MAX_BATCH { return Err(Error::Limit("blobs batch > 1000 objects".into())); }
        let (creds, repo, now) = (S3Creds::from_env(&self.env)?, self.repo_id()?, now_ms());   // repo_id from meta (8)
        let idx = Index(&self.sql()); let mut issued = false;
        let mut out = Vec::with_capacity(b.objects.len());
        for o in &b.objects {
            let id = oid(&o.oid)?;                                             // 40 hex: the only path to a key
            if o.size > MAX_BLOB { return Err(Error::Limit("blob > 5 GiB needs the multipart flow".into())); }
            if idx.lookup(&[id])?.into_iter().next().flatten().is_some() {      // live already: skip the upload
                out.push(json!({ "oid": o.oid, "have": true })); continue;
            }
            let size = i64::try_from(o.size).map_err(|_| Error::Protocol("size".into()))?;
            self.q("INSERT INTO blob_claims(sha,size,claimed_at) VALUES(?,?,?) \
                    ON CONFLICT(sha) DO UPDATE SET size=excluded.size, claimed_at=excluded.claimed_at",
                   vec![V::from(o.oid.as_str()), V::from(size), V::from(now)])?;
            issued = true;
            let sum = base64::engine::general_purpose::STANDARD.encode(id.as_slice());
            out.push(json!({ "oid": o.oid, "have": false, "expires_in": TTL_S,
                             "headers": { "x-amz-checksum-sha1": sum },
                             "put": presign(&creds, "PUT", &keys::blob(&repo, &o.oid), now,
                                            &[("x-amz-checksum-sha1", &sum)])? }));
        }
        if issued { jobs::enqueue(&self.sql(), JobKind::BlobSweep, now + CLAIM_MS, "{}")?; }   // dedups (4.5)
        json(json!({ "objects": out }))          // fetch calls jobs::rearm(&self).await after this span (A3)
    }
    /// Which of <= 1,000 ids carry a claim; the UPDATE leases every returned row, so a request that sees a
    /// claim holds it CLAIM_MS - max_ms (7.1) beyond the sweep's reach.
    pub fn blobs_claims(&self, b: &ClaimsDto) -> Result<Response, Error> {
        if b.ids.len() > 1_000 { return Err(Error::Internal("claims > 1000 ids".into())); }
        let arr = serde_json::to_string(&b.ids).map_err(|e| Error::Internal(e.to_string()))?;  // A6: json_each
        let rows = self.q("SELECT sha, size FROM blob_claims WHERE sha IN (SELECT value FROM json_each(?))",
                          vec![V::from(arr.as_str())])?.to_array::<ShaRow>()?;
        if !rows.is_empty() {
            self.q("UPDATE blob_claims SET claimed_at=? WHERE sha IN (SELECT value FROM json_each(?))",
                   vec![V::from(now_ms()), V::from(arr)])?;
        }
        json(json!({ "claimed": rows.iter().map(|r| json!({ "oid": r.sha, "size": r.size })).collect::<Vec<_>>() }))
    }
}

// ---- src/pack/ingest/stage.rs (edge): turns connectivity holes into entries of a second normalized pack. ----
#[derive(serde::Deserialize)] struct Claims { claimed: Vec<Claimed> }
#[derive(serde::Deserialize)] struct Claimed { oid: String, size: i64 }
/// `missing` = links that resolved neither to this push's pack nor to a live pack (2.5). Every id must carry a
/// claim and then be rehashed out of R2 into pack2; any hole is `unpack error missing object <oid>` (10, A2).
pub async fn stage(bucket: &Bucket, stub: &Stub, repo: &RepoRoute, missing: &[ObjectId], budget: &mut ReqBudget)
    -> Result<Option<(PackMeta, Vec<ObjRow>)>, Error> {
    if missing.is_empty() { return Ok(None); }
    let mut want: HashMap<ObjectId, u64> = HashMap::new();
    for chunk in missing.chunks(1_000) {
        let ids: Vec<String> = chunk.iter().map(ToString::to_string).collect();
        let r: Claims = stub_json(stub, repo, "/_do/blobs/claims", &json!({ "ids": ids }), budget).await?;
        for c in r.claimed {
            let id = ObjectId::from_hex(c.oid.as_bytes()).map_err(|e| Error::Internal(e.to_string()))?;
            want.insert(id, u64::try_from(c.size).map_err(|_| Error::Internal("claim size".into()))?);
        }
    }
    if let Some(id) = missing.iter().find(|id| !want.contains_key(*id)) {
        return Err(unpack(format!("missing object {id}")));             // not staged and not live: reject
    }
    let pack = PackId::random();
    let count = u32::try_from(missing.len()).map_err(|_| Error::Limit("too many staged blobs".into()))?;
    let mut out = PackWriter::create(bucket, &keys::pack(&bucket.repo, &pack), count, budget).await?;    // A1
    let mut rows = Vec::with_capacity(missing.len());
    for id in missing {
        let declared = want.get(id).copied().ok_or_else(|| Error::Internal("claim lost".into()))?;
        match copy_blob(bucket, &keys::blob(&bucket.repo, &id.to_string()), *id, declared, &mut out, budget).await {
            Ok(r) => rows.push(r), Err(e) => { out.abort().await; return Err(e); }
        }
    }
    Ok(Some((out.finish(budget).await?, rows)))                         // pack2 durable before commit (3.1)
}
/// Streams `blob <n>\0<content>` out of R2: the checksum header is a hint, the per-byte rehash is the proof.
/// Resident memory: one stream chunk + one deflate buffer + the <= 8 MiB part — nothing holds the object
/// whole, which is the only reason the A7 cap is lifted for this path (write-back).
async fn copy_blob(bucket: &Bucket, key: &str, want: ObjectId, declared: u64, out: &mut PackWriter,
    budget: &mut ReqBudget) -> Result<ObjRow, Error> {
    budget.charge(1)?;
    let head = bucket.inner.head(key).await?.ok_or_else(|| unpack("staged blob missing"))?;
    if let Some(sum) = head.checksum().sha1 {                           // kept checksum: early reject, not proof
        if sum.as_slice() != want.as_slice() { return Err(unpack("staged blob checksum mismatch")); }
    }
    budget.charge(1)?;
    let body = bucket.inner.get(key).execute().await?.and_then(|o| o.body())
        .ok_or_else(|| unpack("staged blob missing"))?;
    let mut stream = body.stream()?;
    let (mut h, mut pre) = (gix_hash::hasher(H::Sha1), Vec::<u8>::new());
    let mut ent: Option<(u64, deflate::Write<Vec<u8>>)> = None;
    while let Some(chunk) = stream.try_next().await? {
        h.update(&chunk);
        if let Some((_, z)) = ent.as_mut() {
            z.write_all(&chunk).map_err(|e| Error::Internal(format!("deflate: {e}")))?;
        } else {
            pre.extend_from_slice(&chunk);
            let Some(nul) = pre.iter().position(|b| *b == 0) else {
                if pre.len() > 64 { return Err(unpack("bad staged header")); }
                continue;
            };
            let size = std::str::from_utf8(pre.get(..nul).ok_or_else(|| unpack("bad staged header"))?)
                .map_err(|_| unpack("bad staged header"))?
                .strip_prefix("blob ").and_then(|n| n.parse::<u64>().ok())
                .ok_or_else(|| unpack("bad staged header"))?;
            if size != declared || head.size() != size.saturating_add(nul as u64).saturating_add(1) {
                return Err(unpack("staged blob size mismatch"));
            }
            let start = out.begin_entry(size)?;                          // varint entry header (write-back)
            let mut z = deflate::Write::new(Vec::new(), gix_zlib::Compression::DEFAULT);
            z.write_all(pre.get(nul.saturating_add(1)..).ok_or_else(|| unpack("bad staged header"))?)
                .map_err(|e| Error::Internal(format!("deflate: {e}")))?;
            ent = Some((start, z)); pre = Vec::new();
        }
        if let Some((_, z)) = ent.as_mut() {
            z.flush().map_err(|e| Error::Internal(format!("deflate: {e}")))?;        // sync-flush per chunk
            let produced = std::mem::take(z.get_mut());                              // get_mut: unverified
            out.append_encoded(&produced)?;
        }
        out.flush_if_full(budget).await?;
        if js_sys::Date::now() - budget.started_ms > budget.max_ms { return Err(Error::Budget); }   // 7.1
    }
    let (start, mut z) = ent.ok_or_else(|| unpack("staged blob truncated"))?;
    z.try_finish().map_err(|e| Error::Internal(format!("deflate: {e}")))?;           // stream end; name unverified
    let produced = std::mem::take(z.get_mut()); out.append_encoded(&produced)?;
    out.flush_if_full(budget).await?;
    let got = h.try_finalize().map_err(|_| unpack("sha1 collision"))?;
    if got != want { return Err(unpack("staged blob does not hash to its name")); }
    let (idx, len) = out.end_entry(start)?;                                          // count += 1
    Ok(ObjRow { sha: want, idx, offset: start, len, kind: Kind::Blob, size: declared })
}

// ---- store::PackWriter additions (1.2 write-back; everything else of 2.4/A1 unchanged) ----
impl PackWriter {
    /// Starts a Blob entry whose content arrives in pieces; the staged-blob path is the only caller.
    pub fn begin_entry(&mut self, size: u64) -> Result<u64, Error> {                 // returns entry offset
        let mut hdr = Vec::with_capacity(9);
        gix_pack::data::entry::Header::Blob.write_to(size, &mut hdr).map_err(|e| Error::Internal(format!("hdr: {e}")))?;
        self.append_encoded(&hdr)?;
        Ok(self.offset.saturating_sub(u64::try_from(hdr.len()).map_err(|_| Error::Internal("len".into()))?))
    }
    pub fn append_encoded(&mut self, bytes: &[u8]) -> Result<(), Error> {
        self.hasher.update(bytes); self.part.extend_from_slice(bytes);
        self.offset = self.offset.saturating_add(u64::try_from(bytes.len()).map_err(|_| Error::Internal("len".into()))?);
        Ok(())
    }
    pub fn end_entry(&mut self, start: u64) -> Result<(u32, u32), Error> {           // (idx, len)
        let len = u32::try_from(self.offset.saturating_sub(start)).map_err(|_| Error::Limit("entry > 4 GiB".into()))?;
        let idx = self.count; self.count = self.count.saturating_add(1); Ok((idx, len))
    }
}

// ---- src/jobs/blob_sweep.rs: JobKind::BlobSweep, one slice per firing (4.2); never set_alarm (4.1). ----
pub async fn run_slice(d: &RepoDo, _job: &Job, budget: &mut SliceBudget) -> Result<SliceOutcome, Error> {
    let now = now_ms(); let bucket = d.bucket()?;
    // Rows whose URL died >= GRACE ago; the claims lease keeps any row a live request can see fresh.
    let stale: Vec<ShaRow> = d.q("SELECT sha, size FROM blob_claims WHERE claimed_at < ? ORDER BY claimed_at LIMIT ?",
        vec![V::from(now.saturating_sub(CLAIM_MS)), V::from(HEADS_PER_SLICE as i64)])?.to_array()?;
    let (mut done, mut keys) = (Vec::new(), Vec::new());
    for r in &stale {
        if budget.spent_80pct() { break; }                                          // A4
        budget.subrequests_used = budget.subrequests_used.saturating_add(1);
        if bucket.inner.head(&keys::blob(&bucket.repo, &r.sha)).await?.is_some() {
            keys.push(keys::blob(&bucket.repo, &r.sha));
        }
        done.push(r.sha.clone());
    }
    if !keys.is_empty() {
        budget.subrequests_used = budget.subrequests_used.saturating_add(1);
        bucket.inner.delete_multiple(keys).await?;    // scratch keys: refs never resolve them, so same-slice
    }                                               // mark-then-delete cannot orphan data (unlike 5.3 packs)
    for sha in &done { d.q("DELETE FROM blob_claims WHERE sha=?", vec![V::from(sha.as_str())])?; }  // fresh span
    Ok(if stale.len() >= HEADS_PER_SLICE { SliceOutcome::Continue { cursor: String::new() } } else { SliceOutcome::Done })
}

// ---- src/edge/blobs.rs: POST /:owner/:repo.git/blobs/batch (JSON in, JSON out) ----
pub async fn batch(mut req: Request, env: &Env, repo: &RepoRoute, p: &Principal, budget: &mut ReqBudget)
    -> Result<Response, Error> {
    if !p.can_write { return Err(Error::Forbidden); }
    let text = req.text().await?;
    if text.len() > 1 << 20 { return Err(Error::Protocol("batch body > 1 MiB".into())); }
    let b: serde_json::Value = serde_json::from_str(&text).map_err(|e| Error::Protocol(e.to_string()))?;
    Response::from_json(&stub_json(&repo.stub(env)?, repo, "/_do/blobs/batch", &b, budget).await?)
}

// ---- the tail of pack::ingest::run (two-phase-push): the connectivity block now collects holes and stages ----
//   let meta = out.finish(budget).await?; sink.post(&meta, &tail, budget).await?;      // unchanged
//   let mut missing = Vec::new();
//   for chunk in links.chunks(1_000) {
//       missing.extend(lookup(stub, repo, chunk, Some(&pack), budget).await?.into_iter()
//           .filter_map(|(id, l)| l.is_none().then_some(id)));
//   }
//   let mut pack_ids = vec![pack.0.clone()];
//   if let Some((m2, rows2)) = stage(bucket, stub, repo, &missing, budget).await? {
//       IndexSink { stub, repo, pack: m2.pack.clone(), push: push.clone(), links: Vec::new() }
//           .post(&m2, &rows2, budget).await?;                                        // blob rows carry no links
//       pack_ids.push(m2.pack.0);
//   }
//   // CommitRequest write-back: pack_id: Option<String> -> pack_ids: Vec<String>; commit_push step 3 loops its
//   // 'ingesting'->'live' UPDATE + changes()==1 once per id in the same sync span; run returns Ok(pack_ids).
```

## Why it works

- **No ref can ever point at a loose key.** Both losing interleavings in the review needed a committed ref to depend on `objects/<oid>`. Here refs resolve only through the 2.3 query into a `live` pack, and every blob a tree names is copied *into* a pack before `/_do/push/commit`. `BlobSweep` may delete `blobs/<oid>` at any time: the worst outcome is a 404 on the mid-copy `get`, one `unpack error`, never a dangling ref. The lease makes even that unreachable: `blobs_claims` sets `claimed_at = now` on the rows it returns and the sweep only touches rows older than `CLAIM_MS` (2 h), so a claim a live request can see is at least `CLAIM_MS - max_ms` (7.1) from eligibility — ~116 minutes against a 240 s request.
- **Nothing awaits inside the ref transaction.** Verification is the streamed rehash in `copy_blob`, run in the edge before commit; `commit_push` is the unchanged sync span except that step 3 loops `pack_ids`, still `changes()`-checked per pack, still `gc_epoch`-guarded (3 step 2). The claims read is a separate sync span and is not trusted anyway: a claim only says "try this key"; the copy demands `sha1(body) == oid` over every byte, so a claim for an absent, short or wrong body fails the push, not the data.
- **Ordering is section 3's, twice.** `stage` runs `create` -> per-blob `copy_blob` -> `finish` -> index post, so pack2 obeys "durable, then rows, then commit" exactly like pack1; both packs flip `ingesting -> live` in one span, so at no instant does a live ref resolve into a non-live pack. A mid-stage failure calls `abort` and leaves an `ingesting` row the Janitor kills at `PUSH_TIMEOUT` (5.2) plus an incomplete multipart for the lifecycle rule (two-phase-push Known limits).
- **Content addressing keeps every step idempotent.** The key names the bytes; a re-PUT of identical bytes is a no-op; a re-run batch returns `have` for what is already live and fresh URLs for the rest; two pushes sharing a blob each copy it into their own pack — duplicate shas across live packs are legal (2.3) and `GcConsolidate` dedups. A stale claim racing an ordinary pack-carried push touches nothing, because the ordinary path never consults `blobs/` at all.
- **Declared size is checked three ways** — `blob_claims.size`, the staged object's own `blob <n>\0` header, and `head().size()` — before it can reach `ObjRow.size`, which is what fetch filters and `BlobLimit` later read.
- **Budget (7).** `blobs/batch` costs 0 subrequests (signing is a handful of SHA-256/HMAC blocks per URL). Ingest adds `ceil(missing/1,000)` claims calls plus, per staged blob, 1 `head` + 1 `get` + `bytes/8 MiB` `upload_part`s: a 4 GiB blob costs ~515, ten of them ~5,200 of the 9,000. A sweep slice costs at most 151.
- **Memory and the cap.** One stream chunk + one deflate buffer + the <= 8 MiB part are resident; the 16 MiB object cap of A7 exists for paths that hold an object whole, and is lifted for this path only, by write-back. The `pre` header buffer is bounded at 64 bytes before it errors.
- **Errors map the contract way (10, A2).** Unclaimed or un-copyable links are `Error::Unpack` raised in the edge, answered after the receive header with HTTP 200, `unpack error missing object <oid>` and `ng <ref> unpack failed` for every command. `Limit` (batch > 1,000, blob > 5 GiB, entry > 4 GiB) is 413 before the header. No `unwrap`/`[]`/narrowing on client or R2 bytes: `oid()`, `try_from`, `get`, `?` throughout.

## Changes from the first pass

| First-pass finding (quoted) | Kind | How addressed |
|---|---|---|
| "Janitor must not delete R2 objects; it races both this proof's own `resolveBlobs()` and the ordinary push path, and (with global keys) other tenants. Delete rows only." | blocker | Closed structurally: no ref resolves through `blobs/<oid>` — the key is scratch like `pending/` (2.2) and the copied bytes live in a pack. `BlobSweep` deletes keys only for claims older than TTL + GRACE and deletes rows after keys; keys are per repo (`r/<repo_id>/blobs/`), so the cross-tenant case cannot exist. Worst case is a failed push, never a dangling ref. |
| "`resolveBlobs()` must re-check row state after each `await` and run before the synchronous ref CAS transaction, not inside it." | blocker | There is no resolve-in-transaction. `copy_blob` runs in the edge before `/_do/push/commit`; `commit_push` remains one sync span (its only change is looping `pack_ids` in step 3). Nothing needs re-checking after the awaits because the row is never trusted — the bytes are rehashed. |
| "Prove on a real bucket that a presigned PUT whose body does not hash to the signed `x-amz-checksum-sha1` returns 400 and that `head().checksums.sha1` is populated; otherwise the inline `rehash()` on a 4 GB blob happens inside the push request." | blocker | Not closable from documents: kept as the day-1 gate in Known limits. But the design no longer depends on it — the rehash is folded into the copy that must read the bytes anyway, so an ignored checksum means a wrong body fails the push inside `copy_blob`, never a `rehash()` pass and never a stored bad object. |
| "`git-remote-edge` is the bulk of the effort and is unshown; without it the feature has zero users." | caveat | Not addressed: the client is still bespoke (it must drive `POST git-receive-pack` itself — `connect` would make git run its own `send-pack` and ship the blobs). Recorded in Known limits. The server surface is exactly `blobs/batch` plus ordinary receive-pack. |
| "Filter gitlinks; page `blobs/batch`; enforce count/size quotas at batch time since PUT bytes bypass the Worker." | caveat | `blobs_batch` caps at 1,000 objects and 5 GiB each (`Limit` -> 413); the missing set is paged at 1,000 ids per `/_do/blobs/claims`. Gitlinks: `extract_links` must skip mode-160000 tree entries — flagged as a write-back; the same gap exists in the foundation today, so this is a dependency, not a new hole. |
| "Multipart (>5 GiB) has no R2-side checksum; every such object costs a full read-back hash in an alarm." | caveat | Single-PUT only: > 5 GiB is `Limit`. The presigned `CreateMultipartUpload`/`UploadPart` flow is not built (Known limits). The read-back hash is now inside the mandatory copy, not an alarm pass. |
| "Key scheme must be reconciled with `content-addressed-r2-keys` (per-repo prefix) before shipping." | caveat | `r/<repo_id>/blobs/<oid>` obeys the same per-repo rule as 2.2; cross-repo dedup stays out of scope (section 12). |
| "`size` is inconsistent: batch stores git content size, promotion stores `head.size`" | caveat | `size` is always git content bytes: `blob_claims.size`, the staged `blob <n>\0` field and `ObjRow.size` agree; `head().size()` is compared against `size + header len`, never stored. |
| "Presigned PUT must carry `x-amz-checksum-sha1` byte-identical to the signed value ... `Content-Length` is unsigned and free." | caveat | The checksum is the only signed extra header (`headers` map in the batch reply echoes it); content-length stays unsigned and the declared size is enforced at copy time. |
| "Tree entries of mode `160000` ... would be sent to `head()` and fail the push unless filtered." | caveat (body) | Gitlinks land in `missing` only if `extract_links` emits them; the write-back makes it skip them, as upstream connectivity does. Until then they fail as `unpack error missing object`, same as an ordinary push of a submodule repo does today. |

## Known limits

- **The client is still bespoke.** `git-remote-edge` must batch, PUT, then speak receive-pack itself (advertisement, command pkt-lines, `pack-objects --revs --thin --stdout --filter=blob:limit`, report-status parsing). That is weeks of work, unshown, and without it the feature has no users — unchanged from the first pass.
- **Day-1 gate on real R2:** a presigned PUT whose body does not hash to the signed `x-amz-checksum-sha1` must be rejected, and a matching PUT must leave `head().checksums.sha1` populated. The local simulator has no S3 presign endpoint, so this needs `wrangler dev --remote` or a deployed Worker. If enforcement is absent the copy-time rehash still guarantees integrity; the only loss is discovering a wrong body after a full read instead of at PUT. R2 credentials (`R2_ACCOUNT_ID`, `R2_BUCKET_NAME`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`) are the same four bindings native-lfs adds.
- **Single PUT only:** 5 GiB per object; the presigned multipart flow is not built. `ObjRow.len` is `u32`, so an entry whose compressed bytes pass 4 GiB fails `end_entry` with `Limit` — the effective cap is ~4 GiB, not 5.
- **A7 write-back consequences.** Giant entries at rest mean `GcConsolidate` cannot copy them with `read_entries` (whole-object reads); it must stream entries above a threshold window-wise — flagged, not shown. `write_pack` already copies window-wise (section 9 step 6). A thin push that deltas against a giant staged blob still hits the 32 MiB external-base cap (2.4) — such a base cannot be applied and the client must send a full object.
- **CPU:** deflating multi-GB inside a 240 s `max_ms`; deflate throughput on workerd is unmeasured (content-addressed-r2-keys). A stored-block zlib stream (deflate level 0) is a valid pack entry and removes the cost — the `Compression` variant name is unverified.
- **Quotas:** PUT bytes bypass the Worker, so admission control is `MAX_BATCH` and `MAX_BLOB` per call; per-repo byte quotas are out of scope (section 12).
- **Lease race residue:** a request that reads a claim in the last ~4 min before its 2 h expiry can still be mid-copy when `BlobSweep` fires; the `get` 404s, the push fails `unpack`, and a retry re-claims. Fails closed.
- Subrequests and CPU are not enforced by local workerd (#7); `ReqBudget`/`SliceBudget` counters are the only guard until the deployed measurement.
- Scenarios this proof must pass (section 11): 7, 9, 14 (ordinary pushes are unaffected). Added (two): **(a)** "staged push" — not producible by stock git, so the harness drives it: POST `blobs/batch` for a 20 MiB blob, PUT to the returned URL with the checksum header, POST a hand-built receive-pack body whose pack came from `git pack-objects --revs --stdout --filter=blob:limit=1m`; assert `ok`, the blob's `objects` row points into the second pack, the loose key is gone after the clock passes CLAIM_MS and the alarm fires, and a fresh clone reproduces the blob byte-exact. Needs a real bucket. **(b)** "expired claim": batch, never PUT, advance the fake clock past CLAIM_MS, fire the alarm — row gone, no key; a push referencing the oid then gets `unpack error missing object <oid>` with `ng` per ref.

## Depends on

- two-phase-push
- streaming-pack-parser
- repo-do-ref-authority
- refs-sqlite-objects-r2
- native-lfs (the `sigv4` signer and the R2 credential bindings)
- gc-and-repack-alarm
