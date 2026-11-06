# Two-phase push

> Second pass · Idea #6 · verdict: **lands with caveats** · feasibility 4/5 · reliability 4/5 · correctness 4/5 (first pass 4/3/3)
> First pass: [proof](./archived/two-phase-push.md) · [review](../reviews/archived/two-phase-push.md) · Second pass: [review](../reviews/two-phase-push.md) · Contract: [CONTRACTS.md](../../CONTRACTS.md)


## Mechanism
This idea is subsumed by CONTRACTS.md: phase one is `pack::ingest` running in the edge Worker (section 2.4: `pending/<push>.pack`, then the normalized `packs/<pack>.pack`, rows to `/_do/push/index`), phase two is `RepoDo::commit_push` on `/_do/push/commit` (section 3, proved in `repo-do-ref-authority`), and the sweeper is the `Janitor` job kind of the `jobs` dispatcher (sections 4, 5). What was still unwritten, and is written here, is the glue that makes it two-phase: `pack::ingest::run`, which sequences `stream_to_pending`, the thin-pack base lookup, `resolve_and_normalize`, `PackWriter::finish`, the index posts and the section 2.5 connectivity lookups in the order section 3 demands; the two sync DO routes it talks to, `push_lookup` and `push_index`; and the Janitor slice that expires `open` pushes, kills their `ingesting` packs, and deletes R2 keys only for rows marked at least `GRACE` earlier. Tables touched: `pushes`, `packs`, `objects`, `meta` (`gc_epoch`, `repo_id`), `jobs`; keys: `keys::pending`, `keys::pack`.

## Primitives
- `Stub::fetch_with_request` for every edge-to-DO call (`/_do/push/{begin,lookup,index,commit}`): verified (memo section 1, spike). Typed DO RPC: experimental, not used.
- `worker::Request::new_with_init` + `RequestInit::{with_method, with_body}` to build the JSON stub request: present in `worker` 0.8.5 source; the spike forwarded the client request instead, so this exact constructor path is **unverified at runtime**.
- `State::storage().sql().exec` synchronous, `SqlCursor::{one, to_array}`, `SELECT changes()` as the only write oracle: verified (spike), measured 1 / 0 / 1 (platform-facts #1). `rows_written` is never read.
- Sync-span atomicity for `push_lookup`, `push_index`, Janitor steps 1-2 and the post-delete row removal: measured (platform-facts #4).
- `gix_pack::data::header::decode(&[u8; 12]) -> (Version, u32)` for the 0-object-pack branch: verified in 0.74.2 source (sibling `streaming-pack-parser`); `gix_pack::data::entry::Header::RefDelta { base_id }` for the base-id scan: verified in source, measured by the spike.
- `gix_hash::hasher(Sha1)` / `Hasher::{update, try_finalize}`: measured on workerd (spike).
- `store::Bucket::delete(&[String])` (section 1.2): whether `worker` 0.8.5's `Bucket::delete` binds the JS multi-key form is **unverified**; the fallback is one call per key, which turns the 400-key slice cap into 300 (80 % of the 400-subrequest slice budget, section 4.3).
- R2 multipart through `PackWriter::{create, finish, abort}`: API verified in source, behaviour measured on the local simulator only (platform-facts #6). Automatic abort of an incomplete multipart upload by an R2 lifecycle rule: **unverified**, see Known limits.
- `PushId::random()` / `PackId::random()` from 16 random bytes: `web_sys::Crypto` binding path **unverified** (section 8.2).
- `js_sys::Date::now()` for `began_at`, `ended_at`, `dead_at`, `created_at`: standard `js-sys`.
- Alarm: this module only returns `SliceOutcome`; `jobs::rearm` is the sole `set_alarm` caller (section 4.1). Second `setAlarm` cancels the first: measured (#5).
- SQLite `INSERT ... ON CONFLICT(id) DO UPDATE ... WHERE`: the same upsert syntax family the contract already relies on in section 3 step 4 (`ON CONFLICT DO NOTHING`, SQLite >= 3.24).

## Proof code
```rust
// src/pack/ingest/run.rs (edge) + src/repo_do/push.rs (DO) + src/jobs/janitor.rs (DO). CONTRACTS.md 1.2-1.4, 2.2-2.5, 3, 4, 5, 7.
// worker 0.8.5, gix-pack 0.74.2, gix-hash 0.26.2. `q`, `changes`, `oid`, `json`, `now_ms`, `bucket` are the RepoDo helpers of the
// repo-do-ref-authority proof; `impl From<worker::Error> for Error` maps to Error::Storage.
use std::collections::HashMap;
use gix_hash::{Kind as H, ObjectId};
use gix_pack::data::entry::Header;
use worker::{Method, Request, RequestInit, SqlStorageValue as V, Stub};
use crate::{edge::{BodyReader, RepoRoute}, error::Error, jobs::{Job, SliceBudget, SliceOutcome}, repo_do::RepoDo,
            store::{keys, Bucket, Index, ObjLoc, ObjRow, PackId, PackMeta, PackWriter, PushId}, ReqBudget};
const GRACE_MS: i64 = 3_600_000; const PUSH_TIMEOUT_MS: i64 = 3_600_000; const KEYS_PER_SLICE: usize = 400;   // section 5
const MAX_LINKS: usize = 1_000_000;                                            // 20 MiB of ObjectId in the edge, see Known limits
fn unpack(m: impl Into<String>) -> Error { Error::Unpack(m.into()) }

/// Phase one. Returns the pack id that commit_push step 3 flips to live, or None when the push carried no objects (2.4).
/// On Ok: pending/ and packs/ are durable, every ObjRow and the packs row ('ingesting') are in the DO, every link out of
/// the pack resolves (2.5). Nothing is visible to readers until the caller's /_do/push/commit (section 3 ordering 1-2-3).
pub async fn run(body: &mut BodyReader, bucket: &Bucket, stub: &Stub, repo: &RepoRoute, push: &PushId, budget: &mut ReqBudget)
    -> Result<Option<PackId>, Error> {
    if !body.fill(12).await? {                                                 // the caller pushed PktReader::remainder() back first
        return if body.buffered().is_empty() { Ok(None) } else { Err(unpack("pack header truncated")) };   // delete-only: no PACK
    }
    let head: [u8; 12] = body.buffered().get(..12).and_then(|s| s.try_into().ok()).ok_or_else(|| unpack("pack header"))?;
    let (_, count) = gix_pack::data::header::decode(&head).map_err(|e| unpack(e.to_string()))?;
    if count == 0 {                                                            // new ref at an existing commit: header + trailer only
        if !body.fill(32).await? || body.fill(33).await? { return Err(unpack("bad empty pack")); }
        let mut h = gix_hash::hasher(H::Sha1); h.update(&head);
        let want = h.try_finalize().map_err(|_| unpack("sha1 collision"))?;
        if body.buffered().get(12..32) != Some(want.as_slice()) { return Err(unpack("bad pack checksum")); }
        return Ok(None);
    }
    let (entries, _) = super::stream_to_pending(body, bucket, push, budget).await?;      // pass A (2.4): pending/<push>.pack durable
    let mut bases: Vec<ObjectId> = entries.iter()                              // thin-pack bases: whatever is live now (2.4);
        .filter_map(|r| match r.kind_or_delta { Header::RefDelta { base_id } => Some(base_id), _ => None }).collect();
    bases.sort_unstable(); bases.dedup();                                      // misses may still be in-pack; pass B decides
    let mut external: HashMap<ObjectId, ObjLoc> = HashMap::new();
    for chunk in bases.chunks(1_000) { external.extend(lookup(stub, repo, chunk, None, budget).await?.into_iter().filter_map(|(id, l)| Some((id, l?)))); }
    let pack = PackId::random();
    let mut sink = IndexSink { stub, repo, pack: pack.clone(), push: push.clone(), links: Vec::new() };
    sink.post(&PackMeta::EMPTY, &[], budget).await?;                          // packs row 'ingesting' exists before any part is uploaded
    let mut out = PackWriter::create(bucket, keys::pack(&bucket.repo, &pack), budget).await?;
    let pending = keys::pending(&bucket.repo, push);
    let tail = match super::resolve_and_normalize(bucket, &pending, &entries, &external, &mut out, &mut sink, budget).await {
        Ok(rows) => rows, Err(e) => { out.abort().await; return Err(e); }     // no completed object is ever left without a row
    };
    drop(entries); drop(external);
    let meta = out.finish(budget).await?;                                      // section 3 (1): pack durable in R2
    sink.post(&meta, &tail, budget).await?;                                    // section 3 (2): tail rows + real count/bytes/commit_lo/hi
    let mut links = std::mem::take(&mut sink.links); links.sort_unstable(); links.dedup();
    for chunk in links.chunks(1_000) {                                         // 2.5: links - {this pack} must be live; the DO subtracts
        if let Some(id) = lookup(stub, repo, chunk, Some(&pack), budget).await?.into_iter().find_map(|(id, l)| l.is_none().then_some(id)) {
            return Err(unpack(format!("missing object {id}")));                // -> HTTP 200 `unpack error ...` + ng for every ref (10)
        }
    }
    Ok(Some(pack))                                                             // section 3 (3) is the caller's /_do/push/commit
}

pub struct IndexSink<'a> { stub: &'a Stub, repo: &'a RepoRoute, pack: PackId, push: PushId, pub links: Vec<ObjectId> }
impl IndexSink<'_> {
    /// One /_do/push/index call (<= 10,000 rows, 1.3). resolve_and_normalize calls it every 10,000 rows and extends `links`.
    pub async fn post(&mut self, meta: &PackMeta, rows: &[ObjRow], budget: &mut ReqBudget) -> Result<(), Error> {
        if self.links.len() > MAX_LINKS { return Err(Error::Limit("push references too many objects (1,000,000 max)".into())); }
        let body = serde_json::json!({ "pack": { "id": self.pack, "push_id": self.push, "count": meta.count, "bytes": meta.bytes,
                                                 "commit_lo": meta.commit_lo, "commit_hi": meta.commit_hi }, "rows": rows });
        let _: serde_json::Value = stub_json(self.stub, self.repo, "/_do/push/index", &body, budget).await?;
        Ok(())
    }
}
#[derive(serde::Deserialize)] struct LookupResponse { locs: Vec<Option<ObjLoc>> }
/// /_do/push/lookup for <= 1,000 ids (1.3, 7.3). `pack` = rows of this push's own ingesting pack also count (2.5 subtraction).
async fn lookup(stub: &Stub, repo: &RepoRoute, ids: &[ObjectId], pack: Option<&PackId>, budget: &mut ReqBudget)
    -> Result<Vec<(ObjectId, Option<ObjLoc>)>, Error> {
    let hex: Vec<String> = ids.iter().map(ToString::to_string).collect();
    let res: LookupResponse = stub_json(stub, repo, "/_do/push/lookup", &serde_json::json!({ "ids": hex, "pack": pack }), budget).await?;
    if res.locs.len() != ids.len() { return Err(Error::Internal("lookup length".into())); }
    Ok(ids.iter().copied().zip(res.locs).collect())
}
/// One stub round-trip, charged first (7.1), x-ge-owner/x-ge-repo set (8.1). A non-200 carries the DO's Error (section 10).
async fn stub_json<T: serde::de::DeserializeOwned>(stub: &Stub, repo: &RepoRoute, path: &str, body: &impl serde::Serialize,
    budget: &mut ReqBudget) -> Result<T, Error> {
    budget.charge(1)?;
    let text = serde_json::to_string(body).map_err(|e| Error::Internal(e.to_string()))?;
    let mut req = Request::new_with_init(&format!("https://do{path}"), RequestInit::new().with_method(Method::Post).with_body(Some(text.into())))?;
    repo.apply_headers(&mut req)?;                                             // RequestInit path unverified at runtime, see Primitives
    let mut resp = stub.fetch_with_request(req).await?;
    if resp.status_code() != 200 { return Err(Error::from_do_response(resp).await); }
    resp.json::<T>().await.map_err(|e| Error::Internal(format!("do response: {e}")))
}

// ---- src/repo_do/push.rs: both routes are "Awaits inside: none" (1.3), one sync span each. ----
#[derive(serde::Deserialize)] pub struct LookupDto { ids: Vec<String>, pack: Option<String> }
#[derive(serde::Deserialize)] pub struct IndexDto { pack: PackMetaDto, rows: Vec<ObjRow> }
#[derive(serde::Deserialize)] struct PackMetaDto { id: String, push_id: String, count: i64, bytes: i64, commit_lo: i64, commit_hi: i64 }
#[derive(serde::Deserialize)] struct StateRow { state: String }
impl RepoDo {
    pub fn push_lookup(&self, b: &LookupDto) -> Result<worker::Response, Error> {
        if b.ids.len() > 1_000 { return Err(Error::Internal("lookup > 1000 ids".into())); }
        let ids = b.ids.iter().map(|h| oid(h)).collect::<Result<Vec<_>, Error>>()?;
        let sql = self.sql(); let idx = Index(&sql);
        let mut locs = idx.lookup(&ids)?;                                       // the 2.3 reader query: live packs only
        if let Some(p) = &b.pack {                                             // presence in the caller's own ingesting pack (2.5)
            for (id, loc) in ids.iter().zip(locs.iter_mut()) { if loc.is_none() { *loc = idx.lookup_in_pack(id, &PackId(p.clone()))?; } }
        }
        json(serde_json::json!({ "locs": locs }))
    }
    /// Upsert the packs row (state 'ingesting') and insert <= 10,000 rows. Refused once the push is not 'open', so a push the
    /// Janitor expired (5.1) can never add rows after 5.2 killed its pack, and commit (3 step 1) will refuse it too.
    pub fn push_index(&self, b: &IndexDto) -> Result<worker::Response, Error> {
        if b.rows.len() > 10_000 { return Err(Error::Internal("index > 10000 rows".into())); }
        let m = &b.pack;
        let st = self.q("SELECT state FROM pushes WHERE id=?", vec![V::from(m.push_id.as_str())])?.to_array::<StateRow>()?.into_iter().next();
        if st.map(|s| s.state).as_deref() != Some("open") { return Err(Error::Conflict("push not open".into())); }
        self.q("INSERT INTO packs(id,state,count,bytes,commit_lo,commit_hi,push_id,created_at) VALUES(?,'ingesting',?,?,?,?,?,?) \
                ON CONFLICT(id) DO UPDATE SET count=excluded.count, bytes=excluded.bytes, commit_lo=excluded.commit_lo, \
                commit_hi=excluded.commit_hi WHERE packs.state='ingesting' AND packs.push_id=excluded.push_id",
               vec![V::from(m.id.as_str()), V::from(m.count), V::from(m.bytes), V::from(m.commit_lo), V::from(m.commit_hi),
                    V::from(m.push_id.as_str()), V::from(now_ms())])?;
        if self.changes()? != 1 { return Err(Error::Conflict("pack not ingesting for this push".into())); }
        Index(&self.sql()).insert_objects(&PackId(m.id.clone()), &b.rows)?;    // INSERT ... ON CONFLICT DO NOTHING, per row
        json(serde_json::json!({}))
    }
}

// ---- src/jobs/janitor.rs: section 5 Janitor, one slice per alarm firing (4.2). Never calls set_alarm (4.1). ----
#[derive(serde::Deserialize)] struct IdRow { id: String }
pub async fn run_slice(d: &RepoDo, _job: &Job, budget: &mut SliceBudget) -> Result<SliceOutcome, Error> {
    let now = now_ms();
    // 5.1 + 5.2 in one sync span: expire stale open pushes, then kill the ingesting packs of pushes that can never commit.
    d.q("UPDATE pushes SET state='expired', ended_at=? WHERE state='open' AND began_at < ?", vec![V::from(now), V::from(now.saturating_sub(PUSH_TIMEOUT_MS))])?;
    let dead: Vec<IdRow> = d.q("SELECT p.id FROM packs p JOIN pushes u ON u.id=p.push_id WHERE p.state='ingesting' AND u.state IN ('expired','rejected')", vec![])?.to_array()?;
    for r in &dead {
        d.q("DELETE FROM objects WHERE pack_id=?", vec![V::from(r.id.as_str())])?;
        d.q("UPDATE packs SET state='dead', dead_at=? WHERE id=? AND state='ingesting'", vec![V::from(now), V::from(r.id.as_str())])?;
    }
    // 5.3: only rows that an earlier slice, >= GRACE ago, marked. GRACE (1 h) > max_ms (240 s, 7.1): no request can still hold them.
    let cutoff = now.saturating_sub(GRACE_MS);
    let pushes: Vec<IdRow> = d.q("SELECT id FROM pushes WHERE state!='open' AND ended_at < ? ORDER BY ended_at LIMIT ?",
                                 vec![V::from(cutoff), V::from(KEYS_PER_SLICE as i64)])?.to_array()?;
    let packs: Vec<IdRow> = d.q("SELECT id FROM packs WHERE state='dead' AND dead_at < ? ORDER BY dead_at LIMIT ?",
                                vec![V::from(cutoff), V::from(KEYS_PER_SLICE.saturating_sub(pushes.len()) as i64)])?.to_array()?;
    if pushes.is_empty() && packs.is_empty() { return Ok(SliceOutcome::Done); }   // dispatcher re-enqueues Janitor at now + 15 min (4.2)
    let bucket = d.bucket()?;                                                  // repo_id from meta (8), never ctx.id.name
    let keys: Vec<String> = pushes.iter().map(|r| keys::pending(&bucket.repo, &PushId(r.id.clone())))
        .chain(packs.iter().map(|r| keys::pack(&bucket.repo, &PackId(r.id.clone())))).collect();
    budget.subrequests_used = budget.subrequests_used.saturating_add(1);       // <= 400 keys: one delete call (5.3)
    bucket.delete(&keys).await?;                                               // a key that was never written is a no-op delete
    // The await opened the input gate, but 'dead' and 'not open' are one-way states older than GRACE: a fresh sync span suffices.
    for r in &packs { d.q("DELETE FROM packs WHERE id=? AND state='dead'", vec![V::from(r.id.as_str())])?; }
    for r in &pushes { d.q("DELETE FROM pushes WHERE id=? AND state!='open'", vec![V::from(r.id.as_str())])?; }   // each key deleted once
    Ok(if keys.len() >= KEYS_PER_SLICE { SliceOutcome::Continue { cursor: String::new() } } else { SliceOutcome::Done })
}
```

## Why it works
- **Phase boundary is git's own.** `receive-pack` reads commands, then a pack, then answers per ref; the server may take as long as it likes between the pack's last byte and `report-status`. `run` consumes exactly the PACK left after `wire::parse_receive_header` (section 1.1, 6.3) and returns before any ref moves; the caller's `/_do/push/commit` is git's `ref_transaction_commit` (section 3). The three cases git sends are covered in the first ten lines: no PACK after the flush (delete-only), a 12 + 20 byte 0-object pack (new ref at an existing commit), or a real pack; the first two yield `pack_id = None` so step 3 of section 3 is skipped (2.4 last paragraph).
- **Ordering 1-2-3 is enforced by code shape, not convention.** `out.finish()` (pack durable) precedes the final `sink.post` (rows + real meta) which precedes returning `Some(pack)`; only then does `receive_pack` send commit. The packs row is created `ingesting` before the first part is uploaded, so at no instant does a completed `packs/<pack>.pack` object exist without a row the Janitor can find (5.2). `Index::lookup` joins on `state='live'` (2.3), which only `commit_push` step 3 sets; an `ingesting` pack is invisible to every reader and to `GcMark`.
- **Connectivity by induction (2.5).** Every link `extract_links` produced during pass B is looked up after the pack is fully indexed, with `pack` set so the DO counts the push's own rows; a miss is `unpack error missing object <oid>`, and since live packs are closed under references, no deep walk is needed. Tips are re-checked per ref in `commit_push` step 4 (`apply_one`, sibling proof) after step 3 has made the pack live, so a bad tip costs one `ng`, not the push.
- **The sweep race of the first pass cannot recur.** There is no shared key: `pending/<push>.pack` is per push and `packs/<pack>.pack` is per fresh `PackId`; a concurrent push never writes a key the Janitor is about to delete. The Janitor deletes only keys whose rows were marked `dead` / not `open` at least `GRACE = 1 h` earlier (5.3), longer than `max_ms = 240 s` (7.1), and marking and deleting never share a slice; readers never resolve through R2 key presence, only through the 2.3 query. A sweep between the connectivity lookup and commit is caught by `gc_epoch` (section 3 step 2, section 5 "why the two review races are now impossible").
- **Expiry is atomic with every mutation.** `push_index` and `commit_push` read `pushes.state` in the same sync span as their writes; the Janitor's `UPDATE ... state='expired'` is itself a sync statement. A push older than `PUSH_TIMEOUT` therefore either gets `Conflict` on its next DO call or was fully committed before expiry; there is no window in which it is both expired and adding rows, so 5.2 kills only packs that can never become live.
- **Budget (7.3).** A push of N objects, P pack bytes, B thin bases: `begin` + `2·P/8 MiB` parts and windows + `B/1,000` + `1 + N/10,000` index posts + `N/1,000` link lookups + `commit`, each through `budget.charge(1)`; 1,000,000 objects and 4 GB of pack fit in 9,000 subrequests. The Janitor slice makes one `delete` call for at most 400 keys and returns `Continue` when a backlog remains, so `dispatch` gets the very next firing (4.2).
- **Error policy (section 10).** Every client-caused failure in `run` is `Error::Unpack` (or `Limit` for the 1,000,000-link cap), which `receive_pack` reports as HTTP 200, `unpack <msg>`, `ng <ref> unpack failed` for every command; the sibling's `Err(Error::Unpack(msg))` arm must also catch `Limit` and `Budget`, as section 10 requires after the header is parsed. Nothing in this code indexes or unwraps client bytes; `get(..12)`, `try_into`, `?` on every gitoxide result.
- **Identity and background rules.** Keys use `bucket.repo` = `meta.repo_id` (section 8); the Janitor only returns `SliceOutcome` and `jobs::rearm` arms the single alarm (4.1); Janitor re-enqueues at `now + 15 min` through the dispatcher, not here.

## Changes from the first pass
| First-pass finding (quoted) | Kind | How addressed |
|---|---|---|
| "Janitor race: alarm computes its orphan set, awaits R2 ... a new push begins and PUTs the same sha, janitor deletes it after the PUT ... silent data loss" | blocker | Closed structurally. No content-addressed key exists (2.2): per-push `pending/` and per-pack `packs/` keys cannot be shared with a newer push. Janitor 5.3 block: deletion targets only rows `dead` / not `open` for >= GRACE, marked by an earlier slice; readers resolve through SQLite `state='live'` only. `gc_epoch` (3 step 2) guards lookup-to-commit. |
| "commit() returns ok <ref> for refs it did not move whenever any sibling ref fails CAS" | blocker | Closed in `commit_push` step 4 (sibling proof `repo-do-ref-authority`): each ref is its own CAS with `SELECT changes()`, results are per ref in client order; `run` never decides ref outcomes and `receive_pack` reports exactly the DO's list. `atomic` is not advertised (1.1 rule 5). |
| "report-status is emitted without sideband ... small pushes arrive with Content-Encoding: gzip which the Worker never decodes" | blocker | Closed by contract modules, not this code: `wire::write_report_status` frames in band 1 under `side-band-64k` (1.1 rule 4); `BodyReader::new` pipes gzip through `DecompressionStream` (6.1). `run` receives the `BodyReader` after the header. The `wasm-streams` binding path is unverified (6.1). |
| "Crash before the manifest is written leaks objects permanently; 'janitor never lists the bucket'" | caveat | There is no manifest. `push_begin` writes the `pushes` row before any R2 byte; both keys are derived from ids the DO knows; the packs row is posted before the first part upload (`sink.post(&PackMeta::EMPTY, ..)`); `out.abort()` runs on any pass-B error; the Janitor deletes `pending/<push>` for every non-open push after GRACE whether or not the key was ever written (delete of a missing key is a no-op). Residual: an isolate kill mid-multipart leaves an incomplete upload, see Known limits. |
| "commit() checks push expiry before an R2 await instead of inside transactionSync" | caveat | `commit_push` reads `pushes.state` in its sync span (3 step 1); `push_index` does the same before writing rows; Janitor 5.1 expiry is one sync `UPDATE`. No await separates check from write anywhere. |
| "Connectivity failure throws out of the DO (HTTP 500) instead of an 'unpack error' pkt-line" | caveat | `run` returns `Error::Unpack("missing object <oid>")` from the edge (line after the `links.chunks` loop); the DO never throws for it. Section 10 maps it to 200 + `unpack error ...` + `ng` per ref. |
| "Whole-object crypto.subtle.digest plus recompression means blobs near 100MB exceed the 128MB heap; DecompressionStream cannot report consumed bytes" | caveat | Closed by `pack::ingest` (sibling `streaming-pack-parser`): incremental `gix_hash::Hasher`, `gix_zlib::Decompress::total_in` boundaries, 8 MiB windows, 32 MiB single-object cap (2.4). `run` only adds the 0-object-pack trailer check. |
| "Delete-only pushes send no PACK; parser must accept EOF after the flush. Push is v0/v1 regardless of the v2-only sibling" | caveat | First ten lines of `run`: empty buffer after the flush -> `Ok(None)`; 0-object pack verified and -> `Ok(None)`. receive-pack is v0 by contract (1.1 rules 5, 8). Scenario 4. |
| "One R2 PUT per object and a single JSON manifest cap practical push size around 100k objects" | caveat | Two multipart uploads per push (pending, normalized) regardless of object count; rows in 10,000-row posts; links in 1,000-id lookups; cost formula in Why it works. New cap: 1,000,000 unique links (`MAX_LINKS`), which is about 1,000,000 objects, matching 7.3. |
| First-pass limit: "Objects are not literally written under a pending prefix" | limit | Now literal: `pending/<push>.pack` is scratch, `packs/<pack>.pack` is the durable artifact (2.2). |
| First-pass limit: "Per-link SELECT in SQLite: a million-link commit spends about a second of DO CPU in one transaction" | limit | Lookups are batched 1,000 per sync span (`push_lookup`), and other DO events run between batches; the commit span itself does only the per-ref tip lookups. |
| First-pass limit: "Fixed 15-minute push timeout; longer legitimate pushes are rejected at commit" | limit | `PUSH_TIMEOUT = 1 h` and `GRACE = 1 h` (section 5); a slow push still fails cleanly (`Conflict`) rather than racing a sweep, and its keys are only removed an hour after expiry. |
| First-pass limit: "Connectivity trusts Worker-extracted links" | limit | Still true and accepted by 2.5; mitigated because ids are recomputed from resolved bytes (`compute_hash` in pass B), so a corrupt body cannot masquerade as a known object. |
| First-pass limit: "30s default CPU cap is exceeded by monorepo initial pushes" | limit | `limits.cpu_ms = 300000`, `ReqBudget.max_ms = 240 s` (section 7); not measured on a deployed Worker. |

## Known limits
- Memory: `links` adds 20 B x <= 1,000,000 = 20 MiB in the edge on top of the 2.4 budget, held through pass B; `bases` is freed before pass B. The joint bound (48 MB `EntryRec` + 48 MiB cache + windows + parts + links) is not enforced and, as the sibling proof admits, exceeds 128 MB in the worst case; day-1 test pushes 1,000,000 small objects and measures.
- `MAX_LINKS` makes the practical object cap about 1,000,000 per push (every object is linked about once), below the 2,000,000 `EntryRec` cap of 2.4; a larger push fails with `unpack error push references too many objects`.
- Subrequests are budgeted (`ReqBudget`, `SliceBudget`) and not measured (platform-facts #7). If `worker` 0.8.5's `Bucket::delete` is single-key only, `KEYS_PER_SLICE` becomes 300.
- Write-backs proposed to CONTRACTS.md, each small: `/_do/push/lookup` body gains optional `pack` and `Index::lookup_in_pack(id, pack)` (presence in the caller's own ingesting pack, 2.5); `/_do/push/index` upserts the `packs` row (meta is only final after `finish`), so `Index::insert_pack` becomes the upsert above; `PackMeta::EMPTY`; `commit_lo` "none" sentinel is `i64::MAX` because SQLite INTEGER cannot hold `u64::MAX`; the Janitor deletes a `pushes` row after deleting its pending key so each key is deleted exactly once (the contract is silent; the alternative is a `swept_at` column if `pushes.result` must outlive GRACE — the reflog keeps every ok move either way); `receive_pack` maps `Limit`/`Budget` as well as `Unpack` to the 200 report.
- An isolate kill (CPU limit, crash) between `PackWriter::create` and `finish`/`abort` leaves an incomplete multipart upload that is not an object and that no row references; cleanup relies on an R2 lifecycle rule for incomplete multipart uploads (unverified; set on bucket creation and checked on first deploy). The same applies to the pending writer in pass A.
- `Request::new_with_init` + `RequestInit` for the JSON stub body and `Error::from_do_response` are unverified at runtime; the spike only forwarded the client request.
- CPU and 128 MB behaviour on a deployed Worker, real-R2 multipart (5 MiB minimum, equal parts), cold start of the full crate: not measured (platform-facts "still open").
- Scenarios this proof must pass: 2, 4, 6, 7, 9, 14. Added (two): (a) "crash mid-ingest": kill the edge after `stream_to_pending` returned and the provisional index post landed; advance the fake clock past `PUSH_TIMEOUT`, fire the alarm: push `expired`, pack `dead`, `objects` rows gone; past GRACE, fire again: both keys absent, both rows deleted, `ls-remote` unchanged. (b) "connectivity hole" (not producible by stock git): the harness posts a hand-built receive-pack body whose tree names a blob absent from pack and repo; expects `unpack error missing object <oid>`, `ng` for every ref, no `live` pack, and the pack row `dead` after the next Janitor run.

## Depends on
- repo-do-ref-authority
- streaming-pack-parser
- refs-sqlite-objects-r2
- gc-and-repack-alarm
- info-refs-endpoint
