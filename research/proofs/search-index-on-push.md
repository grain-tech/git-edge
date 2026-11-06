# Search index built on push (D1 FTS / Vectorize)

> Second pass · Idea #26 · verdict: **lands with caveats** · feasibility 4/5 · reliability 3/5 · correctness 4/5 (first pass 4/3/3)
> First pass: [proof](./archived/search-index-on-push.md) · [review](../reviews/archived/search-index-on-push.md) · Second pass: [review](../reviews/search-index-on-push.md) · Contract: [CONTRACTS.md](../../CONTRACTS.md)


## Mechanism
Section 12 does not name a search index, so this is a post-foundation module (amendment A9) that keeps every foundation rule: the push path itself is untouched except for one line — `commit_push` step 7 gains `jobs::enqueue(JobKind::IndexPush, now)` beside the existing `GcMark` enqueue (section 3) — and all indexing runs as slices of the section 4 dispatcher, never inside a request. The first pass's premise is replaced: there are no loose objects. Every object at rest is a full entry of a normalized pack (2.1), so the job resolves shas with `Index::lookup` (2.3, live packs only), reads the entries with `Bucket::read_entries` (coalesced, 7.2, A1) and inflates them with `codec::decode_entry`; no delta resolution at index time, no `objects/xx/yyyy` keys, no second object index. The job keeps one `meta` row, `search.tip` = the last fully indexed `meta.head` target, and walks the tree diff between it and the current head as a section-9 round loop whose resumable state is two tables — `search_frontier` (path prefix, old tree oid, new tree oid) and `search_todo` (path, blob sha) — the `gc_frontier` pattern of 5.1, so the job cursor stays a few dozen bytes and a slice returns `Continue` at 80 % of its budget (A4) and resumes exactly where it stopped. Changed blobs are read in 64-entry coalesced batches (A4), binary-sniffed (a NUL in the first 8,000 bytes, git's own heuristic) and capped at 512 KiB; every discovered path gets a row in `blobs_fts` (FTS5, `unicode61`), an empty body when the blob is binary, oversized or undecodable, so a corrupt object degrades to a path-only row instead of poisoning the queue (4.4). Deletes are the diff's own: a vanished file is a point `DELETE`, a vanished directory one range `DELETE` over the path prefix. The read side is a new sync DO route `POST /_do/search` (1.3, "Awaits inside: none") behind edge `GET /:owner/:repo/search?q=`: the query is rewritten into quoted FTS5 phrases so no user byte reaches `MATCH` as syntax, hits come back as `snippet`/`bm25` rows, and the `indexed` versus `head` tips in the response are the lag signal; a lagging index re-enqueues the job (dedup, 4.5). The index covers the default branch only, which is the review's own suggested fix for branch conflation. The Vectorize / Workers-AI stage is dropped: it was a sketch, neither binding is in the memo or the contract, and it remains a later job kind over the same `(path, blob_sha)` rows.

## Primitives
- FTS5 in DO SQLite — `CREATE VIRTUAL TABLE ... USING fts5(path, blob_sha UNINDEXED, body, tokenize='unicode61')`, `MATCH`, `bm25()`, `snippet()`, `UNINDEXED` columns, `DELETE` with `WHERE` on a column, `DELETE` without `WHERE`: documented on Cloudflare's DO SQLite / D1 extension list per the first-pass review; **not exercised by the spike or platform-facts** — unverified on workerd, a day-1 check (Known limits).
- `worker::SqlStorage::exec` (synchronous), `SqlCursor::to_array`, `SqlStorageValue::{String, Integer, Null}`: verified (memo section 1, spike). `SELECT changes()` is not needed: no write outcome here is decisioned.
- Jobs (4, A3, A4): `JobKind::IndexPush` variant with a `run_slice` arm, `SliceOutcome::{Continue, Reschedule, Done}`, JSON cursor on `job.cursor`, `enqueue` dedup by kind, `jobs::rearm` the sole `set_alarm` caller (4.1; second `setAlarm` cancels the first — measured, platform-facts #5). A `dead` maintenance job is re-enqueued at `boot` (A4).
- `SliceBudget::spent_80pct()` and `SliceBudget.req: ReqBudget`: the helper shape the sibling proofs assume (A4); slice limits are 20,000 ms wall and 400 subrequests (4.3); every `read_entries` call is kept under 64 coalesced spans (A4).
- `Index::lookup` (2.3): sync, live packs only. `Bucket::read_entries` (7.2) over `Range::OffsetWithLength` (name verified in `worker` 0.8.5 source): range reads measured on the **local simulator only** (#6); the DO subrequest limit is **not enforced locally** (#7), so `ReqBudget::charge` is the only guard (7.1).
- `store::codec::decode_entry` (1.2, sync): inflate of a pack entry ran on workerd in the spike (CONTRACTS correction 2).
- `gix_object::TreeRefIter::from_bytes`, `EntryMode::{is_tree, is_commit}`, `CommitRef::from_bytes(..).tree()` (gix-object 0.64.1): CI-built for wasm32 (memo section 3); a commit walk over an in-memory `Find` ran on workerd (spike); `TreeRefIter` and the entry field names are per docs, **not run in the spike**.
- Sync-span atomicity for `begin`, `diff`, the upsert batches after each read await, and `finish`: measured (#4).
- `js_sys::Date::now()` (`now_ms`): synchronous host call, allowed in a sync span.
- `Stub::fetch_with_request` plus `stub_json` / `RepoRoute` for `/_do/search`: unverified at runtime, as in two-phase-push.
- `worker::Url::query_pairs` for `?q=`: url-crate API, re-export assumed, **unverified**.
- `serde_json` for the job cursor and DTOs (`SearchIn` lives in `wire::http`, A8): standard.
- Workers AI / Vectorize / D1: not in the memo or the contract — not used (Known limits).

## Proof code
```rust
// src/jobs/index_push.rs + src/repo_do/search.rs + src/edge/search.rs -- CONTRACTS.md 1.3, 2.1, 2.3, 3, 4, 5, 7, 9; A1-A4, A6-A9.
// REGISTRY (A9). Adds: route POST /_do/search (awaits: none); edge route GET /:owner/:repo/search?q=; JobKind::IndexPush;
//   tables blobs_fts / search_frontier / search_todo; meta row 'search.tip'; R2 key prefixes: none.
//   Foundation edits: commit_push step 7 gains `jobs::enqueue(&sql, JobKind::IndexPush, now, "{}")?` when any_ok (same span,
//   beside GcMark); repo_do::fetch dispatch arm; jobs::run_slice arm; boot's schema_version migration and dead-job re-enqueue
//   list (A4); DTO `SearchIn { q: String }` in wire::http (A8).
// exec / meta_str / meta_opt / now_ms / json / oid / bucket(): the RepoDo helpers of the sibling proofs.
//   CREATE VIRTUAL TABLE blobs_fts USING fts5(path, blob_sha UNINDEXED, body, tokenize='unicode61');
//   CREATE TABLE search_frontier(prefix TEXT NOT NULL, old TEXT, new TEXT NOT NULL);          -- rowid stack
//   CREATE TABLE search_todo(path TEXT PRIMARY KEY, sha TEXT NOT NULL) WITHOUT ROWID;
use std::collections::HashMap;
use bstr::BString;
use gix_hash::ObjectId;
use gix_object::{CommitRef, Kind, TreeRefIter};
use serde::{Deserialize, Serialize};
use worker::{Env, Request, Response, SqlStorage, SqlStorageValue as V};
use crate::{edge::{stub_json, RepoRoute}, error::Error, jobs::{self, Job, JobKind, SliceBudget, SliceOutcome},
            repo_do::RepoDo, store::{codec, Bucket, Index, ObjLoc}, ReqBudget};
const BATCH: i64 = 64;                      // one read_entries call charges at most 64 coalesced spans (A4)
const BLOB_CAP: u64 = 512 << 10;            // under the 2 MB SQLite value cap and the 16 MiB object cap (A7)
const NUL_PROBE: usize = 8_000;             // git's binary heuristic
const MAX_HITS: i64 = 50;
struct Ent { oid: ObjectId, tree: bool, gitlink: bool }
#[derive(Serialize, Deserialize)] struct Cur { new: String }   // the captured head; the work itself lives in the tables
#[derive(Deserialize)] struct N { n: i64 }
#[derive(Deserialize)] struct T { target: String }
#[derive(Deserialize)] struct FRow { rowid: i64, prefix: String, old: Option<String>, new: String }
#[derive(Deserialize)] struct TRow { path: String, sha: String }

/// jobs::run_slice arm for JobKind::IndexPush (4.2). Tables hold the work, meta.search.tip holds the watermark: a
/// Continue, a crash and a Reschedule all resume identically, and the cursor only carries the captured head.
pub async fn run_index_push(d: &RepoDo, job: &Job, budget: &mut SliceBudget) -> Result<SliceOutcome, Error> {
    let sql = d.state.storage().sql();
    let busy = !exec(&sql, "SELECT 1 AS n FROM search_frontier LIMIT 1", vec![])?.to_array::<N>()?.is_empty()
        || !exec(&sql, "SELECT 1 AS n FROM search_todo LIMIT 1", vec![])?.to_array::<N>()?.is_empty();
    let new = match (job.cursor.as_deref().map(serde_json::from_str::<Cur>), busy) {
        (Some(Ok(c)), true) => c.new,
        _ => match begin(d, &sql)? { Some(n) => n, None => return Ok(SliceOutcome::Done) },
    };
    let bucket = d.bucket()?;
    loop {
        if budget.spent_80pct() {                                            // A4: yield, next firing resumes
            return Ok(SliceOutcome::Continue { cursor: serde_json::to_string(&Cur { new: new.clone() })
                .map_err(|e| Error::Internal(e.to_string()))? });
        }
        if drain_todo(&sql, &bucket, budget).await? { continue; }
        let Some(f) = exec(&sql, "SELECT rowid, prefix, old, new FROM search_frontier ORDER BY rowid DESC LIMIT 1", vec![])?
            .to_array::<FRow>()?.into_iter().next() else { return finish(d, &sql, &new) };
        exec(&sql, "DELETE FROM search_frontier WHERE rowid=?", vec![f.rowid.into()])?;
        let (a, b) = (load_tree(&sql, &bucket, f.old.as_deref(), &mut budget.req).await?,
                      load_tree(&sql, &bucket, Some(&f.new), &mut budget.req).await?
                          .ok_or_else(|| Error::Internal("new tree".into()))?);
        diff(&sql, &f.prefix, a.as_deref(), &b)?;                            // one sync span: queue rows, file deletes
    }
}
/// Sync. `new` = current target of meta.head; `old` = meta.search.tip. An old tip that is no longer live (force-push,
/// then GcSweep deleted its pack) means rebuild, not an eternal retry: clear the table and diff from empty.
fn begin(d: &RepoDo, sql: &SqlStorage) -> Result<Option<String>, Error> {
    let Some(new) = exec(sql, "SELECT target FROM refs WHERE name=?", vec![d.meta_str("head")?.into()])?
        .to_array::<T>()?.into_iter().next().map(|t| t.target) else { return Ok(None) };   // empty repo
    let mut old = d.meta_opt("search.tip")?;
    if old.as_deref() == Some(new.as_str()) { return Ok(None) }                          // head already indexed
    if let Some(o) = &old {
        if Index(sql).lookup(&[oid(o)?])?.into_iter().next().flatten().is_none() {       // 2.3: live packs only
            exec(sql, "DELETE FROM blobs_fts", vec![])?; old = None;
        }
    }
    exec(sql, "DELETE FROM search_frontier", vec![])?; exec(sql, "DELETE FROM search_todo", vec![])?;
    exec(sql, "INSERT INTO search_frontier(prefix, old, new) VALUES('', ?, ?)",
         vec![old.as_deref().map_or(V::Null, V::from), V::from(new.as_str())])?;
    Ok(Some(new))
}
/// Sync: union of both trees' entries. Equal shas and gitlinks are skipped; subtrees become frontier rows; file
/// deletes land here; blob bodies wait in search_todo for the next batch.
fn diff(sql: &SqlStorage, prefix: &str, old: Option<&[u8]>, new: &[u8]) -> Result<(), Error> {
    let (a, b) = (entries(old)?, entries(Some(new))?);
    for (name, e) in &b {
        if e.gitlink || a.get(name).is_some_and(|o| o.oid == e.oid) { continue; }
        let path = format!("{prefix}{}", String::from_utf8_lossy(name.as_ref()));
        match (a.get(name), e.tree) {
            (Some(o), true) if o.tree => add_frontier(sql, &path, Some(o.oid), e.oid)?,
            (o, true) => { if o.is_some_and(|o| !o.gitlink) { del_path(sql, &path)?; } add_frontier(sql, &path, None, e.oid)?; }
            (o, false) => { if o.is_some_and(|o| o.tree) { drop_subtree(sql, &path)?; } add_todo(sql, &path, e.oid)?; }
        }
    }
    for (name, o) in &a {
        if o.gitlink || b.contains_key(name) { continue; }
        let path = format!("{prefix}{}", String::from_utf8_lossy(name.as_ref()));
        if o.tree { drop_subtree(sql, &path)?; } else { del_path(sql, &path)?; }
    }
    Ok(())
}
fn entries(data: Option<&[u8]>) -> Result<HashMap<BString, Ent>, Error> {
    let mut m = HashMap::new();
    if let Some(d) = data {
        for e in TreeRefIter::from_bytes(d) {
            let e = e.map_err(|e| Error::Unpack(e.to_string()))?;
            m.insert(e.filename.to_owned(), Ent { oid: e.oid, tree: e.mode.is_tree(), gitlink: e.mode.is_commit() });
        }
    }
    Ok(m)
}
/// Commit or tree oid -> tree bytes; a commit costs a second lookup+read. None -> the empty side of the diff.
async fn load_tree(sql: &SqlStorage, bucket: &Bucket, hex: Option<&str>, b: &mut ReqBudget) -> Result<Option<Vec<u8>>, Error> {
    let Some(hex) = hex else { return Ok(None) };
    match read_obj(sql, bucket, &oid(hex)?, b).await? {
        (Kind::Tree, d) => Ok(Some(d)),
        (Kind::Commit, d) => { let t = CommitRef::from_bytes(&d).map_err(|e| Error::Unpack(e.to_string()))?.tree();
                               Ok(Some(read_obj(sql, bucket, &t, b).await?.1)) }
        _ => Err(Error::Internal("ref target is not a commit".into())),
    }
}
/// 2.3 lookup (live only) then one coalesced read (7.2, charged 7.1). A miss is impossible for a live ref (2.5).
async fn read_obj(sql: &SqlStorage, bucket: &Bucket, id: &ObjectId, b: &mut ReqBudget) -> Result<(Kind, Vec<u8>), Error> {
    let loc = Index(sql).lookup(&[*id])?.into_iter().next().flatten()
        .ok_or_else(|| Error::Storage(format!("reachable object {id} not live")))?;
    let (_, entry) = bucket.read_entries(&[(*id, loc)], b).await?.into_iter().next()
        .ok_or_else(|| Error::Storage("entry read".into()))?;
    codec::decode_entry(&entry)
}
/// One batch of pending blob rows. The (path, blob_sha) check makes a mid-slice crash cheap: committed rows are
/// skipped, never re-read. Binary, oversized and undecodable blobs get an empty body: the path still matches, the
/// row still marks the work done, and one corrupt object can never stall the queue (4.4).
async fn drain_todo(sql: &SqlStorage, bucket: &Bucket, budget: &mut SliceBudget) -> Result<bool, Error> {
    let batch = exec(sql, "SELECT path, sha FROM search_todo LIMIT ?", vec![BATCH.into()])?.to_array::<TRow>()?;
    if batch.is_empty() { return Ok(false); }
    let (mut want, mut meta) = (Vec::new(), Vec::new());
    for t in &batch {
        let sha = oid(&t.sha)?;
        let done = !exec(sql, "SELECT 1 AS n FROM blobs_fts WHERE path=? AND blob_sha=? LIMIT 1",
                         vec![t.path.as_str().into(), t.sha.as_str().into()])?.to_array::<N>()?.is_empty();
        if done { del_todo(sql, &t.path)?; continue; }
        match Index(sql).lookup(&[sha])?.into_iter().next().flatten() {
            Some(loc) if loc.size <= BLOB_CAP => { want.push((sha, loc)); meta.push((t.path.clone(), t.sha.clone(), sha)); }
            Some(_) => { upsert(sql, &t.path, &t.sha, "")?; del_todo(sql, &t.path)?; }     // too big: path row only
            None => { del_todo(sql, &t.path)?; }                                          // impossible by 2.5; drop
        }
    }
    for (sha, entry) in bucket.read_entries(&want, &mut budget.req).await? {              // <= 64 coalesced spans (A4)
        let Some((path, hex, _)) = meta.iter().find(|m| m.2 == sha) else { continue };
        let body = match codec::decode_entry(&entry) {
            Ok((Kind::Blob, d)) if d.iter().take(NUL_PROBE).all(|b| *b != 0) => String::from_utf8_lossy(&d).into_owned(),
            _ => String::new(),                                                          // binary or undecodable
        };
        upsert(sql, path, hex, &body)?; del_todo(sql, path)?;
    }
    Ok(true)
}
fn add_frontier(sql: &SqlStorage, dir: &str, old: Option<ObjectId>, new: ObjectId) -> Result<(), Error> {
    exec(sql, "INSERT INTO search_frontier(prefix, old, new) VALUES(?,?,?)",
         vec![format!("{dir}/").into(), old.map_or(V::Null, |o| V::from(o.to_string())), V::from(new.to_string())])?; Ok(())
}
fn add_todo(sql: &SqlStorage, path: &str, sha: ObjectId) -> Result<(), Error> {
    exec(sql, "INSERT OR IGNORE INTO search_todo(path, sha) VALUES(?,?)", vec![path.into(), sha.to_string().into()])?; Ok(())
}
fn del_path(sql: &SqlStorage, path: &str) -> Result<(), Error> { exec(sql, "DELETE FROM blobs_fts WHERE path=?", vec![path.into()])?; Ok(()) }
fn del_todo(sql: &SqlStorage, path: &str) -> Result<(), Error> { exec(sql, "DELETE FROM search_todo WHERE path=?", vec![path.into()])?; Ok(()) }
/// [dir/, dir0): every indexed path under dir/ and nothing else ('/' is 0x2F, '0' is 0x30).
fn drop_subtree(sql: &SqlStorage, dir: &str) -> Result<(), Error> {
    exec(sql, "DELETE FROM blobs_fts WHERE path>=? AND path<?", vec![format!("{dir}/").into(), format!("{dir}0").into()])?; Ok(())
}
fn upsert(sql: &SqlStorage, path: &str, sha: &str, body: &str) -> Result<(), Error> {
    exec(sql, "DELETE FROM blobs_fts WHERE path=?", vec![path.into()])?;
    exec(sql, "INSERT INTO blobs_fts(path, blob_sha, body) VALUES(?,?,?)", vec![path.into(), sha.into(), body.into()])?; Ok(())
}
/// One sync span. A head that moved during the job (a push's enqueue deduped against this running row, 4.5)
/// reschedules; the next firing finds empty work tables and begins a diff from the tip written here.
fn finish(d: &RepoDo, sql: &SqlStorage, new: &str) -> Result<SliceOutcome, Error> {
    exec(sql, "INSERT OR REPLACE INTO meta(key, value) VALUES('search.tip', ?)", vec![new.into()])?;
    let tip = exec(sql, "SELECT target FROM refs WHERE name=?", vec![d.meta_str("head")?.into()])?.to_array::<T>()?
        .into_iter().next().map(|t| t.target);
    Ok(match tip { Some(t) if t != new => SliceOutcome::Reschedule { run_at: now_ms() }, _ => SliceOutcome::Done })
}

// ---- src/repo_do/search.rs: POST /_do/search, "Awaits inside: none" (1.3). ----
#[derive(Deserialize, Serialize)] struct Hit { path: String, snip: String, rank: f64 }
impl RepoDo {
    pub fn search(&self, b: &SearchIn) -> Result<Response, Error> {
        if b.q.len() > 1_024 { return Err(Error::Protocol("search query too long".into())); }
        let sql = self.sql();
        let tip = exec(&sql, "SELECT target FROM refs WHERE name=?", vec![self.meta_str("head")?.into()])?
            .to_array::<T>()?.into_iter().next().map(|t| t.target);
        let indexed = self.meta_opt("search.tip")?;
        if indexed != tip { jobs::enqueue(&sql, JobKind::IndexPush, now_ms(), "{}")?; }    // demand-driven, dedups (4.5)
        let hits = match fts_query(&b.q).as_str() {
            "" => Vec::new(),
            m => exec(&sql, "SELECT path, snippet(blobs_fts, 2, '[', ']', '...', 12) AS snip, bm25(blobs_fts) AS rank \
                             FROM blobs_fts WHERE blobs_fts MATCH ? ORDER BY rank LIMIT ?",
                        vec![m.into(), MAX_HITS.into()])?.to_array::<Hit>()?,
        };
        json(serde_json::json!({ "indexed": indexed, "head": tip, "hits": hits }))
    }
}
/// Whitespace-split terms as quoted FTS5 phrases: no user byte reaches MATCH as syntax (first-pass review nit).
fn fts_query(q: &str) -> String {
    q.split_whitespace().take(64).map(|t| format!("\"{}\"", t.replace('"', ""))).collect::<Vec<_>>().join(" ")
}
// ---- src/edge/search.rs: GET /:owner/:repo/search?q=<terms>; any authenticated principal (section 12). ----
pub async fn search(req: &Request, env: &Env, repo: &RepoRoute, budget: &mut ReqBudget) -> Result<Response, Error> {
    let q = req.url()?.query_pairs().find(|(k, _)| k == "q").map(|(_, v)| v.into_owned()).unwrap_or_default();
    let v: serde_json::Value = stub_json(&repo.stub(env)?, repo, "/_do/search", &serde_json::json!({ "q": q }), budget).await?;
    Response::from_json(&v).map_err(Error::from)
}
```

## Why it works
- **Indexing is a job slice, not push-path work.** `commit_push` gains one `jobs::enqueue(IndexPush, now)` beside `GcMark` in section 3 step 7; the row lands in the same sync span as the ref CAS (measured, platform-facts #4), `enqueue` dedups by kind (4.5) and `rearm` arms the alarm after the span (A3). The push response is untouched — `report-status` still ships on the commit span's result — and an indexer that fails, retries or goes `dead` never reaches a push.
- **The loose-object blocker is gone by the storage contract.** Every object at rest is a full entry of a normalized pack (2.1); `read_obj` is `Index::lookup` (2.3, live only) plus `read_entries` (7.2) plus `codec::decode_entry`. No `objects/xx/yyyy` keys, no delta resolution at index time — ingest already resolved every delta (2.4) — and the `objects` index the push itself wrote is the locator.
- **The livelock cannot recur.** Resumable state is the two work tables plus `meta.search.tip`, not a wall-clock guess: `Continue` persists the cursor and 4.2 gives the next firing; a crashed slice re-runs from the last committed span (a span's writes roll back together, #4) and the `(path, blob_sha)` row check skips the R2 read for anything already upserted — exactly the review's `WHERE NOT EXISTS` fix. A 10,000-file initial push is about 160 `read_entries` calls of 64 blobs plus tree reads — one or two slices inside the 400-subrequest budget (4.3) — not an unbounded restart loop.
- **One bad object cannot stall the queue.** Binary, oversized (`loc.size > BLOB_CAP`, no read needed) and undecodable blobs degrade to an empty-body row — the path still matches `MATCH` on the `path` column and the work is marked done. Only tree/commit read errors propagate, and 2.5 makes a missing object impossible for a live ref; a genuinely persistent failure hits 4.4's backoff and goes `dead` after 8 attempts, which never blocks `queued` selection, and `boot` re-enqueues a dead maintenance kind (A4).
- **Branch conflation is removed, not patched.** Rows are keyed by `path` alone and always describe `meta.head`, the default branch — the review's own alternative ("or index only the default branch"). Push order is irrelevant: `finish` re-reads the live `refs` target in its sync span and `Reschedule`s when a push landed mid-job (its enqueue deduped against the running row), so the index converges to the newest head.
- **Deletes are the diff's own output.** A file present in the old tree and absent in the new loses its row (`del_path`); a vanished directory costs one range `DELETE` on `[dir/, dir0)` (`drop_subtree`) without walking it; a rename is that plus a re-add; directory↔file type changes take the matching arm (`diff`). No stale hits survive a converged job.
- **Reads can never see a hole.** `lookup` joins `p.state='live'` (2.3); a pack `GcSweep` kills keeps its R2 bytes for GRACE = 1 h (5), longer than any slice, so a read issued before a sweep still completes. The one place a missing object is expected — the captured old tip after a force-push plus a completed GC — is caught by the sync lookup in `begin` and becomes a rebuild (`DELETE FROM blobs_fts`, `old=None`), not a retry loop against a dead pack.
- **The query cannot throw.** `fts_query` emits whitespace-split double-quoted phrases, so no user byte reaches `MATCH` as FTS5 syntax (the review's interop nit); an oversized `q` is `Error::Protocol` → 400 (section 10). `indexed`/`head` in the response are the lag signal the first pass exposed as `X-Index-Lag`.
- **The amendments are obeyed.** No `IN (...)` list anywhere (A6); the only multi-row statement is the range `DELETE`. Every R2 call charges `budget.req` (A1); blob reads are ≤64-span calls (A4); the 512 KiB text cap sits under the 2 MB SQLite value cap and the 16 MiB object cap (A7). Blob-batch heap is ≤64 entries × 512 KiB decoded plus the compressed entries — about 40 MiB worst case, inside 128 MB. `js_sys::Reflect` is never touched; the search DTO lives in `wire::http` (A8); nothing here calls `set_alarm` (4.1, CI grep).
- **No wire impact.** The push and fetch protocols are unchanged; `search` is a plain authenticated HTTP route outside `git-upload-pack`/`git-receive-pack`, so every conformance scenario the foundation passes still passes. The optional v2 `command=search` of `agent-native-commands` maps onto the same `blobs_fts` table whenever that module lands.
- Conformance scenarios this proof must pass (section 11): 2, 4, 14 (push, delete-push, janitor/GC behaviour unchanged). The feature itself is not exercisable by stock `git`; the harness drives the HTTP route with `curl`. Added scenario 16: push commits adding `src/main.rs` with a distinctive token and a nested `docs/` tree; fire the alarm until `search.tip` equals the head target; `GET /:o/:r/search?q=<token>` returns the path with a `[`…`]` snippet and `indexed == head`; push a delete of the file, fire the alarm, the token returns zero hits; push the same token on `refs/heads/topic`, fire the alarm, results still show the default branch only. Added scenario 17 (resume): push 200 text files, kill the DO mid-slice (test hook), advance the fake clock and fire the alarm; assert the job resumes from the cursor, `search.tip` converges to head, `x-ge-subrequests` stays within the 400-per-slice budget, and `git fsck` on a fresh clone is clean.

## Changes from the first pass
| First-pass finding (quoted) | Kind | How addressed |
|---|---|---|
| "Resume path livelocks on any job larger than one 20 s slice (no cursor, no skip-if-indexed). Trivial fix, but as written initial pushes of real repos never index." | blocker | Work state lives in `search_frontier`/`search_todo` rows, not in flight: `Continue` at 80 % (A4) resumes mid-diff; a crashed slice's committed upserts are skipped by the `(path, blob_sha)` check before any R2 read (`drain_todo`). An initial push of any size converges in a bounded number of slices. |
| "Depends on every pushed object existing as a loose zlib object at `objects/xx/yyyy` in R2; if packs are stored as packs (the sane choice for a big push), `readBlob`/`readTree` need pack-idx + delta resolution that this proof does not contain." | blocker | There are no loose objects (2.1). `read_obj` resolves any sha through the 2.3 index (`Index::lookup`, live packs) and reads it with `read_entries` (7.2); `codec::decode_entry` inflates the stored full object. No delta resolution anywhere — ingest normalizes (2.4). |
| "Branch conflation: single flat `path` key across all refs; results depend on push order." | caveat | Only `meta.head`'s branch is indexed; `finish` reschedules when head moved mid-job, so the index converges to the newest head. The `(ref, path)` all-branches variant is recorded in Known limits. |
| "Alarm retries are bounded (~6); a poison job stalls the whole queue (`ORDER BY id LIMIT 1`)." | caveat | The section 4 dispatcher owns retries: exponential backoff, `dead` after 8 attempts, and `dead` rows never block `queued` selection (4.4); `boot` re-enqueues a dead maintenance kind (A4). Per-blob corruption degrades to an empty-body row instead of failing the job (`drain_todo`). |
| "Deletes/renames not handled; stale hits persist." | caveat | The diff emits a point `DELETE` per removed file and one range `DELETE` (`path >= 'dir/' AND path < 'dir0'`) per removed directory; a rename is a delete plus an add. |
| "FTS5 in DO SQLite is documented, but the D1 fallback loses the 'same transaction as the ref flip' property the proof leans on." | caveat | The index stays in DO SQLite (no D1). The property the proof actually needed — the trigger and the ref move are atomic — is kept: `enqueue` runs inside `commit_push`'s span (section 3, write-back). FTS5 presence on workerd is unverified and gated as a day-1 check (Known limits). |
| "Cost: one Class B op per changed tree+blob; 10k-file push = 10k GETs plus minutes of alarm time; no batching." | caveat | Blob reads are 64-entry `read_entries` calls that coalesce to ≤64 spans and usually far fewer (same pack, adjacent offsets, 7.2); tree reads are 1-2 per pair. A 10,000-file initial push is ~160+ calls across ~2 slices, not 10,000 single GETs. |
| "Vectorize stage is a sketch (no chunking, dedup, or cost model)." | caveat | Dropped, not fixed: neither binding is in the memo or the contract. Stated in Mechanism; remains a later job kind over the same rows. |
| Review crash walk-through: "the FTS table holds 40 fresh rows and 260 stale ones ... Re-run re-walks the whole diff" | caveat | Rows written before a crash are valid content-addressed upserts; on resume the `(path, blob_sha)` check skips their R2 reads, and the tables resume the walk without re-diffing committed spans. |
| Review concurrency: "the index has no `ref` column: `feature` indexing `README.md` deletes and replaces `main`'s row ... Search results are a race, not a view of any branch." | caveat | Only the default branch is indexed; a non-head push enqueues the job, `begin` finds `old == new` and exits. |
| Review interop: "`MATCH ?` with a raw user string throws on FTS5 syntax ... must escape/quote terms or return 400." | caveat | `fts_query` emits only quoted phrases; `q` over 1 KiB is `Protocol` → 400. |
| Review interop: "a 'protocol-v2 `search` command' can only ride on upload-pack's v2 capability advertisement" | caveat | The v2 command is dropped from this module; the route is plain authenticated HTTP, and `agent-native-commands` can bind `command=search` to `blobs_fts` later (Depends on). |
| First-pass limit: "Indexing is asynchronous: a `search` immediately after `git push` can miss the last commit; expose `index_jobs` depth as `X-Index-Lag`" | limit | Still asynchronous by design; the response carries `indexed` and `head` tips, and a lagging index self-heals because `search` enqueues the job (dedup 4.5). |
| First-pass limit: "Deleted files are handled ... only when the path reappears" | limit | Handled by the diff's delete pass (`del_path`, `drop_subtree`). |
| First-pass limit: "One R2 GET per changed tree and blob" | limit | Coalesced `read_entries` batches (7.2); see the caveat row above. |
| First-pass limit: "Only loose objects read from R2 are shown; blobs stored inside packs ... need delta resolution" | limit | Obsolete under 2.1/2.3: all objects are full entries in packs and every read goes through the index. |

## Known limits
- **FTS5 in DO SQLite is unverified on workerd.** Documented on Cloudflare's extension list per the first-pass review; never exercised in the spike or platform-facts. Day-1 check: `CREATE VIRTUAL TABLE` + `MATCH`/`snippet`/`bm25` inside a DO. If absent, the fallback is a D1 table per repo (`env.d1` binding, `fetch`-shaped calls from the edge, no sync API in a DO) which loses the DO's span atomicity — a different module, not a write-back taken here.
- **Asynchronous by design.** A push is visible to `search` only after at least one alarm round-trip; `indexed` vs `head` in the response is the contract with callers. A burst of pushes coalesces: the deduped job always diffs to the newest head.
- **Default branch only.** Searching `refs/heads/topic` is not possible; keying rows by `(ref, path)` multiplies the walk and the FTS table by the ref count and was left out per the review's own fix. `meta.head` changing (re-targeting HEAD) is a rebuild the next run performs via the `begin` diff.
- **Non-UTF-8 filenames** are lossy-mangled at insert and delete alike (`String::from_utf8_lossy`), so behaviour is consistent but two distinct byte names can share one row — last writer wins. Bodies are lossy-decoded too.
- **Cost remains proportional to the diff.** Coalescing turns per-object GETs into spans, but a 100k-file first index still costs ~1,600 `read_entries` calls across ~4-5 slices plus FTS writes; `x-ge-subrequests` asserts the per-slice bound. `search_frontier`/`search_todo` rows persist a crashed job's backlog and are cleared by `begin`.
- **A swept old tip is a full rebuild**, not an incremental: `DELETE FROM blobs_fts` and re-add. Rare (requires force-push plus completed GC before the job ran) and bounded by the same per-slice budget. A generation column that marks survivors would preserve unchanged rows; not done.
- **Memory and CPU per slice**: ≤64 entries × 512 KiB decoded plus their compressed bytes (~40 MiB worst case), two trees of at most the 16 MiB object cap (A7), a few thousand `Ent`/`BString` map nodes per diff — inside 128 MB. CPU is `LIMIT` queries + inflate; far under the 20 s slice wall (4.3).
- **Subrequests are budgeted, not measured** (platform-facts #7): `SliceBudget`/`ReqBudget` are the only guards until a deployed-Worker measurement exists; the DO subrequest limit is not enforced locally.
- **Unverified, day-1 list.** FTS5 presence and `DELETE ... WHERE <column>` on an FTS5 table; `TreeRefIter` / `EntryMode::{is_tree, is_commit}` / `CommitRef::tree()` / `e.filename` names in gix-object 0.64.1; `Url::query_pairs` re-export; `stub_json`/`RepoRoute`/`Request::new_with_init` at runtime (as in two-phase-push); real-R2 range-read behaviour (#6); that `meta_opt` exists as the sibling proofs assume.
- **Write-backs proposed to CONTRACTS.md.** `JobKind::IndexPush` and its `run_slice` arm (1.4, 4.5); route `/_do/search` (1.3, awaits: none) and edge `GET /:owner/:repo/search`; `commit_push` step 7's second enqueue; `boot`'s `schema_version` migration for the three tables and its dead-maintenance-job re-enqueue list (A4); `meta` row `search.tip`; `SearchIn`/`Hit` DTOs in `wire::http` (A8); section 12 gains "FTS5 virtual tables in DO SQLite" once the day-1 check passes.

## Depends on
- repo-do-ref-authority
- refs-sqlite-objects-r2
- two-phase-push
- streaming-pack-parser
- gc-and-repack-alarm
- auth-and-multitenancy
- (optional) agent-native-commands — a v2 `search` command over `blobs_fts`; vectorized-commit-graph — the dropped semantic stage
