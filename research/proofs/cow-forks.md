# Copy-on-write forks

> Second pass · Idea #22 · verdict: **risky** · feasibility 4/5 · reliability 3/5 · correctness 3/5 (first pass 4/2/3)
> First pass: [proof](./archived/cow-forks.md) · [review](../reviews/archived/cow-forks.md) · Second pass: [review](../reviews/cow-forks.md) · Contract: [CONTRACTS.md](../../CONTRACTS.md)

## Mechanism
A fork is an ordinary `RepoDo` whose `objects` index starts life as a copy of the parent's — but the unit of sharing is now the pack, not the loose object (there are no loose objects, section 2.1). The fork copies the parent's `objects` rows verbatim for a pinned set of packs and registers each such pack in its own `packs` table as `live`; the new `borrowed` table records which `repo_id` owns the bytes. Reads resolve `sha -> (pack_id, offset, len)` through the unchanged 2.3 query, then range-read `r/<owner_repo>/packs/<pack>.pack` through `Bucket::for_repo` (2.2). Writes are unchanged: pushes to the fork ingest into fork-owned packs (2.4), and dedup across the fork boundary is git's own negotiation against the copied refs — the first-pass's zero-head-check CoW boundary survives.

REGISTRY (amendment A9): edge routes `POST /:o/:r/fork`, `POST /:o/:r/fork/sync`, `POST /:o/:r/fork/detach`; DO routes `/_do/fork`, `/_do/fork/sync`, `/_do/fork/detach` (fork side) and `/_do/fork/begin`, `/_do/fork/page`, `/_do/fork/unpin` (any repo in its parent role); `JobKind::Fork`; tables `forks`, `pack_pins`, `borrowed`, `fork_unpins`; meta keys `parent`, `fork_state`; no new R2 prefixes. Foundation edits, listed here once: `GcSweep`'s dead-mark gains `AND id NOT IN (SELECT pack_id FROM pack_pins)`; a fork's `GcMark` candidate query gains `AND id NOT IN (SELECT pack_id FROM borrowed WHERE released=0)`; a fork's `GcSweep` queues `fork_unpins` for swept borrowed packs and enqueues `Fork`; the `/_do/push/lookup` response gains `owner` per loc; `wire::http` gains `RepoHeaders::for_name`; `store::Bucket` gains `for_repo`.

The first-pass's hardest blocker — the parent's GC collecting objects a fork still resolves — is closed by `pack_pins` inside the parent's own sync spans: `/_do/fork/begin` pins every live pack in one span, and the parent's `GcSweep` dead-mark re-checks pins inside its own span (5.3). Because a pinned pack can neither be consolidated nor swept, every row the fork can ever import is backed by bytes that cannot be collected while the pin stands. Pins release only through `/_do/fork/unpin`, which the fork's `Fork` job sends for packs its own sweep already dropped after `fork_detach` made them candidates. Protection is transitive: a fork-of-fork pins the direct parent's live set (including packs the parent itself borrowed), so an ancestor pack stays pinned at its owner through each intermediate fork until the whole chain releases. What the contract cannot close — pin granularity, abandoned-fork retention, parent durability — is stated plainly in Known limits.

## Primitives
- `env.durable_object("REPO").id_from_name(parent).get_stub()` + `Stub::fetch_with_request` for every fork-to-parent call: verified (memo section 1, spike). Typed DO RPC: experimental, not used.
- `Request::new_with_init` + `RequestInit::{with_method, with_body}` for the DO-to-DO JSON call: **unverified at runtime** (same caveat as two-phase-push).
- Sync-span atomicity of `fork_parent_begin` against the parent's `GcSweep` span: pin insert and dead-mark cannot interleave inside one DO — measured (platform-facts #4).
- `INSERT ... ON CONFLICT DO NOTHING` / `INSERT OR IGNORE` + `SELECT changes()` for the fork claim and idempotent page replay: measured 1 / 0 / 1 (platform-facts #1).
- `json_each(?)` for the unpin and candidate `IN` lists, and `INSERT ... SELECT ... WHERE EXISTS` for the import guard: plain SQLite inside the 100-bound-parameter cap (A6).
- `SliceBudget { started_ms, subrequests_used }`, `SliceOutcome::{Done, Continue}`, `jobs::enqueue` with dispatcher-side `jobs::rearm` (A3, A4): this module never calls `set_alarm` (4.1). Second `setAlarm` cancels the first: measured (#5).
- `worker::Bucket: Clone` for `for_repo`: **unverified**; fallback is `env.bucket("BUCKET")` per owner.
- `SqlStorageValue: From<Option<String>>` for `refs.peeled`: **unverified**; fallback is two INSERT forms.
- `js_sys::Date::now()` for `created_at`/`updated_at`: standard. No new gix APIs — a fork is wire-identical to a normal repo.

## Proof code
```rust
// src/repo_do/fork.rs + src/jobs/fork.rs + src/store/bucket.rs -- worker 0.8.5. CONTRACTS.md 2.2-2.3, 3, 4, 5, 7, 8.
// `q`, `changes`, `meta`, `meta_opt`, `now_ms`, `json`, `parse`, `list_refs` are the RepoDo helpers of
// repo-do-ref-authority; `Error::from_do_response` is the section-10 mapping. Any dispatch arm whose sync span
// enqueued a job awaits `jobs::rearm(self)` before responding (A3). REGISTRY: see Mechanism (A9).
use serde::Deserialize;
use worker::{Method, Request, RequestInit, Response, SqlStorageValue as V};
use crate::{error::Error, jobs::{self, Job, JobKind, SliceBudget, SliceOutcome}, repo_do::RepoDo,
            store::{Bucket, RepoId}, wire::http::RepoHeaders};
const PAGE_ROWS: i64 = 20_000;                                  // ~2 MB of JSON, one stub subrequest per page (7.1)

#[derive(Deserialize)] struct N { n: i64 }
#[derive(Deserialize)] pub struct BeginDto { fork_repo: String, name: String }
#[derive(Deserialize)] pub struct PageDto { fork_repo: String, sha: String, pack: String }
#[derive(Deserialize)] pub struct UnpinDto { fork_repo: String, packs: Vec<String> }
#[derive(Deserialize)] struct PinPack { id: String, count: i64, bytes: i64, commit_lo: i64, commit_hi: i64,
                                        created_at: i64, owner_repo: Option<String> }
#[derive(Deserialize)] struct ObjRec { sha: String, pack_id: String, idx: i64, offset: i64, len: i64, kind: i64, size: i64 }
#[derive(Deserialize)] struct RefDto { name: String, target: String, peeled: Option<String> }
#[derive(Deserialize)] struct BeginOut { repo_id: String, head: String, refs: Vec<RefDto>, packs: Vec<PinPack> }
#[derive(Deserialize)] struct PageOut { rows: Vec<ObjRec>, cursor: Option<String> }
#[derive(Deserialize)] struct IdRow { pack_id: String }

// ---- fork side --------------------------------------------------------------
impl RepoDo {
    /// /_do/fork. The claim is the ON CONFLICT miss on meta.parent inside one sync span (8, A2): a duplicate
    /// POST /fork or any non-empty repo loses here and gets Conflict -> 409, never a half-initialised fork.
    pub fn fork_claim(&self, parent: &str) -> Result<Response, Error> {
        let used = self.q("SELECT (SELECT COUNT(*) FROM refs)+(SELECT COUNT(*) FROM packs) AS n", vec![])?.one::<N>()?.n;
        if used != 0 { return Err(Error::Conflict("repo not empty".into())); }
        self.q("INSERT INTO meta(key,value) VALUES('parent',?) ON CONFLICT DO NOTHING", vec![V::from(parent)])?;
        if self.changes()? != 1 { return Err(Error::Conflict("already a fork".into())); }
        self.q("INSERT INTO meta(key,value) VALUES('fork_state','pinning')", vec![])?;
        jobs::enqueue(&self.sql(), JobKind::Fork, now_ms(), "{}")?;
        json(serde_json::json!({ "ok": true, "importing": true }))
    }
    /// /_do/fork/sync: re-pin the parent's now-live set and import the rows that arrived since. Idempotent.
    pub fn fork_sync(&self) -> Result<Response, Error> {
        if self.meta("fork_state")?.as_str() != "done" { return Err(Error::Conflict("fork busy".into())); }
        self.q("UPDATE meta SET value='pinning' WHERE key='fork_state'", vec![])?;
        jobs::enqueue(&self.sql(), JobKind::Fork, now_ms(), "{}")?;
        json(serde_json::json!({ "ok": true }))
    }
    /// /_do/fork/detach: borrowed packs become ordinary GC candidates; the section-5 chain copies the entries
    /// this fork still reaches into a fork-owned pack, and the sweep queues the parent unpins.
    pub fn fork_detach(&self) -> Result<Response, Error> {
        self.q("UPDATE borrowed SET released=1", vec![])?;
        jobs::enqueue(&self.sql(), JobKind::GcMark, now_ms(), "{}")?;
        json(serde_json::json!({ "ok": true }))
    }
    /// Which repo_id's prefix holds a pack; one sync query per pack on the fetch path (9.6 is per pack anyway).
    pub fn owner_of(&self, pack: &str) -> Result<String, Error> {
        #[derive(Deserialize)] struct O { owner_repo: String }
        match self.q("SELECT owner_repo FROM borrowed WHERE pack_id=?", vec![V::from(pack)])?.to_array::<O>()?.into_iter().next() {
            Some(o) => Ok(o.owner_repo), None => self.meta("repo_id"),
        }
    }
}

// ---- parent role --------------------------------------------------------------
impl RepoDo {
    /// /_do/fork/begin: one sync span vs GcSweep's one sync span (5, platform-facts #4) -- each live pack is
    /// pinned in the same instant it is listed, so a sweep either already ran (pack not live, never returned)
    /// or must skip it. Idempotent: a retried begin re-pins the now-live set; INSERT OR IGNORE only grows pins.
    pub fn fork_parent_begin(&self, b: &BeginDto) -> Result<Response, Error> {
        let now = now_ms();
        self.q("INSERT INTO forks(fork_repo,name,created_at) VALUES(?,?,?) ON CONFLICT DO NOTHING",
               vec![V::from(b.fork_repo.as_str()), V::from(b.name.as_str()), V::from(now)])?;
        self.q("INSERT OR IGNORE INTO pack_pins(pack_id,fork_repo,created_at) \
                SELECT id,?,? FROM packs WHERE state='live'", vec![V::from(b.fork_repo.as_str()), V::from(now)])?;
        let packs = self.q("SELECT p.id,p.count,p.bytes,p.commit_lo,p.commit_hi,p.created_at,w.owner_repo \
                            FROM pack_pins n JOIN packs p ON p.id=n.pack_id LEFT JOIN borrowed w ON w.pack_id=p.id \
                            WHERE n.fork_repo=?", vec![V::from(b.fork_repo.as_str())])?.to_array::<PinPack>()?;
        let (_h, refs) = self.list_refs()?;                                  // RefRow carries peeled (A5)
        json(serde_json::json!({ "repo_id": self.meta("repo_id")?, "head": self.meta("head")?,
                                 "refs": refs, "packs": packs }))
    }
    /// /_do/fork/page. Rows are served only for pinned packs: a push the parent commits after begin lands in a
    /// pack this query cannot return, so the fork's index can never reference unpinned bytes. The cursor is the
    /// objects primary key (sha, pack_id), so same-sha duplicates across packs cannot straddle a page (2.3).
    pub fn fork_parent_page(&self, b: &PageDto) -> Result<Response, Error> {
        let rows = self.q("SELECT o.sha,o.pack_id,o.idx,o.offset,o.len,o.kind,o.size FROM objects o \
                           JOIN pack_pins n ON n.pack_id=o.pack_id AND n.fork_repo=? \
                           WHERE o.sha > ? OR (o.sha=? AND o.pack_id > ?) ORDER BY o.sha,o.pack_id LIMIT ?",
                          vec![V::from(b.fork_repo.as_str()), V::from(b.sha.as_str()), V::from(b.sha.as_str()),
                               V::from(b.pack.as_str()), V::from(PAGE_ROWS)])?.to_array::<ObjRec>()?;
        let cursor = (rows.len() == PAGE_ROWS as usize).then(|| rows.last().map(|r| format!("{}:{}", r.sha, r.pack_id))).flatten();
        json(serde_json::json!({ "rows": rows, "cursor": cursor }))
    }
    /// /_do/fork/unpin. The caller only sends pack_ids its own sweep already dropped, so this is unconditional.
    pub fn fork_parent_unpin(&self, b: &UnpinDto) -> Result<Response, Error> {
        let arr = serde_json::to_string(&b.packs).map_err(|e| Error::Internal(e.to_string()))?;
        self.q("DELETE FROM pack_pins WHERE fork_repo=? AND pack_id IN (SELECT value FROM json_each(?))",
               vec![V::from(b.fork_repo.as_str()), V::from(arr.as_str())])?;            // one bound param (A6)
        self.q("DELETE FROM forks WHERE fork_repo=? AND NOT EXISTS(SELECT 1 FROM pack_pins WHERE fork_repo=?)",
               vec![V::from(b.fork_repo.as_str()), V::from(b.fork_repo.as_str())])?;
        json(serde_json::json!({}))
    }
}

// ---- src/jobs/fork.rs: JobKind::Fork, one slice per alarm firing (4.2), one stub subrequest per slice (7.1) ----
pub async fn run_slice(d: &RepoDo, job: &Job, budget: &mut SliceBudget) -> Result<SliceOutcome, Error> {
    match d.meta("fork_state")?.as_str() {
        "pinning" => {
            let out: BeginOut = parent_json(d, "/_do/fork/begin", &serde_json::json!(
                { "fork_repo": d.meta("repo_id")?, "name": format!("{}/{}", d.meta("owner")?, d.meta("repo")?) }),
                budget).await?;
            // sync span: pin set, refs and head land together or not at all; a Storage/Internal Err propagates (A2)
            for p in &out.packs {
                d.q("INSERT INTO packs(id,state,count,bytes,commit_lo,commit_hi,push_id,created_at) \
                     VALUES(?,'live',?,?,?,?,NULL,?) ON CONFLICT DO NOTHING",
                    vec![V::from(p.id.as_str()), V::from(p.count), V::from(p.bytes), V::from(p.commit_lo),
                         V::from(p.commit_hi), V::from(p.created_at)])?;
                d.q("INSERT OR IGNORE INTO borrowed(pack_id,owner_repo) VALUES(?,?)",
                    vec![V::from(p.id.as_str()), V::from(p.owner_repo.clone().unwrap_or_else(|| out.repo_id.clone()))])?;
            }
            for r in &out.refs {
                d.q("INSERT OR IGNORE INTO refs(name,target,peeled,updated_at) VALUES(?,?,?,?)",
                    vec![V::from(r.name.as_str()), V::from(r.target.as_str()), V::from(r.peeled.clone()), V::from(now_ms())])?;
            }
            d.q("UPDATE meta SET value=? WHERE key='head'", vec![V::from(out.head.as_str())])?;
            d.q("UPDATE meta SET value='importing' WHERE key='fork_state'", vec![])?;
            Ok(SliceOutcome::Continue { cursor: ":".into() })                 // sha "", pack ""
        }
        "importing" => {
            let (sha, pack) = job.cursor.as_deref().unwrap_or(":").split_once(':').unwrap_or(("", ""));
            let page: PageOut = parent_json(d, "/_do/fork/page", &serde_json::json!(
                { "fork_repo": d.meta("repo_id")?, "sha": sha, "pack": pack }), budget).await?;
            for r in &page.rows {            // sync span; the EXISTS guard imports only registered borrowed packs
                d.q("INSERT OR IGNORE INTO objects(sha,pack_id,idx,offset,len,kind,size) \
                     SELECT ?,?,?,?,?,?,? WHERE EXISTS(SELECT 1 FROM borrowed WHERE pack_id=?)",
                    vec![V::from(r.sha.as_str()), V::from(r.pack_id.as_str()), V::from(r.idx), V::from(r.offset),
                         V::from(r.len), V::from(r.kind), V::from(r.size), V::from(r.pack_id.as_str())])?;
            }
            match page.cursor {
                Some(c) => Ok(SliceOutcome::Continue { cursor: c }),
                None => { d.q("UPDATE meta SET value='done' WHERE key='fork_state'", vec![])?; Ok(SliceOutcome::Done) }
            }
        }
        _ => {   // "done": drain the unpin queue this repo's GcSweep left for detached packs; Err retries (4.4)
            let pend = d.q("SELECT u.pack_id FROM fork_unpins u \
                            WHERE NOT EXISTS(SELECT 1 FROM pack_pins p WHERE p.pack_id=u.pack_id) LIMIT 90",
                           vec![])?.to_array::<IdRow>()?;
            if pend.is_empty() { return Ok(SliceOutcome::Done); }
            let ids: Vec<&str> = pend.iter().map(|r| r.pack_id.as_str()).collect();
            parent_json::<serde_json::Value>(d, "/_do/fork/unpin", &serde_json::json!(
                { "fork_repo": d.meta("repo_id")?, "packs": ids }), budget).await?;
            for r in &pend { d.q("DELETE FROM fork_unpins WHERE pack_id=?", vec![V::from(r.pack_id.as_str())])?; }
            Ok(SliceOutcome::Continue { cursor: String::new() })
        }
    }
}

/// The only DO this module ever calls is the direct parent: every pin a fork holds lives in its parent's
/// pack_pins table, even pins covering grandparent-owned packs, so protection is transitive down the chain.
/// Headers carry the parent's owner/repo -- its boot verifies them (8.2). One subrequest per call (7.1).
async fn parent_json<T: serde::de::DeserializeOwned>(d: &RepoDo, path: &str, body: &impl serde::Serialize,
    budget: &mut SliceBudget) -> Result<T, Error> {
    budget.subrequests_used = budget.subrequests_used.saturating_add(1);
    let parent = d.meta("parent")?;
    let stub = d.env().durable_object("REPO")?.id_from_name(&parent)?.get_stub()?;
    let text = serde_json::to_string(body).map_err(|e| Error::Internal(e.to_string()))?;
    let mut req = Request::new_with_init(&format!("https://do{path}"),
        RequestInit::new().with_method(Method::Post).with_body(Some(text.into())))?;    // unverified at runtime
    RepoHeaders::for_name(&parent)?.apply(&mut req)?;
    let mut resp = stub.fetch_with_request(req).await?;
    if resp.status_code() != 200 { return Err(Error::from_do_response(resp).await); }
    resp.json::<T>().await.map_err(|e| Error::Internal(format!("parent {path}: {e}")))
}

// ---- the foundation edits (REGISTRY), all inside spans that already exist: ----
// jobs::gc_sweep (5.3):  UPDATE packs SET state='dead', dead_at=? WHERE id IN (SELECT value FROM json_each(?))
//                          AND id NOT IN (SELECT pack_id FROM pack_pins);        -- pin check inside the span
//   fork, same span:     INSERT OR IGNORE INTO fork_unpins(pack_id) SELECT pack_id FROM borrowed
//                          WHERE pack_id IN (SELECT value FROM json_each(?));
//                        DELETE FROM borrowed WHERE pack_id IN (SELECT value FROM json_each(?));
//                        jobs::enqueue(sql, JobKind::Fork, now, "{}");           -- dispatcher rearms (A3)
//   The Janitor's later delete of r/<this repo>/packs/<borrowed id> is a no-op (2.2: never written here).
// jobs::gc_mark candidate query (5.1), in a fork:  ... AND id NOT IN (SELECT pack_id FROM borrowed WHERE released=0)
// pack::generate::write_pack (9.6): per pack, `d.owner_of(pack)` then `bucket.for_repo(RepoId(owner))`.
// pack::ingest: /_do/push/lookup returns {loc, owner} per id; bases are grouped by owner for read_entries (7.2).
// push_begin / fetch_v2_entry, first line: if meta_opt("fork_state") is Some(s != "done") -> Conflict("fork is importing").
// src/store/bucket.rs: impl Bucket { pub fn for_repo(&self, repo: RepoId) -> Self { Self { inner: self.inner.clone(), repo } } }
```

## Why it works
- **The snapshot is consistent because the pin unit is the pack.** `fork_parent_begin` pins every live pack and returns that same set in one sync span; `fork_parent_page` then serves only `objects` rows joined to those pins. A live pack's row set is frozen (2.3: rows are inserted only while `ingesting`, deleted only by a sweep that can no longer touch a pinned pack), so the imported index is exactly the parent's live object set at begin — a later parent push lands in a new pack the page query cannot return, and a sweep cannot remove what was returned. The copied refs resolve entirely inside the pin set by the 2.5 closure invariant.
- **The cross-repo GC blocker closes inside the parent's own span, not by cross-DO trust.** The pin insert and the `NOT IN pack_pins` dead-mark are sync spans in the same DO; they serialize (platform-facts #4), so no interleaving yields a collected pack that a fork still resolves. The guarantee is only as durable as the parent's SQLite — the same assumption the foundation already makes for `refs`.
- **Fork-of-fork flattens without a chain walk.** `owner_repo` is carried through `borrowed` in every generation, so a grandchild range-reads `r/<grandparent_repo>/packs/...` directly. The pin that protects it lives in the *direct* parent's `pack_pins` keyed by pack_id; an intermediate fork cannot finish releasing its own borrow while a child's pin stands, because its sweep's `NOT IN` covers borrowed rows and its unpin drain re-checks `pack_pins`. Each DO only ever talks to its direct parent.
- **Detach is the foundation's own GC chain.** `released=1` makes borrowed packs ordinary candidates; `GcConsolidate` copies their marked entries verbatim through `read_entries`/`PackWriter` (5.2, 7.2) into a fork-owned pack that is `live` before the sweep drops the borrowed rows (duplicate live rows are legal, 2.3); the sweep queues `fork_unpins` and enqueues `Fork`. An unpin that never lands leaves a retained pack — a leak, never a dangling read.
- **`gc_epoch` needs no extension.** The fork's own `GcSweep` bumps `meta.gc_epoch` (5.3) whether the dead packs were owned or borrowed, so a fork push that resolved a base in a since-swept pack is rejected with `ng <ref> gc ran during push, retry` (3 step 2) — the same machinery, nothing fork-specific.
- **The wire never changes.** `ls-refs`/`fetch`/`receive-pack` are untouched; the copied refs are the fork's advertisement, which is what makes client-side dedup work. `HEAD` is copied as `meta.head` and `peeled` rides along in `refs` (A5), so `unborn`/`symref-target` are correct (1.1 rule 6) — closing the review's interop finding.
- **Budgets.** Import: one stub subrequest and at most 20,000 row inserts per slice, ~100 slices for a 2M-object parent. Reads on a fork cost the same coalesced `read_entries` as a normal repo (7.2), grouped by owner — zero extra subrequests. The unpin drain sends at most 90 ids per call through `json_each` (A6).

## Changes from the first pass
| First-pass finding (quoted) | Kind | How addressed |
|---|---|---|
| "`gc-and-repack-alarm` as written uses roots = ref tips only, sweeps unmarked loose objects, and explicitly plans to delete loose objects once a pack "covers" them. The fork resolves *only* loose keys `${layer}/aa/rest`. After the parent's second GC, every inherited object the fork points at is gone from R2" | blocker | The contract already adopted the reviewer's second fix: objects live only inside normalized packs addressed as `(pack, offset, len)` (2.1, 2.2) and are read by range read. The remaining half — the parent must not collect what a fork resolves — is `pack_pins`: `GcSweep`'s dead-mark gains `AND id NOT IN (SELECT pack_id FROM pack_pins)` inside the same sync span that flips packs to `dead`, and `GcMark` never selects a pinned pack as a candidate. Residual (retention granularity) is in Known limits. |
| "`layer` is the mutable string `objects/<owner>/<repo>`. Rename or delete-and-recreate of `alice/repo` silently re-targets every fork row. Layer must be the parent DO id" | blocker | `borrowed.owner_repo` is the parent's `meta.repo_id` — the immutable 32-hex id of section 8, not a name; R2 keys are `r/<repo_id>/packs/<pack>` (2.2, 8.4), so a parent rename changes only `meta` rows. The two-proof key-layout disagreement is moot: one layout exists now. |
| "Pins cover ref tips at page 0 only; objects imported on later pages, or reached by a server-side "sync fork", are unpinned and sweepable" | blocker | The pin unit is the whole pack, taken over the entire live set in `begin`'s span; `page` joins `objects` to `pack_pins`, so only pinned-pack rows are ever imported and a post-begin parent push is simply invisible, never dangling. `/_do/fork/sync` re-pins the now-live set idempotently (`INSERT OR IGNORE` everywhere) and re-imports missing rows. |
| "Duplicate `POST /fork` races past the `exists` check (awaited stub call before the meta insert); second caller gets a UNIQUE-constraint 500" | caveat | The claim is `INSERT INTO meta ... VALUES('parent',?) ON CONFLICT DO NOTHING` plus the empty-repo check inside one sync span in the target's own DO; the loser gets `Conflict` -> 409 (A2). No awaited call precedes the claim. |
| "Parent pin obligation is a promise made by another DO; nothing in this DO can verify it. A parent that is deleted leaves the fork with a full index and no bytes." | caveat | Partially closed: the obligation is now the parent's own `WHERE` clause against its own tables — no cross-DO trust while the parent DO lives. Not closed: parent SQLite loss or out-of-band R2 deletion is undetectable by the fork until a read fails, and then it fails loud (`Storage` -> band-3 `ERR`, section 10), not silently. Repo deletion is out of scope (section 12). |
| "Fork clone of a large repo pays per-object GETs; fork should trigger its own `gc-and-repack-alarm` pack build" | caveat | Superseded by the contract: there are no per-object loose-key GETs at all — borrowed objects stream from the parent's pack through the same coalesced `read_entries` range reads as local objects (7.2), grouped per owner by `for_repo`. A fork clone costs what any clone costs. |
| "Cross-tenant read of another prefix is by design; a parent going private cannot revoke bytes already indexed" | caveat | Unchanged by design: one bucket, so a borrowed range read needs no credential. Multi-tenancy is out of scope for the foundation (section 12); the policy consequence is stated in Known limits. |
| "Import of a 2M-object parent is ~400 alarm ticks and ~2M SQLite row writes per fork; fork-of-fork repeats the whole cost since it flattens rather than shares" | caveat | Still O(N): ~100 slices at 20,000 rows and one stub subrequest each. Flattening is kept via `owner_repo` carried in the pin set, so reads stay O(1); the per-generation import cost is inherent to index-level CoW and is restated in Known limits. |
| "the fork advertises no HEAD and `git clone` of a fork warns `remote HEAD refers to nonexistent ref, unable to checkout`" | caveat (interop check) | `begin` returns `meta.head` and the fork writes it; refs are copied with `peeled` (A5); `unborn`/`symref-target` emission is `wire` (1.1 rule 6). |
| "`readObject` for a sha in the index returns `null` mid-`streamPack`, i.e. a truncated pack with a bad trailer -- `git fetch` fails with `fatal: early EOF`" (concurrency walk-through) | caveat | The failure mode is removed, not papered over: rows are written only for packs pinned at the owner, so a resolved sha's bytes cannot be collected. If bytes are ever missing anyway (parent storage lost out-of-band), `read_entries` returns `Err(Storage)`, which section 10 maps to a band-3 `ERR` mid-stream — an explicit failure, not a silent truncation. |

## Known limits
- **What the contract cannot close — retention granularity.** The pin is the whole pack, dead weight included: a parent with forks reclaims nothing inside a pinned pack until every fork unpins or detaches. An abandoned fork pins its parent's live set forever; `forks`/`pack_pins` give ops the data to force-unpin, but there is no cross-DO liveness probe — the contract provides none. Object-granularity cross-repo refcounting would need O(objects) pin rows in the parent or the eager copy this idea exists to avoid; both rejected.
- **Parent durability is assumed, not verified.** If the parent DO's SQLite is wiped or `r/<parent_repo>/packs/*` is deleted out-of-band, the fork keeps rows for bytes that are gone; the next borrowed read fails `Storage` -> band-3 `ERR` (section 10). Loud, but not self-healing: the `Fork` job retries a dead parent until the job goes `dead` after 8 attempts (4.4), and `fork_sync`/`unpin` can never complete without the parent DO. `gc_epoch` cannot help here — it is per-repo and guards pushes, not borrowed reads.
- **No service during import.** `push_begin` and `fetch` return `Conflict` while `fork_state != 'done'`; a 2M-object import is ~100 alarm firings — minutes — and each fork generation pays it again. The first-pass's parent-probe fallback was dropped deliberately: partial-index reads on every path were the mechanism behind blocker 3.
- **Refs are a snapshot.** A parent push after `begin` is invisible until `/_do/fork/sync`, which re-pins and re-pages the whole pin set — idempotent but O(total pin set), not incremental.
- **Unverified bindings:** `Request::new_with_init` on the DO-to-DO path (same as two-phase-push), `worker::Bucket::clone` for `for_repo`, `SqlStorageValue: From<Option<String>>` for `peeled`.
- Scenarios this proof must pass: 2, 3, 5, 14. Added scenario "fork lifecycle": push commits to a parent, `POST /fork` via curl (stock git has no fork verb), fire the alarm until `fork_state='done'`, clone the fork — `fsck` clean and HEAD checks out; push a new commit to the fork; then force-push and GC the parent past GRACE and clone the fork again — still `fsck` clean (pins held). Added scenario "detach": `POST /fork/detach`, run the GC chain and the `Fork` job, assert the parent's `pack_pins` is empty and the fork clone still passes `fsck`.

## Depends on
- refs-sqlite-objects-r2
- two-phase-push
- repo-do-ref-authority
- streaming-pack-parser
- the section-5 GC chain (`gc-and-repack-alarm` as specified in CONTRACTS.md, carrying the two pin-aware edits listed in the REGISTRY)
