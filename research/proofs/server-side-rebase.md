# Server-side rebase and squash as protocol v2 extensions

> Second pass · Idea #18 · verdict: **lands with caveats** · feasibility 4/5 · reliability 4/5 · correctness 3/5 (first pass 4/2/2)
> First pass: [proof](./archived/server-side-rebase.md) · [review](../reviews/archived/server-side-rebase.md) · Second pass: [review](../reviews/server-side-rebase.md) · Contract: [CONTRACTS.md](../../CONTRACTS.md)

## Mechanism
The first-pass shape survives: two extra v2 capability lines (`rebase`, `rebase-status`), a `command=rebase` / `command=rebase-status` pair on `POST /o/r/git-upload-pack`, a Durable Object that replays `merge-base..branch` as three-way tree merges, and a CAS flip of the branch guarded by `expect`. Everything underneath is rewritten to the contract. There are no loose objects (2.1): replay output is one normalized pack `r/<repo>/packs/<pack>.pack` written by `PackWriter` with `objects` rows inserted per slice and a `packs` row `ingesting` with `push_id` NULL (the gc_consolidate precedent, 2.3); a terminal sync span flips it `live` in the same span as the ref CAS (section 3 ordering, mirrored). The rebase engine runs inside the DO — it already owns the `jobs` dispatcher, the ref tables, and `pack::generate`-side R2 access — as `JobKind::Rebase` slices of at most 20 s / 400 subrequests (4.3, A4), with the durable cursor on a `rebase_jobs` row rather than in a private `setAlarm`. The edge authenticates with `can_write` (section 12), parses the extended command with `wire::parse_v2_command` (body <= 1 MiB, 6.3), posts a JSON DTO to `/_do/rebase`, and frames the JSON result with `PktWriter` — flush-terminated, never `response-end` over HTTP (1.1 rules 1 and 3). `/_do/rebase` runs one inline slice so a small rebase answers `ack <oid>` in-request; longer ones answer `pending` + job id and the client polls `rebase-status`. The merge is tree-level only: `gix-merge` 0.20.1 is not in the pinned crate set, is absent from gitoxide's wasm CI, has no `wasm` feature, and `blob::Platform` hard-depends on `gix_filter`/`gix_worktree` (memo section 3) — so a blob both sides changed is a `conflict`, not a merge. The advertisement and command additions are wire write-backs, listed in the REGISTRY block; no framing rule of 1.1 changes, and stock `git` provably ignores the two unknown capability lines (first-pass review, verified against git 2.4x).

## Primitives
- `SqlStorage::exec` synchronous, `SqlCursor::{to_array, one}`, `SELECT changes()` as the CAS oracle: verified (spike), measured 1 / 0 / 1 (platform-facts #1). Sync-span atomicity of the terminal `live`+CAS span: measured (#4).
- `jobs::{enqueue, rearm, dispatch, run_slice}`, `SliceOutcome`, `SliceBudget` (with `req: ReqBudget`, as in bundle-uri): contract 4 + A3/A4. Second `setAlarm` cancels the first: measured (#5); this module never calls it.
- `Stub::fetch_with_request` + `Request::new_with_init` JSON round-trip (`stub_json`): verified shape (memo 1); the constructor path is **unverified at runtime**, as in every sibling proof.
- `Bucket::read_entries` coalesced range reads (7.2) and `read_range` of commit regions `[commit_lo, commit_hi)` (7.4): contract; real-R2 behaviour measured on the local simulator only (#6).
- `PackWriter::{create, append_entry, flush_if_full, finish, abort}`: contract 1.2 + A1 (`append_entry` returns `Result`). `PackWriter::resume` over `Bucket::resume_multipart_upload`: binding verified in source (memo 1), `PackWriter` signature is a write-back (section 5's GcConsolidate needs the same). Re-uploading an already-uploaded part number with identical bytes before `complete`: allowed by the S3 multipart model, **unverified** against real R2 — safe here only because regenerated entry bytes are deterministic (Why it works).
- `gix_object::TreeRefIter` / `CommitRefIter::from_bytes(..).parent_ids()` / `compute_hash`: memo section 3 + contract section 9 usage; exact field names (`mode`, `filename`, `oid`) per 0.64.1 source, **pinned at compile time**. `gix-actor` is *not* needed: the author line is byte-copied out of the source commit.
- Tree serialization hand-encoded (`<mode octal> <name>\0<20 raw oid>` per entry, directories sorted as `name/`): no gix-object encode dependency; `compute_hash(Kind::Tree, ..)` supplies the id.
- `gix_validate::reference::name_partial` (0.11.4): verified. `web_sys::Crypto` random for `PackId`: binding path unverified (8.2). `js_sys::Date::now`: standard.
- `gix-merge`/`gix-diff`/`imara-diff`: **not used** — none is in the pinned crate list (CONTRACTS.md preamble), and gix-merge does not build for wasm32 (memo 3). Blob-level merge is the vendored 926-line `gix-merge` text driver (imara_diff + bstr only), a named later dependency.

## Proof code
```rust
// src/edge/rebase.rs + src/repo_do/rebase.rs + src/jobs/rebase.rs.  CONTRACTS 1.1, 1.3, 2.1-2.5, 3, 4, 7, 9; A1-A10.
// `q`, `changes`, `oid`, `json`, `now_ms`, `sql`, `bucket()` are the RepoDo helpers of repo-do-ref-authority;
// `stub_json`, `RepoRoute`, `Principal` are the edge helpers of two-phase-push; DTOs live in wire::http (A8).
//
// REGISTRY (A9) -- every addition over the foundation lists of 1.3/1.4:
//   DO routes:  /_do/rebase         JSON RebaseDto -> RebaseResult   awaits: rearm + one inline slice (/_do/fetch precedent)
//               /_do/rebase-status  JSON {job} -> RebaseResult       awaits: rearm
//   JobKind::Rebase -> run_slice arm run_rebase_slice (below); enqueue dedup by kind is safe because the runner
//     drains the whole rebase_jobs table, not one payload.
//   table: CREATE TABLE rebase_jobs (id TEXT PRIMARY KEY, branch TEXT NOT NULL, onto TEXT NOT NULL,
//     expect TEXT NOT NULL, squash INTEGER NOT NULL, message TEXT, author TEXT, principal TEXT NOT NULL,
//     state TEXT NOT NULL /* running|done|conflict|stale|failed */, cursor TEXT NOT NULL DEFAULT '{}',
//     result TEXT, created_at INTEGER NOT NULL, ended_at INTEGER) WITHOUT ROWID;   -- cursor: JSON of Cursor
//   wire write-backs: advertisement gains `rebase`, `rebase-status` lines (1.1 rule 6); V2Command gains
//     Rebase(RebaseArgs { onto, branch, expect, squash, message, author }) and RebaseStatus { job }.
//   no new R2 key prefixes: output is a normal pack under r/<repo>/packs/ (2.2); packs.push_id stays NULL (2.3).
use bstr::{BStr, ByteSlice, BString};
use gix_hash::ObjectId;
use gix_object::{CommitRefIter, Kind, TreeRefIter};
use std::collections::{BTreeMap, VecDeque};
use worker::{Response, SqlStorageValue as V, Stub};
use crate::{error::Error, jobs::{self, Job, JobKind, SliceBudget, SliceOutcome}, repo_do::RepoDo,
            store::{codec, keys, Bucket, Index, MemFind, ObjRow, PackId, PackMeta, PackWriter}, wire, ReqBudget};
const REPLAY_MAX: usize = 5_000;                 // cursor.todo JSON <= ~205 KiB on the row; beyond -> Limit -> 'failed'
const MEM_MAX: usize = 64 << 20;                 // loaded-object ceiling per slice, the section-9 bound
const TREE: u32 = 0o40000;
#[derive(Clone, Copy, PartialEq)] struct E { mode: u32, id: ObjectId }           // one tree entry; name lives in the parent
#[derive(serde::Serialize, serde::Deserialize, Default)] #[serde(default)]
struct Cursor { head: Option<ObjectId>, base: Option<ObjectId>, todo: VecDeque<ObjectId>,
                upload: Option<String>, parts: Vec<String>, pos: u64, pending: Option<(ObjectId, u64)> }
// pending = the entry straddling pos: (source commit, bytes already uploaded). Regenerated identically on resume.
#[derive(serde::Deserialize)] struct Row { id: String, branch: String, onto: String, expect: String, squash: i64,
    message: Option<String>, author: Option<String>, principal: String, created_at: i64,
    pack_id: Option<String>, cursor: String }
#[derive(serde::Deserialize)] struct TargetRow { target: String }
#[derive(serde::Deserialize)] struct IdRow { id: String }
#[derive(serde::Deserialize)] struct StateRow { state: String, result: Option<String> }

// ---- edge: upload-pack dispatcher arm; the request already carried Git-Protocol: version=2 (1.1 rule 6 context). ----
pub async fn rebase(stub: &Stub, repo: &RepoRoute, who: &Principal, a: &wire::RebaseArgs, budget: &mut ReqBudget)
    -> Result<Response, Error> {
    if !who.can_write { return Err(Error::Forbidden); }                          // same gate as receive-pack (12)
    let r: RebaseResult = stub_json(stub, repo, "/_do/rebase", &RebaseDto::of(a, &who.name), budget).await?;
    let mut w = wire::PktWriter { out: Vec::new() };
    match r.state.as_str() {
        "done"     => w.text(&format!("ack {}\n", r.tip.as_deref().unwrap_or_default()))?,
        "conflict" => { w.text("conflict\n")?; w.delim(); for p in &r.paths { w.text(&format!("path {p}\n"))?; } }
        "running"  => { w.text("pending\n")?; w.delim(); w.text(&format!("job {}\n", r.id))?; }
        s          => w.text(&format!("{s}\n"))?,                                 // stale | failed
    }
    w.flush();                                                                    // never response-end over HTTP (1.1 rule 3)
    let mut resp = Response::from_bytes(w.out).map_err(|e| Error::Internal(e.to_string()))?;
    resp.headers_mut().set("content-type", "application/x-git-upload-pack-result").map_err(|e| Error::Internal(e.to_string()))?;
    Ok(resp)
}

// ---- DO routes. Every mutation below is inside a sync span; awaits come only after enqueue (A3). ----
impl RepoDo {
    pub async fn rebase_begin(&self, b: &RebaseDto) -> Result<Response, Error> {
        let (onto, expect) = (oid(&b.onto)?, oid(&b.expect)?);                    // parse fully before storage (3)
        if !b.branch.starts_with("refs/heads/")
            || gix_validate::reference::name_partial(b.branch.as_bytes().as_bstr()).is_err() {
            return Err(Error::Protocol("bad ref name".into())); }
        let tip = self.q("SELECT target FROM refs WHERE name=?", vec![V::from(b.branch.as_str())])?
            .to_array::<TargetRow>()?.into_iter().next();
        if tip.map(|t| t.target).as_deref() != Some(b.expect.as_str()) {          // advisory; the terminal CAS is authoritative
            return json(RebaseResult::stateless("stale"));
        }
        let id = PackId::random().0;                                              // job id == pack id: one name, one row
        self.q("INSERT INTO rebase_jobs(id,branch,onto,expect,squash,message,author,principal,state,created_at) \
                VALUES(?,?,?,?,?,?,?,?,'running',?)",
               vec![V::from(id.as_str()), V::from(b.branch.as_str()), V::from(onto.to_string()), V::from(expect.to_string()),
                    V::from(i64::from(b.squash)), V::from(b.message.as_deref()), V::from(b.author.as_deref()),
                    V::from(b.principal.as_str()), V::from(now_ms())])?;
        jobs::enqueue(&self.sql(), JobKind::Rebase, now_ms(), "{}")?;             // sync, dedup by kind (4.5)
        jobs::rearm(self).await?;                                                 // the only set_alarm caller (4.1, A3)
        let mut sb = SliceBudget::fresh();                                        // 20 s / 400 subrequests (4.3)
        let _ = jobs::rebase::step(self, &id, &mut sb).await;                     // inline slice; Err stays resumable via the row
        let row = self.q("SELECT state,result FROM rebase_jobs WHERE id=?", vec![V::from(id.as_str())])?.one::<StateRow>()?;
        json(RebaseResult::of(&id, &row))
    }
    /// A poll is also the liveness nudge: enqueue dedups, rearm re-arms. Closes the first-pass stuck-job blocker.
    pub async fn rebase_status(&self, b: &StatusDto) -> Result<Response, Error> {
        let row = self.q("SELECT state,result FROM rebase_jobs WHERE id=?", vec![V::from(b.job.as_str())])?.one::<StateRow>()?;
        if row.state == "running" { jobs::enqueue(&self.sql(), JobKind::Rebase, now_ms(), "{}")?; }
        jobs::rearm(self).await?;
        json(RebaseResult::of(&b.job, &row))
    }
}

// ---- JobKind::Rebase slice (4.2). One slice = one rebase; Continue re-fires immediately for the next row. ----
pub async fn run_rebase_slice(d: &RepoDo, _job: &Job, budget: &mut SliceBudget) -> Result<SliceOutcome, Error> {
    let next = d.q("SELECT id FROM rebase_jobs WHERE state='running' ORDER BY created_at LIMIT 1", vec![])?
        .to_array::<IdRow>()?.into_iter().next();
    let Some(row) = next else { return Ok(SliceOutcome::Done) };
    step(d, &row.id, budget).await?;                                              // Err -> 4.4 retry/backoff; row stays resumable
    let more = d.q("SELECT id FROM rebase_jobs WHERE state='running' LIMIT 1", vec![])?.to_array::<IdRow>()?;
    Ok(if more.is_empty() { SliceOutcome::Done } else { SliceOutcome::Continue { cursor: String::new() } })
}

/// Resume or start the rebase's pack; replay until done / conflict / 80% spent (A4). All mid-run state is the
/// rebase_jobs cursor; the pack row stays 'ingesting' (invisible to the 2.3 reader query) until commit_rebase.
pub async fn step(d: &RepoDo, id: &str, budget: &mut SliceBudget) -> Result<(), Error> {
    let r: Row = d.q("SELECT * FROM rebase_jobs WHERE id=? AND state='running'", vec![V::from(id)])?.one()?;   // sync
    let (bucket, mut mem) = (d.bucket()?, MemFind::default());
    let mut cur: Cursor = serde_json::from_str(&r.cursor).map_err(|e| Error::Internal(e.to_string()))?;
    if cur.head.is_none() {                                                       // first slice: merge-base + todo by rounds (9)
        let (onto, tip) = (oid(&r.onto)?, oid(&r.expect)?);
        let base = merge_base(d, &bucket, &mut mem, onto, tip, budget).await?;    // commit-region reads (7.4); Limit if not live
        cur.base = Some(base);
        if r.squash == 1 { cur.todo = VecDeque::from([tip]); }                    // squash: one merge, no per-commit replay
        else { cur.todo = rev_list(&mem, base, tip)?; }                           // oldest first; merge commits dropped (git default)
        if cur.todo.len() > REPLAY_MAX { return fail(d, &r, "rebase too long"); }
        cur.head = Some(onto);                                                    // empty todo -> fast-forward CAS onto `onto`
    }
    let pack = PackId(r.pack_id.clone().unwrap_or_else(|| PackId::random().0));
    let key = keys::pack(&bucket.repo, &pack);
    let mut out = match cur.upload.clone() {                                      // deterministic resume, Why it works
        Some(u) => PackWriter::resume(&bucket, &key, u, &cur.parts, cur.pos, &mut budget.req).await?,
        None => { insert_pack_row(d, &r, &pack)?; PackWriter::create(&bucket, &key, 0, &mut budget.req).await? }
    };
    let (mut rows, mut conf): (Vec<ObjRow>, Vec<String>) = (Vec::new(), Vec::new());
    while let Some(src) = cur.todo.pop_front() {
        if budget.spent_80pct() { cur.todo.push_front(src); return checkpoint(d, &r, &pack, &cur, &rows, &mut out).await; }
        replay_one(d, &bucket, &mut mem, &r, &mut cur, src, &mut out, &mut rows, &mut conf, budget).await?;
        if !conf.is_empty() { return conflict(d, &r, &pack, out, &conf).await; }    // terminal: dead pack + paths on the row
        if rows.len() >= 10_000 { Index(&d.sql()).insert_objects(&pack, &rows)?; rows.clear(); }   // 1.2 row cap, sync
    }
    out.flush_if_full(&mut budget.req).await?;
    let meta = out.finish(&mut budget.req).await?;                                // pack durable in R2 before any 'live' row (3)
    Index(&d.sql()).insert_objects(&pack, &rows)?;
    let head = cur.head.ok_or_else(|| Error::Internal("no head".into()))?;
    commit_rebase(d, &r, &pack, &meta, head, now_ms())                            // sync terminal span, below
}

/// Sync 3-way tree merge over MemFind (9). Both-changed non-tree paths go to `conf` — there is no blob merge on
/// wasm (Primitives) — which is conservative but never wrong. Missing trees push to `need`; the caller prefetches
/// coalesced (7.2) and re-runs, the section-9 rounds shape.
fn merge_trees(mem: &MemFind, base: Option<E>, ours: Option<E>, theirs: Option<E>, path: &str,
               out: &mut PackWriter, rows: &mut Vec<ObjRow>, conf: &mut Vec<String>, need: &mut Vec<ObjectId>)
    -> Result<Option<E>, Error> {
    if ours == theirs { return Ok(ours); }                                        // identical both sides (incl. both absent)
    if ours == base { return Ok(theirs); }                                        // only theirs changed
    if theirs == base { return Ok(ours); }                                        // only ours changed
    if ![base, ours, theirs].into_iter().flatten().all(|e| e.mode == TREE) {      // delete/modify, type change, blob/blob:
        conf.push(path.to_string()); return Ok(ours); }                           //   value unused: the rebase aborts
    let triples = union(mem, base, ours, theirs, need)?;                          // per-child triples across the three trees
    if !need.is_empty() { return Ok(ours); }                                      // caller prefetches `need`, re-runs (9 rounds)
    let mut merged: Vec<(BString, E)> = Vec::new();
    for (name, b, o, t) in triples {
        if let Some(e) = merge_trees(mem, b, o, t, &format!("{path}/{name}"), out, rows, conf, need)? {
            merged.push((name, e));
        }
    }
    write_tree(merged, out, rows).map(Some)
}

/// Git-ordered serialization (directories sort as "name/"), then the 2.1 entry shape via PackWriter.
fn write_tree(mut es: Vec<(BString, E)>, out: &mut PackWriter, rows: &mut Vec<ObjRow>) -> Result<E, Error> {
    es.sort_by(|a, b| sort_name(&a.0, a.1).cmp(sort_name(&b.0, b.1)));            // sort_name appends '/' when mode == TREE
    let mut data = Vec::new();
    for (name, e) in &es { data.extend(format!("{:o} ", e.mode).as_bytes()); data.extend(name.as_slice()); data.push(0); data.extend(e.id.as_slice()); }
    let id = gix_object::compute_hash(Kind::Tree, &data).map_err(|e| Error::Internal(e.to_string()))?;
    let (offset, len) = out.append_entry(Kind::Tree, &data)?;                     // A1: Result; varint hdr + zlib body (2.1)
    rows.push(ObjRow::new(id, offset, len, Kind::Tree, data.len() as u64));
    Ok(E { mode: TREE, id })
}

/// One replayed commit: ORIGINAL author header line byte-copied, committer = principal stamped at job.created_at
/// (deterministic across resumes), message = source message verbatim or the squash `message` arg. gpgsig is dropped.
fn write_commit(src: &[u8], tree: ObjectId, parent: ObjectId, r: &Row, msg: Option<&[u8]>,
                out: &mut PackWriter, rows: &mut Vec<ObjRow>) -> Result<ObjectId, Error> {
    let author = header_line(src, b"author ").ok_or_else(|| Error::Internal("no author".into()))?;
    let body = [format!("tree {tree}\nparent {parent}\n").into_bytes(), author.to_vec(),
                format!("committer {} <rebase@git-edge> {} +0000\n\n", r.principal, r.created_at / 1000).into_bytes(),
                msg.map(<[u8]>::to_vec).unwrap_or_else(|| commit_message(src))].concat();
    let id = gix_object::compute_hash(Kind::Commit, &body).map_err(|e| Error::Internal(e.to_string()))?;
    let (offset, len) = out.append_entry(Kind::Commit, &body)?;
    rows.push(ObjRow::new(id, offset, len, Kind::Commit, body.len() as u64));
    Ok(id)
}

/// Terminal sync span — section 3 steps 3-7 reduced to one ref. `finish` returned before this runs, so at no
/// instant does a ref name a non-live pack. On CAS loss the pack goes 'dead' here and the Janitor's 5.3 sweep
/// removes the key after GRACE; 'ingesting' rows of a rebase that can never finish go through the same path.
fn commit_rebase(d: &RepoDo, r: &Row, pack: &PackId, meta: &PackMeta, head: ObjectId, now: i64) -> Result<(), Error> {
    d.q("UPDATE packs SET state='live',count=?,bytes=?,commit_lo=?,commit_hi=? WHERE id=? AND state='ingesting'",
        vec![V::from(meta.count), V::from(meta.bytes), V::from(meta.commit_lo), V::from(meta.commit_hi), V::from(pack.0.as_str())])?;
    if d.changes()? != 1 { return Err(Error::Internal("rebase pack not ingesting".into())); }
    d.q("UPDATE refs SET target=?,updated_at=? WHERE name=? AND target=?",
        vec![V::from(head.to_string()), V::from(now), V::from(r.branch.as_str()), V::from(r.expect.as_str())])?;
    if d.changes()? == 1 {
        d.q("INSERT INTO reflog(name,old,new,push_id,principal,at) VALUES(?,?,?,?,?,?)",
            vec![V::from(r.branch.as_str()), V::from(r.expect.as_str()), V::from(head.to_string()),
                 V::from(format!("rebase:{}", r.id)), V::from(r.principal.as_str()), V::from(now)])?;
        d.q("UPDATE meta SET value=value+1 WHERE key='refs_version'", vec![])?;
        jobs::enqueue(&d.sql(), JobKind::GcMark, now + 600_000, "{}")?;           // 3 step 7, dedups
        d.q("UPDATE rebase_jobs SET state='done',result=?,ended_at=? WHERE id=?",
            vec![V::from(format!("{{\"tip\":\"{head}\"}}")), V::from(now), V::from(r.id.as_str())])?;
    } else {                                                                      // branch moved mid-replay
        d.q("DELETE FROM objects WHERE pack_id=?", vec![V::from(pack.0.as_str())])?;
        d.q("UPDATE packs SET state='dead',dead_at=? WHERE id=?", vec![V::from(now), V::from(pack.0.as_str())])?;
        d.q("UPDATE rebase_jobs SET state='stale',ended_at=? WHERE id=?", vec![V::from(now), V::from(r.id.as_str())])?;
    }
    Ok(())
}
```

## Why it works
- **Wire legality.** The two capability lines extend the rule-6 list only; every framing rule of 1.1 stands: all output goes through `PktWriter` (length prefix counts itself, `0001` delim, flush terminates, `response-end` never written over HTTP), and unknown capability lines are ignored by stock `git` — verified in the first-pass review against git 2.4x `ls-remote`/`fetch`. `command=rebase` reuses the v2 command envelope (capability lines, delim, arguments, flush) that `parse_v2_command` already owns; the write-back adds two `V2Command` variants, and unknown or malformed arguments still map to `Error::Protocol` -> HTTP 400 before any response byte (section 10, spike correction 4). A stock client can never emit the command, so the new code path is unreachable for it — the same argument the review accepted.
- **Serialization is the same CAS as `git push`.** The terminal span runs `UPDATE refs ... WHERE name=? AND target=expect` with `SELECT changes()` as oracle (3 step 4; measured platform-facts #1, #4), flips the pack `live`, writes the reflog, bumps `refs_version` and enqueues `GcMark` — one sync span, so pushes, other rebases and `GcSweep` cannot interleave inside it. A push landing mid-replay makes the CAS write 0 and the rebase reports `stale`, never split-brain; a rebase landing first makes the push's CAS fail, the same outcome as two racing pushes (scenario 6). The brief "moving `onto` tip" is deliberately *not* implemented: `onto` is a client-resolved oid pinned at insert, and the result is always a descendant of that pinned base — `git rebase` onto a stale base is legal, and the client's `fetch`+`reset` hides it exactly as before (documented per the review's "or document it").
- **Connectivity holds by construction, not by a separate pass.** Merged trees only ever reference (a) ids copied out of trees loaded through the `state='live'` reader query — live children of live objects, by the 2.5 invariant — or (b) ids appended to the same pack; commits parent only onto `onto` (live ref target) or earlier replayed commits in the same pack. The pack flips `live` only in the terminal span where the ref tip — its own last commit — is checked into `refs` at once, so the 2.5 invariant is preserved without an `extract_links` pass.
- **The stuck-job hole is closed three ways.** `/_do/rebase` and `/_do/rebase-status` both run `enqueue` + `rearm().await` (A3), so every request and every poll re-arms the dispatcher; `run_rebase_slice` drains the `rebase_jobs` table and returns `Continue` while rows remain, which 4.2 schedules on the very next firing — the first pass's `LIMIT 1` starvation cannot occur; and an `Err` out of `step` takes the 4.4 path (attempts+1, exponential backoff, `dead` after 8) with the row still resumable. A `dead` Rebase jobs-row does not block enqueues (dedup counts only `queued`/`running`), so the next request revives the work; the Janitor write-back additionally fails `running` rows older than `PUSH_TIMEOUT` so an abandoned job cannot pin an `ingesting` pack forever.
- **Crash resume is exact because objects are deterministic.** Committer timestamp is `created_at`, the author line is byte-copied, and `merge_trees`/`write_tree` are pure functions of `mem`, so a re-run slice regenerates byte-identical entry bytes; `PackWriter::resume` continues the multipart upload at `cursor.pos`, re-uploading a part number that may already exist is safe under R2's overwrite-before-complete semantics (Primitives: unverified on real R2), and `cursor.pending` records the mid-entry straddle so the tail of a split entry is re-appended. A crash mid-await rolls back only the uncommitted span (3); the `ingesting` pack, posted rows and cursor checkpoint are all durable or all absent. The first pass's `Date.now()` oid-drift orphan class is gone by construction.
- **Squash is a branch, not a list element.** `squash=1` sets `todo = [tip]` with `head = onto` and the merge triple is `merge_trees(base, onto_tree, tip_tree)` — one merge, one commit, `message` arg or a default line — which is exactly the `mergeTrees(tree(base), tree(onto), tree(tip))` the review prescribed. Merge commits inside `base..branch` are skipped by `rev_list`, matching `git rebase`'s default linearization (no `--rebase-merges`).
- **Budgets.** One replayed commit costs about one coalesced `read_entries` (7.2) plus amortized part uploads; `spent_80pct` is checked between commits (A4) against the 20 s / 400-subrequest slice (4.3), and the inline slice in `/_do/rebase` uses the same `SliceBudget` so the request returns `pending` instead of overrunning. `REPLAY_MAX = 5,000` keeps the JSON cursor under ~205 KiB and caps a rebase at roughly 50 slices worst-case; `mem` is bounded by `MEM_MAX` (the section-9 bound) and trees are capped at 16 MiB inflated by A7. Memory inside a slice: one window + `MemFind` + one entry buffer, far under 128 MB.
- **Error policy (10, A2).** Bad arguments fail as `Protocol` -> 400 before response bytes; `Limit`/`Storage`/`Internal` inside a DO span propagate as `Err` out of `fetch` (A2) so the platform discards the span; the edge maps them per section 10. `Forbidden` is checked at the edge before the stub call, same as receive-pack.

## Changes from the first pass
| First-pass finding (quoted) | Kind | How addressed |
|---|---|---|
| "Squash path throws: `step()` treats `{squashOf, msg}` as a commit oid. Needs a separate branch: `mergeTrees(tree(base), tree(onto), tree(tip))` then one commit." | blocker | `squash == 1` sets `todo = [tip]`, `head = onto`; `replay_one` runs the single merge `merge_trees(base_tree, onto_tree, tip_tree)` and one commit with the `message` arg. No JSON blob ever reaches the commit loader. |
| "`rebase-status` is advertised but unimplemented, so every multi-step (`pending`) rebase is unobservable by the client." | blocker | `command=rebase-status` is a parsed `V2Command` variant routed to `/_do/rebase-status`, which reads the `rebase_jobs` row and returns `status running|done|conflict|stale|failed` plus a delim'd detail section (`tip`, `path`, `error`). |
| "`alarm()` `LIMIT 1` without re-arming when other `done=0` rows exist starves concurrent jobs; a thrown `step()` leaves a job stuck with no alarm." | blocker | No private alarm exists. `JobKind::Rebase` rides the shared dispatcher (4.2): `Continue` gets the next firing, `Err` gets 4.4 backoff, `rearm` follows every `enqueue` (A3), both client routes re-arm, and the runner drains the whole `rebase_jobs` table rather than one row. |
| "R2 key layout and body encoding (`repos/<doId>/objects/`, zlib'd) diverge from `content-addressed-r2-keys` ... so the fetch path may not serve the rebased commits." | blocker | Loose objects are gone. Output is a normalized pack under `r/<repo>/packs/` (2.1, 2.2) with `objects` rows and a `packs` row flipped `live` at CAS time; `send_set`/`fetch` serve it through the single 2.3 reader query like any pushed pack. |
| "No file-level merge until diff3 lands; until then most real rebases report `conflict`. Use `node-diff3`-style pure JS rather than waiting on the Wasm core." | caveat | Still a `conflict` on both-changed blobs — the honest bound: no merge crate exists in the pinned set and `gix-merge` does not build on wasm (memo 3). The Rust equivalent of node-diff3 is the vendored 926-line `gix-merge` text driver (imara_diff + bstr only); named as a dependency on `server-side-merge`, not claimed here. |
| "Original author/date/signature are discarded; pass `author` through as a command argument and copy it from the source commit by default." | caveat | `write_commit` byte-copies the source commit's `author ` header line (name, email, timestamp, tz); an optional `author` arg overrides it for squash. Committer is the authenticated principal — `git rebase` rewrites committer too. `gpgsig` headers are not copied (cannot re-sign server-side; Known limits). |
| "`onto` is pinned at job creation; a long replay lands on a stale base. Either re-read `tip(onto)` per step and restart, or document it." | caveat | Documented: `onto` is a client-resolved oid, pinned at insert; the result is always a descendant of that base, which is a legal rebase outcome. Re-reading per step was rejected because a restart would orphan the in-flight pack and a hot `main` could starve the job; clients wanting tip-freshness pass a fresh `ls-refs` oid and retry on `stale`-branch. |
| "Orphaned R2 objects from retried/lost jobs need a GC hook (mark objects reachable from refs, sweep `objects/` older than N days)." | caveat | The orphan class no longer exists: objects live only inside the one pack, which is `ingesting` (invisible) then `live` at a successful CAS or `dead` on conflict/stale — `dead` keys are the Janitor's existing 5.3 sweep. Retried slices regenerate identical bytes (deterministic committer), so no duplicate objects either. |
| "Hot-branch push latency degrades while a job runs on the same DO; the 40-commit step budget should be time-based (e.g. 5 s wall clock), not count-based." | caveat | Replaced by `SliceBudget::spent_80pct` checked between commits (A4): 20 s wall clock and 400 subrequests per slice, and every R2 await inside a slice opens the input gate so pushes interleave rather than queue behind a fixed commit count. |
| "the DO serializes the operation with every push is overstated. DO input gates release on non-storage awaits" | caveat | Correct, and now load-bearing: mid-replay mutation is confined to `rebase_jobs` + the `ingesting` pack (invisible to the 2.3 query), and only the terminal sync span touches `refs`/`packs(live)`/`reflog`/`meta`. Interleaving is safe because the span is atomic (measured #4). |
| "merge commits in `revList` are cherry-picked against `parentTree(c)` ... with no stated policy" | caveat | Policy stated: `rev_list` drops merge commits — `git rebase`'s default; `--rebase-merges` is out of scope (Known limits). |
| "writeCommit stamps `Date.now()`, so a retried step mints a different oid and the earlier commit/tree objects are orphaned" | caveat | Committer timestamp is `rebase_jobs.created_at`, fixed for the job; regeneration is byte-identical, and resume re-uploads the same part numbers rather than writing new objects. |
| "a v0/v1 client ... falls through to 404" / "the proof's dispatcher ignores both [Content-Type and Git-Protocol] headers" | caveat | Addressed by the contract, not this module: upload-pack dispatch is inside the foundation's version-checked router (1.1 rules 6-8; `protocol-v2-only` proof), which requires `Git-Protocol: version=2` before `parse_v2_command` ever sees the body. |

## Known limits
- **No blob-level merge.** A path both sides changed at blob granularity reports `conflict` — correct but conservative, and the dominant real-world limit. The upgrade path is the vendored `gix-merge` 0.20.1 text driver (926 lines, imports only `imara_diff` + `bstr`, memo 3) behind `server-side-merge`; `merge_trees` would then call it where `conf.push` now fires. Rename detection is likewise absent (git's merge-ort feature).
- **Write-backs proposed to CONTRACTS.md**, each small: advertisement gains `rebase`, `rebase-status` (rule 6; the "exactly" list is extended, framing rules unchanged); `V2Command` gains `Rebase`/`RebaseStatus` in `parse_v2_command`; `/_do/rebase` and `/_do/rebase-status` join the 1.3 route table (both may `await` — `/_do/fetch` precedent); `JobKind::Rebase` (4.5); `rebase_jobs` table; `PackWriter::resume(bucket, key, upload_id, parts, pos, budget)` (5 already presumes it for GcConsolidate); Janitor gains `UPDATE rebase_jobs SET state='failed' ... WHERE state='running' AND created_at < now-PUSH_TIMEOUT`, kills `ingesting` packs of terminal rebase rows (`push_id IS NULL`), and deletes `ended_at < cutoff` rows in its 5.3 pass; `SliceBudget::fresh()` (the 20 s/400-subrequest constructor) is named.
- **`onto` pinned at creation.** Landing on a stale base is a legal rebase result but not tip-fresh; a client that needs `onto == current main` re-reads `ls-refs` and retries. An `expect-onto` CAS argument is a possible later flag, not built.
- **No interactive rebase.** No edit/reword/drop/reorder, no `--rebase-merges`, no `exec`; merge commits in `base..branch` are dropped, matching `git rebase` defaults. `rebase-status` is a poll, not a control channel.
- **Signatures dropped.** `gpgsig`/`ssh-sig` headers are not copied into replayed commits (the server cannot re-sign); a signed-history policy would need client-side re-signing after `fetch`.
- **Message shape.** `message` is one v2 argument line (<= `MAX_PKT_DATA` 65,516 bytes); multi-paragraph squash messages must be folded client-side or sent as repeated `message` lines (parser joins with `\n` — wire detail, unverified against a real client since none exists).
- **Unverified items:** `Request::new_with_init` for the stub call (as in two-phase-push); `PackWriter::resume` and part-overwrite-before-`complete` on real R2 (simulator only, #6); `TreeRefIter`/`CommitRefIter` field names pinned at compile; subrequest enforcement inside a DO (#7); the `x-ge-principal`-free JSON body carrying `principal` (DTO, not header).
- **Scenarios.** Must pass: 1, 2, 12 (stock `git` clone/push/fetch provably unaffected by the two extra capability lines — the review's interop check made this the surviving claim). Added (two, since stock `git` cannot emit the commands): (a) harness POSTs a hand-framed `command=rebase onto=<main tip> branch=refs/heads/feat expect=<feat tip>` pkt-line body with `Git-Protocol: version=2`, polls `command=rebase-status job <id>` to `done`, then a stock `git fetch` + `rev-parse` shows `feat` equal to the `ack`'d oid and `git fsck --strict` is clean; (b) the same harness races a `git push` to `feat` against a multi-slice rebase (harness sleeps mid-poll): exactly one of push/rebase wins the CAS, the other reports `stale`, and a fresh clone fsck's clean.

## Depends on
- repo-do-ref-authority
- two-phase-push
- streaming-pack-parser
- refs-sqlite-objects-r2
- gc-and-repack-alarm
- protocol-v2-only
- server-side-merge (blob-level merge, later; this proof lands tree-level only)
