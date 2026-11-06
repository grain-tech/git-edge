# Snapshots via R2 object versioning of ref state

> Second pass · Idea #21 · verdict: **lands with caveats** · feasibility 4/5 · reliability 3/5 · correctness 4/5 (first pass 4/2/3)
> First pass: [proof](./archived/r2-versioned-snapshots.md) · [review](../reviews/archived/r2-versioned-snapshots.md) · Second pass: [review](../reviews/r2-versioned-snapshots.md) · Contract: [CONTRACTS.md](../../CONTRACTS.md)

## Mechanism

```
REGISTRY (amendment A9) — everything this module adds:
JobKind       SnapPrune -> snap::prune_slice · SnapReindex -> snap::reindex_slice   (4.5 dispatch arms)
R2 prefix     snap/<owner>/<repo>/<refs_version:020>.json     — owner/repo from meta (x-ge headers, 8.1),
              not repo_id (boot regenerates it on a wiped DO, 8.2) and never ctx.id.name (8.3): the locator
              must be what a recreated namespace provably shares with its predecessor (review blocker).
meta key      snap_pending = '1' while SnapReindex has work; boot re-enqueues on it (A4)
routes        none new; /_do/push/commit's 1.3 row gains post-span awaits (one put + jobs::rearm)
write-backs   RepoDo fields pending_snap, restored (RefCell) · fetch calls snap::maybe_restore between boot
              and dispatch · commit arm calls snap::after_commit · store::Bucket gains list_page
              (put_bytes/get_bytes exist per branch-level-dos; delete per 1.2) · boot re-enqueues SnapReindex
```

R2 has no S3-style bucket versioning, so — as the first pass admitted — the title feature is emulated with immutable keys. Under the contract the sequence number is `meta.refs_version`, which section 3 step 5 bumps inside the `commit_push` span exactly when a ref move lands; `after_commit` captures `refs` + `peeled` (A5), `head`, `gc_epoch`, `repo_id` and the live `packs` list in that same span, so a key's content is always precisely the post-state of the version it is named for. The `LATEST` pointer and its `onlyIf` etag are deleted outright — restore selects `max(list(prefix))` over every page — and the write is a single `put` issued by the commit route after the span and before the response, so an `ok` line can reach the client only if the snapshot is durable. `blockConcurrencyWhile` is likewise unnecessary: `fetch` awaits `maybe_restore` before dispatch, so no route is ever served against un-restored tables. The wiped-DO test needs no flag: `refs_version = 0` with empty `refs` and `pushes` is impossible for a DO that ever committed. Restore rewrites `refs`, `meta.head`, `refs_version`, `gc_epoch` and `repo_id` in one rechecking span and hands the object side to `SnapReindex`, which rebuilds `packs`/`objects` rows for every pack still listed under `r/<repo_id>/packs/` — the live listing is the manifest, not the snapshot — while `SnapPrune` keeps the newest `RETAIN` keys in slices. Ref transactions, wire framing and the alarm dispatcher are the contract's; the edge Worker is untouched.

## Primitives

- `worker::Bucket::{put, get, list, delete_multiple}`; `PutOptionsBuilder::execute`, `GetOptionsBuilder::execute`, `ObjectBody::bytes`, `Object::{key, size, body}`, `ListOptionsBuilder::{prefix, limit, start_after, execute}`, `Objects::{objects, truncated}`: verified in `worker` 0.8.5 source (`r2/mod.rs`; `delete_multiple` per native-lfs, `put`/`get` per memo section 1 row 25). `store::Bucket::{put_bytes, get_bytes}` write-back names per branch-level-dos; `list_page` is new (REGISTRY). R2 listing is lexicographic per the S3-compatible docs — **not measured** on the simulator; nothing depends on it for correctness (restore scans every page for the max; a wrong order in `prune_slice` only leaves keys behind).
- `onlyIf`/`etag` conditional puts (`PutOptionsBuilder::only_if`, memo row 24): verified, deliberately unused — there is no pointer left to guard.
- `State::block_concurrency_while`: present in 0.8.5 (memo section 1), deliberately unused — the pre-dispatch await gives the same no-service-before-restore guarantee per request.
- Sync-span atomicity and `SELECT changes()`: measured (platform-facts #1, #4); `transactionSync` is absent, the no-await span is the unit (A3). The commit span's writes roll back if `fetch` throws before the next await — that is what makes an `after_commit` error reject the push (A2).
- `jobs::{enqueue, rearm, run_slice, SliceOutcome, SliceBudget::spent_80pct}`: contract 4; `enqueue` is sync and dedups by kind, `rearm` is the sole `set_alarm` caller (4.1, A3); a second `setAlarm` cancels the first (measured #5). Dead per-repo jobs are re-enqueued at `boot` (A4) — `snap_pending` is what `SnapReindex` re-enqueues on.
- `store::{codec::entry_header, Index::insert_objects, keys::pack}`, `Index::insert_objects` being `INSERT ... ON CONFLICT DO NOTHING` per row: contract 1.2/2.3; stored packs hold only full objects (2.1), so `entry_header` never sees a delta on this path.
- `gix_pack::data::header::decode(&[u8; 12]) -> (_, u32 count)`: verified in 0.74.2 (two-phase-push, spike).
- `gix_zlib::Inflate` driven incrementally with a consumed-byte count: the A10 pass-A pattern; the exact feed/result names are **unverified** — code marks the one call site.
- `gix_object::compute_hash(kind, data) -> ObjectId`: memo section 3, verified; used by `reindex_slice` to re-derive each sha from inflated bytes.
- `serde_json` DTOs and `RepoDo` helpers (`q`, `meta`, `sql`, `bucket`, `now_ms`): repo-do-ref-authority; `js_sys::Date::now()`: standard. No `js_sys::Reflect`, no `ctx.id.name`, no `platform` use — owner/repo come from `meta` (8.1/8.2).
- R2 from inside a DO: GA per the first-pass review; the DO-side subrequest limit is unenforced locally (#7), so every call goes through a `ReqBudget` (A1) or a `SliceBudget` charge (4.3).

## Proof code

```rust
// src/repo_do/snap.rs + src/jobs/snap.rs + src/repo_do/mod.rs fragments. CONTRACTS.md 1.2-1.4, 3, 4, 5, 7, 8;
// A1-A4, A9. worker 0.8.5, gix-pack 0.74.2, gix-object 0.64.1, gix-zlib 0.1.0.
// store::Bucket write-back (A1): list_page(&self, prefix, limit, start_after: Option<String>, rb) ->
//   Result<(Vec<(String, u64 /*size*/)>, bool /*truncated*/)> via inner.list().prefix().limit().start_after().
// `q`, `meta`, `sql`, `bucket`, `now_ms`: RepoDo helpers (repo-do-ref-authority); `parse`, `json`, `respond`
// live in wire::http (A8). ObjRow fields = objects columns (2.3).
use worker::SqlStorageValue as V;
use crate::{error::Error, jobs::{self, Job, JobKind, SliceBudget, SliceOutcome}, repo_do::{CommitResponse, RepoDo},
            store::{codec, Bucket, Index, ObjRow, PackId}, ReqBudget};
const RETAIN: i64 = 50;                          // first-pass retention window
const SNAP_MAX: usize = 8 << 20;                 // ~120k refs; a bigger map rejects the push, never half-commits
const PRUNE_AT: i64 = 300_000;                   // moving commit -> prune armed at +5 min (enqueue dedups, 4.5)
const PAGE: u32 = 300;                           // page + one delete_multiple <= 80% of the 400-subrequest slice (4.3)
const WIN: u64 = 8 << 20;                        // reindex window (6.4)
fn prefix(owner: &str, repo: &str) -> String { format!("snap/{owner}/{repo}/") }
fn slice_rb(b: &SliceBudget) -> ReqBudget {
    ReqBudget { max_subrequests: 400, used: b.subrequests_used, started_ms: b.started_ms, max_ms: 20_000.0 }
}
#[derive(serde::Deserialize)] struct N { n: i64 }
/// refs_version 0 AND no refs AND no pushes: impossible for a DO that ever committed (3 step 5).
fn looks_fresh(d: &RepoDo) -> Result<bool, Error> {
    Ok(d.q("SELECT COALESCE((SELECT CAST(value AS INTEGER) FROM meta WHERE key='refs_version'),0)\
            +(SELECT COUNT(*) FROM refs)+(SELECT COUNT(*) FROM pushes) AS n", vec![])?.one::<N>()?.n == 0)
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct Snap { v: i64, at: i64, repo_id: String, head: String, gc_epoch: i64,
                  refs: Vec<(String, String, Option<String>)>,        // name, target, peeled (A5)
                  packs: Vec<String> }                                // audit only; reindex lists, never trusts

/// Commit-arm hook inside the same sync span: the bytes are exactly the post-commit state, keyed by the
/// refs_version this span just bumped. None when no ref moved; its error propagates before the first
/// post-span await, so a too-big snapshot discards the whole commit (A2) rather than commit unbacked state.
pub fn after_commit(d: &RepoDo, res: &CommitResponse) -> Result<Option<(String, Vec<u8>)>, Error> {
    if !res.results.iter().any(|r| r.1.is_none()) { return Ok(None); }
    #[derive(serde::Deserialize)] struct R { name: String, target: String, peeled: Option<String> }
    #[derive(serde::Deserialize)] struct P { id: String }
    let snap = Snap {
        v: d.meta("refs_version")?.parse().map_err(|_| Error::Internal("refs_version".into()))?,
        at: now_ms(), repo_id: d.meta("repo_id")?, head: d.meta("head")?,
        gc_epoch: d.meta("gc_epoch")?.parse().map_err(|_| Error::Internal("gc_epoch".into()))?,
        refs: d.q("SELECT name,target,peeled FROM refs ORDER BY name", vec![])?.to_array::<R>()?
            .into_iter().map(|r| (r.name, r.target, r.peeled)).collect(),
        packs: d.q("SELECT id FROM packs WHERE state='live'", vec![])?.to_array::<P>()?
            .into_iter().map(|p| p.id).collect(),
    };
    let body = serde_json::to_vec(&snap).map_err(|e| Error::Internal(e.to_string()))?;
    if body.len() > SNAP_MAX { return Err(Error::Limit("refs snapshot > 8 MiB".into())); }
    jobs::enqueue(&d.sql(), JobKind::SnapPrune, now_ms() + PRUNE_AT, "{}")?;      // armed by the route (A3)
    Ok(Some((format!("{}{:020}.json", prefix(&d.meta("owner")?, &d.meta("repo")?), snap.v), body)))
}

/// fetch hook (write-back): awaited between boot and dispatch — blockConcurrencyWhile in per-request form.
/// Hot path is one SELECT, no await; the R2 work runs only while the DO looks fresh and `restored` is unset.
pub async fn maybe_restore(d: &RepoDo) -> Result<(), Error> {
    if d.restored.get() || !looks_fresh(d)? { return Ok(()); }
    let (owner, repo, bucket) = (d.meta("owner")?, d.meta("repo")?, d.bucket()?);
    let mut rb = ReqBudget { max_subrequests: 70, used: 0, started_ms: now_ms() as f64, max_ms: 60_000.0 };
    let (mut start, mut best) = (None, None);                            // newest = max key over EVERY page;
    loop {                                                             // list ordering is never assumed
        let (page, more) = bucket.list_page(&prefix(&owner, &repo), 1_000, start, &mut rb).await?;
        for (k, _) in &page { if best.as_deref() < Some(k.as_str()) { best = Some(k.clone()); } }
        match (more, page.last()) { (true, Some((k, _))) => start = Some(k.clone()), _ => break }
    }
    let Some(key) = best else { d.restored.set(true); return Ok(()) };   // genuinely new repo
    let body = bucket.get_bytes(&key, &mut rb).await?
        .ok_or_else(|| Error::Storage(format!("snapshot {key} vanished")))?;
    if body.len() > SNAP_MAX { return Err(Error::Limit(format!("snapshot {key} > 8 MiB"))); }
    let s: Snap = serde_json::from_slice(&body).map_err(|e| Error::Internal(format!("bad snapshot {key}: {e}")))?;
    apply(d, &s)?;                                                     // sync span; rechecks freshness inside
    jobs::rearm(d).await?;                                             // apply enqueued SnapReindex (A3)
    d.restored.set(true);
    Ok(())
}
/// One sync span. A push that slipped in during the awaits made the DO non-fresh and wins outright; its
/// commit writes a newer snapshot, so the restore no-ops instead of clobbering acknowledged state.
fn apply(d: &RepoDo, s: &Snap) -> Result<(), Error> {
    if !looks_fresh(d)? { return Ok(()); }
    for (name, target, peeled) in &s.refs {
        d.q("INSERT INTO refs(name,target,peeled,updated_at) VALUES(?,?,?,?)",
            vec![V::from(name.as_str()), V::from(target.as_str()), peeled.as_deref().map_or(V::Null, V::from),
                 V::from(s.at)])?;
    }
    for (k, v) in [("repo_id", s.repo_id.clone()), ("head", s.head.clone()), ("refs_version", s.v.to_string()),
                   ("gc_epoch", s.gc_epoch.to_string()), ("snap_pending", "1".into())] {
        d.q("INSERT OR REPLACE INTO meta(key,value) VALUES(?,?)", vec![V::from(k), V::from(v.as_str())])?;
    }
    jobs::enqueue(&d.sql(), JobKind::SnapReindex, now_ms(), "{}")        // manifest is the live pack listing
}

// ---- src/jobs/snap.rs ----
/// JobKind::SnapPrune (REGISTRY): one list page per slice; deletes keys below the floor pad(v - RETAIN + 1).
/// A page holding a floor-or-newer key ends the job (lexicographic listing per R2 docs, unverified: the
/// failure mode is leftover keys, never a wrong delete). Continue while a whole page was stale.
pub async fn prune_slice(d: &RepoDo, _job: &Job, budget: &mut SliceBudget) -> Result<SliceOutcome, Error> {
    let v: i64 = d.meta("refs_version")?.parse().map_err(|_| Error::Internal("refs_version".into()))?;
    let floor = format!("{:020}.json", v.saturating_sub(RETAIN).saturating_add(1));
    let (owner, repo, bucket) = (d.meta("owner")?, d.meta("repo")?, d.bucket()?);
    let mut rb = slice_rb(budget);
    let (page, more) = bucket.list_page(&prefix(&owner, &repo), PAGE, None, &mut rb).await?;
    budget.subrequests_used = rb.used;
    let base = |k: &str| k.rsplit('/').next().unwrap_or(k);
    let stale: Vec<String> = page.iter().map(|p| p.0.clone()).filter(|k| base(k) < floor.as_str()).collect();
    let hit_floor = page.iter().any(|p| base(&p.0) >= floor.as_str());
    if !stale.is_empty() { budget.subrequests_used = budget.subrequests_used.saturating_add(1);
                           bucket.delete(&stale).await?; }             // <= 300 keys, one delete_multiple
    Ok(if more && !hit_floor { SliceOutcome::Continue { cursor: String::new() } } else { SliceOutcome::Done })
}

#[derive(serde::Serialize, serde::Deserialize, Default)]
struct Cur { key: String, off: u64, idx: u32, n: u32, cnt: u32, lo: u64, hi: u64 }   // entry-boundary checkpoint
fn pack_id(k: &str) -> String { k.rsplit('/').next().unwrap_or(k).trim_end_matches(".pack").to_string() }
/// Ensure `win` covers `pos`..`pos`+hint; refills the 8 MiB window at `pos`. win[0] is always absolute `woff`.
async fn need(b: &Bucket, k: &str, pos: u64, hint: u64, win: &mut Vec<u8>, woff: &mut u64, rb: &mut ReqBudget)
    -> Result<(), Error> {
    if pos < *woff || pos.saturating_add(hint) > woff.saturating_add(win.len() as u64) {
        *win = b.read_range(k, pos, WIN, rb).await?; *woff = pos;
        if (win.len() as u64) < hint { return Err(Error::Storage(format!("pack {k} truncated at {pos}"))); }
    }
    Ok(())
}
/// JobKind::SnapReindex (REGISTRY): rebuilds packs+objects rows for every pack still under r/<repo>/packs/
/// by walking entries — the A10 pattern (header, incremental inflate, sha over inflated bytes). Inserts are
/// ON CONFLICT DO NOTHING and the checkpoint sits on entry boundaries, so a retried slice is idempotent.
/// Packs rows go 'ingesting' then 'live' exactly as ingest (2.3); created_at = now keeps them out of any
/// in-flight GC (5.1 age gate). A pack deleted externally mid-walk stalls the job -> Known limits.
pub async fn reindex_slice(d: &RepoDo, job: &Job, budget: &mut SliceBudget) -> Result<SliceOutcome, Error> {
    #[derive(serde::Deserialize)] struct S { value: String }
    if d.q("SELECT value FROM meta WHERE key='snap_pending'", vec![])?.to_array::<S>()?.is_empty() {
        return Ok(SliceOutcome::Done);
    }
    let mut c: Cur = job.cursor.as_deref().and_then(|s| serde_json::from_str(s).ok()).unwrap_or_default();
    let bucket = d.bucket()?;                                            // repo_id already the restored one
    let pref = format!("r/{}/packs/", bucket.repo.0);
    let mut rb = slice_rb(budget);
    let (mut win, mut woff, mut rows): (Vec<u8>, u64, Vec<ObjRow>) = (Vec::new(), 0, Vec::new());
    loop {
        if budget.spent_80pct() { break; }                               // checkpoint lands on an entry boundary
        if c.off == 0 {                                                  // next pack: first key past the last done
            let (page, _) = bucket.list_page(&pref, 1, Some(c.key.clone()), &mut rb).await?;
            budget.subrequests_used = rb.used;
            let Some((k, bytes)) = page.into_iter().next() else {
                d.q("DELETE FROM meta WHERE key='snap_pending'", vec![])?;
                return Ok(SliceOutcome::Done);                           // rebuilt everything that survived
            };
            let w = bucket.read_range(&k, 0, WIN, &mut rb).await?;
            budget.subrequests_used = rb.used;
            let h: &[u8; 12] = w.get(..12).and_then(|s| s.try_into().ok())
                .ok_or_else(|| Error::Storage(format!("pack {k} short")))?;
            let (_, cnt) = gix_pack::data::header::decode(h).map_err(|e| Error::Storage(e.to_string()))?;
            d.q("INSERT INTO packs(id,state,count,bytes,commit_lo,commit_hi,push_id,created_at) \
                 VALUES(?,'ingesting',?,?,?,0,NULL,?) ON CONFLICT(id) DO NOTHING",
                vec![V::from(pack_id(&k).as_str()), V::from(i64::from(cnt)),
                     V::from(i64::try_from(bytes).map_err(|_| Error::Internal("bytes".into()))?),
                     V::from(i64::MAX), V::from(now_ms())])?;            // A5 sentinel; push_id NULL (2.3)
            c = Cur { key: k, off: 12, cnt, lo: u64::MAX, ..Default::default() };
            win = w; woff = 0;
        }
        if c.n == c.cnt {                                                // pack complete: flip live in this span
            Index(&d.sql()).insert_objects(&PackId(pack_id(&c.key)), &rows)?; rows.clear();
            let lo = if c.lo == u64::MAX { i64::MAX } else { i64::try_from(c.lo).map_err(|_| Error::Internal("lo".into()))? };
            d.q("UPDATE packs SET state='live', commit_lo=?, commit_hi=? WHERE id=? AND state='ingesting'",
                vec![V::from(lo), V::from(i64::try_from(c.hi).map_err(|_| Error::Internal("hi".into()))?),
                     V::from(pack_id(&c.key).as_str())])?;
            c = Cur { key: std::mem::take(&mut c.key), ..Default::default() };   // off=0 -> next pack
            continue;
        }
        need(&bucket, &c.key, c.off, 64, &mut win, &mut woff, &mut rb).await?;   // entry header window
        budget.subrequests_used = rb.used;
        let at = usize::try_from(c.off - woff).map_err(|_| Error::Internal("win".into()))?;
        let (kind, size, hlen) = codec::entry_header(win.get(at..).ok_or_else(|| Error::Storage("entry".into()))?)?;
        let mut p = c.off.saturating_add(u64::try_from(hlen).map_err(|_| Error::Internal("hlen".into()))?);
        let (mut inf, mut data) = (gix_zlib::Inflate::default(), Vec::new());
        loop {                                                           // stream the zlib body across refills
            need(&bucket, &c.key, p, 1, &mut win, &mut woff, &mut rb).await?;
            budget.subrequests_used = rb.used;
            let at = usize::try_from(p - woff).map_err(|_| Error::Internal("win".into()))?;
            let st = inf.feed(win.get(at..).ok_or_else(|| Error::Storage("entry".into()))?, &mut data)
                .map_err(|e| Error::Storage(e.to_string()))?;            // A10 pattern; return shape unverified
            p = p.saturating_add(u64::try_from(st.consumed).map_err(|_| Error::Internal("in".into()))?);
            if st.done { break; }
        }
        rows.push(ObjRow { sha: gix_object::compute_hash(kind, &data), idx: c.idx, offset: c.off,
                           len: u32::try_from(p - c.off).map_err(|_| Error::Limit("entry > u32".into()))?,
                           kind, size });
        if kind == gix_object::Kind::Commit { c.lo = c.lo.min(c.off); c.hi = c.hi.max(p); }
        c.off = p; c.idx += 1; c.n += 1;
        if rows.len() >= 10_000 { Index(&d.sql()).insert_objects(&PackId(pack_id(&c.key)), &rows)?; rows.clear(); }
    }
    Index(&d.sql()).insert_objects(&PackId(pack_id(&c.key)), &rows)?;    // flush before the checkpoint
    let cursor = serde_json::to_string(&c).map_err(|e| Error::Internal(e.to_string()))?;
    Ok(SliceOutcome::Continue { cursor })
}

// ---- src/repo_do/mod.rs (write-backs). RepoDo gains pending_snap: RefCell<Option<(String, Vec<u8>)>> and
// restored: RefCell<bool>; in fetch, between boot and dispatch: snap::maybe_restore(self).await? — alarm needs
// none: a wiped DO has no jobs rows, so dispatch is a no-op until a fetch restores. Commit arm, in-span:
//   (Method::Post, "/_do/push/commit") => {
//       let res = self.commit_push(&parse::<CommitRequest>(&body)?)?;                  // section 3, verbatim
//       *self.pending_snap.borrow_mut() = snap::after_commit(self, &res)
//           .map_err(|e| Error::Storage(format!("snapshot: {e}")))?;                  // same span, post-state
//       json(res) }                                                                  // Storage -> Err out (A2)
// After `out` is computed, before respond(out) — an Err `out` skips this block so a failed span still rolls
// back; on Ok the snapshot lands before the edge can write `ok`:
//   if out.is_ok() {
//       if let Some((key, bytes)) = self.pending_snap.take() {
//           jobs::rearm(self).await?;                                               // SnapPrune armed (A3)
//           let mut rb = ReqBudget { max_subrequests: 4, used: 0, started_ms: now_ms() as f64, max_ms: 20_000.0 };
//           self.bucket()?.put_bytes(&key, bytes, &mut rb).await?;                  // one R2 write per moving push
//       }
//   }
```

## Why it works

- **Acknowledged implies snapshotted.** Inside `/_do/push/commit` the order is span, `put_bytes`, response; the edge writes `ok`/`unpack ok` only after the stub returns, so a client-visible `ok` cannot precede the R2 write. A `put` error propagates as `Err` out of `fetch` (A2): the client sees the failure, the move is durable but unacknowledged, and the retry gets `ng ... failed to update ref` (section 3 CAS) — the first pass's chosen loss side, now enforced by code shape. A `Limit`/`Internal` inside `after_commit` is converted to `Storage` in the arm, throws before any post-span await, and discards the commit span entirely: a repo whose ref map exceeds `SNAP_MAX` cannot commit unbacked state.
- **The reviewed regression class is structurally gone.** There is no `LATEST`. Interleaved commits A (v) and B (v+1) write independent immutable keys whose content was captured inside their own spans; whichever put lands, `max(list)` is the newest landed version and contains every earlier acknowledged move. B's `ok` implies B's put resolved, so restore of the max key can lose only moves the client never saw acknowledged. The "A writes the pointer after B" ordering that stranded `LATEST` at seq 1 cannot exist — nothing is overwritten.
- **The recreated DO can find its backup.** `snap/<owner>/<repo>` derives from `x-ge` headers stored in `meta` (8.1/8.2), identical for any DO behind `id_from_name("owner/repo")`. `ctx.id` changes across namespace recreation and `ctx.id.name` is barred as key material (8.3); `repo_id` is regenerated by `boot` on empty meta — which is why the snapshot carries it and `apply` adopts it back, keeping `r/<repo_id>/packs/` addressable.
- **Restore cannot clobber a live push.** `maybe_restore` does its R2 awaits first; `apply` rechecks `refs_version = 0 ∧ refs empty ∧ pushes empty` inside the write span (platform-facts #4 atomicity). A `push_begin` or commit that slipped in during the awaits wins outright — its own commit then writes a newer snapshot — and duplicated concurrent restores converge because the second `apply` no-ops.
- **Objects are rebuilt, not just refs.** `SnapReindex` lists the pack prefix itself rather than trusting `snap.packs`, so packs created after the newest snapshot (a `GcConsolidate` pack, a push whose put failed) and even pre-wipe `dead` packs still get indexed. Each pack goes `ingesting` → `live` like ingest (2.3), so readers and `GcMark` never see a half-indexed pack; `created_at = now` keeps reindexed packs out of the running GC (5.1); a pack absent from the listing gets no row, so the Janitor can never mark-delete bytes it cannot see.
- **Retention is a slice, not a list.** `prune_slice` does one 300-key page plus one `delete_multiple` per firing and `Continue`s while the page was all-stale — backlog drains across consecutive firings (4.2), so pushes outrunning the alarm is a backlog, not a truncation bug.
- **Budgets and memory.** Commit gains exactly one subrequest (`ReqBudget` of 4, A1). `maybe_restore` is capped at 70 and runs at most once per fresh-looking DO instance — one extra `SELECT` is the steady-state cost. Slices charge through `SliceBudget` and stop at `spent_80pct` (A4): reindex costs one list per pack plus one range read per 8 MiB plus sync inserts. Snapshot build holds ≤ 8 MiB of JSON; reindex holds one window + one inflated object ≤ 16 MiB (A7) + ≤ 10,000 `ObjRow`. No statement binds more than 4 parameters (A6).

## Changes from the first pass

| First-pass finding (quoted) | Kind | How addressed |
|---|---|---|
| "LATEST pointer regression under interleaved pushes (input gate open during R2 awaits; conditional put result ignored)." | blocker | Pointer deleted: immutable per-`refs_version` keys, restore = max over all list pages with no ordering assumption. Content is captured inside the commit span, and `ok` reaches the client only after `put_bytes` resolves, so the max key always contains every acknowledged move. |
| "R2 prefix keyed on `ctx.id` defeats the namespace-recreate/class-migration recovery scenario the idea exists for." | blocker | Prefix is `snap/<owner>/<repo>` from `meta`/`x-ge` headers (8.1/8.2) — identical across namespace recreation. `ctx.id.name` is never key material (8.3); `repo_id`, regenerated by `boot` on a wiped DO, is carried inside the snapshot and adopted back so the pack namespace survives. |
| "HEAD and any other symrefs are not snapshotted; restore yields an uncloneable default branch." | caveat | `Snap.head` restores `meta.head` (the symbolic row, section 3) and `peeled` rides the ref tuples (A5); post-restore `ls-refs` emits `HEAD symref-target:` and `peeled:` as before. |
| "Non-atomic pushes are made atomic by the throw-inside-transaction; report-status is under-populated." | caveat | Addressed by contract section 3: `commit_push` applies each command as an independent CAS with `changes()` and returns one result per command in client order. The snapshot never sees a throw — it only observes `any_ok`. |
| "Restore trusts objects still exist; GC horizon must exceed retention, as the proof notes but does not enforce." | caveat | Now enforced structurally: `SnapReindex` rebuilds from the live `r/<repo_id>/packs/` listing — a pack GC already deleted is simply not listed and gets no row — while listed packs are indexed `ingesting` → `live`. Retention bounds keys, not correctness; the restored `refs_version` snapshot's refs are served by whatever actually survives. |
| "Prune alarm `list` caps at 1000 keys; fine at RETAIN=50 but unbounded if pushes outrun alarms." | caveat | `SnapPrune` is a `jobs` slice (4.2): one 300-key page + one `delete_multiple` per firing, `Continue` while the page is all-stale. No unbounded single list exists. |
| "Adds 2-3 R2 round trips (20-50 ms) inside the per-repo serial section; lowers push throughput per repo." | caveat | Reduced to exactly one `put` per commit that moved a ref (no `head`, no conditional write — the pointer is gone). Still one R2 write of added commit latency; in Known limits. |
| "\"R2 object versioning\" in the title is fiction on today's R2; what ships is a per-seq key log plus pointer." | caveat | Still true, stated in Mechanism: emulated by immutable `snap/<o>/<r>/<v>.json` keys — now without even the pointer. |
| First-pass limit: "Full-snapshot-per-push is O(#refs) per push: a repo with 20k refs writes ~1-2 MB to R2 on every flip" | limit | Kept; bounded by `SNAP_MAX` (8 MiB, ~120k refs), beyond which the push is refused whole. The delta-plus-fold variant stays future work. |
| First-pass limit: "commit-then-PUT ordering ... a DO crash in that small window drops the last flip" | limit | Same side deliberately; now structural: a crash between span and put can only lose a move the client never saw acknowledged, and the gap in key numbering is harmless under max-key restore. |
| First-pass limit: "Restoring a repo with many refs in the constructor costs CPU before the first request" | limit | Moved out of the constructor into a bounded pre-dispatch await (≤ ~66 subrequests, ≤ 8 MiB body); the write span's INSERT count is bounded by `SNAP_MAX`. DO SQLite PITR (30-day bookmarks) remains the cheaper recovery for the same-storage crash class; this module targets storage loss. |

## Known limits

- Restored refs are advertised before their objects are re-indexed: `ls-remote` is right immediately, but `fetch` of a tip whose pack is still `ingesting` gets `ERR upload-pack: not our ref <oid>` until `SnapReindex` flips it `live`. A mirror `git push` does not self-heal — the client dedupes against the advertised tips — so reindex is the only repair; it re-inflates every surviving object (job-sliced: ~pack_bytes/8 MiB reads + inflate CPU, minutes for large repos).
- Pre-wipe `dead` packs are also reindexed (their objects become live rows again: unreferenced, never deleted — a small resurrection, never a loss). Packs deleted externally mid-walk stall `SnapReindex` into `dead`, which `boot` re-enqueues while `snap_pending` (A4); delete the meta key to abandon.
- `reflog`, `pushes` and `jobs` history are not snapshotted: the restored repo keeps refs, `head`, `repo_id`, `refs_version` and `gc_epoch`; `refs/at/` (time-travel-refs sibling) sees no pre-wipe moves.
- Lost-race wipe: a push that begins during the restore awaits aborts the restore (the DO is no longer fresh); the repo continues as fresh and the snapshot files remain for a later wipe or operator replay. On that path the old `r/<repo_id>/` tree leaks in R2 — storage cost only, operator cleanup.
- Repo rename (section 12, out of scope) orphans the `snap/<owner>/<repo>/` prefix: the renamed repo's fresh DO looks under its new name and finds nothing. Deletion likewise leaves keys behind.
- `SNAP_MAX` = 8 MiB (~120k refs) is a hard push ceiling: beyond it every moving commit is rejected as `unpack error snapshot: refs snapshot > 8 MiB`. Accepting that keeps "committed ⇒ snapshotted" absolute; a delta-snapshot variant is the named escape.
- `worker::Bucket::list` ordering and `Object::key`/`size` on list results are verified in 0.8.5 source but not exercised on the simulator; `gix_zlib::Inflate`'s feed signature is **unverified** (one call site, marked).
- Scenarios this proof must pass: 2, 4, 6, 14 (push path regression: identical plus one put). Added (two): (a) "DO wipe": after pushes, the harness deletes the DO's persisted storage (local workerd persist dir) and re-requests `git ls-remote` — restore runs, alarms are fired until `SnapReindex` drains, then `git clone` + `git fsck --strict` must be clean with HEAD resolving to the pre-wipe branch; (b) "interleaved snapshot": two concurrent pushes to different refs with a test hook evicting the DO between commit span and put — whichever push saw `ok` must appear in the state restored from `max(list)`.

## Depends on

- repo-do-ref-authority
- two-phase-push
- refs-sqlite-objects-r2
- gc-and-repack-alarm
