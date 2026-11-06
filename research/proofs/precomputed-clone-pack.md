# Precomputed pack slices for clone

> Second pass · Idea #7 · verdict: **risky** · feasibility 4/5 · reliability 3/5 · correctness 4/5 (first pass 4/3/3)
> First pass: [proof](./archived/precomputed-clone-pack.md) · [review](../reviews/archived/precomputed-clone-pack.md) · Second pass: [review](../reviews/precomputed-clone-pack.md) · Contract: [CONTRACTS.md](../../CONTRACTS.md)


## Mechanism
CONTRACTS.md section 12 lists precomputed clone packs as out of scope for the foundation and section 2.2 allows no R2 key other than `packs/` and `pending/`, so this idea is reshaped rather than subsumed: the physical full pack is the one `GcConsolidate` already writes (section 5.2), and what this module precomputes is the **send-set** for a fresh clone, not a second copy of the bytes. A new job kind `ClonePlan` (registered per section 4.5, in `jobs`) walks from every ref at a captured `refs_version` with the round loop of section 9 and stores one bitmap per live pack in a new `clone_plan` table plus `clone.*` rows in `meta` (the shape of `gc.*` in section 5); because every entry at rest is a full object (2.1), any subset of entries in any order is a valid pack, so the "full" and "blobless" slices are two bitmaps over the same bytes. `RepoDo::fetch_v2` gains one sync gate between section 9 steps 1 and 3: a request with `done`, no `have`, no `deepen`, and a filter of none or `blob:none`, whose wants are all captured tips and whose plan packs are all still `live`, skips steps 3 to 5 and hands the stored bitmaps to `pack::generate::write_pack`; when the plan is one fully marked pack with no filter, the R2 object is streamed byte for byte with no hasher, which is the idea in its original form. Every miss calls `jobs::enqueue(ClonePlan, ..)` (4.5 dedups), so the plan is demand-driven and self-healing after a push or a GC sweep.

## Primitives
- DO SQLite through `worker::SqlStorage::exec` (sync, `SqlCursor::to_array`), `SELECT changes()` never needed here (no CAS): binding shape **unverified**, wrapped in the sibling proofs' `exec` helper. BLOB columns for bitmaps: SQLite feature, `SqlStorageValue` blob variant **unverified**.
- Jobs dispatcher (section 4): `JobKind::ClonePlan` as a new variant, `run_slice` arm, `SliceOutcome::{Continue, Reschedule, Done}`, cursor in the job row, `enqueue` dedup. No `set_alarm` call in this module (4.1). Second `set_alarm` cancels the first: **measured** (platform-facts #5), which is why only `jobs::rearm` sets it.
- `Index::lookup` (2.3 reader query, live packs only) and `Bucket::read_entries` (coalesced range reads, 7.2): sync and async halves of the section 9 loop. `Range::OffsetWithLength` variant verified in `worker/src/r2/builder.rs` (streaming-pack-parser); real R2 range reads **local simulator only** (#6).
- `gix_object::{CommitRef, TagRef, TreeRefIter}::from_bytes`, `EntryMode::is_commit`, `gix_object::Find::try_find` over `MemFind`: CI-built for wasm32 (memo section 3); `CommitRef::from_bytes` and a commit walk over a `HashMap` `Find` ran on workerd in the spike. `TagRef`/`TreeRefIter` **not run in the spike**.
- `pack::generate::write_pack(bucket, &SendSet, &mut Sideband, budget)` (1.4, section 9 step 6): the contract's own streamer; its `SendSet` is "one bitmap per pack", so a stored bitmap is a drop-in input. Owned by protocol-v2-only; **unverified at runtime** like `Response::from_stream`.
- `futures_util::stream::unfold` (memo manifest lists `futures-util`) to build the `TryStream` that `worker::Response::from_stream` consumes (`S::Ok: Into<Vec<u8>>`, memo section 1): API shape from docs, **unverified at runtime**.
- `wire::{PktWriter, Sideband}` (1.1): frames of at most 65515 data bytes, band 3 for a mid-stream error (section 10). `band_to_write` **not exercised by the spike**.
- Slice budget: 20,000 ms wall (`js_sys::Date::now()`) and 400 subrequests per alarm firing (4.3); the DO subrequest limit is **not enforced by local workerd** (#7), so `SliceBudget`/`ReqBudget` are the only guards.
- git wire facts used: a v2 `fetch` with `done` and no `have` omits `acknowledgments` (rule 3); `index-pack` accepts a pack that is a superset of the wanted closure; a full-object pack needs no `ofs-delta` capability. Checked against gitprotocol-v2 and the first-pass review's interop walk-through, not yet by scenario run.

## Proof code
```rust
// src/jobs/clone_plan.rs + src/repo_do/clone.rs -- CONTRACTS.md 1.2, 1.3, 1.4, 2.1, 2.3, 2.5, 4, 5, 7, 9, 10
use std::collections::HashSet;
use gix_hash::ObjectId;
use gix_object::{CommitRef, Find, Kind, TagRef, TreeRefIter};
use serde::{Deserialize, Serialize};
use worker::SqlStorage;
use crate::{error::Error, jobs::{self, Job, JobKind, SliceBudget, SliceOutcome}, pack::generate::{PackSlice, SendSet},
            store::{codec, keys, Bucket, Index, MemFind, ObjLoc, PackId}, wire::{Filter, FetchArgs, PktWriter, Sideband},
            ReqBudget, RepoDo};
// exec() / meta_i64() / meta_str() / struct N { n: i64 } helpers: as in the refs-sqlite-objects-r2 proof. meta_opt(): Ok(None) if absent.
// Schema added at schema_version 2 by RepoDo::boot (8.2); only this module reads or writes it:
//   CREATE TABLE clone_plan  (pack_id TEXT PRIMARY KEY, full BLOB NOT NULL, noblob BLOB NOT NULL) WITHOUT ROWID;  -- served
//   CREATE TABLE clone_build (pack_id TEXT PRIMARY KEY, full BLOB NOT NULL, noblob BLOB NOT NULL) WITHOUT ROWID;  -- in progress
//   meta rows, like gc.* in section 5: clone.tips (JSON hex list), clone.refs_version, clone.gc_epoch, clone.built_at, clone.build_ms
pub const CLONE_QUIET_MS: i64 = 10 * 60 * 1000;   // GC_QUIET (section 5): the plan is rebuilt no sooner than GcMark would run
const WINDOW: u64 = 8 << 20;                        // 6.5 / 7.2: one range read per 8 MiB window
#[derive(Serialize, Deserialize, Default)]
struct Cursor { tips: Vec<String>, refs_version: i64, gc_epoch: i64, frontier: Vec<String>, started_ms: f64 }
// Bitmaps: HashMap<PackId, (Vec<u8> full, Vec<u8> noblob)>, one bit per `objects.idx`, sized from packs.count (2.3).
// load/save move rows of clone_build; set(sql, loc) -> Ok(true) if the bit was already set (the object is expanded already),
// creating the pair for a pack first seen in this build. Popcount = SendSet count. ~40 lines of bit twiddling, not shown.

/// jobs::run_slice arm for JobKind::ClonePlan: GcMark's loop (section 9, "the same loop shape"), with three differences:
/// it walks from the refs captured on the first slice, it does not exempt young packs, and it keeps a second bitmap
/// without blobs. Blobs are never read: their ObjLoc is enough. A sha in two live packs (2.3) is marked in one only.
pub async fn run_clone_plan(d: &RepoDo, job: &Job, budget: &mut SliceBudget) -> Result<SliceOutcome, Error> {
    let sql = d.state.storage().sql();
    let mut cur = match job.cursor.as_deref().map(serde_json::from_str::<Cursor>) {
        Some(Ok(c)) if !c.frontier.is_empty() => c,             // Continue: resume. Reschedule after a finished build: restart below.
        _ => start(d, &sql)?,
    };
    let (bucket, mut bits, mut mem) = (d.bucket()?, Bitmaps::load(&sql, "clone_build")?, MemFind::default());
    while !cur.frontier.is_empty() {
        if budget.spent_80pct() {                                  // 4.3: save and yield; the next firing continues
            bits.save(&sql, "clone_build")?;
            return Ok(SliceOutcome::Continue { cursor: serde_json::to_string(&cur).map_err(|e| Error::Internal(e.to_string()))? });
        }
        let ids = std::mem::take(&mut cur.frontier).iter()
            .map(|h| ObjectId::from_hex(h.as_bytes()).map_err(|_| Error::Internal("bad oid in cursor".into())))
            .collect::<Result<Vec<_>, _>>()?;
        let mut load: Vec<(ObjectId, ObjLoc)> = Vec::new();
        for (id, loc) in ids.iter().zip(Index(&sql).lookup(&ids)?) {           // sync, live packs only (2.3)
            let loc = loc.ok_or_else(|| Error::Internal(format!("live tip reaches non-live object {id}")))?;   // impossible by 2.5
            if bits.set(&sql, &loc)? { continue; }
            if loc.kind != Kind::Blob { load.push((*id, loc)); }
        }
        for (id, entry) in bucket.read_entries(&load, &mut budget.req).await? {  // async, coalesced (7.2), charged (7.1)
            let (kind, data) = codec::decode_entry(&entry)?;
            mem.insert(id, kind, data);
        }
        cur.frontier = expand(&mem, &load)?;                        // sync gitoxide parsing over MemFind (section 9 rule)
        mem.clear();
    }
    bits.save(&sql, "clone_build")?;
    finish(d, &sql, &cur, &bits)
}

/// First slice, sync: capture what the plan will be valid for. If a GC job is queued or running, wait for it:
/// GcSweep would make the plan's packs dead and the walk would be wasted (section 5.3).
fn start(d: &RepoDo, sql: &SqlStorage) -> Result<Cursor, Error> {
    #[derive(Deserialize)] struct T { target: String }
    let gc_busy = !exec(sql, "SELECT 1 FROM jobs WHERE kind IN ('GcMark','GcConsolidate','GcSweep') AND state IN ('queued','running')", vec![])?
        .to_array::<serde_json::Value>()?.is_empty();
    if gc_busy { return Err(Error::Conflict("gc running".into())); }   // 4.4 retry with backoff; cursor stays None
    exec(sql, "DELETE FROM clone_build", vec![])?;
    let tips: Vec<String> = exec(sql, "SELECT DISTINCT target FROM refs", vec![])?.to_array::<T>()?.into_iter().map(|t| t.target).collect();
    Ok(Cursor { frontier: tips.clone(), tips, refs_version: d.meta_i64("refs_version")?, gc_epoch: d.meta_i64("gc_epoch")?,
                started_ms: js_sys::Date::now() })
}

/// Sync: parents and root tree of commits, target of tags, non-gitlink entries of trees. Blobs never enter the frontier.
fn expand(mem: &MemFind, loaded: &[(ObjectId, ObjLoc)]) -> Result<Vec<String>, Error> {
    let (mut next, mut buf) = (Vec::new(), Vec::new());
    for (id, _) in loaded {
        let obj = mem.try_find(id, &mut buf).map_err(|e| Error::Internal(e.to_string()))?
            .ok_or_else(|| Error::Storage(format!("entry {id} not read")))?;
        match obj.kind {
            Kind::Commit => { let c = CommitRef::from_bytes(obj.data).map_err(|e| Error::Unpack(e.to_string()))?;
                              next.push(c.tree().to_string()); next.extend(c.parents().map(|p| p.to_string())); }
            Kind::Tag => next.push(TagRef::from_bytes(obj.data).map_err(|e| Error::Unpack(e.to_string()))?.target().to_string()),
            Kind::Tree => for e in TreeRefIter::from_bytes(obj.data) {
                              let e = e.map_err(|e| Error::Unpack(e.to_string()))?;
                              if !e.mode.is_commit() { next.push(e.oid.to_string()); }   // submodule commits are not ours
                          },
            Kind::Blob => {}
        }
    }
    Ok(next)
}

/// One sync span: the plan swaps in all at once or not at all. Refs that moved during the build do not make the
/// plan wrong (tips are captured, objects are immutable, pack liveness is checked at serve time); they make it stale,
/// so the job is rescheduled with a rate-aware quiet time: never more than one build per 10 build-durations.
fn finish(d: &RepoDo, sql: &SqlStorage, cur: &Cursor, bits: &Bitmaps) -> Result<SliceOutcome, Error> {
    let now = js_sys::Date::now();
    exec(sql, "DELETE FROM clone_plan", vec![])?;
    exec(sql, "INSERT INTO clone_plan SELECT pack_id, full, noblob FROM clone_build", vec![])?;
    exec(sql, "DELETE FROM clone_build", vec![])?;
    let tips = serde_json::to_string(&cur.tips).map_err(|e| Error::Internal(e.to_string()))?;
    for (k, v) in [("clone.tips", tips), ("clone.refs_version", cur.refs_version.to_string()), ("clone.gc_epoch", cur.gc_epoch.to_string()),
                   ("clone.built_at", (now as i64).to_string()), ("clone.build_ms", ((now - cur.started_ms) as i64).to_string())] {
        exec(sql, "INSERT OR REPLACE INTO meta(key, value) VALUES(?, ?)", vec![k.into(), v.into()])?;
    }
    if bits.len() > 1 {                                             // nudge GC so the steady state is one pack (verbatim path)
        let newest: i64 = exec(sql, "SELECT MAX(created_at) AS n FROM packs WHERE state='live'", vec![])?.to_array::<N>()?.first().map_or(0, |r| r.n);
        jobs::enqueue(sql, JobKind::GcMark, (now as i64).max(newest + 60 * 60 * 1000 + 60_000), "{}")?;   // GRACE + 1 min, dedups
    }
    if d.meta_i64("refs_version")? != cur.refs_version {
        let quiet = CLONE_QUIET_MS.max(((now - cur.started_ms) * 10.0) as i64);   // host numbers, not client bytes
        return Ok(SliceOutcome::Reschedule { run_at: now as i64 + quiet });  // cursor kept, frontier empty => restart
    }
    Ok(SliceOutcome::Done)
}

#[derive(Deserialize)] struct PlanRow { pack_id: String, full: Vec<u8>, noblob: Vec<u8>, state: String, count: u32, bytes: u64 }
impl RepoDo {
    /// Sync gate in fetch_v2 after section 9 step 1 (every want already resolved to a live object; unknown wants
    /// were rejected there). Some(set): stream it with write_pack or stream_verbatim and skip steps 3 to 5.
    pub fn clone_plan(&self, args: &FetchArgs) -> Result<Option<SendSet>, Error> {
        let blobless = matches!(args.filter, Some(Filter::BlobNone));
        let fresh = args.done && args.haves.is_empty() && args.deepen.is_none() && args.shallow.is_empty()
            && (args.filter.is_none() || blobless);                 // blob:limit, have, deepen: the negotiated path
        if !fresh { return Ok(None); }
        let sql = self.state.storage().sql();
        let now = js_sys::Date::now() as i64;
        let miss = |sql: &SqlStorage| -> Result<Option<SendSet>, Error> {   // demand-driven rebuild; enqueue dedups (4.5)
            jobs::enqueue(sql, JobKind::ClonePlan, now + CLONE_QUIET_MS, "{}")?; Ok(None)
        };
        let Some(tips_json) = self.meta_opt("clone.tips")? else { return miss(&sql) };
        let tips: HashSet<ObjectId> = serde_json::from_str::<Vec<String>>(&tips_json).map_err(|e| Error::Internal(e.to_string()))?
            .iter().map(|h| ObjectId::from_hex(h.as_bytes()).map_err(|_| Error::Internal("bad oid in clone.tips".into()))).collect::<Result<_, _>>()?;
        if !args.wants.iter().all(|w| tips.contains(w)) { return miss(&sql); }   // a ref moved since the plan: negotiate
        let rows = exec(&sql, "SELECT c.pack_id, c.full, c.noblob, p.state, p.count, p.bytes FROM clone_plan c JOIN packs p ON p.id = c.pack_id", vec![])?
            .to_array::<PlanRow>()?;
        if rows.is_empty() || rows.iter().any(|r| r.state != "live") { return miss(&sql); }   // GcSweep ran since the plan
        Ok(Some(SendSet { packs: rows.into_iter().map(|r| PackSlice { pack: PackId(r.pack_id), count: r.count, bytes: r.bytes,
                                                                       bitmap: if blobless { r.noblob } else { r.full } }).collect() }))
    }
}

struct Verbatim { bucket: Bucket, key: String, off: u64, total: u64, prelude: Option<Vec<u8>>, budget: ReqBudget, ended: bool }
/// The idea in its original form, taken when the plan is one pack, every entry marked, no filter: the R2 object is
/// byte for byte the pack git expects (2.1: header count is packs.count, trailer is the object's own SHA-1), so no
/// hasher runs and the DO copies each byte once into band-1 frames. `prelude` is the pkt-lines up to `packfile\n`.
pub fn stream_verbatim(bucket: Bucket, pack: &PackId, total: u64, prelude: Vec<u8>, budget: ReqBudget) -> worker::Response {
    let key = keys::pack(&bucket.repo, pack);                       // r/<repo_id>/packs/<pack_id>.pack (2.2)
    let st = Verbatim { bucket, key, off: 0, total, prelude: Some(prelude), budget, ended: false };
    let body = futures_util::stream::unfold(st, |mut s| async move {   // shape per docs; unverified at runtime
        if s.ended { return None; }
        let mut w = PktWriter { out: s.prelude.take().unwrap_or_default() };
        if s.off >= s.total { w.flush(); s.ended = true; return Some((Ok::<Vec<u8>, Error>(w.out), s)); }   // rule 3: flush ends it
        let len = WINDOW.min(s.total.saturating_sub(s.off));
        match s.bucket.read_range(&s.key, s.off, len, &mut s.budget).await {   // one subrequest per window (7.1)
            Ok(win) => { Sideband::new(&mut w).data(&win); s.off = s.off.saturating_add(len); }
            Err(e) => { Sideband::new(&mut w).error(&format!("ERR {e}")); s.ended = true; }   // section 10: band 3, then end
        }
        Some((Ok(w.out), s))
    });
    worker::Response::from_stream(body).unwrap_or_else(|_| worker::Response::error("stream", 500).unwrap_or_default())
}
```

## Why it works
- **A fresh clone is deterministic given its wants, so its answer can be precomputed.** With `done`, no `have`, no `deepen`, the v2 response is `packfile` plus the closure of the wants (rule 3; the prelude writer in protocol-v2-only omits `acknowledgments`). `clone_plan` accepts exactly that shape, and `wants ⊆ clone.tips` means closure(wants) ⊆ closure(tips) = the marked bits, so the bitmap is a superset of what git needs. `index-pack` indexes the extra objects and the clone is correct (first-pass review, interop check).
- **No walk at request time.** Section 9 steps 3 to 5 are where the fetch path spends CPU, memory (64 MiB `MemFind`, 200,000-commit cap) and subrequest rounds. The gate replaces them with two `SELECT`s in the same sync span as step 1 and hands `write_pack` the same `SendSet` type step 5 would have produced (1.4). The DO never holds object bytes for a fast-path clone, only bitmaps (about 125 KB per million objects).
- **Slices are bitmaps because packs are full-object.** 2.1 stores every entry as `varint(kind,size) + zlib(data)` with no `ofs-delta` or `ref-delta`; an entry is valid wherever it lands, so "blobless" is the `noblob` bitmap over the same pack, and the client's `ofs-delta` capability is irrelevant. The first pass needed type ordering and two trailers; this needs neither.
- **The trailer is always right.** `write_pack` computes each response's trailer with `gix_hash::Hasher` over the header it wrote (section 9 step 6), so full and blobless responses each get their own SHA-1. The verbatim path (`stream_verbatim`) sends the stored object whole, whose 12-byte header already carries `packs.count` and whose last 20 bytes are the SHA-1 of the bytes before them (2.1, `PackWriter::finish`). No hash state is snapshotted anywhere.
- **The plan cannot point at bytes that are gone.** A pack is immutable once `live` (2.2) and its R2 key survives GRACE = 1 h after `dead_at` (5, Janitor step 3). `clone_plan` checks `p.state = 'live'` for every plan pack in the same sync span that starts the response; a `GcSweep` that runs later, while the stream is in flight, leaves the bytes for an hour, longer than any request lives. A plan whose packs died is a miss, and the miss enqueues a rebuild.
- **The build follows the section 9 rule to the letter.** `run_clone_plan` is `async` only around `read_entries`; `expand` is a `fn` over `MemFind`; the frontier is the cursor (4.2), the bitmaps are saved before every `Continue`, and `finish` is one sync span so a reader sees either the old plan or the new one (section 3's "no await inside the span" argument, platform-facts #4). The walk cannot hit a hole: every object reachable from a live ref is in a live pack (2.5), and a `None` from `lookup` is therefore `Error::Internal`, retried by 4.4.
- **Nothing here sets the alarm or touches the GC tables.** The module calls `jobs::enqueue` only (4.1); `GcMark` is nudged with a future `run_at` so a multi-pack repo converges to one pack and then to the verbatim path; `start` yields to a running GC with `Error::Conflict`, which 4.4 turns into a backoff retry. `marked`, `gc_frontier`, `packs`, `objects`, `refs`, `pushes` are read-only to this module.
- Conformance scenarios this proof must pass (section 11): 1 (no plan, empty repo, miss path), 3 (tags: `TagRef` target in `expand`, peeled objects in the pack), 10 (`blob:none` served from `noblob`; the later `want <blob>` is not a tip and negotiates), 12 (a fetch with `have` never enters the gate). Added scenario 16: push 3 commits, fire the alarm until `clone_plan` has rows, `git clone`, assert `x-ge-subrequests` equals `ceil(pack_bytes / 8 MiB)` and `fsck` is clean; then push one more commit and assert the next clone negotiates and a `ClonePlan` row is queued.

## Changes from the first pass
| First-pass finding (quoted) | How addressed |
|---|---|
| Blocker 1: "pkt-line helper `pkt()` omits the 4 length bytes ... git aborts with 'protocol error: bad line length character'" | No hand-written framing remains. The prelude comes from `wire::PktWriter` (1.1 rule 1, `gix-packetline`, measured against git 2.43 in the spike) and pack bytes go through `Sideband::data` (rule 2). `stream_verbatim` lines `let mut w = PktWriter { .. }` and `Sideband::new(&mut w).data(&win)`. |
| Blocker 2: "Blobless trailer is derived by snapshotting the full pack's SHA-1 at the blob boundary ... Needs a second hasher seeded with the blobless header" | No precomputed trailer exists. Each response is hashed by `write_pack` over its own header (section 9 step 6); the blobless slice is the `noblob` bitmap handed to the same writer (`clone_plan`, last statement). The verbatim path is taken only when the stored object is the response byte for byte, whose trailer is already correct (2.1). |
| Blocker 3: "`writePack` is unspecified and holds all the platform risk: 1,000 subrequests per alarm invocation, chunked multipart state across alarms, ofs-delta topological ordering, and idea #2's loose-object R2 layout has no delta entries to 'copy verbatim'" | There is no pack build. The physical pack is `GcConsolidate`'s (5.2, its multipart resume is that proof's problem). This module's job walks, it does not write R2: `run_clone_plan` spends subrequests only on `read_entries`, bounded by `SliceBudget` (4.3: 400 per slice, `Continue` at 80%). Delta ordering is moot (2.1: no deltas at rest). Storage is packs, not loose objects (2.1). |
| Caveat: "Old-pack lifecycle undefined: no deletion, no grace period, delete-while-streaming behavior unspecified, orphan packs after a crash between multipart complete and the SQL insert need a janitor" | The module owns no R2 object, so it has no lifecycle to define. Pack death and deletion are section 5 (GcSweep marks, Janitor deletes after GRACE); delete-while-streaming is covered by GRACE > request lifetime; `clone_plan` re-checks `p.state = 'live'` per request. A crash mid-build loses at most one slice: bitmaps are saved before each `Continue`, and `clone_plan` reads only the published table. |
| Caveat: "Do not advertise sideband-all or ref-in-want; both change the wire format" | Section 1.1 rule 6 fixes the v2 advertisement and names both as not advertised; `wire` is the only place that writes it. |
| Caveat: "bundle-uri claim is wrong (a bare .pack is not a bundle); packfile-uris is the v2 feature that takes a raw pack" | Claim dropped. Neither `bundle-uri` nor `packfile-uris` is advertised (rule 6, section 12); the pack is served in-band only. bundle-uri stays its own idea. |
| Caveat: "Superset pack ships the whole repo for --single-branch / --branch tag and for non-blob:none filters; valid but not what the client asked for" | Partly addressed. `blob:limit` and any other filter now take the negotiated path (`clone_plan`: `args.filter.is_none() \|\| blobless`), so a client never gets blobs it asked to skip. `--single-branch` still gets the superset (Known limits). |
| Caveat: "O(repo size) R2 Class A writes per debounced push burst and a >=30s+build staleness window; busy repos rarely hit the fast path" | Class A writes: zero, the plan is SQLite rows. Staleness: `CLONE_QUIET_MS` (10 min, same as GC) plus the walk, and `finish` makes the rebuild rate-aware (`quiet = max(10 min, 10 × build duration)`). A busy repo still misses between a push and the next plan; the miss costs exactly one negotiated clone (section 9), not a failure. |
| Review body: "a chunked build that re-reads `refs` per chunk instead of persisting the captured tips would pack a moving target" | `start` captures `tips` into the cursor on the first slice; every later slice walks from the cursor and `finish` publishes the captured list. |
| Review body: "Annotated tags: `git clone` wants tag refs; tags must be packed" | `expand` follows `TagRef::target`, and tag refs are tips, so tag objects and their targets are marked in both bitmaps. |
| Review body: "Progress messages (band 2) are omitted" | Not addressed because `write_pack` (section 9 step 6) owns the stream and the contract's `Sideband::progress` exists for it; the verbatim path sends none, which git tolerates (`no-progress` or silence until the pack ends). |

## Known limits
- **Steady state is usually two packs, not one.** GC exempts packs younger than GRACE (5.1), so after a push the repo holds the consolidated pack plus a young one; the verbatim branch (`stream_verbatim`) is reached only after the `GcMark` nudge in `finish` has consolidated and a rebuild has run. Until then the fast path is `write_pack` over two bitmaps: still no negotiation and no traversal, but one range read per 8 MiB window with `gix_hash::Hasher` over the stream (sha1-checked, collision-detecting; a few seconds of CPU per GB, inside `limits.cpu_ms = 300000`).
- **`--single-branch` and `--branch <old-tag>` get the whole repo.** Correct, superset, but a clone of one small branch of a large repo costs the full pack. A per-tip bitmap (one `clone_plan` row per tip) would fix it and multiply the walk by the ref count; not done here.
- **First clone after a push negotiates.** The plan is demand-driven: the miss enqueues the job and the clone that missed pays section 9's full cost, bounded by its 200,000-commit and 64 MiB caps. A repo above those caps has no clone path at all until the plan exists; the plan job itself has no such cap (its frontier spills to a table like `gc_frontier`, 5.1), so once it runs, clones of any size stream. That ordering, plan-before-first-clone, is what the review asked for and it holds only if something triggers the job before the first clone: `boot` could enqueue `ClonePlan` next to the Janitor (8.2), which is a one-line write-back to the contract not made here.
- **Memory and CPU in the job.** Per round: `load` entries for at most one frontier (commits and trees only, blobs never read), decoded into `MemFind` and cleared each round; bitmaps 1 bit per object per pack. A single 32 MiB tree entry (2.4 cap) plus an 8 MiB window is the worst case, under 128 MB. `Bitmaps::load`/`save` copy every bitmap per slice (125 KB per million objects per pack), negligible. The `clone.tips` `meta` row is the ref count × 41 bytes; a repo with 100,000 refs stores 4 MB of JSON in one TEXT value, parsed on every fast-path clone.
- **Subrequests.** Job: 400 per slice by contract, `Continue` at 80%. Serve: `ceil(bytes / 8 MiB)` for the verbatim path and `write_pack`'s window count otherwise, charged against 9,000 (7.1); a 64 GB pack would exceed it and needs `ObjectBody::stream()` (memo lists it) to make one subrequest carry the whole object. Not written because `Bucket` exposes only `read_range` (1.2) and the stream-to-`Sideband` adapter would be another 30 lines.
- **Waiting for GC uses the retry ladder.** `start` returns `Error::Conflict` while a GC job is queued or running, so 4.4 retries with backoff: 8 attempts span about 3 h, after which the row is `dead` and the next clone miss enqueues a fresh one (dedup ignores `dead`). A GC longer than 3 h therefore delays the plan by one more miss, nothing worse.
- **Unverified, day-1 list.** `SqlStorageValue` blob binding for BLOB columns; `futures_util::stream::unfold` under `worker::Response::from_stream` (S::Ok into Vec<u8>) at runtime; `TagRef::from_bytes`/`target()`, `TreeRefIter` and `EntryMode::is_commit` names against gix-object 0.64.1; that `Reschedule` keeps the cursor (4.2 says "as given", read here as run_at only); `jobs.state` values as strings in `start`'s query; real R2 range reads on multi-GB objects (#6); the DO subrequest limit on a deployed Worker (#7).
- **Write-backs this proof needs.** `SendSet { packs: Vec<PackSlice { pack, bitmap, count, bytes }> }` as the concrete shape of section 9's "one bitmap per pack"; `RepoDo::meta_opt`, `RepoDo::bucket()`, `Sideband::new`, `SliceBudget::spent_80pct()` and `SliceBudget.req: ReqBudget` as helpers the sibling proofs also assume; `schema_version = 2` migration in `boot` for the two tables; `JobKind::ClonePlan` in 1.4 and section 12's "precomputed clone packs" line pointing at this module.

## Depends on
- protocol-v2-only
- refs-sqlite-objects-r2
- content-addressed-r2-keys
- gc-and-repack-alarm
- want-have-negotiation
