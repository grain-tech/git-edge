//! GC chain (CONTRACTS.md 5): GcMark -> GcConsolidate -> GcSweep, ported from
//! research/proofs/gc-and-repack-alarm. All durable state lives in tables (marked, gc_frontier,
//! gc_seen, gc_parts, gc.* meta), so a requeued 'running' row resumes mid-flight and a kill
//! mid-slice replays idempotently.

use std::collections::{HashMap, HashSet, VecDeque};

use gix_hash::ObjectId;
use gix_object::Kind;
use worker::{SqlStorage, SqlStorageValue as V};

use super::{compact, enqueue, Job, JobKind, SliceBudget, SliceOutcome};
use crate::error::Error;
use crate::pack::ingest::extract_links;
use crate::platform::{hex16, now_ms};
use crate::repo_do::RepoDo;
use crate::store::{
    codec, keys, Bucket, Index, ObjLoc, ObjRow, ObjSqlRow, PackId, PackMeta, PackWriter, WriterCkpt,
    PART, PART_MIN,
};

const ROUND: i64 = 512; // frontier ids per round
const ROWS: usize = 9_000; // insert_objects cap is 10_000 (1.2)
const LOAD: u64 = 32 << 20; // A4 read cap per chunk
const SPANS: usize = 64; // A4: one read_entries call charges <= 64 coalesced spans
const BATCH_N: usize = 90; // idx batch, <= A6's 100 bound params
/// One buffered read's ceiling: entries with more wire bytes than this are copied
/// fragment-by-fragment instead of through read_entries (which reads whole entries).
const SPAN: u64 = 8 << 20;
/// gc_tail row size — under the SqlStorage value ceiling; a <PART tail is ~100 rows.
const TAIL_CHUNK: usize = 64 << 10;

/// Every scratch table a GC chain owns — one list so no reset site can forget
/// `gc_pending` (PR5 §7). Kill/init/sweep/purge all iterate this.
pub(crate) const SCRATCH_TABLES: &[&str] =
    &["marked", "gc_frontier", "gc_seen", "gc_parts", "gc_tail", "gc_pending"];

#[derive(serde::Deserialize)]
struct N {
    #[allow(dead_code)]
    n: i64,
}
#[derive(serde::Deserialize)]
struct MinGen {
    n: Option<i64>,
}
#[derive(serde::Deserialize)]
struct S {
    sha: String,
}
#[derive(serde::Deserialize)]
pub(crate) struct P {
    pub pack_id: String,
}
#[derive(serde::Deserialize)]
struct B {
    #[allow(dead_code)]
    pack_id: String,
    #[serde(with = "serde_bytes")]
    bitmap: Vec<u8>,
    count: i64,
}
#[derive(serde::Deserialize)]
struct BM {
    #[serde(with = "serde_bytes")]
    bitmap: Vec<u8>,
}
#[derive(serde::Deserialize)]
struct PE {
    part_no: i64,
    etag: String,
}
/// Barrier pop row: a frontier sha plus the generation its batch pops at (§4.5).
#[derive(serde::Deserialize)]
struct SG {
    sha: String,
    gen: i64,
}
/// One pending-page window row: sha plus its idx inside the pack (§5.2).
#[derive(serde::Deserialize)]
struct PW {
    sha: String,
    idx: i64,
}
/// One gc_pending row as the page span sees it (§5.2).
#[derive(serde::Deserialize)]
struct PD {
    seq: i64,
    pack_id: String,
    cursor: i64,
}

// container-level serde(default): checkpoints written by older binaries must
// still parse — a missing field degrades to its zero value (frag=0 = fresh entry)
#[derive(serde::Serialize, serde::Deserialize, Default, Clone)]
#[serde(default)]
struct Pos {
    ci: u32,
    /// bitmap cursor into cands[ci]: the idx of the next entry to copy — or, when
    /// frag > 0, the idx of the in-flight streamed entry. Only ever points at
    /// entries whose bytes have landed (or are mid-copy at a checkpoint); it must
    /// never run ahead of the WriterCkpt, or a replay silently skips entries.
    idx: u32,
    pack: String,
    upload: String,
    st: Option<WriterCkpt>,
    /// bytes of the in-flight streamed entry already appended at the last
    /// checkpoint — 0 means the next copy of cands[ci].idx starts at its offset.
    /// When `tail > 0`, (ci,idx,frag) name the scan position instead of the
    /// durable boundary: the undrained tail bytes live in gc_tail and resume
    /// continues from here without replaying.
    frag: u64,
    total: u32,
    lo: i64,
    hi: i64,
    /// length of the undrained buffer persisted to gc_tail at the last yield —
    /// 0 means the writer held nothing undrained (or the cursor predates tails).
    tail: u64,
    /// durable-boundary entry (bci,bidx,bfrag) for the replay fallback taken
    /// when a persisted tail is missing/corrupt — only meaningful when tail > 0.
    bci: u32,
    bidx: u32,
    bfrag: u64,
    /// runtime-only: entries already completed inside a just-loaded tail —
    /// skipped on persist, set by the resume path, consumed once by `build`.
    #[serde(skip)]
    ord0: u32,
}

/// One appended entry's span in the NEW pack — the durable-cursor lookup table.
/// `checkpoint` uploads uniform parts whose boundaries ignore entry edges, so the
/// durable offset can land mid-entry; the persisted cursor rewinds to whichever
/// span (or the in-flight `cur`) contains it.
#[derive(Clone, Copy)]
struct Span {
    ci: u32,
    idx: u32,
    ord: u32,
    off: u64,
    len: u64,
}

fn exec(sql: &SqlStorage, q: &str, args: Vec<V>) -> Result<worker::SqlCursor, Error> {
    crate::store::exec(sql, q, args)
}
fn js<T: serde::Serialize + ?Sized>(v: &T) -> Result<V, Error> {
    Ok(serde_json::to_string(v).map_err(|e| Error::Internal(e.to_string()))?.into())
}
pub(crate) fn put(d: &RepoDo, k: &str, v: impl Into<String>) -> Result<(), Error> {
    exec(
        &d.sql(),
        "INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        vec![k.into(), v.into().into()],
    )?;
    Ok(())
}

/// Payload the mark stamps on the consolidate/sweep rows it enqueues: the id of
/// the chain that owns them. See `stray` for how a row outliving its chain exits.
/// A missing gc.chain (repair-resurrected or a pre-field enqueue) degrades to the
/// unguarded payload; the phase guards then decide — a legacy chain still
/// mid-flight is aborted, anything else is Done.
pub(crate) fn chain_payload(d: &RepoDo) -> Result<String, Error> {
    Ok(match d.meta_opt("gc.chain")? {
        Some(c) => format!("{{\"chain\":\"{c}\"}}"),
        None => "{}".into(),
    })
}

/// True when the job row was enqueued by a GC chain that has since been aborted —
/// the payload's `gc.chain` no longer matches (abort wiped it, or a newer mark
/// superseded it). A stray row must run nothing: its build/sweep state described
/// the old chain. Rows with no chain field (repair-resurrected or pre-chain
/// enqueues) pass through to the phase guard, and `legacy_chain` below decides
/// what a pre-field chain's remaining state deserves.
fn stray(d: &RepoDo, job: &Job) -> Result<bool, Error> {
    #[derive(serde::Deserialize)]
    struct C {
        #[serde(default)]
        chain: Option<String>,
    }
    let chain = serde_json::from_str::<C>(&job.payload).map(|c| c.chain).unwrap_or_default();
    Ok(match chain {
        Some(c) => d.meta_opt("gc.chain")?.as_deref() != Some(c.as_str()),
        None => false,
    })
}

/// True while a chain started before gc.phase/gc.chain existed is still
/// mid-flight: no phase, no chain id, but gc.started_at stands (a completed
/// or aborted chain wipes all three). The phase guards would Done its rows
/// and strand gc.pos's MPU plus the ingesting build pack — the fix aborts it
/// like any other broken chain: wipe gc.* and enqueue a fresh mark.
pub(crate) fn legacy_chain(d: &RepoDo) -> Result<bool, Error> {
    Ok(d.meta_opt("gc.phase")?.is_none()
        && d.meta_opt("gc.chain")?.is_none()
        && d.meta_opt("gc.started_at")?.is_some())
}

/// PR5 §1: barrier mode is a property of the persisted chain, never of the
/// env var — every GC arm and every commit_push reads these gc.* rows.
/// Barrier-active ≡ chain id set ∧ started_at set ∧ gc.barrier=1 ∧ mode ≠
/// compact; every chain exit wipes gc.*, so the mode dies with the chain.
pub(crate) fn barrier_active(d: &RepoDo) -> Result<bool, Error> {
    Ok(d.meta_opt("gc.chain")?.is_some()
        && d.meta_opt("gc.started_at")?.is_some()
        && d.meta_opt("gc.barrier")?.as_deref() == Some("1")
        && d.meta_opt("gc.mode")?.as_deref() != Some("compact"))
}

/// The mark-init capture of GE_GC_BARRIER (default off) with the gc_barrier
/// test knob — read once here, persisted as gc.barrier=1 only when on; every
/// arm afterwards reads the meta, never the env.
fn barrier_capture(d: &RepoDo) -> bool {
    crate::testhooks::knob(d, "gc_barrier")
        .map(|v| v != 0)
        .unwrap_or_else(|| d.env_i64("GE_GC_BARRIER", 0) != 0)
}

fn refs_moved(d: &RepoDo) -> Result<bool, Error> {
    Ok(d.meta_i64("refs_version")? != d.meta_i64("gc.refs_version")?)
}

/// Legacy mark guard while phase is build/sweep: compare frozen gc.refs_version to live refs.
fn refs_frozen_moved(d: &RepoDo) -> Result<bool, Error> {
    match d.meta_opt("gc.refs_version")? {
        Some(old) => match old.parse::<i64>() {
            Ok(o) => Ok(o != d.meta_i64("refs_version")?),
            Err(_) => Ok(false),
        },
        None => Ok(false),
    }
}

fn min_frontier_gen(sql: &SqlStorage) -> Result<Option<i64>, Error> {
    Ok(exec(sql, "SELECT MIN(gen) AS n FROM gc_frontier", vec![])?.one::<MinGen>()?.n)
}

/// Barrier §4.6 handoff when gen-0 is drained: `Some(Done)` if consolidate is enqueued.
fn barrier_handoff_if_ready(
    d: &RepoDo,
    sql: &SqlStorage,
    job: &Job,
    chain: Option<&str>,
    min_gen: Option<i64>,
) -> Result<Option<SliceOutcome>, Error> {
    let no_gen0 = match min_gen {
        None => true,
        Some(g) => g >= 1,
    };
    if !no_gen0 {
        return Ok(None);
    }
    if !mark_fence(d, sql, job, chain)? {
        return Ok(Some(SliceOutcome::Done));
    }
    let pending_n = exec(sql, "SELECT COUNT(*) AS n FROM gc_pending", vec![])?.one::<N>()?.n;
    let rounds = d
        .meta_opt("gc.delta_rounds")?
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(0);
    let cap = crate::testhooks::knob(d, "mark_delta_rounds").unwrap_or(8);
    let both_empty = pending_n == 0 && min_gen.is_none();
    if both_empty || (rounds >= cap && refs_moved(d)?) {
        enqueue(sql, JobKind::GcConsolidate, now_ms(), &chain_payload(d)?)?;
        put(d, "gc.phase", "build")?;
        return Ok(Some(SliceOutcome::Done));
    }
    Ok(None)
}

/// Drop every trace of a chain without touching the queue: the §6 keep rule
/// on gc.new_pack, then all gc.* meta, then the scratch tables — gc.* first so
/// a partial kill never leaves gc.new_pack set over an emptied `marked` (the
/// keep predicate reads `marked` while it is still intact). Shared by
/// wipe_all, abort and (through wipe_all) orphan_abort.
pub(crate) fn wipe_all(d: &RepoDo, sql: &SqlStorage) -> Result<(), Error> {
    // §3.2 8b (barrier-only — gc.retire never exists flag-off/compact): a
    // retirement interrupted mid-flight still drops the object rows of its
    // already-dead ids so the janitor can reap them. Lenient parse — a wipe
    // never blocks on a corrupt intent.
    if let Some(it) = d
        .meta_opt("gc.retire")?
        .and_then(|s| serde_json::from_str::<RetireIntent>(&s).ok())
    {
        exec(
            sql,
            "DELETE FROM objects WHERE pack_id IN (SELECT value FROM json_each(?)) \
             AND pack_id NOT IN (SELECT id FROM packs WHERE state='live')",
            vec![js(&it.ids)?],
        )?;
    }
    if let Some(p) = d.meta_opt("gc.new_pack")? {
        let live = exec(
            sql,
            "SELECT COUNT(*) AS n FROM packs WHERE id=? AND state='live'",
            vec![p.clone().into()],
        )?
        .one::<N>()?
            .n
            == 1;
        if live && !compact::cohort_live(d, sql)? {
            // a cohort pack already retired: this build pack is the surviving
            // copy — keep it live and drop the dead cohort's leftover object
            // rows (the janitor only reaps dead packs that have none)
            exec(
                sql,
                "DELETE FROM objects WHERE pack_id IN (SELECT pack_id FROM marked) \
                 AND pack_id NOT IN (SELECT id FROM packs WHERE state='live')",
                vec![],
            )?;
        } else {
            exec(sql, "DELETE FROM objects WHERE pack_id=?", vec![p.clone().into()])?;
            exec(
                sql,
                "UPDATE packs SET state='dead', dead_at=? WHERE id=? AND state IN ('ingesting','live')",
                vec![now_ms().into(), p.into()],
            )?;
        }
    }
    exec(sql, "DELETE FROM meta WHERE key LIKE 'gc.%'", vec![])?;
    for t in SCRATCH_TABLES {
        exec(sql, &format!("DELETE FROM {t}"), vec![])?;
    }
    Ok(())
}

// ---- marked-bitmap helpers (one bit per entry idx) ----
fn bm_set(bm: &mut Vec<u8>, i: u32) -> Result<(), Error> {
    let byte = usize::try_from(i / 8).unwrap_or(usize::MAX);
    // bitmaps are sized (count+7)/8 at mark start — an idx past the end is a corrupt
    // objects row, not a reason to allocate (idx = u32::MAX would try 512 MiB)
    let b = bm
        .get_mut(byte)
        .ok_or_else(|| Error::Internal("idx past pack count".into()))?;
    *b |= 1 << (i % 8);
    Ok(())
}
fn bm_idxs_from(bm: &[u8], from: u32, limit: usize) -> Vec<u32> {
    let mut out = Vec::new();
    let mut i = from;
    while out.len() < limit && usize::try_from(i / 8).map(|b| b < bm.len()).unwrap_or(false) {
        if bm[usize::try_from(i / 8).unwrap_or(0)] & (1 << (i % 8)) != 0 {
            out.push(i);
        }
        i = i.saturating_add(1);
    }
    out
}
fn bm_popcount(bm: &[u8]) -> u32 {
    bm.iter().map(|b| b.count_ones()).sum()
}

/// Group loads so one `read_entries` call charges <= 64 coalesced spans and <= LOAD bytes (A4).
/// Upper bound: <= SPANS entries since a span covers at least one entry.
fn chunks(loads: &[(ObjectId, ObjLoc)]) -> Vec<Vec<(ObjectId, ObjLoc)>> {
    let mut out = Vec::new();
    let (mut cur, mut bytes) = (Vec::new(), 0u64);
    for l in loads {
        if cur.len() >= SPANS || bytes.saturating_add(l.1.size) > LOAD {
            out.push(std::mem::take(&mut cur));
            bytes = 0;
        }
        bytes = bytes.saturating_add(l.1.size);
        cur.push(l.clone());
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

/// GcMark (5.1): exact reachability over live objects; one bit per (pack, idx). A frontier row is
/// deleted only inside the span that records its expansion — a kill mid-round replays it.
pub async fn gc_mark(d: &RepoDo, job: &Job, budget: &mut SliceBudget) -> Result<SliceOutcome, Error> {
    let sql = d.sql();
    // a chain started before gc.phase/gc.chain shipped can also be caught
    // mid-MARK at deploy: gc.started_at stands so the init arm below is
    // skipped (no young-pack seed, no chain id), the resumed walk finishes
    // and stamps gc.phase=build — the chain then builds/sweeps off refs-only
    // marks. Abort it like the consolidate/sweep guards do, before anything
    // resumes or sets the phase.
    if legacy_chain(d)? {
        return abort(d, &sql);
    }
    // R1/T3: a compact chain owns gc.* — a live one reschedules the mark
    // behind the quiet window; an orphaned one is aborted losslessly and the
    // mark falls through to its normal init in this same slice.
    if d.meta_opt("gc.mode")?.as_deref() == Some("compact") {
        if compact::orphaned(d, &sql)? {
            compact::orphan_abort(d, &sql)?;
        } else {
            return Ok(SliceOutcome::Reschedule { run_at: now_ms() + d.gc_quiet_ms() });
        }
    }
    // §4.7 M4: a mark slice while the chain sits at build/sweep — ordered
    // after the legacy_chain and compact guards. Barrier: an orphaned owner
    // aborts losslessly and this slice falls through to init in the same
    // slice (the pushed mark is the chain's only re-owner); a live owner
    // just reschedules behind the quiet window — no writes either way.
    // Legacy: moved refs aborts; stable refs are Done — never the old
    // phase=build re-stamp, which wedged a second consolidate behind the
    // live one (§1.1).
    if matches!(d.meta_opt("gc.phase")?.as_deref(), Some("build") | Some("sweep")) {
        if barrier_active(d)? {
            if compact::orphaned(d, &sql)? {
                compact::orphan_abort(d, &sql)?;
            } else {
                return Ok(SliceOutcome::Reschedule { run_at: now_ms() + d.gc_quiet_ms() });
            }
        } else {
            return if refs_frozen_moved(d)? {
                abort(d, &sql)
            } else {
                Ok(SliceOutcome::Done)
            };
        }
    }
    if d.meta_opt("gc.started_at")?.is_none() {
        // first slice: refuse to overlap a chain already mid-flight
        let busy = exec(
            &sql,
            "SELECT 1 AS n FROM jobs WHERE kind IN ('gc_consolidate','gc_sweep') \
             AND state IN ('queued','running') LIMIT 1",
            vec![],
        )?
        .to_array::<N>()?;
        if !busy.is_empty() {
            return Ok(SliceOutcome::Reschedule { run_at: now_ms() + d.gc_quiet_ms() });
        }
        let now = now_ms();
        // §2: wipe, then the chain id and the frozen refs_version, then the
        // cohort and roots — gc.started_at and gc.barrier land LAST so an Err
        // mid-init is redone from scratch and never leaves a half-active
        // barrier chain (an Err between the two writes stays flag-off, safe)
        for t in SCRATCH_TABLES {
            exec(&sql, &format!("DELETE FROM {t}"), vec![])?;
        }
        exec(&sql, "DELETE FROM meta WHERE key LIKE 'gc.%'", vec![])?;
        // C3: name this chain — consolidate/sweep rows carry it and a row that
        // outlives its chain's abort exits via `stray` instead of wedging
        put(d, "gc.chain", hex16()?)?;
        put(d, "gc.refs_version", d.meta("refs_version")?)?;
        // captured now, persisted at the end — every arm reads the meta
        let barrier = barrier_capture(d);
        // gc_candidate_before_ms knob (testhooks): an absolute candidate
        // cutoff — a test splits packs into old candidates and young keepers
        // without racing a moving now-grace boundary
        let cutoff = crate::testhooks::knob(d, "gc_candidate_before_ms")
            .unwrap_or_else(|| now.saturating_sub(d.gc_grace_ms()));
        exec(
            &sql,
            "INSERT INTO marked(pack_id,bitmap) SELECT id, zeroblob((count+7)/8) FROM packs \
             WHERE state='live' AND created_at < ?",
            vec![cutoff.into()],
        )?;
        let marked_n = exec(&sql, "SELECT COUNT(*) AS n FROM marked", vec![])?.one::<N>()?.n;
        if marked_n == 0 {
            // every live pack is inside the grace window — the chain would
            // walk the whole repo just for begin_build to wipe its own
            // state; the seeds below exist to serve a candidate set. Skip
            // the walk: wipe like begin_build's early-out and stop.
            wipe_all(d, &sql)?;
            return Ok(SliceOutcome::Done);
        }
        // §2 roots at gen 0 (the column default): ref targets ∪ peeled ∪ pins —
        // a pin equals a ref target, so the pin seed is a defensive no-op
        exec(
            &sql,
            "INSERT OR IGNORE INTO gc_frontier(sha) SELECT target FROM refs UNION \
             SELECT peeled FROM refs WHERE peeled IS NOT NULL UNION \
             SELECT sha FROM pins",
            vec![],
        )?;
        // C2 stopgap: a young pack is kept by the grace cutoff but is never a
        // mark root — seed the frontier with every non-blob object of every
        // live non-candidate pack, so everything they link to gets marked
        exec(
            &sql,
            "INSERT OR IGNORE INTO gc_frontier(sha) SELECT o.sha FROM objects o \
             JOIN packs p ON p.id=o.pack_id WHERE p.state='live' AND o.kind != 3 \
             AND o.pack_id NOT IN (SELECT pack_id FROM marked)",
            vec![],
        )?;
        put(d, "gc.started_at", now.to_string())?;
        // §2: the barrier stamp is the LAST init write — between it and
        // started_at an Err leaves a flag-off chain, which is safe
        crate::testhooks::failpoint(d, "gc.mark.init_barrier")?;
        if barrier {
            put(d, "gc.barrier", "1")?;
        }
        crate::testhooks::failpoint(d, "gc.mark.init")?;
    }
    let bucket = d.bucket()?;
    let cand: HashSet<String> = exec(&sql, "SELECT pack_id FROM marked", vec![])?
        .to_array::<P>()?
        .into_iter()
        .map(|r| r.pack_id)
        .collect();
    let mut bits: HashMap<String, Vec<u8>> = HashMap::new(); // touched bitmaps, flushed by commit_ids
    // §4.1: a barrier chain has no refs_version fence in the walk — the sweep
    // is the single retirement fence and the barrier protects every landing.
    // Read once per slice: gc.* only changes under a chain exit, which the
    // lease/heartbeat fences already cover.
    let barrier = barrier_active(d)?;
    // §4.2: the chain this slice serves — captured after any init; every mark
    // write span (both heartbeat sites, the pending page, the empty-frontier
    // transition and the delta_rounds write) fences on it plus the lease CAS
    // and an unset gc.phase, so a stale slice writes nothing in either mode
    let chain = d.meta_opt("gc.chain")?;
    loop {
        if budget.spent_80pct() {
            return Ok(SliceOutcome::Continue { cursor: "{}".into() });
        }
        if !barrier && refs_moved(d)? {
            // §1.2/§4.2: even the legacy walk abort is a fenced write span —
            // a slice whose chain/phase/lease went stale drops its bitmaps
            // instead of wiping a chain that is no longer its own
            if !mark_fence(d, &sql, job, chain.as_deref())? {
                return Ok(SliceOutcome::Done);
            }
            return abort(d, &sql);
        }
        // §4.6: once no gen-0 rows remain, evaluate the handoff BEFORE the
        // next gen-1 pop — a continuous writer keeps gen-1 nonempty forever,
        // and the empty-batch arm alone would never fire (review c1 major).
        // Both-empty still hands off on stable refs; the delta arm hands off
        // with leftover gen-1 / pending (PR5's sweep then aborts; PR6 drains).
        if barrier {
            let min_gen = min_frontier_gen(&sql)?;
            if let Some(out) = barrier_handoff_if_ready(d, &sql, job, chain.as_deref(), min_gen)? {
                return Ok(out);
            }
        }
        match walk_step(d, &sql, job, chain.as_deref(), None, barrier, &cand, &mut bits, &bucket, budget).await? {
            Step::Done => return Ok(SliceOutcome::Done),
            Step::Stepped => {}
            Step::Empty => {
                // Barrier handoff (both-empty and delta) already ran above
                // when no gen-0 remained. Reaching here with barrier means
                // pending still feeds and the delta cap is not yet met —
                // keep paging. Legacy: empty frontier is the only handoff,
                // with gc_seen wiped.
                if !mark_fence(d, &sql, job, chain.as_deref())? {
                    return Ok(SliceOutcome::Done);
                }
                if barrier {
                    continue; // pending still feeds gen-1 — keep draining
                }
                exec(&sql, "DELETE FROM gc_seen", vec![])?;
                // the chain enters build: a consolidate (incl. a chain-less
                // resurrected one) may only run while this phase stands.
                // §4.6: the owner row is enqueued BEFORE the phase is published —
                // an Err between them must never leave a phase with no owner
                enqueue(&sql, JobKind::GcConsolidate, now_ms(), &chain_payload(d)?)?;
                put(d, "gc.phase", "build")?;
                return Ok(SliceOutcome::Done);
            }
        }
    }
}

/// What one walk_step did — the mark loop and the drain loop branch on it.
enum Step {
    /// A fence failed — the caller stops as Done, dropping unflushed bits.
    Done,
    /// The frontier pop found nothing (mark: continue/handoff; drain: back
    /// to the swap check).
    Empty,
    /// One batch's expansion committed (bits flushed, kids admitted).
    Stepped,
}

/// §3.1: the shared mark/drain step — pending page (barrier) → min-gen pop →
/// live-row lookup → bit plan → chunked R2 reads → fenced commit_ids.
/// `want_phase` names the fence: None = the mark (phase unset), Some("sweep")
/// = the sweep drain. `barrier` selects the barrier semantics (pending page,
/// gen-aware kids, the §4.4 miss split); the drain always passes true. Bits
/// are set only in marked.bitmap, never built; misses/kids/flush semantics
/// are exactly the mark's.
#[allow(clippy::too_many_arguments)]
async fn walk_step(
    d: &RepoDo,
    sql: &SqlStorage,
    job: &Job,
    chain: Option<&str>,
    want_phase: Option<&str>,
    barrier: bool,
    cand: &HashSet<String>,
    bits: &mut HashMap<String, Vec<u8>>,
    bucket: &Bucket,
    budget: &mut SliceBudget,
) -> Result<Step, Error> {
    let mark = want_phase.is_none();
    // §5.4: at most one pending page span per iteration, before the pop —
    // pending pages can't starve the snapshot generations
    if barrier && !pending_page(d, sql, job, chain, want_phase)? {
        return Ok(Step::Done);
    }
    // span A: pop a batch, plan marks and loads; nothing commits until a chunk's span B.
    // Barrier mode takes only the minimum generation (§4.5): every gen-0
    // (snapshot) row drains before any gen-1 delta feed is touched
    let (batch, batch_gen): (Vec<String>, i64) = if barrier {
        let rows = exec(
            sql,
            "SELECT sha, gen FROM gc_frontier \
             WHERE gen=(SELECT MIN(gen) FROM gc_frontier) LIMIT ?",
            vec![ROUND.into()],
        )?
        .to_array::<SG>()?;
        let gen = rows.first().map(|r| r.gen).unwrap_or(0);
        (rows.into_iter().map(|r| r.sha).collect(), gen)
    } else {
        (
            exec(sql, "SELECT sha FROM gc_frontier LIMIT ?", vec![ROUND.into()])?
                .to_array::<S>()?
                .into_iter()
                .map(|r| r.sha)
                .collect(),
            0,
        )
    };
    if batch.is_empty() {
        return Ok(Step::Empty);
    }
    // §4.6: one delta round per gen-1 pop span — the counter the
    // handoff's stall arm reads, written under the fence
    if barrier && batch_gen == 1 {
        if !phase_fence(d, sql, job, chain, want_phase)? {
            return Ok(Step::Done);
        }
        delta_round(sql)?;
    }
    let rows = exec(
        sql,
        // GROUP BY keeps one row per sha — N duplicate live packs per sha would
        // otherwise materialize N×batch rows (attacker-seeded OOM, and only the
        // first row per sha is ever used below)
        "SELECT o.sha, o.pack_id, o.idx, o.offset, o.len, o.kind, o.size FROM objects o \
         JOIN packs p ON p.id=o.pack_id WHERE p.state='live' AND \
         o.sha IN (SELECT value FROM json_each(?)) GROUP BY o.sha ORDER BY o.sha",
        vec![js(&batch)?],
    )?
    .to_array::<ObjSqlRow>()?;
    let (mut loads, mut prev) = (Vec::<(ObjectId, ObjLoc)>::new(), String::new());
    let mut marked_sha = false;
    for r in &rows {
        if prev != r.sha {
            prev = r.sha.clone();
            marked_sha = false;
            if r.kind != 3 {
                loads.push(r.loc()?); // blobs never read (5.1)
            }
        }
        if !marked_sha && cand.contains(&r.pack_id) {
            if !bits.contains_key(&r.pack_id) {
                let b = exec(sql, "SELECT bitmap FROM marked WHERE pack_id=?", vec![r.pack_id.clone().into()])?
                    .to_array::<BM>()?
                    .into_iter()
                    .next()
                    .map(|x| x.bitmap)
                    .unwrap_or_default();
                bits.insert(r.pack_id.clone(), b);
            }
            if let Some(bm) = bits.get_mut(&r.pack_id) {
                bm_set(bm, u32::try_from(r.idx).map_err(|_| Error::Internal("idx".into()))?)?;
                marked_sha = true;
            }
        }
    }
    // §4.4: the shas with a live row in this span — the barrier miss
    // split needs it to tell hits from zero-row pops
    let live_shas: HashSet<String> = if barrier {
        rows.iter().map(|r| r.sha.clone()).collect()
    } else {
        HashSet::new()
    };
    let load_ids: HashSet<String> = loads.iter().map(|(id, _)| id.to_string()).collect();
    for chunk in chunks(&loads) {
        let ids: Vec<String> = chunk.iter().map(|(id, _)| id.to_string()).collect();
        // await the whole chunk FIRST — after_read then sits between sync
        // spans, where a crash can't bisect the kids-flush writes below
        let entries = bucket.read_entries_chunked(&chunk, &mut budget.req).await?;
        crate::testhooks::failpoint_async(
            d,
            if mark { "gc.mark.after_read" } else { "gc.drain.after_read" },
        )
        .await?;
        // §4.2 post-await fence: the last await is behind us, so the lease
        // CAS + captured chain + phase gate run before every write in
        // this span — a stale slice (requeued and reclaimed, or a chain
        // that moved on) lands nothing and drops its bitmaps as Done
        // (A19); a live one refreshes started_at so repair never calls
        // it a straggler
        if !phase_fence(d, sql, job, chain, want_phase)? {
            return Ok(Step::Done);
        }
        let mut kids: Vec<String> = Vec::new();
        for (_id, entry) in entries {
            let (kind, data) = codec::decode_entry(&entry)?; // A7 cap inside
            for l in extract_links(kind, &data, d.obj_kind()?)? {
                kids.push(l.to_string());
            }
            // a giant tree can yield hundreds of thousands of links in one chunk —
            // flush admission early rather than OOM mid-slice (an OOM kill never
            // increments attempts and would wedge the job forever)
            if kids.len() > 50_000 {
                commit_ids(d, sql, &[], &[], &kids, bits, batch_gen, barrier)?;
                kids.clear();
            }
        }
        commit_ids(d, sql, &ids, &[], &kids, bits, batch_gen, barrier)?; // span B: expansion commits
        // models a real death only while another await follows in the
        // same slice — at the slice's true end nothing is left to roll back
        if mark {
            crate::testhooks::failpoint(d, "gc.mark.after_commit")?;
        }
        if budget.spent_80pct() {
            break;
        }
    }
    let rest: Vec<String> = batch.iter().filter(|s| !load_ids.contains(*s)).cloned().collect();
    if !rest.is_empty() {
        // same-span fence as the chunk loop's — an all-blob batch ran no
        // read above but still writes
        if !phase_fence(d, sql, job, chain, want_phase)? {
            return Ok(Step::Done);
        }
        if barrier {
            // §4.4: hits (live blob rows) commit like §4.3; misses — pops
            // with no live row — are never seen-recorded and leave the
            // frontier only while still absent
            let (hits, misses): (Vec<String>, Vec<String>) =
                rest.into_iter().partition(|s| live_shas.contains(s.as_str()));
            commit_ids(d, sql, &hits, &misses, &[], bits, batch_gen, true)?;
        } else {
            commit_ids(d, sql, &rest, &[], &[], bits, 0, false)?; // blobs/misses: nothing to read
        }
        if mark {
            crate::testhooks::failpoint(d, "gc.mark.after_commit")?;
        }
    }
    Ok(Step::Stepped)
}

/// One sync span (§4.3, both modes): ids are now expanded — bitmaps flush
/// FIRST so K holds (bits land before seen), then the seen insert, kids at
/// the batch's generation, and the frontier delete LAST, so any Err leaves
/// the batch in the frontier to replay idempotently. `misses` is the
/// barrier-mode zero-row split (§4.4): never seen-recorded, and deleted only
/// while still absent — a SHA that gained a live row during the await stays.
/// `gen` is the batch's frontier generation (barrier mode); `barrier` picks
/// the gen-aware kids upsert and the conditional miss delete.
#[allow(clippy::too_many_arguments)]
fn commit_ids(
    d: &RepoDo,
    sql: &SqlStorage,
    ids: &[String],
    misses: &[String],
    kids: &[String],
    bits: &mut HashMap<String, Vec<u8>>,
    gen: i64,
    barrier: bool,
) -> Result<(), Error> {
    // 1. bitmaps — OR-merge so a duplicate slice's different bits never drop
    for (p, mut bm) in bits.drain() {
        #[derive(serde::Deserialize)]
        struct Cur {
            #[serde(with = "serde_bytes")]
            bitmap: Vec<u8>,
        }
        if let Some(old) = exec(sql, "SELECT bitmap FROM marked WHERE pack_id=?", vec![p.clone().into()])?
            .to_array::<Cur>()?
            .into_iter()
            .next()
        {
            if old.bitmap.len() > bm.len() {
                bm.resize(old.bitmap.len(), 0);
            }
            for (a, b) in bm.iter_mut().zip(old.bitmap.iter()) {
                *a |= *b;
            }
        }
        exec(sql, "UPDATE marked SET bitmap=? WHERE pack_id=?", vec![bm.into(), p.into()])?;
    }
    crate::testhooks::failpoint(d, "gc.mark.commit_ids.after_bits")?;
    // 2. seen
    exec(sql, "INSERT OR IGNORE INTO gc_seen(sha) SELECT value FROM json_each(?)", vec![js(ids)?])?;
    crate::testhooks::failpoint(d, "gc.mark.commit_ids.after_seen")?;
    // 3. kids not in gc_seen — barrier mode tags them with the batch's
    //    generation through the min-gen upsert (§4.5); legacy rows stay 0
    if barrier {
        exec(
            sql,
            "INSERT INTO gc_frontier(sha, gen) SELECT j.value, ? FROM json_each(?) j \
             WHERE NOT EXISTS(SELECT 1 FROM gc_seen s WHERE s.sha=j.value) \
             ON CONFLICT(sha) DO UPDATE SET gen=MIN(gc_frontier.gen, excluded.gen)",
            vec![V::from(gen), js(kids)?],
        )?;
    } else {
        exec(
            sql,
            "INSERT OR IGNORE INTO gc_frontier(sha) SELECT j.value FROM json_each(?) j \
             WHERE NOT EXISTS(SELECT 1 FROM gc_seen s WHERE s.sha=j.value)",
            vec![js(kids)?],
        )?;
    }
    crate::testhooks::failpoint(d, "gc.mark.commit_ids.after_kids")?;
    // 4. frontier delete LAST — hits unconditionally; misses only while they
    //    still have no live row (§4.4)
    exec(sql, "DELETE FROM gc_frontier WHERE sha IN (SELECT value FROM json_each(?))", vec![js(ids)?])?;
    if barrier && !misses.is_empty() {
        exec(
            sql,
            "DELETE FROM gc_frontier WHERE sha IN (SELECT value FROM json_each(?)) \
             AND NOT EXISTS(SELECT 1 FROM objects o JOIN packs p ON p.id=o.pack_id \
             WHERE p.state='live' AND o.sha=gc_frontier.sha)",
            vec![js(misses)?],
        )?;
    }
    Ok(())
}

/// §3.1/§4.2: the fence every mark or drain write span passes — the lease
/// CAS, `gc.chain ==` the id this slice captured, and the phase gate
/// `want_phase` names: None = the mark's "phase unset", Some(p) = the
/// drain's `gc.phase == p`. A lost lease is still `stale_lease` (the
/// outcome span reports `stale`); a moved chain or a wrong phase answers
/// `Ok(false)` and the caller stops as Done, dropping its local bitmaps.
fn phase_fence(
    d: &RepoDo,
    sql: &SqlStorage,
    job: &Job,
    chain: Option<&str>,
    want_phase: Option<&str>,
) -> Result<bool, Error> {
    // §4.2: chain/phase first — a resumed slice whose chain was aborted must
    // not refresh jobs.started_at before returning Done (review c1)
    if d.meta_opt("gc.chain")?.as_deref() != chain {
        return Ok(false);
    }
    match want_phase {
        None => {
            if d.meta_opt("gc.phase")?.is_some() {
                return Ok(false);
            }
        }
        Some(p) => {
            if d.meta_opt("gc.phase")?.as_deref() != Some(p) {
                return Ok(false);
            }
        }
    }
    if !super::heartbeat(sql, job)? {
        return Err(super::stale_lease());
    }
    Ok(true)
}

/// The mark's write fence — `phase_fence` with the phase-unset gate.
fn mark_fence(d: &RepoDo, sql: &SqlStorage, job: &Job, chain: Option<&str>) -> Result<bool, Error> {
    phase_fence(d, sql, job, chain, None)
}

/// §4.6: one delta round per gen-1 pop span and per pending page span — the
/// persisted counter the handoff's stall arm reads. The caller holds the
/// mark fence; the upsert keeps it monotone (it is wiped with gc.* on exit).
fn delta_round(sql: &SqlStorage) -> Result<(), Error> {
    exec(
        sql,
        "INSERT INTO meta(key,value) VALUES('gc.delta_rounds','1') \
         ON CONFLICT(key) DO UPDATE SET value=CAST(CAST(value AS INTEGER)+1 AS TEXT)",
        vec![],
    )?;
    Ok(())
}

/// §5: one fenced pending-page span per mark/drain iteration (barrier mode
/// only), run before the frontier pop so pending work can't starve the
/// snapshot generations. The drain first deletes rows whose pack is absent
/// or no longer live; then the oldest live row's next idx window feeds
/// gc_frontier at gen 1 (minus gc_seen, min-gen upsert), the cursor
/// advances, and the row deletes once a short page has covered the pack —
/// frontier insert → cursor → delete, so a failure never loses a page.
/// `want_phase` names the phase fence (None for the mark, Some("sweep")
/// for the drain); `Ok(false)` means the fence failed: stop as Done.
fn pending_page(
    d: &RepoDo,
    sql: &SqlStorage,
    job: &Job,
    chain: Option<&str>,
    want_phase: Option<&str>,
) -> Result<bool, Error> {
    let n = exec(sql, "SELECT COUNT(*) AS n FROM gc_pending", vec![])?.one::<N>()?.n;
    if n == 0 {
        return Ok(true);
    }
    if !phase_fence(d, sql, job, chain, want_phase)? {
        return Ok(false);
    }
    // §5.3 drain: a row whose pack is gone or not live protects nothing
    exec(
        sql,
        "DELETE FROM gc_pending WHERE pack_id NOT IN \
         (SELECT id FROM packs WHERE state='live')",
        vec![],
    )?;
    let Some(row) = exec(
        sql,
        "SELECT seq, pack_id, cursor FROM gc_pending ORDER BY seq LIMIT 1",
        vec![],
    )?
    .to_array::<PD>()?
    .into_iter()
    .next() else {
        delta_round(sql)?;
        return Ok(true);
    };
    // knobs: the test values keep the windows small; production defaults
    // are 50 000 idx / 5 000 rows per span (§5.2)
    let window = crate::testhooks::knob(d, "pending_window").unwrap_or(50_000).max(1);
    let page_rows = crate::testhooks::knob(d, "pending_page").unwrap_or(5_000).max(1);
    let hi = row.cursor.saturating_add(window);
    let page = exec(
        sql,
        "SELECT sha, idx FROM objects WHERE pack_id=? AND idx>? AND idx<=? AND kind != 3 \
         ORDER BY idx LIMIT ?",
        vec![
            V::from(row.pack_id.as_str()),
            V::from(row.cursor),
            V::from(hi),
            V::from(page_rows),
        ],
    )?
    .to_array::<PW>()?;
    // §5.2: the window's non-blobs enter at gen 1, minus gc_seen — the same
    // min-gen upsert the barrier's ref feed uses, so an earlier generation
    // is never overwritten
    let shas: Vec<String> = page.iter().map(|r| r.sha.clone()).collect();
    exec(
        sql,
        "INSERT INTO gc_frontier(sha, gen) SELECT j.value, 1 FROM json_each(?) j \
         WHERE NOT EXISTS(SELECT 1 FROM gc_seen s WHERE s.sha=j.value) \
         ON CONFLICT(sha) DO UPDATE SET gen=MIN(gc_frontier.gen, excluded.gen)",
        vec![js(&shas)?],
    )?;
    crate::testhooks::failpoint(d, "gc.mark.page.after_frontier")?;
    // a full page resumes at its last idx; a short page has covered the
    // whole window, so the cursor jumps to the window's end either way
    let page_rows_usize = usize::try_from(page_rows).unwrap_or(usize::MAX);
    let full = page.len() >= page_rows_usize;
    let new_cursor = if full {
        page.last().map(|r| r.idx).unwrap_or(hi)
    } else {
        hi
    };
    exec(
        sql,
        "UPDATE gc_pending SET cursor=? WHERE seq=?",
        vec![V::from(new_cursor), V::from(row.seq)],
    )?;
    crate::testhooks::failpoint(d, "gc.mark.page.after_cursor")?;
    let max_idx = exec(
        sql,
        "SELECT COALESCE(MAX(idx),-1) AS n FROM objects WHERE pack_id=?",
        vec![V::from(row.pack_id.as_str())],
    )?
    .one::<N>()?
    .n;
    // §5.2: a short page whose cursor reached the pack's last idx is done —
    // frontier insert → cursor → delete, so a failure never loses a page
    if !full && new_cursor >= max_idx {
        exec(sql, "DELETE FROM gc_pending WHERE seq=?", vec![V::from(row.seq)])?;
    }
    delta_round(sql)?;
    Ok(true)
}

/// S4's fence, shared by every post-await write in consolidate and its
/// callees in both modes: the PR1 heartbeat CAS plus `gc.chain == captured`
/// (compact mode additionally requires `gc.new_pack == pos.pack`). A chain
/// that moved on answers `false` — the caller stops as Done with no writes; a
/// lost lease still errors `stale_lease`.
pub(crate) fn chain_fence(
    d: &RepoDo,
    sql: &SqlStorage,
    job: &Job,
    chain: Option<&str>,
    pack: &str,
) -> Result<bool, Error> {
    if !super::heartbeat(sql, job)? {
        return Err(super::stale_lease());
    }
    if d.meta_opt("gc.chain")?.as_deref() != chain {
        return Ok(false);
    }
    if d.meta_opt("gc.mode")?.as_deref() == Some("compact")
        && d.meta_opt("gc.new_pack")?.as_deref() != Some(pack)
    {
        return Ok(false);
    }
    Ok(true)
}

/// The 5.3 abort, shared by every arm: the shared kill helper (keep rule on
/// gc.new_pack, gc.* then scratch), then a fresh mark after the quiet window.
/// A compact chain's abort is lossless — it enqueues nothing; the next
/// trigger restarts compaction.
fn abort(d: &RepoDo, sql: &SqlStorage) -> Result<SliceOutcome, Error> {
    let compact = d.meta_opt("gc.mode")?.as_deref() == Some("compact");
    wipe_all(d, sql)?;
    if !compact {
        enqueue(sql, JobKind::GcMark, now_ms() + d.gc_quiet_ms(), "{}")?;
    }
    Ok(SliceOutcome::Done)
}

pub(crate) enum Flow {
    Yield,
    Done,
    Retry(Error),
    Rebuild,
}

/// GcConsolidate (5.2): one new normalized pack holding exactly the marked entries, verbatim.
pub async fn gc_consolidate(d: &RepoDo, job: &Job, budget: &mut SliceBudget) -> Result<SliceOutcome, Error> {
    let sql = d.sql();
    // a chain started before gc.phase/gc.chain shipped is still mid-flight:
    // the guards below would just drop this row and strand gc.pos's MPU and
    // the ingesting build pack — abort it instead so the wipe and a fresh
    // mark clean the slate. Checked before stray so a chain-stamped row
    // hitting that state aborts too.
    if legacy_chain(d)? {
        return abort(d, &sql);
    }
    // C3: a consolidate row from an aborted chain must not resume its build
    if stray(d, job)? {
        return Ok(SliceOutcome::Done);
    }
    // a chain-less row (repair resurrection carries payload "{}") may only
    // build while a live chain actually reached the build phase — building
    // off a younger chain's half-filled marks would repack a wrong set
    if d.meta_opt("gc.phase")?.as_deref() != Some("build") {
        return Ok(SliceOutcome::Done);
    }
    // S4: the chain this slice serves — captured before the first await;
    // chain_fence fences every post-await write on it from here down
    let chain = d.meta_opt("gc.chain")?;
    // R7: a compact slice whose frozen cohort already broke aborts losslessly
    // on entry — a lossless abort beats "marked entry lacks objects row" retries
    if d.meta_opt("gc.mode")?.as_deref() == Some("compact") && !compact::cohort_live(d, &sql)? {
        wipe_all(d, &sql)?;
        return Ok(SliceOutcome::Done);
    }
    let bucket = d.bucket()?;
    loop {
        let Some(mut pos) = (match d.meta_opt("gc.pos")? {
            Some(j) => Some(
                // a corrupt cursor is not "nothing to repack" — Done would wipe the job
                // and strand marked/gc_parts/the ingesting pack forever. Fail loudly.
                serde_json::from_str::<Pos>(&j)
                    .map_err(|e| Error::Internal(format!("corrupt gc.pos: {e}")))?,
            ),
            None => {
                let p = begin_build(d, &sql)?;
                // a crash means a build began — begin_build's None arm already
                // committed the terminal "nothing to repack" writes and owns
                // no crash point
                if p.is_some() {
                    crate::testhooks::failpoint(d, "gc.consolidate.begin")?;
                }
                p
            }
        }) else {
            return Ok(SliceOutcome::Done); // nothing to repack: state already wiped
        };
        let key = keys::pack(&bucket.repo, &PackId(pos.pack.clone()));
        let etags: Vec<String> = exec(&sql, "SELECT part_no, etag FROM gc_parts ORDER BY part_no", vec![])?
            .to_array::<PE>()?
            .into_iter()
            .enumerate()
            .map(|(i, r)| {
                // resume re-derives part numbers from position — a gap would silently
                // rebind every later etag to the wrong part number
                if r.part_no != i64::try_from(i).unwrap_or(0) + 1 {
                    Err(Error::Internal("gc_parts not contiguous".into()))
                } else {
                    Ok(r.etag)
                }
            })
            .collect::<Result<_, _>>()?;
        if pos.st.is_none() && !pos.upload.is_empty() {
            // upload id persisted but no checkpoint: nothing was uploaded — the MPU
            // is an orphan; abort it rather than recreate-and-leak
            if let Ok(mpu) = bucket.inner.resume_multipart_upload(&key, &pos.upload) {
                mpu.abort().await.ok();
            }
            pos.upload.clear();
        }
        let mut out = if pos.upload.is_empty() {
            let w = PackWriter::create(&bucket, &key, pos.total, d.obj_kind()?, &mut budget.req).await?;
            pos.upload = w.upload_id().await;
            // S4: the gc.pos persist must not land under a dead lease or a moved chain
            if !chain_fence(d, &sql, job, chain.as_deref(), &pos.pack)? {
                return Ok(SliceOutcome::Done);
            }
            // persist immediately: a kill before the first checkpoint would otherwise
            // leave the id invisible, and the orphan MPU would sit in R2 (~7d) instead
            // of being aborted on the next slice
            put(d, "gc.pos", serde_json::to_string(&pos).map_err(|e| Error::Internal(e.to_string()))?)?;
            w
        } else {
            PackWriter::resume(
                &bucket,
                &key,
                &pos.upload,
                &etags,
                pos.st.as_ref().ok_or_else(|| Error::Internal("gc.pos without WriterCkpt".into()))?,
                &mut budget.req,
            )
            .await?
        };
        // gc_part_bytes knob (testhooks): a shrunken uniform part makes
        // consolidate multi-part on a tiny repo — after_part becomes reachable.
        // Clamped: R2 refuses non-final parts under PART_MIN even in dev.
        out.set_part_size(
            crate::testhooks::knob(d, "gc_part_bytes")
                .and_then(|v| usize::try_from(v).ok())
                .unwrap_or(PART)
                .clamp(PART_MIN, PART),
        );
        // Fold a yield-persisted tail back in. (bci,bidx,bfrag) is the durable
        // boundary recorded beside the scan cursor: a short/corrupt tail can't be
        // trusted, so the fallback rewinds the scan to it and replays — the rows
        // and etags the tail covered were already posted, and replay is
        // byte-identical, so insert_objects' ON CONFLICT absorbs them.
        #[derive(serde::Deserialize)]
        struct TB {
            #[serde(with = "serde_bytes")]
            blob: Vec<u8>,
        }
        let tail: Vec<u8> = exec(&sql, "SELECT blob FROM gc_tail ORDER BY seq", vec![])?
            .to_array::<TB>()?
            .into_iter()
            .flat_map(|r| r.blob)
            .collect();
        // S4: a stale or chain-outlived slice must not delete the persisted tail
        if !chain_fence(d, &sql, job, chain.as_deref(), &pos.pack)? {
            return Ok(SliceOutcome::Done);
        }
        exec(&sql, "DELETE FROM gc_tail", vec![])?;
        if pos.tail > 0 {
            let base = pos.st.as_ref().map(|s| s.pos).unwrap_or(0);
            if tail.len() as u64 == pos.tail {
                #[derive(serde::Deserialize)]
                struct Tally {
                    n: i64,
                }
                #[derive(serde::Deserialize)]
                struct CSpan {
                    lo: Option<i64>,
                    hi: Option<i64>,
                }
                // entries that completed inside the tail posted their rows at the
                // yield — count them so the writer's totals and the build ordinal
                // pick up where the slice left off
                let n: u32 = exec(
                    &sql,
                    "SELECT COUNT(*) AS n FROM objects WHERE pack_id=? AND offset+len>? AND offset+len<=?",
                    vec![
                        pos.pack.clone().into(),
                        i64::try_from(base).unwrap_or(0).into(),
                        i64::try_from(base.saturating_add(pos.tail)).unwrap_or(i64::MAX).into(),
                    ],
                )?
                .to_array::<Tally>()?
                .into_iter()
                .next()
                .map(|t| u32::try_from(t.n).unwrap_or(0))
                .unwrap_or(0);
                let cs = exec(
                    &sql,
                    "SELECT MIN(offset) AS lo, MAX(offset+len) AS hi FROM objects \
                     WHERE pack_id=? AND offset+len>? AND offset+len<=? AND kind=1",
                    vec![
                        pos.pack.clone().into(),
                        i64::try_from(base).unwrap_or(0).into(),
                        i64::try_from(base.saturating_add(pos.tail)).unwrap_or(i64::MAX).into(),
                    ],
                )?
                .to_array::<CSpan>()?
                .into_iter()
                .next()
                .unwrap_or(CSpan { lo: None, hi: None });
                out.resume_tail(
                    &tail,
                    n,
                    cs.lo.and_then(|v| u64::try_from(v).ok()).unwrap_or(u64::MAX),
                    cs.hi.and_then(|v| u64::try_from(v).ok()).unwrap_or(0),
                );
                pos.ord0 = n;
            } else {
                // tail bytes lost/short — replay from the durable boundary
                (pos.ci, pos.idx, pos.frag) = (pos.bci, pos.bidx, pos.bfrag);
            }
            pos.tail = 0;
        }
        match build(d, &sql, &bucket, &key, out, &mut pos, job, budget, chain.as_deref()).await? {
            Flow::Yield => return Ok(SliceOutcome::Continue { cursor: "{}".into() }),
            Flow::Done => return Ok(SliceOutcome::Done),
            Flow::Retry(e) => return Err(e), // 4.4 backoff; second failure -> Rebuild
            Flow::Rebuild => wipe_build(d, &sql)?, // dead upload: marks intact, restart
        }
    }
}

/// Fill >= 5 MiB parts with the marked entries in (pack_id, idx) order — deterministic, so a
/// killed slice replays the same bytes to the same part numbers and insert_objects' ON CONFLICT
/// DO NOTHING absorbs replayed rows.
async fn build(
    d: &RepoDo,
    sql: &SqlStorage,
    bucket: &Bucket,
    key: &str,
    mut out: PackWriter,
    pos: &mut Pos,
    job: &Job,
    budget: &mut SliceBudget,
    chain: Option<&str>,
) -> Result<Flow, Error> {
    let cands: Vec<String> = exec(sql, "SELECT pack_id FROM marked ORDER BY pack_id", vec![])?
        .to_array::<P>()?
        .into_iter()
        .map(|r| r.pack_id)
        .collect();
    let (mut staged, mut bm): (Vec<ObjRow>, (u32, Vec<u8>)) = (vec![], (u32::MAX, vec![]));
    // ord0 = entries that completed inside a just-loaded tail — their ordinals are
    // already spent; the next scanned entry continues after them
    let mut ordinal: u32 = pos
        .st
        .as_ref()
        .map(|s| s.count)
        .unwrap_or(0)
        .saturating_add(pos.ord0);
    // appended-entry spans + the in-flight entry: the durable-cursor table for flush
    let mut spans: VecDeque<Span> = VecDeque::new();
    let mut cur: Option<Span> = None;
    loop {
        if budget.spent_80pct() {
            // persist_tail: the undrained buffer must survive the slice or a
            // sparse mark set (≈1 read/entry) can never buffer a full part —
            // gc.pos would replay the same span every slice forever
            if let Some(f) = flush(
                d, sql, bucket, key, &mut out, pos, &mut staged, &mut spans, &cur, job, budget,
                true, chain,
            )
            .await?
            {
                return Ok(f);
            }
            return Ok(Flow::Yield);
        }
        let mut idxs: Vec<u32> = Vec::new();
        while idxs.is_empty() {
            match cands.get(pos.ci as usize) {
                // input exhausted: finish owns tail+trailer as the last part (any size allowed)
                None => {
                    Index(sql).insert_objects(&PackId(pos.pack.clone()), &staged)?;
                    staged.clear();
                    return match out.finish(&mut budget.req).await {
                        Ok(meta) => {
                            // R12: after the MPU completes, before the swap's final
                            // fence — the pause a push or clone interleaves through
                            if d.meta_opt("gc.mode")?.as_deref() == Some("compact") {
                                crate::testhooks::failpoint_async(d, "gc.compact.before_swap").await?;
                            }
                            finish_build(d, sql, pos, &meta, job, chain)
                        }
                        Err(e) => mpu_err(d, sql, bucket, key, pos, e, budget, job, chain).await,
                    };
                }
                Some(cid) => {
                    if bm.0 != pos.ci {
                        // §2 (C11): the frozen as-copied snapshot drives the
                        // copy — a drain mark that lands after admission can
                        // never enlarge the build's input set
                        bm.1 = exec(
                            sql,
                            "SELECT COALESCE(built, bitmap) AS bitmap FROM marked WHERE pack_id=?",
                            vec![cid.clone().into()],
                        )?
                        .to_array::<BM>()?
                        .into_iter()
                        .next()
                        .map(|x| x.bitmap)
                        .unwrap_or_default();
                        bm.0 = pos.ci;
                    }
                    idxs = bm_idxs_from(&bm.1, pos.idx, BATCH_N);
                    // pos.idx is NOT advanced at planning time: it is the persisted
                    // copy cursor and may only move once a chunk's bytes have landed
                    // — advancing it here would let a checkpoint claim entries that
                    // were never copied, and a yield would then skip them forever
                    if idxs.is_empty() {
                        pos.ci = pos.ci.saturating_add(1);
                        pos.idx = 0;
                    }
                }
            }
        }
        let rows = exec(
            sql,
            // ORDER BY idx makes the build fully deterministic: a replay after a kill
            // must reproduce identical append offsets, or ON CONFLICT keeps stale rows
            "SELECT sha, pack_id, idx, offset, len, kind, size FROM objects WHERE pack_id=? \
             AND idx IN (SELECT value FROM json_each(?)) ORDER BY idx",
            vec![cands.get(pos.ci as usize).cloned().unwrap_or_default().into(), js(&idxs)?],
        )?
        .to_array::<ObjSqlRow>()?;
        if rows.len() != idxs.len() {
            return Err(Error::Internal("marked entry lacks objects row".into()));
        }
        let locs: Vec<(ObjectId, ObjLoc)> =
            rows.iter().map(ObjSqlRow::loc).collect::<Result<_, _>>()?;
        for chunk in chunks(&locs) {
            if budget.spent_80pct() {
                // the inner batch can run ~90 reads — still bounded; persist_tail
                // carries the undrained buffer into the next slice
                if let Some(f) = flush(
                    d, sql, bucket, key, &mut out, pos, &mut staged, &mut spans, &cur, job,
                    budget, true, chain,
                )
                .await?
                {
                    return Ok(f);
                }
                return Ok(Flow::Yield);
            }
            // An entry too big for one buffered read can't ride read_entries (it
            // materializes whole entries and refuses > 48 MiB). Small entries coalesce
            // as before; big ones stream SPAN-sized fragments straight into the writer
            // via raw_extend, checkpointing mid-entry so a slice boundary or a kill
            // never loses more than ~one buffered part of progress. Order stays the
            // chunk's idx order, so replay is byte-identical.
            let mut got: HashMap<ObjectId, VecDeque<Vec<u8>>> = HashMap::new();
            // a frag-resumed small entry rides the streamed path — exclude it here or
            // its read_entries bytes would be fetched but never consumed
            let small: Vec<(ObjectId, ObjLoc)> = chunk
                .iter()
                .filter(|(_, l)| u64::from(l.len) <= SPAN && !(l.idx == pos.idx && pos.frag > 0))
                .cloned()
                .collect();
            for (id, entry) in bucket.read_entries_chunked(&small, &mut budget.req).await? {
                got.entry(id).or_default().push_back(entry);
            }
            for (id, l) in &chunk {
                pos.idx = l.idx; // the in-flight cursor: any checkpoint now names this entry
                if pos.frag > u64::from(l.len) {
                    return Err(Error::Internal("gc.pos frag past entry end".into()));
                }
                let (off, len) = if u64::from(l.len) > SPAN || pos.frag > 0 {
                    // start = the entry's true offset in the new pack — on a mid-entry
                    // resume the writer offset already includes pos.frag of this entry
                    let start = out.offset().saturating_sub(pos.frag);
                    cur = Some(Span {
                        ci: pos.ci,
                        idx: l.idx,
                        ord: ordinal,
                        off: start,
                        len: u64::from(l.len),
                    });
                    let src = keys::pack(&bucket.repo, &l.pack);
                    let end = l.offset.saturating_add(u64::from(l.len));
                    // resume mid-entry: pos.frag is bytes already appended & checkpointed
                    let mut p = l.offset.saturating_add(pos.frag);
                    while p < end {
                        if budget.spent_80pct() {
                            // persist the tail so mid-entry progress survives: the
                            // frag bytes already buffered go to gc_tail
                            if let Some(f) = flush(
                                d, sql, bucket, key, &mut out, pos, &mut staged, &mut spans, &cur,
                                job, budget, true, chain,
                            )
                            .await?
                            {
                                return Ok(f);
                            }
                            return Ok(Flow::Yield);
                        }
                        let frag = bucket
                            .read_range(&src, p, end.saturating_sub(p).min(SPAN), &mut budget.req)
                            .await?;
                        if p == l.offset {
                            // parity with append_stored's entry_header check: the
                            // wire header must be a full object matching objects.kind
                            let (k, _, _) = crate::store::codec::entry_header(&frag)?;
                            if k != l.kind {
                                return Err(Error::Internal("objects.kind disagrees with wire header".into()));
                            }
                        }
                        p = p.saturating_add(u64::try_from(frag.len()).map_err(|_| Error::Internal("frag".into()))?);
                        out.raw_extend(&frag);
                        pos.frag = p.saturating_sub(l.offset);
                        if let Some(f) = flush(
                            d, sql, bucket, key, &mut out, pos, &mut staged, &mut spans, &cur,
                            job, budget, false, chain,
                        )
                        .await?
                        {
                            return Ok(f);
                        }
                    }
                    let r = out.raw_entry_done(l.kind, start)?;
                    spans.push_back(cur.take().ok_or_else(|| {
                        Error::Internal("streamed entry missing cur span".into())
                    })?);
                    r
                } else {
                    let entry = got
                        .get_mut(id)
                        .and_then(|q| q.pop_front())
                        .ok_or_else(|| Error::Storage("entry missing from read".into()))?;
                    let r = out.append_stored(&entry)?; // verbatim; sha+count inside
                    spans.push_back(Span {
                        ci: pos.ci,
                        idx: l.idx,
                        ord: ordinal,
                        off: r.0,
                        len: u64::from(r.1),
                    });
                    r
                };
                staged.push(ObjRow {
                    sha: *id,
                    idx: ordinal,
                    offset: off,
                    len,
                    kind: l.kind,
                    size: l.size,
                    parents: None, // restage: the graph rows already exist from ingest
                });
                ordinal = ordinal.saturating_add(1);
                pos.frag = 0;
                pos.idx = l.idx.saturating_add(1);
                if l.kind == Kind::Commit {
                    pos.lo = pos.lo.min(i64::try_from(off).unwrap_or((1 << 53) - 1));
                    pos.hi = pos
                        .hi
                        .max(i64::try_from(off.saturating_add(u64::from(len))).unwrap_or((1 << 53) - 1));
                }
            }
            // flush per chunk, not per batch: `out.part` would otherwise accumulate the
            // whole 90-entry batch (~1.4 GiB of buffered pack bytes) and OOM the isolate
            if let Some(f) = flush(
                d, sql, bucket, key, &mut out, pos, &mut staged, &mut spans, &cur, job, budget,
                false, chain,
            )
            .await?
            {
                return Ok(f);
            }
        }
        // a multi-MiB chunk loop can run past the 60 s straggler window on slow R2 —
        // chain_fence keeps repair from requeueing a live consolidate into a duplicate
        // slice, and a duplicate that lost the row or outlived its chain stops before
        // its next write (A19/S4). It runs BEFORE the staged rows land.
        if !chain_fence(d, sql, job, chain, &pos.pack)? {
            return Ok(Flow::Done);
        }
        if staged.len() >= ROWS {
            Index(sql).insert_objects(&PackId(pos.pack.clone()), &staged)?;
            staged.clear();
        }
        if let Some(f) = flush(
            d, sql, bucket, key, &mut out, pos, &mut staged, &mut spans, &cur, job, budget,
            false, chain,
        )
        .await?
        {
            return Ok(f);
        }
    }
}

/// Upload every full PART-sized part, then commit — one span — the rows, the etags and
/// the position. Parts are uniform because R2 rejects complete() when non-final part
/// sizes differ. The durable boundary (WriterCkpt.pos = appended minus buffered) can
/// land mid-entry and even inside an entry that finished copying.
///
/// `persist_tail=false` (mid-slice): the persisted cursor rewinds (ci, idx, frag) to
/// the entry containing the boundary — a resume re-copies only bytes that were never
/// uploaded. `persist_tail=true` (yield): the undrained <PART tail is written to
/// gc_tail and the cursor instead records the *scan* position plus the boundary
/// entry separately (bci/bidx/bfrag) — a resume loads the tail into the writer and
/// continues without replaying. Without it a sparse-mark pack (≈1 read per entry)
/// can never buffer a whole part inside the request budget: every slice would
/// re-copy the same undrained span forever.
#[allow(clippy::too_many_arguments)]
async fn flush(
    d: &RepoDo,
    sql: &SqlStorage,
    bucket: &Bucket,
    key: &str,
    out: &mut PackWriter,
    pos: &mut Pos,
    staged: &mut Vec<ObjRow>,
    spans: &mut VecDeque<Span>,
    cur: &Option<Span>,
    job: &Job,
    budget: &mut SliceBudget,
    persist_tail: bool,
    chain: Option<&str>,
) -> Result<Option<Flow>, Error> {
    let mut news: Vec<(u16, String)> = Vec::new();
    let mut st = None;
    while out.buffered() >= out.part_size() {
        match out.checkpoint(&mut budget.req).await {
            Ok(Some((n, e, s))) => {
                news.push((n, e));
                st = Some(s);
            }
            Ok(None) => break,
            Err(e) => return mpu_err(d, sql, bucket, key, pos, e, budget, job, chain).await.map(Some),
        }
        crate::testhooks::failpoint(d, "gc.consolidate.after_part")?;
    }
    // a yield persists even when nothing drained — the tail is the progress
    let mut st = match st {
        Some(s) => s,
        None if persist_tail => out.snapshot(),
        None => return Ok(None),
    };
    // S4 before the checkpoint span's writes — a stale or chain-outlived slice
    // must not land rows, etags or the cursor under the replacement lease
    if !chain_fence(d, sql, job, chain, &pos.pack)? {
        return Ok(Some(Flow::Done));
    }
    Index(sql).insert_objects(&PackId(pos.pack.clone()), staged)?; // OR IGNORE: replay-safe
    staged.clear();
    for (n, e) in news {
        exec(
            sql,
            "INSERT OR REPLACE INTO gc_parts(part_no,etag) VALUES(?,?)",
            vec![i64::from(n).into(), e.into()],
        )?;
    }
    // etags posted by a slice that died before its checkpoint landed sit past the
    // durable prefix — prune so a resume never feeds them to complete()
    exec(
        sql,
        "DELETE FROM gc_parts WHERE part_no > ?",
        vec![V::from(i64::try_from(st.pos / out.part_size()).unwrap_or(0))],
    )?;
    // durable = last uploaded byte. If it falls short of appended, the boundary
    // lands inside an entry — mid-slice persists rewind to it; a yield keeps the
    // scan cursor and records the boundary entry separately for the replay path.
    let durable = st.pos;
    let mut pp = pos.clone();
    if durable < out.offset() {
        let hit = cur
            .filter(|c| durable >= c.off)
            .or_else(|| spans.iter().copied().find(|s| durable >= s.off && durable < s.off + s.len));
        match hit {
            Some(s) => {
                st.count = s.ord; // entries fully durable = the boundary entry's ordinal
                if persist_tail {
                    (pp.bci, pp.bidx, pp.bfrag) = (s.ci, s.idx, durable - s.off);
                } else {
                    (pp.ci, pp.idx, pp.frag) = (s.ci, s.idx, durable - s.off);
                    // keep the fallback boundary fresh too — a later yield reuses it
                    (pp.bci, pp.bidx, pp.bfrag) = (pp.ci, pp.idx, pp.frag);
                }
            }
            None => {
                // the boundary entry predates this slice's spans only when nothing
                // drained at all (durable == the loaded checkpoint) — reuse the
                // persisted boundary. durable==0 means nothing ever uploaded: the
                // fallback replays from scratch.
                if persist_tail && durable == 0 {
                    (pp.bci, pp.bidx, pp.bfrag) = (0, 0, 0);
                    st.count = 0;
                } else if persist_tail && pos.st.as_ref().map(|s| s.pos) == Some(durable) {
                    (pp.bci, pp.bidx, pp.bfrag) = (pos.bci, pos.bidx, pos.bfrag);
                    st.count = pos.st.as_ref().map(|s| s.count).unwrap_or(st.count);
                } else {
                    return Err(Error::Internal(format!(
                        "durable boundary {durable} outside appended spans"
                    )));
                }
            }
        }
    } else if persist_tail {
        // fully drained at yield — the fallback boundary is the scan position
        (pp.bci, pp.bidx, pp.bfrag) = (pp.ci, pp.idx, pp.frag);
    }
    pp.tail = if persist_tail { out.buffered() } else { 0 };
    if persist_tail {
        // the tail replaces the previous generation wholesale; chunk under the
        // SqlStorage value-size ceiling
        exec(sql, "DELETE FROM gc_tail", vec![])?;
        for chunk in out.buffered_bytes().chunks(TAIL_CHUNK) {
            exec(
                sql,
                "INSERT INTO gc_tail(blob) VALUES(?)",
                vec![V::from(chunk.to_vec())],
            )?;
        }
    }
    pp.st = Some(st);
    put(
        d,
        "gc.pos",
        serde_json::to_string(&pp).map_err(|e| Error::Internal(e.to_string()))?,
    )?;
    pos.st = pp.st;
    // spans behind the durable boundary can't contain a later one — durable is monotone
    while spans.front().map(|s| s.off + s.len <= durable) == Some(true) {
        spans.pop_front();
    }
    // checkpoint span: refresh the repair lease — the same S4 fence, so a
    // stale or chain-outlived slice stops instead of heartbeating onward
    if !chain_fence(d, sql, job, chain, &pos.pack)? {
        return Ok(Some(Flow::Done));
    }
    Ok(None)
}

/// complete() may have succeeded before an error/kill: head() on the key is the oracle. A truly
/// dead upload (expired MPU, aborted) gives no key: retry once through 4.4, then rebuild from
/// the intact marks.
async fn mpu_err(
    d: &RepoDo,
    sql: &SqlStorage,
    bucket: &Bucket,
    key: &str,
    pos: &Pos,
    e: Error,
    budget: &mut SliceBudget,
    job: &Job,
    chain: Option<&str>,
) -> Result<Flow, Error> {
    budget.req.charge(1)?;
    let head = bucket.inner.head(key).await?;
    // S4: the fails counter and the HEAD-recovery finish fence alike
    if !chain_fence(d, sql, job, chain, &pos.pack)? {
        return Ok(Flow::Done);
    }
    if let Some(h) = head {
        // R12: the same pause as the normal finish path — after the head
        // await, before the swap's final fence
        if d.meta_opt("gc.mode")?.as_deref() == Some("compact") {
            crate::testhooks::failpoint_async(d, "gc.compact.before_swap").await?;
        }
        // complete() had already won. pos.lo/hi are already the full commit
        // span here: a HEAD hit follows only a completed upload, and a replay
        // from the last checkpoint stays under one part because the part size
        // is constant (PART in production; the gc_part_bytes knob is
        // test-only), so every commit the pack holds has already folded into
        // the in-memory pos.
        let meta = PackMeta {
            pack: PackId(pos.pack.clone()),
            push_id: None,
            count: pos.total,
            bytes: h.size(),
            commit_lo: u64::try_from(pos.lo).unwrap_or(u64::MAX),
            commit_hi: u64::try_from(pos.hi).unwrap_or(0),
            created_at: now_ms(),
        };
        return finish_build(d, sql, pos, &meta, job, chain);
    }
    let f = d.meta_i64("gc.fails").unwrap_or(0).saturating_add(1);
    put(d, "gc.fails", f.to_string())?;
    Ok(if f >= 2 { Flow::Rebuild } else { Flow::Retry(e) })
}

/// First build slice (5.2): repack only if >= 2 candidates or any candidate has unmarked
/// entries — otherwise wipe gc state and Done, because sweeping without a new pack would
/// delete reachable objects.
fn begin_build(d: &RepoDo, sql: &SqlStorage) -> Result<Option<Pos>, Error> {
    // §2: compact mode skips the refs_version checks outright — it sets no
    // gc.refs_version, and a moved version must not read as "aborted" here
    let compact = d.meta_opt("gc.mode")?.as_deref() == Some("compact");
    if !compact {
        // C3: a missing gc.refs_version means the chain was aborted since this row
        // was enqueued — the abort already wiped the state, so there is nothing to
        // build (a Storage error here wedged the job: retry, backoff, dead)
        if d.meta_opt("gc.refs_version")?.is_none() {
            return Ok(None);
        }
        if barrier_active(d)? {
            // §4.8: the refs_version abort is gone in barrier mode — the
            // sweep is the single retirement fence. What remains is the
            // cohort liveness guard: a dead cohort pack means the frozen
            // bitmaps describe bytes that no longer exist, so the chain
            // wipes like any other broken chain
            if !compact::cohort_live(d, sql)? {
                wipe_all(d, sql)?;
                return Ok(None);
            }
            // §2 (C11): freeze the as-copied snapshot — `total`/`unmarked`
            // below read `built`, never the live bitmap, so a mark that lands
            // after admission can't enlarge what the swap's subset check
            // compares against. A full re-admission re-snapshots; a rebuild
            // (wipe_build) keeps `built` and re-copies the same bits.
            exec(sql, "UPDATE marked SET built=bitmap", vec![])?;
        } else if refs_moved(d)? {
            abort(d, sql)?;
            return Ok(None);
        }
    }
    let cands = exec(
        sql,
        "SELECT m.pack_id, COALESCE(m.built, m.bitmap) AS bitmap, p.count FROM marked m \
         JOIN packs p ON p.id=m.pack_id ORDER BY m.pack_id",
        vec![],
    )?
    .to_array::<B>()?;
    let (mut total, mut unmarked) = (0u32, false);
    for c in &cands {
        let n = bm_popcount(&c.bitmap);
        total = total.saturating_add(n);
        unmarked |= i64::from(n) < c.count;
    }
    if total == 0 && !cands.is_empty() {
        if compact {
            // S11: a compact chain never enters the sweep phase — wipe and stop
            wipe_all(d, sql)?;
        } else {
            // nothing reachable in any candidate: no repack to write — sweep deletes them outright
            exec(sql, "DELETE FROM gc_parts", vec![])?;
            exec(sql, "DELETE FROM gc_tail", vec![])?;
            exec(sql, "DELETE FROM meta WHERE key IN ('gc.pos','gc.new_pack','gc.fails')", vec![])?;
            // §4.6: the sweep row exists before the phase is published —
            // gc.build.handoff is the error-mode point between them
            enqueue(sql, JobKind::GcSweep, now_ms(), &chain_payload(d)?)?;
            crate::testhooks::failpoint(d, "gc.build.handoff")?;
            put(d, "gc.phase", "sweep")?;
            // §3.3: the try/slice baseline publishes with the phase
            if barrier_active(d)? {
                swap_meta_store(d, &SwapMeta { rv: d.meta_i64("refs_version")?, tries: 0, slices: 0 })?;
            }
        }
        return Ok(None);
    }
    if !(cands.len() >= 2 || unmarked) {
        // nothing to repack (a single fully-marked candidate is kept as-is):
        // wipe the chain's state and stop — the new_pack arm inside covers a
        // half-written leftover with no pos
        wipe_all(d, sql)?;
        return Ok(None);
    }
    // a retried begin re-enters here with gc.new_pack still set — the prior build pack
    // has push_id IS NULL, which the janitor deliberately never touches, so dead-mark
    // it now or the row (and eventually its R2 key) is orphaned forever
    if let Some(p) = d.meta_opt("gc.new_pack")? {
        exec(sql, "DELETE FROM objects WHERE pack_id=?", vec![p.clone().into()])?;
        exec(
            sql,
            "UPDATE packs SET state='dead', dead_at=? WHERE id=? AND state='ingesting'",
            vec![now_ms().into(), p.into()],
        )?;
    }
    // stale gc_parts from a dead build must not survive into the new upload — resume
    // numbers parts by these rows, so leftovers would rebind etags to wrong part numbers
    exec(sql, "DELETE FROM gc_parts", vec![])?;
    exec(sql, "DELETE FROM gc_tail", vec![])?;
    let pack = PackId::random()?;
    exec(
        sql,
        "INSERT INTO packs(id,state,count,bytes,commit_lo,commit_hi,push_id,created_at) \
         VALUES(?,'ingesting',0,0,?,0,NULL,?)",
        vec![pack.0.as_str().into(), ((1i64 << 53) - 1).into(), now_ms().into()],
    )?;
    let pos = Pos { pack: pack.0.clone(), total, lo: (1 << 53) - 1, ..Pos::default() };
    put(d, "gc.new_pack", pack.0.clone())?;
    put(d, "gc.pos", serde_json::to_string(&pos).map_err(|e| Error::Internal(e.to_string()))?)?;
    Ok(Some(pos))
}

fn wipe_build(d: &RepoDo, sql: &SqlStorage) -> Result<(), Error> {
    if let Some(p) = d.meta_opt("gc.new_pack")? {
        exec(sql, "DELETE FROM objects WHERE pack_id=?", vec![p.clone().into()])?;
        exec(
            sql,
            "UPDATE packs SET state='dead', dead_at=? WHERE id=? AND state='ingesting'",
            vec![now_ms().into(), p.into()],
        )?;
    }
    exec(sql, "DELETE FROM gc_parts", vec![])?;
    exec(sql, "DELETE FROM gc_tail", vec![])?;
    exec(sql, "DELETE FROM meta WHERE key IN ('gc.pos','gc.new_pack','gc.fails')", vec![])?;
    Ok(())
}

fn finish_build(
    d: &RepoDo,
    sql: &SqlStorage,
    pos: &Pos,
    meta: &PackMeta,
    job: &Job,
    chain: Option<&str>,
) -> Result<Flow, Error> {
    // covers both callers — the normal finish path and mpu_err's HEAD-recovery
    crate::testhooks::failpoint(d, "gc.consolidate.before_finish")?;
    // §5: a compact chain swaps in place of the phase=sweep handoff — N goes
    // live, the cohort dies, scratch wipes, no epoch bump, no gc_sweep row
    if d.meta_opt("gc.mode")?.as_deref() == Some("compact") {
        return compact::swap(d, sql, &pos.pack, meta, job, chain);
    }
    // S4 before the live flip + sweep enqueue — a stale or chain-outlived
    // slice must not land either (A19)
    if !chain_fence(d, sql, job, chain, &pos.pack)? {
        return Ok(Flow::Done);
    }
    exec(
        sql,
        "UPDATE packs SET state='live', count=?, bytes=?, commit_lo=?, commit_hi=? \
         WHERE id=? AND state IN ('ingesting','live')",
        vec![
            i64::from(meta.count).into(),
            i64::try_from(meta.bytes).map_err(|_| Error::Internal("bytes".into()))?.into(),
            i64::try_from(meta.commit_lo).unwrap_or((1 << 53) - 1).min((1 << 53) - 1).into(),
            i64::try_from(meta.commit_hi).unwrap_or(0).into(),
            pos.pack.as_str().into(),
        ],
    )?;
    if d.changes()? != 1 {
        return Err(Error::Internal("build pack row".into()));
    }
    // §4.6: the owner row is enqueued BEFORE the phase is published — same
    // order as the begin_build total==0 arm
    enqueue(sql, JobKind::GcSweep, now_ms(), &chain_payload(d)?)?; // the only deleter (5.3)
    put(d, "gc.phase", "sweep")?;
    // §3.3: the try/slice baseline publishes with the phase — a sweep slice
    // that finds it absent initialises it identically without charging a try
    if barrier_active(d)? {
        swap_meta_store(d, &SwapMeta { rv: d.meta_i64("refs_version")?, tries: 0, slices: 0 })?;
    }
    Ok(Flow::Done) // gc.* cleared by the sweep span
}

/// GcSweep (5.3 + PR6 §3): flag-off and pre-PR6 (built IS NULL) barrier chains
/// keep the PR5 path — the whole step is one sync span: refs_version check,
/// the row deletes and the gc_epoch bump are atomic. A barrier chain instead
/// drains the write side's reachability inside this same job, then runs the
/// fenced subset-aware swap (or the bounded-retry lossless fallback). Still
/// never touches R2 for the deletes: dead keys are the Janitor's, after GRACE.
pub async fn gc_sweep(d: &RepoDo, job: &Job, budget: &mut SliceBudget) -> Result<SliceOutcome, Error> {
    let sql = d.sql();
    // a chain started before gc.phase/gc.chain shipped is still mid-flight:
    // the guards below would just drop this row and strand gc.pos's MPU and
    // the ingesting build pack — abort it instead so the wipe and a fresh
    // mark clean the slate
    if legacy_chain(d)? {
        return abort(d, &sql);
    }
    // C3: a sweep row from an aborted chain must not delete anything
    if stray(d, job)? {
        return Ok(SliceOutcome::Done);
    }
    // §2 defence in depth: a compact chain never has a sweep phase — Done now
    if d.meta_opt("gc.mode")?.as_deref() == Some("compact") {
        return Ok(SliceOutcome::Done);
    }
    // claim-only point: the slice never awaits, so a real death rolls the
    // whole span back — "crashed here" == "died before the slice ran"
    crate::testhooks::failpoint(d, "gc.sweep.before")?;
    // a chain-less sweep row may only delete while a live chain actually
    // finished a build — resurrected rows carry no chain id
    if d.meta_opt("gc.phase")?.as_deref() != Some("sweep") {
        return Ok(SliceOutcome::Done);
    }
    if d.meta_opt("gc.refs_version")?.is_none() {
        return Ok(SliceOutcome::Done);
    }
    // §2 deploy rule: a flag-off chain takes the PR5 path outright, and a
    // barrier chain whose cohort predates the built snapshot (any marked row
    // has built IS NULL) is a pre-PR6 chain — the same PR5 path, never a
    // drain, a subset check or a gc_last_swap record
    if !barrier_active(d)? || built_null(&sql)? {
        return pr5_sweep(d, &sql);
    }
    // §3: a barrier chain — no refs_version check anywhere below: the drain
    // and the fenced swap are the retirement decision
    let chain = d.meta_opt("gc.chain")?;
    // §3.2 8a: a recorded retire intent resumes before any drain work
    if let Some(out) = retire_resume(d, &sql, job, chain.as_deref())? {
        return Ok(out);
    }
    let bucket = d.bucket()?;
    let cand: HashSet<String> = exec(&sql, "SELECT pack_id FROM marked", vec![])?
        .to_array::<P>()?
        .into_iter()
        .map(|r| r.pack_id)
        .collect();
    let mut bits: HashMap<String, Vec<u8>> = HashMap::new();
    loop {
        if budget.spent_80pct() {
            // §3.3: a sweep slice ending Continue charges one drain slice —
            // written under the phase fence so a stale slice bumps nothing
            if phase_fence(d, &sql, job, chain.as_deref(), Some("sweep"))? {
                let mut sm = swap_meta(d)?;
                sm.slices += 1;
                swap_meta_store(d, &sm)?;
            }
            return Ok(SliceOutcome::Continue { cursor: "{}".into() });
        }
        // §3.2: the swap span at the top of every drain iteration — sync,
        // with no await between it and the preceding commit_ids and no R2
        // reads inside
        match swap_check(d, &sql, job, chain.as_deref())? {
            Check::Done | Check::Finished => return Ok(SliceOutcome::Done),
            Check::Drain => {}
        }
        match walk_step(d, &sql, job, chain.as_deref(), Some("sweep"), true, &cand, &mut bits, &bucket, budget).await? {
            Step::Done => return Ok(SliceOutcome::Done),
            Step::Empty | Step::Stepped => {}
        }
    }
}

/// The PR5 sweep path — flag-off chains and pre-PR6 (built IS NULL) barrier
/// chains: refs_moved → abort, else epoch-first whole-cohort retirement.
/// Byte-for-byte the pre-PR6 sweep.
fn pr5_sweep(d: &RepoDo, sql: &SqlStorage) -> Result<SliceOutcome, Error> {
    if refs_moved(d)? {
        return abort(d, sql);
    }
    // §1.4b: epoch bump FIRST (a refs-moved abort above never reaches it),
    // then dead-mark, then the row delete, then gc.*, then scratch — a kill
    // anywhere leaves either the whole retirement or a keep-rule-clean slate
    exec(sql, "UPDATE meta SET value=CAST(CAST(value AS INTEGER)+1 AS TEXT) WHERE key='gc_epoch'", vec![])?;
    exec(
        sql,
        "UPDATE packs SET state='dead', dead_at=? WHERE id IN (SELECT pack_id FROM marked)",
        vec![now_ms().into()],
    )?;
    // error-mode point: the cohort is dead-marked but its rows still stand —
    // the keep rule's partial-retirement window
    crate::testhooks::failpoint(d, "gc.sweep.after_deadmark")?;
    exec(sql, "DELETE FROM objects WHERE pack_id IN (SELECT pack_id FROM marked)", vec![])?;
    // the delete span really ran — the chain completed, so repair's bounded
    // resurrection counters reset here; a stray or no-op sweep Done must not
    exec(sql, "DELETE FROM meta WHERE key LIKE 'resurrected.gc_%'", vec![])?;
    exec(sql, "DELETE FROM meta WHERE key LIKE 'gc.%'", vec![])?;
    for t in SCRATCH_TABLES {
        exec(sql, &format!("DELETE FROM {t}"), vec![])?;
    }
    Ok(SliceOutcome::Done)
}

/// §2 deploy rule's oracle: any marked row whose `built` snapshot is NULL —
/// a pre-PR6 barrier cohort (or a flag-off chain, which never reaches the
/// barrier path anyway).
fn built_null(sql: &SqlStorage) -> Result<bool, Error> {
    Ok(exec(sql, "SELECT COUNT(*) AS n FROM marked WHERE built IS NULL", vec![])?.one::<N>()?.n > 0)
}

/// §3.3's one-row bookkeeping {"rv","tries","slices"} — always written as a
/// single upsert so a baseline and a count can't tear.
#[derive(serde::Serialize, serde::Deserialize, Default, Clone)]
#[serde(default)]
struct SwapMeta {
    rv: i64,
    tries: i64,
    slices: i64,
}

/// The gc.swap row — absent (deploy, or an Err between the sweep enqueue and
/// the publish) initialises to the same baseline the publish span writes,
/// WITHOUT charging a try.
fn swap_meta(d: &RepoDo) -> Result<SwapMeta, Error> {
    match d.meta_opt("gc.swap")?.and_then(|s| serde_json::from_str(&s).ok()) {
        Some(sm) => Ok(sm),
        None => Ok(SwapMeta { rv: d.meta_i64("refs_version")?, ..SwapMeta::default() }),
    }
}

fn swap_meta_store(d: &RepoDo, sm: &SwapMeta) -> Result<(), Error> {
    put(d, "gc.swap", serde_json::to_string(sm).map_err(|e| Error::Internal(e.to_string()))?)
}

/// §3.2 step 7's resumable-retirement record {"ids","lossy","fallback"} —
/// the first write of a retirement span, so a resume knows exactly which
/// packs the one-statement dead-mark targeted and which path chose them.
#[derive(serde::Serialize, serde::Deserialize)]
struct RetireIntent {
    ids: Vec<String>,
    lossy: bool,
    fallback: bool,
}

/// What the top-of-iteration swap check decided.
enum Check {
    /// The phase fence failed — the caller stops as Done.
    Done,
    /// Frontier or pending still has work — run a walk_step.
    Drain,
    /// A terminal span (swap, fallback, or a wipe arm) committed — Done.
    Finished,
}

/// §3.2/§3.3: the fenced swap check — one sync span evaluated at the top of
/// every drain iteration (slice entry or right after the preceding
/// commit_ids, with no await in between and no R2 reads). Frontier+pending
/// empty runs steps 4–11; non-empty charges a try iff refs_version moved
/// and falls through to the fallback trigger.
fn swap_check(d: &RepoDo, sql: &SqlStorage, job: &Job, chain: Option<&str>) -> Result<Check, Error> {
    // step 1: the barrier fence — every write below sits inside its span
    if !phase_fence(d, sql, job, chain, Some("sweep"))? {
        return Ok(Check::Done);
    }
    // claim-only — the check span never awaits, so a real death rolls back
    crate::testhooks::failpoint(d, "gc.swap.before")?;
    // step 2 (PR5 §5.3): a pending row whose pack is gone protects nothing
    exec(
        sql,
        "DELETE FROM gc_pending WHERE pack_id NOT IN (SELECT id FROM packs WHERE state='live')",
        vec![],
    )?;
    // the absent-row baseline initialises here WITHOUT charging a try —
    // the deploy-over-PR5 and died-before-publish cases (§3.3)
    let mut sm = swap_meta(d)?;
    // step 3: BOTH emptiness checks are mandatory before a swap
    let frontier_n = exec(sql, "SELECT COUNT(*) AS n FROM gc_frontier", vec![])?.one::<N>()?.n;
    let pending_n = exec(sql, "SELECT COUNT(*) AS n FROM gc_pending", vec![])?.one::<N>()?.n;
    if frontier_n == 0 && pending_n == 0 {
        swap_meta_store(d, &sm)?;
        fenced_swap(d, sql, &sm)?; // steps 4–11
        return Ok(Check::Finished);
    }
    // §3.3: the trigger reads the counts BEFORE this check's try is
    // charged — a try is a *completed* non-empty check that saw refs move,
    // so K=1 still lets the first moved check drain once and the fallback
    // fires on the next non-empty check
    let k = crate::testhooks::knob(d, "swap_tries").unwrap_or(8);
    let s = crate::testhooks::knob(d, "swap_max_slices").unwrap_or(64);
    if sm.tries >= k || sm.slices >= s {
        // §3.3: the bounded-retry lossless fallback — sync, fenced (this
        // span), frontier may be non-empty; it never enqueues a mark
        fallback_span(d, sql, &sm)?;
        return Ok(Check::Finished);
    }
    // §3.3: a try = a non-empty check that also saw refs_version move —
    // writes between checks (incl. delete-only pushes) count, over-counting
    // is deliberate; an absent baseline persists unchanged
    let rv = d.meta_i64("refs_version")?;
    if rv != sm.rv {
        sm.rv = rv;
        sm.tries += 1;
    }
    swap_meta_store(d, &sm)?; // always ONE upsert — baseline or counts, never torn
    Ok(Check::Drain)
}

/// §3.2 steps 4–11 for the quiesced drain: cohort match → build-pack live →
/// the C11 subset check → 6a or the retirement span. Runs inside the check
/// span's fence.
fn fenced_swap(d: &RepoDo, sql: &SqlStorage, sm: &SwapMeta) -> Result<(), Error> {
    // step 4: a non-live cohort pack is a corruption guard, not an error
    if !compact::cohort_live(d, sql)? {
        return wipe_path(d, sql, sm, false);
    }
    // step 5: a published sweep must not retire a chain whose build pack
    // never went live — wipe like 5a. The total==0 arm has no gc.new_pack
    // and passes.
    if let Some(p) = d.meta_opt("gc.new_pack")? {
        let live = exec(
            sql,
            "SELECT COUNT(*) AS n FROM packs WHERE id=? AND state='live'",
            vec![p.into()],
        )?
        .one::<N>()?
            .n
            == 1;
        if !live {
            return wipe_path(d, sql, sm, false);
        }
    }
    // step 6: P is retirable iff bitmap & !built == 0 — every entry marked
    // now was copied into the build; lossy iff the build missed entries
    // (popcount(built) < count), which retires objects -> epoch bump
    #[derive(serde::Deserialize)]
    struct Row {
        pack_id: String,
        #[serde(with = "serde_bytes")]
        bitmap: Vec<u8>,
        built_null: i64,
        #[serde(with = "serde_bytes")]
        built: Vec<u8>,
        count: i64,
    }
    let rows = exec(
        sql,
        "SELECT m.pack_id, m.bitmap, (m.built IS NULL) AS built_null, \
         COALESCE(m.built, x'') AS built, p.count FROM marked m \
         JOIN packs p ON p.id=m.pack_id",
        vec![],
    )?
    .to_array::<Row>()?;
    let mut ids: Vec<String> = Vec::new();
    let mut lossy = false;
    for r in &rows {
        if r.built_null == 0 && bm_bitmap_subset(&r.bitmap, &r.built) {
            lossy |= bm_lossy(&r.built, r.count);
            ids.push(r.pack_id.clone());
        }
    }
    // step 6a: nothing is duplicated — cohort intact, so wipe_all dead-marks
    // the build pack; steps 9–11 still land the record
    if ids.is_empty() {
        return wipe_path(d, sql, sm, false);
    }
    retire_span(d, sql, RetireIntent { ids, lossy, fallback: false }, sm)
}

/// §3.3: the bounded-retry lossless fallback — steps 1 (the caller's span
/// fence), 4, 5/5a, then the duplicate-aware retirable predicate and the
/// net-positive rule. Frontier may be non-empty; it never enqueues a mark.
fn fallback_span(d: &RepoDo, sql: &SqlStorage, sm: &SwapMeta) -> Result<(), Error> {
    // step 4
    if !compact::cohort_live(d, sql)? {
        return wipe_path(d, sql, sm, true);
    }
    // step 5 + the build pack's byte count for the net-positive rule
    let mut n_bytes = 0i64;
    if let Some(p) = d.meta_opt("gc.new_pack")? {
        #[derive(serde::Deserialize)]
        struct PB {
            state: String,
            bytes: i64,
        }
        let row = exec(sql, "SELECT state, bytes FROM packs WHERE id=?", vec![p.into()])?
            .to_array::<PB>()?
            .into_iter()
            .next();
        match row {
            Some(r) if r.state == "live" => n_bytes = r.bytes,
            _ => return wipe_path(d, sql, sm, true),
        }
    }
    // P retirable iff every entry without a live copy in a non-cohort pack
    // was built — the copy may be the build pack's, another candidate's or
    // a younger pack's, so repeats stay duplicate-aware
    #[derive(serde::Deserialize)]
    struct Row {
        pack_id: String,
        built_null: i64,
        #[serde(with = "serde_bytes")]
        built: Vec<u8>,
        bytes: i64,
    }
    #[derive(serde::Deserialize)]
    struct I {
        idx: i64,
    }
    let rows = exec(
        sql,
        "SELECT m.pack_id, (m.built IS NULL) AS built_null, COALESCE(m.built, x'') AS built, \
         p.bytes FROM marked m JOIN packs p ON p.id=m.pack_id",
        vec![],
    )?
    .to_array::<Row>()?;
    let mut ids: Vec<String> = Vec::new();
    let mut ret_bytes = 0i64;
    for r in &rows {
        let need: Vec<i64> = exec(
            sql,
            "SELECT idx FROM objects o WHERE o.pack_id=? AND NOT EXISTS(\
                 SELECT 1 FROM objects o2 JOIN packs p2 ON p2.id=o2.pack_id \
                 WHERE o2.sha=o.sha AND p2.state='live' AND o2.pack_id NOT IN \
                 (SELECT pack_id FROM marked))",
            vec![V::from(r.pack_id.as_str())],
        )?
        .to_array::<I>()?
        .into_iter()
        .map(|i| i.idx)
        .collect();
        if r.built_null == 0 && bm_all_set(&r.built, &need) {
            ret_bytes = ret_bytes.saturating_add(r.bytes);
            ids.push(r.pack_id.clone());
        }
    }
    // net-positive: retire at least bytes(N) of cohort — an empty set or a
    // net loss goes to 6a (wipe_all dead-marks N) instead
    if ids.is_empty() || ret_bytes < n_bytes {
        return wipe_path(d, sql, sm, true);
    }
    // steps 7–11 with lossy=false, fallback=true — NO epoch bump
    retire_span(d, sql, RetireIntent { ids, lossy: false, fallback: true }, sm)
}

/// §3.2 step 7 → the retire intent (the retirement's first write), the
/// lossy epoch bump (precedes every destructive write) and steps 8–11.
fn retire_span(d: &RepoDo, sql: &SqlStorage, it: RetireIntent, sm: &SwapMeta) -> Result<(), Error> {
    // step 7: ONE upsert — 8a resumes on it after any Err below
    put(d, "gc.retire", serde_json::to_string(&it).map_err(|e| Error::Internal(e.to_string()))?)?;
    if it.lossy {
        // the bump precedes every destructive write of a lossy retirement
        exec(sql, "UPDATE meta SET value=CAST(CAST(value AS INTEGER)+1 AS TEXT) WHERE key='gc_epoch'", vec![])?;
        crate::testhooks::failpoint(d, "gc.swap.after_epoch")?;
    }
    retire_finish(d, sql, &it, sm)
}

/// §3.2 steps 8–11 — the one-statement dead-mark (a no-op on the 8a L==0
/// resume), the recorded failpoint, the object-row delete and the
/// end-of-chain bookkeeping.
fn retire_finish(d: &RepoDo, sql: &SqlStorage, it: &RetireIntent, sm: &SwapMeta) -> Result<(), Error> {
    // step 8: exactly the recorded ids, in one statement — a push that lands
    // between the check and here links at live objects only, so a recorded
    // pack that is still live can still go
    exec(
        sql,
        "UPDATE packs SET state='dead', dead_at=? WHERE id IN (SELECT value FROM json_each(?)) \
         AND state='live'",
        vec![now_ms().into(), js(&it.ids)?],
    )?;
    // kept = cohort packs still live now the retire set is dead
    let stats = swap_stats(sql, &it.ids)?;
    crate::testhooks::failpoint(
        d,
        if it.fallback { "gc.swap.fallback.after_deadmark" } else { "gc.swap.after_deadmark" },
    )?;
    exec(
        sql,
        "DELETE FROM objects WHERE pack_id IN (SELECT value FROM json_each(?))",
        vec![js(&it.ids)?],
    )?;
    end_of_chain(d, sql, &stats, it.lossy, it.fallback, sm)
}

/// §3.2 8a: a sweep slice entry that finds gc.retire set resumes the
/// recorded retirement — Some(outcome) when the chain ended (or the fence
/// failed); None when the all-live arm deleted the intent and the caller
/// must run the full drain + check again.
fn retire_resume(
    d: &RepoDo,
    sql: &SqlStorage,
    job: &Job,
    chain: Option<&str>,
) -> Result<Option<SliceOutcome>, Error> {
    let Some(raw) = d.meta_opt("gc.retire")? else {
        return Ok(None);
    };
    if !phase_fence(d, sql, job, chain, Some("sweep"))? {
        return Ok(Some(SliceOutcome::Done));
    }
    let sm = swap_meta(d)?;
    let it: Option<RetireIntent> = serde_json::from_str(&raw).ok();
    let live_n = match &it {
        Some(it) if !it.ids.is_empty() => exec(
            sql,
            "SELECT COUNT(*) AS n FROM packs WHERE id IN (SELECT value FROM json_each(?)) \
             AND state='live'",
            vec![js(&it.ids)?],
        )?
        .one::<N>()?
        .n,
        _ => -1,
    };
    match it {
        // L == len(ids): the Err hit before the one-statement dead-mark —
        // delete the intent and re-validate from step 2 (a push since may
        // have linked into a recorded pack, un-retiring it)
        Some(it) if live_n == it.ids.len() as i64 => {
            exec(sql, "DELETE FROM meta WHERE key='gc.retire'", vec![])?;
            Ok(None)
        }
        // L == 0: the dead-mark ran — re-run steps 8–11 for exactly the
        // recorded ids; lookups filter live, so no push can have linked in
        Some(it) if live_n == 0 => {
            retire_finish(d, sql, &it, &sm)?;
            Ok(Some(SliceOutcome::Done))
        }
        // mixed (or corrupt): never dead-mark the unvalidated remainder — 5a
        other => {
            wipe_path(d, sql, &sm, other.map(|i| i.fallback).unwrap_or(false))?;
            Ok(Some(SliceOutcome::Done))
        }
    }
}

/// §3.2 5a/6a: the keep-rule end — object rows of non-live marked packs go
/// first (the total==0 arm has no build pack to carry them), then wipe_all's
/// keep rule (cohort intact ⇒ N dead-marked; cohort NOT intact ⇒ N kept
/// live as the surviving copy), then steps 9–11. Never an Err path.
fn wipe_path(d: &RepoDo, sql: &SqlStorage, sm: &SwapMeta, fallback: bool) -> Result<(), Error> {
    let stats = swap_stats(sql, &[])?; // kept = the live cohort, before the wipe
    exec(
        sql,
        "DELETE FROM objects WHERE pack_id IN (SELECT pack_id FROM marked) \
         AND pack_id NOT IN (SELECT id FROM packs WHERE state='live')",
        vec![],
    )?;
    wipe_all(d, sql)?;
    end_of_chain(d, sql, &stats, false, fallback, sm)
}

/// The gc_last_swap counters — evaluated inside the span that retires (or
/// wipes): `kept` counts cohort packs still live, so on a retirement it is
/// read after the dead-mark and on a wipe before it.
struct SwapStats {
    retired: i64,
    kept: i64,
    retired_bytes: i64,
    kept_bytes: i64,
}

fn swap_stats(sql: &SqlStorage, ids: &[String]) -> Result<SwapStats, Error> {
    #[derive(serde::Deserialize)]
    struct Agg {
        n: i64,
        b: Option<i64>,
    }
    let kept = exec(
        sql,
        "SELECT COUNT(*) AS n, SUM(bytes) AS b FROM packs \
         WHERE id IN (SELECT pack_id FROM marked) AND state='live'",
        vec![],
    )?
    .one::<Agg>()?;
    let retired_bytes = if ids.is_empty() {
        0
    } else {
        exec(
            sql,
            "SELECT COALESCE(SUM(bytes),0) AS n FROM packs WHERE id IN (SELECT value FROM json_each(?))",
            vec![js(ids)?],
        )?
        .one::<N>()?
        .n
    };
    Ok(SwapStats {
        retired: i64::try_from(ids.len()).unwrap_or(i64::MAX),
        kept: kept.n,
        retired_bytes,
        kept_bytes: kept.b.unwrap_or(0),
    })
}

/// §3.2 steps 9–11: the durable gc_last_swap record and the gc_fallbacks
/// counter (both outside gc.* so they survive the wipe), then the same
/// wipe order as the PR5 sweep, then the compaction release.
fn end_of_chain(
    d: &RepoDo,
    sql: &SqlStorage,
    stats: &SwapStats,
    lossy: bool,
    fallback: bool,
    sm: &SwapMeta,
) -> Result<(), Error> {
    // step 9 — same span as the wipe, so a resumed run never double-counts
    put(
        d,
        "gc_last_swap",
        serde_json::to_string(&serde_json::json!({
            "retired": stats.retired,
            "kept": stats.kept,
            "retired_bytes": stats.retired_bytes,
            "kept_bytes": stats.kept_bytes,
            "lossy": lossy,
            "fallback": fallback,
            "tries": sm.tries,
            "slices": sm.slices,
        }))
        .map_err(|e| Error::Internal(e.to_string()))?,
    )?;
    let prev = d
        .meta_opt("gc_fallbacks")?
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(0);
    put(d, "gc_fallbacks", (if fallback { prev.saturating_add(1) } else { 0 }).to_string())?;
    // step 10 — the delete span really ran: repair's bounded resurrection
    // counters reset here, then gc.*, then scratch, same order as the sweep
    exec(sql, "DELETE FROM meta WHERE key LIKE 'resurrected.gc_%'", vec![])?;
    exec(sql, "DELETE FROM meta WHERE key LIKE 'gc.%'", vec![])?;
    for t in SCRATCH_TABLES {
        exec(sql, &format!("DELETE FROM {t}"), vec![])?;
    }
    // step 11 — held compaction runs next, ahead of any queued mark
    compact::trigger(d)?;
    compact::release_hold(d)?;
    Ok(())
}

/// §3.2 step 6: every bit `bitmap` marks was copied — `bitmap & !built == 0`
/// over byte slices; a byte missing from `built` counts as unbuilt (ragged
/// lengths), bits in `built` past `bitmap`'s end can't retire it.
fn bm_bitmap_subset(bitmap: &[u8], built: &[u8]) -> bool {
    bitmap
        .iter()
        .enumerate()
        .all(|(i, &b)| b & !built.get(i).copied().unwrap_or(0) == 0)
}

/// §3.2 step 6: lossy = the build missed entries — popcount(built) < count,
/// where only bits inside the entry range count: padding bits in the last
/// byte are not entries and must not mask a real loss.
fn bm_lossy(built: &[u8], count: i64) -> bool {
    let count = usize::try_from(count.max(0)).unwrap_or(usize::MAX);
    let mut n = 0u64;
    for (i, &b) in built.iter().enumerate() {
        let keep = count.saturating_sub(i.saturating_mul(8)).min(8);
        n += u64::from((b & ((1u16 << keep) - 1) as u8).count_ones());
    }
    n < count as u64
}

/// §3.3: every listed entry idx is set in `built` — the fallback's per-pack
/// retirable predicate.
fn bm_all_set(built: &[u8], idxs: &[i64]) -> bool {
    idxs.iter().all(|&i| {
        u32::try_from(i).ok().map_or(false, |i| {
            built
                .get(usize::try_from(i / 8).unwrap_or(usize::MAX))
                .map(|b| b & (1 << (i % 8)) != 0)
                .unwrap_or(false)
        })
    })
}

#[cfg(test)]
mod tests {
    use super::{bm_all_set, bm_bitmap_subset, bm_lossy, Pos};

    /// A gc.pos persisted before tail/(bci,bidx,bfrag) existed must still load —
    /// serde defaults degrade a missing tail to 0, which the resume path reads
    /// as "nothing undrained persisted" and falls back to boundary replay.
    #[test]
    fn pos_backward_compat() {
        let old = r#"{"ci":0,"idx":20407,"pack":"p","upload":"u","st":null,
                     "frag":0,"total":718383,"lo":0,"hi":0}"#;
        let p: Pos = serde_json::from_str(old).unwrap();
        assert_eq!(p.idx, 20407);
        assert_eq!(p.tail, 0);
        assert_eq!((p.bci, p.bidx, p.bfrag), (0, 0, 0));
        // a cursor with the fields round-trips
        let new = r#"{"ci":1,"idx":5,"pack":"p","upload":"u","st":null,"frag":9,
                     "total":10,"lo":0,"hi":0,"tail":6427865,"bci":0,"bidx":3,"bfrag":2}"#;
        let p: Pos = serde_json::from_str(new).unwrap();
        assert_eq!(p.tail, 6427865);
        assert_eq!((p.bci, p.bidx, p.bfrag), (0, 3, 2));
    }

    /// §3.2 step 6: bitmap ⊆ built — a mark copied nowhere keeps the pack out
    /// of the retire set, and a byte missing from `built` counts as unbuilt.
    #[test]
    fn subset_predicate() {
        assert!(bm_bitmap_subset(&[0b0000_0101], &[0b0000_0111]));
        assert!(!bm_bitmap_subset(&[0b0000_0110], &[0b0000_0101])); // bit 1 unbuilt
        assert!(bm_bitmap_subset(&[0x00], &[])); // no marks: vacuously a subset
        assert!(!bm_bitmap_subset(&[0x00, 0x80], &[0xFF])); // ragged: built lacks byte 1
        assert!(bm_bitmap_subset(&[0x00], &[0xFF, 0xFF])); // longer built is fine
    }

    /// §3.2 step 6: lossy = popcount(built) < count — padding bits past the
    /// entry count are not entries and must not mask a real loss.
    #[test]
    fn lossy_predicate() {
        assert!(bm_lossy(&[0b0000_0111], 5)); // 3 built of 5 entries
        assert!(!bm_lossy(&[0b0001_1111], 5)); // all 5 built
        assert!(!bm_lossy(&[0b1111_1111], 5)); // trailing bits can't inflate the count
        assert!(bm_lossy(&[], 1)); // nothing built of a non-empty pack
        assert!(!bm_lossy(&[0xFF, 0x01], 9)); // 9 built of 9
        assert!(bm_lossy(&[0xFF, 0x00], 9)); // 8 built of 9
        assert!(!bm_lossy(&[], 0)); // empty pack: nothing to lose
    }

    /// §3.3: the fallback's per-pack predicate — every unbuilt-by-choice idx
    /// must be set in `built`.
    #[test]
    fn fallback_all_set() {
        assert!(bm_all_set(&[0b0000_0101], &[0, 2]));
        assert!(!bm_all_set(&[0b0000_0101], &[0, 1]));
        assert!(bm_all_set(&[0b0000_0101], &[])); // no required idxs
        assert!(!bm_all_set(&[0xFF], &[8])); // idx past the bitmap
        assert!(!bm_all_set(&[], &[0]));
    }
}
