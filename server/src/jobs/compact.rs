//! G4a lossless tiered compaction (design/pr4-compaction.md; §15 > §14 > §13
//! > §12 > §2–§9). A third chain kind that shares PR1's single-chain
//! machinery: gc.* meta, `marked`, the MPU builder, the phase/legacy/stray
//! guards and the lease CAS. Compaction copies every cohort entry verbatim,
//! so there is no mark, no refs_version abort and no gc_epoch bump — a push or
//! clone mid-build is legal by construction.

use std::collections::HashSet;

use worker::{SqlStorage, SqlStorageValue as V};

use super::{enqueue, gc, Job, JobKind, SliceOutcome};
use crate::error::Error;
use crate::platform::{hex16, now_ms};
use crate::repo_do::RepoDo;
use crate::store::{exec, PackMeta};

const BASE_N: i64 = 8; // S2: the object-count ladder root
const LADDER: i64 = 16; // §14: the fixed tier bounds are 8·16^k
const TIERS: usize = 5;
const MAX_BYTES: i64 = 64 << 20; // a pack this size is left to G4
const COHORT_BYTES: i64 = 256 << 20;
const RETRY_MS: i64 = 30_000; // COMPACT_RETRY_MS
const QUAR_CAP: usize = 256; // S5: compact.quarantine is a bounded strike map

/// The compaction knobs — hook-gated `testhooks::knob` first, env default
/// otherwise (the same read path `gc_candidate_before_ms` uses).
pub struct Knobs {
    pub fanin: i64,    // T, clamped >= 2
    pub min_age_ms: i64,
    pub max_rows: i64, // the init bitmap scan / swap row-delete budget
}

pub fn knobs(d: &RepoDo) -> Knobs {
    let k = |name: &str, env: &str, def: i64| {
        crate::testhooks::knob(d, name).unwrap_or_else(|| d.env_i64(env, def))
    };
    Knobs {
        fanin: k("compact_fanin", "GE_COMPACT_FANIN", 16).max(2),
        min_age_ms: k("compact_min_age_ms", "GE_COMPACT_MIN_AGE_MS", 60_000),
        max_rows: k("compact_max_rows", "GE_COMPACT_MAX_ROWS", 100_000),
    }
}

/// One live pack row — the select_cohort input.
#[derive(serde::Deserialize, Clone)]
pub struct LivePack {
    pub id: String,
    pub count: i64,
    pub bytes: i64,
    pub created_at: i64,
}

/// Tiers by object count (§14 T1), the fixed ladder
/// [1,8) [8,128) [128,2048) [2048,32768) [32768, MAX_ROWS/2] — tier bounds do
/// not move with the fanin knob, only the >=T admission threshold does.
fn tier(count: i64) -> usize {
    let mut t = 0usize;
    let mut bound = BASE_N;
    while count >= bound && t < TIERS - 1 {
        t += 1;
        bound = bound.saturating_mul(LADDER);
    }
    t
}

fn eligible(p: &LivePack, quarantined: &HashSet<String>, cutoff: i64, knobs: &Knobs) -> bool {
    p.count >= 1
        && p.count <= knobs.max_rows / 2
        && p.bytes < MAX_BYTES
        && p.created_at <= cutoff
        && !quarantined.contains(&p.id)
}

/// S2/T1: the pure cohort selector, shared by the commit_push trigger, the
/// janitor trigger, the swap's drain re-trigger and init. The tier with the
/// most eligible packs wins, provided it holds at least T (ties go to the
/// lowest tier); the cohort is the oldest (created_at, id) taken greedily
/// while n <= 2T, Σcount <= MAX_ROWS and Σbytes <= 256 MiB. Fewer than 2 is
/// no cohort at all.
pub fn select_cohort(
    packs: &[LivePack],
    quarantined: &HashSet<String>,
    now: i64,
    knobs: &Knobs,
) -> Option<Vec<LivePack>> {
    let cutoff = now.saturating_sub(knobs.min_age_ms);
    let mut per_tier: [Vec<&LivePack>; TIERS] = Default::default();
    for p in packs {
        if eligible(p, quarantined, cutoff, knobs) {
            per_tier[tier(p.count)].push(p);
        }
    }
    let t = usize::try_from(knobs.fanin).unwrap_or(2).max(2);
    let mut best: Option<usize> = None;
    for (i, tier_packs) in per_tier.iter().enumerate() {
        if tier_packs.len() >= t
            && best.map(|b| tier_packs.len() > per_tier[b].len()).unwrap_or(true)
        {
            best = Some(i);
        }
    }
    let mut cohort = std::mem::take(&mut per_tier[best?]);
    cohort.sort_by(|a, b| (a.created_at, a.id.as_str()).cmp(&(b.created_at, b.id.as_str())));
    let (mut out, mut rows, mut bytes) = (Vec::new(), 0i64, 0i64);
    for p in cohort {
        if out.len() >= 2 * t
            || rows.saturating_add(p.count) > knobs.max_rows
            || bytes.saturating_add(p.bytes) > COHORT_BYTES
        {
            break;
        }
        rows = rows.saturating_add(p.count);
        bytes = bytes.saturating_add(p.bytes);
        out.push(p.clone());
    }
    (out.len() >= 2).then_some(out)
}

/// R5: when the age floor is the only thing keeping every tier below T — the
/// earliest moment a tier reaches T eligible packs. The swap's drain and the
/// push trigger use it to enqueue a deferred gc_compact rather than waiting
/// for the next writer.
pub fn next_wake(
    packs: &[LivePack],
    quarantined: &HashSet<String>,
    knobs: &Knobs,
) -> Option<i64> {
    let mut times: [Vec<i64>; TIERS] = Default::default();
    for p in packs {
        if p.count >= 1
            && p.count <= knobs.max_rows / 2
            && p.bytes < MAX_BYTES
            && !quarantined.contains(&p.id)
        {
            times[tier(p.count)].push(p.created_at.saturating_add(knobs.min_age_ms));
        }
    }
    let t = usize::try_from(knobs.fanin).unwrap_or(2).max(2);
    times
        .iter_mut()
        .filter_map(|v| {
            v.sort_unstable();
            v.get(t.saturating_sub(1)).copied()
        })
        .min()
}

fn live_packs(d: &RepoDo) -> Result<Vec<LivePack>, Error> {
    Ok(exec(
        &d.sql(),
        "SELECT id, count, bytes, created_at FROM packs WHERE state='live'",
        vec![],
    )?
    .to_array::<LivePack>()?)
}

// ---- compact.quarantine: JSON [[id, n], ...] in bump order — S5's per-pack
// strike map; n >= 2 excludes a pack from cohorts. ----

/// The pure decode: `None` on a corrupt stored value. A caller that writes
/// the map back must treat `None` as "stash the raw bytes aside, then
/// continue empty" — saving an empty fallback over a corrupt read would
/// silently wipe the map AND its evidence.
fn quarantine_parse(s: &str) -> Option<Vec<(String, i64)>> {
    serde_json::from_str(s).ok()
}

fn quarantine_encode(q: &[(String, i64)]) -> Result<String, Error> {
    serde_json::to_string(q).map_err(|e| Error::Internal(e.to_string()))
}

/// The stored strike map: `None` = a stored value failed to parse (corrupt);
/// an absent key reads as `Some(vec![])`.
fn quarantine_read(d: &RepoDo) -> Result<Option<Vec<(String, i64)>>, Error> {
    Ok(match d.meta_opt("compact.quarantine")? {
        None => Some(Vec::new()),
        Some(s) => match quarantine_parse(&s) {
            q @ Some(_) => q,
            None => {
                worker::console_log!("compact.quarantine unparseable");
                None
            }
        },
    })
}

/// The trigger's lenient read: it only enqueues maintenance and never writes
/// the map, so a corrupt strike map still reads as empty rather than failing
/// the enqueue path.
fn quarantine_load(d: &RepoDo) -> Result<Vec<(String, i64)>, Error> {
    Ok(quarantine_read(d)?.unwrap_or_default())
}

fn quarantine_save(d: &RepoDo, q: &[(String, i64)]) -> Result<(), Error> {
    exec(
        &d.sql(),
        "INSERT INTO meta(key,value) VALUES('compact.quarantine',?) \
         ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        vec![V::from(quarantine_encode(q)?)],
    )?;
    Ok(())
}

/// The corrupt-read path: `quarantine_read` reported `None`, so the stored
/// value exists but won't parse. Copy it raw to `compact.quarantine.corrupt`
/// — once only, the first corruption is the diagnostic a later strike save
/// must not overwrite — and hand back a fresh empty map the caller's normal
/// save then persists, so strikes keep accumulating instead of freezing
/// forever behind the unreadable value.
fn quarantine_corrupt(d: &RepoDo) -> Result<Vec<(String, i64)>, Error> {
    if let Some(raw) = d.meta_opt("compact.quarantine")? {
        exec(
            &d.sql(),
            "INSERT INTO meta(key,value) VALUES('compact.quarantine.corrupt',?) \
             ON CONFLICT(key) DO NOTHING",
            vec![V::from(raw)],
        )?;
    }
    Ok(Vec::new())
}

fn quarantine_ids(q: &[(String, i64)]) -> HashSet<String> {
    q.iter()
        .filter(|(_, n)| *n >= 2)
        .map(|(id, _)| id.clone())
        .collect()
}

fn quarantine_bump(q: &mut Vec<(String, i64)>, id: &str, n: i64) {
    match q.iter_mut().find(|(p, _)| p.as_str() == id) {
        Some(e) => e.1 = e.1.saturating_add(n),
        None => q.push((id.to_string(), n)),
    }
    // S5: the oldest entry drops at the cap — entries are in bump order
    if q.len() > QUAR_CAP {
        q.remove(0);
    }
}

/// The shared trigger (§3/R5): a cohort that exists now enqueues gc_compact
/// at the earliest moment it can run (now); when only min_age blocks the next
/// cohort it enqueues at that moment instead. `enqueue` dedups against a
/// queued row, so every call site is unconditional.
pub fn trigger(d: &RepoDo) -> Result<(), Error> {
    let packs = live_packs(d)?;
    let quarantined = quarantine_ids(&quarantine_load(d)?);
    let k = knobs(d);
    let run_at = if select_cohort(&packs, &quarantined, now_ms(), &k).is_some() {
        Some(now_ms())
    } else {
        next_wake(&packs, &quarantined, &k)
    };
    if let Some(t) = run_at {
        enqueue(&d.sql(), JobKind::GcCompact, t, "{}")?;
        // a never-dispatched row already queued for a later wake pulls forward
        // to t — but never a row sitting out an error backoff or a busy
        // reschedule (a reschedule requeues attempts=0/last_error NULL yet
        // leaves started_at standing): pulling those forward would retry a
        // failing gc_compact on every push and burn its attempt budget
        exec(
            &d.sql(),
            "UPDATE jobs SET run_at=MIN(run_at,?) WHERE kind='gc_compact' AND state='queued' \
             AND attempts=0 AND last_error IS NULL AND started_at IS NULL",
            vec![V::from(t)],
        )?;
    }
    Ok(())
}

// ---- S1: the owner-based orphan predicate ----

/// The job kind that owns the in-flight chain, from (gc.mode, gc.phase).
fn owner_kind(mode: Option<&str>, phase: Option<&str>) -> JobKind {
    match (mode, phase) {
        (_, Some("sweep")) => JobKind::GcSweep,
        (_, Some("build")) => JobKind::GcConsolidate,
        (Some("compact"), _) => JobKind::GcConsolidate,
        _ => JobKind::GcMark, // a GC chain with no phase yet is still marking
    }
}

/// S1: a chain is orphaned iff gc.started_at stands and no row of its owner
/// kind is queued/running carrying this chain's id — or no chain id at all
/// (resurrected rows run chain-less). Legacy chains are excluded (T3): the
/// legacy_chain guards own them.
pub fn orphaned(d: &RepoDo, sql: &SqlStorage) -> Result<bool, Error> {
    let Some(chain) = d.meta_opt("gc.chain")? else {
        return Ok(false); // no chain id: either no chain or a legacy one
    };
    if d.meta_opt("gc.started_at")?.is_none() || gc::legacy_chain(d)? {
        return Ok(false);
    }
    let owner = owner_kind(
        d.meta_opt("gc.mode")?.as_deref(),
        d.meta_opt("gc.phase")?.as_deref(),
    );
    #[derive(serde::Deserialize)]
    struct N {
        n: i64,
    }
    let n = exec(
        sql,
        "SELECT COUNT(*) AS n FROM jobs WHERE kind=? AND state IN ('queued','running') \
         AND COALESCE(json_extract(payload,'$.chain'), ?) = ?",
        vec![V::from(owner.as_str()), V::from(chain.as_str()), V::from(chain.as_str())],
    )?
    .one::<N>()?
    .n;
    Ok(n == 0)
}

/// S1/R6: the orphaned chain's lossless abort — dead-mark the half-built
/// pack, wipe the scratch and gc.*, preserve the dead gc_* rows' last_error
/// under `dead.<kind>` before deleting them (so boot repair cannot resurrect
/// them as chain-less duplicates), and bump quarantine strikes when the
/// orphan was a compact chain. Never enqueues a GcMark.
pub fn orphan_abort(d: &RepoDo, sql: &SqlStorage) -> Result<(), Error> {
    let compact = d.meta_opt("gc.mode")?.as_deref() == Some("compact");
    // captured before the wipe: the dead-row cleanup matches on it, and the
    // cohort's pack ids must be read before `marked` drops
    let chain = d.meta_opt("gc.chain")?.unwrap_or_default();
    let cohort = orphan_abort_cohort_ids(sql, compact)?;
    gc::wipe_all(d, sql)?; // dead-marks gc.new_pack, drops scratch + gc.*
    orphan_abort_preserve_dead_errors(sql, &chain)?;
    if !cohort.is_empty() {
        orphan_abort_quarantine_strikes(d, &cohort)?;
    }
    Ok(())
}

fn orphan_abort_cohort_ids(sql: &SqlStorage, compact: bool) -> Result<Vec<String>, Error> {
    if !compact {
        return Ok(Vec::new());
    }
    Ok(exec(sql, "SELECT pack_id FROM marked", vec![])?
        .to_array::<gc::P>()?
        .into_iter()
        .map(|r| r.pack_id)
        .collect())
}

fn orphan_abort_preserve_dead_errors(sql: &SqlStorage, chain: &str) -> Result<(), Error> {
    #[derive(serde::Deserialize)]
    struct DeadRow {
        kind: String,
        last_error: Option<String>,
    }
    let dead = exec(
        sql,
        "SELECT kind, last_error FROM jobs WHERE state='dead' \
         AND kind IN ('gc_mark','gc_consolidate','gc_sweep','gc_compact') \
         AND COALESCE(json_extract(payload,'$.chain'), ?) = ?",
        vec![V::from(chain), V::from(chain)],
    )?
    .to_array::<DeadRow>()?;
    for r in &dead {
        if let Some(e) = &r.last_error {
            exec(
                sql,
                "INSERT INTO meta(key,value) VALUES(?,?) \
                 ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                vec![V::from(format!("dead.{}", r.kind)), V::from(e.as_str())],
            )?;
        }
    }
    exec(
        sql,
        "DELETE FROM jobs WHERE state='dead' \
         AND kind IN ('gc_mark','gc_consolidate','gc_sweep','gc_compact') \
         AND COALESCE(json_extract(payload,'$.chain'), ?) = ?",
        vec![V::from(chain), V::from(chain)],
    )?;
    Ok(())
}

fn orphan_abort_quarantine_strikes(d: &RepoDo, cohort: &[String]) -> Result<(), Error> {
    // a corrupt stored map is stashed under compact.quarantine.corrupt
    // (once) and replaced — the strike lands on the fresh map either way
    let mut q = match quarantine_read(d)? {
        Some(q) => q,
        None => quarantine_corrupt(d)?,
    };
    for id in cohort {
        quarantine_bump(&mut q, id, 1);
    }
    quarantine_save(d, &q)
}

/// The cohort-live guard (R7/§5): the frozen `marked` set must still be live
/// for the build's copy to be lossless.
pub fn cohort_live(_d: &RepoDo, sql: &SqlStorage) -> Result<bool, Error> {
    #[derive(serde::Deserialize)]
    struct N {
        n: i64,
    }
    let m = exec(sql, "SELECT COUNT(*) AS n FROM marked", vec![])?.one::<N>()?.n;
    if m == 0 {
        return Ok(false);
    }
    let live = exec(
        sql,
        "SELECT COUNT(*) AS n FROM packs WHERE id IN (SELECT pack_id FROM marked) AND state='live'",
        vec![],
    )?
    .one::<N>()?
    .n;
    Ok(live == m)
}

/// GcCompact (§2): one sync span, no await. Busy check + orphan abort, cohort
/// selection, T5's row-count corruption gate, S6's per-pack dedup bitmaps —
/// then the chain is staked and the shared builder takes over.
pub fn gc_compact(d: &RepoDo, _job: &Job) -> Result<SliceOutcome, Error> {
    let sql = d.sql();
    if let Some(out) = gc_compact_entry_guard(d, &sql)? {
        return Ok(out);
    }
    let packs = live_packs(d)?;
    let quar = gc_compact_prune_quarantine(d, &packs)?;
    let k = knobs(d);
    let Some(cohort) = select_cohort(&packs, &quarantine_ids(&quar), now_ms(), &k) else {
        return Ok(SliceOutcome::Done);
    };
    let (good, _, done) = gc_compact_t5_filter(d, &sql, cohort, quar)?;
    if done {
        return Ok(SliceOutcome::Done); // strike bumps persisted; no chain
    }
    let bitmaps = gc_compact_dedup_bitmaps(&sql, good)?;
    gc_compact_stake_chain(d, &sql, bitmaps)?;
    // a crash here models "init span committed, row stranded running": the
    // chain is staked and its builder queued — the outcome just never landed
    crate::testhooks::failpoint(d, "gc.compact.init")?;
    Ok(SliceOutcome::Done)
}

fn gc_compact_entry_guard(d: &RepoDo, sql: &SqlStorage) -> Result<Option<SliceOutcome>, Error> {
    if d.meta_opt("gc.started_at")?.is_some() {
        if orphaned(d, sql)? {
            orphan_abort(d, sql)?; // lossless — then a fresh cohort below
            return Ok(None);
        }
        return Ok(Some(SliceOutcome::Reschedule { run_at: now_ms() + RETRY_MS }));
    }
    // §2: a chain-less consolidate/sweep row still stands between init and
    // a new chain — its phase guard must see a clean table, so retry later
    #[derive(serde::Deserialize)]
    struct N {
        #[allow(dead_code)]
        n: i64,
    }
    let busy = exec(
        sql,
        "SELECT 1 AS n FROM jobs WHERE kind IN ('gc_consolidate','gc_sweep') \
         AND state IN ('queued','running') LIMIT 1",
        vec![],
    )?
    .to_array::<N>()?;
    if !busy.is_empty() {
        return Ok(Some(SliceOutcome::Reschedule { run_at: now_ms() + RETRY_MS }));
    }
    Ok(None)
}

fn gc_compact_prune_quarantine(d: &RepoDo, packs: &[LivePack]) -> Result<Vec<(String, i64)>, Error> {
    // S5: non-live quarantine ids are pruned at every init. `None` = the
    // stored map failed to parse — it reads as empty for selection, its raw
    // bytes are stashed under compact.quarantine.corrupt, and the fresh map
    // carries on: every save below persists normally.
    let mut quar = match quarantine_read(d)? {
        Some(q) => q,
        None => quarantine_corrupt(d)?,
    };
    let live_ids: HashSet<String> = packs.iter().map(|p| p.id.clone()).collect();
    let pruned = quar.len();
    quar.retain(|(id, _)| live_ids.contains(id));
    if quar.len() != pruned {
        quarantine_save(d, &quar)?;
    }
    Ok(quar)
}

/// T5/U3: rows > count is corruption — the pack is quarantined at once and
/// excluded. rows < count (a pushed pack that carried a sha twice) is
/// healthy and stays.
fn gc_compact_t5_filter(
    d: &RepoDo,
    sql: &SqlStorage,
    cohort: Vec<LivePack>,
    mut quar: Vec<(String, i64)>,
) -> Result<(Vec<LivePack>, Vec<(String, i64)>, bool), Error> {
    #[derive(serde::Deserialize)]
    struct N {
        n: i64,
    }
    let mut good = Vec::new();
    let mut poisoned = false;
    for p in &cohort {
        let rows = exec(
            sql,
            "SELECT COUNT(*) AS n FROM (SELECT 1 FROM objects WHERE pack_id=? LIMIT ?)",
            vec![V::from(p.id.as_str()), V::from(p.count.saturating_add(1))],
        )?
        .one::<N>()?
        .n;
        if rows > p.count {
            quarantine_bump(&mut quar, &p.id, 2);
            poisoned = true;
        } else {
            good.push(p.clone());
        }
    }
    if poisoned {
        quarantine_save(d, &quar)?;
    }
    let short = good.len() < 2;
    Ok((good, quar, short))
}

fn gc_compact_dedup_bitmaps(sql: &SqlStorage, mut good: Vec<LivePack>) -> Result<Vec<(String, Vec<u8>)>, Error> {
    // S6: per-pack dedup reads in pack_id order (objects_pack index); the
    // first occurrence wins a bit — the build copies `marked` in the same
    // order, so the emitted pack holds each sha exactly once.
    good.sort_by(|a, b| a.id.cmp(&b.id));
    #[derive(serde::Deserialize)]
    struct Row {
        idx: i64,
        sha: String,
    }
    let mut seen: HashSet<String> = HashSet::new();
    let mut bitmaps: Vec<(String, Vec<u8>)> = Vec::new();
    for p in &good {
        let rows = exec(
            sql,
            "SELECT idx, sha FROM objects WHERE pack_id=? ORDER BY idx",
            vec![V::from(p.id.as_str())],
        )?
        .to_array::<Row>()?;
        let max_idx = rows.iter().map(|r| r.idx).max().unwrap_or(-1);
        let bits = usize::try_from(p.count.max(max_idx.saturating_add(1)))
            .map_err(|_| Error::Internal("count".into()))?
            .div_ceil(8);
        let mut bm = vec![0u8; bits];
        for r in rows {
            if seen.insert(r.sha) {
                let byte = usize::try_from(r.idx / 8).map_err(|_| Error::Internal("idx".into()))?;
                *bm.get_mut(byte)
                    .ok_or_else(|| Error::Internal("idx past pack count".into()))? |=
                    1u8 << (r.idx % 8);
            }
        }
        bitmaps.push((p.id.clone(), bm));
    }
    Ok(bitmaps)
}

fn gc_compact_stake_chain(d: &RepoDo, sql: &SqlStorage, bitmaps: Vec<(String, Vec<u8>)>) -> Result<(), Error> {
    for t in gc::SCRATCH_TABLES {
        exec(sql, &format!("DELETE FROM {t}"), vec![])?;
    }
    exec(sql, "DELETE FROM meta WHERE key LIKE 'gc.%'", vec![])?;
    gc::put(d, "gc.mode", "compact")?;
    gc::put(d, "gc.chain", hex16()?)?;
    gc::put(d, "gc.started_at", now_ms().to_string())?;
    gc::put(d, "gc.phase", "build")?;
    for (id, bm) in bitmaps {
        exec(
            sql,
            "INSERT INTO marked(pack_id,bitmap) VALUES(?,?)",
            vec![V::from(id.as_str()), V::from(bm)],
        )?;
    }
    enqueue(sql, JobKind::GcConsolidate, now_ms(), &gc::chain_payload(d)?)?;
    Ok(())
}

/// §5's swap span — compact finish_build: fence, cohort-live guard, N live
/// with created_at = MAX(cohort), cohort dead + its rows deleted, scratch and
/// gc.* wiped. No gc_epoch bump; only `resurrected.gc_compact` resets.
pub(crate) fn swap(
    d: &RepoDo,
    sql: &SqlStorage,
    pack: &str,
    meta: &PackMeta,
    job: &Job,
    chain: Option<&str>,
) -> Result<gc::Flow, Error> {
    if !gc::chain_fence(d, sql, job, chain, pack)? {
        return Ok(gc::Flow::Done);
    }
    if !cohort_live(d, sql)? {
        gc::wipe_all(d, sql)?; // lossless abort — never a mark, never resurrect N
        return Ok(gc::Flow::Done);
    }
    swap_promote_output_pack(d, sql, pack, meta)?;
    swap_retire_cohort_packs(d, sql)?;
    swap_wipe_compact_scratch(sql)?;
    // S7: the compact swap owns only this counter — GC's sweep keeps the rest
    exec(sql, "DELETE FROM meta WHERE key='resurrected.gc_compact'", vec![])?;
    trigger(d)?; // R5: the drain re-trigger runs inside the swap span
    Ok(gc::Flow::Done)
}

fn swap_promote_output_pack(d: &RepoDo, sql: &SqlStorage, pack: &str, meta: &PackMeta) -> Result<(), Error> {
    exec(
        sql,
        "UPDATE packs SET state='live', count=?, bytes=?, commit_lo=?, commit_hi=?, \
         created_at=(SELECT MAX(p2.created_at) FROM packs p2 \
             WHERE p2.id IN (SELECT pack_id FROM marked)) \
         WHERE id=? AND state IN ('ingesting','live')",
        vec![
            i64::from(meta.count).into(),
            i64::try_from(meta.bytes).map_err(|_| Error::Internal("bytes".into()))?.into(),
            i64::try_from(meta.commit_lo).unwrap_or((1 << 53) - 1).into(),
            i64::try_from(meta.commit_hi).unwrap_or(0).into(),
            V::from(pack),
        ],
    )?;
    if d.changes()? != 1 {
        return Err(Error::Internal("build pack row".into()));
    }
    Ok(())
}

fn swap_retire_cohort_packs(d: &RepoDo, sql: &SqlStorage) -> Result<(), Error> {
    exec(
        sql,
        "UPDATE packs SET state='dead', dead_at=? WHERE id IN (SELECT pack_id FROM marked)",
        vec![now_ms().into()],
    )?;
    // error-mode point: the cohort is dead-marked but its rows still stand —
    // the keep rule's partial-retirement window, same as the sweep's
    crate::testhooks::failpoint(d, "gc.compact.after_deadmark")?;
    exec(sql, "DELETE FROM objects WHERE pack_id IN (SELECT pack_id FROM marked)", vec![])?;
    Ok(())
}

fn swap_wipe_compact_scratch(sql: &SqlStorage) -> Result<(), Error> {
    // same order as the sweep and the kill helper: gc.* before the scratch
    exec(sql, "DELETE FROM meta WHERE key LIKE 'gc.%'", vec![])?;
    for t in gc::SCRATCH_TABLES {
        exec(sql, &format!("DELETE FROM {t}"), vec![])?;
    }
    Ok(())
}

/// §3.2 step 11 (d1 D): release the compaction hold once a barrier chain's
/// swap lands. Any gc_compact row sitting out a busy reschedule
/// (attempts=0 AND last_error IS NULL — error backoffs keep their own
/// run_at) is pulled forward to now; then, when a compact row is due,
/// EVERY queued gc_mark is pushed behind now + max(gc_quiet_ms,1) so the
/// compaction runs before the next chain can grab the hold again. Called
/// from the sweep's end_of_chain on barrier chains only.
pub fn release_hold(d: &RepoDo) -> Result<(), Error> {
    #[derive(serde::Deserialize)]
    struct N {
        n: i64,
    }
    let sql = d.sql();
    let now = now_ms();
    exec(
        &sql,
        "UPDATE jobs SET run_at=? WHERE kind='gc_compact' AND state='queued' AND attempts=0 AND last_error IS NULL",
        vec![V::from(now)],
    )?;
    let due = exec(
        &sql,
        "SELECT COUNT(*) AS n FROM jobs WHERE kind='gc_compact' AND state='queued' AND run_at<=?",
        vec![V::from(now)],
    )?
    .one::<N>()?
    .n
        > 0;
    if due {
        let hold = now.saturating_add(d.gc_quiet_ms().max(1));
        exec(
            &sql,
            "UPDATE jobs SET run_at=MAX(run_at,?) WHERE kind='gc_mark' AND state='queued'",
            vec![V::from(hold)],
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn pack(id: &str, count: i64, bytes: i64, created_at: i64) -> LivePack {
        LivePack { id: id.into(), count, bytes, created_at }
    }
    fn knobs() -> Knobs {
        Knobs { fanin: 16, min_age_ms: 60_000, max_rows: 100_000 }
    }
    fn none() -> HashSet<String> {
        HashSet::new()
    }

    /// T1: the fixed ladder — tier bounds don't move with the fanin knob.
    #[test]
    fn tier_bounds() {
        assert_eq!(tier(1), 0);
        assert_eq!(tier(7), 0);
        assert_eq!(tier(8), 1);
        assert_eq!(tier(127), 1);
        assert_eq!(tier(128), 2);
        assert_eq!(tier(2048), 3);
        assert_eq!(tier(32767), 3);
        assert_eq!(tier(32768), 4);
        assert_eq!(tier(50_000), 4);
    }

    /// Eligibility: count window, byte cap, age floor, quarantine.
    #[test]
    fn eligibility_gates() {
        let k = knobs();
        let now = 1_000_000i64;
        let packs = vec![
            pack("a", 0, 100, 0),              // count=0: never eligible (S11)
            pack("b", 50_001, 100, 0),         // over MAX_ROWS/2
            pack("c", 4, 64 << 20, 0),         // at the byte cap
            pack("d", 4, 100, now - 59_999),   // inside the age floor
            pack("e", 4, 100, 0),              // eligible but quarantined
        ];
        let mut q = HashSet::new();
        q.insert("e".to_string());
        assert!(select_cohort(&packs, &q, now, &k).is_none());
        let mut packs = packs;
        packs.extend((0..16).map(|i| pack(&format!("p{i}"), 4, 100, 0)));
        let c = select_cohort(&packs, &q, now, &k).unwrap();
        assert_eq!(c.len(), 16);
    }

    /// The fullest tier wins (>= T); a tie goes to the lowest tier.
    #[test]
    fn tier_choice() {
        let k = Knobs { fanin: 4, ..knobs() };
        let now = 1_000_000i64;
        let mut packs: Vec<LivePack> = (0..5).map(|i| pack(&format!("s{i}"), 4, 100, 0)).collect();
        packs.extend((0..5).map(|i| pack(&format!("b{i}"), 100, 100, 0)));
        let c = select_cohort(&packs, &none(), now, &k).unwrap();
        assert_eq!(c[0].id, "s0"); // 5 == 5: the lowest tier wins the tie
        packs.extend((0..2).map(|i| pack(&format!("x{i}"), 100, 100, 0)));
        let c = select_cohort(&packs, &none(), now, &k).unwrap();
        assert_eq!(c[0].id, "b0"); // 7 > 5: the fuller tier wins
    }

    /// The cohort is oldest-first and capped at 2T, MAX_ROWS and 256 MiB.
    #[test]
    fn cohort_budgets() {
        let k = Knobs { fanin: 2, ..knobs() };
        let now = 1_000_000i64;
        let packs: Vec<LivePack> = (0..10)
            .map(|i| pack(&format!("p{i}"), 4, 100, i * 1000))
            .collect();
        let c = select_cohort(&packs, &none(), now, &k).unwrap();
        assert_eq!(c.len(), 4); // 2T
        assert_eq!(c[0].id, "p0");
        // the row budget cuts a top-tier cohort short but keeps it feasible
        let big: Vec<LivePack> = (0..4)
            .map(|i| pack(&format!("g{i}"), 40_000, 100, i))
            .collect();
        let c = select_cohort(&big, &none(), now, &k).unwrap();
        assert_eq!(c.len(), 2); // 3×40k > MAX_ROWS stops at two
        // a singleton tier never yields a cohort
        assert!(select_cohort(&big[..1], &none(), now, &k).is_none());
    }

    /// R5's deferred enqueue: the T-th eligibility instant of a tier.
    #[test]
    fn wake_is_t_th_eligible() {
        let k = Knobs { fanin: 3, ..knobs() };
        let packs: Vec<LivePack> = (0..3).map(|i| pack(&format!("p{i}"), 4, 100, i)).collect();
        assert_eq!(next_wake(&packs, &none(), &k), Some(2 + 60_000));
        assert_eq!(next_wake(&packs[..2], &none(), &k), None);
    }

    /// S5's corrupt-map contract: parse reports `None` — writers stash the
    /// raw value under compact.quarantine.corrupt and continue on a fresh
    /// map, since a lenient empty fallback would let the next save silently
    /// wipe every strike. Encode/parse round-trips a strike map.
    #[test]
    fn quarantine_corrupt_parse_is_not_empty() {
        assert_eq!(quarantine_parse("not json"), None);
        assert_eq!(quarantine_parse("{\"p1\":2}"), None); // wrong shape
        assert_eq!(quarantine_parse("[]"), Some(Vec::new()));
        let q = vec![("p1".to_string(), 2i64), ("p2".to_string(), 1)];
        assert_eq!(quarantine_parse(&quarantine_encode(&q).unwrap()), Some(q));
    }
}
