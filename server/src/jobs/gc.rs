//! GC chain (CONTRACTS.md 5): GcMark -> GcConsolidate -> GcSweep, ported from
//! proofs-v2/gc-and-repack-alarm. All durable state lives in tables (marked, gc_frontier,
//! gc_seen, gc_parts, gc.* meta), so a requeued 'running' row resumes mid-flight and a kill
//! mid-slice replays idempotently.

use std::collections::{HashMap, HashSet, VecDeque};

use gix_hash::ObjectId;
use gix_object::Kind;
use worker::{SqlStorage, SqlStorageValue as V};

use super::{enqueue, Job, JobKind, SliceBudget, SliceOutcome};
use crate::error::Error;
use crate::pack::ingest::extract_links;
use crate::platform::now_ms;
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

#[derive(serde::Deserialize)]
struct N {
    #[allow(dead_code)]
    n: i64,
}
#[derive(serde::Deserialize)]
struct S {
    sha: String,
}
#[derive(serde::Deserialize)]
struct P {
    pack_id: String,
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
fn put(d: &RepoDo, k: &str, v: impl Into<String>) -> Result<(), Error> {
    exec(
        &d.sql(),
        "INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        vec![k.into(), v.into().into()],
    )?;
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
        for t in ["marked", "gc_frontier", "gc_seen", "gc_parts", "gc_tail"] {
            exec(&sql, &format!("DELETE FROM {t}"), vec![])?;
        }
        exec(&sql, "DELETE FROM meta WHERE key LIKE 'gc.%'", vec![])?;
        put(d, "gc.refs_version", d.meta("refs_version")?)?;
        put(d, "gc.started_at", now.to_string())?;
        exec(
            &sql,
            "INSERT INTO marked(pack_id,bitmap) SELECT id, zeroblob((count+7)/8) FROM packs \
             WHERE state='live' AND created_at < ?",
            vec![now.saturating_sub(d.gc_grace_ms()).into()],
        )?;
        exec(
            &sql,
            "INSERT OR IGNORE INTO gc_frontier(sha) SELECT target FROM refs UNION \
             SELECT peeled FROM refs WHERE peeled IS NOT NULL",
            vec![],
        )?;
        crate::testhooks::failpoint(d, "gc.mark.init")?;
    }
    let bucket = d.bucket()?;
    let cand: HashSet<String> = exec(&sql, "SELECT pack_id FROM marked", vec![])?
        .to_array::<P>()?
        .into_iter()
        .map(|r| r.pack_id)
        .collect();
    let mut bits: HashMap<String, Vec<u8>> = HashMap::new(); // touched bitmaps, flushed by commit_ids
    loop {
        if budget.spent_80pct() {
            return Ok(SliceOutcome::Continue { cursor: "{}".into() });
        }
        if d.meta_i64("refs_version")? != d.meta_i64("gc.refs_version")? {
            return abort(d, &sql);
        }
        // span A: pop a batch, plan marks and loads; nothing commits until a chunk's span B
        let batch: Vec<String> = exec(&sql, "SELECT sha FROM gc_frontier LIMIT ?", vec![ROUND.into()])?
            .to_array::<S>()?
            .into_iter()
            .map(|r| r.sha)
            .collect();
        if batch.is_empty() {
            exec(&sql, "DELETE FROM gc_seen", vec![])?;
            enqueue(&sql, JobKind::GcConsolidate, now_ms(), "{}")?;
            return Ok(SliceOutcome::Done);
        }
        let rows = exec(
            &sql,
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
                    let b = exec(&sql, "SELECT bitmap FROM marked WHERE pack_id=?", vec![r.pack_id.clone().into()])?
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
        let load_ids: HashSet<String> = loads.iter().map(|(id, _)| id.to_string()).collect();
        for chunk in chunks(&loads) {
            let ids: Vec<String> = chunk.iter().map(|(id, _)| id.to_string()).collect();
            // await the whole chunk FIRST — after_read then sits between sync
            // spans, where a crash can't bisect the kids-flush writes below
            let entries = bucket.read_entries_chunked(&chunk, &mut budget.req).await?;
            crate::testhooks::failpoint(d, "gc.mark.after_read")?;
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
                    commit_ids(&sql, &[], &kids, &mut bits)?;
                    kids.clear();
                }
            }
            commit_ids(&sql, &ids, &kids, &mut bits)?; // span B: expansion commits
            // CAS heartbeat: a progressing slice is not a straggler — and a stale
            // one (requeued + reclaimed) stops before its next write (A19)
            if !super::heartbeat(&sql, job)? {
                return Err(super::stale_lease());
            }
            // models a real death only while another await follows in the
            // same slice — at the slice's true end nothing is left to roll back
            crate::testhooks::failpoint(d, "gc.mark.after_commit")?;
            if budget.spent_80pct() {
                break;
            }
        }
        let rest: Vec<String> = batch.iter().filter(|s| !load_ids.contains(*s)).cloned().collect();
        if !rest.is_empty() {
            commit_ids(&sql, &rest, &[], &mut bits)?; // blobs/misses: nothing to read
            if !super::heartbeat(&sql, job)? {
                return Err(super::stale_lease());
            }
            crate::testhooks::failpoint(d, "gc.mark.after_commit")?;
        }
    }
}

/// One sync span: ids are now expanded — seen-recorded, unseen kids admitted, frontier rows
/// deleted, bits flushed.
fn commit_ids(
    sql: &SqlStorage,
    ids: &[String],
    kids: &[String],
    bits: &mut HashMap<String, Vec<u8>>,
) -> Result<(), Error> {
    exec(sql, "INSERT OR IGNORE INTO gc_seen(sha) SELECT value FROM json_each(?)", vec![js(ids)?])?;
    exec(
        sql,
        "INSERT OR IGNORE INTO gc_frontier(sha) SELECT j.value FROM json_each(?) j \
         WHERE NOT EXISTS(SELECT 1 FROM gc_seen s WHERE s.sha=j.value)",
        vec![js(kids)?],
    )?;
    exec(sql, "DELETE FROM gc_frontier WHERE sha IN (SELECT value FROM json_each(?))", vec![js(ids)?])?;
    for (p, mut bm) in bits.drain() {
        // a duplicate concurrent mark slice may have flushed different bits since our
        // last SELECT — OR-merge with the stored bitmap inside this sync span so a
        // last-writer-wins overwrite can never drop a reachable object's mark
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
    Ok(())
}

/// The 5.3 abort, shared by every arm: kill the build pack if one exists, drop all gc state,
/// enqueue a fresh mark after the quiet window.
fn abort(d: &RepoDo, sql: &SqlStorage) -> Result<SliceOutcome, Error> {
    if let Some(p) = d.meta_opt("gc.new_pack")? {
        exec(sql, "DELETE FROM objects WHERE pack_id=?", vec![p.clone().into()])?;
        exec(
            sql,
            "UPDATE packs SET state='dead', dead_at=? WHERE id=? AND state IN ('ingesting','live')",
            vec![now_ms().into(), p.into()],
        )?;
    }
    for t in ["marked", "gc_frontier", "gc_seen", "gc_parts", "gc_tail"] {
        exec(sql, &format!("DELETE FROM {t}"), vec![])?;
    }
    exec(sql, "DELETE FROM meta WHERE key LIKE 'gc.%'", vec![])?;
    enqueue(sql, JobKind::GcMark, now_ms() + d.gc_quiet_ms(), "{}")?;
    Ok(SliceOutcome::Done)
}

enum Flow {
    Yield,
    Done,
    Retry(Error),
    Rebuild,
}

/// GcConsolidate (5.2): one new normalized pack holding exactly the marked entries, verbatim.
pub async fn gc_consolidate(d: &RepoDo, job: &Job, budget: &mut SliceBudget) -> Result<SliceOutcome, Error> {
    let sql = d.sql();
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
        match build(d, &sql, &bucket, &key, out, &mut pos, job, budget).await? {
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
                true,
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
                        Ok(meta) => finish_build(d, sql, pos, &meta),
                        Err(e) => mpu_err(d, sql, bucket, key, pos, e, budget).await,
                    };
                }
                Some(cid) => {
                    if bm.0 != pos.ci {
                        bm.1 = exec(
                            sql,
                            "SELECT bitmap FROM marked WHERE pack_id=?",
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
                    budget, true,
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
                                job, budget, true,
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
                            job, budget, false,
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
                false,
            )
            .await?
            {
                return Ok(f);
            }
        }
        if staged.len() >= ROWS {
            Index(sql).insert_objects(&PackId(pos.pack.clone()), &staged)?;
            staged.clear();
        }
        // a multi-MiB chunk loop can run past the 60 s straggler window on slow R2 —
        // CAS-heartbeat so repair doesn't requeue a live consolidate into a duplicate
        // slice, and a duplicate that lost the row stops before its next write (A19)
        if !super::heartbeat(sql, job)? {
            return Err(super::stale_lease());
        }
        if let Some(f) = flush(
            d, sql, bucket, key, &mut out, pos, &mut staged, &mut spans, &cur, job, budget,
            false,
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
            Err(e) => return mpu_err(d, sql, bucket, key, pos, e, budget).await.map(Some),
        }
        crate::testhooks::failpoint(d, "gc.consolidate.after_part")?;
    }
    // a yield persists even when nothing drained — the tail is the progress
    let mut st = match st {
        Some(s) => s,
        None if persist_tail => out.snapshot(),
        None => return Ok(None),
    };
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
    // checkpoint span: refresh the repair lease — CAS on it, so a stale slice
    // that lost the row stops instead of heartbeating the new owner's started_at
    if !super::heartbeat(sql, job)? {
        return Err(super::stale_lease());
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
) -> Result<Flow, Error> {
    budget.req.charge(1)?;
    if let Some(h) = bucket.inner.head(key).await? {
        // complete() had already won
        let meta = PackMeta {
            pack: PackId(pos.pack.clone()),
            push_id: None,
            count: pos.total,
            bytes: h.size(),
            commit_lo: u64::try_from(pos.lo).unwrap_or(u64::MAX),
            commit_hi: u64::try_from(pos.hi).unwrap_or(0),
            created_at: now_ms(),
        };
        return finish_build(d, sql, pos, &meta);
    }
    let f = d.meta_i64("gc.fails").unwrap_or(0).saturating_add(1);
    put(d, "gc.fails", f.to_string())?;
    Ok(if f >= 2 { Flow::Rebuild } else { Flow::Retry(e) })
}

/// First build slice (5.2): repack only if >= 2 candidates or any candidate has unmarked
/// entries — otherwise wipe gc state and Done, because sweeping without a new pack would
/// delete reachable objects.
fn begin_build(d: &RepoDo, sql: &SqlStorage) -> Result<Option<Pos>, Error> {
    if d.meta_i64("refs_version")? != d.meta_i64("gc.refs_version")? {
        abort(d, sql)?;
        return Ok(None);
    }
    let cands = exec(
        sql,
        "SELECT m.pack_id, m.bitmap, p.count FROM marked m JOIN packs p ON p.id=m.pack_id \
         ORDER BY m.pack_id",
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
        // nothing reachable in any candidate: no repack to write — sweep deletes them outright
        exec(sql, "DELETE FROM gc_parts", vec![])?;
        exec(sql, "DELETE FROM gc_tail", vec![])?;
        exec(sql, "DELETE FROM meta WHERE key IN ('gc.pos','gc.new_pack','gc.fails')", vec![])?;
        enqueue(sql, JobKind::GcSweep, now_ms(), "{}")?;
        return Ok(None);
    }
    if !(cands.len() >= 2 || unmarked) {
        if let Some(p) = d.meta_opt("gc.new_pack")? {
            // half-written leftover, no pos
            exec(sql, "DELETE FROM objects WHERE pack_id=?", vec![p.clone().into()])?;
            exec(
                sql,
                "UPDATE packs SET state='dead', dead_at=? WHERE id=? AND state='ingesting'",
                vec![now_ms().into(), p.into()],
            )?;
        }
        for t in ["marked", "gc_frontier", "gc_seen", "gc_parts", "gc_tail"] {
            exec(sql, &format!("DELETE FROM {t}"), vec![])?;
        }
        exec(sql, "DELETE FROM meta WHERE key LIKE 'gc.%'", vec![])?;
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

fn finish_build(d: &RepoDo, sql: &SqlStorage, pos: &Pos, meta: &PackMeta) -> Result<Flow, Error> {
    // covers both callers — the normal finish path and mpu_err's HEAD-recovery
    crate::testhooks::failpoint(d, "gc.consolidate.before_finish")?;
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
    enqueue(sql, JobKind::GcSweep, now_ms(), "{}")?; // the only deleter (5.3)
    Ok(Flow::Done) // gc.* cleared by the sweep span
}

/// GcSweep (5.3): the whole step is one sync span — the refs_version check, the row deletes and
/// the gc_epoch bump are atomic — and it never touches R2: dead keys are the Janitor's, after GRACE.
pub async fn gc_sweep(d: &RepoDo) -> Result<SliceOutcome, Error> {
    // claim-only point: the slice never awaits, so a real death rolls the
    // whole span back — "crashed here" == "died before the slice ran"
    crate::testhooks::failpoint(d, "gc.sweep.before")?;
    let sql = d.sql();
    if d.meta_opt("gc.refs_version")?.is_none() {
        return Ok(SliceOutcome::Done);
    }
    if d.meta_i64("refs_version")? != d.meta_i64("gc.refs_version")? {
        return abort(d, &sql);
    }
    exec(
        &sql,
        "UPDATE packs SET state='dead', dead_at=? WHERE id IN (SELECT pack_id FROM marked)",
        vec![now_ms().into()],
    )?;
    exec(&sql, "DELETE FROM objects WHERE pack_id IN (SELECT pack_id FROM marked)", vec![])?;
    exec(&sql, "UPDATE meta SET value=CAST(CAST(value AS INTEGER)+1 AS TEXT) WHERE key='gc_epoch'", vec![])?;
    for t in ["marked", "gc_frontier", "gc_seen", "gc_parts", "gc_tail"] {
        exec(&sql, &format!("DELETE FROM {t}"), vec![])?;
    }
    exec(&sql, "DELETE FROM meta WHERE key LIKE 'gc.%'", vec![])?;
    Ok(SliceOutcome::Done)
}

#[cfg(test)]
mod tests {
    use super::Pos;

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
}
