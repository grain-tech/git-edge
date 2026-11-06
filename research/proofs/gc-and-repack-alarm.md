# GC and repack as a DO alarm

> Second pass · Idea #55 · verdict: **lands with caveats** · feasibility 4/5 · reliability 3/5 · correctness 3/5 (first pass 3/2/2)
> First pass: [proof](./archived/gc-and-repack-alarm.md) · [review](../reviews/archived/gc-and-repack-alarm.md) · Second pass: [review](../reviews/gc-and-repack-alarm.md) · Contract: [CONTRACTS.md](../../CONTRACTS.md)

## Mechanism
Sections 4 and 5 of CONTRACTS.md already decide this idea: the one DO alarm belongs to `jobs::rearm` alone (4.1, second `setAlarm` cancels the first — measured, platform-facts #5), GC is the pre-registered chain `GcMark -> GcConsolidate -> GcSweep` (4.5), enqueued by `commit_push` step 7 ten quiet minutes after a ref move and dedup'd by kind, and the sweep's atomicity rule — check `refs_version`, delete `objects` rows, kill candidate packs, bump `gc_epoch`, all in one sync span, with R2 deletes deferred to the Janitor past `GRACE` — is the contract's own answer to the first-pass race. What was never written down, and is written here: the three `run_slice` arms. The storage model the first pass assumed is gone — there are no `objects/<sha>` keys, no `links` column, no `pack_builds` table, no CompressionStream (2.1, 2.2). `GcMark` walks reachability over the live `objects`/`packs` join (2.3) and re-derives links from entry bytes with `pack::ingest::extract_links` (1.4); a sha marks at most one entry across candidate packs, so the built pack's header count is exact. `GcConsolidate` copies stored entries verbatim through a resumable `PackWriter` (5.2) — no inflate, no deflate — into a normal `packs/<id>.pack` whose `objects` rows post per slice under `state='ingesting'` (invisible to the 2.3 reader query) and whose `push_id` is NULL (the server-side-rebase precedent). Two deliberate strengthenings over the letter of 5.1/5.2: the whole frontier lives in `gc_frontier` (not just the >50,000 spill — a mid-slice kill replays, it can never lose unexpanded ids), and the build cursor is `gc.pos` + `gc_parts` committed in the same span as the part it describes, not the job row the dispatcher rewrites only at slice end. Two honest weakenings: the output is a full-object normalized pack — no delta search exists anywhere in the contract (12), so repack merges packs and prunes unreachable entries but does not shrink live bytes; and any ref move between mark start and sweep aborts the whole run (5.3), so this is a quiet-repo compactor, not a hot-repo one.

## Primitives
- `SqlStorage::exec` + `SqlCursor::to_array` synchronous; sync-span atomicity around an await-free stretch: **measured** (platform-facts #1, #4). Await interleaving between spans is real (#4) and is what lets pushes run between rounds.
- `zeroblob`, `json_each(?)` as the `IN`-list mechanism: SQLite builtins; the A6 rule (<= 100 bound params per statement, `IN` lists batched <= 90 or `json_each`). `Vec<u8> -> SqlStorageValue` blob binding: **unverified**, same caveat as precomputed-clone-pack.
- `jobs::{enqueue, SliceOutcome::{Done, Continue, Reschedule}, SliceBudget::spent_80pct}`: contract 4.2-4.4; `SliceBudget.req: ReqBudget` mirror and `SliceBudget::fresh()` are declared sibling write-backs (server-side-rebase, bundle-uri). Dead maintenance jobs re-enqueue at boot (A4). `enqueue` dedups by kind — safe here because each kind drains shared tables, not a per-run payload.
- `Bucket::read_entries` coalesced range reads (7.2) and `read_range`: contract; real-R2 behavior measured on the **local simulator only** (#6). Frontier/build loads are chunked so one call charges <= 64 coalesced spans and <= 32 MiB (A4's own bound).
- `PackWriter::{create, flush_if_full, finish, abort}`: contract 1.2 + A1. `PackWriter::resume(bucket, key, upload_id, etags, state, budget)` over `Bucket::resume_multipart_upload`: **write-back already declared by server-side-rebase** (5.2 presumes it); this proof adds `PackWriter::append_stored(&entry) -> Result<(u64,u32)>` (the verbatim copy section 9 step 6 describes) and `PackWriter::checkpoint(&mut self, budget) -> Result<Option<(UploadedPart, PackState)>>` (force-upload the non-empty buffer as the next part; callers guarantee the >= 5 MiB rule for non-final parts). `PackState = { pos: u64, sha: CkptSha1, count: u32 }` — serde data, so the trailer hasher survives a slice; `gix_hash::Hasher` state cannot be exported, hence `CkptSha1` below.
- `Bucket::{create_multipart_upload, resume_multipart_upload, head}`, `MultipartUpload::{upload_part, complete, abort, upload_id}`, `UploadedPart{part_number, etag}`: API surface per memo section 1; `resume_multipart_upload` is a synchronous handle constructor; exact getter/field names **unverified**. Part re-upload with identical bytes before `complete` is S3 MPU semantics — **unverified on real R2** (#6); relied on only because replayed part content is byte-identical.
- `codec::decode_entry` (inflate an entry, <= 16 MiB cap per A7): spike-verified on workerd (contract correction 2). `extract_links(kind, data)` (1.4): contract signature; the mode-160000 gitlink skip is a flagged write-back (presigned-direct-upload).
- `Index::insert_objects` (1.2): <= 10,000 rows per call, `ON CONFLICT DO NOTHING` per row (two-phase-push) — which is exactly what makes replayed inserts safe.
- `js_sys::Date::now()`: standard. `PackId::random` over `web_sys::Crypto`: binding path unverified (8.2). DO-side subrequest limit is **not enforced locally** (#7); `ReqBudget::charge` is the only guard.

## Proof code
```rust
// src/jobs/gc.rs — the run_slice arms for JobKind::{GcMark, GcConsolidate, GcSweep} (CONTRACTS.md 1.4, 4.5, 5).
// worker 0.8.5 · gix-object 0.64.1 · gix-hash 0.26.2. exec / changed / meta / meta_opt / meta_i64 / now_ms / oid /
// sql / bucket are the RepoDo helpers of the sibling proofs; ObjRow::new(sha, offset, len, kind, size) as in
// server-side-rebase; impl From<worker::Error> for Error.
//
// REGISTRY (amendment A9) — this module IS the section-5 chain, so most names are the contract's already:
//   routes     none · JobKinds  none new (1.4) · R2 prefixes  none new (build artifact is a normal packs/ key, 2.2)
//   tables     marked(pack_id TEXT PRIMARY KEY, bitmap BLOB NOT NULL) WITHOUT ROWID          -- named in 5.1
//              gc_frontier(sha TEXT PRIMARY KEY) WITHOUT ROWID  -- named in 5.1; whole frontier, not only the spill
//              gc_seen(sha TEXT PRIMARY KEY) WITHOUT ROWID      -- expanded-set; invariant frontier ∩ seen = ∅
//              gc_parts(part_no INTEGER PRIMARY KEY, etag TEXT NOT NULL)                     -- 5.2 "parts so far"
//   meta       gc.refs_version · gc.started_at (5.1) · gc.new_pack · gc.pos · gc.fails (this proof)
//   write-backs PackWriter::{resume, append_stored, checkpoint} and PackState (Primitives) · Bucket.inner pub(crate)
//              (as native-lfs) · SliceBudget.req (as clone-plan) · boot requeues stale 'running' GC rows
//              (A4 covers 'dead' only — all three arms resume from tables, so a requeued row just continues)
use std::collections::{HashMap, HashSet};
use gix_hash::ObjectId;
use worker::{SqlStorage, SqlStorageValue as V};
use crate::{error::Error, jobs::{self, Job, JobKind, SliceBudget, SliceOutcome}, pack::ingest::extract_links,
            repo_do::RepoDo,
            store::{codec, keys, Bucket, Index, ObjLoc, ObjRow, PackId, PackMeta, PackState, PackWriter}};
const GRACE_MS: i64 = 3_600_000; const GC_QUIET_MS: i64 = 600_000;                    // section 5
const ROUND: i64 = 512; const BATCH: usize = 90; const ROWS: usize = 9_000;          // round ids · A6 · 1.2 cap
const LOAD: u64 = 32 << 20; const MIN_PART: usize = 5 << 20;                         // A4 read cap · R2 part min
#[derive(serde::Deserialize)] struct N { n: i64 }
#[derive(serde::Deserialize)] struct S { sha: String }
#[derive(serde::Deserialize)] struct P { pack_id: String }
#[derive(serde::Deserialize)] struct B { pack_id: String, bitmap: Vec<u8>, count: i64 }
#[derive(serde::Deserialize)] struct BM { bitmap: Vec<u8> }
#[derive(serde::Deserialize)] struct R { sha: String, pack_id: String, idx: u32, offset: u64, len: u32, kind: u8, size: u64 }
#[derive(serde::Deserialize)] struct E { etag: String }
#[derive(serde::Serialize, serde::Deserialize, Default)]
struct Pos { ci: u32, idx: u32, pack: String, upload: String, st: Option<PackState>,
             total: u32, lo: i64, hi: i64 }                       // committed only at part boundaries (see build)
fn js<T: serde::Serialize>(v: &T) -> Result<V, Error> { Ok(serde_json::to_string(v).map_err(|e| Error::Internal(e.to_string()))?.into()) }
fn put(d: &RepoDo, k: &str, v: impl Into<String>) -> Result<(), Error> {
    exec(&d.sql(), "INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
         vec![k.into(), v.into().into()])?; Ok(())
}
fn loc(r: &R) -> Result<(ObjectId, ObjLoc), Error> {
    Ok((oid(&r.sha)?, ObjLoc { pack: PackId(r.pack_id.clone()), idx: r.idx, offset: r.offset, len: r.len, size: r.size,
        kind: match r.kind { 1 => gix_object::Kind::Commit, 2 => gix_object::Kind::Tree, 3 => gix_object::Kind::Blob,
                             4 => gix_object::Kind::Tag, k => return Err(Error::Internal(format!("kind {k}"))) } }))
}
/// FIPS 180-1 SHA-1 with serde state — shown, not asserted (first-pass caveat). PackWriter holds one and PackState
/// carries it across slices; gix_hash::Hasher cannot be exported. Unit-checked against gix_hash::hasher vectors.
#[derive(serde::Serialize, serde::Deserialize, Clone, Default)]
struct CkptSha1 { h: [u32; 5], len: u64, buf: Vec<u8> }                               // buf.len() < 64
impl CkptSha1 {
    fn new() -> Self { Self { h: [0x67452301, 0xEFCDAB89, 0x98BADCFE, 0x10325476, 0xC3D2E1F0], ..Self::default() } }
    fn update(&mut self, mut d: &[u8]) {
        self.len = self.len.wrapping_add(d.len() as u64);
        while !d.is_empty() {
            let n = (64usize).saturating_sub(self.buf.len()).min(d.len());
            let (a, b) = d.split_at(n); self.buf.extend_from_slice(a); d = b;
            if self.buf.len() == 64 { let mut x = [0u8; 64]; x.copy_from_slice(&self.buf); self.block(&x); self.buf.clear(); }
        }
    }
    fn block(&mut self, b: &[u8; 64]) {
        let mut w = [0u32; 80];
        for (i, c) in b.chunks_exact(4).enumerate() { w[i] = u32::from_be_bytes([c[0], c[1], c[2], c[3]]); }
        for i in 16..80 { w[i] = (w[i-3] ^ w[i-8] ^ w[i-14] ^ w[i-16]).rotate_left(1); }
        let mut s = self.h;
        for (i, wi) in w.iter().enumerate() {
            let (f, k) = match i { 0..=19 => ((s[1] & s[2]) | (!s[1] & s[3]), 0x5A827999u32), 20..=39 => (s[1] ^ s[2] ^ s[3], 0x6ED9EBA1),
                40..=59 => ((s[1] & s[2]) | (s[1] & s[3]) | (s[2] & s[3]), 0x8F1BBCDC), _ => (s[1] ^ s[2] ^ s[3], 0xCA62C1D6) };
            s = [s[0].rotate_left(5).wrapping_add(f).wrapping_add(s[4]).wrapping_add(k).wrapping_add(*wi), s[0], s[1].rotate_left(30), s[2], s[3]];
        }
        for (h, x) in self.h.iter_mut().zip(s) { *h = h.wrapping_add(x); }
    }
    fn fin(mut self) -> [u8; 20] {
        let bits = self.len.wrapping_mul(8); self.update(&[0x80]);
        while self.buf.len() != 56 { self.update(&[0]); }
        self.update(&bits.to_be_bytes());
        let mut o = [0u8; 20]; for (i, h) in self.h.iter().enumerate() { o[i*4..i*4+4].copy_from_slice(&h.to_be_bytes()); } o
    }
}
// bm_set / bm_get / bm_idxs_from(from, limit) / bm_popcount over the marked BLOB: ~30 lines of bit twiddling, not
// shown (same elision as precomputed-clone-pack's Bitmaps). chunks(loads): groups of <= 64 coalesced spans /
// <= LOAD bytes / <= 2,048 entries per read_entries call — the 7.2 merge with A4's cap, ~12 lines, not shown.

/// GcMark (5.1): exact reachability over live objects; one bit per (pack, idx), and each sha marks at most one
/// candidate entry, so the header count the build writes is exact. A frontier row is deleted only inside the span
/// that records its expansion: a kill mid-round replays it — bits are idempotent, kid inserts OR IGNORE.
pub async fn gc_mark(d: &RepoDo, _job: &Job, budget: &mut SliceBudget) -> Result<SliceOutcome, Error> {
    let sql = d.sql();
    if d.meta_opt("gc.started_at")?.is_none() {                                       // first slice (5.1)
        let busy = exec(&sql, "SELECT 1 AS n FROM jobs WHERE kind IN ('gc_consolidate','gc_sweep') \
                               AND state IN ('queued','running') LIMIT 1", vec![])?.to_array::<N>()?;
        if !busy.is_empty() { return Ok(SliceOutcome::Reschedule { run_at: now_ms() + GC_QUIET_MS }); } // chain mid-flight
        let now = now_ms();
        for t in ["marked", "gc_frontier", "gc_seen", "gc_parts"] { exec(&sql, &format!("DELETE FROM {t}"), vec![])?; }
        exec(&sql, "DELETE FROM meta WHERE key LIKE 'gc.%'", vec![])?;
        put(d, "gc.refs_version", d.meta("refs_version")?)?; put(d, "gc.started_at", now.to_string())?;
        exec(&sql, "INSERT INTO marked(pack_id,bitmap) SELECT id, zeroblob((count+7)/8) FROM packs \
                    WHERE state='live' AND created_at < ?", vec![now.saturating_sub(GRACE_MS).into()])?;  // candidates
        exec(&sql, "INSERT OR IGNORE INTO gc_frontier(sha) SELECT target FROM refs UNION \
                    SELECT peeled FROM refs WHERE peeled IS NOT NULL", vec![])?;                  // roots; peeled (A5)
    }
    let bucket = d.bucket()?;
    let cand: HashSet<String> = exec(&sql, "SELECT pack_id FROM marked", vec![])?.to_array::<P>()?
        .into_iter().map(|r| r.pack_id).collect();
    let mut bits: HashMap<String, Vec<u8>> = HashMap::new();           // touched bitmaps, flushed by commit_ids
    loop {
        if budget.spent_80pct() { return Ok(SliceOutcome::Continue { cursor: "{}".into() }); }        // A4
        if d.meta_i64("refs_version")? != d.meta_i64("gc.refs_version")? { return abort(d, &sql); }   // stale marks
        // ---- span A: pop a batch, plan marks and loads; nothing commits until a chunk's span B ----
        let batch: Vec<String> = exec(&sql, "SELECT sha FROM gc_frontier LIMIT ?", vec![ROUND.into()])?
            .to_array::<S>()?.into_iter().map(|r| r.sha).collect();
        if batch.is_empty() {                                                          // frontier drained: chain on
            exec(&sql, "DELETE FROM gc_seen", vec![])?;
            jobs::enqueue(&sql, JobKind::GcConsolidate, now_ms(), "{}")?;              // dispatcher rearms (4.2)
            return Ok(SliceOutcome::Done);
        }
        let rows = exec(&sql, "SELECT o.sha, o.pack_id, o.idx, o.offset, o.len, o.kind, o.size FROM objects o \
            JOIN packs p ON p.id=o.pack_id WHERE p.state='live' AND o.sha IN (SELECT value FROM json_each(?)) \
            ORDER BY o.sha", vec![js(&batch)?])?.to_array::<R>()?;
        let (mut loads, mut prev) = (Vec::<(ObjectId, ObjLoc)>::new(), String::new());
        let mut marked_sha = false;
        for r in &rows {
            if prev != r.sha { prev = r.sha.clone(); marked_sha = false;
                if r.kind != 3 { loads.push(loc(r)?); } }                               // blobs never read (5.1)
            if !marked_sha && cand.contains(&r.pack_id) {                               // mark one candidate copy
                if !bits.contains_key(&r.pack_id) {
                    let b = exec(&sql, "SELECT bitmap FROM marked WHERE pack_id=?", vec![r.pack_id.clone().into()])?
                        .to_array::<BM>()?.into_iter().next().map(|x| x.bitmap).unwrap_or_default();
                    bits.insert(r.pack_id.clone(), b);
                }
                if let Some(bm) = bits.get_mut(&r.pack_id) { bm_set(bm, r.idx); marked_sha = true; }
            }
        }
        let load_ids: HashSet<String> = loads.iter().map(|(id, _)| id.to_string()).collect();
        for chunk in chunks(&loads) {                                                   // <= 64 spans / <= LOAD (A4)
            let ids: Vec<String> = chunk.iter().map(|(id, _)| id.to_string()).collect();
            let mut kids: Vec<String> = Vec::new();
            for (_id, entry) in bucket.read_entries(&chunk, &mut budget.req).await? {   // 7.2; gate open, pushes run
                let (kind, data) = codec::decode_entry(&entry)?;                        // A7 cap inside
                for l in extract_links(kind, &data)? { kids.push(l.to_string()); }
            }
            commit_ids(&sql, &ids, &kids, &mut bits)?;                                  // span B: expansion commits
            if budget.spent_80pct() { break; }                                          // unprocessed ids stay queued
        }
        let rest: Vec<String> = batch.iter().filter(|s| !load_ids.contains(*s)).cloned().collect();
        if !rest.is_empty() { commit_ids(&sql, &rest, &[], &mut bits)?; }               // blobs/misses: nothing to read
    }
}
/// One sync span: ids are now expanded — seen-recorded, unseen kids admitted, frontier rows deleted, bits flushed.
fn commit_ids(sql: &SqlStorage, ids: &[String], kids: &[String], bits: &mut HashMap<String, Vec<u8>>) -> Result<(), Error> {
    exec(sql, "INSERT OR IGNORE INTO gc_seen(sha) SELECT value FROM json_each(?)", vec![js(ids)?])?;
    exec(sql, "INSERT OR IGNORE INTO gc_frontier(sha) SELECT j.value FROM json_each(?) j \
                WHERE NOT EXISTS(SELECT 1 FROM gc_seen s WHERE s.sha=j.value)", vec![js(kids)?])?;
    exec(sql, "DELETE FROM gc_frontier WHERE sha IN (SELECT value FROM json_each(?))", vec![js(ids)?])?;
    for (p, bm) in bits.drain() { exec(sql, "UPDATE marked SET bitmap=? WHERE pack_id=?", vec![bm.into(), p.into()])?; }
    Ok(())
}
/// The 5.3 abort, shared by every arm: kill the build pack if one exists (dead_at starts its Janitor GRACE), drop
/// all gc state, enqueue a fresh mark after the quiet window. The run is over; nothing here is sticky.
fn abort(d: &RepoDo, sql: &SqlStorage) -> Result<SliceOutcome, Error> {
    if let Some(p) = d.meta_opt("gc.new_pack")? {
        exec(sql, "DELETE FROM objects WHERE pack_id=?", vec![p.clone().into()])?;
        exec(sql, "UPDATE packs SET state='dead', dead_at=? WHERE id=? AND state IN ('ingesting','live')",
             vec![now_ms().into(), p.into()])?;
    }
    for t in ["marked", "gc_frontier", "gc_seen", "gc_parts"] { exec(sql, &format!("DELETE FROM {t}"), vec![])?; }
    exec(sql, "DELETE FROM meta WHERE key LIKE 'gc.%'", vec![])?;
    jobs::enqueue(sql, JobKind::GcMark, now_ms() + GC_QUIET_MS, "{}")?;
    Ok(SliceOutcome::Done)
}
/// GcConsolidate (5.2): one new normalized pack holding exactly the marked entries, copied verbatim.
pub async fn gc_consolidate(d: &RepoDo, _job: &Job, budget: &mut SliceBudget) -> Result<SliceOutcome, Error> {
    let sql = d.sql(); let bucket = d.bucket()?;
    loop {
        let Some(mut pos) = (match d.meta_opt("gc.pos")? {
            Some(j) => serde_json::from_str::<Pos>(&j).ok(), None => begin_build(d, &sql)?,
        }) else { return Ok(SliceOutcome::Done) };                       // nothing to repack: state already wiped
        let key = keys::pack(&bucket.repo, &PackId(pos.pack.clone()));
        let etags: Vec<String> = exec(&sql, "SELECT etag FROM gc_parts ORDER BY part_no", vec![])?
            .to_array::<E>()?.into_iter().map(|r| r.etag).collect();
        let mut out = if pos.upload.is_empty() {
            let w = PackWriter::create(&bucket, &key, pos.total, &mut budget.req).await?;   // "PACK"+v2+count hdr
            pos.upload = w.upload_id().to_string(); w               // upload id commits with the first part; a
        } else { PackWriter::resume(&bucket, &key, &pos.upload, &etags, pos.st.as_ref()    // kill before then only
            .ok_or_else(|| Error::Internal("gc.pos without PackState".into()))?, &mut budget.req).await? }; // orphans the MPU (~7d)
        match build(d, &sql, &bucket, &key, &mut out, &mut pos, budget).await? {
            Flow::Yield => return Ok(SliceOutcome::Continue { cursor: "{}".into() }),
            Flow::Done => return Ok(SliceOutcome::Done),
            Flow::Retry(e) => return Err(e),                             // 4.4 backoff; second failure -> Rebuild
            Flow::Rebuild => wipe_build(d, &sql)?,                       // dead upload: marks are intact, restart
        }
    }
}
enum Flow { Yield, Done, Retry(Error), Rebuild }
/// Fill >= 5 MiB parts with the marked entries in (pack_id, idx) order — deterministic, so a killed slice replays
/// the same bytes to the same part numbers and insert_objects' ON CONFLICT DO NOTHING absorbs replayed rows.
async fn build(d: &RepoDo, sql: &SqlStorage, bucket: &Bucket, key: &str, out: &mut PackWriter, pos: &mut Pos,
               budget: &mut SliceBudget) -> Result<Flow, Error> {
    let cands: Vec<String> = exec(sql, "SELECT pack_id FROM marked ORDER BY pack_id", vec![])?
        .to_array::<P>()?.into_iter().map(|r| r.pack_id).collect();
    let (mut staged, mut bm, mut in_part): (Vec<ObjRow>, (u32, Vec<u8>), u64) = (vec![], (u32::MAX, vec![]), 0);
    loop {
        if budget.spent_80pct() { return Ok(Flow::Yield); }
        let mut idxs: Vec<u32> = Vec::new();
        while idxs.is_empty() {                                                     // next <= BATCH marked idxs
            match cands.get(pos.ci as usize) {
                // input exhausted: finish owns tail+trailer as the last part (any size allowed), then completes.
                // Resume-safe: a kill between that upload and complete replays finish with identical bytes/parts.
                None => {
                    Index(sql).insert_objects(&PackId(pos.pack.clone()), &staged)?; staged.clear();
                    return match out.finish(&mut budget.req).await {
                        Ok(meta) => finish_build(d, sql, pos, &meta),
                        Err(e) => mpu_err(d, sql, bucket, key, pos, e, budget).await,
                    };
                }
                Some(cid) => {
                    if bm.0 != pos.ci { bm.1 = exec(sql, "SELECT bitmap FROM marked WHERE pack_id=?",
                        vec![cid.clone().into()])?.to_array::<BM>()?.into_iter().next().map(|x| x.bitmap)
                        .unwrap_or_default(); bm.0 = pos.ci; }
                    idxs = bm_idxs_from(&bm.1, pos.idx, BATCH);
                    match idxs.last() { Some(l) => pos.idx = l.saturating_add(1),
                                        None => { pos.ci = pos.ci.saturating_add(1); pos.idx = 0; } }
                }
            }
        }
        let rows = exec(sql, "SELECT sha, pack_id, idx, offset, len, kind, size FROM objects WHERE pack_id=? \
            AND idx IN (SELECT value FROM json_each(?))",
            vec![cands.get(pos.ci as usize).cloned().unwrap_or_default().into(), js(&idxs)?])?.to_array::<R>()?;
        if rows.len() != idxs.len() { return Err(Error::Internal("marked entry lacks objects row".into())); }
        let locs: Vec<(ObjectId, ObjLoc)> = rows.iter().map(loc).collect::<Result<_,_>>()?;
        for chunk in chunks(&locs) {                                                // <= 64 spans / <= LOAD (A4)
            for (id, entry) in bucket.read_entries(&chunk, &mut budget.req).await? {
                let r = rows.iter().find(|r| r.sha == id.to_string()).ok_or_else(|| Error::Internal("row".into()))?;
                let (off, len) = out.append_stored(&entry)?;                         // verbatim; sha+count inside
                staged.push(ObjRow::new(id, off, len, loc(r)?.1.kind, r.size));
                in_part = in_part.saturating_add(u64::from(len));
                if r.kind == 1 { pos.lo = pos.lo.min(off as i64); pos.hi = pos.hi.max(off.saturating_add(u64::from(len)) as i64); }
            }
        }
        if staged.len() >= ROWS { Index(sql).insert_objects(&PackId(pos.pack.clone()), &staged)?; staged.clear(); }
        if let Some(f) = flush(d, sql, bucket, key, out, pos, &mut staged, &mut in_part, budget).await? { return Ok(f); }
    }
}
/// Upload the buffered part once it reaches MIN_PART, then commit — one span — the rows, the etag and the
/// position. The sub-5 MiB tail is never checkpointed: finish folds it into the final part (R2's last-part rule).
/// Returns None while the build continues; Some(Flow) only on a failed upload.
async fn flush(d: &RepoDo, sql: &SqlStorage, bucket: &Bucket, key: &str, out: &mut PackWriter, pos: &mut Pos,
               staged: &mut Vec<ObjRow>, in_part: &mut u64, budget: &mut SliceBudget) -> Result<Option<Flow>, Error> {
    if *in_part < MIN_PART as u64 { return Ok(None); }
    let Some((up, st)) = (match out.checkpoint(&mut budget.req).await {
        Ok(x) => x,
        Err(e) => return mpu_err(d, sql, bucket, key, pos, e, budget).await.map(Some),
    }) else { return Ok(None) };
    *in_part = 0;
    Index(sql).insert_objects(&PackId(pos.pack.clone()), staged)?; staged.clear();  // OR IGNORE: replay-safe
    exec(sql, "INSERT OR REPLACE INTO gc_parts(part_no,etag) VALUES(?,?)", vec![i64::from(up.part_number()).into(), up.etag().into()])?;
    pos.st = Some(st);
    put(d, "gc.pos", serde_json::to_string(pos).map_err(|e| Error::Internal(e.to_string()))?)?;
    Ok(None)
}
/// complete() may have succeeded before an error/kill: head() on the key is the oracle. A truly dead upload
/// (expired MPU, aborted) gives no key: retry once through 4.4, then rebuild from the intact marks.
async fn mpu_err(d: &RepoDo, sql: &SqlStorage, bucket: &Bucket, key: &str, pos: &Pos, e: worker::Error,
                 budget: &mut SliceBudget) -> Result<Flow, Error> {
    budget.req.charge(1)?;
    if let Some(h) = bucket.inner.head(key).await? {                            // complete() had already won
        let meta = PackMeta { count: pos.total, bytes: h.size(),
            commit_lo: u64::try_from(pos.lo).unwrap_or(u64::MAX), commit_hi: u64::try_from(pos.hi).unwrap_or(0) };
        return finish_build(d, sql, pos, &meta).map(|_| Flow::Done);
    }
    let f = d.meta_i64("gc.fails").unwrap_or(0).saturating_add(1);
    put(d, "gc.fails", f.to_string())?;
    Ok(if f >= 2 { Flow::Rebuild } else { Flow::Retry(Error::Storage(e.to_string())) })
}
/// First build slice (5.2): repack only if >= 2 candidates or any candidate has unmarked entries — otherwise wipe
/// gc state and Done, because sweeping without a new pack would delete reachable objects.
fn begin_build(d: &RepoDo, sql: &SqlStorage) -> Result<Option<Pos>, Error> {
    if d.meta_i64("refs_version")? != d.meta_i64("gc.refs_version")? { abort(d, sql)?; return Ok(None); }
    let cands = exec(sql, "SELECT m.pack_id, m.bitmap, p.count FROM marked m JOIN packs p ON p.id=m.pack_id ORDER BY m.pack_id", vec![])?.to_array::<B>()?;
    let (mut total, mut unmarked) = (0u32, false);
    for c in &cands { let n = bm_popcount(&c.bitmap); total = total.saturating_add(n); unmarked |= i64::from(n) < c.count; }
    if !(cands.len() >= 2 || unmarked) {
        if let Some(p) = d.meta_opt("gc.new_pack")? {                              // half-written leftover, no pos
            exec(sql, "DELETE FROM objects WHERE pack_id=?", vec![p.clone().into()])?;
            exec(sql, "UPDATE packs SET state='dead', dead_at=? WHERE id=? AND state='ingesting'", vec![now_ms().into(), p.into()])?;
        }
        for t in ["marked", "gc_frontier", "gc_seen", "gc_parts"] { exec(sql, &format!("DELETE FROM {t}"), vec![])?; }
        exec(sql, "DELETE FROM meta WHERE key LIKE 'gc.%'", vec![])?;
        return Ok(None);
    }
    let pack = PackId::random();
    exec(sql, "INSERT INTO packs(id,state,count,bytes,commit_lo,commit_hi,push_id,created_at) VALUES(?,'ingesting',0,0,?,0,NULL,?)",
         vec![pack.0.as_str().into(), i64::MAX.into(), now_ms().into()])?;
    let pos = Pos { pack: pack.0.clone(), total, lo: i64::MAX, ..Pos::default() };
    put(d, "gc.new_pack", pack.0)?; put(d, "gc.pos", serde_json::to_string(&pos).map_err(|e| Error::Internal(e.to_string()))?)?;
    Ok(Some(pos))
}
fn wipe_build(d: &RepoDo, sql: &SqlStorage) -> Result<(), Error> {
    if let Some(p) = d.meta_opt("gc.new_pack")? {
        exec(sql, "DELETE FROM objects WHERE pack_id=?", vec![p.clone().into()])?;
        exec(sql, "UPDATE packs SET state='dead', dead_at=? WHERE id=? AND state='ingesting'", vec![now_ms().into(), p.into()])?;
    }
    exec(sql, "DELETE FROM gc_parts", vec![])?;
    exec(sql, "DELETE FROM meta WHERE key IN ('gc.pos','gc.new_pack','gc.fails')", vec![])?;
    Ok(())
}
fn finish_build(d: &RepoDo, sql: &SqlStorage, pos: &Pos, meta: &PackMeta) -> Result<SliceOutcome, Error> {
    exec(sql, "UPDATE packs SET state='live', count=?, bytes=?, commit_lo=?, commit_hi=? WHERE id=? AND state IN ('ingesting','live')",
         vec![i64::from(meta.count).into(), meta.bytes.into(), i64::try_from(meta.commit_lo).unwrap_or(i64::MAX).into(),
              i64::try_from(meta.commit_hi).unwrap_or(0).into(), pos.pack.as_str().into()])?;
    if !changed(sql)? { return Err(Error::Internal("build pack row".into())); }
    jobs::enqueue(sql, JobKind::GcSweep, now_ms(), "{}")?;                           // the only deleter (5.3)
    Ok(SliceOutcome::Done)                                                         // gc.* cleared by the sweep span
}
/// GcSweep (5.3): the whole step is one sync span — the refs_version check, the row deletes and the gc_epoch bump
/// are atomic — and it never touches R2: dead keys are the Janitor's, after GRACE.
pub fn gc_sweep(d: &RepoDo) -> Result<SliceOutcome, Error> {
    let sql = d.sql();
    if d.meta_opt("gc.refs_version")?.is_none() { return Ok(SliceOutcome::Done); }
    if d.meta_i64("refs_version")? != d.meta_i64("gc.refs_version")? { return abort(d, &sql); }
    exec(&sql, "UPDATE packs SET state='dead', dead_at=? WHERE id IN (SELECT pack_id FROM marked)", vec![now_ms().into()])?;
    exec(&sql, "DELETE FROM objects WHERE pack_id IN (SELECT pack_id FROM marked)", vec![])?;
    exec(&sql, "UPDATE meta SET value=value+1 WHERE key='gc_epoch'", vec![])?;
    for t in ["marked", "gc_frontier", "gc_seen", "gc_parts"] { exec(&sql, &format!("DELETE FROM {t}"), vec![])?; }
    exec(&sql, "DELETE FROM meta WHERE key LIKE 'gc.%'", vec![])?;
    Ok(SliceOutcome::Done)
}
```

## Why it works
- **The dangling-ref race is closed at every interleaving.** `gc_sweep` is one sync span: the `refs_version` comparison, the `objects`-row deletes, the `packs.state='dead'` flips and the `gc_epoch` bump all land or none do — there is no "check, then await, then delete" seam like the first pass had. A push that commits between mark start and sweep bumps `refs_version` in its own span (3 step 5), so `gc_sweep` takes `abort` — kills the build pack, wipes state, re-marks; the push's objects are safe because their pack is `live` and was never a candidate (`created_at` predicate is evaluated at mark start and can only shrink, never grow). A push that looked up objects before the sweep and commits after is rejected by `gc_epoch` at `commit_push` step 2 with `gc ran during push, retry` — no dangling ref, a resubmittable push. A push that begins after the sweep simply never sees the dead rows. And a fetch that resolved locs before the sweep still reads bytes afterwards, because R2 deletion is the Janitor's job at `dead_at + GRACE` (1 h) while a request lives at most `max_ms` 240 s (7.1).
- **Stall-to-death cannot recur.** There is no `meta.gc` latch anywhere: liveness is the `jobs` row, and a dead `GcMark`/`GcConsolidate`/`GcSweep` is re-enqueued at the next `boot` (A4). Every await boundary replays, never loses: the frontier is a table row deleted only in the span that records its expansion; parts re-upload byte-identically (deterministic `(pack_id, idx)` scan plus verbatim entries, so `upload_part` overwrite-before-complete is safe); `objects` rows land `ON CONFLICT DO NOTHING`. `complete`-then-crash is disambiguated by `head(key)` — key exists means the upload finished and `finish_build` re-runs its idempotent span; key absent means the MPU is dead (`NoSuchUpload` is permanent under S3 semantics), so `gc.fails >= 2` triggers `wipe_build` and a rebuild from the intact `marked` bitmaps — the 7-day MPU expiry caveat becomes a restart, not a wedge. A marked idx with no `objects` row is pre-existing corruption, so it fails the build loudly (`Internal`) instead of writing a short pack.
- **Alarm ownership is structural.** This module contains no `set_alarm` call at all; it adds no rows beyond job rows. `enqueue` writes a row synchronously (4.5), the dispatcher marks it `running`, runs a slice, applies the `SliceOutcome`, and `jobs::rearm` — the only alarm setter (4.1, A3; #5 measured) — schedules the next firing. Janitor, precomputed clone, cache and any future kind coexist in the same queue; the dedup-by-kind and the sibling-busy `Reschedule` in `gc_mark` keep two GC links from overlapping.
- **Entry count, trailer and consumers are right this time.** `pos.total` is the popcount of the marked bitmaps and marking sets at most one bit per sha, so the header count equals appended entries exactly — no marked-vs-emitted mismatch. The 20-byte SHA-1 trailer is inside the object (`finish` appends it before `complete`), so the stored key is a well-formed pack that `index-pack` and the 2.3 read path accept, and precomputed-clone-pack's own second pass consumes it as the clone source. "Covered tips" is a non-problem under the contract: readers resolve through `objects` rows, not through a `pack_builds` record, and the clone-pack job computes its own bitmaps.
- **Budgets are honest.** A mark round pops <= 512 ids, reads them in chunks of <= 64 coalesced spans / <= 32 MiB / <= 2,048 entries, and the 80% check runs between chunks — worst case ~6 chunks per 400-subrequest slice, i.e. ~380 scattered objects per slice and far more when entries pack contiguously. A build unit is one objects batch + one coalesced read + one `upload_part` per >= 5 MiB part (~1.6 GiB written per slice when parts upload back-to-back). `gc_sweep` costs zero subrequests. There is no per-object GET and no per-object inflate/deflate — the cost review item collapses to roughly `candidate_bytes / 8 MiB` reads plus the same again in `upload_part`s per full GC.
- Conformance scenarios this proof must pass (section 11): **14** (its own), **3** (annotated tags — `refs.peeled` seeds the frontier, A5), **6** (concurrent pushes against `gc_epoch`), **12** (post-GC clone still fsck-clean). Added to the list, up to two: **(a)** force-push landing mid-GC — sweep aborts, every ref intact, fresh `GcMark` queued, clone clean; **(b)** a push begun before the sweep commits after it — commit returns `gc ran during push, retry`, `gc_epoch` bumped exactly once, retry succeeds.

## Changes from the first pass
| First-pass finding (quoted) | Kind | How addressed |
|---|---|---|
| "Schema/key/encoding contract mismatch with `two-phase-push`, `content-addressed-r2-keys` and `precomputed-clone-pack` — mark, pack and serve each fail on the sibling-built repo" | blocker | Storage is normalized packs only (2.1, 2.2): reachability walks the live `objects`/`packs` join (2.3), links come from `extract_links` over entry bytes (1.4) — no `links` column needed — and the build copies stored entries verbatim via `append_stored`. No `objects/<sha>` reads, no `pack_builds`, no DecompressionStream; the output is an ordinary `packs/` key with `objects` rows, consumed through the same index as any push pack and by precomputed-clone-pack by design. |
| "Sweep race: `refs_version` check is not atomic with the R2 delete; a resurrecting push during the delete leaves a dangling ref" | blocker | `gc_sweep` is one sync span and performs zero R2 calls: check, row deletes, pack kills, `gc_epoch` bump are atomic; the Janitor deletes keys after `GRACE`. The three interleavings are enumerated in Why it works — every ordering ends either in `abort` (refs moved), a `gc_epoch` commit rejection, or a harmless stale-loc fetch inside `GRACE`. |
| "Stall-to-death: `mpu.complete` re-run after rollback, spread-args RangeError, or a missing row exhausts alarm retries and `meta.gc` then suppresses re-arming forever; no watchdog" | blocker | No latch exists: state is derived rows (`gc.*`, `gc_parts`, `marked`), jobs are re-enqueued at boot (A4), `Err` gets 4.4 backoff. `complete`-then-crash is resolved by `head()` on the key (finish idempotently vs. rebuild); a truly dead MPU restarts after `gc.fails >= 2`. No spread-arguments anywhere; `Vec::extend_from_slice` throughout. |
| "Single DO alarm shared with at least four sibling ideas; `setAlarm(Date.now())` chaining silently cancels theirs" | blocker | The alarm is owned by `jobs::rearm` alone (4.1); GC is three `JobKind` rows in the shared queue (4.5), chained by `enqueue` at `Done`-time. A push-enqueued `GcMark` while the chain is mid-flight hits the sibling-busy check and reschedules itself — it never steals the alarm. |
| "`gc.buf` as `number[]` in JSON exceeds the 2 MB SQLite row limit and costs ~8× the bytes in heap; must become BLOB chunks or an R2 scratch key" | caveat | The part buffer lives only inside `PackWriter` in memory, bounded by `checkpoint` at >= 5 MiB; durable build state is `gc.pos` (~200 B JSON) + `gc_parts` etag rows + `marked` BLOB bitmaps (~125 KiB per million entries). Nothing large is serialized. |
| "No delta compression and loose objects retained after repack: R2 storage roughly doubles; the pack is a clone accelerator, not a space reclaimer, until a second sweep learns 'covered by pack'" | caveat | Half superseded, half honestly retained. There are no loose objects to retain (2.1); candidates go `dead` and their keys are deleted after `GRACE`, so dead objects and duplicate copies are genuinely reclaimed. But normalized packs are full-object by contract (2.1/12): no delta search happens, so reachable bytes do not shrink — this is merge-plus-prune, not `git repack -adf`. |
| "Cost/throughput: one Class B GET plus an inflate+deflate per reachable object per build; 1M-object repos need churn gating and take hours of alarm slices during which every `commit()` queues behind mark batches" | caveat | Reads are `read_entries`-coalesced (bytes/8 MiB spans, not per-object), blobs are never read at all, and entries are copied verbatim — zero inflate/deflate. Churn gating is the 5.2 trigger (>= 2 candidates or any unmarked), `GC_QUIET` debounce and dedup'd enqueue. Commits do not queue behind mark batches: sync spans are ~90-row queries and awaits open the input gate between rounds (#4). |
| "`resumeMultipartUpload` uploads expire after 7 days; state must detect `NoSuchUpload` and restart the pack phase" | caveat | Explicitly handled: `mpu_err` distinguishes *upload finished* (`head` finds the key — complete won the race) from *upload gone* (`gc.fails` increments; at 2 the build wipes and restarts from intact `marked` bitmaps). Re-uploaded parts carry identical bytes. |
| "Pure-JS serialisable SHA-1 is asserted, not shown; `crypto.subtle` cannot checkpoint" | caveat | Shown in full: `CkptSha1` above — `{h:[u32;5], len:u64, buf}` is serde state carried inside `PackState`, unit-checked against `gix_hash::hasher` on the FIPS vectors and a live pack build. |
| "Marked count vs emitted entries disagree when a marked sha lacks an index row — the pack header promise breaks and `index-pack` dies" (interop check) | blocker | `rows.len() != idxs.len()` returns `Internal` before a byte is uploaded; marking dedups to one bit per sha so `pos.total` (popcount) equals appended entries, and `finish` writes that exact count. A short or padded pack is impossible. |
| "The object stored in R2 has no trailer, so 'the same R2 object is a well-formed .pack' is false" (interop check) | blocker | `PackWriter::finish` appends the 20-byte trailer (from `CkptSha1`, checkpointable across slices) as the final part before `complete`; the stored object is the `.pack`, byte-exact per 2.1. |
| "Covered tips are never recorded; a consumer using refs-at-completion misses commits pushed mid-GC" (interop check) | caveat | Dissolved by the storage model: the pack is not a standalone artifact — it is read through `objects` rows (2.3) like every other pack, so "coverage" is whatever the index says. Clone-snapshot tip selection belongs to precomputed-clone-pack, which records its own. |
| "Open (uncommitted) pushes must be added as mark roots or excluded by `created_at`, otherwise their objects die mid-flight" (first-pass Known limit) | blocker | Resolved by `gc_epoch` (3 step 2), not by extra roots: an open push's `ingesting` pack is invisible to the mark's live-only query, and if the sweep runs first the commit is rejected with `gc ran during push, retry`. Correctness holds; the cost is a resubmitted push. |

## Known limits
- **Not a delta repack.** All packs at rest are full-object (2.1, 12); consolidate merges packs, drops unreachable entries and dedups across packs, but does not shrink the bytes of live objects. The title's "repack" claim survives in the weaker form — storage falls to `live-reachable bytes`, never below.
- **Starvation is real.** Any ref move before the sweep aborts the run (5.3). The per-round and per-build-slice `refs_version` rechecks bound wasted work to at most one round/part of extra effort, but a repo pushed more often than a full chain takes never sweeps — it keeps merging attempts. `GC_QUIET` and the candidate-age rule only shift where the threshold sits.
- **Pushes lose the sweep window.** A push whose lookup spans the sweep is rejected at commit (`gc ran during push, retry`) — correct, client-visible, and forces a full re-push.
- **Stuck `running` rows.** An isolate kill mid-slice leaves a `jobs` row `running`, which no selection predicate reaches (4.2 picks `queued`); A4 re-enqueues `dead` maintenance jobs only. Write-back: `boot` also requeues stale `running` rows — safe for all three arms because resume state lives in tables, not the row.
- **Abandoned MPUs** (kill between `create` and first `checkpoint`, or after `wipe_build`) rely on R2's incomplete-upload abort (~7 days); a bucket lifecycle rule is the same day-1 item as in two-phase-push.
- **Unverified bindings** (day-1 check): `Vec<u8> -> SqlStorageValue` blob binding, `MultipartUpload::upload_id`/`UploadedPart::{part_number, etag}` accessors, `upload_part` overwrite semantics, real-R2 multipart behavior generally (#6), `web_sys::Crypto` path, and the `PackWriter::{resume, append_stored, checkpoint}` write-backs this module shares with server-side-rebase.
- **Memory and table growth.** Bitmap cache is `count/8` per touched pack per slice; `gc_frontier` + `gc_seen` hold one 40-hex row per pending/expanded object — a 10M-object walk stores ~800 MB of TEXT across the run, well inside DO storage but worth a comment in the ops docs.
- **Mark reads every reachable non-blob entry once** — inherent to exact reachability without a `links` index, and the price paid for the first-pass schema fix. Indexing links eagerly at ingest (an `objects.links` column) would make mark a pure SQLite walk and is a separate idea; this proof deliberately does not require it.

## Depends on
`repo-do-ref-authority` (jobs dispatcher, `meta` helpers, ref/`refs_version` contract), `refs-sqlite-objects-r2` (`Index::lookup` shape, `objects`/`packs` semantics, `exec`/`changes` helpers), `two-phase-push` (`commit_push` step-7 enqueue, `pushes.gc_epoch`, Janitor contract), `streaming-pack-parser` (`decode_entry`, `extract_links`, ingest-side `objects` rows), `precomputed-clone-pack` (the consumer that makes the built pack pay rent).
