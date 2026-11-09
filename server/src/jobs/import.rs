//! I1: server-side resumable pack import (ROADMAP I1). One `import_pack` job rides
//! the normal push lifecycle: /_do/import/start opens the pushes row and queues the
//! job; each alarm slice ingests another bounded share of the staged pack under a
//! fresh request budget; the final slice runs the same commit_push a live push does
//! (atomic) and Done deletes the job row.
//!
//! Slice-resume model, mirrored on gc_consolidate:
//!   * pass A parses entry headers off the staged pack into `import_toc` — the
//!     cursor is a byte offset, resumable at any entry boundary.
//!   * pass B appends normalized entries to the output MPU. Durable state = R2
//!     upload parts (import_parts etags) + WriterCkpt; objects rows may run ahead
//!     of the durable prefix, so every checkpoint deletes rows past it — a resume
//!     re-runs exactly the entries whose bytes never landed, and the boundary
//!     entry itself resumes mid-body via `frag`/`one_entry(skip)`.
//!   * 2.5 links live in `push_links` (SQLite), not memory — the set survives
//!     slices and packs whose link count exceeds the in-memory MAX_LINKS cap.
//!   * the pushes row's began_at heartbeats each slice so the janitor's 1h
//!     PUSH_TIMEOUT never expires an actively progressing import.

use std::collections::{HashMap, HashSet};

use gix_hash::ObjectId;
use gix_pack::data::{entry::Header, Entry as PackEntry};
use gix_zlib::{Decompress, FlushDecompress, Inflate, Status};
use serde::{Deserialize, Serialize};
use worker::{SqlStorage, SqlStorageValue as V};

use super::{Job, SliceBudget, SliceOutcome};
use crate::error::Error;
use crate::pack::ingest::{
    one_entry, AnySink, Cache, Cx, Externals, IdMap, JobSink, Source, Step, Toc, Window,
    MAX_ENTRY_WIRE, MAX_OBJ, MAX_STREAM_BLOB,
};
use crate::platform;
use crate::remote;
use crate::repo_do::{scope_allows, CmdDto, CommitRequest, RepoDo};
use crate::store::{keys, Bucket, Index, ObjRow, PackId, PackMeta, PackWriter, PushId, RawWriter, WriterCkpt, PART};
use crate::wire::{Pkt, PktReader};
use crate::ReqBudget;
use futures_util::StreamExt;

/// Fresh queue rows per resolve pass — 20k small entries ≈ 40 R2 reads worst case,
/// well inside a 400-subrequest slice even with delta-base lookups.
const QUEUE_BATCH: i64 = 20_000;
/// import_toc insert fan-out: DO sqlite binds ~100 params per exec (9 cols × 10).
const TOC_FANOUT: usize = 10;
/// 2.5 sweep page.
const CHECK_PAGE: i64 = 1_000;
/// import_tail row size — under the SqlStorage value ceiling; a <PART tail is
/// ~128 rows.
const TAIL_CHUNK: usize = 64 << 10;

#[derive(Deserialize, Serialize, Clone)]
struct Part {
    key: String,
    bytes: u64,
}

#[derive(Deserialize)]
pub struct Payload {
    push: String,
    /// output pack id — minted at /_do/import/start
    pack: String,
    /// staged client parts — empty for a url import until the fetch phase
    /// streams the remote's pack into pending/ and rewrites this payload
    #[serde(default)]
    parts: Vec<Part>,
    principal: String,
    #[serde(default)]
    commands: Vec<CmdDto>,
    /// WILD url import: the remote smart-HTTP base the fetch phase clones
    #[serde(default)]
    url: Option<String>,
    /// the remote's HEAD symref — adopted at commit so clones check out the
    /// same default branch the source advertises
    #[serde(default)]
    head: Option<String>,
    /// 'sha1'|'sha256' — set at import_start for staged pushes, discovered from
    /// the remote's advertisement for url imports. Absent means sha1.
    #[serde(default)]
    format: Option<String>,
}
impl Payload {
    fn kind(&self) -> gix_hash::Kind {
        match self.format.as_deref() {
            Some("sha256") => gix_hash::Kind::Sha256,
            _ => gix_hash::Kind::Sha1,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct Parked {
    i: u32,
    base: String,
}

fn neg_one() -> i64 {
    -1
}

#[derive(Serialize, Deserialize)]
#[serde(default)]
struct Cursor {
    phase: String, // "" | "resolve" | "check" | "commit"
    // pass A ("" = still parsing the entry table)
    parse_pos: u64,
    next_idx: u32,
    count: u32,
    // pass B
    parked: Vec<Parked>,
    upload: String,
    st: Option<WriterCkpt>,
    // 2.5 sweep
    check_after: String,
    /// gc_epoch pinned when the check phase started (C1): commit_slice and
    /// commit_meta compare it to the live epoch — a GC sweep between check and
    /// commit means the connectivity verdicts are stale and the check re-runs
    check_epoch: Option<i64>,
    /// set when a commit guard sent the job back through check after a sweep:
    /// inside such a re-check a still-missing link was swept, not merely
    /// pending inside another open push — reject with the reason instead of
    /// retrying (C1); a first-run missing link keeps the old retry behaviour
    recheck: bool,
    // resolve queue position: entries at or below it are done (objects row),
    // parked (c.parked), or non-durable (requeue set rebuilt per slice) —
    // the scan is monotonic and never re-walks them
    #[serde(default = "neg_one")]
    scan_after: i64,
    // byte length of the writer tail persisted to import_tail at the last
    // yield — 0 when nothing undrained survived the slice
    #[serde(default)]
    tail: u64,
}
impl Default for Cursor {
    fn default() -> Self {
        Self {
            phase: String::new(),
            parse_pos: 0,
            next_idx: 0,
            count: 0,
            parked: Vec::new(),
            upload: String::new(),
            st: None,
            check_after: String::new(),
            check_epoch: None,
            recheck: false,
            scan_after: -1,
            tail: 0,
        }
    }
}

fn exec(sql: &SqlStorage, q: &str, args: Vec<V>) -> Result<worker::SqlCursor, Error> {
    crate::store::exec(sql, q, args)
}
fn i64v(n: u64) -> Result<V, Error> {
    Ok(V::from(i64::try_from(n).map_err(|_| Error::Internal("u64 to i64".into()))?))
}
fn unpack(m: impl Into<String>) -> Error {
    Error::Unpack(m.into())
}
fn continue_(c: &Cursor) -> Result<SliceOutcome, Error> {
    Ok(SliceOutcome::Continue {
        cursor: serde_json::to_string(c).map_err(|e| Error::Internal(e.to_string()))?,
    })
}

/// C1: send the job back through the connectivity check — the live gc_epoch
/// moved since the check pinned its own, so its link verdicts are stale. The
/// re-run re-pins the epoch and rejects a still-missing link outright.
fn reset_to_check(c: &mut Cursor) {
    c.phase = "check".into();
    c.check_after.clear();
    c.check_epoch = None;
    c.recheck = true;
}

/// True for a legacy (pre-check_epoch) cursor caught mid-check: it carries
/// check_after pages but pins no epoch, so the pages it already walked were
/// verified under an unknown older epoch — pinning the live epoch now would
/// let a sweep between those pages and the upgrade go unnoticed (C1). The
/// check restarts from the front instead.
fn check_needs_restart(check_epoch: Option<i64>, check_after: &str) -> bool {
    check_epoch.is_none() && !check_after.is_empty()
}

pub async fn run_slice(d: &RepoDo, job: &Job, budget: &mut SliceBudget) -> Result<SliceOutcome, Error> {
    let p: Payload =
        serde_json::from_str(&job.payload).map_err(|e| Error::Internal(format!("import payload: {e}")))?;
    let mut c: Cursor = job
        .cursor
        .as_deref()
        .map(serde_json::from_str)
        .transpose()
        .map_err(|e| Error::Internal(format!("import cursor: {e}")))?
        .unwrap_or_default();
    let sql = d.sql();
    // the pushes row is the janitor's lease: keep it alive while we progress, and
    // notice immediately if it was aborted/expired out from under the job
    #[derive(Deserialize)]
    struct S {
        state: String,
    }
    let st = exec(&sql, "SELECT state FROM pushes WHERE id=?", vec![V::from(p.push.as_str())])?
        .to_array::<S>()?
        .into_iter()
        .next()
        .map(|s| s.state)
        .unwrap_or_default();
    if st != "open" {
        // the pushes row is the truth: committed means a dead slice's
        // commit_push already won (Done deletes the job row), aborted/expired
        // means the import is moot — either way there is nothing to retry
        worker::console_log!("import {} push is {st} — job done", p.push);
        return Ok(SliceOutcome::Done);
    }
    exec(
        &sql,
        "UPDATE pushes SET began_at=? WHERE id=?",
        vec![V::from(platform::now_ms()), V::from(p.push.as_str())],
    )?;
    let out = match c.phase.as_str() {
        // a url import's first slice: the worker is the git client — ls-refs
        // mints the commands, one v2 fetch streams the pack into pending/ and
        // rewrites this job's payload into the staged shape the phases below own
        "fetch" => fetch_slice(d, job, &sql, &p, &mut c, budget).await,
        "" => toc_slice(d, job, &sql, &p, &mut c, budget).await,
        "resolve" => resolve_slice(d, job, &sql, &p, &mut c, budget).await,
        "check" => check_slice(d, job, &sql, &p, &mut c, budget).await,
        "commit" => commit_slice(d, job, &sql, &p, &mut c, budget).await,
        ph => Err(Error::Internal(format!("import phase {ph}"))),
    };
    match out {
        // The bound MPU is gone — aborted by a dead slice's finish path, reaped
        // server-side, or lost with the isolate. Retrying the same slice fails
        // forever: either commit the durable object complete() already landed,
        // or wipe the output state and rebuild it on a fresh upload.
        Err(e) if dead_upload(&e) => recover_dead_upload(d, job, &sql, &p, &mut c, budget).await,
        out => out,
    }
}

// ── url import: fetch phase ───────────────────────────────────────────────

/// Deterministic fetch-side failure — close the push so status surfaces the
/// reason and Done retires the job. Retrying a 404 or a scope miss forever
/// would just burn attempts.
async fn reject_push(sql: &SqlStorage, job: &Job, push: &str, why: &str) -> Result<SliceOutcome, Error> {
    // CAS before write: a stale slice must not land the rejection (A19)
    if !super::heartbeat(sql, job)? {
        return Err(super::stale_lease());
    }
    worker::console_log!("import {push} rejected: {why}");
    exec(
        sql,
        "UPDATE pushes SET state='rejected', result=?, ended_at=? WHERE id=? AND state='open'",
        vec![
            V::from(serde_json::json!([["remote", why]]).to_string().as_str()),
            V::from(platform::now_ms()),
            V::from(push),
        ],
    )?;
    Ok(SliceOutcome::Done)
}

/// A fetch answer that can never succeed — protocol/remote rejection — versus
/// a transport blip a re-fetch might beat. Only the first kind rejects the push.
fn deterministic(e: &Error) -> bool {
    // the memory-pressure Limit is transient: the slice retries it
    matches!(e, Error::Protocol(_) | Error::Limit(_)) && !crate::try_grow::is_retry(e)
}

/// payload.url set: clone the remote ourselves. ls-refs mints the command set
/// (every advertised ref becomes a create — a url import mirrors the source's
/// tips), the push's captured token scope vets each, then one v2 fetch streams
/// the whole-history pack through a sha1-lagged hasher into a pending/ part.
/// The slice cannot yield mid-response, so a dead isolate restarts the whole
/// download — bounded by the remote's pack size. Success rewrites this job's
/// payload into the staged shape and falls into "" (pass A) next slice.
async fn fetch_slice(
    d: &RepoDo,
    job: &Job,
    sql: &SqlStorage,
    p: &Payload,
    c: &mut Cursor,
    budget: &mut SliceBudget,
) -> Result<SliceOutcome, Error> {
    let url = p
        .url
        .as_deref()
        .ok_or_else(|| Error::Internal("fetch phase without url".into()))?;
    let adv = match remote::ls_refs(url).await {
        Ok(a) => a,
        Err(e) if deterministic(&e) => return reject_push(sql, job, &p.push, &e.message()).await,
        Err(e) => return Err(e),
    };
    if adv.refs.is_empty() {
        return reject_push(sql, job, &p.push, "remote advertises no refs").await;
    }
    // CAS before write: a stale slice must not pin the repo format (A19)
    if !super::heartbeat(sql, job)? {
        return Err(super::stale_lease());
    }
    // pin the repo to the remote's format before anything lands — a conflict
    // means the repo was already written under the other algorithm
    if let Err(e) = d.pin_obj_format(adv.format) {
        return reject_push(sql, job, &p.push, &e.message()).await;
    }
    let zero = ObjectId::null(adv.format).to_string();
    // every advertised ref becomes a command row — cap so a hostile or spammed
    // remote can't write an unbounded command list into the job payload
    if adv.refs.len() > 100_000 {
        return reject_push(sql, job, &p.push, &format!("remote advertises {} refs (cap 100k)", adv.refs.len())).await;
    }
    // same gate import_start applies to client commands — the token scope a
    // url import runs under covers every derived ref or the import refuses
    #[derive(Deserialize)]
    struct PS {
        scope: Option<String>,
    }
    let scope = exec(sql, "SELECT scope FROM pushes WHERE id=?", vec![V::from(p.push.as_str())])?
        .to_array::<PS>()?
        .into_iter()
        .next()
        .and_then(|s| s.scope);
    let mut commands = Vec::with_capacity(adv.refs.len());
    let mut wants: Vec<ObjectId> = Vec::with_capacity(adv.refs.len());
    for r in &adv.refs {
        if !scope_allows(scope.as_deref(), &r.name) {
            return reject_push(sql, job, &p.push, &format!("ref {} outside token scope", r.name)).await;
        }
        commands.push(serde_json::json!({
            "old": zero,
            "new": r.oid.to_string(),
            "name": r.name,
            "peeled": r.peeled.map(|o| o.to_string()),
        }));
        if !wants.contains(&r.oid) {
            wants.push(r.oid);
        }
    }
    let mut resp = match remote::fetch_open(url, &wants, adv.format).await {
        Ok(r) => r,
        Err(e) if deterministic(&e) => return reject_push(sql, job, &p.push, &e.message()).await,
        Err(e) => return Err(e),
    };
    let mut stream = resp.stream().map_err(Error::from)?;
    let mut rdr = PktReader::default();
    let bucket = d.bucket()?;
    let key = keys::pending_part(&bucket.repo, &PushId(p.push.clone()), "remote");
    let mut out = RawWriter::create(&bucket, key.clone(), &mut budget.req).await?;
    // hash over all-but-the-trailer: `hold` lags the hash input by one digest so
    // the trailer lands in the ring, not the digest — then digest == trailer
    // proves the whole pack arrived intact before the payload trusts it
    let ds = adv.format.len_in_bytes();
    let mut h = gix_hash::hasher(adv.format);
    let mut hold: Vec<u8> = Vec::with_capacity(64);
    let mut in_pack = false;
    let mut total = 0u64;
    let mut fatal: Option<Error> = None;
    'stream: while fatal.is_none() {
        loop {
            match rdr.next() {
                Err(e) => {
                    fatal = Some(e);
                    break 'stream;
                }
                Ok(None) => break,
                Ok(Some(Pkt::Flush)) if in_pack => break 'stream,
                Ok(Some(Pkt::Data(d))) => {
                    if !in_pack {
                        in_pack = *d == b"packfile\n"[..];
                        continue;
                    }
                    let (Some(band), data) = (d.first(), &d[1..]) else { continue };
                    match *band {
                        1 => {
                            if let Err(e) = out.append(data) {
                                fatal = Some(e);
                                break 'stream;
                            }
                            hold.extend_from_slice(data);
                            if hold.len() > ds {
                                let n = hold.len() - ds;
                                h.update(&hold[..n]);
                                hold.drain(..n);
                            }
                            total += data.len() as u64;
                            if let Err(e) = out.flush_if_full(&mut budget.req).await {
                                fatal = Some(e);
                                break 'stream;
                            }
                        }
                        2 => {}
                        _ => {
                            fatal = Some(Error::Protocol(format!(
                                "remote ERR: {}",
                                String::from_utf8_lossy(data)
                            )));
                            break 'stream;
                        }
                    }
                }
                Ok(Some(_)) => {}
            }
        }
        match stream.next().await {
            Some(Ok(chunk)) => rdr.push(&chunk),
            Some(Err(e)) => fatal = Some(Error::Internal(format!("remote stream: {e}"))),
            None => fatal = Some(Error::Protocol("remote stream ended mid-pack".into())),
        }
    }
    if let Some(e) = fatal {
        out.abort().await;
        return if deterministic(&e) { reject_push(sql, job, &p.push, &e.message()).await } else { Err(e) };
    }
    let ok = hold.len() == ds
        && matches!(h.try_finalize(), Ok(id) if id.as_bytes() == hold.as_slice());
    if !ok {
        out.abort().await;
        return reject_push(sql, job, &p.push, "remote pack trailer mismatch").await;
    }
    out.finish(&mut budget.req).await?;
    // verified pack staged — rewrite the payload into the staged shape so the
    // ordinary phases own it from here; the fetch phase never runs again
    let payload = serde_json::json!({
        "push": p.push, "pack": p.pack, "principal": p.principal,
        "parts": [{ "key": key, "bytes": total }],
        "commands": commands,
        "url": p.url,
        "head": adv.head,
        "format": if adv.format == gix_hash::Kind::Sha256 { "sha256" } else { "sha1" },
    });
    // CAS before write: a stale slice must not rewrite the payload (A19)
    if !super::heartbeat(sql, job)? {
        return Err(super::stale_lease());
    }
    exec(
        sql,
        "UPDATE jobs SET payload=? WHERE id=?",
        vec![V::from(payload.to_string().as_str()), V::from(job.id)],
    )?;
    c.phase = String::new();
    continue_(c)
}

/// upload_part/complete errors against a dead MPU. `resume_multipart_upload`
/// is lazy so a dead upload only surfaces at the first R2 call touching it —
/// mid-drain, mid-checkpoint, or mid-finish — anywhere in the slice body.
fn dead_upload(e: &Error) -> bool {
    let m = e.message();
    (m.contains("multipart upload") && m.contains("not exist")) || m.contains("NoSuchUpload")
}

/// A slice died on a spent upload: probe the pack key, then either commit the
/// already-durable object or wipe the output tables and reslice the rebuild.
async fn recover_dead_upload(
    d: &RepoDo,
    job: &Job,
    sql: &SqlStorage,
    p: &Payload,
    c: &mut Cursor,
    budget: &mut SliceBudget,
) -> Result<SliceOutcome, Error> {
    let bucket = d.bucket()?;
    let pack = PackId(p.pack.clone());
    let key = keys::pack(&bucket.repo, &pack);
    budget.req.charge(1)?;
    // complete() may have won inside the dead slice — the object is durable,
    // only commit_push never ran. Synthesize its meta from the posted spans.
    if c.phase == "commit" {
        if let Some(h) = bucket.inner.head(&key).await? {
            worker::console_log!("import {} output already landed; committing", p.push);
            let meta = meta_from_tables(sql, &p.push, &pack, h.size())?;
            return commit_meta(d, job, sql, p, c, &bucket, meta).await;
        }
    }
    worker::console_log!("import {} output upload dead — wiping output state, rebuild follows", p.push);
    // CAS before write: a stale slice must not wipe the output state (A19)
    if !super::heartbeat(sql, job)? {
        return Err(super::stale_lease());
    }
    wipe_output(sql, &p.push, &pack, c)?;
    continue_(c)
}

/// Output state refers to the dead upload — etags, byte spans, the persisted
/// tail and checkpoint all go; the TOC and push survive so resolve re-runs.
fn wipe_output(sql: &SqlStorage, push: &str, pack: &PackId, c: &mut Cursor) -> Result<(), Error> {
    for t in ["import_parts", "import_open", "import_tail"] {
        exec(sql, &format!("DELETE FROM {t} WHERE push_id=?"), vec![V::from(push)])?;
    }
    exec(sql, "DELETE FROM objects WHERE pack_id=?", vec![V::from(pack.0.as_str())])?;
    c.upload.clear();
    c.st = None;
    c.tail = 0;
    c.scan_after = -1;
    c.check_after.clear(); // a mid-check wipe must re-verify the whole link set
    c.check_epoch = None; // the next check run pins a fresh epoch
    c.recheck = false;
    c.phase = "resolve".into();
    Ok(())
}

/// PackMeta for a pack whose object is already durable: the writer is gone, so
/// count and commit span come from the per-entry spans posted into import_open.
fn meta_from_tables(sql: &SqlStorage, push: &str, pack: &PackId, bytes: u64) -> Result<PackMeta, Error> {
    #[derive(Deserialize)]
    struct N {
        n: i64,
    }
    let n = exec(
        sql,
        "SELECT COUNT(*) AS n FROM import_open WHERE push_id=? AND end IS NOT NULL",
        vec![V::from(push)],
    )?
    .one::<N>()?
    .n;
    #[derive(Deserialize)]
    struct CS {
        lo: Option<i64>,
        hi: Option<i64>,
    }
    let cs = exec(
        sql,
        "SELECT MIN(off) AS lo, MAX(end) AS hi FROM import_open \
         WHERE push_id=? AND kind=1 AND end IS NOT NULL",
        vec![V::from(push)],
    )?
    .one::<CS>()?;
    Ok(PackMeta {
        pack: pack.clone(),
        push_id: None,
        count: u32::try_from(n).unwrap_or(0),
        bytes,
        commit_lo: cs.lo.and_then(|v| u64::try_from(v).ok()).unwrap_or(u64::MAX),
        commit_hi: cs.hi.and_then(|v| u64::try_from(v).ok()).unwrap_or(0),
        created_at: platform::now_ms(),
    })
}

// ---- pass A: entry table -> import_toc ----

/// Byte pull over the staged part(s) — the same fill/buffered/consume shape
/// BodyReader gives the request body, but sourced from R2.
struct PartReader<'a> {
    bucket: &'a Bucket,
    parts: &'a [Part],
    /// absolute offset of the next byte to fetch
    fetch: u64,
    /// absolute offset of buf[0]
    base: u64,
    buf: Vec<u8>,
    eof: bool,
}
impl PartReader<'_> {
    async fn fill(&mut self, min: usize, budget: &mut ReqBudget) -> Result<bool, Error> {
        const CHUNK: u64 = 8 << 20;
        while self.buf.len() < min && !self.eof {
            let mut base = 0u64;
            let mut hit = None;
            for part in self.parts {
                let end = base.saturating_add(part.bytes);
                if self.fetch < end {
                    hit = Some((part.key.as_str(), self.fetch - base, part.bytes - (self.fetch - base)));
                    break;
                }
                base = end;
            }
            let Some((key, off, left)) = hit else {
                self.eof = true;
                break;
            };
            let n = left.min(CHUNK);
            let got = self.bucket.read_range(key, off, n, budget).await?;
            if self.buf.is_empty() {
                self.base = self.fetch;
            }
            self.fetch = self.fetch.saturating_add(got.len() as u64);
            self.buf.extend_from_slice(&got);
        }
        Ok(self.buf.len() >= min)
    }
    fn buffered(&self) -> &[u8] {
        &self.buf
    }
    fn consume(&mut self, n: usize) {
        let n = n.min(self.buf.len());
        self.buf.drain(..n);
        self.base = self.base.saturating_add(n as u64);
    }
}

/// Pass A slice: parse entry headers + zlib boundaries into import_toc rows.
/// Resume point = (parse_pos, next_idx), always an entry boundary.
async fn toc_slice(
    d: &RepoDo,
    job: &Job,
    sql: &SqlStorage,
    p: &Payload,
    c: &mut Cursor,
    budget: &mut SliceBudget,
) -> Result<SliceOutcome, Error> {
    let bucket = d.bucket()?;
    let mut r = PartReader {
        bucket: &bucket,
        parts: &p.parts,
        fetch: c.parse_pos,
        base: c.parse_pos,
        buf: Vec::new(),
        eof: false,
    };
    let (mut z, mut sinkbuf) = (Decompress::new(), vec![0u8; 64 << 10]);
    let mut rows: Vec<(u64, u8, i64, Option<i64>, Option<String>, u32, u64)> = Vec::new();
    if c.next_idx == 0 && c.parse_pos == 0 {
        if !r.fill(12, &mut budget.req).await? {
            return Err(unpack("pack header truncated"));
        }
        let head: [u8; 12] = r
            .buffered()
            .get(..12)
            .and_then(|s| s.try_into().ok())
            .ok_or_else(|| unpack("pack header"))?;
        let (version, count) = gix_pack::data::header::decode(&head).map_err(|e| unpack(e.to_string()))?;
        if version != gix_pack::data::Version::V2 {
            return Err(unpack("pack version"));
        }
        c.count = count;
        c.parse_pos = 12;
        r.consume(12);
    }
    // idx counts parsed entries; next_idx counts *durable* rows (flushed only).
    // The loop must run on idx — gating on next_idx over-parses past the trailer
    // whenever the tail batch never reaches TOC_FANOUT.
    let mut idx = c.next_idx;
    while idx < c.count {
        if budget.spent_80pct() {
            break;
        }
        let entry_off = c.parse_pos;
        r.fill(30, &mut budget.req).await?;
        let e = PackEntry::from_bytes(r.buffered(), entry_off, p.kind())
            .map_err(|e| unpack(format!("entry at {entry_off}: {e}")))?;
        // same ceilings pass A on the edge enforces
        let streamable = e.header == Header::Blob;
        if e.decompressed_size > MAX_STREAM_BLOB {
            return Err(Error::Limit("object too large (2 GiB max)".into()));
        }
        if e.decompressed_size > MAX_OBJ && !streamable {
            return Err(Error::Limit("object too large (16 MiB max)".into()));
        }
        let hlen = e.header_size();
        r.consume(hlen);
        c.parse_pos = c.parse_pos.saturating_add(hlen as u64);
        let (mut clen, mut st) = (0u64, Status::Ok);
        z.reset();
        while st != Status::StreamEnd {
            if r.buffered().is_empty() && !r.fill(1, &mut budget.req).await? {
                return Err(unpack("pack truncated inside object"));
            }
            let (bi, bo) = (z.total_in(), z.total_out());
            st = z
                .decompress(r.buffered(), &mut sinkbuf, FlushDecompress::None)
                .map_err(|e| unpack(format!("zlib at {}: {e}", c.parse_pos)))?;
            let used =
                usize::try_from(z.total_in().saturating_sub(bi)).map_err(|e| Error::Internal(e.to_string()))?;
            if used == 0 && z.total_out() == bo {
                return Err(unpack(format!("zlib stalled at {}", c.parse_pos)));
            }
            r.consume(used);
            clen = clen.saturating_add(used as u64);
            c.parse_pos = c.parse_pos.saturating_add(used as u64);
        }
        if z.total_out() != e.decompressed_size {
            return Err(unpack(format!("object at {entry_off}: size mismatch")));
        }
        if clen > MAX_ENTRY_WIRE && !streamable {
            return Err(unpack(format!("entry at {entry_off}: wire size exceeds 32 MiB")));
        }
        let (ktype, kaux_n, kaux_id) = match e.header {
            Header::Commit => (0, None, None),
            Header::Tree => (1, None, None),
            Header::Blob => (2, None, None),
            Header::Tag => (3, None, None),
            Header::OfsDelta { base_distance } => {
                (4, Some(i64::try_from(base_distance).map_err(|_| unpack("ofs distance"))?), None)
            }
            Header::RefDelta { base_id } => (5, None, Some(base_id.to_string())),
        };
        rows.push((
            entry_off,
            u8::try_from(hlen).map_err(|_| unpack("header too long"))?,
            ktype,
            kaux_n,
            kaux_id,
            u32::try_from(clen).map_err(|_| Error::Limit("entry too large".into()))?,
            e.decompressed_size,
        ));
        idx += 1;
        // CAS heartbeat after the entry's fills and before the batch lands — a
        // stale slice must not write TOC rows under a dead lease (A19)
        if !super::heartbeat(sql, job)? {
            return Err(super::stale_lease());
        }
        if rows.len() >= TOC_FANOUT {
            // rows[0].idx == next_idx: rows buffer contiguous entries in order
            toc_insert(sql, &p.push, c.next_idx, &rows)?;
            c.next_idx = idx;
            rows.clear();
        }
    }
    if !rows.is_empty() {
        toc_insert(sql, &p.push, c.next_idx, &rows)?;
        c.next_idx = idx;
    }
    if c.next_idx < c.count {
        return continue_(c);
    }
    // trailer: the staged object ends with the pack hash. Per-entry adler32 already
    // vetted the bytes; a re-hash would need a second pass over GiBs — the trailer
    // is read for completeness but not re-verified here (documented in CONTRACTS).
    if !r.fill(p.kind().len_in_bytes(), &mut budget.req).await? {
        return Err(unpack("pack trailer truncated"));
    }
    c.phase = "resolve".into();
    continue_(c)
}

/// import_toc batches of TOC_FANOUT (9 params/row < the 100-bind limit).
fn toc_insert(
    sql: &SqlStorage,
    push: &str,
    first_idx: u32,
    rows: &[(u64, u8, i64, Option<i64>, Option<String>, u32, u64)],
) -> Result<(), Error> {
    let marks = std::iter::repeat("(?,?,?,?,?,?,?,?,?)").take(rows.len()).collect::<Vec<_>>().join(",");
    let mut args = Vec::with_capacity(rows.len() * 9);
    for (k, r) in rows.iter().enumerate() {
        args.push(V::from(push));
        args.push(V::from(i64::from(first_idx).saturating_add(k as i64)));
        args.push(i64v(r.0)?);
        args.push(V::from(i64::from(r.1)));
        args.push(V::from(r.2));
        args.push(r.3.map_or(V::Null, V::from));
        args.push(r.4.clone().map_or(V::Null, V::from));
        args.push(V::from(i64::from(r.5)));
        args.push(i64v(r.6)?);
    }
    // OR REPLACE: a crashed slice replays the same idx range — re-inserts must not
    // collide, the parse is deterministic so the rows are identical anyway
    exec(
        sql,
        &format!(
            "INSERT OR REPLACE INTO import_toc(push_id,idx,offset,hlen,ktype,kaux_n,kaux_id,clen,size) VALUES{marks}"
        ),
        args,
    )?;
    Ok(())
}

// ---- pass B: resolve + normalize, sliced ----

/// The tail of one resume+drain pass. `out`/`sink`/`rows` stay alive so the caller
/// can checkpoint (resolve) or finish (commit); `done` = the queue and the parked
/// set both drained — every entry's bytes are in upload parts or the live buffer.
struct Drain<'a> {
    out: PackWriter,
    sink: JobSink<'a>,
    rows: Vec<ObjRow>,
    done: bool,
}

/// Shared resume machinery for the resolve and commit phases: bind (or create)
/// the output MPU, derive the durable boundary, restore/delete what it implies,
/// then run the TOC queue until it drains or the budget does. `done` is only set
/// when the queue is exhausted AND nothing is parked — at which point the only
/// bytes not yet durable are the live writer's <PART tail, which `finish` uploads.
#[allow(clippy::too_many_arguments)]
async fn resume_drain<'a>(
    d: &'a RepoDo,
    job: &'a Job,
    sql: &'a SqlStorage,
    p: &'a Payload,
    c: &mut Cursor,
    budget: &mut SliceBudget,
) -> Result<Drain<'a>, Error> {
    let bucket = d.bucket()?;
    let pack = PackId(p.pack.clone());
    let key = keys::pack(&bucket.repo, &pack);
    // a crash between MPU create and the first checkpoint left an upload with no
    // WriterCkpt — etags alone can't rebuild the sha state, so that upload is
    // unresumable: abort it and start over; staged input parts are unaffected.
    if !c.upload.is_empty() && c.st.is_none() {
        if let Ok(mpu) = bucket.inner.resume_multipart_upload(&key, &c.upload) {
            let _ = mpu.abort().await;
        }
        // CAS before write: a stale slice must not wipe the output rows (A19)
        if !super::heartbeat(sql, job)? {
            return Err(super::stale_lease());
        }
        for t in ["import_parts", "import_open", "import_tail"] {
            exec(sql, &format!("DELETE FROM {t} WHERE push_id=?"), vec![V::from(p.push.as_str())])?;
        }
        exec(sql, "DELETE FROM objects WHERE pack_id=?", vec![V::from(pack.0.as_str())])?;
        c.upload.clear();
        // the scan cursor and tail refer to the deleted output — start over
        c.scan_after = -1;
        c.tail = 0;
    }
    let mut out = if c.upload.is_empty() {
        let w = PackWriter::create(&bucket, &key, c.count, p.kind(), &mut budget.req).await?;
        c.upload = w.upload_id().await;
        w
    } else {
        // parts past the durable boundary are orphans from a slice that uploaded
        // then died before its cursor persisted — their bytes are deterministic-
        // identical to what the resumed writer re-appends, but the WriterCkpt's
        // sha1 doesn't cover them, so resume must not claim them
        let st = c
            .st
            .as_ref()
            .ok_or_else(|| Error::Internal("import.st without WriterCkpt".into()))?;
        let live_parts = usize::try_from(st.pos / PART as u64).map_err(|e| Error::Internal(e.to_string()))?;
        let etags: Vec<String> = part_etags(sql, &p.push)?.into_iter().take(live_parts).collect();
        PackWriter::resume(&bucket, &key, &c.upload, &etags, st, &mut budget.req).await?
    };
    crate::testhooks::failpoint_async(d, "import.drain.after_create").await?;
    // CAS before write: the create/resume awaits are behind us — the tail and
    // object-row deletes, the shadow-row re-seals and every later write of
    // this span must not land under a dead lease (A19)
    if !super::heartbeat(sql, job)? {
        return Err(super::stale_lease());
    }
    let part_tuples: Vec<(String, u64)> = p.parts.iter().map(|x| (x.key.clone(), x.bytes)).collect();
    let pack_len = part_tuples.iter().fold(0u64, |a, x| a.saturating_add(x.1));
    let src = if part_tuples.len() == 1 {
        Source::Key(part_tuples[0].0.as_str())
    } else {
        Source::Parts(&part_tuples)
    };
    let idx = Index(sql);
    let mut cx = Cx {
        win: Window { reads: crate::pack::ingest::Reader::Bucket(&bucket), src, pack_len, start: 0, buf: Vec::new() },
        cache: Cache::default(),
        z: Inflate::default(),
        by_id: IdMap::Sql { idx: &idx, pack: &pack.0, mem: HashMap::new() },
        externals: Externals::Idx(&idx),
        obj_kind: p.kind(),
        res_depth: 0,
        chain_live: 0,
        rows_held: 0,
        // the DO path keeps the generous chain bound and no heap guard —
        // its alarm slices carry their own budget
        mem: None,
    };
    let toc = Toc::Sql { sql, push: &p.push };
    #[derive(Deserialize)]
    struct One {
        n: i64,
    }
    let need_ids = exec(
        sql,
        "SELECT COUNT(*) AS n FROM import_toc WHERE push_id=? AND ktype=5",
        vec![V::from(p.push.as_str())],
    )?
    .one::<One>()?
    .n
        > 0;
    let mut sink = JobSink {
        sql,
        pack: pack.clone(),
        push: &p.push,
        job,
        d,
        links_buf: Vec::new(),
        links_total: 0,
        tags: HashMap::new(),
    };
    let mut any = AnySink::Job(&mut sink);
    let mut rows: Vec<ObjRow> = Vec::new();
    let mut pending: HashMap<ObjectId, Vec<u32>> = HashMap::new();
    let mut parked_set: HashSet<u32> = HashSet::new();
    // --- derive the durable boundary: identical for a clean continue and a crash.
    // Writers that never posted a row at-or-below the boundary (bounded by the
    // post batch): a fully-durable one is re-sealed by direct-inserting the shadow
    // row (byte-identical replay is impossible — the writer can't write out of
    // order); the single straddler — always last, later writers start past the
    // boundary — resumes mid-body at skip = durable - off.
    let durable = c.st.as_ref().map(|s| s.pos).unwrap_or(0);
    #[derive(Deserialize)]
    struct W {
        idx: i64,
        off: i64,
        sha: Option<String>,
        kind: i64,
        size: i64,
        end: Option<i64>,
        next_off: Option<i64>,
    }
    let lost: Vec<W> = exec(
        sql,
        "SELECT io.idx, io.off, io.sha, io.kind, io.size, io.end, \
         (SELECT MIN(n.off) FROM import_open n WHERE n.push_id=io.push_id AND n.off>io.off) AS next_off \
         FROM import_open io WHERE io.push_id=? AND io.off<=? \
         AND NOT EXISTS(SELECT 1 FROM objects o WHERE o.pack_id=? AND o.idx=io.idx) ORDER BY io.off",
        vec![V::from(p.push.as_str()), i64v(durable)?, V::from(pack.0.as_str())],
    )?
    .to_array::<W>()?;
    // The yield-persisted tail (import_tail) sits between `durable` and
    // `durable+c.tail`: a yield-resume restores it into the writer, so objects
    // rows inside it keep their bytes; a crash-resume finds no/short rows and
    // falls back to the bare durable bound — those entries re-run via requeue.
    // A straddler re-run appends at out.offset()==durable with skip=durable-off,
    // so discovering one forces the tail to be discarded — its bytes belong to
    // offsets the straddler re-run would overwrite.
    let straddler = lost.iter().any(|w| {
        w.end
            .and_then(|v| u64::try_from(v).ok())
            .into_iter()
            .chain(w.next_off.and_then(|v| u64::try_from(v).ok()))
            .min()
            .unwrap_or(u64::MAX)
            > durable
    });
    #[derive(Deserialize)]
    struct TB {
        #[serde(with = "serde_bytes")]
        blob: Vec<u8>,
    }
    let tail: Vec<u8> = exec(
        sql,
        "SELECT blob FROM import_tail WHERE push_id=? ORDER BY seq",
        vec![V::from(p.push.as_str())],
    )?
    .to_array::<TB>()?
    .into_iter()
    .flat_map(|r| r.blob)
    .collect();
    exec(
        sql,
        "DELETE FROM import_tail WHERE push_id=?",
        vec![V::from(p.push.as_str())],
    )?;
    let tail_len = if !straddler && c.tail > 0 && tail.len() as u64 == c.tail {
        c.tail
    } else {
        0 // tail lost/short/unsafe — entries past `durable` re-run via requeue
    };
    c.tail = 0;
    // Posted rows past the live end describe bytes that never landed —
    // delete them so the queue re-runs those entries.
    exec(
        sql,
        "DELETE FROM objects WHERE pack_id=? AND offset+len>?",
        vec![V::from(pack.0.as_str()), i64v(durable.saturating_add(tail_len))?],
    )?;
    let mut done_set: HashSet<u32> = HashSet::new();
    for w in lost {
        let (woff, wend, wnext) = (
            u64::try_from(w.off).map_err(|e| Error::Internal(e.to_string()))?,
            w.end.and_then(|e| u64::try_from(e).ok()),
            w.next_off.and_then(|e| u64::try_from(e).ok()),
        );
        let entry_end = wend.into_iter().chain(wnext).min().unwrap_or(u64::MAX);
        if entry_end <= durable {
            // CAS before write: an awaited straddler re-run in an earlier
            // iteration split the span — a stale slice must not land these
            // shadow rows under a dead lease (A19)
            if !super::heartbeat(sql, job)? {
                return Err(super::stale_lease());
            }
            // fully durable, row lost: post the shadow row — the bytes are already
            // uploaded, only the index entry is missing
            let sha = w
                .sha
                .as_deref()
                .ok_or_else(|| Error::Internal(format!("lost writer {} unsealed", w.idx)))?;
            exec(
                sql,
                "INSERT INTO objects(sha,pack_id,idx,offset,len,kind,size) VALUES(?,?,?,?,?,?,?) \
                 ON CONFLICT(sha,pack_id) DO NOTHING",
                vec![
                    V::from(sha),
                    V::from(pack.0.as_str()),
                    V::from(w.idx),
                    i64v(woff)?,
                    i64v(entry_end.saturating_sub(woff))?,
                    V::from(w.kind),
                    V::from(w.size),
                ],
            )?;
            exec(
                sql,
                "UPDATE import_open SET end=? WHERE push_id=? AND idx=?",
                vec![i64v(entry_end)?, V::from(p.push.as_str()), V::from(w.idx)],
            )?;
            continue;
        }
        // straddler: its base necessarily resolved before it wrote (bases precede
        // bodies in output order) — a deferral here can't happen, fail loud if it does
        let frag = durable.saturating_sub(woff);
        if frag > 0 {
            match one_entry(
                &mut cx,
                &toc,
                w.idx as usize,
                need_ids,
                &mut out,
                &mut any,
                &mut rows,
                &mut budget.req,
                frag,
            )
            .await?
            {
                Step::Done(id) => {
                    done_set.insert(w.idx as u32);
                    run_wakes(&mut cx, &toc, need_ids, &mut out, &mut any, &mut rows, budget, &mut pending, &mut parked_set, &mut done_set, id)
                        .await?
                }
                Step::Await(b) => {
                    return Err(Error::Internal(format!("boundary entry {woff} deferred on {b}")))
                }
            }
        }
    }
    if tail_len > 0 {
        // restore the yield-persisted <PART tail now that any boundary re-runs are
        // done: entries that completed inside it posted their rows at the yield —
        // count them so the writer's totals and commit span continue exactly
        #[derive(Deserialize)]
        struct TN {
            n: i64,
        }
        // completions inside the tail region: `end` in (durable, durable+tail] —
        // `end>durable` not `off>durable`, or a sealed straddler (started below
        // the boundary, finished in the tail) is counted nowhere and the
        // writer's total comes up one short at finish
        let n = exec(
            sql,
            "SELECT COUNT(*) AS n FROM import_open WHERE push_id=? AND end>? AND end<=?",
            vec![
                V::from(p.push.as_str()),
                i64v(durable)?,
                i64v(durable.saturating_add(tail_len))?,
            ],
        )?
        .one::<TN>()?
        .n;
        #[derive(Deserialize)]
        struct CS {
            lo: Option<i64>,
            hi: Option<i64>,
        }
        let cs = exec(
            sql,
            "SELECT MIN(off) AS lo, MAX(end) AS hi FROM import_open \
             WHERE push_id=? AND end>? AND end<=? AND kind=1",
            vec![
                V::from(p.push.as_str()),
                i64v(durable)?,
                i64v(durable.saturating_add(tail_len))?,
            ],
        )?
        .one::<CS>()?;
        out.resume_tail(
            &tail,
            u32::try_from(n).unwrap_or(0),
            cs.lo.and_then(|v| u64::try_from(v).ok()).unwrap_or(u64::MAX),
            cs.hi.and_then(|v| u64::try_from(v).ok()).unwrap_or(0),
        )?;
    }
    // re-attempt parked entries rather than trusting the cursor's base ids: a base
    // may have resolved since the cursor saved (crash slices lose wake bookkeeping)
    let parked_list = std::mem::take(&mut c.parked);
    let mut parked_iter = parked_list.into_iter().peekable();
    while let Some(pk) = parked_iter.next() {
        if budget.spent_80pct() {
            // re-park this and every un-attempted entry — the take already
            // emptied the persisted list, so dropping them here would lose them
            if let Ok(b) = crate::repo_do::oid(&pk.base) {
                pending.entry(b).or_default().push(pk.i);
            }
            parked_set.insert(pk.i);
            for pk in parked_iter.by_ref() {
                if let Ok(b) = crate::repo_do::oid(&pk.base) {
                    pending.entry(b).or_default().push(pk.i);
                }
                parked_set.insert(pk.i);
            }
            break;
        }
        match one_entry(&mut cx, &toc, pk.i as usize, need_ids, &mut out, &mut any, &mut rows, &mut budget.req, 0)
            .await?
        {
            Step::Done(id) => {
                done_set.insert(pk.i);
                run_wakes(&mut cx, &toc, need_ids, &mut out, &mut any, &mut rows, budget, &mut pending, &mut parked_set, &mut done_set, id)
                    .await?
            }
            Step::Await(b) => {
                pending.entry(b).or_default().push(pk.i);
                parked_set.insert(pk.i);
            }
        }
    }
    // TOC idx is 0-based; -1 = from the start. Entries at or below it are done,
    // parked, or handled by the requeue pass — the scan is monotonic.
    let mut scan_after = c.scan_after;
    // Undone entries at or below the persisted scan cursor re-run here — the
    // queue scan below is forward-only and never revisits them. Undone means:
    // no objects row (resume-time DELETE + never-posted), and no import_open
    // row sealed at-or-inside the live end. This catches marked-then-lost
    // spans AND never-marked entries — one that parked only in a dead slice's
    // memory, or the boundary entry a yield consumed without processing —
    // both invisible to an import_open-based probe.
    {
        #[derive(Deserialize)]
        struct Rq {
            idx: i64,
        }
        let requeue: Vec<i64> = exec(
            sql,
            "SELECT t.idx AS idx FROM import_toc t WHERE t.push_id=? AND t.idx<=? \
             AND NOT EXISTS(SELECT 1 FROM objects o WHERE o.pack_id=? AND o.idx=t.idx) \
             AND NOT EXISTS(SELECT 1 FROM import_open io WHERE io.push_id=? AND io.idx=t.idx \
                            AND io.end IS NOT NULL AND io.end<=?) \
             ORDER BY t.idx",
            vec![
                V::from(p.push.as_str()),
                V::from(scan_after),
                V::from(pack.0.as_str()),
                V::from(p.push.as_str()),
                i64v(durable.saturating_add(tail_len))?,
            ],
        )?
        .to_array::<Rq>()?
        .into_iter()
        .map(|r| r.idx)
        .collect();
        for i in requeue {
            if budget.spent_80pct() {
                break;
            }
            let Ok(i) = u32::try_from(i) else { continue };
            if parked_set.contains(&i) || done_set.contains(&i) {
                continue;
            }
            match one_entry(&mut cx, &toc, i as usize, need_ids, &mut out, &mut any, &mut rows, &mut budget.req, 0)
                .await?
            {
                Step::Done(id) => {
                    done_set.insert(i);
                    run_wakes(&mut cx, &toc, need_ids, &mut out, &mut any, &mut rows, budget, &mut pending, &mut parked_set, &mut done_set, id).await?;
                }
                Step::Await(base) => {
                    pending.entry(base).or_default().push(i);
                    parked_set.insert(i);
                }
            }
            if rows.len() >= 10_000 {
                // JobSink::post CASes the lease itself before writing (A19)
                any.post(&rows, &mut budget.req).await?;
                rows.clear();
            }
        }
    }
    // Enumerate the TOC idx-ordered, resuming at the persisted scan cursor —
    // a slice never re-walks the done prefix (that rescan alone could eat a
    // whole slice's budget near the tail — the freeze this cursor prevents).
    let mut done = false;
    'queue: loop {
        if budget.spent_80pct() {
            break;
        }
        #[derive(Deserialize)]
        struct Q {
            idx: i64,
        }
        let page: Vec<i64> = exec(
            sql,
            "SELECT t.idx AS idx FROM import_toc t WHERE t.push_id=? AND t.idx>? \
             AND NOT EXISTS(SELECT 1 FROM import_open io WHERE io.push_id=? AND io.idx=t.idx \
                            AND (io.end<=? OR EXISTS(SELECT 1 FROM objects o WHERE o.pack_id=? AND o.idx=io.idx))) \
             ORDER BY t.idx LIMIT ?",
            vec![
                V::from(p.push.as_str()),
                V::from(scan_after),
                V::from(p.push.as_str()),
                i64v(durable)?,
                V::from(pack.0.as_str()),
                V::from(QUEUE_BATCH),
            ],
        )?
        .to_array::<Q>()?
        .into_iter()
        .map(|q| q.idx)
        .collect();
        let exhausted = page.len() < QUEUE_BATCH as usize;
        for i in page {
            // scan_after advances only once the entry is durably tracked —
            // processed, or known-parked/done. Consuming it before the budget
            // check would lose the boundary entry at every yield.
            let Ok(i) = u32::try_from(i) else { scan_after = i; continue };
            if parked_set.contains(&i) || done_set.contains(&i) {
                scan_after = i64::from(i);
                continue;
            }
            if budget.spent_80pct() {
                break 'queue;
            }
            let i64_i = i64::from(i);
            match one_entry(&mut cx, &toc, i as usize, need_ids, &mut out, &mut any, &mut rows, &mut budget.req, 0)
                .await?
            {
                Step::Done(id) => {
                    done_set.insert(i);
                    run_wakes(&mut cx, &toc, need_ids, &mut out, &mut any, &mut rows, budget, &mut pending, &mut parked_set, &mut done_set, id).await?;
                }
                Step::Await(base) => {
                    pending.entry(base).or_default().push(i);
                    parked_set.insert(i);
                }
            }
            scan_after = i64_i;
            if rows.len() >= 10_000 {
                // JobSink::post CASes the lease itself before writing (A19)
                any.post(&rows, &mut budget.req).await?;
                rows.clear();
            }
        }
        if exhausted {
            if pending.is_empty() {
                done = true;
            } else {
                let base = pending.keys().next().copied().expect("pending non-empty");
                return Err(unpack(format!("missing base {base}")));
            }
            break;
        }
        if !super::heartbeat(sql, job)? {
            return Err(super::stale_lease());
        }
    }
    // parked state persists across slices of either phase — a base that resolves
    // in a later slice wakes them without re-scanning the whole TOC
    c.parked = pending
        .iter()
        .flat_map(|(b, ws)| ws.iter().map(move |i| Parked { i: *i, base: b.to_string() }))
        .collect();
    c.scan_after = scan_after;
    Ok(Drain { out, sink, rows, done })
}

/// Resolve phase: drain as much as the slice budget allows, checkpoint, continue.
/// The phase only advances when the queue AND the parked set are empty — the
/// <PART tail still undurable at that point re-runs inside the commit slice.
async fn resolve_slice(
    d: &RepoDo,
    job: &Job,
    sql: &SqlStorage,
    p: &Payload,
    c: &mut Cursor,
    budget: &mut SliceBudget,
) -> Result<SliceOutcome, Error> {
    let mut dr = resume_drain(d, job, sql, p, c, budget).await?;
    if dr.done {
        c.phase = "check".into();
    }
    flush_ckpt(sql, job, &mut dr.out, &p.push, &mut dr.sink, &mut dr.rows, c, budget).await?;
    continue_(c)
}

/// Run the entries whose awaited base just resolved — transitively, since a woken
/// entry can itself be another waiter's base.
#[allow(clippy::too_many_arguments)]
async fn run_wakes(
    cx: &mut Cx<'_>,
    toc: &Toc<'_>,
    need_ids: bool,
    out: &mut PackWriter,
    any: &mut AnySink<'_, '_>,
    rows: &mut Vec<ObjRow>,
    budget: &mut SliceBudget,
    pending: &mut HashMap<ObjectId, Vec<u32>>,
    parked_set: &mut HashSet<u32>,
    done_set: &mut HashSet<u32>,
    id: ObjectId,
) -> Result<(), Error> {
    let mut stack: Vec<u32> = pending.remove(&id).unwrap_or_default();
    while let Some(j) = stack.pop() {
        parked_set.remove(&j);
        match one_entry(cx, toc, j as usize, need_ids, out, any, rows, &mut budget.req, 0).await? {
            Step::Done(id2) => {
                done_set.insert(j);
                stack.extend(pending.remove(&id2).unwrap_or_default());
            }
            Step::Await(b) => {
                pending.entry(b).or_default().push(j);
                parked_set.insert(j);
            }
        }
    }
    Ok(())
}

fn part_etags(sql: &SqlStorage, push: &str) -> Result<Vec<String>, Error> {
    #[derive(Deserialize)]
    struct E {
        etag: String,
    }
    Ok(exec(
        sql,
        "SELECT etag FROM import_parts WHERE push_id=? ORDER BY part_no",
        vec![V::from(push)],
    )?
    .to_array::<E>()?
    .into_iter()
    .map(|e| e.etag)
    .collect())
}

/// Drain full parts, sync import_parts, then make the durable prefix the cursor:
/// post rows whose entries are fully durable, delete objects rows past it, and
/// name the boundary entry (frag_i/frag) for the mid-body resume.
#[allow(clippy::too_many_arguments)]
async fn flush_ckpt(
    sql: &SqlStorage,
    job: &Job,
    out: &mut PackWriter,
    push: &str,
    sink: &mut JobSink<'_>,
    rows: &mut Vec<ObjRow>,
    c: &mut Cursor,
    budget: &mut SliceBudget,
) -> Result<(), Error> {
    // collect first, write after the CAS — the upload awaits must not be
    // followed by writes a stale slice could land under the replacement lease
    let mut news = Vec::new();
    while out.buffered() >= PART as u64 {
        match out.checkpoint(&mut budget.req).await? {
            Some((n, e, s)) => news.push((n, e, s)),
            None => break,
        }
    }
    // CAS before write (A19)
    if !super::heartbeat(sql, job)? {
        return Err(super::stale_lease());
    }
    for (n, e, s) in news {
        exec(
            sql,
            "INSERT OR REPLACE INTO import_parts(push_id,part_no,etag) VALUES(?,?,?)",
            vec![V::from(push), V::from(i64::from(n)), V::from(e)],
        )?;
        c.st = Some(s);
    }
    // flush_if_full inside one_entry uploads too — fold every uploaded part into
    // the table so resume's etag list is always complete, and drop rows for parts
    // a dead slice uploaded past the boundary the cursor actually saved
    for (k, e) in out.etags().iter().enumerate() {
        exec(
            sql,
            "INSERT OR REPLACE INTO import_parts(push_id,part_no,etag) VALUES(?,?,?)",
            vec![
                V::from(push),
                V::from(i64::try_from(k + 1).map_err(|_| Error::Internal("part".into()))?),
                V::from(e.clone()),
            ],
        )?;
    }
    exec(
        sql,
        "DELETE FROM import_parts WHERE push_id=? AND part_no>?",
        vec![
            V::from(push),
            V::from(i64::try_from(out.etags().len()).map_err(|_| Error::Internal("part".into()))?),
        ],
    )?;
    // the live snapshot IS the durable state here: buffered<PART post-drain, so
    // pos = uploaded prefix and sha covers exactly it — including parts
    // flush_if_full uploaded mid-slice, which c.st (checkpoint-only) misses
    let mut st = out.snapshot();
    let durable = st.pos;
    // every completed row posts — the ingesting pack is invisible to live lookups.
    // Rows ahead of the durable prefix are NOT deleted here: their bytes sit in
    // the live writer's buffer this slice still owns, and dropping them would make
    // the next slice's done-check re-run a completed entry (double-append). The
    // resume-time DELETE >durable at slice start is where crash correction lives —
    // by then the undurable tail is genuinely gone (never uploaded).
    if !rows.is_empty() {
        // JobSink::post fences the lease itself before writing (A19); its
        // body never awaits, so this span is unbroken — no CAS needed here
        sink.post(rows, &mut budget.req).await?;
        rows.clear();
    }
    #[derive(Deserialize)]
    struct N2 {
        n: i64,
    }
    // appended-and-sealed entries inside the boundary — the writer's replay count;
    // objects rows undercount (unposted + dup-sha rows are missing by design)
    st.count = exec(
        sql,
        "SELECT COUNT(*) AS n FROM import_open WHERE push_id=? AND end<=?",
        vec![V::from(push), i64v(durable)?],
    )?
    .one::<N2>()?
    .n as u32;
    sink.flush_links()?;
    c.st = Some(st);
    // A32 for the import side: the undrained <PART tail must survive the slice
    // or a sub-part slice's appends die with the isolate — a requeue set just
    // under PART would re-run forever without ever draining
    exec(sql, "DELETE FROM import_tail WHERE push_id=?", vec![V::from(push)])?;
    for (i, chunk) in out.buffered_bytes().chunks(TAIL_CHUNK).enumerate() {
        exec(
            sql,
            "INSERT INTO import_tail(push_id,seq,blob) VALUES(?,?,?)",
            vec![
                V::from(push),
                V::from(i64::try_from(i).map_err(|_| Error::Internal("tail seq".into()))?),
                V::from(chunk.to_vec()),
            ],
        )?;
    }
    c.tail = out.buffered();
    Ok(())
}

// ---- 2.5 connectivity, paged ----

async fn check_slice(
    d: &RepoDo,
    job: &Job,
    sql: &SqlStorage,
    p: &Payload,
    c: &mut Cursor,
    budget: &mut SliceBudget,
) -> Result<SliceOutcome, Error> {
    let idx = Index(sql);
    let pack = PackId(p.pack.clone());
    // C1: pin the epoch this check run verifies under — commit_meta re-runs
    // the whole phase when the live epoch has moved since (a sweep ran). A
    // legacy cursor mid-check restarts from the front first: its earlier
    // pages predate any epoch it could pin.
    if check_needs_restart(c.check_epoch, &c.check_after) {
        c.check_after.clear();
    }
    if c.check_epoch.is_none() {
        c.check_epoch = Some(d.meta_i64("gc_epoch")?);
    }
    loop {
        if budget.spent_80pct() {
            return continue_(c);
        }
        #[derive(Deserialize)]
        struct S {
            sha: String,
        }
        let page: Vec<String> = exec(
            sql,
            "SELECT sha FROM push_links WHERE push_id=? AND sha>? ORDER BY sha LIMIT ?",
            vec![V::from(p.push.as_str()), V::from(c.check_after.as_str()), V::from(CHECK_PAGE)],
        )?
        .to_array::<S>()?
        .into_iter()
        .map(|s| s.sha)
        .collect();
        if page.is_empty() {
            // claim-only point: the slice never awaits after this — a real
            // death here is indistinguishable from dying before it ran.
            // ONE divergence: run_slice's `UPDATE pushes SET began_at=?`
            // lease refresh lands before the failpoint, so the simulated
            // crash keeps it where a real death would roll it back
            crate::testhooks::failpoint(d, "import.check.done")?;
            c.phase = "commit".into();
            return continue_(c);
        }
        let ids: Vec<ObjectId> =
            page.iter().map(|h| crate::repo_do::oid(h)).collect::<Result<_, Error>>()?;
        let locs = idx.lookup(&ids)?;
        for (id, loc) in ids.iter().zip(locs.iter()) {
            // links - {this pack} must be live; this pack's own rows count (2.5).
            // A missing link rejects only once a GC actually ran — either this
            // is the post-sweep re-check (recheck) or the epoch moved mid-run —
            // because then "missing" means swept, never pending inside another
            // still-open push. Before that it may yet arrive: keep the old
            // retry (C1).
            if loc.is_none() && idx.lookup_in_pack(id, &pack)?.is_none() {
                if !c.recheck && c.check_epoch == Some(d.meta_i64("gc_epoch")?) {
                    return Err(unpack(format!("missing object {id}")));
                }
                // CAS before the abort's await: the c.upload MPU may belong to
                // a NEW lease-owner's slice by the time abort lands — a stale
                // slice must fail the lease check and touch nothing (A19).
                // reject_push CASes again after the await.
                if !super::heartbeat(sql, job)? {
                    return Err(super::stale_lease());
                }
                // reject_push retires the job Done and the janitor only reaps
                // uploads of *dead* jobs — abort the output MPU first or it
                // leaks until R2's own expiry. A transient abort failure
                // propagates instead: retiring the job would delete the
                // cursor that holds the upload id and strand it for good;
                // an already-absent upload is what abort wanted anyway.
                if !c.upload.is_empty() {
                    let bucket = d.bucket()?;
                    let key = keys::pack(&bucket.repo, &pack);
                    if let Ok(mpu) = bucket.inner.resume_multipart_upload(&key, &c.upload) {
                        if let Err(e) = mpu.abort().await.map_err(Error::from) {
                            if !dead_upload(&e) {
                                return Err(e);
                            }
                        }
                    }
                }
                return reject_push(sql, job, &p.push, &format!("missing object {id}")).await;
            }
        }
        c.check_after = page.last().cloned().unwrap_or_default();
        if !super::heartbeat(sql, job)? {
            return Err(super::stale_lease());
        }
    }
}

// ---- commit: drain the undurable tail, finish the pack, commit the push ----

/// The <PART tail a resolve slice can never make durable re-runs here through the
/// same resume machinery — a commit slice that doesn't drain inside its budget
/// checkpoints and retries, so the tail advances incrementally like any other.
async fn commit_slice(
    d: &RepoDo,
    job: &Job,
    sql: &SqlStorage,
    p: &Payload,
    c: &mut Cursor,
    budget: &mut SliceBudget,
) -> Result<SliceOutcome, Error> {
    // C1: a sweep since the check pinned its epoch makes the link verdicts
    // stale — reset to the check phase BEFORE draining/finishing the upload
    // (the re-run used to finish first and had to recover a spent MPU).
    // commit_meta keeps the same-span compare as the final guard.
    if c.check_epoch != Some(d.meta_i64("gc_epoch")?) {
        reset_to_check(c);
        return continue_(c);
    }
    let mut dr = resume_drain(d, job, sql, p, c, budget).await?;
    flush_ckpt(sql, job, &mut dr.out, &p.push, &mut dr.sink, &mut dr.rows, c, budget).await?;
    if !dr.done {
        return continue_(c);
    }
    let bucket = d.bucket()?;
    // finish_resumable: a mid-finish failure keeps the MPU alive so the retry
    // resumes and re-finishes — aborting here would force a full re-emit
    let meta = dr.out.finish_resumable(&mut budget.req).await?;
    // pause probe for the post-finish C1 arm: a sweep landing HERE — after
    // finish consumed the MPU, before commit_meta's epoch compare — must
    // reset the job to check; the re-commit's finish then fails dead-upload
    // and recover_dead_upload's HEAD arm commits the durable object
    crate::testhooks::failpoint_async(d, "import.commit.after_finish").await?;
    commit_meta(d, job, sql, p, c, &bucket, meta).await
}

/// Terminal commit span: post the real pack meta, run commit_push, and drop
/// every trace of staging. Shared by commit_slice and the already-durable
/// recovery in recover_dead_upload.
async fn commit_meta(
    d: &RepoDo,
    job: &Job,
    sql: &SqlStorage,
    p: &Payload,
    c: &mut Cursor,
    bucket: &Bucket,
    meta: PackMeta,
) -> Result<SliceOutcome, Error> {
    crate::testhooks::failpoint(d, "import.commit.before")?;
    // C1: the check phase's connectivity verdicts are only valid while no GC
    // sweep has run — gc_epoch bumps inside the sweep's span. A moved epoch
    // means links verified live may be dead now: reset to the start of the
    // check phase and re-verify against the post-GC index instead of
    // committing a possibly-dangling ref. The gate and the ref update are in
    // the same synchronous span, so nothing can interleave between them.
    let epoch = d.meta_i64("gc_epoch")?;
    if c.check_epoch != Some(epoch) {
        reset_to_check(c);
        return continue_(c);
    }
    // CAS before the commit span's writes — a stale slice must not land pack
    // meta, ref updates or staging cleanup under a dead lease (A19)
    if !super::heartbeat(sql, job)? {
        return Err(super::stale_lease());
    }
    let pack = PackId(p.pack.clone());
    // post real pack meta over the 'ingesting' placeholder (push_index parity)
    exec(
        sql,
        "UPDATE packs SET count=?, bytes=?, commit_lo=?, commit_hi=? WHERE id=? AND state='ingesting'",
        vec![
            V::from(i64::from(meta.count)),
            i64v(meta.bytes)?,
            i64v(meta.commit_lo)?,
            i64v(meta.commit_hi)?,
            V::from(pack.0.as_str()),
        ],
    )?;
    // the epoch guard exists to catch a commit whose connectivity verdicts
    // predate a sweep — advancing the captured epoch is honest only because
    // the gate above proved the check ran inside this very epoch
    exec(
        sql,
        "UPDATE pushes SET gc_epoch=? WHERE id=?",
        vec![V::from(epoch), V::from(p.push.as_str())],
    )?;
    let r = d.commit_push(&CommitRequest {
        push_id: p.push.clone(),
        pack_id: Some(pack.0.clone()),
        principal: p.principal.clone(),
        commands: p.commands.clone(),
        atomic: true,
    });
    // Whatever commit_push decided is terminal — the pushes row records the
    // truth (committed, or rejected + per-ref reasons import_status surfaces).
    // A spent-MPU retry is not a dead end but the designed path: a post-finish
    // epoch reset re-commits through recover_dead_upload's HEAD arm instead.
    match &r {
        Ok(resp) => {
            if let Some((name, err)) = resp.results.iter().find(|(_, e)| e.is_some()) {
                worker::console_log!("import {} commit rejected {name}: {}", p.push, err.as_deref().unwrap_or(""));
            }
        }
        Err(e) => worker::console_log!("import {} commit error: {e}", p.push),
    }
    // a url import adopts the remote's HEAD symref when that ref landed —
    // clones then default to the same branch the source advertises
    if let (Ok(_), Some(head)) = (&r, &p.head) {
        #[derive(Deserialize)]
        struct N {
            n: i64,
        }
        let exists = exec(sql, "SELECT COUNT(*) AS n FROM refs WHERE name=?", vec![V::from(head.as_str())])?
            .one::<N>()?
            .n
            > 0;
        if exists {
            exec(sql, "UPDATE meta SET value=? WHERE key='head'", vec![V::from(head.as_str())])?;
        }
    }
    // staging tables + the staged pack itself are garbage now
    for t in ["push_links", "import_toc", "import_parts", "import_open", "import_tail"] {
        exec(sql, &format!("DELETE FROM {t} WHERE push_id=?"), vec![V::from(p.push.as_str())])?;
    }
    for part in &p.parts {
        let _ = bucket.inner.delete(part.key.as_str()).await;
    }
    r.map(|_| SliceOutcome::Done)
}

#[cfg(test)]
mod tests {
    use super::{check_needs_restart, dead_upload, Cursor};
    use crate::error::Error;

    /// The legacy-cursor decision: only a mid-check cursor with no pinned
    /// epoch restarts. A fresh check ("") pins straight away; a pinned one
    /// resumes its pages — clearing either would rescan for no reason.
    #[test]
    fn check_needs_restart_truth_table() {
        assert!(check_needs_restart(None, "abc")); // legacy mid-check
        assert!(!check_needs_restart(None, "")); // fresh: nothing walked yet
        assert!(!check_needs_restart(Some(7), "abc")); // pinned: resume pages
        assert!(!check_needs_restart(Some(7), ""));
    }

    /// A cursor persisted before scan_after/tail existed must still load —
    /// serde defaults or a live import's saved state fails to deserialize and
    /// the job restarts from zero (or dies).
    #[test]
    fn cursor_backward_compat() {
        let old = r#"{"phase":"resolve","parse_pos":100,"next_idx":50,"count":462299,
                     "parked":[{"i":7,"base":"c2a8424708547d87ce798c608beed5cd37c745da"}],
                     "upload":"u","st":null,"check_after":""}"#;
        let c: Cursor = serde_json::from_str(old).unwrap();
        assert_eq!(c.scan_after, -1, "missing scan_after must mean full rescan");
        assert_eq!(c.tail, 0, "missing tail must mean no persisted tail");
        assert_eq!(c.phase, "resolve");
        assert_eq!(c.parked.len(), 1);
        // and a cursor with the fields round-trips
        let new = r#"{"phase":"resolve","parse_pos":0,"next_idx":0,"count":0,"parked":[],
                      "upload":"","st":null,"check_after":"","scan_after":337551,"tail":6427865}"#;
        let c: Cursor = serde_json::from_str(new).unwrap();
        assert_eq!(c.scan_after, 337551);
        assert_eq!(c.tail, 6427865);
    }

    /// dead_upload gates the wipe-and-rebuild recovery — too narrow and a dead
    /// MPU retries to dead-letter; too broad and an unrelated "not found"
    /// wipes hours of valid output state.
    #[test]
    fn dead_upload_matching() {
        assert!(dead_upload(&Error::Storage(
            "Error: uploadPart: The specified multipart upload does not exist.".into()
        )));
        assert!(dead_upload(&Error::Storage("NoSuchUpload: The specified upload does not exist.".into())));
        // a missing source object / key is not a dead upload — no rebuild
        assert!(!dead_upload(&Error::Storage("Error: get: object does not exist".into())));
        assert!(!dead_upload(&Error::Unpack("missing base aa".into())));
        assert!(!dead_upload(&Error::Budget));
    }
    /// The "treat as temporary" rule: the memory-pressure Limit retries the
    /// slice; every other Protocol/Limit still rejects the push for good.
    #[test]
    fn deterministic_leaves_the_retry_limit_to_the_slice() {
        assert!(!super::deterministic(&Error::Limit(crate::try_grow::MEM_RETRY_MSG.into())), "the retry Limit is transient");
        assert!(super::deterministic(&Error::Limit("pack too big".into())), "a real Limit still rejects");
        assert!(super::deterministic(&Error::Protocol("bad ref".into())), "protocol errors still reject");
        assert!(!super::deterministic(&Error::Storage("r2 blip".into())), "transport errors were never deterministic");
    }
}
