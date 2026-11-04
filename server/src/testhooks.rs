//! PR0 deterministic test hooks — inert unless GE_TEST_HOOKS=1.
//!
//! Edge routes `/:o/:r/_admin/test/*` (global write token, see edge/mod.rs)
//! forward to the `/_do/test/*` handlers here. With the flag off every route
//! answers NotFound exactly like an unknown route, `failpoint` is a no-op that
//! runs no SQL, `test_failpoints` is never created, and `alarm` dispatches
//! normally (manual mode is off).
//!
//!   POST /_do/test/step        {"kind"?: "gc_mark"|...} — run EXACTLY ONE job
//!                              slice through the shared dispatcher (run_at
//!                              ignored, optional kind filter)
//!   POST /_do/test/jobs        {} — jobs + gc.* meta + packs view
//!   POST /_do/test/failpoint   {"name","action":"error"|"crash"|"off","count":n}
//!   POST /_do/test/repair      {} — age 'running' rows 61 s + jobs::repair
//!   POST /_do/test/fsck        {"mode":"refs"|"all"} — index+R2 integrity oracle
//!   POST /_do/test/drop_object {"sha","action"?:"drop"|"corrupt"} — index corruption jig
//!   POST /_do/test/drop_pack   {"pack"} — delete the pack's R2 object (fsck must notice)
//!   POST /_do/test/knob        {"name","value"?} — integer knobs (slice_subreqs, gc_part_bytes)

use std::collections::{HashMap, HashSet, VecDeque};

use gix_hash::{self, ObjectId};
use gix_object::Kind;
use worker::{Env, Response, SqlStorageValue as V};

use crate::error::Error;
use crate::jobs::{self, JobKind};
use crate::pack::ingest::extract_links;
use crate::repo_do::RepoDo;
use crate::store::{codec, keys, ObjLoc, ObjSqlRow, PackId, RepoId};
use crate::wire::http::{json, parse};
use crate::ReqBudget;

/// The flag parse itself: only exactly "1" is on. Pure — unit-tested below.
fn flag_on(v: Option<&str>) -> bool {
    v == Some("1")
}

/// The single env-flag reader. pub(crate): the edge's test_forward uses it too.
pub(crate) fn env_flag(env: &Env, name: &str) -> bool {
    flag_on(env.var(name).ok().map(|v| v.to_string()).as_deref())
}

/// The one gate: GE_TEST_HOOKS=1 — absent or anything but "1" = off.
pub fn enabled(d: &RepoDo) -> bool {
    env_flag(&d.env, "GE_TEST_HOOKS")
}

/// GE_TEST_MANUAL_JOBS=1 (plus the hooks flag): alarm() never dispatches —
/// slices run only via /_do/test/step.
pub fn manual_jobs(d: &RepoDo) -> bool {
    enabled(d) && env_flag(&d.env, "GE_TEST_MANUAL_JOBS")
}

/// Lazily created here and in set_failpoint — deliberately NOT in the
/// production schema list (store::schema::DDL).
const DDL: &str =
    "CREATE TABLE IF NOT EXISTS test_failpoints(name TEXT PRIMARY KEY, action TEXT NOT NULL, remaining INTEGER NOT NULL)";

/// Named failpoints — one decision in one place; gc.rs and import.rs each
/// carry a one-line `testhooks::failpoint(d, "...")?;` call. `remaining` is
/// consumed before the action fires; a row at 0 is inert. Flag off = a no-op
/// that runs no SQL at all.
pub fn failpoint(d: &RepoDo, name: &str) -> Result<(), Error> {
    if !enabled(d) {
        return Ok(());
    }
    d.q(DDL, vec![])?;
    d.q(
        "UPDATE test_failpoints SET remaining=remaining-1 WHERE name=? AND remaining>0",
        vec![V::from(name)],
    )?;
    if d.changes()? != 1 {
        return Ok(());
    }
    #[derive(serde::Deserialize)]
    struct A {
        action: String,
    }
    let action = d
        .q("SELECT action FROM test_failpoints WHERE name=?", vec![V::from(name)])?
        .to_array::<A>()?
        .into_iter()
        .next()
        .ok_or_else(|| Error::Internal("failpoint row vanished".into()))?
        .action;
    match action.as_str() {
        "crash" => Err(crash(name)),
        _ => Err(Error::Internal(format!("failpoint {name}"))),
    }
}

const CRASH_MARK: &str = "failpoint-crash:";

/// A "crash" failpoint's error — dispatch_inner recognises it and returns
/// WITHOUT the outcome span: the job row stays 'running' holding its lease,
/// exactly what a killed isolate leaves behind (and every write the slice
/// made before the failpoint persists, true of a real crash between sync
/// spans).
fn crash(name: &str) -> Error {
    Error::Internal(format!("{CRASH_MARK}{name}"))
}
pub fn is_crash(e: &Error) -> bool {
    matches!(e, Error::Internal(m) if m.starts_with(CRASH_MARK))
}

// ---- jobs/gc_meta/packs view (embedded in the step response too) ----

#[derive(serde::Deserialize)]
struct JobRow {
    id: i64,
    kind: String,
    state: String,
    run_at: i64,
    attempts: i64,
    strands: i64,
    last_error: Option<String>,
    cursor_present: i64,
}
#[derive(serde::Deserialize)]
struct KV {
    key: String,
    value: String,
}
#[derive(serde::Deserialize)]
struct PackRow {
    id: String,
    state: String,
    count: i64,
    created_at: i64,
    push_id_null: i64,
}

fn view(d: &RepoDo) -> Result<serde_json::Value, Error> {
    let jobs = d
        .q(
            "SELECT id,kind,state,run_at,attempts,strands,last_error, \
             (cursor IS NOT NULL) AS cursor_present FROM jobs ORDER BY id",
            vec![],
        )?
        .to_array::<JobRow>()?;
    let gc_meta: serde_json::Map<String, serde_json::Value> = d
        .q(
            "SELECT key,value FROM meta WHERE key LIKE 'gc.%' OR key IN ('gc_epoch','refs_version')",
            vec![],
        )?
        .to_array::<KV>()?
        .into_iter()
        .map(|r| (r.key, r.value.into()))
        .collect();
    let packs = d
        .q(
            "SELECT id,state,count,created_at,(push_id IS NULL) AS push_id_null FROM packs ORDER BY id",
            vec![],
        )?
        .to_array::<PackRow>()?;
    Ok(serde_json::json!({
        "jobs": jobs.iter().map(|j| serde_json::json!({
            "id": j.id, "kind": j.kind, "state": j.state, "run_at": j.run_at,
            "attempts": j.attempts, "strands": j.strands, "last_error": j.last_error,
            "cursor_present": j.cursor_present != 0,
        })).collect::<Vec<_>>(),
        "gc_meta": gc_meta,
        "packs": packs.iter().map(|p| serde_json::json!({
            "id": p.id, "state": p.state, "count": p.count,
            "created_at": p.created_at, "push_id_null": p.push_id_null != 0,
        })).collect::<Vec<_>>(),
    }))
}

pub fn jobs_view(d: &RepoDo) -> Result<Response, Error> {
    json(view(d)?)
}

/// Lazily created by set_knob — deliberately NOT in the production schema list.
const KNOBS_DDL: &str = "CREATE TABLE IF NOT EXISTS test_knobs(name TEXT PRIMARY KEY, value INTEGER)";

/// Named integer knobs (slice_subreqs, gc_part_bytes) — read ONLY when the
/// hooks flag is on; hooks off means no SQL at all (and no lazy table).
pub(crate) fn knob(d: &RepoDo, name: &str) -> Option<i64> {
    if !enabled(d) {
        return None;
    }
    #[derive(serde::Deserialize)]
    struct R {
        value: i64,
    }
    // to_array, never one() — a zero-row cursor makes workers-rs throw a
    // "Critical error" that kills the isolate outright
    d.q("SELECT value FROM test_knobs WHERE name=?", vec![V::from(name)])
        .ok()
        .and_then(|c| c.to_array::<R>().ok())
        .and_then(|rows| rows.into_iter().next())
        .map(|r| r.value)
}

/// POST /_do/test/knob {"name":..,"value":..} — value null deletes the knob.
pub fn set_knob(d: &RepoDo, body: &[u8]) -> Result<Response, Error> {
    #[derive(serde::Deserialize)]
    struct B {
        name: String,
        value: Option<i64>,
    }
    let b: B = parse(body)?;
    if b.name.is_empty() || b.name.len() > 64 {
        return Err(Error::Protocol("bad knob name".into()));
    }
    if b.name == "gc_part_bytes" && d.meta_opt("gc.pos")?.is_some() {
        // a mid-build part-size change would desync the etag prune on resume
        return Err(Error::Conflict("gc build in progress".into()));
    }
    d.q(KNOBS_DDL, vec![])?;
    match b.value {
        None => {
            d.q("DELETE FROM test_knobs WHERE name=?", vec![V::from(b.name.as_str())])?;
        }
        Some(v) => {
            d.q(
                "INSERT INTO test_knobs(name,value) VALUES(?,?) \
                 ON CONFLICT(name) DO UPDATE SET value=excluded.value",
                vec![V::from(b.name.as_str()), V::from(v)],
            )?;
        }
    }
    json(serde_json::json!({ "knob": b.name, "value": b.value }))
}

pub fn set_failpoint(d: &RepoDo, body: &[u8]) -> Result<Response, Error> {
    #[derive(serde::Deserialize)]
    struct B {
        name: String,
        action: String,
        #[serde(default)]
        count: Option<i64>,
    }
    let b: B = parse(body)?;
    if b.name.is_empty() || b.name.len() > 128 {
        return Err(Error::Protocol("bad failpoint name".into()));
    }
    d.q(DDL, vec![])?;
    match b.action.as_str() {
        "off" => {
            d.q("DELETE FROM test_failpoints WHERE name=?", vec![V::from(b.name.as_str())])?;
        }
        a @ ("error" | "crash") => {
            let n = b.count.unwrap_or(1);
            if !(0..=1_000_000).contains(&n) {
                return Err(Error::Protocol("count must be 0..=1000000".into()));
            }
            d.q(
                "INSERT INTO test_failpoints(name,action,remaining) VALUES(?,?,?) \
                 ON CONFLICT(name) DO UPDATE SET action=excluded.action, remaining=excluded.remaining",
                vec![V::from(b.name.as_str()), V::from(a), V::from(n)],
            )?;
        }
        _ => return Err(Error::Protocol("action must be error|crash|off".into())),
    }
    json(serde_json::json!({ "armed": b.name, "action": b.action }))
}

/// Turn a simulated crash into a requeue deterministically: age every 'running'
/// row past the 60 s straggler window, then run the same jobs::repair boot does.
pub fn repair(d: &RepoDo) -> Result<Response, Error> {
    d.q(
        "UPDATE jobs SET started_at=started_at-61000 WHERE state='running'",
        vec![],
    )?;
    jobs::repair(&d.sql())?;
    jobs_view(d)
}

/// Test-only corruption: delete a sha's objects rows so the fsck hook can
/// prove it detects the loss. The R2 bytes stay — the index just forgets them.
/// action "corrupt" instead shifts the indexed offset so decode/hash fail —
/// the index lies about bytes that still exist.
pub fn drop_object(d: &RepoDo, body: &[u8]) -> Result<Response, Error> {
    #[derive(serde::Deserialize)]
    struct B {
        sha: String,
        action: Option<String>,
        to: Option<String>,
    }
    let b: B = parse(body)?;
    // only well-formed hex: a typo must fail loudly, not silently delete
    // nothing and leave a test believing the repo is corrupt
    if !(4..=64).contains(&b.sha.len()) || !b.sha.bytes().all(|c| c.is_ascii_hexdigit()) {
        return Err(Error::Protocol("sha must be hex".into()));
    }
    match b.action.as_deref() {
        None | Some("drop") => {
            d.q("DELETE FROM objects WHERE sha=?", vec![V::from(b.sha.as_str())])?;
            json(serde_json::json!({ "dropped": b.sha, "rows": d.changes()? }))
        }
        Some("corrupt") => {
            d.q("UPDATE objects SET offset=offset+1 WHERE sha=?", vec![V::from(b.sha.as_str())])?;
            json(serde_json::json!({ "corrupted": b.sha, "rows": d.changes()? }))
        }
        Some("len_plus") => {
            // len+1: the extent still fits the pack but the entry under-reads it
            d.q("UPDATE objects SET len=len+1 WHERE sha=?", vec![V::from(b.sha.as_str())])?;
            json(serde_json::json!({ "corrupted": b.sha, "rows": d.changes()? }))
        }
        Some("size_plus") => {
            // size+1: decode works but the inflated length disagrees with the index
            d.q("UPDATE objects SET size=size+1 WHERE sha=?", vec![V::from(b.sha.as_str())])?;
            json(serde_json::json!({ "corrupted": b.sha, "rows": d.changes()? }))
        }
        Some("alias") => {
            // copy `to`'s extent onto `sha`'s row in the SAME pack — the bytes
            // decode fine but hash to `to`: an index row that names the wrong
            // object. (Also why fsck keys its checked set by row, not offset.)
            let to = b
                .to
                .as_deref()
                .ok_or_else(|| Error::Protocol("alias needs \"to\"".into()))?;
            #[derive(serde::Deserialize)]
            struct L {
                pack_id: String,
                offset: i64,
                len: i64,
            }
            let mut rows = d
                .q(
                    "SELECT t.pack_id,t.offset,t.len FROM objects t \
                     JOIN objects s ON s.pack_id=t.pack_id AND s.sha=? \
                     WHERE t.sha=? LIMIT 1",
                    vec![V::from(b.sha.as_str()), V::from(to)],
                )?
                .to_array::<L>()?
                .into_iter();
            let l = rows
                .next()
                .ok_or_else(|| Error::Protocol("alias: no shared-pack row".into()))?;
            // the WHERE mirrors the join — only the row in THAT pack moves
            d.q(
                "UPDATE objects SET offset=?,len=? WHERE sha=? AND pack_id=?",
                vec![
                    V::from(l.offset),
                    V::from(l.len),
                    V::from(b.sha.as_str()),
                    V::from(l.pack_id.as_str()),
                ],
            )?;
            json(serde_json::json!({ "aliased": b.sha, "onto": to, "rows": d.changes()? }))
        }
        Some(a) => Err(Error::Protocol(format!("action must be drop|corrupt|len_plus|size_plus|alias, not {a}"))),
    }
}

/// POST /_do/test/drop_pack {"pack":"<id>"} — delete the pack's R2 object
/// outright. The index keeps its rows — fsck must report them missing.
pub async fn drop_pack(d: &RepoDo, body: &[u8]) -> Result<Response, Error> {
    if !enabled(d) {
        return Err(Error::NotFound);
    }
    #[derive(serde::Deserialize)]
    struct B {
        pack: String,
    }
    let b: B = parse(body)?;
    let key = keys::pack(&d.repo_id()?, &PackId(b.pack.clone()));
    d.bucket()?.inner.delete(key).await.map_err(Error::from)?;
    json(serde_json::json!({ "dropped_pack": b.pack }))
}

/// POST /_do/test/step — one job slice via jobs::dispatch_inner, the same code
/// path alarm() takes (run_at ignored so a backoff-scheduled retry still runs).
pub async fn step(d: &RepoDo, body: &[u8]) -> Result<Response, Error> {
    if !enabled(d) {
        return Err(Error::NotFound);
    }
    if !manual_jobs(d) {
        // without manual mode the alarm races the step — refuse rather than
        // hand a test a non-deterministic slice
        return Err(Error::Conflict("step requires GE_TEST_MANUAL_JOBS=1".into()));
    }
    #[derive(serde::Deserialize, Default)]
    struct B {
        kind: Option<String>,
    }
    let b: B = if body.is_empty() { B::default() } else { parse(body)? };
    let kind = b.kind.as_deref().map(JobKind::of).transpose()?;
    let report = jobs::step(d, kind).await?;
    let mut v = view(d)?;
    if let Some(m) = v.as_object_mut() {
        m.insert("ran".into(), report.is_some().into());
        match &report {
            Some(r) => {
                m.insert("job".into(), serde_json::json!({ "id": r.job_id, "kind": r.kind }));
                m.insert("outcome".into(), r.outcome.into());
                m.insert(
                    "error".into(),
                    r.error.clone().map_or(serde_json::Value::Null, |e| e.into()),
                );
            }
            None => {
                m.insert("job".into(), serde_json::Value::Null);
                m.insert("outcome".into(), serde_json::Value::Null);
                m.insert("error".into(), serde_json::Value::Null);
            }
        }
    }
    json(v)
}

// ---- fsck: integrity oracle over the live index + R2 ----

// FSCK_MAX rows + one R2 HEAD per distinct pack must fit the 9000-subrequest
// paid() cap a single call gets — 4000 keeps that guarantee comfortably.
const FSCK_MAX: usize = 4_000;
const FSCK_CAP: usize = 100; // missing/corrupt entry caps
const FSCK_BATCH: usize = 64; // drain bound per walk round
pub async fn fsck(d: &RepoDo, body: &[u8]) -> Result<Response, Error> {
    if !enabled(d) {
        return Err(Error::NotFound);
    }
    #[derive(serde::Deserialize)]
    struct M {
        mode: Option<String>,
    }
    let m: M = if body.is_empty() { M { mode: None } } else { parse(body)? };
    let all = match m.mode.as_deref() {
        None | Some("") | Some("refs") => false,
        Some("all") => true,
        Some(x) => return Err(Error::Protocol(format!("fsck mode {x}"))),
    };
    json(fsck_run(d, all).await?)
}

fn note_missing(missing: &mut Vec<serde_json::Value>, sha: &str, by: &str) {
    if missing.len() < FSCK_CAP {
        missing.push(serde_json::json!({ "sha": sha, "referenced_by": by }));
    }
}

fn note_corrupt(corrupt: &mut Vec<serde_json::Value>, sha: &str, why: String) {
    if corrupt.len() < FSCK_CAP {
        corrupt.push(serde_json::json!({ "sha": sha, "why": why }));
    }
}

fn kind_str(k: Kind) -> &'static str {
    match k {
        Kind::Commit => "commit",
        Kind::Tree => "tree",
        Kind::Blob => "blob",
        Kind::Tag => "tag",
    }
}

/// git object id: hash("<kind> <len>\0" + data) in the repo's object format.
fn object_id(hash_kind: gix_hash::Kind, k: Kind, data: &[u8]) -> Result<ObjectId, Error> {
    let mut h = gix_hash::hasher(hash_kind);
    h.update(format!("{} {}\0", kind_str(k), data.len()).as_bytes());
    h.update(data);
    h.try_finalize().map_err(|_| Error::Internal("object hash".into()))
}

/// Every objects row of every live pack, sha-sorted — mode=all iterates it
/// directly (I2: rows, not shas); the by-sha map below keys ALL its rows.
fn live_rows(d: &RepoDo) -> Result<Vec<ObjSqlRow>, Error> {
    let rows = d
        .q(
            "SELECT o.sha,o.pack_id,o.idx,o.offset,o.len,o.kind,o.size FROM objects o \
             JOIN packs p ON p.id=o.pack_id WHERE p.state='live' ORDER BY o.sha",
            vec![],
        )?
        .to_array::<ObjSqlRow>()?;
    if rows.len() > FSCK_MAX {
        return Err(Error::Internal(format!("fsck: {} live objects > {FSCK_MAX}", rows.len())));
    }
    Ok(rows)
}

/// Every ref target and peeled target is a walk root — (sha, referenced_by).
fn ref_roots(d: &RepoDo) -> Result<VecDeque<(String, String)>, Error> {
    #[derive(serde::Deserialize)]
    struct T {
        name: String,
        target: String,
        peeled: Option<String>,
    }
    let mut roots = VecDeque::new();
    for r in d.q("SELECT name,target,peeled FROM refs", vec![])?.to_array::<T>()? {
        roots.push_back((r.target, r.name.clone()));
        if let Some(p) = r.peeled {
            roots.push_back((p, r.name));
        }
    }
    Ok(roots)
}

/// Drain the queue into one round of unseen live locations — missing shas are
/// recorded; EVERY live row behind each sha comes back for checking.
fn unseen_batch(
    queue: &mut VecDeque<(String, String)>,
    live: &HashMap<String, Vec<ObjLoc>>,
    seen: &mut HashSet<String>,
    missing: &mut Vec<serde_json::Value>,
) -> Result<Vec<(String, String, Vec<ObjLoc>)>, Error> {
    let mut locs = Vec::new();
    while let Some((sha, by)) = queue.pop_front() {
        if !seen.insert(sha.clone()) {
            continue;
        }
        if seen.len() > FSCK_MAX {
            return Err(Error::Internal(format!("fsck: visited > {FSCK_MAX}")));
        }
        match live.get(&sha) {
            None => note_missing(missing, &sha, &by),
            Some(rows) => locs.push((sha, by, rows.clone())),
        }
        if locs.len() >= FSCK_BATCH {
            break;
        }
    }
    Ok(locs)
}

/// Per-call oracle state: the repo handle + format, the pack HEAD cache, the
/// two report lists, and the paid request budget R2 reads charge against.
/// Per-object failures land in `missing`/`corrupt`; only storage-level
/// errors (SQL, R2 faults, budget) abort the call.
struct Fsck<'d> {
    d: &'d RepoDo,
    repo: RepoId,
    obj_kind: gix_hash::Kind,
    packs: HashMap<String, Option<u64>>, // pack id -> R2 size (None = no key)
    // (sha, pack_id) already adjudicated by walk_refs — verified AND failed:
    // all-mode skips them so each row reports at most once. Row identity, not
    // offset — offsets aren't unique in the schema (see the alias action).
    checked_rows: HashSet<(String, String)>,
    missing: Vec<serde_json::Value>,
    corrupt: Vec<serde_json::Value>,
    checked: usize,
    budget: ReqBudget,
}

impl Fsck<'_> {
    fn new(d: &RepoDo) -> Result<Fsck<'_>, Error> {
        Ok(Fsck {
            d,
            repo: d.repo_id()?,
            obj_kind: d.obj_kind()?,
            packs: HashMap::new(),
            checked_rows: HashSet::new(),
            missing: Vec::new(),
            corrupt: Vec::new(),
            checked: 0,
            budget: ReqBudget::paid(),
        })
    }

    /// R2 size of a pack, one HEAD per call cached — None = the key is absent.
    async fn pack_size(&mut self, pack: &str) -> Result<Option<u64>, Error> {
        if let Some(v) = self.packs.get(pack) {
            return Ok(*v);
        }
        self.budget.charge(1)?;
        let key = keys::pack(&self.repo, &PackId(pack.to_string()));
        let size = self.d.bucket()?.inner.head(&key).await?.map(|h| h.size());
        self.packs.insert(pack.to_string(), size);
        Ok(size)
    }

    /// The both-modes location gate: pack exists in R2 and the entry extent
    /// fits it — a bad row is recorded and reports false (don't read it).
    async fn extent_ok(&mut self, sha: &str, loc: &ObjLoc, by: &str) -> Result<bool, Error> {
        match self.pack_size(&loc.pack.0).await? {
            None => {
                note_missing(&mut self.missing, sha, by);
                Ok(false)
            }
            Some(size) if loc.offset.saturating_add(u64::from(loc.len)) > size => {
                note_corrupt(
                    &mut self.corrupt,
                    sha,
                    format!("extent {}+{} past pack size {size}", loc.offset, loc.len),
                );
                Ok(false)
            }
            Some(_) => Ok(true),
        }
    }

    /// Read + decode one indexed location, strictly: the entry must consume
    /// exactly its indexed extent AND inflate to the indexed size.
    /// Error classes — a missing R2 key is `missing`; decode/short-read
    /// faults are `corrupt`; budget and other R2/infra errors PROPAGATE so a
    /// partial check can never report ok.
    async fn decoded(&mut self, sha: &str, loc: &ObjLoc, by: &str) -> Result<Option<(Kind, Vec<u8>)>, Error> {
        let key = keys::pack(&self.repo, &loc.pack);
        let entry = match self
            .d
            .bucket()?
            .read_range(&key, loc.offset, u64::from(loc.len), &mut self.budget)
            .await
        {
            Ok(b) => b,
            Err(Error::Storage(m)) if m.starts_with("missing ") => {
                note_missing(&mut self.missing, sha, by);
                return Ok(None);
            }
            Err(Error::Storage(m)) if m.starts_with("short read") => {
                note_corrupt(&mut self.corrupt, sha, m);
                return Ok(None);
            }
            Err(e) => return Err(e),
        };
        let (kind, data, used) = match codec::decode_entry_counted(&entry) {
            Ok(v) => v,
            Err(e) => {
                note_corrupt(&mut self.corrupt, sha, e.to_string());
                return Ok(None);
            }
        };
        if usize::try_from(loc.len).ok() != Some(used) {
            note_corrupt(
                &mut self.corrupt,
                sha,
                format!("entry consumes {used} bytes, index says len {}", loc.len),
            );
            return Ok(None);
        }
        if u64::try_from(data.len()).ok() != Some(loc.size) {
            note_corrupt(
                &mut self.corrupt,
                sha,
                format!("inflates to {} bytes, index says size {}", data.len(), loc.size),
            );
            return Ok(None);
        }
        Ok(Some((kind, data)))
    }

    /// The shared per-row gate both modes run: extent + read + strict decode +
    /// indexed-kind equality. Ok(None) = already recorded, don't use the row.
    /// Every call ends in exactly one adjudication, so the row is marked
    /// checked up front — all-mode never double-reports it.
    async fn row(&mut self, sha: &str, loc: &ObjLoc, by: &str) -> Result<Option<(Kind, Vec<u8>)>, Error> {
        self.checked_rows.insert((sha.to_string(), loc.pack.0.clone()));
        if !self.extent_ok(sha, loc, by).await? {
            return Ok(None);
        }
        let Some((kind, data)) = self.decoded(sha, loc, by).await? else { return Ok(None) };
        if kind != loc.kind {
            note_corrupt(
                &mut self.corrupt,
                sha,
                format!("indexed {} but decodes {}", kind_str(loc.kind), kind_str(kind)),
            );
            return Ok(None);
        }
        Ok(Some((kind, data)))
    }

    /// The all-mode hash check — the index claims (sha,kind,size); the bytes
    /// must hash back to the sha.
    fn hash_ok(&mut self, sha: &str, kind: Kind, data: &[u8]) -> Result<bool, Error> {
        let got = object_id(self.obj_kind, kind, data)?;
        if got.to_string() == sha {
            Ok(true)
        } else {
            note_corrupt(&mut self.corrupt, sha, format!("content hashes to {got}"));
            Ok(false)
        }
    }

    /// refs walk: BFS over the link closure of the roots — EVERY live row
    /// behind EVERY reachable sha (blobs included) is read and hash-verified
    /// via row()+hash_ok; links expand once per sha from a verified non-blob.
    async fn walk_refs(
        &mut self,
        live: &HashMap<String, Vec<ObjLoc>>,
        roots: VecDeque<(String, String)>,
        seen: &mut HashSet<String>,
    ) -> Result<(), Error> {
        let mut queue = roots;
        while !queue.is_empty() {
            for (sha, by, locs) in unseen_batch(&mut queue, live, seen, &mut self.missing)? {
                let mut linked = false;
                for loc in &locs {
                    let Some((kind, data)) = self.row(&sha, loc, &by).await? else { continue };
                    if !self.hash_ok(&sha, kind, &data)? {
                        continue;
                    }
                    self.checked += 1;
                    if kind == Kind::Blob || linked {
                        continue;
                    }
                    linked = true;
                    self.enqueue_links(&mut queue, &sha, kind, &data);
                }
            }
        }
        Ok(())
    }

    /// Parse a verified non-blob's links onto the walk queue; a parse failure
    /// is per-object `corrupt`, not an abort.
    fn enqueue_links(
        &mut self,
        queue: &mut VecDeque<(String, String)>,
        sha: &str,
        kind: Kind,
        data: &[u8],
    ) {
        match extract_links(kind, data, self.obj_kind) {
            Ok(links) => {
                for l in links {
                    queue.push_back((l.to_string(), sha.to_string()));
                }
            }
            Err(e) => note_corrupt(&mut self.corrupt, sha, e.to_string()),
        }
    }

    /// mode=all (I1+I2): EVERY objects row of every live pack — extent,
    /// strict decode, kind, hash — and each live sha's links resolve live.
    /// Locations walk_refs already verified are skipped, not re-read.
    async fn check_all(
        &mut self,
        rows: &[ObjSqlRow],
        live: &HashMap<String, Vec<ObjLoc>>,
        seen: &mut HashSet<String>,
    ) -> Result<(), Error> {
        for r in rows {
            if self.checked_rows.contains(&(r.sha.clone(), r.pack_id.clone())) {
                continue;
            }
            let (_, loc) = match r.loc() {
                Ok(l) => l,
                Err(e) => {
                    note_corrupt(&mut self.corrupt, &r.sha, e.to_string());
                    continue;
                }
            };
            let Some((kind, data)) = self.row(&r.sha, &loc, &r.sha).await? else { continue };
            if !self.hash_ok(&r.sha, kind, &data)? {
                continue;
            }
            self.checked += 1;
            if kind == Kind::Blob || !seen.insert(r.sha.clone()) {
                continue;
            }
            if seen.len() > FSCK_MAX {
                return Err(Error::Internal(format!("fsck: visited > {FSCK_MAX}")));
            }
            self.links_live(&r.sha, kind, &data, live);
        }
        Ok(())
    }

    /// I1: a verified non-blob's links must resolve to live objects.
    fn links_live(&mut self, sha: &str, kind: Kind, data: &[u8], live: &HashMap<String, Vec<ObjLoc>>) {
        match extract_links(kind, data, self.obj_kind) {
            Ok(links) => {
                for l in links {
                    if !live.contains_key(&l.to_string()) {
                        note_missing(&mut self.missing, &l.to_string(), sha);
                    }
                }
            }
            Err(e) => note_corrupt(&mut self.corrupt, sha, e.to_string()),
        }
    }
}

async fn fsck_run(d: &RepoDo, all: bool) -> Result<serde_json::Value, Error> {
    let rows = live_rows(d)?;
    let mut f = Fsck::new(d)?;
    // sha -> ALL its live locations — a sha duplicated across live packs gets
    // every row checked
    let mut live: HashMap<String, Vec<ObjLoc>> = HashMap::new();
    for r in &rows {
        match r.loc() {
            Ok((_, l)) => live.entry(r.sha.clone()).or_default().push(l),
            Err(e) => {
                note_corrupt(&mut f.corrupt, &r.sha, e.to_string());
                // already adjudicated — check_all must not report it twice
                f.checked_rows.insert((r.sha.clone(), r.pack_id.clone()));
            }
        }
    }
    let mut seen = HashSet::new();
    f.walk_refs(&live, ref_roots(d)?, &mut seen).await?;
    if all {
        f.check_all(&rows, &live, &mut seen).await?;
    }
    Ok(serde_json::json!({
        "ok": f.missing.is_empty() && f.corrupt.is_empty(),
        "missing": f.missing,
        "corrupt": f.corrupt,
        "checked": f.checked,
    }))
}

#[cfg(test)]
mod tests {
    /// The hooks gate itself: only exactly "1" is on — every other value,
    /// including whitespace variants, keeps the hooks inert.
    #[test]
    fn flag_on_only_exactly_one() {
        for v in [None, Some(""), Some("0"), Some("true"), Some(" 1")] {
            assert!(!super::flag_on(v), "{v:?}");
        }
        assert!(super::flag_on(Some("1")));
    }
}
