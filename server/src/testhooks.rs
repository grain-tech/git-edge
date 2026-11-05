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
//!   POST /_do/test/meta        {"key","value"|null} — set/delete a meta row (legacy-state jig)
//!   POST /_do/test/enqueue     {"kind","payload"?} — insert a job row like repair resurrection

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
        // "pause" rows belong to failpoint_async — a sync call site cannot
        // await, so it must leave the arm untouched rather than eat the count
        "UPDATE test_failpoints SET remaining=remaining-1 \
         WHERE name=? AND action<>'pause' AND remaining>0",
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

/// The async sibling of [`failpoint`] — identical armed-count semantics for
/// "error"/"crash" (delegated), plus action "pause": the arm is consumed, then
/// the slice awaits, polling the row every 100 ms until `off` deletes it or
/// 30 s pass. The `Delay` await is the point where a DO interleaves other
/// requests — a test uses that window to strand and reclaim the paused slice's
/// job row underneath it (C7). Armed at a site that never calls this fn, a
/// "pause" row simply stays armed and inert.
pub async fn failpoint_async(d: &RepoDo, name: &str) -> Result<(), Error> {
    if !enabled(d) {
        return Ok(());
    }
    d.q(DDL, vec![])?;
    // the pause arm decrements here and the row survives so the poll can see
    // it still says "pause" — set_failpoint "off" deletes it to release
    d.q(
        "UPDATE test_failpoints SET remaining=remaining-1 \
         WHERE name=? AND action='pause' AND remaining>0",
        vec![V::from(name)],
    )?;
    if d.changes()? != 1 {
        return failpoint(d, name);
    }
    #[derive(serde::Deserialize)]
    struct A {
        action: String,
    }
    let deadline = crate::platform::now_ms() + 30_000;
    while crate::platform::now_ms() < deadline {
        let armed = d
            .q("SELECT action FROM test_failpoints WHERE name=?", vec![V::from(name)])?
            .to_array::<A>()?
            .into_iter()
            .next()
            .map(|a| a.action == "pause")
            .unwrap_or(false);
        if !armed {
            return Ok(());
        }
        worker::Delay::from(std::time::Duration::from_millis(100)).await;
    }
    Ok(())
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
    cursor: Option<String>,
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
    bytes: i64,
    created_at: i64,
    commit_lo: i64,
    commit_hi: i64,
    push_id_null: i64,
    marked: i64,
}
#[derive(serde::Deserialize)]
struct PushRow {
    id: String,
    state: String,
}
#[derive(serde::Deserialize)]
struct N {
    n: i64,
}

fn view(d: &RepoDo) -> Result<serde_json::Value, Error> {
    let jobs = d
        .q(
            "SELECT id,kind,state,run_at,attempts,strands,last_error,cursor, \
             (cursor IS NOT NULL) AS cursor_present FROM jobs ORDER BY id",
            vec![],
        )?
        .to_array::<JobRow>()?;
    let gc_meta: serde_json::Map<String, serde_json::Value> = d
        .q(
            "SELECT key,value FROM meta WHERE key LIKE 'gc.%' OR key LIKE 'dead.%' \
             OR key IN ('compact.quarantine','compact.quarantine.corrupt','gc_epoch','refs_version')",
            vec![],
        )?
        .to_array::<KV>()?
        .into_iter()
        .map(|r| (r.key, r.value.into()))
        .collect();
    let packs = d
        .q(
            "SELECT id,state,count,bytes,created_at,commit_lo,commit_hi, \
             (push_id IS NULL) AS push_id_null, \
             EXISTS(SELECT 1 FROM marked m WHERE m.pack_id=packs.id) AS marked \
             FROM packs ORDER BY id",
            vec![],
        )?
        .to_array::<PackRow>()?;
    let pushes = d
        .q("SELECT id,state FROM pushes ORDER BY id", vec![])?
        .to_array::<PushRow>()?;
    let rate_rows = d.q("SELECT COUNT(*) AS n FROM rate", vec![])?.one::<N>()?.n;
    #[derive(serde::Deserialize)]
    struct Fp {
        name: String,
        action: String,
        remaining: i64,
    }
    // the table is lazily created by set_failpoint/failpoint — a repo that
    // never armed one just reports an empty list
    let failpoints = match d.q("SELECT name,action,remaining FROM test_failpoints ORDER BY name", vec![]) {
        Ok(c) => c.to_array::<Fp>().unwrap_or_default(),
        Err(_) => Vec::new(),
    };
    Ok(serde_json::json!({
        "jobs": jobs.iter().map(|j| serde_json::json!({
            "id": j.id, "kind": j.kind, "state": j.state, "run_at": j.run_at,
            "attempts": j.attempts, "strands": j.strands, "last_error": j.last_error,
            "cursor_present": j.cursor_present != 0, "cursor": j.cursor,
        })).collect::<Vec<_>>(),
        "gc_meta": gc_meta,
        // counts + a content digest per GC scratch table — the C7 stale-lease
        // oracle: compare snapshots across the lease hand-off and any write a
        // stale slice managed to land shows up
        "gc_tables": serde_json::json!({
            "marked": table_digest(d, "marked", "pack_id || ':' || hex(bitmap)")?,
            "gc_seen": table_digest(d, "gc_seen", "sha")?,
            "gc_frontier": table_digest(d, "gc_frontier", "sha")?,
            // the import side's write targets — the C7 stale-lease test
            // snapshots a stale import slice against these the same way
            "objects": table_digest(d, "objects",
                "sha || ':' || pack_id || ':' || idx || ':' || offset || ':' || len || ':' || kind || ':' || size")?,
            "push_links": table_digest(d, "push_links", "push_id || ':' || sha")?,
            "import_open": table_digest(d, "import_open",
                "push_id || ':' || idx || ':' || off || ':' || COALESCE(sha,'') || ':' || kind || ':' || size || ':' || COALESCE(end,'')")?,
            "import_parts": table_digest(d, "import_parts", "push_id || ':' || part_no || ':' || etag")?,
            "import_tail": table_digest(d, "import_tail", "push_id || ':' || seq || ':' || hex(blob)")?,
        }),
        "failpoints": failpoints.iter().map(|f| serde_json::json!({
            "name": f.name, "action": f.action, "remaining": f.remaining,
        })).collect::<Vec<_>>(),
        "packs": packs.iter().map(|p| serde_json::json!({
            "id": p.id, "state": p.state, "count": p.count, "bytes": p.bytes,
            "created_at": p.created_at, "commit_lo": p.commit_lo,
            "commit_hi": p.commit_hi, "push_id_null": p.push_id_null != 0,
            "marked": p.marked != 0,
        })).collect::<Vec<_>>(),
        "pushes": pushes.iter().map(|p| serde_json::json!({
            "id": p.id, "state": p.state,
        })).collect::<Vec<_>>(),
        "rate_rows": rate_rows,
    }))
}

/// One GC scratch table's (row count, content digest). `expr` renders each row
/// as one string; the ORDER BY makes the digest deterministic.
fn table_digest(d: &RepoDo, table: &str, expr: &str) -> Result<serde_json::Value, Error> {
    #[derive(serde::Deserialize)]
    struct R {
        v: String,
    }
    let rows = d
        .q(&format!("SELECT {expr} AS v FROM {table} ORDER BY v"), vec![])?
        .to_array::<R>()?;
    Ok(serde_json::json!({
        "n": rows.len(),
        "hash": rows_digest(rows.iter().map(|r| r.v.as_str())),
    }))
}

/// FNV-1a over the row strings with a per-row separator, so ["ab","c"] and
/// ["a","bc"] hash differently. Pure — unit-tested below.
fn rows_digest<'a>(rows: impl Iterator<Item = &'a str>) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for r in rows {
        for b in r.bytes().chain(std::iter::once(0xff)) {
            h = (h ^ u64::from(b)).wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    h
}

pub fn jobs_view(d: &RepoDo) -> Result<Response, Error> {
    json(view(d)?)
}

/// Lazily created by set_knob — deliberately NOT in the production schema list.
const KNOBS_DDL: &str = "CREATE TABLE IF NOT EXISTS test_knobs(name TEXT PRIMARY KEY, value INTEGER)";

/// Named integer knobs (slice_subreqs, gc_part_bytes, max_live_packs) — read
/// ONLY when the hooks flag is on; hooks off means no SQL at all (and no lazy table).
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

/// POST /_do/test/meta {"key","value"} — write one meta row directly (a
/// legacy-GC-chain fixture deletes gc.phase/gc.chain this way); null deletes.
pub fn set_meta(d: &RepoDo, body: &[u8]) -> Result<Response, Error> {
    #[derive(serde::Deserialize)]
    struct B {
        key: String,
        value: Option<String>,
    }
    let b: B = parse(body)?;
    if b.key.is_empty() || b.key.len() > 128 {
        return Err(Error::Protocol("bad meta key".into()));
    }
    match &b.value {
        None => {
            d.q("DELETE FROM meta WHERE key=?", vec![V::from(b.key.as_str())])?;
        }
        Some(v) => {
            d.q(
                "INSERT INTO meta(key,value) VALUES(?,?) \
                 ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                vec![V::from(b.key.as_str()), V::from(v.as_str())],
            )?;
        }
    }
    json(serde_json::json!({ "key": b.key, "value": b.value }))
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
        a @ ("error" | "crash" | "pause") => {
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
        _ => return Err(Error::Protocol("action must be error|crash|pause|off".into())),
    }
    json(serde_json::json!({ "armed": b.name, "action": b.action }))
}

/// POST /_do/test/enqueue {"kind","payload"?} — queue a job row exactly the
/// way repair resurrection does (jobs::enqueue still dedups against queued
/// rows of the kind). The GC phase-guard test inserts a chain-less "{}"
/// consolidate mid-mark; default payload is "{}", like a resurrected row.
pub fn enqueue_job(d: &RepoDo, body: &[u8]) -> Result<Response, Error> {
    #[derive(serde::Deserialize)]
    struct B {
        kind: String,
        #[serde(default)]
        payload: Option<String>,
    }
    let b: B = parse(body)?;
    jobs::enqueue(
        &d.sql(),
        JobKind::of(&b.kind)?,
        crate::platform::now_ms(),
        b.payload.as_deref().unwrap_or("{}"),
    )?;
    jobs_view(d)
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

/// GET /_do/test/objects — {"shas": sorted distinct SHAs over live packs'
/// objects rows, "rows": n, "dead_rows": objects rows whose pack is neither
/// live nor ingesting}. The PR4 lossless-compaction oracle: the live SHA set
/// must be byte-identical before and after a swap, and the swap must delete
/// the cohort's rows — fsck only reads live packs, so dead_rows is the only
/// place a leaked row shows up.
pub fn objects_view(d: &RepoDo) -> Result<Response, Error> {
    #[derive(serde::Deserialize)]
    struct S {
        sha: String,
    }
    let rows = d
        .q(
            "SELECT o.sha AS sha FROM objects o JOIN packs p ON p.id=o.pack_id \
             WHERE p.state='live'",
            vec![],
        )?
        .to_array::<S>()?;
    let mut shas: Vec<String> = rows.iter().map(|r| r.sha.clone()).collect();
    shas.sort_unstable();
    shas.dedup();
    #[derive(serde::Deserialize)]
    struct N {
        n: i64,
    }
    let dead_rows = d
        .q(
            "SELECT COUNT(*) AS n FROM objects o JOIN packs p ON p.id=o.pack_id \
             WHERE p.state NOT IN ('live','ingesting')",
            vec![],
        )?
        .one::<N>()?
        .n;
    json(serde_json::json!({ "shas": shas, "rows": rows.len(), "dead_rows": dead_rows }))
}

/// POST /_do/test/pack_state {"pack","state"} — force packs.state (e.g. kill
/// a cohort pack under a paused swap); 'dead' stamps dead_at like the real
/// transitions do.
pub fn pack_state(d: &RepoDo, body: &[u8]) -> Result<Response, Error> {
    #[derive(serde::Deserialize)]
    struct B {
        pack: String,
        state: String,
    }
    let b: B = parse(body)?;
    if !matches!(b.state.as_str(), "ingesting" | "live" | "dead") {
        return Err(Error::Protocol("state must be ingesting|live|dead".into()));
    }
    d.q(
        "UPDATE packs SET state=? WHERE id=?",
        vec![V::from(b.state.as_str()), V::from(b.pack.as_str())],
    )?;
    let rows = d.changes()?;
    if b.state == "dead" {
        d.q(
            "UPDATE packs SET dead_at=? WHERE id=? AND dead_at IS NULL",
            vec![V::from(crate::platform::now_ms()), V::from(b.pack.as_str())],
        )?;
    }
    json(serde_json::json!({ "pack": b.pack, "state": b.state, "rows": rows }))
}

/// POST /_do/test/kill_job {"kind"} — every queued/running row of `kind` ->
/// 'dead' with last_error 'test kill': strands a chain the way a poisoned job
/// row would (its owner vanishes, so the next gc_compact sees an orphan).
pub fn kill_job(d: &RepoDo, body: &[u8]) -> Result<Response, Error> {
    #[derive(serde::Deserialize)]
    struct B {
        kind: String,
    }
    let b: B = parse(body)?;
    let kind = JobKind::of(&b.kind)?;
    d.q(
        "UPDATE jobs SET state='dead', last_error='test kill', lease=NULL \
         WHERE kind=? AND state IN ('queued','running')",
        vec![V::from(kind.as_str())],
    )?;
    json(serde_json::json!({ "killed": kind.as_str(), "rows": d.changes()? }))
}

/// POST /_do/test/pack_check {"pack"} — decode the pack's R2 object fully and
/// compare it with its index rows: {"header_count","decoded","index_rows",
/// "distinct","commit_lo","commit_hi"}. commit_lo/hi are the decoded
/// commit-entry span — min start offset, max end (start + wire length),
/// PackWriter's convention — or null when the pack holds no commits. The
/// order-independent oracle for a packs row's span: pack_id ordering and
/// entry sizes can't skew it. The PR4 output-pack oracle over replay and
/// HEAD-recovery.
pub async fn pack_check(d: &RepoDo, body: &[u8]) -> Result<Response, Error> {
    if !enabled(d) {
        return Err(Error::NotFound);
    }
    #[derive(serde::Deserialize)]
    struct B {
        pack: String,
    }
    let b: B = parse(body)?;
    let bytes = pack_check_fetch_bytes(d, &b.pack).await?;
    let ds = d.obj_kind()?.len_in_bytes();
    let header_count = pack_check_header_count(&bytes)?;
    let (decoded, span) = pack_check_decode_span(&bytes, ds)?;
    let (index_rows, distinct) = pack_check_index_counts(d, &b.pack)?;
    json(serde_json::json!({
        "header_count": header_count, "decoded": decoded,
        "index_rows": index_rows, "distinct": distinct,
        "commit_lo": span.map(|s| s.0), "commit_hi": span.map(|s| s.1),
    }))
}

async fn pack_check_fetch_bytes(d: &RepoDo, pack: &str) -> Result<Vec<u8>, Error> {
    let key = keys::pack(&d.repo_id()?, &PackId(pack.to_string()));
    let mut budget = ReqBudget::paid();
    budget.charge(1)?;
    let obj = d
        .bucket()?
        .inner
        .get(&key)
        .execute()
        .await?
        .ok_or_else(|| Error::Storage(format!("missing {key}")))?;
    Ok(obj
        .body()
        .ok_or_else(|| Error::Storage(format!("no body for {key}")))?
        .bytes()
        .await?)
}

fn pack_check_header_count(bytes: &[u8]) -> Result<u32, Error> {
    if bytes.len() >= 12 && bytes[..4] == *b"PACK" {
        Ok(u32::from_be_bytes(
            bytes[8..12].try_into().map_err(|_| Error::Internal("hdr".into()))?,
        ))
    } else {
        Ok(0)
    }
}

fn pack_check_decode_span(bytes: &[u8], ds: usize) -> Result<(u32, Option<(u64, u64)>), Error> {
    let end = bytes.len().saturating_sub(ds);
    let (mut decoded, mut pos) = (0u32, 12usize);
    let mut span: Option<(u64, u64)> = None;
    while pos < end {
        let (kind, _, used) = codec::decode_entry_counted(
            bytes.get(pos..).ok_or_else(|| Error::Internal("entry".into()))?,
        )?;
        if kind == Kind::Commit {
            let (lo, hi) = (pos as u64, pos.saturating_add(used) as u64);
            span = Some(match span {
                None => (lo, hi),
                Some((l, h)) => (l.min(lo), h.max(hi)),
            });
        }
        decoded = decoded.saturating_add(1);
        pos = pos.saturating_add(used);
    }
    if pos != end {
        return Err(Error::Protocol("pack entries do not fill the trailer".into()));
    }
    Ok((decoded, span))
}

fn pack_check_index_counts(d: &RepoDo, pack: &str) -> Result<(i64, i64), Error> {
    #[derive(serde::Deserialize)]
    struct N {
        n: i64,
    }
    let index_rows = d
        .q("SELECT COUNT(*) AS n FROM objects WHERE pack_id=?", vec![V::from(pack)])?
        .one::<N>()?
        .n;
    let distinct = d
        .q("SELECT COUNT(DISTINCT sha) AS n FROM objects WHERE pack_id=?", vec![V::from(pack)])?
        .one::<N>()?
        .n;
    Ok((index_rows, distinct))
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

fn note_capped(out: &mut Vec<serde_json::Value>, build: impl FnOnce() -> serde_json::Value) {
    if out.len() < FSCK_CAP {
        out.push(build());
    }
}

fn note_missing(missing: &mut Vec<serde_json::Value>, sha: &str, by: &str) {
    note_capped(missing, || serde_json::json!({ "sha": sha, "referenced_by": by }));
}

fn note_corrupt(corrupt: &mut Vec<serde_json::Value>, sha: &str, why: String) {
    note_capped(corrupt, || serde_json::json!({ "sha": sha, "why": why }));
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

    /// rows_digest: deterministic for an ordered row set, and the per-row
    /// separator keeps ["x","y"], ["xy"] and ["y","x"] distinct — the C7
    /// snapshot compare relies on both properties.
    #[test]
    fn rows_digest_properties() {
        let a = super::rows_digest(["x", "y"].into_iter());
        assert_eq!(a, super::rows_digest(["x", "y"].into_iter()));
        assert_ne!(a, super::rows_digest(["xy"].into_iter()));
        assert_ne!(a, super::rows_digest(["y", "x"].into_iter()));
        assert_ne!(a, super::rows_digest([].into_iter()));
    }
}
