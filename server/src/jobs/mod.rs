//! Job dispatcher (CONTRACTS.md 4, 5; A3, A4) + running-row repair (A12).
//! `rearm` is the sole `set_alarm` caller; `enqueue` is sync and only writes a row.

pub mod compact;
pub mod events;
pub mod gc;
pub mod import;
pub mod janitor;
pub mod purge;

use worker::{SqlStorage, SqlStorageValue as V};

use crate::error::Error;
use crate::platform;
use crate::repo_do::RepoDo;
use crate::store::exec;
use crate::ReqBudget;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum JobKind {
    Janitor,
    GcMark,
    GcConsolidate,
    GcSweep,
    GcCompact,
    PurgeRepo,
    ImportPack,
    PushEvent,
}
impl JobKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            JobKind::Janitor => "janitor",
            JobKind::GcMark => "gc_mark",
            JobKind::GcConsolidate => "gc_consolidate",
            JobKind::GcSweep => "gc_sweep",
            JobKind::GcCompact => "gc_compact",
            JobKind::PurgeRepo => "purge_repo",
            JobKind::ImportPack => "import_pack",
            JobKind::PushEvent => "push_event",
        }
    }
    pub fn of(s: &str) -> Result<Self, Error> {
        match s {
            "janitor" => Ok(JobKind::Janitor),
            "gc_mark" => Ok(JobKind::GcMark),
            "gc_consolidate" => Ok(JobKind::GcConsolidate),
            "gc_sweep" => Ok(JobKind::GcSweep),
            "gc_compact" => Ok(JobKind::GcCompact),
            "purge_repo" => Ok(JobKind::PurgeRepo),
            "import_pack" => Ok(JobKind::ImportPack),
            "push_event" => Ok(JobKind::PushEvent),
            k => Err(Error::Internal(format!("unknown job kind {k}"))),
        }
    }
}

pub struct Job {
    pub id: i64,
    pub kind: JobKind,
    pub run_at: i64,
    pub attempts: u32,
    pub cursor: Option<String>,
    pub payload: String,
    /// The fencing token `dispatch` wrote at claim time. Heartbeats CAS on it
    /// (A19): a slice whose row was requeued as stranded and reclaimed no longer
    /// matches, and must stop writing before the fenced outcome span runs.
    pub lease: String,
}

pub enum SliceOutcome {
    Done,
    Continue { cursor: String },
    Reschedule { run_at: i64 },
}

/// Slice budget (4.3): 20,000 ms wall clock and 400 subrequests; Continue at 80%.
pub struct SliceBudget {
    pub started_ms: f64,
    pub req: ReqBudget,
    /// Soft yield point in subrequests (default 80% of 400): spent_80pct's
    /// threshold. NOT the hard cap — lowering the cap would turn the yield
    /// into a Budget error instead of a Continue.
    yield_after: u32,
}
impl SliceBudget {
    pub fn fresh(d: &RepoDo) -> Self {
        // slice_subreqs knob (testhooks): pull the yield forward so a job
        // yields Continue early — the hard 400 cap never moves
        let yield_after = crate::testhooks::knob(d, "slice_subreqs")
            .and_then(|v| u32::try_from(v).ok())
            .unwrap_or(320)
            .min(320);
        Self {
            started_ms: js_sys::Date::now(),
            req: ReqBudget { max_subrequests: 400, used: 0, started_ms: js_sys::Date::now(), max_ms: 20_000.0, sink: None },
            yield_after,
        }
    }
    pub fn spent_80pct(&self) -> bool {
        let elapsed = js_sys::Date::now() - self.started_ms;
        elapsed > 16_000.0 || self.req.used > self.yield_after
    }
}

/// A checkpoint span refreshes the lease: `repair` may only requeue a row whose started_at
/// has gone stale — a progressing slice heartbeats and is never mistaken for a stranded one.
///
/// A19: the heartbeat is a CAS on the fencing token, not a bare `id` match. A slice
/// whose row was repair-requeued (`state='queued'`) or reclaimed under a fresh lease
/// (`lease` changed) gets `false` — it must stop immediately via `stale_lease()`,
/// because every write it still had queued belongs to the new lease-holder, and an
/// unconditional `started_at` bump would mask a genuinely stalled new owner from
/// `repair`.
pub fn heartbeat(sql: &SqlStorage, job: &Job) -> Result<bool, Error> {
    exec(
        sql,
        "UPDATE jobs SET started_at=? WHERE id=? AND state='running' AND lease=?",
        vec![
            V::from(platform::now_ms()),
            V::from(job.id),
            V::from(job.lease.as_str()),
        ],
    )?;
    #[derive(serde::Deserialize)]
    struct N {
        n: i64,
    }
    exec(sql, "SELECT changes() AS n", vec![])?
        .one::<N>()
        .map(|r| r.n == 1)
        .map_err(|e| Error::Storage(e.to_string()))
}

/// The read-only half of the lease fence: true while the row is 'running'
/// under our token. Sufficient inside one sync span — nothing can interleave
/// between the SELECT and the writes that follow it, so a mid-span write
/// batch (JobSink) needs no CAS; heartbeat's UPDATE additionally refreshes
/// started_at for repair visibility, which a fenced-out slice doesn't want.
pub fn lease_held(sql: &SqlStorage, job: &Job) -> Result<bool, Error> {
    #[derive(serde::Deserialize)]
    struct N {
        n: i64,
    }
    // to_array, never one() — a zero-row cursor throws in workers-rs; COUNT(*)
    // always returns exactly one row so the field carries the verdict
    exec(
        sql,
        "SELECT COUNT(*) AS n FROM jobs WHERE id=? AND state='running' AND lease=?",
        vec![V::from(job.id), V::from(job.lease.as_str())],
    )?
    .to_array::<N>()
    .map(|rows| rows.first().map(|r| r.n > 0).unwrap_or(false))
    .map_err(|e| Error::Storage(e.to_string()))
}

/// Returned by a slice whose heartbeat CAS failed: the row belongs to another
/// lease now. The fenced outcome span no-ops on it, `attempts` is not consumed,
/// and dispatch reports the event as `stale`, never a retry.
pub fn stale_lease() -> Error {
    Error::Internal("stale job lease".into())
}

/// Sync; writes the row only (A3). Dedups against 'queued' rows: at most one queued row per kind.
/// Deduping against 'running' too would suppress a job's own re-enqueue (edge review), so a running
/// row does not block a new enqueue — the next dispatch simply runs the kind again, which is
/// idempotent by construction.
pub fn enqueue(sql: &SqlStorage, kind: JobKind, run_at: i64, payload: &str) -> Result<(), Error> {
    exec(
        sql,
        "INSERT INTO jobs(kind,run_at,payload) SELECT ?,?,? \
         WHERE NOT EXISTS(SELECT 1 FROM jobs WHERE kind=? AND state='queued')",
        vec![V::from(kind.as_str()), V::from(run_at), V::from(payload), V::from(kind.as_str())],
    )?;
    Ok(())
}

/// The only `set_alarm` in the crate (4.1). Alarm = MIN(run_at) over queued rows, or deleted.
pub async fn rearm(d: &RepoDo) -> Result<(), Error> {
    #[derive(serde::Deserialize)]
    struct R {
        run_at: i64,
    }
    let next = d
        .q("SELECT run_at FROM jobs WHERE state='queued' ORDER BY run_at LIMIT 1", vec![])?
        .to_array::<R>()?
        .into_iter()
        .next();
    match next {
        Some(r) => {
            // set_alarm interprets i64/Duration as an *offset from now*; jobs.run_at is an
            // absolute epoch-ms timestamp, so it must go through a Date (ScheduledTime::new).
            let when = js_sys::Date::new(&js_sys::Number::from(r.run_at as f64));
            d.state.storage().set_alarm(worker::ScheduledTime::new(when)).await?
        }
        None => d.state.storage().delete_alarm().await?,
    }
    Ok(())
}

/// A4 + A12, called from boot: re-enqueue dead maintenance jobs, and requeue 'running' rows a
/// killed isolate stranded (a slice cannot legally exceed ~20 s; 60 s is generous).
pub fn repair(sql: &SqlStorage) -> Result<(), Error> {
    let now = platform::now_ms();
    // A stranded 'running' row is a crashed isolate, not a job error — it counts
    // against `strands`, not `attempts`, so a legitimately long multi-slice job
    // survives rebuild/restart churn. A crash-looping job still dies: strands
    // only reset when a slice completes, so 64 consecutive dead isolates means
    // the slice itself is what kills them.
    exec(
        sql,
        "UPDATE jobs SET state='dead', last_error='stranded: isolate died 64x without a completed slice' \
         WHERE state='running' AND started_at < ? AND strands >= 64",
        vec![V::from(now.saturating_sub(60_000))],
    )?;
    exec(
        sql,
        "UPDATE jobs SET state='queued', run_at=?, strands=strands+1, last_error='stranded mid-slice', \
         lease=NULL WHERE state='running' AND started_at < ? AND strands < 64",
        vec![V::from(now), V::from(now.saturating_sub(60_000))],
    )?;
    // a dead or missing Janitor is re-enqueued at every boot (A4)
    #[derive(serde::Deserialize)]
    struct N {
        n: i64,
    }
    let janitors = exec(
        sql,
        "SELECT COUNT(*) AS n FROM jobs WHERE kind='janitor' AND state IN ('queued','running')",
        vec![],
    )?
    .one::<N>()
    .map_err(|e| Error::Storage(e.to_string()))?;
    if janitors.n == 0 {
        enqueue(sql, JobKind::Janitor, now, "{}")?;
    }
    for kind in [JobKind::GcMark, JobKind::GcConsolidate, JobKind::GcSweep, JobKind::GcCompact] {
        // dead maintenance jobs of a chain that may have been mid-flight resume via re-enqueue (A4)
        let n = exec(
            sql,
            "SELECT COUNT(*) AS n FROM jobs WHERE kind=? AND state='dead'",
            vec![V::from(kind.as_str())],
        )?
        .one::<N>()
        .map_err(|e| Error::Storage(e.to_string()))?;
        if n.n > 0 {
            // keep the last failure inspectable — the rows are deleted and a fresh job
            // has no error context otherwise
            #[derive(serde::Deserialize)]
            struct Dead {
                last_error: Option<String>,
            }
            let err = exec(
                sql,
                "SELECT last_error FROM jobs WHERE kind=? AND state='dead' AND last_error IS NOT NULL \
                 ORDER BY id DESC LIMIT 1",
                vec![V::from(kind.as_str())],
            )?
            .to_array::<Dead>()
            .map_err(|e| Error::Storage(e.to_string()))?
            .into_iter()
            .next()
            .and_then(|d| d.last_error);
            if let Some(e) = err {
                exec(
                    sql,
                    "INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    vec![V::from(format!("dead.{}", kind.as_str())), V::from(e)],
                )?;
            }
            // bound resurrection: a deterministically failing chain (corrupt pack,
            // wedge) would otherwise restart at every boot forever — a ~400-subrequest
            // slice of doomed work each time. resurrected.<kind> counts restarts and
            // resets when a chain really completes (the sweep's delete span clears
            // it — a stray or no-op sweep Done doesn't). T4: under gc.mode=compact a
            // resurrected builder charges the compact chain's own counter — the swap
            // resets only that one.
            let key = if kind == JobKind::GcConsolidate
                && exec(
                    sql,
                    "SELECT value FROM meta WHERE key='gc.mode'",
                    vec![],
                )?
                .to_array::<M>()?
                .into_iter()
                .next()
                .map(|m| m.value == "compact")
                .unwrap_or(false)
            {
                "resurrected.gc_compact".to_string()
            } else {
                format!("resurrected.{}", kind.as_str())
            };
            #[derive(serde::Deserialize)]
            struct M {
                value: String,
            }
            let tries = exec(sql, "SELECT value FROM meta WHERE key=?", vec![V::from(key.as_str())])?
                .to_array::<M>()
                .map_err(|e| Error::Storage(e.to_string()))?
                .into_iter()
                .next()
                .and_then(|m| m.value.parse::<i64>().ok())
                .unwrap_or(0);
            if tries >= 3 {
                continue; // leave the dead rows and dead.<kind> for inspection
            }
            exec(sql, "DELETE FROM jobs WHERE kind=? AND state='dead'", vec![V::from(kind.as_str())])?;
            enqueue(sql, kind, now, "{}")?;
            exec(
                sql,
                "INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                vec![V::from(key.as_str()), V::from((tries + 1).to_string())],
            )?;
        }
    }
    Ok(())
}

/// Which queued row a dispatch claims (4.2): the alarm honours run_at; the test
/// step ignores it and may pin a kind, so a backoff-scheduled retry still runs
/// on demand. One dispatch_inner, two selections — no duplicated dispatcher.
pub enum Pick {
    Due,
    Any { kind: Option<JobKind> },
}

/// What one dispatched slice did — the /_do/test/step hook reports it verbatim;
/// the alarm path discards it (its observability is job_event + the console log).
pub struct Report {
    pub job_id: i64,
    pub kind: &'static str,
    /// done | continue | reschedule | retry | dead | crashed | stale
    pub outcome: &'static str,
    pub error: Option<String>,
}

/// alarm() -> dispatch (4.2): one slice per firing. Never lets an error escape (4.4).
pub async fn dispatch(d: &RepoDo) -> Result<(), Error> {
    let r = dispatch_inner(d, Pick::Due).await.map(|_| ());
    if let Err(e) = &r {
        worker::console_log!("jobs::dispatch: {e}");
    }
    // A18: `jobs_dead > 0` is the wedge signal — one gauge datapoint per alarm
    // pass while any row is dead, so an external alarm does not need _state polls.
    dead_gauge(d);
    let _ = rearm(d).await;
    r
}

/// The /_do/test/step hook (testhooks.rs, GE_TEST_HOOKS-gated): EXACTLY one
/// slice through dispatch_inner — identical machinery to the alarm, run_at
/// ignored. rearm keeps the alarm honest about the queue it left.
pub async fn step(d: &RepoDo, kind: Option<JobKind>) -> Result<Option<Report>, Error> {
    let r = dispatch_inner(d, Pick::Any { kind }).await;
    let _ = rearm(d).await;
    r
}

/// A18 dead-job gauge: high-cardinality-safe (repo index, fixed blobs, one double).
/// Query errors — e.g. a `purge_repo` slice dropped the schema — are ignored.
fn dead_gauge(d: &RepoDo) {
    #[derive(serde::Deserialize)]
    struct N {
        n: i64,
    }
    let dead = d
        .q("SELECT COUNT(*) AS n FROM jobs WHERE state='dead'", vec![])
        .ok()
        .and_then(|c| c.one::<N>().ok())
        .map(|r| r.n)
        .unwrap_or(0);
    if dead > 0 {
        let repo = platform::do_id_name(&d.state).unwrap_or_else(|| "-".into());
        platform::jobs_dead_gauge(&d.env, &repo, dead);
    }
}

#[derive(serde::Deserialize)]
struct JobRow {
    id: i64,
    kind: String,
    run_at: i64,
    attempts: i64,
    cursor: Option<String>,
    payload: String,
}

/// The queued row a Pick claims — the only part of dispatch that differs
/// between the alarm (due rows) and the step hook (any queued row).
fn pick_row(d: &RepoDo, pick: Pick) -> Result<Option<JobRow>, Error> {
    const SEL: &str = "SELECT id,kind,run_at,attempts,cursor,payload FROM jobs WHERE state='queued'";
    let cur = match pick {
        Pick::Due => d.q(
            &format!("{SEL} AND run_at <= ? ORDER BY run_at, id LIMIT 1"),
            vec![V::from(platform::now_ms())],
        )?,
        Pick::Any { kind: None } => {
            d.q(&format!("{SEL} ORDER BY run_at, id LIMIT 1"), vec![])?
        }
        Pick::Any { kind: Some(k) } => d.q(
            &format!("{SEL} AND kind=? ORDER BY run_at, id LIMIT 1"),
            vec![V::from(k.as_str())],
        )?,
    };
    Ok(cur.to_array::<JobRow>()?.into_iter().next())
}

async fn dispatch_inner(d: &RepoDo, pick: Pick) -> Result<Option<Report>, Error> {
    let Some(r) = pick_row(d, pick)? else { return Ok(None) };
    let now = platform::now_ms();
    // mark running + record started_at (A12) + a lease token in one sync span. The
    // lease survives heartbeats (which rewrite started_at) and fences the outcome
    // span: a slice whose row was repair-requeued and reclaimed by another dispatch
    // can no longer mutate the row when it finally finishes.
    let lease = platform::hex16()?;
    d.q(
        "UPDATE jobs SET state='running', started_at=?, lease=? WHERE id=? AND state='queued'",
        vec![V::from(now), V::from(lease.as_str()), V::from(r.id)],
    )?;
    if d.changes()? != 1 {
        return Ok(None); // someone else took it; impossible inside a span, but harmless
    }
    let job = Job {
        id: r.id,
        kind: JobKind::of(&r.kind)?,
        run_at: r.run_at,
        attempts: u32::try_from(r.attempts).unwrap_or(0),
        cursor: r.cursor.clone(),
        payload: r.payload.clone(),
        lease: lease.clone(),
    };
    // A17 metrics: the repo label comes from ctx.id.name, not meta — a purge_repo
    // slice wipes meta mid-flight and the label must survive it.
    let repo = platform::do_id_name(&d.state).unwrap_or_else(|| "-".into());
    let attempt = job.attempts.saturating_add(1);
    platform::job_event(&d.env, &repo, job.kind.as_str(), "start", "ok", attempt, 0, false, "");
    let mut budget = SliceBudget::fresh(d);
    let out = run_slice(d, &job, &mut budget).await;
    // a "crash" failpoint is a simulated isolate death (testhooks.rs): return
    // now, WITHOUT the outcome span — the row stays 'running' holding its lease
    // and every earlier write persists, exactly what a killed isolate leaves.
    if let Some(rep) = out.as_ref().err().and_then(|e| crash_report(d, e, &job)) {
        return Ok(Some(rep));
    }
    let dur_ms = (js_sys::Date::now() - budget.started_ms).max(0.0) as i64;
    worker::console_log!("job {}#{} attempts={} -> {}", job.kind.as_str(), job.id, job.attempts,
        match &out { Ok(SliceOutcome::Done) => "done".into(),
            Ok(SliceOutcome::Continue { .. }) => "continue".into(),
            Ok(SliceOutcome::Reschedule { .. }) => "reschedule".into(),
            Err(e) => format!("err {e}") });
    land_outcome(d, &job, &lease, &repo, attempt, dur_ms, now, out).map(Some)
}

/// A "crash" failpoint's report — dispatch_inner returns it WITHOUT landing an
/// outcome: the row stays 'running' holding its lease. The hooks flag must be
/// on too — a production error that happens to start with the crash marker is
/// an ordinary retryable error, never a simulated death.
fn crash_report(d: &RepoDo, e: &Error, job: &Job) -> Option<Report> {
    if crate::testhooks::enabled(d) && crate::testhooks::is_crash(e) {
        Some(Report {
            job_id: job.id,
            kind: job.kind.as_str(),
            outcome: "crashed",
            error: Some(e.to_string()),
        })
    } else {
        None
    }
}

/// Apply the outcome in a sync span (4.2, 4.4) — fenced on our lease: if `repair`
/// requeued this row mid-slice and another dispatch claimed it (started_at changed),
/// this slice is stale and must not mutate the row or run Done-side-effects — a
/// concurrent gc_mark sibling would otherwise lose its bitmap updates.
fn land_outcome(
    d: &RepoDo,
    job: &Job,
    lease: &str,
    repo: &str,
    attempt: u32,
    dur_ms: i64,
    now: i64,
    out: Result<SliceOutcome, Error>,
) -> Result<Report, Error> {
    let fenced = |q: &str, mut args: Vec<V>| -> Result<bool, Error> {
        args.push(V::from(job.id));
        args.push(V::from(lease));
        match d.q(q, args) {
            Ok(_) => Ok(d.changes()? == 1),
            // a purge_repo slice drops the schema under its own outcome span —
            // "no such table" means the row is gone for good: not landed, not an
            // error worth logging on every successful purge
            Err(e) if e.message().contains("no such table") => Ok(false),
            Err(e) => Err(e),
        }
    };
    let emit = |event: &str, outcome: &str, will_retry: bool, class: &str| {
        platform::job_event(&d.env, repo, job.kind.as_str(), event, outcome, attempt, dur_ms, will_retry, class);
    };
    // a fenced write that lands nothing means this slice lost the row mid-flight —
    // report `stale`, never a retry (the new owner's attempt must not consume ours)
    let mut outcome = "stale";
    let mut error = None;
    match out {
        Ok(SliceOutcome::Done) => {
            if fenced("DELETE FROM jobs WHERE id=? AND state='running' AND lease=?", vec![])? {
                if job.kind == JobKind::Janitor {
                    enqueue(&d.sql(), JobKind::Janitor, now + 15 * 60 * 1000, "{}")?;
                }
                emit("done", "ok", false, "");
                outcome = "done";
            } else {
                emit("done", "stale", false, "");
            }
        }
        Ok(SliceOutcome::Continue { cursor }) => {
            if fenced(
                "UPDATE jobs SET state='queued', run_at=?, cursor=?, strands=0, lease=NULL WHERE id=? AND state='running' AND lease=?",
                vec![V::from(now), V::from(cursor.as_str())],
            )? {
                emit("continue", "ok", true, "");
                outcome = "continue";
            } else {
                emit("continue", "stale", false, "");
            }
        }
        Ok(SliceOutcome::Reschedule { run_at }) => {
            if fenced(
                "UPDATE jobs SET state='queued', run_at=?, cursor=NULL, strands=0, lease=NULL WHERE id=? AND state='running' AND lease=?",
                vec![V::from(run_at)],
            )? {
                emit("reschedule", "ok", true, "");
                outcome = "reschedule";
            } else {
                emit("reschedule", "stale", false, "");
            }
        }
        Err(e) => {
            error = Some(e.to_string());
            let backoff = (30_000i64).saturating_mul(1i64.checked_shl(attempt.min(20)).unwrap_or(1 << 20));
            let next = now.saturating_add(backoff.min(3_600_000));
            if attempt >= 8 {
                if fenced(
                    "UPDATE jobs SET state='dead', last_error=? WHERE id=? AND state='running' AND lease=?",
                    vec![V::from(e.to_string().as_str())],
                )? {
                    emit("dead", "dead", false, e.class());
                    outcome = "dead";
                } else {
                    emit("dead", "stale", false, e.class());
                }
            } else if fenced(
                "UPDATE jobs SET state='queued', attempts=?, last_error=?, run_at=?, lease=NULL WHERE id=? AND state='running' AND lease=?",
                vec![V::from(i64::from(attempt)), V::from(e.to_string().as_str()), V::from(next)],
            )? {
                emit("retry", "retry", true, e.class());
                outcome = "retry";
            } else {
                emit("retry", "stale", false, e.class());
            }
        }
    }
    Ok(Report {
        job_id: job.id,
        kind: job.kind.as_str(),
        outcome,
        error,
    })
}

/// One slice of the job (4.5 dispatch arms).
pub async fn run_slice(d: &RepoDo, job: &Job, budget: &mut SliceBudget) -> Result<SliceOutcome, Error> {
    match job.kind {
        JobKind::Janitor => janitor::run_slice(d, job, budget).await,
        JobKind::GcMark => gc::gc_mark(d, job, budget).await,
        JobKind::GcConsolidate => gc::gc_consolidate(d, job, budget).await,
        JobKind::GcSweep => gc::gc_sweep(d, job).await,
        JobKind::GcCompact => compact::gc_compact(d, job),
        JobKind::PurgeRepo => purge::run_slice(d, job, budget).await,
        JobKind::ImportPack => import::run_slice(d, job, budget).await,
        JobKind::PushEvent => events::run_slice(d, job, budget).await,
    }
}
