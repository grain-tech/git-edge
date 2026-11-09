//! Every `js_sys::Reflect` use lives here (CONTRACTS.md A8). Nothing else in the crate may call Reflect.

use crate::error::Error;

#[cfg(test)]
pub fn now_ms() -> i64 {
    // js_sys::Date::now() panics on native targets — tests get the shared
    // deterministic clock instead (lib.rs::test_clock).
    crate::test_clock::now() as i64
}
#[cfg(not(test))]
pub fn now_ms() -> i64 {
    js_sys::Date::now() as i64
}

/// 16 random bytes as lowercase hex (repo_id, pack_id, push_id: section 8.2, 1.2).
pub fn hex16() -> Result<String, Error> {
    let crypto: web_sys::Crypto = js_sys::Reflect::get(&js_sys::global(), &"crypto".into())
        .ok()
        .and_then(|v| wasm_bindgen::JsCast::dyn_into(v).ok())
        .ok_or_else(|| Error::Internal("no crypto".into()))?;
    let mut b = [0u8; 16];
    crypto
        .get_random_values_with_u8_array(&mut b)
        .map_err(|_| Error::Internal("getRandomValues".into()))?;
    Ok(b.iter().map(|x| format!("{x:02x}")).collect())
}

/// The single permitted `ctx.id.name` read (section 8.3). Cross-check only; decides nothing.
/// worker 0.8.5 binds `ObjectId::name()` directly — no Reflect needed.
pub fn do_id_name(state: &worker::State) -> Option<String> {
    state.id().name()
}

/// Job-lifecycle datapoint (CONTRACTS.md A17), emitted by `jobs::dispatch` for every
/// job event while `GE_METRICS` is bound. Unbound costs one binding lookup and
/// returns; a failed write is dropped on the floor like `edge::metric` — metrics
/// must never fail a job.
///
/// Positional layout (query as index1/blobN/doubleN):
///   index1  = repo — "owner/repo" from `ctx.id.name`, which survives a meta wipe
///   blob1   = "job" — discriminator: request datapoints carry op here, gauges "gauge"
///   blob2   = kind — janitor | gc_mark | gc_consolidate | gc_sweep | purge_repo
///   blob3   = event — start | done | continue | reschedule | retry | dead | stale
///   blob4   = outcome — ok | retry | dead | stale
///   blob5   = error_class — `Error::class()`, "" when none (bounded cardinality)
///   double1 = attempt — 1-based attempt number of this slice
///   double2 = duration_ms — slice wall time (0 on start)
///   double3 = will_retry — 1.0 when the job runs again (continue/reschedule/retry)
pub fn job_event(
    env: &worker::Env,
    repo: &str,
    kind: &str,
    event: &str,
    outcome: &str,
    attempt: u32,
    duration_ms: i64,
    will_retry: bool,
    error_class: &str,
) {
    let Ok(ds) = env.analytics_engine("GE_METRICS") else {
        return;
    };
    let _ = worker::AnalyticsEngineDataPointBuilder::new()
        .indexes([repo])
        .blobs(["job", kind, event, outcome, error_class])
        .doubles([f64::from(attempt), duration_ms as f64, if will_retry { 1.0 } else { 0.0 }])
        .write_to(&ds);
}

/// `jobs_dead` gauge (CONTRACTS.md A18): emitted once per alarm pass while dead
/// rows exist — blob1 "gauge" / blob2 "jobs_dead", double1 the count. Combined
/// with the `dead` job_event it gives an external alarm both the trigger and
/// the per-kind detail.
pub fn jobs_dead_gauge(env: &worker::Env, repo: &str, dead: i64) {
    let Ok(ds) = env.analytics_engine("GE_METRICS") else {
        return;
    };
    let _ = worker::AnalyticsEngineDataPointBuilder::new()
        .indexes([repo])
        .blobs(["gauge", "jobs_dead"])
        .doubles([dead as f64])
        .write_to(&ds);
}
