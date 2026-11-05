//! `purge_repo` (CONTRACTS.md A20): asynchronously delete an entire repo — every
//! R2 object under `r/<repo_id>/`, every SQLite row, then DO storage itself.
//! `POST /:o/:r/_admin/delete` enqueues it; the job needs no payload.
//!
//! Resumable like the GC chain: the R2 phase pages `list` with a cursor kept in
//! `jobs.cursor` (`r2:<cursor>`), and the final phase (`wipe`) is a bounded
//! row-delete + `delete_all`. A kill anywhere replays idempotently: deletes are
//! no-ops on absent data, and `schema::migrate` recreates the tables if a
//! completed `delete_all` dropped them before the outcome span could run.
//! Fencing: the R2 keys are namespaced by the random `repo_id`, so a stale slice
//! can never touch a re-created repo's objects, and the destructive span is
//! gated on a heartbeat CAS.
//!
//! REGISTRY (A9): JobKind::PurgeRepo ("purge_repo"); no new tables, no routes.
//! R2 keys: everything under the existing `r/<repo_id>/` prefix (packs/, pending/).

use worker::{SqlStorage, SqlStorageValue as V};

use super::{heartbeat, stale_lease, Job, SliceBudget, SliceOutcome};
use crate::error::Error;
use crate::repo_do::RepoDo;
use crate::store::schema;

const PAGE: u32 = 1_000; // one list page == one delete_multiple call (<= 1000 keys)

/// Every table but `jobs` — our own row goes last so a re-sliced run can find it.
const TABLES: &[&str] = &[
    "objects", "packs", "pushes", "refs", "reflog", "tokens", "pins", "protected", "commit_parents", "push_events", "rate",
    "marked", "gc_frontier", "gc_seen", "gc_parts", "gc_tail", "meta",
];

#[derive(serde::Deserialize)]
struct N {
    #[allow(dead_code)]
    n: i64,
}

pub async fn run_slice(d: &RepoDo, job: &Job, budget: &mut SliceBudget) -> Result<SliceOutcome, Error> {
    let sql = d.sql();
    // Phase A — the R2 wipe. Skipped when the cursor says it finished, when a
    // crashed final span already removed `meta` (repo_id is the prefix source),
    // or when a completed delete_all took the schema with it.
    let have_meta = !crate::store::exec(
        &sql,
        "SELECT 1 AS n FROM sqlite_master WHERE type='table' AND name='meta'",
        vec![],
    )?
    .to_array::<N>()
    .map_err(|e| Error::Storage(e.to_string()))?
    .is_empty();
    let r2_done =
        job.cursor.as_deref() == Some("wipe") || !have_meta || d.meta_opt("repo_id")?.is_none();
    if !r2_done {
        let bucket = d.bucket()?;
        let prefix = format!("r/{}/", bucket.repo.0);
        let mut cursor = job
            .cursor
            .as_deref()
            .and_then(|c| c.strip_prefix("r2:"))
            .map(str::to_string);
        loop {
            if budget.spent_80pct() {
                let c = cursor.map_or_else(|| "r2:".to_string(), |c| format!("r2:{c}"));
                return Ok(SliceOutcome::Continue { cursor: c });
            }
            budget.req.charge(1)?;
            let mut q = bucket.inner.list().prefix(prefix.clone()).limit(PAGE);
            if let Some(c) = cursor.take() {
                q = q.cursor(c);
            }
            let page = q.execute().await?;
            let keys: Vec<String> = page.objects().iter().map(|o| o.key()).collect();
            if !keys.is_empty() {
                budget.req.charge(1)?;
                bucket
                    .inner
                    .delete_multiple(keys.iter().map(String::as_str).collect())
                    .await?;
            }
            // CAS heartbeat between pages: a stale slice (requeued after the 60 s
            // lease and reclaimed) stops here instead of wiping on. Its deletes are
            // still safe — the prefix is the dead repo_id — but the row is not ours.
            if !heartbeat(&sql, job)? {
                return Err(stale_lease());
            }
            match (page.truncated(), page.cursor()) {
                (true, Some(c)) => cursor = Some(c),
                // not truncated, or a truncated page with no cursor (never seen
                // on R2, but re-listing from the start would spin forever —
                // leftover keys become orphans under the dead prefix, same as
                // keys written after the cursor)
                _ => break,
            }
        }
    }
    wipe(d, job, &sql).await
}

/// Phase B — one bounded span: every row but our job's, the pending alarm, then
/// `delete_all` for whatever remains (KV and, on current workerd, the tables
/// themselves). Idempotent: every DELETE tolerates an empty or re-created table.
async fn wipe(d: &RepoDo, job: &Job, sql: &SqlStorage) -> Result<SliceOutcome, Error> {
    // last fence before the point of no return — a stale slice must not wipe
    // storage that a re-created repo (or a parallel purge) is already using
    if !heartbeat(sql, job)? {
        return Err(stale_lease());
    }
    // a previous attempt may have completed delete_all (schema dropped) and died
    // before the fenced Done ran — recreate so the deletes below still parse
    schema::migrate(sql)?;
    for t in TABLES {
        crate::store::exec(sql, &format!("DELETE FROM {t}"), vec![])?;
    }
    // no janitor/gc row may resurrect into a half-wiped repo — every job but this one
    crate::store::exec(sql, "DELETE FROM jobs WHERE id <> ?", vec![V::from(job.id)])?;
    // kill the pending alarm before the wipe: a re-fire after the schema is gone
    // would fail boot("no headers") and retry forever; rearm's no-queued-rows
    // delete_alarm covers the other orderings
    let _ = d.state.storage().delete_alarm().await;
    d.state.storage().delete_all().await.map_err(Error::from)?;
    // delete_all may have dropped the schema under a live `booted` flag — the next
    // fetch must re-migrate or every query fails on missing tables
    d.unboot();
    // our job row is gone with the wipe (or dies now on the fenced Done) — either
    // way nothing requeues: the next request boots a fresh, empty repo
    Ok(SliceOutcome::Done)
}
