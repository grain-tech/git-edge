//! Janitor (5.1): 15-minute cadence, contract steps 1–3. Each arm is bounded; a kill mid-slice
//! re-issues the same work next run — marking and R2 deleting never happen in one slice
//! (deletion only targets rows marked dead/not-open at least GRACE earlier).
//! NB: LIMIT on UPDATE/DELETE requires SQLITE_ENABLE_UPDATE_DELETE_LIMIT, which workerd's build
//! does not guarantee — all bounds are applied on the SELECT, mutations key by row id.

use worker::SqlStorageValue as V;

use super::{Job, SliceBudget, SliceOutcome};
use crate::error::Error;
use crate::platform;
use crate::repo_do::RepoDo;
use crate::store::{keys, PackId};

const BATCH: i64 = 400; // keys per slice (5.1: at most 400)
/// Dead-pack grace — pub(crate): the packfile-URI TTL derives from it (S9/R10).
pub(crate) const GRACE_MS: i64 = 3_600_000;
const PUSH_TIMEOUT_MS: i64 = 3_600_000;
const REFLOG_KEEP_MS: i64 = 90 * 24 * 3_600_000;

#[derive(serde::Deserialize)]
struct I {
    id: String,
}
#[derive(serde::Deserialize)]
struct N {
    id: i64,
}

pub async fn run_slice(d: &RepoDo, job: &Job, budget: &mut SliceBudget) -> Result<SliceOutcome, Error> {
    let now = platform::now_ms();

    // 1. expire open pushes past PUSH_TIMEOUT
    let stale: Vec<I> = d
        .q(
            "SELECT id FROM pushes WHERE state='open' AND began_at < ? LIMIT ?",
            vec![V::from(now.saturating_sub(PUSH_TIMEOUT_MS)), V::from(BATCH)],
        )?
        .to_array::<I>()?
        .into_iter()
        .collect();
    for p in &stale {
        d.q(
            "UPDATE pushes SET state='expired', ended_at=? WHERE id=? AND state='open'",
            vec![V::from(now), V::from(p.id.as_str())],
        )?;
    }

    // 2. ingesting packs whose push is expired/rejected -> dead + drop object rows (one span each)
    let dead_packs: Vec<I> = d
        .q(
            "SELECT p.id FROM packs p JOIN pushes u ON u.pack_id = p.id \
             WHERE p.state='ingesting' AND u.state IN ('expired','rejected') LIMIT ?",
            vec![V::from(BATCH)],
        )?
        .to_array::<I>()?
        .into_iter()
        .collect();
    for p in &dead_packs {
        d.q("DELETE FROM objects WHERE pack_id=?", vec![V::from(p.id.as_str())])?;
        d.q(
            "UPDATE packs SET state='dead', dead_at=? WHERE id=? AND state='ingesting'",
            vec![V::from(now), V::from(p.id.as_str())],
        )?;
    }
    // a pack 'ingesting' whose owning push is not open is abandoned (crash between pack
    // finish and commit, or an import job that died): dead after PUSH_TIMEOUT so step 3
    // reclaims it. The link is packs.push_id — pushes.pack_id stays NULL until commit, so
    // joining on it would mark every live multi-hour import's pack dead mid-run.
    // push_id IS NULL marks the GC consolidation build pack — its lifetime is owned by
    // the gc_consolidate job, not the janitor.
    let orphaned: Vec<I> = d
        .q(
            "SELECT id FROM packs WHERE state='ingesting' AND created_at < ? AND push_id IS NOT NULL \
             AND NOT EXISTS(SELECT 1 FROM pushes u WHERE u.id = packs.push_id AND u.state='open') LIMIT ?",
            vec![V::from(now.saturating_sub(PUSH_TIMEOUT_MS)), V::from(BATCH)],
        )?
        .to_array::<I>()?
        .into_iter()
        .collect();
    for p in &orphaned {
        d.q("DELETE FROM objects WHERE pack_id=?", vec![V::from(p.id.as_str())])?;
        d.q(
            "UPDATE packs SET state='dead', dead_at=? WHERE id=? AND state='ingesting'",
            vec![V::from(now), V::from(p.id.as_str())],
        )?;
    }

    // 3. R2 deletes: pending packs of not-open pushes older than GRACE (swept_at marks done);
    //    dead packs past GRACE whose object rows are gone.
    let pend: Vec<I> = d
        .q(
            "SELECT id FROM pushes WHERE state <> 'open' AND began_at < ? AND swept_at IS NULL \
             ORDER BY began_at LIMIT ?",
            vec![V::from(now.saturating_sub(GRACE_MS)), V::from(BATCH)],
        )?
        .to_array::<I>()?
        .into_iter()
        .collect();
    // per-key failure is logged and counted but does not wedge the phase — a single
    // poisoned R2 key must not starve every other sweep (and `swept_at` is still only
    // written after a confirmed delete)
    let mut del_fail = 0u32;
    for p in &pend {
        if budget.spent_80pct() {
            return Ok(SliceOutcome::Continue { cursor: "{}".into() });
        }
        // I1: a staged import parks N parts under `pending/<push>.` (one `.pack`
        // and any `.part-*` keys) — sweep the whole prefix, not just `.pack`.
        let prefix = format!("r/{}/pending/{}.", d.repo_id()?.0, p.id);
        let bucket = d.bucket()?;
        let mut done = true;
        let mut cur: Option<String> = None;
        for _ in 0..8 {
            // cap pages per push per slice; an unfinished prefix re-sweeps next
            // pass since swept_at stays NULL (list re-reads the remainder)
            if budget.spent_80pct() {
                done = false;
                break;
            }
            budget.req.charge(1)?;
            let mut q = bucket.inner.list().prefix(prefix.clone()).limit(100);
            if let Some(c) = cur.take() {
                q = q.cursor(c);
            }
            match q.execute().await {
                Ok(page) => {
                    let keys: Vec<String> = page.objects().iter().map(|o| o.key()).collect();
                    if !keys.is_empty() {
                        budget.req.charge(1)?;
                        if let Err(e) = bucket
                            .inner
                            .delete_multiple(keys.iter().map(String::as_str).collect())
                            .await
                        {
                            del_fail += 1;
                            worker::console_log!("janitor: pending delete {} failed: {e}", p.id);
                            done = false;
                            break;
                        }
                    }
                    match (page.truncated(), page.cursor()) {
                        (true, Some(c)) => cur = Some(c),
                        _ => break,
                    }
                }
                Err(e) => {
                    del_fail += 1;
                    worker::console_log!("janitor: pending list {} failed: {e}", p.id);
                    done = false;
                    break;
                }
            }
        }
        if done {
            // I1 staging tables die with the push: committed imports already cleaned
            // theirs (no-op), expired/rejected ones get theirs dropped here
            for t in ["push_links", "import_toc", "import_parts", "import_open", "import_tail"] {
                d.q(&format!("DELETE FROM {t} WHERE push_id=?"), vec![V::from(p.id.as_str())])?;
            }
            d.q("UPDATE pushes SET swept_at=? WHERE id=?", vec![V::from(now), V::from(p.id.as_str())])?;
        }
        // CAS heartbeat (A19): a stale slice stops before its next delete
        if !super::heartbeat(&d.sql(), job)? {
            return Err(super::stale_lease());
        }
    }
    let dead: Vec<I> = d
        .q(
            "SELECT id FROM packs WHERE state='dead' AND dead_at < ? \
             AND NOT EXISTS(SELECT 1 FROM objects o WHERE o.pack_id = packs.id) \
             ORDER BY dead_at LIMIT ?",
            vec![V::from(now.saturating_sub(GRACE_MS)), V::from(BATCH)],
        )?
        .to_array::<I>()?
        .into_iter()
        .collect();
    for p in &dead {
        if budget.spent_80pct() {
            return Ok(SliceOutcome::Continue { cursor: "{}".into() });
        }
        let key = keys::pack(&d.repo_id()?, &PackId(p.id.clone()));
        // §5: error-mode point right before the R2 delete — a retry re-takes
        // the same pack (the dead row and its TTL still stand)
        crate::testhooks::failpoint(d, "janitor.before_r2_delete")?;
        budget.req.charge(1)?;
        match d.bucket()?.inner.delete(key.as_str()).await {
            Ok(()) => {
                d.q("DELETE FROM packs WHERE id=?", vec![V::from(p.id.as_str())])?;
            }
            Err(e) => {
                del_fail += 1;
                worker::console_log!("janitor: pack delete {} failed: {e}", p.id);
            }
        }
        // CAS heartbeat (A19): a stale slice stops before its next delete
        if !super::heartbeat(&d.sql(), job)? {
            return Err(super::stale_lease());
        }
    }

    // 3b. dead import_pack jobs still own an open R2 multipart upload (their output
    // pack never completed): rebind + abort so uploaded parts don't leak, then drop
    // the job row. The push/staging cleanup above handles the rest on its own clock.
    #[derive(serde::Deserialize)]
    struct DeadJob {
        id: i64,
        payload: String,
        cursor: Option<String>,
    }
    let dead_jobs: Vec<DeadJob> = d
        .q(
            "SELECT id, payload, cursor FROM jobs WHERE kind='import_pack' AND state='dead' LIMIT ?",
            vec![V::from(BATCH)],
        )?
        .to_array::<DeadJob>()?
        .into_iter()
        .collect();
    for j in dead_jobs {
        if budget.spent_80pct() {
            return Ok(SliceOutcome::Continue { cursor: "{}".into() });
        }
        let pack = serde_json::from_str::<serde_json::Value>(&j.payload)
            .ok()
            .and_then(|v| v.get("pack").and_then(|p| p.as_str()).map(String::from));
        let upload = j
            .cursor
            .as_deref()
            .and_then(|c| serde_json::from_str::<serde_json::Value>(c).ok())
            .and_then(|v| v.get("upload").and_then(|u| u.as_str()).map(String::from))
            .filter(|u| !u.is_empty());
        if let (Some(pack), Some(upload)) = (pack, upload) {
            let key = keys::pack(&d.repo_id()?, &PackId(pack));
            if let Ok(mpu) = d.bucket()?.inner.resume_multipart_upload(&key, &upload) {
                budget.req.charge(1)?;
                if let Err(e) = mpu.abort().await {
                    del_fail += 1;
                    worker::console_log!("janitor: import mpu abort {} failed: {e}", j.id);
                    continue; // keep the row — retry next run
                }
            }
        }
        d.q("DELETE FROM jobs WHERE id=?", vec![V::from(j.id)])?;
    }

    // 4. reflog expiry: 90 days, keyed by row id.
    let old_log: Vec<N> = d
        .q(
            "SELECT id FROM reflog WHERE at < ? LIMIT ?",
            vec![V::from(now.saturating_sub(REFLOG_KEEP_MS)), V::from(BATCH)],
        )?
        .to_array::<N>()?
        .into_iter()
        .collect();
    for r in &old_log {
        d.q("DELETE FROM reflog WHERE id=?", vec![V::from(r.id)])?;
    }

    // G4a backstop: the same cohort check the push path runs fires at every
    // janitor pass, for repos that went quiet right after a burst (§3b) —
    // best-effort, a trigger hiccup must not fail the pass itself
    if let Err(e) = super::compact::trigger(d) {
        worker::console_log!("janitor: compact trigger: {e}");
    }

    // failed deletes stay unmarked and retry next run — but the job records the error
    // so a persistently-poisoned key is visible rather than silently stuck
    if del_fail > 0 {
        return Err(Error::Storage(format!("{del_fail} R2 deletes failed")));
    }
    Ok(SliceOutcome::Done)
}
