# Push-triggered CI as a DO alarm chain

> Second pass · Idea #14 · verdict: **lands with caveats** · feasibility 4/5 · reliability 4/5 · correctness 4/5 (first pass 4/3/3)
> First pass: [proof](./archived/alarm-chain-ci.md) · [review](../reviews/archived/alarm-chain-ci.md) · Second pass: [review](../reviews/alarm-chain-ci.md) · Contract: [CONTRACTS.md](../../CONTRACTS.md)

## Mechanism
Section 12 puts "CI chains" outside the foundation, so this is a post-foundation module (A9). Under the contract the first pass's shape is impossible on purpose: there is one DO per repo, one alarm per DO, and `jobs::rearm` is the only `set_alarm` caller (4.1, A3; a second `setAlarm` cancels the first, measured #5). The per-`owner/repo@sha` `CiRun` DO, its private SQLite and its own alarm all collapse into the `jobs` dispatcher of section 4 — "every future background feature registers a new `JobKind` variant and a `run_slice` arm; none may set the alarm" (4.5). What survives of the idea, and is written here: (a) `ci::schedule`, a new step 8 inside `commit_push`'s sync span (section 3) that writes the run outbox — `ci_runs` + `ci_stages` rows plus `jobs::enqueue(CiRun)` and `enqueue(CiTick)` — exactly the fix the review named ("insert an outbox row in the repo DO's transaction alongside the ref flip"); (b) `ci::run_slice`, the shared driver arm for both kinds: claim a stage (sync span), execute it through the `STAGE_WORKER` service binding or publish it (the only awaits), record the result under an attempt token (sync span), loop while `SliceBudget` holds (A4); (c) the verdict is still a fetchable blob under `refs/ci/<sha>`, now produced by the normal machinery — a one-blob normalized pack through `PackWriter` (2.1, 2.2), an `objects` row, a `refs` upsert and a `refs_version` bump — so `git ls-remote 'refs/ci/*'` and `git fetch origin refs/ci/<sha>` work through the foundation's own paths (`want <blob>` is accepted, scenario 10). Stage code still runs in a bound Worker (first-pass limit kept); the DO sends it `{repo, sha, stage, attempt}` and the stage reads objects through the public `git-upload-pack` endpoint. Honest weakening, stated up front: the first pass gave every run its own DO and alarm, hence unbounded per-run parallelism; here all of a repo's CI drains through at most two job rows on the single repo DO — per-run stage order is preserved (claim predicate), cross-run concurrency is at most two, and a stage's wall clock is the slice's (20 s / 400 subrequests, 4.3), not a whole alarm invocation's.

## Primitives
- `jobs::{enqueue, dispatch, SliceOutcome, SliceBudget::spent_80pct}` (section 4, A3, A4): `enqueue` is sync, dedups by kind, never arms; `rearm` after the span is the only `set_alarm` (second `setAlarm` cancels the first, measured #5). `Reschedule` clears the cursor (A4).
- `SqlStorage::exec` synchronous + `SELECT changes()` as the CAS oracle: verified (memo section 1, spike), measured 1 / 0 / 1 (platform-facts #1). `rows_written` never read.
- Sync-span atomicity for `schedule`, `claim`, `record` and publish's final span: measured (platform-facts #4); the same fact is why a second driver slice *can* interleave during `execute`'s fetch await — handled by the attempt token, below.
- `Env::fetcher` + `Fetcher::fetch_with_request` (service bindings, `worker` 0.8.5): supported per memo section 1; called **from inside a DO** is unverified at runtime (the spike exercised stub and edge fetches only). Counts against the DO's subrequest budget, which local workerd does not enforce (#7).
- `Request::new_with_init` + `RequestInit::{with_method, with_body}` for the stage POST: present in 0.8.5 source, unverified at runtime (same caveat as two-phase-push).
- `PackWriter::{create, append_entry, finish}` inside the DO for the verdict pack: the same path `gc_consolidate` uses (5.2); A1 gives them `budget: &mut ReqBudget` and `Result` returns. Real-R2 multipart measured on the local simulator only (#6).
- `gix_object::compute_hash(Kind::Blob, data)` for the verdict sha: verified (memo section 3; `compute_hash` ran on workerd in the spike).
- `Index::lookup` (2.3) for the tip-kind check in `schedule`: the only way a sha is resolved; sees the push's pack because step 3 flipped it `live` in the same span.
- `js_sys::Date::now()` for `started_at`, `created_at`, `run_at`: verified; allowed inside sync spans.
- Alarm at-least-once retry on throw: relied on only as backstop; liveness comes from the stage lease and the independent `CiTick` row, not from the platform.

## Proof code
```rust
// src/jobs/ci.rs + one jobs::run_slice arm + one call in RepoDo::commit_push (new step 8, section 3).
// CONTRACTS.md 1.2-1.4, 3, 4 (A3/A4), 5, 7, 9, 10. worker 0.8.5, gix-object 0.64.1.
// REGISTRY (A9): JobKind CiRun + CiTick (jobs arm -> ci::run_slice); tables ci_runs + ci_stages (boot schema
// bump); route POST /_do/ci/rerun {sha}; no new R2 prefix (verdict = ordinary packs/ key, 2.2); meta key
// ci.stages (JSON stage names, absent = publish only); binding STAGE_WORKER. `q`/`sql`/`changes`/`meta`/`bucket`/
// `now_ms`/`oid` are the repo-do-ref-authority helpers; RepoDo.env is pub(crate) like RepoDo.state (write-back).
use worker::{Method, Request, RequestInit, SqlStorageValue as V};
use crate::{error::Error, jobs::{self, Job, JobKind, SliceBudget, SliceOutcome},
            repo_do::RepoDo, store::{keys, Index, PackId, PackWriter}};
const MAX_ATTEMPTS: i64 = 3;
const LEASE_MS: i64 = 90_000;      // stage lease; kept above the stage fetch timeout (review caveat)
const TICK_MS: i64 = 90_000;       // driver cadence while any stage is non-terminal
const PUBLISH: &str = "__publish"; // terminal stage at seq 255: verdict blob + refs/ci/<sha> upsert
// ci_runs(sha TEXT PRIMARY KEY, ref, push_id, status, created_at) WITHOUT ROWID
// ci_stages(run_sha REFERENCES ci_runs(sha), seq, name, status DEFAULT 'pending',  -- pending|running|ok|fail|skipped
//           attempts DEFAULT 0, started_at, log, PRIMARY KEY(run_sha, seq)) WITHOUT ROWID
/// Section 3 step 8, inside commit_push's sync span: the review's outbox — run rows exist iff the refs moved.
/// `moved` = ok commands with non-null new. One row per sha: idFromName(repo@sha) became ON CONFLICT on the PK.
pub fn schedule(d: &RepoDo, push: &str, moved: &[(String, gix_hash::ObjectId)], now: i64) -> Result<(), Error> {
    let sql = d.sql();
    let stages: Vec<String> = d.q("SELECT value FROM meta WHERE key='ci.stages'", vec![])?.to_array::<Val>()?
        .into_iter().next().and_then(|v| serde_json::from_str(&v.value).ok()).unwrap_or_default();
    for (refname, tip) in moved {
        if refname.starts_with("refs/ci/") { continue; }                           // a verdict ref never re-triggers
        let kind = Index(&sql).lookup(&[*tip])?.into_iter().flatten().next().map(|l| l.kind);
        if kind != Some(gix_object::Kind::Commit) { continue; }                    // CI runs on commits only
        let sha = tip.to_string();
        d.q("INSERT INTO ci_runs(sha,ref,push_id,status,created_at) VALUES(?,?,?,'running',?) ON CONFLICT(sha) DO NOTHING",
            vec![sha.as_str().into(), refname.as_str().into(), push.into(), now.into()])?;
        if d.changes()? == 0 { continue; }                                         // same sha, other ref: shared run
        for (seq, name) in stages.iter().enumerate() {
            d.q("INSERT INTO ci_stages(run_sha,seq,name) VALUES(?,?,?)",
                vec![sha.as_str().into(), (seq as i64).into(), name.as_str().into()])?;
        }
        d.q("INSERT INTO ci_stages(run_sha,seq,name) VALUES(?,255,?)", vec![sha.as_str().into(), PUBLISH.into()])?;
        jobs::enqueue(&sql, JobKind::CiRun, now, "{}")?;                           // prompt driver; dedup is safe: drainers
        jobs::enqueue(&sql, JobKind::CiTick, now + TICK_MS, "{}")?;                // independent row, covers a stuck CiRun
    }
    Ok(())   // the commit route's post-span jobs::rearm (A3, already required for step 7's GcMark) arms the alarm
}
/// jobs::run_slice arm for JobKind::{CiRun, CiTick}: claim (sync) -> execute (await) -> record (sync), looped
/// while the slice budget holds (A4). Never calls set_alarm.
pub async fn run_slice(d: &RepoDo, job: &Job, budget: &mut SliceBudget) -> Result<SliceOutcome, Error> {
    loop {
        let Some(st) = claim(d, job, now_ms())? else {
            return Ok(if nonterminal(d)? { SliceOutcome::Reschedule { run_at: now_ms() + TICK_MS } }
                      else { SliceOutcome::Done });          // all runs terminal: dispatcher deletes this job row
        };
        let res = execute(d, &st, budget).await;               // the only awaits; the input gate is open (#4)
        record(d, &st, res, now_ms())?;                        // fresh span, attempt-token CAS
        if budget.spent_80pct() { return Ok(SliceOutcome::Continue { cursor: String::new() }); }
    }
}
struct Stage { run_sha: String, seq: i64, name: String, token: i64 }   // token = the attempts value this claim wrote
#[derive(serde::Deserialize)] struct SRow { run_sha: String, seq: i64, name: String, attempts: i64 }
/// One sync span: reclaim expired stage leases, repair a driver row orphaned by a killed slice, claim one stage.
fn claim(d: &RepoDo, job: &Job, now: i64) -> Result<Option<Stage>, Error> {
    let sql = d.sql();
    d.q("UPDATE ci_stages SET status='pending' WHERE status='running' AND started_at < ?",
        vec![(now - LEASE_MS).into()])?;                                 // mid-stage isolate death resumes here
    d.q("UPDATE jobs SET state='queued', run_at=? WHERE kind IN ('ci_run','ci_tick') AND state='running' AND id<>? \
         AND NOT EXISTS(SELECT 1 FROM ci_stages WHERE status='running' AND started_at > ?)",
        vec![now.into(), V::from(job.id), (now - LEASE_MS).into()])?;    // a live driver always holds a fresh lease
    let Some(r) = d.q("SELECT s.run_sha, s.seq, s.name, s.attempts FROM ci_stages s WHERE s.status='pending' \
                      AND NOT EXISTS(SELECT 1 FROM ci_stages e WHERE e.run_sha=s.run_sha AND e.seq<s.seq \
                                     AND e.status IN ('pending','running')) ORDER BY s.run_sha, s.seq LIMIT 1",
                     vec![])?.to_array::<SRow>()?.into_iter().next() else { return Ok(None) };
    d.q("UPDATE ci_stages SET status='running', attempts=attempts+1, started_at=? \
         WHERE run_sha=? AND seq=? AND status='pending'",
        vec![now.into(), r.run_sha.as_str().into(), r.seq.into()])?;
    if d.changes()? != 1 { return Ok(None) }                             // a twin driver took it; next unit re-claims
    Ok(Some(Stage { run_sha: r.run_sha, seq: r.seq, name: r.name, token: r.attempts + 1 }))
}
/// No Err for stage-level failure: attempts/log live on the stage row (4.4's job retry is the last resort).
async fn execute(d: &RepoDo, st: &Stage, budget: &mut SliceBudget) -> Result<(bool, String), Error> {
    if st.name == PUBLISH { return publish(d, &st.run_sha, budget).await; }
    let Ok(f) = d.env.fetcher("STAGE_WORKER") else { return Ok((true, "no stage binding".into())) };  // feature off
    let body = serde_json::json!({"stage": st.name, "repo": d.meta("repo")?, "sha": st.run_sha,
                                  "attempt": st.token}).to_string();   // the stage reads via git-upload-pack
    let req = Request::new_with_init("https://stage/run",
        RequestInit::new().with_method(Method::Post).with_body(Some(body.into())))?;   // unverified path, Primitives
    budget.subrequests_used = budget.subrequests_used.saturating_add(1);                // 4.3: 400 per slice
    let mut resp = f.fetch_with_request(req).await.map_err(|e| Error::Storage(e.to_string()))?;
    Ok((resp.status_code() == 200, resp.text().await.map_err(|e| Error::Storage(e.to_string()))?))
}
/// Seq 255: verdict JSON -> one-blob normalized pack (2.1) -> refs/ci/<sha>. The packs row goes in 'dead'
/// BEFORE the upload (push_index's 'ingesting' pattern): a kill leaves a dead_at the Janitor collects (5.3).
async fn publish(d: &RepoDo, sha: &str, budget: &mut SliceBudget) -> Result<(bool, String), Error> {
    let body = verdict_json(d, sha)?;                                        // sync span: stage rows -> JSON bytes
    let (sql, now, pack) = (d.sql(), now_ms(), PackId::random());
    d.q("INSERT INTO packs(id,state,count,bytes,commit_lo,commit_hi,created_at,dead_at) VALUES(?,'dead',0,0,?,0,?,?)",
        vec![pack.0.as_str().into(), i64::MAX.into(), now.into(), now.into()])?;   // commit_lo none = i64::MAX (A5)
    let mut rb = budget.as_req_budget();                                       // A1: R2 calls take ReqBudget (write-back)
    let bucket = d.bucket()?;
    let mut out = PackWriter::create(&bucket, &keys::pack(&bucket.repo, &pack), 1, &mut rb).await?;
    let (off, len) = out.append_entry(gix_object::Kind::Blob, &body)?;           // A1: Result; varint+zlib inside (1.2)
    let meta = out.finish(&mut rb).await?;                                     // pack durable in R2
    let blob = gix_object::compute_hash(gix_object::Kind::Blob, &body).map_err(|e| Error::Internal(e.to_string()))?;
    // ---- final sync span: rows + ref + refs_version + GcMark, all-or-nothing ----
    d.q("UPDATE packs SET state='live', dead_at=NULL, count=?, bytes=? WHERE id=? AND state='dead'",
        vec![i64::from(meta.count).into(), meta.bytes.into(), pack.0.as_str().into()])?;
    if d.changes()? != 1 { return Err(Error::Internal("publish pack row".into())); }
    d.q("INSERT INTO objects(sha,pack_id,idx,offset,len,kind,size) VALUES(?,?,0,?,?,3,?)",
        vec![blob.to_string().as_str().into(), pack.0.as_str().into(), (off as i64).into(), i64::from(len).into(),
             (body.len() as i64).into()])?;
    d.q("INSERT INTO refs(name,target,peeled,updated_at) VALUES(?,?,NULL,?) \
         ON CONFLICT(name) DO UPDATE SET target=excluded.target, updated_at=excluded.updated_at",
        vec![format!("refs/ci/{sha}").as_str().into(), blob.to_string().as_str().into(), now.into()])?;
    d.q("UPDATE meta SET value=value+1 WHERE key='refs_version'", vec![])?;      // advertised; aborts a racing sweep (5.3)
    jobs::enqueue(&sql, JobKind::GcMark, now + 600_000, "{}")?;                  // section 3 step 7 for the new ref
    Ok((true, "published".into()))
}
/// Result write CAS'd on the attempt token (review caveat): a superseded attempt's answer is dropped.
fn record(d: &RepoDo, st: &Stage, res: Result<(bool, String), Error>, now: i64) -> Result<(), Error> {
    let retry = st.token < MAX_ATTEMPTS;                    // 'running' survives: the lease is the retry backoff
    let (status, log) = match res {
        Ok((true, l)) => ("ok", l),
        Ok((false, l)) => (if retry { "running" } else { "fail" }, l),
        Err(e) => (if retry { "running" } else { "fail" }, e.to_string()),
    };
    d.q("UPDATE ci_stages SET status=?, log=substr(?,1,8192), started_at=CASE WHEN ?='running' THEN ? ELSE started_at END \
         WHERE run_sha=? AND seq=? AND attempts=? AND status='running'",
        vec![status.into(), log.as_str().into(), status.into(), now.into(),
             st.run_sha.as_str().into(), st.seq.into(), st.token.into()])?;
    if d.changes()? != 1 { return Ok(()); }                                  // superseded attempt: drop the write
    if status == "fail" {
        d.q("UPDATE ci_runs SET status='fail' WHERE sha=?", vec![st.run_sha.as_str().into()])?;
        d.q("UPDATE ci_stages SET status='skipped' WHERE run_sha=? AND status='pending' AND seq<255",
            vec![st.run_sha.as_str().into()])?;                              // publish still runs: it reports the fail
    }
    if st.seq == 255 && status == "ok" { d.q("UPDATE ci_runs SET status='ok' WHERE sha=?", vec![st.run_sha.as_str().into()])?; }
    Ok(())
}
#[derive(serde::Deserialize)] struct Val { value: String }
#[derive(serde::Deserialize)] struct N { n: i64 }
#[derive(serde::Deserialize, serde::Serialize)] struct SOut { name: String, status: String, attempts: i64, log: Option<String> }
fn nonterminal(d: &RepoDo) -> Result<bool, Error> {
    Ok(!d.q("SELECT 1 AS n FROM ci_stages WHERE status IN ('pending','running') LIMIT 1", vec![])?.to_array::<N>()?.is_empty())
}
fn verdict_json(d: &RepoDo, sha: &str) -> Result<Vec<u8>, Error> {
    let s = d.q("SELECT name,status,attempts,log FROM ci_stages WHERE run_sha=? ORDER BY seq", vec![sha.into()])?;
    serde_json::to_vec(&serde_json::json!({"sha": sha, "stages": s.to_array::<SOut>()?})).map_err(|e| Error::Internal(e.to_string()))
}
/// POST /_do/ci/rerun {sha} (REGISTRY), sync span: the review's reset path; publish's upsert is the "non-null old".
pub fn ci_rerun(d: &RepoDo, sha: &str, now: i64) -> Result<(), Error> {
    let sql = d.sql();
    d.q("UPDATE ci_stages SET status='pending', attempts=0, started_at=NULL, log=NULL WHERE run_sha=?", vec![sha.into()])?;
    d.q("UPDATE ci_runs SET status='running' WHERE sha=?", vec![sha.into()])?;
    jobs::enqueue(&sql, JobKind::CiRun, now, "{}")?; jobs::enqueue(&sql, JobKind::CiTick, now + TICK_MS, "{}")
}
```

## Why it works
- **The "cannot be lost" claim is now literally true.** The outbox (`ci_runs`, `ci_stages`) and both `jobs::enqueue` calls are statements inside `commit_push`'s existing sync span (section 3): if the span commits, the refs moved *and* the run is queued; if it throws, neither happened (platform rollback, section 3). There is no RPC and no second storage to fail between them — the first blocker's crash gap does not exist because there is no gap. Cost on the push path: about four SQL statements inside a span that already exists; no round-trip is added, so the review's band-2 "remote: CI scheduled" nicety is unnecessary.
- **The single-alarm rule is kept by construction.** The module adds `JobKind` variants and a `run_slice` arm (4.5) and returns `SliceOutcome`s; `jobs::rearm` — invoked by the dispatcher after every slice and by the commit route after its span (A3, already required for the step-7 `GcMark` enqueue) — is the only `set_alarm` caller, so the measured "second setAlarm cancels the first" (#5) cannot bite. `enqueue` dedup-by-kind is safe here precisely because the jobs are drainers, not per-run handles: a dropped `enqueue` only means a `queued` or `running` driver already exists, and every driver scans the whole `ci_stages` table.
- **Serial order with bounded duplication.** The claim's `NOT EXISTS(earlier pending|running)` predicate admits only the lowest actionable stage of each run, so two live drivers can never run a run's stages out of order; the `status='pending'` CAS plus `changes()` (oracle, #1) makes a claim single-winner even if both drivers race. Result writes are CAS'd on the attempt token (`attempts=? AND status='running'`), so a lease-reclaimed stage whose first attempt answers late has its write dropped (review's "gate result writes on an attempt token"), and `LEASE_MS = 90 s` sits above the stage fetch timeout (review's other option). Await interleaving is real (#4); every place it can hurt is covered by a token or a one-way state.
- **Termination cannot strand.** Publish is a stage row (`seq 255`), not epilogue code after a `deleteAlarm`: a run is non-terminal until its verdict is written, so there is no "run finished, verdict lost" state — the second blocker is removed structurally. A killed isolate leaves either a `running` stage (reclaimed by the lease pass on the next tick) or a `dead` packs row the Janitor collects after GRACE (5.3); it never leaves an unreferenced R2 key, because the row precedes the upload. A driver job row stuck `running` by a kill mid-dispatch is repaired by the claim span's `jobs` UPDATE (guarded by "no fresh stage lease"), and `CiTick` is a second, independent row so liveness never rests on one row or on the platform's unspecified alarm retry.
- **The verdict is a first-class object.** `append_entry`/`finish` produce a byte-exact normalized pack (2.1), the `objects` row makes the blob resolvable through the only reader query (2.3), and `refs/ci/<sha>` is an ordinary ref: `ls-refs` advertises it, `want <blob>` fetches it (scenario 10), `GcMark` marks it reachable (the `GcMark` enqueue mirrors section 3 step 7), and the `refs_version` bump makes a racing `GcSweep` abort (5.3). The janitor needs no special case — the first pass's loose `objects/` key, which the review flagged as invisible to the orphan sweep, no longer exists.
- **Budgets.** A stage unit costs one charged subrequest (4.3's 400) plus two sync spans; publish costs the multipart part + `finish` (2-3 subrequests) on the `ReqBudget` mirror (A1). The loop checks `spent_80pct()` between units (A4) and continues on the very next firing (4.2). `MAX_ATTEMPTS = 3` bounds retries on the stage row; `Err` out of `run_slice` only on real SQLite breakage, which is what 4.4's backoff and the `dead`-row inspection are for.
- Scenarios this proof must pass (section 11): 14 (Janitor/GC interplay: the `dead`-marked publish pack is collected, the `live` verdict pack and ref survive). Added (two): (a) "isolate kill mid-stage": start a run, kill the DO while the stage fetch is outstanding, advance the fake clock past `LEASE_MS`, fire the alarm — the stage is reclaimed, `attempts` incremented, the run finishes and `refs/ci/<sha>` appears. (b) "push two refs at one sha": `ci_runs` holds one row, `ls-remote` shows `refs/ci/<sha>`, and `git fetch origin refs/ci/<sha>` + `fsck` confirm a real blob.

## Changes from the first pass
| First-pass finding (quoted) | Kind | How addressed |
|---|---|---|
| "Enqueue is not transactional with the ref flip; the proof's central 'cannot be lost' claim needs the outbox-in-repo-DO pattern before this lands." | blocker | `ci::schedule` is step 8 of `commit_push`, inside the same sync span as the ref CASes (section 3): `ci_runs`/`ci_stages` are the outbox table and the repo DO's own alarm (the section 4 dispatcher) drains it — the review's prescribed fix verbatim. |
| "`finish()` ordering (`deleteAlarm` before R2 put and `set-ref`) can strand a run with no verdict and no pending alarm." | blocker | There is no `deleteAlarm` (jobs owns the alarm) and no cross-DO `set-ref`: publish is the terminal stage row `seq 255`, executed by the same loop; the packs row is inserted `dead` before the upload and flipped `live` in the final span with the `objects`/`refs` writes, so every kill point leaves either pending work or a collectible row. |
| "Alarm overlap during a non-storage await is unverified against docs; either lift the watchdog above the stage fetch timeout or gate result writes on an attempt token." | caveat | Both. `LEASE_MS` (90 s) exceeds the stage fetch timeout, and `record` writes through `WHERE attempts=? AND status='running'` — a superseded attempt's `changes()` is 0 and its log is dropped. Claims are a `status='pending'` CAS, so a race between two live drivers admits exactly one. |
| "Same-SHA-different-ref pushes share one run; re-runs need an explicit reset path and a non-null CAS `old`." | caveat | Kept and documented: `ci_runs.sha` is the PK (`ON CONFLICT DO NOTHING`, first ref wins `ref`). `POST /_do/ci/rerun` resets the rows; publish's `ON CONFLICT(name) DO UPDATE` replaces the old verdict — no `old` oid needed because `refs/ci/` is our own namespace, not a client CAS. |
| "'CI' here means Worker stages (no shell, no Linux); arbitrary user code needs Workers for Platforms (paid) or the `hooks-as-workers` slug." | caveat | Not addressed — it is the honest shape of the idea. `STAGE_WORKER` is a service binding; arbitrary user stage code remains a dependency on `hooks-as-workers` / Workers for Platforms. |
| "Verdict blobs written by CI are new `objects/` keys the orphan janitor must tolerate; `gitBlobSha`/zlib helpers and the `set-ref` endpoint are assumed from other slugs." | caveat | Gone structurally. The verdict is a normalized pack under `packs/` (2.1-2.2) with an `objects` row, so the Janitor and GC treat it like every other object; `compute_hash` and `PackWriter::append_entry` replace `gitBlobSha`/`zlibDeflate`; the ref write is local SQL in the same DO, no `set-ref` endpoint. |
| "During the `STAGE_WORKER.fetch` await the input gate is open, so a watchdog alarm ... the same stage runs twice in the same isolate" | caveat | Accepted and engineered for: interleaving during awaits is measured (#4). Duplicates are bounded by the claim CAS, harmless because stages are required idempotent (they read objects and return a log; a duplicate publish writes a second live copy of identical bytes, which 2.3 makes legal, then upserts the same ref). |

## Known limits
- Weakened relative to the first pass (stated, not silent): per-run parallelism becomes at most two concurrent stage executions per repo (the two job kinds), and a stage's time slice is 20 s wall / 400 subrequests (4.3) instead of a dedicated alarm invocation. Per-run ordering is unchanged.
- Stage code is Worker JS/Wasm reached through `STAGE_WORKER`; no shell, no Linux, no `eval` (Workers restriction, first-pass Known limit carried). Arbitrary user scripts need Workers for Platforms or `hooks-as-workers`.
- A stage fetch longer than `LEASE_MS` produces a duplicate concurrent execution; stages must be idempotent. The stage worker reads objects through `POST /:owner/:repo/git-upload-pack` (`want <oid>`, scenario 10) with its own read credential — a stage that needs many blobs pays its own subrequests, not the DO's.
- `refs/ci/*` is an ordinary namespace: a user's `git push origin :refs/ci/<sha>` or `push --mirror` can delete or overwrite verdicts, and `clone --mirror` copies them (review interop note, kept). Protecting the namespace is a hook concern, out of scope.
- A re-push of a sha whose run is terminal does not re-run CI (dedup by sha); rerun is explicit via `/_do/ci/rerun`. The `ref` column records the first ref only.
- `meta 'ci.stages'` is a JSON stage-name list written out of band (no config system exists in the foundation); absent, runs consist of the publish stage only. Without the `STAGE_WORKER` binding all stages no-op `ok`.
- Foundation write-backs this proof assumes, all small: `commit_push` collects ok+non-null-new commands into `moved` and calls `ci::schedule` as step 8; `SliceBudget::as_req_budget()` produces the `ReqBudget` A1 wants on R2 calls inside slices; `RepoDo.env` is `pub(crate)` like `RepoDo.state` (A1); the claim span repairs a sibling `jobs` row — the contract is silent on driver-row repair (A4 covers only `dead` rows at `boot`).
- Unverified: `Fetcher` inside a DO, `Request::new_with_init` + `RequestInit`, real-R2 multipart (#6), subrequest enforcement inside a DO (#7), platform alarm retry after a kill (backstop only — the lease and `CiTick` provide liveness without it).

## Depends on
- repo-do-ref-authority
- refs-sqlite-objects-r2
- two-phase-push
- streaming-pack-parser
- gc-and-repack-alarm
- hooks-as-workers
- auth-and-multitenancy
