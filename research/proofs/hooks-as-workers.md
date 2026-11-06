# Pre/post-receive hooks as Workers via service bindings

> Second pass · Idea #23 · verdict: **lands with caveats** · feasibility 4/5 · reliability 4/5 · correctness 4/5 (first pass 4/2/3)
> First pass: [proof](./archived/hooks-as-workers.md) · [review](../reviews/archived/hooks-as-workers.md) · Second pass: [review](../reviews/hooks-as-workers.md) · Contract: [CONTRACTS.md](../../CONTRACTS.md)

## Mechanism
Section 12 lists hooks as out of scope for the foundation, so this is a post-foundation module registered per A9. One relocation is forced by section 3: the pre-receive gate cannot run inside `commit_push` (one sync span, no awaits) and running it inside the DO would hold the repo's serialisation point open for the hook's wall-clock; it therefore runs in the edge Worker, inside the push flow, between `pack::ingest::run` (objects durable and indexed, `ingesting` — the contract's quarantine) and `/_do/push/commit` (the ref CAS). That is git's own ordering: unpack into quarantine, run pre-receive, then take the ref lock.

The owner registers hooks with `PUT /<owner>/<repo>/hooks` (write principal, section 12) -> `/_do/hooks/set` (one sync span, replace-all). `push_begin` returns the list with the begin response (write-back), so the pre-receive set is the snapshot as of push begin. Each target resolves to `service:` (a static binding, `env.service(name)`), `dispatch:` (a Workers-for-Platforms dispatch namespace, `env.dynamic_dispatcher("HOOKS")`), or `https://` (a webhook, HMAC-signed when `hooks.secret` is set). The call carries git's byte-exact hook stdin (`<old> <new> <ref>\n` per command) in the body and only bounded metadata in headers. A non-2xx, timed-out hook declines the whole push — git's all-or-nothing pre-receive: `/_do/push/reject` closes the `pushes` row so the Janitor kills the `ingesting` pack (5.2), and the edge answers under A2's after-header rule: HTTP 200, `unpack ok`, `ng <ref> pre-receive hook declined` per command, the hook's body as band-2 `remote:` lines when `side-band-64k` was negotiated. An unreachable hook is crash-equivalent: the edge returns `Error::Storage`, which A2 maps to the same 200 report with `unpack error` and `ng ... unpack failed`.

A hook inspects the pushed objects with an ordinary v2 `fetch` carrying `x-ge-push: <push_id>`: the edge accepts the header as a read credential on the upload-pack routes and `/_do/fetch` validates `pushes.state='open'` in one sync SELECT, then widens the section 9 reader query to that push's `ingesting` pack (`Index::lookup_or_pack`, write-back) — the contract's `GIT_QUARANTINE_PATH`. The token is the unguessable `PushId` and dies at commit, reject, or `PUSH_TIMEOUT` expiry.

Post-receive is genuinely inside the commit: `commit_push` inserts one `hook_outbox` row per registered post-receive hook — in the same sync span as the ref flip, covering only accepted commands as git does — then `jobs::enqueue(PostReceive)` (dedup by kind, 4.5). The route's post-span `jobs::rearm` (A3, already needed for step 7's GcMark) arms the single dispatcher alarm; the `PostReceive` slice drains the outbox through the same call path with the 4.4 backoff formula, dead-letters after 12 attempts (~5 h), and obeys 4.1/4.2/A4 exactly. A hook that pushes back is bounded by `x-ge-depth` (`depth > 1` -> `Forbidden`); the nested push still needs ordinary write auth.

Additions (all in the REGISTRY block): tables `hooks`, `hook_outbox`; `JobKind::PostReceive`; DO routes `/_do/hooks/set`, `/_do/push/reject`; header `x-ge-push` on `/_do/fetch` and `/_do/ls-refs`; edge route `PUT /<owner>/<repo>/hooks`; module `src/hooks.rs` (`edge -> hooks`, `jobs -> hooks`). No R2 keys (2.2 unchanged).

## Primitives
- `Env::service(name) -> Fetcher`, `Fetcher::{fetch, fetch_request}`: verified in worker 0.8.5 source/docs.rs; service bindings listed supported in memo section 1. Called from inside the DO (alarm context) through `d.env`: same `Env` object, runtime **unverified**.
- `Env::dynamic_dispatcher(name) -> DynamicDispatcher`, `DynamicDispatcher::get(name) -> Fetcher`: verified in worker 0.8.5 docs.rs; the dispatch-namespace feature itself is a Workers-for-Platforms paid add-on, **not measured**.
- `Fetch::{Request, Url}::send` / `send_with_signal(&AbortSignal)`: verified (docs.rs). `RequestInit` has **no `signal` field** (docs.rs field list), so `Fetcher::fetch_request` cannot be aborted; the timeout is a `Delay` race and the losing arm keeps running (Known limits).
- `worker::Delay: Future + From<Duration>` raced with `futures_util::future::select` (futures-util is already a dependency, memo section 4): verified.
- `Request::new_with_init` + `RequestInit::{with_method, with_headers, with_body}` for hook and stub requests: **unverified at runtime**, as in every sibling proof.
- `jobs::{enqueue, rearm}`, `SliceBudget::spent_80pct`, `SliceOutcome::{Done, Continue, Reschedule}`: contract section 4 as amended by A3/A4. Second `setAlarm` cancels the first: measured (platform-facts #5).
- `SqlStorage::exec` sync, `SELECT changes()`: measured 1 / 0 / 1 (platform-facts #1); sync-span atomicity measured (#4).
- `platform::hmac_sha256` for `https:` targets (web_sys `SubtleCrypto`, A8 owns the binding): **unverified**.
- No gix APIs beyond the foundation's: hook stdin is plain formatting; the quarantine fetch reuses section 9 unchanged.

## Proof code
```rust
// src/hooks.rs (shared call path) + src/repo_do/hooks.rs + src/jobs/post_receive.rs + src/edge/receive.rs fragments.
// worker 0.8.5. CONTRACTS.md 1.3, 3, 4, 5, 7, 10; amendments A1-A10. Helpers q/changes/oid/now_ms/json/meta_str/N/IdRow
// and stub_json/report/Begin/CommitRequest/CmdDto per repo-do-ref-authority and two-phase-push; DTOs live in wire::http (A8).
//
// REGISTRY (A9) — every addition this module makes:
//   tables (schema_version 4, migrated in boot):
//     hooks(id INTEGER PRIMARY KEY AUTOINCREMENT, phase TEXT NOT NULL, target TEXT NOT NULL, secret TEXT,
//           created_at INTEGER NOT NULL)
//     hook_outbox(id INTEGER PRIMARY KEY AUTOINCREMENT, hook_id INTEGER NOT NULL, payload TEXT NOT NULL,
//                 attempts INTEGER NOT NULL DEFAULT 0, next_at INTEGER NOT NULL, state TEXT NOT NULL DEFAULT 'queued')
//   JobKind::PostReceive (4.5): one queued/running row, drains hook_outbox. Nothing here calls set_alarm (4.1).
//   DO routes: POST /_do/hooks/set, POST /_do/push/reject — both sync spans (1.3 "Awaits inside: none").
//   DO headers: x-ge-push on /_do/fetch and /_do/ls-refs — the quarantine token, validated inside the route.
//   edge: PUT /<owner>/<repo>/hooks (can_write); x-ge-depth honoured on git-receive-pack; x-ge-push substitutes
//     read auth on upload-pack GET/POST (Mechanism). Module src/hooks.rs: edge->hooks, jobs->hooks (1.1 extended here).
//   R2 keys: none (2.2).
use std::time::Duration;
use futures_util::{future::{self, Either}, pin_mut};
use worker::{Delay, Env, Fetch, Fetcher, Headers, Method, Request, RequestInit, Response, SqlStorageValue as V};
use crate::{error::Error, jobs::{self, Job, JobKind, SliceBudget, SliceOutcome}, repo_do::RepoDo,
            store::{PackId, PushId}, wire::http::{CmdDto, HookDto, HookPayload}, ReqBudget};
const HOOK_TIMEOUT_MS: u64 = 10_000;            // the hook Worker's own 30 s CPU cap is separate and larger
const MAX_HOOKS: usize = 32;                    // bounds hooks_set and the sequential fan-out
const MAX_ATTEMPTS: u32 = 12;                   // min(30 s * 2^n, 1 h) — the 4.4 formula — sums to ~5 h, then 'dead'
const GRACE_MS: i64 = 3_600_000;                // section 5

// ---- src/hooks.rs: resolve + call, the one path used by edge (pre) and jobs (post). wire::http DTOs (A8):
//   HookDto{id,phase,target}, HooksSetDto{hooks:Vec<HookInDto{phase,target,secret}>}, RejectDto{push_id,reason},
//   HookPayload{push,who,depth,cmds,hook}; CmdDto is CommitRequest's command row, Cmd the parsed in-DO form.
pub enum Caller { Bound(Fetcher), Web }
pub enum Outcome { Ok, Declined(Vec<String>), Unreachable, TimedOut }
/// service:NAME -> a static binding; dispatch:NAME -> the HOOKS dispatch namespace (WfP); https:// -> webhook.
/// Returns the URL the Request must carry: bindings ignore it, Fetch::Request uses it verbatim.
fn resolve(env: &Env, target: &str) -> Result<(Caller, String), Error> {
    if let Some(n) = target.strip_prefix("service:") { return Ok((Caller::Bound(env.service(n)?), "https://hook.invalid/".into())); }
    if let Some(n) = target.strip_prefix("dispatch:") { return Ok((Caller::Bound(env.dynamic_dispatcher("HOOKS")?.get(n)?), "https://hook.invalid/".into())); }
    if target.starts_with("https://") { return Ok((Caller::Web, target.into())); }
    Err(Error::Internal(format!("bad hook target {target}")))
}
/// git hook stdin, byte-identical: `<old> <new> <ref>\n` per command; creates/deletes carry the zero oid.
pub fn stdin_lines(cmds: &[CmdDto]) -> Vec<u8> {
    cmds.iter().flat_map(|c| format!("{} {} {}\n", c.old, c.new, c.name).into_bytes()).collect()
}
/// One call with a wall-clock cap. Commands ride in the BODY (git's stdin); headers stay ~200 B so the 16 KB
/// per-header cap cannot fire on a --mirror push. 2xx == git's exit 0; non-2xx declines and the body -> remote: lines.
pub async fn call(env: &Env, target: &str, phase: &str, stdin: Vec<u8>, repo: &str, push: &str, depth: u32) -> Outcome {
    let (caller, url) = match resolve(env, target) { Ok(x) => x, Err(_) => return Outcome::Unreachable };
    let depth_s = depth.saturating_add(1).to_string();                 // hooks see the incoming push's depth + 1
    let h = Headers::new();
    for (k, v) in [("content-type", "text/plain"), ("x-ge-hook", phase), ("x-ge-repo", repo),
                   ("x-ge-push", push), ("x-ge-depth", depth_s.as_str())] {
        if h.set(k, v).is_err() { return Outcome::Unreachable; }
    }
    let mut init = RequestInit::new();
    init.with_method(Method::Post).with_headers(h).with_body(Some(stdin.into()));
    let req = match Request::new_with_init(&url, init) { Ok(r) => r, Err(_) => return Outcome::Unreachable }; // ctor unverified at runtime
    let fut = async move { match caller {
        Caller::Bound(f) => f.fetch_request(req).await.map_err(|e| e.to_string()),
        Caller::Web => Fetch::Request(req).send().await.map_err(|e| e.to_string()),   // send_with_signal exists here only
    }};
    pin_mut!(fut);
    match match future::select(fut, Delay::from(Duration::from_millis(HOOK_TIMEOUT_MS))).await {
        Either::Left((r, _)) => r, Either::Right(_) => return Outcome::TimedOut } {
        Err(_) => Outcome::Unreachable,
        Ok(mut r) if !(200..300).contains(&r.status_code()) =>
            Outcome::Declined(r.text().await.unwrap_or_default().lines().take(200)
                              .map(|l| l.trim_end().chars().take(200).collect()).collect()),  // each <= MAX_BAND_DATA
        Ok(_) => Outcome::Ok,
    }
}

// ---- src/edge/receive.rs: inside the push flow, after ingest, before commit (section 3 ordering 2 -> gate -> 3). ----
// Inserted into the sibling's receive_pack: the depth check runs before BodyReader; `commit` (the CommitRequest,
// now carrying `depth`) is built right after ingest::run; the gate loop sits between it and the commit POST.
// let depth: u32 = req.headers().get("x-ge-depth")?.and_then(|v| v.parse().ok()).unwrap_or(0);
// if depth > 1 { return Err(Error::Forbidden); }          // advisory recursion stop; 403 before any body byte (10)
// ... parse header, /_do/push/begin (response gains hooks: Vec<HookDto>), ingest::run, build commit — as the siblings ...
// pack_id may be None (delete-only / 0-object pack, 2.4): hooks still run; git runs pre-receive for deletes too.
for hook in begin.hooks.iter().filter(|h| h.phase == "pre-receive") {
    budget.charge(1)?;                                     // one subrequest per hook call (7.1); <= 32 of them
    let lines = match hooks::call(env, &hook.target, "pre-receive", hooks::stdin_lines(&commit.commands),
                                &repo.name(), &push_id.0, depth).await {
        hooks::Outcome::Ok => continue,
        hooks::Outcome::Declined(l) => l,                  // hook body -> band-2 remote: lines
        hooks::Outcome::TimedOut => vec!["pre-receive hook timed out".into()],
        hooks::Outcome::Unreachable =>                     // crash-equivalent: push row stays 'open', Janitor expires it
            return Err(Error::Storage("pre-receive hook unreachable".into())),   // A2 -> 200 + unpack error + ng
    };
    let _: serde_json::Value = stub_json(&stub, repo, "/_do/push/reject",
        &serde_json::json!({"push_id": push_id.0, "reason": "pre-receive hook declined"}), &mut budget).await?;
    let results = commit.commands.iter()
        .map(|c| RefResult::Ng(c.name.clone().into(), "pre-receive hook declined")).collect();
    return report_remote(&hdr, Ok(()), &results, &lines);   // A2: 200 + unpack ok + ng per ref + band-2 lines (write-back)
}
// ... CommitRequest gains depth: u32 (write-back); /_do/push/commit and report() unchanged ...

// ---- src/repo_do/hooks.rs: two sync-span routes + the commit step that writes the outbox. ----
impl RepoDo {
    /// POST /_do/hooks/set. Validate everything before the first write; replace-all is atomic inside the span.
    pub fn hooks_set(&self, b: &HooksSetDto) -> Result<Response, Error> {
        if b.hooks.len() > MAX_HOOKS { return Err(Error::Limit("at most 32 hooks per repo".into())); }
        for h in &b.hooks {
            let ok = (h.phase == "pre-receive" || h.phase == "post-receive")
                && (h.target.starts_with("service:") || h.target.starts_with("dispatch:") || h.target.starts_with("https://"))
                && (h.secret.is_none() || h.target.starts_with("https://"));
            if !ok { return Err(Error::Protocol("bad hook row".into())); }
        }
        self.q("DELETE FROM hooks", vec![])?;
        for h in &b.hooks {
            self.q("INSERT INTO hooks(phase,target,secret,created_at) VALUES(?,?,?,?)",
                   vec![h.phase.as_str().into(), h.target.as_str().into(),
                        h.secret.clone().map(V::from).unwrap_or(V::Null), now_ms().into()])?;
        }
        json(serde_json::json!({"hooks": b.hooks.len()}))
    }
    /// POST /_do/push/reject: the gate declined; the push can never commit and 5.2 kills its pack next slice.
    pub fn push_reject(&self, b: &RejectDto) -> Result<Response, Error> {
        let result = serde_json::to_string(&b.reason).map_err(|e| Error::Internal(e.to_string()))?;  // A5: rejected pushes write result
        self.q("UPDATE pushes SET state='rejected', ended_at=?, result=? WHERE id=? AND state='open'",
               vec![now_ms().into(), result.into(), b.push_id.as_str().into()])?;
        if self.changes()? != 1 { return Err(Error::Conflict("push not open".into())); }
        json(serde_json::json!({}))
    }
    /// commit_push step 4b, inside the one sync span (section 3, A2): post-receive outbox, accepted refs only.
    /// `accepted` holds the sibling proof's parsed `Cmd`s whose step-4 result was ok.
    fn enqueue_post_receive(&self, sql: &worker::SqlStorage, req: &CommitRequest, accepted: &[Cmd], now: i64) -> Result<(), Error> {
        if accepted.is_empty() { return Ok(()); }
        let posts: Vec<HookDto> = self.q("SELECT id,phase,target FROM hooks WHERE phase='post-receive'", vec![])?.to_array()?;
        if posts.is_empty() { return Ok(()); }
        for h in posts {
            let cmds: Vec<CmdDto> = accepted.iter()
                .map(|c| CmdDto { old: c.old.to_string(), new: c.new.to_string(), name: c.name.clone() }).collect();
            let payload = serde_json::to_string(&HookPayload { push: req.push_id.clone(), who: req.principal.clone(),
                depth: req.depth, cmds, hook: h.id }).map_err(|e| Error::Internal(e.to_string()))?;
            self.q("INSERT INTO hook_outbox(hook_id,payload,next_at) VALUES(?,?,?)", vec![h.id.into(), payload.into(), now.into()])?;
        }
        jobs::enqueue(sql, JobKind::PostReceive, now, "{}")?;                  // sync, dedups by kind (4.5, A3)
        // A queued PostReceive parked at a backoff run_at would sit past this due-now row: pull it forward so
        // the post-span rearm (A3) fires now. A 'running' one needs no nudge: its drain loop sees the rows.
        self.q("UPDATE jobs SET run_at=? WHERE kind='PostReceive' AND state='queued' AND run_at>?",
               vec![now.into(), now.into()])?;
        Ok(())
    }
    /// The quarantine token on /_do/fetch and /_do/ls-refs (write-back): one sync SELECT; the token dies with the push.
    fn push_scope(&self, push: Option<&str>) -> Result<Option<PackId>, Error> {
        let Some(push) = push else { return Ok(None) };
        if self.q("SELECT 1 AS n FROM pushes WHERE id=? AND state='open'", vec![push.into()])?.to_array::<N>()?.is_empty() {
            return Err(Error::Forbidden);
        }
        Ok(self.q("SELECT id FROM packs WHERE push_id=? AND state='ingesting'", vec![push.into()])?
            .to_array::<IdRow>()?.into_iter().next().map(|r| PackId(r.id)))
        // fetch_v2 step 1 then resolves wants/haves via Index::lookup_or_pack(ids, scope) — the 2.3 reader query
        // widened to `o.sha=? AND (p.state='live' OR p.id=?)` (write-back).
    }
}

// ---- src/jobs/post_receive.rs: the run_slice arm for JobKind::PostReceive (4.2). Never calls set_alarm (4.1). ----
#[derive(serde::Deserialize)] struct OutRow { id: i64, payload: String, attempts: u32, target: Option<String> }
#[derive(serde::Deserialize)] struct MinRow { n: Option<i64> }
pub async fn run_post_receive(d: &RepoDo, _job: &Job, budget: &mut SliceBudget) -> Result<SliceOutcome, Error> {
    let now = now_ms();
    d.q("DELETE FROM hook_outbox WHERE state='dead' AND next_at<?", vec![now.saturating_sub(GRACE_MS).into()])?;
    loop {
        let now = now_ms();
        let row = d.q("SELECT o.id,o.payload,o.attempts,h.target FROM hook_outbox o LEFT JOIN hooks h ON h.id=o.hook_id \
                       WHERE o.state='queued' AND o.next_at<=? ORDER BY o.id LIMIT 1", vec![now.into()])?
            .to_array::<OutRow>()?.into_iter().next();
        let Some(row) = row else { break };
        if budget.spent_80pct() { return Ok(SliceOutcome::Continue { cursor: String::new() }); }     // A4
        match row.target {
            None => { d.q("DELETE FROM hook_outbox WHERE id=?", vec![row.id.into()])?; }             // hook deleted since commit
            Some(target) => {
                budget.subrequests_used = budget.subrequests_used.saturating_add(1);               // one service/fetch call
                let p: HookPayload = serde_json::from_str(&row.payload).map_err(|e| Error::Internal(e.to_string()))?;
                let repo = format!("{}/{}", d.meta_str("owner")?, d.meta_str("repo")?);
                match crate::hooks::call(&d.env, &target, "post-receive",
                        crate::hooks::stdin_lines(&p.cmds), &repo, &p.push, p.depth).await {
                    crate::hooks::Outcome::Ok => { d.q("DELETE FROM hook_outbox WHERE id=?", vec![row.id.into()])?; }
                    _ if row.attempts.saturating_add(1) >= MAX_ATTEMPTS =>
                        { d.q("UPDATE hook_outbox SET state='dead', attempts=attempts+1 WHERE id=?", vec![row.id.into()])?; }
                    _ => { let wait = 30_000i64.checked_shl(row.attempts.min(7)).unwrap_or(i64::MAX).min(GRACE_MS);  // 4.4 formula
                           d.q("UPDATE hook_outbox SET attempts=attempts+1, next_at=? WHERE id=?",
                              vec![now.saturating_add(wait).into(), row.id.into()])?; }
                }
            }
        }
    }
    // Tail of the same sync span as the last SELECT: a commit that landed during a hook await left due rows the loop
    // sees (delivered above); one that lands after this span finds no queued job and its enqueue+rearm fires fresh.
    match d.q("SELECT MIN(next_at) AS n FROM hook_outbox WHERE state='queued'", vec![])?.one::<MinRow>()?.n {
        Some(t) if t <= now => Ok(SliceOutcome::Continue { cursor: String::new() }),
        Some(t) => Ok(SliceOutcome::Reschedule { run_at: t }),              // backoff-parked rows
        None => Ok(SliceOutcome::Done),
    }
}
```

## Why it works
- **The gate sits exactly where git puts it.** Section 3's ordering is pack durable (1) -> rows indexed (2) -> `commit_push` (3); pre-receive runs between 2 and 3, when objects are quarantined (`ingesting`, invisible to the 2.3 reader query) and no ref has moved. Decline -> `/_do/push/reject` -> Janitor 5.2 kills the pack and 5.3 deletes both keys after GRACE; the garbage cost is identical to git unpacking into a quarantine the hook then rejects.
- **The commit span is untouched.** Every hook await happens in the edge; `commit_push` keeps "Awaits inside: none" (1.3). During the gate the DO serves other pushes normally — only the `pushes` row stays `open` (bounded by `PUSH_TIMEOUT`). Two racing pushes both pass their hooks and the CAS decides (measured #4): the loser gets `ng <ref> failed to update ref`, exactly git's ref-lock race. A `GcSweep` in the window flips `gc_epoch` and the commit is rejected with `gc ran during push, retry` (3 step 2).
- **Error rules (A2, section 10) are honoured by construction.** The header is already parsed, so every outcome is HTTP 200 report-status: decline -> `unpack ok` + `ng <ref> pre-receive hook declined`; unreachable -> `unpack error pre-receive hook unreachable` + `ng ... unpack failed`. `RefResult::Ng` carries `&'static str`, so hook output *cannot* reach the pkt stream — the blocker is closed by the type, and the body goes out as band-2 `remote:` lines under `side-band-64k` (write-back `write_report_status_remote`, rules 2 and 4), dropped without sideband exactly as git drops stderr.
- **The alarm blocker cannot recur.** Nothing in this module calls `set_alarm` (4.1, CI grep): post-receive delivery is a `jobs` row, not an alarm; `jobs::enqueue` writes it inside the commit span (atomic with the outbox rows and the ref flip) and the route's existing post-span `jobs::rearm` (A3) arms the dispatcher. The first pass's collision — one `alarm()` draining the outbox while the janitor alarm is silently cancelled — has no alarm left to collide with: `rearm` computes `MIN(run_at)` across all queued kinds, so janitor, gc and `PostReceive` share the one alarm correctly.
- **The outbox cannot be orphaned by the dedup race.** `enqueue` dedups by kind, so a commit during a running drain adds no row; the drain's final `SELECT`s run in the same sync span as `dispatch`'s outcome application (a ready future's `.await` does not yield), so a commit either landed before the tail (its rows are due -> delivered or `Continue`) or after (job row already consumed -> fresh `enqueue` -> `rearm` fires). A queued job parked at a backoff `run_at` is pulled forward by the commit-span `UPDATE jobs`, so `MIN(run_at)` is never stale.
- **At-least-once with a real retry budget.** Rows are claimed by `ORDER BY id` (per-repo delivery order preserved), deleted on 2xx, rescheduled `min(30 s * 2^attempts, 1 h)` (the 4.4 formula), and dead-lettered — not dropped — at 12 attempts, ~5 h of retry, retained GRACE for inspection. The hook dedupes on `x-ge-push` for redeliveries after a crash between delivery and row delete (review case B).
- **Quarantine is real, not hand-waved.** `x-ge-push` is the `PushId` — 32 hex, unguessable, stored on a `pushes` row that must be `open`. `push_scope` validates in one sync SELECT; `lookup_or_pack` widens the 2.3 query to the push's `ingesting` pack; the token dies at commit/reject/expiry, after which post-receive hooks fetching the same oids resolve them as `live`. The hook runs a real `git fetch` (`http.extraHeader: x-ge-push=...`, `filter=blob:none` for policy checks), bounded by section 9's 200,000-commit / 64 MiB fetch cap.
- **Budgets hold.** Edge: <= 32 hook calls + 1 reject call, each `budget.charge(1)` — trivially inside 9,000 (7.1). DO slice: one charged subrequest per delivery, `spent_80pct` between units (A4), 400-subrequest ceiling (4.3). Hook Workers run under their own budget; the fan-out is sequential so memory is one payload (<~1.2 MB for a max-size 1 MiB command section) at a time.

## Changes from the first pass
| First-pass finding (quoted) | Kind | How addressed |
|---|---|---|
| "Single-alarm collision with two-phase-push's janitor (and gc-and-repack-alarm, which also owns `setAlarm`): needs one dispatcher `alarm()` that computes the earliest of `pending` sweep, `outbox.next_at`, gc, and re-arms from all tables." | blocker | Addressed by contract section 4, which is that dispatcher: one alarm, `rearm` sets `MIN(run_at)` over all queued kinds. `PostReceive` is a `JobKind` variant and `hook_outbox` rows, never an alarm; `enqueue` is inside the commit span and `rearm` after it (A3). Review case C — "the two-phase-push janitor alarm is gone" — cannot occur: there is no second `set_alarm` call in the crate to cancel it. |
| "Hook rejection message can contain `\n` and corrupt report-status; must be sanitised or moved to band 2." | blocker | Both. The ng reason is the literal `&'static str` `pre-receive hook declined` — `RefResult::Ng` makes hook text in the pkt stream a compile error — and the body is moved to band 2 `remote:` lines (`write_report_status_remote`, each frame <= `MAX_BAND_DATA`, rule 2), which is exactly what git does with hook stderr. |
| "Upload-pack serving not-yet-committed objects under a `pushId` token is required for any real pre-receive policy and is not designed." | blocker | The `x-ge-push` path: edge accepts the header in place of Basic auth on upload-pack GET/POST, `/_do/fetch` validates `pushes.state='open'` in one sync SELECT, `Index::lookup_or_pack` widens the reader query to the push's `ingesting` pack. The hook does a normal v2 `git fetch` of the pending objects. Token = `PushId`, dies with the push. |
| "'Users deploy a Worker and register it' is only true with Workers for Platforms; otherwise it is a webhook with HMAC, whose secret storage/rotation is absent." | caveat | Still true, stated in Mechanism and Known limits rather than weakened: `service:` targets are deploy-time only; `dispatch:` is the verified `env.dynamic_dispatcher("HOOKS").get(name)` API but a WfP paid add-on; `https:` is the only fully dynamic target. `hooks.secret` now stores the webhook secret; rotation is another `PUT /hooks`; `platform::hmac_sha256` is unverified. |
| "Commands in a request header hit the 16 KB per-header limit on large or `--mirror` pushes." | caveat | Commands moved to the request body — which is git's stdin anyway. Headers carry only `x-ge-hook`, `x-ge-repo`, `x-ge-push`, `x-ge-depth`: ~200 bytes, fixed. |
| "Retry budget of ~8.5 min then drop makes post-receive lossy for any realistic outage." | caveat | Backoff is now the 4.4 formula `min(30 s * 2^attempts, 1 h)` with `MAX_ATTEMPTS = 12` ≈ 5 h; the 12th failure marks the row `dead` and retains it GRACE for inspection instead of deleting it. Beyond ~5 h delivery is still lost — bounded and inspectable, stated in Known limits. |
| "Hooks receive stdin lines but nothing else git provides (`update` hook, push-options, `GIT_PUSH_OPTION_*`, quarantine); non-fast-forward checks cost R2 reads per commit." | caveat | Quarantine is real (blocker 3 row). Push-options cannot arrive: `push-options` is not advertised (rule 5, section 12), so `GIT_PUSH_OPTION_*` never exists. `update`/`post-update`/`proc-receive` remain unmodelled (Known limits). The fast-forward check is one `fetch --filter=blob:none` inside the hook Worker — the cost leaves the push path entirely and is bounded by section 9's fetch cap. |
| "isolate dies after the transaction commits but before line 85's `setAlarm` reaches storage" (crash case A) | caveat | `enqueue` writes the job row inside the commit span, so it is atomic with the outbox row and the ref flip; only `rearm` is post-span. A crash between them leaves a queued job with no armed alarm — delivered on the next alarm-causing event; the boot-rearm write-back (A4: boot already re-enqueues dead per-repo maintenance kinds) closes the window. Delayed, never lost. |
| "the `hooks` table is read at two different times with awaits between ... benign but document it" | caveat | Documented semantics: pre-receive uses the begin snapshot, post-receive reads `hooks` inside the commit span — as-of-commit. A hook registered mid-push sees the post-receive only, matching git (the hook executable is read at each phase's run time, and the phases differ). |
| "the `x-git-edge-depth` recursion stop is asserted, never enforced by any endpoint in the proof" | caveat | Enforced: `receive_pack` refuses `x-ge-depth > 1` with `Forbidden` before parsing the body (403, section 10 pre-header mapping). Advisory, not a security boundary — any pusher can omit the header; the real bounds are write auth on the nested push and the CAS. |
| "with side-band-64k every report-status line must be wrapped in band 1 ... `report-status-v2` (`ok <ref>` may be followed by `option ...` lines) is not addressed" | caveat | Framing stays `wire`'s (rule 4); band-2 `remote:` lines precede the band-1 report. The foundation writes `report-status-v2` without option lines (allowed, section 12) and hook output adds none. |
| "`commit()` calls `setAlarm(Date.now())` ... the janitor alarm two-phase-push armed in `begin()` ... is silently overwritten" (crash case C) | blocker | Same root as blocker 1: under 4.1/A3 there is exactly one alarm, owned by `rearm`; outbox timing is expressed as `jobs.run_at`, which `MIN(run_at)` covers alongside janitor and gc rows. |

## Known limits
- **"Register a Worker" is only literally true under Workers for Platforms.** `service:` targets must be declared in git-edge's `wrangler.jsonc` at deploy time; `dispatch:` needs the paid add-on and a `HOOKS` namespace binding; `https://` is the only fully dynamic target and loses the in-process latency (it is a public hop with HMAC via the unverified `platform::hmac_sha256`). Not weakened, restated.
- **The timeout does not cancel.** `RequestInit` has no `signal` field in worker 0.8.5, so the `Delay` race abandons the future but the hook's subrequest runs on; it still consumes the hook Worker's own CPU budget, and our one charged subrequest slot is already spent. `Fetch::send_with_signal` exists only on the `https:` arm.
- **The token is repo-read, not object-scoped.** `x-ge-push` grants reads of all live objects plus the pending pack for the `open` window (<= `PUSH_TIMEOUT`), because ancestry checks need live bases. It is unguessable and dies with the push; narrower scoping is scoped-token-remotes' business.
- **Depth is advisory.** `x-ge-depth` bounds only cooperative hooks; a malicious hook omits it. The load-bearing bounds are `auth::authenticate` on the nested push and the CAS.
- **Pre-receive latency adds to the open window, not the commit span.** The DO is never held during the await; the `pushes` row sits `open` (bounded by `PUSH_TIMEOUT` = 1 h), and a declined or unreachable hook leaves cleanup to `/_do/push/reject` + Janitor, or to expiry in the crash-equivalent case.
- **A declined push has already paid full ingest** — normalized pack written and indexed — the same garbage cost git pays unpacking into quarantine. The pack is `dead` at the next Janitor slice and its keys go after GRACE.
- **Post-receive is bounded retry, not infinite.** ~5 h across 12 attempts, then `dead` for GRACE and deleted. Better than git's fire-and-forget, not a queue. A `dead` `PostReceive` job row itself is re-enqueued at next `boot` per A4's maintenance-kind rule.
- **Unmodelled git surface:** `update` (per-ref partial reject), `post-update`, `proc-receive`, push-options (unadvertised, so impossible), quarantine env vars, hook exit-code nuance (any non-2xx is one `declined` reason). Non-fast-forward policy costs the hook one filtered fetch per check.
- **Unverified, day-1 list:** `Request::new_with_init` + `RequestInit` (as every sibling); `env.service`/`dynamic_dispatcher().get()` used from inside a DO alarm; `platform::hmac_sha256`; `worker::Delay` inside a DO; the DO subrequest cap on a deployed Worker (platform-facts #7).
- **Write-backs this proof needs.** `Begin` response gains `hooks: Vec<HookDto>`; `CommitRequest` gains `depth: u32`; `Index::lookup_or_pack(ids, Option<&PackId>)` (the 2.3 query OR'd with one pack id); `wire::write_report_status_remote(w, unpack, results, caps, remote: &[String])` emitting band-2 frames; `platform::hmac_sha256`; `RepoDo.env` `pub(crate)` like `state` (A1); `boot` calls `jobs::rearm` whenever any `queued` job row exists (A4-adjacent: closes the crash-between-span-and-rearm window for every kind, not just this one); `JobKind::PostReceive` + `run_slice` arm; `schema_version` 4 for the two tables.
- **Scenarios (section 11).** Must pass 2, 4, 6, 14 (a declined push's pack is `dead` at the next Janitor slice; `ls-remote` unchanged). Stock `git` cannot supply a hook endpoint, so the harness registers either a `service:` target bound to a tiny hook Worker in the same `wrangler dev` project or an `https:` target on a local HTTP sink. Added scenario A: register a pre-receive hook that 403s with a two-line body on any update to `refs/heads/main`; `git push origin main` -> `! [remote rejected] main -> main (pre-receive hook declined)` plus both `remote:` lines; assert `pushes.state='rejected'`, pack `dead` after the next alarm. Added scenario B: post-receive endpoint fails twice then returns 200; advance the fake clock through the backoffs and fire alarms; assert >= 2 deliveries of the identical payload, covering only the accepted refs, and an empty outbox after.
- **Dependency direction** extends `edge -> hooks` and `jobs -> hooks` (REGISTRY); `hooks` imports `worker` + `wire::http` only, so `wire` and `store::codec` stay worker-free (1.1, A8).

## Depends on
- two-phase-push
- repo-do-ref-authority
- refs-sqlite-objects-r2
- gc-and-repack-alarm
- auth-and-multitenancy
- scoped-token-remotes (optional: narrower hook tokens)
- github-webhook-compat (optional: alternative post-receive payload)
