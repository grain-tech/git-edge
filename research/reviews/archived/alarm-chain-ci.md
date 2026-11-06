# Review: Push-triggered CI as a DO alarm chain

> Idea #14 · edge · verdict: **lands with caveats** · feasibility 4/5 · reliability 3/5 · correctness 3/5 · effort: weeks
> Proof: [proofs/alarm-chain-ci.md](../../proofs/archived/alarm-chain-ci.md) · Review: [reviews/alarm-chain-ci.md](./alarm-chain-ci.md)

# Review: alarm-chain-ci (idea #14)

## Scores
- Feasibility: 4/5. Every primitive is GA today: DO SQLite (`ctx.storage.sql`), alarms (one per DO, at-least-once, retried on throw), DO RPC via `extends DurableObject`, R2 get/put, service bindings; `limits.cpu_ms` up to 300 000 ms on Paid. Workers for Platforms is a paid add-on, not needed for the built-in-stage variant. 128 MB and the 1000-subrequest cap are acknowledged and bind only the stage author. Docked one point because the proof asserts that a second alarm never overlaps a running `alarm()`; that is only guaranteed while the handler is blocked on storage (input gate). During the `STAGE_WORKER.fetch` await the input gate is open, so a watchdog alarm that comes due while a stage is still running is at best queued and at worst delivered concurrently (PLAUSIBLE, not verified in docs) — either way the same stage runs twice in the same isolate.
- Reliability: 3/5. Chain body is sound (write `running` + watchdog before the subrequest; output gate makes those durable before the stage sees anything). Two real holes: the enqueue is not atomic with the ref flip (below), and `finish()` deletes the alarm before doing its R2 put and the cross-DO `set-ref`, so a failure there leaves a run that never terminates.
- Correctness: 3/5. It achieves "serial, ordered, retried stages triggered by push with no queue" — but the headline claim "CI can never be lost between ref moved and job enqueued" is false as written, and "real CI" is explicitly out of scope (Worker-only stages). The verdict-as-`refs/ci/<sha>` trick is genuinely interoperable.
- Effort: weeks (given the foundation slugs exist); the CiRun DO itself is days.

## Crash walk-through
Repo DO commits the ref flip in its own SQLite, then calls `CI_RUN.get(...).start()` over RPC. The two DOs have separate storage; there is no cross-DO transaction. If the repo DO isolate dies (or the RPC times out / CiRun namespace is briefly unavailable) after the ref commit and before `start()` returns, the client may or may not get `ok refs/heads/main`, but CI is silently never scheduled for that SHA — exactly the gap the proof says cannot exist. Fix: insert an outbox row (`ci_pending(sha, ref)`) in the *repo DO's* transaction alongside the ref flip and let the repo DO's own alarm drain the outbox with retries; `start()` idempotency by SHA makes the drain safe.
Second crash: stage 2 succeeds, `finish()` runs `deleteAlarm()` (line 76), then the `BUCKET.put` or the `set-ref` fetch throws. The alarm is gone, the last stage row is `ok`, no `refs/ci/<sha>` ever appears, and the orphaned verdict blob sits in R2 (harmless, content-addressed, but a janitor must know about `objects/` written by CI). If the throw propagates, the runtime's own alarm retry may or may not fire after an explicit `deleteAlarm` — unspecified; do not rely on it. Fix: model "publish verdict" as a terminal stage row and delete the alarm only after `set-ref` returns.
Mid-stage isolate death is handled correctly: `running` + watchdog persist, re-fire in 90 s, `attempts` bounds the loop.

## Concurrency walk-through
Two pushes land the same SHA on two refs (branch off main, push both): both map to `idFromName("owner/repo@sha")`, the second `start()` is a no-op, the `run.ref` row records only the first ref. No corruption, but the second ref never gets an "its own" run and any ref-aware stage sees the wrong ref. Acceptable if documented.
Force-push while a run is in flight: CiRun keeps running against the old SHA and later CASes `refs/ci/<oldsha>` with `old: null`; the repo DO serializes that against user pushes, so refs never split-brain. Re-run of a finished SHA is impossible as written (rows exist; CAS with `old: null` fails on the second publish) — needs a `rerun` method that resets rows and passes the current blob as `old`.
Long stage (> 90 s external runner): watchdog comes due while the fetch is outstanding; the stage executes twice and both invocations write the same row (last writer wins on `log`). Idempotent stages make this benign, but the proof should either lift `WATCHDOG_MS` above the stage's fetch timeout or tag each attempt and drop results whose `attempts` no longer matches.
`start()`'s check-then-insert is safe: no `await` between the SELECT and the INSERTs, and the DO is single-threaded.

## Interop check
`git push` (2.4x) uses protocol v0/v1 for receive-pack; v2 does not cover push. The client requests `report-status side-band-64k`, so the `unpack ok` / `ok refs/heads/main` lines must be wrapped in sideband band 1 (and a `git push` will hang on the flush otherwise) — that belongs to the receive-pack slug, but the CiRun RPC sits on that path and must not exceed the client's patience; better to do the RPC (or outbox insert) and then stream a band-2 line like `remote: CI scheduled <run-id>`.
`refs/ci/<sha>` pointing at a blob: `git ls-remote origin 'refs/ci/*'` works (v2 `ls-refs` with `ref-prefix refs/ci/`), and `git fetch origin refs/ci/<sha>:refs/ci/<sha>` works because the `want` is an advertised tip; upload-pack must be willing to pack a lone blob with no commit (thin-pack negotiation degenerates to "no haves", fine). Default fetch refspecs never pull `refs/ci/*`, so no noise, but `clone --mirror` / `push --mirror` will copy or delete them — document it.

## Blockers
- Enqueue is not transactional with the ref flip; the proof's central "cannot be lost" claim needs the outbox-in-repo-DO pattern before this lands.
- `finish()` ordering (`deleteAlarm` before R2 put and `set-ref`) can strand a run with no verdict and no pending alarm.

## Caveats
- Alarm overlap during a non-storage await is unverified against docs; either lift the watchdog above the stage fetch timeout or gate result writes on an attempt token.
- Same-SHA-different-ref pushes share one run; re-runs need an explicit reset path and a non-null CAS `old`.
- "CI" here means Worker stages (no shell, no Linux); arbitrary user code needs Workers for Platforms (paid) or the `hooks-as-workers` slug.
- Verdict blobs written by CI are new `objects/` keys the orphan janitor must tolerate; `gitBlobSha`/zlib helpers and the `set-ref` endpoint are assumed from other slugs.

## Verdict
lands-with-caveats. The chain mechanics (lease row + watchdog alarm + bounded attempts, verdict as a git blob under `refs/ci/`) are correct and buildable on GA primitives today; what does not hold is the atomicity story at the push boundary and at the terminal step, both fixable in a day with an outbox and reordering.
