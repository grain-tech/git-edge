# Review: GitHub-compatible webhook payloads

> Idea #49 · wild · verdict: **lands with caveats** · feasibility 4/5 · reliability 3/5 · correctness 3/5 · effort: days
> Proof: [proofs/github-webhook-compat.md](../../proofs/archived/github-webhook-compat.md) · Review: [reviews/github-webhook-compat.md](./github-webhook-compat.md)

# Review: github-webhook-compat (#49)

## Scores
- Feasibility: 4/5. Every primitive used is GA (DO SQLite + `transactionSync`, alarms, R2 GET, WebCrypto HMAC, outbound `fetch`, `randomUUID`). Nothing exotic; no Queues/D1/Vectorize needed. Payload build for <=20 commits is negligible CPU; fetch waits are wall time, not CPU. One point off because `setAlarm` sits *outside* the transaction (see crash walk) and the alarm drain does serial 10 s fetches x 20 rows = up to 200 s wall per alarm invocation.
- Reliability: 3/5. Outbox-in-same-transaction is the right pattern, but the drain has a poison-pill (`.one()` on a deleted hook throws, alarm retries 6x then dies, drain stalls until the next push), a lost-alarm gap, no ORDER BY, and silent drop after 5 attempts with no dead-letter.
- Correctness: 3/5. "Byte-for-byte GitHub `push`" is overstated. It is a *dispatchable lookalike*: ref/before/after/head_commit/repository.full_name/signature are right, but `added/removed/modified` are empty, `forced` is always false, no `X-Hub-Signature` (sha1), synthetic ids, and `head_commit` is null whenever `after` is not in the pushed pack (branch created from existing history). "Actions runners" as a consumer is wrong: GitHub Actions is not webhook-driven.
- Effort: days for the push event as written plus the fixes below; weeks for file lists + `forced` + create/delete/tag events.

## Crash walk-through
Push to `refs/heads/main`, phase 2 runs `commitPush`. `transactionSync` commits refs + N outbox rows (durable). DO is evicted before `await setAlarm(Date.now())` returns / is issued. Result: ref moved, rows persist, **no alarm exists**. Nothing delivers until the next push to this repo re-arms the alarm (could be days). No data loss, but "survives DO eviction" is only half true. Fix: call `setAlarm` inside the `transactionSync` callback (alarm writes participate in the storage transaction) or re-arm from the constructor via `blockConcurrencyWhile` + `SELECT MIN(next_at)`.
Second crash: alarm fetch returns 200, DO dies before `DELETE`. Row re-fires with the same `X-GitHub-Delivery` -> duplicate delivery; acceptable at-least-once and correctly claimed.
Third: hook deleted after enqueue. `SELECT ... FROM hooks WHERE id=?`.one()` throws on zero rows, alarm() throws, runtime retries ~6x with backoff then gives up; the trailing `setAlarm(MIN)` never runs, so *every* other pending delivery for the repo stalls. Needs `ON DELETE CASCADE` or a `maybeOne()`/skip.

## Concurrency walk-through
Two pushes A (main: X->Y) then B (main: Y->Z) 100 ms apart. DO serializes them, rows inserted in order. Alarm `SELECT ... LIMIT 20` has no `ORDER BY`, so SQLite may return B's row first; a subscriber that deploys "the sha I was told" can deploy Z then Y. Also: alarm() awaits fetch, which yields the DO; push B lands mid-drain and calls `setAlarm(now)`, overriding the running alarm's schedule -- fine, since the running alarm's final `setAlarm(MIN)` includes B's row. Refs never split-brain (single DO CAS); only delivery order is unspecified. Add `ORDER BY rowid`. N hooks x M refs rows per push fan out serially; 5 hooks x 4 refs x 10 s timeout on a dead subscriber = 200 s per alarm pass, and a single dead hook delays everyone's deliveries -- correctly noted as a limit, but it is the default path, not an edge case.

## Interop check
Git wire: unaffected. `commitPush` runs after phase-2 ref flip and before the report-status pkt-lines are flushed; `setAlarm` is awaited but is a local storage write, so `unpack ok` / `ok refs/heads/main` / flush-pkt return promptly on both v0 and v2 receive-pack (receive-pack has no v2 variant; git 2.4x uses v0 for push). Delivery is fully out of band. Nothing here would break `git push`.
Receiver side, concrete breaks: (1) Jenkins GitHub plugin verifies `X-Hub-Signature` (HMAC-SHA1); only sha256 is emitted, so signed-hook installs reject with 403 -- emit both like GitHub does. (2) Any `paths:`-style filter sees `added/removed/modified == []`. (3) `head_commit: null` on `created: true` pushes of existing commits (new branch from existing tip) -- go-playground/webhooks and Octokit tolerate it, some Slack/Discord formatters NPE. (4) Argo CD matches `repository.html_url`/`clone_url` against the app repoURL, so `env_base()` must be the real clone URL, not `git-edge.example`.

## Blockers
- None hard. The pattern is sound and buildable today.

## Caveats
- Poison-pill on deleted hook stalls the whole repo's outbox (must fix before shipping).
- `setAlarm` outside the transaction: eviction gap leaves rows undelivered until next push.
- No `ORDER BY` -> out-of-order deliveries; no dead-letter after 5 attempts.
- Missing sha1 `X-Hub-Signature`, file lists, `forced`, real ids; `head_commit` null for pushes of existing history.
- Serial fetch fan-out inside one alarm; move to Queues (commit-event-stream) for >3-5 hooks.

## Verdict
lands-with-caveats. Ships as a workable GitHub-shaped `push` event that most verifiers and dispatchers accept, not the byte-identical payload it claims; four small fixes (alarm-in-transaction, tolerate missing hook, ORDER BY, dual signatures) make it dependable.
