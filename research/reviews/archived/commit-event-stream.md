# Review: Commit-as-event-stream

> Idea #34 · wild · verdict: **lands with caveats** · feasibility 4/5 · reliability 3/5 · correctness 3/5 · effort: days
> Proof: [proofs/commit-event-stream.md](../../proofs/archived/commit-event-stream.md) · Review: [reviews/commit-event-stream.md](./commit-event-stream.md)

# Review: commit-event-stream (#34, wild)

## Scores
- Feasibility 4/5 — every primitive named is GA (SQLite DOs + `transactionSync`, alarms, R2, Queues `sendBatch`/`ack`/`retry`/DLQ, Workers AI, outbound fetch, Vectorize/D1). Limits respected: `LIMIT 100` matches the 100-message `sendBatch` cap, 128 KB/msg noted, alarm re-arms in pages. Two unstated limits bite: a commit message can exceed 128 KB (must truncate), and the consumer loop is serial (100 msgs x AI + 10 s webhook blows the 15 min `queue()` wall budget unless batch size is small or the loop is `Promise.allSettled`).
- Reliability 3/5 — the transactional outbox is the right pattern and the DO-as-single-writer makes ref CAS sound. But the alarm is armed *outside* the transaction and the proof relies (silently) on DO write coalescing to make "commit + setAlarm" atomic; one inserted `await` strands rows with no sweeper. Multi-ref pushes double-insert events (see below).
- Correctness 3/5 — delivers "every commit object in the pack", attributed to *every* ref command in the push, not "every commit on ref X". Report-status is emitted raw, which breaks when the client negotiated `side-band-64k`.
- Effort: days for the outbox/alarm/queue/consumer on top of the foundations; weeks if `streaming-pack-parser` (delta resolution, thin packs) is not already done, since the commit-identification step depends on it.

## Crash walk-through
Push of 3 commits to `main`. `parsePack` writes 3 objects to R2 (content-addressed, so a retried push is harmless). `transactionSync` flips `main` and inserts 3 outbox rows. Isolate is killed right after `transactionSync` returns, before `setAlarm` resolves.
- Best case: there is no `await` between the sync commit and the `setAlarm` call, so DO write coalescing flushes both in one batch and the output gate holds the response; client either sees `ok refs/heads/main` with alarm armed, or nothing persisted. OK, but only by an undocumented-in-the-proof property; the proof text claims "the alarm is durable" without saying why the alarm exists at all after this crash.
- If a future edit adds an `await` (e.g. logging) between the two: ref moved, rows inserted, no alarm. Client gets no report-status, retries `git push`, gets "Everything up-to-date", and events sit until the *next* unrelated push to that repo. Fix: arm the alarm inside `ctx.storage.transaction(async)` or scan `outbox` in the constructor / on every `fetch` and re-arm.
- Crash after `sendBatch` resolves but before `DELETE`: rows re-sent on next alarm -> duplicate events. Acknowledged; consumers must key on `sha+ref`. Same at the consumer: AI + webhook done, crash before `ack()` -> duplicate webhook + double AI spend.

## Concurrency walk-through
Pushes A (main: o->a) and B (main: o->b) arrive together. `receivePack` awaits R2 puts (non-storage awaits), so the DO input gate lets A and B interleave during pack parsing; that is fine because each `transactionSync` is atomic and single-threaded. A's CAS wins, B gets `ng refs/heads/main fetch-first`. No split-brain. But B's objects are already in R2 (orphans, GC's problem) and, correctly, no events for B.
Second scenario, one push updating `main` and `feature` with a 5-commit pack: the inner `for (const ev of newCommits)` runs per command, so 10 rows are inserted and every commit is reported on both refs. Worse, if `main` fails CAS and `feature` succeeds, `main`'s commits are attributed to `feature`. Attribution needs a walk from each new tip over the pack's commit graph, which the proof admits it hand-waves.
Alarm racing a push: alarm reads rows [1..n], awaits `sendBatch` (gate opens), push inserts rows n+1.., alarm deletes `id <= n`. Safe. The push's `setAlarm(now)` may be overwritten by the alarm's backoff `setAlarm(now+2^k s)` in the catch path — a delay, not a loss.

## Interop check
Push is v0/v1 only (there is no v2 `receive-pack`), so protocol v2 is moot; git 2.4x sends `report-status` (or `report-status-v2` if advertised) plus `side-band-64k` whenever the server advertised it. Exact break: if the server advertises `side-band-64k`, the client expects the report-status stream wrapped in band-1 pkt-lines (`PKT-LINE("\x01" + PKT-LINE("unpack ok\n"))`...) followed by a flush; the proof returns bare `pktLines(status)` with no trailing `0000`, so `git push` prints "error: remote unpack failed" / hangs. Either do not advertise side-band or wrap.
Second wire detail: commits in a pack are frequently OFS_DELTA/REF_DELTA entries (type 6/7), not type 1, and `git push` sends thin packs (`--thin`), so REF_DELTA bases may live only in R2. `obj.type === "commit"` is only valid after delta resolution; the "free to identify" claim is wrong for deltified commits.
Minor: `INSERT OR REPLACE ... newSha` with a zero sha on a ref delete writes a bogus ref instead of deleting it.

## Blockers
- None hard; every primitive is GA and the pattern is standard. The idea cannot be demoed without `streaming-pack-parser` delta resolution, which it depends on but treats as free.

## Caveats
- Side-band-64k wrapping and trailing flush on report-status.
- Per-ref duplication / misattribution of events in multi-ref pushes; needs a reachability walk from each new tip.
- Alarm arming should be inside an async `transaction()` or backed by a constructor-time outbox scan; do not rely on write coalescing implicitly.
- Truncate commit messages to stay under the 128 KB queue message cap; parallelize the consumer and cap `max_batch_size`.
- At-least-once everywhere (outbox, queue, consumer); the webhook target must dedupe on `sha+ref`.
- Cost: one Workers AI call per commit with no per-push coalescing; a 5,000-commit push is 5,000 inferences.
- Ref deletion path is wrong (`INSERT OR REPLACE` with zeros).

## Verdict
lands-with-caveats. The transactional outbox + alarm + Queues design is the correct serverless shape and needs nothing beyond GA APIs, but as written it double-reports commits on multi-ref pushes, mis-handles side-band report-status, and only "sees" undeltified commits. A few days of fixes on top of a working pack parser makes it real.
