# Review: Bisect on the server with parallel test Workers

> Idea #43 · wild · verdict: **lands with caveats** · feasibility 4/5 · reliability 2/5 · correctness 3/5 · effort: weeks
> Proof: [proofs/server-side-bisect.md](../../proofs/archived/server-side-bisect.md) · Review: [reviews/server-side-bisect.md](./server-side-bisect.md)

# Review: server-side-bisect (#43, wild)

## Scores
- Feasibility: 4/5. Every primitive is GA: DO SQLite (`sql.exec`), alarms (auto-retry up to 6x on throw), service bindings, Workers for Platforms (paid), R2 `get`, `DecompressionStream("deflate")` (zlib-wrapped, which is what loose git objects are). The 6-concurrent-subrequest cap and `limits.cpu_ms` up to 300s are quoted correctly. One dishonest line: the tester uses `env.BUCKET` directly while the payload carries a "scoped read token" it never uses. A tenant Worker in a dispatch namespace cannot be handed the platform's R2 binding without giving it every repo; the honest path is HTTP reads through the edge Worker, which makes each object read a subrequest (1000/invocation cap, 6 in flight) and turns a wide tree walk into a hard ceiling.
- Reliability: 2/5. Crash-safe (state in SQLite, deterministic picks), but two real bugs: (a) `alarm()` returns after `done`/`ambiguous` without re-arming, so any second `running` row queued behind it never runs; (b) picks are not filtered on `verdict IS NULL`, and rejected tester calls leave `verdict` NULL with no retry cap, so a round where all k probes fail or `skip` (while other untested interior commits exist) re-alarms at `Date.now()` with identical picks forever, a hot loop billing the tester indefinitely.
- Correctness: 3/5. It does find a first-parent culprit given monotonic verdicts, and `git bisect replay` can consume the log it emits. But the "parallel" claim is thin: k is capped at 6, wall time per round is one full test, and `git bisect run` locally already gets log2 N; the win is k+1 vs 2 per round, about 2.8x fewer rounds, not the headline. Non-monotonic verdicts (`good` at higher idx than a `bad`) drive `lo > hi`, `hi-lo <= 1` is true, and it reports the sha at `hi` as culprit instead of the conflict git reports. First-parent-only is disclosed but is a weaker lookalike of `git bisect` on merge-heavy histories.
- Effort: weeks. Orchestration is a few hundred lines; the tree walk, pack-index reads (most pushed objects are packed, not loose, unless content-addressed-r2-keys explodes packs), token-scoped object reads, and per-bisect child DO are the real work.

## Crash walk-through
Alarm round picks idx {250,500,750}, tester answers for 250 and 500 (two `UPDATE bisect_probe` writes), then the DO is evicted mid-fan-out. SQLite writes are durable in call order; `bisect.lo/hi` is untouched. Alarm retries (the runtime re-fires an alarm whose handler did not complete). `SELECT ... running` finds the same row, computes the same picks, re-calls the tester for all three (250/500 are re-tested since picks ignore existing verdicts; wasteful, not wrong), and shrinks. No data loss, no orphaned objects (bisect never writes R2). If the tester is non-idempotent (posts a status somewhere) it sees duplicates; the proof does not mention idempotency keys.

## Concurrency walk-through
Two users POST `/bisect` within the same second. Both `startBisect` calls run serialized on the DO; both insert `running` rows; the second `setAlarm(Date.now())` overwrites the first (one alarm per DO), harmless. `alarm()` takes `LIMIT 1` (unordered), runs bisect A to completion, hits `state='done'`, and `return`s without `setAlarm`. Bisect B sits at `running` forever; `GET /bisect/B` says running. Nothing else wakes the DO alarm unless a third bisect starts. Separately, a force-push during a bisect is fine: the chain is materialized in `bisect_probe` and objects remain in R2 absent GC; the DO input gate only blocks during storage ops, so pushes interleave with the awaited tester fetches without a split-brain on refs (bisect never writes refs).

## Interop check
No git wire protocol is involved; this is a REST endpoint plus a JSON tester contract, so a stock git 2.4x client is unaffected. The only git-facing surface is the optional `git bisect replay` log, which must be emitted as `git bisect start\n# bad: [<sha>] <subject>\ngit bisect bad <sha>\n# good: ...` with the leading `git bisect start` line, or `replay` errors with "couldn't run..." on the first unrecognized line. The proof names the format but shows no emitter. Object reads assume loose zlib objects at `objects/<sha>` (the prose says `get(sha)`, the code says `objects/${sha}`; pick one); if the store keeps packs, the tester needs idx fan-out + range reads and the "one R2 GET per object" claim fails.

## Blockers
- `alarm()` never re-arms after finishing a session; queued sessions starve.
- No `verdict IS NULL` filter on picks and no retry/backoff cap: all-fail or all-skip rounds hot-loop the alarm and the tester.
- Tester reads R2 via a platform binding, not the scoped token; multi-tenant isolation is not actually designed.

## Caveats
- Non-monotonic verdicts produce a wrong culprit instead of a conflict error.
- First-parent linearization only; merge-heavy repos get the merge commit, not the true culprit.
- `linearize` walks unboundedly if `good` is not a first-parent ancestor (throws only at root); cap the walk.
- `.one()` throws on zero rows, so the `row?.parent1` guard is dead code and the error message is misleading.
- Assumes loose objects; packed storage needs the precomputed-clone-pack index path.
- Speedup is bounded by the 6-subrequest cap; the practical gain over local `git bisect run` is small unless tests are slow and embarrassingly parallel.

## Verdict
lands-with-caveats. The idea is buildable on GA primitives and the state machine is the right shape, but the proof as written has a starvation bug, an unbounded retry loop, and an unaddressed tenant-isolation hole; fixing those is days, the packed-object and token-scoped read paths are weeks.
