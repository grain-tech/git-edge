# Review: Time-travel refs

> Idea #20 · edge · verdict: **lands with caveats** · feasibility 4/5 · reliability 4/5 · correctness 4/5 · effort: days
> Proof: [proofs/time-travel-refs.md](../../proofs/archived/time-travel-refs.md) · Review: [reviews/time-travel-refs.md](./time-travel-refs.md)

# Review: time-travel-refs (idea #20)

## Scores
- Feasibility: 4/5. Only DO SQLite (`sql.exec`, `transactionSync`), DO alarms, R2 get, and Request/Response streams — all GA. CPU/memory trivial for a point lookup. One real gap: a DO has exactly ONE alarm; the proof's `alarm()` calls `setAlarm(+24h)` and would clobber the gc-and-repack-alarm schedule unless both are multiplexed through a single dispatcher (not shown).
- Reliability: 4/5. Reflog row and ref CAS commit in one `transactionSync`, so log/ref can never diverge. Residual: reflog-expiry -> GC race (below), and no `ts` monotonic clamp (proof admits it is hand-waved).
- Correctness: 4/5. Achieves `main@{<time>}` semantics on the server side. Weaker than "any past state" only in ways git itself shares (server receive time, no HEAD symref through time). Wire gaps listed under Interop.
- Effort: days (on top of the refs-sqlite + v2 base).

## Crash walk-through
Push flips `main` A->B. Objects are already in R2 (base design uploads before CAS). DO crashes mid-`transactionSync`: SQLite rolls back both the `refs` upsert and the `reflog` insert; client sees connection error, retries, CAS `old=A` still matches, retry succeeds once. No orphaned reflog row, no ref without a log entry. Crash after commit but before `ok main` is sent: client retries, CAS fails `fetch first`, client re-fetches and sees B — normal git behavior. Crash inside `alarm()` mid-`DELETE`: single statement, atomic; alarm is retried by the platform. Verdict: no data-loss path for refs or reflog.

## Concurrency walk-through
Two pushers race on `main`: the DO is single-threaded and `transactionSync` is synchronous, so writer 1 commits A->B and writer 2's CAS `old=A` fails with `ng fetch first`. Reflog gets exactly one row per accepted flip, `seq` strictly increasing; `ORDER BY ts DESC, seq DESC` picks correctly even when both land in the same second. No split-brain because there is one authority DO. The one real race: reader does `ls-refs refs/at/T/main` -> sha S; expiry alarm deletes the row; GC alarm drops S's objects; reader's `fetch want S` now fails (`not our ref` or missing object). Window is bounded by expiry cadence and only affects refs older than retention; acceptable but should be documented as a "stale advertisement" error path.

## Interop check
- `refs/at/1735689600/main` passes `check-ref-format`; git 2.4x sends `ref-prefix refs/at/1735689600/main` for that refspec, gets one line back, then `want <sha>`. Works.
- BREAKS if the base server advertises `ref-in-want` in the v2 capability list: git >= 2.19 then sends `want-ref refs/at/T/main` inside the `fetch` command instead of `want <sha>`, and the proof only patches `ls-refs`. The `fetch` handler must also call `resolveAt()` for `want-ref` and echo a `wanted-refs` section. Either wire that or do not advertise `ref-in-want`.
- Enumeration under bare `refs/at/`: two flips of the same ref in the same second produce two lines with the same refname and different shas. `git ls-remote` tolerates it; `git fetch origin 'refs/at/*:refs/remotes/at/*'` will warn/refuse on the duplicate. Encode `<ts>-<seq>` or dedupe to the last per second.
- `pkt()` uses `s.length` (UTF-16 units), not byte length; a non-ASCII branch name in the enumeration produces a malformed pkt-line. Use `TextEncoder`.
- `resolveAt` fallback tries `refs/heads/x` then `refs/tags/x` then raw `x`: a branch deleted at T silently resolves to a tag of the same name. Minor, but not git semantics.
- ls-refs `symrefs`/`peel`/`unborn` args are ignored here; presumably handled by the base handler, but `peel` on a time-travel ref pointing at an annotated tag will not emit `peeled:`.

## Blockers
- `want-ref`/`ref-in-want` path unhandled if the base advertises that capability (it is the common default in a v2 server implementation).
- Single DO alarm must be shared with gc-and-repack-alarm; proof's `setAlarm` would overwrite it.

## Caveats
- Timestamps are server receive time; no monotonic clamp yet, so a DO restart with clock skew can make `ts<=?` pick the wrong row (seq ordering only helps within equal ts).
- Bare `refs/at/` enumeration is unbounded (tens of thousands of lines for a hot repo over 90 days); cap or require an epoch prefix.
- Expiry deletes rows and GC then drops objects; advertisements older than retention become dangling until the row is gone. Force-push history is the case users will actually want and the one most likely to be reaped.
- Byte-length pkt-line bug, same-second duplicate refnames, branch->tag fallback ambiguity.
- Any edge KV ref replica must proxy `refs/at/` to the DO.

## Verdict
lands-with-caveats. The core mechanism (reflog committed atomically with the ref, resolved at ls-refs time, served as a plain `want <sha>`) is sound and cheap. It is not shippable as written until `want-ref` is handled (or `ref-in-want` is not advertised), the alarm is multiplexed, and the pkt-line length/duplicate-name bugs are fixed — all days-scale work.
