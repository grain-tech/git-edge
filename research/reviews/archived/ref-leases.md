# Review: Ref leases

> Idea #44 · wild · verdict: **lands with caveats** · feasibility 5/5 · reliability 4/5 · correctness 4/5 · effort: days
> Proof: [proofs/ref-leases.md](../../proofs/archived/ref-leases.md) · Review: [reviews/ref-leases.md](./ref-leases.md)

# Review: ref-leases (#44)

## Scores
- Feasibility: 5/5. DO SQLite (`sql.exec`, `transactionSync`), alarms, and Workers are all GA; R2/KV/Queues are not touched. Workload is a handful of SQLite rows per push, far inside CPU/memory limits. Nothing speculative.
- Reliability: 4/5. Check + CAS + refresh sit in one synchronous transaction on a single-threaded DO, so there is no split-brain path for refs. Deductions: `armAlarm()` is `void`-ed inside `transactionSync` (an async storage call fired from a sync txn; harmless but sloppy), and the single-alarm-per-DO clobber with `gc-and-repack-alarm`/`alarm-chain-ci` is acknowledged but unsolved.
- Correctness: 4/5. Achieves the stated goal (branch locked to one principal for N minutes, enforced at the only ref-write choke point). Weaker than the prose in two spots: (a) text says holder pushes "refresh the lease" but code only refreshes when `-o lease=` is present; (b) holder granularity is the auth principal, not a session, so two jobs on one token cannot fence each other (admitted).
- Effort: days (2-4 dev-days on top of `two-phase-push` + `repo-do-ref-authority`).

## Crash walk-through
Scenario: client pushes `-o lease=15m`; phase one lands objects under `pending/` in R2; DO enters `commitPush`, `transactionSync` commits (ref moved, lease row written), then the DO is evicted before the report-status body is sent.
- Refs: committed atomically; ref points at `new`. No partial state.
- Lease: row committed with the ref update; holder is protected even though they never saw `ok`.
- Alarm: `setAlarm` may not have completed; expired rows linger until the next `acquire()` re-arms. Lazy `expires_at > now` check means a stale row never blocks anyone. Correct as claimed.
- Client: sees a transport error, retries with the same `old new`; CAS now fails (`ng fetch first`). Client fetches, sees its commit already on the branch. No data loss.
- Crash before commit: nothing in SQLite changes; `pending/` objects are orphaned in R2. That cleanup belongs to `two-phase-push`, not this idea, but this idea creates a new rejection path (`ng lease held`) that also leaves `pending/` orphans, so `two-phase-push` GC must treat lease-rejected pushes like any other rejection.

## Concurrency walk-through
Scenario: alice holds `refs/heads/main` (expires T+15m); bob and carol push concurrently; alice pushes too.
- `await req.json()` in `commitPush` is non-storage I/O, so DO input gates do NOT block other requests during it; three `commitPush` calls can interleave up to the `transactionSync` line. That is fine: all reads and writes of `leases`/`refs` happen inside the synchronous callback, which cannot yield, so the three transactions execute strictly one after another.
- bob: lease foreign and unexpired -> `ng refs/heads/main lease held by alice until ...`; CAS untouched.
- alice: passes lease check, CAS ok, ref moves; with `-o lease=` her lease is refreshed in the same txn.
- carol at T+15m+1ms with no lease: lease row still present (alarm not yet fired) but `expires_at > now` is false -> she gets through. Same instant, alice's refresh races: whichever enters `transactionSync` first wins; the other gets a `ng` (lease) or `ng fetch first` (CAS). No split brain, no double-write.
- `POST /leases` from CI and a push from the same principal: single-threaded acquire, `INSERT OR REPLACE`; last writer's TTL wins. Acceptable.

## Interop check
- Report-status v1: `unpack ok\n`, then `ok <ref>` / `ng <ref> <msg>` with free-text msg, then flush. Matches git's `receive_status()`; the client prints `! [remote rejected] main -> main (lease held by alice until ...)`. Correct.
- Wire detail that will break if missed: the server MUST advertise `push-options` in the receive-pack capability line of the `/info/refs?service=git-receive-pack` advertisement, or `git push -o lease=15m` aborts client-side with "the receiving end does not support push options" and never sends the request. The push-option block arrives as commands, flush, options, flush, PACK; the proof's parsing must expect that second flush.
- Second wire detail: if the client asked for `side-band-64k`, the entire report-status must be framed on band 1 inside the packed sideband stream, and if the server advertises `report-status-v2` the response format changes (`option` lines); the proof only supports v1, so it must not advertise v2.
- `--atomic`: if the server advertises `atomic`, a single `ng` must roll back every command; the proof loop commits `ok` lines independently. Do not advertise `atomic` (or implement it).
- Push is protocol v0 regardless of `protocol.version=2`; v2 has no receive-pack, so no v2 issue here.

## Blockers
- None technical. Requires `two-phase-push` and `repo-do-ref-authority` to exist first; on their own this idea is ~150 lines.

## Caveats
- `push-options` capability must be advertised; proof code never shows the advertisement.
- Alarm multiplexing across ideas sharing the DO is unsolved (admitted); until then the lease GC alarm can clobber GC/CI alarms or vice versa.
- Holder identity = auth principal; no per-job fencing token yet.
- No break-lease/admin override; a crashed holder blocks the branch for the full TTL (clamped 60 min).
- Leases are invisible to `ls-refs`/fetch; discoverable only via REST or a rejected push.
- Every lease-rejected push leaves phase-one objects in `pending/`; GC path must cover it.

## Verdict
lands-with-caveats. The mechanism is the right one (enforce at the DO's single ref-write transaction), the primitives are all GA, and the crash/concurrency stories hold. The gaps are integration-level (capability advertisement, alarm sharing, orphan cleanup), not design flaws.
