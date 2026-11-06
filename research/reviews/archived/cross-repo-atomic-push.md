# Review: Cross-repo atomic pushes

> Idea #48 · wild · verdict: **risky** · feasibility 3/5 · reliability 2/5 · correctness 2/5 · effort: weeks
> Proof: [proofs/cross-repo-atomic-push.md](../../proofs/archived/cross-repo-atomic-push.md) · Review: [reviews/cross-repo-atomic-push.md](./cross-repo-atomic-push.md)

# Review: cross-repo-atomic-push (#48, wild)

## Scores
- Feasibility: 3/5. Every primitive named is GA (DO SQLite, `transactionSync`, DO RPC with `ReadableStream` args, alarms, R2, `DecompressionStream`). But `req.formData()` buffers the whole multipart body in Worker memory, so the "streaming per repo" claim is false at the gateway: three packs share the Worker's 128MB, and a streaming multipart parser cannot hand out part N until parts 1..N-1 are consumed, so parallel `prepare()` needs buffering anyway. `this.ctx.id.name` is `undefined` inside a DO (the object does not know its own name), so `TxnDO.alarm()` calls `repo.commit(undefined)` and the re-drive path never works as written. A DO has exactly one alarm; `setAlarm` here silently clobbers the `gc-and-repack-alarm` schedule (a listed dependency) and vice versa.
- Reliability: 2/5. The 2PC skeleton (decide-guard `WHERE state='pending'`, participant never decides alone, `outcome()` converts undecided to abort, idempotent `commit`) is sound. The participant is not: see the lost-update below.
- Correctness: 2/5. Achieves all-or-nothing ref updates across N repos, but not the stated "push to three repos in one request" for a git client, and not isolation: a fetch between the phase-2 calls observes repo A flipped and repo B not.
- Effort: weeks, assuming `two-phase-push` and `streaming-pack-parser` already exist.

## Crash walk-through
Worker dies after `coord.decide()` returned `commit` but before `repo.commit()` reached repo B. B's `prepared` rows sit with `expires = t+30s`; its alarm fires, calls `outcome()`, gets `commit`, flips refs. Repo A was already committed by the Worker. Coordinator's 60s alarm would re-drive B too, but crashes on `commit(undefined)` (the `ctx.id.name` bug) and retries forever; harmless only because B self-recovered. No data loss, no split-brain. Second scenario: Worker dies between `coord.begin()` and `decide()`. Fast participant A's alarm at 30s calls `outcome()`, which flips `pending -> abort`; slower participant C, still indexing a large pack, returns its yes-vote to a dead Worker and later aborts via its own alarm. Correct but it means the 30s participant timer starts per-participant at prepare completion, so any transaction whose slowest pack takes >30s longer than its fastest is always aborted by design, before the Worker ever gets to decide.

## Concurrency walk-through
Two writers on repo A ref `main`: txn T1 `prepare()` and a normal single-repo push P. In `prepare()` the CAS read (`SELECT sha FROM refs`, line 54) is followed by `await BUCKET.head(...)` (line 57) before the `INSERT INTO prepared` (line 59). DO input gates only block other events while *storage* ops are outstanding, not during R2 network I/O, so P runs during that await: P sees no `prepared` row, CAS passes against the same old sha, P writes `refs.main = X`. T1 resumes, inserts `prepared(old=stale, new=Y)`, votes yes, later `commit()` writes `refs.main = Y` unconditionally. P's update is silently lost, with no fast-forward check on Y over X. Same window for two concurrent transactions: the second `INSERT` hits the `ref` PRIMARY KEY and throws, rejecting the Worker's `Promise.all`; the first transaction's `prepared` rows survive 30s until its alarm aborts a transaction that could have committed. Fix is mechanical (do all `head()`s first, then CAS + insert with no await between, or `blockConcurrencyWhile`), but as written the lock does not lock. Also `commit()` re-applies `new` without re-checking `refs == old`, so any path that bypasses the `prepared` check corrupts history.

## Interop check
Stock `git` (2.4x, v2) cannot produce this request: there is no multi-remote receive-pack, and the endpoint is not a smart-HTTP service, so `git push` never talks to it. The proof's report-status is returned inside a JSON envelope, which no git client parses. If the fallback `git push -o txn=...` variant were built, the exact wire break is: git requests `side-band-64k` whenever the server advertises it, and then expects `report-status` framed inside band-1 sideband packets; `reportStatus()` here emits bare pkt-lines, which git rejects as a protocol error unless sideband is never advertised. The push-option variant also needs `push-options` advertised, three concurrent connections held open until the coordinator decides, and a client-side orchestrator that git does not provide. Object connectivity is checked for the tip only; a committing transaction whose pack lacks a parent produces a corrupt ref, not an `ng`.

## Blockers
- `prepare()` TOCTOU: await between CAS read and lock insert loses concurrent single-repo pushes (data loss).
- `this.ctx.id.name` is undefined inside the DO; coordinator re-drive path is dead code as written.
- Single DO alarm slot collides with `gc-and-repack-alarm` on the same repo DO; needs an alarm multiplexer.
- No client: the feature is unreachable from `git`; needs a wrapper that captures receive-pack bytes (a fake remote helper) or a server-side push-option protocol that is only sketched.

## Caveats
- Gateway buffers all N packs in memory (`formData()`); no streaming multipart in Workers, and request body caps (100MB free / 500MB paid) bound the transaction size.
- Per-participant 30s timer starts at each prepare's completion; heterogeneous pack sizes >30s apart always abort.
- Atomic but not isolated: readers can observe partially committed transactions between phase-2 calls.
- Coordinator DOs are never `deleteAll`'d; one per transaction accumulates forever.
- Aborted transactions leave paid-for R2 orphans until GC; `commit()` does not re-verify `old` at flip time.
- Depends on `two-phase-push` honouring the `prepared` table on the normal path, which is asserted, not shown.

## Verdict
risky. The coordinator side is a correct textbook 2PC over DO SQLite, but the participant has a real lost-update race, the recovery path has a fatal typo-class bug, and nothing here is reachable by a real git client. Fixable in weeks by someone who already has the pack parser and single-repo push landed.
