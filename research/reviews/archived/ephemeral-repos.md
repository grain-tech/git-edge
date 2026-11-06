# Review: Ephemeral repos with a self-destruct alarm

> Idea #30 · wild · verdict: **lands with caveats** · feasibility 4/5 · reliability 2/5 · correctness 3/5 · effort: days
> Proof: [proofs/ephemeral-repos.md](../../proofs/archived/ephemeral-repos.md) · Review: [reviews/ephemeral-repos.md](./ephemeral-repos.md)

# Review: ephemeral-repos (idea #30)

## Scores
- Feasibility: 4/5. Every primitive is GA (SQLite DOs, alarms, R2 list/bulk-delete, idFromName). No CPU/memory risk in the alarm path (one list + one delete per tick). Two code-level landmines: (a) `ctx.id.name` is not populated inside the DO for ids made via `idFromName` — the R2 prefix becomes `repos/undefined/<epoch>/`; the name must be passed in on `/create` and persisted in `meta`. (b) `deleteAll()` drops the tables the constructor created, so every later request on the same live isolate throws `no such table: meta` (HTTP 500) instead of the claimed 404, until the DO is evicted. Schema must be re-ensured per request or right after `deleteAll()`.
- Reliability: 2/5. Design is sound; the code as written orphans R2 objects in three ways (below) and has no sweeper.
- Correctness: 3/5. It is "expire and wipe", not "delete the DO" (proof admits this; unavoidable). Interop is fine for the lifecycle surface.
- Effort: days on top of the listed dependencies (ref-authority DO, receive-pack, pack parser); the ephemeral layer itself is ~200 lines plus the fixes.

## Crash walk-through
Alarm tick 3 of a 5-page deletion: `list()` and `delete()` succeed, then the isolate dies before the cursor write / `setAlarm` commit. Alarm is at-least-once, so it re-fires; SQLite rolls back to the previous cursor; re-listing from that cursor simply skips the already-deleted keys (the cursor is a key position, not a snapshot). No loss, no split-brain. `deleteAlarm()` before `deleteAll()` is the right order. Two gaps: (1) Cloudflare retries a throwing alarm only ~6 times with backoff; a multi-minute R2 outage exhausts retries and the repo is left `state=expired` with objects in R2 and no alarm — forever. No cron/D1 sweeper exists. (2) The cursor table is unnecessary — restarting from the prefix head each tick is equally correct and removes the rollback subtlety.

## Concurrency walk-through
Two `git push` to `refs/heads/main` from the same base: both await `BUCKET.put` (input gate is open during R2 awaits, so they interleave), then each runs the synchronous meta-check + CAS; the second gets `ng main fetch first`. Correct — the output gate guarantees the `ok` is not sent before the ref write commits. Loser's pack is orphaned until expiry, acceptable.
Push racing the alarm: pack is `put` after the alarm's final `list()` page ran; alarm does `deleteAll()`; push's `meta()` then throws (bug b) or, if fixed, returns `ng ... repo expired` — but the pack key is never deleted. Orphan.
Re-create racing deletion: `/create` is allowed when `state=expired`, does `INSERT OR REPLACE` over the old meta and `setAlarm(new expiry)`, which overwrites the chained deletion alarm. The old epoch's remaining pages are never deleted. The proof's "epochs never collide" claim is true but the old life's objects leak permanently. Must 409 while `state=expired` or track pending epochs.

## Interop check
v0 smart-HTTP advertisement shape is right (`# service=`, flush, `\0caps` on first line, `capabilities^{}` zero-id form for empty repo). Stock git ignores the unknown `expires-at=` capability token. Git 2.4x sends `Git-Protocol: version=2`; a server that answers with a v0 advertisement is legal and the client downgrades — nothing breaks. Two details that would break: `sideband1()` is applied unconditionally — if the client did not request `side-band-64k` (it will when advertised, but a `--no-...`/older client will not) the report-status is misparsed as garbage. And `report-status` requires the client to have asked for it; the handler always emits it, fine for stock git but the cap set is missing `delete-refs` so `git push :branch` gets a client-side refusal (a lifecycle non-issue, but "real client interop" claim overreaches). 404 on `/info/refs` does produce `repository 'X' not found` as claimed.

## Blockers
- `ctx.id.name` undefined inside the DO: R2 prefix must come from persisted meta, not the id.
- `deleteAll()` leaves the isolate with no schema: post-expiry requests 500 until eviction.
- `/create` during an in-flight deletion overwrites the deletion alarm and leaks the old epoch's R2 keys.

## Caveats
- Push-after-final-list orphans a pack; needs either a pending-keys table the alarm drains, or one extra list pass after refusing writes.
- Alarm retry exhaustion leaves zombies; add an R2 lifecycle rule on a date-bucketed prefix (day granularity) as a backstop, or a cron sweeper over a D1 index.
- TTL is "at least", not "exactly", one hour (documented).
- Incompatible with any cross-repo shared object layout (dedup/content-addressed keys) — documented, real.
- Receive-pack buffering the whole pack in memory violates 128 MB for large pushes; deferred to `streaming-pack-parser`, fine.

## Verdict
lands-with-caveats. The alarm-chained, cursor-resumable wipe with an epoch-scoped prefix is the correct shape and uses only GA primitives; the three blockers are each a few-line fix but the code as pasted would 500 after expiry and leak R2 storage on re-create.
