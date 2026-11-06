# Review: Refs in DO SQLite, objects in R2

> Idea #2 · foundation · verdict: **lands with caveats** · feasibility 4/5 · reliability 2/5 · correctness 3/5 · effort: weeks
> Proof: [proofs/refs-sqlite-objects-r2.md](../../proofs/archived/refs-sqlite-objects-r2.md) · Review: [reviews/refs-sqlite-objects-r2.md](./refs-sqlite-objects-r2.md)

# Review: refs-sqlite-objects-r2

## Scores
- Feasibility: 4/5. Every primitive named is GA today: SQLite-backed DOs (`ctx.storage.sql`, `transactionSync`), alarms, R2 binding put/get/range, streaming request bodies through a DO stub, `DecompressionStream("deflate")`, `crypto.subtle.digest`. Limits mostly respected, with two gaps: (a) `puts[]` retains every inflated `obj.body` until `Promise.all` resolves, so a push with a few hundred MB of blobs blows the 128 MB isolate; needs bounded concurrency and body release. (b) Pack trailer SHA-1 over a streamed response needs an incremental hasher; `crypto.subtle.digest` is one-shot, so a JS/Wasm SHA-1 is required (not listed). CPU (30 s) is fine for typical pushes, not for delta-heavy multi-hundred-MB packs, which the proof concedes.
- Reliability: 2/5. Ref CAS is correct, but the sweeper and the prefix handling have data-loss paths (below).
- Correctness: 3/5. The storage split genuinely delivers the stated goal (refs never touch R2, objects never touch SQLite). The sketch has several wire bugs that a real client would hit on the first request.
- Effort: weeks.

## Crash walk-through
Push A: 3 ref commands, 500 objects. The DO inserts `pending` and `objects` rows per object and fires R2 puts. Isolate crashes after 300 puts resolve, before `updateRefs`.
- Refs: untouched, correct (CAS never ran). No split-brain.
- R2: 300 orphans under the sha keys. Idempotent, harmless. Good.
- `objects` index: up to 500 rows now claim shas that may not exist in R2 (insert happens before the put resolves, and SQLite writes may have flushed). A later `updateRefs` validates only against this index, so a ref can be accepted pointing at a missing object; `uploadPack` then dereferences `o!` on a null and the fetch aborts mid-pack. Proof admits this but underrates it: it is the only integrity check on the write path.
- Sweeper: `setAlarm` is only called after a successful push, so a crashed push on a quiet repo leaves `pending` rows forever. Minor.
- Real hole: the alarm interleaves with in-flight pushes (DO is single-threaded, but requests interleave at every `await`; nothing uses `blockConcurrencyWhile`). Push A crashes at t0 leaving sha X pending. Push B at t0+59m includes X (`INSERT OR IGNORE` keeps created_at=t0), awaits its puts. Alarm from an earlier push fires, sees X stale, deletes X from R2 and from `objects`. B resumes; if X is a blob/tree deep in the closure, `updateRefs` succeeds and the new ref now points at a missing object. Data loss, reachable from ordinary retry behaviour.

## Concurrency walk-through
Pushes A (`S0->S1` on main) and B (`S0->S2` on main) arrive together. Both stream objects and interleave at `await Promise.all(puts)`. `updateRefs` is synchronous inside `transactionSync`, so whichever resumes first wins; the other gets `ng refs/heads/main fetch first`, exactly what git expects. Refs cannot split-brain: one DO, one SQLite. Two harmless interleavings: A's `DELETE FROM pending` wipes B's pending rows (B loses janitor coverage only), and `INSERT OR IGNORE INTO objects` dedups. However `this.prefix` is set from an `x-repo-prefix` header the Worker never sends, so every repo falls back to `objects/default/`. Object dedup across repos is then accidental, and repo A's alarm deleting "stale" sha X deletes it for repo B, which may reference it. Cross-tenant data loss as written.

## Interop check
Would `git 2.4x` (protocol v2 default) interoperate? Partly. It sends `Git-Protocol: version=2`; the server ignores it and answers v0, and git falls back cleanly. After that, exact details that break:
1. Worker routes `https://do/${rest}` with `rest` taken from `pathname`, so `?service=git-upload-pack` is dropped; the DO advertises `# service=null` and git aborts with "invalid server response". First request fails.
2. `pkt()` uses `s.length` (UTF-16 units) not byte length; any non-ASCII ref name (`refs/heads/función`) produces a malformed pkt-line.
3. No `HEAD` line and no `symref=HEAD:refs/heads/main` capability in the advertisement, so `git clone` ends with "remote HEAD refers to nonexistent ref, unable to checkout".
4. `git push` sends thin packs by default (`send-pack --thin`); ref-deltas reference bases the server already has, which live only in R2. `parsePack(reader)` has no R2 access, so it cannot resolve them. Every incremental push of a modified file fails.
5. A delete-only push (`git push origin :branch`) sends no PACK at all; `parsePack` must tolerate an empty body.
6. `uploadPack` ignores `have`/`done` and always sends the full closure; correct but every fetch is a clone-sized transfer until want-have-negotiation lands. Also the receive-pack capability string is reused for upload-pack advertisement; harmless, git ignores unknown caps.
The protocol shape (commands, 0000, PACK, `unpack ok`, per-ref `ok/ng`) is otherwise right.

## Blockers
- Query string dropped in Worker->DO routing (advertisement never works).
- Thin-pack ref-delta bases must be fetched from R2 during receive.
- Shared `objects/default/` prefix plus per-repo sweeper delete = cross-repo object deletion.
- Sweeper/push interleaving can delete objects a succeeding push is about to reference; sweep must run under `blockConcurrencyWhile` or check a live-push generation.
- `objects` index rows must be written after the R2 put resolves, not before.

## Caveats
- Bound put concurrency and drop bodies after put; incremental SHA-1 for pack trailers.
- HEAD/symref in advertisement; byte-length pkt-lines; empty-pack deletes.
- Per-object R2 GET per fetched object is fine for pushes and small fetches; clone performance depends on precomputed-clone-pack.
- Reconciliation `list` in the alarm to catch lost R2 writes.

## Verdict
lands-with-caveats. The architectural claim is sound and every primitive is GA; the split delivers exactly what it promises for refs, and object immutability makes retries safe. The sketch as written would not survive the first `git clone` and has two real object-deletion paths, but each is a bounded fix, not a redesign. Weeks to a working version once the pack parser and thin-pack resolution exist.
