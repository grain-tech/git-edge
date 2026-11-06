# Review: Wasm git core (gitoxide/libgit2) for delta resolution and merge

> Idea #25 · edge · verdict: **risky** · feasibility 4/5 · reliability 3/5 · correctness 3/5 · effort: weeks
> Proof: [proofs/wasm-git-core.md](../../proofs/archived/wasm-git-core.md) · Review: [reviews/wasm-git-core.md](./wasm-git-core.md)

# Review: wasm-git-core (idea #25)

## Scores
- Feasibility: 4/5. Every primitive is GA: `CompiledWasm` module rules + `WebAssembly.instantiate` on a bundled Module, SQLite DOs, `setAlarm`, R2 get/put/range, `limits.cpu_ms` up to 300 s on Paid, 10 MB script cap. No Asyncify/JSPI/threads needed. The one unknown is the Rust build: `gix-pack` delta apply + `gix-object` compile for `wasm32-unknown-unknown` with default features off, but `gix-merge` drags in `gix-command`/`gix-filter`/`gix-tempfile`; expect a week of feature-flag surgery or a vendored copy of the text driver. Memory: base+delta+result resident in one linear memory under the 128 MB isolate cap is honestly stated (~40 MB objects max).
- Reliability: 3/5. No data loss or split-brain (refs stay in the DO, R2 keys content-addressed), but three stall paths: unbounded `alloc` with no free/reset leaks linear memory across requests until the isolate OOMs; a missing base re-arms the alarm every 50 ms forever; `setAlarm` collides with two-phase-push's 15-min janitor (one alarm per DO).
- Correctness: 3/5. The Wasm boundary design (host does I/O, pure bytes-in/bytes-out) is right, and delta result type = base type, id = sha1("type len\0"+content) is right. But the ofs-delta base path is wrong against the sibling parser, chains are not handled, and Wasm is wired into the rare path (parked deltas) while the hot path (inline apply in streaming-pack-parser) stays JS.
- Effort: weeks (2-4).

## Crash walk-through
Push with 3 parked deltas; alarm fires `resolvePending`; DO evicted after `BUCKET.put(objects/<sha>)` for seq 1 but before the SQL `DELETE`. Alarm retries (alarms retry on throw/eviction); seq 1 is re-fetched, re-applied, re-`put` to the same content-addressed key: idempotent, no loss. Crash after SQL DELETE but before `BUCKET.delete(pending/..)`: orphan `pending/` key; two-phase-push's janitor sweeps it. Crash after the R2 delete but before the DO's storage write is durably committed (the output gate only guards outgoing messages, and the R2 delete already left): row survives, `pending/<pushId>/<seq>` is gone, and line 69's `!` non-null assertion throws on every alarm forever; the push never commits and never reports `ng`. Same terminal state for a thin pack whose ref-delta base the server genuinely lacks: `continue` leaves `remaining > 0`, alarm re-arms at +50 ms indefinitely, burning DO wall-clock and blocking the janitor.

## Concurrency walk-through
Push A (large, parked deltas draining via alarm) and push B (small) to the same repo. The DO is single-threaded but `await`s on R2 inside `resolvePending` yield, so B's `commitPush` RPC interleaves between A's slices. Both write `objects/<sha>` idempotently, `pending_deltas` rows are keyed by `push_id`, and the ref CAS lives in one DO transaction: no split-brain. Problem: B's commit calls `setAlarm(now + 15 min)` (janitor) after A's slice set `setAlarm(now + 50 ms)`; there is one alarm per DO, so A's drain is postponed 15 minutes and A's client sits in `git push` until that alarm (or the client's HTTP timeout, after which the push is neither acked nor rejected). Conversely A's +50 ms overwrite silently cancels the janitor. Needs a single scheduler that stores "next drain" and "next sweep" in SQLite and arms `min()`.

## Interop check
This proof touches no pkt-lines, so nothing here breaks `git` 2.4x / protocol v2 directly; the wire surface is two-phase-push's `report-status`. Format details checked: delta header = two 7-bit LSB-first varints, then 0x80 copy ops (size 0 => 0x10000) or 1..127 literal ops; `gix_pack::data::delta::apply` handles all of these. Two real gaps: (1) `loadBase` for `ofs-delta` range-reads `<pack-key>@<offset>` and calls `parseOnePackEntry`, but streaming-pack-parser never stores the raw pack in R2 (it writes loose `objects/<sha>` and parks only delta entries), so the key does not exist; and if it did, the entry at that offset may itself be a delta (git chains to depth 50), which the code would treat as content, hash to a garbage sha, and store as an orphan; the tip connectivity check then fails the push. (2) "byte-identical to `git merge`": `gix-merge`'s text driver uses imara-diff Myers; git's xdiff default (`diff.indentHeuristic=true`, marker labels `<<<<<<< HEAD` / `>>>>>>> <branch>`) can place hunks and labels differently in edge cases. Conflict detection is equivalent; byte identity is not guaranteed.

## Blockers
1. ofs-delta base resolution is inconsistent with the parser it depends on: either persist the raw pack under a key the parser records, or make the parser park `offset -> pending key` and resolve chains recursively (topologically by seq) in the DO.
2. Terminal-stall paths: missing base and missing `pending/` key must fail the push (`ng <ref> missing base <sha>`), not loop; cap retries per pushId.
3. Linear-memory leak: expose `reset()`/bump-allocator reset per call (or `free`), otherwise the per-isolate instance grows to the 128 MB cap and every request on that isolate dies.

## Caveats
- Alarm collision with the janitor (see concurrency); needs one scheduler.
- Wasm is only used on the parked-delta path; to earn "heavy lifting" the inline apply in streaming-pack-parser must call `applyDelta` too (it can: same bundle, same sync call).
- `gix-merge` on wasm32 is unverified; fallback is vendoring the ~1k-line text driver.
- Free plan 3 MB script cap is tight if tree-sitter (semantic-diffs) is also bundled.
- CPU: 64 deltas x 40 MB worst case can exceed 30 s; needs `limits.cpu_ms` or a byte-budget, not a row-count budget, per slice.

## Verdict
risky. The core idea (pure gitoxide subset in Wasm, TS does all I/O, one instance per isolate) is sound and buildable on GA primitives, but the proof code as written mis-resolves ofs-delta chains against its own dependency, leaks Wasm memory, and has two infinite-alarm stall paths. Fix those three and it becomes lands-with-caveats.
