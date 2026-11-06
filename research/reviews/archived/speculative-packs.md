# Review: Speculative packs

> Idea #35 · wild · verdict: **risky** · feasibility 3/5 · reliability 4/5 · correctness 2/5 · effort: weeks
> Proof: [proofs/speculative-packs.md](../../proofs/archived/speculative-packs.md) · Review: [reviews/speculative-packs.md](./speculative-packs.md)

# Review: speculative-packs (#35)

## Scores
- Feasibility: 3/5. Every primitive named is GA (DO SQLite, alarms, R2 range/lifecycle, DigestStream). Two limits are not respected: (a) prewarm does "one R2 range read per object"; R2 binding calls are subrequests, capped at 1000 per invocation (alarm included), so any incremental pack with >1000 objects throws mid-build unless ranges are coalesced; (b) `speculate` holds the whole pack as a stream, then `arrayBuffer`, then per-chunk `subarray` copies bound into SQLite: 2-3x of 32 MB is 64-96 MB against a 128 MB DO heap, so SPEC_MAX must be ~10-12 MB, not 32.
- Reliability: 4/5. It is a cache with a fallback; nothing here owns refs or objects. Worst case is a missed prewarm or an orphaned `spec/` object that lifecycle deletes.
- Correctness: 2/5. The `ready` one-shot shape is right, but the proof keys on `wants[0]`, ignores shallow/filter, and the sideband writer as written breaks every real client (see Interop).
- Effort: weeks, and only after #56, #4 and #6 exist; standalone it is a stub around three stubs.

## Crash walk-through
Push moves main A->B; `refMoved` inserts `pending_prewarm` and arms the alarm. Alarm fires, `speculate(A,B)` finishes `BUCKET.put("spec/A..B.pack")`, then the isolate is evicted before the synchronous SQLite inserts run. Outcome: R2 object without a `spec_packs` row (orphan, expires in 7 days via lifecycle); `pending_prewarm` row survives (its write committed in the push event); the platform retries the alarm, `speculate` re-checks `spec_packs`, rebuilds, and overwrites the same key. No data loss, no split-brain; cost is one wasted build. If the crash lands inside the chunk INSERT loop instead, the loop is synchronous, so all inserts and the `spec_packs` row commit or roll back together at the output gate. Fine.

## Concurrency walk-through
Alarm is mid-`speculate(A,B)` awaiting an R2 put (the input gate only serialises storage awaits, not R2 I/O, so the "DO serialises it with the next fetch" claim in the proof is wrong). A second push moves main B->C: `refMoved` does `INSERT OR REPLACE pending_prewarm(main, B, C)`; `getAlarm()` is null during handler execution, so a new alarm is armed. The running alarm loop then executes `DELETE FROM pending_prewarm WHERE ref='main'`, deleting the (B,C) row it never processed; the new alarm fires and finds nothing. Result: no B..C or A..C prewarm, clients pay the miss path. Not a correctness bug, but the coalescing story is broken; delete by `(ref, new)` not `ref`. Concurrent fetchers hitting the same cached pack are read-only and fine. A fetch interleaving during the build sees no `spec_packs` row until the sync insert block runs, so it is a clean miss.

## Interop check
1. `pkt("")` in the sideband transform emits a literal `0004` before every data frame. git's `demultiplex_sideband` treats a zero-length payload as `fatal: protocol error: no band designator`. Every hit and miss response dies on git 2.4x. The proof calls this a placeholder but it is in the serve path.
2. `want = wants[0]`: a default `git fetch` wants every changed branch tip. The cached `A..B` pack only covers main; the client runs `index-pack` fine, then `check_connected` fails with `error: remote did not send all necessary objects`. Cache key must be the full sorted want set (or the hit must require `wants == {B}`).
3. No `ref` on the wire: v2 `fetch` carries only OIDs (no `want-ref` unless ref-in-want is advertised), so `fetch_log.ref` has to be derived by matching `want` against the refs table at serve time; the signature `uploadPack(client, ref, ...)` assumes data the request does not contain.
4. Shallow and partial: CI runners (the stated target) mostly run `--depth=1`/`filter=blob:none`; those requests carry `deepen`/`filter` and expect a `shallow-info` section and a different object set. The cache must be bypassed when either is present, which removes most of the intended audience; what actually helps them is a cached depth-1 pack for the current tip, a different idea.
5. Miss path always writes `acknowledgments` + `ready` even with zero ACKs and even when the client sent `done`; git tolerates the section (it peeks for it) but a `ready` with no ACK after a partial have list makes the client stop negotiating and accept a pack the server built against haves it did not confirm. Must emit `NAK` and a flush when no common base was found and the client did not send `done`.
The thin-pack/ofs-delta assumption is acceptable: git sends both features by default, but the pack must be rebuilt (or the hit refused) when a client omits them.

## Blockers
- Sideband framing bug (item 1) breaks every response; trivial fix but must happen.
- Cache key must be the full want set, not `wants[0]` (item 2).
- Per-object R2 reads in the alarm exceed the 1000-subrequest cap for any non-trivial pack; needs range coalescing from the pack index (#4).

## Caveats
- Composition `A..B ++ B..C` is asserted only; it holds (disjoint object sets, deltas resolve inside the union or under A) but costs a full SHA-1 pass and is not in the code.
- Alarm CPU: 8 predicted haves x pack builds in one handler will exceed 30 s; the proof itself says "one pack per invocation and re-arm", which is the real design.
- Memory cap should be ~10 MB per speculated pack, not 32 MB.
- The prewarm hit rate is bounded to single-branch, non-shallow trackers; agent-harness DOs and mirrors qualify, GitHub-Actions-style runners largely do not.
- Client identity (`client`) is not on the wire; must be the auth principal or IP, both weak.

## Verdict
risky. The core insight is sound: v2 `ready` makes the fetch response a fixed wrapper around a deterministic `(A,B)` pack, so pre-building it is legitimate and safe (fallback is a normal miss). But as written it does not interoperate with git, is keyed on the wrong thing, and its prewarm loop hits a hard platform limit. With the three blockers fixed and #56/#4/#6 in place, it lands as a modest win for mirrors and agent sessions.
