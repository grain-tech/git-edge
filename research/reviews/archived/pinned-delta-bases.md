# Review: Delta bases pinned per repo

> Idea #8 · foundation · verdict: **lands with caveats** · feasibility 4/5 · reliability 4/5 · correctness 3/5 · effort: weeks
> Proof: [proofs/pinned-delta-bases.md](../../proofs/archived/pinned-delta-bases.md) · Review: [reviews/pinned-delta-bases.md](./pinned-delta-bases.md)

# Review: pinned-delta-bases (idea #8)

## Scores
- Feasibility: 4/5. Every primitive is GA (DO SQLite `sql.exec`, alarms, R2 `get`, `nodejs_compat` `node:zlib`/`node:crypto`, streams). Two concrete code bugs: (a) `ctx.id.name` is undefined inside a DO created with `idFromName` (the name is not transmitted with the id), so `repoKey` yields `objects/undefined/<sha>` and every cold-path object is "missing"; fix by having the Worker pass owner/repo once and persisting it in SQLite. (b) `alarm()` deletes from `pins` while lazily iterating a cursor over `pins`; SQLite row visitation under in-flight mutation is unspecified, so `toArray()` first. `deflateSync` blocks the single-threaded DO and cold objects are fully materialised (128MB cap), both acknowledged.
- Reliability: 4/5. Pins/deltas are a pure cache over R2; worst case is a slower fetch, never data loss. See walk-throughs.
- Correctness: 3/5. Achieves the narrow "push then pull a minute later" workload. `clientHas(base)` is the entire safety argument and is deferred to a commit graph that only knows commits, not blob/tree reachability; shallow and partial-clone clients break it (see Interop).
- Effort: weeks. Gated on `streaming-pack-parser` exposing per-entry zlib boundaries (DecompressionStream does not report consumed bytes; needs `node:zlib` `inflateSync(..., {info:true})` or a wasm inflater) and on `want-have-negotiation` giving an ancestor test.

## Crash walk-through
Push P2 indexed; Worker calls `pinAndRecord`. All `sql.exec` calls run synchronously before the first `await` (`setAlarm`), so DO output-gating commits pins+deltas atomically or not at all. Crash before `setAlarm`: pins persist over budget until the next push arms the alarm; storage bloats by at most one push's hot set, no correctness effect. Crash mid-`fetchPack`: the TransformStream aborts, client `index-pack` sees a truncated pack (bad trailer / short read) and fails cleanly; the only server side effect is `last_used` bumps. If the ref flip and `pinAndRecord` are separate DO calls, a crash between them leaves pins for an unflipped tip: harmless garbage, evicted by LRU, but it means pins can describe objects the two-phase-push janitor later deletes from the pending prefix; `fetchPack` never wants them because no ref points there. No orphaned R2 objects, no ref split-brain: this idea never touches refs.

## Concurrency walk-through
Fetch F is streaming (interleaves at every `await emit`) while push P3 lands and the eviction alarm fires. Each entry's row is copied into JS memory in the synchronous window before its `emit`, so an eviction between awaits cannot tear an entry; the header count is fixed from `wants` up front and every want falls through to R2 if its pin/delta vanished. `INSERT OR REPLACE` on content-addressed oids is idempotent, so two racing pushes of overlapping objects converge. One real hazard: F's `wants` were computed from refs at negotiation time; if P3 force-pushes and a GC (idea 55) deletes now-unreachable loose objects before F reaches them, F throws `missing <oid>`, which is a fetch failure the client retries, not corruption. Pushes and fetches serialise on one DO, so a large cold fetch (hundreds of `deflateSync`) stalls all pushes to that repo for its CPU duration.

## Interop check
Pack format is right: `OBJ_REF_DELTA` = type 7 header carrying the delta's own uncompressed length, 20-byte binary base sha, zlib delta; mixed reused/fresh entries; SHA-1 trailer over everything. v2 `fetch` with `thin-pack` does make git run `index-pack --fix-thin`. What is missing from the proof: the v2 response framing (`packfile` section, band-1 sideband pkt-lines of at most 65520 bytes); the code returns a raw pack and silently assumes another layer chunks it. The wire detail that breaks: a client that cloned with `--depth=1` sends `have <tip>` plus `shallow` lines; the base for a stored delta was pinned at an ancestor of that tip, "reachable from a have" says yes, git receives `REF_DELTA` against an object it does not have and `index-pack --fix-thin` dies with `fatal: pack has 1 unresolved delta`. Same failure for `filter=blob:none` clients (idea 10), whose haves are real commits but whose blobs are absent. Neither case is handled; the fallback must be "no thin deltas when `shallow`/`deepen*`/`filter` appear in the fetch args".

## Blockers
- `ctx.id.name` is undefined inside the DO; `repoKey` must come from a persisted value, otherwise the cold path never works.
- `clientHas` must reject shallow and filtered clients (or refuse thin-pack for them); as written it produces packs real git cannot index.

## Caveats
- Delta zlib boundaries need an inflater that reports bytes consumed; DecompressionStream in the parser dependency cannot. Re-deflating the delta is a valid fallback (any zlib stream is fine on the wire).
- v2 sideband framing and `packfile` section header are assumed, not shown.
- Cold objects load whole into memory and deflate synchronously; anything over tens of MB needs LFS or the precomputed pack.
- Alarm eviction iterates a live cursor while deleting; materialise first. `deltas` is only bounded by pin eviction.
- Only client-chosen deltas are reused, no server-side delta chains; benefit is confined to fetch-soon-after-push.

## Verdict
lands-with-caveats. The pack-format reasoning is sound and every primitive is GA; the two blockers are small, mechanical fixes, but the shallow/partial `clientHas` hole would ship packs that stock git rejects.
