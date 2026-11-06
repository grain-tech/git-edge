# Review: Precomputed pack slices for clone

> Idea #7 · foundation · verdict: **lands with caveats** · feasibility 4/5 · reliability 3/5 · correctness 3/5 · effort: weeks
> Proof: [proofs/precomputed-clone-pack.md](../../proofs/archived/precomputed-clone-pack.md) · Review: [reviews/precomputed-clone-pack.md](./precomputed-clone-pack.md)

# Review: precomputed-clone-pack (idea #7)

## Scores
- Feasibility 4/5. Every primitive is GA: DO SQLite, alarms, R2 range `get`, R2 multipart (incl. `resumeMultipartUpload` for cross-alarm builds), streaming `Response` + `TransformStream`. Data plane respects limits: DO is a row lookup only, Worker buffers <=65515 B + one R2 chunk, byte copying for a multi-GB pack is seconds of CPU spread over a long-lived streaming response. The unknowns are all in the hand-waved `writePack`: a DO alarm gets 1,000 subrequests per invocation and R2 binding calls count, so packing N loose objects means >= N/1000 chained alarms; CPU is 30s default (raisable to 5 min via `limits.cpu_ms`). If objects are stored loose in R2 (as idea #2 implies), "copy existing delta entries verbatim" has nothing to copy from.
- Reliability 3/5. No ref split-brain (single DO, refs never touched). But R2 write + SQL rows are not one transaction, old packs are never deleted in the shown code, and deleting one while a clone streams from it is unspecified.
- Correctness 3/5. The design is sound (v2 fetch with `done` and no haves legitimately skips the acknowledgments section; a superset pack is accepted by index-pack), but the proof code as written breaks every fast-path clone on the wire (see Interop), and the blobless trailer is computed incorrectly.
- Effort: weeks (fast path alone is days; the chunked builder is the real work and is shared with gc-and-repack-alarm).

## Crash walk-through
Alarm builds pack, `mp.complete()` succeeds, DO evicted before the `INSERT INTO pack_builds`. Result: a complete but unreferenced pack in R2 (storage cost, no data loss); the alarm is retried and writes a second pack. Crash before `complete()`: a dangling multipart upload, cleaned up only by an R2 lifecycle rule for incomplete multipart uploads (must be configured). Neither case affects clones: `cloneSlice` only sees committed rows, and a clone that races a fallback returns `null` from `BUCKET.get` and degrades to negotiation. Mid-stream Worker death truncates the pack; git fails with "unexpected EOF" and the retry is stateless. Needs a janitor that reconciles `packs/<id>/*` against `pack_builds`; not in the proof.

## Concurrency walk-through
Push A flips refs, sets alarm T+30. Alarm fires, reads `refs` into `tips`. Push B lands mid-build (DO interleaves fetch() during the alarm's awaits) and calls `setAlarm` again, which is fine: the running build keeps its captured tips, objects are immutable, and `pack_tips` records what was actually walked, so B's new tip fails `wants ⊆ tips` and clones negotiate until the next build. Two hazards: (1) a chunked build that re-reads `refs` per chunk instead of persisting the captured tips would pack a moving target; (2) two concurrent alarm invocations cannot happen (DO alarms are single-flight), but a manual "rebuild now" endpoint plus the alarm could double-build; harmless but wasteful. No split-brain path.

## Interop check
- Wire bug (would break every fast-path clone): `pkt = s => enc.encode(s.length.toString(16).padStart(4,"0") + s)` omits the 4 length bytes. `"packfile\n"` is emitted as `0009packfile\n`; git reads a 9-byte packet `packf`, then hits `ile\n` as a length field and dies with "protocol error: bad line length character". Must be `(s.length + 4)`. The band-1 `frame()` helper gets this right (5 + len), so the `sideband-64k` framing itself is correct: max pkt 65520 = 4 + 1 + 65515.
- Blobless trailer: the "snapshot the SHA-1 state at the blob boundary" trick is wrong. The blobless pack's header carries a different object count than the full pack's, so its SHA-1 diverges at byte 8; the boundary snapshot yields a trailer git rejects with "pack is corrupted (SHA1 mismatch)". Needs a second hasher seeded with the blobless header (cheap; counts are known after enumeration, before writing).
- Section structure is right: with `done` and zero `have`s the acknowledgments section MUST be omitted, so `packfile` + band-1 + flush is a valid response. Do not advertise `sideband-all` (git would then expect the `packfile` header itself in band 1) or `ref-in-want` (git would send `want-ref` lines the parser ignores). `include-tag` and `--single-branch` are satisfied by the superset pack, at the cost of shipping the whole repo for a one-branch clone.
- `ofs-delta` is sent by git whenever `repack.useDeltaBaseOffset` (default true); the fallback for its absence is correct. Rebased deltas must be topologically ordered so every ofs-delta base precedes it; ref-delta entries from pushed thin packs cannot be "copied verbatim".
- `bundle-uri` claim is wrong: a bare `.pack` is not a bundle (needs `# v2 git bundle` header + ref lines). `packfile-uris` is the v2 feature that takes a raw pack.
- Annotated tags: `git clone` wants tag refs; tags must be packed (before commits, or excluded from the blobless count as the proof notes). Filters other than `blob:none` are silently served the full pack: valid, but not partial.

## Blockers
1. pkt-line length bug in `pkt()` (fast path cannot complete a single clone until fixed).
2. Blobless trailer hash is computed over the wrong header; separate hasher required.
3. `writePack` is unspecified and is where feasibility lives: subrequest cap (1000/invocation), chunked multipart state, delta topo-ordering, and the loose-object storage format of idea #2 must be reconciled.

## Caveats
- Old-pack lifecycle (grace period, delete-while-streaming, orphan janitor) is not designed.
- Cost: O(repo size) Class A writes per push burst; needs push-rate-aware debounce.
- Staleness window >= 30s + build time after every push; busy repos may rarely hit the fast path.
- Whole-repo pack served for `--single-branch` / `--branch tag`; correct but not what the client asked for.

## Verdict
lands-with-caveats. The idea is real and the data-plane design is the right shape for Workers; the two wire bugs are trivial to fix once named, but the reviewer should not accept "see gc-and-repack-alarm" for the builder, which is the only part with genuine platform risk.
