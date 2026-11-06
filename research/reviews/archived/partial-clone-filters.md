# Review: Shallow and partial clone as first-class filters

> Idea #10 · foundation · verdict: **risky** · feasibility 3/5 · reliability 4/5 · correctness 3/5 · effort: weeks
> Proof: [proofs/partial-clone-filters.md](../../proofs/archived/partial-clone-filters.md) · Review: [reviews/partial-clone-filters.md](./partial-clone-filters.md)

# Review: partial-clone-filters (idea #10)

## Scores
- Feasibility 3/5. Every primitive is GA: DO SQLite with recursive CTEs + `json_each`, DO RPC, R2 `get` with `range.offset`, `TransformStream` response, `CompressionStream("deflate")` (zlib framing, correct for PACK entries). Memory is bounded (one 64 KB frame + one R2 chunk in flight). But the 1,000-subrequests-per-invocation cap (R2 binding calls count) is hit by the idea's own headline path, not just first clones (see Blockers). `plan()` is synchronous DO CPU: on a 1M-row `tree_entries` closure it blocks every push/fetch on that repo for seconds, and the single `objs` array return runs into the 32 MiB RPC payload cap around ~400k objects. Sibling inconsistency: `two-phase-push` and `streaming-pack-parser` write `objects/<sha>` (one of them zlib'd); this proof range-reads `objects/<owner>/<repo>/<2>/<38>` uncompressed. If the stored bytes are zlib'd the `range.offset` trick returns garbage.
- Reliability 4/5. Read-only path; no ref writes, no R2 writes, no state to split-brain. Only failure is a truncated pack when a planned object is missing in R2 (GC race), which git detects by SHA-1/EOF and the retry is stateless.
- Correctness 3/5. Filter semantics (`blob:none`, `blob:limit` with `size < n`, `tree:N`, explicitly wanted objects bypass the filter) and v2 section layout (`shallow-info` delim `packfile`, no acknowledgments after `done`, side-band-64k band 1, 65515 B payload) match upload-pack. But annotated tags are never emitted, `deepen` boundary detection is approximate, and the "one GET per blob" promise silently becomes "one request with N GETs" under real client behaviour.
- Effort: weeks. The CTE planner and streamer are days; fan-out for >1,000 objects, paging the plan, tag/`include-tag` handling, the missing `deepen-*` variants and composing with `want-have-negotiation` haves are the real work.

## Crash walk-through
Client sends `want <blob>` x 300 + `done`. `plan()` returns; Worker has streamed 120 entries when the isolate is evicted (or R2 GET #121 returns `null` because `gc-and-repack-alarm` deleted an object unreachable after a force-push). `w.abort` closes the socket mid-band; git prints "fetch-pack: unexpected disconnect while reading sideband packet" / "index-pack: SHA1 mismatch" and, for a promisor fetch, "could not fetch ... from promisor remote". Nothing server-side changed: no refs, no rows, no R2 keys. The retry recomputes the plan from the current index, so a repeated failure only happens if the index still lists an object R2 lacks -- an index/R2 skew that belongs to the GC design, not this idea. Verdict: no data loss, no orphans.

## Concurrency walk-through
Fetch F calls `plan()` while push P is between phase 1 (R2 PUTs) and phase 2 (`commit`). `plan()` has no `await`, so it runs atomically under the DO input gate and sees either pre-commit or post-commit `objects`/`tree_entries` rows, never a half-inserted set (two-phase-push commits rows and refs in one SQLite transaction). If F sees post-commit rows, every object is already in R2 because phase 2 only runs after phase 1 finished. Force-push P2 + GC while F streams the old tip: covered above (truncation, retry). Two fetches, or fetch vs `ls-refs`, share nothing mutable. The only cross-request hazard is starvation: a multi-second synchronous CTE blocks the DO's event loop, so pushes queue behind big blobless clones. No split-brain path.

## Interop check
- Wire detail that breaks: git 2.24+ `checkout` (and `read-tree`, `merge`, `diff` via `prefetch`) does NOT lazy-fetch blobs one at a time; `check_updates()` collects every missing blob and issues ONE `git fetch --filter=blob:none --stdin` with all of them. A blobless clone of a 5,000-file tree therefore produces one upload-pack request with 5,000 `want` lines. `fetchResponse` issues 5,000 `env.BUCKET.get` calls in one invocation, the runtime throws "Too many subrequests" at #1,001, the stream aborts, and the checkout fails every time. The proof's statement that 1,000 "covers every lazy promisor batch git normally makes" is wrong; the limit bites the idea's primary use case, not an edge case.
- Annotated tags: `git clone` wants every advertised tag oid. `plan()` accepts type-4 wants in `known` but `byType(4)` feeds no query, so the tag object is never in `objs`; the clone's connectivity check then lazy-fetches it, gets a 0-object pack, and fails ("remote did not send all necessary objects"). Also `include-tag` is ignored.
- `deepen N`: cap `c.d + 1 < N` and reporting depth-(N-1) commits with parents as `shallow` matches upload-pack for BFS-reached graphs; the real gap is that the client's own `shallow <sha>` lines only feed `unshallow` and never stop the walk, and `deepen-relative` / `deepen-since` / `deepen-not` are parsed away. Because `shallow` is advertised, a client on a shallow clone sends them (`git fetch --deepen=1`, `--unshallow`) and receives a wrong `shallow-info`, corrupting its `.git/shallow`. Either implement them or answer with an `ERR` pkt.
- Promisor fetch shape is correctly anticipated: no haves -> `done` in the first request, `filter blob:none` present, wants are terminal. `blob:limit` `k`/`m` suffixes must be parsed or git's `--filter=blob:limit=1m` is misapplied.
- Pack encoding: `entryHeader` varint (3-bit type, 4-bit low size, 7-bit continuation) is correct; trailer SHA-1 covers header + entries and is sent outside `band1` (not hashed) -- correct. Header count is exact because the plan precedes streaming. Non-delta pack is always acceptable to index-pack.
- `new Uint8Array([1, ...b.subarray(...)])` spreads 65 KB into an argument array per frame: works, slow; use `set`.

## Blockers
1. 1,000-subrequest cap vs batched promisor fetches: needs chunked fan-out via a self service-binding (each nested call = 1 subrequest, 1,000 GETs inside) or objects sourced from a pack via a `.idx`-like table with grouped range reads; unimplemented and hand-waved.
2. Annotated tag objects are never sent; any repo with a tag fails to clone.
3. Storage format contract with siblings (key layout, uncompressed loose bytes) must be pinned; the range-offset trick depends on it.

## Caveats
- Synchronous multi-second `plan()` blocks the repo DO; materialise to a temp table and page.
- Sequential GET+deflate with no prefetch window: 1,000 objects = 10-30 s wall; add ~16-way concurrency.
- Haves are ignored, so every incremental fetch on this path is a full re-send until `want-have-negotiation` is composed in.
- `deepen-*` variants, `combine:`, `sparse:oid`, `include-tag` absent; `blob:limit` suffixes unparsed.
- Per-blob Class B cost is acknowledged; `git log -p` on blobless clones multiplies it.

## Verdict
risky. The planner-in-SQLite / stream-from-R2 split is the right architecture and the wire framing is mostly correct, but the proof as written cannot survive a `git checkout` after a `blob:none` clone of any non-trivial repo, and cannot clone a tagged repo at all. Fixable, but the fix (fan-out) is the actual engineering and is not shown.
