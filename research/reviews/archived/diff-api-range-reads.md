# Review: Diff API served with R2 range reads

> Idea #27 · edge · verdict: **risky** · feasibility 4/5 · reliability 3/5 · correctness 2/5 · effort: weeks
> Proof: [proofs/diff-api-range-reads.md](../../proofs/archived/diff-api-range-reads.md) · Review: [reviews/diff-api-range-reads.md](./diff-api-range-reads.md)

# Review: diff-api-range-reads (#27)

## Scores
- Feasibility 4/5. Every primitive is GA: DO SQLite, alarms, R2 `get(key,{range})`, `node:zlib` `inflateSync` under `nodejs_compat`, `Response.json`. No presigned URLs, Queues, KV, D1 or WfP involved. Limits are respected on the fast path (one delta entry in memory); the fallback path holds up to `pack.depth` chain links on the recursion stack, so a 40 MB blob with a deep chain can exceed 128 MB. `inflateSync` on an over-estimated range is asserted, not verified, for workerd's zlib port; the proof already names the fix (store exact clen).
- Reliability 3/5. The endpoint is read-only, so no ref or object can be lost. Failure modes are stale-row-vs-deleted-pack races and 404 -> `obj!` TypeError (see walk-throughs).
- Correctness 2/5. Two problems. (a) The premise "packs already store deltas" is false in this stack: `streaming-pack-parser` resolves every delta and writes whole objects to `objects/<sha>`, and `gc-and-repack-alarm` explicitly emits non-delta packs ("No delta compression ... needs a delta encoder"). The "path-aware repack that deltifies against the same-path predecessor" this proof depends on does not exist in any sibling; `pinned-delta-bases` keeps only client-supplied deltas for a tiny allowlist of hot paths. Fast-path hit rate today: ~0%. (b) Even with such a packer, a git delta is a compression artefact, not a diff: copy ops may be out of order, overlapping or repeated (moved code), and matches shorter than the 16-byte rabin window surface as literal inserts of unchanged bytes, so `insertedBytes`/"uncovered base ranges = deletions" disagree with `git diff --numstat`. Any user-facing unified diff still needs the base blob (proof admits it), i.e. one full reconstruction, possibly a chain walk. What lands is "target-side-free byte-level change summary", a weaker lookalike.
- Effort: weeks for the endpoint on top of the existing foundation (pack_objects table, delta parser, tree-walk, Myers); months if the delta-producing repack is counted, which it must be for the fast path to ever fire.

## Crash walk-through
Diff request in flight: DO has read `pack_objects` rows for `from`/`to`, awaits `BUCKET.get(range)`; DO is evicted mid-await. Effect: the client gets a 5xx; nothing was written, no orphan, no split-brain. Retry is safe (pure read). The dangerous crash belongs to the repack: new pack multipart-completed to R2, DO dies before the `pack_objects` rewrite commits -> orphan pack in R2 (leak, caught by the next GC's "delete builds older than newest"), rows still point at the old pack which by sibling policy is not deleted until the *next* sweep, so reads stay valid. Acceptable, provided the table rewrite and the "covered" bookkeeping happen in one `transactionSync`, which the proof does not state.

## Concurrency walk-through
Push A (parser inserting rows for pushId A) while diff reader B asks for an oid from A. If the parser inserts `pack_objects` rows before the R2 put/multipart-complete resolves, B range-reads a key that is not there yet -> `obj` is null -> `obj!.arrayBuffer()` throws TypeError (500). Same shape with the repack: the DO is single-threaded but `await`s interleave, so B can read a row, the alarm can swap the table and enqueue deletion of the old pack, and B's `get` lands on a deleted key. The sibling's rule "loose objects survive one extra GC cycle" covers loose objects, but the proof's own `pack` column requires the same grace rule for packs and never states it. Two concurrent diff readers on one repo serialize through one DO; a commit-level diff over N files is N+ serialized R2 GETs plus tree reads, so a busy repo's diff API is bottlenecked on one isolate. No data loss, no split-brain refs (refs untouched).

## Interop check
No git wire protocol is spoken by this endpoint; `git` 2.4x is unaffected either way. The pack-format details that would break if wrong: (1) `clen` derived as "next entry offset - offset" is only computable after the parser inflates the entry (it does), and for the last entry must exclude the 20-byte SHA-1 trailer; (2) OFS_DELTA rows need `base_oid` resolved from the pack-relative negative offset via the `byOffset` map, REF_DELTA rows from the 20-byte sha; (3) `parseDelta` computes `off |= d[i++] << 24`, which goes negative in JS for base offsets >= 2 GiB, and `len` of 0 must map to 0x10000 (done). (4) Thin-pack deltas arriving on push are resolved and discarded by the sibling parser, so the raw delta bytes this API wants are never persisted unless the parser is changed to keep them.

## Blockers
- No delta-storing pack exists in the foundation: the parser writes resolved loose objects and the repack emits non-delta packs; a same-path-predecessor delta encoder (window search, rabin index) in a 30 s DO alarm is unbuilt and non-trivial.
- Delta ops are not a diff: non-monotonic copies, repeated copies and sub-16-byte literal inserts make the "zero-read summary" numerically wrong versus `git diff`; only "changed or not" is trustworthy.

## Caveats
- `inflateSync` tolerance of trailing bytes in workerd's `node:zlib` is unverified; persist exact compressed length instead.
- Fallback path can exceed 128 MB (chain depth x base size); needs a size cap that 413s.
- Pack swap and parser row insert need "R2 put complete before row visible" and a one-GC grace period for old packs.
- `TextDecoder` on delta inserts mangles binary blobs and split UTF-8 sequences; mark inserts as base64 or bytes.
- Commit-pair -> blob-pair tree walk is hand-waved and is where the real R2 read count lives.

## Verdict
risky. The mechanism (one range read per pack entry, decode the copy/insert stream) is sound and cheap to build, but it depends on a delta-producing repack this design does not have, and what the fast path returns is a compression edit script, not a diff. Without the encoder it degrades to "two full reconstructions + Myers in the isolate", which is the ordinary diff server the idea claimed to avoid.
