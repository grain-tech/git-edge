# Review: Tiny in-DO object cache with alarm-driven eviction

> Idea #9 · foundation · verdict: **risky** · feasibility 4/5 · reliability 4/5 · correctness 2/5 · effort: days
> Proof: [proofs/in-do-object-cache.md](../../proofs/archived/in-do-object-cache.md) · Review: [reviews/in-do-object-cache.md](./in-do-object-cache.md)

# Review: in-do-object-cache (idea #9)

## Scores
- Feasibility 4/5. Every primitive is GA: SQLite-backed DO (`sql.exec` with BLOB columns, 2 MB/value, 10 GB/DO), `setAlarm`/`alarm()` (one per DO, at-least-once), R2 `get`+`customMetadata`, `DecompressionStream("deflate")` (accepts zlib-wrapped streams), `blockConcurrencyWhile`. The pack-header varint and its decoder are byte-correct. Deduction: the memory tier has no admission budget at all between sweeps (see Concurrency), so the 128 MB isolate cap is not actually respected by the code as written.
- Reliability 4/5. Nothing here can lose data or split-brain: every row is immutable, content-addressed, re-fetchable from R2, and refs live elsewhere. The only failure mode is the DO itself dying (OOM) which takes ref serving down with it.
- Correctness 2/5. The mechanism achieves the stated goal only if R2 stores zlib'd bodies with a numeric `kind` metadata key. Every sibling proof stores the opposite: `content-addressed-r2-keys` PUTs the uncompressed `"<type> <size>\0"+content` with `customMetadata:{type,size}` (and explicitly says uncompressed is required for R2 `sha1:` verification); `streaming-pack-parser` and `refs-sqlite-objects-r2` PUT bare uncompressed `content` with `customMetadata:{type}` and no `size`. Against any of those, the miss path emits a pack entry that no git client can read (details below). The idea's "package.json, lockfiles" framing is also admitted to be a lookalike: admission is by oid recency, not path.
- Effort: days once the R2 body contract is settled (the file is ~100 lines; the fix is a `CompressionStream` on miss plus a size cap on the map). Weeks if `streaming-pack-parser`/`content-addressed-r2-keys` are still in flux, because this proof is a consumer of whichever encoding wins.

## Crash walk-through
Request misses on oid X: `await BUCKET.get` returns, `INSERT OR REPLACE` runs, `mem.set`, then the isolate dies before the response is flushed. DO output gate semantics: the SQLite write is either committed or rolled back atomically with the (never-sent) response; either outcome leaves a cache row that is a pure copy of an immutable R2 object, so a retry re-fetches or hits, both correct. `hits` (in-memory, unflushed) is lost, so `last_hit` for hot rows stays at insert time and the next sweep may evict rows that were actually hot; a cache-effectiveness bug, not a correctness bug. Alarm crashes mid-sweep (after `DELETE`, before `setAlarm`): alarms are at-least-once, the handler re-runs, deletes are idempotent, the alarm is re-armed. Cold start with a wiped alarm: `blockConcurrencyWhile` re-arms it. Verdict: no data loss, no orphans, no ref impact. Solid.

## Concurrency walk-through
`getObject` awaits R2 (external I/O), and DO input gates release during non-storage awaits, so N concurrent partial-clone follow-ups for the same oid all miss and all `INSERT OR REPLACE` the identical bytes: idempotent, fine. The dangerous case is N concurrent misses for *different* oids: a `git clone --filter=blob:none && git checkout` of a mid-size repo issues thousands of `want <blob>` follow-ups within seconds; each object up to 512 KiB is `mem.set` unconditionally and the map is only cleared by the 5-minute alarm. 10k blobs averaging 50 KiB is 500 MB in the heap; the isolate is killed at 128 MB, taking the ref authority for the whole repo down with it, and the next request cold-starts and repeats. The proof's own "Known limits" says the memory budget "must stay far below" 128 MB, but the code has no budget on the map, only on SQLite, and the SQLite trim also runs only at sweep time. Fix is trivial (cap `mem` by bytes with LRU eviction on insert, or skip the memory tier for the batch `fetch` path and use it only for `object-info`/raw reads), but it is absent. Refs are untouched by this idea, so no split-brain.

## Interop check
- Exact breakage: with R2 bodies as siblings write them, `z = await obj.arrayBuffer()` is raw uncompressed content (or `"blob 123\0"+content`), and `concat(packHeader(kind,size), z)` is spliced into the `PACK`. git 2.4x `index-pack` inflates the entry and dies with `fatal: pack has bad object at offset N: inflate returned -3` (zlib `Z_DATA_ERROR`, the first byte `0x62` 'b' or the content's first byte is not a valid zlib CMF/FLG pair). Every fetch that contains one cached-or-missed object fails, not just hits.
- Second breakage, same root: `Number(obj.customMetadata?.kind)` is `NaN` when siblings write `type: "blob"`; `NaN << 4` is 0, so the header type field is 0 (invalid in pack v2), and `size` is `NaN` when `size` metadata is absent (`streaming-pack-parser` omits it), so the varint is `0`. Client rejects the pack header before even inflating.
- What is right: pack entry = `varint(type,size) || zlib`, `PACK`+v2+count header, SHA-1 trailer; `DecompressionStream("deflate")` handles git's zlib wrapper for the raw API; deltified entries are avoided (siblings store fully resolved objects). A real client would interoperate once the miss path strips the loose header and runs `CompressionStream("deflate")` (a zlib stream of any compression level inflating to exactly `size` bytes is acceptable to `index-pack`). The SHA-1 trailer over the pack still has to be computed by a streaming JS SHA-1 (`crypto.subtle` is one-shot); not this proof's job but "no recompression on the hit path" is not "no CPU".

## Blockers
1. Body-format contract mismatch with all three sibling proofs (zlib vs uncompressed, `kind` numeric vs `type` string, `size` present vs absent). The proof as written cannot produce a valid pack against the repo the other proofs build.
2. Unbounded in-memory tier between sweeps; a single lazy checkout can OOM the repo DO.

## Caveats
- One alarm per DO is shared with `gc-and-repack-alarm`, `alarm-chain-ci`, `ephemeral-repos`; the proof admits the scheduler is hand-waved.
- `DELETE ... last_hit <= cutoff` can over-evict rows sharing the boundary millisecond; harmless.
- `hits` flush is one `UPDATE` per hot oid per sweep; thousands of writes every 5 minutes are billed and are the same magnitude the design claims to avoid on the hit path.
- Does not know filenames; "package.json/lockfile" prewarming is not delivered, only "small oids read twice".
- No read parallelism: every cached object still serializes through one DO; the win is R2 class-B op count and latency, not throughput.

## Verdict
risky. The cache skeleton, alarm eviction, and durability story are sound and cheap, but the byte-level output is wrong against the sibling storage contract and the memory tier can kill the DO. Both fixes are small and well-understood; until they land, this does not produce a pack git can read.
