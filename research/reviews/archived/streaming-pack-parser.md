# Review: Packfile parsing in a Worker with a streaming inflater

> Idea #4 · foundation · verdict: **lands with caveats** · feasibility 4/5 · reliability 3/5 · correctness 3/5 · effort: weeks
> Proof: [proofs/streaming-pack-parser.md](../../proofs/archived/streaming-pack-parser.md) · Review: [reviews/streaming-pack-parser.md](./streaming-pack-parser.md)

# Review: streaming-pack-parser (idea #4)

## Scores
- Feasibility 4/5. Every primitive is GA: streaming `Request.body`, `nodejs_compat` `node:zlib` (default-on for compat dates >= 2024-09-23), SubtleCrypto SHA-1, R2 put/get/head, SQLite DO with `sql.exec`/`transactionSync`/`setAlarm`, `limits.cpu_ms` up to 300 000 (Paid plan). The one unverified point the proof itself flags: `inflateSync(..., {info:true}).engine.bytesWritten` in workerd's port. Node semantics were tested; workerd reuses Node's JS layer (`zlibBufferSync`) so it is likely fine, but it is a deploy-time check, and the fallback (pako/fflate exposing `next_in`) is a 1-day swap. Hard limit the proof omits: Workers request body cap (100 MB Free/Pro, 200 MB Business, 500 MB Enterprise) rejects big pushes before any byte is parsed. Memory: fine for typical pushes, but `sha1()` spreads the body into a JS array (`[...hdr, ...body]`), ~10x blow-up; a 10 MB blob turns into ~100 MB of heap and OOMs. Trivial fix, real bug as written.
- Reliability 3/5. No ref split-brain (single DO CAS) and no data loss on crash, but orphans are only partly swept and there is no existence/connectivity check before the flip (prose says so, code does not).
- Correctness 3/5. Achieves "parse a push pack in a Worker and land objects in R2" but not as titled (DecompressionStream is correctly shown unusable) and with two interop breaks below. Pack trailer SHA-1 is elided; `crypto.DigestStream("SHA-1")` (Cloudflare GA extension) solves it cheaply, proof missed it.

## Crash walk-through
Worker dies after object 4 000 of 10 000. R2 holds 4 000 `objects/<sha>` (immutable, unreferenced), refs untouched, client sees a broken connection and reports "the remote end hung up". Retry re-PUTs the same keys (idempotent) and succeeds. Correct, but: the orphans under `objects/` are only ever reclaimed by idea 55's GC, and the `pending/<pushId>` sweep alarm is set only inside `commitPush`, i.e. never for a push that crashed before the manifest, so pending junk lives until some later push to that repo sets an alarm. Crash after `transactionSync` but before the response: refs advanced, client sees failure, re-push gets `ng fetch first`; a `git fetch` shows the tip is already there. That is normal git-server behavior. DO crash mid-transaction: SQLite rolls back, all-or-nothing.

## Concurrency walk-through
Two clients push to `refs/heads/main` with the same `<old>`. Both parse in parallel in stateless Workers (fine, only CPU cost duplicated), both write overlapping content-addressed keys (harmless), both call `commitPush`; the DO is single-threaded so the second sees `cur !== old` and gets `ng main fetch first`. Correct fetch-first semantics. Weaker case: push A in flight while a GC/repack (idea 55) runs: A's objects sit under `objects/` and look unreachable, GC deletes them, then A's CAS succeeds and `main` points at a missing commit. The DO must at minimum `head()` each new tip and ideally refuse to flip while the janitor/GC holds the repo; the code does neither. Note also non-atomic multi-ref pushes are evaluated inside one `transactionSync`, so per-ref ok/ng is right but a thrown error rolls back refs the client was already told succeeded only if the throw happens before the response, so keep the report generation after the transaction as written.

## Interop check
Real `git` 2.4x pushes over smart HTTP with protocol v0 (receive-pack has no v2), thin packs (`--thin` default), chunked transfer above `http.postBuffer`, no `Content-Encoding` on the receive-pack body. Two exact wire details break the proof code:
1. Delete-only push (`git push origin :topic`) or a push whose commands are all deletes sends the pkt-line commands + `0000` and NO pack at all (send-pack only runs pack-objects when a non-delete command exists). `receivePack` does `ensure(12)` and throws "bad PACK header"; the client sees no `report-status` and errors.
2. Ofs-delta whose base was itself parked under `pending/` yields `byOffset.get()` undefined; the entry is stored with `base: ""` and can never be resolved. Rare with git-produced packs (bases are written before deltas), but any parked thin-pack base cascades into unresolvable chains.
Smaller: the pkt-line parser (info-refs-endpoint) and this parser each call `body.getReader()`; whatever the first one over-read past the flush pkt is lost unless they share the `Window`. `report-status` must be wrapped in side-band-64k band 1 when advertised; the proof knows this. Object sizes >= 2^31 corrupt the varint (`<<` is 32-bit), irrelevant under the 40 MB ceiling.

## Blockers
- Delete-only / pack-less pushes crash the parser (must branch on "no more bytes after flush").
- `sha1()` array-spread makes any multi-MB blob OOM the isolate.
- No tip-existence check before the ref flip; combined with a GC that treats `objects/` as loose, refs can point at nothing.

## Caveats
- `info:true`/`bytesWritten` under workerd unverified; fallback JS inflater ready.
- Request body caps (100/200/500 MB by plan) bound push size independent of CPU.
- Sequential `await BUCKET.put` per object and one R2 GET per delta base: a 10k-object push is ~5-10 min of wall clock; needs a per-push base cache and bounded-concurrency puts.
- Retry-from-start inflate costs up to 2x CPU on large objects; `cpu_ms` must be raised.
- Pack trailer SHA-1 elided; use `crypto.DigestStream`.
- Storing only loose objects in R2 pushes all delta reconstruction cost onto later fetches (other ideas' problem, but this design creates it).

## Verdict
lands-with-caveats. The mechanism is sound and every primitive exists today; the proof's honesty about DecompressionStream is its best part. Fix the three blockers (each < 1 day), verify the zlib `info` option on workerd, and this is a working receive-pack in about two weeks.
