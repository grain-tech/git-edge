# Review: Two-phase push

> Idea #6 · foundation · verdict: **lands with caveats** · feasibility 4/5 · reliability 3/5 · correctness 3/5 · effort: weeks
> Proof: [proofs/two-phase-push.md](../../proofs/archived/two-phase-push.md) · Review: [reviews/two-phase-push.md](./two-phase-push.md)

# Review: two-phase-push (idea #6)

## Scores
- Feasibility 4/5. Every primitive named is GA: DO SQLite (`sql.exec`, `transactionSync`), alarms, DO RPC, R2 put/get/bulk delete (1000 keys), request-body streaming, `DecompressionStream("deflate")`. Nothing beta. Deductions: `DecompressionStream` does not report consumed input bytes, so object boundaries inside a pack cannot be found with it alone (needs a JS/Wasm inflater, delegated to `streaming-pack-parser` but it gates this idea); `crypto.subtle.digest` needs the whole inflated object in memory, so one 100MB blob (inflated + recompressed by `zlibLoose`) blows the 128MB heap; 30s CPU default is admitted.
- Reliability 3/5. Ref CAS inside the single-threaded DO is sound. But the janitor has a real data-loss race (below), a crash before the manifest is written leaks objects forever, and `commit()` re-checks expiry before an `await` rather than inside the transaction.
- Correctness 3/5. Achieves "atomic ref advance after durable object write", which is the substance of the idea (honest about not using a pending prefix for objects). Two concrete bugs: (a) `if (results.some(!ok)) return results` makes every push all-or-nothing while still reporting `ok <ref>` for refs that were NOT moved; the client believes the push landed. (b) report-status is emitted without sideband; if `info-refs` advertises `side-band-64k` (every real server does) git will die on the first byte.
- Effort: weeks (days for this file; the working version is gated on the pack parser and pkt-line/advert code).

## Crash walk-through
Worker dies after 10k of 50k object PUTs, before `pending/<id>/manifest.json` exists. Client gets a cut connection, retries: new pushId, same content-addressed keys rewritten (idempotent), commit succeeds. Alarm fires 15 min later for the first push: `BUCKET.get(manifest)` returns null, `m?.objects ?? {}` is empty, so it deletes the manifest that is not there and drops the row. The 10k keys the retry did not cover (if the user amended and re-pushed different content) are orphaned forever, and the proof's claim "the janitor never needs to list the bucket" is exactly what makes them unrecoverable short of `gc-and-repack-alarm`. Not data loss, but a permanent leak the proof says cannot happen. Fix: write the manifest in chunks as objects stream (`pending/<id>/part-N`), or have the sweep fall back to listing `objects/` against the index.
Second crash: Worker dies after `commit()` returned but before the response reached the client. Refs advanced, client sees error, next fetch reconciles. Matches upstream git; acceptable.

## Concurrency walk-through
Push A (shares blob X with nothing) crashed 20 min ago. Alarm starts: reads open manifests, computes `open` set, then `await`s R2 for A's manifest. DO input gates release during external I/O, so push C's `begin()` runs now and C starts PUTting `objects/X` (same sha as one of A's orphans). Janitor's `orphans` list was computed from state before C existed and C has no manifest yet, so `X` is not protected. Janitor `delete(objects/X)` lands after C's `put`. C finishes, writes manifest, `commit()` inserts `X` into the `objects` index, ref moves. Index says present, R2 has no bytes: silent data loss, discovered on the next clone as "missing object". Same race exists between `commit()`'s pre-`await` expiry check and a sweep at the 15-minute boundary. Fixes are small but absent: (1) alarm records `sweep_started_at` and aborts (re-arms) if any `pending` row has `started_at > sweep_started_at`, or logs deleted shas into a `swept(sha, at)` table that `commit()` intersects with its manifest and re-PUTs; (2) re-`SELECT` the pending row inside `transactionSync`.
Two concurrent pushes to the same ref: both `begin()`, both write objects, first `commit()` wins CAS, second gets `ng fetch first`. Correct, no split-brain, no lock. Shared blobs between the two are protected by the `open` set. Good.

## Interop check
- Push is not protocol v2: `git-receive-pack` speaks v0/v1 regardless of `Git-Protocol: version=2`; sibling #3 "v2 only" does not apply here and the advert/report code must be v0. The proof's pkt-line parsing (`\0caps` only on first line, flush, then PACK) is right.
- Exact breakage 1: git 2.4x sends `report-status side-band-64k object-format=sha1 agent=...` whenever the server advertised `side-band-64k`. The proof then returns `0009unpack ok\n` unwrapped; the client reads band byte `u` (0x75) and aborts with `protocol error: bad band #117`. Either never advertise side-band on receive-pack or wrap report-status in band 1 (and progress in band 2). `report-status-v2` similarly if advertised.
- Exact breakage 2: for bodies under `http.postBuffer` (1 MiB default) remote-curl sends the POST with `Content-Encoding: gzip`. The Worker must check that header and pipe through `DecompressionStream("gzip")` before pkt-line parsing, or every small push fails at the first pkt-line. Large pushes arrive chunked, uncompressed.
- Exact breakage 3: a delete-only push (`<sha> 0000...0 refs/heads/x`) sends no PACK at all; `parsePack(reader)` must accept EOF after the flush.
- Thin packs are the default on push; ref-delta bases missing from the pack must be fetched from R2 (acknowledged, parser sibling).
- Reporting bug (a) above is a correctness break with a real client: `git push a b` where `a` is stale prints `ok b` yet `b` did not move.

## Blockers
1. Janitor-vs-new-push race deletes an object that a newer push then indexes (data loss). Needs sweep-abort-on-newer-begin or a `swept` table checked at commit.
2. `commit()` reports `ok` for refs it did not update when any sibling ref fails CAS. Either apply the ok refs or report `ng` for all with `atomic` semantics.
3. report-status without sideband and no gzip request-body handling: no real git client completes a push until `info-refs-endpoint` and this Worker agree.

## Caveats
- Crash before manifest write leaks objects permanently; needs chunked manifests or a bucket listing fallback.
- Re-check pending row inside `transactionSync`, not before the R2 `await`.
- Connectivity error path `throw`s out of the DO, giving the client a 500 instead of `unpack error` pkt-line.
- Memory: whole-object SHA-1 and recompression; blobs approaching 100MB need a streaming hasher.
- One class-A PUT per object and a single JSON manifest cap practical push size around 10^5 objects; admitted.
- SQLite `objects` index is a second source of truth beside R2 key presence; every reader must honor it.

## Verdict
lands-with-caveats. The CAS-in-DO plus content-addressed idempotent writes is the right shape and needs no non-GA API. The proof as written would lose data under one sweep race and lie to the client in the mixed-result case; both are ten-line fixes, and the interop gaps are in the sibling handshake code. Weeks to a working push with a stock git client.
