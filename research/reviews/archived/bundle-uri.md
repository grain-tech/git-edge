# Review: Bundle-URI support

> Idea #12 · foundation · verdict: **lands with caveats** · feasibility 4/5 · reliability 3/5 · correctness 3/5 · effort: days
> Proof: [proofs/bundle-uri.md](../../proofs/archived/bundle-uri.md) · Review: [reviews/bundle-uri.md](./bundle-uri.md)

# Review: bundle-uri (idea #12)

## Scores
- Feasibility 4/5. Every primitive is GA: DO SQLite + alarms, R2 binding `put` with a `FixedLengthStream` body, R2 range `get` with `offset`, public bucket on a custom domain with Cloudflare cache in front, Cache API for the private route. Limits hold: nothing is buffered in the DO (get -> pipeTo -> put), copying is near-zero CPU, DO alarms get up to 15 min wall clock. Two real edges: single-`put` cap (~5 GiB, multipart not shown) and the CDN's cacheable-object size limit (512 MB on non-Enterprise plans), above which the "CDN" path is just R2 origin reads.
- Reliability 3/5. No split-brain: bundle refs are advisory copies under the client's `refs/bundles/*`; authority stays in the DO. But R2 put and the SQL row are not atomic, old bundle rows are deleted while their R2 objects are never removed, and the header is built from a ref snapshot taken at a different time than the pack (see Concurrency), which produces a bundle git rejects.
- Correctness 3/5. Response and capability wire shapes are right (`bundle-uri` bare in the advertisement, `bundle.version=1/mode/heuristic/<id>.uri/<id>.creationToken`, flush-terminated). Goal is achieved only for clients that opt in (`transfer.bundleURI=true` or `--bundle-uri`), so the "bulk bytes never pass through the DO" claim holds for near zero default clients; the precomputed pack path carries everyone else.
- Effort: days on top of gc-and-repack-alarm + precomputed-clone-pack (whose builder is the real work); weeks if counted end to end.

## Crash walk-through
Alarm: `BUCKET.put(...)` completes, DO evicted before the `INSERT INTO bundles`. Result: a complete `bundles/<id>/<token>.bundle` that nothing references (orphan, no data loss). The alarm is retried by the runtime, produces a second object under a new token, and inserts that one. Crash mid-put: the R2 put never commits (no partial objects), the alarm retries. Crash after `INSERT` but before the `DELETE ... LIMIT 2` is impossible (same synchronous SQL turn) but harmless either way. The proof has no janitor for `bundles/*` and the row-pruning `DELETE` never deletes R2 objects, so every rebuild leaks a full-pack-sized object forever; on a hot repo that is O(repo size) per minute of push activity. Client-side, a truncated download is detected by index-pack's trailing SHA-1 and clone falls back to a normal fetch with a warning: no corruption, just lost bandwidth.

## Concurrency walk-through
Push A lands, repack alarm builds full pack P1 covering refs {main=a}. Push B (main=b) lands, calls `/receive-pack-done`, bundle alarm fires 60 s later. `alarm()` reads `packs.full=1` (P1, built for tip a) and `refs` (main=b) and writes a header line `b refs/heads/main` over a pack that does not contain b. On the client `unbundle` succeeds (index-pack is fine) but `refs/bundles/heads/main` -> b fails with "Trying to write ref with nonexistent object"; clone warns and does a full ordinary fetch. Every bundle cut in a window where refs moved past the last repack is dead on arrival. Fix is trivial and mandatory: snapshot tips with the pack row (precomputed-clone-pack already records `pack_tips`) and write the header from that, never from live `refs`. Alarm-vs-fetch interleaving is otherwise safe (single-flight alarm, `setAlarm` during a running alarm just schedules the next). `fullClonePack()` deleting-while-streaming is the same unspecified hazard as idea #7.

## Interop check
- `command=bundle-uri` request has no argument section: `command=bundle-uri`, capabilities, then flush with no `0001` delim. The Worker parser must not require a delim (proof says the Worker parses on `command=`; make sure this shape is handled).
- Over smart HTTP, git only reaches `bundle-uri` via `stateless-connect` take-over in remote-curl (2.38+); each command is one POST to `/git-upload-pack`. Works with GA git 2.4x.
- Refs land as `refs/bundles/heads/main` (`refs/` stripped), not `refs/bundles/<id>/<refname>` as stated. Cosmetic in the proof, but matters if the DO ever special-cases those names.
- `bundle.heuristic=creationToken` is 2.40+. On 2.38/2.39 the key is unknown and `mode=all` means *all* listed bundles are downloaded and unbundled: the "keep newest two" listing doubles clone bytes there. List only the newest row; keep the older R2 object around for in-flight downloads.
- `have`s from `refs/bundles/*` do reach the server: fetch-pack's `mark_tips` walks all local refs. The DO's negotiation must tolerate haves it does not know (force-pushed-away tips) by ignoring them, which is standard.
- Bundle header over a valid pack: correct. v3 `@object-format` only needed for sha256, not advertised.
- `transfer.bundleURI` default remains false; `git clone --bundle-uri=<url>` against the R2 object works with no server changes.

## Blockers
1. Header refs are read from live `refs` instead of the pack's snapshot: any push between repack and bundle alarm yields a bundle git refuses to apply. Must use the tips recorded with the pack.
2. Bundle R2 objects are never deleted (only SQL rows are pruned): unbounded storage growth on active repos. Needs a grace-period delete or janitor.
3. Hard dependency on a self-contained full pack from gc-and-repack-alarm; without it there is nothing correct to wrap.

## Caveats
- Near-zero default uptake: only opt-in clients benefit; the DO-side value is really the shared object with precomputed-clone-pack.
- CDN cache size limit (512 MB non-Enterprise) and 5 GiB single-put limit bound the "big repo" story without multipart and an Enterprise plan.
- Private repos fall back to per-colo Cache API through an auth Worker; presigned S3 URLs bypass the cache entirely.
- Incremental bundles are out of scope; `fetch.bundleCreationToken` bookkeeping on the client is therefore wasted.

## Verdict
lands-with-caveats. Wire format and primitives are right and the design is a thin layer over idea #7; the ref-snapshot race and the R2 leak are real bugs but each is a few lines once pack tips are persisted with the pack.
