# Review: Content-addressed R2 keys

> Idea #5 · foundation · verdict: **lands with caveats** · feasibility 4/5 · reliability 4/5 · correctness 4/5 · effort: days
> Proof: [proofs/content-addressed-r2-keys.md](../../proofs/archived/content-addressed-r2-keys.md) · Review: [reviews/content-addressed-r2-keys.md](./content-addressed-r2-keys.md)

# Review: content-addressed-r2-keys

## Scores
- Feasibility: 4/5. Every primitive is GA: R2 binding `put` with `sha1` integrity option, `head`, `customMetadata`, DO SQLite (`new_sqlite_classes` migration required), `crypto.subtle.digest("SHA-1")`. R2 is strongly consistent, so HEAD-after-PUT is safe. Limits: paid-plan subrequest cap is 10,000/request and R2 binding calls count, so HEAD+PUT per object caps a single receive-pack request at ~5,000 objects (matches the proof's "few thousand" claim; it is a subrequest cap, not the "30s wall" the proof cites -- CPU is 30s default/5min max, wall is unbounded while the client stays connected). 128MB isolate means any single object over ~40-60MB (Uint8Array + header copy + inflate buffers) is unhashable; Web Crypto has no incremental SHA-1.
- Reliability: 4/5. Idempotent by construction for the object layer. One real hazard is the HEAD-skip vs GC-delete race (below); it is a cross-idea contract, not a bug in this code.
- Correctness: 4/5. It achieves exactly the stated goal (idempotent, retry-safe object writes) and nothing weaker. It hardcodes SHA-1; a client with `object-format=sha256` would silently produce wrong keys unless rejected at capability negotiation.
- Effort: days. ~100 lines shown are nearly the whole thing; the cost lives in the parser and two-phase-push it depends on.

## Crash walk-through
Push of 3,000 objects; Worker isolate dies after 1,800 PUTs, before the DO `/objects` POST. State: 1,800 R2 keys with byte-exact content, DO index unaware, refs untouched, client got no `report-status` (HTTP stream ended) and reports failure. R2 single `put` is atomic, so object 1,801 is either whole or absent -- no torn bodies. User re-runs `git push`: same objects, 1,800 HEAD hits (skipped), 1,200 PUTs, DO upsert, ref CAS. Converges. If the user never retries, 1,800 orphans sit in R2 (cost only) until `gc-and-repack-alarm` sweeps keys not in the DO index. Crash between DO `/objects` upsert and ref CAS: objects indexed but unreachable; same sweep. No data loss, no split-brain; refs never move on this path.

## Concurrency walk-through
Two clients push overlapping histories to the same repo simultaneously. Both HEAD-miss the same blob, both PUT identical bytes to the same key; R2 last-writer-wins but both writers carry the same SHA-1-verified body and the same etag (MD5 of body), so ordering is invisible. DO index upserts are `ON CONFLICT DO NOTHING`; ref CAS is serialized by the single DO. Fine.
The dangerous interleaving is push vs GC: push B HEADs object X (present, unreachable since a force-push), skips the PUT; GC deletes X; B's DO connectivity check HEADs X before the delete lands, ref advances pointing at a missing object -> fetch of that ref fails with "did not receive expected object". Fix is git's own rule: GC may only delete objects whose `seen_at`/R2 upload time is older than a grace window (hours), and connectivity check must re-HEAD after taking the DO write lock. Must be written into `gc-and-repack-alarm`.

## Interop check
Nothing in this idea touches the wire; git never sees R2 bytes. Hash input `"<type> <size>\0content"` is correct (size = content length, decimal, no leading zeros -- the code uses `byteLength`, correct). Exact details that bite adjacent ideas:
- `git push` sends thin packs by default (`send-pack` uses `--thin`): ref-deltas whose base is NOT in the pack must be fetched from R2 via `readObject` before hashing. The proof says "parser's job"; the parser then needs this idea's read path, a circular dependency to be explicit about.
- Serving requires the pack trailer SHA-1 over the whole stream; no incremental Web Crypto digest, so upload-pack needs a JS/Wasm SHA-1 -- the same gap this idea flags for big blobs.
- v2 `object-info` reply needs `size` per oid; the DO table has it, good.
- If `object-format` capability is ever advertised as sha256 the key scheme must change; today reject it.

## Blockers
- None for the idea as scoped. It is a correct foundation.

## Caveats
- Conflicts with `two-phase-push` as catalogued ("pending prefix"): R2 has no rename, so pending->final would be a second Class A op plus full byte copy per object. Content-addressed final keys make the pending prefix unnecessary; reconcile the two ideas (write final keys directly, use DO index + grace-window GC as the janitor).
- 2 subrequests per object -> ~5,000 objects per request on paid plan; larger pushes need chunked/resumable ingest or pack-level storage, as the proof concedes.
- Uncompressed storage is 2-4x R2 bytes and a `CompressionStream` per object on every fetch; the "R2 verifies for free" benefit is real but the CPU is paid on read instead of write.
- GC/push race requires a grace window contract documented in `gc-and-repack-alarm`.
- Large-blob memory ceiling (~50MB) is a hard failure mode without Wasm SHA-1 or LFS.

## Verdict
lands-with-caveats. The mechanism is exactly git's own content addressing mapped onto R2, uses only GA primitives, and is idempotent under crash and concurrent push. Its caveats are integration contracts (GC grace window, pending-prefix conflict, per-request object cap) rather than flaws in the idea.
