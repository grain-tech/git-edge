# Review: Per-blob presigned direct upload for giant pushes

> Idea #24 · edge · verdict: **risky** · feasibility 4/5 · reliability 2/5 · correctness 3/5 · effort: weeks
> Proof: [proofs/presigned-direct-upload.md](../../proofs/archived/presigned-direct-upload.md) · Review: [reviews/presigned-direct-upload.md](./presigned-direct-upload.md)

# Review: presigned-direct-upload (idea #24)

## Scores
- Feasibility 4/5. Every primitive is GA: DO SQLite + alarms, R2 binding `head()`/streamed `get()`, R2 S3 presigned PUT via aws4fetch (7-day max), `crypto.DigestStream`. R2 changelog (2023-06-16) says S3 PutObject honours `x-amz-checksum-sha1`, and (2022-09-19) that user-supplied checksums surface on `head()`; neither is in the S3 compatibility table, so the reject-on-mismatch path is a day-one test, not a given. Worker limits are respected: bytes never transit a Worker, the trees-only pack is small, `rehash()` streams. Two paper cuts: aws4fetch's `unsignableHeaders` silently drops `content-length`, so the declared size is not actually enforced by the signature (only the checksum is, which is what matters); and R2 single PUT caps at 5 GiB, beyond which the unshown multipart flow has no checksum at all and every object goes through `rehash()`.
- Reliability 2/5. The janitor deletes R2 keys, and two independent interleavings turn that into a committed ref pointing at a missing blob (below). The proof's claim that "two concurrent pushes sharing a blob cannot race the janitor" is false because DO input gates open during `await env.BUCKET.head()`. Both fixes are small, but as written this loses data.
- Correctness 3/5. It does achieve the catalog's goal (blobs direct to R2, manifest to the DO) and the "manifest is the trees pack" trick is sound. But the client is bespoke by the proof's own admission, `resolveBlobs()` runs `await`s inside what `two-phase-push` describes as one sync SQLite transaction, and tree entries of mode `160000` (gitlinks) would be sent to `head()` and fail the push unless filtered.
- Effort: weeks. Server side is ~100 lines plus the `two-phase-push` hook; the real work is `git-remote-edge`, which must enumerate new big blobs, PUT them, then speak `git-receive-pack` itself (ref advertisement, command pkt-lines, `pack-objects --revs --thin --stdout --filter=blob:limit=8m`, `report-status` parsing) because `git send-pack` has no `--filter`. Plus the R2 checksum conformance test.

## Crash walk-through
Client batches oid X at T0 (row `claimed`, alarm at T0+61min), PUTs 4 GB over a 10 Mbit link (~55 min), and pushes at T0+62min. Presigned expiry is checked at request start so the PUT lands. Push reaches `commit()` -> `resolveBlobs(X)` -> `await head(X)` returns a good sha1. Input gate is open during that R2 await; the alarm fires, sees X `claimed` and older than cutoff, deletes the row and `waitUntil(BUCKET.delete(objects/X))`. `resolveBlobs` resumes and `INSERT ... 'ok'`. Ref advances; `objects/X` is gone. Every clone of that branch now fails on a missing blob and the `ok` row means no later `head()` will ever notice. That is data loss with no crash needed; a DO restart between `head()` and the INSERT merely fails the push (retry is idempotent) and is the benign case.
Fix: never delete R2 keys from this janitor (content-addressed keys are harmless; leave R2 GC to `gc-and-repack-alarm`), and re-read the row after every `await` before promoting.

## Concurrency walk-through
Direct-upload client A claims X and abandons it. Client B pushes the same blob through the ordinary `streaming-pack-parser` path an hour later: phase 1 PUTs `objects/X`, then A's stale claim expires and this janitor deletes `objects/X`; B's `commit()` trusts the manifest (two-phase-push does no R2 reads) and advances the ref. Missing blob again. The two-phase-push janitor consults open pushes before deleting; this one does not, and neither consults the other's table.
Ref race: P1 and P2 push the same branch. P1 is parked in `await head()`; P2's `commit()` runs to completion and moves the ref. If P1's old-value CAS was evaluated before its awaits, P1 then overwrites P2's ref: split-brain. `resolveBlobs()` must run before, and outside, the ref transaction, with the CAS re-done synchronously afterwards; the proof does not show the order.
If the key namespace is global `objects/<oid>` (as two-phase-push/streaming-pack-parser use, unlike content-addressed-r2-keys' per-repo prefix), repo A's janitor can delete a blob repo B committed long ago; the comment "if it is not referenced by any committed push" is not implemented.
Two direct-upload clients sharing X: both claim, both PUT identical bytes, first `resolveBlobs` promotes, second sees `ok`. Correct.

## Interop check
- Stock `git push` (2.4x, v2) cannot participate; the wire protocol has no "blob X already exists" and receive-pack has no filter. Correctly disclaimed. The only interop question is whether the custom server accepts what the helper sends.
- `git pack-objects --revs --thin --stdout --filter=blob:limit=8m` emits a valid PACK with dangling tree->blob edges; upstream `receive-pack`/`index-pack --strict` would reject it, and so will `two-phase-push`'s `links.every(known)` unless `known()` is the `resolveBlobs()` extension. The proof asserts the hook; the sibling proof does not have it yet.
- Exact detail that breaks: the remote helper cannot use the `connect` capability (git would then run its own `send-pack` and ship full blobs, defeating the idea). It must implement the `push` capability and drive `POST git-receive-pack` itself, including `report-status` parsing and `ng <ref> missing-blobs` mapping to `error <ref> ...`.
- Tree entries with mode `160000` are commit oids of another repo; sending them to `head()` yields `missing-blobs` on any repo with submodules. Skip gitlinks as upstream connectivity does.
- Presigned PUT must carry `x-amz-checksum-sha1` byte-identical to the signed value (it is in `X-Amz-SignedHeaders`); `Content-Length` is unsigned and free.
- `size` is inconsistent: batch stores git content size, promotion stores `head.size` (header + content). Whoever builds packs from the index must know which.

## Blockers
1. Janitor must not delete R2 objects; it races both this proof's own `resolveBlobs()` and the ordinary push path, and (with global keys) other tenants. Delete rows only.
2. `resolveBlobs()` must re-check row state after each `await` and run before the synchronous ref CAS transaction, not inside it.
3. Prove on a real bucket that a presigned PUT whose body does not hash to the signed `x-amz-checksum-sha1` returns 400 and that `head().checksums.sha1` is populated; otherwise the inline `rehash()` on a 4 GB blob happens inside the push request.

## Caveats
- `git-remote-edge` is the bulk of the effort and is unshown; without it the feature has zero users.
- Filter gitlinks; page `blobs/batch`; enforce count/size quotas at batch time since PUT bytes bypass the Worker.
- Multipart (>5 GiB) has no R2-side checksum; every such object costs a full read-back hash in an alarm.
- Key scheme must be reconciled with `content-addressed-r2-keys` (per-repo prefix) before shipping.

## Verdict
risky. The primitives are GA and the trees-only-pack manifest is the right design, but the proof's central reliability argument is wrong: the janitor's R2 delete plus open input gates during `head()` yields a committed ref with a missing blob in two ordinary interleavings. Drop the delete, move the await out of the transaction, and it becomes lands-with-caveats; the remaining cost is a bespoke remote helper that speaks receive-pack, which is weeks not days.
