# Review: Git LFS natively via presigned R2 URLs

> Idea #11 · foundation · verdict: **lands with caveats** · feasibility 4/5 · reliability 3/5 · correctness 4/5 · effort: days
> Proof: [proofs/native-lfs.md](../../proofs/archived/native-lfs.md) · Review: [reviews/native-lfs.md](./native-lfs.md)

# Review: native-lfs (idea #11)

## Scores
- Feasibility 4/5. Every primitive is GA: DO SQLite, alarms, R2 binding `head()`, R2 S3 presigned URLs (1 s to 7 days, `X-Amz-SignedHeaders` honoured; R2 docs themselves show signing a `Content-Type` header to restrict uploads), aws4fetch in Workers. The proof's biggest hedge is resolved by the R2 changelog: "2023-06-16: S3 putObject now supports sha256 and sha1 checksums" and "user-specified object checksums are available in `get()`/`head()`" (2022-09-19), so `head.checksums.sha256` is populated for single-part S3 PUTs that carried the header. Bytes never touch the Worker, so the 128 MB / CPU limits are irrelevant to payload size. Deductions: needs an S3 API token as a Worker secret (no `presign()` on the binding), the S3 compatibility table still does not list `x-amz-checksum-*` on PutObject so the mismatch-rejection path (expect `400 BadDigest`) must be tested on day one, and multipart objects will not carry a sha256 at all.
- Reliability 3/5. Single DO serializes batch/verify per repo and content-addressed keys make re-uploads idempotent; no split-brain possible. But `verify` uses `UPDATE` not upsert, and the alarm can delete the pending row underneath it (below); the alarm is also re-armed on every batch, so a repo with steady traffic never sweeps.
- Correctness 4/5. This is the real LFS batch API with `basic` transfer plus `verify`, the same shape GitHub/GitLab serve, not a lookalike. Missing locks API is spec-tolerated. Missing input validation on `oid` is the one thing that turns a feature into a hole.
- Effort: days. ~150 lines plus edge routing and auth; a week including the R2 checksum conformance test and a git-lfs end-to-end run.

## Crash walk-through
Client batch-uploads oid X (row `pending`, alarm armed at +1h), PUTs 4 GB to R2 over a slow link, and the upload outlives the TTL. Presigned expiry is checked at request start, so the PUT still succeeds. Meanwhile the alarm fires: `head()` returns null (object not yet visible), row deleted. Client now POSTs `verify`: `head()` is fine, sha matches, `UPDATE ... WHERE oid=X` affects zero rows, DO returns 200, git-lfs reports success. R2 has the bytes, SQLite says nothing: every subsequent `download` batch answers `404 Object does not exist` and clones fail until someone re-pushes X (re-PUT is idempotent, re-verify then inserts). Not data loss, but a visible lie to the client. Fix is one line: `INSERT OR REPLACE ... 'ok'` in `verify`.
Second crash: DO dies between `head()` and `UPDATE` inside `verify`. Client gets 5xx, git-lfs fails the push, user re-pushes; batch hands out a new URL, PUT is idempotent, verify flips the row. Or the alarm adopts it on the next sweep because the hash matches. Clean.

## Concurrency walk-through
Two pushers upload the same oid X concurrently. Both batches insert/replace the `pending` row (second resets `created_at`), both PUT identical bytes to the same key (last writer wins with identical content), both verify and flip `ok`. Correct.
Verify vs alarm: DO input gates open during `await env.BUCKET.head()` (R2 I/O is not storage I/O), so the alarm's `DELETE` can interleave between verify's `head()` and its `UPDATE`; same zero-row outcome as the crash case, same upsert fix. Alarm vs batch: `setAlarm(now+TTL)` on every batch overwrites the pending alarm, so a repo that sees a batch at least hourly never sweeps; rows only, so it is a leak not a corruption. Guard with `if (!(await getAlarm()))`.
Cross-repo dedup is explicitly disclaimed; there is no R2 delete path at all, so orphan payloads (uploaded, never verified, row swept because the PUT was slow) accumulate until a global GC exists. Admitted in the proof.

## Interop check
- git-lfs 3.x with a `https://host/o/r.git` remote derives `/o/r.git/info/lfs/objects/batch`, sends `Accept`/`Content-Type: application/vnd.git-lfs+json`, and requires the same content-type back: done.
- `basic` upload: client echoes the `header` map verbatim, so `X-Amz-Checksum-Sha256` rides along; SigV4 lowercases so Go's canonical casing is harmless. Extra client headers (`Content-Type` from mime sniffing, `Content-Length`) are unsigned and ignored by the signature. `authenticated: true` correctly suppresses git-lfs's own `Authorization`, which would otherwise make R2 return 400 (two auth mechanisms). Path-style `<account>.r2.cloudflarestorage.com/<bucket>/key` is supported by R2.
- Download: presigned GET, client hashes the body against the oid locally; `Range` for resume is unsigned and passes. Per-object `error: {code: 404}` is the spec's per-object failure. Upload of an existing object returns no `actions`, which git-lfs treats as "skip".
- Exact wire detail that breaks as written: the verify href is `/o/r.git/info/lfs/verify` but the DO matches only `/verify`; unless the edge Worker rewrites the path, every push ends with `verify failed` (404) after the bytes are already in R2. Trivial, but the proof does not show the router.
- Second: `oid` is never validated as `^[0-9a-f]{64}$`. `new URL()` normalizes `..` segments, so `oid = "../../objects/<sha1>"` presigns a PUT to the git-object prefix in the same bucket. Only the checksum header (garbage base64 of NaN bytes) saves it, and only if R2 actually rejects mismatches. Validate before `key()`.
- `objects[].size` in download responses echoes the client's number instead of `row.size`; harmless for git-lfs, wrong per spec.
- No `expires_at`, no locks (client prints a warning and continues), no `multipart` transfer: objects over 5 GiB single-PUT fail, as stated.

## Blockers
1. `verify` must upsert, not `UPDATE`; otherwise the alarm/slow-upload interleaving strands verified objects as 404.
2. Validate `oid` against `^[0-9a-f]{64}$` before deriving the R2 key or signing anything.
3. Edge router must map `/info/lfs/objects/batch` and `/info/lfs/verify` onto the DO paths; the proof assumes it.

## Caveats
- Confirm on a real bucket that a presigned PUT whose body does not hash to the signed `x-amz-checksum-sha256` is rejected (S3 API table omits the header even though the changelog says it is supported).
- Alarm re-arm on every batch starves the sweep; use `getAlarm()` guard.
- Orphan LFS payloads are never deleted from R2; needs `gc-and-repack-alarm` to learn the `lfs/` prefix.
- Presigned hrefs bypass the CDN and any custom domain; hot downloads are R2 Class B ops per fetch.
- The batch handler does not check the caller's read/write scope per `operation`; delegated to `auth-and-multitenancy`, must not be forgotten.

## Verdict
lands-with-caveats. This is the same design every major forge uses on S3, every API it needs is GA on R2 today, and the checksum-in-signed-header trick is confirmed by the R2 changelog. The proof as written has one correctness bug (verify UPDATE), one input-validation hole (unvalidated oid into the key path), and one unshown router, all fixable in an afternoon; a stock git-lfs client should push and pull within days.
