# Review: Encrypted-at-rest with client-held keys

> Idea #37 · wild · verdict: **risky** · feasibility 4/5 · reliability 2/5 · correctness 3/5 · effort: weeks
> Proof: [proofs/client-key-encryption.md](../../proofs/archived/client-key-encryption.md) · Review: [reviews/client-key-encryption.md](./client-key-encryption.md)

# Review: client-key-encryption (#37, wild)

## Scores
- Feasibility: 4/5. Every primitive is GA (DO SQLite + `transactionSync`, alarms, R2 `put(stream)`/`head`/`get`, WebCrypto). No Worker limit is violated for small repos. Limits bite at scale: the push manifest is one `req.json()` in a 128MB DO (a 1M-object initial push is ~40-80MB of JSON), the serial `BUCKET.head` loop is O(objects) awaited I/O inside the repo DO, and one R2 PUT/GET per object makes a 100k-object clone 100k Class B requests. `new Uint8Array([...header, ...plain])` on the client spreads a blob into an argument list and will throw on multi-MB files; AES-GCM in WebCrypto is one-shot, so a 2GB blob cannot be sealed at all.
- Reliability: 2/5. No ref split-brain (CAS inside `transactionSync`), but there are two data-destruction paths and one permanent leak (see walk-throughs). Nothing is authenticated: the Worker PUT overwrites any existing ciphertext unconditionally and `/push` accepts a manifest from anyone.
- Correctness: 3/5. It does put only ciphertext in R2 and only hashes in the DO, so the stated goal is met literally. But it is not a git smart-HTTP server any more: it is a bespoke object store with a git-flavoured manifest, usable only through a remote helper the proof does not write. The "only key holders can push" claim in Known limits is false as written, and the DO takes the client's `introduced` list on trust (no fsck, no connectivity beyond commit parents).
- Effort: weeks.

## Crash walk-through
Client seals and PUTs 1,000 objects, dies after 500, then reruns. R2 holds 500 ciphertexts; DO is untouched; the retry re-PUTs byte-identical envelopes (deterministic HMAC nonce) and the push lands. Fine. Now the bad variants: (a) the `pending` table is never written by the Worker PUT path and never cleared by `/push`, so the sketched alarm janitor either does nothing (orphans leak forever, no GC exists for force-pushed history either) or, if implemented as the comment says, deletes ciphertexts 15 minutes after they were claimed by a successful push -- permanent loss of live objects. There is no "claim" step between upload and manifest. (b) DO commits the transaction, the response is lost; the helper retries with the same `old` and gets 409 `fetch first` although the push succeeded -- the helper must reconcile via `/refs`, which the proof does not do. (c) A client crashes mid-PUT of one object; `BUCKET.put(req.body)` with a truncated stream fails and R2 keeps the old value, so no torn object -- good.

## Concurrency walk-through
Two pushers A and B on `main` from the same base. Both pass the awaited `head`/parent checks interleaved (the input gate does not hold across R2 I/O), then each runs `transactionSync`; the second one's CAS mismatches, throws, and the transaction rolls back including its `INSERT OR IGNORE INTO commits`. Loser gets 409, refetches, retries. Correct, no split-brain. Different refs: both win. The real concurrency hole is at R2: a slow or malicious client (or a different repo key after rotation) PUTs `enc/<oid>` while that oid is already referenced by a committed ref -- unconditional `put` replaces the sealed bytes, and every fetcher now fails GCM auth on that object. Needs `put(..., { onlyIf: { etagDoesNotMatch: "*" } })` or an existence check, and needs auth. Ref deletion writes a `0000...` row instead of deleting, so `/refs` lists ghost refs afterwards.

## Interop check
A stock `git push https://git-edge.dev/o/acme/app` first does `GET /o/acme/app/info/refs?service=git-receive-pack`; the Worker regex only matches `/objects/<oid>`, `/push`, `/fetch`, `/refs`, so git gets 404 and prints `repository not found`. Even the intended path requires a `git-remote-edge` helper implementing the gitremote-helpers `capabilities`/`list`/`push`/`fetch` verbs, `refspec` handling, and writing fetched objects via `git hash-object -w -t <type>` (works, but per-object process spawns; a pack + `git index-pack` is the sane route). Protocol v2 is irrelevant here because git's transport is never spoken. The proof concedes this; the catalog title does not.

## Blockers
1. No authentication or authorization anywhere; unconditional R2 overwrite lets anyone destroy referenced ciphertexts; anyone can move refs.
2. Upload/claim/sweep protocol is undefined: `pending` is never populated or cleared, so the janitor is either a no-op or a data-loss bug.
3. No client exists; the remote helper (the only way a real git talks to this) is the bulk of the work and is unwritten.

## Caveats
- Metadata leaks: ref names, full commit DAG, per-commit object counts, ciphertext sizes; convergent encryption lets a server confirm known plaintext by oid.
- Per-object PUT/GET, no deltas, no packs: cost and latency scale linearly with object count.
- Manifest trusted from client; server cannot fsck or detect a missing-object push until a fetcher fails.
- Key rotation means resealing every object; key distribution is entirely out of scope.
- Kills every server-side plaintext feature in the catalog (merge, search, diff, semantic).

## Verdict
risky. The DO/R2 mechanics are sound and buildable today, but as written it is an unauthenticated object store with a leaking janitor sketch and no client. Add auth, conditional PUTs, a real upload-claim step, and a remote helper (weeks) and it becomes lands-with-caveats -- as a separate product, not as a mode of the smart-HTTP server.
