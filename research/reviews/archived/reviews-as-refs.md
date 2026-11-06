# Review: Pull-request review data as git objects under refs/reviews

> Idea #47 · wild · verdict: **lands with caveats** · feasibility 4/5 · reliability 3/5 · correctness 3/5 · effort: weeks
> Proof: [proofs/reviews-as-refs.md](../../proofs/archived/reviews-as-refs.md) · Review: [reviews/reviews-as-refs.md](./reviews-as-refs.md)

# Review: reviews-as-refs (idea #47)

## Scores
- Feasibility: 4/5. Every primitive is GA (DO SQLite + `transactionSync`, DO RPC, R2 put/get, WebCrypto SHA-1, `CompressionStream("deflate")`). No alarms, WebSockets, Queues, presigned URLs or beta APIs needed. Objects are KB-sized; CPU/memory/body limits are irrelevant. Point off because the proof does not fit the storage model its own `Depends on` list defines (see Blockers).
- Reliability: 3/5. Atomic ref+graph flip via `transactionSync`; objects-before-refs ordering means no dangling ref. But: no idempotency key (retry after commit = duplicate event), no CAS retry loop (loser gets a 500), and a client push into `refs/reviews/*` desynchronises the `review_events`-derived `seq` from the tree, after which the next server append can write a duplicate entry name into `events/` (`git fsck`: "contains duplicate file entries").
- Correctness: 3/5. The git-side idea is right: a fast-forward-only ref of ordinary commits is fetchable by any client with a refspec and survives `--mirror`. As written, though, `git fetch` of the ref would fail against the sibling stack (Interop check). "`git fsck` on the clone passes" is false whenever `ev.author` is not `Name <email>` (missingEmail/badEmail).
- Effort: weeks.

## Crash walk-through
`appendReviewEvent` PUTs blob, events tree, root tree, commit to R2, then runs the SQLite CAS. Crash after any PUT but before the transaction: R2 holds up to four unreferenced loose objects, ref and `commits` untouched, nothing advertised, GC reclaims them; client sees an error and retries, and because `at`/commit timestamp differ the retry produces a second orphan set. Crash after `transactionSync` commits but before the RPC returns: the DO output gate guarantees the write is durable, but the Worker/client sees a failure and retries, producing event N+1 with identical body. No data loss, no dangling ref; duplication is possible because there is no client-supplied idempotency key. A Worker-crash mid-`git push` into `refs/reviews/*` is handled by `two-phase-push` (objects first, ref flip last).

## Concurrency walk-through
Two reviewers comment on PR 42 at once. The DO is single-threaded but `await`s on R2 between reading `tip` and the CAS release the input gate, so both requests read tip T and seq n+1. Both write four objects to R2 (different SHAs: different bodies/timestamps). Request A's `transactionSync` sees `cur === T`, inserts commit, flips ref, inserts `review_events(42, n+1)`. Request B's transaction sees `cur !== T`, throws, rolls back. Result: ref is linear and correct (no split-brain), B's four objects are orphans, B's user gets "CAS failed; retry" with no automatic retry. Even if the CAS were skipped, the `review_events` PK would reject B. Mixed case: reviewer pushes `refs/reviews/42` with git while the server appends: same CAS on the same `refs` row, so one loses; but the winner-via-push adds entries the `review_events` table never sees, and the next server append reuses a `seq` already present as a filename.

## Interop check
A real `git` 2.4x client with protocol v2 would do `ls-refs ref-prefix refs/reviews/` then `fetch want <tip>`. Two details break on the wire against the sibling proofs this idea depends on:
1. Object bytes/keys. `content-addressed-r2-keys` and `streaming-pack-parser` store *uncompressed* `"<type> <len>\0"+content` at `objects/<owner>/<repo>/<sha>` (or `objects/<sha>` with a `type` customMetadata) and deflate per object while streaming the pack. This proof stores *zlib-compressed* loose bytes at `objects/<aa>/<bbbb...>` with no repo prefix. The pack builder either misses the key entirely or deflates already-deflated bytes and emits the loose header inside the pack entry. A PACK entry body must inflate to the bare content whose SHA-1 (over `<type> <size>\0` + content) equals the object id; index-pack would compute a different id, and the client fails with `fatal: did not receive expected object <tip>` / `error: remote did not send all necessary objects`.
2. Commit graph. `want-have-negotiation` computes the send set from `commits(oid,gen,tree)`, `parents(oid,parent,idx)` and `introduced(commit,oid)`. This proof inserts into a different `commits(sha,parents,tree)` shape and never populates `parents`/`introduced`, so the negotiation walk finds no objects to send: the ref is advertised but the packfile section is empty, same client error as above.
Also: tree sorting uses `localeCompare` (ICU, locale-dependent) rather than bytewise `memcmp` with the `/` suffix on directories; safe for the fixed names used here but not for arbitrary pushed names. `author` needs `Name <email>` or `transfer.fsckObjects=true` clients reject the fetch.

## Blockers
- Store review objects in the same key layout and byte format (uncompressed header+content, repo prefix, `sha1:` integrity) as `content-addressed-r2-keys`, or the fetch pack is wrong/missing objects.
- Populate `want-have-negotiation`'s `commits`/`parents`/`introduced` tables (and `refs-sqlite-objects-r2`'s `objects(sha,type,size)` index) for each synthesised object, or the ref advertises a tip the server never sends.

## Caveats
- Add an idempotency key per event and a bounded CAS retry inside the DO; today concurrent comments 500 for the loser and retries duplicate.
- Derive `seq` from the current `events/` tree, not `COUNT(review_events)`, so client pushes and server appends cannot collide; reject duplicate entry names in pre-receive.
- Sort tree entries bytewise; validate `author` as `Name <email>`; cap pushed tree size in pre-receive.
- Four R2 Class A puts and an O(n) tree rewrite per comment; sharding `events/` and periodic packing are needed before review-heavy repos are pleasant to clone.
- Pre-receive shape check does not verify `meta.json` is unchanged or that event JSON is well-formed; a client push can corrupt the review's semantics while remaining valid git.

## Verdict
lands-with-caveats. The core claim (reviews as fast-forward commits under `refs/reviews/*`, cloneable and mirrorable by stock git) is sound and uses only GA primitives. The proof code, however, would not produce a fetchable ref when dropped into the sibling architecture: the R2 byte format/key layout and the commit-graph tables disagree with the proofs it depends on. Fixing that plus idempotency, seq derivation and tree sorting is days of work; a usable review feature (sharding, materialized view, push validation) is weeks.
