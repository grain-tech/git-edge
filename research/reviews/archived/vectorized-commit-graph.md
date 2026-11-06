# Review: Commit graph in Vectorize for semantic git log

> Idea #45 · wild · verdict: **lands with caveats** · feasibility 4/5 · reliability 3/5 · correctness 2/5 · effort: weeks
> Proof: [proofs/vectorized-commit-graph.md](../../proofs/archived/vectorized-commit-graph.md) · Review: [reviews/vectorized-commit-graph.md](./vectorized-commit-graph.md)

# Review: vectorized-commit-graph (idea #45)

## Scores
- Feasibility: 4/5. Every primitive is GA (DO SQLite, alarms, R2 get + DecompressionStream, Vectorize v2 namespaces/metadata range filters, Workers AI). bge-m3 is catalog-beta; bge-base fallback is fine. One hard limit is violated as written: Vectorize vector IDs are capped at 64 bytes and `${ctx.id}:${sha}` is 64+1+40 = 105 bytes, so every upsert is rejected. Use the bare sha as id (namespace already scopes it). Namespace names are also capped at 64 bytes; a DO id hex string is exactly 64, so it fits with zero slack. Vectorize also caps namespaces at 50k per index, so "one shared index" needs index sharding at 50k repos, not 5M vectors.
- Reliability: 3/5. No git data is ever at risk (refs and R2 objects are untouched), but the index silently diverges: poison rows, orphan vectors on force-push/branch delete/repo delete, and a head-of-line block described below.
- Correctness: 2/5. The proof itself concedes both halves of the idea title: `git log --semantic` cannot exist in a stock client, and diffs are not embedded, only changed path names. What lands is a server-side "semantic commit search" endpoint over message+paths. Useful, but a lookalike of the stated goal.
- Effort: weeks. The embed/query core is 2-3 days; tree diffing in R2, backfill throttling, reachability filtering, deletion, and the foundation dependencies (#1, #4, #6, #56) make it weeks.

## Crash walk-through
Push lands; phase 2 inserts 40 shas into `embed_queue`, sets alarm. Alarm fires, selects 32, reads commits/trees from R2, writes `commit_graph` rows, calls `AI.run`, then `VECTORIZE.upsert` succeeds, then the DO is evicted before the `DELETE FROM embed_queue` loop. Alarm retries (DO alarms auto-retry with backoff): the same 32 are re-read, re-embedded, re-upserted. Upsert is keyed by id so the index ends correct; cost is one wasted AI batch. No loss. Second, worse path: sha #17's object is missing from R2 (janitor swept a pending prefix, or the walk enqueued a tag/tree sha). `readCommit` throws *outside* the try/catch, `tries` never increments, the whole alarm throws, and every retry re-selects the same 32 rows in the same order. The repo's embed queue is blocked forever and all later pushes never get indexed. Fix: per-sha try/catch that bumps `tries`, plus a dead-letter state after 5.

## Concurrency walk-through
Two pushes to the same repo serialize through the DO (idea #1), so `enqueueForEmbedding` never races itself. The alarm handler, however, yields at `await BUCKET.get` / `AI.run` / `VECTORIZE.upsert`, and a receive-pack request can run in those gaps (input gates only block around storage ops). Interleaving: alarm selects batch {A,B}; push enqueues C and a *second* copy attempt of A (INSERT OR IGNORE, no-op); push calls `setAlarm(now+1s)`, overwriting the alarm's own `setAlarm(now+200ms)`. Outcome is still correct: A,B deleted after upsert, C drained on the next alarm. No split-brain because this path never writes refs. Cross-repo: each DO has its own namespace; concurrent upserts from thousands of DOs into one index are fine functionally, but Vectorize mutations are batched asynchronously, so a query issued seconds after a push misses the newest commits (proof admits this). Force-push race: branch B is rewound while alarm is embedding its now-unreachable commits; they are indexed and returned by `/log?q=` even though `git log` would never show them. The proof claims reachability is "re-applied in the DO" but `semanticLog` does no such filter.

## Interop check
Nothing on the git wire changes: receive-pack still replies `unpack ok` / `ok refs/heads/main` before any embedding runs, and stock git 2.4x with protocol v2 never sees the endpoint. If the optional v2 `search` command is advertised in the capability list, stock git ignores unknown capability lines, so `ls-refs`/`fetch` are unaffected. The one detail to watch: the connectivity walk in phase 2 yields *all* new object shas; the proof's `enqueueForEmbedding(newCommits)` must filter by object type or `readCommit` will choke on trees/blobs/annotated tags (see crash walk-through). Also, if `streaming-pack-parser` moves to packed storage (`precomputed-clone-pack`), the `objects/<sha>` key assumption breaks and reads need a pack index lookup.

## Blockers
- Vector id `${doid}:${sha}` exceeds the 64-byte Vectorize id limit; as written no vector is ever stored.
- `readCommit`/`changedPaths` failure is outside the retry accounting; one bad sha wedges the repo's queue permanently.

## Caveats
- Stated goal is not achievable: no `git log --semantic`, no diff embeddings. Deliverable is an HTTP/agent search endpoint over message + changed paths.
- No reachability filter at query time; force-pushed/deleted commits stay searchable. No delete path at all (Vectorize has no delete-by-namespace; must `deleteByIds` from `commit_graph`).
- Backfill of a large imported history runs at ~32 commits/s-ish on one single-threaded DO alarm and competes with pushes; needs a cap or a separate backfill DO. Neuron cost is per-commit and per-query with no query-embedding cache.
- 50k namespaces/index and 5M vectors/index both force index sharding; the code assumes one index.
- bge-m3 is beta in the Workers AI catalog; pin bge-base-en-v1.5 (768 dims) if GA is required.

## Verdict
lands-with-caveats. The re-scoped feature (semantic commit search endpoint) is buildable on GA primitives in weeks once the two blockers are fixed, but it is a weaker lookalike of "log --semantic with diffs" and needs reachability/deletion handling before it is trustworthy.
