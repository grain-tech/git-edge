# Review: Want/have negotiation with a commit-graph in SQLite

> Idea #56 · edge · verdict: **lands with caveats** · feasibility 4/5 · reliability 3/5 · correctness 3/5 · effort: weeks
> Proof: [proofs/want-have-negotiation.md](../../proofs/archived/want-have-negotiation.md) · Review: [reviews/want-have-negotiation.md](./want-have-negotiation.md)

# Review: want-have-negotiation (idea #56)

## Scores
- Feasibility: 4/5. Every primitive is GA (DO SQLite + `transactionSync`, DO RPC, streaming `Response`, R2 `get`, `nodejs_compat` zlib/crypto). Limits are the problem, not APIs: `queue.sort()` on every pop makes the walk O(n^2 log n) (a 50k-commit interesting set is billions of comparisons, blowing the 30 s DO CPU cap long before the "1M" the proof admits); `stub.negotiate()` returns the whole `objs` array over DO RPC, which is capped at 32 MiB (~800k 40-char oids); `obj.arrayBuffer()` per blob breaks on any object near 128 MB.
- Reliability: 3/5. No split-brain on the graph itself (single DO, `transactionSync`), but graph/refs/R2 consistency is delegated to sibling ideas without stating a transaction boundary, and a pack-stream failure leaves the client hanging (see crash walk).
- Correctness: 3/5. The negotiation logic is genuinely git's two-colour generation-ordered walk and the `introduced` union argument is sound (superset only). But the emitted packfile section violates pkt-line size and real git will die on it (see interop).

## Crash walk-through
Push: pack parser writes objects to R2, then calls `recordCommit` per commit. Scenario: Worker/DO dies after the R2 PUTs but before `recordCommit`, or after `recordCommit` but before the ref update (the proof never says those are one `transactionSync`). Outcome A (graph missing, ref updated): the advertised ref's tip is not in `commits`, `push(want)` silently drops it, `interesting` is empty, and the client gets an empty/ERR fetch for a ref the server advertised -- a permanent, user-visible inconsistency until a re-push. Outcome B (graph recorded, ref not updated): harmless, objects orphaned in R2 (content-addressed, retried push is idempotent via `INSERT OR IGNORE`). Also: `recordCommit` calls `.one()` on the parent's gen row -- pack object order is not topological, so a child processed before its parent throws mid-push, aborting the whole `transactionSync`; the parser must topo-sort commits first.
Fetch: `env.BUCKET.get` returns null or throws mid-stream inside the detached async IIFE. The `throw` becomes an unhandled rejection; the writer is neither closed nor aborted, so the client sits on a half-sent pack until Cloudflare tears the connection down (`fatal: early EOF` / `unexpected disconnect`). No server data loss, but needs `w.abort(err)` and an ERR sideband (band 3) frame.

## Concurrency walk-through
Two pushers to the same repo: both land in the same DO, which is single-threaded; each `recordCommit` is a `transactionSync`, so `gen` for a merge whose parents came from the other push is computed on committed rows. Fetcher concurrent with a push: `negotiate` is fully synchronous, so it sees an atomic snapshot of `commits`/`introduced`; the objects it then reads from R2 are immutable content-addressed keys already written before `recordCommit`. Safe. The one hazard is objects that reach R2 outside `recordCommit` (presigned upload) -- proof acknowledges it; they are invisible to fetch forever.

## Interop check
- Sideband framing is wrong: `emit` cuts 65519-byte data chunks, so a frame is 4 + 1 + 65519 = 65524 bytes. git's `packet_read_with_status` dies when `len - 4 >= LARGE_PACKET_MAX (65520)` with `protocol error: bad line length 65520`. git itself sends at most 65515 data bytes per band-1 frame (`LARGE_PACKET_MAX - 5`). Every clone of a repo with any blob over 64 KB fails against git 2.4x. One-constant fix, but it means the proof was never run against a real client.
- Section order, `0001` delim, `ACK <oid>`/`NAK`/`ready` spelling, omitted acknowledgments after `done`, `application/x-git-upload-pack-result`, and the OBJ_* varint header are all correct.
- `ready` semantics: says ready as soon as any ACK exists and no want is painted -- ok_to_give_up in git additionally requires every want to reach a common commit. Result is a superset pack; git accepts it, but an old orphan `have` can trigger a near-full-history pack.
- Missing for real clients: annotated-tag wants (need `tags` peel row or the walk starts nowhere), `include-tag`, `deepen`/`filter` args must be rejected explicitly (unknown args -> git expects `ERR`, proof would silently ignore and send a full pack), thin-pack is fine (server may always send full objects).

## Blockers
- Sideband frame size (65519 -> 65515) -- hard client-side die on any >64 KB blob.
- `recordCommit` assumes parents already inserted; pack order is not topological -> pushes with >1 new commit will intermittently throw.
- Walk uses per-pop `Array.sort`; must be a priority queue or it exceeds DO CPU on medium repos.

## Caveats
- `introduced` tree-diff at push time is hand-waved; it is the expensive part (parent trees from R2) and is what makes a 5M-object repo ~400 MB of SQLite in one DO.
- `objs` crosses DO RPC as one array (32 MiB limit) and `IN (...)` needs batching at 32k params; fresh clones must route to `precomputed-clone-pack`.
- Stream error handling must abort the writer; otherwise clients hang rather than fail.
- Ref update and `recordCommit` must be in the same DO transaction or fetch can see refs the graph does not know.

## Verdict
lands-with-caveats. The core claim -- decide the send set from a SQLite commit graph with zero R2 reads -- is real and the algorithm is git's own. But the proof code as written does not interoperate with git (sideband frame overflow) and the push side has an ordering bug; both are days of work, the surrounding tree-diff and scaling items are weeks.
