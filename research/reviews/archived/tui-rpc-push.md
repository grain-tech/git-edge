# Review: Push from a sibling workspace DO over RPC, no HTTP

> Idea #29 · edge · verdict: **lands with caveats** · feasibility 4/5 · reliability 3/5 · correctness 3/5 · effort: weeks
> Proof: [proofs/tui-rpc-push.md](../../proofs/archived/tui-rpc-push.md) · Review: [reviews/tui-rpc-push.md](./tui-rpc-push.md)

# Review: tui-rpc-push (idea #29)

## Scores
- Feasibility: 4/5. Every primitive is GA: DO JS RPC (`extends DurableObject`, typed stubs, cross-script `script_name` bindings), DO SQLite, R2 put/head, `crypto.subtle` SHA-1, `CompressionStream("deflate")` (zlib framing, correct for loose objects), streams as RPC args. What it under-states: the 1000-subrequest-per-invocation cap. R2 binding calls are subrequests, so `pushWorkspace` (N puts) and `commitPush` (N heads via `Promise.all`) each die at ~1000 files in one invocation; "chunk across alarms" is mentioned but not designed. RPC payload cap (~32 MiB) is respected because only oid strings cross the wire.
- Reliability: 3/5. Ref CAS is correct and serialized by the DO; the two SQLite writes in `commitPush` have no `await` between them so they commit in one implicit transaction, and output gating means `{ok:true}` is only delivered after durable commit. Holes: GC race (below) and unbounded orphan growth on partial pushes.
- Correctness: 3/5. Achieves the stated goal (no HTTP, no pkt-line, real objects, real ref CAS). But it is a *snapshot commit builder*, not a git push: it cannot carry arbitrary history, and the proof code produces fsck-invalid trees for any real workspace (see Interop).
- Effort: days for the flat-tree proof; ~2 weeks for nested trees, binary/exec-bit files, chunked alarm-driven writes, and the object-cache.

## Crash walk-through
Workspace DO writes 40 blobs, crashes (eviction/CPU limit) before the tree/commit. R2 now holds 40 content-addressed loose objects nobody references; the ref is untouched; the TUI gets an error and retries; the retry re-PUTs the same keys (idempotent) and completes. No data loss, no split-brain, only orphans. Second crash point: repo DO dies inside `commitPush` after the 40 `head`s resolve but before the SQL runs -- nothing written, RPC rejects, client retries with the same `parent`, CAS still matches. Correct. Third: crash after the SQL but before the RPC reply flushes -- output gating guarantees the write is durable iff the reply was sent, so a retry gets `stale` (cur is now newOid) and the TUI must treat `stale` where `cur === newOid` as success; the proof does not do this (idempotent-retry gap, minor).

## Concurrency walk-through
Two workspace DOs (A, B) both hold `parent = P` for `refs/heads/main`. A calls `commitPush` first: heads pass, CAS `cur === P` passes, ref -> A'. B's call queues behind A on the single-threaded repo DO... except `commitPush` awaits R2 heads, so B's heads interleave with A's; that is fine because the CAS + SQL section has no await and runs atomically. B gets `stale`. Correct, git-equivalent semantics.
Real race: the GC alarm. Objects are unreferenced from the moment the workspace DO PUTs them until the ref flips. A GC pass in the repo DO (or a separate sweeper) running between `head` and the ref update deletes the just-verified objects and the ref then points at nothing -- exactly the invariant the proof claims to preserve. Fix is standard (grace window by R2 `uploaded` timestamp, or GC only inside `blockConcurrencyWhile`) but is absent.

## Interop check
No git wire protocol is spoken here, so nothing on the push side can break a `git` client; interop is entirely via the clone/fetch path (other ideas) reading `objects/<sha>`. The stored bytes are correct loose objects. What breaks a cloned result under `git fsck`:
1. `f.path` is used as a tree entry name. Any path containing `/` yields fsck error `fullPathname` ("contains full pathnames"); the flat tree is not merely a simplification, it is invalid for every real workspace.
2. Sort uses JS UTF-16 string comparison; git requires byte order (differs for non-BMP names) and dir-as-`name/` ordering once nested.
3. `content: string` -> TextEncoder: binary files corrupt; no `100755`/`120000` modes, so exec bits and symlinks are lost.
4. `parent` is supplied by the TUI; no check that `oldOid` is actually an ancestor -- a stale-but-lucky client can force-push silently (git would say `non-fast-forward`).

## Blockers
- Tree builder is fsck-invalid for nested paths (`fullPathname`) -- must be implemented before this "lands", not after.
- 1000-subrequest cap per invocation with no chunking design: pushes of >~998 files fail on both DOs.

## Caveats
- GC/orphan race: objects are unreferenced until the ref flips; GC needs a grace window.
- Retry after committed-but-unacked push returns `stale`; needs `cur === newOid` idempotency.
- No fast-forward check; `stale` != `non-fast-forward`.
- Binary files, exec bits, symlinks unsupported by the `files` schema as used.
- RPC-in-account is "implicitly trusted": `commitPush` never validates that `oids` actually cover the tree closure, so a buggy caller can publish a ref with a dangling tree.
- Cost: N R2 class-A PUTs per push with no delta or skip-cache.

## Verdict
lands-with-caveats. The core claim -- typed DO RPC replacing receive-pack, objects direct to R2, ref CAS in the repo DO -- is sound on GA primitives and the CAS/crash story holds. The proof code as written cannot produce a valid repo for any workspace with a subdirectory and has no answer for the subrequest cap; both are known-solvable, days-to-weeks work.
