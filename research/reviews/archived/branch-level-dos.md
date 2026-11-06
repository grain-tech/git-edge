# Review: Branch-level Durable Objects for monorepos

> Idea #16 · edge · verdict: **risky** · feasibility 4/5 · reliability 3/5 · correctness 2/5 · effort: weeks
> Proof: [proofs/branch-level-dos.md](../../proofs/archived/branch-level-dos.md) · Review: [reviews/branch-level-dos.md](./branch-level-dos.md)

# Review: branch-level-dos (idea #16)

## Scores
- Feasibility 4/5. Every primitive is GA: DO SQLite + `transactionSync`, RPC via `extends DurableObject`, `idFromName`, alarms, R2 `head`/`get(range)`, `DecompressionStream("deflate")`. Nothing large enters a DO (only SHAs/refnames), so 128 MB and single-threading are respected. Pack indexing is delegated to #4/#5/#6 and inherits their chunked-body / R2 known-length problem. One platform nit: `env.SHARD.get(idFromName(x)).listRefs()` for a prefix that maps to no real namespace still instantiates a DO and runs `CREATE TABLE` in the constructor, so every stray `ref-prefix` (git sends `main`, `refs/main`, `refs/remotes/main/HEAD`, ...) mints a billable empty DO with SQLite storage.
- Reliability 3/5. No split-brain on any single ref (each ref has exactly one authoritative DO, CAS inside `transactionSync`). Cross-shard partial application, a registry crash window, and `Promise.all` rejection semantics are the weak points (below).
- Correctness 2/5. Ref-prefix routing as written breaks the default `git clone` / `git fetch` (see Interop). The "quiet branch never touches the busy DO" claim holds only for readers with a fully-qualified prefix; every push still awaits the single `RepoRoot.register()` and a full-fan-out v0 advertisement, so writers gain nothing per-repo. The hot ref (`main`) is still one DO, which the proof admits.
- Effort: weeks. The shard/CAS core is days; correct prefix routing, HEAD/symref emission, receive-pack advertisement caching, and a real-git test matrix on top of #1/#4/#5/#6/#53 is 3-5 weeks.

## Crash walk-through
Push moves `refs/heads/team-a/x` (shard A) and `refs/heads/ci/build-9` (shard B). Objects land in `objects/<sha>` (idempotent, fine). `Promise.all` fires both `updateRefs`; A commits, then the Worker isolate dies before B returns or before `root.register`. Outcomes: (1) A advanced, B did not, client saw a transport error - identical to real git's per-ref non-atomic semantics; a retry gets "up to date" for A after re-advertisement. Acceptable. (2) If `team-a` was a brand-new namespace, shard A now holds a ref that is absent from the registry. A subsequent bare `git clone` or `git ls-remote` fans out over the registry and silently omits `refs/heads/team-a/*`; a targeted `fetch origin team-a/x` still works because it routes by prefix. That is a durable, silent listing divergence, not a transient one; it persists until the next push to that namespace. The proof names the fix (register before update) but the code does the wrong order. (3) Objects for a push that dies before any CAS are orphans in `objects/` and need #6's sweeper; no ref dangles because objects always precede refs.

## Concurrency walk-through
Two pushes to `refs/heads/team-a/x`: `X->Y` and `X->Z`. Both Workers do `BUCKET.head`, both call shard A's `updateRefs`; the `await head()` inside the DO opens the input gate, so both calls are in flight, but each CAS runs inside `transactionSync` (synchronous, single instance): first wins, second gets `ng ... fetch first`. Correct. A push to `main` at the same moment is on a different DO and is not queued behind either - the stated goal, for that case. Failure mode: shard B throws (DO reset, overloaded, `transactionSync` exception) while A succeeded; `Promise.all` rejects, the Worker returns 5xx with no report-status, and A's ref moved anyway. Use `allSettled` and emit `ng <ref> internal` per shard. Second failure mode: the pre-check-then-write loop returns `ng` for the whole shard batch on one stale old-sha; real git rejects only the stale ref and applies the rest (non-atomic). Third: `head()` then CAS is not atomic against a GC that deletes `objects/<sha>` between them (shared with #1/#6).

## Interop check
1. Breaks default clone/fetch. `git clone` (v2) sends `ref-prefix refs/heads/`, `ref-prefix refs/tags/`, plus `symrefs`, `peel`. `shardOf("refs/heads/")` splits to `["refs","heads",""]` and returns `"refs/heads/"` - a namespace that no push ever creates - so the Worker queries one empty DO and answers `0000`. Same for `git fetch`/`git pull` with the default `+refs/heads/*` refspec. Result: clone of a populated repo yields "warning: You appear to have cloned an empty repository." Routing must be: for each prefix, select registry namespaces where `ns.startsWith(prefix) || prefix.startsWith(ns)`; only fully-qualified prefixes (`refs/heads/main`, `refs/heads/team-a/`) collapse to one shard.
2. `git fetch origin main` sends six prefixes (`main`, `refs/main`, `refs/tags/main`, `refs/heads/main`, `refs/remotes/main`, `refs/remotes/main/HEAD`); the code maps each to a shard id and instantiates five empty DOs per fetch.
3. ls-refs response omits `HEAD` and the `symref-target:` attribute even though `symrefs` is requested; clone then cannot pick the default branch and falls back to guessing/`master` warnings. `RepoRoot.head()` exists but is never consulted. `peel` (`peeled:<sha>` for annotated tags) is also unimplemented.
4. The v2 branch does not check `command=ls-refs`; a `command=fetch` request on the same endpoint is parsed as ls-refs. Belongs to #53 but this endpoint code will run first.
5. receive-pack: the sideband framing is correct only if `side-band-64k` was negotiated and `report-status` requested; the advertisement (`GET info/refs?service=git-receive-pack`) is not shown and must fan out to all shards plus root for HEAD. Must not advertise `atomic` (then `git push --atomic` fails client-side with a clear message instead of half-applying).

## Blockers
- Prefix-to-shard routing: default clone/fetch prefixes (`refs/heads/`, `refs/tags/`) map to a nonexistent shard and return zero refs. Needs registry-driven prefix matching.
- Registry ordering: `register` after `updateRefs` leaves a permanent silent omission of a new namespace from clone/ls-remote on crash. Register first.
- `Promise.all` on shard RPC: one shard failure hides successful ref moves on others from the client.

## Caveats
- Every push still awaits the single root DO (`register`) and a full-shard advertisement; sharding relieves readers, not writers, per repo. Cache registration in the Worker or `waitUntil` it.
- Hot ref ceiling unchanged; `refs/heads/main` throughput is one DO, and a "monorepo" where everyone pushes `main` gets nothing from this idea.
- Whole-shard `ng` on one stale old-sha is stricter than git; cross-shard `--atomic` needs #cross-repo-atomic-push.
- Cross-shard `ls-refs` fan-out returns no consistent snapshot; a moving ref pair can list at mismatched points.
- Stray `ref-prefix` values create billable empty DOs; gate `listRefs` on registry membership.

## Verdict
risky. The sharding mechanism (one DO per namespace, CAS per shard, shared objects in R2) is sound and buildable on GA primitives, and it does deliver the read-side goal for fully-qualified prefixes. As written it breaks ordinary clone/fetch through the prefix mapping, drops HEAD/symrefs, and keeps a root-DO write on every push. Fixable in weeks, not a rethink, but the proof code cannot be tested against real git until routing is redone.
