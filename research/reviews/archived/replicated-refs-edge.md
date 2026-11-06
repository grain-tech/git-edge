# Review: Refs replicated to every region via KV and DO location hints

> Idea #13 · edge · verdict: **lands with caveats** · feasibility 4/5 · reliability 3/5 · correctness 3/5 · effort: days
> Proof: [proofs/replicated-refs-edge.md](../../proofs/archived/replicated-refs-edge.md) · Review: [reviews/replicated-refs-edge.md](./replicated-refs-edge.md)

# Review: replicated-refs-edge (idea #13)

## Scores
- Feasibility 4/5. Every primitive is GA: SQLite DO storage + `transactionSync`, `setAlarm`/`getAlarm`, `locationHint`, KV `get(..., {cacheTtl})` (60s minimum is real), KV ~1 write/s/key and 25 MiB value cap are quoted correctly. The proof is honest that DO read replicas are not used. Limits are trivially respected (ref list only). One misframing: KV is not "replicated to every colo"; it is a central store with pull-through edge caching, so the first `ls-refs` in a colo (or after the 60s TTL) still pays a round trip to KV's central tier, and only repeat reads within 60s are truly local. `void getAlarm().then(setAlarm)` is a dangling promise racing the response; usually fine inside a DO, but not guaranteed.
- Reliability 3/5. No data loss on the authority (single DO, atomic CAS, KV snapshot is one atomic value so readers never see a torn ref list). But the publish pipeline has two real holes (below) that leave KV stale indefinitely, not just for 60s.
- Correctness 3/5. It delivers "eventually-consistent ref advertisement from an edge cache", which is a weaker lookalike of "reads from nearest edge": the v2 `GET info/refs` that precedes every `ls-refs` is still routed to the DO in the proof code (`return authority().fetch(req)`), so the cross-ocean hop the idea exists to remove is still paid once per `git fetch`. Read-your-writes only works for clients that echo a custom header, which stock git does not.
- Effort: days on top of #1/#2/#3 (the publish/alarm fixes, static `info/refs`, HEAD symref, and a KV/DO version test are each hours).

## Crash walk-through
Push commits `main: X->Y` in `transactionSync` (version 7, dirty=1, durable). The DO is evicted or the isolate dies before the un-awaited `setAlarm` lands. Nothing re-arms the alarm on restart: the constructor creates tables but never checks `dirty`. KV stays at version 6 for every colo until the *next* push to this repo, which for a quiet repo may be days. Every other client (`git ls-remote`, CI in another region) is lied to for that whole window; the pusher only escapes because of the version header. Fix: in the constructor (`blockConcurrencyWhile`) read `dirty` and `setAlarm` if set; better, make setAlarm part of the same request before responding (await it).

Second hole, no crash needed: `alarm()` awaits `REFS_KV.put`; input gates do not block on external I/O, so a push can run `updateRefs` during that await, setting dirty=1 and re-arming. `alarm()` then unconditionally writes `dirty=0`, the re-armed alarm fires, sees `dirty=0`, returns. The second push is never published (lost publish, same indefinite-stale outcome). Fix: capture `version` before the put and clear dirty only `WHERE version = captured`, or drop the dirty flag and store `publishedVersion`, publishing whenever `version > publishedVersion`.

## Concurrency walk-through
Pushers A (`X->Y main`) and B (`X->Z main`) hit the single DO; `transactionSync` is synchronous, so B's CAS sees Y and fails. No split-brain: KV is never written by anyone but this DO's alarm, and each KV value is a full snapshot with a monotonic version, so a reader in Tokyo and one in Frankfurt may see versions 6 and 7 but never a mix. However, the CAS failure path is broken as written: `throw new Error("cas")` inside `transactionSync` propagates out of `updateRefs` (no try/catch), so the caller never receives `{ok:false, status:["ng ..."]}` and the `status` array holds only the commands processed before the failure; a real client gets a 500, not `ng main fetch first`. Also all-or-nothing rollback is only correct when the client sent `atomic`; git's default is per-ref. Force-push + stale KV: a reader may be advertised a tip already discarded; the fetch still succeeds if #6/#GC honours a grace period >= KV lag + cacheTtl, which the proof notes but no sibling enforces.

## Interop check
`git fetch` with protocol v2 sends `GET info/refs?service=git-upload-pack` with `Git-Protocol: version=2`, then `POST git-upload-pack` with `command=ls-refs`, caps, `0001`, and args `peel`, `symrefs`, `unborn`, `ref-prefix ...`. Things that break with the proof as written:
1. `symrefs` is ignored and the snapshot has no `HEAD`. `git clone` asks `ref-prefix HEAD` and expects `<oid> HEAD symref-target:refs/heads/main`; without it clone completes but prints "remote HEAD refers to nonexistent ref, unable to checkout" and leaves an empty worktree. The `Ref` type needs a `symref` field and the publisher must emit `HEAD`.
2. `ref-prefix` is ignored: legal (server MAY return a superset, client filters), but for a 200k-ref repo every fetch ships the whole list; `unborn` on an empty repo must produce `unborn HEAD symref-target:refs/heads/main` or clone of an empty repo misbehaves.
3. The v2 `info/refs` reply is static (`version 2`, `agent`, `ls-refs=unborn`, `fetch=shallow wait-for-done`, `server-option`, `object-format=sha1`, flush) and must be served by the Worker for the latency claim to hold; the proof routes it to the DO.
4. `x-git-edge-version` is never sent by stock git; needs `http.extraHeader` per remote. The stated goal for laptops is therefore plain eventual consistency (up to 60s KV + 60s cacheTtl).
5. `pkt()` computes length from `s.length` (UTF-16 code units); a refname with non-ASCII bytes yields a wrong pkt-line length and `fatal: protocol error: bad line length`. Use byte length.

## Blockers
- Lost-publish race in `alarm()` (dirty cleared after concurrent push) and no alarm re-arm after crash: KV can stay stale indefinitely, which turns "up to 2 minutes old" into "until someone pushes again".
- CAS rejection throws out of `updateRefs` instead of returning `ng`; real `git push` sees a 500 on any non-fast-forward.
- `HEAD`/`symref-target` absent from the snapshot: `git clone` cannot check out.

## Caveats
- KV is a pull-through cache, not a replica: cold or expired colos still make a central round trip; the win is repeat fetches within 60s per colo.
- `info/refs` must be answered at the edge too or the DO hop remains on every fetch.
- Read-your-writes requires client configuration; unconfigured users get eventual consistency and may observe a rollback after their own push.
- GC grace period must exceed KV lag; owned by a sibling idea, not enforced here.
- `locationHint` is creation-time only and best-effort; the title over-promises.

## Verdict
lands-with-caveats. The mechanism (single-writer DO, versioned KV snapshot, pkt-line render at the edge) is sound and every API is GA, but the proof code has one indefinite-staleness bug, one broken rejection path, and one clone-breaking omission (HEAD symref). All are days of work; the remaining limits (eventual consistency for stock git, cache-not-replica) are inherent and should be stated in the idea's description.
