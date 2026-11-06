# Review: Server-side three-way merge in the Worker

> Idea #17 · edge · verdict: **risky** · feasibility 4/5 · reliability 3/5 · correctness 2/5 · effort: weeks
> Proof: [proofs/server-side-merge.md](../../proofs/archived/server-side-merge.md) · Review: [reviews/server-side-merge.md](./server-side-merge.md)

# Review: server-side-merge (idea #17)

## Scores
- Feasibility 4/5. Everything named is GA: streaming request body, `DecompressionStream`/`CompressionStream("deflate")`, `crypto.subtle` SHA-1, DO SQLite + `transactionSync`, DO RPC, R2 get/put, alarms (indirect). `node-diff3` is plain JS. Limits: every both-sides-edited blob is fully inflated into a JS string, split on `\n`, and re-encoded; a pair of 40 MB generated files exceeds the 128 MB isolate, and diff3 on 1M-line inputs can exceed the 30 s CPU cap. The proof admits both. Tree recursion is 3 sequential GETs per level (siblings parallel), acceptable.
- Reliability 3/5. Ref movement is a true CAS inside the single-threaded DO and both refs move in one transaction, so no split-brain and no data loss. But the merge objects live outside the two-phase-push bookkeeping: they are never in any manifest, so the janitor the proof leans on cannot see them; every lost CAS race, every crash after `writeObject`, and every `merge conflict` rejection that already wrote partial blobs/trees (line 67 writes merged blobs before the conflict tally is final) leaks R2 keys forever.
- Correctness 2/5. Against the sibling `commit()` as written the non-fast-forward path cannot succeed: `commit()` checks `known(c.new)` against the manifest plus the `objects` index, and `mergeSha` is in neither, so main's command returns `missing necessary objects` every time. Even with that check bypassed, the merge commit/trees/blobs are never inserted into `objects`, `commits`, `parents`, `introduced`, so `want-have-negotiation` cannot serve main's new tip and the next `mergeBase(main, ...)` finds no row and reports "unrelated histories". Only the fast-forward and already-merged branches work today. The merge semantics themselves are a reasonable merge-ort subset (no renames, single base, safe-direction failures).
- Effort: weeks. Days for the merge code; the rest is re-plumbing object registration through the DO and fixing the sibling's report-status/sideband path.

## Crash walk-through
Client pushes `feature` with `-o merge=main`. Pack streams to R2, manifest written, `mergeBase` returns B, `mergeTrees` writes 3 merged blobs and 2 trees, `writeObject("commit")` writes M. Worker is evicted before `stub.commit(pushId, ...)`. Refs untouched: correct. Fifteen minutes later the janitor sweeps the pending row and the manifest's objects. The 6 merge objects are in no manifest and not in `objects`, so they are never listed and never deleted: permanent orphans (the proof states the opposite). Client retries: new pushId, feature pack re-PUT idempotently, merge recomputed with a new `Date.now()` so a second commit object M' is written; M stays leaked. Nothing reachable is wrong, refs are consistent, but the "janitor sweeps merge objects" claim is false and only `gc-and-repack-alarm` with a bucket listing can reclaim them.
Second crash: after `commit()` returns, before report-status reaches the client. `feature` and `main` both advanced atomically; client prints an error and the next fetch reconciles. Same as upstream git over HTTP; acceptable.

## Concurrency walk-through
Pushes A (`feat-a`) and B (`feat-b`) both carry `merge=main`, both observe `main = T0`, both compute merges MA, MB against T0 and write them. A's `commit()` runs first in the DO: CAS `main T0 -> MA` succeeds, `feat-a` moves. B's `commit()`: `feat-b` CAS passes, `main` CAS sees MA != T0 and returns `fetch first`; with the transaction all-or-nothing nothing moves. B's loop matches `reason === "fetch first"` on the main line, re-reads tip MA, re-runs `mergeBase(MA, b)` (needs MA in `commits`, which A's commit never inserted: BFS on MA returns `[]` parents, so base is null and B is rejected as unrelated histories, a false conflict caused by the indexing gap), else merges against MA and commits. Bounded at 3 retries with a clear error; no lost update, no interleaving visible to any fetch. MB leaks (see above). Note the CAS on main is evaluated by the DO, not by the Worker's `ours` read, so the TOCTOU between `getRef` and `commit` is closed. One real gap: `commit()` reports `ok` for `feat-b` inside the same results array when main fails; the retry loop hides that, but if the loop exits at 3 attempts `report()` must not have leaked those earlier `ok` lines.

## Interop check
- Wire framing is right: receive-pack is v0 regardless of `Git-Protocol: version=2`; git 2.4x sends `push-options` in the first command's capability list only if the advert carried it, then options one per pkt-line after the command flush, then a second flush, then PACK. `info-refs-endpoint` advertises `push-options`; the Worker must also check the client echoed the capability, otherwise the byte after the command flush is `PACK`, not an option section, and the parser would eat the pack header as options.
- Exact break 1 (inherited): the advert also carries `side-band-64k`, so report-status must be wrapped in band 1; unwrapped `unpack ok` makes the client abort with `protocol error: bad band #117`. The proof's `report()` is unspecified.
- Exact break 2: `report()` forwards the `refs/heads/main` result line, a ref the client never named. `send-pack` prints `warning: remote reported status on unknown ref: refs/heads/main` (warning, not the fatal the proof claims) but `main`'s ok/ng must not be relied on to reach the user; the proof's own design (ride on feature's line + band 2 message) is correct, the code does not implement it.
- Exact break 3: tree entries are sorted by JS `<` (UTF-16 code units), not by bytes; a directory with names above U+FFFF or mixed high bytes produces a tree git orders differently, which `fetch.fsckObjects`/`transfer.fsckObjects` rejects as `treeNotSorted`. Sort on `TextEncoder` output.
- Edge: a delete push (`new = ZERO`) or an empty pack (branch already on server) reaches the merge path unguarded; `treeOf(ZERO)` throws.
- `mergeBase`'s SQL (`commits(sha, parents)` string column) does not match the sibling's `parents(oid, parent)` table.

## Blockers
1. Merge objects are invisible to `commit()`: `known(mergeSha)` fails, so any real (non-ff) merge is rejected with `missing necessary objects`. Needs a `stub.register(pushId, objects+links)` (or a second manifest) before `commit()`, which also populates `objects/commits/parents/introduced`.
2. Without (1), even a patched CAS leaves main's tip unfetchable and un-merge-able next time; without janitor coverage, abandoned merge objects leak permanently.
3. Sideband-wrapped report-status and push-options capability echo check, so a stock `git push -o merge=main` completes at all.

## Caveats
- Single BFS merge base without generation ordering can pick an older ancestor in merge-heavy history: false conflicts, never silent wrong merges given the sha-equality shortcuts.
- Line 67 writes merged blobs before conflicts elsewhere are known; conflict rejections still leave R2 writes behind.
- `x-git-user` must be set by the auth layer and stripped from client requests, or authorship is spoofable; merge commit is unsigned.
- Memory/CPU bound by largest both-sides-edited blob; large-file merges should be refused with an explicit "too large to merge server-side" rather than a fake conflict.
- Proof's statement that `send-pack` rejects unknown-ref status lines is wrong (it warns); the design conclusion still stands.

## Verdict
risky. The push-option plumbing and the tree-level merge are sound and buildable on GA primitives, but the proof code as written cannot land a single non-fast-forward merge against its own dependencies, and its cleanup story relies on a janitor that cannot see the objects it creates.
