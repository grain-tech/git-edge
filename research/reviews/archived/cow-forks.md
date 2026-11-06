# Review: Copy-on-write forks

> Idea #22 · edge · verdict: **risky** · feasibility 4/5 · reliability 2/5 · correctness 3/5 · effort: weeks
> Proof: [proofs/cow-forks.md](../../proofs/archived/cow-forks.md) · Review: [reviews/cow-forks.md](./cow-forks.md)

# Review: cow-forks (idea #22)

## Scores
- Feasibility: 4/5. Every primitive is GA: DO SQLite (`sql.exec`, `transactionSync`), alarms, stub `fetch`, R2 `get/put` with `sha1` integrity, `crypto.subtle`. 5000-row pages (~500 KB JSON) fit comfortably under 128 MB and the 30 s CPU budget; 2M rows x ~80 B = 160 MB is inside the 10 GB SQLite cap. Deductions: `initFork` does an awaited DO-to-DO call before its uniqueness check commits (input gates only block during storage ops, so a duplicate `POST /fork` races in), and the whole design assumes objects stay addressable as loose keys forever (see Blockers).
- Reliability: 2/5. The index import is not a consistent snapshot, pins are page-0-only, GC pin honoring lives in another DO that does not implement it, and `layer` is keyed by mutable repo name.
- Correctness: 3/5. On the wire it is a real fork (valid packs, real dedup via advertised refs), but the "never copy bytes" promise only holds while the parent never repacks, renames, or force-pushes-then-GCs, and the fork's own clones lose the precomputed-pack fast path. It is CoW at the index layer, not a durable CoW store.
- Effort: weeks (2-4) for a correct version once `gc-and-repack-alarm` grows a pins/pack-offset contract; days for the demo as written.

## Crash walk-through
Fork DO crashes after `stub.fetch("/snapshot?after=")` returns but before `transactionSync` commits. Parent has already committed `forks` row + `gc_pins` (its own transaction). Fork has no `meta`, so the user retries; `initFork` passes the `exists` check, parent re-pins with `INSERT OR IGNORE` (tips may have moved, so the pin set grows -- harmless leak). Alarm-chain crash mid-page: cursor is written in the same `transactionSync` as the rows, `INSERT OR IGNORE` makes replay idempotent, and DO alarms auto-retry. No loss. `receivePack` crash after some `BUCKET.put`s: objects land in `objects/bob/repo` with index rows but no ref; the fork's own GC sweeps them later. No loss. Verdict: crash paths are clean; the damage comes from time, not crashes (below).

## Concurrency walk-through
Parent pushes while the fork is paging its index. Pages are `sha > cursor ORDER BY sha`, so objects pushed at T2 (after page 0 pinned tips at T1) appear in later pages with `layer=objects/alice/repo` but are *not* pinned. Then alice force-pushes and her GC sweeps them. The fork now holds index rows for keys that 404. `negotiate()` decides the fork "has" only by ref reachability, so the fork will not advertise them as haves, but `readObject` for a sha in the index returns `null` mid-`streamPack`, i.e. a truncated pack with a bad trailer -- `git fetch` fails with `fatal: early EOF`/`index-pack failed`, and the fork cannot self-heal because it does not know which rows are dead. Second: two concurrent pushes to the fork interleave at every `await BUCKET.put`; that is fine (content-addressed, `INSERT OR REPLACE` idempotent) as long as `applyRefUpdates` does CAS, which is delegated. No split-brain on refs since one DO owns them; the split-brain is index-vs-R2.

## Interop check
- Push to fork: the fork advertises alice's tips, so `git push` (`send-pack --thin`) sends only new objects and a thin pack whose `REF_DELTA` bases live in the parent prefix; `readObject` resolves them, `parsePack` stores the resolved full object in the fork prefix. This is the correct behavior and matches what `index-pack --fix-thin` needs. Works.
- Fetch/clone from fork: undeltified pack assembled from two prefixes is byte-indistinguishable. Works, but every clone is N Class-B GETs (no precomputed pack), so a 100k-object fork clone is the same problem `content-addressed-r2-keys` already admits: ~10^5 sequential R2 round-trips through one DO stream.
- Exact wire detail that breaks: `ls-refs` with `symrefs` must emit `unborn`/`symref-target:refs/heads/main` for `HEAD`. The proof copies only `refs(name, sha)` rows; the parent stores HEAD nowhere in that table (per `refs-sqlite-objects-r2`), so the fork advertises no HEAD and `git clone` of a fork warns `remote HEAD refers to nonexistent ref, unable to checkout` and leaves an empty worktree. Trivial to fix, but it is not in the proof.

## Blockers
1. `gc-and-repack-alarm` as written uses roots = ref tips only, sweeps unmarked loose objects, and explicitly plans to delete loose objects once a pack "covers" them. The fork resolves *only* loose keys `${layer}/aa/rest`. After the parent's second GC, every inherited object the fork points at is gone from R2 (it lives only inside `packs/<parent-id>/<build>.pack`). This is not a caveat; it is a guaranteed data-loss path for every fork older than two parent GC cycles. Fix: either the parent never deletes loose objects while `forks` is non-empty, or `objects.layer` becomes `(packKey, offset, length)` and `readObject` does a range read.
2. `layer` is the mutable string `objects/<owner>/<repo>`. Rename or delete-and-recreate of `alice/repo` silently re-targets every fork row. Layer must be the parent DO id (which `gc-and-repack-alarm` already uses for pack keys, `packs/${ctx.id}`), and the two proofs disagree on key layout (`objects/<sha>` vs `objects/<owner>/<repo>/aa/rest`).
3. Pins cover ref tips at page 0 only; objects imported on later pages, or reached by a server-side "sync fork", are unpinned and sweepable (the concurrency walk-through). Pin on every page, or import only what is reachable from the pinned tips.

## Caveats
- Duplicate `POST /fork` races past the `exists` check (awaited stub call before the meta insert); second caller gets a UNIQUE-constraint 500, no corruption, but should be a 409.
- Parent pin obligation is a promise made by another DO; nothing in this DO can verify it. A parent that is deleted leaves the fork with a full index and no bytes.
- Fork clone of a large repo pays per-object GETs; fork should trigger its own `gc-and-repack-alarm` pack build immediately after import, otherwise "fork then clone" is the slowest path in the system.
- Cross-tenant read of another prefix is by design; a parent going private cannot revoke bytes already indexed, which needs to be stated as product policy.
- Import of a 2M-object parent is ~400 alarm ticks and ~2M SQLite row writes per fork; fork-of-fork repeats the whole cost since it flattens rather than shares.

## Verdict
**risky.** The wire-level mechanism (index-only fork, refs-as-haves dedup, thin-pack base resolution across prefixes) is correct and cheap, and the crash story is sound. But as specified it loses data on a fixed schedule: the sibling GC it depends on will delete the loose keys the fork points to after repack, and the layer key is a mutable name. It lands only after `gc-and-repack-alarm` gains `gc_pins` + pack-offset addressing (or a no-delete-while-forked rule) and the layer becomes an immutable DO id.
