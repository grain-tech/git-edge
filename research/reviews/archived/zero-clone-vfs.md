# Review: Zero-clone execution: repo as a virtual filesystem inside an agent DO

> Idea #40 · wild · verdict: **risky** · feasibility 4/5 · reliability 2/5 · correctness 3/5 · effort: weeks
> Proof: [proofs/zero-clone-vfs.md](../../proofs/archived/zero-clone-vfs.md) · Review: [reviews/zero-clone-vfs.md](./zero-clone-vfs.md)

# Review: zero-clone-vfs (idea #40)

## Scores
- Feasibility: 4/5. Every primitive is GA: DO SQLite (`ctx.storage.sql`), alarms, DO RPC, R2 get/put/range, `DecompressionStream("deflate")` (zlib wrapper, correct for loose objects), `crypto.subtle` SHA-1. Limits hold for small files; `new Uint8Array([...hdr, ...payload])` in `write()` spreads a byte array through the JS call/iterator path and will stall or throw on multi-MB blobs — use `set()`. The real gap is that the proof only reads `objects/<sha>` loose keys, and every object that arrives via a real `git push` is inside a pack.
- Reliability: 2/5. Three concrete holes: GC-vs-overlay race (below), lost overlay writes during `commit()` interleaving, and no rebase on CAS failure (admitted).
- Correctness: 3/5. Read/list/write are clone-free and produce fsck-valid objects. But the idea is "zero-clone *execution*" and the proof concedes bash/tests still need a container; `commit()`, delete, and mode preservation are unimplemented; symlinks (120000) and submodules (160000) are mishandled; annotated tags and SHA-256 repos break `rootTree` (`subarray(5,45)` assumes a commit header and 40-hex oids).
- Claim check: warm read is two R2 GETs, not one — `lookup()` calls `rootTree(root)` (a commit fetch) unconditionally on every lookup; the commit->tree oid is never memoised. Missing paths also refetch the parent tree on every miss because `INSERT OR IGNORE` cannot distinguish "loaded, absent" from "not loaded".

## Crash walk-through
Agent writes `src/a.ts`: `write()` puts blob B to R2, then inserts overlay row (SQLite commit is output-gated behind the awaited put, so the row never outlives a failed put — good). DO evicted before `commit()`. B sits in R2 unreferenced by any ref. `gc-and-repack-alarm` runs, walks refs, finds B unreachable and deletes it. Agent DO wakes (overlay persists in SQLite), calls `commit()`, builds trees pointing at B, CAS-advances the ref. Every later `git fetch` of that branch dies with `fatal: bad object <B>` / pack missing objects. This is a data-loss/corruption path unless GC honours a lease on overlay objects or `commit()` re-verifies (HEAD) each overlay oid before CAS. Second crash: after `casRef` succeeds but before the overlay is cleared, a retry sees binding.commit != tip, treats it as "ref moved", replays the overlay onto its own commit and publishes an empty duplicate commit.

## Concurrency walk-through
Two agent DOs bound to `main`, both edit and `commit()`. Repo DO CAS serialises: one wins, the other gets a CAS failure and has no recovery (overlay stuck until server-side-rebase exists). No split-brain, but the loser's work is stranded. Inside one agent DO: the DO is single-threaded but interleaves across non-storage awaits (R2 puts are not input-gated). `commit()` reads the overlay, awaits several R2 puts for trees/commit, then would `DELETE FROM overlay`. A `write()` that lands during those awaits inserts a row that is then deleted without ever being in a tree — silent lost write. Needs `blockConcurrencyWhile` or a generation column on overlay rows.

## Interop check
The DO never speaks the git wire protocol; interop is only through the objects it emits and the ref CAS. Exact wire detail that breaks: `git fetch` (v2 `fetch` command) expects upload-pack to send a PACK containing every object reachable from the new tip. If the fetch path builds packs from `pack_idx` (packed storage) and does not also enumerate loose `objects/<sha>` keys, the client aborts with "did not receive expected object". Second exact detail: tree objects must be sorted by name with directory names compared as `name/`; a naive lexical sort of `foo` vs `foo-bar` vs `foo/` yields `error: tree not properly sorted` under `transfer.fsckObjects`/`receive.fsckObjects`, and `git fsck` flags the repo. `write()` hardcodes `100644`, so editing an executable file publishes a mode change the client will see as a diff. Commit objects need `author`/`committer` lines with epoch+tz; proof omits them.

## Blockers
- Packed-object read path (pack_idx table, range GET, ofs/ref-delta resolution) — without it nothing pushed by a real client is readable.
- GC/overlay interlock so unpublished overlay blobs are not collected.
- Concurrency guard in `commit()` (blockConcurrencyWhile or generation stamps).

## Caveats
- Fix `lookup()` to memoise commit->root tree; cache `rootTree` result in `binding`.
- Peel annotated tags in `bind()`; reject or support SHA-256 repos explicitly.
- Add tombstones for delete, preserve mode from the memo on edit, implement git tree sort order and author/committer headers in `commit()`.
- Alarm drops `vfs_tree` but never re-resolves the ref, contrary to the comment.
- "Execution" is not delivered; this is lazy read + commit, execution still needs a sandbox.

## Verdict
risky. The lazy-VFS mechanism is sound and standard, but the proof as written only works on a repo that has never received a packed push, and has a GC race that can publish a commit pointing at a deleted blob. Effort to a working version: weeks (pack/delta reader, commit() with correct tree encoding, rebase-on-CAS-fail, GC lease).
