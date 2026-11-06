# Second-pass review: Shallow and partial clone as first-class filters

# Review v2: partial-clone-filters (idea #10)

## Scores
- Feasibility 4/5 (v1: 3). Every primitive is in the memo: sync `SqlStorage::exec`, R2 `Range::OffsetWithLength`, `Response::from_stream`, gix-object parsers, `gix_hash::hasher`. Unverified names (`TagRef::target_kind`, `CommitRefIter::tree_id`, `EntryMode::is_tree`, `Hasher::try_finalize`) all exist in the pinned versions to my knowledge; the proof lists them for day 1. The first-pass 1,000-GET wall is gone by construction (pack entries, not keys).
- Reliability 4/5 (v1: 4). Still read-only: no SQL writes, no R2 writes, no alarm. Budget is projected before the first byte (`plan_reads`), so the mid-band abort is gone. GRACE = 1 h > `max_ms` = 240 s keeps bytes under a streaming read.
- Correctness 3/5 (v1: 3). Filter bypass for explicit wants, tag peeling, `deepen`/`shallow`/`unshallow` all match `upload-pack.c`; `--unshallow` = `deepen 2147483647` is right (builtin/fetch.c hardcodes it). But the commit walk reads one R2 span per BFS level (section 9.3's commit-region read is deferred to a sibling), so a `blob:none` clone of a repo with a 10k-commit chain exceeds the 9,000-subrequest projection and gets 413. That is the headline path.
- Effort: weeks (2-3 inside the foundation: commit-region read, `unfold` stream driver, `entries_of`/`pack_meta`, prelude write-backs, scenarios 3/10/11/12/17/18).

## Contract compliance
Followed: module `src/pack/generate.rs`; `send_set`/`write_pack` signatures of 1.4; key layout via `keys::pack` (2.2); reader query semantics (live packs only, 2.3); `objects.idx`/`packs.count,bytes` schema; `BlobLimit` as `size > n` (9.5); bitmap-per-pack `SendSet` (9.5); verbatim entry copy, popcount header, `gix_hash` trailer (9.6); `budget.charge(1)` before `read_range` (7.1); 256 KiB gap merge and 8 MiB split (7.2); error mapping (413 `Budget`/`Limit`, band-3 after first byte). `changes()`, CAS span, alarm/jobs, janitor: not applicable, nothing is written and no alarm is set. No `unwrap`/`[]`/`as` on request or storage values.
Deviations (each declared by the proof as a write-back except 1, 5, 6):
1. **Section 9.3 commit-region read omitted.** Contract: "for each pack read `[commit_lo, commit_hi)` once per request (cached)". Proof `load` does `lookup` + `read_entries` per round and says the cache "slots in here" from want-have-negotiation. Behavioural, see Blockers.
2. `Bucket::read_entries(&[(ObjectId, ObjLoc)], &mut ReqBudget)` vs contract `read_entries(&self, locs: &[ObjLoc])`. (The contract's own return type `(ObjectId, Vec<u8>)` cannot be produced from `ObjLoc`, so the write-back is right, but the code does not compile against 1.2 as written.)
3. `Index::pack_meta`, `Index::entries_of`, `MemFind::default()` are not in 1.2.
4. `send_set_shallow(.., client_shallow, ..)` is a new entry point; 1.4's `send_set` has no shallow input, so `FetchArgs::shallow` cannot reach the module through the contract signature.
5. `d.state.storage()` and `bucket.repo` read private fields of `RepoDo` (1.3) and `Bucket` (1.2) from another module; and `pack` importing `crate::RepoDo` breaks "nothing imports `repo_do` except `lib.rs` and `edge`" (the contract's own 1.4 signature forces this; take `&Index` instead).
6. Section 10: gitoxide parse errors on stored objects map to `Error::Storage`; contract says `Protocol` or `Unpack`. Storage is the better code, but it is a deviation.

## First-pass blockers
1. Subrequest cap vs batched promisor fetch: **resolved.** N wants are rows, not keys; `plan_reads` falls back to `coalesce(pi, &locs, WINDOW)` when gap-merging yields more than `p.bytes.div_ceil(WINDOW)` reads, and `if budget.used.saturating_add(n) > budget.max_subrequests { return Err(Error::Budget) }` refuses before any byte. Residual, acknowledged: the 1 MiB command cap (6.3) refuses a checkout batch above ~20,900 `want` lines with 400, every time; needs the proposed 6.3 write-back. Also `load(.., &edge, ..)` runs even when `edge` is empty, so "zero awaits" holds only if `read_entries(&[])` issues no subrequest.
2. Annotated tags: **resolved.** `match loc.kind { .. Kind::Tag => tags.push(*id) ..}` marks the tag at step 1 and `while !tags.is_empty()` peels via `TagRef::target_kind`, nested tags loop again.
3. Storage format: **resolved** by CONTRACTS 2.1-2.3; `pack_chunk` does `h.update(e); out.data(e);` on `buf[start..start+len]`, no header skipping or deflate.

## Crash walk-through
`git checkout` after a blobless clone sends 2,500 `want <blob>` + `filter blob:none` + `done`. `send_set` marks 2,500 rows (one sync span), `plan_reads` yields say 3 reads of 8 MiB. Header, chunk 1, chunk 2 streamed; the DO is evicted during `read_range` of chunk 3. Client: "unexpected disconnect while reading sideband packet", index-pack fails, promisor fetch reports "could not fetch". Server: no row, key or alarm was touched at any point (the module has no `INSERT`/`UPDATE`, no `put`, no `enqueue`). Retry recomputes from the current index. If eviction happens before the first byte (inside `send_set`), the edge sees a failed stub call and returns 500 with one `ERR` pkt. No orphan, no half-state.

## Concurrency walk-through
Fetch F (full `blob:none` clone) is mid-walk; each `load` awaits R2, so the gate opens. Push P runs `/_do/push/index` and `/_do/push/commit` between two of F's rounds: `lookup` sees a new live pack only from the next round; every loc it returns is in a pack whose bytes are durable (commit is after `finish`), so `read_entries` cannot miss. `GcSweep` runs in one sync span between F's rounds: candidate packs are `created_at < started_at - GRACE` and unmarked at `gc.refs_version`; if F's want was force-pushed away 10+ min ago and is now unreachable, F's next `lookup` misses -> `Error::Internal("reachable .. is not live")` -> 500 or band-3 ERR; retry-safe. Bytes of a pack that went dead mid-stream stay in R2 for GRACE, so `pack_chunk` never short-reads. Two concurrent large fetches in one DO each hold up to 64 MiB `MemFind` + 8 MiB read + `seen`; the caps are per request, not per isolate, so two 60 MiB walks can exceed 128 MB. No split-brain path exists.

## Interop check
- Promisor fetch shape (git 2.24+ `check_updates` prefetch, `fetch.negotiationAlgorithm=noop`, `done` in the first request, no `have`): matches; wants bypass the filter as `NOT_USER_GIVEN` requires.
- `blob:limit`: the client canonicalises `1m` to `blob:limit=1048576` (`expand_list_objects_filter_spec`), so the k/m/g 400 never fires against stock git; the `size > n` vs `>= n` superset is harmless.
- `deepen 1` / `deepen 3` on a shallow clone / `--unshallow`: shallow, unshallow and CLIENT_SHALLOW-not-resent rules match `send_shallow`/`send_unshallow`; `deepen 2147483647` is the literal string `builtin/fetch.c` sends.
- Pack bytes: `PACK`, `00000002`, exact count, full entries, SHA-1 trailer in band 1: index-pack accepts.
- The wire byte that would break lives in the sibling this proof leans on (`write_fetch_prelude`), not here: CONTRACTS 1.1 rule 3 reads "delim, optional shallow-info section, delim, packfile" for the `done` case; emitting `0001` before `000dpackfile\n` when there is no shallow-info makes `fetch-pack` die on `expected 'packfile'`. With `done` and no shallow-info the response must begin directly with `000dpackfile\n`.
- `not our ref` as HTTP 400 (section 10) instead of git's 200 + `ERR` pkt: the client reports "RPC failed; HTTP 400" rather than the ref name; error path only.

## Blockers
1. Commit walk without the 9.3 commit-region read: one coalesced R2 read per BFS level, so a `blob:none` (or plain) clone of a repo whose longest ancestor chain exceeds ~9,000 commits fails the `plan_reads` projection with 413, and one of ~5,000 commits spends ~100-150 s of the 240 s wall on sequential reads. `--depth` is unaffected. Fix is the contract's own line: read `[commit_lo, commit_hi)` per pack once and fall back to `read_entries` for stragglers; days, but it must be in this module.
2. Compile-level contract mismatches (deviations 2-5 above) are declared but not adopted; until the write-backs land, the code does not build against CONTRACTS section 1.

## Caveats
- `tree:0` is not delivered (section 12); the idea's title over-promises.
- Checkout batches above ~20,900 blobs are refused by the 1 MiB command cap (6.3); the 16 MiB write-back is needed for large monorepos.
- `write_pack` as signed buffers the whole pack in the `PktWriter`; the `unfold` driver that meets 6.5 is described, not shown.
- `entries_of` materialises every row of a pack (56 B each) in one sync span; 1M-entry packs cost seconds of DO CPU and ~56 MB.
- Per-request memory caps are not per-isolate caps; concurrent large fetches can OOM the DO.
- Unverified API names: `gix_hash::hasher`/`try_finalize`, `TagRef::target_kind`, `CommitRefIter::tree_id`, `EntryMode::is_tree`, `SqlStorage::exec` bindings; real R2 range reads and the deployed subrequest limit (platform-facts #6, #7).
- Deepen on an already-shallow clone resends the client's existing depth (`deepen-relative` out of scope); superset, bounded by depth.

## Verdict
risky, but materially better than v1: feasibility 3->4, the three first-pass blockers are genuinely closed by code, and the promisor path now costs one lookup per want and reads bounded by pack windows. What keeps it from "lands-with-caveats" is one omitted contract step (the 9.3 commit-region read) that turns the headline `blob:none` clone into a 413 on ordinary repos, plus the declared-but-unadopted signature write-backs. Both are days of work with a fully specified design.
