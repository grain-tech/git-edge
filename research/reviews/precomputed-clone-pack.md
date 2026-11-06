# Second-pass review: Precomputed pack slices for clone

# Review v2: precomputed-clone-pack (idea #7)

## Scores
- Feasibility 4/5 (v1: 4). Nothing new is asked of the platform: no R2 writes, no multipart, no alarm of its own; two tables, a job kind, a sync gate, and a stream. Unverified and design-relevant: BLOB round-trip through `SqlCursor::to_array::<Vec<u8>>` (DO SQLite returns `ArrayBuffer`, which serde-wasm-bindgen does not deserialize as a `Vec<u8>` seq; use `raw()` + `Uint8Array::to_vec`), `Response::from_stream` with `crate::Error: Into<worker::Error>`.
- Reliability 3/5 (v1: 3, different reasons). v1's R2 lifecycle problems are gone. New: a stale job cursor after `Reschedule` publishes an incomplete plan forever (blocker 1); one wide frontier round exceeds the slice budget deterministically and the retry ladder kills the job (blocker 2).
- Correctness 4/5 (v1: 3). The wire path is now the contract's `wire` module; no hand framing, no snapshot trailer. The remaining correctness risk is state, not bytes.
- Effort: weeks. The v1 "chunked multipart builder" is gone; what is left is ~300 lines of glue plus the two fixes, on top of `write_pack` and `GcConsolidate` from the foundation.
Genuinely better than v1: v1 could not complete one clone (pkt length bug) and had an unspecified builder with all the platform risk; v2 serves a byte-correct pack and moved the risk into two ~20-line state bugs.

## Contract compliance
- Modules: `jobs/clone_plan.rs` + `repo_do/clone.rs`, `JobKind::ClonePlan` as a `run_slice` arm (4.5). No `set_alarm` (4.1). No new R2 key; only `keys::pack` (2.2). No `rows_written`; no CAS. `finish` and `clone_plan` are await-free spans. Janitor untouched. Error enum used. Compliant on every named rule.
- Deviations (compile-level, need write-back): `bucket.read_range(key, off, len, &mut budget)` has 4 args vs 1.2's 3; `read_entries(&[(ObjectId, ObjLoc)], budget)` vs 1.2's `&[ObjLoc]` (1.2 and section 9 already disagree; the proof follows section 9). `d.state`, `bucket.repo` are private fields read from other modules. `SliceBudget.req`, `spent_80pct`, `Sideband::new`, `meta_opt`, `bucket()`, `PackSlice`/`SendSet` shape, `schema_version = 2` are declared write-backs, not silent.
- Semantic misuse: `Error::Conflict("gc running")` (Conflict is "push state wrong", section 10) and `Error::Unpack` for a corrupt stored object (should be `Storage`). Minor.
- `serde_json` is not in the memo manifest; trivial add.

## First-pass blockers
1. pkt() length bug: **resolved**. `let mut w = PktWriter { out: s.prelude.take().unwrap_or_default() };` and `Sideband::new(&mut w).data(&win)` — framing is `wire` (1.1 rules 1-2, encoder measured against git 2.43 in the spike).
2. Blobless trailer: **resolved**. No stored trailer exists; `bitmap: if blobless { r.noblob } else { r.full }` hands the slice to `write_pack`, which hashes its own header (section 9 step 6). The verbatim path is taken only unfiltered, where the stored trailer is the response's trailer (2.1).
3. writePack unspecified: **resolved by reshaping**. "There is no pack build"; the job only calls `read_entries` under `SliceBudget`. The risk moves to `GcConsolidate` (foundation-owned) and to the GC nudge in `finish` (see caveats).
Review-body items (captured tips, tags via `TagRef::target`, no sideband-all/ref-in-want, bundle-uri claim dropped) are all resolved in code or by 1.1 rule 6.

## Crash walk-through
DO evicted after `read_entries` returns but before the next `bits.save`: in-memory bits and `cur` are lost; the row still holds the last `Continue` cursor and `clone_build` the matching bitmaps (both written in one sync span, `bits.save` then dispatcher `cursor = ?` with no await between). Restart is consistent. Crash inside `finish`: `DELETE clone_plan` + `INSERT ... SELECT` + meta rows are one span, rolled back together; old plan survives. Crash mid-`stream_verbatim`: truncated pack, git reports EOF, retry is stateless. Correct.

## Concurrency walk-through
Build takes 2 slices; a push lands between them. Slice 1: `Continue{cursor F1}` -> row.cursor = F1. Slice 2: resumes F1, frontier drains, `finish` publishes plan(closure of tips), sees `refs_version` moved, returns `Reschedule`. Dispatcher (4.2, "as given") leaves row.cursor = F1. Slice 3: `Some(Ok(c)) if !c.frontier.is_empty() => c` resumes F1 with `clone_build` already deleted by `finish` -> `Bitmaps::load` is empty -> publishes plan = closure(F1) only, with `clone.tips` = the old tips, then `cur.refs_version` (old) still != meta -> `Reschedule` again, forever. Every fresh clone of an unchanged branch passes the gate (wants ⊆ old tips, packs live) and gets a pack missing the tips and root trees: `git clone` fails at connectivity check ("remote did not send all necessary objects"). No miss is ever raised, `enqueue` dedups against the looping row, so it never heals. Second scenario: GcSweep interleaves mid-build; the plan ends with dead-pack rows, the gate misses, one build is wasted. Safe, but `finish` stores `clone.gc_epoch` and never compares it.

## Interop check
git 2.43-2.47 fresh clone sends `want`s + `done`, no `have` (fetch-pack writes `done` when `add_haves` adds nothing), plus `thin-pack ofs-delta include-tag`; the gate accepts it. Response `000dpackfile\n`, band-1 frames <= 65515 data bytes, `0000`: valid with the acknowledgments section omitted (rule 3). Verbatim pack: header count = `packs.count`, trailer = own SHA-1 (`PackWriter::finish`). No breaking wire byte found on the fast path. Note: the verbatim branch does not need "every entry marked"; any single unfiltered pack is a valid superset, so the predicate can be relaxed to one pack + no filter.

## Blockers
1. Stale cursor after `Reschedule` (walk-through above). Fix: on the resume branch also require `clone_build` non-empty, or have `finish` return `Continue` with an empty-frontier "done" cursor before rescheduling.
2. Frontier not chunked against `SliceBudget`: `read_entries(&load)` for one round charges every coalesced span at once; round 1 is every ref tip, so a repo with more than ~320 tips in distinct 256 KiB regions gets `Error::Budget` on the same cursor 8 times (~4 h) and a `dead` row; the next miss re-enqueues the same failure. Fix: take at most N ids per round, check `spent_80pct` per chunk, `Continue` with the remainder.

## Caveats
- DO SQLite row/value limit is 2 MB: `clone.tips` JSON (44 B per ref) and the cursor, which holds tips twice on round 1, cap the module at roughly 22,000 distinct ref targets; the proof's own "4 MB in one TEXT value" would not write. Bitmaps are fine (16M objects per pack).
- BFS with one R2 round per depth: a linear history of N commits costs N rounds; the section 9 commit-region read is not used. 100k commits ≈ 100k subrequests ≈ 250 slices; acceptable in background, slow.
- GC nudge in `finish` turns every push into a second full-repo consolidate once the young pack ages; each sweep invalidates the plan and forces a rebuild. Cost, not correctness.
- A force-push + sweep during a build makes `lookup` return `None` -> `Error::Internal` retried 8 times on the same cursor (deterministic); should restart instead.
- `Error::Conflict` for "gc busy" burns the attempt ladder; `Reschedule` is the intended tool.
- Unverified per memo: real R2 range reads on multi-GB objects (#6), deployed subrequest limit (#7), `TagRef`/`TreeRefIter`/`EntryMode::is_commit` names on 0.64.1, `futures_util::stream::unfold` under `from_stream`.
- Serve-side hashing on the two-pack path uses `sha1-checked`; ~10 GB is tens of seconds of CPU inside `cpu_ms = 300000`, fine, but a 64 GB pack exceeds the 9,000-subrequest budget as the proof states.
- Fast-path selection between `write_pack` and `stream_verbatim` is described, not coded.

## Verdict
risky. The reshaping is right and closes all three v1 blockers for real; the wire is now correct by construction. But the code as written, after any multi-slice build overlapping a push, serves incomplete packs to every fresh clone and never heals, and dies on repos with a few hundred refs. Both fixes are small and local; with them this is lands-with-caveats.
