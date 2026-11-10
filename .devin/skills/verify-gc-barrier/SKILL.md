---
name: verify-gc-barrier
description: Model-check git-edge's GC chain against racing pushes, fetches and janitor passes with TLA+/TLC, in barrier mode (GE_GC_BARRIER=1) and legacy mode. Use when a change touches server/src/jobs/gc.rs (barrier_active, gc_mark, walk_step, commit_ids, pending_page, barrier_handoff_if_ready, abort, wipe_all, begin_build, wipe_build, mpu_err, finish_build, gc_sweep, pr5_sweep, swap_check, fenced_swap, fallback_span, retire_span, retire_finish, retire_resume, wipe_path, end_of_chain), commit_push or its barrier helpers in server/src/repo_do/mod.rs, server/src/jobs/janitor.rs, compact::cohort_live / orphaned / orphan_abort, or fetch planning in server/src/pack/generate.rs, or before trusting a GC or barrier change.
---

# Verify the GC barrier

GC must never delete an object row or the R2 bytes of a pack that a live ref, an in-flight push or an in-flight fetch still needs. Pushes, fetches, janitor passes and GC slices interleave at every `.await`, slice boundary and request. A broken interleaving loses data silently. A clone then fails with a missing object, long after the GC chain ended. The harness tests exercise a few schedules. `verification/tla/GcBarrier.tla` checks every interleaving of one push, one fetch and up to two GC chains, in both modes.

## Run

```sh
bash verification/tla/verify.sh
```

The script needs bash, curl, sha256sum and tar. It uses `java` 11 or later from PATH. Otherwise, on Linux x86_64, it downloads Temurin JRE 21.0.13+9 and checks the tarball's sha256 `a535a1df9c6284d9618872984e026ae0352d1f7e9e0605425c0e6114f47351dd`. It downloads `tla2tools.jar` v1.7.4 and refuses any jar whose sha256 is not `936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`. Both live in `${GE_TLA_CACHE:-$HOME/.cache/git-edge-tla}`. TLC state files go to a temp dir, so the script writes nothing into the repo. A run takes about one and a half minutes.

The script runs four kinds of check:

- A clean config must exit 0 with no error. Deadlock checking is on, and the `Done` action marks the real end states.
- A `Reach_*` guard from `GcBarrierReach.tla` must be violated. Each guard names a state that the clean model must reach, such as "the thin push landed and P0 still retired". A guard that holds means the clean run is vacuous.
- `statechart/ThinPushRace.cfg` must check clean.
- A Bug variant must exit 12 on its named invariant.

Expected output at the sync commit:

```
ok  GcBarrier_barrier.cfg                        clean, 8461 distinct states
ok  GcBarrier_legacy.cfg                         clean, 3418 distinct states
ok  statechart/ThinPushRace.cfg                  clean, 138 distinct states
ok  reach barrier Reach_LandThenRetire           reached
ok  reach legacy Reach_LandThenRetire            reached
ok  reach barrier Reach_LoserThenRetire          reached
ok  reach legacy Reach_LoserThenRetire           reached
ok  reach barrier Reach_BareLandsMidChain        reached
ok  reach legacy Reach_BareLandsMidChain         reached
ok  reach barrier Reach_ErrThenSurvivor          reached
ok  reach legacy Reach_ErrThenSurvivor           reached
ok  reach barrier Reach_EpochReject              reached
ok  reach legacy Reach_EpochReject               reached
ok  reach barrier Reach_FetchOutlivesPack        reached
ok  reach legacy Reach_FetchOutlivesPack         reached
ok  reach barrier Reach_DrainMarksNew            reached
ok  reach barrier Reach_DeltaHandoff             reached
ok  GcBarrier_bug_noBarrierPending.cfg           violated YoungPacksClosed, 1049 distinct states
ok  GcBarrier_bug_noLandingFeed.cfg              violated RefsResolvable, 523 distinct states
ok  GcBarrier_bug_noEpochCheck.cfg               violated YoungPacksClosed, 1116 distinct states
ok  GcBarrier_bug_sweepIgnoresRefsVersion.cfg    violated RefsResolvable, 434 distinct states
ok  GcBarrier_bug_abortDropsSurvivor.cfg         violated RefsResolvable, 373 distinct states
ok  GcBarrier_bug_janitorNoGrace.cfg             violated FetchReadsSucceed, 565 distinct states
verify: all checks passed
```

A different state count with the same verdicts means the model or a config changed. Confirm that the change was intended before you update this block.

## What each invariant means

| Invariant | Promise | Harm if broken |
| --- | --- | --- |
| `TypeOK` | Every variable stays inside its declared domain. | The model itself is wrong. |
| `RefsResolvable` | Every object reachable from any ref has an objects row in a live pack whose R2 bytes exist. | A clone or fetch of that ref fails with a missing object. |
| `LiveBytesPresent` | No live pack ever lacks its R2 bytes. | Reads from a live pack fail. |
| `CommittedPushResolvable` | A ref that a committed push reported as landed resolves completely while the ref still points there. | The server said `ok` and then lost the pushed history. |
| `YoungPacksClosed` | Every object in a live pack written during the run resolves completely. Such packs are pushed packs, a CAS loser's pack and build packs. | A later push can name that object as a thin-pack base and land a ref onto a broken history. |
| `FetchReadsSucceed` | Every pack that an in-flight fetch planned to read still has its R2 bytes. | A fetch fails halfway through its pack stream. |
| `PushBaseReadsSucceed` | Every pack that an in-flight push resolved a thin-pack base to still has its R2 bytes. | Ingest cannot resolve a delta and the push fails. |

`YoungPacksClosed` is the model's form of the `U ⊆ P` oracle in `testhooks.rs` `mark_check`. GC treats every live non-candidate pack as a root, through the young-pack seed in `gc_mark` and the pending page.

## Bug variants

Each variant is a one-site break of the real code:

| `Bug` | Code break it mirrors | Violates |
| --- | --- | --- |
| `noBarrierPending` | `repo_do/mod.rs` `commit_push` step 3 skips `self.barrier_write_pending_before_promote(...)`, so no `gc_pending` row and no `refs_version` bump precede the promote. | `YoungPacksClosed` |
| `noLandingFeed` | `repo_do/mod.rs` `apply_one` skips `self.barrier_feed_landing(&feeds)`. | `RefsResolvable` |
| `noEpochCheck` | `repo_do/mod.rs` `commit_push` step 2 skips the `gc_epoch != push.gc_epoch` rejection. | `YoungPacksClosed` |
| `sweepIgnoresRefsVersion` | `jobs/gc.rs` `pr5_sweep` skips its `refs_moved` abort. Legacy mode. | `RefsResolvable` |
| `abortDropsSurvivor` | `jobs/gc.rs` `wipe_all` always takes the kill arm for `gc.new_pack`, even when a cohort pack already died. | `RefsResolvable` |
| `janitorNoGrace` | `jobs/janitor.rs` `run_slice` deletes dead-pack R2 keys without the `dead_at < now - GRACE_MS` filter. | `FetchReadsSucceed` |

`noBarrierPending` needs a non-atomic push whose ref update loses the CAS. A landed tip is already fed by `barrier_feed_landing`, so only the loser's live pack depends on the pending row. `noEpochCheck` reports the loser's pack because BFS meets the loser shape first. With `YoungPacksClosed` left out of the config, a trace of the same length lands `main` at `c2` and breaks `RefsResolvable`.

## Coverage record

Sync commit: `11a52051d8b0d1f7687ead8202f24913249ba82a`. This branch was cut from that commit, and `server/`, `tests/`, `CONTRACTS.md` and `SETUP.md` were unchanged on the branch when the model was confirmed. After a rebase onto a newer main, reread every function in this table at the new commit. Move the sync commit forward only after you confirm the model still describes the code.

| Model action | Code |
| --- | --- |
| `PushBegin` | `repo_do/mod.rs` `push_begin`: the pushes row records `gc_epoch` |
| `PushLookup` | `repo_do/mod.rs` `push_lookup`, a separate sync request |
| `PushUpload` | `pack/ingest.rs` pass B with its external base read, then `repo_do/mod.rs` `push_index` |
| `PushCommit` | `repo_do/mod.rs` `commit_push` steps 1 to 5, one sync span: the epoch check, `jobs::gc::barrier_active`, `barrier_write_pending_before_promote`, the promote, `apply_one` with `ref_cas_ok`, `barrier_feed_landing` and the bumps |
| `FetchPlan` | `pack/generate.rs` `send_set` and `plan_reads`, including the one-time `rebuild` |
| `FetchRead` | `pack/generate.rs` `pack_chunk` |
| `JanitorExpire`, `JanitorReap`, `JanitorDelete` | `jobs/janitor.rs` `run_slice`: stale pushes, ingesting packs of expired or rejected pushes, dead-pack R2 keys after `GRACE_MS` |
| `GcMarkInit` | `jobs/gc.rs` `gc_mark` first slice: wipe, `gc.chain`, `gc.refs_version`, the candidate set, ref roots, the young-pack seed, `gc.started_at`, `gc.barrier` |
| `GcPage` | `jobs/gc.rs` `pending_page` |
| `GcPop` | `jobs/gc.rs` `walk_step` span A, the pop and the live-row lookup before the R2 read, plus the legacy `refs_moved` check in the `gc_mark` loop |
| `GcCommit` | `jobs/gc.rs` `commit_ids`, behind `phase_fence` |
| `GcHandoff`, `GcHandoffLegacy` | `jobs/gc.rs` `barrier_handoff_if_ready`, and the `Step::Empty` arm of `gc_mark` |
| `GcAbortLegacy` | `jobs/gc.rs` `abort` from the `gc_mark` walk loop, from `begin_build`, and from the `gc_mark` slice that finds the chain at build or sweep |
| `GcOrphanAbort` | `compact.rs` `orphaned` and `orphan_abort`, reached from the M4 arm of `gc_mark` |
| `GcBeginBuild` | `jobs/gc.rs` `begin_build`, with `compact.rs` `cohort_live` |
| `GcBuildFinish` | `jobs/gc.rs` `build`, `finish_pack` or the HEAD hit in `mpu_err`, then `finish_build` |
| `GcRebuild` | `jobs/gc.rs` `mpu_err` returning `Flow::Rebuild`, `wipe_build`, then `begin_build` in the same span |
| `GcSweepLegacy` | `jobs/gc.rs` `pr5_sweep`, with an optional Err at `gc.sweep.after_deadmark` |
| `GcSwap` | `jobs/gc.rs` `swap_check`, `fenced_swap`, `retire_span`, `retire_finish`, `wipe_path`, `end_of_chain`, one sync span, with an optional Err at `gc.swap.after_deadmark` |
| `GcFallback` | `jobs/gc.rs` `swap_check` past its tries or slices bound, then `fallback_span` |
| `GcRetireResume` | `jobs/gc.rs` `retire_resume`, the all-dead arm |
| `WipeAll`, `WipePath` | `jobs/gc.rs` `wipe_all` with the gc.retire arm and the keep rule, and `wipe_path` |

Not covered:

- Compaction: `gc_compact`, `compact::swap`, compact-mode chains and their quarantine. Only `cohort_live` is modelled.
- `--atomic` pushes and their demote of the promoted pack.
- More than one push, concurrent pushes to the same ref, ref deletes, tags with peeled targets, pins and protected refs.
- Import jobs, `import_start` and `import_pack`.
- Boot repair and its resurrection of chain-less job rows, the `stray` and `legacy_chain` guards, and pre-PR6 chains with `built IS NULL`.
- An Err before a dead-mark, the mixed and all-live arms of `retire_resume`, and crashes that roll a span back. A rolled-back span is the same as a span that never ran.
- A GC slice's own R2 reads after the janitor deleted bytes.
- The checkpoint, tail and part layout inside `build`, and the multipart upload's byte content.
- Quotas, scopes, tokens and rate limits in `commit_push`.

## Bounds and assumptions

- One candidate pack P0 holds `c1`, `b` and `z`. Ref `main` points at `c1`. `b` is unreachable at the mark snapshot. `z` is garbage, so every repack runs and every retirement of P0 is lossy.
- One push. At begin it takes one of two shapes. A thin push uploads pack Q with `c2`, which links to `c1` and `b` and omits both. Its update of `main` to `c2` lands or loses the CAS. A bare push has no pack and creates ref `g` at `b`.
- One fetch of `main`. Up to two mark inits. At most one Err after a dead-mark. At most one rebuild a run, because one build id always stays free for a later chain.
- The model has no clock. GRACE is longer than a request, so the janitor keeps a dead pack's bytes while an in-flight request planned to read that pack. A planned pack was live at plan time, so it died after the request started. The `janitorNoGrace` variant drops this hold.
- Candidates are the old packs. Every pack created during the run is young and never a candidate.
- The push timeout is a free janitor expiry of an open push. The swap tries and drain slice bounds are a free fallback trigger at any non-empty swap check. The net-positive byte rule in `fallback_span` is a free choice.
- `refs_version` is a flag that says the version moved since this chain's init. Every read of `refs_version` in the code compares it with the init capture.
- A frontier batch is one sha. The pending page covers a whole pack in one span. `gc.delta_rounds` saturates at `DeltaCap`, which is 1 in the configs.
- An action splits only at an `.await`, a slice boundary or a separate request. Some actions are finer than the code, such as the pending page and the pop. A finer split adds interleavings, so it can only add counterexamples, never hide one.

## Read a result

- A clean run, every guard reached and every variant failing as expected mean the change keeps the promises within the bounds above.
- If a clean config fails, read the trace. Each state names its action and shows `packs`, `rows`, `bytes`, `refs`, `push`, `phase`, `marked`, `built`, `fr` and `pending`. Replay the trace against the code at the sync commit, one action for each span in the coverage record. A real reproduction needs the harness in `tests/harness/test_barrier.py` or `test_gc_safety.py`. If the code breaks the same way, it is a real bug. Report it and keep the invariant. If the code does not break, the model has drifted from the code, so fix the model.
- If a guard holds, the clean run no longer reaches that state. Fix the model before you trust a clean run.
- If a variant passes, or fails a different invariant, the model can no longer see that bug. Fix the model before you trust a clean run.
- Read every TLC result with the `/statechart` skill. It tells each violation as its shortest path in plain words, and on a pass it draws how the system moves.

## Statechart

`verification/tla/statechart/` charts the most important flow. A thin push races one barrier-mode chain: the push lands a ref onto an unmarked candidate object, the landing feed and the pending page queue its objects, and the sweep keeps or retires the old pack. `ThinPushRace.cfg` narrows the model with `Scope = "chart"` to the thin push and one chain, with no fetch, no expiry, no rebuild, no orphan abort and no Err. `ThinPushRace.view.ts` gives the reader's words in two regions, the push and the GC. Run `/statechart` on `GcBarrier.tla` with that config and view, from a scratch folder outside the repo. It reaches 138 states and draws 16 chart states and 19 arrows. Set `Bug` to a variant in a scratch copy of the config to see where the variant breaks.
