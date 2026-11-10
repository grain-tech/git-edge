---
name: verify-gc-barrier
description: Model-check git-edge's GC chain against racing pushes, fetches and janitor passes with TLA+/TLC, in barrier mode (GE_GC_BARRIER=1) and legacy mode. Use when a change touches server/src/jobs/gc.rs (barrier_active, gc_mark, walk_step, commit_ids, pending_page, barrier_handoff_if_ready, abort, wipe_all, begin_build, wipe_build, mpu_err, finish_build, gc_sweep, pr5_sweep, swap_check, fenced_swap, fallback_span, retire_span, retire_finish, retire_resume, wipe_path, end_of_chain), commit_push or its barrier helpers in server/src/repo_do/mod.rs, server/src/jobs/janitor.rs, compact::cohort_live / orphaned / orphan_abort, or fetch planning in server/src/pack/generate.rs, or before trusting a GC or barrier change.
---

# Verify the GC barrier

GC must never delete an object row or the R2 bytes of a pack that a live ref, an in-flight push or an in-flight fetch still needs. Pushes, fetches, janitor passes and GC slices interleave at every `.await`, slice boundary and request. A broken interleaving loses data silently. A clone then fails with a missing object, long after the GC chain ended. The harness tests exercise a few schedules. `verification/tla/GcBarrier.tla` checks every interleaving of up to two pushes, one fetch and up to two GC chains, in both modes. The model has one known finding in the real design. The Findings section describes it.

## Run

```sh
bash verification/tla/verify.sh
```

The script needs bash, curl, sha256sum and tar. It uses `java` 11 or later from PATH. Otherwise, on Linux x86_64, it downloads Temurin JRE 21.0.13+9 and checks the tarball's sha256 `a535a1df9c6284d9618872984e026ae0352d1f7e9e0605425c0e6114f47351dd`. It downloads `tla2tools.jar` v1.7.4 and refuses any jar whose sha256 is not `936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`. Both live in `${GE_TLA_CACHE:-$HOME/.cache/git-edge-tla}`. TLC state files go to a temp dir, so the script writes nothing into the repo. Each TLC run takes one to two seconds. A full run takes under two minutes, most of it JVM start-up.

The script runs five kinds of check:

- A clean config must exit 0 with no error. Deadlock checking is on, and the `Done` action marks the real end states.
- A `Reach_*` guard from `GcBarrierReach.tla` must be violated. Each guard names a state that the clean model must reach, such as "the thin push landed and P0 still retired". A guard that holds means the clean run is vacuous.
- `statechart/ThinPushRace.cfg` must check clean.
- A Bug variant must exit 12 on its named invariant.
- A known finding must exit 12 on its named invariant. The script prints the line with `FINDING` in place of `ok`. A finding is a property that the real design breaks. The model keeps the property and the script records the break. If a finding stops reproducing, the script fails. Then the code or the model changed. Read the finding below, confirm the fix in the code, and turn the case into a clean check.

Expected output at the sync commit:

```
ok  GcBarrier_barrier.cfg                        clean, 16744 distinct states
ok  GcBarrier_legacy.cfg                         clean, 6554 distinct states
ok  GcBarrier_split_legacy.cfg                   clean, 39596 distinct states
ok  GcBarrier_kept_barrier.cfg                   clean, 310306 distinct states
ok  GcBarrier_kept_legacy.cfg                    clean, 92250 distinct states
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
ok  reach barrier Reach_FetchRebuild             reached
ok  reach legacy Reach_FetchRebuild              reached
ok  reach barrier Reach_PlanNamesDead            reached
ok  reach legacy Reach_PlanNamesDead             reached
ok  reach barrier Reach_DrainMarksNew            reached
ok  reach barrier Reach_DeltaHandoff             reached
ok  reach barrier split Reach_PartialRetire      reached
ok  reach barrier kept Reach_FallbackRetire      reached
ok  reach barrier kept Reach_NonLossySwap        reached
ok  reach barrier kept Reach_ThinOverLosslessRetire reached
ok  GcBarrier_bug_noBarrierPending.cfg           violated YoungPacksClosed, 1230 distinct states
ok  GcBarrier_bug_noLandingFeed.cfg              violated RefsResolvable, 605 distinct states
ok  GcBarrier_bug_noEpochCheck.cfg               violated YoungPacksClosed, 1309 distinct states
ok  GcBarrier_bug_sweepIgnoresRefsVersion.cfg    violated RefsResolvable, 517 distinct states
ok  GcBarrier_bug_abortDropsSurvivor.cfg         violated RefsResolvable, 430 distinct states
ok  GcBarrier_bug_janitorNoGrace.cfg             violated FetchReadsSucceed, 654 distinct states
ok  GcBarrier_bug_fallbackUsesBitmap.cfg         violated RefsResolvable, 5237 distinct states
FINDING GcBarrier_finding_split.cfg              violated LiveClosed, 3360 distinct states
FINDING GcBarrier_finding_split_refs.cfg         violated RefsResolvable, 8219 distinct states
verify: all checks passed, with 2 known findings
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
| `LiveClosed` | Every object in any live pack links only to objects that resolve. This is the connectivity rule of `CONTRACTS.md` section 2.5. | Ingest and `apply_one` check only that a link target or a tip is live. A later push can then land a ref onto a broken history. |
| `FetchReadsSucceed` | Every pack that an in-flight fetch planned to read still has its R2 bytes. | A fetch fails halfway through its pack stream. |
| `PushBaseReadsSucceed` | Every pack that an in-flight push resolved a thin-pack base to still has its R2 bytes. | Ingest cannot resolve a delta and the push fails. |

`YoungPacksClosed` is the model's form of the `U ⊆ P` oracle in `testhooks.rs` `mark_check`. GC treats every live non-candidate pack as a root, through the young-pack seed in `gc_mark` and the pending page. `LiveClosed` also covers old packs. An old pack that a sweep keeps is not a root, so only `LiveClosed` sees the finding below.

## What each check means

Clean configs. Each one checks every invariant in every reachable state.

| Config | World and mode |
| --- | --- |
| `GcBarrier_barrier.cfg`, `GcBarrier_legacy.cfg` | Single world: one candidate, one thin or bare push, both modes. |
| `GcBarrier_split_legacy.cfg` | Split world, legacy mode. `pr5_sweep` retires the whole cohort, so the finding does not apply. |
| `GcBarrier_kept_barrier.cfg`, `GcBarrier_kept_legacy.cfg` | Kept world without garbage, both modes. Covers the fallback retirement and the swap with no epoch bump. |
| `statechart/ThinPushRace.cfg` | The chart scope of the single world. |

Vacuity guards. Each one names a state that the clean model must reach.

| Guard | The state it reaches |
| --- | --- |
| `Reach_LandThenRetire` | The thin push landed `main` at `c2`, and a later sweep still retired P0. |
| `Reach_LoserThenRetire` | A push lost its CAS, its pack went live, and P0 still retired. |
| `Reach_BareLandsMidChain` | The bare push created ref `g` while a chain stood. |
| `Reach_ErrThenSurvivor` | A sweep hit an Err after its dead-mark, and the chain ended with a live build pack. |
| `Reach_EpochReject` | The `gc_epoch` check rejected the push. |
| `Reach_FetchOutlivesPack` | A fetch planned a pack that died before the fetch read it. |
| `Reach_FetchRebuild` | `plan_reads` found rows of a walked pack gone and rebuilt the plan. |
| `Reach_PlanNamesDead` | `plan_reads` kept a pack that was dead but still had its rows. That is the window of an Err after a dead-mark. |
| `Reach_DrainMarksNew` | The barrier drain marked `b` after the built snapshot. |
| `Reach_DeltaHandoff` | The barrier delta arm handed off to build with a non-empty frontier. |
| `Reach_PartialRetire` | Split world: the swap kept P1 and retired P0 after a ref landed at `c`. This is the schedule of Finding 1. |
| `Reach_FallbackRetire` | Kept world: `fallback_span` retired P0 with no `gc_epoch` bump. |
| `Reach_NonLossySwap` | Kept world: `fenced_swap` retired P0 with no `gc_epoch` bump. |
| `Reach_ThinOverLosslessRetire` | Kept world: a thin push's lookup resolved a base to P0, and P0 retired with no `gc_epoch` bump before the commit. Barrier only, because `pr5_sweep` always bumps. |

Bug variants. Each one is in the table above and must fail on its named invariant.

Known findings. Each one must still fail on its named invariant.

| Config | Fails | Meaning |
| --- | --- | --- |
| `GcBarrier_finding_split.cfg` | `LiveClosed` | Finding 1. A partial retirement leaves a live object that links to a deleted object. |
| `GcBarrier_finding_split_refs.cfg` | `RefsResolvable` | Finding 1, the harm. The config leaves `LiveClosed` out, so the run goes on until a later push lands a ref on that object. |

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
| `fallbackUsesBitmap` | `jobs/gc.rs` `fallback_span` calls `bm_all_set(&r.bitmap, &need)` in place of `bm_all_set(&r.built, &need)`. An entry marked after the built snapshot then counts as copied. This is the C11 mistake. Kept world. | `RefsResolvable` |

`fallbackUsesBitmap` needs a mark after the built snapshot while the drain is still not empty. A push lands ref `gc` at `c` during the build. The drain marks `c` in P1 and feeds `b`. The fallback fires with `b` still in the frontier. The bug counts `c` as copied, so P1 retires and ref `gc` loses `c`. The real rule keeps P1, because `c` is not built and has no live copy outside the cohort.

`noBarrierPending` needs a non-atomic push whose ref update loses the CAS. A landed tip is already fed by `barrier_feed_landing`, so only the loser's live pack depends on the pending row. `noEpochCheck` reports the loser's pack because BFS meets the loser shape first. With `YoungPacksClosed` left out of the config, a trace of the same length lands `main` at `c2` and breaks `RefsResolvable`.

## Findings

### Finding 1: a partial retirement breaks the connectivity rule

**What goes wrong.** In barrier mode, `fenced_swap` decides each candidate pack on its own. A pack whose marked entries were all built retires. A pack that gained a mark after the built snapshot stays live. The kept pack can hold an unreachable object that links into a retired pack. The build did not copy the link target, because nothing reached the target either. The swap deletes the row of the target. The kept object now links to an object with no row. That breaks the connectivity rule in `CONTRACTS.md` section 2.5.

**Why it matters.** Ingest and `apply_one` check only that a link target or a tip is live. They trust the connectivity rule for everything behind it. A push that starts after the `gc_epoch` bump can land a ref on the kept object. Every clone or fetch of that ref then fails with a missing object.

**The trace.** `GcBarrier_finding_split.cfg` breaks `LiveClosed` in 12 states. The model and the code use these names:

- P0 and P1 are old packs, so both are GC candidates.
- P0 holds `c1`, `b` and `z`. Ref `main` points at `c1`.
- P1 holds `a`, which links to `b`, and `c`, which links to nothing.
- No ref reaches `a`, `b`, `c` or `z`.

The steps:

1. `push_begin` records `gc_epoch` 0 for a push.
2. `gc_mark` starts a barrier chain. P0 and P1 form the cohort. The frontier holds `c1`.
3. `walk_step` marks `c1` in P0. `barrier_handoff_if_ready` moves the chain to build.
4. `commit_push` runs a push with no objects that creates ref `gc` at `c`. `apply_one` finds `c` live in P1. `barrier_feed_landing` puts `c` in `gc_frontier` at gen 1.
5. `begin_build` copies the bitmap to `built`, which is `{c1}`. `finish_build` makes build pack N1 live with `c1`.
6. The sweep drain's `walk_step` pops `c` and marks it in P1.
7. `swap_check` finds `gc_frontier` and `gc_pending` empty and calls `fenced_swap`. The bitmap of P0 is a subset of its built bits, so P0 retires. The retirement is lossy, because `b` and `z` were not built. The bitmap of P1 has `c` and its built bits are empty, so P1 stays live.
8. `retire_span` bumps `gc_epoch` to 1. `retire_finish` dead-marks P0 and deletes its rows. Object `a` in live pack P1 links to `b`, and `b` has no row.

`GcBarrier_finding_split_refs.cfg` leaves `LiveClosed` out and continues the same trace to `RefsResolvable`, in 14 states:

9. A second push begins after the bump. `push_begin` records `gc_epoch` 1.
10. `commit_push` creates ref `ga` at `a`. Step 2 passes, because the epochs match. `apply_one` finds `a` live. Ref `ga` cannot resolve `b`.

The full TLC output of both runs is in the evidence file `tlc-finding-partial-retirement.txt` of the review that found it.

**Why the code allows it.** These are the code sites at the sync commit:

- `jobs/gc.rs` `fenced_swap` checks `bm_bitmap_subset(&r.bitmap, &r.built)` for each `marked` row on its own. No rule ties the retirement of one pack to a pack that stays live.
- `jobs/gc.rs` `gc_mark` seeds the frontier only from live non-candidate packs. A kept candidate is not a root, and nothing marks the link targets of its unreachable objects.
- `jobs/gc.rs` `retire_span` bumps `gc_epoch` for a lossy retirement. The bump rejects only a push that began before it.
- `repo_do/mod.rs` `apply_one` accepts a new tip when `idx.lookup` finds one live row. `pack/run.rs` `run` accepts a link when the target has a live row. Neither walks further.

The legacy sweep `pr5_sweep` retires the whole cohort, so `a` dies with `b`. `GcBarrier_split_legacy.cfg` checks clean.

**Can a normal git client reach it?** Step 4 needs any mark in P1 after the built snapshot. A normal push that sends `c` again in a new pack also works. Then `c` has live rows in P1 and in the new pack, and the `GROUP BY` in `walk_step` can return the row in P1. Step 10 needs a client that names `a` without sending `a`. Git sends a commit only when the commit is not reachable from a ref the server advertised. Smart HTTP advertises refs in one request and sends the pack in a later request. A client that saw a ref at `a` can push a child of `a` as a thin pack after the ref was deleted. The gap between the two requests must cover the GC quiet time and the whole chain. A client that names `a` by its sha needs no gap.

**How to fix it.** This skill does not change `server/`. One fix passed every invariant in a scratch copy of the model, at 36902 states in the split world. Retire a pack lossily only when every cohort pack retires. When any cohort pack stays live, retire only the packs whose rows were all built. A fix must keep `LiveClosed` in every config. After the fix, `GcBarrier_finding_split.cfg` checks clean, and `verify.sh` fails until you turn both finding cases into clean checks.

## Coverage record

Sync commit: `5f4938c5bd5c3c544d745db46f533c11dfc8d6ef`. The model was first confirmed at `11a52051d8b0d1f7687ead8202f24913249ba82a`. Between the two commits, `server/`, `CONTRACTS.md` and `SETUP.md` are byte-identical, and only `tests/worker/` changed, which this model does not cover. After a rebase onto a newer main, reread every function in this table at the new commit. Move the sync commit forward only after you confirm the model still describes the code.

| Model action | Code |
| --- | --- |
| `PushBegin` | `repo_do/mod.rs` `push_begin`: the pushes row records `gc_epoch` |
| `PushLookup` | `repo_do/mod.rs` `push_lookup`, a separate sync request |
| `PushUpload` | `pack/ingest.rs` pass B with its external base read, then `repo_do/mod.rs` `push_index` |
| `PushCommit` | `repo_do/mod.rs` `commit_push` steps 1 to 5, one sync span: the epoch check, `jobs::gc::barrier_active`, `barrier_write_pending_before_promote`, the promote, `apply_one` with `ref_cas_ok`, `barrier_feed_landing` and the bumps |
| `FetchWalk` | `pack/generate.rs` `send_set`, with its `load` and `load_commits` rounds |
| `FetchPlan` | `pack/generate.rs` `plan_reads`: `store` `entries_of` with no live filter, then the one-time `rebuild` |
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
- Concurrent pushes. The split and kept worlds run two pushes, one after the other. The single world runs one push.
- Pushes to the same ref, ref deletes, tags with peeled targets, pins and protected refs.
- Import jobs, `import_start` and `import_pack`.
- Boot repair and its resurrection of chain-less job rows, the `stray` and `legacy_chain` guards, and pre-PR6 chains with `built IS NULL`.
- An Err before a dead-mark, the mixed and all-live arms of `retire_resume`, and crashes that roll a span back. A rolled-back span is the same as a span that never ran.
- A GC slice's own R2 reads after the janitor deleted bytes.
- The checkpoint, tail and part layout inside `build`, and the multipart upload's byte content.
- Quotas, scopes, tokens and rate limits in `commit_push`.

## Bounds and assumptions

- One candidate pack P0 holds `c1`, `b` and `z`. Ref `main` points at `c1`. `b` is unreachable at the mark snapshot. `z` is garbage, so every repack runs and every retirement of P0 is lossy.
- The single world has one push. At begin it takes one of two shapes. A thin push uploads pack Q with `c2`, which links to `c1` and `b` and omits both. Its update of `main` to `c2` lands or loses the CAS. A bare push has no pack and creates ref `g` at `b`.
- `Garbage` puts the unreachable object `z` in P0. The single and split worlds set it to `TRUE`. The kept world sets it to `FALSE`, so every P0 entry can be built.
- The kept world adds old pack P1 with `c`, which links to `b`. Two pushes run one after the other. The first push is bare and creates ref `g` at `b` or ref `gc` at `c`. The second push is bare or thin. A thin second push can have its bases retired with no `gc_epoch` bump, after the first push made every P0 entry reachable. This world reaches the fallback retirement and a swap with no `gc_epoch` bump. The single world cannot reach either path. A single fully marked candidate makes `begin_build` wipe, and an unmarked entry makes every retirement lossy.
- The split world adds old pack P1 with `a` and `c`. Object `a` links to `b`. Two bare pushes run one after the other. Each one creates ref `gc` at `c` or ref `ga` at `a`. A second push begins only after the first one ends.
- No sha has rows in two candidate packs, so the model keeps marks as a set of shas.
- One fetch of `main`. Up to two mark inits. At most one Err after a dead-mark. At most one rebuild a run, because one build id always stays free for a later chain.
- The model has no clock. In place of GRACE, the janitor keeps a dead pack's bytes while an in-flight request names that pack. The `janitorNoGrace` variant drops this hold. The hold rests on these facts at the sync commit:
  - `jobs/janitor.rs` sets `GRACE_MS` and `PUSH_TIMEOUT_MS` both to `3_600_000`, which is one hour.
  - `run_slice` deletes the R2 key of a dead pack only when `dead_at < now - GRACE_MS` and the pack has no `objects` rows.
  - Every pack a request reads comes from a lookup that ran after the request began. `store` `lookup`, which `send_set`, `load` and `push_lookup` use, returns rows of live packs only. So the `dead_at` of that pack is not earlier than the start of the request.
  - `store` `entries_of`, which `plan_reads` calls, does not filter on live packs. A plan can therefore name a pack that is already dead but still has rows. That pack came from a live lookup of the walk, so the fact above still holds. `FetchPlan` keeps such a pack, and `Reach_PlanNamesDead` reaches that case.
  - A push. The janitor deletes the bytes of a base pack only after that pack was dead for GRACE. The pack died after the push began, so the push began more than `PUSH_TIMEOUT_MS` ago. Step 1 of the same janitor slice expires that push before step 3 deletes keys, at most 400 pushes a slice. GRACE equal to PUSH_TIMEOUT is the smallest value that keeps this fact true.
  - A fetch has no timeout. The hold assumes that a fetch ends less than GRACE after its last lookup. A fetch that runs longer can fail halfway.
  - `FetchReadsSucceed` and `PushBaseReadsSucceed` are about reads in flight. A read that finds no bytes fails that request. That failure loses no stored data, because ingest writes resolved objects into the new pack, and readers resolve through SQLite rows.
- Candidates are the old packs. Every pack created during the run is young and never a candidate.
- The push timeout is a free janitor expiry of an open push. The swap tries and drain slice bounds are a free fallback trigger at any non-empty swap check. The net-positive byte rule in `fallback_span` is a free choice.
- `refs_version` is a flag that says the version moved since this chain's init. Every read of `refs_version` in the code compares it with the init capture.
- A frontier batch is one sha. The pending page covers a whole pack in one span. `gc.delta_rounds` saturates at `DeltaCap`, which is 1 in the configs.
- Most actions are one synchronous span of the code. They split at an `.await`, a slice boundary or a separate request. `GcPop` and `GcCommit` are finer than the code. A finer split adds interleavings, so it can only add counterexamples.
- Five actions merge code that can interleave. Each one is harmless for these invariants, for the reason given:

| Action | Code it merges | Why the merge hides no counterexample |
| --- | --- | --- |
| `FetchWalk` | The lookup and R2 read rounds of `send_set`, through `load` and `load_commits`. | Each lookup filters on live packs and runs after the request began, so the GRACE hold keeps every pack the walk names. A row that goes away mid-walk makes `load` fail the fetch closed. A pack that dies before its mark flags the set stale, and `plan_reads` rebuilds as `FetchPlan` does. |
| `PushUpload` | The external base reads, the R2 upload and the `push_index` batches. | Pack Q stays `ingesting`. No lookup, mark or swap reads an `ingesting` pack. The base packs are held by GRACE. An expiry mid-upload ends like an expiry before the upload, because `push_index` refuses a push that is not open. |
| `GcBuildFinish` | The build slices, the `finish_pack` await and `finish_build`. | The build pack stays `ingesting` with `push_id` NULL. No lookup reads such a pack and the janitor skips the pack. `built` is frozen at `begin_build`, so a mark during the slices cannot change what the build copies. |
| `GcRebuild` | The HEAD await in `mpu_err`, then `wipe_build` and `begin_build`. | The HEAD reads only the chain's own upload. No other actor touches that upload, so the await only moves the span later. |
| `GcPage` | One `pending_page` window per span in the code. The model feeds the whole pack in one action. | A window only feeds `gc_frontier`. The `gc_pending` row stays until the last window, so no swap lands mid-page. A sha is marked only when a pop takes it. Any split of a page across the snapshot or a swap check is reachable as a whole page and then single pops. |

- The fetch limit. `FetchWalk` wants the closure of the current target of `main`. A real client names its wants from an earlier advertisement, so a want can be an older tip. The model does not cover a want that no ref reaches any more.

## Read a result

- A clean run, every guard reached and every variant failing as expected mean the change keeps the promises within the bounds above.
- If a clean config fails, read the trace. Each state names its action and shows `packs`, `rows`, `bytes`, `refs`, `push`, `phase`, `marked`, `built`, `fr` and `pending`. Replay the trace against the code at the sync commit, one action for each span in the coverage record. A real reproduction needs the harness in `tests/harness/test_barrier.py` or `test_gc_safety.py`. If the code breaks the same way, it is a real bug. Report it and keep the invariant. If the code does not break, the model has drifted from the code, so fix the model.
- If a guard holds, the clean run no longer reaches that state. Fix the model before you trust a clean run.
- If a variant passes, or fails a different invariant, the model can no longer see that bug. Fix the model before you trust a clean run.
- To read a trace without other tools, run TLC by hand on the config with the `-difftrace` flag. Use the java and jar that `verify.sh` caches. Example: `java -jar tla2tools.jar -workers 1 -difftrace -metadir "$(mktemp -d)" -config GcBarrier_finding_split.cfg GcBarrier.tla`, run in `verification/tla`.
- TLC prints `Error: Invariant <name> is violated.` and then the shortest path to the first state that breaks the invariant. Each step starts with `State <n>: <Action line ...>`. The action name is the model action of the coverage record. With `-difftrace`, State 1 lists every variable, and each later state lists only the variables that the action changed. Read the action names in order as the schedule, and map each one to its code span in the coverage record.
- The `statechart` skill of the pstack plugin can tell a violation as a path in plain words and draw a chart of a clean run. This repo does not include that skill. A reader without the plugin uses the steps above.

## Statechart

`verification/tla/statechart/` charts the most important flow. A thin push races one barrier-mode chain: the push lands a ref onto an unmarked candidate object, the landing feed and the pending page queue its objects, and the sweep keeps or retires the old pack. `ThinPushRace.cfg` narrows the model with `Scope = "chart"` to the thin push and one chain, with no fetch, no expiry, no rebuild, no orphan abort and no Err. `ThinPushRace.view.ts` gives the reader's words in two regions, the push and the GC. The chart tool is the `statechart` skill of the pstack plugin, which is not part of this repo. Run that skill on `GcBarrier.tla` with that config and view, from a scratch folder outside the repo. It reaches 138 states and draws 16 chart states and 19 arrows. Set `Bug` to a variant in a scratch copy of the config to see where the variant breaks.
