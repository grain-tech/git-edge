--------------------------- MODULE GcBarrier ---------------------------
(***************************************************************************)
(* GcBarrier: a finite model of git-edge's GC chain racing pushes and a    *)
(* fetch, in barrier mode (GE_GC_BARRIER=1) and in legacy mode.            *)
(*                                                                         *)
(* Promise: GC never deletes an object row or R2 bytes that a live ref, an *)
(* in-flight push or an in-flight fetch still needs, however pushes,       *)
(* fetches, janitor passes and GC slices interleave.                       *)
(*                                                                         *)
(* Code (names, not line numbers -- the branch gets rebased):              *)
(*   server/src/repo_do/mod.rs   push_begin, push_lookup, push_index,      *)
(*       commit_push, barrier_write_pending_before_promote,                *)
(*       barrier_feed_landing, apply_one                                   *)
(*   server/src/jobs/gc.rs       barrier_active, gc_mark, walk_step,       *)
(*       commit_ids, pending_page, barrier_handoff_if_ready, abort,        *)
(*       wipe_all, begin_build, wipe_build, mpu_err, finish_build,         *)
(*       gc_sweep, pr5_sweep, swap_check, fenced_swap, fallback_span,      *)
(*       retire_span, retire_finish, retire_resume, wipe_path,             *)
(*       end_of_chain                                                      *)
(*   server/src/jobs/compact.rs  cohort_live, orphaned, orphan_abort       *)
(*   server/src/jobs/janitor.rs  run_slice                                 *)
(*   server/src/pack/generate.rs send_set, plan_reads, pack_chunk          *)
(*   server/src/pack/ingest.rs   the external thin-pack base read          *)
(*                                                                         *)
(* One action is one synchronous span of the code. Actions split only at  *)
(* an .await, a slice boundary or a separate request. Splitting a span     *)
(* more finely than the code only adds interleavings, so it is sound for   *)
(* the safety invariants below; no action merges code that can interleave. *)
(*                                                                         *)
(* World "single". Objects c1, b, z, c2 with links c2 -> {c1, b}. The     *)
(* old pack P0 holds c1, b and z and is the only GC candidate. Ref main ->  *)
(* c1, so b is unreachable when the mark snapshot is taken. z is garbage:  *)
(* P0 always has an unmarked entry, so begin_build always repacks and a    *)
(* retirement of P0 is always lossy. One push takes one of two shapes,     *)
(* picked at begin:                                                        *)
(*   thin: a pack Q = {c2} that omits its bases c1 and b because lookup    *)
(*         found them live; its command main: c1 -> c2 lands or loses the  *)
(*         CAS (a loser still promotes Q, non-atomic push).                *)
(*   bare: no pack; the command creates ref g -> b.                        *)
(*                                                                         *)
(* World "split". Two old candidates. P0 holds c1, b and z as above. P1    *)
(* holds a, which links to b, and c, which links to nothing. Nothing       *)
(* reaches a or c at the snapshot. Two bare pushes run one after the       *)
(* other; each creates ref gc -> c or ga -> a. A second candidate lets     *)
(* fenced_swap keep one pack and retire the other.                         *)
(*                                                                         *)
(* World "kept". Two old candidates. P0 holds c1 and b, and z only when    *)
(* Garbage. P1 holds c, which links to b. Two bare pushes each create ref  *)
(* g -> b or gc -> c. Without z every P0 entry can be built, so the        *)
(* lossless arms are reachable: fallback_span retiring a pack, and a       *)
(* fenced_swap with no gc_epoch bump. The single world cannot reach them:  *)
(* a single fully marked candidate makes begin_build wipe instead.         *)
(*                                                                         *)
(* One fetch wants main. Build packs N1..N3 are fresh ids for the repack;  *)
(* one id is always kept for a later chain, so a run rebuilds at most      *)
(* once. Compaction is out of scope; cohort_live, which                    *)
(* it shares with the barrier paths, is modelled.                          *)
(*                                                                         *)
(* Abstractions, each argued in .devin/skills/verify-gc-barrier/SKILL.md:  *)
(*  - No clock. GRACE > request lifetime becomes a hold: the janitor may   *)
(*    delete a dead pack's bytes only while no in-flight request (fetch    *)
(*    plan, push base read) names that pack. A named pack was live when    *)
(*    the request planned it, so it died after the request started.        *)
(*  - Candidate = old: P0 (and P1 in "split") are older than the grace    *)
(*    cutoff; every pack created during the run is young and never a       *)
(*    candidate. marked is a set of shas, sound because no sha has rows in *)
(*    two candidate packs.                                                 *)
(*  - The push timeout is a nondeterministic janitor expiry of an open     *)
(*    push. Swap tries and drain slices are a nondeterministic fallback    *)
(*    trigger at any non-empty swap check.                                 *)
(*  - refs_version is a flag "moved since this chain's init"; every read   *)
(*    of it in the code is an equality test against the init capture.     *)
(*  - A frontier batch is one sha (ROUND = 1). The pending page covers a   *)
(*    whole pack in one span. gc.delta_rounds saturates at DeltaCap.       *)
(*  - An Err after a dead-mark (failpoints gc.swap.after_deadmark and      *)
(*    gc.sweep.after_deadmark) happens at most MaxErrs times; DO SQLite    *)
(*    keeps the writes before it.                                          *)
(***************************************************************************)

EXTENDS Naturals, FiniteSets

CONSTANTS
  Barrier,    \* TRUE: GE_GC_BARRIER captured as gc.barrier=1 at mark init
  Bug,        \* "none" or one named one-site break (GcBarrier_bug_*.cfg)
  MaxChains,  \* mark inits allowed: one chain plus restarts
  DeltaCap,   \* mark_delta_rounds knob (gc.delta_rounds handoff cap)
  MaxErrs,    \* Errs after a dead-mark the run may suffer
  Scope,      \* "full" for every check; "chart" narrows the run to the
              \* thin push racing one chain, for the statechart view
  World,      \* "single", "split" or "kept": the object world, see the header
  Garbage,    \* TRUE: P0 also holds z, which nothing ever reaches
  MaxPushes   \* pushes the run may begin, one after another

None == "none"

(* ---- the fixed world ---------------------------------------------- *)

Objs      == {"c1", "b", "z", "c2", "a", "c"}
Links     == [o \in Objs |-> CASE o = "c2" -> {"c1", "b"}
                              [] o = "a"  -> {"b"}
                              [] o = "c" /\ World = "kept" -> {"b"}
                              [] OTHER    -> {}]
(* every link target has no links of its own, so one step is the whole
   closure *)
Closure   == [o \in Objs |-> {o} \cup Links[o]]
OldPacks  == IF World = "single" THEN {"P0"} ELSE {"P0", "P1"}
BuildIds  == <<"N1", "N2", "N3">>
Packs     == {"P0", "P1", "Q", "N1", "N2", "N3"}
RefNames  == {"main", "g", "gc", "ga"}
(* the shas a bare push may land, and the ref each one creates *)
BareTips  == CASE World = "single" -> {"b"}
               [] World = "split"  -> {"c", "a"}
               [] OTHER            -> {"b", "c"}
RefOf(t)  == CASE t = "b" -> "g" [] t = "c" -> "gc" [] OTHER -> "ga"
P0Init    == {"c1", "b"} \cup (IF Garbage THEN {"z"} ELSE {})
P1Init    == CASE World = "split" -> {"a", "c"}
               [] World = "kept"  -> {"c"}
               [] OTHER           -> {}
PState    == {"absent", "ingesting", "live", "dead"}
PushSt    == {"idle", "begun", "looked", "uploaded",
              "committed", "rejected", "expired", "failed"}
Open      == {"begun", "looked", "uploaded"}
Phases    == {"idle", "walking", "build", "sweep"}
NoGen     == 2                      \* frontier "no row"; gens are 0 and 1

VARIABLES
  packs,       \* packs.state per pack id
  rows,        \* objects rows: pack -> set of shas
  bytes,       \* R2 key packs/<id>.pack present
  refs,        \* refs table
  moved,       \* refs_version # gc.refs_version (moved since init)
  epoch,       \* meta gc_epoch
  push,        \* the current push: [st, shape, tip, epoch, base, cas]
  npush,       \* pushes begun so far
  landed,      \* ref -> sha the committed push landed (CommittedPush...)
  fetch,       \* [st, plan]
  phase,       \* gc.phase, "idle" when no chain stands
  bar,         \* gc.barrier=1 captured by this chain
  cohort,      \* marked.pack_id: the frozen candidate set
  marked,      \* marked.bitmap bits (shas, all in cohort packs)
  built,       \* marked.built (barrier) -- the as-copied snapshot
  fr,          \* gc_frontier: sha -> gen (0/1) or NoGen
  seen,        \* gc_seen
  pending,     \* gc_pending pack ids
  rounds,      \* gc.delta_rounds, saturating at DeltaCap
  newPack,     \* gc.new_pack
  retire,      \* gc.retire intent ids ({} = no intent row)
  walk,        \* one walk_step between its pop span and commit_ids
  chains,      \* mark inits so far
  errs,        \* Errs after a dead-mark so far
  diedBy       \* ghost: the path that dead-marked each old pack

vars == <<packs, rows, bytes, refs, moved, epoch, push, npush, landed, fetch,
          phase, bar, cohort, marked, built, fr, seen, pending, rounds,
          newPack, retire, walk, chains, errs, diedBy>>

gcVars == <<phase, bar, cohort, marked, built, fr, seen, pending, rounds,
            newPack, retire, walk>>

NoWalk == [sha |-> None, gen |-> 0, pk |-> None]

(* ---- derived predicates ------------------------------------------- *)

Live(p)          == packs[p] = "live"
Holders(o)       == {p \in Packs : Live(p) /\ o \in rows[p]}
HasLive(o)       == Holders(o) # {}
Resolves(o)      == \E p \in Holders(o) : bytes[p]
ClosureOk(o)     == \A x \in Closure[o] : Resolves(x)

(* compact.rs cohort_live: marked is non-empty and every marked pack is
   live *)
CohortLive == cohort # {} /\ \A p \in cohort : Live(p)

(* gc.rs barrier_active: a chain stands and captured gc.barrier=1 *)
BarrierActive == phase # "idle" /\ bar

Min(a, b) == IF a <= b THEN a ELSE b

(* the min-gen upsert of commit_ids / pending_page / barrier_feed_landing:
   shas in S that are not in gc_seen enter at gen g or keep a lower gen *)
Feed(f, S, g, sn) ==
  [o \in Objs |-> IF o \in S /\ o \notin sn THEN Min(f[o], g) ELSE f[o]]

MinGen == IF \E o \in Objs : fr[o] = 0 THEN 0
          ELSE IF \E o \in Objs : fr[o] = 1 THEN 1 ELSE NoGen

FrontierEmpty == \A o \in Objs : fr[o] = NoGen

FreeIds == {i \in 1..3 : packs[BuildIds[i]] = "absent"}
Fresh   == BuildIds[CHOOSE i \in FreeIds : \A j \in FreeIds : i <= j]

(* the janitor's GRACE, as a hold: a request that is still in flight and
   planned reads from p *)
Held(p) ==
  \/ fetch.st = "planned" /\ p \in fetch.plan
  \/ push.st = "looked" /\ p \in push.base

(* ---- chain exits --------------------------------------------------- *)

(* gc.* and every scratch table gone: wipe_all's tail, pr5_sweep's and
   end_of_chain's DELETEs *)
ClearGc ==
  /\ phase' = "idle" /\ bar' = FALSE /\ cohort' = {} /\ marked' = {}
  /\ built' = {} /\ fr' = [o \in Objs |-> NoGen] /\ seen' = {}
  /\ pending' = {} /\ rounds' = 0 /\ newPack' = None /\ retire' = {}
  /\ walk' = NoWalk /\ moved' = FALSE

(* gc.rs wipe_all's row/pack effects on the state (st, rw): the gc.retire
   rows arm, then the keep rule on gc.new_pack. Keep the build pack iff it
   is live and the cohort is no longer intact -- it is the surviving copy;
   otherwise kill it. Bug abortDropsSurvivor always kills it. *)
WipeRows(st, rw) ==
  LET rw1  == [p \in Packs |-> IF p \in retire /\ st[p] # "live"
                               THEN {} ELSE rw[p]]
      cl   == cohort # {} /\ \A p \in cohort : st[p] = "live"
      keep == newPack # None /\ st[newPack] = "live" /\ ~cl
              /\ Bug # "abortDropsSurvivor"
  IN  IF newPack = None THEN rw1
      ELSE IF keep
           THEN [p \in Packs |-> IF p \in cohort /\ st[p] # "live"
                                 THEN {} ELSE rw1[p]]
           ELSE [rw1 EXCEPT ![newPack] = {}]
WipeState(st) ==
  LET cl   == cohort # {} /\ \A p \in cohort : st[p] = "live"
      keep == newPack # None /\ st[newPack] = "live" /\ ~cl
              /\ Bug # "abortDropsSurvivor"
  IN  IF newPack # None /\ ~keep /\ st[newPack] \in {"ingesting", "live"}
      THEN [st EXCEPT ![newPack] = "dead"] ELSE st

(* gc.rs wipe_all (and abort / compact.rs orphan_abort, which call it) *)
WipeAll ==
  /\ rows' = WipeRows(packs, rows)
  /\ packs' = WipeState(packs)
  /\ ClearGc

(* gc.rs wipe_path: rows of non-live cohort packs, then wipe_all, then
   end_of_chain's bookkeeping *)
WipePath ==
  LET rw0 == [p \in Packs |-> IF p \in cohort /\ ~Live(p) THEN {} ELSE rows[p]]
  IN  /\ rows' = WipeRows(packs, rw0)
      /\ packs' = WipeState(packs)
      /\ ClearGc

(* ================================================================== *)
(* push: repo_do/mod.rs                                                *)
(* ================================================================== *)

Shapes == CASE Scope = "chart"   -> {"thin"}
          [] World = "single" -> {"thin", "bare"}
          [] OTHER            -> {"bare"}

(* push_begin: the pushes row records the gc_epoch the push validates
   against. The shape and a bare push's tip are the client's choice. A
   later push begins only once the one before it has ended. *)
PushBegin ==
  /\ push.st \in {"idle", "committed", "rejected", "expired", "failed"}
  /\ npush < MaxPushes
  /\ npush' = npush + 1
  /\ \E sh \in Shapes, t \in BareTips :
       push' = [st |-> "begun", shape |-> sh,
                tip |-> IF sh = "thin" THEN "c2" ELSE t,
                epoch |-> epoch, base |-> {}, cas |-> FALSE]
  /\ UNCHANGED <<packs, rows, bytes, refs, moved, epoch, landed, fetch,
                 chains, errs, diedBy>> /\ UNCHANGED gcVars

(* push_lookup (separate sync request): the thin pack's bases c1 and b
   must be live; the edge then reads their bytes from the packs it found
   (pack/ingest.rs external bases). A miss fails the push. *)
PushLookup ==
  /\ push.st = "begun" /\ push.shape = "thin"
  /\ IF HasLive("c1") /\ HasLive("b")
     THEN \E p1 \in Holders("c1"), p2 \in Holders("b") :
            push' = [push EXCEPT !.st = "looked", !.base = {p1, p2}]
     ELSE push' = [push EXCEPT !.st = "failed"]
  /\ UNCHANGED <<packs, rows, bytes, refs, moved, epoch, npush, landed,
                 fetch, chains, errs, diedBy>> /\ UNCHANGED gcVars

(* ingest pass B plus push_index: the base reads complete, Q's bytes are
   durable in R2, its rows are posted and Q is 'ingesting'. push_index
   refuses a push that is no longer open. *)
PushUpload ==
  /\ push.st = "looked"
  /\ push' = [push EXCEPT !.st = "uploaded", !.base = {}]
  /\ packs' = [packs EXCEPT !["Q"] = "ingesting"]
  /\ rows' = [rows EXCEPT !["Q"] = {"c2"}]
  /\ bytes' = [bytes EXCEPT !["Q"] = TRUE]
  /\ UNCHANGED <<refs, moved, epoch, npush, landed, fetch, chains, errs, diedBy>>
  /\ UNCHANGED gcVars

(* commit_push: ONE synchronous span. Step 1 open check, step 2 gc_epoch
   check, barrier_active read, step 3 barrier_write_pending_before_promote
   then the promote, step 4 apply_one (tip-live re-guard, CAS, barrier
   landing feed and bump, ref write), step 5 bump. *)
PushCommit ==
  /\ \/ push.st = "uploaded" /\ push.shape = "thin"
     \/ push.st = "begun" /\ push.shape = "bare"
  /\ IF push.epoch # epoch /\ Bug # "noEpochCheck"
     THEN \* step 2: "gc ran during push, retry"
          /\ push' = [push EXCEPT !.st = "rejected"]
          /\ UNCHANGED <<packs, rows, bytes, refs, moved, epoch, npush,
                         landed, fetch, chains, errs, diedBy>> /\ UNCHANGED gcVars
     ELSE
       LET b       == BarrierActive
           thin    == push.shape = "thin"
           pend    == b /\ thin /\ Bug # "noBarrierPending"
           pk1     == IF thin THEN [packs EXCEPT !["Q"] = "live"] ELSE packs
           tip     == push.tip
           ref     == IF thin THEN "main" ELSE RefOf(tip)
           tipLive == \E p \in Packs : pk1[p] = "live" /\ tip \in rows[p]
       \* a thin push updates main and may lose the CAS to another
       \* writer; a bare push creates its ref, which lands iff it is absent
       IN \E cas \in (IF thin THEN {TRUE, FALSE} ELSE {refs[ref] = None}) :
          LET lands == tipLive /\ cas
              feed  == b /\ lands /\ Bug # "noLandingFeed"
          IN /\ packs' = pk1
             /\ pending' = IF pend THEN pending \cup {"Q"} ELSE pending
             /\ fr' = IF feed THEN Feed(fr, {tip}, 1, seen) ELSE fr
             /\ refs' = IF lands THEN [refs EXCEPT ![ref] = tip] ELSE refs
             /\ landed' = IF lands THEN [landed EXCEPT ![ref] = tip]
                          ELSE landed
             /\ moved' = (phase # "idle" /\ (moved \/ pend \/ lands \/ thin))
             /\ push' = [push EXCEPT !.st = "committed", !.cas = cas]
             /\ UNCHANGED <<rows, bytes, epoch, npush, fetch, chains, errs,
                            phase, bar, cohort, marked, built, seen, rounds,
                            newPack, retire, walk, diedBy>>

(* ================================================================== *)
(* fetch: pack/generate.rs                                             *)
(* ================================================================== *)

(* send_set walk plus plan_reads, one resolve against the live index:
   each wanted sha maps to one live row (the GROUP BY picks any). The
   code's one-time rebuild of a stale plan is this resolve happening late.
   A sha with no live row fails the fetch closed. *)
FetchPlan ==
  /\ fetch.st = "idle" /\ Scope = "full"
  /\ LET want == Closure[refs["main"]]
     IN  IF \A o \in want : HasLive(o)
         THEN \E pick \in [want -> Packs] :
                /\ \A o \in want : pick[o] \in Holders(o)
                /\ fetch' = [st |-> "planned",
                             plan |-> {pick[o] : o \in want}]
         ELSE fetch' = [st |-> "error", plan |-> {}]
  /\ UNCHANGED <<packs, rows, bytes, refs, moved, epoch, push, npush, landed,
                 chains, errs, diedBy>> /\ UNCHANGED gcVars

(* pack_chunk: the planned range reads, after the await *)
FetchRead ==
  /\ fetch.st = "planned"
  /\ fetch' = [st |-> "done", plan |-> {}]
  /\ UNCHANGED <<packs, rows, bytes, refs, moved, epoch, push, npush, landed,
                 chains, errs, diedBy>> /\ UNCHANGED gcVars

(* ================================================================== *)
(* janitor: jobs/janitor.rs run_slice                                  *)
(* ================================================================== *)

(* an open push past PUSH_TIMEOUT -> 'expired' *)
JanitorExpire ==
  /\ push.st \in Open /\ Scope = "full"
  /\ push' = [push EXCEPT !.st = "expired", !.base = {}]
  /\ UNCHANGED <<packs, rows, bytes, refs, moved, epoch, npush, landed,
                 fetch, chains, errs, diedBy>> /\ UNCHANGED gcVars

(* the ingesting pack of an expired or rejected push: rows, then dead *)
JanitorReap ==
  /\ packs["Q"] = "ingesting" /\ push.st \in {"expired", "rejected"}
  /\ packs' = [packs EXCEPT !["Q"] = "dead"]
  /\ rows' = [rows EXCEPT !["Q"] = {}]
  /\ UNCHANGED <<bytes, refs, moved, epoch, push, npush, landed, fetch,
                 chains, errs, diedBy>> /\ UNCHANGED gcVars

(* a dead pack with no object rows, past GRACE: delete its R2 key.
   Bug janitorNoGrace ignores the grace hold. *)
JanitorDelete(p) ==
  /\ packs[p] = "dead" /\ rows[p] = {} /\ bytes[p]
  /\ (~Held(p) \/ Bug = "janitorNoGrace")
  /\ bytes' = [bytes EXCEPT ![p] = FALSE]
  /\ UNCHANGED <<packs, rows, refs, moved, epoch, push, npush, landed, fetch,
                 chains, errs, diedBy>> /\ UNCHANGED gcVars

(* ================================================================== *)
(* GC mark: jobs/gc.rs gc_mark                                         *)
(* ================================================================== *)

(* gc_mark first slice: wipe, chain id, gc.refs_version capture, the
   candidate set (live and older than the cutoff), roots = ref targets,
   the young-pack seed (every non-blob of every live non-candidate pack),
   started_at, then gc.barrier. No candidate: wipe_all and stop. *)
GcMarkInit ==
  /\ phase = "idle" /\ chains < MaxChains
  /\ chains' = chains + 1
  /\ LET cand  == {p \in OldPacks : Live(p)}
         roots == {refs[r] : r \in RefNames} \ {None}
         young == UNION {rows[p] : p \in {q \in Packs \ cand : Live(q)}}
     IN  IF cand = {}
         THEN UNCHANGED <<phase, bar, cohort, marked, built, fr, seen,
                          pending, rounds, newPack, retire, walk, moved>>
         ELSE /\ phase' = "walking" /\ bar' = Barrier /\ cohort' = cand
              /\ marked' = {} /\ built' = {}
              /\ fr' = [o \in Objs |-> IF o \in roots \cup young
                                       THEN 0 ELSE NoGen]
              /\ seen' = {} /\ pending' = {} /\ rounds' = 0
              /\ newPack' = None /\ retire' = {} /\ walk' = NoWalk
              /\ moved' = FALSE
  /\ UNCHANGED <<packs, rows, bytes, refs, epoch, push, npush, landed, fetch, errs, diedBy>>

(* a mark slice, or a barrier sweep slice past retire_resume (which runs
   first on every sweep slice entry and ends the chain when an intent
   stands) *)
Walking == phase = "walking" \/ (phase = "sweep" /\ bar /\ retire = {})
Idle    == walk.sha = None

(* walk_step span A: pop one sha of the minimum generation (barrier) or
   any sha (legacy), look up its live rows; the bit plan sets the bit of
   the row the GROUP BY returns when that pack is a candidate. Barrier:
   a gen-1 pop counts a delta round. The R2 read of the row's bytes is the
   await that follows. Legacy re-checks refs_version before each pop. *)
GcPop ==
  /\ Walking /\ Idle /\ pending = {}
  /\ bar \/ ~moved
  /\ \E o \in Objs :
       /\ fr[o] # NoGen
       /\ (~bar \/ fr[o] = MinGen)
       /\ \E pk \in (IF HasLive(o) THEN Holders(o) ELSE {None}) :
            walk' = [sha |-> o, gen |-> IF bar THEN fr[o] ELSE 0, pk |-> pk]
       /\ rounds' = IF bar /\ fr[o] = 1 THEN Min(rounds + 1, DeltaCap)
                    ELSE rounds
  /\ UNCHANGED <<packs, rows, bytes, refs, moved, epoch, push, npush, landed, fetch,
                 phase, bar, cohort, marked, built, fr, seen, pending,
                 newPack, retire, chains, errs, diedBy>>

(* commit_ids (span B, after the read's await, behind phase_fence): bits
   first, then gc_seen, then the kids at the batch gen minus gc_seen,
   then the frontier delete. Barrier: a miss (no live row at pop) is not
   seen-recorded and leaves the frontier only while still absent. *)
GcCommit ==
  /\ walk.sha # None
  /\ LET o    == walk.sha
         hit  == walk.pk # None
         sn   == IF bar /\ ~hit THEN seen ELSE seen \cup {o}
         f1   == Feed(fr, IF hit THEN Links[o] ELSE {}, walk.gen, sn)
         drop == hit \/ ~bar \/ ~HasLive(o)
     IN  /\ marked' = IF hit /\ walk.pk \in cohort THEN marked \cup {o}
                      ELSE marked
         /\ seen' = sn
         /\ fr' = IF drop THEN [f1 EXCEPT ![o] = NoGen] ELSE f1
         /\ walk' = NoWalk
  /\ UNCHANGED <<packs, rows, bytes, refs, moved, epoch, push, npush, landed, fetch,
                 phase, bar, cohort, built, pending, rounds, newPack, retire,
                 chains, errs, diedBy>>

(* pending_page (barrier, before the pop): drop rows whose pack is not
   live, then feed the oldest live row's non-blobs at gen 1 (minus
   gc_seen) and delete the row; one delta round either way *)
GcPage ==
  /\ Walking /\ bar /\ Idle /\ pending # {}
  /\ LET lp == {p \in pending : Live(p)}
     IN  /\ fr' = IF lp = {} THEN fr
                  ELSE Feed(fr, UNION {rows[p] : p \in lp}, 1, seen)
         /\ pending' = {}
  /\ rounds' = Min(rounds + 1, DeltaCap)
  /\ UNCHANGED <<packs, rows, bytes, refs, moved, epoch, push, npush, landed, fetch,
                 phase, bar, cohort, marked, built, seen, newPack, retire,
                 walk, chains, errs, diedBy>>

(* barrier_handoff_if_ready: no gen-0 row left, and either frontier and
   gc_pending are both empty, or the delta cap is hit while refs moved *)
GcHandoff ==
  /\ phase = "walking" /\ bar /\ Idle
  /\ MinGen # 0
  /\ \/ FrontierEmpty /\ pending = {}
     \/ rounds >= DeltaCap /\ moved
  /\ phase' = "build"
  /\ UNCHANGED <<packs, rows, bytes, refs, moved, epoch, push, npush, landed, fetch,
                 bar, cohort, marked, built, fr, seen, pending, rounds,
                 newPack, retire, walk, chains, errs, diedBy>>

(* legacy walk: Step::Empty with refs unmoved -> gc_seen wiped, build *)
GcHandoffLegacy ==
  /\ phase = "walking" /\ ~bar /\ Idle /\ ~moved /\ FrontierEmpty
  /\ phase' = "build" /\ seen' = {}
  /\ UNCHANGED <<packs, rows, bytes, refs, moved, epoch, push, npush, landed, fetch,
                 bar, cohort, marked, built, fr, pending, rounds, newPack,
                 retire, walk, chains, errs, diedBy>>

(* legacy abort, every refs_moved site except pr5_sweep's own: the walk
   loop (before a pop), begin_build, and a gc_mark slice that finds the
   chain at build/sweep with refs_frozen_moved. abort = wipe_all plus a
   fresh mark. *)
GcAbortLegacy ==
  /\ phase \in {"walking", "build", "sweep"} /\ ~bar /\ Idle /\ moved
  /\ WipeAll
  /\ UNCHANGED <<bytes, refs, epoch, push, npush, landed, fetch, chains, errs, diedBy>>

(* compact.rs orphaned + orphan_abort, reached from gc_mark's M4 arm: a
   barrier chain at build/sweep whose owner row died (any Err that
   exhausted its retries). wipe_all with the keep rule; no slice runs. *)
GcOrphanAbort ==
  /\ phase \in {"build", "sweep"} /\ bar /\ Idle /\ Scope = "full"
  /\ WipeAll
  /\ UNCHANGED <<bytes, refs, epoch, push, npush, landed, fetch, chains, errs, diedBy>>

(* ================================================================== *)
(* GC build: gc_consolidate                                            *)
(* ================================================================== *)

(* begin_build: barrier checks cohort_live and freezes built = bitmap.
   (Legacy's refs_moved abort is GcAbortLegacy.) Nothing marked: straight
   to sweep with no build pack. One fully marked candidate: wipe_all.
   Otherwise a fresh build pack is 'ingesting' and gc.new_pack names it. *)
GcBeginBuild ==
  /\ phase = "build" /\ newPack = None
  /\ bar \/ ~moved
  /\ IF bar /\ ~CohortLive
     THEN WipeAll
     ELSE IF marked = {}
     THEN /\ phase' = "sweep"
          /\ built' = IF bar THEN marked ELSE built
          /\ UNCHANGED <<packs, rows, bar, cohort, marked, fr, seen,
                         pending, rounds, newPack, retire, walk, moved>>
     ELSE IF ~(Cardinality(cohort) >= 2 \/
               \E p \in cohort : ~(rows[p] \subseteq marked))
     THEN WipeAll
     ELSE /\ FreeIds # {}
          /\ packs' = [packs EXCEPT ![Fresh] = "ingesting"]
          /\ newPack' = Fresh
          /\ built' = IF bar THEN marked ELSE built
          /\ UNCHANGED <<rows, phase, bar, cohort, marked, fr, seen,
                         pending, rounds, retire, walk, moved>>
  /\ UNCHANGED <<bytes, refs, epoch, push, npush, landed, fetch, chains, errs, diedBy>>

(* build's last flush: the multipart upload completes (finish_pack, or
   mpu_err's HEAD hit), the copied rows are posted, then finish_build:
   chain_fence, the build pack goes live, gc_sweep is enqueued, phase
   sweep. Copied set: built (barrier) or the bitmap (legacy). *)
GcBuildFinish ==
  /\ phase = "build" /\ newPack # None /\ packs[newPack] = "ingesting"
  /\ packs' = [packs EXCEPT ![newPack] = "live"]
  /\ rows' = [rows EXCEPT ![newPack] = IF bar THEN built ELSE marked]
  /\ bytes' = [bytes EXCEPT ![newPack] = TRUE]
  /\ phase' = "sweep"
  /\ UNCHANGED <<refs, moved, epoch, push, npush, landed, fetch, bar, cohort,
                 marked, built, fr, seen, pending, rounds, newPack, retire,
                 walk, chains, errs, diedBy>>

(* mpu_err -> Flow::Rebuild -> wipe_build, then begin_build again in the
   same span: the dead upload's ingesting pack is dead-marked (never
   live, no bytes) and a fresh one starts. Bound: one id always stays
   free for a later chain's begin_build, so at most one rebuild a run. *)
GcRebuild ==
  /\ phase = "build" /\ newPack # None /\ packs[newPack] = "ingesting"
  /\ Cardinality(FreeIds) >= 2 /\ Scope = "full"
  /\ packs' = [packs EXCEPT ![newPack] = "dead", ![Fresh] = "ingesting"]
  /\ rows' = [rows EXCEPT ![newPack] = {}]
  /\ newPack' = Fresh
  /\ UNCHANGED <<bytes, refs, moved, epoch, push, npush, landed, fetch, phase, bar,
                 cohort, marked, built, fr, seen, pending, rounds, retire,
                 walk, chains, errs, diedBy>>

(* ================================================================== *)
(* GC sweep                                                            *)
(* ================================================================== *)

(* pr5_sweep (legacy chain): refs_moved -> abort; else gc_epoch bump,
   dead-mark the cohort, delete its rows, wipe gc.* and scratch. Bug
   sweepIgnoresRefsVersion skips the abort. An Err at
   gc.sweep.after_deadmark keeps the bump and the dead-mark; the row
   stays at phase sweep and retries. *)
GcSweepLegacy ==
  /\ phase = "sweep" /\ ~bar /\ Idle
  /\ IF moved /\ Bug # "sweepIgnoresRefsVersion"
     THEN WipeAll /\ UNCHANGED <<bytes, refs, epoch, push, npush, landed, fetch,
                                 chains, errs, diedBy>>
     ELSE LET pk1 == [p \in Packs |-> IF p \in cohort THEN "dead"
                                      ELSE packs[p]]
          IN  /\ epoch' = epoch + 1
              /\ packs' = pk1
              /\ \/ /\ rows' = [p \in Packs |-> IF p \in cohort THEN {}
                                                ELSE rows[p]]
                    /\ ClearGc
                    /\ UNCHANGED errs
                 \/ /\ errs < MaxErrs /\ errs' = errs + 1
                    /\ UNCHANGED <<rows, phase, bar, cohort, marked, built,
                                   fr, seen, pending, rounds, newPack,
                                   retire, walk, moved>>
              /\ diedBy' = [p \in DOMAIN diedBy |->
                              IF p \in cohort THEN "pr5" ELSE diedBy[p]]
              /\ UNCHANGED <<bytes, refs, push, npush, landed, fetch, chains>>

(* barrier gc_sweep: swap_check finds frontier and gc_pending empty and
   runs fenced_swap -> retire_span -> retire_finish -> end_of_chain in the
   same span. cohort_live / build pack live guards -> wipe_path; P is
   retirable iff bitmap is a subset of built; lossy iff the build missed
   entries, which bumps gc_epoch before any destructive write. An Err at
   gc.swap.after_deadmark keeps the intent, the bump and the dead-mark. *)
GcSwap ==
  /\ phase = "sweep" /\ bar /\ Idle /\ retire = {}
  /\ FrontierEmpty /\ {p \in pending : Live(p)} = {}
  /\ IF ~CohortLive \/ (newPack # None /\ ~Live(newPack))
     THEN WipePath /\ UNCHANGED <<bytes, refs, epoch, push, npush, landed, fetch,
                                  chains, errs, diedBy>>
     ELSE
       LET ids   == {p \in cohort : marked \cap rows[p] \subseteq built}
           lossy == \E p \in ids : ~(rows[p] \subseteq built)
           pk1   == [p \in Packs |-> IF p \in ids THEN "dead" ELSE packs[p]]
       IN  IF ids = {}
           THEN WipePath /\ UNCHANGED <<bytes, refs, epoch, push, npush, landed,
                                        fetch, chains, errs, diedBy>>
           ELSE /\ epoch' = IF lossy THEN epoch + 1 ELSE epoch
                /\ packs' = pk1
                /\ \/ /\ rows' = [p \in Packs |-> IF p \in ids THEN {}
                                                  ELSE rows[p]]
                      /\ ClearGc
                      /\ UNCHANGED errs
                   \/ /\ errs < MaxErrs /\ errs' = errs + 1
                      /\ retire' = ids
                      /\ pending' = {}
                      /\ UNCHANGED <<rows, phase, bar, cohort, marked,
                                     built, fr, seen, rounds, newPack,
                                     walk, moved>>
                /\ diedBy' = [p \in DOMAIN diedBy |->
                                IF p \in ids THEN "swap" ELSE diedBy[p]]
                /\ UNCHANGED <<bytes, refs, push, npush, landed, fetch, chains>>

(* swap_check on a non-empty drain once tries or drain slices run out:
   fallback_span. P is retirable iff every entry with no live copy outside
   the cohort was built; the net-positive byte rule is a free choice;
   lossy is always FALSE, so no epoch bump. *)
GcFallback ==
  /\ phase = "sweep" /\ bar /\ Idle /\ retire = {}
  /\ ~(FrontierEmpty /\ {p \in pending : Live(p)} = {})
  /\ LET outside(o) == \E q \in Packs \ cohort : Live(q) /\ o \in rows[q]
         \* Bug fallbackUsesBitmap reads the live bitmap in place of the
         \* built snapshot: bm_all_set(&r.bitmap, ..) for bm_all_set(&r.built, ..)
         safe == IF Bug = "fallbackUsesBitmap" THEN marked ELSE built
         ids == {p \in cohort :
                   \A o \in rows[p] : outside(o) \/ o \in safe}
     IN  IF ~CohortLive \/ (newPack # None /\ ~Live(newPack)) \/ ids = {}
         THEN WipePath /\ UNCHANGED diedBy
         ELSE \/ WipePath /\ UNCHANGED diedBy      \* net-positive fails
              \/ /\ packs' = [p \in Packs |-> IF p \in ids THEN "dead"
                                               ELSE packs[p]]
                 /\ rows' = [p \in Packs |-> IF p \in ids THEN {}
                                             ELSE rows[p]]
                 /\ ClearGc
                 /\ diedBy' = [p \in DOMAIN diedBy |->
                                IF p \in ids THEN "fallback" ELSE diedBy[p]]
  /\ UNCHANGED <<bytes, refs, epoch, push, npush, landed, fetch, chains, errs>>

(* retire_resume on the next sweep slice: the recorded ids are all dead
   (L == 0), so retire_finish re-runs steps 8-11 *)
GcRetireResume ==
  /\ phase = "sweep" /\ bar /\ Idle /\ retire # {}
  /\ \A p \in retire : ~Live(p)
  /\ rows' = [p \in Packs |-> IF p \in retire THEN {} ELSE rows[p]]
  /\ ClearGc
  /\ UNCHANGED <<packs, bytes, refs, epoch, push, npush, landed, fetch, chains, errs, diedBy>>

(* ================================================================== *)
(* spec                                                                *)
(* ================================================================== *)

Init ==
  /\ packs = [p \in Packs |-> IF p \in OldPacks THEN "live" ELSE "absent"]
  /\ rows = [p \in Packs |-> CASE p = "P0" -> P0Init
                             [] p = "P1" -> P1Init
                             [] OTHER    -> {}]
  /\ bytes = [p \in Packs |-> p \in OldPacks]
  /\ refs = [r \in RefNames |-> IF r = "main" THEN "c1" ELSE None]
  /\ moved = FALSE /\ epoch = 0
  /\ push = [st |-> "idle", shape |-> None, tip |-> None, epoch |-> 0,
             base |-> {}, cas |-> FALSE]
  /\ npush = 0
  /\ landed = [r \in RefNames |-> None]
  /\ fetch = [st |-> "idle", plan |-> {}]
  /\ phase = "idle" /\ bar = FALSE /\ cohort = {} /\ marked = {}
  /\ built = {} /\ fr = [o \in Objs |-> NoGen] /\ seen = {}
  /\ pending = {} /\ rounds = 0 /\ newPack = None /\ retire = {}
  /\ walk = NoWalk /\ chains = 0 /\ errs = 0
  /\ diedBy = [p \in {"P0", "P1"} |-> "none"]

JanitorWork ==
  \/ push.st \in Open
  \/ packs["Q"] = "ingesting" /\ push.st \in {"expired", "rejected"}
  \/ \E p \in Packs : packs[p] = "dead" /\ rows[p] = {} /\ bytes[p]

(* every actor finished: lets TLC tell a real deadlock from the end *)
Quiesced ==
  /\ push.st \in {"committed", "rejected", "expired", "failed"}
  /\ npush = MaxPushes
  /\ (fetch.st \in {"done", "error"} \/ Scope = "chart")
  /\ phase = "idle" /\ chains = MaxChains
  /\ ~JanitorWork

Done == Quiesced /\ UNCHANGED vars

Next ==
  \/ PushBegin \/ PushLookup \/ PushUpload \/ PushCommit
  \/ FetchPlan \/ FetchRead
  \/ JanitorExpire \/ JanitorReap \/ \E p \in Packs : JanitorDelete(p)
  \/ GcMarkInit \/ GcPop \/ GcCommit \/ GcPage \/ GcHandoff
  \/ GcHandoffLegacy \/ GcAbortLegacy \/ GcOrphanAbort
  \/ GcBeginBuild \/ GcBuildFinish \/ GcRebuild
  \/ GcSweepLegacy \/ GcSwap \/ GcFallback \/ GcRetireResume
  \/ Done

Spec == Init /\ [][Next]_vars

(* ================================================================== *)
(* invariants                                                          *)
(* ================================================================== *)

(* TypeOK: every variable stays inside its declared domain. *)
TypeOK ==
  /\ packs \in [Packs -> PState]
  /\ rows \in [Packs -> SUBSET Objs]
  /\ bytes \in [Packs -> BOOLEAN]
  /\ refs \in [RefNames -> Objs \cup {None}]
  /\ moved \in BOOLEAN /\ epoch \in 0..4
  /\ push.st \in PushSt /\ push.shape \in {None, "thin", "bare"}
  /\ push.tip \in Objs \cup {None} /\ npush \in 0..MaxPushes
  /\ push.epoch \in 0..4 /\ push.base \subseteq Packs
  /\ push.cas \in BOOLEAN
  /\ landed \in [RefNames -> Objs \cup {None}]
  /\ fetch.st \in {"idle", "planned", "done", "error"}
  /\ fetch.plan \subseteq Packs
  /\ phase \in Phases /\ bar \in BOOLEAN /\ cohort \subseteq OldPacks
  /\ marked \subseteq Objs /\ built \subseteq Objs
  /\ fr \in [Objs -> {0, 1, NoGen}] /\ seen \subseteq Objs
  /\ pending \subseteq Packs /\ rounds \in 0..DeltaCap
  /\ newPack \in {None, "N1", "N2", "N3"} /\ retire \subseteq Packs
  /\ walk.sha \in Objs \cup {None} /\ walk.gen \in {0, 1}
  /\ walk.pk \in Packs \cup {None}
  /\ chains \in 0..MaxChains /\ errs \in 0..MaxErrs
  /\ diedBy \in [{"P0", "P1"} -> {"none", "pr5", "swap", "fallback"}]

(* RefsResolvable: every object reachable from any ref has an objects row
   in a live pack whose R2 bytes exist. *)
RefsResolvable ==
  \A r \in RefNames : refs[r] # None => ClosureOk(refs[r])

(* LiveBytesPresent: no live pack ever lacks its R2 bytes. *)
LiveBytesPresent ==
  \A p \in Packs : Live(p) => bytes[p]

(* CommittedPushResolvable: a ref the server reported as landed by a
   committed push resolves completely for as long as it still points
   there. (landed is written only by a committing push.) *)
CommittedPushResolvable ==
  \A r \in RefNames :
    (landed[r] # None /\ refs[r] = landed[r]) => ClosureOk(landed[r])

(* YoungPacksClosed: every object in a live pack written during the run
   (a pushed pack, a loser's pack, a build pack) resolves completely. GC
   treats young packs as roots (gc_mark's young-pack seed, the pending
   page) because a later push may build on any object they hold; this is
   the U-subset-of-P oracle of testhooks mark_check. *)
YoungPacksClosed ==
  \A p \in Packs \ OldPacks : Live(p) => \A o \in rows[p] : ClosureOk(o)

(* LiveClosed: CONTRACTS.md 2.5, the connectivity rule. Every object in a
   live pack links only to objects that resolve: a live row with R2
   bytes. Ingest and apply_one check only that a link target or a tip is
   live, and rely on this rule for everything behind it. *)
LiveClosed ==
  \A p \in Packs : Live(p) =>
    \A o \in rows[p] : \A x \in Links[o] : Resolves(x)

(* FetchReadsSucceed: every pack an in-flight fetch planned to read still
   has its R2 bytes (GRACE > request lifetime). *)
FetchReadsSucceed ==
  fetch.st = "planned" => \A p \in fetch.plan : bytes[p]

(* PushBaseReadsSucceed: every pack an in-flight push's lookup resolved a
   thin-pack base to still has its R2 bytes (GRACE > request lifetime). *)
PushBaseReadsSucceed ==
  push.st = "looked" => \A p \in push.base : bytes[p]

=============================================================================
