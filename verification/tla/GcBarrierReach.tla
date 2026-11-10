-------------------------- MODULE GcBarrierReach --------------------------
(***************************************************************************)
(* Vacuity guards for GcBarrier. Each Reach_* is the negation of a state   *)
(* the clean model must be able to reach, so verify.sh expects TLC to      *)
(* report each one as violated. If a model edit makes a hazard            *)
(* unreachable, the clean checks would still pass; these fail instead.     *)
(***************************************************************************)
EXTENDS GcBarrier

(* the thin push landed main -> c2, and a later sweep still retired P0:
   c1 and b were copied into a build pack first *)
Reach_LandThenRetire ==
  ~(refs["main"] = "c2" /\ packs["P0"] = "dead" /\ phase = "idle")

(* a non-atomic push lost its CAS, its pack went live, and P0 was still
   retired safely afterwards *)
Reach_LoserThenRetire ==
  ~(push.st = "committed" /\ ~push.cas /\ packs["P0"] = "dead")

(* barrier drain: b was marked after the built snapshot, so the swap
   must keep P0 *)
Reach_DrainMarksNew ==
  ~(phase = "sweep" /\ "b" \in marked /\ "b" \notin built)

(* the bare push landed g -> b while a chain stood *)
Reach_BareLandsMidChain ==
  ~(refs["g"] = "b" /\ phase \in {"walking", "build", "sweep"})

(* a sweep hit an Err after its dead-mark, and the chain still ended with
   a live build pack as the surviving copy *)
Reach_ErrThenSurvivor ==
  ~(errs = 1 /\ phase = "idle" /\ packs["P0"] = "dead"
    /\ \E n \in {"N1", "N2", "N3"} : packs[n] = "live")

(* the gc_epoch check rejected the push *)
Reach_EpochReject ==
  ~(push.st = "rejected")

(* an in-flight fetch planned a pack that has since died *)
Reach_FetchOutlivesPack ==
  ~(fetch.st = "planned" /\ \E p \in PlanPacks(fetch.picks) : packs[p] = "dead")

(* plan_reads found a walked pack's rows gone and rebuilt the plan *)
Reach_FetchRebuild ==
  ~(fetch.st = "planned" /\ fetch.rb)

(* plan_reads kept a pack that was already dead but still had its rows:
   the Err-after-dead-mark window *)
Reach_PlanNamesDead ==
  ~(fetch.st = "planned" /\ ~fetch.rb
    /\ \E p \in PlanPacks(fetch.picks) : packs[p] = "dead" /\ rows[p] # {})

(* the barrier delta arm handed off to build with the frontier not empty *)
Reach_DeltaHandoff ==
  ~(phase = "build" /\ ~FrontierEmpty)

(* split world, the schedule of the partial-retirement finding
   under the fixed fenced_swap: a ref landed at c during the chain, the
   drain marked c in P1 after the built snapshot, so the swap kept P1 and
   held the lossy retirable P0 live; the chain ended with no gc_epoch bump *)
Reach_LossyHeldBack ==
  ~("P0" \in held /\ refs["gc"] = "c" /\ packs["P0"] = "live"
    /\ packs["P1"] = "live" /\ epoch = 0 /\ phase = "idle")

(* kept world: fallback_span retired P0 with no gc_epoch bump *)
Reach_FallbackRetire ==
  ~(packs["P0"] = "dead" /\ diedBy["P0"] = "fallback" /\ epoch = 0)

(* kept world: fenced_swap retired P0 losslessly, with no gc_epoch bump *)
Reach_NonLossySwap ==
  ~(packs["P0"] = "dead" /\ diedBy["P0"] = "swap" /\ epoch = 0)

(* fenced_swap step 6's partial arm: a swap retired one candidate
   losslessly while another candidate stays live, with no gc_epoch bump.
   The live candidate was in that swap's cohort and was kept, so whole was
   FALSE and ids was not empty *)
Reach_PartialLosslessSwap ==
  ~(\E p, q \in OldPacks : p # q /\ packs[p] = "dead" /\ diedBy[p] = "swap"
      /\ packs[q] = "live" /\ epoch = 0)

(* kept world: a thin push's lookup resolved a base to P0, and P0 has
   since retired with no gc_epoch bump, so step 2 will not reject the
   push *)
Reach_ThinOverLosslessRetire ==
  ~(push.st = "looked" /\ push.shape = "thin" /\ "P0" \in push.base
    /\ packs["P0"] = "dead" /\ epoch = push.epoch)

=============================================================================
