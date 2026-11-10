"""PR6 phase A — RED tests for the G4 fenced swap (.pr6-design.md §6).

Every test encodes the POST-phase-B contract: a barrier-mode chain reaching
phase=sweep runs the drain loop + fenced swap of §3 instead of today's
refs_version abort. On current code the sweep still fences on refs_version,
`gc_last_swap`/`gc_fallbacks`/`gc.retire`/`gc.swap`/`marked.built` do not
exist and the `gc.drain.*`/`gc.swap.*` failpoints have no host sites — so
most assertions fail today. The flag-off test (§6.8) and parts of the crash
matrix pin behaviour that must NOT change.

Barrier on unless stated. Every test is self-contained (own repo), runs
under GE_TEST_MANUAL_JOBS=1 (deterministic stepping; sleeps only inside the
pause poll, matching test_barrier/test_gc_safety).
"""

import os
import sys
import threading
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ge
import test_harness as th
import test_gc_safety as tgs
import test_barrier as tb

OWNER = th.OWNER + "-swp"
ZERO = "0" * 40


def repo(name):
    return ge.Repo("%s/%s" % (OWNER, name))


def cutoff_now(r, extra_ms=1000):
    """Every pack created so far is a GC candidate."""
    ge.knob(r.name, "gc_candidate_before_ms", int(time.time() * 1000) + extra_ms)


def packs_by_id(r):
    return {p["id"]: p for p in ge.jobs(r.name)["packs"]}


def cohort_ids(r):
    """Pack ids in the candidate bitmap (packs.marked flag in the jobs view)."""
    return {p["id"] for p in ge.jobs(r.name)["packs"] if p["marked"]}


def live_ids(r):
    return {p["id"] for p in tgs.live_packs(r)}


def orphan_commit(r, tag, parent=None):
    """A commit on a detached line (not on local main): blob+tree+commit."""
    b = ge.git("hash-object", "-w", "--stdin", cwd=r.dir,
               input="%s\n" % tag).stdout.strip()
    t = tgs.mktree(r, [("100644", "blob", b, tag)])
    return tgs.commit_tree(r, t, [] if parent is None else [parent], tag)


def push_garbage(r, tag="g", parent=None):
    """Orphan-line commit pushed to a branch then deleted — its whole pack
    is unreachable. Returns (commit_sha, pack_id)."""
    c = orphan_commit(r, tag, parent)
    before = live_ids(r)
    ge.git("push", "-q", r.url, "%s:refs/heads/%s" % (c, tag), cwd=r.dir)
    pack = next(p["id"] for p in tgs.live_packs(r) if p["id"] not in before)
    ge.git("push", "-q", r.url, ":refs/heads/%s" % tag, cwd=r.dir)
    return c, pack


def push_commit(r, tag):
    """A new commit on local main, pushed."""
    c = tb.new_commit(r, tag)
    r.push()
    return c


def mark_to_build(r, limit=40):
    """Step gc_mark until gc.phase=build publishes. Keyed on the persisted
    phase, never on a row's outcome: a push staked while a mark slice was
    parked leaves a queued gc_mark row behind, and once build stands that
    row only Reschedules at the M4 guard — outcome-keyed stepping could
    spin on it forever."""
    for _ in range(limit):
        if tb.meta(r).get("gc.phase") == "build":
            return
        ge.step(r.name, "gc_mark")
    raise AssertionError("mark never reached phase=build: %r" % tb.meta(r))


def finish_chain(r, limit=80):
    """Step ONLY the live chain's owner until gc.phase is gone — never the
    queued gc_mark a mid-chain push leaves behind. tgs.gc_drain steps that
    mark next, and under GE_GC_QUIET_MS=0 it runs a WHOLE SECOND chain
    whose fenced swap overwrites gc_last_swap, resets gc_fallbacks and
    retires more packs — so every swap/epoch/pack assertion on THIS chain
    must read state observed between finish_chain and any later drain.
    The owner choice mirrors gc_drain's (build -> gc_consolidate, sweep ->
    gc_sweep); a phase standing with no owner queued is a setup bug, never
    a reason to step another kind."""
    for _ in range(limit):
        v = ge.jobs(r.name)
        phase = (v.get("gc_meta") or {}).get("gc.phase")
        if phase is None:
            return
        owner = {"build": "gc_consolidate", "sweep": "gc_sweep"}.get(phase)
        queued = {j["kind"] for j in v["jobs"] if j["state"] == "queued"}
        if owner is None or owner not in queued:
            raise AssertionError(
                "gc.phase=%r but %s not queued: %r" % (phase, owner, v["jobs"]))
        ge.step(r.name, owner)
    raise AssertionError(
        "chain never ended in %d steps: %r" % (limit, tb.meta(r)))


def pause_drain(r):
    """Start a gc_sweep slice parked at gc.drain.after_read; (thread, box).
    Requires drain work queued (non-empty gc_frontier/gc_pending) or the
    slice swaps before the drain iterates."""
    ge.failpoint(r.name, "gc.drain.after_read", "pause", 1)
    box = {}

    def bg():
        try:
            box["s"] = ge.step(r.name, "gc_sweep")
        except Exception as e:
            box["err"] = e

    t = threading.Thread(target=bg, daemon=True)
    t.start()
    deadline = time.time() + 15
    while time.time() < deadline:
        v = ge.jobs(r.name)
        armed = any(
            f["name"] == "gc.drain.after_read" and f["action"] == "pause"
            for f in v.get("failpoints", [])
        )
        consumed = any(
            f["name"] == "gc.drain.after_read" and f["remaining"] <= 0
            for f in v.get("failpoints", [])
        )
        running = any(
            j["kind"] == "gc_sweep" and j["state"] == "running" for j in v["jobs"]
        )
        if (consumed or not armed) and running:
            return t, box
        time.sleep(0.1)
    raise AssertionError("drain slice never entered the pause")


def release_drain(r, t, box):
    ge.failpoint(r.name, "gc.drain.after_read", "off")
    t.join(45)
    if box.get("err") is not None:
        raise box["err"]
    return box.get("s")


def import_pack_of(r, shas):
    """The staged-import pack body: exactly the listed objects."""
    return ge.pack_objects(r.dir, shas)


def epoch(r):
    """gc_epoch as an int (0 while the meta row does not exist yet)."""
    return int(tb.meta(r).get("gc_epoch") or 0)


def assert_dead_rows_zero(tc, r):
    tc.assertEqual(
        ge.live_objects(r.name)["dead_rows"], 0,
        "dead-pack object rows leaked: %r" % ge.live_objects(r.name))


def assert_clean(tc, r):
    """Server fsck all + a fresh clone's fsck --strict."""
    f = ge.fsck(r.name, "all")
    tc.assertTrue(f["ok"], "%r" % f)
    clone = r.clone()
    ge.git("fsck", "--strict", cwd=clone)


@unittest.skipUnless(os.environ.get("GE_HOOKS_OFF") != "1", "needs the hooks-on server")
class Swap(unittest.TestCase):
    # -- §6.1: the repro — sweep completes under a writer -------------------

    def test_sweep_completes_under_writer(self):
        """§6.1: push before every gc_sweep step. Today the sweep fences on
        refs_version and aborts the chain; the swap must drain the pushed
        work and still land: gc_last_swap{fallback:false}, garbage pack
        retired, gc_epoch +1 (the cohort is lossy), fsck all + clone ok.

        Expected red: `ge.last_swap` is None — no fenced swap exists yet."""
        r = repo("sweep-writer")
        tb.barrier_on(r)
        tb.seed(r, 2)
        _, g_pack = push_garbage(r, "g")
        cutoff_now(r)
        tb.to_stage(r, "presweep")
        cohort = cohort_ids(r)  # the marked flag wipes with the chain
        epoch0 = epoch(r)
        gone = False
        for i in range(40):
            push_commit(r, "w%d" % i)
            s = ge.step(r.name, "gc_sweep")
            v = ge.jobs(r.name)
            if not any(j["kind"] == "gc_sweep" and j["state"] in ("queued", "running")
                       for j in v["jobs"]):
                gone = True
                break
        self.assertTrue(gone, "gc_sweep never finished under a writer: %r" % s)
        swap = ge.last_swap(r.name)
        self.assertIsNotNone(swap, "no gc_last_swap — the sweep never swapped")
        self.assertFalse(swap["fallback"], "%r" % swap)
        self.assertGreaterEqual(swap["retired"], 1, "%r" % swap)
        self.assertTrue(swap["lossy"], "garbage cohort swapped lossless: %r" % swap)
        self.assertEqual(epoch(r), epoch0 + 1,
                         "lossy swap did not bump gc_epoch")
        packs = packs_by_id(r)
        self.assertTrue(all(packs[c]["state"] == "dead" for c in cohort),
                        "a cohort pack survived the swap: %r"
                        % [packs[c] for c in cohort])
        self.assertEqual(packs[g_pack]["state"], "dead",
                         "garbage pack survived the swap: %r" % packs[g_pack])
        assert_dead_rows_zero(self, r)
        assert_clean(self, r)

    # -- §6.2: C11 subset — a resurrected cohort pack stays live ------------

    def test_subset_keeps_resurrected_pack(self):
        """§6.2 (C11): after the build, push a ref to a commit that was
        garbage at the snapshot and lives ONLY in cohort pack P. The drain
        marks it → bitmap⊄built → P stays live while other packs retire;
        the object still reads (fresh clone of that ref, fsck --strict).
        Mutant: a subset check against live `bitmap` (not `built`) fails.

        Expected red: today the push's moved refs abort the chain — and a
        later quiesced chain retires P outright (or its marked objects only
        in N), so `packs[g_pack]` is dead / the pack does not stay live."""
        r = repo("subset-res")
        tb.barrier_on(r)
        tb.seed(r, 2)
        g, g_pack = push_garbage(r, "g")
        cutoff_now(r)
        tb.to_stage(r, "presweep")
        # resurrect the garbage commit — an empty-pack ref create whose new
        # oid is only reachable inside the (unbuilt) cohort pack
        st, body = ge.receive_pack(
            r.name, [(ZERO, g, "refs/heads/res")], ge.empty_pack())
        self.assertIn(b"ok refs/heads/res", body,
                      "%r" % ge.redact(body[:300].decode("utf-8", "replace")))
        # finish only THIS chain — the res push's queued gc_mark would run a
        # second chain under a full drain, retiring g_pack legitimately once
        # its marks are copied into that chain's own build pack
        finish_chain(r, limit=60)
        packs = packs_by_id(r)
        self.assertEqual(packs[g_pack]["state"], "live",
                         "resurrected pack was retired: %r" % packs[g_pack])
        swap = ge.last_swap(r.name)
        self.assertIsNotNone(swap, "no swap recorded")
        self.assertTrue(any(p["state"] == "dead" for p in packs.values()),
                        "no other cohort pack retired: %r" % packs)
        clone = r.clone("-res")
        self.assertEqual(
            ge.git("rev-parse", "refs/remotes/origin/res", cwd=clone).stdout.strip(), g)
        ge.git("fsck", "--strict", cwd=clone)
        tgs.gc_drain(r, limit=120)
        assert_clean(self, r)

    def test_partial_retire_keeps_live_closure(self):
        """§2.5 across a partial swap: cohort pack P1 holds A (parent B) and
        C, cohort pack P0 holds B; all three are garbage at the snapshot.
        After the build a ref to C lands, so P1 is kept, and P0 is lossy.
        A swap that still retires P0 leaves A in a live pack pointing at a
        dead B: a later ref-only create of A passes the tip check and
        publishes a broken history. Every live pack must stay closed, so the
        ref to A must either be refused or read back whole."""
        r = repo("partial-closure")
        tb.barrier_on(r)
        tb.seed(r, 2)
        b = orphan_commit(r, "b")
        before = live_ids(r)
        ge.git("push", "-q", r.url, "%s:refs/heads/b0" % b, cwd=r.dir)
        p0 = next(p["id"] for p in tgs.live_packs(r) if p["id"] not in before)
        a = orphan_commit(r, "a", parent=b)
        c = orphan_commit(r, "c")
        before = live_ids(r)
        # b0 is still advertised, so this pack carries A without B
        ge.git("push", "-q", r.url, "%s:refs/heads/a0" % a,
               "%s:refs/heads/c0" % c, cwd=r.dir)
        p1 = next(p["id"] for p in tgs.live_packs(r) if p["id"] not in before)
        ge.git("push", "-q", r.url, ":refs/heads/a0", ":refs/heads/b0",
               ":refs/heads/c0", cwd=r.dir)
        cutoff_now(r)
        tb.to_stage(r, "presweep")
        st, body = ge.receive_pack(
            r.name, [(ZERO, c, "refs/heads/c")], ge.empty_pack())
        self.assertIn(b"ok refs/heads/c", body,
                      "%r" % ge.redact(body[:300].decode("utf-8", "replace")))
        epoch0 = epoch(r)
        finish_chain(r, limit=60)
        self.assertEqual(packs_by_id(r)[p1]["state"], "live",
                         "resurrected pack was retired: %r" % packs_by_id(r)[p1])
        # B's lossy pack is held back while P1 stays, and nothing left the
        # live set, so pushes in flight stay valid
        self.assertEqual(packs_by_id(r)[p0]["state"], "live",
                         "lossy pack retired beside a kept one: %r" % packs_by_id(r)[p0])
        self.assertFalse(ge.last_swap(r.name)["lossy"], "%r" % ge.last_swap(r.name))
        self.assertEqual(epoch(r), epoch0, "a swap that dropped nothing moved gc_epoch")
        f = ge.fsck(r.name, "all")
        self.assertTrue(f["ok"], "a live pack references a dead object: %r" % f)
        st, body = ge.receive_pack(
            r.name, [(ZERO, a, "refs/heads/a")], ge.empty_pack())
        if b"ok refs/heads/a" in body:
            clone = r.clone("-a")
            self.assertEqual(ge.git("rev-parse", "refs/remotes/origin/a",
                                    cwd=clone).stdout.strip(), a)
            ge.git("fsck", "--strict", cwd=clone)
        tgs.gc_drain(r, limit=120)
        assert_clean(self, r)

    # -- §6.3: lossless keeps the epoch, lossy bumps it; in-flight push -----

    def test_lossless_swap_keeps_epoch(self):
        """§6.3: an all-reachable cohort retires with gc_epoch UNCHANGED, so
        an in-flight push (staged before the chain) commits after the swap —
        the spec's "in-flight pushes stay valid on a lossless swap".

        The in-flight push is a staged import (the same pushes-row vehicle
        test_gc_safety's C1 tests use): its gc_epoch is pinned at stage time
        and its commit lands after the swap.

        Expected red: no swap exists — today's quiesced sweep retires the
        whole cohort AND bumps the epoch, so `gc_last_swap` is absent and
        `gc_epoch` moved."""
        r = repo("epoch-lossless")
        tb.barrier_on(r)
        tip = tb.seed(r, 3)
        cutoff_now(r)
        # in-flight push: commits C on top of tip after the swap
        c = tb.new_commit(r, "inflight")
        listing = ge.git("rev-list", "--objects", c, "--not", tip, cwd=r.dir).stdout
        st = ge.import_stage(r.name, ge.pack_objects(r.dir, listing))
        body = ge.import_begin(
            r.name, st["push"],
            [{"key": st["key"], "bytes": st["bytes"]}],
            [{"old": tip, "new": c, "name": "refs/heads/main"}])
        self.assertIn(b"queued", body)
        tgs.import_until_commit_phase(r)
        epoch0 = epoch(r)
        tb.to_stage(r, "presweep")
        finish_chain(r, limit=60)
        swap = ge.last_swap(r.name)
        self.assertIsNotNone(swap, "no gc_last_swap")
        self.assertFalse(swap["lossy"], "all-reachable cohort swapped lossy: %r" % swap)
        self.assertEqual(epoch(r), epoch0,
                         "lossless swap moved gc_epoch")
        tgs.step_until(r, "import_pack", {"done"})
        imp = ge.import_status(r.name, st["push"])
        self.assertEqual(imp["push_state"], "committed", "%r" % imp)
        assert_clean(self, r)

    def test_lossy_swap_bumps_epoch(self):
        """§6.3: a cohort with garbage → gc_epoch +1, and an in-flight push
        that began before the swap is refused — the staged import's commit
        links to a commit only in the retired garbage pack, so its re-check
        reports the parent missing and rejects (the spec's "gc ran during
        push" contract made deterministic).

        Expected red: `gc_last_swap` is absent — today the quiesced sweep
        retires the cohort without recording a swap."""
        r = repo("epoch-lossy")
        tb.barrier_on(r)
        tip = tb.seed(r, 2)
        g, g_pack = push_garbage(r, "g")
        cutoff_now(r)
        # in-flight push: C1 links to g, which lives only in the garbage pack
        b = ge.git("hash-object", "-w", "--stdin", cwd=r.dir,
                   input="c1\n").stdout.strip()
        t = tgs.mktree(r, [("100644", "blob", b, "c1")])
        c1 = tgs.commit_tree(r, t, [g], "c1")
        st = ge.import_stage(r.name, import_pack_of(r, [c1, t, b]))
        body = ge.import_begin(
            r.name, st["push"],
            [{"key": st["key"], "bytes": st["bytes"]}],
            [{"old": ZERO, "new": c1, "name": "refs/heads/c1"}])
        self.assertIn(b"queued", body)
        tgs.import_until_commit_phase(r)
        epoch0 = epoch(r)
        tb.to_stage(r, "presweep")
        finish_chain(r, limit=60)
        swap = ge.last_swap(r.name)
        self.assertIsNotNone(swap, "no gc_last_swap")
        self.assertTrue(swap["lossy"], "garbage cohort swapped lossy: %r" % swap)
        self.assertEqual(epoch(r), epoch0 + 1,
                         "lossy swap did not bump gc_epoch")
        tgs.step_until(r, "import_pack", {"done"})
        imp = ge.import_status(r.name, st["push"])
        self.assertEqual(imp["push_state"], "rejected",
                         "stale-epoch push landed after a lossy swap: %r" % imp)
        assert_clean(self, r)

    # -- §6.4: forced fallback after K tries — both branches ----------------

    def test_forced_fallback_after_k_tries(self):
        """§6.4 + §3.3: swap_tries=1; pause the drain at gc.drain.after_read,
        push while parked, release → the moved refs_version charges one try
        → the fallback span runs: gc_last_swap.fallback=true, gc_epoch
        UNCHANGED (every retired entry has a live copy — lossless by
        construction), gc_fallbacks incremented, fsck all ok.

        (a) retire branch: fully-built/duplicated cohort, retirable bytes
            >= bytes(N) → cohort packs dead, N live.
        (b) wipe branch: a resurrected garbage object keeps one pack
            non-retirable, retirable bytes < bytes(N) → N dead, cohort live.
        (c) swap_max_slices=1 + slice_subreqs=1 — the slice backstop fires.

        Expected red: `gc.drain.after_read` has no host site — pause_drain
        times out today."""
        for arm in ("retire", "wipe", "tiny_slices"):
            with self.subTest(arm=arm):
                r = repo("fb-%s" % arm)
                tb.barrier_on(r)
                if arm == "wipe":
                    # mixed cohort: pack1 {one+two} (two unbuilt at mark),
                    # pack2 {three} fully built
                    r.commit_file("f", "one\n", "one")
                    two = r.commit_file("f", "two\n", "two")
                    r.push()
                    ge.git("reset", "--hard", "HEAD~1", cwd=r.dir)
                    r.commit_file("f", "three\n", "three")
                    r.push(force=True)
                else:
                    tb.seed(r, 2)
                cutoff_now(r)
                tb.to_stage(r, "presweep")
                cohort = cohort_ids(r)
                new_pack = tb.meta(r).get("gc.new_pack")
                epoch0 = epoch(r)
                ge.knob(r.name, "swap_tries", 1)
                if arm == "tiny_slices":
                    ge.knob(r.name, "swap_max_slices", 1)
                    ge.knob(r.name, "slice_subreqs", 1)
                # drain work so the pause point is reached
                push_commit(r, "pre")
                t, box = pause_drain(r)
                if arm == "wipe":
                    # resurrect an unbuilt object inside pack1 mid-drain
                    st, body = ge.receive_pack(
                        r.name, [(ZERO, two, "refs/heads/res")], ge.empty_pack())
                    self.assertIn(b"ok refs/heads/res", body,
                                  "%r" % ge.redact(body[:300].decode("utf-8", "replace")))
                else:
                    push_commit(r, "during")
                release_drain(r, t, box)
                # observe THIS chain's fallback before the queued mark the
                # mid-drain pushes left runs a second (quiesced) chain —
                # its swap would overwrite gc_last_swap and gc_fallbacks
                finish_chain(r, limit=80)
                swap = ge.last_swap(r.name)
                self.assertIsNotNone(swap, "no gc_last_swap")
                self.assertTrue(swap["fallback"], "not a fallback: %r" % swap)
                self.assertEqual(epoch(r), epoch0,
                                 "fallback bumped gc_epoch")
                self.assertGreaterEqual(ge.fallbacks(r.name), 1,
                                        "gc_fallbacks not incremented")
                packs = packs_by_id(r)
                if arm == "wipe":
                    self.assertEqual(packs[new_pack]["state"], "dead",
                                     "wipe branch kept N: %r" % packs[new_pack])
                    self.assertTrue(all(
                        packs[c]["state"] == "live" for c in cohort),
                        "wipe branch retired a cohort pack: %r" % packs)
                else:
                    self.assertEqual(packs[new_pack]["state"], "live",
                                     "retire branch killed N: %r" % packs[new_pack])
                    self.assertTrue(any(
                        packs[c]["state"] == "dead" for c in cohort),
                        "retire branch retired nothing: %r" % packs)
                tgs.gc_drain(r, limit=120)
                assert_clean(self, r)

    # -- §6.5: a stable drain never counts tries -----------------------------

    def test_stable_large_drain_never_falls_back(self):
        """§6.5 + §3.3 baseline: writes land BEFORE phase=sweep publishes the
        try baseline (mark hands off at the delta-round cap with gen-1
        leftover); after that the writer quiesces. The drain is large but
        refs_version never moves → zero tries → the swap lands even with
        swap_tries=1.

        Expected red: `gc_last_swap` absent today."""
        r = repo("stable-drain")
        tb.barrier_on(r)
        tb.seed(r, 2)
        cutoff_now(r)
        ge.knob(r.name, "mark_delta_rounds", 1)  # cap forces leftover gen-1
        # the writes must land inside a live mark window — an uncapped init
        # step on a tiny seed can hand the whole mark off to build first,
        # and a mark queued after handoff only Reschedules at the M4 build
        # guard (same pattern as tb.test_mark_completes_under_writer)
        t, box = tb.pause_mark(r)
        for i in range(5):
            push_commit(r, "pre%d" % i)  # pending rows + gen-1, all pre-baseline
        tb.release_pause(r, t, box)
        mark_to_build(r)
        tb.step_until(r, "gc_consolidate", {"done"})
        ge.knob(r.name, "swap_tries", 1)
        # the pushes staked under the parked mark left a rescheduled gc_mark
        # queued — finish this chain only, then observe its swap
        finish_chain(r, limit=80)
        swap = ge.last_swap(r.name)
        self.assertIsNotNone(swap, "no gc_last_swap")
        self.assertFalse(swap["fallback"], "stable drain fell back: %r" % swap)
        tgs.gc_drain(r, limit=120)
        assert_clean(self, r)

    # -- §6.17: the try baseline counts only post-sweep writes ---------------

    def test_try_baseline_after_presweep_writes(self):
        """§6.17: presweep writes are inside the baseline rv — with
        swap_tries=1 a quiesced drain must still swap, not fall back.
        A DELETE-ONLY push during a paused drain bumps refs_version and
        charges a try; with no drain work the next check is empty so a
        normal (non-fallback) swap lands — assert tries>=1, allow
        fallback=false.

        Expected red: no `gc_last_swap`; the paused-drain site is missing."""
        for mode in ("quiesce", "delete_during_drain"):
            with self.subTest(mode=mode):
                r = repo("try-%s" % mode)
                tb.barrier_on(r)
                tb.seed(r, 2)
                victim = orphan_commit(r, "victim")
                ge.git("push", "-q", r.url,
                       "%s:refs/heads/del" % victim, cwd=r.dir)
                cutoff_now(r)
                ge.knob(r.name, "mark_delta_rounds", 1)
                # same live-window requirement — the writes must land while
                # the mark is mid-flight, or the mark they queue stalls at
                # the M4 build guard and the chain never reaches phase=sweep
                t, box = tb.pause_mark(r)
                push_commit(r, "pre")
                if mode == "delete_during_drain":
                    # pre-baseline on purpose: drain work inside the try
                    # baseline — a post-baseline push would charge the try
                    # before gc.drain.after_read could ever park the slice
                    push_commit(r, "pre2")
                tb.release_pause(r, t, box)
                mark_to_build(r)
                tb.step_until(r, "gc_consolidate", {"done"})
                ge.knob(r.name, "swap_tries", 1)
                if mode == "delete_during_drain":
                    t, box = pause_drain(r)
                    ge.git("push", "-q", r.url, ":refs/heads/del", cwd=r.dir)
                    release_drain(r, t, box)
                    # the delete queued a gc_mark — observe this chain's
                    # try/fallback before any second chain can overwrite it
                    finish_chain(r, limit=80)
                    swap = ge.last_swap(r.name)
                    self.assertIsNotNone(swap, "no gc_last_swap")
                    # delete charged a try (refs_version moved) but added no
                    # drain work, so the next check is empty and a normal
                    # fenced swap lands (fallback=false). Spec orders the
                    # emptiness check before the K-try trigger.
                    self.assertGreaterEqual(
                        swap["tries"], 1,
                        "a delete-only push did not charge a try: %r" % swap)
                else:
                    finish_chain(r, limit=80)
                    swap = ge.last_swap(r.name)
                    self.assertIsNotNone(swap, "no gc_last_swap")
                    self.assertFalse(
                        swap["fallback"],
                        "presweep writes were charged as tries: %r" % swap)
                tgs.gc_drain(r, limit=120)
                assert_clean(self, r)

    # -- §6.6: crash/error injection at every point --------------------------

    CRASH_CASES = [
        # (name, failpoint, action, drive-to-stage, extra setup)
        ("mark-read", "gc.mark.after_read", "crash", "start", None),
        ("mark-bits", "gc.mark.commit_ids.after_bits", "error", "start", None),
        ("mark-seen", "gc.mark.commit_ids.after_seen", "error", "start", None),
        ("mark-kids", "gc.mark.commit_ids.after_kids", "error", "start", None),
        ("cons-begin", "gc.consolidate.begin", "crash", "markdone", None),
        ("cons-part", "gc.consolidate.after_part", "crash", "markdone", "parts"),
        ("cons-finish", "gc.consolidate.before_finish", "crash", "markdone", None),
        ("drain-read", "gc.drain.after_read", "crash", "presweep", "push"),
        ("swap-epoch", "gc.swap.after_epoch", "error", "presweep", None),
        ("swap-deadmark", "gc.swap.after_deadmark", "error", "presweep", None),
        ("swap-fb-deadmark", "gc.swap.fallback.after_deadmark",
         "error", "presweep", "fallback"),
    ]

    def test_crash_points(self):
        """§6.6: every named injection point — mark read, the three
        commit_ids points, consolidate begin/part/finish, the drain's
        after_read, the swap's after_epoch/after_deadmark and the fallback's
        after_deadmark. After repair + resume: fsck all ok, dead_rows==0,
        a fresh clone passes fsck --strict.

        Expected red: the gc.drain.*/gc.swap.* sites don't exist — their
        arms step straight to "done" instead of "retry"/"crashed"."""
        for name, fp, action, stage, extra in self.CRASH_CASES:
            with self.subTest(case=name):
                r = repo("crash-%s" % name)
                tb.barrier_on(r)
                if extra == "parts":
                    # after_part only exists past the first uploaded part —
                    # a single-part seed never reaches it. The
                    # consolidate_parts recipe (test_harness._failpoint_case):
                    # part size at the dev-R2 non-final floor plus ~6 MiB of
                    # incompressible binary, committed before the seed pushes
                    # so it lands inside a candidate pack
                    ge.knob(r.name, "gc_part_bytes", 5 << 20)
                    with open(os.path.join(r.dir, "big"), "wb") as fh:
                        fh.write(os.urandom(6 << 20))
                    ge.git("add", "big", cwd=r.dir)
                    ge.git("commit", "-qm", "big", cwd=r.dir)
                    th.seed_gc_repo(r)
                elif extra == "fallback":
                    # all-reachable cohort so the forced fallback takes the
                    # RETIRE branch (a garbage cohort would wipe N instead —
                    # the failpoint sits in the fallback's retire span)
                    tb.seed(r, 2)
                else:
                    th.seed_gc_repo(r)
                cutoff_now(r)
                if stage == "markdone":
                    tb.step_until(r, "gc_mark", {"done"})
                elif stage == "presweep":
                    tb.to_stage(r, "presweep")
                if extra == "fallback":
                    ge.knob(r.name, "swap_tries", 1)
                    push_commit(r, "fb")  # moved rv + drain work → fallback
                elif extra == "push":
                    push_commit(r, "pre")  # drain work so after_read fires
                kind = {"start": "gc_mark", "markdone": "gc_consolidate",
                        "presweep": "gc_sweep"}[stage]
                ge.failpoint(r.name, fp, action, 1)
                s = ge.step(r.name, kind)
                if action == "crash":
                    self.assertEqual(
                        s["outcome"], "crashed",
                        "%s never fired: %r" % (fp, s))
                    ge.repair(r.name)
                else:
                    self.assertEqual(
                        s["outcome"], "retry",
                        "%s never fired: %r" % (fp, s))
                ge.failpoint(r.name, fp, "off")
                tgs.gc_drain(r, limit=120)
                assert_dead_rows_zero(self, r)
                assert_clean(self, r)

    # -- §6.7: a stale drain slice writes nothing ----------------------------

    def test_stale_drain_slice_writes_nothing(self):
        """§6.7: park one drain slice at gc.drain.after_read, repair-requeue
        the row, let a FRESH sweep slice finish the swap, then release the
        parked one → its write spans fail the phase/lease fence: gc table
        digests and pack states identical, exactly one retirement.

        Expected red: `gc.drain.after_read` doesn't exist → the pause
        never parks."""
        r = repo("stale-drain")
        tb.barrier_on(r)
        th.seed_gc_repo(r)
        cutoff_now(r)
        tb.to_stage(r, "presweep")
        push_commit(r, "pre")  # drain work so the pause is inside a slice
        t, box = pause_drain(r)
        ge.repair(r.name)  # requeue the stranded row → a fresh slice owns it
        tb.step_until(r, "gc_sweep", {"done"})
        swap = ge.last_swap(r.name)
        self.assertIsNotNone(swap, "no swap after the requeue")
        retired0 = swap["retired"]
        t0 = ge.jobs(r.name)["gc_tables"]
        p0 = ge.jobs(r.name)["packs"]
        release_drain(r, t, box)
        t1 = ge.jobs(r.name)["gc_tables"]
        p1 = ge.jobs(r.name)["packs"]
        self.assertEqual(t0, t1, "stale drain slice wrote: %r -> %r" % (t0, t1))
        self.assertEqual(p0, p1, "stale drain slice changed packs")
        self.assertEqual(ge.last_swap(r.name)["retired"], retired0,
                         "a second retirement ran")
        assert_clean(self, r)

    # -- §6.8: flag-off invariance ---------------------------------------------

    def test_flag_off_sweep_unchanged(self):
        """§6.8: no gc_barrier knob — a push before the sweep aborts the
        chain exactly as before: gc_last_swap absent, gc_epoch unchanged,
        cohort live, the build pack dead-marked (the unchanged keep rule).
        PASSES today — it pins the flag-off contract."""
        r = repo("flag-off")
        th.seed_gc_repo(r)
        cutoff_now(r)
        tb.step_until(r, "gc_mark", {"done"})
        tb.step_until(r, "gc_consolidate", {"done"})
        cohort = cohort_ids(r)  # captured now — marked wipes with the chain
        epoch0 = epoch(r)
        push_commit(r, "mv")
        s = ge.step(r.name, "gc_sweep")
        self.assertTrue(s["ran"], "%r" % s)
        m = tb.meta(r)
        self.assertNotIn("gc.started_at", m,
                         "flag-off chain survived moved refs: %r" % m)
        self.assertIsNone(ge.last_swap(r.name),
                          "a flag-off swap was recorded")
        self.assertEqual(epoch(r), epoch0,
                         "the abort moved gc_epoch")
        packs = packs_by_id(r)
        self.assertTrue(all(packs[c]["state"] == "live" for c in cohort),
                        "cohort pack retired on the flag-off abort: %r" % packs)
        tgs.gc_drain(r, limit=120)
        tb.assert_pack_ok(self, r)

    # -- §6.11: deploy transition — built IS NULL takes the PR5 path ---------

    def test_deploy_built_null_takes_pr5_path(self):
        """§6.11 / §2: a barrier chain at phase=sweep whose cohort rows have
        marked.built IS NULL is a pre-PR6 chain — its sweep takes the
        unchanged PR5 path: refs moved ⇒ abort; stable ⇒ whole-cohort
        retirement. No drain, no subset check, gc_last_swap absent either
        way.

        Expected red: `marked.built` doesn't exist yet — the marked_built
        hook's UPDATE errors (that error IS the red signal)."""
        for moved in (True, False):
            with self.subTest(refs_moved=moved):
                r = repo("deploy-%s" % ("moved" if moved else "stable"))
                tb.barrier_on(r)
                th.seed_gc_repo(r)
                cutoff_now(r)
                tb.to_stage(r, "presweep")
                cohort = cohort_ids(r)  # marked wipes with the chain end
                ge.marked_built_null(r.name)  # 500s until the column ships
                epoch0 = epoch(r)
                if moved:
                    push_commit(r, "mv")
                s = ge.step(r.name, "gc_sweep")
                self.assertTrue(s["ran"], "%r" % s)
                self.assertIsNone(
                    ge.last_swap(r.name),
                    "a built-null chain recorded a swap")
                m = tb.meta(r)
                self.assertNotIn("gc.started_at", m,
                                 "chain still live after the sweep: %r" % m)
                packs = packs_by_id(r)
                if moved:
                    self.assertEqual(epoch(r), epoch0,
                                     "the abort moved gc_epoch")
                    self.assertTrue(
                        all(packs[c]["state"] == "live" for c in cohort),
                        "moved-refs arm retired the cohort: %r" % packs)
                else:
                    self.assertEqual(epoch(r), epoch0 + 1,
                                     "PR5 whole-cohort arm kept the epoch")
                    self.assertTrue(
                        any(packs[c]["state"] == "dead" for c in cohort),
                        "stable arm retired nothing: %r" % packs)
                tgs.gc_drain(r, limit=120)
                tb.assert_pack_ok(self, r)

    # -- §6.12: total==0 arm × after_deadmark + the orphan arm ---------------

    def test_total0_resume_after_deadmark_error(self):
        """§6.12 / §3.2 8a: a cohort that marks NOTHING (total==0 → no build
        pack) dead-marks its retire set, then dies at gc.swap.after_deadmark;
        the retry resumes from gc.retire and completes: dead_rows==0.
        Orphan arm: instead of retrying, the sweep row is killed and the
        next gc_mark runs the M4 orphan arm (8b) → dead_rows==0.

        Expected red: `gc.swap.after_deadmark` doesn't exist — the sweep
        finishes "done" instead of "retry"."""
        for orphan in (False, True):
            with self.subTest(orphan_abort=orphan):
                r = repo("total0-%s" % ("orph" if orphan else "retry"))
                tb.barrier_on(r)
                # garbage pack strictly older than the seed packs: the only
                # candidate, zero marks → total==0 (same fixture as
                # tb.test_keep_total0_handoff_order). On an empty repo the
                # g push is the adopted HEAD — re-point it before deleting.
                g = orphan_commit(r, "g")
                packs0 = {p["id"] for p in ge.jobs(r.name)["packs"]}
                ge.git("push", "-q", r.url, "%s:refs/heads/g" % g, cwd=r.dir)
                ge.meta(r.name, "head", "refs/heads/main")
                ge.git("push", "-q", r.url, ":refs/heads/g", cwd=r.dir)
                garbage = next(
                    p["id"] for p in ge.jobs(r.name)["packs"]
                    if p["id"] not in packs0)
                time.sleep(1.1)
                tb.seed(r, 2)
                created = next(p["created_at"] for p in ge.jobs(r.name)["packs"]
                               if p["id"] == garbage)
                ge.knob(r.name, "gc_candidate_before_ms", created + 1)
                tb.to_stage(r, "presweep")
                ge.failpoint(r.name, "gc.swap.after_deadmark", "error", 1)
                s = ge.step(r.name, "gc_sweep")
                self.assertEqual(s["outcome"], "retry",
                                 "gc.swap.after_deadmark never fired: %r" % s)
                if orphan:
                    ge.failpoint(r.name, "gc.swap.after_deadmark", "off")
                    # strand the owner; the next mark runs the orphan arm
                    ge.meta(r.name, "resurrected.gc_sweep", "3")
                    ge.kill_job(r.name, "gc_sweep")
                    ge.enqueue(r.name, "gc_mark")
                    s = ge.step(r.name, "gc_mark")
                    self.assertTrue(s["ran"], "%r" % s)
                else:
                    ge.failpoint(r.name, "gc.swap.after_deadmark", "off")
                    tb.step_until(r, "gc_sweep", {"done"})
                assert_dead_rows_zero(self, r)
                tgs.gc_drain(r, limit=120)
                tb.assert_pack_ok(self, r)

    # -- §6.12b: after_epoch error → the retry re-validates -------------------

    def test_after_epoch_error_revalidates(self):
        """§6.12b / §3.2 8a first branch: lossy cohort; the error lands AFTER
        gc.retire + the epoch bump but BEFORE the dead-mark — every recorded
        id is still live on retry, so the resume re-runs the full swap. A
        push in between resurrects a commit that lives only in a retire pack
        → the re-validation marks it, the pack is NOT retired, the ref
        clones and fsck --strict passes.

        Expected red: `gc.swap.after_epoch` doesn't exist — the first sweep
        step reports "done", not "retry"."""
        r = repo("epoch-revalid")
        tb.barrier_on(r)
        tb.seed(r, 2)
        g, g_pack = push_garbage(r, "g")
        cutoff_now(r)
        tb.to_stage(r, "presweep")
        ge.failpoint(r.name, "gc.swap.after_epoch", "error", 1)
        s = ge.step(r.name, "gc_sweep")
        self.assertEqual(s["outcome"], "retry",
                         "gc.swap.after_epoch never fired: %r" % s)
        # between the error and the retry: resurrect the garbage commit —
        # its only home is a recorded retire id, still live → re-validated
        st, body = ge.receive_pack(
            r.name, [(ZERO, g, "refs/heads/res")], ge.empty_pack())
        self.assertIn(b"ok refs/heads/res", body,
                      "%r" % ge.redact(body[:300].decode("utf-8", "replace")))
        tb.step_until(r, "gc_sweep", {"done"})
        packs = packs_by_id(r)
        self.assertEqual(packs[g_pack]["state"], "live",
                         "post-error resurrected pack was retired: %r"
                         % packs[g_pack])
        clone = r.clone("-res")
        self.assertEqual(
            ge.git("rev-parse", "refs/remotes/origin/res", cwd=clone).stdout.strip(), g)
        ge.git("fsck", "--strict", cwd=clone)
        assert_clean(self, r)

    # -- §6.13: a push between the dead-mark error and the retry --------------

    def test_push_between_error_and_retry(self):
        """§6.13: the error lands after the retire-set dead-mark; a push
        lands before the retry; the resume still completes. Across a
        lossless retirement an in-flight push that began before the swap
        commits; across a lossy one it is rejected at its epoch check.

        Expected red: `gc.swap.after_deadmark` doesn't exist — the first
        sweep reports "done", not "retry"."""
        for arm in ("lossless", "lossy"):
            with self.subTest(arm=arm):
                r = repo("midretry-%s" % arm)
                tb.barrier_on(r)
                tip = tb.seed(r, 2)
                g = None
                if arm == "lossy":
                    g, _ = push_garbage(r, "g")
                cutoff_now(r)
                # an in-flight push spanning the whole swap (epoch pinned)
                if arm == "lossless":
                    c = tb.new_commit(r, "inflight")
                    listing = ge.git("rev-list", "--objects", c, "--not", tip,
                                     cwd=r.dir).stdout
                    pack = ge.pack_objects(r.dir, listing)
                    cmds = [{"old": tip, "new": c, "name": "refs/heads/main"}]
                else:
                    b = ge.git("hash-object", "-w", "--stdin", cwd=r.dir,
                               input="c1\n").stdout.strip()
                    t = tgs.mktree(r, [("100644", "blob", b, "c1")])
                    c = tgs.commit_tree(r, t, [g], "c1")
                    pack = import_pack_of(r, [c, t, b])
                    cmds = [{"old": ZERO, "new": c, "name": "refs/heads/c1"}]
                st = ge.import_stage(r.name, pack)
                body = ge.import_begin(
                    r.name, st["push"],
                    [{"key": st["key"], "bytes": st["bytes"]}], cmds)
                self.assertIn(b"queued", body)
                tgs.import_until_commit_phase(r)
                tb.to_stage(r, "presweep")
                ge.failpoint(r.name, "gc.swap.after_deadmark", "error", 1)
                s = ge.step(r.name, "gc_sweep")
                self.assertEqual(s["outcome"], "retry",
                                 "gc.swap.after_deadmark never fired: %r" % s)
                # a push lands between the error and the retry — on a side
                # branch: pushing local main would move refs/heads/main
                # before the import's own tip->c CAS lands, failing it with
                # "failed to update ref" rather than the epoch rule under
                # test. The commit is built on tip so local main never
                # moves (it already points at the import's c / the seed tip)
                mb = ge.git("hash-object", "-w", "--stdin", cwd=r.dir,
                            input="mid\n").stdout.strip()
                mt = tgs.mktree(r, [("100644", "blob", mb, "mid")])
                mid = tgs.commit_tree(r, mt, [tip], "mid")
                ge.git("push", "-q", r.url,
                       "%s:refs/heads/mid" % mid, cwd=r.dir)
                ge.failpoint(r.name, "gc.swap.after_deadmark", "off")
                tb.step_until(r, "gc_sweep", {"done"})
                tgs.step_until(r, "import_pack", {"done"})
                imp = ge.import_status(r.name, st["push"])
                if arm == "lossless":
                    self.assertEqual(imp["push_state"], "committed", "%r" % imp)
                else:
                    self.assertEqual(imp["push_state"], "rejected", "%r" % imp)
                assert_clean(self, r)

    # -- §3.3/§6.18: repeated fallbacks stay net-positive ---------------------

    def test_repeated_fallback_retires_duplicates(self):
        """§3.3 + §6.18: two forced-fallback cycles (swap_tries=1 + a push
        during a paused drain each time; the candidate cutoff moves forward
        so the first build pack joins cycle 2's cohort). The older build
        pack — a cohort pack whose entries all have a live non-cohort copy —
        is retired by the second fallback; each cycle's live bytes
        (excluding the test's own writer packs) stay <= the pre-cycle
        total — the net-positive rule; gc_fallbacks reaches 2 and a later
        fenced swap resets it to 0. every_pack_garbage arm: wipe keeps
        the same live-bytes bound when duplicates can't cover bytes(N).

        Expected red: `gc.drain.after_read` has no host site."""
        for garbage in (False, True):
            with self.subTest(every_pack_garbage=garbage):
                r = repo("fbcyc-%d" % garbage)
                tb.barrier_on(r)
                if garbage:
                    # two mixed packs: {one+two} then {three+g2}
                    r.commit_file("f", "one\n", "one")
                    r.commit_file("f", "two\n", "two")
                    r.push()
                    ge.git("reset", "--hard", "HEAD~1", cwd=r.dir)
                    r.commit_file("f", "three\n", "three")
                    g2 = orphan_commit(r, "g2")
                    ge.git("push", "-qf", r.url, "HEAD:refs/heads/main",
                           "%s:refs/heads/g2" % g2, cwd=r.dir)
                    ge.git("push", "-q", r.url, ":refs/heads/g2", cwd=r.dir)
                else:
                    tb.seed(r, 2)
                cutoff_now(r)
                new_pack1 = None
                writers = set()  # packs the test's own pre/mid pushes add
                for cycle in (1, 2):
                    # live bytes before this cycle's build — the net-positive
                    # baseline; the test's writer packs are excluded below
                    bytes_before = sum(p["bytes"] for p in tgs.live_packs(r))
                    tb.to_stage(r, "presweep")
                    if cycle == 1:
                        new_pack1 = tb.meta(r).get("gc.new_pack")
                    ge.knob(r.name, "swap_tries", 1)
                    before = live_ids(r)
                    push_commit(r, "pre%d" % cycle)  # drain work for the pause
                    writers |= (live_ids(r) - before)
                    t, box = pause_drain(r)
                    before = live_ids(r)
                    push_commit(r, "mid%d" % cycle)
                    writers |= (live_ids(r) - before)
                    release_drain(r, t, box)
                    # observe THIS cycle's fallback before the queued mark
                    # the mid-drain pushes left starts the NEXT cycle early —
                    # a full drain would run it past the swap assert below
                    finish_chain(r, limit=80)
                    swap = ge.last_swap(r.name)
                    self.assertIsNotNone(swap, "cycle %d: no gc_last_swap" % cycle)
                    self.assertTrue(swap["fallback"],
                                    "cycle %d did not fall back: %r" % (cycle, swap))
                    # exclude the test's writer packs — they are not cohort —
                    # so the live-bytes bound checks the GC effect alone
                    live_gc = [p for p in tgs.live_packs(r)
                               if p["id"] not in writers]
                    self.assertLessEqual(
                        sum(p["bytes"] for p in live_gc), bytes_before,
                        "live bytes grew across fallback cycle %d: %d -> %d"
                        % (cycle, bytes_before,
                           sum(p["bytes"] for p in live_gc)))
                    # the next cohort spans every pack including the build
                    # pack this cycle left live
                    ge.knob(r.name, "gc_candidate_before_ms",
                            int(time.time() * 1000) + 1000)
                self.assertEqual(ge.fallbacks(r.name), 2,
                                 "gc_fallbacks != cycles")
                packs = packs_by_id(r)
                if new_pack1 is not None:
                    self.assertEqual(packs[new_pack1]["state"], "dead",
                                     "cycle 2 kept the older duplicate: %r"
                                     % packs[new_pack1])
                # a later quiesced chain swaps — the counter resets
                tb.to_stage(r, "presweep")
                finish_chain(r, limit=80)
                swap = ge.last_swap(r.name)
                self.assertIsNotNone(swap, "no post-fallback swap")
                self.assertFalse(swap["fallback"], "%r" % swap)
                self.assertEqual(ge.fallbacks(r.name), 0,
                                 "a fenced swap did not reset gc_fallbacks")
                assert_clean(self, r)

    # -- §6.14: an in-flight import across the swap -----------------------------

    def test_inflight_import_across_swap(self):
        """§6.14: an import is staged and checked BEFORE the swap (epoch
        pinned at check time) and commits after it. Lossless ⇒ the epoch
        held, it commits. Lossy ⇒ the epoch moved — its re-check finds the
        swept parent missing and rejects; fsck all ok either way.

        Expected red: `gc_last_swap` absent — no swap is recorded today."""
        for arm in ("lossless", "lossy"):
            with self.subTest(arm=arm):
                r = repo("import-%s" % arm)
                tb.barrier_on(r)
                tip = tb.seed(r, 2)
                g = None
                if arm == "lossy":
                    g, _ = push_garbage(r, "g")
                cutoff_now(r)
                if arm == "lossless":
                    c = tb.new_commit(r, "inflight")
                    listing = ge.git("rev-list", "--objects", c, "--not", tip,
                                     cwd=r.dir).stdout
                    pack = ge.pack_objects(r.dir, listing)
                    cmds = [{"old": tip, "new": c, "name": "refs/heads/main"}]
                else:
                    b = ge.git("hash-object", "-w", "--stdin", cwd=r.dir,
                               input="c1\n").stdout.strip()
                    t = tgs.mktree(r, [("100644", "blob", b, "c1")])
                    c = tgs.commit_tree(r, t, [g], "c1")
                    pack = import_pack_of(r, [c, t, b])
                    cmds = [{"old": ZERO, "new": c, "name": "refs/heads/c1"}]
                st = ge.import_stage(r.name, pack)
                body = ge.import_begin(
                    r.name, st["push"],
                    [{"key": st["key"], "bytes": st["bytes"]}], cmds)
                self.assertIn(b"queued", body)
                tgs.import_until_commit_phase(r)  # checked; commit still queued
                tb.to_stage(r, "presweep")
                finish_chain(r, limit=60)  # the swap lands mid-flight
                self.assertIsNotNone(
                    ge.last_swap(r.name), "no swap recorded mid-import")
                tgs.step_until(r, "import_pack", {"done"})
                imp = ge.import_status(r.name, st["push"])
                if arm == "lossless":
                    self.assertEqual(imp["push_state"], "committed", "%r" % imp)
                else:
                    self.assertEqual(imp["push_state"], "rejected", "%r" % imp)
                assert_clean(self, r)

    # -- §6.15: an empty retire set still dead-marks the build pack -----------

    def test_empty_retire_set_deadmarks_build_pack(self):
        """§6.15 / §3.2 6a: EVERY cohort pack gains a post-build mark (the
        drain resurrects an unbuilt object in each), so the retire set is
        empty → wipe_all dead-marks the build pack N (nothing duplicated),
        the whole cohort stays live, gc_last_swap.retired == 0.
        Mutant: "keep N when nothing retired" fails.

        Expected red: the pushes move refs → today's sweep aborts, N is
        dead-marked by the abort keep-rule but no gc_last_swap exists."""
        r = repo("empty-retire")
        tb.barrier_on(r)
        # two MIXED packs, each holding one unbuilt garbage object
        r.commit_file("f", "one\n", "one")
        two = r.commit_file("f", "two\n", "two")
        r.push()  # pack1: one built, two unbuilt
        ge.git("reset", "--hard", "HEAD~1", cwd=r.dir)
        r.commit_file("f", "three\n", "three")
        g2 = orphan_commit(r, "g2")
        ge.git("push", "-qf", r.url, "HEAD:refs/heads/main",
               "%s:refs/heads/g2" % g2, cwd=r.dir)  # pack2: three + g2
        ge.git("push", "-q", r.url, ":refs/heads/g2", cwd=r.dir)
        cutoff_now(r)
        tb.to_stage(r, "presweep")
        cohort = cohort_ids(r)
        new_pack = tb.meta(r).get("gc.new_pack")
        # resurrect one unbuilt object per cohort pack → none retirable
        st, body = ge.receive_pack(
            r.name,
            [(ZERO, two, "refs/heads/res1"), (ZERO, g2, "refs/heads/res2")],
            ge.empty_pack())
        self.assertIn(b"ok refs/heads/res1", body,
                      "%r" % ge.redact(body[:300].decode("utf-8", "replace")))
        self.assertIn(b"ok refs/heads/res2", body,
                      "%r" % ge.redact(body[:300].decode("utf-8", "replace")))
        # the res pushes left a queued gc_mark — observe this chain's empty
        # retire set before a second chain can retire the cohort for real
        finish_chain(r, limit=60)
        swap = ge.last_swap(r.name)
        self.assertIsNotNone(swap, "no gc_last_swap")
        self.assertEqual(swap["retired"], 0, "%r" % swap)
        packs = packs_by_id(r)
        self.assertEqual(packs[new_pack]["state"], "dead",
                         "empty retire kept the build pack: %r" % packs[new_pack])
        self.assertTrue(all(packs[c]["state"] == "live" for c in cohort),
                        "empty retire killed a cohort pack: %r" % packs)
        tgs.gc_drain(r, limit=120)
        assert_clean(self, r)

    # -- §6.16: the compaction hold is released after the swap -----------------

    def test_compaction_released_after_swap(self):
        """§6.16 / §3.2 step 11: a gc_compact row that took a busy Reschedule
        while the chain stood is pulled forward (run_at <= now) right after
        the swap — and after a fallback. A queued never-started gc_mark is
        pushed to now + max(quiet,1), i.e. strictly behind it. GE_GC_QUIET_MS
        is 0 here, so the oracle is ordering: compact.run_at <= mark.run_at.

        Expected red: `gc_last_swap` absent and release_hold unimplemented —
        the compact row keeps its pushed-out run_at."""
        for mode in ("swap", "fallback"):
            with self.subTest(mode=mode):
                r = repo("crel-%s" % mode)
                tb.barrier_on(r)
                tb.seed(r, 3)
                cutoff_now(r)
                ge.knob(r.name, "compact_min_age_ms", 0)
                ge.knob(r.name, "compact_fanin", 2)
                tb.to_stage(r, "presweep")
                # a compact row arriving mid-chain reschedules behind it
                ge.enqueue(r.name, "gc_compact")
                s = ge.step(r.name, "gc_compact")
                self.assertIn(s["outcome"], ("reschedule", "done"), "%r" % s)
                if mode == "fallback":
                    ge.knob(r.name, "swap_tries", 1)
                    push_commit(r, "fb")
                # runs the swap/fallback to the chain's end — never the
                # queued mark the fb push left, which would run a second
                # chain whose swap overwrites gc_last_swap
                finish_chain(r, limit=80)
                swap = ge.last_swap(r.name)
                self.assertIsNotNone(swap, "no gc_last_swap")
                self.assertEqual(swap["fallback"], mode == "fallback",
                                 "%r" % swap)
                v = ge.jobs(r.name)
                crow = next(j for j in v["jobs"] if j["kind"] == "gc_compact")
                mrow = next((j for j in v["jobs"]
                             if j["kind"] == "gc_mark" and j["state"] == "queued"),
                            None)
                if mrow is None:
                    ge.enqueue(r.name, "gc_mark")
                    mrow = next(j for j in ge.jobs(r.name)["jobs"]
                                if j["kind"] == "gc_mark")
                self.assertLessEqual(
                    crow["run_at"], mrow["run_at"],
                    "compaction not released ahead of the queued mark: "
                    "%r vs %r" % (crow, mrow))
                # the released row actually runs between chains
                s = ge.step(r.name, "gc_compact")
                self.assertTrue(s["ran"], "%r" % s)
                tb.assert_pack_ok(self, r)


if __name__ == "__main__":
    unittest.main()
