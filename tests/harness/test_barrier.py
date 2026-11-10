"""PR5 phase A — RED tests for the G4 write barrier + snapshot mark with
delta re-mark (.pr5-design.md §11). Every test encodes the POST-phase-B
contract: the push-kind x stage matrix, mark-under-writer, the miss race,
the phase guard, pending packs (§5.5), the §6 keep rule, commit_ids
error-mode, flag-off inertness and the index checks. Most fail today —
gc.barrier is never persisted, gc_pending/gen do not exist and the new
failpoints have no host sites; the flag-off and oracle-basics arms pass
already and pin behaviour that must NOT change.
"""

import json
import os
import sys
import threading
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ge
import test_harness as th
import test_gc_safety as tgs

OWNER = th.OWNER + "-bar"
ZERO = "0" * 40


def repo(name):
    return ge.Repo("%s/%s" % (OWNER, name))


def step_until(r, kind, outcomes, limit=40):
    s = None
    for _ in range(limit):
        s = ge.step(r.name, kind)
        if s["outcome"] in outcomes:
            return s
    raise AssertionError("%s never reached %s: %r" % (kind, outcomes, s))


def meta(r):
    return ge.jobs(r.name)["gc_meta"]


def tables(r):
    return ge.jobs(r.name)["gc_tables"]


def frontier(r):
    return tables(r)["gc_frontier"]


def pending(r):
    return tables(r)["gc_pending"]


def live_packs(r):
    return tgs.live_packs(r)


def mark_check(r):
    """GET /_admin/mark_check — the §8 oracle (404 until phase B wires it)."""
    st, data = ge.http("GET", "/%s/_admin/mark_check" % r.name)
    if st != 200:
        raise AssertionError(
            "mark_check -> %d: %s" % (st, ge.redact(data[:300].decode("utf-8", "replace")))
        )
    return json.loads(data)


def oracle(tc, r, subset=True):
    """The barrier invariant: U ⊆ P, walk untruncated."""
    mc = mark_check(r)
    tc.assertFalse(mc["limit_hit"], "mark_check truncated: %r" % mc)
    if subset:
        tc.assertTrue(mc["u_subset_p"], "U not covered by P: %r" % mc)
    return mc


def barrier_on(r):
    """Ask the NEXT mark init to persist gc.barrier=1 (phase B reads the knob)."""
    ge.knob(r.name, "gc_barrier", 1)


def cohort_ids(r):
    """Pack ids in the candidate bitmap. Read before the sweep: marked
    wipes with the chain."""
    return {p["id"] for p in ge.jobs(r.name)["packs"] if p["marked"]}


def assert_cohort_live(tc, r, cohort):
    """A sweep that aborted retired nothing: every cohort pack is still live."""
    tc.assertTrue(cohort, "no cohort to check")
    packs = {p["id"]: p for p in ge.jobs(r.name)["packs"]}
    missing = sorted(c for c in cohort if c not in packs)
    tc.assertFalse(
        missing,
        "cohort pack(s) missing from packs: %r; packs=%r" % (missing, packs))
    dead = sorted(c for c in cohort if packs[c]["state"] != "live")
    tc.assertFalse(
        dead,
        "the sweep retired the cohort instead of aborting: %r" % packs)


def barrier_active(r):
    m = meta(r)
    return (
        m.get("gc.chain") is not None
        and m.get("gc.started_at") is not None
        and m.get("gc.barrier") == "1"
        and m.get("gc.mode") != "compact"
    )


def chain_alive(r):
    """The chain was NOT aborted before its sweep: gc.* still stands."""
    m = meta(r)
    return m.get("gc.started_at") is not None


def seed(r, pushes=2):
    """A couple of ordinary pushes — packs plus a queued gc_mark."""
    r.commit_file("f", "one\n", "one")
    r.push()
    for i in range(pushes - 1):
        r.commit_file("f", "p%d\n" % i, "p%d" % i)
        r.push()
    return git_tip(r)


def git_tip(r):
    return ge.git("rev-parse", "HEAD", cwd=r.dir).stdout.strip()


def pause_mark(r):
    """Start a gc_mark slice paused at gc.mark.after_read; returns (thread, box)."""
    ge.failpoint(r.name, "gc.mark.after_read", "pause", 1)
    box = {}

    def bg():
        try:
            box["s"] = ge.step(r.name, "gc_mark")
        except Exception as e:
            box["err"] = e

    t = threading.Thread(target=bg, daemon=True)
    t.start()
    deadline = time.time() + 15
    while time.time() < deadline:
        v = ge.jobs(r.name)
        armed = any(
            f["name"] == "gc.mark.after_read" and f["action"] == "pause"
            for f in v.get("failpoints", [])
        )
        consumed = any(
            f["name"] == "gc.mark.after_read" and f["remaining"] <= 0
            for f in v.get("failpoints", [])
        )
        running = any(j["kind"] == "gc_mark" and j["state"] == "running" for j in v["jobs"])
        if (consumed or not armed) and running:
            break
        time.sleep(0.1)
    else:
        raise AssertionError("mark slice never entered the pause")
    return t, box


def release_pause(r, t, box):
    ge.failpoint(r.name, "gc.mark.after_read", "off")
    t.join(45)
    if box.get("err") is not None:
        raise box["err"]
    return box.get("s")


def to_stage(r, stage):
    """Drive the chain to `stage`; returns a release() callable or None."""
    if stage == "init":
        # one capped slice: init + chain id + frontier seeds, walk barely started.
        # A tiny repo can legitimately finish the whole mark in this slice —
        # "done" just means the cell lands on the build edge instead.
        ge.knob(r.name, "slice_subreqs", 1)
        s = ge.step(r.name, "gc_mark")
        assert s["ran"] and s["outcome"] in ("continue", "done"), "%r" % s
        return None
    if stage == "midmark":
        t, box = pause_mark(r)
        return lambda: release_pause(r, t, box)
    if stage in ("markdone", "build", "midbuild", "presweep"):
        step_until(r, "gc_mark", {"done"})
    if stage == "midbuild":
        ge.knob(r.name, "slice_subreqs", 1)
        ge.step(r.name, "gc_consolidate")  # whatever it yields, build is live
        return None
    if stage == "presweep":
        step_until(r, "gc_consolidate", {"done"})
    return None


def assert_pack_ok(tc, r):
    """The post-drain oracle every matrix cell shares: fsck + fresh clone."""
    f = ge.fsck(r.name, "refs")
    tc.assertTrue(f["ok"], "%r" % f)
    clone = r.clone()
    ge.git("fsck", "--strict", cwd=clone)


def new_commit(r, tag):
    return r.commit_file("f", "%s\n" % tag, tag)


def do_action(r, kind, n, ctx):
    """Perform one push-kind action. Returns (accepted, detail)."""
    if kind == "ff":
        c = new_commit(r, "ff%d" % n)
        r.push()
        return True, c
    if kind == "force":
        ge.git("reset", "--hard", "HEAD~1", cwd=r.dir)
        c = new_commit(r, "fc%d" % n)
        r.push(force=True)
        return True, c
    if kind == "oldc":
        # ref created at a commit that was unreachable at snapshot time
        c = ctx["old"]
        ge.git("push", "-q", r.url, "%s:refs/heads/old%d" % (c, n), cwd=r.dir)
        return True, c
    if kind == "tag":
        ge.git("tag", "-a", "-m", "t%d" % n, "v%d" % n, cwd=r.dir)
        ge.git("push", "-q", r.url, "refs/tags/v%d" % n, cwd=r.dir)
        return True, "v%d" % n
    if kind == "emptyref":
        st, body = ge.receive_pack(
            r.name, [(ZERO, ctx["tip"], "refs/heads/e%d" % n)], ge.empty_pack()
        )
        return b"ok refs/heads/e%d" % n in body, body
    if kind == "loser":
        # non-atomic: every command loses the CAS but the pack still promotes
        c = new_commit(r, "ls%d" % n)
        listing = ge.git("rev-list", "--objects", c, "--not", "HEAD~1", cwd=r.dir).stdout
        st, body = ge.receive_pack(
            r.name, [("1" * 40, c, "refs/heads/l%d" % n)], ge.pack_objects(r.dir, listing)
        )
        return b"ng refs/heads/l%d" % n in body, body
    if kind == "delete":
        ge.git("push", "-q", r.url, ":refs/heads/del%d" % n, cwd=r.dir)
        return True, None
    if kind == "import":
        c = new_commit(r, "im%d" % n)
        parent = ge.git("rev-parse", "HEAD~1", cwd=r.dir).stdout.strip()
        listing = ge.git("rev-list", "--objects", c, "--not", parent, cwd=r.dir).stdout
        st = ge.import_stage(r.name, ge.pack_objects(r.dir, listing))
        body = ge.import_begin(
            r.name,
            st["push"],
            [{"key": st["key"], "bytes": st["bytes"]}],
            [{"old": parent, "new": c, "name": "refs/heads/main"}],
        )
        tgs.step_until(r, "import_pack", {"done"})
        imp = ge.import_status(r.name, st["push"])
        return imp.get("push_state") == "committed", imp
    if kind == "reject_pin":
        # freeze main at the seeded tip, then try to move it with an
        # empty-pack update — policy refuses before any barrier write
        ge.admin_route(r.name, "POST", "pin",
                       {"ref": "refs/heads/main", "sha": ctx["tip"]})
        try:
            c = new_commit(r, "pinrej%d" % n)
            st, body = ge.receive_pack(
                r.name, [(ctx["tip"], c, "refs/heads/main")], ge.empty_pack())
            ok = b"ng refs/heads/main" in body and b"ref is pinned" in body
            return ok, body
        finally:
            ge.admin_route(r.name, "POST", "unpin", {"ref": "refs/heads/main"})
    raise AssertionError("unknown kind %r" % kind)


# The matrix: (kind, stage) cells — each kind lands at a representative subset
# of the §11 stages; rejects are separate tests below.
MATRIX = [
    ("ff", "init"), ("ff", "midmark"), ("ff", "build"), ("ff", "presweep"),
    ("force", "midmark"), ("force", "presweep"),
    ("oldc", "init"), ("oldc", "midbuild"),
    ("tag", "midmark"), ("tag", "build"),
    ("import", "midmark"), ("import", "presweep"),
    ("emptyref", "midmark"), ("emptyref", "presweep"),
    ("loser", "midmark"), ("loser", "presweep"),
    ("delete", "midbuild"), ("delete", "presweep"),
]


@unittest.skipUnless(os.environ.get("GE_HOOKS_OFF") != "1", "needs the hooks-on server")
class Barrier(unittest.TestCase):
    # -- §11.1: the push-kind x stage matrix ---------------------------------

    def test_push_kind_stage_matrix(self):
        for kind, stage in MATRIX:
            with self.subTest(kind=kind, stage=stage):
                r = repo("mx-%s-%s" % (kind, stage))
                barrier_on(r)
                ge.knob(r.name, "slice_subreqs", 1)
                seed(r, 2)
                # an old commit for the 'oldc' kind + a ref to delete later
                old = ge.git("rev-parse", "HEAD~1", cwd=r.dir).stdout.strip()
                ge.git("push", "-q", r.url, "%s:refs/heads/del0" % old, cwd=r.dir)
                ctx = {"old": old, "tip": git_tip(r)}
                release = to_stage(r, stage)
                th.assert_barrier(self, r, True)
                ok, detail = do_action(r, kind, 0, ctx)
                if release is not None:
                    release()
                # the push landed (or lost, for 'loser') and the chain was
                # NOT aborted before its sweep — barrier mode has no
                # refs_version fence outside the sweep
                self.assertTrue(chain_alive(r),
                                "%s@%s aborted the chain: %r" % (kind, stage, meta(r)))
                mc = oracle(self, r)
                self.assertFalse(mc["limit_hit"])
                tgs.gc_drain(r, limit=120)
                # once the chain is done U must be empty and the repo intact
                mc = mark_check(r)
                self.assertFalse(mc["limit_hit"], "%r" % mc)
                self.assertEqual(mc["u"], [], "%r" % mc)
                assert_pack_ok(self, r)

    def test_policy_rejects_feed_nothing(self):
        """§11.1 rejects: a pinned update, a protected non-ff/delete and a
        stale-CAS loser are refused BEFORE any barrier write — no gc_pending
        row, no gen-1 frontier row, no refs_version bump."""
        r = repo("rejects")
        barrier_on(r)
        seed(r, 2)
        ge.knob(r.name, "slice_subreqs", 1)
        ge.step(r.name, "gc_mark")  # a live chain for the assertions to bite
        th.assert_barrier(self, r, True)
        tip = git_tip(r)
        tip1 = ge.git("rev-parse", "HEAD~1", cwd=r.dir).stdout.strip()
        ctx = {"tip": tip}
        # admin setup that itself lands nothing GC-visible except the
        # prot-1 create (empty pack -> no promotion); the pin for the
        # reject_pin case is handled inside do_action
        ge.admin_route(r.name, "POST", "protect", {"ref": "refs/heads/prot-*"})
        st, body = ge.receive_pack(
            r.name, [(ZERO, tip, "refs/heads/prot-1")], ge.empty_pack())
        self.assertIn(b"ok refs/heads/prot-1", body, "%r" % body[:300])
        before = ge.jobs(r.name)
        rv0 = before["gc_meta"].get("refs_version")
        f1_0 = before["gc_tables"]["gc_frontier"].get("gen", {}).get("1", 0)
        p0 = before["gc_tables"]["gc_pending"]["n"]
        # pinned update on main — via the do_action jig so the route path is
        # exercised end to end
        ok, detail = do_action(r, "reject_pin", 0, ctx)
        self.assertTrue(ok, "%r" % (detail[:300] if isinstance(detail, bytes) else detail))
        # protected non-ff, protected delete, stale-CAS — inline for the
        # snapshot precision
        cases = [
            ([(tip, tip1, "refs/heads/prot-1")], b"protected ref: non-fast-forward"),
            ([(tip, ZERO, "refs/heads/prot-1")], b"protected ref: deletion refused"),
            ([("1" * 40, tip, "refs/heads/cas")], b"failed to update ref"),
        ]
        for cmds, why in cases:
            st, body = ge.receive_pack(r.name, cmds, ge.empty_pack())
            self.assertIn(b"ng ", body, "%r" % body[:300])
            self.assertIn(why, body, "%r" % body[:300])
        after = ge.jobs(r.name)
        self.assertEqual(after["gc_meta"].get("refs_version"), rv0,
                         "a reject bumped refs_version")
        self.assertEqual(after["gc_tables"]["gc_frontier"].get("gen", {}).get("1", 0),
                         f1_0, "a reject fed the frontier")
        self.assertEqual(after["gc_tables"]["gc_pending"]["n"], p0,
                         "a reject left a pending row")
        ge.admin_route(r.name, "POST", "unprotect", {"ref": "refs/heads/prot-*"})
        tgs.gc_drain(r, limit=120)
        assert_pack_ok(self, r)

    # -- §11.2: mark completes under a writer pushing between every round -----

    def test_mark_completes_under_writer(self):
        """§4.6 / §11.2: with a low delta-rounds cap, a push on every
        after_read pause keeps gen-1 nonempty; the delta arm must still
        hand off to phase=build (the empty-frontier arm alone would stall)."""
        r = repo("mark-writer")
        barrier_on(r)
        seed(r, 2)
        for i in range(6):
            r.commit_file("f", "w%d\n" % i, "w%d" % i)
            r.push()
        ge.knob(r.name, "slice_subreqs", 1)
        ge.knob(r.name, "mark_delta_rounds", 3)
        reached_build = False
        leftover_gen1 = False
        for i in range(40):
            t, box = pause_mark(r)
            if i == 0:
                th.assert_barrier(self, r, True)
            # a writer lands during the await so gen-1 stays fed
            new_commit(r, "wr%d" % i)
            r.push()
            release_pause(r, t, box)
            m = meta(r)
            if m.get("gc.phase") == "build":
                v = ge.jobs(r.name)
                leftover_gen1 = (
                    v["gc_tables"]["gc_frontier"].get("gen", {}).get("1", 0) > 0
                    or v["gc_tables"]["gc_pending"]["n"] > 0
                )
                self.assertGreaterEqual(
                    int(m.get("gc.delta_rounds") or 0), 3,
                    "delta_rounds never reached the cap: %r" % m)
                reached_build = True
                break
            if not any(j["kind"] == "gc_mark" and j["state"] == "queued"
                       for j in ge.jobs(r.name)["jobs"]):
                break
        self.assertTrue(reached_build, "mark never reached phase=build: %r" % meta(r))
        self.assertTrue(leftover_gen1,
                        "handoff fired only via the empty arm; delta arm untested: %r" % meta(r))
        tgs.gc_drain(r, limit=120)
        assert_pack_ok(self, r)

    # -- §11.3: the miss race --------------------------------------------------

    def test_miss_race_stays_in_frontier(self):
        """§11.3 miss race: X sits in the frontier with no live row when its
        batch pops; a pack holding X is promoted during the await. X must
        NOT be recorded seen — it stays in the frontier, re-pops live, and
        its links (cohort-resident) still get marked: U drains to empty."""
        r = repo("miss-race")
        barrier_on(r)
        seed(r, 1)
        # X's closure first lands in pack py under a side ref that then goes
        # away: py stays live (a cohort candidate) holding xt/xb whose ONLY
        # reachable path is through xc.
        xb = ge.git("hash-object", "-w", "--stdin", cwd=r.dir, input="x\n").stdout.strip()
        xt = tgs.mktree(r, [("100644", "blob", xb, "x")])
        cy = tgs.commit_tree(r, xt, [], "cy")
        packs0 = {p["id"] for p in live_packs(r)}
        ge.git("push", "-q", r.url, "%s:refs/heads/y" % cy, cwd=r.dir)
        py = next(p["id"] for p in live_packs(r) if p["id"] not in packs0)
        ge.git("push", "-q", r.url, ":refs/heads/y", cwd=r.dir)
        # xc carries only itself — xt/xb resolve against py, so pack px is
        # exactly {xc}: killing px makes xc a frontier miss while its links
        # stay live in the cohort.
        xc = tgs.commit_tree(r, xt, [], "xc")
        packs0 = {p["id"] for p in live_packs(r)}
        st, body = ge.receive_pack(
            r.name, [(ZERO, xc, "refs/heads/x")], ge.pack_objects(r.dir, [xc]))
        self.assertIn(b"ok refs/heads/x", body, "%r" % body[:300])
        px = next(p["id"] for p in live_packs(r) if p["id"] not in packs0)
        ge.pack_state(r.name, px, "dead")
        # Force a yield after the first read so release_pause returns before
        # the slice re-pops xc and records it seen (Codex c2). Then park at
        # after_read and promote a pack holding xc during the await.
        ge.knob(r.name, "slice_subreqs", 0)
        t, box = pause_mark(r)
        th.assert_barrier(self, r, True)
        st, body = ge.receive_pack(
            r.name, [(ZERO, xc, "refs/heads/x2")], ge.pack_objects(r.dir, [xc]))
        self.assertIn(b"ok refs/heads/x2", body, "%r" % body[:300])
        release_pause(r, t, box)
        # Assert on the ORIGINAL chain while marked is intact: the miss must
        # still be in the frontier and must NOT be in gc_seen (a "record miss
        # as seen" mutation survives a post-drain check).
        v = ge.jobs(r.name)
        front = set(v["gc_tables"]["gc_frontier"]["shas"])
        seen = set(v["gc_tables"]["gc_seen"]["shas"])
        self.assertIn(xc, front, "miss was dropped from the frontier: %r" % sorted(front)[:8])
        self.assertNotIn(xc, seen, "miss was recorded as seen: %r" % sorted(seen)[:8])
        tgs.gc_drain(r, limit=160)
        mc = mark_check(r)
        self.assertEqual(mc["u"], [], "miss race leaked U: %r" % mc)
        assert_pack_ok(self, r)

    # -- §11.4: the phase guard, both modes ------------------------------------

    def test_phase_guard_legacy_and_barrier(self):
        """§11.4 / §4.7 M4: a stray gc_mark while the chain sits at phase=sweep
        must be a no-op — marked untouched, phase unmoved, no second
        consolidate. Legacy then aborts on moved refs at the sweep fence;
        barrier mode has no walk fence, so the chain survives.

        Each arm sets the gc_barrier knob. Without a 0 on the legacy arms
        the chain takes the server's GE_GC_BARRIER default, and a server
        started with GE_GC_BARRIER=1 runs a barrier chain for both."""
        for barrier in (False, True):
            with self.subTest(barrier=barrier):
                r = repo("phase-%s" % ("barrier" if barrier else "legacy"))
                if barrier:
                    barrier_on(r)
                else:
                    th.barrier_off(r)
                seed(r, 2)
                step_until(r, "gc_mark", {"done"})
                th.assert_barrier(self, r, barrier)
                step_until(r, "gc_consolidate", {"done"}, limit=10)
                # phase=sweep now, sweep queued — stray mark must not wedge it
                before = tables(r)
                n_con = sum(1 for j in ge.jobs(r.name)["jobs"]
                            if j["kind"] == "gc_consolidate")
                ge.enqueue(r.name, "gc_mark")
                s = ge.step(r.name, "gc_mark")
                self.assertTrue(s["ran"], "%r" % s)
                v = ge.jobs(r.name)
                self.assertEqual(v["gc_tables"]["marked"], before["marked"],
                                 "stray mark wrote mid-sweep")
                self.assertEqual(v["gc_meta"].get("gc.phase"), "sweep",
                                 "stray mark moved the phase: %r" % v["gc_meta"])
                n_con2 = sum(1 for j in v["jobs"] if j["kind"] == "gc_consolidate")
                self.assertEqual(n_con, n_con2, "a second consolidate enqueued")
                # finish the chain before draining — a rescheduled stray mark
                # would starve gc_drain's kind order otherwise
                if any(j["kind"] == "gc_sweep" and j["state"] == "queued"
                       for j in ge.jobs(r.name)["jobs"]):
                    step_until(r, "gc_sweep", {"done"}, limit=10)
                tgs.gc_drain(r, limit=120)
                assert_pack_ok(self, r)
        # legacy arm: refs moved between build and sweep -> the sweep fence
        # aborts the chain and wipes gc.*
        r = repo("phase-legacy-moved")
        th.barrier_off(r)
        seed(r, 2)
        step_until(r, "gc_mark", {"done"})
        th.assert_barrier(self, r, False)
        step_until(r, "gc_consolidate", {"done"}, limit=10)
        cohort = cohort_ids(r)
        new_commit(r, "mv")
        r.push()
        s = ge.step(r.name, "gc_sweep")
        self.assertTrue(s["ran"], "%r" % s)
        self.assertNotIn("gc.started_at", meta(r),
                         "legacy sweep survived moved refs: %r" % meta(r))
        assert_cohort_live(self, r, cohort)
        tgs.gc_drain(r, limit=120)
        assert_pack_ok(self, r)

    # -- §5.5: pending packs ----------------------------------------------------

    def test_pending_feeds_and_drains(self):
        """A push mid-chain promotes a pack -> gc_pending row -> the page span
        feeds its non-blobs to the frontier -> the row deletes at the end."""
        r = repo("pending-basic")
        barrier_on(r)
        seed(r, 2)
        ge.knob(r.name, "slice_subreqs", 1)
        ge.step(r.name, "gc_mark")  # init
        th.assert_barrier(self, r, True)
        c = new_commit(r, "pend")
        r.push()
        # the promoted pack's pending row is visible before the mark drains it
        rows = pending(r)["rows"]
        self.assertTrue(rows, "no gc_pending row after mid-chain push: %r" % pending(r))
        tgs.gc_drain(r, limit=120)
        self.assertEqual(pending(r)["rows"], [], "pending row never drained")
        mc = mark_check(r)
        self.assertEqual(mc["u"], [], "%r" % mc)
        assert_pack_ok(self, r)

    def test_pending_blob_heavy_window(self):
        """An all-blob pack: the page's kind!=3 filter returns nothing but the
        cursor must still advance window by window until the row deletes —
        otherwise the mark never hands off (the §5.5 wedge)."""
        r = repo("pending-blobs")
        barrier_on(r)
        seed(r, 1)
        # small pending window/page so a few hundred objects span several
        # windows — the >50k-object version reset the isolate (test workload,
        # not a server bug)
        ge.knob(r.name, "pending_window", 64)
        ge.knob(r.name, "pending_page", 8)
        ge.knob(r.name, "slice_subreqs", 1)
        ge.step(r.name, "gc_mark")  # init: barrier chain live before the push
        th.assert_barrier(self, r, True)
        # one pack of tiny blobs + a commit ref — python-built so the idx
        # layout is exact and no local objects are needed
        pack, csha = blob_pack(300)
        st, body = ge.receive_pack(r.name, [(ZERO, csha, "refs/heads/bulk")], pack)
        self.assertIn(b"ok refs/heads/bulk", body,
                      "%r" % ge.redact(body[:300].decode("utf-8", "replace")))
        # the promoted pack's pending row exists; drive the mark — it must
        # drain window by window, not wedge on the all-blob pages
        deadline = time.time() + 240
        while time.time() < deadline:
            tgs.gc_drain(r, limit=20)
            if not any(j["state"] == "queued" for j in ge.jobs(r.name)["jobs"]
                       if j["kind"].startswith("gc_")):
                break
        self.assertEqual(pending(r)["rows"], [], "blob-heavy pending never drained")
        mc = mark_check(r)
        self.assertFalse(mc["limit_hit"], "%r" % mc)
        assert_pack_ok(self, r)

    def test_pending_nonblob_window_edge(self):
        """A non-blob sitting past the first window: a later page must pick it
        up (cursor = last idx rule, not cursor += window on a short page)."""
        r = repo("pending-edge")
        barrier_on(r)
        seed(r, 1)
        ge.knob(r.name, "pending_window", 64)
        ge.knob(r.name, "pending_page", 8)
        ge.knob(r.name, "slice_subreqs", 1)
        ge.step(r.name, "gc_mark")  # init before the push so the row pends
        th.assert_barrier(self, r, True)
        # tree+commit land at idx ~100 — past the first 64-idx window, so the
        # second page span is what feeds them
        pack, csha = blob_pack(100, tail_nonblob=True)
        st, body = ge.receive_pack(r.name, [(ZERO, csha, "refs/heads/edge")], pack)
        self.assertIn(b"ok refs/heads/edge", body)
        tgs.gc_drain(r, limit=160)
        mc = mark_check(r)
        self.assertFalse(mc["limit_hit"], "%r" % mc)
        self.assertEqual(mc["u"], [], "%r" % mc)
        assert_pack_ok(self, r)

    def test_pending_demoted_pack_deleted(self):
        """§5.5: a pending row whose pack was demoted (atomic push failed)
        deletes on the next page span — it is not protection."""
        r = repo("pending-demoted")
        barrier_on(r)
        seed(r, 2)
        ge.knob(r.name, "slice_subreqs", 1)
        ge.step(r.name, "gc_mark")
        th.assert_barrier(self, r, True)
        # atomic push where one command loses: the whole push is rejected,
        # the promoted pack demotes, and its pending row must not protect it
        c = new_commit(r, "dem")
        listing = ge.git("rev-list", "--objects", c, "--not", "HEAD~1", cwd=r.dir).stdout
        pack = ge.pack_objects(r.dir, listing)
        st, body = ge.receive_pack(
            r.name,
            [(ZERO, c, "refs/heads/good"), (ZERO, "f" * 40, "refs/heads/bad")],
            pack,
            caps=("report-status", "atomic"),
        )
        self.assertIn(b"ng refs/heads/good", body, "%r" % body[:300])
        self.assertIn(b"ng refs/heads/bad", body, "%r" % body[:300])
        self.assertNotIn(b"ok refs/heads/good", body, "%r" % body[:300])
        tgs.gc_drain(r, limit=120)
        self.assertEqual(pending(r)["rows"], [], "demoted pack's pending row leaked")
        mc = mark_check(r)
        self.assertFalse(mc["limit_hit"], "%r" % mc)
        assert_pack_ok(self, r)

    # -- §6: the keep rule -----------------------------------------------------

    def test_keep_flagoff_abort_deadmarks_newpack(self):
        """Flag OFF, ordinary refs-moved sweep abort: gc.new_pack is
        dead-marked exactly like PR4 — the keep rule changes nothing here.

        The knob pins the mode to off. Without it the chain takes the
        server's GE_GC_BARRIER default, and a server started with
        GE_GC_BARRIER=1 runs a barrier chain, which has no walk fence
        and does not abort the sweep on moved refs."""
        r = repo("keep-flagoff")
        th.barrier_off(r)
        seed(r, 2)
        step_until(r, "gc_mark", {"done"})
        th.assert_barrier(self, r, False)
        step_until(r, "gc_consolidate", {"done"}, limit=10)
        cohort = cohort_ids(r)
        # move refs between build and sweep so the sweep fence aborts
        r.commit_file("f", "mv\n", "mv")
        r.push()
        s = ge.step(r.name, "gc_sweep")
        self.assertTrue(s["ran"], "%r" % s)
        assert_cohort_live(self, r, cohort)
        v = ge.jobs(r.name)
        self.assertNotIn("gc.new_pack", v["gc_meta"], "%r" % v["gc_meta"])
        # the build pack was dead-marked
        self.assertTrue(any(p["state"] == "dead" for p in v["packs"]), "%r" % v["packs"])
        tgs.gc_drain(r, limit=120)
        assert_pack_ok(self, r)

    def test_keep_partial_sweep(self):
        """A sweep dying after the cohort dead-mark keeps gc.new_pack live
        and reports dead_rows == 0 — the surviving new pack is NOT cohort.
        Killing the sweep owner then stepping gc_mark runs the M4 orphan arm:
        wipe_all(keep) dead-marks-or-keeps, deletes the dead cohort's object
        rows, and leaves gc.new_pack live. (PR6: the fenced swap's
        gc.swap.after_deadmark is the same post-dead-mark window.)"""
        r = repo("keep-partial")
        barrier_on(r)
        seed(r, 2)
        step_until(r, "gc_mark", {"done"})
        th.assert_barrier(self, r, True)
        step_until(r, "gc_consolidate", {"done"}, limit=10)
        v = ge.jobs(r.name)
        new_pack = v["gc_meta"].get("gc.new_pack")
        self.assertIsNotNone(new_pack, "%r" % v["gc_meta"])
        ge.failpoint(r.name, "gc.swap.after_deadmark", "error", 1)
        s = ge.step(r.name, "gc_sweep")  # dies mid-retirement
        self.assertEqual(s["outcome"], "retry", "failpoint never fired: %r" % s)
        v = ge.jobs(r.name)
        fp = next((f for f in v["failpoints"]
                   if f["name"] == "gc.swap.after_deadmark"), None)
        self.assertIsNotNone(fp)
        self.assertEqual(fp["remaining"], 0, "failpoint was never consumed")
        packs = {p["id"]: p for p in v["packs"]}
        # partial retirement: the build pack survives for the retry
        self.assertEqual(packs[new_pack]["state"], "live",
                         "partial sweep killed the new pack: %r" % packs[new_pack])
        self.assertGreater(ge.live_objects(r.name)["dead_rows"], 0,
                           "cohort object rows vanished before the retry")
        # strand the sweep owner so the next gc_mark slice takes the M4 arm.
        # Exhaust the resurrection budget BEFORE kill_job — otherwise the next
        # admin call's boot→repair resurrects a chain-less gc_sweep, which
        # orphaned() counts as the owner (COALESCE), and M4 only reschedules
        # (same pattern as CompactionTests.test_orphan_*).
        ge.meta(r.name, "resurrected.gc_sweep", "3")
        ge.kill_job(r.name, "gc_sweep")
        ge.enqueue(r.name, "gc_mark")
        s = ge.step(r.name, "gc_mark")
        self.assertTrue(s["ran"], "%r" % s)
        v = ge.jobs(r.name)
        packs = {p["id"]: p for p in v["packs"]}
        self.assertEqual(packs[new_pack]["state"], "live",
                         "keep path dead-marked the surviving pack")
        self.assertEqual(ge.live_objects(r.name)["dead_rows"], 0,
                         "keep path left dead-pack object rows behind")
        ge.failpoint(r.name, "gc.swap.after_deadmark", "off")
        tgs.gc_drain(r, limit=120)
        assert_pack_ok(self, r)

    def test_keep_partial_compact_swap(self):
        """The compact-mode arm: an error after cohort retirement inside
        swap keeps gc.new_pack for the lossless retry — wipe_all(keep)
        leaves it live and deletes the dead cohort's object rows."""
        r = repo("keep-compact")
        seed(r, 3)
        # a compact cohort: every pack eligible immediately, fanin 2 -> the
        # push path staked a gc_compact chain (or we drive it manually)
        ge.knob(r.name, "compact_min_age_ms", 0)
        ge.knob(r.name, "compact_fanin", 2)
        ge.knob(r.name, "slice_subreqs", 1)
        # kill the pushes' gc_mark trigger so no mark chain merges the packs
        # the compact cohort needs
        ge.kill_job(r.name, "gc_mark")
        if not any(j["kind"] == "gc_compact" and j["state"] == "queued"
                   for j in ge.jobs(r.name)["jobs"]):
            ge.enqueue(r.name, "gc_compact")
        s = ge.step(r.name, "gc_compact")  # init: cohort marked, build queued
        self.assertTrue(s["ran"], "%r" % s)
        # step consolidate slices until the armed failpoint has fired —
        # post-B the partial swap's wipe_all deletes gc.*, ending the chain
        ge.failpoint(r.name, "gc.compact.after_deadmark", "error", 1)
        new_pack = None
        for _ in range(120):
            s = ge.step(r.name, "gc_consolidate")
            if not s["ran"]:
                break
            v = ge.jobs(r.name)
            if v["gc_meta"].get("gc.new_pack"):
                new_pack = v["gc_meta"]["gc.new_pack"]
            if v["gc_meta"].get("gc.started_at") is None:
                break  # chain wiped: keep path ran (or swap completed)
        self.assertIsNotNone(new_pack, "compact build never stamped gc.new_pack")
        v = ge.jobs(r.name)
        fp = next((f for f in v["failpoints"]
                   if f["name"] == "gc.compact.after_deadmark"), None)
        self.assertIsNotNone(fp, "failpoint row vanished")
        self.assertEqual(fp["remaining"], 0,
                         "gc.compact.after_deadmark never fired: %r" % s)
        packs = {p["id"]: p for p in v["packs"]}
        self.assertEqual(packs[new_pack]["state"], "live",
                         "partial swap killed the new pack: %r" % packs[new_pack])
        self.assertEqual(ge.live_objects(r.name)["dead_rows"], 0,
                         "keep path left dead-pack object rows behind")
        ge.failpoint(r.name, "gc.compact.after_deadmark", "off")
        tgs.gc_drain(r, limit=120)
        assert_pack_ok(self, r)

    def test_keep_m4_orphan_abort(self):
        """M4: barrier chain at phase=build, dead consolidate owner, exhausted
        resurrection budget, a push lands -> the next gc_mark slice runs the
        orphan arm: the old chain aborts (new gc.chain id) and a fresh chain
        — barrier included — takes over."""
        r = repo("keep-orphan")
        barrier_on(r)
        seed(r, 2)
        step_until(r, "gc_mark", {"done"})
        th.assert_barrier(self, r, True)
        old_chain = meta(r).get("gc.chain")
        # strand the consolidate: exhaust the budget first, then kill
        # (boot→repair on every admin call would otherwise resurrect it)
        ge.meta(r.name, "resurrected.gc_consolidate", "3")
        ge.kill_job(r.name, "gc_consolidate")
        new_commit(r, "orph")
        r.push()
        s = ge.step(r.name, "gc_mark")
        self.assertTrue(s["ran"], "%r" % s)
        th.assert_barrier(self, r, True)
        m = meta(r)
        self.assertIsNotNone(m.get("gc.chain"), "%r" % m)
        self.assertNotEqual(m.get("gc.chain"), old_chain,
                            "orphan arm did not replace the chain: %r" % m)
        tgs.gc_drain(r, limit=120)
        assert_pack_ok(self, r)

    def test_keep_init_error_between_started_and_barrier(self):
        """An Err between the started_at and gc.barrier writes leaves a chain
        that is NOT barrier-active: the next push aborts it like legacy."""
        r = repo("keep-init-err")
        barrier_on(r)
        seed(r, 2)
        ge.failpoint(r.name, "gc.mark.init_barrier", "error", 1)
        s = ge.step(r.name, "gc_mark")  # init dies between the two writes
        self.assertEqual(s["outcome"], "retry",
                         "init_barrier failpoint never fired: %r" % s)
        ge.failpoint(r.name, "gc.mark.init_barrier", "off")
        m = meta(r)
        self.assertIn("gc.started_at", m, "%r" % m)
        self.assertNotIn("gc.barrier", m, "barrier persisted despite the Err: %r" % m)
        self.assertFalse(barrier_active(r),
                         "half-init chain reads as barrier-active: %r" % m)
        # not barrier-active -> a moved refs_version still aborts the mark
        new_commit(r, "post")
        r.push()
        tgs.gc_drain(r, limit=120)
        assert_pack_ok(self, r)

    def test_keep_total0_handoff_order(self):
        """begin_build total==0: the sweep row is enqueued BEFORE gc.phase is
        published — an Err between them is recoverable on retry. The fixture
        needs a candidate cohort that marks NOTHING: one old unreachable
        pack (zero marks), every other pack too young to be a candidate."""
        r = repo("keep-handoff")
        barrier_on(r)
        # garbage pack OLDER than the seed packs: unreachable, so its objects
        # never reach the frontier and marked stays empty for it
        gb = ge.git("hash-object", "-w", "--stdin", cwd=r.dir, input="g\n").stdout.strip()
        gt = tgs.mktree(r, [("100644", "blob", gb, "g")])
        g = tgs.commit_tree(r, gt, [], "g")
        packs0 = {p["id"] for p in ge.jobs(r.name)["packs"]}
        ge.git("push", "-q", r.url, "%s:refs/heads/g" % g, cwd=r.dir)
        # the first heads/* push made g the adopted HEAD (the adopt rule
        # fires while head's ref is missing) — point it back at main so the
        # delete below isn't "the current branch"; main itself lands with
        # the seed pushes and keeps every older pack unreachable-free
        ge.meta(r.name, "head", "refs/heads/main")
        ge.git("push", "-q", r.url, ":refs/heads/g", cwd=r.dir)
        garbage_pack = next(
            p for p in ge.jobs(r.name)["packs"] if p["id"] not in packs0)
        time.sleep(1.1)  # created_at must sort strictly before the seed packs
        seed(r, 2)
        garbage_created = garbage_pack["created_at"]
        # candidates = packs older than this cutoff: only the garbage pack
        ge.knob(r.name, "gc_candidate_before_ms", garbage_created + 1)
        ge.failpoint(r.name, "gc.build.handoff", "error", 1)
        step_until(r, "gc_mark", {"done"}, limit=10)
        th.assert_barrier(self, r, True)
        s = ge.step(r.name, "gc_consolidate")
        self.assertEqual(s["outcome"], "retry",
                         "handoff failpoint never fired: %r" % s)
        # the armed row was consumed — retry recovers through the normal path
        ge.failpoint(r.name, "gc.build.handoff", "off")
        tgs.gc_drain(r, limit=120)
        assert_pack_ok(self, r)

    # -- §11.8: commit_ids error-mode ------------------------------------------

    def test_commit_ids_error_modes(self):
        """Arm each error-mode point between commit_ids statements; the mark
        retries and K still holds — U drains to empty, fsck clean."""
        for fp in ("gc.mark.commit_ids.after_bits",
                   "gc.mark.commit_ids.after_seen",
                   "gc.mark.commit_ids.after_kids"):
            with self.subTest(failpoint=fp):
                r = repo("ci-%s" % fp.rsplit(".", 1)[-1])
                barrier_on(r)
                seed(r, 2)
                ge.knob(r.name, "slice_subreqs", 1)
                ge.failpoint(r.name, fp, "error", 1)
                saw_retry = False
                for _ in range(20):
                    s = ge.step(r.name, "gc_mark")
                    if not s["ran"]:
                        break
                    th.assert_barrier(self, r, True)
                    if s["outcome"] == "retry":
                        saw_retry = True
                    if s["outcome"] == "done":
                        break
                self.assertTrue(saw_retry,
                                "%s never fired: %r" % (fp, s))
                tgs.gc_drain(r, limit=120)
                mc = mark_check(r)
                self.assertFalse(mc["limit_hit"], "%r" % mc)
                self.assertEqual(mc["u"], [], "%r" % mc)
                assert_pack_ok(self, r)

    # -- §1 flag-off inertness --------------------------------------------------

    def test_flagoff_pending_and_gen1_stay_empty(self):
        """§1: with the barrier knob OFF a full push+GC cycle writes no
        gc_pending rows and no gen=1 frontier rows — the schema additions
        are inert.

        The knob pins the mode to off. Without it the chain takes the
        server's GE_GC_BARRIER default, and a server started with
        GE_GC_BARRIER=1 runs a barrier chain, whose pending and gen-1
        rows are the opposite of this contract."""
        r = repo("flagoff")
        th.barrier_off(r)
        seed(r, 2)
        ge.knob(r.name, "slice_subreqs", 1)
        s = ge.step(r.name, "gc_mark")
        self.assertIn(s["outcome"], ("continue", "done"), "%r" % s)
        th.assert_barrier(self, r, False)
        # mid-mark snapshot: no pending rows, no gen-1 frontier rows
        t = tables(r)
        self.assertEqual(t["gc_pending"]["n"], 0, "%r" % t["gc_pending"])
        self.assertEqual(t["gc_frontier"].get("gen", {}).get("1", 0), 0,
                         "%r" % t["gc_frontier"])
        new_commit(r, "off")
        r.push()
        tgs.gc_drain(r, limit=120)
        t = tables(r)
        self.assertEqual(t["gc_pending"]["n"], 0, "%r" % t["gc_pending"])
        self.assertEqual(t["gc_frontier"].get("gen", {}).get("1", 0), 0,
                         "%r" % t["gc_frontier"])
        assert_pack_ok(self, r)

    # -- §11.6: index checks -----------------------------------------------------

    def test_explain_index_checks(self):
        """EXPLAIN QUERY PLAN: the frontier pop seeks (gen,sha) and the pending
        page seeks objects(pack_id,idx) — no temp B-tree, bounded scan."""
        r = repo("explain")
        seed(r, 2)
        # the pop query shape phase B must serve — fails until gen exists
        plan = explain(r, "EXPLAIN QUERY PLAN SELECT sha FROM gc_frontier "
                            "WHERE gen=(SELECT MIN(gen) FROM gc_frontier) LIMIT 512")
        detail = " ".join(str(row) for row in plan["rows"])
        self.assertIn("gc_frontier", detail)
        self.assertIn("INDEX", detail.upper())
        self.assertNotIn("TEMP B-TREE", detail.upper())
        # the pending page query — objects rows by (pack_id, idx)
        plan = explain(r, "EXPLAIN QUERY PLAN SELECT sha, idx FROM objects "
                            "WHERE pack_id='x' AND idx>-1 AND idx<=49999 "
                            "AND kind != 3 ORDER BY idx LIMIT 5000")
        detail = " ".join(str(row) for row in plan["rows"])
        self.assertIn("objects", detail)
        self.assertNotIn("TEMP B-TREE", detail.upper())


def explain(r, sql):
    return ge.admin(r.name, "POST", "query", {"sql": sql})


# -- python-side pack builder -------------------------------------------------
# A git pack is "PACK" + u32 version + u32 count + entries + sha1 trailer.
# Each entry is a varint header — first byte: type in bits 4-6, size's low
# 4 bits in bits 0-3, MSB = continuation; each further byte carries 7 size
# bits with MSB continuation — followed by zlib(raw object content). The
# "<type> <len>\0" framing is the LOOSE-object format: it feeds the SHA-1
# (obj_sha) but is NEVER stored inside a pack entry.
import hashlib
import struct
import zlib


def obj_sha(kind, content):
    h = hashlib.sha1()
    h.update(("%s %d\0" % (kind, len(content))).encode())
    h.update(content)
    return h.hexdigest()


def _entry(kind, content):
    """One pack entry for `kind` (1=commit 2=tree 3=blob 4=tag) holding the
    RAW object bytes — header size is the uncompressed content length."""
    size = len(content)
    first = (kind << 4) | (size & 0x0F)
    size >>= 4
    hdr = bytearray()
    if size:
        first |= 0x80
    hdr.append(first)
    while size:
        b = size & 0x7F
        size >>= 7
        hdr.append(b | (0x80 if size else 0))
    return bytes(hdr) + zlib.compress(content)


def blob_pack(n_blobs, tail_nonblob=False):
    """n_blobs tiny blobs, then a tree+commit — returns (pack, commit_sha).
    With tail_nonblob the non-blobs land past idx 50000 so the pending-page
    cursor must span two windows to see them."""
    entries = []
    blob_shas = []
    for i in range(n_blobs):
        content = ("b%d\n" % i).encode()
        entries.append((3, content))
        blob_shas.append(obj_sha("blob", content))
    # binary tree entry: "<mode> <name>\0" + raw oid — one blob "f"
    tree_content = b"100644 f\x00" + bytes.fromhex(blob_shas[-1])
    tree_sha = obj_sha("tree", tree_content)
    commit_content = (
        "tree %s\nauthor t <t@t> 0 +0000\ncommitter t <t@t> 0 +0000\n\nx\n" % tree_sha
    ).encode()
    commit_sha = obj_sha("commit", commit_content)
    if tail_nonblob:
        entries.append((2, tree_content))
        entries.append((1, commit_content))
    else:
        entries.insert(0, (1, commit_content))
        entries.insert(1, (2, tree_content))
    body = bytearray(b"PACK" + struct.pack(">II", 2, len(entries)))
    for kind, content in entries:
        body += _entry(kind, content)
    body += hashlib.sha1(bytes(body)).digest()
    return bytes(body), commit_sha


class AssertCohortLiveTests(unittest.TestCase):
    """No server: a missing pack must be an assertion, not a KeyError."""

    def _check(self, packs, cohort):
        class R:
            name = "x"
        saved = ge.jobs
        ge.jobs = lambda _name: {"packs": packs}
        try:
            assert_cohort_live(self, R(), cohort)
        finally:
            ge.jobs = saved

    def test_missing_pack_is_assertion_not_keyerror(self):
        with self.assertRaises(AssertionError) as cm:
            self._check([{"id": "a", "state": "live"}], {"a", "gone"})
        msg = str(cm.exception)
        self.assertIn("missing", msg)
        self.assertIn("gone", msg)

    def test_dead_pack_is_assertion(self):
        with self.assertRaises(AssertionError) as cm:
            self._check([{"id": "a", "state": "dead"}], {"a"})
        self.assertIn("retired the cohort", str(cm.exception))

    def test_empty_cohort_is_assertion(self):
        with self.assertRaises(AssertionError) as cm:
            self._check([{"id": "a", "state": "live"}], set())
        self.assertIn("no cohort", str(cm.exception))

    def test_live_cohort_passes(self):
        self._check(
            [{"id": "a", "state": "live"}, {"id": "b", "state": "live"}],
            {"a", "b"},
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
