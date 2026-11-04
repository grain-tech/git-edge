"""PR1 phase A — RED tests for the GC safety bugs in the audit (§2: C1, C2,
C3, C7). Every test must FAIL on unfixed code; each docstring names the exact
assertion expected to fail and why. Phase B lands the fixes against these
oracles. Same hooks-on dev server as test_harness.py (README.md).
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

ZERO = "0" * 40


def repo(name):
    return th.repo(name)


def step_until(r, kind, outcomes, limit=30):
    """ge.step(r, kind) until the outcome lands in `outcomes`."""
    s = None
    for _ in range(limit):
        s = ge.step(r.name, kind)
        if s["outcome"] in outcomes:
            return s
    raise AssertionError("%s never reached %s in %d steps: %r" % (kind, outcomes, limit, s))


def gc_chain(r, limit=30):
    """Drive one full mark -> consolidate -> sweep via kind-pinned steps.
    Kind-agnostic stepping would also pick up unrelated queued jobs (e.g. the
    in-flight import of the C1 test) and interleave them into the chain.
    A chain can end without a sweep: a fully-marked single candidate is kept
    as-is (nothing to repack), so 'no sweep queued' is also done."""
    step_until(r, "gc_mark", {"done"}, limit)
    step_until(r, "gc_consolidate", {"done"}, limit)
    for _ in range(limit):
        s = ge.step(r.name, "gc_sweep")
        if not s["ran"] or s["outcome"] == "done":
            return s
    raise AssertionError("gc_sweep never finished")


def gc_drain(r, limit=60):
    """Step GC jobs until none of the three kinds is queued — unlike gc_chain
    this tolerates mid-chain aborts (an abort re-enqueues gc_mark) and chains
    that end without a sweep."""
    for _ in range(limit):
        v = ge.jobs(r.name)
        queued = {j["kind"] for j in v["jobs"] if j["state"] == "queued"}
        kind = next(
            (k for k in ("gc_mark", "gc_consolidate", "gc_sweep") if k in queued),
            None,
        )
        if kind is None:
            return
        ge.step(r.name, kind)
    raise AssertionError("gc chain never drained in %d steps" % limit)


def import_until_commit_phase(r, limit=40):
    """Step an import_pack job until its persisted cursor shows phase=commit —
    the connectivity check has fully passed."""
    for _ in range(limit):
        s = ge.step(r.name, "import_pack")
        j = next((j for j in s["jobs"] if j["kind"] == "import_pack"), None)
        if j is None:
            raise AssertionError("import job vanished mid-run: %r" % s["jobs"])
        cur = json.loads(j["cursor"]) if j["cursor"] else {}
        if j["state"] == "queued" and cur.get("phase") == "commit":
            return
    raise AssertionError("import never reached phase=commit")


def commit_tree(r, tree, parents, msg):
    args = ["commit-tree", tree]
    for p in parents:
        args += ["-p", p]
    return ge.git(*(args + ["-m", msg]), cwd=r.dir).stdout.strip()


def mktree(r, entries):
    """entries = [(mode, kind, sha, name)] -> tree sha."""
    spec = "".join("%s %s %s\t%s\n" % e for e in entries)
    return ge.git("mktree", cwd=r.dir, input=spec).stdout.strip()


def live_packs(r):
    return [p for p in ge.jobs(r.name)["packs"] if p["state"] == "live"]


class GcSafety(unittest.TestCase):
    def assert_push_ok(self, st, body, ref):
        """A report-status body on a fully accepted push."""
        self.assertEqual(st, 200, "%r" % ge.redact(body[:400].decode("utf-8", "replace")))
        self.assertIn(b"unpack ok", body)
        self.assertIn(b"ok " + ref.encode(), body)
        self.assertNotIn(b"ng ", body)

    # -- C1: an import's connectivity check predates its commit; a GC in the
    #        window retires the checked-against pack ----------------------------

    def test_c1_import_vs_gc(self):
        """Expected failing assertion:
            self.assertTrue(f["ok"], "%r" % f)
        Today the import commits main -> C1 after GC retired C0's pack, and
        refs-mode fsck reports C0 missing (referenced_by C1)."""
        r = repo("c1-import")
        # main is HEAD with its own history, unrelated to the C0 line — the
        # server refuses to delete the ref HEAD names, so x must not be it
        m = r.commit_file("f", "m\n", "m")
        r.push()  # pack PM: main -> M
        # x holds orphan C0 in its own pack P
        blob_x = ge.git("hash-object", "-w", "--stdin", cwd=r.dir, input="x\n").stdout.strip()
        c0 = commit_tree(r, mktree(r, [("100644", "blob", blob_x, "x")]), [], "c0")
        ge.git("push", "-q", r.url, "%s:refs/heads/x" % c0, cwd=r.dir)
        # C1 is C0's child — never pushed as a ref; the import carries only its
        # own objects so parent C0 is an external link the check phase verifies
        blob_1 = ge.git("hash-object", "-w", "--stdin", cwd=r.dir, input="c1\n").stdout.strip()
        tree_1 = mktree(r, [("100644", "blob", blob_1, "f")])
        c1 = commit_tree(r, tree_1, [c0], "c1")
        st = ge.import_stage(r.name, ge.pack_objects(r.dir, [c1, tree_1, blob_1]))
        body = ge.import_begin(
            r.name,
            st["push"],
            [{"key": st["key"], "bytes": st["bytes"]}],
            [{"old": m, "new": c1, "name": "refs/heads/main"}],
        )
        self.assertIn(b"queued", body)
        # step the import until its check phase has fully passed — the jobs
        # view exposes the persisted cursor; phase "commit" means check is done
        import_until_commit_phase(r)
        # delete x (allowed: main is HEAD), then run a full GC chain — C0's
        # pack goes dead while the import sits queued at its commit phase
        ge.git("push", "-q", r.url, ":refs/heads/x", cwd=r.dir)
        self.assertNotIn("refs/heads/x", ge.git("ls-remote", r.url).stdout)
        gc_chain(r)
        self.assertTrue(any(p["state"] == "dead" for p in ge.jobs(r.name)["packs"]),
                        "GC never retired C0's pack")
        step_until(r, "import_pack", {"done"})
        imp = ge.import_status(r.name, st["push"])
        # the re-check under the post-GC epoch finds C0 missing and REJECTS —
        # committing would have left main -> C1 dangling on a swept parent
        self.assertEqual(imp["push_state"], "rejected", "%r" % imp)
        self.assertIn("missing object %s" % c0, str(imp.get("result")), "%r" % imp)
        f = ge.fsck(r.name, "refs")
        self.assertTrue(f["ok"], "push_state=%r fsck=%r" % (imp.get("push_state"), f))

    def test_c1_revalidated_import_commits(self):
        """The other C1 arm: a GC runs between check and commit but takes
        NOTHING the import needs — the epoch pre-check sends the job back
        through check, every link re-verifies live, and the import COMMITS."""
        r = repo("c1-revalidated")
        m = r.commit_file("f", "m\n", "m")
        r.push()
        blob_x = ge.git("hash-object", "-w", "--stdin", cwd=r.dir, input="x\n").stdout.strip()
        c0 = commit_tree(r, mktree(r, [("100644", "blob", blob_x, "x")]), [], "c0")
        ge.git("push", "-q", r.url, "%s:refs/heads/x" % c0, cwd=r.dir)  # C0 stays reachable
        # an unrelated garbage pack for the sweep to retire: push g, delete g
        blob_g = ge.git("hash-object", "-w", "--stdin", cwd=r.dir, input="g\n").stdout.strip()
        g = commit_tree(r, mktree(r, [("100644", "blob", blob_g, "g")]), [], "g")
        ge.git("push", "-q", r.url, "%s:refs/heads/g" % g, cwd=r.dir)
        ge.git("push", "-q", r.url, ":refs/heads/g", cwd=r.dir)
        blob_1 = ge.git("hash-object", "-w", "--stdin", cwd=r.dir, input="c1\n").stdout.strip()
        tree_1 = mktree(r, [("100644", "blob", blob_1, "f")])
        c1 = commit_tree(r, tree_1, [c0], "c1")
        st = ge.import_stage(r.name, ge.pack_objects(r.dir, [c1, tree_1, blob_1]))
        body = ge.import_begin(
            r.name,
            st["push"],
            [{"key": st["key"], "bytes": st["bytes"]}],
            [{"old": m, "new": c1, "name": "refs/heads/main"}],
        )
        self.assertIn(b"queued", body)
        import_until_commit_phase(r)
        # every pack is a candidate: G's objects are unmarked, so the chain
        # repacks and sweeps — gc_epoch moves while C0 stays live under x
        ge.knob(r.name, "gc_candidate_before_ms", int(time.time() * 1000) + 1)
        gc_drain(r)
        self.assertTrue(any(p["state"] == "dead" for p in ge.jobs(r.name)["packs"]),
                        "GC never retired the garbage pack")
        step_until(r, "import_pack", {"done"})
        imp = ge.import_status(r.name, st["push"])
        self.assertEqual(imp["push_state"], "committed", "%r" % imp)
        self.assertEqual(
            ge.git("ls-remote", r.url, "refs/heads/main").stdout.split()[0], c1)
        f = ge.fsck(r.name, "refs")
        self.assertTrue(f["ok"], "%r" % f)

    def test_c1_reset_after_finish_commits(self):
        """The post-finish C1 arm: the pause lands a sweep between
        finish_resumable (the MPU is consumed) and commit_meta's epoch
        compare. The compare resets the job to check; the re-commit's finish
        fails on the spent MPU and recover_dead_upload's HEAD arm commits
        the already-durable object — the import still ends 'committed'."""
        r = repo("c1-after-finish")
        m = r.commit_file("f", "m\n", "m")
        r.push()
        blob_x = ge.git("hash-object", "-w", "--stdin", cwd=r.dir, input="x\n").stdout.strip()
        c0 = commit_tree(r, mktree(r, [("100644", "blob", blob_x, "x")]), [], "c0")
        ge.git("push", "-q", r.url, "%s:refs/heads/x" % c0, cwd=r.dir)  # C0 stays reachable
        # an unrelated garbage pack for the sweep to retire (revalidated-style)
        blob_g = ge.git("hash-object", "-w", "--stdin", cwd=r.dir, input="g\n").stdout.strip()
        g = commit_tree(r, mktree(r, [("100644", "blob", blob_g, "g")]), [], "g")
        ge.git("push", "-q", r.url, "%s:refs/heads/g" % g, cwd=r.dir)
        ge.git("push", "-q", r.url, ":refs/heads/g", cwd=r.dir)
        blob_1 = ge.git("hash-object", "-w", "--stdin", cwd=r.dir, input="c1\n").stdout.strip()
        tree_1 = mktree(r, [("100644", "blob", blob_1, "f")])
        c1 = commit_tree(r, tree_1, [c0], "c1")
        st = ge.import_stage(r.name, ge.pack_objects(r.dir, [c1, tree_1, blob_1]))
        body = ge.import_begin(
            r.name,
            st["push"],
            [{"key": st["key"], "bytes": st["bytes"]}],
            [{"old": m, "new": c1, "name": "refs/heads/main"}],
        )
        self.assertIn(b"queued", body)
        import_until_commit_phase(r)
        # every pack a candidate: G's objects are unmarked, so the chain
        # repacks and sweeps — gc_epoch moves while C0 stays live under x
        ge.knob(r.name, "gc_candidate_before_ms", int(time.time() * 1000) + 1)
        ge.failpoint(r.name, "import.commit.after_finish", "pause", 1)
        box = {}

        def bg_step():
            try:
                box["s"] = ge.step(r.name, "import_pack")
            except Exception as e:
                box["err"] = e

        t = threading.Thread(target=bg_step, daemon=True)
        t.start()
        # "the slice is parked after finish", deterministically: consumed
        # arm + running row — the same probe as the C7 pauses
        deadline = time.time() + 15
        while time.time() < deadline:
            v = ge.jobs(r.name)
            armed = any(f["name"] == "import.commit.after_finish" and f["action"] == "pause"
                        for f in v.get("failpoints", []))
            consumed = any(f["name"] == "import.commit.after_finish" and f["remaining"] <= 0
                           for f in v.get("failpoints", []))
            running = any(j["kind"] == "import_pack" and j["state"] == "running"
                          for j in v["jobs"])
            if (consumed or not armed) and running:
                break
            time.sleep(0.1)
        else:
            self.fail("commit slice never entered the pause")
        if not armed:
            self.fail("pause failpoint vanished before release")
        # the sweep lands in the finish/commit gap: the garbage pack is
        # retired and gc_epoch bumps under the parked slice
        gc_drain(r)
        self.assertTrue(any(p["state"] == "dead" for p in ge.jobs(r.name)["packs"]),
                        "GC never retired the garbage pack")
        ge.failpoint(r.name, "import.commit.after_finish", "off")  # release
        t.join(45)
        self.assertIsNone(box.get("err"), "%r" % box.get("err"))
        s = box.get("s")
        self.assertIsNotNone(s, "paused step never returned")
        # commit_meta's epoch compare sent the job back through check
        self.assertEqual(s["outcome"], "continue", "%r" % s)
        step_until(r, "import_pack", {"done"})
        imp = ge.import_status(r.name, st["push"])
        self.assertEqual(imp["push_state"], "committed", "%r" % imp)
        self.assertEqual(
            ge.git("ls-remote", r.url, "refs/heads/main").stdout.split()[0], c1)
        f = ge.fsck(r.name, "refs")
        self.assertTrue(f["ok"], "%r" % f)

    # -- C2: a young pack is kept but never a root; its commits' links into an
    #        old candidate pack are swept out from under it ---------------------

    def _c2_scenario(self, name):
        """Build P (old, holds tree X behind commit A on main) and Q (young,
        holds only commit C which reuses X), drop every ref path to X, then run
        a full GC. Returns (repo, tree_x, commit_c, live_blob)."""
        r = repo(name)
        a = r.commit_file("f", "x\n", "a")
        tree_x = ge.git("rev-parse", "HEAD^{tree}", cwd=r.dir).stdout.strip()
        r.push()  # P = {A, treeX, blob}
        p = live_packs(r)[0]
        time.sleep(1.1)  # created_at is ms-resolution — P and Q must differ
        c = commit_tree(r, tree_x, [a], "c")  # C reuses X exactly
        st, body = ge.receive_pack(
            r.name, [(ZERO, c, "refs/heads/y")], ge.pack_objects(r.dir, [c])
        )
        self.assert_push_ok(st, body, "refs/heads/y")  # Q = {C} only
        q = next(x for x in live_packs(r) if x["id"] != p["id"])
        self.assertLess(p["created_at"], q["created_at"],
                        "pack timestamps collide — the cutoff can't split them")
        ge.git("push", "-q", r.url, ":refs/heads/y", cwd=r.dir)  # C unreachable
        # force main to orphan B (different tree): X leaves the ref closure
        blob_b = ge.git("hash-object", "-w", "--stdin", cwd=r.dir, input="b\n").stdout.strip()
        b = commit_tree(r, mktree(r, [("100644", "blob", blob_b, "g")]), [], "b")
        ge.git("push", "-q", "-f", r.url, "%s:refs/heads/main" % b, cwd=r.dir)
        # an absolute cutoff between P and Q: P a candidate, Q young — no
        # moving grace boundary to race (created_at is monotone here)
        ge.knob(r.name, "gc_candidate_before_ms", p["created_at"] + 1)
        step_until(r, "gc_mark", {"done"})
        packs = {x["id"]: x for x in ge.jobs(r.name)["packs"]}
        self.assertTrue(packs[p["id"]]["marked"], "P was not a GC candidate")
        self.assertFalse(packs[q["id"]]["marked"], "Q was wrongly a GC candidate")
        gc_drain(r)
        # a completed chain leaves no gc.* state — the sweep arm and the
        # nothing-to-repack wipe both clear it (P is dead only in the red world)
        self.assertNotIn("gc.started_at", ge.jobs(r.name)["gc_meta"],
                         "GC chain never ran")
        f = ge.fsck(r.name, "refs")
        self.assertTrue(f["ok"], "%r" % f)  # baseline: main -> B is intact
        return r, tree_x, c, blob_b

    def test_c2_young_commit_old_tree_via_child(self):
        """Expected failing assertion:
            self.assertTrue(f["ok"], "%r" % f)
        Today the child push lands refs/heads/child -> D (tip D is live, its
        direct link C is live) and refs-mode fsck reports X and A missing."""
        r, tree_x, c, blob_b = self._c2_scenario("c2-child")
        # D's brand-new tree links only at live objects, so the pack can carry
        # exactly {commit D, tree D} — nothing of C's dead closure is resent
        tree_d = mktree(r, [("100644", "blob", blob_b, "sub")])
        d = commit_tree(r, tree_d, [c], "d")
        st, body = ge.receive_pack(
            r.name, [(ZERO, d, "refs/heads/child")], ge.pack_objects(r.dir, [d, tree_d])
        )
        self.assert_push_ok(st, body, "refs/heads/child")
        f = ge.fsck(r.name, "refs")
        self.assertTrue(f["ok"], "tree_x=%s fsck=%r" % (tree_x, f))
        clone = r.clone()
        ge.git("fsck", "--strict", cwd=clone)

    def test_c2_young_commit_old_tree_via_empty_pack_ref(self):
        """Expected failing assertion:
            self.assertTrue(f["ok"], "%r" % f)
        Today a 0-object pack creates refs/heads/z -> C (tip C is live in the
        kept pack Q) and refs-mode fsck reports X and A missing."""
        r, tree_x, c, _ = self._c2_scenario("c2-emptyref")
        st, body = ge.receive_pack(r.name, [(ZERO, c, "refs/heads/z")], ge.empty_pack())
        self.assert_push_ok(st, body, "refs/heads/z")
        f = ge.fsck(r.name, "refs")
        self.assertTrue(f["ok"], "tree_x=%s fsck=%r" % (tree_x, f))
        clone = r.clone()
        ge.git("fsck", "--strict", cwd=clone)

    def test_c2_non_atomic_loser_pack_goes_live(self):
        """Expected failing assertion:
            self.assertTrue(f["ok"], "%r" % f)
        A non-atomic push whose every command loses still promotes its pack to
        live. Pre-fix refs_version never moves, so the running chain's mark
        snapshot goes stale without aborting: X is swept even though the new
        live pack's commit C links to it, and the z ref create dangles."""
        r = repo("c2-loser")
        a = r.commit_file("f", "x\n", "a")
        tree_x = ge.git("rev-parse", "HEAD^{tree}", cwd=r.dir).stdout.strip()
        r.push()  # P = {A, treeX, blob}
        p = live_packs(r)[0]
        # move main to orphan B: X and A leave the ref closure — P is garbage
        blob_b = ge.git("hash-object", "-w", "--stdin", cwd=r.dir, input="b\n").stdout.strip()
        b = commit_tree(r, mktree(r, [("100644", "blob", blob_b, "g")]), [], "b")
        ge.git("push", "-q", "-f", r.url, "%s:refs/heads/main" % b, cwd=r.dir)
        # absolute cutoff now: every existing pack is a candidate; the loser
        # pack pushed mid-chain below is created AFTER it and stays young
        cutoff = int(time.time() * 1000) + 1
        ge.knob(r.name, "gc_candidate_before_ms", cutoff)
        step_until(r, "gc_mark", {"done"})  # mid-chain: consolidate is queued
        packs = {x["id"]: x for x in ge.jobs(r.name)["packs"]}
        self.assertTrue(packs[p["id"]]["marked"], "P was not a GC candidate")
        # the loser push: a real pack (commit C, reusing tree X) whose only
        # command is a stale CAS — the ref update fails but the pack goes live
        c = commit_tree(r, tree_x, [a], "c")
        st, body = ge.receive_pack(
            r.name, [("1" * 40, c, "refs/heads/q")], ge.pack_objects(r.dir, [c])
        )
        self.assertEqual(st, 200, "%r" % ge.redact(body[:400].decode("utf-8", "replace")))
        self.assertIn(b"ng refs/heads/q", body)  # every command lost
        q = next(x for x in live_packs(r) if x["id"] not in packs)
        self.assertGreaterEqual(q["created_at"], cutoff,
                                "loser pack was a candidate — fixture broken")
        gc_drain(r)
        # tip C is live in the young pack: a 0-object pack lands the ref
        st, body = ge.receive_pack(r.name, [(ZERO, c, "refs/heads/z")], ge.empty_pack())
        self.assert_push_ok(st, body, "refs/heads/z")
        f = ge.fsck(r.name, "refs")
        self.assertTrue(f["ok"], "tree_x=%s fsck=%r" % (tree_x, f))
        clone = r.clone()
        ge.git("fsck", "--strict", cwd=clone)

    # -- GC phase guard: a chain-less (resurrected) consolidate/sweep row may
    #        only run while a live chain actually reached that phase ---------

    def test_gc_phase_stray_consolidate_is_noop(self):
        """Expected failing assertion:
            self.assertEqual(before[k], after[k], ...)  (gc_meta/gc_tables/packs)
        A "{}" consolidate stepping in while a fresh chain is still marking
        must be a no-op; without the phase gate it builds from half-filled
        marks and lands gc.new_pack/gc.pos/parts rows mid-chain."""
        r = repo("gc-phase-stray")
        th.seed_gc_repo(r)
        ge.knob(r.name, "slice_subreqs", 1)  # mark yields mid-run
        s = ge.step(r.name, "gc_mark")
        self.assertEqual(s["outcome"], "continue", "%r" % s)  # mark mid-run
        # insert a consolidate row exactly the way repair resurrects a dead one
        before = ge.enqueue(r.name, "gc_consolidate", "{}")
        s = ge.step(r.name, "gc_consolidate")
        self.assertTrue(s["ran"])
        self.assertEqual(s["outcome"], "done", "%r" % s)
        after = ge.jobs(r.name)
        for k in ("gc_meta", "gc_tables", "packs"):
            self.assertEqual(before[k], after[k],
                             "stray consolidate wrote to %s:\n%r\n->\n%r"
                             % (k, before[k], after[k]))
        # the real chain completes unaffected
        ge.knob(r.name, "slice_subreqs", None)
        gc_drain(r)
        self.assertTrue(ge.fsck(r.name, "all")["ok"])

    def test_gc_legacy_chain_aborts_instead_of_stranding(self):
        """A chain started before gc.phase/gc.chain shipped (the pre-PR1
        deploy case) is still mid-flight: gc.pos and an ingesting build pack
        exist but there is no phase or chain id. The guards used to Done its
        row and strand that state; now the next GC slice aborts it — gc.*
        wiped, the build pack dead, a fresh gc_mark queued — and a full
        clean cycle then completes."""
        r = repo("gc-legacy-chain")
        # same multi-part build recipe as the C3 wedge test: the consolidate
        # must yield 'continue' mid-build so gc.pos stands
        ge.knob(r.name, "gc_part_bytes", 5 << 20)
        ge.knob(r.name, "slice_subreqs", 1)
        with open(os.path.join(r.dir, "big"), "wb") as f:
            f.write(os.urandom(6 << 20))
        ge.git("add", "big", cwd=r.dir)
        ge.git("commit", "-qm", "big", cwd=r.dir)
        r.push()
        r.commit_file("f", "two\n", "two")
        r.push()
        step_until(r, "gc_mark", {"done"})
        s = ge.step(r.name, "gc_consolidate")
        self.assertTrue(s["ran"])
        self.assertEqual(s["outcome"], "continue",
                         "consolidate was not mid-build: %r" % s)
        v = ge.jobs(r.name)
        self.assertIn("gc.pos", v["gc_meta"], "%r" % v["gc_meta"])
        build_pack = v["gc_meta"]["gc.new_pack"]
        # simulate the pre-PR1 chain: these two fields did not exist yet
        ge.meta(r.name, "gc.phase", None)
        ge.meta(r.name, "gc.chain", None)
        s = ge.step(r.name, "gc_consolidate")
        self.assertTrue(s["ran"])
        self.assertEqual(s["outcome"], "done", "%r" % s)
        v = ge.jobs(r.name)
        self.assertNotIn("gc.pos", v["gc_meta"], "%r" % v["gc_meta"])
        self.assertNotIn("gc.started_at", v["gc_meta"], "%r" % v["gc_meta"])
        packs = {p["id"]: p for p in v["packs"]}
        self.assertEqual(packs[build_pack]["state"], "dead",
                         "build pack left %s" % packs[build_pack]["state"])
        queued = {j["kind"] for j in v["jobs"] if j["state"] == "queued"}
        self.assertIn("gc_mark", queued, "%r" % v["jobs"])
        ge.knob(r.name, "gc_part_bytes", None)
        ge.knob(r.name, "slice_subreqs", None)
        gc_drain(r)
        self.assertTrue(ge.fsck(r.name, "all")["ok"])

    def test_gc_legacy_mid_mark_aborts(self):
        """A chain started before gc.phase/gc.chain shipped caught mid-MARK at
        deploy: gc.started_at stands and the frontier is half-walked, but there
        is no phase and no chain id. The resumed mark used to skip init
        (started_at exists — no young-pack seed, no chain) and finish by
        stamping gc.phase=build over the refs-only marks, then build/sweep ran
        the C2-vulnerable chain on an accepted "{}" payload. Now the mark's own
        legacy_chain guard aborts it first — gc.* wiped, a fresh gc_mark
        queued — and a full clean cycle then completes."""
        r = repo("gc-legacy-mark")
        th.seed_gc_repo(r)
        ge.knob(r.name, "slice_subreqs", 1)  # mark yields mid-walk
        s = ge.step(r.name, "gc_mark")
        self.assertTrue(s["ran"])
        self.assertEqual(s["outcome"], "continue", "mark was not mid-run: %r" % s)
        v = ge.jobs(r.name)
        self.assertIn("gc.started_at", v["gc_meta"], "%r" % v["gc_meta"])
        self.assertGreater(v["gc_tables"]["gc_frontier"]["n"], 0,
                           "%r" % v["gc_tables"])
        # the pre-PR1 chain: no chain id (gc.phase is already absent mid-mark)
        ge.meta(r.name, "gc.chain", None)
        s = ge.step(r.name, "gc_mark")
        self.assertTrue(s["ran"])
        self.assertEqual(s["outcome"], "done", "%r" % s)
        v = ge.jobs(r.name)
        self.assertNotIn("gc.started_at", v["gc_meta"], "%r" % v["gc_meta"])
        self.assertNotIn("gc.chain", v["gc_meta"], "%r" % v["gc_meta"])
        self.assertEqual(v["gc_tables"]["gc_frontier"]["n"], 0,
                         "%r" % v["gc_tables"])
        queued = {j["kind"] for j in v["jobs"] if j["state"] == "queued"}
        self.assertIn("gc_mark", queued, "%r" % v["jobs"])
        ge.knob(r.name, "slice_subreqs", None)
        s = th.drive_until_sweep(r, limit=40)
        dead = [p for p in s["packs"] if p["state"] == "dead"]
        self.assertTrue(dead, "no dead pack after sweep: %r" % s["packs"])
        self.assertTrue(ge.fsck(r.name, "all")["ok"])

    def test_c3_mark_during_multislice_consolidate_does_not_wedge(self):
        """Expected failing assertions:
            self.assertLess(max_attempts, 2, ...)   # retries climb 1,2,...,8
            self.assertFalse(dead, ...)             # the consolidate goes 'dead'
        Today the mid-build consolidate hits a missing gc.refs_version after
        the mark's abort() wiped gc.* — a Storage error — and backs off until
        dead (its last_error, collected in `errs`, is "meta.gc.refs_version
        missing")."""
        r = repo("c3-wedge")
        ge.knob(r.name, "gc_part_bytes", 5 << 20)
        # pull the yield point all the way forward: the first build slice must
        # yield 'continue' mid-build, not race to done inside one budget
        ge.knob(r.name, "slice_subreqs", 1)
        # ~6 MiB of truly random binary so the build spans >= 2 parts — dev R2
        # enforces the real 5 MiB non-final part minimum (as test_j does)
        with open(os.path.join(r.dir, "big"), "wb") as f:
            f.write(os.urandom(6 << 20))
        ge.git("add", "big", cwd=r.dir)
        ge.git("commit", "-qm", "big", cwd=r.dir)
        r.push()  # pack 1: commit + big blob
        r.commit_file("f", "two\n", "two")
        r.push()  # pack 2: two candidate packs, all reachable
        step_until(r, "gc_mark", {"done"})
        s = ge.step(r.name, "gc_consolidate")
        self.assertTrue(s["ran"])
        self.assertEqual(s["outcome"], "continue",
                         "consolidate was not mid-build: %r" % s)
        r.commit_file("f", "three\n", "three")
        r.push()  # refs_version++ and a gc_mark is queued
        s = ge.step(r.name, "gc_mark")
        self.assertTrue(s["ran"])  # today: it abort()s and wipes gc.* mid-build
        # drive the chain: prefer the in-flight consolidate, then sweep, then
        # mark — bounded at 60 steps, tracking attempts/dead/errors as we go
        max_attempts, dead, errs, swept = 0, [], [], False
        for _ in range(60):
            v = ge.jobs(r.name)
            for j in v["jobs"]:
                if not j["kind"].startswith("gc_"):
                    continue
                max_attempts = max(max_attempts, j["attempts"])
                if j["state"] == "dead":
                    dead.append(j["kind"])
                if j["last_error"]:
                    errs.append(j["last_error"])
            queued = {j["kind"] for j in v["jobs"] if j["state"] == "queued"}
            kind = next((k for k in ("gc_consolidate", "gc_sweep", "gc_mark")
                         if k in queued), None)
            if kind is None:
                break
            s = ge.step(r.name, kind)
            if s["job"] and s["job"]["kind"] == "gc_sweep" and s["outcome"] == "done":
                swept = True
                break
        self.assertTrue(swept, "no completed gc_sweep within 60 steps")
        self.assertLess(max_attempts, 2, "attempts grew: %r" % errs)
        self.assertFalse(dead, "gc jobs went dead: %r" % dead)
        self.assertFalse(errs, "gc job errors: %r" % errs)
        self.assertTrue(ge.fsck(r.name, "all")["ok"])

    # -- C7: a slice that lost its lease must not write inside the slice -------

    def test_c7_stale_lease_slice_cannot_write(self):
        """Expected failing assertion:
            self.assertEqual(before, after, ...)
        Today the released stale slice reaches commit_ids before its heartbeat
        CAS, so gc_seen/gc_frontier repopulate after the fresh-lease mark
        emptied them — the digest moves. After the fix the slice is fenced
        before any write and the snapshot is identical."""
        r = repo("c7-stale")
        th.seed_gc_repo(r)
        ge.failpoint(r.name, "gc.mark.after_read", "pause", 1)
        box = {}

        def bg_step():
            try:
                box["s"] = ge.step(r.name, "gc_mark")
            except Exception as e:  # surfaced in the main thread
                box["err"] = e

        t = threading.Thread(target=bg_step, daemon=True)
        t.start()
        # "the slice is inside the pause", deterministically: the armed count
        # shows consumed in the jobs view's failpoints list
        deadline = time.time() + 15
        while time.time() < deadline:
            v = ge.jobs(r.name)
            armed = any(f["name"] == "gc.mark.after_read" and f["action"] == "pause"
                        for f in v.get("failpoints", []))
            consumed = any(f["name"] == "gc.mark.after_read" and f["remaining"] <= 0
                           for f in v.get("failpoints", []))
            running = any(j["kind"] == "gc_mark" and j["state"] == "running"
                          for j in v["jobs"])
            if (consumed or not armed) and running:
                break
            time.sleep(0.1)
        else:
            self.fail("mark slice never entered the pause")
        if not armed:
            self.fail("pause failpoint vanished before release")
        # the running row is stranded-and-requeued exactly like a dead isolate;
        # the next step claims it under a fresh lease and completes the mark
        ge.repair(r.name)
        step_until(r, "gc_mark", {"done"})
        before = ge.jobs(r.name)["gc_tables"]
        ge.failpoint(r.name, "gc.mark.after_read", "off")  # release the pause
        t.join(45)
        self.assertIsNone(box.get("err"), "%r" % box.get("err"))
        s = box.get("s")
        self.assertIsNotNone(s, "paused step never returned")
        self.assertEqual("stale", s["outcome"], "%r" % s)
        after = ge.jobs(r.name)["gc_tables"]
        self.assertEqual(before, after,
                         "stale slice wrote after losing its lease:\n%r\n->\n%r" % (before, after))

    def test_c7_import_stale_slice_cannot_write(self):
        """Expected failing assertion:
            self.assertEqual(before, after, ...)
        resume_drain deletes import_tail/objects rows and JobSink writes
        push_links/import_open/object rows right after awaited R2 calls. A
        slice paused in that window and reclaimed must land none of it —
        the lease CAS must precede those writes (A19)."""
        r = repo("c7-import")
        a = r.commit_file("f", "a\n", "a")
        r.push()
        i = r.commit_file("i", "i\n", "i")  # parent a — its one external link
        listing = ge.git("rev-list", "--objects", i, "--not", a, cwd=r.dir).stdout
        st = ge.import_stage(r.name, ge.pack_objects(r.dir, listing))
        body = ge.import_begin(
            r.name,
            st["push"],
            [{"key": st["key"], "bytes": st["bytes"]}],
            [{"old": ZERO, "new": i, "name": "refs/heads/i"}],
        )
        self.assertIn(b"queued", body)
        s = ge.step(r.name, "import_pack")  # pass A: toc -> phase "resolve"
        self.assertEqual(s["outcome"], "continue", "%r" % s)
        ge.failpoint(r.name, "import.drain.after_create", "pause", 1)
        box = {}

        def bg_step():
            try:
                box["s"] = ge.step(r.name, "import_pack")
            except Exception as e:
                box["err"] = e

        t = threading.Thread(target=bg_step, daemon=True)
        t.start()
        # "the slice is inside the pause", deterministically: consumed arm +
        # running row, same probe as the gc_mark pause above
        deadline = time.time() + 15
        while time.time() < deadline:
            v = ge.jobs(r.name)
            armed = any(f["name"] == "import.drain.after_create" and f["action"] == "pause"
                        for f in v.get("failpoints", []))
            consumed = any(f["name"] == "import.drain.after_create" and f["remaining"] <= 0
                           for f in v.get("failpoints", []))
            running = any(j["kind"] == "import_pack" and j["state"] == "running"
                          for j in v["jobs"])
            if (consumed or not armed) and running:
                break
            time.sleep(0.1)
        else:
            self.fail("import slice never entered the pause")
        if not armed:
            self.fail("pause failpoint vanished before release")
        # stranded-and-requeued like a dead isolate; the new owner claims the
        # row, creates its own MPU and lands the resolve writes
        ge.repair(r.name)
        s = ge.step(r.name, "import_pack")
        self.assertTrue(s["ran"])
        self.assertIn(s["outcome"], ("continue", "done"), "%r" % s)
        before = ge.jobs(r.name)["gc_tables"]
        ge.failpoint(r.name, "import.drain.after_create", "off")  # release
        t.join(45)
        self.assertIsNone(box.get("err"), "%r" % box.get("err"))
        s = box.get("s")
        self.assertIsNotNone(s, "paused step never returned")
        self.assertEqual("stale", s["outcome"], "%r" % s)
        after = ge.jobs(r.name)["gc_tables"]
        self.assertEqual(before, after,
                         "stale import slice wrote after losing its lease:\n%r\n->\n%r" % (before, after))
        # and the import itself was unharmed — the new owner finishes it
        step_until(r, "import_pack", {"done"})
        st2 = ge.import_status(r.name, st["push"])
        self.assertEqual(st2["push_state"], "committed", "%r" % st2)

    def test_c7_import_stale_entry_write(self):
        """Expected failing assertion (with JobSink::fence reverted):
            self.assertEqual(before, after, ...)
        Same lease hand-off as the drain-level test, but the pause sits INSIDE
        one_entry: the entry's R2 read just returned and mark/seal write next,
        so the slice is already past resume_drain's heartbeat CAS — only the
        JobSink fence itself can stop the stale write (A19). The new owner runs
        the import to DONE, not just one more slice: commit_meta deletes
        import_open/push_links/import_parts, so an unfenced stale mark
        repopulates an empty table — digest-visible. On live rows it could
        never discriminate: the stale mark+seal rewrites byte-identical
        values (the transient end=NULL inside mark is repaired by the same
        entry's own seal before the snapshot)."""
        r = repo("c7-import-entry")
        a = r.commit_file("f", "a\n", "a")
        r.push()
        i = r.commit_file("i", "i\n", "i")  # parent a — its one external link
        listing = ge.git("rev-list", "--objects", i, "--not", a, cwd=r.dir).stdout
        st = ge.import_stage(r.name, ge.pack_objects(r.dir, listing))
        body = ge.import_begin(
            r.name,
            st["push"],
            [{"key": st["key"], "bytes": st["bytes"]}],
            [{"old": ZERO, "new": i, "name": "refs/heads/i"}],
        )
        self.assertIn(b"queued", body)
        s = ge.step(r.name, "import_pack")  # pass A: toc -> phase "resolve"
        self.assertEqual(s["outcome"], "continue", "%r" % s)
        ge.failpoint(r.name, "import.entry.after_read", "pause", 1)
        box = {}

        def bg_step():
            try:
                box["s"] = ge.step(r.name, "import_pack")
            except Exception as e:
                box["err"] = e

        t = threading.Thread(target=bg_step, daemon=True)
        t.start()
        # "the slice is inside the pause", deterministically: consumed arm +
        # running row, same probe as the gc_mark pause above
        deadline = time.time() + 15
        while time.time() < deadline:
            v = ge.jobs(r.name)
            armed = any(f["name"] == "import.entry.after_read" and f["action"] == "pause"
                        for f in v.get("failpoints", []))
            consumed = any(f["name"] == "import.entry.after_read" and f["remaining"] <= 0
                           for f in v.get("failpoints", []))
            running = any(j["kind"] == "import_pack" and j["state"] == "running"
                          for j in v["jobs"])
            if (consumed or not armed) and running:
                break
            time.sleep(0.1)
        else:
            self.fail("import slice never entered the pause")
        if not armed:
            self.fail("pause failpoint vanished before release")
        # stranded-and-requeued like a dead isolate; the new owner claims the
        # row and runs the import to completion — commit_meta wipes the
        # staging tables, so a stale slice write after release must land in
        # EMPTY tables where the digest cannot miss it
        ge.repair(r.name)
        step_until(r, "import_pack", {"done"})
        st2 = ge.import_status(r.name, st["push"])
        self.assertEqual(st2["push_state"], "committed", "%r" % st2)
        before = ge.jobs(r.name)["gc_tables"]
        ge.failpoint(r.name, "import.entry.after_read", "off")  # release
        t.join(45)
        self.assertIsNone(box.get("err"), "%r" % box.get("err"))
        s = box.get("s")
        self.assertIsNotNone(s, "paused step never returned")
        self.assertEqual("stale", s["outcome"], "%r" % s)
        after = ge.jobs(r.name)["gc_tables"]
        self.assertEqual(before, after,
                         "stale import slice wrote after losing its lease:\n%r\n->\n%r" % (before, after))


if __name__ == "__main__":
    unittest.main(verbosity=2)
