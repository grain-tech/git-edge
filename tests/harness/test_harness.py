"""PR0 deterministic GC harness suite — proves the test hooks work.

Requires the dev server started with:
  GE_TEST_HOOKS=1 GE_TEST_MANUAL_JOBS=1 GE_GC_QUIET_MS=0 GE_GC_GRACE_MS=0
  GE_RATE_PUSHES_PER_MIN=0
(GE_URL selects it, default http://test:write-test-token@127.0.0.1:8795.)
Deterministic by construction: manual mode means jobs move only when _step is
called — no sleeps, no polling. test_f carries the G2 no-walk-clone regression:
>100 live packs, with duplicate rows straddling the 5,000-row dedup page.
"""

import json
import os
import sys
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ge

OWNER = "harness-%d-%d" % (int(time.time()), os.getpid())


def repo(name):
    return ge.Repo("%s/%s" % (OWNER, name))


def seed_gc_repo(r):
    """Two pushes: pack 1 holds commits one+two, pack 2 the live tip three —
    the abandoned commit two is the garbage a GC cycle must retire."""
    r.commit_file("f", "one\n", "one")
    r.commit_file("f", "two\n", "two")
    r.push()  # pack 1: commits one+two
    ge.git("reset", "--hard", "HEAD~1", cwd=r.dir)  # back to "one"
    tip = r.commit_file("f", "three\n", "three")
    r.push(force=True)  # pack 2: three; "two" is now unreachable
    return tip


def barrier_off(r):
    """Pin the next mark init to flag-off. The knob beats GE_GC_BARRIER, so
    the chain is legacy whatever the server was started with."""
    ge.knob(r.name, "gc_barrier", 0)


def assert_barrier(tc, r, on):
    """The live chain captured the mode its test pinned. Mark init persists
    gc.barrier=1 only when on, so call this while gc.started_at stands."""
    m = ge.jobs(r.name)["gc_meta"]
    tc.assertIn("gc.started_at", m, "no live GC chain to check: %r" % m)
    tc.assertEqual(m.get("gc.barrier"), "1" if on else None,
                   "chain captured the wrong barrier mode: %r" % m)


def drive_until_sweep(r, limit=12):
    """Step (kind-agnostic) until gc_sweep reports done; returns the step dict."""
    for _ in range(limit):
        s = ge.step(r.name)
        if s["job"] and s["job"]["kind"] == "gc_sweep" and s["outcome"] == "done":
            return s
    raise AssertionError("GC chain never reached a completed gc_sweep")


def stage_import(r):
    """The conformance _admin/import flow: stage a local pack, begin the
    import — queues import_pack; check/commit then run under step."""
    r.commit_file("f", "import\n", "import")
    tip = ge.git("rev-parse", "HEAD", cwd=r.dir).stdout.strip()
    st = ge.import_stage(r.name, ge.pack_file(r))
    body = ge.import_begin(
        r.name,
        st["push"],
        [{"key": st["key"], "bytes": st["bytes"]}],
        [{"old": "0" * len(tip), "new": tip, "name": "refs/heads/main"}],
    )
    assert b"queued" in body
    return tip, st["push"]


def step_until(r, kind, outcomes, limit=16, before_each=None):
    for _ in range(limit):
        if before_each is not None:
            before_each()
        s = ge.step(r.name, kind)
        if s["outcome"] in outcomes:
            return s
    raise AssertionError(
        "%s never reached %s in %d steps: %r" % (kind, outcomes, limit, s["jobs"])
    )


class HarnessTests(unittest.TestCase):
    def test_a_hooks_gated(self):
        # (a) Hooks-OFF inertness is tested against a second server by
        # test_hooks_off.py (GE_HOOKS_OFF=1); here the gate is structural —
        # test_forward's env.var check runs BEFORE admin auth and returns the
        # same Error::NotFound as the route table's `_` arm, and
        # failpoint()/manual_jobs() consult enabled() before touching SQL.
        # What this server CAN prove: hooks on + admin auth, exactly like the
        # other _admin routes — unauthenticated is 401, unknown test paths 404.
        st, _ = ge.http("GET", "/x/y/_admin/test/jobs", auth=False)
        self.assertEqual(st, 401)
        st, _ = ge.http("GET", "/x/y/_admin/test/does-not-exist")
        self.assertEqual(st, 404)
        st, _ = ge.http("GET", "/x/y/_admin/does-not-exist")
        self.assertEqual(st, 404)

    def test_b_step_one_slice(self):
        r = repo("b-step")
        seed_gc_repo(r)
        # a repo big enough that a capped slice can't drain the whole frontier,
        # and slice_subreqs=4 so the mark yields Continue mid-walk (knobs are
        # read only with hooks on — set before the job is stepped)
        tip = ""
        for i in range(12):
            tip = r.commit_file("f", "more%d\n" % i, "more%d" % i)
            r.push()
        ge.knob(r.name, "slice_subreqs", 4)
        s = ge.step(r.name, "gc_mark")
        self.assertTrue(s["ran"])
        self.assertEqual(s["job"]["kind"], "gc_mark")
        self.assertEqual(s["outcome"], "continue")  # capped budget forced a yield
        j = next(j for j in s["jobs"] if j["kind"] == "gc_mark")
        self.assertEqual(j["state"], "queued")
        self.assertTrue(j["cursor_present"])
        # a SECOND explicit step is needed — and eventually it ends 'done'
        for _ in range(24):
            s = ge.step(r.name, "gc_mark")
            self.assertTrue(s["ran"])
            if s["outcome"] == "done":
                break
        else:
            self.fail("gc_mark never finished under slice_subreqs=4")
        self.assertIn("gc.started_at", s["gc_meta"])
        kinds = {(j["kind"], j["state"]) for j in s["jobs"]}
        self.assertIn(("gc_consolidate", "queued"), kinds)
        s = drive_until_sweep(r, limit=40)
        dead = [p for p in s["packs"] if p["state"] == "dead"]
        self.assertTrue(dead, "no dead pack after sweep: %r" % s["packs"])
        f = ge.fsck(r.name, "all")
        self.assertTrue(f["ok"], "%r %r" % (f["missing"], f["corrupt"]))
        clone = r.clone()
        self.assertEqual(ge.git("rev-parse", "main", cwd=clone).stdout.strip(), tip)
        ge.git("fsck", "--strict", cwd=clone)

    def test_c_failpoint_error(self):
        r = repo("c-retry")
        seed_gc_repo(r)
        ge.failpoint(r.name, "gc.mark.after_read", "error", 1)
        s = ge.step(r.name, "gc_mark")
        self.assertTrue(s["ran"])
        self.assertEqual(s["outcome"], "retry")
        j = next(j for j in s["jobs"] if j["kind"] == "gc_mark")
        self.assertEqual(j["attempts"], 1)
        self.assertIn("failpoint gc.mark.after_read", j["last_error"] or "")
        # the armed count was consumed — the chain now runs clean
        drive_until_sweep(r)
        self.assertTrue(ge.fsck(r.name, "all")["ok"])

    def test_d_failpoint_crash(self):
        r = repo("d-crash")
        tip = seed_gc_repo(r)
        s = ge.step(r.name, "gc_mark")
        self.assertEqual(s["outcome"], "done")
        # crash between MPU complete and finish_build — reached on any repo,
        # no >=PART-sized build needed
        ge.failpoint(r.name, "gc.consolidate.before_finish", "crash", 1)
        s = ge.step(r.name, "gc_consolidate")
        self.assertTrue(s["ran"])
        self.assertEqual(s["outcome"], "crashed")
        j = next(j for j in s["jobs"] if j["kind"] == "gc_consolidate")
        self.assertEqual(j["state"], "running")  # stranded, like a dead isolate
        v = ge.repair(r.name)
        j = next(j for j in v["jobs"] if j["kind"] == "gc_consolidate")
        self.assertEqual(j["state"], "queued")
        self.assertEqual(j["strands"], 1)
        drive_until_sweep(r)
        self.assertTrue(ge.fsck(r.name, "all")["ok"])
        clone = r.clone()
        self.assertEqual(ge.git("rev-parse", "main", cwd=clone).stdout.strip(), tip)
        ge.git("fsck", "--strict", cwd=clone)

    def test_e_fsck_detects_loss(self):
        r = repo("e-fsck")
        r.commit_file("f", "payload\n", "one")
        r.push()
        blob = ge.git("hash-object", "f", cwd=r.dir).stdout.strip()
        f = ge.fsck(r.name, "all")
        self.assertTrue(f["ok"], f["missing"])
        ge.drop_object(r.name, blob)
        f = ge.fsck(r.name, "refs")
        self.assertFalse(f["ok"])
        self.assertIn(blob, [m["sha"] for m in f["missing"]])

    def test_f_clone_over_101_live_packs(self):
        """G2 regression: a plain clone of a repo with >100 live packs must
        succeed. no_walk_set used to bind live_packs+1 params in one statement
        and trip the DO SQLite 100-param A6 guard -> 500; binding the pack
        list as one json_each parameter must still dedupe a sha's copies
        across packs AND 5,000-row pages."""
        r = repo("f-101packs")
        # Every commit in this test is pinned to one timestamp so the shas —
        # and therefore the row order the server's dedup scan walks — are
        # deterministic; the straddle itself is asserted below, not assumed.
        PIN = {"GIT_AUTHOR_DATE": "1700000000 +0000", "GIT_COMMITTER_DATE": "1700000000 +0000",
               "GIT_CONFIG_GLOBAL": "/dev/null"}
        with open(os.path.join(r.dir, "f"), "w") as fh:
            fh.write("seed\n")
        ge.git("add", "f", cwd=r.dir, env=PIN)
        ge.git("commit", "-qm", "seed", cwd=r.dir, env=PIN)
        r.push()
        ge.git("checkout", "--orphan", "dup", cwd=r.dir)
        ge.git("rm", "-rf", "-q", ".", cwd=r.dir)
        os.mkdir(os.path.join(r.dir, "d"))
        for i in range(1650):
            with open(os.path.join(r.dir, "d", "%04d" % i), "w") as fh:
                fh.write("x%d\n" % i)
        ge.git("add", "d", cwd=r.dir, env=PIN)
        ge.git("commit", "-qm", "dup", cwd=r.dir, env=PIN)
        # 3 live packs with identical objects: deleting the server ref between
        # pushes makes git resend every object (no advertised dup tip to
        # negotiate against). The pinned dates fix every sha, so whether a dup
        # sha's 3-row run straddles the 5,000-row dedup page is deterministic —
        # and asserted below rather than left to modular arithmetic.
        for _ in range(3):
            r.push("HEAD:dup")
            ge.git("push", "-q", r.url, ":dup", cwd=r.dir)
        ge.git("checkout", "-q", "main", cwd=r.dir)
        for i in range(98):
            with open(os.path.join(r.dir, "f"), "w") as fh:
                fh.write("n%d\n" % i)
            ge.git("add", "f", cwd=r.dir, env=PIN)
            ge.git("commit", "-qm", "n%d" % i, cwd=r.dir, env=PIN)
            r.push()
        # Straddle precondition, simulated locally: the dedup scan walks every
        # live objects row ordered by sha, so a dup-reachable sha contributes 3
        # adjacent rows (one per identical pack) and any other sha one. Rows
        # 4,999 and 5,000 must share a sha for a run to cross the page.
        dup = {
            line.split(" ", 1)[0]
            for line in ge.git("rev-list", "--objects", "dup", cwd=r.dir).stdout.splitlines()
        }
        allo = set(
            ge.git("cat-file", "--batch-all-objects",
                   "--batch-check=%(objectname)", cwd=r.dir).stdout.splitlines()
        )
        rows = sorted(sha for sha in allo for _ in range(3 if sha in dup else 1))
        self.assertGreater(len(rows), 5000)
        self.assertEqual(
            rows[4999],
            rows[5000],
            "fixture no longer straddles the 5,000-row page; retune the file count",
        )
        live = [p for p in ge.jobs(r.name)["packs"] if p["state"] == "live"]
        self.assertGreaterEqual(len(live), 101)
        self.assertEqual(len([p for p in live if p["count"] == len(dup)]), 3)
        self.assertEqual(sum(p["count"] for p in live), len(rows))
        target = r.dir + "-clone"
        res = ge.git("-c", "transfer.fsckObjects=true", "clone", "-q",
                     r.url, target, check=False)
        self.assertEqual(res.returncode, 0,
                         "clone failed: %s" % ge.redact(res.stderr + res.stdout))
        ge.git("fsck", "--strict", cwd=target)
        # The clone's single pack must carry each unique sha exactly once:
        # comparing the pack header's emitted count against the unique count
        # catches a duplicate row emitted twice (across packs or across
        # 5,000-row dedup pages) just as it catches a missing one. Every local
        # object was pushed, so the local unique count IS the server's.
        expected = len(ge.git("cat-file", "--batch-all-objects",
                              "--batch-check=%(objectname)", cwd=r.dir)
                       .stdout.splitlines())
        packdir = os.path.join(target, ".git", "objects", "pack")
        packs = [p for p in os.listdir(packdir) if p.endswith(".pack")]
        self.assertEqual(len(packs), 1, packs)
        with open(os.path.join(packdir, packs[0]), "rb") as pf:
            emitted = int.from_bytes(pf.read(12)[8:12], "big")
        self.assertEqual(emitted, expected)

    def test_g_fsck_corrupt_location(self):
        # index rows that lie about their bytes: offset+1 misdecodes, len+1
        # stays inside the pack but the entry under-reads, size+1 inflates
        # short — each must be `corrupt` in BOTH modes
        for action in ("corrupt", "len_plus", "size_plus"):
            with self.subTest(action=action):
                r = repo("g-%s" % action)
                r.commit_file("f", "payload\n", "one")
                r.push()
                blob = ge.git("hash-object", "f", cwd=r.dir).stdout.strip()
                ge.corrupt_object(r.name, blob, action)
                for mode in ("refs", "all"):
                    f = ge.fsck(r.name, mode)
                    self.assertFalse(f["ok"], "%s %s: %r" % (action, mode, f))
                    self.assertIn(
                        blob,
                        [c["sha"] for c in f["corrupt"]],
                        "%s %s: %r" % (action, mode, f),
                    )

    def test_h_fsck_unreachable_live_object(self):
        # unreachable-but-live objects are normal between pushes; a missing one
        # must not trip refs mode but MUST trip all mode (I1 covers them)
        r = repo("h-unreach")
        seed_gc_repo(r)  # "two" commit + its blob are unreachable but live
        blob = ge.git("hash-object", "--stdin", input="two\n").stdout.strip()
        ge.drop_object(r.name, blob)
        f = ge.fsck(r.name, "refs")
        self.assertTrue(f["ok"], "%r %r" % (f["missing"], f["corrupt"]))
        f = ge.fsck(r.name, "all")
        self.assertFalse(f["ok"])
        self.assertIn(blob, [m["sha"] for m in f["missing"]])

    def test_i_fsck_missing_r2_pack(self):
        # drop the pack's R2 object under a live index — every object in it is
        # "missing" to the oracle (HEAD-absent, cached per pack)
        r = repo("i-pack")
        tip = r.commit_file("f", "payload\n", "one")
        r.push()
        packs = [p["id"] for p in ge.jobs(r.name)["packs"] if p["state"] == "live"]
        self.assertEqual(len(packs), 1)
        ge.drop_pack(r.name, packs[0])
        f = ge.fsck(r.name, "refs")
        self.assertFalse(f["ok"])
        self.assertIn(tip, [m["sha"] for m in f["missing"]], "%r" % f)

    def test_j_every_failpoint_crashes_and_repairs(self):
        """Table-driven: every named failpoint crashes the job that owns it —
        row stays 'running', repair requeues it, disarm + steps finish clean."""
        # (failpoint, job kind to step, seed recipe)
        for i, (name, kind, how) in enumerate([
            ("gc.mark.init", "gc_mark", "seed"),
            ("gc.mark.after_read", "gc_mark", "seed"),
            ("gc.mark.after_commit", "gc_mark", "seed"),
            ("gc.consolidate.begin", "gc_consolidate", "consolidate"),
            ("gc.consolidate.after_part", "gc_consolidate", "consolidate_parts"),
            ("gc.consolidate.before_finish", "gc_consolidate", "consolidate"),
            ("gc.sweep.before", "gc_sweep", "presweep"),
            ("import.check.done", "import_pack", "import"),
            ("import.commit.before", "import_pack", "import"),
        ]):
            with self.subTest(failpoint=name):
                self._failpoint_case(i, name, kind, how)

    def test_k_fsck_aliased_index_row(self):
        # an unreachable row repointed at a reachable row's extent IN THE SAME
        # PACK: refs never visits it (ok), all-mode reads the aliased bytes —
        # they decode fine but hash to the other sha -> corrupt
        r = repo("k-alias")
        seed_gc_repo(r)  # pack1: reachable "one\n" blob + unreachable "two\n" blob
        seen_blob = ge.git("hash-object", "--stdin", input="one\n").stdout.strip()
        dead_blob = ge.git("hash-object", "--stdin", input="two\n").stdout.strip()
        ge.admin(
            r.name,
            "POST",
            "drop_object",
            {"sha": dead_blob, "action": "alias", "to": seen_blob},
        )
        f = ge.fsck(r.name, "refs")
        self.assertTrue(f["ok"], "%r %r" % (f["missing"], f["corrupt"]))
        f = ge.fsck(r.name, "all")
        self.assertFalse(f["ok"])
        self.assertIn(dead_blob, [c["sha"] for c in f["corrupt"]], "%r" % f)

    def _failpoint_case(self, i, name, kind, how):
        r = repo("j-%d" % i)
        if how == "import":
            tip, push = stage_import(r)
        else:
            if how == "consolidate_parts":
                # dev R2 enforces the real 5 MiB non-final part minimum — the
                # knob is clamped to it, so the pack needs ~6 MiB of truly
                # random BINARY (hex text deflates ~2x under the bar).
                # Committed BEFORE the seed pushes: it lands in the old pack.
                ge.knob(r.name, "gc_part_bytes", 5 << 20)
                with open(os.path.join(r.dir, "big"), "wb") as f:
                    f.write(os.urandom(6 << 20))
                ge.git("add", "big", cwd=r.dir)
                ge.git("commit", "-qm", "big", cwd=r.dir)
            tip = seed_gc_repo(r)
            if how in ("consolidate", "consolidate_parts", "presweep"):
                step_until(r, "gc_mark", {"done"})
            if how == "presweep":
                step_until(r, "gc_consolidate", {"done"})
        ge.failpoint(r.name, name, "crash", 1)
        # claim-only points never await inside the slice — a real death there
        # rolls the whole span back, so after the crash ONLY the job row may
        # differ from the state IMMEDIATELY before the crashing step (the
        # snapshot refreshes before each stepped slice; the last one counts)
        snap = {}

        def take_snapshot():
            snap["view"] = ge.jobs(r.name)
            snap["refs"] = ge.git("ls-remote", r.url).stdout
            if kind == "import_pack":
                snap["push_state"] = ge.import_status(r.name, push).get("push_state")

        claim_only = name in ("gc.sweep.before", "import.check.done")
        s = step_until(
            r, kind, {"crashed"}, before_each=take_snapshot if claim_only else None
        )
        self.assertIn("failpoint-crash:%s" % name, s["error"] or "")
        if claim_only:
            after = ge.jobs(r.name)
            self.assertEqual(after["packs"], snap["view"]["packs"], "%s moved packs" % name)
            self.assertEqual(
                after.get("gc_meta"), snap["view"].get("gc_meta"), "%s moved gc_meta" % name
            )
            self.assertEqual(
                ge.git("ls-remote", r.url).stdout, snap["refs"], "%s moved refs" % name
            )
            if kind == "import_pack":
                # push_state must not move (began_at legitimately can — skipped)
                st = ge.import_status(r.name, push)
                self.assertEqual(
                    st.get("push_state"), snap["push_state"], "%s moved push_state" % name
                )
        j = next(j for j in s["jobs"] if j["kind"] == kind)
        self.assertEqual(j["state"], "running")  # stranded, like a dead isolate
        v = ge.repair(r.name)
        j = next(j for j in v["jobs"] if j["kind"] == kind)
        self.assertEqual(j["state"], "queued")
        self.assertEqual(j["strands"], 1)
        ge.failpoint(r.name, name, "off")
        if kind == "import_pack":
            step_until(r, kind, {"done"}, limit=100)
            st = ge.import_status(r.name, push)
            self.assertEqual(st.get("push_state"), "committed", "%r" % st)
        else:
            # a crash that only LOOKED healed would strand a later stage —
            # drive the whole gc chain to a completed sweep before verifying
            s = drive_until_sweep(r, limit=100)
            dead = [p for p in s["packs"] if p["state"] == "dead"]
            self.assertTrue(dead, "%r" % s["packs"])
        f = ge.fsck(r.name, "all")
        self.assertTrue(f["ok"], "%r %r" % (f["missing"], f["corrupt"]))
        clone = r.clone()
        self.assertEqual(ge.git("rev-parse", "main", cwd=clone).stdout.strip(), tip)
        ge.git("fsck", "--strict", cwd=clone)



    def test_l_fresh_knob_error_leaves_job_queued(self):
        """A SliceBudget::fresh failure (hooks on, knob read error) must
        leave the job queued, never running — the budget is built before
        the claim."""
        r = repo("fresh-fail")
        before = ge.enqueue(r.name, "janitor")
        queued = [j for j in before["jobs"] if j["state"] == "queued"]
        self.assertTrue(queued, "no queued job to step: %r" % before["jobs"])
        ge.failpoint(r.name, "knob.read", "error", 1)
        st, body = ge.http("POST", "/%s/_admin/test/step" % r.name,
                           {"kind": "janitor"})
        self.assertEqual(st, 500, "step should fail on a knob read error: %s"
                         % ge.redact(body[:400].decode("utf-8", "replace")))
        after = ge.jobs(r.name)
        running = [j for j in after["jobs"] if j["state"] == "running"]
        self.assertEqual(running, [],
                         "fresh() failure left a job running: %r" % running)
        after_by_id = {j["id"]: j for j in after["jobs"]}
        for j in queued:
            a = after_by_id.get(j["id"])
            self.assertIsNotNone(a, "queued job %s vanished" % j["id"])
            self.assertEqual(a["state"], "queued",
                             "job %s was touched: %r" % (j["id"], a))
            self.assertEqual(a["attempts"], j["attempts"],
                             "job %s attempts changed: %r" % (j["id"], a))
            self.assertEqual(a["strands"], j["strands"],
                             "job %s strands changed: %r" % (j["id"], a))


class AdmissionTests(unittest.TestCase):
    """PR3 (G2b): the live-pack admission cap.

    When live+ingesting packs reach `max_live_packs` (hook-gated test knob;
    GE_MAX_LIVE_PACKS env, default 1000), a NEW push begin or import start
    must refuse with HTTP 503 + Retry-After: 60 and write nothing of its own.
    An already-admitted operation (an open push re-begin, a queued import's
    repeated start) is never refused, and a finished janitor re-queues a
    gc_mark while the repo sits at the cap so the refusal can clear."""

    def setUp(self):
        self._repos = []

    def tearDown(self):
        for r in self._repos:
            ge.knob(r.name, "max_live_packs", None)

    def _repo(self, name):
        r = repo(name)
        self._repos.append(r)
        return r

    def live(self, r):
        """Packs counted by the cap: state live or ingesting."""
        return sum(1 for p in ge.jobs(r.name)["packs"] if p["state"] in ("live", "ingesting"))

    def snapshot(self, r):
        """(packs, jobs, pushes, rate_rows, ls-remote) — a refused request
        must not move any of it."""
        v = ge.jobs(r.name)
        packs = sorted((p["id"], p["state"]) for p in v["packs"])
        jobs = sorted((j["kind"], j["state"], j["run_at"]) for j in v["jobs"])
        pushes = sorted((p["id"], p["state"]) for p in v["pushes"])
        return packs, jobs, pushes, v["rate_rows"], ge.git("ls-remote", r.url).stdout

    def test_push_at_cap_503_retry_after_no_state_change(self):
        """Pins: at the cap, receive-pack is refused at push_begin with a real
        HTTP 503 + Retry-After: 60 — before even the rate row is written — so
        packs/jobs/refs are identical after a raw POST and after `git push`."""
        r = self._repo("push-at-cap")
        seed_gc_repo(r)
        ge.knob(r.name, "max_live_packs", self.live(r))
        tip = r.commit_file("f", "capped\n", "capped")
        cmd = ("%s %s refs/heads/capped\0report-status object-format=sha1\n" % ("0" * 40, tip)).encode()
        body = b"%04x" % (len(cmd) + 4) + cmd + b"0000" + ge.pack_file(r)
        snap = self.snapshot(r)
        st, hdrs, _ = ge.http_h(
            "POST",
            "/%s/git-receive-pack" % r.name,
            raw=body,
            content_type="application/x-git-receive-pack-request",
        )
        self.assertEqual(st, 503)
        self.assertEqual(hdrs.get("retry-after"), "60")
        self.assertEqual(self.snapshot(r), snap)
        res = ge.git("push", r.url, "HEAD:refs/heads/capped2", cwd=r.dir, check=False)
        self.assertNotEqual(res.returncode, 0)
        self.assertIn("HTTP 503", ge.redact(res.stderr + res.stdout))
        self.assertEqual(self.snapshot(r), snap)

    def test_import_at_cap_503_no_state_change(self):
        """Pins: import start is capped like push begin — with the pack staged
        BEFORE the cap is set, the _admin/import begin gets 503 + Retry-After;
        no import_pack job is queued and no pack row appears."""
        r = self._repo("import-at-cap")
        r.commit_file("f", "seed\n", "seed")
        r.push()
        tip = r.commit_file("f", "import\n", "import")
        st = ge.import_stage(r.name, ge.pack_file(r))  # staged before the cap
        ge.knob(r.name, "max_live_packs", self.live(r))
        snap = self.snapshot(r)
        body = json.dumps(
            {
                "push": st["push"],
                "parts": [{"key": st["key"], "bytes": st["bytes"]}],
                "commands": [{"old": "0" * len(tip), "new": tip, "name": "refs/heads/main"}],
            }
        ).encode()
        code, hdrs, _ = ge.http_h(
            "POST",
            "/%s/_admin/import" % r.name,
            raw=body,
            content_type="application/json",
        )
        self.assertEqual(code, 503)
        self.assertEqual(hdrs.get("retry-after"), "60")
        self.assertEqual(self.snapshot(r), snap)

    def test_below_cap_push_succeeds(self):
        """Pins the lower edge: one slot of headroom lets a push through and
        adds exactly one live pack."""
        r = self._repo("below-cap")
        seed_gc_repo(r)
        n = self.live(r)
        ge.knob(r.name, "max_live_packs", n + 1)
        r.commit_file("f", "more\n", "more")
        r.push()
        self.assertEqual(self.live(r), n + 1)

    def test_delete_only_push_at_cap_refused(self):
        """The delete-only exemption is gone (H13): at the cap even an
        all-delete push gets the same 503 — the ref stays, nothing moves."""
        r = self._repo("delete-at-cap")
        r.commit_file("f", "seed\n", "seed")
        r.push()
        r.push("HEAD:refs/heads/side")
        ge.knob(r.name, "max_live_packs", self.live(r))
        snap = self.snapshot(r)
        res = ge.git("push", r.url, ":refs/heads/side", cwd=r.dir, check=False)
        self.assertNotEqual(res.returncode, 0)
        self.assertIn("HTTP 503", ge.redact(res.stderr + res.stdout))
        self.assertIn("refs/heads/side", ge.git("ls-remote", r.url).stdout)
        self.assertEqual(self.snapshot(r), snap)

    def test_janitor_requeues_gc_when_capped(self):
        """H12: a capped repo writes nothing on refusal, so nothing else would
        ever queue the GC that recovers it — a finished janitor must enqueue a
        gc_mark while the count sits at/above the cap with none in flight."""
        r = self._repo("janitor-requeue")
        r.commit_file("f", "seed\n", "seed")
        r.push()  # the ref commit queued a gc_mark
        v = {}
        for _ in range(40):
            v = ge.jobs(r.name)
            if not any(
                j["kind"].startswith("gc_") and j["state"] in ("queued", "running")
                for j in v["jobs"]
            ):
                break
            ge.step(r.name)
        else:
            self.fail("gc_* jobs never drained: %r" % v["jobs"])
        # uncapped, a finished janitor must not queue a gc_mark — the requeue
        # is gated on at_pack_cap
        step_until(r, "janitor", {"done"}, limit=8)
        self.assertFalse(any(
            j["kind"] == "gc_mark" and j["state"] in ("queued", "running")
            for j in ge.jobs(r.name)["jobs"]))
        # cap = 1: one live pack survives any GC, so the repo stays at the cap
        ge.knob(r.name, "max_live_packs", 1)
        self.assertFalse(any(
            j["kind"] == "gc_mark" and j["state"] in ("queued", "running")
            for j in ge.jobs(r.name)["jobs"]))
        step_until(r, "janitor", {"done"}, limit=8)
        self.assertTrue(any(
            j["kind"] == "gc_mark" and j["state"] == "queued"
            for j in ge.jobs(r.name)["jobs"]))

    def test_rebegin_open_staged_push_at_cap_succeeds(self):
        """I1: every part of a staged import re-begins ONE open push. A
        re-begin is a no-op on an already-admitted push, so it passes even at
        the cap — admission applies only to a NEW push row."""
        r = self._repo("rebegin-at-cap")
        r.commit_file("f", "seed\n", "seed")
        r.push()  # one live pack, so live(r) is a real cap
        st = ge.import_stage(r.name, ge.pack_file(r))  # first part, uncapped
        ge.knob(r.name, "max_live_packs", self.live(r))
        code, _, _ = ge.http_h(
            "POST",
            "/%s/_admin/import/stage?push=%s&part=p2" % (r.name, st["push"]),
            raw=ge.pack_file(r),
        )
        self.assertEqual(code, 200)

    def test_ingesting_pack_counts_toward_cap(self):
        """An 'ingesting' pack consumes cap capacity: a staged import's planted
        pack row (manual mode keeps it 'ingesting' — the job never runs unless
        stepped) counts toward live+ingesting, so a new push is refused."""
        r = self._repo("ingesting-counts")
        stage_import(r)  # stage + begin while uncapped
        v = ge.jobs(r.name)
        self.assertEqual(
            sum(1 for p in v["packs"] if p["state"] == "ingesting"),
            1,
            "%r" % v["packs"],
        )
        ge.knob(r.name, "max_live_packs", self.live(r))  # counts the ingesting row
        res = ge.git("push", r.url, "HEAD:refs/heads/side", cwd=r.dir, check=False)
        self.assertNotEqual(res.returncode, 0)
        self.assertIn("HTTP 503", ge.redact(res.stderr + res.stdout))

    def test_stage_at_cap_503(self):
        """The stage route's push/begin is capped too: a raw POST to
        _admin/import/stage at the cap is 503 + Retry-After: 60."""
        r = self._repo("stage-at-cap")
        r.commit_file("f", "seed\n", "seed")
        r.push()
        ge.knob(r.name, "max_live_packs", self.live(r))
        code, hdrs, _ = ge.http_h("POST", "/%s/_admin/import/stage" % r.name, raw=b"pack")
        self.assertEqual(code, 503)
        self.assertEqual(hdrs.get("retry-after"), "60")

    def test_large_push_at_cap_503(self):
        """A push above http.postBuffer is streamed chunked — the 503 must
        still reach git (the status is answered before the upload drains)."""
        r = self._repo("large-at-cap")
        r.commit_file("f", "seed\n", "seed")
        r.push()
        with open(os.path.join(r.dir, "big"), "wb") as f:
            f.write(os.urandom(64 << 10))  # incompressible — the pack can't shrink
        ge.git("add", "big", cwd=r.dir)
        ge.git("commit", "-qm", "big", cwd=r.dir)
        ge.knob(r.name, "max_live_packs", self.live(r))
        res = ge.git(
            "-c", "http.postBuffer=1024",
            "push", r.url, "HEAD:refs/heads/big",
            cwd=r.dir, check=False,
        )
        self.assertNotEqual(res.returncode, 0)
        self.assertIn("HTTP 503", ge.redact(res.stderr + res.stdout))

    def test_recovers_after_gc_drops_count(self):
        """Pins that the cap is a count, not a ban: a push refused at the cap
        succeeds unchanged once a GC cycle sweeps the dead pack below it, and
        the fsck oracle stays ok."""
        r = self._repo("cap-recovers")
        seed_gc_repo(r)  # 2 live packs; commit "two" is the garbage
        cap = self.live(r)
        ge.knob(r.name, "max_live_packs", cap)
        r.commit_file("f", "new\n", "new")
        res = ge.git("push", r.url, "HEAD:main", cwd=r.dir, check=False)
        self.assertNotEqual(res.returncode, 0)
        self.assertIn("HTTP 503", ge.redact(res.stderr + res.stdout))
        drive_until_sweep(r, limit=40)
        self.assertLess(self.live(r), cap)
        r.push()
        f = ge.fsck(r.name, "all")
        self.assertTrue(f["ok"], "%r %r" % (f["missing"], f["corrupt"]))


if __name__ == "__main__":
    unittest.main(verbosity=2)
