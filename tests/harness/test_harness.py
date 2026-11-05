"""PR0 deterministic GC harness suite — proves the test hooks work.

Requires the dev server started with:
  GE_TEST_HOOKS=1 GE_TEST_MANUAL_JOBS=1 GE_GC_QUIET_MS=0 GE_GC_GRACE_MS=0
  GE_RATE_PUSHES_PER_MIN=0
(GE_URL selects it, default http://test:write-test-token@127.0.0.1:8795.)
Deterministic by construction: manual mode means jobs move only when _step is
called — no sleeps, no polling.
"""

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


def drive_until_sweep(r, limit=12):
    """Step (kind-agnostic) until gc_sweep reports done; returns the step dict."""
    for _ in range(limit):
        s = ge.step(r.name)
        if s["job"] and s["job"]["kind"] == "gc_sweep" and s["outcome"] == "done":
            return s
    raise AssertionError("GC chain never reached a completed gc_sweep")


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

    def test_f_a6_guard_rejects_101_param_clone(self):
        """Pins today's behaviour: a plain clone of a repo with >= 100 live
        packs makes no_walk_set bind live_packs+1 params, the A6 guard rejects
        the statement, and the clone fails with the "sql: N bound params > 100
        (A6)" line in the server log. When G2 (walk-set chunking) lands, flip
        this test to assert a successful clone + git fsck instead."""
        log = os.environ.get("GE_SERVER_LOG")
        if not log or not os.path.isfile(log):
            self.fail("GE_SERVER_LOG must point at the dev server's log "
                      "(run.sh exports it, default /tmp/ge-dev.log)")
        r = repo("f-100packs")
        r.commit_file("f", "seed\n", "seed")
        r.push()
        for i in range(100):
            r.commit_file("f", "n%d\n" % i, "n%d" % i)
            r.push()
        v = ge.jobs(r.name)
        live = [p for p in v["packs"] if p["state"] == "live"]
        self.assertGreaterEqual(len(live), 100)
        log_off = os.path.getsize(log)  # only log written after THIS point counts
        res = ge.git("clone", "-q", r.url, r.dir + "-clone", check=False)
        self.assertNotEqual(res.returncode, 0,
                            "clone succeeded — did G2 land? flip this test")
        self.assertIn("500", ge.redact(res.stderr + res.stdout))
        # wrangler flushes its log asynchronously — the guard line lands a few
        # hundred ms after clone returns. Polls on log I/O, not job timing.
        deadline = time.time() + 10
        new = ""
        while time.time() < deadline:
            with open(log, errors="replace") as f:
                f.seek(log_off)
                new = f.read()
            if "bound params" in new:
                break
            time.sleep(0.2)
        else:
            self.fail("no 'bound params' guard line in new server log text: %s"
                      % new[-2048:])

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
            tip, push = self._stage_import(r)
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
                self._step_until(r, "gc_mark", {"done"})
            if how == "presweep":
                self._step_until(r, "gc_consolidate", {"done"})
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
        s = self._step_until(
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
            self._step_until(r, kind, {"done"}, limit=100)
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

    def _stage_import(self, r):
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
        self.assertIn(b"queued", body)
        return tip, st["push"]

    def _step_until(self, r, kind, outcomes, limit=16, before_each=None):
        for _ in range(limit):
            if before_each is not None:
                before_each()
            s = ge.step(r.name, kind)
            if s["outcome"] in outcomes:
                return s
        raise AssertionError(
            "%s never reached %s in %d steps: %r" % (kind, outcomes, limit, s["jobs"])
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
