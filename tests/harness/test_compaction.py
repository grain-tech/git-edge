"""PR4 phase A — RED tests for G4a lossless tiered compaction
(design/pr4-compaction.md; §14 supersedes §13 supersedes §12 supersedes
§2–§9). Every test must FAIL on unfixed code; each docstring names the
exact assertion expected to fail and why. On current code the first red
is always an AssertionError out of ge.admin: step/enqueue kind
"gc_compact" answers "unknown job kind" (non-200), and the new hooks —
GET objects, pack_check, pack_state, kill_job — answer 404. Phase B lands
the gc_compact kind, the "compact" mode of the PR1 chain, and the hooks
against these oracles. Same hooks-on dev server as test_harness.py
(README.md). Later tests added for mutation coverage name the mutant each
one kills in its docstring.
"""

import itertools
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

ZERO = "0" * 40
_INV = itertools.count()
OWNER = th.OWNER + '-cmp'


def repo(name):
    return ge.Repo('%s/%s' % (OWNER, name))


def small_packs(r, n):
    """n commits, one push each, each writing a distinct file -> n live
    packs of a few objects apiece (the lowest object-count tier)."""
    tip = None
    for i in range(n):
        tip = r.commit_file("f%d" % i, "%d\n" % i, "c%d" % i)
        r.push()
    return tip


def live_packs(r):
    """Ids of the live packs."""
    return [p["id"] for p in ge.jobs(r.name)["packs"] if p["state"] == "live"]


def queued_kinds(r):
    return {j["kind"] for j in ge.jobs(r.name)["jobs"] if j["state"] == "queued"}


def drain_compact(r, limit=80):
    """Step gc_compact/gc_consolidate until neither is queued — consolidate
    first, since a queued gc_compact reschedules behind a live build. A
    stranded 'running' row (a crash step) is repaired back to queued."""
    for _ in range(limit):
        v = ge.jobs(r.name)
        rows = [j for j in v["jobs"]
                if j["kind"] in ("gc_consolidate", "gc_compact")]
        if any(j["state"] == "running" for j in rows):
            ge.repair(r.name)
            continue
        kind = next(
            (k for k in ("gc_consolidate", "gc_compact")
             if any(j["kind"] == k and j["state"] == "queued" for j in rows)),
            None,
        )
        if kind is None:
            return
        ge.step(r.name, kind)
    raise AssertionError("compaction never drained in %d steps" % limit)


def snapshot(r):
    """{shas, gc_epoch, refs_version} — the lossless-compaction oracle.
    gc_epoch and refs_version are plain meta rows already in the jobs view;
    shas needs the new GET objects hook."""
    v = ge.jobs(r.name)
    return {
        "shas": sorted(ge.live_objects(r.name)["shas"]),
        "gc_epoch": v["gc_meta"].get("gc_epoch"),
        "refs_version": v["gc_meta"].get("refs_version"),
    }


def invariants(r, before):
    """The §7 end-state oracle: the live SHA set is byte-identical, fsck
    mode=all is clean, a fresh clone passes git fsck --strict, gc_epoch is
    unmoved, and refs_version is unmoved when the snapshot carries it
    (tests that push mid-compaction drop the key — pushes legitimately
    bump refs_version). Returns the fresh clone's path."""
    after = ge.live_objects(r.name)
    if after["dead_rows"] != 0:
        raise AssertionError(
            "objects rows under dead packs leaked: %r" % after["dead_rows"])
    missing = sorted(set(before["shas"]) - set(after["shas"]))
    extra = sorted(set(after["shas"]) - set(before["shas"]))
    if missing or extra:
        raise AssertionError("live SHA set changed: -%r +%r" % (missing, extra))
    f = ge.fsck(r.name, "all")
    if not f["ok"]:
        raise AssertionError("fsck not clean: %r" % f)
    clone = r.clone(suffix="-inv-%d" % next(_INV))
    ge.git("fsck", "--strict", cwd=clone)
    meta = ge.jobs(r.name)["gc_meta"]
    if meta.get("gc_epoch") != before["gc_epoch"]:
        raise AssertionError(
            "gc_epoch moved: %r -> %r" % (before["gc_epoch"], meta.get("gc_epoch")))
    if "refs_version" in before and meta.get("refs_version") != before["refs_version"]:
        raise AssertionError(
            "refs_version moved: %r -> %r"
            % (before["refs_version"], meta.get("refs_version")))
    return clone


class CompactionTests(unittest.TestCase):
    def repo(self, name):
        """A scratch repo with the compact trigger live: min_age 0 makes a
        pack eligible the moment it lands, fanin 4 makes >= 4 same-tier
        eligible packs enqueue gc_compact on the push path."""
        r = repo(name)
        for k, v in (("compact_min_age_ms", 0), ("compact_fanin", 4)):
            ge.knob(r.name, k, v)
            self.addCleanup(ge.knob, r.name, k, None)
        return r

    def assert_push_ok(self, st, body, ref):
        """A report-status body on a fully accepted push."""
        self.assertEqual(st, 200, "%r" % ge.redact(body[:400].decode("utf-8", "replace")))
        self.assertIn(b"unpack ok", body)
        self.assertIn(b"ok " + ref.encode(), body)
        self.assertNotIn(b"ng ", body)

    def wait_in_pause(self, r, name, kind):
        """"The slice/request is parked", deterministically — the probe from
        test_gc_safety's C7 waits: the armed pause row shows consumed
        (remaining<=0), plus a 'running' job row when the pause is inside a
        job slice (kind=None for an in-flight fetch). Returns the jobs view
        seen at the pause."""
        deadline = time.time() + 15
        while time.time() < deadline:
            v = ge.jobs(r.name)
            armed = any(f["name"] == name and f["action"] == "pause"
                        for f in v.get("failpoints", []))
            consumed = any(f["name"] == name and f["remaining"] <= 0
                           for f in v.get("failpoints", []))
            running = kind is None or any(
                j["kind"] == kind and j["state"] == "running" for j in v["jobs"])
            if (consumed or not armed) and running:
                return v
            time.sleep(0.1)
        self.fail("%s never entered the pause" % name)

    def bg_step(self, r, kind, box):
        """ge.step in a daemon thread; result lands in box["s"], any harness
        error in box["err"]."""
        def run():
            try:
                box["s"] = ge.step(r.name, kind)
            except Exception as e:
                box["err"] = e
        t = threading.Thread(target=run, daemon=True)
        t.start()
        return t

    # -- 1. trigger + lossless end state ------------------------------------

    def test_trigger_and_lossless_compaction(self):
        """Expected failing assertion:
            self.assertTrue(any gc_compact row is queued)
        Current code has no trigger and no such kind, so no row is ever
        queued; failing that, snapshot() dies in ge.admin (GET objects ->
        404). Also kills created_at_future: the swap must stamp the build
        pack with MAX(cohort.created_at) — recorded off the marked rows
        right after init — not the swap's own now()."""
        r = self.repo("cmp-basic")
        small_packs(r, 6)
        v = ge.jobs(r.name)
        self.assertTrue(
            any(j["kind"] == "gc_compact" and j["state"] == "queued"
                for j in v["jobs"]),
            "no gc_compact queued with >= T eligible packs: %r" % v["jobs"])
        before = snapshot(r)
        s = ge.step(r.name, "gc_compact")  # init: the cohort is marked now
        self.assertTrue(s["ran"], "%r" % s)
        marked = [p for p in ge.jobs(r.name)["packs"] if p["marked"]]
        self.assertTrue(marked,
                        "init marked no cohort: %r" % ge.jobs(r.name)["packs"])
        cohort_created = max(p["created_at"] for p in marked)
        drain_compact(r)
        packs = ge.jobs(r.name)["packs"]
        live = [p for p in packs if p["state"] == "live"]
        self.assertLess(len(live), 6, "cohort was not merged: %r" % packs)
        new = [p for p in live if p["push_id_null"]]
        self.assertTrue(new, "no built pack went live: %r" % packs)
        self.assertEqual(cohort_created, new[0]["created_at"],
                         "created_at is not MAX(cohort): %r" % packs)
        invariants(r, before)
        chk = ge.pack_check(r.name, new[0]["id"])
        self.assertEqual(chk["header_count"], chk["decoded"], "%r" % chk)
        self.assertEqual(chk["decoded"], chk["index_rows"], "%r" % chk)
        self.assertEqual(chk["index_rows"], chk["distinct"], "%r" % chk)
        # the packs-row span agrees with the span decoded off the pack bytes
        self.assertEqual(
            (chk["commit_lo"], chk["commit_hi"]),
            (new[0]["commit_lo"], new[0]["commit_hi"]),
            "packs-row commit span != decoded pack span: %r vs %r"
            % (new[0], chk))

    def test_dedup_across_cohort(self):
        """Expected failing assertion:
            AssertionError from ge.admin inside snapshot() — GET objects ->
            404 on current code; the drain then 400s on the unknown kind
            "gc_compact"."""
        r = self.repo("cmp-dedup")
        r.commit_file("x", "same\n", "a")
        r.push()  # P1 holds the "same" blob
        blob_x = ge.git("rev-parse", "HEAD:x", cwd=r.dir).stdout.strip()
        # a second pack carrying the IDENTICAL blob — an explicit
        # pack_objects listing, since git's thin push would omit it
        ge.git("checkout", "-q", "-b", "dup", cwd=r.dir)
        b = r.commit_file("y", "y\n", "b")
        blob_y = ge.git("rev-parse", "HEAD:y", cwd=r.dir).stdout.strip()
        tree_b = ge.git("rev-parse", "HEAD^{tree}", cwd=r.dir).stdout.strip()
        st, body = ge.receive_pack(
            r.name,
            [(ZERO, b, "refs/heads/dup")],
            ge.pack_objects(r.dir, [b, tree_b, blob_y, blob_x]),
        )
        self.assert_push_ok(st, body, "refs/heads/dup")  # P2 also holds blob_x
        ge.git("checkout", "-q", "main", cwd=r.dir)
        small_packs(r, 4)
        before = snapshot(r)
        drain_compact(r)
        new = [p for p in ge.jobs(r.name)["packs"]
               if p["state"] == "live" and p["push_id_null"]]
        self.assertTrue(new, "no built pack went live")
        chk = ge.pack_check(r.name, new[0]["id"])
        # blob_x was in two cohort packs: the build copies it once, so the
        # header promise, the decoded entries, the index rows and the
        # distinct SHAs are all the same number
        self.assertEqual(chk["distinct"], chk["index_rows"], "%r" % chk)
        self.assertEqual(chk["index_rows"], chk["header_count"], "%r" % chk)
        self.assertEqual(chk["header_count"], chk["decoded"], "%r" % chk)
        invariants(r, before)

    def test_young_packs_ineligible(self):
        """Expected failing assertion:
            AssertionError from ge.admin inside snapshot() — GET objects ->
            404 on current code. (With the age gate missing there is also
            nothing to step and nothing to discriminate, so the oracle has
            to come from the hook.)"""
        r = self.repo("cmp-young")
        ge.knob(r.name, "compact_min_age_ms", 3_600_000)
        small_packs(r, 6)
        before = snapshot(r)
        if "gc_compact" in queued_kinds(r):
            ge.step(r.name, "gc_compact")
        v = ge.jobs(r.name)
        self.assertEqual(len(live_packs(r)), 6,
                         "a young pack died: %r" % v["packs"])
        self.assertFalse(any(p["push_id_null"] for p in v["packs"]),
                         "a build pack exists: %r" % v["packs"])
        self.assertNotIn("gc.mode", v["gc_meta"], "%r" % v["gc_meta"])
        invariants(r, before)

    def test_crash_at_each_step(self):
        """Expected failing assertion (per subTest):
            AssertionError from ge.admin inside snapshot() — GET objects ->
            404 on current code; on a partial implementation the next red
            is the unknown kind "gc_compact" or an unarmed failpoint."""
        for i, (name, action) in enumerate([
            ("gc.compact.init", "crash"),
            ("gc.consolidate.begin", "crash"),
            ("gc.consolidate.before_finish", "crash"),
            ("gc.compact.before_swap", "crash"),
            ("gc.compact.before_swap", "error"),
        ]):
            with self.subTest(failpoint=name, action=action):
                r = self.repo("cmp-crash-%d" % i)
                small_packs(r, 6)
                before = snapshot(r)
                ge.failpoint(r.name, name, action, 1)
                for _ in range(80):
                    v = ge.jobs(r.name)
                    rows = [j for j in v["jobs"]
                            if j["kind"] in ("gc_consolidate", "gc_compact")]
                    if any(j["state"] == "running" for j in rows):
                        ge.repair(r.name)  # stranded by the crash: requeue
                        continue
                    kind = next(
                        (k for k in ("gc_consolidate", "gc_compact")
                         if any(j["kind"] == k and j["state"] == "queued"
                                for j in rows)),
                        None,
                    )
                    if kind is None:
                        break
                    s = ge.step(r.name, kind)
                    if name == "gc.compact.init" and s.get("outcome") == "crashed":
                        # the crash landed post-commit: the chain is already
                        # staked and its builder queued — only the init row is
                        # stranded 'running'
                        self.assertTrue(
                            s["gc_meta"].get("gc.chain"),
                            "init committed no chain: %r" % s["gc_meta"])
                        self.assertEqual(
                            "build", s["gc_meta"].get("gc.phase"),
                            "%r" % s["gc_meta"])
                else:
                    self.fail("compaction never drained after %s" % name)
                dead = [j for j in ge.jobs(r.name)["jobs"] if j["state"] == "dead"]
                self.assertFalse(dead, "jobs went dead: %r" % dead)
                self.assertLess(len(live_packs(r)), 6,
                                "compaction never completed after %s" % name)
                invariants(r, before)
                # the recovered build pack's header promise, decoded entries,
                # index rows and distinct SHAs all agree (covers the HEAD
                # recovery the before_finish crash drives)
                new = [p for p in ge.jobs(r.name)["packs"]
                       if p["state"] == "live" and p["push_id_null"]]
                self.assertTrue(new, "no build pack went live after %s" % name)
                chk = ge.pack_check(r.name, new[0]["id"])
                self.assertEqual(chk["header_count"], chk["decoded"], "%r" % chk)
                self.assertEqual(chk["decoded"], chk["index_rows"], "%r" % chk)
                self.assertEqual(chk["index_rows"], chk["distinct"], "%r" % chk)

    def test_head_recovery_commit_span(self):
        """Regression of the HEAD-recovery path and its span: crash at
        gc.consolidate.before_finish after complete() won, repair requeues
        the builder, the resumed finish() errors on the consumed MPU and
        HEAD recovery publishes the pack. Its packs-row commit_lo/commit_hi
        must equal the span pack_check decodes off the R2 bytes."""
        blobs = [os.urandom(1200 << 10) for _ in range(6)]

        def seeded(name):
            r = self.repo(name)
            ge.knob(r.name, "gc_part_bytes", 5 << 20)  # the clamp minimum
            self.addCleanup(ge.knob, r.name, "gc_part_bytes", None)
            ge.knob(r.name, "slice_subreqs", 1)  # yield mid-build
            self.addCleanup(ge.knob, r.name, "slice_subreqs", None)
            return r

        r = seeded("cmp-hr-span")
        # one history: the seed pack is 10 objects (a higher tier, never in
        # the cohort) and each later push is one commit+tree+~1.2 MiB blob
        for i in range(8):
            with open(os.path.join(r.dir, "s%d" % i), "w") as f:
                f.write("seed%d\n" % i)
        ge.git("add", ".", cwd=r.dir)
        ge.git("commit", "-qm", "seed", cwd=r.dir)
        shas = [ge.git("rev-parse", "HEAD", cwd=r.dir).stdout.strip()]
        for i, blob in enumerate(blobs):
            with open(os.path.join(r.dir, "b"), "wb") as f:
                f.write(blob)
            ge.git("add", "b", cwd=r.dir)
            ge.git("commit", "-qm", "b%d" % i, cwd=r.dir)
            shas.append(ge.git("rev-parse", "HEAD", cwd=r.dir).stdout.strip())
        for sha in shas:
            ge.git("push", "-q", r.url, "%s:refs/heads/main" % sha, cwd=r.dir)

        s = ge.step(r.name, "gc_compact")  # init: cohort marked, build queued
        self.assertTrue(s["ran"], "%r" % s)
        ge.failpoint(r.name, "gc.consolidate.before_finish", "crash", 1)
        saw_crash = False
        for _ in range(80):
            v = ge.jobs(r.name)
            rows = [j for j in v["jobs"]
                    if j["kind"] in ("gc_consolidate", "gc_compact")]
            if any(j["state"] == "running" for j in rows):
                ge.repair(r.name)  # stranded by the crash: requeue
                continue
            kind = next(
                (k for k in ("gc_consolidate", "gc_compact")
                 if any(j["kind"] == k and j["state"] == "queued"
                        for j in rows)),
                None,
            )
            if kind is None:
                break
            s = ge.step(r.name, kind)
            saw_crash = saw_crash or s["outcome"] == "crashed"
        else:
            self.fail("compaction never drained after before_finish")
        self.assertTrue(saw_crash, "before_finish crash never fired")
        packs = ge.jobs(r.name)["packs"]
        new = [p for p in packs if p["state"] == "live" and p["push_id_null"]]
        self.assertEqual(1, len(new), "no HEAD-recovered pack: %r" % packs)
        self.assertGreater(new[0]["bytes"], 5 << 20,
                           "build was not multi-part: %r" % new[0])

        self.assertLess(new[0]["commit_lo"], new[0]["commit_hi"],
                        "inverted/empty span: %r" % new[0])
        # the pack bytes are the order-independent oracle: the packs row's
        # span must equal the span decoded off the R2 object itself — no
        # reference repo, no dependence on cohort ORDER BY pack_id or entry
        # compressed sizes
        chk = ge.pack_check(r.name, new[0]["id"])
        self.assertEqual(
            (chk["commit_lo"], chk["commit_hi"]),
            (new[0]["commit_lo"], new[0]["commit_hi"]),
            "packs-row commit span != decoded pack span: %r vs %r"
            % (new[0], chk))
        # the recovered pack carries the same index truth the span came from
        self.assertEqual(chk["header_count"], chk["decoded"], "%r" % chk)
        self.assertEqual(chk["decoded"], chk["index_rows"], "%r" % chk)
        self.assertEqual(chk["index_rows"], chk["distinct"], "%r" % chk)

    def test_push_between_every_slice(self):
        """Expected failing assertion:
            AssertionError from ge.admin inside snapshot() — GET objects ->
            404 on current code (the drive below needs the gc_compact kind
            that does not exist there)."""
        r = self.repo("cmp-push-slices")
        ge.knob(r.name, "gc_part_bytes", 5 << 20)  # the clamp minimum
        self.addCleanup(ge.knob, r.name, "gc_part_bytes", None)
        ge.knob(r.name, "slice_subreqs", 1)  # yield mid-build, like C3's wedge
        self.addCleanup(ge.knob, r.name, "slice_subreqs", None)
        # six packs of ~1.2 MiB random blobs: lowest-tier by object count,
        # ~7 MiB across the cohort, so the build spans several MPU parts
        pushed = []
        for i in range(6):
            with open(os.path.join(r.dir, "b%d" % i), "wb") as f:
                f.write(os.urandom(1200 << 10))
            ge.git("add", "b%d" % i, cwd=r.dir)
            ge.git("commit", "-qm", "b%d" % i, cwd=r.dir)
            pushed.append(ge.git("rev-parse", "HEAD", cwd=r.dir).stdout.strip())
            r.push()
        before = snapshot(r)
        before.pop("refs_version")  # every push below legitimately bumps it
        saw_done = False
        for _ in range(80):
            v = ge.jobs(r.name)
            rows = [j for j in v["jobs"]
                    if j["kind"] in ("gc_consolidate", "gc_compact")]
            if any(j["state"] == "running" for j in rows):
                ge.repair(r.name)
                continue
            kind = next(
                (k for k in ("gc_consolidate", "gc_compact")
                 if any(j["kind"] == k and j["state"] == "queued" for j in rows)),
                None,
            )
            if kind is None or saw_done:
                break
            s = ge.step(r.name, kind)
            # one push after EVERY compaction slice — compaction has no ref
            # fence, so the push must land (rc 0, no "gc ran during push")
            pushed.append(
                r.commit_file("mid%d" % len(pushed), "mid%d\n" % len(pushed), "mid"))
            res = r.push()
            self.assertNotIn("gc ran during push", res.stderr + res.stdout)
            before["shas"] = sorted(
                set(before["shas"]) | set(ge.live_objects(r.name)["shas"]))
            saw_done = kind == "gc_consolidate" and s["outcome"] == "done"
        else:
            self.fail("compaction never drained under per-slice pushes")
        drain_compact(r)  # retire anything the pushes re-triggered
        self.assertLess(len(live_packs(r)), len(pushed),
                        "no compaction ever completed: %r" % ge.jobs(r.name)["packs"])
        clone = invariants(r, before)
        for c in pushed:
            ge.git("cat-file", "-e", c, cwd=clone)

    def test_push_and_clone_while_swap_paused(self):
        """Expected failing assertion:
            AssertionError from ge.admin inside snapshot() — GET objects ->
            404 on current code; failing that, step kind "gc_compact" ->
            "unknown job kind" (400) and the pause hook is never hit."""
        r = self.repo("cmp-swap-pause")
        small_packs(r, 6)
        before = snapshot(r)
        before.pop("refs_version")  # the push below bumps it legitimately
        s = ge.step(r.name, "gc_compact")  # init: gc_consolidate queued
        self.assertTrue(s["ran"], "%r" % s)
        ge.failpoint(r.name, "gc.compact.before_swap", "pause", 1)
        box = {}
        t = self.bg_step(r, "gc_consolidate", box)
        self.wait_in_pause(r, "gc.compact.before_swap", "gc_consolidate")
        # the swap is parked after MPU complete, before the final fence:
        # both a push and a clone must interleave cleanly with the
        # cohort-still-live bytes
        c = r.commit_file("during", "during\n", "during")
        res = r.push()
        self.assertNotIn("gc ran during push", res.stderr + res.stdout)
        clone = r.clone()
        ge.git("fsck", "--strict", cwd=clone)
        ge.failpoint(r.name, "gc.compact.before_swap", "off")  # release
        t.join(45)
        self.assertIsNone(box.get("err"), "%r" % box.get("err"))
        self.assertIsNotNone(box.get("s"), "paused step never returned")
        before["shas"] = sorted(
            set(before["shas"]) | set(ge.live_objects(r.name)["shas"]))
        drain_compact(r)
        clone = invariants(r, before)
        # the commit pushed under the parked swap is reachable in a fresh clone
        self.assertEqual(
            c, ge.git("rev-parse", "main", cwd=clone).stdout.strip())

    def test_walking_fetch_across_swap(self):
        """Expected failing assertion:
            AssertionError from ge.admin inside snapshot() — GET objects ->
            404 on current code. Were the hook in place, today's code still
            fails green-side: the walking fetch's plan reads a pack the swap
            retired mid-fetch ("pack index changed mid-fetch" / not live),
            instead of re-planning onto the merged pack's identical SHAs."""
        r = self.repo("cmp-fetch-swap")
        r.commit_file("seed", "seed\n", "seed")
        r.push()
        clone = r.clone()  # C at the seed tip — the fetch below walks
        small_packs(r, 6)
        r.commit_file("tail", "tail\n", "tail")
        r.push()
        before = snapshot(r)
        ge.failpoint(r.name, "fetch.walk.before_plan", "pause", 1)
        box = {}

        def bg_fetch():
            box["res"] = ge.git("fetch", "-q", "origin", cwd=clone, check=False)

        t = threading.Thread(target=bg_fetch, daemon=True)
        t.start()
        self.wait_in_pause(r, "fetch.walk.before_plan", None)
        drain_compact(r)  # the whole swap lands under the parked fetch
        ge.failpoint(r.name, "fetch.walk.before_plan", "off")  # release
        t.join(60)
        res = box.get("res")
        self.assertIsNotNone(res, "fetch never returned")
        self.assertEqual(res.returncode, 0,
                         "%s" % ge.redact(res.stderr + res.stdout))
        ge.git("fsck", "--strict", cwd=clone)
        invariants(r, before)

    def test_walking_fetch_stale_first_mark(self):
        """Kills no_stale: same shape as test_walking_fetch_across_swap, but
        the fetch parks at fetch.walk.after_load — inside the first non-empty
        load, after the index lookup resolved the pack locations but before
        the caller's marks land — so the swap retires the packs the earlier
        want/commit marks landed on between lookup and mark. The stale-mark
        path must re-resolve every recorded sha onto the merged pack, not
        trust the retired bitmaps."""
        r = self.repo("cmp-fetch-stale")
        r.commit_file("seed", "seed\n", "seed")
        r.push()
        clone = r.clone()  # C at the seed tip — the fetch below walks
        small_packs(r, 6)
        r.commit_file("tail", "tail\n", "tail")
        r.push()
        before = snapshot(r)
        ge.failpoint(r.name, "fetch.walk.after_load", "pause", 1)
        box = {}

        def bg_fetch():
            box["res"] = ge.git("fetch", "-q", "origin", cwd=clone, check=False)

        t = threading.Thread(target=bg_fetch, daemon=True)
        t.start()
        self.wait_in_pause(r, "fetch.walk.after_load", None)
        drain_compact(r)  # the whole swap lands under the parked fetch
        ge.failpoint(r.name, "fetch.walk.after_load", "off")  # release
        t.join(60)
        res = box.get("res")
        self.assertIsNotNone(res, "fetch never returned")
        self.assertEqual(res.returncode, 0,
                         "%s" % ge.redact(res.stderr + res.stdout))
        ge.git("fsck", "--strict", cwd=clone)
        invariants(r, before)

    def test_mark_reschedules_during_compaction(self):
        """Expected failing assertion:
            AssertionError from ge.admin — step kind "gc_compact" ->
            "unknown job kind" (400) on current code."""
        r = self.repo("cmp-mark-busy")
        small_packs(r, 6)
        s = ge.step(r.name, "gc_compact")  # init: mode=compact, build queued
        self.assertTrue(s["ran"], "%r" % s)
        v = ge.jobs(r.name)
        self.assertEqual("compact", v["gc_meta"].get("gc.mode"),
                         "%r" % v["gc_meta"])
        chain = v["gc_meta"].get("gc.chain")
        self.assertTrue(chain, "%r" % v["gc_meta"])
        self.assertIn("gc_consolidate", queued_kinds(r), "%r" % v["jobs"])
        ge.enqueue(r.name, "gc_mark")
        s = ge.step(r.name, "gc_mark")
        self.assertEqual("reschedule", s["outcome"], "%r" % s)
        v = ge.jobs(r.name)
        self.assertEqual("compact", v["gc_meta"].get("gc.mode"),
                         "%r" % v["gc_meta"])
        self.assertEqual(chain, v["gc_meta"].get("gc.chain"),
                         "the mark moved the compact chain: %r" % v["gc_meta"])
        drain_compact(r)
        tgs.step_until(r, "gc_mark", {"done"})  # now it is the mark's turn
        tgs.gc_drain(r)

    def test_compact_reschedules_during_gc_chain(self):
        """Expected failing assertion:
            AssertionError from ge.admin — POST kill_job -> 404 on current
            code (and "gc_compact" is an unknown kind for enqueue/step)."""
        r = self.repo("cmp-gc-busy")
        th.seed_gc_repo(r)
        small_packs(r, 4)
        ge.kill_job(r.name, "gc_compact")  # clear the trigger's row, if any
        tgs.step_until(r, "gc_mark", {"done"})  # a GC chain now owns the chain
        v = ge.jobs(r.name)
        self.assertEqual("build", v["gc_meta"].get("gc.phase"),
                         "%r" % v["gc_meta"])
        packs_before = v["packs"]
        ge.enqueue(r.name, "gc_compact")
        s = ge.step(r.name, "gc_compact")
        self.assertEqual("reschedule", s["outcome"], "%r" % s)
        self.assertEqual(packs_before, ge.jobs(r.name)["packs"],
                         "the busy check touched pack state")
        tgs.gc_drain(r)  # let the GC chain finish clean

    def test_pull_forward_skips_dispatched_rows(self):
        """Kills pull_forward_rescheduled: the mutation drops
        `AND started_at IS NULL` from the trigger's pull-forward predicate,
        which exists so only a never-dispatched row is moved. A busy
        Reschedule requeues gc_compact with attempts=0 and last_error NULL
        but started_at still set — a predicate on attempts/last_error alone
        would pull it forward on every push and burn its backoff. A fresh
        queued row must still be pulled forward."""
        r = self.repo("cmp-pullfwd")
        small_packs(r, 5)  # a cohort exists: gc_compact was enqueued by a push
        ge.enqueue(r.name, "gc_consolidate")  # chain-less builder -> busy
        s = ge.step(r.name, "gc_compact")
        self.assertEqual("reschedule", s["outcome"], "%r" % s)
        row = next(j for j in s["jobs"]
                   if j["kind"] == "gc_compact" and j["state"] == "queued")
        resched_at = row["run_at"]
        self.assertEqual(0, row["attempts"], "%r" % row)
        self.assertIsNone(row["last_error"], "%r" % row)
        ge.kill_job(r.name, "gc_consolidate")  # clear the busy builder
        # the next push fires the trigger with a cohort still available, so
        # t=now — a naive pull-forward would move the rescheduled row
        r.commit_file("trig", "t\n", "trig")
        r.push()
        v = ge.jobs(r.name)
        row = next(j for j in v["jobs"]
                   if j["kind"] == "gc_compact" and j["state"] == "queued")
        self.assertEqual(
            resched_at, row["run_at"],
            "a dispatched-then-rescheduled row was pulled forward: %r"
            % v["jobs"])
        # a never-dispatched row still pulls forward: park one at a deferred
        # wake via the age floor, then drop the floor — the next push's
        # trigger must move run_at down to ~now. Exhaust the resurrection
        # budget BEFORE the kill: boot's jobs::repair runs on every request
        # and would otherwise resurrect the dead gc_compact at run_at=now —
        # the trigger's enqueue then dedups behind it and MIN() can never
        # push the wake out.
        ge.meta(r.name, "resurrected.gc_compact", "3")
        ge.kill_job(r.name, "gc_compact")
        ge.knob(r.name, "compact_min_age_ms", 3_600_000)
        r.commit_file("defer", "d\n", "defer")
        r.push()  # nothing eligible for an hour -> enqueue at next_wake
        row = next(j for j in ge.jobs(r.name)["jobs"]
                   if j["kind"] == "gc_compact" and j["state"] == "queued")
        deferred = row["run_at"]
        self.assertGreater(deferred, resched_at,
                           "fresh row was not deferred: %r" % row)
        ge.knob(r.name, "compact_min_age_ms", 0)
        r.commit_file("now", "n\n", "now")
        r.push()  # cohort eligible again -> pull the deferred wake to ~now
        row = next(j for j in ge.jobs(r.name)["jobs"]
                   if j["kind"] == "gc_compact" and j["state"] == "queued")
        self.assertLess(row["run_at"], deferred,
                        "fresh row was not pulled forward: %r" % row)
        self.assertLessEqual(row["run_at"], int(time.time() * 1000) + 5000,
                             "fresh row pulled past now: %r" % row)

    def test_orphaned_compact_chain_recovers(self):
        """Kills orphan_any_gc_row: PR1's jobs::repair runs on every request
        and resurrects a dead gc_consolidate up to 3 times — in compact mode
        it charges the meta key resurrected.gc_compact (jobs/mod.rs) — so the
        budget must be exhausted BEFORE kill_job or boot repair bounces the
        builder back and the orphan path never runs. Asserts the orphan step
        is not a reschedule, gc.chain moved on, the dead row's last_error was
        preserved under meta dead.gc_consolidate, and no dead gc_consolidate
        row remains for a later boot to resurrect chain-less."""
        r = self.repo("cmp-orphan")
        small_packs(r, 6)
        before = snapshot(r)
        s = ge.step(r.name, "gc_compact")  # init: build queued
        self.assertTrue(s["ran"], "%r" % s)
        chain = ge.jobs(r.name)["gc_meta"].get("gc.chain")
        self.assertTrue(chain, "%r" % ge.jobs(r.name)["gc_meta"])
        # exhaust the bounded-resurrection budget: under gc.mode=compact a
        # dead gc_consolidate charges resurrected.gc_compact — set both so the
        # kill below strands the chain instead of resurrecting the builder
        for k in ("resurrected.gc_compact", "resurrected.gc_consolidate"):
            ge.meta(r.name, k, "3")
        ge.kill_job(r.name, "gc_consolidate")  # the builder row dies -> orphan
        ge.enqueue(r.name, "gc_mark")
        ge.enqueue(r.name, "gc_compact")
        s = ge.step(r.name, "gc_compact")
        self.assertTrue(s["ran"], "%r" % s)
        self.assertNotEqual("reschedule", s["outcome"],
                            "orphan was not detected/aborted: %r" % s)
        v = ge.jobs(r.name)
        # the recorded chain is gone — wiped and (here) replaced by the fresh
        # cohort the same step staked; either way it must not be the old id
        self.assertNotEqual(chain, v["gc_meta"].get("gc.chain"),
                            "orphan's chain id survived: %r" % v["gc_meta"])
        # the dead row's last_error was preserved for inspection...
        self.assertIn("dead.gc_consolidate", v["gc_meta"],
                      "%r" % v["gc_meta"])
        # ...and the row itself is gone, so repair cannot resurrect it into a
        # chain-less duplicate behind the fresh chain
        self.assertFalse(
            any(j["kind"] == "gc_consolidate" and j["state"] == "dead"
                for j in v["jobs"]),
            "dead gc_consolidate row left behind: %r" % v["jobs"])
        # the orphan was aborted losslessly: any half-built pack it left is
        # dead — never live over a cohort the abort could not verify
        self.assertFalse(
            any(p["push_id_null"] and p["state"] == "live" for p in v["packs"]),
            "orphaned build pack is live: %r" % v["packs"])
        drain_compact(r)  # a fresh cohort run completes the compaction
        self.assertLess(len(live_packs(r)), 6,
                        "no compaction after the orphan abort")
        invariants(r, before)

    def test_quarantine_needs_two_strikes(self):
        """Kills quarantine_first_strike: an orphaned compact chain bumps its
        cohort's compact.quarantine strike map by ONE — n >= 2 excludes a
        pack, n == 1 must not. After one orphan the cohort still compacts on
        the next drain; after two orphans the same cohort sits in
        compact.quarantine with n >= 2 and init stakes no chain over it."""
        r = self.repo("cmp-quar-one")
        small_packs(r, 6)
        s = ge.step(r.name, "gc_compact")  # init: chain staked, build queued
        self.assertTrue(s["ran"], "%r" % s)
        for k in ("resurrected.gc_compact", "resurrected.gc_consolidate"):
            ge.meta(r.name, k, "3")
        ge.kill_job(r.name, "gc_consolidate")
        ge.enqueue(r.name, "gc_compact")
        s = ge.step(r.name, "gc_compact")  # orphan abort: cohort strike n=1
        self.assertTrue(s["ran"], "%r" % s)
        self.assertNotEqual("reschedule", s["outcome"], "%r" % s)
        # one strike does NOT exclude: the same packs compact on the drain
        drain_compact(r)
        self.assertLess(len(live_packs(r)), 6,
                        "one strike wrongly excluded the cohort")

        r = self.repo("cmp-quar-two")
        small_packs(r, 6)
        s = ge.step(r.name, "gc_compact")
        self.assertTrue(s["ran"], "%r" % s)
        cohort = sorted(p["id"] for p in ge.jobs(r.name)["packs"] if p["marked"])
        self.assertGreaterEqual(len(cohort), 2, "%r" % ge.jobs(r.name)["packs"])
        for k in ("resurrected.gc_compact", "resurrected.gc_consolidate"):
            ge.meta(r.name, k, "3")
        for _ in range(2):
            # each orphan-aborted init re-stakes a chain over the cohort
            # while n < 2 — kill its builder and orphan it again
            ge.kill_job(r.name, "gc_consolidate")
            ge.enqueue(r.name, "gc_compact")
            s = ge.step(r.name, "gc_compact")
            self.assertTrue(s["ran"], "%r" % s)
            self.assertNotEqual("reschedule", s["outcome"], "%r" % s)
        v = ge.jobs(r.name)
        quar = dict(json.loads(v["gc_meta"].get("compact.quarantine", "[]")))
        for pid in cohort:
            self.assertGreaterEqual(
                quar.get(pid, 0), 2,
                "cohort pack %s under-struck: %r" % (pid, quar))
        # two strikes DO exclude: init staked no chain over the cohort
        self.assertFalse(
            v["gc_meta"].get("gc.chain"),
            "a compact chain staked over quarantined packs: %r" % v["gc_meta"])
        self.assertFalse(
            any(j["kind"] == "gc_consolidate"
                and j["state"] in ("queued", "running") for j in v["jobs"]),
            "%r" % v["jobs"])
        self.assertEqual(6, len(live_packs(r)),
                         "a quarantined cohort compacted: %r" % v["packs"])

    def test_corrupt_quarantine_preserved_and_strikes_resume(self):
        """Kills corrupt_map_freezes_strikes: quarantine_read's None on an
        unparseable map must not skip the save — the raw bytes are stashed
        once under compact.quarantine.corrupt and a fresh map takes the new
        strike. Plant a corrupt map, drive an orphan abort like the other
        quarantine tests, and the corrupt key must hold the original bytes
        while compact.quarantine parses again carrying the cohort's strike."""
        r = self.repo("cmp-quar-corrupt")
        small_packs(r, 6)
        s = ge.step(r.name, "gc_compact")  # init: cohort marked, chain staked
        self.assertTrue(s["ran"], "%r" % s)
        cohort = sorted(p["id"] for p in ge.jobs(r.name)["packs"] if p["marked"])
        self.assertGreaterEqual(len(cohort), 2, "%r" % ge.jobs(r.name)["packs"])
        ge.meta(r.name, "compact.quarantine", "not json {")  # plant corruption
        for k in ("resurrected.gc_compact", "resurrected.gc_consolidate"):
            ge.meta(r.name, k, "3")
        ge.kill_job(r.name, "gc_consolidate")  # strand the builder -> orphan
        ge.enqueue(r.name, "gc_compact")
        s = ge.step(r.name, "gc_compact")  # orphan abort: stash + strike save
        self.assertTrue(s["ran"], "%r" % s)
        self.assertNotEqual("reschedule", s["outcome"], "%r" % s)
        v = ge.jobs(r.name)
        self.assertEqual(
            "not json {", v["gc_meta"].get("compact.quarantine.corrupt"),
            "corrupt bytes were not preserved: %r" % v["gc_meta"])
        quar = dict(json.loads(v["gc_meta"].get("compact.quarantine", "[]")))
        for pid in cohort:
            self.assertEqual(
                1, quar.get(pid, 0),
                "strike not recorded for %s: %r" % (pid, quar))

    def test_cohort_pack_died_before_swap(self):
        """Expected failing assertion:
            AssertionError from ge.admin — step kind "gc_compact" ->
            "unknown job kind" (400); failing that POST pack_state -> 404
            and the pause hook is never hit."""
        r = self.repo("cmp-cohort-died")
        small_packs(r, 6)
        s = ge.step(r.name, "gc_compact")  # init: build queued
        self.assertTrue(s["ran"], "%r" % s)
        ge.failpoint(r.name, "gc.compact.before_swap", "pause", 1)
        box = {}
        t = self.bg_step(r, "gc_consolidate", box)
        v = self.wait_in_pause(r, "gc.compact.before_swap", "gc_consolidate")
        cohort = [p for p in v["packs"] if p["marked"] and p["state"] == "live"]
        self.assertTrue(cohort, "no marked cohort packs: %r" % v["packs"])
        marks_before = sum(1 for j in v["jobs"] if j["kind"] == "gc_mark")
        ge.pack_state(r.name, cohort[0]["id"], "dead")  # purge got one
        ge.failpoint(r.name, "gc.compact.before_swap", "off")  # release
        t.join(45)
        self.assertIsNone(box.get("err"), "%r" % box.get("err"))
        v = ge.jobs(r.name)
        packs = {p["id"]: p for p in v["packs"]}
        # the cohort-still-live guard aborts losslessly: the build pack is
        # dead (never promoted over a dead input), the rest of the cohort
        # stays live, and the abort never enqueues a gc_mark
        self.assertFalse(
            any(p["push_id_null"] and p["state"] == "live" for p in v["packs"]),
            "build pack went live over a dead cohort pack: %r" % v["packs"])
        self.assertTrue(
            any(p["push_id_null"] and p["state"] == "dead" for p in v["packs"]),
            "no dead build pack after the abort: %r" % v["packs"])
        for p in cohort[1:]:
            self.assertEqual("live", packs[p["id"]]["state"],
                             "cohort pack %s moved: %r" % (p["id"], packs[p["id"]]))
        marks_after = sum(1 for j in v["jobs"] if j["kind"] == "gc_mark")
        self.assertEqual(marks_before, marks_after,
                         "the abort enqueued gc_mark: %r" % v["jobs"])

    def test_cohort_died_before_build(self):
        """Kills no_entry_cohort_check: the consolidate must re-verify the
        frozen cohort's liveness on entry, before any build work — a cohort
        pack dead between init and the first builder slice makes the slice
        abort losslessly: no build pack live or ingesting, the rest of the
        cohort untouched, gc.chain wiped, and no gc_mark enqueued. The armed
        gc.consolidate.begin crash is the discriminating oracle: the entry
        check runs before it, so a slice that began build work (the mutant)
        reports 'crashed' — without the arm the mutant's only observable
        abort is the swap's own later cohort check."""
        r = self.repo("cmp-died-build")
        small_packs(r, 6)
        s = ge.step(r.name, "gc_compact")  # init: cohort marked, build queued
        self.assertTrue(s["ran"], "%r" % s)
        v = ge.jobs(r.name)
        cohort = [p for p in v["packs"] if p["marked"]]
        self.assertTrue(cohort, "no marked cohort packs: %r" % v["packs"])
        marks_before = sum(1 for j in v["jobs"] if j["kind"] == "gc_mark")
        ge.pack_state(r.name, cohort[0]["id"], "dead")  # purge got one
        ge.failpoint(r.name, "gc.consolidate.begin", "crash", 1)
        s = ge.step(r.name, "gc_consolidate")
        self.assertTrue(s["ran"], "%r" % s)
        # the entry check aborted before begin_build — the armed crash never
        # fired; 'crashed' means the slice ran build work over a dead cohort
        self.assertNotEqual("crashed", s["outcome"],
                            "build began over a dead cohort: %r" % s)
        v = ge.jobs(r.name)
        packs = {p["id"]: p for p in v["packs"]}
        self.assertFalse(
            any(p["push_id_null"] and p["state"] in ("live", "ingesting")
                for p in v["packs"]),
            "a build pack survived the entry abort: %r" % v["packs"])
        for p in cohort[1:]:
            self.assertEqual("live", packs[p["id"]]["state"],
                             "cohort pack %s moved: %r" % (p["id"], packs[p["id"]]))
        self.assertFalse(v["gc_meta"].get("gc.chain"),
                         "gc.chain survived the abort: %r" % v["gc_meta"])
        marks_after = sum(1 for j in v["jobs"] if j["kind"] == "gc_mark")
        self.assertEqual(marks_before, marks_after,
                         "the abort enqueued gc_mark: %r" % v["jobs"])
        self.assertTrue(
            any(f["name"] == "gc.consolidate.begin" and f["remaining"] > 0
                for f in v["failpoints"]),
            "gc.consolidate.begin fired: %r" % v["failpoints"])
        ge.failpoint(r.name, "gc.consolidate.begin", "off")  # disarm

    def test_quiet_burst_drains_multiple_cohorts(self):
        """Expected failing assertion:
            AssertionError from ge.admin inside snapshot() — GET objects ->
            404 on current code; nothing drains because "gc_compact" is an
            unknown step kind there, leaving all 12 packs live."""
        r = self.repo("cmp-burst")
        ge.knob(r.name, "compact_fanin", 2)  # cohorts of up to 4 at T=2
        small_packs(r, 12)
        before = snapshot(r)
        drain_compact(r)  # the swap's re-trigger drives further cohorts
        self.assertLessEqual(len(live_packs(r)), 4,
                             "cohorts did not drain: %r" % live_packs(r))
        invariants(r, before)

    def test_chain_fence_blocks_foreign_chain(self):
        """Kills no_chain_check: the swap's final fence compares gc.chain to
        the id the slice captured at claim. Rewrite gc.chain under the paused
        swap — a fence-less swap would promote the build pack over a chain it
        no longer owns and kill the cohort underneath a live reader."""
        r = self.repo("cmp-fence")
        small_packs(r, 6)
        before = snapshot(r)
        s = ge.step(r.name, "gc_compact")  # init: build queued
        self.assertTrue(s["ran"], "%r" % s)
        ge.failpoint(r.name, "gc.compact.before_swap", "pause", 1)
        box = {}
        t = self.bg_step(r, "gc_consolidate", box)
        v = self.wait_in_pause(r, "gc.compact.before_swap", "gc_consolidate")
        cohort = [p["id"] for p in v["packs"] if p["marked"]]
        self.assertTrue(cohort, "no marked cohort packs: %r" % v["packs"])
        ge.meta(r.name, "gc.chain", "f" * 32)  # a foreign chain id
        ge.failpoint(r.name, "gc.compact.before_swap", "off")  # release
        t.join(45)
        self.assertIsNone(box.get("err"), "%r" % box.get("err"))
        v = ge.jobs(r.name)
        packs = {p["id"]: p for p in v["packs"]}
        # the swap fenced on the moved chain and ran nothing: the build pack
        # never went live and every cohort pack survived
        self.assertFalse(
            any(p["push_id_null"] and p["state"] == "live" for p in v["packs"]),
            "build pack went live under a foreign chain: %r" % v["packs"])
        for pid in cohort:
            self.assertEqual("live", packs[pid]["state"],
                             "cohort pack %s moved: %r" % (pid, packs[pid]))
        self.assertEqual(before["shas"],
                         sorted(ge.live_objects(r.name)["shas"]),
                         "live SHA set changed under a fenced swap")

    def test_fence_blocks_foreign_build_pack(self):
        """Kills no_fence_newpack: same shape as
        test_chain_fence_blocks_foreign_chain, but the rewrite hits
        gc.new_pack — compact mode's fence also requires gc.new_pack ==
        pos.pack, so a foreign build-pack id must park the swap the same way
        a foreign chain does. The real build pack never goes live and the
        cohort survives."""
        r = self.repo("cmp-fence-newpack")
        small_packs(r, 6)
        before = snapshot(r)
        s = ge.step(r.name, "gc_compact")  # init: build queued
        self.assertTrue(s["ran"], "%r" % s)
        ge.failpoint(r.name, "gc.compact.before_swap", "pause", 1)
        box = {}
        t = self.bg_step(r, "gc_consolidate", box)
        v = self.wait_in_pause(r, "gc.compact.before_swap", "gc_consolidate")
        cohort = [p["id"] for p in v["packs"] if p["marked"]]
        self.assertTrue(cohort, "no marked cohort packs: %r" % v["packs"])
        ge.meta(r.name, "gc.new_pack", "0" * 32)  # a foreign build-pack id
        ge.failpoint(r.name, "gc.compact.before_swap", "off")  # release
        t.join(45)
        self.assertIsNone(box.get("err"), "%r" % box.get("err"))
        v = ge.jobs(r.name)
        packs = {p["id"]: p for p in v["packs"]}
        # the swap fenced on the moved build-pack id and ran nothing: the real
        # build pack never went live and every cohort pack survived
        self.assertFalse(
            any(p["push_id_null"] and p["state"] == "live" for p in v["packs"]),
            "build pack went live under a foreign gc.new_pack: %r" % v["packs"])
        for pid in cohort:
            self.assertEqual("live", packs[pid]["state"],
                             "cohort pack %s moved: %r" % (pid, packs[pid]))
        self.assertEqual(before["shas"],
                         sorted(ge.live_objects(r.name)["shas"]),
                         "live SHA set changed under a fenced swap")

    def test_sweep_noop_in_compact_mode(self):
        """Kills sweep_runs_in_compact: a compact chain has no sweep phase —
        gc_sweep must Done out without touching packs or gc_epoch even when
        gc.phase is (mis)set to 'sweep' and a chain-less sweep row exists."""
        r = self.repo("cmp-sweep-guard")
        small_packs(r, 6)
        s = ge.step(r.name, "gc_compact")  # init stakes the compact chain
        self.assertTrue(s["ran"], "%r" % s)
        v = ge.jobs(r.name)
        self.assertEqual("compact", v["gc_meta"].get("gc.mode"),
                         "%r" % v["gc_meta"])
        packs_before = v["packs"]
        epoch_before = v["gc_meta"].get("gc_epoch")
        ge.enqueue(r.name, "gc_sweep")
        ge.meta(r.name, "gc.phase", "sweep")  # force past the phase guard
        # and past the refs_version guard too — a sweep that ignores the
        # compact guard must be free to reach its delete span
        ge.meta(r.name, "gc.refs_version", v["gc_meta"].get("refs_version"))
        s = ge.step(r.name, "gc_sweep")
        self.assertEqual("done", s["outcome"], "%r" % s)
        v = ge.jobs(r.name)
        self.assertEqual(packs_before, v["packs"],
                         "a sweep in compact mode touched pack state")
        self.assertEqual(epoch_before, v["gc_meta"].get("gc_epoch"),
                         "gc_epoch moved: %r" % v["gc_meta"])
        ge.meta(r.name, "gc.phase", "build")  # restore, then finish normally
        drain_compact(r)
        self.assertLess(len(live_packs(r)), 6,
                        "compaction did not complete after the sweep noop")


if __name__ == "__main__":
    unittest.main(verbosity=2)
