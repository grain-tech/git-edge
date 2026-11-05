"""hooks-OFF inertness — run only against a server started WITHOUT
GE_TEST_HOOKS. run.sh runs only this file when GE_HOOKS_OFF=1. The server
also needs short GC timings (GE_GC_QUIET_MS<=2000 GE_GC_GRACE_MS<=1000) or
the alarm-driven sweep won't land inside the deadline.

Two guarantees: the test routes are ordinary 404s — before auth, and with
the same body an unknown route returns (a 401 or a different body would
leak that the route exists) — and the alarm dispatcher still runs jobs
normally (a gc chain's dead pack is the client-visible signal).
"""

import os
import sys
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ge

OWNER = "hoff-%d-%d" % (int(time.time()), os.getpid())

ROUTES = [
    ("POST", "step"),
    ("POST", "failpoint"),
    ("POST", "repair"),
    ("POST", "drop_object"),
    ("POST", "drop_pack"),
    ("POST", "knob"),
    ("POST", "meta"),
    ("POST", "enqueue"),
    ("GET", "jobs"),
    ("GET", "fsck"),
]


@unittest.skipUnless(os.environ.get("GE_HOOKS_OFF") == "1", "needs a hooks-OFF server (GE_HOOKS_OFF=1)")
class HooksOff(unittest.TestCase):
    def test_routes_are_plain_404s(self):
        # hooks-off must be indistinguishable from an unknown route: same
        # status AND same body, authed or not
        st, canon = ge.http("GET", "/%s/x/_admin/does-not-exist" % OWNER)
        self.assertEqual(st, 404)
        for method, path in ROUTES:
            with self.subTest(route=path):
                p = "/%s/off/_admin/test/%s" % (OWNER, path)
                for auth in (True, False):
                    st, body = ge.http(method, p, auth=auth)
                    self.assertEqual(
                        st, 404, "%s %s auth=%s -> %d" % (method, path, auth, st)
                    )
                    self.assertEqual(
                        body, canon, "%s %s auth=%s body" % (method, path, auth)
                    )

    def test_alarm_still_dispatches(self):
        r = ge.Repo("%s/alarm" % OWNER)
        r.commit_file("f", "one\n", "one")
        r.commit_file("f", "two\n", "two")
        r.push()
        ge.git("reset", "--hard", "HEAD~1", cwd=r.dir)
        tip = r.commit_file("f", "three\n", "three")
        r.push(force=True)
        # The client-visible job signal without hooks: the alarm-dispatched
        # gc chain (mark->consolidate->sweep) marks the orphan pack dead in
        # /_state. Needs GE_GC_QUIET_MS<=2000 GE_GC_GRACE_MS<=1000 on this
        # server; the clone check runs first so a real break still fails.
        clone = r.clone()
        self.assertEqual(ge.git("rev-parse", "main", cwd=clone).stdout.strip(), tip)
        ge.git("fsck", "--strict", cwd=clone)
        deadline = time.time() + 60
        while time.time() < deadline:
            st, s = ge.state(r.name)
            if st == 200 and s and s.get("packs_dead"):
                break
            time.sleep(0.5)
        else:
            self.fail(
                "no sweep within 60s — hooks-off server needs "
                "GE_GC_QUIET_MS<=2000 GE_GC_GRACE_MS<=1000"
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
