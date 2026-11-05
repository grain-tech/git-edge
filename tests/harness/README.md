# tests/harness — deterministic GC/job harness (PR0)

Python 3 stdlib-only unittest suite (`test_harness.py`, helpers in `ge.py`)
driving the PR0 test-only hooks. Everything is gated on `GE_TEST_HOOKS=1`;
without it the routes are ordinary 404s (same `Error::NotFound` as the route
table's fall-through, checked before admin auth), `failpoint()` returns early
with no SQL, `test_failpoints` is never created, and `alarm()` dispatches
normally. With the flag off the edge never forwards `_admin/test/*` at all —
there is no other way to reach the hooks.

## Server

    cd server && npx wrangler dev --port 8795 --ip 127.0.0.1 \
      --var GE_TEST_HOOKS:1 --var GE_TEST_MANUAL_JOBS:1 \
      --var GE_GC_QUIET_MS:0 --var GE_GC_GRACE_MS:0 --var GE_RATE_PUSHES_PER_MIN:0 \
      > /tmp/ge-dev.log 2>&1

- `GE_TEST_MANUAL_JOBS=1`: `alarm()` no longer dispatches — every job slice runs
  only via `POST /<o>/<r>/_admin/test/step`, which is what makes the suite
  deterministic (no sleeps, no polling).
- `GE_GC_QUIET_MS=0 GE_GC_GRACE_MS=0`: a ref commit's `gc_mark` is due
  immediately and every live pack is a candidate at once.
- `GE_RATE_PUSHES_PER_MIN=0`: only needed by the 100-pack param-guard case.

## Run

    GE_URL=http://test:write-test-token@127.0.0.1:8795 tests/harness/run.sh

Hooks-OFF inertness check — start a second server WITHOUT the test vars but
with short GC timings (`GE_GC_QUIET_MS<=2000 GE_GC_GRACE_MS<=1000`, so the
alarm-driven sweep lands inside the deadline) and:

    GE_HOOKS_OFF=1 GE_URL=... tests/harness/run.sh   # runs only test_hooks_off.py

`run.sh` exports `GE_SERVER_LOG` (default `/tmp/ge-dev.log`) pointing at the
server's log. `test_f_a6_guard_rejects_101_param_clone` polls it — only text
written after the test starts, for up to 10 s since wrangler flushes
asynchronously — to confirm the failing clone was rejected by the A6
bound-param guard (`sql: N bound params > 100 (A6)`; the message is
`Error::Internal`, sanitized to "internal error" on the wire, so the literal
string exists only in the log). The test fails if the file is missing.

## Hooks (all write-token gated)

| Route | Purpose |
|---|---|
| `POST _admin/test/step` | `{"kind"?}` — run ONE slice through the shared dispatcher (run_at ignored). Returns `{ran, job{id,kind}, outcome, error}` + the jobs/gc_meta/packs view. 409 unless `GE_TEST_MANUAL_JOBS=1`. |
| `GET  _admin/test/jobs` | jobs rows, `gc.*` meta + `gc_epoch`/`refs_version`, packs. |
| `POST _admin/test/failpoint` | `{"name","action":"error"|"crash"|"off","count":n}` — arm a named failpoint (`gc.mark.init`, `gc.mark.after_read`, `gc.mark.after_commit`, `gc.consolidate.begin`, `gc.consolidate.after_part`, `gc.consolidate.before_finish`, `gc.sweep.before`, `import.check.done`, `import.commit.before`). `error` = ordinary retry; `crash` = simulated isolate death (row stays `running`). `gc.sweep.before` and `import.check.done` are CLAIM-ONLY points — their slices never await, so a real death rolls back the whole span and "crashed here" is equivalent to "died before the slice ran" — EXCEPT that `import.check.done` sits after run_slice's `UPDATE pushes SET began_at=?` lease refresh, which the simulated crash keeps but a real death would roll back. |
| `POST _admin/test/repair` | age `running` jobs 61 s + run boot's `jobs::repair` — turns a crash into a `queued` row (strands+1). |
| `GET  _admin/test/fsck?mode=refs\|all` | server-side oracle over the live index + R2: BOTH modes read, strictly decode and hash-verify every checked row (pack HEAD cached per pack, extent must fit, consumed bytes == indexed len, inflated size == indexed size, kind + hash must match). `refs` walks every live row of every reachable sha; `all` also iterates every objects row of every live pack and resolves every non-blob's links (I1/I2). Returns `{ok, missing, corrupt, checked}`; a budget/R2 fault aborts rather than reporting a partial check as ok. |
| `POST _admin/test/drop_object` | `{"sha","action"?,"to"?}` — `drop` (default) deletes objects rows; `corrupt` shifts the indexed offset; `len_plus`/`size_plus` bump the indexed len/size so strict decode fails while the extent stays in-pack; `alias` copies `to`'s extent onto `sha`'s row in the same pack (decodes fine, hashes wrong). |
| `POST _admin/test/drop_pack` | `{"pack"}` — delete the pack's R2 object under a live index (fsck reports missing). |
| `POST _admin/test/knob` | `{"name","value"?}` — integer knobs, read only with hooks on: `slice_subreqs` moves the slice's SOFT yield point only (the hard 400-subrequest cap is unchanged); `gc_part_bytes` shrinks the consolidate part size, clamped to [5 MiB, 8 MiB] (multi-part build on a small repo; 409 while a gc build is in progress). value null deletes. |
