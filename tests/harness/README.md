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
- `GE_RATE_PUSHES_PER_MIN=0`: only needed by test_f's ~100-push run.

## Run

    GE_URL=http://test:write-test-token@127.0.0.1:8795 tests/harness/run.sh

Hooks-OFF inertness check — start a second server WITHOUT the test vars but
with short GC timings (`GE_GC_QUIET_MS<=2000 GE_GC_GRACE_MS<=1000`, so the
alarm-driven sweep lands inside the deadline) and:

    GE_HOOKS_OFF=1 GE_URL=... tests/harness/run.sh   # runs only test_hooks_off.py

`run.sh` exports `GE_SERVER_LOG` (default `/tmp/ge-dev.log`) pointing at the
server's log — handy for debugging; no test reads it.
`test_f_clone_over_101_live_packs` (G2) pushes a repo past 100 live packs —
three of them holding identical objects. Every commit is pinned to a fixed
timestamp so the sha ordering the dedup scan walks is deterministic, and the
test asserts locally that a sha's 3-row duplicate run straddles the 5,000-row
page before cloning. It then asserts a plain clone succeeds, `git fsck
--strict` is clean, and the emitted pack's object count equals the unique-sha
count. Before G2 that clone hit the A6 bound-param guard and 500'd.

## Hooks (all write-token gated)

| Route | Purpose |
|---|---|
| `POST _admin/test/step` | `{"kind"?}` — run ONE slice through the shared dispatcher (run_at ignored). Returns `{ran, job{id,kind}, outcome, error}` + the jobs/gc_meta/packs view. 409 unless `GE_TEST_MANUAL_JOBS=1`. |
| `GET  _admin/test/jobs` | jobs rows (incl. the persisted `cursor`, e.g. an import's `phase`), `gc.*` meta + `gc_epoch`/`refs_version`, packs (each with a `marked` flag — present in the GC candidate bitmap), `failpoints` (armed rows incl. consumed `remaining`), and `gc_tables` — a `{n, hash}` snapshot per `marked`/`gc_seen`/`gc_frontier`/`objects`/`push_links`/`import_open`/`import_parts`/`import_tail` (count + FNV-1a digest over sorted rows; compare two snapshots and any in-slice write shows up). |
| `POST _admin/test/enqueue` | `{"kind","payload"?}` — insert a job row exactly like repair resurrection (payload defaults to `"{}"`; `jobs::enqueue` still dedups against queued rows of the kind). |
| `POST _admin/test/meta` | `{"key","value"}` — set one meta row directly (e.g. delete `gc.phase`/`gc.chain` to simulate a chain started before the phase guard shipped); `null` value deletes. |
| `POST _admin/test/failpoint` | `{"name","action":"error"|"crash"|"pause"|"off","count":n}` — arm a named failpoint (`gc.mark.init`, `gc.mark.after_read`, `gc.mark.after_commit`, `gc.consolidate.begin`, `gc.consolidate.after_part`, `gc.consolidate.before_finish`, `gc.sweep.before`, `import.check.done`, `import.commit.before`, `import.commit.after_finish`, `import.drain.after_create`, `import.entry.after_read`). `error` = ordinary retry; `crash` = simulated isolate death (row stays `running`); `pause` = the arm is consumed like any other, then the slice AWAITS — polling the row every 100 ms until `off` deletes it or 30 s pass. `pause` only fires where the call site awaits it (`testhooks::failpoint_async`, currently `gc.mark.after_read`, `import.drain.after_create`, `import.entry.after_read` and `import.commit.after_finish`); at every other site the arm stays untouched and inert. `gc.sweep.before` and `import.check.done` are CLAIM-ONLY points — their slices never await, so a real death rolls back the whole span and "crashed here" is equivalent to "died before the slice ran" — EXCEPT that `import.check.done` sits after run_slice's `UPDATE pushes SET began_at=?` lease refresh, which the simulated crash keeps but a real death would roll back. |
| `POST _admin/test/repair` | age `running` jobs 61 s + run boot's `jobs::repair` — turns a crash into a `queued` row (strands+1). |
| `GET  _admin/test/fsck?mode=refs\|all` | server-side oracle over the live index + R2: BOTH modes read, strictly decode and hash-verify every checked row (pack HEAD cached per pack, extent must fit, consumed bytes == indexed len, inflated size == indexed size, kind + hash must match). `refs` walks every live row of every reachable sha; `all` also iterates every objects row of every live pack and resolves every non-blob's links (I1/I2). Returns `{ok, missing, corrupt, checked}`; a budget/R2 fault aborts rather than reporting a partial check as ok. |
| `POST _admin/test/drop_object` | `{"sha","action"?,"to"?}` — `drop` (default) deletes objects rows; `corrupt` shifts the indexed offset; `len_plus`/`size_plus` bump the indexed len/size so strict decode fails while the extent stays in-pack; `alias` copies `to`'s extent onto `sha`'s row in the same pack (decodes fine, hashes wrong). |
| `POST _admin/test/drop_pack` | `{"pack"}` — delete the pack's R2 object under a live index (fsck reports missing). |
| `POST _admin/test/knob` | `{"name","value"?}` — integer knobs, read only with hooks on: `slice_subreqs` moves the slice's SOFT yield point only (the hard 400-subrequest cap is unchanged); `gc_part_bytes` shrinks the consolidate part size, clamped to [5 MiB, 8 MiB] (multi-part build on a small repo; 409 while a gc build is in progress); `gc_candidate_before_ms` is an absolute created_at cutoff — packs older than it are mark candidates (split packs into old candidates and young keepers with no sleep race); `max_live_packs` overrides the G2b live-pack admission cap (`GE_MAX_LIVE_PACKS`, default 1000; `<= 0` disables). value null deletes. |
