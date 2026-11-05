# git-edge handoff — 2026-09-16 (P0–P2 all done; full conformance green)

Serverless Git smart-HTTP host: Rust/WASM Cloudflare Worker + per-repo SQLite
Durable Object + R2 packfiles. Repo: `idl3/git-edge`. Main is at `9369116`
(PR #17 C2 merged). **Note:** PR #18 (C1) shows MERGED on GitHub but its merge
commit never reached main — `p2-robustness` carries its commit, so merging the
branch lands C1's content.

## Where things stand

All P0/P1/P2 roadmap items are done and verified. Everything sits on
`p2-robustness` (11 commits on main): C1 `packfile-uris`, the robustness/P2
batch (gc_tail, shared-push import, no-walk clone, token scopes, LFS, atomic),
I1 resumable import with dead-MPU recovery + strand accounting, the no-walk
cross-pack dedup fix, and the conformance suite additions.

**Full conformance PASS** — all flags (`GC`, `IMPORT`, `LIMITS`, `PURGE`,
`URIS`) on a dedicated wrangler with quota caps (`--persist-to` fresh state +
`--var GE_QUOTA_MAX_*`/`GE_RATE_PUSHES_PER_MIN`; a shared dev instance can't
run LIMITS because owner-registry claims accumulate across runs — only
repo-delete releases them).

- **C2 verbatim consolidated-pack fast path** (`consolidated_pack` +
  `VerbatimStream` in `repo_do/mod.rs`): plain-clone-shaped fetch + exactly
  one live pack covering all wants → stream that R2 object verbatim, one
  `bucket.get`, `x-ge-subrequests: 1`. **Re-benched on react: 263k-object
  clone in 18.7 s at 1 subrequest** (was HTTP 413 at the old wall).
- **C1 `packfile-uris`** (`pack_uri_response` + `sign.rs` + edge `pack_get`):
  opted-in clients (`fetch.uriprotocols`, git >= 2.40) instead get a signed
  `/_packs/<id>.pack?e=&r=&s=` URL — HMAC-SHA256 over `v1\nrepo_id\npack\nexp`
  keyed by `GE_URL_SIGNING_KEY`, 1h TTL. Inline `packfile` section is a legal
  empty pack; the `<hash>` token is the pack's real trailer SHA-1 (client
  verifies). Bandwidth fully bypasses the Worker. **React via URI: 14.8 s.**
- **I1 server-side resumable import** (`jobs/import.rs`, A31):
  `POST /_admin/import/stage` streams a pack part into `pending/` and opens a
  push; further parts reuse it via `?push=<id>&part=<n>` (idempotent re-begin —
  required, since only the job's own push gets heartbeat protection from the
  janitor's PUSH_TIMEOUT sweep); `POST /_admin/import {push, parts, commands}`
  queues an `import_pack` job; `GET /_admin/import/<push>` reports
  phase/objects. The job rides the
  normal push lifecycle — pass A parses a persistent `import_toc` off the
  staged parts, pass B normalizes entries into a resumable output MPU
  (`import_parts` etags + `WriterCkpt`; `import_open` shadow rows name the
  mid-entry resume point), 2.5 links live in `push_links` (no 1M cap), and the
  final slice runs `commit_push` with atomic semantics. Kills the
  single-commit ingest ceiling (TypeScript's 222k-object commit).
  **URL import** (`remote.rs` + the job's `fetch` phase): `POST /_admin/import
  {url}` needs no staging — the DO runs the v2 client itself (`ls-refs` →
  command mint, `fetch` → one RawWriter part under `pending/`, sha1 trailer
  verified before the payload is rewritten into the staged shape). Deterministic
  remote failures (404/401/no-v2/out-of-scope/no-refs) reject the push with the
  reason instead of burning attempts; a mid-stream isolate death re-downloads
  the whole pack (no mid-response resume exists). http is loopback-only,
  remotes get no auth, refs cap at 100k.

**I1 correctness model, compressed**: durable = uploaded parts only.
`objects`/`push_links` rows may run ahead inside a slice; every resume
re-derives — delete objects rows past `WriterCkpt.pos`, restore fully-durable
unposted rows from `import_open` shadow cols, frag-resume the one straddling
writer (`skip = durable - off`), retry parked REF_DELTA entries rather than
trusting cursor wake bookkeeping. `end <= durable` OR an objects row = done —
covers dup-sha dedup and posted-but-durable cases.

**Bugs the smoke run flushed out** (all fixed): TOC loop gated on the flushed
counter so `count < TOC_FANOUT` packs over-parsed past the trailer; a
checkpoint-time `DELETE >durable` killed just-posted rows every slice (resume
owns that delete, not the checkpoint); the queue scan's `idx > 0` skipped
entry 0; `PackWriter::resume` at `pos=0` dropped the 12-byte pack header
(clone died on `pack signature mismatch`) — resume now re-emits it.

**Bugs the 462k react run flushed out** (all fixed): the checkpoint reused a
stale `c.st` instead of the live `out.snapshot()`, freezing the durable prefix
while mid-slice `flush_if_full` uploads piled into `import_parts` — a
zero-progress livelock; dead slices left stale etags past `pos/PART` that a
resume would have assembled into the final MPU alongside the rewritten parts —
resume now truncates the list to the live prefix and `flush_ckpt` prunes rows
beyond it; each
`import/stage` call minted its own `open` push, so 16 sibling part pushes hit
PUSH_TIMEOUT mid-import and the janitor swept the source parts from under the
running job — fixed by shared-push staging (`?push=&part=`) + a janitor
prefix delete of `pending/<push>.`.

**Verified live**: full conformance incl. `GE_CONFORMANCE_IMPORT=1` PASS
(import → commit → clone → fsck). The 462k-object react pack committed —
1,149 refs, `push:committed` after a dead-MPU wipe+rebuild — and cloned back
6.41 GiB in 219 s, `fsck --strict` clean, HEAD exact. TypeScript followed:
984,826 objects staged in 44 parts, imported ~1h52m with zero retries,
normalized to one 18.63 GiB live pack (6.85× undeltified expansion),
cloned back via `packfile-uris` in 1,020 s, `fsck --strict` clean,
324/324 refs + HEAD exact. That closes the parity matrix — every
benchmarked repo fits.

**Latent bug C2 exposed and fixed**: `PackWriter::append_stored` hashed each
entry eagerly AND again at part upload, so every GC-consolidated pack stored
a wrong trailer. Walking-path fetches rebuild trailers and never noticed;
verbatim streaming made it visible (`index-pack`: SHA1 mismatch). One-line
fix in `store/mod.rs`; `GE_CONFORMANCE_GC=1` is the regression check.

Verified live: base suite + `GE_CONFORMANCE_GC=1 GE_CONFORMANCE_URIS=1` PASS
on wrangler dev — including a real git 2.55 clone over the signed URI.

## Remaining ceilings (post-I1)

| Wall | Evidence | Status |
|---|---|---|
| **Clone** 413 between 110k–263k objects | benchmark round 10 | **cleared** — react 263k in 18.7 s @ 1 subrequest (A29); URI-offloaded in 14.8 s (A30) |
| **Import** single commit > ingest budget (TS: 222k objects) | unsplittable-slice error | **cleared by I1** — react's 462k pack and TypeScript's 985k both committed end-to-end |
| **Inline clone > ~7 GiB wire** | TypeScript's 18.63 GiB normalized pack hit `ERR request budget exhausted` at ~245 s (240 s wall ÷ ~32 MiB/s sideband rate) | inherent platform bound — `packfile-uris` is the path past it (1,020 s clone incl. index-pack). Documented in COMPATIBILITY |
| **Multi-pack / pre-GC fetch** still walks the index | residual case neither A29 nor A30 covers | **cleared by #22** — `no_walk_set` marks all live packs and streams every live object in (pack,idx) order; the 200k walk bound now applies only to haves-ful incremental fetches |
| **No-walk clone during GC transition** | `index-pack: same object appears twice` on the public-read conformance clone | **cleared** — a sha can sit in two live packs between gc_commit and gc_sweep; `no_walk_set` now scans `objects` ordered by sha and marks only the first copy (unit test + conformance overlap-window regression) |
| **Import push expires during a >1h DO stall** | first TypeScript run: host disk filled (~17:59), froze all DO sqlite incl. the began_at heartbeat, ~35 min downtime pushed past `PUSH_TIMEOUT_MS` (1h), boot janitor swept push+TOC+staging | inherent — the heartbeat is correct; the guard is operational. Long imports need the host healthy. Dead MPU part blobs in dev state were reclaimed by hand (see below) |

## Next work

- **Re-bench rails post-C2/C1** (787k objects — biggest cloneable repo) once
  consolidated; confirm the react number generalizes. ✅ done — 718,383
  objects consolidated 23→1 pack, clone + `fsck --strict` clean.
- **TypeScript via I1** — ✅ done. 984,826 objects / 44 parts / ~1h52m /
  324 refs + HEAD exact / clone 1,020 s via URIs / fsck clean. First run
  was lost to host disk-full + push expiry (ceilings table); the rerun
  was a clean single pass (attempts 0, strands 0).
- P2 sweep — all done: #18 rename (resolved-wontfix, delete+repush
  documented), #19 per-ref token scopes (`scope` on mint, enforced in
  `apply_one` + `import_start`), #20 domain/Access docs, #21 LFS basic
  transfer (A33), #22 no-walk clone (incl. the cross-pack dedup fix).
- Push `p2-robustness` + open PR — carries C1's content (PR #18's merge never
  landed on main), so this merge resolves that anomaly.
- ~~Then the WILD section (TTL repos, GitHub-URL import, synthetic-ref seeding).~~
  WILD is open — GitHub-URL import shipped (PR #20). SHA-256 object format is
  now done too (was a COMPATIBILITY red row): `meta.obj_format` pins a repo,
  unpinned repos advertise both formats and the first write pins the client's
  pick; `POST /_admin/format {object_format}` pins explicitly; url-import
  captures the remote's advertised format. Format threads through
  ingest/resolve/generate/trailer widths (`kind.len_in_bytes()`), and the
  resumable pack writer got a checkpointable `CkptSha256` beside `CkptSha1`
  (`CkptHash` enum; old `sha:` checkpoint fields still deserialize — KAT +
  resume tests in `store::tests`). v3 bundle export emits
  `@object-format=sha256` when pinned — without it a sha256 client rejects the
  header. Verified: v0 pin-on-first-push, explicit pin, push+clone+`fsck
  --strict`+peel on 64-hex refs, sha1-into-sha256 refusal (client-side),
  sha256 fetch on sha1 repo (`ERR object-format sha256 does not match repo
  sha1`), loopback url-import of a sha256 repo, and the full conformance suite
  (`sha256:` block runs unconditionally; LIMITS cap is now exactly 7 — the
  suite peaks at 7 live repos/owner, the quota owner trips on its 8th claim).

**Bugs the rails GC + react 462k re-run flushed out** (all fixed): GC's
`gc.pos` only persisted at 8 MiB part boundaries, so a sparse-mark slice
(~1 R2 read/entry → ~6.4 MiB buffered inside the request budget) replayed
the same span forever — `gc_tail` now persists the undrained buffer across
yields with a scan/boundary-split cursor (A32). Import's durable-boundary
snapshot didn't claim mid-slice `flush_if_full` uploads, so `st.pos` could
freeze identically (same disease, second host) — `import_tail` now mirrors
`gc_tail`, restored after any boundary re-runs (a straddler re-run forces
discard: its bytes would be overwritten). `import_stage` minted a fresh
push per part — sibling `open` pushes timed out and the janitor swept
their `pending/` keys out from under the running job; parts now share the
import's push (`?push=&part=`), janitor sweeps by prefix. Three resumable-
import losses found by the 462k re-run: `scan_after` advanced before the
per-entry budget check, so each yield's boundary entry was scanned but
never processed (158 lost roots → 893 parked on missing bases); the
requeue probe ran FROM import_open so never-marked entries were invisible
(now FROM import_toc); the parked re-attempt loop had no budget guard and
drained c.parked via mem::take (now budget-gated with re-park). Job
liveness: strands (isolate death mid-slice) now count separately from
attempts (real errors) — strands reset per completed slice, cap 64, so
rebuild/restart churn can't dead-letter a healthy multi-hour import.
Dead-MPU recovery (A34): `finish` used to abort the output MPU on any
inner failure, so a retry looped on `uploadPart … does not exist` until
dead-letter — the job now probes the pack key on that error shape and
either commits the already-landed object from `import_open` spans or
wipes output state and re-emits onto a fresh MPU; imports finish via
`finish_resumable` (no abort) and a non-`open` pushes row ends the job
Done. Undeltified normalization means the 1.12 GiB react pack lands ~6×
bigger in R2 — `GE_QUOTA_MAX_BYTES` must be sized for the *stored*
footprint.

## Environment / workflow

- Build: `cargo check --target wasm32-unknown-unknown` in `server/`
  (`worker-build` lives in `~/.cargo/bin`).
- Dev server: `cd server && wrangler dev --port 8794` (wrangler `--var` uses
  `KEY:VALUE` colon syntax, not `=`).
- Conformance: `tests/conformance/run.sh` against a running dev server; flags
  `GE_CONFORMANCE_GC=1 GE_CONFORMANCE_PURGE=1 GE_CONFORMANCE_LIMITS=1
  GE_CONFORMANCE_URIS=1`, `GE_REPO` for owner prefix, `EDGE_BASE`,
  `EDGE_TOKEN`. URIS needs `GE_URL_SIGNING_KEY` in `.dev.vars` and git >= 2.40
  as `GE_GITBIN` (Apple git 2.39 lacks packfile-uris; Homebrew git works).
- GC test harness (PR0): `tests/harness/` — deterministic job stepping,
  failpoints, server-side fsck over index+R2. Dev server needs
  `GE_TEST_HOOKS=1 GE_TEST_MANUAL_JOBS=1 GE_GC_QUIET_MS=0 GE_GC_GRACE_MS=0`;
  see tests/harness/README.md.
- **Workflow rules**: commit/push/PR via `/atlas-engineering:commit-push-pr`;
  no PR template → fallback Summary/Changes/Testing format. Do NOT run
  `cargo fmt` globally — repo is not rustfmt-clean, it creates noise. Never
  put tokens in git URLs (credential helper / `GE_TOKEN` env only).
- Owner quota (50 repos default) accumulates across conformance runs — use a
  unique `GE_REPO` owner or expect registry-DO claim exhaustion.
- **miniflare dev-state leak**: dead/aborted MPU part blobs persist in
  `.wrangler/state/v3/r2` (`_mf_multipart_parts` rows + `blobs/` files) even
  after complete/abort — reclaim by deleting part blob files by `blob_id`
  then clearing both `_mf_multipart_*` tables (only when no live jobs).
  Orphaned `pending/` objects whose DO no longer exists can be deleted the
  same way via `_mf_objects`. Freed ~18 GiB after the TS expiry sweep.

## Local state on the old machine (won't transfer)

- `wrangler dev` on :8794 still running with imported repos (react at
  `packs_live:1` — handy for C2 verification; re-import takes ~6min via
  `tools/git-edge-import.sh` or `tests/bench/oss-bench.sh`).
- `/tmp/ts-bare.git` — TypeScript bare clone (2.8GB) for import-wall tests.
- `/tmp/ge-bench-*.log`, `/tmp/ge-bench-dev.log` — bench + worker logs.
- Two wedged Docker containers from AFS runs — `docker restart` daemon to
  reclaim.

## Backlog pointers

- `ROADMAP.md` — P2 #23/#24 are the clone fixes above; wild bucket has TTL
  repos, GitHub-URL import, synthetic-ref seeding (I2).
- Open research: upstream the artifact-fs `AFS_FSMONITOR_EXE` fsmonitor
  fork-bomb fix to `cloudflare/artifact-fs` (context in `findings/afs-e2e.md`).
