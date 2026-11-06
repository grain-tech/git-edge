# git-edge roadmap

Target profile: **agent-built small and disposable apps** — repos up to a few
hundred MB and tens of thousands of commits, high churn (create/push/dispose),
API-driven workflows, minimal ceremony. Not a monorepo host, not a GitHub
replacement.

## Can real repos fit? (benchmarked)

Public repos staged-pushed to a local worker via `tests/bench/oss-bench.sh`
(<60 MiB / <30k objects per slice), then cloned back:

| Repo | Commits | Objects | Pack size | Import | Clone | Result |
|---|---|---|---|---|---|---|
| sinatra/sinatra | 4,684 | 22.6k | 8 MiB | 1 push, 11 s | 2 s, fsck clean | fits |
| expressjs/express | 6,169 | 32.5k | 11 MiB | 2 pushes, 15 s | 8 s, fsck clean | fits |
| vitejs/vite | 9,678 | 110k | 75 MiB | 4 pushes, 337 s | 133 s, fsck clean | fits |
| facebook/react | 21,698 | 263k | 1,078 MiB | 25 pushes, 337 s | **18.7 s inline / 14.8 s via packfile-uris, fsck clean** | fits post-A29/A30 |
| rails/rails | 99,661 | 787k | 308 MiB | 39 pushes, 644 s | **fsck clean** — consolidated 23→1 pack (718,383 objects) then verbatim-cloned | fits post-A29/A30 |
| facebook/react — all refs | 21,698+branches/tags | 462k | 1,123 MiB | server-side import (1 job, dead-MPU rebuild survived) | 219 s for 6.41 GiB, fsck clean, 1,149 refs exact | fits via #25 |
| microsoft/TypeScript | 39,366 | 984,826 | 2,720 MiB | server-side import, ~1h52m, 44 parts | 1,020 s via packfile-uris (18.63 GiB normalized), fsck clean, 324 refs + HEAD exact | fits via #25 — inline clone needs `fetch.uriprotocols` past ~7 GiB wire |

**Re-bench post-A29/A30 (C2/C1):** the react clone that used to die at HTTP 413
now streams the consolidated pack verbatim — `x-ge-subrequests: 1/9000`, one R2
GET for the whole 1.08 GiB. The URI-offloaded variant moves pack bytes off the
Worker entirely. The remaining wall was import-side only.

**The binding constraints are now mapped.** Import scales far past the profile
(rails' 787k objects landed fine in 39 slices); the walls that bite are:

- **~100 MB request body cap** on initial import — staged pushes (or the
  `tools/git-edge-import.sh` slicer) are the workaround; for a pack that can't
  be sliced (one giant commit), `POST /_admin/import` runs the ingest as a job.
- ~~**Single-commit object explosions can't be staged**~~ — `/_admin/import`
  (I1) ingests a staged pack across resumable job slices; a 222k-object commit
  no longer has to fit inside one request. Conformance covers stage → job →
  commit → clone/fsck; the 462k-object react pack validates scale in flight.
- **Per-request subrequest budget on clone/fetch** — cleared by A29: a
  consolidated single-pack repo streams verbatim at ~1 subrequest instead of
  thousands. Non-consolidated or have-heavy fetches still walk the index.
  Everything else has 5–25× headroom at profile scale (200k-commit walk,
  1M objects/fetch, 2M objects/pack, 2 GiB pending pack, 65k refs).

For the small-app profile — typically well under 50k objects — both clone and
import sit comfortably inside limits. Repos in the react/rails class import
fine and clone fine once GC consolidates them to one live pack.

The original (private-repo) benchmark flushed out two real bugs, both long fixed:

- **Clone 413 on wide histories** — `read_entries` bounds one call at 48 MiB of
  coalesced spans; a 10k-tree chunk or a multi-pack commit prefetch can exceed
  it. `read_entries_chunked` now splits the set and retries the halves.
- **Dangling `HEAD` on non-`main` first push** — `meta.head` was hardcoded to
  `refs/heads/main`; a `master`-first repo cloned without checkout. `commit_push`
  now adopts an existing branch when the configured head is dangling.

## Priority 0 — required for the profile

| # | Item | Why | Size |
|---|---|---|---|
| 1 | ~~Fetch read-batch splitting~~ | done — was a hard clone failure at 70k+ objects | S |
| 2 | ~~HEAD adoption on first push~~ | done — every scaffolded repo pushes `master` | XS |
| 3 | ~~**Repo delete**~~ | done — `POST /_admin/delete` tombstones (410), `purge_repo` wipes R2+DO, name reusable after | M |
| 4 | ~~**Bulk import path**~~ | done via option (a) — `tools/git-edge-import.sh` auto-slices a repo into <SLICE_MIB pushes with resume, `--all-branches`, `--dry-run`; option (b) `POST /_admin/import` (R2 bundle/pack) remains the structural fix | S–M |
| 5 | ~~**Anonymous/public read**~~ | done — `meta.public` flag via `POST /_admin/public`; anonymous reads only when no credential presented | S |
| 6 | ~~**Dead-job / GC alerting**~~ | done — `blob3=dead` job datapoints + `jobs_dead` gauge per alarm pass; alert wiring documented (A18) | S |
| 7 | ~~**Job-lifecycle metrics**~~ | done — `job_event` datapoints: kind/event/outcome/error-class/attempt/duration (A17) | S |
| 8 | ~~**Agent skill**~~ | done — `.devin/skills/git-edge/SKILL.md`: URL shape, credential-helper recipes (never tokens in URLs), `_admin/tokens` mint/revoke, staged-push recipe for >80 MiB, `_state` introspection, limits table incl. A29/A30 clone scale | S |

## Priority 1 — production hardening

| # | Item | Why | Size |
|---|---|---|---|
| 9 | ~~Repo quota + abuse limits~~ | done — `GE_QUOTA_MAX_REPOS_PER_OWNER` (50) via `owner!<o>` registry DO, `GE_QUOTA_MAX_OBJECTS` (2M)/`GE_QUOTA_MAX_BYTES` (4 GiB) at `commit_push` (A26) | M |
| 10 | ~~Rate limiting~~ | done — sliding-window `rate` table check in `push_begin`, `GE_RATE_PUSHES_PER_MIN` (30), 429 + `Retry-After` (A27); zone rules remain the heavy hammer | S |
| 11 | ~~**Export endpoint**~~ | done — `GET /_admin/export` streams a real v3 `git bundle`; verify + clone-from-bundle pass (A25) | M |
| 12 | ~~**`x-ge-subrequests` on error responses**~~ | done — request-wide `Spend` tally + `ReqBudget::reporting`; `respond` stamps every error (and non-streamed success), DO stamps its own errors, `from_do_response` folds DO spend in | XS |
| 13 | ~~**Lease-overlap hardening**~~ | done — heartbeat is a lease CAS; stale slices bail as `stale` without consuming attempts (A19) | S |
| 14 | ~~**`coalesce` `unwrap_or(u32::MAX)`**~~ | done — loud `Error::Limit` → 413 (A21) | XS |
| 15 | ~~ls-refs caching~~ | done — DO memoizes the refs snapshot + `/_do/refs`/`/_do/ls-refs` bytes on `refs_version` (A28); `_state` reports hits/misses | S |
| 16 | ~~**Ref pinning**~~ | done — `pins` table + `/_admin/pin|unpin`; push to a pinned ref gets `ng "ref is pinned"` (A24) | S |

## Priority 2 — worthwhile, not blocking

| # | Item | Why | Size |
|---|---|---|---|
| 17 | ~~**`--atomic` push**~~ | done — advertised; `commit_push` dry-runs every command's CAS predicate before any write, whole push rejects on any failure (git's `atomic push failed` wording, pack demotes for the janitor) | M |
| 18 | ~~**Repo rename / owner move**~~ | resolved-wontfix — `id_from_name(owner/repo)` makes the DO name immutable; a registry indirection on every hot path is disproportionate for cosmetics. Delete+repush is the documented recipe (SKILL.md) | S |
| 19 | ~~**Per-ref token scopes / deploy keys**~~ | done — `scope` on token mint (full `refs/…` pattern, optional trailing `*`), captured into `pushes.scope` at begin, enforced centrally in `apply_one` for pushes and `import_start` for imports; `token_list` exposes it | M |
| 20 | ~~**Custom domain + Cloudflare Access**~~ | done — deployment/config documentation in PRODUCTION-UAT.md (workers.dev vs custom domain, Access in front, bindings/secrets/rollback) | S |
| 21 | ~~**Git LFS**~~ | done — basic transfer: `POST /info/lfs/objects/batch` + HMAC-signed `GET|PUT /_lfs/<oid>` URLs (A30 key, `lfs`-domain-separated sig binds repo+oid+expiry+op); objects under `r/<id>/lfs/` inside the purge prefix; upload quota = packs + lfs + declared ≤ `GE_QUOTA_MAX_BYTES`; no verify/locking/custom transfers (A33) | L |
| 22 | ~~**Streaming no-walk clone**~~ | done — `no_walk_set` marks every live-pack bit and rides `plan_reads`/`FetchStream`; plain clones stream all live objects in (pack_id, idx) order, no 200k-commit walk | M |
| 23 | ~~**Verbatim consolidated-pack fast path**~~ | done — packs_live=1 + plain-clone shape streams the live pack verbatim as the fetch response; 1 subrequest per R2 `get` instead of thousands of entry reads (A29). Lifts the 110k–263k-object clone wall for default clients once a repo consolidates | M |
| 24 | ~~**`packfile-uris` offload**~~ | done — same coverage test as #23; opted-in clients (`fetch.uriprotocols`) get a signed `/_packs/<id>.pack` URL (HMAC-SHA256 over repo_id+pack+expiry, `GE_URL_SIGNING_KEY`), inline packfile is a valid empty pack, hash token is the real trailer SHA-1 (A30). Clone spend ~4 subrequests; bandwidth bypasses the Worker | M |
| 25 | ~~**Server-side resumable import (I1)**~~ | done — `POST /_admin/import/stage` (stream a pack part to `pending/`) + `POST /_admin/import` starts an `import_pack` job that ingests the staged pack across alarm slices: pass A parses a persistent `import_toc`, pass B normalizes entries into a resumable output MPU with `import_open`/`import_parts` crash-resume, links live in `push_links` (no 1M cap), commit rides `commit_push` atomically. Kills the single-commit ingest ceiling | L |

## Wild bucket — parked, worth remembering

- **Synthetic-ref object seeding** — split an oversize commit's objects across
  staging refs so the real push dedups against them; client-side fix for
  TypeScript-class imports, zero server changes (`findings/scale-ceilings.md` I2).
- ~~**GitHub URL import**~~ — done: `POST /_admin/import {url}` makes the
  Worker the git client — v2 `ls-refs` mints the command set (capped at 100k
  refs, gated by the push's token scope), one `fetch` streams the remote's
  pack through a trailer-verifying hasher into `pending/`, then the ordinary
  `import_pack` phases own it. Verified against github.com (octocat/Hello-World:
  10,163 objects, 3,626 refs + HEAD adopted) and in conformance over loopback.
  Public sources only — no credentials are sent; http is loopback-only.
- **TTL repos** — `expires_at` on meta; janitor sweeps expired repos. The
  purest expression of "disposable".
- **Bundle/PR-preview conventions** — `refs/pr/*` namespace with automatic
  retention policy.
- **Signed-push / commit-signature policy** — enforce `gpgsig` on push for
  release branches.
- ~~**SHA-256 object format**~~ — done: `meta.obj_format` pins a repo
  (`sha1`|`sha256`), unpinned repos advertise both formats and the first write
  pins the client's pick; `POST /_admin/format` pins explicitly. The format
  threads through negotiation, ingest, delta resolution, fetch traversal,
  pack trailers, resumable writers (`CkptSha256` beside `CkptSha1`), exports,
  and both import paths. Conformance covers push→clone→fsck on 64-hex repos
  plus the sha1→sha256 mismatch refusal.
- **v0/v1 fetch** (pre-2.26 clients), **`tree:`/`combine:` filters**,
  **`object-info`**, **`bundle-uri`** —
  deliberately unadvertised; add when a client actually needs them.
- **Jurisdiction-pinned DOs** — EU repos in EU DOs if compliance demands it.
- **artifact-fs compatibility** — decided: client-compatible, not a server
  feature. Cloudflare's artifact-fs is an open-source FUSE lazy-mount client
  that works with *any* git remote, and git-edge already speaks everything it
  needs (v2, `blob:none`, promisor backfill, `allowAnySHA1InWant`) — its e2e
  suite passes against git-edge (`findings/afs-e2e.md`). Remaining work is
  docs-only: document the mount path for users. Building our own mount
  client is not on the table.

## Explicit non-goals

- Monorepo-scale hosting (multi-GB packs, million-commit walks)
- Web UI / code browsing / issue tracking — this is a storage service
- Hooks (`pre-receive`, `post-receive`) — no user code runs in the DO
- Self-serve multi-tenant SaaS — auth is tokens, not accounts
- Managed fork graph — reserved as the deferred out-of-tree sister
  `idl3/git-edge-fork`; core has no fork concept
- Product notes API / notes JSON layer — no product surface; core already
  round-trips raw `refs/notes/*` as ordinary refs, which is all stock
  `git notes` needs
