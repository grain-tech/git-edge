# git-edge — feature & compatibility reference

What git-edge is: a Git smart-HTTP server compiled to WASM on a Cloudflare
Worker. One SQLite Durable Object per repository holds refs, the object index,
and job state; R2 holds normalized packfiles. There is no always-on machine and
no upstream Git service in the request path. All claims below are verified
against a real stock `git` client on local workerd **and** against a live
Cloudflare deployment (`git-edge.<acct>.workers.dev`, Paid plan); see
`findings/implementation.md` for the test evidence.

## Transport & protocol versions

| Surface | Upstream git | git-edge | Status |
|---|---|---|---|
| Smart HTTP `GET info/refs?service=git-upload-pack` | v0/v1/v2 | v2 advertisement; v1 emits a `version 1` packet | ✅ |
| Smart HTTP `POST git-upload-pack` (fetch/clone) | v0/v1/v2 | **protocol v2 only** — v0/v1 POST → HTTP 400 `ERR protocol v2 required (git >= 2.26)` | ⚠️ git < 2.26 cannot fetch |
| Smart HTTP `info/refs?service=git-receive-pack` + `POST git-receive-pack` (push) | v0 | v0 receive-pack (same as upstream — git clients never speak v2 push) | ✅ |
| `git://` protocol | ✅ | — | ❌ HTTP(S) only |
| SSH transport | ✅ | — | ❌ HTTP(S) only |
| Dumb HTTP protocol | ✅ | — | ❌ |

**Client floor:** any git ≥ 2.26 (March 2020) clones, fetches, and pushes
normally. Older clients fail fetch with a clean protocol error, not a hang or
corruption. (Cloudflare Artifacts negotiates v1 fetches too; git-edge is
v2-only on `git-upload-pack`.)

## Advertised capabilities (v2)

```
version 2
agent=git-edge/0.1
ls-refs=unborn
fetch=shallow filter packfile-uris
object-format=sha1        # the repo's pinned format; unpinned repos list both
object-format=sha256      #   sha1 and sha256 — first write pins the client's pick
```

receive-pack: `report-status delete-refs side-band-64k quiet ofs-delta atomic
object-format=sha1` — same pinning rule; `object-format=` appears once per
supported format while the repo is unpinned.

## Fetch / clone (upload-pack)

| Feature | Upstream | git-edge | Notes |
|---|---|---|---|
| `want`/`have`/`done` negotiation | ✅ | ✅ | multi-round with ACK/NAK; `ready` gating matches git |
| Stateless multi-round fetch | ✅ | ✅ | verified with partial-ACK rounds |
| `want-ref` (fetch ref by name) | ✅ | ✅ parsed & resolved | `ref-in-want` is not advertised, so stock clients won't send it |
| `want` by arbitrary oid (`allowAnySHA1InWant` equivalent) | off by default | **always on** | required for promisor backfill; see caveats |
| `deepen <n>` | ✅ | ✅ | descends through client-shallow commits; deepens correctly from depth-1 clones |
| `deepen-since <ts>` | ✅ | ✅ | |
| `deepen-not <ref>` | ✅ | ✅ | ref search order (`mid` → `refs/tags/mid`); annotated tags peeled; unresolvable → protocol error |
| `deepen-relative` | ✅ | ✅ | depth counted beneath the old shallow boundary |
| `--unshallow` | ✅ | ✅ | `unshallow` emitted only when parents are actually delivered |
| `shallow`/`unshallow`/`shallow-info` lines | ✅ | ✅ | client-held commits never re-emitted as `shallow` |
| `filter blob:none` | ✅ | ✅ | promisor backfill works (verified checkout) |
| `filter blob:limit=<n>` | ✅ | ✅ | |
| `filter tree:<n>`, `sparse:oid`, `combine:`, `object:type=` | ✅ | — | clean `unknown argument` protocol error |
| `thin-pack` arg | ✅ | accepted | packs sent are self-contained — deltas never appear on the wire |
| `include-tag` | ✅ | ✅ | peeled tags of wanted commits only |
| `sideband-all` | ✅ | — | not advertised; plain side-band-64k semantics |
| `packfile-uris` (CDN offload) | ✅ | ✅ | advertised; opted-in plain clones get a signed `/_packs` URL (A30) |
| `wait-for-done` | ✅ | — | not advertised |
| `no-done` | ✅ | tolerated, not advertised | parsed; we answer before `done` anyway |
| `object-info`, `bundle-uri`, `server-option` commands | ✅ | — | unknown command → protocol error |
| `session-id` | ✅ | ignored | parsed, not echoed |
| Fetch of hidden/unadvertised refs | ✅ | ✅ via `want-ref` (unadvertised) or raw oid | |

## Push (receive-pack)

| Feature | Upstream | git-edge | Notes |
|---|---|---|---|
| Create / update / delete refs | ✅ | ✅ | incl. delete-only pushes (no PACK) |
| Compare-and-swap (`old-oid` guard, `--force-with-lease`) | ✅ | ✅ | stale push rejected; verified under concurrency |
| Thin packs (delta vs repo objects) | ✅ | ✅ | forward-**and** backward `REF_DELTA`, nested chains, `OFS_DELTA` |
| `report-status` | ✅ | ✅ | |
| `report-status-v2` | ✅ | accepted, not advertised | option lines not supported |
| `delete-refs`, `side-band-64k`, `quiet` | ✅ | ✅ | |
| `atomic` | ✅ | ✅ | advertised; dry-run validation rejects the whole push on any failure |
| `push-options` (`-o`) | ✅ | — | not advertised → client-side refusal |
| Signed pushes (`push-cert`) | ✅ | — | |
| `shallow` lines from a shallow client | ✅ | parsed, ignored | push proceeds; see caveats |
| Push to unborn/empty repo | ✅ | ✅ | |
| Flush-only push ("Everything up-to-date") | ✅ | ✅ | |

## Refs & object model

| Feature | Status |
|---|---|
| Full ref namespace (`refs/heads`, `refs/tags`, `refs/remotes`, `refs/notes/*`, arbitrary) | ✅ |
| Annotated tags + `peel` in ls-refs | ✅ |
| Symrefs (`symrefs` arg, `HEAD`) | ✅ |
| Unborn `HEAD` (`ls-refs=unborn`) | ✅ |
| `ref-prefix` filtering (≤ 32 prefixes) | ✅ |
| SHA-1 object format | ✅ |
| SHA-256 repos | ✅ | pin on first push (v0 cap) or `POST /_admin/format`; v2 `object-format` negotiated per request; clone/push/tag-peel/url-import/export/`fsck --strict` all verified on 64-hex repos; a mismatched client is refused (`ERR object-format sha256 does not match repo sha1`) |
| `git fsck --strict` on clones | ✅ clean across all tests |

`HEAD` defaults to `refs/heads/main` at repo creation; on the first push that
lands a branch while `HEAD` is dangling (e.g. a `master`-first repo), the
server adopts an existing branch — clones check out like a GitHub import.
`POST /_admin/import {url}` imports a public remote directly (no staging): the
fetch phase mints one create-command per advertised ref and adopts the remote's
HEAD symref, so a url import lands exactly the source's tip set.

## Storage & maintenance model

| Property | Upstream `git http-backend` | git-edge |
|---|---|---|
| Pack storage | loose + packed on disk | normalized full-object packs in R2, indexed in per-repo SQLite |
| Wire deltas on fetch | yes (thin packs) | **never** — all entries sent as full objects (larger fetch bodies, simpler/constant memory) |
| Delta compression at rest | yes | no — deltas resolved at ingest, stored inflated |
| GC / repack | `git gc`, manual or auto | automatic mark→consolidate→sweep via DO alarms (`GE_GC_QUIET_MS` 10 min, `GE_GC_GRACE_MS` 1 h); opt-in barrier mode (`GE_GC_BARRIER`) for mark-under-write; automatic tiered compaction (`GE_COMPACT_FANIN`/`_MIN_AGE_MS`/`_MAX_ROWS`) bounds live-pack growth without reachability; `GE_MAX_LIVE_PACKS` caps live+ingesting packs |
| Job slices | n/a | 20 s wall / 400 subrequest slices, resumable cursors, lease-fenced, dead-lettered |
| Quarantine on push | quarantine dir | `pending/` R2 key + `open` push row; ref update is a CAS transaction |

## Hard limits (vs upstream)

| Limit | git-edge | Upstream |
|---|---|---|
| Full object (blob/commit/tree/tag pushed whole) | **~2 GiB** (the pending-pack bound) — streamed verbatim, never materialized | none in git (GitHub: 100 MiB) |
| Delta *result* object | **16 MiB** — delta resolution materializes in isolate memory | none |
| Non-delta commit/tree/tag | 16 MiB (parsed for links; always tiny in practice) | none |
| Pack entry wire size (non-blob) | 32 MiB | none |
| Pending pack per push | 2 GiB | none (disk) |
| HTTP request body per push (Cloudflare platform) | **~100 MB** — larger pushes must be split (`git push <mid>:main` then `git push main`) | none |
| Objects per pack | 2,000,000 | none |
| Delta chain depth | 64 (git default: 50) | unbounded-ish |
| Aggregate delta-chain bytes | 64 MiB compressed / 128 MiB live | none |
| Refs per repo | 65,536 | none |
| Fetch commit-walk bound (haves-ful fetches) | 200,000 commits or 64 MiB mem → `ERR fetch too large` | none |
| Reachable objects per fetch | 1,000,000 | none |
| Clone object bound | none — plain clones stream all live objects in pack order (no walk); verified ≥718k objects | n/a |
| Push links (tree edges) per push | 1,000,000 | none |
| upload-pack request body | 1 MiB | ~unbounded |
| ls-refs prefixes | 32 | unbounded |
| Per-request budget | 9,000 subrequests / 240 s | n/a (self-hosted) |

Every limit fails with a clean `ERR`/`unpack`/`ng` line — never a hang, silent
truncation, or corrupted ref state.

## Caveats & known mismatches

1. **git < 2.26 cannot fetch.** Protocol v2 is mandatory on `git-upload-pack`.
   Push is unaffected (all gits speak v0 receive-pack anyway).
2. **Any reachable oid is fetchable** (`allowAnySHA1InWant` semantics always
   on). Required so partial-clone promisor backfill works. Upstream leaves it
   off by default — here it is a deliberate choice, not an oversight.
3. **No wire deltas, ever.** `thin-pack`/`ofs-delta` requests are accepted but
   responses carry full objects only. Legal per protocol; fetch bodies are
   larger than a deltifying server's. This is what makes constant-memory
   streaming possible.
4. **`-o <push-option>` refuses client-side; `--atomic` works.** Push options
   are not advertised, so the client errors before the request — clean.
   `--atomic` is advertised and honored: `commit_push` dry-runs every
   command's CAS predicate before any write and rejects the whole push on
   any failure.
5. **Shallow-push tracking is not maintained.** `shallow` lines from a shallow
   client are parsed and ignored; the server does not remember that a pushed
   history was truncated. Consequence is benign (objects are stored; the
   connectivity rule still requires referenced bases to exist).
6. **v0/v1 fetch, `tree:`/`sparse:`/`combine:` filters, `sideband-all`,
   `object-info`, `bundle-uri`** — deliberately unadvertised;
   clients get a protocol error, never silent misbehavior.
   (SHA-256 is supported — the repo's format is pinned by the first write or
   `POST /_admin/format`, and every advertised `object-format=` line names an
   algo the server actually honors.)
7. **Auth is static tokens, but now per-repo too.** `GE_READ_TOKEN`/
   `GE_WRITE_TOKEN` remain the deployment-wide admin credentials (HTTP Basic or
   Bearer). Per-repo tokens are minted via `POST /:owner/:repo/_admin/tokens`
   `{name, level}` (global-write-token only — repo credentials cannot mint more),
   listed via `GET`, revoked via `DELETE /_admin/tokens/<id>`. Only sha1 hashes
   are stored; a token is shown once at creation. Read tokens get 403 on push.
   `POST /_admin/public {enabled}` opens a repo to anonymous reads (ls-refs,
   fetch, export) — a request with no credential is admitted; a presented bad
   token still gets a 401. Tokens accept an optional `scope`: a comma list
   of `refs/…` patterns (a trailing `*` makes the pattern a prefix glob),
   enforced on every ref update a push or import attempts — e.g.
   `refs/heads/scoped-*` lets a deploy key push only matching branches.
   No user accounts.
8. **Git LFS: basic transfer only.** The batch endpoint
   `POST /:owner/:repo/info/lfs/objects/batch` answers the spec and mints
   HMAC-signed `GET|PUT /_lfs/<oid>` URLs (same capability model as
   `/_packs/`; the sig binds repo+oid+expiry+op). Objects live in R2 under
   `r/<id>/lfs/` — inside the repo's purge prefix — and count toward
   `GE_QUOTA_MAX_BYTES`. Requires `GE_URL_SIGNING_KEY`; without it the
   batch route 403s. No `verify` callback, locking API, or custom
   transfers. Without LFS, full objects stream verbatim up to the 2 GiB
   pending-pack bound, but anything pushed *as a delta* whose result
   exceeds 16 MiB is still rejected (`unpack object too large`), and a
   REF_DELTA whose *base* is a streamed >16 MiB object is rejected with
   `delta base <oid> exceeds 16 MiB`. Note `git push --no-thin` does
   **not** prevent a client from sending REF_DELTA (verified on git 2.54)
   — the reliable workaround is pushing the object undeltified, e.g.
   `git -c core.bigFileThreshold=1 push`. The ~100 MB platform body cap
   applies per request.
9. **No hooks, repo rename, or web UI.** A repo is created by pushing to it and
   deleted by `POST /_admin/delete` (write-auth): the repo tombstones to 410
   immediately and a `purge_repo` job reclaims R2 packs and DO storage in the
   background. Other management endpoints: `/_state` (write-auth introspection),
   `/_admin/tokens`, `/_admin/public`, `/_admin/pin` + `/_admin/unpin`
   (read-auth `GET /_admin/export` streams a v3 `git bundle` of all live refs).
   Basic request metrics (op, status, ms, subrequests per
   repo) are emitted to Analytics Engine when the `GE_METRICS` binding exists.
10. **Real-deploy verified:** R2 multipart semantics, DO alarms, and Paid-plan
    subrequest limits are all confirmed against a live deployment
    (`git-edge.<acct>.workers.dev`); the ~100 MB body cap is real.

## Verified performance envelope

Published headline numbers come from `tests/bench/oss-bench.sh` against a real
Paid-plan Worker + Durable Object + R2 deploy — see the README benchmark
table. The rows below are the wider envelope: **local workerd unless marked
(prod)**.

| Workload | Result |
|---|---|
| Push 20k-commit linear history (60k objects) | 8.8 s |
| Clone 20k commits | 4.3 s — 5/9000 projected subrequests |
| Clone `--depth 15000` | 1.4 s, fsck clean |
| Push 200 MiB / 10,207 objects | 21 s |
| 4× parallel 20k clones | 6.9 s total |
| 5× parallel CAS-divergent pushes | all correctly rejected |
| GC during concurrent clone (60k objects) | mark→sweep < 4 s, clone clean |
| Nested forward `REF_DELTA` pack | resolved, byte-exact |
| Cyclic `REF_DELTA` pack | `unpack missing base` in ~60 ms |
| Push 30 MiB single blob (prod) | 9 s, clone fsck-clean, byte-identical |
| Push 20 MiB blob → GC consolidate → clone (local) | streamed verbatim copy, fsck clean |
| Push ~95 MiB pack (prod) | 35 s — near the ~100 MB platform body cap |
| Push ~130 MiB pack (prod) | HTTP 413 at the zone before app code — split the push |
| Import sinatra/sinatra (4,684 commits, 22.6k objects, 8 MiB pack) | 1 push, 11 s; clone 2 s, fsck clean |
| Import expressjs/express (6,169 commits, 32.5k objects, 11 MiB pack) | 2 staged pushes, 15 s; clone 8 s, fsck clean |
| Import vitejs/vite (9,678 commits, 110k objects, 75 MiB pack) | 4 staged pushes, 337 s; clone 133 s, fsck clean |
| Import facebook/react (21,698 commits, 263k objects, 1,078 MiB pack) | 25 staged pushes, 337 s; clone 18.7 s @ 1 subrequest verbatim / 14.8 s via signed `packfile-uris` — the budget wall is cleared once consolidated |
| Import facebook/react — all 1,149 refs (462,299 objects, 1.12 GiB pack) | server-side import, committed across ~7 isolate strands incl. a dead-MPU rebuild; clone-back 6.41 GiB in 219 s, `fsck --strict` clean, refs exact |
| Import rails/rails (99,661 commits, 787k objects, 308 MiB pack) | 39 staged pushes, 644 s; GC consolidated 23→1 live pack (718,383 objects); clone + `fsck --strict` clean |
| Import microsoft/TypeScript (39,366 commits, 984,826 objects, 2.72 GiB pack) | server-side import in ~1h52m (44 parts, zero retries); normalized to one 18.63 GiB live pack; clone 1,020 s via signed `packfile-uris`, `fsck --strict` clean, 324 refs + HEAD exact. Inline verbatim clone exceeds the 240 s request wall past ~7 GiB wire — set `fetch.uriprotocols` |

## Interoperability test matrix (git 2.54, live)

`clone`, `clone --depth`, `clone --filter=blob:none` + promisor checkout,
`fetch`, `fetch --depth/--deepen/--shallow-since/--shallow-exclude`,
`--unshallow`, `push` (initial, thin-delta incremental, branch, tag,
delete-only, force-with-lease CAS), `ls-remote`, multi-round negotiation —
all pass, all clones `fsck --strict` clean. Full details:
`findings/implementation.md`, `tests/conformance/run.sh`.
