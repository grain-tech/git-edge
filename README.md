# git-edge

git-edge is a git server that has no server. git keeps every version of a set
of files and lets many people share them; normally that needs a machine that
runs all day. git-edge uses Cloudflare's network instead: one small program per
project keeps the branch list, a large file store keeps the contents, and
nothing has to be managed, patched, or restarted.

## What this is — and what it isn't

**Agentic *and* Cloudflare-native git hosting.**

- Built for agent fleets: many small, ephemeral repos — create-by-push, one
  SQLite Durable Object per repo as the isolation boundary, billed on
  requests and stored bytes.
- Runs as a Worker + one Durable Object per repo + R2 pack storage on *your*
  Cloudflare account.
- Speaks stock git over **HTTPS smart HTTP only** — protocol v2 fetch, v0
  receive-pack (git ≥ 2.26).

**Not a drop-in replacement for a classic always-on git server or GitHub:**

- No `git://`, no SSH, no dumb HTTP. No Issues/PRs/Actions/UI, no
  server-side hooks, no org/IAM product surface.
- "Self-host" means deploy this Worker to a Cloudflare account — not a
  bare-metal `git-daemon`.
- Limits that matter to operators: ~100 MB HTTP body cap per push, no wire
  deltas on fetch, 16 MiB delta-result cap, undeltified R2 packs. The full
  matrix is in [COMPATIBILITY.md](COMPATIBILITY.md).

## vs Cloudflare Artifacts

Cloudflare [Artifacts](https://blog.cloudflare.com/artifacts-git-for-agents-beta/)
is a **managed** product: versioned storage that speaks Git,
agent-first, operated by Cloudflare. Git objects live primarily in each
repo's Durable Object SQLite (chunked under the ~2 MB row cap), R2 holds
snapshots, and access is a native Workers API (create / fork / import, token
minting) plus REST and a plain git remote — with first-class forks and
git-notes, and [ArtifactFS](https://developers.cloudflare.com/artifacts/concepts/how-artifacts-works/)
for lazy-mounting large repos from any remote. Garbage collection is managed
internally
([how it works](https://developers.cloudflare.com/artifacts/concepts/how-artifacts-works/)).

| | Cloudflare Artifacts | git-edge |
|---|---|---|
| What it is | Managed CF product: versioned storage that speaks Git, agent-first | Open-source Worker you deploy on your own CF account |
| Who runs it | Cloudflare operates it | You (`wrangler deploy`) |
| Object storage | Git objects primarily in the DO SQLite (chunked ~2 MB rows); R2 for snapshots | Packfiles in R2; the DO holds refs, the object index, and job/GC state |
| API surface | Native Workers API (create/fork/import + mint tokens) + REST + git remote | Stock git smart-HTTP + `/_admin/*` (tokens, import, delete, pin, export) |
| Forks / notes | First-class fork + git-notes | No managed fork graph; notes are not a product feature |
| Large repos | ArtifactFS lazy mount (works with any git remote) | Consolidated single-pack clone / `packfile-uris`; artifact-fs listed as a future idea in [ROADMAP.md](ROADMAP.md) |
| Continuous-write GC | Managed internally | Explicit alarm-driven GC: mark → consolidate → sweep, opt-in `GE_GC_BARRIER`, G4a compaction (`GE_COMPACT_*`), live-pack admission (`GE_MAX_LIVE_PACKS`) |

They are complementary, not competitors-to-conflate: Artifacts is the managed
product; git-edge is the open-source, self-deployed remote for when you want
pack-in-R2, operator-visible GC, and full control of the Worker on your own
account.

## What works today

All of this is exercised by the conformance suite (`tests/conformance/run.sh`)
against a real `git` client; every listed clone lands `git fsck --strict`
clean.

- **Clone, fetch, push over smart HTTP.** Protocol v2 fetch (multi-round
  want/have, `deepen`, shallow, `blob:none`/`blob:limit` filters with
  promisor backfill, `want-ref`, any-oid fetch) and v0 receive-pack
  (create/update/delete, force-with-lease CAS, delete-only pushes, thin packs
  both directions, `--atomic` pushes).
- **Per-repo credentials.** Deployment-wide read/write tokens plus per-repo
  `ge_*` tokens minted on `/_admin/tokens` with optional ref scopes — deploy
  keys that may only push `refs/heads/release-*`, say. Anonymous read is a
  per-repo flag.
- **Repo lifecycle.** Pin a ref to freeze it; `/_admin/delete` tombstones and
  a purge job reclaims the bytes; `/_admin/export` streams a real
  `git bundle`.
- **Git LFS** basic transfer (batch API + HMAC-signed URLs; no
  verify/locking/custom transfers).
- **Server-side import.** `POST /_admin/import {"url": …}` makes the Worker
  the git client — the path for repos whose history can't fit the ~100 MB
  request cap (proven on facebook/react's 462k objects and
  microsoft/TypeScript's 984k). `tools/git-edge-import.sh` slices medium
  repos into staged pushes client-side.
- **Quotas + abuse limits.** Per-owner repo caps, per-repo object/byte caps,
  per-credential push rate limiting, live-pack admission cap.
- **Self-healing jobs.** GC (mark → consolidate → sweep, plus opt-in barrier
  mode and automatic tiered compaction), imports, purges, and push webhooks
  run as alarm-driven jobs with resumable cursors and lease fencing.

## Benchmarks — live Cloudflare Worker + Durable Object

Headline numbers are measured against a real Paid-plan deploy — a Worker +
per-repo Durable Objects + R2 on a Cloudflare account — with a stock `git`
client, via `tests/bench/oss-bench.sh`. Local `wrangler dev` / workerd is for
smoke and conformance, not the published table. Reproduce with
[INSTALL.md](INSTALL.md) + [QUICKSTART.md](QUICKSTART.md).

Measured 2026-11-06 on a live deploy (stock git 2.47), staged pushes of
≤8k objects / ≤30 MiB each — sized under the per-request ingest wall:

| Repo | Commits | Objects | Pack | Pushes | Import | Clone-back | fsck |
|---|---|---|---|---|---|---|---|
| pallets/flask | 5,566 | 26k | 13 MiB | 5 | 91 s | 14 s | clean (5 pre-existing) |
| sinatra/sinatra | 4,684 | 23k | 8 MiB | 4 | 71 s | 9 s | clean (3 pre-existing) |
| expressjs/express | 6,186 | 33k | 11 MiB | 6 | 83 s | 12 s | clean |

"clean (N pre-existing)" means the clone's `fsck --strict` output matches the
source repo's own findings byte-for-byte — old repos carry quirks like
`zeroPaddedFilemode` that a faithful clone reproduces. Clone-backs here are
pre-consolidation multi-pack clones; after GC merges the pushes into one live
pack, clones get cheaper still.

To run it yourself against your deploy:

```bash
EDGE_BASE=https://git-edge.<acct>.workers.dev EDGE_TOKEN=… \
BENCH_PREFIX=bench BENCH_REPOS="https://github.com/pallets/flask" \
  tests/bench/oss-bench.sh        # -> bench/flask, one markdown row per repo
```

The shape that matters: each push lands as its own pack, and GC consolidates a
repo toward one live pack — after which a plain clone streams that pack
verbatim at ~1 R2 read regardless of history size. Clients that opt into
`fetch.uriprotocols` get a signed `/_packs/…` URL and the pack bytes never
touch the Worker at all. Undeltified storage is the honest trade: R2 bills
bytes at rest, and `GE_QUOTA_MAX_BYTES` bounds it.

For the big-repo envelope (react, rails, TypeScript — 263k–985k objects),
server-side import and `packfile-uris` clone-back are verified on local
workerd and partially on prod; the full matrix with per-row provenance is in
[COMPATIBILITY.md](COMPATIBILITY.md#verified-performance-envelope) and
[ROADMAP.md](ROADMAP.md#can-real-repos-fit-benchmarked).

## The walls, and the way around each

Every limit below was hit by a real run against a real repository; each had a
seam. Full detail in [COMPATIBILITY.md](COMPATIBILITY.md).

| Wall | The way around it |
|---|---|
| **9,000 subrequests / request** | GC consolidates toward one live pack that streams verbatim at ~1 subrequest; multi-pack clones take a no-walk path |
| **~100 MB request body** | `tools/git-edge-import.sh` slices history into fast-forward pushes; `/_admin/import` ingests a staged pack as a job |
| **The unsplittable commit** (e.g. TypeScript's ~222k-object single commit) | Server-side import parses, resolves deltas, and commits across alarm slices |
| **Isolate death mid-job** | Resumable cursors over durable state; strand-vs-error accounting so a rebuild can't exhaust retries |
| **240 s per request** | Past ~7 GiB on the wire, `packfile-uris` hands the client a signed URL — bytes bypass the Worker entirely |
| **Undeltified storage** (~6× wire size at rest) | Documented trade for O(1) streaming; bounded by `GE_QUOTA_MAX_BYTES`, reclaimed by GC |
| **Continuous writes → live-pack growth** | Automatic tiered compaction (`GE_COMPACT_*`) bounds live packs without reachability; opt-in `GE_GC_BARRIER` keeps marking correct under write pressure; `GE_MAX_LIVE_PACKS` caps admission |

## Get started

- **[INSTALL.md](INSTALL.md)** — deploy to your own Cloudflare account, or run
  locally with `wrangler dev`. ~10 minutes.
- **[QUICKSTART.md](QUICKSTART.md)** — zero to first clone in under five
  minutes: create-by-push, clone back, mint a scoped token, URL-import a
  public repo.
- **[SETUP.md](SETUP.md)** — the full operator guide: secrets, tuning vars,
  the `/_admin/*` surface, what to put in front for production.
- **[COMPATIBILITY.md](COMPATIBILITY.md)** — every git feature, every hard
  limit, verified performance envelope.
- **[PRODUCTION-UAT.md](PRODUCTION-UAT.md)** — the runnable deploy-and-verify
  checklist.

## The study behind the server

This repo began as a study: 56 ideas for a serverless git host, each proven in
code and then attacked by a second agent told to break it — 33 landed with
caveats, 21 risky, 1 did not land. The verdicts held up: one small program per
project that changes a ref only if it still has the expected value does exactly
what a normal git server does. The working server in `server/` is that study
compiled to a Rust/WASM Worker — built and debugged by agents (Devin on
SWE-2 Max) in the same reproduce → trace → fix → prove loop the study used.

Start reading at [findings/how-to-read.md](findings/how-to-read.md), then
[cross-cutting-defects.md](findings/cross-cutting-defects.md) and
[scoreboard.md](findings/scoreboard.md); [plain/](plain/) has one plain-language
explainer per idea and [site/index.html](site/index.html) is the interactive
map.

## Repo layout

| Path | What it is |
|---|---|
| `server/` | The working server: contract compiled to a Rust/WASM Cloudflare Worker, tested with real git |
| `CONTRACTS.md` | The one set of rules every proof and the server follow: storage layout, the ref transaction, the job timer, the janitor |
| `findings/`, `plain/`, `site/` | The study's summary docs, per-idea explainers, and interactive map |
| `research/` | Canonical Rust proofs + reviews (archived first-pass TS under `archived/`) |
| `tests/conformance/` | The acceptance gate: push → clone → `fsck --strict` against a real git client |
| `tests/bench/` | `oss-bench.sh`: staged-push benchmark for public repos |
| `tools/` | `git-edge-import.sh`: client-side slicer for repos too big for one request |
| `.devin/skills/git-edge/` | Agent skill: URL shape, credentials, tokens, staged pushes, limits |

## License

Apache-2.0 — see [LICENSE](LICENSE) and [NOTICE](NOTICE). Cloudflare's
platform (Workers, Durable Objects, R2) is the entire substrate; their
artifacts/artifact-fs projects showed that large object payloads belong in R2
behind a thin edge layer — git-edge applies that shape to the thing developers
version. gitoxide supplies the Rust building blocks that read and write git's
formats inside a Worker.
