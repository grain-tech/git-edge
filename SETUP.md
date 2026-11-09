# Setting up git-edge

A deterministic deploy guide: from a fresh checkout to a verified running
service, then what to put in front of it for production. For the short path —
first deploy or local `wrangler dev` in ~10 minutes — see
[INSTALL.md](INSTALL.md); for first-repo commands see
[QUICKSTART.md](QUICKSTART.md).

## What you're deploying

One Cloudflare Worker (Rust/WASM), one Durable Object class (`RepoDo`, SQLite
storage — one DO per repository), one R2 bucket (packfiles, staged import
parts, LFS objects). `server/wrangler.jsonc` declares all of it; `wrangler
deploy` creates the bucket and runs the DO migration.

## Prerequisites

- A Cloudflare account with Workers + R2 + Durable Objects enabled
  (Durable Objects need a paid plan — the DO SQLite backend is not on the
  free tier).
- Rust toolchain with the wasm target:
  `rustup target add wasm32-unknown-unknown`
- Node ≥ 22 for wrangler (wrangler@4 engines); the repo pins it via
  `server/package.json`: `cd server && npm ci` pins wrangler only.
  `worker-build` — the Rust→wasm→worker glue that `wrangler.jsonc`'s
  `build.command` (`worker-build --release`) runs — is a separate cargo
  tool: `cargo install worker-build`. It does not come from npm (the npm
  `worker-build` package is unrelated).
- `wrangler login` (or `CLOUDFLARE_API_TOKEN` for CI).

## Secrets

Two bearer tokens + one signing key. Set them as **secrets**, never in
`wrangler.jsonc` — that file is committed:

```bash
cd server
wrangler secret put GE_READ_TOKEN     # deployment-wide read (clone/fetch)
wrangler secret put GE_WRITE_TOKEN    # deployment-wide write AND admin
wrangler secret put GE_URL_SIGNING_KEY  # ≥32 random bytes — enables packfile-uris + LFS signed URLs
```

Generate with `openssl rand -hex 32` (or 64). `GE_URL_SIGNING_KEY` is optional
but recommended: without it `packfile-uris` offload and LFS signed URLs are
silently disabled, and clones past ~7 GiB wire can't complete inside the
240 s request budget.

| Credential | Can do |
|---|---|
| `GE_WRITE_TOKEN` | **Everything**: push anywhere, mint/revoke per-repo tokens, flip repos public, delete+purge repos, pin refs, run imports. The god credential — treat it like a root key. |
| `GE_READ_TOKEN` | Clone/fetch every repo, including private ones. |
| `GE_URL_SIGNING_KEY` | Not a credential users see — it signs the `/_packs/` and `/_lfs/` URLs the server mints. |

## Vars (optional tuning)

All have defaults; set in `wrangler.jsonc` `[vars]` or `wrangler dev --var`:

| Var | Default | Meaning |
|---|---|---|
| `GE_QUOTA_MAX_OBJECTS` | 2,000,000 | Objects per repo |
| `GE_QUOTA_MAX_BYTES` | 4 GiB | Stored bytes per repo (the normalized pack — undeltified ~6× the wire size) |
| `GE_QUOTA_MAX_REPOS_PER_OWNER` | 50 | Live repo claims per `owner/` prefix |
| `GE_RATE_PUSHES_PER_MIN` | 30 | Pushes per credential per repo per minute |
| `GE_GC_QUIET_MS` | 600,000 (10 min) | Quiet period before GC consolidates after a push |
| `GE_GC_GRACE_MS` | 3,600,000 (1 h) | Grace before dead packs are swept |
| `GE_GC_BARRIER` | 0 (off) | Barrier-mode mark: pending page during the walk, retirement fence at sweep, no `refs_version` fence mid-walk — keeps marking correct while pushes keep landing. Captured once per GC chain at mark-init (persisted as `gc.barrier=1`); arms read the meta, not the env, so it applies per-chain, not retroactively |
| `GE_COMPACT_FANIN` | 16 (min 2) | G4a compaction: minimum same-tier live packs that trigger a lossless compact |
| `GE_COMPACT_MIN_AGE_MS` | 60,000 | Minimum pack age before a pack can join a compact cohort |
| `GE_COMPACT_MAX_ROWS` | 100,000 | Object-row budget per compact cohort (packs over `max_rows`/2 objects are ineligible) |
| `GE_MAX_LIVE_PACKS` | 1,000 | Live+ingesting packs per repo; min 2 when enabled, ≤0 disables — over-cap pushes/imports get 503 + `Retry-After: 60`. Compaction (`GE_COMPACT_*`) is what normally keeps live packs under this cap |
| `GE_EDGE_INGEST_SLOTS` / `GE_EDGE_INGEST_WAIT_MS` | 1 / 240,000 | Concurrent pass-B push ingests per isolate, and how long a push waits for a slot or a drained heap before answering busy in-band; the effective wait is also capped by the request budget minus `GE_EDGE_INGEST_PASS_B_RESERVE_MS`, and a push that cannot cover the reserve fails busy up front; slots ≤0 disables the gate. The 40 MiB heap low-water means one large in-flight pass B (~60 MiB live) blocks a second ingest, so effective concurrency for large pushes is 1 even with slots > 1 |
| `GE_EDGE_INGEST_PASS_B_RESERVE_MS` | 150,000 | Request-budget milliseconds pass B must still have for a queued push to be admitted — below it the push fails busy instead of starting pass B already doomed to a budget failure; ≤0 drops the reserve |

`GE_METRICS` (Analytics Engine binding in `wrangler.jsonc`) is optional —
one datapoint per request; delete the binding line if you don't want it.

## Deploy

```bash
cargo install worker-build               # Rust→wasm glue for wrangler.jsonc
                                         # build.command — once per machine
cd server
npm ci                                   # pins wrangler
wrangler r2 bucket create git-edge       # the bucket wrangler.jsonc binds
wrangler deploy                          # builds wasm, runs the DO migration, uploads
```

The output prints the workers.dev URL. Then verify with the conformance
suite — it is the deterministic acceptance gate, not a vibes check. Run it
from the repo root (the deploy block left the shell in `server/`) with the
write token exported in this shell:

```bash
cd ..
# export GE_WRITE_TOKEN=<the value you `wrangler secret put` above> first
GE_URL="https://test:$GE_WRITE_TOKEN@git-edge.<acct>.workers.dev" \
GE_CONFORMANCE_GC=1 GE_CONFORMANCE_PURGE=1 GE_CONFORMANCE_IMPORT=1 \
GE_CONFORMANCE_LIMITS=1 GE_CONFORMANCE_URIS=1 \
bash tests/conformance/run.sh     # ... == PASS
```

(LIMITS/GC want the tuning vars small — for a production verify you can skip
those two flags, or run the full set against a staging deploy with caps.)

For a custom domain, add a `routes` entry or `workers.dev` stays the endpoint.
Custom domain recommended — see "In front" below.

## First repository

```bash
REMOTE=https://git-edge.example.com
TOKEN=$GE_WRITE_TOKEN

# push works out of the box (repos are created on first push under the
# owner's repo quota):
git clone "$REMOTE/acme/app"   # empty — then push

# mint a scoped deploy key instead of handing out the god token:
curl -u "ci:$TOKEN" -X POST "$REMOTE/acme/app/_admin/tokens" \
  -d '{"name":"ci-bot","level":"write","scope":"refs/heads/release-*"}'
# -> {"token":"ge_…"} — push-only, and only to release-* refs

# public read:
curl -u "ops:$TOKEN" -X POST "$REMOTE/acme/app/_admin/public" -d '{"enabled":true}'

# seed from an existing public repo — no client staging:
curl -u "ops:$TOKEN" -X POST "$REMOTE/acme/app/_admin/import" \
  -H 'Content-Type: application/json' -d '{"url":"https://github.com/org/repo"}'

# a sha256 repo: pin the format BEFORE pushing (a v0 client that sees only
# sha1 advertised aborts client-side), or just push — an unpinned repo
# advertises both formats and the first write pins the client's pick
curl -u "ops:$TOKEN" -X POST "$REMOTE/acme/app/_admin/format" \
  -d '{"object_format":"sha256"}'
```

Scopes fail closed: a token minted without `scope` is stored with the explicit
pattern `refs/*` (all refs), and a push whose token row is missing or whose
stored scope is empty is refused. Only `GE_WRITE_TOKEN` pushes without a token
row. Rows minted before this rule are rewritten to `refs/*` on DO boot.

Tokens can expire: add `"ttl_s": <seconds>` (1 to 31,622,400, i.e. 366 days) when
minting. The response and `GET /_admin/tokens` carry `expires_at` (epoch ms).
An expired token gets a 401 on every route, can't open a push, and a push it
opened is rejected at commit (`token no longer valid`) — the same check that
catches a token revoked mid-push. Omit `ttl_s` for a non-expiring token.

Protected refs are fast-forward-only and can't be deleted, for every credential
including `GE_WRITE_TOKEN`:

```bash
curl -u "ops:$TOKEN" -X POST "$REMOTE/acme/app/_admin/protect" -d '{"ref":"refs/heads/main"}'
curl -u "ops:$TOKEN" -X POST "$REMOTE/acme/app/_admin/unprotect" -d '{"ref":"refs/heads/main"}'
```

`ref` takes the token-scope grammar (`refs/heads/release-*`); `_state` lists the
patterns under `protected`. A push or import that would rewrite or delete a
protected ref gets `ng <ref> protected ref: non-fast-forward` or `… deletion
refused`. Ancestry is proven from a commit graph recorded at ingest; history
pushed before this release has no graph rows, so an update whose walk meets
one is refused with `cannot prove fast-forward` (fail closed). Unprotect,
push, re-protect to move such a ref once.

### Push events (signed webhook)

Set `GE_PUSH_WEBHOOK_URL` (var) and `GE_PUSH_WEBHOOK_SECRET` (secret, at least 32
bytes) and every committed push POSTs one JSON event:

```json
{"event":"push","owner":"acme","repo":"app","repo_id":"…","push_id":"…",
 "principal":"ci-bot","at":1790000000000,
 "refs":[{"ref":"refs/heads/main","old":"<sha>","new":"<sha>"}]}
```

Only refs that actually moved are listed; a rejected push sends nothing.
Headers: `x-ge-event: push`, `x-ge-delivery: <repo_id>:<seq>` (stable across
retries — dedup on it), `x-ge-timestamp: <epoch ms>`, and `x-ge-signature:
v1=<hex HMAC-SHA256(secret, "v1\n<timestamp>\n<delivery>\n" + body)>`.
Verify the signature, bound the timestamp, dedup on the delivery id —
`tests/conformance/push_event_receiver.py` is a reference receiver.

The event row is written in the same storage transaction as the ref update, so
an event exists exactly when refs moved. Delivery is at-least-once and in order
per repo: a non-2xx answer stops the drain and retries with backoff. Either
setting missing turns the feature off (nothing recorded, nothing sent unsigned).
The per-repo outbox holds 10,000 events; past that pushes still land and
`_state.push_events_dropped` counts the gap (`push_events_pending` shows the
backlog).

Repo layout is `/:owner/:repo`; `owner` is the quota/claims namespace, not an
ACL — real access control is per-repo tokens + public flags.

## The admin surface — who can do what

Everything under `/_admin/*` requires `GE_WRITE_TOKEN`. There is no separate
admin credential: the deployment write token *is* the admin credential. That
means the admin surface is exactly as wide as the set of people holding that
token — which is the thing to fix for production, not with more code but by
putting a layer in front.

Data-plane endpoints (`info/refs`, `git-upload-pack`, `git-receive-pack`,
`/_packs/<sig>`, LFS) authenticate per request: deployment read/write tokens,
per-repo `ge_*` tokens (level + optional ref scope, enforced on every ref
update a push or import attempts), or anonymous for public repos. `/_state`
introspection is read-level.

## What sits in front: three options, in order of effort

**1. Cloudflare Access (zero code, do this).** Put an Access application over
`*git-edge.example.com/*/_admin/*` — or over the whole hostname for an
internal deployment. Requests without a valid Access identity (Google/GitHub/
your SSO) are 302'd to login before they reach the Worker; the Worker still
checks `GE_WRITE_TOKEN`, so admin becomes SSO-identity AND god-token. Free
under 50 users. WAF custom rules are the same idea for IPs/ASNs/mTLS instead
of identity.

**2. A gateway Worker (the real control plane).** For anything multi-tenant:
a small fronting Worker owns the mapping `user → repos → allowed ops`,
holds `GE_WRITE_TOKEN` server-side, and mints per-repo scoped tokens on
behalf of authenticated users. End users never see a deployment credential;
git-edge stays the dumb-fast protocol engine. This is also where you'd put
repo-name registration, billing, and per-user rate identity. The DO/R2 layer
needs no changes — the gateway is just an auth translation proxy in front of
the same routes.

**3. Token discipline alone (minimum viable).** `GE_WRITE_TOKEN` lives only
in your deploy environment + the handful of operators. Everything else —
CI, devs, agents — gets per-repo `ge_*` tokens, optionally ref-scoped. Rotate
by flipping the secrets (`wrangler secret put` again); repo tokens revoke via
`DELETE /:owner/:repo/_admin/tokens/<id>`.

## Production checklist

- [ ] Custom domain, not the workers.dev hostname (you control the CNAME; the
      workers.dev name is harder to retire).
- [ ] Access or WAF rule over `/_admin/*` — the god token shouldn't be the
      only gate on delete/purge/public.
- [ ] `GE_URL_SIGNING_KEY` ≥ 32 random bytes — enables URI offload (clones
      past ~7 GiB wire need it) and LFS signed URLs.
- [ ] Quotas set deliberately: the defaults assume repos up to TypeScript
      scale (18 GiB normalized). If you don't want that, lower
      `GE_QUOTA_MAX_BYTES`.
- [ ] R2 lifecycle rule on the bucket to abort incomplete multipart uploads —
      crashed staged writes can orphan MPU parts (see HANDOFF.md for the
      reclaim recipe; a lifecycle rule makes it automatic).
- [ ] Rotate the deployment secrets on a schedule; per-repo tokens have their
      own revoke path.
- [ ] `GE_METRICS` bound if you want request telemetry in Analytics Engine.
- [ ] Run the conformance suite against the deployed URL after every deploy —
      it's the regression gate.

## Failure modes worth knowing

- **A stalled host outlives a push lease.** In-flight imports heartbeat per
  slice; if the DO can't write for ~an hour the janitor legitimately expires
  the push and sweeps the run. In practice: only a DO storage outage does
  this — the import just needs re-POSTing.
- **Inline clone wall.** A single HTTP request has a 240 s budget; verbatim
  sideband streams ~32 MiB/s, so packs past ~7 GiB wire need
  `fetch.uriprotocols` to negotiate the signed `/_packs/` offload (needs the
  signing key, and the client's allowed scheme must match — `http` for
  localhost, `https` in production).
- **URL imports fetch public remotes only** — no credentials are sent, and
  `http` URLs must be loopback. A private GitHub repo needs the staged-pack
  path instead.

## Teardown

`wrangler delete` removes the Worker; the DOs and R2 objects are billed until
the bucket is emptied/deleted (`wrangler r2 bucket delete git-edge`) — repo
deletes tombstone+sweep per-repo, but retired deployments need the bucket
gone too.
