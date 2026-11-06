# Installing git-edge

Two ways to get a running git-edge: deploy the Worker to your own Cloudflare
account, or run it locally with `wrangler dev`. This is the short path — the
deep operator guide (every var, the `/_admin/*` surface, production hardening)
is [SETUP.md](SETUP.md).

## Option A — your own Cloudflare account

Durable Objects need a **paid** Cloudflare plan. Prerequisites: Node ≥ 22,
a Rust toolchain with the wasm target plus `worker-build`
(`cargo install worker-build`), and `wrangler`.

```bash
rustup target add wasm32-unknown-unknown
cargo install worker-build        # Rust→wasm→worker glue for wrangler.jsonc's
                                  # build.command — a cargo tool, NOT an npm package
cd server && npm ci               # pins wrangler
wrangler login
wrangler r2 bucket create git-edge      # the bucket wrangler.jsonc binds

# secrets — never commit these; `wrangler secret put` stores them server-side
wrangler secret put GE_READ_TOKEN       # any random string — clone/fetch
wrangler secret put GE_WRITE_TOKEN      # any random string — push + admin
wrangler secret put GE_URL_SIGNING_KEY  # openssl rand -hex 32 — enables
                                       # packfile-uris offload + LFS URLs

wrangler deploy                         # builds wasm, runs the DO migration
```

The deploy prints your URL: `https://git-edge.<acct>.workers.dev`. Verify:

```bash
cd ..   # back to the repo root — the deploy block left the shell in server/

curl https://git-edge.<acct>.workers.dev/healthz    # -> ok

# export the write token in this shell first — $GE_WRITE_TOKEN expands below:
#   export GE_WRITE_TOKEN=<the value you `wrangler secret put` above>
GE_URL="https://test:$GE_WRITE_TOKEN@git-edge.<acct>.workers.dev" \
  bash tests/conformance/run.sh                     # -> == PASS
```

The suite is the deterministic acceptance gate — run it after every deploy.
On a production deploy you can drop the `GE_CONFORMANCE_*` tuning flags or set
small test caps first; see [SETUP.md](SETUP.md#deploy).

Optional tuning (`GE_QUOTA_*`, `GE_MAX_LIVE_PACKS`, `GE_GC_*`,
`GE_COMPACT_*`, `GE_RATE_PUSHES_PER_MIN`) goes in `wrangler.jsonc` `[vars]` —
defaults and meanings in [SETUP.md](SETUP.md#vars-optional-tuning).

For a custom domain instead of `*.workers.dev`, add a `routes` entry to
`wrangler.jsonc` — see [SETUP.md](SETUP.md#what-sits-in-front-three-options-in-order-of-effort).

## Option B — local (`wrangler dev`)

```bash
cargo install worker-build        # once per machine — wrangler.jsonc build.command;
                                  # not provided by npm ci
cd server
cp .dev.vars.example .dev.vars    # edit tokens if you like; the example works
npm ci                           # pins wrangler
wrangler dev                      # serves http://localhost:8787
```

Then the conformance suite from the repo root:

```bash
GE_URL="http://test:write-test-token@localhost:8787" \
  bash tests/conformance/run.sh   # -> == PASS
```

Local `.dev.vars` already carries a dev `GE_URL_SIGNING_KEY`, so the
`packfile-uris` and LFS paths work too (needs git ≥ 2.40).

Either way, next stop: [QUICKSTART.md](QUICKSTART.md) — first repo in under
five minutes.
