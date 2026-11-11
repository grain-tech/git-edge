# ci: the `git-edge-ci` required check

This directory holds the CI runner for this repository. A Worker receives
GitHub webhooks, and a Container runs the three merge gates on the pushed
commit. The result appears on the pull request as one check run named
`git-edge-ci`. The runner deploys to a development Cloudflare account only:
`wrangler.jsonc` pins its `account_id`, so a deploy with a token or login for
any other account fails instead of landing there. It uses no production credentials and no GitHub Actions.

## The gates

`ci/gates.sh` runs every gate, even after an earlier gate fails:

| Gate | Command |
|---|---|
| `cargo` | `cd server && cargo test --locked` |
| `harness-barrier-0` | `tests/harness/run.sh` against a local `wrangler dev` with `GE_GC_BARRIER=0` |
| `harness-barrier-1` | the same harness, fresh state, `GE_GC_BARRIER=1` |
| `tla` | `bash verification/tla/verify.sh` |

The check passes only when the script exits 0 and prints a `pass` line for all
four gates. A missing gate line fails the check.

A harness gate needs more than `run.sh` exiting 0. Its unittest summary must
say `Ran N tests` and end in a bare `OK`, so a skip or an expected failure
fails the gate. N must equal the number of test cases unittest loads from
`tests/harness/test_*.py`, leaving out `test_hooks_off.py`, which needs a
hooks-off server. A module that `run.sh` no longer runs therefore fails the
gate. N must also be at least `HARNESS_MIN_TESTS` in `gates.sh`, 97 today, so
deleting tests fails too. Raise the minimum when you add harness tests.
`ci/gates.sh --harness-expected` prints the expected N.

Run the gates locally from the repository root:

    ci/gates.sh                                 # all gates, port 8795
    GE_CI_PORT=8899 ci/gates.sh                 # another port
    GE_CI_GATES="cargo tla" ci/gates.sh         # a subset, for local use only

Local needs: Rust with `wasm32-unknown-unknown`, `worker-build`, Node 22 or
later, Python 3, git, and curl. `verify.sh` downloads a JRE when no Java 11+
is on PATH. Each harness run keeps its dev-server state in a temporary
directory, so a local run does not touch `server/.wrangler/state`. The dev
server runs without `CLOUDFLARE_API_TOKEN` or `CLOUDFLARE_ACCOUNT_ID`.

## How a run works

1. GitHub sends `pull_request` (opened, synchronize, reopened) or `push` to
   `POST /webhook`. The Worker checks `X-Hub-Signature-256` against
   `WEBHOOK_SECRET`. A bad or missing signature gets 401.
2. The Worker ignores every event except those, and every repository except
   `CI_REPO`. A push runs only on the default branch. A pull request from a
   fork gets a failed check with a reason, and no run.
3. One Durable Object per head commit creates the check run `in_progress`.
   The Details link is a signed log URL that expires after `LOG_TTL_DAYS`.
4. The Durable Object starts the Container (`INSTANCE`, default
   `standard-3`). It sets an alarm for `RUN_TIMEOUT_MIN` (default 75).
5. The Container clones the commit, runs `ci/gates.sh`, and uploads its log
   every 60 seconds and at the end. The log goes to the R2 bucket
   `git-edge-ci-logs`.
6. The Durable Object completes the check as `success` or `failure`, with a
   gate table and the log tail. A timeout completes it as `timed_out`. A
   Container that stops without a result completes it as `failure`, 90
   seconds after the exit. The wait lets a `/done` report that is still
   arriving complete the run, since the exit can reach the Durable Object
   before the report does.

The Container runs the copy of `gates.sh` that is in the image. A pull
request that edits `ci/gates.sh` changes the check only after a redeploy.

## Network and credential controls

Enforced in code:

- The Container starts with `enableInternet: false`. DNS resolves only the
  intercepted hosts below. Every other connection fails, on every port.
- `src/egress.ts` decides each intercepted HTTPS request:
  - `github.com`: only `info/refs?service=git-upload-pack` and
    `git-upload-pack` for `CI_REPO`. Push (`git-receive-pack`), other
    repositories, and web pages get 403.
  - `index.crates.io`, `static.crates.io`, `registry.npmjs.org`: GET and
    HEAD only. These hosts are reached only when a pull request changes
    `Cargo.lock` or `package-lock.json`. The image already holds the caches
    for the tree it was built from.
- Redirects are not followed. They go back to the Container, which cannot
  resolve hosts outside the list.
- The Container never holds a GitHub credential. The egress Worker adds a
  short-lived installation token to allowed git fetches only. The token is
  scoped to `CI_REPO` with `checks:write` and `contents:read`.
- `http://ci.internal` is the only way to report. Each report needs the run
  nonce, which the Durable Object compares in constant time. Root holds the
  nonce and passes it to curl from a root-only file, never on a command
  line. The clone and the gates run as user `ci` with a
  clean environment, so the code under test cannot read the nonce or post a
  result. Root owns the log file and keeps it in `/root`, which `ci` cannot
  enter. The Worker decodes each report as it streams in and keeps only its
  last 10 Mi characters. Memory therefore depends on that tail, not on the
  size of the log, but the peak holds several copies of it: the kept
  pieces, their join, the cut tail and the copy sent to the Durable Object.
  For non-Latin-1 text, stored two bytes per character, that is roughly
  60-80 MB of the isolate's 128 MB. A report over 256 MiB is refused.
- Root owns the toolchain, worker-build's tool cache, the JRE and
  `tla2tools.jar`, and the gate script. Code under test cannot swap one of
  those binaries to fake a pass. The image build fails if `ci` can write to
  any of them.
- The Durable Object handles one state change at a time. Two deliveries for
  one commit start one run. A result and the timeout complete the check
  once. A failed check update is stored for its own run and retried from
  the alarm, up to 10 times. A later run for the same commit does not
  replace it.
- The dev server in the harness gets no Cloudflare account variables.

Not enforced:

- Code under test can write the crate registry, the build target directory
  and the npm cache. The runner trusts pull requests from this repository's
  own branches. Fork pull requests do not run.
- The GitHub "Re-run" button does nothing. To run again, push a new commit
  or close and reopen the pull request.
- The runner does not cancel an older run when a pull request gets a new
  head. Each head gets its own run and check.
- There is no cap on concurrent Containers in code. The Durable Object
  scheduling policy does not support `max_instances`. Account limits apply.
- A pull request that bumps the wasm-bindgen version needs a new image.
  worker-build downloads that tool from GitHub releases, which is not on the
  egress list.
- A pull request that changes the JRE or `tla2tools.jar` hash in `verify.sh`
  needs a new image, for the same reason.

## Setup on the development account

These steps need a person with the named role. Nothing here runs in CI.

1. GitHub org owner: create a GitHub App that is not public, with
   `checks:write` and `contents:read`, and events `pull_request` and `push`.
   Set its webhook URL to `https://<worker>.<subdomain>.workers.dev/webhook`
   and give it a webhook secret. Install it on this repository only.
2. Development account, in a deploy-token window:

        cd ci && npm ci
        npx wrangler r2 bucket create git-edge-ci-logs
        npx wrangler deploy
        npx wrangler secret put WEBHOOK_SECRET
        npx wrangler secret put APP_ID
        npx wrangler secret put APP_PRIVATE_KEY     # the PEM GitHub issued
        npx wrangler secret put LOG_URL_KEY         # openssl rand -hex 32
        npx wrangler secret put CI_REPO             # owner/name

   Optionally expire old logs: `npx wrangler r2 bucket lifecycle add
   git-edge-ci-logs expire-logs runs/ --expire-days 30`.
   The Worker answers 503 until all five secrets exist. `wrangler deploy`
   builds the image, so it needs Docker.
3. Repository admin: after the first `git-edge-ci` result, require
   `git-edge-ci` on the default branch.

## Tests

    cd ci && npm ci
    npm test            # webhook, egress, check payloads, log links, App auth, wiring, entrypoint
    npm run typecheck   # generates worker-configuration.d.ts, then tsc

The tests run on Node 23.6 or later, which runs TypeScript directly.
`test/cf-stub.mjs` stands in for `cloudflare:workers`.

## Cost

At 120 runs a month of about 15 minutes on `standard-3`, the Containers
charge above the Workers Paid allotment is about $4. The total with the $5
plan is about $9 a month. At 20 heads a day it is about $40 a month.
