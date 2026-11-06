# git-edge — production deployment UAT checklist

Runnable acceptance checklist for a git-edge deployment. Every item is a
command or observation with an explicit pass condition. Run the full battery
after any deploy that touches ingest, fetch, GC, auth, or the job dispatcher;
run §3 smoke on config-only changes.

Terminology: `$BASE` = worker URL (production: `https://git-edge.<acct>.workers.dev`),
`$RW` = `GE_WRITE_TOKEN`, `RO` = `GE_READ_TOKEN`. Local equivalent: `wrangler
dev` on `http://localhost:8787` with tokens from `server/.dev.vars`.

## 1. Pre-deploy gates

- [ ] `cargo check` clean (server/)
- [ ] `worker-build --release` clean
- [ ] `tests/conformance/run.sh` PASS against local `wrangler dev`
- [ ] `git diff` of the release contains no secrets; `server/.dev.vars` is gitignored
- [ ] `wrangler.jsonc` bindings unchanged unless intended: `REPO` (DO `RepoDo`),
      `BUCKET` (R2), `GE_METRICS` (Analytics Engine `git_edge`), `cpu_ms: 300000`

## 2. Deploy

- [ ] R2 bucket exists: `wrangler r2 bucket list | grep git-edge`
- [ ] Secrets set (never committed): `wrangler secret put GE_READ_TOKEN`,
      `wrangler secret put GE_WRITE_TOKEN`
- [ ] `wrangler deploy` succeeds; startup log lists all three bindings
- [ ] Record the deployed version id + git sha in the release notes

## 3. Smoke (every deploy)

- [ ] `GET $BASE/healthz` → 200 `ok`
- [ ] `git ls-remote https://x:$RO@host/owner/repo` → 401 without token, refs with token
- [ ] Small push + `git clone` → `git fsck --strict` clean
- [ ] `GET /o/r/_state` (write token) → JSON, `packs_live` ≥ 1
- [ ] One Analytics Engine datapoint visible:
      `dataset git_edge` — check via `wrangler analytics` / dashboard; blobs[0]=op,
      doubles = [status, ms, subrequests]

## 4. Protocol conformance (live)

Run `tests/conformance/run.sh` with `$BASE` pointed at production — must be
fully green: empty-repo push, incremental thin-delta push, branch create/delete,
tag push, delete-only push, clone + strict fsck, incremental fetch, CAS
rejection, malformed-pack `ERR`, GC mark/consolidate/sweep cycle.

- [ ] Full suite PASS on production URL (incl. `GE_CONFORMANCE_IMPORT=1`:
      shared-push staged parts → `import_pack` job → atomic commit → clone+fsck)
- [ ] Protocol v2 required: v0 `POST git-upload-pack` → HTTP 400 + `ERR` pkt-line
- [ ] Shallow battery: `--depth=1`, `--depth=5` deepen, `--deepen`,
      `--shallow-since`, `--shallow-exclude`, `--unshallow` — each fsck-clean
- [ ] Partial clone: `--filter=blob:none` works; lazy blob backfill on checkout works

## 5. Large-object battery

- [ ] 16 MiB blob (materialized path) + 16 MiB+1 blob (streamed path): push,
      clone byte-identical, fsck clean
- [ ] 30 MiB blob push → clone byte-identical (production-verified 2025; re-run
      on ingest/GC changes)
- [ ] Delta result >16 MiB → clean `unpack object too large` + `ng` report-status
      (HTTP 200, no transport reset)
- [ ] Repo with big blob → GC consolidate → post-GC clone byte-identical
- [ ] ~95 MiB total push succeeds; >~100 MiB push fails with 413 at the zone
      (documented platform cap — workaround: stage pushes by ref range)
- [ ] Fetch `x-ge-subrequests` response header present and < 9000/… budget

## 6. Auth battery

- [ ] Global read token: clone/fetch ok, push → 403
- [ ] Global write token: push ok, `_admin/*` ok
- [ ] `POST /o/r/_admin/tokens {"name":"ci","level":"read"}` (write token) →
      returns `token` once; `GET _admin/tokens` shows id/name/level, NO secret
- [ ] Repo read token: `ls-remote` ok, push → 403; on a DIFFERENT repo → 401
- [ ] Repo write token: push ok; `DELETE _admin/tokens/<id>` → immediate 401 on reuse
- [ ] Repo token on `_admin/*` → 401 (cannot mint credentials)
- [ ] No/invalid credential → 401 with `WWW-Authenticate` challenge

## 7. Concurrency & failure injection

- [ ] Two concurrent pushes to same ref → exactly one wins, loser gets `ng`/`rejected`
- [ ] Clone DURING GC sweep → completes, fsck clean
- [ ] Cyclic REF_DELTA pack → rejected <1s, `unpack missing base`, `_state` consistent
- [ ] Push killed mid-pack (client abort) → no live refs change; pending pack
      reaped by janitor (check `_state` pending count returns to 0)

## 8. Jobs & storage health

- [ ] After any push: `_state` `jobs_queued` drains within GC_QUIET + alarm cadence
      (janitor re-arms ~15 min — a standing queue entry is normal)
- [ ] `packs_dead` → 0 after sweep; no `jobs_dead` accumulation
- [ ] R2 bucket: no orphaned `pending/` objects older than janitor TTL; MPU
      aborts verified (aborted uploads don't count against storage)

## 9. Observability & alerting

- [ ] `git_edge` dataset receiving datapoints; query hourly error rate:
      `SELECT blobs[0] AS op, doubles[0] AS status, count() FROM git_edge ...`
- [ ] External monitor: cron `git ls-remote` (read token) + `_state` scrape (write
      token); alert on non-200 or subrequests near budget
- [ ] Error budget trigger: sustained 5xx or `doubles[0] >= 500` rate >1% → investigate

## 10. Known-limit sign-off (accept or block)

| Limit | Value | Workaround | Accepted? |
|---|---|---|---|
| Request body | ~100 MB (platform) | stage pushes by sha range; or `/_admin/import` (no per-part cap) | ☐ |
| Delta results | 16 MiB | push objects un-deltified (`core.bigFileThreshold`) | ☐ |
| Clone walk bound | none for plain clone (#22 streams all live objects); 200k commits for negotiated fetches | — | ☐ |
| Fetch objects | 1M reachable | partial clone filters | ☐ |
| Client floor | git ≥ 2.26 | — | ☐ |
| Object format | sha1 + sha256 (unpinned repos advertise both; first write pins) | — | ☐ |
| LFS | basic transfer implemented (A33): batch + signed GET/PUT | no verify/locking/custom transfers | ☐ |
| Auth model | global + per-repo tokens, no per-user identity | name tokens per principal; per-ref `scope` on mint (ROADMAP #19) | ☐ |
| Paid plan only | 10k subrequests vs 9k budget | — | ☐ |

## 11. Custom domain + Cloudflare Access (optional, ROADMAP #20)

Zero-code auth upgrade when the worker sits in a Cloudflare zone.

- [ ] Custom domain: `wrangler.jsonc` →
      `"routes": [{ "pattern": "git.example.com", "custom_domain": true }]`, or
      dashboard → Workers → git-edge → Domains. `*.workers.dev` can be disabled
      (Workers → Settings) once the custom domain answers.
- [ ] Access in front: dashboard → Access → Applications → self-hosted,
      `git.example.com` — but note the split:
  - `_admin/*` + `_state` browser/API use → Access IdP works end to end.
  - git clients (clone/push/fetch) cannot complete an Access browser flow —
      they need `cf-access-client-id` / `cf-access-client-secret` service-token
      headers via `git config http.https://git.example.com/.extraHeader`, or
      leave the git endpoints on token auth and gate only `_admin/*` with
      Access (recommended: one Access policy whose `include` list matches
      `/_admin/` and `/_state` paths; service tokens for CI).
- [ ] Access does not replace `GE_*_TOKEN`: keep bearer auth on — Access is an
      outer gate, the token model still authorizes per-repo inside the worker.
- [ ] Verify: `curl https://git.example.com/healthz` → 200; anonymous
      `/_admin/tokens` → Access login page or 403 with service-token only.

## 12. Rollback & DR

- [ ] Rollback: `wrangler rollback` or redeploy previous version id; DO schema
      migrations are additive-only — verify old code tolerates `tokens` table
- [ ] DR: repo data = R2 packs + DO sqlite; backup path = `git clone --mirror`
      per repo (documented); no other restore mechanism exists
- [ ] Incident: janitor/GC are self-healing (lease fencing + dead-letter); a
      wedged DO recovers via alarm — escalate only if `jobs_dead` grows

## Sign-off

- [ ] All §3–§8 items PASS on the production URL
- [ ] §10 limits reviewed and accepted for the intended workload
- [ ] Metrics flowing; external monitor armed
- [ ] Record: date, deployed version, git sha, operator, notes
