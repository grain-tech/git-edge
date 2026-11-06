---
name: git-edge
description: Use when cloning from, pushing to, importing into, or managing repositories on a git-edge deployment — the serverless Git smart-HTTP host (Cloudflare Worker + per-repo SQLite Durable Object + R2 packfiles). Covers URL shape, credential setup, per-repo token minting, the staged-push recipe for large imports, the public/delete/pin/export lifecycle, _state introspection, and known limits.
---

# git-edge

Serverless Git smart-HTTP host. A repo is created by pushing to it — there is no
provisioning step, no web UI, no hooks. Protocol v2 fetch (git ≥ 2.26 required),
v0 receive-pack.

## URL shape

```
https://<host>/<owner>/<repo>
```

e.g. `http://localhost:8787/acme/webapp` or
`https://git-edge.<acct>.workers.dev/acme/webapp`. The repo does not exist until
the first push lands.

## Credentials — never put tokens in URLs

`https://user:token@host/...` embeds the token in `git` argv — visible to every
user on the machine via `ps`. Use a credential helper or askpass instead:

```bash
export GE_TOKEN=ge_...           # or the deployment's write token
git -c credential.helper= \
    -c 'credential.helper=!f() { echo username=edge; echo "password=$GE_TOKEN"; }; f' \
    clone https://<host>/<owner>/<repo>
```

or persist per-host once:

```bash
printf 'protocol=https\nhost=<host>\nusername=edge\npassword=%s\n' "$GE_TOKEN" | git credential approve
```

The password is the token; the username is only a label (it is recorded as the
reflog principal). `GIT_ASKPASS` equivalent: point it at a script that prints
`$GE_TOKEN`. For curl/scripts, `Authorization: Bearer <token>` also works.

## Per-repo tokens

The deployment-wide secrets (`GE_READ_TOKEN` / `GE_WRITE_TOKEN` in `.dev.vars`)
are admin credentials. Mint scoped per-repo tokens with the global write token:

```bash
# create — token value shown ONCE: {"id","token":"ge_<64hex>","level","name"}
curl -u "edge:$GE_ADMIN_TOKEN" -X POST \
  https://<host>/<owner>/<repo>/_admin/tokens \
  -d '{"name":"ci","level":"write"}'        # level: read | write

# deploy key — ref-scoped write: this token can only push matching refs
# (scope = comma list of refs/… patterns; trailing * is a prefix glob)
curl -u "edge:$GE_ADMIN_TOKEN" -X POST \
  https://<host>/<owner>/<repo>/_admin/tokens \
  -d '{"name":"release-bot","level":"write","scope":"refs/heads/release-*,refs/tags/v*"}'

curl -u "edge:$GE_ADMIN_TOKEN" https://<host>/<owner>/<repo>/_admin/tokens   # list (ids, never secrets)
curl -u "edge:$GE_ADMIN_TOKEN" -X DELETE \
  https://<host>/<owner>/<repo>/_admin/tokens/<id>                           # revoke
```

Only the global write token can mint; a repo token cannot mint more tokens. A
`read` token clones/fetches but gets 403 on push. Cap: 256 tokens per repo.

A scoped `write` token's pushes land per-ref: commands outside the scope get
`ng "ref outside token scope"` (in-scope siblings still land — same shape as a
pin rejection). The scope is captured when the push begins, so editing the
token mid-push can't widen it, and imports over `_admin/import` obey it too.
Omit `scope` (or send null) for an unrestricted token.

## Repo lifecycle (global write token)

```bash
R=https://<host>/<owner>/<repo>

# anonymous read toggle — clone/fetch/export need no credential; pushes stay
# token-gated either way. NOTE: a presented-but-bad token still gets 401, so a
# stale credential in a helper breaks "anonymous" access — clone truly
# anonymously with `git -c credential.helper= clone $R`
curl -u "edge:$GE_ADMIN_TOKEN" -X POST $R/_admin/public -d '{"enabled":true}'

# export: v3 git bundle of all live refs (read-level auth; anonymous if public)
curl -u "edge:$GE_TOKEN" $R/_admin/export -o repo.bundle
git clone repo.bundle dst          # bundles are clonable directly

# pin/unpin a ref — pushes to a pinned ref get `ng "ref is pinned"`
curl -u "edge:$GE_ADMIN_TOKEN" -X POST $R/_admin/pin \
  -d '{"ref":"refs/heads/main","sha":"<sha>"}'
curl -u "edge:$GE_ADMIN_TOKEN" -X POST $R/_admin/unpin -d '{"ref":"refs/heads/main"}'

# delete: tombstones immediately (every route answers 410), a purge_repo job
# then wipes DO storage + R2 packs; the name is reusable once purge lands
curl -u "edge:$GE_ADMIN_TOKEN" -X POST $R/_admin/delete
```

## Rename / move (ROADMAP #18)

A repo's Durable Object id is derived from `owner/repo` and can't be rebound —
there is no server-side rename verb. The supported recipe is mirror-move:

```bash
git clone --bare https://x:$GE_TOKEN@<host>/<o>/<r> move.git
git -C move.git push --mirror https://x:$GE_TOKEN@<host>/<new-o>/<new-r>
curl -u "edge:$GE_ADMIN_TOKEN" -X POST https://<host>/<o>/<r>/_admin/delete
```

For >80 MiB repos run the staged-push/import path against the new name instead
of `push --mirror`. Refs, reflog-credited principals, pins, and tokens are
per-repo — re-create pins/tokens on the new name.

## Importing a repo larger than ~80 MiB — staged push

The Cloudflare zone caps each request body at ~100 MB, so one giant push fails
with HTTP 413. Push commit ranges oldest→newest instead; each push carries only
the objects introduced in that range:

```bash
git clone --bare <src> src.git
BR=$(git -C src.git symbolic-ref --short HEAD)   # usually main or master
git -C src.git rev-list --first-parent --reverse "$BR" > commits
# pick boundary shas so each slice stays under ~60 MiB of new object bytes and
# well under ~100k objects (the per-request wall clock is 240 s; ~10k objects
# took ~20 s in benchmarks)
git -C src.git push "$REMOTE" "$(sed -n "${HI}p" commits):refs/heads/$BR"
# ...repeat oldest→newest; last push must be the branch tip
```

`--first-parent` matters: it makes every boundary sha a strict descendant of
the previous one, so each push is a clean fast-forward. Plain `--topo-order`
can land a boundary on a merge's side-branch commit — pushing it moves the ref
sideways and is rejected non-fast-forward.

`tools/git-edge-import.sh` automates this slice-and-push loop (plus resume,
`--all-branches`, `--dry-run`, and 429 retry):

```bash
GE_TOKEN=<write-token> tools/git-edge-import.sh <local-repo-or-url> <edge-url>
```

### Server-side import (unsplittable packs — e.g. one commit > ~50k objects)

For a **public** smart-HTTP remote there is no staging at all — the Worker
clones it itself (v2 `ls-refs` + `fetch`, pack verified by trailer before the
import pipeline owns it):

```bash
curl -u "edge:$GE_TOKEN" -X POST "$REMOTE/_admin/import" -H 'Content-Type: application/json' \
  -d '{"url":"https://github.com/owner/repo"}'     # -> {"push":"<id>","queued":true}
curl -u "edge:$GE_TOKEN" "$REMOTE/_admin/import/<push>"   # job_phase: fetch -> resolve -> ...
```

Every advertised ref lands as a create (100k cap, obeying the token's scope)
and the remote's HEAD symref becomes the repo's default branch. Public sources
only — no credentials are sent; http urls must be loopback. A rejected push's
`result` names the reason (`remote answered HTTP 404`, `outside token scope`, …).

When the source is not a public remote, stage the pack itself and let a DO job
ingest it across slices — no per-request cap applies:

```bash
git -C src.git pack-objects --stdout --all > all.pack        # REF deltas fine
split -b 64m all.pack part.                                  # one call per part
# first part mints the push; every later part REUSES it (?push=&part=) so the
# import job's heartbeat covers all staged keys — never stage parts on separate
# pushes, their PUSH_TIMEOUT expiry sweeps sibling parts mid-import
PUSH=$(curl -u "edge:$GE_TOKEN" -X POST "$REMOTE/_admin/import/stage" --data-binary @part.aa | jq -r .push)
curl -u "edge:$GE_TOKEN" -X POST "$REMOTE/_admin/import/stage?push=$PUSH&part=1" --data-binary @part.ab
#   -> {"push":"<id>","key":"r/<repo>/pending/<id>.part-1","bytes":N}
curl -u "edge:$GE_TOKEN" -X POST "$REMOTE/_admin/import" -H 'Content-Type: application/json' -d '{
  "push":"<id>", "parts":[{"key":"...","bytes":N}],
  "commands":[{"old":"0000000000000000000000000000000000000000","new":"<tip>","name":"refs/heads/main"}]}'
curl -u "edge:$GE_TOKEN" "$REMOTE/_admin/import/<push>"   # poll: phase, objects_done
```

The job runs `parse → resolve → check → commit` over alarm slices; the push
commits atomically at the end (per-ref results land in `result`). A re-POST of
the same push returns the running job's pack — safe to retry. Commands behave
exactly like push ref updates: creates need `old` = 40 zeros, the ref must be
a full `refs/…` name, and pinned/non-FF rules still apply.

Then verify: clone back and `git fsck --strict`. Old repos may carry
pre-existing fsck findings (e.g. `zeroPaddedFilemode`) — compare against fsck
of the source, not against empty output.

## Introspection

```bash
curl -u "edge:$GE_TOKEN" https://<host>/<owner>/<repo>/_state   # any write-level credential
```

Write-gated (a repo `write` token suffices; `_admin/*` routes do not — they
want the global write token). Returns `refs`, `objects`, `packs_live`/`packs_ingesting`/
`packs_dead`, `pushes`, `jobs_queued`/`jobs_running`/`jobs_dead`, `marked`,
`tokens`, `pins` (list of `{ref,sha}`), `public`, `deleted`, `rate_rows`,
`refs_memo_hits`/`refs_memo_misses`, `gc_tail` (rows in the GC
yield-persisted buffer — nonzero only mid-consolidate; a count that never
moves is a wedged GC). `jobs_dead > 0` means a maintenance job
wedged. Pushes are CAS; a stale push is rejected — refetch and retry, don't
`--force` blindly.

## Known limits (clean errors, not corruption)

| Limit | Value | Symptom / workaround |
|---|---|---|
| Request body per push | ~100 MB (zone) | HTTP 413 → stage the push (above) |
| Per-request budget | 9,000 subrequests / 240 s | `unpack request budget exhausted` → smaller slices |
| Inflated non-blob object / delta result | 16 MiB | `object too large (16 MiB max)` / `delta base <oid> exceeds 16 MiB` → `git -c core.bigFileThreshold=1 push` sends it as a full blob. **`git push --no-thin` does NOT prevent REF_DELTA on git 2.54** |
| Full blob (pushed whole) | ~2 GiB | streamed verbatim — fine |
| Fetch walk (non-clone fetches with haves) | 200k commits / 1M objects | `ERR fetch too large for this server; clone instead` — incremental fetches only |
| Push links (tree edges) per push | 1,000,000 (8M via `_admin/import`) | `push references too many objects` → smaller slices, or import the pack server-side |
| Push rate | 30/min per credential per repo (`GE_RATE_PUSHES_PER_MIN`) | HTTP 429 + `Retry-After` → wait and retry |
| Live packs | 1,000 live+ingesting per repo (`GE_MAX_LIVE_PACKS`) | HTTP 503 + `Retry-After: 60` → wait and retry; the janitor re-queues GC so the cap self-clears within ~1¼ h (GC grace + janitor period) |
| Quotas | 2M objects / 4 GiB stored per repo, 50 repos/owner (`GE_QUOTA_MAX_*`) | `unpack objects N > GE_QUOTA_MAX_OBJECTS=M` etc. — the message names the cap. LFS objects count toward the byte quota |
| Full clone scale | verified ≥700k objects; no walk bound | Plain clones take the no-walk path (all live objects streamed in pack order) — the 200k-commit walk cap applies only to haves-ful fetches. When `packs_live: 1` (post-GC consolidation) the live pack streams verbatim (~1 subrequest); clients with `fetch.uriprotocols` set get a signed `/_packs/…` URL instead (bandwidth bypasses the Worker entirely, needs `GE_URL_SIGNING_KEY`) |
| LFS objects | basic transfer only, sha256 oids | No `verify` callback, no locking API, no custom transfers — plain upload/download via signed URLs |
| Client floor | git ≥ 2.26 | v0/v1 fetch → `ERR protocol v2 required` |

Each push lands as its own pack; GC consolidates after a ~10 min quiet window
(`GE_GC_QUIET_MS`). Give a fresh staged import a few minutes before cloning.

## Git LFS (basic transfer)

`POST /{owner}/{repo}/info/lfs/objects/batch` implements the LFS batch API.
Upload needs a write credential, download a read one (or `public`). Response
actions are HMAC-signed `/{owner}/{repo}/_lfs/<oid>?e=&r=&s=` URLs (same model
as `/_packs/`): PUT the object bytes to upload, GET to download. Objects land
in R2 under `r/<id>/lfs/` — inside the repo's delete/purge prefix — and count
toward `GE_QUOTA_MAX_BYTES`. Already-present uploads return no action; missing
downloads get a per-object `{"error":{"code":404}}`. Requires
`GE_URL_SIGNING_KEY` — without it the batch route 403s. No `verify` callback,
locking, or custom transfer adapters.

No push-options, no hooks, no rename. `--atomic` is supported (the
whole push rejects on any failing ref). GC is automatic
(mark→consolidate→sweep via DO alarms, 1 h grace before R2 deletion),
with opt-in barrier mode (`GE_GC_BARRIER`) and automatic tiered compaction
(`GE_COMPACT_*`) bounding live-pack growth under continuous writes.

Full reference: `COMPATIBILITY.md`; evidence: `findings/implementation.md`;
end-to-end example: `tests/conformance/run.sh`.
