# Quickstart — zero to first clone in under five minutes

Assumes a running git-edge: your own deploy or local `wrangler dev` — see
[INSTALL.md](INSTALL.md). Everything below works against either; only
`$GE_HOST` and `$GE_TOKEN` change.

```bash
export GE_HOST=https://git-edge.<acct>.workers.dev   # or http://localhost:8787
export GE_TOKEN=<your GE_WRITE_TOKEN>                # local: write-test-token
export GE_REPO=me/hello                              # created by the first push
```

Never put the token in a URL (`https://user:token@host` leaks it into `ps`).
Use a credential helper once:

```bash
gitge() {
  git -c credential.helper= \
      -c 'credential.helper=!f() { echo username=edge; echo "password=$GE_TOKEN"; }; f' \
      "$@"
}
```

## 1. Create-by-push

```bash
mkdir hello && cd hello && git init -q -b main
echo "hello from git-edge" > README.md && git add . && git commit -m init
gitge push "$GE_HOST/$GE_REPO" main
```

The repo did not exist before this push — pushing created it.

## 2. Clone it back + fsck

```bash
gitge clone "$GE_HOST/$GE_REPO" ../hello-back
git -C ../hello-back fsck --strict     # clean
git -C ../hello-back log --oneline     # your commit is there
```

## 3. Mint a scoped per-repo token

Don't hand the deployment write token to CI — mint a deploy key that can only
push `release-*` branches:

```bash
curl -u "edge:$GE_TOKEN" -X POST "$GE_HOST/$GE_REPO/_admin/tokens" \
  -d '{"name":"ci","level":"write","scope":"refs/heads/release-*"}'
# -> {"id":"…","token":"ge_…","level":"write","name":"ci"}   (shown once)
```

Use the `ge_…` value as `$GE_TOKEN` for pushes to this repo; revoke with
`curl -u "edge:$GE_TOKEN" -X DELETE "$GE_HOST/$GE_REPO/_admin/tokens/<id>"`.

## 4. (Optional) URL-import a public repo

For public remotes no staging is needed — the Worker clones it itself:

```bash
curl -u "edge:$GE_TOKEN" -X POST "$GE_HOST/me/flask/_admin/import" \
  -H 'Content-Type: application/json' \
  -d '{"url":"https://github.com/pallets/flask"}'
# -> {"push":"…","queued":true} — poll GET "$GE_HOST/me/flask/_admin/import/<push>"
```

When the job finishes, `gitge clone "$GE_HOST/me/flask"` works like any remote.
Repos bigger than the ~100 MB request cap need the staged-push path — see
`.devin/skills/git-edge/SKILL.md` and [SETUP.md](SETUP.md#first-repository).

## Local path

Same commands with `GE_HOST=http://localhost:8787` and
`GE_TOKEN=write-test-token` (from `server/.dev.vars`). Smoke-test the whole
protocol surface with the conformance suite:

```bash
GE_URL="http://test:write-test-token@localhost:8787" \
  bash tests/conformance/run.sh    # -> == PASS
```

Next: [SETUP.md](SETUP.md) for operations, [COMPATIBILITY.md](COMPATIBILITY.md)
for the limits, `.devin/skills/git-edge/SKILL.md` for the full agent-facing
reference (`_state`, lifecycle endpoints, staged pushes, LFS).
