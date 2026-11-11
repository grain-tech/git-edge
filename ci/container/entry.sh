#!/usr/bin/env bash
# Container entrypoint, run as root. Root trusts the egress CA, keeps the run
# nonce, uploads the log every 60 s and posts the final result. The clone and
# the gates run as user ci with a clean environment, so code under test
# cannot read the nonce and cannot post a result.
# Inputs (from the Durable Object): CI_REPO CI_SHA CI_FETCH_REF CI_RUN_ID
# CI_NONCE CI_REPORT_URL.
set -uo pipefail

: "${CI_REPO:?}" "${CI_SHA:?}" "${CI_FETCH_REF:?}" "${CI_RUN_ID:?}" "${CI_NONCE:?}" "${CI_REPORT_URL:?}"
# Root owns the log, in root's home, which ci cannot enter. The gates write
# to it only through the inherited stdout.
LOG=/root/run.log
: >"$LOG"
chmod 644 "$LOG"
# The nonce goes to curl in a root-only header file, never in argv, which
# every user can read in /proc.
HDR="$(mktemp /root/ci-hdr.XXXXXX)"
chmod 600 "$HDR"
printf 'x-ci-nonce: %s\n' "$CI_NONCE" >"$HDR"
unset CI_NONCE

# HTTPS egress goes through the Worker's intercept, which presents a cert
# signed by this ephemeral CA.
CA=/etc/cloudflare/certs/cloudflare-containers-ca.crt
if [ -f "$CA" ]; then
  cp "$CA" /usr/local/share/ca-certificates/cloudflare-containers-ca.crt
  update-ca-certificates >/dev/null 2>&1
fi

post() { # path [exit-code]
  curl -sS -m 60 -X POST --data-binary "@$LOG" \
    -H "@$HDR" -H "x-ci-exit: ${2:-0}" \
    -H "content-type: text/plain" "$CI_REPORT_URL/$1" >/dev/null || true
}

( while sleep 60; do post log; done ) &
UPLOADER=$!

as_ci() {
  runuser -u ci -- env -i HOME=/home/ci PATH=/usr/local/cargo/bin:/usr/local/bin:/usr/bin:/bin \
    CARGO_HOME=/home/ci/.cargo RUSTUP_HOME=/usr/local/rustup CARGO_TARGET_DIR=/opt/warm/target \
    XDG_CACHE_HOME=/opt/tools-cache GE_TLA_CACHE=/opt/tla-cache \
    WRANGLER_SEND_METRICS=false CLOUDFLARE_CF_FETCH_ENABLED=false \
    NODE_EXTRA_CA_CERTS=/etc/ssl/certs/ca-certificates.crt TMPDIR=/tmp \
    CI_REPO="$CI_REPO" CI_SHA="$CI_SHA" CI_FETCH_REF="$CI_FETCH_REF" "$@"
}

rc=0
{
  echo "run $CI_RUN_ID: $CI_REPO at $CI_SHA ($CI_FETCH_REF)"
  as_ci bash -euo pipefail -c '
    git init -q /work/repo && cd /work/repo
    git remote add origin "https://github.com/$CI_REPO.git"
    git fetch -q --depth=1 origin "$CI_SHA" \
      || git fetch -q --depth=50 origin "+$CI_FETCH_REF:refs/ci/head"
    git -c advice.detachedHead=false checkout -q "$CI_SHA"
    test "$(git rev-parse HEAD)" = "$CI_SHA"
    echo "checked out $(git log -1 --format="%h %s")"
  ' && as_ci /opt/ci/gates.sh /work/repo
} >>"$LOG" 2>&1 || rc=$?

echo "runner exit $rc" >>"$LOG"
kill "$UPLOADER" 2>/dev/null || true
post done "$rc"
exit "$rc"
