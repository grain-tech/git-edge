#!/usr/bin/env bash
# Runs the three required merge gates on one checkout of this repository:
#   cargo    : cd server && cargo test --locked
#   harness  : tests/harness/run.sh against a local `wrangler dev`, run twice,
#              with GE_GC_BARRIER=0 and then GE_GC_BARRIER=1, each on fresh state
#   tla      : verification/tla/verify.sh
# Every gate runs even when an earlier gate fails. Each gate prints one line
#   ::gate:: <name> <pass|fail> <seconds>s
# and the script exits 0 only when every gate passes. The CI container runs
# this script, and a developer runs the same script locally:
#   ci/gates.sh [repo-dir]
# GE_CI_PORT (default 8795) picks the local dev server port. GE_CI_GATES
# (default "cargo harness-barrier-0 harness-barrier-1 tla") picks gates for a
# local run; the CI container always runs all of them.
set -uo pipefail

REPO="$(cd "${1:-$(dirname "${BASH_SOURCE[0]}")/..}" && pwd)"
PORT="${GE_CI_PORT:-8795}"
GATES="${GE_CI_GATES:-cargo harness-barrier-0 harness-barrier-1 tla}"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/ge-ci.XXXXXX")"
SERVER_PID=""

cleanup() {
  stop_server
  rm -rf "$WORK"
}
trap cleanup EXIT

log() { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*"; }

stop_server() {
  [ -n "$SERVER_PID" ] || return 0
  # setsid made the server its own process group: stop wrangler and workerd.
  kill -TERM -- "-$SERVER_PID" 2>/dev/null || true
  for _ in $(seq 1 20); do
    kill -0 "$SERVER_PID" 2>/dev/null || break
    sleep 0.5
  done
  kill -KILL -- "-$SERVER_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
  SERVER_PID=""
}

npm_ready=0
prepare_server() {
  [ "$npm_ready" = 1 ] && return 0
  (cd "$REPO/server" && npm ci --prefer-offline --no-audit --no-fund) || return 1
  # The harness tokens come from the checked-in example. Keep a developer's
  # own .dev.vars when one exists.
  if [ ! -f "$REPO/server/.dev.vars" ]; then
    cp "$REPO/server/.dev.vars.example" "$REPO/server/.dev.vars" || return 1
  fi
  npm_ready=1
}

# harness_run <barrier>: fresh state dir, server up, harness, server down.
harness_run() {
  local barrier="$1" state="$WORK/state-$1" slog="$WORK/devserver-$1.log" i
  prepare_server || return 1
  # A server already on the port would answer the readiness probe, and the
  # harness would test it instead of this tree.
  if curl -s -o /dev/null "http://127.0.0.1:$PORT/"; then
    log "port $PORT is already in use; set GE_CI_PORT"; return 1
  fi
  mkdir -p "$state"
  # The dev server never sees an account token: the harness runs fully local.
  ( cd "$REPO/server" && exec setsid bash -c 'exec "$@"' _ \
      env -u CLOUDFLARE_API_TOKEN -u CLOUDFLARE_ACCOUNT_ID -u CF_API_TOKEN \
      WRANGLER_SEND_METRICS=false CLOUDFLARE_CF_FETCH_ENABLED=false \
      npx wrangler dev --port "$PORT" --ip 127.0.0.1 --persist-to "$state" \
        --var GE_TEST_HOOKS:1 --var GE_TEST_MANUAL_JOBS:1 \
        --var GE_GC_QUIET_MS:0 --var GE_GC_GRACE_MS:0 \
        --var GE_RATE_PUSHES_PER_MIN:0 --var "GE_GC_BARRIER:$barrier" \
      >"$slog" 2>&1 ) &
  SERVER_PID=$!
  for i in $(seq 1 300); do
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
      log "dev server exited before it was ready"; tail -n 60 "$slog"; SERVER_PID=""; return 1
    fi
    curl -s -o /dev/null "http://127.0.0.1:$PORT/" && break
    sleep 2
  done
  if ! curl -s -o /dev/null "http://127.0.0.1:$PORT/"; then
    log "dev server not ready after 600s"; tail -n 60 "$slog"; stop_server; return 1
  fi
  log "dev server up after ~$((i * 2))s (GE_GC_BARRIER=$barrier)"
  local rc=0
  (cd "$REPO" && GE_URL="http://test:write-test-token@127.0.0.1:$PORT" \
     GE_SERVER_LOG="$slog" tests/harness/run.sh) || rc=$?
  stop_server
  if [ "$rc" != 0 ]; then
    log "dev server log tail (GE_GC_BARRIER=$barrier):"; tail -n 40 "$slog"
  fi
  return "$rc"
}

run_gate() { # name -> runs, records, never exits
  local name="$1" start rc=0
  start=$(date +%s)
  log "=== gate $name: start"
  case "$name" in
    cargo) (cd "$REPO/server" && cargo test --locked) || rc=$? ;;
    harness-barrier-0) harness_run 0 || rc=$? ;;
    harness-barrier-1) harness_run 1 || rc=$? ;;
    tla) bash "$REPO/verification/tla/verify.sh" || rc=$? ;;
    *) log "unknown gate $name"; rc=2 ;;
  esac
  local secs=$(( $(date +%s) - start )) verdict=pass
  [ "$rc" = 0 ] || verdict=fail
  echo "::gate:: $name $verdict ${secs}s"
  [ "$rc" = 0 ]
}

failed=0
for g in $GATES; do
  run_gate "$g" || failed=1
done
if [ "$failed" = 0 ]; then
  echo "::gates:: pass"
else
  echo "::gates:: fail"
fi
exit "$failed"
