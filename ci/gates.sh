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
#
# A harness gate passes only when run.sh exits 0 AND its unittest summary
# shows every expected test ran and passed: "Ran N tests" with N equal to the
# number of test cases unittest loads from tests/harness/test_*.py (all but
# test_hooks_off.py, which needs a hooks-off server), N at least
# HARNESS_MIN_TESTS, and a final status line of exactly "OK" (no skips, no
# expected failures). This catches a module dropped from run.sh, a test that
# skips, and a deleted test. Raise HARNESS_MIN_TESTS when tests are added;
# lowering it is a reviewed change to this file, and the CI image carries its
# own copy (see ci/README.md).
#
# Two helper modes, used by the gate and by ci/test/gates.test.ts:
#   ci/gates.sh --harness-expected [repo-dir]       prints the expected N
#   ci/gates.sh --check-harness-output FILE N       0 when FILE shows N/N OK
set -uo pipefail

HARNESS_MIN_TESTS=97

# harness_expected <repo>: test cases unittest loads from the modules run.sh
# runs on a hooks-on server. Importing them touches no server.
harness_expected() {
  python3 - "$1/tests/harness" <<'PY'
import pathlib, sys, unittest
d = pathlib.Path(sys.argv[1])
sys.path.insert(0, str(d))
names = sorted(p.stem for p in d.glob("test_*.py") if p.stem != "test_hooks_off")
loader = unittest.TestLoader()
suite = loader.loadTestsFromNames(names)
if loader.errors or not names:
    print("cannot load the harness tests: %r" % (loader.errors or "none found"), file=sys.stderr)
    sys.exit(1)
print(suite.countTestCases())
PY
}

# check_harness_output <file> <expected>: the unittest summary in file shows
# exactly expected tests, at least HARNESS_MIN_TESTS, and a bare OK.
check_harness_output() {
  local out="$1" want="$2" ran status
  ran="$(sed -n 's/^Ran \([0-9][0-9]*\) tests\{0,1\} in .*/\1/p' "$out" | tail -n 1)"
  status="$(grep -E '^(OK|FAILED)( \(.*\))?$' "$out" | tail -n 1)"
  if ! [[ "$want" =~ ^[0-9]+$ ]]; then
    echo "harness count: no expected count"; return 1
  fi
  if [ "$want" -lt "$HARNESS_MIN_TESTS" ]; then
    echo "harness count: only $want tests found, fewer than the minimum $HARNESS_MIN_TESTS"; return 1
  fi
  if [ -z "$ran" ]; then
    echo "harness count: no 'Ran N tests' line"; return 1
  fi
  if [ "$ran" != "$want" ]; then
    echo "harness count: ran $ran of $want tests"; return 1
  fi
  if [ "$status" != "OK" ]; then
    echo "harness count: status '${status:-missing}', want a bare OK"; return 1
  fi
  echo "harness count: $ran/$want passed"
}

case "${1:-}" in
  --harness-expected)
    harness_expected "$(cd "${2:-$(dirname "${BASH_SOURCE[0]}")/..}" && pwd)"; exit ;;
  --check-harness-output)
    check_harness_output "$2" "$3"; exit ;;
esac

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
  local rc=0 want out="$WORK/harness-$barrier.out"
  (cd "$REPO" && GE_URL="http://test:write-test-token@127.0.0.1:$PORT" \
     GE_SERVER_LOG="$slog" tests/harness/run.sh) 2>&1 | tee "$out" || rc=$?
  stop_server
  if [ "$rc" = 0 ]; then
    want="$(harness_expected "$REPO")" || want=""
    check_harness_output "$out" "$want" || rc=1
  fi
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
