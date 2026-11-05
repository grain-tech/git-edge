#!/usr/bin/env bash
# PR0 deterministic GC harness suite. The dev server must run with
# GE_TEST_HOOKS=1 GE_TEST_MANUAL_JOBS=1 GE_GC_QUIET_MS=0 GE_GC_GRACE_MS=0
# (GE_RATE_PUSHES_PER_MIN=0 for test_f's 101-pack clone) — tests/harness/README.md.
# GE_HOOKS_OFF=1 runs ONLY the hooks-off inertness suite (server WITHOUT the
# test vars) instead.
set -euo pipefail
cd "$(dirname "$0")/../.."
if [ "${GE_HOOKS_OFF:-0}" = "1" ]; then
  exec python3 -m unittest -v tests/harness/test_hooks_off.py
fi
export GE_SERVER_LOG="${GE_SERVER_LOG:-/tmp/ge-dev.log}"
exec python3 -m unittest -v tests/harness/test_harness.py tests/harness/test_gc_safety.py tests/harness/test_compaction.py
