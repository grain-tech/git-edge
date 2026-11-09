#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
if [ "${GE_SKIP_BUILD:-0}" != "1" ]; then
  (cd "$ROOT/server" && worker-build --release -- --offline --locked)
fi
exec node "$ROOT/tests/worker/upload_pack_cap.mjs"
