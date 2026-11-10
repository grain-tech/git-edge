#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

# loader.mjs uses module.registerHooks, which needs Node 22.15+ or 23.5+.
node_ver="$(node -p 'process.versions.node')"
IFS=. read -r node_major node_minor _ <<<"$node_ver"
node_ok=0
if [ "$node_major" -gt 23 ]; then
  node_ok=1
elif [ "$node_major" -eq 23 ] && [ "$node_minor" -ge 5 ]; then
  node_ok=1
elif [ "$node_major" -eq 22 ] && [ "$node_minor" -ge 15 ]; then
  node_ok=1
fi
if [ "$node_ok" -ne 1 ]; then
  echo "tests/worker/run.sh needs Node 22.15 or later (or 23.5+). Found v${node_ver}." >&2
  echo "loader.mjs uses module.registerHooks, which older Node does not provide." >&2
  exit 1
fi

if [ "${GE_SKIP_BUILD:-0}" != "1" ]; then
  (cd "$ROOT/server" && worker-build --release -- --offline --locked)
fi
exec node "$ROOT/tests/worker/upload_pack_cap.mjs"
