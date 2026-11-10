#!/usr/bin/env bash
# Runs TLC on GcBarrier: every clean config must check clean, every vacuity
# guard in GcBarrierReach must be reachable, the statechart config must
# check clean, each Bug variant must violate its named invariant, and each
# known finding must still violate its named invariant. Caches the JRE and tla2tools.jar under
# ${GE_TLA_CACHE:-$HOME/.cache/git-edge-tla}; TLC state goes to a temp dir.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CACHE="${GE_TLA_CACHE:-$HOME/.cache/git-edge-tla}"

TOOLS="$CACHE/tla2tools.jar"
TOOLS_SHA256="936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88"
TOOLS_URL="https://github.com/tlaplus/tlaplus/releases/download/v1.7.4/tla2tools.jar"

JRE_DIR="$CACHE/jdk-21.0.13+9-jre"
JRE_TGZ="$CACHE/jre.tgz"
JRE_SHA256="a535a1df9c6284d9618872984e026ae0352d1f7e9e0605425c0e6114f47351dd"
JRE_URL="https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.13%2B9/OpenJDK21U-jre_x64_linux_hotspot_21.0.13_9.tar.gz"

mkdir -p "$CACHE"
META="$(mktemp -d "${TMPDIR:-/tmp}/ge-tlc.XXXXXX")"
trap 'rm -rf "$META"' EXIT

sha_ok() { [ -f "$1" ] && echo "$2  $1" | sha256sum -c - >/dev/null 2>&1; }

fetch() { # url dest sha256
  if ! sha_ok "$2" "$3"; then
    mkdir -p "$(dirname "$2")"
    curl -fSL "$1" -o "$2"
  fi
  sha_ok "$2" "$3" || { echo "verify: sha256 mismatch on $2" >&2; exit 1; }
}

java_major() { # prints the major version of a java binary, or nothing
  "$1" -version 2>&1 | sed -nE 's/.*version "([0-9]+)(\.([0-9]+))?.*".*/\1 \3/p' \
    | awk '{ if ($1 == 1) print $2; else print $1 }' | head -1
}

pick_java() {
  if command -v java >/dev/null 2>&1; then
    local maj
    maj="$(java_major java || true)"
    if [ "${maj:-0}" -ge 11 ] 2>/dev/null; then JAVA="$(command -v java)"; return; fi
  fi
  if [ ! -x "$JRE_DIR/bin/java" ]; then
    if [ "$(uname -sm)" != "Linux x86_64" ]; then
      echo "verify: need java >= 11 on PATH (no linux x86_64 JRE download for this platform)" >&2
      exit 1
    fi
    fetch "$JRE_URL" "$JRE_TGZ" "$JRE_SHA256"
    tar -xzf "$JRE_TGZ" -C "$CACHE"
  fi
  JAVA="$JRE_DIR/bin/java"
}

pick_java
fetch "$TOOLS_URL" "$TOOLS" "$TOOLS_SHA256"

distinct_states() {
  grep -oE '[0-9]+ distinct states found' "$1" | tail -1 | grep -oE '^[0-9]+' \
    || echo "?"
}

violated_invariant() {
  sed -nE 's/^Error: Invariant ([A-Za-z0-9_]+) is violated( by the initial state)?\.$/\1/p' "$1" | head -1
}

N=0
run_tlc() { # spec cfg-path -> sets OUT and RC
  N=$((N + 1))
  OUT="$META/run$N.log"
  mkdir -p "$META/run$N.states"
  RC=0
  (cd "$HERE" && "$JAVA" -XX:+UseParallelGC -jar "$TOOLS" -workers 1 \
     -metadir "$META/run$N.states" -config "$2" "$1") >"$OUT" 2>&1 || RC=$?
}

fail() { echo "verify: FAIL: $*" >&2; exit 1; }

run_clean() { # cfg: TLC exits 0 and finds no error, deadlock checked
  run_tlc GcBarrier.tla "$1"
  [ "$RC" -eq 0 ] && grep -q "^Model checking completed. No error has been found.$" "$OUT" \
    || { cat "$OUT" >&2; fail "$1: expected a clean run, TLC exited $RC"; }
  printf 'ok  %-44s clean, %s distinct states\n' "$1" "$(distinct_states "$OUT")"
}

run_case() { # cfg invariant: TLC exits 12 (safety violation) on that invariant
  run_tlc GcBarrier.tla "$1"
  local got
  got="$(violated_invariant "$OUT")"
  [ "$RC" -eq 12 ] && [ "$got" = "$2" ] \
    || { grep -E '^Error' "$OUT" >&2 || cat "$OUT" >&2; fail "$1: expected $2 violated (exit 12), got '${got:-none}' (exit $RC)"; }
  printf 'ok  %-44s violated %s, %s distinct states\n' "$1" "$got" "$(distinct_states "$OUT")"
}

run_reach() { # barrier world pushes guard: the clean model reaches the guarded state
  local cfg="$META/reach-$1-$2-$4.cfg"
  printf 'SPECIFICATION Spec\nCONSTANTS\n  Barrier = %s\n  Bug = "none"\n  MaxChains = 2\n  DeltaCap = 1\n  MaxErrs = 1\n  Scope = "full"\n  World = "%s"\n  Garbage = %s\n  MaxPushes = %s\nINVARIANT\n  %s\n' \
    "$1" "$2" "$([ "$2" = kept ] && echo FALSE || echo TRUE)" "$3" "$4" >"$cfg"
  run_tlc GcBarrierReach.tla "$cfg"
  local got mode
  got="$(violated_invariant "$OUT")"
  mode="$([ "$1" = TRUE ] && echo barrier || echo legacy)"
  [ "$2" = single ] || mode="$mode $2"
  [ "$RC" -eq 12 ] && [ "$got" = "$4" ] \
    || { grep -E '^Error' "$OUT" >&2 || true; fail "$4 ($mode): guarded state unreachable (exit $RC) -- the clean run is vacuous"; }
  printf 'ok  %-44s reached\n' "reach $mode $4"
}

run_finding() { # cfg invariant: a known finding in the real design. TLC
  # must still exit 12 on that invariant. If it stops doing so, the code or
  # the model changed: re-read the finding in the skill before flipping
  # the case to a clean check.
  run_tlc GcBarrier.tla "$1"
  local got
  got="$(violated_invariant "$OUT")"
  if [ "$RC" -eq 12 ] && [ "$got" = "$2" ]; then
    printf 'FINDING %-40s violated %s, %s distinct states\n' "$1" "$got" "$(distinct_states "$OUT")"
    return
  fi
  grep -E '^Error' "$OUT" >&2 || true
  fail "$1: the known finding no longer reproduces (expected $2 violated, got '${got:-none}', exit $RC). If server/ was fixed, confirm the fix against the Findings section of .devin/skills/verify-gc-barrier/SKILL.md, then turn this case into a clean check"
}

run_clean GcBarrier_barrier.cfg
run_clean GcBarrier_legacy.cfg
run_clean GcBarrier_split_legacy.cfg
run_clean GcBarrier_kept_barrier.cfg
run_clean GcBarrier_kept_legacy.cfg
run_clean statechart/ThinPushRace.cfg

for g in Reach_LandThenRetire Reach_LoserThenRetire Reach_BareLandsMidChain \
         Reach_ErrThenSurvivor Reach_EpochReject Reach_FetchOutlivesPack \
         Reach_FetchRebuild Reach_PlanNamesDead; do
  run_reach TRUE single 1 "$g"
  run_reach FALSE single 1 "$g"
done
run_reach TRUE single 1 Reach_DrainMarksNew
run_reach TRUE single 1 Reach_DeltaHandoff
run_reach TRUE split 2 Reach_PartialRetire
run_reach TRUE kept 2 Reach_FallbackRetire
run_reach TRUE kept 2 Reach_NonLossySwap
run_reach TRUE kept 2 Reach_ThinOverLosslessRetire

run_case GcBarrier_bug_noBarrierPending.cfg        YoungPacksClosed
run_case GcBarrier_bug_noLandingFeed.cfg           RefsResolvable
run_case GcBarrier_bug_noEpochCheck.cfg            YoungPacksClosed
run_case GcBarrier_bug_sweepIgnoresRefsVersion.cfg RefsResolvable
run_case GcBarrier_bug_abortDropsSurvivor.cfg      RefsResolvable
run_case GcBarrier_bug_janitorNoGrace.cfg          FetchReadsSucceed
run_case GcBarrier_bug_fallbackUsesBitmap.cfg      RefsResolvable

run_finding GcBarrier_finding_split.cfg      LiveClosed
run_finding GcBarrier_finding_split_refs.cfg RefsResolvable

echo "verify: all checks passed, with 2 known findings"
