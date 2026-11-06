#!/usr/bin/env bash
# git-edge OSS benchmark: bare-clone a public repo, staged-push it into a
# git-edge deployment in <SLICE_MIB slices, clone back, fsck, print a row.
#
# Token always via EDGE_TOKEN env (never argv — `ps` would leak it).
#
# Single repo (owner/repo is the git-edge path, not the source path):
#   EDGE_TOKEN=… tests/bench/oss-bench.sh <git-url> <edge-base> <owner/repo>
#   EDGE_TOKEN=… tests/bench/oss-bench.sh /tmp/flask.git http://localhost:8794 pallets/flask
#
# Batch — remote path is owner/repo from the URL, or BENCH_PREFIX/name when set:
#   BENCH_REPOS="https://github.com/pallets/flask https://github.com/vitejs/vite" \
#     EDGE_BASE=https://git-edge.<acct>.workers.dev EDGE_TOKEN=… BENCH_PREFIX=bench \
#     tests/bench/oss-bench.sh
#
# Tunables:
#   SLICE_MIB   target staged-push slice size in MiB of on-disk object bytes
#               (default 60 — leaves headroom under the ~100 MB zone body cap)
#   SLICE_OBJS  max objects per staged push (default 30000 — larger slices
#               risk the ingest request budget / 240 s wall clock; if pushes
#               die mid-ingest, lower this — each push is one request)
#   BENCH_PREFIX  batch mode only: write to <prefix>/<name> instead of
#               <owner>/<name> — e.g. BENCH_PREFIX=bench -> bench/flask for
#               https://github.com/pallets/flask, so re-runs never collide
#               with real owner paths
#   BENCH_KEEP  set to keep the work dirs (bare clone + clone-back)
#
# Re-runs are cheap: a remote tip on the first-parent chain resumes mid-import;
# a remote already at the source tip is clone+fsck'd only (0 pushes).
set -euo pipefail

SLICE_MIB="${SLICE_MIB:-60}"
SLICE_BYTES=$((SLICE_MIB * 1048576))
SLICE_OBJS="${SLICE_OBJS:-30000}"
TOKEN=""; EDGE="" SRC=""; COMMITS=""

note() { echo "== $*" >&2; }
die()  { echo "FAIL: $*" >&2; return 1; }

# git with credentials from $GE_BENCH_TOKEN — never put tokens in remote URLs
# (argv shows up in `ps` for every user on the box).
gitge() {
  git -c credential.helper= \
      -c 'credential.helper=!f() { echo username=edge; echo "password=$GE_BENCH_TOKEN"; }; f' \
      "$@"
}

# "bytes objects" first reachable from commits (lo,hi] — on-disk compressed
# size is a good proxy for wire pack size; object count proxies the ingest
# subrequest budget.
est_slice() { # lo hi (indices into $COMMITS, 1-based; lo=0 means from root)
  local lo=$1 hi=$2 tip prev=""
  tip=$(sed -n "${hi}p" "$COMMITS")
  [ "$lo" -gt 0 ] && prev=$(sed -n "${lo}p" "$COMMITS")
  git -C "$SRC" rev-list --objects "$tip" ${prev:+--not "$prev"} \
    | awk '{print $1}' \
    | git -C "$SRC" cat-file --batch-check='%(objectsize:disk)' 2>/dev/null \
    | awk '{s+=$1; n++} END {print s+0, n+0}'
}

bench_inner() { # W url repo -> prints one markdown row
  local W=$1 url=$2 repo=$3
  local SRC="$W/src.git"; COMMITS="$W/commits"
  local LOG="$W/git.log" row="| $repo "

  note "$repo: bare clone $url"
  git clone --bare --quiet "$url" "$SRC" 2>"$LOG" || {
    echo "$row| - | - | - | - | - | - | **clone-src FAIL** $(tail -1 "$LOG" | tr '|' '/') |"
    return 0
  }
  local br commits objects packmib
  br=$(git -C "$SRC" symbolic-ref --short HEAD 2>/dev/null || echo main)
  commits=$(git -C "$SRC" rev-list --count "$br")
  objects=$(git -C "$SRC" rev-list --objects "$br" | wc -l | tr -d ' ')
  packmib=$(git -C "$SRC" count-objects -v | awk '/size-pack/{printf "%.0f", $2/1024}')

  # Stage boundaries on the first-parent chain: every boundary sha is a strict
  # descendant of the previous one, so each push is a clean fast-forward.
  # (Plain --topo-order can place side-branch commits between mainline ones;
  # a boundary landing there pushes a non-FF and gets rejected.)
  git -C "$SRC" rev-list --first-parent --reverse "$br" > "$COMMITS"
  local n; n=$(wc -l < "$COMMITS" | tr -d ' ')
  local lo=0 hi slice
  local REMOTE="$EDGE/$repo" t0 pushes=0 import_s clone_s fsck
  export GE_BENCH_TOKEN="$TOKEN"
  # Resume: if the remote already holds a first-parent tip (an earlier import
  # died mid-way), continue from it instead of restarting.
  local rtip
  rtip=$(gitge -C "$SRC" ls-remote "$REMOTE" "refs/heads/$br" 2>/dev/null | awk '{print $1}')
  if [ -n "$rtip" ]; then
    lo=$(grep -n "^$rtip\$" "$COMMITS" | head -1 | cut -d: -f1)
    [ -n "$lo" ] || {
      echo "$row| $commits | $objects | $packmib | - | - | - | **remote tip not on first-parent chain** |"
      return 0
    }
    # A remote already at the source tip needs no pushes: bounds stays empty,
    # the push loop is a no-op, and the shared clone+fsck+tip-check tail still
    # runs — fsck is compared against the source's own output, so pre-existing
    # findings don't false-fail the row.
    if [ "$lo" -ge "$n" ]; then
      note "$repo: already fully imported — clone+fsck only"
    else
      note "$repo: resuming from remote tip at index $lo"
    fi
  fi
  local -a bounds=()
  slice=$(( n * SLICE_MIB / (packmib + 1) ))
  local oslice=$(( n * SLICE_OBJS / (objects + 1) ))
  [ $oslice -lt $slice ] && slice=$oslice
  [ $slice -lt 256 ] && slice=256; [ $slice -gt $n ] && slice=$n
  while [ $lo -lt $n ]; do
    hi=$(( lo + slice )); [ $hi -gt $n ] && hi=$n
    while :; do
      local est eb eo; est=$(est_slice $lo $hi)
      eb=${est% *}; eo=${est#* }
      [ "$eb" -le "$SLICE_BYTES" ] && [ "$eo" -le "$SLICE_OBJS" ] && break
      [ "$hi" -le $((lo + 1)) ] && break
      hi=$(( lo + (hi - lo + 1) / 2 ))
    done
    bounds+=("$hi"); lo=$hi
  done
  note "$repo: $n commits, $objects objects, ${packmib} MiB pack -> ${#bounds[@]} staged push(es) of <=${SLICE_MIB} MiB/<=${SLICE_OBJS} objects"

  t0=$SECONDS
  for hi in "${bounds[@]}"; do
    local sha; sha=$(sed -n "${hi}p" "$COMMITS")
    gitge -C "$SRC" push "$REMOTE" "$sha:refs/heads/$br" >>"$LOG" 2>&1 || {
      echo "$row| $commits | $objects | $packmib | $pushes+FAIL | $((SECONDS - t0)) | - | **push FAIL @$hi** $(tail -1 "$LOG" | tr '|' '/') |"
      return 0
    }
    pushes=$((pushes + 1))
  done
  import_s=$((SECONDS - t0))

  note "$repo: clone back + fsck"
  t0=$SECONDS
  gitge clone --quiet "$REMOTE" "$W/clone" >>"$LOG" 2>&1 || {
    echo "$row| $commits | $objects | $packmib | $pushes | ${import_s}s | - | **clone FAIL** $(tail -1 "$LOG" | tr '|' '/') |"
    return 0
  }
  clone_s=$((SECONDS - t0))
  local tip_src tip_dst
  tip_src=$(git -C "$SRC" rev-parse "$br")
  tip_dst=$(git -C "$W/clone" rev-parse HEAD)
  [ "$tip_src" = "$tip_dst" ] || {
    echo "$row| $commits | $objects | $packmib | $pushes | ${import_s}s | ${clone_s}s | **tip mismatch** |"
    return 0
  }
  # Old repos carry pre-existing fsck findings (e.g. zeroPaddedFilemode);
  # the clone is clean iff its fsck output matches the source's byte-for-byte.
  git -C "$SRC" fsck --strict 2>&1 | sort >"$W/fsck.src"
  git -C "$W/clone" fsck --strict 2>&1 | sort >"$W/fsck.dst"
  if cmp -s "$W/fsck.src" "$W/fsck.dst"; then
    if [ -s "$W/fsck.dst" ]; then
      fsck="clean ($(wc -l <"$W/fsck.dst" | tr -d ' ') pre-existing)"
    else
      fsck=clean
    fi
  else
    fsck="**fsck FAIL** $(diff "$W/fsck.src" "$W/fsck.dst" | grep -c '^>' | tr -d ' ') new"
  fi
  echo "$row| $commits | $objects | $packmib | $pushes | ${import_s}s | ${clone_s}s | $fsck |"
}

bench_repo() {
  local W; W=$(mktemp -d "${TMPDIR:-/tmp}/ge-oss-bench.XXXXXX")
  local rc=0
  bench_inner "$W" "$1" "$2" || rc=$?
  [ -n "${BENCH_KEEP:-}" ] || rm -rf "$W"
  return $rc
}

# Token always from EDGE_TOKEN (never argv — shows up in `ps`).
TOKEN=${EDGE_TOKEN:?set EDGE_TOKEN}
if [ $# -ge 3 ]; then
  EDGE=${2%/}
  bench_repo "$1" "$3" || echo "| $3 | - | - | - | - | - | - | **error** |"
elif [ -n "${BENCH_REPOS:-}" ]; then
  EDGE=${EDGE_BASE:?set EDGE_BASE and EDGE_TOKEN}; EDGE=${EDGE%/}
  echo "| repo | commits | objects | pack MiB | pushes | import | clone | fsck |"
  echo "|---|---|---|---|---|---|---|---|"
  for u in $BENCH_REPOS; do
    p=${u%.git}; p=${p%/}
    owner=$(basename "$(dirname "$p")"); name=$(basename "$p")
    if [ -n "${BENCH_PREFIX:-}" ]; then
      remote_path="${BENCH_PREFIX}/${name}"
    else
      remote_path="${owner}/${name}"
    fi
    bench_repo "$u" "$remote_path" \
      || echo "| $remote_path | - | - | - | - | - | - | **error** |"
  done
else
  sed -n '2,20p' "$0" >&2; exit 2
fi
