#!/usr/bin/env bash
# git-edge-import — push a repository into a git-edge deployment in staged
# slices that each stay under the ~100 MB per-request body cap. Zero
# server-side work: it is just `git push` of commit ranges, oldest to newest,
# so every push is a clean fast-forward.
#
#   tools/git-edge-import.sh [options] <local-repo-path-or-url> <edge-repo-url>
#
#   tools/git-edge-import.sh ./myrepo http://localhost:8796/acme/myrepo
#   GE_TOKEN=ge_... tools/git-edge-import.sh \
#     https://github.com/pallets/flask https://<host>/pallets/flask
#
# Options:
#   --branch <name>   import only this branch (default: the source's HEAD
#                     branch — usually main or master)
#   --all-branches    import every refs/heads/*; the HEAD branch goes first so
#                     it becomes the remote default. Each branch is sliced on
#                     its own first-parent chain.
#   --tags            after the branches, push all refs/tags/* in one push
#   --dry-run         print the slice plan (boundaries, est MiB + objects per
#                     slice) and push nothing
#   --keep            keep the temp bare clone made for URL sources
#
# Environment:
#   GE_TOKEN    write token. Used through a credential helper — never embedded
#               in the remote URL (argv shows up in `ps` for every user on the
#               box). Unset = rely on the URL's own credentials or a
#               configured helper.
#   SLICE_MIB   target max MiB of new on-disk object bytes per push (def 60 —
#               headroom under the ~100 MB zone body cap; the wire pack is
#               usually smaller than the on-disk estimate)
#   SLICE_OBJS  max new objects per push (def 30000 — the ingest subrequest
#               budget died mid-push at ~50k objects on a wide first slice)
#
# Resume: if the remote branch already ends at a commit on this repo's
# first-parent chain — or at any ancestor of the branch tip — an earlier
# import died mid-way and this run continues from it. If the remote tip is
# NOT an ancestor of the source tip the import is refused: this tool never
# force-pushes. Re-running is always safe; each slice is idempotent.
set -euo pipefail

note() { echo "== $*" >&2; }
die()  { echo "git-edge-import: FAIL: $*" >&2; exit 1; }

SLICE_MIB="${SLICE_MIB:-60}"
SLICE_OBJS="${SLICE_OBJS:-30000}"
case "$SLICE_MIB" in ''|*[!0-9]*) die "SLICE_MIB must be a positive integer" ;; esac
case "$SLICE_OBJS" in ''|*[!0-9]*) die "SLICE_OBJS must be a positive integer" ;; esac
SLICE_BYTES=$((SLICE_MIB * 1048576))

usage() { sed -n '2,39p' "$0" >&2; exit "${1:-2}"; }

# git with credentials from $GE_TOKEN — see header for why not in the URL.
gitge() {
  if [ -n "${GE_TOKEN:-}" ]; then
    git -c credential.helper= \
        -c 'credential.helper=!f() { echo username=edge; echo "password=$GE_TOKEN"; }; f' \
        "$@"
  else
    git "$@"
  fi
}

ALL_BRANCHES=0 DRY_RUN=0 PUSH_TAGS=0 KEEP=0 BRANCH=""
POS=()
while [ $# -gt 0 ]; do
  case "$1" in
    --branch)        BRANCH="${2:?--branch needs a name}"; shift ;;
    --branch=*)      BRANCH="${1#*=}" ;;
    --all-branches)  ALL_BRANCHES=1 ;;
    --tags)          PUSH_TAGS=1 ;;
    --dry-run)       DRY_RUN=1 ;;
    --keep)          KEEP=1 ;;
    -h|--help)       usage 0 ;;
    --)              shift; break ;;
    -*)              die "unknown option: $1" ;;
    *)               POS+=("$1") ;;
  esac
  shift
done
POS+=("$@")
[ "${#POS[@]}" -eq 2 ] || usage
[ "$ALL_BRANCHES" = 0 ] || [ -z "$BRANCH" ] || die "--all-branches and --branch are mutually exclusive"
SRC_IN="${POS[0]}"
REMOTE="${POS[1]%/}"; REMOTE="${REMOTE%.git}"
[ "$SLICE_MIB" -lt 95 ] || note "warning: SLICE_MIB=$SLICE_MIB leaves no headroom under the ~100 MB body cap"

WORK="$(mktemp -d "${TMPDIR:-/tmp}/ge-import.XXXXXX")"
trap '[ "$KEEP" = 1 ] || rm -rf "$WORK"' EXIT
PLOG="$WORK/push.log"

# --- resolve the source into a git dir we can run plumbing against ---------
if [ -d "$SRC_IN" ]; then
  SRC=$(git -C "$SRC_IN" rev-parse --absolute-git-dir 2>/dev/null) \
    || die "not a git repository: $SRC_IN"
  note "source: local repo $SRC"
else
  SRC="$WORK/src.git"
  note "source: bare-cloning $SRC_IN"
  gitge clone --bare --quiet "$SRC_IN" "$SRC" || die "source clone failed: $SRC_IN"
fi

# --- pick branches ----------------------------------------------------------
HEAD_BR=$(git -C "$SRC" symbolic-ref --short HEAD 2>/dev/null || true)
BRANCHES=()
if [ "$ALL_BRANCHES" = 1 ]; then
  while IFS= read -r b; do BRANCHES+=("$b"); done \
    < <(git -C "$SRC" for-each-ref --format='%(refname:short)' refs/heads/)
  [ "${#BRANCHES[@]}" -gt 0 ] || die "source has no branches under refs/heads/"
  # HEAD branch first: the server adopts the first pushed branch as the
  # remote default, and it gets the biggest share of the object graph anyway.
  if [ -n "$HEAD_BR" ]; then
    re=(); for b in "${BRANCHES[@]}"; do [ "$b" = "$HEAD_BR" ] || re+=("$b"); done
    BRANCHES=("$HEAD_BR" ${re[@]+"${re[@]}"})
  fi
elif [ -n "$BRANCH" ]; then
  BRANCHES=("$BRANCH")
elif [ -n "$HEAD_BR" ]; then
  BRANCHES=("$HEAD_BR")
else
  die "source HEAD is detached — pass --branch <name>"
fi
for b in "${BRANCHES[@]}"; do
  git -C "$SRC" rev-parse --verify --quiet "refs/heads/$b" >/dev/null \
    || die "no such branch in source: $b"
done

# --- remote reachability ----------------------------------------------------
if gitge ls-remote "$REMOTE" >"$WORK/ls0" 2>"$WORK/ls0.err"; then
  :
elif [ "$DRY_RUN" = 1 ]; then
  note "warning: $REMOTE unreachable — planning from scratch: $(tail -1 "$WORK/ls0.err")"
else
  die "cannot reach $REMOTE — check the URL and GE_TOKEN: $(tail -1 "$WORK/ls0.err")"
fi

remote_tip() { # branch -> sha or empty
  gitge -C "$SRC" ls-remote "$REMOTE" "refs/heads/$1" 2>/dev/null | awk '{print $1}'
}

# "bytes objects" first reachable from (prev, tip]: on-disk compressed size is
# a good proxy for wire pack size; object count proxies the ingest budget.
# prev defaults to chain[$lo]; pass an override sha for off-chain resumes.
CF=""
est_slice() { # lo hi [prev-sha]
  local lo=$1 hi=$2 tip prev="${3:-}"
  tip=$(sed -n "${hi}p" "$CF")
  if [ -z "$prev" ] && [ "$lo" -gt 0 ]; then prev=$(sed -n "${lo}p" "$CF"); fi
  git -C "$SRC" rev-list --objects "$tip" ${prev:+--not "$prev"} \
    | awk '{print $1}' \
    | git -C "$SRC" cat-file --batch-check='%(objectsize:disk)' 2>/dev/null \
    | awk '{s+=$1; n++} END {print s+0, n+0}'
}

# push one slice with 429/503 retry; return 2 on non-fast-forward, 1 other failure
push_slice() { # sha branch
  local attempt=1
  while :; do
    if gitge -C "$SRC" push "$REMOTE" "$1:refs/heads/$2" >"$PLOG" 2>&1; then
      return 0
    fi
    if grep -qi 'non-fast-forward\|fetch first\|stale info' "$PLOG"; then
      return 2
    fi
    # 429 = push rate limit, 503 = live-pack admission cap — both carry
    # Retry-After, so the same wait-and-retry applies
    if grep -q 'HTTP 429\|HTTP 503' "$PLOG" && [ "$attempt" -lt 4 ]; then
      note "push throttled ($(grep -o 'HTTP 429\|HTTP 503' "$PLOG" | head -1)) — sleeping 65s, retry $attempt/3"
      grep -q 'HTTP 503' "$PLOG" \
        && note "the repo is at its live-pack cap — if the retries run out, re-run later"
      sleep 65; attempt=$((attempt + 1)); continue
    fi
    return 1
  done
}

mib() { awk -v b="$1" 'BEGIN{printf "%.1f", b/1048576}'; }

plan_over=0 total_slices=0 total_bytes=0

for BR in "${BRANCHES[@]}"; do
  CF="$WORK/commits.${BR//\//_}"
  # First-parent chain: every boundary sha is a strict descendant of the
  # previous one, so each push is a clean fast-forward. Plain --topo-order
  # can land a boundary on a merge's side-branch commit -> non-FF reject.
  git -C "$SRC" rev-list --first-parent --reverse "$BR" > "$CF"
  n=$(wc -l < "$CF" | tr -d ' ')
  if [ "$n" -eq 0 ]; then note "[$BR] no commits — skipped"; continue; fi
  tip=$(tail -1 "$CF")

  # --- resume point ---------------------------------------------------------
  lo=0 first_floor="" first_prev=""
  rtip=$(remote_tip "$BR" || true)
  if [ "$rtip" = "$tip" ]; then
    note "[$BR] already imported (remote tip == source tip)"
    continue
  elif [ -n "$rtip" ]; then
    k=$(grep -n "^$rtip\$" "$CF" | head -1 | cut -d: -f1 || true)
    if [ -n "$k" ]; then
      lo=$k
      note "[$BR] resuming: remote tip is first-parent commit $k/$n"
    elif git -C "$SRC" cat-file -e "$rtip^{commit}" 2>/dev/null \
         && git -C "$SRC" merge-base --is-ancestor "$rtip" "$tip"; then
      # Remote tip is an ancestor off the first-parent chain (e.g. history
      # was rewritten between runs). Binary-search the first chain commit
      # that contains it; the first slice must reach at least that boundary
      # or the push is not a fast-forward.
      a=0; b=$n
      while [ $((b - a)) -gt 1 ]; do
        m=$(( (a + b) / 2 ))
        if git -C "$SRC" merge-base --is-ancestor "$rtip" "$(sed -n "${m}p" "$CF")"; then
          b=$m; else a=$m; fi
      done
      lo=$((b - 1)); first_floor=$b; first_prev=$rtip
      note "[$BR] resuming: remote tip ${rtip:0:12} is an ancestor off the first-parent chain; first slice must reach commit $b/$n"
    else
      die "[$BR] remote tip $rtip is not an ancestor of source tip $tip — the remote branch has diverged. git-edge-import never force-pushes. Delete the remote repo (POST $REMOTE/_admin/delete) or import under a different name."
    fi
  fi

  # --- slice plan -----------------------------------------------------------
  objects=$(git -C "$SRC" rev-list --objects "$BR" | wc -l | tr -d ' ') \
    || die "[$BR] rev-list failed"
  packmib=$(git -C "$SRC" count-objects -v | awk '/^size-pack/{printf "%.0f", $2/1024}') \
    || die "[$BR] count-objects failed"
  slice=$(( n * SLICE_MIB / (packmib + 1) ))
  oslice=$(( n * SLICE_OBJS / (objects + 1) ))
  [ "$oslice" -lt "$slice" ] && slice=$oslice
  [ "$slice" -lt 256 ] && slice=256; [ "$slice" -gt "$n" ] && slice=$n

  bounds=(); rl=(); ests=(); over_idx=""
  cur=$lo
  while [ "$cur" -lt "$n" ]; do
    hi=$(( cur + slice )); [ "$hi" -gt "$n" ] && hi=$n
    minhi=$(( cur + 1 ))
    [ -n "$first_floor" ] && [ "$first_floor" -gt "$minhi" ] && minhi=$first_floor
    while :; do
      est=$(est_slice "$cur" "$hi" "$first_prev") \
        || die "[$BR] size estimate failed for range $cur..$hi"
      eb=${est% *}; eo=${est#* }
      { [ "$eb" -le "$SLICE_BYTES" ] && [ "$eo" -le "$SLICE_OBJS" ]; } && break
      if [ "$hi" -le "$minhi" ]; then over_idx=$hi; break; fi
      hi=$(( cur + (hi - cur + 1) / 2 )); [ "$hi" -lt "$minhi" ] && hi=$minhi
    done
    rl+=("$cur"); bounds+=("$hi"); ests+=("$eb $eo")
    cur=$hi; first_floor=""; first_prev=""
    [ -n "$over_idx" ] && break
  done

  if [ -n "$over_idx" ]; then
    csha=$(sed -n "${over_idx}p" "$CF")
    last_lo=${rl[$(( ${#rl[@]} - 1 ))]}
    msg="[$BR] unsplittable: the range ending at commit $over_idx/$n ($csha) introduces ~$(mib "$eb") MiB / $eo objects"
    [ "$over_idx" -eq $((last_lo + 1)) ] \
      && msg="[$BR] unsplittable: commit $csha alone introduces ~$(mib "$eb") MiB / $eo objects"
    if [ "$DRY_RUN" = 1 ]; then
      note "$msg — exceeds ${SLICE_MIB} MiB / ${SLICE_OBJS} objects per push"
      plan_over=1
    else
      die "$msg — exceeds the per-push cap (${SLICE_MIB} MiB / ${SLICE_OBJS} objects). A single push step cannot be split further. Options: raise SLICE_MIB/SLICE_OBJS if it still fits the real server limits (~100 MB body, ~50k objects ingest); run 'git gc --aggressive' (or 'git repack -adf') in the source to shrink deltas; or use a server-side import (ROADMAP P0 #4 option b) once it exists."
    fi
  fi

  # --- dry-run: print the plan, push nothing --------------------------------
  if [ "$DRY_RUN" = 1 ]; then
    echo "branch $BR: $n first-parent commits, resuming at $lo -> ${#bounds[@]} slice(s)"
    for i in "${!bounds[@]}"; do
      e=${ests[$i]}
      printf '  slice %d: commits %d..%-6d -> %s  ~%s MiB, %s objects%s\n' \
        $((i + 1)) $(( ${rl[$i]} + 1 )) "${bounds[$i]}" \
        "$(sed -n "${bounds[$i]}p" "$CF" | cut -c1-12)" \
        "$(mib "${e% *}")" "${e#* }" \
        "$([ "$over_idx" = "${bounds[$i]}" ] && echo '  ** OVER CAP **')"
    done
    continue
  fi

  # --- push ------------------------------------------------------------------
  nb=${#bounds[@]}
  tb=0; for e in "${ests[@]}"; do tb=$((tb + ${e% *})); done
  note "[$BR] $n commits, $objects objects, ${packmib} MiB pack -> $nb staged push(es) of <=${SLICE_MIB} MiB/<=${SLICE_OBJS} objects"
  b_done=0; b_t0=$SECONDS
  for i in "${!bounds[@]}"; do
    hi=${bounds[$i]}; sha=$(sed -n "${hi}p" "$CF"); e=${ests[$i]}
    t0=$SECONDS
    rc=0; push_slice "$sha" "$BR" || rc=$?
    case "$rc" in
      0) ;;
      2) die "[$BR] remote moved during import — push of ${sha:0:12} rejected non-fast-forward at slice $((i + 1))/$nb. Another writer pushed concurrently. Re-run the same command to resume; if it persists, the remote history diverged." ;;
      *) die "[$BR] push failed at slice $((i + 1))/$nb: $(tail -3 "$PLOG" | tr '\n' ' ')" ;;
    esac
    b_done=$((b_done + ${e% *}))
    total_bytes=$((total_bytes + ${e% *})); total_slices=$((total_slices + 1))
    eta="?"
    if [ "$b_done" -gt 0 ] && [ "$tb" -gt "$b_done" ]; then
      eta=$(( (SECONDS - b_t0) * (tb - b_done) / b_done ))
    elif [ "$b_done" -ge "$tb" ]; then eta=0; fi
    note "[$BR] slice $((i + 1))/$nb pushed in $((SECONDS - t0))s — est $(mib "$b_done")/$(mib "$tb") MiB this branch — ETA ~${eta}s"
  done
  rnow=$(remote_tip "$BR" || true)
  [ "$rnow" = "$tip" ] || die "[$BR] post-push remote tip $rnow != $tip"
done

# --- tags --------------------------------------------------------------------
if [ "$PUSH_TAGS" = 1 ]; then
  ntags=$(git -C "$SRC" tag | wc -l | tr -d ' ')
  if [ "$ntags" -eq 0 ]; then
    note "no tags in source"
  elif [ "$DRY_RUN" = 1 ]; then
    echo "tags: would push $ntags ref(s) in one push"
  else
    note "pushing $ntags tag(s) in one push"
    gitge -C "$SRC" push "$REMOTE" 'refs/tags/*:refs/tags/*' >"$PLOG" 2>&1 \
      || die "tag push failed: $(tail -3 "$PLOG" | tr '\n' ' ')"
  fi
fi

if [ "$DRY_RUN" = 1 ]; then
  [ "$plan_over" = 0 ] || die "plan contains unsplittable slice(s) — see above"
  note "dry-run only — nothing pushed"
  exit 0
fi
[ "$total_slices" -gt 0 ] \
  && echo "imported ${#BRANCHES[@]} branch(es) in $total_slices slice(s), ~$(mib "$total_bytes") MiB est — verify with: git clone $REMOTE" \
  || echo "nothing to push"
