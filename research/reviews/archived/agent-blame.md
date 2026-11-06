# Review: Blame that knows which agent wrote each line

> Idea #51 · wild · verdict: **risky** · feasibility 3/5 · reliability 3/5 · correctness 2/5 · effort: weeks
> Proof: [proofs/agent-blame.md](../../proofs/archived/agent-blame.md) · Review: [reviews/agent-blame.md](./agent-blame.md)

# Review: agent-blame (idea #51)

## Scores
- Feasibility: 3/5. Every primitive is GA (DO SQLite `sql.exec`, DO RPC, alarms, R2 get, `DecompressionStream("deflate")` = zlib, matching `zlibLoose` from two-phase-push). Limits are not respected as written: (a) line 60 fetches `cur` *before* recursing, so a cold blame with budget 1000 holds up to 1000 inflated blobs + 1000 JSON memo arrays on the stack at once -- a 200 KB file blows the 128 MB isolate; (b) memo rows are whole-file JSON with a 40-char sha per line; DO SQLite caps a single value at 2 MB, so a ~15k-line file cannot be memoised and every `INSERT` throws; (c) `blobAtPath` is a tree walk of depth d = d+1 R2 GETs per revision, not "one R2 GET per revision", and the chain visits *every* first-parent commit, not just the ones that touch the path -- a 50k-commit repo, depth-4 path is ~250k R2 GETs (~1-2 h of alarm chain) for one cold blame and 50k memo rows for a file touched ten times. The "~1 GB" estimate in Known limits undercounts by the touched/total ratio.
- Reliability: 3/5. No ref is written, so no split-brain. But memo poisoning is permanent (see Concurrency), `blame_jobs` that consistently exceed the 30 s DO CPU budget exhaust the 6 alarm retries and sit forever while the agent polls `status pending`, and `recordTrailers` is only atomic with the ref flip if the push commit step calls it synchronously in the same tick -- the proof shows it as a separate RPC (`stub.recordTrailers()`), so a crash between the two leaves an agent commit reported as `human` with no backfill.
- Correctness: 2/5. The trailer + attestation design is sound and git-native. The blame is not: first-parent-only means any PR merged with a merge commit attributes every line of the feature branch to the *merge* commit, whose trailers (if any) belong to the merging human/bot -- exactly the mainstream agent workflow loses attribution. Squash merges also drop trailers (acknowledged). It "achieves the goal" only for fast-forward/rebase-merge linear histories. Two code bugs on top: `cursor.one()` throws when zero rows match, so line 54 (memo miss) and line 50 (unknown sha) throw on every cold call; and `cur.split("\n")` on a newline-terminated file yields a trailing empty line, so line numbers are 1-based off the real count by one.
- Effort: weeks. Fixes needed: iterative (not recursive) walk, skip revisions whose tree entry sha is unchanged, memo eviction plus chunking under 2 MB, all-parent merge handling, `.one()`->`toArray()[0]`, pkt-line long-line handling. Client is trivial if agent-native-commands' pkt writer exists.

## Crash walk-through
Agent pushes 3 commits with `Agent-Session` trailers. Phase 2 `stub.commit()` flips `refs/heads/feat`, then the Worker calls `stub.recordTrailers()` per commit and the DO isolate is evicted after the first. State: ref advanced (durable), `commit_agent` has 1 of 3 rows, `commits` graph complete. No retry path re-inflates commits already committed, so `blame` forever prints `human` for the two lost commits -- silent misattribution, not loss of repo data. Second crash: alarm draining a 1000-revision job dies after 400 memo inserts (each `exec` is its own implicit transaction, so the 400 persist). The alarm retries, resumes from the memo boundary -- correct and idempotent. If instead it dies from CPU every time (large file, Myers O(ND) on 1000 revisions), after 6 retries the alarm is dropped and `blame_jobs` keeps the row; the next agent call re-`setAlarm`s and the cycle repeats. Nothing corrupts, nothing completes.

## Concurrency walk-through
DO is single-threaded, but R2 `get` does not hold the input gate, so a push commit step and a blame interleave. Agent A asks `blame path rev=X` where X is a sha whose objects are in R2 (phase 1 done) but whose `commits` row is not yet inserted (phase 2 pending or rejected). `blobAtPath(X)` succeeds from R2, `firstParent(X)` finds no row -> (with the `.one()` bug fixed) `null` -> `base=[]` -> every line attributed to X, and line 68 memoises that permanently. Once the push lands, every later blame of X and of all its descendants inherits the poisoned row. Two agents blaming the same cold (path, rev) concurrently do duplicate R2/diff work and both `INSERT OR REPLACE` identical results -- harmless but doubles the cost. A concurrent force-push does not affect memo keys (immutable shas), only leaves dead rows.

## Interop check
- Trailers: `Agent-Session: session_...` is a valid trailer token; `git commit --trailer` (2.32+) writes it, `git log --format=%(trailers:key=Agent-Session,valueonly)` reads it. The parser deviates from `interpret-trailers` (git allows up to 25% non-trailer lines and `Signed-off-by`-anchored paragraphs; this one rejects the whole paragraph on one stray line) so some agent messages silently lose attribution, but no wire breakage.
- Advertisement: `blame=agent` as an extra v2 capability is ignored by git's `process_capabilities_v2`; `git clone/fetch/ls-remote` 2.4x unaffected. `command=blame` on `POST git-upload-pack` with `application/x-git-upload-pack-request` is well-formed v2; real git can never send it (acknowledged).
- Exact wire detail that breaks: a pkt-line payload is at most 65516 bytes. Line 96 emits one pkt per source line with the full text; any minified/vendored file with a line over ~65 KB makes `pkt()` produce an invalid length (or throw), and the agent's parser desyncs mid-stream. Needs side-band-style chunking or a `lines` section with continuation pkts. Also the response has no `0001` delimiter between the `blame` section header and rows, unlike `fetch`'s sections -- consistent within this proof but different from the v2 idiom used elsewhere in the catalog.

## Blockers
- Merge commits: first-parent-only blame discards agent attribution for every merge-based PR flow; needs all-parent inheritance (as git does) before the feature meets its own title.
- `cursor.one()` throws on zero rows (lines 50, 54): every cold blame 500s as written.
- Recursion holds every revision's blob in memory before unwinding; must be an iterative forward pass from the nearest memo.
- Memo poisoning when `rev` lacks a `commits` row: must refuse (`ERR unknown commit`) rather than attribute to `rev` and cache.
- Memo rows exceed DO SQLite's 2 MB value cap for large files; no eviction.

## Caveats
- Full-chain walk (every commit, tree walk per commit) makes cold blame of an old file hours of alarm chain and hundreds of thousands of Class B reads; skipping unchanged tree entries is mandatory, not an optimisation.
- `recordTrailers` must run inside the same synchronous tick/`transactionSync` as the ref flip or misattribution after a crash is permanent.
- Attestation only means anything with per-session scoped tokens (`scoped-token-remotes`); unscoped tokens make the `?` suffix universal.
- Non-git parser rejects trailer paragraphs git would accept; renames reset attribution; pkt-line 65 KB line cap; off-by-one on trailing newline.

## Verdict
risky -- the trailer/attestation half is git-native and cheap; the blame half is a first-parent, memory-unbounded, poison-prone approximation that misattributes exactly the merge-commit workflow agents most commonly use. Lands after a rewrite of `blame()`, not as written.
