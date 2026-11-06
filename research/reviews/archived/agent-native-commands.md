# Review: Agent-native protocol v2 commands (search, explain-diff, suggest-merge)

> Idea #32 · wild · verdict: **lands with caveats** · feasibility 4/5 · reliability 3/5 · correctness 3/5 · effort: weeks
> Proof: [proofs/agent-native-commands.md](../../proofs/archived/agent-native-commands.md) · Review: [reviews/agent-native-commands.md](./agent-native-commands.md)

# Review: agent-native-commands (idea #32)

## Scores
- Feasibility: 4/5. Every primitive is GA: Workers streaming via TransformStream, DecompressionStream/CompressionStream, DO SQLite `sql.exec` (FTS5 is in Cloudflare's supported-extension list for SQLite-backed DOs, but the proof never shows a test that `CREATE VIRTUAL TABLE ... fts5` actually ran), DO RPC, R2 get/put, Workers AI. No wall-clock limit on an HTTP Worker while the client stays connected, so serial AI calls are fine for CPU (they are I/O). Real risk: `@cf/meta/llama-3.1-8b-instruct` on Workers AI has an ~8k-token context; a 24 KB diff is ~6-8k tokens plus system prompt, so the `explain-diff` cap is at the edge, and `suggest-merge` feeds full ours/base/theirs with no truncation at all, so any conflict over ~20 KB total fails the AI call and the whole command returns `ERR`.
- Reliability: 3/5. No ref is ever written, so no split-brain. But the FTS table is append-only (no `DELETE WHERE path=?`), the response stream is produced by an un-awaited IIFE not registered with `ctx.waitUntil`, and `suggest/` blobs are orphans by design with a "hand-waved" sweeper.
- Correctness: 3/5. The three commands exist and the pkt-line shape is right, but the headline claim that the agent can `fetch want <sha>` the suggestion is false as written (see Interop), `mergeBase` is a `return ""` stub, and `search` has no ref/branch dimension and returns dead history after force-pushes.
- Effort: weeks, assuming `server-side-merge`, `two-phase-push`, `want-have-negotiation` and the pkt-line codec already exist; days of that is the agent-side client.

## Crash walk-through
`suggest-merge` with 5 conflicting paths; the Worker isolate is evicted (or the agent disconnects) after the 3rd AI call. State: `suggest/<repo>/<sha1..3>` written to R2, nothing in the DO, no ref touched. The agent received `merge` section, `0001`, and three `conflict ...` lines but no terminating `0000`; because the writer lives in a fire-and-forget async IIFE with no `waitUntil`, the remaining work is simply dropped. Repo integrity: intact. Data loss: none the repo cares about; the agent must treat "no flush pkt" as failure and retry, which recomputes and re-runs AI for all 5 paths (no memo for suggestions, only for explanations), producing new blobs under new shas -- the first three become permanent orphans until a janitor that the proof does not implement runs. Same crash inside `explain-diff` after `env.AI.run` but before `rememberExplanation`: one wasted AI call, retry recomputes; benign. Crash inside `indexBlobs` mid-loop: DO SQLite writes are transactional per `exec` call, not per RPC, so half the push's blobs are indexed and the push's ref update (if done in a separate call) may or may not have happened -- index and refs drift silently.

## Concurrency walk-through
Two agents ask `explain-diff base=A head=B` at once. Both miss the memo, both read R2, both call AI, both `INSERT OR REPLACE`; last writer wins, the two agents may receive different summaries for the same (A,B). No corruption, 2x AI cost; acceptable. Worse case: agent X runs `search` while a force-push to `main` lands. `indexBlobs` inserts the new blobs; the old blobs for the same paths stay. `search` now returns two rows for `src/foo.ts` with different shas, one of them unreachable from any ref, and the `rank` ordering can put the dead one first. Nothing tells the agent which row is live. Note the proof's own line 65 also runs the full R2 diff before checking the memo, so the "no second cost on retry" claim only covers the AI call, not the Class B reads.

## Interop check
- Real `git` 2.4x: `info/refs?service=git-upload-pack` with `Git-Protocol: version=2` returns `version 2` + capability lines. git's `process_capabilities_v2` stores unknown `key[=value]` lines and ignores them, so `search`, `explain-diff=ai`, `suggest-merge=dry-run` do not break clone/fetch/ls-remote. Correct.
- Request framing `command=<name>` / caps / `0001` / args / `0000` on `POST git-upload-pack` with content-type `application/x-git-upload-pack-request` is exactly v2; response sections with `0001` and `0000` mirror `fetch`. An agent's existing v2 parser will read them. Correct.
- The wire detail that breaks: the suggestion blob is PUT to `suggest/<owner/repo>/<sha>`, but the `fetch` command (per `refs-sqlite-objects-r2`) resolves `want <sha>` against `objects/<sha>`. A real `git fetch origin <sha>` therefore gets `ERR upload-pack: not our ref <sha>`; even if the key were fixed, v2 `fetch` of an unreachable oid requires the server to advertise/allow `uploadpack.allowAnySHA1InWant`, which the proof never mentions. So "fetch it as a normal object" does not happen without two extra changes. Also, the model's `response` routinely wraps output in ``` fences, so the "genuine git blob" will usually contain fence lines; the sha is real, the content is not the merged file.
- `ERR` lines are emitted as ordinary section lines after data has started, not as the first pkt; git's own `ERR` convention only applies before the section stream, so an agent parser copying git semantics will not see them as errors.

## Blockers
- `suggest/` prefix is unreachable by `fetch`; must write to `objects/<sha>` (or teach the pack builder a second prefix) and allow any-sha1-in-want for the suggestion oids.
- `mergeBase` is a stub; the whole `suggest-merge` path depends on the SQLite commit graph from `want-have-negotiation` being present.
- No truncation of conflict content before Workers AI; context overflow kills the command instead of degrading to `conflict <path>` with no suggestion.

## Caveats
- FTS index needs per-path replace and a tombstone on delete/force-push, plus a branch column, or `search` degrades into "grep over everything ever pushed".
- Stream producer must be wrapped in `ctx.waitUntil` and the agent must treat a missing `0000` as failure.
- `suggest-merge` is a write (R2 PUT + AI spend) hiding behind the upload-pack endpoint; scoped tokens must classify it as such, and the janitor for `suggest/` is unwritten.
- `explain-diff` should check the memo before doing the R2 tree/blob reads, not after.
- Verify FTS5 on DO SQLite with a one-line `wrangler dev` test before committing to the in-DO index; fall back to D1 FTS if it is missing.

## Verdict
lands-with-caveats. The protocol extension itself is sound and real git is unaffected; the read-only design means no ref-level data-loss path. But as written the proof delivers a weaker lookalike: suggestions cannot actually be fetched, the search index drifts, and the merge-base is a stub. Fixes are each days of work, none require a new primitive.
