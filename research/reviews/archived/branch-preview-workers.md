# Review: Branch previews deployed as Workers on push

> Idea #41 · wild · verdict: **lands with caveats** · feasibility 3/5 · reliability 3/5 · correctness 3/5 · effort: weeks
> Proof: [proofs/branch-preview-workers.md](../../proofs/archived/branch-preview-workers.md) · Review: [reviews/branch-preview-workers.md](./branch-preview-workers.md)

# Review: Branch previews deployed as Workers on push (#41)

## Scores
- Feasibility: 3/5. Every primitive is GA (DO SQLite, alarms, R2 get, DecompressionStream "deflate" = zlib, WfP script PUT/DELETE, dispatch_namespaces binding). Two limits are missed: (a) R2 binding calls count toward the 1,000-subrequest cap per invocation, so the alarm cannot deploy a tree with >~1,000 objects at all, never mind CPU; (b) `<branch>--<repo>.preview.example.com` is two labels below the apex, so Universal SSL does not cover it -- needs Advanced Certificate Manager or Cloudflare for SaaS. WfP itself is a paid add-on.
- Reliability: 3/5. Alarm + SQLite row is a sound durable job queue and WfP PUT is idempotent, but the ref CAS and the `deploys` INSERT are in separate calls, and `.one()` throws on an empty result.
- Correctness: 3/5. It ships a tree of pre-built ES modules as a Worker; it does not "deploy the tree" for any repo with a build step, and `scriptName` can collide distinct branches onto one script.
- Effort: weeks.

## Crash walk-through
DO evicted mid-`PUT` (after `readObject` loop, before `DELETE FROM deploys`): the row survives, the alarm is re-delivered (alarm retries on exception, and a persisted alarm re-fires after eviction), the PUT is a full replace so re-running is safe. Good.
Crash between ref CAS and `onRefUpdated`'s INSERT: `onRefUpdated` is "called by the receive-pack handler after the ref CAS succeeded" -- a separate method, so a fault between the two leaves `refs/heads/preview/foo` advanced with no deploy row. The client saw `ok refs/heads/preview/foo` and the preview silently never updates. Fix: INSERT inside the same `transactionSync` as the ref write.
`SELECT ... LIMIT 1).one()` throws when the table is empty (DO SQL `.one()` requires exactly one row), so the `if (!job) return` guard is dead and an empty-queue alarm fails and is retried up to the backoff limit. Use `.toArray()[0]`.
A non-ok WfP response (e.g. 429 rate limit, 413 over 10 MB) is logged and the job deleted -- that push is lost permanently, as the proof admits.

## Concurrency walk-through
Two pushes to `preview/foo` (sha A, then sha B) plus a delete: the DO serialises the ref CAS, rows are appended in id order, the alarm processes one row per firing, so the script ends at the last-written state. Good.
Alarm `await fetch(...)` opens the DO input gate, so a push can insert rows mid-alarm; the end-of-alarm re-check catches it. Fine.
Real problem: `scriptName` lowercases and maps every non-`[a-z0-9-]` char to `-`, then truncates to 63. `preview/Foo`, `preview/foo_`... and `preview/foo/` all collapse, and repo `a--b`/branch `c` collides with repo `a`/branch `b--c`. Two branches then overwrite each other's Worker in an order that depends only on push timing -- a silent split-brain of the preview URL. Needs a hash suffix or a name->script table.
`0{40}` delete detection is SHA-1 only; a SHA-256 repo would upload a script from the zero OID.

## Interop check
git side is unaffected: receive-pack ends at report-status, and push is v0/v1 (`git` 2.4x does not use protocol v2 for push), so deferring via alarm is invisible to the client. The wire risk is upstream of this proof: if receive-pack stores the incoming pack instead of exploding it, `objects/<sha>` misses ofs/ref-delta objects and `readObject` throws on the first thin-pack push; the proof states this dependency but a `git push` from a fresh clone will send deltas by default.
WfP side: multipart part names must equal module names and non-JS parts are sent as `text/plain`, so any binary file (png, wasm, font) is corrupted; wasm needs `application/wasm` and other bytes `application/octet-stream`. Every file in the repo becomes a module and counts against the 100-module default.

## Blockers
- R2 subrequest cap (1,000/invocation) bounds a preview to well under 1,000 files; the proof only budgets CPU time.
- Ref CAS and deploy INSERT are not one transaction -> lost deploys after `ok` was already sent.
- `scriptName` collisions let unrelated branches overwrite each other's preview.
- No build step: only repos that commit a ready-to-run `index.js` module tree are "deployed"; anything with npm/TS/bundling is out of scope, so the idea as stated is a lookalike for most real repos.

## Caveats
- `.one()` throws on empty table; WfP failures drop the job with no retry or status ref.
- Two-level wildcard hostname needs ACM/Cloudflare for SaaS; `workers.dev` cannot serve dispatch scripts.
- Broad account API token lives in the DO; a leak exposes every preview namespace.
- 128 MB DO memory and 10 MB gzipped script size cap previews; static assets need the separate upload-session flow.
- Single repo DO serialises all deploys; a busy monorepo queues behind slow tree walks.

## Verdict
lands-with-caveats. The alarm-as-post-receive-hook and the tree->multipart mapping are correct and buildable on GA APIs today, but only for small, pre-built module trees, and the proof code has two real bugs (`.one()`, non-atomic INSERT) and one design hole (name collisions) that must be fixed before it is a trustworthy preview system. Expect a few weeks to a robust version with retries, a status ref, ACM setup and an external build path.
