# Review: Auth and multi-tenancy: owner/repo routing to DO ids

> Idea #54 · foundation · verdict: **lands with caveats** · feasibility 5/5 · reliability 4/5 · correctness 4/5 · effort: days
> Proof: [proofs/auth-and-multitenancy.md](../../proofs/archived/auth-and-multitenancy.md) · Review: [reviews/auth-and-multitenancy.md](./auth-and-multitenancy.md)

# Review: auth-and-multitenancy (idea #54)

## Scores
- Feasibility 5/5. Workers `fetch`, `crypto.subtle` HMAC-SHA256, DO SQLite (`ctx.storage.sql.exec`, `new_sqlite_classes`), `idFromName`, `jurisdiction("eu")`, the `DurableObject` RPC base class (compat date >= 2024-04-03), KV: all GA. No alarms, R2, Queues, WebSockets. CPU per request is microseconds; streaming `req.body` into `stub.fetch` keeps memory flat. Multi-statement `sql.exec` without bindings is allowed. Nothing exotic.
- Reliability 4/5. This idea writes only `meta`/`acl` rows via idempotent `INSERT OR REPLACE`; synchronous writes in one DO event are coalesced into one atomic commit, so no torn state. Deductions: KV-based revocation is eventually consistent (up to 60s, admitted); ACL is checked once at request start, so a `grant`/revoke landing mid-stream does not affect the in-flight push (acceptable, same as GitHub, but undocumented); every request for a non-existent repo runs `CREATE TABLE` in the constructor and thus materializes durable storage for a DO that should never exist.
- Correctness 4/5. Routing, edge token verification and per-repo ACL in SQLite are exactly what was asked, and the 401/403 placement matches git. Deductions below: the "do not leak existence" claim is false for anonymous callers; DO naming is inconsistent with the two sibling proofs it depends on; `jurisdiction()` contradicts "no storage read on the hot path"; the bullet about un-rewindable pack bodies misdescribes git.
- Effort: days. Under 200 lines, testable with `git ls-remote`/`git push` and `GIT_TRACE_CURL=1` in a day; the alias table and revocation list are another day or two each.

## Crash walk-through
Admin calls `create("alice","private")`: two `exec` calls with no `await` between them. If the DO is evicted before the event completes, the output gate never opens, neither row is durable, and the caller's RPC rejects; retry re-runs both `INSERT OR REPLACE` statements, which are idempotent. If the crash happens after commit but before the RPC reply, retry is a harmless overwrite. Same for `grant`. The request path (`authorize`) is read-only, so eviction mid-`fetch` surfaces to the Worker as a rejected `stub.fetch` (uncaught in the proof, so a 1101 error page rather than a clean 502; git treats both as a failed request and the user retries). No R2 objects are written by this idea, so nothing is orphaned. Wrapping `create` in `transactionSync` costs nothing and removes reliance on write coalescing.

## Concurrency walk-through
Push from bob and `grant("bob","read")` (downgrade from write) arrive at the same DO within the same millisecond. The DO runs one event at a time; `authorize` does two synchronous `SELECT`s with no `await`, so bob's push sees either the old row or the new one, never a torn view. If the push is admitted and then the protocol layer awaits (streaming pack from R2), the downgrade commits while the push is in flight and the push still completes: the verdict is point-in-time, not continuously enforced. Two admins calling `create` concurrently: last `INSERT OR REPLACE` wins for visibility, no corruption. Two Workers in different colos resolving `idFromName("alice/repo")` get the same DO, so there is no path to two ACL authorities for one repo. The only split-brain risk lives in the unimplemented rename alias: once `alias:<old>` lives in KV, a request racing a rename can resolve to the old DO for up to 60s, and a new repo created at the old name collides with the stale alias unless the alias lookup runs before `idFromName` on every request (which reintroduces a KV read on the hot path).

## Interop check
- First request anonymous, `401` + `WWW-Authenticate: Basic realm=...` on `info/refs`, git calls `credential_fill` and retries once; a second `401` with credentials present triggers `credential_reject` and `fatal: Authentication failed`. Matches `http.c:handle_curl_result` / `http_request_reauth`. The `WWW-Authenticate` header is load-bearing: without it libcurl reports no available auth method and never attaches the credential. Present in the proof. Correct.
- `403` for valid-token-wrong-repo: git dies with "The requested URL returned error: 403", no prompt loop. Correct.
- Username coupling: `subject === user` requires the Basic username to equal the token subject. Git prompts "Username for 'https://host':" and stores whatever the user typed; anyone who clones with `https://x@host/...` or uses `x-access-token` gets `401` then `fatal: Authentication failed`. Not a wire break, but GitHub/GitLab ignore the username for PATs; do the same, or the helper-stored username becomes a hidden failure mode.
- Overstated claim: "git cannot rewind a streamed pack body on a late 401" is not how remote-curl behaves. For bodies over `http.postBuffer`, `post_rpc` first sends `probe_rpc` (a POST whose body is a lone `0000` flush) to settle auth, and only then streams the chunked pack. Challenging on `info/refs` is still the right design, but the protocol layer must answer that flush-only POST to `/git-receive-pack` with 200 or large authenticated pushes break with "RPC failed; HTTP 4xx".
- `Authorization: Bearer` (common in CI via `http.extraHeader`) is rejected as anonymous. Fine for v1, document it.
- Malformed `Authorization` (bad base64, missing `.`) throws inside `atob`/`b64u` and yields a 500 instead of 401. Real git never sends this; fuzz robustness only.
- No wire detail breaks a git 2.4x client on the paths this proof owns.

## Blockers
None for the idea in isolation. It delivers the routing, edge verification and per-repo ACL it promises.

## Caveats
- Existence leak: anonymous `info/refs` on a private repo gets `401`, on a non-existent repo gets `403`. An unauthenticated caller can enumerate which private repos exist, the opposite of the stated intent. Return the `401` challenge to anonymous callers in both cases and `403`/`404` only after a principal is verified.
- DO name mismatch: this proof lowercases `owner/repo` and strips `.git`; `repo-do-ref-authority` and `info-refs-endpoint` call `idFromName` on the raw captures. `Foo/Bar` would hit different DOs on different code paths. Centralize `repoName(owner, repo)` and use it everywhere.
- `jurisdiction("eu").idFromName(x)` is a different id from `idFromName(x)`, so the per-tenant jurisdiction must be known at the edge before routing, i.e. a KV/owner lookup on the hot path or a fixed rule derived from the owner name. The "zero storage round-trips" claim only holds without data residency.
- Do not run schema creation in the constructor; gate it on `create()` so enumeration of non-existent repos does not materialize billed SQLite storage per name (or keep a KV exists-set at the edge, as the proof suggests).
- Catch `stub.fetch` rejections and map to 502; wrap `create` in `transactionSync`; cache the imported HMAC key at module scope.
- Renames/transfers, org/team roles, SSO and instant revocation are explicitly out of scope; each adds a lookup to a path the proof advertises as lookup-free.

## Verdict
lands-with-caveats. Every primitive is GA, the crash and concurrency stories are clean because the DO is the sole authority and all writes are idempotent, and the 401/403 choreography matches what `remote-curl` and `http.c` actually do. Fix the anonymous existence leak, unify DO naming with the sibling proofs, drop the username-must-match rule, and correct the pack-rewind claim (git probes first); then this is a few days of work.
