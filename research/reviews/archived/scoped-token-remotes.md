# Review: Rate-limited, token-scoped remote URLs

> Idea #28 · edge · verdict: **lands with caveats** · feasibility 4/5 · reliability 3/5 · correctness 3/5 · effort: days
> Proof: [proofs/scoped-token-remotes.md](../../proofs/archived/scoped-token-remotes.md) · Review: [reviews/scoped-token-remotes.md](./scoped-token-remotes.md)

# Review: scoped-token-remotes (idea #28)

## Scores
- Feasibility 4/5 — every primitive is GA (WebCrypto HMAC, DO SQLite `sql.exec`, `setAlarm/getAlarm`, DO RPC incl. `ReadableStream` args, streaming request bodies, Worker secrets). Limits fine: the Worker parses a few hundred bytes then hands off. Docked one because the proof code's `rest` stream is broken (see Blockers) and Cloudflare's per-plan request-body cap (100–500 MB) bounds push size for the whole project.
- Reliability 3/5 — exact counting is real (single DO, sync SQL), but reserve-before-ingest with no compensation burns budget on failure, and the byte budget is never actually debited for chunked pushes.
- Correctness 3/5 — achieves "one ref, one hour, N pushes" at the wire level, but breaks two common client shapes (shallow CI checkouts; new-branch pushes send full history) and the early-reject path risks an HTTP/2 stream cancel.
- Effort: days (given info-refs-endpoint, streaming-pack-parser, two-phase-push exist).

## Crash walk-through
Client pushes `refs/heads/feature` with `maxPushes=1`. Worker parses commands, calls `reserve` → DO writes `pushes=1` (durable via output gate before RPC returns). Worker then calls `ingestPush`; the Worker isolate is evicted mid-PACK. Result: no ref moved, pending objects swept by two-phase-push's janitor (no orphans, no split-brain — refs live only in the DO). But the token is now spent: the client's retry gets `ng refs/heads/feature rate limit: 1 pushes per token`. Variant: DO crashes after the UPDATE but before returning — the write persisted, the RPC throws, a retrying Worker debits again. Neither loses data; both fail closed against the legitimate user. Fix is to debit inside `ingestPush`'s CAS transaction (or reserve a lease id and settle it on commit/abort), which the proof gestures at ("re-checks lease.id") but does not do.

## Concurrency walk-through
Two `git push` runs with the same token hit two Worker isolates simultaneously; both pass the HMAC and command checks; both call `reserve`. The DO serializes: no `await` between SELECT and UPDATE, so the second sees `pushes=1` and gets `ng`. Correct and exact. Concurrent tokened push vs. a normally-authed push to the same ref: both go through the repo DO's ref CAS; loser gets `ng ... fetch first`-style rejection. No split-brain. Gap: for pushes above `http.postBuffer` (chunked, no Content-Length) `bytes` is reserved as 0, and nothing shown ever writes the ingester's real count back, so `maxBytes` across pushes is only enforced per-push by the ingester, not cumulatively. Also `revoke` rows are never deleted (alarm skips `revoked=1`); unbounded but tiny.

## Interop check
- Push is v0 `receive-pack` even under `protocol.version=2` — correct; caps `report-status delete-refs` without `side-band-64k` is legal and the client will parse raw `unpack ok` / `ng` lines. The token stays in the URL across both requests; 403 (not 401) avoids the credential prompt. All good.
- BREAKS: `send-pack.c` emits `shallow <oid>` pkt-lines before the command lines whenever the pushing repo is shallow (`advertise_shallow_grafts_buf`). `readCommands` parses that as `{old:"shallow", nw:<oid>, ref:undefined}` and the scope filter answers `ng undefined token scoped to ...`. `actions/checkout` defaults to `fetch-depth: 1`, i.e. the headline CI use case for scoped tokens is exactly the client that fails. Must skip `shallow ` lines.
- BREAKS (as written): `rest`'s `pull` calls `body.getReader()` on every pull and never releases; the second pull throws "ReadableStream is locked", so any PACK larger than the first chunk dies. One reader held in a closure fixes it.
- DEGRADES: advertising only the scoped ref means the client's `send-pack` assumes the server has nothing else. First push of a new branch (zero refs advertised, or just `capabilities^{}`) ships the entire reachable history as the PACK. Advertise all refs (or `.have` lines) and reject on the command check instead.
- RISK: on out-of-scope commands the Worker responds without draining the body. Over HTTP/2 the runtime cancels the request stream (RST_STREAM); curl surfaces this as "HTTP/2 stream was not closed cleanly" and git prints `error: RPC failed; curl 92` instead of the nice `! [remote rejected]`. Real `git-receive-pack` always drains the pack before `execute_commands`; do the same (cheap: read to EOF, discard) before replying.
- Minor: `delete-refs` is advertised, so the "push one branch" token can also delete that branch; probably should reject `<new>=0{40}` unless the scope says so.

## Blockers
1. `readCommands` handoff stream is non-functional for multi-chunk bodies (locked-reader bug); proof code cannot push a real pack until fixed.
2. Shallow-clone pushes (`shallow` pkt-lines) are rejected — the primary CI audience.

## Caveats
- Reserve-then-ingest burns push/byte budget on any downstream failure; move the debit into the ref-CAS transaction.
- Byte budget is not cumulative for chunked (>1 MiB) pushes; ingester must report actual bytes back to `token_usage`.
- Scoped-ref-only advertisement inflates packs for new branches; drain body before early rejection to avoid HTTP/2 cancel.
- Token in URL leaks to config/logs (acknowledged); no `kid` for key rotation (acknowledged).
- Cloudflare request-body cap (plan-dependent, 100–500 MB) bounds any single push.

## Verdict
lands-with-caveats. The HMAC-in-path, parse-commands-first, DO-counts design is sound and cheap, and a real git client will interoperate once the two wire bugs (shallow lines, locked reader) and the drain-before-reject detail are fixed — all of which are hours of work, not a redesign.
