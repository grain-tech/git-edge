# Review: Live fetch over hibernating WebSockets

> Idea #15 · edge · verdict: **lands with caveats** · feasibility 4/5 · reliability 3/5 · correctness 2/5 · effort: weeks
> Proof: [proofs/live-fetch-websocket.md](../../proofs/archived/live-fetch-websocket.md) · Review: [reviews/live-fetch-websocket.md](./live-fetch-websocket.md)

# Review: live-fetch-websocket (#15, edge)

## Scores
- Feasibility: 4/5. Every primitive is GA: DO SQLite, `acceptWebSocket`/`getWebSockets(tag)`/`serializeAttachment`, `setWebSocketAutoResponse`, alarms, R2 get. Forwarding the Upgrade from Worker to DO stub works. Limits the proof ignores: max 10 tags per socket (`?refs=` list >10 throws), 2 KiB attachment cap, 1 MiB per WebSocket message, no `bufferedAmount`/backpressure on `ws.send`, 128 MB DO memory for buffered pack frames.
- Reliability: 3/5. Refs never split-brain (single DO, synchronous CAS in SQL). Nudges are at-most-once and the reconnect path silently assumes the client is current, so a ref move during a disconnect is never delivered.
- Correctness: 2/5. The notification channel is real; the "byte-identical v2 fetch over the socket" claim is wrong on the wire (see Interop) and the negotiation stub (`objectsBetween` returns `wants`) means no real pack construction is proven.
- Effort: weeks (notification-only path: days; in-socket pack path with flow control and a remote helper: weeks, on top of want-have-negotiation and two-phase-push landing first).

## Crash walk-through
Push P advances `refs/heads/main` A->B. `updateRef` runs the SQL `INSERT OR REPLACE`, then loops `ws.send(nudge)`. DO output gates hold the sends until the SQLite write is durable, so no subscriber ever sees a nudge for a ref that did not commit (good). DO is evicted between the commit and the loop: the ref is B, nudges are lost, subscribers keep waiting. They reconnect later; `fetch()` seeds their attachment with `currentTips(refs)` = B, i.e. the server now believes the client already has B and sends nothing. The client is stuck on A until some other push happens. No data loss, but the stated goal ("nudged the instant a ref moves") fails under a single eviction. Crash mid-`packStream`: the client gets a truncated pack with no SHA-1 trailer, `index-pack` rejects it, the attachment was not yet rewritten, so a retry resends the same objects. Benign.

## Concurrency walk-through
Two pushes P1 (A->B) and P2 (A->C) on `main`. Both parse packs with `await`s on R2, so they interleave inside the single isolate, but `updateRef` is synchronous SQL: P1 CASes A->B, P2's CAS reads B != A and throws -> `ng ... fetch first`. Correct, no split-brain. Now subscriber S receives P1's nudge and sends a v2 fetch frame. While `webSocketMessage` is `for await`-streaming sideband frames from R2, P3 (B->D) lands and `updateRef` calls `ws.send(text nudge)` on the same socket from a different event. The text frame lands in the middle of S's binary pack frames. Recoverable only because the proof declares text=control/binary=pack, but `pkt("acknowledgments\n")`, `"0001"` and `"0000"` are sent as strings, i.e. text frames, so the helper's own framing rule is violated by the server. Also `state.have[w] = w` appends every fetched want forever; after ~25 fetches the attachment exceeds 2 KiB and `serializeAttachment` throws, killing the negotiation state.

## Interop check
Stock `git` never touches the socket; the only real client is a custom helper, and the proof concedes this. For the in-socket pack the response is not what `git fetch-pack` accepts: the request carries `done`, and for a `done` request `upload-pack` omits the acknowledgments section entirely (`process_haves_and_send_acks`: `if (data->done) ret = 1` with no `acknowledgments` written). `fetch-pack.c` mirrors this: after `send_fetch_request` returns 1 it enters `FETCH_GET_PACK` and calls `process_section_header(&reader, "packfile", 0)`, which dies with `expected 'packfile', received 'acknowledgments'` on the proof's `acknowledgments\nNAK\n0001packfile\n` prefix. So a helper that pipes these bytes into `git fetch-pack --stateless-rpc` (the obvious implementation) fails on the first frame. Secondary: sideband pkt payloads must be <= 65515 bytes and the whole pkt-line <= 1 MiB WebSocket frame; `sideband()` is only declared. Delta-less packs are valid but bloated.

## Blockers
- Reconnect seeds `have` from server tips instead of client-reported tips: missed moves are never caught up; must accept client tips in the handshake and nudge on mismatch.
- v2 response framing wrong for `done` requests (extra `acknowledgments` section) -> real `fetch-pack` dies.
- Attachment growth (`state.have[w] = w`) breaks the 2 KiB `serializeAttachment` cap after a few dozen fetches.

## Caveats
- `>10` subscribed refs cannot be expressed as tags; needs a namespace/tag scheme or subscribe-all.
- No send backpressure: a slow client on a large pack buffers into DO memory; big fetches must be forced to HTTP.
- Code deploys and DO restarts drop hibernated sockets; clients must reconnect with tip comparison (same fix as blocker 1).
- Auth expiry via alarm walking `getWebSockets()` is unimplemented; constructor `sql.exec` should sit in `blockConcurrencyWhile`.
- Busy repos never hibernate, so the cost argument only holds for quiet repos.
- Proof code sends pkt-line strings as text frames, contradicting its own text=control convention.

## Verdict
lands-with-caveats, but only as a notification channel that triggers a normal HTTP `git fetch`. The in-socket packfile transport as written does not interoperate with git's own fetch-pack and needs the three blockers fixed plus flow control before it is more than a demo.
