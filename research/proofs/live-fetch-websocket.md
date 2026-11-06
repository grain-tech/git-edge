# Live fetch over hibernating WebSockets

> Second pass · Idea #15 · verdict: **lands with caveats** · feasibility 4/5 · reliability 4/5 · correctness 4/5 (first pass 4/3/2)
> First pass: [proof](./archived/live-fetch-websocket.md) · [review](../reviews/archived/live-fetch-websocket.md) · Second pass: [review](../reviews/live-fetch-websocket.md) · Contract: [CONTRACTS.md](../../CONTRACTS.md)


## Mechanism
Section 12 lists WebSockets outside the foundation, so nothing here is subsumed: this is a post-foundation module (A9) — one file, three write-backs, one REGISTRY block at the top of the code. The edge route `GET /<owner>/<repo>/live` runs `auth::authenticate` (section 12; a read token suffices) and forwards the upgrade untouched to the `id_from_name("owner/repo")` stub. `RepoDo` accepts the socket for hibernation with tags — the subscribed ref names, or `*` when the client asks for all or for more than the 10-tag platform cap — sets a `ping`/`pong` auto-response so keepalives never wake it, and stores a ~40-byte attachment `{exp, busy_ms}`. Nothing of the first pass's `state.have` survives: haves travel inside the fetch frame as `have` lines, client tips inside a `{"t":"tips"}` control frame. When `commit_push` moves a ref, one added line in the commit arm calls `live_fanout` inside the same sync span (write-back b): `get_websockets_with_tag(name)` and `("*")`, one JSON nudge `{"t":"ref","ref","old","new","v":refs_version}` per socket, released by the output gate iff the move commits. A binary frame is one complete v2 command body run through the exact section-9 pipeline — `parse_v2_command`, `Index::lookup`, `write_fetch_prelude`, `send_set_shallow`, `write_pack` — into a `PktWriter` and chunked into 64 KiB binary messages: byte-identical to `/_do/fetch`, `acknowledgments` omitted after `done` (rule 3). Over `LIVE_BYTES_MAX` of entry bytes the reply is `{"t":"http"}` and the helper runs an ordinary `git fetch`; that notification path is also the only stock-`git` mode, unchanged from the first pass.

## Primitives
- `State::{accept_websocket_with_tags(&ws, &[&str]), get_websockets() -> Vec, get_websockets_with_tag(&str) -> Vec, set_websocket_auto_response(&pair)}`, `WebSocket::{send_with_str, send_with_bytes, close(Option<u16>, Option<S>), serialize_attachment(T), deserialize_attachment() -> Result<Option<T>>}`, `WebSocketPair::new() -> Result<{client, server}>`, `WebSocketRequestResponsePair::new(&str, &str) -> Result<_, JsValue>`, `Response::from_websocket(WebSocket)`, `DurableObject::{websocket_message, websocket_close, websocket_error}` with `WebSocketIncomingMessage::{String, Binary}`: all **verified against docs.rs worker 0.8.5** (memo section 1 lists the feature set; signatures read directly this pass). Every one is synchronous — none returns a promise — which is what lets `live_fanout` sit inside the commit span. None was exercised by the spike.
- Hibernation semantics (sockets, tags, attachments survive DO eviction; `websocket_message` wakes a hibernated DO; auto-response answers without a wake): platform feature, GA per memo section 1, **not measured** by the spike.
- Output gate: `ws.send` issued inside a sync span is released iff the span's writes commit — DO output-gate semantics per the platform docs, asserted by the first-pass review ("output gates hold the sends until the SQLite write is durable"), **not measured** in the spike.
- Platform limits (first-pass review, docs): at most 10 tags per socket, 2 KiB `serializeAttachment`, 1 MiB per message. `bufferedAmount` is **verified absent** from `worker` 0.8.5's `WebSocket` method list.
- `Stub::fetch_with_request` forwarding of an `Upgrade: websocket` request and pass-through of the DO's 101: stub forwarding verified in the spike for ordinary requests; the upgrade is the documented DO-WebSocket pattern, **unverified at runtime**.
- `wire::{parse_v2_command, write_fetch_prelude, PktWriter, Sideband::new}` (1.1, protocol-v2-only): prelude emits `acknowledgments` only when `!done` (rule 3), `Sideband::data` frames at 65515 bytes max (rule 2). `pack::generate::{send_set_shallow, write_pack}` and `SendSet::{reads, shallow, unshallow}`: sibling `partial-clone-filters`, signatures per 1.4/A1.
- `Index::lookup` (the 2.3 reader query, live packs only), `RepoDo::{sql, q, meta, bucket}` and `now_ms` as `pub(crate)` helpers, `auth::authenticate`, `RepoRoute::{name, apply_headers}`, `env.durable_object(..).id_from_name(..).get_stub()`: contract 1.2–1.4, 8 and sibling conventions.
- `jobs::{enqueue, rearm, dispatch}`, `JobKind::LiveSweep`, `SliceOutcome::{Done, Reschedule}`: section 4, A3, A4. Only `rearm` calls `set_alarm`; a second `setAlarm` cancels the first, **measured** (#5).
- `json_each(?)` for the tips name list: A6 (one bound parameter, any list length).
- `serde_json` for control frames; `gix_validate::reference::name_partial` 0.11.4 and `ObjectId::from_hex` 0.26.2 for `?refs=` and tips values — all client bytes (section 10). `js_sys::Date::now()` for `exp`, `busy_ms`.

## Proof code
```rust
// src/live/mod.rs + two dispatch lines in src/repo_do/mod.rs -- CONTRACTS.md 1.1, 1.3, 2.3, 4, 7, 9, 10, 12; A1-A9.
// worker 0.8.5, gix-validate 0.11.4, gix-hash 0.26.2, serde_json, url (via worker::Request::url). `q`, `meta`,
// `sql`, `bucket`, `now_ms` are the RepoDo helpers of repo-do-ref-authority, `pub(crate)`; `RepoHeaders` and
// `respond` live in wire::http (A8); `CommitRequest`/`CommitResponse` are the repo_do DTOs.
//
// REGISTRY (A9) — everything this module adds:
//   routes:    edge GET /<owner>/<repo>/live -> live_route; DO GET .../live, matched before req.bytes().
//   job kinds: JobKind::LiveSweep (arm at the bottom; self-Reschedule while sockets exist).
//   tables:    none.  R2 key prefixes: none.  wire changes: none.
//   write-backs: (a) the /live arm + rearm in fetch; (b) the live_fanout call in the commit arm;
//   (c) JobKind::LiveSweep; (d) write_fetch_prelude(.., acks, shallow, unshallow) gains `unshallow` lines
//   (protocol-v2-only); (e) impl DurableObject gains websocket_{message,close,error}; (f) RepoDo helpers pub(crate).
use std::collections::{BTreeMap, HashMap};
use bstr::ByteSlice;
use gix_hash::ObjectId;
use serde::Deserialize;
use worker::{durable::WebSocketIncomingMessage as Msg, Env, Method, Request, Response, SqlStorageValue as V,
             WebSocket, WebSocketPair, WebSocketRequestResponsePair};
use crate::{auth, error::Error, jobs::{self, Job, JobKind, SliceBudget, SliceOutcome}, pack::generate,
            repo_do::{CommitRequest, CommitResponse, RepoDo}, store::Index,
            wire::{self, http::RepoHeaders, FetchArgs, PktWriter, Sideband, V2Command}, ReqBudget};

const MAX_TAGS: usize = 10;             // platform: at most 10 tags per hibernated socket
const LIVE_BYTES_MAX: u64 = 8 << 20;    // in-socket reply cap on entry bytes; over it -> {"t":"http"}
const WS_MSG: usize = 64 << 10;         // binary message size, well under the 1 MiB message cap
const SESSION_MS: i64 = 86_400_000;     // a socket is closed by its next message or the next sweep after this
const BUSY_STALE_MS: i64 = 300_000;     // a busy mark this old means the handler died mid-fetch
const SWEEP_MS: i64 = 900_000;          // LiveSweep period, the Janitor's 15-minute cadence (section 5)
const MAX_TIPS: usize = 64;

/// All per-socket state, ~40 bytes of the 2 KiB cap. No have-set is persisted (blocker 3): haves are `have`
/// lines inside each fetch frame, and tips are claimed by the client, never stored (blocker 1).
#[derive(serde::Serialize, serde::Deserialize, Default)]
pub(crate) struct Attach { exp: i64, busy_ms: i64 }

fn nudge(name: &str, old: Option<&str>, new: Option<&str>, v: &str) -> String {
    serde_json::json!({"t":"ref","ref":name,"old":old,"new":new,"v":v}).to_string()   // null = "not present"
}
fn non_zero(s: &str) -> Option<&str> { (s.len() == 40 && s.bytes().any(|b| b != b'0')).then_some(s) }
fn err(e: &Error) -> String { serde_json::json!({"t":"err","msg":e.to_string()}).to_string() }

/// edge: authenticate, then forward the upgrade untouched; the Worker never terminates the socket.
pub async fn live_route(mut req: Request, env: &Env, repo: &crate::edge::RepoRoute) -> Result<Response, Error> {
    auth::authenticate(&req, env)?;                                    // section 12: either token may subscribe
    if !req.headers().get("Upgrade").ok().flatten().is_some_and(|v| v.eq_ignore_ascii_case("websocket")) {
        return Err(Error::Protocol("expected websocket upgrade".into()));
    }
    repo.apply_headers(&mut req)?;                                     // x-ge-owner / x-ge-repo (8.1)
    let stub = env.durable_object("REPO").map_err(|e| Error::Internal(e.to_string()))?
        .id_from_name(&repo.name()).map_err(|e| Error::Internal(e.to_string()))?
        .get_stub().map_err(|e| Error::Internal(e.to_string()))?;
    stub.fetch_with_request(req).await.map_err(|e| Error::Storage(e.to_string()))  // the DO's 101 passes through
}

// ---- RepoDo::fetch, write-backs (a), (b) ----
//   between `let hdr = RepoHeaders::from_request(&req);` and `req.bytes().await`:
//     if req.method() == Method::Get && req.path().ends_with("/live") {
//         let out = self.boot(&hdr).and_then(|_| self.live_upgrade(&req));
//         let _ = jobs::rearm(self).await;                                  // A3: arm the enqueue after the span
//         return wire::http::respond(out);
//     }
//   and the commit arm becomes: `self.commit_push(&r).map(|res| { self.live_fanout(&r, &res); res }).and_then(json)`

impl RepoDo {
    /// GET .../live: one sync span like every non-fetch route (1.3). Accept for hibernation; tags are fan-out.
    pub(crate) fn live_upgrade(&self, req: &Request) -> Result<Response, Error> {
        if let Ok(p) = WebSocketRequestResponsePair::new("ping", "pong") {   // keepalives answered while hibernated
            self.state.set_websocket_auto_response(&p);
        }
        let url = req.url().map_err(|e| Error::Internal(e.to_string()))?;
        let refs: Vec<String> = url.query_pairs().find(|(k, _)| k.as_ref() == "refs")
            .map(|(_, v)| v.split(',').map(str::to_string).collect()).unwrap_or_default();
        let wild = refs.is_empty() || refs.iter().any(|r| r == "*");
        if !wild { for r in &refs {                                          // client bytes: validate before tagging
            if gix_validate::reference::name_partial(r.as_bytes().as_bstr()).is_err() {
                return Err(Error::Protocol(format!("bad ref name {r}")));
            } } }
        let tags: Vec<&str> = if wild || refs.len() > MAX_TAGS { vec!["*"] } else { refs.iter().map(String::as_str).collect() };
        let pair = WebSocketPair::new().map_err(|e| Error::Internal(e.to_string()))?;
        self.state.accept_websocket_with_tags(&pair.server, &tags);          // hibernation accept (memo section 1)
        pair.server.serialize_attachment(Attach { exp: now_ms().saturating_add(SESSION_MS), busy_ms: 0 })
            .map_err(|e| Error::Storage(e.to_string()))?;
        jobs::enqueue(&self.sql(), JobKind::LiveSweep, now_ms().saturating_add(SWEEP_MS), "{}")?;  // dedups (4.5)
        Response::from_websocket(pair.client).map_err(|e| Error::Internal(e.to_string()))
    }

    /// Write-back (b): inside the /_do/push/commit sync span. Both calls are synchronous, so the frames queue in
    /// the span's output gate and are released iff the ref moves commit (A2) — no await and no return between the
    /// CAS and this loop. Best effort by contract: a send failure is dropped; tips is the catch-up.
    pub(crate) fn live_fanout(&self, req: &CommitRequest, res: &CommitResponse) {
        let Ok(v) = self.meta("refs_version") else { return };               // already bumped (section 3 step 5)
        for (cmd, (_, ng)) in req.commands.iter().zip(res.results.iter()) {
            if ng.is_some() { continue; }
            let msg = nudge(&cmd.name, non_zero(&cmd.old), non_zero(&cmd.new), &v);
            for tag in [cmd.name.as_str(), "*"] {                            // a socket has ref tags or "*", never both
                for ws in self.state.get_websockets_with_tag(tag) { let _ = ws.send_with_str(&msg); }
            }
        }
    }

    /// {"t":"tips","tips":{ref:oid|null}}: one sync span; diff the claim against refs, nudge every mismatch —
    /// moved, created and deleted alike (null on either side). The reconnect catch-up (blocker 1).
    fn live_tips(&self, ws: &WebSocket, s: &str) -> Result<(), Error> {
        #[derive(Deserialize)] struct Tips { t: String, tips: BTreeMap<String, Option<String>> }
        #[derive(Deserialize)] struct Row { name: String, target: String }
        let m: Tips = serde_json::from_str(s).map_err(|e| Error::Protocol(e.to_string()))?;
        if m.t != "tips" || m.tips.len() > MAX_TIPS { return Err(Error::Protocol("bad tips frame".into())); }
        for (n, tip) in &m.tips {
            if gix_validate::reference::name_partial(n.as_bytes().as_bstr()).is_err() {
                return Err(Error::Protocol(format!("bad ref name {n}")));
            }
            if let Some(t) = tip { ObjectId::from_hex(t.as_bytes()).map_err(|e| Error::Protocol(e.to_string()))?; }
        }
        let names: Vec<&String> = m.tips.keys().collect();
        let j = serde_json::to_string(&names).map_err(|e| Error::Internal(e.to_string()))?;
        let cur: HashMap<String, String> = self.q(
            "SELECT name, target FROM refs WHERE name IN (SELECT value FROM json_each(?))",   // A6: one parameter
            vec![V::from(j.as_str())])?.to_array::<Row>()?.into_iter().map(|r| (r.name, r.target)).collect();
        let v = self.meta("refs_version")?;
        for (name, claimed) in &m.tips {
            let target = cur.get(name);
            if claimed.as_deref() != target.map(String::as_str) {              // null on both sides = in sync
                ws.send_with_str(nudge(name, claimed.as_deref(), target.map(String::as_str), &v))
                    .map_err(|e| Error::Storage(e.to_string()))?;
            }
        }
        Ok(())
    }

    /// A binary frame is one complete v2 command body — the same bytes the edge POSTs to /_do/fetch. The reply is
    /// the section-9 byte stream in WS_MSG-sized binary messages. Text frames carry only JSON control, so a nudge
    /// during a streamed pack lands between binary messages, never inside pack bytes (review, concurrency).
    async fn live_fetch(&self, ws: &WebSocket, att: &mut Attach, body: &[u8], now: i64) -> Result<(), Error> {
        if att.busy_ms.saturating_add(BUSY_STALE_MS) > now {
            return Err(Error::Protocol("one fetch at a time".into()));         // one reply in flight per socket
        }
        let args = match wire::parse_v2_command(body)? {                       // 1.1; the 1 MiB cap bounds the frame
            V2Command::Fetch(a) => a,
            V2Command::LsRefs(_) => return Err(Error::Protocol("live: fetch command only".into())),
        };
        att.busy_ms = now;
        ws.serialize_attachment(&*att).map_err(|e| Error::Storage(e.to_string()))?;
        let out = self.live_fetch_bytes(&args).await;
        att.busy_ms = 0;
        let _ = ws.serialize_attachment(&*att);
        match out? {
            None => ws.send_with_str("{\"t\":\"http\"}").map_err(|e| Error::Storage(e.to_string()))?,
            Some(b) => for c in b.chunks(WS_MSG) { ws.send_with_bytes(c).map_err(|e| Error::Storage(e.to_string()))?; },
        }
        Ok(())
    }

    /// Section 9 verbatim except the drain: lookups (steps 1-2), send_set_shallow (3-5), prelude + write_pack (6)
    /// into one PktWriter. Ok(None) = over the byte cap; the caller answers {"t":"http"}.
    async fn live_fetch_bytes(&self, args: &FetchArgs) -> Result<Option<Vec<u8>>, Error> {
        let sql = self.sql(); let idx = Index(&sql);
        let locs = idx.lookup(&args.wants)?;                                   // step 1, live packs only (2.3)
        if let Some(want) = args.wants.iter().zip(&locs).find_map(|(w, l)| l.is_none().then_some(*w)) {
            let mut w = PktWriter { out: Vec::new() };
            w.text(&format!("ERR upload-pack: not our ref {want}"))?;          // the section-10 ERR line is the reply
            w.flush();
            return Ok(Some(w.out));
        }
        let acks: Vec<ObjectId> = args.haves.iter().zip(idx.lookup(&args.haves)?)
            .filter_map(|(h, l)| l.is_some().then_some(*h)).collect();         // unknown haves dropped (9.1)
        let mut w = PktWriter { out: Vec::new() };
        if !(args.done || args.haves.is_empty() || !acks.is_empty()) {         // 9.2: a NAK round ends the response
            wire::write_fetch_prelude(&mut w, args, &acks, &[], &[])?;         // acknowledgments + NAK + flush
            return Ok(Some(w.out));
        }
        let bucket = self.bucket()?;
        let mut budget = ReqBudget::paid();                                    // 7.1: 9,000 subrequests / 240 s
        let set = generate::send_set_shallow(self, &bucket, &args.wants, &args.haves,
                                             args.filter.as_ref(), args.deepen, &args.shallow, &mut budget).await?;
        let bytes: u64 = set.reads.iter().flat_map(|r| r.ents.iter()).map(|(_, l)| u64::from(*l)).sum();
        if bytes > LIVE_BYTES_MAX { return Ok(None); }                         // exact: entries are copied verbatim (2.1)
        wire::write_fetch_prelude(&mut w, args, &acks, &set.shallow, &set.unshallow)?;   // rule 3, step 6 (write-back d)
        { let mut sb = Sideband::new(&mut w); generate::write_pack(&bucket, &set, &mut sb, &mut budget).await?; }
        w.flush();                                                             // rule 3: the stream ends with flush
        Ok(Some(w.out))
    }

    pub(crate) async fn live_message(&self, ws: &WebSocket, msg: Msg) -> Result<(), Error> {
        let mut att = ws.deserialize_attachment::<Attach>().map_err(|e| Error::Internal(e.to_string()))?
            .ok_or_else(|| Error::Protocol("no attachment".into()))?;
        if att.exp <= now_ms() { let _ = ws.close(Some(1000), Some("session expired")); return Ok(()); }
        match msg {
            Msg::String(s) => self.live_tips(ws, &s),                          // text frames are JSON control, always
            Msg::Binary(b) => self.live_fetch(ws, &mut att, &b, now_ms()).await,
        }
    }
}

// ---- impl DurableObject for RepoDo, write-back (e): the trait's provided hooks (worker 0.8.5, docs.rs) ----
// async fn websocket_message(&self, ws: WebSocket, msg: WebSocketIncomingMessage) -> worker::Result<()> {
//     self.boot(&RepoHeaders::NONE).map_err(worker::Error::from)?;        // a hibernated wake re-boots (8.2)
//     match self.live_message(&ws, msg).await {
//         Ok(()) => Ok(()),
//         Err(e) => { let _ = ws.send_with_str(err(&e)); Ok(()) }         // every Error is {"t":"err"}; socket stays
//     } }
// async fn websocket_close(&self, ws: WebSocket, _c: usize, _r: String, _clean: bool) -> worker::Result<()> {
//     let _ = ws.close(None, None::<&str>); Ok(()) }
// async fn websocket_error(&self, ws: WebSocket, _e: worker::Error) -> worker::Result<()> {
//     let _ = ws.close(Some(1011), None::<&str>); Ok(()) }

// ---- src/jobs/live_sweep.rs: the run_slice arm for JobKind::LiveSweep (c). Never calls set_alarm (4.1). ----
/// Closes expired sessions and sockets with unreadable attachments; the runtime owns the list, so one firing is
/// one sync scan. Done when none remain — the next live_upgrade re-enqueues the kind (4.5 dedup).
pub async fn run_live_sweep(d: &RepoDo, _job: &Job, _b: &mut SliceBudget) -> Result<SliceOutcome, Error> {
    let (now, mut any) = (now_ms(), false);
    for ws in d.state.get_websockets() {
        any = true;
        match ws.deserialize_attachment::<Attach>() {
            Ok(Some(a)) if a.exp > now => {}
            _ => { let _ = ws.close(Some(1000), Some("session expired")); }
        }
    }
    Ok(if any { SliceOutcome::Reschedule { run_at: now.saturating_add(SWEEP_MS) } } else { SliceOutcome::Done })
}
```

## Why it works
- **A nudge is released iff the ref move committed.** `get_websockets_with_tag` and `send_with_str` are synchronous (verified signatures), so `live_fanout` sits inside the `/_do/push/commit` sync span after step 5's `refs_version` bump. Output gates hold the queued text frames until the span's writes commit; an `Err` propagated per A2 rolls back the writes *and* the sends. The first-pass crash window — commit durable, eviction before the send loop — cannot exist: there is no await and no return between the CAS and the fanout. `live_fanout` returns `()` and swallows per-socket failures, so a dead socket can never veto a commit. Nudges stay at-most-once, which is now safe rather than a hole because the tips frame is the catch-up.
- **Reconnect heals every missed move (blocker 1).** The helper sends `{"t":"tips"}` after every connect; `live_tips` diffs the claim against `refs` in one sync span (`json_each`, A6) and nudges each mismatch with `old` = the client's claim and `new` = the current target or null for a deleted ref. The server never stores or trusts its own idea of the client's state — the attachment carries no tips at all.
- **The bytes on the wire are `/_do/fetch`'s bytes (blocker 2).** The socket path is the section-9 pipeline with the same call sequence and the same `wire` functions: `Index::lookup` for wants (an unknown want answers with the section-10 `ERR` pkt-line, which `fetch-pack` reports), dropped unknown haves (9.1), readiness per 9.2, `write_fetch_prelude` — `acknowledgments` only when `!done`, `shallow-info`, `packfile` — `send_set_shallow` for 9.3–9.5, `write_pack` copying verbatim full-object entries (2.1), flush to end (rule 3). Only the drain differs: `w.out` chunked into 64 KiB `send_with_bytes` calls instead of `Response::from_stream`. A helper concatenates the binary messages and pipes them to `git fetch-pack --stateless-rpc` or `index-pack`; a NAK round is itself a legal response, so multi-round negotiation works too.
- **Text and binary can never interleave corruptly (review concurrency).** The server writes pkt bytes exclusively as binary messages and control exclusively as text; a nudge delivered mid-pack lands between binary messages, and WebSocket ordering keeps the binary stream intact. `busy_ms` serializes fetches per socket: a second fetch frame while one is in flight gets `{"t":"err","one fetch at a time"}`, and a mark older than 5 minutes is treated as a handler death, not a wedge.
- **Backpressure by construction.** The reply size is the exact sum of marked entry lengths (`set.reads.ents`, exact because entries are copied verbatim) checked before `write_pack` runs; above 8 MiB the answer is `{"t":"http"}`. At most one response is in flight per socket and every send happens after generation completes, so a mid-generation `Error::Budget` (A1, every R2 call charged through `send_set_shallow`/`write_pack`) discards the whole buffer — nothing partial reaches the wire, unlike the HTTP path's mid-stream band-3. Buffered bytes per socket ≈ 8.2 MiB plus queued nudges.
- **Tags close the >10-refs caveat.** A `?refs=` list of at most 10 validated names becomes that many tags; `*`, absent `refs`, or a longer list becomes the single `*` tag; fanout queries `get_websockets_with_tag(name)` and `("*")`. A socket has one kind or the other by construction, so each nudge is sent exactly once. `*` is never a ref name (it fails `name_partial`), so no collision is possible.
- **Cost while idle.** `set_websocket_auto_response` answers `ping` without a wake; there are no new tables and no R2 keys — the runtime owns sockets, tags and the ~40-byte attachments, so a hibernated subscriber costs zero DO work. `LiveSweep` is the only recurring cost and reschedules itself only while sockets exist (`Done` otherwise; the next `live_upgrade` re-enqueues it, dedup 4.5).
- **Memory and budget.** Peak ≈ 17 MiB during `live_fetch_bytes` (reply buffer ≤ ~8.2 MiB + one 8 MiB read window inside `write_pack`) on top of `send_set_shallow`'s own bounds (64 MiB `MemFind`, 200,000 commits — 9.3). Session expiry: `exp` is checked lazily on every message and by `LiveSweep` — under the foundation's static tokens (section 12) it is a hard 24 h bound, and a deployment with expiring credentials just shortens it.
- **Foundation rules kept.** Budget-carrying signatures (A1); `enqueue` inside the sync upgrade span, `rearm` awaited after it (A3); `json_each` for the IN list (A6); no `set_alarm` outside `jobs::rearm` (4.1); errors inside a DO span propagate as `Err` (A2) except `live_fanout`, which is deliberately infallible; client bytes validated with `gix_validate`/`ObjectId::from_hex`, no `unwrap`/indexing on them (section 10).

## Changes from the first pass
| First-pass finding (quoted) | Kind | How addressed |
|---|---|---|
| "Reconnect seeds `have` from server tips instead of client-reported tips: missed moves are never caught up; must accept client tips in the handshake and nudge on mismatch." | blocker | The client claims its tips in a `{"t":"tips"}` control frame on every (re)connect; `live_tips` diffs them against `refs` in one sync span and nudges every mismatch — moved, created, and deleted (`new: null`) refs alike. The attachment stores no tips, so the server can never again believe the client is current when it is not. |
| "v2 response framing wrong for `done` requests (extra `acknowledgments` section) -> real `fetch-pack` dies." | blocker | The response is produced by `wire::write_fetch_prelude` + `pack::generate::write_pack`, the identical code path as `/_do/fetch`: `acknowledgments` is written only when `!args.done` (rule 3; protocol-v2-only's first-pass blocker was the same fix). Bytes are identical by construction; only the 64 KiB binary-message drain differs. |
| "Attachment growth (`state.have[w] = w`) breaks the 2 KiB `serializeAttachment` cap after a few dozen fetches." | blocker | `Attach { exp, busy_ms }` is ~40 bytes, constant-size. Haves travel as `have` lines inside each v2 fetch body (unknown haves dropped per 9.1); nothing accumulates across fetches. |
| Crash walk-through: "DO is evicted between the commit and the loop: the ref is B, nudges are lost ... They reconnect later; `fetch()` seeds their attachment with `currentTips(refs)` = B ... The client is stuck on A" | blocker-class | Both halves closed. The nudge loop moved inside the commit sync span (output gate releases sends iff the writes commit — no eviction can land between). And reconnect seeds nothing: the client reports its own tips, so a missed nudge is re-derived on the next connect. |
| Concurrency: "The text frame lands in the middle of S's binary pack frames ... the proof's own framing rule is violated by the server" | caveat | By construction: pkt bytes are only ever `send_with_bytes`, control only `send_with_str`; an interleaved nudge lands between binary messages, not inside pack bytes. `busy_ms` additionally serializes fetch replies per socket. |
| Interop: "sideband pkt payloads must be <= 65515 bytes and the whole pkt-line <= 1 MiB WebSocket frame; `sideband()` is only declared" | caveat | `Sideband::data` owns the 65515-byte frame cap (rule 2, contract-owned). The ws messages are 64 KiB byte chunks of the same stream — chunk boundaries are invisible to the pkt-line parser the helper feeds. |
| ">10 subscribed refs cannot be expressed as tags; needs a namespace/tag scheme or subscribe-all." | caveat | `?refs=` of ≤ 10 validated names → per-ref tags; `*`, absent, or > 10 → the single `*` tag; `live_fanout` queries both. `*` cannot collide with a ref name (`name_partial` rejects it). |
| "No send backpressure: a slow client on a large pack buffers into DO memory; big fetches must be forced to HTTP." | caveat | `LIVE_BYTES_MAX` (8 MiB of exact entry bytes) gates before `write_pack`; over it → `{"t":"http"}` and the helper fetches over smart HTTP. One reply in flight per socket; sends happen only after generation completes. `bufferedAmount` is verified absent from `worker` 0.8.5, so the cap is the flow control. |
| "Code deploys and DO restarts drop hibernated sockets; clients must reconnect with tip comparison (same fix as blocker 1)." | caveat | Addressed by the same mechanism: on reconnect the helper sends `tips` and the diff produces the missed nudges. Stated as the client contract in Known limits. |
| "Auth expiry via alarm walking `getWebSockets()` is unimplemented; constructor `sql.exec` should sit in `blockConcurrencyWhile`." | caveat | `JobKind::LiveSweep` (REGISTRY) scans `get_websockets()` every 15 min and closes sockets past `exp` or with unreadable attachments; `live_message` refuses expired sessions lazily. No constructor exec exists: schema lives in `boot` (8.2), run inside each handler's span, so `block_concurrency_while` is not needed. |
| "Busy repos never hibernate, so the cost argument only holds for quiet repos." | caveat | Not addressed — it is a platform fact. Restated in Known limits; the zero-duty-cycle claim applies to quiet repos. |
| "Proof code sends pkt-line strings as text frames, contradicting its own text=control convention." | caveat | All protocol bytes go through `send_with_bytes`; text frames carry only JSON (`ref`, `err`, `http`, and client-side `tips`). |
| First-pass limit: "Stock `git` cannot speak this: it needs a remote helper (git-remote-ws) or a sidecar that runs `git fetch` on nudge" | limit | Unchanged — no server-side change can teach git WebSockets. The nudge-plus-HTTP-fetch path remains the stock-compatible mode; the in-socket pack path is helper-only and now byte-exact. |
| First-pass limit: "`ws.send` is fire-and-forget ... Need per-socket flow control" | limit | The 8 MiB cap plus one-in-flight-per-socket is the flow control: a slow client can hold at most ~8.2 MiB of buffered frames. No `bufferedAmount` exists in 0.8.5 to do better. |
| First-pass limit: "Single DO per repo: fan-out to N sockets is O(N) sends on the push path" | limit | Unchanged, now cheaper to reason about: the O(N) sync sends ride inside the already-atomic commit span. Fine for hundreds of subscribers; tens of thousands is `branch-level-dos` territory (section 12). |
| First-pass limit: "Delta compression is skipped ... packs are larger" | limit | No longer a deviation: section 2.1 makes every object at rest a full-object entry, so the socket stream is byte-identical to what `/_do/fetch` produces for any client. |
| First-pass limit: "No cross-region replication of the socket" | limit | Unchanged — the contract fixes one DO per repo as the ref authority (section 8); the socket must terminate there. |

## Known limits
- **A helper is still required.** Stock `git` has no WebSocket transport and this proof adds none: the socket is a helper channel. The fully compatible mode is nudge → `{"t":"tips"}` → `git fetch` over smart HTTP; the in-socket pack path exists for helpers that want one round-trip and is byte-exact for `fetch-pack`/`index-pack`.
- **In-socket fetches are capped.** `LIVE_BYTES_MAX` counts marked entry bytes exactly but is checked after `send_set_shallow`, so a refused fetch still paid its negotiation reads (all charged, 7.1/A1). Helpers should treat the socket as the incremental path and HTTP as the bulk path.
- **Client contract.** The helper must send `tips` after every (re)connect and must treat `old`/`new` as opaque — including `new: null` for deletions and reordered or duplicated nudges around a reconnect. Deploys and DO restarts drop hibernated sockets; reconnect-and-tips is the only recovery.
- **Fan-out and hibernation.** N sends inside the commit span per moved ref — hundreds of subscribers are fine; a repo under continuous push never hibernates, collapsing the cost model to an always-on DO (unchanged caveat).
- **Unverified, day-1 list.** Upgrade + 101 pass-through on `Stub::fetch_with_request`; output-gate release of ws sends on span commit; whether `serialize_attachment` inside a span is also output-gated; the platform caps the review cites (10 tags, 2 KiB attachment, 1 MiB message, concurrent sockets per DO); `Request::url().query_pairs()` shape; `send_set_shallow`'s export (partial-clone-filters); `Sideband::new` and `jobs::rearm(self)` argument shape.
- **Write-backs this proof needs.** (a) The `/live` arm before `req.bytes()` plus `jobs::rearm` after the span; (b) one `live_fanout` call in the `/_do/push/commit` arm; (c) `JobKind::LiveSweep` and its `run_slice` arm (4.5); (d) `write_fetch_prelude` gains an `unshallow` parameter emitted inside `shallow-info` (protocol-v2-only already owns it); (e) `impl DurableObject for RepoDo` gains the three `websocket_*` hooks; (f) `RepoDo::{sql, q, meta, bucket}` and `now_ms` as `pub(crate)` — the same visibility every post-foundation sibling assumes.
- **Scenarios (section 11).** Stock `git` cannot exercise this; the harness step is a small ws client (node inside workerd's test runner, or a `tsc` helper). Must still pass 2, 6, 12, 14 (push and GC paths are touched only by the fanout line). Added scenario 19: connect with `?refs=refs/heads/main`, send `tips {main: A}`, push A→B over HTTP — assert a `{"t":"ref",...,"new":"B"}` text frame; disconnect, force-push B→C, reconnect, send `tips {main: B}` — assert a nudge `{old:"B",new:"C"}` arrives before any other frame. Added scenario 20: send one binary frame carrying a v2 `fetch` body with `want C`, `have B`, `done`; concatenate the binary replies and feed them to `git index-pack --stdin` inside a temp repo — assert it parses, and assert the first pkt-line is `packfile`, not `acknowledgments` (the blocker-2 regression check); then repeat with a want whose oid is fabricated and assert the reply is a single `ERR upload-pack: not our ref` pkt.

## Depends on
- repo-do-ref-authority
- two-phase-push
- want-have-negotiation
- protocol-v2-only
- refs-sqlite-objects-r2
- info-refs-endpoint
