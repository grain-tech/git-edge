# Rate-limited, token-scoped remote URLs

> Second pass · Idea #28 · verdict: **lands with caveats** · feasibility 4/5 · reliability 4/5 · correctness 4/5 (first pass 4/3/3)
> First pass: [proof](./archived/scoped-token-remotes.md) · [review](../reviews/archived/scoped-token-remotes.md) · Second pass: [review](../reviews/scoped-token-remotes.md) · Contract: [CONTRACTS.md](../../CONTRACTS.md)


## Mechanism
Under the contract this is a second credential kind in `auth` plus three small additions to the push path. The foundation's two-token Basic auth (section 12) is untouched and still guards every non-`/t/` route. The remote URL is `https://<zone>/t/<payload>.<hmac>/<owner>/<repo>[.git]/...`, where `<payload>` is base64url JSON `{id, repo, ref, exp, maxPushes, maxBytes, del}` and `<hmac>` is base64url HMAC-SHA256 over it under `GE_TOKEN_HMAC_KEY` (`_PREV` accepted for one rotation window). `edge::route` strips the `/t/` prefix and calls `auth::verify_token` — a sync `fn` like `authenticate` (section 1.4) — before `RepoRoute::parse`; every failure is `Forbidden` (403, never 401: a challenge would make git prompt for a credential that cannot exist), so no Durable Object wakes and no repo-existence signal leaks. Token URLs serve receive-pack only: `info/refs?service=git-receive-pack` gets the ordinary rule-5 advertisement in full — all refs, the review's fix for first-push pack inflation — and `POST git-receive-pack` runs `wire::parse_receive_header` over `BodyReader` (sections 1.1, 6), which already consumes `shallow <oid>` lines, so the scope check sees only `RefCommand`s. When no command is in scope the body is drained to EOF before the `ng` report, because HTTP/2 cancels an early response. Otherwise the request is the sibling's two-phase push with a `TokenDto` (wire::http, A8) riding on `BeginDto` (advisory pre-check) and `CommitRequest` (authoritative). Inside `commit_push`'s single sync span (section 3), `token_gate` lazily creates the `token_usage` row and denies on `revoked`, `pushes >= max_pushes` or `bytes + packs.bytes > max_bytes`; `apply_one` returns `ng` for commands outside `ref` and for deletes when `del` is false; and only when at least one ref moved does the same span run `UPDATE token_usage SET pushes = pushes + 1, bytes = bytes + <packs.bytes>`. Because the debit sits inside the CAS span, a crash anywhere in ingest costs the token nothing, while the counters stay exact — one DO, no await inside the span (platform-facts #4). Expiry is one `DELETE` in the Janitor slice: this module registers no `JobKind` and never calls `set_alarm` (4.1; a second `setAlarm` cancels the first, platform-facts #5). Semantic changes vs the first pass, stated not silently: `maxPushes` now counts pushes that moved a ref (was: presented pushes, debited up front), and `maxBytes` is metered on the normalized `packs.bytes` (was: `Content-Length`, which chunked pushes lack). The scope promise itself is unchanged.

```
REGISTRY (A9) — everything this module adds:
  routes   GET  /t/<token>/<owner>/<repo>[.git]/info/refs?service=git-receive-pack
           POST /t/<token>/<owner>/<repo>[.git]/git-receive-pack      (upload-pack: Forbidden; a push token does not read)
           POST /_do/token/revoke                                    (internal; the admin edge route is auth-and-multitenancy's)
  table    token_usage(id TEXT PRIMARY KEY, exp_ms INTEGER NOT NULL, pushes INTEGER NOT NULL DEFAULT 0,
                       bytes INTEGER NOT NULL DEFAULT 0, revoked INTEGER NOT NULL DEFAULT 0)
  secrets  GE_TOKEN_HMAC_KEY, GE_TOKEN_HMAC_KEY_PREV
  DTOs     wire::http::TokenDto; BeginDto.token; CommitRequest.token  (A8)
  JobKind  none — expiry folds into the Janitor slice (4.1)
  R2 keys  none
```

## Primitives
- `hmac` 0.12 + `sha2` 0.10 (`Hmac<Sha256>`, `Mac::{new_from_slice, update, verify_slice}`): pure Rust, no OS or `getrandom` dependency, not in the memo's pinned list and not built in the spike: **unverified on wasm32** (same caveat class as `base64` in auth-and-multitenancy). `verify_slice` compares in constant time. Chosen over `crypto.subtle` — the first pass's primitive — so `auth` stays a sync `fn` (1.4) and needs no `js_sys::Reflect`/`platform` path (A8); a verify is two SHA-256 compressions over <1 KiB, too cheap for a promise.
- `base64` 0.22 `general_purpose::URL_SAFE_NO_PAD.decode`: pure Rust, **unverified on wasm32** (as auth-and-multitenancy).
- `Env::secret("GE_TOKEN_HMAC_KEY") -> worker::Secret` (Display): docs-listed, not run by the spike: **unverified at runtime**. An absent binding fails closed (`ok` stays false).
- `wire::parse_receive_header` returning `ReceiveHeader { commands, caps, shallow }` and the `PktReader::remainder` handoff: contract signatures (1.1, 6.3). `shallow <oid>` lines are consumed before the commands — the property that closes blocker 2.
- `BodyReader::{new, fill, buffered, consume}` (section 6): contract signatures; one reader for the whole request drives the header parse, the drain and the ingest handoff.
- `SqlStorage::exec` (sync), `SqlCursor::{one, to_array}`: verified (spike). Sync-span atomicity — no DO event inside a span, writes discarded on `Err` out of `fetch` — measured (platform-facts #4), A2.
- `Stub::fetch_with_request`, `RepoRoute::{stub, internal_request, name, parse}`: verified pattern (memo section 1, spike); `RequestInit`/`new_with_init` bodies on stub requests **unverified at runtime** (as every sibling).
- `jobs::enqueue`/the Janitor slice: contract sections 4 and 5; this module adds no `JobKind` and never calls `set_alarm`.
- `js_sys::Date::now()` for the `exp` check and `exp_ms`: standard `js-sys`.
- git client behaviour relied on: `send-pack` emits `shallow <oid>` lines on shallow pushes; `remote-curl` reads the response only after the body is sent (an early reply is an HTTP/2 RST, `curl 92`); push is v0 `receive-pack` even under `protocol.version=2`; chunked bodies carry no `Content-Length` above `http.postBuffer`. From git 2.4x source and the first-pass review's interop check; not re-run.
- `serde_json` for `TokenScope`/`TokenDto`: the contract's DTO codec (1.3, A8).

## Proof code
```rust
// src/auth/token.rs + src/edge/route.rs + src/repo_do/push.rs + src/jobs/janitor.rs.
// worker 0.8.5. CONTRACTS.md 1.1, 1.4, 3, 4, 5, 6, 8, 10, 12; A1-A3, A8, A9. REGISTRY block in Mechanism.
// `q`, `one`, `to_array`, `N`, `json`, `respond`, `respond_text`, `report`, `read_receive_header` are the sibling helpers
// (repo-do-ref-authority, info-refs-endpoint); `impl From<worker::Error> for Error` maps to Error::Storage.
use {base64::Engine as _, bstr::ByteSlice, hmac::Mac as _};
use worker::{Env, Method, Request, Response, SqlStorageValue as V};
use crate::{auth::Principal, edge::{BodyReader, RepoRoute}, error::Error, repo_do::RepoDo,
            wire::{RefCommand, RefResult}, ReqBudget};
fn now_ms() -> i64 { js_sys::Date::now() as i64 }                            // host float, not client data

// ---- src/auth/token.rs: a sync fn like authenticate (1.4). Pure-Rust HMAC: no crypto.subtle promise, no Reflect (A8).
#[derive(serde::Deserialize)] pub struct TokenScope {
    pub id: String, pub repo: String, #[serde(rename = "ref")] pub refname: String, pub exp: i64,   // exp: unix seconds
    #[serde(rename = "maxPushes")] pub max_pushes: i64, #[serde(rename = "maxBytes")] pub max_bytes: i64,
    #[serde(default)] pub del: bool }                              // delete-refs is advertised (rule 5); del=false forbids deletes
fn b64u(s: &str) -> Result<Vec<u8>, Error> { base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(s).map_err(|_| Error::Forbidden) }
/// `/t/<b64url payload>.<b64url hmac>/...`. Every failure is Forbidden (403), never Auth (401 prompts for a
/// credential that cannot exist). `exp` is enforced here, once per request, before any stub exists.
pub fn verify_token(seg: &str, env: &Env) -> Result<TokenScope, Error> {
    let (body, sig) = seg.split_once('.').ok_or(Error::Forbidden)?;
    let (payload, sig) = (b64u(body)?, b64u(sig)?);
    let mut ok = false;                                                             // an absent binding fails closed
    for name in ["GE_TOKEN_HMAC_KEY", "GE_TOKEN_HMAC_KEY_PREV"] {
        if let Ok(secret) = env.secret(name) {
            if let Ok(mut mac) = <hmac::Hmac<sha2::Sha256> as Mac>::new_from_slice(secret.to_string().as_bytes()) {
                mac.update(&payload); ok |= mac.verify_slice(&sig).is_ok(); } } }     // constant-time inside hmac
    if !ok { return Err(Error::Forbidden); }
    let s: TokenScope = serde_json::from_slice(&payload).map_err(|_| Error::Forbidden)?;
    if s.exp.saturating_mul(1000) <= now_ms() || s.max_pushes < 0 || s.max_bytes < 0
        || s.id.len() > 64 || s.repo.len() > 130 || s.refname.len() > 256 { return Err(Error::Forbidden); }
    Ok(s)
}
#[derive(serde::Serialize, serde::Deserialize, Clone)] pub struct TokenDto {           // wire::http (A8)
    pub id: String, #[serde(rename = "ref")] pub refname: String, pub exp_ms: i64,
    pub max_pushes: i64, pub max_bytes: i64, pub del: bool }
impl From<&TokenScope> for TokenDto {
    fn from(s: &TokenScope) -> Self { Self { id: s.id.clone(), refname: s.refname.clone(), exp_ms: s.exp.saturating_mul(1000),
        max_pushes: s.max_pushes, max_bytes: s.max_bytes, del: s.del } } }

// ---- src/edge/route.rs: inside route(), before the Basic-auth path: `path.strip_prefix("/t/")` -> route_scoped.
fn service_is(req: &Request, want: &str) -> bool {
    req.url().map(|u| u.query_pairs().any(|(k, v)| k == "service" && v == want)).unwrap_or(false) }
/// Reads the rest of the body and discards it. git only reads the response after sending the body, so an early reply
/// lets HTTP/2 RST the stream and git prints `curl 92` instead of the ng lines (review RISK). Real receive-pack drains
/// the pack before execute_commands for the same reason. No subrequests; bounded by the zone body cap (6.2).
async fn drain(body: &mut BodyReader) -> Result<(), Error> {
    while body.fill(1).await? { let n = body.buffered().len(); body.consume(n); }
    Ok(()) }
pub async fn route_scoped(req: Request, env: &Env, tail: &str) -> worker::Result<Response> {
    let (is_post, is_info) = (req.method() == Method::Post, tail.ends_with("/info/refs"));
    let pre = (|| {
        let (seg, tail) = tail.split_once('/').ok_or(Error::NotFound)?;
        let scope = verify_token(seg, env)?;                                   // 403 before any stub or DO exists
        let path = format!("/{tail}");
        let (repo, rest) = RepoRoute::parse(&path).ok_or(Error::NotFound)?;    // Option-form parse (8.1), info-refs-endpoint
        if scope.repo != repo.name() { return Err(Error::Forbidden); }
        Ok((scope, repo, rest.to_owned()))
    })();
    let (scope, repo, rest) = match pre {
        Ok(t) => t,
        Err(e) => { if is_post { if let Ok(mut b) = BodyReader::new(&req) { let _ = b.fill(1 << 20).await; } }  // 6.3 cap
                    return if is_info { respond_text(Err(e)) } else { respond(Err(e)) }; }
    };
    let ctype = req.headers().get("Content-Type")?.unwrap_or_default();
    let mut budget = ReqBudget { max_subrequests: 9_000, used: 0, started_ms: js_sys::Date::now(), max_ms: 240_000.0 };  // 7.1
    let out = async {
        match (req.method(), rest.as_str()) {
            (Method::Get, "info/refs") if service_is(&req, "git-receive-pack") =>
                receive::info_refs(&req, env, &repo, &mut budget).await,       // FULL advertisement (review DEGRADES)
            (Method::Post, "git-receive-pack") if ctype == "application/x-git-receive-pack-request" =>
                receive_pack_scoped(req, env, &repo, &scope, &mut budget).await,
            (Method::Post, _) => { if let Ok(mut b) = BodyReader::new(&req) { let _ = b.fill(1 << 20).await; }
                                   Err(Error::Forbidden) }                     // drain, then refuse
            _ => Err(Error::Forbidden),                                        // a push token does not read
        }
    }.await;
    if is_info { respond_text(out) } else { respond(out) }
}
/// Scope is decided on the parsed header: parse_receive_header already consumed `shallow <oid>` lines (1.1), so
/// `commands` is the only list to check — the first pass's `ng undefined` on shallow pushes cannot occur.
async fn receive_pack_scoped(req: Request, env: &Env, repo: &RepoRoute, s: &TokenScope, budget: &mut ReqBudget)
    -> Result<Response, Error> {
    let mut body = BodyReader::new(&req)?;                                     // one reader for the whole request (6)
    let hdr = read_receive_header(&mut body).await?;                           // <= 1 MiB command section (6.3)
    let in_scope = |c: &RefCommand| c.name.as_bstr() == s.refname.as_bytes().as_bstr() && (s.del || !c.new.is_null());
    if !hdr.commands.iter().any(in_scope) {
        drain(&mut body).await?;
        let results = hdr.commands.iter().map(|c| RefResult::Ng(c.name.clone(), "token scope forbids this ref")).collect();
        return report(&hdr, Ok(()), &results);          // 200 + report-status (A2, 10); a probe_rpc (0 commands) lands here too
    }
    // From here: the sibling's receive_pack with the token forwarded. Write-back: it takes `token: Option<TokenDto>`,
    // puts it on BeginDto and CommitRequest, and runs begin/ingest/commit exactly as repo-do-ref-authority shows.
    let who = Principal { name: format!("token:{}", s.id), can_write: true };    // reflog principal carries the token id
    receive::receive_pack_body(body, hdr, env, repo, &who, Some(TokenDto::from(s)), budget).await   // env: &Env
}

// ---- src/repo_do/push.rs: all of this lives inside the existing sync spans; Storage/Internal still propagate as
// Err out of fetch (A2). `UseRow`/`RevokeDto` are wire::http DTOs (A8).
#[derive(serde::Deserialize)] struct UseRow { pushes: i64, bytes: i64, revoked: i64 }
#[derive(serde::Deserialize)] struct RevokeDto { id: String, exp_ms: i64 }
enum Gate { Admit(i64 /*pack bytes*/), Deny(&'static str) }
impl RepoDo {
    /// Advisory, non-debiting check; first statement of push_begin (BeginDto gains `token: Option<TokenDto>`).
    /// Conflict here is post-header, so the edge turns it into 200 + `ng` for every command (A2).
    fn token_precheck(&self, t: &TokenDto) -> Result<(), Error> {
        match self.q("SELECT pushes,bytes,revoked FROM token_usage WHERE id=?", vec![V::from(t.id.as_str())])?
            .to_array::<UseRow>()?.into_iter().next() {
            Some(u) if u.revoked != 0 => Err(Error::Conflict("token revoked".into())),
            Some(u) if u.pushes >= t.max_pushes => Err(Error::Conflict("rate limit".into())),
            _ => Ok(()) }
    }
    /// commit_push, after the step-2 gc_epoch check (CommitRequest gains `token`). The row is created lazily so a token
    /// that never commits owns nothing; a stale row past exp_ms only under-counts (the edge already enforced exp).
    /// Deny is a normal 'rejected' finish, not an exception: the span commits it.
    fn token_gate(&self, req: &CommitRequest, t: &TokenDto) -> Result<Gate, Error> {
        self.q("INSERT INTO token_usage(id,exp_ms,pushes,bytes,revoked) VALUES(?,?,0,0,0) ON CONFLICT DO NOTHING",
               vec![V::from(t.id.as_str()), V::from(t.exp_ms)])?;
        let u = self.q("SELECT pushes,bytes,revoked FROM token_usage WHERE id=?", vec![V::from(t.id.as_str())])?
            .one::<UseRow>()?;
        if u.revoked != 0 { return Ok(Gate::Deny("token revoked")); }
        if u.pushes >= t.max_pushes { return Ok(Gate::Deny("rate limit")); }
        let pbytes = match &req.pack_id {                                     // cumulative and real: the DO's own byte count
            Some(p) => self.q("SELECT bytes AS n FROM packs WHERE id=?", vec![V::from(p.as_str())])?.to_array::<N>()?
                .into_iter().next().ok_or_else(|| Error::Internal("ingesting pack row missing".into()))?.n,
            None => 0 };
        if u.bytes.saturating_add(pbytes) > t.max_bytes { return Ok(Gate::Deny("byte budget exceeded")); }
        Ok(Gate::Admit(pbytes))
    }
    // Inside commit_push (sibling's span), three insertions:
    //   after step 2:  let mut tbytes = 0;  if let Some(t) = &req.token { match self.token_gate(req, t)? {
    //                  Gate::Deny(r) => return self.finish_push(req, "rejected", now,
    //                      cmds.iter().map(|c| (c.name.clone(), Some(r))).collect()),  Gate::Admit(b) => tbytes = b } }
    //   apply_one(token: Option<&TokenDto>) first lines:
    //                  if let Some(t) = token { if c.name != t.refname { return Ok(Some("out of token scope")); }
    //                      if c.new.is_null() && !t.del { return Ok(Some("token cannot delete")); } }
    //   inside `if any_ok`, after the refs_version bump:
    //                  if let Some(t) = &req.token { self.q("UPDATE token_usage SET pushes=pushes+1, bytes=bytes+?
    //                      WHERE id=?", vec![V::from(tbytes), V::from(t.id.as_str())])?; }
    /// POST /_do/token/revoke — sync span. exp_ms is the token's own expiry (the admin knows the token), so a revoked
    /// row still dies in the Janitor sweep instead of living forever (first pass set exp = 2^31).
    pub fn token_revoke(&self, b: &RevokeDto) -> Result<Response, Error> {
        self.q("INSERT INTO token_usage(id,exp_ms,pushes,bytes,revoked) VALUES(?,?,0,0,1)
                ON CONFLICT(id) DO UPDATE SET revoked=1", vec![V::from(b.id.as_str()), V::from(b.exp_ms)])?;
        json(serde_json::json!({}))
    }
}
// ---- src/jobs/janitor.rs: one more statement in the 5.1/5.2 sync span, revoked rows included. No second alarm (4.1).
// d.q("DELETE FROM token_usage WHERE exp_ms < ?", vec![V::from(now)])?;
```

## Why it works
- **Shallow pushes work because the contract's parser does the reading.** `wire::parse_receive_header` consumes `shallow <oid>` lines before the command lines and returns them in `ReceiveHeader.shallow` (1.1), so `in_scope` iterates `commands` only and the `ng undefined` failure mode is gone by construction. `BodyReader` is the single reader for the whole request (6): the PACK left in `remainder()` is pushed back for ingest (6.3), so the multi-chunk "locked reader" bug has no equivalent — there is no second stream to lock.
- **Early reject still drains.** The all-out-of-scope path reads the body to EOF in `drain` before `report`; real `git-receive-pack` reads the pack before `execute_commands`, and an early response under HTTP/2 RSTs the stream so git prints `curl 92` (review RISK). Draining costs no subrequests and is bounded by the zone body cap (6.2). The same path answers `remote-curl`'s flush-only `probe_rpc` (zero commands) with a 200 report-status, as auth-and-multitenancy requires.
- **Full advertisement, enforcement at the command check.** `/t/` `info/refs` forwards to the ordinary receive-pack advertisement (rule-5 bytes, via `receive::info_refs`), so `send-pack` knows the server's haves and a new-branch push ships a thin pack instead of the whole reachable history (review DEGRADES). Scope is enforced on commands at the edge and again per ref inside the commit span, not by hiding refs. Disclosure of the ref list to a push token is accepted; see Known limits.
- **The debit moved into the CAS span.** `token_gate` runs inside `commit_push` after the `gc_epoch` check, and the `UPDATE token_usage` runs only under `any_ok`: a push that crashes mid-ingest, fails connectivity (2.5), or loses every ref CAS never debits (review crash walk-through and caveat). Exactness is unchanged — one DO per repo, no await inside the span, so `pushes+1` serialises against every concurrent push presenting the same token (platform-facts #4). `max_bytes` debits `packs.bytes`, the normalized size the DO already recorded, so chunked pushes debit real bytes cumulatively (review caveat 2). An exhausted token is only known for sure at commit, so `push_begin`'s `token_precheck` fails that common case fast, before ingest; it never debits.
- **Denial is data, not an exception.** `Gate::Deny` writes `state='rejected'` plus a per-ref `ng` through `finish_push` — the same shape as the `gc_epoch` rejection — and returns `Ok`; only `Storage`/`Internal` propagate as `Err` so the platform discards the span (A2). Post-header, every error maps to 200 + report-status (A2, section 10).
- **403, never 401; receive-pack only.** Every token failure is `Forbidden`, so git never prompts for a credential that cannot exist. `git-upload-pack` and every other suffix on a `/t/` URL are `Forbidden`: a push token does not read. `Git-Protocol` is ignored on receive-pack per rule 8.
- **Per-ref semantics preserved.** Out-of-scope commands get `ng <ref> out of token scope` inside `apply_one` and in-scope commands proceed independently, matching the contract's per-ref model (section 3; `atomic` is not advertised, rule 5). A `del: false` token cannot delete its own ref even though `delete-refs` is advertised (review minor).
- **No second alarm.** Expiry is `DELETE FROM token_usage WHERE exp_ms < now` inside the Janitor slice's sync span — revoked rows die at `exp_ms` too (the first pass made them immortal). Section 4.1: only `jobs::rearm` calls `set_alarm`; a second `setAlarm` would cancel the job queue's alarm (measured #5). Deletion-after-expiry is safe because the edge rejects an expired token before any DO call.
- **Budget.** The token path adds zero subrequests beyond the foundation's: `info/refs` still costs the one `/_do/refs` stub call, `receive-pack` still costs begin + lookups + index posts + commit (7.3), each charged through `budget.charge(1)` inside the sibling code (A1). HMAC verify is two SHA-256 compressions; the scope check runs before any stub call, so an out-of-scope push costs no DO CPU and no R2 write.
- **Trust boundary.** The DO trusts the stub request, which only the edge can make (`internal_request` carries exactly `x-ge-owner`/`x-ge-repo`, section 8.1) — so the edge-verified scope travels as `TokenDto` and the DO enforces the data side: refname equality, the `revoked` flag, and the counters. The HMAC never leaves the edge.

## Changes from the first pass
| First-pass finding (quoted) | Kind | How addressed |
|---|---|---|
| "`rest`'s `pull` calls `body.getReader()` on every pull and never releases; the second pull throws 'ReadableStream is locked', so any PACK larger than the first chunk dies" | blocker | Closed structurally: one `BodyReader` per request (6.1) drives `parse_receive_header`, `drain` and ingest; the PACK hands off through `PktReader::remainder()` pushed back into the same reader (6.3). No second stream exists to lock. |
| "`send-pack.c` emits `shallow <oid>` pkt-lines before the command lines whenever the pushing repo is shallow ... `readCommands` parses that as `{old:"shallow", nw:<oid>, ref:undefined}`" | blocker | `wire::parse_receive_header` parses `shallow` lines into `ReceiveHeader.shallow` (1.1); the scope check sees only `commands`. The CI case (`actions/checkout` depth-1) is covered by added scenario (a). |
| "reserve -> DO writes `pushes=1` ... the Worker isolate is evicted mid-PACK ... the token is now spent: the client's retry gets `ng ... rate limit: 1 pushes per token`. Fix is to debit inside `ingestPush`'s CAS transaction (or reserve a lease id and settle it on commit/abort)" | blocker | Taken the first way: `token_gate` and the `UPDATE token_usage` sit inside `commit_push`'s sync span, and the debit runs only under `any_ok`. A crash anywhere before commit — mid-PACK included — leaves `token_usage` untouched; the orphan pack is the Janitor's ordinary job (section 5). |
| "Reserve-then-ingest burns push/byte budget on any downstream failure; move the debit into the ref-CAS transaction" | caveat | Same fix: nothing is debited at `push_begin` (`token_precheck` is a read-only early-out); the only write is inside the commit span after the ref CASes. A push whose every ref fails CAS does not consume the token either. |
| "for pushes above `http.postBuffer` (chunked, no Content-Length) `bytes` is reserved as 0, and nothing shown ever writes the ingester's real count back, so `maxBytes` across pushes is only enforced per-push" | caveat | `token_gate` reads `packs.bytes` — the normalized byte count the DO already trusts — and adds it inside the same span. `Content-Length` is never consulted (6.2 forbids trusting it anyway). Semantic note: budget is on stored bytes, see Known limits. |
| "advertising only the scoped ref means the client's `send-pack` assumes the server has nothing else. First push of a new branch ... ships the entire reachable history as the PACK. Advertise all refs ... and reject on the command check instead" | caveat | `/t/` `info/refs` calls the normal `receive::info_refs`, which writes the full rule-5 advertisement for every ref. |
| "on out-of-scope commands the Worker responds without draining the body. Over HTTP/2 the runtime cancels the request stream (RST_STREAM); curl surfaces this as 'HTTP/2 stream was not closed cleanly'" | caveat | `drain()` reads to EOF before the `ng` report-status; `route_scoped` also drains (bounded, 1 MiB) before refusing a wrong-suffix POST. |
| "`delete-refs` is advertised, so the 'push one branch' token can also delete that branch; probably should reject `<new>=0{40}` unless the scope says so" | caveat | Scope gains `del: bool`, default false; `apply_one` returns `ng <ref> token cannot delete` for a zero new-oid without it, and the edge's `in_scope` treats such a command as out of scope. |
| "`revoke` rows are never deleted (alarm skips `revoked=1`); unbounded but tiny" | caveat | `token_revoke` keeps the token's real `exp_ms`; the Janitor deletes `exp_ms < now` on revoked and unrevoked rows alike. |
| "Token in URL leaks to config/logs (acknowledged); no `kid` for key rotation (acknowledged)" | caveat | URL-borne by design; unchanged and re-stated in Known limits. Rotation now has a two-key window (`GE_TOKEN_HMAC_KEY_PREV`); a `kid` field remains future work. |
| "Cloudflare request-body cap (plan-dependent, 100-500 MB) bounds any single push" | caveat | Section 6.2: enforced before our code runs; documented. The drain path inherits the same cap. |
| "Push is v0 `receive-pack` even under `protocol.version=2` ... 403 (not 401) avoids the credential prompt" | interop (correct as written) | Kept: receive-pack is v0 by contract (rules 5, 8); token routes are receive-pack only; every token failure is `Forbidden`. |
| "a `kid` is added to the payload" / "Token minting ... and the `revoke` endpoint are hand-waved" (first-pass Known limits) | limit | `/_do/token/revoke` is now real (one sync span). Minting and the admin edge route remain a named dependency on auth-and-multitenancy; `kid` not built. |
| "A push that starts before `exp` and finishes after it is accepted" (first-pass Known limit) | limit | Same shape, deliberate: `exp` is checked once at request start; the commit span does not re-check it, so slow pushes are not lost at the last step. |
| "every push to the repo, tokened or not, serializes through the repo DO" (first-pass Known limit) | limit | Unchanged and intended (repo-do-ref-authority); the limiter adds no extra stub round-trips beyond one `SELECT` in begin's and commit's spans. |

## Known limits
- The token lives in the URL: `.git/config`, shell history, proxy logs and Worker request logs all see it (first pass, unchanged). Mitigations are short `exp` and `/_do/token/revoke`; a header-borne credential cannot be expressed in a clone URL.
- `maxPushes` counts pushes that moved at least one ref — the semantic shift the review's fix implies, stated in Mechanism. A token below its limits can still pay for unbounded ingests that never commit; each is bounded by `ReqBudget` (240 s, 9,000 subrequests) and the zone body cap, not by the token.
- `maxBytes` is metered on `packs.bytes`, the normalized full-object size, which can exceed the received byte count the first pass metered. Delta-heavy pushes are therefore charged more than the wire size; the discrepancy is bounded by the object content itself.
- A push token cannot fetch: `git-upload-pack` on a `/t/` URL is 403, so the headline shallow-CI case still needs an ordinary credential for the clone. Read-scoped tokens are a scope-field extension, not built.
- The full advertisement discloses every ref name and oid to the token holder — the review's fix for pack inflation. A partial advertisement (scoped ref plus the haves send-pack needs) is possible future work.
- `exp` is enforced at request start only (first-pass limit kept deliberately); a `revoke` on a token with no `token_usage` row creates the row with the caller-supplied `exp_ms`, which an honest admin sets to the token's expiry.
- Rotation is a two-key window (`_PREV`); `kid`-keyed multi-secret minting is not built.
- Unverified at runtime: `hmac`/`sha2`/`base64` on wasm32 (pure Rust; same caveat class as auth-and-multitenancy), `Env::secret` Display, `RequestInit` stub requests, and the write-back signature `receive::receive_pack_body(body, hdr, env, repo, who, token, budget)`.
- Scenarios this proof must pass: 2, 4 (a `del:false` token pushing a delete prints `ng ... token cannot delete`), 6 (two pushes on one token: exactly one commits), 11 (shallow push — the headline tokened case). Added (two): (a) `git clone --depth 1`, `git push <token-url>` with `maxPushes=1`: first `ok`, immediate retry prints `ng refs/heads/<ref> rate limit`; (b) a hand-built receive-pack body with a second command on `refs/heads/other`: `ok` for the scoped ref, `ng ... out of token scope` for the other, and `GET /t/<tok>/o/r/info/refs?service=git-upload-pack` returns 403.

## Depends on
- auth-and-multitenancy
- info-refs-endpoint
- repo-do-ref-authority
- two-phase-push
- streaming-pack-parser
