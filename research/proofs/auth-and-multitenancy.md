# Auth and multi-tenancy: owner/repo routing to DO ids

> Second pass · Idea #54 · verdict: **lands with caveats** · feasibility 5/5 · reliability 4/5 · correctness 4/5 (first pass 5/4/4)
> First pass: [proof](./archived/auth-and-multitenancy.md) · [review](../reviews/archived/auth-and-multitenancy.md) · Second pass: [review](../reviews/auth-and-multitenancy.md) · Contract: [CONTRACTS.md](../../CONTRACTS.md)


## Mechanism
This idea is subsumed by CONTRACTS.md and is written here as the three concrete modules that replace it: `auth::authenticate` (section 1.4, the two-token Basic-auth check of section 12), `edge::route` with `RepoRoute` (section 8.1: the one owner/repo parser, `id_from_name("owner/repo")`, the `x-ge-owner`/`x-ge-repo` headers, and the section 10 status mapping), and `RepoDo::boot` (section 8.2: the `meta` table as the source of truth for `repo_id`, `owner`, `repo`, with the single permitted `ctx.id.name` cross-check of 8.3). The first pass's HMAC personal tokens and per-repo `acl` table are not built: section 12 fixes the foundation's auth to `GE_READ_TOKEN`/`GE_WRITE_TOKEN` compared at the edge with no storage read, refuses anonymous access, and lists multi-tenancy and per-user permissions as out of scope, so the ACL becomes a named dependency (Depends on) rather than code. The order inside `route` is path shape (404), credential (401 with `WWW-Authenticate: Basic`), permission (403 for a read token on `git-receive-pack`), then and only then the DO stub, so an unauthenticated request never wakes a Durable Object and never learns whether a repo exists. Tenancy is the DO name plus `meta.repo_id`: R2 keys are `r/<repo_id>/...` (section 2.2), so nothing in R2 or SQLite is derived from the URL or from `ctx.id.name`.

## Primitives
- `#[event(fetch)]` entry, `Request::{path, url, method, headers}`, `Headers::get`, `Response::error`, `DurableObjectNamespace::id_from_name(..).get_stub()`, `Stub::fetch_with_request`: verified on workerd (memo section 1, spike `spikes/rust-ls-refs`).
- `Env::secret("GE_WRITE_TOKEN")` returning `worker::Secret` (Display): in the `worker` 0.8.5 docs, not in the memo's table and not run in the spike: **unverified at runtime**. Reads a binding property, no promise, so `authenticate` stays `fn`.
- `Request::new_with_init(url, &RequestInit)` with `RequestInit { method, headers, body }`, `Headers::new/set`, `Response::from_bytes(..).with_status(..).with_headers(..)`: docs-listed; two-phase-push marks the `RequestInit` path unverified at runtime.
- `base64` 0.22 (`general_purpose::STANDARD.decode`): pure Rust, no OS dependency, not in the memo's crate list and not built in the spike: **unverified on wasm32 here**. The constant-time compare is hand-written over `core::hint::black_box` (no `subtle` dependency); it is a proof, not an audited primitive.
- `SqlStorage::exec` (sync), `SqlCursor::to_array`, `SELECT changes()`: verified (spike, platform-facts #1). `jobs::enqueue` (section 4): contract signature, the only alarm path.
- `web_sys::Crypto::get_random_values_with_u8_array` reached through `js_sys::global()`: section 8.2 says the exact binding path is **unverified**; `crypto.getRandomValues` is synchronous, so `boot` stays `fn`.
- `ctx.id.name` via `js_sys::Reflect::get` on the DO id (workers-rs issue #760): measured populated on workerd 4.129 (platform-facts #2), production **unverified**; read once, in `boot`, decides nothing (8.3). The `ObjectId -> JsValue` conversion is marked in the code.
- git client behaviour (`http.c`: 401 + `WWW-Authenticate` -> `credential_fill` and one retry, 403 terminal; `remote-curl.c`: `probe_rpc` flush-only POST before a body larger than `http.postBuffer`): from git 2.43 source and the first-pass review's interop check; not re-run here.
- `BodyReader::fill` (section 6) for the bounded drain before a 401/403 on a POST: contract signature; `DecompressionStream`/`wasm-streams` inside it are unverified (section 6.1).

## Proof code
```rust
// src/auth/mod.rs -- section 1.4 signature, section 12 two-token model. `fn`: no await, no host promise.
use worker::{Env, Request};
use crate::error::Error;                 // error.rs also provides `impl From<worker::Error> for Error` (-> Error::Storage)
pub struct Principal { pub name: String, pub can_write: bool }

/// Constant-time equality including length. `black_box` keeps the fold from short-circuiting. No `[]`, no `as`.
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    let mut diff = a.len() ^ b.len();
    for i in 0..a.len().max(b.len()) { diff |= usize::from(a.get(i).copied().unwrap_or(0) ^ b.get(i).copied().unwrap_or(0)); }
    core::hint::black_box(diff) == 0
}
/// Basic only (section 12). The username is a label for the reflog, never a credential (review interop 3).
/// Every malformed header is Error::Auth (401), never a 500 (review interop 5). Anonymous is Error::Auth.
pub fn authenticate(req: &Request, env: &Env) -> Result<Principal, Error> {
    let hdr = req.headers().get("Authorization")?.ok_or(Error::Auth)?;
    let b64 = hdr.strip_prefix("Basic ").ok_or(Error::Auth)?;               // `Bearer` refused: Known limits
    if b64.len() > 1024 { return Err(Error::Auth); }
    let raw = base64::engine::general_purpose::STANDARD.decode(b64.trim()).map_err(|_| Error::Auth)?;
    let colon = raw.iter().position(|b| *b == b':').ok_or(Error::Auth)?;
    let (user, rest) = raw.split_at_checked(colon).ok_or(Error::Auth)?;
    let pass = rest.get(1..).ok_or(Error::Auth)?;
    let write = env.secret("GE_WRITE_TOKEN")?.to_string();                    // Secret: Display, unverified at runtime
    let read = env.secret("GE_READ_TOKEN")?.to_string();
    let (is_w, is_r) = (ct_eq(pass, write.as_bytes()), ct_eq(pass, read.as_bytes()));   // both always evaluated
    if !(is_w || is_r) { return Err(Error::Auth); }
    let label = std::str::from_utf8(user).ok()
        .filter(|u| u.len() <= 64 && u.bytes().all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b)));
    Ok(Principal { name: format!("{}:{}", if is_w { "write" } else { "read" }, label.unwrap_or("-")), can_write: is_w })
}

// src/edge/route.rs -- section 8.1 routing and section 10 mapping. The only module that reads client headers.
use worker::{console_log, Env, Headers, Method, Request, RequestInit, Response, Stub};
use crate::{auth::{self, Principal}, edge::BodyReader, error::Error, wire::PktWriter};
pub enum Service { UploadPack, ReceivePack }
pub enum Op { InfoRefs(Service), Post(Service) }
pub struct RepoRoute { pub owner: String, pub repo: String }
impl RepoRoute {
    fn seg_ok(s: &str) -> bool { (1..=64).contains(&s.len()) && s.bytes().all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b)) }
    /// `/:owner/:repo[.git]/<rest>` -> (route, rest). One parser for every module (first-pass caveat 2). Case-sensitive: 8.1 does not lowercase.
    pub fn parse(path: &str) -> Result<(Self, String), Error> {
        let mut it = path.strip_prefix('/').ok_or(Error::NotFound)?.splitn(3, '/');
        let (owner, repo, rest) = (it.next().unwrap_or(""), it.next().unwrap_or(""), it.next().unwrap_or(""));
        let repo = repo.strip_suffix(".git").unwrap_or(repo);
        if !Self::seg_ok(owner) || !Self::seg_ok(repo) { return Err(Error::NotFound); }
        Ok((Self { owner: owner.into(), repo: repo.into() }, rest.into()))
    }
    pub fn name(&self) -> String { format!("{}/{}", self.owner, self.repo) }            // the DO name and nothing else (8.1)
    pub fn stub(&self, env: &Env) -> Result<Stub, Error> { Ok(env.durable_object("REPO")?.id_from_name(&self.name())?.get_stub()?) }
    /// A fresh Request carrying only x-ge-owner / x-ge-repo (8.1): no client header, including Authorization, reaches the DO.
    pub fn internal_request(&self, path: &str, body: Vec<u8>) -> Result<Request, Error> {
        let headers = Headers::new();
        headers.set("x-ge-owner", &self.owner)?; headers.set("x-ge-repo", &self.repo)?;
        let body = Some(js_sys::Uint8Array::from(body.as_slice()).into());
        Ok(Request::new_with_init(&format!("https://do{path}"), &RequestInit { method: Method::Post, headers, body, ..RequestInit::default() })?)
    }
}
fn op_of(req: &Request, rest: &str) -> Result<Op, Error> {
    let svc = |s: &str| match s { "git-upload-pack" => Ok(Service::UploadPack), "git-receive-pack" => Ok(Service::ReceivePack), _ => Err(Error::NotFound) };
    match (req.method(), rest) {
        (Method::Get, "info/refs") => {                                                  // no `service=` is dumb HTTP: 404 (section 10)
            let url = req.url()?;
            Ok(Op::InfoRefs(svc(url.query_pairs().find(|(k, _)| k == "service").map(|(_, v)| v.into_owned()).as_deref().unwrap_or(""))?))
        }
        (Method::Post, "git-upload-pack") => Ok(Op::Post(Service::UploadPack)),
        (Method::Post, "git-receive-pack") => Ok(Op::Post(Service::ReceivePack)),
        _ => Err(Error::NotFound),
    }
}
/// section 1.4 `edge::route`. Order: shape (404) -> credential (401) -> permission (403) -> stub. No DO wakes before all three pass.
pub async fn route(req: Request, env: Env) -> worker::Result<Response> {
    let is_post = req.method() == Method::Post;
    let pre = RepoRoute::parse(&req.path()).and_then(|(repo, rest)| Ok((repo, op_of(&req, &rest)?))).and_then(|(repo, op)| {
        let who: Principal = auth::authenticate(&req, &env)?;                            // sync, every route, anonymous refused
        if matches!(op, Op::InfoRefs(Service::ReceivePack) | Op::Post(Service::ReceivePack)) && !who.can_write { return Err(Error::Forbidden); }
        Ok((repo, op, who))
    });
    let (repo, op, who) = match pre {
        Ok(t) => t,
        Err(e) => {   // bounded drain before an early status: workerd restarted on an unread POST body (spike dev.log)
            if is_post { if let Ok(mut b) = BodyReader::new(&req) { let _ = b.fill(1 << 20).await; } } return respond(Err(e), is_post); }
    };
    let stub = match repo.stub(&env) { Ok(s) => s, Err(e) => return respond(Err(e), is_post) };
    let out = match op {                                                                 // every arm forwards through RepoRoute::internal_request
        Op::InfoRefs(Service::UploadPack) => crate::edge::upload::info_refs(req, &stub, &repo).await,       // protocol-v2-only
        Op::InfoRefs(Service::ReceivePack) => crate::edge::receive::info_refs(req, &stub, &repo).await,     // rule 5 advertisement
        Op::Post(Service::UploadPack) => crate::edge::upload::upload_pack(req, &stub, &repo).await,
        Op::Post(Service::ReceivePack) => crate::edge::receive::receive_pack(req, &env, &who, &repo).await, // repo-do-ref-authority
    };
    respond(out, is_post)
}
/// Section 10: one `ERR <msg>\n` pkt-line for git-protocol POSTs, plain text for info/refs. 500 bodies never carry the internal message.
pub fn respond(out: Result<Response, Error>, pkt: bool) -> worker::Result<Response> {
    let (status, msg): (u16, String) = match out {
        Ok(r) => return Ok(r),
        Err(Error::Protocol(m)) => (400, m), Err(Error::Auth) => (401, "authentication required".into()),
        Err(Error::Forbidden) => (403, "write access required".into()), Err(Error::NotFound) => (404, "not found".into()),
        Err(Error::Conflict(m)) => (409, m), Err(Error::Budget) => (413, "request exceeds this server's budget".into()),
        Err(Error::Limit(m)) => (413, m),
        Err(Error::Storage(m)) | Err(Error::Internal(m)) => { console_log!("500: {m}"); (500, "internal error".into()) },
    };
    let body = if pkt { let mut w = PktWriter { out: Vec::new() }; let _ = w.text(&format!("ERR {msg}")); w.out } else { format!("{msg}\n").into_bytes() };
    let h = Headers::new();
    if status == 401 { h.set("WWW-Authenticate", "Basic realm=\"git-edge\"")?; h.set("Cache-Control", "no-store")?; }
    Ok(Response::from_bytes(body)?.with_status(status).with_headers(h))
}

// src/repo_do/boot.rs -- section 8.2. `new` touches no storage; `boot` is the first call in every fetch and alarm. Sync span.
use std::cell::RefCell;
use worker::SqlStorageValue as V;
use crate::{jobs::{self, JobKind}, repo_do::RepoDo, store};
pub struct RepoHeaders { pub owner: Option<String>, pub repo: Option<String> }
impl RepoHeaders {
    pub const NONE: RepoHeaders = RepoHeaders { owner: None, repo: None };                 // alarm(): no headers (8.2)
    pub fn from_request(req: &Request) -> Self {
        Self { owner: req.headers().get("x-ge-owner").ok().flatten(), repo: req.headers().get("x-ge-repo").ok().flatten() }
    }
}
pub struct Meta { pub repo_id: String, pub owner: String, pub repo: String, pub head: String, pub refs_version: i64, pub gc_epoch: i64 }
#[derive(serde::Deserialize)] struct KV { key: String, value: String }
fn hex16() -> Result<String, Error> {           // 16 random bytes from the global `crypto` (8.2: binding path unverified, sync call)
    let c: web_sys::Crypto = js_sys::Reflect::get(&js_sys::global(), &"crypto".into()).ok()
        .and_then(|v| wasm_bindgen::JsCast::dyn_into(v).ok()).ok_or_else(|| Error::Internal("no crypto".into()))?;
    let mut b = [0u8; 16];
    c.get_random_values_with_u8_array(&mut b).map_err(|_| Error::Internal("getRandomValues".into()))?;
    Ok(b.iter().map(|x| format!("{x:02x}")).collect())
}
impl RepoDo {
    pub fn boot(&self, hdr: &RepoHeaders) -> Result<Meta, Error> {
        let sql = self.sql();
        if !*self.booted.borrow() { store::schema::migrate(&sql)?; *self.booted.borrow_mut() = true; }   // CREATE TABLE IF NOT EXISTS, 2.3/3/4/8
        let rows = self.q("SELECT key, value FROM meta", vec![])?.to_array::<KV>()?;
        let get = |k: &str| rows.iter().find(|r| r.key == k).map(|r| r.value.clone());
        let meta = match get("repo_id") {
            Some(repo_id) => {
                let need = |k: &str| get(k).ok_or_else(|| Error::Internal(format!("meta.{k} missing")));
                let num = |k: &str| need(k)?.parse::<i64>().map_err(|_| Error::Internal(format!("meta.{k}")));
                Meta { repo_id, owner: need("owner")?, repo: need("repo")?, head: need("head")?, refs_version: num("refs_version")?, gc_epoch: num("gc_epoch")? }
            }
            None => {                                                                     // first request ever: create identity, same span
                let (owner, repo) = (hdr.owner.clone().ok_or_else(|| Error::Internal("boot without headers".into()))?,
                                     hdr.repo.clone().ok_or_else(|| Error::Internal("boot without headers".into()))?);
                let (repo_id, now) = (hex16()?, js_sys::Date::now() as i64);            // `as` on a host float, not client data
                for (k, v) in [("repo_id", repo_id.as_str()), ("owner", owner.as_str()), ("repo", repo.as_str()), ("head", "refs/heads/main"),
                               ("refs_version", "0"), ("gc_epoch", "0"), ("created_at", &now.to_string()), ("schema_version", "1")] {
                    self.q("INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT DO NOTHING", vec![V::from(k), V::from(v)])?;
                }
                jobs::enqueue(&sql, JobKind::Janitor, now, "{}")?;                        // section 4.5; enqueue -> rearm is the only set_alarm
                Meta { repo_id, owner, repo, head: "refs/heads/main".into(), refs_version: 0, gc_epoch: 0 }
            }
        };
        if let (Some(o), Some(r)) = (&hdr.owner, &hdr.repo) {
            if *o != meta.owner || *r != meta.repo { return Err(Error::Internal("identity mismatch".into())); }   // 500 (8.2)
        }
        // The one permitted ctx.id.name read in the crate (8.3, CI grep): a cross-check that logs and decides nothing.
        // PSEUDO: `ObjectId -> JsValue` conversion for Reflect::get is unverified in worker 0.8.5 (issue #760 workaround).
        let seen = js_sys::Reflect::get(&self.state.id().into(), &"name".into()).ok().and_then(|v| v.as_string());
        if seen.as_deref() != Some(&format!("{}/{}", meta.owner, meta.repo)) { console_log!("warn: ctx.id.name {seen:?} vs meta {}/{}", meta.owner, meta.repo); }
        Ok(meta)
    }
}
```

## Why it works
- **The 401 dance is git's own.** `remote-curl` sends `GET info/refs` without credentials; `http.c` treats a 401 that carries `WWW-Authenticate` as "ask the credential helper, retry once" and a second 401 as `fatal: Authentication failed` (`respond`, the 401 arm, section 10). Every route runs `authenticate` before anything else, so the challenge always lands on `info/refs`, the one request git can repeat freely, and the following POSTs arrive with the credential already attached (libcurl reuses it for the process).
- **403 is terminal, so a read token cannot loop.** A read-token `git push` gets 403 on `info/refs?service=git-receive-pack` and git prints `The requested URL returned error: 403` once (`route`, the `can_write` check; section 10 `Forbidden`). Returning 401 here would make git re-prompt forever for a credential that is already correct.
- **No existence signal to anyone without a token.** An anonymous request gets 404 for a malformed path (a decision on the URL text alone) or 401 for a well-formed one, before `repo.stub(&env)` is called. The DO is not woken, no SQLite is created, no `meta` row is read (first-pass caveat 1).
- **Large pushes survive.** `remote-curl.c` `post_rpc` sends `probe_rpc`, a POST whose body is one flush, before streaming a body larger than `http.postBuffer`; `receive_pack` (repo-do-ref-authority) reads a header with zero commands and no PACK and answers 200 with an empty body, which is what git ignores and then streams the pack (review interop 4). A request that fails auth on a POST is drained up to 1 MiB before the 401/403 (section 6.3 cap) so workerd does not restart on an unread body.
- **One name, one DO.** `RepoRoute::parse` is the only owner/repo parser in the crate; `name()` is the only place the DO name is formed (8.1 rule: `[A-Za-z0-9._-]{1,64}`, `.git` stripped, case kept). `id_from_name` is deterministic across colos, so `Foo/Bar` on `info/refs` and `Foo/Bar` on `git-receive-pack` are the same DO on every code path (first-pass caveat 2). Two-phase-push's `stub_json` and protocol-v2-only's `upload_pack` both call `internal_request`.
- **Tenancy boundary is structural.** The DO reads `owner`/`repo` from `meta` (8.2) and only cross-checks the headers; R2 keys are `r/<repo_id>/...` with `repo_id` from `meta` (2.2, 8.4). `internal_request` builds a fresh `Request` with exactly two headers, so a client cannot forward `x-ge-owner: other` or its `Authorization` into the DO; a mismatch between headers and `meta` is `Error::Internal("identity mismatch")`, 500, and moves nothing.
- **Nothing is materialised in `new`.** `boot` runs inside `fetch`/`alarm`, after the edge authenticated; the first authenticated request to a name creates `meta` and enqueues the Janitor in one sync span (8.2, 4.5). `ON CONFLICT DO NOTHING` makes a repeated boot safe. Only `jobs::enqueue` is called; `set_alarm` is not (4.1).
- **Secrets never touch the hot path storage and are revocable in one step.** `env.secret` is a binding read; the compare is constant-time and both tokens are always compared, so timing leaks neither which token matched nor its length beyond the 1 KiB cap. `wrangler secret put GE_WRITE_TOKEN` rotates every writer at once, which is the instant revocation the first pass could not offer for HMAC tokens.
- **Errors are statuses, never exceptions.** Every `worker::Error` and every parse failure is mapped in `respond` (section 10, correction 4): 400/401/403/404/409/413/500, `ERR <msg>\n` as one pkt-line on POSTs, plain text on `info/refs`, and the internal message of a 500 goes to `console_log!` only. `authenticate` uses no `unwrap`, `[]`, or `as` on client bytes (`split_at_checked`, `get(1..)`, `usize::from`).
- **Conformance.** Scenarios 1-13 run with `-c credential.helper` supplying the write token. Two additions: (16) anonymous `git ls-remote` gets 401 then succeeds with the read token, and a read-token `git push` prints the 403 line and exits 128 with no ref moved; (17) a client that sends `x-ge-owner: other` and `Authorization` on `git-upload-pack` gets a normal response and the DO log shows no such header.

## Changes from the first pass
| First-pass item | Kind | How addressed |
|---|---|---|
| Idea text: "token verification at the edge" (HMAC personal tokens, `crypto.subtle`) | design | Replaced by section 12: `auth::authenticate` compares the Basic password against `GE_READ_TOKEN`/`GE_WRITE_TOKEN` in constant time (`ct_eq`). No signing key, no `importKey` on the hot path, sync `fn` as section 1.4 demands. |
| Idea text: "per-repo ACL in DO SQLite" (`acl(principal, role)`, `visibility`) | design | Not addressed because section 12 lists multi-tenancy and per-user permissions as out of scope for the foundation. Extension point named in Known limits; dependency on scoped-token-remotes for finer grants. |
| Caveat 1: "anonymous `info/refs` on a private repo gets 401, on a non-existent repo gets 403 ... enumerate which private repos exist" | caveat | `route`: 404 only for a malformed path, 401 for every anonymous well-formed request, 403 only after a token verified. The stub is obtained after all three checks, so no DO state is consulted for an anonymous caller. |
| Caveat 2: "this proof lowercases and strips `.git`; sibling proofs call `idFromName` on the raw captures ... centralize `repoName`" | caveat | `RepoRoute::parse`/`name()` is the single parser and name former (8.1 regex, `.git` stripped, no lowercasing); `stub()` and `internal_request()` are what every sibling uses. |
| Caveat 3: "`jurisdiction("eu")` is a different id ... needs a lookup on the hot path" | caveat | Not addressed because the contract has no jurisdictions; `id_from_name` only. A per-owner jurisdiction rule would be a new `RepoRoute::stub` branch and is out of scope. |
| Caveat 4: "Do not run schema creation in the constructor ... enumeration materializes billed SQLite storage per name" | caveat | `DurableObject::new` stores `state`/`env` only; `boot` runs after the edge authenticated. Anonymous enumeration creates nothing. A token holder can still create repos by name on first request (8.2); see Known limits. |
| Caveat 5: "Catch `stub.fetch` rejections and map to 502; wrap `create` in `transactionSync`; cache the HMAC key" | caveat | `respond` maps every `worker::Error` (`From` -> `Error::Storage`) to 500 per section 10, never an uncaught exception (correction 4). No `create`, no HMAC key: the two-token model removed both. |
| Caveat 6: "Renames/transfers, org/team roles, SSO and instant revocation are out of scope; each adds a lookup" | caveat | Rename: 8.4, R2 keys use `repo_id`, the DO name change is still a row copy (not built). Revocation: secret rotation is instant and global. Roles/SSO: out of scope (section 12). |
| Interop 3: "`subject === user` ... anyone who clones with `x-access-token@host` gets 401; ignore the username" | interop | `authenticate` checks only the password; the username is a sanitised label in `Principal.name` for the reflog `principal` column (section 3), never a credential. |
| Interop 4: "git cannot rewind a streamed pack body" is wrong; "`probe_rpc` ... the protocol layer must answer that flush-only POST with 200" | interop | Stated correctly in Why it works; the flush-only header (zero commands, no PACK) must be answered 200 empty by `receive_pack`; that line is a requirement placed on repo-do-ref-authority's module, not code here, and auth is settled on `info/refs` anyway. |
| Interop 5: "`Authorization: Bearer` is rejected as anonymous. Fine for v1, document it" | interop | Documented in Known limits; `authenticate` returns `Error::Auth` (401 with the Basic challenge) for `Bearer`. |
| Interop 6: "Malformed `Authorization` throws inside `atob`/`b64u` and yields a 500" | interop | Every decode step is `?`/`ok_or(Error::Auth)`: bad base64, no colon, over 1 KiB, non-UTF-8 username all give 401. |
| Reliability note: "ACL is checked once at request start; a revoke landing mid-stream does not affect the in-flight push" | note | Same shape: a rotated secret does not stop a push already past `authenticate`. Accepted and stated in Known limits. |
| Reliability note: "every request for a non-existent repo runs `CREATE TABLE` in the constructor" | note | Moved to `boot` behind auth; `migrate` runs once per isolate lifetime (`booted`), `meta` is read each request (one sync SELECT). |

## Known limits
- **Two global tokens, no tenancy of principals.** Every reader shares `GE_READ_TOKEN`, every writer `GE_WRITE_TOKEN`; the reflog label is client-typed and unauthenticated. The extension point is an `acl(principal, role)` table in `RepoDo` read inside the same sync span as `boot`, keyed by a principal the edge verified; it needs per-user tokens that the foundation does not issue. Named as a dependency on scoped-token-remotes.
- **A token holder creates repos by naming them.** Section 8.2 creates `meta` on the first authenticated request, so a read-token `ls-remote` of `a/b` materialises a DO with an empty SQLite database and a Janitor job. A `create` API and an "exists" gate are wave-1 items; anonymous callers cannot trigger this.
- **`Bearer` and `http.extraHeader` tokens are refused** with a Basic challenge. Adding `Bearer <token>` is one more `strip_prefix` arm and is left out to match section 12's wording.
- **Unverified at runtime**: `Env::secret` Display, `RequestInit`/`new_with_init`, `Response::with_headers`, the `base64` crate on wasm32, the `crypto` global path, and the `ObjectId -> JsValue` conversion for the `ctx.id.name` cross-check (marked PSEUDO). None of them is on the git wire; a failure shows up on the first `wrangler dev` request.
- **`Conflict` -> 409** is not in section 10's mapping table; it is proposed here (native-lfs uses the same) and needs writing back into the contract.
- **Budget**: `route` adds zero subrequests beyond the one stub call and no R2 reads; the pre-auth drain is capped at 1 MiB, so the 128 MB isolate is untouched. CPU is a base64 decode and two 1 KiB compares. A POST whose body exceeds 1 MiB and fails auth is not fully drained; whether production workerd restarts on that is unmeasured (the local log showed a restart on an unread body).
- **Rename** still needs a copy of the SQLite rows to a new DO name; R2 is untouched (8.4). Not built.
- **TLS** is Cloudflare's: the Worker cannot refuse a plain-HTTP `Authorization` header; the zone must set "Always Use HTTPS".
- **Point-in-time auth**: a secret rotated while a push streams does not stop that push (same as the first pass and as GitHub).

## Depends on
repo-do-ref-authority, protocol-v2-only, two-phase-push, scoped-token-remotes (per-repo, per-user grants: the ACL this idea originally promised)
