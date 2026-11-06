# The /info/refs?service= entrypoint and pkt-line codec

> Second pass · Idea #53 · verdict: **lands with caveats** · feasibility 4/5 · reliability 5/5 · correctness 4/5 (first pass 5/5/4)
> First pass: [proof](./archived/info-refs-endpoint.md) · [review](../reviews/archived/info-refs-endpoint.md) · Second pass: [review](../reviews/info-refs-endpoint.md) · Contract: [CONTRACTS.md](../../CONTRACTS.md)


## Mechanism
This idea is subsumed by CONTRACTS.md: the pkt-line codec is the `wire` module (section 1.1, proven byte-for-byte in protocol-v2-only), and what remains as a concrete module is `edge` (section 1.4): `edge::route`, the `info/refs` handler that applies rules 5 to 8, the section 8.1 identity headers (`RepoRoute`, `RepoHeaders`, `internal_request`), and `edge::respond`, the section 10 status mapping that turns every `Error` into an HTTP status before a byte is written so no `worker::Error` ever escapes as a 500 (correction 4). `GET /<owner>/<repo>[.git]/info/refs?service=` runs `auth::authenticate` first (section 12), then either writes the static v2 advertisement with no DO call (`Git-Protocol: version=2` on `git-upload-pack`, rule 6) or makes exactly one `GET /_do/refs` stub call (`RepoDo::list_refs`, sync, `refs` and `meta` tables, section 1.3) and writes the v0 listing with `wire::write_advertisement_v0` (rules 5, 7, 8). The response is one `Vec<u8>` handed to `Response::from_bytes`, so there is no background writer to abort when the client disconnects. This proof also carries the two `PktReader` methods and the codec conformance tests that protocol-v2-only left out.

## Primitives
- `worker` 0.8.5: `#[event(fetch)]` entry, `Env::durable_object("REPO").id_from_name(..).get_stub()`, `Stub::fetch_with_request`, `Request::{path, method, url, headers}`, `Headers::{get, set}`, `Response::{from_bytes, with_headers, with_status, status_code, bytes}`: verified, the spike served real git 2.43 through them (`research/rust-spike.md`).
- `worker` 0.8.5: `RequestInit::{with_method, with_headers, with_body}`, `Request::new_with_init(&str, &RequestInit)`, `Request::headers_mut()`: present in the crate source at these signatures (`request.rs:106,219`, `request_init.rs:35-55`), **not run** by the spike; the stub request path is therefore unverified at runtime, as two-phase-push also notes.
- `gix-packetline` 0.22.2 `decode::streaming`: read from the pinned source. `0000`/`0001`/`0002` are control lines; `0003` is `InvalidLineLength`; `0004` is `DataIsEmpty`; a non-hex prefix is `HexDecode`; a length over 65520 is `DataLengthLimitExceeded`; a length past the buffer is `Incomplete`. All map to `Error::Protocol` or `Ok(None)` in `PktReader::next` (rule 1). `blocking_io::encode::{data_to_write, flush_to_write, response_end_to_write}`: verified on wasm32 (spike, correction 1).
- `js_sys::Date::now()` for `ReqBudget.started_ms`, `js_sys::Uint8Array::from(&[u8])` for a stub request body: standard `js-sys`, verified in general, this call site not run.
- `auth::authenticate(&Request, &Env) -> Result<Principal, Error>`: contract signature (section 1.4, section 12), body in auth-and-multitenancy.
- Measured (spike `dev.log` line 76): answering a POST with 400 before reading its body made local workerd log `Can't read from request stream after response has been sent` and restart. The edge drains a rejected POST body (bounded, 1 MiB) before answering. **Local only.**
- Measured (platform-facts #7): the subrequest limit is not enforced by local workerd; `ReqBudget` and the `x-ge-subrequests` header are the only guard (section 7).
- git wire facts used: `remote-curl.c:discover_refs` takes the smart path only when the content type is `application/x-git-<service>-advertisement` and the first pkt is `# service=<name>\n` followed by flush; it retries with credentials only after a 401 on `info/refs`; `Git-Protocol` is a colon-separated list. Checked against git 2.43 source; the v0 and v2 handshakes ran against git 2.43 in the spike.

## Proof code
```rust
// src/edge/mod.rs -- CONTRACTS 1.4 (edge::route), 8.1 (identity headers), 10 (status mapping); rules 5-8 through `wire`.
// `?` on a worker::Result relies on `impl From<worker::Error> for Error` = Error::Storage (error.rs, refs-sqlite-objects-r2).
use {bstr::BString, gix_hash::ObjectId, worker::{console_log, Env, Headers, Method, Request, RequestInit, Response, Stub}};
use crate::{auth, error::Error, wire::{self, PktWriter, RefRow, Service}, ReqBudget};
mod body; mod receive; mod upload;   // BodyReader (section 6), receive_pack (two-phase-push), upload_pack + proto_of (protocol-v2-only)
pub use body::BodyReader; use upload::{proto_of, Proto};
const NO_CACHE: [(&str, &str); 3] = [("Cache-Control", "no-cache, max-age=0, must-revalidate"),
                                     ("Expires", "Fri, 01 Jan 1980 00:00:00 GMT"), ("Pragma", "no-cache")];   // git-http-backend hdr_nocache

/// `/<owner>/<repo>[.git]/<rest>`; owner and repo `[A-Za-z0-9._-]{1,64}`, never `.` or `..` (8.1). Anything else is 404.
pub struct RepoRoute { pub owner: String, pub repo: String }
impl RepoRoute {
    pub fn parse(path: &str) -> Option<(RepoRoute, &str)> {
        let mut it = path.strip_prefix('/')?.splitn(3, '/');
        let (owner, repo, rest) = (it.next()?, it.next()?, it.next()?);
        let repo = repo.strip_suffix(".git").unwrap_or(repo);
        let ok = |s: &str| (1..=64).contains(&s.len()) && s != "." && s != ".."
            && s.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'));
        (ok(owner) && ok(repo)).then(|| (RepoRoute { owner: owner.to_owned(), repo: repo.to_owned() }, rest))
    }
    pub fn stub(&self, env: &Env) -> Result<Stub, Error> {       // 8.1: one DO per owner/repo. The DO trusts meta, not this name (8.3).
        Ok(env.durable_object("REPO")?.id_from_name(&format!("{}/{}", self.owner, self.repo))?.get_stub()?)
    }
    pub fn apply_headers(&self, req: &mut Request) -> Result<(), Error> {   // headers_mut: crate source, not run
        let h = req.headers_mut()?; h.set("x-ge-owner", &self.owner)?; h.set("x-ge-repo", &self.repo)?; Ok(())
    }
}
/// One stub request carrying the 8.1 headers. `body` is raw bytes (JSON or pkt-lines); None for GET /_do/refs.
pub fn internal_request(repo: &RepoRoute, path: &str, method: Method, body: Option<Vec<u8>>) -> Result<Request, Error> {
    let mut init = RequestInit::new();
    init.with_method(method).with_headers(Headers::new()).with_body(body.map(|b| js_sys::Uint8Array::from(b.as_slice()).into()));
    let mut req = Request::new_with_init(&format!("https://do{path}"), &init)?;   // RequestInit path: unverified at runtime
    repo.apply_headers(&mut req)?;
    Ok(req)
}
/// What RepoDo::boot compares against meta (8.2). `NONE` for alarm(), which carries no headers.
pub struct RepoHeaders { pub owner: Option<String>, pub repo: Option<String> }
impl RepoHeaders {
    pub const NONE: RepoHeaders = RepoHeaders { owner: None, repo: None };
    pub fn from_request(req: &Request) -> Self { let g = |k: &str| req.headers().get(k).ok().flatten(); RepoHeaders { owner: g("x-ge-owner"), repo: g("x-ge-repo") } }
}
// ---- /_do/refs body, both directions (1.3: JSON with hex ids). Used by RepoDo::fetch (repo-do-ref-authority) and info_refs. ----
#[derive(serde::Serialize, serde::Deserialize)] struct RefDto { name: String, target: String, peeled: Option<String> }
#[derive(serde::Serialize, serde::Deserialize)] struct RefsDto { head: Option<String>, refs: Vec<RefDto> }
pub fn refs_json((head, refs): (Option<BString>, Vec<RefRow>)) -> Result<Response, Error> {
    let refs = refs.iter().map(|r| RefDto { name: r.name.to_string(), target: r.target.to_string(), peeled: r.peeled.map(|p| p.to_string()) }).collect();
    Ok(Response::from_json(&RefsDto { head: head.map(|h| h.to_string()), refs })?)
}
fn refs_from_json(b: &[u8]) -> Result<(Option<BString>, Vec<RefRow>), Error> {   // DO bytes are ours, but still no unwrap
    let d: RefsDto = serde_json::from_slice(b).map_err(|e| Error::Internal(format!("refs json: {e}")))?;
    let oid = |s: &str| ObjectId::from_hex(s.as_bytes()).map_err(|_| Error::Internal("bad oid from DO".into()));
    let refs = d.refs.iter().map(|r| Ok(RefRow { name: r.name.as_str().into(), target: oid(&r.target)?,
                                                 peeled: r.peeled.as_deref().map(oid).transpose()? })).collect::<Result<_, Error>>()?;
    Ok((d.head.map(BString::from), refs))
}
/// Section 10, before any response byte. `respond`: one `ERR <msg>\n` pkt-line (git POSTs, DO routes); `respond_text`: plain text (info/refs).
pub fn respond(out: Result<Response, Error>) -> worker::Result<Response> { finish(out, true) }
pub fn respond_text(out: Result<Response, Error>) -> worker::Result<Response> { finish(out, false) }
fn finish(out: Result<Response, Error>, pkt: bool) -> worker::Result<Response> {
    let e = match out { Ok(r) => return Ok(r), Err(e) => e };
    let (status, tag, msg): (u16, &str, &str) = match &e {
        Error::Protocol(m) => (400, "protocol", m), Error::Unpack(m) => (400, "unpack", m), Error::Auth => (401, "auth", "authentication required"),
        Error::Forbidden => (403, "forbidden", "write access denied"), Error::NotFound => (404, "not_found", "not found"),
        Error::Conflict(m) => (409, "conflict", m), Error::Budget => (413, "budget", "request exceeds this server's budget"),
        Error::Limit(m) => (413, "limit", m), Error::Storage(m) => { console_log!("git-edge storage: {m}"); (500, "storage", "internal error") }
        Error::Internal(m) => { console_log!("git-edge internal: {m}"); (500, "internal", "internal error") }
    };
    let msg: String = msg.chars().take(1024).collect();                         // keeps the ERR pkt under MAX_PKT_DATA
    let body = if pkt { let mut w = PktWriter::default(); w.text(&format!("ERR {msg}"))?; w.out } else { format!("{msg}\n").into_bytes() };
    let h = Headers::new();
    h.set("x-ge-error", tag)?;                                                  // Error::from_do_response rebuilds the variant from it
    if matches!(e, Error::Auth) { h.set("WWW-Authenticate", "Basic realm=\"git-edge\"")?; }
    Ok(Response::from_bytes(body)?.with_status(status).with_headers(h))
}
impl Error {   // src/error.rs: the edge-side inverse of `finish` for a non-200 DO response.
    pub async fn from_do_response(mut resp: Response) -> Error {
        let tag = resp.headers().get("x-ge-error").ok().flatten().unwrap_or_default();
        let m = resp.text().await.unwrap_or_default().trim_start_matches(|c| c != 'E').trim_start_matches("ERR ").trim().to_owned();
        match tag.as_str() { "protocol" => Error::Protocol(m), "unpack" => Error::Unpack(m), "auth" => Error::Auth, "forbidden" => Error::Forbidden,
            "not_found" => Error::NotFound, "conflict" => Error::Conflict(m), "budget" => Error::Budget, "limit" => Error::Limit(m),
            _ => Error::Storage(format!("DO {}: {m}", resp.status_code())) }
    }
}
/// Rules 5-8. v2 upload-pack: static bytes, no DO call. Otherwise one GET /_do/refs (sync in the DO, 1.3) and the v0 listing.
async fn info_refs(req: &Request, env: &Env, repo: &RepoRoute, budget: &mut ReqBudget) -> Result<Response, Error> {
    let url = req.url()?;
    let service = url.query_pairs().find(|(k, _)| k == "service").map(|(_, v)| v.into_owned());
    let proto = proto_of(req);
    let (service, name) = match service.as_deref() {
        Some("git-upload-pack") => (Service::UploadPack { v1: matches!(proto, Proto::V1) }, "git-upload-pack"),
        Some("git-receive-pack") => (Service::ReceivePack, "git-receive-pack"),   // version=1 and version=2 ignored (rule 8; git has no v2 push)
        Some(_) => return Err(Error::Protocol("unknown service".into())),
        None => return Err(Error::Protocol("dumb HTTP is not served; use smart HTTP (git >= 1.6.6)".into())),
    };
    let mut w = PktWriter::default();
    match service {
        Service::UploadPack { .. } if matches!(proto, Proto::V2) => {           // rule 6, byte-exact; `# service` prelude is written here
            w.text("# service=git-upload-pack")?; w.flush();
            wire::write_capability_advertisement_v2(&mut w);
        }
        _ => {
            budget.charge(1)?;                                                  // 7.1: the only subrequest on this route
            let fwd = internal_request(repo, "/_do/refs", Method::Get, None)?;
            let mut resp = repo.stub(env)?.fetch_with_request(fwd).await?;   // DO eviction or throw: Err(Storage) -> 500, never a raw exception
            if resp.status_code() != 200 { return Err(Error::from_do_response(resp).await); }
            let (head, refs) = refs_from_json(&resp.bytes().await?)?;
            wire::write_advertisement_v0(&mut w, service, head.as_deref(), &refs);   // rules 5, 7, 8: prelude, [version 1], caps, refs, flush
        }
    }
    let h = Headers::new();
    h.set("Content-Type", &format!("application/x-{name}-advertisement"))?;
    for (k, v) in NO_CACHE { h.set(k, v)?; }
    h.set("x-ge-subrequests", &budget.used.to_string())?;                       // section 7: what the harness asserts on
    Ok(Response::from_bytes(w.out)?.with_headers(h))                            // one buffer, no background writer to abort
}
/// CONTRACTS 1.4. Auth before any stub call (section 12); every Error becomes a status here (section 10, correction 4).
pub async fn route(req: Request, env: Env) -> worker::Result<Response> {
    let path = req.path();
    let Some((repo, rest)) = RepoRoute::parse(&path) else { return respond_text(Err(Error::NotFound)) };
    let mut budget = ReqBudget { max_subrequests: 9_000, used: 0, started_ms: js_sys::Date::now(), max_ms: 240_000.0 };   // 7.1
    let out = async {
        let who = auth::authenticate(&req, &env)?;                              // a 401 on info/refs is what makes git retry with credentials
        let ctype = req.headers().get("Content-Type")?.unwrap_or_default();
        match (rest, req.method()) {
            ("info/refs", Method::Get) => info_refs(&req, &env, &repo, &mut budget).await,
            ("git-upload-pack", Method::Post) if ctype == "application/x-git-upload-pack-request" =>
                upload::upload_pack(req, &repo.stub(&env)?, &repo).await,
            ("git-receive-pack", Method::Post) if ctype == "application/x-git-receive-pack-request" => {
                if !who.can_write { return Err(Error::Forbidden); }
                receive::receive_pack(req, &env, &repo, &who, &mut budget).await
            }
            ("info/refs" | "git-upload-pack" | "git-receive-pack", m) => {
                if m == Method::Post { let _ = BodyReader::new(&req)?.fill(1 << 20).await; }   // drain: unread body restarted local workerd
                Err(Error::Protocol("wrong method or content-type for this route".into()))
            }
            _ => Err(Error::NotFound),                                          // no dumb-HTTP objects/ or info/packs routes (section 12)
        }
    }.await;
    if rest == "info/refs" { respond_text(out) } else { respond(out) }
}
// ---- src/wire/mod.rs additions: the two PktReader methods protocol-v2-only omitted, and response_end (never sent over HTTP, rule 3). ----
impl PktReader { pub fn push(&mut self, bytes: &[u8]) { self.buf.extend_from_slice(bytes); }
                 pub fn remainder(&mut self) -> Vec<u8> { let tail = self.buf.split_off(self.pos); self.buf.clear(); self.pos = 0; tail } }
impl PktWriter { pub fn response_end(&mut self) { let _ = encode::response_end_to_write(&mut self.out); } }
#[cfg(test)] mod pkt_tests {   // inside `wire`: rule 1 and rules 5-6, byte-exact; run natively and under wasm-bindgen-test on workerd
    use super::*;
    fn first(b: &[u8]) -> Result<bool, Error> { let mut r = PktReader::default(); r.push(b); r.next().map(|p| p.is_some()) }
    #[test] fn rule_1() {
        for bad in [&b"0003"[..], b"-001", b"ffff"] { assert!(matches!(first(bad), Err(Error::Protocol(_)))); }   // 0003, non-hex, over 65520
        assert!(matches!(first(b"0009abc"), Ok(false)));                                                          // overrun: need more bytes
        let mut r = PktReader::default(); r.push(b"0007abc0000PACK"); r.next().ok(); r.next().ok(); assert_eq!(r.remainder(), b"PACK");
    }
    #[test] fn advertisements() {   // prefixes computed, not hand-counted: 30, 14, 23, 19, 25, 23 bytes
        let mut w = PktWriter::default(); w.text("# service=git-upload-pack").ok(); w.flush(); write_capability_advertisement_v2(&mut w);
        assert_eq!(w.out, b"001e# service=git-upload-pack\n0000000eversion 2\n0017agent=git-edge/0.1\n0013ls-refs=unborn\n0019fetch=shallow filter\n0017object-format=sha1\n0000".to_vec());
        let mut w = PktWriter::default(); write_advertisement_v0(&mut w, Service::ReceivePack, None, &[]);
        assert!(w.out.ends_with(b"0000000000000000000000000000000000000000 capabilities^{}\0report-status report-status-v2 delete-refs side-band-64k quiet ofs-delta object-format=sha1 agent=git-edge/0.1\n0000"));
    }
}
```

## Why it works
- The smart path is taken for the reason git checks, and only that reason. `remote-curl.c:discover_refs` looks at the content type `application/x-git-<service>-advertisement` and at the first pkt `# service=<name>\n` plus flush; `info_refs` writes exactly those (rules 5 and 6), so git proceeds to `POST git-upload-pack` or `git-receive-pack` instead of falling back to dumb `objects/` GETs, which `route` answers 404 (section 12).
- The v2 advertisement is static, so it needs no DO call. A client that sends `Git-Protocol: version=2` stays in command mode after `version 2`; the ref listing happens later in `ls-refs` (section 1.3 route `/_do/ls-refs`). The bytes are rule 6 exactly, checked in `pkt_tests::advertisements`: `ls-refs=unborn`, `fetch=shallow filter`, `object-format=sha1`, `agent=git-edge/0.1`, nothing else. Every advertised token is honoured by protocol-v2-only and section 9.
- The v0 listing is one sync DO read. `GET /_do/refs` has "Awaits inside: none" (section 1.3), so `list_refs` returns a point-in-time view of `refs` and `meta.head` that cannot be torn by a concurrent `commit_push` span (section 3). `write_advertisement_v0` writes rule 5's receive-pack capability list verbatim (no `atomic`, no `push-options`) or rule 7's upload-pack list `object-format=sha1 agent=git-edge/0.1`, plus `version 1` first under rule 8. The empty-repo line `<40 zeros> capabilities^{}\0<caps>` is what `send-pack` and `fetch-pack` expect (scenario 1).
- Auth is where git needs it. `route` runs `auth::authenticate` before `info_refs`, and `respond_text` maps `Error::Auth` to 401 with `WWW-Authenticate: Basic realm="git-edge"` (section 10); git's HTTP client retries `info/refs` with credentials only after that 401. Write access is checked at the edge (`Forbidden`, 403) before any body is read.
- No error can escape as an exception. Every arm of `route` returns `Result<_, Error>`, and `finish` turns each variant into the section 10 status before a byte is written: plain text for `info/refs`, one `ERR <msg>\n` pkt-line for the POSTs, which `remote-curl` prints as `fatal: remote error: <msg>` (correction 4, scenario 13). A DO that throws or is evicted mid-call surfaces as `Err` from `fetch_with_request` and becomes a 500, not an unhandled rejection. A non-200 DO response is rebuilt into the original variant through the `x-ge-error` tag.
- Identity follows section 8. The DO is derived by `id_from_name("owner/repo")` and every stub request carries `x-ge-owner`/`x-ge-repo` (8.1); `RepoDo::boot` compares them with `meta` (8.2) and never derives anything from the URL or from `ctx.id.name` (8.3). `RepoRoute::parse` enforces `[A-Za-z0-9._-]{1,64}` and strips `.git` at the edge.
- The codec obeys rule 1 in both directions. `PktReader::next` (protocol-v2-only) delegates to `gix_packetline::decode::streaming`, which the pinned source shows rejects `0003`, non-hex prefixes and lengths over 65520 and reports overruns as `Incomplete`; `pkt_tests::rule_1` pins each case to `Error::Protocol` or `Ok(None)`. `remainder()` hands the bytes after the receive header (the PACK) to `pack::ingest` untouched (section 6.3). `response_end` exists but is never written over smart HTTP (rule 3).
- Budget accounting starts at the edge. `ReqBudget` is built with 7.1's numbers, the one stub call of the v0 path is charged first, and `x-ge-subrequests` on the response is what the harness asserts on, because local workerd does not enforce the limit (platform-facts #7).
- No cache can serve a stale ref list: the three `NO_CACHE` headers are `git-http-backend`'s `hdr_nocache`, so neither Cloudflare's cache nor a proxy keeps the advertisement.

## Changes from the first pass
| First-pass item | Kind | How addressed |
|---|---|---|
| "Capability advertisement over-promises (filter, shallow, wait-for-done, atomic, push-options, report-status-v2)" | caveat | `info_refs` writes nothing itself: v2 is `wire::write_capability_advertisement_v2` (rule 6: `ls-refs=unborn`, `fetch=shallow filter`, honoured by protocol-v2-only and section 9 step 5), v0 is `wire::write_advertisement_v0` (rule 5 for receive-pack, which drops `atomic` and `push-options` and keeps `report-status-v2` because rule 4 implements it; rule 7 for upload-pack, `object-format=sha1 agent=` only). `pkt_tests::advertisements` pins both byte strings. |
| "Git-Protocol version=1 is answered as plain v0 without the 'version 1' line" | caveat | `Service::UploadPack { v1: matches!(proto, Proto::V1) }` in `info_refs`; `write_advertisement_v0` emits `version 1\n` after the prelude (rule 8). Receive-pack ignores the header, also rule 8. |
| "No error handling around stub.fetch or the async stream writer: DO eviction or client disconnect mid-advertisement yields an unhandled rejection / 1101 page" | caveat | `fetch_with_request(..).await?` in `info_refs` turns a stub failure into `Error::Storage` and `finish` into a 500 with a body. The response is `Response::from_bytes(w.out)`: one buffer, no writer task, so a client disconnect has nothing to abort. |
| "pkt-line decoder maps '0003' to flush and lets parseInt accept malformed lengths ('-001') silently; malformed bodies should 400" | caveat | The hand-written decoder is gone; `PktReader::next` uses `gix_packetline::decode::streaming`, whose source rejects `0003` (`InvalidLineLength`) and `-001` (`HexDecode`). `pkt_tests::rule_1` asserts `Error::Protocol` for both, and `respond` maps it to 400 with an `ERR` pkt. |
| "Auth (401 + WWW-Authenticate) must be placed in the Worker before stub.fetch on the info/refs GET" | caveat | `route` calls `auth::authenticate` before any route body, and `finish` adds `WWW-Authenticate: Basic realm="git-edge"` on `Error::Auth` (section 10, section 12). |
| "Request-body limit stated as 500 MB for 'paid' is wrong: it follows the zone plan" | caveat | Corrected in Known limits and in section 7's table: 100 MB Free/Pro, 200 MB Business, 500 MB Enterprise, enforced before the Worker runs (section 6.2). |
| Review interop: "Advertising HEAD and symref= for receive-pack is more than real receive-pack does" | interop | Kept as a harmless superset: `write_advertisement_v0` lists `HEAD` first for both services and no `symref=` at all (rule 5's list is exact). `send-pack` ignores `HEAD`. |
| Review interop: "Missing no-done in v0 upload-pack caps costs one extra round-trip" | interop | Moot: there is no v0 upload-pack negotiation (rule 7, section 12); a v0 `POST git-upload-pack` gets 400 `ERR protocol v2 required (git >= 2.26)`. |
| Review crash walk-through: "stub.fetch throws ... should map to 502" | interop | Mapped to 500 (`Error::Storage`), because section 10 fixes the table and has no 502. Behaviour to git is the same: a failed `info/refs`. |
| Known limit: "practical ceiling is tens of thousands of refs before the 30 s CPU limit" | limit | Still a limit, now sized: the v0 listing is one `Vec<u8>` of about 90 bytes per ref (Known limits). v2 clients never take this path. |

## Known limits
- The v0 advertisement is buffered, not streamed: 100,000 refs is about 9 MB in one `Vec<u8>` plus the JSON copy from `/_do/refs` (about the same again), well inside 128 MB but a single 20 MB allocation per `ls-remote` from a v0 client. A v2 client pays nothing here. No cap is enforced; a repo with millions of refs would hit the DO's 128 MB in `list_refs` first.
- `gix-packetline` rejects `0004` (`DataIsEmpty`) while git's `packet_read` accepts it as an empty data line. No git client sends `0004`; documented as a parser deviation, not fixed.
- `RequestInit`, `Request::new_with_init`, `Request::headers_mut` on a stub request, and `js_sys::Uint8Array` bodies are read from the `worker` 0.8.5 source and not run; the spike forwarded the incoming `Request` itself. Day-1 test: `GET /_do/refs` with the two headers set arrives at `RepoHeaders::from_request` intact.
- `Error::from_do_response` parses the `ERR ` pkt body loosely (`trim_start_matches`); a DO body that is not one pkt-line degrades to `Error::Storage` with the raw text, never to a wrong success.
- Ref names are moved through JSON as `String`, so a non-UTF-8 ref name (legal for git) would fail `list_refs` with `Error::Internal`. The `refs.name` column is `TEXT` (section 3), so the foundation already assumes UTF-8; recorded here so the ls-refs path shares the assumption.
- Dumb HTTP (`info/refs` without `service=`, `objects/`, `info/packs`) is 400 or 404, not implemented (section 12). Clients before git 1.6.6 cannot clone.
- Draining a rejected POST body is bounded at 1 MiB; a larger body on a wrong-content-type POST may still trigger the local workerd restart seen in the spike. Production behaviour unmeasured.
- Subrequests: `info/refs` costs 0 (v2) or 1 (v0) stub calls; CPU is one JSON parse and one buffer build; the subrequest limit itself is unverified on a deployed Worker (platform-facts #7). Request bodies are capped by the zone plan (100/200/500 MB) before our code runs (section 6.2).
- `Conflict -> 409` and `Unpack -> 400` are this proof's additions where section 10 is silent; in practice neither reaches `finish` on the public routes, because `receive_pack` converts them into `report-status` (section 10, last paragraph).
- Scenarios this proof must pass (section 11): 1 (empty-repo advertisement, both services), 2 (receive-pack advertisement precedes the push), 13 (v0 client sees the `ERR` pkt, not a hang). Added scenario 19: `curl -si "$U/o/r/info/refs?service=git-upload-pack"` without credentials returns 401 with `WWW-Authenticate: Basic realm="git-edge"`, and `git -c protocol.version=1 ls-remote` with credentials sees a `version 1` pkt and the same refs as v0 (`GIT_TRACE_PACKET=1`). Added scenario 20: `printf '0003' | curl --data-binary @- -H 'Content-Type: application/x-git-upload-pack-request' -H 'Git-Protocol: version=2'` returns 400 whose body is one pkt-line starting `ERR `, and the wrangler log shows no restart.

## Depends on
- protocol-v2-only
- repo-do-ref-authority
- refs-sqlite-objects-r2
- auth-and-multitenancy
- two-phase-push
