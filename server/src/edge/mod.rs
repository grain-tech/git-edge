//! edge (CONTRACTS.md 1.1, 6, 8, 10): stateless router. Auth first, then the DO.
//! The receive-pack flow honours A2: once the command header parses, every later failure
//! returns HTTP 200 carrying `unpack <err>` + `ng <ref>` for each command.

use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;

use futures_util::{Stream, StreamExt};
use gix_hash::ObjectId;
use worker::{Env, Method, Request, Response, Stub};

use crate::auth::{self, Level};
use crate::error::{respond, Error};
use crate::pack;
use crate::platform;
use crate::store::{Bucket, PackId, PushId, RepoId};
use crate::store::BodyChunk;
use crate::wire::{
    self,
    http::{stub_json, stub_raw, RepoRoute},
    PktReader, PktWriter, RefResult, Service,
};
use crate::{ReqBudget, Spend};

const CMD_CAP: usize = 1 << 20; // section 6.3: command section cap
const FILL_STEP: usize = 64 << 10; // 6.3: fill in 64 KiB steps

/// The one size check every capped request body shares — the declared
/// Content-Length and each streamed chunk's running total. Over-cap is
/// the caller's Protocol wording; the check itself allocates nothing.
fn check_body_len(len: usize, cap: usize, too_big: &'static str) -> Result<(), Error> {
    if len > cap {
        return Err(Error::Protocol(too_big.into()));
    }
    Ok(())
}

/// The declared Content-Length checked before a byte is read. An absent
/// or unparsable header falls through — the streamed check bounds the real
/// body. A length beyond usize's reach counts as over-cap.
fn declared_len_ok(header: Option<&str>, cap: usize, too_big: &'static str) -> Result<(), Error> {
    if let Some(n) = header.and_then(|v| v.parse::<u64>().ok()) {
        check_body_len(usize::try_from(n).unwrap_or(usize::MAX), cap, too_big)?;
    }
    Ok(())
}

/// `Request::bytes()` without worker's infallible `vec![0; len]` inside
/// `Uint8Array::to_vec()`, and bounded by `cap`: the declared
/// Content-Length is checked before a byte is read, and the streamed
/// length is checked before each chunk's reservation — an over-cap body
/// never buys a reservation. A refused reservation is the in-band
/// memory Limit.
async fn request_body_bytes(req: &mut Request, cap: usize, too_big: &'static str) -> Result<Vec<u8>, Error> {
    let declared = req.headers().get("content-length").ok().flatten();
    declared_len_ok(declared.as_deref(), cap, too_big)?;
    let Some(raw) = req.inner().body() else {
        return Ok(Vec::new()); // no body — the same empty Vec array_buffer() produced
    };
    use futures_util::StreamExt as _;
    use wasm_bindgen::JsCast as _;
    let chunks = wasm_streams::ReadableStream::from_raw(raw.unchecked_into())
        .into_stream()
        .map(body_chunk);
    fill_capped(cap, too_big, chunks).await
}

/// The capped body read, generic over the chunk type so native tests drive
/// it with counting fakes: each chunk's `len()` is checked against the
/// running total BEFORE it is copied — an over-cap chunk is the caller's
/// Protocol error, never copied and never followed by another poll. An
/// admitted chunk's growth is reserved fallibly first (a refusal is the
/// in-band retry Limit), then copied into the new tail.
async fn fill_capped<C, S>(cap: usize, too_big: &'static str, stream: S) -> Result<Vec<u8>, Error>
where
    C: BodyChunk,
    S: Stream<Item = Result<C, Error>>,
{
    use futures_util::StreamExt as _;
    let mut body = Vec::new();
    let mut stream = std::pin::pin!(stream);
    while let Some(item) = stream.next().await {
        let chunk = item?;
        check_body_len(body.len().saturating_add(chunk.len()), cap, too_big)?;
        crate::try_grow::vec(&mut body, chunk.len())?;
        let start = body.len();
        body.resize(start + chunk.len(), 0);
        chunk.copy_into(&mut body[start..]);
    }
    Ok(body)
}

/// One stream chunk as its own Vec — the copy `BodyReader`'s stream
/// performs per item. The reservation is fallible BEFORE the copy, so a
/// refused allocation is the in-band retry Limit and the chunk is never
/// copied.
fn chunk_vec<C: BodyChunk>(c: &C) -> Result<Vec<u8>, Error> {
    let mut v = crate::try_grow::zeroed(c.len())?;
    c.copy_into(&mut v);
    Ok(v)
}

/// One item off a request-body ReadableStream as a Uint8Array — the
/// acceptance rule `request_body_bytes` and the R2 reader share: bytes
/// arrive as a Uint8Array or an ArrayBuffer; anything else is a Storage
/// error.
fn body_chunk(item: Result<wasm_bindgen::JsValue, wasm_bindgen::JsValue>) -> Result<js_sys::Uint8Array, Error> {
    use wasm_bindgen::JsCast as _;
    let v = item.map_err(|e| Error::Storage(format!("body stream: {e:?}")))?;
    Ok(match v.dyn_into::<js_sys::Uint8Array>() {
        Ok(a) => a,
        Err(v) if v.is_instance_of::<js_sys::ArrayBuffer>() => js_sys::Uint8Array::new(&v),
        Err(v) => return Err(Error::Storage(format!("body chunk is not bytes: {v:?}"))),
    })
}

/// One stream item as the chunk Vec `BodyReader` buffers.
fn body_vec(item: Result<wasm_bindgen::JsValue, wasm_bindgen::JsValue>) -> Result<Vec<u8>, Error> {
    chunk_vec(&body_chunk(item)?)
}

/// The request body as a chunked byte stream (section 6). gzip is decoded through a
/// `DecompressionStream` when `Content-Encoding: gzip` is set; other encodings are rejected.
pub struct BodyReader {
    stream: Pin<Box<dyn Stream<Item = Result<Vec<u8>, Error>>>>,
    buf: Vec<u8>,
    eof: bool,
    pub total: u64,
    /// The identity-encoded body's declared Content-Length — evidence of
    /// how many bytes (and so how many pack entries) can follow. None for
    /// gzip, where the wire length says nothing about the decoded size,
    /// and for chunked uploads that declare no length.
    pub declared_len: Option<u64>,
}
impl BodyReader {
    pub fn new(req: &mut Request) -> Result<Self, Error> {
        let enc = req.headers().get("content-encoding").ok().flatten();
        let gzip = enc.as_deref().map(|e| e.eq_ignore_ascii_case("gzip")).unwrap_or(false);
        if !gzip {
            match enc.as_deref() {
                None | Some("identity") => {}
                Some(e) => return Err(Error::Protocol(format!("unsupported content-encoding {e}"))),
            }
        }
        if gzip {
            let raw = req
                .inner()
                .body()
                .ok_or_else(|| Error::Protocol("missing request body".into()))?;
            let ds = web_sys::DecompressionStream::new(web_sys::CompressionFormat::Gzip)
                .map_err(|e| Error::Internal(format!("DecompressionStream: {e:?}")))?;
            let body = raw.pipe_through(wasm_bindgen::JsCast::unchecked_ref::<web_sys::ReadableWritablePair>(&ds));
            let stream = wasm_streams::ReadableStream::from_raw(body).into_stream().map(body_vec);
            return Ok(Self::wrap(stream));
        }
        let declared_len = req
            .headers()
            .get("content-length")
            .ok()
            .flatten()
            .and_then(|v| v.trim().parse::<u64>().ok());
        let raw = req
            .inner()
            .body()
            .ok_or_else(|| Error::Protocol("no body for request".into()))?;
        let stream = wasm_streams::ReadableStream::from_raw(raw).into_stream().map(body_vec);
        let mut r = Self::wrap(stream);
        r.declared_len = declared_len;
        Ok(r)
    }
    /// Test-only: a reader over in-memory chunks, so pass A runs natively.
    #[cfg(test)]
    pub(crate) fn from_chunks(chunks: Vec<Vec<u8>>, declared_len: Option<u64>) -> Self {
        let mut r = Self::wrap(futures_util::stream::iter(chunks.into_iter().map(Ok)));
        r.declared_len = declared_len;
        r
    }
    fn wrap<S>(stream: S) -> Self
    where
        S: Stream<Item = Result<Vec<u8>, Error>> + 'static,
    {
        Self { stream: Box::pin(stream), buf: Vec::new(), eof: false, total: 0, declared_len: None }
    }
    /// Read until buf.len() >= min or EOF. Ok(false) means EOF before min.
    pub async fn fill(&mut self, min: usize) -> Result<bool, Error> {
        while self.buf.len() < min && !self.eof {
            match self.stream.next().await {
                Some(Ok(chunk)) => {
                    self.total = self.total.saturating_add(chunk.len() as u64);
                    crate::try_grow::vec(&mut self.buf, chunk.len())?;
                    self.buf.extend_from_slice(&chunk);
                }
                Some(Err(e)) => return Err(e),
                None => self.eof = true,
            }
        }
        Ok(self.buf.len() >= min)
    }
    pub fn buffered(&self) -> &[u8] {
        &self.buf
    }
    pub fn consume(&mut self, n: usize) {
        let n = n.min(self.buf.len());
        self.buf.drain(..n);
    }
    /// Put bytes back at the front (the PktReader's remainder after the receive header).
    pub fn unread(&mut self, bytes: Vec<u8>) {
        if !bytes.is_empty() {
            let mut b = bytes;
            b.extend_from_slice(&self.buf);
            self.buf = b;
        }
    }
    /// Consume the rest of the request body, bounded. On error paths the response
    /// must not go out while the client is still uploading — Cloudflare resets the
    /// connection and the client sees a transport failure instead of our report-status.
    /// Bounded by bytes AND wall-clock: a stalled or slow-drip client could otherwise
    /// pin the isolate forever — idle awaits don't burn the cpu_ms budget.
    pub async fn drain(&mut self) {
        const MAX_DRAIN: u64 = 2 << 30; // the same ceiling a pushed pack gets
        const IDLE_MS: u64 = 15_000; // per-chunk stall bound, not total
        self.buf.clear();
        while self.total < MAX_DRAIN && !self.eof {
            let next = self.stream.next();
            let timeout = worker::Delay::from(std::time::Duration::from_millis(IDLE_MS));
            futures_util::pin_mut!(next, timeout);
            match futures_util::future::select(next, timeout).await {
                futures_util::future::Either::Left((Some(Ok(chunk)), _)) => {
                    self.total = self.total.saturating_add(chunk.len() as u64);
                }
                // stream end, read error, or stalled past IDLE_MS — answer anyway
                _ => break,
            }
        }
    }
}

/// The one public entry point (lib.rs #[event(fetch)]).
pub async fn fetch(req: Request, env: Env) -> worker::Result<Response> {
    // wasm-bindgen-futures never settles the request promise after a panic —
    // the runtime then cancels the invocation as "hung". Log the panic message
    // (never request data) so the console shows the real cause. wasm-only:
    // native test builds keep the default hook. Installed once — every fetch
    // used to re-box the hook and drop the old one.
    #[cfg(target_arch = "wasm32")]
    {
        static PANIC_HOOK: std::sync::Once = std::sync::Once::new();
        PANIC_HOOK.call_once(|| {
            std::panic::set_hook(Box::new(|info| {
                worker::console_error!("git-edge panic: {info}");
            }));
        });
    }
    let started = platform::now_ms();
    // Request-wide subrequest tally: every ReqBudget the request creates reports
    // into it, so respond() stamps x-ge-subrequests even on error paths where the
    // charging budget was already dropped. Failures before the first stub/R2 call
    // (route parse, rejected credentials) report 0.
    let spend: Spend = Rc::new(Cell::new(0));
    let path = req.path();
    if path == "/healthz" {
        return Response::ok("ok");
    }
    let (route, rest) = match RepoRoute::parse(&path) {
        Ok(x) => x,
        Err(e) => return respond(Err(e), false, spend.get()),
    };
    // section 10: git-protocol POSTs get a pkt-line ERR; info/refs and _state get plain text
    let git_pkt = matches!((&req.method(), rest.as_str()), (Method::Post, "git-upload-pack" | "git-receive-pack"));
    let (op, repo) = (op_name(&req.method(), &rest), route.name());
    // resolved before the handlers move `env` — absent in local dev, never fatal
    let ds = env.analytics_engine("GE_METRICS").ok();
    let r = match (req.method(), rest.as_str()) {
        (Method::Get, "info/refs") => info_refs(&req, &env, &route, &spend).await,
        (Method::Get, "_state") => state_probe(&req, &env, &route, &spend).await,
        (Method::Post, "git-upload-pack") => upload_pack(req, &env, &route, &spend).await,
        (Method::Get, p) if p.starts_with("_packs/") => {
            pack_get(&req, &env, &route, &p["_packs/".len()..], &spend).await
        }
        (Method::Post, "info/lfs/objects/batch") => lfs_batch(req, &env, &route, &spend).await,
        (Method::Get, p) if p.starts_with("_lfs/") => {
            lfs_object(req, &env, &route, &p["_lfs/".len()..], "get", &spend).await
        }
        (Method::Put, p) if p.starts_with("_lfs/") => {
            lfs_object(req, &env, &route, &p["_lfs/".len()..], "put", &spend).await
        }
        (Method::Post, "git-receive-pack") => receive_pack(req, env, route, &spend).await,
        (Method::Post, "_admin/tokens") => token_create(req, &env, &route, &spend).await,
        (Method::Get, "_admin/tokens") => token_list(&req, &env, &route, &spend).await,
        (Method::Delete, p) if p.starts_with("_admin/tokens/") => {
            token_revoke(&req, &env, &route, &p["_admin/tokens/".len()..], &spend).await
        }
        (Method::Post, "_admin/delete") => repo_delete(&req, &env, &route, &spend).await,
        (Method::Post, "_admin/public") => repo_public(req, &env, &route, &spend).await,
        (Method::Post, "_admin/format") => repo_format(req, &env, &route, &spend).await,
        (Method::Post, "_admin/pin") => pin_ref(req, &env, &route, "/_do/pin", &spend).await,
        (Method::Post, "_admin/unpin") => pin_ref(req, &env, &route, "/_do/unpin", &spend).await,
        (Method::Post, "_admin/protect") => protect_ref(req, &env, &route, "/_do/protect", &spend).await,
        (Method::Post, "_admin/unprotect") => protect_ref(req, &env, &route, "/_do/unprotect", &spend).await,
        (Method::Post, "_admin/import/stage") => import_stage(req, &env, &route, &spend).await,
        (Method::Post, "_admin/import") => import_begin(req, &env, &route, &spend).await,
        (Method::Get, p) if p.starts_with("_admin/import/") => {
            import_status(&req, &env, &route, &p["_admin/import/".len()..], &spend).await
        }
        (Method::Get, "_admin/export") => export_bundle(&req, &env, &route, &spend).await,
        // PR0 test hooks: GE_TEST_HOOKS off = 404 exactly like an unknown route
        // (checked inside test_forward before admin auth, never reaching the DO)
        (Method::Post, "_admin/test/step") => test_forward(req, &env, &route, "/_do/test/step", &spend).await,
        (Method::Post, "_admin/test/failpoint") => test_forward(req, &env, &route, "/_do/test/failpoint", &spend).await,
        (Method::Post, "_admin/test/repair") => test_forward(req, &env, &route, "/_do/test/repair", &spend).await,
        (Method::Post, "_admin/test/drop_object") => test_forward(req, &env, &route, "/_do/test/drop_object", &spend).await,
        (Method::Post, "_admin/test/knob") => test_forward(req, &env, &route, "/_do/test/knob", &spend).await,
        (Method::Post, "_admin/test/meta") => test_forward(req, &env, &route, "/_do/test/meta", &spend).await,
        (Method::Post, "_admin/test/enqueue") => test_forward(req, &env, &route, "/_do/test/enqueue", &spend).await,
        (Method::Post, "_admin/test/marked_built") => test_forward(req, &env, &route, "/_do/test/marked_built", &spend).await,
        (Method::Post, "_admin/test/drop_pack") => test_forward(req, &env, &route, "/_do/test/drop_pack", &spend).await,
        (Method::Get, "_admin/test/jobs") => test_forward(req, &env, &route, "/_do/test/jobs", &spend).await,
        (Method::Get, "_admin/test/fsck") => test_forward(req, &env, &route, "/_do/test/fsck", &spend).await,
        // PR4 (G4a compaction): the lossless oracle + build/cohort jigs
        (Method::Get, "_admin/test/objects") => test_forward(req, &env, &route, "/_do/test/objects", &spend).await,
        (Method::Post, "_admin/test/pack_check") => test_forward(req, &env, &route, "/_do/test/pack_check", &spend).await,
        (Method::Post, "_admin/test/pack_state") => test_forward(req, &env, &route, "/_do/test/pack_state", &spend).await,
        (Method::Post, "_admin/test/kill_job") => test_forward(req, &env, &route, "/_do/test/kill_job", &spend).await,
        (Method::Post, "_admin/test/query") => test_forward(req, &env, &route, "/_do/test/query", &spend).await,
        // PR5 §8 oracle — GET (no _admin/test prefix per spec), same hooks gate
        (Method::Get, "_admin/mark_check") => test_forward(req, &env, &route, "/_do/test/mark_check", &spend).await,
        _ => Err(Error::NotFound),
    };
    let resp = respond(r, git_pkt, spend.get());
    metric(ds.as_ref(), &repo, op, started, resp.as_ref().ok());
    resp
}

fn op_name(method: &Method, rest: &str) -> &'static str {
    match (method, rest) {
        (Method::Get, "info/refs") => "info-refs",
        (Method::Get, "_state") => "state",
        (Method::Post, "git-upload-pack") => "fetch",
        (Method::Post, "git-receive-pack") => "push",
        (Method::Get, p) if p.starts_with("_packs/") => "pack-uri",
        (_, p) if p.starts_with("_admin/") => "admin",
        _ => "other",
    }
}

/// One Analytics Engine datapoint per request when GE_METRICS is bound; absent in
/// local dev — never fatal. For streamed fetch responses the duration is
/// time-to-first-byte: the stream outlives the handler.
fn metric(
    ds: Option<&worker::AnalyticsEngineDataset>,
    repo: &str,
    op: &'static str,
    started: i64,
    resp: Option<&Response>,
) {
    let Some(ds) = ds else {
        return;
    };
    let status = resp.map(|r| f64::from(r.status_code())).unwrap_or(0.0);
    let subreqs = resp
        .and_then(|r| r.headers().get("x-ge-subrequests").ok().flatten())
        .and_then(|v| v.split('/').next().and_then(|n| n.parse::<f64>().ok()))
        .unwrap_or(0.0);
    let _ = worker::AnalyticsEngineDataPointBuilder::new()
        .indexes([repo])
        .blobs([op])
        .doubles([status, (platform::now_ms().saturating_sub(started)) as f64, subreqs])
        .write_to(ds);
}

fn git_resp(body: Vec<u8>, content_type: &str) -> Result<Response, Error> {
    let h = worker::Headers::new();
    h.set("Content-Type", content_type).map_err(|e| Error::Internal(e.to_string()))?;
    h.set("Cache-Control", "no-cache").map_err(|e| Error::Internal(e.to_string()))?;
    Ok(Response::from_bytes(body).map_err(|e| Error::Internal(e.to_string()))?.with_headers(h))
}

/// Same as git_resp but streams the body — the DO's fetch response is a stream and must
/// stay one: buffering it would hold an entire clone's pack in edge memory.
fn git_resp_stream<S>(stream: S, content_type: &str) -> Result<Response, Error>
where
    S: Stream<Item = Result<Vec<u8>, worker::Error>> + 'static,
{
    let h = worker::Headers::new();
    h.set("Content-Type", content_type).map_err(|e| Error::Internal(e.to_string()))?;
    h.set("Cache-Control", "no-cache").map_err(|e| Error::Internal(e.to_string()))?;
    Response::from_stream(stream)
        .map(|r| r.with_headers(h))
        .map_err(|e| Error::Internal(e.to_string()))
}

fn protocol_version(req: &Request) -> Result<Option<u8>, Error> {
    Ok(req
        .headers()
        .get("git-protocol")
        .map_err(|e| Error::Internal(e.to_string()))?
        .and_then(|v| {
            v.split(':')
                .filter_map(|p| p.trim().strip_prefix("version=").and_then(|n| n.parse::<u8>().ok()))
                .next()
        }))
}

/// GET /:owner/:repo/info/refs?service=... — v2 advertisement, else v0/v1 (1.1 rules 5, 7).
async fn info_refs(req: &Request, env: &Env, route: &RepoRoute, spend: &Spend) -> Result<Response, Error> {
    let url = req.url().map_err(|e| Error::Internal(e.to_string()))?;
    let service = url.query_pairs().find(|(k, _)| k == "service").map(|(_, v)| v.into_owned());
    let (service, level, ct) = match service.as_deref() {
        Some("git-upload-pack") => (Service::UploadPack { v1: protocol_version(req)? == Some(1) }, Level::Read, "application/x-git-upload-pack-advertisement"),
        Some("git-receive-pack") => (Service::ReceivePack, Level::Write, "application/x-git-receive-pack-advertisement"),
        _ => return Err(Error::Protocol("service must be git-upload-pack or git-receive-pack".into())),
    };
    auth::authenticate(req, env, level, route, spend).await?; // before the DO wakes (8.1)
    let mut w = PktWriter::default();
    // the DO answers head+refs+the repo's object-format; v2 needs the format for
    // its capability lines too, so both protocol paths pay the one subrequest
    let (stub, mut budget) = (route.stub(env)?, ReqBudget::paid().reporting(spend));
    #[derive(serde::Deserialize)]
    struct RefDto {
        name: String,
        target: String,
        peeled: Option<String>,
    }
    #[derive(serde::Deserialize)]
    struct RefsDto {
        head: Option<String>,
        refs: Vec<RefDto>,
        obj_format: Option<String>,
        #[serde(default)]
        obj_format_pinned: bool,
    }
    let mut init = worker::RequestInit::new();
    init.with_method(Method::Get);
    let mut r = worker::Request::new_with_init("https://do/_do/refs", &init)
        .map_err(|e| Error::Internal(e.to_string()))?;
    route.apply_headers(&mut r)?;
    budget.charge(1)?;
    let mut resp = stub.fetch_with_request(r).await?;
    if resp.status_code() != 200 {
        return Err(Error::from_do_response(resp, &budget).await);
    }
    let refs_version = resp.headers().get("x-ge-refs-version").ok().flatten();
    let dto: RefsDto = resp.json().await.map_err(|e| Error::Internal(e.to_string()))?;
    let kind = match dto.obj_format.as_deref() {
        Some("sha256") => gix_hash::Kind::Sha256,
        _ => gix_hash::Kind::Sha1,
    };
    // unpinned: both formats — first write pins; pinned: the repo's own
    let formats: &[gix_hash::Kind] = if dto.obj_format_pinned {
        std::slice::from_ref(&kind)
    } else {
        const BOTH: [gix_hash::Kind; 2] = [gix_hash::Kind::Sha1, gix_hash::Kind::Sha256];
        &BOTH
    };
    if protocol_version(req)? == Some(2) && matches!(service, Service::UploadPack { .. }) {
        wire::write_capability_advertisement_v2(&mut w, formats);
    } else {
        let refs: Vec<wire::RefRow> = dto
            .refs
            .into_iter()
            .map(|r| {
                Ok(wire::RefRow {
                    name: r.name.into(),
                    target: ObjectId::from_hex(r.target.as_bytes())
                        .map_err(|e| Error::Internal(e.to_string()))?,
                    peeled: r
                        .peeled
                        .map(|p| ObjectId::from_hex(p.as_bytes()).map_err(|e| Error::Internal(e.to_string())))
                        .transpose()?,
                })
            })
            .collect::<Result<_, Error>>()?;
        wire::write_advertisement_v0(&mut w, service, dto.head.as_deref().map(|s| bstr::ByteSlice::as_bstr(s.as_bytes())), &refs, formats);
    }
    let mut out = git_resp(w.out, ct)?;
    if let Some(v) = refs_version {
        out.headers_mut()
            .set("x-ge-refs-version", &v)
            .map_err(|e| Error::Internal(e.to_string()))?;
    }
    Ok(out)
}

/// GET /:owner/:repo/_state — internal observability probe (write-token gated).
async fn state_probe(req: &Request, env: &Env, route: &RepoRoute, spend: &Spend) -> Result<Response, Error> {
    auth::authenticate(req, env, Level::Write, route, spend).await?;
    let (stub, mut budget) = (route.stub(env)?, ReqBudget::paid().reporting(spend));
    let mut init = worker::RequestInit::new();
    init.with_method(Method::Get);
    let mut r = worker::Request::new_with_init("https://do/_do/state", &init)
        .map_err(|e| Error::Internal(e.to_string()))?;
    route.apply_headers(&mut r)?;
    budget.charge(1)?;
    let mut resp = stub.fetch_with_request(r).await?;
    if resp.status_code() != 200 {
        return Err(Error::from_do_response(resp, &budget).await);
    }
    let bytes = resp.bytes().await.map_err(|e| Error::Internal(e.to_string()))?;
    git_resp(bytes, "application/json")
}

/// The upload-pack command body read whole under `cap`. The cap check runs
/// BEFORE each append and counts the bytes `fill` already pulled into the
/// buffer — checking the assembled body alone would let a chunked upload
/// overshoot by a whole fill step. Each append's growth is reserved
/// fallibly first: a refusal is the in-band retry Limit, over-cap the
/// upload-pack Limit.
async fn read_upload_body(reader: &mut BodyReader, cap: usize) -> Result<Vec<u8>, Error> {
    let mut body = Vec::new();
    while reader.fill(FILL_STEP).await? {
        if body.len().saturating_add(reader.buffered().len()) > cap {
            return Err(Error::Limit("upload-pack body > 1 MiB".into()));
        }
        crate::try_grow::vec(&mut body, reader.buffered().len())?;
        body.extend_from_slice(reader.buffered());
        reader.consume(usize::MAX);
    }
    if body.len().saturating_add(reader.buffered().len()) > cap {
        return Err(Error::Limit("upload-pack body > 1 MiB".into()));
    }
    crate::try_grow::vec(&mut body, reader.buffered().len())?;
    body.extend_from_slice(reader.buffered()); // EOF tail shorter than FILL_STEP
    Ok(body)
}

/// POST /:owner/:repo/git-upload-pack — v2 only; route on the command name, forward raw.
async fn upload_pack(mut req: Request, env: &Env, route: &RepoRoute, spend: &Spend) -> Result<Response, Error> {
    auth::authenticate(&req, env, Level::Read, route, spend).await?;
    if protocol_version(&req)? != Some(2) {
        // contract 1.1 rule 7: v0 upload-pack POST -> HTTP 400 with the ERR pkt-line
        let mut w = PktWriter::default();
        let _ = w.data(b"ERR protocol v2 required (git >= 2.26)\n");
        w.flush();
        return git_resp(w.out, "application/x-git-upload-pack-result")
            .map(|r| r.with_status(400));
    }
    // bound the read: a declared Content-Length is rejected before any byte is buffered,
    // and read_upload_body caps a chunked body at CMD_CAP while it streams in
    if let Some(n) = req
        .headers()
        .get("content-length")
        .ok()
        .flatten()
        .and_then(|v| v.parse::<u64>().ok())
    {
        if n > CMD_CAP as u64 {
            return Err(Error::Limit("upload-pack body > 1 MiB".into()));
        }
    }
    let mut reader = BodyReader::new(&mut req)?;
    let body = read_upload_body(&mut reader, CMD_CAP).await?;
    let path = match wire::parse_v2_command(&body)? {
        wire::V2Command::LsRefs(_) => "/_do/ls-refs",
        wire::V2Command::Fetch(_) => "/_do/fetch",
    };
    // the fetch may answer with packfile-uris URLs (A30) — the DO composes them
    // from the public origin, forwarded on the internal request as x-ge-base
    let base = req.url().ok().map(|u| u.origin().ascii_serialization());
    let (stub, mut budget) = (route.stub(env)?, ReqBudget::paid().reporting(spend));
    let mut resp = stub_raw(&stub, route, path, body, &mut budget, base.as_deref()).await?;
    if resp.status_code() != 200 {
        return Err(Error::from_do_response(resp, &budget).await);
    }
    // fetch responses carry immutable headers; rebuild around the same stream so the
    // pack flows through the edge instead of being buffered whole in memory — and
    // propagate the DO's subrequest accounting so clients see the real budget cost
    let subreqs = resp.headers().get("x-ge-subrequests").ok().flatten();
    let stream = resp.stream().map_err(|e| Error::Internal(e.to_string()))?;
    let mut out = git_resp_stream(stream, "application/x-git-upload-pack-result")?;
    if let Some(v) = subreqs {
        out.headers_mut()
            .set("x-ge-subrequests", &v)
            .map_err(|e| Error::Internal(e.to_string()))?;
    }
    Ok(out)
}

/// GET /:owner/:repo/_packs/<id>.pack?e=<exp>&s=<sig> — signed pack download (A30).
/// URI fetches carry no auth headers (packfile-uris sends the client through
/// `git http-fetch` bare), so the HMAC in `s` is the credential: it binds
/// repo+pack+expiry. No token check here by design — the DO only mints these
/// for fetches that already passed read auth.
async fn pack_get(req: &Request, env: &Env, _route: &RepoRoute, name: &str, spend: &Spend) -> Result<Response, Error> {
    let signing = crate::sign::signing_key(env).ok_or(Error::NotFound)?; // feature off: never minted, never served
    let pack = name
        .strip_suffix(".pack")
        .filter(|p| RepoRoute::seg_ok(p))
        .ok_or(Error::NotFound)?;
    let url = req.url().map_err(|e| Error::Internal(e.to_string()))?;
    let qp = |k: &str| {
        url.query_pairs()
            .find(|(q, _)| q == k)
            .map(|(_, v)| v.into_owned())
    };
    let exp: i64 = qp("e").and_then(|v| v.parse().ok()).ok_or(Error::Forbidden)?;
    if exp < platform::now_ms() / 1000 {
        return Err(Error::Forbidden);
    }
    // r= is the opaque repo_id the DO signed (the R2 namespace); the path's
    // owner/repo is routing sugar only — the sig is the authority
    let repo_id = qp("r").filter(|r| RepoRoute::seg_ok(r)).ok_or(Error::Forbidden)?;
    let sig = qp("s").ok_or(Error::Forbidden)?;
    if !crate::sign::pack_sig_ok(&signing, &repo_id, pack, exp, &sig) {
        return Err(Error::Forbidden);
    }
    let mut budget = ReqBudget::paid().reporting(spend);
    let key = crate::store::keys::pack(&RepoId(repo_id), &PackId(pack.into()));
    budget.charge(1)?;
    let obj = env
        .bucket("BUCKET")?
        .get(&key)
        .execute()
        .await?
        .ok_or(Error::NotFound)?;
    let body = obj.body().ok_or_else(|| Error::Storage(format!("no body for {key}")))?;
    let mut resp = Response::from_body(body.response_body()?).map_err(Error::from)?;
    let h = resp.headers_mut();
    h.set("Content-Type", "application/octet-stream")
        .map_err(|e| Error::Internal(e.to_string()))?;
    // the URL is a bearer credential — never let a cache serve it past expiry
    h.set("Cache-Control", "private, no-store")
        .map_err(|e| Error::Internal(e.to_string()))?;
    Ok(resp)
}

/// The lfs_batch gate as a seam: `auth` settles before `body` is invoked —
/// a failed auth returns its error and the body reader never runs. The
/// auth output comes back with the body so the caller can act on what the
/// resolution granted.
async fn auth_then_body<S, T, A, B, FB>(auth: A, body: B) -> Result<(S, T), Error>
where
    A: Future<Output = Result<S, Error>>,
    B: FnOnce() -> FB,
    FB: Future<Output = Result<T, Error>>,
{
    let s = auth.await?;
    let t = body().await?;
    Ok((s, t))
}

/// POST /:owner/:repo/info/lfs/objects/batch — Git LFS batch API (#21, basic
/// transfer). The operation picks the auth level: download needs read, upload
/// needs write. The DO does existence checks + mints the signed `/_lfs/` URLs.
async fn lfs_batch(mut req: Request, env: &Env, route: &RepoRoute, spend: &Spend) -> Result<Response, Error> {
    // one resolution before the body is read — the granted level decides
    // download-vs-upload after `operation` parses, with no second DO call.
    // Credentials come out synchronously so the resolve future holds no
    // borrow on `req`.
    let creds = auth::credentials(&req);
    let (grant, body) = auth_then_body(
        auth::resolve(creds, env, route, spend),
        || request_body_bytes(&mut req, 1_048_576, "lfs batch body too large"),
    )
    .await?;
    let v: serde_json::Value =
        serde_json::from_slice(&body).map_err(|e| Error::Protocol(format!("lfs batch: {e}")))?;
    let need = match v.get("operation").and_then(|o| o.as_str()) {
        Some("download") => Level::Read,
        Some("upload") => Level::Write,
        _ => return Err(Error::Protocol("lfs operation must be download or upload".into())),
    };
    auth::admit(grant, need)?;
    let (stub, mut budget) = (route.stub(env)?, ReqBudget::paid().reporting(spend));
    let base = req.url().ok().map(|u| u.origin().ascii_serialization());
    let mut resp = stub_raw(&stub, route, "/_do/lfs/batch", body, &mut budget, base.as_deref()).await?;
    let out = resp.bytes().await.map_err(|e| Error::Internal(e.to_string()))?;
    if resp.status_code() != 200 {
        return Err(Error::from_do_response(resp, &budget).await);
    }
    git_resp(out, "application/vnd.git-lfs+json")
}

/// GET|PUT /:owner/:repo/_lfs/<oid>?r=<repo_id>&e=<exp>&s=<sig> — signed LFS
/// object transfer (#21). Same capability model as `_packs/` (A30): the HMAC
/// binds repo_id + oid + expiry + op; no bearer check by design. `op=get`
/// streams the object; `op=put` writes it via RawWriter MPU.
async fn lfs_object(req: Request, env: &Env, _route: &RepoRoute, oid: &str, op: &str, spend: &Spend) -> Result<Response, Error> {
    let signing = crate::sign::signing_key(env).ok_or(Error::NotFound)?; // never minted, never served
    if oid.len() != 64 || !oid.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(Error::NotFound);
    }
    let url = req.url().map_err(|e| Error::Internal(e.to_string()))?;
    let qp = |k: &str| url.query_pairs().find(|(q, _)| q == k).map(|(_, v)| v.into_owned());
    let exp: i64 = qp("e").and_then(|v| v.parse().ok()).ok_or(Error::Forbidden)?;
    if exp < platform::now_ms() / 1000 {
        return Err(Error::Forbidden);
    }
    let repo_id = qp("r").filter(|r| RepoRoute::seg_ok(r)).ok_or(Error::Forbidden)?;
    let sig = qp("s").ok_or(Error::Forbidden)?;
    if !crate::sign::lfs_sig_ok(&signing, &repo_id, oid, exp, op, &sig) {
        return Err(Error::Forbidden);
    }
    let mut budget = ReqBudget::paid().reporting(spend);
    let key = crate::store::keys::lfs(&RepoId(repo_id.clone()), oid);
    let bucket = env.bucket("BUCKET")?;
    if op == "get" {
        budget.charge(1)?;
        let obj = bucket.get(&key).execute().await?.ok_or(Error::NotFound)?;
        let body = obj.body().ok_or_else(|| Error::Storage(format!("no body for {key}")))?;
        let mut resp = Response::from_body(body.response_body()?).map_err(Error::from)?;
        let h = resp.headers_mut();
        h.set("Content-Type", "application/octet-stream")
            .map_err(|e| Error::Internal(e.to_string()))?;
        h.set("Content-Length", &obj.size().to_string()).ok();
        h.set("Cache-Control", "private, no-store").ok();
        Ok(resp)
    } else {
        // put — stream the body through an MPU; the sig already vetted it
        let mut req = req;
        let mut body = BodyReader::new(&mut req)?;
        let bkt = Bucket::new(bucket, RepoId(repo_id));
        let mut out = crate::store::RawWriter::create(&bkt, key.clone(), &mut budget).await?;
        let mut err = None;
        while err.is_none() {
            match body.fill(FILL_STEP).await {
                Ok(_) => {
                    let n = body.buffered().len();
                    if n == 0 {
                        break;
                    }
                    if let Err(e) = out.append(body.buffered()) {
                        err = Some(e);
                        break;
                    }
                    body.consume(n);
                    if let Err(e) = out.flush_if_full(&mut budget).await {
                        err = Some(e);
                    }
                }
                Err(e) => err = Some(e),
            }
        }
        match err {
            Some(e) => {
                out.abort().await;
                Err(e)
            }
            None => {
                out.finish(&mut budget).await?;
                git_resp(Vec::new(), "application/vnd.git-lfs+json")
            }
        }
    }
}

/// POST /:owner/:repo/_admin/tokens {name, level} — mint a per-repo credential.
/// Global-write-token only (authenticate_admin): a repo-level token must not mint
/// more credentials. The token value is returned once and only its hash is stored.
async fn token_create(mut req: Request, env: &Env, route: &RepoRoute, spend: &Spend) -> Result<Response, Error> {
    auth::authenticate_admin(&req, env)?;
    // {name, level} needs a few hundred bytes; cap before buffering the body whole
    let body = request_body_bytes(&mut req, 65_536, "token create body too large").await?;
    let v: serde_json::Value =
        serde_json::from_slice(&body).map_err(|e| Error::Protocol(format!("token create body: {e}")))?;
    let (stub, mut budget) = (route.stub(env)?, ReqBudget::paid().reporting(spend));
    let out: serde_json::Value = stub_json(&stub, route, "/_do/tokens", &v, &mut budget).await?;
    git_resp(serde_json::to_vec(&out).map_err(|e| Error::Internal(e.to_string()))?, "application/json")
}

/// GET /:owner/:repo/_admin/tokens — list per-repo credentials (never the secrets).
async fn token_list(req: &Request, env: &Env, route: &RepoRoute, spend: &Spend) -> Result<Response, Error> {
    auth::authenticate_admin(req, env)?;
    let (stub, mut budget) = (route.stub(env)?, ReqBudget::paid().reporting(spend));
    let mut init = worker::RequestInit::new();
    init.with_method(Method::Get);
    let mut r = worker::Request::new_with_init("https://do/_do/tokens", &init)
        .map_err(|e| Error::Internal(e.to_string()))?;
    route.apply_headers(&mut r)?;
    budget.charge(1)?;
    let mut resp = stub.fetch_with_request(r).await?;
    if resp.status_code() != 200 {
        return Err(Error::from_do_response(resp, &budget).await);
    }
    let bytes = resp.bytes().await.map_err(|e| Error::Internal(e.to_string()))?;
    git_resp(bytes, "application/json")
}

/// DELETE /:owner/:repo/_admin/tokens/<id> — revoke a per-repo credential.
async fn token_revoke(req: &Request, env: &Env, route: &RepoRoute, id: &str, spend: &Spend) -> Result<Response, Error> {
    auth::authenticate_admin(req, env)?;
    if id.is_empty() || id.len() > 64 || !id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_') {
        return Err(Error::Protocol("bad token id".into()));
    }
    let (stub, mut budget) = (route.stub(env)?, ReqBudget::paid().reporting(spend));
    let out: serde_json::Value = stub_json(
        &stub,
        route,
        "/_do/tokens/revoke",
        &serde_json::json!({ "id": id }),
        &mut budget,
    )
    .await?;
    git_resp(serde_json::to_vec(&out).map_err(|e| Error::Internal(e.to_string()))?, "application/json")
}

/// Bounded JSON body for the small _admin payloads (public/pin/unpin).
async fn json_body(req: &mut Request) -> Result<serde_json::Value, Error> {
    let body = request_body_bytes(req, 4_096, "admin body too large").await?;
    serde_json::from_slice(&body).map_err(|e| Error::Protocol(format!("admin body: {e}")))
}

/// POST /:owner/:repo/_admin/delete — tombstone the repo and enqueue purge_repo.
/// Every repo route answers 410 from the moment this returns. Idempotent.
async fn repo_delete(req: &Request, env: &Env, route: &RepoRoute, spend: &Spend) -> Result<Response, Error> {
    auth::authenticate_admin(req, env)?;
    let (stub, mut budget) = (route.stub(env)?, ReqBudget::paid().reporting(spend));
    let out: serde_json::Value =
        stub_json(&stub, route, "/_do/delete", &serde_json::json!({}), &mut budget).await?;
    owner_release(env, route).await; // free the A26 quota slot; best-effort — tombstone already landed
    git_resp(serde_json::to_vec(&out).map_err(|e| Error::Internal(e.to_string()))?, "application/json")
}

/// POST /:owner/:repo/_admin/public {enabled: bool} — anonymous read flag.
/// Write paths (receive-pack, token minting) stay token-gated regardless.
async fn repo_public(mut req: Request, env: &Env, route: &RepoRoute, spend: &Spend) -> Result<Response, Error> {
    auth::authenticate_admin(&req, env)?;
    let v = json_body(&mut req).await?;
    if v.get("enabled").and_then(|b| b.as_bool()).is_none() {
        return Err(Error::Protocol("body must be {\"enabled\": bool}".into()));
    }
    let (stub, mut budget) = (route.stub(env)?, ReqBudget::paid().reporting(spend));
    let out: serde_json::Value = stub_json(&stub, route, "/_do/public", &v, &mut budget).await?;
    git_resp(serde_json::to_vec(&out).map_err(|e| Error::Internal(e.to_string()))?, "application/json")
}

/// POST /:owner/:repo/_admin/format {object_format} — pin sha1|sha256 before the
/// first write. How a sha256 repo is born for v0 push: pin first, then push —
/// the advertised object-format cap names the pinned algo.
async fn repo_format(mut req: Request, env: &Env, route: &RepoRoute, spend: &Spend) -> Result<Response, Error> {
    auth::authenticate_admin(&req, env)?;
    let v = json_body(&mut req).await?;
    let (stub, mut budget) = (route.stub(env)?, ReqBudget::paid().reporting(spend));
    let out: serde_json::Value = stub_json(&stub, route, "/_do/format", &v, &mut budget).await?;
    git_resp(serde_json::to_vec(&out).map_err(|e| Error::Internal(e.to_string()))?, "application/json")
}

/// POST /:owner/:repo/_admin/pin {ref, sha} / _admin/unpin {ref} — ref pinning.
async fn pin_ref(mut req: Request, env: &Env, route: &RepoRoute, path: &str, spend: &Spend) -> Result<Response, Error> {
    auth::authenticate_admin(&req, env)?;
    let v = json_body(&mut req).await?;
    if v.get("ref").and_then(|r| r.as_str()).is_none()
        || (path == "/_do/pin" && v.get("sha").and_then(|s| s.as_str()).is_none())
    {
        return Err(Error::Protocol("body must be {\"ref\": ..., \"sha\": ...}".into()));
    }
    let (stub, mut budget) = (route.stub(env)?, ReqBudget::paid().reporting(spend));
    let out: serde_json::Value = stub_json(&stub, route, path, &v, &mut budget).await?;
    git_resp(serde_json::to_vec(&out).map_err(|e| Error::Internal(e.to_string()))?, "application/json")
}

/// POST /:owner/:repo/_admin/protect {ref} / _admin/unprotect {ref} — protected
/// ref patterns (fast-forward-only, undeletable). Global admin only.
async fn protect_ref(mut req: Request, env: &Env, route: &RepoRoute, path: &str, spend: &Spend) -> Result<Response, Error> {
    auth::authenticate_admin(&req, env)?;
    let v = json_body(&mut req).await?;
    if v.get("ref").and_then(|r| r.as_str()).is_none() {
        return Err(Error::Protocol("body must be {\"ref\": ...}".into()));
    }
    let (stub, mut budget) = (route.stub(env)?, ReqBudget::paid().reporting(spend));
    let out: serde_json::Value = stub_json(&stub, route, path, &v, &mut budget).await?;
    git_resp(serde_json::to_vec(&out).map_err(|e| Error::Internal(e.to_string()))?, "application/json")
}

/// POST /:owner/:repo/_admin/import/stage — stream a client-assembled pack into
/// pending/<push> untouched (I1). No parse at the edge: the import job's pass A
/// re-reads it from R2 across slices, which is the point — a pack this large
/// cannot fit receive-pack's single-request ingest. Returns {push, key, bytes}
/// for the follow-up _admin/import call.
async fn import_stage(mut req: Request, env: &Env, route: &RepoRoute, spend: &Spend) -> Result<Response, Error> {
    let principal = auth::authenticate(&req, env, Level::Write, route, spend).await?;
    let rate_key = auth::presented_hash(&req)?;
    let (stub, mut budget) = (route.stub(env)?, ReqBudget::paid().reporting(spend));
    // I1: parts of one import share ONE open push — `?push=<id>` re-begins it
    // (idempotent for the same principal) and `?part=<name>` picks the key
    // `pending/<push>.part-<name>`. Without reuse every part is its own open push
    // whose PUSH_TIMEOUT expiry would sweep sibling parts mid-import.
    let url = req.url().map_err(|e| Error::Internal(e.to_string()))?;
    let qp = |k: &str| url.query_pairs().find(|(n, _)| n == k).map(|(_, v)| v.into_owned());
    let ok_id = |s: &str| {
        !s.is_empty()
            && s.len() <= 64
            && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    };
    let (push, part) = match qp("push") {
        Some(id) => {
            if !ok_id(&id) {
                return Err(Error::Protocol("bad push id".into()));
            }
            let part = qp("part").filter(|p| ok_id(p)).ok_or_else(|| {
                Error::Protocol("reused push needs ?part=<name>".into())
            })?;
            (PushId(id), Some(part))
        }
        None => (PushId::random()?, None),
    };
    #[derive(serde::Deserialize)]
    struct Begin {
        repo_id: String,
        #[serde(default)]
        claimed: bool,
    }
    let begin: Begin = stub_json(
        &stub,
        route,
        "/_do/push/begin",
        &serde_json::json!({
            "push_id": push.0, "principal": principal, "key": rate_key,
            "admin": auth::is_admin(&req, env),
        }),
        &mut budget,
    )
    .await?;
    // same A26 claim as receive_inner: an unclaimed repo must take a slot before
    // the staged upload burns bandwidth
    if !begin.claimed {
        if let Err(e) = owner_claim(env, route, &mut budget).await {
            let _: serde_json::Value = stub_json(
                &stub,
                route,
                "/_do/push/abort",
                &serde_json::json!({ "push_id": push.0 }),
                &mut budget,
            )
            .await
            .unwrap_or_default();
            return Err(e);
        }
    }
    let bucket = Bucket::new(env.bucket("BUCKET")?, RepoId(begin.repo_id));
    let key = match &part {
        Some(p) => crate::store::keys::pending_part(&bucket.repo, &push, p),
        None => crate::store::keys::pending(&bucket.repo, &push),
    };
    let mut body = BodyReader::new(&mut req)?;
    let mut out =
        crate::store::RawWriter::create(&bucket, key.clone(), &mut budget)
            .await?;
    let mut total = 0u64;
    let mut err = None;
    while err.is_none() {
        match body.fill(FILL_STEP).await {
            Ok(_) => {
                let n = body.buffered().len();
                if n == 0 {
                    break;
                }
                if let Err(e) = out.append(body.buffered()) {
                    err = Some(e);
                    break;
                }
                body.consume(n);
                total = total.saturating_add(n as u64);
                if let Err(e) = out.flush_if_full(&mut budget).await {
                    err = Some(e);
                }
            }
            Err(e) => err = Some(e),
        }
    }
    match err {
        Some(e) => {
            out.abort().await;
            let _: serde_json::Value = stub_json(
                &stub,
                route,
                "/_do/push/abort",
                &serde_json::json!({ "push_id": push.0 }),
                &mut budget,
            )
            .await
            .unwrap_or_default();
            Err(e)
        }
        // finish() aborts its own MPU on failure — only the pushes row needs closing
        None => match out.finish(&mut budget).await {
            Ok(()) => git_resp(
                serde_json::to_vec(&serde_json::json!({
                    "push": push.0,
                    "key": key,
                    "bytes": total,
                }))
                .map_err(|e| Error::Internal(e.to_string()))?,
                "application/json",
            ),
            Err(e) => {
                let _: serde_json::Value = stub_json(
                    &stub,
                    route,
                    "/_do/push/abort",
                    &serde_json::json!({ "push_id": push.0 }),
                    &mut budget,
                )
                .await
                .unwrap_or_default();
                Err(e)
            }
        },
    }
}

/// Bounded body for _admin/import — commands for a many-ref import run to a few
/// hundred KB, so this gets its own cap rather than json_body's 4 KiB.
async fn import_body(req: &mut Request) -> Result<serde_json::Value, Error> {
    let body = request_body_bytes(req, 4 << 20, "import body too large").await?;
    serde_json::from_slice(&body).map_err(|e| Error::Protocol(format!("import body: {e}")))
}

/// POST /:owner/:repo/_admin/import {push, parts:[{key,bytes}], commands:[{old,new,name,peeled?}]}
/// — point a queued import_pack job at the staged pack(s) (I1). The DO validates
/// that every part lives under this repo's pending/ namespace; the job rides the
/// stage call's open push to the same commit_push a live push runs.
async fn import_begin(mut req: Request, env: &Env, route: &RepoRoute, spend: &Spend) -> Result<Response, Error> {
    let principal = auth::authenticate(&req, env, Level::Write, route, spend).await?;
    let v = import_body(&mut req).await?;
    let (stub, mut budget) = (route.stub(env)?, ReqBudget::paid().reporting(spend));
    // WILD url import — {url} instead of {push,parts,commands}: the job's fetch
    // phase acts as the git client, stages the remote's pack in pending/, and
    // mints the ref commands itself. Same push lifecycle: begin → claim → start.
    if let Some(url) = v.get("url").and_then(|u| u.as_str()) {
        let url = crate::remote::normalize(url)?; // 400 here beats a rejected push later
        let rate_key = auth::presented_hash(&req)?;
        let (push, pack) = (PushId::random()?, PackId::random()?);
        #[derive(serde::Deserialize)]
        struct Begin {
            #[serde(default)]
            claimed: bool,
        }
        let begin: Begin = stub_json(
            &stub,
            route,
            "/_do/push/begin",
            &serde_json::json!({
            "push_id": push.0, "principal": principal, "key": rate_key,
            "admin": auth::is_admin(&req, env),
        }),
            &mut budget,
        )
        .await?;
        if !begin.claimed {
            if let Err(e) = owner_claim(env, route, &mut budget).await {
                let _: serde_json::Value = stub_json(
                    &stub,
                    route,
                    "/_do/push/abort",
                    &serde_json::json!({ "push_id": push.0 }),
                    &mut budget,
                )
                .await
                .unwrap_or_default();
                return Err(e);
            }
        }
        // a refused start must not leak the internal open push row — close it
        // out exactly like an owner_claim failure above
        let out: serde_json::Value = match stub_json(
            &stub,
            route,
            "/_do/import/start",
            &serde_json::json!({ "push": push.0, "pack": pack.0, "url": url }),
            &mut budget,
        )
        .await
        {
            Ok(v) => v,
            Err(e) => {
                let _: serde_json::Value = stub_json(
                    &stub,
                    route,
                    "/_do/push/abort",
                    &serde_json::json!({ "push_id": push.0 }),
                    &mut budget,
                )
                .await
                .unwrap_or_default();
                return Err(e);
            }
        };
        return git_resp(serde_json::to_vec(&out).map_err(|e| Error::Internal(e.to_string()))?, "application/json");
    }
    let ok_id = |s: &str| !s.is_empty() && s.len() <= 64 && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_');
    let push = v
        .get("push")
        .and_then(|p| p.as_str())
        .filter(|p| ok_id(p))
        .ok_or_else(|| Error::Protocol("import needs a push id".into()))?;
    let parts = v.get("parts").and_then(|p| p.as_array()).filter(|p| !p.is_empty()).ok_or_else(|| {
        Error::Protocol("import needs parts:[{key,bytes}]".into())
    })?;
    for part in parts {
        if part.get("key").and_then(|k| k.as_str()).is_none()
            || part.get("bytes").and_then(|b| b.as_u64()).is_none()
        {
            return Err(Error::Protocol("part needs {key,bytes}".into()));
        }
    }
    let commands = v.get("commands").and_then(|c| c.as_array()).filter(|c| !c.is_empty()).ok_or_else(|| {
        Error::Protocol("import needs commands".into())
    })?;
    for c in commands {
        for f in ["old", "new", "name"] {
            if c.get(f).and_then(|x| x.as_str()).is_none() {
                return Err(Error::Protocol(format!("command needs {f}")));
            }
        }
    }
    let pack = PackId::random()?;
    let out: serde_json::Value = stub_json(
        &stub,
        route,
        "/_do/import/start",
        &serde_json::json!({
            "push": push,
            "pack": pack.0,
            "parts": parts,
            "principal": principal,
            "commands": commands,
            "format": v.get("format"),
        }),
        &mut budget,
    )
    .await?;
    git_resp(serde_json::to_vec(&out).map_err(|e| Error::Internal(e.to_string()))?, "application/json")
}

/// GET /:owner/:repo/_admin/import/<push> — job/push progress for a staged import.
async fn import_status(req: &Request, env: &Env, route: &RepoRoute, push: &str, spend: &Spend) -> Result<Response, Error> {
    auth::authenticate(req, env, Level::Write, route, spend).await?;
    if push.is_empty() || push.len() > 64 || !push.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_') {
        return Err(Error::Protocol("bad push id".into()));
    }
    let (stub, mut budget) = (route.stub(env)?, ReqBudget::paid().reporting(spend));
    let out: serde_json::Value =
        stub_json(&stub, route, "/_do/import/status", &serde_json::json!({ "push": push }), &mut budget).await?;
    git_resp(serde_json::to_vec(&out).map_err(|e| Error::Internal(e.to_string()))?, "application/json")
}

/// PR0: `/:o/:r/_admin/test/*` — the deterministic GC harness (testhooks.rs).
/// GE_TEST_HOOKS != "1" answers NotFound exactly like an unknown route, before
/// auth even runs; on, the routes take the same global-write-token gate as the
/// other _admin routes and forward verbatim to the DO's /_do/test/* handlers.
/// fsck is a GET carrying ?mode= — the DO route reads it from a JSON body.
async fn test_forward(mut req: Request, env: &Env, route: &RepoRoute, path: &str, spend: &Spend) -> Result<Response, Error> {
    if !crate::testhooks::env_flag(env, "GE_TEST_HOOKS") {
        return Err(Error::NotFound);
    }
    auth::authenticate_admin(&req, env)?;
    let body = if path == "/_do/test/fsck" {
        let mode = req
            .url()
            .ok()
            .and_then(|u| u.query_pairs().find(|(k, _)| k == "mode").map(|(_, v)| v.into_owned()))
            .unwrap_or_else(|| "refs".into());
        serde_json::to_vec(&serde_json::json!({ "mode": mode })).map_err(|e| Error::Internal(e.to_string()))?
    } else {
        request_body_bytes(&mut req, 1 << 20, "test body too large").await?
    };
    let (stub, mut budget) = (route.stub(env)?, ReqBudget::paid().reporting(spend));
    let mut resp = stub_raw(&stub, route, path, body, &mut budget, None).await?;
    if resp.status_code() != 200 {
        return Err(Error::from_do_response(resp, &budget).await);
    }
    let bytes = resp.bytes().await.map_err(|e| Error::Internal(e.to_string()))?;
    git_resp(bytes, "application/json")
}

/// GET /:owner/:repo/_admin/export — stream a v3 git bundle of every live ref.
/// Read-level auth suffices (anonymous on a public repo); the DO builds the pack
/// with the same send_set machinery as fetch, framed by the bundle header.
async fn export_bundle(req: &Request, env: &Env, route: &RepoRoute, spend: &Spend) -> Result<Response, Error> {
    auth::authenticate(req, env, Level::Read, route, spend).await?;
    let (stub, mut budget) = (route.stub(env)?, ReqBudget::paid().reporting(spend));
    let mut resp = stub_raw(&stub, route, "/_do/export", Vec::new(), &mut budget, None).await?;
    if resp.status_code() != 200 {
        return Err(Error::from_do_response(resp, &budget).await);
    }
    let subreqs = resp.headers().get("x-ge-subrequests").ok().flatten();
    let stream = resp.stream().map_err(|e| Error::Internal(e.to_string()))?;
    let mut out = git_resp_stream(stream, "application/x-git-bundle")?;
    if let Some(v) = subreqs {
        out.headers_mut()
            .set("x-ge-subrequests", &v)
            .map_err(|e| Error::Internal(e.to_string()))?;
    }
    Ok(out)
}

/// POST /:owner/:repo/git-receive-pack — two-phase push (2.4, 3) with the A2 error arm.
async fn receive_pack(mut req: Request, env: Env, route: RepoRoute, spend: &Spend) -> Result<Response, Error> {
    let principal = auth::authenticate(&req, &env, Level::Write, &route, spend).await?;
    // the pass-B memory guard's "is the isolate busy" count — pass A isn't
    // admission-gated but still holds heap, so the whole handler counts.
    // Created only AFTER auth: an unauthenticated request must not count as
    // an in-progress receive-pack peer (it would skew busy-vs-slices).
    let receive_pack_guard = pack::run::ReceivePackGuard::new();
    // A27: sha1 of the presented token is the rate-limit bucket key — the raw
    // credential never crosses the stub boundary (8.1)
    let rate_key = auth::presented_hash(&req)?;
    // PR-3a: the DO needs to know a tokens-row miss is the global admin, not a
    // revoked repo token — computed before the body borrows the request
    let admin = auth::is_admin(&req, &env);
    let mut body = BodyReader::new(&mut req)?;

    // ---- pre-header: parse errors are still normal HTTP errors (A2 arm not yet active) ----
    let (hdr, leftover) = match receive_header(&mut body).await {
        Ok(x) => x,
        Err(e) => {
            // let the client finish uploading first — answering mid-upload gets the
            // connection reset and the client sees a transport error, not our message
            body.drain().await;
            return Err(e);
        }
    };
    body.unread(leftover);

    // flush-only request: "everything up-to-date" — no pack follows, no report needed
    if hdr.commands.is_empty() {
        let mut w = PktWriter::default();
        w.flush();
        return git_resp(w.out, "application/x-git-receive-pack-result");
    }

    // ---- post-header (A2): everything from here reports HTTP 200 + report-status ----
    let lease = receive_pack_guard.lease();
    match receive_inner(&mut body, &env, &route, &hdr, &principal, &rate_key, admin, spend, lease).await {
        Ok(resp) => Ok(resp),
        Err(e) => {
            // A27/G2b: a throttled or over-cap push answers a real HTTP
            // 429/503 + Retry-After, not an in-band unpack error — and skips
            // the drain, since shedding load is the point (a client still
            // mid-upload may see a reset instead of the status).
            if matches!(e, Error::RateLimit(_) | Error::Busy(_)) {
                return Err(e);
            }
            body.drain().await; // finish the client's upload before answering (see drain)
            report_status_200(&hdr, Err(e.client_message()), &[])
        }
    }
}

/// Parse the receive-pack command header: pkt-lines up to the first flush, then the
/// pack follows. Re-parses the whole accumulated buffer each round (bounded by CMD_CAP)
/// because parse_receive_header's accumulators don't survive an Incomplete result.
async fn receive_header(body: &mut BodyReader) -> Result<(wire::ReceiveHeader, Vec<u8>), Error> {
    let mut pr = PktReader::default();
    loop {
        let mut fresh = PktReader::default();
        fresh.push(&pr.buf);
        match wire::parse_receive_header(&mut fresh)? {
            Some(h) => return Ok((h, fresh.remainder())),
            None => {
                body.fill(FILL_STEP).await?;
                if body.buffered().is_empty() {
                    return Err(Error::Protocol("truncated receive header".into()));
                }
                pr.push(&body.buffered().to_vec());
                body.consume(usize::MAX);
                if pr.buf.len() > CMD_CAP {
                    return Err(Error::Protocol("receive header > 1 MiB".into()));
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn receive_inner(
    body: &mut BodyReader,
    env: &Env,
    route: &RepoRoute,
    hdr: &wire::ReceiveHeader,
    principal: &str,
    rate_key: &str,
    admin: bool,
    spend: &Spend,
    lease: std::rc::Rc<pack::run::Lease>,
) -> Result<Response, Error> {
    // the budget carries this request's lease: its charges (and the pass-B
    // guard, which borrows the handle) renew only this request's leases
    let (stub, mut budget) = (route.stub(env)?, ReqBudget::paid().reporting(spend).leased(lease));
    let push = PushId::random()?;
    #[derive(serde::Deserialize)]
    struct Begin {
        repo_id: String,
        #[serde(default)]
        claimed: bool,
    }
    let begin: Begin = stub_json(
        &stub,
        route,
        "/_do/push/begin",
        &serde_json::json!({
            "push_id": push.0, "principal": principal, "key": rate_key, "admin": admin,
            "object_format": if hdr.caps.object_format == gix_hash::Kind::Sha256 { "sha256" } else { "sha1" },
        }),
        &mut budget,
    )
    .await?;
    // A26: a repo that has never committed a pack must claim one of the owner's
    // GE_QUOTA_MAX_REPOS_PER_OWNER slots in the `owner!<owner>` registry DO before
    // ingest burns bandwidth. Failure here aborts the open push like any post-begin
    // error; the Limit message reaches the client in `unpack`.
    if !begin.claimed {
        if let Err(e) = owner_claim(env, route, &mut budget).await {
            push_abort(&stub, route, &push, spend).await;
            return Err(e);
        }
    }
    let bucket = Bucket::new(env.bucket("BUCKET")?, RepoId(begin.repo_id));

    // a post-begin failure must close the open push row now — leaving it for the
    // janitor's 1h expiry lets 64 failures DoS all pushes
    let run = pack::run::run(body, &bucket, &stub, route, &push, hdr.caps.object_format, env, &mut budget).await;
    if run.is_err() {
        push_abort(&stub, route, &push, spend).await;
    }
    let (pack, tags) = run?;
    let commands: Vec<serde_json::Value> = hdr
        .commands
        .iter()
        .map(|c| {
            serde_json::json!({
                "old": c.old.to_string(),
                "new": c.new.to_string(),
                "name": c.name.to_string(),
                "peeled": tags.get(&c.new).map(|t| t.to_string()),
            })
        })
        .collect();
    #[derive(serde::Deserialize)]
    struct Commit {
        results: Vec<(String, Option<String>)>,
    }
    let commit = stub_json(
        &stub,
        route,
        "/_do/push/commit",
        &serde_json::json!({
            "push_id": push.0, "pack_id": pack.map(|p| p.0), "principal": principal, "commands": commands,
            "atomic": hdr.caps.atomic,
        }),
        &mut budget,
    )
    .await;
    // a failed commit (network error, DO 500) must also close the row — the abort is
    // idempotent so a DO-side rejection that already ended the push is harmless
    let res: Commit = match commit {
        Ok(r) => r,
        Err(e) => {
            push_abort(&stub, route, &push, spend).await;
            return Err(e);
        }
    };
    let results: Vec<RefResult> = res
        .results
        .into_iter()
        .map(|(n, e)| match e {
            None => RefResult::Ok(n.into()),
            Some(m) => RefResult::Ng(n.into(), leak_reason(&m)),
        })
        .collect();
    report_status_200(hdr, Ok(()), &results)
}

/// One bounded attempt at /_do/push/abort. The request's own budget is
/// often WHY the abort runs — an admission timeout or a Budget failure
/// means it is already exhausted — so the abort gets a fresh small budget
/// (ReqBudget::cleanup) that still reports into the request's spend tally.
/// Best-effort: the result is discarded; if this attempt also fails the
/// row expires via the janitor.
async fn push_abort(stub: &Stub, route: &RepoRoute, push: &PushId, spend: &Spend) {
    let mut budget = ReqBudget::cleanup(spend);
    let _: serde_json::Value = stub_json(
        stub,
        route,
        "/_do/push/abort",
        &serde_json::json!({ "push_id": push.0 }),
        &mut budget,
    )
    .await
    .unwrap_or_default();
}

/// report-status `ng` reasons are `&'static str`; the DO's known reasons get their static form,
/// anything else falls back to git's generic string (the detail is in the unpack line anyway).
fn leak_reason(m: &str) -> &'static str {
    match m {
        "funny refname" => "funny refname",
        "deletion of the current branch prohibited" => "deletion of the current branch prohibited",
        "missing necessary objects" => "missing necessary objects",
        "gc ran during push, retry" => "gc ran during push, retry",
        "ref is pinned" => "ref is pinned",
        "ref outside token scope" => "ref outside token scope",
        "token no longer valid" => "token no longer valid",
        "protected ref: deletion refused" => "protected ref: deletion refused",
        "protected ref: non-fast-forward" => "protected ref: non-fast-forward",
        "protected ref: cannot prove fast-forward" => "protected ref: cannot prove fast-forward",
        _ => "failed to update ref",
    }
}

/// A2 post-header response: HTTP 200, `unpack <msg>` + `ng <ref> <why>` for every command.
fn report_status_200(
    hdr: &wire::ReceiveHeader,
    unpack: Result<(), String>,
    results: &[RefResult],
) -> Result<Response, Error> {
    // report-status is a negotiated capability — a client that didn't ask for it gets
    // a bare 200, not a report body it isn't demuxing
    if !hdr.caps.report_status && !hdr.caps.report_status_v2 {
        return git_resp(Vec::new(), "application/x-git-receive-pack-result");
    }
    let mut w = PktWriter::default();
    let results_buf;
    let results = if results.is_empty() && unpack.is_err() {
        results_buf = hdr
            .commands
            .iter()
            .map(|c| RefResult::Ng(c.name.clone(), "unpack failed"))
            .collect::<Vec<_>>();
        &results_buf
    } else {
        results
    };
    wire::write_report_status(&mut w, unpack.as_ref().map(|_|()).map_err(String::as_str), results, &hdr.caps)?;
    git_resp(w.out, "application/x-git-receive-pack-result")
}

/// Integer env knob, edge side — mirrors RepoDo::env_i64.
fn env_i64(env: &Env, name: &str, default: i64) -> i64 {
    env.var(name)
        .ok()
        .and_then(|v| v.to_string().parse::<i64>().ok())
        .unwrap_or(default)
}

/// A26: claim one of the owner's GE_QUOTA_MAX_REPOS_PER_OWNER slots in the
/// `owner!<owner>` registry DO — the same RepoDo class under a name no repo route
/// can produce (`!` fails seg_ok), so no extra migration or binding is needed.
/// `<= 0` disables the cap entirely (no registry hop). Over-cap -> Error::Limit,
/// which lands in the client's `unpack` line via the A2 arm.
async fn owner_claim(env: &Env, route: &RepoRoute, budget: &mut ReqBudget) -> Result<(), Error> {
    if env_i64(env, "GE_QUOTA_MAX_REPOS_PER_OWNER", 50) <= 0 {
        return Ok(());
    }
    let stub = env
        .durable_object("REPO")
        .map_err(|e| Error::Internal(e.to_string()))?
        .id_from_name(&format!("owner!{}", route.owner))
        .map_err(|e| Error::Internal(e.to_string()))?
        .get_stub()
        .map_err(|e| Error::Internal(e.to_string()))?;
    let mut init = worker::RequestInit::new();
    init.with_method(Method::Post);
    let mut r = worker::Request::new_with_init("https://do/_owner/claim", &init)
        .map_err(|e| Error::Internal(e.to_string()))?;
    route.apply_headers(&mut r)?;
    budget.charge(1)?;
    let resp = stub.fetch_with_request(r).await?;
    if resp.status_code() != 200 {
        return Err(Error::from_do_response(resp, budget).await);
    }
    Ok(())
}

/// Release the owner's quota slot on delete. The claim row lives in the
/// `owner!<owner>` registry DO — `purge_repo` only wipes the repo's own DO, so
/// without this hop a deleted repo's slot would leak. Best-effort: the tombstone
/// is already durable, a failed release just leaves a claim the owner can exceed.
async fn owner_release(env: &Env, route: &RepoRoute) {
    let Ok(ns) = env.durable_object("REPO") else { return };
    let Ok(id) = ns.id_from_name(&format!("owner!{}", route.owner)) else {
        return;
    };
    let Ok(stub) = id.get_stub() else { return };
    let mut init = worker::RequestInit::new();
    init.with_method(Method::Post);
    let mut r = match worker::Request::new_with_init("https://do/_owner/release", &init) {
        Ok(r) => r,
        Err(_) => return,
    };
    if route.apply_headers(&mut r).is_err() {
        return;
    }
    let _ = stub.fetch_with_request(r).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The declared-length precheck: over-cap refuses with the caller's
    /// Protocol wording, len == cap is admitted, and an absent or
    /// unparsable header falls through to the streamed check. A length
    /// beyond usize's reach counts as over-cap.
    #[test]
    fn declared_len_precheck() {
        let err = declared_len_ok(Some("65"), 64, "declared too large").unwrap_err();
        assert!(matches!(&err, Error::Protocol(m) if m == "declared too large"), "{err}");
        declared_len_ok(Some("64"), 64, "declared too large").unwrap();
        declared_len_ok(None, 64, "declared too large").unwrap();
        declared_len_ok(Some("not-a-number"), 64, "declared too large").unwrap();
        declared_len_ok(Some("18446744073709551615"), 64, "declared too large").unwrap_err();
    }

    /// A waker that does nothing — auth_then_body's fakes resolve on
    /// first poll, so its future never really pends in these tests.
    fn noop_waker() -> std::task::Waker {
        use std::task::{RawWaker, RawWakerVTable, Waker};
        unsafe fn clone(_: *const ()) -> RawWaker {
            RawWaker::new(std::ptr::null(), &VTABLE)
        }
        unsafe fn no_op(_: *const ()) {}
        static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, no_op, no_op, no_op);
        unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
    }

    /// Drive `f` to completion — the seam's fakes never pend.
    fn block_on<F: std::future::Future>(f: F) -> F::Output {
        let w = noop_waker();
        let mut cx = std::task::Context::from_waker(&w);
        let mut f = std::pin::pin!(f);
        for _ in 0..1_000_000 {
            if let std::task::Poll::Ready(v) = f.as_mut().poll(&mut cx) {
                return v;
            }
        }
        panic!("future did not resolve");
    }

    /// lfs_batch's order as a seam: a failed Read auth returns its own
    /// error and the body reader is never invoked; on success auth runs
    /// before the body read.
    #[test]
    fn auth_then_body_gates_the_body_read() {
        let ran = Rc::new(Cell::new(false));
        let r = Rc::clone(&ran);
        let err = block_on(auth_then_body(
            async { Err::<(), Error>(Error::Forbidden) },
            move || {
                r.set(true);
                async { Ok::<Vec<u8>, Error>(Vec::new()) }
            },
        ))
        .unwrap_err();
        assert!(matches!(err, Error::Forbidden), "{err}");
        assert!(!ran.get(), "the body reader ran despite the auth failure");

        let order = Rc::new(std::cell::RefCell::new(Vec::new()));
        let (a, b) = (Rc::clone(&order), Rc::clone(&order));
        let (_, out) = block_on(auth_then_body(
            async move {
                a.borrow_mut().push("auth");
                Ok::<(), Error>(())
            },
            move || {
                let b = Rc::clone(&b);
                async move {
                    b.borrow_mut().push("body");
                    Ok(vec![1u8, 2])
                }
            },
        ))
        .unwrap();
        assert_eq!(out, vec![1, 2]);
        assert_eq!(order.borrow().as_slice(), ["auth", "body"]);
    }

    /// A chunk that counts its copies, so a test can see that none ran.
    struct Counted(Vec<u8>, Rc<Cell<usize>>);
    impl BodyChunk for Counted {
        fn len(&self) -> usize {
            self.0.len()
        }
        fn copy_into(&self, dst: &mut [u8]) {
            self.1.set(self.1.get() + 1);
            dst.copy_from_slice(&self.0);
        }
    }

    /// A body stream that counts its polls.
    struct Probe<C> {
        items: std::collections::VecDeque<C>,
        polls: Rc<Cell<usize>>,
    }
    impl<C: Unpin> Stream for Probe<C> {
        type Item = Result<C, Error>;
        fn poll_next(
            mut self: Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            self.polls.set(self.polls.get() + 1);
            std::task::Poll::Ready(self.items.pop_front().map(Ok))
        }
    }
    fn probe<C>(items: Vec<C>) -> (Probe<C>, Rc<Cell<usize>>) {
        let polls = Rc::new(Cell::new(0));
        (Probe { items: items.into(), polls: polls.clone() }, polls)
    }
    fn counted(items: &[&[u8]], copies: &Rc<Cell<usize>>) -> Vec<Counted> {
        items.iter().map(|b| Counted(b.to_vec(), copies.clone())).collect()
    }

    /// Many small chunks under the cap join into the exact body.
    #[test]
    fn fill_capped_joins_chunks_under_the_cap() {
        let copies = Rc::new(Cell::new(0));
        let want: Vec<u8> = (0..200u8).collect();
        let parts: Vec<&[u8]> = vec![&want[..1], &want[1..100], &want[100..101], &want[101..]];
        let (s, polls) = probe(counted(&parts, &copies));
        let body = block_on(fill_capped(1 << 20, "test body too large", s)).unwrap();
        assert_eq!(body, want);
        assert_eq!(copies.get(), parts.len(), "every admitted chunk is copied once");
        assert_eq!(polls.get(), parts.len() + 1, "chunks plus the end-of-stream poll");
    }

    /// A single oversized FIRST chunk is the Protocol error: it is never
    /// copied, never reaches the try_grow ask log, and the stream is
    /// polled exactly once.
    #[test]
    fn fill_capped_rejects_an_oversized_first_chunk_uncopied() {
        let _guard = crate::try_grow::HookGuard;
        let copies = Rc::new(Cell::new(0));
        let (s, polls) = probe(counted(&[&[0u8; 65]], &copies));
        crate::try_grow::log_asks();
        let err = block_on(fill_capped(64, "test body too large", s)).unwrap_err();
        assert!(matches!(&err, Error::Protocol(m) if m == "test body too large"), "{err}");
        assert_eq!(copies.get(), 0, "the oversized chunk must not be copied");
        assert_eq!(polls.get(), 1, "the stream must not be polled again");
        assert!(crate::try_grow::take_asks().is_empty(), "the refused chunk must not reach try_grow");
    }

    /// A chunk that crosses the cap mid-stream is the Protocol error:
    /// earlier chunks were copied, the crossing chunk was not, and the
    /// stream was never polled past it.
    #[test]
    fn fill_capped_rejects_the_chunk_crossing_the_cap() {
        let copies = Rc::new(Cell::new(0));
        let (s, polls) = probe(counted(&[&[1, 2], &[3, 4, 5], &[6], &[7]], &copies));
        let err = block_on(fill_capped(4, "test body too large", s)).unwrap_err();
        assert!(matches!(&err, Error::Protocol(m) if m == "test body too large"), "{err}");
        assert_eq!(copies.get(), 1, "only the first chunk may be copied");
        assert_eq!(polls.get(), 2, "the stream stops at the crossing chunk");
    }

    /// A body whose chunks sum to exactly the cap is admitted.
    #[test]
    fn fill_capped_admits_a_body_exactly_at_the_cap() {
        let copies = Rc::new(Cell::new(0));
        let want: Vec<u8> = (0..64u8).collect();
        let parts: Vec<&[u8]> = vec![&want[..10], &want[10..40], &want[40..]];
        let (s, _) = probe(counted(&parts, &copies));
        let body = block_on(fill_capped(64, "test body too large", s)).unwrap();
        assert_eq!(body, want);
        assert_eq!(copies.get(), parts.len());
    }

    /// An empty body — an empty stream, or one of only empty chunks — is Ok.
    #[test]
    fn fill_capped_accepts_an_empty_body() {
        let (s, polls) = probe(Vec::<Counted>::new());
        assert!(block_on(fill_capped(8, "test body too large", s)).unwrap().is_empty());
        assert_eq!(polls.get(), 1);
        let copies = Rc::new(Cell::new(0));
        let (s, _) = probe(counted(&[&[], &[]], &copies));
        assert!(block_on(fill_capped(8, "test body too large", s)).unwrap().is_empty());
    }

    /// A refused reservation is the in-band retry Limit and the chunk is
    /// never copied.
    #[test]
    fn fill_capped_refused_reservation_is_the_retry_limit() {
        let _guard = crate::try_grow::HookGuard;
        let copies = Rc::new(Cell::new(0));
        let (s, polls) = probe(counted(&[&[7u8; 100]], &copies));
        crate::try_grow::fail_above(Some(50));
        let err = block_on(fill_capped(1 << 20, "test body too large", s)).unwrap_err();
        assert!(crate::try_grow::is_retry(&err), "{err}");
        assert_eq!(copies.get(), 0, "a refused reservation must not copy the chunk");
        assert_eq!(polls.get(), 1);
    }

    /// chunk_vec copies the chunk exactly once into a fresh Vec.
    #[test]
    fn chunk_vec_copies_the_chunk_exactly_once() {
        let copies = Rc::new(Cell::new(0));
        let c = Counted(vec![1, 2, 3], copies.clone());
        assert_eq!(chunk_vec(&c).unwrap(), vec![1, 2, 3]);
        assert_eq!(copies.get(), 1);
    }

    /// A refused reservation is the in-band retry Limit and the chunk is
    /// never copied.
    #[test]
    fn chunk_vec_refused_reservation_is_the_retry_limit() {
        let _guard = crate::try_grow::HookGuard;
        let copies = Rc::new(Cell::new(0));
        let c = Counted(vec![7u8; 100], copies.clone());
        crate::try_grow::fail_above(Some(50));
        let err = chunk_vec(&c).unwrap_err();
        assert!(crate::try_grow::is_retry(&err), "{err}");
        assert_eq!(copies.get(), 0, "a refused reservation must not copy the chunk");
    }

    /// A zero-length chunk yields an empty Vec without asking the gate.
    #[test]
    fn chunk_vec_zero_length_asks_nothing() {
        let _guard = crate::try_grow::HookGuard;
        crate::try_grow::log_asks();
        crate::try_grow::fail_above(Some(0));
        let c = Counted(Vec::new(), Rc::new(Cell::new(0)));
        assert!(chunk_vec(&c).unwrap().is_empty());
        assert!(crate::try_grow::take_asks().is_empty(), "an empty chunk must not reach the gate");
    }

    /// The reader's own buffer grows through the same fallible
    /// reservation: a refused growth is the in-band retry Limit and
    /// buffers nothing.
    #[test]
    fn body_reader_fill_refused_buf_growth_is_the_retry_limit() {
        let _guard = crate::try_grow::HookGuard;
        let mut body = BodyReader::from_chunks(vec![vec![7u8; 100]], None);
        crate::try_grow::fail_above(Some(50));
        let err = block_on(body.fill(1)).unwrap_err();
        assert!(crate::try_grow::is_retry(&err), "{err}");
        assert!(body.buffered().is_empty(), "a refused growth buffers nothing");
    }

    /// A chunked upload-pack body (no Content-Length) is capped WHILE it
    /// streams: 64 x 64 KiB refuses with the Limit error after ~1 MiB has
    /// been pulled — reader.total proves the refusal did not wait for EOF.
    #[test]
    fn upload_body_cap_fires_while_streaming() {
        let chunks: Vec<Vec<u8>> = (0..64).map(|_| vec![0u8; FILL_STEP]).collect();
        let mut reader = BodyReader::from_chunks(chunks, None);
        let err = block_on(read_upload_body(&mut reader, CMD_CAP)).unwrap_err();
        assert!(
            matches!(&err, Error::Limit(m) if m == "upload-pack body > 1 MiB"),
            "{err}"
        );
        assert!(
            reader.total <= CMD_CAP as u64 + 2 * FILL_STEP as u64,
            "reader.total = {} — the cap must fire while streaming, not after EOF",
            reader.total
        );
    }

    /// Chunks summing to exactly the cap are admitted whole.
    #[test]
    fn upload_body_admits_exactly_cap() {
        let chunks: Vec<Vec<u8>> = (0..16).map(|i| vec![i as u8; FILL_STEP]).collect();
        let mut reader = BodyReader::from_chunks(chunks, None);
        let body = block_on(read_upload_body(&mut reader, CMD_CAP)).unwrap();
        assert_eq!(body.len(), CMD_CAP);
        assert_eq!(body[0], 0);
        assert_eq!(body[FILL_STEP], 1);
        assert_eq!(body[CMD_CAP - 1], 15);
    }

    /// A small chunked body passes through untouched.
    #[test]
    fn upload_body_admits_a_small_body() {
        let payload: Vec<u8> = (0..300u32).map(|i| (i % 251) as u8).collect();
        let mut reader = BodyReader::from_chunks(vec![payload.clone()], None);
        let body = block_on(read_upload_body(&mut reader, CMD_CAP)).unwrap();
        assert_eq!(body, payload);
    }
}
