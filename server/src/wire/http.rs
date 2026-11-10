//! Shared request/response helpers (CONTRACTS.md A8): the place DTOs, route headers and the
//! stub-call glue live so `repo_do` never imports `edge`. `worker` types are allowed here.

use worker::{Headers, Method, Request, RequestInit, Response, Stub};

use crate::error::Error;
use crate::ReqBudget;

/// x-ge-owner / x-ge-repo / x-ge-base, set by the edge on every stub request
/// (section 8.1). `base` is the public origin (scheme://host[:port]) — present
/// only on routes that may compose client-facing URLs (fetch: packfile-uris).
pub struct RepoHeaders {
    pub owner: Option<String>,
    pub repo: Option<String>,
    pub base: Option<String>,
}
impl RepoHeaders {
    pub const NONE: RepoHeaders = RepoHeaders { owner: None, repo: None, base: None };
    pub fn from_request(req: &Request) -> Self {
        Self {
            owner: req.headers().get("x-ge-owner").ok().flatten(),
            repo: req.headers().get("x-ge-repo").ok().flatten(),
            base: req.headers().get("x-ge-base").ok().flatten(),
        }
    }
}

/// One parser for `/:owner/:repo[.git]/<rest>`; the only place the DO name is formed.
pub struct RepoRoute {
    pub owner: String,
    pub repo: String,
}
impl RepoRoute {
    pub(crate) fn seg_ok(s: &str) -> bool {
        (1..=64).contains(&s.len())
            && s.bytes().all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
            && s != "."
            && s != ".."
    }
    pub fn parse(path: &str) -> Result<(Self, String), Error> {
        let mut it = path.strip_prefix('/').ok_or(Error::NotFound)?.splitn(3, '/');
        let (owner, repo, rest) = (
            it.next().unwrap_or(""),
            it.next().unwrap_or(""),
            it.next().unwrap_or(""),
        );
        let repo = repo.strip_suffix(".git").unwrap_or(repo);
        if !Self::seg_ok(owner) || !Self::seg_ok(repo) {
            return Err(Error::NotFound);
        }
        Ok((Self { owner: owner.into(), repo: repo.into() }, rest.into()))
    }
    pub fn name(&self) -> String {
        format!("{}/{}", self.owner, self.repo)
    }
    pub fn stub(&self, env: &worker::Env) -> Result<Stub, Error> {
        Ok(env.durable_object("REPO")?.id_from_name(&self.name())?.get_stub()?)
    }
    /// The only two headers a DO ever sees (section 8.1). No client header crosses.
    pub fn apply_headers(&self, req: &mut Request) -> Result<(), Error> {
        req.headers_mut()?.set("x-ge-owner", &self.owner)?;
        req.headers_mut()?.set("x-ge-repo", &self.repo)?;
        Ok(())
    }
    /// A fresh internal Request: POST + the two identity headers + a byte body.
    pub fn internal_request(&self, path: &str, body: Vec<u8>) -> Result<Request, Error> {
        let headers = Headers::new();
        headers.set("x-ge-owner", &self.owner)?;
        headers.set("x-ge-repo", &self.repo)?;
        let mut init = RequestInit::new();
        init.with_method(Method::Post);
        init.with_headers(headers);
        init.with_body(Some(js_sys::Uint8Array::from(body.as_slice()).into()));
        Ok(Request::new_with_init(&format!("https://do{path}"), &init)?)
    }
}

/// JSON body parse for DO routes; bad JSON from the edge is Internal (edge produced it).
pub fn parse<T: serde::de::DeserializeOwned>(b: &[u8]) -> Result<T, Error> {
    serde_json::from_slice(b).map_err(|e| Error::Internal(format!("do body: {e}")))
}

/// JSON response for DO routes.
pub fn json(v: serde_json::Value) -> Result<Response, Error> {
    Response::from_json(&v).map_err(|e| Error::Internal(e.to_string()))
}

/// Turn a DO-side Error into a JSON response the edge can reconstruct. Storage and Internal
/// never reach this: they propagate out of `fetch` so the platform rolls back the span (A2).
/// `subreqs` is the DO's own R2 spend — stamped as x-ge-subrequests so the edge
/// can fold it into the request tally on its error response (audit P3); sync-span
/// routes never reach R2 and pass 0.
pub fn do_error_response(e: &Error, subreqs: u32) -> Result<Response, Error> {
    let kind = match e {
        Error::Protocol(_) => "protocol",
        Error::Auth => "auth",
        Error::Forbidden => "forbidden",
        Error::NotFound => "notfound",
        Error::Gone => "gone",
        Error::Conflict(_) => "conflict",
        Error::Unpack(_) => "unpack",
        Error::Budget => "budget",
        Error::Limit(_) => "limit",
        Error::RateLimit(_) => "ratelimit",
        Error::Busy(_) => "busy",
        Error::Storage(_) | Error::Internal(_) => "internal",
    };
    let mut body = serde_json::json!({ "error": kind, "message": e.message() });
    if let Error::RateLimit(secs) | Error::Busy(secs) = e {
        body["retry_after"] = (*secs).into();
    }
    let mut resp = Response::from_json(&body).map_err(|e| Error::Internal(e.to_string()))?;
    resp.headers_mut()
        .set("x-ge-subrequests", &format!("{subreqs}/{}", ReqBudget::PAID_SUBREQUESTS))
        .ok();
    Ok(resp.with_status(e.status()))
}

/// One stub round-trip, charged first (7.1), identity headers set (8.1).
/// A non-200 body carries the DO's JSON error (section 10, A2).
pub async fn stub_json<T: serde::de::DeserializeOwned>(
    stub: &Stub,
    repo: &RepoRoute,
    path: &str,
    body: &impl serde::Serialize,
    budget: &mut ReqBudget,
) -> Result<T, Error> {
    let text = serde_json::to_string(body).map_err(|e| Error::Internal(e.to_string()))?;
    stub_json_body(stub, repo, path, text.into_bytes(), budget).await
}

/// `stub_json` with the body already serialized — callers that must bound
/// the serialization's memory (the pass-B index posts) build `body` at an
/// exact capacity themselves.
pub async fn stub_json_body<T: serde::de::DeserializeOwned>(
    stub: &Stub,
    repo: &RepoRoute,
    path: &str,
    body: Vec<u8>,
    budget: &mut ReqBudget,
) -> Result<T, Error> {
    budget.charge(1)?;
    let req = repo.internal_request(path, body)?;
    let mut resp = stub.fetch_with_request(req).await?;
    if resp.status_code() != 200 {
        return Err(Error::from_do_response(resp, budget).await);
    }
    resp.json::<T>().await.map_err(|e| Error::Internal(format!("do response: {e}")))
}

/// Forward a raw pkt-line body to a DO route (ls-refs, fetch). The response streams back as-is.
/// `base` is the public request origin — forwarded as x-ge-base on the fetch route so the DO
/// can compose client-facing packfile-uris URLs (A30).
pub async fn stub_raw(
    stub: &Stub,
    repo: &RepoRoute,
    path: &str,
    body: Vec<u8>,
    budget: &mut ReqBudget,
    base: Option<&str>,
) -> Result<Response, Error> {
    budget.charge(1)?;
    let mut req = repo.internal_request(path, body)?;
    if let Some(b) = base {
        req.headers_mut()?.set("x-ge-base", b)?;
    }
    stub.fetch_with_request(req).await.map_err(|e| Error::Storage(e.to_string()))
}
