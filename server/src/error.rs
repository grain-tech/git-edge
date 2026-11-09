use worker::Response;

use crate::ReqBudget;

/// The single error enum (CONTRACTS.md section 10).
#[derive(Debug)]
pub enum Error {
    Protocol(String),
    Auth,
    Forbidden,
    NotFound,
    Conflict(String),
    Unpack(String),
    Budget,
    Limit(String),
    Gone,
    /// Push rate limit (A27): carries the Retry-After hint in seconds.
    RateLimit(u32),
    /// G2b live-pack admission cap: the server is shedding work for a while;
    /// carries the Retry-After hint in seconds.
    Busy(u32),
    Storage(String),
    Internal(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Protocol(m) => write!(f, "protocol error: {m}"),
            Error::Auth => write!(f, "authentication required"),
            Error::Forbidden => write!(f, "forbidden"),
            Error::NotFound => write!(f, "not found"),
            Error::Conflict(m) => write!(f, "conflict: {m}"),
            Error::Unpack(m) => write!(f, "unpack failed: {m}"),
            Error::Budget => write!(f, "request budget exhausted"),
            Error::Limit(m) => write!(f, "limit exceeded: {m}"),
            Error::Gone => write!(f, "repository deleted"),
            Error::RateLimit(s) => write!(f, "rate limited, retry in {s}s"),
            Error::Busy(s) => write!(f, "server busy, retry in {s}s"),
            Error::Storage(m) => write!(f, "storage error: {m}"),
            Error::Internal(m) => write!(f, "internal error: {m}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<worker::Error> for Error {
    fn from(e: worker::Error) -> Self {
        Error::Storage(e.to_string())
    }
}

/// Crossing into worker::Error only ever happens at a DO/fetch boundary where the runtime
/// turns it into a 500; Storage/Internal in the DO take this path deliberately (A2 rollback).
impl From<Error> for worker::Error {
    fn from(e: Error) -> Self {
        worker::Error::RustError(e.to_string())
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::Internal(e.to_string())
    }
}

impl Error {
    /// Variant name for metrics and dead-job alerting — deliberately not the
    /// message, which is unbounded and would blow the datapoint's cardinality.
    pub fn class(&self) -> &'static str {
        match self {
            Error::Protocol(_) => "protocol",
            Error::Auth => "auth",
            Error::Forbidden => "forbidden",
            Error::NotFound => "notfound",
            Error::Conflict(_) => "conflict",
            Error::Unpack(_) => "unpack",
            Error::Budget => "budget",
            Error::Limit(_) => "limit",
            Error::Storage(_) => "storage",
            Error::Gone => "gone",
            Error::RateLimit(_) => "ratelimit",
            Error::Busy(_) => "busy",
            Error::Internal(_) => "internal",
        }
    }

    /// Rebuild the DO's Error from its JSON error body (wire::http::do_error_response).
    /// The DO stamps its own R2 spend in x-ge-subrequests; it is folded into the
    /// request tally (not charged — the edge budget already paid for this call)
    /// so the edge's error response reports the true request total.
    pub async fn from_do_response(mut resp: worker::Response, budget: &ReqBudget) -> Error {
        if let Some(v) = resp.headers().get("x-ge-subrequests").ok().flatten() {
            if let Some(n) = v.split('/').next().and_then(|s| s.parse::<u32>().ok()) {
                budget.report(n);
            }
        }
        #[derive(serde::Deserialize)]
        struct E {
            error: Option<String>,
            message: Option<String>,
            retry_after: Option<u32>,
        }
        let parsed: Option<E> = resp.json().await.ok();
        match parsed {
            Some(e) => {
                let msg = e.message.unwrap_or_default();
                match e.error.as_deref() {
                    Some("protocol") => Error::Protocol(msg),
                    Some("auth") => Error::Auth,
                    Some("forbidden") => Error::Forbidden,
                    Some("notfound") => Error::NotFound,
                    Some("gone") => Error::Gone,
                    Some("conflict") => Error::Conflict(msg),
                    Some("unpack") => Error::Unpack(msg),
                    Some("budget") => Error::Budget,
                    Some("limit") => Error::Limit(msg),
                    Some("ratelimit") => Error::RateLimit(e.retry_after.unwrap_or(60)),
                    Some("busy") => Error::Busy(e.retry_after.unwrap_or(60)),
                    _ => Error::Storage(msg),
                }
            }
            None => Error::Storage(format!("DO returned {}", resp.status_code())),
        }
    }

    /// Message carried to the client inside `unpack <msg>` / `ERR <msg>`.
    pub fn message(&self) -> String {
        match self {
            Error::Protocol(m) | Error::Unpack(m) | Error::Limit(m) | Error::Storage(m)
            | Error::Internal(m) | Error::Conflict(m) => m.clone(),
            Error::Auth => "authentication required".into(),
            Error::Forbidden => "forbidden".into(),
            Error::NotFound => "not found".into(),
            Error::Budget => "request budget exhausted".into(),
            Error::Gone => "repository deleted".into(),
            Error::RateLimit(s) => format!("rate limit exceeded, retry in {s}s"),
            Error::Busy(s) => format!("server busy, retry in {s}s"),
        }
    }
    /// The client-safe form: Internal/Storage details (R2 keys, SQL errors) stay in the
    /// worker log; clients get a generic string. Everything else passes through —
    /// sanitized: messages can carry raw client bytes, and control chars/newlines
    /// would corrupt the pkt-line stream or the user's terminal.
    pub fn client_message(&self) -> String {
        let m = match self {
            Error::Storage(_) | Error::Internal(_) => "internal error".into(),
            _ => self.message(),
        };
        let clean: String = m
            .chars()
            .take(512)
            .map(|c| if c.is_ascii_graphic() || c == ' ' { c } else { '?' })
            .collect();
        clean
    }

    /// Pre-response mapping (section 10, amended by A2).
    pub fn status(&self) -> u16 {
        match self {
            Error::Protocol(_) => 400,
            Error::Auth => 401,
            Error::Forbidden => 403,
            Error::NotFound => 404,
            Error::Gone => 410,
            Error::Conflict(_) => 409,
            Error::Budget => 413,
            // the memory-pressure Limit is transient — a client that waits
            // and retries can succeed, so it is a 503, not a permanent 413
            Error::Limit(_) if crate::try_grow::is_retry(self) => 503,
            Error::Limit(_) => 413,
            Error::RateLimit(_) => 429,
            Error::Busy(_) => 503,
            Error::Unpack(_) | Error::Storage(_) | Error::Internal(_) => 500,
        }
    }
}

/// The Retry-After value an error response carries, if any: RateLimit and
/// Busy keep their own seconds floored at 1; the transient memory-pressure
/// Limit gets a fixed minute; every other error gets none.
fn retry_after(e: &Error) -> Option<String> {
    match e {
        Error::RateLimit(secs) | Error::Busy(secs) => Some((*secs).max(1).to_string()),
        _ if crate::try_grow::is_retry(e) => Some("60".into()),
        _ => None,
    }
}

/// The x-ge-subrequests stamp: `<spent>/<max>` where `spent` is the request-wide
/// tally — every ReqBudget the request created reported into it. Error paths are
/// exactly where the accounting matters (413/429/500), so it goes on every
/// response; paths that failed before the first stub/R2 call report 0.
fn stamp_subrequests(resp: &mut Response, subreqs: u32) {
    resp.headers_mut()
        .set("x-ge-subrequests", &format!("{subreqs}/{}", ReqBudget::PAID_SUBREQUESTS))
        .ok();
}

/// Map a Result into a worker Response with the section-10 statuses.
/// `git_pkt` wraps the message as one pkt-line `ERR <msg>\n` for git-protocol POSTs.
/// `subreqs` is the request-wide tally for the x-ge-subrequests header.
pub fn respond(r: Result<Response, Error>, git_pkt: bool, subreqs: u32) -> worker::Result<Response> {
    match r {
        Ok(mut resp) => {
            // streamed fetch/export responses already carry the DO's projected
            // spend — don't overwrite it with the edge's smaller tally
            if resp.headers().get("x-ge-subrequests").ok().flatten().is_none() {
                stamp_subrequests(&mut resp, subreqs);
            }
            Ok(resp)
        }
        Err(e) => {
            let status = e.status();
            if matches!(e, Error::Storage(_) | Error::Internal(_)) {
                worker::console_log!("git-edge: {e}");
            }
            let body = if git_pkt {
                let msg = format!("ERR {}\n", e.client_message());
                let len = msg.len() + 4;
                format!("{len:04x}{msg}")
            } else {
                format!("{}\n", e.client_message())
            };
            let mut resp = Response::ok(body)?;
            if matches!(e, Error::Auth) {
                resp.headers_mut().set("WWW-Authenticate", "Basic realm=\"git-edge\"").ok();
            }
            if let Some(v) = retry_after(&e) {
                resp.headers_mut().set("Retry-After", &v).ok();
            }
            stamp_subrequests(&mut resp, subreqs);
            Ok(resp.with_status(status))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The memory-pressure Limit is transient: 503, not the permanent 413
    /// every other Limit/Budget keeps.
    #[test]
    fn retry_memory_limit_maps_to_503_other_limits_to_413() {
        assert_eq!(Error::Limit(crate::try_grow::MEM_RETRY_MSG.into()).status(), 503);
        assert_eq!(Error::Limit("pack too big".into()).status(), 413);
        assert_eq!(Error::Budget.status(), 413);
        assert_eq!(Error::Busy(5).status(), 503);
    }

    /// Every Retry-After case: RateLimit/Busy carry their own seconds
    /// floored at 1, the transient memory-pressure Limit gets a fixed
    /// minute, and every other error gets no header.
    #[test]
    fn retry_after_covers_every_hint() {
        assert_eq!(retry_after(&Error::RateLimit(30)).as_deref(), Some("30"));
        assert_eq!(retry_after(&Error::RateLimit(0)).as_deref(), Some("1"));
        assert_eq!(retry_after(&Error::Busy(7)).as_deref(), Some("7"));
        assert_eq!(retry_after(&Error::Busy(0)).as_deref(), Some("1"));
        assert_eq!(
            retry_after(&Error::Limit(crate::try_grow::MEM_RETRY_MSG.into())).as_deref(),
            Some("60")
        );
        assert_eq!(retry_after(&Error::Limit("pack too big".into())), None);
        assert_eq!(retry_after(&Error::Budget), None);
        assert_eq!(retry_after(&Error::Auth), None);
        assert_eq!(retry_after(&Error::Protocol("bad".into())), None);
    }
}
