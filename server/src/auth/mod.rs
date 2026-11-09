//! Edge authentication (CONTRACTS.md 1.1/8): GE_READ_TOKEN, GE_WRITE_TOKEN, then
//! per-repo tokens held by the repo's DO. Global secrets answer without waking the
//! DO; anything else is one `/_do/auth` round-trip. Anonymous gets a 401 challenge;
//! a read-level token on a write route gets 403, not a second challenge.

use worker::{Env, Request};

use crate::error::Error;
use crate::wire::http::{stub_json, RepoRoute};
use crate::{ReqBudget, Spend};

#[derive(Clone, Copy)]
pub enum Level {
    Read,
    Write,
}

/// Missing optional secret is not an internal error — the compare just fails.
fn secret_opt(env: &Env, name: &str) -> Option<String> {
    env.secret(name).ok().map(|s| s.to_string())
}

/// RFC 7235: the auth scheme is case-insensitive.
fn scheme<'a>(hdr: &'a str, name: &str) -> Option<&'a str> {
    let (s, rest) = hdr.split_at_checked(name.len())?;
    (s.eq_ignore_ascii_case(name) && rest.starts_with(' ')).then(|| &rest[1..])
}

/// sha1 hex of a presented token — the DO stores hashes, never raw tokens, so the
/// edge sends only the hash across the stub boundary. Infallible in practice, but a
/// Result anyway: an unwrap_or_default would collapse every hash to "" and match the
/// first such row — a fail-open, not a fail-closed, error.
pub fn token_hash(token: &str) -> Result<String, Error> {
    let mut h = gix_hash::hasher(gix_hash::Kind::Sha1);
    h.update(token.as_bytes());
    h.try_finalize()
        .map(|id| id.to_string())
        .map_err(|_| Error::Internal("token hash".into()))
}

/// Extract (token, principal) from the Authorization header.
pub(crate) fn credentials(req: &Request) -> Result<(String, String), Error> {
    let hdr = req
        .headers()
        .get("authorization")
        .map_err(|e| Error::Internal(e.to_string()))?
        .ok_or(Error::Auth)?;
    let (token, principal) = if let Some(t) = scheme(&hdr, "Bearer") {
        (t.trim().to_string(), "bearer".to_string())
    } else if let Some(b) = scheme(&hdr, "Basic") {
        // git sends Basic base64(user:token); the token is the password, the user names the principal
        let decoded = b64_decode(b.trim()).ok_or(Error::Auth)?;
        let s = String::from_utf8_lossy(&decoded);
        match s.split_once(':') {
            Some((u, p)) => (p.to_string(), if u.is_empty() { "basic".to_string() } else { u.to_string() }),
            None => (s.to_string(), "basic".to_string()),
        }
    } else {
        return Err(Error::Auth);
    };
    // an empty presented token must never match anything — an empty secret value
    // (misconfigured env) would otherwise authenticate every empty credential
    if token.is_empty() {
        return Err(Error::Auth);
    }
    Ok((token, principal))
}

/// Deployment-admin check: the global write token only. Token management can't be
/// delegated to repo-level tokens or any write holder could mint more credentials.
pub fn authenticate_admin(req: &Request, env: &Env) -> Result<String, Error> {
    let (token, principal) = credentials(req)?;
    let write = secret_opt(env, "GE_WRITE_TOKEN");
    if write.map(|w| ct_eq(token.as_bytes(), w.as_bytes())) == Some(true) {
        Ok(principal)
    } else {
        Err(Error::Auth)
    }
}

/// True only when the presented credential is the global GE_WRITE_TOKEN. Push
/// begin forwards this so the DO can tell "deployment admin, all refs" apart
/// from "repo token whose row is gone" — both have no tokens row, and only the
/// first may push. Any parse/secret failure is `false` (fail closed).
pub fn is_admin(req: &Request, env: &Env) -> bool {
    let Ok((token, _)) = credentials(req) else {
        return false;
    };
    secret_opt(env, "GE_WRITE_TOKEN").map(|w| ct_eq(token.as_bytes(), w.as_bytes())) == Some(true)
}

/// Returns the principal string recorded in the reflog — for a repo token, its
/// admin-assigned name, so pushes attribute to a meaningful identity. `spend`
/// tallies the stub call so a 401/403 still reports x-ge-subrequests.
pub async fn authenticate(req: &Request, env: &Env, need: Level, route: &RepoRoute, spend: &Spend) -> Result<String, Error> {
    authenticate_with(credentials(req), env, need, route, spend).await
}

/// The post-credential half of authenticate. Split out so a caller that
/// must not hold a borrow on `req` across the await — lfs_batch reads the
/// body after auth — can extract credentials synchronously, then sequence
/// the check before the read.
pub(crate) async fn authenticate_with(
    creds: Result<(String, String), Error>,
    env: &Env,
    need: Level,
    route: &RepoRoute,
    spend: &Spend,
) -> Result<String, Error> {
    let (token, principal) = match creds {
        Ok(c) => c,
        // no usable credential on a read route: a public repo answers anonymously.
        // (A presented-but-invalid credential still gets the challenge — anonymous
        // fallback is only for requests that never offered one.)
        Err(Error::Auth) if matches!(need, Level::Read) => return anonymous(env, route, spend).await,
        Err(e) => return Err(e),
    };
    // optional for read-only deployments: an unset write secret just never matches,
    // it must not 500 a read route
    let write = secret_opt(env, "GE_WRITE_TOKEN");
    if write.map(|w| ct_eq(token.as_bytes(), w.as_bytes())) == Some(true) {
        return Ok(principal);
    }
    let read = secret_opt(env, "GE_READ_TOKEN");
    if read.map(|r| ct_eq(token.as_bytes(), r.as_bytes())) == Some(true) {
        return match need {
            Level::Read => Ok(principal),
            // a valid read token is forbidden, not unauthenticated: no second challenge
            Level::Write => Err(Error::Forbidden),
        };
    }
    // per-repo tokens are `ge_` + 64 lowercase hex — anything else can be rejected
    // here without waking the DO (a non-global credential must not cost a DO boot
    // + SQLite work + a billable subrequest at attacker-controlled line rate)
    if !is_repo_token(&token) {
        return Err(Error::Auth);
    }
    // per-repo tokens live in the DO's tokens table — one stub call. Storage errors
    // fail closed (propagate): never silently treat a DO failure as unauthenticated.
    #[derive(serde::Deserialize)]
    struct AuthRow {
        level: String,
        name: String,
    }
    let stub = route.stub(env)?;
    let mut budget = ReqBudget::paid().reporting(spend);
    let row: Result<AuthRow, Error> = stub_json(
        &stub,
        route,
        "/_do/auth",
        &serde_json::json!({ "hash": token_hash(&token)? }),
        &mut budget,
    )
    .await;
    match row {
        Ok(r) if r.level == "write" => Ok(r.name),
        Ok(r) if r.level == "read" && matches!(need, Level::Read) => Ok(r.name),
        Ok(_) => Err(Error::Forbidden), // repo read token on a write route
        Err(Error::Auth) | Err(Error::NotFound) => Err(Error::Auth),
        Err(e) => Err(e),
    }
}

/// No credential presented on a read route: one `/_do/public` probe decides
/// whether the repo serves anonymous reads. Non-Auth DO errors propagate — a
/// tombstoned repo answers 410, not a fresh 401 challenge.
async fn anonymous(env: &Env, route: &RepoRoute, spend: &Spend) -> Result<String, Error> {
    #[derive(serde::Deserialize)]
    struct P {
        public: bool,
    }
    let stub = route.stub(env)?;
    let mut budget = ReqBudget::paid().reporting(spend);
    let p: P = stub_json(&stub, route, "/_do/public", &serde_json::json!({}), &mut budget).await?;
    if p.public {
        Ok("anonymous".into())
    } else {
        Err(Error::Auth)
    }
}

/// sha1 of the presented token — the rate-limit key (A27). The raw token never
/// crosses the stub boundary; the hash is stable per credential.
pub fn presented_hash(req: &Request) -> Result<String, Error> {
    let (token, _) = credentials(req)?;
    token_hash(&token)
}

/// `ge_` + 64 lowercase hex — the shape token_create mints.
fn is_repo_token(token: &str) -> bool {
    token.len() == 67
        && token.starts_with("ge_")
        && token[3..].bytes().all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}

/// Constant-time token compare: length mismatch still exits early (token length is not
/// secret — it is observable in the request anyway), content compare never short-circuits.
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// RFC 4648, no dependencies (the Workers runtime's `atob` is not reachable from workers-rs
/// without a Reflect call, which belongs to `platform` — a plain table keeps this testable).
fn b64_decode(s: &str) -> Option<Vec<u8>> {
    fn v(b: u8) -> Option<u8> {
        match b {
            b'A'..=b'Z' => Some(b - b'A'),
            b'a'..=b'z' => Some(b - b'a' + 26),
            b'0'..=b'9' => Some(b - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }
    let s = s.trim_end_matches('=');
    let mut out = Vec::with_capacity(s.len() * 3 / 4);
    let mut it = s.bytes();
    loop {
        let a = match it.next() {
            None => break,
            Some(b) => v(b)?,
        };
        let (b_, c_, d_) = (it.next().map(|x| v(x).unwrap_or(0)), it.next().map(|x| v(x).unwrap_or(0)), it.next().map(|x| v(x).unwrap_or(0)));
        let (b_, c_, d_) = match (b_, c_, d_) {
            (Some(b), c, d) => (b, c, d),
            _ => return None, // a trailing 6-bit group alone is not valid base64
        };
        out.push((a << 2) | (b_ >> 4));
        if let Some(c) = c_ {
            out.push((b_ << 4) | (c >> 2));
            if let Some(d) = d_ {
                out.push((c << 6) | d);
            }
        }
    }
    Some(out)
}
