//! RepoDo — one Durable Object per repo: refs, meta, pushes, jobs tables; internal HTTP routes.
//! CONTRACTS.md 1.3, 3, 8. Ported from repo-do-ref-authority + refs-sqlite-objects-r2 + auth proofs.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;

use bstr::{BString, ByteSlice};
use futures_util::{stream, StreamExt};
use gix_hash::ObjectId;
use worker::{durable_object, DurableObject, Env, Method, Request, Response, SqlCursor, SqlStorage, SqlStorageValue as V, State};

use crate::error::Error;
use crate::jobs::{self, JobKind};
use crate::pack::generate;
use crate::platform;
use crate::store::{keys, schema, Bucket, Index, ObjLoc, PackId, RepoId};
use crate::wire::{self, http::{do_error_response, json, parse, RepoHeaders}, FetchArgs, PktWriter, RefRow, V2Command};
use crate::{ReqBudget, Spend};

/// A ref row is ~90 bytes in an ls-refs advertisement — a token holder could otherwise
/// mint refs until the advertisement alone exceeds the isolate. 65k refs is already huge.
const MAX_REFS: i64 = 65_536;

/// A26 quota defaults for the small-disposable-app profile (ROADMAP P1 #9): bite abuse
/// before the structural limits (2M objects/pack, 2 GiB/push) do. Env vars override;
/// <= 0 disables that cap.
const DEFAULT_MAX_REPOS_PER_OWNER: i64 = 50; // GE_QUOTA_MAX_REPOS_PER_OWNER
const DEFAULT_MAX_OBJECTS: i64 = 2_000_000; // GE_QUOTA_MAX_OBJECTS
const DEFAULT_MAX_BYTES: i64 = 4 << 30; // GE_QUOTA_MAX_BYTES (4 GiB)
/// A27: pushes per minute per credential per repo (GE_RATE_PUSHES_PER_MIN). Abuse
/// damping only — the heavy hammer is a zone-level Cloudflare rate-limit rule.
const DEFAULT_PUSHES_PER_MIN: i64 = 30;

/// A28: memoized advertisement state, valid for exactly one refs_version. `ls` holds a
/// few (request body -> response body) pairs for /_do/ls-refs so a lazy-mount poller
/// repeating an identical ls-refs skips the parse+render too.
struct RefsMemo {
    version: i64,
    /// object format + pin state at memo build — a pin can land without a
    /// refs_version bump, so validity is (version, kind, pinned)
    kind: gix_hash::Kind,
    pinned: bool,
    head: Option<BString>,
    refs: Vec<RefRow>,
    json: Vec<u8>, // the /_do/refs response body
    ls: RefCell<Vec<(Vec<u8>, Vec<u8>)>>,
}

#[derive(serde::Deserialize, Clone)]
pub struct CmdDto {
    old: String,
    new: String,
    name: String,
    peeled: Option<String>,
}
#[derive(serde::Deserialize)]
pub struct CommitRequest {
    pub push_id: String,
    pub pack_id: Option<String>,
    pub principal: String,
    pub commands: Vec<CmdDto>,
    /// `--atomic`: all commands land or none do (capability advertised).
    #[serde(default)]
    pub atomic: bool,
}
#[derive(serde::Serialize)]
pub struct CommitResponse {
    pub results: Vec<(String, Option<&'static str>)>, // None = ok
}
#[derive(serde::Deserialize)]
struct BeginDto {
    push_id: String,
    principal: String,
    /// sha1 of the presented token — the A27 rate-limit bucket key.
    #[serde(default)]
    key: String,
    /// the client's negotiated `object-format` — a v0 push carries it; the
    /// staging pushes import_begin makes leave it unset (format is pinned at
    /// import_start / url-fetch time instead)
    #[serde(default)]
    object_format: Option<String>,
    /// PR-3a: the edge matched the global GE_WRITE_TOKEN — the only credential
    /// that pushes without a tokens row. Absent (older edge) = false: fail closed.
    #[serde(default)]
    admin: bool,
}
#[derive(serde::Deserialize)]
struct ImportStartDto {
    push: String,
    pack: String,
    #[serde(default)]
    parts: Vec<serde_json::Value>,
    #[serde(default)]
    commands: Vec<serde_json::Value>,
    /// WILD url import: no client parts — the job's fetch phase clones the
    /// remote and rewrites this payload with the staged shape
    #[serde(default)]
    url: Option<String>,
    /// 'sha1'|'sha256' — the staged pack's object format; a url import leaves
    /// it unset and the fetch phase pins whatever the remote advertises
    #[serde(default)]
    format: Option<String>,
}
#[derive(serde::Deserialize)]
struct AuthDto {
    hash: String,
}
#[derive(serde::Deserialize)]
struct NewToken {
    name: String,
    level: String,
    /// #19: optional ref scope — a comma list of ref patterns, `*` allowed only
    /// as a trailing wildcard (e.g. `refs/heads/release-*`, `refs/heads/dev/*`).
    /// Omitted/empty at mint = all refs, stored explicitly as `ALL_REFS` — a
    /// stored scope is never empty, so an empty one at check time denies.
    scope: Option<String>,
    /// PR-3: optional lifetime in seconds (1..=MAX_TOKEN_TTL_S). Omitted = no
    /// expiry (existing behaviour). Expiry is checked at auth, begin and commit.
    #[serde(default)]
    ttl_s: Option<i64>,
}
#[derive(serde::Deserialize)]
struct RevokeDto {
    id: String,
}
#[derive(serde::Deserialize)]
struct PublicDto {
    enabled: Option<bool>,
}
#[derive(serde::Deserialize)]
struct PinDto {
    #[serde(rename = "ref")]
    name: String,
    sha: String,
}
#[derive(serde::Deserialize)]
struct ProtectDto {
    /// a ref pattern in the token-scope grammar: `refs/heads/main`, `refs/tags/*`
    #[serde(rename = "ref")]
    pattern: String,
}
#[derive(serde::Deserialize)]
struct UnpinDto {
    #[serde(rename = "ref")]
    name: String,
}
#[derive(serde::Deserialize)]
struct N {
    n: i64,
}
#[derive(serde::Deserialize)]
struct PushRow {
    state: String,
    gc_epoch: i64,
    scope: Option<String>,
    /// PR-3b: who opened the push — `ADMIN_AUTH`, or the presenting token's
    /// hash — so commit can re-validate the credential, not just trust begin.
    auth: Option<String>,
}
struct Cmd {
    old: ObjectId,
    new: ObjectId,
    name: String,
    peeled: Option<String>,
}

pub struct Meta {
    pub repo_id: String,
    pub owner: String,
    pub repo: String,
    pub head: String,
    pub refs_version: i64,
    pub gc_epoch: i64,
    pub deleted: bool,
}

#[durable_object]
pub struct RepoDo {
    pub(crate) state: State,
    pub(crate) env: Env,
    booted: RefCell<bool>,
    refs_memo: RefCell<Option<Rc<RefsMemo>>>,
    memo_hits: Cell<u64>,
    memo_misses: Cell<u64>,
}

impl DurableObject for RepoDo {
    fn new(state: State, env: Env) -> Self {
        Self {
            state,
            env,
            booted: RefCell::new(false),
            refs_memo: RefCell::new(None),
            memo_hits: Cell::new(0),
            memo_misses: Cell::new(0),
        }
    }

    async fn fetch(&self, mut req: Request) -> worker::Result<Response> {
        let hdr = RepoHeaders::from_request(&req);
        let body = req.bytes().await?; // the only await on a "none" route
        let path = req.path();
        let method = req.method();
        // ---- sync span from here to the response for every "none" route ----
        // A26: `owner!<owner>` registry DOs never boot as repos — no meta, no jobs,
        // no alarm. Their routes are intercepted before boot.
        let out: Result<Option<Response>, Error> = if path.starts_with("/_owner/") {
            self.owner_route(&method, &path, &hdr).map(Some)
        } else {
            self.boot(&hdr).and_then(|meta| {
            if meta.deleted {
                return Err(Error::Gone); // tombstoned: every repo route is 410
            }
            match (&method, path.as_str()) {
                (Method::Get, "/_do/refs") => self.refs_route(&meta),
                (Method::Get, "/_do/state") => self.debug_state(),
                (Method::Post, "/_do/push/begin") => self.push_begin(&meta, &parse::<BeginDto>(&body)?),
                (Method::Post, "/_do/push/lookup") => self.push_lookup(&parse(&body)?),
                (Method::Post, "/_do/push/index") => self.push_index(&parse(&body)?),
                (Method::Post, "/_do/push/abort") => self.push_abort(&parse(&body)?),
                (Method::Post, "/_do/push/commit") => {
                    self.commit_push(&parse::<CommitRequest>(&body)?).and_then(|r| json(serde_json::to_value(&r)?))
                }
                (Method::Post, "/_do/import/start") => self.import_start(&meta, &parse::<ImportStartDto>(&body)?),
                (Method::Post, "/_do/import/status") => self.import_status(&parse(&body)?),
                (Method::Post, "/_do/ls-refs") => self.ls_refs(&meta, &body),
                (Method::Post, "/_do/auth") => self.auth_lookup(&parse(&body)?),
                (Method::Post, "/_do/tokens") => self.token_create(&parse(&body)?),
                (Method::Get, "/_do/tokens") => self.token_list(),
                (Method::Post, "/_do/tokens/revoke") => self.token_revoke(&parse(&body)?),
                (Method::Post, "/_do/delete") => self.delete_repo(),
                (Method::Post, "/_do/public") => self.set_public(&parse::<PublicDto>(&body)?),
                (Method::Post, "/_do/format") => self.set_format(&body),
                (Method::Post, "/_do/pin") => self.pin(&parse::<PinDto>(&body)?),
                (Method::Post, "/_do/unpin") => self.unpin(&parse::<UnpinDto>(&body)?),
                (Method::Post, "/_do/protect") => self.protect(&parse::<ProtectDto>(&body)?),
                (Method::Post, "/_do/unprotect") => self.unprotect(&parse::<ProtectDto>(&body)?),
                // PR0 test hooks (testhooks.rs): the pure-SQL routes. The flag
                // check is the second gate (the edge already 404s) — with
                // GE_TEST_HOOKS off these fall through to the same NotFound as
                // any unknown route.
                (Method::Post, "/_do/test/jobs") if crate::testhooks::enabled(self) => crate::testhooks::jobs_view(self),
                (Method::Post, "/_do/test/failpoint") if crate::testhooks::enabled(self) => crate::testhooks::set_failpoint(self, &body),
                (Method::Post, "/_do/test/repair") if crate::testhooks::enabled(self) => crate::testhooks::repair(self),
                (Method::Post, "/_do/test/drop_object") if crate::testhooks::enabled(self) => crate::testhooks::drop_object(self, &body),
                (Method::Post, "/_do/test/knob") if crate::testhooks::enabled(self) => crate::testhooks::set_knob(self, &body),
                (Method::Post, "/_do/test/meta") if crate::testhooks::enabled(self) => crate::testhooks::set_meta(self, &body),
                (Method::Post, "/_do/test/enqueue") if crate::testhooks::enabled(self) => crate::testhooks::enqueue_job(self, &body),
                _ => return Ok(None),
            }
            .map(Some)
            })
        };
        let resp = match out {
            Ok(Some(r)) => Ok(r),
            Ok(None) => match (&method, path.as_str()) {
                // the await route lives outside the sync span (1.3); boot may still have
                // enqueued jobs in its span, so a successful fetch must rearm the alarm
                (Method::Post, "/_do/fetch") => {
                    let r = self.fetch_v2(&body, &hdr).await;
                    // boot may have enqueued jobs in this span — rearm even on error;
                    // a propagated Storage/Internal error rolls the span back anyway
                    let _ = jobs::rearm(self).await;
                    return r;
                }
                (Method::Post, "/_do/export") => {
                    let r = self.export_bundle().await;
                    let _ = jobs::rearm(self).await;
                    return r;
                }
                (Method::Post, "/_do/lfs/batch") => {
                    let r = self.lfs_batch(&body, &hdr).await;
                    let _ = jobs::rearm(self).await;
                    return r;
                }
                // PR0 test hooks that await (job slice, R2 reads) — the flag is
                // re-checked inside each handler, so off = NotFound either way.
                (Method::Post, "/_do/test/step") => {
                    let r = crate::testhooks::step(self, &body).await;
                    let _ = jobs::rearm(self).await;
                    return test_resp(r);
                }
                (Method::Post, "/_do/test/fsck") => {
                    return test_resp(crate::testhooks::fsck(self, &body).await);
                }
                (Method::Post, "/_do/test/drop_pack") => {
                    return test_resp(crate::testhooks::drop_pack(self, &body).await);
                }
                _ => Err(Error::NotFound),
            },
            Err(e) => Err(e),
        };
        match resp {
            Ok(r) => {
                // A3/A11: every span that can enqueue gets an unconditional rearm after it.
                let _ = jobs::rearm(self).await;
                Ok(r)
            }
            // Storage/Internal propagate so the platform rolls back the uncommitted span (A2).
            Err(e @ (Error::Storage(_) | Error::Internal(_))) => {
                Err(worker::Error::RustError(e.message()))
            }
            Err(e) => {
                // non-fatal errors commit the span — a boot-time enqueue survives, so rearm.
                // Spend is 0: every sync-span route above is pure SQLite; the only
                // R2-charging routes (fetch/export) are awaited outside the span.
                let r = do_error_response(&e, 0)?;
                let _ = jobs::rearm(self).await;
                Ok(r)
            }
        }
    }

    async fn alarm(&self) -> worker::Result<Response> {
        self.boot(&RepoHeaders::NONE)?; // alarm has no headers, section 8.2
        if crate::testhooks::manual_jobs(self) {
            // GE_TEST_MANUAL_JOBS: slices run only via /_do/test/step. Clear the
            // alarm rather than rearm — a queued row with run_at<=now would
            // refire instantly and spin; the step hook rearms as needed.
            let _ = self.state.storage().delete_alarm().await;
        } else {
            let _ = jobs::dispatch(self).await; // never lets an Err escape, section 4.4
        }
        Response::empty()
    }
}

/// The awaited test routes (PR0): errors answer as the usual DO JSON error —
/// NotFound included, so a flag-off request is indistinguishable from an
/// unknown route. Awaited routes have no open sync span left to roll back.
fn test_resp(r: Result<Response, Error>) -> worker::Result<Response> {
    match r {
        Ok(r) => Ok(r),
        Err(e) => do_error_response(&e, 0).map_err(worker::Error::from),
    }
}

pub use crate::store::oid; // hex->ObjectId boundary parse — canonical home is store

/// A full valid refname under refs/ — the same gate apply_one uses on push commands.
fn valid_ref(name: &str) -> Result<&str, Error> {
    if !name.as_bytes().starts_with(b"refs/")
        || gix_validate::reference::name(name.as_bytes().as_bstr()).is_err()
    {
        return Err(Error::Protocol("bad ref name".into()));
    }
    Ok(name)
}

/// #19 token scope: one pattern is `refs/…` text, `*` allowed only trailing
/// (prefix glob). Rejects anything else so a malformed scope can't widen access.
fn scope_pattern_ok(p: &str) -> bool {
    let p = p.strip_suffix('*').unwrap_or(p);
    if !p.starts_with("refs/") || p.contains('*') {
        return false;
    }
    // `ns/*` strips to `ns/` — validate the namespace prefix, not the
    // trailing-slash form name_partial rejects
    let name = p.strip_suffix('/').unwrap_or(p);
    !name.is_empty()
        && gix_validate::reference::name_partial(name.as_bytes().as_bstr()).is_ok()
}

/// The explicit "every ref" scope (PR-3a). Minted for an unscoped token and
/// captured for a GE_WRITE_TOKEN push; apply_one already confines refs to
/// `refs/`, so this is exactly the old unrestricted behaviour, written down.
pub(crate) const ALL_REFS: &str = "refs/*";

/// PR-3b: `pushes.auth` marker for a GE_WRITE_TOKEN push. Token hashes are
/// 40/64 hex, so this can never collide with one.
const ADMIN_AUTH: &str = "admin";

/// The `ng` reason when the push's credential no longer checks out at commit.
const TOKEN_INVALID: &str = "token no longer valid";

/// PR-3b: re-validate a push's credential at commit, in the same sync span as
/// the ref writes (no TOCTOU against a revoke). `auth` is the push row's
/// marker, `row` the presenting token's current (level, scope) if it still
/// exists. Ok(None) = admin (only the captured scope applies); Ok(Some(s)) =
/// the token's *current* scope, which every command must ALSO satisfy, so a
/// scope narrowed mid-push takes effect and a widened one doesn't. A legacy
/// row with no marker, a vanished token or a non-write token: Err — deny.
fn commit_auth(auth: Option<&str>, row: Option<(&str, Option<&str>)>) -> Result<Option<String>, &'static str> {
    match auth.map(str::trim).filter(|a| !a.is_empty()) {
        None => Err(TOKEN_INVALID),
        Some(ADMIN_AUTH) => Ok(None),
        Some(_) => match row {
            None => Err(TOKEN_INVALID),
            Some((level, _)) if level != "write" => Err(TOKEN_INVALID),
            Some((_, scope)) => scope
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(|s| Some(s.to_string()))
                .ok_or(TOKEN_INVALID),
        },
    }
}

/// A command ref matches a scope when any comma pattern covers it — exact match,
/// or prefix when the pattern ends in `*`. PR-3a: missing or empty scope DENIES.
/// "All refs" is the explicit `ALL_REFS` pattern, never the absence of one —
/// so a lost token row or a NULL column can't widen a push to every ref.
pub(crate) fn scope_allows(scope: Option<&str>, name: &str) -> bool {
    match scope.map(str::trim).filter(|s| !s.is_empty()) {
        None => false,
        Some(s) => s.split(',').map(str::trim).filter(|p| !p.is_empty()).any(|p| {
            if let Some(prefix) = p.strip_suffix('*') {
                name.starts_with(prefix)
            } else {
                name == p
            }
        }),
    }
}

/// PR-3a: the scope a new push row captures. The global admin gets `ALL_REFS`;
/// a repo token must still exist, be write-level, and carry a non-empty scope.
/// Anything else is refused at begin — fail closed, nothing staged.
fn push_scope(admin: bool, key: &str, row: Option<(&str, Option<&str>)>) -> Result<String, Error> {
    if admin {
        return Ok(ALL_REFS.to_string());
    }
    if key.is_empty() {
        return Err(Error::Auth);
    }
    match row {
        // revoked between edge auth and begin
        None => Err(Error::Auth),
        Some((level, _)) if level != "write" => Err(Error::Forbidden),
        Some((_, scope)) => scope
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .ok_or(Error::Forbidden),
    }
}

/// Protected refs: the FF walk's row budget — past it, ancestry is unproven
/// and the update is refused (fail closed), never assumed.
const FF_WALK_MAX: i64 = 200_000;

/// Protected refs: why a command on a protected ref is refused, or None. Pure
/// so the rule table is unit-tested. `ff` is the ancestry verdict for an update
/// (Some(true) = old is an ancestor of new, Some(false) = provably not,
/// None = unproven: missing graph rows or walk budget hit).
fn protected_verdict(old_null: bool, new_null: bool, ff: Option<bool>) -> Option<&'static str> {
    if new_null {
        return Some("protected ref: deletion refused");
    }
    if old_null {
        return None; // create: an absent protected ref may be born once; deletion can't reset it
    }
    match ff {
        Some(true) => None,
        Some(false) => Some("protected ref: non-fast-forward"),
        None => Some("protected ref: cannot prove fast-forward"),
    }
}

/// PR-3: longest lifetime a minted token may ask for — one year.
const MAX_TOKEN_TTL_S: i64 = 366 * 24 * 3600;

/// PR-3: a token row is live until `expires_at` (ms, exclusive). NULL = no expiry.
/// Every lookup filters through this, so an expired token reads as a missing one
/// and inherits the same fail-closed paths (auth 401, begin Auth, commit ng).
fn token_live(expires_at: Option<i64>, now: i64) -> bool {
    expires_at.map_or(true, |e| now < e)
}

/// PR-3: mint-time `ttl_s` → stored `expires_at` (ms). Out-of-range is refused,
/// never clamped, so a caller can't silently get a longer-lived credential.
fn mint_expiry(ttl_s: Option<i64>, now: i64) -> Result<Option<i64>, Error> {
    match ttl_s {
        None => Ok(None),
        Some(t) if (1..=MAX_TOKEN_TTL_S).contains(&t) => Ok(Some(now + t * 1000)),
        Some(_) => Err(Error::Protocol(format!("ttl_s must be 1..={MAX_TOKEN_TTL_S}"))),
    }
}

/// PR-3a: normalize a mint-time scope — omitted/empty becomes the explicit
/// `ALL_REFS`, anything else must be valid patterns. Never returns empty.
fn mint_scope(scope: Option<&str>) -> Result<String, Error> {
    let Some(s) = scope.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok(ALL_REFS.to_string());
    };
    if s.len() > 256 || s.split(',').map(str::trim).any(|p| !scope_pattern_ok(p)) {
        return Err(Error::Protocol("scope must be <=256 bytes of refs/… patterns (trailing * only)".into()));
    }
    Ok(s.to_string())
}
/// The /_do/refs DTO body — built once per refs_version and memoized (A28).
/// `obj_format` lets the edge advertise the right `object-format=` capability.
fn refs_value(head: &Option<BString>, refs: &[RefRow], kind: gix_hash::Kind, pinned: bool) -> serde_json::Value {
    serde_json::json!({
        "head": head.as_ref().map(|h| h.to_string()),
        "obj_format": if kind == gix_hash::Kind::Sha256 { "sha256" } else { "sha1" },
        "obj_format_pinned": pinned,
        "refs": refs.iter().map(|r| serde_json::json!({
            "name": r.name.to_string(), "target": r.target.to_string(),
            "peeled": r.peeled.map(|p| p.to_string()) })).collect::<Vec<_>>()
    })
}

impl RepoDo {
    pub fn sql(&self) -> SqlStorage {
        self.state.storage().sql()
    }
    /// `purge_repo` may drop the schema under a live `booted` flag; the next
    /// request must re-run `schema::migrate` or every query hits missing tables.
    pub(crate) fn unboot(&self) {
        *self.booted.borrow_mut() = false;
    }
    pub fn q(&self, s: &str, args: Vec<V>) -> Result<SqlCursor, Error> {
        crate::store::exec(&self.sql(), s, args)
    }
    /// The one CAS oracle (section 3): issued right after the write, same span. Never rows_written.
    pub fn changes(&self) -> Result<i64, Error> {
        Ok(self.q("SELECT changes() AS n", vec![])?.one::<N>()?.n)
    }
    pub fn meta(&self, key: &str) -> Result<String, Error> {
        // one() on an empty cursor throws at the JS boundary (not a catchable Rust
        // Err) — a missing key must not crash the isolate, so go through meta_opt
        self.meta_opt(key)?.ok_or_else(|| Error::Storage(format!("meta.{key} missing")))
    }
    pub fn meta_opt(&self, key: &str) -> Result<Option<String>, Error> {
        #[derive(serde::Deserialize)]
        struct S {
            value: String,
        }
        Ok(self
            .q("SELECT value FROM meta WHERE key=?", vec![V::from(key)])?
            .to_array::<S>()?
            .into_iter()
            .next()
            .map(|s| s.value))
    }
    pub fn meta_i64(&self, key: &str) -> Result<i64, Error> {
        self.meta(key)?.parse().map_err(|_| Error::Internal(format!("meta.{key}")))
    }
    fn ref_count(&self) -> Result<i64, Error> {
        Ok(self.q("SELECT COUNT(*) AS n FROM refs", vec![])?.one::<N>()?.n)
    }
    pub fn bucket(&self) -> Result<Bucket, Error> {
        Ok(Bucket::new(self.env.get_binding("BUCKET")?, self.repo_id()?))
    }
    pub fn repo_id(&self) -> Result<RepoId, Error> {
        Ok(RepoId(self.meta("repo_id")?))
    }
    /// The repo's object format — `meta.obj_format` pins it ('sha1'|'sha256');
    /// unset means sha1 (every repo predating the feature is sha1).
    pub fn obj_kind(&self) -> Result<gix_hash::Kind, Error> {
        Ok(match self.meta_opt("obj_format")?.as_deref() {
            Some("sha256") => gix_hash::Kind::Sha256,
            _ => gix_hash::Kind::Sha1,
        })
    }
    /// 'sha1'|'sha256' ↔ gix_hash::Kind, shared by every DTO that names a format.
    /// Kind is #[non_exhaustive] upstream — an unknown variant falls back to sha1.
    pub fn kind_name(kind: gix_hash::Kind) -> &'static str {
        match kind {
            gix_hash::Kind::Sha256 => "sha256",
            _ => "sha1",
        }
    }
    pub fn parse_kind(s: Option<&str>) -> Result<Option<gix_hash::Kind>, Error> {
        match s {
            None => Ok(None),
            Some("sha1") => Ok(Some(gix_hash::Kind::Sha1)),
            Some("sha256") => Ok(Some(gix_hash::Kind::Sha256)),
            Some(other) => Err(Error::Protocol(format!("object-format {other} unsupported"))),
        }
    }
    /// Whether the format has been pinned — unpinned repos advertise both
    /// formats and the first write locks the client's choice in.
    pub fn obj_format_pinned(&self) -> Result<bool, Error> {
        Ok(self.meta_opt("obj_format")?.is_some())
    }
    /// v2 negotiation check: a pinned repo only answers for its own format.
    /// Unpinned repos accept either — there are no objects to contradict yet —
    /// and the effective kind is the client's, so e.g. an empty pack's trailer
    /// is the width a sha256 client expects.
    pub fn check_object_format(&self, client: gix_hash::Kind) -> Result<gix_hash::Kind, Error> {
        let repo = self.obj_kind()?;
        if !self.obj_format_pinned()? {
            return Ok(client);
        }
        if client != repo {
            let (r, c) = (Self::kind_name(repo), Self::kind_name(client));
            return Err(Error::Protocol(format!("object-format {c} does not match repo {r}")));
        }
        Ok(repo)
    }
    /// Pin the object format. CAS: only the first writer sets it; a later
    /// writer with the same kind is a no-op, a different kind is a conflict
    /// the caller turns into a protocol error.
    pub fn pin_obj_format(&self, kind: gix_hash::Kind) -> Result<(), Error> {
        let name = Self::kind_name(kind);
        self.q(
            "INSERT INTO meta(key,value) VALUES('obj_format',?) ON CONFLICT(key) DO NOTHING",
            vec![V::from(name)],
        )?;
        if self.obj_kind()? != kind {
            return Err(Error::Conflict(format!(
                "repo is {} — cannot accept {name} objects",
                if kind == gix_hash::Kind::Sha1 { "sha256" } else { "sha1" }
            )));
        }
        Ok(())
    }
    /// GC quiet period in ms — GE_GC_QUIET_MS overrides the 10-minute contract default so
    /// the chain can be exercised in tests and dev without waiting.
    pub fn gc_quiet_ms(&self) -> i64 {
        self.env_ms("GE_GC_QUIET_MS", 600_000)
    }
    /// GC grace period in ms — GE_GC_GRACE_MS overrides the contract's 1-hour window.
    /// Below it, packs stay out of `marked` entirely so a push can never lose bytes mid-flight.
    pub fn gc_grace_ms(&self) -> i64 {
        self.env_ms("GE_GC_GRACE_MS", 3_600_000)
    }
    fn env_ms(&self, name: &str, default: i64) -> i64 {
        self.env_i64(name, default)
    }
    /// Any integer knob: compiled-in default, `env.var` override, unparseable falls back.
    fn env_i64(&self, name: &str, default: i64) -> i64 {
        self.env
            .var(name)
            .ok()
            .and_then(|v| v.to_string().parse::<i64>().ok())
            .unwrap_or(default)
    }

    /// A26: `owner!<owner>` registry routes — the cross-repo quota oracle. These DOs
    /// never boot as repos: schema migrate runs lazily, but no meta rows, no jobs,
    /// no alarm are created. `claim:<repo>` keys live in `meta` (no schema change).
    /// Identity arrives via the usual x-ge-owner/x-ge-repo headers.
    fn owner_route(&self, method: &Method, path: &str, hdr: &RepoHeaders) -> Result<Response, Error> {
        if !*self.booted.borrow() {
            schema::migrate(&self.sql())?;
            *self.booted.borrow_mut() = true;
        }
        let (owner, repo) = (
            hdr.owner.as_deref().ok_or_else(|| Error::Internal("owner route without identity".into()))?,
            hdr.repo.as_deref().ok_or_else(|| Error::Internal("owner route without identity".into()))?,
        );
        match (method, path) {
            // Idempotent claim of one of the owner's GE_QUOTA_MAX_REPOS_PER_OWNER slots.
            // Over-cap inserts are handed back so rejected names never accumulate.
            (Method::Post, "/_owner/claim") => {
                let cap = self.env_i64("GE_QUOTA_MAX_REPOS_PER_OWNER", DEFAULT_MAX_REPOS_PER_OWNER);
                if cap <= 0 {
                    return json(serde_json::json!({ "claimed": true, "cap": 0 }));
                }
                let key = format!("claim:{repo}");
                self.q(
                    "INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT DO NOTHING",
                    vec![V::from(key.as_str()), V::from(platform::now_ms())],
                )?;
                let inserted = self.changes()? == 1;
                let n = self
                    .q("SELECT COUNT(*) AS n FROM meta WHERE key LIKE 'claim:%'", vec![])?
                    .one::<N>()?
                    .n;
                if inserted && n > cap {
                    self.q("DELETE FROM meta WHERE key=?", vec![V::from(key.as_str())])?;
                    return Err(Error::Limit(format!(
                        "owner {owner} has {n} repos, over GE_QUOTA_MAX_REPOS_PER_OWNER={cap}"
                    )));
                }
                json(serde_json::json!({ "claimed": true, "repos": n, "cap": cap }))
            }
            // Called by repo delete (ROADMAP #3) once that lands — frees the slot.
            (Method::Post, "/_owner/release") => {
                self.q(
                    "DELETE FROM meta WHERE key=?",
                    vec![V::from(format!("claim:{repo}"))],
                )?;
                json(serde_json::json!({ "released": self.changes()? == 1 }))
            }
            _ => Err(Error::NotFound),
        }
    }

    /// Section 8.2. Sync span: migrate schema, write/verify meta, enqueue Janitor.
    pub fn boot(&self, hdr: &RepoHeaders) -> Result<Meta, Error> {
        let sql = self.sql();
        if !*self.booted.borrow() {
            schema::migrate(&sql)?;
            *self.booted.borrow_mut() = true;
        }
        #[derive(serde::Deserialize)]
        struct KV {
            key: String,
            value: String,
        }
        let rows = self.q("SELECT key, value FROM meta", vec![])?.to_array::<KV>()?;
        let get = |k: &str| rows.iter().find(|r| r.key == k).map(|r| r.value.clone());
        let meta = match get("repo_id") {
            Some(repo_id) => {
                let need = |k: &str| get(k).ok_or_else(|| Error::Internal(format!("meta.{k} missing")));
                let num = |k: &str| need(k)?.parse::<i64>().map_err(|_| Error::Internal(format!("meta.{k}")));
                Meta {
                    repo_id,
                    owner: need("owner")?,
                    repo: need("repo")?,
                    head: need("head")?,
                    refs_version: num("refs_version")?,
                    gc_epoch: num("gc_epoch")?,
                    deleted: get("deleted").is_some(),
                }
            }
            None => {
                let (owner, repo) = (
                    hdr.owner.clone().ok_or_else(|| Error::Internal("boot without headers".into()))?,
                    hdr.repo.clone().ok_or_else(|| Error::Internal("boot without headers".into()))?,
                );
                let (repo_id, now) = (platform::hex16()?, platform::now_ms());
                for (k, v) in [
                    ("repo_id", repo_id.as_str()),
                    ("owner", owner.as_str()),
                    ("repo", repo.as_str()),
                    ("head", "refs/heads/main"),
                    ("refs_version", "0"),
                    ("gc_epoch", "0"),
                    ("created_at", &now.to_string()),
                    ("schema_version", "1"),
                ] {
                    self.q(
                        "INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT DO NOTHING",
                        vec![V::from(k), V::from(v)],
                    )?;
                }
                jobs::enqueue(&sql, JobKind::Janitor, now, "{}")?;
                Meta {
                    repo_id,
                    owner,
                    repo,
                    head: "refs/heads/main".into(),
                    refs_version: 0,
                    gc_epoch: 0,
                    deleted: false,
                }
            }
        };
        if meta.deleted {
            // tombstoned: the only work left is purge_repo. Keep one queued (a dead
            // or missing row is re-enqueued at every boot), requeue a stranded
            // 'running' row, and skip repair — the other tables are about to be
            // dropped, so resurrecting janitor/GC would only race the wipe.
            let now = platform::now_ms();
            self.q(
                "UPDATE jobs SET state='queued', run_at=?, attempts=attempts+1, \
                 last_error='stranded mid-slice' \
                 WHERE kind='purge_repo' AND state='running' AND started_at < ?",
                vec![V::from(now), V::from(now.saturating_sub(60_000))],
            )?;
            #[derive(serde::Deserialize)]
            struct NJ {
                n: i64,
            }
            let n = self
                .q(
                    "SELECT COUNT(*) AS n FROM jobs WHERE kind='purge_repo' AND state IN ('queued','running')",
                    vec![],
                )?
                .one::<NJ>()?
                .n;
            if n == 0 {
                jobs::enqueue(&sql, JobKind::PurgeRepo, now, "{}")?;
            }
        } else {
            // A4: re-queue a dead maintenance job at every boot, and requeue 'running'
            // rows that a killed isolate stranded (A12).
            jobs::repair(&sql)?;
        }
        if let (Some(o), Some(r)) = (&hdr.owner, &hdr.repo) {
            if *o != meta.owner || *r != meta.repo {
                return Err(Error::Internal("identity mismatch".into()));
            }
        }
        // The one permitted ctx.id.name read (8.3): a cross-check that logs and decides nothing.
        let seen = platform::do_id_name(&self.state);
        if seen.as_deref() != Some(&format!("{}/{}", meta.owner, meta.repo)) {
            worker::console_log!("warn: ctx.id.name {seen:?} vs meta {}/{}", meta.owner, meta.repo);
        }
        Ok(meta)
    }

    /// GET /_do/state — internal observability: row counts per table and jobs by state.
    /// Not part of the git protocol surface; used by tests and ops to watch the job chain.
    fn debug_state(&self) -> Result<Response, Error> {
        #[derive(serde::Deserialize)]
        struct N {
            n: i64,
        }
        let count = |q: &str| -> Result<i64, Error> {
            Ok(self.q(q, vec![])?.one::<N>()?.n)
        };
        #[derive(serde::Deserialize)]
        struct Pin {
            name: String,
            sha: String,
        }
        let pins = self
            .q("SELECT name, sha FROM pins ORDER BY name", vec![])?
            .to_array::<Pin>()?;
        #[derive(serde::Deserialize)]
        struct Prot {
            pattern: String,
        }
        let protected = self
            .q("SELECT pattern FROM protected ORDER BY pattern", vec![])?
            .to_array::<Prot>()?;
        json(serde_json::json!({
            "refs": count("SELECT COUNT(*) AS n FROM refs")?,
            "objects": count("SELECT COUNT(*) AS n FROM objects")?,
            "packs_live": count("SELECT COUNT(*) AS n FROM packs WHERE state='live'")?,
            "packs_ingesting": count("SELECT COUNT(*) AS n FROM packs WHERE state='ingesting'")?,
            "packs_dead": count("SELECT COUNT(*) AS n FROM packs WHERE state='dead'")?,
            "jobs_queued": count("SELECT COUNT(*) AS n FROM jobs WHERE state='queued'")?,
            "jobs_running": count("SELECT COUNT(*) AS n FROM jobs WHERE state='running'")?,
            "jobs_dead": count("SELECT COUNT(*) AS n FROM jobs WHERE state='dead'")?,
            "pushes": count("SELECT COUNT(*) AS n FROM pushes")?,
            "marked": count("SELECT COUNT(*) AS n FROM marked")?,
            // A32: rows in the GC yield-persisted tail — >0 only while a
            // consolidate holds undrained buffered bytes between slices;
            // a nonzero count that never moves is the livelock tell
            "gc_tail": count("SELECT COUNT(*) AS n FROM gc_tail")?,
            "tokens": count("SELECT COUNT(*) AS n FROM tokens")?,
            "pins": pins
                .iter()
                .map(|p| serde_json::json!({ "ref": p.name, "sha": p.sha }))
                .collect::<Vec<_>>(),
            "protected": protected.iter().map(|p| p.pattern.as_str()).collect::<Vec<_>>(),
            "push_events_pending": count("SELECT COUNT(*) AS n FROM push_events")?,
            "push_events_dropped": self.meta_opt("push_events_dropped")?.and_then(|v| v.parse::<i64>().ok()).unwrap_or(0),
            "public": self.meta_opt("public")?.is_some(),
            "deleted": self.meta_opt("deleted")?.is_some(),
            "rate_rows": count("SELECT COUNT(*) AS n FROM rate")?,
            // A28 memo counters — hits/(hits+misses) is the advertisement-rebuild
            // saving a lazy-mount poller would otherwise cost per request
            "refs_memo_hits": self.memo_hits.get(),
            "refs_memo_misses": self.memo_misses.get(),
        }))
    }

    /// POST /_do/auth {hash} — edge authenticates a presented token by its sha1 hash.
    /// The raw token never crosses the stub boundary. No row: Auth, which the edge
    /// maps back to a 401 challenge.
    fn auth_lookup(&self, b: &AuthDto) -> Result<Response, Error> {
        #[derive(serde::Deserialize)]
        struct R {
            level: String,
            name: String,
            scope: Option<String>,
            expires_at: Option<i64>,
        }
        let now = platform::now_ms();
        let row = self
            .q("SELECT level, name, scope, expires_at FROM tokens WHERE hash=?", vec![V::from(b.hash.as_str())])?
            .to_array::<R>()?
            .into_iter()
            .next()
            .filter(|r| token_live(r.expires_at, now)) // PR-3: expired = unknown → 401
            .ok_or(Error::Auth)?;
        json(serde_json::json!({
            "level": row.level, "name": row.name, "scope": row.scope, "expires_at": row.expires_at
        }))
    }

    /// POST /_do/tokens {name, level, scope?} — the edge has already gated this on the global
    /// write token. Returns the token value once; only its sha1 hash is stored.
    fn token_create(&self, b: &NewToken) -> Result<Response, Error> {
        if !matches!(b.level.as_str(), "read" | "write") {
            return Err(Error::Protocol("level must be read or write".into()));
        }
        if b.name.is_empty() || b.name.len() > 128 {
            return Err(Error::Protocol("name must be 1-128 bytes".into()));
        }
        let scope = mint_scope(b.scope.as_deref())?;
        let now = platform::now_ms();
        let expires_at = mint_expiry(b.ttl_s, now)?;
        let n = self.q("SELECT COUNT(*) AS n FROM tokens", vec![])?.one::<N>()?.n;
        if n >= 256 {
            return Err(Error::Limit("too many tokens (256 max)".into()));
        }
        let token = format!("ge_{}{}", platform::hex16()?, platform::hex16()?);
        let id = platform::hex16()?;
        self.q(
            "INSERT INTO tokens(id,hash,level,name,created_at,scope,expires_at) VALUES(?,?,?,?,?,?,?)",
            vec![
                V::from(id.as_str()),
                V::from(crate::auth::token_hash(&token)?),
                V::from(b.level.as_str()),
                V::from(b.name.as_str()),
                V::from(now),
                V::from(scope.as_str()),
                expires_at.map(V::from).unwrap_or(V::Null),
            ],
        )?;
        json(serde_json::json!({
            "id": id, "token": token, "level": b.level, "name": b.name, "scope": scope,
            "expires_at": expires_at
        }))
    }

    /// GET /_do/tokens — id/name/level only; hashes and token values never leave.
    fn token_list(&self) -> Result<Response, Error> {
        #[derive(serde::Deserialize)]
        struct T {
            id: String,
            name: String,
            level: String,
            created_at: i64,
            scope: Option<String>,
            expires_at: Option<i64>,
        }
        let rows = self
            .q("SELECT id, name, level, created_at, scope, expires_at FROM tokens ORDER BY created_at", vec![])?
            .to_array::<T>()?;
        json(serde_json::json!({
            "tokens": rows
                .iter()
                .map(|t| serde_json::json!({
                    "id": t.id, "name": t.name, "level": t.level, "created_at": t.created_at,
                    "scope": t.scope, "expires_at": t.expires_at,
                }))
                .collect::<Vec<_>>()
        }))
    }

    /// POST /_do/tokens/revoke {id}.
    fn token_revoke(&self, b: &RevokeDto) -> Result<Response, Error> {
        self.q("DELETE FROM tokens WHERE id=?", vec![V::from(b.id.as_str())])?;
        json(serde_json::json!({ "revoked": self.changes()? > 0 }))
    }

    /// POST /_do/delete — tombstone the repo (meta.deleted) and enqueue purge_repo.
    /// Idempotent: the marker is a key upsert and enqueue dedups on a queued row.
    fn delete_repo(&self) -> Result<Response, Error> {
        let now = platform::now_ms();
        self.q(
            "INSERT INTO meta(key,value) VALUES('deleted',?) \
             ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            vec![V::from(now.to_string().as_str())],
        )?;
        jobs::enqueue(&self.sql(), JobKind::PurgeRepo, now, "{}")?;
        json(serde_json::json!({ "deleted": true }))
    }

    /// POST /_do/public {enabled} sets the anonymous-read flag (presence of
    /// meta.public); {} with no field is the edge's probe for unauthenticated reads.
    fn set_public(&self, b: &PublicDto) -> Result<Response, Error> {
        match b.enabled {
            Some(true) => {
                self.q(
                    "INSERT INTO meta(key,value) VALUES('public','1') \
                     ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    vec![],
                )?;
            }
            Some(false) => {
                self.q("DELETE FROM meta WHERE key='public'", vec![])?;
            }
            None => {}
        }
        json(serde_json::json!({ "public": self.meta_opt("public")?.is_some() }))
    }

    /// POST /_do/format {object_format} — pin the repo's hash algorithm before the
    /// first write. A v0 client can't create a sha256 repo unaided (its push needs
    /// the cap advertised first), so creating one goes through here, then the push.
    /// Re-pinning the same value is a no-op; a different one once pinned 409s.
    fn set_format(&self, body: &[u8]) -> Result<Response, Error> {
        #[derive(serde::Deserialize)]
        struct F {
            object_format: Option<String>,
        }
        let b: F = parse(body)?;
        let Some(kind) = Self::parse_kind(b.object_format.as_deref())? else {
            return Err(Error::Protocol("object_format required".into()));
        };
        self.pin_obj_format(kind)?;
        json(serde_json::json!({
            "obj_format": Self::kind_name(self.obj_kind()?),
            "pinned": self.obj_format_pinned()?,
        }))
    }

    /// POST /_do/pin {ref, sha} — freeze a ref at exactly sha. The ref must already
    /// resolve to it: a pin asserts the current value, it never moves a ref.
    fn pin(&self, b: &PinDto) -> Result<Response, Error> {
        let name = valid_ref(&b.name)?;
        let target = oid(&b.sha)?;
        if target.is_null() {
            return Err(Error::Protocol("pin sha must be non-zero".into()));
        }
        #[derive(serde::Deserialize)]
        struct T {
            target: String,
        }
        let cur = self
            .q("SELECT target FROM refs WHERE name=?", vec![V::from(name)])?
            .to_array::<T>()?
            .into_iter()
            .next();
        match cur {
            Some(t) if t.target == target.to_string() => {}
            _ => return Err(Error::Conflict(format!("{name} is not at {target}"))),
        }
        let n = self
            .q("SELECT COUNT(*) AS n FROM pins WHERE name<>?", vec![V::from(name)])?
            .one::<N>()?
            .n;
        if n >= 256 {
            return Err(Error::Limit("too many pins (256 max)".into()));
        }
        self.q(
            "INSERT INTO pins(name,sha,created_at) VALUES(?,?,?) \
             ON CONFLICT(name) DO UPDATE SET sha=excluded.sha",
            vec![V::from(name), V::from(target.to_string().as_str()), V::from(platform::now_ms())],
        )?;
        json(serde_json::json!({ "pinned": name, "sha": target.to_string() }))
    }

    /// POST /_do/unpin {ref}.
    fn unpin(&self, b: &UnpinDto) -> Result<Response, Error> {
        let name = valid_ref(&b.name)?;
        self.q("DELETE FROM pins WHERE name=?", vec![V::from(name)])?;
        json(serde_json::json!({ "unpinned": self.changes()? > 0 }))
    }

    /// POST /_do/protect {ref} — mark a ref pattern fast-forward-only and
    /// undeletable. Applies to every credential, GE_WRITE_TOKEN included:
    /// changing history on a protected ref means unprotecting it first, which
    /// is an explicit, separate admin call.
    fn protect(&self, b: &ProtectDto) -> Result<Response, Error> {
        let p = b.pattern.trim();
        if p.len() > 256 || !scope_pattern_ok(p) {
            return Err(Error::Protocol("ref must be a refs/… pattern (trailing * only)".into()));
        }
        let n = self
            .q("SELECT COUNT(*) AS n FROM protected WHERE pattern<>?", vec![V::from(p)])?
            .one::<N>()?
            .n;
        if n >= 64 {
            return Err(Error::Limit("too many protected patterns (64 max)".into()));
        }
        self.q(
            "INSERT OR IGNORE INTO protected(pattern,created_at) VALUES(?,?)",
            vec![V::from(p), V::from(platform::now_ms())],
        )?;
        json(serde_json::json!({ "protected": p }))
    }

    /// POST /_do/unprotect {ref} — exact pattern match.
    fn unprotect(&self, b: &ProtectDto) -> Result<Response, Error> {
        self.q("DELETE FROM protected WHERE pattern=?", vec![V::from(b.pattern.trim())])?;
        json(serde_json::json!({ "unprotected": self.changes()? > 0 }))
    }

    /// Whether any protected pattern covers this ref name.
    fn is_protected(&self, name: &str) -> Result<bool, Error> {
        #[derive(serde::Deserialize)]
        struct P {
            pattern: String,
        }
        let pats = self.q("SELECT pattern FROM protected", vec![])?.to_array::<P>()?;
        Ok(pats.iter().any(|p| scope_allows(Some(&p.pattern), name)))
    }

    /// Protected refs: is `old` an ancestor of (or equal to) `new`? Walks the
    /// commit_parents graph from `new` inside the commit span — the push's own
    /// commits were recorded at index time, so no R2 read. Some(false) only
    /// when the whole reachable set was recorded and `old` isn't in it.
    fn ff_ancestor(&self, old: &str, new: &str) -> Result<Option<bool>, Error> {
        #[derive(serde::Deserialize)]
        struct W {
            hit: i64,
            unknown: i64,
            seen: i64,
        }
        let w = self
            .q(
                "WITH RECURSIVE anc(sha) AS (SELECT ?1 UNION SELECT p.parent FROM commit_parents p                  JOIN anc ON p.sha=anc.sha WHERE p.n>=0 LIMIT ?3)                  SELECT (SELECT COUNT(*) FROM anc WHERE sha=?2) AS hit,                  (SELECT COUNT(*) FROM anc a WHERE NOT EXISTS                    (SELECT 1 FROM commit_parents k WHERE k.sha=a.sha AND k.n=-1)) AS unknown,                  (SELECT COUNT(*) FROM anc) AS seen",
                vec![V::from(new), V::from(old), V::from(FF_WALK_MAX)],
            )?
            .one::<W>()?;
        Ok(if w.hit > 0 {
            Some(true)
        } else if w.unknown > 0 || w.seen >= FF_WALK_MAX {
            None
        } else {
            Some(false)
        })
    }

    /// The pinned sha for a ref name — one row in `pins` freezes the ref.
    fn pin_sha(&self, name: &str) -> Result<Option<String>, Error> {
        #[derive(serde::Deserialize)]
        struct P {
            sha: String,
        }
        Ok(self
            .q("SELECT sha FROM pins WHERE name=?", vec![V::from(name)])?
            .to_array::<P>()?
            .into_iter()
            .next()
            .map(|r| r.sha))
    }

    /// A28: refs snapshot memoized on refs_version. A push is the only writer and
    /// bumps the version inside the commit span, so a matching version is a
    /// byte-stable answer; an evicted DO just rebuilds once. Both advertisement
    /// routes share the snapshot — the edge still does auth and the protocol-version
    /// branch per request.
    fn refs_snapshot(&self, meta: &Meta) -> Result<Rc<RefsMemo>, Error> {
        let (kind, pinned) = (self.obj_kind()?, self.obj_format_pinned()?);
        if let Some(m) = self.refs_memo.borrow().as_ref() {
            if m.version == meta.refs_version && m.kind == kind && m.pinned == pinned {
                self.memo_hits.set(self.memo_hits.get() + 1);
                return Ok(Rc::clone(m));
            }
        }
        let (head, refs) = self.list_refs()?;
        let json =
            serde_json::to_vec(&refs_value(&head, &refs, kind, pinned)).map_err(|e| Error::Internal(e.to_string()))?;
        let m = Rc::new(RefsMemo {
            version: meta.refs_version,
            kind,
            pinned,
            head,
            refs,
            json,
            ls: RefCell::new(Vec::new()),
        });
        self.memo_misses.set(self.memo_misses.get() + 1);
        *self.refs_memo.borrow_mut() = Some(Rc::clone(&m));
        Ok(m)
    }

    /// GET /_do/refs — memoized body; stamps x-ge-refs-version so the edge can echo
    /// it on info/refs responses (a poller can watch it move).
    fn refs_route(&self, meta: &Meta) -> Result<Response, Error> {
        let m = self.refs_snapshot(meta)?;
        let r = Response::from_bytes(m.json.clone()).map_err(|e| Error::Internal(e.to_string()))?;
        let h = r.headers();
        h.set("Content-Type", "application/json").map_err(|e| Error::Internal(e.to_string()))?;
        h.set("x-ge-refs-version", &m.version.to_string()).map_err(|e| Error::Internal(e.to_string()))?;
        Ok(r)
    }

    /// GET /_do/refs (1.3, awaits: none).
    pub fn list_refs(&self) -> Result<(Option<BString>, Vec<RefRow>), Error> {
        #[derive(serde::Deserialize)]
        struct R {
            name: String,
            target: String,
            peeled: Option<String>,
        }
        let head = self.meta("head")?;
        let mut refs = Vec::new();
        for r in self
            .q("SELECT name, target, peeled FROM refs ORDER BY name", vec![])?
            .to_array::<R>()?
        {
            let target =
                ObjectId::from_hex(r.target.as_bytes()).map_err(|_| Error::Internal("bad oid in refs".into()))?;
            let peeled = r
                .peeled
                .as_deref()
                .map(|p| ObjectId::from_hex(p.as_bytes()).map_err(|_| Error::Internal("bad peeled".into())))
                .transpose()?;
            refs.push(RefRow { name: r.name.into(), target, peeled });
        }
        Ok((Some(head.into()), refs))
    }

    /// POST /_do/ls-refs (raw v2 body, awaits: none). Response bytes memoized per
    /// (refs_version, request body) — a poller's identical ls-refs skips the parse
    /// and render entirely (A28).
    fn ls_refs(&self, meta: &Meta, body: &[u8]) -> Result<Response, Error> {
        let m = self.refs_snapshot(meta)?;
        if let Some((_, out)) = m.ls.borrow().iter().find(|(b, _)| b.as_slice() == body) {
            return Response::from_bytes(out.clone()).map_err(|e| Error::Internal(e.to_string()));
        }
        let args = match wire::parse_v2_command(body)? {
            V2Command::LsRefs(a) => a,
            _ => return Err(Error::Protocol("not ls-refs".into())),
        };
        self.check_object_format(args.object_format)?;
        let mut w = PktWriter::default();
        wire::write_ls_refs(&mut w, &args, m.head.as_deref().map(|v| bstr::ByteSlice::as_bstr(v.as_slice())), &m.refs);
        let mut ls = m.ls.borrow_mut();
        if ls.len() >= 8 {
            ls.remove(0); // small FIFO — arg mixes past 8 simply re-render
        }
        ls.push((body.to_vec(), w.out.clone()));
        drop(ls);
        Response::from_bytes(w.out).map_err(|e| Error::Internal(e.to_string()))
    }

    /// Section 3 step 0: the push row carrying the gc_epoch the whole push validates against.
    fn push_begin(&self, meta: &Meta, b: &BeginDto) -> Result<Response, Error> {
        self.rate_check(&b.key)?; // A27: before the row exists — throttled pushes hold nothing
        // object-format negotiation lands here: a v0 push's declared format pins
        // an unpinned repo, and a conflicting one is refused before ingest sees bytes
        if let Some(kind) = Self::parse_kind(b.object_format.as_deref())? {
            self.pin_obj_format(kind)?;
        }
        // I1 multi-part staging reuses ONE open push for every part key so the import
        // job's began_at heartbeat covers them all: re-beginning an existing open push
        // under the same principal is a no-op; any other state or owner is an error.
        #[derive(serde::Deserialize)]
        struct Existing {
            state: String,
            principal: String,
            auth: Option<String>,
        }
        let existing = self
            .q(
                "SELECT state, principal, auth FROM pushes WHERE id=?",
                vec![V::from(b.push_id.as_str())],
            )?
            .to_array::<Existing>()?
            .into_iter()
            .next();
        match existing {
            // PR-3b: same principal AND same credential — commit re-validates the
            // begin credential, so a second part can't ride under another token
            Some(e)
                if e.state == "open"
                    && e.principal == b.principal
                    && e.auth.as_deref() == Some(if b.admin { ADMIN_AUTH } else { b.key.as_str() }) => {}
            Some(e) if e.state == "open" => return Err(Error::Forbidden),
            Some(e) => return Err(Error::Conflict(format!("push is {}", e.state))),
            None => {
                // an 'open' push owns pending/ keys and eventually a packs row; bound
                // how many a client may hold at once (expired ones are reaped)
                let open = self
                    .q("SELECT COUNT(*) AS n FROM pushes WHERE state='open'", vec![])?
                    .one::<N>()?
                    .n;
                if open >= 64 {
                    return Err(Error::Limit("too many open pushes".into()));
                }
                // #19: capture the presenting token's ref scope on the push row —
                // commit_push/import enforce it per command, and a mid-push token
                // edit can't retroactively widen an open push
                // PR-3a: fail closed — a missing row, a read token or an empty scope
                // refuses here; only the edge-attested global admin gets ALL_REFS
                #[derive(serde::Deserialize)]
                struct Sc {
                    level: String,
                    scope: Option<String>,
                    expires_at: Option<i64>,
                }
                let now = platform::now_ms();
                let row = if b.admin || b.key.is_empty() {
                    None
                } else {
                    self.q("SELECT level, scope, expires_at FROM tokens WHERE hash=?", vec![V::from(b.key.as_str())])?
                        .to_array::<Sc>()?
                        .into_iter()
                        .next()
                        .filter(|r| token_live(r.expires_at, now)) // PR-3: expired = missing
                };
                let scope =
                    push_scope(b.admin, &b.key, row.as_ref().map(|r| (r.level.as_str(), r.scope.as_deref())))?;
                self.q(
                    "INSERT INTO pushes(id,state,principal,began_at,gc_epoch,scope,auth) VALUES(?,'open',?,?,?,?,?)",
                    vec![
                        V::from(b.push_id.as_str()),
                        V::from(b.principal.as_str()),
                        V::from(platform::now_ms()),
                        V::from(meta.gc_epoch),
                        V::from(scope.as_str()),
                        V::from(if b.admin { ADMIN_AUTH } else { b.key.as_str() }),
                    ],
                )?;
            }
        }
        // A26: a repo counts against GE_QUOTA_MAX_REPOS_PER_OWNER until its first
        // committed pack lands (live, or dead after sweep). `claimed=false` tells the
        // edge to take the registry hop; a repo that once committed keeps pushing even
        // if the cap was tightened after (grandfathered — it never re-claims).
        let claimed =
            self.q("SELECT COUNT(*) AS n FROM packs WHERE state<>'ingesting'", vec![])?.one::<N>()?.n > 0;
        json(serde_json::json!({
            "repo_id": meta.repo_id, "refs_version": meta.refs_version, "gc_epoch": meta.gc_epoch,
            "claimed": claimed
        }))
    }

    /// A27 sliding-window throttle: two fixed 60 s counter buckets in `rate`; the
    /// estimate is cur + prev*(1-frac_elapsed). The attempt is counted before the
    /// check so an abusive credential stays over the line instead of hovering at it.
    /// Keyed on the presented token's sha1 (`push:<hash>`); per-repo by construction —
    /// the counter lives in this DO. GE_RATE_PUSHES_PER_MIN <= 0 disables.
    fn rate_check(&self, key: &str) -> Result<(), Error> {
        let limit = self.env_i64("GE_RATE_PUSHES_PER_MIN", DEFAULT_PUSHES_PER_MIN);
        if limit <= 0 {
            return Ok(());
        }
        let now = platform::now_ms();
        let win = now.div_euclid(60_000);
        let frac = now.rem_euclid(60_000) as f64 / 60_000.0;
        let bucket = format!("push:{key}");
        self.q(
            "INSERT INTO rate(bucket,window,count) VALUES(?,?,1) \
             ON CONFLICT(bucket,window) DO UPDATE SET count=count+1",
            vec![V::from(bucket.as_str()), V::from(win)],
        )?;
        #[derive(serde::Deserialize)]
        struct R {
            window: i64,
            count: i64,
        }
        let (mut cur, mut prev) = (0i64, 0i64);
        for r in self
            .q(
                "SELECT window, count FROM rate WHERE bucket=? AND window>=?",
                vec![V::from(bucket.as_str()), V::from(win - 1)],
            )?
            .to_array::<R>()?
        {
            if r.window == win {
                cur = r.count;
            } else {
                prev = r.count;
            }
        }
        self.q(
            "DELETE FROM rate WHERE bucket=? AND window<?",
            vec![V::from(bucket.as_str()), V::from(win - 1)],
        )?;
        if cur as f64 + prev as f64 * (1.0 - frac) > limit as f64 {
            let retry = u32::try_from((60_000 - now.rem_euclid(60_000)).div_euclid(1_000)).unwrap_or(60);
            return Err(Error::RateLimit(retry.max(1)));
        }
        Ok(())
    }

    /// POST /_do/push/abort — a post-begin failure closes the row immediately instead
    /// of leaving it `open` for the janitor's 1h timeout (64 leaked rows = push DoS).
    fn push_abort(&self, b: &serde_json::Value) -> Result<Response, Error> {
        let id = b.get("push_id").and_then(|v| v.as_str()).ok_or_else(|| Error::Protocol("push_id".into()))?;
        self.q(
            "UPDATE pushes SET state='dead' WHERE id=? AND state='open'",
            vec![V::from(id)],
        )?;
        json(serde_json::json!({ "ok": true }))
    }

    /// POST /_do/push/lookup (sync). `pack` = the caller's own ingesting pack counts (2.5).
    fn push_lookup(&self, b: &LookupDto) -> Result<Response, Error> {
        if b.ids.len() > 1_000 {
            return Err(Error::Internal("lookup > 1000 ids".into()));
        }
        let ids = b.ids.iter().map(|h| oid(h)).collect::<Result<Vec<_>, Error>>()?;
        let sql = self.sql();
        let idx = Index(&sql);
        let mut locs = idx.lookup(&ids)?;
        if let Some(p) = &b.pack {
            let pack = PackId(p.clone());
            for (id, loc) in ids.iter().zip(locs.iter_mut()) {
                if loc.is_none() {
                    *loc = idx.lookup_in_pack(id, &pack)?;
                }
            }
        }
        json(serde_json::json!({ "locs": locs }))
    }

    /// Upsert the packs row ('ingesting') and insert <= 10,000 rows. Refused once the push
    /// is not 'open', so an expired push can never add rows (5.1/5.2, 3 step 1).
    fn push_index(&self, b: &IndexDto) -> Result<Response, Error> {
        if b.rows.len() > 10_000 {
            return Err(Error::Internal("index > 10000 rows".into()));
        }
        let m = &b.pack;
        #[derive(serde::Deserialize)]
        struct StateRow {
            state: String,
        }
        let st = self
            .q("SELECT state FROM pushes WHERE id=?", vec![V::from(m.push_id.as_str())])?
            .to_array::<StateRow>()?
            .into_iter()
            .next();
        if st.map(|s| s.state).as_deref() != Some("open") {
            return Err(Error::Conflict("push not open".into()));
        }
        self.q(
            "INSERT INTO packs(id,state,count,bytes,commit_lo,commit_hi,push_id,created_at) \
             VALUES(?,'ingesting',?,?,?,?,?,?) \
             ON CONFLICT(id) DO UPDATE SET count=excluded.count, bytes=excluded.bytes, \
             commit_lo=excluded.commit_lo, commit_hi=excluded.commit_hi \
             WHERE packs.state='ingesting' AND packs.push_id=excluded.push_id",
            vec![
                V::from(m.id.as_str()),
                V::from(m.count),
                V::from(m.bytes),
                V::from(m.commit_lo),
                V::from(m.commit_hi),
                V::from(m.push_id.as_str()),
                V::from(platform::now_ms()),
            ],
        )?;
        if self.changes()? != 1 {
            return Err(Error::Conflict("pack not ingesting for this push".into()));
        }
        Index(&self.sql()).insert_objects(&PackId(m.id.clone()), &b.rows)?;
        json(serde_json::json!({}))
    }

    /// I1: POST /_do/import/start — open the pushes row, plant the 'ingesting'
    /// packs row the job will write into, and queue the import_pack job. One sync
    /// span; a second start for the same push updates payload + run_at instead of
    /// queueing a duplicate row (the generic enqueue dedup is per-kind, which
    /// would silently swallow a distinct push's job).
    fn import_start(&self, meta: &Meta, b: &ImportStartDto) -> Result<Response, Error> {
        #[derive(serde::Deserialize)]
        struct S {
            state: String,
            principal: String,
            scope: Option<String>,
        }
        let st = self
            .q("SELECT state, principal, scope FROM pushes WHERE id=?", vec![V::from(b.push.as_str())])?
            .to_array::<S>()?
            .into_iter()
            .next();
        let Some(st) = st else {
            return Err(Error::Conflict("unknown push".into()));
        };
        if st.state != "open" {
            return Err(Error::Conflict(format!("import push is {}", st.state)));
        }
        // a url import stages nothing up front — the fetch phase derives
        // parts+commands (and re-checks scope against the advertised refs), so
        // the staged-payload validation below only applies without one
        let url = b.url.as_deref();
        if url.is_none() && (b.parts.is_empty() || b.commands.is_empty()) {
            return Err(Error::Protocol("import needs parts and commands".into()));
        }
        // staged imports declare the pack's format (default: the repo's own) and
        // pin it before the job exists; a url import pins the remote's format at
        // fetch time
        let format = if url.is_none() {
            let kind = Self::parse_kind(b.format.as_deref())?.unwrap_or(self.obj_kind()?);
            self.pin_obj_format(kind)?;
            Some(Self::kind_name(kind))
        } else {
            None
        };
        if url.is_none() {
            // #19: an import obeys the push's captured token scope — every command's
            // ref must be covered, or the whole import 400s before a job exists.
            // PR-3a: a NULL scope (legacy row) covers nothing.
            for c in &b.commands {
                let name = c.get("name").and_then(|n| n.as_str()).unwrap_or_default();
                if !scope_allows(st.scope.as_deref(), name) {
                    return Err(Error::Protocol(format!("ref {name} outside token scope")));
                }
            }
            // part keys must stay inside this repo's pending namespace — the R2 bucket
            // is shared across repos, so an unchecked key would read across tenancy
            let pending_ns = format!("r/{}/pending/", self.repo_id()?.0);
            for part in &b.parts {
                let key = part
                    .get("key")
                    .and_then(|k| k.as_str())
                    .ok_or_else(|| Error::Protocol("part key".into()))?;
                if !key.starts_with(&pending_ns) {
                    return Err(Error::Protocol("part outside pending namespace".into()));
                }
                if part.get("bytes").and_then(|n| n.as_u64()).is_none() {
                    return Err(Error::Protocol("part bytes".into()));
                }
            }
            // malformed commands should 400 here, not kill the job mid-import
            serde_json::from_value::<Vec<CmdDto>>(serde_json::Value::Array(b.commands.clone()))
                .map_err(|e| Error::Protocol(format!("commands: {e}")))?;
        }
        // same-push start is idempotent: a live job for it already owns a pack —
        // hand that pack id back rather than planting a second ingesting row
        #[derive(serde::Deserialize)]
        struct J {
            payload: String,
        }
        let existing = self
            .q(
                "SELECT payload FROM jobs WHERE kind='import_pack' AND state IN ('queued','running')",
                vec![],
            )?
            .to_array::<J>()?
            .into_iter()
            .find(|j| {
                serde_json::from_str::<serde_json::Value>(&j.payload)
                    .ok()
                    .and_then(|v| v.get("push").and_then(|p| p.as_str()).map(String::from))
                    .as_deref()
                    == Some(b.push.as_str())
            });
        if let Some(j) = existing {
            let pack = serde_json::from_str::<serde_json::Value>(&j.payload)
                .ok()
                .and_then(|v| v.get("pack").and_then(|p| p.as_str()).map(String::from))
                .unwrap_or_default();
            return json(serde_json::json!({ "queued": true, "push": b.push, "pack": pack }));
        }
        self.q(
            "INSERT INTO packs(id,state,count,bytes,commit_lo,commit_hi,push_id,created_at) \
             VALUES(?,'ingesting',0,0,0,0,?,?) ON CONFLICT(id) DO NOTHING",
            vec![
                V::from(b.pack.as_str()),
                V::from(b.push.as_str()),
                V::from(platform::now_ms()),
            ],
        )?;
        // the begin-time principal is authoritative — the client-supplied one is
        // ignored so a replayed start can't rewrite who the reflog credits
        let payload = if let Some(u) = url {
            serde_json::json!({
                "push": b.push, "pack": b.pack,
                "principal": st.principal, "url": u,
            })
            .to_string()
        } else {
            serde_json::json!({
                "push": b.push, "pack": b.pack, "parts": b.parts,
                "principal": st.principal, "commands": b.commands, "format": format,
            })
            .to_string()
        };
        // url mode starts in the fetch phase — the cursor seeds it, the job
        // row's payload gets rewritten once the remote's pack is staged
        self.q(
            "INSERT INTO jobs(kind,run_at,payload,cursor) VALUES('import_pack',?,?,?)",
            vec![
                V::from(platform::now_ms()),
                V::from(payload.as_str()),
                if url.is_some() { V::from("{\"phase\":\"fetch\"}") } else { V::Null },
            ],
        )?;
        let _ = meta;
        json(serde_json::json!({ "queued": true, "push": b.push }))
    }

    /// GET-style status for the edge's /_admin/import/<push>: push state, job
    /// state/phase, and how much of the staged pack is ingested so far.
    fn import_status(&self, b: &serde_json::Value) -> Result<Response, Error> {
        let push = b.get("push").and_then(|v| v.as_str()).ok_or_else(|| Error::Protocol("push".into()))?;
        #[derive(serde::Deserialize)]
        struct P {
            state: String,
            result: Option<String>,
        }
        let prow = self
            .q("SELECT state, result FROM pushes WHERE id=?", vec![V::from(push)])?
            .to_array::<P>()?
            .into_iter()
            .next();
        #[derive(serde::Deserialize)]
        struct J {
            state: String,
            attempts: i64,
            cursor: Option<String>,
            last_error: Option<String>,
            payload: String,
        }
        let job = self
            .q(
                "SELECT state, attempts, cursor, last_error, payload FROM jobs \
                 WHERE kind='import_pack' ORDER BY id DESC LIMIT 8",
                vec![],
            )?
            .to_array::<J>()?
            .into_iter()
            .find(|j| {
                serde_json::from_str::<serde_json::Value>(&j.payload)
                    .ok()
                    .and_then(|v| v.get("push").and_then(|p| p.as_str()).map(String::from))
                    .as_deref()
                    == Some(push)
            });
        #[derive(serde::Deserialize)]
        struct N2 {
            n: i64,
        }
        let pack = job.as_ref().and_then(|j| {
            serde_json::from_str::<serde_json::Value>(&j.payload)
                .ok()
                .and_then(|v| v.get("pack").and_then(|p| p.as_str()).map(String::from))
        });
        let done = pack.as_ref().map_or(Ok(0), |pk| {
            self.q("SELECT COUNT(*) AS n FROM objects WHERE pack_id=?", vec![V::from(pk.as_str())])?
                .one::<N2>()
                .map(|r| r.n)
        })?;
        let total = job.as_ref().and_then(|j| {
            j.cursor.as_deref().and_then(|c| {
                serde_json::from_str::<serde_json::Value>(c)
                    .ok()
                    .and_then(|v| v.get("count").and_then(|n| n.as_u64()))
            })
        });
        let phase = job.as_ref().and_then(|j| {
            j.cursor.as_deref().and_then(|c| {
                serde_json::from_str::<serde_json::Value>(c)
                    .ok()
                    .and_then(|v| v.get("phase").and_then(|p| p.as_str()).map(String::from))
            })
        });
        json(serde_json::json!({
            "push": push,
            "push_state": prow.as_ref().map(|p| p.state.as_str()),
            "result": prow.as_ref().and_then(|p| p.result.clone()),
            "job_state": job.as_ref().map(|j| j.state.as_str()),
            "job_phase": phase,
            "attempts": job.as_ref().map(|j| j.attempts),
            "last_error": job.as_ref().and_then(|j| j.last_error.clone()),
            "objects_done": done,
            "objects_total": total,
        }))
    }

    /// Section 3, steps 1-7. One sync span: no await, no R2, no stub between first SELECT and return.
    pub fn commit_push(&self, req: &CommitRequest) -> Result<CommitResponse, Error> {
        let cmds = req
            .commands
            .iter()
            .map(|c| {
                Ok(Cmd {
                    old: oid(&c.old)?,
                    new: oid(&c.new)?,
                    name: c.name.clone(),
                    peeled: c.peeled.clone(),
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        let now = platform::now_ms();
        let push = self
            .q("SELECT state, gc_epoch, scope, auth FROM pushes WHERE id=?", vec![V::from(req.push_id.as_str())])?
            .to_array::<PushRow>()?
            .into_iter()
            .next()
            .ok_or_else(|| Error::Conflict("unknown push".into()))?;
        if push.state != "open" {
            return Err(Error::Conflict(format!("push is {}", push.state))); // step 1
        }
        // PR-3b: re-validate the credential now, not just at begin — a token
        // revoked (or narrowed) while the pack streamed must not land refs.
        // Before step 3, so a rejected push leaves its pack 'ingesting' for the janitor.
        let current_scope = {
            #[derive(serde::Deserialize)]
            struct T {
                level: String,
                scope: Option<String>,
                expires_at: Option<i64>,
            }
            let row = match push.auth.as_deref() {
                Some(a) if a != ADMIN_AUTH && !a.is_empty() => self
                    .q("SELECT level, scope, expires_at FROM tokens WHERE hash=?", vec![V::from(a)])?
                    .to_array::<T>()?
                    .into_iter()
                    .next()
                    .filter(|r| token_live(r.expires_at, now)), // PR-3: expired mid-push = revoked
                _ => None,
            };
            match commit_auth(push.auth.as_deref(), row.as_ref().map(|r| (r.level.as_str(), r.scope.as_deref()))) {
                Ok(s) => s,
                Err(why) => {
                    let results = cmds.iter().map(|c| (c.name.clone(), Some(why))).collect();
                    return self.finish_push(req, "rejected", now, results);
                }
            }
        };
        let scopes = [push.scope.as_deref(), current_scope.as_deref().or(push.scope.as_deref())];
        if self.meta_i64("gc_epoch")? != push.gc_epoch {
            // step 2
            let results = cmds
                .iter()
                .map(|c| (c.name.clone(), Some("gc ran during push, retry")))
                .collect::<Vec<_>>();
            return self.finish_push(req, "rejected", now, results);
        }
        if let Some(pack) = &req.pack_id {
            // A26: per-repo object/byte quotas, evaluated now that ingest has posted
            // the pack's real counts. Live packs plus this push's own ingesting pack
            // are the total; other pushes' in-flight packs don't count against us.
            // A delete-only push (no pack) skips the check so an over-cap repo can
            // still shrink. The whole push rejects with the cap named in the message.
            let (objects, bytes) = self.storage_totals(&req.push_id)?;
            let max_obj = self.env_i64("GE_QUOTA_MAX_OBJECTS", DEFAULT_MAX_OBJECTS);
            let max_bytes = self.env_i64("GE_QUOTA_MAX_BYTES", DEFAULT_MAX_BYTES);
            let over = if max_obj > 0 && objects > max_obj {
                Some(format!("objects {objects} > GE_QUOTA_MAX_OBJECTS={max_obj}"))
            } else if max_bytes > 0 && bytes > max_bytes {
                Some(format!("bytes {bytes} > GE_QUOTA_MAX_BYTES={max_bytes}"))
            } else {
                None
            };
            if let Some(m) = over {
                // 'rejected' + pack_id lets the janitor reap the ingesting pack now
                // rather than on the orphan timeout
                self.finish_push(
                    req,
                    "rejected",
                    now,
                    cmds.iter().map(|c| (c.name.clone(), Some("over repo quota"))).collect(),
                )?;
                return Err(Error::Limit(format!("repo quota exceeded: {m}")));
            }
            // step 3
            self.q(
                // bound to this push: an ingesting pack that belongs to a different push
                // must not be promotable by someone else's commit
                "UPDATE packs SET state='live' WHERE id=? AND state='ingesting' AND push_id=?",
                vec![V::from(pack.as_str()), V::from(req.push_id.as_str())],
            )?;
            if self.changes()? != 1 {
                return Err(Error::Conflict("pack not in state ingesting".into()));
            }
        }
        let head = self.meta("head")?;
        let sql = self.sql();
        let idx = Index(&sql);
        if req.atomic {
            // --atomic (capability): validate every command before any write —
            // a dry-run inside this same span is deterministic. Any failure
            // rejects the whole push; failing refs keep their reason and the
            // rest get git's stock atomic message.
            let mut results = Vec::with_capacity(cmds.len());
            let mut any_fail = false;
            for c in &cmds {
                let r = self.apply_one(&idx, &head, c, &req.push_id, &req.principal, now, false, &scopes)?;
                any_fail |= r.is_some();
                results.push((c.name.clone(), r));
            }
            if any_fail {
                if let Some(pack) = &req.pack_id {
                    // step 3 promoted it; demote so 'rejected' lets the janitor reap
                    self.q(
                        "UPDATE packs SET state='ingesting' WHERE id=? AND push_id=?",
                        vec![V::from(pack.as_str()), V::from(req.push_id.as_str())],
                    )?;
                }
                let results = results
                    .into_iter()
                    .map(|(n, r)| (n, r.or(Some("atomic push failed"))))
                    .collect();
                return self.finish_push(req, "rejected", now, results);
            }
        }
        let mut results = Vec::with_capacity(cmds.len());
        let mut any_ok = false;
        for c in &cmds {
            // step 4
            let r = self.apply_one(&idx, &head, c, &req.push_id, &req.principal, now, true, &scopes)?;
            any_ok |= r.is_none();
            results.push((c.name.clone(), r));
        }
        // step 5: the version moves when a ref landed OR a pack went live
        // without one — a non-atomic push whose every command lost still
        // promoted its pack at step 3, so the live set a running GC
        // snapshotted changed either way and its refs_version guards must
        // abort it (C2). The atomic all-fail arm above already demoted its
        // pack, so `pack_id` here means "live now".
        if any_ok || req.pack_id.is_some() {
            self.q("UPDATE meta SET value=value+1 WHERE key='refs_version'", vec![])?;
        }
        if any_ok {
            // a repo created by pushing a non-main history (e.g. master-first) leaves
            // meta.head dangling — adopt an existing branch or clones can't check out
            self.q(
                "UPDATE meta SET value=(SELECT name FROM refs WHERE name LIKE 'refs/heads/%' \
                 ORDER BY name LIMIT 1) WHERE key='head' \
                 AND NOT EXISTS(SELECT 1 FROM refs WHERE name=meta.value) \
                 AND EXISTS(SELECT 1 FROM refs WHERE name LIKE 'refs/heads/%')",
                vec![],
            )?;
            jobs::enqueue(&sql, JobKind::GcMark, now + self.gc_quiet_ms(), "{}")?; // step 7 (dedups)
            // push events: the outbox row lands in this span with the ref writes,
            // so an event exists iff refs moved; delivery is the push_event job's
            self.record_push_event(req, &cmds, &results, now)?;
        }
        self.finish_push(req, "committed", now, results) // step 6
    }

    /// Push events (jobs/events.rs): one outbox row for the refs that actually
    /// moved, then enqueue delivery. No-op while the webhook isn't configured.
    fn record_push_event(
        &self,
        req: &CommitRequest,
        cmds: &[Cmd],
        results: &[(String, Option<&'static str>)],
        now: i64,
    ) -> Result<(), Error> {
        if jobs::events::config(&self.env).is_none() {
            return Ok(());
        }
        let pending = self.q("SELECT COUNT(*) AS n FROM push_events", vec![])?.one::<N>()?.n;
        if pending >= jobs::events::MAX_PENDING {
            self.q(
                "INSERT INTO meta(key,value) VALUES('push_events_dropped','1') \
                 ON CONFLICT(key) DO UPDATE SET value=CAST(value AS INTEGER)+1",
                vec![],
            )?;
            return Ok(());
        }
        let refs = cmds
            .iter()
            .zip(results)
            .filter(|(_, (_, r))| r.is_none())
            .map(|(c, _)| serde_json::json!({ "ref": c.name, "old": c.old.to_string(), "new": c.new.to_string() }))
            .collect::<Vec<_>>();
        let body = serde_json::json!({
            "event": "push",
            "owner": self.meta("owner")?, "repo": self.meta("repo")?, "repo_id": self.repo_id()?.0,
            "push_id": req.push_id, "principal": req.principal, "at": now,
            "refs": refs,
        });
        let body = serde_json::to_string(&body).map_err(|e| Error::Internal(e.to_string()))?;
        self.q(
            "INSERT INTO push_events(push_id,at,body) VALUES(?,?,?)",
            vec![V::from(req.push_id.as_str()), V::from(now), V::from(body.as_str())],
        )?;
        jobs::enqueue(&self.sql(), JobKind::PushEvent, now, "{}")?;
        Ok(())
    }

    /// One RefCommand, independent of its siblings unless `atomic` was requested.
    /// `write=false` is the --atomic dry run: every check runs but the ref table
    /// is only read — the CAS predicate becomes a SELECT so a multi-command push
    /// can validate fully before any sibling writes.
    fn apply_one(
        &self,
        idx: &Index<'_>,
        head: &str,
        c: &Cmd,
        push: &str,
        who: &str,
        now: i64,
        write: bool,
        scopes: &[Option<&str>],
    ) -> Result<Option<&'static str>, Error> {
        // refs live under refs/ only — a full valid refname, never HEAD or a bare word
        if !c.name.as_bytes().starts_with(b"refs/")
            || gix_validate::reference::name(c.name.as_bytes().as_bstr()).is_err()
        {
            return Ok(Some("funny refname"));
        }
        // #19 deploy-key scope: the push row carries the token's ref patterns —
        // a scoped token can only touch its refs (per-command `ng`, like a pin)
        // PR-3b: every scope must allow it — the one captured at begin AND the
        // token's current one (commit_push re-reads it in this span)
        if !scopes.iter().all(|s| scope_allows(*s, &c.name)) {
            return Ok(Some("ref outside token scope"));
        }
        if c.old.is_null() && c.new.is_null() {
            return Ok(Some("funny refname"));
        }
        // a pinned ref is immutable: update and delete both fail before CAS/target checks
        if self.pin_sha(&c.name)?.is_some() {
            return Ok(Some("ref is pinned"));
        }
        if c.new.is_null() && c.name == head {
            return Ok(Some("deletion of the current branch prohibited"));
        }
        // protected refs: FF-only + undeletable, for every credential and on
        // every path (receive-pack and import both commit through here)
        if self.is_protected(&c.name)? {
            let ff = if c.old.is_null() || c.new.is_null() {
                None
            } else {
                self.ff_ancestor(&c.old.to_string(), &c.new.to_string())?
            };
            if let Some(why) = protected_verdict(c.old.is_null(), c.new.is_null(), ff) {
                return Ok(Some(why));
            }
        }
        if !c.new.is_null() {
            // 2.5 re-guard: the tip must be live *now*, in this span. The push's pack is live since step 3.
            let loc: Option<ObjLoc> = idx.lookup(&[c.new])?.into_iter().next().flatten();
            if loc.is_none() {
                return Ok(Some("missing necessary objects"));
            }
            // a ref create costs the writer ~90 bytes of header but a reader the whole
            // table — bound it or ls-refs becomes an unbounded advertisement
            if c.old.is_null() && self.ref_count()? >= MAX_REFS {
                return Ok(Some("too many refs"));
            }
        }
        let (o, n, name) = (c.old.to_string(), c.new.to_string(), c.name.as_str());
        if !write {
            // the CAS predicates, read-only: a create lands iff the ref is
            // absent; update/delete land iff target equals the expected old.
            #[derive(serde::Deserialize)]
            struct T {
                target: String,
            }
            let cur = self
                .q("SELECT target FROM refs WHERE name=?", vec![V::from(name)])?
                .to_array::<T>()?
                .into_iter()
                .next();
            let ok = match (&cur, c.old.is_null()) {
                (None, true) => true,
                (Some(t), false) => t.target == o,
                _ => false,
            };
            return Ok(if ok { None } else { Some("failed to update ref") });
        }
        let peeled = c.peeled.clone().map_or(V::Null, |p| V::from(p));
        if c.old.is_null() {
            self.q(
                "INSERT INTO refs(name,target,peeled,updated_at) VALUES(?,?,?,?) ON CONFLICT DO NOTHING",
                vec![V::from(name), V::from(n.as_str()), peeled, V::from(now)],
            )?;
        } else if c.new.is_null() {
            self.q(
                "DELETE FROM refs WHERE name=? AND target=?",
                vec![V::from(name), V::from(o.as_str())],
            )?;
        } else {
            self.q(
                "UPDATE refs SET target=?, peeled=?, updated_at=? WHERE name=? AND target=?",
                vec![V::from(n.as_str()), peeled, V::from(now), V::from(name), V::from(o.as_str())],
            )?;
        }
        if self.changes()? != 1 {
            return Ok(Some("failed to update ref")); // git's own ng string
        }
        self.q(
            "INSERT INTO reflog(name,old,new,push_id,principal,at) VALUES(?,?,?,?,?,?)",
            vec![
                V::from(name),
                V::from(o.as_str()),
                V::from(n.as_str()),
                V::from(push),
                V::from(who),
                V::from(now),
            ],
        )?;
        Ok(None)
    }

    fn finish_push(
        &self,
        req: &CommitRequest,
        state: &str,
        now: i64,
        results: Vec<(String, Option<&'static str>)>,
    ) -> Result<CommitResponse, Error> {
        let result = serde_json::to_string(&results).map_err(|e| Error::Internal(e.to_string()))?;
        self.q(
            "UPDATE pushes SET state=?, ended_at=?, pack_id=?, result=? WHERE id=?",
            vec![
                V::from(state),
                V::from(now),
                req.pack_id.as_deref().map_or(V::Null, V::from),
                V::from(result.as_str()),
                V::from(req.push_id.as_str()),
            ],
        )?;
        Ok(CommitResponse { results })
    }

    /// A26 quota accounting: sum of packs.count/bytes over live packs plus this
    /// push's own ingesting pack (its real counts are posted before commit). Other
    /// pushes' in-flight packs are excluded — they may never commit.
    fn storage_totals(&self, push_id: &str) -> Result<(i64, i64), Error> {
        #[derive(serde::Deserialize)]
        struct T {
            objects: i64,
            bytes: i64,
        }
        let t = self
            .q(
                "SELECT COALESCE(SUM(count),0) AS objects, COALESCE(SUM(bytes),0) AS bytes \
                 FROM packs WHERE state='live' OR (state='ingesting' AND push_id=?)",
                vec![V::from(push_id)],
            )?
            .one::<T>()?;
        Ok((t.objects, t.bytes))
    }

    /// POST /_do/fetch (section 9): the only DO route that awaits (R2 reads).
    async fn fetch_v2(&self, body: &[u8], hdr: &RepoHeaders) -> worker::Result<Response> {
        // the budget lives in the inner fn/stream; the tally outlives both so the
        // error response can still report what the request spent (audit P3)
        let spend: Spend = Rc::new(Cell::new(0));
        match self.fetch_v2_inner(body, &spend, hdr).await {
            Ok(r) => Ok(r),
            Err(e @ (Error::Storage(_) | Error::Internal(_))) => Err(worker::Error::RustError(e.message())),
            Err(e) => {
                worker::console_log!("fetch_v2: {e}");
                do_error_response(&e, spend.get()).map_err(worker::Error::from)
            }
        }
    }

    async fn fetch_v2_inner(&self, body: &[u8], spend: &Spend, hdr: &RepoHeaders) -> Result<Response, Error> {
        let args = match wire::parse_v2_command(body)? {
            V2Command::Fetch(a) => a,
            _ => return Err(Error::Protocol("not fetch".into())),
        };
        let repo_kind = self.check_object_format(args.object_format)?;
        let mut budget = ReqBudget::paid().reporting(spend);
        let bucket = self.bucket()?;
        let sql = self.sql();
        let idx = Index(&sql);
        // step 0: want-ref names resolve against the refs table (HEAD follows meta.head);
        // the resolved pairs ride back in the wanted-refs section
        let mut wants = args.wants.clone();
        let mut wanted_refs: Vec<(ObjectId, BString)> = Vec::new();
        if !args.want_refs.is_empty() {
            #[derive(serde::Deserialize)]
            struct T {
                target: String,
            }
            let head = self.meta("head")?;
            for name in &args.want_refs {
                let qname = if name.as_slice() == b"HEAD" {
                    head.clone()
                } else {
                    String::from_utf8(name.to_vec()).map_err(|_| Error::Protocol("bad want-ref".into()))?
                };
                let target = self
                    .q("SELECT target FROM refs WHERE name=?", vec![V::from(qname.as_str())])?
                    .to_array::<T>()?
                    .into_iter()
                    .next()
                    .ok_or_else(|| Error::Protocol(format!("couldn't find remote ref {qname}")))?;
                let id = oid(&target.target)?;
                wanted_refs.push((id, name.clone()));
                wants.push(id);
            }
        }
        // deepen-not carries ref names: resolve each to its tip commit — peel annotated
        // tags (`peeled` holds the target commit; NULL on pre-column rows → peel one
        // level via the index). Clients send unqualified names ("mid"), so resolve by
        // git's standard search order; an unresolvable name is an error, like want-ref.
        let mut deepen_not: Vec<ObjectId> = Vec::new();
        if !args.deepen_not.is_empty() {
            #[derive(serde::Deserialize)]
            struct T {
                target: String,
                peeled: Option<String>,
            }
            for name in &args.deepen_not {
                let qname = String::from_utf8(name.to_vec())
                    .map_err(|_| Error::Protocol(format!("couldn't find remote ref {}", name.as_bstr())))?;
                let mut found = None;
                for cand in [
                    qname.clone(),
                    format!("refs/{qname}"),
                    format!("refs/tags/{qname}"),
                    format!("refs/heads/{qname}"),
                    format!("refs/remotes/{qname}"),
                    format!("refs/remotes/{qname}/HEAD"),
                ] {
                    if let Some(t) = self
                        .q("SELECT target, peeled FROM refs WHERE name=? LIMIT 1", vec![V::from(cand.as_str())])?
                        .to_array::<T>()?
                        .into_iter()
                        .next()
                    {
                        found = Some(t);
                        break;
                    }
                }
                let t = found.ok_or_else(|| {
                    Error::Protocol(format!("couldn't find remote ref {qname}"))
                })?;
                let mut id = match t.peeled {
                    Some(p) => oid(&p)?,
                    None => oid(&t.target)?,
                };
                // pre-column rows can leave peeled NULL on annotated tags — if the
                // target is itself a tag object, read it and peel one level so the
                // exclusion walk starts at the commit
                if let Some(l) = idx.lookup(&[id])?.into_iter().next().flatten() {
                    if l.kind == gix_object::Kind::Tag {
                        if let Some((_, entry)) =
                            bucket.read_entries(&[(id, l)], &mut budget).await?.into_iter().next()
                        {
                            let (_, data) = crate::store::codec::decode_entry(&entry)?;
                            if let Some(hex) =
                                data.as_slice().lines().find_map(|l| l.strip_prefix(b"object "))
                            {
                                id = ObjectId::from_hex(hex)
                                    .map_err(|_| Error::Internal("bad tag object".into()))?;
                            }
                        }
                    }
                }
                deepen_not.push(id);
            }
        }
        // step 1: acks = known haves (unknown dropped). Wants validated inside send_set.
        let acks: Vec<ObjectId> = args
            .haves
            .iter()
            .zip(idx.lookup(&args.haves)?)
            .filter_map(|(h, l)| l.map(|_| *h))
            .collect();
        // step 2: readiness (stateless-RPC) — partial acks without `done` keep the
        // negotiation open; answering early would ship a pack built on an incomplete
        // have-set
        let ready = args.done || args.haves.is_empty() || acks.len() == args.haves.len();
        let mut w = PktWriter::default();
        if !ready {
            wire::write_fetch_prelude(&mut w, &args, &acks, &[], &[], &[], &[])?;
            return Response::from_bytes(w.out).map_err(|e| Error::Internal(e.to_string()));
        }
        // C2 (ROADMAP #23): a plain-clone-shaped fetch whose wants all live in the
        // repo's single live pack is answered by streaming that pack verbatim —
        // one R2 GET instead of a read_entries pass over every send-set span.
        // packs_live=1 makes "every markable object is in this pack" a construction
        // guarantee (the index only resolves live packs), so the stream is a
        // wire-legal superset: git index-packs extras, connectivity still passes.
        // Any shallow/deepen/filter arg carries a contract a superset can violate,
        // so the gate is strict on request shape.
        let plain_clone = args.haves.is_empty()
            && args.shallow.is_empty()
            && args.deepen.is_none()
            && args.deepen_since.is_none()
            && args.deepen_not.is_empty()
            && !args.deepen_relative
            && args.filter.is_none();
        if plain_clone {
            if let Some((pack, pack_bytes)) = self.consolidated_pack(&idx, &wants)? {
                // C1 (ROADMAP #24): an opted-in client gets a signed URI for the
                // same pack instead of the bytes — bandwidth leaves the Worker
                // entirely. Any gap (no key configured, no public base, scheme
                // not in the client's list) falls through to the C2 stream.
                if let Some(resp) = self
                    .pack_uri_response(&args, &pack, pack_bytes, hdr, &bucket, &mut budget, &mut w, &acks, &wanted_refs)
                    .await?
                {
                    return Ok(resp);
                }
                let key = keys::pack(&bucket.repo, &pack);
                budget.charge(1)?; // one GET streams the whole pack (7.1)
                let obj = bucket
                    .inner
                    .get(&key)
                    .execute()
                    .await?
                    .ok_or_else(|| Error::Storage(format!("missing {key}")))?;
                let body = obj
                    .body()
                    .ok_or_else(|| Error::Storage(format!("no body for {key}")))?;
                let pack_stream = body.stream().map_err(Error::from)?;
                wire::write_fetch_prelude(&mut w, &args, &acks, &wanted_refs, &[], &[], &[])?;
                // the charge above is the whole projected spend — the body stream
                // charges nothing further per chunk
                let projected = budget.used;
                let max_sub = budget.max_subrequests;
                let st = VerbatimStream {
                    budget,
                    body: pack_stream,
                    prelude: Some(w.out),
                    finished: false,
                };
                let s = stream::unfold(st, |mut st| async move {
                    match st.step().await {
                        Ok(Some(chunk)) => Some((Ok::<Vec<u8>, Error>(chunk), st)),
                        Ok(None) => None,
                        Err(e) => {
                            // mid-stream: one band-3 ERR frame, then end (section 10)
                            st.finished = true;
                            let mut w = PktWriter::default();
                            wire::Sideband::new(&mut w).error(&format!("ERR {}", e.client_message()));
                            Some((Ok(w.out), st))
                        }
                    }
                });
                let resp = Response::from_stream(s).map_err(Error::from)?;
                resp.headers()
                    .set("x-ge-subrequests", &format!("{projected}/{max_sub}"))
                    .map_err(|e| Error::Internal(e.to_string()))?;
                return Ok(resp);
            }
        }
        // steps 3-5: the send set — or, for a plain clone over multiple live
        // packs, the all-live-objects set (#22): the same verbatim-copy wire
        // shape with no commit walk, so MAX_COMMITS never binds a big import
        // that hasn't consolidated yet. Emitted objects are a superset of the
        // wanted closure — wire-legal exactly as A29 argues.
        let set = if plain_clone {
            match self.no_walk_set(&idx, &budget)? {
                Some(s) => s,
                None => {
                    generate::send_set(
                        self,
                        &bucket,
                        &wants,
                        &args.haves,
                        args.filter.as_ref(),
                        args.deepen,
                        args.deepen_since,
                        &deepen_not,
                        args.deepen_relative,
                        args.include_tag,
                        &args.shallow,
                        &mut budget,
                    )
                    .await?
                }
            }
        } else {
            generate::send_set(
                self,
                &bucket,
                &wants,
                &args.haves,
                args.filter.as_ref(),
                args.deepen,
                args.deepen_since,
                &deepen_not,
                args.deepen_relative,
                args.include_tag,
                &args.shallow,
                &mut budget,
            )
            .await?
        };
        // step 6: stream header + chunks + trailer + flush as band-1 sideband frames
        wire::write_fetch_prelude(&mut w, &args, &acks, &wanted_refs, &set.shallow, &set.unshallow, &[])?;
        let prelude = w.out;
        // contract 406: the harness asserts on the ReqBudget counters — report projected
        // spend (used so far + every planned pack read) before the stream starts
        let projected = budget
            .used
            .saturating_add(u32::try_from(set.reads.len()).unwrap_or(u32::MAX));
        let max_sub = budget.max_subrequests;
        let st = FetchStream {
            budget,
            bucket,
            set,
            next: 0,
            hasher: gix_hash::hasher(repo_kind),
            kind: repo_kind,
            prelude: Some(prelude),
            _args: args,
        };
        let s = stream::unfold(st, |mut st| async move {
            match st.step().await {
                Ok(Some(chunk)) => Some((Ok::<Vec<u8>, Error>(chunk), st)),
                Ok(None) => None,
                Err(e) => {
                    // mid-stream: one band-3 ERR frame, then the stream ends (section 10).
                    // `next` must advance past the end or the same step would retry forever
                    st.next = usize::MAX;
                    let mut w = PktWriter::default();
                    wire::Sideband::new(&mut w).error(&format!("ERR {}", e.client_message()));
                    Some((Ok(w.out), st))
                }
            }
        });
        let resp = Response::from_stream(s).map_err(Error::from)?;
        resp.headers()
            .set("x-ge-subrequests", &format!("{projected}/{max_sub}"))
            .map_err(|e| Error::Internal(e.to_string()))?;
        Ok(resp)
    }

    /// The C2 gate's server half: exactly one live pack, and every resolved want
    /// has its objects row in it. A want the walk would reject ("not our ref") or
    /// one living in another pack returns None so the walking path answers.
    /// Returns the pack id and its stored byte length (C1 needs it for the
    /// trailer read that mints the URI's hash token).
    fn consolidated_pack(&self, idx: &Index<'_>, wants: &[ObjectId]) -> Result<Option<(PackId, u64)>, Error> {
        #[derive(serde::Deserialize)]
        struct P {
            id: String,
            bytes: i64,
        }
        let live = self
            .q("SELECT id, bytes FROM packs WHERE state='live'", vec![])?
            .to_array::<P>()?;
        let [p] = live.as_slice() else {
            return Ok(None);
        };
        let pack = PackId(p.id.clone());
        for loc in idx.lookup(wants)? {
            match loc {
                Some(l) if l.pack == pack => {}
                _ => return Ok(None),
            }
        }
        // a corrupt row must not 500 every clone shape — degrade to the walk
        let Ok(bytes) = u64::try_from(p.bytes) else {
            return Ok(None);
        };
        Ok(Some((pack, bytes)))
    }

    /// #22 no-walk clone: every live pack's entries are emitted verbatim —
    /// `plan_reads` coalesces contiguous entries into 8 MiB reads exactly like
    /// the walked path, and `pack_chunk` copies each span byte-for-byte.
    /// The same sha can sit in two live packs during a GC transition (the
    /// consolidated pack goes live at gc_commit while gc_sweep — a later job —
    /// still has the sources live), and git's index-pack REJECTS a pack whose
    /// entries name the same object twice. Rows are scanned ordered by sha so
    /// an object's copies across packs are adjacent; marking the first row of
    /// each run emits it exactly once (O(1) memory, no seen-set). The IN list
    /// pins the dedup domain to the snapshot's packs: a pack going live
    /// mid-scan cannot win a sha's single slot and leave it unmarked.
    /// None when <2 live packs: single-pack clones take the A29/C1 path.
    fn no_walk_set(&self, idx: &Index<'_>, budget: &ReqBudget) -> Result<Option<generate::SendSet>, Error> {
        #[derive(serde::Deserialize)]
        struct P {
            id: String,
            count: i64,
            bytes: i64,
        }
        let live = self
            .q("SELECT id, count, bytes FROM packs WHERE state='live'", vec![])?
            .to_array::<P>()?;
        if live.len() < 2 {
            return Ok(None);
        }
        let mut set = generate::SendSet::default();
        let mut by_pack: HashMap<String, usize> = HashMap::new();
        let mut args: Vec<V> = Vec::with_capacity(live.len() + 1);
        let marks = live.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        for p in &live {
            let count = u32::try_from(p.count).map_err(|_| Error::Internal("count".into()))?;
            let bytes = u64::try_from(p.bytes).map_err(|_| Error::Internal("bytes".into()))?;
            let bits = usize::try_from(count).map_err(|_| Error::Internal("count".into()))?.div_ceil(8);
            by_pack.insert(p.id.clone(), set.packs.len());
            args.push(V::from(p.id.as_str()));
            set.packs.push(generate::PackSlice { pack: PackId(p.id.clone()), count, bytes, bitmap: vec![0; bits] });
        }
        let mut last = String::new();
        loop {
            let mut page = args.clone();
            page.push(V::from(last.as_str()));
            let rows = self
                .q(
                    &format!(
                        "SELECT sha, pack_id, idx FROM objects \
                         WHERE pack_id IN ({marks}) AND sha > ? ORDER BY sha LIMIT 5000"
                    ),
                    page,
                )?
                .to_array::<DedupRow>()?;
            let n = rows.len();
            mark_dedup(&rows, &mut set.packs, &by_pack, &mut last)?;
            if n < 5000 {
                break;
            }
        }
        generate::plan_reads(idx, &mut set, budget)?;
        Ok(Some(set))
    }

    /// C1 (A30): when the client sent `packfile-uris` naming our public scheme,
    /// a signing key is configured, and the edge forwarded the request origin,
    /// answer with a `packfile-uris` section — `<trailer-hash> <signed URL>` —
    /// and a valid empty pack as the inline `packfile` section (the client still
    /// index-packs it; an empty pack is legal). The hash token is the pack's
    /// real trailer SHA-1: `git http-fetch --packfile` compares it against the
    /// downloaded pack's checksum and dies on mismatch.
    async fn pack_uri_response(
        &self,
        args: &FetchArgs,
        pack: &PackId,
        pack_bytes: u64,
        hdr: &RepoHeaders,
        bucket: &Bucket,
        budget: &mut ReqBudget,
        w: &mut PktWriter,
        acks: &[ObjectId],
        wanted_refs: &[(ObjectId, BString)],
    ) -> Result<Option<Response>, Error> {
        let kind = self.obj_kind()?;
        let ds = u64::try_from(kind.len_in_bytes()).unwrap_or(20);
        // minimum pack = 12-byte header + trailer
        let (Some(base), Some(owner), Some(repo), true) =
            (hdr.base.as_deref(), hdr.owner.as_deref(), hdr.repo.as_deref(), pack_bytes >= 12 + ds)
        else {
            return Ok(None);
        };
        let proto: &[u8] = if base.starts_with("https://") { b"https" } else { b"http" };
        let uris = args.packfile_uris.as_deref().unwrap_or(&[]);
        if !uris.iter().any(|p| p.as_slice() == proto) {
            return Ok(None);
        }
        let Some(signing) = crate::sign::signing_key(&self.env) else {
            return Ok(None);
        };
        // must not exceed the janitor's dead-pack grace (jobs/janitor.rs
        // GRACE_MS), else a signed URL could outlive its R2 object
        const TTL: i64 = 3600;
        let exp = platform::now_ms() / 1000 + TTL;
        // the signature binds the opaque repo_id (carried as r=) — stronger than
        // the route name: a deleted+recreated repo gets a fresh id, so old sigs die
        let repo_id = bucket.repo.0.clone();
        // a transient trailer-read failure degrades to the A29 verbatim stream
        // rather than failing a fetch that could still be served. The trailer is
        // one object-id digest wide — 20 bytes for sha1, 32 for sha256.
        let Ok(trailer) = bucket
            .read_range(&keys::pack(&bucket.repo, pack), pack_bytes - ds, ds, budget)
            .await
        else {
            return Ok(None);
        };
        let hash = trailer.iter().map(|b| format!("{b:02x}")).collect::<String>();
        let sig = crate::sign::pack_sig(&signing, &repo_id, &pack.0, exp)?;
        let uri =
            format!("{base}/{owner}/{repo}/_packs/{}.pack?e={exp}&r={repo_id}&s={sig}", pack.0);
        // plain-clone shape ⇒ ready ⇒ the prelude always emits the packfile
        // header; Ok(false) is unreachable and would produce a malformed body
        if !wire::write_fetch_prelude(w, args, acks, wanted_refs, &[], &[], &[format!("{hash} {uri}")])? {
            return Err(Error::Internal("packfile-uris prelude".into()));
        }
        wire::Sideband::new(w).data(&empty_pack(kind)?);
        w.flush();
        let resp = Response::from_bytes(std::mem::take(&mut w.out)).map_err(Error::from)?;
        resp.headers()
            .set("x-ge-subrequests", &format!("{}/{}", budget.used, budget.max_subrequests))
            .map_err(|e| Error::Internal(e.to_string()))?;
        Ok(Some(resp))
    }

    /// POST /_do/export — a git bundle (v3, no prerequisites) of every live ref.
    /// Same awaits-as-fetch route arm: send_set + verbatim pack chunks, no pkt framing.
    async fn export_bundle(&self) -> worker::Result<Response> {
        let spend: Spend = Rc::new(Cell::new(0));
        match self.export_inner(&spend).await {
            Ok(r) => Ok(r),
            Err(e @ (Error::Storage(_) | Error::Internal(_))) => Err(worker::Error::RustError(e.message())),
            Err(e) => {
                worker::console_log!("export: {e}");
                do_error_response(&e, spend.get()).map_err(worker::Error::from)
            }
        }
    }

    async fn export_inner(&self, spend: &Spend) -> Result<Response, Error> {
        let (head, refs) = self.list_refs()?;
        let mut wants: Vec<ObjectId> = refs.iter().map(|r| r.target).collect();
        wants.sort_unstable();
        wants.dedup();
        let mut budget = ReqBudget::paid().reporting(spend);
        let bucket = self.bucket()?;
        // a full bundle = every object reachable from every ref tip: the fetch
        // machinery with no haves, no shallow/filter modes, include-tag off (tag
        // objects reachable via refs are already wants)
        let set = generate::send_set(
            self, &bucket, &wants, &[], None, None, None, &[], false, false, &[], &mut budget,
        )
        .await?;
        // v3 header: signature, capabilities (`@`-lines — sha256 repos must
        // declare @object-format or a sha256 client rejects the header), no
        // prerequisite lines, one `<sha> <ref>` line per ref (sorted), a HEAD
        // line through meta.head, blank
        let kind = self.obj_kind()?;
        let mut hdr = b"# v3 git bundle\n".to_vec();
        if kind == gix_hash::Kind::Sha256 {
            hdr.extend_from_slice(b"@object-format=sha256\n");
        }
        for r in &refs {
            hdr.extend_from_slice(r.target.to_string().as_bytes());
            hdr.push(b' ');
            hdr.extend_from_slice(r.name.as_slice());
            hdr.push(b'\n');
        }
        if let Some(h) = &head {
            if let Some(r) = refs.iter().find(|r| r.name.as_slice() == h.as_slice()) {
                hdr.extend_from_slice(r.target.to_string().as_bytes());
                hdr.extend_from_slice(b" HEAD\n");
            }
        }
        hdr.push(b'\n');
        let projected = budget
            .used
            .saturating_add(u32::try_from(set.reads.len()).unwrap_or(u32::MAX));
        let max_sub = budget.max_subrequests;
        let st = ExportStream {
            budget,
            bucket,
            set,
            next: 0,
            hasher: gix_hash::hasher(kind),
            kind,
            prelude: Some(hdr),
        };
        let s = stream::unfold(st, |mut st| async move {
            match st.step().await {
                Ok(Some(chunk)) => Some((Ok::<Vec<u8>, Error>(chunk), st)),
                Ok(None) => None,
                Err(e) => {
                    // mid-stream: a truncated bundle is the only signal left — the
                    // header and pack prefix are already on the wire
                    worker::console_log!("export stream: {e}");
                    None
                }
            }
        });
        let resp = Response::from_stream(s).map_err(Error::from)?;
        resp.headers()
            .set("x-ge-subrequests", &format!("{projected}/{max_sub}"))
            .map_err(|e| Error::Internal(e.to_string()))?;
        Ok(resp)
    }

    /// POST /_do/lfs/batch — Git LFS batch API (#21, basic transfer). The edge
    /// authenticated already (read for download, write for upload); this does
    /// the per-object existence check against R2 and mints signed `/_lfs/`
    /// hrefs with the A30 key (domain-separated `lfs` op). Upload side checks
    /// the A26 byte quota against packs + lfs + newly-declared bytes.
    async fn lfs_batch(&self, body: &[u8], hdr: &RepoHeaders) -> worker::Result<Response> {
        let spend: Spend = Rc::new(Cell::new(0));
        match self.lfs_batch_inner(body, hdr, &spend).await {
            Ok(r) => Ok(r),
            Err(e @ (Error::Storage(_) | Error::Internal(_))) => {
                Err(worker::Error::RustError(e.message()))
            }
            Err(e) => {
                worker::console_log!("lfs batch: {e}");
                do_error_response(&e, spend.get()).map_err(worker::Error::from)
            }
        }
    }

    async fn lfs_batch_inner(
        &self,
        body: &[u8],
        hdr: &RepoHeaders,
        spend: &Spend,
    ) -> Result<Response, Error> {
        #[derive(serde::Deserialize)]
        struct LfsObj {
            oid: String,
            #[serde(default)]
            size: u64,
        }
        #[derive(serde::Deserialize)]
        struct LfsDto {
            operation: String,
            #[serde(default)]
            objects: Vec<LfsObj>,
        }
        let d: LfsDto = parse(body)?;
        let upload = match d.operation.as_str() {
            "upload" => true,
            "download" => false,
            _ => return Err(Error::Protocol("lfs operation must be upload or download".into())),
        };
        let (Some(base), Some(owner), Some(repo)) =
            (hdr.base.as_deref(), hdr.owner.as_deref(), hdr.repo.as_deref())
        else {
            return Err(Error::Internal("lfs batch missing base".into()));
        };
        // the hrefs are capability URLs — no key configured means LFS is off
        let signing = crate::sign::signing_key(&self.env).ok_or(Error::Forbidden)?;
        let bucket = self.bucket()?;
        let mut budget = ReqBudget::paid().reporting(spend);
        // bound like _packs/: below the janitor's dead-pack grace (A30)
        const TTL: i64 = 3600;
        let exp = platform::now_ms() / 1000 + TTL;
        let repo_id = bucket.repo.0.clone();
        let mut out = Vec::with_capacity(d.objects.len());
        let mut new_bytes = 0i64;
        for o in &d.objects {
            if o.oid.len() != 64 || !o.oid.bytes().all(|b| b.is_ascii_hexdigit()) {
                out.push(serde_json::json!({"oid": o.oid, "size": o.size,
                    "error": {"code": 422, "message": "oid must be 64 hex chars"}}));
                continue;
            }
            let key = keys::lfs(&bucket.repo, &o.oid);
            budget.charge(1)?;
            let present = bucket.inner.head(&key).await?.is_some();
            if upload {
                if present {
                    // already stored — per spec the object needs no action
                    out.push(serde_json::json!({"oid": o.oid, "size": o.size}));
                } else {
                    new_bytes = new_bytes.saturating_add(o.size as i64);
                    let sig = crate::sign::lfs_sig(&signing, &repo_id, &o.oid, exp, "put")?;
                    out.push(serde_json::json!({"oid": o.oid, "size": o.size, "actions":
                        {"upload": {"href": format!("{base}/{owner}/{repo}/_lfs/{}?e={exp}&r={repo_id}&s={sig}", o.oid)}}}));
                }
            } else if present {
                let sig = crate::sign::lfs_sig(&signing, &repo_id, &o.oid, exp, "get")?;
                out.push(serde_json::json!({"oid": o.oid, "size": o.size, "actions":
                    {"download": {"href": format!("{base}/{owner}/{repo}/_lfs/{}?e={exp}&r={repo_id}&s={sig}", o.oid)}}}));
            } else {
                out.push(serde_json::json!({"oid": o.oid, "size": o.size,
                    "error": {"code": 404, "message": "object not found"}}));
            }
        }
        // A26 quota, lfs side: packs + stored lfs + newly-declared must fit —
        // the same bound commit_push enforces on the pack side
        if upload && new_bytes > 0 {
            let (_, pack_bytes) = self.storage_totals("")?;
            let lfs_bytes = self.lfs_bytes(&bucket, &mut budget).await?;
            let max = self.env_i64("GE_QUOTA_MAX_BYTES", DEFAULT_MAX_BYTES);
            if pack_bytes.saturating_add(lfs_bytes).saturating_add(new_bytes) > max {
                return Err(Error::Limit(format!(
                    "repo quota exceeded: packs {pack_bytes} + lfs {lfs_bytes} + upload {new_bytes} > GE_QUOTA_MAX_BYTES={max}"
                )));
            }
        }
        json(serde_json::json!({"transfer": "basic", "hash_algo": "sha256", "objects": out}))
    }

    /// Sum of stored lfs bytes — one `list` page per 1k objects under the
    /// repo's lfs/ prefix. Charged like any other R2 op.
    async fn lfs_bytes(&self, bucket: &Bucket, budget: &mut ReqBudget) -> Result<i64, Error> {
        let mut total = 0i64;
        let mut cursor: Option<String> = None;
        loop {
            budget.charge(1)?;
            let mut q = bucket
                .inner
                .list()
                .prefix(format!("r/{}/lfs/", bucket.repo.0))
                .limit(1_000);
            if let Some(c) = cursor.take() {
                q = q.cursor(c);
            }
            let page = q.execute().await?;
            for o in page.objects() {
                total = total.saturating_add(o.size() as i64);
            }
            match (page.truncated(), page.cursor()) {
                (true, Some(c)) => cursor = Some(c),
                _ => break,
            }
        }
        Ok(total)
    }
}

/// One row of `no_walk_set`'s sha-ordered dedup scan.
#[derive(serde::Deserialize)]
struct DedupRow {
    sha: String,
    pack_id: String,
    idx: u32,
}

/// Marks the first row of each same-sha run into its pack slice's bitmap —
/// the scan's `ORDER BY sha` makes a sha's copies across packs adjacent, so
/// each object gets exactly one physical entry in the emitted pack. `last`
/// is the sha cursor: it equals the caller's `sha > ?` page bound, so a group
/// split across pages still dedups (later copies sort ahead of the bound).
fn mark_dedup(
    rows: &[DedupRow],
    packs: &mut [generate::PackSlice],
    by_pack: &HashMap<String, usize>,
    last: &mut String,
) -> Result<(), Error> {
    for r in rows {
        if r.sha == *last {
            continue;
        }
        let i = *by_pack
            .get(&r.pack_id)
            .ok_or_else(|| Error::Internal("object row in unlisted pack".into()))?;
        let byte = usize::try_from(r.idx / 8).map_err(|_| Error::Internal("idx".into()))?;
        *packs
            .get_mut(i)
            .and_then(|p| p.bitmap.get_mut(byte))
            .ok_or_else(|| Error::Storage("idx past pack count".into()))? |= 1u8 << (r.idx % 8);
        *last = r.sha.clone();
    }
    Ok(())
}

struct FetchStream {
    budget: ReqBudget,
    bucket: Bucket,
    set: generate::SendSet,
    next: usize,
    hasher: gix_hash::Hasher,
    kind: gix_hash::Kind,
    prelude: Option<Vec<u8>>,
    _args: FetchArgs,
}
impl FetchStream {
    async fn step(&mut self) -> Result<Option<Vec<u8>>, Error> {
        if let Some(p) = self.prelude.take() {
            return Ok(Some(p));
        }
        let n = self.set.reads.len();
        match self.next {
            0 => {
                // PACK header first (count is exact before the first read: popcount)
                let mut hdr = b"PACK".to_vec();
                hdr.extend_from_slice(&2u32.to_be_bytes());
                hdr.extend_from_slice(&self.set.count().to_be_bytes());
                self.hasher.update(&hdr);
                self.next = 1;
                let mut w = PktWriter::default();
                wire::Sideband::new(&mut w).data(&hdr);
                Ok(Some(w.out))
            }
            i if i <= n => {
                let chunk = generate::pack_chunk(
                    &self.bucket,
                    &self.set,
                    i - 1,
                    &mut self.hasher,
                    &mut self.budget,
                )
                .await?;
                self.next += 1;
                let mut w = PktWriter::default();
                wire::Sideband::new(&mut w).data(&chunk);
                Ok(Some(w.out))
            }
            i if i == n + 1 => {
                // trailer + final flush, once
                let h = std::mem::replace(&mut self.hasher, gix_hash::hasher(self.kind));
                let trailer = h.try_finalize().map_err(|e| Error::Internal(e.to_string()))?;
                let mut w = PktWriter::default();
                wire::Sideband::new(&mut w).data(trailer.as_slice());
                w.flush();
                self.next += 1;
                Ok(Some(w.out))
            }
            _ => Ok(None),
        }
    }
}

/// A legal zero-object pack (PACK header + trailer). C1's inline `packfile`
/// section when every object moved to a URI — the client index-packs it anyway.
fn empty_pack(kind: gix_hash::Kind) -> Result<Vec<u8>, Error> {
    let head = gix_pack::data::header::encode(gix_pack::data::Version::V2, 0).to_vec();
    let mut h = gix_hash::hasher(kind);
    h.update(&head);
    let trailer = h
        .try_finalize()
        .map_err(|_| Error::Internal("empty-pack trailer".into()))?;
    let mut p = head;
    p.extend_from_slice(trailer.as_bytes());
    Ok(p)
}

/// C2's stream: the verbatim R2 pack body re-framed as band-1 sideband data.
/// Prelude (through `packfile`), then each upstream chunk framed — the pack's own
/// header/trailer ride inside the body bytes, so no hashing — then one flush.
/// `charge(0)` per chunk keeps the 240 s wall-clock bound a normal fetch has.
struct VerbatimStream {
    budget: ReqBudget,
    body: worker::ByteStream,
    prelude: Option<Vec<u8>>,
    finished: bool,
}
impl VerbatimStream {
    async fn step(&mut self) -> Result<Option<Vec<u8>>, Error> {
        if let Some(p) = self.prelude.take() {
            return Ok(Some(p));
        }
        if self.finished {
            return Ok(None);
        }
        self.budget.charge(0)?;
        match self.body.next().await {
            Some(Ok(chunk)) => {
                let mut w = PktWriter::default();
                wire::Sideband::new(&mut w).data(&chunk);
                Ok(Some(w.out))
            }
            Some(Err(e)) => Err(Error::from(e)),
            None => {
                self.finished = true;
                let mut w = PktWriter::default();
                w.flush();
                Ok(Some(w.out))
            }
        }
    }
}

/// FetchStream without the pkt/sideband framing: bundle header, then the PACK
/// bytes verbatim, then the trailer — bundle = file format, not a pkt stream.
struct ExportStream {
    budget: ReqBudget,
    bucket: Bucket,
    set: generate::SendSet,
    next: usize,
    hasher: gix_hash::Hasher,
    kind: gix_hash::Kind,
    prelude: Option<Vec<u8>>,
}
impl ExportStream {
    async fn step(&mut self) -> Result<Option<Vec<u8>>, Error> {
        if let Some(p) = self.prelude.take() {
            return Ok(Some(p));
        }
        let n = self.set.reads.len();
        match self.next {
            0 => {
                let mut hdr = b"PACK".to_vec();
                hdr.extend_from_slice(&2u32.to_be_bytes());
                hdr.extend_from_slice(&self.set.count().to_be_bytes());
                self.hasher.update(&hdr);
                self.next = 1;
                Ok(Some(hdr))
            }
            i if i <= n => {
                let chunk =
                    generate::pack_chunk(&self.bucket, &self.set, i - 1, &mut self.hasher, &mut self.budget)
                        .await?;
                self.next += 1;
                Ok(Some(chunk))
            }
            i if i == n + 1 => {
                let h = std::mem::replace(&mut self.hasher, gix_hash::hasher(self.kind));
                let trailer = h.try_finalize().map_err(|e| Error::Internal(e.to_string()))?;
                self.next += 1;
                Ok(Some(trailer.as_slice().to_vec()))
            }
            _ => Ok(None),
        }
    }
}

#[derive(serde::Deserialize)]
pub struct LookupDto {
    ids: Vec<String>,
    pack: Option<String>,
}
#[derive(serde::Deserialize)]
pub struct IndexDto {
    pack: PackMetaDto,
    rows: Vec<crate::store::ObjRow>,
}
#[derive(serde::Deserialize)]
struct PackMetaDto {
    id: String,
    push_id: String,
    count: i64,
    bytes: i64,
    commit_lo: i64,
    commit_hi: i64,
}

#[cfg(test)]
mod tests {
    use super::{commit_auth, mark_dedup, protected_verdict, mint_expiry, mint_scope, token_live, MAX_TOKEN_TTL_S, push_scope, ADMIN_AUTH, TOKEN_INVALID, scope_allows, scope_pattern_ok, DedupRow, ALL_REFS};
    use crate::error::Error;
    use crate::pack::generate;
    use crate::store::PackId;
    use std::collections::HashMap;

    fn slice(id: &str, count: u32) -> generate::PackSlice {
        generate::PackSlice {
            pack: PackId(id.to_string()),
            count,
            bytes: 100,
            bitmap: vec![0; (count as usize).div_ceil(8)],
        }
    }

    fn row(sha: &str, pack: &str, idx: u32) -> DedupRow {
        DedupRow { sha: sha.to_string(), pack_id: pack.to_string(), idx }
    }

    /// The GC-transition shape that produced "same object appears twice in the
    /// pack": the consolidated pack and its not-yet-swept sources are all live,
    /// so a sha has one objects row per pack. Ordered by sha those copies are
    /// adjacent — mark_dedup must emit exactly one physical entry per object.
    #[test]
    fn no_walk_dedup() {
        let mut packs = vec![slice("p1", 2), slice("p2", 2)];
        let by_pack: HashMap<String, usize> =
            [("p1".to_string(), 0usize), ("p2".to_string(), 1)].into_iter().collect();
        // sha order: a in p1@0 + p2@0 (dup), b only p1@1, c only p2@1
        let rows = vec![
            row("a", "p1", 0),
            row("a", "p2", 0),
            row("b", "p1", 1),
            row("c", "p2", 1),
        ];
        let mut last = String::new();
        mark_dedup(&rows, &mut packs, &by_pack, &mut last).unwrap();
        assert_eq!(packs[0].bitmap[0], 0b11); // a + b
        assert_eq!(packs[1].bitmap[0], 0b10); // c only — a's second copy skipped
        assert_eq!(last, "c");

        // page boundary mid-group: `last` carries the boundary sha across
        // pages, so a group's tail rows in the next page are still skipped
        let mut packs = vec![slice("p1", 2), slice("p2", 2)];
        let mut last = String::new();
        mark_dedup(&[row("a", "p1", 0), row("a", "p2", 0)], &mut packs, &by_pack, &mut last).unwrap();
        mark_dedup(&[row("b", "p1", 1), row("b", "p2", 1)], &mut packs, &by_pack, &mut last).unwrap();
        assert_eq!(packs[0].bitmap[0], 0b11);
        assert_eq!(packs[1].bitmap[0], 0);

        // idx past the slice's count errors instead of wrapping into a corrupt set
        let mut packs = vec![slice("p1", 1)];
        assert!(mark_dedup(&[row("z", "p1", 9)], &mut packs, &by_pack, &mut String::new()).is_err());
    }

    #[test]
    fn scope_pattern_validation() {
        for ok in [
            "refs/heads/main",
            "refs/heads/feature-*",
            "refs/heads/x*", // trailing star on a partial name
            "refs/*",
            "refs/tags/v1*", // glob over "v1…" tag names
            "refs/heads/",   // exact-match on a name no ref can have — useless, harmless
        ] {
            assert!(scope_pattern_ok(ok), "{ok}");
        }
        for bad in [
            "main",             // not under refs/
            "*",                // bare wildcard
            "refs/heads/*-x",   // mid-pattern glob
            "refs/heads/**",    // mid-pattern glob
            "refs/../x",        // not a valid partial refname
            "refs/heads/..",    // invalid refname
            "refs/heads/lo..k", // invalid refname
            "refs/tags/v1.*",   // stripped prefix "v1." isn't a partial refname — write v1*
            "",                 // empty
        ] {
            assert!(!scope_pattern_ok(bad), "{bad}");
        }
    }

    #[test]
    fn scope_matching() {
        // unrestricted
        // PR-3a: no scope / blank scope denies — fail closed, never unrestricted
        assert!(!scope_allows(None, "refs/heads/anything"));
        assert!(!scope_allows(Some(""), "refs/heads/anything"));
        assert!(!scope_allows(Some("  "), "refs/heads/anything"));
        assert!(!scope_allows(Some(" , ,"), "refs/heads/anything"));
        // the explicit all-refs sentinel covers every refs/ name
        assert!(scope_allows(Some(ALL_REFS), "refs/heads/main"));
        assert!(scope_allows(Some(ALL_REFS), "refs/tags/v1"));
        assert!(!scope_allows(Some(ALL_REFS), "HEAD"));
        // exact
        assert!(scope_allows(Some("refs/heads/main"), "refs/heads/main"));
        assert!(!scope_allows(Some("refs/heads/main"), "refs/heads/main2"));
        // prefix glob
        let s = Some("refs/heads/scoped-*");
        assert!(scope_allows(s, "refs/heads/scoped-ok"));
        assert!(scope_allows(s, "refs/heads/scoped-"));
        assert!(!scope_allows(s, "refs/heads/escape"));
        assert!(!scope_allows(s, "refs/tags/scoped-x"));
        // comma list: any pattern covers
        let m = Some("refs/heads/a, refs/tags/v1.*");
        assert!(scope_allows(m, "refs/heads/a"));
        assert!(scope_allows(m, "refs/tags/v1.2.3"));
        assert!(!scope_allows(m, "refs/heads/b"));
        // full-repo wildcard
        assert!(scope_allows(Some("refs/*"), "refs/heads/x"));
        assert!(!scope_allows(Some("refs/*"), "not-refs"));
    }

    #[test]
    fn push_scope_fails_closed() {
        // global admin: explicit all-refs, regardless of key/row
        assert_eq!(push_scope(true, "", None).unwrap(), ALL_REFS);
        assert_eq!(push_scope(true, "abc", None).unwrap(), ALL_REFS);
        // repo token present with a scope: captured verbatim (trimmed)
        assert_eq!(push_scope(false, "h", Some(("write", Some(" refs/heads/a ")))).unwrap(), "refs/heads/a");
        assert_eq!(push_scope(false, "h", Some(("write", Some(ALL_REFS)))).unwrap(), ALL_REFS);
        // R2-B1: row missing at begin (revoked / never existed) -> deny
        assert!(matches!(push_scope(false, "h", None), Err(Error::Auth)));
        // no presented key and not admin -> deny
        assert!(matches!(push_scope(false, "", None), Err(Error::Auth)));
        // empty / NULL scope on a presenting credential -> deny
        assert!(matches!(push_scope(false, "h", Some(("write", None))), Err(Error::Forbidden)));
        assert!(matches!(push_scope(false, "h", Some(("write", Some("")))), Err(Error::Forbidden)));
        assert!(matches!(push_scope(false, "h", Some(("write", Some("  ")))), Err(Error::Forbidden)));
        // a read token can never open a push
        assert!(matches!(push_scope(false, "h", Some(("read", Some(ALL_REFS)))), Err(Error::Forbidden)));
    }

    #[test]
    fn mint_scope_is_never_empty() {
        assert_eq!(mint_scope(None).unwrap(), ALL_REFS);
        assert_eq!(mint_scope(Some("")).unwrap(), ALL_REFS);
        assert_eq!(mint_scope(Some("   ")).unwrap(), ALL_REFS);
        assert_eq!(mint_scope(Some(" refs/heads/a,refs/tags/* ")).unwrap(), "refs/heads/a,refs/tags/*");
        assert!(mint_scope(Some("*")).is_err());
        assert!(mint_scope(Some("refs/heads/a,")).is_err());
        assert!(mint_scope(Some(&format!("refs/heads/{}", "a".repeat(260)))).is_err());
        // whatever mint stores, the check side honours
        for s in [None, Some("refs/heads/x")] {
            let stored = mint_scope(s).unwrap();
            assert!(scope_allows(Some(&stored), "refs/heads/x"));
        }
    }

    #[test]
    fn commit_auth_revalidates() {
        // admin: no extra scope beyond the captured one
        assert_eq!(commit_auth(Some(ADMIN_AUTH), None), Ok(None));
        // live write token: its current scope is returned for the AND-check
        assert_eq!(commit_auth(Some("h"), Some(("write", Some("refs/heads/a")))), Ok(Some("refs/heads/a".into())));
        // revoked mid-push
        assert_eq!(commit_auth(Some("h"), None), Err(TOKEN_INVALID));
        // demoted to read / scope blanked
        assert_eq!(commit_auth(Some("h"), Some(("read", Some(ALL_REFS)))), Err(TOKEN_INVALID));
        assert_eq!(commit_auth(Some("h"), Some(("write", None))), Err(TOKEN_INVALID));
        assert_eq!(commit_auth(Some("h"), Some(("write", Some(" ")))), Err(TOKEN_INVALID));
        // legacy push row without a marker: fail closed
        assert_eq!(commit_auth(None, None), Err(TOKEN_INVALID));
        assert_eq!(commit_auth(Some(""), Some(("write", Some(ALL_REFS)))), Err(TOKEN_INVALID));
        // narrowing wins: captured refs/* AND current refs/heads/a
        let cur = commit_auth(Some("h"), Some(("write", Some("refs/heads/a")))).unwrap();
        let scopes = [Some(ALL_REFS), cur.as_deref()];
        assert!(scopes.iter().all(|s| scope_allows(*s, "refs/heads/a")));
        assert!(!scopes.iter().all(|s| scope_allows(*s, "refs/heads/b")));
    }

    #[test]
    fn token_expiry() {
        // no expiry: live forever
        assert!(token_live(None, i64::MAX));
        // exclusive bound: live strictly before expires_at
        assert!(token_live(Some(1000), 999));
        assert!(!token_live(Some(1000), 1000));
        assert!(!token_live(Some(1000), 5000));
        // mint
        assert_eq!(mint_expiry(None, 10).unwrap(), None);
        assert_eq!(mint_expiry(Some(1), 10).unwrap(), Some(1010));
        assert_eq!(mint_expiry(Some(MAX_TOKEN_TTL_S), 0).unwrap(), Some(MAX_TOKEN_TTL_S * 1000));
        assert!(mint_expiry(Some(0), 10).is_err());
        assert!(mint_expiry(Some(-5), 10).is_err());
        assert!(mint_expiry(Some(MAX_TOKEN_TTL_S + 1), 10).is_err());
        // an expired row is filtered to None and so fails every path closed
        let row = Some(("write", Some(ALL_REFS))).filter(|_| token_live(Some(1000), 2000));
        assert!(matches!(push_scope(false, "h", row), Err(Error::Auth)));
        assert_eq!(commit_auth(Some("h"), row), Err(TOKEN_INVALID));
    }

    #[test]
    fn protected_rules() {
        // deletion always refused, whatever ancestry says
        assert_eq!(protected_verdict(false, true, Some(true)), Some("protected ref: deletion refused"));
        assert_eq!(protected_verdict(true, true, None), Some("protected ref: deletion refused"));
        // create allowed (deletion can't have reset it)
        assert_eq!(protected_verdict(true, false, None), None);
        // update: FF only, unproven = refused
        assert_eq!(protected_verdict(false, false, Some(true)), None);
        assert_eq!(protected_verdict(false, false, Some(false)), Some("protected ref: non-fast-forward"));
        assert_eq!(protected_verdict(false, false, None), Some("protected ref: cannot prove fast-forward"));
    }
}
