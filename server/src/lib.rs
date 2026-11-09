//! git-edge: a complete git smart-HTTP server on a Cloudflare Worker + one Durable Object
//! per repo + R2 pack storage. CONTRACTS.md is the binding spec.

pub mod auth;
pub mod edge;
pub mod error;
pub mod jobs;
pub mod mem;
pub mod pack;
pub mod platform;
pub mod remote;
pub mod repo_do;
pub mod sign;
pub mod store;
pub mod testhooks;
pub mod wire;

use std::cell::Cell;
use std::rc::Rc;

use error::Error;

/// Request-wide subrequest tally shared by every ReqBudget the request creates.
/// It outlives each individual budget, so `respond` can stamp x-ge-subrequests
/// on error responses after the charging budget has been dropped (audit P3).
/// Single-threaded isolate: an Rc<Cell> per request is enough — never global,
/// or concurrent fetches in one isolate would charge into each other's tally.
pub type Spend = Rc<Cell<u32>>;

/// Per-request budget (CONTRACTS.md 7). Every Bucket and stub call charges first.
pub struct ReqBudget {
    pub max_subrequests: u32,
    pub used: u32,
    pub started_ms: f64,
    pub max_ms: f64,
    /// Where charges are mirrored for the response header; None keeps the old
    /// behaviour for budgets that own the whole spend (jobs, DO internals).
    pub sink: Option<Spend>,
}
impl ReqBudget {
    /// Paid-plan subrequest ceiling (7.1) — the x-ge-subrequests denominator.
    pub const PAID_SUBREQUESTS: u32 = 9_000;
    /// Paid-plan request wall clock (7.1): 240 s. The pass-B admission
    /// gate's permit lease derives from it (pack::run::PERMIT_LEASE_MS).
    pub const MAX_MS: f64 = 240_000.0;
    /// Paid-plan request budget (7.1): 9,000 subrequests, 240 s wall clock.
    pub fn paid() -> Self {
        Self {
            max_subrequests: Self::PAID_SUBREQUESTS,
            used: 0,
            started_ms: js_sys::Date::now(),
            max_ms: Self::MAX_MS,
            sink: None,
        }
    }
    /// Mirror every charge into `spend` — the request-wide tally the edge
    /// stamps as x-ge-subrequests, including on error responses.
    pub fn reporting(mut self, spend: &Spend) -> Self {
        self.sink = Some(Rc::clone(spend));
        self
    }
    /// Wall-clock milliseconds left in this budget's window — the pass-B
    /// admission wait caps itself at this minus a reserve, so a queued push
    /// can't spend the time pass B itself needs (CONTRACTS.md 7.1).
    pub fn ms_left(&self) -> f64 {
        (self.started_ms + self.max_ms - js_sys::Date::now()).max(0.0)
    }
    /// A small fresh budget for must-run cleanup — the push abort. The
    /// request's own budget is often WHY cleanup runs (it timed out or hit
    /// a cap), so the abort gets its own clock and cap while still
    /// reporting into the request's spend tally.
    pub fn cleanup(spend: &Spend) -> Self {
        Self {
            max_subrequests: 8,
            used: 0,
            started_ms: js_sys::Date::now(),
            max_ms: 10_000.0,
            sink: Some(Rc::clone(spend)),
        }
    }
    /// Subrequests already spent elsewhere in the request (e.g. the DO's R2
    /// spend read from its own x-ge-subrequests header): counted in the
    /// response header but not against this budget's limit — a DO error must
    /// not turn into a second, edge-side Budget failure.
    pub fn report(&self, n: u32) {
        if let Some(s) = &self.sink {
            s.set(s.get().saturating_add(n));
        }
    }
    pub fn charge(&mut self, n: u32) -> Result<(), Error> {
        self.used = self.used.saturating_add(n);
        self.report(n);
        if self.used > self.max_subrequests || js_sys::Date::now() - self.started_ms > self.max_ms {
            return Err(Error::Budget);
        }
        Ok(())
    }
}

#[worker::event(fetch)]
pub async fn fetch(
    req: worker::Request,
    env: worker::Env,
    _ctx: worker::Context,
) -> worker::Result<worker::Response> {
    edge::fetch(req, env).await
}
