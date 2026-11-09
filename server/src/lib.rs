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

/// A deterministic clock for tests — `js_sys::Date::now()` panics on native
/// targets, so every wall-clock read in the crate routes here under
/// `cfg(test)`. Tests advance it to simulate elapsed request/lease time.
#[cfg(test)]
pub(crate) mod test_clock {
    use std::cell::Cell;
    thread_local! {
        static NOW: Cell<f64> = const { Cell::new(0.0) };
    }
    pub fn now() -> f64 {
        NOW.with(|c| c.get())
    }
    pub fn set(ms: f64) {
        NOW.with(|c| c.set(ms));
    }
    pub fn advance(ms: f64) {
        NOW.with(|c| c.set(c.get() + ms));
    }
}

/// Wall clock in milliseconds — js_sys on wasm, the test clock under
/// `cfg(test)` so budgets and leases are drivable natively.
pub(crate) fn wall_now() -> f64 {
    #[cfg(test)]
    return test_clock::now();
    #[cfg(not(test))]
    js_sys::Date::now()
}

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
    /// The owning receive-pack request's lease handle — `charge` renews
    /// THIS request's permit and receive-pack entry, never another's. None
    /// for every other budget: a bystander's charges touch no lease.
    pub lease: Option<Rc<pack::run::Lease>>,
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
            started_ms: wall_now(),
            max_ms: Self::MAX_MS,
            sink: None,
            lease: None,
        }
    }
    /// Mirror every charge into `spend` — the request-wide tally the edge
    /// stamps as x-ge-subrequests, including on error responses.
    pub fn reporting(mut self, spend: &Spend) -> Self {
        self.sink = Some(Rc::clone(spend));
        self
    }
    /// Bind the request's lease handle: every charge on this budget renews
    /// that request's own leases (see pack::run::Lease).
    pub fn leased(mut self, lease: Rc<pack::run::Lease>) -> Self {
        self.lease = Some(lease);
        self
    }
    /// Wall-clock milliseconds left in this budget's window — the pass-B
    /// admission wait caps itself at this minus a reserve, so a queued push
    /// can't spend the time pass B itself needs (CONTRACTS.md 7.1).
    pub fn ms_left(&self) -> f64 {
        (self.started_ms + self.max_ms - wall_now()).max(0.0)
    }
    /// A small fresh budget for must-run cleanup — the push abort. The
    /// request's own budget is often WHY cleanup runs (it timed out or hit
    /// a cap), so the abort gets its own clock and cap while still
    /// reporting into the request's spend tally.
    pub fn cleanup(spend: &Spend) -> Self {
        Self {
            max_subrequests: 8,
            used: 0,
            started_ms: wall_now(),
            max_ms: 10_000.0,
            sink: Some(Rc::clone(spend)),
            lease: None,
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
        if self.used > self.max_subrequests || wall_now() - self.started_ms > self.max_ms {
            return Err(Error::Budget);
        }
        // a charge IS progress on a receive-pack request — renew its own
        // permit/peer leases so a working push never ages past the sweep.
        // Budgets without a lease (every other request) renew nothing.
        if let Some(l) = &self.lease {
            l.renew()?;
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
