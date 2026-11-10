//! Fallible growth for pass-B buffers (CONTRACTS.md A10 memory bound).
//!
//! The memory guard (`pack::ingest::mem_guard_peak`) is the budget and
//! policy layer. It sheds the cache early and answers in-band before most
//! growth. It cannot prove that the allocator finds a contiguous block,
//! though. When wasm `memory.grow` fails, the allocator returns null, and
//! an infallible `Vec` or `HashMap` growth then aborts the isolate. The
//! request hangs. These helpers are the safety net under the guard: every
//! large or preflighted growth reserves through them BEFORE the write that
//! would grow, so a failed allocation becomes the in-band Limit
//! `MEM_RETRY_MSG` instead of a trap.
//!
//! A buffer that a JS or worker API hands back (an R2 body's `bytes()`, a
//! request body) is allocated outside these helpers. Its call site keeps
//! the guard's preflight and says so in a comment.

use std::collections::{HashMap, VecDeque};
use std::hash::{BuildHasher, Hash};

use crate::error::Error;

/// The in-band wording for a failed reservation: the isolate's heap could
/// not place the block right now, and a retry on a calmer isolate helps.
pub(crate) const MEM_RETRY_MSG: &str = "server under memory pressure; retry in a minute";

/// The in-band Limit a failed reservation answers with.
pub(crate) fn failed() -> Error {
    Error::Limit(MEM_RETRY_MSG.into())
}

/// Is `e` the memory-pressure Limit? It is transient: a caller that treats
/// other Limits as final must retry this one.
pub(crate) fn is_retry(e: &Error) -> bool {
    matches!(e, Error::Limit(m) if m == MEM_RETRY_MSG)
}

#[cfg(test)]
thread_local! {
    /// Test hook: reservations whose new block exceeds this many bytes
    /// fail the way a failed `memory.grow` does. None is the real path.
    static FAIL_ABOVE: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
    /// Test hook: when set, only reservations from this call site
    /// (file, line) can fail. None lets every site fail.
    static FAIL_SITE: std::cell::Cell<Option<(&'static str, u32)>> = const { std::cell::Cell::new(None) };
    /// Test log: every gated reservation on this thread while logging is on.
    static ASKS: std::cell::RefCell<Option<Vec<Ask>>> = const { std::cell::RefCell::new(None) };
}

/// Test hook: make every reservation whose new block is larger than `n`
/// bytes fail on this thread. `None` turns the hook off.
#[cfg(test)]
pub(crate) fn fail_above(n: Option<usize>) {
    FAIL_ABOVE.with(|f| f.set(n));
}

/// Test hook: limit `fail_above` to the reservations from one call site.
/// `None` lets every site fail again.
#[cfg(test)]
pub(crate) fn fail_only_at(site: Option<(&'static str, u32)>) {
    FAIL_SITE.with(|f| f.set(site));
}

/// One gated reservation, as the test log records it: the caller's
/// file and line, the new block's size, and whether the hook refused it.
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Ask {
    pub file: &'static str,
    pub line: u32,
    pub bytes: usize,
    pub refused: bool,
}

/// Test log: start recording every gated reservation on this thread.
#[cfg(test)]
pub(crate) fn log_asks() {
    ASKS.with(|a| *a.borrow_mut() = Some(Vec::new()));
}

/// Test log: stop recording and return what was recorded.
#[cfg(test)]
pub(crate) fn take_asks() -> Vec<Ask> {
    ASKS.with(|a| a.borrow_mut().take()).unwrap_or_default()
}

/// Test-only RAII guard: disarms this thread's hooks on drop — the fail
/// threshold, the site filter, the ask log and the mem readings — so a
/// failed assertion cannot leave one armed for the next test on the
/// thread. Hold it for the test's whole scope; arm the hooks after
/// creating it.
#[cfg(test)]
#[must_use]
pub(crate) struct HookGuard;

#[cfg(test)]
impl Drop for HookGuard {
    fn drop(&mut self) {
        fail_above(None);
        fail_only_at(None);
        ASKS.with(|a| *a.borrow_mut() = None);
        crate::mem::set_live_override(None);
        crate::mem::set_linear_override(None);
    }
}

/// The test hook's gate for a new block of `bytes`. A no-op outside
/// tests. In tests, `track_caller` names the helper's caller, so the
/// log and the site filter see the real call site.
#[cfg_attr(test, track_caller)]
fn gate(bytes: usize) -> Result<(), Error> {
    #[cfg(test)]
    {
        let at = std::panic::Location::caller();
        let site_ok = FAIL_SITE.with(|f| f.get()).is_none_or(|(file, line)| at.file() == file && at.line() == line);
        let refused = site_ok && FAIL_ABOVE.with(|f| f.get()).is_some_and(|n| bytes > n);
        ASKS.with(|a| {
            if let Some(log) = a.borrow_mut().as_mut() {
                log.push(Ask { file: at.file(), line: at.line(), bytes, refused });
            }
        });
        if refused {
            return Err(failed());
        }
    }
    let _ = bytes;
    Ok(())
}

/// Reserve room for `additional` more elements with Vec's amortized
/// growth, fallibly. Nothing allocates when they already fit.
#[cfg_attr(test, track_caller)]
pub(crate) fn vec<T>(v: &mut Vec<T>, additional: usize) -> Result<(), Error> {
    let need = v.len().saturating_add(additional);
    if need <= v.capacity() {
        return Ok(());
    }
    let new_cap = need.max(v.capacity().saturating_mul(2));
    gate(new_cap.saturating_mul(std::mem::size_of::<T>()))?;
    v.try_reserve(additional).map_err(|_| failed())
}

/// Reserve room for exactly `additional` more elements, fallibly.
#[cfg_attr(test, track_caller)]
pub(crate) fn vec_exact<T>(v: &mut Vec<T>, additional: usize) -> Result<(), Error> {
    let need = v.len().saturating_add(additional);
    if need <= v.capacity() {
        return Ok(());
    }
    gate(need.saturating_mul(std::mem::size_of::<T>()))?;
    v.try_reserve_exact(additional).map_err(|_| failed())
}

/// An empty Vec with room for exactly `n` elements, fallibly.
#[cfg_attr(test, track_caller)]
pub(crate) fn with_capacity<T>(n: usize) -> Result<Vec<T>, Error> {
    let mut v = Vec::new();
    vec_exact(&mut v, n)?;
    Ok(v)
}

/// A tight copy of `src`: `try_reserve_exact`, then `extend_from_slice`
/// inside the reserved capacity.
#[cfg_attr(test, track_caller)]
pub(crate) fn copy(src: &[u8]) -> Result<Vec<u8>, Error> {
    let mut v = with_capacity(src.len())?;
    v.extend_from_slice(src);
    Ok(v)
}

/// `n` zero bytes, fallibly — an inflate target or a scratch buffer.
#[cfg_attr(test, track_caller)]
pub(crate) fn zeroed(n: usize) -> Result<Vec<u8>, Error> {
    let mut v = with_capacity(n)?;
    v.resize(n, 0);
    Ok(v)
}

/// A copy of `src` in one exact, fallible block.
#[cfg_attr(test, track_caller)]
pub(crate) fn string(src: &str) -> Result<String, Error> {
    let mut s = String::new();
    if !src.is_empty() {
        gate(src.len())?;
        s.try_reserve_exact(src.len()).map_err(|_| failed())?;
    }
    s.push_str(src);
    Ok(s)
}

/// Reserve room for `additional` more entries in a HashMap, fallibly.
/// The gate prices the new table's slots at one `(K, V)` each.
#[cfg_attr(test, track_caller)]
pub(crate) fn map<K: Eq + Hash, V, S: BuildHasher>(
    m: &mut HashMap<K, V, S>,
    additional: usize,
) -> Result<(), Error> {
    let need = m.len().saturating_add(additional);
    if need <= m.capacity() {
        return Ok(());
    }
    let new_cap = need.max(m.capacity().saturating_mul(2));
    gate(new_cap.saturating_mul(std::mem::size_of::<(K, V)>()))?;
    m.try_reserve(additional).map_err(|_| failed())
}

/// Reserve room for `additional` more elements in a VecDeque, fallibly.
#[cfg_attr(test, track_caller)]
pub(crate) fn deque<T>(q: &mut VecDeque<T>, additional: usize) -> Result<(), Error> {
    let need = q.len().saturating_add(additional);
    if need <= q.capacity() {
        return Ok(());
    }
    let new_cap = need.max(q.capacity().saturating_mul(2));
    gate(new_cap.saturating_mul(std::mem::size_of::<T>()))?;
    q.try_reserve(additional).map_err(|_| failed())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn is_retry(e: &Error) -> bool {
        matches!(e, Error::Limit(m) if m == MEM_RETRY_MSG)
    }

    /// A real allocation failure, not the hook: a reservation the
    /// allocator cannot satisfy returns the in-band Limit and never
    /// panics. (Capacity overflow takes the same `TryReserveError` path
    /// as a null from a failed `memory.grow`.)
    #[test]
    fn real_reservation_failure_is_the_in_band_limit() {
        let mut v: Vec<u64> = vec![1];
        assert!(is_retry(&vec(&mut v, usize::MAX).unwrap_err()));
        assert!(is_retry(&vec_exact(&mut v, usize::MAX / 2).unwrap_err()));
        assert_eq!(v, vec![1], "a failed reservation leaves the Vec as it was");
        let mut m: HashMap<u64, u64> = HashMap::new();
        assert!(is_retry(&map(&mut m, usize::MAX).unwrap_err()));
        let mut q: VecDeque<u64> = VecDeque::new();
        assert!(is_retry(&deque(&mut q, usize::MAX).unwrap_err()));
        assert!(is_retry(&with_capacity::<u64>(usize::MAX).unwrap_err()));
    }

    /// The hook fails a new block over N bytes and passes one at N. A
    /// growth that fits the capacity allocates nothing and never fails.
    #[test]
    fn hook_fails_blocks_over_the_threshold_only() {
        let _guard = HookGuard;
        fail_above(Some(64));
        let over = copy(&[0u8; 65]);
        let at = copy(&[0u8; 64]);
        let mut v: Vec<u8> = Vec::with_capacity(1_000);
        let fits = vec(&mut v, 1_000);
        let zero_over = zeroed(65);
        let string_over = string(&"x".repeat(65));
        let mut m: HashMap<u64, u64> = HashMap::new();
        let map_over = map(&mut m, 100);
        let mut q: VecDeque<u64> = VecDeque::new();
        let deque_over = deque(&mut q, 100);
        fail_above(None);
        assert!(is_retry(&over.unwrap_err()));
        assert_eq!(at.unwrap().len(), 64);
        fits.unwrap();
        assert!(is_retry(&zero_over.unwrap_err()));
        assert!(is_retry(&string_over.unwrap_err()));
        assert!(is_retry(&map_over.unwrap_err()));
        assert!(is_retry(&deque_over.unwrap_err()));
        assert_eq!(zeroed(65).unwrap(), vec![0u8; 65], "the hook is off again");
    }

    /// The amortized gate prices Vec's real doubling, not the bare need.
    #[test]
    fn amortized_gate_prices_the_doubling() {
        let _guard = HookGuard;
        let mut v: Vec<u8> = vec![0; 100];
        assert_eq!(v.capacity(), 100);
        fail_above(Some(150));
        let r = vec(&mut v, 1);
        assert!(is_retry(&r.unwrap_err()), "101 bytes fit 150 but the doubled 200-byte block does not");
    }

    /// A need equal to the capacity already fits: Ok without the gate.
    #[test]
    fn exact_fit_never_asks_the_gate() {
        let _guard = HookGuard;
        log_asks();
        fail_above(Some(0));

        let mut v: Vec<u8> = Vec::with_capacity(64);
        v.extend_from_slice(&[0u8; 10]);
        let add = v.capacity() - v.len();
        assert!(vec_exact(&mut v, add).is_ok());

        let mut m: HashMap<u64, u64> = HashMap::with_capacity(8);
        for i in 0..4u64 {
            m.insert(i, i);
        }
        let add = m.capacity() - m.len();
        assert!(map(&mut m, add).is_ok());

        let mut q: VecDeque<u64> = VecDeque::with_capacity(8);
        for i in 0..4u64 {
            q.push_back(i);
        }
        let add = q.capacity() - q.len();
        assert!(deque(&mut q, add).is_ok());

        assert!(take_asks().is_empty(), "an exact fit must not reach the gate");
    }
}
