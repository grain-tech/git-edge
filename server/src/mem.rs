//! Live-heap accounting behind the global allocator (CONTRACTS.md A10 memory
//! bound). Wasm linear memory is a hard ~128 MiB ceiling per isolate and an
//! over-budget allocation aborts inside the futures task queue — the request
//! then hangs instead of answering. `live_bytes()`/`peak_bytes()` give the
//! pass-B loop a signal it can act on before the wall is hit.
//!
//! The wrapper is plain atomics only — no allocation, no locks — so it costs
//! nothing per alloc/free and works identically on wasm32 and native (where
//! `cargo test` exercises it).

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

/// Live/peak byte tally — a standalone type so the arithmetic is unit-testable
/// without touching the process-wide counters (parallel tests share those).
#[derive(Default)]
pub struct Tally {
    live: AtomicUsize,
    peak: AtomicUsize,
}
impl Tally {
    pub const fn new() -> Self {
        Self { live: AtomicUsize::new(0), peak: AtomicUsize::new(0) }
    }
    /// A successful `alloc`: live grows, peak follows the new high-water mark.
    fn alloc(&self, n: usize) {
        let live = self.live.fetch_add(n, Ordering::Relaxed).saturating_add(n);
        self.peak.fetch_max(live, Ordering::Relaxed);
    }
    /// `dealloc` of a block `alloc` counted — sizes always pair by Layout.
    fn dealloc(&self, n: usize) {
        self.live.fetch_sub(n, Ordering::Relaxed);
    }
    /// `realloc` nets out to the size delta: System may extend in place or
    /// move the block, but the live total changes by new - old either way.
    fn realloc(&self, old: usize, new: usize) {
        match new.cmp(&old) {
            std::cmp::Ordering::Greater => self.alloc(new - old),
            std::cmp::Ordering::Less => self.dealloc(old - new),
            std::cmp::Ordering::Equal => {}
        }
    }
    pub fn live(&self) -> usize {
        self.live.load(Ordering::Relaxed)
    }
    pub fn peak(&self) -> usize {
        self.peak.load(Ordering::Relaxed)
    }
}

static HEAP: Tally = Tally::new();

struct Counting;
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        let p = System.alloc(l);
        if !p.is_null() {
            HEAP.alloc(l.size());
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        HEAP.dealloc(l.size());
        System.dealloc(p, l)
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new_size: usize) -> *mut u8 {
        let np = System.realloc(p, l, new_size);
        if !np.is_null() {
            HEAP.realloc(l.size(), new_size);
        }
        np
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

/// Bytes currently held by Rust allocations in this isolate — the number the
/// memory guard compares against its ceilings. JS-heap copies (Uint8Array
/// uploads, request bodies) live outside wasm linear memory and are not
/// counted; the guard's headroom covers them.
pub fn live_bytes() -> usize {
    HEAP.live()
}

/// High-water mark of `live_bytes()` since the isolate booted — diagnostics.
pub fn peak_bytes() -> usize {
    HEAP.peak()
}

/// Wasm linear memory in bytes — the ceiling `live_bytes` grows toward.
/// It NEVER shrinks (pages are not returned to the runtime), so it
/// measures the isolate's high-water mark, not current pressure; the
/// pass-B guard pairs it with `live_bytes` for exactly that reason (see
/// LINEAR_HARD in pack::ingest). Native builds have no such ceiling:
/// returns 0 so the linear-aware trip stays inert, and tests inject their
/// own reading through `mem_guard`'s `linear` parameter.
#[cfg(target_arch = "wasm32")]
pub fn linear_bytes() -> usize {
    // memory_size reports 64 KiB pages for memory index 0
    core::arch::wasm32::memory_size(0) * 65_536
}
#[cfg(not(target_arch = "wasm32"))]
pub fn linear_bytes() -> usize {
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tally_tracks_live_and_peak() {
        let t = Tally::new();
        t.alloc(100);
        t.alloc(50);
        assert_eq!(t.live(), 150);
        assert_eq!(t.peak(), 150);
        t.dealloc(100);
        assert_eq!(t.live(), 50);
        assert_eq!(t.peak(), 150, "peak must not follow live down");
        t.alloc(200);
        assert_eq!(t.peak(), 250);
    }

    #[test]
    fn tally_realloc_nets_the_delta() {
        let t = Tally::new();
        t.alloc(100);
        t.realloc(100, 300);
        assert_eq!(t.live(), 300);
        assert_eq!(t.peak(), 300);
        t.realloc(300, 50);
        assert_eq!(t.live(), 50);
        assert_eq!(t.peak(), 300);
        t.realloc(50, 50);
        assert_eq!(t.live(), 50);
    }

    #[test]
    fn global_counters_are_sane() {
        // the test binary itself allocates through the counting allocator;
        // live can never be negative and peak >= live
        assert!(peak_bytes() >= live_bytes());
    }
}
