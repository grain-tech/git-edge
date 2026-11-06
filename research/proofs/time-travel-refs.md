# Time-travel refs

> Second pass · Idea #20 · verdict: **lands with caveats** · feasibility 4/5 · reliability 4/5 · correctness 3/5 (first pass 4/4/4)
> First pass: [proof](./archived/time-travel-refs.md) · [review](../reviews/archived/time-travel-refs.md) · Second pass: [review](../reviews/time-travel-refs.md) · Contract: [CONTRACTS.md](../../CONTRACTS.md)


## Mechanism
The durable half of this idea is already written by CONTRACTS.md: section 3 step 4 appends one `reflog(name, old, new, push_id, principal, at)` row per accepted ref move inside the `commit_push` sync span, so the log is exactly as ordered and atomic as the refs themselves (`transactionSync` is gone; the no-await span is the unit, A3). What this module adds is a read-only `refs/at/<epoch>/<name>` namespace, resolved inside `/_do/ls-refs` (1.3, "Awaits inside: none") by sync SQLite over `reflog`: `<epoch>` is unix seconds — or any digit prefix of one — and `<name>` is a full ref name (looked up verbatim) or a short name tried in gitrevisions order (`refs/tags/` before `refs/heads/`). A client running `git fetch origin refs/at/1735689600/refs/heads/main` sends `ref-prefix refs/at/1735689600/refs/heads/main`, gets back `<sha> refs/at/1735689600/refs/heads/main`, then sends `want <sha>`, which `send_set` serves like any other oid: section 9 step 1 requires only that the want be `live`, not that a ref point at it. The bare prefix `refs/at/` (or `refs/at/<epoch>`) enumerates tip moves, newest first, capped. The first pass's two blockers are closed by the contract rather than by new code: `ref-in-want` is not advertised (1.1 rule 6, section 12), so git never sends `want-ref` and no `wanted-refs` section exists to write; and reflog expiry is `JobKind::ReflogExpire` on the shared alarm dispatcher (section 4, A3) instead of a private `setAlarm`. Four small write-backs remain, listed in the REGISTRY comment and in Known limits: the `rows()` hook in `ls_refs`, a `reserved()` check plus a monotonic `at` clamp in `apply_one`, reflog tips as `GcMark` roots, and the `ReflogExpire` enqueue at `boot`. No new route, table (besides one index) or R2 key exists; nothing is written on the read path.

## Primitives
- `SqlStorage::exec` + `SqlCursor::{to_array, one}` inside the `/_do/ls-refs` sync span (1.3): verified (memo section 1, spike). Resolution is pure SQLite — no R2, no stub call, no await — so an advertisement is decided atomically with respect to commits, and `SELECT changes()`-style span rules (#1, #4) do not even apply on the read path.
- `reflog` rows written by `apply_one` (section 3 step 4) with `at` in unix ms and `id INTEGER PRIMARY KEY AUTOINCREMENT` as insertion order: contract. The index `reflog_name_at ON reflog(name, at, id)` is new (REGISTRY, A9).
- `wire::{LsRefsArgs.prefixes, RefRow, write_ls_refs}` (1.1): `write_ls_refs` applies `ref-prefix` as a `starts_with` filter and served `peel`/`symrefs`/`unborn` to real git 2.43 in the spike (protocol-v2-only). Byte-length pkt-lines come from `gix-packetline` (rule 1): the first pass's UTF-16 `s.length` bug class cannot recur.
- `jobs::{enqueue, dispatch, run_slice, SliceOutcome}` with `jobs::rearm` as the sole `set_alarm` caller (4.1, A3): second `setAlarm` cancels the first, measured (#5). `ReflogExpire` is one `JobKind` variant registered under A9; `enqueue` dedups by kind and is sync.
- `Index::lookup` on the want path: `send_set` step 1 (`partial-clone-filters`) accepts any live oid; a `refs/at/` sha needs no `refs` row.
- `js_sys::Date::now()` for `at_ms` and `run_slice`: standard `js-sys`, as everywhere (section 8).
- git behaviour used, checked against git 2.43 source and the first-pass review's interop trace, not yet by scenario run: `git fetch origin refs/at/T/name` emits `ref-prefix refs/at/T/name` then `want <sha>`; `git ls-remote origin 'refs/at/*'` emits `ref-prefix refs/at/`; `refs/at/<digits>/...` passes `check-ref-format` (digits and slashes only); without `ref-in-want` in the advertisement no `want-ref` line is ever sent (rule 6).
- No new host API: no R2 key, no fetch, no `js_sys::Reflect`. Bound parameters per statement: at most 4, so A6 is trivially satisfied; no `IN(...)`, no `json_each`.

## Proof code
```rust
// src/repo_do/time_travel.rs -- CONTRACTS.md 1.1, 1.3, 3, 4, 5; amendments A2, A3, A4, A9. worker 0.8.5, gix-hash 0.26.2.
// REGISTRY (A9): JobKind::ReflogExpire; CREATE INDEX reflog_name_at ON reflog(name, at, id). No new routes
// (`/_do/ls-refs` gains the rows() hook), no new tables (reflog is section 3), no new R2 key prefixes.
// Write-backs: (a) ls_refs appends rows() to its RefRow list; (b) apply_one refuses reserved() names and stamps
// reflog.at with at_ms(); (c) GcMark unions gc_roots() into its frontier seed; (d) boot enqueues ReflogExpire like
// Janitor (A4). `d.q`, `d.sql`, `now_ms` are the RepoDo helpers of repo-do-ref-authority (pub(crate)).
use bstr::{BStr, BString, ByteSlice};
use gix_hash::ObjectId;
use worker::SqlStorageValue as V;
use crate::{error::Error, jobs::{self, Job, JobKind, SliceBudget, SliceOutcome}, repo_do::RepoDo, wire::{LsRefsArgs, RefRow}};

const ZERO: &str = "0000000000000000000000000000000000000000";
const KEEP_MS: i64 = 90 * 86_400_000;        // git's gc.reflogExpire default; reflog.at is unix ms (section 3)
const DAY_MS: i64 = 86_400_000;
const ENUM_MAX: i64 = 1_000;                 // bare refs/at/ cap (review caveat): <= ~110 KB of pkt-lines

#[derive(serde::Deserialize)] struct E { name: String, new: String, at: i64 }
#[derive(serde::Deserialize)] struct R { new: String }
#[derive(serde::Deserialize)] struct M { m: Option<i64> }
fn db_oid(hex: &str) -> Result<ObjectId, Error> {
    ObjectId::from_hex(hex.as_bytes()).map_err(|_| Error::Internal("reflog sha".into()))
}

/// (b) `refs/at` and everything under it is read-only; apply_one returns this as the ng reason before the CAS.
pub fn reserved(name: &BStr) -> Option<&'static str> {
    (name == b"refs/at" || name.starts_with(b"refs/at/")).then_some("refs/at/ is read-only")
}

/// (b) Strictly increasing `at` per repo, inside the commit span: a DO restart on a skewed clock can no longer make
/// `at < hi` pick rows out of order (review caveat). Every `at` is unique, so enumeration lines are unique too.
pub fn at_ms(d: &RepoDo) -> Result<i64, Error> {
    let last = d.q("SELECT MAX(at) AS m FROM reflog", vec![])?.one::<M>()?.m.unwrap_or(0);
    Ok(now_ms().max(last.saturating_add(1)))
}

/// (a) Synthetic RefRows for every ref-prefix naming the refs/at/ namespace. Sync; runs in the /_do/ls-refs span.
pub fn rows(d: &RepoDo, args: &LsRefsArgs) -> Result<Vec<RefRow>, Error> {
    let mut out = Vec::new();
    for p in &args.prefixes {
        let p: &[u8] = p.as_ref();
        let spec = if p == b"refs/at" { Some(&[][..]) } else { p.strip_prefix(b"refs/at/") };
        if let Some(spec) = spec { handle(d, spec, &mut out)?; }
    }
    Ok(out)
}

fn handle(d: &RepoDo, spec: &[u8], out: &mut Vec<RefRow>) -> Result<(), Error> {
    let (digits, name) = match spec.iter().position(|&b| b == b'/') {
        Some(i) => { let (dg, rest) = spec.split_at(i); (dg, rest.get(1..).unwrap_or(&[])) }
        None => (spec, &[][..]),
    };
    let Some((lo, hi)) = epoch_range(digits) else { return Ok(()) };   // refs/at/main is not an epoch: no rows
    if name.is_empty() {                                             // refs/at/ , refs/at/<epoch>: enumerate moves
        for r in d.q("SELECT name,new,at FROM reflog WHERE new<>? AND at>=? AND at<? ORDER BY at DESC,id DESC LIMIT ?",
                     vec![V::from(ZERO), V::from(lo), V::from(hi), V::from(ENUM_MAX)])?.to_array::<E>()? {
            out.push(RefRow { name: format!("refs/at/{}/{}", r.at / 1_000, r.name).into(),  // full name re-resolves verbatim
                              target: db_oid(&r.new)?, peeled: None });
        }
        return Ok(());
    }
    // refs/at/<epoch>/<name>: the newest transition of that ref before `hi`. A full `refs/...` name is verbatim;
    // a short name tries refs/tags/ then refs/heads/ — gitrevisions order, not the first pass's heads-first fallback.
    let cands: Vec<BString> = if name.starts_with(b"refs/") { vec![name.into()] } else {
        vec![format!("refs/tags/{}", name.as_bstr()).into(), format!("refs/heads/{}", name.as_bstr()).into()] };
    for c in &cands {
        if let Some(new) = newest(d, c, hi)? {
            out.push(RefRow { name: format!("refs/at/{}", spec.as_bstr()).into(), target: db_oid(&new)?, peeled: None });
            return Ok(());
        }
    }
    Ok(())
}

fn newest(d: &RepoDo, full: &BStr, hi: i64) -> Result<Option<String>, Error> {
    let Some(full) = full.to_str().ok() else { return Ok(None) };   // non-UTF-8 can never match a stored TEXT name
    Ok(d.q("SELECT new FROM reflog WHERE name=? AND at<? ORDER BY at DESC,id DESC LIMIT 1",
           vec![V::from(full), V::from(hi)])?.to_array::<R>()?.into_iter().next()
        .and_then(|r| (r.new != ZERO).then_some(r.new)))            // new=zero: ref was deleted at that instant
}

/// `refs/at/<digits>`: 1-10 digits read as (a prefix of) unix seconds, 11-13 as unix ms. Returns the [lo, hi) ms window.
fn epoch_range(digits: &[u8]) -> Option<(i64, i64)> {
    if digits.is_empty() { return Some((0, i64::MAX)); }
    if !digits.iter().all(|b| b.is_ascii_digit()) { return None; }
    let (w, scale) = match digits.len() { 1..=10 => (10u32, 1_000i64), 11..=13 => (13, 1), _ => return None };
    let d: i64 = digits.to_str().ok()?.parse().ok()?;
    let p = 10i64.checked_pow(w.checked_sub(digits.len() as u32)?)?;
    Some((d.checked_mul(p)?.checked_mul(scale)?, d.checked_add(1)?.checked_mul(p)?.checked_mul(scale)?))
}

/// (c) GC roots: every reflog tip stays live while its row exists, so a refs/at/ answer cannot be swept out from
/// under a later fetch (review caveat). Bounded by reflog size; GcMark spills past 50,000 ids to gc_frontier (5).
pub fn gc_roots(d: &RepoDo) -> Result<Vec<ObjectId>, Error> {
    d.q("SELECT DISTINCT new FROM reflog WHERE new<>?", vec![V::from(ZERO)])?.to_array::<R>()?
        .iter().map(|r| db_oid(&r.new)).collect()
}

/// JobKind::ReflogExpire slice (REGISTRY): one sync statement a day. The newest row per name is kept, so a resolvable
/// refs/at/ answer is always GC-rooted; older rows go, after which GcMark may collect their tips. Never calls
/// set_alarm (4.1): enqueue is sync inside the slice and dispatch rearms after the span (4.2, A3).
pub fn run_slice(d: &RepoDo, _job: &Job, _budget: &mut SliceBudget) -> Result<SliceOutcome, Error> {
    let now = now_ms();
    d.q("DELETE FROM reflog WHERE at<? AND id NOT IN (SELECT MAX(id) FROM reflog GROUP BY name)",
        vec![V::from(now.saturating_sub(KEEP_MS))])?;
    jobs::enqueue(&d.sql(), JobKind::ReflogExpire, now.saturating_add(DAY_MS), "{}")?;
    Ok(SliceOutcome::Done)
}
```

## Why it works
- **The log cannot disagree with the refs.** `apply_one` inserts the reflog row inside the `commit_push` sync span (section 3 step 4), so a `refs/at/` answer and the `refs` row behind it commit or roll back together; the first pass's `transactionSync` is subsumed by the no-await span (A3), with `SELECT changes()` as the CAS oracle (#1).
- **The semantics are git's `@{<time>}`.** `newest` returns the last transition with `at < hi` (`ORDER BY at DESC, id DESC`); after the `at_ms` clamp, `at` ties cannot occur and `id` remains the true order. `new = zero` (a deletion) resolves to no ref, as git does. Epoch precision is explicit: `refs/at/T/...` means the last move inside second `T`, and any digit prefix — `refs/at/1735` — scopes an enumeration to that window.
- **The `want-ref` blocker dies with the capability list.** Rule 6 advertises neither `ref-in-want` nor `sideband-all`, and `parse_fetch` rejects `want-ref` as an unknown argument (protocol-v2-only). git therefore resolves the sha at `ls-refs` time and sends `want <sha>`; `send_set` step 1 accepts any live oid, so no `wanted-refs` section is ever needed.
- **Advertised implies fetchable.** `gc_roots` (write-back c) makes every reflog `new` a `GcMark` root, so `GcSweep` cannot kill an object while a row can still resolve to it — and the sweep is anyway `gc_epoch`-guarded against concurrent pushes (section 5). `ReflogExpire` keeps the newest row per name, so "resolvable" and "rooted" expire together rather than drifting apart.
- **One alarm, one slice.** `ReflogExpire` is a `jobs` row run by `dispatch` (section 4): the `DELETE` and the re-`enqueue` are sync inside the slice, `jobs::rearm` performs the only `set_alarm` after the span (4.1, 4.2, A3), and `boot` re-enqueues it whenever absent (A4). The first pass's private `setAlarm(+24h)` — which would have cancelled the GC alarm (#5) — is gone.
- **Namespace integrity.** `reserved()` (write-back b) gives `ng <name> refs/at/ is read-only` to any command under `refs/at`, so a real ref can never shadow a synthetic row or be silently merged into the namespace; `delete-refs` cannot delete what was never in `refs`.
- **Cost is one indexed read.** An exact lookup is one `reflog_name_at` seek; enumeration is one `LIMIT 1,000` scan; both ride inside the `/_do/ls-refs` stub call the fetch already makes — zero added subrequests (7.3) and no awaits inside the span. At most 4 bound parameters per statement, far under the A6 bound of 100.

## Changes from the first pass
| First-pass finding (quoted) | Kind | How addressed |
|---|---|---|
| "`want-ref`/`ref-in-want` path unhandled if the base advertises that capability (it is the common default in a v2 server implementation)." | blocker | Closed by the contract, not new code: `ref-in-want` is not advertised (1.1 rule 6, section 12) and `parse_fetch` rejects `want-ref`. The client resolves the sha in `ls-refs` and sends `want <sha>`; `send_set` step 1 serves any live oid. |
| "Single DO alarm must be shared with gc-and-repack-alarm; proof's `setAlarm` would overwrite it." | blocker | `ReflogExpire` is a `JobKind` on the shared dispatcher (section 4): `run_slice` is one sync `DELETE` plus a sync `enqueue`; `jobs::rearm` is the sole `set_alarm` caller (4.1, A3; measured #5). Registered in the REGISTRY block (A9). |
| "Timestamps are server receive time; no monotonic clamp yet, so a DO restart with clock skew can make `ts<=?` pick the wrong row (seq ordering only helps within equal ts)." | caveat | `at_ms()`: `apply_one` stamps `at = max(now_ms(), MAX(at)+1)` inside the commit span, strictly increasing per repo regardless of clock skew; uniqueness of `at` also makes every enumeration refname unique. |
| "Bare `refs/at/` enumeration is unbounded (tens of thousands of lines for a hot repo over 90 days); cap or require an epoch prefix." | caveat | `ENUM_MAX = 1,000`, newest first; `refs/at/<epoch>` — including a partial digit prefix like `refs/at/1735` — scopes the scan through the `[lo, hi)` window. |
| "Expiry deletes rows and GC then drops objects; advertisements older than retention become dangling until the row is gone. Force-push history is the case users will actually want and the one most likely to be reaped." | caveat | `gc_roots()` seeds `GcMark` with every reflog tip, so no resolvable row can dangle; expiry keeps the newest row per name, so root and resolution expire together. Residual advertise-to-fetch race in Known limits. |
| "Byte-length pkt-line bug, same-second duplicate refnames, branch->tag fallback ambiguity." | caveat | pkt-lines are `gix-packetline` byte lengths (rule 1) — no UTF-16 `s.length`; the `at` clamp prevents same-second rows entirely; short names resolve in gitrevisions order (`refs/tags/` first) and enumeration advertises full names (`refs/at/<s>/refs/heads/main`) that re-resolve verbatim. |
| "`resolveAt` fallback tries `refs/heads/x` then `refs/tags/x` then raw `x`: a branch deleted at T silently resolves to a tag of the same name." | caveat | Resolution order is now git's own (`refs/tags/` before `refs/heads/`, verbatim `refs/...` first); a `refs/at/<t>/x` that means "the branch x" is expressible unambiguously as `refs/at/<t>/refs/heads/x`. Residual ambiguity for bare short names is documented. |
| "ls-refs `symrefs`/`peel`/`unborn` args are ignored here; ... `peel` on a time-travel ref pointing at an annotated tag will not emit `peeled:`." | caveat | `symrefs`/`unborn` are the base handler's (`write_ls_refs`, spike-tested). `peel` stays `None`: peeling needs the tag object from R2 — an await — and `/_do/ls-refs` is a sync span. Not addressed; in Known limits. |
| "Any edge KV ref replica must proxy `refs/at/` to the DO." | caveat | No replica exists: every `ls-refs` is `/_do/ls-refs` on the authority DO, and replicated refs are out of scope (section 12). |
| First-pass limit: "users expecting 'the commit as of the author date' get the wrong answer around slow or delayed pushes" | limit | Semantics kept: `reflog.at` is server receive time, the same choice git makes for `@{...}`; stated in Mechanism. |
| First-pass limit: "No `HEAD` symref resolution through time (`refs/at/<t>/HEAD`)" | limit | Kept: HEAD is the `meta.head` symbolic row (section 3) and has no reflog stream; `refs/at/<t>/HEAD` resolves to nothing. |
| First-pass limit: "every `ls-refs` for a time-travel ref costs one DO request in addition to the fetch" | limit | Now zero: resolution rides inside the `/_do/ls-refs` span the fetch already pays for. |

## Known limits
- Write-backs proposed to CONTRACTS.md, each small: `ls_refs` calls `rows()` and appends the synthetic RefRows before `write_ls_refs` filters; `apply_one` calls `reserved()` first and stamps `reflog.at` with `at_ms()`; `GcMark` unions `gc_roots()` into its frontier seed; `boot` enqueues `ReflogExpire` whenever absent (A4); `JobKind::ReflogExpire` and `CREATE INDEX reflog_name_at ON reflog(name, at, id)` are registered (A9). Without (c), `refs/at/` answers can dangle after a sweep — the idea is then honestly weaker, advertised-but-maybe-gone.
- `peel` on a synthetic ref emits no `peeled:` line: peeling needs the tag object (an R2 await) and `/_do/ls-refs` is a sync span. git treats a missing `peeled:` as absent annotation info, not an error.
- Stale-advertisement residual: between `ls-refs` and the `fetch` that follows, the row could expire and a full GC could complete — but that needs `GcMark` -> `GcConsolidate` -> `GcSweep` (a >= 10-minute chain, section 5) inside a window of seconds; a `want` that does fail is `ERR upload-pack: not our ref <oid>` and the client retries with a fresh `ls-refs`.
- Retention is a fixed 90 days (`KEEP_MS`); per-repo policy is out of scope (section 12). Expiry can make an old `refs/at/<t>` resolve to an older surviving transition — the same answer git gives after `git reflog expire`.
- `refs/at/` rows are hidden from unprefixed `ls-remote` output by design (only prefixes under `refs/at` synthesize rows); enumeration advertises full names, so `git fetch origin 'refs/at/*:refs/remotes/at/*'` produces deep remote names like `refs/remotes/at/<s>/refs/heads/main` — cosmetic.
- A bare short name that exists under both `refs/tags/` and `refs/heads/` resolves to the tag — git's own order, but worth documenting since the branch is usually meant.
- Scenarios this proof must pass: 3, 12, 13 (regression: ordinary ls-refs/fetch/v0-refusal unchanged). Added (two): (a) "time-travel fetch": push tip A, force-push tip B, then `git fetch origin refs/at/<T>/refs/heads/main` with T between the pushes; assert the fetched sha is A and `git ls-remote origin 'refs/at/*'` shows both moves with unique names. (b) "reflog expiry": advance the fake clock past `KEEP_MS`, fire the alarm until `ReflogExpire` has run; assert only the newest row per name survives, an old `refs/at/<t>` resolves to the surviving transition or nothing, and the expired tips are no longer `gc_roots`.

## Depends on
- repo-do-ref-authority
- protocol-v2-only
- refs-sqlite-objects-r2
- gc-and-repack-alarm
