# Second-pass review: Tiny in-DO object cache with alarm-driven eviction

# Review v2: in-do-object-cache (idea #9)

## Scores
- Feasibility: 4/5 (was 4). Checked against pinned sources, not the memo: `impl From<Vec<u8>> for SqlStorageValue` -> `Blob` (worker 0.8.5 `sql.rs:88`), so the BLOB binding the proof marks unverified is real; `to_array` goes through `serde_wasm_bindgen::from_value`, whose `deserialize_seq` accepts a `Uint8Array` via `js_sys::try_iter` (0.6.5 `de.rs:517`), so `Row { entry: Vec<u8> }` reads back correctly; `gix_hash::hasher(Kind)`, `Hasher::update`, `Hasher::try_finalize` exist (0.26.2 `hasher.rs:95/48/64`); `blocking_io::encode::band_to_write` exists in gix-packetline 0.22.2. Point off: the Cloudflare DO SQLite limits page says **100 bound parameters per query**; the sweep binds 501 and 500. Not in the memo, verified today.
- Reliability: 3/5 (was 4). Nothing can lose or corrupt data (rows are immutable copies of live-pack bytes, resolution still goes through 2.3). But the eviction job, the idea's headline mechanism, throws on its first firing (below), is retried 8 times by 4.4 and goes `dead`; the cache then grows to `2 * SQL_BUDGET` and stops admitting, forever, per repo. The OOM path of the first pass is genuinely gone.
- Correctness: 4/5 (was 2). The emitted pack is byte-correct by construction under 2.1; no wire byte breaks git 2.4x. Point off: no `entries.len() == count` guard before the header is written.
- Effort: days (about 150 lines on top of a finished foundation; three one-line fixes below).
Genuinely better than v1: both first-pass blockers are closed by shown code, and four of the proof's five "unverified" API names are now verified in source.

## Contract compliance
- Read path: `cache_lookup` takes only `(ObjectId, ObjLoc)` pairs that `Index::lookup` (2.3 reader query, live packs only) produced; no second resolver. Pass.
- Bytes: `entry` is the `read_entries` slice `[offset, offset+len)`, checked by `codec::entry_header` against `ObjLoc.kind/size` (1.2, sync, no `worker` import). Pass.
- CAS oracle: `changed(&sql)` = `SELECT changes()` after `INSERT OR IGNORE`; `rows_written` absent. Pass.
- Sync spans: `cache_lookup` and `cache_admit` are `fn`; `read_entries_cached` awaits exactly once between them; `RefCell` borrows are block-scoped, none across the await; the sweep awaits nothing. Pass.
- Alarm: no `set_alarm`; `jobs::enqueue` once (empty -> non-empty), `SliceOutcome::Reschedule` otherwise; new `JobKind::CacheSweep` + `run_slice` arm as 4.5 requires. Pass on the letter; inherits the contract's own `enqueue`-is-sync vs `rearm`-is-async gap (flagged by the proof).
- Rule 4.3 says a slice at 80% budget returns `Continue`; the sweep returns `Reschedule { now + 15 min }`. Deviation: an over-budget trim waits 15 min instead of the next firing. Violation (minor).
- Section 1 signatures: `Bucket::read_entries(&[(ObjectId, ObjLoc)], &mut ReqBudget)` deviates from 1.2 (`&[ObjLoc]`, no budget) while matching section 9; `SliceBudget::spent_80pct`, `Sideband::new`, `Index::entries_of`, `SendSet::count` are not in section 1; `use crate::{ReqBudget, RepoDo}` needs re-exports section 1 does not define. Violations (documented, need write-back).
- Section 12 names this cache out of scope; the proof correctly positions itself as post-foundation and lists the three write-backs (`cache` field, `schema_version = 3`, `JobKind::CacheSweep`). Pass.
- Error policy: `Error::Storage` on header/index disagreement, `Error::Internal` on `u32` overflow; no `unwrap`/indexing on R2 or client bytes. `as i64` is in `repo_do`, outside the lint list. Pass.
- Janitor rules: no R2 deletion anywhere in the module. Pass.

## First-pass blockers
1. "Body-format contract mismatch ... cannot produce a valid pack": **resolved by construction.** No `customMetadata` read, no `packHeader`/`concat`; `cache_admit` stores the `read_entries` bytes and `let (kind, size, _) = codec::entry_header(entry)?; if kind != loc.kind || size != loc.size { return Err(...) }` ties them to the typed index row; `write_small_pack` splices them under `PACK`/2/count and a `gix_hash` trailer. The loose-header and NaN-varint failure modes no longer have a code path.
2. "Unbounded in-memory tier ... can OOM the repo DO": **resolved.** `MemTier::insert`: `if entry.len() > MEM_BUDGET / 8 { return; }` and `if self.bytes.saturating_add(entry.len()) > MEM_BUDGET { self.map.clear(); self.bytes = 0; }`; `hits` capped at `HITS_CAP`; SQLite admission stops at `2 * SQL_BUDGET`; sets above 256 entries / 4 MiB bypass the cache (`Ok(false)`). Heap bound is 8 MiB + 4 MiB pack + section 9's own budget.

## Crash walk-through
Lazy `want <blob>` fetch, 40 entries, 30 misses. `cache_lookup` (no SQL writes) -> `read_entries` await -> `cache_admit` inserts 20 rows, then the isolate dies (panic, `--panic-unwind`) before the meta write. The span's writes are uncommitted and roll back together (section 3 atomicity; PanicError rollback is contract-unverified, scenario 15), so rows and `objcache.bytes` stay consistent; the client retries, misses, re-admits. If the crash lands after the span but before the response flushes, rows are committed and the retry hits: also correct, because rows are pure copies of immutable live-pack bytes. Alarm crash mid-sweep: one span, so all `DELETE`s and the total roll back together; 4.4 retries with backoff; only the drained `hits` map is lost (LRU promotion delayed one interval, cache-quality only). No orphan, no ref impact.

## Concurrency walk-through
A misses {X, Y}, B misses {X, Z}, both awaiting R2. A's `cache_admit`: `before = 0`, `INSERT OR IGNORE` X, Y -> `changed()` true twice, total = |X|+|Y|, meta written, `CacheSweep` enqueued (dedup by kind). B's span: `before = |X|+|Y|`, X -> `changed()` false, not counted; Z counted; total exact; `enqueue` skipped. A sweep cannot interleave because it has no await. Only imprecision: B's `mem.insert(X)` re-adds |X| to `MemTier.bytes` although X is already resident, so the generational clear fires early, never late. Safe direction. Exactly the section 3 pattern (CAS after the network await), measured in platform-facts #4.

## Interop check
git 2.43-2.47 lazy fetch runs with `fetch.negotiationAlgorithm=noop`: `want <blob>` lines and `done`, so per rule 3 the reply is `packfile\n`, band-1 frames <= 65515 bytes, flush; v2 always multiplexes the pack, so `Sideband::data` is right without a `side-band-64k` check. Pack: `PACK`, `00000002`, big-endian count, full-object entries (`varint(kind,size)+zlib`) in any order, SHA-1 trailer over the preceding bytes: `index-pack` accepts it and `fsck` is clean. The one byte that could break: bytes 8-11 (count) if `entries.len() != set.count()` (a short `read_entries` result); the client would print `fatal: pack has N unresolved deltas`/`premature end of pack file`. No guard is written; a one-line `if entries.len() != count as usize { return Err(Error::Storage(..)) }` closes it. No other wire byte at risk.

## Blockers
1. **The sweep exceeds the DO SQLite bound-parameter limit.** Cloudflare's Durable Objects limits page: "Maximum bound parameters per query: 100". `UPDATE objcache SET last_hit=? WHERE sha IN (500 marks)` binds 501; `DELETE FROM objcache WHERE sha IN (...)` binds 500. Every firing throws -> `attempts` to 8 -> `dead` (4.4). Eviction never runs; the table sits at 128 MiB and admission stops. Fix: chunk at 90, or `DELETE FROM objcache WHERE sha IN (SELECT sha FROM objcache WHERE last_hit < ? ORDER BY last_hit, sha LIMIT 500)` after a `SELECT SUM(len)` with the same subquery, in the same span. Note for the contract: `/_do/push/lookup` "<= 1,000 ids" and `insert_objects` "<= 10,000 rows" hit the same cap unless written as multi-row `VALUES`/`json_each`.
2. Contract write-backs the code cannot compile without: 1.2 `read_entries` signature, `Sideband::new`, `Index::entries_of`, `SendSet::count`, `SliceBudget::spent_80pct`, `JobKind::CacheSweep`, `RepoDo.cache`, `schema_version = 3`.

## Caveats
- `Vec<u8>` BLOB readback iterates the `Uint8Array` one JS call per byte (`try_iter` path): correct, but a 256 KiB hit costs ~262k host calls. Use `SqlCursor::raw()` (`SqlStorageValue::Blob`) or `serde_bytes`; day-1 measurement.
- Rule 4.3 deviation (`Reschedule` instead of `Continue` at 80%) delays a large trim by 15 min; bounded by the admission cap.
- The sweep never returns `Done`, so an empty cache still wakes the DO every 15 min forever; return `Done` when `total == 0` and let the next admit re-enqueue.
- `cache_lookup` is one `SELECT` per sha; a 5,000-tree negotiation round is 5,000 sync queries (fine at ~20 us each, but `IN` batches of 90 would be cleaner).
- Inherited, contract-level: sync `enqueue` vs async `rearm`; PanicError rollback (scenario 15); real R2 range reads (#6) and subrequest cap (#7) unmeasured.
- "package.json/lockfile" prewarming is still not delivered; admission is by sha/size/recency only, as the proof states.

## Verdict
risky. The read path is now correct by construction against CONTRACTS 2.1/2.3, both first-pass blockers are closed by shown code, and every API it relies on exists in the pinned crates. But the eviction slice as written dies on its first alarm because of the 100-bind limit, so "alarm-driven eviction" does not run until a one-line rewrite lands; with that fix and the listed write-backs it lands in days.
