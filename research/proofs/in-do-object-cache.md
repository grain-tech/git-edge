# Tiny in-DO object cache with alarm-driven eviction

> Second pass · Idea #9 · verdict: **risky** · feasibility 4/5 · reliability 3/5 · correctness 4/5 (first pass 4/4/2)
> First pass: [proof](./archived/in-do-object-cache.md) · [review](../reviews/archived/in-do-object-cache.md) · Second pass: [review](../reviews/in-do-object-cache.md) · Contract: [CONTRACTS.md](../../CONTRACTS.md)


## Mechanism
CONTRACTS.md section 12 names the in-DO object cache as out of scope for the foundation, so this is not subsumed: it is a post-foundation module, `repo_do/cache.rs` plus one `jobs` arm, that sits *behind* the contract's read path and changes nothing about how a sha is resolved. Every read still starts with the section 2.3 reader query (`Index::lookup`, live packs only); only the locations that query returns are then checked against an in-isolate `MemTier` (hard-capped at 8 MiB) and a new `objcache` SQLite table in `RepoDo` before `Bucket::read_entries` is called for the misses (7.2). What is cached is the pack entry byte-for-byte as it lies in the normalized pack (2.1: `varint(kind,size) + zlib(data)`), which is exactly what `pack::generate` copies into an outgoing pack, so a hit is spliced with no inflate, no recompress and no header rewrite. Eviction is a recurring `JobKind::CacheSweep` slice through the section 4 dispatcher (TTL, then LRU to a byte budget kept as a running total in `meta`); no code here calls `set_alarm`.

## Primitives
- DO SQLite through `worker::SqlStorage::exec` (sync) with `SqlCursor::to_array`: supported in `worker` 0.8.5 (memo section 1), exercised on workerd in the spike. BLOB column binding (`SqlStorageValue` from `Vec<u8>`) and BLOB-to-`Vec<u8>` deserialisation in `to_array`: **unverified**.
- `SELECT changes()` after `INSERT OR IGNORE` to keep the byte total exact under concurrent admissions: **measured** correct in every case (platform-facts #1); `rows_written` is never read.
- One sync span between awaits: **measured** (platform-facts #4); `cache_lookup`, `cache_admit` and the whole sweep slice are each one span.
- Jobs dispatcher (section 4): `JobKind::CacheSweep` as a new variant, `run_slice` arm, `SliceOutcome::Reschedule`, `enqueue` dedup by kind. A second `set_alarm` cancels the first, **measured** (#5), which is why only `jobs::rearm` sets it.
- `Bucket::read_entries` (coalesced range reads, 7.2) charged through `ReqBudget` (7.1); real R2 range reads verified on the **local simulator only** (#6); the DO subrequest limit is **not enforced locally** (#7).
- `store::codec::entry_header` (1.2, sync, no `worker` import) to check an admitted entry's header against its index row; the varint header format is what `gix_pack::data::entry::Header` parsed in the spike.
- `gix_hash::hasher(Kind::Sha1)` -> `Hasher::{update, try_finalize}` for the small-pack trailer: gix-hash 0.26.2 is CI-built for wasm32 and `compute_hash` ran on workerd in the spike; the streaming `Hasher` constructor name is **unverified**.
- `wire::{PktWriter, Sideband}` (1.1): band-1 frames of at most 65515 bytes (rule 2); `band_to_write` **not exercised by the spike**.
- `RefCell<MemTier>` inside `RepoDo` because `DurableObject::fetch` takes `&self` (memo risk 5): plain Rust, no host API; borrows are scoped to sync helpers.
- `js_sys::Date::now()` for timestamps: synchronous host call, allowed in a sync span.
- git wire facts used: a pack whose entries are all full objects is valid in any entry order; `index-pack` accepts any zlib stream that inflates to `size` bytes; the v2 `fetch` response after `packfile\n` is sideband frames then flush (rule 3). Checked against gitprotocol-pack/v2 and the first-pass interop walk-through, not yet by scenario run.

## Proof code
```rust
// src/repo_do/cache.rs + one jobs::run_slice arm -- CONTRACTS.md 1.2, 1.3, 1.4, 2.1, 2.3, 4, 7, 9, 10
use std::{cell::RefCell, collections::HashMap};
use gix_hash::ObjectId;
use serde::Deserialize;
use worker::SqlStorageValue;
use crate::{error::Error, jobs::{self, Job, JobKind, SliceBudget, SliceOutcome}, pack::generate::SendSet,
            store::{codec, Bucket, Index, ObjLoc}, wire::{PktWriter, Sideband}, ReqBudget, RepoDo};
// exec() / meta_i64() / changed() (the SELECT changes() oracle) as in the refs-sqlite-objects-r2 proof.
// Schema, added at schema_version 3 by RepoDo::boot (8.2). `entry` is the pack entry exactly as it lies in a live
// pack, varint(kind,size) + zlib(data) (2.1): the bytes Bucket::read_entries returns for [offset, offset+len).
//   CREATE TABLE objcache (sha TEXT PRIMARY KEY, len INTEGER NOT NULL, entry BLOB NOT NULL, last_hit INTEGER NOT NULL) WITHOUT ROWID;
//   CREATE INDEX objcache_lru ON objcache(last_hit, sha);
//   meta row 'objcache.bytes': running SUM(len), maintained by cache_admit and the sweep; no sweep ever scans the table.
pub const MAX_ENTRY: u32 = 256 << 10;      // per-entry admission cap (SQLite value cap is 2 MB)
pub const SQL_BUDGET: i64 = 64 << 20;      // SQLite tier size the sweep trims to; admission stops at 2x
pub const MEM_BUDGET: usize = 8 << 20;     // in-isolate tier, hard cap enforced on every insert
pub const TTL_MS: i64 = 6 * 3_600_000;
pub const SWEEP_MS: i64 = 15 * 60_000;
const SMALL_ENTRIES: u32 = 256;            // below these, fetch_v2 builds the pack in memory (lazy blob fetches)
const SMALL_BYTES: u64 = 4 << 20;
const HITS_CAP: usize = 10_000;

/// In-isolate tier: `RepoDo { .., cache: RefCell<MemTier> }`. Borrowed only inside sync fns, never across an await.
#[derive(Default)]
pub struct MemTier { map: HashMap<ObjectId, Vec<u8>>, bytes: usize, hits: HashMap<ObjectId, i64> }
impl MemTier {
    fn insert(&mut self, id: ObjectId, entry: Vec<u8>) {
        if entry.len() > MEM_BUDGET / 8 { return; }
        if self.bytes.saturating_add(entry.len()) > MEM_BUDGET { self.map.clear(); self.bytes = 0; }  // generational; refills from SQLite
        self.bytes = self.bytes.saturating_add(entry.len());
        self.map.insert(id, entry);
    }
    fn hit(&mut self, id: ObjectId, now: i64) { if self.hits.len() < HITS_CAP { self.hits.insert(id, now); } }
}
#[derive(Deserialize)] struct Row { entry: Vec<u8> }
#[derive(Deserialize)] struct Victim { sha: String, len: i64 }

impl RepoDo {
    /// Sync. `locs` came from Index::lookup, so every id is already known to be in a live pack (2.3 stays the only
    /// way a sha is resolved; a swept object misses there and never reaches the cache). Returns hits and misses.
    fn cache_lookup(&self, locs: &[(ObjectId, ObjLoc)]) -> Result<(Vec<(ObjectId, Vec<u8>)>, Vec<(ObjectId, ObjLoc)>), Error> {
        let (sql, now) = (self.state.storage().sql(), js_sys::Date::now() as i64);
        let mut mem = self.cache.borrow_mut();
        let (mut hits, mut misses) = (Vec::new(), Vec::new());
        for (id, loc) in locs {
            if let Some(e) = mem.map.get(id) { hits.push((*id, e.clone())); mem.hit(*id, now); continue; }
            if loc.len > MAX_ENTRY { misses.push((*id, loc.clone())); continue; }         // never admitted: skip the query
            match exec(&sql, "SELECT entry FROM objcache WHERE sha=?", vec![id.to_string().into()])?.to_array::<Row>()?.pop() {
                Some(r) => { mem.insert(*id, r.entry.clone()); mem.hit(*id, now); hits.push((*id, r.entry)); }
                None => misses.push((*id, loc.clone())),
            }
        }
        Ok((hits, misses))
    }
    /// Sync, a fresh span after the R2 await. Rows are immutable copies keyed by sha, so a concurrent request that
    /// admitted the same sha meanwhile is harmless: INSERT OR IGNORE, and changes() decides whether the byte total moves.
    fn cache_admit(&self, got: &[(ObjectId, Vec<u8>)], locs: &[(ObjectId, ObjLoc)]) -> Result<(), Error> {
        let (sql, now) = (self.state.storage().sql(), js_sys::Date::now() as i64);
        let before = self.meta_i64("objcache.bytes")?;
        let mut total = before;
        let mut mem = self.cache.borrow_mut();
        for (id, entry) in got {
            let Some((_, loc)) = locs.iter().find(|(i, _)| i == id) else { continue };
            let len = u32::try_from(entry.len()).map_err(|_| Error::Internal("entry len".into()))?;
            if len != loc.len || len > MAX_ENTRY || total.saturating_add(i64::from(len)) > 2 * SQL_BUDGET { continue; }
            let (kind, size, _) = codec::entry_header(entry)?;                            // header must agree with the index row
            if kind != loc.kind || size != loc.size { return Err(Error::Storage(format!("entry {id} disagrees with objects row"))); }
            exec(&sql, "INSERT OR IGNORE INTO objcache(sha, len, entry, last_hit) VALUES(?,?,?,?)",
                 vec![id.to_string().into(), i64::from(len).into(), entry.clone().into(), now.into()])?;   // BLOB binding: unverified
            if changed(&sql)? { total = total.saturating_add(i64::from(len)); }
            mem.insert(*id, entry.clone());
        }
        exec(&sql, "INSERT OR REPLACE INTO meta(key, value) VALUES('objcache.bytes', ?)", vec![total.to_string().into()])?;
        if before == 0 && total > 0 { jobs::enqueue(&sql, JobKind::CacheSweep, now + SWEEP_MS, "{}")?; }   // 4.1/4.5: dedups, never set_alarm
        Ok(())
    }
    /// The section 9 read step with the cache in front. A hit costs zero subrequests; misses are one coalesced
    /// read_entries call (7.2, charged per 7.1). Used by the tree/tag rounds of fetch_v2 and by write_small_pack.
    pub async fn read_entries_cached(&self, bucket: &Bucket, locs: &[(ObjectId, ObjLoc)], budget: &mut ReqBudget)
        -> Result<Vec<(ObjectId, Vec<u8>)>, Error> {
        let (mut out, misses) = self.cache_lookup(locs)?;              // sync span ends here
        if misses.is_empty() { return Ok(out); }
        let got = bucket.read_entries(&misses, budget).await?;          // the only await in this module
        self.cache_admit(&got, &misses)?;                               // new sync span
        out.extend(got);
        Ok(out)
    }
    /// Section 9 step 6 for a small SendSet (a `want <blob>` follow-up after --filter=blob:none, scenario 10). `w` already
    /// holds everything up to and including `packfile\n`. Ok(false) without touching storage when the set is not small;
    /// the caller then streams through write_pack, so bulk clones bypass the cache. Entry order is free: no deltas (2.1).
    pub async fn write_small_pack(&self, bucket: &Bucket, set: &SendSet, w: &mut PktWriter, budget: &mut ReqBudget) -> Result<bool, Error> {
        let count = set.count();                                          // popcount over the bitmaps, sync
        if count > SMALL_ENTRIES { return Ok(false); }
        let locs = Index(&self.state.storage().sql()).entries_of(set)?;   // SELECT sha,idx,offset,len,kind,size FROM objects WHERE pack_id=? AND idx IN (..)
        if locs.iter().map(|(_, l)| u64::from(l.len)).sum::<u64>() > SMALL_BYTES { return Ok(false); }
        let entries = self.read_entries_cached(bucket, &locs, budget).await?;
        let mut pack = Vec::with_capacity(12 + entries.iter().map(|(_, e)| e.len()).sum::<usize>() + 20);
        pack.extend_from_slice(b"PACK"); pack.extend_from_slice(&2u32.to_be_bytes()); pack.extend_from_slice(&count.to_be_bytes());
        for (_, e) in &entries { pack.extend_from_slice(e); }
        let mut h = gix_hash::hasher(gix_hash::Kind::Sha1);              // Hasher constructor name unverified in the spike
        h.update(&pack);
        let trailer = h.try_finalize().map_err(|e| Error::Internal(e.to_string()))?;
        pack.extend_from_slice(trailer.as_slice());
        Sideband::new(w).data(&pack);                                     // band 1, frames <= 65515 bytes (rule 2)
        w.flush();                                                        // rule 3: flush ends the response
        Ok(true)
    }
}

/// jobs::run_slice arm for JobKind::CacheSweep. No I/O at all: the slice is one sync span, so no admission can
/// interleave with an eviction. Rescheduled (4.2) rather than re-enqueued: the row stays, dedup by kind holds.
pub async fn run_cache_sweep(d: &RepoDo, _job: &Job, budget: &mut SliceBudget) -> Result<SliceOutcome, Error> {
    let (sql, now) = (d.state.storage().sql(), js_sys::Date::now() as i64);
    let hot: Vec<ObjectId> = { let mut m = d.cache.borrow_mut(); m.hits.drain().map(|(id, _)| id).collect() };
    for chunk in hot.chunks(500) {                                        // one UPDATE per 500 hot rows, not one per row
        let marks = vec!["?"; chunk.len()].join(",");
        let mut args: Vec<SqlStorageValue> = vec![now.into()];
        args.extend(chunk.iter().map(|id| id.to_string().into()));
        exec(&sql, &format!("UPDATE objcache SET last_hit=? WHERE sha IN ({marks})"), args)?;
    }
    let mut total = d.meta_i64("objcache.bytes")?;
    loop {                                                                // expired rows first, then oldest until under budget
        let over = total > SQL_BUDGET;
        let (q, args): (&str, Vec<SqlStorageValue>) = if over {
            ("SELECT sha, len FROM objcache ORDER BY last_hit, sha LIMIT 500", vec![])
        } else {
            ("SELECT sha, len FROM objcache WHERE last_hit < ? ORDER BY last_hit, sha LIMIT 500", vec![(now - TTL_MS).into()])
        };
        let victims = exec(&sql, q, args)?.to_array::<Victim>()?;
        if victims.is_empty() { break; }
        let marks = vec!["?"; victims.len()].join(",");
        exec(&sql, &format!("DELETE FROM objcache WHERE sha IN ({marks})"), victims.iter().map(|v| v.sha.clone().into()).collect())?;
        total = total.saturating_sub(victims.iter().map(|v| v.len).sum::<i64>()).max(0);
        if budget.spent_80pct() { break; }                                // 4.3; the rest waits for the next sweep
    }
    exec(&sql, "INSERT OR REPLACE INTO meta(key, value) VALUES('objcache.bytes', ?)", vec![total.to_string().into()])?;
    Ok(SliceOutcome::Reschedule { run_at: now + SWEEP_MS })
}
```

## Why it works
- **The cached bytes are the pack bytes.** Section 2.1 makes every object at rest a full pack entry, `varint(kind,size) + zlib(data)`, and `Bucket::read_entries` returns exactly `[offset, offset+len)` of that pack. `cache_admit` stores those bytes unchanged after `codec::entry_header` confirms the varint agrees with the `objects` row, and `write_small_pack` concatenates them under a `PACK`/2/count header with a fresh SHA-1 trailer. That is byte-for-byte what `write_pack` would have produced for the same entries (section 9 step 6), so `index-pack` reads it: every entry is a full object, any order is legal, and the trailer covers what was actually written.
- **Resolution never bypasses the index.** `cache_lookup` takes `(ObjectId, ObjLoc)` pairs that `Index::lookup` already resolved (2.3, "the only way any code resolves a sha"). An object whose pack was swept (5.3) misses in `lookup` and the cache is never asked; an object in two live packs (2.3) is served from whichever bytes were admitted first, both valid. Cache rows therefore need no invalidation on GC, on push, or on rename (8.4: keys are by `repo_id`, the cache is by sha inside the same DO).
- **Both tiers are bounded, at insert time, not only at sweep time.** `MemTier::insert` refuses entries above 1 MiB and clears the map when 8 MiB would be exceeded; `hits` stops recording at 10,000 keys; `cache_admit` refuses entries above 256 KiB (SQLite's 2 MB value cap) and stops admitting when `objcache.bytes` would pass 2 x 64 MiB. A `git checkout` after `--filter=blob:none` that streams 10,000 blobs through the DO thus holds at most 8 MiB in the heap plus the 4 MiB small-pack buffer, whatever the sweep timing.
- **Every storage-touching step is a sync span.** `cache_lookup` and `cache_admit` do not await; `read_entries_cached` awaits exactly once, between them, and holds no `RefCell` borrow across it, so another request's span can run there safely (platform-facts #4). Admission after the await is safe precisely because rows are immutable content-addressed copies: a duplicate `INSERT OR IGNORE` is a no-op and `changes()` (section 3's oracle, measured #1) tells whether the byte total moves. The sweep slice awaits nothing at all, so no admission ever interleaves with an eviction.
- **The alarm rule is respected.** The module calls `jobs::enqueue` once, when the table goes from empty to non-empty (dedup by kind, 4.5), and the sweep returns `Reschedule` (4.2) so its row persists; `set_alarm` is only ever called by `jobs::rearm` (4.1, measured #5). A crash mid-sweep is retried by 4.4 with the deletes already committed atomically per span; a lost `hits` map only delays LRU promotion.
- **Subrequests and CPU.** A hit costs zero R2 calls; a small-pack miss costs one coalesced `read_entries` (7.2) charged to `ReqBudget` (7.1). The only CPU on the hot path is `gix_hash` over at most 4 MiB, well under the 300 s `limits.cpu_ms` (7). Negotiation rounds that load trees and tags (section 9 steps 3 and 5) can call `read_entries_cached` in place of `read_entries` with no other change, because the loop's contract is "async loads bytes into MemFind, sync computes".
- Conformance scenarios this proof must pass (section 11): 10 (`blob:none` clone, the checkout's `want <blob>` fetches take `write_small_pack`; `fsck` clean), 12 (a 50-commit incremental fetch exceeds `SMALL_ENTRIES` and takes `write_pack`), 14 (after GC, a cached sha whose only pack died is not served: `lookup` misses first). Added scenario 16: run scenario 10 twice against the same repo with fresh clones; assert the second checkout's fetch responses carry `x-ge-subrequests: 0` and `fsck` is clean; then advance the fake clock past `TTL_MS`, fire the alarm, and assert `objcache` is empty and `meta.objcache.bytes` is `0`.

## Changes from the first pass
| First-pass finding (quoted) | How addressed |
|---|---|
| Blocker 1: "Body-format contract mismatch with all three sibling proofs (zlib vs uncompressed, `kind` numeric vs `type` string, `size` present vs absent). The proof as written cannot produce a valid pack" | Gone by construction. There are no loose objects and no `customMetadata` reads (2.1, 2.2: metadata is informational, no code path reads it). `cache_admit` stores the `read_entries` bytes verbatim and checks them with `codec::entry_header` against the typed `ObjLoc { kind: Kind, size: u64 }` row; `write_small_pack` splices them under a `PACK` header. No `packHeader`, no `concat`, no `Number(...)`. |
| Blocker 2: "Unbounded in-memory tier between sweeps; a single lazy checkout can OOM the repo DO" | `MemTier::insert`: per-entry cap `MEM_BUDGET / 8`, generational clear at `MEM_BUDGET` (8 MiB); `hits` capped at `HITS_CAP`; `cache_admit` refuses admission past `2 * SQL_BUDGET`. The SQLite tier is the real cache, exactly as the first pass claimed but did not enforce. |
| Caveat: "One alarm per DO is shared with `gc-and-repack-alarm`, `alarm-chain-ci`, `ephemeral-repos`; the proof admits the scheduler is hand-waved" | The scheduler is section 4. `JobKind::CacheSweep` + `run_cache_sweep`; `jobs::enqueue` in `cache_admit`, `SliceOutcome::Reschedule` at the end of the sweep; no `set_alarm` here (4.1, CI grep). |
| Caveat: "`DELETE ... last_hit <= cutoff` can over-evict rows sharing the boundary millisecond" | Victims are selected by `ORDER BY last_hit, sha LIMIT 500` and deleted by explicit sha list (`run_cache_sweep`, `DELETE ... WHERE sha IN`); no timestamp cutoff. |
| Caveat: "`hits` flush is one `UPDATE` per hot oid per sweep; thousands of writes every 5 minutes" | One `UPDATE ... WHERE sha IN (500 marks)` per 500 hot rows with a shared timestamp; LRU is at sweep granularity, which is all the trim needs. Sweep interval 15 min. |
| Caveat: "Does not know filenames; 'package.json/lockfile' prewarming is not delivered, only 'small oids read twice'" | Not addressed because the contract's read path (2.3, section 9) resolves by sha only; a path-aware prewarm would be a tree walk on push in a new job kind. Admission by sha, size and recency is what the code does; the "package.json" framing of the idea is delivered only insofar as those blobs are the ones a `--filter=blob:none` checkout re-requests. |
| Caveat: "No read parallelism: every cached object still serializes through one DO" | Not addressed because one DO per repo is the contract (1.3, section 8). The win claimed here is subrequests (zero on hit) and latency, not throughput. |
| Review body: "The SHA-1 trailer over the pack still has to be computed by a streaming JS SHA-1 (`crypto.subtle` is one-shot)" | `gix_hash::hasher` in Rust (`write_small_pack`); the small-pack path hashes a `Vec<u8>` of at most 4 MiB, `write_pack` keeps its own streaming hasher (section 9 step 6). |
| Review body: "N concurrent misses for the same oid all miss and all `INSERT OR REPLACE` the identical bytes: idempotent, fine" | Kept, made exact: `INSERT OR IGNORE` plus `changed()` so `objcache.bytes` counts each sha once (`cache_admit`). |
| Review body: "skip the memory tier for the batch `fetch` path and use it only for `object-info`/raw reads" | The other option was taken: the memory tier is capped, and the batch path is split by size instead. Sets above 256 entries or 4 MiB go to `write_pack` untouched (`write_small_pack` returns `Ok(false)`), so bulk clones never populate the cache. `object-info` and a raw `/raw/<oid>` route are dropped: section 12 does not advertise `object-info`, and no raw route exists in 1.3. |
| First pass, Known limits: "`blockConcurrencyWhile` re-arms [the alarm] on cold start" | Removed. The alarm is owned by `jobs::rearm`; `boot` (8.2) already re-enqueues the Janitor, and the sweep row persists via `Reschedule`. |

## Known limits
- **Foundation status.** Section 12 lists this cache as out of scope; nothing in the foundation depends on it, and it needs three write-backs to land: a `cache: RefCell<MemTier>` field on `RepoDo` (1.3), `schema_version = 3` in `boot` for `objcache` and the `objcache.bytes` row, and `JobKind::CacheSweep` in 1.4. Two helpers are assumed and not shown: `SendSet::count()` (popcount) and `Index::entries_of(&SendSet) -> Vec<(ObjectId, ObjLoc)>` (the `objects_pack` index makes it one query per pack). `read_entries` is called with `(ObjectId, ObjLoc)` pairs and a budget, as the sibling proofs assume; 1.2 writes it as `&[ObjLoc]`.
- **Memory.** Heap per DO: `MEM_BUDGET` 8 MiB + `hits` (10,000 x 28 bytes) + one small pack (4 MiB + frames) + per-request `MemFind` from section 9 (64 MiB cap). Under 128 MB with the fetch path's own budget; not measured on a deployed Worker (platform-facts, still open #4).
- **CPU.** `cache_lookup` issues one `SELECT` per candidate sha (up to 256 on the small path, one frontier on a negotiation round); an `IN (...)` batch would cut that and is not written. `entry_header` per admission is a varint parse. SHA-1 over 4 MiB per small pack.
- **Subrequests.** Zero on a full hit. On a miss, whatever `read_entries` coalesces (7.2); still charged, still capped by `ReqBudget` (7.1). The DO subrequest limit is not enforced by local workerd (#7), so scenario 16's `x-ge-subrequests: 0` assertion is the only check until a deployed measurement exists.
- **What is not cached.** Entries above 256 KiB (always range-read), delta bases during ingest (that runs in the edge Worker, 2.4, which has no DO cache), commit region reads (7.4 already reads one range per pack per request), and anything a bulk `write_pack` streams.
- **Eviction is coarse.** LRU at 15-minute granularity with a shared timestamp; the SQLite tier may sit up to 2 x `SQL_BUDGET` = 128 MiB between sweeps, inside the 10 GB DO storage cap. A sweep that hits 80% of its slice budget leaves the rest for the next firing; the table cannot grow meanwhile beyond the admission cap.
- **Unverified, day-1 list.** BLOB binding and readback through `SqlStorageValue`/`to_array`; `gix_hash::hasher`/`Hasher::try_finalize` names in 0.26.2; `Sideband::new` and `band_to_write` at runtime; that `Reschedule` keeps the job row's `state = 'queued'` with cursor untouched (4.2 says "as given"); that `jobs::enqueue` is callable from inside a sync span as 1.4 declares it (`rearm` reaches `set_alarm`, which the memo lists as async); the isolate's real memory ceiling under the numbers above.

## Depends on
- protocol-v2-only
- refs-sqlite-objects-r2
- content-addressed-r2-keys
- gc-and-repack-alarm
- partial-clone-filters
