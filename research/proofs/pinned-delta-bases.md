# Delta bases pinned per repo

> Second pass · Idea #8 · verdict: **lands with caveats** · feasibility 4/5 · reliability 3/5 · correctness 4/5 (first pass 4/4/3)
> First pass: [proof](./archived/pinned-delta-bases.md) · [review](../reviews/archived/pinned-delta-bases.md) · Second pass: [review](../reviews/pinned-delta-bases.md) · Contract: [CONTRACTS.md](../../CONTRACTS.md)


## Mechanism
Half of the first pass is gone by contract: CONTRACTS.md 2.1 resolves every client delta at ingest and stores only full entries, and section 12 forbids deltas on the wire (`thin-pack` is accepted, never sent), so there is no stored `zdelta` to replay and no `REF_DELTA` to emit. What survives is the DO-resident byte cache, reshaped as a post-foundation module (section 12 lists "pinned bases" as not built, so no other proof may assume it): after `/_do/push/commit` reports an `ok`, the edge posts the normalized entries of the push it just wrote (commits and trees first, blobs with the remainder, 2 MiB per push, 8 MiB per repo) to a new `RepoDo` route `/_do/push/pin`, which stores them in a `pins` table and evicts the oldest pushes in the same sync span; no alarm and no job kind exist for it (4.1). Two consumers read the table, both only for ids that `Index::lookup` (2.3, the only existence check) has already resolved to a live pack: `RepoDo::load_entries` replaces `Bucket::read_entries` inside the section 9 rounds of `fetch_v2` steps 3 and 5, so a round whose frontier is pinned has no await at all, and `/_do/push/bases` serves the `ref-delta` bases of a thin push (2.4 "between passes") with one stub call instead of R2 range reads. The "delta bases" the title names are therefore the thin-pack bases at ingest; the response bytes of a fetch are unchanged because `write_pack` (section 9 step 6) still copies from R2 windows, and the pins only remove the serial round trips before it.

## Primitives
- DO SQLite through `worker::SqlStorage::exec(query, Vec<SqlStorageValue>)` (sync): verified in the spike. `SqlStorageValue::Blob(Vec<u8>)` with `From<Vec<u8>>`, and the read-back path `SqlCursor::raw()` yielding `Result<Vec<SqlStorageValue>>` with `Uint8Array`/`ArrayBuffer` converted to `Blob`: **read from `worker` 0.8.5 `src/sql.rs`, not run**. `SELECT changes()` is not needed (no CAS in this module).
- `Index::lookup` (2.3 reader query, live packs only) and `Bucket::read_entries` (coalesced range reads, 7.2) with the signatures the refs-sqlite-objects-r2 proof wrote back (`&[(ObjectId, ObjLoc)]`, `&mut ReqBudget`). Real R2 range reads: **local simulator only** (platform-facts #6).
- `store::codec::{encode_entry, decode_entry, entry_header}` (1.2): sync, `gix-zlib` 0.1.0 deflate/inflate; inflate verified on workerd in the spike, level-0 deflate **unverified on wasm32** (streaming-pack-parser lists it).
- `gix_hash::ObjectId::{try_from(&[u8]), as_slice, from_hex, Display}` 0.26.2: `from_hex` verified in the spike; the others read from source (`object_id.rs` lines 128, 315).
- `gix_object::Kind` 0.64.1 (`Tree, Blob, Commit, Tag`, in that order, so the git numbering of 2.3 needs a `match`): read from source.
- `Stub::fetch_with_request` with `Request::new_with_init` + `RequestInit::with_body` and `Response::bytes()`: the two-phase-push helper `stub_json`; the constructor path is **unverified at runtime** (the spike forwarded the client request).
- `<[u8]>::split_at_checked`: std since Rust 1.80; toolchain is 1.94.1 (spike).
- DO SQLite value limit 2 MB per row/BLOB: Cloudflare limits page as cited by the first-pass review, **not measured**; the module stays at 256 KiB inflated per entry.
- git wire facts used: a v2 `fetch` response is a pack of full entries in any order (2.1, section 9 step 6); `index-pack` never runs `--fix-thin` work because no `REF_DELTA`/`OFS_DELTA` entry is ever emitted (section 12); `git push --thin` bases are whatever objects the client believes the server has, most often the previous version of each touched path.

## Proof code
```rust
// src/store/pins.rs (sync codec, no `worker` import) + src/repo_do/pins.rs + two hooks in pack::ingest.
// CONTRACTS.md 1.2, 1.3, 2.1, 2.3, 2.4, 3, 4.1, 7.1, 8, 9, 10, 12. exec()/N as in refs-sqlite-objects-r2; stub_bytes = the
// two-phase-push stub_json with `resp.bytes().await` instead of `resp.json()`; RepoRoute carries x-ge-owner/x-ge-repo (8.1).
use std::collections::HashMap;
use gix_hash::ObjectId;
use gix_object::Kind;
use worker::{SqlStorageValue as V, Stub};
use crate::{error::Error, store::{codec, Bucket, Index, MemFind, ObjLoc}, ReqBudget, RepoDo, RepoRoute};
// Schema, added by RepoDo::boot at the next schema_version (8.2). No other module reads or writes it:
//   CREATE TABLE pins (sha TEXT PRIMARY KEY, kind INTEGER NOT NULL, size INTEGER NOT NULL, bytes INTEGER NOT NULL,
//                      gen INTEGER NOT NULL, entry BLOB NOT NULL) WITHOUT ROWID;   -- entry = codec::encode_entry bytes (2.1)
//   CREATE INDEX pins_gen ON pins(gen);
pub const PIN_BUDGET: i64 = 8 << 20;          // whole table, DO SQLite bytes; never all in memory at once
pub const PIN_PUSH_MAX: usize = 2 << 20;      // one /_do/push/pin body per push; edge holds at most 2x this
pub const PIN_ENTRY_MAX: usize = 256 << 10;   // inflated size cap per pinned object, far under the 2 MB SQLite value limit
const FRAME_HDR: usize = 24;                  // sha (20) + entry length (u32 BE)

// ---- store::pins: body codec of /_do/push/pin (in) and /_do/push/bases (out): repeated `sha20 | len | entry`. ----
pub fn encode_frame(id: &ObjectId, entry: &[u8], out: &mut Vec<u8>) -> Result<(), Error> {
    let len = u32::try_from(entry.len()).map_err(|_| Error::Internal("pin entry too long".into()))?;
    out.extend_from_slice(id.as_slice()); out.extend_from_slice(&len.to_be_bytes()); out.extend_from_slice(entry); Ok(())
}
pub fn decode_frames(mut body: &[u8]) -> Result<Vec<(ObjectId, Vec<u8>)>, Error> {
    let bad = || Error::Protocol("bad pin frame".into());        // the body is our own edge's, but the lint set of section 10 applies
    let mut out = Vec::new();
    while !body.is_empty() {
        let (hdr, rest) = body.split_at_checked(FRAME_HDR).ok_or_else(bad)?;
        let id = ObjectId::try_from(hdr.get(..20).ok_or_else(bad)?).map_err(|_| bad())?;
        let len = hdr.get(20..).and_then(|b| <[u8; 4]>::try_from(b).ok()).map(u32::from_be_bytes).ok_or_else(bad)?;
        let (entry, rest) = rest.split_at_checked(usize::try_from(len).map_err(|_| bad())?).ok_or_else(bad)?;
        codec::entry_header(entry)?;                              // a delta or garbage is refused here, never stored (2.1)
        out.push((id, entry.to_vec())); body = rest;
    }
    Ok(out)
}

// ---- pack::ingest hooks (edge Worker). ----
/// Pass B (2.4): offer() runs once per resolved object right after PackWriter::append_entry, with the same (kind, data).
/// Commits and trees first (every incremental fetch and every thin push touches them first), blobs with what is left.
#[derive(Default)] pub struct PinCollector { hot: Vec<u8>, blobs: Vec<u8> }
impl PinCollector {
    pub fn offer(&mut self, id: &ObjectId, kind: Kind, data: &[u8]) -> Result<(), Error> {
        if data.len() > PIN_ENTRY_MAX { return Ok(()); }
        let (mut entry, mut frame) = (Vec::new(), Vec::new());
        codec::encode_entry(kind, data, &mut entry);              // sync gix-zlib deflate; any valid full entry decodes the same
        encode_frame(id, &entry, &mut frame)?;
        let buf = if kind == Kind::Blob { &mut self.blobs } else { &mut self.hot };
        if buf.len().saturating_add(frame.len()) <= PIN_PUSH_MAX { buf.extend_from_slice(&frame); }
        Ok(())
    }
    /// Sent to /_do/push/pin only after commit_push reported at least one `ok` (section 3 step 4); a failure is logged
    /// and ignored: pins are a cache, the push is already durable and committed.
    pub fn body(mut self) -> Vec<u8> {
        let mut rest = self.blobs.as_slice();
        while let Some((hdr, after)) = rest.split_at_checked(FRAME_HDR) {           // whole blob frames only
            let n = hdr.get(20..).and_then(|b| <[u8; 4]>::try_from(b).ok()).map(u32::from_be_bytes).unwrap_or(u32::MAX);
            let Some((entry, after)) = after.split_at_checked(usize::try_from(n).unwrap_or(usize::MAX)) else { break };
            if self.hot.len().saturating_add(FRAME_HDR).saturating_add(entry.len()) > PIN_PUSH_MAX { break; }
            self.hot.extend_from_slice(hdr); self.hot.extend_from_slice(entry); rest = after;
        }
        self.hot
    }
}
/// Between passes (2.4): after /_do/push/lookup, before Bucket::read_entries. One stub call (7.1) replaces the range
/// reads for every base the DO still pins; misses take the contract path unchanged. `locs` = the lookup result, <= 1000.
pub async fn fetch_bases(stub: &Stub, route: &RepoRoute, locs: &HashMap<ObjectId, ObjLoc>, bucket: &Bucket,
                         budget: &mut ReqBudget) -> Result<HashMap<ObjectId, Vec<u8>>, Error> {
    let ids: Vec<String> = locs.keys().map(|id| id.to_string()).collect();
    let body: Vec<u8> = stub_bytes(stub, route, "/_do/push/bases", &serde_json::json!({ "ids": ids }), budget).await?;
    let mut out: HashMap<ObjectId, Vec<u8>> = decode_frames(&body)?.into_iter().collect();
    let cold: Vec<(ObjectId, ObjLoc)> = locs.iter().filter(|(id, _)| !out.contains_key(id)).map(|(i, l)| (*i, l.clone())).collect();
    if !cold.is_empty() { out.extend(bucket.read_entries(&cold, budget).await?); }
    Ok(out)                                                       // caller decode_entry()s into the 32 MiB base map of 2.4
}

// ---- src/repo_do/pins.rs: routes are "Awaits inside: none" (1.3): body read, then one sync span. ----
fn kind_num(k: Kind) -> i64 { match k { Kind::Commit => 1, Kind::Tree => 2, Kind::Blob => 3, Kind::Tag => 4 } }   // 2.3 numbering
impl RepoDo {
    /// POST /_do/push/pin. Insert, then drop whole generations (oldest push first) until the table fits PIN_BUDGET.
    /// Eviction is in this span; there is no alarm (4.1: only jobs::rearm sets one) and no job kind.
    pub fn pin_put(&self, body: &[u8]) -> Result<(), Error> {
        let sql = self.state.storage().sql();
        let frames = decode_frames(body)?;
        let gen = self.meta_i64("refs_version")?;                 // bumped by every committed push (3.5): a push ordinal, not a clock
        for (id, entry) in &frames {
            let (kind, size, _) = codec::entry_header(entry)?;
            let (size, bytes) = (i64::try_from(size).map_err(|_| Error::Internal("pin size".into()))?,
                                 i64::try_from(entry.len()).map_err(|_| Error::Internal("pin len".into()))?);
            exec(&sql, "INSERT INTO pins(sha,kind,size,bytes,gen,entry) VALUES(?,?,?,?,?,?) ON CONFLICT(sha) DO UPDATE SET gen=excluded.gen",
                 vec![id.to_string().into(), kind_num(kind).into(), size.into(), bytes.into(), gen.into(), V::Blob(entry.clone())])?;
        }
        for _ in 0..8 {                                           // a generation is <= PIN_PUSH_MAX, so 4 rounds always suffice
            if exec(&sql, "SELECT COALESCE(SUM(bytes),0) AS n FROM pins", vec![])?.one::<N>()?.n <= PIN_BUDGET { break; }
            exec(&sql, "DELETE FROM pins WHERE gen = (SELECT MIN(gen) FROM pins)", vec![])?;
        }
        Ok(())
    }
    /// Sync. Pinned entry bytes for ids the caller has ALREADY resolved to a live pack with Index::lookup. The pin is a
    /// byte cache, never an existence check: 2.3 stays the only way a sha is resolved, and a swept object's stale pin
    /// is unreachable because its lookup returns None first.
    pub fn pins_take(&self, ids: &[ObjectId]) -> Result<HashMap<ObjectId, Vec<u8>>, Error> {
        let sql = self.state.storage().sql();
        let mut out = HashMap::new();
        for chunk in ids.chunks(100) {
            let q = format!("SELECT sha, entry FROM pins WHERE sha IN ({})", vec!["?"; chunk.len()].join(","));
            let args: Vec<V> = chunk.iter().map(|id| id.to_string().into()).collect();
            for row in exec(&sql, &q, args)?.raw() {              // raw(): Uint8Array -> Blob, worker 0.8.5 sql.rs (read, not run)
                match row.map_err(|e| Error::Storage(e.to_string()))?.as_slice() {
                    [V::String(sha), V::Blob(entry)] =>
                        { out.insert(ObjectId::from_hex(sha.as_bytes()).map_err(|_| Error::Storage("pin sha".into()))?, entry.clone()); }
                    _ => return Err(Error::Storage("pin row shape".into())),
                }
            }
        }
        Ok(out)
    }
    /// POST /_do/push/bases, body JSON {ids:[hex]} (<= 1000): frames for the ids that are live AND pinned.
    pub fn bases(&self, ids: &[ObjectId]) -> Result<Vec<u8>, Error> {
        let sql = self.state.storage().sql();
        let live: Vec<ObjectId> = ids.iter().zip(Index(&sql).lookup(ids)?).filter_map(|(id, loc)| loc.map(|_| *id)).collect();
        let mut out = Vec::new();
        for (id, entry) in self.pins_take(&live)? { encode_frame(&id, &entry, &mut out)?; }
        Ok(out)
    }
    /// Drop-in for `bucket.read_entries` in the section 9 rounds (fetch_v2 steps 3 and 5; GcMark may use it too): pins
    /// first, sync; only misses go to R2, coalesced (7.2). A round with no miss has no await, so the loop stays inside
    /// one sync span. Memory: <= PIN_BUDGET from pins plus one R2 span; the caller enforces MemFind's 64 MiB cap (9.3).
    pub async fn load_entries(&self, bucket: &Bucket, locs: &[(ObjectId, ObjLoc)], mem: &mut MemFind,
                              budget: &mut ReqBudget) -> Result<(), Error> {
        let ids: Vec<ObjectId> = locs.iter().map(|(id, _)| *id).collect();
        let hit = self.pins_take(&ids)?;
        let cold: Vec<(ObjectId, ObjLoc)> = locs.iter().filter(|(id, _)| !hit.contains_key(id)).cloned().collect();
        for (id, entry) in hit { let (k, d) = codec::decode_entry(&entry)?; mem.insert(id, k, d); }
        if cold.is_empty() { return Ok(()); }
        for (id, entry) in bucket.read_entries(&cold, budget).await? { let (k, d) = codec::decode_entry(&entry)?; mem.insert(id, k, d); }
        Ok(())
    }
}
```

## Why it works
- **No delta ever reaches a client, so the shallow and partial-clone hole cannot open.** Section 12 forbids wire deltas and 2.1 forbids stored ones; `FetchArgs.thin_pack` (1.1) is parsed and ignored. `write_pack` copies full entries in offset order (section 9 step 6), so `index-pack --fix-thin` has nothing to resolve for `--depth`, `deepen`, or `filter=blob:none` clients; the review's `fatal: pack has N unresolved deltas` is unreachable by construction, not by a `clientHas` check.
- **Pins change latency, never bytes.** `load_entries` feeds the same `MemFind` that `read_entries` would (section 9 loop), and the response is still produced from `SendSet` bitmaps by `write_pack` over R2 windows. The pack a client receives is byte-identical with the `pins` table full or empty; the difference is visible only in the `x-ge-subrequests` header (section 7), which is what the harness asserts.
- **Existence is decided by 2.3 alone.** `pins_take` is called only with ids that `Index::lookup` already resolved to a live pack (`bases` does the lookup itself; `load_entries` receives `ObjLoc`s from the caller's lookup). A GC sweep (5.3) or an `expired` pack (5.1) therefore never resurrects through a stale pin; the stale row is dead weight bounded by `PIN_BUDGET` and dropped by generation.
- **Thin-push bases (2.4, scenario 9).** `git push --thin` deltas each changed object against the version the server already has, almost always in the previous push's pack, which is the most recent pin generation. `/_do/push/bases` is one stub call (7.1, charged) and the misses are the contract's coalesced `read_entries`; the ingest base map, its 32 MiB cap and `decode_entry` are unchanged.
- **Incremental fetch rounds (section 9 steps 3 and 5, scenario 12).** A `git fetch` shortly after a push wants exactly the objects of the last one to four pushes: tip commits (step 3 frontier), root trees and their subtrees (step 5 rounds, one serial R2 round trip per tree depth). With those entries pinned, every round is a sync `SELECT ... IN (...)` and the DO reaches `write_pack` after zero awaits; the remaining R2 cost is the window reads of step 6, one per young pack.
- **Atomicity and the alarm rule.** `pin_put` is one sync span: insert then evict, no await between (section 3's argument, platform-facts #4). It calls no `set_alarm` (4.1) and registers no `JobKind` (4.5): eviction is by whole `gen`, `SUM(bytes)` is re-read each round, and no cursor is iterated while rows are deleted.
- **Ordering with the push.** The edge sends pins only after `/_do/push/commit` returned an `ok` (section 3 ordering, step 3 flipped the pack `live`), so a pin never describes an `ingesting` pack, and a crash between commit and pin loses only cache entries. `gen = refs_version` is monotonic (3.5), so eviction order is push order without a clock.
- **Bytes are self-validating.** `decode_frames` runs `codec::entry_header` on every frame and refuses deltas, and both consumers run `decode_entry`, whose inflate fails on corrupt bytes with `Error::Unpack`/`Error::Storage` rather than serving them; `git fsck` in scenarios 9 and 12 is the external check.
- **Conformance.** Scenarios 9 and 12 must pass unchanged. One added scenario, 12b: run scenario 12 twice, once with the `pins` table emptied through a test-only `/_do/pins/clear` route, assert the two packs are byte-identical and that `x-ge-subrequests` is strictly lower with pins present.

## Changes from the first pass
| First-pass finding (quoted) | How addressed |
|---|---|
| Blocker 1: "`ctx.id.name` is undefined inside a DO created via `idFromName`, so `repoKey` yields `objects/undefined/<sha>` and the cold R2 path never finds an object" | No key is derived in this module at all. Pins hold entry bytes keyed by sha; the cold path is `Bucket::read_entries` over `ObjLoc`s, and `Bucket` builds `r/<repo_id>/packs/<pack_id>.pack` from `meta.repo_id` (2.2, section 8). `ctx.id.name` is read nowhere (8.3). |
| Blocker 2: "`clientHas(base)` ... for shallow (`--depth`) and partial-clone (`filter=blob:none`) clients ... answers yes for objects the client lacks, and git's `index-pack --fix-thin` fails" | Closed by section 12 and 2.1: the server emits no `REF_DELTA`/`OFS_DELTA`, so there is no base to have. `FetchArgs.thin_pack` is ignored; `load_entries` only pre-fills `MemFind`, and `write_pack` still sends full entries ("Why it works", bullet 1). There is no `clientHas`. |
| Caveat: "Storing delta bytes 'verbatim' requires the streaming parser to know each entry's zlib boundary; DecompressionStream does not report consumed bytes ... or the delta must be re-deflated" | Moot: no delta bytes are stored (2.1). `PinCollector::offer` re-encodes the resolved `(kind, data)` with `codec::encode_entry` (sync `gix-zlib`), so the pin is a valid full entry regardless of what `PackWriter` buffered. |
| Caveat: "Protocol v2 packfile section and band-1 sideband pkt-line framing (max 65520 bytes) are assumed, not shown; the code returns a raw pack stream" | This module writes no wire bytes. Framing is `wire::Sideband` (1.1 rule 2, 65515-byte frames) driven by `write_pack` (section 9 step 6), owned by protocol-v2-only. |
| Caveat: "Cold-path objects are fully materialised and `deflateSync` blocks the single-threaded DO; large blobs stall pushes and hit the 128MB / CPU limits" | The read path has no deflate: entries at rest are already wire-ready and `write_pack` copies them from 8 MiB windows (2.1, 6.5). The only deflate is `encode_entry` at the edge in `PinCollector::offer`, capped at `PIN_ENTRY_MAX` per object and `PIN_PUSH_MAX` per push. DO memory per request: at most `PIN_BUDGET` of pinned bytes plus one R2 span, under the 64 MiB `MemFind` cap the caller enforces (9.3). |
| Caveat: "`alarm()` deletes from `pins` while iterating a live cursor over `pins`; materialise with `toArray()` first. The `deltas` table is bounded only by pin eviction" | No alarm exists (4.1 would forbid it); `pin_put` evicts in its own sync span by whole generation with `SUM(bytes)` re-read per round, never iterating a cursor across a delete. There is no `deltas` table. |
| Caveat: "Only client-chosen deltas are reused (no server-side delta chains), so the win is confined to fetch-soon-after-push; multi-push-behind fetches get full objects" | Every client gets full objects now (section 12), so this is no longer a degradation but the baseline. The pin window is the last pushes that fit in 8 MiB, so a fetch several pushes behind still hits for all of them; older objects take the section 9 rounds unchanged (Known limits). |
| Review body: "gated on `streaming-pack-parser` exposing per-entry zlib boundaries ... and on `want-have-negotiation` giving an ancestor test" | Neither is needed: no verbatim delta bytes, no ancestor test (bullets above). The remaining dependencies are the foundation slugs whose signatures the code compiles against (Depends on). |
| Review body: "if the ref flip and `pinAndRecord` are separate DO calls, a crash between them leaves pins for an unflipped tip" | The order is reversed and made harmless: pins are sent only after `commit_push` reported an `ok`, so a crash loses cache rows, never leaves pins describing an uncommitted pack; and `pins_take` is reached only through a live `Index::lookup` anyway. |
| First-pass limit: "a repo whose DO SQLite is wiped loses pins and deltas but nothing else" | Still true and now stronger: pins hold bytes that also exist in a live pack, so `DELETE FROM pins` at any time changes only `x-ge-subrequests` (scenario 12b). |

## Known limits
- **The response still costs R2.** `write_pack` reads its windows from R2 (section 9 step 6), so an incremental fetch pays at least one range read per young pack touched; pins remove only the serial rounds of steps 3 and 5 and the ingest base read. The first pass's "zero R2 reads" is not achieved and cannot be under 2.2 (pins are not a second copy of the pack). Making `write_pack` source pinned entries would change its 1.4 signature and is left to in-do-object-cache.
- **Benefit unmeasured.** The saving is R2 range-read latency versus a SQLite `SELECT` (in the DO) or one DO stub call (from the edge); no deploy exists to measure either (platform-facts "still open"). Local workerd does not enforce the subrequest limit (#7), so only the `x-ge-subrequests` header shows the difference in CI.
- **Coverage is push-shaped.** `PIN_PUSH_MAX = 2 MiB` per push and `PIN_BUDGET = 8 MiB` per repo: a large push pins its commits and trees and a prefix of its blobs; a fetch more than a few pushes behind, or a thin push whose bases were last touched long ago, falls back to the contract path. Whole-generation eviction can drop a still-hot generation when pushes are large.
- **Subrequests and CPU.** Edge: +1 stub call for `/_do/push/bases` on thin pushes and +1 for `/_do/push/pin` on committed pushes, against 9,000 (7.1); `encode_entry` over at most 2 MiB per push is milliseconds. DO: at most `PIN_PUSH_MAX / FRAME_HDR` `INSERT`s per push in one span (worst case tens of thousands of tiny objects; bounded, but a day-1 timing test is due), `SELECT ... IN (100)` per chunk on the read side.
- **Memory.** Edge: `PinCollector` holds at most `2 x PIN_PUSH_MAX = 4 MiB` in addition to the pass B budget of 2.4. DO: `pins_take` copies at most `PIN_BUDGET` of bytes per round into `MemFind`; the 64 MiB cap of 9.3 is enforced by the caller, not here.
- **Unverified.** `SqlStorageValue::Blob` round trip through `exec` and `raw()` at runtime (read from `worker` 0.8.5 source only); `Request::new_with_init` + `RequestInit::with_body` for the stub body (two-phase-push lists it); level-0 `gix-zlib` deflate on wasm32; the 2 MB DO SQLite value limit (docs, not measured); whether `SELECT ... IN` with 100 bindings is well inside workerd's binding limit (assumed).
- **Write-backs this proof needs.** Two internal routes in the 1.3 table (`/_do/push/pin`: binary frames in, `{}` out, awaits none; `/_do/push/bases`: JSON `{ids}` in, binary frames out, awaits none); the `pins` table at the next `schema_version`; `RepoDo::load_entries` named in section 9 as the loader the rounds call; `read_entries` with `&[(ObjectId, ObjLoc)]` and `&mut ReqBudget` as refs-sqlite-objects-r2 already asks; section 12's "pinned bases" line pointing at this module as a post-foundation addition.

## Depends on
- refs-sqlite-objects-r2
- streaming-pack-parser
- two-phase-push
- protocol-v2-only
- repo-do-ref-authority
- gc-and-repack-alarm
