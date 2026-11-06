# Shallow and partial clone as first-class filters

> Second pass · Idea #10 · verdict: **risky** · feasibility 4/5 · reliability 4/5 · correctness 3/5 (first pass 3/4/3)
> First pass: [proof](./archived/partial-clone-filters.md) · [review](../reviews/archived/partial-clone-filters.md) · Second pass: [review](../reviews/partial-clone-filters.md) · Contract: [CONTRACTS.md](../../CONTRACTS.md)


## Mechanism
This idea is subsumed by CONTRACTS.md: `wire::Filter { BlobNone, BlobLimit }` and `FetchArgs::{deepen, shallow}` (1.1) are parsed by protocol-v2-only, and section 9 steps 3 to 5 already say where the filter and the depth cut apply, so the proof below is the concrete `pack::generate` module (1.4: `send_set` and `write_pack`) that `RepoDo::fetch_v2` (1.3) calls between step 1 and the `Response::from_stream` of step 6. The "lazy blob" half of the idea falls out of the storage decision rather than out of a filter table: every object at rest is one full entry `[offset, offset+len)` inside a normalized pack (2.1) with one `objects` row (2.3), so a promisor fetch (`want <blob>` lines, `filter blob:none`, `done`, which is what `git checkout` sends after a `--filter=blob:none` clone) is marked from `Index::lookup` rows alone with no walk, and `plan_reads` turns the marked entries into coalesced range reads (7.2) whose count is bounded by the 8 MiB window count of the packs touched, never by the number of wants. The `tree:0` half is dropped: section 12 lists `tree:<n>` as out of scope, so `parse_fetch` answers it with a 400 and this module never sees it.

## Primitives
- `Index::lookup` (2.3 reader query, live packs only) and DO SQLite through `worker::SqlStorage::exec` (sync): supported in `worker` 0.8.5 and exercised on workerd in the spike; the bindings type is wrapped in the siblings' `exec` helper, **unverified** shape. Two helpers this proof needs and the siblings also assume: `Index::pack_meta(&PackId) -> (count, bytes)` and `Index::entries_of(&PackSlice) -> Vec<ObjLoc>` sorted by offset (in-do-object-cache names `entries_of`).
- `Bucket::read_entries(&[(ObjectId, ObjLoc)], &mut ReqBudget)` (7.2 coalescing, charged per span, 7.1) and `Bucket::read_range(key, off, len)` (one subrequest): `Range::OffsetWithLength` verified in `worker/src/r2/builder.rs` (streaming-pack-parser); real R2 range reads **local simulator only** (platform-facts #6); the DO subrequest limit is **not enforced locally** (#7), so `ReqBudget` is the only guard.
- `MemFind` (1.2) implementing `gix_object::Find`: the spike's `HashMap` `Find` ran on workerd (`research/rust-spike.md`, ancestry walk). `codec::decode_entry` (1.2, sync, `gix_zlib::Inflate` per CONTRACTS correction 2): inflate of pack entries ran on workerd in the spike.
- `gix_object::{CommitRefIter::{tree_id, parent_ids}, TagRef::{from_bytes, target, target_kind}, TreeRefIter::from_bytes, tree::EntryMode::{is_tree, is_commit}}` 0.64.1: CI-built for wasm32 (memo section 3); `CommitRef::from_bytes` ran in the spike, the iterator and tag/tree names are per docs.rs, **not run**.
- `gix_hash::{hasher, Hasher::{update, try_finalize}}` 0.26.2 for the trailer: `compute_hash` ran in the spike; the streaming constructor name is **unverified** (content-addressed-r2-keys uses the same).
- `wire::Sideband::data` (1.1 rule 2, frames of at most 65515 bytes over `gix_packetline::blocking_io::encode::band_to_write`): **not exercised by the spike**. `write_fetch_prelude` (protocol-v2-only) for `acknowledgments` and `shallow-info`.
- `ReqBudget { max_subrequests: 9000, max_ms: 240000 }` (7.1): fields are read here for a projection before the first byte; `Error::Budget` and `Error::Limit` map to 413 before any response byte (section 10).
- No `gix_traverse` walk. Section 9 step 4 names `gix_traverse::commit::topo::Builder` with hidden ends and marks its constructor unverified; the rounds of step 3 already yield the send set (every loaded commit is reachable from a want without passing an ack), so the module uses only `gix_object` parsing and needs nothing from gix-traverse 0.61.0.
- git wire facts used, checked against git 2.43 source (`upload-pack.c`, `list-objects-filter.c`, `shallow.c`), not yet by scenario run: explicitly wanted objects are always sent, the filter applies only to objects reached through traversal (`NOT_USER_GIVEN`); `blob:limit=<n>` omits blobs of size at least `n`; `deepen <n>` sends `n` commits deep counting the tip as 1, commits at the boundary that have parents are `shallow`; client `shallow` lines without `deepen` are registered as grafts (`send_shallow_list`), with `deepen` the walk crosses them and those reached inside the new depth are `unshallow`; `git fetch --unshallow` sends `deepen 2147483647`; `pack.useSparse` prunes by comparing subtrees against the uninteresting edge commits' trees path by path; `git checkout` on a blobless clone issues one `fetch --filter=blob:none --stdin` with every missing blob (`check_updates` prefetch, git 2.24+), with `fetch.negotiationAlgorithm=noop`, so the request carries `done` and no `have`.

## Proof code
```rust
// src/pack/generate.rs -- CONTRACTS.md 1.1, 1.2, 1.4, 2.1-2.3, 2.5, 6.5, 7, 9, 10, 12. Runs inside RepoDo::fetch_v2 (1.3).
use std::collections::{HashMap, HashSet};
use bstr::BString;
use gix_hash::ObjectId;
use gix_object::{CommitRefIter, Find, Kind, TagRef, TreeRefIter};
use crate::{error::Error, store::{codec, keys, Bucket, Index, MemFind, ObjLoc, PackId}, wire::{Filter, Sideband}, ReqBudget, RepoDo};
pub const MAX_COMMITS: usize = 200_000; pub const MAX_MEM: usize = 64 << 20;   // 9.3
pub const MAX_OBJECTS: usize = 1_000_000;  // ids kept for dedup, ~40 MB; a fresh clone above this is ClonePlan's (precomputed-clone-pack)
const CHUNK: usize = 10_000; const INFINITE: u32 = 0x7fff_ffff;   // ids per read round (MemFind cleared after each); git's INFINITE_DEPTH (`--unshallow`)
const GAP: u64 = 256 << 10; const WINDOW: u64 = 8 << 20;   // 7.2 merge gap; 6.5 read size, plus at most one straddling entry
const TOO_BIG: &str = "fetch too large for this server; clone instead";
pub struct PackSlice { pub pack: PackId, pub count: u32, pub bytes: u64, pub bitmap: Vec<u8> }   // one bitmap per pack (9.5)
pub struct Read { pub pack: usize, pub off: u64, pub len: u64, pub ents: Vec<(u64, u32)> }       // one range read; (offset, len) per entry in it
#[derive(Default)] pub struct SendSet { pub packs: Vec<PackSlice>, pub reads: Vec<Read>, pub shallow: Vec<ObjectId>, pub unshallow: Vec<ObjectId> }
impl SendSet {
    /// Sync, idempotent: one bit per objects.idx (2.3). A sha in two live packs is marked where lookup found it.
    fn mark(&mut self, idx: &Index<'_>, loc: &ObjLoc) -> Result<(), Error> {
        let i = match self.packs.iter().position(|p| p.pack == loc.pack) { Some(i) => i, None => {
            let (count, bytes) = idx.pack_meta(&loc.pack)?;                                     // SELECT count, bytes FROM packs WHERE id=?
            let bits = usize::try_from(count).map_err(|_| Error::Internal("count".into()))?.div_ceil(8);
            self.packs.push(PackSlice { pack: loc.pack.clone(), count, bytes, bitmap: vec![0; bits] }); self.packs.len().saturating_sub(1) } };
        let byte = usize::try_from(loc.idx / 8).map_err(|_| Error::Internal("idx".into()))?;
        *self.packs.get_mut(i).and_then(|p| p.bitmap.get_mut(byte)).ok_or_else(|| Error::Storage("idx past pack count".into()))? |= 1u8 << (loc.idx % 8);
        Ok(())
    }
    pub fn count(&self) -> u32 { self.packs.iter().flat_map(|p| &p.bitmap).map(|b| b.count_ones()).sum() }
}
pub async fn send_set(d: &RepoDo, bucket: &Bucket, wants: &[ObjectId], haves: &[ObjectId], filter: Option<&Filter>, deepen: Option<u32>,   // 1.4 signature
                      budget: &mut ReqBudget) -> Result<SendSet, Error> { send_set_shallow(d, bucket, wants, haves, filter, deepen, &[], budget).await }
/// Section 9 steps 3 to 5 as rounds: `load` is the only await; between loads everything is sync gitoxide parsing over MemFind. A promisor
/// fetch (`want <blob>`.., `filter blob:none`, `done`, no `have`) enters no loop: its wants are marked from lookup rows alone.
pub async fn send_set_shallow(d: &RepoDo, bucket: &Bucket, wants: &[ObjectId], haves: &[ObjectId], filter: Option<&Filter>,
                              deepen: Option<u32>, client_shallow: &[ObjectId], budget: &mut ReqBudget) -> Result<SendSet, Error> {
    let (sql, cs) = (d.state.storage().sql(), client_shallow.iter().copied().collect::<HashSet<ObjectId>>());
    let idx = Index(&sql); let acks: HashSet<ObjectId> = haves.iter().zip(idx.lookup(haves)?).filter_map(|(h, l)| l.map(|_| *h)).collect();   // step 1: unknown haves dropped
    let (mut set, mut mem, mut seen, mut buf) = (SendSet::default(), MemFind::default(), HashSet::<ObjectId>::new(), Vec::new());
    let (mut commits, mut trees, mut bases, mut edge, mut tags) = (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for (id, loc) in wants.iter().zip(idx.lookup(wants)?) {                                    // step 1: every want must be live
        let loc = loc.ok_or_else(|| Error::Protocol(format!("upload-pack: not our ref {id}")))?;
        set.mark(&idx, &loc)?; seen.insert(*id);                                                // a wanted object is always sent, filter or not
        match loc.kind { Kind::Commit => commits.push(*id), Kind::Tree => trees.push(*id), Kind::Tag => tags.push(*id), Kind::Blob => {} }
    }
    while !tags.is_empty() { load(&idx, bucket, &mut mem, &tags, budget).await?;               // step 3: peel, one round per nesting level
        for t in std::mem::take(&mut tags) {
            let tag = TagRef::from_bytes(find(&mem, &t, &mut buf)?).map_err(|e| Error::Storage(e.to_string()))?;
            match (tag.target_kind, tag.target()) { (Kind::Commit, x) => commits.push(x), (Kind::Tree, x) => trees.push(x), (Kind::Tag, x) => tags.push(x),
                (Kind::Blob, x) => { if let Some(l) = idx.lookup(&[x])?.pop().flatten() { set.mark(&idx, &l)?; seen.insert(x); } } }
        } mem.clear();
    }
    let cap = deepen.unwrap_or(u32::MAX);                                                       // deepen n: depths 0..n-1 are sent (git counts the tip as 1)
    if deepen == Some(INFINITE) { commits.extend(cs.iter().copied()); }                         // --unshallow: every client shallow is walked and unshallowed
    let (mut depth, mut lvl) = (HashMap::<ObjectId, u32>::new(), 0u32);
    while !commits.is_empty() {
        commits.sort_unstable(); commits.dedup(); commits.retain(|c| !depth.contains_key(c));     // BFS by level: the first depth wins
        if depth.len().saturating_add(commits.len()) > MAX_COMMITS { return Err(Error::Limit(TOO_BIG.into())); }
        let (locs, mut next) = (load(&idx, bucket, &mut mem, &commits, budget).await?, Vec::new());   // want-have-negotiation's commit-region cache (7.4) slots in here
        for (c, loc) in std::mem::take(&mut commits).into_iter().zip(locs) {
            let data = find(&mem, &c, &mut buf)?; depth.insert(c, lvl);
            let tree = CommitRefIter::from_bytes(data).tree_id().map_err(|e| Error::Storage(e.to_string()))?;
            let parents: Vec<ObjectId> = CommitRefIter::from_bytes(data).parent_ids().collect();
            let (is_ack, is_cs, boundary) = (acks.contains(&c), cs.contains(&c), lvl.saturating_add(1) >= cap);
            if is_ack { bases.push(tree); } else { set.mark(&idx, &loc)?; trees.push(tree); }     // the client has acks and their trees
            if deepen.is_some() && is_cs && !boundary { set.unshallow.push(c); }                // its parents are inside the new depth (send_unshallow)
            else if deepen.is_some() && boundary && !parents.is_empty() && !is_cs { set.shallow.push(c); }   // get_shallow_commits boundary
            if (deepen.is_some() && boundary) || (deepen.is_none() && (is_ack || is_cs)) { continue; }   // stop at the depth cut; else at acks and client shallows (grafts)
            for p in parents { if deepen.is_some() || !acks.contains(&p) { next.push(p); } else { edge.push(p); } }
        }
        mem.clear(); commits = next; lvl = lvl.saturating_add(1);
    }
    edge.sort_unstable(); edge.dedup(); edge.truncate(64);                                      // ack parents of sent commits: their trees prune the tree walk
    load(&idx, bucket, &mut mem, &edge, budget).await?;
    for e in &edge { bases.push(CommitRefIter::from_bytes(find(&mem, e, &mut buf)?).tree_id().map_err(|e| Error::Storage(e.to_string()))?); }
    mem.clear(); bases.sort_unstable(); bases.dedup(); bases.truncate(64); trees.sort_unstable(); trees.dedup();
    let mut items: Vec<(ObjectId, Vec<ObjectId>)> = trees.into_iter().map(|t| (t, bases.clone())).collect();   // step 5: (tree, counterparts)
    while !items.is_empty() { let (mut next, mut blobs) = (Vec::new(), Vec::new());
        for chunk in items.chunks(CHUNK) {
            let ids: Vec<ObjectId> = chunk.iter().map(|(t, _)| *t).collect();
            let mut bs: Vec<ObjectId> = chunk.iter().flat_map(|(_, b)| b.iter().copied()).collect(); bs.sort_unstable(); bs.dedup();
            for loc in load(&idx, bucket, &mut mem, &ids, budget).await? { set.mark(&idx, &loc)?; }
            load(&idx, bucket, &mut mem, &bs, budget).await?;                                   // counterparts: parsed, never marked
            for (t, b) in chunk { expand_tree(&mem, t, b, filter, &mut seen, &mut next, &mut blobs)?; } mem.clear();
        }
        for (b, loc) in blobs.iter().zip(idx.lookup(&blobs)?) {                                 // sync: size from the row, blob bytes never read (9.5)
            let loc = loc.ok_or_else(|| Error::Internal(format!("reachable blob {b} is not live")))?;   // impossible by 2.5
            if !matches!(filter, Some(Filter::BlobLimit(n)) if loc.size > *n) { set.mark(&idx, &loc)?; }
        } items = next;
    }
    plan_reads(&idx, &mut set, budget)?;
    Ok(set)
}
/// One round of the section 9 loop: sync lookup, one coalesced async read (7.2), sync decode into `mem`. Locations align with `ids`.
async fn load(idx: &Index<'_>, bucket: &Bucket, mem: &mut MemFind, ids: &[ObjectId], budget: &mut ReqBudget) -> Result<Vec<ObjLoc>, Error> {
    let locs: Vec<(ObjectId, ObjLoc)> = ids.iter().zip(idx.lookup(ids)?)
        .map(|(id, l)| l.map(|l| (*id, l)).ok_or_else(|| Error::Internal(format!("reachable {id} is not live")))).collect::<Result<_, _>>()?;   // 2.5
    for (id, entry) in bucket.read_entries(&locs, budget).await? { let (k, data) = codec::decode_entry(&entry)?; mem.insert(id, k, data); }
    if mem.bytes > MAX_MEM { Err(Error::Limit(TOO_BIG.into())) } else { Ok(locs.into_iter().map(|(_, l)| l).collect()) }
}
fn find<'a>(mem: &MemFind, id: &ObjectId, buf: &'a mut Vec<u8>) -> Result<&'a [u8], Error> {
    Ok(mem.try_find(id, buf).map_err(|e| Error::Internal(e.to_string()))?.ok_or_else(|| Error::Storage(format!("entry {id} not read")))?.data)
}
/// Sync. git's sparse edge pruning (pack.useSparse): an entry id held by a counterpart tree is had by the client; a differing subtree descends with the same-name counterparts.
fn expand_tree(mem: &MemFind, tree: &ObjectId, bases: &[ObjectId], filter: Option<&Filter>, seen: &mut HashSet<ObjectId>,
               next: &mut Vec<(ObjectId, Vec<ObjectId>)>, blobs: &mut Vec<ObjectId>) -> Result<(), Error> {
    let (mut had, mut by_name, mut buf) = (HashSet::new(), HashMap::<BString, ObjectId>::new(), Vec::new());
    for b in bases { for e in TreeRefIter::from_bytes(find(mem, b, &mut buf)?) {
        let e = e.map_err(|e| Error::Storage(e.to_string()))?; had.insert(e.oid.to_owned());
        if e.mode.is_tree() { by_name.insert(e.filename.to_owned(), e.oid.to_owned()); } } }
    for e in TreeRefIter::from_bytes(find(mem, tree, &mut buf)?) {
        let (e, oid) = { let e = e.map_err(|e| Error::Storage(e.to_string()))?; (e, e.oid.to_owned()) };
        if e.mode.is_commit() || had.contains(&oid) || !seen.insert(oid) { continue; }        // gitlink, had by the client, or queued already
        if seen.len() > MAX_OBJECTS { return Err(Error::Limit(TOO_BIG.into())); }
        if e.mode.is_tree() { next.push((oid, by_name.get(e.filename).into_iter().copied().collect())); }
        else if !matches!(filter, Some(Filter::BlobNone)) { blobs.push(oid); }              // blob:none: no lookup, no read; blob:limit: decided from the row
    }
    Ok(())
}
/// Sync, after the walk. 7.2 coalescing per pack; if that is more reads than 8 MiB windows, group by window: reads <= sum ceil(bytes / 8 MiB), whatever the want count.
pub fn plan_reads(idx: &Index<'_>, set: &mut SendSet, budget: &ReqBudget) -> Result<(), Error> { let mut reads = Vec::new();
    for (pi, p) in set.packs.iter().enumerate() {
        let locs = idx.entries_of(p)?;                                                          // marked rows of one pack, ORDER BY offset
        let plan = coalesce(pi, &locs, GAP);
        reads.extend(if u64::try_from(plan.len()).unwrap_or(u64::MAX) > p.bytes.div_ceil(WINDOW) { coalesce(pi, &locs, WINDOW) } else { plan });
    }
    let n = u32::try_from(reads.len()).map_err(|_| Error::Budget)?;
    if budget.used.saturating_add(n) > budget.max_subrequests { return Err(Error::Budget); }    // projection: never a mid-stream abort
    set.reads = reads; Ok(())
}
fn coalesce(pack: usize, locs: &[ObjLoc], gap: u64) -> Vec<Read> {
    let mut out: Vec<Read> = Vec::new();
    for l in locs {
        let end = l.offset.saturating_add(u64::from(l.len));
        match out.last_mut() {
            Some(r) if l.offset.saturating_sub(r.off.saturating_add(r.len)) < gap && l.offset.saturating_sub(r.off) < WINDOW =>
                { r.len = r.len.max(end.saturating_sub(r.off)); r.ents.push((l.offset, l.len)); }
            _ => out.push(Read { pack, off: l.offset, len: end.saturating_sub(l.offset), ents: vec![(l.offset, l.len)] }),
        }
    }
    out
}
/// One step of the response stream (6.5: one read in flight): one range read, marked entries copied verbatim (2.1). fetch_v2 drives it through
/// futures_util::stream::unfold into Response::from_stream, as stream_verbatim in precomputed-clone-pack.
pub async fn pack_chunk(bucket: &Bucket, set: &SendSet, i: usize, h: &mut gix_hash::Hasher, out: &mut Sideband<'_>, budget: &mut ReqBudget) -> Result<(), Error> {
    let Some(r) = set.reads.get(i) else { return Ok(()) }; let pack = set.packs.get(r.pack).ok_or_else(|| Error::Internal("read past packs".into()))?;
    budget.charge(1)?; let buf = bucket.read_range(&keys::pack(&bucket.repo, &pack.pack), r.off, r.len).await?;   // one subrequest; packs are immutable (2.2)
    for &(off, len) in &r.ents {
        let start = usize::try_from(off.saturating_sub(r.off)).map_err(|_| Error::Internal("offset".into()))?;
        let e = buf.get(start..start.saturating_add(usize::try_from(len).unwrap_or(usize::MAX))).ok_or_else(|| Error::Storage("short range read".into()))?;
        h.update(e); out.data(e);                                                               // a dead pack keeps its bytes for GRACE (5): a short read is corruption
    }
    Ok(())
}
/// 1.4 signature: header, every chunk, trailer into `out`. Used as written for small sets (a promisor batch); large sets stream per chunk.
pub async fn write_pack(bucket: &Bucket, set: &SendSet, out: &mut Sideband<'_>, budget: &mut ReqBudget) -> Result<(), Error> {
    let mut hdr = b"PACK".to_vec(); hdr.extend_from_slice(&2u32.to_be_bytes()); hdr.extend_from_slice(&set.count().to_be_bytes());
    let mut h = gix_hash::hasher(gix_hash::Kind::Sha1); h.update(&hdr); out.data(&hdr);     // count is exact before the first read: popcount
    for i in 0..set.reads.len() { pack_chunk(bucket, set, i, &mut h, out, budget).await?; }
    out.data(h.try_finalize().map_err(|e| Error::Internal(e.to_string()))?.as_slice()); Ok(())   // 20-byte trailer; fetch_v2 writes the flush (rule 3)
}
```

## Why it works
- **A lazy blob request costs one lookup and one range read per blob, and fewer when blobs sit together.** git's promisor fetch carries `want <blob>` lines, `filter blob:none`, `done` and no `have` (noop negotiator). In `send_set_shallow` such wants are `Kind::Blob`: they are marked from the `Index::lookup` row (2.3) and enter no loop, so `send_set` performs zero awaits. `plan_reads` then reads `[offset, offset+len)` of each entry (2.1: a full entry is already `varint(kind,size) + zlib(data)`, valid wherever it lands in an outgoing pack), merging neighbours under 256 KiB apart (7.2). One blob is one `Read` and one `read_range`; in-do-object-cache's `write_small_pack` can make it zero. No inflate, no recompress, no loose header arithmetic: the first pass's `range.offset` trick over loose objects is gone with section 2.
- **Explicitly wanted objects bypass the filter, reached objects do not.** `list-objects-filter.c` shows every filter only to objects flagged `NOT_USER_GIVEN`; pending (wanted) objects are always shown. `send_set_shallow` marks every want before any walk, and `expand_tree` applies `BlobNone` (no lookup, no read) or `BlobLimit(n)` (decided from `objects.size`, 9.5) only to entries reached through a tree. A wanted tree with `blob:none` therefore yields its tree closure without blobs, which is what a later `git fetch` for a missing tree expects.
- **Header count is exact before the first R2 byte of the pack is read.** `SendSet::count()` is a popcount over the bitmaps (9.5) computed after the walk and before `write_pack` writes `PACK`, `2`, `count`; `index-pack` reads the count before the first entry. The walk reads trees and commits by rounds (9.3 to 9.5) and never a blob.
- **Shallow follows `upload-pack.c`.** `deepen n` sends depths `0..n-1` (git counts the tip as 1, `get_shallow_commits`); a commit at depth `n-1` with parents becomes `shallow` unless the client already listed it (`CLIENT_SHALLOW` is never re-sent); with `deepen` the walk crosses acks because git's depth computation does too; without `deepen`, client `shallow` lines stop the walk exactly as `send_shallow_list` registers them as grafts. `deepen 2147483647` (`--unshallow`) seeds every client shallow into the walk and reports it `unshallow`, as `deepen(INFINITE_DEPTH)` flags them `NOT_SHALLOW`. Commit objects are sent unmodified: the client's `.git/shallow`, not the object, records the cut (scenario 11).
- **Annotated tags are sent and peeled.** A want that is `Kind::Tag` is marked at step 1 and loaded in the peel loop; `TagRef::target_kind` routes the target to the commit, tree, blob or (nested) tag frontier. `git clone` of a tagged repo receives both the tag object and its target (scenario 3).
- **Incremental fetches send new objects only, without walking the whole tree of the repo.** Parents that are acks are `edge`; their root trees and the root trees of any ack met in the walk are `bases`. `expand_tree` is git's `pack.useSparse` edge pruning: an entry whose id appears in any counterpart tree is had by the client (the client has every object under an ack, 2.5 closes the set), and a differing subtree descends with the counterpart of the same name, so reads scale with changed paths. Superset where the heuristic misses (a moved file) is legal: the protocol permits supersets (9.4). Scenario 12's "only new objects" holds for edits in place.
- **The walk obeys the section 9 rule to the letter.** `load` holds the only await of the walk and `pack_chunk` the only await of the stream; `expand_tree`, `find`, `coalesce`, `plan_reads` and `SendSet::mark` are `fn`; `MemFind` is cleared after every chunk, capped at 64 MiB (9.3), commits at 200,000 (9.3), dedup ids at `MAX_OBJECTS`; every miss of `Index::lookup` on a reachable object is `Error::Internal`, impossible by the connectivity invariant (2.5). No `unwrap`, `expect`, `[]` or `as` on request- or storage-derived values (section 10 lint list).
- **Subrequests are bounded by pack bytes, never by wants.** `coalesce(.., GAP)` is 7.2; if it produces more reads than `ceil(bytes / 8 MiB)`, `coalesce(.., WINDOW)` groups entries by 8 MiB of offset space, so reads per pack are at most the window count plus nothing (a straddling entry extends the read, it does not add one). The projection in `plan_reads` against `ReqBudget` (7.1) turns "too many" into `Error::Budget`, HTTP 413 with one `ERR` pkt-line (section 10) before the first response byte, never the review's mid-band abort. On the Paid plan `max_subrequests = 9,000` covers 72 GB of packs.
- **Memory (6.5) and the GC race.** In flight: one `Read` buffer (at most 8 MiB plus one entry, 32 MiB max by 2.4), one 64 KiB frame, the bitmaps, the `reads` plan (16 bytes per entry) and `seen`. A `GcSweep` during the request cannot remove bytes: a dead pack's R2 key survives GRACE = 1 h (5), longer than `max_ms`; a short range read is therefore corruption, reported as band-3 `ERR` (section 10).
- Conformance scenarios this proof must pass (section 11): 3 (tag peeling), 10 (`blob:none` clone, then the checkout's promisor fetch), 11 (`deepen 1`, `shallow-info`), 12 (sparse pruning gives "only new objects"). Added scenario 17: `git clone --filter=blob:none` of a repo with 3,000 files, `git checkout` a branch that changes 2,500 of them; assert the single upload-pack POST carried 2,500 `want` lines, `x-ge-subrequests` is at most `ceil(pack_bytes / 8 MiB) + 1`, `fsck` clean. Added scenario 18: `git clone --depth 1`, then `git fetch --depth=3` and `git fetch --unshallow`; assert `shallow-info` carries `unshallow` for the old boundary and `.git/shallow` ends empty, `fsck` clean.

## Changes from the first pass
| First-pass finding (quoted) | How addressed |
|---|---|
| Blocker 1: "1,000-subrequest cap (R2 binding calls count) vs git's batched promisor fetches: git 2.24+ checkout prefetches ALL missing blobs in one `fetch --filter=blob:none --stdin` ... the fan-out fix is hand-waved" | Objects are pack entries, not R2 keys (2.1), so N wants are not N GETs. `plan_reads` + `coalesce`: 7.2 gap merging first, window grouping when that is more reads, so reads per pack <= `ceil(bytes / 8 MiB)`; the projection line `if budget.used.saturating_add(n) > budget.max_subrequests` answers 413 before any byte. The limit is 10,000 on Paid (7), `ReqBudget` 9,000, and no fan-out or service binding exists. Scenario 17 asserts the count. |
| Blocker 2: "Annotated tag objects are never emitted: type-4 wants pass the existence check but feed no query" | `send_set_shallow`: every want is marked at step 1 whatever its kind; the `while !tags.is_empty()` loop loads the tag, reads `TagRef::target_kind` and routes the target; nested tags loop again. Scenario 3. |
| Blocker 3: "Storage contract with siblings is inconsistent (objects/<sha> zlib'd vs ... uncompressed loose); the range-offset trick only works with the latter" | Pinned by CONTRACTS 2.1 to 2.3: one normalized pack per push, `objects(sha, pack_id, idx, offset, len, kind, size)`, entry bytes copied verbatim. `pack_chunk` copies `buf[start..start+len]` into band 1; no header skipping, no deflate. |
| Caveat: "Synchronous multi-second recursive CTE in plan() blocks the repo DO event loop; objs array must be paged (32 MiB RPC cap)" | No CTE and no RPC payload: the plan is bitmaps (9.5) plus a `reads` list inside the DO, and the response streams from the same DO (`/_do/fetch`, 1.3). Sync spans are one `lookup` of at most 10,000 ids (`CHUNK`) or one `expand_tree` pass between awaits; the gate opens at every `load`, so pushes interleave (read-only walk, nothing to lose). |
| Caveat: "Client `shallow` lines never stop the walk and deepen-relative/since/not are dropped while `shallow` is advertised, so --deepen/--unshallow yields wrong shallow-info" | the `continue` line `(deepen.is_some() && boundary) \|\| (deepen.is_none() && (is_ack \|\| is_cs))` plus the `INFINITE` seeding and the `unshallow` push implement `send_shallow_list`, `deepen` and `send_unshallow` of `upload-pack.c`; `deepen-since`, `deepen-not`, `deepen-relative` are `Error::Protocol` (400) in `parse_fetch` (section 12, protocol-v2-only), so `--deepen=N` fails with a clear message instead of a corrupt `.git/shallow`. Scenario 18. |
| Caveat: "No prefetch window: sequential GET+deflate is 10-30 s per 1,000 objects" | No per-object GET and no deflate: `pack_chunk` copies entries out of one 8 MiB read; 1,000 small blobs of one pack are typically one or two reads. A prefetch window across reads is not written (Known limits). |
| Caveat: "Haves ignored until composed with want-have-negotiation; include-tag, combine:, sparse:oid, blob:limit k/m suffixes absent" | Haves: `acks` from `lookup` stop the commit walk and seed `bases` for tree pruning. `include-tag`, `combine:`, `sparse:oid`, `tree:<n>` and `blob:limit` suffixes: not addressed because section 12 puts them out of scope; `parse_fetch` rejects them with 400 rather than misapplying them. |
| Caveat: "GC race (force-push + sweep) truncates an in-flight pack; stateless retry, no data loss" | Bytes cannot vanish mid-stream: R2 deletion needs `dead_at < now - GRACE` (5, Janitor step 3) and GRACE = 1 h exceeds `max_ms` = 240 s. A `lookup` miss for an object swept mid-walk (force-pushed away and unreachable) is `Error::Internal`, 500 or band-3 `ERR`; retry is stateless. |
| Review body: "`blob:limit` with `size < n`" and "`tree:0` sends commits only" | `blob:limit` follows the contract's `size > n` skip (9.5), one size class wider than git's `>= n`, a legal superset (Known limits). `tree:0` is dropped (section 12). |
| Review body: "`new Uint8Array([1, ...b.subarray(...)])` spreads 65 KB into an argument array per frame" | `Sideband::data` (protocol-v2-only) appends each frame once with `band_to_write`; `pack_chunk` hands it a slice of the read buffer. |
| Review body: "Web Crypto cannot hash incrementally; the pure-JS `Sha1` class is assumed" | `gix_hash::hasher(Sha1)` with `update` per entry and `try_finalize` for the trailer, the same hasher `PackWriter` uses (1.2). |

## Known limits
- **`tree:0` is not delivered.** Section 12 lists `tree:<n>` and `sparse:` as out of scope; `git clone --filter=tree:0` gets a 400 with `ERR fetch: unknown argument filter tree:0` from `parse_fetch`. The idea's title promises it; the foundation delivers `blob:none` and `blob:limit` only.
- **Command section cap bounds a checkout batch at about 20,000 blobs.** Section 6.3 caps the v2 command section at 1 MiB; a `want` line is 50 bytes on the wire, so a promisor fetch with more than about 20,900 wants is refused with 400 `ERR command section over 1 MiB`, and that checkout fails every time. git does not chunk the batch. Proposed write-back to 6.3 and 6.5: 16 MiB for `fetch` bodies (parsed once at the edge, once in the DO; both are `Vec<u8>` of the same size), which raises the bound to about 335,000 wants; the read plan for such a batch is still bounded by pack windows. Not made here because the contract wins.
- **`blob:limit` sends one size class more than git.** 9.5 says skip when `size > n`; git omits `size >= n`. Superset, harmless, and `blob:limit=<n>k/m/g` suffixes are 400 (section 12).
- **Deepen on an already-shallow clone resends the client's existing depth.** With `deepen`, the walk crosses acks (git does too for the depth computation) and marks every non-ack commit inside the depth, including ones the client has between its tip and its old boundary. `deepen-relative` (`--deepen=N`) would avoid the resend and is out of scope. Bounded by the requested depth.
- **Sparse pruning is a heuristic.** `bases` is capped at 64 edge trees and counterparts match by entry name; a file moved between directories, or a subtree present under an ack older than the 64 kept, is resent. Correct (superset), not minimal. Blobs of `bases` themselves are pruned only at the path level where they appear.
- **Caps.** 200,000 commits and 64 MiB of `MemFind` per request (9.3), 1,000,000 dedup ids (`seen`, about 40 MB) beyond which the request is 413 `fetch too large for this server; clone instead`. A fresh clone above the caps is precomputed-clone-pack's `ClonePlan`; an incremental fetch above them has no path in the foundation. The `reads` plan is 16 bytes per entry plus one `Vec` per read; `Index::entries_of` materialises the marked rows of a pack (56 bytes each) once per pack, so a 1,000,000-entry set peaks near 56 MB in that sync span, inside 128 MB with an 8 MiB read buffer and nothing else resident.
- **CPU and wall clock.** Every sync span is one `lookup` of at most 10,000 ids, one `expand_tree` pass over one chunk, or `plan_reads`; SQLite lookups of 1,000,000 ids across a walk are seconds of DO CPU inside `limits.cpu_ms = 300000`, and `max_ms` = 240 s is the wall bound. No prefetch window: reads are sequential (one in flight, 6.5), so a 2 GB pack streams at R2's single-stream rate. Not measured on a deployed Worker.
- **Unverified, day-1 list.** `TagRef::{target, target_kind}`, `CommitRefIter::tree_id`, `EntryMode::{is_tree, is_commit}` names against gix-object 0.64.1; `gix_hash::hasher`/`try_finalize`; `SqlStorage::exec` bindings; real R2 range reads over multi-GB packs (#6); the DO subrequest limit on a deployed Worker (#7); that a `--unshallow` request carries exactly `deepen 2147483647` on git 2.43, 2.45, 2.47 (read in `fetch-pack.c`, not traced).
- **Write-backs this proof needs.** `SendSet { packs: Vec<PackSlice>, reads: Vec<Read>, shallow, unshallow }` as the concrete shape of 9.5, with precomputed-clone-pack's `clone_plan` calling `plan_reads` before streaming; `send_set` gaining `client_shallow` (or `&FetchArgs`) in 1.4; `Index::pack_meta` and `Index::entries_of(&PackSlice)`; `Bucket::read_entries` taking `(ObjectId, ObjLoc)` pairs and a budget, and `read_range` charging through the caller (1.2, as every sibling assumes); `write_fetch_prelude` writing `unshallow <oid>` lines and an empty `shallow-info` section whenever `deepen` was given (git's `send_shallow_info` does); the `objects_pack` index serving `entries_of` by `idx` runs.

## Depends on
- protocol-v2-only
- want-have-negotiation
- refs-sqlite-objects-r2
- content-addressed-r2-keys
- streaming-pack-parser
- two-phase-push
- gc-and-repack-alarm
- precomputed-clone-pack
- in-do-object-cache
