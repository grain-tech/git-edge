//! pack::generate — send_set + write_pack, running inside RepoDo (CONTRACTS.md 1.4, 9).
//! Ported from research/proofs/partial-clone-filters.md.

use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use bstr::BString;
use gix_hash::ObjectId;
use gix_object::{CommitRefIter, Kind, TagRef, TreeRefIter};

use crate::error::Error;
use crate::repo_do::RepoDo;
use crate::store::{codec, keys, Bucket, Index, MemFind, ObjLoc, PackId};
use crate::wire::Filter;
use crate::ReqBudget;

pub const MAX_COMMITS: usize = 200_000;
pub const MAX_MEM: usize = 64 << 20;
pub const MAX_OBJECTS: usize = 1_000_000;
const CHUNK: usize = 10_000;
const INFINITE: u32 = 0x7fff_ffff; // git's INFINITE_DEPTH (--unshallow)
const GAP: u64 = 256 << 10;
const WINDOW: u64 = 8 << 20;
const TOO_BIG: &str = "fetch too large for this server; clone instead";

pub struct PackSlice {
    pub pack: PackId,
    pub count: u32,
    pub bytes: u64,
    pub bitmap: Vec<u8>,
}
pub struct Read {
    pub pack: usize,
    pub off: u64,
    pub len: u64,
    pub ents: Vec<(u64, u32)>,
}
#[derive(Default)]
pub struct SendSet {
    pub packs: Vec<PackSlice>,
    by_pack: HashMap<PackId, usize>,
    pub reads: Vec<Read>,
    pub shallow: Vec<ObjectId>,
    pub unshallow: Vec<ObjectId>,
    /// S3/T2: the walk path keeps every marked sha's raw hash bytes in one
    /// Vec, so plan_reads can re-resolve the whole set after a compaction
    /// swap retired a pack mid-fetch. no_walk_set leaves it None — no list,
    /// and it fails closed exactly as before.
    shas: Option<Vec<u8>>,
    hash_len: usize,
    /// S3: a mark landed on a pack that is no longer live — the plan must be
    /// rebuilt from the sha list rather than trusted.
    stale: bool,
}
impl SendSet {
    /// Sync, idempotent: one bit per objects.idx (2.3). S3: on the walk path a
    /// pack that is no longer live records the sha as stale instead of
    /// erroring — the bytes moved, the sha did not.
    fn mark(&mut self, idx: &Index<'_>, id: &ObjectId, loc: &ObjLoc) -> Result<(), Error> {
        self.mark_with(|p| idx.pack_meta(p), id, loc)
    }
    /// `mark` with the pack-meta lookup injected — the pure seam the unit
    /// tests use to drive the stale-mark path without a SqlStorage.
    fn mark_with(
        &mut self,
        pack_meta: impl Fn(&PackId) -> Result<(u32, u64), Error>,
        id: &ObjectId,
        loc: &ObjLoc,
    ) -> Result<(), Error> {
        if let Some(s) = &mut self.shas {
            s.extend_from_slice(id.as_bytes());
        }
        if let Some(&i) = self.by_pack.get(&loc.pack) {
            return set_bit(&mut self.packs, i, loc);
        }
        let (count, bytes) = match pack_meta(&loc.pack) {
            Ok(v) => v,
            Err(e) if self.shas.is_some() && e.message().ends_with("not live") => {
                self.stale = true;
                return Ok(());
            }
            Err(e) => return Err(e),
        };
        let i = push_slice(&mut self.packs, &mut self.by_pack, &loc.pack, count, bytes)?;
        set_bit(&mut self.packs, i, loc)
    }
    pub fn count(&self) -> u32 {
        self.packs.iter().flat_map(|p| &p.bitmap).map(|b| b.count_ones() as u32).sum()
    }
}

/// A fresh PackSlice for `pack` (bitmap sized off count) registered in
/// `by_pack` — shared by SendSet::mark and the S3 rebuild.
fn push_slice(
    packs: &mut Vec<PackSlice>,
    by_pack: &mut HashMap<PackId, usize>,
    pack: &PackId,
    count: u32,
    bytes: u64,
) -> Result<usize, Error> {
    let bits = usize::try_from(count).map_err(|_| Error::Internal("count".into()))?.div_ceil(8);
    packs.push(PackSlice { pack: pack.clone(), count, bytes, bitmap: vec![0; bits] });
    let i = packs.len().saturating_sub(1);
    by_pack.insert(pack.clone(), i);
    Ok(i)
}

/// The bitmap bit for `loc.idx` in packs[i] — shared by SendSet::mark and
/// the S3 rebuild.
fn set_bit(packs: &mut [PackSlice], i: usize, loc: &ObjLoc) -> Result<(), Error> {
    let byte = usize::try_from(loc.idx / 8).map_err(|_| Error::Internal("idx".into()))?;
    *packs
        .get_mut(i)
        .and_then(|p| p.bitmap.get_mut(byte))
        .ok_or_else(|| Error::Storage("idx past pack count".into()))? |= 1u8 << (loc.idx % 8);
    Ok(())
}

pub async fn send_set(
    d: &RepoDo,
    bucket: &Bucket,
    wants: &[ObjectId],
    haves: &[ObjectId],
    filter: Option<&Filter>,
    deepen: Option<u32>,
    deepen_since: Option<i64>,
    deepen_not: &[ObjectId],
    deepen_relative: bool,
    include_tag: bool,
    client_shallow: &[ObjectId],
    budget: &mut ReqBudget,
) -> Result<SendSet, Error> {
    let sql = d.sql();
    let cs: HashSet<ObjectId> = client_shallow.iter().copied().collect();
    let idx = Index(&sql);
    // step 1: unknown haves are dropped
    let acks: HashSet<ObjectId> =
        haves.iter().zip(idx.lookup(haves)?).filter_map(|(h, l)| l.map(|_| *h)).collect();
    let (mut set, mut mem, mut seen, mut buf) =
        (SendSet::default(), MemFind::default(), HashSet::<ObjectId>::new(), Vec::new());
    // T2: only the walk path keeps the marked-sha list — the rebuild's input
    set.shas = Some(Vec::new());
    set.hash_len = d.obj_kind()?.len_in_bytes();
    let (mut commits, mut trees, mut bases, mut edge, mut tags) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::<ObjectId>::new());
    // want commits are marked into the pack unconditionally (seed), so `sent` must
    // know about them even when the walk later treats them as excluded boundaries
    let mut sent: HashSet<ObjectId> = HashSet::new();
    for (id, loc) in wants.iter().zip(idx.lookup(wants)?) {
        // step 1: every want must be live
        let loc = loc.ok_or_else(|| Error::Protocol(format!("upload-pack: not our ref {id}")))?;
        set.mark(&idx, id, &loc)?;
        seen.insert(*id);
        match loc.kind {
            Kind::Commit => {
                commits.push(*id);
                sent.insert(*id);
            }
            Kind::Tree => trees.push(*id),
            Kind::Tag => tags.push(*id),
            Kind::Blob => {}
        }
    }
    while !tags.is_empty() {
        load(d, &idx, bucket, &mut mem, &tags, budget).await?; // peel, one round per nesting level
        for t in std::mem::take(&mut tags) {
            let tag = TagRef::from_bytes(find(&mem, &t, &mut buf)?, t.kind())
                .map_err(|e| Error::Storage(e.to_string()))?;
            match (tag.target_kind, tag.target()) {
                (Kind::Commit, x) => commits.push(x),
                (Kind::Tree, x) => trees.push(x),
                (Kind::Tag, x) => tags.push(x),
                (Kind::Blob, x) => {
                    if let Some(l) = idx.lookup(&[x])?.pop().flatten() {
                        set.mark(&idx, &x, &l)?;
                        seen.insert(x);
                    }
                }
            }
        }
        mem.clear();
    }
    // S3's interleaving oracle, upstream of before_plan: park after want/have
    // resolution so a compaction swap can retire the packs the seed marks
    // landed on before the tree/commit walk marks anything (test-only)
    crate::testhooks::failpoint_async(d, "fetch.walk.after_wants").await?;
    let cap = deepen.unwrap_or(u32::MAX); // deepen n: the want tips are depth 1
    // shallow mode = any of deepen / deepen-since / deepen-not active
    let shallow_mode = deepen.is_some() || deepen_since.is_some() || !deepen_not.is_empty();
    let mut mem = MemFind::default();
    let mut buf = Vec::new();
    // deepen-not excludes every commit reachable from its tips, not just the tips.
    // Bounded BFS over the excluded side; a too-big exclusion fails the request.
    let mut nots: HashSet<ObjectId> = HashSet::new();
    {
        let mut frontier: Vec<ObjectId> = deepen_not.to_vec();
        while !frontier.is_empty() {
            frontier.sort_unstable();
            frontier.dedup();
            frontier.retain(|c| !nots.contains(c));
            if frontier.is_empty() {
                break;
            }
            if nots.len().saturating_add(frontier.len()) > MAX_COMMITS {
                return Err(Error::Limit(TOO_BIG.into()));
            }
            // a deepen-not tip we don't hold excludes nothing — skip it
            frontier = frontier
                .iter()
                .zip(idx.lookup(&frontier)?)
                .filter_map(|(c, l)| l.map(|_| *c))
                .collect();
            if frontier.is_empty() {
                break;
            }
            // load_commits: prefetched commits stay in `mem` across rounds (7.4)
            let locs = load_commits(&idx, bucket, &mut mem, &frontier, budget).await?;
            let mut nxt = Vec::new();
            for (c, loc) in frontier.drain(..).zip(locs) {
                nots.insert(c);
                if loc.kind != Kind::Commit {
                    continue; // tag tips are already peeled by the client; ignore non-commits
                }
                let data = find(&mem, &c, &mut buf)?;
                for p in CommitRefIter::from_bytes(data, c.kind()).parent_ids() {
                    if !nots.contains(&p) {
                        nxt.push(p);
                    }
                }
            }
            frontier = nxt;
        }
    }
    if deepen == Some(INFINITE) {
        // --unshallow: descend below every *known* client shallow boundary; an id we
        // don't hold just isn't descended past
        let known: Vec<ObjectId> = cs
            .iter()
            .zip(idx.lookup(&cs.iter().copied().collect::<Vec<_>>())?)
            .filter_map(|(c, l)| l.map(|_| *c))
            .collect();
        commits.extend(known);
    }
    // Each queue entry carries its own depth from the want tips. In shallow modes the
    // walk descends THROUGH client-shallow commits and acks — the boundary computation
    // needs the graph below them (git's deepen does the same: a cs commit inside the
    // new boundary is unshallowed and its parents packed). cs commits are client-held:
    // never resent, and whether `unshallow` is emitted is decided post-walk once it is
    // known which parents landed client-side. In non-shallow fetches acks/cs are hard
    // walls — nothing below them can be of use to the client.
    let mut depth = HashMap::<ObjectId, u32>::new();
    let mut parents_of: HashMap<ObjectId, Vec<ObjectId>> = HashMap::new();
    let mut visited_cs: HashSet<ObjectId> = HashSet::new();
    // `counted` marks entries whose depth is the boundary depth: under absolute deepen
    // everything counts from the tips; under deepen-relative only commits BELOW a
    // client-shallow commit count (parents of a descended cs restart at 1), because
    // the boundary is relative to the client's shallow list — commits above it are
    // interior no matter how far from the tip they are.
    let mut queue: Vec<(ObjectId, u32, bool)> =
        commits.into_iter().map(|c| (c, 1, !deepen_relative)).collect();
    while !queue.is_empty() {
        // a commit reached both above and below a cs boundary is boundary-counted —
        // prefer the counted visit, then the shallower depth
        queue.sort_by_key(|(c, d, ct)| (*c, !*ct, *d));
        queue.dedup_by(|a, b| a.0 == b.0);
        queue.retain(|(c, _, _)| !depth.contains_key(c)); // first visit wins
        if depth.len().saturating_add(queue.len()) > MAX_COMMITS {
            return Err(Error::Limit(TOO_BIG.into()));
        }
        let ids: Vec<ObjectId> = queue.iter().map(|(c, _, _)| *c).collect();
        // load_commits keeps prefetched commits resident across levels (7.4)
        let (locs, mut next) = (load_commits(&idx, bucket, &mut mem, &ids, budget).await?, Vec::new());
        for ((c, d, counted), loc) in std::mem::take(&mut queue).into_iter().zip(locs) {
            let data = find(&mem, &c, &mut buf)?;
            depth.insert(c, d);
            let tree = CommitRefIter::from_bytes(data, c.kind()).tree_id().map_err(|e| Error::Storage(e.to_string()))?;
            let parents: Vec<ObjectId> = CommitRefIter::from_bytes(data, c.kind()).parent_ids().collect();
            if shallow_mode {
                parents_of.insert(c, parents.clone());
            }
            // Walls (unsent, unwalked): is_ack (client has it), is_not (excluded side),
            // since_boundary (older than the cutoff). is_cs is the client's existing
            // shallow boundary — client-held, never resent; under any shallow mode the
            // walk may descend beneath it. depth_boundary IS sent then stops.
            let is_ack = acks.contains(&c);
            let is_cs = cs.contains(&c);
            let is_not = nots.contains(&c);
            let since_boundary = deepen_since
                .map(|since| committer_ts(data).map(|t| t < since).unwrap_or(false))
                .unwrap_or(false);
            let depth_boundary =
                deepen.is_some() && deepen != Some(INFINITE) && counted && d >= cap;
            // A client-shallow commit the new boundary will pass: descend below it.
            // Under absolute deepen only interior cs descend (d < cap) — a cs at the
            // boundary stays the boundary; since/not decide interior-ness post-walk.
            let descend_past_cs = is_cs
                && shallow_mode
                && (deepen_relative || deepen == Some(INFINITE) || deepen.is_none() || d < cap);
            if descend_past_cs {
                visited_cs.insert(c);
                bases.push(tree); // client holds it: a valid sparse-edge base
                // below a client-shallow commit relative depth restarts at 1; absolute
                // deepen keeps counting from the tips through the held commit
                let nd = if deepen_relative || deepen == Some(INFINITE) { 1 } else { d.saturating_add(1) };
                for p in parents {
                    next.push((p, nd, true));
                }
                continue;
            }
            if is_ack || is_cs {
                bases.push(tree); // provably client-side: a valid sparse-edge base
            } else if !is_not && !since_boundary {
                set.mark(&idx, &c, &loc)?;
                sent.insert(c);
            }
            if sent.contains(&c) {
                trees.push(tree); // a sent commit still needs its tree expanded
            }
            // walls: excluded side, time cut, depth cap, any cs not descended, and
            // acks — except that in shallow modes the walk must pass acks to reach
            // client-shallow commits that sit below them
            let walled = is_not
                || since_boundary
                || depth_boundary
                || is_cs
                || (is_ack && !shallow_mode);
            if walled {
                continue;
            }
            for p in parents {
                if shallow_mode || !acks.contains(&p) {
                    next.push((p, d.saturating_add(1), counted));
                } else {
                    edge.push(p);
                }
            }
        }
        queue = next;
        // no mem.clear() — prefetched commits carry later levels (7.4); MAX_MEM bounds it
    }
    mem.clear();
    if shallow_mode {
        // unshallow: a visited cs commit is interior once every parent is sent, acked,
        // or itself client-shallow — one pass suffices because a cs parent is present
        // in the client's store whether or not it is unshallowed itself.
        for c in &visited_cs {
            let interior = parents_of
                .get(c)
                .map(|ps| ps.iter().all(|p| sent.contains(p) || acks.contains(p) || cs.contains(p)))
                .unwrap_or(false);
            if interior {
                set.unshallow.push(*c);
            }
        }
        // The client's new bottom edge: a sent commit with any parent the client will
        // not have (excluded side, since cut, depth cap, or unseen). Acks and cs are
        // never emitted — the client holds them with their ancestry, and re-marking
        // them shallow would truncate its history (git's send_shallow skips them).
        for c in &sent {
            if acks.contains(c) || cs.contains(c) {
                continue;
            }
            let Some(ps) = parents_of.get(c) else {
                continue;
            };
            let cut = ps
                .iter()
                .any(|p| !sent.contains(p) && !acks.contains(p) && !cs.contains(p));
            if cut {
                set.shallow.push(*c);
            }
        }
    }
    edge.sort_unstable();
    edge.dedup();
    edge.truncate(64);
    load(d, &idx, bucket, &mut mem, &edge, budget).await?;
    for e in &edge {
        bases.push(
            CommitRefIter::from_bytes(find(&mem, e, &mut buf)?, e.kind())
                .tree_id()
                .map_err(|e| Error::Storage(e.to_string()))?,
        );
    }
    mem.clear();
    bases.sort_unstable();
    bases.dedup();
    bases.truncate(64);
    trees.sort_unstable();
    trees.dedup();
    // every initial item shares the same base set — share it via Rc instead of
    // cloning a ≤64-oid Vec per tree (200k trees × ~1.3 KiB was a real spike)
    let bases = Rc::new(bases);
    let mut items: Vec<(ObjectId, Rc<Vec<ObjectId>>)> =
        trees.into_iter().map(|t| (t, Rc::clone(&bases))).collect();
    while !items.is_empty() {
        let (mut next, mut blobs) = (Vec::new(), Vec::new());
        for chunk in items.chunks(CHUNK) {
            let ids: Vec<ObjectId> = chunk.iter().map(|(t, _)| *t).collect();
            let mut bs: Vec<ObjectId> = chunk.iter().flat_map(|(_, b)| b.iter().copied()).collect();
            bs.sort_unstable();
            bs.dedup();
            for (id, loc) in ids.iter().zip(load(d, &idx, bucket, &mut mem, &ids, budget).await?) {
                set.mark(&idx, id, &loc)?;
            }
            load(d, &idx, bucket, &mut mem, &bs, budget).await?;
            for (t, b) in chunk {
                expand_tree(&mem, t, b.as_slice(), filter, &mut seen, &mut next, &mut blobs)?;
            }
            mem.clear();
        }
        for (b, loc) in blobs.iter().zip(idx.lookup(&blobs)?) {
            let loc = loc.ok_or_else(|| Error::Internal(format!("reachable blob {b} is not live")))?;
            if !matches!(filter, Some(Filter::BlobLimit(n)) if loc.size > *n) {
                set.mark(&idx, b, &loc)?;
            }
        }
        items = next;
    }
    if include_tag {
        // include-tag: every tag object whose peeled target the client will hold rides
        // along — that means sent, already-had, or an existing client shallow boundary,
        // never a walked-but-unsent wall (excluded side / pre-cutoff)
        for r in exec_refs_tags(&sql)? {
            let (Ok(peeled), Ok(tag)) = (
                r.peeled.as_deref().map(|p| ObjectId::from_hex(p.as_bytes())).transpose(),
                ObjectId::from_hex(r.target.as_bytes()),
            ) else {
                continue;
            };
            if let Some(peeled) = peeled {
                if sent.contains(&peeled) || acks.contains(&peeled) || cs.contains(&peeled) {
                    if let Some(l) = idx.lookup(&[tag])?.into_iter().flatten().next() {
                        set.mark(&idx, &tag, &l)?;
                    }
                }
            }
        }
    }
    // S3's interleaving oracle: park the walk between mark and plan so a
    // compaction swap can land under it (test-only failpoint)
    crate::testhooks::failpoint_async(d, "fetch.walk.before_plan").await?;
    plan_reads(&idx, &mut set, budget)?;
    Ok(set)
}

fn exec_refs_tags(sql: &worker::SqlStorage) -> Result<Vec<TagPair>, Error> {
    crate::store::exec(
        sql,
        "SELECT target, peeled FROM refs WHERE name LIKE 'refs/tags/%' AND peeled IS NOT NULL",
        vec![],
    )?
    .to_array::<TagPair>()
    .map_err(|e| Error::Storage(e.to_string()))
}
#[derive(serde::Deserialize)]
struct TagPair {
    target: String,
    peeled: Option<String>,
}
/// `<ts>` of a commit's `committer` header: "committer N <e> <ts> <tz>".
fn committer_ts(data: &[u8]) -> Option<i64> {
    for line in data.split(|b| *b == b'\n') {
        if line.is_empty() {
            break; // headers end at the blank line
        }
        if let Some(rest) = line.strip_prefix(b"committer ") {
            let parts: Vec<&[u8]> = rest.split(|b| *b == b' ').collect();
            let n = parts.len();
            return std::str::from_utf8(parts.get(n.checked_sub(2)?)?).ok()?.parse().ok();
        }
    }
    None
}

/// One round of the section 9 loop: sync lookup, one coalesced async read (7.2), decode into mem.
async fn load(
    d: &RepoDo,
    idx: &Index<'_>,
    bucket: &Bucket,
    mem: &mut MemFind,
    ids: &[ObjectId],
    budget: &mut ReqBudget,
) -> Result<Vec<ObjLoc>, Error> {
    let locs: Vec<(ObjectId, ObjLoc)> = ids
        .iter()
        .zip(idx.lookup(ids)?)
        .map(|(id, l)| l.map(|l| (*id, l)).ok_or_else(|| Error::Internal(format!("reachable {id} is not live"))))
        .collect::<Result<_, _>>()?;
    for (id, entry) in bucket.read_entries_chunked(&locs, budget).await? {
        let (k, data) = codec::decode_entry(&entry)?;
        mem.insert(id, k, data);
        if mem.bytes > MAX_MEM {
            // bail inside the loop: a decoded round must never exceed the budget
            return Err(Error::Limit(TOO_BIG.into()));
        }
    }
    // S3's interleaving oracle: the lookup above already resolved `locs` but
    // the caller's set.mark lands after this point — a swap parked here
    // retires the packs the marks are about to name (test-only failpoint)
    if !locs.is_empty() {
        crate::testhooks::failpoint_async(d, "fetch.walk.after_load").await?;
    }
    Ok(locs.into_iter().map(|(_, l)| l).collect())
}

/// 7.4 commit-region prefetch. A commit walk otherwise pays one coalesced read per BFS
/// level — ~1 subrequest per commit on a linear history, hitting the subrequest budget
/// around ~9k depth. Pushed packs lay commits out near-contiguously, so each level's
/// span is extended by PREFETCH on both sides and every commit entry inside rides the
/// same range read; later levels then resolve from `mem` with no read at all. `mem` must
/// NOT be cleared between levels for this to help (bounded by MAX_MEM / MAX_COMMITS).
const PREFETCH: u64 = 2 << 20;

async fn load_commits(
    idx: &Index<'_>,
    bucket: &Bucket,
    mem: &mut MemFind,
    ids: &[ObjectId],
    budget: &mut ReqBudget,
) -> Result<Vec<ObjLoc>, Error> {
    let locs: Vec<(ObjectId, ObjLoc)> = ids
        .iter()
        .zip(idx.lookup(ids)?)
        .map(|(id, l)| l.map(|l| (*id, l)).ok_or_else(|| Error::Internal(format!("reachable {id} is not live"))))
        .collect::<Result<_, _>>()?;
    let mut missing: Vec<(ObjectId, ObjLoc)> =
        locs.iter().filter(|(id, _)| !mem.exists(id)).cloned().collect();
    if !missing.is_empty() {
        // extend each touched pack's needed span by PREFETCH, clipped to the pack size
        let mut spans: HashMap<PackId, (u64, u64)> = HashMap::new();
        for (_, l) in &missing {
            let e = spans
                .entry(l.pack.clone())
                .or_insert((l.offset, l.offset.saturating_add(u64::from(l.len))));
            e.0 = e.0.min(l.offset);
            e.1 = e.1.max(l.offset.saturating_add(u64::from(l.len)));
        }
        for (pack, (lo, hi)) in spans {
            let (_, bytes) = idx.pack_meta(&pack)?;
            let lo = lo.saturating_sub(PREFETCH);
            let hi = hi.saturating_add(PREFETCH).min(bytes);
            for pair in idx.commits_in_range(&pack, lo, hi)? {
                if !mem.exists(&pair.0) {
                    missing.push(pair);
                }
            }
        }
        missing.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        missing.dedup_by(|a, b| a.0 == b.0);
        // a wide history spread over many packs can prefetch past the read-batch
        // bound — chunked splits the set on Limit rather than failing the fetch
        for (id, entry) in bucket.read_entries_chunked(&missing, budget).await? {
            let (k, data) = codec::decode_entry(&entry)?;
            mem.insert(id, k, data);
            if mem.bytes > MAX_MEM {
                return Err(Error::Limit(TOO_BIG.into()));
            }
        }
    }
    Ok(locs.into_iter().map(|(_, l)| l).collect())
}

fn find<'a>(mem: &'a MemFind, id: &ObjectId, _buf: &mut Vec<u8>) -> Result<&'a [u8], Error> {
    mem.get(id).map(|(_, d)| d).ok_or_else(|| Error::Storage(format!("entry {id} not read")))
}

/// Sync. git's sparse edge pruning: an id held by a counterpart tree is had by the client.
#[allow(clippy::too_many_arguments)]
fn expand_tree(
    mem: &MemFind,
    tree: &ObjectId,
    bases: &[ObjectId],
    filter: Option<&Filter>,
    seen: &mut HashSet<ObjectId>,
    next: &mut Vec<(ObjectId, Rc<Vec<ObjectId>>)>,
    blobs: &mut Vec<ObjectId>,
) -> Result<(), Error> {
    let (mut had, mut by_name, mut buf) = (HashSet::new(), HashMap::<BString, ObjectId>::new(), Vec::new());
    for b in bases {
        for e in TreeRefIter::from_bytes(find(mem, b, &mut buf)?, b.kind()) {
            let e = e.map_err(|e| Error::Storage(e.to_string()))?;
            had.insert(e.oid.to_owned());
            if e.mode.is_tree() {
                by_name.insert(e.filename.to_owned(), e.oid.to_owned());
            }
        }
    }
    for e in TreeRefIter::from_bytes(find(mem, tree, &mut buf)?, tree.kind()) {
        let (e, oid) = {
            let e = e.map_err(|e| Error::Storage(e.to_string()))?;
            (e, e.oid.to_owned())
        };
        if e.mode.is_commit() || had.contains(&oid) || !seen.insert(oid) {
            continue; // gitlink, had by the client, or queued already
        }
        if seen.len() > MAX_OBJECTS {
            return Err(Error::Limit(TOO_BIG.into()));
        }
        if e.mode.is_tree() {
            next.push((oid, Rc::new(by_name.get(e.filename).into_iter().copied().collect())));
        } else if !matches!(filter, Some(Filter::BlobNone)) {
            blobs.push(oid);
        }
    }
    Ok(())
}

/// Sync, after the walk. 7.2 coalescing per pack; bound by the pack's 8 MiB window count.
/// One pack's ObjLoc list lives only for its own coalesce — a multi-pack clone
/// never holds every pack's locations at once (isolate memory).
pub fn plan_reads(idx: &Index<'_>, set: &mut SendSet, budget: &ReqBudget) -> Result<(), Error> {
    let mut reads = Vec::new();
    let mut rebuilt = false;
    // S3: stale marks may have left `packs` empty — every marked sha's pack
    // was retired before its mark landed — in which case the in-loop restart
    // could never fire. Rebuild up front, once, under the same once-only
    // contract as the restart below.
    if set.stale {
        rebuild(idx, set)?;
        rebuilt = true;
    }
    let mut pi = 0;
    while pi < set.packs.len() {
        let l = idx.entries_of(&set.packs[pi].pack, &set.packs[pi].bitmap)?;
        // a gc_sweep/abort or a compaction swap landing between mark and plan
        // deletes `objects` rows out from under the in-memory bitmap — without
        // this check the PACK header count would exceed the entries emitted
        // and the response is wire-corrupt
        let marked: usize = set.packs[pi].bitmap.iter().map(|b| b.count_ones() as usize).sum();
        if l.len() != marked {
            if rebuilt {
                return Err(Error::Storage("pack index changed mid-fetch".into()));
            }
            // S3/R8: ONE rebuild from the recorded sha list — compaction keeps the
            // live SHA set, so the plan re-lands on the merged pack; after a real
            // GC sweep a missing sha still fails closed as before.
            rebuild(idx, set)?;
            reads.clear();
            rebuilt = true;
            pi = 0;
            continue;
        }
        let plan = coalesce(pi, &l, GAP)?;
        reads.extend(if u64::try_from(plan.len()).unwrap_or(u64::MAX)
            > set.packs[pi].bytes.div_ceil(WINDOW)
        {
            coalesce(pi, &l, WINDOW)?
        } else {
            plan
        });
        pi += 1;
    }
    let n = u32::try_from(reads.len()).map_err(|_| Error::Budget)?;
    if budget.used.saturating_add(n) > budget.max_subrequests {
        return Err(Error::Budget); // projection: never a mid-stream abort
    }
    set.reads = reads;
    Ok(())
}

/// S3/U2: re-resolve every marked sha against the CURRENT live index —
/// `lookup` batches at 5000 shas so each batch's map frees before the next —
/// and rebuild the per-pack bitmaps from scratch. The header count is the
/// rebuilt set's popcount, taken after this runs.
fn rebuild(idx: &Index<'_>, set: &mut SendSet) -> Result<(), Error> {
    rebuild_with(set, |ids| idx.lookup(ids), |p| idx.pack_meta(p))
}

fn object_ids_from_sha_bytes(chunk: &[u8], hash_len: usize) -> Result<Vec<ObjectId>, Error> {
    let mut ids = Vec::with_capacity(chunk.len() / hash_len);
    for b in chunk.chunks(hash_len) {
        let hex: String = b.iter().map(|x| format!("{x:02x}")).collect();
        ids.push(crate::store::oid(&hex)?);
    }
    Ok(ids)
}

fn rebuild_mark_loc(
    packs: &mut Vec<PackSlice>,
    by_pack: &mut HashMap<PackId, usize>,
    pack_meta: &impl Fn(&PackId) -> Result<(u32, u64), Error>,
    loc: &ObjLoc,
) -> Result<usize, Error> {
    match by_pack.get(&loc.pack) {
        Some(&i) => Ok(i),
        None => {
            let (count, bytes) = pack_meta(&loc.pack)?;
            push_slice(packs, by_pack, &loc.pack, count, bytes)
        }
    }
}

/// `rebuild` with the index lookups injected — the pure seam the unit tests
/// use to drive the re-resolve path without a SqlStorage.
fn rebuild_with(
    set: &mut SendSet,
    lookup: impl Fn(&[ObjectId]) -> Result<Vec<Option<ObjLoc>>, Error>,
    pack_meta: impl Fn(&PackId) -> Result<(u32, u64), Error>,
) -> Result<(), Error> {
    let (Some(shas), true) = (&set.shas, set.hash_len > 0) else {
        // no sha list (no_walk_set) — fails closed exactly as before
        return Err(Error::Storage("pack index changed mid-fetch".into()));
    };
    let mut packs: Vec<PackSlice> = Vec::new();
    let mut by_pack: HashMap<PackId, usize> = HashMap::new();
    for chunk in shas.chunks(5_000 * set.hash_len) {
        let ids = object_ids_from_sha_bytes(chunk, set.hash_len)?;
        for l in lookup(&ids)?.into_iter() {
            let l = l.ok_or_else(|| Error::Storage("pack index changed mid-fetch".into()))?;
            let i = rebuild_mark_loc(&mut packs, &mut by_pack, &pack_meta, &l)?;
            set_bit(&mut packs, i, &l)?;
        }
    }
    set.packs = packs;
    set.by_pack = by_pack;
    set.stale = false;
    Ok(())
}

fn coalesce(pack: usize, locs: &[ObjLoc], gap: u64) -> Result<Vec<Read>, Error> {
    let mut out: Vec<Read> = Vec::new();
    for l in locs {
        let end = l.offset.saturating_add(u64::from(l.len));
        match out.last_mut() {
            // pack_chunk materializes a whole Read in one range read, so a merged
            // span must never exceed WINDOW — check the new end, not the start
            Some(r)
                if l.offset.saturating_sub(r.off.saturating_add(r.len)) < gap
                    && end.saturating_sub(r.off) <= WINDOW =>
            {
                r.len = end.saturating_sub(r.off);
                r.ents.push((l.offset, l.len));
            }
            _ => {
                // an entry (or remainder) too big for one read is emitted as
                // WINDOW-sized fragments; ents carry byte ranges, so a fragment
                // of one entry is just a smaller copy span — output stays
                // byte-identical and the trailer hash sees the same bytes in order
                let mut pos = l.offset;
                while pos < end {
                    let n = end.saturating_sub(pos).min(WINDOW);
                    out.push(Read {
                        pack,
                        off: pos,
                        len: n,
                        // n <= WINDOW = 8 MiB — the cast cannot fail today, but a
                        // saturating/panicking fallback would hide a broken
                        // invariant: surface Error::Limit (HTTP 413) instead (A21)
                        ents: vec![(
                            pos,
                            u32::try_from(n)
                                .map_err(|_| Error::Limit("read fragment exceeds window".into()))?,
                        )],
                    });
                    pos = pos.saturating_add(n);
                }
            }
        }
    }
    Ok(out)
}

/// One step of the response stream: one range read, marked entries copied verbatim (2.1).
pub async fn pack_chunk(
    bucket: &Bucket,
    set: &SendSet,
    i: usize,
    h: &mut gix_hash::Hasher,
    budget: &mut ReqBudget,
) -> Result<Vec<u8>, Error> {
    let Some(r) = set.reads.get(i) else { return Ok(Vec::new()) };
    let pack = set.packs.get(r.pack).ok_or_else(|| Error::Internal("read past packs".into()))?;
    let buf = bucket.read_range(&keys::pack(&bucket.repo, &pack.pack), r.off, r.len, budget).await?;
    let mut out = Vec::new();
    for &(off, len) in &r.ents {
        let start = usize::try_from(off.saturating_sub(r.off)).map_err(|_| Error::Internal("offset".into()))?;
        let e = buf
            .get(start..start.saturating_add(usize::try_from(len).unwrap_or(usize::MAX)))
            .ok_or_else(|| Error::Storage("short range read".into()))?;
        h.update(e);
        out.extend_from_slice(e);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn oid(hex: &str) -> ObjectId {
        ObjectId::from_hex(hex.as_bytes()).unwrap()
    }

    fn loc(pack: &str, idx: u32) -> ObjLoc {
        ObjLoc {
            pack: PackId(pack.into()),
            idx,
            offset: 0,
            len: 1,
            kind: Kind::Blob,
            size: 1,
        }
    }

    /// Kills no_stale: a mark landing on a pack that is not live records the
    /// sha and flags the set stale (the bytes moved, the sha did not) instead
    /// of erroring — and with `packs` left empty the rebuild path must still
    /// run: re-resolving the recorded sha list lands every mark on the merged
    /// pack's fresh slice and clears the flag.
    #[test]
    fn stale_mark_rebuilds_onto_merged_pack() {
        let id = oid("1111111111111111111111111111111111111111");
        let mut set = SendSet::default();
        set.shas = Some(Vec::new());
        set.hash_len = 20;
        set.mark_with(
            |p| Err(Error::Storage(format!("pack {} not live", p.0))),
            &id,
            &loc("dead", 0),
        )
        .unwrap();
        assert!(set.stale);
        assert!(set.packs.is_empty());
        assert_eq!(set.shas.as_ref().unwrap().len(), 20);

        let live = loc("merged", 3);
        rebuild_with(
            &mut set,
            |ids| {
                assert_eq!(ids.len(), 1);
                assert_eq!(ids[0], id);
                Ok(vec![Some(live.clone())])
            },
            |p| {
                assert_eq!(p.0, "merged");
                Ok((8, 100))
            },
        )
        .unwrap();
        assert!(!set.stale);
        assert_eq!(set.count(), 1);
        assert_eq!(set.packs[0].pack.0, "merged");
        assert_eq!(set.packs[0].bitmap, vec![1u8 << 3]);
    }

    /// The stale path is strictly "pack … not live" plus a recorded sha list:
    /// the same error fails closed on the no_walk path (no shas), and any
    /// other pack_meta failure propagates on the walk path.
    #[test]
    fn mark_propagates_non_stale_errors() {
        let id = oid("2222222222222222222222222222222222222222");
        let not_live = |p: &PackId| -> Result<(u32, u64), Error> {
            Err(Error::Storage(format!("pack {} not live", p.0)))
        };
        // no_walk_set: no sha list -> the same "not live" is a hard error
        let mut set = SendSet::default();
        assert!(set.mark_with(not_live, &id, &loc("dead", 0)).is_err());
        assert!(!set.stale);
        // walk path: a pack_meta failure that is not "not live" propagates
        let mut set = SendSet::default();
        set.shas = Some(Vec::new());
        set.hash_len = 20;
        assert!(set
            .mark_with(|_| Err(Error::Internal("boom".into())), &id, &loc("dead", 0))
            .is_err());
        assert!(!set.stale);
        // ...and a live pack marks the bit without touching stale
        set.mark_with(|_| Ok((4, 10)), &id, &loc("live", 1)).unwrap();
        assert!(!set.stale);
        assert_eq!(set.count(), 1);
    }
}


