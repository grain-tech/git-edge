//! pack::generate — send_set + write_pack, running inside RepoDo (CONTRACTS.md 1.4, 9).
//! Ported from proofs-v2/partial-clone-filters.md.

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
}
impl SendSet {
    /// Sync, idempotent: one bit per objects.idx (2.3).
    fn mark(&mut self, idx: &Index<'_>, loc: &ObjLoc) -> Result<(), Error> {
        let i = match self.by_pack.get(&loc.pack) {
            Some(&i) => i,
            None => {
                let (count, bytes) = idx.pack_meta(&loc.pack)?;
                let bits = usize::try_from(count).map_err(|_| Error::Internal("count".into()))?.div_ceil(8);
                self.packs.push(PackSlice { pack: loc.pack.clone(), count, bytes, bitmap: vec![0; bits] });
                let i = self.packs.len().saturating_sub(1);
                self.by_pack.insert(loc.pack.clone(), i);
                i
            }
        };
        let byte = usize::try_from(loc.idx / 8).map_err(|_| Error::Internal("idx".into()))?;
        *self
            .packs
            .get_mut(i)
            .and_then(|p| p.bitmap.get_mut(byte))
            .ok_or_else(|| Error::Storage("idx past pack count".into()))? |= 1u8 << (loc.idx % 8);
        Ok(())
    }
    pub fn count(&self) -> u32 {
        self.packs.iter().flat_map(|p| &p.bitmap).map(|b| b.count_ones() as u32).sum()
    }
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
    let (mut commits, mut trees, mut bases, mut edge, mut tags) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::<ObjectId>::new());
    // want commits are marked into the pack unconditionally (seed), so `sent` must
    // know about them even when the walk later treats them as excluded boundaries
    let mut sent: HashSet<ObjectId> = HashSet::new();
    for (id, loc) in wants.iter().zip(idx.lookup(wants)?) {
        // step 1: every want must be live
        let loc = loc.ok_or_else(|| Error::Protocol(format!("upload-pack: not our ref {id}")))?;
        set.mark(&idx, &loc)?;
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
        load(&idx, bucket, &mut mem, &tags, budget).await?; // peel, one round per nesting level
        for t in std::mem::take(&mut tags) {
            let tag = TagRef::from_bytes(find(&mem, &t, &mut buf)?, t.kind())
                .map_err(|e| Error::Storage(e.to_string()))?;
            match (tag.target_kind, tag.target()) {
                (Kind::Commit, x) => commits.push(x),
                (Kind::Tree, x) => trees.push(x),
                (Kind::Tag, x) => tags.push(x),
                (Kind::Blob, x) => {
                    if let Some(l) = idx.lookup(&[x])?.pop().flatten() {
                        set.mark(&idx, &l)?;
                        seen.insert(x);
                    }
                }
            }
        }
        mem.clear();
    }
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
                set.mark(&idx, &loc)?;
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
    load(&idx, bucket, &mut mem, &edge, budget).await?;
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
            for loc in load(&idx, bucket, &mut mem, &ids, budget).await? {
                set.mark(&idx, &loc)?;
            }
            load(&idx, bucket, &mut mem, &bs, budget).await?;
            for (t, b) in chunk {
                expand_tree(&mem, t, b.as_slice(), filter, &mut seen, &mut next, &mut blobs)?;
            }
            mem.clear();
        }
        for (b, loc) in blobs.iter().zip(idx.lookup(&blobs)?) {
            let loc = loc.ok_or_else(|| Error::Internal(format!("reachable blob {b} is not live")))?;
            if !matches!(filter, Some(Filter::BlobLimit(n)) if loc.size > *n) {
                set.mark(&idx, &loc)?;
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
                        set.mark(&idx, &l)?;
                    }
                }
            }
        }
    }
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
pub fn plan_reads(idx: &Index<'_>, set: &mut SendSet, budget: &ReqBudget) -> Result<(), Error> {
    let mut reads = Vec::new();
    for (pi, p) in set.packs.iter().enumerate() {
        let locs = idx.entries_of(&p.pack, &p.bitmap)?;
        // a gc_sweep/abort landing between mark and plan deletes `objects` rows out
        // from under the in-memory bitmap — without this check the PACK header count
        // would exceed the entries emitted and the response is wire-corrupt
        let marked: usize = p.bitmap.iter().map(|b| b.count_ones() as usize).sum();
        if locs.len() != marked {
            return Err(Error::Storage("pack index changed mid-fetch".into()));
        }
        let plan = coalesce(pi, &locs, GAP)?;
        reads.extend(if u64::try_from(plan.len()).unwrap_or(u64::MAX) > p.bytes.div_ceil(WINDOW) {
            coalesce(pi, &locs, WINDOW)?
        } else {
            plan
        });
    }
    let n = u32::try_from(reads.len()).map_err(|_| Error::Budget)?;
    if budget.used.saturating_add(n) > budget.max_subrequests {
        return Err(Error::Budget); // projection: never a mid-stream abort
    }
    set.reads = reads;
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


