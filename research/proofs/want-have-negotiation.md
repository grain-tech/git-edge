# Want/have negotiation with a commit-graph in SQLite

> Second pass · Idea #56 · verdict: **risky** · feasibility 4/5 · reliability 3/5 · correctness 3/5 (first pass 4/3/3)
> First pass: [proof](./archived/want-have-negotiation.md) · [review](../reviews/archived/want-have-negotiation.md) · Second pass: [review](../reviews/want-have-negotiation.md) · Contract: [CONTRACTS.md](../../CONTRACTS.md)

## Mechanism
Section 12 names "commit-graph tables in SQLite (`commits`, `introduced`)" as out of scope for the foundation, and section 9 steps 3-5 prescribe the fallback: prefetch commit objects out of pack regions and walk them in rounds. This module adds exactly those two tables plus a `parents` JSON column inside `commits` via a REGISTRY block (amendment A9), fills them at push time, and swaps the object-reading steps of `RepoDo::fetch_v2` for pure SQL. Push side: pass B of `pack::ingest` feeds every decoded commit and tree to `GraphBuild::feed` (one added line in `resolve_and_normalize`); after `PackWriter::finish`, the tail index post and the section 2.5 connectivity lookups — and before `/_do/push/commit` (section 3 ordering) — `post` computes generation numbers by a topological sort in the edge (external parents' gens and root trees come from one new DO route `/_do/graph/commits`), fills `introduced` by an n-way path diff of each commit's root tree against its parents', and posts both row sets to `/_do/graph/index` in 10,000-row bodies. Fetch side: `fetch_v2` keeps section 9 steps 1-2 (`Index::lookup` on wants, the readiness rule) and the rule-3 response shape owned by `write_fetch_prelude` (protocol-v2-only); steps 3-5 become `negotiate`, a two-colour generation-ordered walk over `commits` rows, and `graph_send_set`, an `introduced ⋈ objects ⋈ packs(live)` join that emits the same `SendSet` bitmaps `plan_reads`/`write_pack` consume (partial-clone-filters). Any request carrying `deepen`, `shallow`, a `want <tree>`, or any coverage hole — a want with no `commits` row, a missing parent row mid-walk, a join that loses an oid — falls back to `send_set_shallow`, so nothing the contract advertises is dropped or silently misapplied. No object bytes are read to decide what to send: the idea survives intact. `precomputed-clone-pack`'s gate still runs first when a plan exists, and the 7.4 commit-region cache remains the fallback path's mechanism.

## Primitives
- `SqlStorage::exec` synchronous with `SqlCursor::{to_array, one}` inside the DO: verified (memo section 1, spike). Sync-span atomicity for `graph_index`, `graph_commits`, `negotiate` and `graph_send_set` (all `fn`, zero awaits): measured (platform-facts #4). Errors propagate as `Err` out of `fetch` (A2).
- Bound parameters: every `IN (...)` is chunked at `PARAMS = 90` (A6, 100-param limit); `json_each` is not needed.
- `INSERT .. ON CONFLICT(sha) DO UPDATE .. WHERE` / `INSERT OR IGNORE`, `COUNT(DISTINCT ..)`, `GROUP BY` with a bare column per group: the SQLite >= 3.24 family the contract already relies on (section 3 step 4, A5).
- `gix_object::CommitRefIter::from_bytes(..).{tree_id, parent_ids}` and `TreeRefIter::from_bytes` with `EntryMode::{is_tree, is_commit}` (0.64.1): CI-built for wasm32 (memo section 3); `tree_id`/`parent_ids` are the calls contract 9.3 names; the iterator and mode names are per partial-clone-filters' primitives — **not run in the spike**.
- `Index::lookup` (2.3, live packs only), `Bucket::read_entries` coalesced range reads (7.2, A1 signature), `stub_json`/`lookup` stub helpers (two-phase-push): verified shape; `RequestInit` stub-body path **unverified at runtime** (same as two-phase-push).
- `SendSet`/`plan_reads`/`write_pack`/`pack_chunk` (pack::generate, partial-clone-filters) and `write_fetch_prelude` (protocol-v2-only): contract signatures; `Response::from_stream` **unverified at runtime**.
- `std::collections::BinaryHeap` as the generation-ordered priority queue: std; replaces the first pass's per-pop `Array.sort`.
- No `gix_traverse`: the graph walk needs no gitoxide traversal, so section 9 step 4's unverified `topo::Builder` constructor is moot here.
- `SELECT changes()` is not used by this module (no CAS on its tables); `rowsWritten` is never read (CI grep, section 3).

## Proof code
```rust
// src/graph/mod.rs -- CONTRACTS.md 1.1-1.4, 2.1-2.3, 2.5, 3, 7, 9, 10; amendments A1, A2, A5-A9. worker 0.8.5, gix-object 0.64.1, gix-hash 0.26.2.
// REGISTRY (A9), everything this module adds:
//   tables  commits(sha TEXT PRIMARY KEY, gen INTEGER NOT NULL, tree TEXT NOT NULL, parents TEXT NOT NULL) WITHOUT ROWID   -- parents: JSON hex array
//           introduced(commit TEXT NOT NULL, oid TEXT NOT NULL, PRIMARY KEY(commit,oid)) WITHOUT ROWID                   -- (c,c) is always a row
//   routes  POST /_do/graph/commits {ids:[hex <= 1000]} -> {rows:[{sha,gen,tree}]}                                        -- sparse: absent ids missing
//           POST /_do/graph/index   {push_id, commits:[{sha,gen,tree,parents:[hex]}], introduced:[[commit,oid]]} -> {}    -- <= 10,000 rows
//   JobKind: none. R2 prefixes: none. Nothing is deleted: rows are derived from commit bytes, so a dead push's rows stay correct.
use std::collections::{hash_map::Entry, BinaryHeap, HashMap, HashSet};
use gix_hash::ObjectId;
use gix_object::{CommitRefIter, Kind, TreeRefIter};
use worker::{Response, SqlStorageValue as V, Stub};
use crate::{edge::{lookup, stub_json, RepoRoute}, error::Error, pack::generate::SendSet, repo_do::RepoDo,
            store::{codec, Bucket, ObjLoc, PackId, PushId}, wire::{FetchArgs, Filter}, ReqBudget};
const PARAMS: usize = 90; const ROWS: usize = 10_000; const MAX_POPS: usize = 200_000;   // A6; 1.3 batch size; 9.3's commit bound
const GRAPH_CAP: usize = 64 << 20;                                                      // edge cap on held commit/tree child maps
type Kids = Vec<(Box<[u8]>, ObjectId, bool)>;                                           // (name, oid, is_tree); gitlinks never stored
fn hexes(ids: &[ObjectId]) -> Vec<String> { ids.iter().map(ToString::to_string).collect() }
fn hex_oid(s: &str) -> Result<ObjectId, Error> { ObjectId::from_hex(s.as_bytes()).map_err(|_| Error::Internal(format!("bad oid {s}"))) }
fn kids_of(data: &[u8]) -> Result<Kids, Error> {
    let mut v = Kids::new();
    for e in TreeRefIter::from_bytes(data) { let e = e.map_err(|e| Error::Unpack(e.to_string()))?;
        if !e.mode.is_commit() { v.push((e.filename.to_vec().into_boxed_slice(), e.oid.to_owned(), e.mode.is_tree())); } }
    Ok(v)
}

// ---- edge half. resolve_and_normalize calls feed() once per decoded entry (write-back); ingest::run calls post()
// after the 2.5 connectivity lookups and before /_do/push/commit, so only connectivity-verified pushes write rows.
#[derive(Default)] pub struct GraphBuild { commits: HashMap<ObjectId, (ObjectId, Vec<ObjectId>)>,   // sha -> (root tree, parents)
    kids: HashMap<ObjectId, Kids>, bytes: usize, dead: bool }
impl GraphBuild {
    pub fn feed(&mut self, id: ObjectId, kind: Kind, data: &[u8]) -> Result<(), Error> {   // sync gitoxide parse (section 9 rule)
        if self.dead { return Ok(()); }
        match kind {
            Kind::Commit => { let t = CommitRefIter::from_bytes(data).tree_id().map_err(|e| Error::Unpack(e.to_string()))?;
                              self.commits.insert(id, (t, CommitRefIter::from_bytes(data).parent_ids().collect())); }
            Kind::Tree => { self.kids.insert(id, kids_of(data)?); }
            _ => {}
        }
        self.bytes = self.bytes.saturating_add(data.len());
        if self.bytes > GRAPH_CAP { self.dead = true; }          // oversized push writes no rows; fetches take 9.3-5
        Ok(())
    }
    /// Ok(false) = nothing written. gen(c) = 1 + max(parent gen): in-pack parents resolve by Kahn (pack order is not
    /// topological); external parents come from /_do/graph/commits, absent ones get gen 0 and no tree (superset only).
    pub async fn post(&self, stub: &Stub, repo: &RepoRoute, bucket: &Bucket, push: &PushId, budget: &mut ReqBudget) -> Result<bool, Error> {
        if self.dead || self.commits.is_empty() { return Ok(false); }
        let mut ext: Vec<ObjectId> = self.commits.values().flat_map(|(_, ps)| ps.iter().copied().filter(|p| !self.commits.contains_key(p))).collect();
        ext.sort_unstable(); ext.dedup();
        let (mut gen, mut ptree) = (HashMap::<ObjectId, i64>::new(), HashMap::<ObjectId, ObjectId>::new());
        #[derive(serde::Deserialize)] struct PRow { sha: String, gen: i64, tree: String }
        #[derive(serde::Deserialize)] struct Rows { rows: Vec<PRow> }
        for chunk in ext.chunks(1_000) {
            let r: Rows = stub_json(stub, repo, "/_do/graph/commits", &serde_json::json!({ "ids": hexes(chunk) }), budget).await?;
            for row in r.rows { let id = hex_oid(&row.sha)?; gen.insert(id, row.gen); ptree.insert(id, hex_oid(&row.tree)?); }
        }
        let (mut indeg, mut kids_of_c) = (HashMap::<ObjectId, usize>::new(), HashMap::<ObjectId, Vec<ObjectId>>::new());
        for (c, (_, ps)) in &self.commits {
            indeg.insert(*c, ps.iter().filter(|p| self.commits.contains_key(p)).count());
            for p in ps { if self.commits.contains_key(p) { kids_of_c.entry(*p).or_default().push(*c); } }
        }
        let mut st: Vec<ObjectId> = indeg.iter().filter(|(_, n)| **n == 0).map(|(c, _)| *c).collect();
        while let Some(c) = st.pop() {
            let g = self.commits.get(&c).map(|(_, ps)| ps.iter().filter_map(|p| gen.get(p).copied()).max().unwrap_or(0)).unwrap_or(0).saturating_add(1);
            gen.insert(c, g);
            for ch in kids_of_c.get(&c).into_iter().flatten().copied() { if let Some(n) = indeg.get_mut(&ch) { *n = n.saturating_sub(1); if *n == 0 { st.push(ch); } } }
        }
        for c in indeg.keys() { gen.entry(*c).or_insert(0); }                              // cycle leftovers: impossible for real sha1 content
        let metas: Vec<serde_json::Value> = self.commits.iter().map(|(c, (t, ps))| serde_json::json!({ "sha": c.to_string(), "gen": gen.get(c).copied().unwrap_or(0),
            "tree": t.to_string(), "parents": ps.iter().map(ToString::to_string).collect::<Vec<_>>() })).collect();
        for chunk in metas.chunks(ROWS) { self.post_rows(stub, repo, push, chunk, &[], budget).await?; }   // commits first: a mid-crash leaves detectable holes
        let mut rows: Vec<[String; 2]> = Vec::new();
        for c in self.commits.keys() {
            for o in self.diff(*c, &ptree, stub, repo, bucket, budget).await? {
                rows.push([c.to_string(), o.to_string()]);
                if rows.len() >= ROWS { self.post_rows(stub, repo, push, &[], &rows, budget).await?; rows.clear(); }
            }
        }
        self.post_rows(stub, repo, push, &[], &rows, budget).await?;                   // introduced tail; empty rows is a legal post
        Ok(true)
    }
    async fn post_rows(&self, stub: &Stub, repo: &RepoRoute, push: &PushId, commits: &[serde_json::Value], introduced: &[[String; 2]],
                       budget: &mut ReqBudget) -> Result<(), Error> {
        let _: serde_json::Value = stub_json(stub, repo, "/_do/graph/index",
            &serde_json::json!({ "push_id": push.0, "commits": commits, "introduced": introduced }), budget).await?;
        Ok(())
    }
    /// introduced(c) = closure(tree(c)) \ U closure(tree(p)): an n-way path diff descending only where the commit's entry
    /// differs from every parent's entry at that name; an unmatched subtree is marked whole. Old trees on changed paths are
    /// fetched in coalesced section-9 rounds. Over-marks only where a shared object sits at different paths: legal superset (9.4).
    async fn diff(&self, c: ObjectId, ptree: &HashMap<ObjectId, ObjectId>, stub: &Stub, repo: &RepoRoute,
                  bucket: &Bucket, budget: &mut ReqBudget) -> Result<Vec<ObjectId>, Error> {
        enum W { Diff(ObjectId, Vec<ObjectId>), All(ObjectId) }
        let Some((root, ps)) = self.commits.get(&c) else { return Ok(vec![c]) };
        let vs: Vec<ObjectId> = ps.iter().filter_map(|p| self.commits.get(p).map(|(t, _)| *t).or_else(|| ptree.get(p).copied())).collect();
        let (mut out, mut seen, mut old) = (vec![c], HashSet::from([c]), HashMap::<ObjectId, Kids>::new());
        let mut stack = vec![W::Diff(*root, vs)];
        loop {
            let mut missing = Vec::new();
            while let Some(w) = stack.pop() {
                let kids = |t: &ObjectId| self.kids.get(t).or_else(|| old.get(t));
                match w {
                    W::All(t) => match kids(&t) {
                        None => { missing.push(t); stack.push(W::All(t)); }
                        Some(es) => { if seen.insert(t) { out.push(t); }
                            for (_, o, t2) in es { if *t2 { stack.push(W::All(*o)) } else if seen.insert(*o) { out.push(*o) } } } }
                    W::Diff(me, vs) => {
                        if vs.contains(&me) { continue; }                               // subtree identical to a parent's: nothing introduced
                        if kids(&me).is_none() || vs.iter().any(|v| kids(v).is_none()) {
                            missing.push(me); missing.extend_from_slice(&vs); stack.push(W::Diff(me, vs)); continue; }
                        if seen.insert(me) { out.push(me); }
                        let Some(mine) = kids(&me) else { continue };
                        for (name, o, is_t) in mine {
                            if vs.iter().any(|v| kids(v).is_some_and(|es| es.iter().any(|(n, k, _)| n == name && k == o))) { continue; }
                            if *is_t {
                                let sub: Vec<ObjectId> = vs.iter().filter_map(|v| kids(v).and_then(|es| es.iter()
                                    .find(|(n, _, t)| n == name && *t).map(|(_, k, _)| *k))).collect();
                                stack.push(if sub.is_empty() { W::All(*o) } else { W::Diff(*o, sub) });
                            } else if seen.insert(*o) { out.push(*o) }
                        }
                    }
                }
            }
            if missing.is_empty() { break }
            missing.sort_unstable(); missing.dedup();
            for chunk in missing.chunks(1_000) {
                let locs = lookup(stub, repo, chunk, None, budget).await?;
                let mut ll = Vec::new();
                for (id, l) in locs { let l = l.ok_or_else(|| Error::Unpack(format!("missing object {id}")))?; ll.push((id, l)); }
                for (id, entry) in bucket.read_entries(&ll, budget).await? {           // a miss means a sweep raced: the push fails (3.2)
                    let (k, d) = codec::decode_entry(&entry)?;
                    old.insert(id, if k == Kind::Tree { kids_of(&d)? } else { Kids::new() });
                }
            }
        }
        Ok(out)
    }
}

// ---- DO half. Dispatch gains two arms in 1.3's match; both routes are "Awaits inside: none" (one sync span, Err propagates: A2).
#[derive(serde::Deserialize)] struct IdsIn { ids: Vec<String> }
#[derive(serde::Deserialize)] struct CmIn { sha: String, gen: i64, tree: String, parents: Vec<String> }
#[derive(serde::Deserialize)] struct GraphIn { push_id: String, commits: Vec<CmIn>, introduced: Vec<[String; 2]> }
#[derive(serde::Deserialize)] struct StateRow { state: String } #[derive(serde::Deserialize)] struct ShaRow { sha: String }
#[derive(serde::Deserialize)] struct CRow { gen: i64, parents: String } #[derive(serde::Deserialize)] struct Cnt { c: i64 }
#[derive(serde::Deserialize)] struct LocRow { pack: String, idx: i64, off: i64, len: i64, kind: i64, size: i64 }
#[derive(serde::Deserialize)] struct PRow { sha: String, gen: i64, tree: String }
fn kind_of(k: i64) -> Result<Kind, Error> { match k { 1 => Ok(Kind::Commit), 2 => Ok(Kind::Tree), 3 => Ok(Kind::Blob), 4 => Ok(Kind::Tag), _ => Err(Error::Internal("kind".into())) } }
pub enum Neg { Miss, NotReady(Vec<ObjectId>), Ready { acks: Vec<ObjectId>, interesting: Vec<ObjectId> } }
#[derive(Default)] struct Wlk { heap: BinaryHeap<(i64, ObjectId)>, rows: HashMap<ObjectId, Option<(i64, Vec<ObjectId>)>>,
    color: HashMap<ObjectId, bool>, live: usize }                                      // live = queued entries not known uninteresting
impl RepoDo {
    fn in_commits(&self, ids: &[ObjectId]) -> Result<HashSet<ObjectId>, Error> {        // 90 ids per statement (A6)
        let mut out = HashSet::new();
        for chunk in ids.chunks(PARAMS) {
            let marks = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(",");
            let args = chunk.iter().map(|c| V::from(c.to_string().as_str())).collect();
            for r in self.q(&format!("SELECT sha FROM commits WHERE sha IN ({marks})"), args)?.to_array::<ShaRow>()? { out.insert(hex_oid(&r.sha)?); }
        }
        Ok(out)
    }
    fn crow(&self, id: &ObjectId) -> Result<Option<(i64, Vec<ObjectId>)>, Error> {       // one row read per discovered commit
        let h = id.to_string();
        let Some(r) = self.q("SELECT gen, parents FROM commits WHERE sha=?", vec![V::from(h.as_str())])?.to_array::<CRow>()?.into_iter().next() else { return Ok(None) };
        let ps = serde_json::from_str::<Vec<String>>(&r.parents).map_err(|e| Error::Internal(e.to_string()))?
            .iter().map(|s| hex_oid(s)).collect::<Result<Vec<_>, _>>()?;
        Ok(Some((r.gen, ps)))
    }
    pub fn graph_commits(&self, b: &IdsIn) -> Result<Response, Error> {                  // sparse rows; absent ids simply missing
        if b.ids.len() > 1_000 { return Err(Error::Internal("commits > 1000".into())); }
        let mut rows = Vec::new();
        for chunk in b.ids.chunks(PARAMS) {
            let marks = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(",");
            let args = chunk.iter().map(|s| V::from(s.as_str())).collect();
            for r in self.q(&format!("SELECT sha, gen, tree FROM commits WHERE sha IN ({marks})"), args)?.to_array::<PRow>()? {
                rows.push(serde_json::json!({ "sha": r.sha, "gen": r.gen, "tree": r.tree }));
            }
        }
        json(serde_json::json!({ "rows": rows }))
    }
    /// Same 'open' guard as push_index; rows are content-addressed, so inserts are idempotent and gen only ratchets up.
    pub fn graph_index(&self, b: &GraphIn) -> Result<Response, Error> {
        if b.commits.len().saturating_add(b.introduced.len()) > ROWS { return Err(Error::Internal("graph > 10000 rows".into())); }
        let st = self.q("SELECT state FROM pushes WHERE id=?", vec![V::from(b.push_id.as_str())])?.to_array::<StateRow>()?.into_iter().next();
        if st.map(|s| s.state).as_deref() != Some("open") { return Err(Error::Conflict("push not open".into())); }
        for c in &b.commits {
            let ps = serde_json::to_string(&c.parents).map_err(|e| Error::Internal(e.to_string()))?;
            self.q("INSERT INTO commits(sha,gen,tree,parents) VALUES(?,?,?,?) ON CONFLICT(sha) DO UPDATE SET gen=excluded.gen WHERE excluded.gen > commits.gen",
                   vec![V::from(c.sha.as_str()), V::from(c.gen), V::from(c.tree.as_str()), V::from(ps.as_str())])?;
        }
        for [c, o] in &b.introduced { self.q("INSERT OR IGNORE INTO introduced(commit,oid) VALUES(?,?)", vec![V::from(c.as_str()), V::from(o.as_str())])?; }
        json(serde_json::json!({}))
    }
    fn offer(&self, w: &mut Wlk, id: ObjectId, unint: bool) -> Result<(), Error> {
        match w.color.entry(id) {
            Entry::Occupied(mut e) => { if unint && !*e.get() { e.insert(true); w.live = w.live.saturating_sub(1); } }   // UNINTERESTING is sticky
            Entry::Vacant(e) => { e.insert(unint); let r = self.crow(&id)?;
                w.heap.push((r.as_ref().map(|(g, _)| *g).unwrap_or(0), id)); w.rows.insert(id, r);
                if !unint { w.live = w.live.saturating_add(1); } }
        }
        Ok(())
    }
    /// Section 9 steps 1-5 over SQLite only. `wants` = commit wants (caller marked every want's ObjLoc; tag wants were
    /// peeled through one read_entries round). Miss = shallow args or any coverage hole -> caller runs send_set_shallow.
    pub fn negotiate(&self, args: &FetchArgs, wants: &[ObjectId]) -> Result<Neg, Error> {
        if args.deepen.is_some() || !args.shallow.is_empty() { return Ok(Neg::Miss); }   // shallow semantics live in send_set_shallow
        let known = self.in_commits(&args.haves)?;
        let acks: Vec<ObjectId> = args.haves.iter().copied().filter(|h| known.contains(h)).collect();    // unknown haves dropped (9.1)
        if !(args.done || args.haves.is_empty() || !acks.is_empty()) { return Ok(Neg::NotReady(acks)); } // 9.2: NAK + flush, no pack
        let mut w = Wlk::default();
        for id in wants { self.offer(&mut w, *id, false)?; }                            // a want with no row: crow -> None -> Miss at pop
        for id in &acks { self.offer(&mut w, *id, true)?; }
        let (mut interesting, mut pops) = (Vec::new(), 0usize);
        while w.live > 0 {                                                   // git's everybody_uninteresting(), O(1)
            pops = pops.saturating_add(1); if pops > MAX_POPS { return Err(Error::Limit("fetch too large for this server; clone instead".into())); }
            let Some((_, id)) = w.heap.pop() else { break };
            let unint = *w.color.get(&id).unwrap_or(&true);
            if !unint { w.live = w.live.saturating_sub(1); interesting.push(id); }
            let Some(Some((_, ps))) = w.rows.get(&id).cloned() else { return Ok(Neg::Miss) };   // a row hole mid-walk
            for p in ps { self.offer(&mut w, p, unint)?; }                                     // colour propagates to parents
        }
        Ok(Neg::Ready { acks, interesting })
    }
    /// One join per <= 90 interesting commits; (c,c) rows make every covered commit self-covering. Ok(false) = a commit
    /// with no rows (coverage hole): `set` is then partial and the caller discards it. Introduced oids need no liveness
    /// check: an interesting commit is want-reachable, so its introduced objects are live by 2.5's closure invariant.
    pub fn graph_send_set(&self, interesting: &[ObjectId], filter: Option<&Filter>, set: &mut SendSet) -> Result<bool, Error> {
        let sql = self.sql(); let idx = crate::store::Index(&sql);
        for chunk in interesting.chunks(PARAMS) {
            let marks = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(",");
            let base: Vec<V> = chunk.iter().map(|c| V::from(c.to_string().as_str())).collect();
            let n = self.q(&format!("SELECT COUNT(DISTINCT commit) AS c FROM introduced WHERE commit IN ({marks})"), base.clone())?.one::<Cnt>()?;
            if usize::try_from(n.c).ok() != Some(chunk.len()) { return Ok(false); }   // a commit with no rows: hole -> fallback
            let pred = match filter { None => "", Some(Filter::BlobNone) => "AND o.kind != 3",
                                      Some(Filter::BlobLimit(_)) => "AND NOT (o.kind = 3 AND o.size > ?)" };
            let mut args = base;
            if let Some(Filter::BlobLimit(x)) = filter { args.push(V::from(i64::try_from(*x).map_err(|_| Error::Protocol("blob:limit".into()))?)); }
            let rows = self.q(&format!("SELECT o.pack_id AS pack, o.idx, o.offset AS off, o.len, o.kind, o.size FROM introduced i JOIN objects o \
                                        ON o.sha = i.oid JOIN packs p ON p.id = o.pack_id AND p.state='live' WHERE i.commit IN ({marks}) {pred} \
                                        GROUP BY i.oid"), args)?.to_array::<LocRow>()?;   // every oid of a live-reachable commit is live (2.5)
            for r in rows { set.mark(&idx, &ObjLoc { pack: PackId(r.pack), idx: u32::try_from(r.idx).map_err(|_| Error::Internal("idx".into()))?,
                offset: u64::try_from(r.off).map_err(|_| Error::Internal("off".into()))?, len: u32::try_from(r.len).map_err(|_| Error::Internal("len".into()))?,
                kind: kind_of(r.kind)?, size: u64::try_from(r.size).map_err(|_| Error::Internal("size".into()))? })?; }
        }
        Ok(true)
    }
}
// fetch_v2 wiring (section 9, route /_do/fetch): step 1 (Index::lookup wants; miss -> ERR `upload-pack: not our ref`) and
// step 2's readiness are unchanged. Then: mark every want's ObjLoc into `set` (explicit wants bypass the filter), peel tag
// wants to commit targets via one read_entries round (TagRef::target), `want <tree>` or `deepen`/`shallow` args -> send_set_shallow.
//   Neg::NotReady(acks) -> write_fetch_prelude(args, acks, []) writes acknowledgments + NAK + flush; response ends (rule 3).
//   Neg::Miss -> pack::generate::send_set_shallow as before (the 7.4 commit-region path).
//   Neg::Ready{acks, interesting} + graph_send_set -> plan_reads, write_fetch_prelude (acknowledgments omitted when
//   args.done: rule 3), `packfile\n`, write_pack chunks over pack_chunk/read_range windows, flush. Mid-read Err -> band-3 (10).
```

## Why it works
- **The core claim survives: no object is read to decide what to send.** `negotiate` and `graph_send_set` are `fn` over DO SQLite — `crow` does one `SELECT gen, parents` per discovered commit, `in_commits` ACKs haves by row presence, and the send set is one join per 90 interesting commits. The only R2 traffic of the whole fetch is the tag-peel round plus `pack_chunk`'s `read_range` windows over marked entries (7.2). Contract 9.3's commit-region reads, the `MemFind` rounds and `gix_traverse` all become the fallback path, not the common one.
- **The two-colour walk is git's, with a real queue.** `gen` strictly increases along parent edges, so every child is offered before its parent pops and `color` is final at pop — the first pass's argument, now over `BinaryHeap<(gen, sha)>` instead of `queue.sort()` per pop (review blocker 3). `w.live` is `everybody_uninteresting()` in O(1). UNINTERESTING is sticky and only ever propagates along real ancestry, so it can never paint something the client lacks; a missing or zero `gen` can only pop a node early, which keeps an over-generous colour — a superset, never a hole (9.4 permits supersets).
- **`introduced` is now specified, not hand-waved** (review caveat 1). `introduced(c) = closure(tree(c)) \ U closure(tree(parents))`; `diff` computes it as an n-way path diff that descends only where the commit's entry differs from every parent's entry at the same name — the same cut `diff-tree` makes — and marks an unmatched subtree whole. Exactness: any `o` in the difference is reached, because a prune at an ancestor path would put `o` inside some parent's closure, a contradiction; the only slack is objects shared at different paths, which over-mark. New objects need no subtraction against old parents because an old parent's closure predates the push (2.5). The expensive part — expanding a resurrected old subtree — is bounded by that subtree's size, fetched once per push through coalesced `read_entries`, never per fetch.
- **No under-send under any hole.** Two gates route to `send_set_shallow`: a popped node whose `commits` row is absent (walk-time `Miss`, which covers a want with no row), and the per-batch `COUNT(DISTINCT commit)` check that catches a commit with no `introduced` rows. No object-liveness check is needed: an interesting commit is want-reachable, so everything in its `introduced` set is inside its closure and therefore live by section 2.5's reachability invariant — the `packs.state='live'` join can only drop rows for commits the walk never reaches. `(c,c)` rows make every covered commit self-covering, and the `GROUP BY i.oid` bare-column pick between duplicated live packs is legal because both rows hold the same bytes (2.3). Correctness of the send set itself is the first pass's argument: an object the client lacks has some interesting introducer, and objects whose introducers are all uninteresting are inside a have's closure.
- **The review's consistency race is structural.** Outcome A (ref live, graph absent) cannot happen: `post` runs after the connectivity lookups and before `/_do/push/commit`, and a ref only ever points into a pack that commit flips to `live` in one sync span — if the graph rows were never written, the objects rows weren't either, and the push never committed. Rows are keyed by content, so a rejected or expired push leaves correct rows behind (outcome B was already harmless); `graph_index` keeps the same `pushes.state='open'` guard as `push_index` so a dead push stops mid-batch.
- **GC and dedup need no graph maintenance.** `GcConsolidate` copies objects into a new pack; `commits`/`introduced` are keyed by sha, not pack, and the join resolves through `objects ... state='live'`, so a relocated object resolves to its new location automatically (scenario 14). A dead pack's graph rows stay forever correct.
- **Wire shape is untouched.** `write_fetch_prelude` (protocol-v2-only) implements rule 3 byte-for-byte: `acknowledgments` omitted entirely when the client sent `done`, `NAK` or `ACK <oid>` lines, `ready` before the pack, delim between sections, `packfile`, sideband frames at `MAX_BAND_DATA = 65515` (rule 2), flush, and no `response-end` over HTTP. The first pass's 65519-byte frames are gone with the hand-rolled `emit`. `deepen`/`shallow`/`filter` are advertised (`fetch=shallow filter`, rule 6) and honoured — `filter` becomes a SQL `kind`/`size` predicate here, and anything depth- or graft-related routes to `send_set_shallow`, which implements `send_shallow_list`/`unshallow` semantics; nothing is silently ignored.
- **Budget.** The walk is one sync span: <= 200,000 pops at one row each (9.3's bound, `Error::Limit` beyond), plus `interesting/90` joins — seconds of DO CPU worst case, inside `max_ms = 240 s` (7.1) but blocking other DO events meanwhile (Known limits). Zero subrequests decide the set; the push side adds `ext/1,000` route calls, the diff's changed-path `read_entries` spans, and `rows/10,000` stub posts on top of section 7.3's formula.

## Changes from the first pass
| First-pass finding (quoted) | Kind | How addressed |
|---|---|---|
| "Sideband frame size (65519 -> 65515) -- hard client-side die on any >64 KB blob" | blocker | Closed by contract, not this code: `wire::Sideband::data` chunks at `MAX_BAND_DATA = 65515` (rule 2, `gix_packetline` measured against git 2.43 in the spike); `write_pack`/`pack_chunk` own all pack framing (partial-clone-filters). No hand-rolled `emit` remains. |
| "`recordCommit` assumes parents already inserted; pack order is not topological -> pushes with >1 new commit will intermittently throw" | blocker | Gens are computed in the edge by a Kahn topological sort over in-pack parents after all commits are known; external gens come from `/_do/graph/commits`; the DO inserts verbatim with `ON CONFLICT ... gen > commits.gen` ratchet. No `.one()` on a possibly-absent parent exists anywhere. |
| "Walk uses per-pop `Array.sort`; must be a priority queue or it exceeds DO CPU on medium repos" | blocker | `BinaryHeap<(i64, ObjectId)>`; `everybody_uninteresting()` is the `live` counter, O(1); the walk is capped at `MAX_POPS = 200,000` pops (9.3's bound) -> `Error::Limit`. |
| "Worker/DO dies after the R2 PUTs but before `recordCommit` ... the advertised ref's tip is not in `commits`, `push(want)` silently drops it ... a permanent, user-visible inconsistency" | blocker | Ordering is the section 3 order: graph rows are posted after the object rows and before `/_do/push/commit`; a ref can only move in `commit_push`'s span, which also flips the pack `live`. A ref tip without a `commits` row is impossible for post-feature pushes; any residual hole returns `Neg::Miss` to the foundation walk rather than dropping the want. |
| "`introduced` tree-diff at push time is hand-waved; it is the expensive part (parent trees from R2) and is what makes a 5M-object repo ~400 MB of SQLite in one DO" | caveat | Specified: `diff` descends only changed paths, so a normal push reads a handful of parent trees; resurrected old subtrees are expanded once per push via coalesced `read_entries`. The ~400 MB figure stands (Known limits) — the trade is now explicit: push pays the diff once, fetches pay zero reads. |
| "`objs` crosses DO RPC as one array (32 MiB limit) and `IN (...)` needs batching at 32k params; fresh clones must route to `precomputed-clone-pack`" | caveat | No RPC: edge-to-DO calls are HTTP JSON (`stub_json`), and the send set never leaves the DO — the join feeds `write_pack` in place. `IN` is batched at 90 per A6 (the real limit is 100 params, not 32k). Fresh clones: `precomputed-clone-pack`'s gate still runs first when a plan exists; otherwise this path serves the clone at join speed (the `introduced` union over all commits is the whole object set). |
| "Stream error handling must abort the writer; otherwise clients hang rather than fail" | caveat | Closed by contract section 10: mid-stream failure is one band-3 `ERR` frame then end of stream, and the body is `Response::from_stream` (`pack_chunk`'s `Err` arm). There is no detached writer to leak. |
| "Ref update and `recordCommit` must be in the same DO transaction or fetch can see refs the graph does not know" | caveat | Same-transaction is replaced by ordering plus coverage: rows precede commit (see the crash-walk row), wants are validated against live objects (9.1), and every walk/join coverage miss falls back. A fetch can never under-send; the worst case of a partial graph is a slow fetch, not a wrong one. |
| "Missing for real clients: annotated-tag wants ..., `deepen`/`filter` args must be rejected explicitly (unknown args -> git expects ERR, proof would silently ignore)" | caveat | Tag wants are peeled by the caller through one `read_entries` round (`TagRef::target`, scenario 3). `deepen`/`filter`/`shallow` are advertised (`fetch=shallow filter`, rule 6) and honoured: `filter` becomes SQL `kind`/`size` predicates, `deepen`/`shallow` route to `send_set_shallow` which implements their semantics. `include-tag`, `deepen-since/-not/-relative` and `tree:<n>` stay 400s per section 12 (parse_fetch rejects unknown args). |
| "`ready` semantics: says ready as soon as any ACK exists ... an old orphan `have` can trigger a near-full-history pack" | caveat | Readiness is the contract's own rule (`done \|\| haves empty \|\| acks nonempty`, 9.2, in `write_fetch_prelude`); a superset remains legal (9.4). Not tightened. |
| "objects that reach R2 outside `recordCommit` ... are invisible to fetch forever" | caveat | Under the contract every object enters through `pack::ingest`, and `feed` runs inside pass B for every push — there is no ungraphed ingest path. `presigned-direct-upload` would have to run the same fill; stated as a dependency note. |
| First-pass limit: "the walk runs in JS inside the DO ... a fetch whose interesting set is the whole history of a 1M-commit repo will hit the 30 s CPU limit" | limit | The walk is Rust over sync SQLite; 200,000 pops is the hard bound (9.3) and a fresh clone's send set is one join, not a walk of trees. Above the bound: `Error::Limit`, and `precomputed-clone-pack`/`ClonePlan` is the named path. |
| First-pass limit: "the negotiation layer is independent of ... `OBJ_REF_DELTA` against pinned bases" | limit | Now stronger: all packs at rest are full-object (2.1), so entries are copied verbatim and the `ofs-delta` capability is irrelevant; pinned-delta-bases would layer on `write_pack`, not on negotiation. |

## Known limits
- **Table size.** `introduced` is roughly one row per reachable object (~50-70 B with the autoindex): a 5M-object monorepo is ~350 MB of SQLite in one DO, plus `commits` ~60 B per commit. Inside the 10 GB DO cap; rows are never deleted (content-addressed), so dead pushes and GC'd objects leave permanent rows — graph GC is a named later idea. `branch-level-dos` would be needed to shard.
- **Partial graphs degrade, never corrupt.** A push that overflows `GRAPH_CAP` (64 MiB of held child maps), dies between `graph_index` batches, or predates the feature leaves holes that route every affected fetch to `send_set_shallow` forever — correct, unaccelerated. A `graph_repair` job kind could backfill from `objects`/`packs` history; not built (A9 would admit it).
- **Shallow requests never hit the graph path.** `deepen`/`shallow`/`want <tree>` take the foundation walk, so scenario 11's `shallow-info` correctness lives in partial-clone-filters, not here. Reintroducing depth to the SQL walk is doable (a depth map beside `color`, the `d >= cap` cut, `shallow`/`unshallow` lists) but adds ~15 lines for a minority path.
- **One sync span, worst case seconds.** `negotiate` + `graph_send_set` hold the DO for up to ~200k row reads plus joins; pushes and other fetches wait. The walk is Rust, not JS, and has no awaits; `ReqBudget.max_ms = 240 s` is the wall bound.
- **gen = 0 for unknown external parents** (a parent with no `commits` row) weakens pop order; a bad order can only over-send.
- **Push cost moved, not removed.** `feed` adds two sync parses per commit/tree entry inside pass B; `post` adds `ext/1,000` route calls, the diff's changed-path reads, and `rows/10,000` stub posts (each ~1.5 MB JSON). A revert-style push that re-references a large old subtree expands its closure into `introduced` rows once — bounded by that subtree.
- **The `introduced` semantics are the path-diff's**, not a full closure test: a blob moved between directories is marked introduced although a parent reaches it elsewhere. Superset only (9.4).
- **Unverified, day-1 list.** `TreeRefIter`/`EntryMode::{is_tree,is_commit}`/`CommitRefIter::tree_id` names against gix-object 0.64.1; `SqlCursor::one::<Cnt>` aggregate shape; `COUNT(DISTINCT)`/`GROUP BY` latency over multi-million-row `introduced` in DO SQLite (the idea's whole point — measure against the 7.4 commit-region path it replaces); `RequestInit` stub bodies (two-phase-push's note); `is_some_and` on the pinned toolchain.
- **Write-backs this proof needs.** `feed(id, kind, &data)` hook line inside `resolve_and_normalize`'s loop (streaming-pack-parser); `ingest::run` calls `graph.post(..)` after connectivity lookups (two-phase-push); two dispatch arms in `RepoDo::fetch` for `/_do/graph/{commits,index}` (1.3's match); `SendSet::mark` made `pub(crate)` (partial-clone-filters); `fetch_v2` gains the `Neg` dispatch sketched above; section 12's "commit-graph tables" line points at this module; `schema_version` migration in `boot` for the two tables (8.2).
- **Scenarios this proof must pass (section 11):** 3 (tag wants peeled), 10 (`want <blob>` marked directly, no walk), 11 (via the fallback path), 12 (incremental fetch: ACKs from `commits`, pack contains only the new objects), 14 (post-GC join resolves through the new live pack). Added (two): (a) "coverage miss falls back" — harness deletes a `commits` row (test hook; not producible by stock git), `git fetch` succeeds and `fsck` is clean via `send_set_shallow`; (b) "merge negotiation" — clones A and B push divergent branches, a merge is pushed, a fetch with the merge's first parent as `have` gets a pack containing the second parent's introduced objects, `fsck` clean.

## Depends on
- protocol-v2-only
- partial-clone-filters
- precomputed-clone-pack
- two-phase-push
- streaming-pack-parser
- repo-do-ref-authority
- refs-sqlite-objects-r2
- info-refs-endpoint
