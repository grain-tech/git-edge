# Wasm git core (gitoxide/libgit2) for delta resolution and merge

> Second pass · Idea #25 · verdict: **lands with caveats** · feasibility 4/5 · reliability 4/5 · correctness 3/5 (first pass 4/3/3)
> First pass: [proof](./archived/wasm-git-core.md) · [review](../reviews/archived/wasm-git-core.md) · Second pass: [review](../reviews/wasm-git-core.md) · Contract: [CONTRACTS.md](../../CONTRACTS.md)


## Mechanism
The first pass built a hybrid: TypeScript owns protocol and I/O, a separate Rust `cdylib` compiled to `wasm32-unknown-unknown` does delta application, hashing and blob merge through a pointer/linear-memory FFI. Under CONTRACTS.md that hybrid is gone, because the foundation *is* the wasm git core: one Rust crate targeting `wasm32-unknown-unknown` (section 1) in which every pushed entry is parsed, inflated, delta-resolved, hashed and re-encoded by gix-pack / gix-zlib / gix-hash / gix-object inside `pack::ingest` in the edge Worker (2.4, corrections 2 and 5, A10), and every read path walks commits and trees with gix-object / gix-traverse over `MemFind` inside `RepoDo` (section 9). The first pass's binding constraint — Wasm imports are synchronous, so the host does all I/O first and calls into the core only with everything resident — survives verbatim as section 9's rule ("an `async fn` loads bytes into `MemFind`; a `fn` computes over `MemFind`"): the memo measured the same thing from the other side (section 3: no JSPI or Asyncify for a Rust Worker, `gix_object::Find` is synchronous). The boundary moved from a Wasm FFI to a Rust function call inside one binary, which deletes `alloc`/`take`, the detached-`memory.buffer` view bug class, and the per-call linear-memory copies (memo section 4, "why not the hybrid"). libgit2 is settled by the memo (section 2): `libgit2-sys` has no wasm32 build path and the only libgit2-on-Wasm is an Emscripten C build that cannot be linked into a `wasm-bindgen` Worker, so "gitoxide/libgit2" reads gitoxide only.

What is genuinely new, and what this file proves:

1. `src/core/` — the synchronous git compute the foundation does not already contain: a vendored copy of the `gix-merge` 0.20.1 blob text driver (926 lines; memo section 3: `gix-merge` is not in the wasm CI list and `blob::Platform` hard-depends on `gix_worktree`/`gix_filter`), `imara-diff` 0.2.0 for blob diffs (memo: "use `imara-diff` directly for blob diffs"), and an own three-way tree merge over `gix_object` trees (memo: "write our own tree merge"; the first-pass `server-side-merge` proof already had the table).
2. Two client-facing JSON APIs, the surviving feature: `GET /:owner/:repo/diff?from=<oid>&to=<oid>` (blob pair -> line hunks; commit/tree pair -> changed-path list) and `POST /:owner/:repo/merge {into, theirs, message}` (three-way merge of `theirs` into the current tip of `into`, committed as a synthetic push). `push-options` is not advertised (1.1 rule 5, section 12), so the first pass's merge channel through `server-side-merge` is replaced by an HTTP route whose DO half drives the section 3 machinery itself: `pushes` row, `packs`/`objects` rows, per-ref CAS, reflog, `refs_version`, `GcMark`.
3. The size budget the title implies: foundation measured at 605 KB after wasm-opt / 262 KB gzip on upload (correction 5); this module's additions and the only remaining cap (64 MiB uncompressed on both plans; memo section 1 — the 3 MB/10 MB compressed limits are gone) are tracked in Known limits.

```
REGISTRY (amendment A9) -- everything this module adds:
  edge routes:   GET  /:owner/:repo/diff        query {from,to} -> JSON
                 POST /:owner/:repo/merge       JSON wire::http::MergeRequest -> JSON
  RepoDo routes: POST /_do/diff                 JSON {from,to} -> JSON; awaits: R2 reads
                 POST /_do/merge                JSON MergeRequest -> JSON;   awaits: R2 reads and writes,
                                                mutations in sync spans only (A2)
  JobKind:       none  (a merge finishes inside one request; 4.1, 4.5)
  tables:        none  (the merge is a pushes/packs/objects/reflog push of section 3)
  R2 prefixes:   none  (merge output lands in an ordinary packs/<id>.pack, 2.2)
  crates/files:  imara-diff 0.2.0; src/core/merge_text.rs vendored from gix-merge 0.20.1 (MIT/Apache-2.0)
```

## Primitives
- The section 9 rule itself: `gix_object::{CommitRef, TreeRefIter, Find::try_find}` over `MemFind` — CI-built for wasm32 (memo section 3); `CommitRef::from_bytes` and a `Simple` walk ran on workerd in the spike; `TreeRefIter` and `tree::EntryMode::{is_tree, is_commit, is_link, as_bytes}` read from 0.64.1 source, **unverified at runtime**.
- `gix_pack::data::File::decode_entry` + `gix_zlib::Inflate` for all client-push delta resolution: **measured on workerd** (correction 5: two OFS deltas resolved, ids match `git verify-pack`). Owned by `pack::ingest`; this module adds nothing to it.
- `imara-diff` 0.2.0 `InternedInput::new`, `diff(Algorithm::Myers, &input)`, `Diff::hunks() -> Hunk { before: Range<u32>, after: Range<u32> }`: the crate is memo-named; these exact entry points are read from docs.rs, **unverified** — a build check is day-1.
- `merge_text::merge(base, ours, theirs, ConflictStyle::Merge) -> Result<(Vec<u8>, u32)>`: our own signature over the vendored driver; upstream's exact signature differs and is irrelevant once vendored. `ConflictStyle::{Merge, Diff3, ZealousDiff3}` exist upstream (memo section 3).
- `gix_object::compute_hash(Sha1, kind, data)` for new merge objects: measured (spike).
- `Index::lookup` (2.3, live packs only, sync in the DO) and `Bucket::read_entries` (coalesced, 7.2, `&mut ReqBudget` per A1): contract signatures; real R2 range reads **local simulator only** (platform-facts #6).
- `PackWriter::{create, append_entry, flush_if_full, finish}` called from a `RepoDo` route: 1.2 + A1 signatures; the DO already writes packs in `GcConsolidate` (5.2). Multipart on real R2 **unverified** (#6).
- `RepoDo::push_begin` / `push_index` / `commit_push` (section 3, sibling proof repo-do-ref-authority) called as plain `fn`s inside `/_do/merge`; `SELECT changes()` is the CAS oracle (**measured** #1); `jobs::enqueue` inside the commit span then `jobs::rearm().await` after it (A3; a second `setAlarm` cancels the first, **measured** #5).
- `Stub::fetch_with_request` + `Request::new_with_init`/`RequestInit::with_body` for the two stub calls (the `stub_json` of two-phase-push): constructor path **unverified at runtime**.
- `js_sys::Date::now()` for the committer timestamp; `gix_validate::reference::name_partial` for `into`. No `js_sys::Reflect`, no `set_alarm`, no `transactionSync` anywhere in this module (A8, 4.1, A3).

## Proof code
```rust
// src/core/mod.rs + src/repo_do/api.rs + src/edge/api.rs. CONTRACTS.md 1.2-1.4, 2.1-2.5, 3, 4.1, 5, 7-10;
// corrections 2, 5; A1-A3, A7-A9. worker 0.8.5, gix-object 0.64.1, gix-hash 0.26.2, imara-diff 0.2.0.
use std::collections::{BTreeMap, HashSet, VecDeque};
use bstr::{BStr, BString, ByteSlice, ByteVec};
use gix_hash::{Kind as H, ObjectId};
use gix_object::{tree::EntryMode, CommitRef, Find, Kind, TreeRefIter};
use worker::{Method, Request, Response, SqlStorageValue as V, Stub};
use crate::{auth::Principal, core::{self, Load, Merge}, edge::RepoRoute, error::Error, jobs,
            repo_do::{BeginDto, CmdDto, CommitRequest, IndexDto, PackMetaDto},
            store::{codec, keys, Index, MemFind, ObjLoc, ObjRow, PackId, PackWriter, PushId},
            wire::http::{DiffRequest, MergeRequest}, ReqBudget, RepoDo};
const MEM_CAP: usize = 64 << 20;            // MemFind bound of section 9, enforced in want()
const MAX_OBJ: u64 = 16 << 20;              // A7 single-object cap; merged blobs obey it too
const MERGE_OUT: usize = 64 << 20;          // buffered merge objects before PackWriter::create
const MERGE_MAX_OBJ: usize = 10_000;        // one push_index call carries <= 10,000 rows (1.3)

// ---------- src/core: fns only, over MemFind or &[u8] (section 9). This is the first pass's Wasm core. ----------
pub enum Load<T> { Need(Vec<ObjectId>), Done(T) }        // a sync step reports the ids the async half must fetch

/// Lockstep two-frontier BFS for one merge base. Pure: a commit not yet loaded lands in `missing`,
/// the host loads it (section 9 loop) and calls again. None = unrelated histories.
pub fn merge_base(mem: &MemFind, a: &ObjectId, b: &ObjectId) -> Result<Load<Option<ObjectId>>, Error> {
    let (mut qa, mut qb) = (VecDeque::from([*a]), VecDeque::from([*b]));
    let (mut sa, mut sb) = (HashSet::from([*a]), HashSet::from([*b]));
    let mut missing = Vec::new();
    while !qa.is_empty() || !qb.is_empty() {
        for (q, mine, other) in [(&mut qa, &mut sa, &sb), (&mut qb, &mut sb, &sa)] {
            let Some(id) = q.pop_front() else { continue };
            if other.contains(&id) { return Ok(Load::Done(Some(id))); }
            let mut buf = Vec::new();
            let Some(o) = mem.try_find(&id, &mut buf).map_err(|e| Error::Internal(e.to_string()))? else { missing.push(id); continue };
            if o.kind != Kind::Commit { return Err(Error::Protocol(format!("{id} is not a commit"))); }
            for p in CommitRef::from_bytes(o.data).map_err(|e| Error::Unpack(e.to_string()))?.parents() {
                if mine.insert(p) { q.push_back(p); }
            }
        }
    }
    missing.sort_unstable(); missing.dedup();
    Ok(if missing.is_empty() { Load::Done(None) } else { Load::Need(missing) })
}

#[derive(Clone)] struct Ent { mode: EntryMode, name: BString, oid: ObjectId }
/// Null id = absent tree. Ok(None) = an entry's object is not in mem; its id is in `missing`.
fn tree_list(mem: &MemFind, id: &ObjectId, missing: &mut Vec<ObjectId>) -> Result<Option<Vec<Ent>>, Error> {
    if id.is_null() { return Ok(Some(Vec::new())); }
    let mut buf = Vec::new();
    let Some(o) = mem.try_find(id, &mut buf).map_err(|e| Error::Internal(e.to_string()))? else { missing.push(*id); return Ok(None) };
    if o.kind != Kind::Tree { return Err(Error::Protocol(format!("{id} is not a tree"))); }
    TreeRefIter::from_bytes(o.data).map(|e| e.map(|e| Ent { mode: e.mode, name: e.filename.to_owned(), oid: e.oid.to_owned() })
        .map_err(|e| Error::Unpack(e.to_string()))).collect::<Result<Vec<_>, _>>().map(Some)
}
struct Tm<'m> { mem: &'m MemFind, missing: Vec<ObjectId>, out: Vec<(Kind, Vec<u8>)>, bytes: usize, conflicts: Vec<BString> }
impl Tm<'_> {
    fn blob(&mut self, id: Option<ObjectId>) -> Result<Option<Vec<u8>>, Error> {       // None = absent side or unloaded
        let Some(id) = id else { return Ok(Some(Vec::new())) };
        let mut buf = Vec::new();
        match self.mem.try_find(&id, &mut buf).map_err(|e| Error::Internal(e.to_string()))? {
            Some(o) => Ok(Some(o.data.to_vec())), None => { self.missing.push(id); Ok(None) } }
    }
    fn new_obj(&mut self, k: Kind, d: Vec<u8>) -> Result<ObjectId, Error> {
        if d.len() as u64 > MAX_OBJ { return Err(Error::Limit("merged object > 16 MiB".into())); }   // A7
        self.bytes = self.bytes.saturating_add(d.len());
        if self.bytes > MERGE_OUT || self.out.len() >= MERGE_MAX_OBJ { return Err(Error::Limit("merge output too large".into())); }
        let id = gix_object::compute_hash(H::Sha1, k, &d).map_err(|_| Error::Internal("sha1 collision".into()))?;
        self.out.push((k, d)); Ok(id)
    }
    /// One directory level; the trivial-merge table of `git merge-tree` plus text-blob merge.
    fn dir(&mut self, b: &ObjectId, o: &ObjectId, t: &ObjectId, path: &BStr, depth: u32) -> Result<ObjectId, Error> {
        if depth > 64 { return Err(Error::Limit("tree too deep".into())); }
        let (bl, ol, tl) = (tree_list(self.mem, b, &mut self.missing)?, tree_list(self.mem, o, &mut self.missing)?, tree_list(self.mem, t, &mut self.missing)?);
        let (Some(bl), Some(ol), Some(tl)) = (bl, ol, tl) else { return Ok(ObjectId::null(H::Sha1)) };
        let (bm, om, tm) = (by(&bl), by(&ol), by(&tl));
        let mut out: Vec<Ent> = Vec::new();
        for name in bl.iter().chain(&ol).chain(&tl).map(|e| e.name.as_bstr()).collect::<HashSet<_>>() {
            let (be, oe, te) = (bm.get(name), om.get(name), tm.get(name));
            let same = |x: Option<&&Ent>, y: Option<&&Ent>| x.map(|e| (e.oid, e.mode)) == y.map(|e| (e.oid, e.mode));
            let mut p = path.to_owned(); p.push_str(name);
            if same(oe, te) { if let Some(e) = oe { out.push((*e).clone()) } continue; }       // both agree (incl. both deleted)
            if same(oe, be) { if let Some(e) = te { out.push((*e).clone()) } continue; }       // only theirs changed
            if same(te, be) { if let Some(e) = oe { out.push((*e).clone()) } continue; }       // only ours changed
            match (oe, te) {
                (Some(a), Some(b2)) if a.mode.is_tree() && b2.mode.is_tree() => {
                    let sub = self.dir(&be.map_or_else(|| ObjectId::null(H::Sha1), |e| e.oid), &a.oid, &b2.oid, &p, depth + 1)?;
                    if !self.missing.is_empty() { return Ok(ObjectId::null(H::Sha1)) }
                    out.push(Ent { mode: a.mode, name: name.to_owned(), oid: sub });
                }
                (Some(a), Some(b2)) if a.mode == b2.mode && !a.mode.is_tree() && !a.mode.is_commit() && !a.mode.is_link() => {
                    let (Some(ob), Some(tb)) = (self.blob(Some(a.oid))?, self.blob(Some(b2.oid))?) else { return Ok(ObjectId::null(H::Sha1)) };
                    let bb = self.blob(be.map(|e| e.oid))?.unwrap_or_default();
                    if ob.contains(&0) || tb.contains(&0) { self.conflicts.push(p); continue; }  // binary: git refuses too
                    let (merged, n) = merge_blobs(&bb, &ob, &tb)?;
                    if n > 0 { self.conflicts.push(p); continue; }
                    out.push(Ent { mode: a.mode, name: name.to_owned(), oid: self.new_obj(Kind::Blob, merged)? });
                }
                _ => self.conflicts.push(p),                                                 // modify/delete, add/add, mode-vs-edit, file/dir
            }
        }
        out.sort_by(|x, y| sort_key(x).cmp(&sort_key(y)));                                   // git: a dir sorts as "name/"
        let mut bytes = Vec::new();
        for e in &out { bytes.extend_from_slice(e.mode.as_bytes()); bytes.push(b' '); bytes.extend_from_slice(&e.name);
                        bytes.push(0); bytes.extend_from_slice(e.oid.as_slice()); }
        self.new_obj(Kind::Tree, bytes)
    }
}
fn sort_key(e: &Ent) -> BString { let mut k = e.name.clone(); if e.mode.is_tree() { k.push(b'/') } k }
pub enum Merge { Clean { root: ObjectId, objects: Vec<(Kind, Vec<u8>)> }, Conflict(Vec<BString>) }
/// Three-way tree merge; null `base` = no common tree. New objects land in `objects` bottom-up, ready for PackWriter.
pub fn merge_trees(mem: &MemFind, base: &ObjectId, ours: &ObjectId, theirs: &ObjectId) -> Result<Load<Merge>, Error> {
    let mut cx = Tm { mem, missing: vec![], out: vec![], bytes: 0, conflicts: vec![] };
    let root = cx.dir(base, ours, theirs, BStr::new(""), 0)?;
    if !cx.missing.is_empty() { cx.missing.sort_unstable(); cx.missing.dedup(); return Ok(Load::Need(cx.missing)); }
    if !cx.conflicts.is_empty() { return Ok(Load::Done(Merge::Conflict(cx.conflicts))); }
    Ok(Load::Done(Merge::Clean { root, objects: cx.out }))
}
/// The vendored driver (REGISTRY): git's <<<<<<< markers and a conflict count. Calls merge_text.rs (926 lines).
pub fn merge_blobs(base: &[u8], ours: &[u8], theirs: &[u8]) -> Result<(Vec<u8>, u32), Error> {
    merge_text::merge(base, ours, theirs, merge_text::ConflictStyle::Merge).map_err(|e| Error::Internal(e.to_string()))
}
pub struct Hunk { pub a_start: u32, pub a_len: u32, pub b_start: u32, pub b_len: u32 }
/// Blob diff for /_do/diff, line hunks only. imara-diff 0.2.0 (memo section 3); entry-point names unverified.
pub fn unified(old: &[u8], new: &[u8]) -> Result<Vec<Hunk>, Error> {
    let input = imara_diff::InternedInput::new(old, new);
    Ok(imara_diff::diff(imara_diff::Algorithm::Myers, &input).hunks()
        .map(|h| Hunk { a_start: h.before.start, a_len: h.before.end - h.before.start,
                        b_start: h.after.start, b_len: h.after.end - h.after.start }).collect())
}
/// Changed-path list between two trees, recursively (for commit pairs the caller resolves trees first).
pub fn tree_changes(mem: &MemFind, a: &ObjectId, b: &ObjectId) -> Result<Load<Vec<BString>>, Error> {
    let (mut missing, mut out, mut stack) = (Vec::new(), Vec::new(), vec![(BString::default(), *a, *b)]);
    while let Some((p, ta, tb)) = stack.pop() {
        let (Some(la), Some(lb)) = (tree_list(mem, &ta, &mut missing)?, tree_list(mem, &tb, &mut missing)?) else { continue };
        let (ma, mb) = (by(&la), by(&lb));
        for name in la.iter().chain(&lb).map(|e| e.name.as_bstr()).collect::<HashSet<_>>() {
            let (x, y) = (ma.get(name), mb.get(name));
            if x.map(|e| (e.oid, e.mode)) == y.map(|e| (e.oid, e.mode)) { continue; }
            let mut q = p.clone(); q.push_str(name);
            match (x, y) { (Some(u), Some(v)) if u.mode.is_tree() && v.mode.is_tree() => stack.push((q, u.oid, v.oid)),
                           _ => out.push(q) }
        }
    }
    missing.sort_unstable(); missing.dedup();
    Ok(if missing.is_empty() { Load::Done(out) } else { Load::Need(missing) })
}
fn by(v: &[Ent]) -> BTreeMap<&BStr, &Ent> { v.iter().map(|e| (e.name.as_bstr(), e)).collect() }

// ---------- src/repo_do/api.rs ----------
fn tree_of(mem: &MemFind, c: &ObjectId) -> Result<ObjectId, Error> {
    let mut b = Vec::new();
    let o = mem.try_find(c, &mut b).map_err(|e| Error::Internal(e.to_string()))?.ok_or(Error::NotFound)?;
    Ok(CommitRef::from_bytes(o.data).map_err(|e| Error::Unpack(e.to_string()))?.tree())
}
impl RepoDo {
    /// Index::lookup (2.3, live only) then one coalesced read (7.2). Any id not live is NotFound.
    async fn want(&self, mem: &mut MemFind, ids: &[ObjectId], budget: &mut ReqBudget) -> Result<(), Error> {
        let locs: Vec<(ObjectId, ObjLoc)> = ids.iter().copied().zip(Index(&self.sql()).lookup(ids)?)
            .map(|(id, l)| l.map(|l| (id, l))).collect::<Option<_>>().ok_or(Error::NotFound)?;
        for (id, entry) in self.bucket()?.read_entries(&locs, budget).await? {               // charged (7.1)
            let (k, d) = codec::decode_entry(&entry)?;
            if mem.bytes + d.len() > MEM_CAP { return Err(Error::Limit("merge/diff working set > 64 MiB".into())); }
            mem.insert(id, k, d);
        }
        Ok(())
    }
    /// POST /_do/diff (REGISTRY). Awaits: R2 reads only.
    pub async fn api_diff(&self, req: &DiffRequest, budget: &mut ReqBudget) -> Result<Response, Error> {
        let (a, b) = (oid(&req.from)?, oid(&req.to)?);
        let locs = Index(&self.sql()).lookup(&[a, b])?;                                     // sync (2.3)
        let kind = match locs.as_slice() { [Some(x), Some(y)] if x.kind == y.kind => x.kind,
            [Some(_), Some(_)] => return Err(Error::Protocol("diff of different kinds".into())), _ => return Err(Error::NotFound) };
        let mut mem = MemFind::default();
        self.want(&mut mem, &[a, b], budget).await?;
        let json = match kind {
            Kind::Blob => serde_json::json!({ "kind": "blob", "hunks": core::unified(&got(&mem, &a)?.1, &got(&mem, &b)?.1)? }),
            Kind::Commit => { let (ta, tb) = (tree_of(&mem, &a)?, tree_of(&mem, &b)?);
                loop { match core::tree_changes(&mem, &ta, &tb)? { Load::Need(ids) => self.want(&mut mem, &ids, budget).await?,
                    Load::Done(v) => break serde_json::json!({ "kind": "commit", "changes": v }) } } }
            _ => return Err(Error::Protocol("diff: commits or blobs only".into())),
        };
        crate::wire::http::json(&json)
    }
    /// POST /_do/merge (REGISTRY). Awaits carry no row writes; every mutation is a sync span (A2).
    pub async fn api_merge(&self, req: &MergeRequest, budget: &mut ReqBudget) -> Result<Response, Error> {
        if gix_validate::reference::name_partial(req.into.as_bytes().as_bstr()).is_err() { return Err(Error::Protocol("bad ref".into())); }
        // span 1: pin the epoch and the tip the merge is computed against; both are re-guarded below.
        let epoch0: i64 = self.meta("gc_epoch")?.parse().map_err(|_| Error::Internal("meta.gc_epoch".into()))?;
        #[derive(serde::Deserialize)] struct R { target: String }
        let ours = oid(&self.q("SELECT target FROM refs WHERE name=?", vec![V::from(req.into.as_str())])?
            .to_array::<R>()?.into_iter().next().ok_or(Error::NotFound)?.target)?;
        let theirs = oid(&req.theirs)?;
        let (bucket, mut mem) = (self.bucket()?, MemFind::default());
        let base = loop { match core::merge_base(&mem, &ours, &theirs)? {                    // section 9 rounds
                Load::Need(ids) => self.want(&mut mem, &ids, budget).await?, Load::Done(b) => break b } };
        let Some(base) = base else { return crate::wire::http::json(&serde_json::json!({ "conflicts": ["unrelated histories"] })) };
        let (new_commit, objects) = if base == theirs { (ours, Vec::new())                   // already contained: no-op
        } else if base == ours { (theirs, Vec::new())                                        // fast-forward
        } else {
            let (tb, to, tt) = (tree_of(&mem, &base)?, tree_of(&mem, &ours)?, tree_of(&mem, &theirs)?);
            let merged = loop { match core::merge_trees(&mem, &tb, &to, &tt)? {
                    Load::Need(ids) => self.want(&mut mem, &ids, budget).await?, Load::Done(m) => break m } };
            let Merge::Clean { root, mut objects } = merged else {
                let Merge::Conflict(paths) = merged else { return Err(Error::Internal("merge".into())) };
                return crate::wire::http::json(&serde_json::json!({ "conflicts": paths })) };
            // `root` ids the last tree merge_trees pushed into `objects`; the commit object joins the same pack.
            let msg = format!("tree {root}\nparent {ours}\nparent {theirs}\nauthor {} <{0}@git-edge> {} +0000\ncommitter {0} <{0}@git-edge> {1} +0000\n\n{}\n",
                              req.principal, now_ms() / 1000, req.message);
            let commit = gix_object::compute_hash(H::Sha1, Kind::Commit, msg.as_bytes()).map_err(|_| Error::Internal("sha1".into()))?;
            objects.push((Kind::Commit, msg.into_bytes()));
            (commit, objects)
        };
        // span 2: a sweep since span 1 voids every lookup this merge used -> Conflict, retried by the caller.
        if self.meta("gc_epoch")?.parse::<i64>().map_err(|_| Error::Internal("meta.gc_epoch".into()))? != epoch0 {
            return Err(Error::Conflict("gc ran during merge, retry".into())); }
        let (push, pack) = (PushId::random(), PackId::random());
        self.push_begin(&self.boot_meta()?, &BeginDto { push_id: push.0.clone(), principal: req.principal.clone() })?;
        let pack_id = if objects.is_empty() { None } else {
            let mut w = PackWriter::create(&bucket, &keys::pack(&bucket.repo, &pack),
                u32::try_from(objects.len()).map_err(|_| Error::Limit("merge objects".into()))?, budget).await?;
            let mut rows = Vec::with_capacity(objects.len());
            for (i, (k, d)) in objects.iter().enumerate() {
                let (offset, len) = w.append_entry(*k, d)?; w.flush_if_full(budget).await?;
                rows.push(ObjRow { sha: gix_object::compute_hash(H::Sha1, *k, d).map_err(|_| Error::Internal("sha1".into()))?,
                    idx: u32::try_from(i).map_err(|_| Error::Limit("merge objects".into()))?, offset, len, kind: *k, size: d.len() as u64 });
            }
            let meta = w.finish(budget).await?;                                              // (3.1) durable before any row
            // span 3a: packs 'ingesting' + objects rows (A5 upsert; push must be open) then 3b: commit (3 steps 1-7)
            self.push_index(&IndexDto { pack: PackMetaDto { id: pack.0.clone(), push_id: push.0.clone(), count: meta.count,
                bytes: meta.bytes, commit_lo: meta.commit_lo, commit_hi: meta.commit_hi }, rows })?;
            Some(pack.0.clone())
        };
        let res = self.commit_push(&CommitRequest { push_id: push.0, pack_id, principal: req.principal.clone(),
            commands: vec![CmdDto { old: ours.to_string(), new: new_commit.to_string(), name: req.into.clone() }] })?;
        jobs::rearm(self).await?;                                                            // A3: commit enqueued GcMark (3 step 7)
        if let Some((_, Some(ng))) = res.results.into_iter().next() { return Err(Error::Conflict(ng.into())); }
        crate::wire::http::json(&serde_json::json!({ "commit": new_commit.to_string() }))
    }
}
fn got(mem: &MemFind, id: &ObjectId) -> Result<(Kind, Vec<u8>), Error> {
    let mut b = Vec::new();
    mem.try_find(id, &mut b).map_err(|e| Error::Internal(e.to_string()))?.map(|d| (d.kind, d.data.to_vec()))
        .ok_or_else(|| Error::Internal("not loaded".into()))
}

// ---------- src/edge/api.rs ----------
/// Two JSON routes under /:owner/:repo; the DO is the only writer, the edge forwards. Conflict -> HTTP 409 (A2).
pub async fn api(req: Request, env: &worker::Env, repo: &RepoRoute, who: &Principal) -> Result<Response, Error> {
    let stub = env.durable_object("REPO")?.id_from_name(&repo.name())?.get_stub()?;
    let mut budget = ReqBudget::paid();
    let v: serde_json::Value = match (req.method(), api_action(&req)) {
        (Method::Get, "diff") => stub_json(&stub, repo, "/_do/diff", &DiffRequest::from_query(&req)?, &mut budget).await?,
        (Method::Post, "merge") => { if !who.can_write { return Err(Error::Forbidden) }
            let mut m: MergeRequest = req.json().await.map_err(|e| Error::Protocol(e.to_string()))?;
            m.principal = who.name.clone();                                                  // identity is auth's, never the body's
            stub_json(&stub, repo, "/_do/merge", &m, &mut budget).await? }
        _ => return Err(Error::NotFound),
    };
    crate::wire::http::json(&v)
}
```

## Why it works
- **The collapse is total for deltas.** Every `ofs-delta`, `ref-delta` and thin-pack base is resolved in `pack::ingest` pass B by `gix_pack::data::File::decode_entry` over `pending/<push>.pack` windows (2.4, A10, correction 2 — measured on workerd, correction 5), and nothing is ever stored as a delta (2.1) or sent as one (section 12). The first pass's `pending_deltas` table, `resolvePending` alarm drain, `loadBase`, and `objects/<sha>` loose writes no longer exist anywhere, which is what closes the review's three blockers without new code.
- **The boundary the first pass got right is a contract rule now.** Section 9's async-load/sync-compute loop is exactly "the TypeScript side does all I/O first and calls into Wasm only with everything resident"; `Load::Need` is the same shape returned as data instead of a callback. `merge_base` and `merge_trees` are pure fns, so re-calling them after `want` loads the `missing` ids is the section 9 loop, and it terminates because `Index::lookup` either resolves an id (loaded, progress) or returns `None` (`Error::NotFound`, request ends).
- **A merge is a synthetic push, so it inherits section 3 whole.** `push_begin` writes the `pushes` row with `gc_epoch = epoch0` (checked in span 2, so a sweep during the merge rounds voids the lookups it used); `push_index` requires `pushes.state='open'` and creates the `packs` row `ingesting` (A5); `PackWriter::finish` (3.1) precedes the rows (3.2); `commit_push` in one sync span re-checks `gc_epoch` (step 2), flips the pack `live` (step 3), CASes `refs.target = ours -> new_commit` with `changes()` (step 4, measured #1), writes the reflog, bumps `refs_version` and enqueues `GcMark` (steps 5-7). A tip that moved under the merge is `ng failed to update ref` -> `Error::Conflict` -> HTTP 409, the same answer git gives a non-fast-forward. `jobs::rearm().await` follows the span because A3 forbids `set_alarm` inside `enqueue`.
- **No parked work means no alarm semantics at all.** The review's stall and collision findings were properties of the `pending_deltas` drain; here a merge runs inside one request under `ReqBudget` (9,000 subrequests, 240 s, 7.1) and registers no `JobKind` (4.5), so `jobs::rearm` remains the only alarm writer (4.1) and the Janitor never fights it.
- **Connectivity holds without a link scan.** Every entry of the merge pack references either another entry of the same pack or an id that `Index::lookup` resolved `live` during the rounds (2.5's invariant); the span-2 `gc_epoch` recheck plus `commit_push` step 2 guarantee nothing referenced was swept mid-merge, and step 4's tip check (`new_commit` is in the just-`live` pack) is the same guard a pushed tip gets. New objects are full objects (2.1) produced by `new_obj`/`append_entry`, capped at 16 MiB each (A7) and 64 MiB total (`MERGE_OUT`).
- **Merge correctness is git's trivial-merge table plus a git-compatible text driver.** `same(O,T)`/`same(O,B)`/`same(T,B)` decide by `(oid, mode)` equality, directories recurse, equal-mode blobs go through the vendored gix-merge text driver (`imara_diff` Myers underneath, `ConflictStyle::Merge` markers), binary or asymmetric cases are conflicts — `git merge-tree`/`merge-ort`'s table minus rename detection. `base == theirs` is a no-op, `base == ours` is a fast-forward carried by `commit_push` with `pack_id: None` (section 3 skips step 3), exactly as git's merge resolves them.
- **Budgets.** A diff is 1 stub call + 1 coalesced `read_entries` (7.2) + CPU. A merge is 1 stub call + one `read_entries` per round (bounded by `MEM_CAP` = 64 MiB of loaded objects, enforced in `want`) + `objects/8 MiB` part uploads + `finish`; worst case is a few dozen subrequests, far under 9,000. `x-ge-subrequests` (7) reports both.
- **Size budget.** Foundation is 605 KB wasm-opt / 262 KB gzip measured (correction 5); this module adds `imara-diff`, the 926-line vendored driver, ~150 lines of tree merge and the routes — estimate +100-200 KB uncompressed, **unverified** until built. Cap: 64 MiB uncompressed (memo section 1); cold instantiate measured 20-30 ms on local workerd (correction 5) against a 1 s global-scope budget. The first pass's "3 MB compressed on Free" worry is obsolete.

## Changes from the first pass
| First-pass finding (quoted) | Kind | How addressed |
|---|---|---|
| "ofs-delta base resolution is inconsistent with the parser it depends on: either persist the raw pack under a key the parser records, or make the parser park `offset -> pending key` and resolve chains recursively (topologically by seq) in the DO." | blocker | Closed by the contract the reviewer asked for: the raw pack is persisted as `pending/<push>.pack` (2.2, 2.4) and `resolve_and_normalize`'s `resolve_at` resolves chains recursively in offset order in the edge (A10), with `MAX_DEPTH` and `verified_base_pack_offset` guards. There is no `pack_objects`/`pending_deltas` table and no separate Wasm call. |
| "Terminal-stall paths: missing base and missing `pending/` key must fail the push (`ng <ref> missing base <sha>`), not loop; cap retries per pushId." | blocker | A missing base is `Error::Unpack("missing base <oid>")` from pass B (2.4) and a truncated pack is `unpack` likewise; A2/section 10 maps both to HTTP 200 + `unpack <msg>` + `ng` per ref. No alarm loop exists to stall. |
| "Linear-memory leak: expose `reset()`/bump-allocator reset per call (or `free`), otherwise the per-isolate instance grows to the 128 MB cap and every request on that isolate dies." | blocker | Closed structurally: the whole crate is the Wasm module (section 1); there is no FFI, no `alloc`/`free` protocol, no linear-memory sharing. Object memory is `MemFind` + `Vec<u8>` under the Rust allocator, bounded by `MEM_CAP`/`MERGE_OUT`/A7 and freed by `Drop`. |
| "Alarm collision with the janitor (see concurrency); needs one scheduler." | caveat | Addressed by contract section 4: one `jobs` dispatcher, `enqueue` dedups, `jobs::rearm` is the sole `set_alarm` caller (a second `setAlarm` cancels the first, measured #5). This module registers no `JobKind` and completes inside one request. |
| "Wasm is only used on the parked-delta path; to earn 'heavy lifting' the inline apply in streaming-pack-parser must call `applyDelta` too (it can: same bundle, same sync call)." | caveat | The inline path *is* the only path: every delta is resolved in pass B on the push request itself (2.4), by the same gix code the first pass wanted behind Wasm. The "rare path" no longer exists. |
| "`gix-merge` on wasm32 is unverified; fallback is vendoring the ~1k-line text driver." | caveat | Settled by memo section 3: `gix-merge` is not in the wasm CI list and its `blob::Platform` needs `gix_worktree`/`gix_filter`; the 926-line text driver is vendored (`src/core/merge_text.rs`), tree merge is ours over `gix-object` trees. |
| "Free plan 3 MB script cap is tight if tree-sitter (semantic-diffs) is also bundled." | caveat | Obsolete limit: the compressed caps are gone (memo section 1; only 64 MiB uncompressed remains). Measured foundation is 605 KB / 262 KB gzip (correction 5); this module's delta is estimated +100-200 KB, unverified until built. |
| "CPU: 64 deltas x 40 MB worst case can exceed 30 s; needs `limits.cpu_ms` or a byte-budget, not a row-count budget, per slice." | caveat | No slices remain on this path: ingest and merge run in-request with `limits.cpu_ms = 300000` and `ReqBudget.max_ms = 240 s` (7.1); per-object 16 MiB (A7) and `MERGE_OUT`/`MEM_CAP` byte budgets replace the `LIMIT 64` row budget. |
| "The Wasm boundary design (host does I/O, pure bytes-in/bytes-out) is right" / interop (1): "`loadBase` range-reads `<pack-key>@<offset>` ... the key does not exist; the entry at that offset may itself be a delta (git chains to depth 50), which the code would treat as content, hash to a garbage sha" | caveat | Same fix as blocker 1: bases are read by recorded offset from `pending/<push>.pack` and chains resolve to a full entry before hashing; `compute_hash` runs on resolved bytes (2.4), so a bad chain cannot mint a plausible sha. |
| Interop (2): "'byte-identical to `git merge`': `gix-merge`'s text driver uses imara-diff Myers; git's xdiff ... can place hunks and labels differently ... Conflict detection is equivalent; byte identity is not guaranteed." | caveat | Carried to Known limits, honestly: the vendored driver is imara-diff-Myers, so conflict *detection* is equivalent and marker *text* may differ from git's xdiff output; labels are `ours`/`theirs`, not `HEAD`/branch. No claim of byte identity is made. |
| Crash walk-through: "`!` non-null assertion throws on every alarm forever; the push never commits and never reports `ng`" | caveat | The parked-delta machinery (per-seq `pending/` keys, the drain loop, the `!`) is gone; a crash mid-merge leaves an `open` `pushes` row the Janitor expires (5.1) and an incomplete multipart upload with no rows (cleanup per Known limits of two-phase-push). |

## Known limits
- **Merge semantics.** One merge base (first BFS hit), no criss-cross virtual base, no rename detection — rename+edit reports a modify/delete conflict, and some merges git resolves cleanly are rejected. Rejection is always safe; acceptance differs from git only in conflict-adjacent hunk placement (imara-diff vs xdiff; review interop note carried forward). Merge commits are unsigned, committer identity is `<principal>@git-edge`, and `theirs` must be a full oid (no server-side ref resolution); `into` must already exist (`NotFound` otherwise — no merging into an unborn branch in v1).
- **Byte caps.** `MAX_OBJ` = 16 MiB per merged object (A7), `MERGE_OUT` = 64 MiB buffered new objects, `MEM_CAP` = 64 MiB working set, `MERGE_MAX_OBJ` = 10,000 (one `push_index` post, 1.3). A merge that hits any of these returns `Error::Limit` (413 at the edge), never a partial ref move — the `pushes`/`packs`/`objects` rows it may have written are an `open`/​`ingesting` push the Janitor reclaims (5.1-5.3).
- **DO placement.** The merge/diff compute runs inside `RepoDo` (like `fetch_v2` and `GcConsolidate`), sharing DO CPU with pushes; the alternative — edge-side compute — pays one stub call per section 9 round. Chosen for index locality; a pathological merge degrades to `Limit`, not a stall.
- **Diff surface.** Blob pairs return line-hunk ranges only (no context lines, no rendered patch in v1); commit pairs return a changed-path list, not per-path hunks (that is N further reads; a later idea can extend `tree_changes`).
- **Unverified.** `imara-diff` 0.2.0 entry points (`InternedInput`, `diff`, `hunks` field names — docs.rs read, not built); `EntryMode::{is_tree, is_commit, as_bytes}` names in 0.64.1 (read from source); `Request::new_with_init`/`RequestInit` stub-body path (two-phase-push lists it); real-R2 multipart and subrequest enforcement (#6, #7); deployed size/cold start (platform-facts "still open" 3); `TransactionSync` remains absent (A3), so span atomicity rests on the no-await rule and panic rollback is scenario 15.
- **Write-backs needed.** `wire::http` gains `DiffRequest {from,to}` (+`from_query`), `MergeRequest {into,theirs,principal,message}`, `Hunk` (Serialize) and the `json`/`respond` helpers A8 already locates there; the sibling proofs' `BeginDto`/`IndexDto`/`PackMetaDto`/`CmdDto`/`CommitRequest`, `edge::stub_json`, `edge::api_action` (the path-suffix matcher) and `RepoDo::{push_begin, push_index, commit_push, meta, boot_meta, bucket, sql, q}` become `pub(crate)` so `repo_do::api` can call them; `oid()`/`now_ms()` shared as in the siblings.
- **Conformance.** No stock-git scenario exercises these routes (they are JSON APIs, not protocol); the two added scenarios are (a) `POST /merge`: seed `main` + a pushed branch that edits the same file non-conflictingly and one conflictingly — assert `{"commit": <40-hex>}`, `git fetch && git cat-file -p <sha>` shows two parents and `fsck` is clean, and the conflicting call returns 409 with `{"conflicts":[<path>]}`; (b) `GET /diff`: assert the returned hunks' line ranges equal `git diff --unified=0` between the same two blobs computed locally.

## Depends on
- repo-do-ref-authority
- two-phase-push
- streaming-pack-parser
- refs-sqlite-objects-r2
- gc-and-repack-alarm
- info-refs-endpoint
- auth-and-multitenancy
