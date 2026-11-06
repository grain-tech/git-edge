# Server-side three-way merge in the Worker

> Second pass · Idea #17 · verdict: **lands with caveats** · feasibility 4/5 · reliability 4/5 · correctness 3/5 (first pass 4/3/2)
> First pass: [proof](./archived/server-side-merge.md) · [review](../reviews/archived/server-side-merge.md) · Second pass: [review](../reviews/server-side-merge.md) · Contract: [CONTRACTS.md](../../CONTRACTS.md)

## Mechanism
The idea survives as a receive-pack push-option, but everything underneath moves to the contract. `git push -o merge=main origin feature` still lands on `POST /o/r/git-receive-pack`; the option arrives only because the v0 advertisement gains `push-options` (write-back to the 1.1 rule 5 list) and `wire::parse_receive_header` gains `ReceiveHeader.options` — the pkt-lines between the command flush and the PACK, parsed only when the client echoed the capability, because otherwise that byte position is the pack header (first-pass review interop). The pushed pack is ingested unchanged by `pack::ingest::run` (2.4, two-phase-push). Then `merge_target` validates the option (exactly one command, non-delete, target under `refs/heads/`, not the pushed ref) and `merge_push` runs the merge in the edge Worker as section-9 rounds: `merge_base` is a paint-down-to-common over commit objects and `merge_trees` a sync three-way over `MemFind`, both fed by `/_do/push/lookup` (the `pack` param, a two-phase-push write-back, makes the push's own `ingesting` rows count) plus coalesced `Bucket::read_entries` (7.2). There is no `commits`/`parents` graph table to consult — section 12 keeps those out of the foundation — and no loose objects (2.1): merge output is a normal normalized pack `r/<repo>/packs/<pack>.pack` whose `ObjRow`s are buffered, not posted. The commit lands through a new sync-span route `/_do/merge/cas`: gc_epoch guard, insert `packs` row (`push_id` NULL, the gc_consolidate precedent of 2.3) + `objects` rows + `live` flip, tip liveness check, `refs` CAS — section 3 steps 3-7 reduced to one server-invented command, `jobs::rearm().await` after the span (A3). On CAS loss the same span marks the pack `dead`; the edge re-merges against the new tip, at most 3 attempts. The pushed ref itself goes through the unchanged `/_do/push/commit` after the merge plan has proved conflict-free, so a conflict still moves nothing. Per-ref independence (3; `atomic` is not advertised) means feature's `ok` is a real move even when the merge retries exhaust — the one semantic the contract weakens, named below. `receive_pack` calls `merge_target` after `pack::ingest::run` returns `Ok`; `Some(target)` swaps the plain commit+report for `merge_push`, and the note is emitted as one `Sideband::progress` frame before `write_report_status` when `caps.side_band_64k` (always advertised, rule 5). The report-status names only client refs; the merge outcome rides that band-2 frame.

## Primitives
- `wire::parse_receive_header` + `ReceiveHeader.options`/`ReceiveCaps.push_options`: write-back; the framing itself is verified (gix-packetline, spike). Options section shape (one pkt-line each after the command flush, only when the client echoed `push-options`): gitprotocol-pack, confirmed by the first-pass review's interop check against git 2.4x.
- `Stub::fetch_with_request`: `GET /_do/refs` and `POST /_do/merge/cas` verified shape (memo 1, spike); `Request::new_with_init`/`Request::new` + `repo.apply_headers` constructor path **unverified at runtime**, as in every sibling.
- `SqlStorage::exec` sync, `SELECT changes()` as CAS oracle: measured 1/0/1 (platform-facts #1); sync-span atomicity measured (#4). The whole `merge_cas_span` is one such span.
- `Index::lookup` (the 2.3 live-only reader query), `Index::insert_objects` <= 10,000 rows (1.2), `/_do/push/lookup` `pack` param + `Index::lookup_in_pack` (two-phase-push write-back): contract + sibling.
- `Bucket::read_entries` coalesced (7.2), `PackWriter::{create, append_entry -> Result, finish, abort}` (1.2 + A1): contract; real-R2 multipart measured on the local simulator only (#6).
- `gix_object::{TreeRefIter, CommitRefIter, compute_hash}`, `Find::try_find`/`Exists` on `MemFind` (1.2): memo 3 + contract 9 usage. `commit::ref_iter::Token::{Tree, Parent, Committer}` and `SignatureRef.time.seconds` (gix-actor/gix-date types through gix-object's iter; memo 3 CI list): field names **pinned at compile**, verified in 0.64.1 source.
- Merge base: no merge-base API is confirmed in the pinned crates (`gix_traverse` 0.61.0 offers walks, not paint-down; `gix-merge` does not build on wasm) — own paint-down over `CommitRefIter`, the memo-3 prefetch-then-compute shape.
- Blob merge: `gix-merge` 0.20.1 is not in the pinned set, absent from wasm CI, and `blob::Platform` hard-depends on `gix_filter`/`gix_worktree` (memo 3). The text driver is vendored as `src/merge/text.rs` (926 lines, imports `imara_diff` + `bstr` only) over `imara-diff` 0.2.0 (memo manifest "later milestone"): **unverified until vendored and built**; the fallback is server-side-rebase's conflict-on-both-changed, which never errs in the dangerous direction.
- `jobs::enqueue` + `jobs::rearm` (4.1, A3), `gix_validate::reference::name_partial` (0.11.4): contract/verified. `PackId::random`/`platform::random32`: `web_sys::Crypto` path unverified (8.2). `js_sys::Date::now` for the ident timestamp: standard.

## Proof code
```rust
// src/edge/merge.rs + src/repo_do/merge.rs. CONTRACTS 1.1-1.3, 2.1-2.5, 3, 5, 7, 9; A1-A10.
// `q`,`changes`,`oid`,`json`,`meta`,`now_ms`,`sql` are the RepoDo helpers of repo-do-ref-authority;
// `stub_json`,`RepoRoute`,`lookup` are the edge helpers of two-phase-push; DTOs live in wire::http (A8).
//
// REGISTRY (A9) -- additions over the foundation lists of 1.3/1.4:
//   wire write-backs: v0 receive advertisement gains `push-options` (1.1 rule 5 list); ReceiveCaps gains
//     `push_options: bool`; ReceiveHeader gains `options: Vec<BString>` (the option pkt-lines, above).
//   DO route: POST /_do/merge/cas   JSON MergeCasDto -> {"result": <str>}   awaits: jobs::rearm only.
//   deps: imara-diff 0.2.0 + vendored src/merge/text.rs (gix-merge 0.20.1 builtin_driver/text; memo 3).
//   no new tables, JobKinds or R2 key prefixes: merge output is a normal packs/<pack>.pack, push_id NULL.
use bstr::{BStr, BString, ByteSlice};
use gix_hash::ObjectId;
use gix_object::{commit::ref_iter::Token, tree::{EntryKind, EntryMode}, CommitRefIter, Find, Kind, TreeRefIter};
use std::collections::{BTreeMap, BinaryHeap, HashMap};
use worker::{Method, Request, Response, SqlStorageValue as V, Stub};
use crate::{auth::Principal, error::Error, jobs::{self, JobKind}, repo_do::RepoDo, ReqBudget,
            store::{codec, keys, Bucket, Index, MemFind, ObjLoc, ObjRow, PackId, PackWriter, PushId},
            wire::{self, http::{CommitResponse, MergeCasDto, MergePackDto, RefsDto}}};
const TREE: u32 = 0o40000; const REG: [u32; 2] = [0o100644, 0o100755];           // regular-file modes
const WALK_MAX: usize = 100_000; const MEM_MAX: usize = 64 << 20; const ATTEMPTS: usize = 3;
#[derive(Clone, Copy, PartialEq)] struct E { mode: u32, id: ObjectId }

// ---- edge ----
/// `merge=<name>` guard, run after the header parse (A2: every Err -> 200 + `unpack <msg>` + ng).
pub fn merge_target(hdr: &wire::ReceiveHeader) -> Result<Option<String>, Error> {
    let mut it = hdr.options.iter().filter_map(|o| o.strip_prefix(b"merge="));
    let Some(v) = it.next() else { return Ok(None) };
    let bad = |m: &str| Err(Error::Unpack(m.into()));
    if it.next().is_some() || hdr.commands.len() != 1 { return bad("merge takes one option and one ref update"); }
    let cmd = hdr.commands.first().ok_or_else(|| Error::Internal("no command".into()))?;
    if cmd.new.is_null() { return bad("merge option on a delete"); }
    let (v, cname): (&[u8], &[u8]) = (v.as_ref(), cmd.name.as_ref());
    let raw: Vec<u8> = if v.starts_with(b"refs/") { v.to_vec() } else { [b"refs/heads/".as_slice(), v].concat() };
    let name = String::from_utf8(raw).map_err(|_| Error::Unpack("non-utf8 merge target".into()))?;
    if !name.starts_with("refs/heads/") || name.as_bytes() == cname
        || gix_validate::reference::name_partial(name.as_str().as_bstr()).is_err() { return bad("bad merge target"); }
    Ok(Some(name))
}
/// Attempt loop: sample `ours`, plan (merge objects -> a fresh pack), commit the pushed ref once on
/// attempt 0 (only after the plan proved conflict-free), then CAS the target. Returns (client-ref
/// results, band-2 note); the target ref is never in `results` (send-pack warns on unknown refs).
pub async fn merge_push(stub: &Stub, repo: &RepoRoute, bucket: &Bucket, who: &Principal, push: &PushId,
    push_pack: Option<&PackId>, target: &str, cmd: &wire::RefCommand, budget: &mut ReqBudget)
    -> Result<(Vec<wire::RefResult>, String), Error> {
    let (mut mem, mut results) = (MemFind::default(), Vec::new());
    for attempt in 0..ATTEMPTS {
        budget.charge(1)?;
        let mut req = Request::new("https://do/_do/refs", Method::Get)?;          // unverified ctor path
        repo.apply_headers(&mut req)?;
        let refs: RefsDto = stub.fetch_with_request(req).await?.json::<RefsDto>().await
            .map_err(|e| Error::Internal(e.to_string()))?;
        let ours = refs.refs.iter().find(|r| r.name == target)
            .map(|r| ObjectId::from_hex(r.target.as_bytes())).transpose().map_err(|e| Error::Internal(e.to_string()))?;
        let planned = plan(stub, repo, bucket, &mut mem, who, ours, cmd, target, push_pack, budget).await?;
        if attempt == 0 {
            let cr: CommitResponse = stub_json(stub, repo, "/_do/push/commit", &serde_json::json!({
                "push_id": push.0, "pack_id": push_pack.map(|p| p.0.clone()), "principal": who.name,
                "commands": [{ "old": cmd.old.to_string(), "new": cmd.new.to_string(), "name": cmd.name.to_string() }]
            }), budget).await?;                                                  // section 3, unchanged route
            results = cr.results.into_iter().map(|(n, r)| match r { None => wire::RefResult::Ok(n.into()),
                Some(s) => wire::RefResult::Ng(n.into(), s) }).collect();
        }
        let Some((new, pack)) = planned else { return Ok((results, "already up to date".into())) };
        match merge_cas(stub, repo, push, target, ours, new, pack, who, budget).await?.as_str() {
            "ok" => return Ok((results, format!("merged {} into {target} as {new}", cmd.name))),
            "failed to update ref" => continue,                                  // target moved: re-merge
            other => return Err(Error::Unpack(other.into())),                    // incl. "gc ran during push, retry"
        }
    }
    Ok((results, format!("merge target moved {ATTEMPTS} times; {} applied, merge skipped", cmd.name)))
}
/// ff / create / real merge. Rows are buffered into the MergePackDto (<= 10,000, the 1.2 cap) and
/// committed by merge_cas, so /_do/push/index and its open-guard are untouched.
async fn plan(stub: &Stub, repo: &RepoRoute, bucket: &Bucket, mem: &mut MemFind, who: &Principal,
    ours: Option<ObjectId>, cmd: &wire::RefCommand, target: &str, push_pack: Option<&PackId>,
    budget: &mut ReqBudget) -> Result<Option<(ObjectId, Option<MergePackDto>)>, Error> {
    let (theirs, Some(ours)) = (cmd.new, ours) else { return Ok(Some((cmd.new, None))) };  // create at theirs
    let Some(base) = merge_base(stub, repo, bucket, mem, ours, theirs, push_pack, budget).await?
        else { return Err(Error::Unpack("unrelated histories".into())) };
    if base == theirs { return Ok(None); }                                       // theirs already contained
    if base == ours { return Ok(Some((theirs, None))); }                         // fast-forward, no objects
    let pack_id = PackId::random();
    let mut out = PackWriter::create(bucket, &keys::pack(&bucket.repo, &pack_id), 0, budget).await?;
    let (mut rows, mut conf, mut need) = (Vec::new(), Vec::new(), Vec::new());
    let root = loop {                                                            // section-9 rounds
        need.clear();
        let r = match merge_trees(mem, Some(E::tree(commit_info(mem, &base)?.0)),
                                  Some(E::tree(commit_info(mem, &ours)?.0)),
                                  Some(E::tree(commit_info(mem, &theirs)?.0)), BStr::new(""),
                                  &mut out, &mut rows, &mut conf, &mut need) {
            Ok(r) => r, Err(e) => { out.abort().await; return Err(e); }          // abort on every failure
        };
        if need.is_empty() { break r; }
        if let Err(e) = load(stub, repo, bucket, mem, &need, push_pack, budget).await { out.abort().await; return Err(e); }
    };
    if !conf.is_empty() { out.abort().await; return Err(Error::Unpack(format!("merge conflict: {}", conf.join(", ")))); }
    let root = root.ok_or_else(|| Error::Internal("empty merge".into()))?;
    let ident = format!("{n} <{n}@git-edge> {t} +0000", n = who.name, t = (js_sys::Date::now() as i64) / 1000);
    let body = format!("tree {}\nparent {ours}\nparent {theirs}\nauthor {ident}\ncommitter {ident}\n\nMerge {} into {target}\n",
                       root.id, cmd.name);
    let id = write_obj(Kind::Commit, body.as_bytes(), &mut out, &mut rows)?;
    let meta = out.finish(budget).await?;                                        // durable before any row (3)
    Ok(Some((id, Some(MergePackDto::new(&pack_id, &meta, rows)))))
}
/// Paint-down-to-common (git commit-reach.c) as section-9 rounds; the commit-time heap stands in for
/// generation numbers. Any base returned is a true common ancestor, so choice can only differ from git's.
async fn merge_base(stub: &Stub, repo: &RepoRoute, bucket: &Bucket, mem: &mut MemFind, a: ObjectId, b: ObjectId,
    pack: Option<&PackId>, budget: &mut ReqBudget) -> Result<Option<ObjectId>, Error> {
    const P1: u8 = 1; const P2: u8 = 2; const STALE: u8 = 4; const QUEUED: u8 = 8;
    let mut flags: HashMap<ObjectId, u8> = HashMap::new();
    *flags.entry(a).or_insert(0) |= P1 | QUEUED;                                 // entry API: a == b must keep both flags
    *flags.entry(b).or_insert(0) |= P2 | QUEUED;
    let (mut need, mut heap, mut best) = (vec![a, b], BinaryHeap::<(i64, ObjectId)>::new(), None);
    loop {
        while let Some((_, c)) = heap.pop() {
            let f = flags.get(&c).copied().unwrap_or(0) & 7;
            let mut give = f;
            if f == P1 | P2 { if best.is_none() { best = Some(c); } give |= STALE; }
            for p in commit_info(mem, &c)?.1 {
                let pf = flags.entry(p).or_insert(0);
                if *pf & give == give { continue; }
                *pf |= give; if *pf & QUEUED == 0 { *pf |= QUEUED; need.push(p); }
            }
        }
        if need.is_empty() { return Ok(best); }
        if flags.len() > WALK_MAX { return Err(Error::Limit("merge-base search past 100,000 commits".into())); }
        let round = std::mem::take(&mut need);
        load(stub, repo, bucket, mem, &round.iter().copied().filter(|id| !mem.exists(*id)).collect::<Vec<_>>(), pack, budget).await?;
        for id in round { heap.push((commit_info(mem, &id)?.2, id)); }
    }
}
/// Sync 3-way over MemFind (9). Trivial rules by sha; both-changed trees recurse; both-changed regular
/// files go through the vendored text driver; everything else is a conflict (safe direction).
fn merge_trees(mem: &MemFind, base: Option<E>, ours: Option<E>, theirs: Option<E>, path: &BStr,
    out: &mut PackWriter, rows: &mut Vec<ObjRow>, conf: &mut Vec<String>, need: &mut Vec<ObjectId>)
    -> Result<Option<E>, Error> {
    if ours == theirs { return Ok(ours); }
    if ours == base { return Ok(theirs); }
    if theirs == base { return Ok(ours); }
    let (Some(o), Some(t)) = (ours, theirs) else { conf.push(path.to_string()); return Ok(ours) };
    if o.mode == TREE && t.mode == TREE {
        let triples = union(mem, base, ours, theirs, need)?;
        if !need.is_empty() { return Ok(ours); }                                 // prefetch round, then re-run
        let mut merged: Vec<(BString, E)> = Vec::new();
        for (name, b, o2, t2) in triples {
            if let Some(e) = merge_trees(mem, b, o2, t2, &join(path, &name), out, rows, conf, need)? {
                merged.push((name, e)); }
            if !need.is_empty() { return Ok(ours); }                             // deeper miss: prefetch, re-run
        }
        return write_tree(merged, out, rows).map(Some);
    }
    if o.mode != t.mode || !REG.contains(&o.mode) { conf.push(path.to_string()); return Ok(ours); }
    for e in [base, ours, theirs].into_iter().flatten() { if !mem.exists(e.id) { need.push(e.id); } }
    if !need.is_empty() { return Ok(ours); }
    let get = |e: Option<E>| e.map(|e| object(mem, &e.id).map(|(_, d)| d)).transpose().map(Option::unwrap_or_default);
    let (bb, ob, tb) = (get(base)?, get(ours)?, get(theirs)?);
    if [&bb, &ob, &tb].into_iter().any(|b| b.contains(&0)) { conf.push(path.to_string()); return Ok(ours); }
    match crate::merge::text::merge(&bb, &ob, &tb) {                             // vendored gix-merge driver
        Ok(m) => write_obj(Kind::Blob, &m, out, rows).map(|id| Some(E { mode: o.mode, id })),
        Err(_) => { conf.push(path.to_string()); Ok(ours) }
    }
}
/// Per-child (base, ours, theirs) triples across the three trees; missing tree objects go to `need`.
fn union(mem: &MemFind, b: Option<E>, o: Option<E>, t: Option<E>, need: &mut Vec<ObjectId>)
    -> Result<Vec<(BString, Option<E>, Option<E>, Option<E>)>, Error> {
    let mut map: BTreeMap<BString, (Option<E>, Option<E>, Option<E>)> = BTreeMap::new();
    for (slot, e) in [(0u8, b), (1, o), (2, t)] {
        let Some(e) = e.filter(|e| e.mode == TREE) else { continue };
        if !mem.exists(e.id) { need.push(e.id); continue; }
        let (kind, data) = object(mem, &e.id)?;
        if kind != Kind::Tree { return Err(Error::Internal("tree entry is not a tree".into())); }
        for ent in TreeRefIter::from_bytes(&data) {
            let ent = ent.map_err(|e| Error::Unpack(e.to_string()))?;
            let m = map.entry(ent.filename.to_owned()).or_default();             // tuple fields: no [] (10)
            match slot { 0 => m.0 = Some(E { mode: mode_u32(ent.mode), id: ent.oid }),
                         1 => m.1 = Some(E { mode: mode_u32(ent.mode), id: ent.oid }),
                         _ => m.2 = Some(E { mode: mode_u32(ent.mode), id: ent.oid }) }
        }
    }
    Ok(map.into_iter().map(|(n, (b, o, t))| (n, b, o, t)).collect())
}
/// One section-9 round: <= 1,000 ids per /_do/push/lookup (7.3); bytes by coalesced read_entries (7.2).
async fn load(stub: &Stub, repo: &RepoRoute, bucket: &Bucket, mem: &mut MemFind, ids: &[ObjectId],
    pack: Option<&PackId>, budget: &mut ReqBudget) -> Result<(), Error> {
    for chunk in ids.chunks(1_000) {
        let mut want: Vec<(ObjectId, ObjLoc)> = Vec::with_capacity(chunk.len());
        for (id, loc) in lookup(stub, repo, chunk, pack, budget).await? {        // (id, Option<ObjLoc>) pairs
            match loc { Some(l) => want.push((id, l)),
                        None => return Err(Error::Unpack(format!("missing object {id}"))) } }
        for (id, entry) in bucket.read_entries(&want, budget).await? {
            let (k, d) = codec::decode_entry(&entry)?; mem.insert(id, k, d); }
        if mem.bytes > MEM_MAX { return Err(Error::Limit("merge working set over 64 MiB".into())); }
    }
    Ok(())
}
fn commit_info(mem: &MemFind, id: &ObjectId) -> Result<(ObjectId, Vec<ObjectId>, i64), Error> {
    let (kind, data) = object(mem, id)?;
    if kind != Kind::Commit { return Err(Error::Unpack(format!("{id} is not a commit"))); }
    let (mut tree, mut parents, mut time) = (None, Vec::new(), 0);
    for tok in CommitRefIter::from_bytes(&data) {
        match tok.map_err(|e| Error::Unpack(e.to_string()))? {
            Token::Tree(t) => tree = Some(t), Token::Parent(p) => parents.push(p),
            Token::Committer(s) => time = s.time.seconds, _ => {} } }
    Ok((tree.ok_or_else(|| Error::Unpack("commit without tree".into()))?, parents, time))
}
fn object(mem: &MemFind, id: &ObjectId) -> Result<(Kind, Vec<u8>), Error> {
    let mut buf = Vec::new();
    mem.try_find(*id, &mut buf).map_err(|e| Error::Internal(e.to_string()))?
        .map(|d| (d.kind, d.data.to_vec()))                                    // MemFind owns the bytes; proof-level readback
        .ok_or_else(|| Error::Unpack(format!("missing object {id}")))
}
fn write_tree(mut es: Vec<(BString, E)>, out: &mut PackWriter, rows: &mut Vec<ObjRow>) -> Result<E, Error> {
    es.sort_by(|a, b| sort_name(&a.0, a.1).cmp(&sort_name(&b.0, b.1)));        // raw bytes, dirs as "name/" (review)
    let mut data = Vec::new();
    for (n, e) in &es { data.extend(format!("{:o} ", e.mode).as_bytes()); data.extend(n.as_slice()); data.push(0); data.extend(e.id.as_slice()); }
    write_obj(Kind::Tree, &data, out, rows).map(|id| E { mode: TREE, id })
}
fn write_obj(kind: Kind, data: &[u8], out: &mut PackWriter, rows: &mut Vec<ObjRow>) -> Result<ObjectId, Error> {
    if data.len() > 16 << 20 { return Err(Error::Limit("merged object over 16 MiB".into())); }      // A7
    if rows.len() >= 10_000 { return Err(Error::Limit("merge writes over 10,000 objects".into())); }
    let id = gix_object::compute_hash(kind, data).map_err(|e| Error::Internal(e.to_string()))?;
    let (offset, len) = out.append_entry(kind, data)?;
    rows.push(ObjRow { sha: id, idx: u32::try_from(rows.len()).map_err(|_| Error::Limit("idx".into()))?,
                       offset, len, kind, size: data.len() as u64 });
    Ok(id)
}
async fn merge_cas(stub: &Stub, repo: &RepoRoute, push: &PushId, target: &str, ours: Option<ObjectId>,
    new: ObjectId, pack: Option<MergePackDto>, who: &Principal, budget: &mut ReqBudget) -> Result<String, Error> {
    #[derive(serde::Deserialize)] struct R { result: String }
    Ok(stub_json::<R>(stub, repo, "/_do/merge/cas", &MergeCasDto { push: push.0.clone(), target: target.into(),
        old: ours.map(|o| o.to_string()), new: new.to_string(), principal: who.name.clone(), pack }, budget).await?.result)
}
fn sort_name(n: &BStr, e: E) -> BString { if e.mode == TREE { [n.as_slice(), b"/"].concat().into() } else { n.into() } }
fn join(p: &BStr, n: &BStr) -> BString { if p.is_empty() { n.into() } else { [p.as_slice(), b"/", n.as_slice()].concat().into() } }
fn mode_u32(m: EntryMode) -> u32 { match m.kind() { EntryKind::Tree => 0o40000, EntryKind::Blob => 0o100644,
    EntryKind::BlobExecutable => 0o100755, EntryKind::Link => 0o120000, EntryKind::Commit => 0o160000 } }
impl E { fn tree(id: ObjectId) -> E { E { mode: TREE, id } } }

// ---- src/repo_do/merge.rs: "Awaits inside: rearm" (the /_do/rebase precedent). ----
impl RepoDo {
    pub async fn merge_cas(&self, b: &MergeCasDto) -> Result<Response, Error> {
        let res = self.merge_cas_span(b)?;                                       // Err propagates out of fetch (A2)
        jobs::rearm(self).await?;                                                // enqueue ran in the span (A3)
        json(serde_json::json!({ "result": res }))
    }
    /// One sync span: epoch guard, pack rows + live flip, tip check, CAS, reflog/refs_version/GcMark.
    /// A CAS loss kills the just-flipped pack in the same span; an abandoned pack is only the finish->call residual.
    fn merge_cas_span(&self, b: &MergeCasDto) -> Result<&'static str, Error> {
        let (new, now) = (oid(&b.new)?, now_ms());
        #[derive(serde::Deserialize)] struct P { gc_epoch: i64 }
        let p: P = self.q("SELECT gc_epoch FROM pushes WHERE id=?", vec![V::from(b.push.as_str())])?.one()?;
        if self.meta("gc_epoch")?.parse::<i64>().map_err(|_| Error::Internal("gc_epoch".into()))? != p.gc_epoch {
            return Ok("gc ran during push, retry"); }                            // merge read pre-sweep objects
        if gix_validate::reference::name_partial(b.target.as_str().as_bstr()).is_err() { return Ok("funny refname"); }
        let sql = self.sql(); let idx = Index(&sql);
        if let Some(mp) = &b.pack {
            let m = &mp.meta;
            if mp.rows.len() > 10_000 { return Err(Error::Internal("merge pack > 10000 rows".into())); }
            self.q("INSERT INTO packs(id,state,count,bytes,commit_lo,commit_hi,push_id,created_at) \
                    VALUES(?,'ingesting',?,?,?,?,NULL,?)",
                   vec![V::from(m.id.as_str()), V::from(m.count), V::from(m.bytes), V::from(m.commit_lo),
                        V::from(m.commit_hi), V::from(now)])?;
            idx.insert_objects(&PackId(m.id.clone()), &mp.rows)?;
            self.q("UPDATE packs SET state='live' WHERE id=? AND state='ingesting'", vec![V::from(m.id.as_str())])?;
            if self.changes()? != 1 { return Err(Error::Internal("merge pack not ingesting".into())); }
        }
        if idx.lookup(&[new])?.into_iter().next().flatten().is_none() {          // the only reader query (2.3)
            self.kill_pack(b, now)?; return Ok("missing necessary objects"); }
        match &b.old {
            None => self.q("INSERT INTO refs(name,target,updated_at) VALUES(?,?,?) ON CONFLICT DO NOTHING",
                           vec![V::from(b.target.as_str()), V::from(new.to_string()), V::from(now)])?,
            Some(o) => self.q("UPDATE refs SET target=?,updated_at=? WHERE name=? AND target=?",
                              vec![V::from(new.to_string()), V::from(now), V::from(b.target.as_str()), V::from(o.as_str())])?,
        };
        if self.changes()? != 1 { self.kill_pack(b, now)?; return Ok("failed to update ref"); }   // git's ng string
        self.q("INSERT INTO reflog(name,old,new,push_id,principal,at) VALUES(?,?,?,?,?,?)",
               vec![V::from(b.target.as_str()), V::from(b.old.clone().unwrap_or_else(|| "0".repeat(40))),
                    V::from(new.to_string()), V::from(format!("merge:{}", b.push)), V::from(b.principal.as_str()), V::from(now)])?;
        self.q("UPDATE meta SET value=value+1 WHERE key='refs_version'", vec![])?;                // 3 step 5
        jobs::enqueue(&sql, JobKind::GcMark, now + 600_000, "{}")?;                               // 3 step 7, dedups
        Ok("ok")
    }
    /// Same-span cleanup for a pack whose CAS just failed: dead now, R2 key swept by Janitor 5.3 after GRACE.
    fn kill_pack(&self, b: &MergeCasDto, now: i64) -> Result<(), Error> {
        if let Some(mp) = &b.pack {
            self.q("DELETE FROM objects WHERE pack_id=?", vec![V::from(mp.meta.id.as_str())])?;
            self.q("UPDATE packs SET state='dead',dead_at=? WHERE id=?", vec![V::from(now), V::from(mp.meta.id.as_str())])?;
        }
        Ok(())
    }
}
```

## Why it works
- **Wire legality.** `push-options` is only sent by a client that saw it advertised and echoes it in the command's capability list; parsing the option section is gated on `caps.push_options`, so the parser can never eat the pack header (the review's interop break). Each option is one pkt-line inside the 1 MiB command-section cap (6.3). Report-status goes through `wire::write_report_status` — band 1 under `side-band-64k` (1.1 rule 4) — and the merge note rides `Sideband::progress` on band 2, legal at any point of the response; without sideband the note is dropped and the result still reads from the `ok`/`ng` lines. Only client-named refs get result lines: `send-pack` merely warns on unknown refs (the review's correction), and main's outcome is never load-bearing.
- **Conflict still rejects before anything moves.** The merge plan is fully computed — and the conflict list fully known — before `/_do/push/commit` is sent on attempt 0. A conflict is `Err(Error::Unpack("merge conflict: <paths>"))`, which A2 maps to HTTP 200 + `unpack` + `ng <ref>` on the pushed ref; the merge pack's `PackWriter` is aborted and no `packs`/`objects` row ever existed for it, so the residual is an incomplete multipart upload only — the same accepted residual class as ingest (two-phase-push Known limits).
- **Registration is the pack, not a manifest.** The first-pass blocker (`known(mergeSha)` fails) dissolves: merge objects are appended to a normalized pack (2.1) and their rows + `live` flip land inside `merge_cas_span`, so `apply_one`-equivalent `Index::lookup` sees the merge commit in the same span (the ref-authority "same connection, same span" argument). Fetch serves main's new tip through the single 2.3 reader query and the next merge_base walks it — no `commits`/`parents`/`introduced` tables are needed or exist (12).
- **No leak in any interleaving.** A merge pack only ever exists with `push_id` NULL: on CAS loss `kill_pack` marks it `dead` and deletes its rows in the same span, so Janitor 5.3 removes the R2 key after GRACE; on success it is a normal `live` pack under GC like any pushed pack. The only uncovered window is an isolate kill between `PackWriter::finish` and the cas call — a rowless, completed R2 object, which is precisely the residual the contract already accepts for ingest. The review's "six orphaned merge objects" scenario cannot recur: there are no loose objects to orphan.
- **gc_epoch closes the sweep race for merge reads too.** The merge reads objects between the push's lookups and the cas; if `GcSweep` ran in between, `merge_cas_span`'s first statement compares `meta.gc_epoch` to `pushes.gc_epoch` and returns "gc ran during push, retry" — the pushed ref's commit already reported the same through section 3 step 2, and the half-read merge is discarded rather than accepted with a hole (the section-5 race analysis, applied to one more span).
- **Merge-base is git's algorithm minus generation numbers.** Paint-down flags (PARENT1/PARENT2/STALE) with a commit-time max-heap is `commit-reach.c`; every id in `flags` is queued once, a commit carrying both parent flags is a real common ancestor, and ancestors of a found base are marked stale so they cannot be reported. Without generation cut-off the picked base may differ from git's in merge-heavy or criss-cross history — always a *valid* base, so the failure direction stays "false conflict", never a wrong merge (review caveat, accepted). `WALK_MAX` bounds the walk.
- **Budgets (7).** A merge-base round costs 1 lookup + 1 coalesced `read_entries` per 1,000 commits; a tree level costs the same per 1,000 entries; merge output is capped at 10,000 objects and each merged object at 16 MiB inflated (A7); `MemFind` is capped at 64 MiB (the section-9 bound) — a deeper merge returns `Error::Limit` with a distinct message, never a fake conflict. `ATTEMPTS = 3` caps the whole thing at roughly `3 x (walk_rounds + merge_rounds + 3)` stub calls against the 9,000-subrequest budget, and `ReqBudget.max_ms = 240 s` applies to all of it.
- **Error policy (10, A2).** `Unpack`/`Limit`/`Budget`/`Conflict` raised after the header all surface as the 200 + `unpack <msg>` report; `Storage`/`Internal` inside `merge_cas_span` propagate as `Err` out of `fetch` so the platform discards the span. No `unwrap`/`expect`/indexing on client-derived bytes (`options`, ref names, oids all go through `?`/`strip_prefix`/`from_hex`).
- **Semantics under the contract, stated honestly.** Section 3 makes ref commands per-ref independent and `atomic` is not advertised (1.1 rule 5), so the first pass's "both refs or neither" is unavailable: feature's `ok` is a real move even when the merge's CAS retries exhaust, and a feature whose own CAS failed still lets the merge land (the pushed objects are the merge's content either way). Conflict and `gc ran during push` remain the only merge rejections, and conflict still precedes any commit.

## Changes from the first pass
| First-pass finding (quoted) | Kind | How addressed |
|---|---|---|
| "Merge objects are invisible to `commit()`: `known(mergeSha)` fails, so any real (non-ff) merge is rejected with `missing necessary objects`. Needs a `stub.register(pushId, objects+links)` (or a second manifest) before `commit()`, which also populates `objects/commits/parents/introduced`." | blocker | `/_do/merge/cas` is that registration, in one sync span: `packs` row + `objects` rows + `live` flip precede the CAS, so `Index::lookup` sees `new` immediately. `commits`/`parents`/`introduced` do not exist under the contract (12); ancestry is walked over objects in the edge. |
| "Without (1), even a patched CAS leaves main's tip unfetchable and un-merge-able next time; without janitor coverage, abandoned merge objects leak permanently." | blocker | The merge pack is a normal `live` pack served by the 2.3 query, so the tip is fetchable and mergeable. CAS-loss packs go `dead` + rows deleted in the same span (Janitor 5.3 sweeps the key after GRACE); the only residual is the finish-to-call crash window, the contract's accepted incomplete-upload class. |
| "Sideband-wrapped report-status and push-options capability echo check, so a stock `git push -o merge=main` completes at all." | blocker | `write_report_status` band-1 framing (rule 4); `ReceiveHeader.options` is parsed only when `caps.push_options` was echoed (REGISTRY write-back). |
| "Single BFS merge base without generation ordering can pick an older ancestor in merge-heavy history: false conflicts, never silent wrong merges given the sha-equality shortcuts." | caveat | Paint-down with commit-time heap replaces the lockstep BFS — git's own algorithm minus generation numbers; returned base is always a true common ancestor, so the failure direction is unchanged. |
| "Line 67 writes merged blobs before conflicts elsewhere are known; conflict rejections still leave R2 writes behind." | caveat | Rows are never posted and no `packs` row exists before `merge_cas_span`; the conflict path is `out.abort()` + `Err`. Nothing but an incomplete upload remains. |
| "`x-git-user` must be set by the auth layer and stripped from client requests, or authorship is spoofable; merge commit is unsigned." | caveat | Author and committer are `Principal.name` from `auth::authenticate` (the Basic-token name), `<name>@git-edge`; no client header is read. Unsigned remains (Known limits). |
| "Memory/CPU bound by largest both-sides-edited blob; large-file merges should be refused with an explicit 'too large to merge server-side' rather than a fake conflict." | caveat | `Limit("merged object over 16 MiB")` / `Limit("merge working set over 64 MiB")` / `Limit("merge-base search past 100,000 commits")` surface as `unpack` lines distinct from `merge conflict`. |
| "Proof's statement that `send-pack` rejects unknown-ref status lines is wrong (it warns); the design conclusion still stands." | caveat | `results` carries only client-named refs; the merge outcome rides band-2 progress, or is silent without sideband. |
| "Tree entries are sorted by JS `<` (UTF-16 code units), not by bytes ... `fetch.fsckObjects` rejects as `treeNotSorted`. Sort on `TextEncoder` output." | caveat (interop) | `sort_name` orders `BString` raw bytes, directories as `name/` — the same `write_tree` the rebase proof landed. |
| "A delete push (`new = ZERO`) or an empty pack (branch already on server) reaches the merge path unguarded; `treeOf(ZERO)` throws." | caveat (interop) | `merge_target` rejects delete+merge, multi-command merges and self-merge before any object is touched; a 0-object pack is fine — `theirs` resolves through the live index. |
| "`commit()` reports `ok` for `feat-b` inside the same results array when main fails ... if the loop exits at 3 attempts `report()` must not have leaked those earlier `ok` lines." | caveat (concurrency) | The report is built once after the loop from `results` + note; under per-ref independence feature's `ok` is a real move, so nothing is hidden. |
| "`mergeBase`'s SQL (`commits(sha, parents)` string column) does not match the sibling's `parents(oid, parent)` table." | caveat (interop) | Neither table exists (section 12); `merge_base` parses commit objects loaded via the 2.3 query. |
| First-pass limit: "`commit()` is assumed to accept a list of commands and apply them atomically (both feature and main, or neither)" | limit | `atomic` is not advertised (rule 5) and section 3 is per-ref; the feature command commits alone, the merge is a separate server-invented CAS. Conflict still prevents any commit. |
| First-pass limit: "Merge is done in the Worker ... every both-sides-edited blob is fully inflated and split into lines in memory" | limit | Still fully in-memory per blob, now bounded: 16 MiB each (A7), `MEM_MAX` 64 MiB total, 240 s `ReqBudget`; over-cap is `Limit`, not conflict. |
| First-pass limit: "`mergeBase` runs inside the DO ... one SQLite `SELECT` per commit ... the repo takes no other request" | limit | The walk moved to the edge; the DO sees only 1,000-id lookups and the cas call, each a sync span. |
| First-pass limit: "No rename detection" / "Only one merge base is used" / "Submodule entries (160000) and symlinks (120000) ... mode-only changes ... conflicts" | limit | Unchanged: no merge-ort renames, no recursive virtual base, non-regular both-changed entries conflict. Known limits. |
| First-pass limit: "the sideband message 'merged into main as <sha>' is not shown in the code" | limit | Implemented: band-2 progress via `Sideband::progress` when `caps.side_band_64k`. |

## Known limits
- **Blob merge is the vendored driver, unverified until vendored and built on wasm** (memo 3; the 926 lines import only `imara_diff` + `bstr`, but `gix-merge` itself is absent from wasm CI and the pinned set). If vendoring fails, `merge_trees` falls back to `conf.push` on both-changed blobs — the behaviour server-side-rebase already lands — never a wrong merge.
- **Single merge base, no recursive virtual base.** Criss-cross history can yield a false conflict where git merges; never the reverse. Generation numbers are approximated by the commit-time heap.
- **No rename detection** (merge-ort): rename-vs-edit reports modify/delete. This is where `gix-diff` 0.67.1 (`sha1`+`wasm` features, `getrandom_backend="wasm_js"` rustflag — memo 3/4) would later earn its place.
- **The contract weakens atomicity.** `atomic` is not advertised and section 3 applies each ref independently, so the merge target's move is a separate CAS from the pushed ref's: feature can land while the merge is skipped after 3 CAS losses (reported as `ok` + a band-2 note), and a feature whose own CAS failed still lets the merge land. Conflicts remain pre-commit, so "reject only on real conflicts" is preserved where it is checkable.
- **Rowless completed-pack residual:** an isolate kill between `PackWriter::finish` and the cas call leaves a finished pack no row references — the same class as ingest's incomplete multipart (two-phase-push Known limits); cleanup is the same unverified R2 lifecycle rule.
- **Merge output caps:** 10,000 objects per attempt (the 1.2 insert cap), 16 MiB per object (A7), 64 MiB loaded working set, 100,000-commit base walk. Over any cap: `Limit` -> `unpack <msg>`, not a conflict.
- **Identity is the credential name.** `Principal.name` becomes `<name>@git-edge` on both ident lines; the merge commit is unsigned (a signed-by-default policy needs `signed-reflog`'s server key, out of scope). `who.name` is assumed sanitised by `auth`; a name containing a newline would corrupt the commit header — the write-back is a `auth` invariant, not checked here.
- **Write-backs proposed to CONTRACTS.md**, each small: advertisement gains `push-options` (rule 5 list); `ReceiveCaps.push_options` + `ReceiveHeader.options: Vec<BString>` parsed only on client echo; `POST /_do/merge/cas` joins the 1.3 route table ("Awaits inside: rearm"); `MergeCasDto`/`MergePackDto`/`RefsDto` in `wire::http` (A8); `imara-diff` 0.2.0 + vendored `src/merge/text.rs` join the manifest; `lookup`'s `pack` param and `Index::lookup_in_pack` are shared with the two-phase-push write-back; `refs.peeled` (A5) stays NULL for merge commits — only tag targets peel.
- **Unverified items:** `Request::new`/`new_with_init` + `apply_headers` at runtime; `Token::Committer`/`SignatureRef.time.seconds`, `EntryMode::kind`/`EntryKind` variants, `Exists::exists`, `Find::try_find` return shape, `BinaryHeap<(i64, ObjectId)>` `Ord` — all pinned at compile; `ObjRow` struct-literal field names; `MergePackDto::new`; subrequest enforcement (#7) and real-R2 multipart (#6); the vendored text driver.
- **Scenarios this proof must pass:** 2, 4, 6, 7, 9 (the push path is unchanged when the option is absent; a client that never echoes `push-options` provably cannot produce the options section). Added (two): (a) `git push -o merge=main origin feature` where each side changed different files: `ok refs/heads/feature`, band-2 line names the merge oid, `git fetch` + `rev-parse main^2` shows the feature tip, `git fsck --strict` clean; (b) the same push with overlapping edits: `ng refs/heads/feature merge conflict: <path>`, `ls-remote` shows both refs unchanged, and the DO holds no `packs` row for the merge attempt. (A CAS race is scenario 6 mechanics: a concurrent push to main makes the first cas fail, the retry merges against the new tip, exactly one ordering lands — asserted inside (a) by racing a second push.)
- **Harness note:** `git -o` push options need git >= 2.10; the CI matrix (2.43+) qualifies. Stock git cannot emit a *conflicting* `merge=` case on demand, so (b) constructs the divergence in the fixture repos, not the protocol.

## Depends on
- two-phase-push (ingest ordering, `lookup` `pack` param, `stub_json`/`RepoRoute`)
- repo-do-ref-authority (`commit_push`, `q`/`changes`/`oid`/`json`/`meta` helpers)
- refs-sqlite-objects-r2
- streaming-pack-parser
- gc-and-repack-alarm (Janitor 5.3 sweeps `dead` packs; GcMark on a landed merge)
- info-refs-endpoint (the advertisement carries `push-options`)
- auth-and-multitenancy (`Principal.name` as author identity)
- server-side-rebase (shares the vendored text driver, `write_tree`/`merge_trees` conventions; whichever lands first vendors it)
