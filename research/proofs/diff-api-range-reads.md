# Diff API served with R2 range reads

> Second pass · Idea #27 · verdict: **lands with caveats** · feasibility 4/5 · reliability 4/5 · correctness 3/5 (first pass 4/3/2)
> First pass: [proof](./archived/diff-api-range-reads.md) · [review](../reviews/archived/diff-api-range-reads.md) · Second pass: [review](../reviews/diff-api-range-reads.md) · Contract: [CONTRACTS.md](../../CONTRACTS.md)

## Mechanism
The first pass's premise is dead by contract: section 2.1 stores only full, non-delta entries and section 12 forbids delta compression at rest, so there is no stored `OFS_DELTA`/`REF_DELTA` whose opcode stream could be served as the diff — and the review showed that stream would not be a diff anyway. What the idea becomes is the honest version of its title: a read-only diff endpoint where every object is resolved through `Index::lookup` (2.3, live packs only) and fetched with `Bucket::read_entries` — coalesced R2 range reads of `[offset, offset+len)` (7.2, A1) — with both sides of each changed file fully reconstructed (16 MiB inflated cap, A7) and diffed in-isolate by `imara-diff`. The route is a new `POST /_do/diff` on `RepoDo` (1.3, same awaits class as `/_do/fetch`: R2 reads only), fronted by edge `GET /:owner/:repo/diff?from=&to=` for any authenticated principal (section 12). Arguments are 40-hex oids or ref names (`HEAD`, `refs/...`); commits and tags are peeled to trees (`CommitRef::tree()`, tag `target`, <= 8 hops), a tree pair is narrowed to changed blob pairs by the section-9 round loop over `TreeRefIter`, and pairs are diffed in batches of <= 32 / <= 16 MiB inflated per `read_entries` call. The response is a `Response::from_stream` of NDJSON records — one header, one per changed path, one trailer — so a mid-stream failure is a single `{"error"}` record, the same convention as the band-3 frame of 9.6. There is no stored delta, no `pack_objects` table, no repack dependency: the `objects` index the push itself wrote is the only locator, and "diff without reconstructing files" is the part the contract removes — said plainly in Known limits.

## Primitives
- `worker::SqlStorage::exec` (sync) + `SqlCursor::to_array` for `refs` name resolution and `Index::lookup` batches: verified (memo section 1, spike). No write exists in this module, so `changes()` is never needed.
- `Index::lookup(&[ObjectId]) -> Vec<Option<ObjLoc>>` (2.3 reader query, `state='live'` join) and `Bucket::read_entries(&[(ObjectId, ObjLoc)], &mut ReqBudget)` (A1 signature, coalesced per 7.2): signatures per the contract; range reads measured on the **local simulator only** (platform-facts #6); the DO subrequest limit is **not enforced locally** (#7), so `ReqBudget::charge` is the only guard (7.1).
- `store::codec::decode_entry` (sync `gix-zlib` inflate of one full entry): ran on workerd in the spike (CONTRACTS correction 2). `objects.len` is the exact entry length (2.3), so no over-read tolerance is relied on.
- `gix_object::{TreeRefIter, CommitRef::from_bytes(..).tree(), TagRef::from_bytes(..).target, tree::EntryMode::bits}` 0.64.1: CI-built for wasm32 (memo section 3); a commit walk ran on workerd (spike); `TagRef`/`EntryMode` names per docs.rs, **not run in the spike** — unverified.
- `imara-diff` 0.2.0 `InternedInput::new(&[u8], &[u8])` (line tokenizer), `diff(Algorithm::Histogram, &input, &mut UnifiedDiffBuilder)`, builder -> `String`: the memo's named choice for blob diffs (section 3, manifest "later milestones"); exact 0.2.0 names per docs.rs, **not compiled in the spike** — unverified, one-line fix on first build if the `Sink` API differs.
- `Response::from_stream` over a `TryStream` of `Vec<u8>` chunks: verified API (memo section 1), same shape as `/_do/fetch` (9.6). `futures_util::stream::{once, unfold}` + `StreamExt::chain`: `futures-util` is already a dependency for `BodyReader`'s `Stream` (section 6); feature-flag coverage for `once`/`unfold` under `default-features = false` is **unverified**.
- `gix_validate::reference::name_partial` for ref-name args: verified (spike).
- `Stub::fetch_with_request` + `Request::new_with_init`/`RequestInit::with_body` + `repo.apply_headers` (8.1): the two-phase-push helper pattern; the constructor path is **unverified at runtime** (the spike forwarded the client request). The stub `Response` is forwarded to the client as-is, as the edge does for `/_do/fetch`.
- `worker::Url::query_pairs` for `?from=`/`?to=`: url-crate API, re-export assumed, **unverified** (as in search-index-on-push).
- Awaits inside the DO open the input gate (measured, #4): every state this route depends on is either immutable (oids, pack bytes kept for GRACE, section 5) or re-checked by the live-only lookup (2.3); nothing is written, so no span needs atomicity.

## Proof code
```rust
// src/repo_do/diff.rs + src/edge/diff.rs -- CONTRACTS.md 1.3, 2.1-2.3, 3, 5, 7, 9.6, 10; A1, A4-A9.
// REGISTRY (A9). Adds: DO route POST /_do/diff (awaits: R2 reads, same class as /_do/fetch); edge route
//   GET /:owner/:repo/diff?from=<oid|ref>&to=<oid|ref> (read principal). JobKind: none. Tables: none.
//   R2 key prefixes: none. Foundation edits: dispatch arm in repo_do::fetch beside /_do/fetch;
//   DTO `DiffIn { from: String, to: String }` in wire::http (A8).
// sql()/q()/meta_str()/oid()/bucket()/parse() and respond(): the RepoDo helpers of the sibling proofs.
use std::collections::HashMap;
use bstr::{BString, ByteSlice};
use futures_util::stream::{self, StreamExt};
use gix_hash::ObjectId;
use gix_object::{tree::EntryMode, CommitRef, Kind, TagRef, TreeRefIter};
use worker::{Env, Method, Request, RequestInit, Response, SqlStorage};
use crate::{edge::RepoRoute, error::Error, repo_do::RepoDo, store::{codec, Bucket, Index, ObjLoc},
            wire::http::DiffIn, ReqBudget};
const PAIR_BATCH: u64 = 16 << 20;   // sum of inflated sizes per read_entries call; one call <= 64 coalesced spans (A4)
const TREE_CAP: usize = 32 << 20;   // all decoded trees held during the walk
const DIFF_CAP: u64 = 64 << 20;     // decoded pair bytes per request; beyond -> {"truncated":true}
const MAX_PAIRS: usize = 20_000;
const NUL_PROBE: usize = 8_000;     // git's binary heuristic

#[derive(serde::Deserialize)] struct T { target: String }
struct Ent { oid: ObjectId, mode: EntryMode, tree: bool, gitlink: bool }
struct End { id: ObjectId, mode: Option<u32>, data: Option<Vec<u8>> }   // data: Some only for direct blob args
struct Pair { path: BString, old: Option<End>, new: Option<End> }
#[derive(Default)] struct Walk { trees: HashMap<ObjectId, Vec<u8>>, tbytes: usize,
    frontier: Vec<(BString, Option<ObjectId>, Option<ObjectId>)>, pairs: Vec<Pair> }
enum Phase { Walk, Emit, Done }
enum Side { Tree(ObjectId), Blob(ObjectId, Vec<u8>) }
struct DiffSt { sql: SqlStorage, bucket: Bucket, budget: ReqBudget,
                walk: Walk, next: usize, dbytes: u64, phase: Phase }

impl RepoDo {
    /// Dispatch arm: `(Method::Post, "/_do/diff") => return self.diff_entry(&body).await`, beside /_do/fetch (1.3).
    pub async fn diff_entry(&self, body: &[u8]) -> worker::Result<Response> {
        crate::wire::http::respond(self.diff(body).await)             // pre-stream errors: section 10 statuses
    }
    async fn diff(&self, body: &[u8]) -> Result<Response, Error> {
        let b: DiffIn = parse(body)?;
        let mut st = DiffSt { sql: self.sql(), bucket: self.bucket()?, budget: ReqBudget::paid(),
                              walk: Walk::default(), next: 0, dbytes: 0, phase: Phase::Walk };
        let a = st.side(self.arg_to_oid(&b.from)?).await?;            // bad arg -> 400, dead oid -> 404, before any byte
        let c = st.side(self.arg_to_oid(&b.to)?).await?;
        let mode = match (a, c) {
            (Side::Blob(i, x), Side::Blob(j, y)) => {
                st.walk.pairs.push(Pair { path: BString::new(), new: Some(End { id: j, mode: None, data: Some(y) }),
                                          old: Some(End { id: i, mode: None, data: Some(x) }) });
                st.phase = Phase::Emit; "blob-diff"
            }
            (Side::Tree(x), Side::Tree(y)) => { st.walk.frontier.push((BString::new(), Some(x), Some(y))); "tree-diff" }
            _ => return Err(Error::Protocol("from/to must resolve to the same object kind".into())),
        };
        let hdr = format!("{}\n", serde_json::json!({"mode": mode, "from": b.from, "to": b.to})).into_bytes();
        let s = stream::once(async move { Ok::<Vec<u8>, Error>(hdr) }).chain(stream::unfold(st, |mut st| async move {
            match st.step().await {
                Ok(Some(c)) => Some((Ok(c), st)),
                Ok(None) => None,
                Err(e) => { st.phase = Phase::Done;                   // mid-stream: one error record, then end (9.6)
                            Some((Ok(format!("{{\"error\":{:?}}}\n", e.to_string()).into_bytes()), st)) }
            }
        }));
        Response::from_stream(s).map_err(Error::from)
    }
    /// 40-hex oid, or a full ref name ('HEAD'/'head' resolves through meta.head). Sync span, no awaits.
    fn arg_to_oid(&self, arg: &str) -> Result<ObjectId, Error> {
        if arg.len() > 256 { return Err(Error::Protocol("from/to too long".into())); }
        if let Ok(id) = oid(arg) { return Ok(id); }
        let name = if arg.eq_ignore_ascii_case("head") { self.meta_str("head")? } else { arg.to_string() };
        if gix_validate::reference::name_partial(name.as_bytes().as_bstr()).is_err() {
            return Err(Error::Protocol("bad ref name".into()));
        }
        match self.q("SELECT target FROM refs WHERE name=?", vec![name.into()])?.to_array::<T>()?.into_iter().next() {
            Some(t) => oid(&t.target), None => Err(Error::NotFound),
        }
    }
}

impl DiffSt {
    /// Peel commit/tag to a tree oid (<= 8 hops; one lookup + one read per hop). Blobs keep their bytes.
    async fn side(&mut self, id: ObjectId) -> Result<Side, Error> {
        let mut cur = id;
        for _ in 0..8 {
            match read_obj(&self.sql, &self.bucket, &cur, &mut self.budget).await? {
                (Kind::Commit, d) => cur = CommitRef::from_bytes(&d).map_err(|e| Error::Unpack(e.to_string()))?.tree(),
                (Kind::Tag, d) => cur = TagRef::from_bytes(&d).map_err(|e| Error::Unpack(e.to_string()))?.target,
                (Kind::Tree, d) => { self.walk.tbytes = self.walk.tbytes.saturating_add(d.len());
                                    self.walk.trees.insert(cur, d); return Ok(Side::Tree(cur)) }
                (Kind::Blob, d) => return Ok(Side::Blob(cur, d)),
            }
        }
        Err(Error::Limit("tag chain too deep".into()))
    }
    async fn step(&mut self) -> Result<Option<Vec<u8>>, Error> {
        loop {
            match self.phase {
                Phase::Walk => { self.walk_all().await?; self.phase = Phase::Emit; }
                Phase::Emit => {
                    if self.next == self.walk.pairs.len() {
                        self.phase = Phase::Done;
                        return Ok(Some(format!("{}\n", serde_json::json!({"end": true,
                            "paths": self.walk.pairs.len(), "subrequests": self.budget.used})).into_bytes()));
                    }
                    if self.dbytes > DIFF_CAP { self.phase = Phase::Done;
                                                return Ok(Some(b"{\"truncated\":true}\n".to_vec())); }
                    return self.emit_batch().await.map(Some);
                }
                Phase::Done => return Ok(None),
            }
        }
    }
    /// Section-9 round loop as a tree diff: pop a dir pair, load its <= 2 missing trees in ONE read_entries, diff sync.
    async fn walk_all(&mut self) -> Result<(), Error> {
        while let Some((prefix, o, n)) = self.walk.frontier.pop() {
            let missing: Vec<ObjectId> = o.into_iter().chain(n).filter(|t| !self.walk.trees.contains_key(t)).collect();
            if !missing.is_empty() {
                let locs = Index(&self.sql).lookup(&missing)?;                  // live-only (2.3)
                let want: Vec<(ObjectId, ObjLoc)> = missing.iter().copied().zip(locs)
                    .filter_map(|(id, l)| l.map(|l| (id, l))).collect();
                if want.len() != missing.len() { return Err(Error::Storage("tree swept mid-request".into())); }
                for (id, e) in self.bucket.read_entries(&want, &mut self.budget).await? {
                    let (k, d) = codec::decode_entry(&e)?;
                    if k != Kind::Tree { return Err(Error::Internal("frontier not a tree".into())); }
                    self.walk.tbytes = self.walk.tbytes.saturating_add(d.len());
                    if self.walk.tbytes > TREE_CAP { return Err(Error::Limit("tree walk too large".into())); }
                    self.walk.trees.insert(id, d);
                }
            }
            diff_trees(&mut self.walk, &prefix, o, n)?;                          // sync span
            if self.walk.pairs.len() > MAX_PAIRS { return Err(Error::Limit("too many changed paths".into())); }
        }
        Ok(())
    }
    /// One batch: <= 32 pairs, <= PAIR_BATCH inflated bytes of reads. Sync lookup, ONE read_entries, then records.
    async fn emit_batch(&mut self) -> Result<Vec<u8>, Error> {
        let cand: Vec<&Pair> = self.walk.pairs.get(self.next..).unwrap_or(&[]).iter().take(32).collect();
        let ids: Vec<ObjectId> = cand.iter().flat_map(|p| [&p.old, &p.new]).flatten()
            .filter(|e| e.data.is_none()).map(|e| e.id).collect();
        let loc_of: HashMap<ObjectId, Option<ObjLoc>> =
            ids.iter().copied().zip(Index(&self.sql).lookup(&ids)?).collect();   // <= 64 ids; impl batches at 90 (A6)
        let (mut want, mut got, mut bytes, mut take) = (Vec::new(), HashMap::new(), 0u64, 0usize);
        for p in cand {
            let ends = || [&p.old, &p.new].into_iter().flatten().filter(|e| e.data.is_none());
            let mode_only = matches!((&p.old, &p.new), (Some(o), Some(n)) if o.id == n.id);
            let need: u64 = if mode_only { 0 } else {
                ends().map(|e| loc_of.get(&e.id).cloned().flatten().map_or(0, |l| l.size)).sum() };
            if take != 0 && bytes.saturating_add(need) > PAIR_BATCH { break; }
            bytes = bytes.saturating_add(need); take = take.saturating_add(1);
            if !mode_only { for e in ends() { match loc_of.get(&e.id).cloned().flatten() {
                Some(l) => want.push((e.id, l)),
                None => { got.insert(e.id, None); }             // live at resolve, row swept since (5.3)
            } } }
        }
        if !want.is_empty() {
            for (id, e) in self.bucket.read_entries(&want, &mut self.budget).await? {
                let (k, d) = codec::decode_entry(&e)?;
                if k != Kind::Blob { return Err(Error::Internal("diff end not a blob".into())); }
                got.insert(id, Some(d));
            }
        }
        let mut out = Vec::new();
        for p in self.walk.pairs.get(self.next..self.next.saturating_add(take)).unwrap_or(&[]) {
            let (line, sz) = record(p, &got)?;
            self.dbytes = self.dbytes.saturating_add(sz);
            out.extend_from_slice(&line);
        }
        self.next = self.next.saturating_add(take);
        Ok(out)
    }
}

enum B<'a> { Absent, Swept, Have(&'a [u8]) }
/// One NDJSON record per pair. Identical and mode-only pairs emit no hunks; binary or non-UTF-8 sides emit no patch;
/// adds and deletes diff against an empty side, as git does.
fn record(p: &Pair, got: &HashMap<ObjectId, Option<Vec<u8>>>) -> Result<(Vec<u8>, u64), Error> {
    let mut j = serde_json::json!({"path": p.path.to_string(),
        "old": p.old.as_ref().map(|e| e.id.to_string()), "new": p.new.as_ref().map(|e| e.id.to_string())});
    if let (Some(o), Some(n)) = (&p.old, &p.new) {
        if o.id == n.id && o.mode == n.mode { j["identical"] = true.into();
                                            return Ok((format!("{j}\n").into_bytes(), 0)); }
        if o.id == n.id {
            j["mode_change"] = serde_json::json!([o.mode.map(|m| format!("{m:06o}")),
                                                n.mode.map(|m| format!("{m:06o}"))]);
            return Ok((format!("{j}\n").into_bytes(), 0));
        }
    }
    let d = |e: &Option<End>| match e { None => B::Absent,
        Some(e) => match e.data.as_deref().or_else(|| got.get(&e.id).and_then(|g| g.as_deref())) {
            Some(v) => B::Have(v), None => B::Swept } };
    let sz = match (d(&p.old), d(&p.new)) {
        (B::Swept, _) | (_, B::Swept) => { j["omitted"] = "swept".into(); 0 }
        (a, b) => {
            let x = if let B::Have(v) = a { v } else { &[][..] };
            let y = if let B::Have(v) = b { v } else { &[][..] };
            if binary(x) || binary(y) { j["binary"] = true.into(); }
            else { j["patch"] = unified(x, y)?.into(); }
            x.len().saturating_add(y.len()) as u64
        }
    };
    Ok((format!("{j}\n").into_bytes(), sz))
}
fn binary(d: &[u8]) -> bool { d.iter().take(NUL_PROBE).any(|b| *b == 0) || std::str::from_utf8(d).is_err() }
/// imara-diff 0.2.0 (memo section 3: "use imara-diff directly for blob diffs"). InternedInput::new splits &[u8] into
/// lines; diff() drives UnifiedDiffBuilder -> unified @@ hunks. Exact names per docs.rs, not spike-built (Primitives).
fn unified(a: &[u8], b: &[u8]) -> Result<String, Error> {
    let input = imara_diff::InternedInput::new(a, b);
    let mut out = imara_diff::UnifiedDiffBuilder::new(&input);
    imara_diff::diff(imara_diff::Algorithm::Histogram, &input, &mut out);
    Ok(out.finish())
}
/// Sync span over two in-memory trees (search-index shape). A tree<->blob type change expands the tree side so
/// every file under it is added/deleted individually. Gitlinks are skipped; symlink bodies diff as blobs.
fn diff_trees(w: &mut Walk, prefix: &BString, o: Option<ObjectId>, n: Option<ObjectId>) -> Result<(), Error> {
    let a = entries(o.and_then(|t| w.trees.get(&t)).map(|v| v.as_slice()))?;
    let b = entries(n.and_then(|t| w.trees.get(&t)).map(|v| v.as_slice()))?;
    for (name, e) in &b {
        if e.gitlink { continue; }
        let path = join(prefix, name);
        match a.get(name).filter(|o| !o.gitlink) {
            Some(o) if o.oid == e.oid && o.mode == e.mode => {}
            Some(o) => {
                if o.tree { w.frontier.push((dir(&path), Some(o.oid), None)); }
                if e.tree { w.frontier.push((dir(&path), None, Some(e.oid))); }
                else { w.pairs.push(Pair { path, old: if o.tree { None } else { Some(end(o)) }, new: Some(end(e)) }); }
            }
            None => if e.tree { w.frontier.push((dir(&path), None, Some(e.oid))); }
                   else { w.pairs.push(Pair { path, old: None, new: Some(end(e)) }); },
        }
    }
    for (name, o) in &a {
        if o.gitlink || b.contains_key(name) { continue; }
        let path = join(prefix, name);
        if o.tree { w.frontier.push((dir(&path), Some(o.oid), None)); }
        else { w.pairs.push(Pair { path, old: Some(end(o)), new: None }); }
    }
    Ok(())
}
fn entries(d: Option<&[u8]>) -> Result<HashMap<BString, Ent>, Error> {
    let mut m = HashMap::new();
    if let Some(d) = d { for e in TreeRefIter::from_bytes(d) {
        let e = e.map_err(|e| Error::Unpack(e.to_string()))?;
        m.insert(e.filename.to_owned(), Ent { oid: e.oid, mode: e.mode, tree: e.mode.is_tree(), gitlink: e.mode.is_commit() });
    } }
    Ok(m)
}
fn dir(path: &BString) -> BString { let mut d = path.clone(); d.extend_from_slice(b"/"); d }
fn end(e: &Ent) -> End { End { id: e.oid, mode: Some(u32::from(e.mode.bits())), data: None } }
fn join(prefix: &BString, name: &BString) -> BString { let mut p = prefix.clone(); p.extend_from_slice(name); p }
/// 2.3 lookup (live only) then one read (7.2, charged 7.1). A miss for a client oid is 404.
async fn read_obj(sql: &SqlStorage, bucket: &Bucket, id: &ObjectId, b: &mut ReqBudget) -> Result<(Kind, Vec<u8>), Error> {
    let loc = Index(sql).lookup(&[*id])?.into_iter().next().flatten().ok_or(Error::NotFound)?;
    let (_, e) = bucket.read_entries(&[(*id, loc)], b).await?.into_iter().next()
        .ok_or_else(|| Error::Storage("entry read".into()))?;
    codec::decode_entry(&e)
}

// ---- src/edge/diff.rs: GET /:owner/:repo/diff?from=&to=; the DO stream is forwarded as-is, like /_do/fetch. ----
pub async fn diff(req: &Request, env: &Env, repo: &RepoRoute, budget: &mut ReqBudget) -> Result<Response, Error> {
    let url = req.url()?;
    let q = |k: &str| url.query_pairs().find(|(a, _)| a == k).map(|(_, v)| v.into_owned()).unwrap_or_default();
    let body = serde_json::json!({"from": q("from"), "to": q("to")}).to_string();
    let mut r = Request::new_with_init("https://do/_do/diff",
        RequestInit::new().with_method(Method::Post).with_body(Some(body.into())))?;
    repo.apply_headers(&mut r)?;                                    // x-ge-owner / x-ge-repo (8.1)
    budget.charge(1)?;                                              // 7.1: charged before the stub call
    repo.stub(env)?.fetch_with_request(r).await.map_err(Error::from)
}
```

## Why it works
- **The dead premise is replaced, not patched.** Sections 2.1 and 12 remove stored deltas entirely, so the first pass's fast path cannot exist and no delta encoder is needed or depended on. In exchange `objects.len` is the *exact* entry length (2.3) — the `clen`-overestimate and `inflateSync`-tolerance machinery the review flagged is unnecessary, and `codec::decode_entry` inflates exactly `len` bytes with `gix-zlib`.
- **The output is a real diff.** `unified` runs `imara-diff`'s histogram over both fully reconstructed sides — the algorithm family `git diff --histogram` uses — so the hunks are computed, not decoded from copy/insert opcodes that misread compression artefacts as edits (blocker 2). The conformance check is `git apply` on each `patch` field, not `git diff` byte-equality (Known limits).
- **The hand-waved narrowing is the code.** `walk_all` is the section-9 round loop: a frontier of `(prefix, old_tree, new_tree)`, each pop loading its <= 2 missing trees in one `read_entries` and diffing synchronously over `TreeRefIter`. Tree<->blob type changes expand the tree side so every file under it is added or deleted individually; gitlinks are skipped; a pure mode change emits `mode_change` with no hunks and no blob read at all.
- **The ordering the review demanded is the foundation's.** "R2 put complete before row visible" is section 3's ordering: `Index::lookup` joins on `state='live'`, which only `commit_push` step 3 sets after `PackWriter::finish` and the index posts — a lookup hit implies durable bytes, a miss is `NotFound`, never a null deref. The one-GC grace for old packs is 5.3's `GRACE = 1 h`, longer than the 240 s request cap (7.1); a row deleted mid-request surfaces as `"omitted":"swept"` or one `{"error"}` record, and a sweep that removes a row keeps the R2 bytes for an hour anyway.
- **A request never writes.** `arg_to_oid`, the `Index::lookup` batches and `diff_trees` are sync reads; every await is an R2 read charged through `budget.charge` (7.1, A1). A2's rollback rule is moot (no sync span mutates). Pre-stream errors map through `respond` to section-10 statuses — `Protocol` 400, `NotFound` 404, `Limit`/`Budget` 413 — and an error inside the stream is one `{"error"}` record then end-of-stream, the NDJSON analogue of 9.6's band-3 frame.
- **Bounded by constants, not by repo.** Worst case held at once: <= 32 MiB of trees (`TREE_CAP`) + one batch of <= 16 MiB inflated entries + one decoded pair <= 32 MiB (16 MiB per side, A7) + the pairs vector (~2 MB for `MAX_PAIRS` = 20,000) ≈ 82 MiB, under 128 MB. `DIFF_CAP` bounds total decoded bytes — hence total diff CPU — at 64 MiB per request, after which the stream ends with `{"truncated":true}`.
- **Budget arithmetic.** Peel: <= 8 reads. Walk: <= 2 coalesced spans per changed directory (usually 1; both trees of a pair merge under 7.2 when adjacent). Emit: `ceil(pairs/32)` `read_entries` calls of <= 64 spans each plus one sync lookup of <= 64 ids (`lookup` batches at 90 internally, A6). A 20,000-path diff costs roughly 630 + a few dozen tree reads, far under `max_subrequests = 9,000` (7.1); the trailer's `subrequests` field and `x-ge-subrequests` (7) are what the harness asserts.
- **What survives of the claim.** "No reconstruction" does not survive the contract — both sides are inflated. What survives is the mechanism in the title: the entire diff is served off R2 range reads alone, exact-`len`, coalesced, with no pack scan, no `.idx` file, and no object outside the changed set ever read.

## Changes from the first pass
| First-pass finding (quoted) | Kind | How addressed |
|---|---|---|
| "No delta-storing pack exists in the foundation: the parser writes resolved loose objects and the repack emits non-delta packs; a same-path-predecessor delta encoder (window search, rabin index) in a 30 s DO alarm is unbuilt and non-trivial." | blocker | Confirmed and made permanent by contract: 2.1 stores only full entries, section 12 forbids deltas at rest. The fast path is removed outright — there is no delta to decode — and the endpoint serves the diff the ordinary way (two reconstructed sides + `imara-diff`). The "no reconstruction" promise is not kept; stated in Mechanism and Known limits. |
| "Delta ops are not a diff: non-monotonic copies, repeated copies and sub-16-byte literal inserts make the 'zero-read summary' numerically wrong versus `git diff`; only 'changed or not' is trustworthy." | blocker | No opcode stream is interpreted anywhere. Hunks are produced by `unified` (imara histogram) over the real contents; binary files, mode-only changes, adds and deletes are first-class record shapes. |
| "`inflateSync` tolerance of trailing bytes in workerd's `node:zlib` is unverified; persist exact compressed length instead." | caveat | Moot in Rust and by schema: `objects.len` is the exact entry length including header (2.3), and `codec::decode_entry` inflates exactly those bytes with `gix-zlib` (spike-verified). |
| "Fallback path can exceed 128 MB (chain depth x base size); needs a size cap that 413s." | caveat | No chains exist (2.1). Caps: 16 MiB per side by A7 (enforced at ingest, checked for free on `loc.size`), `PAIR_BATCH`/`TREE_CAP`/`DIFF_CAP` bound a request to ~82 MiB held and 64 MiB decoded; `Limit`/`Budget` map to 413 before the stream (section 10). |
| "Pack swap and parser row insert need 'R2 put complete before row visible' and a one-GC grace period for old packs." | caveat | Addressed by contract, not by this module: `state='live'` is set only after `finish` + index posts (section 3 ordering), `Index::lookup` sees live rows only (2.3), and `dead` packs keep R2 bytes for `GRACE = 1 h` (5.3) — longer than any request. A mid-request row deletion becomes `"omitted":"swept"`. |
| "`TextDecoder` on delta inserts mangles binary blobs and split UTF-8 sequences; mark inserts as base64 or bytes." | caveat | `binary()` gates the patch: a NUL in the first 8,000 bytes (git's heuristic) or a UTF-8 decode failure yields `"binary": true` and no `patch` field. Patches are emitted only for valid UTF-8 text. |
| "Commit-pair -> blob-pair tree walk is hand-waved and is where the real R2 read count lives." | caveat | Implemented as `walk_all`/`diff_trees`/`entries` over `TreeRefIter` — the section-9 round-loop shape with one coalesced read per dir pair. Read count is now accounted: <= 2 spans per changed directory plus `ceil(pairs/32)` emit calls, asserted via `x-ge-subrequests`. |
| Concurrency walk-through: "B range-reads a key that is not there yet -> `obj` is null -> `obj!.arrayBuffer()` throws TypeError (500)" and "the alarm can swap the table and enqueue deletion of the old pack, and B's `get` lands on a deleted key" | caveat | `lookup` only ever returns `live` rows, and `live` implies durable bytes (section 3 ordering). Rows deleted mid-request have bytes for GRACE; the lookup-miss arms are `NotFound` pre-stream, `"tree swept mid-request"`/`"swept"` in-stream — never a thrown deref. |
| Interop check: "Thin-pack deltas arriving on push are resolved and discarded by the sibling parser, so the raw delta bytes this API wants are never persisted unless the parser is changed to keep them"; also the `clen` derivation, OFS base resolution and JS `i32` overflow notes | caveat | Moot: the API no longer wants raw delta bytes, derives no `clen`, resolves no pack-relative bases and parses no opcodes. The `objects` index is the only locator. |
| Crash walk-through: the repack's "`pack_objects` rewrite ... in one `transactionSync`, which the proof does not state" | caveat | There is no `pack_objects` table and no repack in this design (2.1); the route is read-only, so an eviction mid-await costs the client one 5xx or one `{"error"}` record and a safe retry. |
| Concurrency: "a commit-level diff over N files is N+ serialized R2 GETs ... bottlenecked on one isolate" | caveat | Reads are coalesced (`ceil(N/32)` calls, <= 64 spans each) and mode-only/identical pairs read nothing. DO serialization remains a real limit — recorded in Known limits, not fixed. |

## Known limits
- **The contract forces the weaker version, stated plainly:** both sides of every diffed file are fully reconstructed (<= 16 MiB each, A7). "Serve diffs without reconstructing full files" does not exist in this stack; what this module keeps is "diffs served entirely from indexed R2 range reads".
- **Not `git diff`-identical.** Output is NDJSON with per-file `patch` hunks (histogram, not git's default Myers — different-but-valid hunk splits possible); no `diff --git`/`index` header lines; renames appear as a delete plus an add; gitlinks are skipped; symlink bodies diff as blobs; mode-only changes carry `mode_change` with no hunks. `git apply` on each `patch` is the semantic check.
- **Serialization.** Concurrent diffs on one repo interleave inside one DO isolate; each request is bounded (9,000 subrequests, 240 s) but aggregate throughput is one isolate wide. Very large diffs end `{"truncated":true}` past `DIFF_CAP`, or `{"error":"too many changed paths"}` past `MAX_PAIRS`.
- **Partial results are explicit.** A row swept mid-request produces `"omitted":"swept"` for that file or a `{"tree swept mid-request"}` error record; the client sees a partial diff and retries — it never sees stale bytes or a hang.
- **Only live objects resolve.** An oid that exists solely in an `ingesting` pack or whose last live row was swept is `NotFound` (404 pre-stream) — consistent with 2.3 being the only resolver; `get(key)` on raw keys is never used to test existence.
- **Unverified, day-1 list.** `imara-diff` 0.2.0 exact names (`InternedInput::new` on `&[u8]`, `diff(Algorithm::Histogram, .., &mut UnifiedDiffBuilder)`, `finish()`); `TagRef::from_bytes(..).target`, `TreeRefIter`/`EntryMode::{is_tree, is_commit, bits}` field names in gix-object 0.64.1; `Url::query_pairs` re-export; `Request::new_with_init`/`RequestInit` at runtime; `futures-util` `once`/`unfold`/`chain` under `default-features = false`; `SqlStorage`/`Bucket` as `'static` stream state (same shape as `/_do/fetch`); real-R2 range behaviour (#6); DO subrequest enforcement (#7).
- **Write-backs proposed to CONTRACTS.md.** `POST /_do/diff` in the 1.3 route table (awaits: R2 reads); edge `GET /:owner/:repo/diff`; `DiffIn` DTO in `wire::http` (A8). No `JobKind`, no table, no R2 prefix, no change to section 12 needed.
- Scenarios this proof must not break (section 11): 2, 3, 4, 12, 14. The feature is not exercisable by stock `git`; the harness drives the HTTP route with `curl`. Added scenario 18: push c1 adding `src/a.txt` (text) and a NUL-containing blob, push c2 editing `a.txt`, deleting the binary and `chmod +x` a third file; `GET diff?from=<c1>&to=refs/heads/main` returns the header record, a patched record for `a.txt`, a `"binary":true` record, a `mode_change` record, and a trailer whose `paths` matches; `git apply` of each `patch` reproduces the c2 blob byte-exact; `x-ge-subrequests` stays under the accounted bound. Added scenario 19: `?from=<blob>&to=<blob>` returns one patched record; `?from=HEAD&to=HEAD` returns zero pairs; an unknown 40-hex oid returns 404; `?from=<tree>&to=<blob>` returns 400.

## Depends on
- repo-do-ref-authority
- refs-sqlite-objects-r2
- streaming-pack-parser
- gc-and-repack-alarm
- auth-and-multitenancy
