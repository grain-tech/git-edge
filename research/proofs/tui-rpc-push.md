# Push from a sibling workspace DO over RPC, no HTTP

> Second pass · Idea #29 · verdict: **lands with caveats** · feasibility 4/5 · reliability 4/5 · correctness 4/5 (first pass 4/3/3)
> First pass: [proof](./archived/tui-rpc-push.md) · [review](../reviews/archived/tui-rpc-push.md) · Second pass: [review](../reviews/tui-rpc-push.md) · Contract: [CONTRACTS.md](../../CONTRACTS.md)


## Mechanism
Under the contract this is the two-phase push with the edge Worker replaced by the sibling workspace DO itself. The grok-pi `AgentSession` DO snapshots its `files` table into real git objects (blob -> tree -> commit, ids from `gix_object::compute_hash`), writes them as one normalized pack to `r/<repo_id>/packs/<pack>.pack` — there are no loose objects (2.1), so the first pass's per-object `objects/<sha>` PUTs become entries of one multipart upload — posts the `objects` rows through the foundation's `/_do/push/index` route, and commits through `/_do/push/commit` (sections 1.3, 3). The transport is `Stub::fetch_with_request` on those internal JSON routes, the same substrate the edge Worker uses, because typed DO RPC is experimental in workers-rs (memo section 1, workers-rs issue #720); "no HTTP" is thereby weakened to "no git-over-HTTP endpoint, no pkt-line, no edge hop", stated openly rather than silently. What is genuinely new on the git-edge side: the objects never pass through `pack::ingest`, so no server code sees the bytes before they are durable — the review's dangling-tree caveat. The one new DO route, `POST /_do/push/verify`, re-derives the closure from the stored pack itself: a walk from the command tips that resolves every referenced id against the 2.3 reader query or this push's `ingesting` pack and parses every commit, tree and tag it has to trust, including canonical tree order. There is no pass A and no `pending/` key on this path (2.4): the pack the sibling writes is already normalized, and `resolve_and_normalize` is not re-run. REGISTRY (A9):

```
REGISTRY tui-rpc-push
  routes    POST /_do/push/verify   {push_id, pack_id, tips:[oid]} -> {ok:true} | {ok:false, missing}   awaits: R2 reads
  JobKind   none
  tables    none
  R2 keys   none   (sibling writes the existing 2.2 prefix r/<repo_id>/packs/<pack>.pack)
```

## Primitives
- `Stub::fetch_with_request` DO-to-DO on the repo DO's internal routes: verified (memo section 1, spike). Typed DO RPC (`extends DurableObject` method calls): experimental in workers-rs, not used. Cross-script DO binding (`script_name` in wrangler) and a shared R2 bucket binding across two Workers: GA per Cloudflare docs (first-pass primitives), **not re-measured** on workerd.
- `worker::Request::new_with_init` + `RequestInit::{with_method, with_body}` for the sibling's JSON stub requests: present in `worker` 0.8.5 source, **unverified at runtime** (same caveat as two-phase-push; the spike forwarded the client request).
- `State::storage().sql().exec`, `SqlCursor::{one, to_array}`, sync span atomicity, `SELECT changes()`: verified (memo, spike), measured 1 / 0 / 1 and one-winner (platform-facts #1, #4).
- `Bucket::read_entries` / `read_range` from inside the DO, charged per A1: API verified; real-R2 behaviour measured on the **local simulator only** (#6); the DO subrequest limit is **not enforced locally** (#7), so `ReqBudget` is the only guard until deploy (7.1).
- `gix_object::{CommitRef, TagRef}::from_bytes` and `TreeRefIter::from_bytes` over decoded entry bytes; `gix_object::compute_hash` for the sibling's ids: `compute_hash` ran on workerd in the spike with ids matching `git verify-pack`; `CommitRef`/`TagRef`/`TreeRefIter` are CI-built for wasm32 (memo section 3); the exact field/method names (`c.tree`, `c.parents`, `e.mode.is_tree`, `e.filename`) are **unverified** against 0.64.1.
- `codec::entry_header` / `codec::decode_entry` (1.2): verified in source (content-addressed-r2-keys); a delta header in a stored pack is `Error::Internal`, so `decode_entry` doubles as the "normalized" check.
- `PackWriter::{create, append_entry, flush_if_full, finish}` in the sibling's own Worker (A1 signatures): the sibling vendors ~80 lines (`codec::encode_entry` + `PackWriter` of the content-addressed proof); real-R2 5 MiB part minimum and equal-size rule are re-run on first deploy (#6).
- `Index::lookup_in_pack` (write-back from two-phase-push): presence of an id in the caller's own `ingesting` pack (2.5 subtraction).
- `PushId::random` / `PackId::random` in the sibling: `web_sys::Crypto::get_random_values_with_u8_array` binding path **unverified** (8.2).
- `js_sys::Date::now()` for the commit timestamp and `now_ms()`: standard `js-sys`.
- Alarms: nothing here calls `set_alarm`; `commit_push` step 7 enqueues `GcMark` and the DO's route wrapper calls `jobs::rearm().await` after the sync span (4.1, A3). Second `setAlarm` cancels the first: measured (#5).
- git fact used twice: tree entries sort by name with directories compared as `name/`, which is equivalent to bytewise order of the full path; a pack whose entries are all full objects is valid in any entry order (2.1). Checked against the git tree format, exercised by the added scenario.

## Proof code
```rust
// src/repo_do/verify.rs + the sibling-side caller reference (workspace DO's own Worker).
// worker 0.8.5, gix-object 0.64.1, gix-hash 0.26.2. `q`, `json`, `oid`, `now_ms`, `sql`, `bucket()` are the
// RepoDo helpers of repo-do-ref-authority; `stub_json`, `RepoRoute`, `lookup`, `IndexSink`, `Begin`,
// `CommitRequest`, `CmdDto`, `CommitResponse` are the edge helpers and DTOs of two-phase-push /
// repo-do-ref-authority (wire::http per A8, which is also where VerifyDto/VerifyOut live).
use std::collections::HashSet;
use bstr::{BStr, BString, ByteSlice};
use gix_hash::{Kind as H, ObjectId};
use gix_object::{CommitRef, Kind, TagRef, TreeRefIter};
use worker::{Response, SqlStorageValue as V, Stub};
use crate::{error::Error, store::{codec, keys, Bucket, Index, ObjLoc, ObjRow, PackId, PackMeta, PackWriter, PushId, RepoId},
            ReqBudget, RepoDo};

// ------------------------------- git-edge side: the one new route (REGISTRY above) ----------------
const VERIFY_MAX_LOADED: usize = 100_000;   // structural objects actually read; beyond -> Error::Limit
const VERIFY_MAX_SEEN: usize = 1_000_000;   // same bound as MAX_LINKS (A5)
const VERIFY_CHUNK: usize = 64;             // A4: at most 64 coalesced spans per read_entries call
#[derive(serde::Deserialize, serde::Serialize)] pub struct VerifyDto { pub push_id: String, pub pack_id: String, pub tips: Vec<String> }
#[derive(serde::Deserialize, serde::Serialize)] pub struct VerifyOut { pub ok: bool, pub missing: Option<String> }
#[derive(serde::Deserialize)] struct StateRow { state: String }
#[derive(serde::Deserialize)] struct PackRow { state: String, push_id: String, count: i64, commit_lo: i64, commit_hi: i64 }
#[derive(serde::Deserialize)] struct N { n: i64 }

impl RepoDo {
    /// Dispatch arm (1.3, awaits R2 like /_do/fetch):
    ///   (Method::Post, "/_do/push/verify") => return self.push_verify(&parse(&body)?, &mut ReqBudget::paid()).await,
    /// Server-side closure check for packs a sibling DO wrote to R2 itself: every id reachable from `tips`
    /// must resolve live (2.3) or in this ingesting pack; every commit/tree/tag read must parse; tree entries
    /// must be in git order. The caller's own claims (link lists, existence) are never consulted.
    pub async fn push_verify(&self, b: &VerifyDto, budget: &mut ReqBudget) -> Result<Response, Error> {
        let st = self.q("SELECT state FROM pushes WHERE id=?", vec![V::from(b.push_id.as_str())])?
            .to_array::<StateRow>()?.into_iter().next().map(|r| r.state);
        if st.as_deref() != Some("open") { return Err(Error::Conflict("push not open".into())); }
        let p = self.q("SELECT state,push_id,count,commit_lo,commit_hi FROM packs WHERE id=?",
                       vec![V::from(b.pack_id.as_str())])?.to_array::<PackRow>()?.into_iter().next()
            .ok_or_else(|| Error::Conflict("unknown pack".into()))?;
        if p.state != "ingesting" || p.push_id != b.push_id { return Err(Error::Conflict("pack not ingesting for this push".into())); }
        let have = self.q("SELECT COUNT(*) AS n FROM objects WHERE pack_id=?",
                          vec![V::from(b.pack_id.as_str())])?.one::<N>()?.n;      // catches a partial index post
        if have != p.count { return Err(Error::Conflict(format!("pack says {} objects, index has {have}", p.count))); }
        let (pack, bucket) = (PackId(b.pack_id.clone()), self.bucket()?);
        if p.commit_lo != i64::MAX {                                   // spot-check the 7.4 commit region start
            let lo = u64::try_from(p.commit_lo).map_err(|_| Error::Internal("commit_lo".into()))?;
            let len = u64::try_from(p.commit_hi.saturating_sub(p.commit_lo))
                .map_err(|_| Error::Internal("commit_hi".into()))?.min(256 << 10);
            let head = bucket.read_range(&keys::pack(&bucket.repo, &pack), lo, len, budget).await?;
            if codec::entry_header(&head)?.0 != Kind::Commit { return Err(Error::Storage("commit_lo is not a commit".into())); }
        }
        let sql = self.sql(); let idx = Index(&sql);
        let mut seen: HashSet<ObjectId> = HashSet::new();
        let mut pending: Vec<ObjectId> = b.tips.iter().map(|h| oid(h)).collect::<Result<_, Error>>()?;
        let mut loaded = 0usize;
        while !pending.is_empty() {
            let mut structural: Vec<(ObjectId, ObjLoc)> = Vec::new();  // sync span: resolve only, no reads
            for id in pending.drain(..) {
                if !seen.insert(id) { continue; }
                if seen.len() > VERIFY_MAX_SEEN { return Err(Error::Limit("push too large to verify".into())); }
                match idx.lookup(&[id])?.into_iter().next().flatten().or(idx.lookup_in_pack(&id, &pack)?) {
                    None => return json(serde_json::json!({"ok": false, "missing": id.to_string()})),
                    Some(l) if l.pack.0 == pack.0 && l.kind != Kind::Blob => structural.push((id, l)),
                    Some(_) => {}                                      // live in an older pack: closed by 2.5
                }
            }
            for chunk in structural.chunks(VERIFY_CHUNK) {
                let entries = bucket.read_entries(chunk, budget).await?; // the only awaits after the guard span
                loaded = loaded.saturating_add(entries.len());
                if loaded > VERIFY_MAX_LOADED { return Err(Error::Limit("push too large to verify".into())); }
                for (_id, bytes) in &entries {
                    let (kind, data) = codec::decode_entry(bytes)?;      // a delta header -> Storage: not normalized
                    links_of(kind, &data, &mut pending)?;
                }
            }
        }
        json(serde_json::json!({"ok": true}))
    }
}
/// Ids referenced by one resolved object, plus fsck structure: commits must parse with tree/parents/signature
/// headers, tree entries must be strictly increasing under git's name-'/'-for-dirs order.
fn links_of(kind: Kind, data: &[u8], out: &mut Vec<ObjectId>) -> Result<(), Error> {
    match kind {
        Kind::Commit => { let c = CommitRef::from_bytes(data).map_err(|e| Error::Storage(format!("commit: {e}")))?;
                          out.push(c.tree); out.extend(c.parents); }   // field names unverified in 0.64.1
        Kind::Tag    => out.push(TagRef::from_bytes(data).map_err(|e| Error::Storage(format!("tag: {e}")))?.target),
        Kind::Tree   => { let mut prev: Option<(Vec<u8>, bool)> = None;
            for e in TreeRefIter::from_bytes(data) {
                let e = e.map_err(|e| Error::Storage(format!("tree: {e}")))?;
                let (name, dir) = (e.filename.to_vec(), e.mode.is_tree());
                if let Some((pn, pd)) = &prev {
                    if tree_key(pn, *pd) >= tree_key(&name, dir) { return Err(Error::Storage("tree entries out of order".into())); }
                }
                prev = Some((name, dir)); out.push(e.oid);
            } }
        Kind::Blob => {}                                               // never reached: blobs are not loaded
    }
    Ok(())
}
fn tree_key(name: &[u8], dir: bool) -> Vec<u8> { let mut k = name.to_vec(); if dir { k.push(b'/'); } k }

// ------------------------- sibling side: reference caller (workspace DO's Worker) ------------------
pub struct FileEntry { pub path: BString, pub mode: u32 /* 0o100644 | 0o100755 | 0o120000 */, pub data: Vec<u8> }
const MAX_FILE: u64 = 16 << 20;                                        // A7 single-object cap
fn emit(out: &mut PackWriter, rows: &mut Vec<ObjRow>, id: ObjectId, kind: Kind, data: &[u8]) -> Result<(), Error> {
    let (offset, len) = out.append_entry(kind, data)?;
    rows.push(ObjRow { sha: id, idx: u32::try_from(rows.len()).map_err(|_| Error::Limit("count".into()))?, offset, len,
                       kind, size: u64::try_from(data.len()).map_err(|_| Error::Limit("size".into()))? });
    Ok(())
}
/// One workspace snapshot -> one commit on `branch`, through the foundation routes only. Ordering is section 3:
/// packs row 'ingesting' first (A5), pack durable, rows, verify, commit. The caller supplies `parent` = the ref
/// value it based the snapshot on, which becomes the CAS old oid.
pub async fn push_snapshot(stub: &Stub, repo: &RepoRoute, bucket: &Bucket, files: &[FileEntry],
                           branch: &str, msg: &BStr, parent: Option<ObjectId>, budget: &mut ReqBudget)
    -> Result<CommitResponse, Error> {
    let push = PushId::random();
    let begin: Begin = stub_json(stub, repo, "/_do/push/begin",
        &serde_json::json!({"push_id": push.0, "principal": "workspace"}), budget).await?;
    let mut leaves: Vec<(BString, u32, ObjectId, &[u8])> = Vec::with_capacity(files.len());
    for f in files {                                                   // pass 1: hash only, zero R2 bytes
        if u64::try_from(f.data.len()).map_err(|_| Error::Limit("size".into()))? > MAX_FILE {
            return Err(Error::Limit("file > 16 MiB (A7)".into())); }
        leaves.push((f.path.clone(), f.mode,
            gix_object::compute_hash(H::Sha1, Kind::Blob, &f.data).map_err(|e| Error::Internal(e.to_string()))?,
            f.data.as_slice()));
    }
    leaves.sort_by(|a, b| a.0.cmp(&b.0));                              // full-path byte order == git per-level order
    let mut trees: Vec<(Vec<u8>, ObjectId)> = Vec::new();              // (body, id), children before parents
    let root = emit_level(&leaves, 0, leaves.len(), 0, &mut trees)?;
    let who = format!("workspace <bot@grok-pi> {} +0000", now_ms() / 1000);
    let mut cbody: Vec<u8> = format!("tree {root}\n").into_bytes();
    if let Some(p) = parent { cbody.extend(format!("parent {p}\n").into_bytes()); }
    cbody.extend(format!("author {who}\ncommitter {who}\n\n").into_bytes());
    cbody.extend_from_slice(msg);                                      // message bytes verbatim, no UTF-8 assumed
    let commit = gix_object::compute_hash(H::Sha1, Kind::Commit, &cbody).map_err(|e| Error::Internal(e.to_string()))?;
    let mut ids: Vec<ObjectId> = leaves.iter().map(|l| l.2).chain(trees.iter().map(|t| t.1)).collect();
    ids.push(commit); ids.sort_unstable(); ids.dedup();
    let mut live: HashSet<ObjectId> = HashSet::new();                  // dedup: anything already live is not rewritten
    for c in ids.chunks(1_000) {
        for (id, l) in lookup(stub, repo, c, None, budget).await? { if l.is_some() { live.insert(id); } }
    }
    let pack = PackId::random();
    let mut sink = IndexSink { stub, repo, pack: pack.clone(), push: push.clone(), links: Vec::new() };
    sink.post(&PackMeta::EMPTY, &[], budget).await?;                   // packs row 'ingesting' before any R2 byte (A5)
    let expected = u32::try_from(ids.iter().filter(|i| !live.contains(i)).count()).map_err(|_| Error::Limit("count".into()))?;
    let mut out = PackWriter::create(bucket, &keys::pack(&RepoId(begin.repo_id), &pack), expected, budget).await?;
    let (mut rows, mut written) = (Vec::<ObjRow>::new(), HashSet::new());
    for (_, _, id, data) in &leaves {                                  // entries in any order: no deltas (2.1)
        if !live.contains(id) && written.insert(*id) { emit(&mut out, &mut rows, *id, Kind::Blob, data)?; out.flush_if_full(budget).await?; }
    }
    for (body, id) in &trees { if !live.contains(id) && written.insert(*id) { emit(&mut out, &mut rows, *id, Kind::Tree, body)?; } }
    emit(&mut out, &mut rows, commit, Kind::Commit, &cbody)?;          // commits are new by construction (timestamp)
    out.flush_if_full(budget).await?;                                  // + index posts of 10_000 rows inside the loops (elided)
    let meta = out.finish(budget).await?;                              // section 3 (1): pack durable in R2
    sink.post(&meta, &rows, budget).await?;                            // section 3 (2): real count/bytes/commit_lo-hi
    let v: VerifyOut = stub_json(stub, repo, "/_do/push/verify",
        &serde_json::json!({"push_id": push.0, "pack_id": pack.0, "tips": [commit.to_string()]}), budget).await?;
    if !v.ok { return Err(Error::Internal(format!("verify missing {}", v.missing.unwrap_or_default()))); }
    stub_json(stub, repo, "/_do/push/commit", &CommitRequest { push_id: push.0, pack_id: Some(pack.0),
        principal: "workspace".into(), commands: vec![CmdDto { name: format!("refs/heads/{branch}"),
            old: parent.map_or_else(|| "0".repeat(40), |o| o.to_string()), new: commit.to_string() }] }, budget).await
}
/// files[lo..hi] share the path prefix [..off] and are full-path sorted. Emits one git tree whose entries are in
/// order at this level, recursing into each maximal run that shares the next path component (a subdirectory).
fn emit_level(files: &[(BString, u32, ObjectId, &[u8])], lo: usize, hi: usize, off: usize,
              trees: &mut Vec<(Vec<u8>, ObjectId)>) -> Result<ObjectId, Error> {
    let (mut body, mut i) = (Vec::<u8>::new(), lo);
    while i < hi {
        let (path, mode, oid, _) = files.get(i).ok_or_else(|| Error::Internal("range".into()))?;
        let rest = path.get(off..).ok_or_else(|| Error::Internal("prefix".into()))?;
        match rest.iter().position(|b| *b == b'/') {
            None => { body.extend(format!("{:o} ", *mode).as_bytes()); body.extend(rest); body.push(0);
                      body.extend(oid.as_slice()); i += 1; }
            Some(s) => {
                let mut j = i + 1;
                while j < hi && files.get(j).and_then(|f| f.0.get(off..)).map(|r| r.starts_with(&rest[..=s])).unwrap_or(false) { j += 1; }
                let tid = emit_level(files, i, j, off + s + 1, trees)?;
                body.extend(b"40000 "); body.extend(&rest[..s]); body.push(0); body.extend(tid.as_slice());
                i = j;
            }
        }
    }
    let id = gix_object::compute_hash(H::Sha1, Kind::Tree, &body).map_err(|e| Error::Internal(e.to_string()))?;
    trees.push((body, id));
    Ok(id)
}
```

## Why it works
- **The phase split is git's own; the transport is immaterial.** `receive-pack` carries exactly two things — a pack of objects and a ref-command list — and the sibling carries the same two: a normalized pack in R2 plus `CommitRequest.commands`. `commit_push` is the identical phase-two entry point the HTTP path uses (section 3, repo-do-ref-authority), so the CAS semantics the first review verified (`cur === oldOid` serialized by one DO) are literally the same statements: `UPDATE refs ... WHERE name=? AND target=?` plus `changes()`.
- **Storage-model conformance is total, not partial.** Objects at rest exist only inside normalized packs (2.1). The sibling writes `packs/<pack>.pack` through the `PackWriter` shape — 8 MiB parts, header count final from byte 0, SHA-1 trailer — with `codec::encode_entry` entries, byte-identical to what `pack::generate` copies into outgoing packs. Clone and fetch therefore read these objects through the 2.3 query and 7.2 range reads with zero new read-path code, and `git fsck` sees ordinary pack entries.
- **The dangling-tree caveat is closed server-side.** `push_verify` consumes no caller claims: `tips` only selects the walk roots. Every referenced id is resolved through `Index::lookup` (live packs only, 2.3) or `lookup_in_pack` (this push's rows); ids live in an older pack are not expanded because their closure holds by the 2.5 invariant; every commit, tree and tag in this pack that is reachable from a tip must parse, and every tree must be in canonical order — the fsck blocker — or the route returns `Err`/`{ok:false}` and `/_do/push/commit` is never called.
- **The subrequest blocker collapses by construction.** Sibling side: 1 `begin` + `⌈ids/1000⌉` lookups + `1 + ⌈rows/10000⌉` index posts + `⌈pack_bytes/8 MiB⌉` part uploads + 1 `verify` + 1 `commit` — about 20 calls for a 10,000-file workspace, versus roughly 20,000 PUTs and HEADs in the first pass. DO side: no per-oid `head` at all; verify costs `⌈structural/64⌉` coalesced `read_entries` calls (A4 chunking), and because the pack is written contiguously the coalescer of 7.2 merges most of them into a handful of range reads.
- **Dedup is a property of content addressing.** `live` is the set of candidate ids already in live packs; unchanged files and unchanged subtrees produce ids that are already live and are skipped, so a no-change push writes only the commit object. Duplicate shas across live packs are legal (2.3) and `GcConsolidate` removes them later; `IndexSink`/`insert_objects` posts are `ON CONFLICT DO NOTHING`, so a retried index post converges (content-addressed-r2-keys).
- **The GC/orphan race is the contract's, already analysed.** The `pushes` row exists from `begin` and the `packs` row is `ingesting` before the first part is uploaded (A5 upsert), so every R2 byte is accounted to the Janitor: it kills only packs of `expired`/`rejected` pushes (5.2) and deletes keys only after `GRACE` (5.3). A `GcSweep` between verify and commit bumps `gc_epoch`, which commit step 2 compares inside its sync span — the section 5 race analysis applies verbatim with verify standing in for the edge's lookups, so the push is rejected, never accepted with a hole.
- **Crash and retry semantics match the foundation's.** A sibling crash before commit leaves an `open` push plus an `ingesting` pack that expire under `PUSH_TIMEOUT` and are deleted after `GRACE`; a retry uses fresh `push_id`/`pack_id` so no key is ever rewritten. The committed-but-unacked case answers `ng <ref> failed to update ref`, and the sibling confirms the earlier attempt landed by reading `GET /_do/refs` and comparing `target == new`.
- **Contract rules exercised.** Every R2 and stub call takes `budget: &mut ReqBudget` (A1); `Storage`/`Internal`/`Conflict` propagate as `Err` out of `fetch` (A2) — `verify` never swallows a DO-side error into a response; nothing here calls `set_alarm`, and `commit_push`'s `GcMark` enqueue is armed by the route wrapper per A3; the 16 MiB object cap (A7) is enforced as a file-size limit before hashing; `VerifyDto`/`VerifyOut` sit in `wire::http` (A8); all additions are inside one REGISTRY block (A9); pass A and `pending/` do not exist on this path (A10).

## Changes from the first pass
| First-pass finding (quoted) | Kind | How addressed |
|---|---|---|
| "Tree builder is fsck-invalid for nested paths (`fullPathname`) -- must be implemented before this "lands", not after." | blocker | Closed twice. `emit_level` builds real nested trees with byte-sorted entries (full-path byte order is equivalent to git's per-level `name/` rule), correct modes, and binary bodies. Then `push_verify` re-parses every reachable tree server-side and rejects out-of-order or unparseable entries (`tree_key`, `TreeRefIter`), so an fsck-invalid tree can never reach a live ref even if the sibling is buggy. |
| "1000-subrequest cap per invocation with no chunking design: pushes of >~998 files fail on both DOs." | blocker | Gone structurally. Per-object R2 PUTs and HEADs no longer exist: one multipart upload (one call per 8 MiB part) plus batched stub calls — about 20 calls for a 10,000-file workspace (see Why it works). The DO side issues zero per-oid HEADs; verify uses coalesced `read_entries` chunked at 64 spans (A4). |
| "GC/orphan race: objects are unreferenced until the ref flips; GC needs a grace window." | caveat | Closed by contract mechanism: `pushes` row at `begin`, `packs` row `ingesting` before any R2 byte, Janitor kills only packs of expired/rejected pushes (5.2) and deletes keys after GRACE (5.3); the verify-to-commit gap is covered by `gc_epoch` in commit step 2 (section 5 analysis). |
| "Retry after committed-but-unacked push returns `stale`; needs `cur === newOid` idempotency." | caveat | Index posts are idempotent (`ON CONFLICT`, one sync span each); a full retry uses fresh `push_id`/`pack_id`, so no key is rewritten and duplicate live `objects` rows are legal (2.3). The second commit's CAS returns `ng failed to update ref`; the sibling reads `GET /_do/refs` — `target == new` means the first attempt committed. No new code. |
| "No fast-forward check; `stale` != `non-fast-forward`." | caveat | Same exposure as the HTTP path and as stock `git` receive-pack defaults: the CAS on the caller-supplied old oid is the only check, `--force` is a client-side flag, and the contract builds no ancestry check (section 12 lists none). Not strengthened — noted in Known limits; a `denyNonFastForwards` equivalent would be a separate idea. |
| "Binary files, exec bits, symlinks unsupported by the `files` schema as used." | caveat | `FileEntry` carries `data: Vec<u8>` (no `TextEncoder`, no string content) and `mode` in `0o100644 | 0o100755 | 0o120000`; `emit_level` writes the mode it was given, `0o40000` for directories. What the sibling stores per file is grok-pi's schema decision; the git-edge side now accepts any mode byte-pattern. |
| "RPC-in-account is "implicitly trusted": `commitPush` never validates that `oids` actually cover the tree closure, so a buggy caller can publish a ref with a dangling tree." | caveat | `push_verify` walks the closure from the tips against stored pack bytes and index rows — there is no `oids` claim to trust. A missing id yields `{ok:false, missing}`, a malformed one an `Err`, and commit is never reached. Advisory rather than enforced — see Known limits. |
| "Cost: N R2 class-A PUTs per push with no delta or skip-cache." | caveat | One multipart upload per push regardless of file count, and the `live`-set dedup via `/_do/push/lookup` skips every object already stored — a push that changes one file writes one blob plus its ancestor trees. The skip-cache the review asked for is three lines of `live.contains`. |

## Known limits
- The first pass's typed-RPC call (`stub.commitPush({...})`) is weakened to `Stub::fetch_with_request` on internal JSON routes: workers-rs RPC is experimental (memo section 1). What is preserved is the substance — no git-HTTP endpoint, no pkt-line, no edge hop — not the syntax. If the sibling is TypeScript it may use real DO RPC method calls against a JS shim route instead; the DTO contract (`wire::http`) is the same.
- `push_verify` is advisory: `commit_push` does not require it, so a sibling could skip the call and a dangling tree would land. The caller is in-account code (same trust domain as the edge), and making it mandatory needs a `pushes.verified_at`-style flag — a small write-back to CONTRACTS.md, left out to keep the route table minimal. Also: verify covers only objects reachable from the tips — unreachable pack entries are junk accepted unverified, as git does (2.5) — and `CommitRef`/`TagRef` strictness on missing `author`/`committer` headers is unverified (the builder emits both; worst case is an fsck `missingAuthor` on the clone, not corruption).
- `pack_id` is generated by the sibling, not "by the edge at push begin" (1.2 — small write-back): same 32-hex format, and the A5 upsert's `WHERE state='ingesting' AND push_id=excluded.push_id` prevents a guessed id from attaching to another push's row.
- The sibling must maintain `commit_lo`/`commit_hi` correctly (7.4 commit-region reads); `push_verify` spot-checks the entry at `commit_lo` only. A wrong region makes fetches fail (`CommitRef` parse error), not corrupt data; `bytes` is trusted beyond the `count` cross-check.
- Sibling memory: the leaf list and tree bodies cost roughly 50 bytes per path plus one file's content and the 8 MiB part buffer; a workspace with millions of paths should read its `files` table twice (hash pass, write pass) instead of borrowing all contents — the reference code borrows for clarity. `files` is iterated once per pass here.
- A file over 16 MiB (A7) is `Error::Limit` before hashing; chunked objects and LFS are out of scope (section 12). A push whose reachable set exceeds `VERIFY_MAX_SEEN`/`VERIFY_MAX_LOADED` fails verify with `Limit` — the fallback is splitting the push or using the HTTP receive-pack ingest path.
- A sibling crash between `create_multipart_upload` and `finish`/`abort` leaves an incomplete upload — same unverified R2-lifecycle caveat as two-phase-push; a completed pack is never untracked because the `ingesting` row is posted before the first part.
- `Request::new_with_init`/`RequestInit` on stub calls, `web_sys::Crypto` id generation, and the `script_name` cross-script binding are unverified at runtime (see Primitives); the DO subrequest limit is unenforced locally (platform-facts #7).
- Scenarios: stock `git` cannot exercise this path (section 11 permits a harness substitute). The harness adds a test-only route performing `push_snapshot` against `wrangler dev`. Added (two): (a) workspace push of nested paths including an exec-bit file and a symlink — clone, `git fsck --strict` clean, modes preserved; (b) a hand-built pack whose tree names a blob absent from pack and repo — `verify` returns `{ok:false, missing}`, the ref is never moved, and the `ingesting` pack dies via the Janitor after `PUSH_TIMEOUT` + `GRACE`.

## Depends on
- repo-do-ref-authority
- two-phase-push
- refs-sqlite-objects-r2
- content-addressed-r2-keys
- gc-and-repack-alarm
