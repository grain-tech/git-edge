# Refs in DO SQLite, objects in R2

> Second pass · Idea #2 · verdict: **lands with caveats** · feasibility 4/5 · reliability 4/5 · correctness 4/5 (first pass 4/2/3)
> First pass: [proof](./archived/refs-sqlite-objects-r2.md) · [review](../reviews/archived/refs-sqlite-objects-r2.md) · Second pass: [review](../reviews/refs-sqlite-objects-r2.md) · Contract: [CONTRACTS.md](../../CONTRACTS.md)


## Mechanism
This idea is subsumed by CONTRACTS.md: the split it proposed is now the `store` module (section 1.2, R2 key layout 2.2, `packs`/`objects` index 2.3) plus the `repo_do` module (`refs`, `reflog`, `pushes`, `meta` tables, sections 3 and 8), so the proof below is written as those two modules rather than as a sketch. Refs never leave the DO: `RepoDo::list_refs` and `RepoDo::commit_push` are sync spans over SQLite, and the compare-and-swap outcome is `SELECT changes()` (section 3). Objects never enter the DO: they live only inside normalized packs at `r/<repo_id>/packs/<pack_id>.pack`, and every read is `Index::lookup` (one SQLite row, live packs only) followed by `Bucket::read_entries` (coalesced R2 range reads, section 7.2). The two halves meet at exactly one point, `/_do/push/commit`, which flips a pack from `ingesting` to `live` and moves refs in the same span, after the edge has already made the pack durable and indexed (ordering rule, section 3).

## Primitives
- DO SQLite through `worker::SqlStorage::exec` (synchronous, `SqlCursor::to_array`): supported in `worker` 0.8.5 (memo section 1). Exact binding-argument type is **unverified**; the code wraps it in one helper.
- `SELECT changes()` as the CAS outcome: **measured** (platform-facts #1); `rows_written` is never read.
- `transactionSync` binding in `worker` 0.8.5: **unverified**; the span holds without it because no other DO event runs between two awaits (platform-facts #4).
- R2 range read `Bucket::get(key).range(Range).execute()` then `ObjectBody::bytes()`: supported (memo); range read on a multi-GB object verified on the **local simulator only** (platform-facts #6). The `Range` variant name is **unverified**.
- R2 multipart (`PackWriter`, used by ingest, referenced here): local simulator only (#6); 5 MiB minimum and equal-size rule re-checked on first deploy.
- DO alarm: set only by `jobs::rearm` (section 4.1); second `set_alarm` cancels the first, **measured** (#5). `commit_push` only calls `jobs::enqueue`.
- `js_sys::Date::now()` for timestamps inside the DO: a synchronous host call, no promise, allowed inside a sync span.
- `gix_hash::ObjectId::{from_hex, to_hex, is_null}`, `gix_validate::reference::name_partial`, `gix_object::Kind`: CI-built for `wasm32-unknown-unknown` (memo section 3).
- Subrequest limit inside a DO: **not enforced locally** (#7); `ReqBudget::charge` is the only guard.

## Proof code
```rust
// src/repo_do/refs.rs + src/store/{index,bucket}.rs — CONTRACTS.md 1.2, 1.3, 2.3, 3, 7.2
use bstr::BString;
use gix_hash::ObjectId;
use gix_object::Kind;
use serde::Deserialize;
use worker::{Range, SqlCursor, SqlStorage, SqlStorageValue};
use crate::{error::Error, jobs::{self, JobKind}, store::{keys, Index, ObjLoc, PackId},
            wire::{RefCommand, RefResult}, ReqBudget, RepoDo};
// error.rs: impl From<worker::Error> for Error { .. Error::Storage(e.to_string()) }

const GAP: u64 = 256 * 1024;      // 7.2: merge neighbours closer than this
const SPAN: u64 = 8 << 20;        // 7.2: split merged spans at 8 MiB
const GC_QUIET_MS: i64 = 10 * 60 * 1000;

/// `SqlStorage::exec(query, bindings)` per the memo; the bindings parameter shape is unverified,
/// so it is touched in exactly one place.
fn exec(sql: &SqlStorage, q: &str, args: Vec<SqlStorageValue>) -> Result<SqlCursor, Error> {
    Ok(sql.exec(q, Some(args))?)
}
#[derive(Deserialize)] struct N { n: i64 }
/// The only CAS oracle in the crate (section 3): issued right after the statement, same span.
fn changed(sql: &SqlStorage) -> Result<bool, Error> {
    Ok(exec(sql, "SELECT changes() AS n", vec![])?.to_array::<N>()?.first().map(|r| r.n) == Some(1))
}
#[derive(Deserialize)] struct PushRow { state: String, gc_epoch: i64 }
#[derive(Deserialize)] struct RefDb { name: String, target: String }
#[derive(Deserialize)] struct LocRow { pack_id: String, idx: u32, offset: u64, len: u32, kind: u8, size: u64 }
pub struct CommitRequest { pub push_id: String, pub pack_id: Option<PackId>, pub principal: String,
                           pub commands: Vec<RefCommand> }
pub struct CommitResponse { pub results: Vec<RefResult> }

impl RepoDo {
    /// GET /_do/refs: refs are tiny and hot; this never touches R2. Peeling: see Known limits.
    pub fn list_refs(&self) -> Result<(Option<BString>, Vec<RefRow>), Error> {
        let sql = self.state.storage().sql();
        let head = self.meta_str("head")?;                       // 'refs/heads/main' (section 3), symbolic only
        let mut refs = Vec::new();
        for r in exec(&sql, "SELECT name, target FROM refs ORDER BY name", vec![])?.to_array::<RefDb>()? {
            let target = ObjectId::from_hex(r.target.as_bytes()).map_err(|_| Error::Internal("bad oid in refs".into()))?;
            refs.push(RefRow { name: r.name.into(), target, peeled: None });
        }
        Ok((Some(head.into()), refs))                            // wire writes `unborn HEAD symref-target:..` if no row matches
    }

    /// POST /_do/push/commit. One sync span: JSON already parsed by the caller, no await until return.
    pub fn commit_push(&self, req: &CommitRequest) -> Result<CommitResponse, Error> {
        let sql = self.state.storage().sql();
        let now = js_sys::Date::now() as i64;                    // host number, not client bytes; cast is safe
        let push = exec(&sql, "SELECT state, gc_epoch FROM pushes WHERE id=?", vec![req.push_id.clone().into()])?
            .to_array::<PushRow>()?.into_iter().next().ok_or_else(|| Error::Conflict("unknown push".into()))?;
        if push.state != "open" { return Err(Error::Conflict(format!("push is {}", push.state))); }   // step 1
        if self.meta_i64("gc_epoch")? != push.gc_epoch {                                                // step 2
            let results = req.commands.iter().map(|c| RefResult::Ng(c.name.clone(), "gc ran during push, retry")).collect();
            exec(&sql, "UPDATE pushes SET state='rejected', ended_at=? WHERE id=?", vec![now.into(), req.push_id.clone().into()])?;
            return Ok(CommitResponse { results });
        }
        if let Some(p) = &req.pack_id {                                                                 // step 3
            exec(&sql, "UPDATE packs SET state='live' WHERE id=? AND state='ingesting'", vec![p.0.clone().into()])?;
            if !changed(&sql)? { return Err(Error::Conflict("pack not in state ingesting".into())); }
        }                                                        // None: delete-only or 0-object push (2.4), no pack row
        let head = self.meta_str("head")?;
        let (mut results, mut any_ok) = (Vec::with_capacity(req.commands.len()), false);
        for c in &req.commands {                                                                        // step 4
            let r = self.apply_ref(&sql, c, &head, req, now)?;
            any_ok |= matches!(r, RefResult::Ok(_));
            results.push(r);
        }
        if any_ok { exec(&sql, "UPDATE meta SET value=value+1 WHERE key='refs_version'", vec![])?; }   // step 5
        let result_json = serde_json::to_string(&results).map_err(|e| Error::Internal(e.to_string()))?;
        exec(&sql, "UPDATE pushes SET state='committed', ended_at=?, result=? WHERE id=?",                // step 6
             vec![now.into(), result_json.into(), req.push_id.clone().into()])?;
        if any_ok { jobs::enqueue(&sql, JobKind::GcMark, now + GC_QUIET_MS, "{}")?; }                  // step 7, dedups
        Ok(CommitResponse { results })
    }

    /// One RefCommand, independent of the others, git's default (section 3). Reasons are git's own strings.
    fn apply_ref(&self, sql: &SqlStorage, c: &RefCommand, head: &str, req: &CommitRequest, now: i64) -> Result<RefResult, Error> {
        let Ok(name) = std::str::from_utf8(&c.name) else { return Ok(RefResult::Ng(c.name.clone(), "funny refname")) };
        if gix_validate::reference::name_partial(c.name.as_ref()).is_err() { return Ok(RefResult::Ng(c.name.clone(), "funny refname")); }
        let (old, new) = (c.old.to_hex().to_string(), c.new.to_hex().to_string());
        let ok = if c.new.is_null() {
            if name == head { return Ok(RefResult::Ng(c.name.clone(), "deletion of the current branch prohibited")); }
            exec(sql, "DELETE FROM refs WHERE name=? AND target=?", vec![name.into(), old.clone().into()])?;
            changed(sql)?
        } else {
            // re-guard of 2.5: the tip must be in a live pack *now*, in this span, not only at lookup time
            if Index(sql).lookup(&[c.new])?.first().map_or(true, Option::is_none) {
                return Ok(RefResult::Ng(c.name.clone(), "missing necessary objects"));
            }
            if c.old.is_null() {
                exec(sql, "INSERT INTO refs(name,target,updated_at) VALUES(?,?,?) ON CONFLICT DO NOTHING",
                     vec![name.into(), new.clone().into(), now.into()])?;
            } else {
                exec(sql, "UPDATE refs SET target=?, updated_at=? WHERE name=? AND target=?",
                     vec![new.clone().into(), now.into(), name.into(), old.clone().into()])?;
            }
            changed(sql)?
        };
        if !ok { return Ok(RefResult::Ng(c.name.clone(), "failed to update ref")); }
        exec(sql, "INSERT INTO reflog(name,old,new,push_id,principal,at) VALUES(?,?,?,?,?,?)",
             vec![name.into(), old.into(), new.into(), req.push_id.clone().into(), req.principal.clone().into(), now.into()])?;
        Ok(RefResult::Ok(c.name.clone()))
    }
}

impl<'s> Index<'s> {
    /// The reader query of 2.3, verbatim. Rows of `ingesting`/`dead` packs are invisible.
    pub fn lookup(&self, ids: &[ObjectId]) -> Result<Vec<Option<ObjLoc>>, Error> {
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            let row = exec(self.0, "SELECT o.pack_id, o.idx, o.offset, o.len, o.kind, o.size FROM objects o \
                                    JOIN packs p ON p.id = o.pack_id WHERE o.sha = ? AND p.state = 'live' LIMIT 1",
                           vec![id.to_hex().to_string().into()])?.to_array::<LocRow>()?.into_iter().next();
            out.push(match row {
                None => None,
                Some(r) => Some(ObjLoc { pack: PackId(r.pack_id), idx: r.idx, offset: r.offset, len: r.len, size: r.size,
                    kind: match r.kind { 1 => Kind::Commit, 2 => Kind::Tree, 3 => Kind::Blob, 4 => Kind::Tag,
                                         k => return Err(Error::Internal(format!("bad kind {k} in objects"))) } }),
            });
        }
        Ok(out)
    }
}

impl Bucket {
    /// One subrequest. `Range::OffsetWithLength` is the memo's "length from an optional offset"; name unverified.
    pub async fn read_range(&self, key: &str, offset: u64, len: u64, budget: &mut ReqBudget) -> Result<Vec<u8>, Error> {
        budget.charge(1)?;                                                                         // 7.1
        let obj = self.inner.get(key).range(Range::OffsetWithLength { offset, length: len }).execute().await?
            .ok_or_else(|| Error::Storage(format!("missing {key}")))?;
        let bytes = obj.body().ok_or_else(|| Error::Storage(format!("no body for {key}")))?.bytes().await?;
        if u64::try_from(bytes.len()).ok() != Some(len) { return Err(Error::Storage(format!("short read on {key}"))); }
        Ok(bytes)
    }
    /// Coalesced reads (7.2): sort by (pack, offset), merge gaps < 256 KiB, split at 8 MiB, one range read per span.
    pub async fn read_entries(&self, locs: &[(ObjectId, ObjLoc)], budget: &mut ReqBudget) -> Result<Vec<(ObjectId, Vec<u8>)>, Error> {
        let mut sorted: Vec<&(ObjectId, ObjLoc)> = locs.iter().collect();
        sorted.sort_by(|a, b| (&a.1.pack.0, a.1.offset).cmp(&(&b.1.pack.0, b.1.offset)));
        let (mut out, mut i) = (Vec::with_capacity(locs.len()), 0usize);
        while let Some(first) = sorted.get(i) {
            let (start, mut end, mut j) = (first.1.offset, first.1.offset.saturating_add(u64::from(first.1.len)), i + 1);
            while let Some(n) = sorted.get(j) {
                let n_end = n.1.offset.saturating_add(u64::from(n.1.len));
                if n.1.pack.0 != first.1.pack.0 || n.1.offset.saturating_sub(end) >= GAP || n_end.saturating_sub(start) > SPAN { break; }
                end = end.max(n_end); j += 1;
            }
            let key = keys::pack(&self.repo, &first.1.pack);                 // r/<repo_id>/packs/<pack_id>.pack (2.2)
            let bytes = self.read_range(&key, start, end.saturating_sub(start), budget).await?;
            for e in sorted.iter().take(j).skip(i) {
                let lo = usize::try_from(e.1.offset.saturating_sub(start)).map_err(|_| Error::Internal("offset".into()))?;
                let hi = lo.checked_add(usize::try_from(e.1.len).map_err(|_| Error::Internal("len".into()))?)
                           .ok_or_else(|| Error::Internal("len overflow".into()))?;
                let entry = bytes.get(lo..hi).ok_or_else(|| Error::Storage(format!("entry outside span in {key}")))?;
                out.push((e.0, entry.to_vec()));                            // a full pack entry: varint header + zlib
            }
            i = j;
        }
        Ok(out)
    }
}
```

## Why it works
- **Refs are a SQLite-only path.** `GET /_do/refs` and `/_do/ls-refs` are "none" routes in the 1.3 table; `list_refs` above issues one `SELECT` and never names a bucket. `info/refs` is the request every `git fetch` and `ls-remote` makes, so polling cost never reaches R2. `HEAD` comes from `meta.head` (section 3) as a symbolic name, which is what `symref=HEAD:` (v0) and `symref-target:` (v2) advertise.
- **The CAS is git's own rule, decided by `changes()`.** git receive-pack updates each ref only if it still holds the advertised old oid; `apply_ref` encodes that in the `WHERE name=? AND target=?` clause and reads the outcome with `changes()` immediately after, inside the same span (section 3 step 4, platform-facts #1). `ng ... failed to update ref` is the string git prints on that failure; concurrent pushes to one branch (scenario 6) yield exactly one `ok`.
- **The span is atomic because it never awaits.** `commit_push` parses nothing, reads no body, and calls no host API that returns a promise between step 1 and the return; platform-facts #4 measured that a sync CAS after all network awaits has one winner out of eight. `transactionSync` would be belt and braces, and is unverified in `worker` 0.8.5 (section 3).
- **Objects are visible only when durable and committed.** Ingest writes `objects` rows with the pack in `ingesting` state after `PackWriter::finish` returned (2.4); `lookup` joins on `p.state = 'live'` (2.3), and `commit_push` step 3 is the only transition to `live`. A crash anywhere before step 3 leaves rows nobody can resolve and a pack the Janitor kills after `PUSH_TIMEOUT` (5.2). No reader ever sees a sha whose bytes are not in R2.
- **A live ref never points at a dead pack.** Step 2 compares `meta.gc_epoch` with the epoch captured at `/_do/push/begin`; `GcSweep` bumps it in its own sync span (5.3). The `apply_ref` re-guard makes the tip lookup part of the same span as the ref write, so even a sweep that ran between the edge's connectivity check and the commit is caught (section 5, "why the two review races are now impossible").
- **Object reads cost one lookup plus one range read and yield wire-ready bytes.** Every entry at rest is a full object (2.1), so the slice `read_entries` returns is a valid pack entry that `pack::generate` copies verbatim into an outgoing pack (section 9 step 6). Coalescing (7.2) keeps a fetch of N objects at roughly `pack_bytes / 8 MiB` subrequests, not N.
- **R2 keys carry `repo_id`, never a shared prefix.** `Bucket { inner, repo }` binds the repo id from `meta` at construction (section 8), and `keys::pack` is the only key builder the read path uses (2.2). Two repos cannot see or delete each other's packs.
- Conformance scenarios this proof must pass (section 11): 2, 4, 5, 6, 12, and 14 for the `gc_epoch` path. Scenario 15 (panic after step 4) is the day-1 check that the span rolls back.

## Changes from the first pass
| First-pass finding (quoted) | How addressed |
|---|---|
| Blocker: "Worker->DO routing builds the URL from pathname only, dropping ?service=" | The DO no longer sees client URLs. `edge` parses `service=` and `Git-Protocol`, calls `GET /_do/refs` (1.3 table), and formats with `wire::write_advertisement_v0` / `write_ls_refs`. `list_refs` returns rows, not bytes. |
| Blocker: "git push sends thin packs by default; ref-delta bases live only in R2 and parsePack has no way to resolve them" | Section 2.4 between-pass step: external bases are found with `/_do/push/lookup` (the `Index::lookup` above) and fetched with `Bucket::read_entries` (above) into a 32 MiB map; `pack::ingest::resolve_and_normalize` applies them. The delta code itself is the streaming-pack-parser slug. |
| Blocker: "Worker never sets x-repo-prefix, so all repos share objects/default/" | `keys::pack(&self.repo, ..)` in `read_entries`; `self.repo` is `meta.repo_id` (section 8, 2.2). There is no default prefix and no header-derived prefix. |
| Blocker: "Alarm sweeper interleaves with in-flight pushes ... can delete a sha a concurrent push is about to make reachable" | `commit_push` step 2 (`gc_epoch`) and the `apply_ref` tip re-guard, both in the same span as the ref write; `GcSweep` aborts on a `refs_version` change (5.3); R2 bytes are removed only after GRACE (5, Janitor step 3). No `blockConcurrencyWhile` is needed because no await exists to block. |
| Blocker: "objects index rows are inserted before the R2 put resolves" | Rows are inserted only after `PackWriter::finish` returns (2.4), under `packs.state = 'ingesting'`; `lookup` filters `p.state = 'live'`; step 3 flips the state. Rows before durability are impossible, rows before commit are invisible. |
| Caveat: "puts[] holds every inflated body until Promise.all; needs bounded concurrency" | No per-object puts exist. One multipart upload with 8 MiB parts (`PackWriter`, 6.4) and the pass-B memory budget of 2.4. Not in this code block; owned by streaming-pack-parser. |
| Caveat: "Pack trailer SHA-1 over a streamed response needs an incremental hasher" | `gix_hash::Hasher` in `PackWriter` and in `write_pack` (1.2, section 9 step 6). Not in this block. |
| Caveat: "No HEAD or symref=HEAD capability in the advertisement" | `list_refs` returns `meta.head` as the first tuple element; `wire` emits `symref=HEAD:` (v0) or `HEAD ... symref-target:` / `unborn` (v2). |
| Caveat: "pkt() uses UTF-16 length not byte length" | All framing is bytes in `wire` over `gix-packetline` (1.1 rule 1). Ref names are `BString`; `apply_ref` rejects non-UTF-8 names with git's `funny refname` before they reach a TEXT column. |
| Caveat: "Delete-only pushes carry no PACK; parser must accept an empty body" | `CommitRequest.pack_id: Option<PackId>`; step 3 is skipped on `None` (2.4 last paragraph). |
| Caveat: "uploadPack ignores have/done so every fetch is clone-sized" | Section 9 `fetch_v2` negotiation with ACKs and the round loop; owned by protocol-v2-only. This block only provides `lookup` and `read_entries` that the loop calls. |
| Caveat: "setAlarm only after a successful push, so orphans from a crash on a quiet repo are never swept" | Janitor is self-enqueued in `boot` (4.5, 8.2) and re-enqueues itself every 15 min; `commit_push` calls `jobs::enqueue` only, never `set_alarm` (4.1). |
| Caveat: "Per-object R2 GET per fetched object makes clone performance depend on precomputed-clone-pack" | Full-object normalized packs plus `read_entries` coalescing (2.1, 7.2); `write_pack` streams marked entries in 8 MiB windows. precomputed-clone-pack is out of scope (section 12). |
| Caveat (review body): "Reconciliation list in the alarm to catch lost R2 writes" | Not addressed because the ordering rule trusts `MultipartUpload::complete` returning `Object` as durability; a lost object after that is an R2 durability failure the foundation does not model. |

## Known limits
- **Peeled tags cannot be produced by a sync route as the contract stands.** `RefRow.peeled` exists (1.1), `GET /_do/refs` and `/_do/ls-refs` allow no awaits (1.3), scenario 3 asserts a peeled line, but `refs` has no `peeled` column (section 3). `list_refs` therefore returns `peeled: None`. Proposed write-back: add `peeled TEXT` to `refs`, filled from `CommitRequest` by the edge, which already sees every tag object in the pack (or reads a pre-existing tag target with one range read). Not done here because it would be a schema deviation.
- Three signature mismatches between 1.2 and section 9 that this code resolves in favour of section 9 and rule 7.1, to be written back: `Bucket::read_range`/`read_entries` take `&mut ReqBudget` (1.2 has none); `read_entries` takes `&[(ObjectId, ObjLoc)]` because `ObjLoc` carries no id yet the return type does; `jobs::enqueue` is `fn` while `Storage::set_alarm` is async, so `rearm` must defer the alarm call past the span.
- 128 MB: `read_entries` holds one span (<= 8 MiB, or one entry up to 32 MiB) plus the copied entries; callers bound the total through `MemFind.bytes` (64 MiB, section 9 step 3). A `lookup` of 1,000 ids is 1,000 `SELECT`s in one span; fine for SQLite, but `objects` rows are about 120 bytes each so a 1,000,000-object repo is roughly 120 MB of DO SQLite (10 GB limit, not a concern; boot time unmeasured).
- CPU: paid plan, `limits.cpu_ms = 300000`; the sync span here is microseconds. Subrequests: `read_entries` charges one per span against 9,000; the limit is not enforced by local workerd (#7), so only `ReqBudget` and the `x-ge-subrequests` header assertion catch a regression before deploy.
- Unverified, day-1 list: `SqlStorage::exec` bindings type and `SqlStorageValue: From<String/i64>`; `Range::OffsetWithLength` variant name; `transactionSync` binding; panic rollback of a sync span under `--panic-unwind` (scenario 15); real R2 range reads and multipart (#6); DO subrequest limit on a deployed Worker.
- A sha in two live packs is legal and `lookup` returns either row (2.3); `gc_consolidate` removes the duplicate later. The `UPDATE meta SET value=value+1` relies on SQLite numeric coercion of a TEXT column, exactly as section 3 writes it.

## Depends on
- repo-do-ref-authority
- two-phase-push
- streaming-pack-parser
- protocol-v2-only
- info-refs-endpoint
- gc-and-repack-alarm
- auth-and-multitenancy
