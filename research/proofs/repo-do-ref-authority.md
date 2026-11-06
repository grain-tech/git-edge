# One Durable Object per repo as the ref authority

> Second pass · Idea #1 · verdict: **lands with caveats** · feasibility 4/5 · reliability 4/5 · correctness 4/5 (first pass 4/3/3)
> First pass: [proof](./archived/repo-do-ref-authority.md) · [review](../reviews/archived/repo-do-ref-authority.md) · Second pass: [review](../reviews/repo-do-ref-authority.md) · Contract: [CONTRACTS.md](../../CONTRACTS.md)


## Mechanism
This idea is subsumed by CONTRACTS.md: it is the `repo_do` module (section 1.3) and the ref transaction contract (section 3). The edge Worker never touches refs; it derives one `RepoDo` stub with `id_from_name("owner/repo")` (section 8), runs ingest to completion (pack durable in R2, `packs` row `ingesting`, `objects` rows inserted, section 2.4), and only then calls `POST /_do/push/commit`. `RepoDo::commit_push` is one sync span over the `pushes`, `meta`, `packs`, `refs`, `reflog` and `jobs` tables: it checks `gc_epoch`, flips the pack to `live`, applies each `RefCommand` as an SQLite compare-and-swap whose outcome is `SELECT changes()`, writes the reflog, bumps `refs_version`, records the result on the `pushes` row and enqueues `GcMark`. Because the platform delivers no other event to the DO inside a sync span, one DO per repo replaces every distributed lock, and the CAS on `refs.target` is exactly git's `ref_transaction_update` with the advertised old oid.

## Primitives
- `#[durable_object]` / `DurableObject { new, fetch, alarm }` in `worker` 0.8.5: verified (memo section 1, spike `spikes/rust-ls-refs`).
- `State::storage().sql()` -> `SqlStorage::exec(query, bindings) -> SqlCursor`, synchronous; `SqlCursor::{one, to_array}`: verified (memo, spike). `SqlCursor::rows_written` exists and is never read (section 3).
- `SELECT changes()` reports 1 / 0 / 1 for insert / no-match / delete on a `WITHOUT ROWID` table: measured (platform-facts #1).
- Sync span atomicity: eight concurrent DO calls with a synchronous SQL CAS after an R2 await had exactly one winner: measured (platform-facts #4). Rollback of a span's writes on throw: memo section 1; rollback on `PanicError` inside `fetch` is **unverified** (section 10, scenario 15).
- `transactionSync` binding in `worker` 0.8.5: **unverified**. The span holds without it; if it binds, the span is wrapped in it in addition.
- `DurableObjectNamespace::id_from_name(..).get_stub()` and `Stub::fetch_with_request`: verified (memo, spike). Typed DO RPC: experimental, not used.
- `js_sys::Date::now()` for `updated_at`, `began_at`, `ended_at`: standard `js-sys`, verified.
- `web_sys::Crypto::get_random_values_with_u8_array` for `repo_id` at boot: exact binding path **unverified** (section 8.2). `ctx.id.name` through `js_sys::Reflect::get`: measured populated locally (#2), production unverified, read only in `boot` as a cross-check.
- `gix_validate::reference::name_partial` (0.11.4, wasm CI-built) for ref names; `gix_hash::ObjectId::from_hex` (0.26.2) for ids: verified.
- Alarms: only `jobs::rearm` calls `set_alarm` (section 4); this module only calls `jobs::enqueue`. Second `setAlarm` cancels the first: measured (#5).

## Proof code
```rust
// src/repo_do/mod.rs  -- worker 0.8.5, gix-hash 0.26.2, gix-validate 0.11.4. Tables: sections 2.3, 3, 4, 8.
use std::cell::RefCell;
use bstr::ByteSlice;
use gix_hash::ObjectId;
use worker::{durable_object, DurableObject, Env, Method, Request, Response, SqlCursor, SqlStorage, SqlStorageValue as V, State};
use crate::{error::Error, jobs::{self, JobKind}, store::{Index, ObjLoc, PackId}};

#[derive(serde::Deserialize)] struct CmdDto { old: String, new: String, name: String }
#[derive(serde::Deserialize)] pub struct CommitRequest { pub push_id: String, pub pack_id: Option<String>,
                                                          pub principal: String, pub commands: Vec<CmdDto> }
#[derive(serde::Serialize)]   pub struct CommitResponse { pub results: Vec<(String, Option<&'static str>)> } // None = ok
#[derive(serde::Deserialize)] struct BeginDto { push_id: String, principal: String }
#[derive(serde::Deserialize)] struct N { n: i64 }
#[derive(serde::Deserialize)] struct PushRow { state: String, gc_epoch: i64 }
struct Cmd { old: ObjectId, new: ObjectId, name: String }

#[durable_object]
pub struct RepoDo { state: State, env: Env, booted: RefCell<bool> }

impl DurableObject for RepoDo {
    fn new(state: State, env: Env) -> Self { Self { state, env, booted: RefCell::new(false) } }
    async fn fetch(&self, mut req: Request) -> worker::Result<Response> {
        let hdr = crate::edge::RepoHeaders::from_request(&req);          // x-ge-owner / x-ge-repo, section 8.1
        let body = req.bytes().await?;                                    // the only await on a "none" route
        // ---- sync span from here to the response for every route below except /_do/fetch ----
        let out: Result<Response, Error> = self.boot(&hdr).and_then(|meta| match (req.method(), req.path().as_str()) {
            (Method::Get,  "/_do/refs")        => self.list_refs().and_then(crate::edge::refs_json),
            (Method::Post, "/_do/push/begin")  => self.push_begin(&meta, &parse::<BeginDto>(&body)?),
            (Method::Post, "/_do/push/lookup") => self.push_lookup(&parse(&body)?),
            (Method::Post, "/_do/push/index")  => self.push_index(&parse(&body)?),
            (Method::Post, "/_do/push/commit") => self.commit_push(&parse::<CommitRequest>(&body)?).and_then(json),
            (Method::Post, "/_do/ls-refs")     => self.ls_refs(&meta, &body),
            (Method::Post, "/_do/fetch")       => return self.fetch_v2_entry(&meta, &body).await, // R2 reads, section 9
            _ => Err(Error::NotFound),
        });
        crate::edge::respond(out)                                         // section 10 status mapping
    }
    async fn alarm(&self) -> worker::Result<Response> {
        self.boot(&crate::edge::RepoHeaders::NONE)?;                      // alarm has no headers, section 8.2
        jobs::dispatch(self).await.map_err(worker::Error::from)?;         // never lets an Err escape, section 4.4
        Response::empty()
    }
}

fn parse<T: serde::de::DeserializeOwned>(b: &[u8]) -> Result<T, Error> {
    serde_json::from_slice(b).map_err(|e| Error::Internal(format!("do body: {e}")))
}
fn oid(hex: &str) -> Result<ObjectId, Error> { ObjectId::from_hex(hex.as_bytes()).map_err(|e| Error::Protocol(e.to_string())) }
fn now_ms() -> i64 { js_sys::Date::now() as i64 }

impl RepoDo {
    fn sql(&self) -> SqlStorage { self.state.storage().sql() }
    fn q(&self, s: &str, args: Vec<V>) -> Result<SqlCursor, Error> {
        self.sql().exec(s, Some(args)).map_err(|e| Error::Storage(e.to_string()))
    }
    /// The one CAS oracle. Issued immediately after the write, in the same sync span. Never `rows_written`.
    fn changes(&self) -> Result<i64, Error> { Ok(self.q("SELECT changes() AS n", vec![])?.one::<N>()?.n) }
    fn meta(&self, key: &str) -> Result<String, Error> {
        #[derive(serde::Deserialize)] struct S { value: String }
        Ok(self.q("SELECT value FROM meta WHERE key=?", vec![V::from(key)])?.one::<S>()?.value)
    }

    /// Section 3 step 0: the push row that carries the gc_epoch the whole push is validated against.
    fn push_begin(&self, meta: &Meta, b: &BeginDto) -> Result<Response, Error> {
        self.q("INSERT INTO pushes(id,state,principal,began_at,gc_epoch) VALUES(?,'open',?,?,?)",
               vec![V::from(b.push_id.as_str()), V::from(b.principal.as_str()), V::from(now_ms()), V::from(meta.gc_epoch)])?;
        json(serde_json::json!({ "repo_id": meta.repo_id, "refs_version": meta.refs_version, "gc_epoch": meta.gc_epoch }))
    }

    /// Section 3, steps 1-7. One sync span: no await, no R2, no stub call between the first SELECT and the return.
    pub fn commit_push(&self, req: &CommitRequest) -> Result<CommitResponse, Error> {
        let cmds = req.commands.iter()                                    // parse fully before touching storage
            .map(|c| Ok(Cmd { old: oid(&c.old)?, new: oid(&c.new)?, name: c.name.clone() }))
            .collect::<Result<Vec<_>, Error>>()?;
        let now = now_ms();
        let push = self.q("SELECT state, gc_epoch FROM pushes WHERE id=?", vec![V::from(req.push_id.as_str())])?
            .to_array::<PushRow>()?.into_iter().next().ok_or_else(|| Error::Conflict("unknown push".into()))?;
        if push.state != "open" { return Err(Error::Conflict(format!("push is {}", push.state))); }   // step 1
        let epoch: i64 = self.meta("gc_epoch")?.parse().map_err(|_| Error::Internal("meta.gc_epoch".into()))?;
        if epoch != push.gc_epoch {                                                                    // step 2
            let results = cmds.iter().map(|c| (c.name.clone(), Some("gc ran during push, retry"))).collect::<Vec<_>>();
            return self.finish_push(req, "rejected", now, results);
        }
        if let Some(pack) = &req.pack_id {                                                            // step 3
            self.q("UPDATE packs SET state='live' WHERE id=? AND state='ingesting'", vec![V::from(pack.as_str())])?;
            if self.changes()? != 1 { return Err(Error::Conflict("pack not in state ingesting".into())); }
        }
        let head = self.meta("head")?;
        let sql = self.sql();
        let idx = Index(&sql);
        let mut results = Vec::with_capacity(cmds.len());
        let mut any_ok = false;
        for c in &cmds {                                                                              // step 4
            let r = self.apply_one(&idx, &head, c, &req.push_id, &req.principal, now)?;
            any_ok |= r.is_none();
            results.push((c.name.clone(), r));
        }
        if any_ok {
            self.q("UPDATE meta SET value=value+1 WHERE key='refs_version'", vec![])?;                 // step 5
            jobs::enqueue(&sql, JobKind::GcMark, now + 10 * 60 * 1000, "{}")?;                        // step 7 (dedups)
        }
        self.finish_push(req, "committed", now, results)                                              // step 6
    }

    /// One RefCommand, independent of its siblings (git default, `atomic` not advertised: section 1.1 rule 5).
    fn apply_one(&self, idx: &Index<'_>, head: &str, c: &Cmd, push: &str, who: &str, now: i64)
        -> Result<Option<&'static str>, Error> {
        if gix_validate::reference::name_partial(c.name.as_bytes().as_bstr()).is_err() { return Ok(Some("funny refname")); }
        if c.old.is_null() && c.new.is_null() { return Ok(Some("funny refname")); }
        if c.new.is_null() && c.name == head { return Ok(Some("deletion of the current branch prohibited")); }
        if !c.new.is_null() {
            // Section 2.5: the tip must be live. The pack of this push became live in step 3, same span, so it counts.
            let loc: Option<ObjLoc> = idx.lookup(&[c.new])?.into_iter().next().flatten();
            if loc.is_none() { return Ok(Some("missing necessary objects")); }
        }
        let (o, n, name) = (c.old.to_string(), c.new.to_string(), c.name.as_str());
        if c.old.is_null() {
            self.q("INSERT INTO refs(name,target,updated_at) VALUES(?,?,?) ON CONFLICT DO NOTHING",
                   vec![V::from(name), V::from(n.as_str()), V::from(now)])?;
        } else if c.new.is_null() {
            self.q("DELETE FROM refs WHERE name=? AND target=?", vec![V::from(name), V::from(o.as_str())])?;
        } else {
            self.q("UPDATE refs SET target=?, updated_at=? WHERE name=? AND target=?",
                   vec![V::from(n.as_str()), V::from(now), V::from(name), V::from(o.as_str())])?;
        }
        if self.changes()? != 1 { return Ok(Some("failed to update ref")); }       // git's own ng string
        self.q("INSERT INTO reflog(name,old,new,push_id,principal,at) VALUES(?,?,?,?,?,?)",
               vec![V::from(name), V::from(o.as_str()), V::from(n.as_str()), V::from(push), V::from(who), V::from(now)])?;
        Ok(None)
    }

    fn finish_push(&self, req: &CommitRequest, state: &str, now: i64, results: Vec<(String, Option<&'static str>)>)
        -> Result<CommitResponse, Error> {
        let result = serde_json::to_string(&results).map_err(|e| Error::Internal(e.to_string()))?;
        self.q("UPDATE pushes SET state=?, ended_at=?, pack_id=?, result=? WHERE id=?",
               vec![V::from(state), V::from(now), V::from(req.pack_id.as_deref()), V::from(result.as_str()), V::from(req.push_id.as_str())])?;
        Ok(CommitResponse { results })
    }
}

// src/edge/receive.rs -- the caller enforces the ordering of section 3: (1) finish, (2) index, (3) commit.
pub async fn receive_pack(req: Request, env: &Env, who: &Principal, repo: &RepoRoute) -> Result<Response, Error> {
    let mut body = BodyReader::new(&req)?;                                 // gzip + chunked, section 6
    let hdr = read_receive_header(&mut body).await?;                       // wire::parse_receive_header, <= 1 MiB
    let stub = env.durable_object("REPO")?.id_from_name(&repo.name())?.get_stub()?;
    let push_id = PushId::random();
    let begin: Begin = stub_json(&stub, repo, "/_do/push/begin", &json!({"push_id": push_id, "principal": who.name})).await?;
    let mut budget = ReqBudget::paid();
    let pack_id = match pack::ingest::run(&mut body, env, &stub, repo, &push_id, &begin, &mut budget).await {
        Ok(p) => p,                                                        // None for delete-only and 0-object packs
        Err(Error::Unpack(msg)) => return report(&hdr, Err(&msg), &[]),      // unpack <msg> + ng for every ref, section 10
        Err(e) => return Err(e),
    };
    let commit = CommitRequest { push_id: push_id.0, pack_id: pack_id.map(|p| p.0), principal: who.name.clone(),
        commands: hdr.commands.iter().map(|c| CmdDto { old: c.old.to_string(), new: c.new.to_string(), name: c.name.to_string() }).collect() };
    let res: CommitResponse = stub_json(&stub, repo, "/_do/push/commit", &commit).await?;
    let results: Vec<RefResult> = res.results.into_iter()
        .map(|(n, ng)| match ng { None => RefResult::Ok(n.into()), Some(r) => RefResult::Ng(n.into(), r) }).collect();
    report(&hdr, Ok(()), &results)   // wire::write_report_status: band 1 under side-band-64k, else raw pkt-lines (rule 4)
}
```

## Why it works
- The CAS is git's own check. `receive-pack` passes the advertised old oid to `ref_transaction_update`; `UPDATE refs ... WHERE name=? AND target=?` (create: `INSERT ... ON CONFLICT DO NOTHING`; delete: `DELETE ... WHERE target=?`) is that check in SQL, and `changes()` is its outcome (section 3 step 4). A stale `--force` fails server-side here exactly as in real git (scenario 5). The zero-oid convention from the wire (`old` zero = create, `new` zero = delete) selects the statement.
- No lost update. Both racing pushes reach `commit_push` after their own awaits are over; the span from the first `SELECT` to the response contains no await, and the platform delivers no other DO event inside a sync span (section 3, measured platform-facts #4). The second push's `UPDATE ... WHERE target=X` matches 0 rows and gets `ng refs/heads/main failed to update ref` (scenario 6).
- No dangling ref after a crash. The ordering rule of section 3 is enforced by the caller: `PackWriter::finish` (pack durable) and `/_do/push/index` (rows present, pack `ingesting`) complete before `/_do/push/commit` is sent. Step 3 flips the pack to `live` in the same span as the ref moves, so at no instant does a `refs` row point into a pack that is not `live`. A crash before commit leaves an `open` push that the Janitor expires and whose `ingesting` pack it kills (section 5); no ref referenced it. A crash after commit leaves the ref moved and the pack live; the client's retry gets `ng ... failed to update ref`, which is git-over-HTTP behaviour on any server.
- No lost objects under GC. `push_begin` stores `gc_epoch` on the `pushes` row; step 2 compares it against `meta.gc_epoch` inside the span. `GcSweep` bumps `gc_epoch` in its own sync span (section 5). If a sweep ran between the edge's lookups and the commit, the push is rejected with `ng <ref> gc ran during push, retry`; if it runs after, `refs_version` (step 5) makes the sweep abort. Both interleavings are analysed in section 5.
- Connectivity. Ingest checks `extract_links(all entries) - {entries}` against live packs before commit (section 2.5); the invariant that live objects are closed under references makes a deep walk unnecessary. `apply_one` re-checks that each new tip is live through `Index::lookup`, the only reader query (section 2.3), which sees the pack flipped in step 3 because it is the same connection and span.
- Wire compliance. The DO returns JSON, not pkt-lines; `wire::write_report_status` frames `unpack ok`, `ok`/`ng` and the flush in band 1 when the client negotiated `side-band-64k` (section 1.1 rule 4). `delete-refs` is advertised (rule 5), so deletes arrive; a delete-only push has no PACK and a new-ref-at-existing-commit push has a 0-object pack, both yield `pack_id = None` (section 2.4) and skip step 3. `atomic` is not advertised, so per-ref independence is what the client expects.
- Repo identity. `repo_id`, `owner`, `repo`, `head` come from `meta` written at `boot` (section 8); the DO stub is `id_from_name("owner/repo")`; nothing in this module reads `ctx.id.name` except the cross-check in `boot`. HEAD is the `meta.head` symbolic row, so `apply_one` can refuse deleting the current branch without a `refs` row for HEAD.
- Background work goes through `jobs::enqueue` only (step 7); this module never calls `set_alarm` (section 4 rule 1).

## Changes from the first pass
| First-pass item | Kind | How addressed |
|---|---|---|
| "ref CAS commits before objects are durable under their final key and no committed-pushId record is written ... janitor can orphan a live ref" | blocker | Ordering moved to the caller: `receive_pack` runs `pack::ingest::run` (finish + index) before `/_do/push/commit`; `commit_push` step 3 flips `packs.state` to `live` and `finish_push` writes `pushes.state='committed'` and `pack_id` in the same span. The Janitor only kills packs of `expired`/`rejected` pushes and only deletes `dead` rows older than GRACE (section 5). |
| "`cursor.rowsWritten` ... counts index-row writes ... rejects every branch create/delete" | blocker | `RepoDo::changes()` issues `SELECT changes()` immediately after each write statement in the same span; `refs` is `WITHOUT ROWID`; `rows_written` is never read (CI grep, section 3). Measured 1 / 0 / 1 (platform-facts #1). |
| "chunked bodies vs R2 known-length put; must be multipart with >= 5 MiB parts" | blocker | Not in this module. `receive_pack` builds `BodyReader` (section 6: gzip via `DecompressionStream`, no `Content-Length` trusted) and `pack::ingest::stream_to_pending` writes 8 MiB multipart parts through `PackWriter` (section 2.4). Multipart is measured only on the local simulator (#6). Dependency: two-phase-push, streaming-pack-parser. |
| "No connectivity check ... requires #4 + #56" | caveat | Section 2.5 connectivity by induction runs in ingest before commit; `apply_one` re-checks each tip with `Index::lookup`; step 2 (`gc_epoch`) guards the gap between lookup and commit. No commit graph is needed. |
| "side-band-64k framing, delete-refs advertised, deletion-only pushes carry no PACK, 0-object packs" | caveat | Framing and advertisement are `wire` (section 1.1 rules 4 and 5, `report()` in the code). `pack_id: Option<PackId>` in `CommitRequest` covers both empty-pack cases; step 3 is skipped when `None`. |
| "Every push and ls-refs serialize through one DO in one colo; cross-region RPC ~100-300 ms; `idFromName` makes rename a migration" | caveat | Rename: R2 keys and `objects` rows use `meta.repo_id`, so a rename changes two `meta` rows and no R2 key (section 8.4); the stub name change still needs a row copy to a new DO, not addressed here. Latency and single-writer throughput: not addressed because the contract fixes one DO per repo; a push now costs 3 + N/1000 + N/10000 stub round-trips, see Known limits. |
| "`--atomic`, `report-status-v2`, push-options unimplemented, must not be advertised; 'fetch first' is client-side" | caveat | `atomic` and `push-options` are not advertised (rule 5). `report-status-v2` is advertised and written by `wire` without option lines, which section 12 allows. The ng reason is now `failed to update ref`, git's server string. |
| "R2 `head` before the transaction is a TOCTOU gap against the janitor" (first-pass limit) | limit | The DO makes no R2 call on the push path; existence is `Index::lookup` in the same span, and `gc_epoch` covers the lookup-to-commit gap. |
| "packfile must never enter the DO" (first-pass limit) | limit | Kept: only JSON rows (<= 10,000 `ObjRow` per `/_do/push/index` call) and `CommitRequest` cross the stub. |

## Known limits
- Stub round-trips per push: `begin`, `lookup` x ceil(bases/1000), `index` x ceil(N/10,000), `commit`. Each is a cross-colo hop when the edge is far from the DO; the contract accepts this (section 7.3). Not measured.
- Throughput of one repo is one DO: every push, `ls-refs` and `fetch` is serialised on one isolate. Fetches hold the DO for the duration of their R2 reads (they await, so pushes interleave, but CPU is shared). Sharding is out of scope (section 12: per-branch DOs).
- Ref names cross the stub as JSON strings; a non-UTF-8 ref name is rejected at the edge with `ng <ref> funny refname` before reaching the DO. git allows high bytes in ref names; this is a deviation from git accepted for the foundation.
- `transactionSync` in `worker` 0.8.5 and rollback of a sync span on `PanicError` are unverified; scenario 15 is the day-1 test. Without rollback, a panic between step 3 and step 6 could leave a pack `live` with the push row still `open`; the Janitor then expires the push but does not kill a `live` pack, leaving unreferenced but harmless objects that `GcMark` ignores and `GcConsolidate` drops.
- `web_sys::Crypto` binding path for `repo_id` generation and the `Reflect::get` read of `ctx.id.name` in production are unverified (section 8).
- The DO handler's 128 MB and CPU limits are not exercised by this module: the largest body it parses is a 10,000-row `/_do/push/index` JSON (about 1.5 MB). The 300 s CPU limit applies to ingest in the edge, not here.
- Subrequest budget is not enforced by local workerd (#7); `ReqBudget` in the edge is the only guard.
- Scenarios this proof must pass: 2, 4, 5, 6, 14, 15. Added scenario: "crash between index and commit": kill the edge after `/_do/push/index` returns, advance the fake clock past `PUSH_TIMEOUT`, fire the alarm, assert the push is `expired`, its pack `dead`, and `ls-remote` unchanged.

## Depends on
- refs-sqlite-objects-r2
- two-phase-push
- streaming-pack-parser
- info-refs-endpoint
- auth-and-multitenancy
- gc-and-repack-alarm
