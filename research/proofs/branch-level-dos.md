# Branch-level Durable Objects for monorepos

> Second pass · Idea #16 · verdict: **lands with caveats** · feasibility 4/5 · reliability 3/5 · correctness 3/5 (first pass 4/3/2)
> First pass: [proof](./archived/branch-level-dos.md) · [review](../reviews/archived/branch-level-dos.md) · Second pass: [review](../reviews/branch-level-dos.md) · Contract: [CONTRACTS.md](../../CONTRACTS.md)

## Mechanism
The mechanism as designed — a `RepoRoot` DO plus N `RefShard` DOs at `id_from_name("owner/repo#<ns>")`, each owning a partitioned `refs` table — cannot exist under CONTRACTS.md, and the reason is structural, not only section 12's "per-branch DOs" ban. Section 8.1 derives exactly one stub per repo (`id_from_name("{owner}/{repo}")`) and A9's registration list (routes, `JobKind` variants, tables, R2 key prefixes) contains no DO namespace to register. The deeper conflict: `commit_push` works because step 4's per-ref CAS sees step 3's `packs.state='live'` flip and the `objects` rows inside one sync span (section 3); in a shard, `Index::lookup` is an async stub call away, and platform-facts #4 measured that an await between check and write loses seven of eight updates — sharding reintroduces defect 5 at the exact point the contract removed it. `GcSweep`'s `refs_version` guard (section 5) is a per-DO atomicity token that cannot span shards, connectivity (2.5) is a repo-wide invariant, and `fetch` (section 9) resolves wants against `Index`, which a ref shard could never serve anyway. What survives is the read half, reshaped. The monorepo pain under one DO is not the CAS — it is serialization: `/_do/refs`, `/_do/ls-refs` and the v0 receive-pack advertisement each turn every ref into bytes inside one sync span, so a 500k-ref repo pays O(#refs) DO CPU on every push's advertisement and every `ls-remote`, and every in-flight commit waits behind that span. This module moves the O(#refs) work off the DO: `JobKind::AdvBuild` (section 4, dedup'd by kind) serializes `{head, refs}` to `r/<repo_id>/adv/<refs_version>.json` (new prefix, A9) in chunked sync spans inside a slice and publishes only if `refs_version` is unchanged; `POST /_do/adv` answers `{v, ready}` in O(1); the edge does one R2 `get` and formats the v0 advertisement and ls-refs itself through `wire` (1.1: sync, no `worker` imports), applying `ref-prefix` filters edge-side. Refs, CAS, `gc_epoch` and connectivity never leave the one DO — only the bytes did. While no snapshot is published, the edge falls back to `GET /_do/refs`, the same span the foundation already pays.

## Primitives
- `Stub::fetch_with_request` + `Request::new_with_init` JSON round-trips (`/_do/adv`, `/_do/refs`): API verified (memo section 1, spike); the exact constructor path is **unverified at runtime** (same caveat as two-phase-push).
- `SqlStorage::exec` synchronous, `to_array`: verified (memo, spike). Sync-span atomicity for `adv_pointer`, the drift re-checks and the publish span: measured (platform-facts #4).
- `worker::Bucket::put(key, impl Into<Data>)`, `Bucket::get(key).execute()`, `ObjectBody::bytes()`: verified in `worker` 0.8.5 source (memo section 1); exposed on `store::Bucket` as `put_bytes`/`get_bytes` write-backs (Known limits).
- `store::Bucket::delete(&[String])` (1.2): multi-key binding **unverified** (two-phase-push); stale-version batches are a handful of keys, and the single-key fallback costs one subrequest each.
- `jobs::enqueue` dedup by kind, `jobs::rearm` the sole `set_alarm` caller (4.1, A3): contract; a second `setAlarm` cancels the first, measured (#5).
- `SliceBudget::spent_80pct`, `SliceOutcome::{Done, Reschedule}` (A4); one slice per alarm firing (4.2); `dead` maintenance jobs re-enqueued at `boot` (A4).
- `wire::{write_advertisement_v0, write_ls_refs, write_capability_advertisement_v2, PktWriter, LsRefsArgs, RefRow, Service}`: contract 1.1; the v0 advertisement and ls-refs ran against real git 2.43 in the spike (rust-server corrections 5).
- `{"head","refs":[{name,target,peeled}]}` JSON over the stub and in R2: "rows are JSON with hex ids" (1.3); the shared DTO lives in `wire::http` (A8, write-back).
- `js_sys::Date::now()`: standard `js-sys`. No gitoxide API is used; the DO reads its own `refs` table.
- R2 `put` then `get` consistency for the snapshot key, and `get` on a deleted key returning `None`: R2 is strongly consistent per its docs, **not measured** (platform-facts #6 covers multipart only, on the local simulator).

## Proof code
```rust
// src/repo_do/adv.rs + src/jobs/adv_build.rs + src/edge/adv.rs -- CONTRACTS.md 1.1, 1.3, 3, 4, 5.2, 7.1, 8, 9; A3, A4, A8, A9.
// REGISTRY (amendment A9):
//   route:    POST /_do/adv                         (awaits: none)
//   job kind: JobKind::AdvBuild -> adv_build::run_slice; also enqueued inside commit_push's any_ok arm (write-back)
//   table:    CREATE TABLE adv(version INTEGER PRIMARY KEY, built_at INTEGER NOT NULL) WITHOUT ROWID  -- schema_version 4
//             built_at: 0 = build in flight or dead, -1 = over SNAP_MAX (fallback forever), >0 = published
//   R2:       r/<repo_id>/adv/<refs_version>.json = {"head":"<symref>","refs":[{"name","target","peeled"}]}
// No second DO namespace is registered: A9 has none, and 8.1 + section 12 keep one RepoDo per repo (see Mechanism).
// `q`, `sql`, `meta`, `meta_i64`, `now_ms`, `json`, `bucket` are the RepoDo helpers of repo-do-ref-authority /
// bundle-uri; `stub_json`, `RepoRoute::apply_headers`, `Error::from_do_response` come from two-phase-push.
use bstr::BString;
use gix_hash::ObjectId;
use serde::Deserialize;
use worker::{Method, Request, RequestInit, SqlStorageValue as V, Stub};
use crate::{edge::{stub_json, RepoRoute}, error::Error, jobs::{self, Job, JobKind, SliceBudget, SliceOutcome},
            repo_do::RepoDo, store::Bucket, wire, ReqBudget};
const CHUNK: i64 = 5_000;                  // one SELECT per sync span inside the slice (A4)
const SNAP_MAX: u64 = 64 << 20;            // the edge holds the snapshot whole; bigger listings fall back permanently
const KEEP_MS: i64 = 600_000;              // > 2 * ReqBudget.max_ms (240 s, 7.1): no live request still names a deleted key
const REBUILD_MS: i64 = 5_000;             // drift backoff: a Reschedule is not an attempt (4.4), so pace restarts
fn adv_key(repo: &str, v: i64) -> String { format!("r/{repo}/adv/{v}.json") }

// ---- src/repo_do/adv.rs ----
#[derive(Deserialize)] struct ARow { built_at: i64 }
impl RepoDo {
    /// POST /_do/adv, one sync span (1.3): the O(1) pointer every listing path buys first. `ready` means an object
    /// named by THIS refs_version is published, so the edge can never be served a mixed or stale snapshot.
    pub fn adv_pointer(&self) -> Result<worker::Response, Error> {
        let v = self.meta_i64("refs_version")?;
        let b = self.q("SELECT built_at FROM adv WHERE version=?", vec![V::from(v)])?.to_array::<ARow>()?
                   .into_iter().next().map_or(0, |r| r.built_at);
        if b == 0 { jobs::enqueue(&self.sql(), JobKind::AdvBuild, now_ms(), "{}")?; }   // absent or died; dedups (4.5)
        json(serde_json::json!({ "v": v, "ready": b > 0 }))                             // -1: capped, don't re-enqueue
    }
}

// ---- src/jobs/adv_build.rs ----
#[derive(Deserialize, serde::Serialize)] struct Row { name: String, target: String, peeled: Option<String> }
#[derive(Deserialize)] struct VRow { version: i64 }
/// One slice: serialize every ref, each SELECT its own sync span (A4). Published only when refs_version is still `v`
/// in the publish span, so rows torn by a mid-build commit are never served (the first pass's consistency caveat).
pub async fn run_slice(d: &RepoDo, _job: &Job, budget: &mut SliceBudget) -> Result<SliceOutcome, Error> {
    let (v, head) = (d.meta_i64("refs_version")?, d.meta("head")?);                 // the version this build claims
    d.q("INSERT INTO adv(version,built_at) VALUES(?,0) ON CONFLICT(version) DO NOTHING", vec![V::from(v)])?;
    let mut buf = format!("{{\"head\":{},\"refs\":[",
        serde_json::to_string(&head).map_err(|e| Error::Internal(e.to_string()))?).into_bytes();
    let (mut last, mut first) = (String::new(), true);
    loop {
        if budget.spent_80pct() { return Err(Error::Budget) }                     // 4.4 retry restarts; Known limits
        if d.meta_i64("refs_version")? != v {                                     // a commit landed mid-build: these
            return Ok(SliceOutcome::Reschedule { run_at: now_ms() + REBUILD_MS }); // rows are torn; restart, don't publish
        }
        let rows: Vec<Row> = d.q("SELECT name,target,peeled FROM refs WHERE name>? ORDER BY name LIMIT ?",
                                 vec![V::from(last.as_str()), V::from(CHUNK)])?.to_array()?;
        let Some(tail) = rows.last() else { break };
        last = tail.name.clone();
        for r in &rows {
            if !first { buf.push(b','); } first = false;
            serde_json::to_writer(&mut buf, r).map_err(|e| Error::Internal(e.to_string()))?;
        }
        if u64::try_from(buf.len()).unwrap_or(u64::MAX) > SNAP_MAX {
            d.q("UPDATE adv SET built_at=-1 WHERE version=?", vec![V::from(v)])?; // pointer serves ready=false forever
            return Ok(SliceOutcome::Done);
        }
        if rows.len() < CHUNK as usize { break }
    }
    buf.extend_from_slice(b"]}");
    let bucket = d.bucket()?;
    let mut rb = ReqBudget { max_subrequests: 400, used: budget.subrequests_used, started_ms: budget.started_ms, max_ms: 20_000.0 };
    bucket.put_bytes(&adv_key(&bucket.repo.0, v), buf, &mut rb).await?;           // write-back: inner.put + charge (7.1)
    let now = now_ms();                                                          // publish span: the second version check
    if d.meta_i64("refs_version")? != v {                                        //   is what makes the bytes a snapshot of
        return Ok(SliceOutcome::Reschedule { run_at: now + REBUILD_MS });        //   exactly one refs_version
    }
    d.q("UPDATE adv SET built_at=? WHERE version=?", vec![V::from(now), V::from(v)])?;
    // 5.3-shaped sweep: only versions aged past KEEP_MS (> any request's life), current version excluded.
    let cutoff = now.saturating_sub(KEEP_MS);
    let stale: Vec<VRow> = d.q("SELECT version FROM adv WHERE version!=? AND built_at<?",
                               vec![V::from(v), V::from(cutoff)])?.to_array()?;
    if !stale.is_empty() {
        rb.charge(1)?;
        bucket.delete(&stale.iter().map(|r| adv_key(&bucket.repo.0, r.version)).collect::<Vec<_>>()).await?; // absent key: no-op
        for r in &stale {
            d.q("DELETE FROM adv WHERE version=? AND built_at<?", vec![V::from(r.version), V::from(cutoff)])?;
        }
    }
    budget.subrequests_used = rb.used;
    Ok(SliceOutcome::Done)
}

// ---- src/edge/adv.rs ----
#[derive(Deserialize)] struct Ptr { v: i64, ready: bool }
#[derive(Deserialize)] struct Snap { head: Option<String>, refs: Vec<Row> }       // wire::http DTO (A8 write-back)
/// One stub + one R2 read for a whole listing; the DO spent O(1). Falls back to the foundation route (1.3) while no
/// snapshot is published — the same O(#refs) span the foundation already pays.
async fn adv_rows(stub: &Stub, repo: &RepoRoute, bucket: &Bucket, budget: &mut ReqBudget) -> Result<Snap, Error> {
    let p: Ptr = stub_json(stub, repo, "/_do/adv", &serde_json::json!({}), budget).await?;
    if p.ready {
        if let Ok(Some(bytes)) = bucket.get_bytes(&adv_key(&bucket.repo.0, p.v), budget).await {  // write-back: inner.get
            if let Ok(s) = serde_json::from_slice::<Snap>(&bytes) { return Ok(s); }
        }                                                                          // raced a sweep or R2 hiccup: fall back
    }
    stub_get(stub, repo, "/_do/refs", budget).await                                  // GET; identical JSON shape (1.3)
}
async fn stub_get<T: serde::de::DeserializeOwned>(stub: &Stub, repo: &RepoRoute, path: &str, budget: &mut ReqBudget)
    -> Result<T, Error> {
    budget.charge(1)?;
    let mut req = Request::new_with_init(&format!("https://do{path}"), RequestInit::new().with_method(Method::Get))?;
    repo.apply_headers(&mut req)?;                                                 // 8.1 headers, same as stub_json
    let mut resp = stub.fetch_with_request(req).await?;
    if resp.status_code() != 200 { return Err(Error::from_do_response(resp).await); }
    resp.json::<T>().await.map_err(|e| Error::Internal(format!("do response: {e}")))
}
/// v2 ls-refs: `ref-prefix` filtering is one `retain` over the snapshot (blocker 1); HEAD arrives inside it (interop 3).
pub async fn ls_refs_edge(stub: &Stub, repo: &RepoRoute, bucket: &Bucket, args: &wire::LsRefsArgs, budget: &mut ReqBudget)
    -> Result<Vec<u8>, Error> {
    let mut snap = adv_rows(stub, repo, bucket, budget).await?;
    if !args.prefixes.is_empty() {
        snap.refs.retain(|r| args.prefixes.iter().any(|p| r.name.as_bytes().starts_with(&p[..])));
    }
    let head = snap.head.as_deref().map(BString::from);
    let mut w = wire::PktWriter::default();
    wire::write_ls_refs(&mut w, args, head.as_ref().map(|h| h.as_bstr()), &to_ref_rows(&snap.refs)?)?;  // symref-target:, peeled:
    Ok(w.out)
}
/// v0 advertisement for both services on info/refs: same snapshot, formatted edge-side; caps are fixed by rule 5.
pub async fn advertise_v0_edge(stub: &Stub, repo: &RepoRoute, bucket: &Bucket, service: wire::Service, budget: &mut ReqBudget)
    -> Result<Vec<u8>, Error> {
    let snap = adv_rows(stub, repo, bucket, budget).await?;
    let head = snap.head.as_deref().map(BString::from);
    let mut w = wire::PktWriter::default();
    wire::write_advertisement_v0(&mut w, service, head.as_ref().map(|h| h.as_bstr()), &to_ref_rows(&snap.refs)?)?;
    Ok(w.out)
}
fn to_ref_rows(rows: &[Row]) -> Result<Vec<wire::RefRow>, Error> {
    let oid = |h: &str| ObjectId::from_hex(h.as_bytes()).map_err(|e| Error::Internal(e.to_string()));  // DO-written hex
    rows.iter().map(|r| Ok(wire::RefRow { name: BString::from(r.name.as_str()), target: oid(&r.target)?,
                                          peeled: r.peeled.as_deref().map(oid).transpose()? })).collect()
}
```

## Why it works
- **The listing the edge serves is exactly one `refs_version`.** The R2 key names its version; `adv_pointer` reports `ready` only when a row for the *current* `refs_version` is published; the publish span re-checks `refs_version` after the `put` before setting `built_at`. A build torn by a mid-build commit fails the second check and reschedules instead of publishing — so unlike the first pass's `Promise.all` over shards, which the review showed could mix ref states, a torn set is structurally unserveable. This is strictly stronger than git's own guarantee: on any stock server the advertisement at T and the reality at T+ε already diverge, and CAS (`commit_push` step 4) plus `not our ref` want-validation (section 9 step 1) are what keep clients correct — both are unchanged here.
- **The O(#refs) cost moved to the tier that scales.** `adv_pointer` is two `SELECT`s on a `WITHOUT ROWID` table; the edge formats through `wire`, which is sync and imports no `worker` (1.1, 1). A `ls-remote` or advertisement now costs the DO one O(1) span instead of an O(#refs) span, so it can no longer queue a commit behind a listing longer than one span — the most the contract permits of the idea's goal. The write path the idea wanted to relieve was already mostly off the DO: ingest lives in the edge (2.4) and a push's DO time is bounded sync spans (`begin`, `lookup`, `index`, `commit`).
- **Enqueue dedup is exactly the firehose policy.** `jobs::enqueue` keeps at most one pending `AdvBuild` (4.5), and each slice builds the *newest* `refs_version`, so intermediate versions are skipped rather than queued; a churning repo builds once after the burst. Drift mid-build reschedules at `REBUILD_MS`, not the retry ladder, so a busy repo does not burn attempts (4.4); in the worst case every listing simply falls back, which is the foundation's status quo.
- **Deletion follows the section 5.3 shape.** Stale `adv` rows are listed and their keys deleted only after `KEEP_MS` (600 s > `max_ms` = 240 s, 7.1), the key name is never reused (`refs_version` is monotonic), and a `get` that loses the race anyway falls through to `/_do/refs` — so deletion is not load-bearing.
- **The write authority died where it had to.** Splitting `refs` across DOs would break section 3 (CAS must see the pack flip and `objects` rows in-span), section 5 (`refs_version` is a per-DO token), and 2.5 (repo-wide connectivity) simultaneously; A9 cannot register a DO namespace regardless. Per the brief, this proof does not pretend otherwise: what is implemented is the reader-isolation kernel, and Known limits says plainly what was given up.
- **Budget (7.1).** A listing is 1 stub + 1 R2 `get`; a build is `refs/5,000` sync `SELECT`s + 1 `put` + at most 1 `delete` inside the 400-subrequest slice (4.3); the fallback path is the foundation's own cost. Every host call goes through `charge`/`spent_80pct` (A1, A4).
- Scenarios this proof must pass (section 11): 1 (empty snapshot -> empty advertisement), 2, 3, 4, 12, 13. Added: "listing consistency under churn" — interleave 20 pushes (each moving a ref pair) with 50 `git ls-remote`; the harness records each committed ref state and asserts every `ls-remote` output equals one of them, never a torn pair.

## Changes from the first pass
| First-pass finding (quoted) | Kind | How addressed |
|---|---|---|
| "Prefix-to-shard routing: default clone/fetch prefixes (`refs/heads/`, `refs/tags/`) map to a nonexistent shard and return zero refs. Needs registry-driven prefix matching." | blocker | Closed structurally: there are no shards to miss. `LsRefsArgs.prefixes` (1.1) is applied by `retain`/`starts_with` over the whole snapshot in `ls_refs_edge`; `refs/heads/` matches every head. |
| "Registry ordering: `register` after `updateRefs` leaves a permanent silent omission of a new namespace from clone/ls-remote on crash. Register first." | blocker | Closed structurally: no registry exists; `refs` is the listing and a snapshot is built under one `refs_version`, so a namespace cannot be half-registered. |
| "`Promise.all` on shard RPC: one shard failure hides successful ref moves on others from the client." | blocker | Closed by contract: one `commit_push` span produces per-ref results in client order (3 step 4); there is no fan-out. The review's second failure mode ("whole-shard `ng` on one stale old-sha") is the per-ref CAS below. |
| "`head()` then CAS is not atomic against a GC that deletes `objects/<sha>` between them" (concurrency walk-through) | blocker | There are no `objects/<sha>` keys (2.2). Tip existence is `Index::lookup` inside the commit span and the lookup-to-commit gap is guarded by `gc_epoch` (3 step 2, section 5). |
| "ls-refs response omits `HEAD` and the `symref-target:` attribute even though `symrefs` is requested; clone then cannot pick the default branch ... `peel` (`peeled:<sha>`) is also unimplemented." (interop 3) | caveat | `head` travels inside the snapshot (`meta.head`, section 8); `wire::write_ls_refs` emits `symref-target:`, `peeled:` (A5 peeled column) and `unborn` per args. |
| "The v2 branch does not check `command=ls-refs`; a `command=fetch` request on the same endpoint is parsed as ls-refs." (interop 4) | caveat | `wire::parse_v2_command` returns `V2Command::{LsRefs, Fetch}` (1.1); this module only replaces the row source behind ls-refs. Fetch still runs in `RepoDo::fetch_v2` (section 9). |
| "the advertisement (`GET info/refs?service=git-receive-pack`) is not shown and must fan out to all shards plus root for HEAD. Must not advertise `atomic`" (interop 5) | caveat | `advertise_v0_edge` builds the v0 advertisement in the edge via `wire::write_advertisement_v0`; caps are fixed by rule 5 (`atomic` never advertised), HEAD comes from the snapshot. |
| "Every push still awaits the single root DO (`register`) and a full-shard advertisement; sharding relieves readers, not writers, per repo." | caveat | No root DO and no registration write: pushes run the foundation's begin/lookup/index/commit sequence on the one RepoDo. The advertisement half is now one O(1) span plus an edge-side R2 read. Writer serialization is unchanged — see Known limits. |
| "Hot ref ceiling unchanged; `refs/heads/main` throughput is one DO, and a "monorepo" where everyone pushes `main` gets nothing from this idea." | caveat | Not addressed — now contract-mandated (8.1, 12). What the module removes is the listing CPU the firehose causes, not the CAS serialization, which is the foundation's chosen consistency unit. |
| "Whole-shard `ng` on one stale old-sha is stricter than git; cross-shard `--atomic` needs #cross-repo-atomic-push." | caveat | Closed by contract: per-ref independent CAS in client order (3 step 4); `atomic` is not advertised (1.1 rule 5), so git never asks for cross-shard semantics. |
| "Cross-shard `ls-refs` fan-out returns no consistent snapshot; a moving ref pair can list at mismatched points." | caveat | Closed and inverted: the snapshot is one `refs_version`, re-checked in the publish span before `built_at` is set; a torn build is rescheduled, never served. |
| "Stray `ref-prefix` values create billable empty DOs; gate `listRefs` on registry membership." | caveat | Closed structurally: no per-prefix DOs can be created; a stray prefix is a `retain` pass over the snapshot and mints nothing. |

## Known limits
- The idea's core does not survive, and this proof says so rather than renaming it: per-branch write authority needs a second DO namespace (none exists in A9's list; 8.1 derives one stub per repo; section 12 names per-branch DOs and replicated refs out of scope), and even if it could be registered, the CAS/`gc_epoch`/connectivity atomicity of sections 3, 5 and 2.5 cannot cross an async stub boundary (platform-facts #4). All pushes, all `fetch` negotiation and every `ls-refs` pointer still serialize on the one RepoDo; only the O(#refs) byte work moved.
- Under a true write firehose the snapshot is never current: every listing pays the `/_do/refs` fallback (the foundation's status quo) plus at most one `AdvBuild` slice at a time (dedup, 4.5). Net cost over the foundation: one background slice and one stub call per listing.
- The build is single-slice: it must finish within `SliceBudget` (20 s / 400 subrequests, 4.3, A4) or `Err(Budget)` restarts it via 4.4 backoff, `dead` after 8 attempts and re-enqueued at `boot` (A4). Whether 5,000-row `SELECT`s can serialize ~600k refs in one slice is **unverified**; if not, the follow-up is the `resume_multipart_upload` cursor pattern of 5.2 (GcConsolidate), and until then the repo permanently falls back.
- `SNAP_MAX` = 64 MiB ≈ 600k refs at ~110 B/row; beyond, `built_at=-1` makes the pointer serve `ready=false` forever and every listing falls back. The edge holds a snapshot whole, so raising it is bounded by the 128 MB isolate (section 7).
- Advertisement staleness is git's inherent race, not a new one: the snapshot is current at the pointer span and at most one request lifetime old; a want for a since-deleted tip fails `not our ref` exactly as it can on stock git.
- Write-backs to CONTRACTS.md, each small: `Bucket::put_bytes(key, bytes, budget)` and `Bucket::get_bytes(key, budget) -> Result<Option<Vec<u8>>>` over `inner` (7.1-charged); the `{"head","refs"}` DTO and the `/_do/adv` request/response types in `wire::http` (A8); `commit_push`'s `any_ok` arm gains `jobs::enqueue(AdvBuild, now, "{}")` beside `GcMark` (3 step 7; `rearm` rides the same post-span call, A3); `jobs::run_slice` gains the `AdvBuild` arm; `meta_i64` is the sibling proofs' helper.
- `/_do/ls-refs` (1.3) stays a valid internal route but the edge stops calling it; `GET /_do/refs` is now only the fallback path.
- `stub_get`/`adv_pointer`'s `Request::new_with_init` path and R2 put/get consistency are unverified at runtime (same class of caveat as two-phase-push; platform-facts #6 covers multipart only).
- The `adv` table leaks at most one row per `refs_version` minus sweeps; rows stuck at `built_at=0` for superseded versions are deleted by the same `built_at < now - KEEP_MS` predicate.

## Depends on
- repo-do-ref-authority
- two-phase-push
- refs-sqlite-objects-r2
- info-refs-endpoint
- protocol-v2-only
- gc-and-repack-alarm
- auth-and-multitenancy
