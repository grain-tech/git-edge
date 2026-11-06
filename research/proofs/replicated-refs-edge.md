# Refs replicated to every region via KV and DO location hints

> Second pass · Idea #13 · verdict: **lands with caveats** · feasibility 4/5 · reliability 4/5 · correctness 4/5 (first pass 4/3/3)
> First pass: [proof](./archived/replicated-refs-edge.md) · [review](../reviews/archived/replicated-refs-edge.md) · Second pass: [review](../reviews/replicated-refs-edge.md) · Contract: [CONTRACTS.md](../../CONTRACTS.md)


## Mechanism
This idea is only partially subsumed. The authority half — one `RepoDo` per repo, per-ref compare-and-swap, the `refs_version` bump — is contract section 3 (`commit_push`, proved in `repo-do-ref-authority`), and the v2 `info/refs?service=git-upload-pack` advertisement is already static at the edge with no DO call (rule 6, `info-refs-endpoint`). What remains genuinely new is a post-foundation module in two halves. `repo_do/refs_publish.rs` is a `JobKind::RefPublish` slice — the only writer of KV — that serialises `refs` (`name`, `target`, `peeled` from A5) plus `meta.head` plus `meta.refs_version` into one KV value `refs/<owner>/<repo>` on binding `REFS_KV`, whenever `refs_version` passes a new `meta` cursor `refs_published`, debounced to KV's ~1 write/s/key by a 1 s `run_at`. `edge/refs_kv.rs` serves `command=ls-refs` (and the v0 upload-pack `info/refs`) from that snapshot through the same `wire::write_ls_refs` / `wire::write_advertisement_v0` the DO path uses, with the client's `x-git-edge-version` header as a read-your-writes floor; a miss, a stale snapshot, a corrupt one, or a KV error all fall through to `/_do/ls-refs` (one sync span, 1.3). The `fetch` command, `git-receive-pack`, and the receive-pack `info/refs` keep going to the DO. Everything added is in the REGISTRY block (A9); the four small contract deltas are listed there and in Known limits. Two weakenings are stated, not hidden: KV is a pull-through edge cache, not a replica (a cold or expired colo still pays a central read), and `locationHint` is creation-time and best-effort — the replicas here are KV values, not DOs.

## Primitives
- `Env::kv("REFS_KV")` -> `KvStore::{get, put}`; `get(..).cache_ttl(60).json::<T>() -> Result<Option<T>, KvError>`; `put(..)? -> PutOptionsBuilder::execute()`: verified in `worker` 0.8.5 source (docs.rs). **Not exercised by the spike**, which touched R2 only; KV from a DO alarm context and local-workerd KV TTL/propagation semantics are **unverified**.
- KV platform limits (GA, quoted correctly by the first pass and its review): ~1 write/s/key (hence `DEBOUNCE_MS`), 25 MiB value cap, `cacheTtl` minimum 60 s, eventual consistency typically under 60 s. Source: Cloudflare KV docs.
- `ObjectNamespace::get_by_name_with_location_hint(name, hint) -> Result<Stub>`: verified in `worker` 0.8.5 source (docs.rs), **not run**. `locationHint` applies at first creation only and is best-effort (Cloudflare DO docs; first-pass review).
- `jobs::{enqueue, dispatch}` with a new `JobKind::RefPublish` arm and `SliceOutcome::{Done, Reschedule}`: contract section 4, A3/A4. `enqueue` is sync and dedups against `queued` **or** `running` rows of the same kind (4.5) — a property this design depends on, see Why it works. Only `jobs::rearm` calls `set_alarm`; a second `setAlarm` cancels the first, measured (#5).
- Sync-span atomicity for the two spans around the KV put: measured (platform-facts #4). The put await opens the input gate; that is handled by the post-await recheck, not assumed away.
- `SELECT changes()` / `CAST(value AS INTEGER)` over TEXT `meta` values: `changes()` measured 1/0/1 (#1); `refs_published` is stored as TEXT like every `meta` value.
- `wire::write_ls_refs` / `write_advertisement_v0` covering `symrefs`, `peel`, `unborn`, `ref-prefix`, `HEAD`-first: measured in the spike against git 2.43 (`protocol-v2-only`). `refs.peeled` exists per A5.
- `Stub::fetch_with_request` + `internal_request` for the fallthrough: stub call verified (spike); the `RequestInit` constructor path is **unverified at runtime**, as in every sibling proof.
- `serde_json::{to_vec, from_slice}` over our own snapshot bytes: verified. `js_sys::Date::now()`: standard `js-sys`.

## Proof code
```rust
// src/repo_do/refs_publish.rs + src/edge/refs_kv.rs + glue lines in edge/{upload,info-refs,receive}.rs
// CONTRACTS 1.1 rules 3,5-7; 3 steps 4-7; 4; 7.1; 8.1-8.2; A1-A5, A8, A9. worker 0.8.5.
// `q`, `meta`, `meta_i64`, `changes`, `now_ms`, `sql` are the RepoDo helpers of repo-do-ref-authority /
// refs-sqlite-objects-r2; `internal_request`, `RepoRoute`, `ReqBudget` as in info-refs-endpoint. `RefDto` is
// the /_do/refs DTO, relocated to wire::http per A8 (repo_do never imports edge); Snapshot joins it there.
//
// REGISTRY (A9) -- every addition over the foundation lists of 1.3/1.4:
//   JobKind::RefPublish                  -> run_slice arm run_ref_publish (below)
//   meta row 'refs_published'            -> init '0' at first boot alongside gc_epoch (8.2)
//   KV binding REFS_KV (kv_namespaces)   -> key refs/<owner>/<repo>, one JSON Snapshot
//   headers: x-git-edge-version (request = read-your-writes floor; response = version served),
//            x-git-edge-source: kv (KV answers only), x-ge-subrequests (7)
//   env var GE_DO_HINT (optional)        -> locationHint for stub creation (8.1 write-back)
//   contract deltas: commit_push step 7 also enqueues RefPublish on any_ok; CommitResponse gains
//   refs_version; boot calls publish_boot_check; RepoRoute::stub honours GE_DO_HINT. See Known limits.
use bstr::BString;
use gix_hash::ObjectId;
use worker::{Env, Headers, Method, Request, Response, SqlStorageValue as V, Stub};
use crate::{edge::{internal_request, RepoRoute}, error::Error,
            jobs::{self, Job, JobKind, SliceBudget, SliceOutcome},
            repo_do::RepoDo, wire::{self, http::RefDto, LsRefsArgs, PktWriter, RefRow}, ReqBudget};
const DEBOUNCE_MS: i64 = 1_000;         // KV ~1 write/s/key (GA)
const KV_TTL_S: u64 = 60;               // GetOptionsBuilder::cache_ttl floor is 60
const KV_MAX_BYTES: usize = 24 << 20;   // 25 MiB value cap minus envelope margin

#[derive(serde::Deserialize)] struct RefSql { name: String, target: String, peeled: Option<String> }
// wire::http (A8): the one value under refs/<owner>/<repo>, written only by run_ref_publish.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct Snapshot { pub version: i64, pub head: Option<String>, pub refs: Vec<RefDto> }

// ---- DO side: the only writer of REFS_KV. Two sync spans around one KV put. ----
impl RepoDo {
    /// boot calls this (8.2): heals a publish whose alarm was lost to a crash between the commit span and
    /// the post-span rearm (first-pass review hole 1). Sync; the caller's jobs::rearm (A3) arms the alarm.
    /// Idempotent: enqueue dedups by kind (4.5), so a queued row is left alone.
    pub fn publish_boot_check(&self) -> Result<(), Error> {
        if self.meta_i64("refs_version")? > self.meta_i64("refs_published")? {
            jobs::enqueue(&self.sql(), JobKind::RefPublish, now_ms() + DEBOUNCE_MS, "{}")?;
        }
        Ok(())
    }
    /// refs/<owner>/<repo>: name-derived because the edge must compute it before any DO contact (8.1).
    /// owner/repo come from meta (8.2), never from ctx.id.name.
    fn kv_key(&self) -> Result<String, Error> { Ok(format!("refs/{}/{}", self.meta("owner")?, self.meta("repo")?)) }
}
/// jobs::run_slice arm. Invariant: publish whenever refs_version > refs_published.
pub async fn run_ref_publish(d: &RepoDo, _job: &Job, budget: &mut SliceBudget) -> Result<SliceOutcome, Error> {
    // Span 1: version and rows are one sync read, so `body` is exactly version `v` -- a commit cannot tear it.
    let v = d.meta_i64("refs_version")?;
    if v <= d.meta_i64("refs_published")? { return Ok(SliceOutcome::Done); }
    let rows = d.q("SELECT name,target,peeled FROM refs ORDER BY name", vec![])?.to_array::<RefSql>()?;
    let snap = Snapshot { version: v, head: Some(d.meta("head")?),
        refs: rows.iter().map(|r| RefDto { name: r.name.clone(), target: r.target.clone(), peeled: r.peeled.clone() }).collect() };
    let body = serde_json::to_vec(&snap).map_err(|e| Error::Internal(e.to_string()))?;
    if body.len() > KV_MAX_BYTES {                                  // one KV value cannot hold this repo
        d.q("INSERT OR REPLACE INTO meta(key,value) VALUES('refs_published',?)", vec![V::from(v.to_string())])?;
        return Ok(SliceOutcome::Done);                              // cursor still advances: edge misses -> DO path
    }
    let kv = d.env.kv("REFS_KV").map_err(|e| Error::Storage(e.to_string()))?;
    budget.subrequests_used = budget.subrequests_used.saturating_add(1);                      // 4.3
    kv.put(&d.kv_key()?, body).map_err(|e| Error::Storage(e.to_string()))?
        .execute().await.map_err(|e| Error::Storage(e.to_string()))?;   // the only await; the input gate opens here
    // Span 2: a commit during the await bumped refs_version, and its enqueue deduped against this *running*
    // row (4.5) -- this recheck is the only place that can see it (first-pass review hole 2).
    d.q("UPDATE meta SET value=? WHERE key='refs_published' AND CAST(value AS INTEGER) < ?",
        vec![V::from(v.to_string()), V::from(v)])?;
    if d.meta_i64("refs_version")? > v {
        return Ok(SliceOutcome::Reschedule { run_at: now_ms() + DEBOUNCE_MS });   // burst coalesces into one more put
    }
    Ok(SliceOutcome::Done)
}

// ---- edge side: read path. Every KV failure mode degrades to the DO, the authority; a broken KV never 500s. ----
fn min_version(req: &Request) -> i64 {   // the pusher echoes the header receive_pack set on its response
    req.headers().get("x-git-edge-version").ok().flatten().and_then(|h| h.parse().ok()).unwrap_or(0)
}
async fn kv_snapshot(env: &Env, repo: &RepoRoute, min: i64, budget: &mut ReqBudget) -> Option<Snapshot> {
    if budget.charge(1).is_err() { return None; }                   // out of budget: the DO call charges and errors
    match env.kv("REFS_KV").ok()?.get(&format!("refs/{}/{}", repo.owner, repo.repo))
        .cache_ttl(KV_TTL_S).json::<Snapshot>().await {
        Ok(Some(s)) if s.version >= min => Some(s),                 // stale-for-this-client counts as a miss
        _ => None,
    }
}
/// Our own JSON, still no unwrap: bad hex makes the whole snapshot a miss, not a 500.
fn into_rows(s: &Snapshot) -> Option<(Option<BString>, Vec<RefRow>)> {
    let oid = |h: &str| ObjectId::from_hex(h.as_bytes()).ok();
    let refs = s.refs.iter().map(|r| Some(RefRow { name: r.name.as_str().into(), target: oid(&r.target)?,
        peeled: r.peeled.as_deref().and_then(oid) })).collect::<Option<Vec<RefRow>>>()?;
    Some((s.head.clone().map(BString::from), refs))
}
/// upload_pack's ls-refs arm (protocol-v2-only), after `body.fill` and `wire::parse_v2_command`; `args` is the
/// parsed LsRefsArgs, `body` the raw v2 command forwarded verbatim on fallthrough. `Fetch` is unchanged (9.1).
async fn ls_refs_edge(req: &Request, env: &Env, repo: &RepoRoute, stub: &Stub, args: &LsRefsArgs, body: &[u8],
                      budget: &mut ReqBudget) -> Result<Response, Error> {
    if let Some(s) = kv_snapshot(env, repo, min_version(req), budget).await {
        if let Some((head, refs)) = into_rows(&s) {
            let mut w = PktWriter::default();
            wire::write_ls_refs(&mut w, args, head.as_deref(), &refs);   // HEAD, symref-target, peeled, unborn, prefixes
            let h = Headers::new();
            h.set("Content-Type", "application/x-git-upload-pack-result")?;
            h.set("x-git-edge-version", &s.version.to_string())?; h.set("x-git-edge-source", "kv")?;
            h.set("x-ge-subrequests", &budget.used.to_string())?;        // what the harness asserts on (7)
            return Ok(Response::from_bytes(w.out)?.with_headers(h));
        }
    }
    budget.charge(1)?;                                                  // the authority: one sync span (1.3)
    stub.fetch_with_request(internal_request(repo, "/_do/ls-refs", Method::Post, Some(body.to_vec()))?)
        .await.map_err(|e| Error::Storage(e.to_string()))
}
// Glue, not repeated in full:
// * info_refs (info-refs-endpoint): Service::UploadPack's non-v2 arm tries kv_snapshot(.., 0, ..) + into_rows
//   and renders wire::write_advertisement_v0 from the same fields before the existing GET /_do/refs path.
//   Service::ReceivePack never reads KV: its advertised oids feed the push CAS (3 step 4), so a stale
//   advertisement would turn a legal fast-forward into a spurious `ng failed to update ref`.
// * receive_pack (repo-do-ref-authority): CommitResponse gains `refs_version`; the report-status response
//   sets `x-git-edge-version` to it. That is the floor configured clients echo via http.extraHeader.
// * RepoRoute::stub (8.1) honours GE_DO_HINT:
//     match env.var("GE_DO_HINT").ok().map(|v| v.to_string()).filter(|h| !h.is_empty()) {
//         Some(hint) => env.durable_object("REPO")?.get_by_name_with_location_hint(&name, &hint),
//         None => env.durable_object("REPO")?.id_from_name(&name)?.get_stub(),
//     }
//   (get_by_name_with_location_hint: worker 0.8.5 source; unverified at runtime; creation-time only.)
```

## Why it works
- **The KV answer is byte-identical to the DO answer.** The snapshot stores `head` (`meta.head`, symbolic, section 3) and per-ref `peeled` (A5), and the edge renders it with the same `write_ls_refs`/`write_advertisement_v0` the DO path uses — so `symref-target:`, `peeled:`, `unborn HEAD`, `ref-prefix` filtering and byte-length pkt-lines are identical by construction (rules 1, 5-7; spike-measured). `git clone` checks out because the writer emits `HEAD`, not because KV special-cases it.
- **One writer, one atomic value, one monotonic version.** `refs_version` is bumped inside the commit span (3 step 5); span 1 captures the rows and the version together, so the published JSON is exactly version `v`. A reader in Tokyo and one in Frankfurt can see v6 and v7, never a torn list — KV stores the snapshot atomically.
- **Lost publish cannot recur (the review's two holes).** The enqueue rides inside the commit's sync span — no dangling `getAlarm().then(setAlarm)` — and A3's post-span `rearm` arms the alarm before the response. If the DO dies between span and rearm, the `queued` row survives but fires nothing; `publish_boot_check` closes that on the next request of any kind, since `refs_version > refs_published` re-enqueues (deduped) and the caller's `rearm` arms it — a quiet repo is healed by its next read, not its next push. A commit during the KV await bumps `refs_version` while its own `enqueue` dedups against the *running* row (4.5); the slice's post-await recheck is the only place that can see that, and `Reschedule` publishes the newer version one debounce later. Worst staleness is bounded by KV propagation (~60 s) + `cacheTtl` (60 s), never "until someone pushes again".
- **Stale can only mean old, never wrong.** Refs name immutable objects; an older snapshot yields an older-but-consistent advertisement. `fetch` always goes to the DO, which validates every `want` against live refs through `Index::lookup` (9.1), so KV lag can never mint a want for an unknown object or a bad pack. A force-pushed-away tip stays fetchable because its pack is only deletable `dead_at + GRACE` (section 5): GRACE 1 h against ~2 min worst KV staleness is a 30x margin — the review's "GC grace must exceed KV lag" is now a contract constant, not a sibling's TODO.
- **Read-your-writes is a floor, not a flag.** `x-git-edge-version` on the report-status response (via `CommitResponse.refs_version`) is echoed by configured clients; `min_version` refuses any snapshot older than what this client already saw and falls through to `/_do/ls-refs`. Unconfigured stock git gets plain eventual consistency — stated in Known limits, matching the review's verdict that this is inherent.
- **Failure policy.** KV errors, misses, stale, oversized (`body > KV_MAX_BYTES` advances the cursor without a put) and corrupt snapshots all degrade to the DO path; KV can never turn `git fetch` into a 500. The publish job's own errors retry through section 4.4 backoff and a `dead` row never blocks the queue (4.5); A4's boot rule re-enqueues it anyway.
- **Budget arithmetic (7.1).** ls-refs costs 1 KV subrequest instead of 1 stub call — the count is the same but the KV read is colo-local within the TTL; publish costs 1 put + sync SQL per ref-change burst, debounced; receive-pack is unchanged plus one sync `enqueue`. The receive-pack `info/refs` stays on the DO deliberately: its advertised oids feed the CAS (3 step 4) and staleness there produces spurious `ng`, while the v2 upload-pack advertisement needed no work at all (static, rule 6).

## Changes from the first pass
| First-pass finding (quoted) | Kind | How addressed |
|---|---|---|
| "Lost-publish race in `alarm()` (dirty cleared after concurrent push) and no alarm re-arm after crash: KV can stay stale indefinitely, which turns 'up to 2 minutes old' into 'until someone pushes again'" — and "void getAlarm().then(setAlarm) is a dangling promise racing the response" | blocker | The `dirty` flag is replaced by a version cursor `refs_published`. Enqueue happens inside the commit span (no dangling promise); A3 `rearm` arms it post-span. Crash between span and rearm: the `queued` row persists and `publish_boot_check` re-arms on the next request whenever `refs_version > refs_published`. Commit during the KV await: post-await recheck -> `Reschedule`, because the interleaved `enqueue` deduped against the running row (4.5). |
| "CAS rejection throws out of `updateRefs` instead of returning `ng`; real `git push` sees a 500 on any non-fast-forward" — and "all-or-nothing rollback is only correct when the client sent `atomic`; git's default is per-ref" | blocker | Closed by the contract, not this module: `commit_push` (section 3) applies each ref as its own CAS with `SELECT changes()` and reports `ng <ref> failed to update ref` per ref in client order; nothing throws out of the span (A2), and `atomic` is not advertised (rule 5). |
| "`HEAD`/`symref-target` absent from the snapshot: `git clone` cannot check out" | blocker | `Snapshot.head` carries `meta.head` and `Snapshot.refs[].peeled` carries the A5 column; `write_ls_refs` emits `symref-target:` on `symrefs` and `unborn HEAD symref-target:` on an empty repo (protocol-v2-only, spike-tested against git 2.43). |
| "`pkt()` computes length from `s.length` (UTF-16 code units); a refname with non-ASCII bytes yields a wrong pkt-line length" | caveat | Closed structurally: `gix-packetline`'s `data_to_write` computes byte lengths over `&[u8]` and ref names are `BString` end to end (rule 1). |
| "`ref-prefix` is ignored ... `unborn` on an empty repo must produce `unborn HEAD symref-target:refs/heads/main`" | caveat | `write_ls_refs` implements both (protocol-v2-only). An empty repo never reaches KV — `refs_version` 0 equals `refs_published` 0, so nothing is published and every request falls through to the DO, which answers correctly. |
| "The v2 `info/refs` reply is static ... and must be served by the Worker for the latency claim to hold; the proof routes it to the DO" / "`info/refs` must be answered at the edge too or the DO hop remains on every fetch" | caveat | The v2 advertisement was already static at the edge (rule 6, `info-refs-endpoint`). The v0 upload-pack advertisement now tries the snapshot first (`info_refs` glue). The receive-pack advertisement deliberately stays on the DO — see Why it works. |
| "`x-git-edge-version` is never sent by stock git; needs `http.extraHeader` per remote" / "Read-your-writes requires client configuration" | caveat | Inherent; unchanged. The push response sets `x-git-edge-version` (write-back via `CommitResponse.refs_version`); configured clients echo it and `min_version` enforces the floor. Everyone else gets eventual consistency, which the review called the correct honest statement. |
| "KV is a pull-through cache, not a replica: cold or expired colos still make a central round trip; the win is repeat fetches within 60s per colo" | caveat | Inherent; stated in Mechanism and Known limits. `cache_ttl(60)` makes repeat reads colo-local; nothing here claims otherwise. |
| "GC grace period must exceed KV lag; owned by a sibling idea, not enforced here" | caveat | Now owned by the contract: `GRACE = 1 h` (section 5) exceeds worst KV staleness (~2 min) by 30x, and dead packs are only deletable after it. |
| "`locationHint` is creation-time only and best-effort; the title over-promises" | caveat | Kept, honest, and now code-backed: `RepoRoute::stub` honours `GE_DO_HINT` through `get_by_name_with_location_hint` (worker 0.8.5 source). It cannot move an existing DO — Known limits. |

## Known limits
- KV is a pull-through edge cache, not a replica: the first read in a colo, and any read after the 60 s TTL, still pays a central KV read. Nothing in this module changes that; the win is repeat `ls-refs`/`ls-remote`/`fetch` negotiation within the TTL, which is the common case the idea targeted.
- Read-your-writes needs `http.extraHeader: x-git-edge-version` per remote (CI runners can set it; stock git cannot send custom headers without config). Unconfigured clients can observe up to ~120 s of staleness (60 s propagation + 60 s TTL), including an apparent rollback after their own push.
- 25 MiB value cap is ~250k refs at ~100 B/ref of JSON. Past it, `refs_published` advances without a put and the repo silently runs on the DO path — operator-visible only via the absence of `x-git-edge-source: kv`. Sharding is `branch-level-dos`'s problem.
- `locationHint` influences first creation only and is best-effort; it cannot move a DO whose pushers migrate continents. `get_by_name_with_location_hint` is verified in `worker` 0.8.5 source but unverified at runtime; `GE_DO_HINT` is one env-wide value, not per-repo.
- The KV key is `refs/<owner>/<repo>`, name-derived because the edge must compute it before any DO contact; a rename (out of scope, section 12) would strand the old key. R2 keys use `repo_id` (8.4) precisely because they outlive names.
- Write-backs proposed to CONTRACTS.md, each small: `commit_push` step 7 also runs `jobs::enqueue(&sql, JobKind::RefPublish, now + DEBOUNCE_MS, "{}")` when `any_ok`; `CommitResponse` gains `refs_version` and `receive_pack` sets `x-git-edge-version` on the report-status response; `boot` calls `publish_boot_check` after the A4 Janitor check; `meta` gains `refs_published` (init `'0'`); `RepoRoute::stub` honours `GE_DO_HINT`.
- KV on local workerd (`wrangler dev` local emulation) is assumed to work for the harness; its TTL/propagation is not simulated, so staleness is exercised through `refs_version` ordering, not wall-clock lag. No platform-facts row covers KV.
- Scenarios this proof must pass (section 11): 1, 2, 12, 13. Added (two): (a) "stale snapshot + read-your-writes": `git push` a branch, capture `x-git-edge-version` N from the response; before the alarm fires, plain `git ls-remote` shows the old tip while `git -c http.extraHeader="x-git-edge-version: N" ls-remote` shows the new one; fire the alarm; plain `ls-remote` shows N. (b) "burst coalesces and heals": two pushes inside one debounce window, fire the alarm once, assert `refs_published == refs_version` and exactly one KV value change; then push once more, make any DO-bound request (`git fetch`) so `boot` runs `publish_boot_check`, fire the alarm, assert the new version is served from KV (`x-git-edge-source: kv`).

## Depends on
- repo-do-ref-authority
- protocol-v2-only
- info-refs-endpoint
- refs-sqlite-objects-r2
- gc-and-repack-alarm
