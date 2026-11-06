# Git LFS natively via presigned R2 URLs

> Second pass · Idea #11 · verdict: **lands with caveats** · feasibility 4/5 · reliability 4/5 · correctness 4/5 (first pass 4/3/4)
> First pass: [proof](./archived/native-lfs.md) · [review](../reviews/archived/native-lfs.md) · Second pass: [review](../reviews/native-lfs.md) · Contract: [CONTRACTS.md](../../CONTRACTS.md)


## Mechanism
The LFS batch API is a JSON endpoint beside the git routes, so it is not subsumed by the contract; it is a wave-1 feature that CONTRACTS.md section 12 lists as out of scope, and this proof writes it as four small additions that obey every foundation rule. The edge (`edge::lfs`) authenticates with `auth::authenticate`, checks `can_write` per operation, and forwards `/:owner/:repo.git/info/lfs/objects/batch` and `.../info/lfs/verify` to two new `RepoDo` routes, `/_do/lfs/batch` (awaits: none, one sync span) and `/_do/lfs/verify` (one R2 `head`, then one sync span), over `Stub::fetch_with_request` with the section 8 headers. The DO keeps a new `lfs_objects` table beside `refs`/`packs`, signs presigned R2 URLs in pure Rust (`lfs::sigv4`, sync, no I/O) for the key `r/<repo_id>/lfs/<oid>` (a section 2.2 write-back; `repo_id` from `meta`), and the client PUTs and GETs payloads straight to `<account>.r2.cloudflarestorage.com`, never through the isolate. A new `JobKind::LfsSweep` in the section 4 dispatcher adopts or kills stale `pending` rows and deletes R2 keys only for rows marked `dead` at least `GRACE` earlier, exactly as the section 5 Janitor rule demands.

## Primitives
- R2 S3 presigned URLs: GET/PUT/HEAD/DELETE, expiry 1 s to 7 days, signed headers restrict the request (`X-Amz-SignedHeaders`), S3 domain only, no custom domains. Verified (Cloudflare docs `r2/api/s3/presigned-urls`, 2026-08-22). Needs an R2 API token (access key id + secret) as Worker secrets: `Env::secret(..)` and `Env::var(..)` with `Display` on the binding, verified in `worker` 0.8.5 `env.rs`.
- `worker::Bucket::head(key) -> Result<Option<Object>>`, `Object::{size, checksum}` with `R2Checksums { sha256: Option<Vec<u8>>, .. }`, `Bucket::get(key).execute()`, `ObjectBody::stream() -> ByteStream` (`Stream<Item = Result<Vec<u8>>>`), `Bucket::delete_multiple(Vec<impl Deref<Target = str>>)`: all verified in 0.8.5 source (`r2/mod.rs`, `worker-sys r2/checksums.rs`). `delete_multiple` also closes the sibling two-phase-push "single-key only?" question.
- R2 `head().checksums.sha256` is populated when a sha256 checksum was supplied at upload; docs state this for the `put()` binding (Workers API reference, "Checksums"). Whether an S3 presigned `PutObject` carrying `x-amz-checksum-sha256` (a) stores that checksum and (b) rejects a body that does not hash to it is **unverified** here: the S3 compatibility page has a "Checksum Types" table the search did not return, and the first-pass review cites the 2023-06-16 changelog. Day-1 gate, see Known limits; the code does not depend on (a) and only gains the overwrite guard from (b).
- AWS SigV4 query signing in Rust: `sha2` 0.10, `hmac` 0.12.1, `hex` 0.4.3, `base64` 0.22, all pure Rust with no OS dependency. `hmac` and `hex` are already in the spike's dependency tree (cargo registry); `sha2` and `base64` are **not in the memo's verified list** and were not built here.
- DO SQLite `exec` sync, `SqlStorageValue::{Null, Integer, String}`, `SELECT changes()`: verified (spike, platform-facts #1). Sync-span atomicity after an R2 await: measured (#4).
- `jobs::enqueue` dedup by kind and `jobs::rearm` as the single `set_alarm` caller: contract section 4; second `setAlarm` cancels the first: measured (#5).
- `js_sys::Date::now()` for `created_at`, `X-Amz-Date`, `expires_at`: standard.
- Stub JSON round-trip (`stub_json`, `RepoRoute`, `Request::new_with_init`): from the two-phase-push proof, **unverified at runtime** there too.
- git-lfs 3.x client behaviour (`basic` transfer, `verify` action, per-object `error`, `authenticated: true` suppressing `Authorization`, locks 404 tolerated): from the LFS batch API spec and the first-pass review's interop check; not run here.

## Proof code
```rust
// src/lfs/{api,sigv4}.rs (sync; Env only in from_env), src/repo_do/lfs.rs, src/jobs/lfs_sweep.rs, src/edge/lfs.rs. worker 0.8.5.
// `q`, `changes`, `now_ms`, `json`, `sql`, `bucket()`: RepoDo helpers of repo-do-ref-authority; `stub_json`, `RepoRoute`: two-phase-push.
use futures_util::TryStreamExt; use hmac::{Hmac, Mac}; use sha2::{Digest, Sha256}; use serde_json::json;
use worker::{Env, Method, Request, Response, SqlStorageValue as V};
use crate::{auth::Principal, edge::{stub_json, RepoRoute}, error::Error, jobs::{self, Job, JobKind, SliceBudget, SliceOutcome},
            repo_do::RepoDo, store::{keys, Bucket}, ReqBudget};
pub const TTL_S: u32 = 3_600; const TTL_MS: i64 = 3_600_000; const GRACE_MS: i64 = 3_600_000;        // section 5 GRACE
const MAX_BATCH: usize = 1_000; const MAX_BASIC: u64 = 5 << 30; const MAX_HASH_FALLBACK: u64 = 256 << 20; const HEADS_PER_SLICE: usize = 320;
// ---- src/lfs/api.rs ----   store::keys gains (2.2 write-back): pub fn lfs(repo: &RepoId, oid: &str) -> String { format!("r/{}/lfs/{oid}", repo.0) }
#[derive(serde::Deserialize)] pub struct LfsObj { pub oid: String, pub size: u64 }   #[derive(serde::Deserialize)] pub struct BatchIn { pub operation: String, pub objects: Vec<LfsObj>, pub public_base: String }
/// The only path from a client oid to an R2 key, a signature or a SQL binding: 64 lowercase hex, nothing else (blocker 2).
pub fn oid_bytes(oid: &str) -> Result<[u8; 32], Error> {
    if oid.len() != 64 || !oid.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) { return Err(Error::Protocol("bad lfs oid".into())); }
    hex::decode(oid).ok().and_then(|v| v.try_into().ok()).ok_or_else(|| Error::Protocol("bad lfs oid".into()))
}
// ---- src/lfs/sigv4.rs: AWS SigV4 query presigning. Pure CPU: 2 SHA-256 + 5 HMAC per URL, zero subrequests. ----
pub struct S3Creds { pub account: String, pub bucket: String, pub key_id: String, pub secret: String }
impl S3Creds { pub fn from_env(env: &Env) -> Result<Self, Error> { Ok(Self { account: env.var("R2_ACCOUNT_ID")?.to_string(),
    bucket: env.var("R2_BUCKET_NAME")?.to_string(), key_id: env.secret("R2_ACCESS_KEY_ID")?.to_string(), secret: env.secret("R2_SECRET_ACCESS_KEY")?.to_string() }) } }
fn hmac(key: &[u8], msg: &[u8]) -> Result<Vec<u8>, Error> { let mut m = Hmac::<Sha256>::new_from_slice(key)
    .map_err(|_| Error::Internal("hmac key".into()))?; m.update(msg); Ok(m.finalize().into_bytes().to_vec()) }
fn enc(s: &str) -> String { s.bytes().map(|b| if b.is_ascii_alphanumeric() || b"-_.~".contains(&b)      // RFC 3986 unreserved
    { char::from(b).to_string() } else { format!("%{b:02X}") }).collect() }
fn amz_date(ms: i64) -> (String, String) {                                     // UTC civil date from unix ms (Hinnant's civil_from_days)
    let s = ms.div_euclid(1_000); let (days, t) = (s.div_euclid(86_400), s.rem_euclid(86_400));
    let z = days + 719_468; let (era, doe) = (z.div_euclid(146_097), z.rem_euclid(146_097));
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365; let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153; let d = doy - (153 * mp + 2) / 5 + 1; let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    (format!("{y:04}{m:02}{d:02}"), format!("{y:04}{m:02}{d:02}T{:02}{:02}{:02}Z", t / 3_600, t % 3_600 / 60, t % 60))
}
fn rfc3339(ms: i64) -> String { let a = amz_date(ms).1; format!("{}-{}-{}T{}:{}:{}Z", &a[0..4], &a[4..6], &a[6..8], &a[9..11], &a[11..13], &a[13..15]) }
/// Presigned `method` on `key`, valid TTL_S from `now_ms`. `extra` headers are signed: the client must echo them byte for byte.
pub fn presign(c: &S3Creds, method: &str, key: &str, now_ms: i64, extra: &[(&str, &str)]) -> Result<String, Error> {
    let (date, amz) = amz_date(now_ms); let host = format!("{}.r2.cloudflarestorage.com", c.account);
    let mut hdrs: Vec<(String, String)> = extra.iter().map(|(k, v)| (k.to_ascii_lowercase(), v.trim().to_owned())).collect();
    hdrs.push(("host".into(), host.clone())); hdrs.sort();
    let signed = hdrs.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>().join(";"); let scope = format!("{date}/auto/s3/aws4_request");
    let mut q = vec![("X-Amz-Algorithm", "AWS4-HMAC-SHA256".to_owned()), ("X-Amz-Credential", enc(&format!("{}/{scope}", c.key_id))),
                     ("X-Amz-Date", amz.clone()), ("X-Amz-Expires", TTL_S.to_string()), ("X-Amz-SignedHeaders", enc(&signed))];
    q.sort(); let qs = q.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join("&");
    let path = format!("/{}/{key}", c.bucket);                                  // key = r/<32 hex>/lfs/<64 hex>: nothing to escape
    let canon = format!("{method}\n{path}\n{qs}\n{}\n{signed}\nUNSIGNED-PAYLOAD", hdrs.iter().map(|(k, v)| format!("{k}:{v}\n")).collect::<String>());
    let sts = format!("AWS4-HMAC-SHA256\n{amz}\n{scope}\n{}", hex::encode(Sha256::digest(canon.as_bytes())));
    let mut k = format!("AWS4{}", c.secret).into_bytes();
    for part in [date.as_str(), "auto", "s3", "aws4_request"] { k = hmac(&k, part.as_bytes())?; }
    Ok(format!("https://{host}{path}?{qs}&X-Amz-Signature={}", hex::encode(hmac(&k, sts.as_bytes())?)))
}
// ---- src/repo_do/lfs.rs. lfs_objects(oid TEXT PRIMARY KEY, size INTEGER NOT NULL, state TEXT NOT NULL /*pending|ok|dead*/, created_at INTEGER NOT NULL, verified_at INTEGER, dead_at INTEGER) WITHOUT ROWID
#[derive(serde::Deserialize)] struct Row { size: i64, state: String }   pub enum Head { Missing, Match, Mismatch }
impl RepoDo {
    /// POST /_do/lfs/batch: "Awaits inside: none" (1.3). The edge already returned 403 for upload without can_write.
    pub fn lfs_batch(&self, b: &BatchIn) -> Result<Response, Error> {
        if b.objects.len() > MAX_BATCH { return Err(Error::Limit("lfs batch > 1000 objects".into())); }
        let (creds, repo, now) = (S3Creds::from_env(&self.env)?, self.repo_id()?, now_ms());           // repo_id from meta (8)
        let upload = match b.operation.as_str() { "upload" => true, "download" => false, _ => return Err(Error::Protocol("bad lfs operation".into())) };
        let mut out = Vec::with_capacity(b.objects.len());
        for o in &b.objects {
            let raw = oid_bytes(&o.oid)?; let key = keys::lfs(&repo, &o.oid); let row = self.q("SELECT size, state FROM lfs_objects WHERE oid=?", vec![V::from(o.oid.as_str())])?.to_array::<Row>()?.into_iter().next();
            let ok = row.as_ref().is_some_and(|r| r.state == "ok");
            out.push(match (upload, ok) {
                (false, false) => json!({ "oid": o.oid, "size": o.size, "error": { "code": 404, "message": "Object does not exist" } }),
                (false, true) => json!({ "oid": o.oid, "size": row.map_or(0, |r| r.size), "authenticated": true, "actions": { "download": {
                    "href": presign(&creds, "GET", &key, now, &[])?, "expires_in": TTL_S, "expires_at": rfc3339(now + TTL_MS) } } }),
                (true, true) => json!({ "oid": o.oid, "size": o.size }),                     // no actions: git-lfs skips the upload
                (true, false) if o.size > MAX_BASIC => json!({ "oid": o.oid, "size": o.size, "error": { "code": 422, "message": "exceeds 5 GiB basic transfer limit" } }),
                (true, false) => {
                    let size = i64::try_from(o.size).map_err(|_| Error::Protocol("size".into()))?;
                    self.q("INSERT INTO lfs_objects(oid,size,state,created_at) VALUES(?,?,'pending',?) ON CONFLICT(oid) DO UPDATE SET \
                            size=excluded.size, state='pending', created_at=excluded.created_at, dead_at=NULL WHERE lfs_objects.state!='ok'",
                           vec![V::from(o.oid.as_str()), V::from(size), V::from(now)])?;
                    let sum = base64::engine::general_purpose::STANDARD.encode(raw);          // signed header pins the body to the oid
                    json!({ "oid": o.oid, "size": o.size, "authenticated": true, "actions": {
                        "upload": { "href": presign(&creds, "PUT", &key, now, &[("x-amz-checksum-sha256", &sum)])?,
                                    "header": { "x-amz-checksum-sha256": sum }, "expires_in": TTL_S, "expires_at": rfc3339(now + TTL_MS) },
                        "verify": { "href": format!("{}/verify", b.public_base), "expires_in": TTL_S } } })
                }
            });
        }
        if upload { jobs::enqueue(&self.sql(), JobKind::LfsSweep, now + TTL_MS + GRACE_MS, "{}")?; }    // dedups by kind (4.5): never starves
        json(json!({ "transfer": "basic", "objects": out }))
    }
    /// POST /_do/lfs/verify: one head (plus one get for the hash fallback), then ONE sync span that upserts (blocker 1).
    pub async fn lfs_verify(&self, o: &LfsObj, budget: &mut ReqBudget) -> Result<Response, Error> {
        let raw = oid_bytes(&o.oid)?; let bucket = self.bucket()?; let key = keys::lfs(&bucket.repo, &o.oid);
        let st = match self.lfs_head(&bucket, &key, &raw, o.size, budget).await? { Head::Match => "ok", Head::Mismatch => "dead",
            Head::Missing => return Err(Error::Conflict("object not in storage".into())) };   // row stays pending; the sweep decides
        self.lfs_set_state(&o.oid, o.size, st, "pending")?;                                    // fresh sync span after the await
        if st == "ok" { json(json!({})) } else { Err(Error::Conflict("stored bytes do not hash to oid".into())) }
    }
    /// Upsert with a state guard: `from` = the state the caller observed, so verify and sweep never overwrite each other's decision.
    pub fn lfs_set_state(&self, oid: &str, size: u64, st: &str, from: &str) -> Result<(), Error> {
        let now = now_ms(); let size = i64::try_from(size).map_err(|_| Error::Protocol("size".into()))?;
        let (vat, dat) = (if st == "ok" { V::from(now) } else { V::Null }, if st == "dead" { V::from(now) } else { V::Null });
        self.q("INSERT INTO lfs_objects(oid,size,state,created_at,verified_at,dead_at) VALUES(?,?,?,?,?,?) ON CONFLICT(oid) DO UPDATE SET \
                state=excluded.state, size=excluded.size, verified_at=excluded.verified_at, dead_at=excluded.dead_at WHERE lfs_objects.state=?",
               vec![V::from(oid), V::from(size), V::from(st), V::from(now), vat, dat, V::from(from)])?;
        if self.changes()? != 1 { return Err(Error::Conflict("lfs state changed underneath".into())); }   // CAS outcome = changes() (3)
        Ok(())
    }
    /// The stored sha256 is the check when R2 kept the signed header; otherwise stream-hash up to 256 MiB, one chunk resident.
    pub async fn lfs_head(&self, bucket: &Bucket, key: &str, raw: &[u8; 32], size: u64, budget: &mut ReqBudget) -> Result<Head, Error> {
        budget.charge(1)?;
        let Some(obj) = bucket.inner.head(key).await? else { return Ok(Head::Missing) };
        if obj.size() != size { return Ok(Head::Mismatch); }
        match obj.checksum().sha256 {
            Some(sum) => Ok(if sum.as_slice() == raw { Head::Match } else { Head::Mismatch }),
            None if size > MAX_HASH_FALLBACK => Ok(Head::Mismatch),                           // no checksum, too big to hash here
            None => { budget.charge(1)?; let got = bucket.inner.get(key).execute().await?;
                let Some(body) = got.as_ref().and_then(|o| o.body()) else { return Ok(Head::Missing) };
                let (mut h, mut s) = (Sha256::new(), body.stream()?); while let Some(chunk) = s.try_next().await? { h.update(&chunk); }
                Ok(if h.finalize().as_slice() == raw { Head::Match } else { Head::Mismatch }) }
        }
    }
}
// ---- src/jobs/lfs_sweep.rs: JobKind::LfsSweep, one slice per firing (4.2), never set_alarm (4.1) ----   #[derive(serde::Deserialize)] struct OidRow { oid: String, size: i64 }   #[derive(serde::Deserialize)] struct Next { t: Option<i64> }
pub async fn run_slice(d: &RepoDo, _job: &Job, budget: &mut SliceBudget) -> Result<SliceOutcome, Error> {
    let now = now_ms(); let bucket = d.bucket()?; let mut rb = ReqBudget { max_subrequests: 400, used: budget.subrequests_used, started_ms: budget.started_ms, max_ms: 20_000.0 };
    // 1. pending rows whose URL expired >= GRACE ago: adopt an intact upload the client never verified, else mark dead or drop the row.
    let pend: Vec<OidRow> = d.q("SELECT oid, size FROM lfs_objects WHERE state='pending' AND created_at < ? ORDER BY created_at LIMIT ?",
                                vec![V::from(now - TTL_MS - GRACE_MS), V::from(HEADS_PER_SLICE as i64)])?.to_array()?;
    for r in &pend {
        let (raw, key) = (oid_bytes(&r.oid)?, keys::lfs(&bucket.repo, &r.oid));
        let size = u64::try_from(r.size).map_err(|_| Error::Internal("lfs size".into()))?;
        match d.lfs_head(&bucket, &key, &raw, size, &mut rb).await? {                          // each await, then one sync statement
            Head::Missing => { d.q("DELETE FROM lfs_objects WHERE oid=? AND state='pending'", vec![V::from(r.oid.as_str())])?; }
            Head::Match => { let _ = d.lfs_set_state(&r.oid, size, "ok", "pending"); }        // Conflict = a verify won; fine
            Head::Mismatch => { let _ = d.lfs_set_state(&r.oid, size, "dead", "pending"); }
        }
    }
    budget.subrequests_used = rb.used;
    // 2. R2 deletion only for rows an EARLIER slice marked dead >= GRACE ago (5.3); rows marked in step 1 have dead_at = now.
    let dead: Vec<OidRow> = d.q("SELECT oid, size FROM lfs_objects WHERE state='dead' AND dead_at < ? LIMIT 1000", vec![V::from(now - GRACE_MS)])?.to_array()?;
    if !dead.is_empty() { budget.subrequests_used = budget.subrequests_used.saturating_add(1);
        bucket.inner.delete_multiple(dead.iter().map(|r| keys::lfs(&bucket.repo, &r.oid)).collect::<Vec<String>>()).await?;
        for r in &dead { d.q("DELETE FROM lfs_objects WHERE oid=? AND state='dead'", vec![V::from(r.oid.as_str())])?; }
    }
    if pend.len() >= HEADS_PER_SLICE || dead.len() >= 1_000 { return Ok(SliceOutcome::Continue { cursor: String::new() }); }
    let next = d.q("SELECT MIN(CASE state WHEN 'pending' THEN created_at + ? WHEN 'dead' THEN dead_at + ? END) AS t FROM lfs_objects WHERE state!='ok'",
                   vec![V::from(TTL_MS + GRACE_MS), V::from(GRACE_MS)])?.one::<Next>()?;
    Ok(match next.t { Some(t) => SliceOutcome::Reschedule { run_at: t.max(now + 60_000) }, None => SliceOutcome::Done })
}
// ---- src/edge/lfs.rs: /:owner/:repo(.git)?/info/lfs/<rest>. JSON in, application/vnd.git-lfs+json out (blocker 3). ----
pub async fn route(mut req: Request, env: &Env, repo: &RepoRoute, rest: &str, p: &Principal, budget: &mut ReqBudget) -> Result<Response, Error> {
    if req.method() != Method::Post { return Err(Error::NotFound); }
    let text = req.text().await?; if text.len() > 1 << 20 { return Err(Error::Protocol("lfs body > 1 MiB".into())); }
    let mut b: serde_json::Value = serde_json::from_str(&text).map_err(|e| Error::Protocol(e.to_string()))?; let stub = repo.stub(env)?;
    let v: serde_json::Value = match rest {
        "objects/batch" => {
            if b.get("operation").and_then(|o| o.as_str()) == Some("upload") && !p.can_write { return Err(Error::Forbidden); }   // caveat 5
            let url = req.url()?;                                                              // absolute hrefs, as the LFS spec requires
            b["public_base"] = json!(format!("{}{}", url.origin().ascii_serialization(), url.path().trim_end_matches("/objects/batch")));
            stub_json(&stub, repo, "/_do/lfs/batch", &b, budget).await?
        }
        "verify" if p.can_write => stub_json(&stub, repo, "/_do/lfs/verify", &b, budget).await?,
        "verify" => return Err(Error::Forbidden),
        _ => return Err(Error::NotFound),        // locks API: git-lfs prints "Remote does not support the Git LFS locking API" and continues
    };
    let mut resp = Response::from_json(&v)?; resp.headers_mut().set("content-type", "application/vnd.git-lfs+json")?; Ok(resp)
}
```

## Why it works
- **It is the real batch API, not a lookalike.** git-lfs derives `<remote>/info/lfs/objects/batch`, POSTs `{operation, transfers:["basic"], objects:[{oid,size}]}` with `Accept: application/vnd.git-lfs+json`, and requires that content type back; `edge::lfs::route` sets it on every response. `basic` transfer is "PUT/GET the raw bytes to `href` with these `header`s", which is exactly a presigned S3 URL; the `header` map is echoed verbatim, so the signed `x-amz-checksum-sha256` rides along (`lfs_batch`, `(true, false)` arm). `authenticated: true` stops git-lfs from adding its own `Authorization` to the R2 request, which R2 would reject as a second auth mechanism. Per-object `error: {code: 404}` on download and `{code: 422}` for oversize uploads are the spec's per-object failures; an `ok` object on upload gets no `actions`, which git-lfs reads as "skip".
- **Every foundation rule is kept.** `lfs_batch` is one sync span (section 1.3 "Awaits inside: none"): the JSON is fully parsed by the edge and by serde before any `q`, the per-object `SELECT`/upsert pairs and the `enqueue` run with no await between them. `lfs_verify` and the sweep do their R2 `head` first and then write in a fresh sync span with a state-guarded upsert whose outcome is `SELECT changes()` (section 3; platform-facts #1 and #4). No `rows_written`, no `RETURNING`. The key is `r/<repo_id>/lfs/<oid>` with `repo_id` from `meta` (section 8), never from `ctx.id.name` or the URL. `LfsSweep` only returns `SliceOutcome`; `jobs::rearm` is the sole `set_alarm` caller (section 4.1), `enqueue` dedups by kind (4.5), one slice per firing with `Continue` for backlog (4.2), and the 320-head cap is 80 % of the 400-subrequest slice budget (4.3).
- **R2 deletion obeys section 5 to the letter.** A key is deleted only for a row in state `dead` with `dead_at < now - GRACE`; rows are marked `dead` by `lfs_verify` (checksum mismatch) or by sweep step 1 with `dead_at = now`, so a row marked in this slice cannot qualify in this slice's step 2. `GRACE = 1 h` exceeds `max_ms = 240 s` (section 7.1) and the 1 h URL TTL, so no request and no live presigned URL can still target the key. A `pending` row whose key was never written (client never PUT) is a row delete only; there is no R2 call.
- **The interleavings the review walked through now end well.** Slow PUT outlives the TTL: the row is `pending` until `created_at + TTL + GRACE` (2 h), then the sweep `head`s and *adopts* an intact object as `ok`; a verify arriving before or after the sweep upserts `ok` with `from = 'pending'` and, if the sweep already flipped it, gets `changes() = 0`, which `lfs_set_state` reports as `Conflict` and the sweep ignores while `lfs_verify` returns 409; git-lfs retries the whole object once, the batch then answers "no actions", and a later `download` succeeds because the row is `ok`. Two pushers of one oid: both upserts are `pending`, both PUTs write identical bytes, the first verify wins the CAS, the second sees `ok` and is a no-op conflict. Verify vs sweep during the `head` await: both writes are guarded by `WHERE lfs_objects.state='pending'`, so exactly one decision lands, and both decisions are the same because they read the same immutable object.
- **The signed checksum header does two jobs.** It makes the PUT URL useless for any bytes other than the oid's (R2 rejects a mismatched body, day-1 gate), and it makes `head().checksum().sha256` the verification oracle at zero payload bytes through the Worker. `lfs_head` does not *depend* on the first: if the header is ignored, the stored sha256 is absent and the code stream-hashes the object up to 256 MiB in chunks (`ByteStream` yields one `Vec<u8>` at a time, so memory stays a few MiB), or marks it `dead` above that.
- **Budget (section 7).** A 1,000-object batch: 0 subrequests, about 1,000 SQLite point reads and 7,000 SHA-256 block operations, well under 10 ms of DO CPU. A verify: 1 subrequest, 2 on the fallback path. A sweep slice: at most 320 `head`s + 1 `delete_multiple` (up to 1,000 keys). Payload bytes never enter the isolate, so the 128 MB cap does not apply to object size at all.
- **Error policy (section 10) with one extension.** `Protocol` -> 400, `Forbidden` -> 403, `NotFound` -> 404, `Conflict` -> 409, `Limit` -> 413 as before, but on `/info/lfs/*` the body is `{"message": ...}` with the LFS content type instead of a pkt-line `ERR`; git-lfs prints `message` on non-2xx. A 401 carries `WWW-Authenticate: Basic`, which git-lfs answers through the git credential helper. No `unwrap`, `expect`, `[]` on client data or `as` narrowing: `oid_bytes` gates every oid, `i64::try_from` every size, `?` on every JSON parse; the `&a[0..4]` slices in `rfc3339` are on a string this code generated.

## Changes from the first pass
| First-pass finding (quoted) | Kind | How addressed |
|---|---|---|
| "`verify` uses UPDATE instead of upsert ... verify returns 200 with zero rows touched, and the object is served as 404 until re-pushed" | blocker | `lfs_set_state`: `INSERT ... ON CONFLICT(oid) DO UPDATE ... WHERE lfs_objects.state=?` with `SELECT changes()` checked; a missing row is inserted as `ok`, and the sweep no longer deletes rows the client is about to verify (it adopts them, `Head::Match` arm in `run_slice`). |
| "`oid` is never validated as ^[0-9a-f]{64}$; `new URL()` normalizes `..`, so a crafted oid presigns a PUT into the git-object prefix" | blocker | `oid_bytes` is the only way an oid reaches `keys::lfs`, `presign` or a SQL binding; it accepts exactly 64 lowercase hex. The key is built by `format!`, never by URL parsing, and lives under `r/<repo_id>/lfs/`, so even a valid oid cannot name a `packs/` or `pending/` key. |
| "The verify href (`/info/lfs/verify`) and batch path do not match the DO's `/verify` and `/objects/batch` routes; without the unshown edge rewrite every push ends in `verify failed`" | blocker | `edge::lfs::route` maps `objects/batch` -> `/_do/lfs/batch` and `verify` -> `/_do/lfs/verify`, and passes `public_base` (origin + path up to `/info/lfs`) so the DO builds the absolute `verify.href` git-lfs will call. |
| "test that a mismatched body is actually rejected (400) on a real bucket" | caveat | Not closable from documents: the docs confirm signed headers restrict uploads and that `checksums.sha256` is exposed when supplied, but the S3 `PutObject` checksum table was not retrievable here. Kept as the day-1 gate in Known limits; `lfs_head` is written so that verification does not depend on the stored checksum (stream-hash fallback). |
| "setAlarm on every batch overwrites the pending alarm, so a repo with hourly traffic never sweeps" | caveat | Closed structurally: this code never calls `set_alarm`. `jobs::enqueue(LfsSweep, ..)` is a no-op while a row of that kind is queued (4.5), and `rearm` arms `MIN(run_at)` (4.1). |
| "No R2 delete path: unverified/orphan LFS payloads accumulate until a global GC learns the lfs/ prefix" | caveat | Per-repo key `r/<repo_id>/lfs/<oid>` makes deletion safe without global knowledge; sweep step 2 deletes keys of `dead` rows after GRACE (section 5 rule). Uploaded-but-never-verified intact objects are adopted, not orphaned. Still open: payloads no pointer blob references (reachability GC), see Known limits. |
| "Presigned hrefs bypass CDN and custom domains; hot downloads cost R2 Class B ops per fetch" | caveat | Not addressed: the docs state presigned URLs work only on the S3 domain. Recorded in Known limits with the alternative (public bucket + WAF HMAC, Pro plan). |
| "Per-operation read/write authorization is not in the handler; delegated to auth-and-multitenancy" | caveat | `edge::lfs::route`: `upload` and `verify` require `Principal::can_write` (403 otherwise); `download` needs any authenticated principal, matching the foundation's two-token model (section 12). Finer ACLs remain the dependency. |
| "basic transfer only: objects above the 5 GiB single-PUT cap fail; no locks API, no expires_at" | caveat | `expires_at` (RFC 3339) added beside `expires_in`; oversize uploads get a per-object 422 in the batch reply instead of a failing PUT. 5 GiB cap and no locks remain (Known limits); locks 404 is spec-tolerated. |
| First-pass limit: "Cross-repo dedup is trivial ... but then no repo can safely delete an object" | limit | Reversed by design: keys are per repo, dedup across repos is given up, deletion becomes local and safe. |
| First-pass limit: "pathological monorepos with 100k+ LFS files per batch will hit DO request time limits and should page" | limit | `MAX_BATCH = 1,000` -> `Limit` (413); git-lfs batches 100 by default. |

## Known limits
- **Day-1 gate (unverified):** on a real bucket, a presigned PUT whose body does not hash to the signed `x-amz-checksum-sha256` must be rejected and a matching body must leave `head().checksum().sha256` populated. If (b) fails, every verify takes the stream-hash fallback (1 extra subrequest, CPU proportional to size, 256 MiB cap). If (a) fails, a still-valid PUT URL (1 h) could overwrite an `ok` object with other bytes; the next verify or sweep would mark it `dead` and delete it, which is loss of a good payload. Fallback if (a) fails: sign `if-none-match: *` as well (R2 documents conditional headers on `PutObject`), which turns every re-upload of an existing key into a 412 and needs a client-visible retry rule, or route uploads through the edge with `PutOptionsBuilder::sha256` (verified in 0.8.5) under the 100 MB body cap.
- The local harness cannot exercise this idea: the wrangler R2 simulator has no S3 endpoint, so presigned URLs need `wrangler dev --remote` or a deployed Worker with a real bucket and an R2 API token scoped to that bucket. Scenarios (two, per section 11): (a) `git lfs track '*.bin'`, commit a 3 MiB file, `git push` (expect batch upload, PUT to `r2.cloudflarestorage.com` with the checksum header, verify 200, row `ok`), fresh `git clone` + `git lfs fsck` clean; (b) `curl -X PUT` to the returned href with a wrong body (expect 400), then `verify` (expect 409, row `dead`), advance the clock past GRACE and fire the alarm (expect the key deleted, row gone).
- Reachability GC is not here: an LFS payload whose pointer blob is no longer reachable from any ref stays `ok` forever. `gc-and-repack-alarm`'s `GcMark` must learn to parse pointer blobs (`oid sha256:<hex>`) and mark `lfs_objects` rows; unmarked rows then go `dead` and follow the same GRACE path.
- `basic` transfer only: single PUT, 5 GiB R2 cap; no `multipart` or `ssh` transfer; no locks API; a presigned GET is a bearer token for 1 h. Presigned hrefs bypass the CDN and custom domains (docs), so hot downloads are R2 Class B operations per fetch; the alternative is a public bucket behind WAF HMAC validation (Pro plan) at the cost of per-object auth.
- Cross-repo dedup is given up; the same payload in two repos is stored twice. Forks (`cow-forks`) will need to share the parent's `lfs/` prefix read-only.
- Crates outside the memo: `sha2`, `base64` (not built here), `hmac`, `hex` (present in the spike's tree, not exercised). SigV4 canonicalisation is written from the AWS spec and the docs' example URL, not run against R2; a wrong canonical request shows up as `403 SignatureDoesNotMatch` on the first scenario.
- Time: `js_sys::Date::now()` in the DO is the signing clock; R2 tolerates the usual 15 min skew. `expires_at` is computed from the same instant.
- Budget and memory: batch 0 subrequests, verify 1-2, sweep <= 321 per slice; payloads never enter the isolate; the fallback hash holds one stream chunk. Not measured on a deployed Worker (platform-facts #7).
- Write-backs proposed to CONTRACTS.md: section 2.2 gains `r/<repo_id>/lfs/<oid>` (relaxing "nothing else is ever written to R2"); section 1.3 gains the two routes; `JobKind::LfsSweep`; the `lfs_objects` table; `store::Bucket` exposes `inner` (or gains `head`/`get_stream`/`delete_multiple` wrappers that charge the budget); `Error::Conflict` -> 409 with a JSON body on `/info/lfs/*`; four new bindings (`R2_ACCOUNT_ID`, `R2_BUCKET_NAME` vars; `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY` secrets); section 12 drops "LFS" once landed.

## Depends on
- repo-do-ref-authority
- refs-sqlite-objects-r2
- two-phase-push
- auth-and-multitenancy
- gc-and-repack-alarm
