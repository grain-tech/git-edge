# Signed refs by default with append-only DO reflog

> Second pass · Idea #19 · verdict: **lands with caveats** · feasibility 4/5 · reliability 4/5 · correctness 3/5 (first pass 4/2/3)
> First pass: [proof](./archived/signed-reflog.md) · [review](../reviews/archived/signed-reflog.md) · Second pass: [review](../reviews/signed-reflog.md) · Contract: [CONTRACTS.md](../../CONTRACTS.md)


## Mechanism
A post-foundation module (amendment A9) that turns the foundation `reflog` table (section 3) into a server-signed, hash-chained, append-only audit log, and makes `git push --signed` real on the wire. The signed record is written inside `commit_push`'s existing sync span — the fix for the first pass's fatal shape, which hashed and signed across `crypto.subtle` awaits and thereby opened the input gate between the head read and the CAS (platform-facts #4: seven of eight updates lost). Signing is now synchronous Rust (`ed25519-dalek` over `sha2`): `prev_hash` is read, the per-ref CAS outcome is decided by `changes()` (section 3 step 4), and the signed row is inserted with no await anywhere in the span; `UNIQUE(prev_hash)` makes a hypothetical fork a constraint error that propagates as `Err` out of `fetch` and discards the span's writes (A2). The wire half is a `wire` write-back that accepts the `push-cert` block in send-pack's order — commands live inside the certificate body, not as leading command lines (`queue_commands_from_cert`), which is the review's second blocker — and advertises `push-cert=<nonce>` where the nonce is stateless HMAC-SHA256 (git's `receive.certNonceSeed` scheme), so concurrent advertisements no longer race on a `meta` row and an advertisement costs no storage write. A `JobKind::ReflogAnchor` job, enqueued inside the commit span when any ref moves (dedup, section 4.5; `jobs::rearm` after the span per A3), writes `r/<repo>/reflog/anchor/<seq>.json` plus a `head.json` latest pointer, and — closing the review's wipe scenario — refuses to continue on a `head.json` that names a different public key by setting `meta.reflog_broken`, which fails every later commit inside `signing_key`. `GET /<o>/<r>/reflog` is an authenticated edge route over `POST /_do/reflog`, paginated by `after`/`limit`, returning rows plus the public key so the chain verifies offline. What is genuinely new: the sync signer (`sign_and_log` replacing the foundation's reflog `INSERT`), the cert wire form and gate, the nonce helpers, the anchor job, and the paginated route. The CAS, `pushes` bookkeeping, ordering, and job dispatcher are the contract's.

## Primitives
- `ed25519-dalek` 2, `sha2` 0.10, `hmac` 0.12, `base64` 0.22 on `wasm32-unknown-unknown`: **unverified** — none are in the memo's CI-checked list (memo section 3) or the contract pin list; all are pure Rust and RFC 8032 sign/verify and HMAC need no `getrandom`. Day-1 `cargo build --target wasm32-unknown-unknown`. The Ed25519 seed comes from `platform::random32` (`web_sys::Crypto::get_random_values_with_u8_array`, section 8.2; binding path unverified).
- WebCrypto `crypto.subtle` Ed25519 exists on Workers (first pass) but returns promises; deliberately not used on the sign path — an await between the `prev` read and the CAS is the measured lost-update pattern (platform-facts #4).
- Sync-span atomicity and `Err`-out-of-`fetch` rollback of span writes: measured (#4), contract section 3 and A2.
- `SELECT changes()` as the CAS oracle in `apply_one` upstream: measured 1 / 0 / 1 (#1); `sign_and_log` runs only after it reports 1, in the same span.
- SQLite `BEFORE UPDATE`/`BEFORE DELETE ... RAISE(ABORT)` triggers and `UNIQUE(prev_hash)`: standard SQLite DDL through `SqlStorage::exec` (memo section 1 lists `exec` migrations); trigger execution through worker's `exec` was not exercised in the spike — **unverified at runtime**.
- `jobs::{enqueue, run_slice}` plus a new `JobKind` arm (sections 4.5, A3, A4): contract. Second `setAlarm` cancels the first: measured (#5), relevant only to `rearm`, not to this module.
- `store::Bucket::{get, put}` write-backs over `worker::Bucket::{get, put}`: signatures verified in memo section 1; the budget-carrying form is A1. `SliceBudget.req: ReqBudget` is the bundle-uri write-back this module shares.
- `Stub::fetch_with_request` + `Request::new_with_init` for `/_do/reflog`: verified shape; the `new_with_init` JSON-body path is unverified at runtime (two-phase-push Primitives).
- git wire facts (send-pack.c / receive-pack.c, from source, **not yet a scenario run**): a signed push sends `push-cert\0<caps>` first, the certificate lines, `push-cert-end`, then shallow lines, flush, PACK — there is no leading command line; commands come from the cert body between the blank line and the armor; the advertised capability is `push-cert=<nonce>`; a nonce mismatch is a policy input and strict reject is a legitimate policy.
- SSHSIG (OpenSSH PROTOCOL.sshsig, unverified at runtime): blob = magic `SSHSIG` plus five SSH strings (pubkey, namespace `git`, reserved, `sha512`, signature); the signed message is `SSHSIG` || string(namespace) || string(reserved) || string(hashalg) || string(SHA-512(cert body up to the armor)); the armor is `-----BEGIN SSH SIGNATURE-----`; an OpenSSH public-key line's base64 decodes to exactly the pubkey blob.
- `js_sys::Date::now()`, `serde_json`: standard. `gix_hash::ObjectId::from_hex` for the cert's command oids: verified (spike).

## Proof code
```rust
// src/repo_do/reflog.rs, src/edge/reflog.rs, src/edge/pushcert.rs -- worker 0.8.5.
// CONTRACTS.md 1.1 rule 5, 1.3, 2.3, 3, 4, 5, 7, 8, 10; amendments A1-A9.
// NEW DEPS (not in the memo pin list; wasm32 build is a day-1 check): sha2 0.10, hmac 0.12, ed25519-dalek 2, base64 0.22.
//
// REGISTRY (A9) -- every addition this module makes:
//   tables   reflog += prev_hash TEXT NOT NULL, entry_hash TEXT NOT NULL, sig TEXT NOT NULL,
//            key_id INTEGER NOT NULL DEFAULT 1, UNIQUE(prev_hash); pushes += push_cert TEXT;
//            meta keys reflog_seed / reflog_broken / push_cert_keys; schema_version bump.
//            triggers reflog_ro_u, reflog_ro_d: BEFORE UPDATE|DELETE ON reflog
//            BEGIN SELECT RAISE(ABORT,'reflog is append-only'); END
//   routes   POST /_do/reflog  body wire::http::ReflogDto{ref,after,limit} -> {pub,entries,next_after}  (1.3: awaits none)
//            GET /<o>/<r>/reflog (edge): auth::authenticate, then the stub JSON verbatim
//   job      JobKind::ReflogAnchor, enqueued inside the commit span when any ref moved (3.7 write-back, dedup 4.5);
//            jobs::rearm().await after the span is the route's duty (A3)
//   R2       r/<repo>/reflog/anchor/<seq>.json (immutable), r/<repo>/reflog/head.json; keys::reflog_{anchor,head}
//   wire     ReceiveHeader += push_cert: Option<PushCert>; parse_receive_header accepts the push-cert block;
//            write_advertisement_v0 appends " push-cert=<nonce>" to the cap string when given one (rule 5)
//   commit   CommitRequest += push_cert: Option<String>; push_begin response += cert_keys: Vec<String> from
//            meta.push_cert_keys; finish_push stores the raw cert on pushes; step 4's reflog INSERT -> sign_and_log
use base64::Engine;
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use hmac::{Hmac, Mac};
use serde::Deserialize;
use sha2::{Digest, Sha256, Sha512};
use worker::{Env, Request, Response, SqlStorage, SqlStorageValue as V};
use crate::{auth, edge::{stub_json, RepoRoute}, error::Error, jobs::{Job, SliceBudget, SliceOutcome}, platform,
            repo_do::RepoDo, wire::{http::ReflogDto, ReceiveHeader, RefCommand, RefResult}, ReqBudget};
const NONCE_SLOP_MS: u64 = 300_000;                                    // git receive.certNonceSlop analogue
const PAGE_MAX: i64 = 1_000;
// exec(), json(), now_ms(), oid(), d.q(), d.meta_opt(), d.bucket(): the RepoDo helpers of repo-do-ref-authority.
fn hex(b: &[u8]) -> String { b.iter().map(|x| format!("{x:02x}")).collect() }
fn unhex(s: &str) -> Result<Vec<u8>, Error> {
    s.as_bytes().chunks(2).map(|c| std::str::from_utf8(c).ok().and_then(|h| u8::from_str_radix(h, 16).ok())
        .ok_or_else(|| Error::Internal("bad hex".into()))).collect()
}
fn unhex32(s: &str) -> Result<[u8; 32], Error> { unhex(s)?.try_into().map_err(|_| Error::Internal("seed len".into())) }

// ---- DO side: every function below runs inside commit_push's one sync span (section 3, A2); nothing awaits. ----
/// Per-repo Ed25519 seed, minted on first use in the same span. reflog_broken (set by the anchor job when head.json
/// names another public key) fails every later commit closed: Err out of fetch discards the whole span (A2).
fn signing_key(d: &RepoDo, sql: &SqlStorage) -> Result<SigningKey, Error> {
    if d.meta_opt("reflog_broken")?.is_some() { return Err(Error::Internal("reflog anchor diverged".into())); }
    if let Some(s) = d.meta_opt("reflog_seed")? { return Ok(SigningKey::from_bytes(&unhex32(&s)?)); }
    let seed = platform::random32()?;                                   // the 8.2 generator, shared with PushId/PackId
    exec(sql, "INSERT INTO meta(key,value) VALUES('reflog_seed',?)", vec![hex(&seed).into()])?;
    Ok(SigningKey::from_bytes(&seed))
}
/// Replaces the foundation reflog INSERT of section 3 step 4, on CAS ok only. prev is read in this same span, so the
/// chain head cannot move underneath (platform-facts #4); UNIQUE(prev_hash) turns a residual fork into Err, not a fork.
fn sign_and_log(d: &RepoDo, sql: &SqlStorage, name: &str, old: &str, new: &str, push: &str, who: &str, now: i64)
    -> Result<(), Error> {
    #[derive(Deserialize)] struct H { entry_hash: String }
    let prev = exec(sql, "SELECT entry_hash FROM reflog ORDER BY id DESC LIMIT 1", vec![])?.to_array::<H>()?
        .into_iter().next().map(|r| r.entry_hash).unwrap_or_else(|| "0".repeat(64));
    let canon = format!("{name}\n{old}\n{new}\n{push}\n{who}\n{now}\n{prev}\n");  // binds transition, push, chain
    let entry_hash = hex(&Sha256::digest(canon.as_bytes()));
    let sig = hex(&signing_key(d, sql)?.sign(entry_hash.as_bytes()).to_bytes());  // sig covers the hash: 1 verify/row
    exec(sql, "INSERT INTO reflog(name,old,new,push_id,principal,at,prev_hash,entry_hash,sig,key_id) VALUES(?,?,?,?,?,?,?,?,?,1)",
         vec![name.into(), old.into(), new.into(), push.into(), who.into(), now.into(), prev.into(), entry_hash.into(), sig.into()])?;
    Ok(())
}
/// POST /_do/reflog (1.3 awaits-none): paginated chain + public key. Rows ~500 B, a page is <= ~0.5 MB of JSON.
pub fn reflog_page(d: &RepoDo, b: &ReflogDto) -> Result<Response, Error> {
    #[derive(Deserialize)] struct Row { id: i64, name: String, old: String, new: String, push_id: String, principal: String,
                                        at: i64, prev_hash: String, entry_hash: String, sig: String, key_id: i64 }
    const C: &str = "id,name,old,new,push_id,principal,at,prev_hash,entry_hash,sig,key_id";
    let (after, limit) = (b.after.unwrap_or(0), b.limit.unwrap_or(500).clamp(1, PAGE_MAX));
    let rows: Vec<Row> = match &b.ref_name {
        Some(r) => d.q(&format!("SELECT {C} FROM reflog WHERE name=? AND id>? ORDER BY id LIMIT ?"),
                       vec![r.as_str().into(), after.into(), limit.into()]),
        None    => d.q(&format!("SELECT {C} FROM reflog WHERE id>? ORDER BY id LIMIT ?"),
                       vec![after.into(), limit.into()]),
    }?.to_array()?;
    let pub_hex = d.meta_opt("reflog_seed")?.map(|s| unhex32(&s).map(|k| hex(SigningKey::from_bytes(&k).verifying_key().as_bytes()))).transpose()?;
    json(serde_json::json!({ "pub": pub_hex, "entries": rows, "next_after": rows.last().map(|r| r.id) }))
}
/// JobKind::ReflogAnchor arm. Anchoring only the head still detects any rewrite: changing one row changes every later
/// entry_hash. head.json doubles as the wipe check: a wiped DO mints a new seed on its next push, then the next firing
/// sees pub != ours and sets reflog_broken instead of anchoring a divergent chain.
pub async fn reflog_anchor(d: &RepoDo, _job: &Job, budget: &mut SliceBudget) -> Result<SliceOutcome, Error> {
    #[derive(Deserialize)] struct Head { id: i64, entry_hash: String, sig: String, key_id: i64 }
    let sql = d.sql();
    if d.meta_opt("reflog_broken")?.is_some() { return Ok(SliceOutcome::Done); }
    let Some(h) = exec(&sql, "SELECT id,entry_hash,sig,key_id FROM reflog ORDER BY id DESC LIMIT 1", vec![])?
        .to_array::<Head>()?.into_iter().next() else { return Ok(SliceOutcome::Done) };
    let seed = d.meta_opt("reflog_seed")?.ok_or_else(|| Error::Internal("rows but no key".into()))?;
    let pub_hex = hex(SigningKey::from_bytes(&unhex32(&seed)?).verifying_key().as_bytes());
    let bucket = d.bucket()?;                                            // repo_id from meta (8.2), never ctx.id.name
    let old = bucket.get(&keys::reflog_head(&bucket.repo), &mut budget.req).await?   // A1 budget-carrying write-back
        .map(|b| serde_json::from_slice::<serde_json::Value>(&b)).transpose().map_err(|e| Error::Storage(e.to_string()))?;
    if old.as_ref().and_then(|o| o.get("pub")).and_then(|p| p.as_str()).is_some_and(|p| p != pub_hex) {
        exec(&sql, "INSERT OR REPLACE INTO meta(key,value) VALUES('reflog_broken','1')", vec![])?;
        return Ok(SliceOutcome::Done);
    }
    let body = serde_json::json!({ "repo": bucket.repo, "seq": h.id, "entry_hash": h.entry_hash, "sig": h.sig,
                                   "key_id": h.key_id, "pub": pub_hex, "at": now_ms() }).to_string();
    bucket.put(&keys::reflog_anchor(&bucket.repo, h.id), body.as_bytes(), &mut budget.req).await?;  // immutable per seq
    bucket.put(&keys::reflog_head(&bucket.repo), body.as_bytes(), &mut budget.req).await?;
    Ok(SliceOutcome::Done)                                               // the next anchor is enqueued by the next commit
}

// ---- edge side: awaits allowed; nothing here is inside a DO span. ----
/// Stateless nonce, git's certNonceSeed scheme: caps advertise "push-cert=<unix_ms>-<hmac_sha256(seed,unix_ms)>"
/// with seed = env secret GE_NONCE_SEED. No storage write per advertisement, no cross-advertisement race.
fn nonce(seed: &[u8], now: i64) -> Result<String, Error> { Ok(format!("{now}-{}", hmac_hex(seed, &now.to_string())?)) }
fn nonce_ok(seed: &[u8], n: &str, now: i64) -> Result<bool, Error> {
    let Some((t, tag)) = n.rsplit_once('-') else { return Ok(false) };
    let t: i64 = t.parse().map_err(|_| Error::Protocol("push-cert nonce".into()))?;
    Ok(hmac_hex(seed, &t.to_string())? == tag && t.saturating_sub(now).unsigned_abs() <= NONCE_SLOP_MS)
}
fn hmac_hex(seed: &[u8], msg: &str) -> Result<String, Error> {
    let mut m = <Hmac<Sha256> as Mac>::new_from_slice(seed).map_err(|_| Error::Internal("nonce seed".into()))?;
    m.update(msg.as_bytes()); Ok(hex(&m.finalize().into_bytes()))
}
/// wire write-back: signed pushes send NO leading command lines. send-pack.c order: "push-cert\0<caps>", the cert
/// lines, "push-cert-end", shallow lines, flush, PACK. Commands come from the cert body between its blank line and
/// the armor (receive-pack.c queue_commands_from_cert). Caps stay on the first pkt; shallow parsing is unchanged.
pub struct PushCert { pub raw: String, pub nonce: String }              // nonce = the cert's "nonce <n>" header line
fn commands_from_cert(raw: &str) -> Result<Vec<RefCommand>, Error> {
    let body = raw.get(..raw.find("-----BEGIN").unwrap_or(raw.len())).ok_or_else(|| Error::Protocol("cert".into()))?;
    let (mut cmds, mut past_blank) = (Vec::new(), false);
    for line in body.split('\n') {
        if !past_blank { past_blank = line.trim_end().is_empty(); continue; }
        let mut f = line.split_ascii_whitespace();
        let (Some(o), Some(n), Some(r)) = (f.next(), f.next(), f.next()) else { return Err(Error::Protocol("cert command".into())) };
        cmds.push(RefCommand { old: oid(o)?, new: oid(n)?, name: r.into() });
    }
    if cmds.is_empty() { Err(Error::Protocol("empty push-cert".into())) } else { Ok(cmds) }
}
/// ssh-ed25519 SSHSIG only; a PGP armor or an empty allowed list is stored unverified (Ok(true) either way).
/// Blob: "SSHSIG" + five SSH strings (pubkey, namespace, reserved, hashalg, signature).
fn verify_cert(raw: &str, allowed: &[Vec<u8>]) -> Result<bool, Error> {
    let Some(a) = raw.find("-----BEGIN SSH SIGNATURE-----") else { return Ok(true) };
    if allowed.is_empty() { return Ok(true) }
    let arm = raw.get(a..).and_then(|s| s.split("-----END SSH SIGNATURE-----").next()).ok_or_else(|| Error::Protocol("armor".into()))?;
    let blob = base64::engine::general_purpose::STANDARD.decode(arm.chars().filter(|c| !c.is_whitespace()).collect::<String>())
        .map_err(|e| Error::Protocol(e.to_string()))?;
    if blob.get(..6) != Some(b"SSHSIG") { return Ok(false) }
    let (pk, i) = ssh_str(&blob, 6)?; let (ns, i) = ssh_str(&blob, i)?; let (_r, i) = ssh_str(&blob, i)?;
    let (alg, i) = ssh_str(&blob, i)?; let (sig, _i) = ssh_str(&blob, i)?;
    let (kty, j) = ssh_str(pk, 0)?; let (kb, _j) = ssh_str(pk, j)?; let (_t, j) = ssh_str(sig, 0)?; let (sb, _j) = ssh_str(sig, j)?;
    if ns != b"git" || alg != b"sha512" || kty != b"ssh-ed25519" || !allowed.iter().any(|k| k.as_slice() == pk) { return Ok(false) }
    let body = raw.get(..a).ok_or_else(|| Error::Protocol("cert".into()))?;
    let mut m = b"SSHSIG".to_vec();
    for f in [b"git".as_slice(), &[], b"sha512", &Sha512::digest(body.as_bytes())[..]] { put_str(&mut m, f)?; }
    let key = VerifyingKey::from_bytes(kb.try_into().map_err(|_| Error::Protocol("ssh key".into()))?).map_err(|e| Error::Protocol(e.to_string()))?;
    Ok(key.verify(&m, &Signature::from_slice(sb).map_err(|e| Error::Protocol(e.to_string()))?).is_ok())
}
fn ssh_str(b: &[u8], i: usize) -> Result<(&[u8], usize), Error> {        // SSH "string" = u32be len + bytes
    let e = i.saturating_add(4);
    let n = usize::try_from(u32::from_be_bytes(b.get(i..e).and_then(|s| s.try_into().ok()).ok_or_else(|| Error::Protocol("ssh str".into()))?))
        .map_err(|_| Error::Protocol("ssh str".into()))?;
    let e2 = e.saturating_add(n); Ok((b.get(e..e2).ok_or_else(|| Error::Protocol("ssh str".into()))?, e2))
}
fn put_str(o: &mut Vec<u8>, f: &[u8]) -> Result<(), Error> {
    o.extend_from_slice(&u32::try_from(f.len()).map_err(|_| Error::Internal("ssh str".into()))?.to_be_bytes()); o.extend_from_slice(f); Ok(())
}
/// Runs in receive_pack after read_receive_header + push_begin, before ingest (section 3 ordering unchanged).
/// hdr.commands are already the cert's commands for a signed push. A bad cert is not an unpack failure: report
/// `unpack ok` + per-ref ng in the HTTP-200 report-status that section 10 and A2 mandate after the header.
fn cert_gate(hdr: &ReceiveHeader, seed: &[u8], allowed: &[Vec<u8>]) -> Result<Option<&'static str>, Error> {
    match &hdr.push_cert {
        None => Ok(None),
        Some(c) if !nonce_ok(seed, &c.nonce, now_ms())? => Ok(Some("bad push-cert nonce")),
        Some(c) if !verify_cert(&c.raw, allowed)? => Ok(Some("push-cert signature invalid")),
        Some(_) => Ok(None),
    }
}
// if let Some(r) = cert_gate(&hdr, &seed, &begin.cert_keys)? {
//     return report(&hdr, Ok(()), &hdr.commands.iter().map(|c| RefResult::Ng(c.name.clone(), r)).collect());
// }
/// GET /<o>/<r>/reflog?ref&after&limit -- any valid principal may audit; GE_READ_TOKEN suffices (section 12).
pub async fn reflog(req: &Request, env: &Env, repo: &RepoRoute, b: ReflogDto, budget: &mut ReqBudget) -> Result<Response, Error> {
    auth::authenticate(req, env)?;
    let stub = env.durable_object("REPO")?.id_from_name(&repo.name())?.get_stub()?;
    let v: serde_json::Value = stub_json(&stub, repo, "/_do/reflog", &b, budget).await?;   // two-phase-push helper
    Response::from_json(&v).map_err(|e| Error::Internal(e.to_string()))
}
/// Reference verifier for /reflog consumers (the same math a CLI runs): canon -> sha256 -> Ed25519 over the hex.
pub fn verify_entry(pub_hex: &str, canon: &str, entry_hash: &str, sig_hex: &str) -> Result<bool, Error> {
    if hex(&Sha256::digest(canon.as_bytes())) != entry_hash { return Ok(false) }
    let key = VerifyingKey::from_bytes(&unhex32(pub_hex)?).map_err(|e| Error::Protocol(e.to_string()))?;
    let sig = Signature::from_slice(&unhex(sig_hex)?).map_err(|e| Error::Protocol(e.to_string()))?;
    Ok(key.verify(entry_hash.as_bytes(), &sig).is_ok())
}
```

## Why it works
- **The concurrency blocker cannot recur because nothing is awaited.** Pass one read the chain head and CAS-checked `cur` across `crypto.subtle` awaits; measured platform-facts #4 is that any await opens the input gate, so two concurrent pushes both signed `prev=H0` and both committed. Here every step — `prev` read, per-ref CAS via `changes()`, canonical hash, Ed25519 sign, `INSERT` — is synchronous code inside `commit_push`'s one sync span (section 3, A2). The platform delivers no other DO event inside the span, so the head cannot move between read and insert; `UNIQUE(prev_hash)` additionally makes a residual fork an `Err`, which propagates out of `fetch` and discards the whole span rather than committing half of it. A ref cannot move without its signed record — the atomicity pass one asserted but did not have.
- **Signed pushes now parse the wire shape git actually sends.** `commands_from_cert` mirrors `queue_commands_from_cert`: the `push-cert\0<caps>` first packet, cert lines, `push-cert-end`, then shallow/flush/PACK, with `<old> <new> <ref>` lines extracted from the cert body. Rejection keeps the section 10 / A2 after-header shape — HTTP 200, `unpack ok`, `ng <ref> bad push-cert nonce` or `push-cert signature invalid` per command — so remote-curl prints the reason instead of dropping the body.
- **The stateless nonce removes both nonce defects.** `push-cert=<unix_ms>-<hmac>` needs no `meta` row (no write per advertisement) and no shared state (no cross-advertisement race); verification is recompute-and-compare plus a 5-minute slop, git's `certNonceSeed`/`certNonceSlop` model. The strict-reject policy is a deliberate tightening of git's policy-input semantics, stated in Known limits.
- **Append-only is structural; the record outlives the push.** The triggers reject `UPDATE`/`DELETE` on `reflog` for all application code; `pushes` rows are never deleted (A5 `swept_at`), so the raw push certificate recorded by `finish_push` is as durable as the reflog itself. Enqueue happens in the same span as the CAS, so a crash after commit but before `rearm` can delay the anchor only until the next queued-row dispatch — the Janitor self-enqueues every 15 minutes (section 4.2), which bounds the unanchored window where pass one had none.
- **Wipe detection fails closed.** `head.json` carries the public key; a wiped DO that mints a new seed is caught by the next anchor firing (`pub != ours` -> `reflog_broken`), and every later commit fails in `signing_key` with `Err` out of `fetch` — the "refuse to mint" the review asked for, one job-latency late (Known limits). Anchoring only the head is still complete detection: any tampered row changes every later `entry_hash`.
- **Costs are bounded and charged.** Per moved ref: one extra `SELECT`, one SHA-256, one Ed25519 sign (~tens of µs native, unmeasured on workerd), one `INSERT`, all inside the span. Per firing: one `get`, two `put`s, each `budget.req.charge(1)` (A1). `GET /reflog` pages at `PAGE_MAX` = 1,000 rows of ~500 B — under any response limit, no 32 MB RPC ceiling in play. Per push: one `pushes.push_cert` write (a `<8 KB` string) and one deduped `jobs` row.
- **The claim is honest.** "Signed by default" means server-attested ref transitions on every push; client attestation exists for `ssh-ed25519` `--signed` pushes when `meta.push_cert_keys` names allowed signers, is stored-only otherwise and for GPG, and the signature covers the transition, not the objects (merkle-proofs composes on top for content).

## Changes from the first pass
| First-pass finding (quoted) | Kind | How addressed |
|---|---|---|
| "CAS and head read outside `transactionSync` across non-storage awaits: lost update on `refs` and forked hash chain under concurrent pushes." | blocker | Closed structurally. Signing is synchronous (`ed25519-dalek` + `sha2`, no promises); `sign_and_log` runs inside `commit_push`'s existing sync span after `changes()==1`, so `prev` cannot move between read and insert (platform-facts #4, section 3, A2). `UNIQUE(prev_hash)` — the review's own suggestion — turns any residual fork into a constraint error that propagates as `Err` and discards the span. The WebCrypto path is gone from the sign path entirely. |
| "Signed-push command parsing: commands live inside the `push-cert` block, not in a leading command line; as described, `--signed` pushes are rejected." | blocker | `commands_from_cert` parses the cert body per `queue_commands_from_cert` (blank line to armor); `ReceiveHeader.push_cert` carries `{raw, nonce}`; the advertisement gains `push-cert=<nonce>` (rule 5 write-back). `--signed` pushes reach `commit_push` as ordinary commands with the raw cert stored on `pushes.push_cert`. |
| "`verifyPushCert` is undefined; ship without it and `--signed` is stored-but-unverified (nonce only), which must not be advertised as 'verified'." | caveat | `verify_cert` implements the ssh-ed25519 SSHSIG case end to end: armor decode, five SSH strings, `git`/`sha512` framing, Ed25519 verify against `begin.cert_keys` (`meta.push_cert_keys`). PGP armor and an empty key list are stored-only (`Ok(true)`) and the text says so; nothing claims a stored cert is verified. |
| "Key lives beside the log; append-only is against app code only; anchors in a non-versioned bucket are overwritable." | caveat | Partially addressed. `head.json` now carries the public key and the anchor job refuses to continue on a `pub` mismatch (`reflog_broken` -> every commit fails closed). The seed still lives in DO storage and anchors remain overwritable — detection, not prevention; a versioned bucket stays the `r2-versioned-snapshots` dependency. |
| "Alarm arming outside the transaction; `setAlarm` only after `rows.length` means a repo whose last push crashed pre-arm is never anchored." | caveat | `jobs::enqueue` is a row write inside the commit span (dedup, 4.5), so the anchor job exists atomically with the commit; `jobs::rearm().await` follows the span (A3). A crash between them leaves a `queued` row that any later dispatch picks up — the Janitor's 15-minute self-enqueue bounds the delay, which was unbounded before. |
| "`/reflog` unpaginated; `nonce()` writes on every advertisement; no key-id column for rotation." | caveat | `/_do/reflog` takes `after`/`limit` (default 500, cap `PAGE_MAX`) and returns `next_after`; the nonce is stateless HMAC with no storage write; `key_id` is a column on every row (rotation = new seed + bumped id; tooling remains out of scope, Known limits). |
| "`reflog()` returns the whole chain through an RPC (32 MB RPC/response ceiling; a busy repo blows it without `seq` pagination)" | caveat | Same fix: paginated JSON over `Stub::fetch_with_request` (no typed RPC anywhere), ~0.5 MB per page. |
| "Crash after commit, before `setAlarm`/return ... the R2 anchor is delayed until the next push arms the alarm" (walk-through item 2) | caveat | As above: the enqueue is inside the span; the residual is bounded by the Janitor interval, not by the next push. |
| "DO storage lost/reset: constructor regenerates a fresh keypair silently ... the proof should refuse to mint a second key when an anchor already exists in R2" | caveat | `reflog_anchor` compares `head.json`'s `pub` against the current key and sets `reflog_broken` on mismatch instead of anchoring; `signing_key` then fails every commit. Residual window in Known limits. |
| "git treats a mismatch as a policy input ... the `nonce <n>` line must be compared to what *this* advertisement issued, which the single `meta` row cannot guarantee under concurrent advertisements." | caveat | Stateless nonce: any advertisement's nonce verifies independently, so the guarantee no longer depends on a shared row. The strict-reject choice stands and is documented. |
| First-pass limit: "a push touching thousands of refs does thousands of async WebCrypto calls before `transactionSync`" | limit | Now thousands of sync signs inside the span — still O(refs) sequential DO CPU (unmeasured, Known limits) but never gate-opening. |
| First-pass limit: "~25M ref moves before the table must be rolled to R2" | limit | Unchanged (~500 B/row incl. hash+sig; triggers forbid pruning, so export-and-rotate is an operator action that restarts the chain — anchors make the restart visible). |
| First-pass limit: "the single `meta` nonce row races under concurrent advertisements" | limit | Removed with the `meta` row; see the HMAC nonce. |
| First-pass limit: "no key rotation story" | limit | `key_id` column added; rotating = new `reflog_seed` + bumped id (old rows still verify against the published old `pub` — the endpoint should expose past keys; tooling out of scope). |

## Known limits
- **New dependencies are unverified on wasm32.** `sha2`, `hmac`, `ed25519-dalek`, `base64` are pure Rust and need no RNG for deterministic sign/verify, but none are in the memo's CI-checked list (section 3) or the contract pins; day-1 `cargo build --target wasm32-unknown-unknown`. If they fail, the honest fallback is a `ReflogSign` job that signs rows in a later slice — which weakens "no ref move without its signed record" to eventually-signed and needs a trigger exception for the `sig` update; that weaker shape is not written here.
- **O(refs) sync signing.** Ed25519 sign + SHA-256 per moved ref is ~tens of µs natively but unmeasured on workerd; a tag-mirror push touching 10,000 refs adds on the order of a second of DO CPU inside one span. Still atomic and correct — just longer; the span has no separate subrequest cost (zero awaits).
- **No admin surface for `push_cert_keys`.** Section 12 puts admin APIs out of scope, so the allowed-signers JSON is seeded out-of-band (deployment tooling writing the `meta` row). Unset means every cert is stored-unverified — `verify_cert` returns `Ok(true)`, pushes proceed, and only the nonce is enforced.
- **Wipe-to-anchor window.** A push landing between a DO wipe and the next anchor firing mints a fresh seed and signs on a new chain; the next firing detects the `pub` mismatch and sets `reflog_broken`. If the repo was wiped before its first anchor ever ran, there is no `head.json` and the restart is silent — same residual as pass one, now with an interlock once anchoring has begun.
- **Anchors accumulate.** `anchor/<seq>.json` is one small immutable object per anchored head; `head.json` alone is load-bearing for the wipe check, and a bucket lifecycle rule can expire old anchors (unverified, as in two-phase-push).
- **Strict policy.** Any nonce mismatch or failed signature rejects every ref — stricter than git's `GIT_PUSH_CERT_NONCE_STATUS` policy-input semantics; matching git's laxer modes is a one-line policy change in `cert_gate`.
- **Transitions, not contents.** A signed entry attests "the server moved `name` from `old` to `new` for `principal` at `at`"; binding the new tip's objects needs merkle-proofs. GPG certs and RSA/ECDSA SSHSIG remain stored-only; the extension point is `verify_cert`.
- **Write-backs this proof needs.** The `reflog` columns + `UNIQUE(prev_hash)` + two triggers and `pushes.push_cert` (schema_version bump); `CommitRequest.push_cert`, `push_begin` response `cert_keys`, `finish_push` recording the cert, step 4's `INSERT` -> `sign_and_log`, and the `ReflogAnchor` enqueue beside GcMark in step 7 plus `jobs::rearm` after the span (A3); `ReceiveHeader.push_cert` + the push-cert block in `parse_receive_header` and the `push-cert=<nonce>` cap appended to rule 5's string when a nonce is passed; `wire::http::ReflogDto`; `keys::reflog_{anchor,head}`; `store::Bucket::{get,put}`; `platform::random32`; `SliceBudget.req: ReqBudget` (as in bundle-uri); the four new crates; the `/_do/reflog` and `GET /<o>/<r>/reflog` routes.
- **Scenarios this proof must pass:** 2, 4, 6, 15. Added (two): (a) "signed push": harness generates an `ssh-ed25519` client key, seeds `meta.push_cert_keys` with its base64 blob, runs `git -c gpg.format=ssh -c user.signingkey=<key> push --signed`; assert `ok`, a non-null `pushes.push_cert`, and that `/reflog` entries verify offline via `verify_entry`; repeat with a key absent from `push_cert_keys` and assert `ng <ref> push-cert signature invalid` for every command. (b) "chain audit": run scenario 6's two racing pushes, then assert `SELECT prev_hash, COUNT(*) FROM reflog GROUP BY prev_hash HAVING COUNT(*)>1` is empty, that the `/reflog` chain verifies end to end against `pub`, and that `head.json`'s `seq` equals `MAX(id)` after the next alarm firing.

## Depends on
- repo-do-ref-authority
- two-phase-push
- refs-sqlite-objects-r2
- info-refs-endpoint
- auth-and-multitenancy
- r2-versioned-snapshots (optional, tamper-evident anchors)
