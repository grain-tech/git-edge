# Content-addressed R2 keys

> Second pass · Idea #5 · verdict: **lands with caveats** · feasibility 5/5 · reliability 4/5 · correctness 3/5 (first pass 4/4/4)
> First pass: [proof](./archived/content-addressed-r2-keys.md) · [review](../reviews/archived/content-addressed-r2-keys.md) · Second pass: [review](../reviews/content-addressed-r2-keys.md) · Contract: [CONTRACTS.md](../../CONTRACTS.md)


## Mechanism
This idea is subsumed by CONTRACTS.md section 2: there is no R2 key per git object any more, so "key = SHA of the bytes" moves from the R2 key to the `objects` row, whose primary key is `(sha, pack_id)` (2.3), and the R2 key is `r/<repo_id>/packs/<pack_id>.pack` with a random `pack_id` per push attempt (2.2). The proof below is the concrete `store` module that replaces it: `store::codec` (a full-object pack entry is `varint(kind,size) + zlib(data)`, 2.1), `store::PackWriter` (one multipart upload per push, 8 MiB parts, running `gix_hash::Hasher` for the trailer, 1.2 and 6.4), `Index::{insert_pack, insert_objects}` (`ON CONFLICT DO NOTHING`, so a retried `/_do/push/index` POST is safe to repeat), and the hashing step of `pack::ingest::resolve_and_normalize` where `gix_object::compute_hash` turns resolved bytes into the sha that is the only way any reader resolves them (2.3 reader query). "Retried pushes are harmless" is now a property of three layers together: R2 (a fresh `pack_id` per attempt, so no attempt can overwrite another), the index (duplicate shas across live packs are legal and deduplicated by `gc_consolidate`, 2.3 and 5.2), and refs (the `changes()` CAS of section 3 moves a ref at most once for a given old oid).

## Primitives
- `worker::Bucket::create_multipart_upload(key).custom_metadata(map).execute() -> MultipartUpload`, `MultipartUpload::{upload_part(u16, Vec<u8>) -> UploadedPart, complete(parts) -> Object, abort()}`, `Object::size()`: present in `worker` 0.8.5 (`r2/mod.rs`, `r2/builder.rs`, read here). Multipart end-to-end **measured on the local R2 simulator only** (platform-facts #6); the 5 MiB minimum and equal-size rule are re-run on first deploy (6.4).
- `worker::Data: From<Vec<u8>>`: present (`r2/mod.rs`). `PutOptionsBuilder::sha1(..)` exists for single-shot `put` only; multipart has no integrity option, so R2-side hash verification from the first pass is **not available** on this path.
- `gix_hash::{hasher, Hasher::{update, try_finalize}}` 0.26.2, collision-detecting SHA-1 as git: source read; CI-built for wasm32 (memo section 3).
- `gix_object::compute_hash(Kind::Sha1, kind, &[u8])` 0.64.1: **ran on workerd** in the spike with ids matching `git verify-pack` (research/rust-spike.md).
- `gix_pack::data::entry::Header::{write_to, as_kind}`, `gix_pack::data::Entry::from_bytes`, `gix_pack::data::header::encode(Version::V2, n)` 0.74.2: source read; the entry parser ran on workerd in the spike.
- `gix_zlib::stream::deflate::Write::new(w, Compression::DEFAULT)` (level 6, git's `pack.compression` default) and `gix_zlib::Inflate::once`: source read (0.1.0, direct dependency per the spike correction 2). Deflate on wasm32 is **not exercised by the spike** (only inflate was); day-1 test.
- `SqlStorage::exec` with `SqlStorageValue: From<String / &str / i64>`: source read (`sql.rs`); synchronous, so `insert_objects` is one sync span (1.3 table, "none").
- `js_sys::Date::now()` for `created_at`: standard `js-sys`, no promise.
- `ReqBudget::charge` before every R2 call (7.1): the subrequest limit is **not enforced by local workerd** (#7), so this counter is the only guard before deploy.
- Duplicate objects inside one pack: git's `index-pack` accepts them unless `--strict` (t5308); the `(sha, pack_id)` primary key with `DO NOTHING` matches that. **Unverified** against git 2.47 specifically; scenario 16 below covers it.

## Proof code
```rust
// src/store/{codec,pack_writer,index}.rs + the hash step of src/pack/ingest.rs
// CONTRACTS.md 1.2, 2.1-2.4, 3 (ordering), 6.4, 7.1, 8.4. worker 0.8.5, gix-* pinned as in the memo.
use std::{collections::HashMap, io::Write as _};
use gix_hash::Kind as HashKind;
use gix_object::Kind;
use gix_pack::data::{entry::Header, header as pack_header, Entry, Version};
use worker::{MultipartUpload, SqlStorage, SqlStorageValue as V, UploadedPart};
use crate::{error::Error, store::{Bucket, Index, ObjRow, PackId, PackState}, ReqBudget};

pub const PART: usize = 8 << 20;        // 6.4: every part is exactly 8 MiB except the last
pub const MAX_OBJECT: u64 = 32 << 20;   // 2.4: single-object cap, inflated
pub struct PackMeta { pub pack: PackId, pub push_id: Option<String>, pub count: u32, pub bytes: u64,
                      pub commit_lo: u64, pub commit_hi: u64, pub created_at: i64 }

pub mod codec {   // sync, no `worker` imports (1.2)
    use super::*;
    /// One full (non-delta) entry: varint(kind,size) + zlib(data). The bytes appended to `out` are
    /// exactly the bytes a reader later copies verbatim into an outgoing pack (2.1, section 9 step 6).
    pub fn encode_entry(kind: Kind, data: &[u8], out: &mut Vec<u8>) -> Result<(), Error> {
        let hdr = match kind { Kind::Commit => Header::Commit, Kind::Tree => Header::Tree,
                               Kind::Blob => Header::Blob, Kind::Tag => Header::Tag };
        let size = u64::try_from(data.len()).map_err(|_| Error::Internal("size".into()))?;
        hdr.write_to(size, out).map_err(|e| Error::Internal(format!("entry header: {e}")))?;
        let mut z = gix_zlib::stream::deflate::Write::new(out, gix_zlib::Compression::DEFAULT);
        z.write_all(data).and_then(|_| z.flush()).map_err(|e| Error::Internal(format!("deflate: {e}")))
    }
    /// (kind, inflated size, header length) of the entry at bytes[0]. A delta header means the pack is
    /// not normalized: an invariant break, never a client error, hence Error::Internal (section 10).
    pub fn entry_header(bytes: &[u8]) -> Result<(Kind, u64, usize), Error> {
        let e = Entry::from_bytes(bytes, 0, HashKind::Sha1).map_err(|e| Error::Storage(format!("entry: {e}")))?;
        let kind = e.header.as_kind().ok_or_else(|| Error::Internal("delta entry in a normalized pack".into()))?;
        Ok((kind, e.decompressed_size, e.header_size()))
    }
    pub fn decode_entry(bytes: &[u8]) -> Result<(Kind, Vec<u8>), Error> {
        let (kind, size, hlen) = entry_header(bytes)?;
        if size > MAX_OBJECT { return Err(Error::Limit("object too large (32 MiB max)".into())); }
        let z = bytes.get(hlen..).ok_or_else(|| Error::Storage("truncated entry".into()))?;
        let mut out = vec![0u8; usize::try_from(size).map_err(|_| Error::Limit("size".into()))?];
        let (status, _in, n) = gix_zlib::Inflate::default().once(z, &mut out).map_err(|e| Error::Storage(format!("inflate: {e}")))?;
        if status != gix_zlib::Status::StreamEnd || n != out.len() {   // same rule as gix-pack's own decoder
            return Err(Error::Storage("entry size does not match its header".into()));
        }
        Ok((kind, out))
    }
}

pub struct PackWriter { mpu: MultipartUpload, pack: PackId, part: Vec<u8>, parts: Vec<UploadedPart>, offset: u64,
                        count: u32, expected: u32, hasher: gix_hash::Hasher, commit_lo: u64, commit_hi: u64, created_at: i64 }
impl PackWriter {
    /// One subrequest. `expected` is pass A's verified entry count (2.4), so the 12-byte header is final
    /// from byte 0 and the running hasher never has to re-hash a patched header (see Known limits).
    pub async fn create(bucket: &Bucket, key: String, expected: u32, budget: &mut ReqBudget) -> Result<Self, Error> {
        let name = key.rsplit('/').next().and_then(|f| f.strip_suffix(".pack"))
            .ok_or_else(|| Error::Internal(format!("not a pack key: {key}")))?;          // keys::pack built it (2.2)
        let created_at = js_sys::Date::now() as i64;                                    // host number, not client bytes
        let meta: HashMap<String, String> = [("repo", bucket.repo.0.clone()), ("pack", name.to_string()),
            ("count", expected.to_string()), ("created_at", created_at.to_string())]
            .into_iter().map(|(k, v)| (k.to_string(), v)).collect();                    // informational only (2.2)
        budget.charge(1)?;
        let mpu = bucket.inner.create_multipart_upload(&key).custom_metadata(meta).execute().await?;
        let header = pack_header::encode(Version::V2, expected);
        let mut hasher = gix_hash::hasher(HashKind::Sha1);
        hasher.update(&header);
        Ok(Self { mpu, pack: PackId(name.to_string()), part: header.to_vec(), parts: Vec::new(), offset: 12,
                  count: 0, expected, hasher, commit_lo: u64::MAX, commit_hi: 0, created_at })
    }
    /// Sync; buffers. Returns (offset, len): exactly the `objects` row (2.3) and the range a reader asks for.
    pub fn append_entry(&mut self, kind: Kind, data: &[u8]) -> Result<(u64, u32), Error> {
        let start = self.part.len();
        codec::encode_entry(kind, data, &mut self.part)?;
        let entry = self.part.get(start..).ok_or_else(|| Error::Internal("part buffer".into()))?;
        let len = u32::try_from(entry.len()).map_err(|_| Error::Limit("entry too long".into()))?;
        self.hasher.update(entry);
        let offset = self.offset;
        self.offset = offset.saturating_add(u64::from(len));
        if kind == Kind::Commit { self.commit_lo = self.commit_lo.min(offset); self.commit_hi = self.offset; }
        self.count = self.count.saturating_add(1);
        Ok((offset, len))
    }
    /// Uploads whole 8 MiB parts while the buffer holds that much; a 32 MiB object drains as four parts.
    /// `upload_part` with the same part number again replaces that part, so one retry is safe to repeat.
    pub async fn flush_if_full(&mut self, budget: &mut ReqBudget) -> Result<(), Error> {
        while self.part.len() >= PART {
            let chunk: Vec<u8> = self.part.drain(..PART).collect();
            self.upload(chunk, budget).await?;
        }
        Ok(())
    }
    async fn upload(&mut self, chunk: Vec<u8>, budget: &mut ReqBudget) -> Result<(), Error> {
        let n = u16::try_from(self.parts.len().saturating_add(1)).map_err(|_| Error::Limit("too many parts".into()))?;
        budget.charge(1)?;
        let part = self.mpu.upload_part(n, chunk).await?;
        self.parts.push(part);
        Ok(())
    }
    /// Trailer, last part, complete. When this returns the pack is durable: ordering step 1 of section 3.
    pub async fn finish(mut self, budget: &mut ReqBudget) -> Result<PackMeta, Error> {
        if self.count != self.expected {
            self.abort().await;
            return Err(Error::Internal(format!("wrote {} entries, header says {}", self.count, self.expected)));
        }
        let trailer = self.hasher.try_finalize().map_err(|e| Error::Unpack(format!("sha1: {e}")))?;   // collision = client bytes
        self.part.extend_from_slice(trailer.as_slice());
        self.flush_if_full(budget).await?;
        if !self.part.is_empty() { let last = std::mem::take(&mut self.part); self.upload(last, budget).await?; }
        let bytes = self.offset.saturating_add(20);
        budget.charge(1)?;
        let obj = self.mpu.complete(self.parts).await?;                                  // R2: visible globally now
        if obj.size() != bytes { return Err(Error::Storage(format!("R2 holds {} bytes, wrote {bytes}", obj.size()))); }
        Ok(PackMeta { pack: self.pack, push_id: None, count: self.count, bytes,
                      commit_lo: self.commit_lo, commit_hi: self.commit_hi, created_at: self.created_at })
    }
    pub async fn abort(self) { let _ = self.mpu.abort().await; }   // best effort; Janitor 5.3 is the backstop
}

fn exec(sql: &SqlStorage, q: &str, args: Vec<V>) -> Result<(), Error> { sql.exec(q, Some(args))?; Ok(()) }
fn i(n: u64) -> Result<V, Error> { Ok(V::from(i64::try_from(n).map_err(|_| Error::Internal("u64 to i64".into()))?)) }
impl<'s> Index<'s> {
    /// Both inserts are safe to repeat: a `/_do/push/index` POST the edge retries after a stub timeout
    /// that the DO had already applied changes nothing (the whole route is one sync span, 1.3).
    pub fn insert_pack(&self, m: &PackMeta, state: PackState) -> Result<(), Error> {
        exec(self.0, "INSERT INTO packs(id,state,count,bytes,commit_lo,commit_hi,push_id,created_at) \
                      VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(id) DO NOTHING",
             vec![m.pack.0.clone().into(), state.as_str().into(), i64::from(m.count).into(), i(m.bytes)?,
                  i(m.commit_lo.min(i64::MAX as u64))?, i(m.commit_hi)?,
                  m.push_id.clone().map_or(V::Null, V::from), m.created_at.into()])
    }
    /// PRIMARY KEY (sha, pack_id) is the content address. A pack carrying one object twice (git allows it
    /// without --strict) keeps the first entry; a second live pack with the same sha is legal (2.3).
    pub fn insert_objects(&self, pack: &PackId, rows: &[ObjRow]) -> Result<(), Error> {
        if rows.len() > 10_000 { return Err(Error::Protocol("more than 10,000 rows".into())); }
        for r in rows {
            exec(self.0, "INSERT INTO objects(sha,pack_id,idx,offset,len,kind,size) VALUES(?,?,?,?,?,?,?) \
                          ON CONFLICT(sha,pack_id) DO NOTHING",
                 vec![r.sha.to_hex().to_string().into(), pack.0.clone().into(), i64::from(r.idx).into(), i(r.offset)?,
                      i64::from(r.len).into(), i64::from(git_kind(r.kind)).into(), i(r.size)?])?;
        }
        Ok(())
    }
}
fn git_kind(k: Kind) -> u8 { match k { Kind::Commit => 1, Kind::Tree => 2, Kind::Blob => 3, Kind::Tag => 4 } }

// pack::ingest::resolve_and_normalize, the one step that turns resolved bytes into a content address (2.4).
// `data` is the fully resolved object (deltas already applied by File::decode_entry, spike correction 2).
pub fn record(out: &mut PackWriter, idx: u32, kind: Kind, data: &[u8]) -> Result<ObjRow, Error> {
    let size = u64::try_from(data.len()).map_err(|_| Error::Unpack("size".into()))?;
    if size > MAX_OBJECT { return Err(Error::Unpack("object too large (32 MiB max)".into())); }
    let sha = gix_object::compute_hash(HashKind::Sha1, kind, data).map_err(|e| Error::Unpack(format!("sha1: {e}")))?;
    let (offset, len) = out.append_entry(kind, data)?;      // same bytes that were hashed, deflated once
    Ok(ObjRow { sha, idx, offset, len, kind, size })
}
```

## Why it works
- **The content address is the row, not the key.** git names an object `SHA-1("<kind> <size>\0" + data)`; `record` computes exactly that with `gix_object::compute_hash` and `append_entry` deflates the same `data` into the pack, so the row `(sha, pack_id, offset, len)` inserted by `insert_objects` points at bytes that inflate to `sha` by construction. The reader query of 2.3 is the only resolver, and it joins on `p.state = 'live'`, so a sha is reachable only after `commit_push` step 3 (section 3).
- **A retried push cannot collide in R2.** `pack_id` is fresh per attempt (1.2), so the second attempt writes `packs/<new>.pack` while the first sits in `ingesting` until the Janitor expires the push and marks the pack `dead` (5.1, 5.2), then deletes the key after GRACE (5.3). No attempt ever writes to a key another attempt owns; "no last-writer-wins hazard" holds because there is no second writer per key, not because writers agree on bytes.
- **A retried index write is a no-op.** Both inserts use `ON CONFLICT DO NOTHING`, and the route is one sync span (1.3), so a stub call that timed out after the DO applied it converges on retry. A duplicate object inside one pack keeps its first row, which is git's default `index-pack` behaviour; the second entry is dead bytes in the pack, as in git.
- **A retried part upload is a no-op.** `upload_part(n, ..)` keys on the part number; repeating it replaces the part with identical bytes (S3 multipart rule, R2 follows it). `finish` checks `Object::size()` from `complete` against the byte count it hashed, at no extra subrequest, which catches a lost or duplicated part before any `objects` row exists.
- **A ref moves once.** After objects are durable and indexed, refs advance only through the `changes()` CAS of section 3 step 4 with the client's advertised old oid, so a client that re-sends the same push after a dropped `report-status` gets `ng ... failed to update ref` on the second attempt if the first one committed, exactly what git prints when the remote moved.
- **Wire semantics are untouched.** git sends a pack whose trailer is SHA-1 over the received bytes; pass A verifies it with `Mode::Verify` (2.4) before any of this code runs. The outgoing pack for a fetch is built from these entries verbatim plus a fresh trailer from `gix_hash::Hasher` (section 9 step 6), so what git receives is a valid v2 pack of full objects, which `git fsck --strict` accepts.
- **Budget.** One multipart create, one `upload_part` per 8 MiB, one `complete`, plus `N/10,000` index posts and `N/1,000` lookups (7.3): a 1,000,000-object push fits in 9,000 subrequests. `flush_if_full` and `finish` charge before every call (7.1).
- Conformance scenarios this proof must pass (section 11): 2, 7 (multipart path), 9 (thin pack resolved then normalized), 12 (incremental pack contains only new objects, which needs `insert_objects` to be complete). Added: **16** "Push retried after a dropped response": kill the edge request after `finish` and before `/_do/push/commit`, re-run `git push`; assert `ok`, one live pack, one `dead` pack after the fake clock passes PUSH_TIMEOUT + GRACE, `fsck` clean. **17** "Pack with a duplicate object": `git pack-objects` a pack, append the same blob entry twice by hand, push with `GIT_TRACE_PACKET`; assert `ok`, one `objects` row for the sha, clone `fsck` clean.

## Changes from the first pass
| First-pass item (quoted) | Kind | How addressed |
|---|---|---|
| "Conflicts with two-phase-push's pending prefix: R2 has no rename, so pending->final doubles Class A ops and bytes" | Caveat | No rename and no same-bytes copy: `pending/<push>.pack` holds the raw thin pack and `packs/<pack>.pack` the normalized full-object pack, different bytes with different purposes (2.2, 2.4). `PackWriter::create` above writes the final key directly; the Janitor deletes `pending/` after GRACE (5.3). |
| "HEAD-skip vs GC-delete race can advance a ref to a deleted object; gc-and-repack-alarm must enforce a grace window ... and re-HEAD under the DO write lock" | Caveat | There is no HEAD and no skip: nothing in this code consults R2 to decide whether to write. Existing objects are found through `/_do/push/lookup` (sync SQLite, live packs only) and the commit is re-guarded by `gc_epoch` (section 3 step 2) and the tip lookup in the same span; `GcSweep` aborts on a `refs_version` change and R2 keys are removed only after GRACE (section 5). |
| "Two R2 subrequests per object against the 10,000/request cap limits a single receive-pack request to ~5,000 objects" | Caveat | `PackWriter`: one `create_multipart_upload`, one `upload_part` per 8 MiB, one `complete` (`flush_if_full`, `finish`); rows go in batches of 10,000 (`insert_objects`). Cost is `N/10,000 + N/1,000 + bytes/8 MiB + 2` (7.3). |
| "No incremental SHA-1 in Web Crypto and 128MB isolate: objects over ~50MB cannot be hashed ... the same gap hits the pack trailer checksum" | Caveat | `gix_hash::Hasher::update` runs incrementally per entry in `append_entry` and produces the trailer in `finish`; no Web Crypto. Per-object hashing in `record` is bounded by the 32 MiB cap of 2.4, which is a foundation limit (see Known limits), not a hashing limit. |
| "Uncompressed storage costs 2-4x R2 bytes and a CompressionStream per object on every fetch" | Caveat | Entries at rest are zlib level 6 (`codec::encode_entry`), git's own pack compression; fetch copies them verbatim (2.1, section 9 step 6), so the read path never inflates or deflates. The R2-side `sha1` put option is given up in exchange (Known limits). |
| "Key scheme hardcodes SHA-1; must reject object-format=sha256 at capability negotiation" | Caveat | `HashKind::Sha1` is fixed in `codec`, `PackWriter::create` and `record`; `object-format=sha256` is rejected at capability parsing by `wire` (CONTRACTS.md conventions paragraph, section 12). |
| "Thin packs from git push mean the parser needs this idea's readObject path to resolve ref-delta bases not in the pack (circular dependency)" | Caveat | The read path is `Bucket::read_entries` and the write path is `PackWriter`, both in `store` (1.2); `pack::ingest` uses both (2.4 between-pass step). The dependency is `pack -> store`, one direction (section 1 dependency rule), so it is no longer circular between ideas. |
| "HEAD-then-PUT is a benign race, not atomic create; R2 onlyIf if-not-exists semantics hand-waved" (proof, Known limits) | Caveat | Neither HEAD nor conditional put exists; the only create is an unconditional multipart under a fresh random `pack_id`. |
| "DO objects table is an index not truth; connectivity check must HEAD R2 for rows missing after a crash mid-ingest" (proof, Known limits) | Caveat | Rows are inserted only after `finish` returned (ordering rule, section 3), under `state='ingesting'`, invisible to `lookup` until commit. A crash leaves rows nobody can resolve; no HEAD is ever needed. |
| "Per-repo prefix means no cross-repo dedup" (proof, Known limits) | Caveat | Not addressed because the contract keeps `r/<repo_id>/` per repo (2.2, 8.4) and lists cross-repo dedup and forks as out of scope (section 12). |
| Review "Crash walk-through": "1,800 orphans sit in R2 (cost only) until gc-and-repack-alarm sweeps keys not in the DO index" | Caveat (body) | Orphans are now one multipart upload per failed attempt, not per object. An uncompleted multipart is aborted by `finish` on count mismatch or by `abort`; a completed pack whose push never commits is expired by the Janitor after PUSH_TIMEOUT and deleted after GRACE (5.1-5.3). Nothing lists R2; the `packs` rows are the list. |
| Review "Interop check": "Serving requires the pack trailer SHA-1 over the whole stream ... upload-pack needs a JS/Wasm SHA-1" | Caveat (body) | `gix_hash::Hasher` is the Wasm SHA-1, used in `PackWriter` here and in `write_pack` (section 9 step 6, owned by protocol-v2-only). |

## Known limits
- **R2 no longer verifies bytes against the key.** The first pass's strongest property, `put(..).sha1(oid)`, does not exist for multipart uploads in `worker` 0.8.5 or in R2. What remains: the client's trailer checked in pass A, `compute_hash` over every resolved object, our own trailer, and the `Object::size()` check in `finish`. A bit flip inside R2 after `complete` is not detected until a fetch client's `index-pack` fails; a `verify` job kind (read each live pack, recompute the trailer) is a later idea, not the foundation.
- **Signature write-backs to 1.2**, all in the direction the sibling proof (refs-sqlite-objects-r2) already took: `PackWriter::create` gains `expected: u32` (pass A's count) and `&mut ReqBudget`; `flush_if_full` and `finish` gain `&mut ReqBudget` (7.1 requires the charge); `encode_entry` and `append_entry` return `Result` because `Header::write_to` and deflate are fallible in type even if not in practice. The 1.2 phrase "patches header count" is replaced by "header count from pass A, verified in `finish`": with multipart, part 1 is already uploaded when the count would be known, and re-uploading it would mean keeping 8 MiB resident beyond the 2.4 budget. Pass A's `Mode::Verify` already checks that the header count equals the entry count, so `expected` is exact.
- **32 MiB inflated per object** (2.4). `codec::decode_entry` and `record` refuse larger objects with `Error::Limit` / `Error::Unpack`; the 128 MB isolate is the reason (window 8 MiB + LRU 16 MiB + base map 32 MiB + one base + one result + part buffer 8 MiB + entry vector up to 48 MiB). LFS is out of scope (section 12).
- **Memory of `PackWriter`**: `part` is bounded by 8 MiB plus one entry (up to about 32 MiB deflated worst case, since a 32 MiB object appended to a 8 MiB minus 1 buffer sits there until `flush_if_full` drains it in 8 MiB parts). `parts` is 24 bytes per part, at most 10,000 parts (6.4).
- **CPU**: deflate at level 6 of a 100 MB push is on the order of seconds on Wasm without SIMD; paid plan `limits.cpu_ms = 300000` (section 7). Free plan is refused above 20 range reads (7). Deflate throughput on workerd is **unmeasured**.
- **Subrequests**: charged by `ReqBudget` only; not enforced by local workerd (#7), measured on a deployed Worker on day 1.
- **Unverified, day-1**: multipart against real R2 with 8 MiB equal parts and a range read on a multi-GB object (#6); `gix_zlib` deflate on wasm32 (inflate ran in the spike, deflate did not); `upload_part` same-number replacement on R2 (documented S3 rule, not measured); whether R2 accepts a final part smaller than 5 MiB when it is the only part (a 0-object pack is never written, 2.4, but a 1-object pack is 32 bytes plus one entry); git 2.47 default acceptance of duplicate objects in a pushed pack (scenario 17).
- Duplicate shas across two live packs cost bytes until `gc_consolidate` (5.2); a client that retries a push three times leaves up to three copies for PUSH_TIMEOUT + GRACE. Cost, not correctness.

## Depends on
- streaming-pack-parser
- two-phase-push
- refs-sqlite-objects-r2
- repo-do-ref-authority
- gc-and-repack-alarm
