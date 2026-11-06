# Packfile parsing in a Worker with a streaming inflater

> Second pass · Idea #4 · verdict: **lands with caveats** · feasibility 4/5 · reliability 3/5 · correctness 4/5 (first pass 4/3/3)
> First pass: [proof](./archived/streaming-pack-parser.md) · [review](../reviews/archived/streaming-pack-parser.md) · Second pass: [review](../reviews/streaming-pack-parser.md) · Contract: [CONTRACTS.md](../../CONTRACTS.md)


## Mechanism
This idea is now the `pack::ingest` module of CONTRACTS.md (section 1.4) running in the edge Worker, fed by `edge::BodyReader` (section 6) after `wire::parse_receive_header` has consumed the command section, and writing to `store` keys `pending/<push>.pack` and `packs/<pack>.pack` (2.2), the `packs` and `objects` tables through `/_do/push/index` (2.3, 1.3), and finally `/_do/push/commit` (section 3). Pass A, `stream_to_pending`, streams the raw pack into R2 through 8 MiB multipart parts while a resumable `gix_zlib::Decompress` finds each entry's zlib boundary, a `gix_hash::Hasher` checks the trailer, and every entry becomes a 24-byte `EntryRec`. Pass B, `resolve_and_normalize`, reads the pending pack back in 8 MiB windows, resolves every ofs-delta, ref-delta and thin-pack base, and appends full objects to the normalized pack with `PackWriter`; delta application goes through `gix_pack::data::File::decode_entry` (CONTRACTS.md correction 2) over a two-entry synthetic pack. The title's `DecompressionStream` survives only for the gzip request body (6.1); it cannot split back-to-back zlib members, `gix-zlib` can.

## Primitives
- `worker::Request::stream() -> ByteStream` (`Stream<Item = Result<Vec<u8>>>`), wrapped by `BodyReader` (6.1): verified in the memo; gzip through `web_sys::DecompressionStream` + `wasm-streams`: **unverified** binding path, owned by section 6.
- R2 multipart `create_multipart_upload(key).execute()`, `MultipartUpload::{upload_part(u16, Data), complete(parts) -> Object, abort}`: API verified in `worker` 0.8.5 source; behaviour measured on the **local simulator only** (platform-facts #6). The 5 MiB minimum and equal-size rule are re-tested on first deploy.
- R2 range read `get(key).range(Range::OffsetWithLength { offset, length }).execute()`: variant name verified in `worker/src/r2/builder.rs`; multi-GB range reads unmeasured on real R2 (#6).
- `gix_pack::data::header::decode(&[u8; 12])`, `gix_pack::data::Entry::from_bytes(&[u8], offset, Kind)` with `header`, `decompressed_size`, `header_size()`: verified in the 0.74.2 source; `from_bytes` returns `Corrupt` on short input, never panics.
- `gix_zlib::Decompress::{new, reset, decompress(input, out, FlushDecompress::None), total_in, total_out}` returning `Status::{Ok, BufError, StreamEnd}`: verified in 0.1.0 source. This is the "consumed input" count the first pass got from `node:zlib`; it survives awaits because the state lives in the struct.
- `gix_pack::data::File::<&[u8]>::from_data(..).with_alloc_limit_bytes(..)`, `File::entry(offset)`, `File::decode_entry(entry, &mut out, &mut gix_zlib::Inflate, &resolve, &mut cache::Never)`: **measured on workerd** by the spike (two OFS deltas resolved, ids equal to `git verify-pack`). `gix_pack::data::delta::apply` is `pub(crate)`: not used.
- `gix_pack::data::entry::Header::{write_to, verified_base_pack_offset, as_kind}` and `gix_zlib::stream::deflate::Write::new(inner, Compression::new(0))`: verified in source; level-0 deflate on wasm **unverified** at runtime (pure `zlib-rs`, no OS dependency).
- `gix_hash::hasher(Sha1)` + `Hasher::{update, try_finalize}` (collision-detecting, like git) and `gix_object::compute_hash`: measured by the spike.
- `BytesToEntriesIter` (2.4): **not used** in pass A, see Changes. It is a synchronous `Iterator` over `BufRead` and cannot suspend mid-entry to await more body bytes; the memo's sync-`Find` constraint applies to `BufRead` too.
- Subrequest budget `ReqBudget::charge` (7.1): the only guard, since local workerd does not enforce the limit (#7). CPU: paid plan, `limits.cpu_ms = 300000`.

## Proof code
```rust
// src/pack/ingest.rs. CONTRACTS.md 1.4, 2.4, 2.5, 6, 7, correction 2. worker 0.8.5, gix-pack 0.74.2, gix-zlib 0.1.0, gix-hash 0.26.2, gix-object 0.64.1.
use std::{collections::{HashMap, VecDeque}, io::Write as _, rc::Rc};
use gix_hash::{Kind as H, ObjectId};
use gix_object::Kind;
use gix_pack::data::{entry::Header, Entry as PackEntry, File};
use gix_zlib::{Compression, Decompress, FlushDecompress, Inflate, Status};
use crate::{edge::BodyReader, error::Error, store::{codec, keys, Bucket, ObjLoc, ObjRow, PackWriter, PushId, RawWriter}, ReqBudget};
pub struct EntryRec { pub offset: u64, pub header_len: u8, pub kind_or_delta: Header, pub compressed_len: u32 }
const MAX_ENTRIES: usize = 2_000_000; const MAX_OBJ: u64 = 32 << 20;   // 2.4: 48 MB of EntryRec; 32 MiB inflated single-object cap
const WINDOW: u64 = 8 << 20; const CACHE: usize = 48 << 20;             // 2.4 / 6.4; 16 MiB resolved LRU + 32 MiB external bases as one cache
const MAX_DEPTH: usize = 64;                                            // git's default delta depth is 50
fn unpack(m: impl Into<String>) -> Error { Error::Unpack(m.into()) }   fn internal(e: impl std::fmt::Display) -> Error { Error::Internal(e.to_string()) }
/// Pass A (2.4). Each zlib boundary comes from a resumable `Decompress` whose `total_in` survives awaits: nothing is
/// re-inflated, no entry has to fit in memory. `BodyReader::fill(n)` is `Ok(true)` iff n bytes are buffered.
pub async fn stream_to_pending(body: &mut BodyReader, bucket: &Bucket, push: &PushId, budget: &mut ReqBudget)
    -> Result<(Vec<EntryRec>, u32), Error> {
    let mut hasher = gix_hash::hasher(H::Sha1);                    // pack SHA-1 over every byte written before the trailer
    let mut out = RawWriter::create(bucket, keys::pending(&bucket.repo, push), budget).await?;   // 8 MiB multipart parts, bytes as received
    if !body.fill(12).await? { return Err(unpack("pack header truncated")); }   let head: [u8; 12] = body.buffered().get(..12).and_then(|s| s.try_into().ok()).ok_or_else(|| unpack("pack header"))?;
    let (version, count) = gix_pack::data::header::decode(&head).map_err(|e| unpack(e.to_string()))?;   // "PACK", v2, count
    if version != gix_pack::data::Version::V2 { return Err(unpack("pack version")); }
    take(body, &mut hasher, &mut out, 12, budget).await?;
    let (mut recs, mut pos, mut z, mut sink) = (Vec::new(), 12u64, Decompress::new(), vec![0u8; 64 << 10]);
    for _ in 0..count {
        if recs.len() >= MAX_ENTRIES { return Err(unpack("too many objects")); }
        body.fill(30).await?;                                     // longest header: 10 size bytes + 20-byte base id; short input errors below
        let e = PackEntry::from_bytes(body.buffered(), pos, H::Sha1).map_err(|e| unpack(format!("entry at {pos}: {e}")))?;
        if e.decompressed_size > MAX_OBJ { return Err(Error::Limit("object too large (32 MiB max)".into())); }
        let hlen = e.header_size(); take(body, &mut hasher, &mut out, hlen, budget).await?;
        let (mut clen, mut st) = (0u64, Status::Ok); z.reset();
        while st != Status::StreamEnd {                           // one zlib member; may span many body chunks and awaits
            if body.buffered().is_empty() && !body.fill(1).await? { return Err(unpack("pack truncated inside object")); }
            let (bi, bo) = (z.total_in(), z.total_out());
            st = z.decompress(body.buffered(), &mut sink, FlushDecompress::None).map_err(|e| unpack(format!("zlib at {pos}: {e}")))?;
            let used = usize::try_from(z.total_in().saturating_sub(bi)).map_err(internal)?;
            if used == 0 && z.total_out() == bo { return Err(unpack(format!("zlib stalled at {pos}"))); }
            take(body, &mut hasher, &mut out, used, budget).await?;
            clen = clen.saturating_add(used as u64);              // widening casts only; every narrowing goes through try_from
        }
        if z.total_out() != e.decompressed_size { return Err(unpack(format!("object at {pos}: size mismatch"))); }
        recs.push(EntryRec { offset: pos, header_len: u8::try_from(hlen).map_err(|_| unpack("header too long"))?,
            kind_or_delta: e.header, compressed_len: u32::try_from(clen).map_err(|_| Error::Limit("entry too large".into()))? });
        pos = pos.saturating_add(hlen as u64).saturating_add(clen);
    }
    if !body.fill(20).await? { return Err(unpack("pack trailer truncated")); }
    let want = hasher.try_finalize().map_err(|_| unpack("sha1 collision in pack"))?;
    if body.buffered().get(..20) != Some(want.as_slice()) { return Err(unpack("bad pack checksum")); }
    out.append(want.as_slice()); body.consume(20);   if body.fill(1).await? { return Err(Error::Protocol("bytes after pack trailer".into())); }
    out.finish(budget).await?;                                    // pending/<push>.pack is durable (2.2) before pass B starts
    Ok((recs, count))
}
async fn take(body: &mut BodyReader, h: &mut gix_hash::Hasher, out: &mut RawWriter, n: usize, budget: &mut ReqBudget) -> Result<(), Error> {
    let bytes = body.buffered().get(..n).ok_or_else(|| unpack("pack truncated"))?;
    h.update(bytes); out.append(bytes); body.consume(n);
    out.flush_if_full(budget).await                               // uploads one part when 8 MiB are buffered (6.4), charges 7.1
}

// ---- Pass B (2.4). Deltas are applied only by gix_pack's decode_entry (correction 2); this module never touches delta opcodes. ----
#[derive(Clone, Copy, PartialEq, Eq, Hash)] enum Key { Off(u64), Id(ObjectId) }   type Obj = (Kind, Rc<Vec<u8>>);
#[derive(Default)] struct Cache { bytes: usize, map: HashMap<Key, Obj>, order: VecDeque<Key> }   // insertion-order eviction:
impl Cache {                                                                                    // git writes bases before their deltas
    fn get(&self, k: &Key) -> Option<Obj> { self.map.get(k).cloned() }
    fn put(&mut self, k: Key, kind: Kind, d: Rc<Vec<u8>>) -> Obj {
        self.bytes = self.bytes.saturating_add(d.len()); self.order.push_back(k); self.map.insert(k, (kind, d.clone()));
        while self.bytes > CACHE { match self.order.pop_front().and_then(|k| self.map.remove(&k)) {
            Some((_, v)) => self.bytes = self.bytes.saturating_sub(v.len()), None => break } }
        (kind, d)
    }
}
struct Window<'a> { bucket: &'a Bucket, key: &'a str, pack_len: u64, start: u64, buf: Vec<u8> }
impl Window<'_> {
    /// Raw bytes of one entry; slides to `[offset, +8 MiB)` with one range read when outside, reads a longer entry exactly (7.1).
    async fn entry(&mut self, rec: &EntryRec, budget: &mut ReqBudget) -> Result<Vec<u8>, Error> {
        let len = u64::from(rec.header_len).saturating_add(u64::from(rec.compressed_len));
        let lo = match rec.offset.checked_sub(self.start).filter(|lo| lo.saturating_add(len) <= self.buf.len() as u64) {
            Some(lo) => lo,
            None => { let n = len.max(WINDOW).min(self.pack_len.saturating_sub(rec.offset));
                      self.buf = self.bucket.read_range(self.key, rec.offset, n, budget).await?; self.start = rec.offset; 0 } };
        let (a, b) = (usize::try_from(lo).map_err(internal)?, usize::try_from(lo.saturating_add(len)).map_err(internal)?);
        self.buf.get(a..b).map(<[u8]>::to_vec).ok_or_else(|| Error::Storage("entry outside window".into()))
    }
}
struct Cx<'a> { win: Window<'a>, cache: Cache, z: Inflate, by_id: HashMap<ObjectId, usize>, external: &'a HashMap<ObjectId, ObjLoc> }
/// One decode_entry over `[PACK v2][base as a level-0 zlib full entry][delta re-headed as ofs-delta][20 zero bytes]`. Level 0 = stored blocks.
fn decode_mini(z: &mut Inflate, base: Option<(Kind, &[u8])>, raw: &[u8]) -> Result<(Kind, Vec<u8>), Error> {
    let mut mini = b"PACK\0\0\0\x02\0\0\0\x02".to_vec();
    if let Some((k, d)) = base {
        let hk = match k { Kind::Commit => Header::Commit, Kind::Tree => Header::Tree, Kind::Blob => Header::Blob, Kind::Tag => Header::Tag };
        hk.write_to(d.len() as u64, &mut mini).map_err(internal)?;
        let mut w = gix_zlib::stream::deflate::Write::new(mini, Compression::new(0).unwrap_or_default());   w.write_all(d).and_then(|_| w.flush()).map_err(internal)?; mini = w.into_inner();
    }
    let (at, e) = (mini.len() as u64, PackEntry::from_bytes(raw, 0, H::Sha1).map_err(|e| unpack(e.to_string()))?);
    let body = raw.get(e.header_size()..).ok_or_else(|| unpack("short entry"))?;   let hdr = match (e.header.is_delta(), base) { (true, Some(_)) => Header::OfsDelta { base_distance: at.saturating_sub(12) },
                                                  (true, None) => return Err(unpack("delta without base")), (false, _) => e.header };
    hdr.write_to(e.decompressed_size, &mut mini).map_err(internal)?;   mini.extend_from_slice(body); mini.extend_from_slice(&[0u8; 20]);
    let file = File::<&[u8]>::from_data(&mini, "mini".into(), H::Sha1).map_err(internal)?.with_alloc_limit_bytes(usize::try_from(MAX_OBJ).ok());
    let mut out = Vec::new(); let oc = file.decode_entry(file.entry(at).map_err(|e| unpack(e.to_string()))?, &mut out, z, &|_, _| None, &mut gix_pack::cache::Never)
        .map_err(|e| unpack(format!("delta: {e}")))?;
    Ok((oc.kind, out))
}
/// Thin-pack base (2.4): prefetched into the cache, else one coalesced read of its live location, else `missing base`.
async fn external(cx: &mut Cx<'_>, id: ObjectId, budget: &mut ReqBudget) -> Result<Obj, Error> {
    if let Some(hit) = cx.cache.get(&Key::Id(id)) { return Ok(hit); }
    let loc = cx.external.get(&id).ok_or_else(|| unpack(format!("missing base {id}")))?.clone();   // not in pack, not live
    let (_, entry) = cx.win.bucket.read_entries(&[(id, loc)], budget).await?.pop().ok_or_else(|| Error::Storage("base read".into()))?;
    let (k, d) = codec::decode_entry(&entry)?; Ok(cx.cache.put(Key::Id(id), k, Rc::new(d)))
}
/// In-pack base at `start` (2.4 "re-read from pending/"): walk the chain back to a cached or full entry, then apply forward, caching each link.
async fn resolve_at(cx: &mut Cx<'_>, entries: &[EntryRec], start: u64, budget: &mut ReqBudget) -> Result<Obj, Error> {
    let (mut off, mut chain) = (start, Vec::new());               // (offset, raw delta entry), innermost last
    let (kind, mut data) = loop {
        if let Some(hit) = cx.cache.get(&Key::Off(off)) { break hit; }
        if chain.len() >= MAX_DEPTH { return Err(unpack("delta chain too deep")); }
        let i = entries.binary_search_by_key(&off, |r| r.offset).map_err(|_| unpack("delta base is not an entry"))?;
        let rec = entries.get(i).ok_or_else(|| internal("idx"))?;
        let raw = cx.win.entry(rec, budget).await?;
        match rec.kind_or_delta {
            Header::OfsDelta { base_distance } => { chain.push((off, raw));
                off = Header::verified_base_pack_offset(off, base_distance).ok_or_else(|| unpack("bad ofs-delta"))?; }
            Header::RefDelta { base_id } => { chain.push((off, raw)); match cx.by_id.get(&base_id).copied() {
                Some(j) => off = entries.get(j).ok_or_else(|| internal("idx"))?.offset, None => break external(cx, base_id, budget).await? } }
            _ => { let (k, d) = decode_mini(&mut cx.z, None, &raw)?; break cx.cache.put(Key::Off(off), k, Rc::new(d)); }
        }
    };
    while let Some((o, raw)) = chain.pop() {
        let (_, d) = decode_mini(&mut cx.z, Some((kind, &data)), &raw)?; data = cx.cache.put(Key::Off(o), kind, Rc::new(d)).1;
    }
    Ok((kind, data))
}
/// `sink` posts `/_do/push/index` in 10,000-row batches (packs row `ingesting` on the first post) and collects links (2.5).
pub async fn resolve_and_normalize(bucket: &Bucket, pending_key: &str, entries: &[EntryRec], external_bases: &HashMap<ObjectId, ObjLoc>,
    out: &mut PackWriter, sink: &mut IndexSink, budget: &mut ReqBudget) -> Result<Vec<ObjRow>, Error> {
    let pack_len = entries.last().map_or(32, |r| r.offset.saturating_add(u64::from(r.header_len)).saturating_add(u64::from(r.compressed_len)).saturating_add(20));
    let mut cx = Cx { win: Window { bucket, key: pending_key, pack_len, start: 0, buf: Vec::new() }, cache: Cache::default(), z: Inflate::default(), by_id: HashMap::new(), external: external_bases };
    let (mut pre, mut sum) = (Vec::new(), 0u64);                  // prefetch external bases up to 32 MiB, coalesced (7.2); the rest on demand
    for (id, loc) in external_bases { sum = sum.saturating_add(u64::from(loc.len)); if sum > 32 << 20 { break; } pre.push((*id, loc.clone())); }
    for (id, entry) in bucket.read_entries(&pre, budget).await? { let (k, d) = codec::decode_entry(&entry)?; cx.cache.put(Key::Id(id), k, Rc::new(d)); }
    let need_ids = entries.iter().any(|r| matches!(r.kind_or_delta, Header::RefDelta { .. }));   // by_id only when ref-deltas exist
    let mut rows = Vec::new();
    for (i, rec) in entries.iter().enumerate() {
        let raw = cx.win.entry(rec, budget).await?;
        let base = match rec.kind_or_delta {
            Header::OfsDelta { base_distance } => Some(resolve_at(&mut cx, entries,
                Header::verified_base_pack_offset(rec.offset, base_distance).ok_or_else(|| unpack("bad ofs-delta"))?, budget).await?),
            Header::RefDelta { base_id } => Some(match cx.by_id.get(&base_id).copied() {
                Some(j) => resolve_at(&mut cx, entries, entries.get(j).ok_or_else(|| internal("idx"))?.offset, budget).await?,
                None => external(&mut cx, base_id, budget).await? }),
            _ => None,
        };
        let (kind, data) = decode_mini(&mut cx.z, base.as_ref().map(|(k, d)| (*k, d.as_slice())), &raw)?;
        let id = gix_object::compute_hash(H::Sha1, kind, &data).map_err(|_| unpack("sha1 collision"))?;
        sink.links.extend(extract_links(kind, &data)?);           // 2.5: the caller subtracts this pack's ids and looks up the rest in 1,000s
        let (offset, len) = out.append_entry(kind, &data); out.flush_if_full(budget).await?;   // full object; commit_lo/hi tracked inside
        rows.push(ObjRow { sha: id, idx: u32::try_from(i).map_err(|_| unpack("too many objects"))?, offset, len, kind, size: data.len() as u64 });
        if need_ids { cx.by_id.insert(id, i); }
        cx.cache.put(Key::Off(rec.offset), kind, Rc::new(data));
        if rows.len() >= 10_000 { sink.post(&rows, budget).await?; rows.clear(); }
    }
    Ok(rows)                                                      // tail rows: the caller posts them with the final PackMeta after finish
}
```

## Why it works
- **Wire shape.** After the flush that ends the command section, `git push` sends `PACK`, version 2, count, `count` entries, a 20-byte SHA-1 of everything before it, and nothing else. `stream_to_pending` decodes the header with `gix_pack::data::header::decode`, rejects v3, and treats bytes after the trailer as `Error::Protocol` (section 10). A delete-only push sends no pack and a new-ref-at-existing-commit push sends a 0-object pack; `edge::receive_pack` checks `remainder()` plus `fill(12)` before calling this module and passes `pack_id: None` to commit (2.4 last paragraph, sibling proof repo-do-ref-authority), so neither reaches `stream_to_pending`.
- **Per-object zlib boundary without a length prefix.** A pack entry carries only the inflated size; the compressed length is known when zlib reports stream end. `Decompress::decompress` with `FlushDecompress::None` consumes as much input as it can, `total_in` says how much, and the state persists in the struct between calls, so the loop feeds whatever the body has buffered, awaits `fill(1)` when empty, and never re-inflates (first-pass caveat "retry-from-start 2x CPU" is gone). The trailing bytes after one member are simply the next entry's header, which is what the first pass proved `DecompressionStream` cannot do.
- **Trailer check is incremental.** `gix_hash::Hasher` is updated in `take` with every header and compressed byte that is written to R2, so the pack SHA-1 is computed while streaming; `Mode::Verify` in 2.4 is this check. A mismatch is `unpack error bad pack checksum` (1.4).
- **Memory per request is bounded (6.5).** Pass A holds one body window, one 8 MiB multipart part, a 64 KiB inflate sink and the `EntryRec` vector (24 bytes x 2,000,000 max). Pass B holds one 8 MiB window (or one entry up to 32 MiB compressed), the 48 MiB cache, one base and one result up to 32 MiB each, and the multipart part. `with_alloc_limit_bytes(32 MiB)` makes gix refuse a delta whose header claims a larger result before allocating (2.4 `object too large`).
- **Deltas resolve in offset order with one read per 8 MiB (2.4).** git writes a base before its deltas, so the base is in the insertion-ordered cache when a delta arrives; `resolve_at` is the fallback for an evicted base and costs one range read per chain link outside the window, bounded by `MAX_DEPTH`. `verified_base_pack_offset` rejects an ofs-delta pointing before the pack start, and `binary_search_by_key` rejects one pointing between entries, both as `unpack error` rather than a panic.
- **Thin packs (scenario 9).** `ref-delta` bases not in this pack come from `external_bases`, which the caller built between passes by posting all `ref-delta` base ids to `/_do/push/lookup` in batches of 1,000 (2.4); ids the DO does not know must be in-pack (`by_id`) or the push fails with `missing base <oid>`. The first 32 MiB of external bases are prefetched with one coalesced `read_entries` (7.2), the rest are read on demand.
- **Everything at rest is a full object (2.1).** `decode_mini` produces inflated bytes; `PackWriter::append_entry` re-encodes them as `varint(kind,size) + zlib(data)`, so a reader needs one range read and no base. The object id is `compute_hash(kind, data)` over the resolved bytes, which is git's `"<type> <len>\0" + content` rule. A wrong delta or a corrupt base yields a wrong id that the tip check in `commit_push` (section 3) and `git fsck` on clone (scenario 2, 9) would expose, not a silently accepted object.
- **Ordering rule (section 3).** This module returns before anything is visible: `pending/` is scratch, `packs/<pack>.pack` is invisible until the `packs` row exists (inserted `ingesting` on the first `/_do/push/index` post), and `Index::lookup` joins on `state = 'live'`, which only `commit_push` step 3 sets. A crash anywhere in ingest leaves an `open` push that the Janitor expires and cleans (section 5).
- **Budget (section 7).** A push of N objects and P pack bytes costs about `P / 8 MiB` parts + `P / 8 MiB` window reads + `N / 10,000` index posts + `bases / 1,000` lookups + the coalesced base read, all charged through `budget.charge(1)`; 1,000,000 objects fit in 9,000 subrequests. Conformance scenarios this proof must pass: 2, 7, 8, 9, 11 and, for the empty-pack path, 4. Added scenario: "chain past the cache", a pushed blob with a 60-link delta chain where the cache is forced to 1 MiB, asserting one range read per link in `x-ge-subrequests` and a clean `fsck`.

## Changes from the first pass
| First-pass finding (quoted) | How addressed |
|---|---|
| Blocker: "Delete-only or pack-less pushes ... receivePack throws 'bad PACK header' and the client never gets report-status" | Not this module's decision any more: `edge::receive_pack` inspects `PktReader::remainder()` plus `fill(12)` and passes `pack_id: None` (2.4, sibling proof). `stream_to_pending` is only called when a pack is present, and a 12+20-byte 0-object pack passes its loop zero times. Scenario 4. |
| Blocker: "sha1() builds new Uint8Array([...hdr, ...body]) via array spread, ~10x heap blow-up" | `gix_object::compute_hash` hashes the loose header then the slice in place (no copy); the pack trailer hash is `Hasher::update` per chunk in `take`. No JS arrays exist. |
| Blocker: "DO flips refs without checking the new tip exists ... refs can point at deleted objects" | Out of this module by construction: `commit_push` re-checks each tip with `Index::lookup` in the same sync span and rejects on a `gc_epoch` change (section 3 steps 2 and 4, section 5). Ingest contributes the `links` set for the connectivity check (2.5) through `sink.links`. |
| Caveat: "inflateSync {info:true}.engine.bytesWritten verified on Node 22 but not on workerd's node:zlib port" | Replaced by `gix_zlib::Decompress::total_in` (pure Rust `zlib-rs`, no host API), `stream_to_pending` inner loop. The spike ran `gix_zlib::Inflate` on workerd. |
| Caveat: "Workers request body cap (100 MB Free/Pro, 200 MB Business, 500 MB Enterprise) bounds push size before CPU does" | Stated in 6.2 and in Known limits below; documented, not handled. |
| Caveat: "Ofs-delta whose base was parked under pending/ is stored with base '' and can never be resolved" | No parking exists. Every delta is resolved in pass B before the normalized pack is finished; `resolve_at` follows any chain back to a full entry, reading evicted links from `pending/` by their recorded offset. |
| Caveat: "Sequential per-object R2 put and one R2 GET per delta base ... needs bounded-concurrency puts and a per-push base cache" | One multipart upload for the pending pack and one for the normalized pack (8 MiB parts), never a per-object put (2.1, 6.4). Bases come from the 48 MiB cache; external bases are one coalesced `read_entries` (7.2). |
| Caveat: "Retry-from-start inflate costs up to 2x CPU on large objects; limits.cpu_ms must be raised" | The resumable `Decompress` never restarts; each byte is inflated once in pass A (boundary) and once in pass B (content). `limits.cpu_ms = 300000` is in `wrangler.jsonc` (section 7). |
| Caveat: "Pack trailer SHA-1 elided; crypto.DigestStream('SHA-1') solves it" | `gix_hash::Hasher` in `take`, compared against the 20 trailer bytes before `finish`; `DigestStream` is not needed. |
| Caveat: "pending/ sweep alarm is only set inside commitPush, so a push that crashes before the manifest leaves pending junk" | The Janitor is self-enqueued at `boot` and runs every 15 min (4.5, 5); `pending/<push>.pack` is deleted for every push not `open` and older than GRACE. This module never calls `set_alarm` (4.1). |
| Caveat: "Command-section parser and pack parser each call body.getReader(); over-read bytes past the flush pkt are lost" | One `BodyReader` per request (section 6); `parse_receive_header` leaves the PACK bytes in `remainder()`, which the caller pushes back into the same reader before `stream_to_pending` sees `buffered()`. |
| Review body: "Object sizes >= 2^31 corrupt the varint (<< is 32-bit)" | `PackEntry::from_bytes` decodes the size into `u64` with `checked_shl`/`checked_add` and returns `Overflow`; anything above 32 MiB is refused before inflating. |
| Contract 2.4: "`BytesToEntriesIter::new_from_header(reader, Mode::Verify, EntryDataMode::Ignore, Sha1)` over a `BufRead` adapter" | Not followed, honestly: the iterator is synchronous and reads an entire entry inside one `next()`; when the buffered window ends mid-entry the `BufRead` cannot await more body and the iterator returns an unrecoverable error (its hash state is private, so it cannot be restarted). Pass A uses `Entry::from_bytes` + `Decompress` + `Hasher` for the same `EntryRec` and trailer result. Write-back to 2.4 and 6.4 proposed. |
| Contract 1.4 signatures | Two additions proposed as write-backs: `resolve_and_normalize` takes `sink: &mut IndexSink` (it must post rows in batches because 2,000,000 `ObjRow`s do not fit in memory, and it must return links for 2.5), and `RawWriter` is the "`PackWriter`-style" multipart writer of 2.4 without header patching or trailer. `flush_if_full`/`read_range` take `&mut ReqBudget` as the sibling proofs already do (7.1). |

## Known limits
- 128 MB: the sum of the 2.4 worst cases (48 MB entries + 8 MiB window + 48 MiB cache + 32 MiB base + 32 MiB result + 8 MiB part) exceeds the isolate. Realistically the 2,000,000-entry vector and a 32 MiB object do not co-occur, but the proof does not enforce a joint bound; a day-1 test pushes 1,000,000 small objects and, separately, one 32 MiB blob with a delta, and measures. `by_id` (28 bytes per entry) is only built when the pack contains ref-deltas.
- Level-0 re-encoding of the base for every delta is a memcpy plus adler32 per delta application (`decode_mini`); for a 30 MiB base with many small deltas this is tens of ms each. The optimisation, a per-window `File` over the raw window bytes with a `cache::DecodeEntry` adapter so gix walks in-window chains without copies, is described and not written.
- A ref-delta whose in-pack base appears *later* in the pack (legal, never produced by git's pack-objects) fails with `missing base`; git's index-pack would defer it. Deferred resolution is a second loop over the failed entries, not written.
- CPU: inflate twice, level-0 deflate per delta, SHA-1 twice (pack trailer, object ids) over the whole push; on the paid plan with `cpu_ms = 300000` a 100 MB push is seconds. Not measured on a deployed Worker (platform-facts, still open). Cold start of the ingest crate is unmeasured beyond the spike's 605 KB / 20-30 ms.
- Subrequests: budgeted, not measured (#7). The 5 MiB minimum part size and equal-size rule of real R2 are unverified (#6); the 8 MiB part scheme is designed for them.
- Unverified at runtime: level-0 `gix_zlib::stream::deflate::Write` on wasm32; `RawWriter`/`PackWriter` multipart `complete` returning durability (trusted per section 3); `wasm-streams` gzip path (section 6). `extract_links` (commit parents and tree, tree entries, tag target via `gix_object::{CommitRefIter, TreeRefIter, TagRefIter}`) is ~12 lines, omitted.
- Request body cap by zone plan (100 MB Free/Pro) bounds a push before this code runs (6.2). Objects above 32 MiB inflated are refused; LFS is out of scope (section 12).

## Depends on
- info-refs-endpoint
- two-phase-push
- refs-sqlite-objects-r2
- repo-do-ref-authority
- gc-and-repack-alarm
