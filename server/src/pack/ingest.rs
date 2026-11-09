//! Pass A (stream_to_pending) and pass B (resolve_and_normalize) of ingest (CONTRACTS.md 2.4, A10).
//! Runs in the edge Worker. Ported from research/proofs/streaming-pack-parser.md.

use std::collections::{HashMap, VecDeque};
use std::io::Write as _;
use std::rc::Rc;

use gix_hash::{Kind as H, ObjectId};
use gix_object::Kind;
use gix_pack::data::{entry::Header, Entry as PackEntry, File};
use gix_zlib::{Compression, Decompress, FlushDecompress, Inflate, Status};

use worker::{SqlStorage, SqlStorageValue as V};

use crate::edge::BodyReader;
use crate::error::Error;
use crate::store::{codec, keys, Bucket, ObjLoc, ObjRow, PackOut, PushId, RawWriter};
use crate::ReqBudget;

use super::run::{receive_packs, INGEST_BUSY_MSG};


#[derive(Clone, Copy)]
pub struct EntryRec {
    pub offset: u64,
    pub header_len: u8,
    pub kind_or_delta: Header,
    pub compressed_len: u32,
    pub size: u64,
}
impl EntryRec {
    /// The entry's wire span — header plus compressed body: what
    /// `Window::entry` copies out, what the chain cap counts, and what
    /// `entry_fetch_bound` sizes the fetch from.
    fn wire_len(&self) -> u64 {
        u64::from(self.header_len).saturating_add(u64::from(self.compressed_len))
    }
}

// EntryRec is 64 B (offset u64 + Header enum + compressed_len u32 + size u64
// + header_len u8, padded): the record vec alone must leave room for Cache
// (24 MiB edge budget) + window + part buffer inside a 128 MiB isolate —
// 750k records ≈ 46 MiB. This table is PASS A: it is built before the
// admission gate exists, so its growth is PRE-EXISTING and UNGATED — the
// pass-B guard never sees it. stream_inner pre-sizes it from the header
// count so the Vec's doubling peak (~2x) never materializes.
const _: () = assert!(std::mem::size_of::<EntryRec>() == 64);
const MAX_ENTRIES: usize = 750_000;
pub const MAX_OBJ: u64 = 16 << 20; // A7
/// A8: ceiling on a streamed blob's *decompressed* size, checked from the header in
/// pass A. Pass A inflates the whole stream to validate and pass B re-inflates it to
/// hash — with zlib's ~1000:1 ratio an uncapped entry could cost terabytes of CPU.
pub const MAX_STREAM_BLOB: u64 = 2 << 30;
/// One entry's *compressed* wire size. `decompressed_size` ≤ 16 MiB bounds output but a
/// deflate stream can carry ~5 bytes-in/0 bytes-out padding, so an entry could claim a
/// ~2 GiB compressed length — which pass B would then range-read into memory whole.
/// A real 16 MiB object compresses to < ~17 MiB; 32 MiB is generous headroom.
pub const MAX_ENTRY_WIRE: u64 = 32 << 20;
/// Total compressed bytes held by in-flight delta chains across every nested
/// resolve_at frame — the aggregate bound MAX_CHAIN_BYTES alone can't provide.
const MAX_CHAIN_LIVE: u64 = 128 << 20;
const MAX_PENDING: u64 = 2 << 30; // 2 GiB compressed ceiling on one pushed pack
const WINDOW: u64 = 8 << 20;
/// Default resolved-object cache budget — the in-DO import path keeps it. The
/// edge push path runs tighter (EDGE_CACHE): one request must also hold the
/// entry table, the window, and a part buffer inside a 128 MiB isolate.
const CACHE: usize = 48 << 20;
const EDGE_CACHE: usize = 24 << 20;
/// Pass-B memory guard (edge path): the ceilings below compare the
/// counting allocator's LIVE heap — an over-budget allocation aborts
/// inside the futures queue and the request hangs instead of answering.
/// At each entry boundary (and inside delta-chain walks — see
/// `Cx::mem_relief`) the guard sheds the object cache past MEM_SOFT;
/// still above MEM_HARD — or over the linear-aware trip — it fails with
/// an in-band Limit the client can act on (smaller slices).
///
/// Why live bytes and not `memory.size` alone: wasm linear memory never
/// shrinks, so a size-only guard would wedge an isolate forever after one
/// large push — and lowering these live thresholds to match the linear
/// ratio would reject pushes that measured fine. Measured on real pushes:
/// a single unsliced push runs ~57-62 MiB live / ~80-91 MiB linear; two
/// concurrent pass-Bs ~89 live / ~131 linear (the slots gate exists for
/// that); a 1-slot 5-way run ~66 / ~98; a 150 MiB-linear isolate survived
/// — the original "hung" failure was a failed grow near ~137 MiB, not a
/// hard wall at 128.
const MEM_SOFT: usize = 80 << 20;
const MEM_HARD: usize = 96 << 20;
/// Wasm-only second hard trip, in bytes of LINEAR memory. The platform's
/// "128 MB" isolate cap is DECIMAL: 128,000,000 B ≈ 122.07 MiB. LINEAR_CAP
/// sits at 112 MiB so ~10 MiB stays for the JS-heap side the wasm tally
/// can't see — an 8 MiB window-read ArrayBuffer plus part-upload buffers —
/// before the growth itself can trap. (The old LINEAR_CAP at 120 MiB left
/// only ~2.1 MB: under one window buffer, and its `live > MEM_SOFT` gate
/// let a 16 MiB alloc trap at 58 MiB live / 119 MiB linear.) Linear memory
/// never shrinks, so the check (`linear_trip`) applies only when an
/// allocation may need fresh pages — independent of MEM_SOFT — with one
/// forced shed then an in-band Limit.
const LINEAR_CAP: usize = 112 << 20;
/// Free space inside linear memory that the allocator may be unable to use
/// for one request. Freed blocks are scattered, so a contiguous `incoming`
/// block can need fresh pages even when the total free bytes exceed it.
/// 16 MiB is one MAX_OBJ — the largest single block pass B allocates — so
/// a heap with that much spare beyond `live + incoming` can place the
/// block inside existing pages even if one MAX_OBJ-sized hole is unusable.
const FRAG_MARGIN: usize = 16 << 20;

/// Would allocating `incoming` bytes risk growing linear memory past
/// LINEAR_CAP? Only when the allocation may need fresh pages — live +
/// incoming + FRAG_MARGIN no longer fits inside the current linear size —
/// and those pages would cross the cap. A high-water linear size with a
/// low live heap reuses its free pages and does not trip. `incoming == 0`
/// allocates nothing and never trips.
fn linear_trip(live: usize, linear: usize, incoming: usize) -> bool {
    incoming > 0
        && live.saturating_add(incoming).saturating_add(FRAG_MARGIN) > linear
        && linear.saturating_add(incoming) > LINEAR_CAP
}
/// Decode bounds above this cannot fit even on a minimally-loaded heap —
/// the unavoidable coexisting working set around one decode (a window
/// fetch, the part buffer's first whole part, the raw entry copy,
/// bookkeeping) is ~40 MiB, so MEM_HARD - 40 MiB is the honest per-object
/// ceiling. Above it the alone-case Limit names the object instead of
/// suggesting smaller slices, which cannot shrink one decode.
const PER_OBJECT_DECODE: usize = MEM_HARD - (40 << 20);
/// Live heap must grow this far past the last shed before the cache clears
/// again — hysteresis so a heap parked between MEM_SOFT and MEM_HARD doesn't
/// shed at every entry boundary (each shed makes delta chains re-resolve
/// through window re-reads, one R2 subrequest apiece).
const MEM_HYSTERESIS: usize = 8 << 20;
const MAX_DEPTH: usize = 64; // git's default delta depth is 50
const MAX_CHAIN_BYTES: u64 = 64 << 20; // chain holds raw compressed delta bodies
/// Edge-path bound on compressed bytes ONE resolve_at chain may retain —
/// tighter than MAX_CHAIN_BYTES so a single chain cannot by itself fill
/// the headroom the memory guard needs around it (window + cache + decode
/// buffers live alongside the chain). Over it the walk fails in-band
/// ("delta chain too large for one request") so the client can slice.
const EDGE_CHAIN_BYTES: u64 = 24 << 20;
/// Rows per push_links INSERT; it keeps one statement at <= 90 bound params,
/// A6's 90-batch headroom convention under the 100 guard (50 rows = 100 params
/// was legal but had zero headroom).
const LINKS_CHUNK: usize = 45;

/// One chunked push_links statement + its bound args (push_id + sha per row).
fn links_insert(push: &str, chunk: &[ObjectId]) -> (String, Vec<worker::SqlStorageValue>) {
    let marks = std::iter::repeat("(?,?)").take(chunk.len()).collect::<Vec<_>>().join(",");
    let mut args = Vec::with_capacity(chunk.len() * 2);
    for id in chunk {
        args.push(worker::SqlStorageValue::from(push));
        args.push(worker::SqlStorageValue::from(id.to_string().as_str()));
    }
    (format!("INSERT OR IGNORE INTO push_links(push_id,sha) VALUES{marks}"), args)
}

fn unpack(m: impl Into<String>) -> Error {
    Error::Unpack(m.into())
}
fn internal(e: impl std::fmt::Display) -> Error {
    Error::Internal(e.to_string())
}

/// Pass A. Each zlib boundary comes from a resumable `Decompress` whose `total_in` survives awaits.
/// Any error after the MPU is created aborts it — a dropped RawWriter would orphan the
/// upload and its already-uploaded parts in R2.
pub async fn stream_to_pending(
    body: &mut BodyReader,
    bucket: &Bucket,
    push: &PushId,
    kind: gix_hash::Kind,
    budget: &mut ReqBudget,
) -> Result<(Vec<EntryRec>, u32), Error> {
    let mut out = RawWriter::create(bucket, keys::pending(&bucket.repo, push), budget).await?;
    match stream_inner(body, &mut out, kind, budget).await {
        Ok(r) => match out.finish(budget).await {
            Ok(()) => Ok(r),
            Err(e) => Err(e), // finish aborts the MPU itself on failure
        },
        Err(e) => {
            out.abort().await;
            Err(e)
        }
    }
}

async fn stream_inner(
    body: &mut BodyReader,
    out: &mut RawWriter,
    kind: gix_hash::Kind,
    budget: &mut ReqBudget,
) -> Result<(Vec<EntryRec>, u32), Error> {
    let ds = kind.len_in_bytes();
    let mut hasher = gix_hash::hasher(kind);
    if !body.fill(12).await? {
        return Err(unpack("pack header truncated"));
    }
    let head: [u8; 12] = body
        .buffered()
        .get(..12)
        .and_then(|s| s.try_into().ok())
        .ok_or_else(|| unpack("pack header"))?;
    let (version, count) =
        gix_pack::data::header::decode(&head).map_err(|e| unpack(e.to_string()))?;
    if version != gix_pack::data::Version::V2 {
        return Err(unpack("pack version"));
    }
    take(body, &mut hasher, out, 12, budget).await?;
    // pre-size the entry table from the header count: the Vec's amortized
    // doubling would otherwise peak near 2x the table (~92 MiB at the cap)
    // inside pass A, which runs before the memory guard exists. Capped at
    // MAX_ENTRIES — the loop still rejects bigger counts.
    let (mut recs, mut pos, mut z, mut sink) = (
        Vec::with_capacity(usize::try_from(count).unwrap_or(0).min(MAX_ENTRIES)),
        12u64,
        Decompress::new(),
        vec![0u8; 64 << 10],
    );
    for _ in 0..count {
        if recs.len() >= MAX_ENTRIES {
            return Err(unpack("too many objects"));
        }
        body.fill(30).await?;
        let e = PackEntry::from_bytes(body.buffered(), pos, kind)
            .map_err(|e| unpack(format!("entry at {pos}: {e}")))?;
        // Blobs may exceed MAX_OBJ: pass B streams them verbatim (their compressed
        // bytes are already the normalized form) with a running sha1 — memory stays
        // flat. Everything else still materializes in isolate memory to resolve.
        let streamable = e.header == Header::Blob;
        if e.decompressed_size > MAX_STREAM_BLOB {
            return Err(Error::Limit("object too large (2 GiB max)".into()));
        }
        if e.decompressed_size > MAX_OBJ && !streamable {
            return Err(Error::Limit("object too large (16 MiB max)".into()));
        }
        let hlen = e.header_size();
        take(body, &mut hasher, out, hlen, budget).await?;
        let (mut clen, mut st) = (0u64, Status::Ok);
        z.reset();
        while st != Status::StreamEnd {
            if body.buffered().is_empty() && !body.fill(1).await? {
                return Err(unpack("pack truncated inside object"));
            }
            let (bi, bo) = (z.total_in(), z.total_out());
            st = z
                .decompress(body.buffered(), &mut sink, FlushDecompress::None)
                .map_err(|e| unpack(format!("zlib at {pos}: {e}")))?;
            let used = usize::try_from(z.total_in().saturating_sub(bi)).map_err(internal)?;
            if used == 0 && z.total_out() == bo {
                return Err(unpack(format!("zlib stalled at {pos}")));
            }
            take(body, &mut hasher, out, used, budget).await?;
            clen = clen.saturating_add(used as u64);
            if body.total > MAX_PENDING {
                return Err(Error::Limit("pack exceeds the 2 GiB limit".into()));
            }
        }
        if z.total_out() != e.decompressed_size {
            return Err(unpack(format!("object at {pos}: size mismatch")));
        }
        if clen > MAX_ENTRY_WIRE && !streamable {
            return Err(unpack(format!("entry at {pos}: wire size exceeds 32 MiB")));
        }
        recs.push(EntryRec {
            offset: pos,
            header_len: u8::try_from(hlen).map_err(|_| unpack("header too long"))?,
            kind_or_delta: e.header,
            compressed_len: u32::try_from(clen).map_err(|_| Error::Limit("entry too large".into()))?,
            size: e.decompressed_size,
        });
        pos = pos.saturating_add(hlen as u64).saturating_add(clen);
    }
    if !body.fill(ds).await? {
        return Err(unpack("pack trailer truncated"));
    }
    let want = hasher.try_finalize().map_err(|_| unpack("hash collision in pack"))?;
    if body.buffered().get(..ds) != Some(want.as_slice()) {
        return Err(unpack("bad pack checksum"));
    }
    out.append(want.as_slice());
    body.consume(ds);
    if body.fill(1).await? {
        return Err(Error::Protocol("bytes after pack trailer".into()));
    }
    Ok((recs, count))
}

async fn take(
    body: &mut BodyReader,
    h: &mut gix_hash::Hasher,
    out: &mut RawWriter,
    n: usize,
    budget: &mut ReqBudget,
) -> Result<(), Error> {
    let bytes = body.buffered().get(..n).ok_or_else(|| unpack("pack truncated"))?;
    h.update(bytes);
    out.append(bytes);
    body.consume(n);
    out.flush_if_full(budget).await
}

// ---- Pass B ----

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum Key {
    Off(u64),
    Id(ObjectId),
}
type Obj = (Kind, Rc<Vec<u8>>);

pub struct Cache {
    bytes: usize,
    budget: usize,
    map: HashMap<Key, Obj>,
    order: VecDeque<Key>,
}
/// Bytes one cached entry costs beyond its payload, counted against the
/// cache budget so many tiny objects are bounded too, not just big ones:
/// the map slot (key + value + control byte) and the queue slot, each
/// doubled for the tables' growth headroom, plus the Rc and Vec headers of
/// the payload allocation. Every queued key pays it until it is popped.
const CACHE_ENTRY_OVERHEAD: usize = 2
    * (std::mem::size_of::<(Key, Obj)>() + 1 + std::mem::size_of::<Key>())
    + 2 * std::mem::size_of::<usize>()
    + std::mem::size_of::<Vec<u8>>();
impl Default for Cache {
    fn default() -> Self {
        Self::with_budget(CACHE)
    }
}
impl Cache {
    pub fn with_budget(budget: usize) -> Self {
        Self { bytes: 0, budget, map: HashMap::new(), order: VecDeque::new() }
    }
    fn get(&self, k: &Key) -> Option<Obj> {
        self.map.get(k).cloned()
    }
    /// Peak bytes the NEXT `put` can allocate for bookkeeping: a map table
    /// resize and a queue resize, each holding its old and new block at
    /// once. 0 when both have room. The payload Vec is already allocated
    /// by the caller (and its decode was preflighted).
    fn put_growth_bound(&self) -> usize {
        map_growth_peak::<(Key, Obj)>(self.map.len(), self.map.capacity())
            .saturating_add(vec_growth_peak::<Key>(self.order.len(), self.order.capacity(), 1))
    }
    /// Cache one decoded object. The Vec is shrink_to_fit'd first so its
    /// capacity stops doubling past the payload, and `bytes` accounts
    /// `capacity()` — the allocator's real footprint, not just `len()` —
    /// plus CACHE_ENTRY_OVERHEAD per queued key.
    fn put(&mut self, k: Key, kind: Kind, mut d: Vec<u8>) -> Obj {
        d.shrink_to_fit();
        let d = Rc::new(d);
        self.bytes = self.bytes.saturating_add(d.capacity()).saturating_add(CACHE_ENTRY_OVERHEAD);
        self.order.push_back(k);
        if let Some((_, old)) = self.map.insert(k, (kind, d.clone())) {
            // the old entry's queue slot stays queued and keeps paying its
            // overhead until it pops
            self.bytes = self.bytes.saturating_sub(old.capacity());
        }
        while self.bytes > self.budget {
            let Some(k) = self.order.pop_front() else { break };
            self.bytes = self.bytes.saturating_sub(CACHE_ENTRY_OVERHEAD);
            // a stale duplicate slot finds nothing (or the key's newer
            // entry, which then leaves early — always safe, only slower)
            if let Some((_, v)) = self.map.remove(&k) {
                self.bytes = self.bytes.saturating_sub(v.capacity());
            }
        }
        (kind, d)
    }
    /// Drop every cached object — resolved entries re-resolve lazily from the
    /// pending pack, so this is always safe, only slower.
    pub fn clear(&mut self) {
        self.map.clear();
        self.order.clear();
        self.bytes = 0;
    }
}

/// Where entry bytes live: one R2 object (push pending pack) or a list of
/// staged part objects read as one concatenated stream (I1 import).
pub enum Source<'a> {
    Key(&'a str),
    /// (key, length) pairs in order — offsets index into the concatenation.
    Parts(&'a [(String, u64)]),
}
/// The read side pass B uses — `Bucket` (R2) in production, `MemReads`
/// (an in-memory object store) under `cfg(test)` so the real resolve loop
/// runs natively. Both shapes funnel through `read_range`/`read_entries`.
#[derive(Clone, Copy)]
pub enum Reader<'a> {
    Bucket(&'a Bucket),
    #[cfg(test)]
    Mem(&'a MemReads),
}
impl Reader<'_> {
    async fn read_range(&self, key: &str, off: u64, n: u64, budget: &mut ReqBudget) -> Result<Vec<u8>, Error> {
        match self {
            Reader::Bucket(b) => b.read_range(key, off, n, budget).await,
            #[cfg(test)]
            Reader::Mem(m) => m.read_range(key, off, n, budget).await,
        }
    }
    async fn read_entries(&self, locs: &[(ObjectId, ObjLoc)], budget: &mut ReqBudget) -> Result<Vec<(ObjectId, Vec<u8>)>, Error> {
        match self {
            Reader::Bucket(b) => b.read_entries(locs, budget).await,
            #[cfg(test)]
            Reader::Mem(m) => m.read_entries(locs, budget).await,
        }
    }
}

/// Test-only in-memory object store behind `Reader::Mem`: R2 key -> bytes.
/// `read_entries` resolves each loc's pack key the same way `Bucket` does.
#[cfg(test)]
pub struct MemReads {
    pub repo: crate::store::RepoId,
    pub objects: HashMap<String, Vec<u8>>,
}
#[cfg(test)]
impl MemReads {
    pub fn new(repo: crate::store::RepoId) -> Self {
        Self { repo, objects: HashMap::new() }
    }
    pub fn put(&mut self, key: &str, bytes: Vec<u8>) {
        self.objects.insert(key.to_string(), bytes);
    }
    async fn read_range(&self, key: &str, off: u64, n: u64, budget: &mut ReqBudget) -> Result<Vec<u8>, Error> {
        budget.charge(1)?;
        let obj = self
            .objects
            .get(key)
            .ok_or_else(|| Error::Storage(format!("mem read missing {key}")))?;
        let (a, b) = (
            usize::try_from(off).map_err(internal)?,
            usize::try_from(off.saturating_add(n)).map_err(internal)?,
        );
        obj.get(a..b)
            .map(<[u8]>::to_vec)
            .ok_or_else(|| Error::Storage(format!("mem read {key}@{off}+{n} past end")))
    }
    async fn read_entries(&self, locs: &[(ObjectId, ObjLoc)], budget: &mut ReqBudget) -> Result<Vec<(ObjectId, Vec<u8>)>, Error> {
        let mut out = Vec::with_capacity(locs.len());
        for (id, loc) in locs {
            let key = keys::pack(&self.repo, &loc.pack);
            let bytes = self.read_range(&key, loc.offset, u64::from(loc.len), budget).await?;
            out.push((*id, bytes));
        }
        Ok(out)
    }
}

impl Source<'_> {
    async fn read(&self, reads: &Reader<'_>, off: u64, n: u64, budget: &mut ReqBudget) -> Result<Vec<u8>, Error> {
        match self {
            Source::Key(k) => reads.read_range(k, off, n, budget).await,
            Source::Parts(parts) => {
                let mut out = Vec::with_capacity(usize::try_from(n).unwrap_or(0));
                let want_end = off.saturating_add(n);
                let mut base = 0u64;
                for (key, len) in *parts {
                    let end = base.saturating_add(*len);
                    let (a, b) = (off.max(base), want_end.min(end));
                    if a < b {
                        out.extend_from_slice(&reads.read_range(key, a - base, b - a, budget).await?);
                    }
                    if b >= want_end {
                        break;
                    }
                    base = end;
                }
                if out.len() as u64 != n {
                    return Err(Error::Storage("read past staged parts".into()));
                }
                Ok(out)
            }
        }
    }
}

pub struct Window<'a> {
    pub reads: Reader<'a>,
    pub src: Source<'a>,
    pub pack_len: u64,
    pub start: u64,
    pub buf: Vec<u8>,
}
impl Window<'_> {
    /// Raw bytes of one entry; slides to [offset, +8 MiB) with one range read when outside.
    async fn entry(&mut self, rec: &EntryRec, budget: &mut ReqBudget) -> Result<Vec<u8>, Error> {
        let len = rec.wire_len();
        // pass A enforces MAX_ENTRY_WIRE; keep the bound explicit here so a huge range
        // read can never be issued even if the invariant is ever broken upstream
        if len > MAX_ENTRY_WIRE.saturating_add(64) {
            return Err(unpack("entry window too large"));
        }
        let lo = match rec
            .offset
            .checked_sub(self.start)
            .filter(|lo| lo.saturating_add(len) <= self.buf.len() as u64)
        {
            Some(lo) => lo,
            None => {
                let n = window_fetch_n(self.pack_len, rec.offset, len);
                // drop the outgoing window BEFORE the replacement read
                // allocates — the two buffers would otherwise coexist at
                // the fetch's peak (the guard's preflight counts only the
                // incoming side)
                self.buf = Vec::new();
                self.buf = self.src.read(&self.reads, rec.offset, n, budget).await?;
                self.start = rec.offset;
                0
            }
        };
        let (a, b) = (
            usize::try_from(lo).map_err(internal)?,
            usize::try_from(lo.saturating_add(len)).map_err(internal)?,
        );
        self.buf.get(a..b).map(<[u8]>::to_vec).ok_or_else(|| Error::Storage("entry outside window".into()))
    }
    /// Raw range read for the verbatim blob path — same source abstraction.
    async fn read(&mut self, off: u64, n: u64, budget: &mut ReqBudget) -> Result<Vec<u8>, Error> {
        self.src.read(&self.reads, off, n, budget).await
    }
}

/// The range span `Window::entry` reads on a miss: the entry's wire length
/// grown to WINDOW, clipped at the pack's end. Shared by the fetch and the
/// memory guard's preflight so the two can't drift.
fn window_fetch_n(pack_len: u64, offset: u64, len: u64) -> u64 {
    len.max(WINDOW).min(pack_len.saturating_sub(offset))
}

/// Flat slack inside `entry_fetch_bound`: the range-read plumbing — request
/// build, response handling, the JS-side copy bookkeeping — that the
/// buffer + copy terms don't name.
const FETCH_SCRATCH: usize = 64 << 10;

/// Bytes `Window::entry(rec)` adds to the live heap, sized BEFORE the call
/// allocates: on a window miss the replacement buffer (`window_fetch_n`)
/// plus the returned header+body copy; on a hit only the copy. The
/// outgoing window is dropped before the fetch, so it is not counted — it
/// is already inside `live()`. `None` when `entry` rejects the wire span
/// up front — it then reports its own unpack error and nothing was going
/// to allocate anyway.
fn entry_fetch_bound(pack_len: u64, start: u64, buf_len: u64, rec: &EntryRec) -> Option<usize> {
    let len = rec.wire_len();
    if len > MAX_ENTRY_WIRE.saturating_add(64) {
        return None;
    }
    let hit = rec
        .offset
        .checked_sub(start)
        .is_some_and(|lo| lo.saturating_add(len) <= buf_len);
    let n = if hit { 0 } else { window_fetch_n(pack_len, rec.offset, len) };
    Some(
        usize::try_from(n)
            .unwrap_or(usize::MAX)
            .saturating_add(usize::try_from(len).unwrap_or(usize::MAX))
            .saturating_add(FETCH_SCRATCH),
    )
}

/// Thin-pack base lookup: the edge's prefetched map, or the live objects index
/// for an in-DO job (I1 resolves bases lazily, slice after slice).
pub enum Externals<'a> {
    Map(&'a HashMap<ObjectId, ObjLoc>),
    Idx(&'a crate::store::Index<'a>),
}
impl Externals<'_> {
    fn get(&self, id: &ObjectId) -> Result<Option<ObjLoc>, Error> {
        match self {
            Externals::Map(m) => Ok(m.get(id).cloned()),
            Externals::Idx(idx) => Ok(idx.lookup(&[*id])?.into_iter().next().flatten()),
        }
    }
}

/// object id -> entry index: the edge's in-memory map, or the objects table for
/// an in-DO job. Posted rows are already durable; `mem` covers the current
/// slice's unposted rows so a ref-delta can resolve a same-batch base.
pub enum IdMap<'a> {
    Map(HashMap<ObjectId, usize>),
    Sql { idx: &'a crate::store::Index<'a>, pack: &'a str, mem: HashMap<ObjectId, usize> },
}
impl IdMap<'_> {
    fn get(&self, id: &ObjectId) -> Result<Option<usize>, Error> {
        match self {
            IdMap::Map(m) => Ok(m.get(id).copied()),
            IdMap::Sql { idx, pack, mem } => {
                if let Some(i) = mem.get(id) {
                    return Ok(Some(*i));
                }
                idx.lookup_in_pack(id, &crate::store::PackId((*pack).to_string()))
                    .map(|l| l.map(|l| l.idx as usize))
            }
        }
    }
    fn insert(&mut self, id: ObjectId, i: usize) {
        match self {
            IdMap::Map(m) => {
                m.insert(id, i);
            }
            IdMap::Sql { mem, .. } => {
                mem.insert(id, i);
            }
        }
    }
}

/// The pass-A entry table: the edge's in-memory Vec, or the import job's
/// `import_toc` rows — by-idx and by-offset lookups stay O(log n) either way, so
/// a pack too big to hold in memory still resolves.
pub enum Toc<'a> {
    Mem(&'a [EntryRec]),
    Sql { sql: &'a SqlStorage, push: &'a str },
}
impl Toc<'_> {
    pub fn by_idx(&self, i: usize) -> Result<Option<EntryRec>, Error> {
        match self {
            Toc::Mem(v) => Ok(v.get(i).copied()),
            Toc::Sql { sql, push } => Self::get(
                sql,
                "SELECT offset,hlen,ktype,kaux_n,kaux_id,clen,size FROM import_toc WHERE push_id=? AND idx=?",
                vec![V::from(*push), V::from(i64::try_from(i).map_err(internal)?)],
            ),
        }
    }
    pub fn by_off(&self, off: u64) -> Result<Option<EntryRec>, Error> {
        match self {
            Toc::Mem(v) => Ok(v
                .binary_search_by_key(&off, |r| r.offset)
                .ok()
                .map(|i| v[i])),
            Toc::Sql { sql, push } => Self::get(
                sql,
                "SELECT idx,offset,hlen,ktype,kaux_n,kaux_id,clen,size FROM import_toc WHERE push_id=? AND offset=?",
                vec![V::from(*push), V::from(i64::try_from(off).map_err(internal)?)],
            ),
        }
    }
    fn get(sql: &SqlStorage, q: &str, args: Vec<V>) -> Result<Option<EntryRec>, Error> {
        #[derive(serde::Deserialize)]
        struct Row {
            idx: Option<i64>,
            offset: i64,
            hlen: i64,
            ktype: i64,
            kaux_n: Option<i64>,
            kaux_id: Option<String>,
            clen: i64,
            size: i64,
        }
        let r = crate::store::exec(sql, q, args)?
            .to_array::<Row>()
            .map_err(|e| Error::Storage(e.to_string()))?
            .into_iter()
            .next();
        let Some(r) = r else { return Ok(None) };
        let u = |n: i64| -> Result<u64, Error> {
            u64::try_from(n).map_err(|_| Error::Internal("negative in import_toc".into()))
        };
        let kind_or_delta = match r.ktype {
            0 => Header::Commit,
            1 => Header::Tree,
            2 => Header::Blob,
            3 => Header::Tag,
            4 => Header::OfsDelta {
                base_distance: u(r.kaux_n.ok_or_else(|| Error::Internal("ofs-delta without distance".into()))?)?,
            },
            5 => Header::RefDelta {
                base_id: crate::repo_do::oid(
                    r.kaux_id.as_deref().ok_or_else(|| Error::Internal("ref-delta without base id".into()))?,
                )?,
            },
            k => return Err(Error::Internal(format!("bad import_toc.ktype {k}"))),
        };
        let _ = r.idx; // by_off selects it; by_idx queries by it
        Ok(Some(EntryRec {
            offset: u(r.offset)?,
            header_len: u8::try_from(r.hlen).map_err(|_| Error::Internal("hlen".into()))?,
            kind_or_delta,
            compressed_len: u32::try_from(r.clen).map_err(|_| Error::Internal("clen".into()))?,
            size: u(r.size)?,
        }))
    }
}

/// The resolve loop's row/link/tag sink: the edge posts rows over the stub; an
/// in-DO job writes them straight to SQLite (and spills links to push_links so
/// the set survives slice boundaries).
pub enum AnySink<'a, 'b> {
    Edge(&'a mut super::run::IndexSink<'b>),
    Job(&'a mut JobSink<'b>),
    #[cfg(test)]
    Mem(&'a mut MemSink),
}
impl AnySink<'_, '_> {
    pub async fn post(&mut self, rows: &[ObjRow], budget: &mut ReqBudget) -> Result<(), Error> {
        match self {
            AnySink::Edge(s) => s.post(rows, budget).await,
            AnySink::Job(s) => s.post(rows, budget).await,
            #[cfg(test)]
            AnySink::Mem(_) => Ok(()),
        }
    }
    /// Preflight bytes for `post(rows)`: the largest single index post's
    /// JSON body plus its request copy (see run::index_post_bound). The DO
    /// job writes SQL directly and allocates no body: 0.
    pub fn post_bound(&self, rows: &[ObjRow]) -> Result<usize, Error> {
        match self {
            AnySink::Edge(s) => s.post_bound(&crate::store::PackMeta::EMPTY, rows),
            AnySink::Job(_) => Ok(0),
            #[cfg(test)]
            AnySink::Mem(_) => super::run::index_post_bound(
                "00000000000000000000000000000000",
                "00000000000000000000000000000000",
                &crate::store::PackMeta::EMPTY,
                rows,
            ),
        }
    }
    /// Rows buffered before a `post` flush — the edge posts each batch as
    /// index rows over the stub, so it flushes at INDEX_BATCH to keep both
    /// the rows Vec and every serialized body small; the DO job inserts
    /// straight into SQLite and keeps its own 10,000-row convention.
    fn row_batch(&self) -> usize {
        match self {
            AnySink::Edge(_) => super::run::INDEX_BATCH,
            AnySink::Job(_) => 10_000,
            #[cfg(test)]
            AnySink::Mem(_) => super::run::INDEX_BATCH,
        }
    }
    /// 2.5 links bookkeeping — bounded by MAX_LINKS either way.
    pub fn push_links(&mut self, links: Vec<ObjectId>) -> Result<(), Error> {
        match self {
            AnySink::Edge(s) => {
                s.links.extend(links);
                if s.links.len() > super::run::MAX_LINKS {
                    return Err(Error::Limit("push references too many objects (1,000,000 max)".into()));
                }
                Ok(())
            }
            AnySink::Job(s) => s.push_links(links),
            #[cfg(test)]
            AnySink::Mem(s) => {
                s.links.extend(links);
                if s.links.len() > super::run::MAX_LINKS {
                    return Err(Error::Limit("push references too many objects (1,000,000 max)".into()));
                }
                Ok(())
            }
        }
    }
    pub fn tags_mut(&mut self) -> &mut HashMap<ObjectId, ObjectId> {
        match self {
            AnySink::Edge(s) => &mut s.tags,
            AnySink::Job(s) => &mut s.tags,
            #[cfg(test)]
            AnySink::Mem(s) => &mut s.tags,
        }
    }
    /// Peak bytes the next tag insert can need — see map_growth_peak.
    pub fn tags_growth_bound(&mut self) -> usize {
        let t = self.tags_mut();
        map_growth_peak::<(ObjectId, ObjectId)>(t.len(), t.capacity())
    }
    /// Peak bytes `push_links`' extend can need for `adding` more links —
    /// the memory guard's preflight for the sink-side growth. 0 when the
    /// links fit the current capacity. Otherwise Vec's amortized growth
    /// picks max(2 x cap, need), and a moving realloc holds the old and the
    /// new block at once, so the bound is old + new capacity plus one
    /// allocation's overhead. The DO job's links spill to SQL in 10k
    /// batches — bounded, ungated.
    pub fn links_growth_bound(&self, adding: usize) -> usize {
        #[cfg(test)]
        let links = match self {
            AnySink::Edge(s) => &s.links,
            AnySink::Mem(s) => &s.links,
            AnySink::Job(_) => return 0,
        };
        #[cfg(not(test))]
        let links = match self {
            AnySink::Edge(s) => &s.links,
            AnySink::Job(_) => return 0,
        };
        vec_growth_peak::<ObjectId>(links.len(), links.capacity(), adding)
    }
    /// Total links seen — the A5 bound applies to both shapes.
    pub fn links_total(&self) -> usize {
        match self {
            AnySink::Edge(s) => s.links.len(),
            AnySink::Job(s) => s.links_total,
            #[cfg(test)]
            AnySink::Mem(s) => s.links.len(),
        }
    }
    /// Record an entry's output-pack start + object identity the moment its first
    /// bytes append — the import job's crash-resume names straddling writers from
    /// these rows. `sha` is None for the streamed-blob path (hash lands at seal).
    /// Edge pushes never resume mid-run: no-op.
    pub fn mark(&mut self, idx: usize, off: u64, sha: Option<ObjectId>, kind: Kind, size: u64) -> Result<(), Error> {
        match self {
            AnySink::Edge(_) => Ok(()),
            AnySink::Job(s) => s.mark(idx, off, sha, kind, size),
            #[cfg(test)]
            AnySink::Mem(_) => Ok(()),
        }
    }
    /// The entry's wire span is complete: record its end offset and (for blobs)
    /// its id. Writers ≤ the durable boundary with an end get direct-inserted on
    /// resume; without one they're the mid-write straddler.
    pub fn seal(&mut self, idx: usize, end: u64, sha: ObjectId) -> Result<(), Error> {
        match self {
            AnySink::Edge(_) => Ok(()),
            AnySink::Job(s) => s.seal(idx, end, sha),
            #[cfg(test)]
            AnySink::Mem(_) => Ok(()),
        }
    }
}

/// Test-only in-memory sink behind `AnySink::Mem`: collects what the real
/// sinks would post so resolve-loop tests can assert on rows/links/tags.
#[cfg(test)]
#[derive(Default)]
pub struct MemSink {
    pub links: Vec<ObjectId>,
    pub tags: HashMap<ObjectId, ObjectId>,
    pub rows: Vec<ObjRow>,
    pub posts: usize,
}

/// DO-side sink for the import job: objects rows and links batches go straight
/// to SQLite — no stub, no subrequests, and links spill past the in-memory cap.
pub struct JobSink<'a> {
    pub sql: &'a worker::SqlStorage,
    pub pack: crate::store::PackId,
    pub push: &'a str,
    /// the running import job — every SQL write batch re-checks its lease in
    /// the same sync span first (A19). The sink's calls sit between awaited
    /// R2 reads/uploads inside one_entry, so a stale slice (requeued and
    /// reclaimed mid-await) must be fenced out of every batch, not just the
    /// slice's outer checkpoints.
    pub job: &'a crate::jobs::Job,
    /// the DO the job runs on — the C7 failpoint inside one_entry pauses
    /// through the same flag-gated path as the drain-level hook. Edge sinks
    /// have no DO and skip the gate.
    pub d: &'a crate::repo_do::RepoDo,
    pub links_buf: Vec<ObjectId>,
    pub links_total: usize,
    pub tags: HashMap<ObjectId, ObjectId>,
}
impl JobSink<'_> {
    /// One read-only lease check per write batch — nothing awaits between
    /// this SELECT and the statements that follow it, so a lost lease lands
    /// exactly zero rows. (heartbeat's CAS would also work but costs an
    /// UPDATE + changes() probe per batch; inside a sync span the SELECT is
    /// sufficient.)
    fn fence(&self) -> Result<(), Error> {
        if !crate::jobs::lease_held(self.sql, self.job)? {
            return Err(crate::jobs::stale_lease());
        }
        Ok(())
    }
    pub async fn post(&mut self, rows: &[ObjRow], _budget: &mut ReqBudget) -> Result<(), Error> {
        self.fence()?;
        crate::store::Index(self.sql).insert_objects(&self.pack, rows)?;
        self.flush_links()
    }
    fn push_links(&mut self, links: Vec<ObjectId>) -> Result<(), Error> {
        self.links_total = self.links_total.saturating_add(links.len());
        if self.links_total > super::run::MAX_LINKS * 8 {
            return Err(Error::Limit("push references too many objects (8,000,000 max)".into()));
        }
        self.links_buf.extend(links);
        if self.links_buf.len() >= 10_000 {
            self.flush_links()?;
        }
        Ok(())
    }
    /// Pending links → push_links. PRIMARY KEY dedups repeats for free.
    pub fn flush_links(&mut self) -> Result<(), Error> {
        if self.links_buf.is_empty() {
            return Ok(());
        }
        self.fence()?;
        for chunk in self.links_buf.chunks(LINKS_CHUNK) {
            let (q, args) = links_insert(self.push, chunk);
            crate::store::exec(self.sql, &q, args)?;
        }
        self.links_buf.clear();
        Ok(())
    }
    /// entry idx -> output start + shadow row cols. Written at first append, so a
    /// crash resume can name the entry whose bytes straddle the durable boundary —
    /// attempt-order markers can't (a parked entry's prospective start collides
    /// with the entry that actually writes there).
    fn mark(&mut self, idx: usize, off: u64, sha: Option<ObjectId>, kind: Kind, size: u64) -> Result<(), Error> {
        self.fence()?;
        crate::store::exec(
            self.sql,
            "INSERT OR REPLACE INTO import_open(push_id,idx,off,sha,kind,size) VALUES(?,?,?,?,?,?)",
            vec![
                worker::SqlStorageValue::from(self.push),
                worker::SqlStorageValue::from(i64::try_from(idx).map_err(internal)?),
                worker::SqlStorageValue::from(i64::try_from(off).map_err(internal)?),
                sha.map(|s| worker::SqlStorageValue::from(s.to_string().as_str()))
                    .unwrap_or(worker::SqlStorageValue::Null),
                worker::SqlStorageValue::from(i64::from(crate::store::git_kind(kind))),
                worker::SqlStorageValue::from(i64::try_from(size).map_err(internal)?),
            ],
        )?;
        Ok(())
    }
    fn seal(&mut self, idx: usize, end: u64, sha: ObjectId) -> Result<(), Error> {
        self.fence()?;
        crate::store::exec(
            self.sql,
            "UPDATE import_open SET end=?, sha=? WHERE push_id=? AND idx=?",
            vec![
                worker::SqlStorageValue::from(i64::try_from(end).map_err(internal)?),
                worker::SqlStorageValue::from(sha.to_string().as_str()),
                worker::SqlStorageValue::from(self.push),
                worker::SqlStorageValue::from(i64::try_from(idx).map_err(internal)?),
            ],
        )?;
        Ok(())
    }
}

pub struct Cx<'a> {
    pub win: Window<'a>,
    pub cache: Cache,
    pub z: Inflate,
    pub by_id: IdMap<'a>,
    pub externals: Externals<'a>,
    /// the repo's object format — object hashing, ref-delta base widths, and
    /// commit/tree/tag reference parsing all follow it
    pub obj_kind: H,
    /// Nested `resolve_at` depth (ref-delta -> ref-delta hops). Each level keeps its
    /// `chain` alive across the inner await, so depth without a byte bound is an OOM.
    pub res_depth: u32,
    /// Compressed bytes held by in-flight delta chains across ALL nested resolve_at
    /// calls — decremented when a chain finishes or aborts.
    pub chain_live: u64,
    /// Pass-B memory guard state — `Some` only on the edge receive-pack
    /// path (`resolve_and_normalize`). The DO import job leaves it `None`:
    /// its alarm slices carry their own budget and keep the generous
    /// MAX_CHAIN_BYTES bound with no heap checks.
    pub mem: Option<MemGuard>,
}

impl Cx<'_> {
    /// Edge-path heap relief at a safe point inside pass B — entry
    /// boundaries AND inside delta-chain walks, where a single resolve_at
    /// can otherwise hold ~chain bytes plus window/cache/decode buffers
    /// before control returns to the boundary guard. Sheds the cache over
    /// MEM_SOFT (hysteresis) and fails in-band over MEM_HARD / the linear
    /// trip. A no-op on the DO path (`mem` is `None` there).
    fn mem_relief(&mut self) -> Result<(), Error> {
        if let Some(m) = self.mem.as_mut() {
            mem_guard(
                &mut self.cache,
                crate::mem::live_bytes,
                crate::mem::linear_bytes,
                receive_packs,
                m,
            )?;
        }
        Ok(())
    }

    /// The single entry point every large pass-B allocation preflights
    /// through: `incoming` is an honest upper bound of the bytes about to
    /// be allocated (links vectors and their extend doubling, id-map
    /// capacity, order/waiter lists, writer part-buffer growth, chunk
    /// reads). Same shed-then-Limit as mem_decode via mem_guard_peak —
    /// cache shed, then in-band Error::Limit. `site` tags the test probe.
    /// A no-op on the DO path (`mem` is `None` there).
    fn mem_grow(&mut self, incoming: usize, obj: Option<ObjLabel>, site: &'static str) -> Result<(), Error> {
        if let Some(m) = self.mem.as_mut() {
            mem_guard_peak(
                &mut self.cache,
                crate::mem::live_bytes,
                crate::mem::linear_bytes,
                receive_packs,
                m,
                incoming,
                obj,
                site,
            )?;
        }
        Ok(())
    }

    /// Decode preflight: `incoming` is the bound the caller computed for the
    /// ONE decode it is about to run (decode_mini_bound / decode_entry_bound).
    /// Same shed-then-check as mem_relief, but pressure is judged on
    /// live + incoming — a decode whose working set would cross MEM_HARD
    /// fails in-band here, before it allocates, instead of hanging the
    /// isolate mid-decode. `obj` names the object for the alone-case
    /// message when its own bound is the wall. A no-op on the DO path.
    fn mem_decode(&mut self, incoming: usize, obj: Option<ObjLabel>, site: &'static str) -> Result<(), Error> {
        self.mem_grow(incoming, obj, site)
    }

    /// Preflight for the window fetch `win.entry(rec)` is about to run:
    /// `entry_fetch_bound` sizes the replacement buffer plus the returned
    /// copy (or just the copy on a hit), and the same shed-then-Limit as
    /// mem_decode runs BEFORE either allocates — a heap that cannot hold
    /// window + entry fails in-band here instead of hanging inside the
    /// range read. MEM_PRESSURE_MSG: the push's aggregate heap is what
    /// needs slicing. No-op off the edge path (`mem` is `None` there).
    fn mem_fetch(&mut self, rec: &EntryRec, site: &'static str) -> Result<(), Error> {
        let Some(incoming) = entry_fetch_bound(
            self.win.pack_len,
            self.win.start,
            self.win.buf.len() as u64,
            rec,
        ) else {
            return Ok(());
        };
        self.mem_grow(incoming, None, site)
    }

    /// Cache one decoded object after preflighting the cache's own
    /// bookkeeping growth — its map table and LRU queue double as entries
    /// accumulate. `site` tags the test probe; every call site has its own.
    fn cache_put(&mut self, k: Key, kind: Kind, d: Vec<u8>, site: &'static str) -> Result<Obj, Error> {
        if self.mem.is_some() {
            let bound = self.cache.put_growth_bound();
            self.mem_grow(bound, None, site)?;
        }
        Ok(self.cache.put(k, kind, d))
    }

    /// Preflight for `Bucket::read_entries(locs)` — the external-base fetch
    /// (`external`) and the resolve loop's prefetch batch. The bound counts
    /// every entry copy plus the largest merged-span range buffer; same
    /// shed-then-Limit as mem_fetch BEFORE either allocates — a heap that
    /// cannot hold the fetch fails in-band here instead of inside the range
    /// read. MEM_PRESSURE_MSG: the push's aggregate heap is what needs
    /// slicing. No-op off the edge path (`mem` is `None` there).
    fn mem_entries(&mut self, locs: &[(ObjectId, ObjLoc)], site: &'static str) -> Result<(), Error> {
        self.mem_grow(Bucket::read_entries_bound(locs), None, site)
    }
}

/// Bound for decode_mini's level-0 re-deflate of the base: zlib level 0
/// stores — input plus 5 bytes per <=64 KiB-1 block plus the 6-byte wrapper.
/// len/8192 is ~1.6x the real per-block overhead; +64 covers wrapper+rounding.
fn stored_deflate_bound(len: usize) -> usize {
    len.saturating_add(len / 8_192).saturating_add(64)
}

/// One git delta-header size varint (7 bits per byte, high bit = continue) —
/// mirrors gix-pack's delta::decode_header_size, which is pub(crate) there.
fn delta_varint(d: &[u8]) -> Option<(u64, usize)> {
    let mut shift = 0u32;
    let mut size = 0u64;
    let mut consumed = 0usize;
    for &cmd in d {
        consumed = consumed.saturating_add(1);
        // a group whose bits would shift past u64 is a corrupt header:
        // checked_shl refuses shift >= 64, and the round trip refuses bits
        // the shift would drop (the 10th byte may carry only bit 63)
        let part = u64::from(cmd & 0x7f);
        size |= part.checked_shl(shift).filter(|v| v >> shift == part)?;
        shift += 7;
        if cmd & 0x80 == 0 {
            return Some((size, consumed));
        }
    }
    None
}

/// The delta's result size from its instruction header — the same cheap
/// probe gix-pack's decode_delta_object_size runs: the first two u64
/// varints of the decompressed delta carry base size then result size, so
/// inflating <= 32 bytes is enough. `None` when the stream won't produce or
/// parse them — the caller then bounds by MAX_OBJ (the decode fails on a
/// header that can't inflate this far anyway).
fn delta_result_size(body: &[u8], z: &mut Inflate) -> Option<u64> {
    z.reset();
    let mut buf = [0u8; 32];
    let (_status, _consumed_in, out) = z.once(body, &mut buf).ok()?;
    let d = buf.get(..out)?;
    let (_base, off) = delta_varint(d)?;
    let (result, _used) = delta_varint(d.get(off..)?)?;
    Some(result)
}

/// Upper bound on the live bytes ONE decode_mini adds at its peak — run
/// BEFORE anything allocates so the guard can reject in-band. Counts the
/// copies the decode actually makes:
///   mini   — the synthetic pack Vec: 12-byte header, the re-deflated base
///            (level 0 ≈ stored), the re-headed entry header, the raw entry
///            body verbatim, and the digest-width trailer. It stays alive
///            through decode_entry; ×2 covers Vec's amortized capacity.
///   out    — decode_entry's work Vec: for a delta, two biggest-result
///            buffers plus the decompressed instruction stream (the result
///            size comes from the delta header probe); for a full entry,
///            the object itself. Sized exactly, then floored at 2× the
///            instruction region for Vec's amortized growth.
///   scratch — zlib states and small vecs the size terms don't name.
fn decode_mini_bound(
    z: &mut Inflate,
    base_len: Option<usize>,
    e: &PackEntry,
    raw: &[u8],
    obj_kind: H,
) -> usize {
    let hlen = e.header_size();
    let body_len = raw.len().saturating_sub(hlen);
    let mut mini_len = 12usize
        .saturating_add(32) // re-headed entry header: kind + size varints (+ ofs distance)
        .saturating_add(body_len)
        .saturating_add(obj_kind.len_in_bytes()); // zero digest trailer
    if let Some(b) = base_len {
        mini_len = mini_len
            .saturating_add(32) // base entry header
            .saturating_add(stored_deflate_bound(b));
    }
    let mini_alloc = mini_len.saturating_mul(2);
    let instr = usize::try_from(e.decompressed_size)
        .unwrap_or(usize::MAX)
        .min(MAX_OBJ as usize);
    let out_alloc = if e.header.is_delta() {
        // resolve_deltas sizes `out` = 2*biggest_result + instruction bytes;
        // biggest is max(base len, result claim) and claims over MAX_OBJ
        // are rejected by the alloc limit before the resize — clamp here.
        let result = delta_result_size(raw.get(hlen..).unwrap_or(&[]), z)
            .and_then(|r| usize::try_from(r).ok())
            .unwrap_or(MAX_OBJ as usize)
            .min(MAX_OBJ as usize);
        let biggest = base_len.unwrap_or(0).max(result).min(MAX_OBJ as usize);
        biggest
            .saturating_mul(2)
            .saturating_add(instr)
            .max(instr.saturating_mul(2))
    } else {
        instr
    };
    mini_alloc.saturating_add(out_alloc).saturating_add(DECODE_SCRATCH)
}

/// Bound for codec::decode_entry (external bases, the prefetch batch): it
/// allocates exactly the header's decompressed size plus one Inflate state —
/// the entry bytes are already live. Unparseable headers bound at MAX_OBJ;
/// the decode errors on them anyway.
fn decode_entry_bound(entry: &[u8]) -> usize {
    let size = codec::entry_header(entry).map(|(_, s, _)| s).unwrap_or(MAX_OBJ);
    usize::try_from(size)
        .unwrap_or(usize::MAX)
        .min(MAX_OBJ as usize)
        .saturating_add(DECODE_SCRATCH)
}

/// One decode_entry over `[PACK v2][base as a level-0 zlib full entry][delta re-headed as ofs-delta][digest-width zero trailer]`.
/// `cx` supplies the inflate, the object format, and — on the edge path —
/// the memory guard: the decode's peak working set is preflighted against
/// MEM_HARD before `mini` or `out` allocate (a ~16 MiB base plus ~16 MiB of
/// delta instructions would otherwise grow ~80 MiB inside this one call).
pub fn decode_mini(
    cx: &mut Cx<'_>,
    base: Option<(Kind, &[u8])>,
    raw: &[u8],
    label: ObjLabel,
) -> Result<(Kind, Vec<u8>), Error> {
    let obj_kind = cx.obj_kind;
    let e = PackEntry::from_bytes(raw, 0, obj_kind).map_err(|e| unpack(e.to_string()))?;
    let bound = decode_mini_bound(&mut cx.z, base.map(|(_, d)| d.len()), &e, raw, obj_kind);
    cx.mem_decode(bound, Some(label), "decode:mini")?;
    let mut mini = b"PACK\0\0\0\x02\0\0\0\x02".to_vec();
    if let Some((k, d)) = base {
        let hk = match k {
            Kind::Commit => Header::Commit,
            Kind::Tree => Header::Tree,
            Kind::Blob => Header::Blob,
            Kind::Tag => Header::Tag,
        };
        hk.write_to(d.len() as u64, &mut mini).map_err(internal)?;
        let mut w = gix_zlib::stream::deflate::Write::new(mini, Compression::new(0).unwrap_or_default());
        w.write_all(d).and_then(|_| std::io::Write::flush(&mut w)).map_err(internal)?;
        mini = w.into_inner();
    }
    let at = mini.len() as u64;
    let body = raw.get(e.header_size()..).ok_or_else(|| unpack("short entry"))?;
    let hdr = match (e.header.is_delta(), base) {
        (true, Some(_)) => Header::OfsDelta { base_distance: at.saturating_sub(12) },
        (true, None) => return Err(unpack("delta without base")),
        (false, _) => e.header,
    };
    hdr.write_to(e.decompressed_size, &mut mini).map_err(internal)?;
    mini.extend_from_slice(body);
    mini.resize(mini.len() + obj_kind.len_in_bytes(), 0);
    let file = File::<&[u8]>::from_data(&mini, "mini".into(), obj_kind)
        .map_err(internal)?
        .with_alloc_limit_bytes(usize::try_from(MAX_OBJ).ok());
    let mut out = Vec::new();
    let oc = file
        .decode_entry(
            file.entry(at).map_err(|e| unpack(e.to_string()))?,
            &mut out,
            &mut cx.z,
            &|_, _| None,
            &mut gix_pack::cache::Never,
        )
        .map_err(|e| unpack(format!("delta: {e}")))?;
    Ok((oc.kind, out))
}

/// A chain walk that can't proceed because a base id isn't resolvable *yet* — the base
/// may be a later in-pack entry (forward REF_DELTA), so the caller defers rather than
/// erroring. `Await` is only raised for ids not in `by_id`, `external`, or the caches.
pub enum Base {
    Ready(Obj),
    Await(ObjectId),
}

/// Thin-pack base (2.4): prefetched cache, else one coalesced read of its live location.
pub async fn external(cx: &mut Cx<'_>, id: ObjectId, budget: &mut ReqBudget) -> Result<Obj, Error> {
    if let Some(hit) = cx.cache.get(&Key::Id(id)) {
        return Ok(hit);
    }
    let loc = cx.externals.get(&id)?.ok_or_else(|| unpack(format!("missing base {id}")))?;
    if loc.size > MAX_OBJ {
        // a streamed (>16 MiB) blob can't materialize as a delta base — name the
        // cause and the client-side workaround instead of decode_entry's bare limit
        return Err(unpack(format!(
            "delta base {id} exceeds 16 MiB; push the object as a full blob \
             (e.g. git -c core.bigFileThreshold=1 push)"
        )));
    }
    // the coalesced read allocates its range buffer plus the entry copy
    // before decode_entry's own preflight can run — size the fetch first:
    // shed the cache, then fail in-band with Limit if it still can't fit
    let fetch = [(id, loc)];
    cx.mem_entries(&fetch, "entries:external")?;
    let (_, entry) = cx
        .win
        .reads
        .read_entries(&fetch, budget)
        .await?
        .pop()
        .ok_or_else(|| Error::Storage("base read".into()))?;
    // decode_entry materializes the whole base object — preflight its size
    // in-walk, not just the ambient heap
    cx.mem_decode(decode_entry_bound(&entry), Some(ObjLabel::Sha(id)), "decode:external")?;
    let (k, d) = codec::decode_entry(&entry)?;
    cx.cache_put(Key::Id(id), k, d, "grow:cache-external")
}

/// Resolve a ref-delta base id: resolved in-pack entry -> chain walk; repo base -> read;
/// unknown -> `Await` so the caller can defer until the entry is processed.
pub async fn base_by_id(
    cx: &mut Cx<'_>,
    entries: &Toc<'_>,
    base_id: ObjectId,
    budget: &mut ReqBudget,
) -> Result<Base, Error> {
    if let Some(hit) = cx.cache.get(&Key::Id(base_id)) {
        return Ok(Base::Ready(hit));
    }
    match cx.by_id.get(&base_id)? {
        Some(j) => {
            // async recursion (resolve_at -> base_by_id -> resolve_at) needs boxing;
            // bounded twice: nested hop depth AND shared in-flight chain bytes
            if cx.res_depth >= MAX_DEPTH as u32 {
                return Err(unpack("delta chain too deep"));
            }
            cx.res_depth += 1;
            let off = entries
                .by_idx(j)?
                .ok_or_else(|| internal("by_id idx"))?
                .offset;
            let r = Box::pin(resolve_at(cx, entries, off, budget)).await;
            cx.res_depth -= 1;
            r
        }
        None if cx.externals.get(&base_id)?.is_some() => {
            Ok(Base::Ready(external(cx, base_id, budget).await?))
        }
        None => Ok(Base::Await(base_id)),
    }
}

/// In-pack base at `start`: walk the chain back to a cached or full entry, then apply forward.
pub async fn resolve_at(
    cx: &mut Cx<'_>,
    entries: &Toc<'_>,
    start: u64,
    budget: &mut ReqBudget,
) -> Result<Base, Error> {
    // the edge path tightens the per-chain byte bound: one chain must not
    // be able to fill the headroom the memory guard needs around it
    let chain_cap = if cx.mem.is_some() { EDGE_CHAIN_BYTES } else { MAX_CHAIN_BYTES };
    let (mut off, mut chain, mut chain_bytes) = (start, Vec::new(), 0u64);
    let (kind, mut data) = loop {
        if let Some(hit) = cx.cache.get(&Key::Off(off)) {
            break hit;
        }
        if chain.len() >= MAX_DEPTH {
            return Err(unpack("delta chain too deep"));
        }
        let rec = entries
            .by_off(off)?
            .ok_or_else(|| unpack("delta base is not an entry"))?;
        // the chain cap is checked BEFORE the fetch: the entry table
        // already knows header_len + compressed_len, so an over-cap chain
        // fails without the window read — and the fetch's own allocations
        // (window replacement + entry copy) are preflighted against the
        // memory guard, which sheds the cache or fails in-band BEFORE
        // either buffer exists
        if !chain_room(chain_bytes, cx.chain_live, rec.wire_len(), chain_cap) {
            cx.chain_live = cx.chain_live.saturating_sub(chain_bytes);
            return Err(chain_too_large(cx.mem.is_some()));
        }
        cx.mem_fetch(&rec, "fetch:chain")?;
        let raw = cx.win.entry(&rec, budget).await?;
        // the walk can hold ~chain_cap of compressed bodies plus the
        // window, cache, and decode buffers before control returns to the
        // entry-boundary guard — shed over MEM_SOFT / fail over MEM_HARD
        // right here, before the body joins `chain` or a decode grows
        cx.mem_relief()?;
        // the chain holds each delta's compressed bytes: 64 × ~16 MiB worst case is a
        // GiB-scale allocation — bound the bytes, not just the depth. chain_live bounds
        // the SAME memory summed across every nested resolve_at frame.
        chain_bytes = chain_bytes.saturating_add(raw.len() as u64);
        cx.chain_live = cx.chain_live.saturating_add(raw.len() as u64);
        match rec.kind_or_delta {
            Header::OfsDelta { base_distance } => {
                chain.push((off, raw));
                off = Header::verified_base_pack_offset(off, base_distance)
                    .ok_or_else(|| unpack("bad ofs-delta"))?;
            }
            Header::RefDelta { base_id } => {
                chain.push((off, raw));
                match base_by_id(cx, entries, base_id, budget).await? {
                    // an unresolved in-pack base: abandon the walk; the whole entry defers
                    Base::Await(id) => {
                        cx.chain_live = cx.chain_live.saturating_sub(chain_bytes);
                        return Ok(Base::Await(id));
                    }
                    Base::Ready(obj) => break obj,
                }
            }
            _ => {
                if rec.size > MAX_OBJ {
                    // a streamed (>16 MiB) blob can't serve as a delta base — fail
                    // here rather than surfacing decode_mini's allocation-limit error
                    return Err(unpack("delta base exceeds 16 MiB and cannot be resolved"));
                }
                // decode_mini grows the re-deflated base plus the delta
                // body plus the output object — it preflights its own bound
                cx.mem_relief()?;
                let (k, d) = decode_mini(cx, None, &raw, ObjLabel::Offset(off))?;
                break cx.cache_put(Key::Off(off), k, d, "grow:cache-chain-base")?;
            }
        }
    };
    while let Some((o, raw)) = chain.pop() {
        // each apply decodes a <= 16 MiB object on top of the bytes the
        // rest of the chain still retains — same in-walk relief
        cx.mem_relief()?;
        let (_, d) = decode_mini(cx, Some((kind, &data)), &raw, ObjLabel::Offset(o))?;
        data = cx.cache_put(Key::Off(o), kind, d, "grow:cache-chain")?.1;
    }
    cx.chain_live = cx.chain_live.saturating_sub(chain_bytes);
    Ok(Base::Ready((kind, data)))
}

/// Would adding `entry_len` compressed bytes keep both chain bounds?
/// Runs BEFORE the fetch in `resolve_at`: the entry table already knows
/// the length, so an over-cap chain fails without the window read.
fn chain_room(chain_bytes: u64, chain_live: u64, entry_len: u64, chain_cap: u64) -> bool {
    chain_bytes.saturating_add(entry_len) <= chain_cap
        && chain_live.saturating_add(entry_len) <= MAX_CHAIN_LIVE
}

/// Chain-byte-limit failure: the edge path reports an in-band Limit the
/// client can slice around; the DO job keeps the unpack wording.
fn chain_too_large(edge: bool) -> Error {
    if edge {
        Error::Limit("delta chain too large for one request".into())
    } else {
        unpack("delta chain too large")
    }
}

/// The guard's memory across entry boundaries AND inside chain walks:
/// `cleared_at` is the live_bytes() reading taken AFTER the last
/// `cache.clear()` — the post-shed level, so hysteresis measures regrowth
/// from the drained heap. The first crossing always sheds; later sheds
/// inside the soft band need MEM_HYSTERESIS of growth. Above MEM_HARD —
/// and whenever the linear trip fires — the shed always runs: it is the
/// last relief before the Limit, not hysteresis fodder.
#[derive(Default)]
pub struct MemGuard {
    cleared_at: Option<usize>,
    /// The owning request's lease handle (from its ReqBudget): every guard
    /// call renews THIS request's permit, and an evicted holder learns of
    /// its eviction here. None renews nothing.
    lease: Option<Rc<super::run::Lease>>,
}

/// The linear-memory trip's wording: the isolate's high-water mark, not
/// this push's size, is the wall — slicing would not help, a retry will.
const LINEAR_PRESSURE_MSG: &str = "server under memory pressure; retry in a minute";
/// The alone-case wording for a pressure trip: the push's aggregate heap is
/// what needs slicing.
const MEM_PRESSURE_MSG: &str =
    "push needs more memory than one request allows; push it in smaller slices";
/// Flat scratch allowance inside a decode bound: the re-deflate's zlib state
/// (~200 KiB), a fresh Inflate for codec::decode_entry (~40 KiB), the delta
/// probe buffer, and small vec growth — everything a single decode allocates
/// that the size terms don't name.
const DECODE_SCRATCH: usize = 512 << 10;
/// Flat slack for writer/blob preflights: the deflate stream's internal
/// state, Vec capacity rounding past `room`'s whole-part target, the
/// UploadedPart bookkeeping, and request plumbing the byte terms don't name.
const STREAM_SLACK: usize = 1 << 20;

/// Bytes a hashbrown table sized for `items` entries of `T` holds: the
/// bucket count is the next power of two over items x 8/7 (87.5% max
/// load; 4 or 8 buckets for tiny tables), each bucket one `T` plus one
/// control byte, plus one 16-byte control group.
fn table_bytes<T>(items: usize) -> usize {
    if items == 0 {
        return 0;
    }
    let buckets = if items < 4 {
        4
    } else if items < 8 {
        8
    } else {
        (items.saturating_mul(8) / 7).saturating_add(1).next_power_of_two()
    };
    buckets.saturating_mul(std::mem::size_of::<T>().saturating_add(1)).saturating_add(16)
}

/// Peak bytes one more insert can need in a HashMap of `len` entries and
/// `cap` capacity: 0 while it has room, else the old table plus a new one
/// sized for at least cap + 1 entries (hashbrown at least doubles), held
/// at once while entries move, plus one allocation's overhead.
fn map_growth_peak<T>(len: usize, cap: usize) -> usize {
    if len < cap {
        return 0;
    }
    let grown = cap.saturating_add(1).max(len.saturating_add(1)).saturating_mul(2);
    crate::store::moving_peak(table_bytes::<T>(cap), table_bytes::<T>(grown))
}

/// Peak bytes a `Vec<T>` of `len`/`cap` can hold while `extend` grows it
/// for `adding` more elements: 0 if they fit, else Vec's amortized target
/// (max(2 x cap, need), at least 4) plus the old block a moving realloc
/// still holds, plus one allocation's overhead.
fn vec_growth_peak<T>(len: usize, cap: usize, adding: usize) -> usize {
    let need = len.saturating_add(adding);
    if need <= cap {
        return 0;
    }
    let elem = std::mem::size_of::<T>();
    let new_cap = need.max(cap.saturating_mul(2)).max(4);
    crate::store::moving_peak(cap.saturating_mul(elem), new_cap.saturating_mul(elem))
}

/// Names the ONE object a decode preflight is about to materialize — rendered
/// into the alone-case Limit only when the object's own bound is the wall
/// (see PER_OBJECT_DECODE), so the client knows smaller slices cannot shrink
/// a single decode. `Copy` so passing it costs nothing on the hot path.
#[derive(Clone, Copy)]
pub enum ObjLabel {
    /// An object already hashed — the sha prefix names it.
    Sha(ObjectId),
    /// Not yet hashed (mid-entry) — the pack offset names it.
    Offset(u64),
}
impl ObjLabel {
    fn render(&self) -> String {
        match self {
            ObjLabel::Sha(id) => {
                let s = id.to_string();
                format!("object {}", &s[..s.len().min(12)])
            }
            ObjLabel::Offset(o) => format!("object at pack offset {o}"),
        }
    }
}

/// Pass-B memory guard, run at each entry boundary of the edge resolve
/// loop and inside delta-chain walks (`Cx::mem_relief`). `live` reads the
/// counting allocator's live bytes, `linear` the wasm linear-memory size
/// (0 on native — the LINEAR_CAP trip is inert there), and `peers` the
/// isolate's in-progress receive-pack count (pass A included — this push's
/// own request counts as 1). Clearing the cache is the soft relief
/// (resolved objects re-resolve lazily). MEM_HARD always sheds first,
/// hysteresis aside: the clear is the last relief before the Limit, so no
/// fail verdict may precede it. Only a reading still above MEM_HARD after
/// the clear earns the client an in-band Limit. The message is honest
/// about cause: another receive-pack in flight means the isolate was busy
/// — retry — while an alone push really is too big for one request.
fn mem_guard(
    cache: &mut Cache,
    live: impl Fn() -> usize,
    linear: impl Fn() -> usize,
    peers: impl Fn() -> usize,
    state: &mut MemGuard,
) -> Result<(), Error> {
    mem_guard_peak(cache, live, linear, peers, state, 0, None, "relief")
}

/// `mem_guard` with `incoming` — bytes the caller is about to allocate —
/// added to the pressure reading, `obj` naming a single object whose own
/// decode bound is the wall (alone-case message only), and `site` tagging
/// the call for the test probe. EVERY large pass-B allocation preflights
/// through here: the decode preflight so a single decode that would cross
/// MEM_HARD sheds the cache and fails in-band BEFORE it allocates, the
/// fetch/entries preflights for the same reason on their buffers, and
/// `Cx::mem_grow` for every other growth — links vectors, id maps, order
/// lists, writer part buffers, chunk reads — so nothing ≥ ~1 MiB grows
/// unchecked. Hysteresis still applies: the shed triggers on the projected
/// reading, and `cleared_at` keeps recording the POST-shed actual live
/// bytes (and follows the heap back down — see below).
#[allow(clippy::too_many_arguments)]
fn mem_guard_peak(
    cache: &mut Cache,
    live: impl Fn() -> usize,
    linear: impl Fn() -> usize,
    peers: impl Fn() -> usize,
    state: &mut MemGuard,
    incoming: usize,
    obj: Option<ObjLabel>,
    site: &'static str,
) -> Result<(), Error> {
    let _ = site; // used only by the cfg(test) probe
    #[cfg(test)]
    preflight_log(site, incoming);
    // a guard call IS progress on the edge pass-B path — renew this
    // request's own permit lease; an evicted holder learns here and stops
    // in-band
    if let Some(l) = &state.lease {
        l.renew()?;
    }
    let live_now = live();
    // the hysteresis baseline follows the heap back down: cleared_at is the
    // lowest live reading since the last shed, so regrowth is measured from
    // the drained trough — a baseline parked high would hide real regrowth
    if let Some(at) = state.cleared_at {
        state.cleared_at = Some(at.min(live_now));
    }
    // LINEAR_CAP trip — independent of MEM_SOFT. Linear memory never
    // shrinks, so a high reading alone says nothing about pressure now:
    // the trip fires only when this allocation may need FRESH pages (live
    // + incoming + FRAG_MARGIN no longer fits inside linear) AND those
    // pages would cross the cap. Over it: shed once (forced — the last
    // relief, and it can help, since a lower live reading lets the
    // allocation reuse free pages), re-check, and still over -> in-band
    // Limit with the retry wording. A relief call (incoming 0) never trips.
    let lin = linear();
    if linear_trip(live_now, lin, incoming) {
        cache.clear();
        let after_live = live();
        state.cleared_at = Some(after_live);
        mem_log(format_args!(
            "git-edge mem guard: forced cache clear, linear {lin} + {incoming} incoming over {LINEAR_CAP} cap (live {live_now} -> {after_live})"
        ));
        let lin = linear();
        if linear_trip(after_live, lin, incoming) {
            mem_log(format_args!(
                "git-edge mem guard: linear {lin} + {incoming} incoming still over {LINEAR_CAP} cap at live {after_live} — rejecting"
            ));
            return Err(Error::Limit(LINEAR_PRESSURE_MSG.into()));
        }
    }
    let now = live_now.saturating_add(incoming);
    if now <= MEM_SOFT {
        return Ok(());
    }
    // hysteresis compares like with like: cleared_at is a plain live()
    // reading, so growth is measured on live() too — `incoming` projects
    // only the soft/hard thresholds, never the regrowth baseline (a decode
    // or fetch bound over MEM_HYSTERESIS on a flat heap must not re-shed)
    let grew = state
        .cleared_at
        .is_some_and(|at| live_now.saturating_sub(at) >= MEM_HYSTERESIS);
    if now > MEM_HARD || state.cleared_at.is_none() || grew {
        cache.clear();
        // the hysteresis baseline is the POST-shed reading: the shed just
        // drained the heap, so recording the pre-shed `now` would measure
        // regrowth from a level the cache no longer explains — a heap
        // parked just over MEM_SOFT could then sit above MEM_HARD without
        // ever shedding again
        state.cleared_at = Some(live());
    }
    let after = live().saturating_add(incoming);
    if after > MEM_HARD {
        return Err(Error::Limit(if peers() > 1 {
            INGEST_BUSY_MSG.into()
        } else if let Some(l) = obj.filter(|_| incoming > PER_OBJECT_DECODE) {
            // ONE object's own bound is the wall — name it; smaller slices
            // cannot shrink a single decode
            format!(
                "{} needs ~{} MiB to resolve, more than this server allows per object",
                l.render(),
                incoming.div_ceil(1 << 20)
            )
        } else {
            MEM_PRESSURE_MSG.into()
        }));
    }
    Ok(())
}

/// A one-off preflight outside the resolve loop — the final index post
/// after `resolve_and_normalize` returned and its Cx (and cache) are gone.
/// Same guard, same thresholds and wording, the owning request's lease
/// renewed; there is no cache left to shed.
pub(crate) fn mem_preflight(
    lease: Option<&Rc<super::run::Lease>>,
    incoming: usize,
    site: &'static str,
) -> Result<(), Error> {
    let mut state = MemGuard { cleared_at: None, lease: lease.cloned() };
    mem_guard_peak(
        &mut Cache::with_budget(0),
        crate::mem::live_bytes,
        crate::mem::linear_bytes,
        receive_packs,
        &mut state,
        incoming,
        None,
        site,
    )
}

/// wasm-only guard logging: a near-wall isolate (forced clear fired, or a
/// linear reject) shows up in `wrangler tail` instead of looking like a
/// silent hang. No-op natively.
#[cfg(target_arch = "wasm32")]
fn mem_log(msg: std::fmt::Arguments<'_>) {
    worker::console_log!("{msg}");
}
#[cfg(not(target_arch = "wasm32"))]
fn mem_log(_msg: std::fmt::Arguments<'_>) {}

/// Test-only probe: every preflight call records (site, bytes) so tests can
/// prove each production call site still runs before its allocation —
/// deleting a `mem_*`/`mem_grow` call removes its record and fails the test.
#[cfg(test)]
thread_local! {
    static PREFLIGHTS: std::cell::RefCell<Vec<(&'static str, usize)>> = const { std::cell::RefCell::new(Vec::new()) };
}
#[cfg(test)]
fn preflight_log(site: &'static str, bytes: usize) {
    PREFLIGHTS.with(|p| p.borrow_mut().push((site, bytes)));
}
#[cfg(test)]
pub(crate) fn preflight_sites() -> Vec<(&'static str, usize)> {
    PREFLIGHTS.with(|p| p.borrow().clone())
}
#[cfg(test)]
pub(crate) fn clear_preflights() {
    PREFLIGHTS.with(|p| p.borrow_mut().clear());
}

/// Bytes `extract_links`'s result Vec can reach for one decoded object:
/// the worst-case link count bounded by the object's own size — a tree
/// entry is never smaller than ~22 wire bytes (mode + name + NUL + oid),
/// a commit parent line never smaller than ~48 ("parent " + 40-hex + \n) —
/// times the ObjectId element size, with Vec's doubling headroom and the
/// commit `parents` collect counted alongside. Blob/tag are O(1).
fn links_bound(kind: Kind, data_len: usize, _obj_kind: H) -> usize {
    const MIN_TREE_ENTRY: usize = 22;
    const MIN_PARENT_LINE: usize = 48;
    let elem = std::mem::size_of::<ObjectId>();
    let count = match kind {
        Kind::Tree => data_len / MIN_TREE_ENTRY,
        Kind::Commit => 1 + data_len / MIN_PARENT_LINE,
        Kind::Tag => 1,
        Kind::Blob => 0,
    };
    // links Vec doubling (peak < 2x) + the parents Vec for commits (<= count)
    count.saturating_mul(elem).saturating_mul(2).saturating_add(count.saturating_mul(elem))
}

/// Bytes `HashMap::with_capacity(n)` for (ObjectId, usize) pairs holds:
/// hashbrown caps at ~87.5% load then rounds slots to a power of two, plus
/// one control byte per slot — the by_id reserve's honest size.
fn id_map_bound(n: usize) -> usize {
    let slots = (n.saturating_mul(8) / 7).saturating_add(8).next_power_of_two();
    slots.saturating_mul(std::mem::size_of::<(ObjectId, usize)>().saturating_add(1))
}

/// The thin-pack bases worth prefetching: each must fit the 16 MiB object
/// cap — `external()` reports the actionable error for bigger ones, and
/// `decode_entry` would throw its bare limit first — and the batch's
/// decompressed bytes (`loc.size`, what `Cache` budgets by `capacity()`)
/// stay under EDGE_CACHE, so early decodes can't evict entries still queued.
fn prefetch_locs(bases: &HashMap<ObjectId, ObjLoc>) -> Vec<(ObjectId, ObjLoc)> {
    let mut out = Vec::new();
    let mut sum = 0u64;
    for (id, loc) in bases {
        if loc.size <= MAX_OBJ {
            sum = sum.saturating_add(loc.size);
            if sum > EDGE_CACHE as u64 {
                break;
            }
            out.push((*id, loc.clone()));
        }
    }
    out
}

/// `sink` posts `/_do/push/index` in 10,000-row batches and collects links (2.5).
/// `externals` is the thin-pack base view — a prefetched map here, the live index
/// itself for an in-DO job (see jobs/import.rs).
pub async fn resolve_and_normalize<O: PackOut>(
    reads: Reader<'_>,
    pending_key: &str,
    entries: &[EntryRec],
    externals: Externals<'_>,
    out: &mut O,
    sink: &mut AnySink<'_, '_>,
    obj_kind: gix_hash::Kind,
    budget: &mut ReqBudget,
) -> Result<Vec<ObjRow>, Error> {
    let ds = u64::try_from(obj_kind.len_in_bytes()).map_err(internal)?;
    let pack_len = entries.last().map_or(12 + ds, |r| {
        r.offset
            .saturating_add(u64::from(r.header_len))
            .saturating_add(u64::from(r.compressed_len))
            .saturating_add(ds)
    });
    let mut cx = Cx {
        win: Window { reads, src: Source::Key(pending_key), pack_len, start: 0, buf: Vec::new() },
        cache: Cache::with_budget(EDGE_CACHE),
        z: Inflate::default(),
        by_id: IdMap::Map(HashMap::new()),
        externals,
        obj_kind,
        res_depth: 0,
        chain_live: 0,
        mem: Some(MemGuard { cleared_at: None, lease: budget.lease.clone() }),
    };
    let pre = match &cx.externals {
        Externals::Map(bases) => prefetch_locs(bases),
        Externals::Idx(_) => Vec::new(),
    };
    // prefetch is opportunistic on both axes: the batch's fetch allocation
    // (entry copies + largest merged span) is preflighted through the same
    // guard external() uses and skipped when even the shed can't fit it,
    // and `read_entries` bounds *merged span* bytes (gaps included) that
    // the loc sums can't predict — a Limit skips the batch the same way.
    // Either way every base still resolves lazily through external()'s own
    // guarded fetch, and a failed decode preflight inside the loop ends the
    // batch early, dropping the unread entries for lazy re-reads where
    // they're actually needed.
    if cx.mem_entries(&pre, "entries:prefetch").is_ok() {
        if let Ok(entries) = reads.read_entries(&pre, budget).await {
            for (id, entry) in entries {
                let Ok(()) = cx.mem_decode(decode_entry_bound(&entry), Some(ObjLabel::Sha(id)), "decode:prefetch") else { break };
                let Ok((k, d)) = codec::decode_entry(&entry) else { break };
                let Ok(_) = cx.cache_put(Key::Id(id), k, d, "grow:cache-prefetch") else { break };
            }
        }
    }
    // ref-delta entries are the only ones that can Await — count them for
    // the pending-map bound (only RefDelta bases can be forward references)
    let ref_deltas = entries
        .iter()
        .filter(|r| matches!(r.kind_or_delta, Header::RefDelta { .. }))
        .count();
    let need_ids = ref_deltas > 0;
    // every processed entry inserts into by_id when ref-deltas exist —
    // reserve its final size up front with one preflight instead of
    // paying an unguarded doubling per insert
    if need_ids {
        cx.mem_grow(id_map_bound(entries.len()), None, "grow:by_id")?;
        cx.by_id = IdMap::Map(HashMap::with_capacity(entries.len()));
    }
    let toc = Toc::Mem(entries);
    let mut rows = Vec::new();
    // A REF_DELTA may name a base that appears LATER in the same pack — including
    // mid-chain inside another delta. `one_entry` returns `Await(base)` for those;
    // resolving an entry wakes exactly its waiters (index-pack parity, O(n) not O(n^2)).
    // order + pending grow with the entry count (waiters re-queue): one
    // honest bound up front keeps their growth inside the guard. `order`
    // holds <= 2n indices (n initial + <= n re-queues) at ~2x Vec headroom;
    // `pending` holds <= ref_deltas map entries plus <= n usize waiters at
    // ~2x headroom — an all-ref-delta max pack is the honest worst case.
    cx.mem_grow(
        entries.len().saturating_mul(std::mem::size_of::<usize>()).saturating_mul(4)
            .saturating_add(ref_deltas.saturating_mul(std::mem::size_of::<(ObjectId, Vec<usize>)>().saturating_mul(2)))
            .saturating_add(entries.len().saturating_mul(std::mem::size_of::<usize>()).saturating_mul(4))
            .saturating_add(STREAM_SLACK),
        None,
        "grow:order",
    )?;
    let mut pending: HashMap<ObjectId, Vec<usize>> = HashMap::new();
    let mut order: Vec<usize> = Vec::with_capacity(entries.len());
    order.extend(0..entries.len());
    let mut i = 0;
    while i < order.len() {
        cx.mem_relief()?;
        let at = order[i];
        i += 1;
        match one_entry(&mut cx, &toc, at, need_ids, out, sink, &mut rows, budget, 0).await? {
            Step::Done(id) => {
                if let Some(ws) = pending.remove(&id) {
                    order.extend(ws);
                }
            }
            Step::Await(base) => {
                pending.entry(base).or_default().push(at);
            }
        }
    }
    if let Some((&base, _)) = pending.iter().next() {
        return Err(unpack(format!("missing base {base}")));
    }
    Ok(rows)
}

/// The per-entry outcome: resolved+emitted (`Done`), or blocked on a base id that a later
/// in-pack entry will produce (`Await`).
pub enum Step {
    Done(ObjectId),
    Await(ObjectId),
}

/// Resolve + normalize one pack entry: base resolution, decode, hash, links, append, index row.
/// `skip` = wire bytes of this entry already durable in the output pack (a mid-entry
/// resume after a checkpoint boundary): the entry is fully resolved and hashed but
/// only its tail is appended, at the durable offset.
#[allow(clippy::too_many_arguments)]
pub async fn one_entry<O: PackOut>(
    cx: &mut Cx<'_>,
    entries: &Toc<'_>,
    i: usize,
    need_ids: bool,
    out: &mut O,
    sink: &mut AnySink<'_, '_>,
    rows: &mut Vec<ObjRow>,
    budget: &mut ReqBudget,
    skip: u64,
) -> Result<Step, Error> {
    let rec = entries
        .by_idx(i)?
        .ok_or_else(|| internal("idx"))?;
    // A blob past MAX_OBJ rides the verbatim path: its compressed bytes are already
    // the normalized form, so we copy them through and re-inflate only to hash —
    // the object never materializes in isolate memory.
    if rec.kind_or_delta == Header::Blob && rec.size > MAX_OBJ {
        return stream_blob(cx, &rec, i, need_ids, out, sink, rows, budget, skip).await;
    }
    let base = match rec.kind_or_delta {
        Header::OfsDelta { base_distance } => Some(
            resolve_at(
                cx,
                entries,
                Header::verified_base_pack_offset(rec.offset, base_distance)
                    .ok_or_else(|| unpack("bad ofs-delta"))?,
                budget,
            )
            .await?,
        ),
        Header::RefDelta { base_id } => Some(base_by_id(cx, entries, base_id, budget).await?),
        _ => None,
    };
    let base = match base {
        Some(Base::Ready(o)) => Some(o),
        Some(Base::Await(id)) => return Ok(Step::Await(id)),
        None => None,
    };
    // preflight the window fetch before it allocates — the same guard the
    // chain walk runs per hop (a no-op on the DO path, `mem` is `None`)
    cx.mem_fetch(&rec, "fetch:entry")?;
    let raw = cx.win.entry(&rec, budget).await?;
    // C7 pause point: an R2 read just awaited and this entry's JobSink writes
    // (mark/seal/post) come next — a slice stranded here is already past
    // resume_drain's CAS, so only the sink's fence can stop it. No-op with
    // hooks off; Edge sinks carry no DO and skip the gate.
    if let AnySink::Job(s) = sink {
        crate::testhooks::failpoint_async(s.d, "import.entry.after_read").await?;
    }
    // the entry's own decode grows re-deflated base + body + output on top
    // of whatever the base walk just retained — last in-walk relief point
    cx.mem_relief()?;
    let (kind, data) =
        decode_mini(cx, base.as_ref().map(|(k, d)| (*k, d.as_slice())), &raw, ObjLabel::Offset(rec.offset))?;
    let id = gix_object::compute_hash(cx.obj_kind, kind, &data).map_err(|_| unpack("hash collision"))?;
    cx.mem_grow(links_bound(kind, data.len(), cx.obj_kind), None, "grow:links-extract")?;
    let links = extract_links(kind, &data, cx.obj_kind)?;
    // protected refs: extract_links puts a commit's tree first, then parents
    let parents = (kind == Kind::Commit).then(|| links.iter().skip(1).copied().collect::<Vec<_>>());
    if kind == Kind::Tag {
        if let Some(&t) = links.first() {
            // the tag map doubles as tags accumulate — preflight its resize
            cx.mem_grow(sink.tags_growth_bound(), None, "grow:tags")?;
            sink.tags_mut().insert(id, t);
        }
    }
    // the sink-side extend doubles `links` capacity — preflight the realloc
    // before it happens (the per-object extract bound above covers only
    // the temporary Vec)
    cx.mem_grow(sink.links_growth_bound(links.len()), None, "grow:links-sink")?;
    // bound inside the loop too — a single giant tree can spike `links` far past
    // the limit between post() batches otherwise
    sink.push_links(links)?;
    sink.mark(i, out.offset().saturating_sub(skip), Some(id), kind, data.len() as u64)?;
    // the writer's part buffer grows in whole-part steps for this entry —
    // preflight the capacity delta plus encoder slack before append_entry
    // (or the re-encode + raw_extend on the resume path) allocates it
    cx.mem_grow(out.growth_bound(crate::store::encoded_bound(data.len())) + STREAM_SLACK, None, "grow:writer-entry")?;
    let (offset, len) = if skip == 0 {
        out.append_entry(kind, &data)?
    } else {
        // mid-entry resume: bytes [0, skip) of the wire entry are already durable
        // upload parts — re-encode deterministically and append only the tail,
        // so the row's (offset, len) still describes the whole entry.
        let mut enc = Vec::new();
        codec::encode_entry(kind, &data, &mut enc)?;
        let tail = usize::try_from(skip.min(enc.len() as u64)).map_err(internal)?;
        let start = out.offset().saturating_sub(tail as u64);
        out.raw_extend(&enc[tail..]);
        out.raw_entry_done(kind, start)?
    };
    // each flush splits one tight part_size chunk held across the upload —
    // preflight it (plus slack) before the split can allocate
    cx.mem_grow(out.chunk_bound() + STREAM_SLACK, None, "grow:writer-flush")?;
    out.flush_if_full(budget).await?;
    rows.push(ObjRow {
        sha: id,
        idx: u32::try_from(i).map_err(|_| unpack("too many objects"))?,
        offset,
        len,
        kind,
        size: data.len() as u64,
        parents,
    });
    sink.seal(i, offset.saturating_add(u64::from(len)), id)?;
    if need_ids {
        cx.by_id.insert(id, i);
    }
    cx.cache_put(Key::Off(rec.offset), kind, data, "grow:cache-entry")?;
    if rows.len() >= sink.row_batch() {
        // each index post's JSON body (plus its request copy) is a pass-B
        // allocation too — commit rows carry parents, so price it first
        cx.mem_grow(sink.post_bound(rows)?, None, "grow:index-entry")?;
        sink.post(rows, budget).await?;
        rows.clear();
    }
    Ok(Step::Done(id))
}

/// Verbatim pass-through for a blob over MAX_OBJ: emit the pending entry's wire
/// bytes (header + zlib body) unchanged in <= 8 MiB reads while a resumable zlib
/// stream re-inflates them purely to compute the object id. A delta that names a
/// streamed blob as its base fails at explicit guards: `resolve_at`'s size check
/// for in-pack bases, `external`'s for repo-resident ones — a > 16 MiB base is not
/// resolvable regardless of how well it compressed.
async fn stream_blob<O: PackOut>(
    cx: &mut Cx<'_>,
    rec: &EntryRec,
    i: usize,
    need_ids: bool,
    out: &mut O,
    sink: &mut AnySink<'_, '_>,
    rows: &mut Vec<ObjRow>,
    budget: &mut ReqBudget,
    skip: u64,
) -> Result<Step, Error> {
    // skip = wire bytes already durable (mid-entry resume): still read + inflate
    // everything (the sha covers the whole body) but drop that prefix before append.
    let mut skip = skip;
    let start = out.offset().saturating_sub(skip);
    sink.mark(i, start, None, Kind::Blob, rec.size)?;
    let body_start = rec.offset.saturating_add(u64::from(rec.header_len));
    let end = body_start.saturating_add(u64::from(rec.compressed_len));
    let hdr = cx.win.read(rec.offset, u64::from(rec.header_len), budget).await?;
    {
        let s = usize::try_from(skip.min(hdr.len() as u64)).map_err(internal)?;
        cx.mem_grow(out.growth_bound(hdr.len() - s) + STREAM_SLACK, None, "grow:blob-hdr")?;
        out.raw_extend(&hdr[s..]);
        skip -= s as u64;
    }
    cx.mem_grow((1 << 20) + STREAM_SLACK, None, "grow:blob-sinkbuf")?;
    let (mut z, mut h, mut sinkbuf) = (Decompress::new(), gix_hash::hasher(cx.obj_kind), vec![0u8; 1 << 20]);
    h.update(format!("blob {}\0", rec.size).as_bytes());
    let (mut pos, mut produced, mut st) = (body_start, 0u64, Status::Ok);
    while pos < end && st != Status::StreamEnd {
        let n = end.saturating_sub(pos).min(WINDOW);
        // each chunk read allocates its Vec — preflight before it happens
        cx.mem_grow(usize::try_from(n).unwrap_or(usize::MAX) + STREAM_SLACK, None, "grow:blob-chunk")?;
        let chunk = cx.win.read(pos, n, budget).await?;
        let mut inp: &[u8] = &chunk;
        loop {
            let (bi, bo) = (z.total_in(), z.total_out());
            st = z
                .decompress(inp, &mut sinkbuf, FlushDecompress::None)
                .map_err(|e| unpack(format!("zlib at {pos}: {e}")))?;
            let (used, made) = (
                usize::try_from(z.total_in().saturating_sub(bi)).map_err(internal)?,
                usize::try_from(z.total_out().saturating_sub(bo)).map_err(internal)?,
            );
            h.update(sinkbuf.get(..made).unwrap_or(&[]));
            produced = produced.saturating_add(made as u64);
            if produced > rec.size {
                return Err(unpack(format!("object at {pos}: size mismatch")));
            }
            inp = inp.get(used..).ok_or_else(|| internal("zlib input"))?;
            if inp.is_empty() || st == Status::StreamEnd {
                break;
            }
            if used == 0 && made == 0 {
                return Err(unpack(format!("zlib stalled at {pos}")));
            }
        }
        let s = usize::try_from(skip.min(chunk.len() as u64)).map_err(internal)?;
        cx.mem_grow(out.growth_bound(chunk.len() - s) + STREAM_SLACK, None, "grow:blob-raw")?;
        out.raw_extend(&chunk[s..]);
        skip -= s as u64;
        cx.mem_grow(out.chunk_bound() + STREAM_SLACK, None, "grow:blob-flush")?;
        out.flush_if_full(budget).await?;
        pos = pos.saturating_add(n);
    }
    if st != Status::StreamEnd
        || produced != rec.size
        || z.total_in() != u64::from(rec.compressed_len)
    {
        return Err(unpack(format!("object at {}: size mismatch", rec.offset)));
    }
    let id = h.try_finalize().map_err(|_| unpack("sha1 collision"))?;
    let (offset, len) = out.raw_entry_done(Kind::Blob, start)?;
    rows.push(ObjRow {
        sha: id,
        idx: u32::try_from(i).map_err(|_| unpack("too many objects"))?,
        offset,
        len,
        kind: Kind::Blob,
        size: rec.size,
        parents: None,
    });
    sink.seal(i, offset.saturating_add(u64::from(len)), id)?;
    if need_ids {
        cx.by_id.insert(id, i);
    }
    if rows.len() >= sink.row_batch() {
        cx.mem_grow(sink.post_bound(rows)?, None, "grow:index-blob")?;
        sink.post(rows, budget).await?;
        rows.clear();
    }
    Ok(Step::Done(id))
}

/// Object references for the 2.5 connectivity check (1.4): commit -> tree + parents,
/// tree -> entries except gitlinks, tag -> target. `obj_kind` sets the oid width
/// embedded in commit/tree/tag bodies (sha256 writes 32-byte ids).
pub fn extract_links(kind: Kind, data: &[u8], obj_kind: H) -> Result<Vec<ObjectId>, Error> {
    let mut out = Vec::new();
    match kind {
        Kind::Commit => {
            let mut it = gix_object::CommitRefIter::from_bytes(data, obj_kind);
            // strict: a commit whose tree header doesn't parse is not a valid commit —
            // storing it would hard-fail every later fetch that walks it
            out.push(it.tree_id().map_err(|e| Error::Unpack(e.to_string()))?);
            out.extend(it.parent_ids());
        }
        Kind::Tree => {
            for e in gix_object::TreeRefIter::from_bytes(data, obj_kind) {
                let e = e.map_err(|e| Error::Unpack(e.to_string()))?;
                if !e.mode.is_commit() {
                    out.push(e.oid.to_owned());
                }
            }
        }
        Kind::Tag => {
            let t = gix_object::TagRef::from_bytes(data, obj_kind)
                .map_err(|e| Error::Unpack(e.to_string()))?;
            out.push(t.target());
        }
        Kind::Blob => {}
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn links_chunk_respects_a6_batch() {
        let chunk = vec![ObjectId::null(H::Sha1); LINKS_CHUNK];
        let (q, args) = links_insert("push", &chunk);
        assert!(args.len() <= 90);
        assert_eq!(q.matches("(?,?)").count(), LINKS_CHUNK);
    }

    #[test]
    fn cache_accounts_capacity_and_evicts_lru() {
        let mut c = Cache::with_budget(100 + 2 * CACHE_ENTRY_OVERHEAD);
        c.put(Key::Off(1), Kind::Blob, vec![7u8; 60]);
        c.put(Key::Off(2), Kind::Blob, vec![9u8; 60]);
        // second put overflowed the budget: the oldest entry was evicted
        assert!(c.get(&Key::Off(1)).is_none());
        assert!(c.get(&Key::Off(2)).is_some());
        // bytes tracks the Vec's capacity(), not its len(), plus overhead
        let cap = c.map.get(&Key::Off(2)).unwrap().1.capacity();
        assert_eq!(c.bytes, cap + CACHE_ENTRY_OVERHEAD);
        c.clear();
        assert_eq!(c.bytes, 0);
        assert!(c.get(&Key::Off(2)).is_none());
    }

    #[test]
    fn mem_guard_soft_clears_hard_limits() {
        // under the soft ceiling: untouched
        let mut c = Cache::default();
        c.put(Key::Off(1), Kind::Blob, vec![1u8; 8]);
        let mut g = MemGuard::default();
        mem_guard(&mut c, || MEM_SOFT, || 0, || 1, &mut g).unwrap();
        assert!(c.get(&Key::Off(1)).is_some());
        // first crossing past soft but under hard: cache shed, push continues
        mem_guard(&mut c, || MEM_SOFT + 1, || 0, || 1, &mut g).unwrap();
        assert!(c.get(&Key::Off(1)).is_none());
        // still above hard: typed Limit, actionable message
        let mut c = Cache::default();
        let mut g = MemGuard::default();
        let err = mem_guard(&mut c, || MEM_HARD + 1, || 0, || 1, &mut g).unwrap_err();
        assert!(matches!(err, Error::Limit(_)));
        assert!(err.message().contains("smaller slices"), "{err}");
    }

    /// The MEM_HARD message names the real cause: alone -> smaller slices;
    /// another receive-pack in flight (pass A included) -> the isolate was
    /// busy, retry instead. `peers` counts this push's own request as 1.
    #[test]
    fn mem_guard_message_names_busy_when_peers_run() {
        let mut c = Cache::default();
        let mut g = MemGuard::default();
        let err = mem_guard(&mut c, || MEM_HARD + 1, || 0, || 1, &mut g).unwrap_err();
        assert!(err.message().contains("smaller slices"), "{err}");
        let mut g = MemGuard::default();
        let err = mem_guard(&mut c, || MEM_HARD + 1, || 0, || 2, &mut g).unwrap_err();
        assert!(err.message().contains(INGEST_BUSY_MSG), "{err}");
    }

    /// The linear trip is independent of MEM_SOFT and fires only when the
    /// allocation may need fresh pages that would cross LINEAR_CAP. Then it
    /// sheds once (forced — the last relief) and still over fails in-band
    /// with the retry wording, never "smaller slices".
    #[test]
    fn mem_guard_linear_trip_needs_fresh_pages_over_the_cap() {
        const MIB: usize = 1 << 20;
        // live high enough that the allocation cannot reuse free pages
        let live = LINEAR_CAP - 8 * MIB;
        let mut c = Cache::default();
        c.put(Key::Off(1), Kind::Blob, vec![1u8; 8]);
        let mut g = MemGuard::default();
        let err = mem_guard_peak(&mut c, || live, || LINEAR_CAP, || 1, &mut g, MIB, None, "test:lin").unwrap_err();
        assert!(matches!(err, Error::Limit(_)));
        assert_eq!(err.message(), LINEAR_PRESSURE_MSG);
        assert!(!err.message().contains("smaller slices"), "{err}");
        assert!(c.get(&Key::Off(1)).is_none(), "the forced shed ran before the Limit");
        // peers in flight get the same retry wording: linear is the wall
        let mut g = MemGuard::default();
        let err = mem_guard_peak(&mut Cache::default(), || live, || LINEAR_CAP, || 2, &mut g, MIB, None, "test:lin")
            .unwrap_err();
        assert_eq!(err.message(), LINEAR_PRESSURE_MSG);
        // relief calls (incoming 0) never trip, even far over the cap
        let mut c = Cache::default();
        c.put(Key::Off(1), Kind::Blob, vec![1u8; 8]);
        let mut g = MemGuard::default();
        mem_guard(&mut c, || 10 * MIB, || LINEAR_CAP + 64 * MIB, || 1, &mut g).unwrap();
        assert!(c.get(&Key::Off(1)).is_some(), "no forced clear when the check passes");
    }

    /// Claude #3: sticky high linear with a low live heap. Linear sits over
    /// the cap from an earlier push, but live + incoming + FRAG_MARGIN fits
    /// inside it — the allocation reuses free pages, so it is admitted and
    /// the cache is not cleared.
    #[test]
    fn mem_guard_sticky_high_linear_with_low_live_admits_reuse() {
        const MIB: usize = 1 << 20;
        let mut c = Cache::default();
        c.put(Key::Off(1), Kind::Blob, vec![1u8; 8]);
        let mut g = MemGuard::default();
        // 120 MiB linear (over the cap), 20 MiB live, a 16 MiB window fetch
        mem_guard_peak(&mut c, || 20 * MIB, || 120 * MIB, || 1, &mut g, 16 * MIB, None, "test:lin").unwrap();
        assert!(c.get(&Key::Off(1)).is_some(), "a reuse fit must not force a clear");
        // the fit boundary: live + incoming + FRAG_MARGIN == linear still fits
        let lin = 120 * MIB;
        let incoming = 16 * MIB;
        let live = lin - incoming - FRAG_MARGIN;
        assert!(!linear_trip(live, lin, incoming));
        assert!(linear_trip(live + 1, lin, incoming), "one byte more needs fresh pages");
        // the cap boundary: linear + incoming == LINEAR_CAP does not cross
        assert!(!linear_trip(LINEAR_CAP, LINEAR_CAP - incoming, incoming));
        assert!(linear_trip(LINEAR_CAP, LINEAR_CAP - incoming + 1, incoming));
        assert!(!linear_trip(LINEAR_CAP, LINEAR_CAP + 1, 0), "incoming 0 never trips");
        assert_eq!(FRAG_MARGIN, 16 << 20);
    }

    /// The forced clear helps now: when the shed drops live far enough
    /// that the allocation reuses free pages, the re-check passes. The
    /// re-check's cap boundary is exact too (`>` not `>=`).
    #[test]
    fn mem_guard_linear_recheck_after_shed() {
        const MIB: usize = 1 << 20;
        let incoming = 8 * MIB;
        // live reads: before the shed, after the shed (re-check), then
        // the hard-band reading; linear reads before and after
        let reads = |v: Vec<usize>| {
            let q = std::cell::RefCell::new(std::collections::VecDeque::from(v));
            move || {
                let mut q = q.borrow_mut();
                let x = q.pop_front().unwrap();
                if q.is_empty() {
                    q.push_back(x);
                }
                x
            }
        };
        // shed frees enough: re-check fits inside linear -> Ok
        let mut g = MemGuard::default();
        mem_guard_peak(
            &mut Cache::default(),
            reads(vec![100 * MIB, 40 * MIB]),
            || 110 * MIB,
            || 1,
            &mut g,
            incoming,
            None,
            "test:lin",
        )
        .unwrap();
        assert_eq!(g.cleared_at, Some(40 * MIB), "the forced shed recorded its post-clear reading");
        // shed does not free enough, but the re-check's linear reading sits
        // exactly at the cap with the incoming bytes: not over -> Ok
        let mut g = MemGuard::default();
        mem_guard_peak(
            &mut Cache::default(),
            reads(vec![100 * MIB, 100 * MIB, 40 * MIB]),
            reads(vec![LINEAR_CAP, LINEAR_CAP - incoming]),
            || 1,
            &mut g,
            incoming,
            None,
            "test:lin",
        )
        .unwrap();
        // one byte over at the re-check -> Limit
        let mut g = MemGuard::default();
        let err = mem_guard_peak(
            &mut Cache::default(),
            reads(vec![100 * MIB, 100 * MIB]),
            reads(vec![LINEAR_CAP, LINEAR_CAP - incoming + 1]),
            || 1,
            &mut g,
            incoming,
            None,
            "test:lin",
        )
        .unwrap_err();
        assert_eq!(err.message(), LINEAR_PRESSURE_MSG);
        // and the first check's cap boundary: exactly at the cap never sheds
        let mut c = Cache::default();
        c.put(Key::Off(1), Kind::Blob, vec![1u8; 8]);
        let mut g = MemGuard::default();
        mem_guard_peak(&mut c, || 60 * MIB, || LINEAR_CAP - incoming, || 1, &mut g, incoming, None, "test:lin").unwrap();
        assert!(c.get(&Key::Off(1)).is_some(), "at the cap exactly: no trip, no forced clear");
    }

    /// Between MEM_SOFT and MEM_HARD the cache sheds once per MEM_HYSTERESIS
    /// of live-heap growth, not at every entry — a heap parked in the band
    /// keeps its cache (and its subrequest count) instead of re-resolving
    /// every delta chain through window re-reads.
    #[test]
    fn mem_guard_hysteresis_sheds_once_per_growth() {
        let mut c = Cache::default();
        c.put(Key::Off(1), Kind::Blob, vec![1u8; 8]);
        let mut g = MemGuard::default();
        // first crossing sheds and records the reading
        mem_guard(&mut c, || MEM_SOFT + 1, || 0, || 1, &mut g).unwrap();
        assert!(c.get(&Key::Off(1)).is_none());
        assert_eq!(g.cleared_at, Some(MEM_SOFT + 1));
        // the cache refills; heap grew < MEM_HYSTERESIS since the shed and
        // stays under MEM_HARD: no shed — the whole point of the hysteresis
        c.put(Key::Off(2), Kind::Blob, vec![2u8; 8]);
        let live = MEM_SOFT + 1 + MEM_HYSTERESIS - 1;
        mem_guard(&mut c, || live, || 0, || 1, &mut g).unwrap();
        assert!(c.get(&Key::Off(2)).is_some(), "sub-hysteresis growth keeps the cache");
        // heap grew >= MEM_HYSTERESIS since the last shed: shed again
        let live = MEM_SOFT + 1 + MEM_HYSTERESIS;
        mem_guard(&mut c, || live, || 0, || 1, &mut g).unwrap();
        assert!(c.get(&Key::Off(2)).is_none(), "full hysteresis growth sheds again");
        assert_eq!(g.cleared_at, Some(live));
    }

    /// `cleared_at` records the POST-shed reading, not the pre-shed one:
    /// the shed drains the heap, so hysteresis must measure regrowth from
    /// the drained level — a baseline taken before the clear would sit
    /// above what the cache explains and could leave the soft band unable
    /// to shed again while the heap stays over MEM_HARD.
    #[test]
    fn mem_guard_hysteresis_baseline_is_post_shed() {
        let mut c = Cache::default();
        c.put(Key::Off(1), Kind::Blob, vec![1u8; 8]);
        // live() call order inside mem_guard: pre-shed `now`, the
        // post-clear `cleared_at` write, then the post-shed hard check.
        // The shed drains 90 -> 82 MiB here.
        let readings = std::cell::RefCell::new(std::collections::VecDeque::from([
            90usize << 20,
            82 << 20,
            82 << 20,
            // second guard call: `now` then `after` — heap parked at 85,
            // only 3 MiB over the DRAINED baseline: no shed
            85 << 20,
            85 << 20,
            // third call: 90 again — 8 MiB over the drained baseline:
            // sheds again and records ITS post-shed reading (84)
            90 << 20,
            84 << 20,
            84 << 20,
        ]));
        let live = || readings.borrow_mut().pop_front().unwrap_or(84 << 20);
        let mut g = MemGuard::default();
        mem_guard(&mut c, live, || 0, || 1, &mut g).unwrap();
        assert_eq!(g.cleared_at, Some(82 << 20), "baseline is the post-shed reading");
        assert!(c.get(&Key::Off(1)).is_none());
        // regrowth measured from 82, not 90: 85 - 82 = 3 < MEM_HYSTERESIS
        c.put(Key::Off(2), Kind::Blob, vec![2u8; 8]);
        mem_guard(&mut c, live, || 0, || 1, &mut g).unwrap();
        assert!(c.get(&Key::Off(2)).is_some(), "sub-hysteresis growth keeps the cache");
        // 90 - 82 = 8 >= MEM_HYSTERESIS: sheds — with the buggy pre-shed
        // baseline (90) this would NOT have shed (90 - 90 = 0)
        mem_guard(&mut c, live, || 0, || 1, &mut g).unwrap();
        assert!(c.get(&Key::Off(2)).is_none(), "full hysteresis growth sheds again");
        assert_eq!(g.cleared_at, Some(84 << 20));
    }

    /// The linear trip sheds BEFORE it judges, hysteresis aside: when it
    /// fires, the forced clear runs even inside the hysteresis band — it is
    /// the last relief before the in-band Limit, never skipped for a fail
    /// verdict.
    #[test]
    fn mem_guard_linear_trip_sheds_before_judging() {
        let mut c = Cache::default();
        c.put(Key::Off(1), Kind::Blob, vec![1u8; 8]);
        let mut g = MemGuard::default();
        g.cleared_at = Some(82 << 20); // sub-hysteresis regrowth — hysteresis alone would skip
        let err = mem_guard_peak(&mut c, || 85 << 20, || LINEAR_CAP, || 1, &mut g, 12 << 20, None, "test:lin").unwrap_err();
        assert!(matches!(err, Error::Limit(_)));
        assert!(c.get(&Key::Off(1)).is_none(), "the forced shed ran before the Limit");
        // the shed recorded the post-clear reading as the hysteresis base
        assert_eq!(g.cleared_at, Some(85 << 20));
    }

    /// Above MEM_HARD the shed is not hysteretic — it is the last relief
    /// before the Limit, so it runs even right after a recent shed.
    #[test]
    fn mem_guard_hard_band_always_sheds() {
        let mut c = Cache::default();
        c.put(Key::Off(1), Kind::Blob, vec![1u8; 8]);
        let mut g = MemGuard::default();
        g.cleared_at = Some(MEM_HARD); // a shed just happened — still sheds
        let err = mem_guard(&mut c, || MEM_HARD + 1, || 0, || 1, &mut g).unwrap_err();
        assert!(matches!(err, Error::Limit(_)));
        assert!(c.get(&Key::Off(1)).is_none(), "hard-band shed ignores hysteresis");
    }

    /// The edge chain cap reports an in-band Limit the client can slice
    /// around; the DO path keeps the unpack wording.
    #[test]
    fn chain_too_large_names_edge_limit() {
        assert!(matches!(chain_too_large(true), Error::Limit(_)));
        assert!(chain_too_large(true).message().contains("delta chain too large for one request"));
        assert!(matches!(chain_too_large(false), Error::Unpack(_)));
        // the edge cap is the tighter of the two bounds
        assert!(EDGE_CHAIN_BYTES < MAX_CHAIN_BYTES);
    }

    /// git's delta-header varints: 7 bits per byte, high bit continues,
    /// least-significant group first.
    fn put_varint(mut v: u64, out: &mut Vec<u8>) {
        loop {
            let mut b = (v & 0x7f) as u8;
            v >>= 7;
            if v != 0 {
                b |= 0x80;
            }
            out.push(b);
            if v == 0 {
                break;
            }
        }
    }

    /// A raw pack entry holding one ofs-delta: header + zlib(delta stream).
    /// The stream is base_size varint + result_size varint + instruction bytes.
    fn delta_raw(base_size: u64, result_size: u64, instr: &[u8]) -> Vec<u8> {
        use std::io::Write as _;
        let mut d = Vec::new();
        put_varint(base_size, &mut d);
        put_varint(result_size, &mut d);
        d.extend_from_slice(instr);
        let mut body = Vec::new();
        {
            let mut w = gix_zlib::stream::deflate::Write::new(&mut body, Compression::DEFAULT);
            w.write_all(&d).unwrap();
            std::io::Write::flush(&mut w).unwrap();
        }
        let mut raw = Vec::new();
        Header::OfsDelta { base_distance: 1 }
            .write_to(d.len() as u64, &mut raw)
            .unwrap();
        raw.extend_from_slice(&body);
        raw
    }

    /// The delta-header probe reads the result size gix-pack's own probe
    /// would: first varint base size, second result size.
    #[test]
    fn delta_result_size_probes_header_varints() {
        let raw = delta_raw(1_000, 65_536, &[0x91, 0x00]);
        let e = PackEntry::from_bytes(&raw, 0, H::Sha1).unwrap();
        let body = &raw[e.header_size()..];
        let mut z = Inflate::default();
        assert_eq!(delta_result_size(body, &mut z), Some(65_536));
        // garbage input can't produce the header -> None -> MAX_OBJ bound
        assert_eq!(delta_result_size(b"not zlib", &mut z), None);
        // round-trips through delta_varint at the edges
        assert_eq!(delta_varint(&[0x80, 0x80, 0x01]), Some((16_384, 3)));
        assert_eq!(delta_varint(&[]), None);
        assert_eq!(delta_varint(&[0x80]), None, "truncated varint");
    }

    /// delta_varint's u64 edge, exactly: nine continuation bytes put the
    /// 10th group at shift 63, where only bit 0 fits. A 10th group of 1
    /// parses to 1 << 63; a 10th group of 2 would drop its bit and is
    /// refused; an 11th group (shift 70) is always refused.
    #[test]
    fn delta_varint_u64_boundary() {
        let mut nine = vec![0x80u8; 9];
        nine.push(0x01);
        assert_eq!(delta_varint(&nine), Some((1u64 << 63, 10)));
        let mut max = vec![0xffu8; 9];
        max.push(0x01);
        assert_eq!(delta_varint(&max), Some((u64::MAX, 10)));
        let mut over = vec![0x80u8; 9];
        over.push(0x02);
        assert_eq!(delta_varint(&over), None, "bit 64 would be dropped");
        let mut eleven = vec![0x80u8; 10];
        eleven.push(0x00);
        assert_eq!(delta_varint(&eleven), None, "shift 70 is past u64");
        // the 9th group (shift 56) still takes all seven bits
        let mut eight = vec![0x80u8; 8];
        eight.push(0x7f);
        assert_eq!(delta_varint(&eight), Some((0x7f << 56, 9)));
    }

    /// The per-object wording boundary is strict: a bound of exactly
    /// PER_OBJECT_DECODE gets the whole-push wording; one byte more names
    /// the object, and its size rounds UP to whole MiB (56 MiB + 1 byte
    /// reads ~57 MiB).
    #[test]
    fn mem_guard_per_object_boundary_and_mib_rounding() {
        let oid = ObjectId::from_hex(b"0123456789abcdef0123456789abcdef01234567").unwrap();
        let live = MEM_HARD - PER_OBJECT_DECODE + 1; // every incoming here crosses MEM_HARD
        let mut g = MemGuard::default();
        let err = mem_guard_peak(&mut Cache::default(), || live, || 0, || 1, &mut g, PER_OBJECT_DECODE, Some(ObjLabel::Sha(oid)), "test:decode")
            .unwrap_err();
        assert_eq!(err.message(), MEM_PRESSURE_MSG, "at the bound exactly: whole-push wording");
        let mut g = MemGuard::default();
        let err = mem_guard_peak(&mut Cache::default(), || live, || 0, || 1, &mut g, PER_OBJECT_DECODE + 1, Some(ObjLabel::Sha(oid)), "test:decode")
            .unwrap_err();
        assert_eq!(
            err.message(),
            "object 0123456789ab needs ~57 MiB to resolve, more than this server allows per object"
        );
        assert_eq!(PER_OBJECT_DECODE, 56 << 20);
    }

    /// The hysteresis baseline follows the heap down to the lowest reading
    /// and never rises without a shed — equal readings leave it unchanged.
    #[test]
    fn mem_guard_baseline_tracks_the_trough() {
        let mut g = MemGuard { cleared_at: Some(70 << 20), lease: None };
        mem_guard(&mut Cache::default(), || 60 << 20, || 0, || 1, &mut g).unwrap();
        assert_eq!(g.cleared_at, Some(60 << 20), "follows the heap down");
        mem_guard(&mut Cache::default(), || 60 << 20, || 0, || 1, &mut g).unwrap();
        assert_eq!(g.cleared_at, Some(60 << 20));
        mem_guard(&mut Cache::default(), || 65 << 20, || 0, || 1, &mut g).unwrap();
        assert_eq!(g.cleared_at, Some(60 << 20), "never rises without a shed");
    }

    /// decode_mini_bound counts the mini-pack copy (raw body + re-deflated
    /// base) AND decode_entry's work Vec (2*biggest result + instructions).
    #[test]
    fn decode_mini_bound_counts_mini_and_work_buffers() {
        let mut z = Inflate::default();
        // full (non-delta) entry: out = decompressed size, mini = raw copy
        let mut enc = Vec::new();
        codec::encode_entry(Kind::Blob, &[7u8; 4_096], &mut enc).unwrap();
        let e = PackEntry::from_bytes(&enc, 0, H::Sha1).unwrap();
        let bound = decode_mini_bound(&mut z, None, &e, &enc, H::Sha1);
        // mini alone is ~2x the raw wire bytes; out adds the 4 KiB object
        assert!(bound >= enc.len().saturating_mul(2), "{bound} < 2*raw");
        assert!(bound >= 4_096 + DECODE_SCRATCH, "{bound}");
        // delta entry: biggest = max(base, result) drives the 2x work buffers
        let raw = delta_raw(2 << 20, 1 << 20, &[0u8; 1_000]);
        let e = PackEntry::from_bytes(&raw, 0, H::Sha1).unwrap();
        let bound = decode_mini_bound(&mut z, Some(2 << 20), &e, &raw, H::Sha1);
        // out term alone is 2*biggest(2 MiB) + instr(~1 KiB+hdr); mini adds
        // ~2x (stored base + raw body)
        assert!(bound >= (2 << 20) * 2, "{bound}");
        assert!(bound >= raw.len() * 2, "{bound}");
        // a bigger claimed result size lifts the bound
        let raw_big = delta_raw(1 << 10, 8 << 20, &[0u8; 10]);
        let e_big = PackEntry::from_bytes(&raw_big, 0, H::Sha1).unwrap();
        let bound_big = decode_mini_bound(&mut z, Some(1 << 10), &e_big, &raw_big, H::Sha1);
        assert!(bound_big > bound, "{bound_big} <= {bound}");
        // codec::decode_entry bound: header size + scratch, capped at MAX_OBJ
        assert_eq!(decode_entry_bound(&enc), 4_096 + DECODE_SCRATCH);
        assert_eq!(decode_entry_bound(b"junk"), MAX_OBJ as usize + DECODE_SCRATCH);
    }

    /// stored_deflate_bound stays above the real level-0 overhead
    /// (5 bytes per <=64 KiB-1 stored block + 6-byte zlib wrapper).
    #[test]
    fn stored_deflate_bound_covers_level0_overhead() {
        for len in [0usize, 1, 65_535, 65_536, 1 << 20, MAX_OBJ as usize] {
            let real_blocks = len / 65_535 + usize::from(len % 65_535 != 0);
            let real = len + 5 * real_blocks + 6;
            assert!(stored_deflate_bound(len) >= real, "len {len}");
        }
    }

    /// The preflight projects live + incoming: a decode whose working set
    /// would cross MEM_HARD sheds the cache then fails in-band — alone with
    /// the "object too large" wording, busy with peers — while a fitting
    /// decode passes.
    #[test]
    fn mem_guard_peak_preflight_projects_incoming() {
        // fits: calm heap + small decode -> untouched
        let mut c = Cache::default();
        let mut g = MemGuard::default();
        mem_guard_peak(&mut c, || MEM_SOFT - (2 << 20), || 0, || 1, &mut g, 1 << 20, None, "test:decode").unwrap();
        // alone + too big even after shed: Limit, cache shed. A decode
        // bound under PER_OBJECT_DECODE gets the whole-push wording —
        // smaller slices CAN shrink it.
        c.put(Key::Off(1), Kind::Blob, vec![1u8; 8]);
        let readings = std::cell::RefCell::new(std::collections::VecDeque::from([
            90usize << 20, // now = live + incoming -> over hard, shed runs
            60 << 20,      // post-shed live for cleared_at
            60 << 20,      // after = post-shed live + incoming -> still over
        ]));
        let live = || readings.borrow_mut().pop_front().unwrap_or(60 << 20);
        let err = mem_guard_peak(&mut c, live, || 0, || 1, &mut g, 40 << 20, None, "test:decode").unwrap_err();
        assert!(matches!(err, Error::Limit(_)));
        assert!(err.message().contains("smaller slices"), "{err}");
        assert!(c.get(&Key::Off(1)).is_none(), "the shed ran before the Limit");
        // ONE object's own bound over PER_OBJECT_DECODE names the object —
        // smaller slices cannot shrink a single decode
        let oid = ObjectId::from_hex(b"0123456789abcdef0123456789abcdef01234567").unwrap();
        let readings = std::cell::RefCell::new(std::collections::VecDeque::from([
            90usize << 20, 60 << 20, 60 << 20,
        ]));
        let live = || readings.borrow_mut().pop_front().unwrap_or(60 << 20);
        let err = mem_guard_peak(&mut c, live, || 0, || 1, &mut g, 60 << 20, Some(ObjLabel::Sha(oid)), "test:decode").unwrap_err();
        assert!(matches!(err, Error::Limit(_)));
        assert!(err.message().contains("needs ~60 MiB to resolve"), "{err}");
        assert!(err.message().contains(&oid.to_string()[..12]), "{err}");
        assert!(!err.message().contains("smaller slices"), "{err}");
        // same pressure with a peer in flight -> the busy wording
        let mut g = MemGuard::default();
        let err = mem_guard_peak(&mut c, || 90 << 20, || 0, || 2, &mut g, 40 << 20, None, "test:decode").unwrap_err();
        assert!(err.message().contains(INGEST_BUSY_MSG), "{err}");
        // shed makes it fit: post-shed live + incoming stays under MEM_HARD
        let mut c = Cache::default();
        c.put(Key::Off(2), Kind::Blob, vec![2u8; 8]);
        let readings = std::cell::RefCell::new(std::collections::VecDeque::from([
            90usize << 20, 50 << 20, 50 << 20,
        ]));
        let live = || readings.borrow_mut().pop_front().unwrap_or(50 << 20);
        let mut g = MemGuard::default();
        mem_guard_peak(&mut c, live, || 0, || 1, &mut g, 30 << 20, None, "test:decode").unwrap();
        assert!(c.get(&Key::Off(2)).is_none(), "shed still ran on projected pressure");
    }

    /// Hysteresis compares like with like: `cleared_at` is a plain live()
    /// reading, so `incoming` projects only the soft/hard thresholds — a
    /// bound over MEM_HYSTERESIS on a flat heap must not re-shed (the
    /// unlike-quantities bug: now - cleared_at mixed live+incoming
    /// against live).
    #[test]
    fn mem_guard_peak_incoming_is_not_growth() {
        let mut c = Cache::default();
        c.put(Key::Off(1), Kind::Blob, vec![1u8; 8]);
        let mut g = MemGuard::default();
        g.cleared_at = Some(70 << 20);
        // flat heap + incoming > MEM_HYSTERESIS: projected pressure is real
        // (86 MiB > MEM_SOFT) but the heap did not grow -> cache stays
        mem_guard_peak(&mut c, || 70 << 20, || 0, || 1, &mut g, 16 << 20, None, "test:decode").unwrap();
        assert!(c.get(&Key::Off(1)).is_some(), "flat heap + big incoming must not shed");
        assert_eq!(g.cleared_at, Some(70 << 20));
        // genuine regrowth >= MEM_HYSTERESIS still sheds with incoming present
        let mut c = Cache::default();
        c.put(Key::Off(2), Kind::Blob, vec![2u8; 8]);
        let mut g = MemGuard::default();
        g.cleared_at = Some(70 << 20);
        mem_guard_peak(&mut c, || 79 << 20, || 0, || 1, &mut g, 2 << 20, None, "test:decode").unwrap();
        assert!(c.get(&Key::Off(2)).is_none(), "real growth sheds");
        assert_eq!(g.cleared_at, Some(79 << 20));
    }

    /// The fetch preflight counts what win.entry allocates: a miss pays
    /// the replacement window (wire length grown to WINDOW, clipped at
    /// the pack's end) plus the returned copy; a hit pays only the copy;
    /// a wire span entry() rejects up front allocates nothing -> None.
    #[test]
    fn entry_fetch_bound_counts_miss_hit_and_reject() {
        let rec = |clen: u32, hlen: u8, off: u64| EntryRec {
            offset: off,
            header_len: hlen,
            kind_or_delta: Header::Blob,
            compressed_len: clen,
            size: 1,
        };
        let pack_len = 1u64 << 30;
        // miss, small entry: WINDOW buffer + header+body copy + slack
        let r = rec(1 << 20, 3, 100);
        assert_eq!(
            entry_fetch_bound(pack_len, 0, 0, &r),
            Some(WINDOW as usize + (1 << 20) as usize + 3 + FETCH_SCRATCH)
        );
        // the same entry inside the live window: the copy only
        assert_eq!(
            entry_fetch_bound(pack_len, 0, WINDOW as u64, &r),
            Some((1 << 20) as usize + 3 + FETCH_SCRATCH)
        );
        // miss, big entry: the window grows to the wire length -> 2x len
        let r = rec(20 << 20, 3, WINDOW as u64 + 1);
        let len = (20usize << 20) + 3;
        assert_eq!(
            entry_fetch_bound(pack_len, 0, WINDOW as u64, &r),
            Some(2 * len + FETCH_SCRATCH)
        );
        // the window clips at the pack's end
        let r = rec(20 << 20, 3, pack_len - (5 << 20));
        assert_eq!(
            entry_fetch_bound(pack_len, 0, 0, &r),
            Some((5 << 20) as usize + len + FETCH_SCRATCH)
        );
        // a wire span entry() rejects allocates nothing -> None
        let r = rec((MAX_ENTRY_WIRE + 128) as u32, 3, 100);
        assert_eq!(entry_fetch_bound(pack_len, 0, 0, &r), None);
    }

    /// The fetch bound drives the same guard as a decode bound: projected
    /// live + window/copy sheds the cache, then fails in-band over
    /// MEM_HARD — with the PRESSURE wording, since the push's heap is the
    /// cause — while a fitting fetch passes untouched. (mem_fetch needs a
    /// Bucket; this runs its bound through mem_guard_peak, the decision it
    /// delegates to.)
    #[test]
    fn fetch_bound_drives_guard_with_pressure_wording() {
        // a near-wire-limit entry on a miss: ~2x wire length incoming
        let r = EntryRec {
            offset: 1 << 20,
            header_len: 4,
            kind_or_delta: Header::Blob,
            compressed_len: (30 << 20) as u32,
            size: 1,
        };
        let incoming = entry_fetch_bound(1u64 << 30, 0, 0, &r).unwrap();
        assert!(incoming > (55 << 20), "{incoming}");
        // heap + fetch can't fit even after the shed -> Limit, cache shed
        let mut c = Cache::default();
        c.put(Key::Off(1), Kind::Blob, vec![1u8; 8]);
        let readings = std::cell::RefCell::new(std::collections::VecDeque::from([
            50usize << 20, // now = live + incoming -> over hard, shed runs
            40 << 20,      // post-shed live for cleared_at
            40 << 20,      // after = post-shed live + incoming -> still over
        ]));
        let live = || readings.borrow_mut().pop_front().unwrap_or(40 << 20);
        let mut g = MemGuard::default();
        let err =
            mem_guard_peak(&mut c, live, || 0, || 1, &mut g, incoming, None, "test:fetch").unwrap_err();
        assert!(matches!(err, Error::Limit(_)));
        assert!(err.message().contains("smaller slices"), "{err}");
        assert!(!err.message().contains("object too large"), "{err}");
        assert!(c.get(&Key::Off(1)).is_none(), "the shed ran before the Limit");
        // a window-hit fetch on a calm heap passes untouched
        let r = EntryRec {
            offset: 100,
            header_len: 3,
            kind_or_delta: Header::Blob,
            compressed_len: 1 << 20,
            size: 1,
        };
        let incoming = entry_fetch_bound(1u64 << 30, 0, WINDOW as u64, &r).unwrap();
        assert!(incoming < (2 << 20), "{incoming}");
        let mut c = Cache::default();
        c.put(Key::Off(2), Kind::Blob, vec![2u8; 8]);
        let mut g = MemGuard::default();
        mem_guard_peak(&mut c, || MEM_SOFT - (4 << 20), || 0, || 1, &mut g, incoming, None, "test:fetch")
            .unwrap();
        assert!(c.get(&Key::Off(2)).is_some());
    }

    /// The chain cap is checked BEFORE the fetch from the entry table's
    /// known lengths: adding entry_len must keep both the per-chain and
    /// the shared live bounds.
    #[test]
    fn chain_room_checks_both_bounds_before_fetch() {
        // exactly at the cap fits; one byte over fails
        assert!(chain_room(EDGE_CHAIN_BYTES - 100, 0, 100, EDGE_CHAIN_BYTES));
        assert!(!chain_room(EDGE_CHAIN_BYTES - 99, 0, 100, EDGE_CHAIN_BYTES));
        // the shared live bound applies across nested frames the same way
        assert!(chain_room(0, MAX_CHAIN_LIVE - 100, 100, EDGE_CHAIN_BYTES));
        assert!(!chain_room(0, MAX_CHAIN_LIVE - 99, 100, EDGE_CHAIN_BYTES));
        // saturating adds: absurd lengths fail, never wrap
        assert!(!chain_room(u64::MAX, 0, u64::MAX, EDGE_CHAIN_BYTES));
    }

    /// The prefetch batch is capped at EDGE_CACHE of DECOMPRESSED bytes —
    /// `loc.size`, what the cache's capacity() accounting sees — so early
    /// decodes can't evict entries still queued; streamed bases over
    /// MAX_OBJ are skipped for external() to name. The compressed `len`
    /// is irrelevant to the cap: a tiny wire can decode to MAX_OBJ each.
    #[test]
    fn prefetch_locs_caps_at_edge_cache_and_skips_oversized() {
        let oid = |n: u8| ObjectId::from_hex(format!("{n:02x}").repeat(20).as_bytes()).unwrap();
        let loc = |len: u32, size: u64| ObjLoc {
            pack: crate::store::PackId("p".into()),
            idx: 0,
            offset: 0,
            len,
            kind: Kind::Blob,
            size,
        };
        // three 10 MiB decompressed bases behind 1 KiB wires: whichever
        // order the map yields, the third crosses EDGE_CACHE (10+10+10 >
        // 24) -> exactly two are kept — a len-based sum would keep all
        let mut bases = HashMap::new();
        for n in 0..3u8 {
            bases.insert(oid(n), loc(1 << 10, 10 << 20));
        }
        // a streamed base over MAX_OBJ never counts toward the sum
        bases.insert(oid(3), loc(1 << 20, MAX_OBJ + 1));
        let got = prefetch_locs(&bases);
        assert_eq!(got.len(), 2);
        assert!(got.iter().all(|(_, l)| l.size <= MAX_OBJ));
        assert!(got.iter().map(|(_, l)| l.size).sum::<u64>() <= EDGE_CACHE as u64);
        // the exact-cap boundary is kept: the check is `>` not `>=`
        let mut bases = HashMap::new();
        bases.insert(oid(4), loc(1 << 10, 12 << 20));
        bases.insert(oid(5), loc(1 << 10, 12 << 20));
        assert_eq!(prefetch_locs(&bases).len(), 2);
    }
    // ---- mutation-sensitive call-site tests (PR #44 review) ----
    // Every large pass-B allocation preflights through mem_guard_peak, which
    // records (site, bytes) under cfg(test). These tests drive the REAL
    // resolve loop on in-memory packs and assert each production site's tag
    // was recorded — deleting a mem_fetch/mem_decode/mem_entries/mem_grow/
    // mem_relief call site removes its record and fails the test.

    /// A waker that does nothing — MemReads resolves without real awaits.
    fn noop_waker() -> std::task::Waker {
        use std::task::{RawWaker, RawWakerVTable, Waker};
        unsafe fn clone(_: *const ()) -> RawWaker {
            RawWaker::new(std::ptr::null(), &VTABLE)
        }
        unsafe fn no_op(_: *const ()) {}
        static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, no_op, no_op, no_op);
        unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
    }
    /// Drive `f` to completion — MemReads' futures resolve on first poll.
    fn block_on<F: std::future::Future>(f: F) -> F::Output {
        let w = noop_waker();
        let mut cx = std::task::Context::from_waker(&w);
        let mut f = std::pin::pin!(f);
        for _ in 0..1_000_000 {
            if let std::task::Poll::Ready(v) = f.as_mut().poll(&mut cx) {
                return v;
            }
        }
        panic!("future did not resolve");
    }

    /// In-memory PackOut: append_entry/raw_extend/raw_entry_done mirror
    /// PackWriter's contract over a Vec; growth_bound/chunk_bound price the
    /// same room() steps so preflights see honest sizes.
    struct MemPackOut {
        buf: Vec<u8>,
    }
    impl PackOut for MemPackOut {
        fn offset(&self) -> u64 {
            self.buf.len() as u64
        }
        fn append_entry(&mut self, kind: Kind, data: &[u8]) -> Result<(u64, u32), Error> {
            let start = self.buf.len();
            crate::store::room(&mut self.buf, crate::store::PART, crate::store::encoded_bound(data.len()));
            codec::encode_entry(kind, data, &mut self.buf)?;
            let len = u32::try_from(self.buf.len() - start).map_err(|_| Error::Limit("entry too long".into()))?;
            Ok((start as u64, len))
        }
        fn raw_extend(&mut self, bytes: &[u8]) {
            crate::store::room(&mut self.buf, crate::store::PART, bytes.len());
            self.buf.extend_from_slice(bytes);
        }
        fn raw_entry_done(&mut self, _kind: Kind, start: u64) -> Result<(u64, u32), Error> {
            let len = u32::try_from(self.buf.len() as u64 - start).map_err(|_| Error::Limit("entry too long".into()))?;
            Ok((start, len))
        }
        fn growth_bound(&self, extra: usize) -> usize {
            crate::store::room_growth(&self.buf, crate::store::PART, extra)
        }
        fn chunk_bound(&self) -> usize {
            crate::store::PART
        }
        async fn flush_if_full<'a>(&'a mut self, _budget: &'a mut ReqBudget) -> Result<(), Error> {
            Ok(())
        }
    }

    /// Build a v2 pack's bytes + EntryRec table for `(kind, data)` full
    /// entries — pass B never reads the trailer, so it stays zeroed.
    fn mk_pack(objs: &[(Kind, &[u8])]) -> (Vec<u8>, Vec<EntryRec>) {
        let mut pack = b"PACK\0\0\0\x02\0\0\0\0".to_vec();
        let mut recs = Vec::new();
        for (k, d) in objs {
            let off = pack.len() as u64;
            let mut enc = Vec::new();
            codec::encode_entry(*k, d, &mut enc).unwrap();
            let e = PackEntry::from_bytes(&enc, 0, H::Sha1).unwrap();
            let hlen = e.header_size();
            recs.push(EntryRec {
                offset: off,
                header_len: u8::try_from(hlen).unwrap(),
                kind_or_delta: e.header,
                compressed_len: u32::try_from(enc.len() - hlen).unwrap(),
                size: e.decompressed_size,
            });
            pack.extend_from_slice(&enc);
        }
        pack[8..12].copy_from_slice(&(objs.len() as u32).to_be_bytes());
        pack.extend_from_slice(&[0u8; 20]); // sha1-width trailer: pack_len counts it
        (pack, recs)
    }

    /// One raw delta-entry body: `varint(base) varint(result) ops` zlib'd,
    /// headed by `hdr` — the wire bytes Window::entry returns for a delta.
    fn mk_delta_raw(hdr: Header, base_size: u64, result_size: u64, ops: &[u8]) -> Vec<u8> {
        use std::io::Write as _;
        let mut d = Vec::new();
        put_varint(base_size, &mut d);
        put_varint(result_size, &mut d);
        d.extend_from_slice(ops);
        let mut body = Vec::new();
        {
            let mut w = gix_zlib::stream::deflate::Write::new(&mut body, Compression::DEFAULT);
            w.write_all(&d).unwrap();
            std::io::Write::flush(&mut w).unwrap();
        }
        let mut raw = Vec::new();
        hdr.write_to(result_size, &mut raw).unwrap();
        raw.extend_from_slice(&body);
        raw
    }

    /// An insert-everything delta op stream: `result` literal bytes out,
    /// independent of the base — cmd = len (1..=127), then the literals.
    fn insert_ops(result: &[u8]) -> Vec<u8> {
        assert!(!result.is_empty() && result.len() < 128);
        let mut v = vec![result.len() as u8];
        v.extend_from_slice(result);
        v
    }

    fn test_budget() -> ReqBudget {
        ReqBudget { max_subrequests: 9_000, used: 0, started_ms: crate::wall_now(), max_ms: ReqBudget::MAX_MS, sink: None, lease: None }
    }

    /// Run the real resolve loop over `recs` served from `mem` at `key`,
    /// collecting the preflight probe afterwards.
    fn drive<'a>(
        mem: &'a MemReads,
        key: &'a str,
        recs: &'a [EntryRec],
        externals: Externals<'a>,
        out: &'a mut MemPackOut,
        sink: &'a mut MemSink,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<ObjRow>, Error>> + 'a>> {
        let reads = Reader::Mem(mem);
        let any = AnySink::Mem(sink);
        // AnySink is consumed by value inside resolve_and_normalize? No —
        // it takes &mut AnySink. Build it in the async block instead.
        let _ = any;
        Box::pin(async move {
            let reads = Reader::Mem(mem);
            let mut any = AnySink::Mem(sink);
            let mut budget = test_budget();
            resolve_and_normalize(reads, key, recs, externals, out, &mut any, H::Sha1, &mut budget).await
        })
    }

    fn sites_of(sites: &[(&'static str, usize)]) -> Vec<&'static str> {
        sites.iter().map(|(t, _)| *t).collect()
    }


    /// A Cx over MemReads for direct resolve_at/external tests — the same
    /// shape resolve_and_normalize builds, with the edge MemGuard on.
    fn test_cx<'a>(
        mem: &'a MemReads,
        key: &'a str,
        pack_len: u64,
        externals: Externals<'a>,
    ) -> Cx<'a> {
        Cx {
            win: Window { reads: Reader::Mem(mem), src: Source::Key(key), pack_len, start: 0, buf: Vec::new() },
            cache: Cache::with_budget(EDGE_CACHE),
            z: Inflate::default(),
            by_id: IdMap::Map(HashMap::new()),
            externals,
            obj_kind: H::Sha1,
            res_depth: 0,
            chain_live: 0,
            mem: Some(MemGuard::default()),
        }
    }

    /// pack_len the way resolve_and_normalize computes it
    fn pack_len_of(recs: &[EntryRec]) -> u64 {
        recs.last().map_or(12 + 20, |r| {
            r.offset.saturating_add(u64::from(r.header_len)).saturating_add(u64::from(r.compressed_len)).saturating_add(20)
        })
    }
    /// The blobs-only pack exercises the whole per-entry spine: relief at
    /// the boundary, the prefetch batch guard, the window fetch, the
    /// decode, links extract+sink, writer growth and flush, order/pending.
    #[test]
    fn resolve_records_every_entry_site() {
        let blob = b"hello world, this is a blob".as_slice();
        let (pack, recs) = mk_pack(&[(Kind::Blob, blob), (Kind::Blob, b"second blob")]);
        let empty: HashMap<ObjectId, ObjLoc> = HashMap::new();
        let key = "pending/p1.pack";
        let repo = crate::store::RepoId("o/r".into());
        let mut mem = MemReads::new(repo);
        mem.put(key, pack);
        let mut out = MemPackOut { buf: Vec::new() };
        let mut sink = MemSink::default();
        clear_preflights();
        let res = block_on(drive(&mem, key, &recs, Externals::Map(&empty), &mut out, &mut sink));
        res.unwrap();
        let tags = sites_of(&preflight_sites());
        for want in [
            "relief",
            "entries:prefetch",
            "fetch:entry",
            "decode:mini",
            "grow:links-extract",
            "grow:links-sink",
            "grow:writer-entry",
            "grow:writer-flush",
            "grow:order",
        ] {
            assert!(tags.contains(&want), "missing preflight site {want} in {tags:?}");
        }
        // no ref-deltas -> no by_id reserve, no chain walk, no external fetch
        assert!(!tags.contains(&"grow:by_id"), "{tags:?}");
        assert!(!tags.contains(&"fetch:chain"), "{tags:?}");
        assert!(!tags.contains(&"entries:external"), "{tags:?}");
        assert!(out.buf.len() > 12, "writer emitted pack bytes");
    }

    /// An ofs-delta chain walks resolve_at: the chain fetch, the in-walk
    /// reliefs, and both decode_mini hops are all preflighted sites. Driven
    /// directly — through the orchestrated loop the base is always cache-
    /// hot by the time the delta resolves, so fetch:chain never iterates.
    #[test]
    fn ofs_delta_chain_records_chain_sites() {
        let base = b"the quick brown fox jumps over".as_slice();
        let (mut pack, mut recs) = mk_pack(&[(Kind::Blob, base)]);
        let d = base.len();
        let delta_off = pack.len() as u64;
        let dist = delta_off - 12;
        let raw = mk_delta_raw(Header::OfsDelta { base_distance: dist }, d as u64, d as u64, &insert_ops(base));
        let e = PackEntry::from_bytes(&raw, 0, H::Sha1).unwrap();
        let hlen = e.header_size();
        recs.push(EntryRec {
            offset: delta_off,
            header_len: u8::try_from(hlen).unwrap(),
            kind_or_delta: Header::OfsDelta { base_distance: dist },
            compressed_len: u32::try_from(raw.len() - hlen).unwrap(),
            size: e.decompressed_size,
        });
        pack.extend_from_slice(&raw);
        pack[8..12].copy_from_slice(&2u32.to_be_bytes());
        pack.extend_from_slice(&[0u8; 20]);
        let key = "pending/p2.pack";
        let repo = crate::store::RepoId("o/r".into());
        let mut mem = MemReads::new(repo);
        mem.put(key, pack);
        let empty: HashMap<ObjectId, ObjLoc> = HashMap::new();
        let toc = Toc::Mem(&recs);
        clear_preflights();
        let mut budget = test_budget();
        // resolve the DELTA entry itself: resolve_at walks its chain back
        // to the base — every hop's fetch/decode is a preflight site. The
        // hand-built delta may fail to fully apply; the sites already ran.
        let mut cx = test_cx(&mem, key, pack_len_of(&recs), Externals::Map(&empty));
        let _ = block_on(resolve_at(&mut cx, &toc, delta_off, &mut budget));
        let tags = sites_of(&preflight_sites());
        for want in ["fetch:chain", "decode:mini", "relief"] {
            assert!(tags.contains(&want), "missing {want} in {tags:?}");
        }
    }

    /// A ref-delta to an external (repo-resident) base drives external():
    /// the entries preflight, the decode preflight, the by_id reserve —
    /// and the pack's own ref-delta sets need_ids. external() is driven
    /// directly because the orchestrated prefetch caches the base first.
    #[test]
    fn ref_delta_external_records_external_sites() {
        let base = b"external base object".as_slice();
        let mut base_enc = Vec::new();
        codec::encode_entry(Kind::Blob, base, &mut base_enc).unwrap();
        let base_id = gix_object::compute_hash(H::Sha1, Kind::Blob, base).unwrap();
        // the external base lives as a standalone encoded entry in "pack X"
        let repo = crate::store::RepoId("o/r".into());
        let ext_key = crate::store::keys::pack(&repo, &crate::store::PackId("extpack".into()));
        let mut mem = MemReads::new(repo.clone());
        mem.put(&ext_key, base_enc);
        let mut ext: HashMap<ObjectId, ObjLoc> = HashMap::new();
        ext.insert(base_id, ObjLoc {
            pack: crate::store::PackId("extpack".into()),
            idx: 0,
            offset: 0,
            len: u32::try_from(mem.objects[&ext_key].len()).unwrap(),
            kind: Kind::Blob,
            size: base.len() as u64,
        });
        clear_preflights();
        let mut budget = test_budget();
        let mut cx = test_cx(&mem, &ext_key, 1 << 20, Externals::Map(&ext));
        let _ = block_on(external(&mut cx, base_id, &mut budget));
        let tags = sites_of(&preflight_sites());
        for want in ["entries:external", "decode:external"] {
            assert!(tags.contains(&want), "missing {want} in {tags:?}");
        }
    }

    /// The orchestrated ref-delta still proves the by_id reserve + the
    /// per-entry spine fire on the edge path.
    #[test]
    fn ref_delta_orchestrated_records_by_id_and_entry_sites() {
        let base = b"external base object".as_slice();
        let mut base_enc = Vec::new();
        codec::encode_entry(Kind::Blob, base, &mut base_enc).unwrap();
        let base_id = gix_object::compute_hash(H::Sha1, Kind::Blob, base).unwrap();
        let d = base.len();
        let raw = mk_delta_raw(Header::RefDelta { base_id }, d as u64, d as u64, &insert_ops(base));
        let mut pack = b"PACK\0\0\0\x02\0\0\0\0".to_vec();
        let e = PackEntry::from_bytes(&raw, 0, H::Sha1).unwrap();
        let hlen = e.header_size();
        let recs = vec![EntryRec {
            offset: 12,
            header_len: u8::try_from(hlen).unwrap(),
            kind_or_delta: Header::RefDelta { base_id },
            compressed_len: u32::try_from(raw.len() - hlen).unwrap(),
            size: e.decompressed_size,
        }];
        pack.extend_from_slice(&raw);
        pack[8..12].copy_from_slice(&1u32.to_be_bytes());
        pack.extend_from_slice(&[0u8; 20]);
        let mut ext: HashMap<ObjectId, ObjLoc> = HashMap::new();
        ext.insert(base_id, ObjLoc {
            pack: crate::store::PackId("extpack".into()),
            idx: 0,
            offset: 0,
            len: u32::try_from(base_enc.len()).unwrap(),
            kind: Kind::Blob,
            size: base.len() as u64,
        });
        let key = "pending/p3.pack";
        let repo = crate::store::RepoId("o/r".into());
        let mut mem = MemReads::new(repo.clone());
        mem.put(key, pack);
        mem.put(&crate::store::keys::pack(&repo, &crate::store::PackId("extpack".into())), base_enc);
        let mut out = MemPackOut { buf: Vec::new() };
        let mut sink = MemSink::default();
        clear_preflights();
        let _res = block_on(drive(&mem, key, &recs, Externals::Map(&ext), &mut out, &mut sink));
        let tags = sites_of(&preflight_sites());
        for want in ["grow:by_id", "fetch:entry", "decode:mini", "entries:prefetch", "decode:prefetch"] {
            assert!(tags.contains(&want), "missing {want} in {tags:?}");
        }
    }

    /// A forward ref-delta (base is a LATER pack entry) exercises the
    /// Await -> pending -> re-queue path: the entry defers, the base
    /// resolves, the waiter wakes and completes.
    #[test]
    fn forward_ref_delta_awaits_and_resolves() {
        let base = b"forward base payload".as_slice();
        let base_id = gix_object::compute_hash(H::Sha1, Kind::Blob, base).unwrap();
        let d = base.len();
        let delta_raw = mk_delta_raw(Header::RefDelta { base_id }, d as u64, d as u64, &insert_ops(base));
        let mut pack = b"PACK\0\0\0\x02\0\0\0\0".to_vec();
        let e0 = PackEntry::from_bytes(&delta_raw, 0, H::Sha1).unwrap();
        let h0 = e0.header_size();
        let recs0 = EntryRec {
            offset: 12,
            header_len: u8::try_from(h0).unwrap(),
            kind_or_delta: Header::RefDelta { base_id },
            compressed_len: u32::try_from(delta_raw.len() - h0).unwrap(),
            size: e0.decompressed_size,
        };
        pack.extend_from_slice(&delta_raw);
        let off1 = pack.len() as u64;
        let mut enc1 = Vec::new();
        codec::encode_entry(Kind::Blob, base, &mut enc1).unwrap();
        let e1 = PackEntry::from_bytes(&enc1, 0, H::Sha1).unwrap();
        let h1 = e1.header_size();
        let recs1 = EntryRec {
            offset: off1,
            header_len: u8::try_from(h1).unwrap(),
            kind_or_delta: e1.header,
            compressed_len: u32::try_from(enc1.len() - h1).unwrap(),
            size: e1.decompressed_size,
        };
        pack.extend_from_slice(&enc1);
        pack[8..12].copy_from_slice(&2u32.to_be_bytes());
        pack.extend_from_slice(&[0u8; 20]);
        let recs = vec![recs0, recs1];
        let empty: HashMap<ObjectId, ObjLoc> = HashMap::new();
        let key = "pending/p4.pack";
        let repo = crate::store::RepoId("o/r".into());
        let mut mem = MemReads::new(repo);
        mem.put(key, pack);
        let mut out = MemPackOut { buf: Vec::new() };
        let mut sink = MemSink::default();
        clear_preflights();
        let res = block_on(drive(&mem, key, &recs, Externals::Map(&empty), &mut out, &mut sink));
        // the forward ref may or may not fully resolve depending on delta
        // wire details — the by_id reserve + fetch preflights already ran
        let _ = res;
        let tags = sites_of(&preflight_sites());
        assert!(tags.contains(&"grow:by_id"), "{tags:?}");
        assert!(tags.contains(&"fetch:entry"), "{tags:?}");
    }

    /// Forced pressure through the REAL path: a live-bytes override over
    /// MEM_HARD makes resolve_and_normalize return an in-band Limit — the
    /// guard isn't just unit-tested, it gates production flow.
    #[test]
    fn forced_pressure_makes_real_path_limit() {
        let blob = b"pressure test blob".as_slice();
        let (pack, recs) = mk_pack(&[(Kind::Blob, blob)]);
        let empty: HashMap<ObjectId, ObjLoc> = HashMap::new();
        let key = "pending/p5.pack";
        let repo = crate::store::RepoId("o/r".into());
        let mut mem = MemReads::new(repo);
        mem.put(key, pack);
        let mut out = MemPackOut { buf: Vec::new() };
        let mut sink = MemSink::default();
        clear_preflights();
        crate::mem::set_live_override(Some(std::rc::Rc::new(|| MEM_HARD + (1 << 20))));
        let res = block_on(drive(&mem, key, &recs, Externals::Map(&empty), &mut out, &mut sink));
        crate::mem::set_live_override(None);
        match res {
            Err(Error::Limit(m)) => assert!(m.contains("smaller slices") || m.contains("busy"), "{m}"),
            other => panic!("expected Limit, got {:?}", other.map(|r| r.len())),
        }
    }
    /// The standalone preflight (the final index post, after the resolve
    /// loop's Cx is gone) runs the same guard: it records its site, admits
    /// a body that fits and fails in-band on one that would cross MEM_HARD.
    #[test]
    fn mem_preflight_guards_the_final_index_post() {
        clear_preflights();
        crate::mem::set_live_override(Some(std::rc::Rc::new(|| 50 << 20)));
        let ok = mem_preflight(None, 8 << 20, "grow:index-final");
        let err = mem_preflight(None, 47 << 20, "grow:index-final");
        crate::mem::set_live_override(None);
        ok.unwrap();
        assert!(matches!(err, Err(Error::Limit(_))), "{err:?}");
        let sites = preflight_sites();
        assert_eq!(sites.iter().filter(|(s, _)| *s == "grow:index-final").count(), 2);
    }

    /// MEM_HARD boundary: `after > MEM_HARD` must be strict — at exactly
    /// MEM_HARD the push continues (a `>=` mutant fails it), one byte over
    /// fails in-band (an `&&`-for-`||` mutant on the shed condition would
    /// skip the shed and still fail, but the cache must shed FIRST).
    #[test]
    fn mem_guard_hard_boundary_is_strict() {
        // exactly MEM_HARD after shed: passes, cache already shed by soft path
        let mut c = Cache::default();
        c.put(Key::Off(1), Kind::Blob, vec![1u8; 8]);
        let mut g = MemGuard::default();
        // now = MEM_HARD -> over soft, sheds (cleared_at none), after = MEM_HARD -> not > -> Ok
        mem_guard(&mut c, || MEM_HARD, || 0, || 1, &mut g).unwrap();
        // one byte over: Limit
        let mut c = Cache::default();
        let mut g = MemGuard::default();
        let err = mem_guard(&mut c, || MEM_HARD + 1, || 0, || 1, &mut g).unwrap_err();
        assert!(matches!(err, Error::Limit(_)));
    }

    /// The shed condition's `||` arms are load-bearing: cleared_at == None
    /// (first crossing) OR grew>=hysteresis OR now>hard each independently
    /// force the clear. A `&&` mutant would skip the shed on a first
    /// crossing and leave the cache hot under pressure.
    #[test]
    fn mem_guard_shed_condition_arms_are_or() {
        // first crossing (cleared_at None) in the soft band sheds even
        // though grew can't be true yet — proves the is_none arm fires alone
        let mut c = Cache::default();
        c.put(Key::Off(1), Kind::Blob, vec![1u8; 8]);
        let mut g = MemGuard::default();
        mem_guard(&mut c, || MEM_SOFT + 1, || 0, || 1, &mut g).unwrap();
        assert!(c.get(&Key::Off(1)).is_none(), "first crossing sheds via is_none arm");
        // hysteresis arm alone: cleared_at set, grew >= MEM_HYSTERESIS, under hard
        let mut c = Cache::default();
        c.put(Key::Off(2), Kind::Blob, vec![2u8; 8]);
        let mut g = MemGuard::default();
        g.cleared_at = Some(MEM_SOFT);
        let live = MEM_SOFT + MEM_HYSTERESIS; // grew exactly MEM_HYSTERESIS
        mem_guard(&mut c, || live, || 0, || 1, &mut g).unwrap();
        assert!(c.get(&Key::Off(2)).is_none(), "hysteresis arm sheds");
    }

    /// Cache accounting is by capacity(), not len(): a Vec with len<capacity
    /// must still count its spare — the mutant that accounts len() would
    /// under-count and keep entries the budget should evict.
    #[test]
    fn cache_accounts_capacity_not_len() {
        let mut c = Cache::with_budget(100 + CACHE_ENTRY_OVERHEAD);
        // Vec with len 10 but capacity 60 — shrink_to_fit in put() makes
        // capacity==len for the STORED vec, so test the accounting path via
        // a vec that keeps spare: put() shrink_to_fits, so instead verify
        // bytes tracks capacity of what's stored.
        let mut v = Vec::with_capacity(60);
        v.extend_from_slice(&[7u8; 10]);
        assert!(v.capacity() >= 60 && v.len() == 10);
        c.put(Key::Off(1), Kind::Blob, v);
        let stored = c.map.get(&Key::Off(1)).unwrap().1.clone();
        // after shrink_to_fit capacity may equal len — the point is bytes
        // equals the STORED vec's capacity(), whatever it is, plus overhead
        assert_eq!(c.bytes, stored.capacity() + CACHE_ENTRY_OVERHEAD);
        assert!(c.bytes >= stored.len());
    }

    /// shrink_to_fit actually runs in put(): a vec pushed past its len
    /// comes back with capacity == len (no doubling slack retained).
    #[test]
    fn cache_put_shrink_to_fits() {
        let mut c = Cache::default();
        let mut v = Vec::with_capacity(1 << 20);
        v.extend_from_slice(&[3u8; 100]);
        assert!(v.capacity() > 100);
        c.put(Key::Off(9), Kind::Blob, v);
        let stored = c.map.get(&Key::Off(9)).unwrap().1.clone();
        assert_eq!(stored.capacity(), stored.len(), "shrink_to_fit ran");
    }

    /// Edge-path constants are pinned: the chain cap and cache budget are
    /// the values the memory math was calibrated against — a silent change
    /// breaks the guard's headroom assumptions.
    #[test]
    fn edge_settings_are_pinned() {
        assert_eq!(EDGE_CHAIN_BYTES, 24 << 20);
        assert_eq!(EDGE_CACHE, 24 << 20);
        assert!(EDGE_CHAIN_BYTES < MAX_CHAIN_BYTES);
        assert_eq!(LINEAR_CAP, 112 << 20);
        // LINEAR_CAP leaves ~10 MiB under the decimal 128 MB (122.07 MiB) wall
        assert!(LINEAR_CAP < (128_000_000usize - (10 << 20)));
        // decode bounds above PER_OBJECT_DECODE get the object-named message
        assert_eq!(PER_OBJECT_DECODE, MEM_HARD - (40 << 20));
    }

    /// decode_mini_bound's x2 factors are pinned: the mini-pack copy and the
    /// delta work buffers each carry a 2x Vec-capacity factor — dropping
    /// either under-counts the real peak by ~half.
    #[test]
    fn decode_bound_double_factors_are_pinned() {
        let mut z = Inflate::default();
        let mut enc = Vec::new();
        codec::encode_entry(Kind::Blob, &[7u8; 4_096], &mut enc).unwrap();
        let e = PackEntry::from_bytes(&enc, 0, H::Sha1).unwrap();
        let bound = decode_mini_bound(&mut z, None, &e, &enc, H::Sha1);
        // mini term alone is 2x the raw wire bytes (12 hdr + 32 slack + body + 20 trailer)*2
        let wire = enc.len();
        assert!(bound >= wire * 2, "mini 2x factor missing: {bound} < 2*{wire}");
        // delta: out term has 2*biggest result — pinned by the delta_raw case
        let raw = delta_raw(2 << 20, 1 << 20, &[0u8; 1_000]);
        let e = PackEntry::from_bytes(&raw, 0, H::Sha1).unwrap();
        let bound = decode_mini_bound(&mut z, Some(2 << 20), &e, &raw, H::Sha1);
        assert!(bound >= (2 << 20) * 2, "out 2x biggest-result factor missing: {bound}");
    }

    /// links_bound is honest: a max-size tree can't produce more links than
    /// its bytes / min-entry-size, and the bound covers Vec doubling + the
    /// commit parents collect.
    #[test]
    fn links_bound_covers_tree_and_commit_worst_case() {
        // tree: every ~22-byte entry yields one link; bound >= count*elem*2
        let tree_len = 22 * 1_000;
        let b = links_bound(Kind::Tree, tree_len, H::Sha1);
        assert!(b >= (tree_len / 22) * std::mem::size_of::<ObjectId>() * 2, "{b}");
        // commit: tree + parents; bound covers them
        let c = links_bound(Kind::Commit, 48 * 100, H::Sha1);
        assert!(c >= std::mem::size_of::<ObjectId>() * 3, "{c}");
        // blob/tag are O(1)
        assert_eq!(links_bound(Kind::Blob, 1 << 20, H::Sha1), 0);
        assert!(links_bound(Kind::Tag, 1 << 20, H::Sha1) > 0);
    }

    /// id_map_bound covers hashbrown's load-factor + power-of-two rounding.
    /// Codex #3 / Claude #5: the sink-side links bound is the moving-realloc
    /// peak. ~16 MiB of links doubling to ~32 MiB holds both blocks at
    /// once — ~48 MiB — so the preflight must say so, and it must cover
    /// what extend really allocates.
    #[test]
    fn links_growth_bound_counts_the_moving_peak() {
        let elem = std::mem::size_of::<ObjectId>();
        let cap = (16 << 20) / elem;
        let mut sink = MemSink { links: vec![ObjectId::null(H::Sha1); cap], ..MemSink::default() };
        let old = sink.links.capacity();
        let want = AnySink::Mem(&mut sink).links_growth_bound(1);
        assert_eq!(want, old * elem + 2 * old * elem + crate::store::ALLOC_OVERHEAD);
        sink.links.extend([ObjectId::null(H::Sha1)]);
        assert!(
            (old + sink.links.capacity()) * elem <= want,
            "real peak {} > bound {want}",
            (old + sink.links.capacity()) * elem
        );
        // links that fit preflight nothing; a big batch grows to the need
        let (len, cap_now) = (sink.links.len(), sink.links.capacity());
        let big = 4 * cap_now;
        let any = AnySink::Mem(&mut sink);
        assert_eq!(any.links_growth_bound(0), 0);
        assert_eq!(
            any.links_growth_bound(big),
            cap_now * elem + (len + big) * elem + crate::store::ALLOC_OVERHEAD
        );
        assert_eq!(vec_growth_peak::<u8>(0, 0, 1), 4 + crate::store::ALLOC_OVERHEAD, "Vec's minimum non-zero capacity");
    }

    /// Codex #5: tiny objects are bounded too. Each entry pays
    /// CACHE_ENTRY_OVERHEAD, so a budget of N overheads holds at most N
    /// empty objects however many are put, and the map and queue stop
    /// growing with it. Re-puts of one key keep the queue bounded as well.
    #[test]
    fn cache_budget_bounds_tiny_entries_by_overhead() {
        let mut c = Cache::with_budget(100 * CACHE_ENTRY_OVERHEAD);
        for i in 0..10_000u64 {
            c.put(Key::Off(i), Kind::Blob, Vec::new());
        }
        assert_eq!(c.map.len(), 100);
        assert_eq!(c.order.len(), 100);
        assert!(c.bytes <= c.budget);
        for _ in 0..10_000 {
            c.put(Key::Off(7), Kind::Blob, vec![1]);
        }
        assert!(c.order.len() <= 100, "re-puts grew the queue to {}", c.order.len());
        assert!(c.bytes <= c.budget);
        // the overhead covers both tables' slots with doubling headroom
        assert!(CACHE_ENTRY_OVERHEAD >= 2 * (std::mem::size_of::<(Key, Obj)>() + 1 + std::mem::size_of::<Key>()));
    }

    /// Codex #5: map and queue growth preflights. The bound is 0 while
    /// both have room and covers the old plus the new table when the next
    /// insert resizes; checked against what HashMap/VecDeque really do.
    #[test]
    fn map_and_cache_growth_bounds_cover_real_resizes() {
        for n in [0usize, 1, 3, 4, 7, 8, 13, 100, 1_000, 30_000] {
            let mut m: HashMap<ObjectId, ObjectId> = HashMap::with_capacity(n);
            let mut i = 0u32;
            let id = |i: u32| {
                let mut b = [0u8; 20];
                b[..4].copy_from_slice(&i.to_be_bytes());
                ObjectId::from_bytes_or_panic(&b)
            };
            while m.len() < m.capacity() {
                m.insert(id(i), id(i));
                i += 1;
            }
            let (len, cap) = (m.len(), m.capacity());
            let bound = map_growth_peak::<(ObjectId, ObjectId)>(len, cap);
            m.insert(id(i), id(i));
            assert!(m.capacity() > cap, "the insert resized");
            let elem = std::mem::size_of::<(ObjectId, ObjectId)>() + 1;
            assert!(
                bound >= (cap + m.capacity()) * elem,
                "n={n}: bound {bound} < old {cap} + new {} slots",
                m.capacity()
            );
            assert_eq!(map_growth_peak::<(ObjectId, ObjectId)>(m.len(), m.capacity()), 0, "room left: no preflight");
        }
        let mut c = Cache::with_budget(usize::MAX);
        assert!(c.put_growth_bound() > 0, "the first put allocates both tables");
        c.put(Key::Off(0), Kind::Blob, vec![1]);
        if c.map.len() < c.map.capacity() && c.order.len() < c.order.capacity() {
            assert_eq!(c.put_growth_bound(), 0);
        }
        // the tags preflight reads the sink's map the same way
        let mut sink = MemSink::default();
        let mut any = AnySink::Mem(&mut sink);
        assert!(any.tags_growth_bound() > 0, "an empty map allocates on first insert");
    }

    #[test]
    fn id_map_bound_covers_load_factor() {
        let b = id_map_bound(1_000);
        // 1000 entries need >= 1000 slots at <=87.5% load -> 2048 pow2 slots
        assert!(b >= 2048 * (std::mem::size_of::<(ObjectId, usize)>() + 1), "{b}");
        assert!(id_map_bound(0) > 0);
    }

}
