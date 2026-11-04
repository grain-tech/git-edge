//! Pass A (stream_to_pending) and pass B (resolve_and_normalize) of ingest (CONTRACTS.md 2.4, A10).
//! Runs in the edge Worker. Ported from proofs-v2/streaming-pack-parser.md.

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
use crate::store::{codec, keys, Bucket, ObjLoc, ObjRow, PackWriter, PushId, RawWriter};
use crate::ReqBudget;

use super::run::IndexSink;


#[derive(Clone, Copy)]
pub struct EntryRec {
    pub offset: u64,
    pub header_len: u8,
    pub kind_or_delta: Header,
    pub compressed_len: u32,
    pub size: u64,
}

// EntryRec is ~48 B (offset + Header enum + two lengths + u64 size): the record vec
// alone must leave room for Cache (48 MiB) + window + part buffer inside a 128 MiB
// isolate — 750k records ≈ 36 MiB keeps the worst-case sum comfortably under it
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
const CACHE: usize = 48 << 20; // 16 MiB resolved LRU + 32 MiB external bases as one cache
const MAX_DEPTH: usize = 64; // git's default delta depth is 50
const MAX_CHAIN_BYTES: u64 = 64 << 20; // chain holds raw compressed delta bodies

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
    let (mut recs, mut pos, mut z, mut sink) = (Vec::new(), 12u64, Decompress::new(), vec![0u8; 64 << 10]);
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

#[derive(Default)]
pub struct Cache {
    bytes: usize,
    map: HashMap<Key, Obj>,
    order: VecDeque<Key>,
}
impl Cache {
    fn get(&self, k: &Key) -> Option<Obj> {
        self.map.get(k).cloned()
    }
    fn put(&mut self, k: Key, kind: Kind, d: Rc<Vec<u8>>) -> Obj {
        self.bytes = self.bytes.saturating_add(d.len());
        self.order.push_back(k);
        self.map.insert(k, (kind, d.clone()));
        while self.bytes > CACHE {
            match self.order.pop_front().and_then(|k| self.map.remove(&k)) {
                Some((_, v)) => self.bytes = self.bytes.saturating_sub(v.len()),
                None => break,
            }
        }
        (kind, d)
    }
}

/// Where entry bytes live: one R2 object (push pending pack) or a list of
/// staged part objects read as one concatenated stream (I1 import).
pub enum Source<'a> {
    Key(&'a str),
    /// (key, length) pairs in order — offsets index into the concatenation.
    Parts(&'a [(String, u64)]),
}
impl Source<'_> {
    async fn read(&self, bucket: &Bucket, off: u64, n: u64, budget: &mut ReqBudget) -> Result<Vec<u8>, Error> {
        match self {
            Source::Key(k) => bucket.read_range(k, off, n, budget).await,
            Source::Parts(parts) => {
                let mut out = Vec::with_capacity(usize::try_from(n).unwrap_or(0));
                let want_end = off.saturating_add(n);
                let mut base = 0u64;
                for (key, len) in *parts {
                    let end = base.saturating_add(*len);
                    let (a, b) = (off.max(base), want_end.min(end));
                    if a < b {
                        out.extend_from_slice(&bucket.read_range(key, a - base, b - a, budget).await?);
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
    pub bucket: &'a Bucket,
    pub src: Source<'a>,
    pub pack_len: u64,
    pub start: u64,
    pub buf: Vec<u8>,
}
impl Window<'_> {
    /// Raw bytes of one entry; slides to [offset, +8 MiB) with one range read when outside.
    async fn entry(&mut self, rec: &EntryRec, budget: &mut ReqBudget) -> Result<Vec<u8>, Error> {
        let len = u64::from(rec.header_len).saturating_add(u64::from(rec.compressed_len));
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
                let n = len.max(WINDOW).min(self.pack_len.saturating_sub(rec.offset));
                self.buf = self.src.read(self.bucket, rec.offset, n, budget).await?;
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
        self.src.read(self.bucket, off, n, budget).await
    }
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
}
impl AnySink<'_, '_> {
    pub async fn post(&mut self, rows: &[ObjRow], budget: &mut ReqBudget) -> Result<(), Error> {
        match self {
            AnySink::Edge(s) => s.post(rows, budget).await,
            AnySink::Job(s) => s.post(rows, budget).await,
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
        }
    }
    pub fn tags_mut(&mut self) -> &mut HashMap<ObjectId, ObjectId> {
        match self {
            AnySink::Edge(s) => &mut s.tags,
            AnySink::Job(s) => &mut s.tags,
        }
    }
    /// Total links seen — the A5 bound applies to both shapes.
    pub fn links_total(&self) -> usize {
        match self {
            AnySink::Edge(s) => s.links.len(),
            AnySink::Job(s) => s.links_total,
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
        }
    }
    /// The entry's wire span is complete: record its end offset and (for blobs)
    /// its id. Writers ≤ the durable boundary with an end get direct-inserted on
    /// resume; without one they're the mid-write straddler.
    pub fn seal(&mut self, idx: usize, end: u64, sha: ObjectId) -> Result<(), Error> {
        match self {
            AnySink::Edge(_) => Ok(()),
            AnySink::Job(s) => s.seal(idx, end, sha),
        }
    }
}

/// DO-side sink for the import job: objects rows and links batches go straight
/// to SQLite — no stub, no subrequests, and links spill past the in-memory cap.
pub struct JobSink<'a> {
    pub sql: &'a worker::SqlStorage,
    pub pack: crate::store::PackId,
    pub push: &'a str,
    pub links_buf: Vec<ObjectId>,
    pub links_total: usize,
    pub tags: HashMap<ObjectId, ObjectId>,
}
impl JobSink<'_> {
    pub async fn post(&mut self, rows: &[ObjRow], _budget: &mut ReqBudget) -> Result<(), Error> {
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
        for chunk in self.links_buf.chunks(50) {
            let marks = std::iter::repeat("(?,?)").take(chunk.len()).collect::<Vec<_>>().join(",");
            let mut args = Vec::with_capacity(chunk.len() * 2);
            for id in chunk {
                args.push(worker::SqlStorageValue::from(self.push));
                args.push(worker::SqlStorageValue::from(id.to_string().as_str()));
            }
            crate::store::exec(
                self.sql,
                &format!("INSERT OR IGNORE INTO push_links(push_id,sha) VALUES{marks}"),
                args,
            )?;
        }
        self.links_buf.clear();
        Ok(())
    }
    /// entry idx -> output start + shadow row cols. Written at first append, so a
    /// crash resume can name the entry whose bytes straddle the durable boundary —
    /// attempt-order markers can't (a parked entry's prospective start collides
    /// with the entry that actually writes there).
    fn mark(&mut self, idx: usize, off: u64, sha: Option<ObjectId>, kind: Kind, size: u64) -> Result<(), Error> {
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
}

/// One decode_entry over `[PACK v2][base as a level-0 zlib full entry][delta re-headed as ofs-delta][digest-width zero trailer]`.
pub fn decode_mini(
    z: &mut Inflate,
    base: Option<(Kind, &[u8])>,
    raw: &[u8],
    obj_kind: H,
) -> Result<(Kind, Vec<u8>), Error> {
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
    let (at, e) = (
        mini.len() as u64,
        PackEntry::from_bytes(raw, 0, obj_kind).map_err(|e| unpack(e.to_string()))?,
    );
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
            z,
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
    let (_, entry) = cx
        .win
        .bucket
        .read_entries(&[(id, loc)], budget)
        .await?
        .pop()
        .ok_or_else(|| Error::Storage("base read".into()))?;
    let (k, d) = codec::decode_entry(&entry)?;
    Ok(cx.cache.put(Key::Id(id), k, Rc::new(d)))
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
        let raw = cx.win.entry(&rec, budget).await?;
        // the chain holds each delta's compressed bytes: 64 × ~16 MiB worst case is a
        // GiB-scale allocation — bound the bytes, not just the depth. chain_live bounds
        // the SAME memory summed across every nested resolve_at frame.
        chain_bytes = chain_bytes.saturating_add(raw.len() as u64);
        cx.chain_live = cx.chain_live.saturating_add(raw.len() as u64);
        if chain_bytes > MAX_CHAIN_BYTES || cx.chain_live > MAX_CHAIN_LIVE {
            cx.chain_live = cx.chain_live.saturating_sub(chain_bytes);
            return Err(unpack("delta chain too large"));
        }
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
                let (k, d) = decode_mini(&mut cx.z, None, &raw, cx.obj_kind)?;
                break cx.cache.put(Key::Off(off), k, Rc::new(d));
            }
        }
    };
    while let Some((o, raw)) = chain.pop() {
        let (_, d) = decode_mini(&mut cx.z, Some((kind, &data)), &raw, cx.obj_kind)?;
        data = cx.cache.put(Key::Off(o), kind, Rc::new(d)).1;
    }
    cx.chain_live = cx.chain_live.saturating_sub(chain_bytes);
    Ok(Base::Ready((kind, data)))
}

/// `sink` posts `/_do/push/index` in 10,000-row batches and collects links (2.5).
/// `externals` is the thin-pack base view — a prefetched map here, the live index
/// itself for an in-DO job (see jobs/import.rs).
pub async fn resolve_and_normalize(
    bucket: &Bucket,
    pending_key: &str,
    entries: &[EntryRec],
    externals: Externals<'_>,
    out: &mut PackWriter,
    sink: &mut IndexSink<'_>,
    obj_kind: gix_hash::Kind,
    budget: &mut ReqBudget,
) -> Result<Vec<ObjRow>, Error> {
    let mut sink = AnySink::Edge(sink);
    let ds = u64::try_from(obj_kind.len_in_bytes()).map_err(internal)?;
    let pack_len = entries.last().map_or(12 + ds, |r| {
        r.offset
            .saturating_add(u64::from(r.header_len))
            .saturating_add(u64::from(r.compressed_len))
            .saturating_add(ds)
    });
    let mut cx = Cx {
        win: Window { bucket, src: Source::Key(pending_key), pack_len, start: 0, buf: Vec::new() },
        cache: Cache::default(),
        z: Inflate::default(),
        by_id: IdMap::Map(HashMap::new()),
        externals,
        obj_kind,
        res_depth: 0,
        chain_live: 0,
    };
    let (mut pre, mut sum) = (Vec::new(), 0u64);
    if let Externals::Map(bases) = &cx.externals {
    for (id, loc) in *bases {
        // oversized streamed bases must not be decoded here — external() reports the
        // actionable "delta base too large" error; decode_entry would throw its bare
        // object-too-large limit first
        if loc.size <= MAX_OBJ {
            sum = sum.saturating_add(u64::from(loc.len));
            if sum > 32 << 20 {
                break;
            }
            pre.push((*id, loc.clone()));
        }
    }
    }
    // prefetch is opportunistic: `read_entries` bounds *merged span* bytes (gaps
    // included), which `pre`'s plain size sum can't predict — on Limit just skip the
    // batch; every base still resolves lazily through `external()`
    if let Ok(entries) = bucket.read_entries(&pre, budget).await {
        for (id, entry) in entries {
            let (k, d) = codec::decode_entry(&entry)?;
            cx.cache.put(Key::Id(id), k, Rc::new(d));
        }
    }
    let need_ids = entries.iter().any(|r| matches!(r.kind_or_delta, Header::RefDelta { .. }));
    let toc = Toc::Mem(entries);
    let mut rows = Vec::new();
    // A REF_DELTA may name a base that appears LATER in the same pack — including
    // mid-chain inside another delta. `one_entry` returns `Await(base)` for those;
    // resolving an entry wakes exactly its waiters (index-pack parity, O(n) not O(n^2)).
    let mut pending: HashMap<ObjectId, Vec<usize>> = HashMap::new();
    let mut order: Vec<usize> = (0..entries.len()).collect();
    let mut i = 0;
    while i < order.len() {
        let at = order[i];
        i += 1;
        match one_entry(&mut cx, &toc, at, need_ids, out, &mut sink, &mut rows, budget, 0).await? {
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
pub async fn one_entry(
    cx: &mut Cx<'_>,
    entries: &Toc<'_>,
    i: usize,
    need_ids: bool,
    out: &mut PackWriter,
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
    let raw = cx.win.entry(&rec, budget).await?;
    let (kind, data) =
        decode_mini(&mut cx.z, base.as_ref().map(|(k, d)| (*k, d.as_slice())), &raw, cx.obj_kind)?;
    let id = gix_object::compute_hash(cx.obj_kind, kind, &data).map_err(|_| unpack("hash collision"))?;
    let links = extract_links(kind, &data, cx.obj_kind)?;
    // protected refs: extract_links puts a commit's tree first, then parents
    let parents = (kind == Kind::Commit).then(|| links.iter().skip(1).copied().collect::<Vec<_>>());
    if kind == Kind::Tag {
        if let Some(&t) = links.first() {
            sink.tags_mut().insert(id, t);
        }
    }
    // bound inside the loop too — a single giant tree can spike `links` far past
    // the limit between post() batches otherwise
    sink.push_links(links)?;
    sink.mark(i, out.offset().saturating_sub(skip), Some(id), kind, data.len() as u64)?;
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
    cx.cache.put(Key::Off(rec.offset), kind, Rc::new(data));
    if rows.len() >= 10_000 {
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
async fn stream_blob(
    cx: &mut Cx<'_>,
    rec: &EntryRec,
    i: usize,
    need_ids: bool,
    out: &mut PackWriter,
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
        out.raw_extend(&hdr[s..]);
        skip -= s as u64;
    }
    let (mut z, mut h, mut sinkbuf) = (Decompress::new(), gix_hash::hasher(cx.obj_kind), vec![0u8; 1 << 20]);
    h.update(format!("blob {}\0", rec.size).as_bytes());
    let (mut pos, mut produced, mut st) = (body_start, 0u64, Status::Ok);
    while pos < end && st != Status::StreamEnd {
        let n = end.saturating_sub(pos).min(WINDOW);
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
        out.raw_extend(&chunk[s..]);
        skip -= s as u64;
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
    if rows.len() >= 10_000 {
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
