//! CONTRACTS.md 1.2, 2.1-2.3: R2 key layout, pack entry codec, SQLite index, MemFind,
//! Bucket (coalesced range reads), PackWriter / RawWriter (multipart), Index.

use std::collections::HashMap;

use gix_hash::ObjectId;
use gix_object::Kind;
use serde::{Deserialize, Serialize};
use worker::{MultipartUpload, SqlStorage, SqlStorageValue as V, UploadedPart};

use crate::error::Error;
use crate::platform;
use crate::ReqBudget;

pub const PART: usize = 8 << 20; // 6.4: every part exactly 8 MiB except the last
/// R2's minimum non-final part — a shrunken test part may approach, never pass below.
pub const PART_MIN: usize = 5 << 20;
pub const MAX_OBJECT: u64 = 16 << 20; // A7: single-object cap, inflated

/// The capacity `room` would grow `buf` to for `extra` incoming bytes:
/// whole-part steps, so capacity stays a multiple of `part` (one part plus
/// at most the overshoot append) instead of Vec's doubling holding ~2x
/// across a flush boundary. `part` must be > 0. Returns `cap` unchanged
/// when the need already fits — i.e. no growth.
fn room_target(len: usize, cap: usize, part: usize, extra: usize) -> usize {
    let need = len.saturating_add(extra);
    if need <= cap {
        return cap;
    }
    let step = part.max(1);
    need.div_ceil(step).saturating_mul(step)
}

/// Grow `buf` for `extra` incoming bytes in whole-part steps. The growth
/// is fallible (`try_reserve_exact`): a block the allocator cannot place
/// returns the in-band memory Limit, and the caller's write that follows
/// then fits the capacity without growing.
pub(crate) fn room(buf: &mut Vec<u8>, part: usize, extra: usize) -> Result<(), Error> {
    let target = room_target(buf.len(), buf.capacity(), part, extra);
    if target > buf.capacity() {
        crate::try_grow::vec_exact(buf, target.saturating_sub(buf.len()))?;
    }
    Ok(())
}

/// The writer counters `resume_tail` folds — grouped so the helper stays
/// readable at the call site.
#[derive(Clone, Copy, Debug)]
struct TailState {
    offset: u64,
    count: u32,
    commit_lo: u64,
    commit_hi: u64,
}

/// `resume_tail`'s state transition, factored so the reservation order is
/// testable natively: the room the CLEARED buffer would need is reserved
/// BEFORE `part` is cleared, so a refused reservation changes nothing —
/// the buffered tail bytes and every counter the caller keeps stay as
/// they were. Returns the writer's new counters only on success.
fn resume_tail_apply(
    part: &mut Vec<u8>,
    part_size: usize,
    bytes: &[u8],
    st: TailState,
    extra: u32,
    lo: u64,
    hi: u64,
) -> Result<TailState, Error> {
    let new_offset = st
        .offset
        .saturating_sub(part.len() as u64)
        .saturating_add(bytes.len() as u64);
    // reserve exactly what room() would allocate for the cleared buffer —
    // sized against len 0, not the bytes about to be dropped
    let target = room_target(0, part.capacity(), part_size, bytes.len());
    if target > part.capacity() {
        crate::try_grow::vec_exact(part, target.saturating_sub(part.len()))?;
    }
    part.clear();
    part.extend_from_slice(bytes);
    Ok(TailState {
        offset: new_offset,
        count: st.count.saturating_add(extra),
        commit_lo: st.commit_lo.min(lo),
        commit_hi: st.commit_hi.max(hi),
    })
}

/// Per-allocation overhead a growing buffer can add on top of its byte
/// capacity: the allocator's chunk header and alignment rounding, and
/// `memory.grow` handing out whole 64 KiB wasm pages. One page covers all
/// three for a single allocation.
pub(crate) const ALLOC_OVERHEAD: usize = 64 << 10;

/// The bytes `room` adds to the heap for `extra` incoming bytes — the
/// memory guard's preflight size for append paths. 0 when the need
/// already fits (no allocation). Otherwise the bound is the new block plus
/// ALLOC_OVERHEAD (see `growth_incoming`): the old block is already inside
/// the live and linear readings the guard adds this to.
pub(crate) fn room_growth(buf: &Vec<u8>, part: usize, extra: usize) -> usize {
    let cap = buf.capacity();
    let target = room_target(buf.len(), cap, part, extra);
    if target <= cap {
        return 0;
    }
    growth_incoming(target)
}

/// The incoming bytes a growth to `new_cap` bytes adds on top of what the
/// guard already reads. A moving reallocation holds the old and the new
/// block at once, but the old block is already counted in live bytes and
/// sits inside linear memory, so the peak is live + new_cap. On the linear
/// side the worst case is the same: the new block needs at most new_cap
/// bytes of fresh pages. One ALLOC_OVERHEAD covers the allocator's header,
/// rounding and page granularity.
pub(crate) fn growth_incoming(new_cap: usize) -> usize {
    new_cap.saturating_add(ALLOC_OVERHEAD)
}

/// The bytes a writer's finish adds to the heap before its last upload,
/// for a part buffer `part`, a uniform part size and a `ds`-byte trailer:
/// the trailer's whole-part growth of the buffer (`room_growth`), plus the
/// split's exact-size head copy when the buffer and the trailer reach a
/// whole part. The last part is moved out of the buffer, not copied.
pub(crate) fn finish_growth(part: &Vec<u8>, part_size: usize, ds: usize) -> usize {
    let grow = room_growth(part, part_size, ds);
    let split = if part.len().saturating_add(ds) >= part_size { growth_incoming(part_size) } else { 0 };
    grow.saturating_add(split)
}

/// Upper bound on `codec::encode_entry`'s appended bytes for `n` input bytes:
/// zlib's deflateBound shape (n + n/4096 + n/16384 + n/33M + 13) covers
/// deflate's worst case, and the flat slack covers the varint header and
/// rounding — used to pre-size `part` so the encoder's own growth never
/// doubles past the reserved whole parts.
pub(crate) fn encoded_bound(n: usize) -> usize {
    n.saturating_add(n >> 12)
        .saturating_add(n >> 14)
        .saturating_add(n >> 25)
        .saturating_add(13)
        .saturating_add(320)
}

/// How a PackWriter finish treats its multipart upload on failure.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Finish {
    /// A push: nothing resumes it, so a failure aborts the upload rather
    /// than leave it and its parts orphaned in R2.
    Ordinary,
    /// The import job: the upload is a checkpoint a retry resumes, so a
    /// failure keeps it.
    Resumable,
}

/// The seal's last budget charge before complete(). On failure, returns
/// the error and whether to abort the upload: an ordinary finish must not
/// drop the writer silently (abort best-effort, like every other finish
/// error path); a resumable finish keeps its checkpointed upload.
fn seal_charge(budget: &mut ReqBudget, mode: Finish) -> Result<(), (Error, bool)> {
    budget.charge(1).map_err(|e| (e, mode == Finish::Ordinary))
}

/// Split `buf` at `n`: returns a tight `n`-byte Vec for upload while `buf`
/// keeps the tail in its existing allocation. Always the copy-out form —
/// the head is exact-size so the chunk held across `upload_part` never
/// retains a part-sized-plus allocation for one part of data, and neither
/// branch holds capacity + a second full-size copy: the transient is the
/// head's `n` bytes on top of `buf`'s existing capacity.
/// The head copy is fallible: on failure `buf` is unchanged.
/// Caller guarantees `n <= buf.len()`.
pub(crate) fn split_part(buf: &mut Vec<u8>, n: usize) -> Result<Vec<u8>, Error> {
    let head = crate::try_grow::copy(&buf[..n])?;
    buf.copy_within(n.., 0);
    buf.truncate(buf.len() - n);
    Ok(head)
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct RepoId(pub String);
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PackId(pub String);
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PushId(pub String);

impl PackId {
    pub fn random() -> Result<Self, Error> {
        Ok(Self(platform::hex16()?))
    }
}
impl PushId {
    pub fn random() -> Result<Self, Error> {
        Ok(Self(platform::hex16()?))
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ObjLoc {
    pub pack: PackId,
    pub idx: u32,
    pub offset: u64,
    pub len: u32,
    #[serde(with = "kind_serde")]
    pub kind: Kind,
    pub size: u64,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ObjRow {
    pub sha: ObjectId,
    pub idx: u32,
    pub offset: u64,
    pub len: u32,
    #[serde(with = "kind_serde")]
    pub kind: Kind,
    pub size: u64,
    /// Protected refs: a commit's parent ids, known only where the object was
    /// decoded (receive-pack / import resolve). None = not recorded (GC restage,
    /// legacy rows) — a fast-forward walk that meets one cannot prove ancestry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parents: Option<Vec<ObjectId>>,
}

mod kind_serde {
    use gix_object::Kind;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    pub fn serialize<S: Serializer>(k: &Kind, s: S) -> Result<S::Ok, S::Error> {
        let n: u8 = match k {
            Kind::Commit => 1,
            Kind::Tree => 2,
            Kind::Blob => 3,
            Kind::Tag => 4,
        };
        n.serialize(s)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Kind, D::Error> {
        match u8::deserialize(d)? {
            1 => Ok(Kind::Commit),
            2 => Ok(Kind::Tree),
            3 => Ok(Kind::Blob),
            4 => Ok(Kind::Tag),
            _ => Err(serde::de::Error::custom("bad kind")),
        }
    }
}

pub fn git_kind(k: Kind) -> u8 {
    match k {
        Kind::Commit => 1,
        Kind::Tree => 2,
        Kind::Blob => 3,
        Kind::Tag => 4,
    }
}
pub fn kind_of(k: u8) -> Result<Kind, Error> {
    match k {
        1 => Ok(Kind::Commit),
        2 => Ok(Kind::Tree),
        3 => Ok(Kind::Blob),
        4 => Ok(Kind::Tag),
        k => Err(Error::Internal(format!("bad kind {k} in objects"))),
    }
}

/// hex -> ObjectId at a data boundary — bad hex is Protocol (client input and
/// stored rows alike). Re-exported as repo_do::oid for its many call sites.
pub fn oid(hex: &str) -> Result<ObjectId, Error> {
    ObjectId::from_hex(hex.as_bytes()).map_err(|e| Error::Protocol(e.to_string()))
}

/// A row of the canonical objects(-join-packs) SELECT — sha,pack_id,idx,offset,
/// len,kind,size as stored. `loc` is the single row -> (ObjectId, ObjLoc)
/// converter for readers that need R2 locations (GC mark/build, test fsck).
#[derive(Deserialize)]
pub struct ObjSqlRow {
    pub sha: String,
    pub pack_id: String,
    pub idx: i64,
    pub offset: i64,
    pub len: i64,
    pub kind: i64,
    pub size: i64,
}
impl ObjSqlRow {
    pub fn loc(&self) -> Result<(ObjectId, ObjLoc), Error> {
        let u = |n: i64| -> Result<u64, Error> {
            u64::try_from(n).map_err(|_| Error::Internal("negative integer in objects".into()))
        };
        Ok((
            oid(&self.sha)?,
            ObjLoc {
                pack: PackId(self.pack_id.clone()),
                idx: u32::try_from(self.idx).map_err(|_| Error::Internal("idx".into()))?,
                offset: u(self.offset)?,
                len: u32::try_from(self.len).map_err(|_| Error::Internal("len".into()))?,
                kind: kind_of(u8::try_from(self.kind).map_err(|_| Error::Internal("kind".into()))?)?,
                size: u(self.size)?,
            },
        ))
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum PackState {
    Ingesting,
    Live,
    Dead,
}
impl PackState {
    pub fn as_str(&self) -> &'static str {
        match self {
            PackState::Ingesting => "ingesting",
            PackState::Live => "live",
            PackState::Dead => "dead",
        }
    }
}

#[derive(Clone)]
pub struct PackMeta {
    pub pack: PackId,
    pub push_id: Option<String>,
    pub count: u32,
    pub bytes: u64,
    pub commit_lo: u64,
    pub commit_hi: u64,
    pub created_at: i64,
}
impl PackMeta {
    /// Posted before the first part is uploaded so a completed pack never lacks a row (two-phase-push).
    pub const EMPTY: PackMeta = PackMeta {
        pack: PackId(String::new()),
        push_id: None,
        count: 0,
        bytes: 0,
        commit_lo: u64::MAX,
        commit_hi: 0,
        created_at: 0,
    };
}

pub mod keys {
    use super::{PackId, PushId, RepoId};
    pub fn pack(repo: &RepoId, pack: &PackId) -> String {
        format!("r/{}/packs/{}.pack", repo.0, pack.0)
    }
    pub fn pending(repo: &RepoId, push: &PushId) -> String {
        format!("r/{}/pending/{}.pack", repo.0, push.0)
    }
    /// I1 staged-part key — shares the push's `pending/<id>.` prefix so a janitor
    /// prefix sweep reaps every part with the push.
    pub fn pending_part(repo: &RepoId, push: &PushId, part: &str) -> String {
        format!("r/{}/pending/{}.part-{}", repo.0, push.0, part)
    }
    /// LFS object content (#21) — sits under the repo's prefix so purge_repo's
    /// `r/<id>/` sweep reaps it with everything else.
    pub fn lfs(repo: &RepoId, oid: &str) -> String {
        format!("r/{}/lfs/{}", repo.0, oid)
    }
}

/// Sync, no `worker` imports (1.2).
pub mod codec {
    use gix_hash::Kind as HashKind;
    use gix_object::Kind;
    use gix_pack::data::{entry::Header, Entry};
    use std::io::Write;

    use super::MAX_OBJECT;
    use crate::error::Error;

    /// One full (non-delta) entry: varint(kind,size) + zlib(data) (2.1).
    pub fn encode_entry(kind: Kind, data: &[u8], out: &mut Vec<u8>) -> Result<(), Error> {
        let hdr = match kind {
            Kind::Commit => Header::Commit,
            Kind::Tree => Header::Tree,
            Kind::Blob => Header::Blob,
            Kind::Tag => Header::Tag,
        };
        let size = u64::try_from(data.len()).map_err(|_| Error::Internal("size".into()))?;
        hdr.write_to(size, out).map_err(|e| Error::Internal(format!("entry header: {e}")))?;
        let mut z = gix_zlib::stream::deflate::Write::new(out, gix_zlib::Compression::DEFAULT);
        z.write_all(data)
            .and_then(|_| std::io::Write::flush(&mut z))
            .map(|_| ())
            .map_err(|e| Error::Internal(format!("deflate: {e}")))
    }
    /// (kind, inflated size, header length) of the entry at bytes[0]. A delta header breaks the
    /// normalized-pack invariant: Error::Internal, not a client error (section 10).
    /// The hash kind is load-bearing only for delta base ids — which this
    /// rejects anyway — so the parameter stays sha1 for both repo formats.
    pub fn entry_header(bytes: &[u8]) -> Result<(Kind, u64, usize), Error> {
        let e = Entry::from_bytes(bytes, 0, HashKind::Sha1)
            .map_err(|e| Error::Storage(format!("entry: {e}")))?;
        let kind = e
            .header
            .as_kind()
            .ok_or_else(|| Error::Internal("delta entry in a normalized pack".into()))?;
        Ok((kind, e.decompressed_size, e.header_size()))
    }
    pub fn decode_entry(bytes: &[u8]) -> Result<(Kind, Vec<u8>), Error> {
        decode_entry_counted(bytes).map(|(k, d, _)| (k, d))
    }
    /// (kind, data, consumed bytes incl. header): the fsck oracle verifies the
    /// entry fills its indexed extent exactly — the loose form stays for
    /// callers that only need the payload.
    pub fn decode_entry_counted(bytes: &[u8]) -> Result<(Kind, Vec<u8>, usize), Error> {
        let (kind, size, hlen) = entry_header(bytes)?;
        if size > MAX_OBJECT {
            return Err(Error::Limit("object too large (16 MiB max)".into()));
        }
        let z = bytes.get(hlen..).ok_or_else(|| Error::Storage("truncated entry".into()))?;
        // the inflate target is the decode's one large block: fallible
        let mut out = crate::try_grow::zeroed(usize::try_from(size).map_err(|_| Error::Limit("size".into()))?)?;
        let (status, used, n) = gix_zlib::Inflate::default()
            .once(z, &mut out)
            .map_err(|e| Error::Storage(format!("inflate: {e}")))?;
        if status != gix_zlib::Status::StreamEnd || n != out.len() {
            return Err(Error::Storage("entry size does not match its header".into()));
        }
        Ok((kind, out, hlen + used))
    }
}

#[derive(Default)]
pub struct MemFind {
    objs: HashMap<ObjectId, (Kind, Vec<u8>)>,
    pub bytes: usize,
}
impl MemFind {
    pub fn insert(&mut self, id: ObjectId, kind: Kind, data: Vec<u8>) {
        self.bytes = self.bytes.saturating_add(data.len());
        self.objs.insert(id, (kind, data));
    }
    pub fn clear(&mut self) {
        self.objs.clear();
        self.bytes = 0;
    }
    pub fn get(&self, id: &ObjectId) -> Option<(Kind, &[u8])> {
        self.objs.get(id).map(|(k, d)| (*k, d.as_slice()))
    }
    pub fn exists(&self, id: &ObjectId) -> bool {
        self.objs.contains_key(id)
    }
}
impl gix_object::Find for MemFind {
    fn try_find<'a>(
        &self,
        id: &gix_hash::oid,
        buffer: &'a mut Vec<u8>,
    ) -> Result<Option<gix_object::Data<'a>>, gix_object::find::Error> {
        match self.objs.get(id) {
            Some((kind, data)) => {
                buffer.clear();
                buffer.extend_from_slice(data);
                Ok(Some(gix_object::Data { kind: *kind, object_hash: id.kind(), data: buffer.as_slice() }))
            }
            None => Ok(None),
        }
    }
}
impl gix_object::Exists for MemFind {
    fn exists(&self, id: &gix_hash::oid) -> bool {
        self.objs.contains_key(id)
    }
}

/// Coalesced-read merge rules (7.2): same-pack locs merge across gaps
/// < READ_GAP while the merged span stays <= READ_SPAN. Shared by
/// `read_entries` and `read_entries_bound` so the memory preflight cannot
/// drift from the fetch it prices.
const READ_GAP: u64 = 256 * 1024;
const READ_SPAN: u64 = 8 << 20;
/// Flat slack inside `read_entries_bound`: request/response plumbing and
/// the JS-side copy bookkeeping the size terms don't name — the same role
/// FETCH_SCRATCH plays for window fetches.
const READ_SLACK: usize = 256 << 10;

pub struct Bucket {
    pub inner: worker::Bucket,
    pub repo: RepoId,
}
/// The fallible core every JS→Rust body copy shares: `len` bytes are
/// reserved exactly once through try_grow — a refusal is the in-band
/// memory Limit, never the infallible `vec![0; len]` worker's `bytes()`
/// does internally — zeroed, then `fill` writes them. Native-testable
/// through the fail hook.
pub(crate) fn js_body(len: usize, fill: impl FnOnce(&mut [u8]) -> Result<(), Error>) -> Result<Vec<u8>, Error> {
    let mut v = crate::try_grow::zeroed(len)?;
    fill(v.as_mut_slice())?;
    Ok(v)
}

/// One body chunk the exact-fill core copies from: a JS Uint8Array in
/// production, a byte slice in native tests.
pub(crate) trait BodyChunk {
    fn len(&self) -> usize;
    /// Copy every byte of the chunk into `dst`. `dst.len()` is `len()`.
    fn copy_into(&self, dst: &mut [u8]);
}

impl BodyChunk for js_sys::Uint8Array {
    fn len(&self) -> usize {
        self.length() as usize
    }
    fn copy_into(&self, dst: &mut [u8]) {
        self.copy_to(dst);
    }
}

/// Read exactly `want` bytes from `stream` into one Rust buffer. The
/// buffer is reserved fallibly BEFORE the stream is polled, so a refusal
/// is the in-band memory Limit and no chunk is ever held. Each chunk is
/// copied into place and dropped. A chunk that would pass `want` is a
/// Storage error at once, before it is copied and before the stream is
/// polled again. A stream that ends short of `want` is a Storage error.
pub(crate) async fn fill_exact<C, S>(want: usize, stream: S) -> Result<Vec<u8>, Error>
where
    C: BodyChunk,
    S: futures_util::Stream<Item = Result<C, Error>>,
{
    use futures_util::StreamExt as _;
    let mut buf = crate::try_grow::zeroed(want)?;
    let mut pos = 0usize;
    let mut stream = std::pin::pin!(stream);
    while let Some(item) = stream.next().await {
        let chunk = item?;
        let n = chunk.len();
        let end = match pos.checked_add(n) {
            Some(end) if end <= want => end,
            _ => return Err(Error::Storage(format!("r2 body longer than {want}"))),
        };
        chunk.copy_into(&mut buf[pos..end]);
        pos = end;
    }
    if pos < want {
        return Err(Error::Storage(format!("r2 body {pos} bytes, wanted {want}")));
    }
    Ok(buf)
}

/// `ObjectBody` → Vec without worker's infallible `vec![0; len]`, and
/// without a JS-side copy of the body: `fill_exact` reserves `want`
/// bytes first, then each ReadableStream chunk is cast to a Uint8Array
/// and copied straight into place. An ArrayBuffer chunk gets a view,
/// which copies nothing.
async fn object_body_vec(body: worker::ObjectBody<'_>, want: usize) -> Result<Vec<u8>, Error> {
    use futures_util::StreamExt as _;
    use wasm_bindgen::JsCast as _;
    let stream = match body.response_body()? {
        worker::ResponseBody::Body(b) => {
            if b.len() != want {
                return Err(Error::Storage(format!("r2 body {} bytes, wanted {want}", b.len())));
            }
            return Ok(b);
        }
        worker::ResponseBody::Empty => return Err(Error::Storage("r2 body empty".into())),
        worker::ResponseBody::Stream(rs) => rs,
    };
    let chunks = wasm_streams::ReadableStream::from_raw(stream.unchecked_into())
        .into_stream()
        .map(|item| {
            let v = item.map_err(|e| Error::Storage(format!("r2 body stream: {e:?}")))?;
            Ok(match v.dyn_into::<js_sys::Uint8Array>() {
                Ok(a) => a,
                Err(v) if v.is_instance_of::<js_sys::ArrayBuffer>() => js_sys::Uint8Array::new(&v),
                Err(v) => return Err(Error::Storage(format!("r2 body chunk is not bytes: {v:?}"))),
            })
        });
    fill_exact(want, chunks).await
}

impl Bucket {
    pub fn new(inner: worker::Bucket, repo: RepoId) -> Self {
        Self { inner, repo }
    }
    /// One subrequest (7.1).
    pub async fn read_range(
        &self,
        key: &str,
        offset: u64,
        len: u64,
        budget: &mut ReqBudget,
    ) -> Result<Vec<u8>, Error> {
        budget.charge(1)?;
        // the returned body comes from the R2 binding: the range length
        // is bounded upstream (window fetch, entry wire cap, read_entries
        // span), and object_body_vec reserves it fallibly before copying —
        // callers still preflight through the memory guard
        let obj = self
            .inner
            .get(key)
            .range(worker::Range::OffsetWithLength { offset, length: len })
            .execute()
            .await?
            .ok_or_else(|| Error::Storage(format!("missing {key}")))?;
        let body = obj
            .body()
            .ok_or_else(|| Error::Storage(format!("no body for {key}")))?;
        let want = usize::try_from(len).map_err(|_| Error::Storage(format!("range too large on {key}")))?;
        object_body_vec(body, want).await
    }
    /// Coalesced reads (7.2): sort by (pack, offset), merge gaps < 256 KiB, split at 8 MiB.
    pub async fn read_entries(
        &self,
        locs: &[(ObjectId, ObjLoc)],
        budget: &mut ReqBudget,
    ) -> Result<Vec<(ObjectId, Vec<u8>)>, Error> {
        const MAX_READ: u64 = 48 << 20; // a single call must fit inside the isolate
        let mut sorted: Vec<&(ObjectId, ObjLoc)> = crate::try_grow::with_capacity(locs.len())?;
        sorted.extend(locs.iter());
        sorted.sort_by(|a, b| (&a.1.pack.0, a.1.offset).cmp(&(&b.1.pack.0, b.1.offset)));
        let (mut out, mut i, mut total) = (crate::try_grow::with_capacity(locs.len())?, 0usize, 0u64);
        while let Some(first) = sorted.get(i) {
            let (start, mut end, mut j) =
                (first.1.offset, first.1.offset.saturating_add(u64::from(first.1.len)), i + 1);
            while let Some(n) = sorted.get(j) {
                let n_end = n.1.offset.saturating_add(u64::from(n.1.len));
                if n.1.pack.0 != first.1.pack.0
                    || n.1.offset.saturating_sub(end) >= READ_GAP
                    || n_end.saturating_sub(start) > READ_SPAN
                {
                    break;
                }
                end = end.max(n_end);
                j += 1;
            }
            total = total.saturating_add(end.saturating_sub(start));
            if total > MAX_READ {
                return Err(Error::Limit("read batch exceeds memory budget".into()));
            }
            let key = keys::pack(&self.repo, &first.1.pack);
            let bytes = self.read_range(&key, start, end.saturating_sub(start), budget).await?;
            for e in sorted.iter().take(j).skip(i) {
                let lo = usize::try_from(e.1.offset.saturating_sub(start))
                    .map_err(|_| Error::Internal("offset".into()))?;
                let hi = lo
                    .checked_add(usize::try_from(e.1.len).map_err(|_| Error::Internal("len".into()))?)
                    .ok_or_else(|| Error::Internal("len overflow".into()))?;
                let entry = bytes
                    .get(lo..hi)
                    .ok_or_else(|| Error::Storage(format!("entry outside span in {key}")))?;
                out.push((e.0, crate::try_grow::copy(entry)?));
            }
            i = j;
        }
        Ok(out)
    }
    /// `read_entries` that survives a locs set wider than the read-batch bound:
    /// splits the set and reads the halves. Entries arrive in each half's span
    /// order — callers consume (id, entry) pairs, never positions. A single
    /// oversized span still propagates the Limit.
    pub async fn read_entries_chunked(
        &self,
        locs: &[(ObjectId, ObjLoc)],
        budget: &mut ReqBudget,
    ) -> Result<Vec<(ObjectId, Vec<u8>)>, Error> {
        let mut out = Vec::new();
        let mut stack: Vec<&[(ObjectId, ObjLoc)]> = vec![locs];
        while let Some(chunk) = stack.pop() {
            match self.read_entries(chunk, budget).await {
                Err(Error::Limit(_)) if chunk.len() > 1 => {
                    stack.push(&chunk[chunk.len() / 2..]);
                    stack.push(&chunk[..chunk.len() / 2]);
                }
                r => out.extend(r?),
            }
        }
        Ok(out)
    }

    /// Peak heap `read_entries(locs)` adds, sized BEFORE the call allocates:
    /// every loc's entry copy (`loc.len` wire bytes) stays alive in the
    /// returned Vec, and ONE merged-span range buffer is live at a time —
    /// the largest group's [start, end) under read_entries' own merge rules
    /// (READ_GAP/READ_SPAN, same pack only). The walk below is that merge
    /// minus the reads — keep it in step with read_entries. The sorted/out
    /// vec headers and request plumbing ride in the per-loc term + READ_SLACK.
    pub fn read_entries_bound(locs: &[(ObjectId, ObjLoc)]) -> usize {
        let copies = locs.iter().fold(0usize, |n, (_, l)| n.saturating_add(l.len as usize));
        let vecs = locs.len().saturating_mul(
            std::mem::size_of::<(ObjectId, Vec<u8>)>()
                .saturating_add(std::mem::size_of::<&ObjLoc>()),
        );
        let mut sorted: Vec<&ObjLoc> = locs.iter().map(|(_, l)| l).collect();
        sorted.sort_by(|a, b| (&a.pack.0, a.offset).cmp(&(&b.pack.0, b.offset)));
        let mut max_span = 0u64;
        let mut i = 0usize;
        while let Some(first) = sorted.get(i) {
            let (start, mut end, mut j) =
                (first.offset, first.offset.saturating_add(u64::from(first.len)), i + 1);
            while let Some(n) = sorted.get(j) {
                let n_end = n.offset.saturating_add(u64::from(n.len));
                if n.pack.0 != first.pack.0
                    || n.offset.saturating_sub(end) >= READ_GAP
                    || n_end.saturating_sub(start) > READ_SPAN
                {
                    break;
                }
                end = end.max(n_end);
                j += 1;
            }
            max_span = max_span.max(end.saturating_sub(start));
            i = j;
        }
        copies
            .saturating_add(usize::try_from(max_span).unwrap_or(usize::MAX))
            .saturating_add(vecs)
            .saturating_add(READ_SLACK)
    }
    /// <= 1000 keys per call (R2 delete_multiple).
    pub async fn delete(&self, keys: &[String]) -> Result<(), Error> {
        for chunk in keys.chunks(1_000) {
            let v: Vec<&str> = chunk.iter().map(String::as_str).collect();
            self.inner.delete_multiple(v).await?;
        }
        Ok(())
    }
    /// Raw multipart put, no pack framing (used by pass A for `pending/`).
    pub async fn create_multipart_upload(&self, key: &str) -> Result<MultipartUpload, Error> {
        Ok(self.inner.create_multipart_upload(key).execute().await?)
    }
}

/// Raw byte writer over one multipart upload (no pack header, no trailer hasher).
/// Used by pass A for `pending/<push>.pack`: bytes exactly as received.
pub struct RawWriter {
    mpu: MultipartUpload,
    part: Vec<u8>,
    parts: Vec<UploadedPart>,
}
impl RawWriter {
    pub async fn create(bucket: &Bucket, key: String, budget: &mut ReqBudget) -> Result<Self, Error> {
        budget.charge(1)?;
        let mpu = bucket.inner.create_multipart_upload(&key).execute().await?;
        Ok(Self { mpu, part: Vec::new(), parts: Vec::new() })
    }
    pub fn append(&mut self, bytes: &[u8]) -> Result<(), Error> {
        room(&mut self.part, PART, bytes.len())?;
        self.part.extend_from_slice(bytes);
        Ok(())
    }
    pub async fn flush_if_full(&mut self, budget: &mut ReqBudget) -> Result<(), Error> {
        while self.part.len() >= PART {
            let chunk = split_part(&mut self.part, PART)?;
            let n = u16::try_from(self.parts.len().saturating_add(1))
                .map_err(|_| Error::Limit("too many parts".into()))?;
            budget.charge(1)?;
            self.parts.push(self.mpu.upload_part(n, chunk).await?);
        }
        Ok(())
    }
    pub async fn finish(mut self, budget: &mut ReqBudget) -> Result<(), Error> {
        if let Err(e) = self.finish_inner(budget).await {
            // a dropped MultipartUpload leaves the upload and its parts orphaned in
            // R2 — abort on any mid-finish failure. A failed complete() consumes the
            // handle; that residual upload expires server-side.
            let _ = self.mpu.abort().await;
            return Err(e);
        }
        if let Err(e) = budget.charge(1) {
            // same orphan rule as finish_inner: dropping the writer without
            // abort leaves the MPU and its parts in R2
            let _ = self.mpu.abort().await;
            return Err(e);
        }
        self.mpu
            .complete(std::mem::take(&mut self.parts))
            .await
            .map(|_| ())
            .map_err(Error::from)
    }
    async fn finish_inner(&mut self, budget: &mut ReqBudget) -> Result<(), Error> {
        if !self.part.is_empty() {
            let last = std::mem::take(&mut self.part);
            let n = u16::try_from(self.parts.len().saturating_add(1))
                .map_err(|_| Error::Limit("too many parts".into()))?;
            budget.charge(1)?;
            self.parts.push(self.mpu.upload_part(n, last).await?);
        }
        Ok(())
    }
    pub async fn abort(self) {
        let _ = self.mpu.abort().await;
    }
}

/// The raw byte sink pass A streams `pending/` into — `RawWriter` (R2
/// multipart) in production, an in-memory Vec under tests.
pub trait RawOut {
    fn append(&mut self, bytes: &[u8]) -> Result<(), Error>;
    fn flush_if_full<'a>(&'a mut self, budget: &'a mut ReqBudget) -> impl std::future::Future<Output = Result<(), Error>> + 'a;
}
impl RawOut for RawWriter {
    fn append(&mut self, bytes: &[u8]) -> Result<(), Error> {
        RawWriter::append(self, bytes)
    }
    async fn flush_if_full<'a>(&'a mut self, budget: &'a mut ReqBudget) -> Result<(), Error> {
        RawWriter::flush_if_full(self, budget).await
    }
}

/// The output-pack write surface pass B drives — `PackWriter` (R2 multipart)
/// in production, an in-memory Vec under tests. Only the incremental
/// append/flush calls the resolve loop makes are shared; finish/abort stay
/// on the concrete type.
pub trait PackOut {
    fn offset(&self) -> u64;
    fn append_entry(&mut self, kind: Kind, data: &[u8]) -> Result<(u64, u32), Error>;
    fn raw_extend(&mut self, bytes: &[u8]) -> Result<(), Error>;
    fn raw_entry_done(&mut self, kind: Kind, start: u64) -> Result<(u64, u32), Error>;
    /// Capacity bytes `room` would add for `extra` incoming bytes — the
    /// memory guard's preflight size for append paths (0 when it fits).
    fn growth_bound(&self, extra: usize) -> usize;
    /// Bytes one `flush_if_full` chunk allocates — the uploaded part the
    /// guard preflights before the split can grow unchecked.
    fn chunk_bound(&self) -> usize;
    fn flush_if_full<'a>(&'a mut self, budget: &'a mut ReqBudget) -> impl std::future::Future<Output = Result<(), Error>> + 'a;
}

/// One normalized pack: one multipart upload, running hash for the trailer (1.2, 6.4).
/// The hash follows the repo's object format — sha256 packs carry a 32-byte trailer.
pub struct PackWriter {
    mpu: MultipartUpload,
    pack: PackId,
    part: Vec<u8>,
    parts: Vec<UploadedPart>,
    offset: u64,
    count: u32,
    expected: u32,
    hash: CkptHash,
    kind: gix_hash::Kind,
    commit_lo: u64,
    commit_hi: u64,
    created_at: i64,
    /// The uniform non-final part size — PART in production; GC's test knob
    /// (gc_part_bytes) may shrink it. Must not change between checkpoints.
    part_size: usize,
}
impl PackWriter {
    /// `expected` is pass A's verified entry count (2.4); the header is final from byte 0.
    /// `kind` is the repo's object format — it sizes the pack trailer.
    pub async fn create(
        bucket: &Bucket,
        key: &str,
        expected: u32,
        kind: gix_hash::Kind,
        budget: &mut ReqBudget,
    ) -> Result<Self, Error> {
        let name = key
            .rsplit('/')
            .next()
            .and_then(|f| f.strip_suffix(".pack"))
            .ok_or_else(|| Error::Internal(format!("not a pack key: {key}")))?;
        let created_at = platform::now_ms();
        let meta: HashMap<String, String> = [
            ("repo", bucket.repo.0.clone()),
            ("pack", name.to_string()),
            ("count", expected.to_string()),
            ("created_at", created_at.to_string()),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
        budget.charge(1)?;
        let mpu = bucket.inner.create_multipart_upload(key).custom_metadata(meta).execute().await?;
        let header = gix_pack::data::header::encode(gix_pack::data::Version::V2, expected);
        // the hasher is fed only as bytes are uploaded (flush_if_full/checkpoint/finish),
        // so a WriterCkpt covers exactly the durable prefix — never buffered bytes
        let hash = CkptHash::new(kind);
        Ok(Self {
            mpu,
            pack: PackId(name.to_string()),
            part: header.to_vec(),
            parts: Vec::new(),
            offset: 12,
            count: 0,
            expected,
            hash,
            kind,
            commit_lo: u64::MAX,
            commit_hi: 0,
            created_at,
            part_size: PART,
        })
    }
    /// Sync; buffers. Returns (offset, len): the `objects` row and the range a reader asks for.
    pub fn append_entry(&mut self, kind: Kind, data: &[u8]) -> Result<(u64, u32), Error> {
        let start = self.part.len();
        room(&mut self.part, self.part_size, encoded_bound(data.len()))?;
        codec::encode_entry(kind, data, &mut self.part)?;
        let entry = self.part.get(start..).ok_or_else(|| Error::Internal("part buffer".into()))?;
        let len = u32::try_from(entry.len()).map_err(|_| Error::Limit("entry too long".into()))?;
        let offset = self.offset;
        self.offset = offset.saturating_add(u64::from(len));
        if kind == Kind::Commit {
            self.commit_lo = self.commit_lo.min(offset);
            self.commit_hi = self.offset;
        }
        self.count = self.count.saturating_add(1);
        Ok((offset, len))
    }
    /// Current write offset — the caller records it before a chunked raw entry so
    /// `raw_entry_done` can derive the entry's wire length.
    pub fn offset(&self) -> u64 {
        self.offset
    }
    /// Bytes buffered but not yet uploaded — GC gates its checkpoint on this reaching
    /// the 5 MiB non-final-part minimum.
    pub fn buffered(&self) -> u64 {
        self.part.len() as u64
    }
    /// The buffered bytes themselves — the undrained tail a GC yield persists.
    pub fn buffered_bytes(&self) -> &[u8] {
        &self.part
    }
    /// GC resume counterpart of `buffered_bytes`: load a persisted tail back into
    /// the buffer. `extra`/`lo`/`hi` fold the tail's already-completed entries back
    /// into the writer's counters (their `ObjRow`s were posted at the yield).
    pub fn resume_tail(&mut self, bytes: &[u8], extra: u32, lo: u64, hi: u64) -> Result<(), Error> {
        let st = TailState {
            offset: self.offset,
            count: self.count,
            commit_lo: self.commit_lo,
            commit_hi: self.commit_hi,
        };
        let st = resume_tail_apply(&mut self.part, self.part_size, bytes, st, extra, lo, hi)?;
        self.offset = st.offset;
        self.count = st.count;
        self.commit_lo = st.commit_lo;
        self.commit_hi = st.commit_hi;
        Ok(())
    }
    /// Append raw pack bytes verbatim: any chunk of an entry's `varint header + zlib
    /// body`, exactly as it appeared in the source pack. The caller ends each logical
    /// entry with `raw_entry_done` — checkpoints and index rows must never split one.
    pub fn raw_extend(&mut self, bytes: &[u8]) -> Result<(), Error> {
        room(&mut self.part, self.part_size, bytes.len())?;
        self.part.extend_from_slice(bytes);
        self.offset = self.offset.saturating_add(bytes.len() as u64);
        Ok(())
    }
    /// Seal an entry appended via `raw_extend`; `start` was `offset()` before its first
    /// chunk. Returns (offset, len) for the index row — same contract as append_entry.
    pub fn raw_entry_done(&mut self, kind: Kind, start: u64) -> Result<(u64, u32), Error> {
        let len = u32::try_from(self.offset.saturating_sub(start))
            .map_err(|_| Error::Limit("entry too long".into()))?;
        if kind == Kind::Commit {
            self.commit_lo = self.commit_lo.min(start);
            self.commit_hi = self.offset;
        }
        self.count = self.count.saturating_add(1);
        Ok((start, len))
    }
    /// Uploads whole uniform parts while the buffer holds that much.
    pub async fn flush_if_full(&mut self, budget: &mut ReqBudget) -> Result<(), Error> {
        while self.part.len() >= self.part_size {
            let chunk = split_part(&mut self.part, self.part_size)?;
            let n = u16::try_from(self.parts.len().saturating_add(1))
                .map_err(|_| Error::Limit("too many parts".into()))?;
            budget.charge(1)?;
            self.hash.update(&chunk);
            self.parts.push(self.mpu.upload_part(n, chunk).await?);
        }
        Ok(())
    }
    /// The memory guard's preflight for `finish` (see `finish_growth`).
    /// The edge push runs it before finish, while it can still abort the
    /// upload cleanly; the growth itself is also fallible.
    pub fn finish_bound(&self) -> usize {
        finish_growth(&self.part, self.part_size, self.kind.len_in_bytes())
    }
    /// Trailer, last part, complete. When this returns the pack is durable: section 3 step 1.
    pub async fn finish(mut self, budget: &mut ReqBudget) -> Result<PackMeta, Error> {
        if let Err(e) = self.finish_inner(budget).await {
            // a dropped MultipartUpload leaves the upload and its parts orphaned in
            // R2 — abort on any mid-finish failure. A failed complete() consumes the
            // handle; that residual upload expires server-side.
            let _ = self.mpu.abort().await;
            return Err(e);
        }
        self.seal(budget, Finish::Ordinary).await
    }
    /// finish() for a resumable caller (the import job): a mid-finish failure —
    /// a transient part-upload error, a fixed-in-a-later-build check, or a
    /// budget failure right before complete() — leaves the MPU and its parts
    /// alive so the retry resumes the checkpoint and re-finishes instead of
    /// rebuilding gigabytes of output from scratch.
    pub async fn finish_resumable(mut self, budget: &mut ReqBudget) -> Result<PackMeta, Error> {
        self.finish_inner(budget).await?;
        self.seal(budget, Finish::Resumable).await
    }
    async fn seal(mut self, budget: &mut ReqBudget, mode: Finish) -> Result<PackMeta, Error> {
        let bytes = self
            .offset
            .saturating_add(u64::try_from(self.kind.len_in_bytes()).map_err(|_| Error::Internal("ds".into()))?);
        if let Err((e, abort)) = seal_charge(budget, mode) {
            if abort {
                let _ = self.mpu.abort().await;
            }
            return Err(e);
        }
        let obj = self
            .mpu
            .complete(std::mem::take(&mut self.parts))
            .await
            .map_err(Error::from)?;
        if obj.size() != bytes {
            return Err(Error::Storage(format!("R2 holds {} bytes, wrote {bytes}", obj.size())));
        }
        Ok(PackMeta {
            pack: self.pack,
            push_id: None,
            count: self.count,
            bytes,
            commit_lo: self.commit_lo,
            commit_hi: self.commit_hi,
            created_at: self.created_at,
        })
    }
    async fn finish_inner(&mut self, budget: &mut ReqBudget) -> Result<(), Error> {
        if self.count != self.expected {
            return Err(Error::Internal(format!(
                "wrote {} entries, header says {}",
                self.count, self.expected
            )));
        }
        // the trailer hashes header+entries: self.hash covers only uploaded bytes, so
        // clone it and feed the still-buffered tail before sealing
        let trailer = {
            let mut h = self.hash.clone();
            h.update(&self.part);
            h.fin()
        };
        // fallible: a failed trailer growth or split returns the in-band
        // Limit, and the caller's finish path decides abort vs keep
        room(&mut self.part, self.part_size, trailer.len())?;
        self.part.extend_from_slice(&trailer);
        self.flush_if_full(budget).await?;
        if !self.part.is_empty() {
            let last = std::mem::take(&mut self.part);
            let n = u16::try_from(self.parts.len().saturating_add(1))
                .map_err(|_| Error::Limit("too many parts".into()))?;
            budget.charge(1)?;
            self.parts.push(self.mpu.upload_part(n, last).await?);
        }
        Ok(())
    }
    pub async fn abort(self) {
        let _ = self.mpu.abort().await;
    }

    // ---- GC build support (section 5.2): resume + checkpoint across slices ----

    pub async fn upload_id(&self) -> String {
        self.mpu.upload_id().await
    }

    /// Append a stored entry verbatim (no re-inflate/deflate): section 5.2's verbatim copy.
    /// `entry` is `varint header + zlib body` exactly as read from another pack.
    pub fn append_stored(&mut self, entry: &[u8]) -> Result<(u64, u32), Error> {
        let (kind, _size, _hlen) = codec::entry_header(entry)?;
        let len = u32::try_from(entry.len()).map_err(|_| Error::Limit("entry too long".into()))?;
        room(&mut self.part, self.part_size, entry.len())?;
        self.part.extend_from_slice(entry);
        let offset = self.offset;
        self.offset = offset.saturating_add(u64::from(len));
        if kind == Kind::Commit {
            self.commit_lo = self.commit_lo.min(offset);
            self.commit_hi = self.offset;
        }
        self.count = self.count.saturating_add(1);
        Ok((offset, len))
    }

    /// Upload the oldest PART bytes as the next uniform part — R2 rejects complete()
    /// when non-final parts differ in size, so the whole variable buffer must NOT be
    /// drained in one call. Returns None under a full part. The exported checkpoint
    /// describes only the durable prefix: pos is the uploaded offset (appended minus
    /// buffered) and the hasher covers exactly those bytes.
    /// Returns (part_number, etag, checkpoint) — UploadedPart is not Clone.
    pub async fn checkpoint(
        &mut self,
        budget: &mut ReqBudget,
    ) -> Result<Option<(u16, String, WriterCkpt)>, Error> {
        if self.part.len() < self.part_size {
            return Ok(None);
        }
        let chunk = split_part(&mut self.part, self.part_size)?;
        let n = u16::try_from(self.parts.len().saturating_add(1))
            .map_err(|_| Error::Limit("too many parts".into()))?;
        budget.charge(1)?;
        self.hash.update(&chunk);
        let part = self.mpu.upload_part(n, chunk).await?;
        let rec = (part.part_number(), part.etag());
        self.parts.push(part);
        let mut ckpt = WriterCkpt {
            pos: self.offset.saturating_sub(self.part.len() as u64),
            sha: None,
            sha256: None,
            count: self.count,
            expected: self.expected,
            commit_lo: self.commit_lo,
            commit_hi: self.commit_hi,
        };
        ckpt.set_hash(&self.hash);
        Ok(Some((rec.0, rec.1, ckpt)))
    }

    /// Describe the durable state right now — valid only when `buffered() < PART`
    /// (all full parts drained via checkpoint/flush_if_full), so `pos` is exactly
    /// the uploaded prefix and `sha` covers exactly those bytes. The import job
    /// snapshots at slice end; GC uses checkpoint()'s return instead.
    pub fn snapshot(&self) -> WriterCkpt {
        let mut ckpt = WriterCkpt {
            pos: self.offset.saturating_sub(self.part.len() as u64),
            sha: None,
            sha256: None,
            count: self.count,
            expected: self.expected,
            commit_lo: self.commit_lo,
            commit_hi: self.commit_hi,
        };
        ckpt.set_hash(&self.hash);
        ckpt
    }
    /// GC test knob (gc_part_bytes): shrink the uniform part so consolidate is
    /// multi-part on a tiny repo. Set before the first checkpoint; production
    /// never calls this.
    pub fn set_part_size(&mut self, n: usize) {
        self.part_size = n;
    }
    /// The uniform non-final part size this writer checkpoints at.
    pub fn part_size(&self) -> u64 {
        self.part_size as u64
    }
    /// Etags of all uploaded parts, in part order — the resume token.
    pub fn etags(&self) -> Vec<String> {
        self.parts.iter().map(|p| p.etag()).collect()
    }

    /// Rebind to an existing multipart upload and resume at a checkpoint (5.2).
    /// `etags` are the parts already uploaded, in order.
    pub async fn resume(
        bucket: &Bucket,
        key: &str,
        upload_id: &str,
        etags: &[String],
        state: &WriterCkpt,
        _budget: &mut ReqBudget,
    ) -> Result<Self, Error> {
        let mpu = bucket.inner.resume_multipart_upload(key, upload_id)?;
        let parts: Vec<UploadedPart> = etags
            .iter()
            .enumerate()
            .filter_map(|(i, e)| {
                u16::try_from(i.saturating_add(1)).ok().map(|n| UploadedPart::new(n, e.clone()))
            })
            .collect();
        let created_at = platform::now_ms();
        // pos==0 means nothing ever uploaded — the 12-byte header was buffered-only
        // and is lost, so the resumed writer re-emits it (the durable sha covers no
        // bytes yet, so the header is hashed when its part finally lands)
        let (part, offset) = if state.pos == 0 {
            (
                gix_pack::data::header::encode(gix_pack::data::Version::V2, state.expected).to_vec(),
                12,
            )
        } else {
            (Vec::new(), state.pos)
        };
        let hash = state.hash()?;
        let kind = match hash {
            CkptHash::Sha256(_) => gix_hash::Kind::Sha256,
            CkptHash::Sha1(_) => gix_hash::Kind::Sha1,
        };
        Ok(Self {
            mpu,
            pack: PackId(
                key.rsplit('/').next().and_then(|f| f.strip_suffix(".pack")).unwrap_or_default().to_string(),
            ),
            part,
            parts,
            offset,
            count: state.count,
            expected: state.expected,
            hash,
            kind,
            commit_lo: state.commit_lo,
            commit_hi: state.commit_hi,
            created_at,
            part_size: PART,
        })
    }
}

fn i(n: u64) -> Result<V, Error> {
    Ok(V::from(i64::try_from(n).map_err(|_| Error::Internal("u64 to i64".into()))?))
}

/// A6: DO SQLite refuses statements binding more than 100 parameters — check
/// at the single choke point so an over-bound statement fails with a named
/// error instead of a raw SQLite complaint.
pub const MAX_BOUND_PARAMS: usize = 100;
pub fn check_params(n: usize) -> Result<(), Error> {
    if n > MAX_BOUND_PARAMS {
        return Err(Error::Internal(format!(
            "sql: {n} bound params > {MAX_BOUND_PARAMS} (A6)"
        )));
    }
    Ok(())
}

impl PackOut for PackWriter {
    fn offset(&self) -> u64 {
        self.offset()
    }
    fn append_entry(&mut self, kind: Kind, data: &[u8]) -> Result<(u64, u32), Error> {
        self.append_entry(kind, data)
    }
    fn raw_extend(&mut self, bytes: &[u8]) -> Result<(), Error> {
        self.raw_extend(bytes)
    }
    fn raw_entry_done(&mut self, kind: Kind, start: u64) -> Result<(u64, u32), Error> {
        self.raw_entry_done(kind, start)
    }
    fn growth_bound(&self, extra: usize) -> usize {
        room_growth(&self.part, self.part_size, extra)
    }
    fn chunk_bound(&self) -> usize {
        self.part_size
    }
    async fn flush_if_full<'a>(&'a mut self, budget: &'a mut ReqBudget) -> Result<(), Error> {
        PackWriter::flush_if_full(self, budget).await
    }
}

/// The one `SqlStorage::exec` wrapper — every call site in the crate routes
/// here so the A6 bound-param check cannot be bypassed.
pub fn exec(sql: &SqlStorage, q: &str, args: Vec<V>) -> Result<worker::SqlCursor, Error> {
    check_params(args.len())?;
    sql.exec(q, Some(args)).map_err(|e| Error::Storage(e.to_string()))
}

/// Sync SQLite index, used only inside RepoDo and jobs (1.2).
pub struct Index<'s>(pub &'s SqlStorage);
impl<'s> Index<'s> {
    fn exec(&self, q: &str, args: Vec<V>) -> Result<worker::SqlCursor, Error> {
        exec(self.0, q, args)
    }
    /// The reader query of 2.3, verbatim. Rows of ingesting/dead packs are invisible.
    /// Batches `IN` lists at 90 bound params (A6).
    pub fn lookup(&self, ids: &[ObjectId]) -> Result<Vec<Option<ObjLoc>>, Error> {
        #[derive(Deserialize)]
        struct R {
            sha: String,
            pack_id: String,
            idx: u32,
            offset: u64,
            len: u32,
            kind: u8,
            size: u64,
        }
        let mut by_sha: HashMap<String, ObjLoc> = HashMap::new();
        for chunk in ids.chunks(90) {
            let marks = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(",");
            let args: Vec<V> = chunk.iter().map(|id| V::from(id.to_string().as_str())).collect();
            for r in self
                .exec(
                    &format!(
                        "SELECT o.sha, o.pack_id, o.idx, o.offset, o.len, o.kind, o.size FROM objects o \
                         JOIN packs p ON p.id = o.pack_id WHERE o.sha IN ({marks}) AND p.state = 'live' \
                         GROUP BY o.sha"
                    ),
                    args,
                )?
                .to_array::<R>()?
            {
                by_sha.entry(r.sha).or_insert(ObjLoc {
                    pack: PackId(r.pack_id),
                    idx: r.idx,
                    offset: r.offset,
                    len: r.len,
                    kind: kind_of(r.kind)?,
                    size: r.size,
                });
            }
        }
        Ok(ids.iter().map(|id| by_sha.get(&id.to_string()).cloned()).collect())
    }
    /// Presence in one named (typically `ingesting`) pack — the 2.5 self-subtraction.
    pub fn lookup_in_pack(&self, id: &ObjectId, pack: &PackId) -> Result<Option<ObjLoc>, Error> {
        #[derive(Deserialize)]
        struct R {
            idx: u32,
            offset: u64,
            len: u32,
            kind: u8,
            size: u64,
        }
        let row = self
            .exec(
                "SELECT idx, offset, len, kind, size FROM objects WHERE sha=? AND pack_id=? LIMIT 1",
                vec![V::from(id.to_string().as_str()), V::from(pack.0.as_str())],
            )?
            .to_array::<R>()?
            .into_iter()
            .next();
        Ok(row.map(|r| ObjLoc {
            pack: pack.clone(),
            idx: r.idx,
            offset: r.offset,
            len: r.len,
            kind: kind_of(r.kind).unwrap_or(Kind::Blob),
            size: r.size,
        }))
    }
    /// (count, bytes) for SendSet::mark's bitmap sizing.
    pub fn pack_meta(&self, pack: &PackId) -> Result<(u32, u64), Error> {
        #[derive(Deserialize)]
        struct R {
            count: i64,
            bytes: i64,
        }
        let r = self
            .exec(
                "SELECT count, bytes FROM packs WHERE id=? AND state='live'",
                vec![V::from(pack.0.as_str())],
            )?
            .to_array::<R>()?
            .into_iter()
            .next()
            .ok_or_else(|| Error::Storage(format!("pack {} not live", pack.0)))?;
        Ok((
            u32::try_from(r.count).map_err(|_| Error::Internal("count".into()))?,
            u64::try_from(r.bytes).map_err(|_| Error::Internal("bytes".into()))?,
        ))
    }
    /// Marked entries of one pack, offset order (plan_reads). `bitmap` has one bit per idx.
    pub fn entries_of(&self, pack: &PackId, bitmap: &[u8]) -> Result<Vec<ObjLoc>, Error> {
        #[derive(Deserialize)]
        struct R {
            idx: u32,
            offset: u64,
            len: u32,
            kind: u8,
            size: u64,
        }
        let mut out = Vec::new();
        let mut after: i64 = -1;
        loop {
            let rows = self
                .exec(
                    "SELECT idx, offset, len, kind, size FROM objects WHERE pack_id=? AND idx>? \
                     ORDER BY idx LIMIT 5000",
                    vec![V::from(pack.0.as_str()), V::from(after)],
                )?
                .to_array::<R>()?;
            let n = rows.len();
            for r in rows {
                after = i64::from(r.idx);
                let byte = usize::try_from(r.idx / 8).map_err(|_| Error::Internal("idx".into()))?;
                if bitmap.get(byte).map(|b| b & (1u8 << (r.idx % 8)) != 0) == Some(true) {
                    out.push(ObjLoc {
                        pack: pack.clone(),
                        idx: r.idx,
                        offset: r.offset,
                        len: r.len,
                        kind: kind_of(r.kind)?,
                        size: r.size,
                    });
                }
            }
            if n < 5000 {
                break;
            }
        }
        // idx order != offset order when a forward REF_DELTA was resolved late: the
        // object kept its original idx but was appended at the pack's tail
        out.sort_by_key(|l| l.offset);
        Ok(out)
    }
    /// Commit-kind entries of one pack in the byte range [lo, hi) — 7.4 prefetch:
    /// the commit walk's level reads extend into region reads so a linear history
    /// costs one range read per window, not one per BFS level.
    pub fn commits_in_range(
        &self,
        pack: &PackId,
        lo: u64,
        hi: u64,
    ) -> Result<Vec<(ObjectId, ObjLoc)>, Error> {
        #[derive(Deserialize)]
        struct R {
            sha: String,
            idx: u32,
            offset: u64,
            len: u32,
            size: u64,
        }
        let rows = self
            .exec(
                "SELECT o.sha, o.idx, o.offset, o.len, o.size FROM objects o \
                 JOIN packs p ON p.id = o.pack_id \
                 WHERE o.pack_id=? AND o.kind=1 AND o.offset>=? AND o.offset<? \
                 AND p.state='live' ORDER BY o.offset LIMIT 100000",
                vec![V::from(pack.0.as_str()), i(lo)?, i(hi)?],
            )?
            .to_array::<R>()?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let id = ObjectId::from_hex(r.sha.as_bytes())
                .map_err(|_| Error::Internal("bad sha in objects".into()))?;
            out.push((
                id,
                ObjLoc {
                    pack: pack.clone(),
                    idx: r.idx,
                    offset: r.offset,
                    len: r.len,
                    kind: Kind::Commit,
                    size: r.size,
                },
            ));
        }
        Ok(out)
    }
    /// Safe to repeat: ON CONFLICT DO NOTHING per row (a retried /_do/push/index is a no-op).
    pub fn insert_objects(&self, pack: &PackId, rows: &[ObjRow]) -> Result<(), Error> {
        if rows.len() > 10_000 {
            return Err(Error::Protocol("more than 10,000 rows".into()));
        }
        for r in rows {
            self.exec(
                "INSERT INTO objects(sha,pack_id,idx,offset,len,kind,size) VALUES(?,?,?,?,?,?,?) \
                 ON CONFLICT(sha,pack_id) DO NOTHING",
                vec![
                    V::from(r.sha.to_string().as_str()),
                    V::from(pack.0.as_str()),
                    V::from(i64::from(r.idx)),
                    i(r.offset)?,
                    V::from(i64::from(r.len)),
                    V::from(i64::from(git_kind(r.kind))),
                    i(r.size)?,
                ],
            )?;
            // protected refs: the commit graph the FF check walks. n=-1 marks the
            // commit as recorded (so a root reads as "known, no parents", not
            // "unknown"); n>=0 are its parents in order. Content-addressed facts —
            // never stale, so no lifecycle beyond repo purge.
            if let (Kind::Commit, Some(ps)) = (r.kind, &r.parents) {
                let sha = r.sha.to_string();
                self.exec(
                    "INSERT OR IGNORE INTO commit_parents(sha,n,parent) VALUES(?,-1,NULL)",
                    vec![V::from(sha.as_str())],
                )?;
                for (n, p) in ps.iter().enumerate() {
                    self.exec(
                        "INSERT OR IGNORE INTO commit_parents(sha,n,parent) VALUES(?,?,?)",
                        vec![V::from(sha.as_str()), V::from(n as i64), V::from(p.to_string().as_str())],
                    )?;
                }
            }
        }
        Ok(())
    }
    pub fn set_pack_state(&self, pack: &PackId, state: PackState) -> Result<(), Error> {
        self.exec(
            "UPDATE packs SET state=? WHERE id=?",
            vec![V::from(state.as_str()), V::from(pack.0.as_str())],
        )?;
        Ok(())
    }
}

/// The complete schema (2.3, 3, 4, 8 + GC tables of section 5 / gc-and-repack-alarm's registry).
pub mod schema {
    use crate::error::Error;
    use worker::SqlStorage;

    const DDL: &[&str] = &[
        "CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID",
        "CREATE TABLE IF NOT EXISTS refs (name TEXT PRIMARY KEY, target TEXT NOT NULL, peeled TEXT, updated_at INTEGER NOT NULL) WITHOUT ROWID",
        "CREATE TABLE IF NOT EXISTS reflog (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, old TEXT NOT NULL, new TEXT NOT NULL, push_id TEXT NOT NULL, principal TEXT NOT NULL, at INTEGER NOT NULL)",
        "CREATE INDEX IF NOT EXISTS reflog_at ON reflog(at)",
        "CREATE TABLE IF NOT EXISTS pushes (id TEXT PRIMARY KEY, state TEXT NOT NULL, pack_id TEXT, principal TEXT NOT NULL, began_at INTEGER NOT NULL, ended_at INTEGER, gc_epoch INTEGER NOT NULL, result TEXT, swept_at INTEGER, scope TEXT, auth TEXT) WITHOUT ROWID",
        "CREATE INDEX IF NOT EXISTS pushes_state ON pushes(state)",
        "CREATE TABLE IF NOT EXISTS packs (id TEXT PRIMARY KEY, state TEXT NOT NULL, count INTEGER NOT NULL, bytes INTEGER NOT NULL, commit_lo INTEGER NOT NULL, commit_hi INTEGER NOT NULL, push_id TEXT, created_at INTEGER NOT NULL, dead_at INTEGER) WITHOUT ROWID",
        "CREATE INDEX IF NOT EXISTS packs_state ON packs(state)",
        "CREATE TABLE IF NOT EXISTS objects (sha TEXT NOT NULL, pack_id TEXT NOT NULL, idx INTEGER NOT NULL, offset INTEGER NOT NULL, len INTEGER NOT NULL, kind INTEGER NOT NULL, size INTEGER NOT NULL, PRIMARY KEY (sha, pack_id)) WITHOUT ROWID",
        "CREATE INDEX IF NOT EXISTS objects_pack ON objects(pack_id, idx)",
        "CREATE INDEX IF NOT EXISTS objects_pack_off ON objects(pack_id, offset)",
        "CREATE TABLE IF NOT EXISTS jobs (id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL, run_at INTEGER NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, cursor TEXT, payload TEXT NOT NULL DEFAULT '{}', state TEXT NOT NULL DEFAULT 'queued', last_error TEXT, started_at INTEGER, lease TEXT, strands INTEGER NOT NULL DEFAULT 0)",
        "CREATE TABLE IF NOT EXISTS marked (pack_id TEXT PRIMARY KEY, bitmap BLOB NOT NULL) WITHOUT ROWID",
        "CREATE TABLE IF NOT EXISTS gc_frontier (sha TEXT PRIMARY KEY) WITHOUT ROWID",
        "CREATE TABLE IF NOT EXISTS gc_seen (sha TEXT PRIMARY KEY) WITHOUT ROWID",
        "CREATE TABLE IF NOT EXISTS gc_parts (part_no INTEGER PRIMARY KEY, etag TEXT NOT NULL)",
        // GcConsolidate yield persistence: the undrained <PART tail of the output
        // buffer, chunked. Without it a slice that can't fill one part inside the
        // request budget (sparse marks => ~1 read per entry) replays forever.
        "CREATE TABLE IF NOT EXISTS gc_tail (seq INTEGER PRIMARY KEY, blob BLOB NOT NULL)",
        // PR5 write barrier: one row per pack promoted while a barrier chain is
        // live; cursor = the mark's paging position into that pack's objects
        // (B1 inserts rows only — the page span is phase B2).
        "CREATE TABLE IF NOT EXISTS gc_pending (seq INTEGER PRIMARY KEY AUTOINCREMENT, pack_id TEXT UNIQUE NOT NULL, cursor INTEGER NOT NULL DEFAULT -1)",
        "CREATE TABLE IF NOT EXISTS tokens (id TEXT PRIMARY KEY, hash TEXT NOT NULL, level TEXT NOT NULL, name TEXT NOT NULL, created_at INTEGER NOT NULL, scope TEXT, expires_at INTEGER) WITHOUT ROWID",
        "CREATE INDEX IF NOT EXISTS tokens_hash ON tokens(hash)",
        "CREATE TABLE IF NOT EXISTS pins (name TEXT PRIMARY KEY, sha TEXT NOT NULL, created_at INTEGER NOT NULL) WITHOUT ROWID",
        // protected refs: patterns (same grammar as token scopes) whose refs are
        // fast-forward-only and undeletable; commit_parents is the graph the FF
        // check walks (n=-1 = commit recorded, n>=0 = nth parent)
        // push events outbox (jobs/events.rs): one row per committed push while
        // GE_PUSH_WEBHOOK_URL is set; deleted on 2xx delivery
        "CREATE TABLE IF NOT EXISTS push_events (seq INTEGER PRIMARY KEY AUTOINCREMENT, push_id TEXT NOT NULL, at INTEGER NOT NULL, body TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, last_error TEXT)",
        "CREATE TABLE IF NOT EXISTS protected (pattern TEXT PRIMARY KEY, created_at INTEGER NOT NULL) WITHOUT ROWID",
        "CREATE TABLE IF NOT EXISTS commit_parents (sha TEXT NOT NULL, n INTEGER NOT NULL, parent TEXT, PRIMARY KEY (sha, n)) WITHOUT ROWID",
        // A27: sliding-window rate counters — one row per (bucket, minute window).
        "CREATE TABLE IF NOT EXISTS rate (bucket TEXT NOT NULL, window INTEGER NOT NULL, count INTEGER NOT NULL, PRIMARY KEY (bucket, window)) WITHOUT ROWID",
        // I1 import staging: object references for the 2.5 connectivity check live
        // here instead of memory, so an import job can span slices — and a push
        // whose links exceed MAX_LINKS can overflow onto the same table.
        "CREATE TABLE IF NOT EXISTS push_links (push_id TEXT NOT NULL, sha TEXT NOT NULL, PRIMARY KEY (push_id, sha)) WITHOUT ROWID",
        // the entry TOC parsed off the staged pack, one row per pack entry:
        // ktype 0-3 = gix kind; 4 = ofs-delta (kaux_n = base distance); 5 = ref-delta
        // (kaux_id = base sha hex). idx is the entry's ordinal in the pack.
        "CREATE TABLE IF NOT EXISTS import_toc (push_id TEXT NOT NULL, idx INTEGER NOT NULL, offset INTEGER NOT NULL, hlen INTEGER NOT NULL, ktype INTEGER NOT NULL, kaux_n INTEGER, kaux_id TEXT, clen INTEGER NOT NULL, size INTEGER NOT NULL, PRIMARY KEY (push_id, idx)) WITHOUT ROWID",
        "CREATE INDEX IF NOT EXISTS import_toc_off ON import_toc(push_id, offset)",
        // MPU part etags for the import's output pack — same role as gc_parts.
        "CREATE TABLE IF NOT EXISTS import_parts (push_id TEXT NOT NULL, part_no INTEGER NOT NULL, etag TEXT NOT NULL, PRIMARY KEY (push_id, part_no)) WITHOUT ROWID",
        // I1 crash-resume: entry idx -> output-pack byte offset where its write started
        // (off, at first append) and where its objects row posted (end = offset+len —
        // NULL until then). Done = end <= the durable boundary; the last writer at or
        // below it is the mid-entry resume point. sha/kind/size shadow the eventual
        // objects row so a fully-durable-but-unposted entry can be restored without
        // a byte replay (PackWriter cannot write out of order).
        "CREATE TABLE IF NOT EXISTS import_open (push_id TEXT NOT NULL, idx INTEGER NOT NULL, off INTEGER NOT NULL, end INTEGER, sha TEXT, kind INTEGER, size INTEGER, PRIMARY KEY (push_id, idx)) WITHOUT ROWID",
        "CREATE INDEX IF NOT EXISTS import_open_off ON import_open(push_id, off)",
        // A32 for the import side: the undrained <PART writer tail, chunked
        // under the SqlStorage value ceiling — persisted at yield, reloaded at
        // resume so a sub-part slice's appends don't die with the isolate
        "CREATE TABLE IF NOT EXISTS import_tail (push_id TEXT NOT NULL, seq INTEGER NOT NULL, blob BLOB NOT NULL, PRIMARY KEY (push_id, seq)) WITHOUT ROWID",
    ];
    /// Columns added after first deploy. CREATE TABLE IF NOT EXISTS never updates an
    /// existing table, so DOs booted under an older schema need ALTER TABLE — SQLite
    /// does that only if the column is missing (checked via PRAGMA table_info).
    const LATE_COLS: &[(&str, &str, &str)] = &[
        ("pushes", "swept_at", "swept_at INTEGER"),
        ("packs", "dead_at", "dead_at INTEGER"),
        ("jobs", "started_at", "started_at INTEGER"),
        ("jobs", "lease", "lease TEXT"),
        ("jobs", "strands", "strands INTEGER NOT NULL DEFAULT 0"),
        ("import_open", "sha", "sha TEXT"),
        ("import_open", "kind", "kind INTEGER"),
        ("import_open", "size", "size INTEGER"),
        ("tokens", "scope", "scope TEXT"),
        ("pushes", "scope", "scope TEXT"),
        ("pushes", "auth", "auth TEXT"),
        ("tokens", "expires_at", "expires_at INTEGER"),
        // PR5: frontier rows carry a generation — 0 = snapshot roots, 1 = delta
        // feeds written by the barrier while the mark walks. Existing rows
        // default to 0, which is exactly what they are.
        ("gc_frontier", "gen", "gen INTEGER NOT NULL DEFAULT 0"),
        // PR6 §2 (C11): the as-copied bitmap snapshot. Barrier chains freeze
        // `bitmap` into `built` at admission (begin_build) so the sweep's
        // subset check compares what was actually copied, not what is marked
        // now. NULL on flag-off/compact chains and pre-PR6 marked rows.
        ("marked", "built", "built BLOB"),
    ];
    pub fn migrate(sql: &SqlStorage) -> Result<(), Error> {
        for q in DDL {
            super::exec(sql, q, vec![])?;
        }
        #[derive(serde::Deserialize)]
        struct Col {
            name: String,
        }
        for (table, col, decl) in LATE_COLS {
            let cols = super::exec(sql, &format!("PRAGMA table_info({table})"), vec![])?
                .to_array::<Col>()
                .map_err(|e| Error::Storage(e.to_string()))?;
            if !cols.iter().any(|c| c.name == *col) {
                super::exec(sql, &format!("ALTER TABLE {table} ADD COLUMN {decl}"), vec![])?;
            }
        }
        // After LATE_COLS so the column exists on every schema vintage: the
        // barrier-era frontier pop seeks by generation.
        super::exec(
            sql,
            "CREATE INDEX IF NOT EXISTS gc_frontier_gen ON gc_frontier(gen, sha)",
            vec![],
        )?;
        // PR-3a: a NULL/blank token scope used to mean "all refs" and now denies.
        // Rewrite legacy rows to the explicit pattern so existing tokens keep the
        // access they were minted with. Idempotent; a no-op once every row is set.
        super::exec(
            sql,
            "UPDATE tokens SET scope='refs/*' WHERE scope IS NULL OR trim(scope)=''",
            vec![],
        )?;
        Ok(())
    }
}

/// Resumable build state for GC (5.2): carried in `gc.pos` across slices.
/// (Named WriterCkpt to keep PackState for the packs.state lifecycle.)
#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct WriterCkpt {
    pub pos: u64,
    /// sha1 pack state — checkpoints written before sha256 support always carry
    /// `sha`; new sha256 checkpoints write `sha256` instead and leave this out
    #[serde(default)]
    pub sha: Option<CkptSha1>,
    #[serde(default)]
    pub sha256: Option<CkptSha256>,
    pub count: u32,
    pub expected: u32,
    pub commit_lo: u64,
    pub commit_hi: u64,
}
impl WriterCkpt {
    /// The pack's hash state as the resumable hasher — sha256 wins when both
    /// fields exist (a sha256 ckpt never writes `sha`).
    pub fn hash(&self) -> Result<CkptHash, Error> {
        if let Some(h) = &self.sha256 {
            return Ok(CkptHash::Sha256(h.clone()));
        }
        self.sha
            .clone()
            .map(CkptHash::Sha1)
            .ok_or_else(|| Error::Internal("checkpoint without hash state".into()))
    }
    /// Split the live hasher into whichever field its variant writes.
    pub fn set_hash(&mut self, h: &CkptHash) {
        match h {
            CkptHash::Sha1(s) => {
                self.sha = Some(s.clone());
                self.sha256 = None;
            }
            CkptHash::Sha256(s) => {
                self.sha256 = Some(s.clone());
                self.sha = None;
            }
        }
    }
}

/// FIPS 180-1 SHA-1 with serde state — `gix_hash::Hasher` state cannot be exported, so a
/// resumable PackWriter carries this instead (gc-and-repack-alarm).
#[derive(serde::Serialize, serde::Deserialize, Clone, Default)]
pub struct CkptSha1 {
    h: [u32; 5],
    len: u64,
    buf: Vec<u8>, // buf.len() < 64
}
impl CkptSha1 {
    pub fn new() -> Self {
        Self {
            h: [0x67452301, 0xEFCDAB89, 0x98BADCFE, 0x10325476, 0xC3D2E1F0],
            ..Self::default()
        }
    }
    pub fn update(&mut self, mut d: &[u8]) {
        self.len = self.len.wrapping_add(d.len() as u64);
        while !d.is_empty() {
            let n = (64usize).saturating_sub(self.buf.len()).min(d.len());
            let (a, b) = d.split_at(n);
            self.buf.extend_from_slice(a);
            d = b;
            if self.buf.len() == 64 {
                let mut x = [0u8; 64];
                x.copy_from_slice(&self.buf);
                self.block(&x);
                self.buf.clear();
            }
        }
    }
    fn block(&mut self, b: &[u8; 64]) {
        let mut w = [0u32; 80];
        for (i, c) in b.chunks_exact(4).enumerate() {
            w[i] = u32::from_be_bytes([c[0], c[1], c[2], c[3]]);
        }
        for i in 16..80 {
            w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
        }
        let mut s = self.h;
        for (i, wi) in w.iter().enumerate() {
            let (f, k) = match i {
                0..=19 => ((s[1] & s[2]) | (!s[1] & s[3]), 0x5A827999u32),
                20..=39 => (s[1] ^ s[2] ^ s[3], 0x6ED9EBA1),
                40..=59 => ((s[1] & s[2]) | (s[1] & s[3]) | (s[2] & s[3]), 0x8F1BBCDC),
                _ => (s[1] ^ s[2] ^ s[3], 0xCA62C1D6),
            };
            s = [
                s[0].rotate_left(5).wrapping_add(f).wrapping_add(s[4]).wrapping_add(k).wrapping_add(*wi),
                s[0],
                s[1].rotate_left(30),
                s[2],
                s[3],
            ];
        }
        for (h, x) in self.h.iter_mut().zip(s) {
            *h = h.wrapping_add(x);
        }
    }
    pub fn fin(mut self) -> [u8; 20] {
        let bits = self.len.wrapping_mul(8);
        self.update(&[0x80]);
        while self.buf.len() != 56 {
            self.update(&[0]);
        }
        self.update(&bits.to_be_bytes());
        let mut o = [0u8; 20];
        for (i, h) in self.h.iter_mut().enumerate() {
            o[i * 4..i * 4 + 4].copy_from_slice(&h.to_be_bytes());
        }
        o
    }
}

/// FIPS 180-4 SHA-256 with serde state — the resumable-pack sha1's sibling for
/// sha256-format repos (same reason gix_hash::Hasher can't ride a checkpoint).
#[derive(serde::Serialize, serde::Deserialize, Clone, Default)]
pub struct CkptSha256 {
    h: [u32; 8],
    len: u64,
    buf: Vec<u8>, // buf.len() < 64
}
impl CkptSha256 {
    pub fn new() -> Self {
        Self {
            h: [
                0x6A09E667, 0xBB67AE85, 0x3C6EF372, 0xA54FF53A, 0x510E527F, 0x9B05688C, 0x1F83D9AB,
                0x5BE0CD19,
            ],
            ..Self::default()
        }
    }
    pub fn update(&mut self, mut d: &[u8]) {
        self.len = self.len.wrapping_add(d.len() as u64);
        while !d.is_empty() {
            let n = (64usize).saturating_sub(self.buf.len()).min(d.len());
            let (a, b) = d.split_at(n);
            self.buf.extend_from_slice(a);
            d = b;
            if self.buf.len() == 64 {
                let mut x = [0u8; 64];
                x.copy_from_slice(&self.buf);
                self.block(&x);
                self.buf.clear();
            }
        }
    }
    fn block(&mut self, b: &[u8; 64]) {
        const K: [u32; 64] = [
            0x428A2F98, 0x71374491, 0xB5C0FBCF, 0xE9B5DBA5, 0x3956C25B, 0x59F111F1, 0x923F82A4,
            0xAB1C5ED5, 0xD807AA98, 0x12835B01, 0x243185BE, 0x550C7DC3, 0x72BE5D74, 0x80DEB1FE,
            0x9BDC06A7, 0xC19BF174, 0xE49B69C1, 0xEFBE4786, 0x0FC19DC6, 0x240CA1CC, 0x2DE92C6F,
            0x4A7484AA, 0x5CB0A9DC, 0x76F988DA, 0x983E5152, 0xA831C66D, 0xB00327C8, 0xBF597FC7,
            0xC6E00BF3, 0xD5A79147, 0x06CA6351, 0x14292967, 0x27B70A85, 0x2E1B2138, 0x4D2C6DFC,
            0x53380D13, 0x650A7354, 0x766A0ABB, 0x81C2C92E, 0x92722C85, 0xA2BFE8A1, 0xA81A664B,
            0xC24B8B70, 0xC76C51A3, 0xD192E819, 0xD6990624, 0xF40E3585, 0x106AA070, 0x19A4C116,
            0x1E376C08, 0x2748774C, 0x34B0BCB5, 0x391C0CB3, 0x4ED8AA4A, 0x5B9CCA4F, 0x682E6FF3,
            0x748F82EE, 0x78A5636F, 0x84C87814, 0x8CC70208, 0x90BEFFFA, 0xA4506CEB, 0xBEF9A3F7,
            0xC67178F2,
        ];
        let mut w = [0u32; 64];
        for (i, c) in b.chunks_exact(4).enumerate() {
            w[i] = u32::from_be_bytes([c[0], c[1], c[2], c[3]]);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16].wrapping_add(s0).wrapping_add(w[i - 7]).wrapping_add(s1);
        }
        let mut s = self.h;
        for (i, wi) in w.iter().enumerate() {
            let e1 = s[4].rotate_right(6) ^ s[4].rotate_right(11) ^ s[4].rotate_right(25);
            let ch = (s[4] & s[5]) ^ (!s[4] & s[6]);
            let t1 = s[7]
                .wrapping_add(e1)
                .wrapping_add(ch)
                .wrapping_add(K[i])
                .wrapping_add(*wi);
            let e0 = s[0].rotate_right(2) ^ s[0].rotate_right(13) ^ s[0].rotate_right(22);
            let mj = (s[0] & s[1]) ^ (s[0] & s[2]) ^ (s[1] & s[2]);
            let t2 = e0.wrapping_add(mj);
            s = [t1.wrapping_add(t2), s[0], s[1], s[2], s[3].wrapping_add(t1), s[4], s[5], s[6]];
        }
        for (h, x) in self.h.iter_mut().zip(s) {
            *h = h.wrapping_add(x);
        }
    }
    pub fn fin(mut self) -> [u8; 32] {
        let bits = self.len.wrapping_mul(8);
        self.update(&[0x80]);
        while self.buf.len() != 56 {
            self.update(&[0]);
        }
        self.update(&bits.to_be_bytes());
        let mut o = [0u8; 32];
        for (i, h) in self.h.iter_mut().enumerate() {
            o[i * 4..i * 4 + 4].copy_from_slice(&h.to_be_bytes());
        }
        o
    }
}

/// The checkpointable hasher a PackWriter carries — the variant follows the
/// repo's object format. Old checkpoints hold only `sha`, so WriterCkpt stores
/// them as two optional fields rather than a tagged enum (see its comment).
#[derive(Clone)]
pub enum CkptHash {
    Sha1(CkptSha1),
    Sha256(CkptSha256),
}
impl CkptHash {
    pub fn new(kind: gix_hash::Kind) -> Self {
        match kind {
            gix_hash::Kind::Sha256 => Self::Sha256(CkptSha256::new()),
            _ => Self::Sha1(CkptSha1::new()),
        }
    }
    pub fn update(&mut self, d: &[u8]) {
        match self {
            Self::Sha1(h) => h.update(d),
            Self::Sha256(h) => h.update(d),
        }
    }
    pub fn fin(self) -> Vec<u8> {
        match self {
            Self::Sha1(h) => h.fin().to_vec(),
            Self::Sha256(h) => h.fin().to_vec(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// FIPS 180-4 known-answer vectors: "" and "abc" — CkptSha256 is hand-rolled
    /// crypto, so a wrong round lands as a wrong pack trailer and fsck fails.
    #[test]
    fn sha256_known_answers() {
        let mut h = CkptSha256::new();
        assert_eq!(
            h.clone().fin().iter().map(|b| format!("{b:02x}")).collect::<String>(),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        h.update(b"abc");
        assert_eq!(
            h.fin().iter().map(|b| format!("{b:02x}")).collect::<String>(),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    /// The checkpoint round-trip a slice boundary relies on: split updates across
    /// a serde round-trip must hash identically to one-shot. Both algorithms.
    #[test]
    fn ckpt_hash_resume_roundtrip() {
        for kind in [gix_hash::Kind::Sha1, gix_hash::Kind::Sha256] {
            let mut live = CkptHash::new(kind);
            live.update(b"PACK");
            // checkpoint mid-stream, resume into a fresh hasher, finish there
            let mut ck = WriterCkpt {
                pos: 4,
                sha: None,
                sha256: None,
                count: 0,
                expected: 0,
                commit_lo: 0,
                commit_hi: 0,
            };
            ck.set_hash(&live);
            let json = serde_json::to_string(&ck).unwrap();
            let back: WriterCkpt = serde_json::from_str(&json).unwrap();
            let mut resumed = back.hash().unwrap();
            resumed.update(b"\x00\x00\x00\x02rest");
            live.update(b"\x00\x00\x00\x02rest");
            assert_eq!(resumed.fin(), live.fin());
        }
    }

    /// A6: the bound-parameter guard — 100 binds is the documented ceiling.
    #[test]
    fn bound_param_guard() {
        assert!(check_params(MAX_BOUND_PARAMS).is_ok());
        let e = check_params(MAX_BOUND_PARAMS + 1).unwrap_err();
        assert!(e.message().contains("bound params"), "{e}");
    }

    /// A pre-sha256 checkpoint (only `sha`) still parses and resumes.
    #[test]
    fn ckpt_legacy_sha_field() {
        let json = r#"{"pos":12,"sha":{"h":[1732584193,4023233417,2562383102,271733878,3285377520],"len":0,"buf":[]},"count":0,"expected":0,"commit_lo":0,"commit_hi":0}"#;
        let ck: WriterCkpt = serde_json::from_str(json).unwrap();
        assert!(matches!(ck.hash().unwrap(), CkptHash::Sha1(_)));
    }

    /// split_part hands the head off exact-size and keeps the tail in the
    /// buffer's existing allocation — the chunk held across upload_part
    /// never retains a part-sized-plus allocation for one part of data,
    /// and neither branch holds capacity + a second full-size copy.
    #[test]
    fn split_part_preserves_bytes_and_tight_tail() {
        let mut buf: Vec<u8> = (0..41u8).collect();
        buf.reserve_exact(64); // the part buffer's spare capacity
        let mut flat = Vec::new();
        while buf.len() >= 8 {
            let head = split_part(&mut buf, 8).unwrap();
            assert_eq!(head.len(), head.capacity(), "uploaded chunk is exact-size");
            flat.extend_from_slice(&head);
        }
        flat.extend_from_slice(&buf);
        assert_eq!(flat, (0..41u8).collect::<Vec<_>>(), "bytes and order preserved");
        // buf keeps its allocation — it is the reusable part buffer, not a
        // leak: the tightness contract is on the returned head
    }

    /// One branch for every split: the head is always an exact-size copy,
    /// the tail memmoves down inside the same allocation. A small tail
    /// (the split_off case the old code special-cased) gets the same tight
    /// head — the donor's spare capacity never rides along on the upload.
    #[test]
    fn split_part_branches_on_tail_vs_head() {
        // tail (15) > head (8): head exact-size, tail keeps the buffer
        let mut buf: Vec<u8> = (0..23u8).collect();
        let cap = buf.capacity();
        let head = split_part(&mut buf, 8).unwrap();
        assert_eq!(head, (0..8u8).collect::<Vec<_>>());
        assert_eq!(head.len(), head.capacity(), "head copy is exact-size");
        assert_eq!(buf.as_slice(), &(8..23u8).collect::<Vec<_>>()[..]);
        assert_eq!(buf.capacity(), cap, "tail keeps the part buffer");
        // tail (2) <= head (8): same form — head exact-size, tail tight
        let mut buf: Vec<u8> = (0..10u8).collect();
        let head = split_part(&mut buf, 8).unwrap();
        assert_eq!(head, (0..8u8).collect::<Vec<_>>());
        assert_eq!(head.len(), head.capacity(), "head is exact-size, never the donor's spare");
        assert_eq!(buf.as_slice(), &[8u8, 9]);
    }

    /// `read_entries_bound` prices the fetch before it allocates: every
    /// loc's entry copy (`loc.len` each — they all stay alive in the
    /// result) plus the LARGEST merged-span range buffer under the same
    /// READ_GAP/READ_SPAN rules read_entries merges by.
    #[test]
    fn read_entries_bound_counts_copies_and_max_span() {
        let id = oid(&"ab".repeat(20)).unwrap();
        let loc = |pack: &str, off: u64, len: u32| ObjLoc {
            pack: PackId(pack.into()),
            idx: 0,
            offset: off,
            len,
            kind: Kind::Blob,
            size: 1,
        };
        // the per-loc vec term: result tuples + the sorted ref vec
        let per_loc =
            std::mem::size_of::<(ObjectId, Vec<u8>)>() + std::mem::size_of::<&ObjLoc>();
        // nothing to read: slack only
        assert_eq!(Bucket::read_entries_bound(&[]), READ_SLACK);
        // one loc: its copy + its own span + vec space + slack
        let one = [(id, loc("p", 100, 1 << 20))];
        assert_eq!(Bucket::read_entries_bound(&one), (2 << 20) + per_loc + READ_SLACK);
        // neighbors under READ_GAP merge into ONE span that includes the
        // gap: 512 KiB + 512 KiB copies, a 1112 KiB range buffer
        let near = [(id, loc("p", 0, 512 << 10)), (id, loc("p", 600 << 10, 512 << 10))];
        let far = [(id, loc("p", 0, 512 << 10)), (id, loc("p", 100 << 20, 512 << 10))];
        assert_eq!(
            Bucket::read_entries_bound(&near),
            (1024 << 10) + (1112 << 10) + 2 * per_loc + READ_SLACK
        );
        // the same locs far apart never merge: the buffer is one loc's span
        assert_eq!(
            Bucket::read_entries_bound(&far),
            (1024 << 10) + (512 << 10) + 2 * per_loc + READ_SLACK
        );
        // a merge candidate whose span would pass READ_SPAN splits: the
        // buffer stays one <= READ_SPAN span, not the whole 8.1 MiB extent
        let split = [
            (id, loc("p", 0, 4 << 20)),
            (id, loc("p", (4 << 20) + (100 << 10), 4 << 20)),
        ];
        assert_eq!(Bucket::read_entries_bound(&split), (12 << 20) + 2 * per_loc + READ_SLACK);
        // different packs never merge even when their ranges overlap:
        // two 1 MiB copies, one 1 MiB buffer — a wrong merge would read 1.5 MiB
        let packs = [(id, loc("a", 0, 1 << 20)), (id, loc("b", 512 << 10, 1 << 20))];
        assert_eq!(Bucket::read_entries_bound(&packs), (3 << 20) + 2 * per_loc + READ_SLACK);
    }

    /// room grows capacity in whole-part steps — never Vec's doubling past
    /// one part plus the pending append.
    #[test]
    fn room_bounds_capacity_to_part_multiple() {
        let mut buf = vec![0u8; 16];
        room(&mut buf, 8, 1).unwrap(); // need 17 -> target 24, doubling would give 32
        assert!(buf.capacity() >= 17 && buf.capacity() < 32, "cap={}", buf.capacity());
        let mut buf = Vec::new();
        room(&mut buf, 8, 3).unwrap(); // small need -> one whole part
        assert!(buf.capacity() >= 3 && buf.capacity() <= 16, "cap={}", buf.capacity());
        room(&mut buf, 8, 5).unwrap(); // need 5 still fits the reserved part -> no regrow
        assert!(buf.capacity() <= 16, "cap={}", buf.capacity());
    }

    /// Codex #6: a budget failure at the seal aborts an ordinary finish's
    /// upload but KEEPS a resumable finish's checkpointed upload, so the
    /// import retry resumes instead of rebuilding its output.
    #[test]
    fn seal_budget_failure_aborts_only_ordinary_finish() {
        let spent = || {
            let mut b = ReqBudget::paid();
            b.used = b.max_subrequests;
            b
        };
        let (e, abort) = seal_charge(&mut spent(), Finish::Ordinary).unwrap_err();
        assert!(matches!(e, Error::Budget));
        assert!(abort, "an ordinary finish aborts its upload");
        let (e, abort) = seal_charge(&mut spent(), Finish::Resumable).unwrap_err();
        assert!(matches!(e, Error::Budget));
        assert!(!abort, "a resumable finish keeps its checkpointed upload");
        let mut fresh = ReqBudget::paid();
        assert!(seal_charge(&mut fresh, Finish::Resumable).is_ok());
        assert!(seal_charge(&mut fresh, Finish::Ordinary).is_ok());
        assert_eq!(fresh.used, 2, "the seal charges exactly one subrequest");
    }

    /// Claude r8 #1: the preflight is the NEW block. A retained 16 MiB
    /// buffer growing to 24 MiB allocates the full 24 MiB before it frees
    /// the old 16 MiB, so the bound is 24 MiB plus overhead — not the
    /// 8 MiB net delta. The old 16 MiB is already in the live reading, so
    /// counting it again would reject growth that fits.
    #[test]
    fn room_growth_counts_only_the_new_block() {
        const MIB: usize = 1 << 20;
        let mut buf: Vec<u8> = vec![0; 16 * MIB];
        let cap = buf.capacity();
        // need 16 MiB + 1 -> target 24 MiB in 8 MiB parts
        assert_eq!(room_target(buf.len(), cap, 8 * MIB, 1), 24 * MIB);
        assert_eq!(room_growth(&buf, 8 * MIB, 1), 24 * MIB + ALLOC_OVERHEAD);
        // the bound covers the new block room really allocates; the old
        // block is already in the live reading the guard adds this to
        let want = room_growth(&buf, 8 * MIB, 1);
        room(&mut buf, 8 * MIB, 1).unwrap();
        assert!(buf.capacity() <= want, "new block {} > bound {want}", buf.capacity());
        assert!(want < cap + buf.capacity(), "the old block must not be counted twice");
        // a fit allocates nothing and preflights nothing
        buf.truncate(MIB);
        assert_eq!(room_growth(&buf, 8 * MIB, MIB), 0);
        // an empty buffer pays its first part plus overhead only
        assert_eq!(room_growth(&Vec::new(), 8 * MIB, 3), 8 * MIB + ALLOC_OVERHEAD);
        assert_eq!(growth_incoming(7), 7 + ALLOC_OVERHEAD);
    }

    /// encoded_bound stays above the encoder's real output at the object
    /// cap: 16 MiB of incompressible bytes is deflate's worst case, and a
    /// bound that lands short lets `room` under-reserve so `extend` falls
    /// back to Vec's doubling — a part-buffer spike the guard can't see.
    /// Codex r8 #4: the finish path's growth. An 8 MiB buffer holding just
    /// under one part grows to 16 MiB for a 20-byte trailer, then splits
    /// one 8 MiB part off. The bound names both blocks. A tail that stays
    /// under one part with the trailer needs neither.
    #[test]
    fn finish_growth_counts_trailer_growth_and_split() {
        const MIB: usize = 1 << 20;
        let mut part: Vec<u8> = Vec::with_capacity(8 * MIB);
        part.resize(8 * MIB - 1, 0);
        assert_eq!(part.capacity(), 8 * MIB);
        assert_eq!(
            finish_growth(&part, 8 * MIB, 20),
            growth_incoming(16 * MIB) + growth_incoming(8 * MIB)
        );
        // the real finish allocations stay inside the bound
        let want = finish_growth(&part, 8 * MIB, 20);
        room(&mut part, 8 * MIB, 20).unwrap();
        part.extend_from_slice(&[1u8; 20]);
        let grown = part.capacity();
        let head = split_part(&mut part, 8 * MIB).unwrap();
        assert!(grown + head.capacity() <= want, "{} > {want}", grown + head.capacity());
        // a small tail: the trailer fits the capacity and no split runs
        let mut small: Vec<u8> = Vec::with_capacity(8 * MIB);
        small.resize(MIB, 0);
        assert_eq!(finish_growth(&small, 8 * MIB, 20), 0);
        // exactly one part with the trailer: no growth, one split
        let mut exact: Vec<u8> = Vec::with_capacity(8 * MIB);
        exact.resize(8 * MIB - 20, 0);
        assert_eq!(finish_growth(&exact, 8 * MIB, 20), growth_incoming(8 * MIB));
    }

    /// The writer's growth and split fail in-band, never panic, when the
    /// allocator refuses the block — and leave the buffer as it was.
    #[test]
    fn writer_growth_and_split_fail_in_band() {
        let _guard = crate::try_grow::HookGuard;
        let mut part: Vec<u8> = vec![7u8; 100];
        crate::try_grow::fail_above(Some(150));
        let grow = room(&mut part, 64, 100);
        crate::try_grow::fail_above(Some(10));
        let split_refused = split_part(&mut part, 50);
        crate::try_grow::fail_above(Some(150));
        let split = split_part(&mut part, 99);
        assert!(crate::try_grow::is_retry(&grow.unwrap_err()), "a 256-byte block over a 150-byte cap");
        assert!(crate::try_grow::is_retry(&split_refused.unwrap_err()));
        assert_eq!(split.unwrap().len(), 99, "a 99-byte head fits the cap, the refused split left all 100 bytes");
        assert_eq!(part, vec![7u8; 1]);
    }

    #[test]
    fn encoded_bound_covers_incompressible_max_obj() {
        let mut data = vec![0u8; MAX_OBJECT as usize];
        // xorshift64* — deterministic and incompressible to deflate
        let mut x = 0x9e37_79b9_7f4a_7c15u64;
        for b in &mut data {
            x ^= x >> 12;
            x ^= x << 25;
            x ^= x >> 27;
            *b = (x.wrapping_mul(0x2545_f491_4f6c_dd1d) >> 32) as u8;
        }
        let mut enc = Vec::new();
        codec::encode_entry(Kind::Blob, &data, &mut enc).unwrap();
        assert!(
            encoded_bound(data.len()) >= enc.len(),
            "bound {} < encoded {}",
            encoded_bound(data.len()),
            enc.len()
        );
    }

    impl BodyChunk for Vec<u8> {
        fn len(&self) -> usize {
            self.as_slice().len()
        }
        fn copy_into(&self, dst: &mut [u8]) {
            dst.copy_from_slice(self);
        }
    }
    impl BodyChunk for &[u8] {
        fn len(&self) -> usize {
            <[u8]>::len(self)
        }
        fn copy_into(&self, dst: &mut [u8]) {
            dst.copy_from_slice(self);
        }
    }

    /// A chunk that counts its copies, so a test can see that none ran.
    struct Counted(Vec<u8>, std::rc::Rc<std::cell::Cell<usize>>);
    impl BodyChunk for Counted {
        fn len(&self) -> usize {
            self.0.len()
        }
        fn copy_into(&self, dst: &mut [u8]) {
            self.1.set(self.1.get() + 1);
            dst.copy_from_slice(&self.0);
        }
    }

    /// A body stream that counts its polls and can panic when polled.
    struct Probe<C> {
        items: std::collections::VecDeque<C>,
        polls: std::rc::Rc<std::cell::Cell<usize>>,
        panic_on_poll: bool,
    }
    impl<C: Unpin> futures_util::Stream for Probe<C> {
        type Item = Result<C, Error>;
        fn poll_next(
            mut self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            assert!(!self.panic_on_poll, "the body stream must not be polled");
            self.polls.set(self.polls.get() + 1);
            std::task::Poll::Ready(self.items.pop_front().map(Ok))
        }
    }
    fn probe<C>(items: Vec<C>) -> (Probe<C>, std::rc::Rc<std::cell::Cell<usize>>) {
        let polls = std::rc::Rc::new(std::cell::Cell::new(0));
        (Probe { items: items.into(), polls: polls.clone(), panic_on_poll: false }, polls)
    }
    fn ready<F: std::future::Future>(f: F) -> F::Output {
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        match std::pin::pin!(f).poll(&mut cx) {
            std::task::Poll::Ready(v) => v,
            std::task::Poll::Pending => panic!("fill_exact over a ready stream must not pend"),
        }
    }

    /// The R2 read reserves `want` BEFORE it touches the body: with the
    /// fail hook just below `want`, the answer is the in-band memory
    /// Limit, the stream is never polled, and no chunk is copied. Draining
    /// chunks first, as the old reader did, panics in the probe.
    #[test]
    fn fill_exact_reserves_before_polling_the_body() {
        let _guard = crate::try_grow::HookGuard;
        let want = 4096;
        let copies = std::rc::Rc::new(std::cell::Cell::new(0));
        let (mut s, polls) = probe(vec![Counted(vec![7; want], copies.clone())]);
        s.panic_on_poll = true;
        crate::try_grow::fail_above(Some(want - 1));
        let r = ready(fill_exact(want, s));
        let err = r.unwrap_err();
        assert!(crate::try_grow::is_retry(&err), "{err}");
        assert_eq!(polls.get(), 0, "the stream must not be polled");
        assert_eq!(copies.get(), 0, "no chunk may be copied");
    }

    /// An over-length body fails at the first chunk that passes `want`:
    /// that chunk is not copied and later chunks are not polled.
    #[test]
    fn fill_exact_rejects_an_over_length_body_at_once() {
        let copies = std::rc::Rc::new(std::cell::Cell::new(0));
        let c = |b: &[u8]| Counted(b.to_vec(), copies.clone());
        let (s, polls) = probe(vec![c(&[1, 2]), c(&[3, 4, 5]), c(&[6]), c(&[7])]);
        let err = ready(fill_exact(4, s)).unwrap_err();
        assert!(matches!(&err, Error::Storage(m) if m == "r2 body longer than 4"), "{err}");
        assert_eq!(polls.get(), 2, "the stream must stop at the overflowing chunk");
        assert_eq!(copies.get(), 1, "the overflowing chunk must not be copied");
    }

    /// A body that ends short of `want` is the Storage error.
    #[test]
    fn fill_exact_rejects_a_short_body() {
        let (s, _) = probe(vec![vec![1u8, 2], vec![3]]);
        let err = ready(fill_exact(5, s)).unwrap_err();
        assert!(matches!(&err, Error::Storage(m) if m == "r2 body 3 bytes, wanted 5"), "{err}");
    }

    /// An exact multi-chunk body returns its bytes in order, an empty
    /// chunk included.
    #[test]
    fn fill_exact_joins_an_exact_multi_chunk_body() {
        let data: Vec<u8> = (0..=255u8).cycle().take(10_000).collect();
        let parts: Vec<&[u8]> = vec![&data[..1], &data[1..4096], &data[4096..4096], &data[4096..]];
        let (s, polls) = probe(parts);
        assert_eq!(ready(fill_exact(data.len(), s)).unwrap(), data);
        assert_eq!(polls.get(), 5, "four chunks, then the end");
    }

    /// want == 0: an empty stream, or one with only empty chunks, is an
    /// empty body. Any byte is over-length.
    #[test]
    fn fill_exact_handles_a_zero_length_body() {
        let (s, _) = probe(Vec::<Vec<u8>>::new());
        assert!(ready(fill_exact(0, s)).unwrap().is_empty());
        let (s, _) = probe(vec![Vec::<u8>::new()]);
        assert!(ready(fill_exact(0, s)).unwrap().is_empty());
        let (s, _) = probe(vec![vec![1u8]]);
        assert!(matches!(ready(fill_exact(0, s)), Err(Error::Storage(_))));
    }

    /// js_body reserves `len` through try_grow BEFORE fill runs: the
    /// fail hook's refusal is the in-band Limit and fill never executes;
    /// a fitting reservation returns the filled bytes.
    #[test]
    fn js_body_reserves_fallibly_then_fills() {
        let _guard = crate::try_grow::HookGuard;
        crate::try_grow::fail_above(Some(8));
        let mut filled = false;
        let err = js_body(16, |_| {
            filled = true;
            Ok(())
        })
        .unwrap_err();
        crate::try_grow::fail_above(None);
        assert!(crate::try_grow::is_retry(&err), "{err}");
        assert!(!filled, "fill must not run on a refused reservation");
        let v = js_body(4, |b| {
            b.copy_from_slice(&[1, 2, 3, 4]);
            Ok(())
        })
        .unwrap();
        assert_eq!(v, vec![1, 2, 3, 4]);
    }

    /// resume_tail_apply reserves the cleared buffer's room BEFORE
    /// clearing: a refused reservation is the in-band Limit and the part
    /// bytes stay exactly as they were — the writer's offset/count/commit
    /// counters, which the caller only writes on Ok, are untouched by
    /// construction. On success the returned tuple carries the folded
    /// counters and the part holds only the new tail.
    #[test]
    fn resume_tail_reserves_before_clearing() {
        let _guard = crate::try_grow::HookGuard;
        // a buffer with a live tail to lose if the order were wrong
        let mut part: Vec<u8> = Vec::with_capacity(4 * 1024);
        part.extend_from_slice(&[9u8; 100]);
        let (offset, count, lo, hi) = (500u64, 3u32, 10u64, 20u64);
        let tail = vec![7u8; 5 * 1024]; // bigger than the 4 KiB capacity -> must reserve
        // the cleared buffer needs one whole 8 KiB part — over capacity —
        // refuse anything under it
        let st = TailState { offset, count, commit_lo: lo, commit_hi: hi };
        crate::try_grow::fail_above(Some(2 * 1024));
        let e = resume_tail_apply(&mut part, 8 * 1024, &tail, st, 1, 5, 30).unwrap_err();
        crate::try_grow::fail_above(None);
        assert!(crate::try_grow::is_retry(&e), "{e}");
        assert_eq!(part, vec![9u8; 100], "a refused reservation must not clear the part");
        // success: part holds only the tail; counters fold correctly
        let st = resume_tail_apply(&mut part, 8 * 1024, &tail, st, 1, 5, 30).unwrap();
        assert_eq!(part, tail);
        assert_eq!(
            (st.offset, st.count, st.commit_lo, st.commit_hi),
            (offset - 100 + tail.len() as u64, 4, 5, 30)
        );
    }
}
