//! Phase one of the two-phase push (CONTRACTS.md 2.4-2.5, 3 ordering), in the edge Worker.
//! Ported from research/proofs/two-phase-push.md.

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::collections::{BTreeMap, HashMap};

use gix_hash::ObjectId;
use gix_pack::data::entry::Header;
use worker::{Delay, Env, Stub};

use crate::edge::BodyReader;
use crate::error::Error;
use crate::store::{keys, Bucket, ObjLoc, ObjRow, PackId, PackMeta, PackWriter, PushId};
use crate::wire::http::{stub_json, stub_json_body, RepoRoute};
use crate::ReqBudget;

pub(crate) const MAX_LINKS: usize = 1_000_000; // A5
/// Rows per /_do/push/index POST *and* per edge flush in `one_entry`: the
/// resolve loop posts every INDEX_BATCH rows instead of collecting 10,000,
/// so neither the rows Vec nor one serialized body can hold more than a
/// batch — pass-B memory stays bounded mid-push. The DO accepts up to
/// 10,000 rows per post; `index_chunks` still splits defensively.
pub(crate) const INDEX_BATCH: usize = 2_500;
/// Serialized bytes per /_do/push/index body. Rows alone do not bound a
/// body: a commit row carries every parent id, so 2,500 merge-heavy rows
/// can serialize to tens of MiB. A batch flushes early once its JSON would
/// pass this mark. 4 MiB keeps one body plus its request copy at ~8 MiB,
/// the size of one window read. A single row bigger than this still posts,
/// alone, and its own preflight judges it.
pub(crate) const INDEX_POST_BYTES: usize = 4 << 20;

/// The /_do/push/index body, borrowed — serializes rows straight from the
/// slice instead of building a serde_json::Value tree per row. `json_len`
/// is the exact serialized length, so the body is built at that capacity
/// with no growth.
#[derive(serde::Serialize)]
struct IndexPost<'a> {
    pack: IndexPack<'a>,
    rows: &'a [ObjRow],
    #[serde(skip)]
    json_len: usize,
}

/// An io::Write that only counts — measures a value's JSON length without
/// allocating the text.
struct JsonLen(usize);
impl std::io::Write for JsonLen {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.0 = self.0.saturating_add(b.len());
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn json_len(v: &impl serde::Serialize) -> Result<usize, Error> {
    let mut w = JsonLen(0);
    serde_json::to_writer(&mut w, v).map_err(|e| Error::Internal(e.to_string()))?;
    Ok(w.0)
}

/// Peak bytes one index post allocates: the exact-capacity JSON body plus
/// the request-body copy `internal_request` makes, plus one allocation's
/// overhead. Posts run one at a time and each body frees before the next.
fn post_alloc(json_len: usize) -> usize {
    json_len.saturating_mul(2).saturating_add(crate::store::ALLOC_OVERHEAD)
}

/// The memory-guard preflight for posting `rows` with `meta`: the largest
/// single post's allocation. Pure, so the in-memory test sink prices its
/// posts the same way.
pub(crate) fn index_post_bound(pack: &str, push: &str, meta: &PackMeta, rows: &[ObjRow]) -> Result<usize, Error> {
    Ok(index_chunks(pack, push, 0, meta, rows)?
        .iter()
        .map(|p| post_alloc(p.json_len))
        .max()
        .unwrap_or(0))
}
#[derive(Clone, Copy, serde::Serialize)]
struct IndexPack<'a> {
    id: &'a str,
    push_id: &'a str,
    count: u32,
    bytes: u64,
    commit_lo: i64,
    commit_hi: u64,
}

fn unpack(m: impl Into<String>) -> Error {
    Error::Unpack(m.into())
}

/// Phase one. Returns (the pack id commit_push step 3 flips to live, tag sha -> target for
/// `refs.peeled`). pack is None when the push carried no objects (2.4). On Ok: pending/ and packs/ are durable, every ObjRow and the
/// packs row ('ingesting') are in the DO, every link resolves (2.5). Nothing is visible to
/// readers until the caller's /_do/push/commit (section 3 ordering 1-2-3).
#[allow(clippy::too_many_arguments)]
pub async fn run(
    body: &mut BodyReader,
    bucket: &Bucket,
    stub: &Stub,
    repo: &RepoRoute,
    push: &PushId,
    kind: gix_hash::Kind,
    env: &Env,
    budget: &mut ReqBudget,
) -> Result<(Option<PackId>, HashMap<ObjectId, ObjectId>), Error> {
    if !body.fill(12).await? {
        return if body.buffered().is_empty() {
            Ok((None, HashMap::new())) // delete-only: no PACK
        } else {
            Err(unpack("pack header truncated"))
        };
    }
    let head: [u8; 12] = body
        .buffered()
        .get(..12)
        .and_then(|s| s.try_into().ok())
        .ok_or_else(|| unpack("pack header"))?;
    let (_, count) = gix_pack::data::header::decode(&head).map_err(|e| unpack(e.to_string()))?;
    let ds = kind.len_in_bytes();
    if count == 0 {
        // new ref at an existing commit: header + trailer only
        if !body.fill(12 + ds).await? || body.fill(13 + ds).await? {
            return Err(unpack("bad empty pack"));
        }
        let mut h = gix_hash::hasher(kind);
        h.update(&head);
        let want = h.try_finalize().map_err(|_| unpack("hash collision"))?;
        if body.buffered().get(12..12 + ds) != Some(want.as_slice()) {
            return Err(unpack("bad pack checksum"));
        }
        return Ok((None, HashMap::new()));
    }
    let (entries, _) = super::ingest::stream_to_pending(body, bucket, push, kind, budget).await?;
    // thin-pack bases: count first (no allocation), then preflight and
    // reserve the id list once
    let bases = collect_bases(&entries, budget.lease.as_ref())?;
    let mut external: HashMap<ObjectId, ObjLoc> = HashMap::new();
    for chunk in bases.chunks(1_000) {
        // one lookup's response (<= 1,000 locs) is deserialized from the
        // DO's JSON; the map's growth for its hits is preflighted and
        // reserved before the rows move in
        let found = lookup(stub, repo, chunk, None, budget).await?;
        add_externals(&mut external, found, budget.lease.as_ref())?;
    }
    let pack = PackId::random()?;
    let mut sink = IndexSink { stub, repo, pack: pack.clone(), push: push.clone(), links: Vec::new(), tags: HashMap::new() };
    // Pass B is the isolate's memory-heavy phase: admit at most
    // GE_EDGE_INGEST_SLOTS concurrent ingests — BEFORE the 'ingesting' row
    // and the pack MPU exist, so a busy or cancelled wait leaves nothing
    // behind. Pass A above already streamed the body to R2, so waiting
    // here never holds the client's upload. The permit stays held through
    // out.finish + the final post_meta: the gated span ends there.
    let pack_bytes = entries
        .last()
        .map(|r| r.offset.saturating_add(u64::from(r.header_len)).saturating_add(u64::from(r.compressed_len)))
        .unwrap_or(0);
    let permit = admit_pass_b(env, budget, entries.len(), pack_bytes).await?;
    sink.post_meta(&PackMeta::EMPTY, &[], budget).await?; // packs row 'ingesting' before any part is uploaded
    let key = keys::pack(&bucket.repo, &pack);
    let mut out = match PackWriter::create(bucket, &key, u32::try_from(entries.len()).unwrap_or(u32::MAX), kind, budget).await {
        Ok(w) => w,
        Err(e) => return Err(e),
    };
    let pending = keys::pending(&bucket.repo, push);
    let mut any_sink = super::ingest::AnySink::Edge(&mut sink);
    let tail = match super::ingest::resolve_and_normalize(
        super::ingest::Reader::Bucket(bucket),
        &pending,
        &entries,
        super::ingest::Externals::Map(&external),
        &mut out,
        &mut any_sink,
        kind,
        budget,
    )
    .await
    {
        Ok(rows) => rows,
        Err(e) => {
            out.abort().await;
            return Err(e);
        }
    };
    drop(entries);
    drop(external);
    let meta = finish_writer(out, budget).await?; // section 3 (1): pack durable in R2
    // the tail rows' serialization is pass-B memory too: preflight the
    // largest post before any body is built
    let bound = sink.post_bound(&meta, &tail)?;
    super::ingest::mem_preflight(budget.lease.as_ref(), bound, "grow:index-final")?;
    sink.post_meta(&meta, &tail, budget).await?; // section 3 (2): tail rows + real meta
    drop(permit); // the gated span ends here; the link check below is light
    let mut links = std::mem::take(&mut sink.links);
    links.sort_unstable();
    links.dedup();
    for chunk in links.chunks(1_000) {
        // 2.5: links - {this pack} must be live; the DO counts this pack's own rows via `pack`
        if let Some(id) = lookup(stub, repo, chunk, Some(&pack), budget)
            .await?
            .into_iter()
            .find_map(|(id, l)| l.is_none().then_some(id))
        {
            return Err(unpack(format!("missing object {id}")));
        }
    }
    Ok((Some(pack), std::mem::take(&mut sink.tags))) // section 3 (3) is the caller's commit
}

/// The thin-pack base id list: count first (no allocation), preflight
/// and reserve once, then sort+dedup. run() needs the list, so the
/// "grow:bases" preflight cannot drop out of run() while the list stays.
fn collect_bases(entries: &[super::ingest::EntryRec], lease: Option<&Rc<Lease>>) -> Result<Vec<ObjectId>, Error> {
    let base_of = |r: &super::ingest::EntryRec| match r.kind_or_delta {
        Header::RefDelta { base_id } => Some(base_id),
        _ => None,
    };
    let n_bases = entries.iter().filter(|r| base_of(r).is_some()).count();
    super::ingest::mem_preflight(
        lease,
        crate::store::growth_incoming(n_bases.saturating_mul(std::mem::size_of::<ObjectId>())),
        "grow:bases",
    )?;
    let mut bases: Vec<ObjectId> = crate::try_grow::with_capacity(n_bases)?;
    bases.extend(entries.iter().filter_map(base_of));
    bases.sort_unstable();
    bases.dedup();
    Ok(bases)
}

/// The writer steps `finish_writer` needs: PackWriter in production, an
/// in-memory fake in native tests.
trait FinishWriter {
    fn finish_bound(&self) -> usize;
    async fn abort(self);
    async fn finish(self, budget: &mut ReqBudget) -> Result<PackMeta, Error>;
}
impl FinishWriter for PackWriter {
    fn finish_bound(&self) -> usize {
        PackWriter::finish_bound(self)
    }
    async fn abort(self) {
        PackWriter::abort(self).await
    }
    async fn finish(self, budget: &mut ReqBudget) -> Result<PackMeta, Error> {
        PackWriter::finish(self, budget).await
    }
}

/// Finish the pack writer. finish grows the part buffer for the trailer
/// and splits one part off it: the "grow:writer-finish" preflight prices
/// both while the upload can still abort cleanly. The growth itself is
/// fallible too, and finish aborts on its failure. run() needs the
/// PackMeta, so the preflight cannot drop out of run() while finish stays.
async fn finish_writer<W: FinishWriter>(out: W, budget: &mut ReqBudget) -> Result<PackMeta, Error> {
    if let Err(e) = super::ingest::mem_preflight(budget.lease.as_ref(), out.finish_bound(), "grow:writer-finish") {
        out.abort().await;
        return Err(e);
    }
    out.finish(budget).await
}

/// Move one lookup's hits into the externals map: the "grow:externals"
/// preflight admits the map's growth for them, then the growth is
/// reserved fallibly before any row moves in. A refusal leaves the map
/// as it was. run() fills the map only here, so the preflight cannot drop
/// out of run() while the map is filled.
fn add_externals(
    external: &mut HashMap<ObjectId, ObjLoc>,
    found: Vec<(ObjectId, Option<ObjLoc>)>,
    lease: Option<&Rc<Lease>>,
) -> Result<(), Error> {
    let hits = found.iter().filter(|(_, l)| l.is_some()).count();
    let peak = super::ingest::map_growth_peak::<(ObjectId, ObjLoc)>(external.len(), external.capacity(), hits);
    super::ingest::mem_preflight(lease, peak, "grow:externals")?;
    crate::try_grow::map(external, hits)?;
    external.extend(found.into_iter().filter_map(|(id, l)| Some((id, l?))));
    Ok(())
}

pub struct IndexSink<'a> {
    stub: &'a Stub,
    repo: &'a RepoRoute,
    pack: PackId,
    push: PushId,
    pub links: Vec<ObjectId>,
    pub tags: HashMap<ObjectId, ObjectId>,
}
impl IndexSink<'_> {
    /// Preflight bytes for `post_meta(meta, rows)` — see `index_post_bound`.
    pub fn post_bound(&self, meta: &PackMeta, rows: &[ObjRow]) -> Result<usize, Error> {
        index_post_bound(&self.pack.0, &self.push.0, meta, rows)
    }
    /// One /_do/push/index call (<= 10,000 rows, 1.3).
    pub async fn post(&mut self, rows: &[ObjRow], budget: &mut ReqBudget) -> Result<(), Error> {
        self.post_meta(&PackMeta::EMPTY, rows, budget).await
    }
    pub async fn post_meta(
        &mut self,
        meta: &PackMeta,
        rows: &[ObjRow],
        budget: &mut ReqBudget,
    ) -> Result<(), Error> {
        for post in index_chunks(&self.pack.0, &self.push.0, self.links.len(), meta, rows)? {
            self.post_rows(&post, budget).await?;
        }
        Ok(())
    }
    /// Serialize one post at its exact length — no String growth — and
    /// send it. The caller preflighted `post_alloc(post.json_len)`.
    /// The body is one exact, fallible block; the request copy the stub
    /// call makes lives on the JS side and stays inside the preflight.
    async fn post_rows(&self, post: &IndexPost<'_>, budget: &mut ReqBudget) -> Result<(), Error> {
        let mut body = crate::try_grow::with_capacity(post.json_len)?;
        serde_json::to_writer(&mut body, post).map_err(|e| Error::Internal(e.to_string()))?;
        let _: serde_json::Value =
            stub_json_body(self.stub, self.repo, "/_do/push/index", body, budget).await?;
        Ok(())
    }
}

/// The /_do/push/index posts one post_meta call sends, in order — pure (no
/// stub) so tests count the chunks and check the meta on each. An empty
/// `rows` still yields exactly one post: it plants the 'ingesting' packs
/// row the whole two-phase ordering depends on. Every chunk carries the
/// same real meta — the DO upserts it idempotently. A post closes at
/// INDEX_BATCH rows OR when the next row would push its JSON past
/// INDEX_POST_BYTES, whichever comes first; each post records its exact
/// serialized length.
fn index_chunks<'a>(
    pack: &'a str,
    push: &'a str,
    links: usize,
    meta: &PackMeta,
    rows: &'a [ObjRow],
) -> Result<Vec<IndexPost<'a>>, Error> {
    if links > MAX_LINKS {
        return Err(Error::Limit("push references too many objects (1,000,000 max)".into()));
    }
    const SAFE_MAX: i64 = (1 << 53) - 1;
    let commit_lo = i64::try_from(meta.commit_lo).unwrap_or(SAFE_MAX).min(SAFE_MAX);
    let pack = IndexPack {
        id: pack,
        push_id: push,
        count: meta.count,
        bytes: meta.bytes,
        commit_lo,
        commit_hi: meta.commit_hi,
    };
    // `{"pack":{...},"rows":[]}` — each row adds its own JSON plus a comma
    // after the first
    let envelope = json_len(&IndexPost { pack, rows: &[], json_len: 0 })?;
    let mut posts = Vec::new();
    let (mut start, mut bytes) = (0usize, envelope);
    for (i, row) in rows.iter().enumerate() {
        let row_len = json_len(row)?;
        let n = i - start;
        let with_row = bytes.saturating_add(row_len).saturating_add(usize::from(n > 0));
        if n > 0 && (n >= INDEX_BATCH || with_row > INDEX_POST_BYTES) {
            posts.push(IndexPost { pack, rows: &rows[start..i], json_len: bytes });
            start = i;
            bytes = envelope.saturating_add(row_len);
        } else {
            bytes = with_row;
        }
    }
    posts.push(IndexPost { pack, rows: &rows[start..], json_len: bytes });
    Ok(posts)
}

#[derive(serde::Deserialize)]
struct LookupResponse {
    locs: Vec<Option<ObjLoc>>,
}

/// /_do/push/lookup for <= 1,000 ids (1.3, 7.3). `pack` = the caller's own ingesting pack counts (2.5).
pub async fn lookup(
    stub: &Stub,
    repo: &RepoRoute,
    ids: &[ObjectId],
    pack: Option<&PackId>,
    budget: &mut ReqBudget,
) -> Result<Vec<(ObjectId, Option<ObjLoc>)>, Error> {
    let hex: Vec<String> = ids.iter().map(ToString::to_string).collect();
    let res: LookupResponse =
        stub_json(stub, repo, "/_do/push/lookup", &serde_json::json!({ "ids": hex, "pack": pack }), budget).await?;
    if res.locs.len() != ids.len() {
        return Err(Error::Internal("lookup length".into()));
    }
    Ok(ids.iter().copied().zip(res.locs).collect())
}

// ---- pass-B admission gate (edge pushes only) ----

/// Concurrent pass-B ingests per isolate. The resolve/normalize/finish span
/// is the request's memory-heavy phase; pass A (stream_to_pending) already
/// finished by the time this gate runs, so waiting never holds the upload.
/// Default 1: two concurrent unsliced real-repo pushes in one isolate reached
/// 89 MiB live heap / 131 MiB wasm linear memory — over the documented 128 MB
/// limit — while one slot measured 66 MiB / 98 MiB.
const INGEST_SLOTS_DEFAULT: i64 = 1;
/// How long a push waits for a slot or a calm heap before answering busy.
/// Default 240 s: a large unsliced pass B can run ~90 s, and outwaiting
/// the queue beats rejecting the push busy. This is only the configured
/// upper bound — the request budget's own wall clock caps the effective
/// wait at (time left - GE_EDGE_INGEST_PASS_B_RESERVE_MS), because pass B
/// still needs that reserve to finish; a push that cannot cover it fails
/// busy up front instead of dying mid-pass on Error::Budget.
const INGEST_WAIT_MS_DEFAULT: i64 = 240_000;
/// Milliseconds of request budget pass B must still have for admission to
/// be worth it — the measured worst pass B for a ~26k-object push was
/// ~150 s wall. GE_EDGE_INGEST_PASS_B_RESERVE_MS overrides; <= 0 drops
/// the reserve.
const INGEST_PASS_B_RESERVE_MS_DEFAULT: i64 = 150_000;
/// Poll cadence while waiting — short enough that a freed slot is picked up
/// quickly, long enough that a queued push costs ~nothing.
const INGEST_POLL_MS: u64 = 100;
/// Live heap above this while a peer ingest runs means its leftovers are
/// still draining — a new pass-B waits rather than stacking on top.
/// Caveat: one large pass B holds ~60 MiB live, always above this mark, so
/// while it runs no second ingest admits regardless of GE_EDGE_INGEST_SLOTS
/// — effective concurrency for large pushes is 1 even with slots > 1.
const INGEST_LOW_WATER: usize = 40 << 20;
/// A permit older than this is treated as leaked: when the runtime cancels
/// an invocation, wasm-bindgen-futures can leak the Rust future instead of
/// dropping it, so the RAII release never runs. The request's own wall
/// clock (ReqBudget::MAX_MS) bounds any legitimate hold — every await in
/// the gated span charges the budget — plus a 30 s margin for the last
/// in-flight subrequest to land.
const PERMIT_LEASE_MS: i64 = ReqBudget::MAX_MS as i64 + 30_000;
/// A head ticket that has not polled for this long is evicted — its waiter
/// was leaked by the same cancellation path. Thirty seconds: a live waiter
/// whose timers the isolate delayed past the stale mark re-inserts its
/// ticket on the next poll (try_acquire), so only a truly dead head ever
/// reaches eviction — and it still clears well before the waiters' own
/// deadlines.
const TICKET_STALE_MS: i64 = 30_000;
/// How long an evicted permit fences admission after its eviction. A
/// live holder learns of its eviction at its next guard call or budget
/// charge, which come far more often than this, and then frees its
/// buffers. Past this mark the holder is a leak that will never drain:
/// the fence lets go, and the per-request memory guard and the fallible
/// reservations protect the next pass B instead. Without the expiry a
/// leaked holder above INGEST_LOW_WATER would block every later push
/// until the isolate recycles.
const EVICTED_FENCE_MS: i64 = TICKET_STALE_MS;
/// The in-band busy message, shared by the admit timeout and a peer-contended
/// MEM_HARD trip: a retry helps, smaller slices would not have.
pub(crate) const INGEST_BUSY_MSG: &str =
    "server busy: too many large pushes in progress; retry in a minute";

/// The gate's pure state — unit-tested without a worker::Env or the isolate.
/// `slots == 0` disables admission: every caller acquires, and `permits`
/// still counts so the low-water wait and the honest Limit keep working.
/// Waiters queue on FIFO tickets (`next_ticket` issues them, `waiting`
/// holds ticket -> last-poll ms in order): only the lowest live ticket may
/// take a freed slot, so later arrivals can't starve an earlier push.
///
/// Cancellation safety: when the runtime cancels an invocation,
/// wasm-bindgen-futures can leak the Rust future instead of dropping it —
/// RAII release then never runs and the gate would wedge (`permits` stuck
/// at capacity, or a dead head ticket blocking the FIFO). `permits`
/// (id -> acquire ms) and each ticket's poll stamp let `sweep` reclaim
/// both: a permit older than PERMIT_LEASE_MS frees its slot, and a head
/// ticket that has not polled for TICKET_STALE_MS leaves the queue. Every
/// reclaim logs once.
struct Admission {
    /// permit id -> acquire time (platform ms). `in_flight` IS this map's
    /// length — derived, so a swept permit can't desync the count.
    permits: BTreeMap<u64, i64>,
    next_permit: u64,
    slots: usize,
    next_ticket: u64,
    /// ticket -> last poll time (platform ms). Only the head's stamp
    /// matters for liveness — a stale head is evicted so the line can't
    /// wedge behind it, and a still-live waiter that was evicted early
    /// (isolate stalled its timers) re-inserts on its next poll.
    waiting: BTreeMap<u64, i64>,
    /// Permits the sweep evicted whose holders have not drained yet — the
    /// post-eviction fence. An evicted holder's buffers stay in the live
    /// heap until its future drops (or forever, if it leaked), so while
    /// this set is non-empty and the heap is busy nobody is admitted. An
    /// id leaves when its holder's release arrives; the whole set clears
    /// on the first poll that reads a calm heap, and each id expires
    /// EVICTED_FENCE_MS after its eviction. Value: the eviction time.
    evicted: BTreeMap<u64, i64>,
}

impl Admission {
    fn new(slots: usize) -> Self {
        Self {
            permits: BTreeMap::new(),
            next_permit: 0,
            slots,
            next_ticket: 0,
            waiting: BTreeMap::new(),
            evicted: BTreeMap::new(),
        }
    }
    /// Issue a FIFO ticket and queue the waiter behind every earlier one.
    fn enqueue(&mut self, now: i64) -> u64 {
        let t = self.next_ticket;
        self.next_ticket = self.next_ticket.wrapping_add(1);
        self.waiting.insert(t, now);
        t
    }
    /// A waiter gave up (timeout or cancel): its ticket leaves the queue so
    /// the next-oldest waiter becomes head — an abandoned ticket must never
    /// wedge the line. Removing an already-gone ticket is a no-op.
    fn leave(&mut self, ticket: u64) {
        self.waiting.remove(&ticket);
    }
    /// Reclaim what leaked futures left behind: permits older than
    /// PERMIT_LEASE_MS (any real holder is long dead by then — the request
    /// budget bounds the gated span) and head tickets that stopped polling
    /// TICKET_STALE_MS ago. One log line per reclaimed item; `now` is the
    /// platform clock the caller already reads on every poll.
    /// Every reclaimed permit joins `evicted`, the post-eviction fence: its
    /// holder's buffers still sit in the live heap, so admitting onto them
    /// would stack a second pass B on the first's leftovers.
    fn sweep(&mut self, now: i64) {
        let stale: Vec<(u64, i64)> = self
            .permits
            .iter()
            .filter(|(_, at)| now.saturating_sub(**at) > PERMIT_LEASE_MS)
            .map(|(id, at)| (*id, *at))
            .collect();
        for (id, at) in &stale {
            self.permits.remove(id);
            self.evicted.insert(*id, now);
            admit_log(format_args!(
                "git-edge ingest admit: reclaimed leaked permit {id} (held {}ms)",
                now.saturating_sub(*at)
            ));
        }
        // a fence entry outlives its holder's chance to drain: let go
        let expired: Vec<u64> = self
            .evicted
            .iter()
            .filter(|(_, at)| now.saturating_sub(**at) > EVICTED_FENCE_MS)
            .map(|(id, _)| *id)
            .collect();
        for id in expired {
            self.evicted.remove(&id);
            admit_log(format_args!(
                "git-edge ingest admit: evicted permit {id} never drained; fence expired, memory guard takes over"
            ));
        }
        while let Some((&t, &at)) = self.waiting.first_key_value() {
            if now.saturating_sub(at) <= TICKET_STALE_MS {
                break;
            }
            self.waiting.remove(&t);
            admit_log(format_args!(
                "git-edge ingest admit: evicted stale ticket {t} (no poll for {}ms)",
                now.saturating_sub(at)
            ));
        }
    }
    /// Admit the ticketed waiter iff it is the queue head AND a slot is free
    /// AND the heap is calm. Returns the new permit's id — the caller hands
    /// it back to `release` (RAII via IngestPermit). `now` refreshes the
    /// caller's poll stamp, then drives the lease sweeps. `heap_busy`
    /// reports live_bytes() over INGEST_LOW_WATER; it blocks only while a
    /// peer ingest is in flight (`permits` counts admitted ingests — the
    /// caller isn't one yet), so stale heap from finished work can't stall
    /// the queue and unrelated heap can't either. slots == 0 disables the
    /// gate: every ticket acquires — there is no contention to order.
    fn try_acquire(&mut self, ticket: u64, heap_busy: bool, now: i64) -> Option<u64> {
        // this call IS the waiter's poll — stamp it before the sweep so a
        // live head is never evicted for a gap it just closed. A ticket a
        // younger waiter's sweep already evicted goes back in at its old
        // BTreeMap position: the isolate can stall timers past
        // TICKET_STALE_MS while the waiter is still alive, and without the
        // re-insert it would wait out its whole bound behind a line it
        // silently left.
        match self.waiting.get_mut(&ticket) {
            Some(t) => *t = now,
            None => {
                self.waiting.insert(ticket, now);
            }
        }
        self.sweep(now);
        // a calm heap means every evicted holder's buffers are gone (or
        // never mattered): the fence has done its job
        if !heap_busy {
            self.evicted.clear();
        }
        if self.slots != 0 && self.waiting.first_key_value().map(|(t, _)| *t) != Some(ticket) {
            return None;
        }
        let in_flight = self.permits.len();
        // an evicted-but-undrained holder still counts as a peer on EVERY
        // poll until it drains, not only on the poll whose sweep evicted
        // it: its buffers are still live heap, so heap_busy keeps blocking
        // admission whoever polls next
        let peer = in_flight > 0 || !self.evicted.is_empty();
        let admit = self.slots == 0 || (in_flight < self.slots && !(heap_busy && peer));
        if !admit {
            return None;
        }
        self.waiting.remove(&ticket);
        let id = self.next_permit;
        self.next_permit = self.next_permit.wrapping_add(1);
        self.permits.insert(id, now);
        Some(id)
    }
    /// Release one permit by id — idempotent: a permit the sweep already
    /// reclaimed as leaked is simply absent, so a late Drop can't free a
    /// slot a newer permit now holds. An evicted holder's late release is
    /// the drain signal: its id leaves the post-eviction fence.
    fn release(&mut self, permit: u64) {
        self.permits.remove(&permit);
        self.evicted.remove(&permit);
    }
    /// Renew a live holder's lease on progress — every guard call and
    /// budget charge on the edge pass-B path lands here, so a working
    /// pass B can never age past PERMIT_LEASE_MS. Returns false when the
    /// permit is absent: the sweep already reclaimed it, which means the
    /// gate gave the slot to another waiter — this holder lost its right
    /// and must stop with an in-band Limit so its buffers free.
    fn renew(&mut self, permit: u64, now: i64) -> bool {
        match self.permits.get_mut(&permit) {
            Some(at) => {
                *at = now;
                true
            }
            None => false,
        }
    }
    /// Re-point the gate at this request's slot count; env is fixed per
    /// deployment, and `permits` must survive the rewrite.
    fn set_slots(&mut self, slots: usize) {
        self.slots = slots;
    }
    fn in_flight(&self) -> usize {
        self.permits.len()
    }
    #[cfg(test)]
    fn waiting_len(&self) -> usize {
        self.waiting.len()
    }
}

thread_local! {
    /// The isolate's one gate — wasm runs a single thread, so a thread_local
    /// is the whole isolate's state. Native tests get one per thread and
    /// exercise `Admission` directly; slots starts disabled until admit()
    /// reads env.
    static ADMISSION: RefCell<Admission> = RefCell::new(Admission::new(0));
    /// git-receive-pack requests inside the edge handler right now — pass A
    /// included (pass A isn't gated but still holds heap). The pass-B memory
    /// guard reads this to pick "server busy" over "smaller slices": a peer
    /// in pass A can push a lone pass B over MEM_HARD just like a peer in
    /// pass B can. Entries carry a start time and expire after
    /// PERMIT_LEASE_MS — the same leak shape as the permits: a cancelled
    /// invocation can leave the guard behind, and an evergreen count would
    /// make a lone too-big push report "busy" forever.
    static RECEIVE_PACKS: RefCell<ReceivePacks> = const { RefCell::new(ReceivePacks::new()) };
}

/// The isolate's in-flight receive-pack entries: id -> start ms. The map's
/// length IS the live count, so a swept entry can't desync it — the same
/// shape as `Admission::permits`.
struct ReceivePacks {
    entries: BTreeMap<u64, i64>,
    next: u64,
}
impl ReceivePacks {
    const fn new() -> Self {
        Self { entries: BTreeMap::new(), next: 0 }
    }
    /// Count one in-progress receive-pack request; returns its entry id.
    fn enter(&mut self, now: i64) -> u64 {
        self.sweep(now);
        let id = self.next;
        self.next = self.next.wrapping_add(1);
        self.entries.insert(id, now);
        id
    }
    /// The request ended (or its guard dropped): its entry leaves. A late
    /// drop after the sweep reclaimed the entry is a no-op.
    fn leave(&mut self, id: u64) {
        self.entries.remove(&id);
    }
    /// Reclaim entries older than PERMIT_LEASE_MS — a real request's wall
    /// clock (ReqBudget::MAX_MS) plus margin bounds any legitimate hold.
    /// One log line per reclaimed entry.
    fn sweep(&mut self, now: i64) {
        let stale: Vec<(u64, i64)> = self
            .entries
            .iter()
            .filter(|(_, at)| now.saturating_sub(**at) > PERMIT_LEASE_MS)
            .map(|(id, at)| (*id, *at))
            .collect();
        for (id, at) in stale {
            self.entries.remove(&id);
            admit_log(format_args!(
                "git-edge ingest admit: reclaimed leaked receive-pack entry {id} (held {}ms)",
                now.saturating_sub(at)
            ));
        }
    }
    /// Refresh a live request's lease stamp — re-inserts if the sweep
    /// already reclaimed the entry: a request that is still charging is
    /// definitionally alive, so its peer count restores rather than wedging.
    fn renew(&mut self, id: u64, now: i64) {
        self.entries.insert(id, now);
    }
    /// Live entries after the lease sweep — `now` is injected so tests run
    /// natively (js_sys::Date is wasm-only).
    fn len(&mut self, now: i64) -> usize {
        self.sweep(now);
        self.entries.len()
    }
}

/// One request's lease handle: the ids THIS request holds in the isolate's
/// shared gate tables — its RECEIVE_PACKS entry and, inside the gated span,
/// its pass-B permit. Request-owned, never global: the request's ReqBudget
/// and its pass-B memory guard each carry an Rc to it, so only the owning
/// request's charges and guard calls renew or check its leases. A bystander
/// (a fetch, another push's pass A) charges its own budget and never
/// touches this one — a leaked permit therefore ages out on schedule, and
/// an evicted holder cannot renew a newer holder's permit by mistake.
#[derive(Default)]
pub struct Lease {
    permit: Cell<Option<u64>>,
    pack: Cell<Option<u64>>,
}
impl Lease {
    /// Renew every lease this request holds — the pass-B permit's and the
    /// RECEIVE_PACKS entry's — so a working push tracks its progress instead
    /// of aging to the 270 s sweep. Called from `ReqBudget::charge` (every
    /// subrequest) and from the pass-B memory guard (every guard call). An
    /// absent permit means the sweep evicted it and the gate may have given
    /// the slot away: the holder stops in-band, freeing its buffers.
    /// try_with: teardown drops must not panic.
    pub(crate) fn renew(&self) -> Result<(), Error> {
        self.renew_at(crate::platform::now_ms())
    }
    fn renew_at(&self, now: i64) -> Result<(), Error> {
        if let Some(id) = self.permit.get() {
            let ok = ADMISSION.try_with(|a| a.borrow_mut().renew(id, now)).unwrap_or(false);
            if !ok {
                return Err(Error::Limit(INGEST_BUSY_MSG.into()));
            }
        }
        if let Some(id) = self.pack.get() {
            let _ = RECEIVE_PACKS.try_with(|c| c.borrow_mut().renew(id, now));
        }
        Ok(())
    }
}

/// Counts one in-progress receive-pack request for the isolate — held for
/// the whole edge handler so pass-A heap pressure counts toward the memory
/// guard's busy-vs-slices message. It owns the request's `Lease`; hand
/// `lease()` to the request's ReqBudget. try_with: a drop during
/// thread-local teardown must not panic.
pub(crate) struct ReceivePackGuard {
    id: u64,
    lease: Rc<Lease>,
}
impl ReceivePackGuard {
    pub(crate) fn new() -> Self {
        Self::at(crate::platform::now_ms())
    }
    fn at(now: i64) -> Self {
        let id = RECEIVE_PACKS.with(|c| c.borrow_mut().enter(now));
        let lease = Rc::new(Lease::default());
        lease.pack.set(Some(id));
        Self { id, lease }
    }
    /// The request's lease handle — `ReqBudget::leased` takes it.
    pub(crate) fn lease(&self) -> Rc<Lease> {
        Rc::clone(&self.lease)
    }
}
impl Drop for ReceivePackGuard {
    fn drop(&mut self) {
        let _ = RECEIVE_PACKS.try_with(|c| c.borrow_mut().leave(self.id));
        // a budget that outlives the guard must not re-insert the entry
        self.lease.pack.set(None);
    }
}

/// In-progress receive-pack requests in this isolate, the caller's own
/// included — mem_guard's "is the isolate busy" signal.
pub(crate) fn receive_packs() -> usize {
    receive_packs_at(crate::platform::now_ms())
}

/// `receive_packs` with an injected clock — the lease sweep is testable
/// natively (js_sys::Date is wasm-only, so tests pass `now` explicitly).
fn receive_packs_at(now: i64) -> usize {
    RECEIVE_PACKS.with(|c| c.borrow_mut().len(now))
}

/// Pass-B ingests inside the gated span, this request's permit included —
/// the admit log reads it for diagnostics.
pub(crate) fn ingest_in_flight() -> usize {
    ADMISSION.with(|a| a.borrow().in_flight())
}

/// RAII slot release — every return/error path out of the gated span frees
/// it. The permit id makes the release precise: if the lease sweep already
/// reclaimed a leaked permit, this drop is a no-op instead of freeing a
/// slot a newer permit now holds. The owning request's `Lease` carries the
/// id while the permit lives, and drops it here. try_with: a permit dropped
/// during thread-local teardown must not panic.
struct IngestPermit {
    id: u64,
    lease: Option<Rc<Lease>>,
}
impl IngestPermit {
    fn held(id: u64, lease: Option<&Rc<Lease>>) -> Self {
        if let Some(l) = lease {
            l.permit.set(Some(id));
        }
        Self { id, lease: lease.cloned() }
    }
}
impl Drop for IngestPermit {
    fn drop(&mut self) {
        let _ = ADMISSION.try_with(|a| a.borrow_mut().release(self.id));
        if let Some(l) = &self.lease {
            if l.permit.get() == Some(self.id) {
                l.permit.set(None);
            }
        }
    }
}

/// RAII queue ticket — dropping it (timeout, cancel, any error path) pulls
/// the waiter out of line so the next-oldest ticket can acquire.
struct AdmitTicket(u64);
impl Drop for AdmitTicket {
    fn drop(&mut self) {
        let _ = ADMISSION.try_with(|a| a.borrow_mut().leave(self.0));
    }
}

/// GE_EDGE_INGEST_SLOTS / GE_EDGE_INGEST_WAIT_MS as pure values: absent or
/// unparseable falls back to the defaults (the env_i64 rule), slots <= 0
/// disables the gate, wait <= 0 fails fast on contention.
fn ingest_gate(slots: Option<i64>, wait: Option<i64>) -> (usize, i64) {
    let slots = slots.unwrap_or(INGEST_SLOTS_DEFAULT);
    let wait = wait.unwrap_or(INGEST_WAIT_MS_DEFAULT);
    (usize::try_from(slots).unwrap_or(0), wait.max(0))
}

/// GE_EDGE_INGEST_PASS_B_RESERVE_MS: the CAP on the size-aware reserve —
/// absent or unparseable falls back to the default; <= 0 drops the reserve
/// (the budget caps the wait by itself).
fn ingest_reserve(raw: Option<i64>) -> i64 {
    raw.unwrap_or(INGEST_PASS_B_RESERVE_MS_DEFAULT).max(0)
}
/// Floor on the size-aware reserve: even a one-object push needs some
/// pass-B time (window fetch + decode + write), so a queued wait never
/// assumes zero.
const INGEST_RESERVE_FLOOR_MS: i64 = 15_000;
/// Calibrated pass-B wall rate: the measured ~26k-object push took ~150 s,
/// so ~5.8 ms/object. Bytes add a second term for wide entries — 1 GiB of
/// compressed input is ~150 s of inflate+copy work at the same rate.
const INGEST_RESERVE_PER_OBJECT_MS: i64 = 6;
const INGEST_RESERVE_PER_BYTE_MS: i64 = 150_000; // ms per GiB
/// Size-aware pass-B reserve: what pass A learned (object count, pack
/// bytes) sizes the wall pass B will need. A ~26k-object push reserves the
/// full ~150 s cap and fails fast when the budget can't cover it; a ~5k
/// push reserves ~45 s and may wait correspondingly longer. The env cap
/// still wins — deployments tune the ceiling, not the estimate.
fn pass_b_reserve(objects: usize, pack_bytes: u64, cap_ms: i64) -> i64 {
    let by_objects = INGEST_RESERVE_PER_OBJECT_MS.saturating_mul(objects as i64);
    let by_bytes = INGEST_RESERVE_PER_BYTE_MS.saturating_mul(pack_bytes as i64) / (1 << 30);
    by_objects.max(by_bytes).max(INGEST_RESERVE_FLOOR_MS).min(cap_ms)
}

/// The admission wait bound in ms — `None` when the request budget cannot
/// cover the pass-B reserve, meaning even an instant admission would start
/// pass B already doomed to Error::Budget. Otherwise the configured wait
/// capped by (budget left - reserve): the request's own wall clock is the
/// real deadline.
fn admit_wait_ms(wait_ms: i64, budget_left_ms: i64, reserve_ms: i64) -> Option<i64> {
    let usable = budget_left_ms.saturating_sub(reserve_ms);
    (usable > 0).then(|| wait_ms.min(usable))
}

/// `admit_wait_ms` flattened for `admit_wait`: `None` becomes a zero wait,
/// NOT a rejection — admit_wait still makes its one first try_acquire, so
/// only a push that must queue is turned away. An uncontended push is
/// admitted even on a budget too thin for the reserve: the reserve is a
/// heuristic for how long a QUEUED push may wait, never a verdict on a
/// slot that is free right now.
fn admit_wait_bound(wait_ms: i64, budget_left_ms: i64, reserve_ms: i64) -> i64 {
    match admit_wait_ms(wait_ms, budget_left_ms, reserve_ms) {
        Some(w) => w,
        None => {
            admit_log(format_args!(
                "git-edge ingest admit: budget left cannot cover the {reserve_ms}ms pass-B reserve; one attempt only"
            ));
            0
        }
    }
}

/// console_log! aborts off-wasm (the native test binary has no JS console),
/// so admit diagnostics route through this shim — a no-op in tests.
#[cfg(target_arch = "wasm32")]
fn admit_log(msg: std::fmt::Arguments<'_>) {
    worker::console_log!("{msg}");
}
#[cfg(not(target_arch = "wasm32"))]
fn admit_log(_msg: std::fmt::Arguments<'_>) {}

/// Wait for a pass-B slot (bounded by the effective deadline admit_wait_ms
/// computed), then hold the RAII permit through `out.finish` + the final
/// post_meta. On timeout the push fails in-band — the body is already
/// durable in R2, so the client gets the report-status busy message, never
/// a bare 5xx. Clock, heap reading and sleep are injected so the loop's
/// tests run natively (js_sys::Date and worker::Delay are wasm-only).
async fn admit_wait<F, SF, Fut>(
    wait_ms: i64,
    lease: Option<&Rc<Lease>>,
    heap_busy: F,
    now: impl Fn() -> i64,
    sleep: SF,
) -> Result<IngestPermit, Error>
where
    F: Fn() -> bool,
    SF: Fn(u64) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    // FIFO ticket up front: only the oldest live waiter may take a freed
    // slot. The RAII guard drops it from the queue on timeout/cancel; if
    // the future is leaked instead, the ticket's poll stamp goes stale and
    // the sweep evicts it once it reaches the head.
    let ticket = ADMISSION.with(|a| a.borrow_mut().enqueue(now()));
    let _ticket = AdmitTicket(ticket);
    let started = now();
    // the deadline bounds every attempt after the first — Codex's injected
    // clock acquired at 240,001 ms on a 100 ms bound because the check ran
    // only AFTER try_acquire. First try is unconditional (an uncontended
    // push admits even on a zero bound); every retry checks the bound first.
    let mut waited = false;
    loop {
        if waited && now() - started >= wait_ms {
            let waited_ms = now() - started;
            let n = ingest_in_flight();
            admit_log(format_args!("git-edge ingest admit: timed out after {waited_ms}ms (in_flight={n})"));
            return Err(Error::Limit(INGEST_BUSY_MSG.into()));
        }
        let busy = heap_busy();
        if let Some(permit) = ADMISSION.with(|a| a.borrow_mut().try_acquire(ticket, busy, now())) {
            if waited {
                let waited_ms = now() - started;
                let n = ingest_in_flight();
                admit_log(format_args!("git-edge ingest admit: waited {waited_ms}ms (in_flight={n})"));
            }
            return Ok(IngestPermit::held(permit, lease));
        }
        waited = true;
        sleep(INGEST_POLL_MS).await;
    }
}

/// Wait for a pass-B slot, bounded by GE_EDGE_INGEST_WAIT_MS *and* by what
/// the request budget has left after the pass-B reserve — the budget clock
/// started before pass A, so an uncapped wait could admit a push with no
/// time left and fail it mid-pass on Error::Budget instead of answering
/// busy. `budget` is read-only: waiting itself spends no subrequests.
async fn admit_pass_b(env: &Env, budget: &ReqBudget, objects: usize, pack_bytes: u64) -> Result<IngestPermit, Error> {
    let raw = |n: &str| env.var(n).ok().and_then(|v| v.to_string().parse::<i64>().ok());
    let (slots, wait_ms) = ingest_gate(raw("GE_EDGE_INGEST_SLOTS"), raw("GE_EDGE_INGEST_WAIT_MS"));
    let reserve_ms = pass_b_reserve(objects, pack_bytes, ingest_reserve(raw("GE_EDGE_INGEST_PASS_B_RESERVE_MS")));
    ADMISSION.with(|a| a.borrow_mut().set_slots(slots));
    // The reserve bounds only the CONTENTED wait: admit_wait always makes
    // one try_acquire before it reads the deadline, so a zero bound — the
    // None case included — still admits an uncontended push on a free slot
    // with a calm heap. Only a push that must queue pays the reserve check,
    // because pass B itself needs that time.
    let wait_ms = admit_wait_bound(wait_ms, budget.ms_left() as i64, reserve_ms);
    admit_wait(
        wait_ms,
        budget.lease.as_ref(),
        || crate::mem::live_bytes() > INGEST_LOW_WATER,
        crate::platform::now_ms,
        |ms: u64| Delay::from(std::time::Duration::from_millis(ms)),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A writer that records whether finish or abort ran.
    struct FakeOut {
        bound: usize,
        done: Rc<Cell<&'static str>>,
    }
    impl FinishWriter for FakeOut {
        fn finish_bound(&self) -> usize {
            self.bound
        }
        async fn abort(self) {
            self.done.set("abort");
        }
        async fn finish(self, _budget: &mut ReqBudget) -> Result<PackMeta, Error> {
            self.done.set("finish");
            Ok(PackMeta::EMPTY)
        }
    }

    fn test_budget() -> ReqBudget {
        ReqBudget { max_subrequests: 9_000, used: 0, started_ms: crate::wall_now(), max_ms: ReqBudget::MAX_MS, sink: None, lease: None }
    }

    fn oid(n: u32) -> ObjectId {
        let mut b = [0u8; 20];
        b[..4].copy_from_slice(&n.to_be_bytes());
        ObjectId::from_bytes_or_panic(&b)
    }

    fn loc(n: u32) -> ObjLoc {
        ObjLoc { pack: PackId("p".into()), idx: n, offset: 0, len: 1, kind: gix_object::Kind::Blob, size: 1 }
    }

    /// Two thin-pack entries, each a ref-delta on its own base — enough
    /// for collect_bases to count, reserve and return two ids.
    fn ref_delta_entries() -> Vec<super::super::ingest::EntryRec> {
        use gix_pack::data::entry::Header;
        vec![
            super::super::ingest::EntryRec { offset: 12, header_len: 5, kind_or_delta: Header::RefDelta { base_id: oid(1) }, compressed_len: 10, size: 4 },
            super::super::ingest::EntryRec { offset: 30, header_len: 5, kind_or_delta: Header::RefDelta { base_id: oid(2) }, compressed_len: 10, size: 4 },
        ]
    }

    /// The line of the try_grow call that ends `snippet`, found in this
    /// file's production code — ingest.rs's guarded_site twin, so a test
    /// names a guarded site by its code, not by a line number.
    fn guarded_site(snippet: &str) -> (&'static str, u32) {
        let src = include_str!("run.rs");
        let prod = &src[..src.find("#[cfg(test)]\nmod tests").expect("the tests module")];
        let hits: Vec<usize> = prod.match_indices(snippet).map(|(i, _)| i).collect();
        assert_eq!(hits.len(), 1, "snippet must name one site: {snippet:?}");
        let end = hits[0] + snippet.len();
        (file!(), u32::try_from(prod[..end].matches('\n').count() + 1).unwrap())
    }

    /// The three run()-side growth preflights live inside the steps whose
    /// results run() needs: the base list, the filled externals map and
    /// the finished pack's meta. Each step records its tag with the bytes
    /// it priced. Deleting the preflight inside any step fails here.
    #[test]
    fn run_side_growth_preflights_record_their_tags() {
        use gix_pack::data::entry::Header;
        let _guard = crate::try_grow::HookGuard;
        let entries = vec![
            super::super::ingest::EntryRec { offset: 12, header_len: 5, kind_or_delta: Header::RefDelta { base_id: oid(1) }, compressed_len: 10, size: 4 },
            super::super::ingest::EntryRec { offset: 30, header_len: 5, kind_or_delta: Header::RefDelta { base_id: oid(2) }, compressed_len: 10, size: 4 },
        ];
        crate::mem::set_live_override(Some(std::rc::Rc::new(|| 16 << 20)));
        crate::pack::ingest::clear_preflights();
        let bases = collect_bases(&entries, None).unwrap();
        assert_eq!(bases.len(), 2);
        let mut ext = HashMap::new();
        add_externals(&mut ext, bases.iter().map(|id| (*id, Some(loc(1)))).collect(), None).unwrap();
        assert_eq!(ext.len(), 2);
        let done = Rc::new(Cell::new(""));
        let mut budget = test_budget();
        let out = FakeOut { bound: 1 << 20, done: done.clone() };
        block_on(finish_writer(out, &mut budget)).unwrap();
        assert_eq!(done.get(), "finish");
        let sites = crate::pack::ingest::preflight_sites();
        let tags: Vec<_> = sites.iter().map(|(t, _)| *t).collect();
        assert_eq!(tags, ["grow:bases", "grow:externals", "grow:writer-finish"], "one record per step, in run() order");
        assert_eq!(sites[1].1, super::super::ingest::map_growth_peak::<(ObjectId, ObjLoc)>(0, 0, 2));
        assert_eq!(sites[2].1, 1 << 20, "the finish preflight prices the writer's finish_bound");
    }

    /// A refused finish preflight aborts the upload and never finishes.
    #[test]
    fn finish_writer_refusal_aborts_the_upload() {
        let _guard = crate::try_grow::HookGuard;
        crate::mem::set_live_override(Some(std::rc::Rc::new(|| 200 << 20)));
        let done = Rc::new(Cell::new(""));
        let mut budget = test_budget();
        let res = block_on(finish_writer(FakeOut { bound: 1 << 20, done: done.clone() }, &mut budget));
        assert!(matches!(res, Err(Error::Limit(_))), "a refused finish preflight is the Limit");
        assert_eq!(done.get(), "abort");
    }

    /// Codex r8 #3: the externals map's growth for a lookup's hits is
    /// preflighted (misses cost nothing) and reserved fallibly; a refusal
    /// leaves the map as it was.
    #[test]
    fn add_externals_preflights_and_reserves_hits() {
        let _guard = crate::try_grow::HookGuard;
        let found: Vec<_> = (0..1_000u32).map(|n| (oid(n), (n % 2 == 0).then(|| loc(n)))).collect();
        let mut ext = HashMap::new();
        crate::mem::set_live_override(Some(std::rc::Rc::new(|| 16 << 20)));
        crate::pack::ingest::clear_preflights();
        add_externals(&mut ext, found.clone(), None).unwrap();
        let seen = crate::pack::ingest::preflight_sites();
        assert_eq!(ext.len(), 500, "misses never enter the map");
        let want = super::super::ingest::map_growth_peak::<(ObjectId, ObjLoc)>(0, 0, 500);
        assert!(want > 0);
        assert_eq!(seen, vec![("grow:externals", want)]);
        // a refused preflight and a refused reservation change nothing
        let more: Vec<_> = (1_000..3_000u32).map(|n| (oid(n), Some(loc(n)))).collect();
        crate::mem::set_live_override(Some(std::rc::Rc::new(|| 200 << 20)));
        let err = add_externals(&mut ext, more.clone(), None);
        crate::mem::set_live_override(Some(std::rc::Rc::new(|| 16 << 20)));
        assert!(matches!(err, Err(Error::Limit(_))));
        crate::try_grow::fail_above(Some(1 << 10));
        let err = add_externals(&mut ext, more, None);
        assert!(matches!(&err, Err(e) if crate::try_grow::is_retry(e)));
        assert_eq!(ext.len(), 500);
    }

    /// Codex/Claude r11: collect_bases' "grow:bases" preflight must be able
    /// to refuse — a heap already over MEM_HARD answers the in-band Limit
    /// before the id list reserves. Replacing the `?` with `.ok()` lets
    /// this return Ok and fails here.
    #[test]
    fn collect_bases_refuses_over_the_memory_hard_line() {
        let _guard = crate::try_grow::HookGuard;
        let entries = ref_delta_entries();
        crate::mem::set_live_override(Some(std::rc::Rc::new(|| 200 << 20)));
        let res = collect_bases(&entries, None);
        assert!(
            matches!(&res, Err(Error::Limit(m)) if m.contains("smaller slices")),
            "a refused grow:bases preflight is the memory Limit: {res:?}"
        );
    }

    /// The reservation under the preflight: fail_only_at pins
    /// collect_bases' try_grow call and a zero threshold refuses its
    /// block — the answer is the in-band retry Limit. Reverting to
    /// Vec::with_capacity leaves the site ungated and this returns Ok.
    #[test]
    fn collect_bases_refuses_when_its_reservation_fails() {
        let _guard = crate::try_grow::HookGuard;
        let entries = ref_delta_entries();
        let site = guarded_site("crate::try_grow::with_capacity(n_bases)");
        crate::try_grow::fail_only_at(Some(site));
        crate::try_grow::fail_above(Some(0));
        crate::try_grow::log_asks();
        let res = collect_bases(&entries, None);
        let seen = crate::try_grow::take_asks();
        assert!(matches!(&res, Err(e) if crate::try_grow::is_retry(e)), "a refused reservation is the retry Limit: {res:?}");
        let refused: Vec<_> = seen.iter().filter(|a| a.refused).collect();
        assert_eq!(refused.len(), 1, "one refusal ends the run: {seen:?}");
        assert_eq!((refused[0].file, refused[0].line), site, "the refusal must be at the bases site");
    }
    use gix_object::Kind;

    fn row(i: u32) -> ObjRow {
        ObjRow {
            sha: ObjectId::from_hex(b"0123456789abcdef0123456789abcdef01234567").unwrap(),
            idx: i,
            offset: u64::from(i) * 10,
            len: 100 + i,
            kind: Kind::Blob,
            size: 55,
            parents: None,
        }
    }

    /// The borrowed IndexPost must serialize to exactly the body the DO's
    /// IndexDto deserializes — same shape the old json! Value tree produced.
    #[test]
    fn index_post_matches_do_json_shape() {
        let pack = IndexPack { id: "p1", push_id: "pu", count: 3, bytes: 999, commit_lo: 5, commit_hi: 9 };
        let rows = [row(0), row(1)];
        let got = serde_json::to_value(IndexPost { pack, rows: &rows, json_len: 0 }).unwrap();
        let want = serde_json::json!({
            "pack": { "id": "p1", "push_id": "pu", "count": 3, "bytes": 999,
                      "commit_lo": 5, "commit_hi": 9 },
            "rows": rows,
        });
        assert_eq!(got, want);
        // rows serialize through ObjRow's own impl: parents omitted when
        // None (the DO reads it as default)
        assert!(got["rows"][0].get("parents").is_none());
        assert_eq!(got["rows"][0]["kind"], 3); // Blob -> git_kind
        assert!(INDEX_BATCH <= 10_000, "DO push_index caps at 10,000 rows");
    }

    /// The /_do/push/index chunking post_meta performs, pure: 0 rows still
    /// posts once (the 'ingesting' row), batches cap at INDEX_BATCH, and
    /// every post carries the same real meta — never the EMPTY placeholder.
    #[test]
    fn index_chunks_batch_and_meta() {
        let meta = PackMeta {
            pack: PackId("p".into()),
            push_id: Some("pu".into()),
            count: 7,
            bytes: 4096,
            commit_lo: 11,
            commit_hi: 22,
            created_at: 33,
        };
        // 0 rows -> exactly one post, and it is the empty one
        let posts = index_chunks("pid", "push", 0, &meta, &[]).unwrap();
        assert_eq!(posts.len(), 1);
        assert!(posts[0].rows.is_empty());
        // ceil(n / INDEX_BATCH) posts, rows covered exactly once, real meta on each
        for (n, want) in [(1usize, 1usize), (2_500, 1), (2_501, 2), (10_000, 4)] {
            let rows: Vec<ObjRow> = (0..n as u32).map(row).collect();
            let posts = index_chunks("pid", "push", 0, &meta, &rows).unwrap();
            assert_eq!(posts.len(), want, "{n} rows");
            assert_eq!(posts.iter().map(|p| p.rows.len()).sum::<usize>(), n);
            for p in &posts {
                assert_eq!(p.pack.id, "pid");
                assert_eq!(p.pack.push_id, "push");
                assert_eq!(p.pack.count, 7);
                assert_eq!(p.pack.bytes, 4096);
                assert_eq!(p.pack.commit_lo, 11);
                assert_eq!(p.pack.commit_hi, 22);
            }
        }
        // over MAX_LINKS refuses before any post is built
        assert!(index_chunks("pid", "push", MAX_LINKS + 1, &meta, &[]).is_err());
    }

    fn commit_row(i: u32, parents: usize) -> ObjRow {
        let mut r = row(i);
        r.kind = Kind::Commit;
        r.parents = Some(vec![ObjectId::from_hex(b"89abcdef0123456789abcdef0123456789abcdef").unwrap(); parents]);
        r
    }

    /// Codex #4: posts split by serialized BYTES too. 2,500 commit rows
    /// with 100 parents each serialize to ~11 MiB — far over one 4 MiB
    /// body — so the batch splits early; each post's recorded length is
    /// the exact JSON length the edge allocates, and the preflight bound
    /// is the largest post's body plus its request copy.
    #[test]
    fn index_chunks_split_by_serialized_bytes() {
        let meta = PackMeta::EMPTY;
        let rows: Vec<ObjRow> = (0..2_500).map(|i| commit_row(i, 100)).collect();
        let posts = index_chunks("pid", "push", 0, &meta, &rows).unwrap();
        let total: usize = posts.iter().map(|p| p.json_len).sum();
        assert!(total > 3 * INDEX_POST_BYTES, "fixture must exceed several bodies: {total}");
        assert!(posts.len() >= 3, "{} posts", posts.len());
        assert_eq!(posts.iter().map(|p| p.rows.len()).sum::<usize>(), rows.len());
        let mut max = 0;
        for p in &posts {
            let text = serde_json::to_vec(p).unwrap();
            assert_eq!(p.json_len, text.len(), "recorded length is exact");
            assert!(p.json_len <= INDEX_POST_BYTES, "post of {} bytes", p.json_len);
            max = max.max(p.json_len);
        }
        // a post closes only when the NEXT row would not fit
        for w in posts.windows(2) {
            let next = json_len(&w[1].rows[0]).unwrap();
            assert!(w[0].json_len + 1 + next > INDEX_POST_BYTES);
        }
        assert_eq!(index_post_bound("pid", "push", &meta, &rows).unwrap(), 2 * max + crate::store::ALLOC_OVERHEAD);
        // one row over the byte mark still posts — alone
        let giant = [row(0), commit_row(1, 120_000), row(2)];
        let posts = index_chunks("pid", "push", 0, &meta, &giant).unwrap();
        assert_eq!(posts.iter().map(|p| p.rows.len()).collect::<Vec<_>>(), vec![1, 1, 1]);
        assert!(posts[1].json_len > INDEX_POST_BYTES);
        // small rows still batch by count; exact lengths there too
        let small: Vec<ObjRow> = (0..2_501).map(row).collect();
        let posts = index_chunks("pid", "push", 0, &meta, &small).unwrap();
        assert_eq!(posts.len(), 2);
        assert_eq!(posts[0].json_len, serde_json::to_vec(&posts[0]).unwrap().len());
        assert_eq!(posts[1].json_len, serde_json::to_vec(&posts[1]).unwrap().len());
        let empty = index_chunks("pid", "push", 0, &meta, &[]).unwrap();
        assert_eq!(empty[0].json_len, serde_json::to_vec(&empty[0]).unwrap().len());
    }

    /// Slots cap concurrency, release frees them, and the count can't go
    /// negative — the three behaviours the whole gate rests on.
    #[test]
    fn admission_acquire_release_and_capacity() {
        let mut a = Admission::new(2);
        let (t0, t1, t2) = (a.enqueue(0), a.enqueue(0), a.enqueue(0));
        let (p0, p1) = (
            a.try_acquire(t0, false, 0).unwrap(),
            a.try_acquire(t1, false, 0).unwrap(),
        );
        assert_eq!(a.in_flight(), 2);
        assert!(a.try_acquire(t2, false, 0).is_none(), "third ingest must wait at capacity");
        a.release(p0);
        let p2 = a.try_acquire(t2, false, 0).unwrap();
        assert_eq!(a.in_flight(), 2);
        a.release(p1);
        a.release(p2);
        assert_eq!(a.in_flight(), 0);
        a.release(p2);
        a.release(999);
        assert_eq!(a.in_flight(), 0, "release must be idempotent at zero");
    }

    /// slots == 0 disables the gate: every caller acquires immediately, and
    /// in_flight still counts so the peers checks stay honest.
    #[test]
    fn admission_disabled_acquires_but_still_counts() {
        let mut a = Admission::new(0);
        for _ in 0..5 {
            let t = a.enqueue(0);
            assert!(a.try_acquire(t, true, 0).is_some(), "disabled gate ignores heap pressure too");
        }
        assert_eq!(a.in_flight(), 5);
    }

    /// A free slot is not enough while live heap sits above the low-water
    /// mark AND a peer ingest runs — but alone, or once the heap calms, the
    /// slot admits. This is what keeps leftover heap from stacking up.
    #[test]
    fn admission_heap_busy_waits_only_while_a_peer_runs() {
        let mut a = Admission::new(2);
        let t0 = a.enqueue(0);
        // no peers in flight: busy heap alone can't block (it's not ours to drain)
        assert!(a.try_acquire(t0, true, 0).is_some());
        assert_eq!(a.in_flight(), 1);
        // a slot is free but heap is busy and the first ingest still runs: wait
        let t1 = a.enqueue(0);
        assert!(a.try_acquire(t1, true, 0).is_none());
        // heap calm: the free slot admits despite the peer
        assert!(a.try_acquire(t1, false, 0).is_some());
        assert_eq!(a.in_flight(), 2);
        // at capacity the heap reading is irrelevant
        let t2 = a.enqueue(0);
        assert!(a.try_acquire(t2, false, 0).is_none());
        assert!(a.try_acquire(t2, true, 0).is_none());
    }

    /// Only the oldest live ticket may take a freed slot — a younger
    /// waiter can't barge ahead of an earlier push that has waited longer.
    #[test]
    fn admission_serves_oldest_ticket_first() {
        let mut a = Admission::new(1);
        let (t0, t1, t2) = (a.enqueue(0), a.enqueue(0), a.enqueue(0));
        let p0 = a.try_acquire(t0, false, 0).unwrap();
        a.release(p0); // the slot frees while t1 and t2 still queue
        assert!(a.try_acquire(t2, false, 0).is_none(), "the younger ticket must not barge");
        assert!(a.try_acquire(t1, false, 0).is_some(), "the oldest waiter takes the slot");
        assert_eq!(a.in_flight(), 1);
    }

    /// A waiter that times out or is dropped leaves the queue — its ticket
    /// is removed, so the next-oldest waiter becomes head instead of the
    /// line wedging behind a dead ticket.
    #[test]
    fn admission_abandoned_ticket_does_not_wedge_queue() {
        let mut a = Admission::new(1);
        let (t0, t1, t2) = (a.enqueue(0), a.enqueue(0), a.enqueue(0));
        let p0 = a.try_acquire(t0, false, 0).unwrap();
        a.leave(t1); // t1 timed out or its future was dropped
        a.release(p0);
        let p2 = a.try_acquire(t2, false, 0).unwrap();
        assert_eq!(a.in_flight(), 1);
        // leaving an already-acquired or unknown ticket is a harmless no-op
        a.leave(t0);
        a.leave(999);
        a.release(p2);
        let t3 = a.enqueue(0);
        assert!(a.try_acquire(t3, false, 0).is_some());
    }

    /// A permit the runtime leaked (mem::forget — what a cancelled
    /// invocation leaves behind, since wasm-bindgen-futures never drops
    /// the future) still counts until it outlives the request budget plus
    /// margin; the next poll's sweep then reclaims the slot.
    #[test]
    fn admission_sweep_reclaims_expired_permit() {
        let mut a = Admission::new(1);
        let t0 = a.enqueue(0);
        let _p0 = a.try_acquire(t0, false, 0).unwrap();
        // never released — the Admission-level shape of a leaked permit
        // (the admit_wait-level test below forgets the real RAII permit)
        // inside the lease the dead permit still holds the slot
        let t1 = a.enqueue(1_000);
        assert!(a.try_acquire(t1, false, 1_000).is_none());
        // keep the waiter's stamp fresh while the clock runs past the lease
        let mut now = 1_000i64;
        loop {
            now += 500;
            if a.try_acquire(t1, false, now).is_some() {
                break;
            }
            assert!(now < PERMIT_LEASE_MS + 2_000, "lease sweep never fired");
        }
        assert!(now > PERMIT_LEASE_MS, "reclaimed only after the lease expired");
        assert_eq!(a.in_flight(), 1, "the dead permit's slot was reused, not doubled");
    }

    /// A head ticket whose waiter stopped polling (same leak, other side
    /// of the gate) is evicted once it is TICKET_STALE_MS behind — the
    /// line moves on instead of wedging behind a dead ticket.
    #[test]
    fn admission_sweep_evicts_stale_head_ticket() {
        let mut a = Admission::new(1);
        let t0 = a.enqueue(0);
        let p0 = a.try_acquire(t0, false, 0).unwrap();
        // t1 queues behind the held slot, polls once at t=100, then dies
        let t1 = a.enqueue(100);
        assert!(a.try_acquire(t1, false, 100).is_none());
        // the slot frees; a younger waiter polls past t1's stale horizon —
        // its sweep evicts the dead head and it acquires
        a.release(p0);
        let t2 = a.enqueue(TICKET_STALE_MS + 200);
        assert!(a.try_acquire(t2, false, TICKET_STALE_MS + 200).is_some());
        assert_eq!(a.in_flight(), 1);
        assert_eq!(a.waiting_len(), 0, "both tickets left the queue");
    }

    /// A LIVE head evicted by a younger waiter's sweep (the isolate stalled
    /// its timers past TICKET_STALE_MS) must not be stranded: its next poll
    /// re-inserts the ticket at its old FIFO position and it is admitted in
    /// order once a slot frees — ahead of the younger waiter that evicted it.
    #[test]
    fn admission_live_head_reinserts_after_stale_eviction() {
        let mut a = Admission::new(1);
        let holder = a.enqueue(0);
        let p = a.try_acquire(holder, false, 0).unwrap();
        // w1 queues behind the held slot and polls once at t=100
        let w1 = a.enqueue(100);
        assert!(a.try_acquire(w1, false, 100).is_none());
        // the isolate stalls past the stale mark; w2 polls first and its
        // sweep evicts the still-alive head — the slot is still held, so
        // w2 only waits (and becomes head)
        let w2 = a.enqueue(TICKET_STALE_MS + 200);
        assert!(a.try_acquire(w2, false, TICKET_STALE_MS + 200).is_none());
        assert_eq!(a.waiting_len(), 1, "w1's ticket was evicted, w2's remains");
        a.release(p);
        // w1 resumes: its missing ticket goes back in ahead of w2 (BTreeMap
        // key order IS the FIFO), so it takes the freed slot first
        assert!(
            a.try_acquire(w1, false, TICKET_STALE_MS + 300).is_some(),
            "the evicted live head must re-queue and win its FIFO place"
        );
        assert_eq!(a.in_flight(), 1);
        assert!(a.try_acquire(w2, false, TICKET_STALE_MS + 400).is_none(), "w2 still waits its turn");
        assert_eq!(a.waiting_len(), 1, "only w2 remains queued");
    }

    /// GE_EDGE_INGEST_* parsing: defaults when absent/unparseable (env_i64's
    /// rule), <= 0 slots disables, <= 0 wait fails fast instead of waiting.
    #[test]
    fn ingest_gate_defaults_and_clamps() {
        assert_eq!(ingest_gate(None, None), (1, 240_000));
        assert_eq!(ingest_gate(Some(4), Some(1_234)), (4, 1_234));
        assert_eq!(ingest_gate(Some(0), None).0, 0, "slots <= 0 disables the gate");
        assert_eq!(ingest_gate(Some(-3), None).0, 0);
        assert_eq!(ingest_gate(None, Some(-1)).1, 0, "wait <= 0 fails fast");
        assert_eq!(ingest_gate(Some(8), Some(0)).1, 0);
    }

    /// The pass-B reserve knob: default 150 s, <= 0 drops it.
    #[test]
    fn ingest_reserve_defaults_and_clamps() {
        assert_eq!(ingest_reserve(None), 150_000);
        assert_eq!(ingest_reserve(Some(1_000)), 1_000);
        assert_eq!(ingest_reserve(Some(-1)), 0, "<= 0 drops the reserve");
    }

    /// The effective wait is min(configured wait, budget left - reserve):
    /// `None` when the budget can't cover the reserve — admission then
    /// would start pass B already doomed to Error::Budget, so the push
    /// fails busy instead.
    #[test]
    fn admit_wait_ms_caps_by_budget_minus_reserve() {
        // fresh budget: configured wait is the bound
        assert_eq!(admit_wait_ms(240_000, 240_000, 150_000), Some(90_000));
        // a smaller configured wait still wins
        assert_eq!(admit_wait_ms(60_000, 240_000, 150_000), Some(60_000));
        // budget nearly spent: the cap shrinks to what's left over the reserve
        assert_eq!(admit_wait_ms(240_000, 160_000, 150_000), Some(10_000));
        // exactly the reserve left: nothing usable — fail busy up front
        assert_eq!(admit_wait_ms(240_000, 150_000, 150_000), None);
        // less than the reserve: same
        assert_eq!(admit_wait_ms(240_000, 100_000, 150_000), None);
        // wait <= 0 keeps its fail-fast meaning when the reserve IS covered
        assert_eq!(admit_wait_ms(0, 240_000, 150_000), Some(0));
        assert_eq!(admit_wait_ms(-5, 240_000, 150_000), Some(-5));
    }

    /// A fake admit clock: now() reads the cell, sleep(ms) advances it —
    /// the wait loop's deadline math runs deterministically, no real time.
    #[derive(Clone)]
    struct FakeClock(std::rc::Rc<std::cell::Cell<i64>>);
    impl FakeClock {
        fn new(t: i64) -> Self {
            Self(std::rc::Rc::new(std::cell::Cell::new(t)))
        }
        fn now(&self) -> i64 {
            self.0.get()
        }
        fn sleep(&self, ms: u64) -> std::future::Ready<()> {
            self.0.set(self.0.get() + i64::try_from(ms).unwrap_or(i64::MAX));
            std::future::ready(())
        }
    }

    fn clock_fns(
        clock: &FakeClock,
    ) -> (
        impl Fn() -> i64,
        impl Fn(u64) -> std::future::Ready<()>,
    ) {
        let c = clock.clone();
        let n = move || c.now();
        let c = clock.clone();
        let s = move |ms: u64| c.sleep(ms);
        (n, s)
    }

    /// A sleep the test opens by hand: Pending until the shared flag flips —
    /// lets admit_wait futures queue first, then interleave their polls.
    struct GateSleep(std::rc::Rc<std::cell::Cell<bool>>);
    impl std::future::Future for GateSleep {
        type Output = ();
        fn poll(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<()> {
            if self.0.get() {
                std::task::Poll::Ready(())
            } else {
                std::task::Poll::Pending
            }
        }
    }

    /// A waker that does nothing — admit_wait's injected sleep resolves
    /// immediately, so its future never really pends in these tests.
    fn noop_waker() -> std::task::Waker {
        use std::task::{RawWaker, RawWakerVTable, Waker};
        unsafe fn clone(_: *const ()) -> RawWaker {
            RawWaker::new(std::ptr::null(), &VTABLE)
        }
        unsafe fn no_op(_: *const ()) {}
        static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, no_op, no_op, no_op);
        unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
    }

    /// Poll `f` once with the noop waker.
    fn poll_once<F: std::future::Future>(f: std::pin::Pin<&mut F>) -> std::task::Poll<F::Output> {
        let w = noop_waker();
        let mut cx = std::task::Context::from_waker(&w);
        f.poll(&mut cx)
    }

    /// Drive `f` to completion — the injected clock/sleep make admit_wait
    /// resolve without real time, so a bounded poll loop is enough.
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

    /// A contended waiter that outlives wait_ms answers the in-band busy
    /// Limit, holds no slot, and leaves no ticket behind — the gate still
    /// admits afterwards.
    #[test]
    fn admit_wait_timeout_is_busy_and_leaves_no_ticket() {
        ADMISSION.with(|a| a.borrow_mut().set_slots(1));
        let clock = FakeClock::new(1_000);
        let (n, s) = clock_fns(&clock);
        let hold = block_on(admit_wait(60_000, None, || false, n, s)).unwrap();
        assert_eq!(ingest_in_flight(), 1);
        // the only slot is held; wait_ms=250 -> three 100 ms polls, then busy
        let (n, s) = clock_fns(&clock);
        let err = block_on(admit_wait(250, None, || false, n, s)).err().unwrap();
        assert!(matches!(err, Error::Limit(_)));
        assert!(err.message().contains(INGEST_BUSY_MSG), "{err}");
        assert_eq!(ingest_in_flight(), 1, "the timed-out waiter holds no slot");
        ADMISSION.with(|a| assert_eq!(a.borrow().waiting_len(), 0, "timeout must drop the ticket"));
        drop(hold);
        assert_eq!(ingest_in_flight(), 0, "dropping the permit releases the slot");
        let (n, s) = clock_fns(&clock);
        let _next = block_on(admit_wait(60_000, None, || false, n, s)).unwrap();
        assert_eq!(ingest_in_flight(), 1, "the gate still admits after the timeout");
    }

    /// A waiter dropped mid-wait (the client went away) must leave the
    /// queue — its ticket is RAII, so the next-oldest waiter is not wedged
    /// behind it.
    #[test]
    fn admit_wait_cancel_mid_wait_releases_ticket() {
        ADMISSION.with(|a| a.borrow_mut().set_slots(1));
        let clock = FakeClock::new(0);
        // occupy the only slot so the waiter below must queue
        let (n, s) = clock_fns(&clock);
        let hold = block_on(admit_wait(60_000, None, || false, n, s)).unwrap();
        assert_eq!(ingest_in_flight(), 1);
        let (n, _s) = clock_fns(&clock);
        // Box::pin (not pin!): drop(fut) must drop the future itself so its
        // RAII ticket runs — a pin! shadow would only drop the Pin wrapper
        let mut fut = Box::pin(admit_wait(
            60_000,
            None,
            || false,
            n,
            |_ms: u64| std::future::pending::<()>(),
        ));
        // first poll: acquire fails (slot held), deadline not hit, sleep pends
        assert!(poll_once(fut.as_mut()).is_pending());
        ADMISSION.with(|a| assert_eq!(a.borrow().waiting_len(), 1, "the waiter queued"));
        drop(fut); // client went away — the RAII ticket must leave the queue
        ADMISSION.with(|a| assert_eq!(a.borrow().waiting_len(), 0, "abandoned ticket skipped"));
        assert_eq!(ingest_in_flight(), 1, "the holder is untouched");
        drop(hold);
        assert_eq!(ingest_in_flight(), 0);
    }

    /// End to end: with one slot held, two admit_wait futures queue in
    /// ticket order — the younger one keeps pending even after the slot
    /// frees, and only the head (oldest ticket) acquires.
    #[test]
    fn admit_wait_is_fifo_end_to_end() {
        ADMISSION.with(|a| a.borrow_mut().set_slots(1));
        let clock = FakeClock::new(0);
        let (n, s) = clock_fns(&clock);
        let elder = block_on(admit_wait(60_000, None, || false, n, s)).unwrap();
        assert_eq!(ingest_in_flight(), 1);

        // each waiter's sleep pends on a shared gate the test opens by hand,
        // so both futures queue first and their polls interleave below
        let gate = std::rc::Rc::new(std::cell::Cell::new(false));
        let sleep = {
            let g = gate.clone();
            move |_ms: u64| GateSleep(g.clone())
        };
        let mut w1 = Box::pin(admit_wait(60_000, None, || false, || 0i64, sleep.clone()));
        let mut w2 = Box::pin(admit_wait(60_000, None, || false, || 0i64, sleep));
        assert!(poll_once(w1.as_mut()).is_pending());
        assert!(poll_once(w2.as_mut()).is_pending());
        ADMISSION.with(|a| assert_eq!(a.borrow().waiting_len(), 2));

        drop(elder); // frees the slot — but only the head ticket may take it
        // w2 polls while w1 still sleeps: must NOT acquire (not head)
        assert!(poll_once(w2.as_mut()).is_pending(), "younger waiter can't barge");
        // open the gates: w1 is head with capacity free -> it acquires
        gate.set(true);
        let p1 = match poll_once(w1.as_mut()) {
            std::task::Poll::Ready(Ok(p)) => p,
            std::task::Poll::Ready(Err(e)) => panic!("head waiter errored: {e}"),
            std::task::Poll::Pending => panic!("head waiter should have acquired"),
        };
        assert_eq!(ingest_in_flight(), 1, "the elder waiter's permit is held");
        drop(p1);
        drop(w2); // still queued — its ticket leaves with it
        ADMISSION.with(|a| assert_eq!(a.borrow().waiting_len(), 0));
        assert_eq!(ingest_in_flight(), 0);
    }

    /// The reserve check must never reject an uncontended push: when the
    /// request budget can't cover the pass-B reserve, admit_pass_b's wait
    /// bound flattens to 0 — and admit_wait still makes its one first
    /// try_acquire, so a free slot admits. Only a push that must QUEUE
    /// fails busy on the same thin budget.
    #[test]
    fn uncontended_push_admitted_despite_thin_budget() {
        // the mapping admit_pass_b applies: None -> 0 (one attempt only)
        assert_eq!(admit_wait_ms(240_000, 100_000, 150_000), None);
        assert_eq!(admit_wait_bound(240_000, 100_000, 150_000), 0);
        assert_eq!(admit_wait_bound(240_000, 240_000, 150_000), 90_000);
        ADMISSION.with(|a| a.borrow_mut().set_slots(1));
        let clock = FakeClock::new(0);
        let (n, s) = clock_fns(&clock);
        // wait_ms = 0 is what the thin budget produces: the first
        // try_acquire admits — ms_left never entered the decision
        let p = block_on(admit_wait(0, None, || false, n, s)).unwrap();
        assert_eq!(ingest_in_flight(), 1);
        drop(p);
        assert_eq!(ingest_in_flight(), 0);
        // ...but a CONTENDED push on the same zero wait still fails busy —
        // it would start pass B already doomed to Error::Budget
        let (n, s) = clock_fns(&clock);
        let hold = block_on(admit_wait(60_000, None, || false, n, s)).unwrap();
        let (n, s) = clock_fns(&clock);
        let err = block_on(admit_wait(0, None, || false, n, s)).err().unwrap();
        assert!(matches!(err, Error::Limit(_)));
        assert!(err.message().contains(INGEST_BUSY_MSG), "{err}");
        drop(hold);
        assert_eq!(ingest_in_flight(), 0);
    }

    /// A leaked permit — std::mem::forget, what a cancelled invocation
    /// leaves because wasm-bindgen-futures never drops the future — must
    /// not wedge the gate: once it outlives the request budget plus
    /// margin, the next waiter's poll sweep reclaims the slot.
    #[test]
    fn admit_wait_reclaims_forgotten_permit_after_lease() {
        ADMISSION.with(|a| a.borrow_mut().set_slots(1));
        let clock = FakeClock::new(0);
        let (n, s) = clock_fns(&clock);
        let leaked = block_on(admit_wait(60_000, None, || false, n, s)).unwrap();
        assert_eq!(ingest_in_flight(), 1);
        std::mem::forget(leaked); // the Drop that would release never runs
        assert_eq!(ingest_in_flight(), 1, "the leak still counts until the lease expires");
        // the second push queues; each injected sleep advances the fake
        // clock 100 ms, so its polls run past PERMIT_LEASE_MS and the
        // sweep reclaims the dead permit — the wait bound covers it
        let (n, s) = clock_fns(&clock);
        let p = block_on(admit_wait(PERMIT_LEASE_MS + 60_000, None, || false, n, s)).unwrap();
        assert_eq!(ingest_in_flight(), 1, "the reclaimed slot was reused, not doubled");
        drop(p);
        assert_eq!(ingest_in_flight(), 0, "gate fully recovered");
    }

    /// A leaked waiter — the future forgotten mid-wait, same cancellation
    /// path — leaves a dead head ticket that would wedge the FIFO forever.
    /// Its poll stamp goes stale and the next waiter's sweep evicts it.
    #[test]
    fn admit_wait_evicts_forgotten_ticket_after_stale() {
        ADMISSION.with(|a| a.borrow_mut().set_slots(1));
        let clock = FakeClock::new(0);
        let (n, s) = clock_fns(&clock);
        let hold = block_on(admit_wait(60_000, None, || false, n, s)).unwrap();
        assert_eq!(ingest_in_flight(), 1);
        // a waiter queues behind the held slot, polls once, then is forgotten
        let gate = std::rc::Rc::new(std::cell::Cell::new(false));
        let dead_sleep = {
            let g = gate.clone();
            move |_ms: u64| GateSleep(g.clone())
        };
        let (n, _s) = clock_fns(&clock);
        let mut dead = Box::pin(admit_wait(60_000, None, || false, n, dead_sleep));
        assert!(poll_once(dead.as_mut()).is_pending());
        ADMISSION.with(|a| assert_eq!(a.borrow().waiting_len(), 1, "the waiter queued"));
        std::mem::forget(dead); // cancelled invocation: the ticket is never dropped
        // jump past the stale horizon, then a new waiter polls: its sweep
        // evicts the dead head before the head check
        clock.0.set(TICKET_STALE_MS + 1);
        let next_sleep = {
            let g = gate.clone();
            move |_ms: u64| GateSleep(g.clone())
        };
        let (n, _s) = clock_fns(&clock);
        let mut next = Box::pin(admit_wait(60_000, None, || false, n, next_sleep));
        assert!(poll_once(next.as_mut()).is_pending());
        ADMISSION.with(|a| assert_eq!(a.borrow().waiting_len(), 1, "stale ticket evicted, new one queued"));
        // free the slot and open the sleep gate: the new waiter acquires —
        // proof the dead ticket no longer blocks the line
        drop(hold);
        gate.set(true);
        let p = match poll_once(next.as_mut()) {
            std::task::Poll::Ready(Ok(p)) => p,
            std::task::Poll::Ready(Err(e)) => panic!("waiter errored: {e}"),
            std::task::Poll::Pending => panic!("waiter should have acquired"),
        };
        assert_eq!(ingest_in_flight(), 1);
        drop(p);
        assert_eq!(ingest_in_flight(), 0);
    }

    /// A receive-pack guard leaked by a cancelled invocation (mem::forget —
    /// the same path that leaks permits) must not pin the isolate's peer
    /// count forever: entries expire after PERMIT_LEASE_MS, so a lone
    /// too-big push still gets the "smaller slices" answer, not "busy".
    #[test]
    fn receive_packs_lease_expires_forgotten_guard() {
        let g = ReceivePackGuard::at(1_000);
        assert_eq!(receive_packs_at(1_000), 1);
        std::mem::forget(g); // Drop never runs — the entry stays
        assert_eq!(receive_packs_at(1_000 + PERMIT_LEASE_MS), 1, "inside the lease it still counts");
        assert_eq!(
            receive_packs_at(1_000 + PERMIT_LEASE_MS + 1),
            0,
            "past the lease the leaked entry is swept"
        );
        // a live guard inside its lease is untouched by the sweep
        let live = ReceivePackGuard::at(2_000);
        assert_eq!(receive_packs_at(2_000 + PERMIT_LEASE_MS), 1, "age == lease still counts");
        drop(live);
        assert_eq!(receive_packs_at(2_000 + PERMIT_LEASE_MS), 0);
    }
    /// Codex's mutant: a waiter must NOT acquire after its wait bound —
    /// the deadline is checked BEFORE every attempt after the first, so
    /// an injected clock at 240,001 ms on a 100 ms bound can never admit.
    #[test]
    fn admit_wait_never_acquires_past_deadline() {
        ADMISSION.with(|a| a.borrow_mut().set_slots(1));
        let clock = FakeClock::new(1_000);
        let (n, s) = clock_fns(&clock);
        // hold the only slot so the waiter queues
        let hold = block_on(admit_wait(60_000, None, || false, n, s)).unwrap();
        // waiter bound 100 ms: polls at +0,+100 -> at +100 deadline hits
        // BEFORE the third try_acquire — never acquires
        let clock2 = FakeClock::new(0);
        let (n2, s2) = clock_fns(&clock2);
        let err = block_on(admit_wait(100, None, || false, n2, s2)).err().unwrap();
        assert!(matches!(err, Error::Limit(_)), "{err:?}");
        drop(hold);
    }

    /// Claude r8 #2: a fence entry expires EVICTED_FENCE_MS after its
    /// eviction. A leaked holder that never drains and keeps the heap busy
    /// blocks admission inside that window and no longer after it; the
    /// memory guard takes over from there.
    #[test]
    fn eviction_fence_expires_for_a_holder_that_never_drains() {
        let mut a = Admission::new(1);
        let t0 = a.enqueue(0);
        let _leaked = a.try_acquire(t0, false, 0).unwrap(); // never released
        let w = a.enqueue(0);
        let evicted_at = PERMIT_LEASE_MS + 1;
        assert!(a.try_acquire(w, true, evicted_at).is_none(), "the sweep evicts and fences");
        assert_eq!(a.in_flight(), 0);
        // busy polls up to the fence's edge stay blocked
        let mut now = evicted_at;
        while now + INGEST_POLL_MS as i64 <= evicted_at + EVICTED_FENCE_MS {
            now += INGEST_POLL_MS as i64;
            assert!(a.try_acquire(w, true, now).is_none(), "fence lifted early at {now}");
        }
        assert!(a.try_acquire(w, true, evicted_at + EVICTED_FENCE_MS).is_none(), "age == limit still fences");
        // one millisecond past the limit: the fence lets go, heap still busy
        assert!(a.try_acquire(w, true, evicted_at + EVICTED_FENCE_MS + 1).is_some());
        assert!(a.evicted.is_empty());
        assert_eq!(EVICTED_FENCE_MS, TICKET_STALE_MS);
    }

    /// The post-eviction fence persists: an evicted-but-undrained holder
    /// blocks admission on EVERY heap-busy poll, not only on the poll whose
    /// sweep evicted it — and a non-head waiter's sweep cannot consume it.
    #[test]
    fn eviction_fence_holds_across_busy_polls() {
        let mut a = Admission::new(1);
        let t0 = a.enqueue(0);
        let p0 = a.try_acquire(t0, false, 0).unwrap(); // leaked: never released
        let (w1, w2) = (a.enqueue(0), a.enqueue(0));
        let mut now = PERMIT_LEASE_MS + 1;
        // the NON-head waiter polls first: its sweep evicts p0
        assert!(a.try_acquire(w2, true, now).is_none());
        assert_eq!(a.in_flight(), 0, "the leaked permit was evicted");
        // the head polls again and again with the heap busy: never admitted
        for _ in 0..20 {
            now += INGEST_POLL_MS as i64;
            assert!(a.try_acquire(w1, true, now).is_none(), "fence lifted at {now}");
            assert!(a.try_acquire(w2, true, now).is_none());
        }
        // the evicted holder's late drop is the drain signal: the fence
        // lifts even though the heap still reads busy (no peer remains)
        a.release(p0);
        assert!(a.try_acquire(w1, true, now + 1).is_some());
    }

    /// The fence also lifts on the first calm-heap poll — a truly leaked
    /// holder never releases, and its pages are reused once the heap calms.
    #[test]
    fn eviction_fence_clears_on_calm_heap() {
        let mut a = Admission::new(1);
        let t0 = a.enqueue(0);
        let _leaked = a.try_acquire(t0, false, 0).unwrap();
        let w = a.enqueue(0);
        let now = PERMIT_LEASE_MS + 1;
        assert!(a.try_acquire(w, true, now).is_none());
        assert!(a.try_acquire(w, true, now + 100).is_none(), "second busy poll still fenced");
        assert!(a.try_acquire(w, false, now + 200).is_some(), "calm heap lifts the fence");
        // and it stays lifted: the next holder's release frees normally
        assert!(a.evicted.is_empty());
    }

    /// A permit stamp a live holder no longer refreshes ages out; the
    /// helper drives a request-shaped lease through ReqBudget::charge.
    fn leased_budget(lease: &Rc<Lease>) -> ReqBudget {
        let mut b = ReqBudget::paid().leased(Rc::clone(lease));
        b.max_ms = f64::MAX; // the test clock runs far past one request's wall
        b.max_subrequests = u32::MAX;
        b
    }

    /// Admit through admit_wait on the shared test clock (the clock
    /// `Lease::renew` reads), sleeping by advancing it.
    fn admit_on_test_clock(wait_ms: i64, lease: &Rc<Lease>, busy: bool) -> Result<IngestPermit, Error> {
        block_on(admit_wait(
            wait_ms,
            Some(lease),
            move || busy,
            crate::platform::now_ms,
            |ms: u64| {
                crate::test_clock::advance(ms as f64);
                std::future::ready(())
            },
        ))
    }

    /// Codex #1 / Claude #1 and #7: lease identity is per request. A leaked
    /// holder ages out even while a bystander request charges the whole
    /// time; once a new holder is admitted, the evicted holder's next
    /// renew fails with Limit while the new holder (and the bystander)
    /// renew fine — the evicted holder never refreshes the new permit.
    #[test]
    fn leases_are_per_request_with_interleaved_holders_and_bystander() {
        crate::test_clock::set(0.0);
        ADMISSION.with(|a| *a.borrow_mut() = Admission::new(1));
        let guard_a = ReceivePackGuard::at(0);
        let lease_a = guard_a.lease();
        let mut budget_a = leased_budget(&lease_a);
        let permit_a = admit_on_test_clock(60_000, &lease_a, false).unwrap();
        assert_eq!(lease_a.permit.get(), Some(permit_a.id));
        budget_a.charge(1).unwrap();
        // A's future leaks mid pass B: its permit Drop never runs
        std::mem::forget(permit_a);
        // a bystander request (another receive-pack, here in pass A) keeps
        // charging its own budget every second across A's whole lease
        let guard_c = ReceivePackGuard::at(0);
        let lease_c = guard_c.lease();
        let mut budget_c = leased_budget(&lease_c);
        let mut plain = ReqBudget::paid(); // a fetch: no lease at all
        plain.max_ms = f64::MAX;
        for _ in 0..(PERMIT_LEASE_MS / 1_000 + 2) {
            crate::test_clock::advance(1_000.0);
            budget_c.charge(1).expect("a bystander never fails on another request's lease");
            plain.charge(1).expect("a lease-less budget never checks a lease");
        }
        // B arrives: its sweep evicts A's permit — bystander charges did
        // not keep it alive
        let guard_b = ReceivePackGuard::at(crate::platform::now_ms());
        let lease_b = guard_b.lease();
        let mut budget_b = leased_budget(&lease_b);
        let permit_b = admit_on_test_clock(1_000, &lease_b, false).expect("A's leaked permit aged out");
        assert_eq!(ingest_in_flight(), 1);
        let b_stamp = || ADMISSION.with(|a| a.borrow().permits.get(&permit_b.id).copied());
        let stamp0 = b_stamp().unwrap();
        // interleave: A (actually still alive, resumed late) and B charge
        crate::test_clock::advance(500.0);
        let err = budget_a.charge(1).expect_err("the evicted holder must learn on its next renew");
        assert!(matches!(err, Error::Limit(_)));
        assert!(err.message().contains(INGEST_BUSY_MSG), "{err}");
        assert_eq!(b_stamp(), Some(stamp0), "A's renew must not refresh B's permit");
        budget_b.charge(1).expect("the new holder renews fine");
        assert_eq!(b_stamp(), Some(stamp0 + 500), "B's own charge renews B");
        budget_c.charge(1).expect("bystander still fine");
        assert_eq!(b_stamp(), Some(stamp0 + 500), "a bystander never renews B");
        // A keeps failing on every later renew — it never adopts B's permit
        crate::test_clock::advance(500.0);
        assert!(budget_a.charge(1).is_err());
        assert!(budget_b.charge(1).is_ok());
        // B's drop clears only B's handle; A's stale id stays A's problem
        drop(permit_b);
        assert_eq!(lease_b.permit.get(), None);
        assert!(budget_b.charge(1).is_ok(), "past the gated span B renews no permit");
        drop((guard_a, guard_b, guard_c));
    }

    /// Claude #7: RECEIVE_PACKS renewals are per request too. Request A's
    /// entry survives B entering and leaving (B can't steal the renew
    /// slot), and a leaked guard's entry is never re-inserted by anyone
    /// else's charges — it ages out.
    #[test]
    fn receive_pack_leases_are_per_request() {
        crate::test_clock::set(0.0);
        RECEIVE_PACKS.with(|c| *c.borrow_mut() = ReceivePacks::new());
        let guard_a = ReceivePackGuard::at(0);
        let mut budget_a = leased_budget(&guard_a.lease());
        let leaked = ReceivePackGuard::at(0);
        let leaked_lease = leaked.lease();
        std::mem::forget(leaked); // cancelled invocation: no Drop
        drop(leaked_lease); // the leaked future's handle is unreachable
        {
            let guard_b = ReceivePackGuard::at(0);
            let mut budget_b = leased_budget(&guard_b.lease());
            budget_b.charge(1).unwrap();
        }
        for _ in 0..(PERMIT_LEASE_MS / 10_000 + 2) {
            crate::test_clock::advance(10_000.0);
            budget_a.charge(1).unwrap();
        }
        let now = crate::platform::now_ms();
        assert!(now > PERMIT_LEASE_MS);
        assert_eq!(receive_packs_at(now), 1, "A's own charges keep A alive; the leak aged out");
        drop(guard_a);
        assert_eq!(receive_packs_at(now), 0);
    }

    /// Lease renewal: a live holder's permit stamp refreshes on renew so a
    /// working pass B never ages to the sweep; an evicted holder's renew
    /// returns false -> the caller stops in-band.
    #[test]
    fn permit_renewal_keeps_lease_and_detects_eviction() {
        ADMISSION.with(|a| a.borrow_mut().set_slots(1));
        // hold a permit at t=0
        let permit = ADMISSION.with(|a| {
            let t = a.borrow_mut().enqueue(0);
            a.borrow_mut().try_acquire(t, false, 0).unwrap()
        });
        // renew at t=200_000 (past where a stale stamp would be if not renewed)
        let ok = ADMISSION.with(|a| a.borrow_mut().renew(permit, 200_000));
        assert!(ok, "live permit renews");
        // sweep at t=200_000+PERMIT_LEASE_MS-1: permit (renewed at 200k) is
        // only ~270s-1ms old -> NOT stale -> still held
        let still = ADMISSION.with(|a| a.borrow().permits.contains_key(&permit));
        assert!(still);
        // a permit id that was never issued / already swept: renew -> false
        let bogus = ADMISSION.with(|a| a.borrow_mut().renew(999_999, 300_000));
        assert!(!bogus, "evicted/absent permit detects eviction");
    }

    /// RECEIVE_PACKS entries renew the same way: a live request's peer
    /// count restores after a sweep instead of wedging at 0.
    #[test]
    fn receive_pack_renew_restores_swept_entry() {
        let id = RECEIVE_PACKS.with(|c| c.borrow_mut().enter(0));
        // sweep it away: it's older than the lease
        let _ = receive_packs_at(PERMIT_LEASE_MS + 1);
        // a live request renews -> entry back
        RECEIVE_PACKS.with(|c| c.borrow_mut().renew(id, PERMIT_LEASE_MS + 2));
        assert_eq!(receive_packs_at(PERMIT_LEASE_MS + 2), 1);
        RECEIVE_PACKS.with(|c| c.borrow_mut().leave(id));
    }

    /// Size-aware reserve: ~26k objects reserves the ~150 s cap; ~5k
    /// reserves ~30-45 s; the floor and cap clamp both ends.
    #[test]
    fn pass_b_reserve_scales_with_pack_size() {
        // 26k objects -> by_objects = 26_000 * 6 = 156_000 -> capped at 150_000
        assert_eq!(pass_b_reserve(26_000, 0, 150_000), 150_000);
        // 5k objects -> 30_000 ms (above the 15 s floor, under the cap)
        assert_eq!(pass_b_reserve(5_000, 0, 150_000), 30_000);
        // floor: a 1-object push still reserves 15 s
        assert_eq!(pass_b_reserve(1, 0, 150_000), 15_000);
        // bytes term: ~1 GiB compressed -> 150_000 * 1 = capped
        assert_eq!(pass_b_reserve(10, 1 << 30, 150_000), 150_000);
        // cap override wins: env cap of 60 s clamps the 26k estimate
        assert_eq!(pass_b_reserve(26_000, 0, 60_000), 60_000);
        // cap of 0 drops the reserve entirely (env <= 0 rule via ingest_reserve)
        assert_eq!(pass_b_reserve(26_000, 0, 0), 0);
    }

    /// The lease margin is pinned: PERMIT_LEASE_MS = ReqBudget::MAX_MS + 30 s.
    #[test]
    fn permit_lease_margin_is_pinned() {
        assert_eq!(PERMIT_LEASE_MS, ReqBudget::MAX_MS as i64 + 30_000);
        assert_eq!(PERMIT_LEASE_MS, 270_000);
    }

}
