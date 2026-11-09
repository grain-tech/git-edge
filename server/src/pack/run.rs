//! Phase one of the two-phase push (CONTRACTS.md 2.4-2.5, 3 ordering), in the edge Worker.
//! Ported from research/proofs/two-phase-push.md.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeSet, HashMap};

use gix_hash::ObjectId;
use gix_pack::data::entry::Header;
use worker::{Delay, Env, Stub};

use crate::edge::BodyReader;
use crate::error::Error;
use crate::store::{keys, Bucket, ObjLoc, ObjRow, PackId, PackMeta, PackWriter, PushId};
use crate::wire::http::{stub_json, RepoRoute};
use crate::ReqBudget;

pub(crate) const MAX_LINKS: usize = 1_000_000; // A5
/// Rows per /_do/push/index POST. The DO accepts up to 10,000, but a smaller
/// batch keeps each serialized body (and its JS-heap copy) small inside the
/// isolate's memory budget — the edge posts these mid-push, where a 10k-row
/// body was the last big allocation before the memory guard tripped.
const INDEX_BATCH: usize = 2_500;

/// The /_do/push/index body, borrowed — serializes rows straight from the
/// slice instead of building a serde_json::Value tree per row.
#[derive(serde::Serialize)]
struct IndexPost<'a> {
    pack: IndexPack<'a>,
    rows: &'a [ObjRow],
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
    let mut bases: Vec<ObjectId> = entries
        .iter()
        .filter_map(|r| match r.kind_or_delta {
            Header::RefDelta { base_id } => Some(base_id),
            _ => None,
        })
        .collect();
    bases.sort_unstable();
    bases.dedup();
    let mut external: HashMap<ObjectId, ObjLoc> = HashMap::new();
    for chunk in bases.chunks(1_000) {
        external.extend(
            lookup(stub, repo, chunk, None, budget)
                .await?
                .into_iter()
                .filter_map(|(id, l)| Some((id, l?))),
        );
    }
    let pack = PackId::random()?;
    let mut sink = IndexSink { stub, repo, pack: pack.clone(), push: push.clone(), links: Vec::new(), tags: HashMap::new() };
    // Pass B is the isolate's memory-heavy phase: admit at most
    // GE_EDGE_INGEST_SLOTS concurrent ingests — BEFORE the 'ingesting' row
    // and the pack MPU exist, so a busy or cancelled wait leaves nothing
    // behind. Pass A above already streamed the body to R2, so waiting
    // here never holds the client's upload. The permit stays held through
    // out.finish + the final post_meta: the gated span ends there.
    let permit = admit_pass_b(env, budget).await?;
    sink.post_meta(&PackMeta::EMPTY, &[], budget).await?; // packs row 'ingesting' before any part is uploaded
    let key = keys::pack(&bucket.repo, &pack);
    let mut out = match PackWriter::create(bucket, &key, u32::try_from(entries.len()).unwrap_or(u32::MAX), kind, budget).await {
        Ok(w) => w,
        Err(e) => return Err(e),
    };
    let pending = keys::pending(&bucket.repo, push);
    let tail = match super::ingest::resolve_and_normalize(
        bucket,
        &pending,
        &entries,
        super::ingest::Externals::Map(&external),
        &mut out,
        &mut sink,
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
    let meta = out.finish(budget).await?; // section 3 (1): pack durable in R2
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

pub struct IndexSink<'a> {
    stub: &'a Stub,
    repo: &'a RepoRoute,
    pack: PackId,
    push: PushId,
    pub links: Vec<ObjectId>,
    pub tags: HashMap<ObjectId, ObjectId>,
}
impl IndexSink<'_> {
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
            self.post_rows(&post.pack, post.rows, budget).await?;
        }
        Ok(())
    }
    async fn post_rows(
        &self,
        pack: &IndexPack<'_>,
        rows: &[ObjRow],
        budget: &mut ReqBudget,
    ) -> Result<(), Error> {
        let body = IndexPost { pack: *pack, rows };
        let _: serde_json::Value =
            stub_json(self.stub, self.repo, "/_do/push/index", &body, budget).await?;
        Ok(())
    }
}

/// The /_do/push/index posts one post_meta call sends, in order — pure (no
/// stub) so tests count the chunks and check the meta on each. An empty
/// `rows` still yields exactly one post: it plants the 'ingesting' packs
/// row the whole two-phase ordering depends on. Every chunk carries the
/// same real meta — the DO upserts it idempotently.
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
    if rows.is_empty() {
        return Ok(vec![IndexPost { pack, rows: &[] }]);
    }
    Ok(rows.chunks(INDEX_BATCH).map(|c| IndexPost { pack, rows: c }).collect())
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
/// The in-band busy message, shared by the admit timeout and a peer-contended
/// MEM_HARD trip: a retry helps, smaller slices would not have.
pub(crate) const INGEST_BUSY_MSG: &str =
    "server busy: too many large pushes in progress; retry in a minute";

/// The gate's pure state — unit-tested without a worker::Env or the isolate.
/// `slots == 0` disables admission: every caller acquires, and `in_flight`
/// still counts so the low-water wait and the honest Limit keep working.
/// Waiters queue on FIFO tickets (`next_ticket` issues them, `waiting`
/// holds the live ones in order): only the lowest live ticket may take a
/// freed slot, so later arrivals can't starve an earlier push. A ticket
/// leaves `waiting` when its waiter acquires, times out, or is dropped —
/// the head can never point at a dead waiter.
struct Admission {
    in_flight: usize,
    slots: usize,
    next_ticket: u64,
    waiting: BTreeSet<u64>,
}

impl Admission {
    fn new(slots: usize) -> Self {
        Self { in_flight: 0, slots, next_ticket: 0, waiting: BTreeSet::new() }
    }
    /// Issue a FIFO ticket and queue the waiter behind every earlier one.
    fn enqueue(&mut self) -> u64 {
        let t = self.next_ticket;
        self.next_ticket = self.next_ticket.wrapping_add(1);
        self.waiting.insert(t);
        t
    }
    /// A waiter gave up (timeout or cancel): its ticket leaves the queue so
    /// the next-oldest waiter becomes head — an abandoned ticket must never
    /// wedge the line. Removing an already-gone ticket is a no-op.
    fn leave(&mut self, ticket: u64) {
        self.waiting.remove(&ticket);
    }
    /// Admit the ticketed waiter iff it is the queue head AND a slot is free
    /// AND the heap is calm. `heap_busy` reports live_bytes() over
    /// INGEST_LOW_WATER; it blocks only while a peer ingest is in flight
    /// (`in_flight` counts admitted ingests — the caller isn't one yet), so
    /// stale heap from finished work can't stall the queue and unrelated
    /// heap can't either. slots == 0 disables the gate: every ticket
    /// acquires — there is no contention to order.
    fn try_acquire(&mut self, ticket: u64, heap_busy: bool) -> bool {
        if self.slots != 0 && self.waiting.first() != Some(&ticket) {
            return false;
        }
        let admit =
            self.slots == 0 || (self.in_flight < self.slots && !(heap_busy && self.in_flight > 0));
        if admit {
            self.waiting.remove(&ticket);
            self.in_flight += 1;
        }
        admit
    }
    fn release(&mut self) {
        self.in_flight = self.in_flight.saturating_sub(1);
    }
    /// Re-point the gate at this request's slot count; env is fixed per
    /// deployment, and `in_flight` must survive the rewrite.
    fn set_slots(&mut self, slots: usize) {
        self.slots = slots;
    }
    fn in_flight(&self) -> usize {
        self.in_flight
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
    /// pass B can.
    static RECEIVE_PACKS: Cell<usize> = const { Cell::new(0) };
}

/// Counts one in-progress receive-pack request for the isolate — held for
/// the whole edge handler so pass-A heap pressure counts toward the memory
/// guard's busy-vs-slices message. try_with: a drop during thread-local
/// teardown must not panic.
pub(crate) struct ReceivePackGuard;
impl ReceivePackGuard {
    pub(crate) fn new() -> Self {
        RECEIVE_PACKS.with(|c| c.set(c.get() + 1));
        Self
    }
}
impl Drop for ReceivePackGuard {
    fn drop(&mut self) {
        let _ = RECEIVE_PACKS.try_with(|c| c.set(c.get().saturating_sub(1)));
    }
}

/// In-progress receive-pack requests in this isolate, the caller's own
/// included — mem_guard's "is the isolate busy" signal.
pub(crate) fn receive_packs() -> usize {
    RECEIVE_PACKS.with(|c| c.get())
}

/// Pass-B ingests inside the gated span, this request's permit included —
/// the admit log reads it for diagnostics.
pub(crate) fn ingest_in_flight() -> usize {
    ADMISSION.with(|a| a.borrow().in_flight())
}

/// RAII slot release — every return/error path out of the gated span frees
/// it. try_with: a permit dropped during thread-local teardown must not panic.
struct IngestPermit;
impl Drop for IngestPermit {
    fn drop(&mut self) {
        let _ = ADMISSION.try_with(|a| a.borrow_mut().release());
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

/// GE_EDGE_INGEST_PASS_B_RESERVE_MS: absent or unparseable falls back to the
/// default; <= 0 drops the reserve (the budget caps the wait by itself).
fn ingest_reserve(raw: Option<i64>) -> i64 {
    raw.unwrap_or(INGEST_PASS_B_RESERVE_MS_DEFAULT).max(0)
}

/// The admission wait bound in ms — `None` when the request budget cannot
/// cover the pass-B reserve, meaning even an instant admission would start
/// pass B already doomed to Error::Budget; the push fails busy instead.
/// Otherwise the configured wait capped by (budget left - reserve): the
/// request's own wall clock is the real deadline.
fn admit_wait_ms(wait_ms: i64, budget_left_ms: i64, reserve_ms: i64) -> Option<i64> {
    let usable = budget_left_ms.saturating_sub(reserve_ms);
    (usable > 0).then(|| wait_ms.min(usable))
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
    // slot. The RAII guard drops it from the queue on timeout/cancel.
    let ticket = ADMISSION.with(|a| a.borrow_mut().enqueue());
    let _ticket = AdmitTicket(ticket);
    let started = now();
    let mut waited = false;
    loop {
        let busy = heap_busy();
        if ADMISSION.with(|a| a.borrow_mut().try_acquire(ticket, busy)) {
            if waited {
                let waited_ms = now() - started;
                let n = ingest_in_flight();
                admit_log(format_args!("git-edge ingest admit: waited {waited_ms}ms (in_flight={n})"));
            }
            return Ok(IngestPermit);
        }
        waited = true;
        let waited_ms = now() - started;
        if waited_ms >= wait_ms {
            let n = ingest_in_flight();
            admit_log(format_args!("git-edge ingest admit: timed out after {waited_ms}ms (in_flight={n})"));
            return Err(Error::Limit(INGEST_BUSY_MSG.into()));
        }
        sleep(INGEST_POLL_MS).await;
    }
}

/// Wait for a pass-B slot, bounded by GE_EDGE_INGEST_WAIT_MS *and* by what
/// the request budget has left after the pass-B reserve — the budget clock
/// started before pass A, so an uncapped wait could admit a push with no
/// time left and fail it mid-pass on Error::Budget instead of answering
/// busy. `budget` is read-only: waiting itself spends no subrequests.
async fn admit_pass_b(env: &Env, budget: &ReqBudget) -> Result<IngestPermit, Error> {
    let raw = |n: &str| env.var(n).ok().and_then(|v| v.to_string().parse::<i64>().ok());
    let (slots, wait_ms) = ingest_gate(raw("GE_EDGE_INGEST_SLOTS"), raw("GE_EDGE_INGEST_WAIT_MS"));
    let reserve_ms = ingest_reserve(raw("GE_EDGE_INGEST_PASS_B_RESERVE_MS"));
    ADMISSION.with(|a| a.borrow_mut().set_slots(slots));
    let wait_ms = match admit_wait_ms(wait_ms, budget.ms_left() as i64, reserve_ms) {
        Some(w) => w,
        None => {
            admit_log(format_args!(
                "git-edge ingest admit: request budget cannot cover the {reserve_ms}ms pass-B reserve; busy"
            ));
            return Err(Error::Limit(INGEST_BUSY_MSG.into()));
        }
    };
    admit_wait(
        wait_ms,
        || crate::mem::live_bytes() > INGEST_LOW_WATER,
        crate::platform::now_ms,
        |ms: u64| Delay::from(std::time::Duration::from_millis(ms)),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
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
        let got = serde_json::to_value(IndexPost { pack, rows: &rows }).unwrap();
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

    /// Slots cap concurrency, release frees them, and the count can't go
    /// negative — the three behaviours the whole gate rests on.
    #[test]
    fn admission_acquire_release_and_capacity() {
        let mut a = Admission::new(2);
        let (t0, t1, t2) = (a.enqueue(), a.enqueue(), a.enqueue());
        assert!(a.try_acquire(t0, false));
        assert!(a.try_acquire(t1, false));
        assert_eq!(a.in_flight(), 2);
        assert!(!a.try_acquire(t2, false), "third ingest must wait at capacity");
        a.release();
        assert!(a.try_acquire(t2, false), "a freed slot admits the next waiter");
        assert_eq!(a.in_flight(), 2);
        a.release();
        a.release();
        assert_eq!(a.in_flight(), 0);
        a.release();
        assert_eq!(a.in_flight(), 0, "release must saturate at zero");
    }

    /// slots == 0 disables the gate: every caller acquires immediately, and
    /// in_flight still counts so the peers checks stay honest.
    #[test]
    fn admission_disabled_acquires_but_still_counts() {
        let mut a = Admission::new(0);
        for _ in 0..5 {
            let t = a.enqueue();
            assert!(a.try_acquire(t, true), "disabled gate ignores heap pressure too");
        }
        assert_eq!(a.in_flight(), 5);
    }

    /// A free slot is not enough while live heap sits above the low-water
    /// mark AND a peer ingest runs — but alone, or once the heap calms, the
    /// slot admits. This is what keeps leftover heap from stacking up.
    #[test]
    fn admission_heap_busy_waits_only_while_a_peer_runs() {
        let mut a = Admission::new(2);
        let t0 = a.enqueue();
        // no peers in flight: busy heap alone can't block (it's not ours to drain)
        assert!(a.try_acquire(t0, true));
        assert_eq!(a.in_flight(), 1);
        // a slot is free but heap is busy and the first ingest still runs: wait
        let t1 = a.enqueue();
        assert!(!a.try_acquire(t1, true));
        // heap calm: the free slot admits despite the peer
        assert!(a.try_acquire(t1, false));
        assert_eq!(a.in_flight(), 2);
        // at capacity the heap reading is irrelevant
        let t2 = a.enqueue();
        assert!(!a.try_acquire(t2, false));
        assert!(!a.try_acquire(t2, true));
    }

    /// Only the oldest live ticket may take a freed slot — a younger
    /// waiter can't barge ahead of an earlier push that has waited longer.
    #[test]
    fn admission_serves_oldest_ticket_first() {
        let mut a = Admission::new(1);
        let (t0, t1, t2) = (a.enqueue(), a.enqueue(), a.enqueue());
        assert!(a.try_acquire(t0, false));
        a.release(); // the slot frees while t1 and t2 still queue
        assert!(!a.try_acquire(t2, false), "the younger ticket must not barge");
        assert!(a.try_acquire(t1, false), "the oldest waiter takes the slot");
        assert_eq!(a.in_flight(), 1);
    }

    /// A waiter that times out or is dropped leaves the queue — its ticket
    /// is removed, so the next-oldest waiter becomes head instead of the
    /// line wedging behind a dead ticket.
    #[test]
    fn admission_abandoned_ticket_does_not_wedge_queue() {
        let mut a = Admission::new(1);
        let (t0, t1, t2) = (a.enqueue(), a.enqueue(), a.enqueue());
        assert!(a.try_acquire(t0, false));
        a.leave(t1); // t1 timed out or its future was dropped
        a.release();
        assert!(a.try_acquire(t2, false), "t2 is head now — t1's exit freed it");
        assert_eq!(a.in_flight(), 1);
        // leaving an already-acquired or unknown ticket is a harmless no-op
        a.leave(t0);
        a.leave(999);
        a.release();
        let t3 = a.enqueue();
        assert!(a.try_acquire(t3, false));
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
        let hold = block_on(admit_wait(60_000, || false, n, s)).unwrap();
        assert_eq!(ingest_in_flight(), 1);
        // the only slot is held; wait_ms=250 -> three 100 ms polls, then busy
        let (n, s) = clock_fns(&clock);
        let err = block_on(admit_wait(250, || false, n, s)).err().unwrap();
        assert!(matches!(err, Error::Limit(_)));
        assert!(err.message().contains(INGEST_BUSY_MSG), "{err}");
        assert_eq!(ingest_in_flight(), 1, "the timed-out waiter holds no slot");
        ADMISSION.with(|a| assert_eq!(a.borrow().waiting_len(), 0, "timeout must drop the ticket"));
        drop(hold);
        assert_eq!(ingest_in_flight(), 0, "dropping the permit releases the slot");
        let (n, s) = clock_fns(&clock);
        let _next = block_on(admit_wait(60_000, || false, n, s)).unwrap();
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
        let hold = block_on(admit_wait(60_000, || false, n, s)).unwrap();
        assert_eq!(ingest_in_flight(), 1);
        let (n, _s) = clock_fns(&clock);
        // Box::pin (not pin!): drop(fut) must drop the future itself so its
        // RAII ticket runs — a pin! shadow would only drop the Pin wrapper
        let mut fut = Box::pin(admit_wait(
            60_000,
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
        let elder = block_on(admit_wait(60_000, || false, n, s)).unwrap();
        assert_eq!(ingest_in_flight(), 1);

        // each waiter's sleep pends on a shared gate the test opens by hand,
        // so both futures queue first and their polls interleave below
        let gate = std::rc::Rc::new(std::cell::Cell::new(false));
        let sleep = {
            let g = gate.clone();
            move |_ms: u64| GateSleep(g.clone())
        };
        let mut w1 = Box::pin(admit_wait(60_000, || false, || 0i64, sleep.clone()));
        let mut w2 = Box::pin(admit_wait(60_000, || false, || 0i64, sleep));
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
}
