//! Phase one of the two-phase push (CONTRACTS.md 2.4-2.5, 3 ordering), in the edge Worker.
//! Ported from research/proofs/two-phase-push.md.

use std::cell::RefCell;
use std::collections::HashMap;

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
    sink.post_meta(&PackMeta::EMPTY, &[], budget).await?; // packs row 'ingesting' before any part is uploaded
    let key = keys::pack(&bucket.repo, &pack);
    let mut out = match PackWriter::create(bucket, &key, u32::try_from(entries.len()).unwrap_or(u32::MAX), kind, budget).await {
        Ok(w) => w,
        Err(e) => return Err(e),
    };
    let pending = keys::pending(&bucket.repo, push);
    // Pass B is the isolate's memory-heavy phase: admit at most
    // GE_EDGE_INGEST_SLOTS concurrent ingests. Pass A above already streamed
    // the body to R2, so waiting here never holds the client's upload.
    let permit = admit_pass_b(env).await?;
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
        if self.links.len() > MAX_LINKS {
            return Err(Error::Limit("push references too many objects (1,000,000 max)".into()));
        }
        const SAFE_MAX: i64 = (1 << 53) - 1;
        let commit_lo = i64::try_from(meta.commit_lo).unwrap_or(SAFE_MAX).min(SAFE_MAX);
        let pack = IndexPack {
            id: &self.pack.0,
            push_id: &self.push.0,
            count: meta.count,
            bytes: meta.bytes,
            commit_lo,
            commit_hi: meta.commit_hi,
        };
        // chunk the rows: an empty post still goes out once — it plants the
        // 'ingesting' packs row the whole two-phase ordering depends on
        if rows.is_empty() {
            return self.post_rows(&pack, &[], budget).await;
        }
        for chunk in rows.chunks(INDEX_BATCH) {
            self.post_rows(&pack, chunk, budget).await?;
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
/// Default 240 s: a large unsliced pass B can run ~90 s, and an HTTP request
/// has no wall-clock limit while the client stays connected, so outwaiting
/// the queue beats rejecting the push busy — at 60 s a queued push was
/// refused behind one ~90 s pass B.
const INGEST_WAIT_MS_DEFAULT: i64 = 240_000;
/// Poll cadence while waiting — short enough that a freed slot is picked up
/// quickly, long enough that a queued push costs ~nothing.
const INGEST_POLL_MS: u64 = 100;
/// Live heap above this while a peer ingest runs means its leftovers are
/// still draining — a new pass-B waits rather than stacking on top.
const INGEST_LOW_WATER: usize = 40 << 20;
/// The in-band busy message, shared by the admit timeout and a peer-contended
/// MEM_HARD trip: a retry helps, smaller slices would not have.
pub(crate) const INGEST_BUSY_MSG: &str =
    "server busy: too many large pushes in progress; retry in a minute";

/// The gate's pure state — unit-tested without a worker::Env or the isolate.
/// `slots == 0` disables admission: every caller acquires, and `in_flight`
/// still counts so the low-water wait and the honest Limit keep working.
struct Admission {
    in_flight: usize,
    slots: usize,
}

impl Admission {
    fn new(slots: usize) -> Self {
        Self { in_flight: 0, slots }
    }
    /// Admit one ingest iff a slot is free AND the heap is calm. `heap_busy`
    /// reports live_bytes() over INGEST_LOW_WATER; it blocks only while a
    /// peer ingest is in flight (`in_flight` counts admitted ingests — the
    /// caller isn't one yet), so stale heap from finished work can't stall
    /// the queue and unrelated heap can't either.
    fn try_acquire(&mut self, heap_busy: bool) -> bool {
        if self.slots == 0 {
            self.in_flight += 1;
            return true;
        }
        if self.in_flight >= self.slots || (heap_busy && self.in_flight > 0) {
            return false;
        }
        self.in_flight += 1;
        true
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
}

thread_local! {
    /// The isolate's one gate — wasm runs a single thread, so a thread_local
    /// is the whole isolate's state. Native tests get one per thread and
    /// exercise `Admission` directly; slots starts disabled until admit()
    /// reads env.
    static ADMISSION: RefCell<Admission> = RefCell::new(Admission::new(0));
}

/// Pass-B ingests inside the gated span, this request's permit included —
/// the memory guard reads it to choose between "smaller slices" (alone) and
/// the busy message (peers running).
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

/// GE_EDGE_INGEST_SLOTS / GE_EDGE_INGEST_WAIT_MS as pure values: absent or
/// unparseable falls back to the defaults (the env_i64 rule), slots <= 0
/// disables the gate, wait <= 0 fails fast on contention.
fn ingest_gate(slots: Option<i64>, wait: Option<i64>) -> (usize, i64) {
    let slots = slots.unwrap_or(INGEST_SLOTS_DEFAULT);
    let wait = wait.unwrap_or(INGEST_WAIT_MS_DEFAULT);
    (usize::try_from(slots).unwrap_or(0), wait.max(0))
}

/// Wait for a pass-B slot (bounded by GE_EDGE_INGEST_WAIT_MS), then hold the
/// RAII permit through `out.finish` + the final post_meta. On timeout the
/// push fails in-band — the body is already durable in R2, so the client
/// gets the report-status busy message, never a bare 5xx.
async fn admit_pass_b(env: &Env) -> Result<IngestPermit, Error> {
    let raw = |n: &str| env.var(n).ok().and_then(|v| v.to_string().parse::<i64>().ok());
    let (slots, wait_ms) = ingest_gate(raw("GE_EDGE_INGEST_SLOTS"), raw("GE_EDGE_INGEST_WAIT_MS"));
    ADMISSION.with(|a| a.borrow_mut().set_slots(slots));
    let started = crate::platform::now_ms();
    let mut waited = false;
    loop {
        let heap_busy = crate::mem::live_bytes() > INGEST_LOW_WATER;
        if ADMISSION.with(|a| a.borrow_mut().try_acquire(heap_busy)) {
            if waited {
                let waited_ms = crate::platform::now_ms() - started;
                let n = ingest_in_flight();
                worker::console_log!("git-edge ingest admit: waited {waited_ms}ms (slots={slots} in_flight={n})");
            }
            return Ok(IngestPermit);
        }
        waited = true;
        let waited_ms = crate::platform::now_ms() - started;
        if waited_ms >= wait_ms {
            let n = ingest_in_flight();
            worker::console_log!("git-edge ingest admit: timed out after {waited_ms}ms (slots={slots} in_flight={n})");
            return Err(Error::Limit(INGEST_BUSY_MSG.into()));
        }
        Delay::from(std::time::Duration::from_millis(INGEST_POLL_MS)).await;
    }
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

    /// Slots cap concurrency, release frees them, and the count can't go
    /// negative — the three behaviours the whole gate rests on.
    #[test]
    fn admission_acquire_release_and_capacity() {
        let mut a = Admission::new(2);
        assert!(a.try_acquire(false));
        assert!(a.try_acquire(false));
        assert_eq!(a.in_flight(), 2);
        assert!(!a.try_acquire(false), "third ingest must wait at capacity");
        a.release();
        assert!(a.try_acquire(false), "a freed slot admits the next waiter");
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
            assert!(a.try_acquire(true), "disabled gate ignores heap pressure too");
        }
        assert_eq!(a.in_flight(), 5);
    }

    /// A free slot is not enough while live heap sits above the low-water
    /// mark AND a peer ingest runs — but alone, or once the heap calms, the
    /// slot admits. This is what keeps leftover heap from stacking up.
    #[test]
    fn admission_heap_busy_waits_only_while_a_peer_runs() {
        let mut a = Admission::new(2);
        // no peers in flight: busy heap alone can't block (it's not ours to drain)
        assert!(a.try_acquire(true));
        assert_eq!(a.in_flight(), 1);
        // a slot is free but heap is busy and the first ingest still runs: wait
        assert!(!a.try_acquire(true));
        // heap calm: the free slot admits despite the peer
        assert!(a.try_acquire(false));
        assert_eq!(a.in_flight(), 2);
        // at capacity the heap reading is irrelevant
        assert!(!a.try_acquire(false));
        assert!(!a.try_acquire(true));
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
}
