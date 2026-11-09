//! Phase one of the two-phase push (CONTRACTS.md 2.4-2.5, 3 ordering), in the edge Worker.
//! Ported from research/proofs/two-phase-push.md.

use std::collections::HashMap;

use gix_hash::ObjectId;
use gix_pack::data::entry::Header;
use worker::Stub;

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
pub async fn run(
    body: &mut BodyReader,
    bucket: &Bucket,
    stub: &Stub,
    repo: &RepoRoute,
    push: &PushId,
    kind: gix_hash::Kind,
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
}
