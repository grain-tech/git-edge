# git-edge foundation contracts

Status: binding. Every revised proof is written against this file. Where this file and a proof disagree, this file wins. Where this file says "unverified", the item must be tested on day 1 and the result written back here.

Sources: `research/rust-server.md` (crate versions, API coverage), `research/platform-facts.md` (measured on workerd 4.129: `rowsWritten` counts index rows, `ctx.id.name` is populated, R2 awaits open the input gate with 7 of 8 updates lost, a second `setAlarm` cancels the first, multipart and range reads work on the local simulator only), `hand/cross-cutting-defects.md` (defects 1-9), the eight foundation reviews. Crate versions are pinned exactly as the memo lists them: `worker` 0.8.5, `gix-pack` 0.74.2, `gix-object` 0.64.1, `gix-hash` 0.26.2, `gix-packetline` 0.22.2, `gix-traverse` 0.61.0, `gix-features` 0.49.1, `gix-validate` 0.11.4.

Conventions in this file. `async fn` means the function may await (R2, DO stub, request body, alarm API). `fn` means the function must not await and must not touch any host API that returns a promise. "Sync span" means a run of DO code between two awaits; the platform delivers no other event to the DO inside a sync span. Object ids are per-repository — `meta.obj_format` pins `sha1` (default) or `sha256`; the negotiated `object-format` capability must match a pinned repo, and pins an unpinned one on first write.

---

## 1. Crate and module layout

One crate, `git-edge`, `crate-type = ["cdylib"]`, target `wasm32-unknown-unknown`, manifest as in the memo section 4 with `panic = "unwind"` (see section 10). No code runs at global scope; every module initialises lazily inside a handler.

```
src/
  lib.rs        #[event(fetch)] entry: delegates to edge::route
  wire/         pkt-line, sideband, v2 command parser, receive header parser, report-status writer
  store/        R2 key layout, pack entry codec, SQLite index schema + queries, MemFind
  repo_do/      #[durable_object] RepoDo: refs, meta, pushes, jobs tables; internal HTTP routes
  jobs/         alarm dispatcher + job kinds: janitor, gc_mark, gc_consolidate, gc_sweep
  pack/         ingest (stream, resolve, normalize) and generate (send-set, pack writer)
  auth/         credential check for the edge
  edge/         stateless Worker router; body decoding; calls RepoDo through Stub::fetch_with_request
  error.rs      the single Error enum (section 10)
```

Dependency direction: `edge -> {auth, wire, pack, store}`; `repo_do -> {wire, store, jobs, pack::generate}`; `jobs -> {store, pack}`; `pack -> {store, wire}`; `store -> gix-*`; `wire -> gix-packetline, gix-hash`. `wire` and `store::codec` never import `worker`. Nothing imports `repo_do` except `lib.rs` and `edge`.

### 1.1 `wire` (no awaits anywhere in this module)

```rust
pub const MAX_PKT_DATA: usize = 65516;   // 65520 - 4 length bytes (gix_packetline::MAX_DATA_LEN)
pub const MAX_BAND_DATA: usize = 65515;  // 65520 - 4 - 1 band byte; git's LARGE_PACKET_DATA_MAX - 1

pub enum Pkt<'a> { Data(&'a [u8]), Flush, Delim, ResponseEnd }

pub struct PktReader { buf: Vec<u8>, pos: usize }
impl PktReader {
    pub fn push(&mut self, bytes: &[u8]);                       // append raw body bytes
    pub fn next(&mut self) -> Result<Option<Pkt<'_>>, Error>;   // Ok(None) = need more bytes
    pub fn remainder(&mut self) -> Vec<u8>;                     // unread bytes after the last returned pkt
}
pub struct PktWriter { pub out: Vec<u8> }
impl PktWriter {
    pub fn data(&mut self, b: &[u8]) -> Result<(), Error>;      // Err(Error::Internal) if b.len() > MAX_PKT_DATA
    pub fn text(&mut self, s: &str) -> Result<(), Error>;       // data(s) with a trailing '\n' added if absent
    pub fn flush(&mut self); pub fn delim(&mut self); pub fn response_end(&mut self);
}
pub struct Sideband<'w> { w: &'w mut PktWriter }
impl<'w> Sideband<'w> {
    pub fn data(&mut self, b: &[u8]);          // splits into frames of <= MAX_BAND_DATA on band 1
    pub fn progress(&mut self, s: &str);       // band 2, one frame, truncated to MAX_BAND_DATA
    pub fn error(&mut self, s: &str);          // band 3, one frame
}

pub struct RefCommand { pub old: ObjectId, pub new: ObjectId, pub name: BString }
pub struct ReceiveCaps { pub report_status: bool, pub report_status_v2: bool, pub side_band_64k: bool,
                         pub delete_refs: bool, pub quiet: bool, pub ofs_delta: bool, pub agent: Option<BString> }
pub struct ReceiveHeader { pub commands: Vec<RefCommand>, pub caps: ReceiveCaps, pub shallow: Vec<ObjectId> }
/// Parses `shallow <oid>` lines, then `<old> <new> <name>[\0caps]` lines, up to and including the flush.
/// Ok(None) when the reader needs more bytes. After Ok(Some), `reader.remainder()` is the PACK (possibly empty).
pub fn parse_receive_header(r: &mut PktReader) -> Result<Option<ReceiveHeader>, Error>;

pub enum Filter { BlobNone, BlobLimit(u64) }
pub struct LsRefsArgs { pub symrefs: bool, pub peel: bool, pub unborn: bool, pub prefixes: Vec<BString> }
pub struct FetchArgs { pub wants: Vec<ObjectId>, pub haves: Vec<ObjectId>, pub done: bool, pub thin_pack: bool,
                       pub no_progress: bool, pub include_tag: bool, pub ofs_delta: bool,
                       pub deepen: Option<u32>, pub shallow: Vec<ObjectId>, pub filter: Option<Filter> }
pub enum V2Command { LsRefs(LsRefsArgs), Fetch(FetchArgs) }
/// Parses `command=...`, capability lines, delim, arguments, flush. Unknown command or argument -> Error::Protocol.
pub fn parse_v2_command(body: &[u8]) -> Result<V2Command, Error>;

pub struct RefRow { pub name: BString, pub target: ObjectId, pub peeled: Option<ObjectId> }
pub fn write_capability_advertisement_v2(w: &mut PktWriter);                       // upload-pack, Git-Protocol: version=2
pub fn write_advertisement_v0(w: &mut PktWriter, service: Service, head: Option<&BStr>, refs: &[RefRow]);
pub fn write_ls_refs(w: &mut PktWriter, args: &LsRefsArgs, head: Option<&BStr>, refs: &[RefRow]);
pub enum RefResult { Ok(BString), Ng(BString, &'static str) }
pub fn write_report_status(w: &mut PktWriter, unpack: Result<(), &str>, results: &[RefResult], caps: &ReceiveCaps);
```

Exact wire rules, all enforced inside `wire` and tested against real `git` (section 11):

1. Length prefix counts its own four bytes. `0000` flush, `0001` delim, `0002` response-end. A read of `0003` or a length that overruns the buffer is `Error::Protocol`.
2. Sideband frames carry at most 65515 data bytes. Progress and error frames are single frames.
3. v2 `fetch` response: `acknowledgments` section is **omitted entirely** when the client sent `done`. Otherwise it is `acknowledgments\n`, then `NAK\n` or one `ACK <oid>\n` per known have, then `ready\n` if a pack follows. If no pack follows, the response ends with flush after the section. If a pack follows: delim, optional `shallow-info` section, delim, `packfile\n`, sideband frames, flush. `response-end` is never written over smart HTTP.
4. `report-status` and `report-status-v2` bodies are written in band 1 when `caps.side_band_64k` is true, else as raw pkt-lines. Both end with a flush. Under sideband the final flush is written after the last band-1 frame, outside the band.
5. v0 receive-pack advertisement: `# service=git-receive-pack\n`, flush, then `<oid> <ref>\0<caps>\n` for the first ref, `<oid> <ref>\n` for the rest, flush. Empty repo: `<zeros> capabilities^{}\0<caps>\n` — the null oid is written in the first advertised format. Advertised receive caps, exactly: `report-status delete-refs side-band-64k quiet ofs-delta atomic object-format=<fmt> agent=git-edge/0.1`, where `object-format=` repeats per advertised format — the repo's pinned one, or `object-format=sha1 object-format=sha256` while unpinned. Not advertised: `push-options`, `report-status-v2`.
6. v2 capability advertisement, exactly: `version 2`, `agent=git-edge/0.1`, `ls-refs=unborn`, `fetch=shallow filter packfile-uris`, then one `object-format=<fmt>` line per advertised format (pinned repo's, or both while unpinned), flush. Not advertised: `wait-for-done`, `ref-in-want`, `sideband-all`, `server-option`.
7. Upload-pack is v2 only. A `GET info/refs?service=git-upload-pack` without `Git-Protocol: version=2` gets the v0 advertisement with capabilities `<object-format caps> agent=git-edge/0.1` only. A v0 `POST git-upload-pack` gets HTTP 400 with body `ERR protocol v2 required (git >= 2.26)\n` as one pkt-line.
8. `Git-Protocol: version=1` is answered as v0 with a leading `version 1\n` pkt (upload-pack) or ignored (receive-pack).

### 1.2 `store`

```rust
pub struct RepoId(pub String);   // 32 lowercase hex chars, section 8
pub struct PackId(pub String);   // 32 lowercase hex chars, generated by the edge at push begin
pub struct ObjLoc { pub pack: PackId, pub idx: u32, pub offset: u64, pub len: u32, pub kind: Kind, pub size: u64 }

pub mod keys {   // pure functions
    pub fn pack(repo: &RepoId, pack: &PackId) -> String;            // r/<repo>/packs/<pack>.pack
    pub fn pending(repo: &RepoId, push: &PushId) -> String;         // r/<repo>/pending/<push>.pack
    pub fn pending_part(repo: &RepoId, push: &PushId, part: &str);  // r/<repo>/pending/<push>.part-<part>
}
pub mod codec {  // sync, no worker imports
    /// Encodes one full (non-delta) pack entry: varint type/size header + zlib(data). Returns entry bytes.
    pub fn encode_entry(kind: Kind, data: &[u8], out: &mut Vec<u8>);
    /// Decodes one entry that starts at bytes[0]. Errors if the entry is a delta.
    pub fn decode_entry(bytes: &[u8]) -> Result<(Kind, Vec<u8>), Error>;
    pub fn entry_header(bytes: &[u8]) -> Result<(Kind, u64 /*size*/, usize /*header len*/), Error>;
}
pub struct MemFind { objs: HashMap<ObjectId, (Kind, Vec<u8>)>, pub bytes: usize }
impl gix_object::Find for MemFind { /* try_find returns Ok(None) on miss, never errors */ }
impl gix_object::Exists for MemFind {}
impl MemFind { pub fn insert(&mut self, id: ObjectId, kind: Kind, data: Vec<u8>); pub fn clear(&mut self); }

pub struct Bucket { inner: worker::Bucket, repo: RepoId }
impl Bucket {
    pub async fn read_range(&self, key: &str, offset: u64, len: u64) -> Result<Vec<u8>, Error>; // one subrequest
    pub async fn read_entries(&self, locs: &[ObjLoc]) -> Result<Vec<(ObjectId, Vec<u8>)>, Error>; // coalesced, 7.2
    pub async fn delete(&self, keys: &[String]) -> Result<(), Error>;                            // <= 1000 per call
}
pub struct PackWriter { mpu: MultipartUpload, key: String, part: Vec<u8>, parts: Vec<UploadedPart>,
                        offset: u64, count: u32, hasher: gix_hash::Hasher, commit_lo: u64, commit_hi: u64 }
impl PackWriter {
    pub async fn create(bucket: &Bucket, key: String) -> Result<Self, Error>;
    pub fn append_entry(&mut self, kind: Kind, data: &[u8]) -> (u64 /*offset*/, u32 /*len*/); // sync; buffers
    pub async fn flush_if_full(&mut self) -> Result<(), Error>;   // uploads one 8 MiB part when part.len() >= 8 MiB
    pub async fn finish(mut self) -> Result<PackMeta, Error>;     // patches header count, appends trailer, completes
    pub async fn abort(self);
}
pub struct Index<'s>(pub &'s worker::SqlStorage);  // sync SQLite queries, used only inside RepoDo and jobs
impl<'s> Index<'s> {
    pub fn lookup(&self, ids: &[ObjectId]) -> Result<Vec<Option<ObjLoc>>, Error>;   // live packs only
    pub fn insert_pack(&self, meta: &PackMeta, state: PackState) -> Result<(), Error>;
    pub fn insert_objects(&self, pack: &PackId, rows: &[ObjRow]) -> Result<(), Error>;  // <= 10,000 rows per call
    pub fn set_pack_state(&self, pack: &PackId, state: PackState) -> Result<(), Error>;
}
```

### 1.3 `repo_do`

```rust
#[durable_object]
pub struct RepoDo { state: State, env: Env, booted: RefCell<bool> }
impl DurableObject for RepoDo {
    fn new(state: State, env: Env) -> Self;
    async fn fetch(&self, req: Request) -> worker::Result<Response>;   // dispatches on path, section 1.3.1
    async fn alarm(&self) -> worker::Result<Response>;                 // jobs::dispatch(self)
}
impl RepoDo {
    fn boot(&self, hdr: &RepoHeaders) -> Result<Meta, Error>;          // sync: migrate schema, write/verify meta (section 8)
    fn list_refs(&self) -> Result<(Option<BString>, Vec<RefRow>), Error>;               // sync
    fn commit_push(&self, req: &CommitRequest) -> Result<CommitResponse, Error>;         // sync span, section 3
    async fn fetch_v2(&self, meta: &Meta, args: FetchArgs) -> Result<Response, Error>;  // section 9
}
```

Internal routes (all `POST` except the first, all require the headers of section 8):

| Path | Body in | Body out | Awaits inside |
|---|---|---|---|
| `GET /_do/refs` | - | JSON `{head, refs:[{name,target,peeled}]}` | none |
| `/_do/push/begin` | JSON `{push_id}` | JSON `{repo_id, refs_version, gc_epoch}` | none |
| `/_do/push/lookup` | JSON `{ids:[...]}` (<= 1000) | JSON `{locs:[ObjLoc|null]}` | none |
| `/_do/push/index` | JSON `{pack: PackMeta, rows:[ObjRow]}` (<= 10,000 rows) | `{}` | none |
| `/_do/push/commit` | JSON `CommitRequest` | JSON `CommitResponse` | none |
| `/_do/fetch` | raw v2 fetch body | v2 fetch response stream | R2 reads |
| `/_do/ls-refs` | raw v2 ls-refs body | pkt-line bytes | none |

Rows are JSON with hex ids. A route with "none" in the last column runs as one sync span from first byte parsed to response built.

### 1.4 `jobs`, `pack`, `auth`, `edge`

```rust
// jobs
pub struct Job { pub id: i64, pub kind: JobKind, pub run_at: i64, pub attempts: u32, pub cursor: Option<String>, pub payload: String }
pub enum JobKind { Janitor, GcMark, GcConsolidate, GcSweep }
pub enum SliceOutcome { Done, Continue { cursor: String }, Reschedule { run_at: i64 } }
pub struct SliceBudget { pub started_ms: f64, pub subrequests_used: u32 }   // section 7
pub async fn dispatch(d: &RepoDo) -> Result<(), Error>;                        // called by alarm()
pub fn enqueue(sql: &SqlStorage, kind: JobKind, run_at: i64, payload: &str) -> Result<(), Error>;  // sync, dedups by kind
pub async fn run_slice(d: &RepoDo, job: &Job, budget: &mut SliceBudget) -> Result<SliceOutcome, Error>;

// pack::ingest  (runs in the edge Worker)
pub struct EntryRec { pub offset: u64, pub header_len: u8, pub kind_or_delta: gix_pack::data::entry::Header, pub compressed_len: u32 }
pub async fn stream_to_pending(body: &mut BodyReader, bucket: &Bucket, push: &PushId, budget: &mut ReqBudget)
    -> Result<(Vec<EntryRec>, u32 /*count*/), Error>;     // verifies trailer SHA-1; Error::Protocol("bad pack checksum")
pub async fn resolve_and_normalize(bucket: &Bucket, pending_key: &str, entries: &[EntryRec],
    external_bases: &HashMap<ObjectId, ObjLoc>, out: &mut PackWriter, budget: &mut ReqBudget)
    -> Result<Vec<ObjRow>, Error>;                        // section 2.4
pub fn extract_links(kind: Kind, data: &[u8]) -> Result<Vec<ObjectId>, Error>;   // sync; commit/tree/tag references

// pack::generate  (runs in RepoDo)
pub async fn send_set(d: &RepoDo, bucket: &Bucket, wants: &[ObjectId], haves: &[ObjectId],
    filter: Option<&Filter>, deepen: Option<u32>, budget: &mut ReqBudget) -> Result<SendSet, Error>;  // section 9
pub async fn write_pack(bucket: &Bucket, set: &SendSet, out: &mut Sideband<'_>, budget: &mut ReqBudget) -> Result<(), Error>;

// auth
pub struct Principal { pub name: String, pub can_write: bool }
pub fn authenticate(req: &Request, env: &Env) -> Result<Principal, Error>;   // sync; Basic auth, section 12

// edge
pub async fn route(req: Request, env: Env) -> worker::Result<Response>;
pub struct BodyReader { /* section 6 */ }
```

---

## 2. Object storage spec (defects 3 and 4)

### 2.1 Decision

Every object at rest lives inside a **normalized pack**: a byte-exact git pack v2 whose entries are all full objects (no `ofs-delta`, no `ref-delta`), each entry `varint(kind,size) + zlib(data)`, with the usual 12-byte header and 20-byte SHA-1 trailer. There are no loose objects. Ingest resolves every delta, including thin-pack bases, and writes full objects. Deltas are never stored and never resolved at read time.

Why full objects: a reader resolves any SHA with one SQLite lookup plus **one R2 range read** of `[offset, offset+len)`, and the bytes it gets are already a valid pack entry that `pack::generate` copies verbatim into an outgoing pack. No inflate, no base, no chain on the read path. Why not "store the thin pack and resolve at read": a chain of depth d needs d reads across packs, the sync `Find` constraint means all of them must be prefetched before any compute, and the janitor could not delete a pack without knowing which chains cross into it. Why not one R2 object per git object: two subrequests per object caps a push at about 5,000 objects (review, content-addressed-r2-keys); a pack is one multipart upload whatever the object count.

### 2.2 R2 key layout

```
r/<repo_id>/packs/<pack_id>.pack        normalized pack, immutable once the packs row is live
r/<repo_id>/pending/<push_id>.pack      raw pack bytes exactly as received (thin, deltas, gzip removed), scratch
r/<repo_id>/pending/<push_id>.part-<n>  staged import parts (A31) — same push prefix so the sweep gets them all
```

`repo_id` comes from the DO `meta` table (section 8), never from `ctx.id.name`, never from the URL. Nothing else is ever written to R2 by the foundation. R2 `customMetadata` on a pack: `{ "repo": repo_id, "pack": pack_id, "count": "<n>", "created_at": "<unix ms>" }`. Metadata is informational for operators; no code path reads it.

### 2.3 SQLite tables in RepoDo

```sql
CREATE TABLE packs (
  id          TEXT PRIMARY KEY,           -- pack_id
  state       TEXT NOT NULL,              -- 'ingesting' | 'live' | 'dead'
  count       INTEGER NOT NULL,
  bytes       INTEGER NOT NULL,           -- total pack length including header and trailer
  commit_lo   INTEGER NOT NULL,           -- lowest offset of any commit entry (u64 max if none)
  commit_hi   INTEGER NOT NULL,           -- end offset of the last commit entry (0 if none)
  push_id     TEXT,                       -- null for packs written by gc_consolidate
  created_at  INTEGER NOT NULL,           -- unix ms
  dead_at     INTEGER                     -- set by gc_sweep; janitor deletes the R2 key later
) WITHOUT ROWID;

CREATE TABLE objects (
  sha     TEXT NOT NULL,                  -- 40 hex
  pack_id TEXT NOT NULL REFERENCES packs(id),
  idx     INTEGER NOT NULL,               -- ordinal of the entry inside the pack, 0-based
  offset  INTEGER NOT NULL,
  len     INTEGER NOT NULL,               -- entry length: header + zlib body
  kind    INTEGER NOT NULL,               -- 1 commit, 2 tree, 3 blob, 4 tag (git numbering)
  size    INTEGER NOT NULL,               -- inflated size
  PRIMARY KEY (sha, pack_id)
) WITHOUT ROWID;
CREATE INDEX objects_pack ON objects(pack_id, idx);
```

`created_at` is per pack, not per object; the grace period of section 5 is measured on packs. The reader query, and the only way any code resolves a sha:

```sql
SELECT o.pack_id, o.idx, o.offset, o.len, o.kind, o.size
FROM objects o JOIN packs p ON p.id = o.pack_id
WHERE o.sha = ? AND p.state = 'live' LIMIT 1;
```

A sha that appears in two live packs is legal (two pushes raced with overlapping content). Either row is correct because both hold the same bytes. `gc_consolidate` removes the duplicate.

### 2.4 Ingest (two passes, in the edge Worker)

Pass A, `stream_to_pending`: the body after the receive header (section 6) is streamed into `pending/<push_id>.pack` through `PackWriter`-style multipart with 8 MiB parts, unmodified. In the same pass the bytes are fed to `gix_pack::data::input::BytesToEntriesIter::new_from_header(reader, Mode::Verify, EntryDataMode::Ignore, Sha1)` over a `BufRead` adapter that yields the buffered window (section 6). Each entry produces an `EntryRec` (offset, header, compressed length). `Mode::Verify` checks the trailer. Memory: `EntryRec` is 24 bytes; the vector is capped at 2,000,000 entries (48 MB), beyond which ingest fails with `unpack error too many objects`.

Between passes: all `ref-delta` base ids that are not entries of this pack are looked up in batches of 1,000 through `/_do/push/lookup`. A base that is not live is `unpack error missing base <oid>`. Bases are fetched with `Bucket::read_entries` (coalesced, section 7.2) into a `HashMap<ObjectId, Vec<u8>>` of resolved bytes, capped at 32 MiB; beyond the cap bases are fetched on demand one range read each.

Pass B, `resolve_and_normalize`: the pending pack is read back in 8 MiB windows in offset order (one range read per window). For each entry in order: full object -> inflate; `ofs-delta` -> base is an earlier entry, taken from the resolved LRU (16 MiB) or re-read from `pending/` by its recorded offset (one range read); `ref-delta` -> base from the LRU, the external base map, or the pending pack. Delta application is `gix_pack::data::delta::apply`. The result is hashed with `gix_object::compute_hash`, links are extracted with `extract_links`, the object is appended to the normalized pack with `PackWriter::append_entry`, and an `ObjRow` is recorded. Commit entries update `commit_lo/commit_hi`. Every 10,000 rows are posted to `/_do/push/index`; the `packs` row is inserted with state `ingesting` on the first post. After `PackWriter::finish` returns, the pack is durable in R2 and fully indexed, and only then does the edge call `/_do/push/commit`.

Memory budget for pass B (must fit with the 128 MB isolate): window 8 MiB, resolved LRU 16 MiB, external base map 32 MiB, one base 32 MiB max, one delta result 32 MiB max, multipart part buffer 8 MiB, entry vector 48 MiB worst case. The single-object cap is therefore **32 MiB inflated**; a larger object is `unpack error object too large (32 MiB max)`. This is a foundation limit; LFS is out of scope (section 12). Delta-only packs from git normally place bases before deltas, so the LRU hits in the common case and pass B costs one range read per 8 MiB of pack.

Delete-only pushes carry no PACK; a push of new refs at existing commits carries a 0-object pack (12-byte header + trailer). Both produce no `packs` row and skip both passes.

### 2.5 Connectivity rule

Invariant: every object in a live pack references only objects that are in a live pack (or in the same pack). Ingest enforces it by induction: the set `extract_links(all entries) - {entries of this pack}` is looked up in batches of 1,000; any miss is `unpack error missing object <oid>`. New tips are checked to be in this pack or live. No deep walk is needed, because live objects are closed by the invariant. Pack objects that are unreachable from any command (junk in the pack) are accepted, as git does.

---

## 3. Ref transaction contract (defect 5)

```sql
CREATE TABLE refs (
  name       TEXT PRIMARY KEY,   -- full name, validated by gix_validate::reference::name_partial
  target     TEXT NOT NULL,      -- 40 hex; symbolic refs are not stored here
  updated_at INTEGER NOT NULL
) WITHOUT ROWID;
CREATE TABLE reflog (
  id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, old TEXT NOT NULL, new TEXT NOT NULL,
  push_id TEXT NOT NULL, principal TEXT NOT NULL, at INTEGER NOT NULL
);
CREATE TABLE pushes (
  id TEXT PRIMARY KEY, state TEXT NOT NULL,        -- 'open' | 'committed' | 'rejected' | 'expired'
  pack_id TEXT, principal TEXT NOT NULL, began_at INTEGER NOT NULL, ended_at INTEGER,
  gc_epoch INTEGER NOT NULL, result TEXT           -- JSON of per-ref results
) WITHOUT ROWID;
```

`HEAD` is the row `('head', 'refs/heads/main')` in `meta`; it is a symbolic name only and never stores an oid.

Ordering, strictly: (1) pack durable in R2 (`finish` returned); (2) all `objects` rows and the `packs` row inserted; (3) `/_do/push/commit`. The commit handler is **one sync span**: parse the JSON body fully before touching storage, then:

```
1. SELECT state, gc_epoch FROM pushes WHERE id=?           -> must be 'open'; else Error::Conflict
2. SELECT value FROM meta WHERE key='gc_epoch'             -> must equal pushes.gc_epoch; else reject all refs with
                                                              "ng <ref> gc ran during push, retry" and state='rejected'
3. UPDATE packs SET state='live' WHERE id=? AND state='ingesting'    (skipped when the push had no pack)
4. for each RefCommand, in client order, each independent:
     create:  INSERT INTO refs(name,target,updated_at) VALUES(?,?,?) ON CONFLICT DO NOTHING;  SELECT changes()
     update:  UPDATE refs SET target=?, updated_at=? WHERE name=? AND target=?;               SELECT changes()
     delete:  DELETE FROM refs WHERE name=? AND target=?;                                     SELECT changes()
   ok  <=> changes() == 1, read by the statement issued immediately after, in the same span.
   ng reason: "failed to update ref" (git's own string).
   on ok: INSERT INTO reflog(...)
5. if any ok or a pack went live at step 3: UPDATE meta SET value=value+1 WHERE key='refs_version'
6. UPDATE pushes SET state='committed', ended_at=?, result=? WHERE id=?
7. if any ok: jobs::enqueue(GcMark, now + 10 min)   (dedups: at most one pending GcMark)
```

Step 5's pack arm: a non-atomic push whose every command loses still promotes its pack at step 3 (the atomic all-fail path demotes it instead). That pack joined the live set a running GC snapshotted, so `refs_version` must move or the chain's guards never abort — a kept young pack could then link into swept objects (C2). A bump without a ref move costs only an A28 memo rebuild.

`SqlCursor::rows_written` is never read, by anyone, for any decision. Measured (`research/platform-facts.md` #1): `INSERT` into a `PRIMARY KEY` table reports `rowsWritten = 2` because the autoindex row is counted; on a `WITHOUT ROWID` table it reports 1; `SELECT changes()` reports 1 / 0 / 1 correctly in every case, including inside `transactionSync`. `changes()` is the CAS outcome. `RETURNING` is not used, so the contract does not depend on the SQLite version workerd ships. CI greps the crate for `rows_written` and fails on a hit outside tests.

Atomicity of the span: all statements above execute with no await between them. Measured (#4): eight concurrent DO calls that read, awaited R2, then wrote lost seven updates; the same eight with a synchronous SQL CAS after the await had exactly one winner. The platform commits the writes of one sync span atomically and rolls them back if the handler throws before its next await (memo, section 1; gc review "per-invocation write rollback on throw"). Whether `worker` 0.8.5 binds `transactionSync` is **unverified**; if it does, the span is additionally wrapped in it. If it does not, the span still holds because no other DO event can run inside it.

Multi-ref semantics: **per-ref, independent, in client order** by default (git's behavior). When the client sent `atomic` (advertised per rule 5), step 4 runs twice: a read-only dry pass validates every command — the CAS predicates become `SELECT target` comparisons — and any failure rejects the whole push; failing refs keep their `ng` reason, the rest get `ng <ref> atomic push failed`, the promoted pack demotes back to `ingesting` for the janitor, and no refs write, `refs_version` bump, or gc enqueue happens. A ref name that fails `gix_validate` or targets a non-live object gets `ng` for that ref only. Ref deletion of `HEAD`'s target is refused with `ng refs/heads/main deletion of the current branch prohibited`.

The connectivity result and the `objects` presence check are decided before the span (section 2.5) and re-guarded by `gc_epoch` in step 2. That is what makes a sweep between lookup and commit harmless: the push is rejected, not accepted with a hole.

---

## 4. Job dispatcher contract (defect 6)

```sql
CREATE TABLE jobs (
  id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL, run_at INTEGER NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0, cursor TEXT, payload TEXT NOT NULL DEFAULT '{}',
  state TEXT NOT NULL DEFAULT 'queued',        -- 'queued' | 'running' | 'dead'
  last_error TEXT
);
```

Rules:

1. **Only `jobs::rearm` calls `set_alarm`.** Measured (`research/platform-facts.md` #5): a second `setAlarm` cancels the first; only the later one fires. `enqueue` inserts the row and then calls `rearm(sql, storage)` which sets the alarm to `MIN(run_at) WHERE state='queued'`, or deletes the alarm when no row qualifies. Any module that wants background work calls `enqueue`. No other `set_alarm` call exists in the crate (enforced by a grep in CI).
2. `alarm()` -> `dispatch`: `SELECT ... WHERE state='queued' AND run_at <= now ORDER BY run_at, id LIMIT 1`. Mark it `running`. Run **one slice** with a fresh `SliceBudget`. Apply the outcome in a sync span: `Done` -> delete row (Janitor re-enqueues itself with `run_at = now + 15 min`); `Continue{cursor}` -> `run_at = now, cursor = ?`, state `queued`; `Reschedule{run_at}` -> as given. Then `rearm`. One slice per alarm firing; a `Continue` gets the very next firing.
3. Slice budget: 20,000 ms wall clock measured with `js_sys::Date::now()` (a conservative stand-in for CPU time, which Wasm cannot read), and 400 subrequests. A slice checks the budget between units of work and returns `Continue` when either is 80% spent.
4. Retry: on `Err`, `attempts += 1`, `last_error` set, `run_at = now + min(30 s * 2^attempts, 1 h)`, state `queued`, cursor kept. After 8 attempts the row becomes `dead` and stays for inspection; a dead job never blocks the queue because the selection filters on `queued`. `dispatch` never lets an error escape to the platform; the platform's own alarm retry is therefore never exercised.
5. Job kinds are `Janitor` (recurring, self-enqueued at boot if absent), `GcMark`, `GcConsolidate`, `GcSweep` (chained, section 5). `enqueue` dedups by kind: at most one `queued` or `running` row per kind. Every future background feature registers a new `JobKind` variant and a `run_slice` arm; none may set the alarm.

---

## 5. Janitor and GC contract (defect 2)

Constants: `GRACE = 1 h`, `PUSH_TIMEOUT = 1 h`, `GC_QUIET = 10 min`.

**Janitor** (every 15 min, one slice each):
1. `UPDATE pushes SET state='expired', ended_at=now WHERE state='open' AND began_at < now - PUSH_TIMEOUT` (sync).
2. For each pack with `state='ingesting'` whose push is `expired` or `rejected`: in one sync span set `state='dead', dead_at=now` and delete its `objects` rows.
3. Delete R2 keys: every key under `pending/<push>.` (the `.pack` and any `.part-*` staged parts) for every push not `open` and older than GRACE; `packs/<id>.pack` for every pack `dead` with `dead_at < now - GRACE`, then delete the `packs` row. At most 400 pushes/keys per slice, 8 list pages of 100 keys per push per pass — an unfinished prefix stays `swept_at NULL` and re-sweeps next pass.
The list of keys in step 3 is built from SQLite rows in the same slice, but each deleted key has been `dead`/not-`open` for at least GRACE, which is longer than any request can live. "Never delete in the same slice that listed" therefore means: **R2 deletion only ever targets rows that a previous slice, at least GRACE earlier, marked dead.** Marking and deleting never happen in one slice.

**GC** (`GcMark` -> `GcConsolidate` -> `GcSweep`), enqueued 10 min after a ref change:
1. `GcMark`: records `gc.refs_version`, `gc.started_at` and a fresh `gc.chain` id in `meta` on its first slice. Walks from all `refs` targets using the round loop of section 9, but marks whole entries: a bitmap per live pack (`marked` table: `pack_id, bitmap BLOB`), one bit per `idx`. Cursor = the `gc_frontier` table's unvisited ids. Only packs with `created_at < gc.started_at - GRACE` are candidates; younger packs are exempt from deletion but not from the walk — every non-blob object of every live non-candidate seeds `gc_frontier` at init, so links out of a kept young pack still mark candidate entries (C2). `gc.phase` tracks the chain (`build` when the frontier drains, `sweep` when the build finishes or is unneeded): a row whose payload chain id no longer matches `gc.chain` is a no-op, and a chain-less resurrected row may only run inside its matching phase (C3).
2. `GcConsolidate`: if candidate packs number >= 2 or any candidate has unmarked entries: write one new pack `packs/<new>.pack` containing exactly the marked entries of all candidate packs, copied verbatim via `read_entries` (section 7.2), `PackWriter` with `resume_multipart_upload` between slices (cursor = upload id, parts so far, position). The new pack is inserted `live` with its `objects` rows in the slice that completes it; from that moment every marked object has two live rows.
3. `GcSweep`, **one sync span**: `SELECT value FROM meta WHERE key='refs_version'`; if it differs from `gc.refs_version`, abort the GC (drop `marked`, delete the new pack's rows and mark that pack `dead`, enqueue a fresh `GcMark`) and return. Otherwise: `UPDATE packs SET state='dead', dead_at=now WHERE id IN (candidates)`; `DELETE FROM objects WHERE pack_id IN (candidates)`; `UPDATE meta SET value=value+1 WHERE key='gc_epoch'`; drop `marked`. The R2 keys are removed by the Janitor after GRACE.

Why the two review races are now impossible:
- *Sweep deletes an object a concurrent push is about to reference* (two-phase-push, gc reviews): a push looks objects up (`/_do/push/lookup`, sync) and later commits (sync). Both are sync spans in the same DO as `GcSweep`; they cannot interleave with it. If the sweep span runs between them, `gc_epoch` has changed and the commit is rejected in step 2 of section 3. If it runs after the commit, `refs_version` has changed and the sweep aborts. If before the lookup, the lookup misses and the push fails with `missing object`. No ordering yields a live ref pointing at a dead pack.
- *Janitor deletes bytes of a pack whose ref is live* (repo-do-ref-authority review): R2 deletion needs a `dead` row older than GRACE. A pack becomes `dead` only in `GcSweep` (guarded above) or for a push that is `expired`/`rejected` (never committed, so no ref points into it; `pending/` keys are never referenced by any ref).
- *A refs_version check that is not atomic with the delete* (gc review): the check and the SQLite deletes are the same sync span; the R2 delete is not load-bearing, because readers resolve through SQLite only.

**G4a compaction** (`GcCompact` -> `GcConsolidate`, sharing the single-chain machinery with `gc.mode='compact'`): bounds live-pack growth without reachability — it repacks only what it already holds, so a push or fetch during the build is legal by construction. A cohort is chosen by a pure tiered selector over live packs (tiers by object count from BASE_N=8, fanin T >= 2, packs < 64 MiB, at most 2T packs / MAX_ROWS rows / 256 MiB per swap); triggers fire on every pack that goes live, on each janitor pass, and as a drain re-trigger inside the swap, and each trigger also pulls an already-queued `gc_compact` row forward to the new wake — but only a row that has never been dispatched (`attempts=0 AND last_error IS NULL AND started_at IS NULL`): a row sitting out an error backoff or busy reschedule keeps its `run_at` rather than retrying on every push. `gc_compact` is one sync span: it verifies or aborts an existing chain (S1's owner-keyed orphan check — the dead rows' `last_error` is preserved under `dead.<kind>` and a compact cohort takes a `compact.quarantine` strike, n=2 excluding a pack; a stored strike map that fails to parse is stashed raw under `compact.quarantine.corrupt` (first corruption kept) and replaced by a fresh empty map, so strikes keep accumulating rather than freezing forever), dedups cohort entries into the `marked` bitmaps, and stakes `gc.chain` + `gc.phase='build'`. The shared `GcConsolidate` build then runs with the refs_version checks skipped, a cohort-liveness recheck on entry (a marked pack that died between init and the first build slice aborts losslessly), and every post-await write fenced on the captured `gc.chain` (plus `gc.new_pack == pos.pack` in compact mode); its finish swaps one sync span: the new pack goes `live` with `created_at = MAX(cohort.created_at)`, cohort packs die in place with their `objects` rows deleted, and scratch wipes — no `gc_epoch` bump, no mark, and a `gc_sweep` row in compact mode is a no-op. Fetches survive the swap: the walk path keeps the marked SHAs and replans once against the live index when a marked pack retires mid-fetch — a mark landing on a retired pack flags the send-set stale and forces the replan even when no bitmap survives to compare.

---

## 6. Request body contract (defect 7)

`BodyReader` is built by `edge` for every POST:

```rust
pub struct BodyReader { stream: Pin<Box<dyn Stream<Item = Result<Vec<u8>, Error>>>>, buf: Vec<u8>, eof: bool, total: u64 }
impl BodyReader {
    pub fn new(req: &Request) -> Result<Self, Error>;          // sync; wraps the stream, applies gzip
    pub async fn fill(&mut self, min: usize) -> Result<bool, Error>;   // read until buf.len() >= min or EOF; false on EOF
    pub fn buffered(&self) -> &[u8]; pub fn consume(&mut self, n: usize);
}
```

1. If `Content-Encoding: gzip`, `new` pipes `req.inner().body()` through `web_sys::DecompressionStream::new("gzip")` (`ReadableStream::pipe_through`) and reads the result. Converting the resulting `web_sys::ReadableStream` into a Rust `Stream` uses the `wasm-streams` crate (`ReadableStream::from_raw(..).into_stream()`); whether `worker` re-exports it is **unverified**, so it is listed as a direct dependency. No other encoding is accepted (415).
2. `Content-Length` is never trusted or required; git sends chunked bodies above `http.postBuffer` (1 MiB). The zone body cap (100 MB Free/Pro) applies before our code and is documented, not handled.
3. Command section split: `edge` fills the reader in 64 KiB steps and calls `wire::parse_receive_header` (or `parse_v2_command` on the whole body for upload-pack, whose bodies are small) until it returns `Some`. `PktReader::remainder()` plus the rest of the stream is the PACK. The command section is capped at 1 MiB (`Error::Protocol` beyond).
4. The pack is written to R2 with `create_multipart_upload`, parts of exactly 8 MiB except the last, at most 10,000 parts (80 GB, never reached). The equal-size rule is now *enforced* fact, not assumption: miniflare's `completeMultipartUpload` rejects non-final parts of differing sizes with `BadUpload` (10048), matching production R2 — this is what wedged `gc_consolidate` when `checkpoint()` drained a variable-size buffer (audit round 6). Every part-producing path — `flush_if_full`, `checkpoint`, `finish` — must therefore emit parts of exactly `PART` bytes except the final one. The same 8 MiB buffer is the `BufRead` window for `BytesToEntriesIter`; an entry longer than the window is handled by the iterator's own incremental read because `EntryDataMode::Ignore` skips bodies without buffering them.
5. Memory per receive-pack request: body window 8 MiB + multipart part 8 MiB + entry vector <= 48 MiB in pass A; pass B as in section 2.4. Upload-pack: request body <= 1 MiB; response is streamed through `Response::from_stream` with at most one 8 MiB read window plus one 64 KiB sideband frame in flight.

---

## 7. Subrequest and CPU budgeting (defect 8)

| Limit | Free | Paid | Source |
|---|---|---|---|
| Subrequests per invocation (R2 calls count) | 50 | 10,000 | Workers limits page (memo) |
| CPU per invocation | 10 ms | 30 s default, 300 s with `limits.cpu_ms = 300000` | same |
| Memory per isolate | 128 MB | 128 MB | same |
| Request body | 100 MB | 100/200/500 MB by zone plan | reviews |
| R2 multipart | parts >= 5 MiB, equal size, <= 10,000 | same | R2 docs |

The foundation targets the Paid plan; `wrangler.jsonc` sets `limits.cpu_ms = 300000`. On Free the edge refuses pushes with more than 20 range reads projected (`503 plan limit`). The subrequest limit is **not enforced by local workerd** (`research/platform-facts.md` #7: 1,200 R2 heads succeeded locally), so the conformance harness cannot catch a budget bug; `ReqBudget` is the only guard until the deployed-Worker measurement is done, and the harness asserts on the `ReqBudget` counters reported in a `x-ge-subrequests` response header instead.

```rust
pub struct ReqBudget { pub max_subrequests: u32, pub used: u32, pub started_ms: f64, pub max_ms: f64 }
impl ReqBudget { pub fn charge(&mut self, n: u32) -> Result<(), Error>; }   // Error::Budget when exceeded
```

Rules:
1. Every `Bucket` and stub call goes through `budget.charge(1)` first. A request starts with `max_subrequests = 9,000` (headroom for the shim), `max_ms = 240,000`.
2. **Coalesced range reads.** `Bucket::read_entries` sorts locations by `(pack, offset)`, merges neighbours whose gap is < 256 KiB, splits merged spans at 8 MiB, and issues one range read per span. Callers never issue per-object reads.
3. **Batched lookups.** `/_do/push/lookup` takes up to 1,000 ids; `/_do/push/index` takes up to 10,000 rows. A push of N objects costs about `N/10,000 + N/1,000 + pack_bytes/8 MiB + 2` subrequests, so 1,000,000 objects fit in the paid budget.
4. **Commit region reads.** For negotiation, commits of a pack are loaded with one range read of `[commit_lo, commit_hi)` per pack, cached per request.
5. Anything that cannot finish inside one request's budget is not done in a request: it becomes a job slice (section 4), which saves its cursor and continues on the next alarm.

---

## 8. Repo identity (defect 9)

```sql
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID;
-- rows: repo_id, owner, repo, head, refs_version, gc_epoch, created_at, schema_version
```

1. The edge derives the DO stub with `env.durable_object("REPO").id_from_name(&format!("{owner}/{repo}"))` and sets headers `x-ge-owner: <owner>` and `x-ge-repo: <repo>` on every stub request. Owner and repo are validated at the edge: `[A-Za-z0-9._-]{1,64}` each, a trailing `.git` stripped from the repo.
2. `RepoDo::boot` runs at the start of every `fetch` and `alarm` (it is cheap after the first time): if `meta.repo_id` is absent, it inserts `repo_id` = 32 lowercase hex chars from 16 random bytes (`web_sys::Crypto::get_random_values_with_u8_array` on the global `crypto`; exact binding path unverified), `owner`, `repo`, `head = refs/heads/main`, `refs_version = 0`, `gc_epoch = 0`, and enqueues the Janitor. If present, the headers must equal the stored owner/repo, else `Error::Internal("identity mismatch")` (500). `alarm` has no headers and skips the comparison.
3. Measured (`research/platform-facts.md` #2): on workerd 4.129 `ctx.id.name` **is** populated inside a DO created with `idFromName`, contrary to the five reviews that assumed it is undefined. Production has not been checked. The contract therefore: the stored `meta` rows are the source of truth for owner/repo and repo_id; `ctx.id.name` (reached through `js_sys::Reflect::get` on the inner id, workers-rs issue #760) may be read in exactly one place, `RepoDo::boot`, and only to cross-check the headers, logging a warning on mismatch or absence. No R2 key, SQL row, or response is ever derived from `ctx.id.name`. CI greps `repo_do` for `Reflect::get` and allows the single occurrence in `boot`.
4. R2 keys use `repo_id`, so a future rename changes two `meta` rows and nothing in R2.

---

## 9. Async-then-sync rule

Rule: **an `async fn` loads bytes into `MemFind`; a `fn` computes over `MemFind`; the loop between them is bounded and explicit.** gitoxide code (`gix_traverse`, `gix_pack::data::output`, `gix_object` parsing) is only ever called from a `fn` that receives `&MemFind` (or a `&[u8]` window). A traversal that discovers what to load next is written as rounds:

```rust
loop {
    let missing: Vec<ObjectId> = plan_next(&mem, &state);   // sync: parse loaded objects, list unloaded ids
    if missing.is_empty() { break; }
    let locs = index_lookup(&missing)?;                      // sync (in DO) or one stub call (in edge)
    let bytes = bucket.read_entries(&locs, budget).await?;   // async, coalesced
    for (id, entry) in bytes { let (k, d) = codec::decode_entry(&entry)?; mem.insert(id, k, d); }
}
let answer = compute(&mem);                                  // sync gitoxide
```

Worked example, v2 `fetch` negotiation in `RepoDo::fetch_v2`:

1. Parse `FetchArgs` (sync). Look up every want and have with `Index::lookup` (sync). Unknown want -> ERR `upload-pack: not our ref <oid>`. Unknown haves are dropped. `acks` = known haves.
2. Decide readiness (sync): `ready = args.done || args.haves.is_empty() || !acks.is_empty()`. If not ready, write the acknowledgments section with `NAK`, flush, return. This is git's stateless-RPC behaviour: the client sends more haves or `done`.
3. Prefetch commits by rounds. `frontier` = wants that are commits (annotated tags are peeled by loading the tag object first, one round). Each round: `missing` = frontier ids not in `mem`; group their locations by pack; for each pack read `[commit_lo, commit_hi)` once per request (cached) and insert every commit found; ids still missing after that are read with `read_entries`. Then, sync, for each newly loaded commit `gix_object::CommitRefIter::from_bytes(data).parent_ids()`: a parent that is in `acks` or already loaded is not added; every other parent joins the next frontier. Rounds end when the frontier is empty. Bound: after 200,000 loaded commits or 64 MiB in `mem`, ERR `fetch too large for this server; clone instead` (section 12 names the wave-1 fix).
4. Sync walk: `gix_traverse::commit::topo::Builder::from_iters(&mem, wants, Some(acks))` (exact constructor name to be verified against 0.61.0; the Topo walk with hidden ends is what is required), collecting interesting commit ids. Commits reachable only through non-ack ancestors of an ack are sent as a superset; the protocol permits supersets.
5. Trees and blobs by rounds: frontier = root trees of interesting commits; each round loads missing trees with `read_entries` (blobs are never loaded; their `ObjLoc` is enough), and, sync, `gix_object::TreeRefIter` lists entries; a tree entry id already in `seen` is skipped. `Filter::BlobNone` skips blob entries; `BlobLimit(n)` skips blobs with `size > n` (size comes from the lookup, no read). `deepen n` cuts the commit frontier at depth n and records `shallow` lines. Result `SendSet` = one bitmap per pack.
6. Write: acknowledgments (if not `done`) with ACKs and `ready`, delim, `shallow-info` if any, delim, `packfile`. `write_pack` streams each pack's marked entries in offset order with 8 MiB windows, copying entries verbatim through `Sideband::data`, header count patched up front from the bitmap popcount, trailer from `gix_hash::Hasher`. The whole response is `Response::from_stream`; an error mid-stream writes one band-3 frame and ends the stream.

The same loop shape, with "commit region" replaced by "all live packs in offset order", is `GcMark`; its cursor is the frontier.

---

## 10. Error and panic policy

```rust
pub enum Error {
    Protocol(String),   // malformed client bytes: bad pkt-line, unknown command, bad oid, bad ref name
    Auth,               // 401 with WWW-Authenticate: Basic realm="git-edge"
    Forbidden,          // 403 principal cannot write
    NotFound,           // 404 unknown repo route
    Conflict(String),   // push state wrong (not open, expired)
    Unpack(String),     // ingest failure reported as `unpack <msg>` in report-status
    Budget,             // subrequest or time budget exhausted
    Limit(String),      // foundation limit hit (object > 32 MiB, fetch too large)
    Storage(String),    // R2 or SQLite failure
    Internal(String),   // invariant broken (identity mismatch, encode overflow)
}
```

Mapping. Before any response byte is written: `Protocol` -> 400, `Auth` -> 401, `Forbidden` -> 403, `NotFound` -> 404, `Budget`/`Limit` -> 413, `Storage`/`Internal` -> 500; the body is one pkt-line `ERR <message>\n` for git-protocol POSTs and plain text for `info/refs`. After response bytes have started (fetch stream): one band-3 `ERR` frame, then end the stream. For receive-pack after the header was parsed: HTTP 200 with `unpack <message>` and `ng <ref> unpack failed` for every command, so the client prints a reason rather than "hung up unexpectedly".

Client bytes: no `unwrap`, `expect`, `[]` indexing, `as` narrowing casts, or arithmetic that can overflow on values derived from the request or from R2/SQLite content. Enforced by `#![deny(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing, clippy::panic, clippy::arithmetic_side_effects)]` in `wire`, `store::codec`, `pack`, and `edge`. Every gitoxide `Result` is propagated with `?` into `Error::Protocol` or `Error::Unpack`.

Panic decision: build with `worker-build --release -- --panic-unwind` and `panic = "unwind"` in `[profile.release]`. A panic then becomes a JS `PanicError` that the shim turns into a 500 and that leaves the DO's uncommitted sync-span writes rolled back; with `abort` the isolate dies mid-span and the platform behaviour is less well documented. Whether the rollback holds for a `PanicError` raised inside `fetch` is **unverified**; a day-1 test panics inside `commit_push` after step 4 and checks that no ref moved.

---

## 11. Conformance test plan

Harness: `tests/conformance/run.sh` starts `wrangler dev` (workerd with local R2 and DO emulation) on port 8787, creates a temporary `GIT_DIR`, and runs the scenarios with the system `git`, asserting on exit codes, `git rev-parse`, `git fsck --strict`, and `git ls-remote`. `GIT_TRACE_PACKET=1 GIT_TRACE_CURL=1` output is captured on failure. CI matrix: git 2.43, 2.45, 2.47 (built from tags into the runner image), Paid-plan limits in `wrangler.jsonc`. A `git2`-based second harness (memo, section 2) is added later and is not a gate.

| # | Scenario | Commands | Assertion |
|---|---|---|---|
| 1 | Clone empty repo | `git clone $U/o/r` | exit 0, "warning: You appear to have cloned an empty repository", no refs |
| 2 | Push new branch | `git push origin main` (3 commits) | `ok refs/heads/main`; `ls-remote` shows the sha |
| 3 | Clone with tags | annotated + lightweight tag pushed, `git clone`, `git tag -l`, `fsck` | both tags present, `fsck` clean, peeled line in `ls-refs` |
| 4 | Push delete | `git push origin :topic` | no PACK sent, `ok`, `ls-remote` lacks the ref; delete of `main` gives `ng` |
| 5 | Non-fast-forward rejected | second clone amends and pushes | client prints `! [rejected]`; with `--force` the CAS still fails if the advertised old oid is stale, matching git |
| 6 | Concurrent pushes to same branch | two clones push different commits at once (`&`, `wait`) | exactly one `ok`, the other `ng ... failed to update ref`; `fsck` on a fresh clone clean |
| 7 | Push > 1 MiB, chunked | commit a 3 MiB random blob, `push` | request had no Content-Length, `pending/` multipart used, clone reproduces the blob byte-exact |
| 8 | Small gzip push | `-c http.postBuffer=1` is not enough; use `GIT_CURL_VERBOSE` to confirm `Content-Encoding: gzip` on a small push | push ok |
| 9 | Thin pack | modify one line of a 100 KiB file, push | client log shows `ref-delta` in `GIT_TRACE_PACKET`; ingest resolved it (server log), clone reproduces both versions |
| 10 | Blobless clone + checkout | `git clone --filter=blob:none`, `git checkout HEAD~1`, `git fsck` | lazy fetch of blobs by oid succeeds (`want <blob>` accepted) |
| 11 | Shallow clone + push | `git clone --depth 1`, commit, `push` | fetch sent `deepen 1`, `shallow-info` returned, push with `shallow` lines accepted |
| 12 | Fetch after push (incremental) | clone A pushes 50 commits, clone B `git fetch` | acknowledgments section shows `ACK`, pack contains only new objects (count check), `fsck` clean |
| 13 | v0 upload-pack client | `git -c protocol.version=0 clone` | fails with the `ERR protocol v2 required` line, not a hang |
| 14 | Janitor and GC | force-push away 100 commits, advance the fake clock past 10 min + GRACE, fire the alarm via the local scheduler | dead pack row appears, R2 key removed after GRACE, clone `fsck` clean, `gc_epoch` bumped |
| 15 | Panic rollback (day 1) | test hook panics after CAS step 4 | no ref moved, 500 returned |

Each revised proof names the scenarios it must pass, and adds at most two scenarios of its own to this table. A proof whose feature cannot be exercised by a stock `git` binary states what harness step replaces it.

---

## 12. Out of scope for the foundation

The following are not built, not advertised, and must not be assumed by a revised proof of the 33 non-foundation ideas:

- Delta compression at rest or on the wire (all packs are full-object; `thin-pack` is accepted from clients, never sent).
- `push-options`, `report-status-v2` option lines, `wait-for-done`, `ref-in-want`, `sideband-all`, `packfile-uris`, `bundle-uri`, `object-info`, `server-option`.
- Protocol v0/v1 upload-pack negotiation (`multi_ack_detailed`, `no-done`).
- `deepen-since`, `deepen-not`, `deepen-relative`, `tree:<n>` and `sparse:` filters, `include-tag` beyond peeled tags of wanted commits.
- Objects larger than 32 MiB inflated, LFS, presigned uploads, repository imports.
- Commit-graph tables in SQLite (`commits`, `introduced`), precomputed clone packs, pinned bases, in-DO object cache, replicated refs, cross-repo dedup, forks.
- Repo rename, deletion, quotas, per-branch DOs, WebSockets, queues, hooks, CI chains, webhooks.
- Multi-tenancy and per-user permissions. `auth` in the foundation compares HTTP Basic credentials against two secrets, `GE_READ_TOKEN` and `GE_WRITE_TOKEN`; `can_write` is true only for the write token. Anonymous access is refused.
- SHA-256 repositories.
- Free-plan operation beyond the 20-range-read refusal in section 7.
- Metrics, tracing beyond `console_log!`, admin API.

A revised proof that needs one of these items writes it as a dependency on a named later idea, and its proof code compiles against the signatures in section 1 exactly as written here.

## Corrections from the Rust spike (measured 2026-09-13, see research/rust-spike.md)

These override anything above or in research/rust-server.md that disagrees.

1. `gix_packetline` 0.22.2: the encoder and writer live under `gix_packetline::blocking_io::{encode, Writer, StreamingPeekableIter}`. `gix_packetline::encode` holds only the `Error` type. Use `use gix_packetline::blocking_io::encode;`.
2. `gix_pack::data::delta::apply` and `decode_header_size` are `pub(crate)` in 0.74.2. Resolve deltas through `gix_pack::data::File::<&[u8]>::from_data(bytes, path, kind)`, then `entry(offset)` and `decode_entry(entry, &mut out, &mut gix_zlib::Inflate, &resolve_ref_delta, &mut cache::Never)`. `gix-zlib` 0.1.0 is therefore a direct dependency. `BytesToEntriesIter` yields compressed entry bytes only.
3. `[profile.release] strip = true` breaks `worker-build` (the abort handler needs the `target_features` section that `--strip-all` removes). Use `strip = "debuginfo"`.
4. Client-controlled parse failures must return HTTP 400 from the handler. A `worker::Error` propagated out of `fetch` surfaces as an uncaught exception and HTTP 500.
5. Measured: the whole spike Worker with gix-hash, gix-object, gix-packetline, gix-pack, gix-traverse and gix-zlib linked is 605 KB after wasm-opt, 262 KB gzipped on upload. Cold instantiate cost is 20 to 30 ms on local workerd. Real git 2.43 `ls-remote` over protocol v2 and v0 succeeded against it, and a 6-object pack with two OFS deltas was parsed, resolved, hashed and walked on workerd with ids matching `git verify-pack`.

## Amendments after the foundation re-review (2026-09-13)

The 14 second-pass foundation reviews found the following gaps in this contract itself. These amendments override the sections they name. Edge proofs must follow the amended form.

**A1. Budget-carrying signatures (overrides 1.2, 1.4, 9).** Every function that makes an R2 or stub call takes `budget: &mut ReqBudget` as its last parameter, and `Bucket` holds no budget. Amended signatures:
`Bucket::read_range(&self, key: &str, offset: u64, len: u64, budget: &mut ReqBudget) -> Result<Vec<u8>>`;
`Bucket::read_entries(&self, locs: &[(ObjectId, ObjLoc)], budget: &mut ReqBudget) -> Result<Vec<(ObjectId, Vec<u8>)>>`;
`PackWriter::create(bucket: &Bucket, key: &str, expected: u32, budget: &mut ReqBudget)`, `PackWriter::flush_if_full(&mut self, budget)`, `PackWriter::finish(self, budget) -> Result<PackMeta>`; `append_entry` and `encode_entry` return `Result`;
`pack::ingest::run(body, env, stub, repo, push_id, begin, budget)` (7 args);
`resolve_and_normalize(window, index_sink: &mut IndexSink, budget) -> Result<()>` where `IndexSink::post(&mut self, meta: &PackMeta, rows: Vec<ObjRow>, budget) -> Result<()>` posts every 10,000 rows.
`Bucket.repo` and `RepoDo.state` are `pub(crate)`.

**A2. Error propagation and the after-header rule (overrides 3, 10).** Inside a Durable Object route, a `Storage` or `Internal` error raised inside a sync span must propagate as `Err` out of `fetch` so the platform discards the uncommitted span. Never convert it to a `Response` inside the DO. The edge converts. Before the receive-pack header is parsed, `Conflict` maps to HTTP 409. After the header is parsed, every error, including `Limit`, `Budget`, `Conflict` and `Storage`, is answered with HTTP 200 and a report-status of `unpack <message>` plus `ng <ref> <reason>` for every command, because git's remote-curl discards any body with status 300 or higher. The `ERR` pkt-line for a v0-only client therefore also travels in a 200 response.

**A3. Alarm arming (overrides 4.1).** `jobs::enqueue` is sync and only writes the row. It never calls `set_alarm`, which is async in worker 0.8.5. Every async route or slice that called `enqueue` must call `jobs::rearm(&self).await` after its sync span returns. `transactionSync` is absent from worker 0.8.5; span atomicity rests on the no-await rule alone, and panic rollback is unverified (scenario 15 stays a day-1 test).

**A4. Slices (overrides 4.2, 4.3).** A slice checks `SliceBudget::spent_80pct()` between units of work and returns `Continue { cursor }` when true. `Reschedule` clears the cursor. Frontier loads inside a slice are chunked so one `read_entries` call charges at most 64 coalesced spans. A `dead` job of kind `Janitor`, `GcMark` or any per-repo maintenance kind is re-enqueued at the next `boot`; `boot` enqueues `Janitor` whenever no `queued` or `running` Janitor row exists, not only on first creation.

**A5. Schema additions (overrides 2.3, 3).** `refs` gains `peeled TEXT` filled at commit from the tag object the edge inflated, and `ls-refs` emits `peeled:` from it. `pushes` gains `swept_at INTEGER`; the Janitor sets `swept_at` and never deletes `pushes` rows. A rejected push writes `pushes.result`. `packs` rows are created `ingesting` on the first `/_do/push/index` post with `ON CONFLICT(id) DO UPDATE ... WHERE state='ingesting' AND push_id=excluded.push_id`, so `finish` metadata is recorded; `commit_lo` with no commits is stored as `i64::MAX`. `MAX_LINKS = 1,000,000` per push is a foundation limit and maps to `Limit`.

**A6. Bound parameters (new).** DO SQLite allows at most 100 bound parameters per statement. Any `IN (...)` list is batched at 90 or replaced by `json_each(?)` over one JSON array parameter.

**A7. Memory cap (overrides 2.4).** The single-object cap is 16 MiB inflated, not 32 MiB. The normalize pass must hold at most two copies of an entry at once: the window slice and the decoded output. `decode_mini` is removed; decode reads directly from the window.

**A8. Dependency direction (overrides 1.1).** Shared request headers, DTOs and the `respond`/`finish` helpers live in `wire::http`, not in `edge`, so `repo_do` never imports `edge`. A small `platform` module owns every `js_sys::Reflect` use (random bytes, `ctx.id.name` cross-check); section 8.3's grep allows `Reflect::get` only in `platform`.

**A9. Registration lists (overrides 1.3, 1.4, 12).** A post-foundation module may add routes, `JobKind` variants, tables and R2 key prefixes if it lists them in one `REGISTRY` block at the top of its file. The foundation lists in 1.3 and 1.4 are the minimum, not the maximum.

**A10. Pass A of ingest (overrides 2.4, 6.4).** Pass A does not use `BytesToEntriesIter`, which is synchronous over `BufRead` and cannot await body bytes mid-entry. Pass A parses entry headers with `gix_pack::data::entry::Header::from_bytes`, inflates with `gix_zlib::Inflate` tracking `total_in`, and hashes the raw pack with the SHA-1 hasher. `BytesToEntriesIter` and `File::decode_entry` are used in pass B over in-memory windows.

## Amendments from the implementation pass (2026-09-14, server/ compiles and serves git)

The crate in `server/` now compiles for `wasm32-unknown-unknown`, runs on local workerd,
and passes `tests/conformance/run.sh` (push, incremental push with client deltas, branch
create/delete, tag, delete-only push, clone, incremental fetch, `git fsck --strict`,
malformed-pack report-status). These amendments record what the proofs' prose missed that
only a real compile and a real git client found.

**A11. Report-status inner flush (overrides 1.1 rule 4).** The report-status pkt-stream must
end with a flush packet *inside* the sideband framing (`write_report_status` emits
`body.flush()` before wrapping). Without it, git's demuxed inner reader hits pipe EOF and
dies with `the remote end hung up unexpectedly` — after having already applied the ref
updates. A push can therefore commit and still read as a client failure; this is exactly the
class of bug the proofs' "review by reading" could not see and the conformance suite catches.

**A12. `report-status-v2` is not advertised (overrides 1.1 capability list).** v2 adds an
`option`-line section after ref results; we emit none, so we advertise `report-status` only.
Every observed client (git 2.54) falls back to v1. A proof that wants v2 must implement the
option-line grammar, not just the capability string.

**A13. `Index::lookup` keys by sha (clarifies 2.3).** The `objects JOIN packs` reader query
must `SELECT o.sha` and build its result map keyed on sha, not pack id. The proof text said
"returns Option<ObjLoc> per id"; the compiled version made this concrete — a map keyed on
`pack_id` silently finds nothing and every ref update fails with `missing necessary objects`.

**A14. workers-rs 0.8.5 API facts (extends "Corrections from the Rust spike").**
- `Response::with_status(u16)` consumes `self`; build status responses as
  `Response::ok(body)?.with_status(code)`.
- Responses returned by `stub.fetch_with_request` carry immutable headers. To set
  `Content-Type` on a forwarded DO response, read `resp.bytes()` and rebuild the `Response`.
- The request body is `req.stream() -> ByteStream` (a `Stream<Item = Result<Vec<u8>>>`).
  `req.inner().body()` returns the raw `web_sys::ReadableStream`; it is the right source only
  for the `DecompressionStream` gzip path, which pipes it through and re-wraps with
  `wasm_streams::ReadableStream::from_raw`.
- `wasm-streams` must equal the version `worker` links (0.6 here). Two versions in the tree
  each export `IntoUnderlyingByteSource` and wasm-bindgen refuses the build.
- `#[durable_object]` requires fields shaped `state: State, env: Env` plus ordinary fields;
  the macro generates `new(state, env)` and its ABI derives from the struct itself.
- DO SQL bind values cross the JS boundary as f64: `i64::MAX` does not round-trip
  (`commit_lo` sentinels and any other marker integers must stay under `2^53 - 1`).
- `gix_object` iterator constructors take the hash kind:
  `CommitRefIter::from_bytes(data, gix_hash::Kind::Sha1)` (same for `TreeRefIter`, `TagRef`).
- `gix_zlib::stream::deflate::Write` has no `finish()`: `write_all`, then
  `std::io::Write::flush`, then `into_inner()`.
- `gix_object::Data` carries `object_hash: gix_hash::Kind`; `Find::try_find` takes
  `id: &gix_hash::oid` and returns `Result<Option<Data>, gix_object::find::Error>`.
- `gix_pack::data::entry::Header::from_bytes(bytes, offset, gix_hash::Kind::Sha1)`; `Entry::
  from_bytes(bytes, offset, gix_hash::Kind::Sha1)`; `header::decode(&head)` returns
  `(Version, count)`.
- SQLite `UPDATE/DELETE ... LIMIT` is not compiled in: select keys, then mutate by key.
- `worker::UploadedPart` is not `Clone`; `MultipartUpload::complete(parts)` consumes;
  `abort(&self)` does not.

**A15. Verified behaviour, and what is still unverified (overrides nothing; records fact).**
Verified on local workerd with git 2.54: v2 `ls-refs` on empty and populated repos; v0/v1
receive-pack advertisement; initial push; incremental push of a thin pack containing
client-side deltas resolved against stored objects; 6 MiB binary blob round-trip through
R2 multipart; branch create/delete; tag push; delete-only push (no PACK body);
`git clone` (v2 fetch + streamed pack over sideband-64k, exact count and trailer);
incremental `git fetch` (have/want send-set); CAS `ng` on stale `old`; the A2 arm
(HTTP 200, `unpack <err>`, `ng <ref>`) on a malformed pack; gzip `Content-Encoding` via
`DecompressionStream`. Not yet exercised: GC end-to-end on live storage (the alarm chain is
wired; the mark/consolidate/sweep slices are written and compile but have not reclaimed a
real pack), `wrangler dev`'s local R2 differs from production R2 in MPU orphan semantics,
and subrequest ceilings below paid-plan values are unenforced by the simulator (ReqBudget is
the guard).

**A16. Alarms are absolute timestamps, and BLOBs need `serde_bytes` (extends A14).** Two bugs
found only by firing the alarm chain on live workerd:
- `Storage::set_alarm` interprets `i64`/`Duration` arguments as *offsets from now*, not
  timestamps. `jobs.run_at` is absolute epoch ms, so `rearm` must convert through
  `worker::ScheduledTime::new(js_sys::Date::new(...))`. Passing `run_at` as a Duration
  schedules the alarm ~56 years out — silent, and invisible until the first job never fired.
- DO SQLite `BLOB` columns deserialize as byte arrays, not sequences: any `Vec<u8>` field
  in a `to_array` DTO needs `#[serde(with = "serde_bytes")]`. `marked.bitmap` is the case.

**A17. Empty-repack skip (amends 5.2).** When every candidate pack's bitmap is all zeros
(nothing reachable), `begin_build` must not write an empty normalized pack; it clears the
build state (`gc_parts`, `gc.pos`, `gc.new_pack`, `gc.fails`) and enqueues `GcSweep`
directly. Verified live: a force-push orphaning a full pack collects it cleanly
(`objects 10 -> 3`, `packs_live 2 -> 1`) and a subsequent clone passes `fsck --strict`.

**A18. Test knobs (new).** `GE_GC_QUIET_MS` (default 600 000) and `GE_GC_GRACE_MS`
(default 3 600 000) env vars override the GC quiet/grace windows so the chain is exercisable
in `wrangler dev` and staging. Production deployments leave both unset. `GET
/:owner/:repo/_state` (write-token gated) returns row counts for refs/objects/packs/jobs —
the observability surface the GC tests use.

**A19. Audit-hardening round (amends 6.3, 10, 3).** The live pass review surfaced eight fixes:
- The edge must stream the DO's fetch response through (`Response::from_stream`); buffering
  it holds an entire clone's pack in edge memory.
- `upload-pack` bodies are read through `BodyReader` (gzip honoured) with a declared
  `Content-Length` rejected before buffering and a streamed cap at the 1 MiB command limit.
- One pushed pack is capped at 2 GiB compressed (`MAX_PENDING`); `pushes` holds at most 64
  `open` rows.
- Ref names must be full refnames under `refs/` (`gix_validate::reference::name`); `ok`/`ng`
  report lines echo names with non-graphic bytes replaced by `?` so a rejected name cannot
  inject text into the response stream.
- `Internal`/`Storage` error detail never reaches clients (`client_message` = "internal
  error"); the full error goes to the worker log.
- Token comparison is constant-time.
- `reflog(at)` is indexed for the janitor expiry scan.

**A20. Adversarial round 2 (amends 3.3, 4, 6.3, 7.4, 10, 11).** Five parallel audits
(security, concurrency, protocol, perf/scale, plus a manual pass) plus live client probes
produced these corrections, all verified against git 2.54:

- Receive header parsing is restartable, not resumable: a command section split across a
  64 KiB fill boundary is re-parsed from the accumulated buffer each round (bounded by the
  1 MiB cap). A flush-only receive-pack request is legal (an up-to-date push) and answers
  200 + one flush pkt — never a protocol error.
- The v2 fetch response emits `acknowledgments`/`ready` only when the client sent `have`
  lines; a clone (no haves) goes straight to `shallow-info`/`wanted-refs`/`packfile`, and
  git rejects the negotiation sections when no negotiation happened.
- `deepen-since`, `deepen-not`, `deepen-relative`, `want-ref`, `include-tag`, and
  `shallow`/`unshallow` response lines are implemented (sections 9.2/9.3). `deepen-not`
  carries ref *names* — resolved server-side to tips. The shallow boundary emitted is the
  sent commit adjacent to the cut, and excluded-side commits are never used as sparse-edge
  bases. Verified live: `--depth`, `--deepen`, `--shallow-since`, `--shallow-exclude`,
  `--unshallow`, `--filter=blob:none` with promisor checkout, all `fsck`-clean.
- A mid-stream fetch error is one band-3 `ERR` frame, then the stream ends; the failing
  step is never retried.
- `plan_reads` asserts the objects index still matches the in-memory bitmap: a `gc_sweep`
  or push-abort landing between mark and plan fails the fetch — or, on the walking path
  that recorded the marked SHAs, replans once against the live index — instead of
  writing a wire-corrupt pack.
- Ingest strictness: commits whose headers do not parse and tags that do not parse are
  `unpack` failures — a malformed object can never enter the index and poison every later
  read. Forward `REF_DELTA` bases (a delta naming a later entry) resolve in a bounded
  fixpoint pass, matching index-pack.
- Memory is byte-bounded, not object-bounded: delta chains cap at 64 MiB of compressed
  input (`MAX_CHAIN_BYTES`), `read_entries` refuses a batch over 48 MiB, `MemFind` growth
  is checked inside the decode loop, `MAX_ENTRIES` is 1M (~40 B/record), refs are capped at
  65 536, `ls-refs` takes at most 32 `ref-prefix` arguments.
- `SendSet::mark` is O(1) via a pack-id index, not O(#packs) per object.
- 7.4 commit-region prefetch is implemented: each walk level's coalesced read extends
  ±2 MiB (`PREFETCH`) around the needed span and every commit entry inside rides along;
  `commits_in_range` (objects(pack_id, offset) index) maps the region back to ids. A
  linear history costs one range read per ~4 MiB of commit bytes, not one per level —
  the ~9 000-commit depth ceiling is gone.
- The janitor propagates R2 delete failures (a marked-swept row whose delete failed would
  orphan bytes forever); `pushes(state)` and `packs(state)` are indexed; post-`begin`
  failures close the push row via `/_do/push/abort` rather than leaking `open` rows for
  the 1 h expiry.
- The fetch and error arms of the DO router rearm the job alarm; `gc` slices heartbeat
  `jobs.started_at` through checkpoint spans so `repair` never kills live work.
- Route segments reject `.`/`..`; the auth scheme match is case-insensitive (RFC 7235) and
  a missing `GE_READ_TOKEN` fails the read-token compare instead of erroring writes;
  client-derived strings in `ERR`/`unpack`/`ng` lines are sanitized (non-graphic -> `?`)
  and `x-ge-subrequests: <used+planned>/<max>` rides fetch responses (contract 406).

## Amendments from the production-hardening pass (8d9017a, verified on the live deployment)

- **A8. Streamed blob pass-through (overrides A7 for full blobs).** A `blob`
  entry over 16 MiB is no longer materialized: pass B copies its pending-pack
  wire bytes (varint header + zlib body) verbatim into the normalized pack via
  `PackWriter::raw_extend` in <= 8 MiB reads while a resumable zlib stream
  re-inflates solely to compute the object id. Isolate memory stays flat; the
  effective full-blob ceiling becomes the 2 GiB pending-pack bound. Delta
  results and non-blob objects keep the 16 MiB cap (A7). A delta naming a
  streamed blob as base still fails at the window guard, unchanged.
- **A9. Fetch fragmentation.** `coalesce` emits reads of at most WINDOW
  (8 MiB): entries larger than a window are split into fragments, and a merge
  may not extend a read past the window. Output order and the trailer hash are
  byte-identical; `pack_chunk` is unchanged.
- **A10. GC verbatim big-entry copy.** Consolidation streams entries over
  SPAN (8 MiB) fragment-by-fragment through `raw_extend` +
  `PackWriter::drain_parts` instead of `read_entries`. Parts drained mid-entry
  are recorded in `gc_parts` only inside the next checkpoint span, alongside the
  `WriterCkpt` that accounts for their bytes — replay stays byte-identical and
  no uploaded part exists without checkpoint state.
- **A11. Receive-pack drain-before-error.** Post-header failures drain the
  remainder of the request body (<= 2 GiB) before responding, so Cloudflare
  does not reset mid-upload and the client sees `unpack`/`ng` report-status.
- **A12. Per-repo tokens.** The DO holds a `tokens` table (id, sha1 hash,
  level, name, created_at). Edge auth resolves global write -> global read ->
  `/_do/auth` hash lookup, and fails closed on storage errors. Token values are
  returned once at creation; `/_admin/tokens` routes are global-write-token
  only — a repo token can never mint credentials.
- **A13. Request metrics.** When `GE_METRICS` is bound, each edge request
  emits one Analytics Engine datapoint (index=repo, blob=op,
  doubles=status/ms/subrequests). For streamed responses the duration is
  time-to-first-byte.

## Amendments from the audit-fix and real-repo benchmark passes (460f355, c8e6422)

- **A14. GC durable-byte checkpoint (overrides A10).** `drain_parts` no
  longer exists. `PackWriter::checkpoint` drains exactly `PART`-sized parts —
  a variable-size non-final part can never `complete()` under R2's
  equal-size rule (the audit-round-6 wedge). `PackWriter`'s hasher covers
  uploaded bytes only, so `WriterCkpt.pos`/`sha` describe the durable
  prefix exactly. The persisted `gc.pos` rewinds `(ci, idx, count)` to the
  object containing the last durable byte and may point mid-entry via
  `frag`; resume replays deterministically from that cursor, and a corrupt
  `gc.pos` fails loudly rather than stranding state.
- **A15. Chunked read batches (extends 7.2).** `read_entries` still refuses
  a batch whose coalesced spans exceed 48 MiB. Fetch loaders (`load`,
  `load_commits`) and both `gc.rs` call sites use
  `Bucket::read_entries_chunked`, which splits the locs set on `Limit` and
  reads the halves — a 10,000-tree expansion chunk or a multi-pack commit
  prefetch can no longer fail a clone with HTTP 413.
- **A16. HEAD adoption (extends section 3).** After a commit lands any
  command, if `meta.head` does not resolve to a `refs` row, it adopts the
  alphabetically-first existing `refs/heads/*`. `refs/heads/main` remains
  only the boot-time default; a `master`-first repo now clones with a
  working checkout.

## Amendments from the jobs-observability pass (feat/jobs-observability)

Continuing the audit-fix numbering (last: A16).

- **A17. Job-lifecycle metrics (extends A13).** When `GE_METRICS` is bound,
  `jobs::dispatch` emits one Analytics Engine datapoint per job event, in
  addition to the request datapoints. Positional layout, queried as
  `index1`/`blobN`/`doubleN`: `index1` = repo (`owner/repo`, from
  `ctx.id.name` so the label survives a `purge_repo` meta wipe); `blob1` =
  `"job"` (discriminator — request datapoints carry the op here, gauges
  `"gauge"`); `blob2` = kind (`janitor` | `gc_mark` | `gc_consolidate` |
  `gc_sweep` | `purge_repo`); `blob3` = event (`start` | `done` |
  `continue` | `reschedule` | `retry` | `dead` | `stale`); `blob4` =
  outcome (`ok` | `retry` | `dead` | `stale`); `blob5` = error class
  (`Error::class()` — the variant name, never the message, to stay
  cardinality-safe; `""` when none); `double1` = 1-based attempt;
  `double2` = slice wall-clock ms (0 on `start`); `double3` = 1.0 when the
  job will run again (`continue`/`reschedule`/`retry`), else 0.0. Writes
  are fire-and-forget: an unbound dataset or a failed write never fails a
  job.
- **A18. Dead-job alerting (extends A17).** A job reaching `dead` emits a
  `dead`-event datapoint (`blob3=dead`, `blob4=dead`, `blob5` the error
  class, `double1` the attempts consumed) — distinct and
  cardinality-safe, so an alert can key on `blob1='job' AND blob3='dead'`.
  Additionally, every alarm pass emits a `jobs_dead` gauge datapoint while
  dead rows exist (`blob1='gauge'`, `blob2='jobs_dead'`, `double1` =
  count). An operator wires the alarm either as a Workers Analytics/alert
  query over the dataset (`SELECT blob2, double1 WHERE blob1='gauge' AND
  blob2='jobs_dead' AND double1 > 0`) or as an external cron poller of
  `GET /:o/:r/_state` watching `jobs_dead`.
- **A19. Lease-fenced heartbeats (amends 4.4, hardens the A12/A20 60 s
  straggler window).** `heartbeat` is a CAS:
  `UPDATE jobs SET started_at=? WHERE id=? AND state='running' AND lease=?`.
  A slice whose row was `repair`-requeued and reclaimed under a new lease
  gets `false` and must return `stale_lease()` immediately — every write it
  still had queued belongs to the new lease-holder, and an unconditional
  `started_at` bump would mask a genuinely stalled new owner from `repair`.
  `repair` now clears `lease` on requeue. Dispatch reports a fenced outcome
  write that lands zero rows as event `stale` — never a retry, and
  `attempts` is not consumed by the loser. Residual risk: between
  heartbeats a stale slice can still issue R2 reads/writes — safe because
  `gc_mark` OR-merges bitmaps, `gc_consolidate` replays deterministically
  from `gc.pos`, janitor/purge deletes are idempotent, and `purge_repo`'s
  R2 prefix is the dead `repo_id`.
- **A20. `purge_repo` job kind (new).** `POST /:o/:r/_admin/delete`
  enqueues `purge_repo` (no payload). Slice A pages `list` over the
  `r/<repo_id>/` R2 prefix (cursor persisted in `jobs.cursor` as
  `r2:<cursor>`) and deletes each page via `delete_multiple`; slice B is a
  bounded span — CAS-heartbeat fence, `schema::migrate` (a crashed prior
  attempt may have dropped the schema), `DELETE FROM` every table but the
  job's own row, `DELETE FROM jobs WHERE id <> self`, `delete_alarm`, then
  `storage().delete_all()` and `unboot()` so the next request re-migrates.
  Idempotent and resumable at every await; a re-`POST` after completion
  enqueues a fresh job that wipes a fresh repo — a no-op. Known residual:
  a push committing between the row wipe and `delete_all` could leave a
  stray row in a re-created repo; the window is microseconds inside one
  slice. In-flight multipart uploads leave no listed objects and expire
  server-side (~7 d); R2 keys written after the listing cursor may be
  orphaned as unreferenced bytes under the dead `repo_id` prefix.
- **A21. `coalesce` loud-fail (amends 7.2/A9-fetch-fragmentation).** The
  fragment-length cast is `Error::Limit("read fragment exceeds window")`
  propagated through a `Result` — HTTP 413 — never a saturating
  `u32::MAX` and never a panic inside the DO.

## Amendments from the repo-admin pass (repo delete, public read, pinning, export)

- **A22. Repo delete (new).** `POST /:owner/:repo/_admin/delete` is
  global-write-token only. It sets `meta.deleted` and enqueues a `purge_repo`
  job (A20) in the same span; from that point every repo route — git protocol,
  `_state`, `_admin/*`, `/_do/*` — answers **410** via a new `Error::Gone`
  before any dispatch. `boot` on a tombstoned DO skips `jobs::repair` and
  instead guarantees exactly one `purge_repo` row exists (re-enqueueing a
  dead/missing one and requeueing a stranded `running` row on the same 60 s
  rule) — the purge is the only work that may still run, via the alarm. The
  tombstone lives until the purge's phase-B `delete_all` (A20); after that the
  name is free and the next request re-initializes a fresh repo under a new
  `repo_id`. Delete is idempotent at the API level; a repeated call on a
  tombstoned repo is itself 410.

- **A23. Public read (new).** `POST /:owner/:repo/_admin/public
  {enabled: bool}` (global-write-token only) sets or clears `meta.public`.
  On a public repo, `info/refs?service=git-upload-pack`, `POST
  git-upload-pack`, and `GET _admin/export` need no credential: a request with
  *no* usable `Authorization` credential on a read route falls through to a
  `/_do/public` probe instead of an immediate 401. A presented credential is
  still authenticated normally — an invalid token on a public repo is a 401,
  never a silent downgrade to anonymous. receive-pack, `_state`, and all other
  `_admin/*` routes are unchanged. `_state` reports `public` and `deleted`.

- **A24. Ref pinning (new).** `POST /:owner/:repo/_admin/pin {ref, sha}` and
  `/_admin/unpin {ref}` (global-write-token only) maintain a `pins` table.
  `ref` must be a full valid refname under `refs/` and must already resolve to
  `sha` — a pin asserts the current value, it never moves a ref (mismatch or
  missing ref → 409). A pinned ref rejects every update and delete in
  `commit_push` with `ng <ref> "ref is pinned"`, checked before the
  head-deletion and CAS rules. Pins are capped at 256 per repo and listed in
  `_state` under `pins`.

- **A25. Export (new).** `GET /:owner/:repo/_admin/export` (read-level auth —
  anonymous on a public repo) streams a v3 `git bundle`: the `# v3 git bundle`
  signature, no capability lines (sha1), no prerequisites, one `<sha> <ref>`
  line per live ref in name order, a `HEAD` line when `meta.head` resolves, a
  blank line, then a self-contained PACK built by the same
  `send_set`/`pack_chunk` machinery as fetch (wants = every ref tip, no haves,
  no shallow or filter modes; `include-tag` is unnecessary because every tag
  object reachable via a ref is already a want). The pack entries are verbatim
  copies with the normal trailer hash; the stream carries no pkt framing —
  a bundle is a file format, so a mid-stream failure is a truncated file.

## Amendments from the request-hardening pass (feat/request-hardening)

- **A26. Repo quotas (extends section 6's limit set).** Three env knobs with
  compiled-in defaults; `<= 0` disables that cap. `GE_QUOTA_MAX_REPOS_PER_OWNER`
  (default 50) is enforced at first push: `/_do/push/begin` reports `claimed` —
  true once the repo has any committed (`live` or swept-`dead`) pack — and the
  edge claims a slot in the `owner!<owner>` registry DO before ingest begins.
  Registry DOs are the same `RepoDo` class under a name no repo route can form
  (`!` fails `seg_ok`); `/_owner/*` routes skip `boot` entirely — schema migrate
  only, no meta rows, no jobs, no alarm. Claims are `claim:<repo>` keys in `meta`
  (idempotent `INSERT ... ON CONFLICT DO NOTHING`); over-cap inserts are handed
  straight back so rejected names never accumulate, and `/_owner/release` frees a
  slot for repo delete (ROADMAP #3). A repo that once committed keeps pushing if
  the cap is tightened later — it is grandfathered and never re-claims.
  `GE_QUOTA_MAX_OBJECTS` (default 2 000 000) and `GE_QUOTA_MAX_BYTES` (default
  4 GiB) are enforced in `commit_push` after ingest has posted the pack's real
  counts: live packs plus the push's own ingesting pack are summed; over either
  cap the whole push is finished `rejected` (so the janitor reaps the pack now)
  and `Error::Limit` naming the knob and the numbers rides back in `unpack`.
  A delete-only push (no pack) skips the check so an over-cap repo can shrink.
- **A27. Push rate limit (new).** `/_do/push/begin` runs a sliding-window check
  before the push row exists: two fixed 60 s buckets in the `rate` table,
  estimated as `cur + prev * (1 - frac_elapsed)`, keyed on `push:<sha1(token)>`.
  The attempt is counted before the check so an abusive credential stays over
  the line. Over `GE_RATE_PUSHES_PER_MIN` (default 30; `<= 0` disables) the DO
  returns `ratelimit` + `retry_after`, which surfaces as HTTP 429 with a
  `Retry-After` header — deliberately *without* the A2 drain, since shedding
  load is the point (a client still mid-upload may see a reset instead).
  Counters are per repo per credential. This is abuse damping, not metering:
  zone-level Cloudflare rate-limit rules are the heavy hammer.
- **A28. Advertisement memoization (extends 1.3).** The DO memoizes the refs
  snapshot — head, ref rows, and the rendered `/_do/refs` JSON body — keyed on
  `refs_version`, plus `/_do/ls-refs` response bytes per (refs_version, request
  body) in an 8-entry FIFO. `refs_version` only moves inside a commit span that
  changed refs, so a matching version is a byte-stable answer; an evicted DO
  rebuilds once. The subrequest still happens (the edge can only learn the
  version from the DO) but no refs scan, oid parse, or render runs on a hit.
  Auth and the protocol-version branch stay per-request at the edge; the DO
  stamps `x-ge-refs-version` on `/_do/refs` and the edge echoes it on
  `info/refs`. `_state` reports `refs_memo_hits`/`refs_memo_misses` and
  `rate_rows`.
- **A29. Verbatim consolidated-pack fast path (amends section 9 step 6).**
  When a fetch is plain-clone-shaped — no haves, no `shallow`, no
  `deepen`/`deepen-since`/`deepen-not`/`deepen-relative`, no `filter` — and the
  repo has exactly one live pack containing every resolved want, the DO
  streams that pack's R2 object verbatim as the `packfile` section instead of
  walking a send set and copying entries through `pack_chunk`. `packs_live=1`
  makes the send set a subset of the pack by construction (the index only
  resolves live packs), so the verbatim bytes are a wire-legal superset: the
  client index-packs extras and its connectivity check still passes. Cost is
  one R2 `get` — `x-ge-subrequests` reports `1` — where the walking path
  charges one subrequest per planned read plus the walk's own loads; the
  pack's own header and trailer ride inside the body bytes, so no trailer
  hash is computed. The request-shape gate is strict precisely because
  superset objects are legal: any shallow or filter argument carries a
  contract extra objects would violate. A want the index does not resolve
  into that one pack — including an unresolvable want the walking path would
  reject — falls back to `send_set`, which produces the proper response.
  Mid-stream failures degrade exactly like step 6's: one band-3 `ERR` frame,
  then the stream ends.
- **A30. `packfile-uris` offload (amends section 9 step 6, sits atop A29).**
  The v2 advertisement lists `packfile-uris` among `fetch`'s features; an
  opted-in client (git >= 2.40 with `fetch.uriprotocols` set) then sends one
  `packfile-uris <csv>` argument naming acceptable URI schemes. When the A29
  gate holds AND the client's list admits the request's own scheme AND
  `GE_URL_SIGNING_KEY` is configured at >= 32 bytes (a shorter value
  silently disables the feature; set it with `wrangler secret put`, not
  `[vars]`) AND the edge forwarded the public origin (`x-ge-base`), the
  DO answers with a `packfile-uris` section —
  `<trailer-sha1> <uri>` per the spec's `40-HEXDIGIT SP uri` line — followed
  by a `packfile` section containing a valid zero-object pack, which the
  client index-packs regardless. The URI is
  `GET /<owner>/<repo>/_packs/<id>.pack?e=<exp>&r=<repo_id>&s=<sig>`: the
  signature is HMAC-SHA256 over `v1\n<repo_id>\n<pack>\n<exp>` keyed by
  `GE_URL_SIGNING_KEY`, TTL one hour. URI fetches carry no auth headers by
  protocol design, so `s` is the credential — a bearer capability: anyone
  holding the URL reads that pack until `e`, independent of later token
  revocation. Minting is delegation: a read-token holder may hand the URL
  to a third party for the TTL, and (like A29) the pack is the verbatim
  superset, so objects unreachable from current refs ride along. Binding
  to the opaque `repo_id` (not the route name) means a deleted+recreated
  repo invalidates its old sigs. `e`, `r`, and `s` are validated before
  any R2 read; failures are 403 (bad/expired sig) or 404 (feature off,
  bad name, missing object). The route enforces time, not pack state — a
  sig minted just before consolidation can serve the dead pack until `e`
  or the janitor's R2 delete, whichever comes first. Responses carry
  `Cache-Control: private, no-store`; the URL is a credential. The pack
  body streams via `response_body` — one subrequest, no CPU burn on the
  edge. Any missing precondition falls through to the A29 verbatim
  stream, and so does a failed trailer read — degrading beats failing a
  fetch that could still be served. Hash tokens are the packs' real
  trailer SHA-1s (last 20 bytes — one range read): `git http-fetch
  --packfile` dies on a checksum mismatch, so a corrupt or stale pack
  surfaces loudly rather than silently. Rotating `GE_URL_SIGNING_KEY`
  invalidates outstanding URLs instantly — expected, but worth knowing:
  a clone mid-URI-fetch dies and retries cleanly. Clients that never opt
  in see no behavioral change; the feature degrades to A29.
- **A31. Server-side resumable import (`import_pack` job; I1).** For packs
  no client push can slice — the TypeScript-class case is one commit
  introducing ~222k objects — the owner stages pack parts into R2 and the DO
  ingests them across alarm slices, ending in the same `commit_push` a live
  push gets (atomic semantics, per-ref results, janitor lifecycle).
  - `POST /<o>/<r>/_admin/import/stage` (write token) streams one part to
    `r/<repo_id>/pending/<push>.pack` via a RawWriter MPU and answers
    `{push, key, bytes}`; the call opens the `pushes` row. Later parts of
    the same import pass `?push=<id>&part=<name>` — push begin is
    idempotent for an `open` push under the same principal (a closed push
    is 409, a foreign one 403) and the part lands at
    `pending/<push>.part-<name>`. Sharing one push is required: the import
    job heartbeats only its own `began_at`, so parts parked on separate
    stage pushes would expire at PUSH_TIMEOUT and be swept mid-import.
  - `POST /<o>/<r>/_admin/import` (write token) takes
    `{push, parts: [{key,bytes}], commands: [{old,new,name}]}`; every part
    key must sit under that repo's `pending/` namespace (the bucket is
    shared — an unchecked key would read across tenancy) and commands must
    parse — both fail 400 at enqueue, not mid-job. A start on an unknown or
    non-`open` push is 409. A start while a queued/running `import_pack`
    already owns the push is idempotent: it returns the existing job's
    pack id instead of planting a second `ingesting` row.
  - The payload's `principal` comes from the `pushes` row, not the request
    body — a replayed start can't rewrite who the reflog credits.
  - **Pass A** parses entry headers + zlib boundaries off the staged parts
    into `import_toc` (one row per entry: input offset, header length,
    kind/delta aux, compressed length, size). The cursor is `(parse_pos,
    next_idx)` — always an entry boundary; inserts are `INSERT OR REPLACE`
    so a crashed slice replays cleanly. The staged pack's trailer is read
    but not re-hashed: per-entry adler32 already vetted the bytes and a
    second pass costs GiBs of reads (same trade as the edge path, which
    does hash — the delta is documented).
  - **Pass B** resolves each TOC entry against the staged bytes and
    appends the normalized (non-delta) object to an output `PackWriter`
    MPU. In-pack OFS bases resolve by offset; REF bases resolve through
    `objects` rows plus the current slice's in-memory ids; a REF base not
    yet emitted parks the entry until its base id resolves (`run_wakes`
    drains transitively). External (thin-pack) bases resolve through the
    live index exactly like 2.4.
  - **Durable state** = uploaded R2 parts (`import_parts` etags) plus the
    `WriterCkpt` in the job cursor; `objects`/`push_links`/`import_open`
    rows. `import_open` marks an entry's output start at first append and
    seals `(end, sha)` when its `ObjRow` is cut — an entry counts done iff
    `end <= durable` or its objects row exists (dup-sha dedup keeps no
    second row). Every slice re-derives: delete objects rows past the
    durable boundary, restore fully-durable lost rows straight from shadow
    cols (the writer can't write out of order, so no byte replay), and
    frag-resume the one straddling entry at `skip = durable - off`. A
    boundary entry can never legitimately `Await` — its base necessarily
    emitted before its own bytes — so a deferral there is `Internal`, not
    a park.
  - Objects rows may run ahead of the durable prefix inside a slice; that
    is safe because only the resume-time delete treats them as lost — the
    checkpoint itself never deletes live buffered state.
  - The `check` phase pages `push_links` (1k/page) and accepts each sha
    that resolves in a live pack or the importing pack's own rows — the
    same 2.5 contract as `push_index`, minus the 1M in-memory cap (job
    links cap at 8M staged rows).
  - The `commit` slice drains whatever the resolve tail left undurable
    (the <PART buffer no mid-run checkpoint can upload), finishes the MPU,
    posts real pack meta over the `ingesting` placeholder, stamps the
    check phase's pinned `gc_epoch` onto the pushes row, and calls
    `commit_push`. The epoch is re-compared before the finish and again
    inside the commit span — a moved epoch (a sweep ran) resets the job
    to re-check rather than commit stale link verdicts, and a post-finish
    reset re-commits the already-durable object through dead-upload
    recovery. The pushes row records the outcome (committed, or rejected
    + per-ref reasons); cleanup of `push_links` / `import_toc` /
    `import_parts` / `import_open` / staged parts runs on acceptance and
    rejection alike.
  - Each slice heartbeats `pushes.began_at`; the janitor's orphan reaper
    keys on `packs.push_id` (NULL `pushes.pack_id` would flag an active
    import's pack abandoned). A dead job's MPU is aborted and its staging
    swept in bounded janitor batches.
  - `GET /<o>/<r>/_admin/import/<push>` reports `{push_state, job_state,
    job_phase, attempts, last_error, objects_done, objects_total, result}`.
- **A32. GC yield-persisted tail (`gc_tail`; amends A14/5.2).** A mid-slice
  checkpoint still rewinds `(ci, idx, frag)` to the durable boundary, but a
  *yield* must not depend on having drained a part: a pack whose marked
  bitmap is sparse (~1 R2 read per entry) buffers under 8 MiB inside the
  request budget, so a rewind-only cursor replays the same span every slice
  forever. On `Flow::Yield` the undrained `<PART` buffer is chunked into
  `gc_tail` (64 KiB rows) and `gc.pos` keeps the scan cursor plus a separate
  durable-boundary triple `(bci, bidx, bfrag)`; `pos.tail` is the tail's
  byte length. Resume loads the rows back into the writer wholesale —
  `WriterCkpt` still describes only uploaded bytes, `pos.ord0` re-counts
  the tail's completed entries from `objects` rows so `count`/commit spans
  and ordinals continue exactly. A tail that reads back short or absent
  while `pos.tail > 0` is corrupt — the scan rewinds to `(bci, bidx,
  bfrag)` and replays deterministically; rows the replay re-appends are
  deduped by `insert_objects`' ON CONFLICT. `gc_parts` is pruned to
  `st.pos / PART` on every persist so etags from a slice that died before
  its checkpoint can't rebind to wrong part numbers. All GC wipe paths —
  mark-cycle start, sweep reschedule, begin_build early-outs, rebuild,
  finish — delete `gc_tail` with the rest.
  The import resolver has the same hazard with the same fix
  (`import_tail(push_id, seq, blob)`, cursor `scan_after` + `tail`):
  `scan_after` keeps the TOC queue scan monotonic so resume never re-walks
  the completed prefix, `tail` is the persisted `<PART` buffer length.
  Ordering differs from GC because the import can re-run a boundary entry
  mid-body: the `lost`-writers pass runs at `out.offset()==durable` first;
  if any entry needs the straddler re-run (`end IS NULL` past the boundary),
  the tail is discarded rather than restored — its bytes belong to offsets
  the re-run would overwrite. Otherwise the tail loads back wholesale,
  `import_open` rows inside `[durable, durable+tail)` supply the completed
  count and commit span, and the requeue bound becomes `durable+tail_len`.
  `import_tail` dies with the push on abort, rebuild, terminal cleanup, and
  janitor sweep.

- **A33. Git LFS basic transfer (`/_do/lfs/batch`, `/_lfs/<oid>`).** The
  standard LFS batch endpoint
  `POST /<owner>/<repo>/info/lfs/objects/batch` answers per the spec:
  `{transfer:"basic", hash_algo:"sha256", objects:[…]}`. The edge
  authenticates at the level the `operation` implies (`download` → read or
  `public`, `upload` → write) then forwards the body to the DO with
  `x-ge-base`; the DO does an R2 `head` per object under
  `r/<repo_id>/lfs/<oid>` and mints action hrefs
  `GET|PUT /<owner>/<repo>/_lfs/<oid>?e=<exp>&r=<repo_id>&s=<sig>` signed
  with the A30 key under a `lfs`-domain-separated message that binds
  repo_id + oid + expiry + HTTP op — a download sig cannot upload. Objects
  already present get no upload action; missing downloads get a per-object
  `{code:404}`. Upload batches are quota-checked as `packs + lfs + declared
  new bytes ≤ GE_QUOTA_MAX_BYTES`. LFS keys sit under the repo's `r/<id>/`
  prefix so delete/purge takes them too. No `verify` callback, locking
  API, or custom transfer adapters; a deployment without
  `GE_URL_SIGNING_KEY` answers 403 on every batch.

- **A34. Import dead-upload recovery (amends I1).** The output MPU can
  die independently of the job cursor — aborted by a failing `finish`,
  reaped server-side, or lost with the isolate — and
  `resume_multipart_upload` is lazy, so a dead upload only surfaces at
  the first `upload_part`/`complete` touching it, anywhere inside a
  resolve or commit slice. `run_slice` catches that error shape
  (`multipart upload … not exist` / `NoSuchUpload`) and probes the pack
  key: if the object exists, the dead slice's `complete()` won and
  commit runs straight from the posted `import_open` spans; otherwise
  `import_parts`/`import_open`/`import_tail`/`objects` are wiped,
  `scan_after` resets to -1, and resolve re-emits the pack onto a fresh
  MPU — deterministic bytes make the rebuild safe, just slow. The
  import finishes through `finish_resumable`, which leaves the MPU
  alive on a mid-finish failure (transient part upload, a check fixed
  in a later build) so the retry re-finishes from the checkpoint;
  non-resumable callers keep the aborting `finish` so a failed inline
  push can't orphan an upload. A pushes row that is no longer `open`
  ends the job `Done` rather than erroring — a crash between
  `commit_push` and the fenced job-row delete must not dead-letter an
  import that already committed.
