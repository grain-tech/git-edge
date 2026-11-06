# Speak git protocol v2 only, translate v0 at the edge

> Second pass · Idea #3 · verdict: **lands with caveats** · feasibility 4/5 · reliability 4/5 · correctness 4/5 (first pass 4/3/2)
> First pass: [proof](./archived/protocol-v2-only.md) · [review](../reviews/archived/protocol-v2-only.md) · Second pass: [review](../reviews/protocol-v2-only.md) · Contract: [CONTRACTS.md](../../CONTRACTS.md)


## Mechanism
This idea is subsumed by CONTRACTS.md: it is the `wire` module (section 1.1) plus the version-selection rules 6, 7 and 8 that `edge` enforces, and the "translate v0 at the edge" half is replaced by a decision, not a shim. Upload-pack is v2 only: `GET info/refs` with `Git-Protocol: version=2` gets the static v2 advertisement from the edge without a DO call, a v2 `POST git-upload-pack` is parsed by `wire::parse_v2_command` at the edge (after `BodyReader` removed gzip, section 6) and forwarded raw to `RepoDo` routes `/_do/ls-refs` (sync, `refs` and `meta` tables) or `/_do/fetch` (section 9), while a v0 or v1 `GET info/refs` gets the v0 ref listing so `git ls-remote` works and a v0 `POST git-upload-pack` gets HTTP 400 with one `ERR protocol v2 required (git >= 2.26)` pkt-line (scenario 13). Receive-pack has no v2 in git, so push stays a v0 POST parsed by `wire::parse_receive_header` and answered by `wire::write_report_status`; those two functions are proven in two-phase-push and repo-do-ref-authority. The proof below is the concrete `wire` module (pkt-line reader and writer over `gix-packetline`, sideband, v2 command parser, ls-refs and advertisement writers, the v2 fetch prelude that omits `acknowledgments` after `done`) and the `edge` functions that pick the version.

## Primitives
- `gix-packetline` 0.22.2 with `blocking-io`: `decode::streaming(&[u8]) -> Stream::{Complete{line, bytes_consumed}, Incomplete{..}}`, `PacketLineRef::{Data, Flush, Delimiter, ResponseEnd}`, `blocking_io::encode::{data_to_write, flush_to_write, delim_to_write, error_to_write}` (path per CONTRACTS correction 1): **measured**. The spike served the v2 advertisement, `ls-refs` with `peel`/`symrefs`/`unborn`/`ref-prefix`, and the v0 advertisement to real git 2.43; `git ls-remote` succeeded under `protocol.version` 2 and 0 (`research/rust-spike.md` transcript). `blocking-io` on wasm32 is outside gitoxide's CI matrix but built and ran.
- `gix_packetline::encode::band_to_write(Channel, &[u8], impl Write)` and `Channel::{Data, Progress, Error}`: listed in the memo (section 3), **not exercised by the spike**; argument order per docs.rs, unverified at runtime.
- `gix_hash::ObjectId::{from_hex, Display}` 0.26.2: verified (spike).
- `worker::Request::headers().get("Git-Protocol")`, `Stub::fetch_with_request`, `Response::from_bytes(..).with_headers(..)`: verified (spike). `Response::from_stream` for the fetch body: memo-supported, **unverified at runtime**; it belongs to `fetch_v2`, a dependency.
- `BodyReader` (`web_sys::DecompressionStream("gzip")` + `wasm-streams`): section 6.1, **unverified**; used here only through its contract signature.
- git wire facts used: `Git-Protocol` is a colon-separated list (`version=2:key=val`); a v2 `fetch` response omits `acknowledgments` when the request carried `done` (gitprotocol-v2, confirmed by the first-pass review's interop walk-through); a v0 client whose first advertisement pkt is not a `version` line falls back to v0 (`determine_protocol_version_server`). Checked against git 2.43 source, not yet by scenario run.
- Measured in the spike `dev.log`: a `POST git-upload-pack` answered 400 without reading its body made workerd log `Can't read from request stream after response has been sent` and restart. The edge therefore drains a rejected v0 POST body (bounded) before answering. **Local only**, production unverified.
- Measured (`research/rust-spike.md`, CONTRACTS correction 5): the spike Worker with gix-hash, gix-object, gix-packetline, gix-pack, gix-traverse and gix-zlib is 605 KB after `wasm-opt`, 262 KB gzipped; cold instantiate 20-30 ms on local workerd; warm `ls-refs` POST 6-8 ms. Edge cold start unmeasured (no deploy).
- CONTRACTS correction 4: a `worker::Error` escaping `fetch` is an uncaught exception and HTTP 500. Every `Error::Protocol` here is turned into a 400 with one `ERR` pkt-line by `edge::respond` (section 10) before it can escape.

## Proof code
```rust
// src/wire/mod.rs -- CONTRACTS.md 1.1. No awaits, no `worker` import. gix-packetline 0.22.2 (blocking-io), gix-hash 0.26.2.
// Pkt, Filter, LsRefsArgs, FetchArgs, V2Command, RefRow, the clippy deny list: exactly as in CONTRACTS 1.1 and 10, not repeated.
use bstr::{BStr, BString, ByteSlice};
use gix_hash::ObjectId;
use gix_packetline::{blocking_io::encode, decode, Channel, PacketLineRef};   // blocking_io path: CONTRACTS correction 1
use crate::error::Error;
pub const MAX_PKT_DATA: usize = 65516;
pub const MAX_BAND_DATA: usize = 65515;
const AGENT: &str = "agent=git-edge/0.1";
#[derive(Default)] pub struct PktReader { buf: Vec<u8>, pos: usize }   // push()/remainder(): the one-line Vec operations of 1.1
impl PktReader {
    /// Ok(None) = need more bytes. `0003`, a length past the buffer or over 65520 is a decode error -> Error::Protocol (rule 1).
    pub fn next(&mut self) -> Result<Option<Pkt<'_>>, Error> {
        let rest = self.buf.get(self.pos..).unwrap_or(&[]);
        let (line, used) = match decode::streaming(rest).map_err(|e| Error::Protocol(format!("pkt-line: {e}")))? {
            decode::Stream::Incomplete { .. } => return Ok(None),
            decode::Stream::Complete { line, bytes_consumed } => (line, bytes_consumed),
        };
        self.pos = self.pos.saturating_add(used);
        Ok(Some(match line { PacketLineRef::Data(d) => Pkt::Data(d), PacketLineRef::Flush => Pkt::Flush,
                             PacketLineRef::Delimiter => Pkt::Delim, PacketLineRef::ResponseEnd => Pkt::ResponseEnd }))
    }
}
#[derive(Default)] pub struct PktWriter { pub out: Vec<u8> }
impl PktWriter {
    pub fn data(&mut self, b: &[u8]) -> Result<(), Error> {
        if b.len() > MAX_PKT_DATA { return Err(Error::Internal("pkt-line over 65516 bytes".into())); }
        encode::data_to_write(b, &mut self.out).map(|_| ()).map_err(|e| Error::Internal(e.to_string()))
    }
    pub fn text(&mut self, s: &str) -> Result<(), Error> { if s.ends_with('\n') { self.data(s.as_bytes()) } else { self.data(format!("{s}\n").as_bytes()) } }
    pub fn flush(&mut self) { let _ = encode::flush_to_write(&mut self.out); }   // writes into a Vec cannot fail
    pub fn delim(&mut self) { let _ = encode::delim_to_write(&mut self.out); }
}
/// Writers keep the `()` signatures of 1.1. A line over MAX_PKT_DATA is unreachable (ref names <= 4096, Known limits); if it happens the client sees a pkt-line ERR.
fn line(w: &mut PktWriter, b: &[u8]) { if w.data(b).is_err() { let _ = encode::error_to_write(b"line too long", &mut w.out); } }
pub struct Sideband<'w> { w: &'w mut PktWriter }
impl<'w> Sideband<'w> {   // band_to_write(Channel, &[u8], impl Write): memo-listed, not run in the spike
    /// One copy per frame straight into `out`, no intermediate buffers (first-pass caveat 1).
    pub fn data(&mut self, b: &[u8]) { for c in b.chunks(MAX_BAND_DATA) { let _ = encode::band_to_write(Channel::Data, c, &mut self.w.out); } }
    pub fn error(&mut self, s: &str) { let _ = encode::band_to_write(Channel::Error, s.as_bytes().chunks(MAX_BAND_DATA).next().unwrap_or(b""), &mut self.w.out); }   // progress(): same, Channel::Progress
}
fn oid(hex: &[u8]) -> Result<ObjectId, Error> { ObjectId::from_hex(hex).map_err(|_| Error::Protocol(format!("bad oid {}", hex.as_bstr()))) }
fn bad(what: &str, x: &[u8]) -> Error { Error::Protocol(format!("{what}: unknown argument {}", x.as_bstr())) }
fn num(b: &[u8]) -> Option<u64> { b.to_str().ok()?.parse().ok() }
/// `command=...`, capability lines, delim, arguments, flush. Unknown command or argument -> Error::Protocol (1.1).
pub fn parse_v2_command(body: &[u8]) -> Result<V2Command, Error> {
    let mut r = PktReader { buf: body.to_vec(), pos: 0 };
    let (mut cmd, mut args, mut in_args) = (None::<Vec<u8>>, Vec::<BString>::new(), false);
    loop {
        match r.next()? {
            None | Some(Pkt::ResponseEnd) => return Err(Error::Protocol("truncated v2 command".into())),
            Some(Pkt::Flush) => break,
            Some(Pkt::Delim) => in_args = true,
            Some(Pkt::Data(d)) => {
                let d = d.strip_suffix(b"\n").unwrap_or(d);
                if in_args { args.push(d.into()); }
                else if let Some(c) = d.strip_prefix(b"command=") { cmd = Some(c.to_vec()); }
                else if d == b"object-format=sha256" { return Err(Error::Protocol("object-format sha256 unsupported".into())); }
            }   // other capability lines (agent=, object-format=sha1) are accepted and ignored
        }
    }
    match cmd.as_deref() {
        Some(b"ls-refs") => Ok(V2Command::LsRefs(parse_ls_refs(&args)?)),
        Some(b"fetch") => Ok(V2Command::Fetch(parse_fetch(&args)?)),
        _ => Err(Error::Protocol("unknown command".into())),   // object-info: not advertised (rule 6), not accepted
    }
}
fn parse_ls_refs(args: &[BString]) -> Result<LsRefsArgs, Error> {   // git 2.43 sends `peel`, `symrefs`, `unborn` (spike transcript)
    let mut a = LsRefsArgs { symrefs: false, peel: false, unborn: false, prefixes: Vec::new() };
    for l in args { match l.as_slice() {
        b"symrefs" => a.symrefs = true, b"peel" => a.peel = true, b"unborn" => a.unborn = true,
        x => a.prefixes.push(x.strip_prefix(b"ref-prefix ").ok_or_else(|| bad("ls-refs", x))?.into()),
    } }
    Ok(a)
}
fn parse_fetch(args: &[BString]) -> Result<FetchArgs, Error> {
    let mut f = FetchArgs { wants: vec![], haves: vec![], done: false, thin_pack: false, no_progress: false, include_tag: false,
                            ofs_delta: false, deepen: None, shallow: vec![], filter: None };
    for l in args {
        let (k, v) = l.split_once_str(" ").unwrap_or((l.as_slice(), b""));
        match k {
            b"want" => f.wants.push(oid(v)?), b"have" => f.haves.push(oid(v)?), b"shallow" => f.shallow.push(oid(v)?),
            b"done" => f.done = true, b"thin-pack" => f.thin_pack = true, b"no-progress" => f.no_progress = true,
            b"include-tag" => f.include_tag = true, b"ofs-delta" => f.ofs_delta = true,
            b"deepen" => f.deepen = Some(num(v).and_then(|n| u32::try_from(n).ok()).filter(|n| *n > 0).ok_or_else(|| bad("deepen", v))?),
            b"filter" if v == b"blob:none" => f.filter = Some(Filter::BlobNone),
            b"filter" => f.filter = Some(Filter::BlobLimit(v.strip_prefix(b"blob:limit=").and_then(num).ok_or_else(|| bad("filter", v))?)),
            // deepen-since/-not/-relative, want-ref, sideband-all, packfile-uris, wait-for-done: not advertised (rule 6, section 12)
            _ => return Err(bad("fetch", k)),
        }
    }
    if f.wants.is_empty() { Err(Error::Protocol("fetch: no want lines".into())) } else { Ok(f) }
}
pub enum Service { UploadPack { v1: bool }, ReceivePack }
pub fn write_capability_advertisement_v2(w: &mut PktWriter) {   // rule 6, byte-exact and static: the edge writes it without a DO call
    for l in ["version 2", AGENT, "ls-refs=unborn", "fetch=shallow filter", "object-format=sha1"] { line(w, format!("{l}\n").as_bytes()); }
    w.flush();
}
pub fn write_ls_refs(w: &mut PktWriter, a: &LsRefsArgs, head: Option<&BStr>, refs: &[RefRow]) {   // HEAD first (meta.head, symbolic, section 3), then refs, flush
    let wanted = |n: &[u8]| a.prefixes.is_empty() || a.prefixes.iter().any(|p| n.starts_with(p));
    let peel = |r: &RefRow| if a.peel { r.peeled.map(|p| format!(" peeled:{p}")).unwrap_or_default() } else { String::new() };
    if let Some(h) = head.filter(|_| wanted(b"HEAD")) {
        match refs.iter().find(|r| r.name.as_bstr() == h) {
            Some(r) => line(w, format!("{} HEAD{}{}\n", r.target, if a.symrefs { format!(" symref-target:{h}") } else { String::new() }, peel(r)).as_bytes()),
            None => if a.unborn { line(w, format!("unborn HEAD symref-target:{h}\n").as_bytes()) },   // scenario 1
        }
    }
    for r in refs.iter().filter(|r| wanted(&r.name)) { line(w, format!("{} {}{}\n", r.target, r.name, peel(r)).as_bytes()); }
    w.flush();
}
/// Rules 5, 7, 8: `# service=`, flush, [`version 1`], first ref with \0caps, the rest, `<peeled> <name>^{}` after annotated tags, flush.
pub fn write_advertisement_v0(w: &mut PktWriter, service: Service, head: Option<&BStr>, refs: &[RefRow]) {
    let (name, caps) = match service {
        Service::UploadPack { .. } => ("git-upload-pack", format!("object-format=sha1 {AGENT}")),
        Service::ReceivePack => ("git-receive-pack", format!("report-status report-status-v2 delete-refs side-band-64k quiet ofs-delta object-format=sha1 {AGENT}")),
    };
    line(w, format!("# service={name}\n").as_bytes()); w.flush();
    if matches!(service, Service::UploadPack { v1: true }) { line(w, b"version 1\n"); }
    let (mut first, head_row) = (true, head.and_then(|h| refs.iter().find(|r| r.name.as_bstr() == h)).map(|r| (r.target, BStr::new("HEAD"), None)));
    for (id, n, peeled) in head_row.into_iter().chain(refs.iter().map(|r| (r.target, r.name.as_bstr(), r.peeled))) {
        if first { line(w, format!("{id} {n}\0{caps}\n").as_bytes()); first = false; } else { line(w, format!("{id} {n}\n").as_bytes()); }
        if let Some(p) = peeled { line(w, format!("{p} {n}^{{}}\n").as_bytes()); }
    }
    if first { line(w, format!("{} capabilities^{{}}\0{caps}\n", ObjectId::null(gix_hash::Kind::Sha1)).as_bytes()); }   // empty repo
    w.flush();
}
/// Rule 3 and section 9 step 6. Ok(false): the response ends here (no pack). Otherwise the caller streams Sideband::data frames, then flush.
pub fn write_fetch_prelude(w: &mut PktWriter, args: &FetchArgs, acks: &[ObjectId], shallow: &[ObjectId]) -> Result<bool, Error> {
    let ready = args.done || args.haves.is_empty() || !acks.is_empty();      // section 9 step 2
    if !args.done {                                                          // first-pass blocker 1: no section at all after `done`
        w.text("acknowledgments")?;
        if acks.is_empty() { w.text("NAK")?; } else { for a in acks { w.text(&format!("ACK {a}"))?; } }
        if !ready { w.flush(); return Ok(false); }                           // stateless-RPC round: client posts more haves or done
        w.text("ready")?; w.delim();
    }
    if !shallow.is_empty() { w.text("shallow-info")?; for s in shallow { w.text(&format!("shallow {s}"))?; } w.delim(); }
    w.text("packfile")?;
    Ok(true)
}
// src/edge/upload.rs -- rules 6-8, section 6.3. Error -> HTTP status is edge::respond (section 10), never a raw worker::Error (correction 4).
// info/refs glue (omitted): v2 upload-pack -> write_capability_advertisement_v2, no DO call; else GET /_do/refs then write_advertisement_v0.
pub enum Proto { V0, V1, V2 }
pub fn proto_of(req: &worker::Request) -> Proto {                            // `version=2[:k=v]`, colon-separated as git sends it
    match req.headers().get("Git-Protocol").ok().flatten() {
        Some(h) if h.split(':').any(|p| p.trim() == "version=2") => Proto::V2,
        Some(h) if h.split(':').any(|p| p.trim() == "version=1") => Proto::V1,
        _ => Proto::V0,
    }
}
pub async fn upload_pack(req: worker::Request, stub: &worker::Stub, repo: &RepoRoute) -> Result<worker::Response, Error> {
    let mut body = BodyReader::new(&req)?;                                   // gzip removed here (section 6.1, binding unverified)
    if !matches!(proto_of(&req), Proto::V2) {
        body.fill(1 << 20).await?;                                           // drain first: workerd restarted on an unread body (spike dev.log)
        return Err(Error::Protocol("protocol v2 required (git >= 2.26)".into()));   // 400 + one `ERR ...` pkt-line (rule 7, section 10)
    }
    if body.fill((1 << 20) + 1).await? { return Err(Error::Protocol("command section over 1 MiB".into())); }   // section 6.3
    let path = match parse_v2_command(body.buffered())? { V2Command::LsRefs(_) => "/_do/ls-refs", V2Command::Fetch(_) => "/_do/fetch" };
    let fwd = internal_request(repo, path, body.buffered().to_vec())?;       // x-ge-owner / x-ge-repo headers (section 8.1)
    stub.fetch_with_request(fwd).await.map_err(|e| Error::Storage(e.to_string()))   // /_do/fetch streams; /_do/ls-refs is one sync span
}
```

## Why it works
- Version selection is the header and nothing else. git sends `Git-Protocol: version=2` on `info/refs` and on every POST when `protocol.version` is 2, the default since 2.26; `proto_of` reads it as the colon-separated list git writes. A v2 client that sees `version 2` in the advertisement stays in command mode, so the v2 advertisement of rule 6 is static and the edge writes it with no DO call (section 1.1 rule 6, `info_refs`).
- A fresh clone no longer dies. When the client sends `done` in its first `fetch` (no local refs), `fetch-pack` moves to `FETCH_GET_PACK` and requires the next section to be `packfile` or `shallow-info`; `write_fetch_prelude` writes the `acknowledgments` section only when `args.done` is false, exactly rule 3 and section 9 step 6.
- Incremental fetch is the stateless-RPC round trip git expects: haves without `done` and no common commit get `acknowledgments`, `NAK`, flush and the response ends (`Ok(false)`); the client posts again with more haves or `done`. Known haves produce `ACK <oid>` lines, `ready`, delim, `packfile` (section 9 steps 1 and 2, scenario 12).
- No v0 upload-pack shim exists to get wrong. Rule 7 makes a v0 `POST git-upload-pack` a 400 with one `ERR` pkt-line, which `remote-curl` prints as `fatal: remote error: protocol v2 required (git >= 2.26)` instead of hanging (scenario 13, section 10 mapping). The v0 `GET info/refs` still lists refs with `object-format=sha1 agent=` only, so `git ls-remote -c protocol.version=0` works and no capability that the server does not honour is advertised. `version=1` gets the same listing behind a `version 1` pkt (rule 8), which `determine_protocol_version_server` accepts.
- Wants are validated before any byte is sent: section 9 step 1 runs `Index::lookup` on every want inside `RepoDo::fetch_v2` and answers `ERR upload-pack: not our ref <oid>` for a miss; the parser here refuses a `fetch` with no `want` at all (`parse_fetch`). Unknown haves are dropped, as git does.
- Every advertised capability is honoured. `ls-refs=unborn` is served by `write_ls_refs` (`symrefs`, `peel`, `unborn`, `ref-prefix`). `fetch=shallow filter` maps to `deepen`, `shallow`, `filter blob:none`, `filter blob:limit=<n>` in `parse_fetch`, consumed by section 9 step 5. `wait-for-done`, `object-info`, `server-option`, `sideband-all`, `packfile-uris`, `ref-in-want` are neither advertised nor accepted (`parse_v2_command` and `parse_fetch` return `Error::Protocol`), matching section 12.
- HEAD is symbolic only. `write_ls_refs` resolves `HEAD` through `meta.head` (section 3) and emits `symref-target:` when the client asked for `symrefs`, so `git clone` picks the default branch by name, not by oid match; an empty repo yields `unborn HEAD symref-target:refs/heads/main` (scenario 1).
- Framing is byte copying with no per-frame allocation: `Sideband::data` slices the input into 65515-byte chunks and `band_to_write` appends each once to `out` (rules 1 and 2); progress and error frames are single, truncated frames. Length prefixes, `0000`/`0001`/`0002`, and the `0003` rejection come from `gix-packetline`, the one shared codec the cross-cutting defect 1 fix asks for.
- Packs cannot skew between the DO answer and the R2 read: `SendSet` is a bitmap per `pack_id`, R2 keys are `r/<repo_id>/packs/<pack_id>.pack` and a pack is immutable once its row is `live` (section 2.2); GC writes a new `pack_id` and the old one stays readable until GRACE after `dead_at` (section 5). There is no `latest.pack` to overwrite.
- gzip and chunked bodies are handled before parsing: `BodyReader::new` pipes a `Content-Encoding: gzip` body through `DecompressionStream` and never trusts `Content-Length` (section 6.1 and 6.2), so `parse_v2_command` sees plain pkt-lines. A v2 command section is capped at 1 MiB (section 6.3).
- Push is untouched by this module's version logic: receive-pack advertises rule 5's exact list, `report-status(-v2)` goes out in band 1 under `side-band-64k` (rule 4), and the DO never parses pkt-lines for push (section 1.3 routes take JSON).

## Changes from the first pass
| First-pass item | Kind | How addressed |
|---|---|---|
| "v2 fetch response always emits an acknowledgments/ready section ... every fresh git clone dies with 'expected packfile, received acknowledgments'" | blocker | `write_fetch_prelude`: the section is written only under `if !args.done`; after `done` the response starts at `shallow-info` or `packfile` (rule 3, section 9 step 6). |
| "v0 shim omits multi_ack_detailed and replies NAK + pack to have-only rounds ... the shim is clone-only" | blocker | Not built as a shim, because the contract removed it: rule 7 rejects a v0 `POST git-upload-pack` with 400 and one `ERR protocol v2 required (git >= 2.26)` pkt-line (`upload_pack`, scenario 13), and section 12 lists v0/v1 negotiation (`multi_ack_detailed`, `no-done`) as out of scope. v0 clients keep `ls-remote` through `write_advertisement_v0`. |
| "git gzips upload-pack POST bodies (rpc.gzip_request); the Worker parses the raw body without honouring Content-Encoding: gzip" | blocker | `upload_pack` builds `BodyReader::new(&req)` before parsing; section 6.1 pipes gzip through `DecompressionStream`. Binding path (`wasm-streams`) is unverified and listed as such. |
| "want oids are never validated ... unknown wants return the whole latest.pack instead of an ERR" | blocker | Section 9 step 1: `Index::lookup` on every want in `RepoDo::fetch_v2`, `ERR upload-pack: not our ref <oid>` on a miss. `parse_fetch` rejects a `fetch` with no `want`. The lookup itself lives in want-have-negotiation's proof. |
| "pkt() spreads each 65 KB frame through a JS array; replace with buffer copies" | caveat | `Sideband::data`: `chunks(MAX_BAND_DATA)` + `band_to_write` into one `Vec<u8>`, one copy per frame, no intermediate arrays. |
| "packKey/range from the DO and the R2 read are not pinned to one pack version; a concurrent latest.pack rebuild yields a corrupt pack" | caveat | Removed by the contract: packs are immutable per `pack_id` (section 2.2), GC produces a new id and the janitor deletes the old key only GRACE after `dead_at` (section 5). No etag pinning needed. |
| "wait-for-done, shallow and object-info are advertised but not honoured (--depth silently yields full history)" | caveat | `write_capability_advertisement_v2` advertises exactly rule 6. `shallow` and `filter` are honoured (`parse_fetch` -> section 9 step 5, scenarios 10 and 11); `wait-for-done` and `object-info` are not advertised and `parse_v2_command` rejects `object-info`. |
| "no symref-target/symref=HEAD, so clone chooses the default branch by oid match" | caveat | `write_ls_refs` emits `HEAD` first with ` symref-target:<meta.head>` when `symrefs` is requested, and `unborn HEAD symref-target:` for an empty repo. The v0 listing carries no `symref=` because rule 7 fixes its capability list; v0 clients only get `ls-remote`, which does not need it. |
| "'v2 only' is really v2 fetch + v0 push; dumb HTTP is out of scope" | caveat | Accepted and written into Mechanism: v2 for all upload-pack traffic, v0 for `receive-pack` by git's own design (rule 5), no dumb HTTP (no `objects/info/packs` route; 404 per section 10). |
| Review interop item 5: "the v0 shim ignores the client's capability list; it always sends side-band-64k" | interop | Moot for upload-pack (no v0 POST). For receive-pack, `ReceiveCaps.side_band_64k` decides band 1 versus raw pkt-lines (rule 4), proven in repo-do-ref-authority. |
| Review interop item 6: "`ls-refs` args also carry `peel`, `unborn`, `symrefs`; unhandled" | interop | `parse_ls_refs` handles all three plus `ref-prefix`; anything else is `Error::Protocol`. `peeled:` is written when `RefRow.peeled` is set. |

## Known limits
- `RefRow.peeled` is written when present but this module does not compute it; `list_refs` in refs-sqlite-objects-r2 returns `peeled: None` today and the `/_do/ls-refs` route has no awaits, so the peeled target must be recorded at push time (ingest sees the tag object in pass B). Scenario 3's "peeled line in ls-refs" depends on that.
- Ref names over 4,096 bytes are refused by `parse_receive_header` with `Error::Protocol`; this is a rule of `wire`, not of section 3. A filesystem-backed git server refuses them too (`PATH_MAX`), but it deviates from git's own parser. It is what keeps every `ls-refs` line under `MAX_PKT_DATA`, so the `()` writers of 1.1 cannot fail; the `line` helper's `ERR` fallback is a belt for that brace.
- The edge parses the v2 command once and the DO parses the raw body again; a 1 MiB body is parsed twice. Accepted for the foundation (upload-pack bodies are small; section 6.5).
- `encode::band_to_write` argument order and `Channel` variant names are from docs.rs, not run. `Response::from_stream`, `DecompressionStream` piping and `wasm-streams` are unverified at runtime and belong to `fetch_v2` and `BodyReader`.
- Draining a rejected v0 POST body is bounded at 1 MiB; a v0 client that sends a larger upload-pack body (thousands of haves) may still trigger the workerd `Can't read from request stream` restart seen locally. Production behaviour unmeasured.
- `write_advertisement_v0` emits `<peeled> <name>^{}` lines and `write_ls_refs` emits `peeled:` only when `RefRow.peeled` is set (see the first limit). `Sideband::progress` is omitted from the block for length; it is `error` with `Channel::Progress`. The `info/refs` edge glue (eight lines) is described in a comment, not shown.
- `Git-Protocol: version=1` on receive-pack is ignored (rule 8); git falls back to v0 silently, checked in source, not by scenario.
- `deepen-since`, `deepen-not`, `deepen-relative`, `tree:<n>`, `sparse:` filters and `blob:limit=<n>[kmg]` suffixes are `Error::Protocol` (section 12); `git clone --filter=blob:limit=1m` fails with a clear 400 rather than silently.
- Memory and CPU: this module touches at most one 1 MiB command body plus one 64 KiB frame; nothing here approaches 128 MB or the CPU limit. Subrequests: `info_refs` v0 costs one stub call, `upload_pack` one stub call plus what `fetch_v2` spends; the edge budget is `ReqBudget` only (not enforced locally, platform-facts #7).
- Scenarios this proof must pass: 1, 3 (ls-refs part), 10, 11, 12, 13. Added scenario: "v1 ls-remote": `git -c protocol.version=1 ls-remote` sees a `version 1` pkt and the same refs as v0; and "gzip v2 fetch": `git -c http.postBuffer=1 fetch` after 200 local commits sends a gzip `fetch` body and gets `ACK` lines.

## Depends on
- info-refs-endpoint
- want-have-negotiation
- repo-do-ref-authority
- refs-sqlite-objects-r2
- two-phase-push
- partial-clone-filters
