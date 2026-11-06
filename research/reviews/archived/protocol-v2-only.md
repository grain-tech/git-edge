# Review: Speak git protocol v2 only, translate v0 at the edge

> Idea #3 · foundation · verdict: **risky** · feasibility 4/5 · reliability 3/5 · correctness 2/5 · effort: weeks
> Proof: [proofs/protocol-v2-only.md](../../proofs/archived/protocol-v2-only.md) · Review: [reviews/protocol-v2-only.md](./protocol-v2-only.md)

# Review: protocol-v2-only

## Scores
- Feasibility: 4/5. Everything named is GA: Workers streaming `Response` bodies via `TransformStream`, SQLite-backed DOs (`ctx.storage.sql`), typed DO RPC, R2 `get(key, {range})`. Limits are respected on the read path: the pack never enters Worker memory, framing is byte copying. Two real gaps: (a) `pkt()` spreads a 65 KB `Uint8Array` into a JS array per frame (`[...hdr, ...b]`), so a 1 GB clone is ~16k array spreads and materialises each frame twice; use `cat`/`set` instead or the 30 s CPU budget is in play. (b) git gzips `git-upload-pack` POST bodies (`rpc.gzip_request=1` in remote-curl) once they exceed the small-buffer threshold, and sends `Transfer-Encoding: chunked` for large ones; the Worker must honour `Content-Encoding: gzip` on the request with `DecompressionStream("gzip")` before `parseV2/parseV0`, or an incremental fetch with many haves parses garbage. Neither is a platform limit.
- Reliability: 3/5. The path is read-only and stateless, so nothing it does can lose or split-brain refs. The weak spot is version skew between the DO's answer and the R2 object it points at (below).
- Correctness: 2/5. The architectural claim is right (v2 is stateless, sectioned, and header-selected; receive-pack has no v2). The code as written fails every fresh `git clone` from a v2 client and every incremental fetch from a v0 client, for exact wire reasons below.
- Effort: weeks.

## Crash walk-through
Client sends `fetch` with `want X`, `done`. Worker calls `stub.negotiate`, gets `latest.pack` + range, opens the R2 stream, starts writing sideband frames, and the isolate dies 40% through. Server side: no state was written anywhere; nothing to clean. Client side: `index-pack` sees a truncated pack (no trailer) and fails with `early EOF` / `fetch-pack: invalid index-pack output`; the client retries and gets the full pack. No data loss, no orphans. Same outcome if the DO is evicted mid-RPC: the Worker throws before the first byte and the client sees an HTTP error, retries cleanly. The one non-obvious skew: `negotiate` returns `{packKey, range}` computed from an index in DO SQLite, and the Worker then opens R2 by key. If a pack rebuild (precomputed-clone-pack) overwrites `latest.pack` between those two steps, the range now addresses a different pack layout and the client receives a well-framed but corrupt pack (`index-pack` fails, and keeps failing until the DO index catches up). Fix is cheap: version-suffixed pack keys, or pass the R2 etag from the DO and use `get(key, {onlyIf: {etagMatches}})`.

## Concurrency walk-through
Two rounds of one fetch straddle a push: round 1 `ls-refs` reports `main=A`; push moves `main` to `B`; round 2 `fetch want A`. Objects are immutable and `A` is still in the pack, so this is served correctly; the client simply ends one commit behind, which is what git expects. Concurrent fetchers never contend: the DO answers two tiny synchronous SQLite reads per fetch and all streaming happens in the Worker, so the "low thousands req/s per DO" ceiling the proof states is honest. Refs cannot split-brain here because this idea never writes them. The hole is the unvalidated `want`: `negotiate` never checks that a wanted oid is advertised or reachable (`uploadpack.allowAnySHA1InWant` semantics), so an unknown oid returns the whole `latest.pack` instead of `ERR upload-pack: not our ref`. Not a consistency bug, but a force-push-then-fetch race becomes "serve a stale full clone" rather than a clean error.

## Interop check
Advertisement is right: `# service=git-upload-pack` + flush is accepted by remote-curl in v2 mode, and `version 2` commits the client. `ls-refs` output is right. Then, with git 2.43 (`protocol.version=2` default):
1. Fatal: fresh clone has no haves, so `send_fetch_request` writes `done` in the first `fetch` request and moves to `FETCH_GET_PACK`, which requires the next section to be `packfile` (after optional `shallow-info`/`wanted-refs`/`packfile-uris`). gitprotocol-v2: "if the client ... sent a 'done' line ... the acknowledgments section MUST be omitted". The proof always writes `acknowledgments`/`ready`/delim first, so every clone dies with `fatal: expected 'packfile', received 'acknowledgments'`.
2. Fatal on v0 shim for incremental fetch: v0 clients send haves without `done` until they see ACKs. The advertisement omits `multi_ack_detailed`, so the client expects exactly one `ACK <oid>`/`NAK` per round and then sends another POST; the proof replies `NAK` followed by sideband pack bytes, and `find_common` dies with `git fetch-pack: expected ACK/NAK, got '\x01PACK...'`. Only a v0 fresh clone (`done` in the first body) works. `no-done` is advertised but is only defined alongside `multi_ack_detailed`.
3. `wait-for-done` is advertised; with `push.negotiate`/`--negotiate-only` the client expects no `ready` until it sends `done`. Harmless today but it is exactly the "advertise what you do not honour" trap the proof warns about; `shallow` is advertised and `deepen` args are ignored, so `--depth 1` silently yields a full-history clone.
4. Missing `symref-target:` (`symrefs` arg) in `ls-refs` and `symref=HEAD:` in the v0 caps: clone works but picks the default branch by oid match, detaching HEAD when two branches share a tip.
5. The v0 shim ignores the client's capability list; it always sends side-band-64k, which is only legal if the client asked for it (git always does, `git-upload-archive`-style tools do not).
6. `ls-refs` args also carry `peel`, `unborn`, `symrefs`; unhandled but ignorable. `object-info` is advertised and not implemented (400 on `command=object-info`).

## Blockers
- Omit the `acknowledgments` section when the request contains `done`; emit `NAK`/`ACK` + `ready` only in non-done rounds.
- v0 shim needs `multi_ack_detailed` + per-round `ACK <oid> common|ready` (or refuse v0 incremental fetch); otherwise it only serves clones.
- Decompress gzipped request bodies before parsing.
- Validate `want`s against advertised/reachable oids.

## Caveats
- Replace the array-spread framing with buffer copies; keep zlib off this path.
- Pin pack key/etag between DO answer and R2 read.
- Do not advertise `wait-for-done`, `shallow`, `object-info` until honoured.
- Dumb-HTTP is out of scope as the proof concedes; push is v0 by construction, so "v2 only" is really "v2 fetch, v0 push".

## Verdict
risky. The design is the correct one for Workers and every primitive is GA, and the fixes are all wire-format edits, not architectural. But the proof as written cannot complete a single `git clone` from a modern client (item 1) and its v0 shim is clone-only (item 2), so the stated "one code path for negotiation, v0 shim is thin" claim is unproven. Days to make v2 clone/fetch interoperate once want-have-negotiation and a precomputed pack exist; weeks for a v0 shim that honours multi_ack_detailed and for shallow/filter support.
