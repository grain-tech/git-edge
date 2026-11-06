> Idea #7 · foundation · verdict: **lands with caveats** · feasibility 4/5 · reliability 3/5 · correctness 3/5 · effort: weeks
> Proof: [proofs/precomputed-clone-pack.md](./precomputed-clone-pack.md) · Review: [reviews/precomputed-clone-pack.md](../../reviews/archived/precomputed-clone-pack.md)

# Precomputed pack slices for clone

## Mechanism
A repo DO alarm (debounced 30s after the last ref update) rebuilds one full packfile into R2 at `packs/<owner>/<repo>/<buildId>.pack`, laid out in git's natural type order `[12-byte PACK header][commits][trees][blobs][20-byte SHA-1 trailer]`, and records in DO SQLite the ref tips it covers plus byte-offset "slices" (`full`, `blobless` = header..end-of-trees) each with its object count and precomputed trailer. When `POST /:owner/:repo/git-upload-pack` arrives with a v2 `command=fetch` that has only `want` lines and `done` (a fresh clone), the Worker asks the repo DO whether every want is a covered tip; the DO answers with a slice descriptor `{key, start, end, count, sha1}` or `null`. On a hit the Worker never touches the DO again: it emits `packfile\n`, then a synthesized 12-byte header, then `env.BUCKET.get(key, {range})` streamed through a sideband-64k pkt-line reframer, then the stored trailer and a flush-pkt. On a miss (refs moved since the build, `have`s, `deepen`, no `ofs-delta`) it falls through to the negotiated path.

## Primitives
- Durable Objects with SQLite storage (`ctx.storage.sql.exec`) — GA
- DO alarms (`ctx.storage.setAlarm`, `alarm()`) — GA; used as a debounced repack trigger and for chunked builds
- R2 `get(key, { range: { offset, length } })` streaming range reads — GA
- R2 multipart upload (`createMultipartUpload` / `uploadPart` / `complete`) for writing packs larger than one alarm invocation — GA
- Workers streaming `Response` bodies + `TransformStream` for pkt-line reframing — GA
- (Optional) R2 public bucket / custom domain so the same object doubles as a `bundle-uri` target — GA

## Proof code
```typescript
// ---------- repo DO: decides, never moves pack bytes ----------
type Slice = { key: string; start: number; end: number; count: number; sha1: string };

export class RepoDO implements DurableObject {
  constructor(private ctx: DurableObjectState, private env: Env) {
    ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS pack_builds (id TEXT PRIMARY KEY, key TEXT, built_at INTEGER);
      CREATE TABLE IF NOT EXISTS pack_tips   (build TEXT, oid TEXT, PRIMARY KEY (build, oid));
      CREATE TABLE IF NOT EXISTS pack_slices (build TEXT, kind TEXT, start INTEGER, "end" INTEGER,
                                              count INTEGER, sha1 TEXT, PRIMARY KEY (build, kind));
      CREATE TABLE IF NOT EXISTS refs (name TEXT PRIMARY KEY, oid TEXT)`);
  }

  async fetch(req: Request): Promise<Response> {
    const url = new URL(req.url);
    if (url.pathname === "/clone-slice") {
      const { wants, kind } = await req.json<{ wants: string[]; kind: "full" | "blobless" }>();
      return Response.json(this.cloneSlice(wants, kind));
    }
    if (url.pathname === "/update-ref") {               // called by two-phase-push after CAS
      // ... CAS on refs table ...
      await this.ctx.storage.setAlarm(Date.now() + 30_000); // debounce: replaces pending alarm
      return new Response("ok");
    }
    return new Response("not found", { status: 404 });
  }

  // Fast path iff every want is a tip the newest build was made from.
  cloneSlice(wants: string[], kind: string): Slice | null {
    const sql = this.ctx.storage.sql;
    const build = sql.exec<{ id: string; key: string }>(
      "SELECT id, key FROM pack_builds ORDER BY built_at DESC LIMIT 1").toArray()[0];
    if (!build) return null;
    const tips = new Set(sql.exec<{ oid: string }>(
      "SELECT oid FROM pack_tips WHERE build = ?", build.id).toArray().map(r => r.oid));
    if (!wants.every(w => tips.has(w))) return null;   // refs moved since build -> negotiate instead
    const s = sql.exec<Slice>(
      `SELECT ? AS key, start, "end", count, sha1 FROM pack_slices WHERE build = ? AND kind = ?`,
      build.key, build.id, kind).toArray()[0];
    return s ?? null;
  }

  async alarm(): Promise<void> {
    const id = crypto.randomUUID(), key = `packs/${this.ctx.id}/${id}.pack`;
    const tips = this.ctx.storage.sql.exec<{ oid: string }>("SELECT oid FROM refs").toArray();
    // pack-objects walk (see gc-and-repack-alarm): reachable objects in type order commits,trees,blobs;
    // existing delta entries are copied verbatim (ofs-delta, offsets rebased), no re-deltification.
    // Bytes go to R2 via multipart (>=5 MiB parts); a streaming SHA-1 runs over the bytes and its
    // state is snapshotted at the blob boundary so the blobless trailer costs nothing extra.
    const mp = await this.env.BUCKET.createMultipartUpload(key);
    const { parts, counts, offsets, trailers } = await writePack(mp, tips /* ... */);
    await mp.complete(parts);
    const sql = this.ctx.storage.sql;
    sql.exec("INSERT INTO pack_builds VALUES (?, ?, ?)", id, key, Date.now());
    for (const t of tips) sql.exec("INSERT INTO pack_tips VALUES (?, ?)", id, t.oid);
    sql.exec("INSERT INTO pack_slices VALUES (?, 'full', 12, ?, ?, ?)", id, offsets.trailer, counts.all, trailers.full);
    sql.exec("INSERT INTO pack_slices VALUES (?, 'blobless', 12, ?, ?, ?)", id, offsets.blobs, counts.noBlobs, trailers.blobless);
  }
}

// ---------- Worker: data plane ----------
const enc = new TextEncoder();
const pkt = (s: string) => enc.encode(s.length.toString(16).padStart(4, "0") + s) as Uint8Array; // pkt-line
const FLUSH = enc.encode("0000");
const MAX = 65515; // 65520 pkt cap - 4 len - 1 band byte

// Reframe an arbitrary byte stream into sideband-64k band-1 pkt-lines.
function sideband(): TransformStream<Uint8Array, Uint8Array> {
  let buf = new Uint8Array(0);
  const frame = (d: Uint8Array) => {
    const out = new Uint8Array(5 + d.length);
    out.set(enc.encode((5 + d.length).toString(16).padStart(4, "0"))); out[4] = 1; out.set(d, 5);
    return out;
  };
  return new TransformStream({
    transform(chunk, c) {
      const m = new Uint8Array(buf.length + chunk.length); m.set(buf); m.set(chunk, buf.length); buf = m;
      let i = 0; for (; i + MAX <= buf.length; i += MAX) c.enqueue(frame(buf.subarray(i, i + MAX)));
      buf = buf.slice(i);
    },
    flush(c) { if (buf.length) c.enqueue(frame(buf)); },
  });
}

export async function uploadPack(req: Request, env: Env, repo: DurableObjectStub): Promise<Response> {
  const cmd = parseV2Fetch(await req.arrayBuffer()); // pkt-lines: command=fetch, caps, 0001, want/have/done, 0000
  const isFreshClone = cmd.command === "fetch" && cmd.haves.length === 0 && cmd.done
    && !cmd.deepen && cmd.caps.includes("ofs-delta");
  const slice: Slice | null = isFreshClone
    ? await (await repo.fetch("https://do/clone-slice", { method: "POST",
        body: JSON.stringify({ wants: cmd.wants, kind: cmd.filter === "blob:none" ? "blobless" : "full" }) })).json()
    : null;
  if (!slice) return negotiatedFetch(cmd, env, repo);                 // want-have-negotiation path

  const obj = await env.BUCKET.get(slice.key, { range: { offset: slice.start, length: slice.end - slice.start } });
  if (!obj) return negotiatedFetch(cmd, env, repo);
  const header = new Uint8Array(12);                                  // "PACK" | version 2 | count
  header.set(enc.encode("PACK")); new DataView(header.buffer).setUint32(4, 2); new DataView(header.buffer).setUint32(8, slice.count);
  const trailer = Uint8Array.from(slice.sha1.match(/../g)!.map(h => parseInt(h, 16)));

  const { readable, writable } = new TransformStream<Uint8Array, Uint8Array>();
  (async () => {
    const w = writable.getWriter();
    await w.write(pkt("packfile\n"));
    const sb = sideband();
    const drain = sb.readable.pipeTo(new WritableStream({ write: d => w.write(d) }));
    const sw = sb.writable.getWriter();
    await sw.write(header); sw.releaseLock();
    await obj.body.pipeTo(sb.writable, { preventClose: true });
    const sw2 = sb.writable.getWriter(); await sw2.write(trailer); await sw2.close();
    await drain; await w.write(FLUSH); await w.close();
  })();
  return new Response(readable, { headers: { "content-type": "application/x-git-upload-pack-result", "cache-control": "no-cache" } });
}
```

## Why it works
- A fresh `git clone` over protocol v2 is exactly `command=fetch` + `want <tip>` for every advertised ref + `done` with zero `have`s; the server may skip the acknowledgments section and reply with the `packfile` section straight away, so the entire response is deterministic given the wanted tips — which is what makes it precomputable.
- git accepts any valid pack whose objects are a superset of what it needs (index-pack just indexes the extras), so the response only has to guarantee that every wanted tip and everything reachable from it is inside the pack. Requiring `wants ⊆ tips_at_build` guarantees that without a connectivity walk at request time.
- The pack header count and the SHA-1 trailer are the only whole-pack-dependent bytes, so a slice `[12, end)` of the stored object becomes a standalone valid pack by prepending a rewritten header and appending a trailer computed at build time. Deltas are only ever intra-type in git, and the build writes commits→trees→blobs, so an `ofs-delta` in the blobless slice always points backwards into the slice.
- In the v2 packfile section the pack must arrive as sideband-64k band-1 pkt-lines (≤65520 bytes each); reframing is a copy, no inflate, so the Worker touches every byte once with near-zero CPU regardless of pack size, and the DO is out of the byte path entirely.
- The client asked for `ofs-delta` (git always does in v2), so serving a pack containing ofs-deltas is legal; a client that omitted it is routed to the negotiated path instead of being handed a pack it cannot parse.
- The same R2 object is a well-formed `.pack` on its own (full header + trailer), so it can be advertised via `bundle-uri` or fetched directly from a public bucket with no extra build.

## Known limits
- Staleness window: between a push and the alarm's rebuild (≥30s debounce plus build time), fresh clones miss the fast path and take the negotiated route. Keeping the previous build for a grace period is a straightforward extension (two-row `pack_builds`), not shown.
- The rebuild is the hard part and is hand-waved (`writePack`). Reusing existing delta entries verbatim avoids re-deltification CPU, but a large repo still needs the walk chunked across chained alarms (30s CPU per alarm invocation) with multipart-upload state checkpointed in SQLite; a first push of a huge repo will not have a pack for minutes.
- Every build writes a whole new pack to R2 (Class A multipart ops + storage until the old build is deleted). Cost is O(repo size) per debounced push burst, which is fine for source repos and bad for repos with frequent pushes to huge histories; a push-rate-aware debounce would be needed.
- `wants ⊆ tips` is conservative: `git clone --branch old-tag` after any other ref moved still hits the fast path, but a clone racing a build that just changed one tip falls back even though the previous pack would have served it.
- Blobless slice assumes the build wrote strictly commits, trees, blobs; tags (annotated) must be placed before commits or excluded from the blobless count, and `tree:0` / `blob:limit` filters are not sliceable this way (they need per-object decisions, see partial-clone-filters).
- Shallow (`deepen`) and `have`-bearing fetches always miss; this idea covers only the fresh-clone case.
- The reframer buffers up to 65515 bytes plus one R2 chunk; the DO holds only rows. Memory is bounded well under 128MB, but R2 range GET throughput for a single stream (~hundreds of MB/s at best) sets the clone speed ceiling; a multi-GB clone runs for minutes of wall time, which Workers allow for streaming responses.
- Progress messages (band 2) are omitted; git prints nothing until the pack finishes unless `no-progress` is absent and you add periodic band-2 pkt-lines.

## Depends on
- repo-do-ref-authority
- refs-sqlite-objects-r2
- protocol-v2-only
- info-refs-endpoint
- gc-and-repack-alarm
- want-have-negotiation (fallback path)
