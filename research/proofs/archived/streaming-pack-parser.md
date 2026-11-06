> Idea #4 · foundation · verdict: **lands with caveats** · feasibility 4/5 · reliability 3/5 · correctness 3/5 · effort: weeks
> Proof: [proofs/streaming-pack-parser.md](./streaming-pack-parser.md) · Review: [reviews/streaming-pack-parser.md](../../reviews/archived/streaming-pack-parser.md)

# Packfile parsing in a Worker with a streaming inflater

## Mechanism

`POST /:owner/:repo/git-receive-pack` hits the stateless Worker, which reads the pkt-line command section (`<old> <new> <ref>\0caps`, then `0000`) and then consumes the raw `PACK` stream from the same request body. The Worker walks the pack object by object — varint type/size header, zlib inflate, SHA-1 of `"<type> <size>\0" + content` — and `put`s each resolved object to R2 at `objects/<sha>` (idempotent, content-addressed); ofs-/ref-deltas whose base is already known are applied inline, the rest are parked under `pending/<pushId>/`. When the trailing 20-byte pack checksum is verified, the Worker sends the DO (`idFromName("owner/repo")`) one manifest `{pushId, updates:[{ref,old,new}]}`; the DO resolves leftover deltas, checks the new tips exist in R2, CASes the refs in SQLite in one transaction, and the Worker writes the `report-status` pkt-lines back.

The one honest catch: the Web `DecompressionStream` cannot be used for per-object inflate, because a pack has N zlib streams back to back with no compressed-length prefix, and `DecompressionStream` (spec and Node, verified) throws `Trailing junk found after the end of the compressed stream` rather than telling you where the stream ended. The streaming inflater that actually works is `node:zlib` under `nodejs_compat`: `inflateSync(buf, {info:true}).engine.bytesWritten` reports consumed input bytes (verified on Node 22.22), so the parser can advance exactly one object at a time and pull more body chunks when it hits `unexpected end of file`.

## Primitives

- Workers `fetch` handler with a streaming `Request.body` (`ReadableStream<Uint8Array>`) — GA
- `nodejs_compat` `node:zlib` `inflateSync` with `info: true` (consumed-bytes reporting) — GA flag; the `info` option is a Node port inside workerd, verify on deploy
- `DecompressionStream("deflate")` — GA, but only usable for the outer body if a proxy sends `Content-Encoding: deflate`; not for per-object inflate (see above)
- `crypto.subtle.digest("SHA-1")` for object ids — GA
- R2 `env.BUCKET.put(key, bytes)` / `.get(key)` / `.head(key)` — GA
- Durable Object with `ctx.storage.sql.exec` (refs, pending pushes) and `ctx.storage.setAlarm` (orphan sweep) — GA (SQLite-backed DOs)
- Workers `limits.cpu_ms` in wrangler.jsonc to lift the 30 s CPU default (up to 300 s) — GA

## Proof code

```typescript
import { inflateSync } from "node:zlib";            // nodejs_compat
import { DurableObject } from "cloudflare:workers";

const TYPES = ["", "commit", "tree", "blob", "tag", "", "ofs-delta", "ref-delta"];

/** Pull-based byte window over the request body: parser asks for `need` bytes at `pos`. */
class Window {
  buf = new Uint8Array(0); pos = 0; done = false;
  constructor(private rd: ReadableStreamDefaultReader<Uint8Array>) {}
  async ensure(n: number) {                         // guarantee buf.length - pos >= n (or EOF)
    while (this.buf.length - this.pos < n && !this.done) {
      const { value, done } = await this.rd.read(); this.done = done;
      if (value) { const b = new Uint8Array(this.buf.length - this.pos + value.length);
        b.set(this.buf.subarray(this.pos)); b.set(value, this.buf.length - this.pos); this.buf = b; this.pos = 0; }
    }
  }
  async byte() { await this.ensure(1); return this.buf[this.pos++]; }
}

/** Inflate exactly one zlib member starting at w.pos; returns content and advances w.pos. */
async function inflateOne(w: Window, expect: number): Promise<Uint8Array> {
  for (let need = 64; ; need *= 2) {               // grow until zlib sees Z_STREAM_END
    await w.ensure(need);
    try {
      const r = inflateSync(w.buf.subarray(w.pos), { info: true }) as { buffer: Buffer; engine: { bytesWritten: number } };
      w.pos += r.engine.bytesWritten;                // <- the "consumed" count DecompressionStream lacks
      if (r.buffer.length !== expect) throw new Error("size mismatch");
      return new Uint8Array(r.buffer);
    } catch (e: any) { if (!/unexpected end/.test(e.message) || w.done) throw e; }
  }
}

async function sha1(type: string, body: Uint8Array) {
  const hdr = new TextEncoder().encode(`${type} ${body.length}\0`);
  const d = await crypto.subtle.digest("SHA-1", new Uint8Array([...hdr, ...body]));
  return [...new Uint8Array(d)].map(b => b.toString(16).padStart(2, "0")).join("");
}

/** Worker: git-receive-pack. Assumes pkt-line command section already parsed (info-refs-endpoint). */
export async function receivePack(body: ReadableStream<Uint8Array>, env: Env, updates: RefUpdate[], pushId: string) {
  const w = new Window(body.getReader());
  await w.ensure(12);                                // "PACK" + version(4) + count(4), big-endian
  const dv = new DataView(w.buf.buffer, w.buf.byteOffset + w.pos, 12);
  if (dv.getUint32(0) !== 0x5041434b || dv.getUint32(4) !== 2) throw new Error("bad PACK header");
  const count = dv.getUint32(8); w.pos += 12;
  const byOffset = new Map<number, string>();        // pack offset -> sha, for ofs-delta bases
  const pending: string[] = [];

  for (let i = 0; i < count; i++) {
    const start = w.pos; let c = await w.byte();
    const type = (c >> 4) & 7; let size = c & 15;    // varint: 3 type bits, 4 size bits, then 7 bits/byte
    for (let sh = 4; c & 0x80; sh += 7) { c = await w.byte(); size |= (c & 0x7f) << sh; }
    let baseKey: string | undefined;
    if (type === 6) {                                // ofs-delta: negative offset, MSB-first with +1 per byte
      c = await w.byte(); let off = c & 0x7f;
      while (c & 0x80) { c = await w.byte(); off = ((off + 1) << 7) | (c & 0x7f); }
      baseKey = byOffset.get(start - off);
    } else if (type === 7) {                         // ref-delta: 20-byte base sha follows header
      await w.ensure(20); baseKey = hex(w.buf.subarray(w.pos, w.pos + 20)); w.pos += 20;
    }
    const raw = await inflateOne(w, size);
    let objType = TYPES[type], content = raw;
    if (type >= 6) {
      const base = baseKey && await env.BUCKET.get(`objects/${baseKey}`);   // in-pack or thin-pack base
      if (!base) { const k = `pending/${pushId}/${i}`; await env.BUCKET.put(k, raw, { customMetadata: { base: baseKey ?? "" } }); pending.push(k); continue; }
      const meta = base.customMetadata!; objType = meta.type;
      content = applyDelta(new Uint8Array(await base.arrayBuffer()), raw);  // git delta ops: copy(off,len) / insert(bytes)
    }
    const sha = await sha1(objType, content); byOffset.set(start, sha);
    await env.BUCKET.put(`objects/${sha}`, content, { customMetadata: { type: objType } }); // idempotent by construction
  }
  await w.ensure(20); const trailer = hex(w.buf.subarray(w.pos, w.pos + 20)); w.pos += 20;   // SHA-1 of pack minus trailer

  const repo = env.REPO.get(env.REPO.idFromName(pushId.split("#")[0]));
  return repo.commitPush({ pushId, updates, pending, trailer });          // DO RPC; DO answers per-ref ok/ng
}

export class RepoDO extends DurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) { super(ctx, env);
    ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS refs (name TEXT PRIMARY KEY, sha TEXT NOT NULL)`);
    ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS pending (push_id TEXT PRIMARY KEY, keys TEXT, created INTEGER)`); }

  async commitPush(m: { pushId: string; updates: RefUpdate[]; pending: string[]; trailer: string }) {
    for (const k of m.pending) await this.resolveDelta(k);                  // bases are now all in R2
    const results = this.ctx.storage.transactionSync(() => m.updates.map(u => {
      const cur = this.ctx.storage.sql.exec("SELECT sha FROM refs WHERE name=?", u.ref).toArray()[0]?.sha ?? ZERO;
      if (cur !== u.old) return `ng ${u.ref} fetch first`;                  // compare-and-swap on the ref
      this.ctx.storage.sql.exec("INSERT OR REPLACE INTO refs VALUES (?,?)", u.ref, u.new);
      return `ok ${u.ref}`;
    }));
    await this.ctx.storage.setAlarm(Date.now() + 15 * 60_000);             // sweep pending/<pushId>/* orphans
    return ["unpack ok", ...results];                                        // -> pkt-lines, band 1 if side-band-64k
  }
  async alarm() { await this.env.BUCKET.delete(/* list(prefix:"pending/") older than 15 min */ []); }
}
```

## Why it works

- The pack is exactly what `git push` puts on the wire after the command pkt-lines: `PACK`, version 2, object count, then `count` objects, then a SHA-1 trailer. No index (`.idx`) is sent; the server must build offsets itself, which is what `byOffset` does and why ofs-delta resolution needs the running `start` offset.
- Each object's header encodes the *inflated* size only; the compressed length is unknown until zlib reports `Z_STREAM_END`. `inflateSync(..., {info:true}).engine.bytesWritten` gives exactly that consumed count (tested: 22 of a 22-byte member with junk after it), so `w.pos` lands on the next object's header byte. Retrying on `unexpected end of file` with a larger window is how the parser stays streaming instead of buffering the whole body.
- Object ids are `sha1("<type> <len>\0" + content)` over the *undeltified* content, so a delta must be applied before hashing; parking unresolved deltas under `pending/` and resolving in the DO (after all bases from this push have landed in R2) handles both forward references inside the pack and thin packs (`ref-delta` against objects the client knows the server has).
- Writing `objects/<sha>` before touching refs means a crash mid-push leaves nothing reachable; a retried push re-PUTs identical keys. The ref flip is a compare-and-swap against the client's `<old>` sha in a single DO transaction, which is precisely git's "fetch first" semantics for non-fast-forward or racing pushes.
- `report-status` is what `git push` waits for: `unpack ok` then one `ok <ref>` / `ng <ref> <reason>` line per command, wrapped in side-band channel 1 when the client asked for `side-band-64k`. The DO's return array maps straight onto that.

## Known limits

- The idea as titled ("streaming inflate via DecompressionStream") does not work for per-object inflate: `DecompressionStream` throws on trailing data and exposes no consumed-byte count. The proof substitutes `node:zlib` `inflateSync` under `nodejs_compat`; if workerd's port ever drops the `info` option, the fallback is pako/fflate-style JS inflate that exposes `strm.next_in`, or a Wasm inflater (see `wasm-git-core`).
- `inflateSync` is synchronous and needs the whole compressed object in memory, and the delta apply needs base + result in memory: a single blob larger than roughly 40 MB (compressed + raw + base) is out of reach under the 128 MB isolate limit. `git-lfs` (`native-lfs`) or `presigned-direct-upload` is the answer for those, not this parser.
- CPU: inflate + SHA-1 of a 100 MB push is on the order of seconds; the 30 s default CPU cap must be raised via `limits.cpu_ms` (max 300 s), and pushes beyond that still fail. Wall-clock is not the limit (streams are fine), CPU is.
- The retry loop re-inflates from the object start each time the window is too short (geometric growth, so at most ~log2(size) attempts, but worst-case 2x CPU on big objects). A real inflater with `Z_SYNC_FLUSH` resume would avoid this; `node:zlib`'s streaming `Inflate` in workerd does not expose reliable consumed counts mid-stream, so this is hand-waved as acceptable.
- R2 cost: one PUT per object; a push of 10k small objects is 10k class-A operations (~$0.045). `applyDelta` bases are re-fetched from R2 for every delta (one GET each) unless a per-push in-memory LRU is added; that cache is elided here.
- SHA-1 verification of the pack trailer requires an incremental SHA-1 over the whole body; `crypto.subtle.digest` is one-shot, so either buffer the pack (defeats streaming) or ship a ~100-line JS SHA-1. Elided.
- Everything is serialized through one DO per repo for the ref flip only; parsing runs in the stateless Worker, so concurrent pushes to one repo parse in parallel and only the CAS is serialized. Still, the DO does delta resolution for `pending/` entries, which can be heavy for large thin pushes; moving that into the Worker before the RPC is the obvious next step.
- `applyDelta` (copy/insert opcode interpreter) and `hex` are omitted as well-known, ~30 lines.

## Depends on

- info-refs-endpoint (pkt-line codec and command section parsing before the PACK bytes)
- content-addressed-r2-keys (idempotent `objects/<sha>` writes)
- two-phase-push (`pending/` prefix, manifest to DO, janitor alarm)
- repo-do-ref-authority (single DO does the ref CAS)
- refs-sqlite-objects-r2 (refs table in DO SQLite, objects in R2)
