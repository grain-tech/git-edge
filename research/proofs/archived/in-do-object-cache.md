> Idea #9 · foundation · verdict: **risky** · feasibility 4/5 · reliability 4/5 · correctness 2/5 · effort: days
> Proof: [proofs/in-do-object-cache.md](./in-do-object-cache.md) · Review: [reviews/in-do-object-cache.md](../../reviews/archived/in-do-object-cache.md)

# Tiny in-DO object cache with alarm-driven eviction

## Mechanism
Every object read that is not a bulk clone (protocol v2 `fetch` with `filter blob:none` follow-ups, `object-info`, the raw `/raw/<oid>` API, and delta-base lookups) goes through `RepoDO.getObject(oid)` in the per-repo Durable Object. It checks an in-isolate `Map` first, then a SQLite table `objcache` in `ctx.storage.sql`, and only then does `env.BUCKET.get("objects/<oid>")` on R2; objects up to 512 KiB are admitted into both tiers on miss. What is cached is the *pack-ready* encoding (pack entry header varint + zlib stream), so a hit can be spliced byte-for-byte into an outgoing `PACK` without recompressing. A single DO alarm fires every 5 minutes, flushes batched hit counters, and deletes rows whose `last_hit` is older than the TTL or that fall outside the byte budget in LRU order; the memory tier is dropped to whatever SQLite kept.

## Primitives
- Durable Objects with SQLite-backed storage (`ctx.storage.sql.exec`, BLOB columns) — GA
- DO alarms (`ctx.storage.setAlarm` / `alarm()`) — GA
- R2 (`env.BUCKET.get` by content-addressed key, `arrayBuffer()`) — GA
- Workers `CompressionStream("deflate")` / `DecompressionStream("deflate")` — GA (zlib-wrapped deflate is what git uses)
- `ctx.blockConcurrencyWhile` to arm the first alarm before serving — GA
- Response streams for serving raw blobs — GA

## Proof code
```typescript
// RepoDO cache tier. Wire-up: wrangler.jsonc has
//   durable_objects.bindings [{ name: "REPO", class_name: "RepoDO" }],
//   migrations [{ tag: "v1", new_sqlite_classes: ["RepoDO"] }],
//   r2_buckets [{ binding: "BUCKET", bucket_name: "git-objects" }]
const MAX_ENTRY = 512 * 1024;         // admit only small hot blobs (package.json, lockfiles)
const BUDGET    = 32 * 1024 * 1024;   // SQLite bytes we let the cache hold
const TTL_MS    = 60 * 60 * 1000;     // untouched for an hour -> evicted
const SWEEP_MS  = 5 * 60 * 1000;

type Kind = 1 | 2 | 3 | 4;            // git pack types: commit, tree, blob, tag

export class RepoDO implements DurableObject {
  private mem = new Map<string, Uint8Array>();          // oid -> pack-ready entry
  private hits = new Map<string, number>();             // batched last_hit updates

  constructor(private ctx: DurableObjectState, private env: { BUCKET: R2Bucket }) {
    ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS objcache (
        oid TEXT PRIMARY KEY, kind INTEGER NOT NULL, size INTEGER NOT NULL,
        entry BLOB NOT NULL, last_hit INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS objcache_lru ON objcache(last_hit)`);
    ctx.blockConcurrencyWhile(async () => {
      if ((await ctx.storage.getAlarm()) === null) await ctx.storage.setAlarm(Date.now() + SWEEP_MS);
    });
  }

  /** Returns the pack entry (header varint + zlib) for oid, or null if unknown. */
  async getObject(oid: string): Promise<{ kind: Kind; size: number; entry: Uint8Array } | null> {
    const m = this.mem.get(oid);
    if (m) { this.hits.set(oid, Date.now()); return decodeHeader(m); }

    const row = this.ctx.storage.sql
      .exec<{ kind: Kind; size: number; entry: ArrayBuffer }>(
        "SELECT kind, size, entry FROM objcache WHERE oid = ?", oid).toArray()[0];
    if (row) {
      const entry = new Uint8Array(row.entry);
      this.mem.set(oid, entry); this.hits.set(oid, Date.now());
      return { kind: row.kind, size: row.size, entry };
    }

    // Miss: R2 holds loose objects as "<kind>\0<size>\0" + zlib(content)  (see content-addressed-r2-keys)
    const obj = await this.env.BUCKET.get(`objects/${oid}`);
    if (!obj) return null;
    const kind = Number(obj.customMetadata?.kind) as Kind;
    const size = Number(obj.customMetadata?.size);
    const z = new Uint8Array(await obj.arrayBuffer());              // already zlib, reuse as-is
    const entry = concat(packHeader(kind, size), z);                 // pack entry = varint hdr + zlib
    if (size <= MAX_ENTRY) {
      this.ctx.storage.sql.exec(
        "INSERT OR REPLACE INTO objcache (oid, kind, size, entry, last_hit) VALUES (?,?,?,?,?)",
        oid, kind, size, entry, Date.now());
      this.mem.set(oid, entry);
    }
    return { kind, size, entry };
  }

  /** Raw blob API: strip the pack header and inflate the zlib stream. */
  async serveRaw(oid: string): Promise<Response> {
    const o = await this.getObject(oid);
    if (!o) return new Response("not found", { status: 404 });
    const z = o.entry.subarray(packHeader(o.kind, o.size).length);
    return new Response(new Blob([z]).stream().pipeThrough(new DecompressionStream("deflate")),
      { headers: { "content-type": "application/octet-stream", "content-length": String(o.size) } });
  }

  async alarm(): Promise<void> {
    const sql = this.ctx.storage.sql;
    for (const [oid, t] of this.hits) sql.exec("UPDATE objcache SET last_hit = ? WHERE oid = ?", t, oid);
    this.hits.clear();
    sql.exec("DELETE FROM objcache WHERE last_hit < ?", Date.now() - TTL_MS);
    // LRU trim to byte budget: walk newest->oldest accumulating size, delete the tail.
    let used = 0, cutoff: number | null = null;
    for (const r of sql.exec<{ last_hit: number; size: number }>(
        "SELECT last_hit, size FROM objcache ORDER BY last_hit DESC")) {
      used += r.size; if (used > BUDGET) { cutoff = r.last_hit; break; }
    }
    if (cutoff !== null) sql.exec("DELETE FROM objcache WHERE last_hit <= ?", cutoff);
    this.mem.clear();                                   // memory tier refills lazily from SQLite
    await this.ctx.storage.setAlarm(Date.now() + SWEEP_MS);
  }
}

// git pack entry header: (type<<4 | size&15) then 7-bit little-endian continuation bytes.
function packHeader(kind: Kind, size: number): Uint8Array {
  const out = [ (kind << 4) | (size & 0x0f) ]; size >>>= 4;
  while (size > 0) { out[out.length - 1] |= 0x80; out.push(size & 0x7f); size >>>= 7; }
  return Uint8Array.from(out);
}
function decodeHeader(e: Uint8Array) {
  let kind = ((e[0] >> 4) & 7) as Kind, size = e[0] & 15, shift = 4, i = 0;
  while (e[i] & 0x80) { i++; size |= (e[i] & 0x7f) << shift; shift += 7; }
  return { kind, size, entry: e };
}
function concat(a: Uint8Array, b: Uint8Array) { const o = new Uint8Array(a.length + b.length); o.set(a); o.set(b, a.length); return o; }
```

## Why it works
- A git pack entry for an undeltified object is exactly `varint(type,size) || zlib(content)`; caching that form means a `fetch` response to a partial-clone client (`filter blob:none` then `want <blob-oid>`) is `PACK` header (`"PACK"`, version 2, count) + concatenated cached entries + SHA-1 trailer, no recompression on the hit path.
- The zlib stream stored in R2 for a loose object is already the pack-compatible `deflate` (zlib-wrapped) format, so the miss path does a single `arrayBuffer()` and a header prepend; nothing is inflated unless the raw API asks for it, and then `DecompressionStream("deflate")` handles the zlib wrapper natively.
- Refs never live in this table (they are the DO's SQLite ref table, `refs-sqlite-objects-r2`), so eviction can never make the repo inconsistent: every cached row is immutable, content-addressed, and re-fetchable from R2 by the same key.
- Hot small blobs are what lazy/partial clones and tooling hammer (`git sparse-checkout`, `object-info size`, package managers reading manifests), and those are precisely the reads that go through the DO one object at a time; bulk clone bypasses the cache and streams from R2 (`precomputed-clone-pack`).
- Batching `last_hit` in memory and flushing in the alarm keeps a cache hit at zero SQLite writes; SQLite writes are what DO billing counts, reads are cheap.
- Alarms are at-least-once and survive DO eviction, so the sweep runs even if the DO has been idle; `blockConcurrencyWhile` re-arms it on cold start so a wiped alarm cannot leave the table growing forever.

## Known limits
- The memory tier is per-isolate and vanishes on DO eviction/hibernation or any alarm sweep; the SQLite tier is the real cache. DO isolates are capped at 128 MB, so `BUDGET` for the memory map must stay far below that (the code clears it rather than budgeting it separately).
- SQLite-backed DO storage caps a single value at 2 MB and total storage at 10 GB per DO; `MAX_ENTRY` is set well under that. Objects larger than 512 KiB always go to R2 on every read.
- Admission is by oid, size and recency, not by path. The idea says "package.json, lockfiles" but the cache does not know filenames; it knows which small oids are read repeatedly. Filename-aware prewarming would need a tree walk on push (`speculative-packs` territory) and is not proven here.
- One alarm per DO. `gc-and-repack-alarm`, `alarm-chain-ci`, `ephemeral-repos` all want the same alarm slot, so a real RepoDO needs a tiny scheduler table (`next_job, due_at`) and one dispatching `alarm()`; hand-waved here.
- The LRU trim runs `SELECT ... ORDER BY last_hit` over the whole table on each sweep; at 32 MB / ~10 KB entries that is a few thousand rows, fine, but the budget should not be raised to gigabytes without a running-total column.
- Single-DO throughput: every cached read still serialises through one DO; this helps latency and R2 class-B op costs (one `GET` per miss instead of per read) but does not add read parallelism. Edge read replicas are `replicated-refs-edge` / Cache API, not this.
- R2 `get` on miss counts against the 30 s wall clock of the enclosing request only once; the 128 MB memory limit, not CPU time, is the constraint here. Deltified objects (`ofs-delta` / `ref-delta`) are not cached in delta form; the miss path assumes R2 stores fully resolved loose objects, which `streaming-pack-parser` must guarantee.

## Depends on
- repo-do-ref-authority
- refs-sqlite-objects-r2
- content-addressed-r2-keys
- streaming-pack-parser (must write resolved, zlib'd loose objects with `kind`/`size` custom metadata)
