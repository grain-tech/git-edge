> Idea #55 · edge · verdict: **risky** · feasibility 3/5 · reliability 2/5 · correctness 2/5 · effort: weeks
> Proof: [proofs/gc-and-repack-alarm.md](./gc-and-repack-alarm.md) · Review: [reviews/gc-and-repack-alarm.md](../../reviews/archived/gc-and-repack-alarm.md)

# GC and repack as a DO alarm

## Mechanism
Every ref update on the repo DO (the `commit` step of two-phase push) bumps `refs_version` and arms `ctx.storage.setAlarm()` for a quiet window (e.g. 10 min after the last push); the alarm never needs an HTTP request. `alarm()` runs a resumable three-phase state machine persisted in DO SQLite: **mark** walks reachability from the `refs` table purely through the `objects(sha, type, links)` index that pushes already populate (zero R2 reads), inserting into a `marked` table in bounded batches; **pack** streams each marked object out of R2 (`objects/<sha>`, loose zlib), re-encodes it as a non-delta pack entry (varint type/size header + zlib body) into an R2 multipart upload at `packs/<repo>/<buildId>.pack` while a serialisable SHA-1 state accumulates the trailer; **sweep** deletes unmarked, pre-GC-timestamp objects from R2 in batches of 1000 and drops their index rows. Each alarm invocation does a bounded slice of work, writes its cursor (and multipart `uploadId`, part list, SHA-1 state) to SQLite, and re-arms `setAlarm(Date.now())` to continue, so a repack of any size survives the per-invocation CPU limit and DO eviction. When the pack is complete its trailer and covered ref tips are recorded so `precomputed-clone-pack` serves it, and older packs are deleted.

## Primitives
- Durable Objects with SQLite storage (`ctx.storage.sql.exec`) — GA
- DO alarms (`ctx.storage.setAlarm`, `alarm()`, automatic retry on throw) — GA
- R2 `get` (streamed body), `delete(string[])` up to 1000 keys per call — GA
- R2 multipart upload (`createMultipartUpload`, `resumeMultipartUpload(key, uploadId)`, `uploadPart`, `complete`) — GA; parts min 5 MiB except last
- `DecompressionStream("deflate")` / `CompressionStream("deflate")` (zlib framing, which is what git uses for both loose and pack objects) — GA
- `nodejs_compat` is *not* needed: `node:crypto` hashes cannot export state, so a small pure-JS SHA-1 with serialisable state is used instead
- Nothing beta.

## Proof code
```typescript
import { DurableObject } from "cloudflare:workers";
type Env = { BUCKET: R2Bucket };
type Gc = { phase: "mark" | "pack" | "sweep"; started: number; refsVer: number; build: string;
            uploadId?: string; parts: R2UploadedPart[]; count: number; sha1: Sha1State; buf: number[] };
const TYPE: Record<string, number> = { commit: 1, tree: 2, blob: 3, tag: 4 };
const BATCH = 500, PART = 5 * 1024 * 1024;

export class RepoDO extends DurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS refs    (name TEXT PRIMARY KEY, oid TEXT);
      CREATE TABLE IF NOT EXISTS meta    (k TEXT PRIMARY KEY, v TEXT);
      CREATE TABLE IF NOT EXISTS objects (sha TEXT PRIMARY KEY, type TEXT, size INTEGER,
                                          links TEXT, created_at INTEGER);      -- filled by two-phase-push commit
      CREATE TABLE IF NOT EXISTS marked  (sha TEXT PRIMARY KEY, done INTEGER DEFAULT 0);
      CREATE TABLE IF NOT EXISTS pack_builds (id TEXT PRIMARY KEY, key TEXT, sha1 TEXT, count INTEGER, built_at INTEGER)`);
  }

  // Called by the push commit path after CAS on refs: debounce GC to a quiet window.
  async onRefsChanged() {
    this.meta("refs_version", String(Number(this.meta("refs_version") ?? 0) + 1));
    if (!this.meta("gc")) await this.ctx.storage.setAlarm(Date.now() + 10 * 60_000);
  }

  async alarm() {
    const sql = this.ctx.storage.sql;
    let gc: Gc = JSON.parse(this.meta("gc") ?? "null") ?? this.startGc();
    if (gc.phase === "mark") {
      // Frontier = marked rows not yet expanded. Links come from the SQLite index, never from R2.
      const frontier = sql.exec<{ sha: string }>("SELECT sha FROM marked WHERE done = 0 LIMIT ?", BATCH).toArray();
      if (frontier.length === 0) { gc.phase = "pack"; }
      else for (const { sha } of frontier) {
        const row = sql.exec<{ links: string }>("SELECT links FROM objects WHERE sha = ?", sha).one();
        for (const child of JSON.parse(row.links) as string[])
          sql.exec("INSERT OR IGNORE INTO marked (sha) VALUES (?)", child);
        sql.exec("UPDATE marked SET done = 1 WHERE sha = ?", sha);
      }
    } else if (gc.phase === "pack") {
      const key = `packs/${this.ctx.id}/${gc.build}.pack`;
      const mpu = gc.uploadId ? this.env.BUCKET.resumeMultipartUpload(key, gc.uploadId)
                              : await this.env.BUCKET.createMultipartUpload(key);
      gc.uploadId = mpu.uploadId;
      if (gc.count === 0 && gc.buf.length === 0) {                // "PACK" + version 2 + object count (big-endian u32)
        const n = sql.exec<{ n: number }>("SELECT COUNT(*) n FROM marked").one().n;
        gc.buf.push(...[0x50, 0x41, 0x43, 0x4b, 0, 0, 0, 2, n >>> 24, (n >>> 16) & 255, (n >>> 8) & 255, n & 255]);
      }
      const batch = sql.exec<{ sha: string; type: string; size: number }>(
        `SELECT m.sha, o.type, o.size FROM marked m JOIN objects o USING (sha) WHERE m.done = 1 ORDER BY
         CASE o.type WHEN 'commit' THEN 0 WHEN 'tag' THEN 1 WHEN 'tree' THEN 2 ELSE 3 END, m.sha
         LIMIT ? OFFSET ?`, BATCH, gc.count).toArray();
      for (const o of batch) gc.buf.push(...(await this.packEntry(o)));   // non-delta entries only (no ofs-delta)
      gc.count += batch.length;
      while (gc.buf.length >= PART || (batch.length === 0 && gc.buf.length)) {
        const chunk = new Uint8Array(gc.buf.splice(0, PART));
        gc.sha1 = sha1Update(gc.sha1, chunk);                              // trailer = SHA-1 of everything before it
        gc.parts.push(await mpu.uploadPart(gc.parts.length + 1, chunk));
      }
      if (batch.length === 0) {
        await mpu.complete(gc.parts);                                       // pack body without its 20-byte trailer
        sql.exec("INSERT INTO pack_builds VALUES (?, ?, ?, ?, ?)", gc.build, key, sha1Hex(sha1Final(gc.sha1)), gc.count, Date.now());
        // precomputed-clone-pack appends the stored trailer when serving; record covered tips there too.
        gc.phase = "sweep";
      }
    } else {
      if (this.meta("refs_version") !== String(gc.refsVer)) return this.finish(true); // refs moved: re-mark, never delete on stale marks
      const dead = sql.exec<{ sha: string }>(
        `SELECT sha FROM objects WHERE created_at < ? AND sha NOT IN (SELECT sha FROM marked) LIMIT 1000`, gc.started).toArray();
      if (dead.length === 0) return this.finish(false);
      await this.env.BUCKET.delete(dead.map(d => `objects/${d.sha}`));      // content-addressed loose objects
      sql.exec(`DELETE FROM objects WHERE sha IN (${dead.map(() => "?").join(",")})`, ...dead.map(d => d.sha));
    }
    this.meta("gc", JSON.stringify(gc));
    await this.ctx.storage.setAlarm(Date.now());                           // continue in a fresh invocation
  }

  private startGc(): Gc {
    const sql = this.ctx.storage.sql;
    sql.exec("DELETE FROM marked");
    for (const r of sql.exec<{ oid: string }>("SELECT oid FROM refs").toArray())
      sql.exec("INSERT OR IGNORE INTO marked (sha) VALUES (?)", r.oid);    // roots = every ref tip (+ open pushes, omitted)
    return { phase: "mark", started: Date.now(), refsVer: Number(this.meta("refs_version") ?? 0),
             build: crypto.randomUUID(), parts: [], count: 0, sha1: sha1Init(), buf: [] };
  }

  // Loose object in R2 is zlib("<type> <size>\0" + body); a pack entry is varint(type,size) + zlib(body).
  private async packEntry(o: { sha: string; type: string; size: number }): Promise<Uint8Array> {
    const obj = await this.env.BUCKET.get(`objects/${o.sha}`);
    const raw = new Uint8Array(await new Response(obj!.body.pipeThrough(new DecompressionStream("deflate"))).arrayBuffer());
    const body = raw.subarray(raw.indexOf(0) + 1);
    const hdr: number[] = []; let s = o.size;
    let b = (TYPE[o.type] << 4) | (s & 15); s >>= 4;
    while (s > 0) { hdr.push(b | 0x80); b = s & 127; s >>= 7; } hdr.push(b);
    const z = new Uint8Array(await new Response(new Blob([body]).stream().pipeThrough(new CompressionStream("deflate"))).arrayBuffer());
    const out = new Uint8Array(hdr.length + z.length); out.set(hdr); out.set(z, hdr.length); return out;
  }

  private async finish(restart: boolean) {
    this.ctx.storage.sql.exec("DELETE FROM meta WHERE k = 'gc'");
    // delete pack_builds older than the newest one from R2 here (list + delete), then:
    if (restart) await this.ctx.storage.setAlarm(Date.now());
  }
  private meta(k: string, v?: string): string | undefined {
    const sql = this.ctx.storage.sql;
    if (v !== undefined) { sql.exec("INSERT OR REPLACE INTO meta VALUES (?, ?)", k, v); return v; }
    return sql.exec<{ v: string }>("SELECT v FROM meta WHERE k = ?", k).toArray()[0]?.v;
  }
}
// Sha1State = { h: number[5], len: number, tail: number[] } — a ~50-line pure-JS SHA-1 whose state survives
// JSON round-trips so the trailer hash can span alarm invocations. sha1Init/sha1Update/sha1Final/sha1Hex omitted.
```

## Why it works
- Git GC is exactly mark-and-sweep from the ref tips; because `two-phase-push` records each object's outgoing edges (commit→tree/parents, tree→entries) in the `objects` index at commit time, reachability is a pure SQLite graph walk with no R2 reads, and it is trivially resumable (the `marked.done` flag is the frontier).
- The output is a byte-exact git packfile: `PACK`, version 2, big-endian object count, then entries whose first byte packs type (bits 4-6) and the low 4 size bits with a continuation bit, then zlib bodies; commits-then-trees-then-blobs is git's own ordering so `precomputed-clone-pack` can carve a `blobless` slice by offset. Only non-delta entries are emitted, which any `git index-pack` accepts.
- The pack trailer is SHA-1 over the preceding bytes; keeping the hash state in SQLite means it is computed once while streaming and stored, so the pack can be served with the trailer appended and no second pass.
- Sweep only deletes objects with `created_at < gc.started` and unmarked, so objects landed by pushes during the GC (still pending or newly committed) are never touched; that is the same "grace period" rule as `git prune --expire`.
- The `refs_version` check before sweep closes git's classic race (a push that resurrects an orphaned commit between mark and prune): if any ref moved, the stale mark set is discarded and the walk restarts rather than deleting.
- Loose objects remain in R2 after repack, so readers never race a pack swap; deleting them is just the sweep of the *next* GC after a pack build records them as covered, which is how git handles `.keep`/ordering too.

## Known limits
- No delta compression: the "repack" concatenates full zlib objects, so the pack is as large as the loose set. Real ofs-delta generation needs a delta encoder (`wasm-git-core`) and a window search, which does not fit a 30 s CPU alarm without heavy chunking; this proof only reclaims R2 keys and produces a clone-servable pack.
- Every marked object is re-inflated and re-deflated once (loose header must be stripped). Storing loose objects already in pack-entry form (`content-addressed-r2-keys` variant) would make repack a pure byte copy.
- Alarm CPU limit (30 s default, up to 5 min with `limits.cpu_ms`) and the 128 MB DO memory bound the batch: `BATCH=500` objects and a 5 MiB part buffer per invocation; `gc.buf` as a JSON number array is wasteful (should be base64 or an R2 scratch key) and is a placeholder.
- R2 cost: one GET per reachable object per repack plus one PUT per 5 MiB part; a 1M-object repo repack is ~1M Class B ops. Should be gated on churn (e.g. loose count > N) rather than every quiet window.
- Single-DO throughput: the alarm shares the DO with pushes. Each invocation is short and yields, but a large mark walk adds latency to concurrent `commit` calls; the DO's input gate keeps it correct, not fast.
- `resumeMultipartUpload` uploads expire (R2 aborts incomplete multipart uploads after ~7 days by default); a GC that stalls that long must restart the pack phase.
- Open (uncommitted) pushes must be added as mark roots or excluded by `created_at`; handled by the timestamp here but not the manifest-root case, which is hand-waved.
- The pure-JS serialisable SHA-1 is asserted, not shown; `node:crypto` cannot export hash state.

## Depends on
- repo-do-ref-authority
- refs-sqlite-objects-r2
- content-addressed-r2-keys
- two-phase-push (supplies the `objects` index with links and `created_at`)
- precomputed-clone-pack (consumer of the produced pack and stored trailer)
