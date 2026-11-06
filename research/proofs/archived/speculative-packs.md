> Idea #35 · wild · verdict: **risky** · feasibility 3/5 · reliability 4/5 · correctness 2/5 · effort: weeks
> Proof: [proofs/speculative-packs.md](./speculative-packs.md) · Review: [reviews/speculative-packs.md](../../reviews/archived/speculative-packs.md)

# Speculative packs

## Mechanism
Every protocol-v2 `fetch` that the repo DO answers is logged as `(client_key, ref, delivered_tip)` in DO SQLite; the prediction is trivial and git-exact: a client that was just handed tip `A` for `refs/heads/main` will, on its next fetch of that ref, send `want <new tip>` and `have A`. So when `git-receive-pack` moves `main` from `A` to `B`, the DO sets an alarm ~100 ms out; the alarm handler takes the distinct `delivered_tip`s of recent fetchers of that ref, and for each one builds the incremental pack `A..B` once (commit-graph walk in SQLite, one R2 range read per object from the pack index) and stores it under `spec/<A>..<B>.pack` in R2, mirrored into SQLite BLOB chunks when it is small. The next `POST /<owner>/<repo>/git-upload-pack` whose wants/haves match `(B, A)` is answered straight from the cache as `acknowledgments` + `ready` + sideband `packfile`, with zero object-level R2 reads and no delta work on the request path.

## Primitives
- Durable Object with SQLite storage (`ctx.storage.sql.exec`) - GA
- DO alarms (`ctx.storage.setAlarm`, `alarm()`) - GA
- R2 `put` / `get` with `range: {offset, length}` - GA; R2 lifecycle rule to expire the `spec/` prefix after 7 days - GA
- `crypto.DigestStream("SHA-1")` for the streaming pack trailer - Cloudflare-specific (non-standard WebCrypto extension, but GA on Workers)
- `TransformStream` / `ReadableStream` piping for the sideband-64k framing - GA
- `DecompressionStream("deflate")` is only needed on the miss path (idea #4), not here

## Proof code
```typescript
import { DurableObject } from "cloudflare:workers";

type Env = { BUCKET: R2Bucket };
const enc = new TextEncoder();
const pkt = (s: string) => enc.encode((s.length + 4).toString(16).padStart(4, "0") + s);
const FLUSH = enc.encode("0000"), DELIM = enc.encode("0001");
const SPEC_MAX = 32 << 20;                 // never speculate packs bigger than 32 MB
const INLINE_MAX = 4 << 20;                // mirror packs <= 4 MB into SQLite chunks

export class RepoDO extends DurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS refs(name TEXT PRIMARY KEY, oid TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS fetch_log(client TEXT, ref TEXT, delivered TEXT, ts INTEGER,
        PRIMARY KEY(client, ref));
      CREATE TABLE IF NOT EXISTS spec_packs(have TEXT, want TEXT, size INTEGER, hits INTEGER DEFAULT 0,
        created INTEGER, PRIMARY KEY(have, want));
      CREATE TABLE IF NOT EXISTS spec_chunks(have TEXT, want TEXT, seq INTEGER, bytes BLOB,
        PRIMARY KEY(have, want, seq));
      CREATE TABLE IF NOT EXISTS pending_prewarm(ref TEXT PRIMARY KEY, old TEXT, new TEXT);`);
  }

  // POST /git-upload-pack, body already pkt-line decoded by the Worker into {wants, haves, thin}.
  async uploadPack(client: string, ref: string, wants: string[], haves: string[]): Promise<Response> {
    const sql = this.ctx.storage.sql;
    const want = wants[0];
    // Cache hit iff some have is exactly a tip we previously delivered for this want.
    const hit = sql.exec<{ have: string; size: number }>(
      `SELECT have, size FROM spec_packs WHERE want = ? AND have IN (${haves.map(() => "?").join(",")})`,
      want, ...haves).toArray()[0];
    const body = hit ? this.cachedPack(hit.have, want, hit.size) : await this.buildPack(haves, want); // miss: idea #56 + #4
    if (hit) sql.exec("UPDATE spec_packs SET hits = hits + 1 WHERE have = ? AND want = ?", hit.have, want);
    sql.exec("INSERT OR REPLACE INTO fetch_log VALUES (?,?,?,?)", client, ref, want, Date.now());

    // v2 fetch response: acknowledgments (ACK for the have we matched, ready) | delim | packfile in sideband-64k.
    const head = new Blob([pkt("acknowledgments\n"), ...(hit ? [pkt(`ACK ${hit.have}\n`)] : []),
      pkt("ready\n"), DELIM, pkt("packfile\n")]);
    const sideband = new TransformStream<Uint8Array, Uint8Array>({
      transform(chunk, c) {
        for (let i = 0; i < chunk.length; i += 65515) {
          const part = chunk.subarray(i, i + 65515);
          c.enqueue(pkt("")); // placeholder length below
          c.enqueue(enc.encode((part.length + 5).toString(16).padStart(4, "0") + "\x01")); c.enqueue(part);
        }
      },
      flush(c) { c.enqueue(FLUSH); },
    });
    const out = new Blob([head]).stream().pipeThrough(new IdentityTransform()) as ReadableStream;
    return new Response(concat(out, body.pipeThrough(sideband)),
      { headers: { "content-type": "application/x-git-upload-pack-result", "cache-control": "no-cache" } });
  }

  // receive-pack (idea #6, phase two) calls this after the ref CAS succeeds.
  async refMoved(ref: string, oldOid: string, newOid: string) {
    this.ctx.storage.sql.exec("INSERT OR REPLACE INTO pending_prewarm VALUES (?,?,?)", ref, oldOid, newOid);
    if ((await this.ctx.storage.getAlarm()) == null) await this.ctx.storage.setAlarm(Date.now() + 100);
  }

  async alarm() {
    const sql = this.ctx.storage.sql;
    for (const p of sql.exec<{ ref: string; new: string }>("SELECT ref, new FROM pending_prewarm").toArray()) {
      // Predicted haves = tips we delivered for this ref in the last 7 days, most popular first, at most 8.
      const tips = sql.exec<{ delivered: string }>(
        `SELECT delivered FROM fetch_log WHERE ref = ? AND delivered != ? AND ts > ?
         GROUP BY delivered ORDER BY COUNT(*) DESC LIMIT 8`, p.ref, p.new, Date.now() - 7 * 864e5).toArray();
      for (const { delivered } of tips) await this.speculate(delivered, p.new);
      sql.exec("DELETE FROM pending_prewarm WHERE ref = ?", p.ref);
    }
    sql.exec("DELETE FROM spec_packs WHERE created < ?", Date.now() - 7 * 864e5); // R2 side expires via lifecycle rule
    sql.exec("DELETE FROM spec_chunks WHERE (have, want) NOT IN (SELECT have, want FROM spec_packs)");
  }

  private async speculate(have: string, want: string) {
    const sql = this.ctx.storage.sql;
    if (sql.exec("SELECT 1 FROM spec_packs WHERE have=? AND want=?", have, want).toArray().length) return;
    // buildPack: commit-graph walk in SQLite (idea #56) -> list of (r2PackKey, offset, length) from the pack
    // index (idea #4) -> one env.BUCKET.get(key, {range}) per object, re-emitted as pack entries,
    // thin deltas only against objects reachable from `have`; PACK v2 header + SHA-1 trailer via DigestStream.
    const pack = await this.buildPack([have], want);
    const buf = new Uint8Array(await new Response(pack).arrayBuffer());
    if (buf.length > SPEC_MAX) return;
    await this.env.BUCKET.put(`spec/${have}..${want}.pack`, buf);
    if (buf.length <= INLINE_MAX)
      for (let i = 0, seq = 0; i < buf.length; i += 1 << 20, seq++)   // SQLite values are capped at 2 MB
        sql.exec("INSERT INTO spec_chunks VALUES (?,?,?,?)", have, want, seq, buf.subarray(i, i + (1 << 20)));
    sql.exec("INSERT INTO spec_packs VALUES (?,?,?,0,?)", have, want, buf.length, Date.now());
  }

  private cachedPack(have: string, want: string, size: number): ReadableStream<Uint8Array> {
    const env = this.env, sql = this.ctx.storage.sql;
    return new ReadableStream({
      async start(c) {
        const rows = sql.exec<{ bytes: ArrayBuffer }>(
          "SELECT bytes FROM spec_chunks WHERE have=? AND want=? ORDER BY seq", have, want).toArray();
        if (rows.length) { for (const r of rows) c.enqueue(new Uint8Array(r.bytes)); return c.close(); }
        const obj = await env.BUCKET.get(`spec/${have}..${want}.pack`);          // one GET, streamed through
        if (!obj) throw new Error("spec pack evicted; fall back to buildPack");
        for await (const chunk of obj.body) c.enqueue(chunk);
        c.close();
      },
    });
  }

  private async buildPack(haves: string[], want: string): Promise<ReadableStream<Uint8Array>> { /* idea #56/#4 */ throw 0; }
}
```

## Why it works
- git's own negotiation is the predictor. After a successful fetch the client updates `refs/remotes/origin/main` to the delivered tip, and the next `fetch` sends that tip as its first `have` (it is the ref-tip, so it is always in the first 32-have batch). The prediction is not a heuristic about behaviour, it is the protocol's state machine run one step ahead.
- Correctness does not depend on the prediction being exact. A cached pack `A..B` is a valid answer for any client whose haves *include* `A`, even if it also sends haves the server does not know (local-only branches) or knows but did not use: extra objects in a pack are harmless to `index-pack`, and the pack's thin deltas are only against objects reachable from `A`, which that client provably has. The DO only ACKs `A`, which is exactly what the v2 spec allows ("ACK for each have the server has").
- The v2 `fetch` response is one-shot when the server can say `ready`: `acknowledgments` (`ACK <oid>`, `ready`), delim `0001`, then `packfile` in sideband-64k band 1 with a `0000` flush. That shape is fixed by the protocol, so a pre-built pack can be wrapped at serve time without re-parsing it.
- Pack bytes are cacheable because the set `reachable(B) - reachable(A)` and the PACK v2 encoding (12-byte `PACK\0\0\0\2<count>` header, entries, 20-byte SHA-1 trailer) are deterministic for a given `(A, B)`; nothing about the response is per-client.
- Packs compose: if `main` moved `A -> B -> C` and only `A..B` and `B..C` are cached, `A..C` is their concatenated entry lists with a patched count and re-hashed trailer, since ofs-delta offsets are backward-relative and ref-deltas resolve by SHA. So the alarm never has to rebuild from scratch for stale fetchers; only the trailer needs recomputing (streamable with `DigestStream`).
- The alarm makes the push path free: `receive-pack` only inserts a row and arms one alarm; the 100 ms `setAlarm` coalesces a burst of pushes into one prewarm pass, and the DO serialises it with the next fetch so a hit is either fully built or a plain miss.

## Known limits
- The only fetchers this helps are single-ref trackers: CI runners, mirrors, deploy agents, and agent-harness DOs (grok-pi sessions). A developer with many local branches sends dozens of haves and different wants; the lookup still hits on the tracked tip, but the cached pack may be larger than the minimal one git would compute (it only subtracts `A`, not the developer's other branches).
- Pack bytes cross the DO heap once when built (`arrayBuffer` in `speculate`), so the 32 MB `SPEC_MAX` cap exists to stay well under the DO's 128 MB memory; big ref moves (a merge that touches 100 MB of new blobs) are simply not speculated and fall back to the miss path.
- SQLite chunks are capped at 4 MB per pack by choice (row values are limited to 2 MB, hence the 1 MB chunking) and total DO storage at 10 GB; the R2 copy is the real cache, so a hit that misses the inline chunks still costs exactly one Class B R2 op plus egress-free streaming.
- Prewarm cost is N R2 range reads per predicted pack (one per object) times up to 8 predicted haves per ref move; on a very active branch with many distinct stale fetchers that is real R2 request spend, so the `LIMIT 8` and 7-day window are load-bearing, not cosmetic.
- The alarm runs on the same single DO as fetch/push, so a long prewarm delays the next request on that repo; alarm handlers have the same 30 s CPU budget by default (raise `limits.cpu_ms`), and building 8 packs of 32 MB each can exceed it. In practice the alarm should build one pack per invocation and re-arm itself.
- Pack composition (`A..B ++ B..C`) is asserted, not shown in the proof code; the header count patch is trivial but the trailer re-hash forces a full pass over both packs.
- The `pkt("")` placeholder / `IdentityTransform` / `concat` helpers in the sideband writer are hand-waved; the framing itself (4-byte hex length, band byte `\x01`, 65515-byte payload max) is what matters.
- `buildPack` is a stub; it is ideas #56 and #4 and is what the miss path already needs.

## Depends on
- `want-have-negotiation` (#56) for the commit-graph walk that decides `reachable(B) - reachable(A)`
- `streaming-pack-parser` (#4) for the SQLite pack index that turns an OID into an R2 `(key, offset, length)` range
- `refs-sqlite-objects-r2` (#2), `repo-do-ref-authority` (#1), `protocol-v2-only` (#3)
- `two-phase-push` (#6) for the `refMoved` hook after the ref CAS
- composes with `pinned-delta-bases` (#8) and `in-do-object-cache` (#9), which cover the miss path this idea sidesteps
