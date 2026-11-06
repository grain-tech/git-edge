> Idea #50 · wild · verdict: **lands with caveats** · feasibility 4/5 · reliability 3/5 · correctness 3/5 · effort: weeks
> Proof: [proofs/storage-tiering.md](./storage-tiering.md) · Review: [reviews/storage-tiering.md](../../reviews/archived/storage-tiering.md)

# Storage tiering by heat

## Mechanism
R2 is always the system of record (every object and pack lives at its content-addressed key); "tiering" is three read paths over one canonical copy plus a heat ledger. Every read the per-repo `RepoDO` performs on behalf of `git-upload-pack` (a pack slice for `fetch`, a loose object for delta-base lookup, `object-info`) goes through `RepoDO.read(key)`, which bumps `heat(key).last_touch` in DO SQLite and serves from: (1) HOT — a `hot` BLOB table in `ctx.storage.sql` holding pack-ready entries under 512 KiB; (2) WARM — `env.BUCKET.get(key, { range })` on R2 Standard; (3) COLD — the same `get` on an object whose `storageClass` is `"InfrequentAccess"`, which R2 serves with no rehydration delay but bills retrieval per GB. One DO alarm runs nightly: rows untouched for `HOT_TTL` are simply deleted from `hot` (R2 already has the bytes, so demotion is a `DELETE`, never a copy); keys untouched for `COLD_AFTER` and larger than `COLD_MIN_BYTES` are re-put with `storageClass: "InfrequentAccess"` by streaming `get().body` into `put()` since the Workers binding has no copy/change-class call. Promotion is the mirror: a COLD key touched `PROMOTE_HITS` times inside `PROMOTE_WINDOW` is re-put as `"Standard"` (hysteresis, because IA charges a 30-day minimum), and any WARM/COLD hit under the size cap is admitted to `hot`.

## Primitives
- Durable Objects, SQLite-backed (`ctx.storage.sql.exec`, BLOB columns, `heat` ledger) — GA
- DO alarms (`ctx.storage.setAlarm` / `alarm()`) for the nightly sweep — GA
- R2 binding `get` with `range`, `put` with `storageClass: "Standard" | "InfrequentAccess"`, `head().storageClass` — R2 is GA; the **Infrequent Access storage class is still marked beta** in Cloudflare docs (no SLA, pricing published)
- R2 lifecycle rules `storageClassTransitions` (age-since-upload only; used as a safety net, not the mechanism) — same beta caveat
- Response streams (`ReadableStream` piped from `R2ObjectBody.body` into `put`, and into the `packfile` section of the v2 fetch reply) — GA

## Proof code
```typescript
// RepoDO tiering. wrangler.jsonc: durable_objects.bindings [{name:"REPO",class_name:"RepoDO"}],
// migrations [{tag:"v1",new_sqlite_classes:["RepoDO"]}], r2_buckets [{binding:"BUCKET",bucket_name:"git"}]
const HOT_MAX       = 512 * 1024;            // DO SQLite row cap is 2 MB; keep hot entries small
const HOT_TTL       = 6 * 3600_000;          // untouched 6h -> drop from DO (R2 still has it)
const COLD_AFTER    = 45 * 86_400_000;       // untouched 45d -> InfrequentAccess (IA bills 30d minimum)
const COLD_MIN      = 1024 * 1024;           // IA ops cost ~2-3x Standard; only worth it for packs, not loose objs
const PROMOTE_HITS  = 3, PROMOTE_WINDOW = 3600_000;  // hysteresis so a single git log doesn't flap a pack
const SWEEP_MS      = 24 * 3600_000;

type Tier = "hot" | "warm" | "cold";

export class RepoDO implements DurableObject {
  constructor(private ctx: DurableObjectState, private env: { BUCKET: R2Bucket }) {
    ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS heat (
        key TEXT PRIMARY KEY, tier TEXT NOT NULL, bytes INTEGER NOT NULL,
        last_touch INTEGER NOT NULL, hits INTEGER NOT NULL DEFAULT 0, hits_since INTEGER NOT NULL DEFAULT 0);
      CREATE INDEX IF NOT EXISTS heat_touch ON heat(tier, last_touch);
      CREATE TABLE IF NOT EXISTS hot (key TEXT PRIMARY KEY, entry BLOB NOT NULL)`);
    ctx.blockConcurrencyWhile(async () => {
      if ((await ctx.storage.getAlarm()) === null) await ctx.storage.setAlarm(Date.now() + SWEEP_MS);
    });
  }

  /** Called on every push-side write (streaming-pack-parser) so the ledger knows the key exists. */
  recordWrite(key: string, bytes: number) {
    this.ctx.storage.sql.exec(
      "INSERT OR REPLACE INTO heat(key,tier,bytes,last_touch) VALUES(?,?,?,?)", key, "warm", bytes, Date.now());
  }

  /** Single read path for upload-pack: pack slice (range) or loose object. Returns bytes ready to splice
   *  into an outgoing PACK (entry header varint + zlib stream) or null if unknown. */
  async read(key: string, range?: { offset: number; length: number }): Promise<ReadableStream | null> {
    const sql = this.ctx.storage.sql, now = Date.now();
    const h = sql.exec<{ tier: Tier; bytes: number; hits: number; hits_since: number }>(
      "SELECT tier,bytes,hits,hits_since FROM heat WHERE key=?", key).toArray()[0];
    if (!h) return null;

    if (h.tier === "hot" && !range) {
      const row = sql.exec<{ entry: ArrayBuffer }>("SELECT entry FROM hot WHERE key=?", key).toArray()[0];
      if (row) { sql.exec("UPDATE heat SET last_touch=? WHERE key=?", now, key); return new Blob([row.entry]).stream(); }
    }

    // warm and cold are the same call; cold just costs retrieval $/GB and has no restore latency
    const obj = await this.env.BUCKET.get(key, range ? { range } : undefined);
    if (!obj) return null;

    // hysteresis window for cold -> warm promotion
    const inWindow = now - h.hits_since < PROMOTE_WINDOW;
    const hits = inWindow ? h.hits + 1 : 1;
    sql.exec("UPDATE heat SET last_touch=?, hits=?, hits_since=? WHERE key=?",
      now, hits, inWindow ? h.hits_since : now, key);

    if (h.tier === "cold" && hits >= PROMOTE_HITS) this.ctx.waitUntil(this.setClass(key, "Standard", "warm"));

    if (!range && h.bytes <= HOT_MAX) {                       // admit small objects to DO
      const buf = await obj.arrayBuffer();
      sql.exec("INSERT OR REPLACE INTO hot(key,entry) VALUES(?,?)", key, buf);
      sql.exec("UPDATE heat SET tier='hot' WHERE key=?", key);
      return new Blob([buf]).stream();
    }
    return obj.body;
  }

  /** Nightly demotion sweep. Hot->warm is a DELETE; warm->cold is a streamed re-put. */
  async alarm() {
    const sql = this.ctx.storage.sql, now = Date.now();
    sql.exec(`DELETE FROM hot WHERE key IN (SELECT key FROM heat WHERE tier='hot' AND last_touch<?)`, now - HOT_TTL);
    sql.exec(`UPDATE heat SET tier='warm' WHERE tier='hot' AND last_touch<?`, now - HOT_TTL);

    const stale = sql.exec<{ key: string }>(
      "SELECT key FROM heat WHERE tier='warm' AND bytes>=? AND last_touch<? LIMIT 50", COLD_MIN, now - COLD_AFTER).toArray();
    for (const { key } of stale) await this.setClass(key, "InfrequentAccess", "cold");   // 50/night bounds alarm wall time

    await this.ctx.storage.setAlarm(now + SWEEP_MS);
  }

  /** No copy API in the binding: stream get -> put with the new class. Content-addressed key => idempotent. */
  private async setClass(key: string, storageClass: "Standard" | "InfrequentAccess", tier: Tier) {
    const src = await this.env.BUCKET.get(key);
    if (!src || src.storageClass === storageClass) return;
    await this.env.BUCKET.put(key, src.body, { storageClass, customMetadata: src.customMetadata,
      httpMetadata: src.httpMetadata, md5: undefined });
    this.ctx.storage.sql.exec("UPDATE heat SET tier=?, hits=0 WHERE key=?", tier, key);
  }
}
```

## Why it works
- `git-upload-pack` never sees a tier: the `packfile` section of a v2 `fetch` response is a `PACK` header + entries + trailing SHA-1 regardless of whether an entry's bytes came from a SQLite BLOB, an R2 Standard range read, or an IA object. Entries are stored in their on-the-wire form (type/size varint + zlib), so no re-inflate/re-deflate happens on the hot path — same trick `in-do-object-cache` relies on.
- R2 IA has no restore step (unlike S3 Glacier), so a `have`/`want` negotiation that lands on a 2-year-old pack still answers in one round trip; cold only changes cost and a few ms of latency, never correctness or protocol timing (git clients have no timeout on the `packfile` section but will hang on a `sideband` stall).
- Content-addressed keys make the class change idempotent: re-`put` of the same bytes at the same key is safe if the alarm is retried, and a concurrent `fetch` reading the old object during the swap gets identical content.
- DO SQLite is the only place with a `last_touch` — R2 exposes `uploaded`, not last-access — so heat must be tracked by the DO that serves reads anyway; the ledger write rides on the same single-threaded DO that already serialises ref updates (`repo-do-ref-authority`).
- Demotion out of the DO costs nothing but a `DELETE`, because the DO tier is a cache over R2, never the owner. This is what keeps the "DO 10 GB / 2 MB per row" limits from ever being a data-loss concern.
- Hysteresis (`PROMOTE_HITS` inside `PROMOTE_WINDOW`, `COLD_AFTER` > IA's 30-day minimum) makes the tiering monotone under normal git traffic: a repo that is cloned once a quarter pays IA storage, one that is cloned daily never leaves Standard.

## Known limits
- As stated ("untouched objects migrate to IA, hot objects stay in DO") the idea is only half true in practice: R2 IA saves $0.005/GB-month over Standard but charges ~2-3x per operation plus $0.01/GB retrieval, so per-loose-object tiering *loses* money; the proof tiers only keys >= 1 MiB (packs, `precomputed-clone-pack` slices) and treats DO as a read cache rather than a storage tier. DO SQLite storage is ~13x the price of R2 per GB, so "stays in DO" is a latency win, not a cost win.
- R2 Infrequent Access is beta; the `storageClass` option and `R2Object.storageClass` are in the binding today but have no SLA.
- No copy/set-storage-class call in the Workers binding: warm<->cold moves stream the whole object through the DO. A 1 GB pack costs a Class A put and a full read; the alarm caps itself at 50 keys/night to stay within the 30 s CPU budget (IO is not CPU, but 50 x 1 GB is hours of wall time and DO alarm invocations are not meant for that — chunked multipart copy or the S3 `CopyObject` API with `x-amz-storage-class` from a plain Worker would be the real implementation).
- Heat lives only in the DO's SQLite; if `heat` rows are lost (repo DO recreated from `refs-sqlite-objects-r2` R2 state) every object is assumed warm and a rescan via `BUCKET.list()` + `head().storageClass` is needed to rebuild the ledger.
- Hot admissions are synchronous `arrayBuffer()` + SQLite insert on the read path; a burst of 10k distinct small objects (a full `git log -p`) will churn the `hot` table. A byte budget / LRU cap as in `in-do-object-cache` is needed and hand-waved here.
- R2 lifecycle `storageClassTransitions` fire on age-since-upload, so they cannot express "untouched since last read"; they are only useful as a backstop for repos whose DO never wakes.
- All reads of one repo funnel through one DO (single-DO throughput, ~hundreds of req/s); the `heat` UPDATE per read is cheap but is one more row write billed per fetch.

## Depends on
content-addressed-r2-keys, refs-sqlite-objects-r2, repo-do-ref-authority, in-do-object-cache, precomputed-clone-pack, gc-and-repack-alarm
