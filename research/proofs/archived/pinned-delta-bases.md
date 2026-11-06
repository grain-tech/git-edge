> Idea #8 · foundation · verdict: **lands with caveats** · feasibility 4/5 · reliability 4/5 · correctness 3/5 · effort: weeks
> Proof: [proofs/pinned-delta-bases.md](./pinned-delta-bases.md) · Review: [reviews/pinned-delta-bases.md](../../reviews/archived/pinned-delta-bases.md)

# Delta bases pinned per repo

## Mechanism

When `POST /:owner/:repo/git-receive-pack` finishes indexing a push (`streaming-pack-parser`), the Worker hands the repo DO (`idFromName("owner/repo")`) two things besides the ref manifest: the zlib-compressed bytes of every object that sits at a "hot" path in the new tip (the tip commit, root tree, and any blob whose path is in a small allowlist such as `package.json`, `Gemfile.lock`, `README.md`) and, for every delta object in the incoming pack whose base is one of the DO's currently pinned oids, the raw delta payload exactly as the client compressed it. The DO writes both into SQLite tables `pins(oid, type, size, zbody, pinned_at_commit)` and `deltas(oid, base_oid, delta_size, zdelta)`, keeps `pins` under a byte budget, and arms an alarm that evicts LRU pins. On `POST /git-upload-pack` (`command=fetch`, args include `thin-pack` and `have <oid>`), the DO builds the pack in the isolate: each wanted object is emitted as an `OBJ_REF_DELTA` (type 7, 20-byte base sha, stored `zdelta`) when its base is provably in the client's have-closure, as a full entry straight from `pins.zbody` when pinned, and only otherwise via `env.BUCKET.get("objects/<owner>/<repo>/<sha>")` plus a fresh deflate. The pack checksum is a running `node:crypto` SHA-1 so the whole thing streams as the upload-pack response body.

## Primitives

- Durable Object (SQLite-backed) with `ctx.storage.sql.exec` for `pins` / `deltas` / `commit_graph` tables -- GA
- `ctx.storage.setAlarm` / `alarm()` for LRU eviction of pins to a byte budget -- GA
- R2 `env.BUCKET.get(key)` for the cold-path object body -- GA (R2 range reads are not needed here; loose objects are whole-object gets)
- `nodejs_compat`: `node:zlib` `deflateSync` for cold-path objects and `node:crypto` `createHash("sha1")` for the streaming pack trailer -- GA flag; both are Node ports inside workerd, verify on deploy
- `ReadableStream` / `TransformStream` for the upload-pack response body -- GA
- Workers `fetch` handler + DO stub (`env.REPO.get(id).fetch(...)`) -- GA

## Proof code

```typescript
import { DurableObject } from "cloudflare:workers";
import { deflateSync } from "node:zlib";
import { createHash } from "node:crypto";

const PIN_BUDGET = 24 * 1024 * 1024;       // stay well under DO 128MB; rows are <=2MB each (SQLite row limit)
const T = { commit: 1, tree: 2, blob: 3, tag: 4, REF_DELTA: 7 } as const;

/** git pack entry header: varint with type in bits 4-6 of the first byte, size continues 7 bits/byte. */
function entryHeader(type: number, size: number): Uint8Array {
  const out: number[] = [(type << 4) | (size & 0x0f)]; size >>= 4;
  while (size > 0) { out[out.length - 1] |= 0x80; out.push(size & 0x7f); size >>= 7; }
  return Uint8Array.from(out);
}
const hex2bin = (h: string) => Uint8Array.from(h.match(/../g)!.map((b) => parseInt(b, 16)));

export class RepoDO extends DurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS pins   (oid TEXT PRIMARY KEY, type INT, size INT, zbody BLOB,
                                         pinned_at_commit TEXT, last_used INT, bytes INT);
      CREATE TABLE IF NOT EXISTS deltas (oid TEXT PRIMARY KEY, base_oid TEXT, delta_size INT, zdelta BLOB);
      CREATE INDEX IF NOT EXISTS deltas_base ON deltas(base_oid);`);
  }

  /** Called by the receive-pack Worker after a push is indexed (see two-phase-push manifest). */
  async pinAndRecord(p: { tip: string; pins: { oid: string; type: number; size: number; zbody: Uint8Array }[];
                          deltas: { oid: string; baseOid: string; deltaSize: number; zdelta: Uint8Array }[] }) {
    const sql = this.ctx.storage.sql;
    for (const d of p.deltas)                       // only keep deltas whose base we actually hold
      if (sql.exec("SELECT 1 FROM pins WHERE oid=?", d.baseOid).toArray().length)
        sql.exec("INSERT OR REPLACE INTO deltas VALUES (?,?,?,?)", d.oid, d.baseOid, d.deltaSize, d.zdelta);
    for (const o of p.pins)
      sql.exec("INSERT OR REPLACE INTO pins VALUES (?,?,?,?,?,?,?)",
        o.oid, o.type, o.size, o.zbody, p.tip, Date.now(), o.zbody.byteLength);
    await this.ctx.storage.setAlarm(Date.now() + 60_000);
  }

  /** LRU-evict pins over budget; a delta whose base is evicted is useless, so drop it too. */
  async alarm() {
    const sql = this.ctx.storage.sql;
    let total = sql.exec<{ b: number }>("SELECT COALESCE(SUM(bytes),0) b FROM pins").one().b;
    for (const row of sql.exec<{ oid: string; bytes: number }>("SELECT oid, bytes FROM pins ORDER BY last_used ASC")) {
      if (total <= PIN_BUDGET) break;
      sql.exec("DELETE FROM deltas WHERE base_oid=?", row.oid);
      sql.exec("DELETE FROM pins WHERE oid=?", row.oid);
      total -= row.bytes;
    }
  }

  /** Protocol v2 `fetch` with thin-pack: `wants` already narrowed by want-have-negotiation. */
  fetchPack(wants: string[], clientHas: (oid: string) => boolean): ReadableStream<Uint8Array> {
    const sql = this.ctx.storage.sql, env = this.env, sha = createHash("sha1");
    const ts = new TransformStream<Uint8Array, Uint8Array>();
    const w = ts.writable.getWriter();
    const emit = async (b: Uint8Array) => { sha.update(b); await w.write(b); };
    (async () => {
      const hdr = new Uint8Array(12); hdr.set([0x50, 0x41, 0x43, 0x4b]);      // "PACK"
      new DataView(hdr.buffer).setUint32(4, 2); new DataView(hdr.buffer).setUint32(8, wants.length);
      await emit(hdr);
      for (const oid of wants) {
        const d = sql.exec<{ base_oid: string; delta_size: number; zdelta: ArrayBuffer }>(
          "SELECT base_oid, delta_size, zdelta FROM deltas WHERE oid=?", oid).toArray()[0];
        if (d && clientHas(d.base_oid)) {                                         // hot path 1: verbatim delta
          await emit(entryHeader(T.REF_DELTA, d.delta_size)); await emit(hex2bin(d.base_oid));
          await emit(new Uint8Array(d.zdelta)); continue;
        }
        const p = sql.exec<{ type: number; size: number; zbody: ArrayBuffer }>(
          "SELECT type, size, zbody FROM pins WHERE oid=?", oid).toArray()[0];
        if (p) {                                                                  // hot path 2: pinned full object
          sql.exec("UPDATE pins SET last_used=? WHERE oid=?", Date.now(), oid);
          await emit(entryHeader(p.type, p.size)); await emit(new Uint8Array(p.zbody)); continue;
        }
        const obj = await env.BUCKET.get(`objects/${this.repoKey}/${oid}`);      // cold path: one R2 GET
        if (!obj) throw new Error(`missing ${oid}`);
        const raw = new Uint8Array(await obj.arrayBuffer());                     // "<type> <size>\0" + content
        const nul = raw.indexOf(0), [typeName] = new TextDecoder().decode(raw.subarray(0, nul)).split(" ");
        const body = raw.subarray(nul + 1);
        await emit(entryHeader(T[typeName as keyof typeof T] as number, body.byteLength));
        await emit(new Uint8Array(deflateSync(body)));
      }
      await w.write(new Uint8Array(sha.digest()));                                // 20-byte pack trailer
      await w.close();
    })().catch((e) => w.abort(e));
    return ts.readable;
  }
  private get repoKey() { return this.ctx.id.name!; }                            // "owner/repo"
}
```

## Why it works

- `git push` sends a thin pack by default (`send-pack --thin`), so most changed blobs already arrive as `ofs-delta`/`ref-delta` against the previous version of the same file. The receive side (`streaming-pack-parser`) must resolve those against the base anyway; pinning the previous tip's hot objects in SQLite means that resolution is a local read instead of an R2 GET, and the delta bytes fall out for free to be stored in `deltas`.
- `git fetch` over protocol v2 sends `thin-pack` in its `fetch` args, and afterwards runs `index-pack --fix-thin`, which appends any `REF_DELTA` base the pack references from the client's own object store. So a server may emit `OBJ_REF_DELTA` (type 7, followed by the 20-byte base sha, then a zlib stream of the delta) against any object the client already has; the only correctness obligation is the `clientHas(base)` check, which resolves via the commit-graph table (`want-have-negotiation`): the base must be reachable from a `have` commit.
- A pack is just `"PACK"`, version 2, entry count, N entries, SHA-1 of everything before the trailer. Nothing in that format requires entries to come from one source, so mixing verbatim stored deltas, pre-compressed pinned bodies, and freshly deflated R2 objects in one stream is exactly what upstream `pack-objects` does when it "reuses" deltas from existing packs.
- Git delta payloads are self-describing (source size, target size, copy/insert ops) and the entry header carries the delta's own size, so re-emitting a delta requires no decoding; it is byte copying. The zlib stream boundary is also preserved because the parser stored exactly the consumed bytes.
- Pinned bodies are stored already zlib-compressed, which is the form a pack entry needs. R2 holds the uncompressed loose form (`content-addressed-r2-keys`), so the cold path pays a deflate per object; the hot paths pay none.
- The typical incremental fetch of an active repo is "a handful of commits, a few dozen tree entries, the lockfile and a couple of source files": precisely the set the push side just pinned, so a `git pull` a minute after someone's push completes with zero R2 reads for the objects it sends.

## Known limits

- SQLite-backed DO storage caps a row at 2MB, so blobs above roughly 1.9MB compressed cannot be pinned and always take the R2 path; the pin allowlist should skip them. The `pins` byte budget (24MB here) must also respect the 128MB DO isolate memory, since `fetchPack` materialises each entry in memory before writing it.
- `clientHas(base)` is the whole safety argument and is hand-waved to the commit-graph table from `want-have-negotiation`. If it says yes wrongly, the client's `index-pack --fix-thin` fails with "missing base" and the fetch aborts; the safe fallback is to emit the full object, never a delta, when unsure. A repo whose DO SQLite is wiped loses pins and deltas but nothing else, since every object also lives in R2.
- Only deltas the client chose on push are reused; the server never computes deltas itself. Two branches touching the same file, or a fetch that skips several pushes, will see full objects for anything whose stored delta chains to a base the client lacks (no delta-chain walking in this proof).
- The DO pack build is single-threaded per repo and serialised with pushes; a large fetch (hundreds of cold objects) costs one R2 Class B GET plus a `deflateSync` per object inside a single DO invocation, bounded by the 30 s CPU default (raise `limits.cpu_ms`, or hand large fetches to `precomputed-clone-pack`).
- `deflateSync` from `node:zlib` blocks the isolate while running; acceptable for small blobs, but a big cold blob stalls every other request on that repo DO for its duration.
- The pin allowlist is a heuristic (tip commit, root tree, named hot paths). Tracking real access frequency to choose pins is `in-do-object-cache`; this idea only claims the push-then-fetch-soon workload.

## Depends on

- streaming-pack-parser
- content-addressed-r2-keys
- refs-sqlite-objects-r2
- want-have-negotiation
- protocol-v2-only
