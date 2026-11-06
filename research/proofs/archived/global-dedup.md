> Idea #38 · wild · verdict: **lands with caveats** · feasibility 4/5 · reliability 3/5 · correctness 3/5 · effort: weeks
> Proof: [proofs/global-dedup.md](./global-dedup.md) · Review: [reviews/global-dedup.md](../../reviews/archived/global-dedup.md)

# Deduplication across all repos in one content-addressed bucket

## Mechanism

A push hits `POST /:owner/:repo/git-receive-pack` on the Worker, which streams the packfile through the pack parser and, for every fully resolved object (deltas applied), calls `admit()` on that repo's Durable Object. `admit()` computes the git SHA-1 over `"<type> <len>\0<content>"`, checks the repo's own `objects` table in DO SQLite, then does one `env.OBJECTS.head("o/<sha>")` against a single account-wide R2 bucket whose keys carry no repo name; only on a miss does it `put` the object, stored as the bare zlib stream of the content (the exact bytes a PACK entry carries). Every repo that contains the object owns only a ~60-byte row `(sha, type, size)` in its own SQLite; the bytes exist once in R2. Fetch/clone (`POST /git-upload-pack`) asks the DO for reachable SHAs from its *own* index and assembles a PACK by writing a header, then for each object a type/size varint header plus the R2 body copied verbatim, then the SHA-1 trailer; no recompression, no repo prefix, so `lodash/lodash.js` pushed by ten thousand repos is one R2 object.

## Primitives

- Durable Objects with SQLite storage (`ctx.storage.sql.exec`) — GA. One DO per repo holds the membership index and refs.
- R2 object storage via Workers binding (`head`, `put`, `get`, `customMetadata`) — GA. One bucket, key = `o/<sha1>`.
- `CompressionStream("deflate")` / `DecompressionStream("deflate")` — GA in Workers; note "deflate" here is RFC 1950 zlib-wrapped, which is exactly git's on-disk/pack encoding.
- `crypto.subtle.digest("SHA-1")` — GA; one-shot only (see limits).
- `TransformStream` / streaming `Response` bodies — GA.
- DO alarms (`ctx.storage.setAlarm`) — GA; used only by the optional global-GC shard, not by the hot path.
- Not used and deliberately avoided: R2 conditional `onlyIf: { etagDoesNotMatch: "*" }` (If-None-Match:* on put). It exists on the S3 API; support via the Workers binding is not something I could verify here, and it is not needed because identical bytes under an identical key make a racing double-put harmless.

## Proof code

```typescript
// Global content-addressed store. R2 key is "o/<sha1>" with NO repo in it.
// Body = zlib(content) exactly as a PACK entry carries it; git type and
// inflated size live in customMetadata so pack assembly is pure concat.
interface Env { OBJECTS: R2Bucket; REPO: DurableObjectNamespace }
const TYPE_NAME = ["", "commit", "tree", "blob", "tag"] as const;

export class RepoDO extends DurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS objects (sha TEXT PRIMARY KEY, type INTEGER NOT NULL, size INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS refs    (name TEXT PRIMARY KEY, sha TEXT NOT NULL)`);
  }

  // Called by the streaming pack parser once per RESOLVED object (ofs-delta /
  // ref-delta already applied), i.e. content is the full inflated object body.
  async admit(type: 1 | 2 | 3 | 4, content: Uint8Array): Promise<"present" | "linked" | "new"> {
    const sha = await gitSha1(type, content);
    const sql = this.ctx.storage.sql;
    if (sql.exec("SELECT 1 FROM objects WHERE sha = ?", sha).toArray().length) return "present";
    const key = "o/" + sha;
    const global = await this.env.OBJECTS.head(key);            // 1 Class B op
    if (!global) {                                              // 1 Class A op, only ever once per SHA account-wide
      await this.env.OBJECTS.put(key, await zlib(content), {
        customMetadata: { t: String(type), n: String(content.byteLength) },
      });
    }
    sql.exec("INSERT OR IGNORE INTO objects (sha, type, size) VALUES (?, ?, ?)", sha, type, content.byteLength);
    return global ? "linked" : "new";
  }

  // upload-pack side: `shas` comes from THIS repo's reachability walk over its
  // own objects/refs tables (want-have-negotiation), never from the bucket.
  packResponse(shas: string[]): Response {
    const { readable, writable } = new TransformStream<Uint8Array>();
    const bucket = this.env.OBJECTS;
    const sql = this.ctx.storage.sql;
    (async () => {
      const w = writable.getWriter();
      const trailer = new Sha1Incremental();                    // ~60-line JS SHA-1; subtle.digest is one-shot
      const emit = async (b: Uint8Array) => { trailer.update(b); await w.write(b); };
      const hdr = new Uint8Array(12);                            // "PACK" + version 2 + object count
      hdr.set([0x50, 0x41, 0x43, 0x4b]);
      new DataView(hdr.buffer).setUint32(4, 2); new DataView(hdr.buffer).setUint32(8, shas.length);
      await emit(hdr);
      for (const sha of shas) {
        const row = sql.exec("SELECT type, size FROM objects WHERE sha = ?", sha).one();
        if (!row) throw new Error("index says reachable but not a member: " + sha);   // membership is per repo
        const obj = await bucket.get("o/" + sha);
        if (!obj) throw new Error("global object missing: " + sha);                     // GC bug, see limits
        await emit(entryHeader(row.type as number, row.size as number));               // undeltified entry
        for await (const chunk of obj.body) await emit(chunk);                          // zlib bytes verbatim
      }
      await w.write(trailer.digest());                                                  // 20-byte pack SHA-1
      await w.close();
    })();
    return new Response(readable, { headers: { "content-type": "application/x-git-upload-pack-result" } });
  }
}

// git object id = SHA-1("<type> <len>\0" + content)
async function gitSha1(type: number, content: Uint8Array): Promise<string> {
  const head = new TextEncoder().encode(`${TYPE_NAME[type]} ${content.byteLength}\0`);
  const buf = new Uint8Array(head.length + content.length); buf.set(head); buf.set(content, head.length);
  return [...new Uint8Array(await crypto.subtle.digest("SHA-1", buf))].map(b => b.toString(16).padStart(2, "0")).join("");
}

// CompressionStream("deflate") is RFC 1950 (zlib header + adler32) = what git packs contain.
async function zlib(content: Uint8Array): Promise<Uint8Array> {
  const cs = new CompressionStream("deflate");
  const r = new Response(new Blob([content]).stream().pipeThrough(cs));
  return new Uint8Array(await r.arrayBuffer());
}

// PACK entry header: MSB-continued varint; first byte = C TTT SSSS, rest = C SSSSSSS.
function entryHeader(type: number, size: number): Uint8Array {
  const out: number[] = [];
  let b = (type << 4) | (size & 0x0f); size >>>= 4;
  while (size > 0) { out.push(b | 0x80); b = size & 0x7f; size >>>= 7; }
  out.push(b);
  return Uint8Array.from(out);
}
declare class Sha1Incremental { update(b: Uint8Array): void; digest(): Uint8Array }
```

## Why it works

- Git already content-addresses everything: the SHA-1 of `"blob 12345\0<bytes>"` is identical no matter which repo it lives in, so a global key `o/<sha>` is dedup for free. There is no fuzzy matching; "same file" is exactly "same key".
- `git-receive-pack` promises nothing to the client about storage layout. It only has to ingest the pack (resolving `ofs-delta`/`ref-delta` against objects the repo already has), then report per-ref `ok`/`ng` in pkt-line. Where the resolved bytes land, and whether they were already there, is invisible on the wire, so a hit in the global bucket is a legitimate no-op.
- `git-upload-pack` clients (`index-pack` on receive) verify only two things: each entry's inflated content hashes to its SHA-1, and the trailing 20 bytes equal the SHA-1 of the whole pack. Neither depends on which zlib bytes produced the content or on deltas being present, so a pack of undeltified entries whose zlib bodies are copied byte-for-byte from R2 is valid. Storing the body as `zlib(content)` (not the loose-object `zlib("blob N\0" + content)`) is what makes copy-without-recompression legal.
- The two indexes have different jobs and must stay separate: the bucket answers "do the bytes exist", the repo DO answers "does this repo own it". `have`/`want` negotiation, reachability, and thin-pack delta bases all read the DO table, so a tenant never gets an object it did not push or fetch through its own refs, and R2 existence never leaks into protocol responses.
- Idempotency handles concurrency without coordination: two repos admitting the same new SHA at the same moment both `put` identical bytes under one key; R2 keeps one object. `content-addressed-r2-keys` already required this property for retried pushes.
- `cow-forks` collapses into a special case: a fork is a copy of the parent's `objects` rows (SQLite to SQLite), zero R2 traffic.

## Known limits

- Deletion is the real cost of the idea. "Stored exactly once" also means "owned by everyone", so a per-repo `gc-and-repack-alarm` must never delete from the bucket. Reclaiming space needs a global refcount or mark-and-sweep: e.g. 256 refcount DOs sharded by SHA prefix, incremented in `admit` and decremented on repo GC, with an alarm that deletes at zero. That is a second cross-DO write on every new object and a correctness trap (a crash between the R2 put and the refcount increment). The honest v1 is append-only: never delete, accept ~$0.015/GB-month.
- Existence oracle: `admit` returns "linked" when another tenant already holds the content. That result must not surface to the client (it does not in the code above), but push timing differs between `head`-hit and `head`+`put`, so an attacker who can guess a secret file's exact bytes gets a timing side channel confirming some repo contains it. Mitigation is to always `put` (costs a Class A op per object, removes the "once" write saving but not the storage saving) or to pad timing; I hand-waved this.
- R2 request cost dominates small objects: one `head` (Class B, $0.36/M) plus one `put` (Class A, $4.50/M) per new object, and one `get` per object on clone. A Linux-kernel-sized push (~10M objects) is ~$45 in puts and a 10M-`get` clone is ~$3.60 and far beyond one request's 30s CPU budget without `precomputed-clone-pack`. Practical hybrid: objects under ~4KB stay in the repo DO's SQLite (not globally deduped), only larger blobs go global. Most of lodash's small files would not be deduped under that rule; the big ones would.
- No deltas in the global store. Git repos get most of their compression from deltas between versions of a file; storing every version as a full zlib object trades delta compression for cross-repo dedup. For forks and vendored dependencies dedup wins; for a single repo with long history it loses, possibly by 5-10x. Clone packs are also undeltified, so wire size is larger than what GitHub sends. Layering `pinned-delta-bases` on top helps fetch, not storage.
- Single-DO throughput: every object of a push funnels through one DO doing SQLite lookups and R2 calls serially; ~100-300 objects/s realistic unless `admit` is batched (one `SELECT ... IN (...)` per pack, parallel `head`s). Batching is straightforward but not shown.
- `crypto.subtle.digest` cannot stream, so the pack trailer needs a hand-written incremental SHA-1 (declared, not implemented above). DO memory (128MB) is fine since objects stream through, but `admit` holds one fully resolved object in memory; a 500MB blob does not fit and needs `native-lfs` or `presigned-direct-upload`.
- Bucket-level blast radius: one bucket, one namespace, one accidental `delete` affects every repo. R2 object versioning is not GA on the Workers binding, so a soft-delete convention (rename to `tomb/<sha>`) is the only guard.
- SHA-1 collisions (SHAttered) would merge two repos' distinct objects into one key. Same exposure git itself has; SHA-256 repos would need a parallel `o256/` prefix.

## Depends on

- content-addressed-r2-keys
- streaming-pack-parser
- refs-sqlite-objects-r2
- repo-do-ref-authority
- want-have-negotiation
- (conflicts with, must be adapted) gc-and-repack-alarm
