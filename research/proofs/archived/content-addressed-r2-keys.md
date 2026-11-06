> Idea #5 · foundation · verdict: **lands with caveats** · feasibility 4/5 · reliability 4/5 · correctness 4/5 · effort: days
> Proof: [proofs/content-addressed-r2-keys.md](./content-addressed-r2-keys.md) · Review: [reviews/content-addressed-r2-keys.md](../../reviews/archived/content-addressed-r2-keys.md)

# Content-addressed R2 keys

## Mechanism
`POST /:owner/:repo/git-receive-pack` lands on the stateless Worker, which walks the PACK body object by object (that walk is idea `streaming-pack-parser`; here we only consume its output: `{type, content}` per fully-resolved object). For each object the Worker rebuilds git's loose header `"<type> <size>\0"`, SHA-1s header+content in the Worker (`crypto.subtle.digest`), and PUTs the uncompressed header+content to R2 at `objects/<owner>/<repo>/<sha>` with R2's `sha1:` integrity option so R2 itself refuses a body whose hash is not the key. Because the key is a pure function of the bytes, a retried, duplicated, or concurrently-racing push writes the identical value to the identical key and the outcome is indistinguishable from a single write; the repo DO only learns `(sha, type, size)` rows afterwards, and only advances refs once every sha in the push is known to exist (`two-phase-push`).

## Primitives
- R2 bucket binding: `env.BUCKET.put(key, body, { sha1 })`, `.head(key)`, `.get(key, { range })` -- GA
- R2 put integrity option (`md5`/`sha1`/`sha256`) -- GA
- R2 conditional put `onlyIf: { etagDoesNotMatch }` -- GA, but "if-not-exists" (`*`) semantics on the binding are not clearly documented, so the proof does NOT depend on it
- Durable Object with SQLite storage (`ctx.storage.sql.exec`) for the per-repo object index -- GA
- Web Crypto `crypto.subtle.digest("SHA-1")` in Workers -- GA (SHA-1 is allowed for digest, only disallowed for signing)
- `DecompressionStream("deflate")` for inflating pack entries -- GA (used upstream in the parser, not here)

## Proof code
```typescript
// objects.ts -- shared by the receive-pack Worker and the repo DO.
type GitType = "commit" | "tree" | "blob" | "tag";
type Env = { BUCKET: R2Bucket; REPO: DurableObjectNamespace };

const enc = new TextEncoder();
const hex = (b: ArrayBuffer) => [...new Uint8Array(b)].map(x => x.toString(16).padStart(2, "0")).join("");

// git object id = SHA1("<type> <len>\0" + content). We store exactly those bytes,
// uncompressed, so the R2 body's SHA-1 IS the git oid.
export async function looseBytes(type: GitType, content: Uint8Array) {
  const header = enc.encode(`${type} ${content.byteLength}\0`);
  const body = new Uint8Array(header.byteLength + content.byteLength);
  body.set(header, 0); body.set(content, header.byteLength);
  const oid = hex(await crypto.subtle.digest("SHA-1", body));
  return { oid, body };
}

export const objKey = (owner: string, repo: string, oid: string) =>
  `objects/${owner}/${repo}/${oid.slice(0, 2)}/${oid.slice(2)}`;

/** Idempotent write. Returns true if bytes were uploaded, false if already present. */
export async function putObject(env: Env, owner: string, repo: string, type: GitType, content: Uint8Array) {
  const { oid, body } = await looseBytes(type, content);
  const key = objKey(owner, repo, oid);
  if (await env.BUCKET.head(key)) return { oid, uploaded: false };      // cheap Class B op
  // R2 recomputes SHA-1 over the body and rejects the PUT if it differs -> the key
  // can never point at bytes that do not hash to it, even if our code is buggy.
  await env.BUCKET.put(key, body, {
    sha1: oid,
    httpMetadata: { contentType: "application/x-git-object" },
    customMetadata: { type, size: String(content.byteLength) },
  });
  return { oid, uploaded: true };
}

// receive-pack Worker: consume resolved objects from the streaming pack parser.
// Losing the connection mid-push and re-running this loop is harmless.
export async function ingestPack(env: Env, owner: string, repo: string, objects: AsyncIterable<{ type: GitType; content: Uint8Array }>) {
  const seen: { oid: string; type: GitType; size: number }[] = [];
  for await (const o of objects) {
    const { oid } = await putObject(env, owner, repo, o.type, o.content);
    seen.push({ oid, type: o.type, size: o.content.byteLength });
  }
  // Tell the repo DO which oids now exist; it upserts, so duplicates are no-ops too.
  const stub = env.REPO.get(env.REPO.idFromName(`${owner}/${repo}`));
  await stub.fetch("https://do/objects", { method: "POST", body: JSON.stringify(seen) });
  return seen;
}

// Repo DO: object index in SQLite (refs live in the same DO, see refs-sqlite-objects-r2).
export class RepoDO implements DurableObject {
  constructor(private ctx: DurableObjectState, private env: Env) {
    ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS objects (
      oid TEXT PRIMARY KEY, type TEXT NOT NULL, size INTEGER NOT NULL, seen_at INTEGER NOT NULL)`);
  }
  async fetch(req: Request) {
    if (req.method === "POST" && new URL(req.url).pathname === "/objects") {
      const rows = await req.json<{ oid: string; type: string; size: number }[]>();
      const now = Date.now();
      for (const r of rows)
        this.ctx.storage.sql.exec(
          "INSERT INTO objects (oid,type,size,seen_at) VALUES (?,?,?,?) ON CONFLICT(oid) DO NOTHING",
          r.oid, r.type, r.size, now);
      return new Response("ok");
    }
    return new Response("not found", { status: 404 });
  }
}

// Serving side (upload-pack): the stored body is already "<type> <len>\0content", so a
// pack entry is  varint(type,len) + deflate(content)  where content = body after the NUL.
export async function readObject(env: Env, owner: string, repo: string, oid: string) {
  const obj = await env.BUCKET.get(objKey(owner, repo, oid));
  if (!obj) return null;
  const body = new Uint8Array(await obj.arrayBuffer());
  const nul = body.indexOf(0);
  const [type] = new TextDecoder().decode(body.subarray(0, nul)).split(" ");
  return { type: type as GitType, content: body.subarray(nul + 1) };
}
```

## Why it works
- git already names every object by SHA-1 of `"<type> <size>\0content"`; the R2 key mirrors that exactly, so the key space is git's own namespace. Two pushes containing the same blob (a retry, a force-push re-sending history, a fork) collapse to one key with byte-identical value -- there is no "last writer wins" hazard because all writers write the same bytes.
- `git-receive-pack` semantics are "objects first, then ref update, then `report-status`". Nothing on the wire is acknowledged until the ref update, so a client whose connection dropped simply re-pushes the whole pack; with content-addressed keys the second attempt re-HEADs and skips, and only the ref CAS in the DO is a real state change.
- Storing the loose form uncompressed lets R2's `sha1` integrity option enforce `key == hash(value)` at write time. A corrupt or malicious PUT under the wrong key is rejected by R2, not just by our code. (This is the one place git's format and an R2 feature line up perfectly.)
- The `type` and `size` needed to emit a pack entry header (varint type/len) or an `object-info` v2 reply are in `customMetadata` and in the DO's `objects` table, so ref-advance / connectivity checks never need to GET a body.
- The DO's `INSERT ... ON CONFLICT DO NOTHING` makes the index update idempotent for the same reason the R2 write is, so a crash between "R2 done" and "DO told" and a subsequent retry converge.
- Delta objects (`ofs-delta`, `ref-delta`) in the incoming pack must be resolved to full content BEFORE hashing -- git oids are over the full object, never the delta. That resolution is the parser's job; this idea consumes only resolved objects.

## Known limits
- Uncompressed storage: a loose blob in R2 is typically 2-4x larger than git's zlib'd form. The trade is R2 storage ($0.015/GB-month) for R2-native hashing and zero-CPU verification; serving a pack still costs a `CompressionStream("deflate")` per object in the Worker. A variant that stores zlib bytes and puts the oid in `customMetadata` gives up R2-side verification.
- One R2 object per git object is expensive for object count, not bytes: a 50k-object initial push is 50k HEADs (Class B, $0.36/M) + 50k PUTs (Class A, $4.50/M) and 50k sequential round-trips, well past the 30s Worker wall/CPU budget unless batched with high concurrency (`Promise.all` in chunks of ~50) and/or the push is broken into the `two-phase-push` resumable manifest. Realistic single-request ceiling is a few thousand objects; bigger pushes need `precomputed-clone-pack`-style pack-in-R2 storage with per-object range offsets, keyed by pack sha, not per-object keys.
- HEAD-then-PUT is a benign race, not an atomic create. R2 `onlyIf: { etagDoesNotMatch }` exists but "does not exist" (`*`) semantics on the Workers binding are hand-waved here; not needed for correctness because concurrent writers write identical bytes.
- The Worker must hold one full resolved object in memory to hash it (a 100MB blob = 100MB+ in a 128MB isolate). Streaming SHA-1 is not available in Web Crypto; large blobs need chunked hashing in JS/Wasm or LFS (`native-lfs`).
- Per-repo key prefix means no cross-repo dedup; global `objects/<sha>` (idea `global-dedup`) is a one-line key change but couples deletion/GC across tenants.
- The DO `objects` table is an index, not the truth; if R2 and the DO disagree (crash mid-ingest), a connectivity check that HEADs R2 for missing rows is required before advancing refs.

## Depends on
- streaming-pack-parser (produces resolved `{type, content}` objects; deltas must be applied before hashing)
- refs-sqlite-objects-r2 (the DO that owns the object index and refs)
- repo-do-ref-authority (single DO per repo that does the ref CAS after objects land)
