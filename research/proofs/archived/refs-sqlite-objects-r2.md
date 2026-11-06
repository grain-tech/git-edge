> Idea #2 · foundation · verdict: **lands with caveats** · feasibility 4/5 · reliability 2/5 · correctness 3/5 · effort: weeks
> Proof: [proofs/refs-sqlite-objects-r2.md](./refs-sqlite-objects-r2.md) · Review: [reviews/refs-sqlite-objects-r2.md](../../reviews/archived/refs-sqlite-objects-r2.md)

# Refs in DO SQLite, objects in R2

## Mechanism
`GET /:owner/:repo/info/refs?service=git-upload-pack` and `POST /:owner/:repo/git-receive-pack` land on the Worker, which routes to `env.REPO.idFromName("owner/repo")` — one `RepoDO` per repo. The DO keeps a `refs(name TEXT PRIMARY KEY, sha TEXT)` table plus a small `objects(sha, type, size)` index in `ctx.storage.sql`; every ref read/advertise/compare-and-swap is a local SQLite statement. Object bodies never live in the DO: a push streams its packfile through the Worker, each inflated object is `PUT` to R2 at `objects/<owner>/<repo>/<sha>` (immutable, idempotent), and only after all `put`s resolve does the DO run the ref update inside one `transactionSync`. A fetch reads the wanted SHAs from SQLite, then `env.BUCKET.get(key)` per object (or a range read into a precomputed pack) to build the response pack.

## Primitives
- Durable Objects with SQLite storage (`ctx.storage.sql.exec`, `transactionSync`) — GA
- DO alarms (`ctx.storage.setAlarm`) for sweeping orphaned pending objects — GA
- R2 (`env.BUCKET.put/get/head/list`, `get(key, { range })`) — GA
- Workers streams (`Request.body` ReadableStream, `Response` from a `ReadableStream`) — GA
- `DecompressionStream("deflate")` for zlib-wrapped git objects (each git object stream is zlib with the 2-byte header, which "deflate" mode accepts) — GA in workerd
- `crypto.subtle.digest("SHA-1")` for object ids — GA
- `nodejs_compat` (only if `zlib.inflateSync` is preferred for per-object inflate with an unknown compressed length) — GA

## Proof code
```typescript
// wrangler.jsonc: durable_objects.bindings [{name:"REPO",class_name:"RepoDO"}],
// migrations [{tag:"v1",new_sqlite_classes:["RepoDO"]}], r2_buckets [{binding:"BUCKET",bucket_name:"git-objects"}]
export interface Env { REPO: DurableObjectNamespace; BUCKET: R2Bucket }

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    // /:owner/:repo/(info/refs|git-upload-pack|git-receive-pack)
    const m = new URL(req.url).pathname.match(/^\/([^/]+)\/([^/]+?)(?:\.git)?\/(.+)$/);
    if (!m) return new Response("not found", { status: 404 });
    const [, owner, repo, rest] = m;
    const stub = env.REPO.get(env.REPO.idFromName(`${owner}/${repo}`));
    return stub.fetch(new Request(`https://do/${rest}`, req)); // DO stub keeps the streaming body
  },
};

export class RepoDO implements DurableObject {
  private prefix: string;
  constructor(private ctx: DurableObjectState, private env: Env) {
    this.prefix = ""; // set from first request; idFromName gives no name back
    ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS refs    (name TEXT PRIMARY KEY, sha TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS objects (sha TEXT PRIMARY KEY, type TEXT NOT NULL, size INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS pending (sha TEXT PRIMARY KEY, created_at INTEGER NOT NULL)`);
  }

  async fetch(req: Request): Promise<Response> {
    const url = new URL(req.url);
    this.prefix ||= req.headers.get("x-repo-prefix") ?? "objects/default/"; // Worker sets owner/repo here
    if (url.pathname === "/info/refs") return this.advertise(url.searchParams.get("service")!);
    if (url.pathname === "/git-receive-pack") return this.receivePack(req);
    if (url.pathname === "/git-upload-pack") return this.uploadPack(req);
    return new Response("not found", { status: 404 });
  }

  // ---- refs: hot, tiny, local ------------------------------------------------
  private advertise(service: string): Response {
    const refs = this.ctx.storage.sql.exec<{ name: string; sha: string }>("SELECT name, sha FROM refs ORDER BY name").toArray();
    const caps = "report-status delete-refs ofs-delta agent=git-edge";
    let body = pkt(`# service=${service}\n`) + "0000";
    if (refs.length === 0) body += pkt(`${"0".repeat(40)} capabilities^{}\0${caps}\n`);
    refs.forEach((r, i) => (body += pkt(`${r.sha} ${r.name}${i === 0 ? "\0" + caps : ""}\n`)));
    return new Response(body + "0000", { headers: { "content-type": `application/x-${service}-advertisement` } });
  }

  /** Compare-and-swap every ref line from the push in one SQLite transaction. */
  private updateRefs(cmds: { old: string; nu: string; name: string }[]): string[] {
    const sql = this.ctx.storage.sql;
    return this.ctx.storage.transactionSync(() =>
      cmds.map(({ old, nu, name }) => {
        const cur = sql.exec<{ sha: string }>("SELECT sha FROM refs WHERE name = ?", name).toArray()[0]?.sha ?? "0".repeat(40);
        if (cur !== old) return `ng ${name} fetch first`;                       // stale client, like git's own check
        if (nu === "0".repeat(40)) sql.exec("DELETE FROM refs WHERE name = ?", name);
        else {
          if (!sql.exec("SELECT 1 FROM objects WHERE sha = ?", nu).toArray().length) return `ng ${name} missing object`;
          sql.exec("INSERT INTO refs (name, sha) VALUES (?, ?) ON CONFLICT(name) DO UPDATE SET sha = excluded.sha", name, nu);
        }
        return `ok ${name}`;
      }),
    );
  }

  // ---- objects: big, immutable, R2 ------------------------------------------
  private async receivePack(req: Request): Promise<Response> {
    const reader = req.body!.getReader();
    const cmds = await readPktLineCommands(reader);   // "<old> <new> refs/heads/x\0caps" ... until "0000"
    const puts: Promise<unknown>[] = [];
    // PACK header: "PACK" u32 version u32 count; then N entries of (type,size varint)(zlib body) [see streaming-pack-parser]
    for await (const obj of parsePack(reader)) {      // {sha, type, size, body: Uint8Array} — deltas resolved by parser
      this.ctx.storage.sql.exec("INSERT OR IGNORE INTO pending (sha, created_at) VALUES (?, ?)", obj.sha, Date.now());
      puts.push(this.env.BUCKET.put(this.prefix + obj.sha, obj.body, { customMetadata: { type: obj.type } }));
      this.ctx.storage.sql.exec("INSERT OR IGNORE INTO objects (sha, type, size) VALUES (?, ?, ?)", obj.sha, obj.type, obj.size);
    }
    await Promise.all(puts);                          // objects durable in R2 BEFORE refs move
    const results = this.updateRefs(cmds);
    this.ctx.storage.sql.exec("DELETE FROM pending");
    await this.ctx.storage.setAlarm(Date.now() + 3_600_000); // janitor: sweep pending rows left by a crash mid-push
    const report = pkt("unpack ok\n") + results.map((r) => pkt(r + "\n")).join("") + "0000";
    return new Response(report, { headers: { "content-type": "application/x-git-receive-pack-result" } });
  }

  private async uploadPack(req: Request): Promise<Response> {
    const wants = await readWants(req.body!);         // "want <sha>" pkt-lines; naive: no have-negotiation here
    const shas = closure(this.ctx.storage.sql, wants); // walk commit->tree->blob using the objects index [want-have-negotiation]
    const out = new ReadableStream<Uint8Array>({
      pull: async (c) => {
        c.enqueue(pkt("NAK\n"));
        c.enqueue(packHeader(shas.length));             // "PACK" + version 2 + count
        for (const sha of shas) {
          const o = await this.env.BUCKET.get(this.prefix + sha);         // one R2 GET per object
          c.enqueue(await packEntry(o!.customMetadata!.type, await o!.arrayBuffer())); // type/size varint + deflate
        }
        c.enqueue(await packTrailerSha1()); c.close();
      },
    });
    return new Response(out, { headers: { "content-type": "application/x-git-upload-pack-result" } });
  }

  async alarm(): Promise<void> {
    const stale = this.ctx.storage.sql.exec<{ sha: string }>("SELECT sha FROM pending WHERE created_at < ?", Date.now() - 3_600_000).toArray();
    for (const { sha } of stale) { await this.env.BUCKET.delete(this.prefix + sha); this.ctx.storage.sql.exec("DELETE FROM objects WHERE sha = ?", sha); }
    this.ctx.storage.sql.exec("DELETE FROM pending WHERE created_at < ?", Date.now() - 3_600_000);
  }
}

const pkt = (s: string) => (s.length + 4).toString(16).padStart(4, "0") + s;  // pkt-line: 4 hex len incl. header
```

## Why it works
- A push in smart-HTTP v0 is exactly: pkt-line ref commands, `0000`, then a raw `PACK` stream; `git-receive-pack` expects back `unpack ok` plus one `ok`/`ng` line per ref (the `report-status` capability). The DO produces precisely that, and `ng ... fetch first` is what git itself emits on a non-fast-forward.
- Git's atomicity contract is per-ref compare-and-swap on `<old-sha> <new-sha>`; a single DO per repo plus `transactionSync` gives that trivially (idea 1), with no locks or conditional writes on R2.
- Objects are content-addressed and immutable, so `BUCKET.put(sha)` is idempotent: a retried push, a concurrent push of the same commit, or a crashed push re-run cannot corrupt anything; the worst outcome is an orphan the alarm sweeps.
- Ordering "all R2 puts resolved, then refs flip" is the same invariant loose-object git relies on (objects before refs), so a reader that sees a ref can always fetch its closure.
- The ref advertisement is a `SELECT` over a table of a few KB, so `info/refs` (the request every `git fetch`/`ls-remote` makes) never touches R2; R2 cost scales with objects transferred, not with polling.
- The DO keeps only an `objects(sha,type,size)` index (~60 B/row), so the closure walk for `upload-pack` is a SQLite query; a 100k-object repo is ~6 MB of SQLite, well inside the DO's 10 GB storage.

## Known limits
- Per-object R2 `put`/`get` costs one Class A/B op each; a push of 5,000 small objects is 5,000 puts (~$0.02) and a fresh clone is 5,000 gets plus latency of a few ms each serialized through one DO. Real clones need `precomputed-clone-pack` / `bundle-uri`; per-object R2 is fine for incremental fetch and push.
- The receive path resolves deltas in the Worker/DO. A single request has ~30 s CPU on paid Workers and the DO isolate has 128 MB; a pack with a multi-hundred-MB blob or deep delta chains must be chunked (`presigned-direct-upload`) or offloaded to Wasm (`wasm-git-core`). Hand-waved here as `parsePack`.
- The DO receives the request body as a stream, but the whole pack still flows through a single DO isolate; sustained throughput is bounded by one DO (~tens of MB/s, one push at a time). Acceptable for a repo; shard by `branch-level-dos` for monorepos.
- `transactionSync` cannot await, so R2 puts happen before the transaction and refs are validated only against the SQLite `objects` index, not by re-`head`ing R2. A lost R2 write (put resolved but object missing) is not detectable here; a periodic `list` reconciliation in the alarm would close that gap.
- The ref advertisement uses protocol v0 pkt-line for the proof; `protocol-v2-only` replaces `advertise` with `ls-refs` but the storage model is unchanged.
- `idFromName` does not return the name, so the Worker must pass `owner/repo` (shown as `x-repo-prefix` header); on first request after eviction it is re-derived, which is fine because the DO id is stable.
- SHA-1 only; SHA-256 repos would need a `hash_algo` column, trivially.

## Depends on
- repo-do-ref-authority
- content-addressed-r2-keys
- streaming-pack-parser (for `parsePack` on receive)
- info-refs-endpoint (pkt-line codec shown inline as `pkt`)
