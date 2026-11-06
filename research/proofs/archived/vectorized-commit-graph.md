> Idea #45 · wild · verdict: **lands with caveats** · feasibility 4/5 · reliability 3/5 · correctness 2/5 · effort: weeks
> Proof: [proofs/vectorized-commit-graph.md](./vectorized-commit-graph.md) · Review: [reviews/vectorized-commit-graph.md](../../reviews/archived/vectorized-commit-graph.md)

# Commit graph in Vectorize for semantic git log

## Mechanism
After a successful `git-receive-pack` the repo DO (the ref authority from `repo-do-ref-authority`) already knows every new commit SHA it walked for connectivity; it inserts them into an `embed_queue` table in its SQLite and calls `ctx.storage.setAlarm`. The alarm handler drains the queue in batches of 32: it fetches each commit object from R2 (`objects/<sha>`, zlib-inflated with `DecompressionStream("deflate")`), parses the `tree`/`parent`/`author` headers and message, computes a cheap changed-path summary by diffing the commit's tree against its first parent's tree (tree objects also from R2), builds a text `"<subject>\n\n<body>\n\nfiles: a.ts b.ts"`, embeds it with Workers AI (`@cf/baai/bge-m3`, 1024 dims) and upserts into a single shared Vectorize index with `namespace = <repo DO id>` and metadata `{sha, ts, subject}`. `git log --semantic` is not a real git flag; the query side is an HTTP endpoint `GET /<owner>/<repo>/log?q=...` on the edge Worker (and, if `agent-native-commands` lands, a protocol v2 `search` command) that calls `VECTORIZE.query` with the namespace, then asks the repo DO to hydrate the hit SHAs from its `commit_graph` table and emit `git log --format=...`-style text, ordered by score.

## Primitives
- Durable Objects with SQLite storage (`ctx.storage.sql`) — GA
- DO alarms (`ctx.storage.setAlarm`, `alarm()`) — GA
- R2 `get` with `DecompressionStream` for loose objects — GA
- Workers AI text embeddings (`@cf/baai/bge-m3`) — GA (model itself is marked "beta" in the catalog; `@cf/baai/bge-base-en-v1.5` is the non-beta fallback at 768 dims)
- Vectorize v2 (`upsert`, `query` with namespaces + metadata filters) — GA since Aug 2024; metadata indexes must be created with `wrangler vectorize create-metadata-index` before vectors are inserted
- Workers (edge router) — GA

## Proof code
```typescript
// wrangler.jsonc additions:
//   "ai": { "binding": "AI" },
//   "vectorize": [{ "binding": "VECTORIZE", "index_name": "git-edge-commits" }]   // dims 1024, metric cosine
//   $ wrangler vectorize create-metadata-index git-edge-commits --property-name=ts --type=number

interface Env { BUCKET: R2Bucket; AI: Ai; VECTORIZE: VectorizeIndex; REPO: DurableObjectNamespace }
const EMBED_MODEL = "@cf/baai/bge-m3";

export class RepoDO implements DurableObject {
  constructor(private ctx: DurableObjectState, private env: Env) {
    ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS commit_graph (sha TEXT PRIMARY KEY, parents TEXT, tree TEXT, ts INTEGER, subject TEXT);
      CREATE TABLE IF NOT EXISTS embed_queue (sha TEXT PRIMARY KEY, tries INTEGER DEFAULT 0)`);
  }

  // Called by the receive-pack path after refs are CAS-advanced (two-phase-push, phase 2).
  enqueueForEmbedding(newCommits: string[]) {
    for (const sha of newCommits) this.ctx.storage.sql.exec("INSERT OR IGNORE INTO embed_queue(sha) VALUES (?)", sha);
    this.ctx.storage.setAlarm(Date.now() + 1_000);
  }

  async alarm() {
    const batch = this.ctx.storage.sql.exec<{ sha: string }>("SELECT sha FROM embed_queue WHERE tries < 5 LIMIT 32").toArray();
    if (batch.length === 0) return;
    const docs: { sha: string; text: string; ts: number; subject: string }[] = [];
    for (const { sha } of batch) {
      const c = await this.readCommit(sha);                       // parse headers + message
      const paths = await this.changedPaths(c.tree, c.parents[0]); // tree-vs-parent-tree diff, names only
      docs.push({ sha, ts: c.ts, subject: c.subject, text: `${c.message}\n\nfiles: ${paths.join(" ")}`.slice(0, 6000) });
      this.ctx.storage.sql.exec("INSERT OR REPLACE INTO commit_graph VALUES (?,?,?,?,?)", sha, c.parents.join(","), c.tree, c.ts, c.subject);
    }
    try {
      const { data } = await this.env.AI.run(EMBED_MODEL, { text: docs.map(d => d.text) }) as { data: number[][] };
      await this.env.VECTORIZE.upsert(docs.map((d, i) => ({
        id: `${this.ctx.id}:${d.sha}`, values: data[i], namespace: this.ctx.id.toString(),
        metadata: { sha: d.sha, ts: d.ts, subject: d.subject },
      })));
      for (const d of docs) this.ctx.storage.sql.exec("DELETE FROM embed_queue WHERE sha = ?", d.sha);
    } catch {
      for (const d of docs) this.ctx.storage.sql.exec("UPDATE embed_queue SET tries = tries + 1 WHERE sha = ?", d.sha);
    }
    this.ctx.storage.setAlarm(Date.now() + 200); // keep draining; alarm() has its own CPU budget
  }

  // GET /log?q=...&since=<unix>  -> git-log-shaped text, ordered by cosine score
  async semanticLog(q: string, since?: number, limit = 20): Promise<Response> {
    const { data } = await this.env.AI.run(EMBED_MODEL, { text: [q] }) as { data: number[][] };
    const res = await this.env.VECTORIZE.query(data[0], {
      topK: limit, namespace: this.ctx.id.toString(), returnMetadata: "indexed",
      filter: since ? { ts: { $gte: since } } : undefined,
    });
    const shas = res.matches.map(m => m.id.split(":")[1]);
    const rows = new Map(this.ctx.storage.sql
      .exec<{ sha: string; ts: number; subject: string }>(`SELECT sha, ts, subject FROM commit_graph WHERE sha IN (${shas.map(() => "?").join(",")})`, ...shas)
      .toArray().map(r => [r.sha, r]));
    const out = res.matches.map(m => { const r = rows.get(m.id.split(":")[1])!;
      return `commit ${r.sha}  (score ${m.score.toFixed(3)})\nDate: ${new Date(r.ts * 1000).toISOString()}\n\n    ${r.subject}\n`; });
    return new Response(out.join("\n"), { headers: { "content-type": "text/plain" } });
  }

  // --- git object plumbing (loose object in R2: zlib(header "commit <len>\0" + body)) ---
  private async readCommit(sha: string) {
    const obj = await this.env.BUCKET.get(`objects/${sha}`);
    if (!obj) throw new Error(`missing ${sha}`);
    const raw = new Uint8Array(await new Response(obj.body.pipeThrough(new DecompressionStream("deflate"))).arrayBuffer());
    const text = new TextDecoder().decode(raw.subarray(raw.indexOf(0) + 1));
    const [hdr, ...msg] = text.split("\n\n");
    const lines = hdr.split("\n"), get = (k: string) => lines.filter(l => l.startsWith(k + " ")).map(l => l.slice(k.length + 1));
    const ts = Number(get("committer")[0].split(" ").at(-2));
    const message = msg.join("\n\n");
    return { tree: get("tree")[0], parents: get("parent"), ts, message, subject: message.split("\n")[0] };
  }
  private async changedPaths(tree: string, parent?: string): Promise<string[]> {
    // pseudo: inflate both tree objects (binary "<mode> <name>\0<20-byte sha>" entries), recurse only into
    // subtrees whose sha differs, collect leaf names whose sha differs. Bounded to 200 paths.
    return diffTrees(await this.readTree(tree), parent ? await this.readTree((await this.readCommit(parent)).tree) : []);
  }
  private async readTree(sha: string): Promise<TreeEntry[]> { /* same inflate as readCommit, parse entries */ return []; }
}
```

## Why it works
- Git commit objects are plain text after zlib inflate (`tree`, `parent`, `author`, `committer <name> <email> <unix-ts> <tz>`, blank line, message), so the DO can parse them with string ops and no git binary; the loose object layout in R2 is exactly what `streaming-pack-parser` writes on push.
- The set of "new commits" is a byproduct the DO already computes: connectivity checking during `git-receive-pack` walks from the new tip to known-reachable SHAs, and that walk is the queue. No second history traversal.
- Embedding happens off the request path in `alarm()`, so `receive-pack` returns `unpack ok` / `ok refs/heads/main` on the wire in the same time as before; a push is never blocked on Workers AI or Vectorize latency, and retries are idempotent because Vectorize `upsert` keys on `<doid>:<sha>`.
- One shared Vectorize index with a namespace per repo keeps the index count far under the 50,000-index-per-account limit while namespace filtering makes queries repo-scoped without a metadata filter.
- The `ts` metadata index gives the semantic query a `--since` analogue; ordering/`--author`/ref reachability are re-applied in the DO against `commit_graph`, which `want-have-negotiation` needs anyway, so the two ideas share one table.
- Output is shaped like `git log` so agent tooling that scrapes `commit <sha>` blocks keeps working; the score is appended on the same line as a harmless suffix.

## Known limits
- `git log --semantic` cannot be made to work in a stock git client: `git log` runs locally against the object store and the server never sees it. The closest real thing is a server-side endpoint (`GET /log?q=`) or a v2 `search` command consumed by a `git edge log` alias or an agent harness. This is what the proof implements.
- Diffs are not embedded, only changed path names plus the message. A real diff needs blob inflation and a line diff per file; with bge-m3's 8k-token input (512 for bge-base) and DO memory of 128 MB, embedding large diffs would require chunking into several vectors per commit and a merge-on-query step. Hand-waved.
- Vectorize `topK` is 100 max (20 when `returnValues: true` or `returnMetadata: "all"`), so a semantic result set is at most 100 commits, and there is no pagination cursor; upserts are eventually consistent (seconds), so a query right after push may miss the newest commits.
- Vectorize metadata filters only support 10 indexed properties per index and one index is capped at 5M vectors; a monorepo with millions of commits needs its own index (still under 50k indexes/account, but the code above assumes one shared index).
- Backfill of an existing history on first push (or on `cow-forks`) puts every commit on the queue; at 32 per alarm and ~1 s per Workers AI batch call, 100k commits take about an hour and consume Workers AI neurons proportionally. The alarm is single-threaded per DO, so embedding competes with pushes for that DO's throughput.
- Workers AI `run` and Vectorize `query` each cost per call; every semantic query is one embedding call plus one Vectorize query plus DO hydration. No caching of query embeddings is shown.
- Tree diffing reads both trees recursively from R2 (one GET per differing subtree); deep monorepo commits can be dozens of R2 reads per commit. A packed-object layout (`precomputed-clone-pack`, `pinned-delta-bases`) would need an index lookup instead of `objects/<sha>` keys.

## Depends on
- repo-do-ref-authority
- refs-sqlite-objects-r2
- streaming-pack-parser
- content-addressed-r2-keys
- two-phase-push
- want-have-negotiation
- agent-native-commands (optional, for the protocol v2 surface)
