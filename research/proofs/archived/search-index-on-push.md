> Idea #26 · edge · verdict: **lands with caveats** · feasibility 4/5 · reliability 3/5 · correctness 3/5 · effort: days
> Proof: [proofs/search-index-on-push.md](./search-index-on-push.md) · Review: [reviews/search-index-on-push.md](../../reviews/archived/search-index-on-push.md)

# Search index built on push (D1 FTS / Vectorize)

## Mechanism
`POST /:owner/:repo/git-receive-pack` is handled by the repo DO (`repo-do-ref-authority`); after it has written the pack's objects to R2 and CAS-flipped the refs, it appends one row per updated ref to an `index_jobs` table in its own SQLite and calls `ctx.storage.setAlarm(now)` so the push response goes back to the client before any indexing work starts. The alarm handler pops a job, diffs old-tree vs new-tree by walking tree objects from R2 (only entries whose SHA changed are visited, unchanged subtrees are skipped by tree-SHA equality), inflates each changed blob with `DecompressionStream("deflate")`, and upserts `(path, blob_sha, text)` into an FTS5 virtual table in the same DO SQLite (`ctx.storage.sql.exec`), re-arming the alarm if the budget ran out. `GET /:owner/:repo/search?q=` (or a protocol-v2 `search` command, see `agent-native-commands`) runs `SELECT ... FROM blobs_fts WHERE blobs_fts MATCH ?` in the DO; an optional second stage writes the same chunks to Vectorize via Workers AI embeddings for cross-repo semantic search.

## Primitives
- Durable Object with SQLite storage (GA) — `ctx.storage.sql.exec`, FTS5 virtual tables (FTS5 is an enabled extension in DO SQLite and in D1; if it turns out to be missing in DO SQLite on a given release, the identical DDL runs on D1 via `env.DB.prepare`, at the cost of leaving the DO's transaction boundary)
- DO alarms (GA) — deferred, self-rescheduling post-receive work
- R2 (GA) — `env.BUCKET.get(key)` for tree and blob objects, content-addressed keys
- `DecompressionStream("deflate")` (Workers runtime, GA) — zlib-inflate loose git objects
- Workers AI `@cf/baai/bge-base-en-v1.5` + Vectorize (both GA, Vectorize v2) — optional semantic stage
- D1 (GA) — optional cross-repo FTS index if per-repo DO tables are not enough

## Proof code
```typescript
import { DurableObject } from "cloudflare:workers";

type Env = { BUCKET: R2Bucket; AI: Ai; VEC: VectorizeIndex };
const objKey = (sha: string) => `objects/${sha.slice(0, 2)}/${sha.slice(2)}`;

export class RepoDO extends DurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS refs (name TEXT PRIMARY KEY, sha TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS index_jobs (id INTEGER PRIMARY KEY AUTOINCREMENT,
        ref TEXT, old_sha TEXT, new_sha TEXT);
      CREATE VIRTUAL TABLE IF NOT EXISTS blobs_fts USING fts5(
        path, blob_sha UNINDEXED, body, tokenize='unicode61');`);
  }

  // Called by the receive-pack handler AFTER objects are in R2 and refs are CAS'd.
  // Same SQLite transaction as the ref flip, so a job is never lost or duplicated.
  async postReceive(updates: { ref: string; old: string; new: string }[]) {
    for (const u of updates)
      this.ctx.storage.sql.exec(
        "INSERT INTO index_jobs (ref, old_sha, new_sha) VALUES (?,?,?)", u.ref, u.old, u.new);
    await this.ctx.storage.setAlarm(Date.now()); // fire ASAP, outside the push request
  }

  async alarm() {
    const job = this.ctx.storage.sql
      .exec("SELECT * FROM index_jobs ORDER BY id LIMIT 1").toArray()[0] as any;
    if (!job) return;
    const started = Date.now();
    const oldTree = job.old_sha === "0".repeat(40) ? null : await this.commitTree(job.old_sha);
    const newTree = await this.commitTree(job.new_sha);
    for await (const { path, sha } of this.changedBlobs("", oldTree, newTree)) {
      const body = await this.readBlob(sha);
      if (body === null) continue; // binary or > cap: skip
      this.ctx.storage.sql.exec("DELETE FROM blobs_fts WHERE path = ?", path);
      this.ctx.storage.sql.exec(
        "INSERT INTO blobs_fts (path, blob_sha, body) VALUES (?,?,?)", path, sha, body);
      if (Date.now() - started > 20_000) { // stay well under the CPU budget
        await this.ctx.storage.setAlarm(Date.now() + 100); return; // resume later
      }
    }
    this.ctx.storage.sql.exec("DELETE FROM index_jobs WHERE id = ?", job.id);
    if (this.ctx.storage.sql.exec("SELECT 1 FROM index_jobs LIMIT 1").toArray().length)
      await this.ctx.storage.setAlarm(Date.now());
  }

  search(q: string) {
    return this.ctx.storage.sql.exec(
      `SELECT path, blob_sha, snippet(blobs_fts, 2, '[', ']', '…', 12) AS snip, bm25(blobs_fts) AS rank
         FROM blobs_fts WHERE blobs_fts MATCH ? ORDER BY rank LIMIT 50`, q).toArray();
  }

  // --- git object plumbing (loose-object format: "<type> <size>\0<payload>", zlib) ---
  private async inflate(sha: string): Promise<{ type: string; payload: Uint8Array } | null> {
    const obj = await this.env.BUCKET.get(objKey(sha));
    if (!obj) return null;
    const raw = new Uint8Array(await new Response(
      obj.body.pipeThrough(new DecompressionStream("deflate"))).arrayBuffer());
    const nul = raw.indexOf(0);
    const [type] = new TextDecoder().decode(raw.subarray(0, nul)).split(" ");
    return { type, payload: raw.subarray(nul + 1) };
  }
  private async commitTree(sha: string) {                 // "tree <sha>\n" is line 1 of a commit
    const c = await this.inflate(sha);
    return new TextDecoder().decode(c!.payload).match(/^tree ([0-9a-f]{40})/)![1];
  }
  private async readTree(sha: string) {                   // entries: "<mode> <name>\0<20-byte sha>"
    const t = await this.inflate(sha); const p = t!.payload; const out = new Map<string, { mode: string; sha: string }>();
    for (let i = 0; i < p.length;) {
      const sp = p.indexOf(32, i), nul = p.indexOf(0, sp);
      const mode = new TextDecoder().decode(p.subarray(i, sp));
      const name = new TextDecoder().decode(p.subarray(sp + 1, nul));
      const hex = [...p.subarray(nul + 1, nul + 21)].map(b => b.toString(16).padStart(2, "0")).join("");
      out.set(name, { mode, sha: hex }); i = nul + 21;
    }
    return out;
  }
  private async *changedBlobs(prefix: string, a: string | null, b: string): AsyncGenerator<{ path: string; sha: string }> {
    if (a === b) return;                                   // identical subtree: skip whole thing
    const [ta, tb] = [a ? await this.readTree(a) : new Map(), await this.readTree(b)];
    for (const [name, e] of tb) {
      const prev = ta.get(name); if (prev?.sha === e.sha) continue;
      if (e.mode === "40000") yield* this.changedBlobs(`${prefix}${name}/`, prev?.mode === "40000" ? prev.sha : null, e.sha);
      else if (e.mode !== "160000") yield { path: prefix + name, sha: e.sha }; // skip submodules
    }
  }
  private async readBlob(sha: string): Promise<string | null> {
    const b = await this.inflate(sha);
    if (!b || b.payload.length > 512 * 1024 || b.payload.subarray(0, 8000).includes(0)) return null; // git's own binary heuristic
    return new TextDecoder().decode(b.payload);
  }
}
```

## Why it works
- `git-receive-pack` only requires the server to answer `unpack ok` / `ok refs/heads/x` in pkt-line after refs move; the client does not wait for hooks to finish, so scheduling indexing on a DO alarm mirrors what a real post-receive hook is: fire-and-forget after the ref update.
- The job row and the ref flip live in the same DO SQLite transaction, so "indexed state" never diverges from "ref state" and a crashed alarm is retried by the runtime (alarms retry with backoff until the handler returns without throwing).
- Tree diffing is exactly what `git diff-tree old new` does: compare 20-byte SHAs entry by entry and recurse only into subtrees whose SHA changed, so a one-file push touching a 100k-file monorepo reads O(depth) trees, not the whole tree.
- Blob text is a pure function of `blob_sha`, so re-indexing is idempotent (content-addressed R2 keys from `content-addressed-r2-keys`); a force-push or re-push re-derives the same rows.
- FTS5 with `unicode61` tokenizes identifiers well enough for code search (`snake_case` splits on `_`, `bm25()` ranking is built in), and `snippet()` gives GitHub-style highlighted hits without storing anything extra.

## Known limits
- Index is per repo, inside the repo DO: fine for `grep`-style search within a repo, but cross-repo search needs a fan-out over DOs or a shared D1 FTS table (D1 rows cap at ~1 MB and D1 writes are not transactional with the DO ref flip).
- DO SQLite is capped at 10 GB per DO and 2 MB per row value; FTS5 roughly doubles the stored text size, so large repos need the 512 KB blob cap shown, exclusion of vendored/lock files, or an external D1/Vectorize index. Alarm CPU is bounded (the 30 s wall/CPU budget), hence the 20 s self-reschedule loop.
- Indexing is asynchronous: a `search` immediately after `git push` can miss the last commit; expose `index_jobs` depth as `X-Index-Lag` so callers can wait.
- Only loose objects read from R2 are shown; blobs stored inside packs as `ofs-delta`/`ref-delta` need delta resolution first (`streaming-pack-parser` materializes objects on push, which is what this relies on). If the push was accepted via `precomputed-clone-pack`-style packs only, `readBlob` needs a pack index lookup + R2 range read instead of a single `get`.
- One R2 GET per changed tree and blob: a 10k-file initial push costs ~10k Class B operations and several minutes of alarms; the tree-diff skips this on subsequent pushes.
- Vectorize stage is hand-waved: embeddings via Workers AI cost per token and Vectorize has per-index vector and metadata limits (~5M vectors, 10 KB metadata), so only chunked, deduplicated blobs should go there and it should be a separate alarm job, not part of the FTS loop.
- Deleted files are handled by the `DELETE FROM blobs_fts WHERE path = ?` only when the path reappears; a proper implementation also iterates entries present in `ta` but absent from `tb` (omitted for brevity).

## Depends on
- repo-do-ref-authority
- refs-sqlite-objects-r2
- content-addressed-r2-keys
- streaming-pack-parser
- (optional) agent-native-commands, vectorized-commit-graph
