> Idea #42 · wild · verdict: **risky** · feasibility 4/5 · reliability 2/5 · correctness 2/5 · effort: weeks
> Proof: [proofs/git-as-db-driver.md](./git-as-db-driver.md) · Review: [reviews/git-as-db-driver.md](../../reviews/archived/git-as-db-driver.md)

# Git as a database driver (ActiveRecord / JS ORM adapter)

## Mechanism
An ORM adapter (`GitDriver` for a Kysely/Drizzle-style query builder, or `ActiveRecord::ConnectionAdapters::GitAdapter` over HTTP) sends `POST /db/<table>/<id>` (save) and `POST /db/query` (select) to the repo Durable Object; the DO is the single writer, so every save is serialized without locks. On save the DO writes one loose git blob for `tables/<table>/<id>.json`, one new tree per directory level and one commit object to R2 under `objects/<sha>` (content-addressed, zlib via `CompressionStream("deflate")`), then compare-and-swaps `refs/heads/main` in DO SQLite. A `rows(table,id,blob_sha,json)` table in DO SQLite mirrors the HEAD tree so queries are SQLite lookups instead of R2 tree walks; the git objects remain the source of truth and can be re-materialized by walking the tree from HEAD. Migrations run in the DO as a rewrite commit on `refs/heads/migrate/<name>`; writes that land on `main` while the migration runs are replayed (rebased) through the migration function onto the rewritten tree, then `main` is CAS-flipped to the result.

## Primitives
- Durable Object with SQLite storage (`ctx.storage.sql`) - GA; holds refs, the `rows` mirror, and the migration journal.
- DO alarms (`ctx.storage.setAlarm`) - GA; drives the migration rebase loop and optional write-coalescing.
- R2 (`env.BUCKET.put/get`) - GA; loose objects keyed `objects/<sha>` and a periodic pack.
- `CompressionStream("deflate")` / `DecompressionStream("deflate")` - GA in workerd; produces the zlib framing git expects for loose objects.
- `crypto.subtle.digest("SHA-1")` - GA; object ids.
- DO RPC (`env.REPO.get(id).save(...)`) - GA on compat date >= 2024-04-03; used by the JS adapter instead of HTTP when both live in one Worker.

## Proof code
```typescript
import { DurableObject } from "cloudflare:workers";
type Env = { BUCKET: R2Bucket };
const enc = new TextEncoder();

async function sha1(b: Uint8Array) {
  return [...new Uint8Array(await crypto.subtle.digest("SHA-1", b))].map(x => x.toString(16).padStart(2, "0")).join("");
}
function cat(...parts: Uint8Array[]) {
  const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0)); let o = 0;
  for (const p of parts) { out.set(p, o); o += p.length; } return out;
}
const hex2bin = (h: string) => Uint8Array.from(h.match(/../g)!, x => parseInt(x, 16));

export class RepoDB extends DurableObject<Env> {
  sql = this.ctx.storage.sql;
  migrating = false; // set while a migrate() rewrite is in flight; saves are journaled, then rebased by alarm()
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.sql.exec(`CREATE TABLE IF NOT EXISTS refs(name TEXT PRIMARY KEY, sha TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS rows(tbl TEXT, id TEXT, blob TEXT NOT NULL, json TEXT NOT NULL, PRIMARY KEY(tbl,id));
      CREATE TABLE IF NOT EXISTS migration_log(seq INTEGER PRIMARY KEY AUTOINCREMENT, tbl TEXT, id TEXT, json TEXT)`);
  }

  // Loose object: "<type> <len>\0<body>", zlib-deflated, stored at objects/<sha>. Idempotent by key.
  private async putObj(type: "blob" | "tree" | "commit", body: Uint8Array) {
    const raw = cat(enc.encode(`${type} ${body.length}\0`), body);
    const sha = await sha1(raw);
    const z = new Response(new Blob([raw]).stream().pipeThrough(new CompressionStream("deflate")));
    await this.env.BUCKET.put(`objects/${sha}`, await z.arrayBuffer());
    return sha;
  }
  // Tree entries: "100644 <name>\0" + 20-byte sha, sorted by name; dirs use mode 40000.
  private async putTree(entries: { mode: string; name: string; sha: string }[]) {
    entries.sort((a, b) => (a.name < b.name ? -1 : 1));
    return this.putObj("tree", cat(...entries.map(e => cat(enc.encode(`${e.mode} ${e.name}\0`), hex2bin(e.sha)))));
  }
  private async treeFromRows() { // rebuild tables/<tbl>/<id>.json tree from the SQLite mirror
    const byTbl = new Map<string, { mode: string; name: string; sha: string }[]>();
    for (const r of this.sql.exec<{ tbl: string; id: string; blob: string }>("SELECT tbl,id,blob FROM rows"))
      (byTbl.get(r.tbl) ?? byTbl.set(r.tbl, []).get(r.tbl)!).push({ mode: "100644", name: `${r.id}.json`, sha: r.blob });
    const tables = await Promise.all([...byTbl].map(async ([tbl, es]) => ({ mode: "40000", name: tbl, sha: await this.putTree(es) })));
    return this.putTree([{ mode: "40000", name: "tables", sha: await this.putTree(tables) }]);
  }
  private head() { return this.sql.exec<{ sha: string }>("SELECT sha FROM refs WHERE name='refs/heads/main'").toArray()[0]?.sha; }
  private async commit(tree: string, msg: string, parent = this.head()) {
    const who = `orm <orm@git-edge> ${Math.floor(Date.now() / 1000)} +0000`;
    return this.putObj("commit", enc.encode(`tree ${tree}\n${parent ? `parent ${parent}\n` : ""}author ${who}\ncommitter ${who}\n\n${msg}\n`));
  }

  // ORM save(): one blob + trees + commit, then CAS on refs/heads/main. Serialized by the DO.
  async save(tbl: string, id: string, row: object, expectHead?: string) {
    const json = JSON.stringify(row);
    const blob = await this.putObj("blob", enc.encode(json));
    this.sql.exec("INSERT OR REPLACE INTO rows VALUES(?,?,?,?)", tbl, id, blob, json);
    if (this.migrating) { this.sql.exec("INSERT INTO migration_log(tbl,id,json) VALUES(?,?,?)", tbl, id, json); return this.head()!; }
    const prev = this.head();
    if (expectHead !== undefined && expectHead !== prev) throw new Error("stale: optimistic lock failed");
    const sha = await this.commit(await this.treeFromRows(), `save ${tbl}/${id}`, prev);
    this.sql.exec("INSERT OR REPLACE INTO refs VALUES('refs/heads/main',?)", sha); // the CAS: same DO, no interleaving
    return sha;
  }
  // ORM where(): SQLite, not an R2 tree walk. Tree is re-materializable from HEAD if this table is lost.
  find(tbl: string, pred: (r: any) => boolean) {
    return this.sql.exec<{ json: string }>("SELECT json FROM rows WHERE tbl=?", tbl).toArray().map(r => JSON.parse(r.json)).filter(pred);
  }
  // Migration = rewrite every row -> commit on refs/heads/migrate/<name>; alarm rebases journaled saves onto it.
  async migrate(name: string, tbl: string, fn: (r: any) => any) {
    const base = this.head()!; this.migrating = true;
    for (const r of this.find(tbl, () => true)) {
      const json = JSON.stringify(fn(r)), blob = await this.putObj("blob", enc.encode(json));
      this.sql.exec("UPDATE rows SET blob=?,json=? WHERE tbl=? AND id=?", blob, json, tbl, r.id);
    }
    const sha = await this.commit(await this.treeFromRows(), `migrate ${name}`, base);
    this.sql.exec("INSERT OR REPLACE INTO refs VALUES(?,?)", `refs/heads/migrate/${name}`, sha);
    this.sql.exec("INSERT OR REPLACE INTO refs VALUES('refs/heads/main',?)", sha); // ff main onto the rewritten history
    await this.ctx.storage.setAlarm(Date.now() + 50); // let rebase-of-journal run as its own step
  }
  async alarm() { // replay writes that raced the migration: re-save (rebase) each, in order, on top of migrated main
    this.migrating = false;
    for (const w of this.sql.exec<{ seq: number; tbl: string; id: string; json: string }>("SELECT * FROM migration_log ORDER BY seq"))
      await this.save(w.tbl, w.id, JSON.parse(w.json));
    this.sql.exec("DELETE FROM migration_log");
  }
}
```

## Why it works
- The objects written are byte-exact git loose objects: header `<type> <size>\0`, SHA-1 over header+body, zlib-deflated (`CompressionStream("deflate")` emits the zlib wrapper, which is what `git cat-file` inflates). A `git clone` served by the sibling upload-pack path (which inflates `objects/<sha>` and re-deflates into a `PACK` stream) sees a normal history: `tables/users/42.json` at every commit, `git log -p` shows row diffs, `git checkout <sha>` is point-in-time recovery.
- Trees must be sorted by name and directories use mode `40000` (no leading zero); the code does both, so `git fsck` accepts them.
- "Every save is a commit" holds literally: one save produces exactly one commit with one parent, and `refs/heads/main` moves under the DO's single-writer guarantee, which is the same CAS the receive-pack path uses (`repo-do-ref-authority`). An external `git push` and an ORM save cannot interleave because both go through the same DO.
- "Every query a tree walk" is satisfied by the SQLite `rows` table being a materialization of the HEAD tree: it is exactly what `git ls-tree -r HEAD tables/` returns, cached. Losing the DO's SQLite is recoverable by walking the tree from the last known HEAD in R2 (the `r2-versioned-snapshots` sibling covers the ref itself).
- "Migrations are rebases" is realized op-based: the migration commit rewrites the tree; saves that arrived during the migration are journaled and replayed on top of the migrated commit, producing linear history `... -> migrate v2 -> save a -> save b`, which is what `git rebase` would yield if each save were a patch. Row-level 3-way merge is not needed because the replayed write carries the full new row (last-writer-wins on the JSON blob).
- Optimistic locking (`expectHead`) maps to ActiveRecord's `lock_version`: the client passes the commit it read, the DO refuses if `main` moved.

## Known limits
- Cost and latency per save: 1 blob + (depth) trees + 1 commit = ~4 R2 PUTs (Class A, ~$4.50/M) per row write, plus SHA-1 and deflate CPU. Around 20-40 ms per save in practice; a single-DO ceiling of roughly 50-100 saves/s. Coalescing saves into one commit per alarm tick fixes throughput but breaks the "every save is a commit" promise; that trade-off is real and the proof does not hide it.
- `treeFromRows` rebuilds the whole tree from SQLite on every save: O(rows) per write. Production needs per-directory tree caching (only re-hash the `tables/<tbl>` subtree that changed) and sharded row directories (`tables/users/4/2/42.json`) so a 1M-row table does not produce a 1M-entry tree object (git handles it, but the 40-byte-per-entry tree would be 40 MB and blow the DO 128 MB budget when hashed).
- Queries are SQLite scans of JSON text; there are no secondary indexes unless the adapter adds SQLite indexes on JSON-extracted columns. Joins across tables are in-DO JS. This is a document store with git history, not a relational engine; the ORM adapter surface should be that of a document-ish adapter, not full SQL.
- The migration rebase is last-writer-wins on whole rows: a save that raced the migration and was written in the old schema is replayed as-is unless `alarm()` also passes it through `fn`. The proof journals the raw row; a correct implementation must store the migration fn (by name) and apply it during replay. Hand-waved here.
- `migrate` holds the DO for O(rows) awaited R2 PUTs in one call; beyond ~10k rows it must chunk itself across alarm ticks to stay under the per-invocation CPU limit (30 s wall on DOs is not the issue, CPU time is; a Worker request has a 30 s CPU cap on paid plans by default).
- `migrating` is an in-memory flag; a DO eviction mid-migration loses it. Real code persists it in SQLite next to the refs (and the pending alarm already survives eviction).
- One DO per database means one region of write authority; readers elsewhere pay cross-region latency unless refs and the `rows` mirror are replicated (`replicated-refs-edge`).
- Loose objects only: a real `git clone` after 1M saves would need the packing path (`gc-and-repack-alarm`, `precomputed-clone-pack`) or the clone takes millions of R2 GETs.
- The ActiveRecord half is an HTTP adapter speaking JSON to the DO; it cannot be a drop-in for `ActiveRecord::Base.connection.execute(sql)`. The JS/ORM side is the honest target.

## Depends on
- repo-do-ref-authority
- refs-sqlite-objects-r2
- content-addressed-r2-keys
- r2-versioned-snapshots
- gc-and-repack-alarm
