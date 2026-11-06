> Idea #40 · wild · verdict: **risky** · feasibility 4/5 · reliability 2/5 · correctness 3/5 · effort: weeks
> Proof: [proofs/zero-clone-vfs.md](./zero-clone-vfs.md) · Review: [reviews/zero-clone-vfs.md](../../reviews/archived/zero-clone-vfs.md)

# Zero-clone execution: repo as a virtual filesystem inside an agent DO

## Mechanism
An agent DO (the `AgentSession` shape from grok-pi) is bound to `owner/repo@ref` at creation; its `read`/`list`/`write`/`edit` tools never touch a checkout. `read(path)` resolves the ref to a commit oid through one RPC call to the repo DO (`repo-do-ref-authority`), then walks commit -> root tree -> subtree -> blob with one `env.BUCKET.get("objects/<sha>")` per hop, inflating each loose object through `DecompressionStream("deflate")` (zlib) and parsing the `tree` entries (`<mode> <name>\0<20-byte sha>`). Every resolved `(path -> oid, mode)` is memoised in the agent DO's SQLite so a second read of anything in the same directory is exactly one R2 GET; writes go to an overlay table (`path -> new blob sha`) after `put`-ing the new blob under its own content hash, and `commit()` builds new tree and commit objects in memory, puts them to R2 and CAS-advances the ref via RPC (`tui-rpc-push`). Nothing is ever "checked out": the working tree is the SQLite table `vfs_tree(root_oid, path, oid, mode)` plus an overlay.

## Primitives
- Durable Objects with SQLite storage (`ctx.storage.sql.exec`) for the path->oid memo and write overlay (GA)
- DO-to-DO RPC (`env.REPO.get(id).resolveRef(...)`, `casRef(...)`) for ref lookup and push (GA)
- R2 `get`/`put` keyed by SHA (`objects/<sha>`) for loose objects (GA); R2 range reads (`get(key,{range})`) for objects that live inside a pack (GA)
- `DecompressionStream("deflate")` for zlib-wrapped loose objects and pack entries (GA in workerd)
- `crypto.subtle.digest("SHA-1")` to hash new blobs/trees/commits (GA)
- DO alarm (`ctx.storage.setAlarm`) to drop the memo table when the bound ref moves or the session idles (GA)
- Optional: KV replica of refs (`replicated-refs-edge`) to avoid the RPC hop on read; not required

## Proof code
```typescript
import { DurableObject } from "cloudflare:workers";

type Env = { BUCKET: R2Bucket; REPO: DurableObjectNamespace };

async function inflate(body: ReadableStream): Promise<Uint8Array> {
  // loose object = zlib(deflate) of "<type> <len>\0<payload>"
  const buf = await new Response(body.pipeThrough(new DecompressionStream("deflate"))).arrayBuffer();
  return new Uint8Array(buf);
}
function splitHeader(o: Uint8Array): { type: string; body: Uint8Array } {
  const nul = o.indexOf(0);
  const type = new TextDecoder().decode(o.subarray(0, nul)).split(" ")[0];
  return { type, body: o.subarray(nul + 1) };
}
const hex = (b: Uint8Array) => [...b].map((x) => x.toString(16).padStart(2, "0")).join("");

export class RepoVfs extends DurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS vfs_tree (root TEXT, path TEXT, oid TEXT, mode TEXT, PRIMARY KEY(root, path));
      CREATE TABLE IF NOT EXISTS overlay  (path TEXT PRIMARY KEY, oid TEXT, mode TEXT);
      CREATE TABLE IF NOT EXISTS binding  (k TEXT PRIMARY KEY, v TEXT)`);
  }

  async bind(repo: string, ref: string) {
    const root = await this.env.REPO.get(this.env.REPO.idFromName(repo)).resolveRef(ref); // commit oid
    this.ctx.storage.sql.exec("INSERT OR REPLACE INTO binding VALUES ('repo',?),('ref',?),('commit',?)", repo, ref, root);
    await this.ctx.storage.setAlarm(Date.now() + 15 * 60_000); // idle: drop memo, re-resolve next time
  }

  private async object(oid: string) {
    const o = await this.env.BUCKET.get(`objects/${oid}`); // content-addressed loose object
    if (!o) throw new Error(`missing object ${oid}`);       // packed objects: see pack_idx in Known limits
    return splitHeader(await inflate(o.body));
  }

  /** commit -> tree oid: "tree <hex>\n" is always the first commit header line */
  private async rootTree(commit: string) {
    const { body } = await this.object(commit);
    return new TextDecoder().decode(body.subarray(5, 45));
  }

  /** Parse one tree object and memoise every entry under `dir`. */
  private async loadTree(root: string, dir: string, treeOid: string) {
    const { body } = await this.object(treeOid);
    let i = 0;
    while (i < body.length) {
      const sp = body.indexOf(0x20, i), nul = body.indexOf(0, sp);
      const mode = new TextDecoder().decode(body.subarray(i, sp));
      const name = new TextDecoder().decode(body.subarray(sp + 1, nul));
      const oid = hex(body.subarray(nul + 1, nul + 21));
      this.ctx.storage.sql.exec("INSERT OR IGNORE INTO vfs_tree VALUES (?,?,?,?)", root, dir ? `${dir}/${name}` : name, oid, mode);
      i = nul + 21;
    }
  }

  private async lookup(path: string): Promise<{ oid: string; mode: string } | undefined> {
    const ov = this.ctx.storage.sql.exec<{ oid: string; mode: string }>("SELECT oid,mode FROM overlay WHERE path=?", path).toArray()[0];
    if (ov) return ov;
    const root = this.ctx.storage.sql.exec<{ v: string }>("SELECT v FROM binding WHERE k='commit'").toArray()[0].v;
    const parts = path.split("/");
    let treeOid = await this.rootTree(root), dir = "";
    for (let d = 0; d < parts.length; d++) {
      const p = dir ? `${dir}/${parts[d]}` : parts[d];
      let row = this.ctx.storage.sql.exec<{ oid: string; mode: string }>("SELECT oid,mode FROM vfs_tree WHERE root=? AND path=?", root, p).toArray()[0];
      if (!row) { await this.loadTree(root, dir, treeOid); row = this.ctx.storage.sql.exec<{ oid: string; mode: string }>("SELECT oid,mode FROM vfs_tree WHERE root=? AND path=?", root, p).toArray()[0]; }
      if (!row) return undefined;
      if (d === parts.length - 1) return row;
      if (row.mode !== "40000") return undefined;
      treeOid = row.oid; dir = p;
    }
  }

  async read(path: string): Promise<string | undefined> {
    const hit = await this.lookup(path);
    if (!hit || hit.mode === "40000") return undefined;
    return new TextDecoder().decode((await this.object(hit.oid)).body); // one R2 GET on a warm memo
  }

  async write(path: string, content: string) {
    const payload = new TextEncoder().encode(content);
    const hdr = new TextEncoder().encode(`blob ${payload.length}\0`);
    const raw = new Uint8Array([...hdr, ...payload]);
    const oid = hex(new Uint8Array(await crypto.subtle.digest("SHA-1", raw)));
    const z = new Response(new Blob([raw]).stream().pipeThrough(new CompressionStream("deflate"))).arrayBuffer();
    await this.env.BUCKET.put(`objects/${oid}`, await z); // idempotent: key is the hash
    this.ctx.storage.sql.exec("INSERT OR REPLACE INTO overlay VALUES (?,?,'100644')", path, oid);
  }

  // commit(): fold overlay into vfs_tree, rebuild changed trees bottom-up (same hash/put as write),
  // build "tree <oid>\nparent <commit>\n..." commit object, then REPO.casRef(ref, oldCommit, newCommit).

  async alarm() { this.ctx.storage.sql.exec("DELETE FROM vfs_tree"); }
}
```

## Why it works
- A git working tree is a pure function of a commit oid: `commit -> tree -> entries`. Every object is immutable and content-addressed, so `objects/<sha>` in R2 can be cached forever and the memo table can never go stale for a fixed `root`; only the ref binding changes.
- Loose object format is trivial: zlib-compressed `"<type> <size>\0<bytes>"`. `DecompressionStream("deflate")` is the zlib wrapper (not raw deflate), which is exactly what git writes, so no wasm inflater is needed for loose objects.
- Tree entries are `<octal mode> <name>\0<20-byte binary sha>`, unsorted-by-us but git-sorted; the parser above is the whole format. Directory mode is `40000`, so descending is a mode check.
- Writes are real git objects (correct header, SHA-1 of header+payload, zlib) `put` under their hash, so a later `commit()` produces objects any `git fetch` can consume, and `git fsck` will accept them. Because the key is the hash, retried writes are free (`content-addressed-r2-keys`).
- Publishing is a plain ref CAS in the repo DO, the same path a normal push takes (`repo-do-ref-authority`), so the agent's edits appear to every other client as an ordinary commit with no special server state.
- Cold cost is O(depth) R2 GETs for the first path in a directory; warm cost is one GET per file. For an agent that reads a few dozen files out of a 50k-file repo this is far cheaper than materialising the tree anywhere.

## Known limits
- "No checkout ever" holds for the agent's read/list/write/edit tools, not for `bash`: running a test suite or `npm install` still needs a container/sandbox with a real filesystem (grok-pi's `Executor` eject point). This proof makes the read side and the commit side clone-free; execution of arbitrary processes is out of scope for a Worker.
- Objects that live inside a packfile (anything after a `gc-and-repack-alarm` or a normal push) are not loose. Reading them needs a `pack_idx(oid, pack_key, offset, size, base_oid)` table populated by `streaming-pack-parser`, an R2 range GET, and ofs-delta/ref-delta resolution against the base; a long delta chain can mean several sequential range reads. The proof hand-waves this behind `objects/<oid>` and assumes the pack parser also explodes to loose keys (or a pack index exists).
- Blobs are fully inflated in DO memory (128 MB) and `TextDecoder`'d; multi-hundred-MB files are not readable through this path. Stream-to-client for large blobs and refuse `read` above a size cap.
- `crypto.subtle.digest` and `CompressionStream` run inside the DO's CPU budget (30 s per request, though DOs get generous wall time); writing thousands of files in one turn should be chunked. `commit()` must rebuild every tree on the path from each changed file up to root, which is O(changed dirs) tree objects, each one hashed and `put`.
- Per-object R2 GETs are billed as Class B ops: a cold walk of a deep path costs depth+1 requests, and a `list("src")` of a big directory costs one GET plus one row insert per entry into SQLite. A large first `list` at repo root can exceed a single SQLite statement budget and needs batching.
- The memo table is per agent DO; it is dropped by the idle alarm and on rebind. If the bound ref moves under the agent, `commit()` will fail the CAS and the overlay must be replayed on the new tip (a rebase, `server-side-rebase`), which this proof does not implement.
- Single-DO throughput: all reads for one session serialize through one DO; fine for one agent, not a shared read API.

## Depends on
- content-addressed-r2-keys
- refs-sqlite-objects-r2
- repo-do-ref-authority
- tui-rpc-push
- streaming-pack-parser (only for reading objects that are inside packs)
