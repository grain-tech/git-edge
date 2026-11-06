> Idea #31 · wild · verdict: **does not land** · feasibility 3/5 · reliability 2/5 · correctness 2/5 · effort: weeks
> Proof: [proofs/crdt-branches.md](./crdt-branches.md) · Review: [reviews/crdt-branches.md](../../reviews/archived/crdt-branches.md)

# Multi-writer CRDT branches

## Mechanism
A branch under `refs/heads/live/*` is not a sha in the `refs` table; it is an op log in DO SQLite (`crdt_ops(branch, actor, lamport, path, blob_sha|NULL)`) whose merged state is a path -> blob map (a per-path last-writer-wins register ordered by `(lamport, actor)`, plus per-path diff3 against the common base when two actors touched the same file). Writers reach the `RepoDO` two ways: agents `POST /:owner/:repo/live/:branch/ops` with JSON ops after putting blobs content-addressed into R2, and plain git clients `git push` a commit to `refs/heads/live/foo`; `git-receive-pack` never rejects a non-fast-forward there, it diffs the pushed commit's tree against the tip it was based on and appends those path edits as ops. The DO debounces with `ctx.storage.setAlarm(now+1500)`; the alarm materializes the merged map into real git `tree` objects, then an n-parent merge `commit` whose parents are every contributing commit since the last materialization, `env.BUCKET.put`s them under `objects/<sha>`, and flips `refs/heads/live/foo` in SQLite. The push reply uses `report-status-v2` with `option new-oid <materialized>` so the client's remote-tracking ref lands on the real tip and its next `git pull` is a fast-forward.

## Primitives
- Durable Objects with SQLite storage (`ctx.storage.sql.exec`, `transactionSync`) -- GA
- DO alarms (`ctx.storage.setAlarm`) for debounced materialization and op-log compaction -- GA
- R2 (`env.BUCKET.put/get`, content-addressed keys) for blobs, trees, commits -- GA
- `crypto.subtle.digest("SHA-1")` for git object ids -- GA
- `CompressionStream("deflate")` (zlib) when a materialized object must be served in a pack -- GA
- No beta primitives required

## Proof code
```typescript
// One RepoDO per repo (repo-do-ref-authority). Blobs/trees/commits live in R2 under objects/<sha>
// (content-addressed-r2-keys); refs and the CRDT op log live in DO SQLite (refs-sqlite-objects-r2).
export interface Env { BUCKET: R2Bucket }
type Op = { actor: string; lamport: number; path: string; blob: string | null; base?: string }; // blob=null => delete
const enc = new TextEncoder();
const hex = (b: ArrayBuffer) => [...new Uint8Array(b)].map(x => x.toString(16).padStart(2, "0")).join("");
const unhex = (s: string) => Uint8Array.from(s.match(/../g)!.map(h => parseInt(h, 16)));

export class RepoDO implements DurableObject {
  constructor(private ctx: DurableObjectState, private env: Env) {
    ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS refs     (name TEXT PRIMARY KEY, sha TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS crdt_ops (branch TEXT, actor TEXT, lamport INTEGER, path TEXT, blob TEXT,
                                           base TEXT, commit_sha TEXT, PRIMARY KEY (branch, actor, lamport, path));
      CREATE TABLE IF NOT EXISTS crdt_state (branch TEXT, path TEXT, blob TEXT NOT NULL, lamport INTEGER, actor TEXT,
                                           PRIMARY KEY (branch, path));                    -- compacted LWW registers
      CREATE TABLE IF NOT EXISTS dirty    (branch TEXT PRIMARY KEY)`);
  }

  async fetch(req: Request): Promise<Response> {
    const url = new URL(req.url);
    const m = url.pathname.match(/^\/live\/([^/]+)\/ops$/);
    if (m && req.method === "POST") return this.appendOps(m[1], await req.json() as Op[], null);
    if (url.pathname === "/git-receive-pack") return this.receivePack(req);
    return new Response("not found", { status: 404 });
  }

  // ---- ingest ------------------------------------------------------------------
  private appendOps(branch: string, ops: Op[], commitSha: string | null) {
    const sql = this.ctx.storage.sql;
    this.ctx.storage.transactionSync(() => {
      for (const o of ops)                                     // idempotent: PK collision on replay is a no-op
        sql.exec("INSERT OR IGNORE INTO crdt_ops VALUES (?,?,?,?,?,?,?)",
                 branch, o.actor, o.lamport, o.path, o.blob, o.base ?? null, commitSha);
      sql.exec("INSERT OR IGNORE INTO dirty VALUES (?)", branch);
    });
    void this.ctx.storage.setAlarm(Date.now() + 1500);         // debounce: burst of writers -> one materialization
    return Response.json({ ok: true });
  }

  private async receivePack(req: Request): Promise<Response> {
    // streaming-pack-parser has already indexed the PACK body into R2; here we only see the ref command lines:
    //   pkt-line "<old-oid> <new-oid> refs/heads/live/foo\0report-status-v2 ..."
    const { oldOid, newOid, ref } = parseCommandPktLines(req);
    if (!ref.startsWith("refs/heads/live/")) return classicCompareAndSwap(this.ctx, oldOid, newOid, ref);
    const branch = ref.slice("refs/heads/live/".length);
    const actor = req.headers.get("x-actor") ?? "anon";
    const lamport = Date.now();                                 // good enough for LWW ties; a real clock is per-actor max+1
    const ops: Op[] = [];
    for (const d of await treeDiff(this.env.BUCKET, oldOid, newOid))    // git diff-tree base..pushed, pseudo
      ops.push({ actor, lamport, path: d.path, blob: d.newBlob, base: d.oldBlob ?? undefined });
    this.appendOps(branch, ops, newOid);
    const tip = await this.materialize(branch);                // push replies synchronously with the real tip
    return pktLines([`unpack ok`, `ok ${ref}`, `option new-oid ${tip}`, `option forced-update`]); // report-status-v2
  }

  async alarm() {
    for (const { branch } of this.ctx.storage.sql.exec("SELECT branch FROM dirty").toArray() as { branch: string }[])
      await this.materialize(branch);
  }

  // ---- merge + materialize --------------------------------------------------------
  private async materialize(branch: string): Promise<string> {
    const sql = this.ctx.storage.sql;
    const ops = sql.exec("SELECT * FROM crdt_ops WHERE branch=? ORDER BY lamport, actor", branch).toArray() as any[];
    const parents = new Set<string>(); const prev = sql.exec("SELECT sha FROM refs WHERE name=?", `refs/heads/live/${branch}`).toArray()[0]?.sha as string | undefined;
    if (prev) parents.add(prev);
    for (const o of ops) {
      if (o.commit_sha) parents.add(o.commit_sha);
      const cur = sql.exec("SELECT blob, lamport, actor FROM crdt_state WHERE branch=? AND path=?", branch, o.path).toArray()[0] as any;
      let blob = o.blob;
      if (cur && o.base && cur.blob !== o.base && blob)         // concurrent edit of same file: diff3(base, ours, theirs)
        blob = await diff3Blob(this.env.BUCKET, o.base, cur.blob, blob) ?? blob;   // null => conflict, LWW wins (see limits)
      if (blob === null) sql.exec("DELETE FROM crdt_state WHERE branch=? AND path=?", branch, o.path);
      else sql.exec("INSERT OR REPLACE INTO crdt_state VALUES (?,?,?,?,?)", branch, o.path, blob, o.lamport, o.actor);
    }
    const files = sql.exec("SELECT path, blob FROM crdt_state WHERE branch=? ORDER BY path", branch).toArray() as { path: string; blob: string }[];
    const tree = await this.writeTree(files);                   // nested git tree objects
    const body = `tree ${tree}\n` + [...parents].map(p => `parent ${p}\n`).join("") +
      `author git-edge <live@edge> ${Math.floor(Date.now() / 1000)} +0000\ncommitter git-edge <live@edge> ${Math.floor(Date.now() / 1000)} +0000\n\n` +
      `live merge of ${branch}\n\nCrdt-Ops: ${ops.length}\n`;
    const commit = await this.putObject("commit", enc.encode(body));
    this.ctx.storage.transactionSync(() => {
      sql.exec("INSERT OR REPLACE INTO refs VALUES (?,?)", `refs/heads/live/${branch}`, commit);
      sql.exec("DELETE FROM crdt_ops WHERE branch=?", branch);  // compaction: state table is the CRDT snapshot
      sql.exec("DELETE FROM dirty WHERE branch=?", branch);
    });
    return commit;
  }

  private async writeTree(files: { path: string; blob: string }[], prefix = ""): Promise<string> {
    const entries: Uint8Array[] = []; const dirs = new Map<string, typeof files>();
    for (const f of files) {
      const rel = f.path.slice(prefix.length); const slash = rel.indexOf("/");
      if (slash < 0) entries.push(concat(enc.encode(`100644 ${rel}\0`), unhex(f.blob)));       // "<mode> <name>\0<20-byte sha>"
      else { const d = rel.slice(0, slash); (dirs.get(d) ?? dirs.set(d, []).get(d)!).push(f); }
    }
    for (const [d, sub] of dirs) entries.push(concat(enc.encode(`40000 ${d}\0`), unhex(await this.writeTree(sub, `${prefix}${d}/`))));
    return this.putObject("tree", concat(...entries.sort(gitTreeOrder)));
  }

  private async putObject(type: string, payload: Uint8Array): Promise<string> {
    const raw = concat(enc.encode(`${type} ${payload.byteLength}\0`), payload);   // loose-object header, sha1 over it
    const sha = hex(await crypto.subtle.digest("SHA-1", raw));
    await this.env.BUCKET.put(`objects/${sha}`, raw);            // idempotent (content-addressed-r2-keys)
    return sha;
  }
}
```

## Why it works
- git does not care how a commit came to exist, only that `tree`/`commit` bytes hash to the sha the ref advertises; `putObject` produces byte-exact loose-object content (`"<type> <len>\0"` + payload, tree entries `mode SP name NUL sha20` in git's sort order), so `git fsck` on a clone of `refs/heads/live/foo` passes.
- `git-receive-pack` only requires `unpack ok` and one `ok <ref>` status line per command; with `report-status-v2` (git >= 2.29, the proc-receive hook protocol) the server may add `option new-oid <sha>` / `option forced-update`, and the client then sets `refs/remotes/origin/live/foo` to the materialized merge rather than to what it pushed. This is exactly the mechanism GitHub-style "push rewritten by server" hooks use.
- Because every contributing pushed commit becomes a parent of the materialized merge, each writer's next `git pull` is a fast-forward (their commit is an ancestor of the tip), so classic clients never see a rejected push or a forced divergence on a live branch.
- The CRDT is a per-path LWW register with `(lamport, actor)` total order plus an idempotent op log keyed by `(branch, actor, lamport, path)`: replaying a retried push or agent POST is a no-op, and the DO's single-threaded event loop is the serialization point, so merge order is deterministic without vector clocks across regions.
- Blobs never touch the DO; agents put them into R2 by sha before posting ops, git pushes have them indexed by `streaming-pack-parser`, and materialization only writes small tree/commit objects, so per-materialization R2 writes are O(changed directories + 1).
- `refs/heads/live/*` is served by `ls-refs`/`fetch` exactly like any ref once materialized; non-live refs keep classic compare-and-swap, so the wild behaviour is opt-in per namespace.

## Known limits
- "CRDT" is true at path granularity only: concurrent edits to the same file are reconciled by diff3 when a common base blob is known, and a real diff3 conflict falls back to LWW (the loser's version is preserved only as a parent commit, not in the tree). A line-level text CRDT would need every writer to send character ops, which `git push` cannot express; this is the honest shape of the idea.
- Materialization runs inside the DO: tree building is O(files) SQLite rows and one R2 PUT per changed directory, and diff3 reads two or three blobs from R2 per conflicting path. A 100k-file branch with many touched dirs will approach the 30s wall-clock budget of a single alarm/request; split by dirty-directory pages across alarms (not shown).
- `Date.now()` as a Lamport clock is a hand-wave; correct behaviour needs `lamport = max(seen)+1` per actor, stored in a small `clocks` table, or ties between two actors in the same millisecond resolve by actor id string order.
- Every push to a live branch creates a new merge commit, so history is noisy (one commit per debounce window per branch); DO memory (128MB) is not the constraint, R2 PUT cost per materialization and commit-graph growth are.
- One DO per repo means all live branches of a repo serialize through one event loop; a busy repo should shard live branches into branch DOs (`branch-level-dos`).
- Old clients (git < 2.29, no `report-status-v2`) get plain `ok <ref>` and their remote-tracking ref points at their own pushed commit until the next fetch; harmless but confusing.
- `treeDiff`, `diff3Blob`, `parseCommandPktLines`, `pktLines`, `gitTreeOrder` and `concat` are pseudo; tree walking against R2 is one GET per tree object unless `in-do-object-cache` fronts it.

## Depends on
repo-do-ref-authority, refs-sqlite-objects-r2, content-addressed-r2-keys, streaming-pack-parser, info-refs-endpoint, server-side-merge
