> Idea #18 · edge · verdict: **risky** · feasibility 4/5 · reliability 2/5 · correctness 2/5 · effort: weeks
> Proof: [proofs/server-side-rebase.md](./server-side-rebase.md) · Review: [reviews/server-side-rebase.md](../../reviews/archived/server-side-rebase.md)

# Server-side rebase and squash as protocol v2 extensions

## Mechanism
The Worker's `GET /:owner/:repo/info/refs?service=git-upload-pack` (with `Git-Protocol: version=2`) advertises two extra capability lines, `rebase=squash` and `rebase-status`, next to the standard `ls-refs`/`fetch`/`object-info`; git's v2 spec makes clients ignore capability lines they do not understand, so stock `git` sees nothing new and only a git-edge-aware client sends `command=rebase` on `POST /git-upload-pack`. The Worker parses the pkt-line command section (`onto`, `branch`, `expect` for compare-and-swap, optional `squash`, `message`) and forwards it over RPC to the repo Durable Object (`idFromName("owner/repo")`), which serializes the operation with every push. The DO walks `merge-base..branch` from its SQLite commit-graph table, replays each commit as a three-way tree merge against the moving `onto` tip, writes each new commit/tree object as a zlib loose object to `R2 objects/<sha>` (idempotent, content-addressed), and finally CAS-flips `refs/heads/<branch>` in SQLite and appends a reflog row. Replays longer than one request budget checkpoint their cursor in a `rebase_jobs` row and continue from `alarm()`; the client polls `command=rebase-status` until it gets `ack <newtip>` or a `conflict` section, then does a normal `fetch` + `reset --hard`.

## Primitives
- Durable Objects with SQLite storage (`ctx.storage.sql`) — refs, commit-graph, `rebase_jobs` (GA)
- DO alarms (`ctx.storage.setAlarm`) — continuation of long replays (GA)
- DO RPC (Worker → DO method call via `env.REPO.get(id).rebase(...)`) (GA)
- R2 `put`/`get` for loose objects (GA)
- Web `CompressionStream("deflate")` / `DecompressionStream("deflate")` for zlib loose objects (GA in workerd)
- `crypto.subtle.digest("SHA-1")` for object ids (GA)
- Optional: Wasm merge core for blob-level diff3 — see Depends on (not a Cloudflare primitive; Wasm modules on Workers are GA)

## Proof code
```typescript
// ---- Worker: v2 capability advertisement + command dispatch ------------------
const pkt = (s: string) => (s.length + 4).toString(16).padStart(4, "0") + s;
const FLUSH = "0000", DELIM = "0001";

export default {
  async fetch(req: Request, env: Env) {
    const url = new URL(req.url);
    const [, owner, repo, tail] = url.pathname.match(/^\/([^/]+)\/([^/]+)\/(.*)$/)!;
    const stub = env.REPO.get(env.REPO.idFromName(`${owner}/${repo}`));

    if (tail === "info/refs" && req.headers.get("Git-Protocol")?.includes("version=2")) {
      // Stock git ignores the two unknown capability lines below.
      const caps = ["version 2\n", "agent=git-edge/0.1\n", "ls-refs\n",
        "fetch=shallow wait-for-done\n", "object-info\n", "server-option\n",
        "rebase=squash\n", "rebase-status\n"];
      return new Response(caps.map(pkt).join("") + FLUSH,
        { headers: { "content-type": "application/x-git-upload-pack-advertisement" } });
    }

    if (tail === "git-upload-pack" && req.method === "POST") {
      const lines = parsePktLines(new Uint8Array(await req.arrayBuffer())); // pkt-line -> string[]
      const cmd = lines.find(l => l.startsWith("command="))?.slice(8);
      const args = Object.fromEntries(lines.slice(lines.indexOf(DELIM) + 1)
        .map(l => l.trim().split(/ (.*)/s)) as [string, string][]);
      if (cmd === "rebase") {
        const r = await stub.rebase(args.onto, args.branch, args.expect, "squash" in args, args.message);
        const body = r.kind === "ok" ? pkt(`ack ${r.tip}\n`)
          : r.kind === "pending" ? pkt(`pending ${r.job}\n`)
          : pkt("conflict\n") + DELIM + r.paths.map(p => pkt(p + "\n")).join("");
        return new Response(body + FLUSH, { headers: { "content-type": "application/x-git-upload-pack-result" } });
      }
      // ls-refs / fetch / object-info handled elsewhere (protocol-v2-only)
    }
    return new Response("not found", { status: 404 });
  },
};

// ---- Durable Object: the only writer for this repo's refs ---------------------
export class RepoDO extends DurableObject<Env> {
  sql = this.ctx.storage.sql;
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.sql.exec(`CREATE TABLE IF NOT EXISTS refs(name TEXT PRIMARY KEY, oid TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS commits(oid TEXT PRIMARY KEY, tree TEXT, parents TEXT, raw BLOB);
      CREATE TABLE IF NOT EXISTS rebase_jobs(id TEXT PRIMARY KEY, branch TEXT, expect TEXT,
        todo TEXT, head TEXT, done INTEGER, result TEXT)`);
  }

  async rebase(onto: string, branch: string, expect: string, squash: boolean, msg?: string) {
    const tip = (o: string) => this.sql.exec("SELECT oid FROM refs WHERE name=?", o).one().oid as string;
    if (tip(branch) !== expect) return { kind: "conflict", paths: ["<stale: branch moved>"] } as const;
    const base = this.mergeBase(tip(onto), tip(branch));                 // walk commits table, no R2
    const todo = this.revList(base, tip(branch));                          // oldest → newest
    const id = crypto.randomUUID();
    this.sql.exec("INSERT INTO rebase_jobs VALUES(?,?,?,?,?,0,NULL)", id, branch, expect,
      JSON.stringify(squash ? [{ squashOf: todo, msg }] : todo), tip(onto));
    return this.step(id, 40);                                              // do as much as fits now
  }

  // Replay up to `budget` commits, checkpoint, and let alarm() finish the rest.
  private async step(id: string, budget: number) {
    const job = this.sql.exec("SELECT * FROM rebase_jobs WHERE id=?", id).one();
    let head = job.head as string, todo = JSON.parse(job.todo as string);
    while (todo.length && budget-- > 0) {
      const c = todo.shift();
      const merged = await this.mergeTrees(this.parentTree(c), this.tree(head), this.tree(c)); // 3-way, tree-level
      if (merged.conflicts.length) {
        this.sql.exec("UPDATE rebase_jobs SET done=1,result=? WHERE id=?", JSON.stringify(merged.conflicts), id);
        return { kind: "conflict", paths: merged.conflicts } as const;
      }
      head = await this.writeCommit(merged.tree, [head], c.msg ?? this.message(c));
    }
    this.sql.exec("UPDATE rebase_jobs SET head=?,todo=? WHERE id=?", head, JSON.stringify(todo), id);
    if (todo.length) { await this.ctx.storage.setAlarm(Date.now()); return { kind: "pending", job: id } as const; }
    // CAS flip: expect is re-checked because pushes may have interleaved with alarm steps.
    const n = this.sql.exec("UPDATE refs SET oid=? WHERE name=? AND oid=?", head, job.branch, job.expect).rowsWritten;
    this.sql.exec("UPDATE rebase_jobs SET done=1,result=? WHERE id=?", n ? head : "stale", id);
    return n ? { kind: "ok", tip: head } as const : { kind: "conflict", paths: ["<stale: branch moved>"] } as const;
  }
  async alarm() {
    const j = this.sql.exec("SELECT id FROM rebase_jobs WHERE done=0 LIMIT 1").toArray()[0];
    if (j) await this.step(j.id as string, 40);
  }

  // Loose object: "commit <len>\0" + body, zlib-deflated, keyed by SHA-1 → idempotent R2 put.
  private async writeCommit(tree: string, parents: string[], msg: string) {
    const body = `tree ${tree}\n${parents.map(p => `parent ${p}\n`).join("")}` +
      `author git-edge <edge@example> ${Math.floor(Date.now() / 1e3)} +0000\n` +
      `committer git-edge <edge@example> ${Math.floor(Date.now() / 1e3)} +0000\n\n${msg}\n`;
    const raw = new TextEncoder().encode(`commit ${new TextEncoder().encode(body).length}\0${body}`);
    const oid = hex(await crypto.subtle.digest("SHA-1", raw));
    const z = await new Response(new Blob([raw]).stream().pipeThrough(new CompressionStream("deflate"))).arrayBuffer();
    await this.env.BUCKET.put(`repos/${this.ctx.id}/objects/${oid}`, z);
    this.sql.exec("INSERT OR IGNORE INTO commits VALUES(?,?,?,?)", oid, tree, JSON.stringify(parents), raw);
    return oid;
  }
  // mergeTrees(base, ours, theirs): parse "<mode> <name>\0<20B sha>" entries from R2 (or DO cache),
  // recurse where all three are trees; identical-on-one-side resolves trivially; blob/blob edits on
  // the same path go to diff3 (Wasm core) or are reported as conflicts. Writes new tree objects like writeCommit.
}
```

## Why it works
- Protocol v2 capability advertisement is an open list: `git` only acts on lines it knows (`ls-refs`, `fetch`, `object-info`, `server-option`, `agent`), so `rebase=squash` is silently ignored by every existing client. That is exactly what "unknown to old clients" means; a new command is not a new transport.
- `command=rebase` reuses the v2 request framing unchanged: capability lines, `0001` delim, argument lines, `0000` flush; the response uses the same pkt-line sectioning (`ack`, or `conflict` + delim + paths) that `fetch` uses for `acknowledgments`/`packfile`, so a client written against `pkt-line` needs no new codec.
- A rebase is `git cherry-pick` in a loop: for each commit C, three-way merge `parent(C)` (base), current head (ours), `C` (theirs) and commit the result on head. At tree granularity this is a pure function over immutable objects already in R2, so the DO never needs a working directory or index.
- New commits and trees are ordinary loose objects (`<type> <len>\0` header, zlib) under content-addressed keys, so a subsequent `fetch` by any client, old or new, serves them like anything pushed by `git-receive-pack`; nothing about the rewritten history is server-specific.
- The single repo DO is the ref authority, so the CAS `UPDATE refs ... WHERE oid=expect` gives the same non-fast-forward protection `git push` relies on, even when the replay spans several alarm steps interleaved with pushes.
- Squash is the degenerate case: one merge of the branch tip against `onto`, one commit; no per-commit replay, no alarm chain.

## Known limits
- Stock `git` cannot invoke this. The capability is advertised, but only a git-edge client (a `git edge rebase` helper speaking pkt-line over HTTP) sends `command=rebase`. The only stock-client bridge is a push-option (`git push -o rebase=main`), which is idea #17's channel, not a v2 command.
- Blob-level conflict resolution is hand-waved. Tree-level merge (add/delete/rename-less) is easy in TypeScript; same-file edits need diff3, which realistically means the Wasm git core. Without it, same-path edits are reported as conflicts, which is correct but conservative.
- Every tree object touched is an R2 GET (~µs of CPU but ~10-50 ms of latency each, and each billed as a Class B op). A wide tree with a deep replay is hundreds of round trips per commit; the in-DO object cache and pinned bases mitigate but do not remove this.
- DO memory is 128 MB and a single request is bounded by the CPU limit (30 s default, up to 5 min via `limits.cpu_ms`); the alarm-chained `rebase_jobs` cursor exists purely because of that. Very large blobs must be merged by streaming, not loaded.
- Single-DO serialization: a long rebase holds the same DO that serves pushes. Steps are short by design, but throughput on a hot monorepo branch degrades while a job runs (branch-level DOs would help).
- Rewritten commits lose the original committer/author identity unless the client passes them through; the proof hardcodes `git-edge` as author. Signed commits cannot be re-signed server-side without the client's key, so GPG/SSH signatures are dropped on replay.
- No abort/edit/reword interactive semantics; only replay and squash. `rebase-status` exists to poll, not to steer.

## Depends on
repo-do-ref-authority, refs-sqlite-objects-r2, protocol-v2-only, content-addressed-r2-keys, want-have-negotiation, server-side-merge, wasm-git-core, in-do-object-cache
