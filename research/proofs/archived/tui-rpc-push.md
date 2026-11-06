> Idea #29 · edge · verdict: **lands with caveats** · feasibility 4/5 · reliability 3/5 · correctness 3/5 · effort: weeks
> Proof: [proofs/tui-rpc-push.md](./tui-rpc-push.md) · Review: [reviews/tui-rpc-push.md](../../reviews/archived/tui-rpc-push.md)

# Push from a sibling workspace DO over RPC, no HTTP

## Mechanism
The TUI hits `POST /sessions/:id/push` on the grok-pi Worker, which routes to that session's `AgentSession` DO. The DO snapshots its SQLite `files` table into real git objects (blob -> tree -> commit, SHA-1 hashed, zlib-deflated loose format) and writes each one to the shared R2 bucket at `objects/<sha>` (idempotent, content-addressed). It then calls `env.REPO.get(idFromName("owner/repo")).commitPush({ref, oldOid, newOid, oids})` — a JS RPC method on the repo DO (`extends DurableObject`), no pkt-line, no PACK, no `git-receive-pack`. The repo DO checks every oid exists in R2, compare-and-swaps the ref row in its SQLite, appends a reflog row, and returns the outcome; this is the same phase-two entry point the HTTP push path uses after it has unpacked a PACK.

## Primitives
- Durable Objects with JS RPC (`extends DurableObject` from `cloudflare:workers`, stub method calls) — GA
- DO SQLite storage (`ctx.storage.sql.exec`) for refs and reflog — GA
- R2 `put` / `head` for loose objects, shared bucket binding across both DOs — GA
- `crypto.subtle.digest("SHA-1")` and `CompressionStream("deflate")` (zlib framing, same as git loose objects) — GA
- Cross-script DO binding (`script_name` in wrangler `durable_objects.bindings`) if the repo DO lives in a separate `git-edge` Worker; RPC works over it — GA
- Optional: `ReadableStream` as an RPC argument to stream a PACK instead of JSON manifest — GA (RPC supports streams)

## Proof code
```typescript
import { DurableObject } from "cloudflare:workers";

type Env = { BUCKET: R2Bucket; REPO: DurableObjectNamespace<RepoDO>; AGENT_SESSION: DurableObjectNamespace<AgentSession> };
type PushReq = { ref: string; oldOid: string | null; newOid: string; oids: string[] };
type PushRes = { ok: true } | { ok: false; reason: "stale" | "missing-objects" };

const enc = new TextEncoder();
const hex = (b: ArrayBuffer) => [...new Uint8Array(b)].map((x) => x.toString(16).padStart(2, "0")).join("");
const cat = (...xs: Uint8Array[]) => { const o = new Uint8Array(xs.reduce((n, x) => n + x.length, 0)); let p = 0; for (const x of xs) { o.set(x, p); p += x.length; } return o; };

/** git object = "<type> <len>\0" + body; oid = SHA-1 of that; loose file = zlib(header+body). */
async function writeObject(bucket: R2Bucket, type: "blob" | "tree" | "commit", body: Uint8Array) {
  const raw = cat(enc.encode(`${type} ${body.length}\0`), body);
  const oid = hex(await crypto.subtle.digest("SHA-1", raw));
  const z = new Response(new Blob([raw]).stream().pipeThrough(new CompressionStream("deflate")));
  await bucket.put(`objects/${oid}`, await z.arrayBuffer()); // idempotent: content-addressed key
  return oid;
}

/** The grok-pi workspace DO (files live in SQLite). One new RPC method: pushWorkspace. */
export class AgentSession extends DurableObject<Env> {
  async pushWorkspace(repo: string, branch: string, message: string, parent: string | null): Promise<PushRes> {
    const files = this.ctx.storage.sql
      .exec<{ path: string; content: string }>("SELECT path, content FROM files ORDER BY path").toArray();
    const oids: string[] = [];
    // Flat tree for the proof; nested dirs = recurse, sort dirs as if name had a trailing "/".
    const entries: { name: string; oid: string }[] = [];
    for (const f of files) {
      const oid = await writeObject(this.env.BUCKET, "blob", enc.encode(f.content));
      oids.push(oid); entries.push({ name: f.path, oid });
    }
    entries.sort((a, b) => (a.name < b.name ? -1 : 1)); // git tree entries are byte-sorted by name
    const tree = cat(...entries.map((e) =>
      cat(enc.encode(`100644 ${e.name}\0`), Uint8Array.from(e.oid.match(/../g)!.map((h) => parseInt(h, 16))))));
    const treeOid = await writeObject(this.env.BUCKET, "tree", tree); oids.push(treeOid);
    const who = `grok-pi <agent@grok-pi> ${Math.floor(Date.now() / 1000)} +0000`;
    const commit = enc.encode(`tree ${treeOid}\n${parent ? `parent ${parent}\n` : ""}author ${who}\ncommitter ${who}\n\n${message}\n`);
    const newOid = await writeObject(this.env.BUCKET, "commit", commit); oids.push(newOid);

    // No HTTP: a typed RPC call on the repo DO stub, args are plain structured-clonable JSON.
    const stub = this.env.REPO.get(this.env.REPO.idFromName(repo));
    return stub.commitPush({ ref: `refs/heads/${branch}`, oldOid: parent, newOid, oids });
  }
}

/** The git-edge repo DO: ref authority. commitPush is phase two of two-phase-push, shared with HTTP receive-pack. */
export class RepoDO extends DurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS refs (name TEXT PRIMARY KEY, oid TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS reflog (id INTEGER PRIMARY KEY, name TEXT, old_oid TEXT, new_oid TEXT, at INTEGER)`);
  }
  async commitPush(p: PushReq): Promise<PushRes> {
    // Connectivity check: every object the pusher claims must be in R2 (loose or already packed).
    const heads = await Promise.all(p.oids.map((o) => this.env.BUCKET.head(`objects/${o}`)));
    if (heads.some((h) => h === null)) return { ok: false, reason: "missing-objects" };
    // CAS on the ref, serialized by the single-DO execution model (same guarantee as receive-pack's ref lock).
    const cur = this.ctx.storage.sql.exec<{ oid: string }>("SELECT oid FROM refs WHERE name = ?", p.ref).toArray()[0]?.oid ?? null;
    if (cur !== p.oldOid) return { ok: false, reason: "stale" };
    this.ctx.storage.sql.exec("INSERT INTO refs (name, oid) VALUES (?, ?) ON CONFLICT(name) DO UPDATE SET oid = excluded.oid", p.ref, p.newOid);
    this.ctx.storage.sql.exec("INSERT INTO reflog (name, old_oid, new_oid, at) VALUES (?, ?, ?, ?)", p.ref, p.oldOid, p.newOid, Date.now());
    return { ok: true };
  }
}

// Worker route glue (grok-pi style): POST /sessions/:id/push {repo, branch, message, parent}
export default {
  async fetch(req: Request, env: Env) {
    const m = new URL(req.url).pathname.match(/^\/sessions\/([\w.-]+)\/push$/);
    if (!m || req.method !== "POST") return new Response("not found", { status: 404 });
    const b = await req.json<{ repo: string; branch: string; message: string; parent: string | null }>();
    const ws = env.AGENT_SESSION.get(env.AGENT_SESSION.idFromName(m[1]));
    return Response.json(await ws.pushWorkspace(b.repo, b.branch, b.message, b.parent));
  },
};
```

## Why it works
- `git-receive-pack` over HTTP is just transport for two things: "here are objects" (the PACK) and "move ref X from old to new" (the pkt-line command list). The RPC call carries exactly those two things; the objects go to R2 directly and the command becomes `commitPush({ref, oldOid, newOid})`.
- The objects written are byte-for-byte real git objects: `"<type> <size>\0" + body` SHA-1'd for the oid, zlib-deflated on disk — the same loose-object format `git hash-object -w` produces. Any later `git clone`/`fetch` through the HTTP path (`want-have-negotiation`, `streaming-pack-parser` output) can inflate `objects/<sha>` and pack it into a PACK for the client with no translation.
- Tree entries are `"<mode> <name>\0<20-byte raw sha>"` byte-sorted by name, and the commit body uses git's exact header layout (`tree`, `parent`, `author`, `committer`, blank line, message), so `git fsck` on the cloned result passes.
- The CAS in `commitPush` (`cur === oldOid`) is the same semantics as receive-pack's `old-oid new-oid ref` line plus the ref lock: a stale `parent` gets a rejection instead of clobbering another push. Single-DO execution serializes it with concurrent HTTP pushes to the same repo.
- Existence check with `R2.head` before flipping the ref preserves the two-phase-push invariant that a ref never points at unreachable objects; a crash before `commitPush` leaves only orphan loose objects for the GC alarm.
- RPC args are structured-clonable JSON (oid strings), and the DO stub is typed via `DurableObjectNamespace<RepoDO>`, so the compiler checks the contract that pkt-line never could.

## Known limits
- The workspace DO never builds a PACK, so it does no deltas: every push writes N+2 loose R2 objects (one PUT each, ~$4.50/M class A). Unchanged files are re-hashed but the PUT is idempotent; skipping the PUT for oids already in the last snapshot is an easy cache (`in-do-object-cache`) not shown here.
- The proof uses a flat tree; real workspaces need nested trees (recurse per directory, git's dir-sorts-as-`name/` rule, `040000` mode). Hand-waved, not hard.
- Hashing and deflating happen in the workspace DO's 128 MB / CPU-limited request; a workspace with thousands of files or multi-MB blobs should chunk across alarms or stream a PACK over RPC (`ReadableStream` is a legal RPC arg) to the repo DO's existing `streaming-pack-parser`.
- RPC message payloads have a serialization cap (order of tens of MB); the oid manifest is tiny but do not pass file contents through RPC — they go to R2.
- `commitPush` does `head` per oid; for large pushes batch via `R2.list` with prefix or trust a per-DO `known_objects` SQLite table (`content-addressed-r2-keys`). The HTTP receive-pack path can share the same method only once `two-phase-push` lands.
- Both DOs must share the same R2 bucket binding; if the repo DO lives in a separate Worker, use a `script_name` DO binding (GA), still no HTTP hop.
- No pre-receive hooks or auth here; `auth-and-multitenancy` decides who may call `commitPush`, and since RPC is inside the account boundary the workspace DO is implicitly trusted.

## Depends on
repo-do-ref-authority, refs-sqlite-objects-r2, content-addressed-r2-keys, two-phase-push
