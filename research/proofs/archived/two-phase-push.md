> Idea #6 · foundation · verdict: **lands with caveats** · feasibility 4/5 · reliability 3/5 · correctness 3/5 · effort: weeks
> Proof: [proofs/two-phase-push.md](./two-phase-push.md) · Review: [reviews/two-phase-push.md](../../reviews/archived/two-phase-push.md)

# Two-phase push

## Mechanism
`POST /:owner/:repo/git-receive-pack` lands on the Worker, which first calls `repoDO.begin()` so the repo DO records an open push id in SQLite (and arms the janitor alarm) before a single byte hits R2. The Worker then streams the request body: it reads the pkt-line command section (`<old> <new> <refname>\0report-status ...`), then inflates the `PACK` stream object by object, writing each loose object to R2 at `objects/<sha>` (content-addressed, idempotent) and accumulating a manifest of every sha plus the links it extracted (commit→tree/parents, tree→entries), which it writes to `pending/<pushId>/manifest.json`. Phase two is `repoDO.commit(pushId)`: the DO reads the manifest, checks each ref's old value against its `refs` table (compare-and-swap), checks connectivity using only the manifest links and its own `objects` index (no R2 reads), and in one SQLite transaction inserts the new object rows, moves the refs and closes the push; the Worker turns the result into `report-status` pkt-lines. A crash between the phases leaves an open push row; the alarm sweeps rows older than the push timeout and deletes their R2 keys unless the sha is already committed or claimed by another still-open push.

## Primitives
- Workers (request body streaming, `DecompressionStream("deflate")` for zlib object bodies, `crypto.subtle.digest("SHA-1")`)
- Durable Objects with SQLite storage (`ctx.storage.sql.exec`, `ctx.storage.transactionSync`) — GA
- DO alarms (`ctx.storage.setAlarm`, `alarm()`) — GA
- DO RPC (`WorkerEntrypoint`/`DurableObject` extends from `cloudflare:workers`, typed stub methods) — GA
- R2 (`put`, `get`, `delete` of up to 1000 keys per call) — GA
- Nothing beta is required.

## Proof code
```typescript
import { DurableObject } from "cloudflare:workers";
type Env = { REPO: DurableObjectNamespace<RepoDO>; BUCKET: R2Bucket };
type Cmd = { old: string; new: string; ref: string };
type Manifest = { cmds: Cmd[]; objects: Record<string, string[]> }; // sha -> shas it points at
const PUSH_TIMEOUT_MS = 15 * 60_000;
const ZERO = "0".repeat(40);

export default {
  async fetch(req: Request, env: Env) {
    const [, owner, repo] = new URL(req.url).pathname.split("/"); // /:owner/:repo/git-receive-pack
    const stub = env.REPO.get(env.REPO.idFromName(`${owner}/${repo}`));
    const pushId = await stub.begin();                       // phase 0: DO knows before R2 is touched
    const reader = req.body!.getReader();
    const cmds: Cmd[] = [];                                  // pkt-lines "<old> <new> <ref>\0caps" until "0000"
    for await (const line of pktLines(reader)) { const [o, n, r] = line.split("\0")[0].split(" "); cmds.push({ old: o, new: n, ref: r }); }
    const manifest: Manifest = { cmds, objects: {} };
    // phase 1: "PACK" + version + count, then per-object header (type/size varint) + zlib body;
    // ofs-delta/ref-delta are resolved against R2 or earlier objects in this pack (see streaming-pack-parser)
    for await (const obj of parsePack(reader, env.BUCKET)) {  // {sha, type, body} with sha = SHA1("<type> <size>\0"+body)
      await env.BUCKET.put(`objects/${obj.sha}`, zlibLoose(obj)); // idempotent: same key, same bytes on retry
      manifest.objects[obj.sha] = linksOf(obj);              // commit: [tree, ...parents]; tree: entry shas; blob: []
    }
    await env.BUCKET.put(`pending/${pushId}/manifest.json`, JSON.stringify(manifest));
    const results = await stub.commit(pushId);               // phase 2
    // report-status: "unpack ok\n" then "ok <ref>\n" | "ng <ref> <reason>\n", then flush; sideband omitted here
    const body = ["unpack ok\n", ...results.map(r => r.ok ? `ok ${r.ref}\n` : `ng ${r.ref} ${r.reason}\n`)].map(pkt).join("") + "0000";
    return new Response(body, { headers: { "content-type": "application/x-git-receive-pack-result" } });
  },
};

export class RepoDO extends DurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS refs    (name TEXT PRIMARY KEY, sha TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS objects (sha TEXT PRIMARY KEY);
      CREATE TABLE IF NOT EXISTS pending (id TEXT PRIMARY KEY, started_at INTEGER NOT NULL)`);
  }
  async begin(): Promise<string> {
    const id = crypto.randomUUID();
    this.ctx.storage.sql.exec("INSERT INTO pending (id, started_at) VALUES (?, ?)", id, Date.now());
    if ((await this.ctx.storage.getAlarm()) === null) await this.ctx.storage.setAlarm(Date.now() + PUSH_TIMEOUT_MS);
    return id;
  }
  async commit(pushId: string) {
    const row = this.ctx.storage.sql.exec<{ started_at: number }>("SELECT started_at FROM pending WHERE id = ?", pushId).toArray()[0];
    if (!row || Date.now() - row.started_at > PUSH_TIMEOUT_MS) throw new Error("push expired; janitor may have swept it");
    const m = await (await this.env.BUCKET.get(`pending/${pushId}/manifest.json`))!.json<Manifest>();
    const known = (sha: string) => sha in m.objects ||
      this.ctx.storage.sql.exec("SELECT 1 FROM objects WHERE sha = ?", sha).toArray().length > 0;
    return this.ctx.storage.transactionSync(() => {           // all-or-nothing, single-threaded per repo
      const results = m.cmds.map(c => {
        const cur = this.ctx.storage.sql.exec<{ sha: string }>("SELECT sha FROM refs WHERE name = ?", c.ref).toArray()[0]?.sha ?? ZERO;
        if (cur !== c.old) return { ref: c.ref, ok: false, reason: "fetch first" };   // CAS failed
        if (c.new !== ZERO && !known(c.new)) return { ref: c.ref, ok: false, reason: "missing necessary objects" };
        return { ref: c.ref, ok: true };
      });
      // connectivity: every link out of a new object must land in this manifest or the committed index
      for (const [sha, links] of Object.entries(m.objects))
        if (!links.every(known)) throw new Error(`unpack error: ${sha} references a missing object`);
      if (results.some(r => !r.ok)) return results;         // atomic option: reject all; per-ref is the default
      for (const sha of Object.keys(m.objects)) this.ctx.storage.sql.exec("INSERT OR IGNORE INTO objects (sha) VALUES (?)", sha);
      for (const c of m.cmds) c.new === ZERO
        ? this.ctx.storage.sql.exec("DELETE FROM refs WHERE name = ?", c.ref)
        : this.ctx.storage.sql.exec("INSERT OR REPLACE INTO refs (name, sha) VALUES (?, ?)", c.ref, c.new);
      this.ctx.storage.sql.exec("DELETE FROM pending WHERE id = ?", pushId);
      this.ctx.waitUntil(this.env.BUCKET.delete(`pending/${pushId}/manifest.json`));
      return results;
    });
  }
  async alarm() {                                              // janitor: sweep pushes that never reached phase 2
    const cutoff = Date.now() - PUSH_TIMEOUT_MS;
    const stale = this.ctx.storage.sql.exec<{ id: string }>("SELECT id FROM pending WHERE started_at < ?", cutoff).toArray();
    const open = new Set<string>();                            // shas claimed by pushes still in flight: never delete those
    for (const p of this.ctx.storage.sql.exec<{ id: string }>("SELECT id FROM pending WHERE started_at >= ?", cutoff).toArray()) {
      const m = await (await this.env.BUCKET.get(`pending/${p.id}/manifest.json`))?.json<Manifest>();
      for (const sha of Object.keys(m?.objects ?? {})) open.add(sha);
    }
    for (const p of stale) {
      const m = await (await this.env.BUCKET.get(`pending/${p.id}/manifest.json`))?.json<Manifest>();
      const orphans = Object.keys(m?.objects ?? {}).filter(sha => !open.has(sha) &&
        this.ctx.storage.sql.exec("SELECT 1 FROM objects WHERE sha = ?", sha).toArray().length === 0);
      for (let i = 0; i < orphans.length; i += 1000) await this.env.BUCKET.delete(orphans.slice(i, i + 1000).map(s => `objects/${s}`));
      await this.env.BUCKET.delete(`pending/${p.id}/manifest.json`);
      this.ctx.storage.sql.exec("DELETE FROM pending WHERE id = ?", p.id);
    }
    if (this.ctx.storage.sql.exec("SELECT 1 FROM pending LIMIT 1").toArray().length) await this.ctx.storage.setAlarm(Date.now() + PUSH_TIMEOUT_MS);
  }
}
```

## Why it works
- `git-receive-pack` semantics are exactly "here is a pack, here are ref commands, tell me per ref": the wire format needs only `unpack ok` and `ok/ng <ref>` pkt-lines after the pack is consumed, and nothing requires the server to have finished storing before it starts reading, so R2 writes can happen while the body streams.
- Git's own guarantee on push is per-ref compare-and-swap of `<old>` (what the client last fetched) against the current tip; the DO is single-threaded per repo, so `SELECT sha FROM refs` followed by the write inside `transactionSync` is a true CAS with no distributed lock. Concurrent pushes to the same ref serialize; the loser gets `ng <ref> fetch first`, which is what git prints today.
- Connectivity is the check upstream `receive-pack` does with `rev-list --objects --not --all`: every object reachable from the new tip must exist. Because the Worker already inflated every object to hash it, extracting `tree`/`parent` lines and tree entries is free, so the DO validates from the manifest plus its `objects` index and never reads object bytes from R2.
- Objects are immutable and content-addressed, so a retried push (client re-sends the same pack after a 5xx) rewrites identical bytes to identical keys; nothing about phase one is observable until phase two flips the refs, which is why the pending manifest, not the object keys, is the unit of "in flight".
- The order `begin()` → R2 writes → `commit()` means the DO always knows about a push before any orphan can exist, so the janitor never needs to list the bucket; it reads only manifests it created and skips any sha claimed by a still-open push, which closes the race where two pushes share a blob and one is swept.
- The alarm is re-armed only while `pending` is non-empty, so an idle repo costs nothing; a DO with an alarm set survives eviction and the sweep runs even if no client ever returns.

## Known limits
- "Pending prefix for objects" is not literally what is written: R2 has no rename, so writing objects under `pending/<id>/objects/*` and moving them on commit would double every write. The proof keeps objects at their final content-addressed key and puts only the manifest under `pending/`; orphans are therefore committed-looking keys that the `objects` index does not know about. Reads must consult the index (or a reachable ref) and never trust key presence alone.
- One R2 class-A PUT per object: a 50k-object push is 50k PUTs (roughly $0.23 at $4.50/M) and tens of seconds of wall time even with concurrency; batching small objects into a pack-per-push in R2 (one multipart upload, then range reads) is the real fix and is a sibling idea, not this one.
- The Worker must inflate every object to compute its SHA-1 and to extract links; a delta-heavy pack needs base objects, which means R2 GETs during parse (`streaming-pack-parser`). CPU is capped at 30s by default (configurable up to 5 min on paid plans); a monorepo initial push will exceed it unless split (`presigned-direct-upload`).
- The manifest is a single JSON object; at ~60 bytes per entry a million-object push is a 60MB manifest that the DO must parse in its 128MB heap. Chunking the manifest is straightforward but not shown.
- Connectivity is validated from manifest-declared links, not by re-reading the objects, so it trusts the Worker that produced the manifest. That is acceptable because the Worker is first-party code, but a corrupted object body with a correct sha would not be caught here (git's own `fsck` is equally optional on receive).
- The `objects` table grows one row per object in SQLite; DO storage is 10GB per object, fine for refs and a sha index but the `SELECT 1 ... WHERE sha = ?` per link is a few microseconds each, so a million-link commit spends about a second of DO CPU inside one transaction, during which the repo takes no other request.
- Per-ref success with a partial failure (default git behaviour) means a crashed Worker after `commit()` returned but before `report-status` reached the client leaves refs advanced while the client sees an error; that matches how git over HTTP already behaves and the client's next fetch reconciles it.
- Sweep timeout is a fixed 15 minutes; a legitimately slow push that runs longer is rejected at `commit()` with "push expired" rather than risking a sweep under its feet. `atomic` push-option (all-or-nothing across refs) is one line, shown as a comment, not wired.

## Depends on
- repo-do-ref-authority
- refs-sqlite-objects-r2
- streaming-pack-parser
- content-addressed-r2-keys
- info-refs-endpoint
