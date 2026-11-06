> Idea #1 · foundation · verdict: **lands with caveats** · feasibility 4/5 · reliability 3/5 · correctness 3/5 · effort: weeks
> Proof: [proofs/repo-do-ref-authority.md](./repo-do-ref-authority.md) · Review: [reviews/repo-do-ref-authority.md](../../reviews/archived/repo-do-ref-authority.md)

# One Durable Object per repo as the ref authority

## Mechanism
`POST /:owner/:repo/git-receive-pack` lands on a stateless Worker, which reads the pkt-line command section (`<old-sha> <new-sha> <refname>\0caps`), streams the trailing `PACK` body to R2 under `pending/<pushId>/` (that part is idea #6), then calls `env.REPO.idFromName("owner/repo")` and invokes one RPC method `updateRefs(cmds, pushId)` on the single `RepoDO` instance. The DO owns the `refs` table in its SQLite storage and performs every ref move as `UPDATE ... WHERE name=? AND sha=?` (compare-and-swap) inside `transactionSync`, with no `await` between the read and the write, so the platform's single-threaded execution plus input gates give serialization for free. The DO returns per-ref ok/ng results; the Worker encodes them as the `report-status` pkt-lines git expects. `GET /info/refs?service=...` and `ls-refs` read the same table through `listRefs()`.

## Primitives
- Durable Objects with SQLite storage (`ctx.storage.sql.exec`, `ctx.storage.transactionSync`) — GA
- DO RPC methods on the stub (`stub.updateRefs(...)`, extends `DurableObject` from `cloudflare:workers`) — GA
- `idFromName(owner/repo)` for deterministic single-instance routing — GA
- R2 bucket binding (`env.BUCKET.head`) for "does the new tip object exist" — GA
- DO alarms (`ctx.storage.setAlarm`) only for the orphan-sweep hook, not needed for the CAS itself — GA
- Workers streams / `TextEncoder` for pkt-line output — GA

## Proof code
```typescript
import { DurableObject } from "cloudflare:workers";

type Env = { REPO: DurableObjectNamespace<RepoDO>; BUCKET: R2Bucket };
type RefCmd = { old: string; new: string; name: string };
type RefResult = { name: string; ok: boolean; reason?: string };
const ZERO = "0".repeat(40);

export class RepoDO extends DurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS refs (
      name TEXT PRIMARY KEY, sha TEXT NOT NULL, updated_at INTEGER NOT NULL)`);
  }

  listRefs(): { name: string; sha: string }[] {
    return this.ctx.storage.sql.exec<{ name: string; sha: string }>(
      "SELECT name, sha FROM refs ORDER BY name").toArray();
  }

  // The whole consistency story: one DO instance per repo, one CAS per ref,
  // all inside a synchronous SQLite transaction. No await between the
  // WHERE-check and the write => no interleaving with another push.
  async updateRefs(cmds: RefCmd[], pushId: string): Promise<RefResult[]> {
    // Async I/O happens BEFORE the transaction (input gates only block
    // during storage ops, so we must not await R2 inside the CAS).
    const missing = new Set<string>();
    await Promise.all(cmds.filter(c => c.new !== ZERO).map(async c => {
      const inPending = await this.env.BUCKET.head(`pending/${pushId}/${c.new}`);
      const inRepo = inPending ?? await this.env.BUCKET.head(`objects/${c.new}`);
      if (!inRepo) missing.add(c.new);
    }));

    const sql = this.ctx.storage.sql;
    return this.ctx.storage.transactionSync(() => {
      const now = Date.now();
      const results: RefResult[] = [];
      for (const c of cmds) {
        if (missing.has(c.new)) { results.push({ name: c.name, ok: false, reason: "missing necessary objects" }); continue; }
        let written: number;
        if (c.old === ZERO) {            // create: must not exist
          written = sql.exec("INSERT OR IGNORE INTO refs (name, sha, updated_at) VALUES (?, ?, ?)",
            c.name, c.new, now).rowsWritten;
        } else if (c.new === ZERO) {     // delete: must match old
          written = sql.exec("DELETE FROM refs WHERE name = ? AND sha = ?", c.name, c.old).rowsWritten;
        } else {                         // update: classic CAS
          written = sql.exec("UPDATE refs SET sha = ?, updated_at = ? WHERE name = ? AND sha = ?",
            c.new, now, c.name, c.old).rowsWritten;
        }
        results.push(written === 1 ? { name: c.name, ok: true }
                                   : { name: c.name, ok: false, reason: "fetch first" });
      }
      // Optional atomic mode: if any failed, throw -> transactionSync rolls back all.
      return results;
    });
  }
}

// ---- Worker side: pkt-line in, report-status out ----
const enc = new TextEncoder();
const pkt = (s: string) => { const b = enc.encode(s); return `${(b.length + 4).toString(16).padStart(4, "0")}${s}`; };

function parseCommands(text: string): RefCmd[] {   // command section only, up to "0000"
  const cmds: RefCmd[] = []; let i = 0;
  for (;;) {
    const len = parseInt(text.slice(i, i + 4), 16); if (len === 0) break;
    const line = text.slice(i + 4, i + len).split("\0")[0].trim(); i += len;
    const [old, nw, name] = line.split(" "); cmds.push({ old, new: nw, name });
  }
  return cmds;
}

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const m = new URL(req.url).pathname.match(/^\/([^/]+)\/([^/]+)\/git-receive-pack$/);
    if (!m || req.method !== "POST") return new Response("nope", { status: 404 });
    const stub = env.REPO.get(env.REPO.idFromName(`${m[1]}/${m[2]}`));
    const pushId = crypto.randomUUID();
    // Idea #4/#6 split the body: commands (pkt-lines) then "PACK" bytes -> R2 pending/<pushId>.
    const { commandText } = await splitCommandsAndPack(req.body!, env.BUCKET, pushId); // pseudo
    const results = await stub.updateRefs(parseCommands(commandText), pushId);
    let out = pkt("unpack ok\n");
    for (const r of results) out += pkt(r.ok ? `ok ${r.name}\n` : `ng ${r.name} ${r.reason}\n`);
    out += "0000";                                    // flush-pkt; wrap in band 1 if side-band-64k negotiated
    return new Response(out, { headers: { "content-type": "application/x-git-receive-pack-result" } });
  },
};
```

## Why it works
- `git push` sends exactly the triple git's own `receive-pack` checks: `<old> <new> <ref>`; the client's `old` is its last-seen remote tip, so `UPDATE ... WHERE sha = old` is literally git's non-fast-forward guard, and a stale client gets `ng <ref> fetch first`, the same reason string `git push` prints today.
- The all-zero SHA convention (create when `old` is zero, delete when `new` is zero) maps onto `INSERT OR IGNORE` / `DELETE WHERE sha = old` with `rowsWritten` as the CAS outcome; no extra locking table is needed.
- Durable Objects run one instance per id, single-threaded, and `idFromName("owner/repo")` guarantees every Worker anywhere in the world resolves to that one instance, so "distributed lock" is replaced by "there is only one writer".
- The R2 existence checks are awaited before `transactionSync`; inside it there is no `await`, so even though input gates do not block during non-storage I/O, the read-check-write sequence cannot interleave with a concurrent push.
- `transactionSync` gives all-or-nothing across a multi-ref push (`git push --atomic` semantics) by throwing on the first failure; the per-ref result list gives non-atomic semantics, which is git's default.
- The response body is the `report-status` format `receive-pack` clients require: `unpack ok`, one `ok`/`ng` line per command, flush-pkt. Clients only treat the push as succeeded when they see `ok` for their ref, so a lost RPC response is safe: the ref moved, the client retries, and the retry gets `ng ... fetch first` or (after fetch) a no-op.

## Known limits
- Connectivity is not validated here: only "new tip object exists in R2". Proving every object reachable from the new tip is present (git's `check_connected`) needs the pack index from idea #4 and the commit graph from #56; without it a push of a pack that omits an ancestor is accepted and the repo is silently broken on fetch. This is the main hand-wave.
- R2 `head` before the transaction is a TOCTOU gap only against a GC that deletes pending objects; #6's janitor alarm must not sweep `pending/<pushId>` until the DO has recorded the outcome.
- Single DO throughput: roughly hundreds of ref updates per second per repo, and all pushes to one repo serialize through one isolate in one colo; a monorepo with thousands of concurrent pushers needs #16 (branch-level DOs). Reads (`ls-refs`) also hit the DO unless replicated via #13.
- DO memory is 128 MB and a DO request has the same CPU budget as a Worker (30 s default, configurable up to 5 min): the packfile must never enter the DO; the Worker streams it to R2 and only SHAs and ref names cross the RPC boundary.
- A DO can be evicted or moved; SQLite storage is durable, but in-memory state is not, so no ref state may live outside the `refs` table.
- Each push costs at least one R2 `head` per ref plus the pending writes; cheap (Class B ops), but not zero.
- `splitCommandsAndPack` is pseudo-code; pkt-line parsing shown assumes the command section is small enough to buffer as text, which is true in practice (commands are ~100 bytes each).
- If the Worker crashes after the DO commits but before the client reads `report-status`, the client sees a failed push while the ref has moved; the next push reports `fetch first`. This matches git-over-HTTP behavior on any server.

## Depends on
- refs-sqlite-objects-r2 (refs table lives in DO SQLite, objects in R2)
- content-addressed-r2-keys (the `objects/<sha>` and `pending/<pushId>/<sha>` keys the existence check relies on)
- info-refs-endpoint (pkt-line codec and the smart HTTP handshake around this call)
- auth-and-multitenancy (`idFromName(owner/repo)` routing and who may push)
- two-phase-push (pending prefix and orphan sweep; needed for a crash-safe version)
