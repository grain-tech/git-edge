> Idea #36 · wild · verdict: **risky** · feasibility 3/5 · reliability 2/5 · correctness 3/5 · effort: weeks
> Proof: [proofs/federated-gossip.md](./federated-gossip.md) · Review: [reviews/federated-gossip.md](../../reviews/archived/federated-gossip.md)

# Federated remotes via DO-to-DO gossip

## Mechanism
Every repo DO keeps a `peers` table (peer repo name, or an `https://` smart-HTTP URL for a foreign deployment) and an append-only `outbox`. When `git push` reaches `RepoDO.commit()` (phase two of the two-phase push), the ref compare-and-swap and the outbox insert `{origin, seq, ref, old, new}` happen in one `transactionSync`, and `setAlarm(now)` is armed. The alarm drains the outbox by calling `peerStub.gossip(msg)` over DO RPC (`env.REPO.idFromName(peer)`); the receiving DO dedupes on `(origin, seq)`, makes sure the new tip is reachable in its object store (same-bucket peers: an R2 `head` on the content-addressed key; foreign peers: one protocol-v2 `fetch want=<new> have=<old>` against the origin URL, streamed through the pack parser into R2), then writes the tip to `refs/remotes/<origin>/<ref>` in its own SQLite and re-enqueues the message for its own peers minus the `path` it already travelled. A second, slow alarm performs anti-entropy: it picks a random peer, exchanges `ls-refs` digests, and enqueues whatever differs, so a dropped RPC cannot leave a mirror permanently stale.

## Primitives
- Durable Objects with SQLite storage (`ctx.storage.sql.exec`, `transactionSync`) — GA
- DO alarms (`ctx.storage.setAlarm`, `alarm()`) with at-least-once delivery for outbox drain and anti-entropy — GA
- DO RPC (`DurableObject` subclass methods called on a stub: `env.REPO.get(id).gossip(...)`) — GA
- Same-account DO namespace addressing via `idFromName("owner/repo")` — GA; cross-account/cross-deployment peers cannot be reached by RPC and fall back to smart-HTTP `fetch()` over the public Worker
- R2 (`head`/`put` on `objects/<sha>` content-addressed keys shared by all repos of one deployment) — GA
- `DecompressionStream("deflate")` for inflating the PACK a foreign peer returns — GA
- Workers `fetch()` to a foreign git-edge deployment — GA

## Proof code
```typescript
import { DurableObject } from "cloudflare:workers";
type Env = { REPO: DurableObjectNamespace<RepoDO>; BUCKET: R2Bucket };
type Msg = { origin: string; seq: number; ref: string; old: string; new: string; path: string[]; hops: number };
const MAX_HOPS = 6, ANTI_ENTROPY_MS = 5 * 60_000;

export class RepoDO extends DurableObject<Env> {
  private name = this.ctx.id.name!;                                   // "owner/repo" from idFromName
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS refs   (name TEXT PRIMARY KEY, sha TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS peers  (peer TEXT PRIMARY KEY);            -- "acme/lib" or "https://other.example/acme/lib"
      CREATE TABLE IF NOT EXISTS seen   (origin TEXT, seq INTEGER, PRIMARY KEY(origin, seq));
      CREATE TABLE IF NOT EXISTS outbox (id INTEGER PRIMARY KEY, peer TEXT, msg TEXT, attempts INTEGER DEFAULT 0, next_at INTEGER);
      CREATE TABLE IF NOT EXISTS meta   (k TEXT PRIMARY KEY, v INTEGER)`);
  }

  /** Phase two of a push: CAS refs and enqueue gossip in ONE SQLite transaction. */
  commit(cmds: { ref: string; old: string; new: string }[]): "ok" | "ng" {
    const sql = this.ctx.storage.sql;
    const r = this.ctx.storage.transactionSync(() => {
      for (const c of cmds) {
        const cur = sql.exec<{ sha: string }>("SELECT sha FROM refs WHERE name=?", c.ref).toArray()[0]?.sha ?? "0".repeat(40);
        if (cur !== c.old) return "ng";                                       // receive-pack semantics: stale old-sha => reject
      }
      const seq = (sql.exec<{ v: number }>("SELECT v FROM meta WHERE k='seq'").toArray()[0]?.v ?? 0) + 1;
      sql.exec("INSERT OR REPLACE INTO meta VALUES ('seq', ?)", seq);
      for (const c of cmds) {
        sql.exec("INSERT OR REPLACE INTO refs VALUES (?,?)", c.ref, c.new);
        this.enqueue({ origin: this.name, seq, ref: c.ref, old: c.old, new: c.new, path: [this.name], hops: 0 });
      }
      return "ok";
    });
    if (r === "ok") void this.ctx.storage.setAlarm(Date.now());               // drain soon; alarm is at-least-once
    return r;
  }

  private enqueue(m: Msg) {
    for (const { peer } of this.ctx.storage.sql.exec<{ peer: string }>("SELECT peer FROM peers").toArray())
      if (!m.path.includes(peer))
        this.ctx.storage.sql.exec("INSERT INTO outbox (peer,msg,next_at) VALUES (?,?,?)", peer, JSON.stringify(m), Date.now());
  }

  /** RPC entry point called by a peer DO. Idempotent on (origin, seq). */
  async gossip(m: Msg): Promise<void> {
    const sql = this.ctx.storage.sql;
    if (m.hops > MAX_HOPS || sql.exec("SELECT 1 FROM seen WHERE origin=? AND seq=?", m.origin, m.seq).toArray().length) return;
    if (!(await this.env.BUCKET.head(`objects/${m.new}`)))                    // same-bucket peers already share objects
      await this.fetchFromOrigin(m);                                          // foreign deployment: pull the delta pack
    sql.transactionSync(() => {
      sql.exec("INSERT INTO seen VALUES (?,?)", m.origin, m.seq);
      // Never touch local heads: mirror into a tracking namespace, exactly like `git fetch <remote>` would.
      sql.exec("INSERT OR REPLACE INTO refs VALUES (?,?)", `refs/remotes/${m.origin}/${m.ref.replace(/^refs\/heads\//, "")}`, m.new);
      this.enqueue({ ...m, path: [...m.path, this.name], hops: m.hops + 1 }); // forward to the rest of the mesh
    });
    void this.ctx.storage.setAlarm(Date.now());
  }

  /** Foreign peer: protocol-v2 `fetch` for exactly the new tip, index the PACK into R2. */
  private async fetchFromOrigin(m: Msg) {
    const url = this.ctx.storage.sql.exec<{ peer: string }>("SELECT peer FROM peers WHERE peer LIKE 'https://%' AND peer LIKE ?", `%${m.origin}`).toArray()[0]?.peer;
    if (!url) throw new Error(`no route to objects of ${m.origin}`);
    const pkt = (s: string) => (s.length + 4).toString(16).padStart(4, "0") + s; // pkt-line
    const body = pkt("command=fetch\n") + "0001" + pkt(`want ${m.new}\n`) + (m.old.match(/^0+$/) ? "" : pkt(`have ${m.old}\n`)) + pkt("done\n") + "0000";
    const res = await fetch(`${url}/git-upload-pack`, { method: "POST", headers: { "Git-Protocol": "version=2", "content-type": "application/x-git-upload-pack-request" }, body });
    // parse sideband-1 pkt-lines until "PACK", then hand the stream to the streaming pack parser:
    // PACK header (12 bytes), per-object zlib via DecompressionStream("deflate"), ofs-delta/ref-delta resolution, env.BUCKET.put(`objects/<sha>`)
    await indexPackIntoR2(res.body!, this.env.BUCKET);
  }

  async alarm() {
    const sql = this.ctx.storage.sql, now = Date.now();
    for (const row of sql.exec<{ id: number; peer: string; msg: string; attempts: number }>("SELECT * FROM outbox WHERE next_at<=? LIMIT 50", now).toArray()) {
      try {
        const m: Msg = JSON.parse(row.msg);
        if (row.peer.startsWith("https://")) await fetch(`${row.peer}/gossip`, { method: "POST", body: row.msg });   // foreign edge Worker routes to its RepoDO.gossip
        else await this.env.REPO.get(this.env.REPO.idFromName(row.peer)).gossip(m);                                    // same deployment: DO RPC
        sql.exec("DELETE FROM outbox WHERE id=?", row.id);
      } catch {
        sql.exec("UPDATE outbox SET attempts=attempts+1, next_at=? WHERE id=?", now + Math.min(2 ** row.attempts * 1000, 300_000), row.id);
      }
    }
    // anti-entropy: every 5 min compare ref digests with one random peer and enqueue what differs
    const last = sql.exec<{ v: number }>("SELECT v FROM meta WHERE k='ae'").toArray()[0]?.v ?? 0;
    if (now - last > ANTI_ENTROPY_MS) { await this.antiEntropy(); sql.exec("INSERT OR REPLACE INTO meta VALUES ('ae', ?)", now); }
    const pending = sql.exec("SELECT 1 FROM outbox LIMIT 1").toArray().length;
    void this.ctx.storage.setAlarm(pending ? now + 1000 : now + ANTI_ENTROPY_MS);
  }

  /** ls-refs exchange: peer answers with its heads; anything we lack becomes a synthetic gossip message. */
  async lsRefs(): Promise<Record<string, string>> {
    return Object.fromEntries(this.ctx.storage.sql.exec<{ name: string; sha: string }>("SELECT name,sha FROM refs WHERE name LIKE 'refs/heads/%'").toArray().map(r => [r.name, r.sha]));
  }
  private async antiEntropy() {
    const peers = this.ctx.storage.sql.exec<{ peer: string }>("SELECT peer FROM peers WHERE peer NOT LIKE 'https://%'").toArray();
    if (!peers.length) return;
    const peer = peers[Math.floor(Math.random() * peers.length)].peer;
    const theirs = await this.env.REPO.get(this.env.REPO.idFromName(peer)).lsRefs();
    for (const [ref, sha] of Object.entries(theirs)) {
      const mine = this.ctx.storage.sql.exec<{ sha: string }>("SELECT sha FROM refs WHERE name=?", `refs/remotes/${peer}/${ref.slice(11)}`).toArray()[0]?.sha;
      if (mine !== sha) await this.gossip({ origin: peer, seq: -Date.now(), ref, old: mine ?? "0".repeat(40), new: sha, path: [peer], hops: 0 });
    }
  }
}
declare function indexPackIntoR2(pack: ReadableStream<Uint8Array>, bucket: R2Bucket): Promise<void>; // streaming-pack-parser
```

## Why it works
- A push is only acknowledged (`ok <ref>` in the report-status pkt-lines) after `commit()` returns, and the outbox rows are written in the same SQLite transaction as the ref CAS, so "ref moved" and "peers will hear about it" are atomic; the DO alarm is at-least-once, and `(origin, seq)` dedupe on the receiver makes redelivery harmless.
- Mirrors land tips in `refs/remotes/<origin>/<branch>`, which is precisely what `git fetch <remote>` does on a client; a mirror's own `refs/heads/*` are never rewritten by gossip, so local pushes and federated updates cannot race, and a client cloning the mirror sees the federation as ordinary remote-tracking refs (`ls-refs ref-prefix refs/remotes/`).
- Within one deployment all repos share one content-addressed bucket (`objects/<sha>`), so a ref update is fully described by the 40-byte SHA: the receiver only has to `head` the tip object, and connectivity is the same check the origin already made before accepting the push.
- Across deployments the receiver becomes a normal protocol-v2 client: `command=fetch`, `want <new>`, `have <old>`, `done` gets back exactly the delta pack (`PACK` header, ofs-delta against objects it already holds), which the existing streaming pack parser indexes into R2 unchanged.
- Flood + `path` exclusion + hop cap bounds the message count per update to roughly the number of edges in the mesh, and the periodic `ls-refs` digest exchange is the standard anti-entropy repair, so the mesh converges even if an RPC is lost or a DO is evicted mid-drain.

## Known limits
- No hub means no arbiter: if two peers each push a different `main`, neither wins. The proof sidesteps this by mirroring into tracking refs only; a "fast-forward-only mirror of `refs/heads/*`" policy is possible (check `old == current` before overwriting, else record a `diverged` row) but true multi-master on the same branch needs `crdt-branches` or `cross-repo-atomic-push`, and is out of scope here.
- DO RPC only reaches DOs in the same namespace binding, i.e. the same deployment/account. Cross-account federation degrades to HTTPS between two edge Workers plus a real pack fetch; the `seq` dedupe and `path` list still work but you pay R2 PUTs for every copied object.
- Foreign-origin object fetches run inside `gossip()` on the receiving DO: a large first sync can exceed the DO's 128 MB memory or the 30 s CPU budget unless the pack is streamed in chunks with the alarm re-arming between them (the proof hand-waves this in `indexPackIntoR2`).
- Every ref update costs one alarm wake, N RPCs (N = peers) and up to E forwardings across the mesh; a repo with hundreds of peers or a hot branch (CI bots pushing every few seconds) makes the single origin DO the throughput bottleneck. Batch the outbox per peer (one RPC carrying many `Msg`) before that matters.
- `seen` grows one row per (origin, seq) forever; a janitor alarm should truncate it below the smallest seq every peer is known to have (or bound it to a time window). The anti-entropy `seq: -Date.now()` synthetic messages never collide with real seqs but are never garbage-collected either.
- Anti-entropy is one random peer per 5 minutes, so a partition heals in O(peers * 5 min) rather than immediately; tune per mesh size.
- Peer registration (`INSERT INTO peers`) has no auth in the proof; `auth-and-multitenancy` must gate who may make repo A subscribe to repo B, otherwise anyone can make you mirror anything.

## Depends on
repo-do-ref-authority, refs-sqlite-objects-r2, content-addressed-r2-keys, two-phase-push, streaming-pack-parser, protocol-v2-only, auth-and-multitenancy
