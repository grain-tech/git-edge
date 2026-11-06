> Idea #48 · wild · verdict: **risky** · feasibility 3/5 · reliability 2/5 · correctness 2/5 · effort: weeks
> Proof: [proofs/cross-repo-atomic-push.md](./cross-repo-atomic-push.md) · Review: [reviews/cross-repo-atomic-push.md](../../reviews/archived/cross-repo-atomic-push.md)

# Cross-repo atomic pushes

## Mechanism
A gateway Worker exposes `POST /txn/push` taking a multipart body with one part per target repo; each part is byte-for-byte what `git push` would send to that repo's `/git-receive-pack` (pkt-line ref commands, flush, PACK). The Worker mints a transaction id, creates a coordinator DO (`TXN`, `idFromName(txn)`), and calls `prepare()` over DO RPC on each repo DO (`REPO`, `idFromName(owner/repo)`) in parallel: each repo DO indexes its pack into R2 under content-addressed keys, compare-and-swaps the old ref values against its SQLite `refs` table, and if everything matches writes a `prepared` row (the ref lock) and votes yes. The coordinator durably records commit/abort in its own SQLite before phase two, then the Worker calls `commit()`/`abort()` on every participant and returns one git `report-status` (`unpack ok` / `ok refs/..` / `ng refs/..`) per repo. Alarms on both sides drive recovery: a participant stuck in `prepared` asks the coordinator for the outcome; a coordinator with unacked participants re-drives them.

## Primitives
- Durable Objects with SQLite storage (`ctx.storage.sql`) — GA
- DO RPC (`extends DurableObject`, calling methods on stubs, `ReadableStream` as an RPC argument) — GA
- DO alarms (`ctx.storage.setAlarm`, `alarm()`) — GA
- R2 (`env.BUCKET.put/head`) for content-addressed objects — GA
- Workers `FormData`/multipart parsing and `DecompressionStream` (inside the pack indexer) — GA
- No beta primitives required

## Proof code
```typescript
import { DurableObject } from "cloudflare:workers";
type Env = { REPO: DurableObjectNamespace<RepoDO>; TXN: DurableObjectNamespace<TxnDO>; BUCKET: R2Bucket };
type Cmd = { old: string; new: string; ref: string };
type Vote = { ok: boolean; why?: string };
const ZERO = "0".repeat(40);

// Gateway: one HTTP request carries N receive-pack bodies (multipart, part name = "owner/repo").
export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const parts = [...(await req.formData()).entries()].filter(([, v]) => v instanceof File) as [string, File][];
    const txn = crypto.randomUUID();
    const coord = env.TXN.get(env.TXN.idFromName(txn));
    await coord.begin(parts.map(([n]) => n));
    // Phase 1 (parallel): each repo DO parses its own PACK, writes objects, CAS-checks refs, votes.
    const votes = await Promise.all(parts.map(([name, f]) => env.REPO.get(env.REPO.idFromName(name)).prepare(txn, f.stream())));
    const decision = await coord.decide(votes);                       // durably logged before phase 2
    // Phase 2 (parallel): flip or drop the prepared refs; each returns a git report-status.
    const reports = await Promise.all(parts.map(async ([name], i) => {
      const repo = env.REPO.get(env.REPO.idFromName(name));
      const cmds = decision === "commit" ? await repo.commit(txn) : await repo.abort(txn);
      await coord.ack(name);
      return [name, reportStatus(cmds, decision === "commit" ? undefined : votes.find(v => !v.ok)?.why ?? "peer aborted")];
    }));
    return new Response(JSON.stringify({ txn, decision, reports }), { headers: { "content-type": "application/json" } });
  },
};

export class RepoDO extends DurableObject<Env> {
  sql = this.ctx.storage.sql;
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.sql.exec(`CREATE TABLE IF NOT EXISTS refs(name TEXT PRIMARY KEY, sha TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS prepared(ref TEXT PRIMARY KEY, txn TEXT NOT NULL, old TEXT, new TEXT, expires INTEGER)`);
  }
  async prepare(txn: string, body: ReadableStream<Uint8Array>): Promise<Vote> {
    const { cmds, pack } = await splitReceivePack(body);   // pkt-lines "<old> <new> <ref>\0report-status ..." + flush, then PACK
    await indexPackToR2(this.env.BUCKET, pack);            // streaming-pack-parser: objects/<sha>, idempotent (content-addressed)
    for (const c of cmds) {                                // compare-and-swap + lock check, serialized by the single DO
      const cur = this.sql.exec<{ sha: string }>("SELECT sha FROM refs WHERE name=?", c.ref).toArray()[0]?.sha ?? ZERO;
      if (cur !== c.old) return { ok: false, why: `${c.ref} fetch first` };
      if (this.sql.exec("SELECT 1 FROM prepared WHERE ref=?", c.ref).toArray().length) return { ok: false, why: `${c.ref} locked` };
      if (c.new !== ZERO && !(await this.env.BUCKET.head(`objects/${c.new}`))) return { ok: false, why: `${c.ref} missing object` };
    }
    for (const c of cmds) this.sql.exec("INSERT INTO prepared VALUES(?,?,?,?,?)", c.ref, txn, c.old, c.new, Date.now() + 30_000);
    await this.ctx.storage.setAlarm(Date.now() + 30_000);  // if nobody comes back, ask the coordinator
    return { ok: true };
  }
  async commit(txn: string): Promise<Cmd[]> {
    const rows = this.sql.exec<Cmd & { ref: string }>("SELECT ref, old, new FROM prepared WHERE txn=?", txn).toArray();
    this.ctx.storage.transactionSync(() => {               // ref flip + lock release in one SQLite transaction
      for (const r of rows) r.new === ZERO
        ? this.sql.exec("DELETE FROM refs WHERE name=?", r.ref)
        : this.sql.exec("INSERT INTO refs VALUES(?,?) ON CONFLICT(name) DO UPDATE SET sha=excluded.sha", r.ref, r.new);
      this.sql.exec("DELETE FROM prepared WHERE txn=?", txn);
    });
    return rows;
  }
  async abort(txn: string): Promise<Cmd[]> {
    const rows = this.sql.exec<Cmd>("SELECT ref, old, new FROM prepared WHERE txn=?", txn).toArray();
    this.sql.exec("DELETE FROM prepared WHERE txn=?", txn);  // R2 objects stay; gc-and-repack-alarm sweeps unreachable ones
    return rows;
  }
  async alarm() {                                          // recovery: a prepared participant never decides alone
    for (const { txn } of this.sql.exec<{ txn: string }>("SELECT DISTINCT txn FROM prepared WHERE expires<?", Date.now()).toArray()) {
      const d = await this.env.TXN.get(this.env.TXN.idFromName(txn)).outcome();
      d === "commit" ? await this.commit(txn) : await this.abort(txn);
    }
  }
}

export class TxnDO extends DurableObject<Env> {
  sql = this.ctx.storage.sql;
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.sql.exec(`CREATE TABLE IF NOT EXISTS txn(id INTEGER PRIMARY KEY CHECK(id=1), state TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS parts(name TEXT PRIMARY KEY, acked INTEGER DEFAULT 0)`);
  }
  async begin(names: string[]) {
    this.sql.exec("INSERT OR IGNORE INTO txn VALUES(1,'pending')");
    for (const n of names) this.sql.exec("INSERT OR IGNORE INTO parts(name) VALUES(?)", n);
    await this.ctx.storage.setAlarm(Date.now() + 60_000);
  }
  async decide(votes: Vote[]): Promise<"commit" | "abort"> {
    const d = votes.every(v => v.ok) ? "commit" : "abort";
    this.sql.exec("UPDATE txn SET state=? WHERE state='pending'", d);   // the durable decision point
    return this.outcome();
  }
  async outcome(): Promise<"commit" | "abort"> {          // pending + asked == coordinator died before deciding => abort
    this.sql.exec("UPDATE txn SET state='abort' WHERE state='pending'");
    return this.sql.exec<{ state: "commit" | "abort" }>("SELECT state FROM txn").one().state;
  }
  async ack(name: string) { this.sql.exec("UPDATE parts SET acked=1 WHERE name=?", name); }
  async alarm() {                                          // re-drive phase 2 to anyone that never acked
    const d = await this.outcome();
    for (const { name } of this.sql.exec<{ name: string }>("SELECT name FROM parts WHERE acked=0").toArray()) {
      const repo = this.env.REPO.get(this.env.REPO.idFromName(name));
      d === "commit" ? await repo.commit(this.ctx.id.name!) : await repo.abort(this.ctx.id.name!);
      await this.ack(name);
    }
  }
}

// pkt-line report-status exactly as git-receive-pack would emit it (sideband omitted for brevity)
function reportStatus(cmds: Cmd[], err?: string): string {
  const pkt = (s: string) => (s.length + 5).toString(16).padStart(4, "0") + s + "\n";
  return pkt("unpack ok") + cmds.map(c => pkt(err ? `ng ${c.ref} ${err}` : `ok ${c.ref}`)).join("") + "0000";
}
declare function splitReceivePack(b: ReadableStream<Uint8Array>): Promise<{ cmds: Cmd[]; pack: ReadableStream<Uint8Array> }>;
declare function indexPackToR2(bucket: R2Bucket, pack: ReadableStream<Uint8Array>): Promise<void>;
```

## Why it works
- git's own `--atomic` push is exactly "all ref updates in this receive-pack succeed or none do"; the server side of that is a CAS on every command's `old` sha against the ref store, done under one lock. Here each repo's lock is the repo DO's single-threaded execution plus the `prepared` row, and the cross-repo lock is the coordinator's durable `state` row, so the invariant generalizes to N repos.
- The wire contents per repo are unchanged: the client (or a thin wrapper around `git push`) produces normal `<old> <new> <ref>\0caps` pkt-lines followed by a `PACK` stream, and gets back a normal `report-status` (`unpack ok`, `ok`/`ng` per ref). Aborted participants return `ng <ref> <reason>`, which git prints as `! [remote rejected]`, the same UX as a failed atomic push.
- Objects are safe to write before the decision because R2 keys are content-addressed (`objects/<sha>`): a write from an aborted transaction is just an unreachable object, indistinguishable from a normal interrupted push, and the existing GC alarm sweeps it.
- The `prepared` table doubles as the lock that ordinary single-repo pushes must respect (`ng refs/heads/main locked`), so a concurrent normal push cannot slide in between prepare and commit; the DO serializes both paths.
- Classic 2PC safety holds: a participant that voted yes never decides on its own (its alarm asks `outcome()`), the coordinator converts an undecided transaction to `abort` on first query after it lost the Worker, and `commit()` is idempotent (re-running on an already-committed txn finds no `prepared` rows and flips nothing).
- Every step that matters is in DO SQLite, which is written through output gates before the RPC reply reaches the caller, so "the coordinator said commit" implies the decision survived.

## Known limits
- Not literally "`git push` to three repos in one request": stock git has no multi-remote receive-pack. The single-request form needs a small client wrapper (or `git bundle`-style packaging) that concatenates three receive-pack bodies into the multipart POST. A stock-git-compatible variant (`git push -o txn=<id> -o txn-size=3` to each repo, each response held open until the coordinator decides) works only if the three pushes run concurrently; run sequentially the first push blocks forever, so the gateway must time it out to `ng`.
- Phase 1 runs the full pack parse for all N repos inside one Worker invocation's wall clock (the DO calls run in parallel, but the Worker waits). CPU is spent in the repo DOs, not the Worker, so the 30s Worker CPU limit is fine, but large packs push the end-to-end latency past what clients tolerate; big pushes should use presigned direct upload first.
- Memory: each repo DO indexes its own pack in a streaming fashion; the 128MB DO limit applies per repo, not to the sum, as long as `indexPackToR2` never buffers the whole pack (hand-waved here, it belongs to the streaming-pack-parser proof).
- Lock hold time: refs stay locked for up to the 30s participant timeout if the Worker dies between phases; normal pushes to those refs get `ng locked` in the meantime. Acceptable for a wild feature, not for a hot main branch.
- Blocking during recovery: the coordinator DO is one object per transaction (cheap, but a `deleteAll` after the last ack is needed or these accumulate). Participant alarms only fire if a `prepared` row exists, so idle repos cost nothing.
- R2 costs are the same as N independent pushes (one PUT per new object, one HEAD per ref tip); aborted transactions leave paid-for orphans until GC.
- Hand-waved: `splitReceivePack` (pkt-line parse) and `indexPackToR2` (zlib via DecompressionStream, ofs-delta/ref-delta resolution) are declared, not written; connectivity is checked only for the tip object, not the full reachable closure, which the two-phase-push foundation owns.

## Depends on
- repo-do-ref-authority
- refs-sqlite-objects-r2
- content-addressed-r2-keys
- two-phase-push
- streaming-pack-parser
- gc-and-repack-alarm
- auth-and-multitenancy
