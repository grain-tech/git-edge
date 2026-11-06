> Idea #14 · edge · verdict: **lands with caveats** · feasibility 4/5 · reliability 3/5 · correctness 3/5 · effort: weeks
> Proof: [proofs/alarm-chain-ci.md](./alarm-chain-ci.md) · Review: [reviews/alarm-chain-ci.md](../../reviews/archived/alarm-chain-ci.md)

# Push-triggered CI as a DO alarm chain

## Mechanism
`POST /:owner/:repo/git-receive-pack` is handled by the repo DO (`repo-do-ref-authority`); after it has flipped the refs and *before* it writes the `report-status` pkt-lines back to the client, it calls `env.CI_RUN.get(idFromName("owner/repo@newSha")).start(...)` over DO RPC. The CiRun DO inserts one row per stage into its own SQLite table `stages` and calls `ctx.storage.setAlarm(Date.now())`; nothing else is enqueued anywhere. Each `alarm()` invocation claims the next `pending` stage, runs it (reading the commit's tree blobs from R2 by SHA), records the result in SQLite, and re-arms the alarm for the next stage; the terminal alarm writes a status blob to R2 and asks the repo DO to point `refs/ci/<sha>` at it so `git ls-remote` / `git fetch` can read the verdict with no extra API.

## Primitives
- Durable Objects with SQLite storage (`ctx.storage.sql.exec`) — GA
- DO alarms (`ctx.storage.setAlarm` / `alarm()` / `deleteAlarm`) — GA; at-least-once, one alarm per DO
- DO RPC (`extends DurableObject`, calling methods on a stub) — GA
- R2 (`env.BUCKET.get/put`, content-addressed keys) — GA
- Service bindings / Workers for Platforms dynamic dispatch for user-supplied stage code — service bindings GA; Workers for Platforms is a paid add-on (dispatch namespaces)
- `limits.cpu_ms` in wrangler to lift the 30 s CPU cap per alarm to 300 s — GA on Paid

## Proof code
```typescript
import { DurableObject } from "cloudflare:workers";

type Env = { BUCKET: R2Bucket; REPO: DurableObjectNamespace; CI_RUN: DurableObjectNamespace<CiRun>; STAGE_WORKER: Fetcher };
type Stage = { seq: number; name: string; status: "pending" | "running" | "ok" | "fail"; attempts: number; log: string | null };
const MAX_ATTEMPTS = 3, WATCHDOG_MS = 90_000;

export class CiRun extends DurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS run    (k TEXT PRIMARY KEY, v TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS stages (seq INTEGER PRIMARY KEY, name TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0, log TEXT)`);
  }

  // Called by the repo DO over RPC right after the ref flip, before report-status is sent.
  async start(p: { repo: string; ref: string; sha: string; stages: string[] }): Promise<void> {
    if (this.ctx.storage.sql.exec("SELECT 1 FROM stages LIMIT 1").toArray().length) return; // idempotent: same sha => same DO id
    this.ctx.storage.sql.exec("INSERT INTO run VALUES ('repo',?),('ref',?),('sha',?)", p.repo, p.ref, p.sha);
    p.stages.forEach((name, i) => this.ctx.storage.sql.exec("INSERT INTO stages (seq,name) VALUES (?,?)", i, name));
    await this.ctx.storage.setAlarm(Date.now()); // first stage runs on the next alarm tick, push returns immediately
  }

  async alarm(): Promise<void> {
    const sql = this.ctx.storage.sql;
    const cur = sql.exec<Stage>("SELECT * FROM stages WHERE status IN ('pending','running') ORDER BY seq LIMIT 1").toArray()[0];
    if (!cur) return;                                            // chain finished (or never started)
    if (cur.attempts >= MAX_ATTEMPTS) return this.finish("fail", `${cur.name}: gave up after ${cur.attempts} attempts`);
    sql.exec("UPDATE stages SET status='running', attempts=attempts+1 WHERE seq=?", cur.seq);
    await this.ctx.storage.setAlarm(Date.now() + WATCHDOG_MS); // lease: if this isolate dies mid-stage, alarm re-fires and we retry

    const run = Object.fromEntries(sql.exec<{ k: string; v: string }>("SELECT k,v FROM run").toArray().map(r => [r.k, r.v]));
    let ok = false, log = "";
    try { ({ ok, log } = await this.runStage(cur.name, run.repo, run.sha)); }
    catch (e) { log = String(e); }                                // do NOT rethrow: DO's own retry would ignore our attempt budget

    if (!ok && cur.attempts + 1 < MAX_ATTEMPTS) {                 // transient failure: leave 'running', let watchdog retry
      sql.exec("UPDATE stages SET log=? WHERE seq=?", log, cur.seq); return;
    }
    sql.exec("UPDATE stages SET status=?, log=? WHERE seq=?", ok ? "ok" : "fail", log, cur.seq);
    if (!ok) return this.finish("fail", `${cur.name} failed: ${log.slice(0, 200)}`);
    const more = sql.exec("SELECT 1 FROM stages WHERE status='pending' LIMIT 1").toArray().length > 0;
    if (more) await this.ctx.storage.setAlarm(Date.now());        // next link in the chain
    else await this.finish("ok", "all stages passed");
  }

  // A stage sees the pushed commit's tree by SHA straight from R2; no checkout.
  private async runStage(name: string, repo: string, sha: string): Promise<{ ok: boolean; log: string }> {
    const commit = await this.env.BUCKET.get(`objects/${repo}/${sha}`);       // loose zlib'd "commit <len>\0..." (content-addressed-r2-keys)
    if (!commit) return { ok: false, log: "commit object missing in R2" };
    const treeSha = /* parse "tree <sha>" first line after inflating via new DecompressionStream("deflate") */ "";
    // User-defined stage code runs in a Worker reached through a service binding (or a Workers-for-Platforms
    // dispatch namespace); it gets the tree SHA and reads blobs itself via the repo's object API.
    const res = await this.env.STAGE_WORKER.fetch("https://stage/run", {
      method: "POST", body: JSON.stringify({ stage: name, repo, sha, tree: treeSha }),
    });
    return { ok: res.ok, log: await res.text() };
  }

  private async finish(status: "ok" | "fail", summary: string): Promise<void> {
    await this.ctx.storage.deleteAlarm();
    const run = Object.fromEntries(this.ctx.storage.sql.exec<{ k: string; v: string }>("SELECT k,v FROM run").toArray().map(r => [r.k, r.v]));
    const stages = this.ctx.storage.sql.exec<Stage>("SELECT name,status,attempts,log FROM stages ORDER BY seq").toArray();
    // Verdict is stored as a real git blob so it is fetchable: blobSha = sha1("blob <len>\0" + body)
    const body = new TextEncoder().encode(JSON.stringify({ status, summary, sha: run.sha, stages }));
    const blobSha = await gitBlobSha(body);
    await this.env.BUCKET.put(`objects/${run.repo}/${blobSha}`, zlibDeflate(gitHeader("blob", body.length), body));
    // Repo DO does a CAS on refs/ci/<sha> -> blobSha; clients see it via ls-refs ref-prefix refs/ci/
    await this.env.REPO.get(this.env.REPO.idFromName(run.repo)).fetch("https://repo/internal/set-ref", {
      method: "POST", body: JSON.stringify({ ref: `refs/ci/${run.sha}`, old: null, new: blobSha }),
    });
  }
}
declare function gitBlobSha(b: Uint8Array): Promise<string>;
declare function gitHeader(t: string, n: number): Uint8Array;
declare function zlibDeflate(h: Uint8Array, b: Uint8Array): ReadableStream;
```

## Why it works
- `git push` only cares that `report-status` arrives after the refs are durable: `unpack ok` then `ok refs/heads/main` (or `ng refs/heads/main <reason>`) in pkt-lines, then flush. Scheduling the alarm is a single SQLite write plus `setAlarm`, both in the same DO transaction as the ref flip, so the push returns in the same round-trip and CI can never be lost between "ref moved" and "job enqueued".
- One CiRun DO per `owner/repo@sha` gives free idempotency: a re-push of the same commit or a retried HTTP request lands on the same DO id and `start()` sees the existing rows. This mirrors git's own content-addressing.
- DO alarms are at-least-once with automatic retry on throw. The code relies on that only as a backstop: the `running` state plus the watchdog alarm make a crashed isolate resume at the right stage, and `attempts` bounds retries so a deterministic failure does not loop forever.
- The chain is strictly ordered because a DO has exactly one alarm and `alarm()` invocations on the same DO never overlap; that is the "no queue service" claim made honest, and it is the same guarantee git users expect from hooks (`pre-receive` then `update` then `post-receive`, serially).
- Stages read the pushed commit's tree straight from R2 by SHA, the same objects `git-receive-pack` just indexed (`streaming-pack-parser`), so CI never re-fetches or checks out.
- The verdict is a real blob object under `refs/ci/<sha>`, so `git ls-remote origin 'refs/ci/*'` and `git fetch origin refs/ci/<sha>` show and retrieve CI status using protocol v2 `ls-refs ref-prefix` with no side API; it also survives a DO wipe because it is just objects plus a ref.

## Known limits
- Not real CI as most people mean it: no Linux, no shell, no `npm test`. A "stage" is a Worker (JS/Wasm) reached via service binding or a Workers-for-Platforms dispatch namespace, or a built-in check (lint, schema validation, size limits, calling an external runner). Running arbitrary user scripts requires `hooks-as-workers` or Workers for Platforms; `new Function`/`eval` are blocked in Workers.
- Per-alarm CPU budget is 30 s by default (up to 300 s with `limits.cpu_ms` on Paid). Wall-clock waits on R2 or subrequests do not count, but a stage that needs more CPU must be split across alarm ticks by the stage author.
- One alarm per DO means one running stage per run; parallel matrix jobs need one CiRun DO per job (`idFromName(sha + "/" + job)`) plus a small fan-in row in a parent DO. Each DO is single-threaded, so throughput per run is bounded, not per repo.
- Alarm scheduling is best-effort on timing (typically fires within seconds, can lag under load); the watchdog lease trades duplicate execution for liveness, so stages must be idempotent (they are, if they only write content-addressed objects).
- DO memory is 128 MB; a stage that inflates a big tree into memory must stream (`DecompressionStream`) and not hold whole packs. Large trees also cost one R2 GET per blob (Class B ops), and the 1000-subrequest limit per invocation caps how many blobs one alarm tick can read.
- The proof hand-waves the git object encoding helpers (`gitBlobSha`, zlib header) and the repo DO's `set-ref` CAS endpoint; both are foundation work owned by other slugs.

## Depends on
repo-do-ref-authority, refs-sqlite-objects-r2, content-addressed-r2-keys, two-phase-push, hooks-as-workers
