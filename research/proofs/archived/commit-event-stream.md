> Idea #34 · wild · verdict: **lands with caveats** · feasibility 4/5 · reliability 3/5 · correctness 3/5 · effort: days
> Proof: [proofs/commit-event-stream.md](./commit-event-stream.md) · Review: [reviews/commit-event-stream.md](../../reviews/archived/commit-event-stream.md)

# Commit-as-event-stream

## Mechanism
`POST /:owner/:repo/git-receive-pack` lands on the repo's Durable Object (`idFromName("owner/repo")`), which parses the pushed PACK, writes objects to R2, and flips refs. In the **same SQLite transaction** that flips a ref, the DO inserts one `outbox` row per new commit (the pack's `OBJ_COMMIT` entries, header fields parsed from the inflated object) and calls `ctx.storage.setAlarm(Date.now())`. The alarm drains `outbox` into a Cloudflare Queue with `env.COMMITS.sendBatch`, marking rows delivered on success and re-arming with backoff on failure; a queue-consumer Worker (`queue()` handler) is the fan-out point: it runs Workers AI on the commit message/diff summary, POSTs HMAC-signed webhooks, and can forward to Vectorize/D1 consumers. The push's `report-status` pkt-lines (`unpack ok`, `ok refs/heads/main`) are sent as soon as the transaction commits, so event delivery never blocks the client.

## Primitives
- Durable Objects with SQLite storage (`ctx.storage.sql`, `transactionSync`) — GA
- DO alarms (`ctx.storage.setAlarm` / `alarm()`) — GA
- R2 (`env.BUCKET.put/get`) for the commit objects themselves — GA
- Queues (`producer` binding `send`/`sendBatch`, `queue()` consumer with `retry`/`ack`, dead-letter queue) — GA
- Workers AI (`env.AI.run("@cf/meta/llama-3.1-8b-instruct", …)`) — GA (model catalog changes; some models beta)
- Outbound `fetch` from the consumer Worker for webhooks — GA
- Optional: Vectorize (GA), D1 (GA) as extra consumers of the same queue

## Proof code
```typescript
import { DurableObject } from "cloudflare:workers";

type Env = { BUCKET: R2Bucket; COMMITS: Queue<CommitEvent>; AI: Ai; WEBHOOK_SECRET: string };
type CommitEvent = { repo: string; ref: string; sha: string; tree: string; parents: string[];
                     author: string; message: string; pushedAt: number };

export class RepoDO extends DurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS refs   (name TEXT PRIMARY KEY, sha TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS outbox (id INTEGER PRIMARY KEY AUTOINCREMENT,
        event TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0)`);
  }

  // POST /git-receive-pack: body = pkt-line ref-update commands, "0000", then PACK
  async receivePack(req: Request, repo: string): Promise<Response> {
    const { commands, packStream } = splitCommandsAndPack(req.body!);      // pkt-line parser
    const newCommits: CommitEvent[] = [];
    // streaming-pack-parser: PACK header (sig, v2, count), per entry: type/size varint,
    // ofs-delta/ref-delta base, zlib body via DecompressionStream("deflate")
    for await (const obj of parsePack(packStream)) {
      await this.env.BUCKET.put(`${repo}/objects/${obj.sha}`, obj.raw);    // content-addressed
      if (obj.type === "commit") newCommits.push({ ...parseCommitHeader(obj.raw), sha: obj.sha, repo, ref: "", pushedAt: Date.now() });
    }
    const status: string[] = ["unpack ok\n"];
    this.ctx.storage.transactionSync(() => {                                // atomic ref flip + outbox
      for (const c of commands) {                                           // "<old> <new> refs/heads/x"
        const cur = this.ctx.storage.sql.exec<{ sha: string }>("SELECT sha FROM refs WHERE name=?", c.ref).toArray()[0]?.sha ?? "0".repeat(40);
        if (cur !== c.oldSha) { status.push(`ng ${c.ref} fetch-first\n`); continue; }   // CAS
        this.ctx.storage.sql.exec("INSERT OR REPLACE INTO refs VALUES (?,?)", c.ref, c.newSha);
        for (const ev of newCommits)
          this.ctx.storage.sql.exec("INSERT INTO outbox (event) VALUES (?)", JSON.stringify({ ...ev, ref: c.ref }));
        status.push(`ok ${c.ref}\n`);
      }
    });
    await this.ctx.storage.setAlarm(Date.now());                            // drain asap, off the request path
    return new Response(pktLines(status), { headers: { "content-type": "application/x-git-receive-pack-result" } });
  }

  async alarm(): Promise<void> {
    const rows = this.ctx.storage.sql.exec<{ id: number; event: string; attempts: number }>(
      "SELECT id, event, attempts FROM outbox ORDER BY id LIMIT 100").toArray();
    if (rows.length === 0) return;
    try {
      await this.env.COMMITS.sendBatch(rows.map(r => ({ body: JSON.parse(r.event) as CommitEvent })));
      this.ctx.storage.sql.exec(`DELETE FROM outbox WHERE id <= ?`, rows[rows.length - 1].id);
      if (rows.length === 100) await this.ctx.storage.setAlarm(Date.now());   // more to drain
    } catch {
      this.ctx.storage.sql.exec("UPDATE outbox SET attempts = attempts + 1");
      await this.ctx.storage.setAlarm(Date.now() + Math.min(60_000, 1000 * 2 ** rows[0].attempts));
    }
  }
}

// Consumer Worker: the actual fan-out. Each message is one commit.
export default {
  async queue(batch: MessageBatch<CommitEvent>, env: Env) {
    for (const msg of batch.messages) {
      const c = msg.body;
      try {
        const ai = await env.AI.run("@cf/meta/llama-3.1-8b-instruct", {
          messages: [{ role: "user", content: `One-line summary of this commit:\n${c.message}` }] });
        const payload = JSON.stringify({ ...c, summary: (ai as { response: string }).response });
        const sig = await hmacHex(env.WEBHOOK_SECRET, payload);
        const res = await fetch("https://hooks.example.com/git", { method: "POST",
          headers: { "content-type": "application/json", "x-git-edge-signature": sig }, body: payload });
        if (!res.ok && res.status >= 500) throw new Error(`webhook ${res.status}`);
        msg.ack();
      } catch { msg.retry({ delaySeconds: 30 }); }   // Queues retries; DLQ after max_retries
    }
  },
};
```

## Why it works
- `git-receive-pack` on the wire is: pkt-line ref-update commands (`<old-sha> <new-sha> <ref>\0report-status side-band-64k`), flush `0000`, then a single PACK. The DO already has to inflate every object to store it, so the `OBJ_COMMIT` entries (type 1) are free to identify: the pack contains exactly the objects the client believes the server lacks, which is by definition the set of new commits.
- The client only requires `report-status` lines (`unpack ok`, `ok <ref>` / `ng <ref> <reason>`) to succeed; nothing in the protocol waits on server-side side effects, so fan-out is legitimately asynchronous, the same way `post-receive` hooks run after the ref update in stock git.
- Ref flip and outbox insert share one `transactionSync`, so a commit is either both visible and queued-to-be-published or neither: the classic transactional-outbox pattern with the DO as the single writer (see `repo-do-ref-authority`), no distributed lock needed.
- The alarm is durable: if the isolate dies after commit but before `sendBatch`, the alarm re-fires on the next DO wake and drains the same rows. Delivery is at-least-once, and downstream consumers dedupe on `sha+ref`.
- Queues decouples slow fan-out (Workers AI inference, webhook targets with 10s latencies) from the DO, which is the throughput bottleneck for the repo; the consumer scales horizontally and `retry()`/DLQ handle flaky webhook endpoints without touching git state.
- Commit header parsing (`tree `, `parent `, `author `, blank line, message) is stable plain text in the inflated object, so events carry real metadata without re-reading R2.

## Known limits
- "Every commit" is really "every commit object in the pushed pack". A force-push that rewinds emits nothing new; a push of commits that are already reachable elsewhere (e.g. a branch off an already-pushed commit) emits nothing for them. Determining "first-time-reachable" precisely needs the commit graph (`want-have-negotiation`); this proof hand-waves it.
- Ordering: Queues does not guarantee order; the consumer receives commits from one push possibly out of topological order. Include `pushedAt` and parents so consumers can reorder; strict ordering would need a per-repo consumer keyed on the DO instead.
- Queue message limit is 128 KB and batch limit 256 KB; events carry metadata only, never diffs. Diff-bearing events must reference R2 keys.
- Alarm CPU/wall budget (30s CPU default, 15 min wall) bounds how many rows one drain handles; the code pages 100 at a time and re-arms.
- Webhook fan-out cost: one outbound `fetch` + one Workers AI call per commit; a monorepo push of 5,000 commits is 5,000 inference calls unless the consumer coalesces per push. Workers AI has per-account rate limits and neuron billing.
- R2 request costs are unchanged by this idea (objects are written once by the pack parser); the extra cost is Queue operations (per-million pricing) and DO alarm wakes.
- Single DO per repo: the outbox insert is inside the push transaction, so heavy fan-out volume adds SQLite write bytes to the push critical path (a few hundred bytes per commit; negligible until tens of thousands of commits per push).
- DO 128 MB memory: `newCommits` is held in memory during the push; a pack with millions of commits should stream outbox rows during parsing rather than buffering.
- Exactly-once is not achievable end-to-end; consumers must be idempotent.

## Depends on
- repo-do-ref-authority
- refs-sqlite-objects-r2
- streaming-pack-parser
- content-addressed-r2-keys
- Optional enrichment: github-webhook-compat (payload shape), hooks-as-workers (service-binding targets instead of HTTP webhooks), search-index-on-push and vectorized-commit-graph (additional queue consumers)
