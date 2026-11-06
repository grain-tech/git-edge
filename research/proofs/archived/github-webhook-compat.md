> Idea #49 · wild · verdict: **lands with caveats** · feasibility 4/5 · reliability 3/5 · correctness 3/5 · effort: days
> Proof: [proofs/github-webhook-compat.md](./github-webhook-compat.md) · Review: [reviews/github-webhook-compat.md](../../reviews/archived/github-webhook-compat.md)

# GitHub-compatible webhook payloads

## Mechanism
A `POST /:owner/:repo/git-receive-pack` lands on the Worker, which forwards it to the repo DO (`idFromName("owner/repo")`); after the packfile is indexed and phase two of the push flips refs, the same `transactionSync` that writes the new ref values inserts one row per ref update into a `webhook_outbox` table in DO SQLite, then calls `ctx.storage.setAlarm(Date.now())`. The alarm handler drains the outbox: it builds a byte-for-byte GitHub `push` event JSON (`ref`, `before`, `after`, `created/deleted/forced`, `commits[]`, `head_commit`, `pusher`, `repository`), signs it with the hook secret as `X-Hub-Signature-256: sha256=<hmac>` via WebCrypto, and `fetch()`es each subscribed URL with `X-GitHub-Event`, `X-GitHub-Delivery` and a `GitHub-Hookshot/` user agent. Commit metadata comes from parsing the commit objects that the push just wrote (in-memory during indexing, or one R2 GET each on retry); failures are retried with exponential backoff by re-arming the alarm, so delivery is at-least-once and survives DO eviction because the outbox row is the source of truth.

## Primitives
- Durable Objects with SQLite storage (`ctx.storage.sql.exec`, `ctx.storage.transactionSync`) - GA
- DO alarms (`ctx.storage.setAlarm`, `alarm()`) - GA
- R2 `env.BUCKET.get(key)` for commit objects not still in memory - GA
- WebCrypto `crypto.subtle.sign("HMAC")` for `X-Hub-Signature-256` - GA
- Outbound `fetch()` from the DO to the subscriber URL - GA
- `crypto.randomUUID()` for `X-GitHub-Delivery` - GA
- (optional) Queues for fan-out to many subscribers - GA, but not required here

## Proof code
```typescript
// RepoDO: outbox row written in the same SQLite transaction as the ref flip,
// alarm drains it into GitHub-shaped `push` deliveries.
type RefUpdate = { ref: string; before: string; after: string; pusher: string };
type Commit = { id: string; tree: string; parents: string[]; author: Ident; committer: Ident; message: string };
type Ident = { name: string; email: string; date: string };
const ZERO = "0".repeat(40);

export class RepoDO implements DurableObject {
  constructor(private ctx: DurableObjectState, private env: { BUCKET: R2Bucket }) {
    ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS refs (name TEXT PRIMARY KEY, sha TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS hooks (id TEXT PRIMARY KEY, url TEXT NOT NULL, secret TEXT NOT NULL, events TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS webhook_outbox (
        delivery_id TEXT PRIMARY KEY, hook_id TEXT NOT NULL, event TEXT NOT NULL,
        payload TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, next_at INTEGER NOT NULL);`);
  }

  // Called by the receive-pack path after objects are in R2 (two-phase-push, phase 2).
  // `commits` are the commit objects parsed while indexing the pack (before..after, newest first).
  async commitPush(updates: RefUpdate[], commits: Map<string, Commit>) {
    const sql = this.ctx.storage.sql;
    this.ctx.storage.transactionSync(() => {
      for (const u of updates) {
        if (u.after === ZERO) sql.exec("DELETE FROM refs WHERE name = ?", u.ref);
        else sql.exec("INSERT INTO refs(name, sha) VALUES (?, ?) ON CONFLICT(name) DO UPDATE SET sha = excluded.sha", u.ref, u.after);
        const payload = JSON.stringify(this.pushEvent(u, commits));
        for (const h of sql.exec<{ id: string; events: string }>("SELECT id, events FROM hooks").toArray()) {
          if (!JSON.parse(h.events).includes("push")) continue;
          sql.exec("INSERT INTO webhook_outbox(delivery_id, hook_id, event, payload, next_at) VALUES (?, ?, 'push', ?, ?)",
            crypto.randomUUID(), h.id, payload, Date.now());
        }
      }
    });
    await this.ctx.storage.setAlarm(Date.now()); // drain outside the push request
  }

  // Exact field set GitHub's `push` event carries; consumers (Actions runners, Jenkins,
  // Slack apps, Netlify) key off ref/before/after/head_commit/repository.full_name.
  private pushEvent(u: RefUpdate, commits: Map<string, Commit>) {
    const list = this.walk(u.after, u.before, commits).slice(0, 20); // GitHub caps at 20 too
    const gc = (c: Commit) => ({
      id: c.id, tree_id: c.tree, distinct: true, message: c.message, timestamp: c.author.date,
      url: `${this.env_base()}/commit/${c.id}`,
      author: { name: c.author.name, email: c.author.email }, committer: { name: c.committer.name, email: c.committer.email },
      added: [] as string[], removed: [] as string[], modified: [] as string[], // see Known limits
    });
    const head = commits.get(u.after);
    return {
      ref: u.ref, before: u.before, after: u.after,
      created: u.before === ZERO, deleted: u.after === ZERO,
      forced: false, // needs ancestor check via commit graph; see Known limits
      base_ref: null, compare: `${this.env_base()}/compare/${u.before.slice(0, 12)}...${u.after.slice(0, 12)}`,
      commits: list.map(gc), head_commit: head ? gc(head) : null,
      pusher: { name: u.pusher, email: `${u.pusher}@users.noreply.git-edge` },
      sender: { login: u.pusher, type: "User" },
      repository: this.repoBlock(),
    };
  }

  async alarm() {
    const sql = this.ctx.storage.sql;
    const due = sql.exec<{ delivery_id: string; hook_id: string; event: string; payload: string; attempts: number }>(
      "SELECT delivery_id, hook_id, event, payload, attempts FROM webhook_outbox WHERE next_at <= ? LIMIT 20", Date.now()).toArray();
    for (const d of due) {
      const hook = sql.exec<{ url: string; secret: string }>("SELECT url, secret FROM hooks WHERE id = ?", d.hook_id).one();
      const body = new TextEncoder().encode(d.payload);
      const key = await crypto.subtle.importKey("raw", new TextEncoder().encode(hook.secret), { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
      const sig = [...new Uint8Array(await crypto.subtle.sign("HMAC", key, body))].map(b => b.toString(16).padStart(2, "0")).join("");
      let ok = false;
      try {
        const res = await fetch(hook.url, {
          method: "POST", body,
          headers: {
            "Content-Type": "application/json", "User-Agent": "GitHub-Hookshot/git-edge",
            "X-GitHub-Event": d.event, "X-GitHub-Delivery": d.delivery_id, "X-GitHub-Hook-ID": d.hook_id,
            "X-Hub-Signature-256": `sha256=${sig}`,
          },
          signal: AbortSignal.timeout(10_000), // GitHub also gives receivers 10s
        });
        ok = res.status < 300;
      } catch {}
      if (ok || d.attempts >= 5) sql.exec("DELETE FROM webhook_outbox WHERE delivery_id = ?", d.delivery_id);
      else sql.exec("UPDATE webhook_outbox SET attempts = attempts + 1, next_at = ? WHERE delivery_id = ?",
        Date.now() + 2 ** d.attempts * 30_000, d.delivery_id);
    }
    const next = sql.exec<{ t: number | null }>("SELECT MIN(next_at) AS t FROM webhook_outbox").one().t;
    if (next != null) await this.ctx.storage.setAlarm(next);
  }

  // Newest-first walk from `after` until `before` is reached or the map runs out (R2 fallback elided).
  private walk(after: string, before: string, commits: Map<string, Commit>): Commit[] {
    const out: Commit[] = []; const seen = new Set<string>(); const q = [after];
    while (q.length) { const id = q.shift()!; if (id === before || seen.has(id) || !commits.has(id)) continue;
      seen.add(id); const c = commits.get(id)!; out.push(c); q.push(...c.parents); }
    return out;
  }
  private repoBlock() { return { full_name: this.name, name: this.name.split("/")[1], default_branch: "main",
    clone_url: `${this.env_base()}.git`, html_url: this.env_base(), private: true, owner: { login: this.name.split("/")[0] } }; }
  private get name() { return this.ctx.storage.sql.exec<{ v: string }>("SELECT v FROM meta WHERE k='name'").one().v; }
  private env_base() { return `https://git-edge.example/${this.name}`; }
}
```

## Why it works
- Every field a receiver actually dispatches on (`ref`, `before`, `after`, `created`, `deleted`, `head_commit.id`, `repository.full_name`, `pusher.name`, `X-GitHub-Event`, `X-Hub-Signature-256`) is derivable from the receive-pack command lines (`<old-sha> <new-sha> <refname>` in pkt-line) plus the commit objects the push just delivered, so no extra git state is needed beyond what the push already contains.
- The outbox row is written in the same `transactionSync` as the ref flip, so a webhook is emitted iff the ref actually moved: no ghost events on rejected pushes, no lost events if the DO is evicted between flip and delivery.
- `X-Hub-Signature-256` is `HMAC-SHA256(secret, raw body)` hex-encoded; existing verifiers (`@octokit/webhooks`, Jenkins GitHub plugin, Argo CD) recompute it over the exact bytes, and we sign the same bytes we send.
- Delivery runs in `alarm()`, not in the push request, so the client's `git push` gets its `unpack ok` / `ok refs/heads/main` report-status pkt-lines immediately and a slow subscriber cannot stall the push or eat its CPU budget.
- Retries are idempotent from the receiver's point of view because `X-GitHub-Delivery` is stable across attempts (same row), matching GitHub's redelivery semantics.
- Commit parsing is the plain `tree`/`parent`/`author`/`committer` header lines of the loose commit object (`commit <len>\0` after inflate), which the pack indexer already has to inflate to do connectivity checks.

## Known limits
- `added/removed/modified` per commit require a tree diff against each parent; that is one R2 GET per changed tree object and is left empty here. Path-filtered CI (`paths:` in Actions workflows) would see nothing changed. Filling them means walking trees from R2 in the alarm, bounded to the 20-commit cap.
- `forced` is hard-coded `false`; computing it correctly needs to know whether `before` is an ancestor of `after`, which is a commit-graph query (want-have-negotiation's SQLite graph) rather than something the pushed pack alone tells you.
- The `commits` map only covers objects in this push; a push that references history not in the pack (already on the server) falls back to R2 GETs, costing one Class B request per commit and up to 20 per event.
- Only the `push` event is proven. `create`/`delete` are trivial siblings of the same row; `pull_request`, `issues`, `check_run` etc. have no source of truth in a bare git server unless reviews-as-refs or similar supplies it.
- The outbox drain is single-DO: a repo with N hooks fans out N HTTP calls per ref per push serially inside one alarm, each with a 10 s timeout, inside the alarm's CPU/wall budget. Past a handful of hooks, hand the row to Queues (commit-event-stream) instead of `fetch`ing from the DO.
- `repository.id`, `sender.id`, `installation` and the numeric `hook_id` GitHub Apps expect are synthetic; tooling that calls back to `api.github.com` with them will not work.
- Alarm scheduling is best-effort with retries capped at 5 attempts / ~15 min, versus GitHub's redelivery UI; there is no operator-facing redelivery button unless one is built on the outbox table.

## Depends on
- two-phase-push
- repo-do-ref-authority
- refs-sqlite-objects-r2
- streaming-pack-parser
- alarm-chain-ci (same alarm-as-queue pattern)
- commit-event-stream (for high fan-out)
