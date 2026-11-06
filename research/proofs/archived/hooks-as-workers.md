> Idea #23 · edge · verdict: **risky** · feasibility 4/5 · reliability 2/5 · correctness 3/5 · effort: weeks
> Proof: [proofs/hooks-as-workers.md](./hooks-as-workers.md) · Review: [reviews/hooks-as-workers.md](../../reviews/archived/hooks-as-workers.md)

# Pre/post-receive hooks as Workers via service bindings

## Mechanism
The repo DO keeps a `hooks` table (phase, target) that the owner fills through `PUT /:owner/:repo/hooks`; a target is either a static service binding name (`service:HOOK_LINT`, wired in git-edge's own wrangler.jsonc, for first-party or self-hosted deployments) or, for arbitrary user-deployed Workers, a script name in a Workers for Platforms dispatch namespace (`dispatch:acme-policy`, resolved with `env.HOOKS.get(name)`). During `git-receive-pack`, after the Worker has streamed the pack into R2 and before the DO's ref compare-and-swap transaction, `RepoDO.commit()` resolves each `pre-receive` target to a `Fetcher` and POSTs the exact stdin git would hand a pre-receive hook (`<old-sha> <new-sha> <refname>\n` per line, plus JSON and a scoped read token so the hook can `git fetch` the pending objects) with a 10 s `AbortSignal.timeout`; any non-2xx turns into `ng <ref> <body>` for every ref, mirroring git's all-or-nothing pre-receive. `post-receive` rows are inserted into an `outbox` table inside the same SQLite transaction that flips the refs, and a DO alarm drains the outbox with backoff, so post-receive delivery is at-least-once and survives the DO being evicted.

## Primitives
- Durable Objects with SQLite storage (`ctx.storage.sql.exec`, `transactionSync`) — GA
- DO alarms (`setAlarm` / `alarm()`) for the post-receive outbox — GA
- Service bindings (`Fetcher` in `env`, `fetch()` or RPC via `WorkerEntrypoint`) — GA; static per wrangler config, one binding per registered hook Worker
- Workers for Platforms dispatch namespaces (`env.HOOKS.get(scriptName)`) for user-registered Workers — GA, but a paid add-on; without it "register your own Worker" degrades to an HTTPS webhook URL with an HMAC header
- Dynamic Workers / Worker Loaders (`env.LOADER.get(id, () => ({ mainModule, modules }))`) as an alternative for hook *source* uploaded to the host — **beta**, not used in the proof
- R2 (`pending/<pushId>/` objects already written by the two-phase push; the hook reads them through the normal smart-HTTP endpoint, not R2 directly) — GA
- `AbortSignal.timeout`, `crypto.subtle` HMAC for webhook fallback — GA

## Proof code
```typescript
import { DurableObject } from "cloudflare:workers";
type Env = {
  BUCKET: R2Bucket;
  HOOKS: DispatchNamespace;                       // Workers for Platforms: user-deployed hook Workers
  [staticBinding: string]: unknown;               // e.g. HOOK_LINT: Fetcher, declared in wrangler.jsonc "services"
};
type Cmd = { old: string; new: string; ref: string };
type Hook = { id: number; phase: "pre-receive" | "post-receive"; target: string }; // "service:HOOK_LINT" | "dispatch:acme-policy" | "https://..."
const ZERO = "0".repeat(40);

export class RepoDO extends DurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS refs   (name TEXT PRIMARY KEY, sha TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS hooks  (id INTEGER PRIMARY KEY, phase TEXT NOT NULL, target TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS outbox (id INTEGER PRIMARY KEY, hook_id INTEGER NOT NULL, payload TEXT NOT NULL,
                                         attempts INTEGER NOT NULL DEFAULT 0, next_at INTEGER NOT NULL)`);
  }

  /** Resolve a registered target to something with .fetch(). */
  private fetcher(target: string): Fetcher {
    if (target.startsWith("service:")) return this.env[target.slice(8)] as Fetcher;      // static service binding
    if (target.startsWith("dispatch:")) return this.env.HOOKS.get(target.slice(9));      // user Worker in dispatch namespace
    return { fetch: (r: Request) => fetch(new Request(target, r)) } as Fetcher;         // plain webhook fallback
  }

  private payload(phase: string, cmds: Cmd[], pushId: string) {
    const stdin = cmds.map(c => `${c.old} ${c.new} ${c.ref}\n`).join("");           // byte-identical to git's hook stdin
    return new Request(`https://git-edge.hook/${phase}`, {
      method: "POST",
      headers: { "content-type": "text/plain", "x-git-edge-depth": "1",            // hook pushing back stops at depth>1
                 "x-git-edge-json": JSON.stringify({ repo: this.ctx.id.name, pushId, updates: cmds }),
                 "x-git-edge-token": pushId },                                       // scoped read token: lets hook fetch pending objects
      body: stdin,
      signal: AbortSignal.timeout(10_000),
    });
  }

  /** Phase two of two-phase-push: objects are already in R2; now gate and flip refs. */
  async commit(pushId: string, cmds: Cmd[]) {
    // 1. pre-receive: outside the SQLite transaction (it awaits), all-or-nothing like git
    const pre = this.ctx.storage.sql.exec<Hook>("SELECT * FROM hooks WHERE phase = 'pre-receive'").toArray();
    for (const h of pre) {
      let res: Response;
      try { res = await this.fetcher(h.target).fetch(this.payload("pre-receive", cmds, pushId)); }
      catch (e) { return cmds.map(c => ({ ref: c.ref, ok: false, reason: `pre-receive hook ${h.id} unreachable` })); }
      if (!res.ok) {
        const msg = (await res.text()).trim().slice(0, 200);                          // becomes "remote: <msg>" on the client
        return cmds.map(c => ({ ref: c.ref, ok: false, reason: `pre-receive hook declined: ${msg}` }));
      }
    }
    // 2. CAS + ref flip + post-receive enqueue in ONE transaction; concurrent pushes serialize on this DO
    const post = this.ctx.storage.sql.exec<Hook>("SELECT * FROM hooks WHERE phase = 'post-receive'").toArray();
    const results = this.ctx.storage.transactionSync(() => {
      const out = cmds.map(c => {
        const cur = this.ctx.storage.sql.exec<{ sha: string }>("SELECT sha FROM refs WHERE name = ?", c.ref).toArray()[0]?.sha ?? ZERO;
        return cur === c.old ? { ref: c.ref, ok: true } : { ref: c.ref, ok: false, reason: "fetch first" };
      });
      const accepted = cmds.filter((_, i) => out[i].ok);
      for (const c of accepted) c.new === ZERO
        ? this.ctx.storage.sql.exec("DELETE FROM refs WHERE name = ?", c.ref)
        : this.ctx.storage.sql.exec("INSERT OR REPLACE INTO refs (name, sha) VALUES (?, ?)", c.ref, c.new);
      if (accepted.length) for (const h of post)                                     // git only tells post-receive about updated refs
        this.ctx.storage.sql.exec("INSERT INTO outbox (hook_id, payload, next_at) VALUES (?, ?, ?)",
          h.id, JSON.stringify({ cmds: accepted, pushId }), Date.now());
      return out;
    });
    if (post.length) await this.ctx.storage.setAlarm(Date.now());                    // drain now; the client is not kept waiting
    return results;
  }

  /** post-receive delivery: at-least-once with exponential backoff, capped. */
  async alarm() {
    const due = this.ctx.storage.sql.exec<{ id: number; hook_id: number; payload: string; attempts: number }>(
      "SELECT o.id, o.hook_id, o.payload, o.attempts FROM outbox o WHERE next_at <= ? LIMIT 50", Date.now()).toArray();
    for (const row of due) {
      const hook = this.ctx.storage.sql.exec<Hook>("SELECT * FROM hooks WHERE id = ?", row.hook_id).toArray()[0];
      const { cmds, pushId } = JSON.parse(row.payload);
      let ok = false;
      if (hook) try { ok = (await this.fetcher(hook.target).fetch(this.payload("post-receive", cmds, pushId))).ok; } catch {}
      if (ok || !hook || row.attempts >= 8) this.ctx.storage.sql.exec("DELETE FROM outbox WHERE id = ?", row.id);
      else this.ctx.storage.sql.exec("UPDATE outbox SET attempts = attempts + 1, next_at = ? WHERE id = ?",
        Date.now() + 2 ** row.attempts * 1_000, row.id);
    }
    const next = this.ctx.storage.sql.exec<{ t: number }>("SELECT MIN(next_at) AS t FROM outbox").toArray()[0]?.t;
    if (next != null) await this.ctx.storage.setAlarm(Math.max(next, Date.now() + 1_000));
  }
}

// A user's hook Worker (deployed into the dispatch namespace, or bound as HOOK_LINT):
export default {
  async fetch(req: Request): Promise<Response> {
    for (const line of (await req.text()).split("\n").filter(Boolean)) {
      const [oldSha, newSha, ref] = line.split(" ");
      if (ref === "refs/heads/main" && newSha === ZERO) return new Response("deleting main is not allowed", { status: 403 });
      if (oldSha !== ZERO && newSha !== ZERO && !(await isFastForward(req, oldSha, newSha))) return new Response("non-fast-forward", { status: 403 });
    }
    return new Response("ok");                                // 2xx == exit 0
  },
};
```

## Why it works
- git's pre-receive contract is "stdin gets `<old> <new> <ref>` lines, exit 0 allows, non-zero rejects every ref in the push, stderr goes to the client as `remote:` lines". The proof sends byte-identical stdin, maps HTTP status to the exit code and the response body to the reason; `git push` prints `! [remote rejected] main -> main (pre-receive hook declined: ...)` because `commit()` returns `ng <ref> <reason>` for each command and the Worker emits it as report-status pkt-lines (`ok`/`ng` after `unpack ok`, sideband channel 2 for the free-text message).
- Ordering matches `git-receive-pack`: objects are unpacked (already in R2 under `pending/<pushId>/` from the two-phase push) *before* pre-receive runs, so the hook can inspect the new commits; refs move only after the hook accepts; post-receive sees only refs that actually changed.
- Pre-receive must not run inside `transactionSync` (it awaits), and it does not need to: the DO re-checks `old == current` in the transaction afterwards, so a concurrent push that slipped in between yields `fetch first`, exactly the race git resolves with its ref lock.
- A service-binding call is an in-process subrequest, not a public HTTP hop: the DO's `Fetcher.fetch()` or RPC lands directly in the hook Worker with no DNS, TLS or origin round trip, which is what keeps a synchronous pre-receive gate inside a 10 s budget. Dispatch-namespace stubs (`env.HOOKS.get(name)`) have the same `Fetcher` shape, so one code path serves both.
- Post-receive in git is fire-and-forget and lossy; enqueueing the outbox row in the same SQLite transaction as the ref flip makes it strictly better (atomic with the update, retried by the alarm, single-threaded per repo so ordering per repo is preserved).
- The `x-git-edge-depth` header plus a scoped token lets a hook fetch the pending objects through the ordinary `git-upload-pack` endpoint (with the token unlocking `pending/<pushId>/`) rather than exposing R2, and stops a hook that pushes back to the same repo from recursing.

## Known limits
- **"Register a Worker" literally via service bindings is not possible at runtime.** Service bindings are declared in wrangler.jsonc at deploy time, so `service:` targets only cover hooks the git-edge operator ships (or a self-hosted single-tenant deployment). Letting *users* register their own Worker requires Workers for Platforms (dispatch namespaces, paid add-on) or falls back to an HTTPS webhook with HMAC-signed body, which loses the in-process latency advantage.
- The push client waits on pre-receive synchronously; the DO has no wall-clock limit but the hook Worker has a 30 s CPU cap (up to 5 min on paid) and the proof caps it at 10 s. Long policy checks belong in post-receive or in alarm-chain-ci.
- Only `pre-receive` and `post-receive` are modelled; git's `update` (per-ref, partial reject) and `post-update` are trivial variants but omitted; `proc-receive` / `push-options` (`-o key=val`) are hand-waved (they would ride in the JSON header).
- Hooks cannot run git commands. `isFastForward` in the sample must be implemented by walking commits via git-edge's own HTTP API or by the hook doing a smart-HTTP `fetch` of the pending objects, which costs R2 reads (class B ops) and, for deep history, multiple Worker subrequests; the proof hand-waves this function.
- DO subrequest cap (1000 per incoming request) and 128 MB memory: hook fan-out is per-registered-hook, sequential, and payloads are ref lines only, so this is far from the cap; but a pre-receive hook that itself fetches a giant pack runs in its own Worker under its own limits.
- Single-DO throughput: pre-receive latency adds directly to the repo's push serialisation window (the DO can interleave other requests during the `await`, but CAS conflicts rise). Post-receive is off the critical path.
- Outbox rows carry no authentication secret rotation; HMAC signing and secret storage for the webhook fallback are not shown.

## Depends on
- two-phase-push (objects in R2 under `pending/<pushId>/` before refs flip; `commit()` is its phase two)
- repo-do-ref-authority (the single DO that serialises CAS and hosts the hooks/outbox tables)
- auth-and-multitenancy (owner/repo → DO id, and the scoped `pushId` token the hook uses to read pending objects)
- scoped-token-remotes (optional: the read token the hook receives)
- github-webhook-compat (optional: alternative JSON payload shape for post-receive)
