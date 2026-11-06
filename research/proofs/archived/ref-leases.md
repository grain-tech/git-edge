> Idea #44 · wild · verdict: **lands with caveats** · feasibility 5/5 · reliability 4/5 · correctness 4/5 · effort: days
> Proof: [proofs/ref-leases.md](./ref-leases.md) · Review: [reviews/ref-leases.md](../../reviews/archived/ref-leases.md)

# Ref leases

## Mechanism
A client acquires a lease with a plain HTTP call (`POST /:owner/:repo/leases {ref, ttl}`) or inline on a push via a git push-option (`git push -o lease=15m`); the edge Worker authenticates the caller and forwards to the repo's Durable Object (`idFromName("owner/repo")`), which inserts `(ref, holder, expires_at)` into a `leases` table in DO SQLite and arms `ctx.storage.setAlarm` for the earliest expiry. Enforcement happens in the one place every ref move already funnels through: the DO's phase-two of `git-receive-pack`, where each `old new refname` command line is checked against `leases` in the same synchronous SQLite transaction as the compare-and-swap on `refs`. A foreign, unexpired lease turns that command into `ng refs/heads/main lease held by alice until ...` in the report-status pkt-lines; the holder's own pushes go through and refresh the lease. The alarm just deletes expired rows (and can nudge WebSocket subscribers); correctness never depends on it because every check also compares `expires_at > now`.

## Primitives
- Durable Objects with SQLite storage (`ctx.storage.sql.exec`, `transactionSync`) — GA
- DO alarms (`ctx.storage.setAlarm` / `alarm()`) — GA
- Workers (edge routing, auth, pkt-line encode/decode) — GA
- R2 — untouched by this idea; objects were already written in phase one of the push
- (optional) WebSocket hibernation on the same DO to broadcast lease acquire/release — GA

## Proof code
```typescript
// RepoDO — the one DO per repo that already owns `refs` (CAS) and phase-two of push.
export class RepoDO implements DurableObject {
  constructor(private ctx: DurableObjectState, private env: Env) {
    ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS refs   (name TEXT PRIMARY KEY, sha TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS leases (ref TEXT PRIMARY KEY, holder TEXT NOT NULL, expires_at INTEGER NOT NULL);
    `);
  }

  async fetch(req: Request): Promise<Response> {
    const url = new URL(req.url);
    const holder = req.headers.get("x-git-principal")!;          // set by the edge Worker after token auth
    if (url.pathname === "/leases" && req.method === "POST") {
      const { ref, ttl } = await req.json<{ ref: string; ttl: number }>();
      return this.acquire(ref, holder, ttl) ? json({ ok: true }) : json({ error: "held" }, 409);
    }
    if (url.pathname === "/receive-pack/commit") return this.commitPush(req, holder);
    return json({ error: "not found" }, 404);
  }

  /** Acquire or refresh. Returns false if someone else holds an unexpired lease. Atomic: DO is single-threaded. */
  private acquire(ref: string, holder: string, ttlMs: number): boolean {
    const now = Date.now();
    const cur = this.ctx.storage.sql.exec<{ holder: string; expires_at: number }>(
      "SELECT holder, expires_at FROM leases WHERE ref = ?", ref).toArray()[0];
    if (cur && cur.holder !== holder && cur.expires_at > now) return false;
    this.ctx.storage.sql.exec(
      "INSERT OR REPLACE INTO leases (ref, holder, expires_at) VALUES (?, ?, ?)", ref, holder, now + ttlMs);
    void this.armAlarm();
    return true;
  }

  /** Phase two of a push (objects already in R2 under pending/). Body: the receive-pack command list + push-options. */
  private async commitPush(req: Request, holder: string): Promise<Response> {
    const { commands, pushOptions } = await req.json<{
      commands: { old: string; new: string; ref: string }[]; pushOptions: string[] }>();
    // `git push -o lease=15m` arrives as a push-option line after the command list (push-options capability).
    const leaseOpt = pushOptions.find((o) => o.startsWith("lease="));
    const now = Date.now();
    const results: string[] = [];
    this.ctx.storage.transactionSync(() => {
      for (const c of commands) {
        const lease = this.ctx.storage.sql.exec<{ holder: string; expires_at: number }>(
          "SELECT holder, expires_at FROM leases WHERE ref = ?", c.ref).toArray()[0];
        if (lease && lease.holder !== holder && lease.expires_at > now) {
          results.push(`ng ${c.ref} lease held by ${lease.holder} until ${new Date(lease.expires_at).toISOString()}`);
          continue;
        }
        const cur = this.ctx.storage.sql.exec<{ sha: string }>("SELECT sha FROM refs WHERE name = ?", c.ref).toArray()[0];
        if ((cur?.sha ?? "0".repeat(40)) !== c.old) { results.push(`ng ${c.ref} fetch first`); continue; }
        this.ctx.storage.sql.exec("INSERT OR REPLACE INTO refs (name, sha) VALUES (?, ?)", c.ref, c.new); // CAS ok
        if (leaseOpt) this.acquire(c.ref, holder, parseTtl(leaseOpt.slice(6)));   // acquire/refresh inline
        results.push(`ok ${c.ref}`);
      }
    });
    // report-status: pkt-line "unpack ok", one ok/ng line per command, flush. (Wrapped in side-band-64k by the Worker.)
    const body = [pkt("unpack ok\n"), ...results.map((r) => pkt(r + "\n")), "0000"].join("");
    return new Response(body, { headers: { "content-type": "application/x-git-receive-pack-result" } });
  }

  private async armAlarm() {
    const next = this.ctx.storage.sql.exec<{ t: number | null }>("SELECT MIN(expires_at) AS t FROM leases").toArray()[0]?.t;
    if (next != null) await this.ctx.storage.setAlarm(next);
  }

  async alarm() {
    this.ctx.storage.sql.exec("DELETE FROM leases WHERE expires_at <= ?", Date.now());
    await this.armAlarm();   // re-arm for the next live lease, if any
  }
}

const pkt = (s: string) => (s.length + 4).toString(16).padStart(4, "0") + s;
const parseTtl = (s: string) => Math.min(60 * 60_000, parseInt(s) * (s.endsWith("m") ? 60_000 : 1000));
const json = (v: unknown, status = 200) => Response.json(v, { status });
```

## Why it works
- git-receive-pack's wire contract is: command lines `old new refname` (first carrying capabilities such as `report-status push-options side-band-64k`), then optional push-option lines, then the PACK. The server may answer any individual command with `ng <ref> <reason>` while others get `ok`; the client prints the reason verbatim as `! [remote rejected] main -> main (lease held by alice until ...)`. Leases are therefore expressible with zero client changes.
- Push options (`git push -o key=value`, git >= 2.10, HTTP transport included) are the sanctioned way to carry side data with a push, so `-o lease=15m` rides the same request as the ref update and the acquire happens in the same DO transaction as the CAS — no window between "check lease" and "move ref".
- All ref writes for a repo already serialize through one DO (single-threaded, input gates), so the lease check + CAS + lease refresh in `transactionSync` is atomic without any distributed locking; there is no path that moves a ref without passing this code.
- Expiry is checked lazily (`expires_at > now`) on every enforcement, so the alarm is only garbage collection; a delayed or coalesced alarm cannot let a stale lease block anyone or let an unexpired one be bypassed.
- The out-of-band `POST /leases` endpoint gives non-git tooling (CI, an agent harness) a way to hold a branch before it even has objects to push, which `--force-with-lease` (a CAS on the expected old SHA, not a time lock) cannot do.

## Known limits
- Not a git-native concept: readers cannot discover a lease through `ls-refs`/`fetch`; they only learn of it when a push is rejected or by calling the REST endpoint. Advertising leases as a v2 capability (`lease-info`) would need a client patch (see `agent-native-commands`).
- Lease scope is per ref name, inside the DO that owns the ref. If refs are sharded (`branch-level-dos`), the lease row must live in the shard that owns that ref; cross-shard leases are not covered.
- One alarm per DO: `setAlarm` here would clobber alarms set by `gc-and-repack-alarm` or `alarm-chain-ci` on the same DO. Production needs a tiny alarm multiplexer table (`jobs(kind, due_at)`) and a single `alarm()` dispatcher; the proof hand-waves this.
- Holder identity is whatever the edge auth produces (`x-git-principal`); two CI jobs under one token are the same holder and cannot fence each other. Per-job fencing needs a lease token echoed back (`-o lease-token=...`), an easy extension not shown.
- TTL is clamped to 60 min in the proof; a crashed holder still blocks the branch until expiry — there is deliberately no "break lease" path, and adding one is a policy question (`auth-and-multitenancy` ACLs).
- The Worker must translate the `ng` lines into side-band-64k channel 1 if the client asked for it and stay under the 30s CPU budget; the lease logic itself is microseconds of SQLite and does not touch R2 or the 128MB DO memory limit.

## Depends on
- repo-do-ref-authority
- two-phase-push
- auth-and-multitenancy
- info-refs-endpoint
