> Idea #20 · edge · verdict: **lands with caveats** · feasibility 4/5 · reliability 4/5 · correctness 4/5 · effort: days
> Proof: [proofs/time-travel-refs.md](./time-travel-refs.md) · Review: [reviews/time-travel-refs.md](../../reviews/archived/time-travel-refs.md)

# Time-travel refs

## Mechanism
Every ref flip that `RepoDO.updateRefs` commits also appends a row to a `reflog(seq, ref, old_sha, new_sha, ts)` table in the same `transactionSync`, so the log is exactly as durable and ordered as the refs themselves. When a client runs `git fetch origin refs/at/1735689600/main`, its protocol v2 `ls-refs` command arrives at `POST /:owner/:repo/git-upload-pack` on the Worker, is routed to `env.REPO.idFromName("owner/repo")`, and the DO sees `ref-prefix refs/at/1735689600/main`; it parses the epoch, runs `SELECT new_sha FROM reflog WHERE ref='refs/heads/main' AND ts<=? ORDER BY ts DESC, seq DESC LIMIT 1`, and advertises the synthetic ref line `<sha> refs/at/1735689600/main`. Nothing is written: the client then issues an ordinary `fetch` command with `want <sha>`, which is served from R2 like any other object. The bare prefix `refs/at/` enumerates every reflog transition as `refs/at/<ts>/<short-ref>` so `git ls-remote origin 'refs/at/*'` shows the history of tip moves.

## Primitives
- Durable Objects with SQLite storage (`ctx.storage.sql.exec`, `transactionSync`) — GA
- DO alarms (`ctx.storage.setAlarm`) for reflog expiry, same as `gc.reflogExpire` — GA
- R2 `env.BUCKET.get` for the wanted objects (unchanged from the normal fetch path) — GA
- Workers `Request`/`Response` streams for the pkt-line bodies — GA
- No beta primitives

## Proof code
```typescript
// Same RepoDO as refs-sqlite-objects-r2; only the reflog table and the ls-refs handler are new.
const ZERO = "0".repeat(40);
const pkt = (s: string) => (s.length + 4).toString(16).padStart(4, "0") + s;

export class RepoDO implements DurableObject {
  constructor(private ctx: DurableObjectState, private env: Env) {
    ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS refs   (name TEXT PRIMARY KEY, sha TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS reflog (seq INTEGER PRIMARY KEY AUTOINCREMENT, ref TEXT NOT NULL,
                                         old_sha TEXT NOT NULL, new_sha TEXT NOT NULL, ts INTEGER NOT NULL);
      CREATE INDEX IF NOT EXISTS reflog_ref_ts ON reflog (ref, ts, seq)`);
  }

  /** Compare-and-swap from git-receive-pack; the reflog row lands in the same transaction as the ref. */
  private updateRefs(cmds: { old: string; nu: string; name: string }[]): string[] {
    const sql = this.ctx.storage.sql, now = Math.floor(Date.now() / 1000);
    return this.ctx.storage.transactionSync(() =>
      cmds.map(({ old, nu, name }) => {
        if (name.startsWith("refs/at/")) return `ng ${name} read-only namespace`;
        const cur = sql.exec<{ sha: string }>("SELECT sha FROM refs WHERE name=?", name).toArray()[0]?.sha ?? ZERO;
        if (cur !== old) return `ng ${name} fetch first`;
        if (nu === ZERO) sql.exec("DELETE FROM refs WHERE name=?", name);
        else sql.exec("INSERT INTO refs(name,sha) VALUES(?,?) ON CONFLICT(name) DO UPDATE SET sha=excluded.sha", name, nu);
        sql.exec("INSERT INTO reflog(ref,old_sha,new_sha,ts) VALUES(?,?,?,?)", name, old, nu, now);
        return `ok ${name}`;
      }),
    );
  }

  /** refs/at/<epoch>/<short>  ->  refs/heads/<short> (or refs/tags/<short>) as of <epoch>. */
  private resolveAt(name: string): string | undefined {
    const m = name.match(/^refs\/at\/(\d+)\/(.+)$/);
    if (!m) return undefined;
    const ts = Number(m[1]);
    for (const full of [`refs/heads/${m[2]}`, `refs/tags/${m[2]}`, m[2]]) {
      const row = this.ctx.storage.sql
        .exec<{ new_sha: string }>("SELECT new_sha FROM reflog WHERE ref=? AND ts<=? ORDER BY ts DESC, seq DESC LIMIT 1", full, ts)
        .toArray()[0];
      if (row && row.new_sha !== ZERO) return row.new_sha; // ZERO = ref was deleted at that instant
    }
    return undefined;
  }

  /** protocol v2 `command=ls-refs` with zero or more `ref-prefix` args. */
  private lsRefs(prefixes: string[]): Response {
    const sql = this.ctx.storage.sql;
    const lines: string[] = [];
    for (const p of prefixes.length ? prefixes : [""]) {
      const sha = this.resolveAt(p);                        // exact time-travel ref
      if (sha) { lines.push(`${sha} ${p}`); continue; }
      if (p === "refs/at/" || p === "refs/at") {           // enumerate transition points
        for (const r of sql.exec<{ ref: string; new_sha: string; ts: number }>(
          "SELECT ref,new_sha,ts FROM reflog WHERE new_sha<>? ORDER BY ts,seq", ZERO).toArray())
          lines.push(`${r.new_sha} refs/at/${r.ts}/${r.ref.replace(/^refs\/(heads|tags)\//, "")}`);
        continue;
      }
      for (const r of sql.exec<{ name: string; sha: string }>("SELECT name,sha FROM refs WHERE name LIKE ? ORDER BY name", p + "%").toArray())
        lines.push(`${r.sha} ${r.name}`);
    }
    return new Response(lines.map((l) => pkt(l + "\n")).join("") + "0000",
      { headers: { "content-type": "application/x-git-upload-pack-result" } });
  }

  /** Reflog expiry (git's gc.reflogExpire) as an alarm; keeps tips reachable for GC roots until then. */
  async alarm() {
    const cutoff = Math.floor(Date.now() / 1000) - 90 * 86400;
    this.ctx.storage.sql.exec("DELETE FROM reflog WHERE ts<? AND seq NOT IN (SELECT MAX(seq) FROM reflog GROUP BY ref)", cutoff);
    await this.ctx.storage.setAlarm(Date.now() + 86400_000);
  }

  // Used by gc-and-repack-alarm: reflog tips are GC roots, exactly like git's own reflog.
  gcRoots(): string[] {
    return this.ctx.storage.sql.exec<{ s: string }>(
      "SELECT sha AS s FROM refs UNION SELECT new_sha FROM reflog WHERE new_sha<>?", ZERO).toArray().map((r) => r.s);
  }
}
```

## Why it works
- Protocol v2 `fetch` is keyed on object ids, not ref names: the client first sends `ls-refs` with `ref-prefix refs/at/1735689600/main`, receives `<sha> refs/at/1735689600/main`, then sends `want <sha>`. The server never has to store the synthetic ref; it only has to advertise it consistently.
- `refs/at/<epoch>/<name>` passes `git check-ref-format` (digits and slashes only; ISO timestamps would not, because `:` is forbidden in refnames), so `git fetch origin refs/at/1735689600/main:refs/remotes/origin/main-jan1` and `git ls-remote` work with stock git.
- The reflog row is inserted inside the same `transactionSync` as the ref CAS, so the log can never disagree with the ref; git's own reflog is an append-only file written under the same ref lock.
- `ORDER BY ts DESC, seq DESC LIMIT 1` with `ts <= ?` is exactly `git rev-parse main@{<time>}` semantics: the last entry at or before the instant, and `new_sha = 0000...` (deletion) yields "no such ref".
- Objects behind a historical tip remain fetchable because `gcRoots()` unions reflog tips into the GC root set, mirroring `git gc` keeping reflog-reachable objects until `gc.reflogExpire`.
- v0 (`info/refs?service=git-upload-pack`) clients advertise only real refs; time-travel refs are still reachable there via `allow-reachable-sha1-in-want` if the client learned the sha out of band, but the ergonomic path is v2-only, which is the project's stated baseline.

## Known limits
- Timestamps are the DO's `Date.now()` at ref-flip time (server receive time), not committer or author dates. `main@{time}` in git has the same semantics, but users expecting "the commit as of the author date" get the wrong answer around slow or delayed pushes; a `refs/at/` lookup that predates the first reflog row returns nothing.
- Single-DO write serialization means `ts` is monotone per repo in practice, but clock skew across DO migrations/restarts is possible; `seq` is the true tie-breaker and a small `ts >= previous ts` clamp in `updateRefs` is needed for a hard guarantee (hand-waved above).
- The bare `refs/at/` enumeration returns one line per reflog entry; a hot repo with a 90-day retention can have tens of thousands of rows, and `ls-refs` output for it becomes several MB. Cap the enumeration or require a `refs/at/<epoch>` prefix in production.
- Reflog expiry deletes rows, after which GC may drop the objects and older `refs/at/` lookups start returning tips whose objects are gone. The `gcRoots()` union above only protects rows that still exist; retention must be decided per repo, and force-pushed history is the case where this bites.
- No `HEAD` symref resolution through time (`refs/at/<t>/HEAD`) and no branch renames; a renamed branch has two separate reflog streams, unlike a real git reflog which also does not follow renames, so this is parity rather than a regression.
- Time-travel refs are advertised only from the authority DO; if refs are replicated to KV at the edge (replicated-refs-edge), the replica must either proxy `refs/at/` prefixes to the DO or carry a copy of the reflog.
- Reflog rows are ~100 bytes each; DO SQLite is 10 GB per object, so storage is not a concern, but every `ls-refs` for a time-travel ref costs one DO request in addition to the fetch. No R2 cost is added.

## Depends on
- repo-do-ref-authority
- refs-sqlite-objects-r2
- protocol-v2-only
- gc-and-repack-alarm
