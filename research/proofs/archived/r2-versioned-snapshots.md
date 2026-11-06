> Idea #21 · edge · verdict: **lands with caveats** · feasibility 4/5 · reliability 2/5 · correctness 3/5 · effort: days
> Proof: [proofs/r2-versioned-snapshots.md](./r2-versioned-snapshots.md) · Review: [reviews/r2-versioned-snapshots.md](../../reviews/archived/r2-versioned-snapshots.md)

# Snapshots via R2 object versioning of ref state

## Mechanism
`POST /:owner/:repo/git-receive-pack` reaches the repo DO (`idFromName("owner/repo")`) after the Worker has parsed the pack and written objects to R2. The DO applies the CAS ref updates in one SQLite transaction (`refs` + `meta.seq`), then, before emitting the `report-status` pkt-lines, PUTs a full refs snapshot to `R2 <prefix>/refs-snap/<seq zero-padded>.json` and overwrites `<prefix>/refs-snap/LATEST` (a pointer using `onlyIf` etag). On DO construction, `blockConcurrencyWhile` checks whether SQLite is empty while `LATEST` exists in R2; if so it is a wiped/recreated DO and it reloads `refs` and `seq` from the newest snapshot before serving any request. An alarm prunes old snapshots down to a retention window.

R2 has no S3-style bucket versioning, so "object versioning" is emulated with immutable per-seq keys plus a pointer; see Known limits.

## Primitives
- Durable Objects with SQLite storage (`ctx.storage.sql.exec`, `ctx.storage.transactionSync`) - GA
- `ctx.blockConcurrencyWhile` in the DO constructor for restore-before-serve - GA
- DO alarms (`ctx.storage.setAlarm`, `alarm()`) for snapshot pruning - GA
- R2 `put` / `get` / `list` with `onlyIf: { etagMatches }` conditional writes - GA
- DO SQLite point-in-time recovery (`ctx.storage.getCurrentBookmark` / `onNextSessionRestoreBookmark`) - GA, mentioned as the complementary built-in rollback, not used in the proof
- (Not used: R2 bucket versioning - does not exist on R2 as of 2026-09)

## Proof code
```typescript
// Repo DO: refs are authoritative in SQLite; every ref flip mirrors a snapshot to R2.
export interface Env { BUCKET: R2Bucket }
type RefUpdate = { name: string; oldOid: string; newOid: string }; // from receive-pack command list
const ZERO = "0".repeat(40);
const RETAIN = 50;

export class RepoDO implements DurableObject {
  private prefix: string;
  constructor(private ctx: DurableObjectState, private env: Env) {
    this.prefix = `repos/${ctx.id.toString()}`;
    ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS refs (name TEXT PRIMARY KEY, oid TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT NOT NULL);`);
    // Fresh SQLite but R2 has a snapshot => this DO was wiped/recreated. Restore before serving.
    ctx.blockConcurrencyWhile(() => this.restoreIfWiped());
  }

  private seq(): number {
    const r = this.ctx.storage.sql.exec("SELECT v FROM meta WHERE k='seq'").toArray()[0];
    return r ? Number(r.v) : 0;
  }

  private async restoreIfWiped(): Promise<void> {
    if (this.seq() > 0) return;
    const latest = await this.env.BUCKET.get(`${this.prefix}/refs-snap/LATEST`);
    if (!latest) return; // genuinely new repo
    const snapKey = await latest.text();
    const snap = await this.env.BUCKET.get(snapKey);
    if (!snap) throw new Error(`LATEST points at missing snapshot ${snapKey}`);
    const { seq, refs } = (await snap.json()) as { seq: number; refs: Record<string, string> };
    this.ctx.storage.transactionSync(() => {
      const sql = this.ctx.storage.sql;
      sql.exec("DELETE FROM refs");
      for (const [name, oid] of Object.entries(refs)) sql.exec("INSERT INTO refs VALUES (?, ?)", name, oid);
      sql.exec("INSERT OR REPLACE INTO meta VALUES ('seq', ?)", String(seq));
    });
  }

  // Phase two of a push: objects are already in R2 (content-addressed). CAS all refs, then snapshot.
  async applyPush(updates: RefUpdate[]): Promise<{ ok: boolean; status: string[] }> {
    const sql = this.ctx.storage.sql;
    let seq = 0;
    const status: string[] = [];
    try {
      this.ctx.storage.transactionSync(() => {
        for (const u of updates) {
          const cur = sql.exec("SELECT oid FROM refs WHERE name=?", u.name).toArray()[0]?.oid ?? ZERO;
          if (cur !== u.oldOid) throw new Error(`ng ${u.name} fetch first`); // git's CAS failure wording
          if (u.newOid === ZERO) sql.exec("DELETE FROM refs WHERE name=?", u.name);
          else sql.exec("INSERT OR REPLACE INTO refs VALUES (?, ?)", u.name, u.newOid);
          status.push(`ok ${u.name}`);
        }
        seq = this.seq() + 1;
        sql.exec("INSERT OR REPLACE INTO meta VALUES ('seq', ?)", String(seq));
      });
    } catch (e) {
      return { ok: false, status: [(e as Error).message] };
    }
    // Snapshot AFTER the SQLite commit and BEFORE report-status goes to the client.
    await this.snapshot(seq);
    if ((await this.ctx.storage.getAlarm()) === null) await this.ctx.storage.setAlarm(Date.now() + 60_000);
    return { ok: true, status }; // caller frames these as pkt-lines inside a sideband-64k report-status
  }

  private async snapshot(seq: number): Promise<void> {
    const refs: Record<string, string> = {};
    for (const r of this.ctx.storage.sql.exec("SELECT name, oid FROM refs ORDER BY name").toArray())
      refs[r.name as string] = r.oid as string;
    const key = `${this.prefix}/refs-snap/${String(seq).padStart(12, "0")}.json`;
    await this.env.BUCKET.put(key, JSON.stringify({ seq, at: Date.now(), refs }),
      { httpMetadata: { contentType: "application/json" } });
    // Pointer flip. The DO is single-writer, so etag CAS only guards against a stale duplicate DO instance.
    const cur = await this.env.BUCKET.head(`${this.prefix}/refs-snap/LATEST`);
    await this.env.BUCKET.put(`${this.prefix}/refs-snap/LATEST`, key,
      cur ? { onlyIf: { etagMatches: cur.etag } } : undefined);
  }

  // Retention: keep the newest RETAIN snapshots, delete the rest (never LATEST).
  async alarm(): Promise<void> {
    const listed = await this.env.BUCKET.list({ prefix: `${this.prefix}/refs-snap/` });
    const snaps = listed.objects.map(o => o.key).filter(k => k.endsWith(".json")).sort();
    const stale = snaps.slice(0, Math.max(0, snaps.length - RETAIN));
    if (stale.length) await this.env.BUCKET.delete(stale);
  }
}
```

## Why it works
- git-receive-pack's contract is: the ref update is durable once the client sees `ok <ref>` in `report-status`. The code commits SQLite, then PUTs the snapshot, then returns the status lines - so any `ok` the client observed is backed by both stores. A crash between commit and PUT loses at most a flip the client never saw acknowledged; a retried push is safe because objects are content-addressed and the CAS `old-oid` check either succeeds again or reports `ng ... fetch first`.
- Refs are the only mutable state in a git repo; objects are immutable and already in R2. So a refs snapshot plus the object bucket is a complete repo: `ls-refs` (protocol v2) needs exactly the `name -> oid` map that the snapshot carries.
- The snapshot is written per `seq`, and `seq` is stored in SQLite inside the same transaction as the ref flips, so snapshot N corresponds exactly to the post-state of push N. Recovery restores `refs` and `seq` together, so the next push continues the sequence instead of overwriting snapshot N+1 with a diverged history.
- "Wiped" is detectable without external state: `seq == 0` in SQLite while `LATEST` exists in R2 is impossible for a healthy DO, so the constructor can safely restore without a human flag. `blockConcurrencyWhile` guarantees no `ls-refs` or push is served against empty refs mid-restore.
- Because the snapshot files are immutable and ordered, they also serve `refs/at/<time>` (time-travel) and audit needs cheaply, and the pruning alarm bounds storage.

## Known limits
- R2 has no bucket versioning or version IDs, so the idea as literally stated cannot be built; the proof emulates it with immutable per-seq keys and a `LATEST` pointer. Behaviourally equivalent, but you pay for it in list/delete ops rather than getting it free.
- Full-snapshot-per-push is O(#refs) per push: a repo with 20k refs writes ~1-2 MB to R2 on every flip. At that scale you would write a delta (`{seq, updates[]}`) per push and let the alarm fold deltas into a full snapshot every N pushes; that changes restore to "load last full + replay deltas".
- One Class A R2 op (plus a `head` and a second `put` for the pointer) per push, ~3 ops. At $4.50/M this is negligible, but it adds two R2 round trips (~20-50 ms) to push latency inside the single-writer DO, which directly lowers per-repo push throughput.
- The proof's `applyPush` does not verify connectivity of `newOid` (belongs to two-phase-push). Restore also trusts that objects referenced by the snapshot still exist in R2; a GC that deleted objects unreachable from *current* refs could invalidate an older snapshot, so pruning must never keep snapshots older than the GC horizon.
- The commit-then-PUT ordering means a DO crash in that small window drops the last flip. The alternative (PUT before commit) leaves an orphan "future" snapshot which restore would wrongly adopt; I chose the loss-of-unacknowledged-write side deliberately. DO SQLite PITR (30-day bookmarks) covers the crash case for the DO itself; this idea only adds value when the DO's storage is gone (namespace deleted, class migrated, region evacuated).
- Restoring a repo with many refs in the constructor costs CPU before the first request; well under the 30 s limit for tens of thousands of refs, but the JSON parse holds the whole map in memory (fine against the 128 MB DO ceiling for anything realistic).

## Depends on
- repo-do-ref-authority
- refs-sqlite-objects-r2
- content-addressed-r2-keys
- two-phase-push
