> Idea #22 · edge · verdict: **risky** · feasibility 4/5 · reliability 2/5 · correctness 3/5 · effort: weeks
> Proof: [proofs/cow-forks.md](./cow-forks.md) · Review: [reviews/cow-forks.md](../../reviews/archived/cow-forks.md)

# Copy-on-write forks

## Mechanism
`POST /:owner/:repo/fork?as=bob/repo` hits the Worker, which resolves a brand-new `RepoDO` via `env.REPO.idFromName("bob/repo")` and calls its `/init-fork` with `parent=alice/repo`. The fork DO fetches the parent DO's `refs` rows (small, copied synchronously) and then pages the parent's `objects(sha,type,size,layer)` index into its own SQLite via an alarm chain, keeping each row's `layer` column (an R2 prefix such as `objects/alice/repo`) so a fork-of-a-fork flattens rather than walking a chain; the parent records the fork and pins its ref tips at fork time as extra GC roots. Nothing is copied in R2: `git-upload-pack` on the fork resolves every wanted sha to `${layer}/${sha}` and does `env.BUCKET.get` from whichever prefix owns it, while `git-receive-pack` on the fork writes every uploaded object to `objects/bob/repo/...` and upserts the row with `layer = own`. Deduplication against the parent is done by git negotiation itself: the fork advertises the parent's refs as its own, so a client never uploads objects reachable from them.

## Primitives
- Durable Objects with SQLite storage (`ctx.storage.sql.exec`, `transactionSync`) -- GA
- DO alarms (`ctx.storage.setAlarm`) to page a large object index across requests -- GA
- DO-to-DO calls via stub `fetch` (RPC methods on `DurableObject` subclasses also GA; `fetch` shown to match `refs-sqlite-objects-r2`) -- GA
- R2 (`env.BUCKET.get/put/head`, `get(key,{range})` for pack slices) -- GA
- `crypto.subtle.digest("SHA-1")` -- GA
- No beta primitives required

## Proof code
```typescript
// Layout (from content-addressed-r2-keys): objects/<owner>/<repo>/<aa>/<rest-of-sha>
// A fork's SQLite index is the whole trick: every sha row says which R2 prefix holds it.
export interface Env { REPO: DurableObjectNamespace; BUCKET: R2Bucket }
const key = (layer: string, sha: string) => `${layer}/${sha.slice(0, 2)}/${sha.slice(2)}`;

export class RepoDO implements DurableObject {
  constructor(private ctx: DurableObjectState, private env: Env) {
    ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS meta    (k TEXT PRIMARY KEY, v TEXT NOT NULL);           -- name, parent, import_cursor
      CREATE TABLE IF NOT EXISTS refs    (name TEXT PRIMARY KEY, sha TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS objects (sha TEXT PRIMARY KEY, type TEXT NOT NULL, size INTEGER NOT NULL,
                                          layer TEXT NOT NULL);                             -- R2 prefix owning the bytes
      CREATE TABLE IF NOT EXISTS gc_pins (sha TEXT PRIMARY KEY, holder TEXT NOT NULL);     -- extra GC roots: fork tips
      CREATE TABLE IF NOT EXISTS forks   (name TEXT PRIMARY KEY, created_at INTEGER NOT NULL)`);
  }
  private get name() { return this.ctx.storage.sql.exec("SELECT v FROM meta WHERE k='name'").one().v as string; }
  private get own()  { return `objects/${this.name}`; }

  async fetch(req: Request): Promise<Response> {
    const url = new URL(req.url); const q = url.searchParams;
    switch (url.pathname) {
      case "/init-fork": return this.initFork(q.get("name")!, q.get("parent")!);
      case "/snapshot":  return this.snapshot(q.get("fork")!, q.get("after") ?? "");
      case "/git-upload-pack": return this.uploadPack(req);
      case "/git-receive-pack": return this.receivePack(req);
      default: return new Response("not found", { status: 404 });
    }
  }

  // ---- fork side --------------------------------------------------------------
  private async initFork(name: string, parent: string) {
    const sql = this.ctx.storage.sql;
    if (sql.exec("SELECT 1 FROM meta WHERE k='name'").toArray().length) return new Response("exists", { status: 409 });
    const stub = this.env.REPO.get(this.env.REPO.idFromName(parent));
    // First page also carries the refs (tiny) and makes the parent pin its tips for us.
    const page = await (await stub.fetch(`https://do/snapshot?fork=${name}&after=`)).json() as Snapshot;
    this.ctx.storage.transactionSync(() => {
      sql.exec("INSERT INTO meta VALUES ('name',?),('parent',?),('import_cursor',?)", name, parent, page.cursor);
      for (const r of page.refs) sql.exec("INSERT INTO refs VALUES (?,?)", r.name, r.sha);
      this.insertRows(page.rows);
    });
    if (!page.done) await this.ctx.storage.setAlarm(Date.now()); // keep paging without blocking the HTTP fork call
    return Response.json({ ok: true, importing: !page.done });
  }
  private insertRows(rows: Row[]) {
    for (const o of rows) this.ctx.storage.sql.exec(
      "INSERT OR IGNORE INTO objects VALUES (?,?,?,?)", o.sha, o.type, o.size, o.layer); // layer preserved: fork-of-fork flattens
  }
  async alarm() {
    const sql = this.ctx.storage.sql;
    const parent = sql.exec("SELECT v FROM meta WHERE k='parent'").one().v as string;
    const after  = sql.exec("SELECT v FROM meta WHERE k='import_cursor'").one().v as string;
    const stub = this.env.REPO.get(this.env.REPO.idFromName(parent));
    const page = await (await stub.fetch(`https://do/snapshot?fork=${this.name}&after=${after}`)).json() as Snapshot;
    this.ctx.storage.transactionSync(() => {
      this.insertRows(page.rows);
      sql.exec("UPDATE meta SET v=? WHERE k='import_cursor'", page.done ? "done" : page.cursor);
    });
    if (!page.done) await this.ctx.storage.setAlarm(Date.now());
  }

  // ---- parent side ------------------------------------------------------------
  private snapshot(fork: string, after: string): Response {
    const sql = this.ctx.storage.sql;
    const refs = sql.exec("SELECT name, sha FROM refs").toArray() as { name: string; sha: string }[];
    if (after === "") this.ctx.storage.transactionSync(() => {           // first page: register fork, pin tips as GC roots
      sql.exec("INSERT OR IGNORE INTO forks VALUES (?,?)", fork, Date.now());
      for (const r of refs) sql.exec("INSERT OR IGNORE INTO gc_pins VALUES (?,?)", r.sha, fork);
    });
    const rows = sql.exec("SELECT sha,type,size,layer FROM objects WHERE sha > ? ORDER BY sha LIMIT 5000", after).toArray() as Row[];
    const done = rows.length < 5000;
    return Response.json({ refs, rows, cursor: done ? "done" : rows[rows.length - 1].sha, done } satisfies Snapshot);
  }

  // ---- reads: resolve sha -> layer -> R2 key -------------------------------------
  private async readObject(sha: string): Promise<R2ObjectBody | null> {
    const row = this.ctx.storage.sql.exec("SELECT layer FROM objects WHERE sha=?", sha).toArray()[0];
    let layer = row?.layer as string | undefined;
    if (!layer) {                                                          // index still importing: probe parent prefix
      const parent = this.ctx.storage.sql.exec("SELECT v FROM meta WHERE k='parent'").toArray()[0]?.v as string | undefined;
      if (!parent) return null;
      layer = `objects/${parent}`;
    }
    return this.env.BUCKET.get(key(layer, sha));                           // bytes never copied between prefixes
  }
  private async uploadPack(req: Request): Promise<Response> {
    // want/have negotiation (want-have-negotiation) yields `send: string[]`; the pack is streamed as
    // PACK header + per-object zlib entries (undeltified) exactly as a non-fork repo would do.
    const send: string[] = await negotiate(this.ctx.storage.sql, req);
    return streamPack(send.map(sha => () => this.readObject(sha)));
  }
  // ---- writes: always land in the fork's own prefix -------------------------------
  private async receivePack(req: Request): Promise<Response> {
    for await (const o of parsePack(req.body!)) {                          // streaming-pack-parser, deltas resolved via readObject
      await this.env.BUCKET.put(key(this.own, o.sha), o.bytes, { sha1: o.sha });
      this.ctx.storage.sql.exec("INSERT OR REPLACE INTO objects VALUES (?,?,?,?)", o.sha, o.type, o.size, this.own);
    }
    return applyRefUpdates(this.ctx.storage, req);                          // CAS on refs, report-status pkt-lines
  }
}
type Row = { sha: string; type: string; size: number; layer: string };
type Snapshot = { refs: { name: string; sha: string }[]; rows: Row[]; cursor: string; done: boolean };
declare function negotiate(sql: SqlStorage, req: Request): Promise<string[]>;
declare function streamPack(readers: (() => Promise<R2ObjectBody | null>)[]): Response;
declare function parsePack(body: ReadableStream): AsyncIterable<{ sha: string; type: string; size: number; bytes: Uint8Array }>;
declare function applyRefUpdates(storage: DurableObjectStorage, req: Request): Promise<Response>;
```

## Why it works
- Git objects are immutable and content-addressed, so a byte stored at `objects/alice/repo/<sha>` is exactly as valid for `bob/repo` as for `alice/repo`; only the sha->location mapping needs to be per-repo, and that mapping is a SQLite row, not an R2 copy.
- `git-upload-pack` only needs to produce a valid PACK stream for the wanted set; where each object's bytes come from is invisible on the wire, so a pack assembled from two R2 prefixes is indistinguishable from one assembled from one.
- `git-receive-pack` clients compute the objects to send from the advertised refs ("have" lines come from the server's ref advertisement). The fork advertises the parent's ref tips from its own SQLite copy, so a client pushing a branch built on `alice/main` sends only its new commits/trees/blobs -- that is the copy-on-write boundary and it costs zero head-checks.
- Ref-delta (`REF_DELTA`) and `OFS_DELTA` entries in an incoming thin pack resolve their base through `readObject`, which transparently reads the base from the parent prefix; the resolved full object is then written to the fork prefix, so the fork never stores a delta whose base it does not control.
- Fork-of-fork stays O(1) at read time because rows carry the originating layer; a third-generation fork's index says `objects/alice/repo` for the ancestral blobs, not `objects/carol/repo`.
- Parent GC (`gc-and-repack-alarm`) treats `gc_pins` as roots, so the exact set the fork inherited (everything reachable from the parent tips at fork time) can never be deleted underneath it; objects unreachable at fork time are not pinned but a fork also never relies on them for writes (all uploads land in its own prefix).

## Known limits
- The index copy is O(number of objects in the parent): a 2M-object repo means ~400 pages of 5000 rows across alarm ticks and roughly 2M x ~80 bytes = 160MB of SQLite in the fork (DO SQLite limit is 10GB, so it fits, but forking is minutes, not milliseconds, and each fork repeats it). The `readObject` fallback to the parent prefix keeps the fork usable during import; a chain of forks pays the same cost each time.
- Pins are ref tips at fork time, not a live subscription: if the fork later pulls newer upstream commits (`git fetch upstream`), those objects sit in the parent prefix but are not pinned for the fork. Either the fork's receive path must copy upstream-fetched objects into its own prefix (one extra R2 GET+PUT per object, hand-waved here) or the parent must re-pin on every snapshot page -- the proof re-pins only on page 0.
- Deleting or force-rewriting the parent is constrained forever: the parent DO must refuse to delete its R2 prefix while `forks` is non-empty (or run a detach job that copies pinned objects into each fork's prefix, which is the O(objects) R2 copy this idea was avoiding). GC in the parent must union `gc_pins` into its roots -- that is an obligation on `gc-and-repack-alarm`, not something this DO can enforce alone.
- Precomputed clone packs (`precomputed-clone-pack`) are per-prefix; a fresh clone of a fork cannot be a single range read of the parent's pack once the fork has diverged, so fork clones fall back to per-object R2 GETs (Class B ops, one per object) unless the fork builds its own pack.
- Single-DO throughput: every fork read goes through the fork DO for the sha->layer lookup; the parent DO is only hit during import, so parent load is unaffected, but the fork inherits the usual one-DO-per-repo ceiling.
- Cross-account R2 keys are readable by the fork Worker because every repo shares one bucket; per-repo isolation (`auth-and-multitenancy`) is therefore at the DO layer only. A fork of a private repo that is later made public leaks nothing new, but a parent that becomes private cannot revoke bytes the fork index already points at.
- Worker limits: 30s CPU per request is not a concern for the fork call (it returns after page 0); the 128MB DO memory limit is fine since pages are 5000 rows.

## Depends on
- `refs-sqlite-objects-r2`
- `content-addressed-r2-keys`
- `repo-do-ref-authority`
- `gc-and-repack-alarm` (must honor `gc_pins`)
- `streaming-pack-parser`
- `want-have-negotiation`
