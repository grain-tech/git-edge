> Idea #46 · wild · verdict: **lands with caveats** · feasibility 3/5 · reliability 3/5 · correctness 3/5 · effort: weeks
> Proof: [proofs/time-boxed-history.md](./time-boxed-history.md) · Review: [reviews/time-boxed-history.md](../../reviews/archived/time-boxed-history.md)

# Time-boxed history with cold-storage checkpoints

## Mechanism

History is never rewritten (a Merkle DAG cannot be "squashed" without changing every descendant SHA and breaking every existing clone); instead the repo DO imposes a *shallow horizon*. A daily DO alarm reads the commit-graph table in DO SQLite, picks the boundary commits (`committer_time < now-365d` with a child `>= horizon`, or ref tips that are themselves older), builds one self-contained "checkpoint pack" containing everything reachable from the tips down to and including those boundaries, and writes it to hot R2 under `packs/checkpoint-<isoDate>.pack`; every loose object older than the horizon is re-put with `storageClass: "InfrequentAccess"` (or left to an R2 lifecycle transition rule). A protocol-v2 `fetch` with no `have`/`deepen*` args (a fresh clone) is answered by the DO with a `shallow-info` section listing the boundary OIDs followed by a `packfile` section that is a straight R2 stream of the checkpoint pack, so the client sees the boundary commits as root commits — a checkpoint squash from the client's point of view, with identical SHAs. `git fetch --unshallow` / `--deepen` / `--shallow-since=<older>` hits the cold path: the DO walks the graph below the horizon and packs objects fetched from the Infrequent-Access tier on the fly.

## Primitives

- Durable Object with SQLite storage (`ctx.storage.sql`): `commit_graph`, `checkpoints` tables — GA
- DO alarms (`ctx.storage.setAlarm`) for the daily horizon roll — GA
- R2 `put` with `storageClass: "InfrequentAccess"` and R2 lifecycle `storageClassTransitions` — Infrequent Access storage class is still labelled **beta** in Cloudflare docs (pricing model GA-like, but no SLA)
- R2 `get` with `range` for streaming the checkpoint pack — GA
- Workers `Response` streaming + `TransformStream` for sideband pkt-line framing — GA
- `CompressionStream`/`DecompressionStream` (deflate) for on-the-fly cold packs — GA

## Proof code

```typescript
// RepoDO: only the horizon/checkpoint parts. ls-refs, receive-pack, negotiation live elsewhere.
const YEAR = 365 * 86_400;
const pkt = (s: string) => (s.length + 4).toString(16).padStart(4, "0") + s;
const enc = new TextEncoder();

export class RepoDO implements DurableObject {
  constructor(private ctx: DurableObjectState, private env: { BUCKET: R2Bucket }) {
    ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS commit_graph (oid TEXT PRIMARY KEY, parents TEXT NOT NULL, ctime INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS refs (name TEXT PRIMARY KEY, oid TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS checkpoints (id INTEGER PRIMARY KEY, horizon INTEGER, boundary TEXT, pack_key TEXT, size INTEGER);
    `);
    ctx.blockConcurrencyWhile(async () => { if (!(await ctx.storage.getAlarm())) await ctx.storage.setAlarm(Date.now() + 60_000); });
  }

  // Boundary = oldest commits a client still needs to make every tip's tree resolvable; exactly what
  // `git rev-list --shallow-since=<horizon>` would mark as shallow.
  private boundary(horizon: number): string[] {
    const sql = this.ctx.storage.sql;
    const tips = sql.exec<{ oid: string }>("SELECT oid FROM refs").toArray().map(r => r.oid);
    const graph = new Map(sql.exec<{ oid: string; parents: string; ctime: number }>("SELECT * FROM commit_graph")
      .toArray().map(r => [r.oid, r]));
    const out = new Set<string>(), seen = new Set<string>(), stack = [...tips];
    while (stack.length) {
      const oid = stack.pop()!; if (seen.has(oid)) continue; seen.add(oid);
      const c = graph.get(oid)!;
      if (c.ctime < horizon) { out.add(oid); continue; }       // old commit reached from a new one (or old tip)
      stack.push(...JSON.parse(c.parents));
    }
    return [...out];
  }

  async alarm() {
    const horizon = Math.floor(Date.now() / 1000) - YEAR;
    const boundary = this.boundary(horizon);
    // Pack every object reachable from tips, stopping at the boundary commits' trees (included in full,
    // no deltas against anything below the horizon, so the pack is not "thin").
    const pack = await buildPack(this.env.BUCKET, this.ctx.storage.sql, { stopAt: boundary });   // PACK v2 header, ofs-delta only
    const key = `packs/checkpoint-${new Date().toISOString().slice(0, 10)}.pack`;
    const obj = await this.env.BUCKET.put(key, pack);
    this.ctx.storage.sql.exec("INSERT INTO checkpoints (horizon, boundary, pack_key, size) VALUES (?,?,?,?)",
      horizon, JSON.stringify(boundary), key, obj.size);
    // Demote everything strictly below the horizon to the cold tier (idempotent; content-addressed keys).
    for (const { oid } of this.ctx.storage.sql.exec<{ oid: string }>("SELECT oid FROM commit_graph WHERE ctime < ?", horizon)) {
      for (const k of await objectKeysOf(this.env.BUCKET, oid)) {              // commit, its tree, blobs not in hot set
        const o = await this.env.BUCKET.get(k); if (o) await this.env.BUCKET.put(k, o.body, { storageClass: "InfrequentAccess" });
      }
    }
    await this.ctx.storage.setAlarm(Date.now() + 86_400_000);
  }

  // Protocol v2 `command=fetch` (already de-framed into args by the pkt-line codec).
  async fetch(req: Request): Promise<Response> {
    const args = parseFetchArgs(await req.text()); // {want[], have[], deepen?, deepenSince?, done}
    const cp = this.ctx.storage.sql.exec<{ boundary: string; pack_key: string; size: number }>(
      "SELECT boundary, pack_key, size FROM checkpoints ORDER BY id DESC LIMIT 1").toArray()[0];
    const freshClone = args.have.length === 0 && !args.deepen && !args.deepenSince && args.done;

    if (cp && freshClone) {                                            // HOT PATH: R2 stream, no object walk
      const obj = await this.env.BUCKET.get(cp.pack_key, { range: { offset: 0, length: cp.size } });
      const head = pkt("shallow-info\n") + JSON.parse(cp.boundary).map((o: string) => pkt(`shallow ${o}\n`)).join("")
        + "0001" + pkt("packfile\n");
      return new Response(concat(enc.encode(head), obj!.body.pipeThrough(sideband(1)), enc.encode("0000")),
        { headers: { "content-type": "application/x-git-upload-pack-result" } });
    }
    // COLD PATH: deepen / unshallow / shallow-since older than horizon → walk below the boundary,
    // fetch each object from R2 (Infrequent Access reads bill per GET), pack on the fly.
    const stream = streamPack(this.env.BUCKET, this.ctx.storage.sql, args);   // pkt('packfile') + sideband(1) chunks
    return new Response(stream, { headers: { "content-type": "application/x-git-upload-pack-result" } });
  }
}

// Sideband framing for the v2 packfile section: each pkt-line = 4-hex len + band byte + <=65515 data bytes.
function sideband(band: number) {
  return new TransformStream<Uint8Array, Uint8Array>({
    transform(chunk, ctl) {
      for (let i = 0; i < chunk.length; i += 65515) {
        const d = chunk.subarray(i, i + 65515);
        ctl.enqueue(enc.encode((d.length + 5).toString(16).padStart(4, "0") + String.fromCharCode(band)));
        ctl.enqueue(d);
      }
    },
  });
}
```

## Why it works

- Git already has the exact wire concept: a shallow clone's `.git/shallow` file lists commits treated as parentless. The v2 `fetch` response's `shallow-info` section (`shallow <oid>` lines, then `0001` delimiter, then `packfile`) is what `git clone --shallow-since` receives; `fetch-pack.c` peeks for `shallow-info` before the pack unconditionally, so the same bytes work when the server chooses the horizon.
- Boundary commits keep their real SHAs and full trees, so the client's checkout, `git log` (ending at the graft) and later incremental fetches (`have <tip>`) are all normal. No rewritten history, no force-push fallout.
- The checkpoint pack is self-contained (no ref-delta/ofs-delta whose base is below the horizon), which is the same rule `pack-objects` applies for shallow clones; `git index-pack` therefore accepts it without `--fix-thin`.
- Fresh clones — the bulk of read traffic — become one R2 GET plus a sideband re-framing pass; the DO never inflates objects for them. Object inflation only happens on the cold path.
- `deepen`, `deepen-since`, `deepen-not` and `--unshallow` are already protocol-v2 arguments, so "reach into cold storage" needs no client change; the server just walks below `checkpoints.boundary` and serves `unshallow <oid>` lines as git specifies.
- Tiering is a storage-class flag on content-addressed keys, so re-putting an old object is idempotent and a crash mid-demotion leaves nothing inconsistent.

## Known limits

- **The idea as stated ("squash commits older than a year") is not implementable without breaking every clone**: commit SHAs depend on parent SHAs. What is delivered is a server-imposed shallow horizon, which looks like a squash to clients and preserves all objects. Say so in any product copy.
- Server-imposed `shallow` lines for a client that did not ask for depth are tolerated by git's v2 client parser but not promised by the protocol spec; a client with `fetch.negotiationAlgorithm` quirks or a non-git implementation (JGit, libgit2, gitoxide) may reject or ignore them. Fallback: those clients get the cold path (full history), which must still work.
- Pushes from a shallow clone are fine (they only reference reachable objects), but `git rebase`/`git bisect` past the horizon on the client silently fail unless the user runs `--unshallow`; documentation burden.
- Cold path cost: R2 Infrequent Access charges a retrieval fee per byte and a minimum 30-day storage duration; an `--unshallow` of a 10-year repo means one GET per object unless a cold *pack* is also kept (recommended: write `packs/cold-<date>.pack` in the same alarm; hand-waved here).
- `buildPack` in the alarm must inflate/deflate every hot object; for a large repo that exceeds a single alarm's CPU budget (30 s wall on the free tier, up to 15 min with `limits.cpu_ms` on paid) and DO memory (128 MB). Chunk it: one alarm per N commits, resumable via a `progress` row — same shape as `gc-and-repack-alarm`.
- The `boundary()` walk loads the whole commit graph into memory; for >~1M commits switch to an SQL recursive CTE over `commit_graph` and keep the alarm incremental.
- One DO per repo serializes checkpoint building with pushes; a push that lands mid-alarm is safe (refs are CAS'd separately) but the checkpoint may be one commit stale, which the next incremental fetch covers.
- Range reads of a pack larger than ~5 GB should be split into several R2 range GETs concatenated into the response; single `get` is fine below that.

## Depends on

- `want-have-negotiation` (commit_graph table with committer timestamps)
- `precomputed-clone-pack` (the checkpoint pack *is* the precomputed clone pack, with a horizon)
- `gc-and-repack-alarm` (same alarm-driven pack builder, chunked)
- `refs-sqlite-objects-r2`, `content-addressed-r2-keys`, `storage-tiering`
