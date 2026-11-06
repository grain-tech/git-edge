> Idea #56 · edge · verdict: **lands with caveats** · feasibility 4/5 · reliability 3/5 · correctness 3/5 · effort: weeks
> Proof: [proofs/want-have-negotiation.md](./want-have-negotiation.md) · Review: [reviews/want-have-negotiation.md](../../reviews/archived/want-have-negotiation.md)

# Want/have negotiation with a commit-graph in SQLite

## Mechanism
`POST /:owner/:repo/git-upload-pack` with a v2 `command=fetch` body (`want <oid>`, `have <oid>`..., optionally `done`) is forwarded by the stateless Worker to the repo DO (`env.REPO.idFromName("owner/repo")`). The DO keeps the same data git itself keeps in `.git/objects/info/commit-graph` — `commits(oid, gen, tree)` and `parents(oid, parent)` — plus one extra table `introduced(commit, oid)` = "objects reachable from this commit's tree that are not reachable from any parent's tree"; all three are filled at push time by the pack parser (`streaming-pack-parser`), which already inflates every commit and tree. On fetch the DO ACKs each `have` that is a row in `commits`, runs git's own two-colour walk (pop by generation number descending, haves paint their ancestors UNINTERESTING, stop when the frontier is all uninteresting) with `ctx.storage.sql` row lookups instead of R2 GETs, and the send set is one `SELECT DISTINCT oid FROM introduced WHERE commit IN (interesting)`. Only after the set is known does the Worker touch R2, once per object in the set, to stream the `packfile` section; no commit, tree or blob is read from R2 to *decide* what to send. Because v2 clients resend every `have` on every round, each POST is self-contained and the DO stores nothing between rounds.

## Primitives
- Durable Object with SQLite storage (`ctx.storage.sql.exec`, `transactionSync`) — GA
- Workers `fetch` with streaming `Response` body (`ReadableStream` / `TransformStream`) — GA
- R2 `env.BUCKET.get("objects/<owner>/<repo>/<sha>")` per object in the send set (bodies only, never for negotiation) — GA
- `nodejs_compat` `node:zlib` `deflateSync` for pack entries and `node:crypto` SHA-1 for the trailing pack checksum — GA
- No alarms, KV, Queues or beta features needed for this idea

## Proof code
```typescript
import { DurableObject } from "cloudflare:workers";
import { deflateSync } from "node:zlib";
import { createHash } from "node:crypto";

type Env = { BUCKET: R2Bucket; REPO: DurableObjectNamespace };
const enc = new TextEncoder();
const pkt = (s: string) => enc.encode((s.length + 4).toString(16).padStart(4, "0") + s);

export class RepoDO extends DurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS commits   (oid TEXT PRIMARY KEY, gen INTEGER NOT NULL, tree TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS parents   (oid TEXT NOT NULL, parent TEXT NOT NULL, idx INTEGER NOT NULL, PRIMARY KEY(oid, idx));
      CREATE TABLE IF NOT EXISTS introduced(commit TEXT NOT NULL, oid TEXT NOT NULL, PRIMARY KEY(commit, oid));
      CREATE INDEX IF NOT EXISTS parents_by_parent ON parents(parent);`);
  }

  /** Push side (called by streaming-pack-parser after objects are in R2): gen = 1 + max(parent gen), like commit-graph. */
  recordCommit(oid: string, tree: string, parents: string[], newObjs: string[]) {
    const sql = this.ctx.storage.sql;
    this.ctx.storage.transactionSync(() => {
      const pg = parents.map(p => sql.exec<{ gen: number }>("SELECT gen FROM commits WHERE oid=?", p).one().gen);
      sql.exec("INSERT OR IGNORE INTO commits VALUES (?,?,?)", oid, 1 + Math.max(0, ...pg), tree);
      parents.forEach((p, i) => sql.exec("INSERT OR IGNORE INTO parents VALUES (?,?,?)", oid, p, i));
      for (const o of [oid, ...newObjs]) sql.exec("INSERT OR IGNORE INTO introduced VALUES (?,?)", oid, o);
    });
  }

  /** Fetch side: git's revision walk, but every parent lookup is a local SQLite row, not an R2 GET. */
  negotiate(wants: string[], haves: string[]) {
    const sql = this.ctx.storage.sql;
    const gen = (o: string) => sql.exec<{ gen: number }>("SELECT gen FROM commits WHERE oid=?", o).toArray()[0]?.gen;
    const acks = haves.filter(h => gen(h) !== undefined);          // ACK only commits we actually have
    const color = new Map<string, boolean>();                       // true = UNINTERESTING (reachable from a have)
    const queue: { oid: string; gen: number }[] = [];
    const push = (oid: string, unint: boolean) => {
      const g = gen(oid); if (g === undefined) return;               // unknown want -> caller answers ERR
      if (color.get(oid) === true) return;                          // once uninteresting, stays uninteresting
      if (!color.has(oid)) queue.push({ oid, gen: g });
      color.set(oid, unint || color.get(oid) === true);
    };
    wants.forEach(w => push(w, false)); acks.forEach(h => push(h, true));
    const interesting: string[] = [];
    while (queue.some(q => !color.get(q.oid))) {                    // git: stop when everybody_uninteresting()
      queue.sort((a, b) => b.gen - a.gen);                          // strict gen order => flag is final when popped
      const { oid } = queue.shift()!;
      const unint = color.get(oid)!;
      if (!unint) interesting.push(oid);
      for (const r of sql.exec<{ parent: string }>("SELECT parent FROM parents WHERE oid=? ORDER BY idx", oid)) push(r.parent, unint);
    }
    const marks = interesting.map(() => "?").join(",");
    const objs = interesting.length
      ? sql.exec<{ oid: string }>(`SELECT DISTINCT oid FROM introduced WHERE commit IN (${marks})`, ...interesting).toArray().map(r => r.oid)
      : [];
    // git upload-pack's ok_to_give_up(): every want must reach some common commit before we say "ready"
    const ready = haves.length === 0 || wants.every(w => color.get(w) === false && interesting.length > 0 && acks.length > 0);
    return { acks, ready, objs };
  }
}

/** Worker: one v2 fetch round. Body already pkt-line-decoded by info-refs-endpoint's codec. */
export async function fetchRound(env: Env, repo: string, args: string[]): Promise<Response> {
  const wants = args.filter(a => a.startsWith("want ")).map(a => a.slice(5));
  const haves = args.filter(a => a.startsWith("have ")).map(a => a.slice(5));
  const done = args.includes("done");
  const stub = env.REPO.get(env.REPO.idFromName(repo));
  const { acks, ready, objs } = await stub.negotiate(wants, haves);   // DO RPC, no object bytes cross this hop
  const { readable, writable } = new TransformStream<Uint8Array>();
  const w = writable.getWriter();
  (async () => {
    if (!done) {                                                     // acknowledgments section
      await w.write(pkt("acknowledgments\n"));
      for (const a of acks.length ? acks.map(a => `ACK ${a}\n`) : ["NAK\n"]) await w.write(pkt(a));
      if (ready) await w.write(pkt("ready\n"));
      if (!ready) { await w.write(enc.encode("0000")); await w.close(); return; }   // client sends more haves
      await w.write(enc.encode("0001"));                              // delim-pkt between sections
    }
    await w.write(pkt("packfile\n"));
    const sha = createHash("sha1");
    const emit = async (b: Uint8Array) => {                        // sideband-64k: each frame = pkt-line of "\x01" + <=65519 bytes
      sha.update(b);
      for (let i = 0; i < b.length; i += 65519) { const c = b.subarray(i, i + 65519);
        await w.write(enc.encode((c.length + 5).toString(16).padStart(4, "0") + "\x01")); await w.write(c); }
    };
    const hdr = new Uint8Array(12); hdr.set(enc.encode("PACK")); new DataView(hdr.buffer).setUint32(4, 2); new DataView(hdr.buffer).setUint32(8, objs.length);
    await emit(hdr);
    for (const oid of objs) {                                         // the only R2 traffic: bodies of the send set
      const obj = await env.BUCKET.get(`objects/${repo}/${oid}`); if (!obj) throw new Error(`missing ${oid}`);
      const raw = new Uint8Array(await obj.arrayBuffer());
      const nul = raw.indexOf(0); const [type, size] = new TextDecoder().decode(raw.subarray(0, nul)).split(" ");
      await emit(packEntryHeader({ commit: 1, tree: 2, blob: 3, tag: 4 }[type]!, Number(size)));   // OBJ_* varint header
      await emit(deflateSync(raw.subarray(nul + 1)));                 // full entry; pinned-delta-bases swaps in OBJ_REF_DELTA
    }
    await emit(sha.digest()); await w.write(enc.encode("0000")); await w.close();
  })();
  return new Response(readable, { headers: { "content-type": "application/x-git-upload-pack-result" } });
}

function packEntryHeader(type: number, size: number): Uint8Array {  // git pack varint: type in bits 4-6 of byte 0
  const out = [ (type << 4) | (size & 0x0f) ]; size >>= 4;
  while (size > 0) { out[out.length - 1] |= 0x80; out.push(size & 0x7f); size >>= 7; }
  return Uint8Array.from(out);
}
```

## Why it works
- The tables are exactly git's own commit-graph (`oid`, generation number, parents, root tree); git's `upload-pack` on a real server already uses the commit-graph file for `ok_to_give_up` / `can_all_from_reach` so it does not have to parse commit objects during negotiation. Putting that graph in DO SQLite gives the same property on Cloudflare, where "parsing a commit" would otherwise be a ~20-50 ms R2 round trip per step instead of a ~10 us row read.
- Popping in strictly descending generation number is what makes the two-colour walk correct in one pass: a parent always has a smaller generation than every child, so all children (and therefore the parent's final INTERESTING/UNINTERESTING flag) are settled before the parent is popped. This is the same argument git relies on when it prefers generation numbers over commit dates in `revision.c`.
- `introduced(commit, oid)` makes the object set a single set-union instead of a tree walk. Correctness: any object reachable from a want is first "introduced" by some ancestor commit C of that want; if C is interesting it is sent, and if C is uninteresting the client has C and therefore (git's invariant that a `have` implies the full closure) already has the object. Extra duplicates can be sent when the same blob is introduced on two branches, which `git index-pack` accepts silently — git's own pack-objects has the same boundary imprecision.
- Protocol v2 `fetch` is stateless by design: the client resends all `have`s (including previously ACKed ones) on every round, so the DO can compute `ACK`/`ready` from the request alone, and the Worker emits the sections git expects in order: `acknowledgments` (`NAK` or `ACK <oid>`... then `ready`), delim-pkt `0001`, `packfile`, flush `0000`. When the client sent `done` the acknowledgments section is omitted entirely, which is what `git fetch-pack` requires.
- A `have` is ACKed only if it is a row in `commits`; git sends `have` only for commits and the client ignores ACKs for anything it did not ask about, so a stale ACK list cannot desynchronise the negotiation.
- The pack is an ordinary v2 pack (12-byte `PACK` header, per-entry varint type/size header, zlib body, trailing SHA-1); the negotiation layer is independent of whether entries are full objects (here), `OBJ_REF_DELTA` against pinned bases (`pinned-delta-bases`), or a range read of a precomputed pack (`precomputed-clone-pack`).

## Known limits
- `introduced` must be computed at push time by diffing each new commit's tree against its parents' trees. The pushing pack usually contains the new trees, but parent trees must come from R2 (or the DO object cache); a large push of a wide tree can cost hundreds of R2 GETs and real CPU in the receive path. This proof hand-waves that diff (`newObjs` is passed in already computed).
- The `introduced` table has roughly one row per object in the repo (about 60-80 bytes each including indexes). A 5M-object monorepo is ~400 MB of SQLite in one DO — within the 10 GB storage cap but large; `branch-level-dos` would be needed to shard. `commits`/`parents` alone are small (a 1M-commit history is ~150 MB).
- The walk runs in JS inside the DO with one `sql.exec` per parent lookup and a sort per pop; it is fine for tens of thousands of interesting commits but a fetch whose interesting set is the whole history of a 1M-commit repo will hit the 30 s CPU limit and the 128 MB DO isolate limit (the `interesting` array and `color` map). A fresh clone (no haves) should bypass this path via `precomputed-clone-pack`; the `IN (...)` list also needs batching beyond SQLite's 32k bound-parameter limit.
- `ready` is a simplification of git's `ok_to_give_up`; it says ready as soon as every want has been separated from at least one acked have, which can produce a bigger pack than git would after more rounds. Git tolerates this (it is allowed to send a superset).
- Tags (a `want` of an annotated tag needs a `tags(oid, target)` row so the walk starts at the peeled commit), `deepen`/shallow, `filter` (partial clone) and `ref-in-want` are elided.
- Pack building still costs one R2 GET per object in the send set (Class B operation each) plus one `deflateSync` per object; there is no delta compression in this proof so the pack is larger than git's. All of that runs in the Worker per fetch, so a single hot repo is bounded by the single-DO throughput of the negotiate RPC (fast) and by R2 fan-out in the Worker (the real bottleneck).
- The commit graph is only as good as the push path that fills it; an object that arrives outside `recordCommit` (e.g. `presigned-direct-upload`) must still be registered or it will never be sent.

## Depends on
- repo-do-ref-authority
- refs-sqlite-objects-r2
- streaming-pack-parser
- content-addressed-r2-keys
- protocol-v2-only
- info-refs-endpoint
