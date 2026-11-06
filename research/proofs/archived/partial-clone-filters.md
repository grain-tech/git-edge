> Idea #10 · foundation · verdict: **risky** · feasibility 3/5 · reliability 4/5 · correctness 3/5 · effort: weeks
> Proof: [proofs/partial-clone-filters.md](./partial-clone-filters.md) · Review: [reviews/partial-clone-filters.md](../../reviews/archived/partial-clone-filters.md)

# Shallow and partial clone as first-class filters

## Mechanism
`POST /:owner/:repo/git-upload-pack` with `command=fetch` is parsed by the Worker (`protocol-v2-only`) into `{wants, haves, filter, deepen, shallow[]}` and sent as one RPC to the repo DO (`idFromName("owner/repo")`). The DO never touches R2: it plans the whole response from three SQLite tables the push parser fills at receive time -- `objects(sha,type,size)`, `commits(sha,tree,parents)` and a new `tree_entries(tree,child,kind)` edge table -- using recursive CTEs that apply `deepen N` as a depth cap on the commit walk and `blob:none` / `blob:limit=N` / `tree:N` as a `WHERE` clause on the tree walk, and returns `{shallow[], unshallow[], objs[{sha,type,size}]}`. The Worker then streams the v2 response (`shallow-info` section, then `packfile` section on side-band-64k) and, because it already knows every object's type and size, each object is exactly one `env.BUCKET.get(key, { range: { offset: looseHeaderLen } })` piped through `CompressionStream("deflate")` into a non-delta PACK. A later promisor fetch from the client (`want <blob-sha>` + `done`, which git sends for every missing blob on checkout/diff) goes through the same path with an empty walk, so it costs one SQLite lookup and one R2 range GET per blob.

## Primitives
- Durable Object with SQLite storage: `ctx.storage.sql.exec` with recursive CTEs and `json_each` over the object/commit/tree-edge tables -- GA
- DO RPC (`stub.plan(req)` typed method) -- GA
- R2 `get(key, { range: { offset } })` returning a `ReadableStream` body -- GA
- Workers streaming `Response` over `TransformStream`, `CompressionStream("deflate")` (zlib framing, which is what PACK entries require) -- GA
- Web Crypto has no incremental SHA-1, so the pack trailer uses a ~60-line pure-JS streaming SHA-1 (`Sha1` below) -- plain JS, nothing beta
- No beta primitives required.

## Proof code
```typescript
// Tables written at push time by the pack parser (streaming-pack-parser):
//   objects(sha TEXT PK, type INT /*1 commit,2 tree,3 blob,4 tag*/, size INT)
//   commits(sha TEXT PK, tree TEXT, parents TEXT /*JSON array*/)      -- want-have-negotiation
//   tree_entries(tree TEXT, child TEXT, kind TEXT /*'tree'|'blob'*/, PRIMARY KEY(tree, child))  -- this idea
type Filter = { kind: "none" } | { kind: "blob:none" } | { kind: "blob:limit"; n: number } | { kind: "tree"; depth: number };
type FetchReq = { wants: string[]; haves: string[]; clientShallow: string[]; deepen?: number; filter: Filter };
type Obj = { sha: string; type: number; size: number };

export class RepoDO extends DurableObject<Env> {
  /** Plan a filtered/shallow fetch entirely in SQLite. Zero R2 reads. */
  plan(r: FetchReq): { shallow: string[]; unshallow: string[]; objs: Obj[] } {
    const sql = this.ctx.storage.sql, J = JSON.stringify;
    const known = sql.exec("SELECT sha, type FROM objects WHERE sha IN (SELECT value FROM json_each(?))", J(r.wants)).toArray() as { sha: string; type: number }[];
    if (known.length !== r.wants.length) throw new Error("upload-pack: not our ref");   // v2: any oid may be wanted, but it must exist in THIS repo
    const byType = (t: number) => known.filter(k => k.type === t).map(k => k.sha);
    const depth = r.deepen ?? 1e9;                                                        // deepen N => commit depths 0..N-1
    const commits = sql.exec(
      `WITH RECURSIVE c(sha, d) AS (
         SELECT value, 0 FROM json_each(?1)
         UNION SELECT p.value, c.d + 1 FROM c JOIN commits k ON k.sha = c.sha, json_each(k.parents) p WHERE c.d + 1 < ?2)
       SELECT c.sha, MIN(c.d) AS d, json_array_length(k.parents) AS np FROM c JOIN commits k ON k.sha = c.sha GROUP BY c.sha`,
      J(byType(1)), depth).toArray() as { sha: string; d: number; np: number }[];
    const shallow = r.deepen ? commits.filter(c => c.d === depth - 1 && c.np > 0).map(c => c.sha) : [];
    const inSet = new Set(commits.map(c => c.sha));
    const unshallow = r.clientShallow.filter(s => inSet.has(s) && !shallow.includes(s));   // client's old boundary now has parents
    const f = r.filter, treeDepth = f.kind === "tree" ? f.depth : 1e9;                     // tree:N drops everything at depth >= N
    const blobPred = f.kind === "none" ? "1" : f.kind === "blob:limit" ? `o.size < ${f.n | 0}` : "0";
    const objs = sql.exec(
      `WITH RECURSIVE t(sha, d) AS (
         SELECT k.tree, 0 FROM commits k WHERE k.sha IN (SELECT value FROM json_each(?1))
         UNION SELECT value, 0 FROM json_each(?2)
         UNION SELECT e.child, t.d + 1 FROM t JOIN tree_entries e ON e.tree = t.sha WHERE e.kind = 'tree' AND t.d + 1 < ?3)
       SELECT o.sha, o.type, o.size FROM objects o WHERE o.sha IN (SELECT value FROM json_each(?1))
       UNION SELECT o.sha, o.type, o.size FROM t JOIN objects o ON o.sha = t.sha WHERE t.d < ?3
       UNION SELECT o.sha, o.type, o.size FROM t JOIN tree_entries e ON e.tree = t.sha JOIN objects o ON o.sha = e.child
             WHERE e.kind = 'blob' AND t.d + 1 < ?3 AND ${blobPred}
       UNION SELECT o.sha, o.type, o.size FROM objects o WHERE o.sha IN (SELECT value FROM json_each(?4))`,   // wanted blobs: filter never applies
      J(commits.map(c => c.sha)), J(byType(2)), treeDepth, J(byType(3))).toArray() as Obj[];
    return { shallow, unshallow, objs };   // haves-subtraction is want-have-negotiation's job; omitted here
  }
}

// ---- Worker side: stream the v2 fetch response. pkt/FLUSH/DELIM from protocol-v2-only, Sha1 = streaming SHA-1 ----
const TYPE_NAME = ["", "commit", "tree", "blob", "tag"];
const objKey = (owner: string, repo: string, sha: string) => `objects/${owner}/${repo}/${sha.slice(0, 2)}/${sha.slice(2)}`;
const looseHeaderLen = (o: Obj) => TYPE_NAME[o.type].length + 1 + String(o.size).length + 1;   // "blob 1234\0"
function entryHeader(type: number, size: number): Uint8Array {          // PACK entry: 3-bit type, 4-bit size, 7-bit continuation bytes
  const out = [(type << 4) | (size & 15)]; size = Math.floor(size / 16);
  while (size > 0) { out[out.length - 1] |= 0x80; out.push(size & 127); size = Math.floor(size / 128); }
  return new Uint8Array(out);
}

export async function fetchResponse(env: Env, owner: string, repo: string, req: FetchReq): Promise<Response> {
  const stub = env.REPO.get(env.REPO.idFromName(`${owner}/${repo}`));
  const { shallow, unshallow, objs } = await stub.plan(req);
  const { readable, writable } = new TransformStream<Uint8Array>();
  const w = writable.getWriter(), sha = new Sha1();
  const band1 = async (b: Uint8Array) => {                                // side-band-64k, band 1 = pack data, <= 65515 bytes per pkt
    sha.update(b);
    for (let i = 0; i < b.length; i += 65515) await w.write(pkt(new Uint8Array([1, ...b.subarray(i, i + 65515)])));
  };
  (async () => {
    if (req.deepen || shallow.length || unshallow.length) {
      await w.write(pkt("shallow-info\n"));
      for (const s of shallow) await w.write(pkt(`shallow ${s}\n`));
      for (const s of unshallow) await w.write(pkt(`unshallow ${s}\n`));
      await w.write(DELIM);
    }
    await w.write(pkt("packfile\n"));                                     // client sent `done`: no acknowledgments section
    const hdr = new Uint8Array(12), dv = new DataView(hdr.buffer);
    hdr.set(new TextEncoder().encode("PACK")); dv.setUint32(4, 2); dv.setUint32(8, objs.length);   // count known up front from the plan
    await band1(hdr);
    for (const o of objs) {                                               // exactly ONE R2 GET per object, header skipped by range
      const r2 = await env.BUCKET.get(objKey(owner, repo, o.sha), { range: { offset: looseHeaderLen(o) } });
      if (!r2) throw new Error(`missing ${o.sha}`);
      await band1(entryHeader(o.type, o.size));
      for await (const z of r2.body.pipeThrough(new CompressionStream("deflate"))) await band1(z);   // zlib stream, as PACK requires
    }
    await w.write(pkt(new Uint8Array([1, ...sha.digest()])));             // 20-byte SHA-1 pack trailer
    await w.write(FLUSH); await w.close();
  })().catch(e => w.abort(e));
  return new Response(readable, { headers: { "content-type": "application/x-git-upload-pack-result", "cache-control": "no-cache" } });
}
```

## Why it works
- git protocol v2 says wants "can be anything and are not limited to advertised objects", which is exactly what the promisor machinery relies on: after a `blob:none` clone the client runs `git fetch --filter=blob:none --stdin` with bare blob oids, `fetch.negotiationAlgorithm=noop` and `done`. Our `plan` treats wanted blobs as terminal (no walk, filter not applied, per git's "explicitly wanted objects are always sent"), so the response is header + one entry + trailer, and the only I/O is one SQLite existence check and one R2 range GET.
- The capability line must be `fetch=shallow filter` (from `protocol-v2-only`'s advertisement) or the client refuses with "filtering not recognized by server"; nothing else is needed server-side because `uploadpack.allowFilter` is simply always on here.
- git-index-pack requires the object count in the 12-byte `PACK` header to be exact before it sees the first entry. Planning in SQLite (commit closure + `tree_entries` closure) yields the list, hence the count, before any R2 byte is read; a walk that discovered objects by reading trees from R2 would have to buffer or two-pass.
- Objects are stored as git's loose form `"<type> <size>\0" + content` (`content-addressed-r2-keys`), so type and size from the index give the header length and `range.offset` starts the R2 stream at byte 0 of the content; `CompressionStream("deflate")` emits a zlib stream, which is the framing every PACK entry uses. Non-delta packs (types 1-4 only) are always valid, so the client's `ofs-delta` capability is honoured by simply never emitting deltas.
- Shallow semantics follow upload-pack: `deepen N` caps the commit BFS at depth N-1, boundary commits that still have parents are reported as `shallow`, and any sha the client listed in its `shallow` lines that is now inside the set with parents is reported as `unshallow`; commit objects are sent unmodified because the client's `.git/shallow` file, not the object, records the cut.
- `tree:0` sends commits only; the client's follow-up `want <root-tree>` (again with `--filter=blob:none`) seeds the tree CTE from the wanted tree, so it receives that tree closure minus blobs in one round trip, matching what git's own upload-pack produces for the same request.

## Known limits
- Workers allow 1,000 subrequests per invocation and every `env.BUCKET.get` counts, so one fetch can carry at most ~1,000 objects on this path. That covers every lazy promisor batch git normally makes and the incremental fetches, but NOT a first `blob:none` clone of a repo with >1,000 commits+trees; that case must be served from a prebuilt blobless pack slice (`precomputed-clone-pack`, R2 range stream, 1 subrequest) or by fanning wants out in chunks of ~500 to a self service-binding that returns raw pack entries the outer Worker stitches and hashes. The fan-out is hand-waved above.
- Sequential `get` + deflate is ~10-30 ms per object; 1,000 small objects is 10-30 s of wall time (not CPU: deflate of trees/commits is tiny, so the 30 s CPU limit is not the constraint, but a prefetch window of ~16 in-flight GETs is needed for acceptable latency).
- `tree_entries` is one row per unique (tree, child) pair; for a very large monorepo that is millions of rows in DO SQLite (10 GB cap, fine) and the recursive CTE over the full closure can take seconds of DO CPU. The `objs` array returned by RPC must be paged (e.g. materialised into a temp table and read in 10k-row pages) for repos beyond ~100k objects; the proof returns it in one array.
- `haves` are ignored in the proof; subtracting the closure of haves is `want-have-negotiation`'s CTE and composes with the same depth/filter clauses.
- Not implemented: `deepen-since`, `deepen-not`, `deepen-relative`, `combine:` filters, `sparse:oid`, annotated-tag peeling of wants, and `blob:limit` with `k`/`m` suffixes (parse to bytes in the Worker).
- Each lazy blob is a Class B R2 operation (~$0.36 per million); a `git checkout` on a blobless clone of a 50k-file tree is 50k GETs, i.e. cents, but `git log -p` style workloads that touch every historical blob multiply that. `in-do-object-cache` can absorb the hot lockfile/config blobs.
- Web Crypto cannot hash incrementally; the pure-JS `Sha1` class is assumed, not shown.

## Depends on
- content-addressed-r2-keys
- refs-sqlite-objects-r2
- protocol-v2-only
- streaming-pack-parser
- want-have-negotiation
- precomputed-clone-pack (for initial clones that exceed the subrequest budget)
