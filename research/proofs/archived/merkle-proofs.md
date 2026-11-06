> Idea #39 · wild · verdict: **lands with caveats** · feasibility 4/5 · reliability 5/5 · correctness 3/5 · effort: weeks
> Proof: [proofs/merkle-proofs.md](./merkle-proofs.md) · Review: [reviews/merkle-proofs.md](../../reviews/archived/merkle-proofs.md)

# Merkle inclusion proofs on fetch

## Mechanism
Git is already a Merkle DAG, so the proof is not a new hash structure: it is the chain of git objects from a commit the client already trusts down to one blob (commit -> root tree -> subtree ... -> blob), each of which the client re-hashes with git's own `"<type> <size>\0<body>"` rule. The client sends a protocol-v2 command `proof` (`commit=<oid> path=src/a.rb`) to `POST /owner/repo/git-upload-pack`; the Worker routes it to the repo DO (`idFromName(owner/repo)`), which walks the path by reading each object from R2 (`objects/<sha>`, zlib-inflated with `DecompressionStream("deflate")`), memoises `(commit,path) -> oid` in DO SQLite, and streams the objects back as pkt-lines. Nothing new is written to R2; the only write is the SQLite memo row.

## Primitives
- Durable Objects with SQLite storage (`ctx.storage.sql.exec`) - GA
- R2 `env.BUCKET.get` (whole-object; range reads once objects live in packs) - GA
- Workers `DecompressionStream("deflate")` (zlib, what loose git objects use) - GA
- `crypto.subtle.digest("SHA-1")` for verification on both ends - GA
- Streaming `Response` body (`TransformStream`) for the pkt-line reply - GA

## Proof code
```typescript
// Server side: repo DO. Objects are loose-format, zlib-compressed, keyed objects/<sha>
// (content-addressed-r2-keys). Refs and the memo table live in DO SQLite.
export class RepoDO implements DurableObject {
  constructor(private ctx: DurableObjectState, private env: { BUCKET: R2Bucket }) {
    ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS path_memo (
      commit_oid TEXT, path TEXT, oid TEXT, mode TEXT, PRIMARY KEY(commit_oid, path))`);
  }

  // protocol v2:  command=proof\n  commit=<oid>\n  path=<p>\n  0000
  async proof(commit: string, path: string): Promise<Response> {
    const { readable, writable } = new TransformStream<Uint8Array>();
    const w = writable.getWriter();
    (async () => {
      let obj = await this.readObject(commit);          // { type:'commit', body }
      await w.write(pkt(`${commit} commit ${obj.body.length}\n`)); await w.write(pkt(obj.body));
      let treeOid = /^tree ([0-9a-f]{40})/.exec(new TextDecoder().decode(obj.body))![1];
      for (const seg of path.split("/")) {              // every tree on the path is part of the proof
        obj = await this.readObject(treeOid);
        await w.write(pkt(`${treeOid} tree ${obj.body.length}\n`)); await w.write(pkt(obj.body));
        const ent = parseTree(obj.body).find(e => e.name === seg);
        if (!ent) { await w.write(pkt("ERR path not found\n")); break; }
        treeOid = ent.oid;                                // last iteration: this is the blob oid
      }
      obj = await this.readObject(treeOid);
      await w.write(pkt(`${treeOid} blob ${obj.body.length}\n`)); await w.write(pkt(obj.body));
      this.ctx.storage.sql.exec("INSERT OR REPLACE INTO path_memo VALUES (?,?,?,?)", commit, path, treeOid, "100644");
      await w.write(new Uint8Array([0x30,0x30,0x30,0x30]));   // 0000 flush-pkt
      await w.close();
    })().catch(e => w.abort(e));
    return new Response(readable, { headers: { "content-type": "application/x-git-upload-pack-result" } });
  }

  private async readObject(oid: string): Promise<{ type: string; body: Uint8Array }> {
    const r2 = await this.env.BUCKET.get(`objects/${oid}`);
    if (!r2) throw new Error(`missing ${oid}`);
    const raw = new Uint8Array(await new Response(
      r2.body.pipeThrough(new DecompressionStream("deflate"))).arrayBuffer());
    const nul = raw.indexOf(0);                          // "<type> <size>\0<body>"
    const [type] = new TextDecoder().decode(raw.subarray(0, nul)).split(" ");
    return { type, body: raw.subarray(nul + 1) };
  }
}

// Tree body: repeated "<mode> <name>\0<20-byte-oid>"
function parseTree(b: Uint8Array) {
  const out: { mode: string; name: string; oid: string }[] = []; let i = 0;
  while (i < b.length) {
    const sp = b.indexOf(0x20, i), nul = b.indexOf(0, sp);
    const oid = [...b.subarray(nul + 1, nul + 21)].map(x => x.toString(16).padStart(2, "0")).join("");
    out.push({ mode: new TextDecoder().decode(b.subarray(i, sp)), name: new TextDecoder().decode(b.subarray(sp + 1, nul)), oid });
    i = nul + 21;
  }
  return out;
}
function pkt(d: string | Uint8Array) {
  const body = typeof d === "string" ? new TextEncoder().encode(d) : d;
  const len = (body.length + 4).toString(16).padStart(4, "0");   // pkt-line: 4 hex len incl. header
  return new Uint8Array([...new TextEncoder().encode(len), ...body]);
}

// Client side (any language; shown in TS). trustedCommit came from a signed tag / signed-reflog.
async function verifyProof(trustedCommit: string, path: string, objs: { oid: string; type: string; body: Uint8Array }[]) {
  const sha1 = async (t: string, b: Uint8Array) => {
    const hdr = new TextEncoder().encode(`${t} ${b.length}\0`);
    const buf = new Uint8Array([...hdr, ...b]);
    return [...new Uint8Array(await crypto.subtle.digest("SHA-1", buf))].map(x => x.toString(16).padStart(2, "0")).join("");
  };
  for (const o of objs) if ((await sha1(o.type, o.body)) !== o.oid) throw new Error(`bad hash ${o.oid}`);
  if (objs[0].oid !== trustedCommit) throw new Error("proof not rooted at trusted commit");
  let expect = /^tree ([0-9a-f]{40})/.exec(new TextDecoder().decode(objs[0].body))![1];
  const segs = path.split("/");
  for (let i = 1; i < objs.length - 1; i++) {          // each tree must be the one its parent named
    if (objs[i].oid !== expect) throw new Error("tree not linked");
    expect = parseTree(objs[i].body).find(e => e.name === segs[i - 1])!.oid;
  }
  if (objs.at(-1)!.oid !== expect) throw new Error("blob not linked");
  return objs.at(-1)!.body;                            // verified content of <path> at <trustedCommit>
}
```

## Why it works
- A git commit OID is a SHA-1/SHA-256 commitment over its `tree` line, which commits over every entry name/mode/oid, recursively. Re-hashing each returned object and checking the link named by its parent is exactly a Merkle inclusion proof; no extra hash tree or server-side trust is needed.
- The proof for a path of depth d is the d+1 objects on that path plus the blob; sibling subtrees are not sent, so an agent (zero-clone-vfs) can verify one file in a 1 GB repo with a few KB, which is the point of a partial clone.
- The wire format is plain protocol-v2 pkt-lines under `git-upload-pack`, so it sits next to `ls-refs`/`fetch`/`object-info`; an unmodified git client ignores the advertised `proof` capability and is unaffected.
- Objects come straight from content-addressed R2 keys, so a wrong or tampered object simply fails the client's hash check; the server cannot forge a blob at a path under a commit it did not have.
- Existing blob:none partial clones already verify every lazily fetched blob by hash; this command extends that guarantee to clients that hold nothing but a commit OID.

## Known limits
- Git trees are flat lists hashed as a whole, so a proof must include entire tree objects, not O(log n) siblings. A directory with 10k entries costs ~300 KB per level. A compact side Merkle tree would not be verifiable from the commit OID, so this cannot be improved without changing git's object format.
- The proof only reduces trust to "the commit OID is right". Trusting the ref tip still needs a signed tag/commit or the signed-reflog idea; `ls-refs` output alone is unverifiable.
- Assumes loose zlib objects at `objects/<sha>`. Once objects sit in packs (gc-and-repack-alarm), `readObject` needs a SQLite `pack_index(oid,pack,offset,size)` and an R2 range read, and ofs-delta/ref-delta objects must be resolved (pinned-delta-bases or wasm-git-core); that resolution is hand-waved here.
- One R2 GET per path segment, sequential, from a single DO: latency is d round trips (~d x 20-50 ms). The `path_memo` table only short-cuts the final oid; the proof itself must still fetch every tree each time unless trees are cached in DO storage (in-do-object-cache, subject to the 128 MB DO memory and 10 GB SQLite limits).
- Blob bodies are buffered into memory before hashing on the client and streamed unhashed on the server; a multi-hundred-MB blob is fine to stream but the client verifier shown buffers it.
- Proofs are per-file; there is no single proof that "the partial clone as a whole is complete". Completeness is still the client's `fsck --connectivity-only` job.
- SHA-256 repos need 32-byte oids in `parseTree` and `SHA-256` in the verifier; shown for SHA-1 only.

## Depends on
refs-sqlite-objects-r2, content-addressed-r2-keys, protocol-v2-only, partial-clone-filters, signed-reflog
