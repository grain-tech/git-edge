> Idea #27 · edge · verdict: **risky** · feasibility 4/5 · reliability 3/5 · correctness 2/5 · effort: weeks
> Proof: [proofs/diff-api-range-reads.md](./diff-api-range-reads.md) · Review: [reviews/diff-api-range-reads.md](../../reviews/archived/diff-api-range-reads.md)

# Diff API served with R2 range reads

## Mechanism
`GET /:owner/:repo/diff?from=<blob-oid>&to=<blob-oid>` (or a commit pair that the DO first narrows to changed blob pairs by tree walk) is routed by the Worker to the repo DO (`idFromName(owner/repo)`), which owns a SQLite table `pack_objects(oid, pack, offset, clen, type, base_oid)` filled by the streaming pack parser at push time and rewritten by the repack alarm. If `to` is stored in a pack as an OFS/REF-delta whose base is `from` (or the reverse), the DO issues exactly one `env.BUCKET.get(pack, { range })` for the delta entry, inflates it, and decodes git's copy/insert opcode stream into an edit script; `to` is never reconstructed. The edit script alone answers "what changed, how many bytes inserted, which base ranges dropped"; rendering a unified diff with line context additionally needs the base blob, which is one more range read (or zero if it sits in the in-DO cache). If the packer chose a different base, the DO falls back to two range reads plus an in-Worker Myers diff and says so in the response.

## Primitives
- Durable Objects with SQLite storage (`ctx.storage.sql.exec`) — GA
- DO alarms (`ctx.storage.setAlarm`) for the path-aware repack that makes "delta base == previous version" true by construction — GA
- R2 range reads (`get(key, { range: { offset, length } })`) — GA
- `node:zlib` `inflateSync(..., { info: true })` under `nodejs_compat` (tolerates the over-estimated range; `DecompressionStream("deflate")` works too but rejects trailing bytes, so it needs the exact compressed length) — GA
- Workers `Response` JSON/stream — GA

## Proof code
```typescript
import { inflateSync } from "node:zlib";

type Env = { BUCKET: R2Bucket; REPO: DurableObjectNamespace };
type Entry = { oid: string; pack: string; offset: number; clen: number; type: number; base_oid: string | null };
type Op = { copy: [number, number] } | { insert: Uint8Array };

// Worker: /:owner/:repo/diff?from=<oid>&to=<oid>  ->  repo DO
export default {
  async fetch(req: Request, env: Env) {
    const [, owner, repo] = new URL(req.url).pathname.split("/");
    return env.REPO.get(env.REPO.idFromName(`${owner}/${repo}`)).fetch(req);
  },
};

export class RepoDO implements DurableObject {
  constructor(private ctx: DurableObjectState, private env: Env) {
    // Filled by the streaming pack parser on push (clen = next entry offset - offset, an upper bound)
    // and rewritten by the repack alarm, which deltifies each blob against its same-path predecessor.
    ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS pack_objects (
      oid TEXT PRIMARY KEY, pack TEXT NOT NULL, offset INTEGER NOT NULL, clen INTEGER NOT NULL,
      type INTEGER NOT NULL, base_oid TEXT)`);
  }

  async fetch(req: Request): Promise<Response> {
    const u = new URL(req.url);
    const from = u.searchParams.get("from")!, to = u.searchParams.get("to")!;
    const e = (oid: string) => this.ctx.storage.sql.exec<Entry>("SELECT * FROM pack_objects WHERE oid=?", oid).one();
    const T = e(to), F = e(from);

    if (T.base_oid === from) {            // fast path: pack delta IS the history diff
      const ops = parseDelta(await this.readEntry(T));
      return Response.json({ mode: "pack-delta", base: from, ...summarize(ops), ops: ops.map(jsonOp) });
    }
    if (F.base_oid === to) {              // reversed chain (packer often deltifies older against newer)
      const ops = parseDelta(await this.readEntry(F));
      return Response.json({ mode: "pack-delta-reversed", base: to, ...summarize(ops), ops: ops.map(jsonOp) });
    }
    // Slow path: bases differ; resolve both (each chain link = one range read) and diff in the isolate.
    const [a, b] = await Promise.all([this.resolve(F), this.resolve(T)]);
    return Response.json({ mode: "fallback-myers", hunks: myers(a, b) /* pseudo: line-level Myers */ });
  }

  // One R2 range read for one pack entry. Returns the inflated payload (delta stream or raw object).
  private async readEntry(en: Entry): Promise<Uint8Array> {
    const obj = await this.env.BUCKET.get(en.pack, { range: { offset: en.offset, length: en.clen } });
    const raw = new Uint8Array(await obj!.arrayBuffer());
    let i = 0;                                        // entry header: type in bits 4-6, size varint
    while (raw[i++] & 0x80);
    if (en.type === 6) while (raw[i++] & 0x80);       // OFS_DELTA: negative offset varint
    if (en.type === 7) i += 20;                       // REF_DELTA: 20-byte base SHA
    // zlib stream follows; inflateSync ignores the over-read tail because clen is an upper bound
    return new Uint8Array(inflateSync(raw.subarray(i)));
  }

  // Full reconstruction, only used on the slow path. Depth is bounded by git's pack.depth (default 50).
  private async resolve(en: Entry): Promise<Uint8Array> {
    const body = await this.readEntry(en);
    if (en.type < 6) return body;
    const base = await this.resolve(this.ctx.storage.sql.exec<Entry>("SELECT * FROM pack_objects WHERE oid=?", en.base_oid).one());
    return applyDelta(base, parseDelta(body));
  }
}

// git delta format: varint base_size, varint result_size, then opcodes.
// opcode & 0x80 -> copy from base (bits 0-3 pick offset bytes, bits 4-6 pick size bytes; size 0 means 0x10000)
// opcode 1..127 -> insert that many literal bytes. opcode 0 is reserved.
function parseDelta(d: Uint8Array): Op[] {
  let i = 0;
  const varint = () => { let v = 0, s = 0, b; do { b = d[i++]; v |= (b & 0x7f) << s; s += 7; } while (b & 0x80); return v; };
  varint(); varint();                                 // base size, result size (used for validation)
  const ops: Op[] = [];
  while (i < d.length) {
    const c = d[i++];
    if (c & 0x80) {
      let off = 0, len = 0;
      for (let k = 0; k < 4; k++) if (c & (1 << k)) off |= d[i++] << (8 * k);
      for (let k = 0; k < 3; k++) if (c & (0x10 << k)) len |= d[i++] << (8 * k);
      ops.push({ copy: [off, len || 0x10000] });
    } else if (c) { ops.push({ insert: d.subarray(i, i + c) }); i += c; }
    else throw new Error("reserved delta opcode 0");
  }
  return ops;
}

function summarize(ops: Op[]) {                       // answerable with zero reads of the base
  let inserted = 0, copied = 0;
  for (const o of ops) "copy" in o ? (copied += o.copy[1]) : (inserted += o.insert.length);
  return { insertedBytes: inserted, copiedBytes: copied };
}
const jsonOp = (o: Op) => ("copy" in o ? { copy: o.copy } : { insert: new TextDecoder().decode(o.insert) });
declare function applyDelta(base: Uint8Array, ops: Op[]): Uint8Array;
declare function myers(a: Uint8Array, b: Uint8Array): unknown;
```

## Why it works
- A packfile entry is self-delimiting: `[type|size varint][ofs/ref base][zlib stream]`. With `offset` and an upper-bound `clen` from the pack index kept in DO SQLite, one R2 range read returns exactly one object's delta without touching the rest of the pack; the `.idx` file is never consulted at request time.
- git's delta stream is a copy/insert edit script against a single base object. Decoding it yields the changed byte ranges directly, so "is it changed", inserted/deleted byte counts and the raw inserted text come from the delta alone with no base and no target reconstruction.
- The packer chooses delta bases by name-hash and size, so same-path successive versions are usually each other's base already; the repack alarm makes this deterministic by deltifying every blob against its same-path predecessor in the commit graph. Because we own the packer, "pack delta == history diff" is a policy, not luck.
- Deltas are one-directional but symmetric in information: if the older blob is stored as a delta of the newer one (git's usual direction, since larger objects are packed first), the same ops describe the reverse diff — literal inserts are deletions, uncovered base ranges are additions.
- The response is a normal `Response.json`, so the Worker adds nothing; the DO does at most one R2 request per hunk-less answer and one chain walk per fallback, all well inside the 30s CPU budget.

## Known limits
- The idea as literally stated ("no reconstruction at all") only holds for byte-level answers. A unified diff with line context must inflate the base blob (one range read, more if the base is itself a delta) because you cannot range-read into the middle of a zlib stream; the target side is still never rebuilt.
- When the packer's base is not the history predecessor (fresh push packs use git's own heuristics; renames; cross-path bases), the fast path misses and the DO does a full two-sided reconstruction plus Myers in the isolate. Hit rate depends on the repack alarm having run.
- Loose objects written by the streaming parser before the first repack are whole blobs, not deltas: no fast path until `gc-and-repack-alarm` fires.
- Every blob pair is one DO round trip, so a commit-level diff over N changed files costs N+ R2 GETs (~$0.36 per million Class B ops) and is serialized through the single repo DO.
- Blobs above a few tens of MB blow the 128 MB DO memory on the fallback path; the fast path still works since only the delta is loaded. Chain depth 50 x large bases is the worst case.
- `inflateSync` on an over-estimated range is used to dodge `DecompressionStream` rejecting trailing bytes; storing the exact compressed length (the parser knows it) removes that dependency.
- Tree walk from commit pair to blob pairs is hand-waved; it is the same tree-diff every git server does.

## Depends on
- streaming-pack-parser
- refs-sqlite-objects-r2
- gc-and-repack-alarm
- in-do-object-cache
