> Idea #25 · edge · verdict: **risky** · feasibility 4/5 · reliability 3/5 · correctness 3/5 · effort: weeks
> Proof: [proofs/wasm-git-core.md](./wasm-git-core.md) · Review: [reviews/wasm-git-core.md](../../reviews/archived/wasm-git-core.md)

# Wasm git core (gitoxide/libgit2) for delta resolution and merge

## Mechanism
A small Rust crate (`git_core`, built on gitoxide's `gix-pack` delta applier, `gix-object` parsing and `gix-merge`/`imara-diff` blob merging; no `std::fs`, no threads) is compiled to `wasm32-unknown-unknown` and shipped inside the Worker bundle as a `CompiledWasm` module (`import wasm from "./git_core.wasm"`), instantiated once per isolate. It exposes only pure "bytes in, bytes out" functions over its linear memory: `apply_delta(base, delta)`, `merge_blob(base, ours, theirs)`, `parse_tree`, `sha1_object`. Two callers use it: (1) the repo DO's `resolvePending(pushId)` step of `two-phase-push` reads a parked `ref-delta`/`ofs-delta` chain from `pending/<pushId>/` and its base from `objects/<sha>` in R2, copies both into Wasm memory, applies the chain, SHA-1s the result and `put`s it to `objects/<sha>`; (2) `server-side-merge` swaps its pure-JS diff3 for `merge_blob`, which produces git-identical conflict markers or a clean merged blob that the Worker writes to R2 before the DO CASes the refs. Because Wasm imports must be synchronous and R2 is async, the TypeScript side does all I/O first (it already knows the base SHA / negative offset from the delta header) and calls into Wasm only with everything resident; the Wasm never calls back out.

## Primitives
- Workers Wasm modules (`rules: [{type:"CompiledWasm", globs:["**/*.wasm"]}]`, `WebAssembly.instantiate(module, imports)` on the imported `WebAssembly.Module`) — GA; runtime `WebAssembly.compile` from bytes is *not* allowed, the module must be a bundle import
- Durable Object with SQLite storage (`ctx.storage.sql.exec` for the `pending_deltas` table) and `ctx.storage.setAlarm` to drain the queue in bounded slices — GA
- R2 `env.BUCKET.get(key)` / `.put(key, bytes)` and `get(key, {range})` for reading a base straight out of a stored pack at a known offset — GA
- `crypto.subtle.digest("SHA-1")` (or the Wasm's own sha1; both fine) — GA
- `limits.cpu_ms` in wrangler.jsonc to raise the 30 s default towards 300 s for big delta chains — GA
- Worker script size: 3 MB compressed on Free, 10 MB on Paid; a gitoxide subset (`gix-pack` delta + `gix-object` + `imara-diff`) with `opt-level="z"`, `lto`, `panic="abort"`, `wasm-opt -Oz` lands around 300-600 KB, well inside — GA limit
- Nothing beta is required. (Wasm SIMD is on by default in V8; threads/`SharedArrayBuffer` are not available in Workers, so no `rayon`.)

## Proof code
```typescript
import { DurableObject } from "cloudflare:workers";
import gitCore from "./git_core.wasm";                       // CompiledWasm rule -> WebAssembly.Module
type Env = { BUCKET: R2Bucket; REPO: DurableObjectNamespace<RepoDO> };

// Rust side (gitoxide, wasm32-unknown-unknown, cdylib):
//   #[no_mangle] pub extern "C" fn alloc(n: usize) -> *mut u8
//   #[no_mangle] pub extern "C" fn apply_delta(b:*const u8,bl:usize,d:*const u8,dl:usize,out:*mut usize)->*mut u8
//        -> gix_pack::data::delta::apply(base, &mut out, delta) after decoding the two size varints
//   #[no_mangle] pub extern "C" fn merge_blob(b,bl,o,ol,t,tl,out_len:*mut usize,conflicts:*mut u32)->*mut u8
//        -> gix_merge::blob::builtin_driver::text::merge(..., ConflictStyle::Merge)  (git-identical <<<<<<< markers)
//   #[no_mangle] pub extern "C" fn sha1_object(kind:u8,p,l,out20:*mut u8)      -> gix_object::compute_hash
type Exports = { memory: WebAssembly.Memory; alloc(n: number): number;
  apply_delta(b: number, bl: number, d: number, dl: number, out: number): number;
  merge_blob(b: number, bl: number, o: number, ol: number, t: number, tl: number, out: number, conf: number): number; };

let core: Exports | undefined;                                  // one instance per isolate, reused across requests
async function wasm(): Promise<Exports> {
  if (core) return core;
  const inst = await WebAssembly.instantiate(gitCore, {});     // no imports: the core never calls the host
  return (core = inst.exports as unknown as Exports);
}
const mem = (c: Exports) => new Uint8Array(c.memory.buffer);   // re-read after every call: memory.grow detaches views
function put(c: Exports, bytes: Uint8Array) { const p = c.alloc(bytes.length); mem(c).set(bytes, p); return p; }
function take(c: Exports, ptr: number, len: number) { return mem(c).slice(ptr, ptr + len); }

/** git delta = varint(base size) varint(result size) then ops: 0x80|copy(off/size bitmask) or 1..127 literal bytes. */
export async function applyDelta(base: Uint8Array, delta: Uint8Array): Promise<Uint8Array> {
  const c = await wasm(); const b = put(c, base), d = put(c, delta), outLen = c.alloc(4);
  const outPtr = c.apply_delta(b, base.length, d, delta.length, outLen);
  return take(c, outPtr, new DataView(c.memory.buffer).getUint32(outLen, true));
}
export async function mergeBlob(base: Uint8Array, ours: Uint8Array, theirs: Uint8Array) {
  const c = await wasm(); const [b, o, t] = [put(c, base), put(c, ours), put(c, theirs)];
  const outLen = c.alloc(4), conf = c.alloc(4);
  const p = c.merge_blob(b, base.length, o, ours.length, t, theirs.length, outLen, conf);
  const dv = new DataView(c.memory.buffer);
  return { bytes: take(c, p, dv.getUint32(outLen, true)), conflicts: dv.getUint32(conf, true) };
}

export class RepoDO extends DurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS pending_deltas (
      push_id TEXT, seq INTEGER, kind TEXT, base TEXT, sha_hint TEXT, PRIMARY KEY (push_id, seq))`);
    // kind: 'ref-delta' -> base is a 40-hex sha; 'ofs-delta' -> base is "<pack-key>@<byte-offset>" recorded by the parser
  }

  /** Phase-two helper of two-phase-push: resolve parked deltas in bounded slices, re-arm alarm if more remain. */
  async resolvePending(pushId: string): Promise<{ remaining: number }> {
    const rows = this.ctx.storage.sql.exec<{ seq: number; kind: string; base: string }>(
      `SELECT seq, kind, base FROM pending_deltas WHERE push_id = ? ORDER BY seq LIMIT 64`, pushId).toArray();
    for (const r of rows) {
      const delta = new Uint8Array(await (await this.env.BUCKET.get(`pending/${pushId}/${r.seq}`))!.arrayBuffer());
      const baseObj = await this.loadBase(r);                    // ALL I/O happens here, before entering Wasm
      if (!baseObj) continue;                                    // base is itself still pending -> next pass
      const content = await applyDelta(baseObj.content, delta);
      const sha = await sha1(baseObj.type, content);            // delta result has the base's object type
      await this.env.BUCKET.put(`objects/${sha}`, encodeLoose(baseObj.type, content)); // content-addressed-r2-keys
      this.ctx.storage.sql.exec(`DELETE FROM pending_deltas WHERE push_id = ? AND seq = ?`, pushId, r.seq);
      await this.env.BUCKET.delete(`pending/${pushId}/${r.seq}`);
    }
    const remaining = this.ctx.storage.sql.exec<{ n: number }>(
      `SELECT count(*) n FROM pending_deltas WHERE push_id = ?`, pushId).one().n;
    if (remaining > 0 && rows.length) await this.ctx.storage.setAlarm(Date.now() + 50); // keep each slice under CPU budget
    return { remaining };
  }

  private async loadBase(r: { kind: string; base: string }) {
    if (r.kind === "ref-delta") { const o = await this.env.BUCKET.get(`objects/${r.base}`); return o && decodeLoose(await o.bytes()); }
    const [key, off] = r.base.split("@");                        // ofs-delta: base lives earlier in the same pack
    const slice = await this.env.BUCKET.get(key, { range: { offset: Number(off), length: 1 << 20 } });
    return slice && (await parseOnePackEntry(await slice.bytes())); // varint header + inflate, from streaming-pack-parser
  }
  async alarm() { /* drain: SELECT DISTINCT push_id FROM pending_deltas; resolvePending each; then two-phase-push commit */ }
}
// sha1(type, body) / encodeLoose / decodeLoose / parseOnePackEntry: as in streaming-pack-parser
```

## Why it works
- Delta application is the one part of pack ingest that is pure and CPU-heavy: git's delta format (two size varints, then copy/insert ops) never needs I/O once the base is in hand, so it fits the "host does I/O, Wasm computes" split exactly; `gix-pack::data::delta::apply` is the same code gitoxide uses for real packs.
- `ofs-delta` bases are addressed by a negative byte offset in the same pack, which is precisely what an R2 range read gives you if the parser recorded `<pack-key>@<offset>`; `ref-delta` bases are 20-byte SHAs, i.e. content-addressed R2 keys. Either way the DO can fetch the base without a Wasm callback.
- The type of a delta result is the type of its base, and the object id is `sha1("<type> <len>\0" + content)`; the code preserves both, so resolved objects land at the same `objects/<sha>` the client's `git fsck` will compute.
- `gix-merge`'s text driver reproduces git's `<<<<<<< ours / ======= / >>>>>>> theirs` markers and its conflict-count return value, so `server-side-merge` can reject with `ng <ref> merge conflict` on `conflicts > 0` and otherwise write a blob byte-identical to what `git merge` would have produced locally.
- Instantiating once per isolate and never importing host functions means no Asyncify, no JSPI, no re-entrancy; a resolve slice is a plain synchronous call bounded by the `LIMIT 64` batch and the alarm re-arm.
- The module is bundled by Wrangler as a `WebAssembly.Module`, which is the only way Workers accept Wasm (no runtime `compile` of fetched bytes); this is the GA path and works identically inside a DO and a stateless Worker.

## Known limits
- Not libgit2: libgit2-to-Wasm builds (emscripten "wasm-git") assume a POSIX filesystem and pull in MEMFS, pthreads stubs and libssh2/OpenSSL; the objects here live in R2, so the practical core is a gitoxide *subset*, not a full git library. Full `gix` (repository, odb, refs, config) does not build cleanly for `wasm32-unknown-unknown` today; you pick `gix-pack` delta, `gix-object`, `gix-diff`/`imara-diff`, `gix-merge` blob driver and write your own tree merge on top (the `server-side-merge` proof already has that in TS).
- Memory: Wasm linear memory counts against the isolate's 128 MB, and the base + delta + result are all resident at once, so a 40 MB blob that deltas against a 40 MB base is the ceiling; bigger objects must be stored whole (the pack parser already parks them non-delta) or handled by `native-lfs`.
- CPU: Wasm time counts against the request CPU limit (30 s default, 300 s with `limits.cpu_ms`); the `LIMIT 64` + alarm slicing keeps a pathological push of thousands of deltas from timing out, at the cost of the ref flip waiting for the last alarm.
- No threads, no `memory.grow` beyond the isolate cap, and every `grow` detaches JS views — the helpers re-read `memory.buffer` for that reason; forgetting this is the classic silent-corruption bug.
- R2 cost: each parked delta is one GET for the delta, one GET (or range GET) for the base, one PUT and one DELETE; resolving inside the DO after the push rather than inline in the streaming parser roughly doubles class-A ops for delta-heavy pushes.
- Single-DO throughput: resolution is serialized per repo, so a large push blocks other pushes to the same repo until its alarm chain drains (branch-level-dos would shard this).
- Bundle size: a fat build with `gix-merge` plus its diff algorithms is a few hundred KB compressed; fine on Paid (10 MB), tight-but-OK on Free (3 MB) only if tree-sitter (`semantic-diffs`) is not also bundled.
- Hand-waved: the Rust crate itself is shown as signatures, not built here; error handling (delta overrun, size mismatch) is a return code in practice, and the ofs-delta base slice assumes the base entry is under 1 MB compressed (otherwise loop on range reads).

## Depends on
streaming-pack-parser, two-phase-push, content-addressed-r2-keys, server-side-merge, repo-do-ref-authority
