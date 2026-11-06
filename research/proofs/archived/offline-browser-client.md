> Idea #52 · wild · verdict: **risky** · feasibility 3/5 · reliability 3/5 · correctness 2/5 · effort: months
> Proof: [proofs/offline-browser-client.md](./offline-browser-client.md) · Review: [reviews/offline-browser-client.md](../../reviews/archived/offline-browser-client.md)

# Offline-first browser client with OPFS and the same Wasm core

## Mechanism
The git core (pkt-line codec, protocol v2 `ls-refs`/`fetch` client and server, PACK reader/writer with ofs-delta/ref-delta resolution) is one host-agnostic ES module + Wasm blob that only talks to an `ObjectStore` interface; the Worker instantiates it over `R2 + DO SQLite`, the browser instantiates the identical module over `OPFS (navigator.storage.getDirectory) + IndexedDB` and runs it in a Web Worker. Online, the browser client does `GET /:owner/:repo/info/refs?service=git-upload-pack` and `POST /:owner/:repo/git-upload-pack` with `Git-Protocol: version=2` against the edge Worker exactly like `git` does, and the Worker routes to the repo DO (`idFromName(owner/repo)`), which streams a PACK out of R2. Offline, `commit`/`branch`/`checkout` write loose objects and refs to OPFS only; when `navigator.onLine` flips (or a Background Sync tag fires) the client builds a thin PACK of objects the server lacks and `POST`s it to `/git-receive-pack`, where the DO does the ordinary two-phase push and returns `report-status-v2` (`ok refs/heads/x` / `ng ... non-fast-forward`).

## Primitives
- Workers (edge routing, CORS preflight for `Git-Protocol` header, serves the browser bundle + Wasm as static assets)
- Durable Object SQLite (`ctx.storage.sql.exec`) for refs/CAS on the server side
- R2 (`env.BUCKET.get/put`, range reads for pack slices) for objects
- Workers Static Assets (GA) to host the Wasm core with `Cross-Origin-Embedder-Policy` headers (needed only if the browser core uses SharedArrayBuffer threads; not required for the single-threaded build)
- `DecompressionStream("deflate")` — available in both Workers and browsers, so loose-object inflate is shared; see limits for pack-internal inflate
- Browser-side, not Cloudflare: OPFS `createSyncAccessHandle` (GA in Chrome/Safari/Firefox, only inside a Web Worker), IndexedDB, Background Sync (Chromium only — the fallback is an `online` event listener)

## Proof code
```typescript
// core/store.ts — the single seam. Everything in the git core (pkt-line, v2 commands,
// PACK reader, delta resolver) depends only on this.
export interface ObjectStore {
  has(oid: string): Promise<boolean>;
  get(oid: string): Promise<Uint8Array | null>;      // zlib-compressed loose object bytes
  put(oid: string, zbytes: Uint8Array): Promise<void>;
  ref(name: string): Promise<string | null>;
  cas(name: string, expect: string | null, next: string): Promise<boolean>;
}

// core/client.ts — shared fetch/push; identical bytes on the wire to C git.
export async function fetchV2(base: string, store: ObjectStore, want: string[], have: string[]) {
  const hdr = { "Git-Protocol": "version=2", "Content-Type": "application/x-git-upload-pack-request" };
  await fetch(`${base}/info/refs?service=git-upload-pack`, { headers: hdr }); // capability advert (v2: "version 2\n")
  const body = [...pkt("command=fetch"), ...pkt("object-format=sha1"), DELIM,
    ...want.flatMap(w => pkt(`want ${w}`)), ...have.flatMap(h => pkt(`have ${h}`)),
    ...pkt("done"), FLUSH].join("");
  const res = await fetch(`${base}/git-upload-pack`, { method: "POST", headers: hdr, body });
  // response: pkt "packfile\n" then sideband-64k frames; band 1 = PACK bytes
  const pack = demuxSideband(res.body!);          // ReadableStream<Uint8Array>
  await readPack(pack, store);                     // "PACK", v2, N objects; ofs-delta/ref-delta resolved against store
}

export async function pushV2(base: string, store: ObjectStore, ref: string, oldOid: string, newOid: string) {
  const pack = await writeThinPack(store, { want: newOid, exclude: oldOid }); // objects reachable from new but not old
  const body = concat(
    enc(pkt(`${oldOid} ${newOid} ${ref}\0report-status-v2 side-band-64k object-format=sha1`).join("")),
    enc(FLUSH), pack);
  const res = await fetch(`${base}/git-receive-pack`, { method: "POST",
    headers: { "Content-Type": "application/x-git-receive-pack-request" }, body });
  const report = await res.text();                 // "unpack ok" / "ok refs/heads/main" | "ng refs/heads/main non-fast-forward"
  if (/^ok /m.test(report)) await store.cas(`refs/remotes/origin/${ref.slice(11)}`, null, newOid);
  return report;
}

// hosts/browser.ts — runs inside a Web Worker (OPFS sync handles require it).
export class OpfsStore implements ObjectStore {
  constructor(private root: FileSystemDirectoryHandle, private db: IDBDatabase) {}
  private async file(oid: string, create = false) {
    const dir = await (await this.root.getDirectoryHandle("objects", { create })).getDirectoryHandle(oid.slice(0, 2), { create });
    return dir.getFileHandle(oid.slice(2), { create });
  }
  async has(oid: string) { try { await this.file(oid); return true; } catch { return false; } }
  async get(oid: string) { try { return new Uint8Array(await (await (await this.file(oid)).getFile()).arrayBuffer()); } catch { return null; } }
  async put(oid: string, z: Uint8Array) {
    const h = await (await this.file(oid, true)).createSyncAccessHandle(); // zero-copy, no JS heap growth
    h.write(z); h.flush(); h.close();
  }
  async ref(name: string) { return idb(this.db, "refs", "readonly", s => s.get(name)); }
  async cas(name: string, expect: string | null, next: string) {
    // IndexedDB transaction is the browser's serialization point, same contract as the DO
    return idb(this.db, "refs", "readwrite", async s => {
      const cur = (await req(s.get(name))) ?? null;
      if (cur !== expect) return false; s.put(next, name); return true;
    });
  }
}

// hosts/worker.ts — the same interface over DO SQLite + R2 (excerpt).
export class RepoDO extends DurableObject<Env> implements ObjectStore {
  async get(oid: string) { const o = await this.env.BUCKET.get(`obj/${oid}`); return o ? new Uint8Array(await o.arrayBuffer()) : null; }
  async put(oid: string, z: Uint8Array) { await this.env.BUCKET.put(`obj/${oid}`, z); }
  async has(oid: string) { return !!(await this.env.BUCKET.head(`obj/${oid}`)); }
  async ref(name: string) { return this.ctx.storage.sql.exec("SELECT oid FROM refs WHERE name=?", name).one()?.oid as string ?? null; }
  async cas(name: string, expect: string | null, next: string) {
    const r = this.ctx.storage.sql.exec(
      "UPDATE refs SET oid=? WHERE name=? AND oid IS ?", next, name, expect).rowsWritten;
    return r === 1 || (expect === null && this.ctx.storage.sql.exec(
      "INSERT OR IGNORE INTO refs(name,oid) VALUES(?,?)", name, next).rowsWritten === 1);
  }
}

// Edge Worker: browsers need CORS for the non-simple Git-Protocol header; C git never preflights.
export const cors = (r: Response) => { r.headers.set("Access-Control-Allow-Origin", "*");
  r.headers.set("Access-Control-Allow-Headers", "Git-Protocol, Content-Type"); return r; };

const FLUSH = "0000", DELIM = "0001";
const pkt = (s: string) => [(s.length + 5).toString(16).padStart(4, "0") + s + "\n"];
```

## Why it works
- Git smart HTTP is stateless per request in protocol v2 (`ls-refs`, `fetch`, `done` in one POST), so a browser `fetch()` can be a byte-exact substitute for C git's transport; the DO never learns which host produced the bytes.
- `git-receive-pack` only requires the command list (`old new ref\0caps`) followed by a PACK whose deltas resolve against objects the server already has; the browser can build a thin pack because after a fetch it holds the same `refs/remotes/origin/*` tips and can exclude them.
- `report-status-v2` returns `ng ... non-fast-forward` when the DO's CAS fails, so an offline commit made on a stale base is rejected exactly as with a real remote; the client then fetches, rebases locally in the Wasm core, and retries.
- Loose objects are a single zlib stream, so `DecompressionStream("deflate")` inflates them identically in Workers and browsers; the object layout (`objects/xx/yyyy...`) in OPFS is the same as on-disk git, which makes the local store debuggable with normal tools when exported.
- Refs are the only mutable state on both sides, and both hosts have a serializing primitive: single-threaded DO + SQLite `UPDATE ... WHERE oid IS ?` on the server, an IndexedDB `readwrite` transaction in the browser.

## Known limits
- `DecompressionStream` cannot inflate objects inside a PACK: entries are back-to-back zlib streams with no length prefix, and the Web API does not report bytes consumed. Both hosts need the Wasm inflater (miniz/zlib-rs) for pack reading; DecompressionStream only covers loose objects and the `packfile` sideband when the whole stream is one object.
- Wasm core on the server runs inside a Worker with a 128MB isolate memory cap and 30s CPU; a browser tab has gigabytes and no CPU cap, so "the same core" is true for code but not for resource envelopes. Delta resolution of large packs must stay streaming on the server side (depends on `streaming-pack-parser`).
- OPFS `createSyncAccessHandle` only works in a dedicated Web Worker, and Safari's OPFS quota is smaller and evictable; a large clone can be silently evicted by the browser. Treat OPFS as a cache with a rebuildable origin, not durable storage.
- Browsers send a CORS preflight for the `Git-Protocol` header; the edge Worker must answer `OPTIONS` and echo the header, and per-repo auth must be bearer-token (no `credentials: include` across origins without cookie plumbing). Background Sync is Chromium-only; elsewhere sync runs only while the tab is open.
- Server-side `has()` via `R2 head` per object is one billed Class B op per object during thin-pack validation; at scale this needs the SQLite object index from `want-have-negotiation`/`content-addressed-r2-keys`, not per-object HEADs.
- Hand-waved: `writeThinPack`, `readPack`, `demuxSideband` are named-but-not-shown; they are the Wasm core's job and are the bulk of the actual work. A merge/rebase on a rejected push requires `wasm-git-core` to be complete, not just the pack reader.

## Depends on
- wasm-git-core
- streaming-pack-parser
- protocol-v2-only
- info-refs-endpoint
- repo-do-ref-authority
- two-phase-push
- auth-and-multitenancy
