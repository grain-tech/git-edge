> Idea #53 · foundation · verdict: **lands** · feasibility 5/5 · reliability 5/5 · correctness 4/5 · effort: days
> Proof: [proofs/info-refs-endpoint.md](./info-refs-endpoint.md) · Review: [reviews/info-refs-endpoint.md](../../reviews/archived/info-refs-endpoint.md)

# The /info/refs?service= entrypoint and pkt-line codec

## Mechanism
`git clone|fetch|push https://host/owner/repo.git` always starts with `GET /owner/repo.git/info/refs?service=git-upload-pack|git-receive-pack` (plus a `Git-Protocol: version=2` header from modern clients). The Worker validates path, `service=` and the `Git-Protocol` header, then forwards to the repo DO (`idFromName("owner/repo")`), which reads `refs` out of its SQLite table and streams a pkt-line advertisement back with `content-type: application/x-git-<service>-advertisement` and `cache-control: no-cache`. The pkt-line codec (4-hex-digit length prefix, `0000` flush, `0001` delim, `0002` response-end) is a pair of tiny pure functions plus a `TransformStream` framer/deframer; it is shared with the follow-up `POST /owner/repo.git/git-upload-pack` and `git-receive-pack` requests, whose bodies are pkt-line streams and may arrive `content-encoding: gzip` (git gzips large POST bodies), which we undo with `DecompressionStream("gzip")` before parsing.

## Primitives
- Workers `fetch` handler (GA) — URL/header validation, routing, gzip request-body decode via `DecompressionStream` (GA in workerd).
- Durable Objects with SQLite storage, `ctx.storage.sql.exec` (GA) — refs table is the source of the advertisement.
- `ReadableStream` / `TransformStream` responses (GA) — advertisement and pack results are streamed, never buffered.
- No R2 needed for this idea (objects are only touched by the POST handlers built on it).
- No beta primitives.

## Proof code
```typescript
// ---- pkt-line codec (git Documentation/gitprotocol-common: "0000" flush, "0001" delim, "0002" response-end, max 65520) ----
const enc = new TextEncoder();
export const FLUSH = enc.encode("0000"), DELIM = enc.encode("0001"), RESP_END = enc.encode("0002");
export function pkt(s: string | Uint8Array): Uint8Array {
  const b = typeof s === "string" ? enc.encode(s) : s;
  if (b.length + 4 > 65520) throw new Error("pkt-line too long");
  return concat(enc.encode((b.length + 4).toString(16).padStart(4, "0")), b);
}
/** Byte stream -> stream of {type, data} packets. Handles packets split across chunks. */
export function pktDecoder() {
  let buf = new Uint8Array(0);
  return new TransformStream<Uint8Array, { type: "flush" | "delim" | "end" | "data"; data: Uint8Array }>({
    transform(chunk, ctl) {
      buf = concat(buf, chunk);
      for (;;) {
        if (buf.length < 4) return;
        const len = parseInt(new TextDecoder().decode(buf.subarray(0, 4)), 16);
        if (Number.isNaN(len)) throw new Error("bad pkt-line length");
        if (len < 4) { ctl.enqueue({ type: (["flush", "delim", "end", "flush"] as const)[len], data: new Uint8Array(0) }); buf = buf.subarray(4); continue; }
        if (buf.length < len) return;
        ctl.enqueue({ type: "data", data: buf.subarray(4, len) }); buf = buf.subarray(len);
      }
    },
  });
}
function concat(a: Uint8Array, b: Uint8Array) { const o = new Uint8Array(a.length + b.length); o.set(a); o.set(b, a.length); return o; }

// ---- Worker: smart-HTTP entrypoint ----
const SERVICES = new Set(["git-upload-pack", "git-receive-pack"]);
export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const url = new URL(req.url);
    const m = url.pathname.match(/^\/([\w.-]+)\/([\w.-]+?)(?:\.git)?\/(info\/refs|git-upload-pack|git-receive-pack)$/);
    if (!m) return new Response("not found", { status: 404 });
    const [, owner, repo, ep] = m;
    const stub = env.REPO.get(env.REPO.idFromName(`${owner}/${repo}`));
    const v2 = /(^|:)version=2(:|$)/.test(req.headers.get("git-protocol") ?? "");
    if (ep === "info/refs") {
      if (req.method !== "GET") return new Response(null, { status: 405 });
      const service = url.searchParams.get("service");
      if (!service) return new Response("dumb HTTP not supported", { status: 403 }); // git falls back to dumb; we refuse loudly
      if (!SERVICES.has(service)) return new Response("bad service", { status: 400 });
      return stub.fetch(new Request(`https://repo/advertise?service=${service}&v2=${v2 ? 1 : 0}`));
    }
    // POST body is a pkt-line stream; git sends content-encoding: gzip when the body is large.
    if (req.method !== "POST" || req.headers.get("content-type") !== `application/x-${ep}-request`) return new Response(null, { status: 415 });
    const body = req.headers.get("content-encoding") === "gzip" ? req.body!.pipeThrough(new DecompressionStream("gzip")) : req.body!;
    return stub.fetch(new Request(`https://repo/${ep}?v2=${v2 ? 1 : 0}`, { method: "POST", body, headers: { "content-type": req.headers.get("content-type")! } }));
  },
} satisfies ExportedHandler<Env>;

// ---- Durable Object: one per owner/repo, holds refs in SQLite and emits the advertisement ----
const ZERO = "0".repeat(40);
const CAPS_V0 = { "git-upload-pack": "multi_ack_detailed side-band-64k thin-pack ofs-delta shallow no-progress include-tag allow-tip-sha1-in-want agent=git-edge/0.1",
                  "git-receive-pack": "report-status report-status-v2 delete-refs side-band-64k ofs-delta atomic push-options object-format=sha1 agent=git-edge/0.1" };
export class Repo extends DurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS refs (name TEXT PRIMARY KEY, oid TEXT NOT NULL);
                          CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT)`); // meta.HEAD = 'refs/heads/main'
  }
  async fetch(req: Request): Promise<Response> {
    const url = new URL(req.url);
    if (url.pathname !== "/advertise") return new Response(null, { status: 404 }); // POST handlers: see streaming-pack-parser / want-have-negotiation
    const service = url.searchParams.get("service")!, v2 = url.searchParams.get("v2") === "1";
    const refs = this.ctx.storage.sql.exec<{ name: string; oid: string }>("SELECT name, oid FROM refs ORDER BY name").toArray();
    const head = this.ctx.storage.sql.exec<{ v: string }>("SELECT v FROM meta WHERE k='HEAD'").one()?.v;
    const { readable, writable } = new TransformStream<Uint8Array>();
    (async () => {
      const w = writable.getWriter();
      // smart-HTTP prelude: "# service=<name>\n" then FLUSH, required by git's http client (gitprotocol-http, "smart clients")
      await w.write(pkt(`# service=${service}\n`)); await w.write(FLUSH);
      if (v2 && service === "git-upload-pack") {                                   // protocol v2: capability advertisement only, no refs (client sends ls-refs later)
        for (const l of ["version 2\n", "agent=git-edge/0.1\n", "ls-refs=unborn\n", "fetch=shallow filter wait-for-done\n", "server-option\n", "object-format=sha1\n"]) await w.write(pkt(l));
      } else {                                                                     // v0: "<oid> <ref>\0<caps>\n" on the first line, then plain "<oid> <ref>\n"
        let caps = CAPS_V0[service as keyof typeof CAPS_V0] + (head ? ` symref=HEAD:${head}` : "");
        const lines = head && refs.some(r => r.name === head) ? [{ name: "HEAD", oid: refs.find(r => r.name === head)!.oid }, ...refs] : refs;
        if (lines.length === 0) await w.write(pkt(`${ZERO} capabilities^{}\0${caps}\n`)); // empty repo: git expects this exact dummy line
        for (const [i, r] of lines.entries()) await w.write(pkt(`${r.oid} ${r.name}${i === 0 ? "\0" + caps : ""}\n`));
      }
      await w.write(FLUSH); await w.close();
    })();
    return new Response(readable, { headers: { "content-type": `application/x-${service}-advertisement`, "cache-control": "no-cache, max-age=0, must-revalidate", expires: "Fri, 01 Jan 1980 00:00:00 GMT", pragma: "no-cache" } });
  }
}
```

## Why it works
- git's HTTP transport (`remote-curl.c`) issues exactly `GET .../info/refs?service=git-upload-pack` and decides "smart" vs "dumb" solely from the response content-type being `application/x-git-upload-pack-advertisement` and the first packet being `# service=git-upload-pack\n` followed by a flush; the code emits both, so the client proceeds to `POST .../git-upload-pack` instead of falling back to dumb-HTTP `objects/` GETs.
- The v0 advertisement format (`<oid> <ref>\0<caps>\n` on the first line, `<zero-oid> capabilities^{}` for empty repos, `symref=HEAD:refs/heads/main` for the default branch) is what `git-receive-pack` and old `git-upload-pack` clients parse; the pkt-line length prefix counts the NUL and the trailing `\n`, which `pkt()` handles by measuring the encoded bytes rather than the string.
- With `Git-Protocol: version=2` the spec says the server answers `version 2` plus capability lines and no refs; `git fetch` then sends `command=ls-refs` on the POST, which is why the v2 branch advertises `ls-refs` and `fetch` and leaves ref listing to the DO's SQLite query at that point (protocol-v2-only).
- `0000`/`0001`/`0002` are length-less special packets; the decoder treats `len < 4` as control frames so the POST handlers see `delim` between v2 command args and `flush` at the end of the client's want list, which is the exact framing `git-upload-pack` relies on to know the request is complete.
- Refs come from DO SQLite in one `SELECT`, so the advertisement is consistent for one request: no torn reads between a push's CAS and a concurrent clone, because the DO serializes both.
- `Cache-Control: no-cache` + the 1980 `Expires` header mirror `git-http-backend`'s `hdr_nocache`, preventing Cloudflare's cache or an intermediate proxy from serving a stale ref list.

## Known limits
- Dumb HTTP (GET `/info/refs` without `service=`, then GET `/objects/xx/yyy`) is refused with 403 rather than implemented; every supported git client since 1.6.6 uses smart HTTP, but ancient or minimal clients (some busybox builds) will fail.
- The advertisement is built from `SELECT * FROM refs`; a repo with hundreds of thousands of refs would be a multi-MB response streamed from a single DO, bounded by the ~30 s CPU limit and DO throughput (single-threaded per repo). Practical limit is tens of thousands of refs; v2 `ls-refs` with `ref-prefix` filtering (protocol-v2-only) is the real fix.
- Workers do not transparently decode a `Content-Encoding: gzip` request body, so the manual `DecompressionStream` is required; if git sends the body chunked with `Transfer-Encoding: chunked` and no length, the Worker still streams it fine, but a >100 MB (free) / >500 MB (paid) request body is rejected by the platform before the Worker runs.
- Only SHA-1 (`object-format=sha1`) is advertised; SHA-256 repos would need 64-char oids throughout the refs table.
- This proof does not implement the POST bodies (`want`/`have`, pack streaming); it only proves the handshake and the codec those handlers consume. Auth (401 + `WWW-Authenticate: Basic`) is left to auth-and-multitenancy; git's HTTP client retries with credentials once it sees 401 on the `info/refs` GET, so that hook belongs in the Worker before `stub.fetch`.
- `Git-Protocol` header parsing accepts `version=2` anywhere in a colon-separated list; a client sending `version=1` gets the v0 advertisement prefixed by nothing extra (v1 wants a `version 1` line first), which is a small fidelity gap.

## Depends on
- repo-do-ref-authority
- refs-sqlite-objects-r2
- protocol-v2-only
