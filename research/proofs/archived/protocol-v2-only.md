> Idea #3 · foundation · verdict: **risky** · feasibility 4/5 · reliability 3/5 · correctness 2/5 · effort: weeks
> Proof: [proofs/protocol-v2-only.md](./protocol-v2-only.md) · Review: [reviews/protocol-v2-only.md](../../reviews/archived/protocol-v2-only.md)

# Speak git protocol v2 only, translate v0 at the edge

## Mechanism
The Worker is the only thing that ever sees pkt-lines: `GET /:owner/:repo/info/refs?service=git-upload-pack` with header `Git-Protocol: version=2` gets a static v2 capability advertisement (`version 2`, `ls-refs`, `fetch=shallow wait-for-done`, `object-info`) without touching a DO; `POST /:owner/:repo/git-upload-pack` is parsed into one v2 command (`command=ls-refs` / `command=fetch` / `command=object-info`, args, flush) and forwarded as a JSON RPC to the repo DO (`idFromName("owner/repo")`), which answers `ls-refs` straight from its `refs` SQLite table and answers `fetch` by resolving wants/haves to an object list. The Worker turns that answer back into pkt-lines: the `packfile` section is streamed as side-band-64k frames from R2 range reads, so nothing is buffered in DO memory. A client that omits `Git-Protocol` (v0/v1) hits the same handler behind a shim: the v0 ref advertisement is generated from the same `ls-refs` answer, and a v0 `want`/`have`/`done` body is rewritten into an internal `fetch` command, so there is exactly one code path for negotiation and pack streaming. Push has no v2 at all in git (receive-pack is always v0/v1, even with `protocol.version=2`), so `git-receive-pack` is a single stateless v0 POST (`<old> <new> <ref>\0caps` lines, flush, `PACK`) handled by the push pipeline, not by this shim.

## Primitives
- Workers (fetch handler, `ReadableStream`/`TransformStream` for pkt-line + side-band framing, `Git-Protocol` header sniffing)
- Durable Objects with SQLite storage (`ctx.storage.sql.exec` on the `refs` table for `ls-refs`) — GA
- DO RPC (`stub.lsRefs()` / `stub.negotiate()` typed methods instead of nested `fetch`) — GA
- R2 `get(key, { range })` for streaming the pack body — GA
- No beta primitives required. (`packfile-uris` in the v2 fetch response, used by `bundle-uri`, is a git-side feature, not a Cloudflare one.)

## Proof code
```typescript
// --- pkt-line codec (git Documentation/gitprotocol-common) ---
const enc = new TextEncoder(), dec = new TextDecoder();
const FLUSH = "0000", DELIM = "0001";
const pkt = (s: string | Uint8Array) => {
  const b = typeof s === "string" ? enc.encode(s) : s;
  return new Uint8Array([...enc.encode((b.length + 4).toString(16).padStart(4, "0")), ...b]);
};
function* parsePkts(buf: Uint8Array): Generator<string | "FLUSH" | "DELIM"> {
  for (let i = 0; i + 4 <= buf.length;) {
    const len = parseInt(dec.decode(buf.subarray(i, i + 4)), 16);
    if (len === 0) { yield "FLUSH"; i += 4; continue; }
    if (len === 1) { yield "DELIM"; i += 4; continue; }
    yield dec.decode(buf.subarray(i + 4, i + len)).replace(/\n$/, ""); i += len;
  }
}

// --- v2 command parsed from POST body: command=<name>, caps, DELIM, args, FLUSH ---
type Cmd = { name: string; args: string[] };
function parseV2(body: Uint8Array): Cmd {
  let name = "", inArgs = false; const args: string[] = [];
  for (const p of parsePkts(body)) {
    if (p === "FLUSH") break;
    if (p === "DELIM") { inArgs = true; continue; }
    if (!inArgs && p.startsWith("command=")) name = p.slice(8);
    else if (inArgs) args.push(p);
  }
  return { name, args };
}

// --- v0 shim: a stateless-rpc upload-pack body (want/have/done) becomes the same Cmd ---
function parseV0(body: Uint8Array): Cmd {
  const args: string[] = [];
  for (const p of parsePkts(body)) {
    if (p === "FLUSH" || p === "DELIM") continue;
    const [k, v] = [p.split(" ")[0], p.split(" ")[1]];       // first want carries "\0caps"
    if (k === "want" || k === "have") args.push(`${k} ${v.split("\0")[0]}`);
    if (k === "done") args.push("done");
  }
  return { name: "fetch", args };
}

// --- repo DO: refs are the only state the protocol needs ---
export class RepoDO extends DurableObject<Env> {
  lsRefs(prefixes: string[]) {                                // ls-refs answer straight from SQLite
    const rows = this.ctx.storage.sql.exec<{ name: string; oid: string }>(
      "SELECT name, oid FROM refs ORDER BY name").toArray();
    return rows.filter(r => !prefixes.length || prefixes.some(p => r.name.startsWith(p)));
  }
  negotiate(wants: string[], haves: string[]): { acks: string[]; packKey: string; range?: { offset: number; length: number } } {
    /* want-have-negotiation: walk commit_graph table, return the R2 pack key (+ range) to send */
    return { acks: haves.filter(h => this.ctx.storage.sql.exec("SELECT 1 FROM commit_graph WHERE oid=?", h).toArray().length > 0),
             packKey: `repos/${this.ctx.id}/pack/latest.pack` };
  }
}

// --- Worker: advertisement + command dispatch, v0 and v2 share one path ---
export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const url = new URL(req.url);
    const m = url.pathname.match(/^\/([^/]+\/[^/]+)\/(info\/refs|git-upload-pack)$/)!;
    const stub = env.REPO.get(env.REPO.idFromName(m[1]));
    const v2 = /version=2/.test(req.headers.get("git-protocol") ?? "");
    const adv = { headers: { "content-type": "application/x-git-upload-pack-advertisement", "cache-control": "no-cache" } };

    if (m[2] === "info/refs") {
      const head = pkt("# service=git-upload-pack\n"), flush = enc.encode(FLUSH);
      if (v2) return new Response(cat(head, flush, pkt("version 2\n"), pkt("agent=git-edge\n"), pkt("ls-refs\n"),
        pkt("fetch=shallow wait-for-done\n"), pkt("object-info\n"), pkt("server-option\n"), flush), adv);
      const refs = await stub.lsRefs([]);                      // v0: refs + caps on the first line, \0-separated
      const caps = "\0side-band-64k thin-pack ofs-delta no-done agent=git-edge";
      return new Response(cat(head, flush, ...refs.map((r, i) => pkt(`${r.oid} ${r.name}${i ? "" : caps}\n`)), flush), adv);
    }

    const cmd = v2 ? parseV2(new Uint8Array(await req.arrayBuffer())) : parseV0(new Uint8Array(await req.arrayBuffer()));
    const result = { headers: { "content-type": "application/x-git-upload-pack-result" } };
    if (cmd.name === "ls-refs") {
      const refs = await stub.lsRefs(cmd.args.filter(a => a.startsWith("ref-prefix ")).map(a => a.slice(11)));
      return new Response(cat(...refs.map(r => pkt(`${r.oid} ${r.name}\n`)), enc.encode(FLUSH)), result);
    }
    if (cmd.name === "fetch") {
      const wants = cmd.args.filter(a => a.startsWith("want ")).map(a => a.slice(5));
      const haves = cmd.args.filter(a => a.startsWith("have ")).map(a => a.slice(5));
      const { acks, packKey, range } = await stub.negotiate(wants, haves);
      const obj = await env.BUCKET.get(packKey, range ? { range } : undefined);  // R2 range read, streamed
      const { readable, writable } = new TransformStream<Uint8Array, Uint8Array>();
      const w = writable.getWriter();
      (async () => {
        if (v2) { await w.write(pkt("acknowledgments\n")); for (const a of acks) await w.write(pkt(`ACK ${a}\n`));
                  await w.write(pkt("ready\n")); await w.write(enc.encode(DELIM)); await w.write(pkt("packfile\n")); }
        else    { await w.write(pkt(acks.length ? `ACK ${acks.at(-1)}\n` : "NAK\n")); }
        const rd = obj!.body.getReader();                       // side-band-64k: band 1 = pack data, max 65520 bytes/frame
        for (;;) { const { value, done } = await rd.read(); if (done) break;
          for (let o = 0; o < value.length; o += 65515) await w.write(pkt(cat(new Uint8Array([1]), value.subarray(o, o + 65515)))); }
        await w.write(enc.encode(FLUSH)); await w.close();
      })();
      return new Response(readable, result);
    }
    return new Response(pkt("ERR unknown command\n"), { status: 400 });
  },
};
const cat = (...parts: Uint8Array[]) => { const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0)); let o = 0; for (const p of parts) { out.set(p, o); o += p.length; } return out; };
```

## Why it works
- git sends `Git-Protocol: version=2` as an HTTP header on the `info/refs` GET (and every POST) whenever `protocol.version` is 2, which has been the default since git 2.26; the header is the whole version-selection mechanism, so a Worker can branch on it before doing any work. A server that answers `version 2` in the advertisement commits the client to command mode for the rest of the session.
- Protocol v2 was designed to be stateless: every POST is a self-contained `command=...` block, and `fetch` re-sends all `have`s each round (with `stateless-rpc` semantics baked in). That is exactly the Workers request/response model; no DO needs to hold negotiation state between rounds, only the `refs` and `commit_graph` tables.
- `ls-refs` requires only `<oid> <refname>` pkt-lines plus optional `symref-target:`/`peeled:` attributes; that is a single `SELECT` on the repo DO, so the ref advertisement never scales with repo size beyond the number of refs, and `ref-prefix` args let the DO filter server-side.
- The v2 `fetch` response is sectioned (`acknowledgments`, `ready`, delim, `packfile`) and the pack is carried on side-band band 1 in frames of at most 65520 bytes, which maps 1:1 onto a `TransformStream` fed by an R2 range read; the Worker never sees the whole pack. Band 2 is free for progress text and band 3 for a fatal error, all without breaking the stream.
- The v0 shim is cheap because smart-HTTP v0 is already `stateless-rpc`: the client resends the full `want`/`have`/`done` conversation on each POST, so the only real difference is the wire format of the advertisement (caps hidden after `\0` on the first ref line, `HEAD` first) and the `NAK`/`ACK` line before the pack. Both are handled in a few lines, and `no-done` lets the server send the pack as soon as it has enough haves.
- receive-pack is unaffected: git never negotiates v2 for push, so a v0 `git-receive-pack` POST (commands, flush, `PACK` stream) is the only push path and is stateless anyway; the `report-status-v2` response format is emitted by the push pipeline.

## Known limits
- "v2 only" is not literally achievable: git has no v2 receive-pack, so push is v0/v1 forever. The honest statement is "v2 for all fetch-side traffic, v0 accepted on the wire for fetch (via shim) and required for push".
- Dumb HTTP clients (no `service=` query, or fetching `objects/info/packs`) are not covered by the shim above. They can be served by pointing `objects/info/packs` and `objects/pack/*` at the precomputed R2 pack, but only for a full clone, never for an incremental fetch; the proof hand-waves this.
- The v0 shim collapses multi-round negotiation into "answer with whatever `negotiate` returns"; a correct v0 server must emit `ACK <oid> continue`/`common` lines per round under `multi_ack_detailed`. Without that, v0 clients with a lot of local history may receive a larger pack than necessary (correct, but wasteful).
- Streaming the pack from R2 assumes an object list can be turned into an R2 key plus range (precomputed pack) or a thin pack assembled by another component; the proof does not build packs, it only frames them. Per-object packing for arbitrary want sets is idea 4/56/7 territory.
- Workers request limits: the pack stream is a Response body so it is not subject to the 128 MB Worker memory cap, but the 30 s CPU budget is only safe because framing is byte copying; any zlib work on the fetch path would need chunking across requests.
- `wait-for-done` and `packfile-uris` are advertised only when the corresponding ideas (`want-have-negotiation`, `bundle-uri`) are implemented; advertising a capability you do not honour makes git error out mid-fetch.
- R2 cost: every fetch is at least one Class B GET plus one DO request; `ls-refs` goes to the DO on every `git fetch`, so a hot repo with many pollers is bounded by single-DO throughput (roughly low thousands of req/s) until `replicated-refs-edge` moves reads to KV.

## Depends on
- repo-do-ref-authority
- refs-sqlite-objects-r2
- info-refs-endpoint
- want-have-negotiation
- precomputed-clone-pack
