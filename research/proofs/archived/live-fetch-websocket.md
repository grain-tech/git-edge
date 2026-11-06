> Idea #15 · edge · verdict: **lands with caveats** · feasibility 4/5 · reliability 3/5 · correctness 2/5 · effort: weeks
> Proof: [proofs/live-fetch-websocket.md](./live-fetch-websocket.md) · Review: [reviews/live-fetch-websocket.md](../../reviews/archived/live-fetch-websocket.md)

# Live fetch over hibernating WebSockets

## Mechanism
A client-side helper (not stock `git`, which has no WebSocket transport) opens `GET /:owner/:repo/live` with `Upgrade: websocket`; the Worker routes it by `idFromName("owner/repo")` to the repo DO, which calls `ctx.acceptWebSocket(ws, [...refsSubscribed])` and stores the client's last-known tips in `ws.serializeAttachment(...)`, then lets the DO hibernate (zero duty-cycle cost, socket stays open at the edge). When a push commits a ref move in the DO (the same `UPDATE refs` in DO SQLite that `two-phase-push` performs), the DO calls `ctx.getWebSockets(refname)` and sends each subscriber a tiny JSON nudge `{ref, old, new}`; the helper answers with an ordinary protocol-v2 `fetch` request (`want <new>` / `have <old>` / `done`) over the same socket, and the DO streams back a pkt-line-framed packfile built from R2 objects, exactly as it would over HTTP. The nudge and the fetch are two frames on one socket; nothing about git's object or pack format changes, only the carrier.

## Primitives
- Durable Objects with SQLite storage (`ctx.storage.sql.exec`) for the refs table (GA)
- DO WebSocket Hibernation API: `ctx.acceptWebSocket`, `ctx.getWebSockets(tag)`, `webSocketMessage`/`webSocketClose` handlers, `ws.serializeAttachment` (GA)
- `ctx.setWebSocketAutoResponse` so keepalive pings are answered without waking the DO (GA)
- DO alarms (`ctx.storage.setAlarm`) to expire subscribers whose auth token lapsed (GA)
- R2 `env.BUCKET.get(key)` for loose objects streamed into the pack (GA)
- Workers `WebSocketPair` at the edge only if the Worker needs to inspect the handshake; otherwise the Worker forwards the Upgrade request straight to the DO stub (GA)
- Nothing beta is required.

## Proof code
```typescript
// Repo DO: refs authority + hibernating live-fetch subscribers.
// Wire format on the socket: text frames are JSON control; binary frames are
// raw pkt-line streams identical to the smart-HTTP v2 bodies.
export class RepoDO implements DurableObject {
  constructor(private ctx: DurableObjectState, private env: Env) {
    ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS refs (name TEXT PRIMARY KEY, sha TEXT NOT NULL)`);
    // Answer keepalives while hibernated; no wake, no CPU billed.
    ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong"));
  }

  async fetch(req: Request): Promise<Response> {
    const url = new URL(req.url);
    if (url.pathname === "/live" && req.headers.get("Upgrade") === "websocket") {
      const refs = (url.searchParams.get("refs") ?? "refs/heads/main").split(",");
      const pair = new WebSocketPair();
      // Tags = subscribed refnames, so a push can fan out with getWebSockets(ref).
      this.ctx.acceptWebSocket(pair[1], refs);
      // Persist per-socket negotiation state across hibernation (<=2KB).
      pair[1].serializeAttachment({ have: this.currentTips(refs) });
      return new Response(null, { status: 101, webSocket: pair[0] });
    }
    if (url.pathname === "/receive-pack" && req.method === "POST") return this.receivePack(req);
    return new Response("not found", { status: 404 });
  }

  // Called by the push path after objects are already in R2 (two-phase-push).
  private updateRef(name: string, oldSha: string, newSha: string) {
    const cur = this.ctx.storage.sql.exec<{ sha: string }>("SELECT sha FROM refs WHERE name=?", name).toArray()[0];
    if ((cur?.sha ?? "0".repeat(40)) !== oldSha) throw new Error("ref cas failed"); // git expects "ng <ref> fetch first"
    this.ctx.storage.sql.exec("INSERT OR REPLACE INTO refs(name, sha) VALUES(?,?)", name, newSha);
    // Nudge every hibernated subscriber of this ref. This wakes nothing on the
    // client side but a helper; the DO itself is already awake for the push.
    for (const ws of this.ctx.getWebSockets(name)) {
      ws.send(JSON.stringify({ t: "ref", ref: name, old: oldSha, new: newSha }));
    }
  }

  // Hibernation handler: DO is re-instantiated from scratch when a frame arrives.
  async webSocketMessage(ws: WebSocket, msg: string | ArrayBuffer) {
    if (typeof msg === "string") return; // control frames from client (unsubscribe etc.)
    // Binary frame = a protocol-v2 fetch command body, pkt-line framed:
    //   command=fetch\n 0001 want <sha>\n have <sha>\n done\n 0000
    const { wants, haves } = parseV2Fetch(new Uint8Array(msg));
    const state = ws.deserializeAttachment() as { have: Record<string, string> };
    const objs = await this.objectsBetween(wants, [...haves, ...Object.values(state.have)]); // want-have-negotiation
    // Reply is byte-identical to the smart-HTTP response: pkt-line "packfile\n",
    // then sideband-1 chunks of PACK v2 header + entries, then 0000.
    ws.send(pkt("acknowledgments\n") + pkt("NAK\n") + "0001" + pkt("packfile\n"));
    for await (const chunk of packStream(this.env.BUCKET, objs)) ws.send(sideband(1, chunk));
    ws.send("0000");
    for (const w of wants) state.have[w] = w; // remember what this client now has
    ws.serializeAttachment(state);
  }

  async webSocketClose(ws: WebSocket) { ws.close(); }
  async webSocketError(ws: WebSocket) { ws.close(); }

  private currentTips(refs: string[]) {
    const out: Record<string, string> = {};
    for (const r of refs) {
      const row = this.ctx.storage.sql.exec<{ sha: string }>("SELECT sha FROM refs WHERE name=?", r).toArray()[0];
      if (row) out[r] = row.sha;
    }
    return out;
  }
  private async receivePack(req: Request): Promise<Response> { /* streaming-pack-parser -> R2 -> updateRef */ return new Response("0000"); }
  private async objectsBetween(wants: string[], haves: string[]): Promise<string[]> { /* commit-graph walk in SQLite */ return wants; }
}

// Worker: route the Upgrade straight to the DO; it never terminates the socket itself.
export default {
  async fetch(req: Request, env: Env) {
    const [, owner, repo, rest] = new URL(req.url).pathname.split("/");
    return env.REPO.get(env.REPO.idFromName(`${owner}/${repo}`)).fetch(new Request(new URL(`/${rest}`, req.url), req));
  },
};

const pkt = (s: string) => (s.length + 4).toString(16).padStart(4, "0") + s;
async function* packStream(bucket: R2Bucket, shas: string[]): AsyncGenerator<Uint8Array> {
  yield packHeader(shas.length);                          // "PACK" + version 2 + count
  for (const sha of shas) {
    const obj = await bucket.get(`objects/${sha}`);        // stored as raw zlib'd git object
    yield await packEntry(obj!);                           // type/size varint + zlib body (no deltas: safe, larger)
  }
  yield await sha1Trailer();                               // 20-byte SHA-1 of everything above
}
declare function parseV2Fetch(b: Uint8Array): { wants: string[]; haves: string[] };
declare function sideband(ch: number, b: Uint8Array): Uint8Array;
declare function packHeader(n: number): Uint8Array;
declare function packEntry(o: R2ObjectBody): Promise<Uint8Array>;
declare function sha1Trailer(): Promise<Uint8Array>;
```

## Why it works
- Git protocol v2 is already stateless request/response: a `fetch` command body plus its pkt-line/sideband reply is a self-contained byte string, so it can be carried as one binary WebSocket frame pair with no changes to the negotiation, pack header, or trailer that `git index-pack` verifies.
- The DO is the single ref authority (`repo-do-ref-authority`), so the exact SQLite `UPDATE` that makes a push visible is also the only place a ref can move; fanning out `getWebSockets(refname)` there means zero missed updates and zero polling.
- `getWebSockets` works on tags across hibernation: the socket list is owned by the runtime, not by DO memory, so the push handler sees subscribers even though the DO was evicted between their connect and the push.
- `serializeAttachment` keeps the per-client `have` set through eviction, so the nudge-triggered fetch sends only new objects, exactly like `git fetch` after `have` lines; an unknown client state degrades to a full pack, never to a wrong one.
- `setWebSocketAutoResponse` answers keepalives from the runtime, so thousands of idle subscribers cost no DO wall-clock; the only work is on real pushes.
- Because the nudge carries `{old,new}`, a helper that prefers plain HTTP can just run `git fetch` normally on receipt; the WebSocket is then only a notification channel, which is the fully client-compatible fallback.

## Known limits
- Stock `git` cannot speak this: it needs a remote helper (`git-remote-ws`) or a sidecar that runs `git fetch` on nudge. The in-socket pack path is an optimization; the notification-only path is what works with an unmodified client.
- Pack replies are sent from the DO, so the 128 MB DO memory bound and per-message size limits (1 MiB per WebSocket frame) force chunked `sideband` sends; a large fetch must be spread over many frames and cannot use `Response` streaming backpressure. Big fetches should fall back to HTTP (`precomputed-clone-pack`).
- `ws.send` is fire-and-forget with an outgoing buffer; a slow client on a big pack can exhaust the DO's buffered bytes. Need per-socket flow control (client acks per N frames) which the proof hand-waves.
- Single DO per repo: every subscriber's fetch and every push serialize through one isolate; fan-out to N sockets is O(N) sends on the push path. Fine for hundreds, not tens of thousands (`branch-level-dos` helps).
- Hibernation only happens when no request or alarm is in flight; a repo with continuous pushes never hibernates, so the cost model collapses to a normal always-on DO.
- Auth expiry of long-lived sockets is not handled by the runtime; an alarm has to walk `getWebSockets()` and close stale ones.
- Delta compression is skipped in the proof (loose objects re-emitted as non-delta entries); packs are larger than what `git` would produce. `pinned-delta-bases` / `wasm-git-core` fix that.
- No cross-region replication of the socket: clients far from the DO's home region pay that latency on every nudge; `replicated-refs-edge` does not help since the socket must terminate at the authority.

## Depends on
- repo-do-ref-authority
- refs-sqlite-objects-r2
- protocol-v2-only
- info-refs-endpoint
- want-have-negotiation
- two-phase-push
