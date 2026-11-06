> Idea #28 · edge · verdict: **lands with caveats** · feasibility 4/5 · reliability 3/5 · correctness 3/5 · effort: days
> Proof: [proofs/scoped-token-remotes.md](./scoped-token-remotes.md) · Review: [reviews/scoped-token-remotes.md](../../reviews/archived/scoped-token-remotes.md)

# Rate-limited, token-scoped remote URLs

## Mechanism
The remote URL is `https://git.example/t/<payload>.<hmac>/owner/repo.git`, where `<payload>` is base64url JSON `{id, repo, ref, exp, maxPushes, maxBytes}` and `<hmac>` is HMAC-SHA256 over it with a Worker secret, so the edge Worker verifies scope and expiry with WebCrypto and zero storage lookups. Every git request under that prefix (`info/refs?service=git-receive-pack`, then `POST git-receive-pack`) hits the Worker, which parses the pkt-line command section of the push body *before* touching the PACK and rejects any `<old> <new> <refname>` line whose refname is not the scoped one. If the commands pass, the Worker calls the repo DO (`idFromName(owner/repo)`) over RPC to atomically debit a `token_usage` row in DO SQLite (push count, bytes, revoked flag); only then is the pack streamed to the ingester, and the DO re-checks the same scope inside its ref CAS. A DO alarm set to the earliest `exp` deletes expired usage rows.

## Primitives
- Workers (fetch handler, WebCrypto `crypto.subtle` HMAC-SHA256, Worker secrets via `wrangler secret put`) — GA
- Durable Objects with SQLite storage (`ctx.storage.sql.exec`) — GA
- DO alarms (`ctx.storage.setAlarm` / `alarm()`) — GA
- DO RPC methods on a `DurableObject` subclass (`stub.reserve(...)`) — GA
- Streaming `Request.body` (ReadableStream) forwarded untouched to the pack ingester — GA
- R2 is not touched by this idea directly; the pack ingester it hands off to writes there

## Proof code
```typescript
import { DurableObject } from "cloudflare:workers";

interface Scope { id: string; repo: string; ref: string; exp: number; maxPushes: number; maxBytes: number }
interface Env { REPO: DurableObjectNamespace<RepoDO>; TOKEN_HMAC_KEY: string }
const enc = new TextEncoder();
const b64u = (s: string) => Uint8Array.from(atob(s.replace(/-/g, "+").replace(/_/g, "/")), c => c.charCodeAt(0));

async function verifyToken(seg: string, secret: string): Promise<Scope | null> {
  const [body, sig] = seg.split(".");
  if (!body || !sig) return null;
  const key = await crypto.subtle.importKey("raw", enc.encode(secret), { name: "HMAC", hash: "SHA-256" }, false, ["verify"]);
  if (!(await crypto.subtle.verify("HMAC", key, b64u(sig), enc.encode(body)))) return null;
  const s = JSON.parse(new TextDecoder().decode(b64u(body))) as Scope;
  return s.exp * 1000 > Date.now() ? s : null;
}

/** Read pkt-lines "<old> <new> <ref>\0caps" up to the flush-pkt 0000; leave the PACK unread in the stream. */
async function readCommands(body: ReadableStream<Uint8Array>) {
  const r = body.getReader(); let buf = new Uint8Array(0); const cmds: { old: string; nw: string; ref: string }[] = [];
  for (;;) {
    while (buf.length < 4) { const { value, done } = await r.read(); if (done) throw new Error("eof"); buf = concat(buf, value); }
    const len = parseInt(new TextDecoder().decode(buf.subarray(0, 4)), 16);
    if (len === 0) { buf = buf.subarray(4); break; }                       // flush-pkt: commands end, PACK follows
    while (buf.length < len) { const { value, done } = await r.read(); if (done) throw new Error("eof"); buf = concat(buf, value); }
    const line = new TextDecoder().decode(buf.subarray(4, len)).split("\0")[0].trim();
    const [old, nw, ref] = line.split(" "); cmds.push({ old, nw, ref }); buf = buf.subarray(len);
  }
  r.releaseLock();
  const rest = new ReadableStream({ start(c) { if (buf.length) c.enqueue(buf); }, pull: async c => { const { value, done } = await body.getReader().read(); done ? c.close() : c.enqueue(value); } });
  return { cmds, rest };
}
const concat = (a: Uint8Array, b: Uint8Array) => { const o = new Uint8Array(a.length + b.length); o.set(a); o.set(b, a.length); return o; };
const pkt = (s: string) => (s.length + 4).toString(16).padStart(4, "0") + s;
/** report-status without side-band (we never advertise side-band-64k on token URLs, so band framing is not needed). */
const reportStatus = (lines: string[]) =>
  new Response(pkt("unpack ok\n") + lines.map(l => pkt(l + "\n")).join("") + "0000", { status: 200, headers: { "content-type": "application/x-git-receive-pack-result" } });

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const url = new URL(req.url);
    const m = url.pathname.match(/^\/t\/([^/]+)\/([^/]+\/[^/]+)\.git\/(.*)$/);
    if (!m) return new Response("not found", { status: 404 });
    const scope = await verifyToken(m[1], env.TOKEN_HMAC_KEY);
    // 403, never 401: a 401 makes git prompt for a username/password it does not have.
    if (!scope || scope.repo !== m[2]) return new Response("forbidden", { status: 403 });
    const repo = env.REPO.get(env.REPO.idFromName(scope.repo));
    if (m[3] === "info/refs" && url.searchParams.get("service") === "git-receive-pack")
      return repo.advertiseReceivePack(scope.ref);             // info-refs-endpoint, only the scoped ref + caps "report-status delete-refs"
    if (m[3] === "git-receive-pack" && req.method === "POST") {
      const { cmds, rest } = await readCommands(req.body!);
      const bad = cmds.filter(c => c.ref !== scope.ref);
      if (bad.length) return reportStatus(cmds.map(c => c.ref === scope.ref ? `ng ${c.ref} not pushed (sibling ref rejected)` : `ng ${c.ref} token scoped to ${scope.ref}`));
      const declared = Number(req.headers.get("content-length") ?? 0); // chunked pushes have none; ingester counts bytes too
      const lease = await repo.reserve(scope.id, scope.exp, scope.maxPushes, scope.maxBytes, declared);
      if (!lease.ok) return reportStatus(cmds.map(c => `ng ${c.ref} ${lease.reason}`));
      // Hand off: PACK header + objects stream to the streaming-pack-parser; the DO's CAS re-checks scope.ref & lease.id.
      return repo.ingestPush(cmds, rest, { tokenId: scope.id, onlyRef: scope.ref, maxBytes: scope.maxBytes });
    }
    return new Response("forbidden", { status: 403 });
  },
};

export class RepoDO extends DurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS token_usage(
      id TEXT PRIMARY KEY, exp INTEGER NOT NULL, pushes INTEGER NOT NULL DEFAULT 0,
      bytes INTEGER NOT NULL DEFAULT 0, revoked INTEGER NOT NULL DEFAULT 0)`);
  }
  /** Atomic: DO is single-threaded and sql.exec is synchronous, so read-check-write cannot interleave. */
  async reserve(id: string, exp: number, maxPushes: number, maxBytes: number, bytes: number) {
    const sql = this.ctx.storage.sql;
    sql.exec("INSERT OR IGNORE INTO token_usage(id, exp) VALUES (?, ?)", id, exp);
    const row = sql.exec<{ pushes: number; bytes: number; revoked: number }>("SELECT pushes, bytes, revoked FROM token_usage WHERE id = ?", id).one();
    if (row.revoked) return { ok: false as const, reason: "token revoked" };
    if (row.pushes >= maxPushes) return { ok: false as const, reason: `rate limit: ${maxPushes} pushes per token` };
    if (row.bytes + bytes > maxBytes) return { ok: false as const, reason: "byte budget exceeded" };
    sql.exec("UPDATE token_usage SET pushes = pushes + 1, bytes = bytes + ? WHERE id = ?", bytes, id);
    if ((await this.ctx.storage.getAlarm()) === null) await this.ctx.storage.setAlarm(exp * 1000 + 1000);
    return { ok: true as const };
  }
  async revoke(id: string) { this.ctx.storage.sql.exec("INSERT INTO token_usage(id, exp, revoked) VALUES (?, ?, 1) ON CONFLICT(id) DO UPDATE SET revoked = 1", id, 2 ** 31); }
  async alarm() {
    const sql = this.ctx.storage.sql, now = Math.floor(Date.now() / 1000);
    sql.exec("DELETE FROM token_usage WHERE exp < ? AND revoked = 0", now);
    const next = sql.exec<{ e: number | null }>("SELECT MIN(exp) AS e FROM token_usage WHERE revoked = 0").one().e;
    if (next != null) await this.ctx.storage.setAlarm(next * 1000 + 1000);
  }
  advertiseReceivePack(_ref: string): Promise<Response> { throw new Error("see info-refs-endpoint"); }
  ingestPush(_c: unknown, _s: ReadableStream, _o: unknown): Promise<Response> { throw new Error("see streaming-pack-parser / two-phase-push"); }
}
```

## Why it works
- `git push` to an HTTPS remote does exactly two requests, `GET .../info/refs?service=git-receive-pack` then `POST .../git-receive-pack`; both carry the token because git keeps the full URL from `.git/config` and appends the suffix. Nothing on the client needs to change and no credential helper is involved.
- The receive-pack body is a fixed shape: pkt-lines `<old-sha> <new-sha> <refname>\0capabilities` terminated by a flush-pkt `0000`, then the `PACK` stream. The Worker can therefore evaluate scope after a few hundred bytes and refuse before inflating a single object, so an out-of-scope push costs no R2 writes and no DO CPU.
- Rejection is delivered in the language git already understands: `report-status` (`unpack ok`, `ng <ref> <reason>`, flush). The client prints `! [remote rejected] main -> main (token scoped to refs/heads/feature)` and exits non-zero instead of hanging. Because token URLs never advertise `side-band-64k`, the status does not need band-1 framing.
- Advertising only the scoped ref in `info/refs` is legal: git computes `<old-sha>` from the advertisement, so an unscoped branch simply looks nonexistent and a push to it is caught by the command check anyway. `ok`/`ng` per-ref lines let the DO reject sibling refs while still failing the whole push atomically, which matches `receive-pack`'s default (non-`atomic`) semantics.
- The rate limit is exact, not eventually consistent: one DO per repo (repo-do-ref-authority) plus synchronous `sql.exec` means the read-check-increment in `reserve` cannot interleave with another push using the same token.
- Expiry is enforced twice with no clock skew problem: at the edge from the signed `exp`, and by the alarm that deletes usage rows after `exp`, so a token that is re-presented after expiry both fails the HMAC-time check and finds no budget row.

## Known limits
- The token lives in the URL, so it ends up in `.git/config`, shell history, proxy logs and Worker request logs. Mitigation is short expiry (one hour as stated) and `revoke()`; it cannot be made secret-safe the way an `Authorization` header can.
- `Content-Length` is absent when git switches to chunked transfer for pushes above `http.postBuffer` (1 MiB default), so the byte budget for large pushes must be enforced by the pack ingester counting bytes as it streams, and the reservation above only sees `0`. A malicious client can therefore stream up to `maxBytes` before being cut off; it cannot exceed it.
- A push that starts before `exp` and finishes after it is accepted; the DO CAS could re-check `exp` but then legitimate slow pushes over slow links would be lost at the last step. The proof accepts the request-start check.
- Workers CPU limit (30 s default, more with `limits.cpu_ms`) applies to the Worker doing the command parse and the handoff, which is trivial; the pack ingest cost belongs to streaming-pack-parser. DO memory (128 MB) is irrelevant here because `token_usage` rows are a few dozen bytes.
- Single-DO throughput: every push to the repo, tokened or not, serializes through the repo DO, so a burst of thousands of tokened pushes per second is bounded by the DO, not by the limiter. That is the intended design (repo-do-ref-authority), not a limiter artifact.
- Token minting (`POST /owner/repo/tokens` behind normal auth) and the `revoke` endpoint are hand-waved; they are a straight application of auth-and-multitenancy. Secret rotation invalidates every outstanding token unless a `kid` is added to the payload and two keys are accepted during rollover.
- Push is protocol v0 `receive-pack` even when `protocol.version=2` is set (v2 has no push command as of git 2.4x), so this proof does not need protocol-v2-only, but it does need the pkt-line codec from info-refs-endpoint.

## Depends on
- info-refs-endpoint
- auth-and-multitenancy
- repo-do-ref-authority
- streaming-pack-parser
- two-phase-push
