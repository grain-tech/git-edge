> Idea #54 · foundation · verdict: **lands with caveats** · feasibility 5/5 · reliability 4/5 · correctness 4/5 · effort: days
> Proof: [proofs/auth-and-multitenancy.md](./auth-and-multitenancy.md) · Review: [reviews/auth-and-multitenancy.md](../../reviews/archived/auth-and-multitenancy.md)

# Auth and multi-tenancy: owner/repo routing to DO ids

## Mechanism
Every smart-HTTP request (`/:owner/:repo.git/info/refs?service=…`, `/git-upload-pack`, `/git-receive-pack`) hits one stateless Worker that parses `owner/repo`, verifies the HTTP Basic credential git sends (an HMAC-signed personal access token, checked with `crypto.subtle` at the edge with zero storage round-trips), and forwards to the repo's Durable Object obtained via `env.REPO.idFromName("owner/repo")`. The DO owns a SQLite `acl(principal, role)` table plus a `meta` row (visibility); on each request it decides `read`/`write`/`deny` for the verified principal before touching refs, and answers `401` with `WWW-Authenticate: Basic` or `403` in the exact places git expects. No global user database is consulted on the hot path: the token proves identity, the DO proves authorization, and the DO name *is* the tenancy boundary (one SQLite database and one R2 key prefix `objects/<owner>/<repo>/…` per repo).

## Primitives
- Workers (fetch handler, `crypto.subtle` HMAC-SHA256 verification) - GA
- Durable Objects with SQLite storage (`ctx.storage.sql.exec`, `new_sqlite_classes` migration) - GA
- `DurableObjectNamespace.idFromName` (deterministic id from `owner/repo`), optionally `jurisdiction("eu")` for data-residency tenants - GA
- R2 (only for the per-repo key prefix; not exercised by this proof) - GA
- Workers KV for a token revocation list / owner-rename alias table - GA (optional)

## Proof code
```typescript
// wrangler.jsonc: durable_objects.bindings [{name:"REPO",class_name:"RepoDO"}],
// migrations [{tag:"v1",new_sqlite_classes:["RepoDO"]}], secret TOKEN_SECRET
import { DurableObject } from "cloudflare:workers";

interface Env { REPO: DurableObjectNamespace<RepoDO>; TOKEN_SECRET: string }
type Role = "admin" | "write" | "read";
const ROUTE = /^\/([a-z0-9][a-z0-9-]{0,38})\/([a-z0-9._-]{1,100}?)(?:\.git)?\/(info\/refs|git-upload-pack|git-receive-pack)$/i;

// Token = "ge_" + base64url(payload) + "." + base64url(HMAC(payload)); payload = "user:expiryEpoch".
// Verified at the edge with no storage read; git sends it as Basic <base64(user:token)>.
async function verifyToken(env: Env, auth: string | null): Promise<string | null> {
  if (!auth?.startsWith("Basic ")) return null;
  const [user, tok] = atob(auth.slice(6)).split(":", 2);
  if (!tok?.startsWith("ge_")) return null;
  const [payload, sig] = tok.slice(3).split(".");
  const key = await crypto.subtle.importKey("raw", new TextEncoder().encode(env.TOKEN_SECRET),
    { name: "HMAC", hash: "SHA-256" }, false, ["verify"]);
  const ok = await crypto.subtle.verify("HMAC", key, b64u(sig), b64u(payload));
  const [subject, exp] = new TextDecoder().decode(b64u(payload)).split(":");
  return ok && subject === user && Number(exp) > Date.now() / 1000 ? subject : null;
}
const b64u = (s: string) => Uint8Array.from(atob(s.replace(/-/g, "+").replace(/_/g, "/")), c => c.charCodeAt(0));
const challenge = () => new Response("auth required", { status: 401,
  headers: { "WWW-Authenticate": 'Basic realm="git-edge"', "cache-control": "no-store" } });

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const url = new URL(req.url);
    const m = url.pathname.match(ROUTE);
    if (!m) return new Response("not found", { status: 404 });
    const [, owner, repo, op] = m;
    const service = op === "info/refs" ? url.searchParams.get("service") : op;
    if (service !== "git-upload-pack" && service !== "git-receive-pack") return new Response("dumb http unsupported", { status: 403 });

    const principal = await verifyToken(env, req.headers.get("authorization")); // null = anonymous
    if (req.headers.get("authorization") && !principal) return challenge();      // bad/expired token -> retry prompt
    // Pushes must be challenged on info/refs already: git cannot rewind a streamed pack body on a late 401.
    if (service === "git-receive-pack" && !principal) return challenge();

    const name = `${owner}/${repo}`.toLowerCase();               // tenancy key == DO name
    const stub = env.REPO.get(env.REPO.idFromName(name));         // same name -> same DO, globally
    const inner = new Request(`https://repo/${op}${url.search}`, { method: req.method, headers: req.headers, body: req.body });
    inner.headers.set("x-git-principal", principal ?? "");
    inner.headers.set("x-git-service", service);
    inner.headers.set("x-repo-name", name);
    return stub.fetch(inner);
  },
} satisfies ExportedHandler<Env>;

export class RepoDO extends DurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.blockConcurrencyWhile(async () => ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS acl  (principal TEXT PRIMARY KEY, role TEXT NOT NULL CHECK(role IN ('admin','write','read')));
      CREATE TABLE IF NOT EXISTS refs (name TEXT PRIMARY KEY, oid TEXT NOT NULL);`));
  }

  private authorize(principal: string, service: string): "ok" | 401 | 403 {
    const vis = this.ctx.storage.sql.exec<{ v: string }>("SELECT v FROM meta WHERE k='visibility'").toArray()[0]?.v;
    if (vis === undefined) return 403;                            // repo not created -> do not leak existence
    const role = principal
      ? this.ctx.storage.sql.exec<{ role: Role }>("SELECT role FROM acl WHERE principal=?", principal).toArray()[0]?.role
      : undefined;
    if (service === "git-upload-pack") return vis === "public" || role ? "ok" : principal ? 403 : 401;
    return role === "write" || role === "admin" ? "ok" : principal ? 403 : 401;
  }

  async fetch(req: Request): Promise<Response> {
    const principal = req.headers.get("x-git-principal") ?? "";
    const service = req.headers.get("x-git-service")!;
    const verdict = this.authorize(principal, service);
    if (verdict === 401) return challenge();
    if (verdict === 403) return new Response("forbidden", { status: 403 });
    // Authorized: hand off to the protocol layer (info-refs-endpoint / streaming-pack-parser).
    // e.g. info/refs -> "001e# service=git-receive-pack\n0000" + pkt-line ref advertisement from `refs`.
    return new Response(`ok ${service} for ${principal || "anonymous"}`);
  }

  // Admin RPC (called from a separate /api route, itself gated on role='admin'):
  async create(owner: string, visibility: "public" | "private") {
    this.ctx.storage.sql.exec("INSERT OR REPLACE INTO meta VALUES ('visibility',?)", visibility);
    this.ctx.storage.sql.exec("INSERT OR REPLACE INTO acl VALUES (?, 'admin')", owner);
  }
  async grant(principal: string, role: Role) { this.ctx.storage.sql.exec("INSERT OR REPLACE INTO acl VALUES (?,?)", principal, role); }
}
```

## Why it works
- git's HTTP transport sends the first request anonymously and only attaches credentials (from `credential.helper`) after a `401` carrying `WWW-Authenticate: Basic`; the same credential is reused for the follow-up `POST /git-upload-pack` or `/git-receive-pack`. Returning the challenge from the DO for private reads, and from the edge for every push, matches that dance exactly.
- Challenging `info/refs?service=git-receive-pack` before authorization (even on public repos) is required, not cosmetic: git streams a chunked pack body in the POST, and a late `401` on that POST makes it fail with "RPC failed" because the body is not rewindable (`http.postBuffer` only buffers small pushes).
- `403` versus `401` matters: git treats `403` as terminal ("The requested URL returned error: 403"), while `401` triggers the credential prompt/retry loop, so a valid-token-wrong-repo case must be `403` to avoid an endless prompt.
- `idFromName("owner/repo")` is deterministic and global, so every colo routes the same repo to the same single-writer DO; that is the same DO `repo-do-ref-authority` serializes ref CAS in, so ACL, refs and visibility live in one SQLite database and are checked in the same transaction context with no extra hops.
- Token verification is stateless HMAC at the edge (`crypto.subtle`), so an invalid token is rejected in the Worker without waking a DO; the DO only sees an already-authenticated principal string and does one indexed SQLite lookup per request.
- Tenancy isolation is structural: a DO cannot read another DO's SQLite, and object keys are prefixed `objects/<owner>/<repo>/`, so a bug in one repo's handler cannot reach another tenant's refs or objects (the `global-dedup` idea deliberately breaks this and must add its own per-object read checks).

## Known limits
- Renames and transfers are not free: `idFromName("owner/repo")` is immutable, so renaming a repo needs an alias table (KV `alias:<old>` -> hex DO id, resolved with `idFromString`) and a redirect; the proof above does not implement it.
- Stateless HMAC tokens cannot be revoked before expiry without a revocation list (KV lookup per request, ~ms, eventually consistent up to 60s). Short-lived tokens (hours) plus KV blacklisting is the practical shape; true instant revocation needs a per-user DO hop.
- Repo existence is only known to the DO, so a request for a non-existent repo still wakes a fresh DO (empty SQLite, creation cost) before returning 403; abusive enumeration can create many empty DOs. Mitigate with a KV "exists" bitmap at the edge or a Cloudflare rate limit rule.
- Single-DO throughput: every request, including anonymous clones of a public repo, funnels through one DO (~hundreds of req/s). Hot public repos need `replicated-refs-edge` for read fan-out; the ACL check itself is trivial.
- The Worker-side regex pins owner to GitHub-like rules (39 chars) and lowercases the name; case-insensitive collisions ("Foo/bar" vs "foo/bar") are therefore the same tenant by design.
- Org/team membership and SSO are hand-waved: the ACL is flat per repo. Group roles would need an owner-level DO that the repo DO consults (one extra RPC) or a periodic alarm that syncs group membership into each repo's `acl` table.
- `DurableObject` base class with typed RPC methods (`create`, `grant`) requires `compatibility_date >= 2024-04-03`; the existing repo uses `implements DurableObject` and fetch-only, both styles work.

## Depends on
repo-do-ref-authority, info-refs-endpoint
