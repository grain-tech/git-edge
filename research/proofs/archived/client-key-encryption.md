> Idea #37 · wild · verdict: **risky** · feasibility 4/5 · reliability 2/5 · correctness 3/5 · effort: weeks
> Proof: [proofs/client-key-encryption.md](./client-key-encryption.md) · Review: [reviews/client-key-encryption.md](../../reviews/archived/client-key-encryption.md)

# Encrypted-at-rest with client-held keys

## Mechanism
A stock `git push https://` cannot do this: git-receive-pack hands the server a plaintext packfile, and the server must inflate it to know what it holds. So the wire protocol changes at the client edge: a small remote helper (`git-remote-edge`, same shape as `git-remote-gcrypt`) or the browser client from `offline-browser-client` encrypts every loose object with a repo key that never leaves the client, `PUT`s each ciphertext to the Worker, which streams it straight into R2 under `<repo>/enc/<oid>`; then it `POST`s a hash-only manifest (ref CAS triples, commit oids with parent oids, and the oids each commit introduced) to the per-repo DO. The DO stores that manifest in SQLite (`refs`, `commits`) and does connectivity + compare-and-swap purely in hash space; on fetch it walks the `commits` table from wants to haves and returns oid lists, and the client pulls ciphertexts from R2 and decrypts them locally. Neither the Worker nor the DO ever holds a key or a plaintext byte; the only things the server learns are ref names, object ids and the commit DAG shape.

## Primitives
- Durable Objects with SQLite storage (`ctx.storage.sql`, `transactionSync`) — GA
- DO alarms (`ctx.storage.setAlarm`) for sweeping ciphertexts that never got referenced by a push — GA
- R2 `put` with a streamed request body, `head`, `get` — GA
- Worker `fetch` handler as the stateless upload/download edge in front of R2 — GA
- WebCrypto (`crypto.subtle` AES-GCM + HMAC) on the client side; the same API exists in Workers but is deliberately not used there — GA

## Proof code
```typescript
// ---------- client side: runs wherever the key lives (git-remote-edge helper) ----------
// `git push edge::https://git-edge.dev/o/acme/app main` invokes this instead of git-receive-pack.
type Keys = { aes: CryptoKey; mac: CryptoKey };            // HKDF(repoKey) -> AES-256-GCM key + HMAC-SHA256 key
const enc = (s: string) => new TextEncoder().encode(s);

async function seal(k: Keys, oid: string, type: string, plain: Uint8Array): Promise<Uint8Array> {
  // Deterministic nonce per (repo, oid). oid == sha1(type+len+plain), so equal plaintext -> equal
  // ciphertext (idempotent retries, dedup) and distinct plaintext never reuses a nonce under this key.
  const iv = new Uint8Array(await crypto.subtle.sign("HMAC", k.mac, enc(oid))).slice(0, 12);
  const header = enc(`${type} ${plain.length}\0`);                       // same header git hashes
  const ct = await crypto.subtle.encrypt({ name: "AES-GCM", iv, additionalData: enc(oid) }, k.aes,
    new Uint8Array([...header, ...plain]));
  return new Uint8Array([...iv, ...new Uint8Array(ct)]);
}

async function pushBranch(k: Keys, base: string, ref: string, oldOid: string, newOid: string) {
  const objs = sh(`git rev-list --objects ${newOid} ^${oldOid}`);         // [oid, path] lines
  for (const oid of objs) {                                                // one PUT per object; Worker streams to R2
    const type = sh(`git cat-file -t ${oid}`)[0];
    await fetch(`${base}/objects/${oid}`, { method: "PUT", body: await seal(k, oid, type, shBytes(`git cat-file ${type} ${oid}`)) });
  }
  const commits = sh(`git rev-list ${newOid} ^${oldOid}`).map(c => ({
    oid: c,
    parents: sh(`git rev-list --parents -n1 ${c}`)[0].split(" ").slice(1),
    introduced: sh(`git rev-list --objects ${c} ^${c}^@`),                 // objects first seen in c, hashes only
  }));
  const r = await fetch(`${base}/push`, { method: "POST",
    body: JSON.stringify({ updates: [{ name: ref, old: oldOid, new: newOid }], commits }) });
  if (!r.ok) throw new Error(await r.text());                              // helper prints "error <ref> fetch first"
}

// ---------- Worker: stateless edge, streams ciphertext to/from R2, routes manifests to the repo DO ----------
export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const m = new URL(req.url).pathname.match(/^\/o\/([^/]+\/[^/]+)\/(?:objects\/([0-9a-f]{40})|(push|fetch|refs))$/);
    if (!m) return new Response("not found", { status: 404 });
    const [, repo, oid] = m;
    if (oid) {
      const key = `${repo}/enc/${oid}`;                                    // R2 stores only the sealed envelope
      if (req.method === "PUT") { await env.BUCKET.put(key, req.body); return new Response(null, { status: 201 }); }
      const obj = await env.BUCKET.get(key);
      return obj ? new Response(obj.body, { headers: { "content-type": "application/octet-stream" } }) : new Response(null, { status: 404 });
    }
    return env.REPO.get(env.REPO.idFromName(repo)).fetch(req);
  },
};

// ---------- Durable Object: ref authority that only ever sees hashes ----------
export class EncryptedRepo extends DurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS refs    (name TEXT PRIMARY KEY, oid TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS commits (oid TEXT PRIMARY KEY, parents TEXT NOT NULL, introduced TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS pending (oid TEXT PRIMARY KEY, at INTEGER NOT NULL)`);
  }
  async fetch(req: Request): Promise<Response> {
    const sql = this.ctx.storage.sql, url = new URL(req.url), repo = url.pathname.split("/").slice(2, 4).join("/");
    if (url.pathname.endsWith("/refs"))
      return Response.json(sql.exec("SELECT name, oid FROM refs ORDER BY name").toArray());
    if (url.pathname.endsWith("/push")) {
      const { updates, commits } = await req.json() as any;
      const inPush = new Set<string>(commits.map((c: any) => c.oid));
      for (const c of commits) {
        for (const o of c.introduced)                                       // ciphertext must exist; content never read
          if (!(await this.env.BUCKET.head(`${repo}/enc/${o}`))) return new Response(`missing ${o}`, { status: 400 });
        for (const p of c.parents)                                          // connectivity check in hash space
          if (!inPush.has(p) && !sql.exec("SELECT 1 FROM commits WHERE oid=?", p).toArray().length)
            return new Response(`unreachable parent ${p}`, { status: 400 });
      }
      try {
        this.ctx.storage.transactionSync(() => {                            // atomic: all refs or none
          for (const c of commits) sql.exec("INSERT OR IGNORE INTO commits VALUES (?,?,?)", c.oid, c.parents.join(" "), c.introduced.join(" "));
          for (const u of updates) {
            const cur = sql.exec("SELECT oid FROM refs WHERE name=?", u.name).toArray()[0]?.oid ?? "0".repeat(40);
            if (cur !== u.old) throw new Error(`${u.name} fetch first`);   // same failure git prints for a stale push
            sql.exec("INSERT OR REPLACE INTO refs VALUES (?,?)", u.name, u.new);
          }
        });
      } catch (e) { return new Response(String(e), { status: 409 }); }
      await this.ctx.storage.setAlarm(Date.now() + 15 * 60_000);          // sweep `pending` uploads never claimed by a push
      return Response.json({ ok: updates.map((u: any) => u.name) });
    }
    if (url.pathname.endsWith("/fetch")) {                                  // want/have negotiation over the DAG of hashes
      const { wants, haves } = await req.json() as any;
      const stop = new Set<string>(haves), q: string[] = [...wants], objects: string[] = [];
      while (q.length) {
        const o = q.pop()!; if (stop.has(o)) continue; stop.add(o);
        const row = sql.exec("SELECT parents, introduced FROM commits WHERE oid=?", o).toArray()[0] as any;
        if (!row) continue;
        objects.push(o, ...row.introduced.split(" ").filter(Boolean));
        q.push(...row.parents.split(" ").filter(Boolean));
      }
      return Response.json({ objects });                                    // client GETs each, decrypts, `git hash-object -w`
    }
    return new Response("not found", { status: 404 });
  }
  async alarm() { /* DELETE FROM pending WHERE at < now-15min and R2 delete those enc/<oid> keys (two-phase-push janitor) */ }
}
```

## Why it works
- Git's server-side invariants are all expressible over hashes: a ref update is a compare-and-swap on an oid, and "connectivity" is "every parent oid is known". The DO enforces both from the manifest without ever opening an object, and returns the same `fetch first` / non-fast-forward failure a stale push gets from git-receive-pack.
- Object ids remain sha1 of the plaintext (`type len\0body`), computed by git on the client. On fetch the client decrypts and runs `git hash-object -w -t <type>`, which recomputes the id; a tampered or mis-keyed ciphertext fails AES-GCM authentication (oid is bound in as AAD) or produces a different id, so the server cannot silently substitute objects.
- Deterministic nonces derived from HMAC(key, oid) make sealing idempotent, so a retried push re-uploads byte-identical ciphertext and `content-addressed-r2-keys` idempotency still holds.
- The `introduced` list per commit is exactly what `git rev-list --objects c ^c^@` yields, so a fetch is one DO round trip for the oid closure plus N R2 GETs; no server-side tree walking is required and the server never needs to parse a tree.
- Refs and the commit DAG stay in DO SQLite, which is the same shape as `refs-sqlite-objects-r2` and `want-have-negotiation`; encryption changes only what R2 holds and who may read it.

## Known limits
- Not compatible with unmodified `git push`/`git fetch` over smart HTTP. The idea as stated ("objects encrypted before reaching R2") requires a client-side component: a remote helper (as git-remote-gcrypt does) or the shared Wasm core in a browser. Stock clients would need server-side encryption, which is not client-held keys.
- No packfiles: each object is a separate R2 PUT/GET (Class A/B request costs) and there is no delta compression between objects, so clone of a large history is many requests. Batching several sealed objects into one R2 object with a client-side offset index is the obvious fix and is still hash-only for the server.
- The server learns metadata: ref names, object ids, commit parent edges, per-commit object counts and ciphertext sizes. Ref names could be HMACed and sizes padded; this proof does not do either. Anyone who knows the plaintext of a file can confirm its presence from its oid (convergent encryption within one repo key).
- The DO trusts the manifest: a key holder can claim `introduced` objects that are not the true closure of the commit, leaving a fetcher with missing objects. Only key holders can push, so this is a self-inflicted corruption, not a server compromise, but the server cannot fsck.
- `BUCKET.head` per introduced object inside the push handler is serial and sits in the single repo DO; a push of 50k objects would blow well past comfortable request time. Move existence checks to the Worker on PUT (insert into `pending` via DO RPC) and let the push only verify against that table.
- Key management (rotation, sharing with collaborators, CI) is entirely outside the server and is hand-waved here; re-keying means re-sealing every object.
- Server-side features that need plaintext (`server-side-merge`, `search-index-on-push`, `diff-api-range-reads`, `semantic-diffs`) are impossible on an encrypted repo.

## Depends on
- repo-do-ref-authority
- refs-sqlite-objects-r2
- content-addressed-r2-keys
- two-phase-push
- want-have-negotiation
- offline-browser-client (or an equivalent remote helper; needed for any real client)
