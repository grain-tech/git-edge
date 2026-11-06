> Idea #19 · edge · verdict: **lands with caveats** · feasibility 4/5 · reliability 2/5 · correctness 3/5 · effort: weeks
> Proof: [proofs/signed-reflog.md](./signed-reflog.md) · Review: [reviews/signed-reflog.md](../../reviews/archived/signed-reflog.md)

# Signed refs by default with append-only DO reflog

## Mechanism
`POST /:owner/:repo/git-receive-pack` lands on the Worker, which parses the command section (`<old> <new> <refname>\0report-status push-cert=<nonce>...`), streams the packfile to R2 as in `two-phase-push`, then calls `repoDO.updateRefs(cmds, pushCert?)`. Inside the repo DO, one `transactionSync` per push does the compare-and-swap on the `refs` table and, for every ref that moved, inserts a row into a `reflog` table whose `entry_hash` is `SHA-256(prev_entry_hash || canonical entry bytes)` and whose `sig` is an Ed25519 signature (WebCrypto) over that hash with a per-repo key generated on first use and kept in the same SQLite storage; SQLite `BEFORE UPDATE`/`BEFORE DELETE` triggers make the table append-only for application code. If the client used `git push --signed`, the raw push certificate (`certificate version 0.1`, pusher, pushee, nonce, commands, GPG/SSH signature) is stored verbatim on the row; the server verifies the nonce it advertised and, for `ssh-ed25519` certificates, the signature itself. A DO alarm periodically anchors the log head (`seq`, `entry_hash`, `sig`) to R2 at `reflog/anchor/<seq>.json` so a wiped or tampered DO is detectable, and `GET /:owner/:repo/reflog?ref=refs/heads/main` returns the chain plus the repo public key so anyone can re-verify it offline.

## Primitives
- Workers (`crypto.subtle` with `Ed25519` generateKey/sign/verify and `SHA-256` digest) — GA (Ed25519 has been in Workers WebCrypto since 2023)
- Durable Objects with SQLite storage (`ctx.storage.sql.exec`, `ctx.storage.transactionSync`, SQLite triggers) — GA
- DO alarms (`ctx.storage.setAlarm`, `alarm()`) for periodic R2 anchoring — GA
- DO RPC (`DurableObject` from `cloudflare:workers`, typed stub methods) — GA
- R2 `put` for anchors (tiny JSON objects; optionally under an object-versioned bucket, see `r2-versioned-snapshots`) — GA
- Nothing beta is required.

## Proof code
```typescript
import { DurableObject } from "cloudflare:workers";
type Env = { REPO: DurableObjectNamespace<RepoDO>; BUCKET: R2Bucket };
type Cmd = { old: string; new: string; ref: string };
type Result = { ref: string; ok: boolean; reason?: string };
const ANCHOR_EVERY_MS = 60_000;
const enc = (s: string) => new TextEncoder().encode(s);
const hex = (b: ArrayBuffer) => [...new Uint8Array(b)].map(x => x.toString(16).padStart(2, "0")).join("");

export class RepoDO extends DurableObject<Env> {
  private key!: CryptoKeyPair;
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS refs   (name TEXT PRIMARY KEY, sha TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS meta   (k TEXT PRIMARY KEY, v TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS reflog (
        seq        INTEGER PRIMARY KEY AUTOINCREMENT,
        ref        TEXT NOT NULL, old_sha TEXT NOT NULL, new_sha TEXT NOT NULL,
        pusher     TEXT NOT NULL, ts INTEGER NOT NULL,
        prev_hash  TEXT NOT NULL, entry_hash TEXT NOT NULL UNIQUE, sig TEXT NOT NULL,
        push_cert  TEXT);                                   -- raw "push-cert" block from git push --signed, if any
      CREATE INDEX IF NOT EXISTS reflog_ref ON reflog(ref, seq);
      CREATE TRIGGER IF NOT EXISTS reflog_ro_u BEFORE UPDATE ON reflog BEGIN SELECT RAISE(ABORT, 'reflog is append-only'); END;
      CREATE TRIGGER IF NOT EXISTS reflog_ro_d BEFORE DELETE ON reflog BEGIN SELECT RAISE(ABORT, 'reflog is append-only'); END;`);
    ctx.blockConcurrencyWhile(async () => { this.key = await this.loadOrCreateKey(); });
  }

  private async loadOrCreateKey(): Promise<CryptoKeyPair> {
    const row = this.ctx.storage.sql.exec<{ v: string }>("SELECT v FROM meta WHERE k='ed25519_jwk'").toArray()[0];
    const alg = { name: "Ed25519" };
    if (row) {                                              // JWK round-trip: DO storage keeps bytes, WebCrypto keeps keys
      const { priv, pub } = JSON.parse(row.v);
      return { privateKey: await crypto.subtle.importKey("jwk", priv, alg, false, ["sign"]),
               publicKey:  await crypto.subtle.importKey("jwk", pub,  alg, true,  ["verify"]) };
    }
    const kp = await crypto.subtle.generateKey(alg, true, ["sign", "verify"]) as CryptoKeyPair;
    const jwk = { priv: await crypto.subtle.exportKey("jwk", kp.privateKey), pub: await crypto.subtle.exportKey("jwk", kp.publicKey) };
    this.ctx.storage.sql.exec("INSERT INTO meta (k, v) VALUES ('ed25519_jwk', ?)", JSON.stringify(jwk));
    return kp;
  }

  // Nonce advertised as `push-cert=<nonce>` in the receive-pack capability line; git echoes it inside the cert.
  nonce(): string { const n = `${Date.now()}-${crypto.randomUUID()}`; this.ctx.storage.sql.exec("INSERT OR REPLACE INTO meta VALUES ('nonce', ?)", n); return n; }

  async updateRefs(cmds: Cmd[], pusher: string, pushCert?: string): Promise<Result[]> {
    if (pushCert) {                                         // "certificate version 0.1\npusher ..\npushee ..\nnonce <n>\n\n<cmds>\n-----BEGIN ..."
      const want = this.ctx.storage.sql.exec<{ v: string }>("SELECT v FROM meta WHERE k='nonce'").toArray()[0]?.v;
      if (!pushCert.includes(`\nnonce ${want}\n`)) return cmds.map(c => ({ ref: c.ref, ok: false, reason: "bad push-cert nonce" }));
      if (!(await verifyPushCert(pushCert))) return cmds.map(c => ({ ref: c.ref, ok: false, reason: "push-cert signature invalid" }));
    }
    const sql = this.ctx.storage.sql, ts = Date.now(), results: Result[] = [];
    // Sign outside the transaction is impossible (hash depends on prev row), so sign first then apply atomically:
    // read head, compute all entries in memory, then CAS + insert in one transactionSync. Single DO => no interleaving.
    let prev = sql.exec<{ entry_hash: string }>("SELECT entry_hash FROM reflog ORDER BY seq DESC LIMIT 1").toArray()[0]?.entry_hash ?? "0".repeat(64);
    const rows: any[] = [];
    for (const c of cmds) {
      const cur = sql.exec<{ sha: string }>("SELECT sha FROM refs WHERE name=?", c.ref).toArray()[0]?.sha ?? "0".repeat(40);
      if (cur !== c.old) { results.push({ ref: c.ref, ok: false, reason: "fetch first" }); continue; }   // CAS failure, git's own wording
      const canon = `${c.ref}\n${c.old}\n${c.new}\n${pusher}\n${ts}\n${prev}\n`;
      const entryHash = hex(await crypto.subtle.digest("SHA-256", enc(canon)));
      const sig = hex(await crypto.subtle.sign("Ed25519", this.key.privateKey, enc(entryHash)));
      rows.push([c, prev, entryHash, sig]); prev = entryHash; results.push({ ref: c.ref, ok: true });
    }
    this.ctx.storage.transactionSync(() => {
      for (const [c, p, h, s] of rows) {
        if (c.new === "0".repeat(40)) sql.exec("DELETE FROM refs WHERE name=?", c.ref);
        else sql.exec("INSERT INTO refs (name, sha) VALUES (?, ?) ON CONFLICT(name) DO UPDATE SET sha=excluded.sha", c.ref, c.new);
        sql.exec("INSERT INTO reflog (ref, old_sha, new_sha, pusher, ts, prev_hash, entry_hash, sig, push_cert) VALUES (?,?,?,?,?,?,?,?,?)",
          c.ref, c.old, c.new, pusher, ts, p, h, s, pushCert ?? null);
      }
    });
    if (rows.length && (await this.ctx.storage.getAlarm()) === null) await this.ctx.storage.setAlarm(Date.now() + ANCHOR_EVERY_MS);
    return results;
  }

  async alarm() {                                           // anchor the head outside the DO so a wipe/rewrite is detectable
    const head = this.ctx.storage.sql.exec<{ seq: number; entry_hash: string; sig: string }>("SELECT seq, entry_hash, sig FROM reflog ORDER BY seq DESC LIMIT 1").toArray()[0];
    if (head) await this.env.BUCKET.put(`reflog/anchor/${String(head.seq).padStart(12, "0")}.json`, JSON.stringify({ ...head, ts: Date.now() }));
  }

  async reflog(ref?: string) {                              // GET /:owner/:repo/reflog — chain + pubkey; verifiable offline
    const rows = ref ? this.ctx.storage.sql.exec("SELECT * FROM reflog WHERE ref=? ORDER BY seq", ref).toArray()
                     : this.ctx.storage.sql.exec("SELECT * FROM reflog ORDER BY seq").toArray();
    return { publicKey: await crypto.subtle.exportKey("jwk", this.key.publicKey), entries: rows };
  }
}

// Client-side verification of one server-signed entry (same code a CLI would run):
export async function verifyEntry(pubJwk: JsonWebKey, e: { ref: string; old_sha: string; new_sha: string; pusher: string; ts: number; prev_hash: string; entry_hash: string; sig: string }) {
  const canon = `${e.ref}\n${e.old_sha}\n${e.new_sha}\n${e.pusher}\n${e.ts}\n${e.prev_hash}\n`;
  if (hex(await crypto.subtle.digest("SHA-256", enc(canon))) !== e.entry_hash) return false;
  const pub = await crypto.subtle.importKey("jwk", pubJwk, { name: "Ed25519" }, false, ["verify"]);
  return crypto.subtle.verify("Ed25519", pub, Uint8Array.from(e.sig.match(/../g)!.map(x => parseInt(x, 16))), enc(e.entry_hash));
}

// push-cert: only ssh-ed25519 "SSHSIG" certificates are verified natively (parse the armored blob, check the
// namespace "git", SHA-512 of the cert body, Ed25519 verify against a key from the repo ACL). GPG certs are
// stored but not verified without a Wasm OpenPGP implementation — see Known limits.
declare function verifyPushCert(cert: string): Promise<boolean>;
```

## Why it works
- git-receive-pack's ref update is a compare-and-swap on `<old> <new> <refname>`; because the single repo DO serializes every push (`repo-do-ref-authority`), the reflog's `prev_hash` chain is a true total order of ref moves with no gaps or races, which is what makes a hash chain meaningful.
- The row schema is exactly git's own reflog line (`<old> <new> <committer> <ts> <msg>`) plus `prev_hash`/`sig`; `git reflog`-style queries (`ORDER BY seq` per ref) and `refs/at/<ts>` resolution (`time-travel-refs`) fall out of the same table.
- `git push --signed` is real protocol: the server advertises `push-cert=<nonce>` in its capability line, the client sends a `push-cert` pkt-line block before the pack, and expects `report-status` to say `ng <ref> <reason>` on rejection. Storing the certificate verbatim on the row gives a client-attested record; nonce echo defends against replay of an old certificate, which is what git's `receive.certNonceSeed` is for.
- Signing over `entry_hash` (which covers `prev_hash`) means one Ed25519 verification per entry and one SHA-256 chain walk proves the whole history; a verifier only needs the public key from `/reflog` and can run `verifyEntry` in any WebCrypto runtime, including the browser.
- SQLite triggers reject `UPDATE`/`DELETE` on `reflog` inside the DO, so no code path (including a buggy GC or a server-side rebase) can silently rewrite history; the CAS on `refs` and the `INSERT` into `reflog` are in one `transactionSync`, so a ref can never move without its signed record.
- Force-pushes and deletions are ordinary rows (`old != ancestor(new)`, `new = 0{40}`), so the log is a complete audit of non-fast-forward events, which is the thing GitHub's opaque reflog does not give users.

## Known limits
- "Append-only" is enforced against application code, not against the operator: whoever can run SQL in the DO can `DROP TRIGGER` and rewrite rows, and the private key sits in the same storage. The R2 anchor (alarm, every ~60s) makes rewrites detectable, not impossible; for real non-repudiation the anchor should go to a versioned R2 bucket (`r2-versioned-snapshots`) or a transparency log, and the signing key should live in a separate KMS-like DO or a Worker secret rather than in repo storage. The proof code hand-waves this.
- Per-repo key generated on first use means there is no key rotation story; rotating requires a `meta` row per key id and the entry recording which key signed it (one extra column, not shown).
- `git push --signed` verification: ssh-ed25519 SSHSIG certificates can be verified with WebCrypto (Ed25519 + SHA-512), but GPG certificates need an OpenPGP implementation (Wasm, ~1MB) and RSA/ECDSA SSHSIG need the corresponding WebCrypto algorithms plus SSH wire-format parsing; the proof declares `verifyPushCert` and does not implement it. Without it the push-cert is stored, nonce-checked, but not signature-checked.
- The nonce is stored as a single `meta` row, so two concurrent `info/refs` advertisements race; git's real scheme derives the nonce from `receive.certNonceSeed` + timestamp and tolerates `certNonceSlop` seconds of skew, which is what a production version should do (stateless HMAC nonce keyed from a Worker secret).
- Ed25519 sign + SHA-256 per ref is microseconds, but a push touching thousands of refs (tag mirrors) does thousands of async WebCrypto calls before `transactionSync`; that is fine for CPU (well under the 30s Worker limit, and DO requests have no separate CPU cap beyond isolate limits) but it is O(refs) sequential awaits. Single-DO throughput remains the `repo-do-ref-authority` ceiling (roughly hundreds of pushes/s), not the signing.
- Reflog rows are ~400 bytes; DO SQLite is capped at 10 GB per object, so ~25M ref moves before the table must be rolled to R2 (an alarm can export and, with the trigger dropped inside a migration, prune, which the design otherwise forbids).
- R2 anchor writes are one Class A op per minute per active repo, negligible; the `/reflog` endpoint returning the full chain for a busy repo needs pagination by `seq`, not shown.
- The signature covers the ref transition, not the objects: a signed reflog says "the server moved main from A to B at t on behalf of pusher P"; it does not prove B's contents unless combined with `merkle-proofs`.

## Depends on
- repo-do-ref-authority
- refs-sqlite-objects-r2
- two-phase-push
- auth-and-multitenancy
- r2-versioned-snapshots (optional, for tamper-evident anchors)
