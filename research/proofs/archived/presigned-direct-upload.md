> Idea #24 · edge · verdict: **risky** · feasibility 4/5 · reliability 2/5 · correctness 3/5 · effort: weeks
> Proof: [proofs/presigned-direct-upload.md](./presigned-direct-upload.md) · Review: [reviews/presigned-direct-upload.md](../../reviews/archived/presigned-direct-upload.md)

# Per-blob presigned direct upload for giant pushes

## Mechanism
A custom client (a `git-remote-edge` remote helper, or the grok-pi workspace DO; stock `git push` cannot do this) first calls `POST /:owner/:repo.git/blobs/batch` with `[{oid, size}]` for every blob it is about to push. The Worker forwards it to the repo DO, which skips oids already in its `objects` index, inserts the rest as `claimed` rows in SQLite, and returns one presigned R2 `PUT` URL per oid for the key `objects/<oid>`; the signature covers an `x-amz-checksum-sha1` header equal to the oid, and the body must be the raw git object (`blob <len>\0<bytes>`, uncompressed), so SHA-1 of the body is by definition the oid and R2 itself rejects any body that does not hash to the key. The client PUTs straight to `https://<account>.r2.cloudflarestorage.com/...` (never through a Worker), then does an ordinary `git-receive-pack` push whose pack was built with `git pack-objects --filter=blob:none` (commits and trees only, a few MB). The DO's phase-two commit (from `two-phase-push`) resolves each tree entry against `objects` rows in state `ok` or `claimed`; claimed rows are promoted after an `env.BUCKET.head()` shows the key exists with a matching sha1 checksum, and unpromotable oids fail the push with `ng refs/heads/x missing-blobs`.

## Primitives
- Durable Objects with SQLite storage (`ctx.storage.sql`) - GA
- DO alarms (`ctx.storage.setAlarm`) for sweeping expired claims and optional re-hashing - GA
- R2 bucket binding (`env.BUCKET.head`, `env.BUCKET.get` streamed) - GA
- R2 S3-compatible API presigned URLs (SigV4 query signing via `aws4fetch`, max 7-day expiry) - GA; needs an R2 API token as Worker secrets, the binding has no `presign()`
- R2 `x-amz-checksum-sha1` on PutObject (listed in the R2 S3 compatibility table) and `R2Object.checksums.sha1` on `head()` - GA, but verify on your account; the fallback path below does not need it
- `crypto.DigestStream("SHA-1")` (Workers-specific, streaming digest, no buffering) - GA
- Normal smart-HTTP `git-receive-pack` for the commits/trees pack - from `streaming-pack-parser`

## Proof code
```typescript
import { AwsClient } from "aws4fetch";

interface Env { BUCKET: R2Bucket; R2_ACCOUNT: string; R2_BUCKET: string; R2_KEY: string; R2_SECRET: string }
const TTL_S = 3600, CLAIM_TTL_MS = TTL_S * 1000 + 60_000;
const key = (oid: string) => `objects/${oid}`;

export class RepoDO implements DurableObject {
  private s3: AwsClient;
  constructor(private ctx: DurableObjectState, private env: Env) {
    ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS objects (
      oid TEXT PRIMARY KEY, type TEXT NOT NULL, size INTEGER NOT NULL,
      state TEXT NOT NULL CHECK (state IN ('claimed','ok')), claimed_at INTEGER)`);
    this.s3 = new AwsClient({ accessKeyId: env.R2_KEY, secretAccessKey: env.R2_SECRET, service: "s3", region: "auto" });
  }

  async fetch(req: Request): Promise<Response> {
    const p = new URL(req.url).pathname;
    if (p === "/blobs/batch") return Response.json(await this.batch(await req.json()));
    // "/git-receive-pack" is the ordinary push path (two-phase-push); its commit step calls resolveBlobs()
    return new Response("not found", { status: 404 });
  }

  // Phase A: hand out one presigned PUT per blob the index does not already have.
  private async batch(objs: { oid: string; size: number }[]) {
    const out = [];
    for (const { oid, size } of objs) {
      if (!/^[0-9a-f]{40}$/.test(oid)) continue;
      const row = this.ctx.storage.sql.exec<{ state: string }>("SELECT state FROM objects WHERE oid=?", oid).toArray()[0];
      if (row?.state === "ok") { out.push({ oid, have: true }); continue; }
      this.ctx.storage.sql.exec("INSERT OR REPLACE INTO objects VALUES (?,'blob',?,'claimed',?)", oid, size, Date.now());
      // git oid == sha1("blob <len>\0" + content). Client PUTs exactly those bytes; R2 checks the header.
      const sha1b64 = btoa(String.fromCharCode(...oid.match(/../g)!.map((h) => parseInt(h, 16))));
      const url = `https://${this.env.R2_ACCOUNT}.r2.cloudflarestorage.com/${this.env.R2_BUCKET}/${key(oid)}?X-Amz-Expires=${TTL_S}`;
      const signed = await this.s3.sign(new Request(url, { method: "PUT",
        headers: { "x-amz-checksum-sha1": sha1b64, "content-length": String(size + `blob ${size}\0`.length) } }),
        { aws: { signQuery: true } });
      out.push({ oid, have: false, put: signed.url, headers: { "x-amz-checksum-sha1": sha1b64 } });
    }
    await this.ctx.storage.setAlarm(Date.now() + CLAIM_TTL_MS);
    return out;
  }

  // Phase B hook: called inside the push commit for every blob oid referenced by a tree entry in the pack.
  // Returns the oids that are NOT usable; the caller fails the ref update with "ng <ref> missing-blobs".
  async resolveBlobs(oids: string[]): Promise<string[]> {
    const missing: string[] = [];
    for (const oid of oids) {
      const row = this.ctx.storage.sql.exec<{ state: string }>("SELECT state FROM objects WHERE oid=?", oid).toArray()[0];
      if (row?.state === "ok") continue;
      const head = await this.env.BUCKET.head(key(oid));
      const ok = head && (head.checksums.sha1 ? hex(head.checksums.sha1) === oid : await this.rehash(oid));
      if (!ok) { missing.push(oid); continue; }
      this.ctx.storage.sql.exec("INSERT OR REPLACE INTO objects VALUES (?,'blob',?,'ok',NULL)", oid, head!.size);
    }
    return missing;
  }

  // Fallback when the upload carried no R2 checksum: stream the body through a digest, never buffer it.
  private async rehash(oid: string): Promise<boolean> {
    const obj = await this.env.BUCKET.get(key(oid));
    if (!obj) return false;
    const ds = new crypto.DigestStream("SHA-1");
    await obj.body.pipeTo(ds);
    return hex(await ds.digest) === oid;
  }

  // Janitor: claims that were never followed by a push. Objects are content-addressed, so a stray
  // upload is harmless; we only drop the row (and the key, if it is not referenced by any committed push).
  async alarm() {
    const cutoff = Date.now() - CLAIM_TTL_MS;
    for (const { oid } of this.ctx.storage.sql.exec<{ oid: string }>(
      "SELECT oid FROM objects WHERE state='claimed' AND claimed_at < ?", cutoff).toArray()) {
      this.ctx.storage.sql.exec("DELETE FROM objects WHERE oid=? AND state='claimed'", oid);
      this.ctx.waitUntil(this.env.BUCKET.delete(key(oid)));
    }
    if (this.ctx.storage.sql.exec("SELECT 1 FROM objects WHERE state='claimed' LIMIT 1").toArray().length)
      await this.ctx.storage.setAlarm(Date.now() + CLAIM_TTL_MS);
  }
}
const hex = (b: ArrayBuffer) => [...new Uint8Array(b)].map((x) => x.toString(16).padStart(2, "0")).join("");
```

## Why it works
- A git object id is SHA-1 over `"<type> <len>\0" + content`, nothing else. Uploading exactly those bytes means the R2 key, the R2-verified checksum and the git oid are the same 20 bytes, so the server never has to trust the client about what it uploaded, and the presigned URL cannot be reused to plant a different body under that key because the checksum header is inside the signature.
- The manifest is not a new format: it is the commits and trees git already sends. `git pack-objects --filter=blob:none` produces a valid `PACK` stream whose tree entries point at blobs not in the pack; upstream `receive-pack` would reject that as broken connectivity, but our phase-two connectivity check (from `two-phase-push`) resolves tree entries against the `objects` index, and `resolveBlobs()` is exactly where the direct-uploaded blobs are admitted.
- The Worker request path carries only the small pack, so the 100MB request-body ceiling and the CPU spent inflating in `streaming-pack-parser` no longer scale with blob size; a 4GB blob costs the Worker one `head()`.
- Content addressing makes every step idempotent: a re-run `blobs/batch` after a network failure returns `have: true` for finished blobs and fresh URLs for the rest; a re-PUT of an identical body is a no-op; a re-push of the same pack finds the same rows.
- The `claimed` state is what turns "someone might have uploaded this" into a fact the DO can serialize on: promotion to `ok` happens only inside the single-writer DO, so two concurrent pushes sharing a blob cannot race the janitor (the sweep deletes only rows still `claimed` and older than the URL TTL, after which the URL is dead anyway).

## Known limits
- Stock `git push` will never do this; the wire protocol has no way to say "blob X is already there". The idea works only with a remote helper (`git-remote-edge`), an LFS-like CLI step, or the in-DO push of `tui-rpc-push`. As stated in the catalog ("client PUTs blobs ... then sends the DO a manifest") it is correct, but the client is bespoke.
- Storing the object uncompressed at `objects/<oid>` is required for R2's checksum to equal the git oid. If the rest of the system stores zlib-compressed loose objects, either store both forms or drop the R2 checksum and rely on `rehash()`, which reads the whole blob back once through the DO (streamed, so no 128MB problem, but a 4GB blob is ~4GB of R2 egress to the DO and tens of seconds of hashing; move it to the alarm, not the push request).
- `x-amz-checksum-sha1` enforcement on presigned PutObject and `checksums.sha1` on `head()` are documented for R2 but were not exercised here; if they are missing on your account the fallback path is the only guarantee and an attacker with a valid URL can park garbage under a claimed key until the alarm rehashes it.
- Presigned URLs need a real R2 access key in Worker secrets and the S3 endpoint, which means the client hits `r2.cloudflarestorage.com`, not your custom domain; PUTs are not fronted by the Worker so per-push rate limits and quotas must be enforced at `blobs/batch` (count and total declared size) rather than on the bytes.
- Each blob is one Class A R2 PUT plus one `head()`; a push of 100k tiny blobs is worse than one pack through the Worker. Route only blobs above a threshold (e.g. 8MB) this way; multipart is required above 5GB per object and needs a presigned `CreateMultipartUpload`/`UploadPart` flow not shown.
- `batch()` runs serially in the DO; presigning is pure HMAC (~0.1ms) but 10k oids is still 10k SQLite rows and 10k signatures in one request. Page the batch at a few thousand oids.
- Delta compression is lost for direct-uploaded blobs (each is a full object); `gc-and-repack-alarm` can re-delta them later.

## Depends on
- two-phase-push
- content-addressed-r2-keys
- streaming-pack-parser
- repo-do-ref-authority
- native-lfs (same presign helper and secrets)
