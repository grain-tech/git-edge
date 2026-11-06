> Idea #11 · foundation · verdict: **lands with caveats** · feasibility 4/5 · reliability 3/5 · correctness 4/5 · effort: days
> Proof: [proofs/native-lfs.md](./native-lfs.md) · Review: [reviews/native-lfs.md](../../reviews/archived/native-lfs.md)

# Git LFS natively via presigned R2 URLs

## Mechanism
`POST /:owner/:repo.git/info/lfs/objects/batch` (git-lfs sends `Accept: application/vnd.git-lfs+json`) is authenticated at the Worker edge and forwarded to the repo DO (`idFromName("owner/repo")`). The DO keeps an `lfs_objects(oid, size, state)` table in SQLite; for `download` it answers with presigned R2 `GET` URLs for oids in state `ok`, for `upload` it inserts `pending` rows and answers with presigned R2 `PUT` URLs whose signature covers an `x-amz-checksum-sha256` header equal to the LFS oid, plus a `verify` action. The client PUTs bytes straight to `https://<account>.r2.cloudflarestorage.com/<bucket>/lfs/<oid>` (never through the Worker); the `verify` POST returns to the DO, which `env.BUCKET.head()`s the key, checks size and sha256, and flips the row to `ok`. A DO alarm sweeps `pending` rows older than the URL TTL. LFS payloads are not git objects (git only stores a pointer blob), so "already in R2" means same bucket, separate `lfs/` prefix, keyed by sha256.

## Primitives
- Durable Objects with SQLite storage (`ctx.storage.sql`) - GA
- DO alarms (`ctx.storage.setAlarm`) - GA
- R2 bucket binding (`env.BUCKET.head`) - GA
- R2 S3-compatible API + presigned URLs (SigV4 query signing, max 7-day expiry) - GA; needs an R2 API token (access key id / secret) as a Worker secret, there is no `presign()` on the binding
- R2 `x-amz-checksum-sha256` on PutObject (per R2 S3 compatibility table) - GA, but see Known limits
- WebCrypto HMAC-SHA256 for SigV4 (pure CPU, no network)

## Proof code
```typescript
import { AwsClient } from "aws4fetch"; // SigV4 in WebCrypto; runs in Workers

interface Env { BUCKET: R2Bucket; R2_ACCOUNT: string; R2_BUCKET: string; R2_KEY: string; R2_SECRET: string }
type LfsObj = { oid: string; size: number };
const TTL = 3600;

export class RepoDO implements DurableObject {
  private s3: AwsClient;
  constructor(private ctx: DurableObjectState, private env: Env) {
    ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS lfs_objects (
      oid TEXT PRIMARY KEY, size INTEGER NOT NULL,
      state TEXT NOT NULL CHECK (state IN ('pending','ok')), created_at INTEGER NOT NULL)`);
    this.s3 = new AwsClient({ accessKeyId: env.R2_KEY, secretAccessKey: env.R2_SECRET, service: "s3", region: "auto" });
  }

  async fetch(req: Request): Promise<Response> {
    const url = new URL(req.url);
    if (url.pathname === "/objects/batch") return this.batch(await req.json());
    if (url.pathname === "/verify") return this.verify(await req.json());
    return new Response("not found", { status: 404 });
  }

  private key(oid: string) { return `lfs/${oid.slice(0, 2)}/${oid.slice(2, 4)}/${oid}`; }
  private objUrl(oid: string) { return `https://${this.env.R2_ACCOUNT}.r2.cloudflarestorage.com/${this.env.R2_BUCKET}/${this.key(oid)}`; }

  // Presign: aws4fetch puts X-Amz-Signature etc. in the query; `headers` become X-Amz-SignedHeaders
  private async presign(method: string, oid: string, headers: Record<string, string> = {}) {
    const r = await this.s3.sign(new Request(this.objUrl(oid) + `?X-Amz-Expires=${TTL}`, { method, headers }), { aws: { signQuery: true } });
    return r.url;
  }

  // LFS batch API: {operation:"download"|"upload", transfers:["basic"], objects:[{oid,size}]}
  private async batch(body: { operation: "download" | "upload"; objects: LfsObj[] }) {
    const now = Date.now();
    const objects = [];
    for (const { oid, size } of body.objects) {
      const row = this.ctx.storage.sql.exec("SELECT size, state FROM lfs_objects WHERE oid = ?", oid).toArray()[0];
      if (body.operation === "download") {
        objects.push(row?.state === "ok"
          ? { oid, size, authenticated: true, actions: { download: { href: await this.presign("GET", oid), expires_in: TTL } } }
          : { oid, size, error: { code: 404, message: "Object does not exist" } });
        continue;
      }
      if (row?.state === "ok") { objects.push({ oid, size }); continue; } // already have it: no actions => client skips
      this.ctx.storage.sql.exec("INSERT OR REPLACE INTO lfs_objects VALUES (?, ?, 'pending', ?)", oid, size, now);
      // sha256 oid as base64 in a SIGNED header: R2 rejects a body whose digest differs, so the URL cannot be reused for other bytes
      const sha = btoa(String.fromCharCode(...oid.match(/../g)!.map((h) => parseInt(h, 16))));
      const header = { "x-amz-checksum-sha256": sha };
      objects.push({ oid, size, authenticated: true, actions: {
        upload: { href: await this.presign("PUT", oid, header), header, expires_in: TTL },
        verify: { href: `${this.publicBase}/info/lfs/verify`, expires_in: TTL } } });
    }
    await this.ctx.storage.setAlarm(now + TTL * 1000); // sweep stale pendings
    return json({ transfer: "basic", objects });
  }

  private async verify({ oid, size }: LfsObj) {
    const head = await this.env.BUCKET.head(this.key(oid));
    const ok = head && head.size === size && hex(head.checksums.sha256) === oid;
    if (!ok) return json({ message: "verification failed" }, 422);
    this.ctx.storage.sql.exec("UPDATE lfs_objects SET state = 'ok' WHERE oid = ?", oid);
    return new Response(null, { status: 200 });
  }

  async alarm() {
    const cutoff = Date.now() - TTL * 1000;
    for (const { oid } of this.ctx.storage.sql.exec("SELECT oid FROM lfs_objects WHERE state = 'pending' AND created_at < ?", cutoff).toArray() as { oid: string }[]) {
      const head = await this.env.BUCKET.head(this.key(oid)); // client PUT but never verified: adopt it if intact
      if (head && hex(head.checksums.sha256) === oid) this.ctx.storage.sql.exec("UPDATE lfs_objects SET state='ok' WHERE oid=?", oid);
      else this.ctx.storage.sql.exec("DELETE FROM lfs_objects WHERE oid=?", oid);
    }
  }
  private publicBase = "https://git.example.com/owner/repo.git";
}
const hex = (b?: ArrayBuffer) => b ? [...new Uint8Array(b)].map((x) => x.toString(16).padStart(2, "0")).join("") : "";
const json = (o: unknown, status = 200) => new Response(JSON.stringify(o), { status, headers: { "content-type": "application/vnd.git-lfs+json" } });
```

## Why it works
- git-lfs discovers the endpoint as `<remote>/info/lfs` and only requires the batch API with the `basic` transfer adapter; `basic` is literally "PUT/GET the raw bytes to `href` with these `header`s", which is exactly what an S3 presigned URL is. GitHub/GitLab already serve S3 URLs this way, so the client path is well exercised.
- The `header` map in an action is echoed verbatim by the client on the PUT, so the signed `x-amz-checksum-sha256` rides along and R2 enforces that the body hashes to the LFS oid. The URL is therefore useless for uploading anything but that object, and a `pending` row can never be flipped to a wrong blob.
- The `verify` action is part of the spec: after every upload git-lfs POSTs `{oid,size}` to it, which gives the DO a hook to `head()` and commit the row atomically (single DO per repo = no race between concurrent pushers of the same oid; `INSERT OR REPLACE` is idempotent).
- Download responses with per-object `error: {code: 404}` are how the spec reports missing objects; the client handles them without aborting the whole batch.
- Existence is answered from DO SQLite, not R2 HEADs, so a 1000-object batch costs 1000 HMACs (microseconds each) and zero R2 Class B operations; the bytes never transit the Worker, so the 128 MB DO memory and per-request CPU limits do not apply to the payload at all.
- The lock API (`/info/lfs/locks`) is not implemented; git-lfs prints "Remote does not support the Git LFS locking API" on 404/501 and continues, which is the documented behaviour.

## Known limits
- Presigning needs an R2 S3 API token stored as a Worker secret; the native `R2Bucket` binding cannot mint presigned URLs. Max URL lifetime is 7 days.
- Checksum enforcement rests on R2 honouring `x-amz-checksum-sha256` on presigned PutObject. R2's S3 compatibility table lists sha256 checksums for PutObject and the binding exposes `checksums.sha256`, but if a given deployment finds it ignored (e.g. multipart), the fallback is for `verify` to stream the object through a Worker and hash it incrementally (WebCrypto `digest` is not streaming; needs a JS/Wasm sha256), bounded by the 30 s CPU limit for multi-GB objects.
- `basic` transfer means one single-part PUT per object; R2 single PUT caps at 5 GB (S3 API 5 GiB). Objects above that need the LFS `multipart` transfer adapter (only some clients) or a Worker-mediated multipart flow (see presigned-direct-upload).
- Presigned R2 URLs point at `<account>.r2.cloudflarestorage.com`, not a custom domain, and are not CDN-cached; hot LFS downloads pay R2 Class B ops and egress-free bandwidth but no cache hits. A public-bucket custom domain plus Cache API for `download` hrefs would fix that at the cost of losing per-object auth.
- All batch calls for a repo serialize through one DO; ~1000 objects per batch × HMAC is fine, but pathological monorepos with 100k+ LFS files per batch will hit DO request time limits and should page (git-lfs already batches at 100 by default).
- Cross-repo dedup is trivial (key is `lfs/<sha256>`) but then no repo can safely delete an object; the alarm sweeper above only deletes SQLite rows, never R2 objects, so orphan payloads accumulate until a global GC exists.
- No locking API, no `ssh` transfer, no `expires_at`-based refresh on retries beyond the TTL.

## Depends on
repo-do-ref-authority, auth-and-multitenancy, content-addressed-r2-keys, refs-sqlite-objects-r2
