> Idea #12 · foundation · verdict: **lands with caveats** · feasibility 4/5 · reliability 3/5 · correctness 3/5 · effort: days
> Proof: [proofs/bundle-uri.md](./bundle-uri.md) · Review: [reviews/bundle-uri.md](../../reviews/archived/bundle-uri.md)

# Bundle-URI support

## Mechanism
The Worker's protocol-v2 capability advertisement (`GET /:owner/:repo/info/refs?service=git-upload-pack`) adds the `bundle-uri` capability; when a client with `transfer.bundleURI=true` then sends `command=bundle-uri`, the Worker forwards it to the repo DO, which answers from a small `bundles` SQLite table with `bundle.<id>.uri=` / `creationToken=` key-value pkt-lines pointing at objects in an R2 bucket served on a custom domain (Cloudflare CDN-cached). The bundle object itself is produced by the repo DO's repack alarm: it streams a v2 bundle header (`# v2 git bundle`, one `<sha> <refname>` line per ref, blank line) followed by the already-built full pack into a single R2 `put` through a `FixedLengthStream`, then records the key, creation token and header length in SQLite. The same R2 object doubles as the precomputed clone pack: `git-upload-pack` serves fresh clones with `BUCKET.get(key, {range:{offset: headerLen}})`, so no second copy of the pack is stored. After the client unbundles (refs land under `refs/bundles/*`), its normal `command=fetch` carries those tips as `have`s and the DO's incremental pack is tiny.

## Primitives
- Durable Object with SQLite storage (`ctx.storage.sql.exec`) for the `bundles` table and ref tips - GA
- DO alarms (`ctx.storage.setAlarm`) to rebuild the bundle after pushes settle - GA
- R2 bucket binding `put`/`get` with range reads, `FixedLengthStream` for known-length streaming puts - GA
- R2 public bucket on a custom domain (Cloudflare cache in front of R2 reads) - GA
- Workers `Response` streams / `TransformStream` for pkt-line framing - GA
- (private repos only) Cache API `caches.default` in an auth-checking Worker route instead of the public bucket - GA

## Proof code
```typescript
// Repo DO: bundle-uri command + alarm that materializes the bundle in R2.
const pkt = (s: string) => (s.length + 4).toString(16).padStart(4, "0") + s;
const FLUSH = "0000";

export class RepoDO implements DurableObject {
  constructor(private ctx: DurableObjectState, private env: Env) {
    ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS refs    (name TEXT PRIMARY KEY, sha TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS packs   (key TEXT PRIMARY KEY, bytes INTEGER NOT NULL, full INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS bundles (id TEXT PRIMARY KEY, key TEXT NOT NULL,
                                          creation_token INTEGER NOT NULL, header_len INTEGER NOT NULL)`);
  }

  async fetch(req: Request): Promise<Response> {
    const url = new URL(req.url);
    // Worker already parsed the v2 request body and routed on "command=..."
    if (url.pathname === "/v2/bundle-uri") return this.bundleUri();
    if (url.pathname === "/v2/fetch-full-clone") return this.fullClonePack();
    if (url.pathname === "/receive-pack-done") {      // called by two-phase push after refs flipped
      await this.ctx.storage.setAlarm(Date.now() + 60_000); // debounce: rebuild bundle 1 min after last push
      return new Response("ok");
    }
    return new Response("not found", { status: 404 });
  }

  // Response to `command=bundle-uri` (protocol v2, Documentation/technical/bundle-uri.txt).
  bundleUri(): Response {
    const rows = this.ctx.storage.sql
      .exec<{ id: string; key: string; creation_token: number }>(
        "SELECT id, key, creation_token FROM bundles ORDER BY creation_token").toArray();
    let out = pkt("bundle.version=1\n") + pkt("bundle.mode=all\n") + pkt("bundle.heuristic=creationToken\n");
    for (const b of rows) {
      out += pkt(`bundle.${b.id}.uri=${this.env.BUNDLE_PUBLIC_BASE}/${b.key}\n`);
      out += pkt(`bundle.${b.id}.creationToken=${b.creation_token}\n`);
    }
    return new Response(out + FLUSH, { headers: { "content-type": "application/x-git-upload-pack-result" } });
  }

  // Alarm: wrap the latest full pack (built by the repack alarm) in a v2 bundle header.
  async alarm(): Promise<void> {
    const pack = this.ctx.storage.sql
      .exec<{ key: string; bytes: number }>("SELECT key, bytes FROM packs WHERE full=1 ORDER BY rowid DESC LIMIT 1").one();
    const refs = this.ctx.storage.sql
      .exec<{ name: string; sha: string }>("SELECT name, sha FROM refs WHERE name LIKE 'refs/heads/%' OR name LIKE 'refs/tags/%'").toArray();
    if (refs.length === 0) return;                             // git rejects a bundle with no refs

    // Bundle v2 header. (No prerequisites: this is a base bundle, pack is self-contained.)
    let header = "# v2 git bundle\n";
    for (const r of refs) header += `${r.sha} ${r.name}\n`;   // "<40-hex> refs/heads/main\n"
    header += "\n";                                            // blank line, then raw PACK bytes
    const headerBytes = new TextEncoder().encode(header);

    const token = Date.now();
    const key = `bundles/${this.ctx.id.toString()}/${token}.bundle`;
    const total = headerBytes.byteLength + pack.bytes;

    // R2 needs a known length for streamed puts -> FixedLengthStream; nothing is buffered in the DO.
    const { readable, writable } = new FixedLengthStream(total);
    const src = await this.env.BUCKET.get(pack.key);
    if (!src) return;
    const pump = (async () => {
      const w = writable.getWriter();
      await w.write(headerBytes);
      w.releaseLock();
      await src.body.pipeTo(writable);                        // PACK header + objects streamed through
    })();
    await Promise.all([this.env.BUCKET.put(key, readable, { httpMetadata: { contentType: "application/x-git-bundle" } }), pump]);

    this.ctx.storage.sql.exec(
      "INSERT INTO bundles (id, key, creation_token, header_len) VALUES (?, ?, ?, ?)",
      `base-${token}`, key, token, headerBytes.byteLength);
    // Keep only the newest two so a client mid-download of the previous one still succeeds.
    this.ctx.storage.sql.exec(
      "DELETE FROM bundles WHERE id NOT IN (SELECT id FROM bundles ORDER BY creation_token DESC LIMIT 2)");
  }

  // Same object, skip the header: serves as the precomputed clone pack (idea precomputed-clone-pack).
  async fullClonePack(): Promise<Response> {
    const b = this.ctx.storage.sql
      .exec<{ key: string; header_len: number }>("SELECT key, header_len FROM bundles ORDER BY creation_token DESC LIMIT 1").one();
    const obj = await this.env.BUCKET.get(b.key, { range: { offset: b.header_len } });
    return new Response(obj!.body, { headers: { "content-type": "application/x-git-upload-pack-result" } });
  }
}

// Worker side: advertise the capability in the v2 handshake.
export function capabilityAdvertisement(): string {
  return pkt("version 2\n") + pkt("agent=git-edge/0.1\n") + pkt("ls-refs=unborn\n")
       + pkt("fetch=shallow wait-for-done\n") + pkt("object-format=sha1\n") + pkt("bundle-uri\n") + FLUSH;
}
```

## Why it works
- `git clone` (2.38+) with `transfer.bundleURI=true` checks `server_supports_v2("bundle-uri")` after `ls-refs`, sends `command=bundle-uri`, and expects exactly the `bundle.version=1`, `bundle.mode`, optional `bundle.heuristic` and `bundle.<id>.uri` / `bundle.<id>.creationToken` key-value pkt-lines terminated by a flush; ids must be dot-free, which `base-<token>` satisfies.
- The client downloads each URI with its ordinary HTTP transport and runs `git bundle unbundle` on it, which is `index-pack --fix-thin --stdin` on everything after the blank line. The v2 header format is literally `# v2 git bundle\n` + `<sha> <refname>\n`... + `\n` + PACK, so a header prefix over an already-valid pack (header `PACK`, version 2, object count, ofs-delta entries, trailing SHA-1) is a valid bundle with no repacking.
- Unbundled refs are stored as `refs/bundles/<id>/<refname>`; the subsequent `command=fetch` lists them as `have` lines, so the DO's negotiation (want-have-negotiation) computes only the objects pushed since the bundle was cut. The "bulk" bytes never pass through the DO.
- `bundle.mode=all` with the `creationToken` heuristic makes the client fetch bundles newest-first until one whose prerequisites it satisfies; base bundles have no prerequisites, so a single download is always sufficient and `git clone` records `fetch.bundleCreationToken` for later incremental bundles.
- `--bundle-uri=<url>` on the clone command line also works with the same R2 object, and `git clone` still succeeds if the bundle download fails (it falls back to a normal fetch), so a stale or missing bundle degrades to the ordinary path instead of breaking clones.
- R2 `get` with `range.offset = header_len` yields the byte-exact pack, so the bundle object is also the precomputed clone pack served through `git-upload-pack` for clients without bundle support.

## Known limits
- The client must opt in: `transfer.bundleURI` defaults to false in every released git (checked through 2.4x), so advertising `bundle-uri` alone does nothing for default clients. Explicit `git clone --bundle-uri=` needs no config. Consumers only use the advertisement during clone; later fetches consult `fetch.bundleURI`, which clone sets only when a creationToken heuristic was advertised.
- Private repos: an R2 public bucket leaks history. The fallback is a Worker route `/bundles/*` that checks the same token as the smart-HTTP endpoints and streams from R2 with `caches.default`; git sends its stored credentials to same-host bundle URIs, but per-colo Cache API caching is weaker than the custom-domain CDN path. R2 presigned S3 URLs are an alternative but are not CDN-cached.
- The alarm copies the pack through the DO (R2 get -> R2 put). It is streamed, so 128MB DO memory is not an issue, but wall-clock scales with pack size and a DO alarm invocation is still bounded by the 30s CPU budget; copying itself is near-zero CPU. Packs above the single-put limit (~5GiB) need `createMultipartUpload` with 5MiB-minimum parts, not shown.
- Incremental bundles (with `-<sha>` prerequisite lines) are hand-waved: they require a pack that contains exactly the objects reachable from the new tips minus the old tips, i.e. output of want-have-negotiation, and `mode=all` semantics mean every listed bundle must be internally consistent; a bad prerequisite set makes the client discard the bundle and fall back to a full fetch. The proof only cuts base bundles.
- Depends entirely on a full pack already existing in R2 (`packs.full=1`); without gc-and-repack-alarm there is nothing to wrap, since the server cannot concatenate the per-push packs into one valid PACK stream (single header, single object count, single trailing checksum).
- R2 costs: one Class A put per rebuild plus the CDN-cached reads; the debounce alarm prevents rebuilding on every push but a hot repo still rewrites the whole pack each minute of activity. SHA-1 only (`object-format=sha1`); a v3 bundle header with `@object-format=sha256` is needed for sha256 repos.
- Bundles listed in SQLite that were deleted from R2 (or cut over on a fork with cow-forks) will make clients emit a warning and fall back; the DO does not verify R2 existence on every `bundle-uri` call.

## Depends on
- gc-and-repack-alarm
- precomputed-clone-pack
- refs-sqlite-objects-r2
- protocol-v2-only
- info-refs-endpoint
- want-have-negotiation
