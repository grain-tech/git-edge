> Idea #30 · wild · verdict: **lands with caveats** · feasibility 4/5 · reliability 2/5 · correctness 3/5 · effort: days
> Proof: [proofs/ephemeral-repos.md](./ephemeral-repos.md) · Review: [reviews/ephemeral-repos.md](../../reviews/archived/ephemeral-repos.md)

# Ephemeral repos with a self-destruct alarm

## Mechanism

`POST /:owner/:repo?ttl=3600` (or the first `git push` carrying a scoped token with a TTL claim) hits the edge Worker, which does `env.REPO.idFromName("owner/repo")` and forwards to the per-repo `RepoDO`. The DO writes a `meta` row (`{epoch, expires_at}`) into its SQLite, arms `ctx.storage.setAlarm(expires_at)`, and from then on every object this repo stores goes to R2 under `repos/<owner>/<repo>/<epoch>/objects/<sha>` (loose or packed) while refs live in the DO's `refs` table. When the alarm fires, the DO first flips `meta.state = 'expired'` (so any concurrent `git-receive-pack` fails its ref CAS with `ng ... repo expired`), then pages through `env.BUCKET.list({prefix})` deleting up to 1000 keys per `env.BUCKET.delete(keys)` call, re-arming the alarm at `Date.now()` if the listing was truncated, and finally calls `ctx.storage.deleteAll()` + `deleteAlarm()` so the DO's SQLite is empty and its billing stops. A later `GET /info/refs` on the same name finds no `meta` row and returns 404 (git prints "repository not found"); a later `POST ?ttl=` creates a fresh epoch, so keys never collide with a deletion still in flight.

## Primitives

- Durable Objects with SQLite storage (`ctx.storage.sql.exec`) — GA
- DO alarms (`ctx.storage.setAlarm` / `getAlarm` / `deleteAlarm`, at-least-once with automatic retry) — GA
- `ctx.storage.deleteAll()` (SQLite-backed DO: drops all tables and KV; note it does *not* clear the alarm) — GA
- R2 `list({prefix, cursor})` (1000 keys/page, Class A op) and bulk `delete(string[])` (max 1000 keys/call, free op) — GA
- Workers routing via `DurableObjectNamespace.idFromName` — GA
- Optional: DO `ctx.abort()`/`ctx.storage.deleteAll()` is the only "delete DO" primitive; there is no API to delete a DO *id*, only its storage — this is a GA limitation, not beta

## Proof code

```typescript
// wrangler.jsonc: durable_objects.bindings [{name:"REPO",class_name:"RepoDO"}],
// migrations [{tag:"v1", new_sqlite_classes:["RepoDO"]}], r2_buckets [{binding:"BUCKET", bucket_name:"git-edge"}]
export interface Env { REPO: DurableObjectNamespace; BUCKET: R2Bucket }

type Meta = { epoch: number; expires_at: number; state: "live" | "expired" };

export class RepoDO implements DurableObject {
  constructor(private ctx: DurableObjectState, private env: Env) {
    ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS refs (name TEXT PRIMARY KEY, sha TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS del_cursor (id INTEGER PRIMARY KEY CHECK (id = 1), cursor TEXT);
    `);
  }

  private meta(): Meta | undefined {
    const r = this.ctx.storage.sql.exec<{ v: string }>("SELECT v FROM meta WHERE k='repo'").toArray()[0];
    return r ? JSON.parse(r.v) : undefined;
  }
  private prefix(m: Meta) { return `repos/${this.ctx.id.name}/${m.epoch}/`; } // name = "owner/repo"

  async fetch(req: Request): Promise<Response> {
    const url = new URL(req.url);
    const m = this.meta();

    // Create (or re-create after expiry) with a TTL. Epoch makes the R2 prefix unique per life.
    if (req.method === "POST" && url.pathname === "/create") {
      if (m?.state === "live") return new Response("exists", { status: 409 });
      const ttl = Math.min(Number(url.searchParams.get("ttl") ?? 3600), 24 * 3600);
      const meta: Meta = { epoch: Date.now(), expires_at: Date.now() + ttl * 1000, state: "live" };
      this.ctx.storage.sql.exec("INSERT OR REPLACE INTO meta VALUES ('repo', ?)", JSON.stringify(meta));
      await this.ctx.storage.setAlarm(meta.expires_at);
      return Response.json(meta);
    }
    if (!m || m.state !== "live") return new Response("repository not found", { status: 404 });

    if (url.pathname === "/info/refs") {
      // pkt-line advertisement: "<sha> <ref>\0<caps>\n" ... "0000"; also advertise expiry as an agent cap
      const rows = this.ctx.storage.sql.exec<{ name: string; sha: string }>("SELECT name, sha FROM refs ORDER BY name").toArray();
      const caps = `report-status side-band-64k agent=git-edge/ephemeral expires-at=${m.expires_at}`;
      const body = pktServiceHeader(url.searchParams.get("service")!) + refAdvert(rows, caps) + "0000";
      return new Response(body, { headers: { "content-type": `application/x-${url.searchParams.get("service")}-advertisement` } });
    }

    if (url.pathname === "/git-receive-pack") {
      // 1) parse command pkt-lines "<old> <new> <ref>", 2) stream the PACK body into R2 under this epoch's prefix
      const { commands, packBody } = await parseReceivePack(req.body!);
      const packKey = `${this.prefix(m)}objects/pack/pack-${crypto.randomUUID()}.pack`;
      await this.env.BUCKET.put(packKey, packBody);           // ~PACK header + objects; indexing elsewhere (streaming-pack-parser)
      // 3) atomic ref CAS *and* liveness check in one synchronous SQL step: an alarm that flipped
      //    state='expired' while we were awaiting R2 makes every command fail with a git-visible "ng".
      const live = this.meta()?.state === "live";
      const lines = commands.map(({ oldSha, newSha, ref }) => {
        if (!live) return `ng ${ref} repo expired\n`;
        const cur = this.ctx.storage.sql.exec<{ sha: string }>("SELECT sha FROM refs WHERE name=?", ref).toArray()[0]?.sha ?? "0".repeat(40);
        if (cur !== oldSha) return `ng ${ref} fetch first\n`;
        this.ctx.storage.sql.exec("INSERT OR REPLACE INTO refs VALUES (?,?)", ref, newSha);
        return `ok ${ref}\n`;
      });
      return new Response(sideband1(pkt("unpack ok\n") + lines.map(pkt).join("") + "0000"),
        { headers: { "content-type": "application/x-git-receive-pack-result" } });
    }
    return new Response("not found", { status: 404 });
  }

  // Idempotent, resumable self-destruct. Alarms are at-least-once and retried on throw, so every step tolerates re-entry.
  async alarm(): Promise<void> {
    const m = this.meta();
    if (!m) return;                                            // already wiped
    if (m.state === "live") {
      if (Date.now() < m.expires_at) { await this.ctx.storage.setAlarm(m.expires_at); return; } // TTL was extended
      this.ctx.storage.sql.exec("UPDATE meta SET v=? WHERE k='repo'", JSON.stringify({ ...m, state: "expired" }));
    }
    const saved = this.ctx.storage.sql.exec<{ cursor: string }>("SELECT cursor FROM del_cursor").toArray()[0]?.cursor;
    const page = await this.env.BUCKET.list({ prefix: this.prefix(m), cursor: saved ?? undefined, limit: 1000 });
    if (page.objects.length) await this.env.BUCKET.delete(page.objects.map(o => o.key)); // <=1000 keys/call, free op
    if (page.truncated) {                                      // big repo: chain alarms, one page per tick, no CPU-limit risk
      this.ctx.storage.sql.exec("INSERT OR REPLACE INTO del_cursor VALUES (1, ?)", page.cursor);
      await this.ctx.storage.setAlarm(Date.now());
      return;
    }
    await this.ctx.storage.deleteAlarm();                      // deleteAll() does NOT clear a pending alarm
    await this.ctx.storage.deleteAll();                        // refs, meta, cursor gone; DO storage billing stops
  }
}

// helpers (elided): pkt(s) = 4-hex-len + s; pktServiceHeader(svc) = pkt(`# service=${svc}\n`) + "0000";
// refAdvert(rows, caps) = first line "<sha> <name>\0<caps>\n" (or "<zero-id> capabilities^{}\0<caps>\n" when empty), rest "<sha> <name>\n";
// sideband1(s) wraps s in band-1 pkt-lines; parseReceivePack reads command pkt-lines until "0000" then the raw "PACK" stream.
```

## Why it works

- git's notion of "does this repo exist" is entirely `GET /info/refs?service=...`: a 404 there makes the client print `repository 'X' not found`, so an expired repo needs no tombstone beyond the missing `meta` row (and the DO id can never be deleted anyway — only its storage, which is exactly what `deleteAll()` does).
- A push is only "accepted" when `git-receive-pack` returns `unpack ok` followed by `ok <ref>` in report-status; returning `ng <ref> repo expired` for a push that was in flight during the alarm gives the client the normal `! [remote rejected]` and leaves its local state intact. Because the liveness check and the ref CAS run in the same synchronous SQL section (no `await` between them, so the DO input gate is closed), there is no window where a ref lands after expiry.
- Objects are immutable and only reachable through refs; once the `refs` table is wiped nothing can advertise them, so R2 deletion order does not matter for correctness — a clone that started before expiry may hit `early EOF` when a pack key vanishes mid-stream, which is the same failure git shows for any interrupted transport.
- The per-life `epoch` in the R2 prefix means a re-created `owner/repo` never shares keys with a deletion still paging through `list()`, and the pending alarm chain of the old life cannot delete the new life's objects.
- DO alarms are delivered at least once and retried with backoff on exceptions; the handler persists its `list()` cursor in SQLite, so an R2 hiccup or a large repo (>1000 keys) resumes from where it left off, and each alarm tick is one `list` + one `delete` — far inside the 30 s CPU budget.
- `expires-at=` is advertised as a capability token on the first ref line; stock git ignores unknown capabilities, while an aware client (or the `agent-native-commands` layer) can display the countdown.

## Known limits

- The idea says "deletes its DO state": there is no API to delete a Durable Object identity, only `ctx.storage.deleteAll()`; the DO named `owner/repo` still exists as an id and can be woken by a request (it just has empty storage and returns 404). This is the closest thing that exists and it is what Cloudflare recommends.
- Incompatible with `global-dedup` and with any `content-addressed-r2-keys` layout that shares keys across repos: self-destruct requires that the R2 prefix be exclusively owned by this repo life (`repos/<owner>/<repo>/<epoch>/`). Sharing objects would need refcounts in D1 or a mark phase — hand-waved here.
- `list()` costs one Class A op per 1000 keys ($4.50/M) even though `delete` is free; a loose-object layout with 100k objects means ~100 alarm ticks. Packing on push (`precomputed-clone-pack`) keeps key counts tiny.
- The alarm fires with at-least-once semantics and may run a few seconds late under load; the TTL is "at least one hour", not "exactly one hour". Alarms are also delivered only when the DO is not overloaded; a repo receiving a firehose of pushes at expiry could delay its own death slightly.
- Receive-pack here buffers the pack body into a single `BUCKET.put` for brevity. Real pushes need `streaming-pack-parser` (indexing, ofs-delta resolution) and stay under the DO 128 MB memory limit only if the pack is streamed to R2 (multipart) rather than materialized.
- Single-DO throughput: all pushes and the alarm serialize on one object, which is intended (ref authority), but means a very large deletion chain competes with reads of a re-created repo of the same name.
- Sliding TTL (`touch` on every push) is trivial — update `expires_at` and `setAlarm` — but not shown; the handler already tolerates an alarm that fires before the extended deadline.

## Depends on

- repo-do-ref-authority
- refs-sqlite-objects-r2
- auth-and-multitenancy
- info-refs-endpoint
- streaming-pack-parser
- scoped-token-remotes (optional: TTL-scoped tokens as the creation path)
