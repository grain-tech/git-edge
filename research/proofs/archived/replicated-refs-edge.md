> Idea #13 · edge · verdict: **lands with caveats** · feasibility 4/5 · reliability 3/5 · correctness 3/5 · effort: days
> Proof: [proofs/replicated-refs-edge.md](./replicated-refs-edge.md) · Review: [reviews/replicated-refs-edge.md](../../reviews/archived/replicated-refs-edge.md)

# Refs replicated to every region via KV and DO location hints

## Mechanism
The repo DO (`RepoRefs`, one per `owner/repo`, placed once with `locationHint` near the team that pushes) is the only writer: `git-receive-pack` ends with a compare-and-swap on its SQLite `refs` table, which bumps a monotonic `version` and schedules a 1s alarm that publishes the whole ref list as one KV value `refs/<owner>/<repo>`. Every `git fetch`/`clone` starts with protocol v2 `ls-refs`; the Worker at the client's colo answers it straight from KV (already replicated to that colo, `cacheTtl: 60`) as pkt-lines, never touching the DO. The pusher's own follow-up reads carry `X-Git-Edge-Version` (returned by the push); if KV is behind that version the Worker falls through to the DO, so read-your-writes holds even though KV is eventually consistent. `fetch` itself still goes to the pack builder (DO + R2) and validates wants against the DO's refs, so a stale KV advertisement can only cause an older-but-valid snapshot, never a bad pack.

## Primitives
- Workers (edge entrypoint, pkt-line encoding of `ls-refs`)
- Durable Object with SQLite storage (`ctx.storage.sql`) — the single ref authority (GA)
- DO alarms (`ctx.storage.setAlarm`) — debounce KV publishes to respect the 1 write/sec/key limit (GA)
- DO `locationHint` on `namespace.get(id, { locationHint })` — first-creation placement hint only (GA, best-effort)
- Workers KV — global eventually-consistent replica of the ref list (GA)
- R2 — objects; untouched by this idea except that `fetch` reads from it (GA)
- Not used: DO SQLite read replicas — not a GA product as of this writing, so replicas are KV values, not DOs

## Proof code
```typescript
// Env: REPO (DurableObjectNamespace<RepoRefs>), REFS_KV (KVNamespace), BUCKET (R2Bucket)
type Ref = { name: string; oid: string; peeled?: string };
type Snapshot = { version: number; refs: Ref[] };

// ---- pkt-line helpers (git protocol v2 ls-refs response) ----
const pkt = (s: string) => (s.length + 4).toString(16).padStart(4, "0") + s;
const FLUSH = "0000";
function lsRefsBody(refs: Ref[]): string {
  // ls-refs reply: one "<oid> <refname>[ symref-target:x][ peeled:y]\n" per ref, then flush
  return refs.map(r => pkt(`${r.oid} ${r.name}${r.peeled ? ` peeled:${r.peeled}` : ""}\n`)).join("") + FLUSH;
}

// ---- Worker: reads hit KV at the client's colo, writes go to the authority DO ----
export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const m = new URL(req.url).pathname.match(/^\/([^/]+\/[^/]+)\/(info\/refs|git-upload-pack|git-receive-pack)$/);
    if (!m) return new Response("not found", { status: 404 });
    const [, repo, route] = m;
    const authority = () => env.REPO.get(env.REPO.idFromName(repo), { locationHint: "wnam" }); // hint only matters on first creation

    if (route === "git-upload-pack") {
      const body = await req.text(); // pkt-lines: "command=ls-refs\n" ... or "command=fetch\n" ...
      if (body.includes("command=ls-refs")) {
        const minVersion = Number(req.headers.get("x-git-edge-version") ?? 0); // read-your-writes for the pusher
        const snap = await env.REFS_KV.get<Snapshot>(`refs/${repo}`, { type: "json", cacheTtl: 60 });
        if (snap && snap.version >= minVersion) {
          return new Response(lsRefsBody(snap.refs), {
            headers: { "content-type": "application/x-git-upload-pack-result", "x-git-edge-version": String(snap.version), "x-git-edge-source": "kv" },
          });
        }
        // KV missing or older than what this client already saw -> ask the authority
      }
      // fetch command (want/have -> PACK) and any fallthrough: the DO validates wants against its own refs
      return authority().fetch(req);
    }
    return authority().fetch(req); // info/refs (v2: just "version 2" + capabilities) and receive-pack
  },
};

// ---- Authority DO: SQLite refs + CAS + debounced KV publish ----
export class RepoRefs implements DurableObject {
  constructor(private ctx: DurableObjectState, private env: Env) {
    ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS refs (name TEXT PRIMARY KEY, oid TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v INTEGER NOT NULL);
      INSERT OR IGNORE INTO meta VALUES ('version', 0), ('dirty', 0);
    `);
  }

  // Called at the end of git-receive-pack after the pack's objects are already in R2 (see two-phase-push).
  // cmds come from the receive-pack request: "<old-oid> <new-oid> <refname>" pkt-lines before the PACK.
  updateRefs(cmds: { old: string; nu: string; name: string }[]): { ok: boolean; version: number; status: string[] } {
    const sql = this.ctx.storage.sql;
    const status: string[] = [];
    let version = this.version();
    this.ctx.storage.transactionSync(() => {
      for (const c of cmds) {
        const cur = sql.exec<{ oid: string }>("SELECT oid FROM refs WHERE name=?", c.name).toArray()[0]?.oid ?? "0".repeat(40);
        if (cur !== c.old) { status.push(`ng ${c.name} fetch first`); throw new Error("cas"); } // git's report-status wording
        if (c.nu === "0".repeat(40)) sql.exec("DELETE FROM refs WHERE name=?", c.name);
        else sql.exec("INSERT OR REPLACE INTO refs VALUES (?,?)", c.name, c.nu);
        status.push(`ok ${c.name}`);
      }
      version = version + 1;
      sql.exec("UPDATE meta SET v=? WHERE k='version'", version);
      sql.exec("UPDATE meta SET v=1 WHERE k='dirty'");
    });
    // Debounce: KV allows ~1 write/sec/key, so publish at most once per second, coalescing bursts.
    void this.ctx.storage.getAlarm().then(a => a ?? this.ctx.storage.setAlarm(Date.now() + 1000));
    return { ok: true, version, status };
  }

  async alarm(): Promise<void> {
    const sql = this.ctx.storage.sql;
    if (sql.exec<{ v: number }>("SELECT v FROM meta WHERE k='dirty'").one().v === 0) return;
    const snap: Snapshot = {
      version: this.version(),
      refs: sql.exec<Ref>("SELECT name, oid FROM refs ORDER BY name").toArray(),
    };
    await this.env.REFS_KV.put(`refs/${this.repoName()}`, JSON.stringify(snap)); // replicated to every colo within ~60s
    sql.exec("UPDATE meta SET v=0 WHERE k='dirty'");
    // A push that landed while we were publishing set dirty=1 again; updateRefs already re-armed the alarm.
  }

  async fetch(req: Request): Promise<Response> {
    // receive-pack: parse pkt-line commands, stream the PACK to R2, then this.updateRefs(...) and
    // reply with report-status pkt-lines plus the new version so the client can demand it on its next ls-refs:
    //   headers: { "x-git-edge-version": String(version) }
    // upload-pack fetch: wants are checked against this table (or allow-any-sha1-in-want), pack built from R2.
    return new Response("see updateRefs/alarm", { status: 501 });
  }

  private version() { return this.ctx.storage.sql.exec<{ v: number }>("SELECT v FROM meta WHERE k='version'").one().v; }
  private repoName() { return this.ctx.storage.sql.exec<{ v: string }>("SELECT v FROM meta WHERE k='name'").one().v; } // stored on first receive-pack
}
```

## Why it works
- Every `git fetch`, `git pull`, `git ls-remote` and clone begins with `ls-refs` (protocol v2) and the reply is just a list of `<oid> <refname>` pkt-lines followed by a flush; it has no dependency on negotiation state, so a KV snapshot rendered as pkt-lines is byte-for-byte what `git-upload-pack` would have sent. No DO hop, no cross-ocean round trip for the most frequent request.
- Git already tolerates stale advertisements: refs only ever point at immutable content-addressed objects, so an older tip from KV yields an older but internally consistent fetch; the next fetch catches up. This is the same behaviour as talking to a lagging read-only mirror, which git handles routinely.
- Correctness of the pack is enforced where the truth lives: the `fetch` command (want/have to PACK) is served by the DO/pack builder, which checks wants against its own SQLite refs (or advertises `allow-reachable-sha1-in-want`), so a KV lag can never cause a want for an unknown or unreachable object to succeed.
- Push semantics are untouched: `git-receive-pack` sends `<old> <new> <refname>` commands and expects `report-status` lines `ok <ref>` / `ng <ref> <reason>`; the CAS in `updateRefs` inside `transactionSync` produces exactly that, and the single DO serializes concurrent pushes without locks (per `repo-do-ref-authority`).
- Read-your-writes, which git users will notice (push then immediately `git fetch` in CI), is preserved with the monotonic `version`: the push response returns it, the client (or our CI runner) echoes it, and the Worker refuses to serve a KV snapshot older than it.
- The alarm debounce turns any push rate into at most one KV write per second per repo, respecting KV's per-key write limit while coalescing bursts into a single consistent snapshot.

## Known limits
- KV is eventually consistent (propagation typically under 60s, plus `cacheTtl` minimum 60s), so unrelated clients in other colos can see refs up to ~2 minutes old. `git ls-remote` after someone else's push may lie for that window; only the pusher gets read-your-writes via the version header, and stock `git` does not send custom headers, so that path needs `http.extraHeader` or a wrapper (a CI runner can do it; a laptop user cannot without config).
- Force-pushes and branch deletions are the ugly case for stale reads: a client may briefly be advertised a tip that the authority has already discarded. Objects still exist in R2 (until GC per `gc-and-repack-alarm`), so the fetch still succeeds, but the GC janitor must respect a grace period at least as long as KV lag.
- "DO location hints" do less than the title implies: `locationHint` only influences where the DO is created the first time and is best-effort; it cannot move an existing DO, and there is no DO-level read replication that is GA. The replicas in this design are KV values, not DOs. If a repo's pushers migrate continents, the authority stays where it was born.
- KV write limit of ~1 write/sec/key is why the alarm exists; a repo with sustained multi-push-per-second traffic still publishes correctly but with an extra second of lag per burst. KV values cap at 25 MiB, which is ~250k refs; monorepos beyond that need sharded keys (`branch-level-dos`).
- Only `ls-refs` is served from the edge. The `fetch` command still needs the DO (want validation, commit-graph negotiation) and R2 (pack bytes), so clone latency is dominated by pack streaming, not this optimisation; the win is for the frequent no-op `git fetch` where `ls-refs` shows nothing changed and the client sends no `fetch` at all.
- Protocol v0 clients get refs in `info/refs?service=git-upload-pack`; serving those from KV too is straightforward (same snapshot, v0 framing with capabilities on the first line) but is not shown and depends on `protocol-v2-only`'s shim.
- Hand-waved: `repoName()` bootstrap, receive-pack body parsing, want validation and pack building; those are the concern of sibling ideas.

## Depends on
- repo-do-ref-authority
- refs-sqlite-objects-r2
- protocol-v2-only
- info-refs-endpoint
- two-phase-push
- auth-and-multitenancy
