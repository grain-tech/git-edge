> Idea #16 · edge · verdict: **risky** · feasibility 4/5 · reliability 3/5 · correctness 2/5 · effort: weeks
> Proof: [proofs/branch-level-dos.md](./branch-level-dos.md) · Review: [reviews/branch-level-dos.md](../../reviews/archived/branch-level-dos.md)

# Branch-level Durable Objects for monorepos

## Mechanism
The Worker keeps one `RepoRoot` DO per repo (`idFromName("owner/repo")`) that owns only HEAD, the shard registry and ACLs, and N `RefShard` DOs (`idFromName("owner/repo#refs/heads/team-a")`) that each own the refs under one namespace prefix in their own SQLite `refs` table. On `git-receive-pack` the Worker reads the pkt-line command section (`<old> <new> <refname>\0caps`), indexes the packfile straight into content-addressed R2 (`objects/<sha>`) exactly as the unsharded design does, then groups the commands by namespace and calls each shard's `updateRefs()` in parallel; each shard runs its compare-and-swap in one SQLite transaction and returns per-ref `ok`/`ng` lines the Worker concatenates into the report-status. On protocol v2 `ls-refs`, the `ref-prefix` argument routes the request to exactly the shard(s) that own that prefix, so a fetch of `refs/heads/main` never touches the DO that is absorbing a firehose of pushes to `refs/heads/ci/*`; a prefix-less `ls-refs` or v0 receive-pack advertisement fans out to every shard in the registry with `Promise.all` and merges the sorted lists.

## Primitives
- Durable Objects with SQLite storage (`ctx.storage.sql.exec`, `transactionSync`) - GA
- DO RPC methods (`extends DurableObject`, `stub.updateRefs(...)`) - GA
- DO `idFromName` deterministic sharding by `owner/repo#namespace` - GA
- DO alarms (`ctx.storage.setAlarm`) for shard registry heartbeat/reaping empty shards - GA
- R2 `put`/`head` for content-addressed objects; `get` with `range` for pack slices - GA
- Workers `Response` streaming + `DecompressionStream("deflate")` for zlib entries in the PACK - GA
- Location hints (`newUniqueId({jurisdiction})` / `locationHint`) for placing shards near their writers - GA

## Proof code
```typescript
import { DurableObject } from "cloudflare:workers";

interface Env { ROOT: DurableObjectNamespace<RepoRoot>; SHARD: DurableObjectNamespace<RefShard>; BUCKET: R2Bucket }
type Cmd = { old: string; new: string; ref: string };
const ZERO = "0".repeat(40);

// Namespace = first two path components under refs/ (refs/heads/main, refs/heads/team-a/*, refs/tags/*).
function shardOf(ref: string): string {
  const p = ref.split("/");                       // ["refs","heads","team-a","feature"]
  return p.length > 3 ? p.slice(0, 3).join("/") : p.slice(0, 3).join("/"); // refs/heads/team-a | refs/heads/main
}
const stub = (env: Env, repo: string, ns: string) => env.SHARD.get(env.SHARD.idFromName(`${repo}#${ns}`));

export class RefShard extends DurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS refs (name TEXT PRIMARY KEY, sha TEXT NOT NULL);
                          CREATE TABLE IF NOT EXISTS reflog (seq INTEGER PRIMARY KEY, name TEXT, old TEXT, new TEXT, at INTEGER)`);
  }
  // ls-refs for this namespace; ref-prefix filter applied in SQL so busy shards return only what was asked.
  listRefs(prefix = ""): { name: string; sha: string }[] {
    return this.ctx.storage.sql.exec("SELECT name, sha FROM refs WHERE name LIKE ? ORDER BY name", prefix + "%").toArray() as any;
  }
  // Atomic CAS across every command that landed in THIS shard. Objects are already in R2.
  async updateRefs(cmds: Cmd[]): Promise<string[]> {
    for (const c of cmds) {                       // connectivity: tip must exist in the shared object store
      if (c.new !== ZERO && !(await this.env.BUCKET.head(`objects/${c.new}`))) return cmds.map(c => `ng ${c.ref} missing-object`);
    }
    return this.ctx.storage.transactionSync(() => {
      const sql = this.ctx.storage.sql;
      for (const c of cmds) {                      // pre-check so a single stale old-sha rolls back the whole shard batch
        const cur = sql.exec("SELECT sha FROM refs WHERE name=?", c.ref).toArray()[0]?.sha ?? ZERO;
        if (cur !== c.old) return cmds.map(x => `ng ${x.ref} fetch first`);
      }
      for (const c of cmds) {
        if (c.new === ZERO) sql.exec("DELETE FROM refs WHERE name=?", c.ref);
        else sql.exec("INSERT INTO refs(name,sha) VALUES(?,?) ON CONFLICT(name) DO UPDATE SET sha=excluded.sha", c.ref, c.new);
        sql.exec("INSERT INTO reflog(name,old,new,at) VALUES(?,?,?,?)", c.ref, c.old, c.new, Date.now());
      }
      return cmds.map(c => `ok ${c.ref}`);
    });
  }
}

export class RepoRoot extends DurableObject<Env> {          // HEAD + registry of live namespaces
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS shards (ns TEXT PRIMARY KEY, seen INTEGER)`);
  }
  register(ns: string[]) { for (const n of ns) this.ctx.storage.sql.exec("INSERT INTO shards(ns,seen) VALUES(?,?) ON CONFLICT(ns) DO UPDATE SET seen=excluded.seen", n, Date.now()); }
  shards(): string[] { return this.ctx.storage.sql.exec("SELECT ns FROM shards").toArray().map((r: any) => r.ns); }
  head(): string { return "refs/heads/main"; }
}

const pkt = (s: string) => (s.length + 4).toString(16).padStart(4, "0") + s;

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const u = new URL(req.url);
    const [, owner, name, ...rest] = u.pathname.split("/");
    const repo = `${owner}/${name}`;
    const root = env.ROOT.get(env.ROOT.idFromName(repo));

    if (rest.join("/") === "git-upload-pack" && req.headers.get("git-protocol")?.includes("version=2")) {
      // v2 ls-refs: `ref-prefix refs/heads/main` -> talk to ONE shard. No prefix -> fan out over registry.
      const prefixes = parseV2Args(await req.text()).filter(a => a.startsWith("ref-prefix ")).map(a => a.slice(11));
      const targets = prefixes.length ? [...new Set(prefixes.map(shardOf))] : await root.shards();
      const lists = await Promise.all(targets.map(ns => stub(env, repo, ns).listRefs(prefixes.find(p => shardOf(p) === ns) ?? "")));
      const body = lists.flat().sort((a, b) => a.name < b.name ? -1 : 1).map(r => pkt(`${r.sha} ${r.name}\n`)).join("") + "0000";
      return new Response(body, { headers: { "content-type": "application/x-git-upload-pack-result" } });
    }

    if (rest.join("/") === "git-receive-pack") {
      const { cmds, pack } = splitCommandsAndPack(req.body!);   // pkt-lines until flush "0000", then "PACK" bytes
      await indexPackToR2(pack, env.BUCKET);                    // PACK header, per-entry inflate, ofs/ref-delta -> objects/<sha> (idea 4/5/6)
      const groups = new Map<string, Cmd[]>();
      for (const c of cmds) (groups.get(shardOf(c.ref)) ?? groups.set(shardOf(c.ref), []).get(shardOf(c.ref))!).push(c);
      // Each shard is its own atomic unit; cross-shard batches are reported per-ref, which report-status allows.
      const results = await Promise.all([...groups].map(([ns, g]) => stub(env, repo, ns).updateRefs(g)));
      await root.register([...groups.keys()]);
      const report = [pkt("unpack ok\n"), ...results.flat().map(l => pkt(l + "\n")), "0000"].join("");
      return new Response(pkt("\x01" + report) + "0000", { headers: { "content-type": "application/x-git-receive-pack-result" } }); // side-band-64k
    }
    return new Response("not found", { status: 404 });
  },
};
declare function parseV2Args(body: string): string[];
declare function splitCommandsAndPack(body: ReadableStream<Uint8Array>): { cmds: Cmd[]; pack: ReadableStream<Uint8Array> };
declare function indexPackToR2(pack: ReadableStream<Uint8Array>, b: R2Bucket): Promise<void>;
```

## Why it works
- Git's consistency unit is the ref, not the repo: `git-receive-pack` reports `ok <ref>` / `ng <ref> <reason>` per command in report-status, so independent shards each answering for their own refs is exactly the wire contract a non-`--atomic` push already accepts.
- Objects are shared, refs are partitioned: because every object lives at `objects/<sha>` in R2 before any ref moves (two-phase push), a shard's connectivity check is an R2 `head` on the tip plus its own graph, and never needs another shard's SQLite. A feature-branch push that assumes `main`'s objects exist finds them in R2 regardless of which DO advanced `main`.
- Protocol v2 `ls-refs` carries `ref-prefix`, and modern clients send it for `git fetch origin main` and for every `git pull`; the Worker turns the prefix into a shard id with `idFromName`, so the hot path for readers of a quiet branch is one DO hop and never queues behind the busy shard's single-threaded input gate.
- CAS on `old` sha inside `transactionSync` in each shard preserves the "fetch first" / non-fast-forward semantics git expects; a stale old-sha rolls back only that shard's batch, matching git's per-ref failure model.
- The registry DO is off the write path except for an idempotent `register` after the ref update, so it cannot become the serialization point the idea is trying to remove; a periodic alarm can reap namespaces whose shard has zero refs.

## Known limits
- `git push --atomic` spanning two namespaces cannot be honored by this alone: each shard commits independently. The Worker must either reject with `ng ... atomic-across-shards` when the client advertised `atomic`, or run the two-phase commit from cross-repo-atomic-push across shards (prepare/lock in each shard, then commit) - that reintroduces coordination, but only for the rare cross-namespace push.
- receive-pack has no v2: its ref advertisement must list every ref, so every push still fans out to all shards for the advertisement (reads only, parallel). With hundreds of namespaces that is hundreds of DO round trips per push; a KV/replicated advertisement (replicated-refs-edge) or a root-DO cached snapshot refreshed by alarm is the practical fix, at the cost of a slightly stale advertisement (harmless: CAS in the shard still rejects stale old-shas).
- Prefix-less `ls-refs` (bare `git ls-remote`, first clone) also fans out; the shard registry must be complete or refs silently vanish from the listing. `register` after `updateRefs` leaves a crash window where a shard has refs but is unregistered; fix by registering before the update (idempotent, cheap).
- Namespace granularity is static (`shardOf`). One hot branch such as `refs/heads/main` still lands on one DO and the single-DO throughput ceiling (roughly hundreds of small CAS writes/sec, one request at a time through the input gate) is unchanged for that branch; sharding helps siblings, not the hot ref itself.
- Connectivity is approximated by an R2 `head` on the tip plus whatever the pack contained; a full reachability walk across a branch that was created in another shard needs a commit-graph table (want-have-negotiation) or bounded R2 walks. Each R2 `head`/`get` is a billed Class B op.
- The Worker still has to index the whole pack within the request: DO 128 MB memory is not involved (the pack never enters a DO), but the Worker's CPU limit (30 s default, configurable up to 5 min on paid) and the streaming inflater bound the maximum single-push size.
- HEAD symref, ACLs and push-options live in the root DO, so a fetch that asks for `symrefs` costs one extra hop.

## Depends on
- repo-do-ref-authority
- refs-sqlite-objects-r2
- content-addressed-r2-keys
- two-phase-push
- streaming-pack-parser
- protocol-v2-only
- auth-and-multitenancy
- cross-repo-atomic-push (only for cross-namespace `--atomic`)
