> Idea #41 · wild · verdict: **lands with caveats** · feasibility 3/5 · reliability 3/5 · correctness 3/5 · effort: weeks
> Proof: [proofs/branch-preview-workers.md](./branch-preview-workers.md) · Review: [reviews/branch-preview-workers.md](../../reviews/archived/branch-preview-workers.md)

# Branch previews deployed as Workers on push

## Mechanism
`git push origin HEAD:preview/foo` arrives at the edge Worker as `POST /:owner/:repo/git-receive-pack`; the pack is unpacked into R2 (`objects/<sha>`) and the repo DO does its normal ref CAS. When the updated ref matches `refs/heads/preview/*`, the DO inserts a row into a `deploys` table in its SQLite and calls `ctx.storage.setAlarm(now)` so the client gets its `unpack ok` / `ok refs/heads/preview/foo` report-status immediately instead of waiting on a deploy. The alarm walks the commit's root tree out of R2 (inflate each object with `DecompressionStream("deflate")`, parse `tree` entries), assembles a `multipart/form-data` body, and `PUT`s it to the Workers for Platforms API `accounts/:acct/workers/dispatch/namespaces/:ns/scripts/<repo>--<branch>`. A separate dynamic-dispatch Worker with a `dispatch_namespaces` binding serves `https://<branch>--<repo>.preview.example.com` by `env.DISPATCH.get(name).fetch(req)`; a push that deletes the branch (`:preview/foo`) queues a `DELETE` of the same script.

## Primitives
- Durable Object with SQLite storage (`ctx.storage.sql.exec`) for refs + a `deploys` job table — GA
- DO alarms (`ctx.storage.setAlarm`, `alarm()`) as the post-receive job runner — GA
- R2 `get` of loose objects, `DecompressionStream` for zlib inflate — GA
- Workers for Platforms: dispatch namespaces, script upload API (`PUT .../dispatch/namespaces/:ns/scripts/:name` with multipart metadata + modules), `dispatch_namespaces` binding on a dynamic-dispatch Worker — GA, but a paid add-on to the Workers Paid plan
- Wildcard custom hostname (`*.preview.example.com`) routed to the dispatch Worker — GA (needs the zone on Cloudflare)
- Optional: Workers Static Assets on WfP scripts for pure static previews — GA as of 2025 but uses a separate assets-upload-session flow (not shown)
- Account API token stored as a Worker secret (`env.CF_API_TOKEN`) — GA

## Proof code
```typescript
// Repo DO: post-receive side only. Assumes refs live in SQLite and objects are
// zlib-compressed loose git objects at R2 key `objects/<sha>` (see depends-on).
export class RepoDO extends DurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS deploys (
      id INTEGER PRIMARY KEY AUTOINCREMENT, ref TEXT, sha TEXT, del INTEGER DEFAULT 0)`);
  }

  // Called by the receive-pack handler after the ref CAS succeeded. Returns at once
  // so report-status ("ok refs/heads/preview/foo") flushes to the client.
  async onRefUpdated(repo: string, ref: string, oldSha: string, newSha: string) {
    if (!ref.startsWith("refs/heads/preview/")) return;
    const del = /^0{40}$/.test(newSha) ? 1 : 0;
    this.ctx.storage.sql.exec("INSERT INTO deploys (ref, sha, del) VALUES (?, ?, ?)", ref, newSha, del);
    await this.ctx.storage.setAlarm(Date.now()); // fire ASAP, survives DO eviction
  }

  async alarm() {
    const job = this.ctx.storage.sql.exec("SELECT * FROM deploys ORDER BY id LIMIT 1").one() as any;
    if (!job) return;
    const script = scriptName(this.ctx.id.name!, job.ref);         // "<repo>--<branch>", [a-z0-9-], <=63
    const base = `https://api.cloudflare.com/client/v4/accounts/${this.env.CF_ACCOUNT_ID}` +
      `/workers/dispatch/namespaces/${this.env.WFP_NAMESPACE}/scripts/${script}`;
    const auth = { authorization: `Bearer ${this.env.CF_API_TOKEN}` };
    if (job.del) {
      await fetch(base, { method: "DELETE", headers: auth });
    } else {
      const commit = await this.readObject(job.sha);                 // "commit <n>\0tree <sha>\n..."
      const treeSha = /tree ([0-9a-f]{40})/.exec(new TextDecoder().decode(commit.body))![1];
      const form = new FormData();
      form.set("metadata", JSON.stringify({ main_module: "index.js", compatibility_date: "2025-09-01",
        bindings: [{ type: "plain_text", name: "GIT_SHA", text: job.sha }] }));
      await this.walkTree(treeSha, "", (path, bytes) => form.set(path, new File([bytes], path,
        { type: path.endsWith(".js") || path.endsWith(".mjs") ? "application/javascript+module" : "text/plain" })));
      const r = await fetch(base, { method: "PUT", headers: auth, body: form });
      if (!r.ok) console.error("wfp upload failed", r.status, await r.text());
    }
    this.ctx.storage.sql.exec("DELETE FROM deploys WHERE id = ?", job.id);
    if (this.ctx.storage.sql.exec("SELECT 1 FROM deploys LIMIT 1").toArray().length)
      await this.ctx.storage.setAlarm(Date.now());                  // one job per alarm, chain the rest
  }

  // R2 loose object -> inflate -> split "type size\0" header from body.
  private async readObject(sha: string): Promise<{ type: string; body: Uint8Array }> {
    const obj = await this.env.BUCKET.get(`objects/${sha}`);
    if (!obj) throw new Error(`missing object ${sha}`);
    const raw = new Uint8Array(await new Response(
      obj.body.pipeThrough(new DecompressionStream("deflate"))).arrayBuffer());
    const nul = raw.indexOf(0);
    const type = new TextDecoder().decode(raw.subarray(0, nul)).split(" ")[0];
    return { type, body: raw.subarray(nul + 1) };
  }

  // tree entry on disk: "<mode> <name>\0" + 20 raw sha bytes, repeated. Recurse on mode 40000.
  private async walkTree(sha: string, prefix: string, emit: (p: string, b: Uint8Array) => void) {
    const { body } = await this.readObject(sha);
    let i = 0;
    while (i < body.length) {
      const sp = body.indexOf(0x20, i), nul = body.indexOf(0, sp);
      const mode = new TextDecoder().decode(body.subarray(i, sp));
      const name = new TextDecoder().decode(body.subarray(sp + 1, nul));
      const child = [...body.subarray(nul + 1, nul + 21)].map(b => b.toString(16).padStart(2, "0")).join("");
      i = nul + 21;
      if (mode === "40000") await this.walkTree(child, `${prefix}${name}/`, emit);
      else if (mode !== "160000") emit(prefix + name, (await this.readObject(child)).body); // skip submodules
    }
  }
}

function scriptName(repo: string, ref: string) {
  return `${repo}--${ref.slice("refs/heads/preview/".length)}`
    .toLowerCase().replace(/[^a-z0-9-]/g, "-").slice(0, 63);
}

// Separate dynamic-dispatch Worker (wrangler: "dispatch_namespaces":[{"binding":"DISPATCH","namespace":"previews"}])
export default {
  async fetch(req: Request, env: { DISPATCH: DispatchNamespace }) {
    const script = new URL(req.url).hostname.split(".")[0];          // "<repo>--<branch>.preview.example.com"
    try { return await env.DISPATCH.get(script).fetch(req); }
    catch (e: any) { return new Response(e.message?.includes("not found") ? "no such preview" : "error", { status: 404 }); }
  },
};
```

## Why it works
- git-receive-pack's contract ends at `report-status`: once the pack is unpacked and the ref CAS has succeeded the server must answer `unpack ok` / `ok <ref>` and may do anything afterwards. Queuing a SQLite row plus `setAlarm` is the serverless equivalent of a post-receive hook: it runs after the response, and is persisted so it survives the DO being evicted.
- A commit object's body starts with `tree <sha>`; a tree object is a flat list of `<mode> <name>\0<20-byte sha>` entries with `40000` for subtrees. That is the entire format needed to reconstitute a working tree from content-addressed blobs, no index or checkout required.
- Loose objects are `zlib(type SP size NUL body)`, so `DecompressionStream("deflate")` (zlib-wrapped deflate, which is what git writes) yields the header and body directly from an R2 stream.
- The Workers for Platforms upload API accepts exactly what the tree walk produces: a multipart body with a `metadata` part naming `main_module` and one part per module file. A repo whose root contains `index.js` is deployable with no build step; the dispatch Worker then resolves `<repo>--<branch>` by hostname and forwards the request.
- Branch deletion in git is a push of `0{40}` as the new sha; mapping it to `DELETE` on the same script name gives preview teardown for free.
- The alarm chain processes one deploy per invocation and re-arms itself, so a burst of pushes to many preview branches serializes cleanly inside the single repo DO without a queue service.

## Known limits
- Workers for Platforms is a paid add-on, and the script upload API needs an account-level API token stored as a secret in the git-edge Worker; if that token leaks every preview namespace is writable. Namespace-scoped tokens exist but the DO still holds a broad credential.
- No build step: the tree must already be a deployable Worker (ES modules at the root, `index.js` entry). Anything needing `npm install`, bundling, or TypeScript compilation cannot run in a Worker or DO. A build must happen client-side (push the `dist/` tree) or in an external service, which is the biggest hand-wave relative to "deploys the tree".
- The whole tree is held in memory as a FormData: DO 128 MB and the WfP script size limit (10 MB gzipped, 100 modules default) bound preview size. Static-asset-heavy sites should use the Workers Static Assets upload-session flow on the WfP script instead of modules; that is a three-call protocol (manifest -> upload buckets -> completion JWT) not shown here.
- The tree walk does one R2 GET per object; a 5,000-file tree is 5,000+ class-B R2 reads per push, sequential in this code. Parallelising with `Promise.all` per directory helps but the 30 s CPU limit on the alarm still applies (wall-clock I/O does not count; inflate CPU does).
- Objects that only exist inside a pack (ofs-delta / ref-delta) cannot be read by `objects/<sha>`; this assumes the receive path exploded the pack into loose objects, or an object-lookup layer that resolves pack offsets.
- Single repo DO serializes all preview deploys for the repo; a busy monorepo would want the deploy job sharded (branch-level-dos) or dispatched to a separate deployer DO.
- Wildcard hostnames (`*.preview.example.com`) require the zone on Cloudflare and a wildcard route; `workers.dev` subdomains do not support this pattern for dispatch-namespace scripts.
- Upload failures are logged and the job dropped; a production version needs retry/backoff in the alarm and a status ref (e.g. `refs/previews/foo` pointing at a deploy-record blob) so `git ls-remote` can show deploy state.

## Depends on
repo-do-ref-authority, refs-sqlite-objects-r2, content-addressed-r2-keys, streaming-pack-parser, hooks-as-workers (same post-receive slot; this is a built-in hook rather than a user Worker), alarm-chain-ci (optional: use its stage chain instead of the inline alarm)
