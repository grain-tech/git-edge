> Idea #32 · wild · verdict: **lands with caveats** · feasibility 4/5 · reliability 3/5 · correctness 3/5 · effort: weeks
> Proof: [proofs/agent-native-commands.md](./agent-native-commands.md) · Review: [reviews/agent-native-commands.md](../../reviews/archived/agent-native-commands.md)

# Agent-native protocol v2 commands (search, explain-diff, suggest-merge)

## Mechanism
The v2 capability advertisement on `GET /:owner/:repo/info/refs?service=git-upload-pack` gains three extra lines (`search`, `explain-diff=ai`, `suggest-merge=dry-run`); real git ignores unknown capabilities, an agent harness that sees them POSTs `command=search` / `command=explain-diff` / `command=suggest-merge` to the same `POST /:owner/:repo/git-upload-pack` endpoint in ordinary pkt-line framing (`command=`, caps, `0001`, args, `0000`), so auth, routing (`idFromName("owner/repo")`) and the pkt-line codec are shared with `ls-refs`/`fetch`. The Worker dispatches each command to the repo DO over RPC: `search` is one `SELECT` against an FTS5 table in DO SQLite that the post-receive step fills with paths and the first 64 KB of each changed text blob; `explain-diff` has the DO resolve `base`/`head` to trees, the Worker reads the changed blobs from R2 (`objects/<sha>`, inflated with `DecompressionStream("deflate")`), builds a unified diff, feeds it to Workers AI and memoises the answer in a DO `explanations(base, head, text)` row; `suggest-merge` runs the same tree/diff3 merge as `server-side-merge` in dry-run mode, never touching refs, and for each conflicting path writes an AI-proposed resolution blob to R2 under `suggest/<owner/repo>/<sha>` so the agent can `fetch` it as a normal object or accept it with a later push. Every response is streamed back as sectioned pkt-lines (`section-name`, lines, `0001` between sections, `0000` at end), exactly like a v2 `fetch` response, so the agent's client uses one parser for everything.

## Primitives
- Workers (fetch handler, `TransformStream` response streaming, `DecompressionStream("deflate")` for loose objects) — GA
- Durable Objects with SQLite storage; `ctx.storage.sql.exec` including the FTS5 virtual-table module, which DO SQLite ships (`CREATE VIRTUAL TABLE ... USING fts5`) — GA
- DO RPC (typed stub methods `search()`, `treesFor()`, `cachedExplanation()`) — GA
- R2 `get`/`put` for objects and `suggest/` resolution blobs — GA
- Workers AI text generation (`env.AI.run("@cf/meta/llama-3.1-8b-instruct", ...)`) — GA, but model availability and pricing per neuron change often; treat the model id as config
- Optional: D1 FTS or Vectorize behind `search` when the index outgrows one DO (`search-index-on-push`, `vectorized-commit-graph`); Vectorize is GA, not needed for the proof

## Proof code
```typescript
import { pkt, parseV2, FLUSH, DELIM, cat } from "./pktline";        // codec from protocol-v2-only
import { mergeTrees, treeOf, unifiedDiff } from "./merge";           // from server-side-merge / diff-api-range-reads
type Env = { REPO: DurableObjectNamespace<RepoDO>; BUCKET: R2Bucket; AI: Ai };
const enc = new TextEncoder();

// ---- repo DO: index + memo tables live next to refs ----
export class RepoDO extends DurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.storage.sql.exec(`
      CREATE VIRTUAL TABLE IF NOT EXISTS blob_fts USING fts5(path, body, sha UNINDEXED, tokenize='unicode61');
      CREATE TABLE IF NOT EXISTS explanations(base TEXT, head TEXT, text TEXT, PRIMARY KEY(base, head))`);
  }
  // post-receive (two-phase-push commit hook) calls this with changed text blobs, truncated to 64 KB
  indexBlobs(rows: { path: string; sha: string; body: string }[]) {
    for (const r of rows) this.ctx.storage.sql.exec(
      "INSERT INTO blob_fts(path, body, sha) VALUES (?, ?, ?)", r.path, r.body.slice(0, 65536), r.sha);
  }
  search(q: string, limit = 20) {                                    // FTS5 MATCH + snippet(), one round trip
    return this.ctx.storage.sql.exec<{ path: string; sha: string; snip: string }>(
      "SELECT path, sha, snippet(blob_fts, 1, '>>', '<<', '…', 12) AS snip FROM blob_fts WHERE blob_fts MATCH ? ORDER BY rank LIMIT ?",
      q, limit).toArray();
  }
  cachedExplanation(base: string, head: string) {
    return this.ctx.storage.sql.exec<{ text: string }>("SELECT text FROM explanations WHERE base=? AND head=?", base, head).one()?.text;
  }
  rememberExplanation(base: string, head: string, text: string) {
    this.ctx.storage.sql.exec("INSERT OR REPLACE INTO explanations VALUES (?,?,?)", base, head, text);
  }
  mergeBase(a: string, b: string): string { /* walk commits(sha,parents) graph in SQLite (want-have-negotiation) */ return ""; }
}

// ---- Worker: the three commands hang off the same POST /git-upload-pack dispatcher ----
const arg = (args: string[], k: string) => args.find(a => a.startsWith(k + " "))?.slice(k.length + 1);

export async function agentCommand(cmd: { name: string; args: string[] }, env: Env, repo: string, stub: DurableObjectStub<RepoDO>) {
  const { readable, writable } = new TransformStream<Uint8Array, Uint8Array>();
  const w = writable.getWriter();
  const line = (s: string) => w.write(pkt(s + "\n"));
  const section = (s: string) => line(s);
  (async () => {
    try {
      if (cmd.name === "search") {                                   // args: query <fts5 expr>, limit N
        await section("results");
        for (const r of await stub.search(arg(cmd.args, "query")!, Number(arg(cmd.args, "limit") ?? 20)))
          await line(`${r.sha} ${r.path}\t${r.snip.replace(/\n/g, " ")}`);
      } else if (cmd.name === "explain-diff") {                      // args: base <oid>, head <oid>
        const base = arg(cmd.args, "base")!, head = arg(cmd.args, "head")!;
        let text = await stub.cachedExplanation(base, head);
        const diff = await unifiedDiff(env, await treeOf(env, base), await treeOf(env, head)); // R2 reads + DecompressionStream
        if (!text) {
          const out = await env.AI.run("@cf/meta/llama-3.1-8b-instruct", {
            messages: [{ role: "system", content: "Explain this git diff for a coding agent: intent, risk, files touched. Be terse." },
                       { role: "user", content: diff.slice(0, 24000) }] });                // stay well inside the model context
          text = (out as { response: string }).response;
          await stub.rememberExplanation(base, head, text);
        }
        await section("summary"); for (const l of text.split("\n")) await line(l);
        await w.write(enc.encode(DELIM));
        await section("files"); for (const f of diff.files) await line(`${f.status} ${f.path} +${f.added} -${f.removed}`);
      } else if (cmd.name === "suggest-merge") {                     // args: ours <oid>, theirs <oid>; never moves a ref
        const ours = arg(cmd.args, "ours")!, theirs = arg(cmd.args, "theirs")!;
        const base = await stub.mergeBase(ours, theirs);
        const m = await mergeTrees(env, await treeOf(env, base), await treeOf(env, ours), await treeOf(env, theirs), "");
        await section("merge");
        await line(`base ${base}`); await line(m.conflicts.length ? "status conflict" : "status clean");
        await w.write(enc.encode(DELIM));
        await section("paths");
        for (const p of m.clean) await line(`clean ${p}`);
        for (const c of m.conflicts) {                                 // c = { path, base, ours, theirs } as text
          const out = await env.AI.run("@cf/meta/llama-3.1-8b-instruct", { messages: [
            { role: "system", content: "Resolve this three-way merge conflict. Output only the merged file." },
            { role: "user", content: `<<<<<<< ours\n${c.ours}\n||||||| base\n${c.base}\n=======\n${c.theirs}\n>>>>>>> theirs` }] });
          const body = enc.encode((out as { response: string }).response);
          const sha = await gitBlobSha(body);                            // sha1("blob <len>\0" + body), the real object id
          await env.BUCKET.put(`suggest/${repo}/${sha}`, zlib(cat(enc.encode(`blob ${body.length}\0`), body)));
          await line(`conflict ${c.path} suggestion ${sha}`);           // agent can `fetch want <sha>` or accept via push
        }
      } else await line(`ERR unknown command ${cmd.name}`);
    } catch (e) { await line(`ERR ${(e as Error).message}`); }
    await w.write(enc.encode(FLUSH)); await w.close();
  })();
  return new Response(readable, { headers: { "content-type": "application/x-git-upload-pack-result" } });
}

// capability advertisement (info/refs, v2) grows by three lines; git clients ignore lines they do not know
export const AGENT_CAPS = [pkt("search\n"), pkt("explain-diff=ai\n"), pkt("suggest-merge=dry-run\n")];

async function gitBlobSha(body: Uint8Array) {
  const h = await crypto.subtle.digest("SHA-1", cat(enc.encode(`blob ${body.length}\0`), body));
  return [...new Uint8Array(h)].map(b => b.toString(16).padStart(2, "0")).join("");
}
declare function zlib(b: Uint8Array): ReadableStream;                  // new Blob([b]).stream().pipeThrough(new CompressionStream("deflate"))
```

## Why it works
- Protocol v2 (`gitprotocol-v2`) is explicitly extensible: the advertisement is a list of `key[=value]` capability lines, "clients MUST ignore any unrecognised capabilities", and a request is just `command=<name>` plus args. Adding `search`, `explain-diff`, `suggest-merge` breaks no `git clone`/`git fetch`, and an agent client needs no new transport, only three new command names on the same `POST git-upload-pack` with the same `Git-Protocol: version=2` header and auth.
- The response shape copies v2 `fetch`: named sections separated by `0001` (delim-pkt), terminated by `0000`. An agent's pkt-line parser that already reads `acknowledgments`/`packfile` reads `summary`/`files` or `merge`/`paths` unchanged, and errors travel as `ERR` lines like git's own `ERR` pkt.
- `search` never touches R2 at request time: the FTS5 table is filled in the post-receive step of the two-phase push (the only moment the server has the changed blobs inflated anyway), so a query is one SQLite statement in the DO, single-digit milliseconds.
- `explain-diff` reads only objects that git itself would read for `git diff base head`: two root trees from R2, the subtrees that differ, and the changed blobs; loose objects are `zlib(<type> <len>\0<body>)` so `DecompressionStream("deflate")` plus a header split yields the exact bytes git would show. The memo table means an agent that re-asks after a retry pays no second AI call.
- `suggest-merge` reuses the real three-way tree/diff3 merge from `server-side-merge` (same merge-base from the SQLite commit graph, same tree-level fast paths) but stops before the ref CAS, so it is a pure read; the suggestion is stored as a genuine git blob (id = sha1 of `blob <len>\0body`), which is why the agent can pull it with an ordinary `fetch want <sha>` or reference it in a tree it pushes back. Nothing about the repo changes unless the agent later pushes.
- Every command is stateless request/response, matching Workers: the DO holds only durable tables (FTS index, memo, refs, commit graph), never a conversation, so concurrent agents on one repo just queue on the DO like concurrent `ls-refs` calls.

## Known limits
- Real `git` cannot issue these commands; only an agent harness with its own pkt-line client can (a `git-remote-*` helper or a tiny `fetch()` wrapper). The idea is honest only as "protocol-v2-shaped RPC on the git endpoint", not as something `git` on a laptop discovers.
- Workers AI calls are the long pole: an 8B model answers a 24 KB diff in several seconds of wall time (not CPU, so the 30 s CPU limit is fine), but a Worker request still has an overall duration budget and a merge with dozens of conflicting files would need to move the AI loop into a DO alarm and let the agent poll (`suggest-merge` returns `pending` plus a token). The proof runs it inline.
- The FTS5 table lives inside one DO's SQLite (10 GB storage cap per DO, and `search` throughput is single-DO throughput); indexing only the first 64 KB of text blobs keeps it bounded but means large files are only path-searchable. Beyond that, `search-index-on-push` (D1 FTS / Vectorize) is the real home.
- `explain-diff` truncates the diff to ~24 KB before the model; big refactors get a summary of their first 24 KB only. Summaries are model output and can be wrong; the `files` section is the ground truth.
- Diff and merge inflate blobs in Worker memory; a single 100 MB+ blob on either side blows the 128 MB isolate limit. The proof does not chunk (same limit as `server-side-merge`), and binary blobs are reported as `conflict` with no suggestion.
- R2 costs: `explain-diff`/`suggest-merge` do one Class B GET per tree and changed blob plus one Class A PUT per suggestion; `suggest/` blobs are orphans until accepted and need the `two-phase-push` janitor alarm to sweep them after a TTL (hand-waved here).
- Auth: `suggest-merge` writes to R2 under a read-only-looking command, so scoped tokens (`scoped-token-remotes`) must treat it as a write-class command or the `suggest/` prefix must be quota-limited per token.

## Depends on
- protocol-v2-only
- info-refs-endpoint
- refs-sqlite-objects-r2
- two-phase-push
- server-side-merge
- diff-api-range-reads
- want-have-negotiation
- search-index-on-push
