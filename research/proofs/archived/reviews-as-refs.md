> Idea #47 · wild · verdict: **lands with caveats** · feasibility 4/5 · reliability 3/5 · correctness 3/5 · effort: weeks
> Proof: [proofs/reviews-as-refs.md](./reviews-as-refs.md) · Review: [reviews/reviews-as-refs.md](../../reviews/archived/reviews-as-refs.md)

# Pull-request review data as git objects under refs/reviews

## Mechanism
`POST /:owner/:repo/reviews/:id/comments` (JSON `{path,line,body,author}`) hits the Worker, which resolves `idFromName(owner/repo)` and calls `repoDO.appendReviewEvent(id, event)`. The DO reads the current tip of `refs/reviews/<id>` from its SQLite `refs` table, then synthesizes three ordinary git objects in memory — a blob holding the event JSON, a tree that is the previous tip's tree plus `events/<seq>-<ulid>.json` (plus a `meta.json` blob with the reviewed commit SHA, base/head refs, state), and a commit whose sole parent is the previous tip — hashes each with `crypto.subtle.digest("SHA-1")` over `"<type> <len>\0<body>"`, deflates them with `CompressionStream("deflate")` and `put`s them to R2 at `objects/<sha[0:2]>/<sha[2:]>` (the loose-object layout), and finally compare-and-swaps the ref and inserts a `commits` row so `want-have-negotiation` sees it. Because the result is a plain fast-forward-only branch of plain git objects, `git fetch origin '+refs/reviews/*:refs/reviews/*'` (or `git clone` with `remote.origin.fetch` extended) carries every comment, approval and state change as a commit, and `git log -p refs/reviews/42` is the review transcript. A client may also `git push origin refs/reviews/42` directly (e.g. offline review, or another forge's export); `git-receive-pack` goes through the normal `two-phase-push` path, and the DO's pre-receive rule for the `refs/reviews/` namespace only requires fast-forward plus a tree shape check (top-level `meta.json`, `events/` directory, no other entries).

## Primitives
- Durable Objects with SQLite storage (`ctx.storage.sql.exec`, `transactionSync`) for the ref CAS and the `commits` graph rows — GA
- DO RPC (`DurableObject` from `cloudflare:workers`) from the Worker — GA
- R2 `put`/`get` of loose objects, keyed by SHA (`content-addressed-r2-keys`) — GA
- Workers WebCrypto `SHA-1` digest (git object ids; SHA-256 repos would swap the algorithm and object-format capability) — GA
- `CompressionStream("deflate")` / `DecompressionStream("deflate")` for zlib loose-object framing — GA in Workers
- Nothing beta is required.

## Proof code
```typescript
import { DurableObject } from "cloudflare:workers";
type Env = { BUCKET: R2Bucket };
type ReviewEvent = { kind: "comment" | "approve" | "request-changes" | "state"; path?: string; line?: number; body?: string; author: string };
type Obj = { type: "blob" | "tree" | "commit"; body: Uint8Array };
const enc = new TextEncoder();
const hex = (b: ArrayBuffer) => [...new Uint8Array(b)].map(x => x.toString(16).padStart(2, "0")).join("");
const cat = (...parts: Uint8Array[]) => { const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0)); let o = 0; for (const p of parts) { out.set(p, o); o += p.length; } return out; };
const hexToBytes = (h: string) => Uint8Array.from(h.match(/../g)!, x => parseInt(x, 16));

/** Loose-object framing: "<type> <len>\0" + body; id = SHA-1 of that; on disk it is zlib(deflate) of that. */
async function frame(o: Obj): Promise<{ sha: string; loose: Uint8Array }> {
  const raw = cat(enc.encode(`${o.type} ${o.body.length}\0`), o.body);
  const sha = hex(await crypto.subtle.digest("SHA-1", raw));
  const z = new Blob([raw]).stream().pipeThrough(new CompressionStream("deflate"));   // "deflate" == zlib wrapper, what git expects
  return { sha, loose: new Uint8Array(await new Response(z).arrayBuffer()) };
}
/** Tree entry: "<mode> <name>\0" + 20 raw SHA bytes; entries sorted by name (git sorts trees byte-wise, dirs as "name/"). */
function tree(entries: { mode: "100644" | "40000"; name: string; sha: string }[]): Obj {
  const sorted = [...entries].sort((a, b) => (a.name + (a.mode === "40000" ? "/" : "")).localeCompare(b.name + (b.mode === "40000" ? "/" : "")));
  return { type: "tree", body: cat(...sorted.map(e => cat(enc.encode(`${e.mode} ${e.name}\0`), hexToBytes(e.sha)))) };
}
function commit(treeSha: string, parents: string[], who: string, msg: string): Obj {
  const ts = Math.floor(Date.now() / 1000);
  const lines = [`tree ${treeSha}`, ...parents.map(p => `parent ${p}`), `author ${who} ${ts} +0000`, `committer git-edge <reviews@git-edge> ${ts} +0000`, "", msg, ""];
  return { type: "commit", body: enc.encode(lines.join("\n")) };
}

export class RepoDO extends DurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS refs    (name TEXT PRIMARY KEY, sha TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS commits (sha TEXT PRIMARY KEY, parents TEXT NOT NULL, tree TEXT NOT NULL);   -- shared with want-have-negotiation
      CREATE TABLE IF NOT EXISTS review_events (review TEXT, seq INTEGER, sha TEXT, PRIMARY KEY (review, seq));`);
  }

  private async putObj(o: Obj): Promise<string> {
    const { sha, loose } = await frame(o);
    await this.env.BUCKET.put(`objects/${sha.slice(0, 2)}/${sha.slice(2)}`, loose);        // idempotent: same bytes, same key
    return sha;
  }
  private async readTreeEntries(treeSha: string) {                                          // inflate a stored tree to extend it
    const r = await this.env.BUCKET.get(`objects/${treeSha.slice(0, 2)}/${treeSha.slice(2)}`);
    const raw = new Uint8Array(await new Response(r!.body.pipeThrough(new DecompressionStream("deflate"))).arrayBuffer());
    const entries: { mode: "100644" | "40000"; name: string; sha: string }[] = [];
    for (let i = raw.indexOf(0) + 1; i < raw.length;) {                                    // skip "tree <len>\0" header
      const sp = raw.indexOf(0x20, i), nul = raw.indexOf(0, sp);
      entries.push({ mode: new TextDecoder().decode(raw.subarray(i, sp)) as any, name: new TextDecoder().decode(raw.subarray(sp + 1, nul)), sha: hex(raw.slice(nul + 1, nul + 21).buffer) });
      i = nul + 21;
    }
    return entries;
  }

  /** Append one review event as a commit on refs/reviews/<id>. Returns the new tip. */
  async appendReviewEvent(id: string, ev: ReviewEvent, meta?: { head: string; base: string }): Promise<string> {
    const ref = `refs/reviews/${id}`;
    const tip = this.ctx.storage.sql.exec<{ sha: string }>("SELECT sha FROM refs WHERE name=?", ref).toArray()[0]?.sha;
    const seq = (this.ctx.storage.sql.exec<{ n: number }>("SELECT COUNT(*) n FROM review_events WHERE review=?", id).one().n) + 1;

    const evSha = await this.putObj({ type: "blob", body: enc.encode(JSON.stringify({ ...ev, seq, at: new Date().toISOString() })) });
    let root = tip ? await this.readTreeEntries(this.ctx.storage.sql.exec<{ tree: string }>("SELECT tree FROM commits WHERE sha=?", tip).one().tree) : [];
    let events = root.find(e => e.name === "events") ? await this.readTreeEntries(root.find(e => e.name === "events")!.sha) : [];
    events.push({ mode: "100644", name: `${String(seq).padStart(6, "0")}-${ev.kind}.json`, sha: evSha });
    const eventsSha = await this.putObj(tree(events));
    const metaSha = tip ? root.find(e => e.name === "meta.json")!.sha
                        : await this.putObj({ type: "blob", body: enc.encode(JSON.stringify({ id, ...meta, state: "open" })) });
    const treeSha = await this.putObj(tree([{ mode: "100644", name: "meta.json", sha: metaSha }, { mode: "40000", name: "events", sha: eventsSha }]));
    const newTip = await this.putObj(commit(treeSha, tip ? [tip] : [], ev.author, `review ${id}: ${ev.kind}${ev.path ? ` ${ev.path}:${ev.line}` : ""}`));

    this.ctx.storage.transactionSync(() => {                                               // CAS: ref must still be where we read it
      const cur = this.ctx.storage.sql.exec<{ sha: string }>("SELECT sha FROM refs WHERE name=?", ref).toArray()[0]?.sha;
      if (cur !== tip) throw new Error("refs/reviews CAS failed; retry");
      this.ctx.storage.sql.exec("INSERT INTO commits (sha, parents, tree) VALUES (?, ?, ?)", newTip, tip ?? "", treeSha);
      this.ctx.storage.sql.exec("INSERT OR REPLACE INTO refs (name, sha) VALUES (?, ?)", ref, newTip);
      this.ctx.storage.sql.exec("INSERT INTO review_events (review, seq, sha) VALUES (?, ?, ?)", id, seq, newTip);
    });
    return newTip;   // ls-refs now advertises "<newTip> refs/reviews/<id>"; the loose objects are reachable for `fetch`
  }

  /** Pre-receive rule for client pushes into refs/reviews/*: fast-forward only, tree shape fixed. */
  reviewPushAllowed(oldSha: string, newSha: string, isFastForward: boolean, topLevel: string[]): boolean {
    if (oldSha !== "0".repeat(40) && !isFastForward) return false;
    return topLevel.length === 2 && topLevel.includes("meta.json") && topLevel.includes("events");
  }
}
```

## Why it works
- Git does not care what a ref is called: `ls-refs` (protocol v2) advertises every row in `refs`, and a client fetches `refs/reviews/*` with an explicit refspec exactly as it fetches `refs/pull/*/head` from GitHub today. `refs/reviews/` is not `refs/heads/`, so it never shows up as a branch, is never checked out, and survives `git branch -D`.
- The objects are byte-for-byte what `git hash-object` / `git mktree` / `git commit-tree` would produce: `"<type> <len>\0"` header, SHA-1 over header+body, zlib-deflated loose object. Any pack builder that already serves loose objects from R2 (`streaming-pack-parser` writes them the same way) can send them; `git fsck` on the clone passes.
- Append-only review history maps onto fast-forward commits: each event is one commit with one parent, so `git log refs/reviews/42` is the chronological transcript, `git show` on a commit is the diff of the JSON that changed, and two people committing concurrently is just the ref CAS in the DO rejecting the loser, identical to two concurrent pushes to a branch.
- A clone is self-contained: mirror the repo with `git clone --mirror` and the reviews come with it, because they are reachable objects under an advertised ref. Migrating between forges is `git push --mirror`.
- The pre-receive rule in the DO is enough for client-side pushes: fast-forward is checked against `commits.parents` (already needed by `want-have-negotiation`), and the tree shape check only needs the top-level tree entries, which the push parser already has after inflating the commit and its tree.
- GC (`gc-and-repack-alarm`) walks from every ref, so review objects are protected exactly like code objects; if a PR branch is deleted, `meta.json` still names the reviewed SHA, and keeping that commit reachable is a one-line policy (add the head SHA as a second parent of the first review commit, at the cost of `git log` on the review ref then including code history).

## Known limits
- Extending the `events/` tree re-reads and rewrites the whole tree per event: two R2 GETs and four R2 PUTs per comment, and the tree grows linearly, so a review with 10,000 events has a ~600 KB tree object rewritten every time. Fix is sharding `events/<seq/100>/` (same code, two levels); not shown.
- Loose objects only: no delta compression, so each event costs a separate small R2 object and a clone of a review-heavy repo transfers thousands of tiny objects until `gc-and-repack-alarm` packs them. R2 per-request cost (Class A on put) dominates at ~4 puts per comment.
- Single-DO serialization: every comment on every PR of a repo goes through the one repo DO. Fine for human review rates; an agent swarm commenting at hundreds/s would need `branch-level-dos` sharding by review id.
- Threads, edits and deletions are events, not mutations: a comment is never rewritten, only superseded by a later `edit`/`delete` event referring to its seq. Clients that want a GitHub-style materialized view have to fold the log; the server can cache that view in SQLite but it is not shown.
- Rich UI concepts (line anchoring across force-pushes, suggested changes, CODEOWNERS gating) are policy on top; the events carry `path`/`line` plus the reviewed SHA and nothing more.
- The proof hard-codes SHA-1 and a UTC committer; SHA-256 repos need the `object-format=sha256` capability in `ls-refs` and 32-byte tree entries.
- No `alarm` is needed; all consistency is the ref CAS. The 30 s Worker CPU limit and DO 128 MB memory are far from being hit (objects are KB-sized), unless someone pushes a multi-MB review tree directly, which the tree shape rule does not bound; add a size cap in pre-receive.

## Depends on
- repo-do-ref-authority
- refs-sqlite-objects-r2
- content-addressed-r2-keys
- streaming-pack-parser
- two-phase-push
- want-have-negotiation
- gc-and-repack-alarm
