> Idea #51 · wild · verdict: **risky** · feasibility 3/5 · reliability 3/5 · correctness 2/5 · effort: weeks
> Proof: [proofs/agent-blame.md](./agent-blame.md) · Review: [reviews/agent-blame.md](../../reviews/archived/agent-blame.md)

# Blame that knows which agent wrote each line

## Mechanism
Agents commit with ordinary git trailers (`Agent-Session: session_01MF…`, `Agent-Model: claude-fable-5-1`), which git already parses (`git interpret-trailers`, `%(trailers:key=Agent-Session)`); nothing about the pack or the wire changes. During the commit step of the two-phase push the repo DO (`idFromName("owner/repo")`) already has every new commit object inflated to fill its `commits(sha, parents)` graph, so it additionally splits the message's last paragraph into trailers and writes `commit_agent(sha, session, model, attested)` in DO SQLite, marking `attested=1` only when the pushing token was itself scoped to that session id. An agent then POSTs `command=blame` (args `path`, `rev`) to `POST /:owner/:repo/git-upload-pack` in pkt-line framing; the Worker asks the DO for a per-path blame that is computed incrementally along first-parent history (blame(C) = blame(parent) + one Myers diff of the two blobs read from R2 `objects/<sha>` via `DecompressionStream("deflate")`) and memoised in a `blame_memo(path, commit, lines)` table, then streams back one pkt-line per line: `<commit> <session|human> <model|-> <lineno> <text>`.

## Primitives
- Durable Objects with SQLite storage (`ctx.storage.sql.exec`) for `commit_agent`, `blame_memo` and the existing `commits` graph — GA
- DO RPC (`stub.blame()`, `stub.recordTrailers()`) — GA
- DO alarms (`ctx.storage.setAlarm`) to finish a cold blame of a long-lived file in the background when the inline budget is exceeded — GA
- R2 `env.BUCKET.get("objects/<sha>")` for the blob at each revision — GA
- Workers streaming `Response` over `TransformStream` + `DecompressionStream("deflate")` for loose objects — GA
- Nothing beta; no AI, no Vectorize

## Proof code
```typescript
import { DurableObject } from "cloudflare:workers";
import { pkt, FLUSH } from "./pktline";                          // from protocol-v2-only
import { blobAtPath, myersDiff } from "./objects";               // R2 get + DecompressionStream; LCS diff (diff-api-range-reads)
type Env = { REPO: DurableObjectNamespace<RepoDO>; BUCKET: R2Bucket };
type Line = { commit: string; text: string };

// git trailer rules: last paragraph of the message, every line `Token: value` (or a folded continuation line)
export function trailers(commitObj: string): Record<string, string> {
  const msg = commitObj.slice(commitObj.indexOf("\n\n") + 2);      // skip tree/parent/author/committer headers
  const para = msg.trimEnd().split(/\n\s*\n/).pop() ?? "";
  const out: Record<string, string> = {}; let last = "";
  for (const l of para.split("\n")) {
    const m = /^([A-Za-z0-9-]+):\s*(.*)$/.exec(l);
    if (m) { out[last = m[1]] = m[2]; } else if (/^\s/.test(l) && last) out[last] += " " + l.trim(); else return {};
  }
  return out;
}

export class RepoDO extends DurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS commit_agent(sha TEXT PRIMARY KEY, session TEXT, model TEXT, attested INTEGER);
      CREATE TABLE IF NOT EXISTS blame_memo(path TEXT, commit_sha TEXT, lines TEXT, PRIMARY KEY(path, commit_sha));
      CREATE TABLE IF NOT EXISTS blame_jobs(path TEXT, rev TEXT, PRIMARY KEY(path, rev))`);
  }
  // called from two-phase-push commit step, once per new commit, with the inflated commit object and the push token's scope
  recordTrailers(sha: string, commitObj: string, tokenSession: string | null) {
    const t = trailers(commitObj);
    if (!t["Agent-Session"]) return;
    this.ctx.storage.sql.exec("INSERT OR REPLACE INTO commit_agent VALUES (?,?,?,?)",
      sha, t["Agent-Session"], t["Agent-Model"] ?? null, t["Agent-Session"] === tokenSession ? 1 : 0);
  }
  firstParent(sha: string): string | null {                        // commits(sha, parents) from want-have-negotiation
    return this.ctx.storage.sql.exec<{ parents: string }>("SELECT parents FROM commits WHERE sha=?", sha).one()?.parents.split(" ")[0] || null;
  }
  // blame(C) = blame(first-parent(C)) carried through a diff of the two blobs; memoised per (path, commit)
  async blame(path: string, rev: string, budget = 64): Promise<Line[] | "pending"> {
    const memo = this.ctx.storage.sql.exec<{ lines: string }>("SELECT lines FROM blame_memo WHERE path=? AND commit_sha=?", path, rev).one();
    if (memo) return JSON.parse(memo.lines);
    if (budget === 0) {                                             // cold file with a long history: finish in an alarm, tell the agent
      this.ctx.storage.sql.exec("INSERT OR IGNORE INTO blame_jobs VALUES (?,?)", path, rev);
      await this.ctx.storage.setAlarm(Date.now() + 100); return "pending";
    }
    const cur = await blobAtPath(this.env.BUCKET, rev, path);      // tree walk + R2 get, null if path absent at rev
    if (cur === null) return [];
    const parent = this.firstParent(rev);
    const base = parent ? await this.blame(path, parent, budget - 1) : [];
    if (base === "pending") return base;
    const out: Line[] = [];                                         // walk the diff hunks: equal lines inherit, inserted lines get `rev`
    for (const h of myersDiff(base.map(l => l.text), cur.split("\n")))
      if (h.op === "eq") out.push(base[h.aIdx]); else if (h.op === "ins") out.push({ commit: rev, text: h.text });
    this.ctx.storage.sql.exec("INSERT OR REPLACE INTO blame_memo VALUES (?,?,?)", path, rev, JSON.stringify(out));
    return out;
  }
  async alarm() {                                                   // drain queued cold blames, 1000 revisions per wake
    for (const j of this.ctx.storage.sql.exec<{ path: string; rev: string }>("SELECT * FROM blame_jobs").toArray()) {
      if (await this.blame(j.path, j.rev, 1000) !== "pending")
        this.ctx.storage.sql.exec("DELETE FROM blame_jobs WHERE path=? AND rev=?", j.path, j.rev);
    }
    if (this.ctx.storage.sql.exec("SELECT 1 FROM blame_jobs").toArray().length) await this.ctx.storage.setAlarm(Date.now() + 100);
  }
  agents(shas: string[]) {
    return this.ctx.storage.sql.exec<{ sha: string; session: string; model: string | null; attested: number }>(
      `SELECT * FROM commit_agent WHERE sha IN (${shas.map(() => "?").join(",")})`, ...shas).toArray();
  }
}

// Worker side: hangs off the same v2 command dispatcher as ls-refs/fetch (agent-native-commands)
export async function blameCommand(args: string[], stub: DurableObjectStub<RepoDO>) {
  const arg = (k: string) => args.find(a => a.startsWith(k + " "))!.slice(k.length + 1);
  const { readable, writable } = new TransformStream<Uint8Array, Uint8Array>();
  const w = writable.getWriter(); const line = (s: string) => w.write(pkt(s + "\n"));
  (async () => {
    const res = await stub.blame(arg("path"), arg("rev"));
    if (res === "pending") { await line("blame"); await line("status pending"); }
    else {
      const who = new Map((await stub.agents([...new Set(res.map(l => l.commit))])).map(a => [a.sha, a]));
      await line("blame");
      res.forEach((l, i) => { const a = who.get(l.commit);
        line(`${l.commit} ${a ? a.session + (a.attested ? "" : "?") : "human"} ${a?.model ?? "-"} ${i + 1} ${l.text}`); });
    }
    await w.write(new TextEncoder().encode(FLUSH)); await w.close();
  })();
  return new Response(readable, { headers: { "content-type": "application/x-git-upload-pack-result" } });
}
export const BLAME_CAP = pkt("blame=agent\n");                     // added to the v2 advertisement; real git ignores it
```

## Why it works
- Trailers are a real git feature, not an invention: `git commit --trailer "Agent-Session=…"` writes them, `git log --format=%(trailers:key=Agent-Session,valueonly)` reads them, and `git interpret-trailers` defines the parse (last paragraph, `Token: value`, folded continuations). The commit object on the wire is unchanged, so `git push` from any agent harness works and a plain `git blame --porcelain` plus that `git log` format gives the same answer client-side.
- The server sees every new commit inflated exactly once, in the two-phase-push commit step that already fills the `commits(sha, parents)` graph for want/have negotiation; parsing the trailer there costs a string split, no extra R2 read.
- Blame is defined incrementally: for a first-parent chain, a line unchanged by the diff `blob(parent) -> blob(C)` keeps its attribution, an inserted line is attributed to `C`. That is what `git blame` does before its rename/copy heuristics, and memoising per `(path, commit)` means each revision is diffed once ever; a blame of `HEAD` after one new push is one R2 blob read and one diff.
- The `blame` command rides the v2 extension rules (`gitprotocol-v2`: clients MUST ignore unknown capabilities, requests are `command=<name>` + args), so `git clone`/`fetch` are untouched and an agent uses the same `POST git-upload-pack`, same auth, same pkt-line parser as `fetch`.
- Attestation is honest about trust: the trailer is client-asserted, so the DO marks it `attested` only when the pushing token (`scoped-token-remotes`) was issued to that same session id; the response prints `session?` for unverified claims.

## Known limits
- Attribution is first-parent only and ignores renames, copies and `-M/-C` movement detection; a line that moved across files is credited to the commit that moved it. Merge commits credit the merge to whichever side is not first-parent. This is "blame that knows which agent" for the common agent workflow (linear branches), not a full `git blame` reimplementation.
- Cold blame of a file with thousands of revisions needs one R2 GET and one diff per revision; the inline budget is 64 revisions (fits a 30 s CPU Worker/DO request), the rest finishes in a DO alarm chain and the agent gets `status pending` and must re-ask. Memo rows are `JSON` of the whole file per (path, commit): for a 10k-line file touched 1k times that is ~1 GB of SQLite, so the memo needs an eviction policy (keep tips only); hand-waved here.
- A single DO serialises all blame computation for the repo alongside pushes; a fleet of agents blaming different files at once queue on it (`branch-level-dos` would not help, blame is per-repo state).
- Trailer truth is only as good as the token binding: unscoped tokens produce `session?` and a lying agent can claim any session id. A human squashing agent commits loses the trailers unless they carry them forward.
- Real `git` cannot issue `command=blame`; only an agent client with a pkt-line writer can. The client-side equivalent (`git blame --porcelain` + trailer lookup) needs a clone, which is exactly what zero-clone agents want to avoid.
- Whole blobs are inflated into DO memory per revision (128 MB isolate limit); the proof does not handle files over a few tens of MB or binary files (should return an empty blame).

## Depends on
- protocol-v2-only
- refs-sqlite-objects-r2
- two-phase-push
- want-have-negotiation
- agent-native-commands
- scoped-token-remotes
- diff-api-range-reads
