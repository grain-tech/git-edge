> Idea #43 · wild · verdict: **lands with caveats** · feasibility 4/5 · reliability 2/5 · correctness 3/5 · effort: weeks
> Proof: [proofs/server-side-bisect.md](./server-side-bisect.md) · Review: [reviews/server-side-bisect.md](../../reviews/archived/server-side-bisect.md)

# Bisect on the server with parallel test Workers

## Mechanism
`POST /:owner/:repo/bisect {good, bad, tester}` hits the edge Worker, which forwards to the repo DO (`idFromName(owner/repo)`). The DO linearizes the `good..bad` first-parent chain from its SQLite `commit_graph` table, inserts a `bisect` row plus one `bisect_probe` row per commit, and schedules an immediate alarm. Each alarm run picks k evenly spaced untested midpoints of the still-suspect interval, calls the user's test Worker k times concurrently over a service binding / dispatch namespace (payload: commit SHA + a scoped read token), records verdicts in `bisect_probe`, shrinks the interval to the one segment whose left end is good and right end is bad, and re-alarms until the interval has length 1; the test Worker never checks out anything, it reads tree/blob objects straight from R2 by SHA. `GET /bisect/:id` returns the row (`state`, `culprit`).

## Primitives
- Durable Object with SQLite storage (`ctx.storage.sql.exec`) for the commit graph, bisect session, and probe results
- DO alarms (`ctx.storage.setAlarm`) to run each bisect round as its own invocation, so a long bisect never holds an HTTP request open
- Service bindings (user's test Worker bound as `TESTER`) or Workers for Platforms dispatch namespaces (GA, paid add-on) when the tester is tenant-supplied
- R2 `env.BUCKET.get(sha)` for loose objects, read by the test Worker; `DecompressionStream("deflate")` to inflate the zlib object
- `Promise.allSettled` fan-out of subrequests inside one alarm invocation
- Optional: WebSocket hibernation to push progress instead of polling (not required)

## Proof code
```typescript
// repo DO — bisect portion only. Assumes commit_graph(sha, parent1, ...) exists (want-have-negotiation).
type Verdict = "good" | "bad" | "skip";

export class RepoDO extends DurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS bisect (id TEXT PRIMARY KEY, lo INTEGER, hi INTEGER,
        state TEXT, culprit TEXT, tester TEXT, k INTEGER);
      CREATE TABLE IF NOT EXISTS bisect_probe (id TEXT, idx INTEGER, sha TEXT, verdict TEXT,
        PRIMARY KEY (id, idx))`);
  }

  // Walk first parents from bad back to good using the SQLite commit graph.
  private linearize(good: string, bad: string): string[] {
    const chain: string[] = [];
    for (let sha = bad; sha !== good; ) {
      chain.push(sha);
      const row = this.ctx.storage.sql.exec<{ parent1: string }>(
        "SELECT parent1 FROM commit_graph WHERE sha = ?", sha).one();
      if (!row?.parent1) throw new Error("good is not a first-parent ancestor of bad");
      sha = row.parent1;
    }
    return [good, ...chain.reverse()]; // idx 0 = known good, idx n-1 = known bad
  }

  async startBisect(good: string, bad: string, tester: string, k = 4): Promise<string> {
    const id = crypto.randomUUID();
    const chain = this.linearize(good, bad);
    const sql = this.ctx.storage.sql;
    sql.exec("INSERT INTO bisect VALUES (?, 0, ?, 'running', NULL, ?, ?)", id, chain.length - 1, tester, k);
    chain.forEach((sha, i) => sql.exec("INSERT INTO bisect_probe VALUES (?, ?, ?, ?)", id, i, sha,
      i === 0 ? "good" : i === chain.length - 1 ? "bad" : null));
    await this.ctx.storage.setAlarm(Date.now());
    return id;
  }

  async alarm(): Promise<void> {
    const sql = this.ctx.storage.sql;
    const b = sql.exec<{ id: string; lo: number; hi: number; tester: string; k: number }>(
      "SELECT * FROM bisect WHERE state = 'running' LIMIT 1").toArray()[0];
    if (!b) return;
    if (b.hi - b.lo <= 1) {
      const culprit = sql.exec<{ sha: string }>("SELECT sha FROM bisect_probe WHERE id=? AND idx=?", b.id, b.hi).one().sha;
      sql.exec("UPDATE bisect SET state='done', culprit=? WHERE id=?", culprit, b.id);
      return;
    }
    // k evenly spaced untested midpoints: parallel bisect narrows by (k+1)x per round.
    const span = b.hi - b.lo, n = Math.min(b.k, span - 1);
    const picks = Array.from({ length: n }, (_, i) => b.lo + Math.round(((i + 1) * span) / (n + 1)))
      .filter((idx, i, a) => a.indexOf(idx) === i && idx > b.lo && idx < b.hi);
    const results = await Promise.allSettled(picks.map(async (idx) => {
      const { sha } = sql.exec<{ sha: string }>("SELECT sha FROM bisect_probe WHERE id=? AND idx=?", b.id, idx).one();
      const res = await this.env.TESTER.fetch("https://tester/run", {  // service binding; or env.DISPATCH.get(b.tester)
        method: "POST",
        body: JSON.stringify({ repo: this.ctx.id.toString(), commit: sha, token: await this.readToken(sha) }),
      });
      const v = ((await res.json()) as { verdict: Verdict }).verdict;
      sql.exec("UPDATE bisect_probe SET verdict=? WHERE id=? AND idx=?", v, b.id, idx);
      return { idx, v };
    }));
    // Shrink: new lo = highest 'good' index, new hi = lowest 'bad' index. Skips fall through like git bisect skip.
    let lo = b.lo, hi = b.hi;
    for (const r of results) if (r.status === "fulfilled") {
      if (r.value.v === "good" && r.value.idx > lo) lo = r.value.idx;
      if (r.value.v === "bad" && r.value.idx < hi) hi = r.value.idx;
    }
    const untested = sql.exec("SELECT count(*) AS c FROM bisect_probe WHERE id=? AND idx>? AND idx<? AND verdict IS NULL", b.id, lo, hi).one().c as number;
    if (hi - lo > 1 && untested === 0) { sql.exec("UPDATE bisect SET state='ambiguous', lo=?, hi=? WHERE id=?", lo, hi, b.id); return; }
    sql.exec("UPDATE bisect SET lo=?, hi=? WHERE id=?", lo, hi, b.id);
    await this.ctx.storage.setAlarm(Date.now());  // next round in a fresh invocation
  }
}

// test Worker (user-owned). Reads the commit's tree straight from R2, never clones.
export default {
  async fetch(req: Request, env: { BUCKET: R2Bucket }) {
    const { commit } = await req.json<{ commit: string }>();
    const readObj = async (sha: string) => {
      const obj = await env.BUCKET.get(`objects/${sha}`);                 // content-addressed key
      const raw = new Uint8Array(await new Response(obj!.body.pipeThrough(new DecompressionStream("deflate"))).arrayBuffer());
      const nul = raw.indexOf(0);                                           // "<type> <size>\0"
      return { type: new TextDecoder().decode(raw.subarray(0, nul)).split(" ")[0], body: raw.subarray(nul + 1) };
    };
    const c = new TextDecoder().decode((await readObj(commit)).body);
    const treeSha = /^tree ([0-9a-f]{40})/m.exec(c)![1];
    const tree = (await readObj(treeSha)).body;  // entries: "<mode> <name>\0<20-byte sha>" — pseudo: parse, find "package.json"
    const pkgSha = findEntry(tree, "package.json");
    const pkg = JSON.parse(new TextDecoder().decode((await readObj(pkgSha)).body));
    const verdict: Verdict = pkg.version ? "good" : "bad";   // stand-in for a real in-Worker test (JS/Wasm)
    return Response.json({ verdict });
  },
};
```

## Why it works
- git bisect is a search over a linear order, not over the wire protocol; the DO already holds that order in its `commit_graph` table (populated at push time from the `commit` objects' `parent` lines), so no pack parsing happens during bisect.
- Classic bisect tests 1 midpoint per step (log2 N). Testing k points per round narrows the interval by a factor of k+1, so 1000 commits take 4 rounds at k=6 instead of 10; each round is a fresh alarm invocation with its own CPU budget, and each tester call is a separate Worker with its own limits.
- The state machine (`lo`, `hi`, per-probe verdicts) lives in SQLite, so a crashed round is simply re-run by the alarm; probes already answered are not re-tested, and `skip` maps to git's `bisect skip` semantics (interval stops shrinking past it; if only skips remain the result is `ambiguous`, exactly what git reports).
- The tester needs no checkout: git objects are content-addressed, zlib-compressed `<type> <size>\0<body>` blobs, so a Worker can inflate a commit, follow its `tree` line, walk tree entries (`<mode> <name>\0<20-byte sha>`) and read only the blobs it needs, one R2 GET each.
- The result is a commit SHA, so the client can run `git bisect reset && git checkout <culprit>` or feed it to `git bisect replay` with a log the DO can emit in git's own `git bisect good/bad <sha>` log format.

## Known limits
- The "test" must run inside a Worker: pure JS/TS or Wasm, no shell, no `npm test`, no native toolchains. Anything that needs a real process is out of scope for a serverless-only design; this is the largest hand-wave versus what people mean by bisect.
- Parallel width per alarm invocation is bounded by the Workers limit of 6 simultaneous open connections (subrequests block beyond that), so effective k is about 6 unless fan-out goes through Queues or multiple DO invocations.
- Each tester invocation is bounded by that Worker's CPU limit (30s default, up to 5 min via `limits.cpu_ms`); slow tests must be sharded by the user.
- Linearization walks first parents only, so a bug introduced on a side branch is attributed to the merge commit that brought it in; full-DAG bisect (git's reachability-count halving) needs the graph queries from want-have-negotiation and more code.
- Tree materialization is one R2 GET per object touched, so a test reading many files pays request costs; objects that live inside packs rather than loose need pack-index range reads (precomputed-clone-pack) instead of `get(objects/<sha>)`.
- The repo DO is single-threaded: bisect rounds share the DO with pushes and fetches; heavy bisect load should move the orchestration to a per-bisect child DO.
- DO memory (128MB) is irrelevant here since the DO never loads objects; the test Worker inflates objects into memory, so giant blobs must be streamed rather than `arrayBuffer()`'d as in the proof.

## Depends on
- repo-do-ref-authority
- content-addressed-r2-keys
- want-have-negotiation
- hooks-as-workers
- zero-clone-vfs
- scoped-token-remotes
