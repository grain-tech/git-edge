> Idea #17 · edge · verdict: **risky** · feasibility 4/5 · reliability 3/5 · correctness 2/5 · effort: weeks
> Proof: [proofs/server-side-merge.md](./server-side-merge.md) · Review: [reviews/server-side-merge.md](../../reviews/archived/server-side-merge.md)

# Server-side three-way merge in the Worker

## Mechanism
`git push -o merge=main origin feature` lands on `POST /:owner/:repo/git-receive-pack`; because `info/refs` advertised `push-options`, the pkt-line stream is `<old> <new> refs/heads/feature\0report-status push-options side-band-64k`, flush, one pkt-line per option (`merge=main`), flush, then the `PACK`. The Worker runs the normal two-phase push (objects to R2 at `objects/<sha>`, manifest under `pending/`), then, still in the Worker and before touching any ref, reads the current `refs/heads/main` tip from the repo DO, asks the DO for the merge base (walking the `commits(sha, parents)` graph in DO SQLite plus the parents it just parsed out of the pack), and performs a three-way tree merge by reading the base/ours/theirs tree and blob objects from R2 with `DecompressionStream("deflate")`. Trivially resolvable entries are decided at tree level without reading blobs; both-sides-changed text blobs go through a line-based diff3; a real conflict aborts with `ng refs/heads/feature merge conflict: <paths>` and no ref moves. Otherwise the Worker writes the new tree objects and a two-parent merge commit to R2 and hands the DO two ref commands in one atomic `commit()`: `feature: old→new` (as pushed) and `main: observedTip→mergeCommit` (a compare-and-swap); if main moved under it the Worker re-reads the tip and retries the merge a bounded number of times.

## Primitives
- Workers: streaming request body, `DecompressionStream("deflate")` / `CompressionStream("deflate")` for zlib loose objects, `crypto.subtle.digest("SHA-1")` — GA
- Durable Objects with SQLite storage (`ctx.storage.sql.exec`, `transactionSync`) for refs and the commit graph; DO RPC stub methods — GA
- DO alarms only indirectly (the two-phase-push janitor sweeps merge objects written by a Worker that lost its CAS retries) — GA
- R2 `get`/`put` of loose objects, content-addressed keys, idempotent — GA
- A pure-JS diff3 (`node-diff3`, `diff3Merge`) bundled into the Worker; no Wasm needed for this proof — not a Cloudflare primitive, no GA question
- Nothing beta is required.

## Proof code
```typescript
import { diff3Merge } from "node-diff3";                           // pure JS, bundles into the Worker
type Env = { REPO: DurableObjectNamespace<RepoDO>; BUCKET: R2Bucket };
type Entry = { mode: string; sha: string };                        // git tree entry, mode "100644" | "100755" | "40000" | "120000"
type Tree = Map<string, Entry>;
const ZERO = "0".repeat(40);

// receive-pack body after two-phase push has already streamed the PACK into R2 (see two-phase-push)
async function receivePackWithMerge(req: Request, env: Env, stub: DurableObjectStub<RepoDO>, pushId: string) {
  const { cmds, options, parsed } = await parsePushAndPack(req, env, pushId); // options: ["merge=main"], parsed: sha -> commit parents from pack
  const target = options.find(o => o.startsWith("merge="))?.slice(6);
  if (!target) return stub.commit(pushId);                          // plain push path
  const cmd = cmds[0];                                              // the branch being pushed, e.g. refs/heads/feature
  for (let attempt = 0; attempt < 3; attempt++) {                   // CAS loop: main may move while we merge
    const ours = (await stub.getRef(`refs/heads/${target}`)) ?? ZERO;
    const base = await stub.mergeBase(ours, cmd.new, parsed);       // walks commits(sha,parents) in DO SQLite + pushed parents
    let mergeSha: string;
    if (base === cmd.new) mergeSha = ours;                          // already merged: no-op on main
    else if (base === ours || ours === ZERO) mergeSha = cmd.new;    // fast-forward: no merge commit needed
    else {
      const tree = await mergeTrees(env, await treeOf(env, base), await treeOf(env, ours), await treeOf(env, cmd.new), "");
      if (tree.conflicts.length) return report([{ ref: cmd.ref, ok: false, reason: `merge conflict: ${tree.conflicts.join(",")}` }]);
      const ident = `${req.headers.get("x-git-user") ?? "git-edge <git@edge>"} ${Math.floor(Date.now() / 1000)} +0000`;
      mergeSha = await writeObject(env, "commit", new TextEncoder().encode(
        `tree ${tree.sha}\nparent ${ours}\nparent ${cmd.new}\nauthor ${ident}\ncommitter ${ident}\n\nMerge ${cmd.ref} into ${target}\n`));
    }
    const results = await stub.commit(pushId, [cmd, { ref: `refs/heads/${target}`, old: ours, new: mergeSha }]); // atomic, both or neither
    if (!results.some(r => r.reason === "fetch first" && r.ref.endsWith(target))) return report(results);
  }                                                                // lost the race 3 times: orphan merge objects are swept by the janitor alarm
  return report([{ ref: cmd.ref, ok: false, reason: "merge target moved; retry" }]);
}

// three-way tree merge; trivial cases decided by sha alone, blobs only inflated when both sides changed the same path
async function mergeTrees(env: Env, b: Tree, o: Tree, t: Tree, prefix: string): Promise<{ sha: string; conflicts: string[] }> {
  const out: Tree = new Map(); const conflicts: string[] = [];
  for (const name of new Set([...b.keys(), ...o.keys(), ...t.keys()])) {
    const B = b.get(name), O = o.get(name), T = t.get(name); const path = prefix + name;
    const same = (x?: Entry, y?: Entry) => x?.sha === y?.sha && x?.mode === y?.mode;
    if (same(O, T)) { if (O) out.set(name, O); continue; }         // both sides agree (incl. both deleted)
    if (same(O, B)) { if (T) out.set(name, T); continue; }         // only theirs changed
    if (same(T, B)) { if (O) out.set(name, O); continue; }         // only ours changed
    if (O?.mode === "40000" && T?.mode === "40000") {              // both modified a directory: recurse
      const sub = await mergeTrees(env, B?.mode === "40000" ? await readTree(env, B.sha) : new Map(),
        await readTree(env, O.sha), await readTree(env, T.sha), path + "/");
      conflicts.push(...sub.conflicts); out.set(name, { mode: "40000", sha: sub.sha }); continue;
    }
    if (O && T && O.mode !== "40000" && T.mode !== "40000" && O.mode === T.mode) {        // both edited the same file
      const [bb, ob, tb] = await Promise.all([B ? readBlob(env, B.sha) : "", readBlob(env, O.sha), readBlob(env, T.sha)]);
      if ([bb, ob, tb].some(s => s.includes("\0"))) { conflicts.push(path); continue; }   // binary: git also refuses
      const regions = diff3Merge(ob.split("\n"), bb.split("\n"), tb.split("\n"));
      if (regions.some(r => "conflict" in r)) { conflicts.push(path); continue; }
      const merged = regions.flatMap(r => (r as { ok: string[] }).ok).join("\n");
      out.set(name, { mode: O.mode, sha: await writeObject(env, "blob", new TextEncoder().encode(merged)) }); continue;
    }
    conflicts.push(path);                                          // modify/delete, file/dir, mode change vs edit, add/add different
  }
  return { sha: conflicts.length ? ZERO : await writeTree(env, out), conflicts };
}

// ---- loose object I/O against R2 (zlib "<type> <size>\0<body>", key objects/<sha>) ----
async function readObject(env: Env, sha: string): Promise<{ type: string; body: Uint8Array }> {
  const obj = await env.BUCKET.get(`objects/${sha}`); if (!obj) throw new Error(`missing object ${sha}`);
  const raw = new Uint8Array(await new Response(obj.body.pipeThrough(new DecompressionStream("deflate"))).arrayBuffer());
  const nul = raw.indexOf(0); const [type] = new TextDecoder().decode(raw.subarray(0, nul)).split(" ");
  return { type, body: raw.subarray(nul + 1) };
}
async function writeObject(env: Env, type: string, body: Uint8Array): Promise<string> {
  const hdr = new TextEncoder().encode(`${type} ${body.length}\0`);
  const full = new Uint8Array(hdr.length + body.length); full.set(hdr); full.set(body, hdr.length);
  const sha = [...new Uint8Array(await crypto.subtle.digest("SHA-1", full))].map(b => b.toString(16).padStart(2, "0")).join("");
  const z = await new Response(new Blob([full]).stream().pipeThrough(new CompressionStream("deflate"))).arrayBuffer();
  await env.BUCKET.put(`objects/${sha}`, z);                       // content-addressed: safe to write before the CAS
  return sha;
}
async function readTree(env: Env, sha: string): Promise<Tree> {    // entries: "<mode> <name>\0" + 20 raw sha bytes
  const { body } = await readObject(env, sha); const t: Tree = new Map(); let i = 0;
  while (i < body.length) {
    const sp = body.indexOf(0x20, i), nul = body.indexOf(0, sp);
    const mode = new TextDecoder().decode(body.subarray(i, sp)), name = new TextDecoder().decode(body.subarray(sp + 1, nul));
    t.set(name, { mode, sha: [...body.subarray(nul + 1, nul + 21)].map(b => b.toString(16).padStart(2, "0")).join("") }); i = nul + 21;
  }
  return t;
}
async function writeTree(env: Env, t: Tree): Promise<string> {
  const sortKey = (n: string, e: Entry) => e.mode === "40000" ? n + "/" : n;   // git sorts dirs as if suffixed with "/"
  const parts = [...t].sort(([a, ea], [b, eb]) => sortKey(a, ea) < sortKey(b, eb) ? -1 : 1).flatMap(([name, e]) =>
    [new TextEncoder().encode(`${e.mode} ${name}\0`), Uint8Array.from(e.sha.match(/../g)!.map(h => parseInt(h, 16)))]);
  return writeObject(env, "tree", concat(parts));
}
const readBlob = async (env: Env, sha: string) => new TextDecoder().decode((await readObject(env, sha)).body);
const treeOf = async (env: Env, commit: string) => readTree(env, /^tree ([0-9a-f]{40})/.exec(new TextDecoder().decode((await readObject(env, commit)).body))![1]);

// RepoDO additions (refs/objects/pending tables and commit() are from two-phase-push; commits(sha,parents) from want-have-negotiation)
export class RepoDO extends DurableObject<Env> {
  async getRef(name: string) { return this.ctx.storage.sql.exec<{ sha: string }>("SELECT sha FROM refs WHERE name = ?", name).toArray()[0]?.sha; }
  async mergeBase(a: string, b: string, extra: Record<string, string[]>): Promise<string | null> {
    const parents = (s: string) => extra[s] ?? (this.ctx.storage.sql.exec<{ parents: string }>("SELECT parents FROM commits WHERE sha = ?", s).toArray()[0]?.parents.split(" ").filter(Boolean) ?? []);
    const seenA = new Set<string>(), seenB = new Set<string>(); let qa = [a], qb = [b];   // lockstep BFS; first commit seen from both sides
    while (qa.length || qb.length) {
      for (const [q, mine, theirs] of [[qa, seenA, seenB], [qb, seenB, seenA]] as const) {
        const s = q.shift(); if (!s || mine.has(s)) continue; mine.add(s);
        if (theirs.has(s)) return s;
        q.push(...parents(s));
      }
    }
    return null;                                                    // unrelated histories: caller treats as conflict
  }
}
```

## Why it works
- `push-options` is a real receive-pack capability (Documentation/gitprotocol-pack: after the command list and its flush, each option is one pkt-line, then a flush, then the pack), so `git push -o merge=main` reaches the Worker with no client patch; servers that do not advertise it never see the section. The client only ever names `refs/heads/feature` in its commands, so success/failure is reported against that ref (`ok`/`ng refs/heads/feature ...`); git's `send-pack` rejects status lines for refs it did not ask about, which is why main's outcome rides on feature's line and, optionally, a band-2 sideband progress message.
- Git's merge is a function of three trees and a merge base; the base comes from the commit graph, which the repo DO already holds in SQLite for want/have negotiation, and the pushed commits' parents are known from the pack the Worker just inflated, so no commit object is read from R2 to find the base. The lockstep BFS returns a common ancestor; for a single merge base it is exactly `git merge-base`.
- The tree-level rules (same-on-both, changed-on-one-side, recurse-on-both-dirs) are the trivial-merge table from `git merge-tree`/merge-ort; they resolve the overwhelming majority of paths by sha comparison alone, so a merge touches R2 only for the trees on the changed spine and the blobs that both sides edited. A 10-file change in a 50k-file repo is a few dozen GETs.
- The Worker writes loose objects exactly as git does (`"<type> <size>\0"` + body, SHA-1 over that, zlib) and sorts tree entries with the directory-as-"name/" rule, so the resulting merge commit is byte-for-byte what `git merge --no-ff` would produce given the same author/timestamp, and a subsequent `git fetch` from any client verifies it.
- Because objects are content-addressed and written before any ref moves, the merge is speculative: main's advance is a plain compare-and-swap on the DO (`old: ours`) inside the same atomic `commit()` as the feature update, so a concurrent push to main makes the CAS fail with `fetch first`, the Worker retries against the new tip, and nothing half-merged is ever visible. A merge abandoned after three retries leaves only unreachable objects, which the two-phase-push janitor already sweeps.
- "Reject only on real conflicts" maps to: text regions diff3 cannot resolve, binary blobs edited on both sides, modify/delete, file/directory, add/add with different content, and unrelated histories (`mergeBase` null). Everything else is accepted without the client ever having to fetch main.

## Known limits
- Merge is done in the Worker, not the DO, so it runs in the Worker's 128MB isolate under the 30s CPU default: every both-sides-edited blob is fully inflated and split into lines in memory. A push that touches a multi-MB generated file on both sides can blow either limit; the fallback is to reject with `merge conflict` rather than crash, but that is a false conflict.
- No rename detection. git's default merge-ort detects renames so an edit on one side and a rename on the other merges cleanly; this proof reports modify/delete. Rename detection needs similarity scoring across all added/deleted blobs and is where the Wasm core (`wasm-git-core`) earns its place.
- Only one merge base is used. With criss-cross history git computes a virtual recursive base; the lockstep BFS just returns whichever common ancestor it meets first, so some merges git would resolve cleanly are rejected here (never the reverse in a dangerous direction: a rejection is always safe, but an acceptance could differ from git's textual result at conflict-adjacent hunks because `node-diff3` and git's xdiff choose hunk boundaries differently).
- `mergeBase` runs inside the DO and walks the commit graph with one SQLite `SELECT` per commit; on a long-diverged branch (thousands of commits) that is tens of milliseconds during which the repo takes no other request. Fine for the common case, not for a stale year-old branch on a busy monorepo.
- R2 GETs on the changed spine plus one PUT per new tree/blob/commit; deep trees mean one round trip per directory level per side (base/ours/theirs), so latency is depth-bound (roughly 3 x depth sequential GETs, though siblings are fetched in parallel).
- Author/committer identity is hand-waved as an `x-git-user` header set by the auth layer; `auth-and-multitenancy` needs to provide a real name/email, and the merge commit is unsigned, which conflicts with a signed-by-default policy (`signed-reflog`) unless the server holds a signing key.
- `commit()` is assumed to accept a list of commands and apply them atomically (both feature and main, or neither); the two-phase-push proof leaves atomic-across-refs as a one-line option, so this is a real, if small, change to its interface.
- Submodule entries (mode `160000`) and symlinks (`120000`) are only merged by sha equality; both-sides-changed falls to conflict. Mode-only changes on one side with content edits on the other are treated as conflicts rather than combined.
- The sideband message "merged into main as <sha>" is not shown in the code; without it the user sees only `ok refs/heads/feature` and must `git fetch` to learn main's new tip.

## Depends on
- two-phase-push
- streaming-pack-parser
- repo-do-ref-authority
- refs-sqlite-objects-r2
- content-addressed-r2-keys
- want-have-negotiation
- info-refs-endpoint
- auth-and-multitenancy
