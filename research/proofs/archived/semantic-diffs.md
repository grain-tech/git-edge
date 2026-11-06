> Idea #33 · wild · verdict: **risky** · feasibility 3/5 · reliability 3/5 · correctness 2/5 · effort: weeks
> Proof: [proofs/semantic-diffs.md](./semantic-diffs.md) · Review: [reviews/semantic-diffs.md](../../reviews/archived/semantic-diffs.md)

# Semantic diffs via tree-sitter in Wasm

## Mechanism
`GET /:owner/:repo/diff?from=<blob-oid>&to=<blob-oid>&path=foo.rb&semantic=1` is routed by the Worker to the repo DO (`idFromName(owner/repo)`). The DO first checks its SQLite table `semantic_diff(old_oid, new_oid, lang, json)` — blob pairs are immutable, so a hit is final; on a miss it does two `env.BUCKET.get("objects/<oid>")` reads, inflates each loose git object (`"blob <len>\0" + body`) with `DecompressionStream("deflate")`, and parses both bodies with a tree-sitter runtime plus the JS and Ruby grammars statically linked into one `.wasm` that is imported as an ES module (`import ts from "./ts.wasm"`, instantiated with `new WebAssembly.Instance`, which is the only way Workers allow Wasm — no runtime `WebAssembly.compile` from bytes). It walks both syntax trees to a list of `{kind, qualifiedName, byteRange, bodyHash}` for functions/methods/classes/modules, matches by qualified name (rename = same bodyHash under a different name), emits added/removed/modified/renamed entries with a Myers line diff clipped to each modified function's byte range, writes the JSON into `semantic_diff`, and streams it back.

## Primitives
- Durable Objects with SQLite storage (`ctx.storage.sql.exec`) as a content-addressed result cache — GA
- Wasm modules bundled with the Worker (`import mod from "./x.wasm"` yields a precompiled `WebAssembly.Module`; wrangler rule `{type:"CompiledWasm"}`) — GA
- R2 `get` of loose objects (whole object; no range needed because a blob body must be fully inflated to parse) — GA
- `DecompressionStream("deflate")` (zlib-wrapped, which is what git loose objects are) — GA
- `Response` with JSON body; `limits.cpu_ms` in wrangler.jsonc if a parse pair exceeds the 30 s default — GA
- Optional: DO alarm (`ctx.storage.setAlarm`) to pre-warm the cache for every changed blob pair right after a push — GA

## Proof code
```typescript
// Build step (not runtime): clang --target=wasm32 -nostdlib ... tree-sitter/lib/src/lib.c \
//   tree-sitter-javascript/src/parser.c tree-sitter-javascript/src/scanner.c \
//   tree-sitter-ruby/src/parser.c tree-sitter-ruby/src/scanner.c  -> ts.wasm
// exporting ts_parser_new/ts_parser_set_language/ts_parser_parse_string/ts_tree_root_node/...
// plus tree_sitter_javascript() and tree_sitter_ruby(). One static module: no Emscripten dylink.
import tsWasm from "./ts.wasm"; // WebAssembly.Module, compiled at deploy time
type Env = { BUCKET: R2Bucket; REPO: DurableObjectNamespace };
type Def = { kind: string; name: string; start: number; end: number; hash: string };

export default {
  async fetch(req: Request, env: Env) {
    const [, owner, repo] = new URL(req.url).pathname.split("/");
    return env.REPO.get(env.REPO.idFromName(`${owner}/${repo}`)).fetch(req);
  },
};

export class RepoDO implements DurableObject {
  private ts?: TreeSitter; // thin JS wrapper over the exported C ABI; lazily instantiated per isolate
  constructor(private ctx: DurableObjectState, private env: Env) {
    ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS semantic_diff (
      old_oid TEXT, new_oid TEXT, lang TEXT, json TEXT NOT NULL, PRIMARY KEY (old_oid, new_oid, lang))`);
  }

  async fetch(req: Request): Promise<Response> {
    const u = new URL(req.url);
    const from = u.searchParams.get("from")!, to = u.searchParams.get("to")!;
    const lang = /\.rb$/.test(u.searchParams.get("path") ?? "") ? "ruby" : "javascript";
    const hit = this.ctx.storage.sql
      .exec<{ json: string }>("SELECT json FROM semantic_diff WHERE old_oid=? AND new_oid=? AND lang=?", from, to, lang)
      .toArray()[0];
    if (hit) return new Response(hit.json, { headers: { "content-type": "application/json" } });

    const [a, b] = await Promise.all([this.blob(from), this.blob(to)]);
    this.ts ??= new TreeSitter(new WebAssembly.Instance(tsWasm, { env: { emscripten_notify_memory_growth() {} } }));
    const da = this.defs(this.ts.parse(lang, a), a, lang), db = this.defs(this.ts.parse(lang, b), b, lang);

    const byName = (d: Def[]) => new Map(d.map((x) => [x.name, x]));
    const A = byName(da), B = byName(db), out: unknown[] = [];
    for (const [name, d] of B) {
      const o = A.get(name);
      if (!o) {
        const renamedFrom = da.find((x) => x.hash === d.hash && !B.has(x.name));
        out.push(renamedFrom ? { op: "renamed", from: renamedFrom.name, to: name } : { op: "added", name, kind: d.kind });
      } else if (o.hash !== d.hash) {
        out.push({ op: "modified", name, kind: d.kind, hunks: myers(a.subarray(o.start, o.end), b.subarray(d.start, d.end)) });
      }
    }
    for (const [name, o] of A) if (!B.has(name) && !db.some((x) => x.hash === o.hash)) out.push({ op: "removed", name, kind: o.kind });

    const json = JSON.stringify({ lang, changes: out });
    this.ctx.storage.sql.exec("INSERT OR IGNORE INTO semantic_diff VALUES (?,?,?,?)", from, to, lang, json);
    return new Response(json, { headers: { "content-type": "application/json" } });
  }

  // Loose object in R2: zlib(   "blob <size>\0" + body   ). Strip the header after inflating.
  private async blob(oid: string): Promise<Uint8Array> {
    const obj = await this.env.BUCKET.get(`objects/${oid.slice(0, 2)}/${oid.slice(2)}`);
    if (!obj) throw new Error(`missing object ${oid}`);
    const raw = new Uint8Array(await new Response(obj.body.pipeThrough(new DecompressionStream("deflate"))).arrayBuffer());
    const nul = raw.indexOf(0);
    if (new TextDecoder().decode(raw.subarray(0, 4)) !== "blob") throw new Error("not a blob");
    return raw.subarray(nul + 1);
  }

  // Walk the tree; collect definition nodes with a qualified name (Class#method for Ruby, Class.method for JS).
  private defs(root: Node, src: Uint8Array, lang: string): Def[] {
    const want: Record<string, string[]> = {
      javascript: ["function_declaration", "method_definition", "class_declaration", "lexical_declaration"],
      ruby: ["method", "singleton_method", "class", "module"],
    };
    const out: Def[] = [], dec = new TextDecoder();
    const walk = (n: Node, scope: string[]) => {
      if (want[lang].includes(n.type)) {
        const nameNode = n.childByFieldName("name") ?? n.namedChild(0); // lexical_declaration -> declarator.name
        const name = [...scope, dec.decode(src.subarray(nameNode.startByte, nameNode.endByte))].join(lang === "ruby" ? "#" : ".");
        const body = src.subarray(n.startByte, n.endByte);
        out.push({ kind: n.type, name, start: n.startByte, end: n.endByte, hash: fnv1a(body) });
        scope = [...scope, name.split(/[#.]/).pop()!];
      }
      for (let i = 0; i < n.namedChildCount; i++) walk(n.namedChild(i), scope);
    };
    walk(root, []);
    return out;
  }
}
```

## Why it works
- A git blob is just bytes; semantic diff needs the full body of both versions, so the DO reads two loose objects (or, with `diff-api-range-reads`, two pack entries) and inflates them with `DecompressionStream("deflate")` — git loose objects are zlib streams with the `"<type> <size>\0"` header inside the compressed payload, which the code strips.
- tree-sitter's core is plain C with no libc dependency beyond `malloc`/`memcpy`; it and the generated `parser.c` for JS and Ruby (including Ruby's external `scanner.c`) compile to `wasm32` with clang and link into one module. Workers instantiate bundled modules with `new WebAssembly.Instance(module, imports)`; the build-time module import avoids the runtime-compile restriction that breaks stock `web-tree-sitter` (its Emscripten loader wants to `WebAssembly.compile` each grammar as a dynamic side module).
- Function matching is by qualified name because that is what a reviewer means by "the diff of `User#save`"; renames are detected by equal body hash, which is exactly `git diff -M` at function rather than file granularity.
- The result is keyed by `(old_oid, new_oid, lang)`; because oids are content hashes, the SQLite cache never invalidates and a repeated request (PR page reload, agent re-query) is one SQLite row read with zero R2 traffic and zero Wasm CPU.
- The same DO already owns the commit -> tree -> blob mapping (`refs-sqlite-objects-r2`), so a commit-pair diff is a tree walk to blob pairs followed by this per-pair routine, and a post-receive alarm can precompute it for every changed `.rb`/`.js` blob before anyone asks.

## Known limits
- Workers forbid `WebAssembly.compile`/`instantiate(bytes)` at runtime, so `web-tree-sitter` as published cannot load grammars; the proof assumes a custom static-link build (tree-sitter core + parsers + a hand-written JS shim over the C ABI, or a Rust `tree-sitter` crate build via wasm-bindgen). That shim (`TreeSitter`, `Node`, `myers`, `fnv1a`) is hand-waved above; it is ~200 lines but not tree-sitter-provided. Passing a precompiled `WebAssembly.Module` into Emscripten's dylink loader may also work but was not verified.
- Wasm linear memory counts against the DO's 128 MB. tree-sitter trees are roughly 20-40x source size; two 2 MB minified JS bundles can exceed it. Cap input at ~1 MB per blob and fall back to a plain line diff above that.
- Parsing is CPU-bound: ~1-5 ms per 10 KB of JS, worse for Ruby's heredoc/regex scanner. A 1 MB pair stays well under the 30 s CPU cap, but a commit touching hundreds of files must fan out across requests or an alarm chain, not one request.
- Bundle size: tree-sitter-javascript's `parser.c` alone produces ~1.2 MB of Wasm, Ruby ~1.8 MB; with the runtime the module is ~3 MB compressed, within the 10 MB paid-plan Worker limit but nontrivial cold-start cost (tens of ms) per isolate.
- One repo DO does all parses for that repo serially; a hot monorepo PR view could queue behind pushes. Mitigation: the pure parse step has no state and can run in the plain Worker with the DO only serving the cache.
- Only JS and Ruby as stated; TS/JSX/ERB need extra grammars and the "name" extraction rules differ per grammar (Ruby `singleton_method` names live under `name`, JS `lexical_declaration` needs the `variable_declarator`). Anonymous functions, `define_method`, and metaprogrammed methods have no static name and fall out as "modified enclosing scope".
- Whole-object R2 GETs (two per uncached pair) and one SQLite write; at $0.36/M class-B ops this is negligible, but the cache table grows unbounded and needs the GC alarm to prune pairs whose blobs are unreachable.

## Depends on
refs-sqlite-objects-r2, content-addressed-r2-keys, diff-api-range-reads, wasm-git-core
