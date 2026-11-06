# Review: Semantic diffs via tree-sitter in Wasm

> Idea #33 · wild · verdict: **risky** · feasibility 3/5 · reliability 3/5 · correctness 2/5 · effort: weeks
> Proof: [proofs/semantic-diffs.md](../../proofs/archived/semantic-diffs.md) · Review: [reviews/semantic-diffs.md](./semantic-diffs.md)

# Review: semantic-diffs (idea #33, wild)

## Scores
- Feasibility: 3/5. Every Cloudflare primitive named is GA (DO SQLite `sql.exec`, `CompiledWasm` module import + `new WebAssembly.Instance`, R2 `get`, `DecompressionStream("deflate")` = zlib, `limits.cpu_ms`, alarms). The unknown is the build, not the platform: tree-sitter core and the Ruby scanner need libc (`malloc`, `memcpy`, `iswalpha`/`iswspace`, `clock`, `abort`), so `clang -nostdlib` as written does not link; it needs wasi-sdk plus stubbed `wasi_snapshot_preview1` imports, and the code's `emscripten_notify_memory_growth` import contradicts "no Emscripten". Doable, unverified. Memory/CPU envelopes (128 MB DO, 30 s default CPU) are respected only because of the 1 MB/blob cap.
- Reliability: 3/5. It is a pure content-addressed cache, so no data loss and no ref involvement. But the cache key omits grammar/shim version and `defs()` logic, so every bug fix leaves stale JSON forever; an unsupported extension is parsed as JS and cached forever; growth is unbounded against the 10 GB DO SQLite ceiling with pruning hand-waved.
- Correctness: 2/5. Rename detection is dead code: `hash = fnv1a(src.subarray(n.startByte, n.endByte))` hashes the whole definition node, which includes the name token, so a renamed function never matches. Changes outside any definition (imports, requires, top-level statements, constants) are silently dropped. Container nodes (`class`, `module`, `class_declaration`) are both hashed and reported, so one edited method yields "Class modified" with a hunk over the whole class plus "Class#method modified". Duplicate qualified names (Ruby reopened classes, conditional `def`s, JS `const a=1,b=2`) collapse in the `Map`, last wins.
- Effort: weeks.

## Crash walk-through
Request for uncached pair; DO crashes after both R2 reads, mid-parse. Nothing was written; the client retries and recomputes. Crash after `INSERT OR IGNORE` but before the response reaches the client: the row may or may not have passed the output gate; either way the retry produces identical bytes (oids are content hashes). Crash during the optional post-push pre-warm alarm: alarm retries, each pair is idempotent. No orphaned R2 objects (this path never writes R2), no ref touched. Only residual: a `missing object` throw if a push crashed between R2 upload and ref update and someone asks for the half-pushed oid; that is the push path's problem, not this one's.

## Concurrency walk-through
Two PR-page loads race on the same miss: the DO interleaves at the `await Promise.all` point, both parse, both `INSERT OR IGNORE`; one row survives, both responses are byte-identical. Fine. Diff request concurrent with a push that rewrites the same path: oids differ, so different cache keys, no interference. Diff concurrent with the GC alarm: GC deletes the row for an unreachable pair while a late request reads it — request recomputes from R2, or 500s if R2 GC already removed the blob; retryable, no corruption. Real hazard is the DO being single-threaded for CPU: a 1 MB Ruby parse pair blocks every push and fetch on that repo for hundreds of ms; the proof's own mitigation (parse in the stateless Worker, DO only for cache) is the right design and should be the design.

## Interop check
No git wire protocol involved; `git` 2.4x never sees this endpoint, so protocol v2 interop is N/A. The one wire-format detail that matters is object storage: `blob()` assumes a zlib loose object (`"blob <len>\0" + body`). A real push delivers a packfile; if `content-addressed-r2-keys` stores pack entries rather than re-encoded loose objects, the pack entry header is a type/size varint and the entry is often an OFS_DELTA/REF_DELTA that must be resolved against a base. `blob()` would fail the `"blob"` check on every deltified blob. The proof defers this to `diff-api-range-reads` and `wasm-git-core` but the code does not call either. Also `"deflate"` in `DecompressionStream` is zlib (RFC 1950), which is correct for loose objects and for pack entry payloads alike.

## Blockers
1. Rename detection cannot fire: hash includes the name node. Hash the `body` field (or the span minus the name token) instead.
2. Depends on blobs being stored as loose zlib objects; no delta resolution path exists in this proof.
3. Unverified static Wasm build of tree-sitter + Ruby scanner without Emscripten; the `-nostdlib` recipe as written will not link.

## Caveats
- Add a schema/grammar version to the cache key and 400 unsupported extensions instead of defaulting to JS.
- Report out-of-definition hunks as a `file-level` entry so the diff is complete; skip hunks for container nodes.
- Key defs by name plus ordinal (or start byte) to survive reopened classes and duplicate declarators.
- Move parsing to the stateless Worker; keep the DO for cache only. Deep minified JS can overflow the recursive `walk`; use tree-sitter's cursor API.
- `lexical_declaration` naming and `singleton_method` (`Class.m` vs `Class#m`) need per-grammar rules, as the proof admits.

## Verdict
risky. The platform side lands; the diff algorithm as written is a lookalike (rename detection dead, incomplete coverage, double reporting), and the object-read path only works if the push path re-encodes to loose objects.
