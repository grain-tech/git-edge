# Review: Deduplication across all repos in one content-addressed bucket

> Idea #38 · wild · verdict: **lands with caveats** · feasibility 4/5 · reliability 3/5 · correctness 3/5 · effort: weeks
> Proof: [proofs/global-dedup.md](../../proofs/archived/global-dedup.md) · Review: [reviews/global-dedup.md](./global-dedup.md)

# Review: global-dedup (idea #38)

## Scores
- Feasibility: 4/5. Everything touched is GA: DO SQLite, R2 binding `head/put/get` + `customMetadata`, `CompressionStream("deflate")` (RFC 1950, correct for pack entries), `crypto.subtle.digest("SHA-1")`, streaming `Response`. Limits are respected only for the streaming path; `admit(type, content)` passes a whole resolved object through a DO RPC argument, so a blob past the RPC serialization cap / 128MB DO heap must go a different route (proof admits this, defers to LFS). Serial per-object `head` then `put` through one single-threaded DO caps a push at ~100-300 obj/s; a kernel-sized push is hours and ~$45 of Class A ops.
- Reliability: 3/5. Append-only v1 has no data-loss path (put precedes the index row; R2 is strongly consistent; concurrent same-key puts write identical bytes). Any reclamation path sketched (sharded refcount DOs) reintroduces the classic head-hit-vs-sweep race and is not designed. Cross-tenant existence timing oracle is real and unmitigated.
- Correctness: 3/5. Achieves byte-identical dedup, which is the literal claim. The implied benefit (less storage) is undercut by the proof's own concession: no deltas (5-10x loss on long single-repo history) and a <4KB hybrid that would leave most of lodash's files un-deduped. The shown `packResponse` returns a raw PACK, which no real git client accepts (see Interop).

## Crash walk-through
Push of 10k objects, Worker dies after 6k `admit` calls. State: up to 6k R2 objects exist (some possibly already existed), 6k rows in the repo `objects` table, refs untouched. Because `put` is awaited before `INSERT`, every row points at a durable R2 object; the worst case is one R2 object with no owning row (crash between put and insert), which is a harmless orphan under append-only. Refs never moved, so the client sees a failed push and retries; the retry hits "present" for 6k SHAs and proceeds. No loss, no split-brain. The trap only opens if a GC ever runs: the 6k orphan-by-reachability rows and the ownerless R2 object are exactly what a naive refcount would decrement wrongly.

## Concurrency walk-through
Repos A and B push the same new blob S simultaneously. Both DOs miss in their own table, both `head("o/S")` miss, both `put` identical zlib bytes under one key; R2 keeps one object, both insert a row. Correct, at the price of a wasted Class A op. Two concurrent pushes into the *same* repo: DO input gates do not block across the R2 `await`, so two `admit(S)` calls interleave between SELECT and INSERT; `INSERT OR IGNORE` makes this benign. Ref CAS is delegated to `repo-do-ref-authority`. The bad case is admit-vs-sweep once deletion exists: B's `head` hit, sweeper marks S unreferenced (only A had it, A GC'd), sweeper deletes, B inserts row, B's next clone throws "global object missing". Append-only is the only shown defence.

## Interop check
- PACK header, undeltified entry varint header (`C TTT SSSS` then `C SSSSSSS`), inflated size from the index row, zlib body copied verbatim, 20-byte trailing SHA-1 over all emitted bytes: all correct; `index-pack` only verifies content hash and trailer, so this pack is valid.
- Breaks: `packResponse` writes the pack directly as the HTTP body. Under protocol v2 the client requires a `packfile\n` section header followed by side-band-64k pkt-lines (band byte 1, max 65520 bytes/pkt, then flush); v0/v1 clients that advertised `side-band-64k` need the same framing plus a `NAK` pkt-line first. git 2.4x would fail with "fatal: expected 'packfile'" / "protocol error: bad pack header". Fixable by a ~30-line framing transform, but the proof code as written is not wire-valid.
- Delta resolution on receive needs base objects inflated from R2 (`DecompressionStream`); the proof asserts this happens in the parser but shows no thin-pack base lookup.
- SHA-256 repos need a separate `o256/` key space and a different hash; not handled.

## Blockers
- `packResponse` output lacks pkt-line/side-band framing; real clients reject it.
- No deletion story at all; the proof's own refcount sketch has an unhandled crash window and the head-hit/sweep race.

## Caveats
- Timing oracle: `head`-hit vs `head`+`put` leaks that some tenant holds bytes the attacker can guess; needs constant-work or always-put.
- Storage saving is not demonstrated; loss of deltas plausibly makes the global store larger than per-repo delta packs for anything but forks/vendored deps.
- R2 op cost and single-DO serial throughput make large pushes/clones slow and expensive without batching and `precomputed-clone-pack`.
- Whole-object `admit` over RPC bounds blob size well under 128MB in practice.
- One-bucket blast radius; no R2 versioning via binding, so tombstone renames are the only guard.

## Verdict
lands-with-caveats. The dedup mechanism itself is a small, sound layer over an existing content-addressed store and every primitive is GA; append-only mode is crash- and race-safe. But the shown fetch path is not wire-valid, reclamation is unsolved, and the headline storage win is unproven. Effort: weeks (days for the dedup layer, the rest is the pack parser, negotiation, and framing it depends on).
