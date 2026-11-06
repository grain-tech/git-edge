# Review: Merkle inclusion proofs on fetch

> Idea #39 · wild · verdict: **lands with caveats** · feasibility 4/5 · reliability 5/5 · correctness 3/5 · effort: weeks
> Proof: [proofs/merkle-proofs.md](../../proofs/archived/merkle-proofs.md) · Review: [reviews/merkle-proofs.md](./merkle-proofs.md)

# Review: merkle-proofs (idea #39)

## Scores
- Feasibility: 4/5. Every primitive is GA (DO SQLite, R2 get, `DecompressionStream("deflate")`, `crypto.subtle.digest("SHA-1")`, streaming `Response`). The pack-file path (SQLite pack index + R2 range read + delta resolution) is explicitly hand-waved and is the real work. `readObject` buffers whole objects via `arrayBuffer()`, so a blob over ~100 MB, or a few concurrent proofs on 30 MB blobs, blows the 128 MB DO heap; the "fine to stream" claim is not what the code does. `pkt()` and `sha1()` spread `Uint8Array`s into argument lists (`[...hdr, ...b]`), which is O(n) allocations and will hit engine argument/stack limits on multi-MB objects.
- Reliability: 5/5. Read-only over immutable content-addressed keys; the only write is an idempotent `INSERT OR REPLACE` memo row that nothing ever reads. Nothing to lose or split-brain.
- Correctness: 3/5. The cryptography is right: re-hash each object, check the parent names the child, root at a trusted commit. That is a genuine Merkle inclusion proof, and the doc is honest that it only reduces trust to "the commit OID is right". But the headline goal ("verify a partial clone without trusting the server") is already delivered by git's content addressing plus `fsck --connectivity-only`; this is a convenience RPC for clients holding only an OID, not new assurance. The code also has real bugs (see Interop).
- Effort: weeks. Days for the loose-object demo; weeks once packs, sideband chunking, and a client library exist.

## Crash walk-through
DO evicted after writing the commit and the first tree pkt-line. The async IIFE dies with the isolate; the client sees a truncated body with no flush-pkt and `verifyProof` fails at "blob not linked" or on a short object list. No R2 write happened; the memo row was never inserted (it is written only after the last object). Retry is safe and produces an identical stream. If a concurrent gc (`gc-and-repack-alarm`) deletes a loose object between segments, `readObject` throws, `w.abort(e)` fires, and the client sees a stream error instead of a forged object. Outcome: liveness failure only; no data or trust loss.

## Concurrency walk-through
Two pushers move `refs/heads/main` while a proof is streaming. Irrelevant: the proof is rooted at a client-supplied commit OID, not a ref, and objects are immutable under content-addressed keys. Two proofs for the same `(commit,path)` race on the memo row: both `INSERT OR REPLACE` the same value, last write wins, no divergence. Concurrent proofs do interleave at `await` points inside the single DO, so N simultaneous large-blob proofs share one 128 MB heap. Backpressure is absent: `TransformStream` with default queuing means a slow client holds objects in DO memory.

## Interop check
- Real git (2.4x, v2) is unaffected: it ignores an unknown advertised command, and `ls-refs`/`fetch` are untouched. So "interoperate" here means "does not break git", which holds. No git client will ever send `command=proof`; the consumer is a custom client.
- Wire bug that breaks the custom client: pkt-line length is 4 hex digits, max payload 65516 bytes (git caps at 65520 total). `pkt(obj.body)` emits a single pkt-line per object, so any tree or blob over 64 KB produces a length field that wraps or overflows (`padStart` does not truncate, so a 5-digit hex length corrupts framing). The doc even says a 10k-entry tree is ~300 KB per level. Needs sideband-64k style chunking or a length-prefixed multi-pkt object encoding.
- The server labels the final object `blob` unconditionally; a path naming a directory, a symlink (fine, still blob) or a submodule gitlink (mode 160000, object absent from this repo) yields either a false hash mismatch on the client or a `missing <oid>` abort.
- `verifyProof` assumes exactly `depth+2` objects; a mid-stream `ERR` pkt-line is not parsed as an error, so the verifier misaligns rather than reporting the server's message.
- `path_memo.mode` is hard-coded `100644` and the table is never queried; dead code.

## Blockers
- Object-per-pkt-line framing fails for any object over 64 KB; must be fixed before a single realistic tree proof works.
- Pack-file support (index, range reads, delta resolution) is required for any repo that has ever been repacked and is not designed here.

## Caveats
- Whole-object buffering in the DO bounds proof size by the 128 MB heap; streaming SHA-1 is needed for large blobs.
- Value proposition is thin: a `blob:none` clone plus lazy fetch already yields hash-verified objects; the useful new piece is the ref-tip trust that lives in `signed-reflog`, not here.
- SHA-256 repos need a 32-byte `parseTree` and a different digest; d sequential R2 round trips per proof.

## Verdict
lands-with-caveats. Cryptographically sound and operationally safe (read-only), but the shown code cannot ship: pkt-line framing breaks on every object over 64 KB, it buffers whole objects in a 128 MB isolate, and the pack path is deferred. What lands is a modest convenience RPC, not a new trust boundary.
