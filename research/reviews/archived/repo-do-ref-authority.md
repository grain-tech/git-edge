# Review: One Durable Object per repo as the ref authority

> Idea #1 · foundation · verdict: **lands with caveats** · feasibility 4/5 · reliability 3/5 · correctness 3/5 · effort: weeks
> Proof: [proofs/repo-do-ref-authority.md](../../proofs/archived/repo-do-ref-authority.md) · Review: [reviews/repo-do-ref-authority.md](./repo-do-ref-authority.md)

# Review: repo-do-ref-authority (idea #1)

## Scores
- Feasibility 4/5. Every primitive used is GA: SQLite-backed DO storage, `transactionSync`, RPC stubs via `DurableObject`, `idFromName`, R2 `head`, alarms. Limits are respected because only SHAs/refnames cross into the DO. Two things are hand-waved into `splitCommandsAndPack`: (a) git switches to `Transfer-Encoding: chunked` above `http.postBuffer` (1 MiB), so the Worker has no Content-Length and cannot `BUCKET.put(key, req.body)` in one shot (R2 needs a known length or `FixedLengthStream`); it must go multipart with >=5 MiB buffered parts. (b) `cursor.rowsWritten` is a billing counter that also counts index-row writes; `refs(name TEXT PRIMARY KEY)` has an implicit autoindex, so INSERT/DELETE plausibly report 2, making `written === 1` return `ng` on every branch create/delete. Use `SELECT changes()` or `WITHOUT ROWID` (unverified against docs; must be tested on day 1).
- Reliability 3/5. The CAS itself is sound; the commit point is in the wrong place relative to object promotion (see crash walk-through).
- Correctness 3/5. Achieves "serialized CAS on refs" exactly as receive-pack does (git passes the *advertised* old oid to `ref_transaction_update`, so even a racing `--force` fails server-side in real git too). It does not achieve "consistent repo": no `check_connected`, so an accepted push can leave a ref pointing at a tip whose ancestors are absent; the proof admits this.
- Effort: weeks (ref CAS alone is days; making it crash-safe with #6 and passing a real `git push` is 2-4 weeks with #53).

## Crash walk-through
Push A: Worker streams pack to `pending/<id>/`, DO checks `head(pending/<id>/<tip>)`, CAS commits `main: X->Y`, output gate flushes, then the Worker isolate dies before returning report-status. Client sees a transport error and retries; retry gets `ng ... fetch first`, client fetches, sees Y, fine for the ref. But Y's objects live only under `pending/<id>/`. Nothing in the proof code records `<id>` as committed inside the transaction, and nothing promotes `pending/<id>/*` to `objects/*` (R2's Workers binding has no server-side copy; promotion is a re-upload or S3 CopyObject). The #6 janitor, keyed only on age, sweeps `pending/<id>` and `main` now dangles: data loss with a durable ref pointing at nothing. Fix: write objects directly to `objects/<sha>` (idempotent under #5) *before* the CAS, and keep `pending/` only as a manifest for orphan sweeping; or insert `(pushId, 'committed')` into a DO table in the same `transactionSync` and have the alarm promote.

## Concurrency walk-through
A: `X->Y main`, B: `X->Z main`, both arrive while the DO awaits R2 heads (input gates do not block on R2 awaits, so both `updateRefs` calls are in flight). A's `transactionSync` runs first: `UPDATE ... WHERE sha=X` writes 1 row; B's runs after and matches 0 rows -> `ng main fetch first`. No split-brain, no lost update, because the check-and-write is synchronous and single-instance. Two subtleties: a delete+recreate pair of pushes (`X->0`, then `0->W`) interleaving with a third `X->Y` is also safe (each CAS is independent). Non-atomic multi-ref pushes can partially apply, which is git's default; the atomic path (throw inside `transactionSync`) is described but not wired to the `atomic` capability. Multi-instance DO during colo failover is not a concern: SQLite is the only state.

## Interop check
`git push` (2.4x, protocol.version=2) still uses v0 for receive-pack, so `report-status` is the right format. What breaks with a real client:
1. If `side-band-64k` is advertised in info/refs (git requests it whenever offered), the entire `unpack ok`/`ok`/`ng` block must be framed in band 1; the proof emits raw pkt-lines. Either do not advertise sideband or frame it. Raw output under sideband yields `fatal: protocol error: bad band #...`.
2. `delete-refs` must be advertised or the client refuses to send deletes ("remote does not support deleting refs"), making the DELETE branch dead code.
3. Empty push bodies: deletion-only pushes carry no PACK; a new branch at an existing commit carries a 0-object pack (12-byte header + 20-byte trailer). `splitCommandsAndPack` must accept both.
4. Empty repo advertisement (`<40 zeros> capabilities^{}\0<caps>`) belongs to #53 but this idea's `listRefs()` returning `[]` must map to it or the first push fails.
5. Reason string "fetch first" is a client-side message in real git (server says "failed to update ref"); cosmetic, does not break interop.

## Blockers
- Commit ordering: ref CAS commits before objects are durable under their final `objects/<sha>` key, and no committed-pushId record exists; janitor can orphan a live ref.
- `rowsWritten` likely miscounts on indexed table -> creates/deletes rejected. Needs `SELECT changes()` and a test.
- Chunked push bodies vs R2 known-length put (hidden in pseudo-code; real for any push > 1 MiB).

## Caveats
- No connectivity check: accepted pushes can be unfetchable; requires #4 + #56 before calling the repo consistent.
- All pushes and `ls-refs` for a repo serialize through one DO in one colo; cross-region RPC adds ~100-300 ms per push.
- `idFromName("owner/repo")` makes repo rename a data migration.
- Atomic push (`--atomic`) and `report-status-v2`/push-options are unimplemented; must not be advertised.

## Verdict
lands-with-caveats. The core claim (one DO + synchronous SQLite CAS replaces distributed locks and matches receive-pack's old-oid check) is correct and buildable today; the proof code as written would reject branch creates and can orphan a committed ref after a Worker crash, so it needs the ordering fix and the changes() fix before it is the foundation the other ideas assume.
