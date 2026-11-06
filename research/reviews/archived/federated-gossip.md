# Review: Federated remotes via DO-to-DO gossip

> Idea #36 · wild · verdict: **risky** · feasibility 3/5 · reliability 2/5 · correctness 3/5 · effort: weeks
> Proof: [proofs/federated-gossip.md](../../proofs/archived/federated-gossip.md) · Review: [reviews/federated-gossip.md](./federated-gossip.md)

# Review: federated-gossip (idea #36)

## Scores
- Feasibility: 3/5. Every primitive is GA (DO SQLite + `transactionSync`, alarms, DO RPC, R2 head/put, `DecompressionStream`, Workers `fetch`). But the proof as written does not run: `this.ctx.id.name` is `undefined` inside a Durable Object (the name is only known on the stub that called `idFromName`), so every message carries `origin: undefined`, and `(undefined, seq)` dedupe collapses all origins into one sequence, silently dropping most updates. Trivial fix (persist the name on first request) but it is the identity the whole design hangs on. Second structural issue: the foreign-peer pack fetch runs *inside* `gossip()`, i.e. inside the sender's RPC call, so the sender's alarm blocks for the full pack transfer and one slow peer stalls the whole outbox drain; must ack-then-process.
- Reliability: 2/5. See walk-throughs: tracking refs can regress via out-of-order delivery and anti-entropy only repairs direct peers.
- Correctness: 3/5. It builds a hub-less mesh of remote-tracking refs, not a mesh of mirrors: a plain `git clone <mirror>` never sees `refs/remotes/<origin>/*`.
- Effort: weeks (2-4 on top of the seven dependencies, which include the streaming pack parser and two-phase push).

## Crash walk-through
Origin A commits `main` old->new, outbox rows for B and C written in the same `transactionSync` (good), `setAlarm(now)` fired-and-forgotten. Alarm runs, RPC to B returns, DO is evicted before `DELETE FROM outbox`. On retry the same `(A, seq)` reaches B, hits the `seen` row, returns: harmless. Variant: B's `gossip()` passes the `seen` check, awaits `BUCKET.head`, and a second redelivery interleaves at that `await` (DO input gates release on I/O). Both proceed; the second `INSERT INTO seen` violates the PK inside `transactionSync`, throws back to A, A bumps `attempts`, retries, then sees the row. No loss, but every crash/retry generates an error-path round trip. Foreign-peer variant: B crashes halfway through `indexPackIntoR2`; partial `objects/<sha>` PUTs are content-addressed and idempotent, so only wasted R2 writes. Nothing is orphaned permanently; first-ever push with no alarm history and a crash between commit and `setAlarm` waits for nothing until the next push arms one (no alarm loop exists yet).

## Concurrency walk-through
Mesh A-B, A-C, B-C. A pushes twice quickly: seq 4 (`main`=X) and seq 5 (`main`=Y). C receives seq 5 directly from A first, writes `refs/remotes/A/main`=Y. Then seq 4 arrives via B (path [A,B]) with a fresh `(A,4)` not in `seen`; `gossip()` does `INSERT OR REPLACE` unconditionally, so C's tracking ref regresses to X. Nothing compares `m.old` to the current value or enforces per-(origin,ref) seq monotonicity. Because A is C's direct peer, anti-entropy repairs it within ~5 min; if A were only reachable transitively (C peers with B alone) it stays regressed until A's next push, because `antiEntropy` compares the peer's `refs/heads/*` only, never its `refs/remotes/*`. Same-repo concurrent pushes are fine (DO serialises `commit()`, CAS on `old`). Cross-deployment: two deployments both hosting `acme/lib` produce colliding `origin` strings and `(origin, seq)` dedupe drops one side's updates. Not split-brain on `refs/heads` (never touched by gossip), but tracking refs can be stale or wrong.

## Interop check
Real git clients never speak to the gossip layer; they talk to a mirror over normal smart-HTTP, so nothing breaks at the wire. What silently fails is the goal: `git clone https://mirror/x/y` sends `ls-refs` with `ref-prefix refs/heads/` and `refs/tags/`; the federated tips live under `refs/remotes/acme/lib/main` and are neither listed nor fetched. Users need an explicit refspec (`git fetch mirror 'refs/remotes/acme/lib/*:refs/remotes/acme-lib/*'`), and the mirror's `upload-pack` must treat `refs/remotes/*` as reachability roots or `want` of those tips gets `not our ref` in v0 (v2 allows any want, so v2-only saves it). The foreign fetch body (`command=fetch`, `0001`, `want`, `have`, `done`, `0000`) is well-formed v2; the response has `packfile` section + sideband demux, plus possible `wanted-refs`/`shallow-info` sections, which the proof only hand-waves. A `have` the origin doesn't hold just yields a bigger pack. Ref name `refs/remotes/acme/lib/main` is valid. Tags are never gossiped (`refs/heads/` only in `lsRefs`, no tag handling in `commit`).

## Blockers
- `ctx.id.name` is undefined inside the DO; origin identity must be stored explicitly.
- No monotonicity/CAS on tracking-ref writes; out-of-order flood delivery regresses tips.
- Anti-entropy only covers direct peers' `refs/heads`, so transitive drops never heal (contradicts "cannot leave a mirror permanently stale").
- Foreign pack ingest inside the RPC handler couples the sender's alarm to the receiver's transfer time and to the 128 MB/30 s CPU budget.

## Caveats
- Federated tips are invisible to a default clone; "mirror" is really "remote-tracking namespace".
- Origin names collide across deployments; needs a globally unique origin id (deployment URL + repo).
- `seen` grows unbounded and synthetic negative seqs are never deduped; needs a janitor.
- Peer subscription has no auth (proof acknowledges).
- Assumes R2 holds loose `objects/<sha>` so `head` proves presence; if siblings store packs, presence check needs the pack index.

## Verdict
risky. The mechanics (atomic outbox + at-least-once alarm + dedupe) are sound and buildable on GA primitives, but the proof code has one runtime-fatal bug, a ref-regression race, and an anti-entropy that does not cover the transitive case, and what it delivers is tracking refs rather than mirrors.
