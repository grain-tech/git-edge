# Review: Snapshots via R2 object versioning of ref state

> Idea #21 · edge · verdict: **lands with caveats** · feasibility 4/5 · reliability 2/5 · correctness 3/5 · effort: days
> Proof: [proofs/r2-versioned-snapshots.md](../../proofs/archived/r2-versioned-snapshots.md) · Review: [reviews/r2-versioned-snapshots.md](./r2-versioned-snapshots.md)

# Review: r2-versioned-snapshots (idea #21)

## Scores
- Feasibility: 4/5. Every primitive is GA (DO SQLite + transactionSync, blockConcurrencyWhile, alarms, R2 put/head/list/delete, onlyIf.etagMatches, PITR bookmarks). R2 bindings are callable from a DO. Limits are fine: a 20k-ref JSON is ~1-2 MB, parse is ms, well under 128 MB / 30 s. The proof correctly admits R2 has no bucket versioning, so the title feature is emulated, not used.
- Reliability: 2/5. The proof assumes "DO is single-writer" means applyPush runs to completion unobserved. It does not: DO input gates only close during storage ops, and `await BUCKET.put/head` are outbound I/O, so a second push interleaves. That produces a silent LATEST regression (walk-through below). Also `put(..., onlyIf)` returns null on precondition failure and the code never checks it.
- Correctness: 3/5. Achieves "refs recoverable from R2 if DO storage is wiped" only for the crash class it does not need (SQLite PITR already covers that). For the class it claims to add value for (namespace deleted, class migrated) the R2 prefix is `repos/${ctx.id}`; a recreated namespace yields a different id for the same `idFromName("owner/repo")`, so `LATEST` is never found and restore silently no-ops. Must key on `ctx.id.name` (or owner/repo). HEAD symref is not in the snapshot.

## Crash walk-through
Push N: SQLite commit of refs+seq=N succeeds; DO evicted before `snapshot(N)` PUT. Client never got `ok`, retries, CAS `oldOid` now mismatches -> `ng ... `; user fetches and re-pushes as N+1, which writes snapshot N+1. Snapshot N is a permanent gap in the sequence but LATEST is correct; no data loss, no orphan (nothing was PUT). Acceptable and correctly analysed by the proof. Second sub-case: snapshot JSON PUT lands, crash before LATEST flip. Snapshot N is an orphan on R2 that the prune alarm eventually deletes; LATEST -> N-1 until push N+1. Harmless only because SQLite is authoritative and survives; if the DO storage is genuinely wiped in that window restore loads N-1 while the client saw nothing acknowledged, so still consistent with the receive-pack contract.

## Concurrency walk-through
Pushes A and B to the same repo, both non-conflicting refs. A commits seq=1, awaits `put(0001.json)`. Input gate is open during that await, so B's fetch enters, commits seq=2, awaits `put(0002.json)`. Both then `head(LATEST)` and see etag E0 (or both see no object and PUT unconditionally). Order (a): B writes LATEST=0002 first, A's conditional put fails (null, ignored) -> correct. Order (b): A writes LATEST=0001 first, B's put fails silently -> LATEST points at seq 1 while B's client received `ok`. A later wipe restores refs to seq 1 and drops an acknowledged flip: acknowledged-write loss, the exact thing the proof says cannot happen. No split-brain on the live DO (SQLite is right), but the backup is wrong and nothing detects it. Fix: serialize snapshot under an in-DO promise mutex, or drop LATEST and restore from `max(list(prefix))`; both are a few lines. Duplicate-instance CAS ("stale duplicate DO") is not a real Cloudflare failure mode; the etag guard was aimed at the wrong race.

## Interop check
Wire framing is delegated to two-phase-push, so nothing here breaks pkt-line or sideband by itself. Two details would surface to a real git 2.4x client:
1. On CAS failure the transaction throws at the first bad ref and returns a single status line for a multi-ref push. send-pack expects one `ok`/`ng` per command after `unpack ok`; missing lines print `remote failed to report status` per ref. Not fatal, but wrong. Also the failure text `ng <ref> fetch first` is the client's own phrasing; real servers send `ng <ref> failed to update ref` (harmless).
2. Post-restore, HEAD is gone: `ls-refs` with `symrefs` cannot emit `HEAD symref-target:refs/heads/main`, so `git clone` warns "remote HEAD refers to nonexistent ref, unable to checkout" and leaves an empty worktree. Snapshot must include HEAD / symrefs from `meta`.

## Blockers
- LATEST pointer regression under interleaved pushes (input gate open during R2 awaits; conditional put result ignored).
- R2 prefix keyed on `ctx.id` defeats the namespace-recreate/class-migration recovery scenario the idea exists for.

## Caveats
- HEAD and any other symrefs are not snapshotted; restore yields an uncloneable default branch.
- Non-atomic pushes are made atomic by the throw-inside-transaction; report-status is under-populated.
- Restore trusts objects still exist; GC horizon must exceed retention, as the proof notes but does not enforce.
- Prune alarm `list` caps at 1000 keys; fine at RETAIN=50 but unbounded if pushes outrun alarms.
- Adds 2-3 R2 round trips (20-50 ms) inside the per-repo serial section; lowers push throughput per repo.
- "R2 object versioning" in the title is fiction on today's R2; what ships is a per-seq key log plus pointer.

## Verdict
lands-with-caveats. The building blocks are all GA and the design is simple, but as written it can back up a stale ref state after an acknowledged push and cannot find its own backup after the one failure it is meant to survive. Both fixes are small (mutex or max-key restore; key on `ctx.id.name`; add HEAD). Effort to a working version: days.
