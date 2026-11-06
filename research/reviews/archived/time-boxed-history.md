# Review: Time-boxed history with cold-storage checkpoints

> Idea #46 · wild · verdict: **lands with caveats** · feasibility 3/5 · reliability 3/5 · correctness 3/5 · effort: weeks
> Proof: [proofs/time-boxed-history.md](../../proofs/archived/time-boxed-history.md) · Review: [reviews/time-boxed-history.md](./time-boxed-history.md)

# Review: time-boxed-history (#46, wild)

## Scores
- Feasibility 3/5. DO SQLite, alarms, R2 range get, streams, CompressionStream: GA. R2 Infrequent Access (`storageClass` on put, lifecycle transitions) is still beta-labelled, no SLA. The "15 min alarm CPU" claim is wrong: `limits.cpu_ms` caps at 300 000 ms (5 min) and alarm handlers get the same budget; a daily full pack rebuild with JS SHA-1 over a multi-GB hot set must be chunked (proof admits, does not show). Nothing else exceeds 128 MB if `buildPack` streams; `boundary()` loading the whole graph does not for <1M commits.
- Reliability 3/5. No data loss and no split-brain (objects never deleted, refs stay single-writer in the DO), but the hot path can serve a pack that cannot satisfy the client's wants (below) and a CPU-killed alarm re-arms itself every 60 s via the constructor, burning money forever.
- Correctness 3/5. By design a lookalike: the stated "squash" is impossible; what is built is a server-imposed shallow horizon. That part is in-spec, not merely "tolerated" (protocol-v2: shallow-info is sent when "the server is shallow"; git has cloned from shallow servers since 1.9). But the hot path ignores `want` and current refs, so it is only correct while no push has landed since the checkpoint.
- Effort: weeks (2-4) on top of precomputed-clone-pack and the chunked pack builder; days if those exist.

## Crash walk-through
Alarm: `boundary()` -> `buildPack` -> `BUCKET.put(checkpoint)` -> INSERT checkpoints -> demotion loop -> setAlarm(+24h).
- Crash after put, before INSERT: R2 holds an unreferenced `checkpoint-<date>.pack`; the retried alarm overwrites the same date key. Orphan only if the retry lands on the next UTC day (then a stray pack, cost not correctness). No cleanup of superseded checkpoints exists at all, so R2 grows by one clone-pack per day per repo.
- Crash mid-demotion: re-put is content-addressed and idempotent; IA objects remain online (no restore step), so nothing is unreadable. Fine.
- Crash by CPU limit (uncatchable): DO is reset, no `setAlarm(+24h)` ran, DO retries the alarm (bounded), then the constructor's `getAlarm()==null` re-arms it in 60 s on the next request. Result: a large repo re-runs a doomed multi-minute alarm every minute, with a fresh R2 put each time. Needs a `progress` row and a failure counter.

## Concurrency walk-through
Push lands while the alarm awaits R2 I/O (DO is single-threaded but interleaves at await): refs table now has tip T2, alarm computed boundary and pack from T1 and INSERTs it. Next fresh clone: client sends `want T2`, `done`; hot path streams the T1 pack. index-pack succeeds, but clone's connectivity check fails: "fatal: remote did not send all necessary objects" / "did not receive expected object T2". The proof's "one commit stale, next incremental fetch covers it" is false for clones; the hot path must be gated on `checkpoint.tips == current refs` (or top up with a thin delta and recomputed trailer), falling back to the cold path otherwise. Two writers never race on refs (one DO), so no split-brain.

## Interop check
- Response framing (`shallow-info`, `shallow <oid>` lines, `0001`, `packfile`, sideband-1 pkts of 65520 bytes max, `0000`) matches fetch-pack.c `process_shallow_info` -> `receive_shallow_info` -> `update_shallow` (non-deepen branch: shallows are recorded, then pruned to those reachable from fetched refs). git 2.4x clone would become shallow with the boundary in `.git/shallow`. OK.
- Exact break #1 (push): send-pack.c calls `advertise_shallow_grafts_buf()` unconditionally when the local repo is shallow, so every client cloned through the horizon prepends `shallow <oid>` pkt-lines before the ref-update commands in `git-receive-pack`. A receive-pack that expects the first line to be `<old> <new> <ref>\0caps` rejects the push. The proof says "pushes from a shallow clone are fine" without owning that parser change.
- Exact break #2: the fetch parser drops client `shallow <oid>` lines and `deepen-not`/`deepen-relative`; a fetch from an already-shallow clone that needs `unshallow` lines would get none, and `git fetch --deepen` from a shallow clone is mis-handled.
- Non-git clients: libgit2 >= 1.7 handles shallow-info; JGit's client-side shallow is partial; gitoxide rejects unrequested shallow sections in some versions. Cold path fallback per user-agent is needed but not sketched.
- `--single-branch` clones get the whole-repo pack and shallow lines for other branches (pruned client-side); wasteful, not wrong.

## Blockers
1. Hot path must verify the checkpoint's tips equal the requested wants/current refs; as written, every push invalidates all fresh clones until the next alarm.
2. receive-pack must parse leading `shallow <oid>` lines from shallow clients, or nobody who cloned after the horizon can push.

## Caveats
- `git clone --mirror` and backup tools silently receive a shallow mirror; pushing that mirror elsewhere fails connectivity checks. Product copy must say "history is shallow by default".
- Demotion of "blobs not in hot set" requires a full tree walk of the hot set every alarm; unchanged old blobs (most of them) must stay hot or the checkpoint pack build pays IA retrieval fees daily.
- R2 IA is beta; 30-day minimum billing and per-GB retrieval on every `--unshallow`; write a cold pack (proof hand-waves) or an unshallow is one class-B GET per object.
- Daily rebuild even with zero pushes; gate on ref change.
- Superseded checkpoint packs are never deleted.

## Verdict
lands-with-caveats. The core mechanism (server-side shallow horizon over a precomputed pack) is real, in-spec, and cheap for clones; "squash" is not delivered and cannot be. Two localized wire/consistency fixes are mandatory before it works with a real git client across a push.
