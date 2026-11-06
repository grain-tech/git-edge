# Review: Storage tiering by heat

> Idea #50 · wild · verdict: **lands with caveats** · feasibility 4/5 · reliability 3/5 · correctness 3/5 · effort: weeks
> Proof: [proofs/storage-tiering.md](../../proofs/archived/storage-tiering.md) · Review: [reviews/storage-tiering.md](./storage-tiering.md)

# Review: storage-tiering (#50, wild)

## Scores
- Feasibility: 4/5. Every primitive is GA today: SQLite DOs, alarms (15 min wall cap), R2 `put({storageClass})` and `R2Object.storageClass`, streaming `put(key, obj.body)`. The proof calls IA "beta"; current docs no longer say so. The one real gap is confirmed: the Workers binding has no copy/re-class call, only S3 `CopyObject` + `x-amz-storage-class` (needs SigV4 creds in a Worker) or lifecycle rules (age-since-upload, and IA->Standard is not expressible). Limits: 2 MB row cap respected via HOT_MAX=512 KiB; 50 keys/night x up to 1 GB streamed get->put will blow the 15-minute alarm wall clock, as the proof itself admits.
- Reliability: 3/5. No data-loss path (R2 is canonical, DO tier is a pure cache, refs untouched). But the ledger drifts from R2 truth after a crash and the sweep can permanently starve (see below), and every flap costs a 30-day IA minimum charge.
- Correctness: 3/5. A weaker lookalike of the stated idea. "Hot objects stay in DO" is a <=512 KiB read-through cache; "untouched objects migrate to IA" applies only to keys >= 1 MiB (packs). Loose objects are never tiered, by the proof's own cost argument. Honest, but it is not the catalogued idea.
- Effort: weeks (ledger + sweep in days; replacing the in-DO stream copy with S3 CopyObject, ledger rebuild from `list()`+`head()`, and threading `read()` through every upload-pack path is the real work).

## Crash walk-through
Nightly `alarm()` picks key K (warm, 1.2 GB pack, untouched 45 d). `setClass` does `get(K)` then `put(K, body, {storageClass:"InfrequentAccess"})`. DO is evicted mid-stream. R2 `put` is atomic per key: either the Standard object survives or the IA object landed; no torn object, no orphan (same key). The `UPDATE heat SET tier='cold'` never ran. Platform retries the alarm; if the put had landed, `setClass` hits `src.storageClass === storageClass` and returns early WITHOUT updating `heat`. Result: R2 says IA, ledger says warm. The `SELECT ... LIMIT 50` has no ORDER BY and no exclusion, so the same already-IA keys are re-selected every night and, once 50 such keys exist, demotion of every other key stops forever. Also `hits` is never reset for that key, so it is not eligible for the cold->warm promotion path either (ledger thinks it is warm). Fix: update the ledger before the early return, and order/paginate the sweep by `last_touch`.

## Concurrency walk-through
Two writers: a `git push` (streaming-pack-parser `recordWrite` + R2 `put(K, Standard)`) and the sweep's `setClass(K, IA)` in flight at the same DO await point. Content-addressed key => both puts carry identical bytes; last-writer-wins on class only, so the worst outcome is billing (Standard object marked cold, or an IA object that the push's `INSERT OR REPLACE` marks warm). No split-brain: refs are never touched here. A concurrent `fetch` range-reading K during the re-put sees a consistent old-or-new object with the same bytes. The dangerous interleaving is with `gc-and-repack-alarm`: sweep `get(K)` succeeds, GC deletes K as unreachable, sweep `put(K)` resurrects it as an IA object with a 30-day minimum charge and no ledger row pointing at it (GC removed it) — a billed orphan. Same-DO single-threading does not protect this because both alarms yield at `await`.

## Interop check
The git client never sees a tier; a v2 `fetch` `packfile` section is bytes regardless of source, and R2 IA has no rehydration step, so no protocol timing changes. The wire detail that breaks is inherited from "entries stored pack-ready and spliced": an OFS_DELTA entry carries a negative offset to its base in the *source* pack; spliced into a freshly assembled pack at a different offset the client's index-pack fails with "bad object" / "delta base offset out of bound". Entries must be stored as REF_DELTA or undeltified, or offsets rewritten at splice time. A second, softer break: per-object `await BUCKET.get()` on a 10k-object fetch stalls the sideband for minutes; git has no packfile timeout, but `http.lowSpeedLimit/lowSpeedTime` users and proxies will abort.

## Blockers
- Sweep starvation + ledger drift after a crashed re-class (early return in `setClass` skips the ledger update; unordered LIMIT 50).
- In-DO streamed re-class of GB packs cannot fit the 15-minute alarm wall cap; needs S3 `CopyObject` from a plain Worker, which the proof only mentions as "the real implementation".

## Caveats
- The proof's economics are right: IA tiering only pays for large, rarely-cloned packs; DO storage is ~13x R2 per GB so the "hot in DO" half is a latency feature, not tiering.
- IA is no longer documented as beta, but 30-day minimum billing makes promotion flapping a cost bug, not a correctness bug.
- Heat ledger is unrecoverable without a `list()`+`head()` rescan; a DO recreated from R2 state silently treats everything as warm.
- Hot admission on the read path has no byte budget; a `git log -p` churns the `hot` table (admitted in the proof).
- GC/sweep race resurrects deleted packs unless GC and the sweep share a tombstone or run in the same alarm.

## Verdict
lands-with-caveats. The R2-is-canonical / DO-is-cache design is sound and cannot lose repo data, but the proof code as written has a sweep that stalls after the first crash and a re-class path that cannot run inside an alarm at pack sizes. It delivers "large cold packs to IA + small-object DO cache", not per-object heat tiering.
