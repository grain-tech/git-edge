# Review: Search index built on push (D1 FTS / Vectorize)

> Idea #26 · edge · verdict: **lands with caveats** · feasibility 4/5 · reliability 3/5 · correctness 3/5 · effort: days
> Proof: [proofs/search-index-on-push.md](../../proofs/archived/search-index-on-push.md) · Review: [reviews/search-index-on-push.md](./search-index-on-push.md)

# Review: search-index-on-push (#26)

## Scores
- Feasibility: 4/5. Every primitive is GA: DO SQLite (`ctx.storage.sql`, FTS5 is in the supported-extension list for both DO SQLite and D1), alarms, R2 `get` + body streams, `DecompressionStream("deflate")` (zlib framing, correct for loose objects), Workers AI + Vectorize v2. Limits respected on paper: blobs capped at 512 KB (< 2 MB SQLite value cap, far below 128 MB), 20 s wall self-reschedule fits the default 30 s CPU / 15 min alarm wall budget. The one unstated cost: the design only works if `streaming-pack-parser` materializes every object as a *loose* zlib object in R2 (one PUT per object, delta-resolved). That is a design decision belonging to another idea, and the proof admits packed objects would need pack-index + range reads it does not have.
- Reliability: 3/5. Job row + alarm are durable and idempotent, but there is an unbounded-retry hole and a poison-job hole (below).
- Correctness: 3/5. It builds a per-repo FTS index, but a weaker one than "search the repo": branch conflation, no deletes, and a resume loop that never finishes large jobs.

## Crash walk-through
Push A->B lands; `postReceive` inserts the job and `setAlarm(now)` in the same event-loop turn, so job+refs+alarm are one implicit write batch. Alarm fires, indexes 40 of 300 changed blobs, then the isolate is evicted. DO SQLite writes before each `await` are already committed (implicit transactions are per-turn, not per-handler), so the FTS table holds 40 fresh rows and 260 stale ones; `index_jobs` still holds the row. The runtime retries the alarm (with backoff, but only ~6 times, not "until it succeeds" as the proof claims). Re-run re-walks the whole diff, `DELETE path` / `INSERT` for all 300: correct end state, wasted R2 reads. Two real problems: (1) if the handler *keeps* throwing (a corrupt/missing object in R2, a tree > 2 MB), the alarm is dropped after the retry budget and the queue stalls until the next push re-arms it; `ORDER BY id LIMIT 1` then re-selects the same poison job forever, blocking every later branch's indexing. Needs an `attempts` column and a dead-letter path. (2) The 20 s "resume later" branch does not persist a cursor and never skips already-indexed `(path, blob_sha)` rows, so it restarts from blob 1 every time: a job with > ~20 s of work (any initial push of a few thousand files at ~10-50 ms per R2 GET) loops forever, burning Class B ops and never deleting the job. One `WHERE NOT EXISTS (path, blob_sha)` check fixes it; as written it is a livelock.

## Concurrency walk-through
Two clients push `main` A->B and `feature` X->Y at the same time. The DO serializes them (assuming receive-pack holds an in-DO lock across its awaits; that belongs to `repo-do-ref-authority`), so job rows are ordered and the single alarm drains them FIFO; `setAlarm` from the second push while the alarm is running just re-arms, no lost wakeup. No split-brain of refs: the index never writes refs. But the index has no `ref` column: `feature` indexing `README.md` deletes and replaces `main`'s row for the same path. Searches then return whichever branch pushed last, with `blob_sha` from that branch. Search results are a race, not a view of any branch. Fix: key rows by `(ref, path)` or index only the default branch. A force-push (old=B, new=D, unrelated history) diffs tree(B) vs tree(D) correctly. Deletions (present in old tree, absent in new) are never removed (acknowledged), so a renamed file returns hits under both names until the old path is reused.

## Interop check
No wire impact. `receive-pack` returns `unpack ok` / `ok refs/heads/x` report-status after the CAS; the alarm runs after the response is flushed, so `git push` from 2.4x sees a normal push. Note: `git push` uses protocol v0/v1 (there is no v2 receive-pack), so a "protocol-v2 `search` command" can only ride on upload-pack's v2 capability advertisement; a git client ignores unknown capabilities, so harmless. `X-Index-Lag` is a non-git HTTP header, ignored. One nit: `MATCH ?` with a raw user string throws on FTS5 syntax (unbalanced quotes, leading `*`, `NEAR`), so the HTTP endpoint must escape/quote terms or return 400.

## Blockers
- Resume path livelocks on any job larger than one 20 s slice (no cursor, no skip-if-indexed). Trivial fix, but as written initial pushes of real repos never index.
- Depends on every pushed object existing as a loose zlib object at `objects/xx/yyyy` in R2; if packs are stored as packs (the sane choice for a big push), `readBlob`/`readTree` need pack-idx + delta resolution that this proof does not contain.

## Caveats
- Branch conflation: single flat `path` key across all refs; results depend on push order.
- Alarm retries are bounded (~6); a poison job stalls the whole queue (`ORDER BY id LIMIT 1`).
- Deletes/renames not handled; stale hits persist.
- FTS5 in DO SQLite is documented, but the D1 fallback loses the "same transaction as the ref flip" property the proof leans on.
- Cost: one Class B op per changed tree+blob; 10k-file push = 10k GETs plus minutes of alarm time; no batching.
- Vectorize stage is a sketch (no chunking, dedup, or cost model).

## Verdict
lands-with-caveats. The core loop (durable job row, alarm, tree-diff, FTS5 upsert) is sound and buildable in days on top of the loose-object storage assumption. It needs the four small fixes above (skip-if-indexed, attempts counter, ref column, delete pass) before it is a search index rather than a "last-pushed-branch grep"; the harder unknown is whether the object store really keeps everything loose.
