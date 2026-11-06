# Second-pass review: One Durable Object per repo as the ref authority

# Review v2: repo-do-ref-authority (idea #1)

## Scores
- Feasibility 4/5 (was 4). Every host API used is in `worker` 0.8.5 and exercised by the spike (`SqlStorage::exec(&str, impl Into<Option<Vec<SqlStorageValue>>>)`, `SqlCursor::{one,to_array}`, `id_from_name().get_stub()`). Two compile errors in the shown code: `V::from(req.pack_id.as_deref())` (no `From<Option<&str>>` for `SqlStorageValue`; only bool/i32/i64/f64/String/&str/Vec<u8> exist in `worker-0.8.5/src/sql.rs`), and `return self.fetch_v2_entry(..).await` inside the non-async `and_then` closure. Both are mechanical. `transactionSync` is confirmed absent (only `async fn transaction` in `durable.rs`); the span holds without it, as the contract says.
- Reliability 4/5 (was 3). Ordering is now pack-durable -> rows -> commit, with the `live` flip inside the CAS span; the crash-before-commit case is clean. One hole remains: mid-span `Err` is turned into a Response instead of a throw, so partial writes persist (below).
- Correctness 4/5 (was 3). Per-ref CAS matches `ref_transaction_update` with the advertised old oid; connectivity (2.5) and `gc_epoch` close the "consistent repo" gap the first pass named; sideband, delete-refs, 0-object packs are handled by contract rules 4/5 and `pack_id: Option`.
- Effort: weeks. The DO module alone is days; scenario 2/4/5/6 need ingest, wire and jobs.

## Contract compliance
- Module/struct names, `RepoDo { state, env, booted: RefCell<bool> }`, route table, `commit_push` signature, tables, `changes()` oracle (never `rows_written`), no `set_alarm` outside `jobs::rearm`, R2 keys untouched by the DO: all match sections 1.3, 2.3, 3, 4, 8.
- Deviation 1: steps 5 -> 7 -> 6. The proof calls `jobs::enqueue` before `finish_push`; the contract orders 6 before 7. Harmless only while `enqueue` is truly sync; `Storage::set_alarm` is `async` in 0.8.5, so `rearm` can only stay sync by firing the JS promise unawaited. If anyone "fixes" that with an await, the `pushes` row update lands after a gate opening.
- Deviation 2: `/_do/push/begin` body is `{push_id, principal}`, contract says `{push_id}`. The contract is self-inconsistent (`pushes.principal NOT NULL`), so the contract row needs amending, but as written it is a schema deviation.
- Deviation 3 (edge side): `receive_pack` maps only `Error::Unpack` to the HTTP-200 `unpack <msg>` report; a `Conflict`/`Storage` from `/_do/push/commit` becomes `Err(e)` -> non-200 after the header was parsed, contrary to section 10 ("for receive-pack after the header was parsed: HTTP 200 with unpack <message> and ng ..."). Also `Error::Conflict` has no status in the section 10 mapping at all (contract gap).
- Deviation 4: `respond(out)` converts a mid-span `Err` into an HTTP response and returns `Ok`, so the platform sees a normal return and commits everything written so far. Section 3 relies on "rolls them back if the handler throws". Storage/Internal errors inside `commit_push` must propagate as `Err` out of `fetch` (Corrections #4: that is a throw and a 500) to get the rollback.
- Non-deviations worth noting: step 3 flips the pack `live` even when every CAS then fails (contract orders it that way; the pack becomes GC junk). `report(&hdr, Err(&msg), &[])` relies on `wire` synthesising `ng <ref> unpack failed` per command from an empty results slice; the contract signature does not say it does.

## First-pass blockers
1. Commit ordering / orphaned ref: resolved. `receive_pack` awaits `pack::ingest::run` (finish + index) and only then posts `/_do/push/commit`; `UPDATE packs SET state='live' WHERE id=? AND state='ingesting'` runs in the same span as the ref CAS, and `finish_push` writes `pushes.state='committed', pack_id`. The Janitor kills only `ingesting` packs of `expired`/`rejected` pushes (section 5). Depends on `ingest::run` (other proof) really awaiting `PackWriter::finish`.
2. `rowsWritten` miscount: resolved. `fn changes(&self)` issues `SELECT changes() AS n` immediately after each write; `refs` is `WITHOUT ROWID`; measured 1/0/1 (platform-facts #1) and the spike uses the identical call.
3. Chunked bodies vs known-length put: resolved by dependency, not by code here. `BodyReader::new` (section 6) and `stream_to_pending` 8 MiB multipart (section 2.4) are cited; multipart is measured on the simulator only (#6). Acceptable for this idea's scope.

## Crash walk-through
Edge dies after `/_do/push/index` returns, before commit: push `open`, pack `ingesting`, R2 pack present. `Index::lookup` filters on `state='live'`, so nothing sees it. Janitor after 1 h: `expired`, pack `dead`, objects rows gone, R2 key deleted after GRACE. Refs untouched; client retry re-ingests. Clean.
Edge dies after the commit span returned: ref moved, pack live, push committed; retry gets `ng failed to update ref`, client fetches. Same as any git server. Clean.
The hole: two-ref push, ref 1 CAS + reflog succeed, the reflog INSERT for ref 2 hits a SQLite error. `apply_one` returns `Err(Storage)`, `commit_push` propagates, `respond()` builds a 500 Response, `fetch` returns `Ok`. The span commits: ref 1 moved, pack live, but `refs_version` not bumped, `pushes` still `open`. A `GcSweep` whose mark ran before this push now sees an unchanged `refs_version` and proceeds; if ref 1 was reset to an old, unmarked commit in a candidate pack, that commit is swept and ref 1 dangles. Rare, but it is exactly the race section 5 claims impossible. Fix: throw (return `Err` from `fetch`) for Storage/Internal inside the sync span.

## Concurrency walk-through
A: `X->Y`, B: `X->Z`. Both `begin` (two `open` rows, same `gc_epoch`), both ingest and index (two `ingesting` packs). A's span: pack A live, `UPDATE refs ... WHERE name=? AND target=X` -> `changes()=1`, reflog, `refs_version+1`, `GcMark` enqueued, committed. B's span, strictly after (no await inside either): pack B live, same UPDATE -> `changes()=0` -> `ng refs/heads/main failed to update ref`, `any_ok=false`, committed with the ng result. No lost update, no dangling ref. Side effect: pack B is `live` with unreachable objects until `GcMark`/`GcConsolidate` drops it (created_at young, so exempt for GRACE). Delete `Y->0` racing `Y->W`: first span wins, second gets ng. GcSweep interleavings: covered by `gc_epoch` (step 2) and `refs_version` (step 5), both checked in sync spans.

## Interop check
git 2.43/2.45/2.47 `push` uses v0 receive-pack over HTTP even with `protocol.version=2`; the DO returns JSON and `wire` frames the report, so the DO itself emits no wire bytes. What can break, all in `wire`, not here: under `side-band-64k` real receive-pack sends the pkt-lines *including an inner `0000`* inside band 1, then an outer `0000` (receive-pack.c `report()` does `packet_buf_flush(&buf)` before `send_sideband`, then `packet_flush(1)`). Contract rule 4 says "final flush ... outside the band" and is silent on the inner one; if it is dropped, 2.4x `receive_status` ends on demux EOF rather than the flush, which works today but is not the reference byte stream. `report-status-v2` without `option` lines parses fine. Reason strings (`failed to update ref`, `missing necessary objects`, `funny refname`, `deletion of the current branch prohibited`) are git's own. `name_partial` accepts `main` where git wants `refs/...`; 2.4x never sends bare names, so no wire effect.

## Blockers
- Mid-span `Err` returned as a Response instead of thrown: defeats the rollback that sections 3 and 5 rely on; partial multi-ref commit without `refs_version` bump can be swept. One-line fix in `fetch`.
- Proof code does not compile: `SqlStorageValue::from(Option<&str>)` in `finish_push`; `.await`/`return` inside the `and_then` closure in `fetch`. Mechanical.
- Edge `receive_pack` returns non-200 for commit-time `Conflict`/`Storage` after the header was parsed (section 10 requires a 200 `unpack`/`ng` report); `Conflict` has no HTTP mapping in the contract.

## Caveats
- Step order 5,7,6 vs contract 5,6,7; only safe while `jobs::enqueue` never awaits (`set_alarm` is async in 0.8.5, so `rearm` must fire the promise unawaited).
- `/_do/push/begin` body adds `principal`; contract route schema needs the same amendment.
- `transactionSync` confirmed absent from `worker` 0.8.5 (not merely unverified); span atomicity rests on the no-await rule alone, which is measured (#4).
- Unverified per memo: `PanicError` rollback (scenario 15), `web_sys::Crypto` binding for `PushId::random()`/`repo_id`, `ctx.id.name` in production, R2 multipart on real R2, subrequest limit in production.
- A CAS-rejected push still leaves its pack `live` (contract-ordered); storage churn until GC.
- `report(.., Err, &[])` assumes `wire` synthesises per-ref `ng ... unpack failed` lines.
- Wrangler `class_name` must match the Rust struct (`RepoDo` in contract, `RepoDO` in memo/spike).

## Verdict
lands-with-caveats. Genuinely better than the first pass: ordering, `changes()`, connectivity, sideband and delete-refs are all now enforced by contract-bound code rather than claimed (3/3 first-pass blockers resolved). What remains is small and local: throw instead of respond on mid-span errors, two compile fixes, and the edge error-to-report mapping. Reliability 3->4, correctness 3->4.
