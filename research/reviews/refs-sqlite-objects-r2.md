# Second-pass review: Refs in DO SQLite, objects in R2

# Review v2: refs-sqlite-objects-r2

## Scores
- Feasibility: 4/5 (was 4). Every API in the block exists in the pinned crates, checked against source, not the memo: `SqlStorage::exec(&str, impl Into<Option<Vec<SqlStorageValue>>>)` so `Some(args)` binds; `SqlStorageValue: From<String/&str/i64>`; `Range::OffsetWithLength { offset, length }` is the exact variant name (worker 0.8.5 `r2/builder.rs:98`); `ObjectId::{from_hex(&[u8]), to_hex() -> Display, is_null}`; `gix_validate::reference::name_partial(&BStr)`. Two of the proof's "unverified" items are therefore verified. `transactionSync` is confirmed absent (only async `Storage::transaction`), which the contract tolerates. The point off: `jobs::enqueue` is `fn` but arming the alarm needs async `set_alarm`, and the code shown never arms it.
- Reliability: 4/5 (was 2). Both first-pass data-loss paths are gone by construction (below). Remaining: an `Err` returned mid-loop from `apply_ref` after earlier refs moved leaves a half-applied commit with `pushes.state='open'` and no `refs_version` bump unless the DO `fetch` re-throws it (rollback needs a throw, not a 500 Response); incomplete multipart uploads of crashed pushes are never aborted.
- Correctness: 4/5 (was 3). No wire byte in this slice would break git 2.4x. `peeled` is always `None`, so scenario 3 cannot pass as written.
- Effort: weeks (this slice alone is days; "working" means ingest, wire and jobs around it).

## Contract compliance
- Schemas: `refs`, `reflog`, `pushes`, `packs`, `objects` columns used exactly as in sections 2.3 and 3; reader query in `lookup` is verbatim. Key builder is `keys::pack(&self.repo, ..)` with `repo` from `meta` (2.2, 8). Pass.
- CAS: `changed()` is `SELECT changes() AS n` issued immediately after each statement; `rows_written` never appears. Steps 1-7 in contract order; `commit_push` is `fn`, no await, JSON parsed by caller. Pass.
- Alarm: only `jobs::enqueue` is called, no `set_alarm`. Pass on the letter; but 4.1 says `enqueue` calls `rearm`, which is async, so as written nothing arms the alarm after commit (GcMark waits for the Janitor's 15-min firing). Contract inconsistency; proof flags it, does not resolve it. Violation (documented).
- Signatures: `Bucket::read_range/read_entries` add `&mut ReqBudget` and take `&[(ObjectId, ObjLoc)]`, deviating from 1.2 while matching section 9 and rule 7.1. The contract contradicts itself; the proof picks the side that can compile. Violation (documented, needs write-back).
- `peeled: None` cannot satisfy scenario 3 ("peeled line in ls-refs"); `refs` has no `peeled` column and the route allows no await. Violation of the test plan, honestly declared.
- Step 2 rejection writes `state='rejected', ended_at` but not `result`; `pushes.result` is "JSON of per-ref results". Minor deviation.
- Error policy: `Error::Conflict` has no HTTP mapping in section 10 (contract gap, not the proof's). `as i64` on `Date::now()` is in `repo_do`, outside the lint list. Pass.

## First-pass blockers
1. Query string dropped in routing: resolved. `list_refs` returns `(Option<BString>, Vec<RefRow>)`; the DO sees no client URL (1.3 table). Genuine.
2. Thin-pack bases from R2: resolved for this slice by `Index::lookup` (live-only join) plus `read_entries` returning full entries; delta application itself is deferred to streaming-pack-parser, which is where it belongs. Genuine, with a dependency.
3. Shared `objects/default/` prefix: resolved. `let key = keys::pack(&self.repo, &first.1.pack)` is the only key builder in the read path; no default, no header. Genuine.
4. Sweeper/push interleave deleting a soon-referenced object: resolved. `if self.meta_i64("gc_epoch")? != push.gc_epoch { ... 'rejected' }` and the `Index(sql).lookup(&[c.new])` re-guard sit in the same span as the `UPDATE refs`. A dead pack cannot be resolved by `lookup` (`p.state='live'`), so no ref can be created pointing into it. Genuine.
5. Index rows before R2 put resolves: resolved. Rows land under `state='ingesting'`, invisible to `lookup`; `UPDATE packs SET state='live' WHERE id=? AND state='ingesting'` in step 3 is the only transition and is guarded by `changed()`. Genuine.

## Crash walk-through
Push of 500 objects to `main`. Edge finishes the normalized pack, posts 500 `objects` rows (pack `ingesting`), calls `/_do/push/commit`; the DO isolate dies after step 4 wrote two of three refs. The span's SQLite writes are not flushed, so `refs`, `packs.state`, `pushes.state='open'` are all pre-commit. Client gets a transport error; retry sends a new push with a new pack (duplicate sha rows across two live packs are legal, 2.3). The Janitor expires the old push after 1 h, kills its pack and rows in one span, deletes the R2 key after GRACE. No reader ever resolved a sha from the dead pack because `lookup` joins on `live`. The first-pass "push B includes sha X from crashed push A" case is moot: B's client re-sends X (refs never moved) and B's pack is self-contained. Gap: the crashed push's `pending/` multipart and any unfinished normalized-pack multipart are never `abort`ed; R2's default 7-day cleanup is the only backstop.

## Concurrency walk-through
A (`S0->S1`) and B (`S0->S2`) on `main` ingest in separate edge isolates and post index rows into interleaved sync spans (distinct `pack_id`s, no conflict). Commits serialize: A's span flips pack A live, `UPDATE refs SET target=S1 WHERE name='refs/heads/main' AND target=S0` -> `changes()=1`, reflog, `refs_version+1`, GcMark enqueued. B's span flips pack B live, same UPDATE with `target=S0` -> `changes()=0` -> `ng refs/heads/main failed to update ref`, push `committed` with an ng result, pack B live but unreachable until GC consolidates it. If GcSweep ran between B's `/_do/push/begin` and its commit, `gc_epoch` differs and B gets `ng ... gc ran during push, retry`. Two creates of the same new ref: `ON CONFLICT DO NOTHING` gives the second `changes()=0`. Exactly one `ok`, matching scenario 6.

## Interop check
git 2.43-2.47 sends `Git-Protocol: version=2`; `ls-refs` with `symrefs peel unborn`. From this slice: targets are lowercase 40-hex (`to_hex`), names are raw bytes, HEAD is `symref-target:refs/heads/main`. The missing `peeled:` attribute is legal (git falls back to fetching the tag). Push results reuse git's own strings (`failed to update ref`, `funny refname`, `missing necessary objects`, `deletion of the current branch prohibited`); `report-status-v2` accepts any `ng` text. No breaking byte found. Two non-fatal edges: `name_partial` accepts `HEAD` or `foo` as a ref name (git requires `refs/`), so a non-git client could create a ref `HEAD` and ls-refs would then emit two `... HEAD` lines; and `meta.head` stays `refs/heads/main` when the first pushed branch is `master`, so `git clone` warns "remote HEAD refers to nonexistent ref".

## Blockers
- Scenario 3 cannot pass: add `peeled TEXT` to `refs` (section 3) filled at commit from the tag the edge already inflated; until the contract is amended this proof fails its own named scenario.
- Contract write-backs the code depends on: 1.2 `read_range`/`read_entries` signatures (budget, `(ObjectId, ObjLoc)`), and who calls `rearm` after a sync `enqueue` (the async `fetch` wrapper must `rearm().await` after `commit_push` returns).

## Caveats
- DO `fetch` must propagate `Storage`/`Internal` errors from `commit_push` as `Err(worker::Error)` (a throw) so the span rolls back; building a 500 Response commits a half-applied commit.
- Janitor should `abort` multipart uploads of expired pushes (needs the upload id on the `pushes` row).
- Store `result` on the step-2 rejection path; require `refs/` prefix (`name()` not `name_partial`) for pushed names; adopt the first created branch as `meta.head` when HEAD dangles.
- Unverified on real infra only: R2 range reads and 8 MiB multipart, subrequest limit, panic rollback (scenario 15).

## Verdict
lands-with-caveats. Genuinely better than the first pass: 4/2/3 -> 4/4/4. All five first-pass blockers are closed by code, not prose; the remaining items are contract amendments and one schema column, not redesigns.
