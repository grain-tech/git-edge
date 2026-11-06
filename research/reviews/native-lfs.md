# Second-pass review: Git LFS natively via presigned R2 URLs

# Review v2: native-lfs (idea #11)

## Scores
- Feasibility 4/5 (was 4). Every host call is in `worker` 0.8.5 source: `Bucket::head` (`r2/mod.rs:29`), `delete_multiple` (`:91`), `Object::{size,checksum,body}` (`:203,:241,:275`), `R2Checksums.sha256: Option<Vec<u8>>` (`worker-sys r2/checksums.rs`), `ObjectBody::stream` (`:307`), `Env::{secret,var}` with `Secret`/`Var` = `StringBinding: Display` (`env.rs:202,244-246`), `SqlStorageValue: From<&str|i64>`, `SqlCursor::{to_array,one}`, `Response::from_json`, `Request::{text,url}`. SigV4 canonicalisation reads correct against the AWS spec (sorted encoded query, `host;x-amz-checksum-sha256`, `UNSIGNED-PAYLOAD`, `auto/s3/aws4_request`, Hinnant civil-date) but is unrun. Deduction: the proof says `hmac` and `hex` "are already in the spike's dependency tree"; `spikes/rust-ls-refs/Cargo.lock` has `faster-hex`, `digest`, `sha1`, not `hmac`/`hex`/`sha2`/`base64`; four new crates, all pure Rust, none built.
- Reliability 4/5 (was 3). Stranded-404 and sweep starvation are closed structurally (state-guarded upsert with `changes()`, adoption, `enqueue` dedup). Residual: verify is not idempotent (below), and a revived dead key can be deleted under a fresh PUT during one `delete_multiple` await.
- Correctness 4/5 (was 4). Same real batch API; all three first-pass blockers closed; one new client-visible regression (409 on an already-`ok` object) and one client-controlled panic path in `edge`.
- Effort: days. Two to three days for the four files plus `Bucket` wrappers; two more on a deployed Worker with a real bucket for the checksum gate and a git-lfs round trip (the local simulator has no S3 endpoint).

## Contract compliance
Kept: `changes()` is the only write oracle (line 157), `rows_written` unread; `lfs_batch` is one sync span (`from_env`, `repo_id`, `presign` are sync); `lfs_verify` awaits `head` then writes in a fresh span (145-147); no `set_alarm`, `LfsSweep` returns `SliceOutcome` only, `enqueue` dedups by kind; sweep marks `dead_at=now` in step 1 and deletes only `dead_at < now-GRACE` in step 2 (191-196); `repo_id` from `meta`; `oid_bytes` gates every oid before `keys::lfs`/`presign`/SQL; `MAX_BATCH` -> `Limit` -> 413.
Deviations (all also in the structured list):
1. Section 2.2 "nothing else is ever written to R2": new key `r/<repo_id>/lfs/<oid>`. Declared write-back.
2. Section 1.2: `Bucket { inner, repo }` fields are private and the type has no `head`/`get`/`delete_multiple`; the code reads `bucket.inner`/`bucket.repo` from `repo_do` and `jobs`. Declared write-back, but section 12 requires compiling "against the signatures in section 1 exactly as written".
3. Section 10 lint (`deny(clippy::indexing_slicing)` in `edge`): `b["public_base"] = ...` (line 211) indexes a client `serde_json::Value`; `IndexMut` panics when the body is `[]`, `1` or `"x"`. A client-controlled panic.
4. Section 10 has no pre-response arm for `Conflict`; the proof adds 409 + JSON body on `/info/lfs/*`. Declared write-back.
5. Section 4.3: a slice returns `Continue` at 80 % budget. The sweep uses a fixed 320-head count and `ReqBudget{max_ms: 20_000}`; a slow slice trips `charge` -> `Err` -> attempts++/backoff (4.4), and eight slow slices make the job `dead` until the next batch re-enqueues.
6. Section 1.3 route table, `JobKind`, `lfs_objects` table, new `lfs` module imported by `repo_do` and `edge` (1.1 direction). Declared write-backs.
7. Helpers attributed to repo-do-ref-authority: `q`/`changes`/`sql` exist there but are private `fn`, so `jobs/lfs_sweep.rs` cannot call `d.q`; `bucket()`, `repo_id()` and `json` are defined in no research/proofs file.
Section 5's GRACE argument assumes keys are never reused; `lfs/<oid>` is reused when a `dead` row is revived by a new upload batch (`WHERE state!='ok'`, line 129). See concurrency.

## First-pass blockers
1. `verify` UPDATE -> stranded 404: **resolved**. `lfs_set_state` (154-157) is `INSERT ... ON CONFLICT(oid) DO UPDATE ... WHERE lfs_objects.state=?` + `changes()`; a missing row is inserted as `ok`. Sweep step 1 adopts (`Head::Match => lfs_set_state(.., "ok", "pending")`, line 186) instead of deleting. New regression: a row already `ok` makes the same statement match zero rows -> `Conflict` -> 409 (blocker 1 below).
2. `oid` unvalidated: **resolved**. `oid_bytes` (71-74) is 64 lowercase hex or `Error::Protocol`; called at 118, 144, 182 before any key/signature/binding; key is `format!` under `r/<repo>/lfs/`, no URL normalisation.
3. Router unshown: **resolved**. `edge::lfs::route` (203-219) maps `objects/batch` -> `/_do/lfs/batch`, `verify` -> `/_do/lfs/verify`, and injects `public_base` so `verify.href` is absolute `.../info/lfs/verify`.
Caveats: alarm starvation closed structurally (no `set_alarm`); orphan payloads now swept per repo; `expires_at`, 422 oversize, per-operation `can_write` added; checksum-rejection gate still open, as admitted.

## Crash walk-through
Batch inserts `pending` for X, `LfsSweep` queued at +2 h; client PUTs 3 GiB; DO dies inside `lfs_verify` between the `head` await and `lfs_set_state`. Client sees 5xx (git-lfs: fatal, push fails). Path A: user re-pushes; batch upserts `pending` again (fresh `created_at`, new URL), PUT is a byte-identical overwrite, verify -> `ok` -> 200. Path B: user does nothing; at +2 h the sweep `head`s X, size and stored sha256 match -> adopted `ok`; the next `download` batch serves it. No lie in either path. Sweep dies after `delete_multiple` returns and before the row `DELETE`s: rows stay `dead` with old `dead_at`, the next slice re-lists them, R2 delete of absent keys succeeds, rows go. Edge dies after the DO batch span committed: row `pending`, never PUT; sweep `head` -> `Missing` -> row dropped, no R2 call. Clean.

## Concurrency walk-through
Pushers A and B upload the same oid X at once. Both batches upsert `pending` (both `WHERE state!='ok'` hold), both PUT identical bytes to one key. A verifies: `head` match, `lfs_set_state(.., "ok", "pending")` -> `changes()=1` -> 200. B verifies: `head` match, same statement, `WHERE lfs_objects.state='pending'` is now false -> `changes()=0` -> `Conflict` -> 409. The proof calls this "a no-op conflict" and says git-lfs "retries the whole object once"; git-lfs 3.x wraps a 4xx as a plain `ClientError`, not `RetriableError`, so B's push fails on `verify` and only a manual re-push (batch now returns no actions) succeeds. Same outcome for a verify whose 200 was lost on the wire and retried, and for a verify that arrives after the sweep adopted the object. Second: sweep step 2 lists `dead` X at T and awaits `delete_multiple`; in that await a batch revives X (`state='pending'`) and hands out a PUT URL. If the PUT lands before R2 executes the delete, the good bytes are removed; verify -> `Missing` -> 409; the post-await row `DELETE ... AND state='dead'` matches nothing, so SQLite never lies. Window = one R2 delete round-trip vs. a client batch+PUT, so rare; a generation suffix in the key, or refusing to revive `dead` rows until swept, closes it.

## Interop check
git 2.43-2.47 protocol v2 is untouched: LFS lives on `/info/lfs/*` JSON routes, no pkt-line byte changes, and `.gitattributes`/pointer blobs are ordinary objects through the foundation's push. On the LFS side: `transfer: "basic"`, object-level `authenticated: true`, `actions.upload.header` echoed verbatim (so the signed lowercase `x-amz-checksum-sha256` rides along; SigV4 lowercases), absolute `verify.href`, `expires_in` seconds + RFC 3339 `expires_at`, per-object 404/422, `content-type: application/vnd.git-lfs+json` on every reply, unknown request fields (`ref`, `transfers`, `hash_algo`) ignored by serde: all right. The byte that breaks: HTTP status `409` from `/info/lfs/verify` for an object whose row is already `ok`; git-lfs expects `200` and aborts the push. A wrong SigV4 canonical request would show as `403 SignatureDoesNotMatch` on the first PUT, not silently.

## Blockers
- `lfs_verify` is not idempotent: an already-`ok` row (second concurrent pusher, sweep-adopted object, retried verify) returns 409 and fails the push. On `Conflict`, re-read the row in the same span and return 200 when `state='ok'` and `size` matches.
- `b["public_base"] = ...` in `edge::lfs::route` panics on non-object JSON and is forbidden by the section 10 lint set; deserialize into `BatchIn` (plus `public_base`) at the edge or guard with `is_object()`.
- Code as shown does not compile against section 1 / its siblings: private `Bucket` fields and missing `head`/`get`/`delete_multiple` wrappers; `q` is private to `repo_do` yet called from `jobs`; `bucket()`, `repo_id()`, `json` undefined anywhere.

## Caveats
- Day-1 gate on a real bucket: presigned PUT with mismatched body rejected, and `head().checksum().sha256` populated. If the stored checksum is absent, every object > 256 MiB is `Mismatch` -> `dead` -> deleted after GRACE: a silent size ceiling, and a destructive one.
- `hmac`, `hex`, `sha2`, `base64` are not in the spike tree (proof says two of them are); pure Rust, expected to build on wasm32, unbuilt.
- Key reuse on revived `dead` rows opens a delete-vs-fresh-PUT window (above); section 5's "keys never reused" premise no longer holds for the `lfs/` prefix.
- Sweep budget overrun becomes `Err`/backoff rather than `Continue`; a repo with many stale rows can push the job to `dead`.
- Section 10 `Conflict -> 409` and JSON error bodies on `/info/lfs/*` are new mappings; `stub_json`/`from_do_response` must carry the JSON body through.
- Reachability GC of `ok` payloads, 5 GiB single-PUT cap, no locks/multipart, presigned hrefs off-CDN, cross-repo dedup given up, subrequest limit unmeasured in production: admitted in the proof.
- Local harness cannot run this idea; both scenarios need `wrangler dev --remote` or a deploy.

## Verdict
lands-with-caveats. Better than the first pass by construction, not by claim: all three blockers are closed with code that can be pointed at (`oid_bytes`, `lfs_set_state`+`changes()`, `edge::lfs::route`), the sweep obeys the section 4/5 rules to the letter, and the crash paths walk clean. Reliability 3 -> 4. What is new is small and local: verify must accept an already-verified object instead of 409-ing the client, the edge must not index client JSON, and the helper/`Bucket` seams must be spelled out so the file compiles. Days, not weeks, once the foundation exists.
