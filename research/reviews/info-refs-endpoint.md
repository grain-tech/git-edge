# Second-pass review: The /info/refs?service= entrypoint and pkt-line codec

# Review v2: info-refs-endpoint (idea #53)

## Scores
- Feasibility 4/5 (was 5 for the TS version). Every API is in `worker` 0.8.5 and the core path (`id_from_name`, `get_stub`, `fetch_with_request`, `Response::from_bytes`, `blocking_io::encode`) ran against git 2.43 in the spike. Unrun: `RequestInit`/`Request::new_with_init`/`headers_mut` for the stub request (memo: source only). Compile-level gaps: `finish` uses `?` on `w.text()` (crate `Error`) inside a `worker::Result` fn, so it needs `impl From<Error> for worker::Error` that is not shown; `serde`, `serde_json`, `js-sys` are absent from the memo section 4 manifest.
- Reliability 5/5. Read-only; one `Vec<u8>` response, no writer task; every failure is a status before the first byte.
- Correctness 4/5. v2 advertisement bytes are pinned in `pkt_tests::advertisements` (prefixes 0x1e/0x0e/0x17/0x13/0x19/0x17 recomputed here, all right) and match rule 6; receive-pack caps match rule 5 verbatim. Deductions: `HEAD` in the receive-pack advert, `version 1` bytes not pinned by any test, error bodies git will not display (below).
- Effort: days. Better than the first pass in substance: 5 of 6 first-pass caveats are closed by code in this proof, the sixth by a flag into another module; one new layout defect introduced.

## Contract compliance
- Module/signatures: `edge::route(Request, Env) -> worker::Result<Response>` (1.4) exact; `wire` additions import no `worker`; identity per 8.1 (`id_from_name("owner/repo")`, `x-ge-owner`/`x-ge-repo`, `[A-Za-z0-9._-]{1,64}`, `.git` stripped); budget per 7.1 (9,000 / 240,000 ms, `charge(1)` before the only stub call, `x-ge-subrequests`); status table per section 10 including 401 + `WWW-Authenticate: Basic realm="git-edge"`, plain text on `info/refs`, `ERR` pkt on POSTs; `Conflict->409`, `Unpack->400` are additions where the contract is silent, not conflicts. No writes, no alarms, no `rows_written`, so sections 3-5 do not apply.
- **Violation (dependency direction, section 1):** `repo_do -> {wire, store, jobs, pack::generate}` is binding, yet the proof places `RepoHeaders::from_request` ("what RepoDo::boot compares"), `RefsDto`/`refs_json` ("used by RepoDo::fetch") and `respond`/`finish` ("git POSTs, DO routes") in `src/edge/mod.rs`. Every DO route would import `edge`. Fix: move `RepoHeaders`, the refs DTO and `finish` into `error.rs`/`wire`/`store`; zero wire change.
- Minor: `upload::upload_pack(req, &stub, &repo)` receives no `ReqBudget`, so the fetch stub call is uncharged at the edge unless protocol-v2-only charges it internally (7.1 says every stub call is charged; one budget per request).

## First-pass blockers
The first pass listed no blockers, six caveats:
1. Over-advertised caps: resolved. Line `assert_eq!(w.out, b"001e# service=git-upload-pack\n0000000eversion 2\n...0019fetch=shallow filter\n0017object-format=sha1\n0000")` pins v2; the `ends_with(... capabilities^{}\0report-status report-status-v2 delete-refs side-band-64k quiet ofs-delta object-format=sha1 agent=git-edge/0.1\n0000")` pins rule 5 (no `atomic`, no `push-options`).
2. `version=1`: resolved in design only. `Service::UploadPack { v1: matches!(proto, Proto::V1) }` is here; the `version 1\n` emission lives in protocol-v2-only's `write_advertisement_v0` and no test in this proof pins the bytes. Scenario 19 would catch it.
3. stub/writer errors: resolved. `let mut resp = repo.stub(env)?.fetch_with_request(fwd).await?;` maps eviction to `Error::Storage` -> 500; `Response::from_bytes(w.out)` leaves nothing to abort.
4. Decoder `0003`/`-001`: resolved. `PktReader::next` is `gix_packetline::decode::streaming`; `pkt_tests::rule_1` asserts `Err(Error::Protocol(_))` for `0003`, `-001`, `ffff` and `Ok(false)` for the overrun `0009abc`; `respond` maps it to 400.
5. Auth before stub: resolved. `let who = auth::authenticate(&req, &env)?;` is the first statement of the route body.
6. Body cap by zone plan: resolved (text and section 7 table).

## Crash walk-through
`git push` GETs `info/refs?service=git-receive-pack`. Edge: `authenticate` ok, `charge(1)`, builds the stub request, awaits `fetch_with_request`. The DO is evicted between `boot` and the response: the promise rejects, `?` yields `Error::Storage`, `finish` logs and returns 500 `internal error\n`; git prints "The requested URL returned error: 500" and exits; the retry gets a fresh `list_refs` snapshot. If the edge isolate dies after the DO returned, the client sees a reset before or during the single body write; git reports early EOF. Nothing was written anywhere in either case; there is no orphan because this path has no state. If `boot` created `meta` for a brand-new name and then the DO died, those rows committed in their own sync span, which is the intended implicit-create behaviour of 8.2.

## Concurrency walk-through
Clone A (v0, `/_do/refs`) and push B (`commit_push`) hit the same DO. `GET /_do/refs` has "Awaits inside: none": `boot` + `list_refs` are one sync span, and B's step 1-7 is one sync span (section 3); the platform cannot deliver A inside B or B inside A, so A sees all of B's ref moves or none, never a torn list. A then POSTs receive-pack with the old oids it saw; if B won, A's CAS returns `changes()==0` -> `ng refs/heads/main failed to update ref`, git prints "fetch first". Two v0 clients both take the v2-free path and serialize on the DO (throughput only). v2 clients never touch the DO on this route at all, so the static advertisement has no concurrency surface.

## Interop check
- v2: `# service=git-upload-pack\n` + flush + `version 2` ... + flush with `application/x-git-upload-pack-advertisement` is exactly what `remote-curl.c:discover_refs` then `discover_version` need; spike-verified against git 2.43; a 2.4x client proceeds to `command=ls-refs`. No byte breaks.
- v0 receive-pack: `# service=git-receive-pack\n`, flush, first line with `\0caps`, flush; empty repo `<40 zeros> capabilities^{}\0<caps>\n`: what `send-pack` expects. `Git-Protocol: version=2` on this GET is correctly ignored.
- One wire line that misbehaves: the proof lists `HEAD` first in the **receive-pack** advertisement. Real `receive-pack` (`write_head_info`, `for_each_ref`) never sends `HEAD`. `git push --mirror` derives deletions from remote refs absent locally (`get_local_heads` excludes HEAD), so it sends `<oid> 0{40} HEAD` and gets `ng HEAD`; the push exits non-zero. Drop HEAD for `Service::ReceivePack`.
- Error bodies: `respond_text` sets no `Content-Type`; git's `show_http_message` prints a 4xx body as `remote: ...` only for `text/plain`, so the reason is lost. For POST 400s, git's `post_rpc` runs with `CURLOPT_FAILONERROR`, so the `ERR` pkt is likely never read: the user sees "RPC failed; HTTP 400 curl 22", not "protocol v2 required". Not a hang (scenario 13 still passes on exit code) but the contract's stated message will not appear; git's own servers answer 200 + `ERR` pkt for in-band protocol errors.
- `0004` rejected by gix-packetline (git accepts an empty line): no 2.4x client sends it on this route.

## Blockers
1. Dependency direction (section 1): `RepoHeaders`, `RefsDto`/`refs_json`, `respond`/`finish` live in `edge` but are consumed by `repo_do`. Move them out of `edge` before any DO route compiles against them.

## Caveats
- Stub request path (`RequestInit`, `new_with_init`, `headers_mut`, `Uint8Array` body) unrun; day-1 test as the proof names, fallback is `Request::new` + `Headers` passed to `with_headers`.
- Add `impl From<Error> for worker::Error` (or handle `w.text` without `?`) in `finish`; add `serde`, `serde_json`, `js-sys` to the manifest.
- Remove `HEAD` from the receive-pack advertisement; keep it (rule 7) for v0 upload-pack.
- Set `Content-Type: text/plain; charset=utf-8` on `info/refs` error bodies; measure what git 2.43 prints for a 400 + `ERR` pkt on a POST and, if the message is swallowed, take the 200 + `ERR` question to the contract (section 10, scenario 13).
- `version 1` bytes need a pinned test in `wire` (none here; scenario 19 is the only check).
- `ReqBudget` is not passed to `upload_pack`; either pass it or have protocol-v2-only state that it charges its own stub call and emits `x-ge-subrequests`.
- Section 8.2 implicit creation means any read-token holder creates a DO (plus a recurring Janitor alarm) per probed `owner/repo` on the v0 path; the v2 GET avoids the DO, `ls-refs` does not. Contract-level, worth a cap or an allowlist later.
- Non-UTF-8 ref names fail through the JSON DTO (documented; consistent with `refs.name TEXT`).

## Verdict
lands-with-caveats. The wire bytes for both handshakes are right and pinned, the read-only path has no reliability surface, and every first-pass caveat is closed by code except the `version 1` line, which is delegated. The one thing that must change before the DO side can be written is the placement of the three shared items in `edge`; it is a file move, not a redesign. 2-3 days including the day-1 stub-request test.
