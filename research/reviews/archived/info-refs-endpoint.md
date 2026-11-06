# Review: The /info/refs?service= entrypoint and pkt-line codec

> Idea #53 · foundation · verdict: **lands** · feasibility 5/5 · reliability 5/5 · correctness 4/5 · effort: days
> Proof: [proofs/info-refs-endpoint.md](../../proofs/archived/info-refs-endpoint.md) · Review: [reviews/info-refs-endpoint.md](./info-refs-endpoint.md)

# Review: info-refs-endpoint (idea #53)

## Scores
- Feasibility 5/5. Everything used is GA and boring: Workers `fetch`, DO SQLite `ctx.storage.sql.exec`, `TransformStream`/`ReadableStream` responses, `DecompressionStream("gzip")` (workerd supports gzip/deflate/deflate-raw), streaming request bodies into `stub.fetch`. No alarms, R2, KV, Queues, WebSockets. CPU is trivial per request; nothing is buffered in memory except the ref rows (fine up to tens of thousands of refs, as the proof says). One factual slip: the request-body cap is by zone plan (Free/Pro 100 MB, Business 200 MB, Enterprise 500 MB), not "paid Workers = 500 MB".
- Reliability 5/5. This idea is read-only. The refs snapshot is two synchronous SQL reads inside one DO event, so the advertisement is a consistent point-in-time view; nothing is written, so there is no data-loss, orphan, or split-brain path to open. Truncated streams are detected by git and simply retried by the user.
- Correctness 4/5. A real `git` 2.4x client interoperates for both v0 and v2 handshakes (details below). Deductions: the capability list advertises features the follow-up POST handlers are not yet committed to (`filter`, `shallow`, `atomic`, `push-options`, `wait-for-done`, `report-status-v2`); once advertised, git will use them, so they are contractual. `version=1` is answered as v0 (proof admits this; the client tolerates it because v1 is v0 plus a `version 1` line, and remote-curl only checks that line if present).
- Effort: days. The codec plus handshake is ~150 lines and can be verified end to end with `git ls-remote` and `GIT_TRACE_PACKET=1` in an afternoon; the POST handlers are separate ideas.

## Crash walk-through
`git clone` issues `GET /o/r.git/info/refs?service=git-upload-pack`; the DO reads `refs` and `meta.HEAD`, returns the streaming `Response`, and the async writer is mid-way through 20k ref lines when the DO is evicted or the isolate is reset. The client sees a body that ends without the trailing `0000`; remote-curl reports "fatal: the remote end hung up unexpectedly" / "early EOF" and exits non-zero. Nothing was written, nothing is left behind, and the retry reads a fresh snapshot. If the eviction happens before the `Response` is returned, `stub.fetch` throws and the Worker should map it to 502 (proof does not catch; it would surface as a 1101 error page, which git also handles as a failed fetch). The one thing worth adding is a `try/finally` around the writer so a consumer that disconnects (`git` killed mid-clone) aborts the writable instead of leaving the IIFE writing into a closed pipe and logging an unhandled rejection.

## Concurrency walk-through
Clone A's advertisement and push B's ref CAS arrive at the same repo DO. The DO runs one event at a time; A's two `SELECT`s are synchronous, so A sees either all of B's ref updates or none, never a torn list. Then A POSTs `want <oid>`: by then B may have moved `main`, but the wanted oid still exists (objects are never deleted while reachable), so the fetch succeeds against the old tip; this is standard git stateless-RPC semantics, not a defect. Two concurrent pushes both see the same advertisement; the second's `old-oid` CAS fails in receive-pack (idea repo-do-ref-authority) with `ng refs/heads/main failed to update ref` and git prints "fetch first". No advertisement-side state is involved, so no split brain can originate here. Many concurrent clones of a huge-ref repo serialize on the single DO (throughput, not correctness).

## Interop check
- `# service=git-upload-pack\n` pkt + `0000` before the payload, and `content-type: application/x-git-upload-pack-advertisement`: exactly what `remote-curl.c:discover_refs` requires to take the smart path. Correct.
- v2: `version 2` then capability lines then `0000`, no refs; `ls-refs=unborn`, `fetch=shallow filter wait-for-done`, `object-format=sha1`, `agent`. Matches git's own `serve.c` output; 2.4x clients then POST `command=ls-refs` with `0001` delim. Correct. `Git-Protocol: version=2` is also sent on the POSTs and the Worker re-parses it there. Correct.
- v2 requested for `git-receive-pack` falls back to v0: correct, git has no v2 push and the client expects exactly this.
- v0 first-line `<oid> HEAD\0<caps>\n` with `symref=HEAD:refs/heads/main`, HEAD first then sorted refs, `0000...0 capabilities^{}` for empty repos: correct. `pkt()` measures encoded bytes so the NUL and `\n` are counted. Correct. Payload cap 65516 (65520 total) matches `LARGE_PACKET_MAX`.
- Advertising HEAD and `symref=` for receive-pack is more than real receive-pack does; harmless to `send-pack`.
- Decoder: `0003` is mapped to flush (git treats it as a protocol error); `parseInt` accepts `"-001"`/`"12 3"` silently (index `-1` yields `type: undefined`). Fuzz-level robustness only; real clients never send these, but a malformed body should 400 rather than propagate `undefined`.
- gzip: git sets `Content-Encoding: gzip` only when the POST body exceeds `http.postBuffer` (1 MiB) and switches to chunked; Workers pass both through untouched, `DecompressionStream` handles it. Correct. `Expect:` is suppressed by git so no 100-continue interplay.
- Missing `no-done` in v0 upload-pack caps costs one extra round-trip per fetch with `multi_ack_detailed` over stateless HTTP; not a break.
- Would break nothing at the wire level for git 2.4x. The only thing that will bite is downstream: once `filter` is advertised, `git clone --filter=blob:none` is accepted by the client and the DO must honor `filter blob:none` in the fetch command or the clone fails after negotiation.

## Blockers
None for this idea in isolation. It is a foundation piece and delivers exactly what it claims.

## Caveats
- Capability list is a promise: trim `filter`, `shallow`, `wait-for-done`, `atomic`, `push-options`, `report-status-v2` until the corresponding POST handlers exist, or clients will exercise them.
- Handle `version=1` by emitting `version 1\n` before the v0 body; three lines of code.
- Catch `stub.fetch` failures and writer errors (client disconnect) so eviction mid-stream returns a clean 502 rather than an unhandled rejection.
- Auth (401 + `WWW-Authenticate`) must sit in the Worker before `stub.fetch` on the GET; git only retries with credentials on the `info/refs` response.
- Body-size figure in "Known limits" is wrong for Workers Paid; the cap follows the zone plan.

## Verdict
lands. All-GA primitives, read-only path with no reliability surface, and the wire shapes for both protocol v0 and v2 handshakes match what git 2.4x's `remote-curl` and `fetch-pack` actually check. Fix the over-advertised capabilities and the error handling and this is a day or two of work.
