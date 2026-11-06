# Second-pass review: Auth and multi-tenancy: owner/repo routing to DO ids

# Review v2: auth-and-multitenancy (idea #54)

## Scores
- Feasibility 5/5 (was 5). Everything on the request path is a verified binding: `#[event(fetch)]`, `Request::{path,url,method,headers}`, `id_from_name().get_stub()`, `Stub::fetch_with_request`, sync `SqlStorage::exec` + `SELECT changes()`, `Response::from_bytes(..).with_headers` (spike line 284). Unverified but low-risk: `Env::secret` Display, `base64` 0.22 on wasm32, the `crypto` global path, `ObjectId -> JsValue` (marked PSEUDO).
- Reliability 4/5 (was 4). `boot` is one sync span, idempotent (`ON CONFLICT DO NOTHING`), R2 never touched, so crash and retry are clean. Deductions: a `dead` Janitor is never re-enqueued (4.5), and the pre-auth drain is justified by a misread log line.
- Correctness 4/5 (was 4). Same score, but the four first-pass interop findings are all closed by code; what remains is two mechanical contract slips, one undeclared 409 path, and missing imports in `boot.rs`. Wire behaviour toward git 2.43-2.47 is right.
- Effort: days. ~150 lines that compile against section 1 signatures once `BodyReader`, `jobs::enqueue`, `store::schema::migrate` and the sibling `edge::{upload,receive}` modules exist.

## Contract compliance
1. **Violation (8.3)**: `hex16()` in `src/repo_do/boot.rs` calls `js_sys::Reflect::get(&js_sys::global(), &"crypto".into())`. Section 8.3 says CI greps `repo_do` for `Reflect::get` and allows exactly one occurrence, the `ctx.id.name` cross-check. This is a second one in the same file; CI fails. Move `hex16` to `store` (or reach `crypto` via `web_sys::WorkerGlobalScope`).
2. **Violation (4.5)**: "Janitor (recurring, self-enqueued at boot if absent)". The proof calls `jobs::enqueue(&sql, JobKind::Janitor, ..)` only in the `None => { .. }` create branch. After 8 failed slices the row is `dead`, `enqueue` dedups only on `queued`/`running`, so nothing ever re-creates it. Fix: call `enqueue` on every `boot` (dedup makes it one SELECT).
3. **Extension, declared**: `Err(Error::Conflict(m)) => (409, m)` is not in section 10's table; the proof says so and asks for write-back. Not a contradiction, but it interacts with the section 10 rule "receive-pack after the header was parsed: HTTP 200 with `unpack <message>`": `respond` cannot know the header was parsed, so a `Conflict` from `/_do/push/commit` (push expired during a >1 h push) that `edge::receive` lets escape becomes a 409 and git prints `RPC failed; HTTP 409`. Responsibility sits in two-phase-push, but `respond` gives it no help.
4. Everything else matches: `authenticate(req,&Request, env:&Env) -> Result<Principal,Error>` is `fn` (1.4); two-token Basic, anonymous refused (12); `RepoRoute` regex `[A-Za-z0-9._-]{1,64}`, `.git` stripped, no lowercasing, `x-ge-owner`/`x-ge-repo` only (8.1); `meta` rows exactly the eight of section 8; `new` touches no storage, `boot` at the start of fetch/alarm, headers-vs-meta mismatch is `Internal` 500, alarm skips the compare (8.2); no `rows_written`, no `set_alarm` outside `enqueue`, no await in `boot`; 401 carries `WWW-Authenticate: Basic realm="git-edge"`, POST errors are one `ERR <msg>\n` pkt-line, `info/refs` errors plain text, 500 message goes to `console_log!` only (10).

## First-pass blockers
The first pass had none. Its caveats and interop items, checked against the code:
- Existence leak: **resolved**. `route` computes `pre` (parse -> `op_of` -> `authenticate` -> `can_write`) and only then `let stub = match repo.stub(&env)`. Anonymous gets 404 on a malformed path or 401 on any well-formed one; no DO is woken.
- DO-name drift between proofs: **resolved in scope**. `RepoRoute::parse`/`name()` is the only parser and name former; siblings are claimed to call `internal_request`, which I cannot verify here.
- `jurisdiction()`: **resolved by removal**; the contract has none.
- Schema in the constructor: **resolved**. `new` stores `state`/`env`; `migrate` runs in `boot` behind `booted` after the edge authenticated. Admitted residue: a read-token `ls-remote a/b` still materialises a DO, `meta` and a Janitor row.
- `stub.fetch` rejection -> 500: **resolved** via `impl From<worker::Error> for Error` (-> `Storage`) and the `respond` arm.
- Username coupling: **resolved**. `authenticate` compares only `pass`; the username is a sanitised label (`[A-Za-z0-9._-]{0,64}`, else `-`).
- Pack-rewind claim: **corrected**; `probe_rpc` is described right and the flush-only POST requirement is handed to repo-do-ref-authority.
- `Bearer` refused: **documented**. Malformed `Authorization` -> 401: **resolved** (`decode(..).map_err(|_| Error::Auth)`, `split_at_checked`, `get(1..)`).

## Crash walk-through
First authenticated `git ls-remote` for a name that has no DO. Edge: `authenticate` ok, `stub()`, `GET /_do/refs`. DO: `boot` migrates, `SELECT * FROM meta` empty, `hex16()`, eight `INSERT .. ON CONFLICT DO NOTHING`, `enqueue(Janitor)`, then `list_refs` and the JSON reply, all one sync span. Evicted before the span ends: nothing is durable, the stub call rejects, `respond` returns 500, git prints an error; the retry runs `boot` again with a fresh random `repo_id`, which is fine because no R2 key and no other row ever saw the first one. Evicted after commit, before the reply: retry reads `repo_id` from `meta`, identity stable. Open question the proof inherits from the contract: `enqueue` is `fn` but `Storage::set_alarm` is async in `worker` 0.8.5, so `rearm` must either `spawn_local` the promise or defer it to the handler's tail; if the DO dies between the SQL commit and that promise, a `queued` Janitor row exists with no alarm, and only an `enqueue`-on-every-boot (fix for violation 2) re-arms it.

## Concurrency walk-through
Two writers in different colos push to brand-new `a/b` at once. Both edges verify the write token statelessly, both `id_from_name("a/b")` resolve to one DO. The DO serialises: the first `/_do/push/begin` runs `boot` and creates `meta`; the second is delivered only after the first span ends (no await inside `boot`, `booted` borrows never straddle an await), sees `repo_id`, and both `begin` replies carry the same `repo_id`, so both `pending/` keys land under one `r/<repo_id>/`. The later CAS in section 3 decides the ref race; this module contributes no lost update. Secret rotation between a client's `info/refs` and its POST: the POST gets 401, git reports `Authentication failed` for that push only; the proof states point-in-time auth. Case: `Foo/bar` and `foo/bar` are two DOs by contract; no split-brain.

## Interop check
git 2.43-2.47, protocol v2: `GET /o/r/info/refs?service=git-upload-pack` anonymous -> 401 + `WWW-Authenticate: Basic realm="git-edge"` -> `http.c` `credential_fill`, one retry with `Authorization: Basic` -> 200 from the sibling module. Second 401 -> `credential_reject`, `fatal: Authentication failed`. Read token on `?service=git-receive-pack` -> 403, terminal, no prompt loop, helper credential kept. POSTs reuse the credential, so the `probe_rpc` flush POST and the chunked pack never see a challenge. git sets `CURLOPT_FAILONERROR`, so the `ERR ..` pkt-line body on a 4xx POST is never read; harmless. `Basic ` prefix match is case-sensitive; curl sends exactly that. No wire byte produced by this module breaks a stock client. The one byte-level risk is inherited: a `Conflict` escaping `edge::receive` yields status `409` where git expects `200` + `unpack ...` (see compliance item 3).

## Blockers
1. `Reflect::get` twice in `repo_do` (`hex16` and the `ctx.id.name` check): violates 8.3's single-occurrence rule; CI grep fails. Minutes to move.
2. Janitor is enqueued only on creation, not "at boot if absent" (4.5): a dead Janitor never returns and pending R2 scratch is never deleted. One `enqueue` call in `boot`.

## Caveats
- The drain rationale is wrong: `spikes/rust-ls-refs/dev.log` line 76 says `Can't read from request stream after response has been sent`, i.e. the spike read the body *after* responding, not "workerd restarted on an unread body". The 1 MiB drain is harmless (drain then respond) but unnecessary; drop it or re-measure.
- `boot.rs` does not import `worker::Request`, `Error`, `console_log`, `serde`; `self.q`/`self.sql` are helpers outside the contract's `RepoDo` and must exist. Compile-level, not design-level.
- `auth` parses client bytes but is not in section 10's `#![deny(clippy::..)]` list (wire, store::codec, pack, edge); add it.
- `Conflict -> 409` needs writing into section 10, and `edge::receive` must convert a post-header `Conflict` to 200 + `unpack`.
- Unverified at runtime: `Env::secret` Display, `base64` on wasm32, `crypto` via `js_sys::global()`, `ObjectId -> JsValue`. A missing secret binding surfaces as 500, not a deploy-time error.
- A read-token holder creates repos by naming them (admitted; wave-1 `create`/exists gate).
- `ct_eq` iterates `max(len)` so the longer length leaks; with the 1 KiB cap this is noise.

## Verdict
lands-with-caveats. Genuinely better than the first pass: every first-pass interop and caveat item is closed by a line I can point at, the two-token model removes the HMAC and ACL surface entirely, and the routing order makes the existence leak structurally impossible. The two blockers are one-line contract slips (a second `Reflect::get` in `repo_do`, Janitor not re-enqueued at boot), fixable in an hour; the rest is compile hygiene and one status-code write-back. Days.
