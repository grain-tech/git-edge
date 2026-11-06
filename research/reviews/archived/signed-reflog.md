# Review: Signed refs by default with append-only DO reflog

> Idea #19 · edge · verdict: **lands with caveats** · feasibility 4/5 · reliability 2/5 · correctness 3/5 · effort: weeks
> Proof: [proofs/signed-reflog.md](../../proofs/archived/signed-reflog.md) · Review: [reviews/signed-reflog.md](./signed-reflog.md)

# Review: signed-reflog (idea #19)

## Scores
- Feasibility 4/5. Every primitive is GA: DO SQLite (`sql.exec` multi-statement DDL, AUTOINCREMENT, triggers, `transactionSync`), alarms, DO RPC, R2 `put`, WebCrypto `Ed25519` + `SHA-256` (Workers has shipped Ed25519 in `crypto.subtle` since 2023). Nothing large enters the DO (SHAs, refnames, a <8 KB cert), so 128 MB and single-threading are respected. Two limit-shaped nits: `reflog()` returns the whole chain through an RPC (32 MB RPC/response ceiling; a busy repo blows it without `seq` pagination) and `nonce()` does a storage write on every `info/refs?service=git-receive-pack` GET.
- Reliability 2/5. The refs CAS and the head read happen before `await crypto.subtle.*`, outside `transactionSync`; documented DO input-gate semantics only hold the gate across storage awaits, so two concurrent pushes can both pass CAS on stale reads and both commit (lost update + forked chain). Details below. Everything else (atomic ref+log insert, idempotent anchor, alarm re-arm) is sound.
- Correctness 3/5. The server-signed, hash-chained, tamper-evident reflog is real and verifiable offline. But the `git push --signed` path is described with the wrong wire shape (see Interop), `verifyPushCert` is a declaration, and "signed refs by default" is really "server-attested ref transitions"; the client-attested half only exists for ssh-ed25519 users once someone writes the parser.
- Effort: weeks. Server-signed reflog + anchor + `/reflog` endpoint is 2-3 days on top of #1/#6. Correct push-cert pkt-line parsing, SSHSIG (ssh-ed25519, then RSA/ECDSA) verification, HMAC nonce with slop, pagination, key-id column: another 1-2 weeks with a real-git test matrix.

## Crash walk-through
Push `main A->B`, pack already in R2 (#6). (1) Crash inside `transactionSync`: SQLite rolls back, refs and reflog stay consistent, pack objects are #6's orphans. No dangling ref. (2) Crash after commit, before `setAlarm`/return: ref moved, signed row exists, client sees a transport error with no `report-status`; a retry gets `ng main fetch first`, re-fetch shows B. No loss; only the R2 anchor is delayed until the next push arms the alarm, widening the "wipe is undetectable" window from 60 s to indefinite for a repo that never gets pushed again. Fix: `setAlarm` inside the same `transactionSync` (storage ops in a sync transaction are atomic with it). (3) Crash in `alarm()` after `put`: the alarm retries and rewrites the same `anchor/<seq>.json` idempotently. (4) DO storage lost/reset: constructor regenerates a fresh keypair silently, so the new chain has a different public key and `prev=0^64`; the R2 anchors are the only evidence. Acceptable given the stated limits, but the proof should refuse to mint a second key when an anchor already exists in R2.

## Concurrency walk-through
Pushes P1 (`main A->B`) and P2 (`main A->C`) arrive together. P1 reads head `H0`, reads `cur=A`, then `await digest/sign` - a non-storage await, so the input gate is open and P2's `updateRefs` runs: it also reads `H0` and `cur=A`, passes CAS, awaits signing. P1's `transactionSync` commits `main=B`, row(seq 1, prev H0). P2's `transactionSync` commits `main=C`, row(seq 2, prev H0). Result: P2 silently overwrote B without a `fetch first` (violates the #1 CAS contract the whole design rests on), and the log is a fork (two rows with `prev_hash=H0`) that `verifyEntry` accepts row-by-row. The proof's comment "Single DO => no interleaving" is exactly wrong for this code. Whether workerd happens to resolve `crypto.subtle` promises without yielding is undocumented and must not be relied on. Fixes (small): re-read head and every `cur` inside `transactionSync` and throw/retry on mismatch; add `UNIQUE(prev_hash)` so a fork is a constraint error rather than a silent divergence; or serialize `updateRefs` behind an in-object promise mutex. Same race applies to `nonce()` vs. `updateRefs` (already acknowledged).

## Interop check
1. Unsigned push: fine. `git push` uses v0 receive-pack even with `protocol.version=2` (v2 has no push), and the CAS + `ng <ref> <reason>` semantics match.
2. Signed push wire shape is wrong. When the server advertises `push-cert=<nonce>`, `send-pack.c` does not send `<old> <new> <ref>\0caps` lines at all: the first pkt-line is `push-cert\0report-status side-band-64k ...`, followed by the cert lines (`certificate version 0.1`, `pusher`, `pushee`, `nonce`, blank, one `<old> <new> <ref>` per line, then the armored signature), then `push-cert-end`, then shallow/pack. Commands must be parsed from the cert body (git's `queue_commands_from_cert`). A parser expecting a command line first, as the Mechanism describes, sees "push-cert" as a malformed command and rejects every signed push - the exact case this idea exists for.
3. Nonce: git treats a mismatch as a policy input (`GIT_PUSH_CERT_NONCE_STATUS` = BAD/SLOP/UNSOLICITED), not an automatic reject; rejecting all refs is a legitimate stricter policy, but the `nonce <n>` line must be compared to what *this* advertisement issued, which the single `meta` row cannot guarantee under concurrent advertisements.
4. SSHSIG: the signed blob is `SSHSIG` || namespace `git` || reserved || `sha512` || SHA-512(cert body up to but excluding the signature); the proof's sketch matches. GPG certs are stored unverified, as admitted.

## Blockers
- CAS and head read outside `transactionSync` across non-storage awaits: lost update on `refs` and forked hash chain under concurrent pushes.
- Signed-push command parsing: commands live inside the `push-cert` block, not in a leading command line; as described, `--signed` pushes are rejected.

## Caveats
- `verifyPushCert` is undefined; ship without it and `--signed` is stored-but-unverified (nonce only), which must not be advertised as "verified".
- Key lives beside the log; append-only is against app code only; anchors in a non-versioned bucket are overwritable. All acknowledged, but the security claim in the title outruns the proof.
- Alarm arming outside the transaction; `setAlarm` only after `rows.length` means a repo whose last push crashed pre-arm is never anchored.
- `/reflog` unpaginated; `nonce()` writes on every advertisement; no key-id column for rotation.

## Verdict
lands-with-caveats. The mechanism (server-signed, hash-chained reflog in the same transaction as the ref CAS, anchored to R2) is sound and cheap on GA primitives, but the proof code has a concrete concurrency bug that breaks the CAS guarantee it claims, and its `--signed` wire description would fail against real git. Both fixes are days, not a redesign.
