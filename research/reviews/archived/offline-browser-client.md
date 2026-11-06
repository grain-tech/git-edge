# Review: Offline-first browser client with OPFS and the same Wasm core

> Idea #52 · wild · verdict: **risky** · feasibility 3/5 · reliability 3/5 · correctness 2/5 · effort: months
> Proof: [proofs/offline-browser-client.md](../../proofs/archived/offline-browser-client.md) · Review: [reviews/offline-browser-client.md](./offline-browser-client.md)

# Review: offline-browser-client (#52, wild)

## Scores
- Feasibility: 3/5. Every Cloudflare primitive named is GA (DO SQLite `sql.exec`, R2 get/put/head, Workers Static Assets, Wasm, `DecompressionStream`); browser APIs (OPFS sync handles in a Web Worker, IndexedDB) are GA in all three engines. Not respected: the 1000-subrequest cap per DO invocation, which the `put`-per-object R2 store blows on any push >1000 objects; `RepoDO.get` buffers whole objects into a 128MB isolate; `sql.exec(...).one()` throws on zero rows, so `ref()` for a missing ref crashes instead of returning null.
- Reliability: 3/5. Ref CAS is sound on both hosts; no split-brain. Loose-object writes in OPFS are not atomic (see crash walk).
- Correctness: 2/5. It is a transport + object store, not an offline git client; and the push path as written cannot ever report success (see interop).
- Effort: months to the stated goal; ~3 weeks for fetch/push-only sync once `wasm-git-core` and `streaming-pack-parser` exist.

## Crash walk-through
Browser tab is killed mid-`readPack` during a fetch. `OpfsStore.put` does `getFileHandle(oid, {create:true})` then `createSyncAccessHandle().write()`: a crash between file creation and `flush()` leaves a truncated file at its final `objects/xx/yyyy` name. `has()` is existence-only, so the next `writeThinPack` will exclude that oid as "already have", and the next delta resolve reads garbage. No temp-then-rename, no hash-on-read. The remote-tracking ref is untouched (refs update only after the pack completes), so no ref corruption, but the object store is silently poisoned. Fix: write to `objects/tmp/<rand>` and `FileSystemFileHandle.move()` (Chromium; Safari lacks `move`, needs read-verify-on-open) or verify sha1 in `get()`. Server side: crash after R2 puts but before CAS orphans content-addressed objects in R2 (GC problem only, not correctness), consistent with `two-phase-push`. Client crash after the server's `ok` but before the local `cas(refs/remotes/origin/..)` leaves a stale tracking ref; the next push sends the wrong `oldOid`, gets `ng` for what is really a fast-forward, and the proof's "fetch, rebase, retry" path recovers it. Annoying, not lossy.

## Concurrency walk-through
Two browsers (or two tabs) push different commits on `refs/heads/main` from the same base. Both upload thin packs; the DO interleaves the `await BUCKET.put` calls but each `cas` is one synchronous `UPDATE ... WHERE oid IS ?`, so exactly one wins and the other gets `ng refs/heads/main non-fast-forward`. Loser's objects remain in R2 as unreferenced but valid content-addressed blobs. Correct. Two tabs on one origin writing OPFS: `createSyncAccessHandle` takes an exclusive per-file lock, so a same-oid concurrent `put` throws `NoModificationAllowedError`; the proof does not catch it (should treat as success since content is identical). IndexedDB `readwrite` serializes `cas` across tabs, but the `cas` body awaits `req(s.get())` inside the transaction; that only works if `req` resolves from the IDB `onsuccess` event and nothing else is awaited, otherwise the transaction auto-commits and `s.put` throws `TransactionInactiveError`. Fragile but workable.

## Interop check
The server is unchanged, so C git 2.4x interoperates with the server by construction. The question is whether the browser client speaks correct bytes:
1. Push response is requested with `side-band-64k`, so report-status arrives as band-1 frames (`LLLL\x01` + `000dunpack ok\n0019ok refs/heads/main\n`). The proof does `res.text()` and tests `/^ok /m`; every `ok` line is preceded by a 4-hex pkt length, not a newline, so the regex never matches and every successful push is treated as a failure (tracking ref never advances). Either drop `side-band-64k` from the push caps or demux.
2. v2 fetch: the client never issues `command=ls-refs`; `info/refs` in v2 returns only `version 2` + capabilities, not refs, so `want` has nothing to be populated from.
3. `pkt()` uses `s.length` (UTF-16 units) for the length prefix; any non-ASCII ref name yields a malformed pkt-line.
4. Missing `ofs-delta`/`thin-pack`/`no-progress` fetch args is legal but means ref-delta-only, non-thin packs and band-2 progress noise.
5. `Content-Type: application/x-git-upload-pack-request` on the `info/refs` GET is harmless. CORS handling (`OPTIONS` echoing `Git-Protocol`) is correctly identified as browser-only.
6. receive-pack does accept thin packs unconditionally (`index-pack --fix-thin` semantics), so the thin-push premise holds provided the DO's `two-phase-push` resolves ref-deltas against R2.

## Blockers
- Push success detection is broken as written (sideband on report-status not demuxed).
- No `ls-refs` step, so a fetch cannot discover what to `want`.
- 1000-subrequest cap: per-object R2 `put`/`head` inside one DO request fails for pushes >~1000 objects; needs the packed/indexed store from `content-addressed-r2-keys`.
- Non-atomic OPFS object writes poison the local store on crash.

## Caveats
- "Offline-first client" is really "offline commit + sync"; checkout, index, merge, rebase are deferred to `wasm-git-core`, and the ng-recovery path needs rebase, so the demo is not usable until that dependency is complete.
- `DecompressionStream` cannot inflate pack entries (no bytes-consumed reporting); a Wasm inflater is mandatory, as the proof concedes.
- Safari evicts OPFS (7-day ITP, smaller quota); local store must be treated as cache. Background Sync is Chromium-only.
- `sql.exec(...).one()` throws on no rows; use `.toArray()[0]`.
- Same Wasm code, different envelopes: 128MB / 30s CPU on the server vs unbounded in the tab; server-side delta resolution must stream.
- isomorphic-git already demonstrates browser git over smart HTTP; the novelty is only the shared core, which lowers risk but also the payoff.

## Verdict
risky. The mechanism is well-trodden and the Cloudflare side is plain, but the proof code has two wire-level bugs that make it non-functional as shown, ignores the subrequest cap, and the "client" is ~10% of a client. Months to the stated goal.
