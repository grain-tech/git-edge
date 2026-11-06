# The second pass in plain words

This page says what the second pass found, in plain language. Read [how-to-read.md](how-to-read.md) first if the verdict words are new to you.

## What the second pass was

The first pass wrote 56 proofs in TypeScript. A proof is a small piece of code that shows how an idea could work. A reviewer then tried to break each proof.

The second pass asked a harder question. Can these ideas share one design? So we wrote one rulebook, called the contract, in [CONTRACTS.md](../CONTRACTS.md). The contract says how objects are stored, how a branch moves, how the one timer is shared, and how errors reach the user. Then 33 ideas were written again, this time in Rust, a language that compiles to Wasm, the format Cloudflare Workers run. Wasm, or WebAssembly, is a way to run code from other languages inside a Worker. Each new proof was reviewed again by a fresh reviewer who was told to doubt everything.

The 33 are the foundation and edge ideas. The foundation is the smallest working server. The edge ideas are the ones a real service needs next. The 23 wild ideas were not rewritten.

## The scores

Across the 33 ideas, the three scores moved like this.

| Score | First pass | Second pass |
|---|---|---|
| Feasibility | 4.0 | 4.0 |
| Reliability | 3.0 | 3.7 |
| Correctness | 2.9 | 3.6 |

The verdicts moved like this. Eight ideas moved up from risky to lands with caveats: branch-level Durable Objects, server-side merge, server-side rebase, hooks as Workers, presigned upload, the Wasm git core, the diff API, and the cleanup job. Four ideas moved down: the precomputed clone pack and bundle-URI went from lands with caveats to risky, the want and have negotiation went from lands with caveats to risky, and the entrypoint went from lands to lands with caveats.

A move down is not always bad news. Most of the moves down happened because the reviewers now hold each proof to the contract. The contract made problems visible that the first pass could not see, such as two proofs that cannot compile together because their function shapes disagree.

## What the second pass proved

The main idea holds under the contract. One Durable Object per repo, moving a branch only when the branch still has the value it expects, does what a git server does. Every first-pass blocker on the ref authority, the pack parser, and the two-phase push is closed by contract-bound code. The protocol v2 idea moved from risky to lands with caveats, because the wire format is now one shared module with byte-exact rules.

Two first-pass findings turned out to be design bugs, not idea bugs. The cleanup race that could delete live objects is gone by shape, not by a check. Each push and each pack now writes under its own fresh name, so there is no shared name for a cleanup task to hit. The alarm collisions are gone the same way. There is one timer and one job table, and every background task is a job on it.

## What the second pass found

The new reviews found 86 problems that must be fixed first. Most are small and mechanical, but four patterns repeat across many files, and they are the real second-pass findings.

1. **The proofs do not compile together yet.** Each proof was written alone, and the shared function shapes drifted. One helper is called with six arguments in one proof and seven in the next. One type is read in a shape a sibling never wrote. None of this is deep. It does mean the crate cannot be built until one shape wins for each shared function, and the contract must record the winners.
2. **A killed job can get stuck running forever.** A job row is marked running before its slice of work. If the program dies inside a slice, the row stays running. The deduplication rule then refuses to enqueue the job again, and the boot repair only revives dead rows. Many edge jobs depend on this path. The fix is a rule that takes back a running row after a time limit, and it belongs in the contract.
3. **Errors after the push header must answer 200, and nothing does that yet.** git ignores the body of any answer with status 300 or more. The contract says every post-header error becomes a 200 report with an ng line per ref. Several proofs raise limit, budget, and conflict errors that today reach the client as 413 or 500. One error arm at the edge fixes all of them.
4. **Nobody calls rearm after enqueue.** A job enqueue only writes a row. The one alarm must then be set for the earliest job. The proofs assume the main request handler does this after every sync span. It does not yet. Until it does, enqueued jobs wait for the next alarm or the next request.

## What the contract now owes

Ten amendments were added to the contract during the foundation pass, and the edge pass found more gaps of the same kind. The contract now owes: the winning shape for each shared function, the running-row repair rule, the post-header error arm, the rearm rule in the request handler, and a place for the new tables, routes, job kinds, and key prefixes the edge proofs registered.

## What stays risky

Six ideas stay risky after the second pass. The copy-on-write fork still cannot see moved upstream refs, and its reads reach only two of five byte paths. The want and have negotiation can send too little when its index rows arrive half-written. The object cache, the partial-clone filters, the precomputed clone pack, and bundle-URI each keep one or two open problems with known shapes. None of the six blocks the core server.

## Where the details live

- [scoreboard.md](scoreboard.md) has every idea, both passes, side by side.
- [research/proofs/](../research/proofs/) has the 33 Rust proofs.
- [research/reviews/](../research/reviews/) has the 33 re-reviews.
- [CONTRACTS.md](../CONTRACTS.md) is the rulebook, including the amendments.
