# git-edge

git-edge is a git server that has no server. git is a tool that keeps every version of a set of files, and lets many people share those versions. Normally you need a computer that runs all day to share those versions. git-edge uses Cloudflare's network instead. Small programs run near the user. One small program per project keeps the list of branches. A large file store keeps the file contents. There is no machine to manage, patch, or restart.

Think of it like this. A normal git server is a shop with a shopkeeper who must be there all day, even when nobody comes. git-edge is a vending machine. It wakes up when someone presses a button, does one thing, and goes back to sleep. Many vending machines can stand in many cities, and each one knows exactly what is inside it.

The study below came first: 56 ideas, each proven in code, each reviewed by a second agent trying to break it. The working server in `server/` is the study compiled to a Rust/WASM Cloudflare Worker — one SQLite Durable Object per repository, packfiles in R2 — verified end-to-end against a stock `git` client on repositories up to microsoft/TypeScript's 984,826 objects.

## Why this matters

Git hosting is the last always-on piece of an otherwise serverless stack. A company running CI, agents, and ephemeral environments still pays for a git machine that sits idle between pushes, or leans on a hosted provider's rate limits and per-seat pricing for something that is, at heart, a content-addressed object store with a compare-and-swap ref log.

Two shifts make this the right time for it.

**Agentic development multiplies repositories.** Agents do not share one repo per team. They want a repo per task, per experiment, per preview environment — created in milliseconds, cloned a few times, then thrown away. An always-on server makes that expensive and slow to provision. A vending machine makes it free: a repo exists the moment the first push lands, costs nothing at rest beyond the bytes it stores, and disappears on a `DELETE`. The per-repository Durable Object is also the isolation boundary an agent fleet needs — one repo's job queue, quota, and credentials never share fate with another's.

**The cost shape inverts.** A VM bills for capacity you provisioned. git-edge bills for requests and stored bytes. A clone of a consolidated repo costs about one R2 read no matter how long the history is. Idle repos cost object storage and nothing else. For a company running thousands of agent-created repos, that is the difference between a fleet of servers and a line item measured in cents.

Cloudflare showed the shape was possible: their **artifacts** work serves large blobs straight from R2 at edge scale, and **artifact-fs** mounts a lazy checkout over plain smart-HTTP — a protocol git-edge already speaks end to end (`v2`, `blob:none`, promisor backfill, `allowAnySHA1InWant`). git-edge extends that insight from serving artifacts to *being* the versioned store underneath them.

## What the server does today

All of this is exercised by the conformance suite (`tests/conformance/run.sh`) against a real `git` client, and every listed clone lands `git fsck --strict` clean.

- **Clone, fetch, push over smart HTTP.** Protocol v2 fetch (multi-round want/have, `deepen`, `deepen-since`, `deepen-not`, shallow, `blob:none`/`blob:limit` filters with promisor backfill, `want-ref`, any-oid fetch) and v0 receive-pack (create/update/delete, force-with-lease CAS, delete-only pushes, thin packs both directions).
- **`--atomic` pushes.** Every ref command's preconditions are checked before any write — one failure rejects the whole push.
- **Per-repo credentials.** Deployment-wide read/write tokens, plus per-repo tokens minted on `/_admin/tokens` with optional ref scopes (deploy keys that may only push `refs/heads/release-*`, say). Public/anonymous read is a flag, not a second deployment.
- **Ref pinning, delete, export.** Pin a ref to freeze it; `/_admin/delete` tombstones a repo and a purge job reclaims its bytes; `/_admin/export` streams a real `git bundle`.
- **Git LFS (basic transfer).** The batch API answers `upload`/`download` with HMAC-signed URLs; objects live under the repo's purge prefix and count toward its byte quota.
- **Server-side resumable import.** Stage a pack in parts, then `/_admin/import` runs a job that ingests it across slices — the path that survives a Worker isolate dying mid-history. Proven on a 462,299-object facebook/react pack (1,149 refs) and microsoft/TypeScript's 984,826 objects (324 refs, ~1h52m), whose single 222k-object commit cannot be pushed in slices at all.

- **URL import.** `POST /_admin/import {"url": "https://github.com/owner/repo"}` — no staging at all. The Worker runs protocol v2 as the *client*: `ls-refs` mints the ref commands (scoped, 100k cap), one `fetch` streams the remote's pack through a trailer-verifying hasher into R2, and the ordinary import pipeline commits it atomically — HEAD included. Verified against github.com. Public remotes only; plaintext http is loopback-only.
- **Quota + abuse limits.** Per-owner repo caps, per-repo object/byte caps, per-credential push rate limiting.
- **Self-healing jobs.** GC (mark → consolidate → sweep), imports, and purges run as alarm-driven jobs with resumable cursors, lease fencing, and separate strand-vs-error accounting — a rebuild or isolate death can't kill a multi-hour job.

## Benchmarks (local workerd, real git client)

| Workload | Result |
|---|---|
| Clone 20k commits | 4.3 s — 5/9000 subrequests |
| Clone 263k objects (react subset), consolidated | **18.7 s at 1 subrequest** — the whole pack streams verbatim |
| Same clone via `packfile-uris` offload | **14.8 s**, ~4 subrequests — bandwidth leaves the Worker entirely |
| Clone 718,383 objects (rails, consolidated) | fsck clean |
| Server-side import, facebook/react all refs | 462,299 objects + 1,149 refs committed; clone-back 6.41 GiB in 219 s, fsck clean, HEAD exact |
| Server-side import, microsoft/TypeScript all refs | 984,826 objects + 324 refs in ~1h52m (44 parts, zero retries); normalized to one 18.63 GiB pack; clone-back via `packfile-uris` in 1,020 s, fsck clean, HEAD exact |
| Push 200 MiB / 10,207 objects | 21 s |
| 4× parallel 20k clones | 6.9 s total |

The shape that matters: once a repo consolidates to one live pack, a plain clone costs ~1 R2 read regardless of history size — the subrequest wall that used to stop clones near ~263k objects is gone, and for opted-in clients the pack bytes never touch the Worker at all. The remaining inline-clone bound is wall-clock: verbatim streaming does ~32 MiB/s inside the 240 s request budget, so past ~7 GiB on the wire a clone needs `fetch.uriprotocols` — the URI path downloaded TypeScript's 18.63 GiB pack without touching the fetch budget at all.

## What it took — the walls, and the way around each

Every limit below was hit by a real run against a real repository. None of them required giving up on the model; each had a seam.

| Wall | What happened | The way around it |
|---|---|---|
| **9,000 subrequests per request** | One index lookup per object meant clones died near ~110k–263k objects with `budget exhausted` | GC consolidates a repo toward one live pack; that pack streams verbatim at ~1 subrequest. Multiple live packs take a no-walk path that streams every live object straight from the index |
| **~100 MB request body** | A real repo's history cannot land in one `git push` | Two paths: `tools/git-edge-import.sh` slices history into fast-forward pushes, or the server-side import stages the pack in parts and ingests it as a job |
| **The unsplittable commit** | TypeScript carries a single commit introducing ~222k objects — no client-side slice boundary exists under the ingest budget | `POST /_admin/import`: the pack is staged into R2, then a Durable Object job parses, resolves deltas, checks connectivity, and commits all refs atomically across alarm slices |
| **A Worker isolate can die mid-job** | Multi-hour imports were losing their undrained output buffer on every isolate yield and livelocking on the same bytes | Resumable cursors over durable state, yield-persisted output tails, and *strand accounting* — isolate deaths are counted separately from real errors so a rebuild-heavy dev loop can't exhaust a job's retry budget |
| **A stale multipart upload outlives its slice** | A failed finish aborted the output MPU; the retry then died deterministically on a dead upload | Dead-upload detection probes the result key, commits if the bytes already landed, otherwise wipes output state and rebuilds on a fresh upload |
| **Duplicate objects during GC transition** | Between `gc_commit` and `gc_sweep` every object exists in two live packs; the no-walk clone emitted both copies and `index-pack` rejected the pack | `no_walk_set` scans objects ordered by sha — copies are adjacent — and marks only the first. O(1) extra memory, verified by a conformance regression that clones inside the overlap window |
| **240 s per request** | TypeScript's 18.63 GiB normalized pack needs ~620 s of sideband streaming — no inline clone can finish | `packfile-uris`: the fetch returns a signed `/_packs/…` URL and git downloads the bytes directly — 18.63 GiB without touching the fetch budget |
| **Undeltified storage** | Normalized packs store objects whole: TypeScript's 2.72 GiB delta'd pack became 18.63 GiB on disk (6.85×) | The honest trade for O(1) streaming — R2 bills bytes at rest, not the CPU to reconstruct them. Documented, bounded by `GE_QUOTA_MAX_BYTES`, reclaimed by GC |
| **A stalled host can outlive a push lease** | A disk-full dev host froze all SQLite writes — including the import heartbeat — for over an hour; the janitor legitimately expired the push and swept the run | Operational guard, not a code fix: in-flight imports heartbeat per slice, so the host must stay healthy for the duration. Dead dev-state MPU parts needed manual reclaim (recipe in `HANDOFF.md`) |

## What is in this repository

The study that produced the server, plus the server itself. We took 56 ideas. For each idea, one agent wrote a small proof in code to show how it could work on Cloudflare. Then a second agent tried to break that proof. The second agent looked for three things:

- Will it run on Cloudflare today?
- Can a crash or two users at once lose data?
- Does it do what the idea says, or something weaker?

Read the proofs as an argument, and the reviews as the argument against. A small test program has run on the real runtime locally, and its answers are in [research/platform-facts.md](research/platform-facts.md).

## The result in one table

| Verdict | Count | What it means |
|---|---|---|
| Lands | 1 | Works today. No problem to fix first. |
| Lands with caveats | 33 | Sound idea. Known problems to fix first, each with a known fix. |
| Risky | 21 | Can be built. Some problems have no full fix yet, or the proof does something weaker than the idea. |
| Does not land | 1 | Does not work as stated. A weaker idea does work. |

The reviewers were told to doubt everything. So "lands with caveats" is the real pass mark. The one clean pass is the smallest piece: the first message a git program sends and the reply it expects. The one failure is a branch that many people edit at once, because a normal git program refuses that push before it leaves the user's machine.

**The main idea holds.** One small program per project, which changes a branch only if the branch still has the value it expects, does exactly what a normal git server does. Files stored under their own fingerprint are safe to write twice. No reviewer found a way for two users to end up with two different truths.

## Where to start reading

Read in this order.

1. [findings/how-to-read.md](findings/how-to-read.md). What the verdicts and the scores mean, in plain words.
2. [findings/cross-cutting-defects.md](findings/cross-cutting-defects.md). The nine problems that appeared again and again, with pictures, and the one fix for each.
3. [findings/build-order.md](findings/build-order.md). Six waves, from the first message to the exotic ideas.
4. [findings/scoreboard.md](findings/scoreboard.md). All 56 ideas scored, with a one-line plain summary each.
5. [findings/rust-and-gitoxide.md](findings/rust-and-gitoxide.md). Why the server will be written in Rust with gitoxide, and what that does and does not fix.
6. [findings/second-pass.md](findings/second-pass.md). What changed when 33 ideas were rewritten in Rust against one shared contract, in plain words.
7. [plain/](plain/). One plain-language explainer per idea, with an analogy, a diagram, and each problem explained.
8. [site/index.html](site/index.html). The interactive map. Click an idea to see its plain explainer, its proofs, and its reviews.

## The language decision

The 56 proofs were written in TypeScript. We then asked whether the server can be written in Rust. The answer is yes, with gitoxide, a set of Rust building blocks that already read and write git's file formats. gitoxide takes over the two hardest problems in the list below, the message format and the squeezed packfiles. The first library we looked at, git2-rs, does not run inside a Worker, and the reasons are in the finding. Read [findings/rust-and-gitoxide.md](findings/rust-and-gitoxide.md) for the plain version and [research/rust-server.md](research/rust-server.md) for the engineer memo with every source link.

## Map of the folders

| Folder | What is in it | Who it is for |
|---|---|---|
| `findings/` | The summary documents listed above | Everyone. Start here. |
| `plain/` | One explainer per idea in plain language | Everyone. |
| `research/proofs/` | Canonical (second-pass) Rust proofs against the contract | Engineers. |
| `research/reviews/` | Canonical (second-pass) reviews checking contract compliance | Engineers. |
| `research/proofs/archived/` | First-pass TypeScript proofs (historical) | Engineers. |
| `research/reviews/archived/` | First-pass reviews (historical) | Engineers. |
| `ideas/` | The list of 56 ideas as they went into the study | Everyone. |
| `site/` | The interactive map as one web page | Everyone. |
| `data/` | The structured results every other file was made from | Tools. |
| `research/` | Engineer memos (Rust check, platform facts) plus canonical proofs and reviews | Engineers. |
| `spikes/` | Small real programs run on the Cloudflare runtime to test a fact | Engineers. |
| `CONTRACTS.md` | The one set of rules every revised proof follows: storage layout, the ref transaction, the job timer, the janitor | Engineers. |
| `server/` | The working server: the contract compiled to a Rust/WASM Cloudflare Worker, tested with real git | Engineers. |
| `.devin/skills/git-edge/` | The agent skill: how to clone, push, import, and manage repos on a deployment (tokens, staged pushes, lifecycle endpoints, limits) | Agents operating a deployment. |
| `tools/` | `git-edge-import.sh`: staged-push importer for repos too big for one request | Operators. |
| `SETUP.md` | Deploy guide: secrets, vars, `wrangler deploy`, conformance verification, the admin surface, and what to put in front for production | Operators. |
| `COMPATIBILITY.md` | Every git feature the server speaks, what it does not, and the limits | Everyone. |
| `PRODUCTION-UAT.md` | The runnable deploy-and-verify checklist: gates, live battery, limits sign-off, rollback | Operators. |
| `ROADMAP.md` | Prioritized backlog scoped to the agent-built small/disposable-app profile, plus the public-repo fit benchmark | Everyone. |
| `STYLE.md` | The writing rules and glossary for the plain documents | Writers. |

## Importing an existing repository

The Cloudflare zone caps each request body at ~100 MB, so a repo whose history
is bigger than that cannot land in a single `git push`. Two paths exist.

**Server-side import (the big-repo path).** Build one pack of the source,
stage it in parts, then let the server ingest it as a background job — the
commit walks history itself, so a single commit holding hundreds of thousands
of objects imports fine (the shape that breaks client-side slicing, e.g.
TypeScript's 222k-object mega-commit):

```bash
# one pack of everything reachable, split under the zone's body cap
git -C src.git rev-list --objects --all | git -C src.git pack-objects --stdout > all.pack
split -b 64m all.pack part.

# stage each part onto one open push, then start the job
curl -X POST https://<host>/<owner>/<repo>/_admin/import/stage \
  -H "Authorization: Bearer $GE_TOKEN" --data-binary @part.aa        # -> {push, key, bytes}
curl -X POST "https://<host>/<owner>/<repo>/_admin/import/stage?push=$PUSH&part=ab" \
  -H "Authorization: Bearer $GE_TOKEN" --data-binary @part.ab        # ... per part
curl -X POST https://<host>/<owner>/<repo>/_admin/import \
  -H "Authorization: Bearer $GE_TOKEN" -H 'Content-Type: application/json' \
  -d '{"push":"'$PUSH'","parts":[{"key":"…","bytes":…},…],
       "commands":[{"old":"0000000000000000000000000000000000000000",
                    "new":"<tip>","name":"refs/heads/main"},…]}'

curl https://<host>/<owner>/<repo>/_admin/import/$PUSH \
  -H "Authorization: Bearer $GE_TOKEN"                               # progress + state
```

The job parses the staged pack across slices, resolves deltas server-side,
and commits all refs atomically — it resumes across isolate deaths and
rebuilds its output upload if one goes stale mid-run. Verified: a
1.12 GiB / 462,299-object react pack (1,149 refs) and a 2.72 GiB /
984,826-object TypeScript pack (324 refs) both commit end-to-end and
clone back `fsck --strict` clean.

**Client-side slicing (small/medium repos).** `tools/git-edge-import.sh`
slices the branch's first-parent chain into pushes that each stay under the
cap, pushes them oldest to newest (every one a clean fast-forward), and
resumes from the remote tip if it dies half-way:

```bash
export GE_TOKEN=ge_...            # write token — used via a credential
                                  # helper, never embedded in the URL
tools/git-edge-import.sh ./myrepo https://<host>/<owner>/<repo>
tools/git-edge-import.sh https://github.com/pallets/flask \
  https://<host>/pallets/flask --all-branches --tags
```

- `--branch <name>` picks one branch (default: the source's HEAD branch);
  `--all-branches` imports every `refs/heads/*`, HEAD branch first so it
  becomes the remote default; `--tags` pushes all tags at the end.
- `--dry-run` prints the slice plan — boundary commits, estimated MiB and
  object count per slice — and pushes nothing.
- `SLICE_MIB` (default 60) and `SLICE_OBJS` (default 30000) bound each push.
  The byte cap tracks the ~100 MB body limit; the object cap tracks the ingest
  subrequest budget, which dies around ~50k objects in one push. If one commit
  alone exceeds the caps the tool fails with remediation — the server-side
  import above is the fix.
- Re-running the same command is safe and cheap: a remote tip on the
  first-parent chain resumes mid-import. A remote tip that is not an ancestor
  of the source tip is refused — the tool never force-pushes.

Verify afterwards with a clone and `git fsck --strict`; compare the output
against fsck of the source, since old repos can carry pre-existing findings.

## The nine problems in one breath

These are the causes that kept coming back. The full document explains each with a picture.

1. git checks its message format byte by byte, and many proofs got a byte wrong.
2. The cleanup task can delete a file that a new push started to use.
3. Real pushes arrive squeezed, and many proofs expected them unsqueezed.
4. The proofs did not agree on how files are stored.
5. The small program lets requests collide while it waits on the network.
6. The small program has one timer, and everyone reset it.
7. git sends request bodies in ways the proofs did not expect.
8. One file read per object hits the subrequest limit.
9. The small program does not know its own name.

## How this study was made

Each idea went to one agent with a fixed brief. Show the mechanism in 40 to 120 lines of code on real Cloudflare building blocks. Name what you used. Name what you left out. Each proof then went to a second agent with a different brief. Doubt everything. Walk through one crash. Walk through two users at once. Name the exact byte that would break a real git program. The summary documents were made from those 112 outputs. The plain explainers were then written from the proofs and reviews, using the rules in `STYLE.md`.

The server itself was built the same way the study was — by agents. **Devin**, running on Cognition's **SWE-2 Max** model, carried the implementation from contract to conformance-tested server: the protocol paths, the job machinery, the import pipeline, the GC lifecycle, and every wall-and-workaround in the table above were found, fixed, and verified in the loop described there — reproduce, trace, fix at the root, prove with a real `git` client.

## Acknowledgements

**Cloudflare** — this project exists because of their platform work. **Workers + Durable Objects + R2** are the entire substrate, and their **artifacts** and **artifact-fs** projects showed the world that large object payloads belong in R2 behind a thin edge layer — git-edge applies that same shape to the thing developers version instead of the thing they deploy.

**Devin + SWE-2 Max** (Cognition) — the engineering credit above. The debugging that mattered most was the kind agents are built for: hours of telemetry across a stateful system, a fix that had to be correct on the third resume, and a conformance suite that kept the honest score.

**gitoxide** — the Rust building blocks that read and write git's formats inside a Worker, without which this stays a TypeScript prototype.

## License

Licensed under the [Apache License, Version 2.0](LICENSE). Copyright 2026 Ernest Sim. See [NOTICE](NOTICE).

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in git-edge shall be licensed as above, without any additional terms or conditions.
