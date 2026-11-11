// Worker, Durable Object and entrypoint wiring, with the Workers runtime
// replaced by small fakes (test/cf-stub.mjs and the objects below).
import { test } from "node:test";
import assert from "node:assert/strict";
import { createHmac, generateKeyPairSync } from "node:crypto";
import worker, { CiReport, CiRunner, EXIT_GRACE_MS, GitEgress } from "../src/index.ts";
import { MAX_LOG_CHARS } from "../src/report.ts";

const REPO = "owner/git-edge";
const SHA = "f".repeat(40);
// The runner reads Date.now() for deadlines and retry times. Tests freeze
// it at setup() and move it with advance().
const realNow = Date.now;
let clock = realNow();
Date.now = () => clock;
const advance = (ms: number) => { clock += ms; };

const pem = generateKeyPairSync("rsa", { modulusLength: 2048 }).privateKey
  .export({ type: "pkcs1", format: "pem" }).toString();

class FakeR2 {
  m = new Map<string, string>();
  async put(k: string, v: string) { this.m.set(k, v); }
  async get(k: string) {
    const v = this.m.get(k);
    return v === undefined ? null : { text: async () => v, body: v };
  }
}

class FakeStorage {
  m = new Map<string, unknown>();
  alarm: number | null = null;
  async get(k: string) { return structuredClone(this.m.get(k)); }
  async put(k: string, v: unknown) { this.m.set(k, structuredClone(v)); }
  async delete(k: string) { return this.m.delete(k); }
  async list(o: { prefix: string }) {
    return new Map([...this.m].filter(([k]) => k.startsWith(o.prefix)).map(([k, v]) => [k, structuredClone(v)]));
  }
  async setAlarm(t: number) { this.alarm = t; }
  async deleteAlarm() { this.alarm = null; }
}

class FakeContainer {
  running = false;
  images = { base: "registry/ci@sha256:abc" };
  intercepts: Array<[string, string, unknown]> = [];
  startOpts: Record<string, unknown> | null = null;
  destroyed = 0;
  exit: { resolve: () => void; reject: (e: unknown) => void } | null = null;
  async interceptOutboundHttp(h: string, b: unknown) { this.intercepts.push(["http", h, b]); }
  async interceptOutboundHttps(h: string, b: unknown) { this.intercepts.push(["https", h, b]); }
  start(o: Record<string, unknown>) { this.startOpts = o; this.running = true; }
  async setInactivityTimeout() {}
  monitor() { return new Promise<void>((resolve, reject) => { this.exit = { resolve, reject }; }); }
  async destroy() { this.destroyed++; this.running = false; this.exit?.reject(new Error("destroyed")); }
}

function githubMock(failPatches = 0) {
  const calls: Array<{ url: string; method: string; body: any }> = [];
  let nextId = 42;
  globalThis.fetch = (async (url: string, init: RequestInit = {}) => {
    calls.push({ url: String(url), method: init.method ?? "GET", body: init.body ? JSON.parse(String(init.body)) : undefined });
    if (init.method === "PATCH" && failPatches-- > 0) return new Response("{}", { status: 502 });
    if (String(url).endsWith("/access_tokens")) return new Response(JSON.stringify({ token: "ghs_test" }), { status: 201 });
    if (String(url).endsWith("/check-runs")) return new Response(JSON.stringify({ id: nextId++ }), { status: 201 });
    return new Response("{}", { status: 200 });
  }) as typeof fetch;
  return calls;
}

function setup() {
  clock = realNow();
  const logs = new FakeR2();
  const storage = new FakeStorage();
  const container = new FakeContainer();
  const waits: Promise<unknown>[] = [];
  const env: any = {
    WEBHOOK_SECRET: "whsec", APP_ID: "1", APP_PRIVATE_KEY: pem, LOG_URL_KEY: "logkey", CI_REPO: REPO,
    RUN_TIMEOUT_MIN: "75", LOG_TTL_DAYS: "14", INSTANCE: "standard-3", LOGS: logs,
  };
  const ctx: any = {
    storage, container,
    waitUntil: (p: Promise<unknown>) => waits.push(p),
    blockConcurrencyWhile: (f: () => Promise<unknown>) => { const p = f(); waits.push(p); return p; },
    exports: {
      CiReport: (o: unknown) => ({ entry: "CiReport", ...(o as object) }),
      GitEgress: (o: unknown) => ({ entry: "GitEgress", ...(o as object) }),
    },
  };
  const runner = new CiRunner(ctx, env);
  const begun: unknown[] = [];
  env.CI_RUNNER = {
    getByName: (name: string) => ({
      begin: (plan: unknown, origin: string) => { begun.push({ name, plan, origin }); return runner.begin(plan as any, origin); },
      report: (...a: [string, "log" | "done", string, number]) => runner.report(...a),
    }),
  };
  return { env, ctx, logs, storage, container, runner, begun, waits };
}

const prBody = JSON.stringify({
  action: "synchronize",
  repository: { full_name: REPO, default_branch: "main" },
  installation: { id: 9 },
  pull_request: { number: 3, head: { sha: SHA, repo: { full_name: REPO } } },
});
const hook = (body: string, event: string, secret = "whsec") => new Request("https://ci.example/webhook", {
  method: "POST", body,
  headers: { "x-github-event": event, "x-hub-signature-256": "sha256=" + createHmac("sha256", secret).update(body).digest("hex") },
});

test("webhook: a bad signature is 401 and starts nothing", async () => {
  const s = setup();
  githubMock();
  const res = await worker.fetch(hook(prBody, "pull_request", "wrong"), s.env);
  assert.equal(res.status, 401);
  assert.equal(s.begun.length, 0);
  const unsigned = new Request("https://ci.example/webhook", { method: "POST", body: prBody, headers: { "x-github-event": "pull_request" } });
  assert.equal((await worker.fetch(unsigned, s.env)).status, 401);
});

test("webhook: unconfigured Worker refuses everything", async () => {
  const s = setup();
  s.env.WEBHOOK_SECRET = "";
  assert.equal((await worker.fetch(hook(prBody, "pull_request"), s.env)).status, 503);
});

test("webhook: ping and ignored events start nothing", async () => {
  const s = setup();
  githubMock();
  assert.equal((await worker.fetch(hook("{}", "ping"), s.env)).status, 200);
  const closed = prBody.replace("synchronize", "closed");
  const res = await worker.fetch(hook(closed, "pull_request"), s.env);
  assert.equal(res.status, 202);
  assert.match(await res.text(), /ignored/);
  assert.equal(s.begun.length, 0);
});

test("webhook -> runner: check run in progress, locked-down container, then success", async () => {
  const s = setup();
  const calls = githubMock();
  const res = await worker.fetch(hook(prBody, "pull_request"), s.env);
  assert.equal(res.status, 202);
  assert.match(await res.text(), /^started f{40}-\d{13}-[0-9a-f]{8}\n$/);
  assert.equal((s.begun[0] as any).name, SHA, "one Durable Object per head sha");

  const create = calls.find((c) => c.url.endsWith("/check-runs"))!;
  assert.equal(create.body.name, "git-edge-ci");
  assert.equal(create.body.status, "in_progress");
  assert.equal(create.body.head_sha, SHA);
  assert.match(create.body.details_url, /^https:\/\/ci\.example\/logs\/f{40}-\d{13}-[0-9a-f]{8}\?exp=\d+&sig=[0-9a-f]{64}$/);

  const opts = s.container.startOpts!;
  assert.equal(opts.enableInternet, false);
  assert.equal(opts.instance, "standard-3");
  const cenv = opts.env as Record<string, string>;
  assert.equal(cenv.CI_SHA, SHA);
  assert.equal(cenv.CI_FETCH_REF, "refs/pull/3/head");
  for (const v of Object.values(cenv)) assert.ok(!v.includes("ghs_test") && !v.includes("PRIVATE KEY"), "no credential in the container env");
  assert.deepEqual(s.container.intercepts.map(([p, h]) => `${p}:${h}`), [
    "http:ci.internal", "https:github.com", "https:index.crates.io", "https:static.crates.io", "https:registry.npmjs.org",
  ]);
  assert.ok(s.storage.alarm! > Date.now() + 74 * 60_000);

  // a redelivery while running starts nothing new
  assert.match(await (await worker.fetch(hook(prBody, "pull_request"), s.env)).text(), /already running/);

  const nonce = cenv.CI_NONCE;
  assert.equal(await s.runner.report("forged", "done", "::gates:: pass", 0), 403, "wrong nonce");
  const log = ["cargo", "harness-barrier-0", "harness-barrier-1", "tla"].map((g) => `::gate:: ${g} pass 1s`).join("\n");
  assert.equal(await s.runner.report(nonce, "log", "partial", 0), 204);
  assert.equal(await s.runner.report(nonce, "done", log, 0), 204);
  const done = calls.filter((c) => c.method === "PATCH").at(-1)!;
  assert.equal(done.url, "https://api.github.com/repos/owner/git-edge/check-runs/42");
  assert.equal(done.body.conclusion, "success");
  assert.equal(s.storage.alarm, null);
  assert.equal(await s.runner.report(nonce, "done", log, 0), 403, "a finished run takes no more reports");
});

test("runner: a gate failure, a silent exit and a timeout each fail the check", async () => {
  for (const mode of ["gate", "exit", "timeout"] as const) {
    const s = setup();
    const calls = githubMock();
    await worker.fetch(hook(prBody, "pull_request"), s.env);
    const nonce = (s.container.startOpts!.env as Record<string, string>).CI_NONCE;
    if (mode === "gate") await s.runner.report(nonce, "done", "::gate:: cargo fail 9s", 1);
    if (mode === "exit") {
      s.container.exit!.reject(Object.assign(new Error("x"), { exitCode: 137 }));
      await Promise.all(s.waits);
      advance(EXIT_GRACE_MS);
      await s.runner.alarm();
    }
    if (mode === "timeout") { advance(75 * 60_000); await s.runner.alarm(); }
    const done = calls.filter((c) => c.method === "PATCH").at(-1)!;
    assert.equal(done.body.conclusion, mode === "timeout" ? "timed_out" : "failure", mode);
    if (mode === "exit") assert.match(done.body.output.summary, /without a result \(137\)/);
    if (mode === "timeout") assert.equal(s.container.destroyed, 1);
  }
});

test("runner: a failed check update is retried from the alarm", async () => {
  const s = setup();
  const calls = githubMock(1);
  await worker.fetch(hook(prBody, "pull_request"), s.env);
  const nonce = (s.container.startOpts!.env as Record<string, string>).CI_NONCE;
  await s.runner.report(nonce, "done", "::gate:: cargo fail 9s", 1);
  assert.equal(calls.filter((c) => c.method === "PATCH").length, 1);
  assert.ok(s.storage.alarm! > Date.now(), "retry alarm set");
  await s.runner.alarm();
  assert.equal(calls.filter((c) => c.method === "PATCH").length, 1, "not retried before it is due");
  advance(60_000);
  await s.runner.alarm();
  const patches = calls.filter((c) => c.method === "PATCH");
  assert.equal(patches.length, 2);
  assert.equal(patches[1].body.conclusion, "failure");
  assert.equal((await s.storage.list({ prefix: "pending/" })).size, 0);
  assert.equal(s.storage.alarm, null, "nothing left to wake for");
});

test("runner: a failed check update is given up after 10 tries", async () => {
  const s = setup();
  const calls = githubMock(100);
  await worker.fetch(hook(prBody, "pull_request"), s.env);
  const nonce = (s.container.startOpts!.env as Record<string, string>).CI_NONCE;
  await s.runner.report(nonce, "done", "::gate:: cargo fail 9s", 1);
  for (let i = 0; i < 12; i++) { advance(16 * 60_000); await s.runner.alarm(); }
  assert.equal(calls.filter((c) => c.method === "PATCH").length, 10);
  assert.equal((await s.storage.list({ prefix: "pending/" })).size, 0);
  assert.equal(s.storage.alarm, null);
});

test("runner: concurrent deliveries for one sha start one run and one check", async () => {
  const s = setup();
  const calls = githubMock();
  const [a, b] = await Promise.all([
    worker.fetch(hook(prBody, "pull_request"), s.env),
    worker.fetch(hook(prBody, "pull_request"), s.env),
  ]);
  const texts = [await a.text(), await b.text()].sort();
  assert.match(texts[0], /^already running/);
  assert.match(texts[1], /^started/);
  assert.equal(calls.filter((c) => c.url.endsWith("/check-runs")).length, 1);
});

test("runner: a result racing the alarm completes the check once", async () => {
  const s = setup();
  const calls = githubMock();
  await worker.fetch(hook(prBody, "pull_request"), s.env);
  const nonce = (s.container.startOpts!.env as Record<string, string>).CI_NONCE;
  const log = ["cargo", "harness-barrier-0", "harness-barrier-1", "tla"].map((g) => `::gate:: ${g} pass 1s`).join("\n");
  await Promise.all([s.runner.report(nonce, "done", log, 0), s.runner.alarm()]);
  const patches = calls.filter((c) => c.method === "PATCH");
  assert.equal(patches.length, 1);
  assert.equal(patches[0].body.conclusion, "success");
});

test("runner: the old container's exit never fails a newer run", async () => {
  const s = setup();
  const calls = githubMock();
  const run1Watch = s.waits.length;
  await worker.fetch(hook(prBody, "pull_request"), s.env);
  const nonce = (s.container.startOpts!.env as Record<string, string>).CI_NONCE;
  await s.runner.report(nonce, "done", "::gate:: cargo fail 1s", 1); // finished, container still up
  advance(1); // a new run id
  await worker.fetch(hook(prBody.replace("synchronize", "reopened"), "pull_request"), s.env); // destroys it, starts run 2
  await s.waits[run1Watch]; // run 1's monitor settled when its container was destroyed
  assert.equal(calls.filter((c) => c.method === "PATCH").length, 1, "run 2 is still in progress");
  const st = (await s.storage.get("run")) as any;
  assert.equal(st.finished, false);
  assert.equal(st.exited, undefined, "run 1's exit did not start run 2's grace");
});

test("runner: a new run never drops the previous run's unpublished result", async () => {
  const s = setup();
  const calls = githubMock(2); // run 1's completion fails twice
  await worker.fetch(hook(prBody, "pull_request"), s.env);
  const nonce1 = (s.container.startOpts!.env as Record<string, string>).CI_NONCE;
  await s.runner.report(nonce1, "done", "::gate:: cargo fail 1s", 1); // PATCH 42 fails, pending kept
  advance(1);
  await worker.fetch(hook(prBody.replace("synchronize", "reopened"), "pull_request"), s.env); // run 2, check 43
  const run2 = (await s.storage.get("run")) as any;
  assert.equal(run2.checkRunId, 43);
  assert.equal(s.storage.alarm, Date.now() - 1 + 60_000, "the alarm still serves run 1's retry, the earlier of the two");

  advance(60_000);
  await s.runner.alarm(); // fails again
  advance(120_000);
  await s.runner.alarm(); // lands
  const patches = calls.filter((c) => c.method === "PATCH");
  assert.deepEqual(patches.map((p) => p.url.split("/").at(-1)), ["42", "42", "42"]);
  assert.equal(patches[2].body.conclusion, "failure");
  assert.equal((await s.storage.list({ prefix: "pending/" })).size, 0);
  const st = (await s.storage.get("run")) as any;
  assert.equal(st.finished, false, "the retries did not time out run 2");
  assert.equal(s.storage.alarm, st.deadline, "back to run 2's timeout");

  const nonce2 = (s.container.startOpts!.env as Record<string, string>).CI_NONCE;
  const log = ["cargo", "harness-barrier-0", "harness-barrier-1", "tla"].map((g) => `::gate:: ${g} pass 1s`).join("\n");
  assert.equal(await s.runner.report(nonce2, "done", log, 0), 204);
  const last = calls.filter((c) => c.method === "PATCH").at(-1)!;
  assert.equal(last.url.split("/").at(-1), "43");
  assert.equal(last.body.conclusion, "success");
});

test("runner: two runs begun in the same millisecond keep apart", async () => {
  const s = setup();
  const calls = githubMock(1); // run 1's completion fails once
  await worker.fetch(hook(prBody, "pull_request"), s.env);
  const run1 = (await s.storage.get("run")) as any;
  await s.runner.report((s.container.startOpts!.env as Record<string, string>).CI_NONCE, "done", "::gate:: cargo fail 1s", 1);
  // no advance(): run 2 starts in the same millisecond
  await worker.fetch(hook(prBody.replace("synchronize", "reopened"), "pull_request"), s.env);
  const run2 = (await s.storage.get("run")) as any;
  assert.notEqual(run2.runId, run1.runId);
  assert.equal(run2.runId.split("-")[1], run1.runId.split("-")[1], "same millisecond");
  const log = ["cargo", "harness-barrier-0", "harness-barrier-1", "tla"].map((g) => `::gate:: ${g} pass 1s`).join("\n");
  await s.runner.report((s.container.startOpts!.env as Record<string, string>).CI_NONCE, "done", log, 0);
  assert.deepEqual([...(await s.storage.list({ prefix: "pending/" })).keys()], [`pending/${run1.runId}`], "run 2's publish kept run 1's retry");
  assert.match(s.logs.m.get(`runs/${run1.runId}.log`)!, /cargo fail/, "run 2 did not overwrite run 1's log");
  assert.equal(calls.filter((c) => c.method === "PATCH").at(-1)!.body.conclusion, "success");
});

test("runner: a log that cannot be summarised still completes the check as failed", async () => {
  const s = setup();
  const calls = githubMock();
  await worker.fetch(hook(prBody, "pull_request"), s.env);
  const nonce = (s.container.startOpts!.env as Record<string, string>).CI_NONCE;
  s.logs.get = async () => ({ text: async () => 42 as unknown as string, body: "" }); // parseGates throws
  const log = ["cargo", "harness-barrier-0", "harness-barrier-1", "tla"].map((g) => `::gate:: ${g} pass 1s`).join("\n");
  assert.equal(await s.runner.report(nonce, "done", log, 0), 204);
  const done = calls.filter((c) => c.method === "PATCH").at(-1)!;
  assert.equal(done.body.conclusion, "failure");
  assert.match(done.body.output.summary, /could not be summarised/);
  assert.equal(((await s.storage.get("run")) as any).finished, true);
  assert.equal(s.storage.alarm, null);
});

test("runner: a container that stopped while the object was away fails after the grace", async () => {
  const s = setup();
  const calls = githubMock();
  await worker.fetch(hook(prBody, "pull_request"), s.env);
  s.container.running = false; // stopped; the old object never saw monitor() settle
  const restarted = new CiRunner(s.ctx, s.env);
  await s.waits.at(-1); // the constructor's blockConcurrencyWhile
  const st = (await s.storage.get("run")) as any;
  assert.equal(st.exited, "not running after restart");
  assert.equal(s.storage.alarm, Date.now() + EXIT_GRACE_MS, "not the full timeout");
  advance(EXIT_GRACE_MS);
  await restarted.alarm();
  const done = calls.filter((c) => c.method === "PATCH").at(-1)!;
  assert.equal(done.body.conclusion, "failure");
  assert.match(done.body.output.summary, /without a result \(not running after restart\)/);
});

test("runner: a restarted object with its container still running re-watches it", async () => {
  const s = setup();
  githubMock();
  await worker.fetch(hook(prBody, "pull_request"), s.env);
  const restarted = new CiRunner(s.ctx, s.env);
  await s.waits.at(-1);
  const st = (await s.storage.get("run")) as any;
  assert.equal(st.exited, undefined, "a running container is not an exit");
  s.container.exit!.resolve();
  await s.waits.at(-1); // the new watch
  assert.equal(((await s.storage.get("run")) as any).exited, "exit code 0");
  void restarted;
});

test("runner: a log that cannot be read still completes the check", async () => {
  const s = setup();
  const calls = githubMock();
  await worker.fetch(hook(prBody, "pull_request"), s.env);
  s.logs.get = async () => { throw new Error("r2 down"); };
  await s.runner.alarm(); // not due: nothing happens
  advance(75 * 60_000);
  await s.runner.alarm();
  const done = calls.filter((c) => c.method === "PATCH").at(-1)!;
  assert.equal(done.body.conclusion, "timed_out");
  assert.match(done.body.output.text, /could not be read/);
});

test("runner: a slow /done after a fast container exit still completes the run", async () => {
  const s = setup();
  const calls = githubMock();
  await worker.fetch(hook(prBody, "pull_request"), s.env);
  const nonce = (s.container.startOpts!.env as Record<string, string>).CI_NONCE;
  s.container.exit!.resolve(); // exit reaches the object first
  await Promise.all(s.waits);
  assert.equal(calls.filter((c) => c.method === "PATCH").length, 0, "an exit alone does not fail the run");
  assert.equal(s.storage.alarm, Date.now() + EXIT_GRACE_MS);
  advance(EXIT_GRACE_MS - 1);
  await s.runner.alarm(); // an early wake-up inside the grace changes nothing
  assert.equal(calls.filter((c) => c.method === "PATCH").length, 0);
  const log = ["cargo", "harness-barrier-0", "harness-barrier-1", "tla"].map((g) => `::gate:: ${g} pass 1s`).join("\n");
  assert.equal(await s.runner.report(nonce, "done", log, 0), 204);
  advance(EXIT_GRACE_MS);
  await s.runner.alarm();
  const patches = calls.filter((c) => c.method === "PATCH");
  assert.equal(patches.length, 1);
  assert.equal(patches[0].body.conclusion, "success");
  assert.equal(s.storage.alarm, null);
});

test("runner: an exit with no /done fails once the grace ends", async () => {
  const s = setup();
  const calls = githubMock();
  await worker.fetch(hook(prBody, "pull_request"), s.env);
  const nonce = (s.container.startOpts!.env as Record<string, string>).CI_NONCE;
  s.container.exit!.resolve();
  await Promise.all(s.waits);
  advance(EXIT_GRACE_MS);
  await s.runner.alarm();
  const patches = calls.filter((c) => c.method === "PATCH");
  assert.equal(patches.length, 1);
  assert.equal(patches[0].body.conclusion, "failure");
  assert.match(patches[0].body.output.summary, /without a result \(exit code 0\)/);
  assert.equal(await s.runner.report(nonce, "done", "::gates:: pass", 0), 403, "too late");
});

test("runner: a nonce that differs only in its last character is refused", async () => {
  const s = setup();
  githubMock();
  await worker.fetch(hook(prBody, "pull_request"), s.env);
  const nonce = (s.container.startOpts!.env as Record<string, string>).CI_NONCE;
  const near = nonce.slice(0, -1) + (nonce.endsWith("0") ? "1" : "0");
  assert.equal(await s.runner.report(near, "log", "x", 0), 403);
  assert.equal(await s.runner.report("", "log", "x", 0), 403);
  assert.equal(await s.runner.report(nonce, "log", "x", 0), 204);
});

test("webhook: a fork pull request gets a completed failure check and no run", async () => {
  const s = setup();
  const calls = githubMock();
  const fork = prBody.replace(`"head":{"sha":"${SHA}","repo":{"full_name":"${REPO}"}}`, `"head":{"sha":"${SHA}","repo":{"full_name":"x/git-edge"}}`);
  const res = await worker.fetch(hook(fork, "pull_request"), s.env);
  assert.equal(res.status, 202);
  assert.equal(s.begun.length, 0);
  const c = calls.find((x) => x.url.endsWith("/check-runs"))!;
  assert.equal(c.body.conclusion, "failure");
});

test("logs: served only through a valid signed link", async () => {
  const s = setup();
  const calls = githubMock();
  await worker.fetch(hook(prBody, "pull_request"), s.env);
  const url = calls.find((c) => c.url.endsWith("/check-runs"))!.body.details_url;
  const ok = await worker.fetch(new Request(url), s.env);
  assert.equal(ok.status, 200);
  assert.match(await ok.text(), /starting/);
  const bad = new URL(url); bad.searchParams.set("sig", "0".repeat(64));
  assert.equal((await worker.fetch(new Request(bad), s.env)).status, 403);
});

test("GitEgress: adds the token only on allowed git fetches, never follows redirects", async () => {
  const seen: Array<{ url: string; auth: string | null; redirect: string | undefined }> = [];
  globalThis.fetch = (async (r: Request, init?: RequestInit) => {
    seen.push({ url: r.url, auth: r.headers.get("authorization"), redirect: init?.redirect });
    return new Response("ok");
  }) as typeof fetch;
  const e = new GitEgress({ props: { repo: REPO, token: "ghs_t" } } as any, {} as any);
  const ok = await e.fetch(new Request("https://github.com/owner/git-edge.git/info/refs?service=git-upload-pack"));
  assert.equal(ok.status, 200);
  assert.equal(seen[0].auth, `Basic ${btoa("x-access-token:ghs_t")}`);
  assert.equal(seen[0].redirect, "manual");
  const push = await e.fetch(new Request("https://github.com/owner/git-edge.git/git-receive-pack", { method: "POST", body: "x" }));
  assert.equal(push.status, 403);
  const crate = await e.fetch(new Request("https://static.crates.io/crates/a/a-1.crate", { headers: { authorization: "Bearer leak" } }));
  assert.equal(crate.status, 200);
  assert.equal(seen[1].auth, null, "container-supplied auth is stripped");
  assert.equal(seen.length, 2);
});

test("CiReport: forwards nonce, kind and exit code to the run's Durable Object", async () => {
  const got: unknown[] = [];
  const env: any = { CI_RUNNER: { getByName: (n: string) => ({ report: async (...a: unknown[]) => { got.push([n, ...a]); return 204; } }) } };
  const r = new CiReport({ props: { name: SHA } } as any, env);
  const post = (path: string, body: string | Uint8Array, headers: Record<string, string> = {}) =>
    new Request(`http://ci.internal${path}`, { method: "POST", body, headers });
  const res = await r.fetch(post("/done", "LOG", { "x-ci-nonce": "n", "x-ci-exit": "3" }));
  assert.equal(res.status, 204);
  assert.deepEqual(got[0], [SHA, "n", "done", "LOG", 3]);
  assert.equal((await r.fetch(post("/other", ""))).status, 404);
  // a body larger than the kept log streams through, and only its tail is forwarded
  let sent = 0;
  const big = new ReadableStream({
    pull(c) {
      if (sent++ === 41) { c.enqueue(new TextEncoder().encode("\n::gates:: pass\n")); c.close(); return; }
      c.enqueue(new Uint8Array(1024 * 1024).fill(0x61));
    },
  });
  const streamed = new Request("http://ci.internal/done", { method: "POST", body: big, duplex: "half", headers: { "x-ci-nonce": "n" } } as RequestInit);
  assert.equal(streamed.headers.get("content-length"), null);
  assert.equal((await r.fetch(streamed)).status, 204);
  const fwd = (got[1] as unknown[])[3] as string;
  assert.equal(fwd.length, MAX_LOG_CHARS);
  assert.match(fwd, /^\[log truncated to its last \d+ characters\]\n/);
  assert.ok(fwd.endsWith("\n::gates:: pass\n"));
  // a declared length over the hard cap is refused before reading
  const huge = post("/log", "x", { "content-length": String(512 * 1024 * 1024) });
  assert.equal((await r.fetch(huge)).status, 413);
  assert.equal(got.length, 2);
});
