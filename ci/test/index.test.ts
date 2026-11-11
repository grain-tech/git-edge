// Worker, Durable Object and entrypoint wiring, with the Workers runtime
// replaced by small fakes (test/cf-stub.mjs and the objects below).
import { test } from "node:test";
import assert from "node:assert/strict";
import { createHmac, generateKeyPairSync } from "node:crypto";
import worker, { CiReport, CiRunner, GitEgress } from "../src/index.ts";

const REPO = "owner/git-edge";
const SHA = "f".repeat(40);
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
  globalThis.fetch = (async (url: string, init: RequestInit = {}) => {
    calls.push({ url: String(url), method: init.method ?? "GET", body: init.body ? JSON.parse(String(init.body)) : undefined });
    if (init.method === "PATCH" && failPatches-- > 0) return new Response("{}", { status: 502 });
    if (String(url).endsWith("/access_tokens")) return new Response(JSON.stringify({ token: "ghs_test" }), { status: 201 });
    if (String(url).endsWith("/check-runs")) return new Response(JSON.stringify({ id: 42 }), { status: 201 });
    return new Response("{}", { status: 200 });
  }) as typeof fetch;
  return calls;
}

function setup() {
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
    blockConcurrencyWhile: (f: () => Promise<unknown>) => f(),
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
  assert.match(await res.text(), /^started f{40}-\d{13}/);
  assert.equal((s.begun[0] as any).name, SHA, "one Durable Object per head sha");

  const create = calls.find((c) => c.url.endsWith("/check-runs"))!;
  assert.equal(create.body.name, "git-edge-ci");
  assert.equal(create.body.status, "in_progress");
  assert.equal(create.body.head_sha, SHA);
  assert.match(create.body.details_url, /^https:\/\/ci\.example\/logs\/f{40}-\d{13}\?exp=\d+&sig=[0-9a-f]{64}$/);

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
    if (mode === "exit") { s.container.exit!.reject(Object.assign(new Error("x"), { exitCode: 137 })); await Promise.all(s.waits); }
    if (mode === "timeout") await s.runner.alarm();
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
  const patches = calls.filter((c) => c.method === "PATCH");
  assert.equal(patches.length, 2);
  assert.equal(patches[1].body.conclusion, "failure");
  assert.equal(((await s.storage.get("run")) as any).pending, undefined);
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
  await worker.fetch(hook(prBody, "pull_request"), s.env);
  const nonce = (s.container.startOpts!.env as Record<string, string>).CI_NONCE;
  await s.runner.report(nonce, "done", "::gate:: cargo fail 1s", 1); // finished, container still up
  await worker.fetch(hook(prBody.replace("synchronize", "reopened"), "pull_request"), s.env); // destroys it, starts run 2
  await s.waits[0]; // run 1's monitor settled when its container was destroyed
  assert.equal(calls.filter((c) => c.method === "PATCH").length, 1, "run 2 is still in progress");
  assert.equal(((await s.storage.get("run")) as any).finished, false);
});

test("runner: a new run first retries the previous run's unpublished result", async () => {
  const s = setup();
  const calls = githubMock(1);
  await worker.fetch(hook(prBody, "pull_request"), s.env);
  const nonce = (s.container.startOpts!.env as Record<string, string>).CI_NONCE;
  await s.runner.report(nonce, "done", "::gate:: cargo fail 1s", 1); // PATCH fails, pending kept
  await worker.fetch(hook(prBody.replace("synchronize", "reopened"), "pull_request"), s.env);
  const patches = calls.filter((c) => c.method === "PATCH");
  assert.equal(patches.length, 2, "the old completion was retried before the new run");
  assert.equal(patches[1].body.conclusion, "failure");
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
  // a body over the cap is refused while streaming, with no Content-Length
  const big = new ReadableStream({ start(c) { for (let i = 0; i < 41; i++) c.enqueue(new Uint8Array(1024 * 1024)); c.close(); } });
  const streamed = new Request("http://ci.internal/log", { method: "POST", body: big, duplex: "half" } as RequestInit);
  assert.equal(streamed.headers.get("content-length"), null);
  assert.equal((await r.fetch(streamed)).status, 413);
  assert.equal(got.length, 1);
});
