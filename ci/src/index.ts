// Webhook Worker + CI runner Durable Object. One Durable Object per commit
// sha owns that commit's check run and its container.

import { DurableObject, WorkerEntrypoint } from "cloudflare:workers";
import { decide, verifySignature, type RunPlan } from "./webhook.ts";
import { completePayload, conclude, createPayload, parseGates, rejectPayload, type Conclusion } from "./checks.ts";
import { createCheckRun, installationToken, updateCheckRun } from "./github.ts";
import { ALLOWED_HTTPS_HOSTS, egressDecision } from "./egress.ts";
import { signLogUrl, verifyLogUrl } from "./logurl.ts";

export interface CiEnv extends Env {
  WEBHOOK_SECRET: string;
  APP_ID: string;
  APP_PRIVATE_KEY: string;
  LOG_URL_KEY: string;
  CI_REPO: string;
}

const REPORT_HOST = "ci.internal";
const MAX_LOG_BYTES = 20 * 1024 * 1024;
const MAX_REPORT_BYTES = 2 * MAX_LOG_BYTES;

interface RunState {
  runId: string;
  plan: RunPlan;
  checkRunId: number;
  nonce: string;
  detailsUrl: string;
  finished: boolean;
  /** A completion GitHub has not accepted yet; the alarm retries it. */
  pending?: { payload: object; tries: number };
}

function configured(env: CiEnv): boolean {
  return Boolean(env.WEBHOOK_SECRET && env.APP_ID && env.APP_PRIVATE_KEY && env.LOG_URL_KEY && env.CI_REPO);
}

const nowSec = () => Math.floor(Date.now() / 1000);
const logKey = (runId: string) => `runs/${runId}.log`;

function randomHex(bytes: number): string {
  return [...crypto.getRandomValues(new Uint8Array(bytes))].map((b) => b.toString(16).padStart(2, "0")).join("");
}

function keepTail(log: string): string {
  return log.length <= MAX_LOG_BYTES ? log : `[log truncated to the last ${MAX_LOG_BYTES} bytes]\n${log.slice(-MAX_LOG_BYTES)}`;
}

export class CiRunner extends DurableObject<CiEnv> {
  // Every state change runs one at a time. Awaited fetches open the input
  // gate, so without this two deliveries for one sha could both start a run,
  // or a result and the alarm could both complete the check.
  private chain: Promise<unknown> = Promise.resolve();

  constructor(ctx: DurableObjectState, env: CiEnv) {
    super(ctx, env);
    const c = ctx.container;
    if (c?.running) {
      // A restarted object (e.g. after a deploy) must re-arm both.
      ctx.blockConcurrencyWhile(async () => {
        await c.setInactivityTimeout(this.inactivityMs());
        const st = await this.state();
        if (st) this.watch(st.runId);
      });
    }
  }

  private serial<T>(fn: () => Promise<T>): Promise<T> {
    const run = this.chain.then(fn, fn);
    this.chain = run.catch(() => undefined);
    return run;
  }

  /** Outlives the run timeout. The 60 s log uploads also keep the object active. */
  private inactivityMs(): number {
    return (Number(this.env.RUN_TIMEOUT_MIN) + 10) * 60_000;
  }

  private async state(): Promise<RunState | undefined> {
    return this.ctx.storage.get<RunState>("run");
  }

  /** Starts a run for plan.sha unless one is already in progress. */
  begin(plan: RunPlan, origin: string): Promise<string> {
    return this.serial(() => this.beginNow(plan, origin));
  }

  private async beginNow(plan: RunPlan, origin: string): Promise<string> {
    const prev = await this.state();
    if (prev && !prev.finished) return `already running ${prev.runId}`;
    // A new run replaces the stored state and the alarm, so try the previous
    // run's unpublished completion once before it is dropped.
    if (prev?.pending) await this.publish(prev, prev.pending.payload, prev.pending.tries);
    const env = this.env;
    const runId = `${plan.sha}-${String(Date.now()).padStart(13, "0")}`;
    const ttlSec = Number(env.LOG_TTL_DAYS) * 86400;
    const detailsUrl = await signLogUrl(origin, env.LOG_URL_KEY, runId, nowSec() + ttlSec);
    await env.LOGS.put(logKey(runId), `run ${runId} for ${plan.trigger}: starting\n`);
    const token = await installationToken(env.APP_ID, env.APP_PRIVATE_KEY, plan.installationId, plan.repo, nowSec());
    const checkRunId = await createCheckRun(token, plan.repo, createPayload(plan.sha, detailsUrl, plan.trigger, new Date()));
    const st: RunState = { runId, plan, checkRunId, nonce: randomHex(32), detailsUrl, finished: false };
    await this.ctx.storage.put("run", st);
    await this.ctx.storage.setAlarm(Date.now() + Number(env.RUN_TIMEOUT_MIN) * 60_000);
    try {
      await this.startContainer(st, token);
    } catch (e) {
      await this.finish(st, 1, `Runner failed to start: ${String(e).slice(0, 300)}`);
    }
    return `started ${runId}`;
  }

  private async startContainer(st: RunState, token: string): Promise<void> {
    const c = this.ctx.container;
    if (!c) throw new Error("no container on this object");
    if (c.running) await c.destroy();
    // Register every intercept before start(): the HTTPS CA file must exist
    // when the entrypoint runs, and nothing else may resolve.
    const exp = this.ctx.exports as any;
    await c.interceptOutboundHttp(REPORT_HOST, exp.CiReport({ props: { name: st.plan.sha } }));
    const egress = exp.GitEgress({ props: { repo: st.plan.repo, token } });
    for (const host of ALLOWED_HTTPS_HOSTS) await c.interceptOutboundHttps(host, egress);
    c.start({
      image: c.images.base,
      instance: this.env.INSTANCE as "standard-3",
      enableInternet: false,
      env: {
        CI_REPO: st.plan.repo,
        CI_SHA: st.plan.sha,
        CI_FETCH_REF: st.plan.fetchRef,
        CI_RUN_ID: st.runId,
        CI_NONCE: st.nonce,
        CI_REPORT_URL: `http://${REPORT_HOST}`,
      },
    });
    await c.setInactivityTimeout(this.inactivityMs());
    this.watch(st.runId);
  }

  /** Watches the container started for runId. An exit only ever finishes
   * that run, never a later run that replaced it. */
  private watch(runId: string): void {
    const c = this.ctx.container!;
    this.ctx.waitUntil(
      c.monitor().then(
        () => this.onExit(runId, "exit code 0"),
        (e: unknown) => this.onExit(runId, String((e as { exitCode?: number })?.exitCode ?? e)),
      ),
    );
  }

  private onExit(runId: string, how: string): Promise<void> {
    return this.serial(async () => {
      const st = await this.state();
      if (!st || st.finished || st.runId !== runId) return;
      await this.finish(st, 1, `Runner stopped without a result (${how.slice(0, 200)})`);
    });
  }

  /** Called by CiReport for the container's log uploads and final result. */
  report(nonce: string, kind: "log" | "done", log: string, exitCode: number): Promise<number> {
    return this.serial(async () => {
      const st = await this.state();
      if (!st || st.finished || nonce !== st.nonce) return 403;
      await this.env.LOGS.put(logKey(st.runId), keepTail(log));
      if (kind === "done") await this.finish(st, exitCode);
      return 204;
    });
  }

  alarm(): Promise<void> {
    return this.serial(() => this.alarmNow());
  }

  private async alarmNow(): Promise<void> {
    const st = await this.state();
    if (!st) return;
    if (st.finished) {
      if (st.pending) await this.publish(st, st.pending.payload, st.pending.tries);
      return;
    }
    await this.finish(st, 1, `Timed out after ${this.env.RUN_TIMEOUT_MIN} minutes`, "timed_out");
  }

  private async finish(st: RunState, exitCode: number, note?: string, force?: Conclusion): Promise<void> {
    st.finished = true;
    await this.ctx.storage.put("run", st);
    await this.ctx.storage.deleteAlarm();
    const obj = await this.env.LOGS.get(logKey(st.runId));
    const log = obj ? await obj.text() : "";
    const gates = parseGates(log);
    const { conclusion, missing } = conclude(exitCode, gates);
    const payload = completePayload({
      conclusion: force ?? conclusion, gates, missing, log, detailsUrl: st.detailsUrl, note, now: new Date(),
    });
    const c = this.ctx.container;
    try {
      if (c?.running && (force || note)) await c.destroy();
    } catch (e) {
      console.log(`container destroy failed: ${String(e).slice(0, 300)}`);
    }
    await this.publish(st, payload, 0);
  }

  /** Completes the check run. A GitHub failure leaves the check in progress,
   * so keep the payload and retry from the alarm, up to 10 times. */
  private async publish(st: RunState, payload: object, tries: number): Promise<void> {
    const env = this.env;
    try {
      const token = await installationToken(env.APP_ID, env.APP_PRIVATE_KEY, st.plan.installationId, st.plan.repo, nowSec());
      await updateCheckRun(token, st.plan.repo, st.checkRunId, payload);
      st.pending = undefined;
    } catch (e) {
      console.log(`check run ${st.checkRunId} update failed (try ${tries + 1}): ${String(e).slice(0, 300)}`);
      st.pending = tries + 1 < 10 ? { payload, tries: tries + 1 } : undefined;
      if (st.pending) await this.ctx.storage.setAlarm(Date.now() + 60_000 * 2 ** Math.min(tries, 4));
    }
    await this.ctx.storage.put("run", st);
  }
}

/** Reads a body up to cap bytes, with or without Content-Length; null when larger. */
async function readCapped(req: Request, cap: number): Promise<string | null> {
  if (Number(req.headers.get("content-length") ?? "0") > cap) return null;
  if (!req.body) return "";
  const reader = req.body.getReader();
  const parts: Uint8Array[] = [];
  let n = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    n += value.byteLength;
    if (n > cap) {
      await reader.cancel();
      return null;
    }
    parts.push(value);
  }
  const all = new Uint8Array(n);
  let o = 0;
  for (const p of parts) { all.set(p, o); o += p.byteLength; }
  return new TextDecoder().decode(all);
}

/** Receives the container's HTTP calls to http://ci.internal/{log,done}. */
export class CiReport extends WorkerEntrypoint<CiEnv, { name: string }> {
  async fetch(req: Request): Promise<Response> {
    const url = new URL(req.url);
    const kind = url.pathname === "/done" ? "done" : url.pathname === "/log" ? "log" : null;
    if (req.method !== "POST" || !kind) return new Response("not found", { status: 404 });
    const body = await readCapped(req, MAX_REPORT_BYTES);
    if (body === null) return new Response("log too large", { status: 413 });
    const nonce = req.headers.get("x-ci-nonce") ?? "";
    const exitCode = Number(req.headers.get("x-ci-exit") ?? "1");
    const log = keepTail(body);
    const stub = this.env.CI_RUNNER.getByName(this.ctx.props.name) as unknown as DurableObjectStub<CiRunner>;
    const status = await stub.report(nonce, kind, log, Number.isInteger(exitCode) ? exitCode : 1);
    return new Response(null, { status });
  }
}

/** Forwards allowed outbound HTTPS from the container; see egress.ts. */
export class GitEgress extends WorkerEntrypoint<CiEnv, { repo: string; token: string }> {
  async fetch(req: Request): Promise<Response> {
    const url = new URL(req.url);
    const d = egressDecision(req.method, url, this.ctx.props.repo);
    if (!d.allow) {
      console.log(`egress denied: ${req.method} ${url.host}${url.pathname} (${d.reason})`);
      return new Response(`egress denied: ${d.reason}\n`, { status: d.status });
    }
    const out = new Request(url, req);
    out.headers.delete("authorization");
    if (d.addAuth) out.headers.set("authorization", `Basic ${btoa(`x-access-token:${this.ctx.props.token}`)}`);
    // manual: a redirect goes back to the container, never with the token.
    return fetch(out, { redirect: "manual" });
  }
}

export default {
  async fetch(req: Request, env: CiEnv): Promise<Response> {
    const url = new URL(req.url);
    if (!configured(env)) return new Response("not configured\n", { status: 503 });

    if (req.method === "GET" && url.pathname.startsWith("/logs/")) {
      const runId = await verifyLogUrl(env.LOG_URL_KEY, url, nowSec());
      if (!runId) return new Response("link expired or invalid\n", { status: 403 });
      const obj = await env.LOGS.get(logKey(runId));
      if (!obj) return new Response("no log yet\n", { status: 404 });
      return new Response(obj.body, {
        headers: {
          "content-type": "text/plain; charset=utf-8",
          "cache-control": "private, no-store",
          "x-content-type-options": "nosniff",
        },
      });
    }

    if (req.method === "POST" && url.pathname === "/webhook") {
      const body = await req.arrayBuffer();
      if (!(await verifySignature(env.WEBHOOK_SECRET, body, req.headers.get("x-hub-signature-256")))) {
        return new Response("bad signature\n", { status: 401 });
      }
      const event = req.headers.get("x-github-event");
      if (event === "ping") return new Response("pong\n");
      let payload: unknown;
      try {
        payload = JSON.parse(new TextDecoder().decode(body));
      } catch {
        return new Response("bad json\n", { status: 400 });
      }
      const d = decide(event, payload, env.CI_REPO);
      if (d.kind === "ignore") return new Response(`ignored: ${d.reason}\n`, { status: 202 });
      if (d.kind === "reject") {
        const token = await installationToken(env.APP_ID, env.APP_PRIVATE_KEY, d.plan.installationId, d.plan.repo, nowSec());
        await createCheckRun(token, d.plan.repo, rejectPayload(d.plan.sha, d.reason, new Date()));
        return new Response(`rejected: ${d.reason}\n`, { status: 202 });
      }
      const stub = env.CI_RUNNER.getByName(d.plan.sha) as unknown as DurableObjectStub<CiRunner>;
      const msg = await stub.begin(d.plan, url.origin);
      return new Response(`${msg}\n`, { status: 202 });
    }

    return new Response("not found\n", { status: 404 });
  },
} satisfies ExportedHandler<CiEnv>;
