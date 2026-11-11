// Webhook Worker + CI runner Durable Object. One Durable Object per commit
// sha owns that commit's check run and its container.

import { DurableObject, WorkerEntrypoint } from "cloudflare:workers";
import { decide, verifySignature, type RunPlan } from "./webhook.ts";
import { completePayload, conclude, createPayload, parseGates, rejectPayload, type Conclusion } from "./checks.ts";
import { createCheckRun, installationToken, updateCheckRun } from "./github.ts";
import { ALLOWED_HTTPS_HOSTS, egressDecision } from "./egress.ts";
import { signLogUrl, verifyLogUrl } from "./logurl.ts";
import { keepTail, readTail, sameSecret } from "./report.ts";

export interface CiEnv extends Env {
  WEBHOOK_SECRET: string;
  APP_ID: string;
  APP_PRIVATE_KEY: string;
  LOG_URL_KEY: string;
  CI_REPO: string;
}

const REPORT_HOST = "ci.internal";
/** How long a run whose container exited waits for its /done report before
 * it fails. Longer than the 60 s curl timeout in container/entry.sh, so a
 * slow /done that is still streaming in always lands first. */
export const EXIT_GRACE_MS = 90_000;
const MAX_PUBLISH_TRIES = 10;

interface RunState {
  runId: string;
  plan: RunPlan;
  checkRunId: number;
  nonce: string;
  detailsUrl: string;
  finished: boolean;
  /** When the alarm fails the run if it is still unfinished: the timeout,
   * or the end of the exit grace once the container has exited. */
  deadline: number;
  /** How the container exited, when it exited before reporting a result. */
  exited?: string;
}

/** A completion GitHub has not accepted yet. Each run keeps its own entry,
 * so a later run for the same commit never replaces it; the alarm retries
 * every entry that is due. */
interface Pending {
  runId: string;
  repo: string;
  installationId: number;
  checkRunId: number;
  payload: object;
  tries: number;
  due: number;
}

const PENDING_PREFIX = "pending/";

function configured(env: CiEnv): boolean {
  return Boolean(env.WEBHOOK_SECRET && env.APP_ID && env.APP_PRIVATE_KEY && env.LOG_URL_KEY && env.CI_REPO);
}

const nowSec = () => Math.floor(Date.now() / 1000);
const logKey = (runId: string) => `runs/${runId}.log`;

function randomHex(bytes: number): string {
  return [...crypto.getRandomValues(new Uint8Array(bytes))].map((b) => b.toString(16).padStart(2, "0")).join("");
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

  private async pendings(): Promise<Pending[]> {
    return [...(await this.ctx.storage.list<Pending>({ prefix: PENDING_PREFIX })).values()];
  }

  /** One alarm serves the run deadline and every pending retry: set it to
   * the earliest of them, or clear it when nothing is left. */
  private async rearm(): Promise<void> {
    const st = await this.state();
    const times = (await this.pendings()).map((p) => p.due);
    if (st && !st.finished) times.push(st.deadline);
    if (times.length) await this.ctx.storage.setAlarm(Math.min(...times));
    else await this.ctx.storage.deleteAlarm();
  }

  /** Starts a run for plan.sha unless one is already in progress. */
  begin(plan: RunPlan, origin: string): Promise<string> {
    return this.serial(() => this.beginNow(plan, origin));
  }

  private async beginNow(plan: RunPlan, origin: string): Promise<string> {
    const prev = await this.state();
    if (prev && !prev.finished) return `already running ${prev.runId}`;
    // The previous run's unpublished completion, if any, keeps its own
    // pending entry and alarm retries; replacing "run" does not touch it.
    const env = this.env;
    const runId = `${plan.sha}-${String(Date.now()).padStart(13, "0")}`;
    const ttlSec = Number(env.LOG_TTL_DAYS) * 86400;
    const detailsUrl = await signLogUrl(origin, env.LOG_URL_KEY, runId, nowSec() + ttlSec);
    await env.LOGS.put(logKey(runId), `run ${runId} for ${plan.trigger}: starting\n`);
    const token = await installationToken(env.APP_ID, env.APP_PRIVATE_KEY, plan.installationId, plan.repo, nowSec());
    const checkRunId = await createCheckRun(token, plan.repo, createPayload(plan.sha, detailsUrl, plan.trigger, new Date()));
    const st: RunState = {
      runId, plan, checkRunId, nonce: randomHex(32), detailsUrl, finished: false,
      deadline: Date.now() + Number(env.RUN_TIMEOUT_MIN) * 60_000,
    };
    await this.ctx.storage.put("run", st);
    await this.rearm();
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

  /** The container posts /done and then exits, but the exit can reach this
   * object first: the /done body may still be streaming into CiReport, or
   * curl may have timed out on a slow reply. So an exit does not fail the run
   * at once. It starts a grace period; a /done that lands in it completes the
   * run as usual, and the alarm fails the run when the grace ends. */
  private onExit(runId: string, how: string): Promise<void> {
    return this.serial(async () => {
      const st = await this.state();
      if (!st || st.finished || st.runId !== runId || st.exited !== undefined) return;
      st.exited = how.slice(0, 200);
      st.deadline = Math.min(st.deadline, Date.now() + EXIT_GRACE_MS);
      await this.ctx.storage.put("run", st);
      await this.rearm();
    });
  }

  /** Called by CiReport for the container's log uploads and final result. */
  report(nonce: string, kind: "log" | "done", log: string, exitCode: number): Promise<number> {
    return this.serial(async () => {
      const st = await this.state();
      if (!st || st.finished || !sameSecret(nonce, st.nonce)) return 403;
      await this.env.LOGS.put(logKey(st.runId), keepTail(log));
      if (kind === "done") await this.finish(st, exitCode);
      return 204;
    });
  }

  alarm(): Promise<void> {
    return this.serial(() => this.alarmNow());
  }

  private async alarmNow(): Promise<void> {
    const now = Date.now();
    const st = await this.state();
    if (st && !st.finished && now >= st.deadline) {
      if (st.exited !== undefined) await this.finish(st, 1, `Runner stopped without a result (${st.exited})`);
      else await this.finish(st, 1, `Timed out after ${this.env.RUN_TIMEOUT_MIN} minutes`, "timed_out");
    }
    for (const p of await this.pendings()) if (p.due <= now) await this.publish(p);
    await this.rearm();
  }

  private async finish(st: RunState, exitCode: number, note?: string, force?: Conclusion): Promise<void> {
    st.finished = true;
    await this.ctx.storage.put("run", st);
    // The run is already stored as finished, so nothing after this point may
    // throw before publish() stores the completion: a log that cannot be read
    // completes the check without it.
    let log = "";
    try {
      const obj = await this.env.LOGS.get(logKey(st.runId));
      log = obj ? await obj.text() : "";
    } catch (e) {
      console.log(`log read failed: ${String(e).slice(0, 300)}`);
      log = "[the run log could not be read]\n";
    }
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
    await this.publish({
      runId: st.runId, repo: st.plan.repo, installationId: st.plan.installationId, checkRunId: st.checkRunId,
      payload, tries: 0, due: 0,
    });
    await this.rearm();
  }

  /** Completes one check run. A GitHub failure leaves the check in progress,
   * so keep the payload under the run's own key and let the alarm retry it,
   * up to MAX_PUBLISH_TRIES times in all, backing off from 1 to 16 minutes.
   * The caller re-arms the alarm. */
  private async publish(p: Pending): Promise<void> {
    const env = this.env;
    const key = PENDING_PREFIX + p.runId;
    try {
      const token = await installationToken(env.APP_ID, env.APP_PRIVATE_KEY, p.installationId, p.repo, nowSec());
      await updateCheckRun(token, p.repo, p.checkRunId, p.payload);
      await this.ctx.storage.delete(key);
    } catch (e) {
      console.log(`check run ${p.checkRunId} update failed (try ${p.tries + 1}): ${String(e).slice(0, 300)}`);
      const tries = p.tries + 1;
      if (tries < MAX_PUBLISH_TRIES) {
        await this.ctx.storage.put(key, { ...p, tries, due: Date.now() + 60_000 * 2 ** Math.min(p.tries, 4) });
      } else {
        await this.ctx.storage.delete(key);
      }
    }
  }
}

/** Receives the container's HTTP calls to http://ci.internal/{log,done}. */
export class CiReport extends WorkerEntrypoint<CiEnv, { name: string }> {
  async fetch(req: Request): Promise<Response> {
    const url = new URL(req.url);
    const kind = url.pathname === "/done" ? "done" : url.pathname === "/log" ? "log" : null;
    if (req.method !== "POST" || !kind) return new Response("not found", { status: 404 });
    const log = await readTail(req);
    if (log === null) return new Response("log too large", { status: 413 });
    const nonce = req.headers.get("x-ci-nonce") ?? "";
    const exitCode = Number(req.headers.get("x-ci-exit") ?? "1");
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
