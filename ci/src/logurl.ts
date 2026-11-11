// Short-lived log links: /logs/<runId>?exp=<unix seconds>&sig=<hex HMAC>.

const enc = new TextEncoder();
const RUN_ID_RE = /^[0-9a-f]{40}-[0-9]{13}$/;

export function isRunId(s: string): boolean {
  return RUN_ID_RE.test(s);
}

async function hmacKey(secret: string, usage: "sign" | "verify") {
  return crypto.subtle.importKey("raw", enc.encode(secret), { name: "HMAC", hash: "SHA-256" }, false, [usage]);
}

const msg = (runId: string, exp: number) => enc.encode(`logs:${runId}:${exp}`);

export async function signLogUrl(base: string, secret: string, runId: string, exp: number): Promise<string> {
  const mac = new Uint8Array(await crypto.subtle.sign("HMAC", await hmacKey(secret, "sign"), msg(runId, exp)));
  const sig = [...mac].map((b) => b.toString(16).padStart(2, "0")).join("");
  const u = new URL(`/logs/${runId}`, base);
  u.searchParams.set("exp", String(exp));
  u.searchParams.set("sig", sig);
  return u.toString();
}

export async function verifyLogUrl(secret: string, url: URL, nowSec: number): Promise<string | null> {
  const m = /^\/logs\/([^/]+)$/.exec(url.pathname);
  if (!m || !isRunId(m[1]) || !secret) return null;
  const exp = Number(url.searchParams.get("exp"));
  const sig = url.searchParams.get("sig") ?? "";
  if (!Number.isInteger(exp) || exp < nowSec || !/^[0-9a-f]{64}$/.test(sig)) return null;
  const bytes = new Uint8Array(32);
  for (let i = 0; i < 32; i++) bytes[i] = parseInt(sig.slice(i * 2, i * 2 + 2), 16);
  const ok = await crypto.subtle.verify("HMAC", await hmacKey(secret, "verify"), bytes, msg(m[1], exp));
  return ok ? m[1] : null;
}
