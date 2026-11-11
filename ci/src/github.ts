// GitHub App auth and the Checks API calls. The App key never leaves the
// Worker: the container gets no GitHub credential.

const API = "https://api.github.com";
const UA = "git-edge-ci";

function b64url(bytes: Uint8Array | string): string {
  const raw = typeof bytes === "string" ? bytes : String.fromCharCode(...bytes);
  return btoa(raw).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

function derLength(n: number): number[] {
  if (n < 0x80) return [n];
  const out: number[] = [];
  while (n > 0) { out.unshift(n & 0xff); n >>= 8; }
  return [0x80 | out.length, ...out];
}

function der(tag: number, body: Uint8Array): Uint8Array {
  const len = derLength(body.length);
  const out = new Uint8Array(1 + len.length + body.length);
  out[0] = tag;
  out.set(len, 1);
  out.set(body, 1 + len.length);
  return out;
}

function concat(...parts: Uint8Array[]): Uint8Array {
  const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
  let o = 0;
  for (const p of parts) { out.set(p, o); o += p.length; }
  return out;
}

// version 0, AlgorithmIdentifier { rsaEncryption, NULL }
const PKCS8_RSA_PREFIX = new Uint8Array([
  0x02, 0x01, 0x00,
  0x30, 0x0d, 0x06, 0x09, 0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x01, 0x05, 0x00,
]);

/** GitHub issues App keys as PKCS#1 ("BEGIN RSA PRIVATE KEY"). WebCrypto
 * imports PKCS#8 only, so wrap a PKCS#1 key; pass a PKCS#8 key through. */
export function pemToPkcs8(pem: string): Uint8Array {
  const m = /-----BEGIN (RSA )?PRIVATE KEY-----([\s\S]+?)-----END (RSA )?PRIVATE KEY-----/.exec(pem.trim());
  if (!m) throw new Error("App private key is not a PEM private key");
  const bin = atob(m[2].replace(/\s+/g, ""));
  const body = Uint8Array.from(bin, (c) => c.charCodeAt(0));
  if (!m[1]) return body;
  return der(0x30, concat(PKCS8_RSA_PREFIX, der(0x04, body)));
}

export async function appJwt(appId: string, pem: string, nowSec: number): Promise<string> {
  const key = await crypto.subtle.importKey(
    "pkcs8", pemToPkcs8(pem), { name: "RSASSA-PKCS1-v1_5", hash: "SHA-256" }, false, ["sign"],
  );
  const header = b64url(JSON.stringify({ alg: "RS256", typ: "JWT" }));
  // iat 60 s in the past absorbs clock drift; GitHub allows at most 10 min.
  const claims = b64url(JSON.stringify({ iat: nowSec - 60, exp: nowSec + 540, iss: appId }));
  const input = `${header}.${claims}`;
  const sig = new Uint8Array(await crypto.subtle.sign("RSASSA-PKCS1-v1_5", key, new TextEncoder().encode(input)));
  return `${input}.${b64url(sig)}`;
}

async function gh(path: string, token: string, init: RequestInit = {}): Promise<Response> {
  const res = await fetch(`${API}${path}`, {
    ...init,
    headers: {
      accept: "application/vnd.github+json",
      authorization: `Bearer ${token}`,
      "user-agent": UA,
      "x-github-api-version": "2022-11-28",
      ...(init.body ? { "content-type": "application/json" } : {}),
    },
  });
  if (!res.ok) throw new Error(`GitHub ${init.method ?? "GET"} ${path}: ${res.status} ${(await res.text()).slice(0, 300)}`);
  return res;
}

/** An installation token scoped to one repository with only the two
 * permissions CI needs, even if the App is later granted more. */
export async function installationToken(
  appId: string, pem: string, installationId: number, repo: string, nowSec: number,
): Promise<string> {
  const jwt = await appJwt(appId, pem, nowSec);
  const res = await gh(`/app/installations/${installationId}/access_tokens`, jwt, {
    method: "POST",
    body: JSON.stringify({ repositories: [repo.split("/")[1]], permissions: { checks: "write", contents: "read" } }),
  });
  const json = (await res.json()) as { token?: string };
  if (!json.token) throw new Error("GitHub returned no installation token");
  return json.token;
}

export async function createCheckRun(token: string, repo: string, payload: object): Promise<number> {
  const res = await gh(`/repos/${repo}/check-runs`, token, { method: "POST", body: JSON.stringify(payload) });
  const json = (await res.json()) as { id?: number };
  if (typeof json.id !== "number") throw new Error("GitHub returned no check run id");
  return json.id;
}

export async function updateCheckRun(token: string, repo: string, id: number, payload: object): Promise<void> {
  await gh(`/repos/${repo}/check-runs/${id}`, token, { method: "PATCH", body: JSON.stringify(payload) });
}
