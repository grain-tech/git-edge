import { test } from "node:test";
import assert from "node:assert/strict";
import { createPublicKey, generateKeyPairSync, verify } from "node:crypto";
import { appJwt, createCheckRun, installationToken, pemToPkcs8, updateCheckRun } from "../src/github.ts";

const { privateKey } = generateKeyPairSync("rsa", { modulusLength: 2048 });
const pkcs1 = privateKey.export({ type: "pkcs1", format: "pem" }).toString();
const pkcs8 = privateKey.export({ type: "pkcs8", format: "pem" }).toString();
const pkcs8Der = privateKey.export({ type: "pkcs8", format: "der" });

test("pemToPkcs8 wraps GitHub's PKCS#1 key into the same PKCS#8 bytes", () => {
  assert.ok(pkcs1.includes("BEGIN RSA PRIVATE KEY"));
  assert.deepEqual(Buffer.from(pemToPkcs8(pkcs1)), pkcs8Der);
  assert.deepEqual(Buffer.from(pemToPkcs8(pkcs8)), pkcs8Der);
  assert.throws(() => pemToPkcs8("not a key"));
});

test("pemToPkcs8 handles a 4096-bit key (long DER lengths)", () => {
  const big = generateKeyPairSync("rsa", { modulusLength: 4096 }).privateKey;
  assert.deepEqual(
    Buffer.from(pemToPkcs8(big.export({ type: "pkcs1", format: "pem" }).toString())),
    big.export({ type: "pkcs8", format: "der" }),
  );
});

test("appJwt is an RS256 JWT the App's public key verifies", async () => {
  const jwt = await appJwt("12345", pkcs1, 1_000_000);
  const [h, c, s] = jwt.split(".");
  const dec = (x: string) => JSON.parse(Buffer.from(x, "base64url").toString());
  assert.deepEqual(dec(h), { alg: "RS256", typ: "JWT" });
  assert.deepEqual(dec(c), { iat: 999_940, exp: 1_000_540, iss: "12345" });
  const ok = verify("sha256", Buffer.from(`${h}.${c}`), createPublicKey(privateKey), Buffer.from(s, "base64url"));
  assert.equal(ok, true);
});

function mockFetch(responses: Array<{ status: number; body: unknown }>) {
  const calls: Array<{ url: string; method: string; headers: Record<string, string>; body: unknown }> = [];
  const orig = globalThis.fetch;
  globalThis.fetch = (async (url: string, init: RequestInit) => {
    calls.push({
      url: String(url), method: init.method ?? "GET",
      headers: init.headers as Record<string, string>,
      body: init.body ? JSON.parse(String(init.body)) : undefined,
    });
    const r = responses.shift()!;
    return new Response(JSON.stringify(r.body), { status: r.status });
  }) as typeof fetch;
  return { calls, restore: () => { globalThis.fetch = orig; } };
}

test("installationToken asks for one repository and checks:write + contents:read only", async () => {
  const m = mockFetch([{ status: 201, body: { token: "ghs_x" } }]);
  try {
    assert.equal(await installationToken("1", pkcs1, 99, "owner/git-edge", 1_000_000), "ghs_x");
    assert.equal(m.calls[0].url, "https://api.github.com/app/installations/99/access_tokens");
    assert.equal(m.calls[0].method, "POST");
    assert.deepEqual(m.calls[0].body, { repositories: ["git-edge"], permissions: { checks: "write", contents: "read" } });
    assert.match(m.calls[0].headers.authorization, /^Bearer ey/);
  } finally { m.restore(); }
});

test("check run create and update hit the Checks API and surface errors", async () => {
  const m = mockFetch([{ status: 201, body: { id: 5 } }, { status: 200, body: {} }, { status: 422, body: { message: "bad" } }]);
  try {
    assert.equal(await createCheckRun("t", "owner/git-edge", { name: "git-edge-ci" }), 5);
    assert.equal(m.calls[0].url, "https://api.github.com/repos/owner/git-edge/check-runs");
    await updateCheckRun("t", "owner/git-edge", 5, { status: "completed" });
    assert.equal(m.calls[1].url, "https://api.github.com/repos/owner/git-edge/check-runs/5");
    assert.equal(m.calls[1].method, "PATCH");
    await assert.rejects(updateCheckRun("t", "owner/git-edge", 5, {}), /422/);
  } finally { m.restore(); }
});
