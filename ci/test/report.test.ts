import { test } from "node:test";
import assert from "node:assert/strict";
import { keepTail, readTail, sameSecret } from "../src/report.ts";

const body = (chunks: Array<string | Uint8Array>) => {
  let i = 0;
  const stream = new ReadableStream({
    pull(c) {
      if (i === chunks.length) return c.close();
      const x = chunks[i++];
      c.enqueue(typeof x === "string" ? new TextEncoder().encode(x) : x);
    },
  });
  return new Request("http://ci.internal/log", { method: "POST", body: stream, duplex: "half" } as RequestInit);
};

test("keepTail: short logs pass through, long ones keep the tail and stay within keep", () => {
  assert.equal(keepTail("abc", 100), "abc");
  const t = keepTail("x".repeat(500) + "END", 100);
  assert.equal(t.length, 100);
  assert.match(t, /^\[log truncated to its last 100 characters\]\n/);
  assert.ok(t.endsWith("END"));
  assert.equal(keepTail(t, 100), t, "idempotent");
});

test("readTail: keeps only the tail while streaming, across many chunks", async () => {
  const chunks = Array.from({ length: 200 }, (_, i) => `line ${String(i).padStart(3, "0")}\n`);
  const t = (await readTail(body(chunks), 1 << 20, 100))!;
  assert.equal(t.length, 100);
  assert.ok(t.endsWith("line 199\n"));
  assert.equal(await readTail(body(["short"]), 1 << 20, 100), "short");
  assert.equal(await readTail(body(["0123456789".repeat(10)]), 1 << 20, 100), "0123456789".repeat(10), "exactly keep is not truncated");
});

test("readTail: one chunk longer than the tail is cut, and the result is marked", async () => {
  const t = (await readTail(body(["x".repeat(1000) + "END"]), 1 << 20, 100))!;
  assert.equal(t.length, 100);
  assert.match(t, /^\[log truncated/);
  assert.ok(t.endsWith("END"));
  const u = (await readTail(body(["y".repeat(500), "z".repeat(60)]), 1 << 20, 200))!;
  assert.equal(u.length, 200);
  assert.ok(u.endsWith("z".repeat(60)));
});

test("keepTail: never starts the tail on half of a surrogate pair", () => {
  const m = "[log truncated to its last 100 characters]\n".length;
  for (let shift = 0; shift < 2; shift++) {
    const log = "a".repeat(shift) + "😀".repeat(200);
    const t = keepTail(log, 100);
    const first = t.charCodeAt(m);
    assert.ok(!(first >= 0xdc00 && first <= 0xdfff), `shift ${shift}`);
    assert.ok(t.length <= 100);
  }
});

test("readTail: a multi-byte character split across chunks decodes intact", async () => {
  const euro = new TextEncoder().encode("a€b"); // € is 3 bytes
  const t = await readTail(body([euro.slice(0, 2), euro.slice(2)]), 1 << 20, 100);
  assert.equal(t, "a€b");
});

test("readTail: over the byte cap is refused, by Content-Length or by count", async () => {
  assert.equal(await readTail(body(["x".repeat(60), "y".repeat(60)]), 100, 50), null);
  const declared = new Request("http://ci.internal/log", { method: "POST", body: "x", headers: { "content-length": "101" } });
  assert.equal(await readTail(declared, 100, 50), null);
});

test("sameSecret: equal only for identical non-empty strings", () => {
  const a = "ab".repeat(32);
  assert.equal(sameSecret(a, a), true);
  assert.equal(sameSecret(a, a.slice(0, -1) + "c"), false);
  assert.equal(sameSecret("c" + a.slice(1), a), false);
  assert.equal(sameSecret(a, a + "0"), false);
  assert.equal(sameSecret("", ""), false);
});
