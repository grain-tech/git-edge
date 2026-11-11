import { test } from "node:test";
import assert from "node:assert/strict";
import { signLogUrl, verifyLogUrl } from "../src/logurl.ts";

const RUN = `${"d".repeat(40)}-1762826400000`;

test("a signed log link verifies until it expires", async () => {
  const u = new URL(await signLogUrl("https://ci.example", "k", RUN, 2000));
  assert.equal(u.pathname, `/logs/${RUN}`);
  assert.equal(await verifyLogUrl("k", u, 1999), RUN);
  assert.equal(await verifyLogUrl("k", u, 2001), null, "expired");
});

test("tampered, re-pointed or wrongly keyed links are refused", async () => {
  const u = new URL(await signLogUrl("https://ci.example", "k", RUN, 2000));
  assert.equal(await verifyLogUrl("other", u, 0), null, "wrong key");
  const later = new URL(u); later.searchParams.set("exp", "9999");
  assert.equal(await verifyLogUrl("k", later, 0), null, "extended expiry");
  const other = new URL(u); other.pathname = `/logs/${"e".repeat(40)}-1762826400000`;
  assert.equal(await verifyLogUrl("k", other, 0), null, "other run");
  const traversal = new URL(u); traversal.pathname = "/logs/..%2Fsecret";
  assert.equal(await verifyLogUrl("k", traversal, 0), null, "bad run id");
  const noSig = new URL(u); noSig.searchParams.delete("sig");
  assert.equal(await verifyLogUrl("k", noSig, 0), null, "no sig");
});
