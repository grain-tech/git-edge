import { test } from "node:test";
import assert from "node:assert/strict";
import { egressDecision } from "../src/egress.ts";

const REPO = "owner/git-edge";
const d = (method: string, url: string) => egressDecision(method, new URL(url), REPO);

test("github.com: only fetch (git-upload-pack) of this repository, with auth", () => {
  assert.deepEqual(d("GET", "https://github.com/owner/git-edge.git/info/refs?service=git-upload-pack"), { allow: true, addAuth: true });
  assert.deepEqual(d("POST", "https://github.com/owner/git-edge.git/git-upload-pack"), { allow: true, addAuth: true });
  assert.deepEqual(d("POST", "https://github.com/owner/git-edge/git-upload-pack"), { allow: true, addAuth: true });
});

test("github.com: push, other repositories, web pages and plain http are denied", () => {
  assert.equal(d("GET", "https://github.com/owner/git-edge.git/info/refs?service=git-receive-pack").allow, false);
  assert.equal(d("POST", "https://github.com/owner/git-edge.git/git-receive-pack").allow, false);
  assert.equal(d("POST", "https://github.com/owner/other.git/git-upload-pack").allow, false);
  assert.equal(d("POST", "https://github.com/owner/git-edge-evil.git/git-upload-pack").allow, false);
  assert.equal(d("GET", "https://github.com/owner/git-edge").allow, false);
  assert.equal(d("GET", "http://github.com/owner/git-edge.git/info/refs?service=git-upload-pack").allow, false);
});

test("package registries: GET only, no auth", () => {
  assert.deepEqual(d("GET", "https://index.crates.io/se/rd/serde"), { allow: true, addAuth: false });
  assert.deepEqual(d("GET", "https://static.crates.io/crates/serde/serde-1.0.0.crate"), { allow: true, addAuth: false });
  assert.deepEqual(d("GET", "https://registry.npmjs.org/wrangler"), { allow: true, addAuth: false });
  assert.equal(d("PUT", "https://registry.npmjs.org/wrangler").allow, false);
  assert.equal(d("POST", "https://index.crates.io/x").allow, false);
});

test("every other host is denied", () => {
  for (const u of ["https://api.github.com/user", "https://example.com/", "https://objects.githubusercontent.com/x"]) {
    assert.equal(d("GET", u).allow, false, u);
  }
});
