import { test } from "node:test";
import assert from "node:assert/strict";
import { createHmac } from "node:crypto";
import { decide, verifySignature } from "../src/webhook.ts";

const enc = new TextEncoder();
const sign = (secret: string, body: string) => "sha256=" + createHmac("sha256", secret).update(body).digest("hex");

test("signature: GitHub's documented test vector verifies", async () => {
  // https://docs.github.com/en/webhooks/using-webhooks/validating-webhook-deliveries
  const header = "sha256=757107ea0eb2509fc211221cce984b8a37570b6d7586c22c46f4379c8b043e17";
  assert.equal(await verifySignature("It's a Secret to Everybody", enc.encode("Hello, World!"), header), true);
});

test("signature: bad, tampered, missing or malformed signatures are refused", async () => {
  const body = '{"a":1}';
  const good = sign("s3cret", body);
  assert.equal(await verifySignature("s3cret", enc.encode(body), good), true);
  assert.equal(await verifySignature("other", enc.encode(body), good), false, "wrong secret");
  assert.equal(await verifySignature("s3cret", enc.encode('{"a":2}'), good), false, "tampered body");
  assert.equal(await verifySignature("s3cret", enc.encode(body), null), false, "missing header");
  assert.equal(await verifySignature("s3cret", enc.encode(body), good.replace("sha256=", "sha1=")), false, "wrong scheme");
  assert.equal(await verifySignature("s3cret", enc.encode(body), good.slice(0, -2)), false, "short digest");
  assert.equal(await verifySignature("s3cret", enc.encode(body), "sha256=" + "zz".repeat(32)), false, "non-hex");
  assert.equal(await verifySignature("", enc.encode(body), sign("", body)), false, "empty secret never verifies");
});

const REPO = "owner/git-edge";
const SHA = "a".repeat(40);
const pr = (action: string, over: Record<string, unknown> = {}) => ({
  action,
  repository: { full_name: REPO, default_branch: "main" },
  installation: { id: 7 },
  pull_request: { number: 12, head: { sha: SHA, repo: { full_name: REPO } } },
  ...over,
});
const push = (over: Record<string, unknown> = {}) => ({
  ref: "refs/heads/main",
  after: SHA,
  deleted: false,
  repository: { full_name: REPO, default_branch: "main" },
  installation: { id: 7 },
  ...over,
});

test("events: pull_request opened, synchronize and reopened run the head sha", () => {
  for (const action of ["opened", "synchronize", "reopened"]) {
    const d = decide("pull_request", pr(action), REPO);
    assert.equal(d.kind, "run", action);
    if (d.kind !== "run") continue;
    assert.deepEqual(d.plan, {
      repo: REPO, sha: SHA, fetchRef: "refs/pull/12/head", installationId: 7, trigger: "pull_request #12",
    });
  }
});

test("events: other pull_request actions are ignored", () => {
  for (const action of ["closed", "edited", "labeled", "ready_for_review"]) {
    assert.equal(decide("pull_request", pr(action), REPO).kind, "ignore", action);
  }
});

test("events: a fork pull request gets a failed check, not a run", () => {
  const d = decide("pull_request", pr("opened", {
    pull_request: { number: 12, head: { sha: SHA, repo: { full_name: "someone/git-edge" } } },
  }), REPO);
  assert.equal(d.kind, "reject");
});

test("events: push to the default branch runs; other pushes are ignored", () => {
  const d = decide("push", push(), REPO);
  assert.equal(d.kind, "run");
  if (d.kind === "run") assert.equal(d.plan.fetchRef, "refs/heads/main");
  assert.equal(decide("push", push({ ref: "refs/heads/feature" }), REPO).kind, "ignore");
  assert.equal(decide("push", push({ ref: "refs/tags/v1" }), REPO).kind, "ignore");
  assert.equal(decide("push", push({ deleted: true, after: "0".repeat(40) }), REPO).kind, "ignore");
});

test("events: other repositories, unknown events and malformed payloads are ignored", () => {
  assert.equal(decide("pull_request", pr("opened"), "owner/other").kind, "ignore");
  assert.equal(decide("pull_request", pr("opened"), "").kind, "ignore", "unset CI_REPO serves nothing");
  assert.equal(decide("pull_request", pr("opened"), "OWNER/Git-Edge").kind, "run", "repo match ignores case");
  assert.equal(decide("check_run", pr("rerequested"), REPO).kind, "ignore");
  assert.equal(decide("issues", {}, REPO).kind, "ignore");
  assert.equal(decide("pull_request", null, REPO).kind, "ignore");
  assert.equal(decide("pull_request", pr("opened", { installation: undefined }), REPO).kind, "ignore");
  assert.equal(decide("pull_request", pr("opened", {
    pull_request: { number: 12, head: { sha: "not-a-sha", repo: { full_name: REPO } } },
  }), REPO).kind, "ignore");
});
