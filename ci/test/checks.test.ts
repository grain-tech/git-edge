import { test } from "node:test";
import assert from "node:assert/strict";
import { CHECK_NAME, completePayload, conclude, createPayload, parseGates, rejectPayload } from "../src/checks.ts";

const ALL_PASS = [
  "::gate:: cargo pass 40s",
  "::gate:: harness-barrier-0 pass 160s",
  "::gate:: harness-barrier-1 pass 150s",
  "::gate:: tla pass 70s",
].join("\n");

test("parseGates reads only exact ::gate:: lines", () => {
  const g = parseGates(`noise\n${ALL_PASS}\n  ::gate:: fake pass 1s\n::gate:: tla pass\n`);
  assert.deepEqual(g.map((x) => x.name), ["cargo", "harness-barrier-0", "harness-barrier-1", "tla"]);
  assert.equal(g[1].seconds, 160);
});

test("conclude: success needs exit 0 and all four gates passing", () => {
  assert.equal(conclude(0, parseGates(ALL_PASS)).conclusion, "success");
  assert.equal(conclude(1, parseGates(ALL_PASS)).conclusion, "failure", "non-zero exit");
  const oneFail = ALL_PASS.replace("tla pass", "tla fail");
  assert.equal(conclude(0, parseGates(oneFail)).conclusion, "failure", "a failed gate");
  const noTla = ALL_PASS.split("\n").slice(0, 3).join("\n");
  const c = conclude(0, parseGates(noTla));
  assert.equal(c.conclusion, "failure", "a missing gate");
  assert.deepEqual(c.missing, ["tla"]);
  assert.equal(conclude(0, []).conclusion, "failure", "no gate lines at all");
});

test("createPayload starts the named check in progress with the log link", () => {
  const p = createPayload("b".repeat(40), "https://ci.example/logs/x", "push main", new Date(0));
  assert.equal(p.name, CHECK_NAME);
  assert.equal(p.name, "git-edge-ci");
  assert.equal(p.head_sha, "b".repeat(40));
  assert.equal(p.status, "in_progress");
  assert.equal(p.details_url, "https://ci.example/logs/x");
  assert.equal(p.started_at, "1970-01-01T00:00:00.000Z");
});

test("rejectPayload completes the check as a failure with the reason", () => {
  const p = rejectPayload("c".repeat(40), "fork", new Date(0));
  assert.equal(p.name, CHECK_NAME);
  assert.equal(p.status, "completed");
  assert.equal(p.conclusion, "failure");
  assert.equal(p.output.summary, "fork");
});

test("completePayload: titles, gate table and a log tail under GitHub's cap", () => {
  const ok = completePayload({
    conclusion: "success", gates: parseGates(ALL_PASS), missing: [], log: ALL_PASS,
    detailsUrl: "u", now: new Date(0),
  });
  assert.equal(ok.status, "completed");
  assert.equal(ok.output.title, "All gates passed");
  assert.match(ok.output.summary, /\| tla \| pass \| 70s \|/);

  const huge = "x".repeat(100) + "\n" + "y".repeat(200_000);
  const bad = completePayload({
    conclusion: "failure", gates: parseGates("::gate:: cargo fail 3s"), missing: ["tla"],
    log: huge, detailsUrl: "u", now: new Date(0),
  });
  assert.equal(bad.output.title, "Failed: cargo");
  assert.match(bad.output.summary, /\| tla \| not run \| \|/);
  assert.ok(bad.output.text.length <= 65_535, `text is ${bad.output.text.length} chars`);

  const t = completePayload({
    conclusion: "timed_out", gates: [], missing: [], log: "", detailsUrl: "u", note: "Timed out after 75 minutes", now: new Date(0),
  });
  assert.equal(t.conclusion, "timed_out");
  assert.equal(t.output.title, "Timed out");
});
