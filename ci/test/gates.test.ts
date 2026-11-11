// The harness gate's count check in ci/gates.sh, run through its helper modes.
import { test } from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const gates = fileURLToPath(new URL("../gates.sh", import.meta.url).href);
const repo = fileURLToPath(new URL("../..", import.meta.url).href);
const dir = mkdtempSync(join(tmpdir(), "gates-test-"));

function check(summary: string, want: string) {
  const f = join(dir, `out-${Math.random()}`);
  writeFileSync(f, `test_a (m.T.test_a) ... ok\n${"-".repeat(70)}\n${summary}`);
  const r = spawnSync("bash", [gates, "--check-harness-output", f, want], { encoding: "utf8" });
  return { ok: r.status === 0, out: r.stdout + r.stderr };
}

test("gates: the expected harness count is derived from the test modules and meets the minimum", () => {
  const r = spawnSync("bash", [gates, "--harness-expected", repo], { encoding: "utf8" });
  assert.equal(r.status, 0, r.stderr);
  assert.ok(Number(r.stdout.trim()) >= 97, r.stdout);
});

test("gates: a harness run passes only with every expected test run and a bare OK", () => {
  assert.ok(check("Ran 97 tests in 12.3s\n\nOK\n", "97").ok);
  assert.ok(check("Ran 98 tests in 12.3s\n\nOK\n", "98").ok);
  for (const [summary, want, why] of [
    ["Ran 96 tests in 12.3s\n\nOK\n", "97", /ran 96 of 97/],
    ["Ran 97 tests in 12.3s\n\nOK (skipped=1)\n", "97", /bare OK/],
    ["Ran 97 tests in 12.3s\n\nOK (expected failures=1)\n", "97", /bare OK/],
    ["Ran 97 tests in 12.3s\n\nFAILED (errors=1)\n", "97", /bare OK/],
    ["Ran 90 tests in 12.3s\n\nOK\n", "90", /fewer than the minimum/],
    ["OK\n", "97", /no 'Ran N tests'/],
    ["Ran 97 tests in 12.3s\n\nOK\n", "", /no expected count/],
    ["Ran 97 tests in 12.3s\n\nFAILED (failures=1)\nOK\n", "97", /bare OK/],
    ["Ran 97 tests in 12.3s\n\nFAILED (failures=1)\nRan 97 tests in 1s\n\nOK\n", "97", /exactly one/],
  ] as const) {
    const r = check(summary, want);
    assert.equal(r.ok, false, summary);
    assert.match(r.out, why);
  }
});
