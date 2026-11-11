// Static checks on the container entrypoint. The image cannot run here, so
// these pin the two properties the result's integrity rests on.
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const src = readFileSync(fileURLToPath(new URL("../container/entry.sh", import.meta.url).href), "utf8");
const code = src.split("\n").filter((l) => !l.trimStart().startsWith("#"));

test("the nonce never reaches a command line", () => {
  const uses = code.filter((l) => l.includes("CI_NONCE"));
  assert.deepEqual(uses.map((l) => l.trim()), [
    ': "${CI_REPO:?}" "${CI_SHA:?}" "${CI_FETCH_REF:?}" "${CI_RUN_ID:?}" "${CI_NONCE:?}" "${CI_REPORT_URL:?}"',
    "printf 'x-ci-nonce: %s\\n' \"$CI_NONCE\" >\"$HDR\"",
    "unset CI_NONCE",
  ]);
  assert.ok(code.some((l) => l.includes('-H "@$HDR"')), "curl reads the nonce header from the root-only file");
  assert.ok(code.some((l) => l.trim() === 'chmod 600 "$HDR"'));
});

test("the clone and the gates run as ci with a cleared environment", () => {
  assert.ok(code.some((l) => l.includes("runuser -u ci -- env -i ")));
  assert.ok(code.some((l) => l.includes("as_ci /opt/ci/gates.sh /work/repo")));
  assert.ok(!code.some((l) => /chown\s+ci/.test(l)), "the log stays root-owned");
  assert.ok(code.some((l) => l.trim() === "LOG=/root/run.log"), "the log lives where ci cannot reach it");
});
