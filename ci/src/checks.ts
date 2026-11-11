// GitHub check-run payloads for the required check, and the gate summary
// parsed from the gate script's `::gate::` lines.

export const CHECK_NAME = "git-edge-ci";
export const GATES = ["cargo", "harness-barrier-0", "harness-barrier-1", "tla"] as const;
const MAX_TEXT = 60_000; // GitHub caps output.text at 65,535 characters

export interface GateResult { name: string; pass: boolean; seconds: number }

export function parseGates(log: string): GateResult[] {
  const out: GateResult[] = [];
  for (const m of log.matchAll(/^::gate:: (\S+) (pass|fail) (\d+)s$/gm)) {
    out.push({ name: m[1], pass: m[2] === "pass", seconds: Number(m[3]) });
  }
  return out;
}

export type Conclusion = "success" | "failure" | "timed_out";

/** The conclusion requires the runner to exit 0 AND every required gate to
 * report pass. A missing gate line fails the check. */
export function conclude(exitCode: number, gates: GateResult[]): { conclusion: Conclusion; missing: string[] } {
  const passed = new Set(gates.filter((g) => g.pass).map((g) => g.name));
  const failedAny = gates.some((g) => !g.pass);
  const missing = GATES.filter((g) => !gates.some((r) => r.name === g));
  const ok = exitCode === 0 && !failedAny && missing.length === 0 && GATES.every((g) => passed.has(g));
  return { conclusion: ok ? "success" : "failure", missing };
}

export function createPayload(sha: string, detailsUrl: string, trigger: string, startedAt: Date) {
  return {
    name: CHECK_NAME,
    head_sha: sha,
    status: "in_progress",
    started_at: startedAt.toISOString(),
    details_url: detailsUrl,
    output: {
      title: "Running cargo, harness (barrier 0 and 1), TLA+",
      summary: `Triggered by ${trigger}. Logs: ${detailsUrl}`,
    },
  };
}

export function rejectPayload(sha: string, reason: string, now: Date) {
  return {
    name: CHECK_NAME,
    head_sha: sha,
    status: "completed",
    conclusion: "failure",
    completed_at: now.toISOString(),
    output: { title: "Not run", summary: reason },
  };
}

function tail(log: string, max: number): string {
  if (log.length <= max) return log;
  const cut = log.slice(log.length - max);
  const nl = cut.indexOf("\n");
  return nl >= 0 ? cut.slice(nl + 1) : cut;
}

export function completePayload(opts: {
  conclusion: Conclusion;
  gates: GateResult[];
  missing: string[];
  log: string;
  detailsUrl: string;
  note?: string;
  now: Date;
}) {
  const { conclusion, gates, missing, log, detailsUrl, note, now } = opts;
  const failed = gates.filter((g) => !g.pass).map((g) => g.name);
  const title = conclusion === "success"
    ? "All gates passed"
    : conclusion === "timed_out"
      ? "Timed out"
      : failed.length
        ? `Failed: ${failed.join(", ")}`
        : note ?? "Failed";
  const rows = gates.map((g) => `| ${g.name} | ${g.pass ? "pass" : "FAIL"} | ${g.seconds}s |`);
  for (const m of missing) rows.push(`| ${m} | not run | |`);
  const summary = [
    note ? `${note}\n` : "",
    "| Gate | Result | Time |",
    "|---|---|---|",
    ...rows,
    "",
    `Full log: ${detailsUrl}`,
  ].join("\n");
  const fence = "```";
  const body = tail(log, MAX_TEXT - 20);
  return {
    status: "completed",
    conclusion,
    completed_at: now.toISOString(),
    output: { title, summary, text: `${fence}\n${body}\n${fence}` },
  };
}
