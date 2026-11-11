// Webhook intake: signature check and the decision whether an event runs CI.
// Pure functions only, so the tests run them without a Workers runtime.

const enc = new TextEncoder();

function hexToBytes(hex: string): Uint8Array | null {
  if (hex.length % 2 !== 0 || !/^[0-9a-f]*$/i.test(hex)) return null;
  const out = new Uint8Array(hex.length / 2);
  for (let i = 0; i < out.length; i++) out[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  return out;
}

/** Checks `X-Hub-Signature-256: sha256=<hex>` over the raw body with
 * HMAC-SHA256. crypto.subtle.verify compares in constant time. */
export async function verifySignature(
  secret: string,
  body: ArrayBuffer | Uint8Array,
  header: string | null,
): Promise<boolean> {
  if (!secret || !header || !header.startsWith("sha256=")) return false;
  const sig = hexToBytes(header.slice("sha256=".length));
  if (!sig || sig.length !== 32) return false;
  const key = await crypto.subtle.importKey(
    "raw", enc.encode(secret), { name: "HMAC", hash: "SHA-256" }, false, ["verify"],
  );
  return crypto.subtle.verify("HMAC", key, sig, body);
}

/** One CI run: the commit to test and how to fetch it. */
export interface RunPlan {
  repo: string; // owner/name
  sha: string; // 40-hex head commit
  fetchRef: string; // ref that contains sha, for the clone fallback
  installationId: number;
  trigger: string; // human label, e.g. "pull_request #12" or "push main"
}

/** A run plan, or the reason the event starts nothing. `reject` means the
 * event is for this repository but must get a failed check without a run. */
export type Decision =
  | { kind: "run"; plan: RunPlan }
  | { kind: "reject"; plan: RunPlan; reason: string }
  | { kind: "ignore"; reason: string };

const PR_ACTIONS = new Set(["opened", "synchronize", "reopened"]);
const SHA_RE = /^[0-9a-f]{40}$/;
const ZERO_SHA = "0".repeat(40);

type Json = any;

export function decide(event: string | null, payload: Json, allowedRepo: string): Decision {
  if (!payload || typeof payload !== "object") return { kind: "ignore", reason: "no payload" };
  const repo: unknown = payload.repository?.full_name;
  if (typeof repo !== "string" || !allowedRepo || repo.toLowerCase() !== allowedRepo.toLowerCase()) {
    return { kind: "ignore", reason: "repository not allowed" };
  }
  const installationId = payload.installation?.id;
  if (typeof installationId !== "number") return { kind: "ignore", reason: "no installation id" };

  if (event === "pull_request") {
    if (!PR_ACTIONS.has(payload.action)) return { kind: "ignore", reason: `pull_request action ${payload.action}` };
    const pr = payload.pull_request;
    const sha: unknown = pr?.head?.sha;
    const num: unknown = pr?.number;
    if (typeof sha !== "string" || !SHA_RE.test(sha) || typeof num !== "number") {
      return { kind: "ignore", reason: "malformed pull_request" };
    }
    const plan: RunPlan = {
      repo, sha, fetchRef: `refs/pull/${num}/head`, installationId, trigger: `pull_request #${num}`,
    };
    const headRepo: unknown = pr?.head?.repo?.full_name;
    if (typeof headRepo !== "string" || headRepo.toLowerCase() !== repo.toLowerCase()) {
      return { kind: "reject", plan, reason: "Pull requests from forks do not run automatically. Push the branch to this repository." };
    }
    return { kind: "run", plan };
  }

  if (event === "push") {
    const branch: unknown = payload.repository?.default_branch;
    const sha: unknown = payload.after;
    if (typeof branch !== "string" || payload.ref !== `refs/heads/${branch}`) {
      return { kind: "ignore", reason: "push not to the default branch" };
    }
    if (payload.deleted === true || typeof sha !== "string" || !SHA_RE.test(sha) || sha === ZERO_SHA) {
      return { kind: "ignore", reason: "push without a new head" };
    }
    return { kind: "run", plan: { repo, sha, fetchRef: payload.ref, installationId, trigger: `push ${branch}` } };
  }

  return { kind: "ignore", reason: `event ${event}` };
}
