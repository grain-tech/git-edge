// Outbound policy for the CI container. The container starts with
// enableInternet: false. Only the hosts below get an intercept, so only they
// resolve in DNS. Each intercept runs this policy before it forwards.

export const ALLOWED_HTTPS_HOSTS = [
  "github.com", // git clone of this repository only, read-only
  "index.crates.io", // cargo sparse index, GET only
  "static.crates.io", // crate downloads, GET only
  "registry.npmjs.org", // npm ci when the lockfile is not in the image cache, GET only
] as const;

export type EgressDecision =
  | { allow: false; status: number; reason: string }
  | { allow: true; addAuth: boolean };

/** Decides one outbound request. `repo` is owner/name. Git smart HTTP needs
 * GET info/refs?service=git-upload-pack and POST git-upload-pack. Nothing
 * else reaches github.com, so a push (git-receive-pack) never leaves. */
export function egressDecision(method: string, url: URL, repo: string): EgressDecision {
  if (url.protocol !== "https:") return { allow: false, status: 403, reason: "https only" };
  const host = url.hostname.toLowerCase();
  if (host === "github.com") {
    const base = `/${repo}`.toLowerCase();
    const path = url.pathname.toLowerCase().replace(/\.git(?=\/)/, "");
    if (method === "GET" && path === `${base}/info/refs` && url.searchParams.get("service") === "git-upload-pack") {
      return { allow: true, addAuth: true };
    }
    if (method === "POST" && path === `${base}/git-upload-pack`) return { allow: true, addAuth: true };
    return { allow: false, status: 403, reason: "github.com: only git-upload-pack for this repository" };
  }
  if ((ALLOWED_HTTPS_HOSTS as readonly string[]).includes(host)) {
    if (method === "GET" || method === "HEAD") return { allow: true, addAuth: false };
    return { allow: false, status: 405, reason: `${host}: GET only` };
  }
  return { allow: false, status: 403, reason: `${host}: not allowed` };
}
