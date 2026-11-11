// Pure helpers for the container's reports to http://ci.internal: the run
// nonce check and a bounded read of the log body. No Workers runtime needed,
// so the tests run them directly.

/** The log kept in R2 and handed to the Durable Object, in characters. */
export const MAX_LOG_CHARS = 10 * 1024 * 1024;

/** A report body over this many bytes is refused. readTail keeps only the
 * tail, so memory stays near MAX_LOG_CHARS whatever the body size; the cap
 * only bounds how long one report can stream. */
export const MAX_REPORT_BYTES = 256 * 1024 * 1024;

const marker = (keep: number) => `[log truncated to its last ${keep} characters]\n`;

/** The last `keep` characters of log, marker included. Idempotent. */
export function keepTail(log: string, keep = MAX_LOG_CHARS): string {
  if (log.length <= keep) return log;
  const m = marker(keep);
  return m + log.slice(log.length - (keep - m.length));
}

/** Reads a body, decoding as it streams, and keeps only its tail (see
 * keepTail). Whole decoded pieces are dropped from the front once the rest
 * already covers `keep`, so the peak is about two tails, never the whole body.
 * Null when the body is over `cap` bytes, by Content-Length or by count. */
export async function readTail(req: Request, cap = MAX_REPORT_BYTES, keep = MAX_LOG_CHARS): Promise<string | null> {
  if (Number(req.headers.get("content-length") ?? "0") > cap) return null;
  if (!req.body) return "";
  const reader = req.body.getReader();
  const dec = new TextDecoder();
  const parts: string[] = [];
  let chars = 0;
  let bytes = 0;
  const add = (s: string) => {
    if (!s) return;
    parts.push(s);
    chars += s.length;
    while (parts.length > 1 && chars - parts[0].length > keep) chars -= parts.shift()!.length;
  };
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    bytes += value.byteLength;
    if (bytes > cap) {
      await reader.cancel();
      return null;
    }
    add(dec.decode(value, { stream: true }));
  }
  add(dec.decode());
  return keepTail(parts.join(""), keep);
}

const enc = new TextEncoder();

/** Compares two secrets in time that does not depend on where they differ.
 * Only the length can leak, and the run nonce has a fixed length. */
export function sameSecret(a: string, b: string): boolean {
  const x = enc.encode(a);
  const y = enc.encode(b);
  if (x.length !== y.length || x.length === 0) return false;
  let d = 0;
  for (let i = 0; i < x.length; i++) d |= x[i] ^ y[i];
  return d === 0;
}
