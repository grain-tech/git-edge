import './loader.mjs';
import test from 'node:test';
import assert from 'node:assert/strict';
import { fileURLToPath, pathToFileURL } from 'node:url';
import path from 'node:path';

const buildDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../server/build');
// worker-build's index.js calls addEventListener("error", ...) at module
// load. This no-op lets the bundle load under plain Node.
globalThis.addEventListener = () => {};
const { default: Worker } = await import(pathToFileURL(path.join(buildDir, 'index.js')).href);

const TOKEN = 'ge_worker_edge_upload_pack_cap';
// Must match CMD_CAP and FILL_STEP in server/src/edge/mod.rs (the two
// consts at the top of that file). The compiled worker does not export
// them, so this test copies the values by hand.
const FILL = 64 << 10; // FILL_STEP, one pull per fill round
const CAP = 1 << 20; // CMD_CAP
const CHUNKS = 64; // 4 MiB of zeros, well over the cap
// The streaming cap refuses once body + one buffered fill step would
// exceed CMD_CAP, so at most CMD_CAP/FILL + 1 chunks can be pulled.
// The bound stays exact. A one-chunk margin would hide a handler that
// raises the cap by only one FILL_STEP. The broken cases this test must
// catch pull 33 or more (2*CMD_CAP) or all 64 (old loop or bypass).
// highWaterMark is 0, so the runtime does not prefetch a chunk.
const MAX_PULLS = CAP / FILL + 1;

const worker = new Worker({ waitUntil() {}, passThroughOnException() {} }, { GE_READ_TOKEN: TOKEN });

function chunkedUpload(chunks, headers = {}) {
  let pulled = 0;
  let closed = false;
  const stream = new ReadableStream(
    {
      pull(controller) {
        if (closed) return;
        if (pulled < chunks) {
          pulled += 1;
          controller.enqueue(new Uint8Array(FILL));
        } else {
          closed = true;
          controller.close();
        }
      },
    },
    { highWaterMark: 0 },
  );
  const req = new Request('https://test.invalid/o/r/git-upload-pack', {
    method: 'POST',
    headers: {
      authorization: `Bearer ${TOKEN}`,
      'git-protocol': 'version=2',
      ...headers,
    },
    body: stream,
    duplex: 'half',
  });
  return { req, pulled: () => pulled };
}

// pkt-line framed `command=ls-refs` + delim + flush: a valid v2 command.
const pkt = (s) => `${(s.length + 4).toString(16).padStart(4, '0')}${s}`;
const LS_REFS_BODY = pkt('command=ls-refs\n') + '0001' + '0000';

function smallUpload() {
  const stream = new ReadableStream(
    {
      start(controller) {
        controller.enqueue(new TextEncoder().encode(LS_REFS_BODY));
        controller.close();
      },
    },
    { highWaterMark: 0 },
  );
  return new Request('https://test.invalid/o/r/git-upload-pack', {
    method: 'POST',
    headers: {
      authorization: `Bearer ${TOKEN}`,
      'git-protocol': 'version=2',
    },
    body: stream,
    duplex: 'half',
  });
}

test('chunked over-cap upload-pack body is refused 413 while streaming', async () => {
  const { req, pulled } = chunkedUpload(CHUNKS);
  const resp = await worker.fetch(req);
  const text = await resp.text();
  assert.equal(resp.status, 413, `status ${resp.status}: ${text}`);
  assert.match(text, /upload-pack body/, `unexpected body: ${text}`);
  assert.ok(
    pulled() <= MAX_PULLS,
    `pulled ${pulled()} chunks, the cap must fire while streaming, not after EOF`,
  );
});

test('small valid upload-pack command body is not refused', async () => {
  const resp = await worker.fetch(smallUpload());
  await resp.arrayBuffer();
  // No REPO binding exists under Node: the request clears auth, the v2
  // parse and the body cap, then fails resolving the DO stub, a 500,
  // deterministically not the 413 the cap would give.
  assert.equal(resp.status, 500, `status ${resp.status}`);
});
