// Module hooks that let the worker-build output run under plain Node:
// `cloudflare:workers` resolves to a WorkerEntrypoint shim, and the emitted
// `*.wasm` import becomes a module whose default export is the compiled
// WebAssembly.Module.
import { registerHooks } from 'node:module';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

const SHIM = `export class WorkerEntrypoint {
  constructor(ctx, env) {
    this.ctx = ctx;
    this.env = env;
  }
}
`;

registerHooks({
  resolve(specifier, context, nextResolve) {
    if (specifier === 'cloudflare:workers') {
      return { url: 'cloudflare:workers', shortCircuit: true };
    }
    return nextResolve(specifier, context);
  },
  load(url, context, nextLoad) {
    if (url === 'cloudflare:workers') {
      return { format: 'module', source: SHIM, shortCircuit: true };
    }
    if (url.endsWith('.wasm')) {
      const path = fileURLToPath(url);
      const source = `import { readFileSync } from 'node:fs';
export default new WebAssembly.Module(readFileSync(${JSON.stringify(path)}));`;
      return { format: 'module', source, shortCircuit: true };
    }
    return nextLoad(url, context);
  },
});
