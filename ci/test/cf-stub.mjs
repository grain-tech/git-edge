// Test-only stand-in for the `cloudflare:workers` module, so node --test can
// import src/index.ts. Registered by test/register.mjs.
export class DurableObject {
  constructor(ctx, env) { this.ctx = ctx; this.env = env; }
}
export class WorkerEntrypoint {
  constructor(ctx, env) { this.ctx = ctx; this.env = env; }
}
