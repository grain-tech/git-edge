import { register } from "node:module";
register(
  "data:text/javascript," + encodeURIComponent(`
    export async function resolve(spec, ctx, next) {
      if (spec === "cloudflare:workers") return { url: new URL("./cf-stub.mjs", ${JSON.stringify(import.meta.url)}).href, shortCircuit: true };
      return next(spec, ctx);
    }`),
);
