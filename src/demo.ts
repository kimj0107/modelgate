import { createFakeProvider } from "./fake-provider.js";
import { createGateway } from "./gateway.js";
import { close, listen } from "./lifecycle.js";

const fake = createFakeProvider();
let gateway: ReturnType<typeof createGateway> | undefined;
try {
  const origin = await listen(fake);
  gateway = createGateway({
    providers: { openai: { endpoint: `${origin}/openai` }, anthropic: { endpoint: `${origin}/anthropic` } },
    log: record => console.log(JSON.stringify(record)),
  });
  const url = await listen(gateway.server, process.argv.includes("--smoke") ? 0 : 8787);
  console.log(`Local ModelGate demo: ${url}/v1/stream`);
  if (process.argv.includes("--smoke")) {
    for (const provider of ["openai", "anthropic"]) {
      const response = await fetch(`${url}/v1/stream`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ provider, model: "local-demo", messages: [{ role: "user", content: "hello" }] }),
        signal: AbortSignal.timeout(5000),
      });
      const body = await response.text();
      if (!response.ok || !body.includes("event: done") || body.includes("event: error") || !body.includes("🌍")) {
        throw new Error("Demo smoke failed");
      }
      console.log(`${provider}: start, deltas, done verified`);
    }
  } else {
    await new Promise<void>(resolve => {
      const stop = (): void => {
        process.off("SIGINT", stop);
        process.off("SIGTERM", stop);
        resolve();
      };
      process.once("SIGINT", stop);
      process.once("SIGTERM", stop);
    });
  }
} finally {
  if (gateway) await close(gateway.server);
  await close(fake);
}
