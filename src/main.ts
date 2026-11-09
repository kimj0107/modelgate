import { createGateway } from "./gateway.js";
import type { ProviderConfig, ProviderName } from "./providers.js";
import { close, listen } from "./lifecycle.js";

const providers: Partial<Record<ProviderName, ProviderConfig>> = {};
for (const name of ["openai", "anthropic"] as const) {
  const prefix = name.toUpperCase();
  const endpoint = process.env[`${prefix}_ENDPOINT`];
  if (endpoint) {
    providers[name] = { endpoint };
    const key = process.env[`${prefix}_API_KEY`];
    if (key) providers[name].apiKey = key;
  }
}
const port = Number(process.env.PORT ?? 8787);
if (!Number.isInteger(port) || port < 1 || port > 65535) throw new Error("Invalid PORT");
if (!Object.keys(providers).length) throw new Error("Configure a provider ENDPOINT, or use npm run demo");
const { server } = createGateway({ providers, log: record => console.log(JSON.stringify(record)) });
await listen(server, port);
console.log(`ModelGate listening on 127.0.0.1:${port}; localhost prototype only`);
let stopping = false;
const stop = (): void => {
  if (stopping) return;
  stopping = true;
  void close(server).catch(() => { process.exitCode = 1; });
};
process.once("SIGINT", stop);
process.once("SIGTERM", stop);
