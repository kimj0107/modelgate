#!/usr/bin/env node
const args = process.argv.slice(2);
const help = `ModelGate — local SSE normalization gateway (Node 24+)

Usage:
  modelgate                        Start gateway on 127.0.0.1:8787
  modelgate --demo                 Start gateway + deterministic local providers
  modelgate --demo --smoke         Verify both local providers, then stop
  modelgate --help                 Show this help

Server environment:
  PORT                            Local port (default 8787)
  OPENAI_ENDPOINT                 Full OpenAI-style SSE endpoint
  OPENAI_API_KEY                   Optional server-side credential
  ANTHROPIC_ENDPOINT               Full Anthropic-style SSE endpoint
  ANTHROPIC_API_KEY                Optional server-side credential

POST /v1/stream accepts ModelGate's protocol, NOT /v1/chat/completions.
No authentication or quotas: localhost use only. No external calls in --demo.
`;

if (args.length === 1 && (args[0] === "--help" || args[0] === "-h")) {
  process.stdout.write(help);
} else if (args.some(arg => arg !== "--demo" && arg !== "--smoke")
  || new Set(args).size !== args.length || (args.includes("--smoke") && !args.includes("--demo"))) {
  process.stderr.write("Invalid arguments. Run modelgate --help.\n");
  process.exitCode = 2;
} else {
  try {
    if (args.includes("--demo")) await import("./demo.js");
    else await import("./main.js");
  } catch {
    // Startup failures may contain provider URLs or credentials; never print raw errors.
    process.stderr.write("ModelGate startup/smoke failed. Check local configuration and port availability.\n");
    process.exitCode = 1;
  }
}
