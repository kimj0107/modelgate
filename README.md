# ModelGate

A TypeScript streaming-core prototype: normalize two provider SSE dialects,
propagate downstream backpressure, and cancel upstream work when a client leaves.
Built for studying the hard boundary between a local coding tool and a streaming
model endpoint—not a model, chat UI, or SDK-compatible drop-in proxy.

**Localhost only.** No authentication, quotas, tenant isolation, or production
hardening. The CLI binds **127.0.0.1**; do not expose it through a public interface
or tunnel. Any local process can call it and spend configured provider credits.
The tests and demo require no keys and make no external model calls.

## Run / package

Requires Node **24+**, npm, and no runtime dependencies.

```powershell
npm ci
npm test
npm run check
npm run build
npm run demo
```

In another terminal:

```powershell
curl.exe -N http://127.0.0.1:8787/v1/stream `
  -H 'Content-Type: application/json' `
  --data-raw '{"provider":"openai","model":"local-demo","messages":[{"role":"user","content":"hello"}]}'
```

Use `"provider":"anthropic"` to exercise the other adapter. The deterministic
fake provider returns `Local streaming works. 🌍`; it does not infer from prompts.
Ctrl+C stops both HTTP servers. `npm run demo:smoke` exercises both and exits.

The package exposes the `modelgate` binary and an ESM `createGateway` API:

```powershell
npm pack
# From a separate consumer directory, install the resulting tarball:
npm install 'C:\path\to\modelgate\modelgate-0.1.0.tgz'
npx --no-install modelgate --help
npx --no-install modelgate --demo
```

`npm run smoke:package` packs, checks the file allowlist, installs offline into a
fresh project-local consumer, invokes the installed **npm bin shim**, runs both
demo transports, checks the public ESM import, and removes all artifacts. Only
compiled `dist/src`, declarations, README, LICENSE and package metadata ship.
This project has not been published; registry name availability is not asserted.

For a real configured transport, set `OPENAI_ENDPOINT` and/or
`ANTHROPIC_ENDPOINT` to the **full server-owned endpoint URL**; optionally set
`OPENAI_API_KEY` / `ANTHROPIC_API_KEY`. Then `npm start` or `modelgate`.
`PORT` defaults to 8787. No default external endpoints are contacted. Keys are
never accepted in request bodies. Redirects are rejected. HTTP is supported for
local fixtures; use HTTPS for real endpoints and do not put credentials in URLs.
The demo ignores external endpoint/key environment variables.

## Local coding-tool use

A CLI/editor integration can POST selected text and consume normalized events;
it must implement **ModelGate's protocol**, not point an unmodified OpenAI SDK
at this gateway. There is deliberately no claim of compatibility with any
particular coding assistant. For example, save this as `review-file.mjs` in your
own tool directory and run `node review-file.mjs .\example.ts` while the demo runs:

```js
import { readFile } from "node:fs/promises";

const source = await readFile(process.argv[2], "utf8");
const response = await fetch("http://127.0.0.1:8787/v1/stream", {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: JSON.stringify({
    provider: "openai", model: "local-demo",
    messages: [{ role: "user", content: `Suggest a small improvement:\n${source}` }],
    maxTokens: 256
  }),
  signal: AbortSignal.timeout(25_000)
});
if (!response.ok) throw new Error(`Gateway returned HTTP ${response.status}`);
let pending = "";
let terminal = false;
for await (const chunk of response.body.pipeThrough(new TextDecoderStream())) {
  pending += chunk;
  let end;
  while ((end = pending.indexOf("\n\n")) !== -1) {
    const frame = pending.slice(0, end);
    pending = pending.slice(end + 2);
    const [event, data] = frame.split("\n");
    const value = JSON.parse(data.slice(6));
    if (event === "event: delta") process.stdout.write(value.text);
    if (event === "event: error") throw new Error(`Gateway: ${value.code}`);
    if (event === "event: done") terminal = true;
  }
}
if (!terminal || pending) throw new Error("Truncated gateway stream");
process.stdout.write("\n");
```

When using real endpoints, substitute a model accepted by that endpoint.
Only send files you are authorized to share: selected content leaves your
machine for the configured provider. The default 64 KiB request limit and
16,384-character message limit intentionally reject large files; there is no
repository crawling, tool execution, automatic context collection, or agent loop.
Aborting the tool's fetch cancels upstream work.

## HTTP contract (version 1)

`POST /v1/stream`, `Content-Type: application/json`, uncompressed UTF-8 JSON.
Unknown properties are rejected, including caller-supplied URLs and credentials.

| Field | Constraints |
|---|---|
| `provider` | `openai` or `anthropic`, configured on the server |
| `model` | 1–128 ASCII letters/digits or `._:/-`; first character alphanumeric |
| `messages` | 1–32 objects containing only `role` (`user`/`assistant`) and `content` (1–16,384 JS characters) |
| `maxTokens` | Optional integer 1–4096; default 256 |

Success is `text/event-stream; charset=utf-8` with LF-delimited, single-line JSON
data. SSE contains no provider-native payloads:

```text
event: start
data: {"version":1,"requestId":"<uuid>","provider":"openai"}

event: delta
data: {"text":"Hello"}

event: done
data: {"reason":"stop"}

```

`start` is emitted after upstream HTTP status/content type validation.
Zero or more `delta`s precede one terminal `done` or `error`.
`done.reason` is `stop`, `length`, or `other`. No replay IDs, resume, usage
accounting or heartbeats are provided. Clients must treat EOF **without** a
terminal event as failure (e.g. a dropped connection). `x-request-id` matches
the start event.

Before SSE headers are sent, errors are JSON:
`{"error":{"code":"invalid_request"},"requestId":"..."}`.
HTTP statuses: 400 invalid schema/unconfigured provider, 404 route, 405 method,
413 body cap, 415 unsupported media/encoding, 502 failed/invalid provider
response, 504 deadline. Once headers are sent, failure emits only
`event: error` with `data: {"code":"invalid_upstream"}` (or
`deadline_exceeded` / `upstream_unavailable` / `internal_error`) and closes.
No raw provider status bodies, error messages, or exception stacks are returned.
Delivery of a terminal error is best-effort if the client is disconnected or
stopped reading.

## Architecture and guarantees

```text
HTTP request -> bounded JSON validation -> configured fetch transport
                                         |
client <- write/drain <- normalized text events <- bounded SSE parser
   |                                             |
   +-- disconnect / total deadline -> AbortController -> fetch + reader
```

- One fetch per accepted stream, **no retries**, even before output.
- Provider URLs come only from snapshotted server configuration. Fetch rejects
  redirects and validates status/content type without reading error bodies.
- Incremental parser supports fragmented UTF-8, BOM at stream start, LF/CRLF/lone
  CR, comments, and multiline `data:`. Invalid UTF-8/JSON, oversized events,
  incomplete frames, or EOF without a provider terminal marker fail closed.
- The default raw event cap is **32 KiB**, including line endings and comments;
  the JSON request cap is **64 KiB**, enforced with and without Content-Length.
  Complete events, not whole responses, are retained. The parser holds
  O(event cap) line/data storage plus one fetch-supplied transport chunk.
- `res.write() === false` suspends event consumption until `drain` or abort.
  The application write queue is bounded by the Node high-water mark plus one
  normalized frame (conservative frame bound: 6 × event cap + fixed protocol
  overhead), plus one small terminal error after deadline failure.
  Node fetch/HTTP and the OS have their own bounded, implementation-dependent
  buffers: this is **not** a total process-RSS or zero-prefetch guarantee.
- Request deadline defaults to **30 s**, starting when Node delivers the
  request (not TCP connect); upstream deadline defaults to **20 s**, covering
  fetch headers, body reads, and downstream waiting. Neither resets on tokens.
  Stalled incoming request bodies and stalled upstream reads are interrupted.
  Node also limits header time. A non-reading peer is destroyed after up to
  1 s of terminal-response grace. Transport failure may prevent error delivery.
- A request owns one AbortController. Disconnect, deadline, completion and
  failure release fetch readers, abort fetch, remove listeners and clear work
  timers. Shutdown force-closes active HTTP connections.
- Logs contain only `{requestId, provider, status, durationMs}`. No prompts,
  completion text, API keys, endpoint URLs or raw exceptions. Midstream failures
  use a code instead of the already-sent HTTP 200 status. The CLI's startup and
  demo messages contain only local bind information / static status text.
- `createGateway(config)` returns `{server, stats}` for embedding.
  `stats` reports active work, drain waits and peak Node writable bytes, not
  token accounting or production monitoring. Bind the returned server to
  `127.0.0.1`; an embedding application controls its own listen address.

### Supported provider subset

OpenAI-style POST sends `model`, `messages`, `max_tokens`, `stream:true`, and
optional server-side Bearer authentication. One choice (`index:0`), text deltas,
an explicit `finish_reason`, optional empty-choice usage chunks, then `[DONE]`
are supported. Tool/function calls and nonempty refusals are rejected.

Anthropic-style POST sends the same basic fields with `anthropic-version:
2023-06-01` and optional `x-api-key`. It accepts a message-start, sequential
non-overlapping text blocks, text deltas, block stops, a stop-reason
message-delta, and message-stop; ping events are ignored. Initial text in a
block is preserved. Tools, thinking, images, system prompts, citations,
structured output, multiple choices and multimodal content are not supported.
Provider schema evolution can require adapter changes.

**Only local HTTP fixture compatibility is tested**, not live provider accounts,
all OpenAI-compatible servers, or any production coding assistant.

## Validation and next work

`npm test` builds and runs Node's built-in test runner: real local HTTP transports,
fragmented SSE, schema/body/event caps, sanitized failures, truncation,
no retries, deadline cancellation, disconnect cancellation and a paused HTTP
client with bounded application writes. Parser tests check pull suspension and
reader cancellation. `npm run check` is strict TypeScript checking;
`npm run demo:smoke` and `npm run smoke:package` verify executable entry points.
These are correctness checks, not throughput, latency, or production-usage claims.

Next, **not implemented**: Redis quotas, OpenTelemetry, authentication,
concurrency/rate admission limits, richer provider schemas and a production
deployment/security review. The gateway currently trusts its local users and
server configuration; it is not a network security boundary.

## Authorship / license

Original project developed with substantial GitHub Copilot AI assistance,
including implementation, tests and documentation. Generated work is not
evidence of the candidate's independent mastery: a candidate should personally
review and understand the parser, lifecycle, backpressure tradeoffs and tests
before claiming authorship or discussing this as an internship portfolio item.
No employment/research/team code, invented users or fabricated benchmarks.
MIT licensed; see [LICENSE](LICENSE).
