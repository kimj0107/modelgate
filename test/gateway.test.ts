import assert from "node:assert/strict";
import { once } from "node:events";
import { createServer, request as httpRequest } from "node:http";
import type { IncomingMessage, ServerResponse } from "node:http";
import test from "node:test";
import type { TestContext } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";
import { createGateway } from "../src/gateway.js";
import type { GatewayConfig, LogRecord } from "../src/gateway.js";
import { listen, close } from "../src/lifecycle.js";
import { openaiChunk, openaiEnd, anthropicStart, anthropicEnd, anthropicEvent } from "../src/fake-provider.js";

type Handler = (req: IncomingMessage, res: ServerResponse) => void | Promise<void>;
async function setup(t: TestContext, handler: Handler, limits: Omit<GatewayConfig, "providers"> = {}) {
  const provider = createServer((req, res) => {
    Promise.resolve(handler(req, res)).catch(() => res.destroy());
  });
  const upstream = await listen(provider);
  const records: LogRecord[] = [];
  const gateway = createGateway({
    providers: { openai: { endpoint: `${upstream}/openai`, apiKey: "server-token-secret" },
      anthropic: { endpoint: `${upstream}/anthropic`, apiKey: "anthropic-token-secret" } },
    requestTimeoutMs: 8000, upstreamTimeoutMs: 7000,
    log: record => records.push(record), ...limits,
  });
  const origin = await listen(gateway.server);
  t.after(async () => { await close(gateway.server); await close(provider); });
  const send = (body: unknown = valid(), headers = { "content-type": "application/json" }) =>
    fetch(`${origin}/v1/stream`, {
      method: "POST", headers, body: JSON.stringify(body), signal: AbortSignal.timeout(6000),
    });
  return { ...gateway, origin, upstream, send, records };
}
function valid(provider = "openai") {
  return { provider, model: "fixture-model", messages: [{ role: "user", content: "prompt-secret" }] };
}
function sse(res: ServerResponse): void {
  res.writeHead(200, { "content-type": "text/event-stream; charset=utf-8" });
}
function events(text: string): { event: string; data: Record<string, unknown> }[] {
  return text.trim().split("\n\n").map(frame => {
    const [event, data] = frame.split("\n");
    assert.ok(event && event.startsWith("event: "));
    assert.ok(data && data.startsWith("data: "));
    return { event: event.slice(7), data: JSON.parse(data.slice(6)) as Record<string, unknown> };
  });
}
async function waitFor(predicate: () => boolean, message: string, timeout = 4000): Promise<void> {
  const until = Date.now() + timeout;
  while (!predicate()) {
    if (Date.now() > until) throw new Error(message);
    await sleep(10);
  }
}

test("real network transports select server endpoints and map both providers", async t => {
  const paths: string[] = [];
  const app = await setup(t, async (req, res) => {
    paths.push(req.url!);
    const chunks: Buffer[] = [];
    for await (const chunk of req) chunks.push(chunk as Buffer);
    const body = JSON.parse(Buffer.concat(chunks).toString()) as Record<string, unknown>;
    assert.equal(body.stream, true);
    assert.equal(body.max_tokens, 256);
    assert.equal(body.model, "fixture-model");
    sse(res);
    if (req.url === "/openai") {
      assert.equal(req.headers.authorization, "Bearer server-token-secret");
      res.end(openaiChunk("Hello 🌍") + openaiEnd);
    } else {
      assert.equal(req.headers["x-api-key"], "anthropic-token-secret");
      assert.equal(req.headers["anthropic-version"], "2023-06-01");
      res.end(anthropicStart + anthropicEvent("content_block_delta", {
        index: 0, delta: { type: "text_delta", text: "Hello 🌍" },
      }) + anthropicEnd);
    }
  });
  for (const provider of ["openai", "anthropic"]) {
    const response = await app.send(valid(provider));
    assert.equal(response.status, 200);
    assert.match(response.headers.get("content-type")!, /text\/event-stream/);
    const output = events(await response.text());
    assert.deepEqual(output.map(e => e.event), ["start", "delta", "done"]);
    assert.equal(output[0]?.data.provider, provider);
    assert.equal(output[0]?.data.requestId, response.headers.get("x-request-id"));
    assert.equal(output[1]?.data.text, "Hello 🌍");
    assert.equal(output[2]?.data.reason, "stop");
  }
  assert.deepEqual(paths, ["/openai", "/anthropic"]);
  assert.equal(app.stats.activeRequests, 0);
});

test("HTTP UTF-8 fragments, CRLF, comments, and multiline data normalize correctly", async t => {
  const app = await setup(t, async (req, res) => {
    req.resume();
    sse(res);
    const wire = Buffer.from(': heartbeat\r\n\r\ndata: {"choices": [\r\ndata: {"index":0,"delta":{"content":"café 🌍"},\r\ndata: "finish_reason":null}]}\r\n\r\n'
      + openaiEnd.replaceAll("\n", "\r\n"));
    // Separate actual HTTP writes inside multi-byte characters and delimiters.
    for (let i = 0; i < wire.length; i += 3) {
      if (res.destroyed) return;
      res.write(wire.subarray(i, i + 3));
      await new Promise<void>(resolve => setImmediate(resolve));
    }
    res.end();
  });
  const output = events(await (await app.send()).text());
  assert.equal(output[1]?.data.text, "café 🌍");
  assert.equal(output.at(-1)?.event, "done");
});

test("validates request schema, methods, media type, routes, and both body caps", async t => {
  let upstreamCalls = 0;
  const app = await setup(t, (req, res) => { upstreamCalls++; req.resume(); res.end(); }, { maxBodyBytes: 512 });
  const invalid = [
    { ...valid(), endpoint: "http://caller-controlled.invalid" },
    { ...valid(), provider: "unknown" },
    { ...valid(), model: "" },
    { ...valid(), messages: [] },
    { ...valid(), messages: [{ role: "system", content: "not supported" }] },
    { ...valid(), messages: [{ role: "user", content: "x", extra: true }] },
    { ...valid(), maxTokens: 0 },
    { ...valid(), maxTokens: null },
    { ...valid(), maxTokens: 1.5 },
    null,
  ];
  for (const body of invalid) {
    const response = await app.send(body);
    assert.equal(response.status, 400);
    assert.equal((await response.json() as { error: { code: string } }).error.code, "invalid_request");
  }
  const tooBig = await app.send({ ...valid(), model: "x".repeat(600) });
  assert.equal(tooBig.status, 413);
  await tooBig.text();
  const chunked = await new Promise<{ status: number; body: string }>((resolve, reject) => {
    const req = httpRequest(`${app.origin}/v1/stream`, {
      method: "POST", headers: { "content-type": "application/json" },
    }, res => {
      let body = "";
      res.setEncoding("utf8").on("data", chunk => { body += String(chunk); });
      res.on("end", () => resolve({ status: res.statusCode!, body }));
    });
    req.on("error", reject);
    req.write("x".repeat(600));
    req.end();
  });
  assert.equal(chunked.status, 413);
  const wrongType = await app.send(valid(), { "content-type": "text/plain" });
  assert.equal(wrongType.status, 415);
  await wrongType.text();
  for (const [path, status] of [["/v1/stream", 405], ["/missing", 404]] as const) {
    const response = await fetch(app.origin + path);
    assert.equal(response.status, status);
    await response.text();
  }
  const malformed = await fetch(`${app.origin}/v1/stream`, {
    method: "POST", headers: { "content-type": "application/json" }, body: "{",
  });
  assert.equal(malformed.status, 400);
  await malformed.text();
  assert.equal(upstreamCalls, 0);
});

test("pre-header provider failures are JSON, redirects are not followed, logs contain only metadata", async t => {
  let requests = 0;
  const app = await setup(t, (req, res) => {
    req.resume();
    requests++;
    res.writeHead(requests === 1 ? 401 : requests === 2 ? 302 : 200,
      { location: "/secret-target", "content-type": "application/json" });
    res.end("provider-error-secret server-token-secret prompt-secret");
  });
  for (let i = 0; i < 3; i++) {
    const response = await app.send();
    assert.equal(response.status, 502);
    assert.match(response.headers.get("content-type")!, /application\/json/);
    assert.equal((await response.json() as { error: { code: string } }).error.code, "upstream_unavailable");
  }
  assert.equal(requests, 3);
  assert.equal(app.records.length, 3);
  for (const record of app.records) {
    assert.deepEqual(Object.keys(record).sort(), ["durationMs", "provider", "requestId", "status"]);
    assert.ok(record.durationMs >= 0);
  }
  assert.doesNotMatch(JSON.stringify(app.records), /secret|Bearer|provider-error/);
});

test("midstream errors are sanitized terminal errors and never retried after output", async t => {
  let requests = 0;
  const app = await setup(t, (req, res) => {
    req.resume();
    requests++;
    sse(res);
    res.end(openaiChunk("completion-secret") + 'data: {"error":{"message":"provider-secret server-token-secret"}}\n\n');
  });
  const response = await app.send();
  assert.equal(response.status, 200);
  const body = await response.text();
  const output = events(body);
  assert.deepEqual(output.map(e => e.event), ["start", "delta", "error"]);
  assert.deepEqual(output.at(-1)?.data, { code: "invalid_upstream" });
  assert.doesNotMatch(body, /provider-secret|server-token-secret/);
  assert.equal(requests, 1);
  assert.doesNotMatch(JSON.stringify(app.records), /secret/);
  assert.equal(app.records[0]?.status, "invalid_upstream");
});

test("malformed, truncated, oversized, invalid UTF-8, and missing semantic endings fail closed", async t => {
  const cases: (string | Buffer)[] = [
    "data: not-json\n\n",
    openaiChunk("prefix") + "data: {",
    openaiChunk("prefix"),
    'data: {"choices":[]}\n\n',
    "data: [DONE]\n\n",
    "data: " + "x".repeat(1100) + "\n\n",
    Buffer.concat([Buffer.from("data: "), Buffer.from([0xc3, 0x28]), Buffer.from("\n\n")]),
    'data: {"choices":[{"index":1,"delta":{"content":"wrong choice"}}]}\n\n',
    'data: {"choices":[{"index":0,"delta":{"tool_calls":[]},"finish_reason":null}]}\n\n',
    'data: {"choices":[{"index":0,"delta":{"refusal":"not text"},"finish_reason":null}]}\n\n',
  ];
  let index = 0;
  const app = await setup(t, (req, res) => { req.resume(); sse(res); res.end(cases[index++]); }, { maxFrameBytes: 1024 });
  for (const _ of cases) {
    const output = events(await (await app.send()).text());
    assert.equal(output.at(-1)?.event, "error");
    assert.deepEqual(output.at(-1)?.data, { code: "invalid_upstream" });
    assert.equal(output.filter(e => e.event === "done").length, 0);
  }
});

test("Anthropic rejects truncated lifecycle and non-text deltas", async t => {
  let count = 0;
  const app = await setup(t, (req, res) => {
    req.resume(); sse(res);
    res.end(count++ === 0 ? anthropicStart + anthropicEvent("message_stop")
      : anthropicStart + anthropicEvent("content_block_delta", {
        index: 0, delta: { type: "input_json_delta", partial_json: "secret" },
      }));
  });
  for (let i = 0; i < 2; i++) {
    const output = events(await (await app.send(valid("anthropic"))).text());
    assert.equal(output.at(-1)?.event, "error");
  }
});

test("Anthropic preserves initial block text and validates block lifecycle", async t => {
  let count = 0;
  const start = anthropicEvent("message_start", { message: { role: "assistant", content: [] } })
    + anthropicEvent("content_block_start", { index: 0, content_block: { type: "text", text: "initial" } });
  const app = await setup(t, (req, res) => {
    req.resume(); sse(res);
    res.end(count++ === 0 ? start + anthropicEnd
      : start + anthropicEvent("content_block_delta", { index: 1, delta: { type: "text_delta", text: "wrong" } }));
  });
  const success = events(await (await app.send(valid("anthropic"))).text());
  assert.equal(success[1]?.data.text, "initial");
  assert.equal(success.at(-1)?.event, "done");
  const failure = events(await (await app.send(valid("anthropic"))).text());
  assert.equal(failure.at(-1)?.event, "error");
});

test("upstream deadline aborts hanging headers and is observed by fixture", async t => {
  let closed = false;
  const app = await setup(t, (req, res) => {
    req.resume();
    res.once("close", () => { closed = true; });
  }, { upstreamTimeoutMs: 150 });
  const response = await app.send();
  assert.equal(response.status, 504);
  assert.equal((await response.json() as { error: { code: string } }).error.code, "deadline_exceeded");
  await waitFor(() => closed && app.stats.activeRequests === 0, "upstream fetch was not cancelled");
});

test("deadline interrupts stalled upstream body reads after output, with one terminal error", async t => {
  let closed = false;
  const app = await setup(t, (req, res) => {
    req.resume(); sse(res);
    res.write(openaiChunk("first"));
    res.once("close", () => { closed = true; });
  }, { upstreamTimeoutMs: 150 });
  const response = await app.send();
  const output = events(await response.text());
  assert.deepEqual(output.map(e => e.event), ["start", "delta", "error"]);
  assert.deepEqual(output.at(-1)?.data, { code: "deadline_exceeded" });
  await waitFor(() => closed, "stalled upstream body not cancelled");
});

test("request deadline covers stalled incoming body reads before fetch", async t => {
  let calls = 0;
  const app = await setup(t, (req, res) => { calls++; req.resume(); res.end(); }, { requestTimeoutMs: 150 });
  const result = await new Promise<{ status: number; body: string }>((resolve, reject) => {
    const req = httpRequest(`${app.origin}/v1/stream`, {
      method: "POST", headers: { "content-type": "application/json", "content-length": 1000 },
    }, res => {
      let body = "";
      res.setEncoding("utf8");
      res.on("data", chunk => { body += String(chunk); });
      res.on("end", () => { req.destroy(); resolve({ status: res.statusCode!, body }); });
    });
    req.on("error", reject);
    req.write('{"provider":');
  });
  assert.equal(result.status, 504);
  assert.equal(JSON.parse(result.body).error.code, "deadline_exceeded");
  assert.equal(calls, 0);
  assert.equal(app.stats.activeRequests, 0);
});

test("aborting an incomplete request body cleans up without contacting a provider", async t => {
  let calls = 0;
  const app = await setup(t, (req, res) => { calls++; req.resume(); res.end(); });
  const request = httpRequest(`${app.origin}/v1/stream`, {
    method: "POST", headers: { "content-type": "application/json", "content-length": 1000 },
  });
  request.on("error", () => {});
  request.write('{"provider":');
  await waitFor(() => app.stats.activeRequests === 1, "request was not accepted");
  request.destroy();
  await waitFor(() => app.stats.activeRequests === 0, "aborted body leaked request state");
  assert.equal(calls, 0);
});

test("downstream disconnect cancels an active upstream HTTP body", async t => {
  let closed = false;
  let count = 0;
  const app = await setup(t, (req, res) => {
    req.resume();
    count++;
    sse(res);
    res.write(openaiChunk("first"));
    res.once("close", () => { closed = true; });
  });
  const request = httpRequest(`${app.origin}/v1/stream`, {
    method: "POST", headers: { "content-type": "application/json" },
  });
  const received = once(request, "response") as Promise<[IncomingMessage]>;
  request.end(JSON.stringify(valid()));
  const [response] = await received;
  await once(response, "data");
  response.destroy();
  await waitFor(() => closed && app.stats.activeRequests === 0, "disconnect did not propagate");
  assert.equal(count, 1);
  assert.equal(app.records[0]?.status, "client_disconnected");
});

test("slow HTTP consumer applies bounded backpressure, then disconnect releases producer", async t => {
  let sent = 0;
  let closed = false;
  const total = 8192;
  const frame = openaiChunk("x".repeat(8192));
  const app = await setup(t, async (req, res) => {
    req.resume(); sse(res);
    res.once("close", () => { closed = true; });
    for (; sent < total && !res.destroyed; sent++) {
      if (!res.write(frame)) {
        await new Promise<void>(resolve => {
          const done = (): void => { res.off("drain", done); res.off("close", done); resolve(); };
          res.once("drain", done);
          res.once("close", done);
        });
      }
    }
    if (!res.destroyed) res.end(openaiEnd);
  }, { maxFrameBytes: 16_384 });
  const request = httpRequest(`${app.origin}/v1/stream`, {
    method: "POST", headers: { "content-type": "application/json" },
  });
  const received = once(request, "response") as Promise<[IncomingMessage]>;
  request.end(JSON.stringify(valid()));
  const [response] = await received;
  response.pause();
  await waitFor(() => app.stats.backpressureWaits > 0, "downstream never backpressured");
  await sleep(150);
  assert.ok(sent < total, "producer should not drain an entire 64 MiB stream into memory");
  assert.ok(app.stats.maxWritableBytes <= 128 * 1024, `unbounded write queue: ${app.stats.maxWritableBytes}`);
  assert.equal(app.stats.activeRequests, 1);
  response.destroy();
  await waitFor(() => closed && app.stats.activeRequests === 0, "backpressured producer leaked after disconnect");
});

test("deadline releases a backpressured upstream and force-closes the non-reading peer", async t => {
  let closed = false;
  const frame = openaiChunk("x".repeat(8192));
  const app = await setup(t, async (req, res) => {
    req.resume(); sse(res);
    res.once("close", () => { closed = true; });
    while (!res.destroyed) {
      if (!res.write(frame)) {
        await new Promise<void>(resolve => {
          const done = (): void => { res.off("drain", done); res.off("close", done); resolve(); };
          res.once("drain", done);
          res.once("close", done);
        });
      }
    }
  }, { upstreamTimeoutMs: 500, maxFrameBytes: 16_384 });
  let socketClosed = false;
  app.server.once("connection", socket => socket.once("close", () => { socketClosed = true; }));
  const request = httpRequest(`${app.origin}/v1/stream`, {
    method: "POST", headers: { "content-type": "application/json" },
  });
  request.on("error", () => {});
  t.after(() => request.destroy());
  const received = once(request, "response") as Promise<[IncomingMessage]>;
  request.end(JSON.stringify(valid()));
  const [response] = await received;
  response.on("error", () => {});
  response.pause();
  await waitFor(() => app.stats.backpressureWaits > 0, "missing downstream pressure");
  await waitFor(() => closed && app.stats.activeRequests === 0, "deadline failed to release upstream");
  assert.equal(app.records[0]?.status, "deadline_exceeded");
  await waitFor(() => socketClosed, "stalled downstream socket leaked after grace");
  response.destroy();
});

test("configured provider map is snapshotted, missing providers are rejected", async t => {
  const app = await setup(t, (req, res) => { req.resume(); sse(res); res.end(openaiEnd); });
  const providers = { openai: { endpoint: `${app.upstream}/openai` } };
  const { server } = createGateway({ providers });
  const origin = await listen(server);
  t.after(() => close(server));
  providers.openai.endpoint = "http://127.0.0.1:1/should-not-be-used";
  const success = await fetch(`${origin}/v1/stream`, {
    method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(valid()),
  });
  assert.equal(events(await success.text()).at(-1)?.event, "done");
  const response = await fetch(`${origin}/v1/stream`, {
    method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(valid("anthropic")),
  });
  assert.equal(response.status, 400);
  await response.text();
  assert.throws(() => createGateway({ providers: { openai: { endpoint: "file:///not-http" } } }));
  assert.throws(() => createGateway({ providers: {}, maxFrameBytes: -1 }));
  assert.equal(app.stats.activeRequests, 0);
});
