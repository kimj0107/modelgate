import { randomUUID } from "node:crypto";
import { createServer } from "node:http";
import type { IncomingMessage, ServerResponse, Server } from "node:http";
import { GatewayError, abortError, publicError } from "./errors.js";
import type { ErrorCode } from "./errors.js";
import { normalize, openProvider } from "./providers.js";
import type { ProviderConfig, ProviderName, StreamRequest } from "./providers.js";

export interface LogRecord {
  requestId: string;
  provider: ProviderName | "unknown";
  status: number | ErrorCode;
  durationMs: number;
}

export interface GatewayConfig {
  providers: Partial<Record<ProviderName, ProviderConfig>>;
  requestTimeoutMs?: number;
  upstreamTimeoutMs?: number;
  maxBodyBytes?: number;
  maxFrameBytes?: number;
  log?: (record: Readonly<LogRecord>) => void;
}

export interface FlowStats {
  activeRequests: number;
  backpressureWaits: number;
  maxWritableBytes: number;
}

function positive(value: number | undefined, fallback: number): number {
  const result = value ?? fallback;
  if (!Number.isSafeInteger(result) || result <= 0 || result > 2_147_483_647) {
    throw new Error("Invalid gateway limit");
  }
  return result;
}

function validate(value: unknown): StreamRequest {
  const bad = (): never => { throw new GatewayError("invalid_request", 400); };
  if (!value || typeof value !== "object" || Array.isArray(value)) return bad();
  const v = value as Record<string, unknown>;
  if (Object.keys(v).some(k => !["provider", "model", "messages", "maxTokens"].includes(k))) return bad();
  if (v.provider !== "openai" && v.provider !== "anthropic") return bad();
  if (typeof v.model !== "string" || !/^[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,127}$/.test(v.model)) return bad();
  if (!Array.isArray(v.messages) || !v.messages.length || v.messages.length > 32) return bad();
  const messages: StreamRequest["messages"] = v.messages.map((message: unknown): StreamRequest["messages"][number] => {
    if (!message || typeof message !== "object" || Array.isArray(message)) return bad();
    const m = message as Record<string, unknown>;
    if (Object.keys(m).some(k => k !== "role" && k !== "content")) return bad();
    if (m.role !== "user" && m.role !== "assistant") return bad();
    if (typeof m.content !== "string" || !m.content.length || m.content.length > 16_384) return bad();
    return { role: m.role, content: m.content };
  });
  const maxTokens = v.maxTokens === undefined ? 256 : v.maxTokens;
  if (typeof maxTokens !== "number" || !Number.isInteger(maxTokens) || maxTokens < 1 || maxTokens > 4096) return bad();
  return { provider: v.provider, model: v.model, messages, maxTokens };
}

async function readBody(req: IncomingMessage, max: number, signal: AbortSignal): Promise<StreamRequest> {
  const contentType = req.headers["content-type"]?.split(";")[0]?.trim().toLowerCase();
  if (contentType !== "application/json" || req.headers["content-encoding"]) {
    throw new GatewayError("unsupported_media_type", 415);
  }
  if (Number(req.headers["content-length"] ?? 0) > max) throw new GatewayError("body_too_large", 413);
  const bytes = await new Promise<Buffer>((resolve, reject) => {
    const chunks: Buffer[] = [];
    let size = 0;
    const cleanup = (): void => {
      req.off("data", data);
      req.off("end", end);
      req.off("error", error);
      signal.removeEventListener("abort", abort);
      req.pause();
    };
    const fail = (failure: unknown): void => { cleanup(); reject(failure); };
    const abort = (): void => fail(abortError(signal));
    const error = (): void => fail(new GatewayError("client_disconnected", 499));
    const data = (chunk: Buffer): void => {
      size += chunk.length;
      if (size > max) return fail(new GatewayError("body_too_large", 413));
      chunks.push(chunk);
    };
    const end = (): void => { cleanup(); resolve(Buffer.concat(chunks, size)); };
    req.on("data", data);
    req.once("end", end);
    req.once("error", error);
    signal.addEventListener("abort", abort, { once: true });
    if (signal.aborted) abort();
  });
  try {
    return validate(JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes)) as unknown);
  } catch (error) {
    if (error instanceof GatewayError) throw error;
    throw new GatewayError("invalid_request", 400);
  }
}

export function encodeEvent(type: string, value: unknown): string {
  return `event: ${type}\ndata: ${JSON.stringify(value)}\n\n`;
}

/** Never pulls another upstream event while Node's downstream buffer is saturated. */
async function write(
  res: ServerResponse,
  frame: string,
  signal: AbortSignal,
  stats: FlowStats,
  onQueued?: () => void,
): Promise<void> {
  if (signal.aborted) throw abortError(signal);
  if (res.destroyed) throw new GatewayError("client_disconnected", 499);
  const ready = res.write(frame);
  onQueued?.();
  stats.maxWritableBytes = Math.max(stats.maxWritableBytes, res.writableLength);
  if (ready) return;
  stats.backpressureWaits++;
  await new Promise<void>((resolve, reject) => {
    const cleanup = (): void => {
      res.off("drain", drain);
      res.off("close", close);
      res.off("error", close);
      signal.removeEventListener("abort", abort);
    };
    const drain = (): void => { cleanup(); resolve(); };
    const close = (): void => { cleanup(); reject(new GatewayError("client_disconnected", 499)); };
    const abort = (): void => { cleanup(); reject(abortError(signal)); };
    res.once("drain", drain);
    res.once("close", close);
    res.once("error", close);
    signal.addEventListener("abort", abort, { once: true });
    if (signal.aborted) abort();
    else if (res.destroyed) close();
  });
}

export function createGateway(config: GatewayConfig): { server: Server; stats: Readonly<FlowStats> } {
  const requestTimeout = positive(config.requestTimeoutMs, 30_000);
  const upstreamTimeout = positive(config.upstreamTimeoutMs, 20_000);
  const maxBody = positive(config.maxBodyBytes, 64 * 1024);
  const maxFrame = positive(config.maxFrameBytes, 32 * 1024);
  // Snapshot configuration; neither request fields nor later caller mutations can redirect a stream.
  const providers: Partial<Record<ProviderName, ProviderConfig>> = {};
  for (const name of ["openai", "anthropic"] as const) {
    const provider = config.providers[name];
    if (!provider) continue;
    const url = new URL(provider.endpoint);
    if (!["http:", "https:"].includes(url.protocol) || url.username || url.password || url.hash) {
      throw new Error("Invalid provider endpoint");
    }
    providers[name] = { ...provider, endpoint: url.href };
  }
  const stats: FlowStats = { activeRequests: 0, backpressureWaits: 0, maxWritableBytes: 0 };
  const server = createServer((req, res) => {
    void handle(req, res);
  });
  server.headersTimeout = Math.min(requestTimeout, 10_000);
  server.requestTimeout = requestTimeout;
  server.keepAliveTimeout = 1000;

  async function handle(req: IncomingMessage, res: ServerResponse): Promise<void> {
    stats.activeRequests++;
    const started = performance.now();
    const requestId = randomUUID();
    const controller = new AbortController();
    const { signal } = controller;
    let provider: ProviderName | "unknown" = "unknown";
    let status: LogRecord["status"] = 500;
    let upstreamTimer: ReturnType<typeof setTimeout> | undefined;
    let upstreamBody: ReadableStream<Uint8Array> | undefined;
    let terminalQueued = false;
    const abort = (error: GatewayError): void => { if (!signal.aborted) controller.abort(error); };
    const disconnected = (): void => abort(new GatewayError("client_disconnected", 499));
    const responseClosed = (): void => { if (!res.writableFinished) disconnected(); };
    // Error listeners stay attached through response finish and never expose error objects.
    const responseError = (): void => disconnected();
    const requestError = (): void => disconnected();
    req.once("aborted", disconnected);
    req.on("error", requestError);
    res.once("close", responseClosed);
    res.on("error", responseError);
    const requestTimer = setTimeout(() => abort(new GatewayError("deadline_exceeded", 504)), requestTimeout);
    res.setHeader("x-request-id", requestId);
    res.setHeader("cache-control", "no-store");
    try {
      if (req.url !== "/v1/stream") throw new GatewayError("not_found", 404);
      if (req.method !== "POST") {
        res.setHeader("allow", "POST");
        throw new GatewayError("method_not_allowed", 405);
      }
      const request = await readBody(req, maxBody, signal);
      provider = request.provider;
      const transport = providers[provider];
      if (!transport) throw new GatewayError("invalid_request", 400);
      upstreamTimer = setTimeout(() => abort(new GatewayError("deadline_exceeded", 504)), upstreamTimeout);
      const response = await openProvider(request, transport, signal);
      upstreamBody = response.body!;
      res.statusCode = 200;
      res.setHeader("content-type", "text/event-stream; charset=utf-8");
      res.setHeader("x-accel-buffering", "no");
      await write(res, encodeEvent("start", { version: 1, requestId, provider }), signal, stats);
      for await (const event of normalize(provider, upstreamBody, maxFrame, signal)) {
        if (event.type === "delta") await write(res, encodeEvent("delta", { text: event.text }), signal, stats);
        else await write(res, encodeEvent("done", { reason: event.reason }), signal, stats,
          () => { terminalQueued = true; });
      }
      status = 200;
      res.end();
    } catch (failure) {
      const error = publicError(failure, signal);
      status = error.code;
      if (!res.destroyed && !res.writableEnded) {
        if (!res.headersSent) {
          res.statusCode = error.status;
          res.setHeader("content-type", "application/json; charset=utf-8");
          res.setHeader("connection", "close");
          res.end(JSON.stringify({ error: { code: error.code }, requestId }));
        } else if (error.code !== "client_disconnected") {
          // One bounded terminal frame, no drain wait after a deadline. end() queues it
          // behind prior output; a non-reading client is destroyed below after a grace period.
          res.shouldKeepAlive = false;
          // A queued done cannot be retracted if its drain wait was interrupted.
          res.end(terminalQueued ? undefined : encodeEvent("error", { code: error.code }));
        }
      }
    } finally {
      clearTimeout(requestTimer);
      clearTimeout(upstreamTimer);
      // Fetch owns its AbortSignal until body cancellation; never reuse this controller.
      if (!signal.aborted) controller.abort(new GatewayError("client_disconnected", 499));
      if (upstreamBody && !upstreamBody.locked) await upstreamBody.cancel().catch(() => {});
      req.off("aborted", disconnected);
      req.off("error", requestError);
      res.off("close", responseClosed);
      // A stalled TCP peer cannot hold a response/socket forever after the work deadline.
      const forceClose = setTimeout(() => res.destroy(), 1000);
      forceClose.unref();
      const finished = (): void => {
        clearTimeout(forceClose);
        res.off("close", finished);
        res.off("finish", finished);
        res.off("error", responseError);
      };
      res.once("close", finished);
      res.once("finish", finished);
      if (res.writableFinished || res.destroyed) finished();
      stats.activeRequests--;
      try { config.log?.({ requestId, provider, status, durationMs: Math.round(performance.now() - started) }); }
      catch { /* Logging must not alter request handling. */ }
    }
  }
  return { server, stats };
}
