import { GatewayError } from "./errors.js";
import { parseSse } from "./sse.js";

export type ProviderName = "openai" | "anthropic";
export interface StreamRequest {
  provider: ProviderName;
  model: string;
  messages: { role: "user" | "assistant"; content: string }[];
  maxTokens: number;
}
export interface ProviderConfig {
  /** Complete endpoint, controlled by the server, never by a request body. */
  endpoint: string;
  apiKey?: string;
}
export type NormalizedEvent =
  | { type: "delta"; text: string }
  | { type: "done"; reason: "stop" | "length" | "other" };

function object(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new GatewayError("invalid_upstream", 502);
  }
  return value as Record<string, unknown>;
}

function reason(value: unknown): "stop" | "length" | "other" {
  if (value === "stop" || value === "end_turn" || value === "stop_sequence") return "stop";
  if (value === "length" || value === "max_tokens") return "length";
  return "other";
}

export async function openProvider(
  request: StreamRequest,
  config: ProviderConfig,
  signal: AbortSignal,
): Promise<Response> {
  const anthropic = request.provider === "anthropic";
  const headers: Record<string, string> = {
    "content-type": "application/json",
    accept: "text/event-stream",
  };
  if (anthropic) {
    headers["anthropic-version"] = "2023-06-01";
    if (config.apiKey) headers["x-api-key"] = config.apiKey;
  } else if (config.apiKey) {
    headers.authorization = `Bearer ${config.apiKey}`;
  }
  const response = await fetch(config.endpoint, {
    method: "POST",
    headers,
    body: JSON.stringify({
      model: request.model,
      messages: request.messages,
      max_tokens: request.maxTokens,
      stream: true,
    }),
    signal,
    redirect: "error",
  });
  if (!response.ok || response.headers.get("content-type")?.split(";")[0]?.trim() !== "text/event-stream"
    || !response.body) {
    await response.body?.cancel().catch(() => {});
    throw new GatewayError("upstream_unavailable", 502);
  }
  return response;
}

export async function* normalize(
  provider: ProviderName,
  body: ReadableStream<Uint8Array>,
  maxFrameBytes: number,
  signal: AbortSignal,
): AsyncGenerator<NormalizedEvent> {
  let finish: unknown;
  let started = false;
  let sawFinish = false;
  let blockIndex = -1;
  let blockOpen = false;
  for await (const frame of parseSse(body, maxFrameBytes, signal)) {
    if (provider === "openai" && frame.data === "[DONE]") {
      if (!sawFinish) throw new GatewayError("invalid_upstream", 502);
      yield { type: "done", reason: reason(finish) };
      return;
    }
    let value: Record<string, unknown>;
    try { value = object(JSON.parse(frame.data) as unknown); }
    catch { throw new GatewayError("invalid_upstream", 502); }
    if (value.error || frame.event === "error") throw new GatewayError("invalid_upstream", 502);
    if (provider === "openai") {
      if (!Array.isArray(value.choices)) throw new GatewayError("invalid_upstream", 502);
      // Usage-only chunks are legal; this prototype supports one completion choice.
      if (value.choices.length === 0) continue;
      if (value.choices.length !== 1) throw new GatewayError("invalid_upstream", 502);
      const choice = object(value.choices[0]);
      if (choice.index !== 0 || sawFinish) throw new GatewayError("invalid_upstream", 502);
      const delta = object(choice.delta);
      if (delta.tool_calls !== undefined || delta.function_call !== undefined || delta.refusal) {
        throw new GatewayError("invalid_upstream", 502);
      }
      if (delta.content !== undefined && delta.content !== null) {
        if (typeof delta.content !== "string") throw new GatewayError("invalid_upstream", 502);
        if (delta.content) yield { type: "delta", text: delta.content };
      }
      if (choice.finish_reason !== undefined && choice.finish_reason !== null) {
        if (typeof choice.finish_reason !== "string") throw new GatewayError("invalid_upstream", 502);
        finish = choice.finish_reason;
        sawFinish = true;
      }
    } else {
      if (value.type !== frame.event) throw new GatewayError("invalid_upstream", 502);
      switch (value.type) {
        case "message_start":
          if (started) throw new GatewayError("invalid_upstream", 502);
          if (object(value.message).role !== "assistant"
            || !Array.isArray(object(value.message).content)
            || (object(value.message).content as unknown[]).length !== 0) {
            throw new GatewayError("invalid_upstream", 502);
          }
          started = true;
          break;
        case "content_block_delta": {
          if (!started || sawFinish || !blockOpen || value.index !== blockIndex) {
            throw new GatewayError("invalid_upstream", 502);
          }
          const delta = object(value.delta);
          if (delta.type !== "text_delta" || typeof delta.text !== "string") {
            throw new GatewayError("invalid_upstream", 502);
          }
          if (delta.text) yield { type: "delta", text: delta.text };
          break;
        }
        case "message_delta":
          if (!started || sawFinish || blockOpen) throw new GatewayError("invalid_upstream", 502);
          finish = object(value.delta).stop_reason;
          if (typeof finish !== "string") throw new GatewayError("invalid_upstream", 502);
          sawFinish = true;
          break;
        case "message_stop":
          if (!started || !sawFinish) throw new GatewayError("invalid_upstream", 502);
          yield { type: "done", reason: reason(finish) };
          return;
        case "ping":
          break;
        case "content_block_start": {
          if (!started || sawFinish || blockOpen || value.index !== blockIndex + 1) {
            throw new GatewayError("invalid_upstream", 502);
          }
          const block = object(value.content_block);
          if (block.type !== "text" || typeof block.text !== "string") {
            throw new GatewayError("invalid_upstream", 502);
          }
          blockIndex++;
          blockOpen = true;
          if (block.text) yield { type: "delta", text: block.text };
          break;
        }
        case "content_block_stop":
          if (!started || sawFinish || !blockOpen || value.index !== blockIndex) {
            throw new GatewayError("invalid_upstream", 502);
          }
          blockOpen = false;
          break;
        default:
          throw new GatewayError("invalid_upstream", 502);
      }
    }
  }
  throw new GatewayError("invalid_upstream", 502);
}
