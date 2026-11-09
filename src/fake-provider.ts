import { createServer } from "node:http";
import type { ServerResponse } from "node:http";

export function openaiChunk(text: string): string {
  return `data: ${JSON.stringify({ choices: [{ index: 0, delta: { content: text }, finish_reason: null }] })}\n\n`;
}

export const openaiEnd = 'data: {"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}\n\ndata: [DONE]\n\n';

export function anthropicEvent(type: string, fields: Record<string, unknown> = {}): string {
  return `event: ${type}\ndata: ${JSON.stringify({ type, ...fields })}\n\n`;
}

export const anthropicStart = anthropicEvent("message_start", { message: { role: "assistant", content: [] } })
  + anthropicEvent("content_block_start", { index: 0, content_block: { type: "text", text: "" } });
export const anthropicEnd = anthropicEvent("content_block_stop", { index: 0 })
  + anthropicEvent("message_delta", { delta: { stop_reason: "end_turn" } })
  + anthropicEvent("message_stop");

async function send(res: ServerResponse, text: string): Promise<void> {
  if (res.destroyed) return;
  if (!res.write(text)) {
    await new Promise<void>(resolve => {
      const done = (): void => {
        res.off("drain", done);
        res.off("close", done);
        res.off("error", done);
        resolve();
      };
      res.once("drain", done);
      res.once("close", done);
      res.once("error", done);
      if (res.destroyed) done();
    });
  }
}

/** Deterministic local transport fixture; no model, credentials, or external network. */
export function createFakeProvider() {
  return createServer((req, res) => {
    req.resume();
    void (async () => {
      if (req.url !== "/openai" && req.url !== "/anthropic") {
        res.writeHead(404).end();
        return;
      }
      res.writeHead(200, { "content-type": "text/event-stream" });
      if (req.url === "/anthropic") await send(res, anthropicStart);
      for (const text of ["Local ", "streaming ", "works. 🌍"]) {
        await send(res, req.url === "/openai" ? openaiChunk(text)
          : anthropicEvent("content_block_delta", { index: 0, delta: { type: "text_delta", text } }));
      }
      res.end(req.url === "/openai" ? openaiEnd : anthropicEnd);
    })().catch(() => res.destroy());
  });
}
