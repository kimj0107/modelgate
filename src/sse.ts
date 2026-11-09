import { GatewayError, abortError } from "./errors.js";

export interface SseFrame {
  event: string;
  data: string;
}

/** Pulls only when the consumer asks; retains at most one bounded event and one transport chunk. */
export async function* parseSse(
  body: ReadableStream<Uint8Array>,
  maxFrameBytes: number,
  signal: AbortSignal,
): AsyncGenerator<SseFrame> {
  const reader = body.getReader();
  const decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
  let line: number[] = [];
  let frameBytes = 0;
  let data: string[] = [];
  let event = "";
  let pendingCr = false;
  let firstLine = true;
  const cancel = (): void => { void reader.cancel().catch(() => {}); };
  signal.addEventListener("abort", cancel, { once: true });

  function finishLine(): SseFrame | undefined {
    let text: string;
    try { text = decoder.decode(Uint8Array.from(line)); }
    catch { throw new GatewayError("invalid_upstream", 502); }
    line = [];
    if (firstLine) {
      text = text.replace(/^\uFEFF/, "");
      firstLine = false;
    }
    if (text === "") {
      const frame = data.length ? { event: event || "message", data: data.join("\n") } : undefined;
      data = [];
      event = "";
      frameBytes = 0;
      return frame;
    }
    if (text.startsWith(":")) return;
    const colon = text.indexOf(":");
    const field = colon < 0 ? text : text.slice(0, colon);
    let value = colon < 0 ? "" : text.slice(colon + 1);
    if (value.startsWith(" ")) value = value.slice(1);
    if (field === "data") data.push(value);
    if (field === "event") event = value;
    return;
  }

  try {
    if (signal.aborted) throw abortError(signal);
    while (true) {
      const next = await reader.read();
      if (signal.aborted) throw abortError(signal);
      if (next.done) break;
      for (const byte of next.value) {
        if (pendingCr) {
          pendingCr = false;
          if (byte === 10) {
            if (++frameBytes > maxFrameBytes) throw new GatewayError("invalid_upstream", 502);
            const frame = finishLine();
            if (frame) yield frame;
            continue;
          }
          const frame = finishLine();
          if (frame) yield frame;
        }
        if (++frameBytes > maxFrameBytes) throw new GatewayError("invalid_upstream", 502);
        if (byte === 13) {
          pendingCr = true;
        } else if (byte === 10) {
          const frame = finishLine();
          if (frame) yield frame;
        } else {
          line.push(byte);
        }
      }
    }
    if (pendingCr) {
      const frame = finishLine();
      if (frame) yield frame;
    }
    if (line.length || frameBytes || data.length || event) {
      throw new GatewayError("invalid_upstream", 502);
    }
  } finally {
    signal.removeEventListener("abort", cancel);
    await reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}
