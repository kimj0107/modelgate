import assert from "node:assert/strict";
import test from "node:test";
import { parseSse } from "../src/sse.js";

function bytes(text: string): ReadableStream<Uint8Array> {
  const input = new TextEncoder().encode(text);
  let i = 0;
  return new ReadableStream<Uint8Array>({
    pull(controller) {
      if (i === input.length) controller.close();
      else controller.enqueue(input.subarray(i, ++i));
    },
  }, { highWaterMark: 0 });
}
test("parser handles every-byte UTF-8 fragmentation, BOM, lone CR, CRLF and multiline fields", async () => {
  const result = [];
  for await (const frame of parseSse(bytes("\uFEFF: comment\r\n\r\nevent: token\rdata: café 🌍\r\ndata: second\r\r"), 256, new AbortController().signal)) {
    result.push(frame);
  }
  assert.deepEqual(result, [{ event: "token", data: "café 🌍\nsecond" }]);
});
test("parser caps comment-only frames too and cancels source on errors", async () => {
  let cancelled = false;
  const body = new ReadableStream<Uint8Array>({
    start(controller) { controller.enqueue(new TextEncoder().encode(":".repeat(33))); },
    cancel() { cancelled = true; },
  });
  await assert.rejects(async () => {
    for await (const _ of parseSse(body, 32, new AbortController().signal)) { /* no frames expected */ }
  }, /invalid_upstream/);
  assert.equal(cancelled, true);
});
test("frame cap includes both bytes of CRLF", async () => {
  const consume = async (limit: number) => {
    const result = [];
    for await (const frame of parseSse(bytes("data: x\r\n\r\n"), limit, new AbortController().signal)) result.push(frame);
    return result;
  };
  assert.deepEqual(await consume(11), [{ event: "message", data: "x" }]);
  await assert.rejects(consume(10), /invalid_upstream/);
});
test("parser does not pull the next transport chunk while consumer is suspended at a frame", async () => {
  let pulls = 0;
  let cancelled = false;
  const body = new ReadableStream<Uint8Array>({
    pull(controller) {
      pulls++;
      controller.enqueue(new TextEncoder().encode(`data: ${pulls}\n\n`));
    },
    cancel() { cancelled = true; },
  }, { highWaterMark: 0 });
  const iterator = parseSse(body, 64, new AbortController().signal);
  assert.deepEqual((await iterator.next()).value, { event: "message", data: "1" });
  await new Promise<void>(resolve => setImmediate(resolve));
  assert.equal(pulls, 1);
  await iterator.return(undefined);
  assert.equal(cancelled, true);
  assert.equal(body.locked, false);
});
