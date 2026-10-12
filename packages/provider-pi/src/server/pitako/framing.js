export const frameBytes = 64 * 1024;
export function frame(value, label = "Agent pane") {
  const data = JSON.stringify(value) + "\n";
  if (Buffer.byteLength(data) > frameBytes) throw new Error(`${label} frame exceeds 64KiB`);
  return data;
}
/** Byte accounting happens before UTF-8 decoding; neither peers nor JSON can allocate an unbounded frame. */
export function frames(receive, { label = "Agent pane", fatalUtf8 = false } = {}) {
  let pending = Buffer.alloc(0);
  return chunk => {
    while (chunk.length) {
      // Socket high-water marks are not an inbound security bound.
      const end = chunk.indexOf(10);
      const part = end < 0 ? chunk : chunk.subarray(0, end + 1);
      if (pending.length + part.length > frameBytes) throw new Error(`${label} frame exceeds 64KiB`);
      pending = Buffer.concat([pending, part]);
      if (end < 0) return;
      const value = JSON.parse(fatalUtf8
        ? new TextDecoder("utf-8", { fatal: true }).decode(pending)
        : pending.toString("utf8"));
      pending = Buffer.alloc(0);
      receive(value);
      chunk = chunk.subarray(end + 1);
    }
  };
}
