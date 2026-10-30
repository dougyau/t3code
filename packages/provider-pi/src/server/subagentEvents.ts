import * as Predicate from "effect/Predicate";
import { piRecordNumber, piRecordString, type PiRpcRecord } from "./rpc.ts";
import { parsePiThinkingLevel, type PiThinkingLevel } from "./thinkingCapabilities.ts";

export const PI_SUBAGENT_EVENT_BYTES = 256 * 1024;
const MAX_LINE = 1024 * 1024;

export interface PiSubagentEventCursor {
  offset: number;
  pending: string;
  skipLine: boolean;
  decoder?: InstanceType<typeof TextDecoder>;
  skippedRecords?: number;
}

export interface PiSubagentToolEvent {
  readonly kind: "tool";
  readonly record: PiRpcRecord;
  readonly runId: string;
  readonly stepIndex: number;
  readonly toolCallId: string;
  readonly toolName: string;
  readonly phase: "start" | "update" | "end";
}

export interface PiSubagentThinkingEvent {
  readonly kind: "thinking";
  readonly runId: string;
  readonly stepIndex: number;
  readonly timestamp: number;
  readonly contentIndex: number;
  readonly text: string;
}

export interface PiSubagentModelEvent {
  readonly kind: "model";
  readonly runId: string;
  readonly stepIndex: number;
  readonly timestamp: number;
  readonly model: string;
  readonly thinking?: PiThinkingLevel | undefined;
}

type PiSubagentEvent = PiSubagentToolEvent | PiSubagentThinkingEvent | PiSubagentModelEvent;

/** A truncated record must not become a complete activity or Thinking block. */
export function piSubagentEventLines(
  cursor: PiSubagentEventCursor,
  chunk: string,
): ReadonlyArray<PiSubagentEvent> {
  if (cursor.skipLine) {
    const end = chunk.indexOf("\n");
    if (end < 0) return [];
    chunk = chunk.slice(end + 1);
    cursor.skipLine = false;
  }
  const lines = (cursor.pending + chunk).split("\n");
  const encoder = new TextEncoder();
  const oversized = (line: string) =>
    line.length > MAX_LINE || encoder.encode(line).byteLength > MAX_LINE;
  const skipRecord = () => {
    cursor.skippedRecords = (cursor.skippedRecords ?? 0) + 1;
  };
  cursor.pending = lines.pop() ?? "";
  if (oversized(cursor.pending)) {
    cursor.pending = "";
    cursor.skipLine = true;
    skipRecord();
  }
  const events: PiSubagentEvent[] = [];
  for (const line of lines) {
    if (oversized(line)) {
      skipRecord();
      continue;
    }
    let record: unknown;
    try {
      record = JSON.parse(line);
    } catch {
      continue;
    }
    if (!Predicate.isObject(record) || record["subagentSource"] !== "child") continue;
    const type = record["type"];
    const runId = piRecordString(record, "subagentRunId");
    const stepIndex = piRecordNumber(record, "subagentStepIndex");
    if (
      !runId ||
      runId.length > 256 ||
      stepIndex === undefined ||
      !Number.isSafeInteger(stepIndex) ||
      stepIndex < 0 ||
      stepIndex > 127
    )
      continue;
    if (type === "message_end") {
      const message = record["message"];
      if (!Predicate.isObject(message) || message["role"] !== "assistant") continue;
      const timestamp = piRecordNumber(message, "timestamp");
      const content = message["content"];
      if (timestamp === undefined || !Number.isSafeInteger(timestamp) || timestamp < 0) continue;
      const provider = piRecordString(message, "provider");
      const model = piRecordString(message, "model");
      if (
        provider &&
        model &&
        provider.length <= 256 &&
        model.length <= 256 &&
        !/[\u0000-\u001f]/.test(provider + model)
      )
        events.push({
          kind: "model",
          runId,
          stepIndex,
          timestamp,
          model: `${provider}/${model}`,
          thinking: parsePiThinkingLevel(message["thinkingLevel"]),
        });
      if (!Array.isArray(content)) continue;
      for (const [contentIndex, block] of content.slice(0, 128).entries()) {
        if (!Predicate.isObject(block) || block["type"] !== "thinking") continue;
        const text = piRecordString(block, "thinking");
        if (text?.trim())
          events.push({ kind: "thinking", runId, stepIndex, timestamp, contentIndex, text });
      }
      continue;
    }
    const phase =
      type === "tool_execution_start"
        ? "start"
        : type === "tool_execution_update"
          ? "update"
          : type === "tool_execution_end"
            ? "end"
            : undefined;
    const toolCallId = piRecordString(record, "toolCallId");
    const toolName = piRecordString(record, "toolName");
    if (
      phase === undefined ||
      !toolCallId ||
      !toolName ||
      toolCallId.length > 256 ||
      toolName.length > 256
    )
      continue;
    events.push({ kind: "tool", record, runId, stepIndex, toolCallId, toolName, phase });
  }
  return events;
}
