import { describe, expect, it } from "vite-plus/test";
import { piSubagentEventLines, type PiSubagentEventCursor } from "./subagentEvents.ts";

describe("native public event framing", () => {
  it("retains the model and thinking level reported by the child's own assistant message", () => {
    const cursor: PiSubagentEventCursor = { offset: 0, pending: "", skipLine: false };
    const records = piSubagentEventLines(
      cursor,
      JSON.stringify({
        type: "message_end",
        subagentSource: "child",
        subagentRunId: "child",
        subagentStepIndex: 0,
        message: {
          role: "assistant",
          provider: "openai-codex",
          model: "gpt-6.1-sol",
          thinkingLevel: "high",
          timestamp: 123,
          content: [],
        },
      }) + "\n",
    );
    expect(records).toContainEqual({
      kind: "model",
      runId: "child",
      stepIndex: 0,
      timestamp: 123,
      model: "openai-codex/gpt-6.1-sol",
      thinking: "high",
    });
  });
  it("resumes partial lines and rejects unrelated or oversized records", () => {
    const cursor: PiSubagentEventCursor = { offset: 0, pending: "", skipLine: false };
    const event = JSON.stringify({
      type: "tool_execution_end",
      subagentSource: "child",
      subagentRunId: "run",
      subagentStepIndex: 0,
      toolCallId: "call",
      toolName: "read",
      isError: false,
      result: { content: [{ type: "text", text: "result" }] },
    });
    expect(piSubagentEventLines(cursor, event.slice(0, 30))).toEqual([]);
    expect(piSubagentEventLines(cursor, event.slice(30) + "\n")[0]).toMatchObject({
      runId: "run",
      toolCallId: "call",
      toolName: "read",
      phase: "end",
    });
    expect(piSubagentEventLines(cursor, "é".repeat(600_000))).toEqual([]);
    expect(cursor.skippedRecords).toBe(1);
    expect(
      piSubagentEventLines(cursor, 'discard\n{"type":"tool_execution_end"}\n' + event + "\n"),
    ).toHaveLength(1);
  });

  it("keeps a large tool result split across bounded UTF-8 reads", () => {
    const cursor: PiSubagentEventCursor = { offset: 0, pending: "", skipLine: false };
    const text = "é".repeat(90_000);
    const bytes = new TextEncoder().encode(
      JSON.stringify({
        type: "tool_execution_end",
        subagentSource: "child",
        subagentRunId: "run",
        subagentStepIndex: 0,
        toolCallId: "call",
        toolName: "bash",
        isError: false,
        result: { content: [{ type: "text", text }] },
      }) + "\n",
    );
    const decoder = new TextDecoder();
    expect(
      piSubagentEventLines(cursor, decoder.decode(bytes.subarray(0, 65_535), { stream: true })),
    ).toEqual([]);
    const events = piSubagentEventLines(
      cursor,
      decoder.decode(bytes.subarray(65_535), { stream: true }),
    );
    expect(events).toHaveLength(1);
    expect(events.find((event) => event.kind === "tool")?.record["result"]).toEqual({
      content: [{ type: "text", text }],
    });
    expect(cursor.pending).toBe("");
  });

  it("keeps public thinking separate from answer text and encrypted signatures", () => {
    const cursor: PiSubagentEventCursor = { offset: 0, pending: "", skipLine: false };
    const event = {
      type: "message_end",
      subagentSource: "child",
      subagentRunId: "run",
      subagentStepIndex: 0,
      message: {
        role: "assistant",
        timestamp: 123,
        content: [
          { type: "thinking", thinking: "Public reasoning", thinkingSignature: "not display text" },
          { type: "thinking", thinking: "", thinkingSignature: "encrypted only" },
          { type: "text", text: "Final answer" },
        ],
      },
    };
    expect(piSubagentEventLines(cursor, JSON.stringify(event) + "\n")).toEqual([
      {
        kind: "thinking",
        runId: "run",
        stepIndex: 0,
        timestamp: 123,
        contentIndex: 0,
        text: "Public reasoning",
      },
    ]);
    expect(
      piSubagentEventLines(
        cursor,
        JSON.stringify({ ...event, message: { ...event.message, role: "user" } }) + "\n",
      ),
    ).toEqual([]);
  });
});
