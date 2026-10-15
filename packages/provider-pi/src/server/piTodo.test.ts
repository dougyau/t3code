import { describe, expect, it } from "@effect/vitest";
import { normalizePiTodoBranch, piSessionEntries } from "./piSessionEntries.ts";

const task = (id: number, subject: string, status = "pending") => ({ id, subject, status });
const todo = (id: string, parentId: string | null, tasks: ReadonlyArray<unknown>, nextId = 3) => ({
  id,
  parentId,
  type: "message",
  message: {
    role: "toolResult",
    toolName: "todo",
    toolCallId: `call-${id}`,
    content: [{ type: "text", text: "Filtered display, not membership" }],
    details: { tasks, nextId },
  },
});

describe("Pi native todo selection", () => {
  it("retains hidden and deleted membership across results and all native ancestor kinds", () => {
    const entries = piSessionEntries([
      todo("created", null, [task(1, "Visible"), task(2, "Hidden")]),
      { id: "marker", parentId: "created", type: "custom" },
      todo("filtered", "marker", [task(1, "Visible", "completed"), task(2, "Hidden", "deleted")]),
      { id: "compacted", parentId: "filtered", type: "compaction", firstKeptEntryId: "filtered" },
      todo("abandoned", "created", []),
    ]);
    expect(normalizePiTodoBranch(entries, "compacted")).toEqual({
      state: "ready",
      sourceEntryId: "filtered",
      tasks: [
        { ...task(1, "Visible", "completed"), identityEntryId: "created" },
        { ...task(2, "Hidden", "deleted"), identityEntryId: "created" },
      ],
    });
    expect(normalizePiTodoBranch(entries, "marker").sourceEntryId).toBe("created");
    expect(normalizePiTodoBranch(entries, "abandoned").tasks).toEqual([]);
  });

  it("clear/reuse starts a new native task lifetime; rollback recovers the original identity", () => {
    const entries = piSessionEntries([
      todo("original", null, [task(1, "Original")], 2),
      todo("clear", "original", [], 1),
      todo("reused", "clear", [task(1, "New lifetime")], 2),
    ]);
    expect(normalizePiTodoBranch(entries, "reused").tasks).toEqual([
      { ...task(1, "New lifetime"), identityEntryId: "reused" },
    ]);
    expect(normalizePiTodoBranch(entries, "clear")).toEqual({
      state: "ready",
      sourceEntryId: "clear",
      tasks: [],
    });
    expect(normalizePiTodoBranch(entries, "original").tasks[0]?.identityEntryId).toBe("original");
  });

  it("selected malformed matching data is unavailable, while nonmatching results are ignored", () => {
    const valid = todo("valid", null, [task(1, "Valid")]);
    const malformed = todo("invalid", "valid", [task(1, "", "unknown")]);
    const nonmatching = {
      ...todo("not-a-snapshot", "valid", []),
      message: { role: "toolResult", toolName: "todo", details: { tasks: [], nextId: "wrong" } },
    };
    const entries = piSessionEntries([valid, malformed, nonmatching]);
    expect(normalizePiTodoBranch(entries, "invalid").state).toBe("unavailable");
    expect(normalizePiTodoBranch(entries, "invalid").tasks).toEqual([]);
    expect(normalizePiTodoBranch(entries, "not-a-snapshot").sourceEntryId).toBe("valid");
  });

  it("proves empty selection separately from broken ancestry", () => {
    const entries = piSessionEntries([
      todo("unrelated", null, [task(1, "Other branch")]),
      { id: "empty", parentId: null, type: "custom" },
      { id: "broken", parentId: "missing", type: "custom" },
    ]);
    expect(normalizePiTodoBranch(entries, "empty")).toEqual({
      state: "ready",
      sourceEntryId: null,
      tasks: [],
    });
    expect(normalizePiTodoBranch(entries, "broken").state).toBe("unavailable");
    expect(normalizePiTodoBranch(null, undefined).state).toBe("unavailable");
  });
});
