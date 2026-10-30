import { describe, expect, it } from "vite-plus/test";
import { piTodoBranch } from "./piTodo.ts";

describe("native TODO branch replay", () => {
  it("preserves task incarnations, clear and unavailable without falling back", () => {
    const entry = (id: string, tasks: unknown) => ({
      id,
      message: { role: "toolResult", toolName: "todo", details: { tasks, nextId: 2 } },
    });
    const task = { id: 1, subject: "Inspect", status: "pending" };
    const first = entry("first", [task]);
    const updated = entry("updated", [{ ...task, status: "completed" }]);
    expect(piTodoBranch([first, updated]).tasks[0]?.identityEntryId).toBe("first");
    expect(piTodoBranch([first, entry("clear", []), updated]).tasks[0]?.identityEntryId).toBe(
      "updated",
    );
    expect(piTodoBranch([first, entry("invalid", [{ ...task, status: "unknown" }])]).state).toBe(
      "unavailable",
    );
    expect(piTodoBranch(null).state).toBe("unavailable");
    expect(piTodoBranch([])).toEqual({ state: "ready", sourceEntryId: null, tasks: [] });
  });
});
