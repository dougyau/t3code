import { describe, expect, it } from "vite-plus/test";

import { normalizePiTodoSelection, PI_TODO_FORMAT } from "./piTodo.ts";

const task = {
  id: 1,
  identityEntryId: "entry-task-1",
  subject: "Keep the deleted row",
  status: "deleted",
};

describe("normalizePiTodoSelection", () => {
  it.each([
    [task, { ...task, identityEntryId: "another-lifespan" }],
    [task, { ...task, status: "pending" }],
    [{ ...task, id: 0 }],
    [{ ...task, id: -1 }],
    [{ ...task, id: Number.MAX_SAFE_INTEGER + 1 }],
    [{ ...task, subject: "   " }],
    [{ ...task, identityEntryId: "" }],
    [{ ...task, status: "cancelled" }],
  ])(
    "rejects malformed selected full membership without retaining earlier tasks (%j)",
    (...tasks) => {
      expect(
        normalizePiTodoSelection({
          head: { state: "ready", sourceEntryId: "selected-malformed" },
          snapshot: { format: PI_TODO_FORMAT, tasks },
        }),
      ).toEqual({ state: "unavailable", sourceEntryId: null, tasks: [] });
    },
  );

  it("treats a missing head as unavailable rather than an empty list", () => {
    expect(normalizePiTodoSelection({ head: undefined, snapshot: undefined })).toEqual({
      state: "unavailable",
      sourceEntryId: null,
      tasks: [],
    });
  });

  it("does not fall back when the selected snapshot is malformed", () => {
    expect(
      normalizePiTodoSelection({
        head: { state: "ready", sourceEntryId: "entry-1" },
        snapshot: {
          format: PI_TODO_FORMAT,
          tasks: [
            { ...task, status: "pending" },
            { ...task, id: 1.5 },
          ],
        },
      }),
    ).toEqual({ state: "unavailable", sourceEntryId: null, tasks: [] });
  });

  it("keeps a proven empty branch empty and ignores a stale snapshot", () => {
    expect(
      normalizePiTodoSelection({
        head: { state: "ready", sourceEntryId: null },
        snapshot: { format: PI_TODO_FORMAT, tasks: [{ ...task, status: "pending" }] },
      }),
    ).toEqual({ state: "ready", sourceEntryId: null, tasks: [] });
  });

  it("keeps deleted membership from the selected snapshot", () => {
    expect(
      normalizePiTodoSelection({
        head: { state: "ready", sourceEntryId: "entry-1" },
        snapshot: { format: PI_TODO_FORMAT, tasks: [task, { ...task, id: 2, status: "pending" }] },
      }).tasks,
    ).toEqual([task, { ...task, id: 2, status: "pending" }]);
  });
});
