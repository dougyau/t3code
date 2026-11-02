import { describe, expect, it } from "vite-plus/test";
import {
  piTodoBranch,
  piTodoEmptyFold,
  piTodoSnapshot,
  piTodoStep,
  piTodoSteps,
} from "./piTodo.ts";

const task = { id: 1, subject: "Inspect", status: "pending" };
const tool = (id: string, tasks: unknown) => ({
  id,
  message: { role: "toolResult", toolName: "todo", details: { tasks, nextId: 3 } },
});
const state = (id: string, tasks: unknown, extra = {}) => ({
  id,
  type: "custom",
  customType: "pitako-todo-state",
  data: { version: 1, tasks, nextId: 3, ...extra },
});
const first = tool("first", [task]);
const updated = tool("updated", [{ ...task, status: "completed" }]);
const invalid = state("invalid", [{ ...task, status: "unknown" }]);
const clear = state("clear", []);
const saved = state("saved", [task]);
const legacy = {
  ...state("legacy", [task]),
  customType: "rpiv-todo-state",
  data: { tasks: [task], nextId: 3 },
};

describe("native TODO branch replay", () => {
  it("preserves task incarnations, clear and unavailable without falling back", () => {
    const cases = [
      { entries: [first, updated], source: "updated", identities: ["first"] },
      { entries: [first, tool("clear", []), updated], source: "updated", identities: ["updated"] },
      {
        entries: [first, tool("bad", [{ ...task, status: "unknown" }])],
        source: null,
        identities: [],
        unavailable: true,
      },
      { entries: [saved, updated], source: "saved", identities: ["saved"] },
      { entries: [first, invalid], source: null, identities: [], unavailable: true },
      { entries: [first, invalid, updated], source: null, identities: [], unavailable: true },
      { entries: [first, invalid, updated, saved], source: "saved", identities: ["first"] },
      {
        entries: [first, tool("bad", [{ ...task, status: "unknown" }]), updated],
        source: "updated",
        identities: ["first"],
      },
      {
        entries: [state("missing", [task], { version: undefined }), updated],
        source: "updated",
        identities: ["updated"],
      },
      {
        entries: [state("future", [task], { version: 2 }), updated],
        source: "updated",
        identities: ["updated"],
      },
      { entries: [legacy, updated], source: "legacy", identities: ["legacy"] },
      {
        entries: [saved, state("large", [task], { metadata: "é".repeat(131072) })],
        source: null,
        identities: [],
        unavailable: true,
      },
      {
        entries: [state("small", [task], { metadata: "a".repeat(131072) })],
        source: "small",
        identities: ["small"],
      },
      {
        entries: [
          state(
            "parents",
            [0, -1, 1.5, "1", null, {}, undefined].map((parentId, i) => ({
              ...task,
              id: i + 1,
              parentId,
            })),
          ),
        ],
        source: "parents",
        identities: Array(7).fill("parents"),
      },
      {
        entries: [tool("two", [task, { ...task, id: 2, status: "deleted" }]), saved],
        source: "saved",
        identities: ["two"],
      },
      { entries: [first, clear, first, saved], source: "saved", identities: ["saved"] },
      {
        entries: [saved, state("shape", null), { id: "unrelated", message: { role: "user" } }],
        source: "saved",
        identities: ["saved"],
      },
      {
        entries: [
          saved,
          tool("loser", [task, { ...task, id: 2 }]),
          state("both", [task, { ...task, id: 2 }]),
        ],
        source: "both",
        identities: ["saved", "loser"],
      },
      {
        entries: [saved, state("duplicate", [task, task])],
        source: null,
        identities: [],
        unavailable: true,
      },
      {
        entries: [saved, state("blank", [{ ...task, subject: "  " }])],
        source: null,
        identities: [],
        unavailable: true,
      },
      {
        entries: [saved, state("long", [{ ...task, subject: "a".repeat(2001) }])],
        source: null,
        identities: [],
        unavailable: true,
      },
      {
        entries: [
          saved,
          state(
            "many",
            Array.from({ length: 1001 }, (_, i) => ({ ...task, id: i + 1 })),
          ),
        ],
        source: null,
        identities: [],
        unavailable: true,
      },
    ];
    for (const { entries, source, identities, unavailable } of cases) {
      const fold = entries.reduce(piTodoStep, piTodoEmptyFold());
      expect(fold.winner, JSON.stringify(entries).slice(0, 200)).toEqual(
        piTodoBranch(entries).winner,
      );
      expect(fold.winner.state).toBe(unavailable ? "unavailable" : "ready");
      expect(fold.winner.sourceEntryId).toBe(source);
      expect(fold.winner.tasks.map((t) => t.identityEntryId)).toEqual(identities);
      const last = entries.at(-1)!;
      expect(piTodoStep(fold, last)).toBe(fold);
    }
    const parents = piTodoBranch([
      state("parents", [task, { ...task, id: 2, parentId: 1 }, { ...task, id: 3, parentId: "1" }]),
    ]).winner;
    expect(parents.tasks.map((t) => t.parentId)).toEqual([undefined, 1, undefined]);
    expect(piTodoBranch(null).winner.state).toBe("unavailable");
    expect(piTodoBranch(Array.from({ length: 20001 }, () => first)).winner.state).toBe(
      "unavailable",
    );
    expect(piTodoBranch([]).winner).toEqual({ state: "ready", sourceEntryId: null, tasks: [] });
  });

  it.each([
    {
      name: "statuses and trimmed text",
      tasks: [
        { ...task, status: "cancelled", subject: "  Stop  " },
        { ...task, id: 2, status: "in_progress" },
        { ...task, id: 3, status: "completed" },
        { ...task, id: 4, status: "deleted" },
      ],
      rows: [
        [1, "⊘ Stop", "completed"],
        [2, "Inspect", "running"],
        [3, "Inspect", "completed"],
      ],
    },
    {
      name: "children before parent and relative order",
      tasks: [
        { ...task, id: 2, parentId: 1, subject: "  Child  ", status: "cancelled" },
        { ...task, id: 4 },
        task,
        { ...task, id: 3, parentId: 1 },
      ],
      rows: [
        [4, "Inspect", "pending"],
        [1, "Inspect", "pending"],
        [2, "↳ ⊘ Child", "completed"],
        [3, "↳ Inspect", "pending"],
      ],
    },
    {
      name: "orphan deleted nested and self parents",
      tasks: [
        { ...task, parentId: 99 },
        { ...task, id: 2, parentId: 3 },
        { ...task, id: 3, status: "deleted" },
        { ...task, id: 4, parentId: 1 },
        { ...task, id: 5, parentId: 5 },
      ],
      rows: [
        [1, "Inspect", "pending"],
        [2, "Inspect", "pending"],
        [4, "Inspect", "pending"],
        [5, "Inspect", "pending"],
      ],
    },
    {
      name: "no truncation",
      tasks: [{ ...task, subject: "x".repeat(2000), status: "cancelled" }],
      rows: [[1, `⊘ ${"x".repeat(2000)}`, "completed"]],
    },
  ])("renders $name", ({ tasks, rows }) => {
    expect(piTodoSteps(piTodoSnapshot({ tasks, nextId: 6 }, "snapshot").tasks)).toEqual(
      rows.map(([id, text, status]) => ({ id: `snapshot:${id}`, text, status })),
    );
  });
});
