import type { PiChecklistCapture, PiChecklistNativeTask } from "@t3tools/contracts";

/** Public snapshot format. One selected entry, not a merged history. */
export const PI_TODO_FORMAT = "rpiv-todo-v1" as const;

const TASK_STATUSES = new Set(["pending", "in_progress", "completed", "deleted"]);

export type PiTodoSnapshotInput = {
  readonly format?: unknown;
  readonly tasks?: unknown;
};

/** Head of the selected matching. Missing head is unsupported, not an empty list. */
export type PiTodoHeadInput =
  | { readonly state: "ready"; readonly sourceEntryId: string | null }
  | { readonly state: "unavailable" };

const unavailable = {
  state: "unavailable",
  sourceEntryId: null,
  tasks: [],
} as const satisfies PiChecklistCapture["source"];

function isNativeTask(value: unknown): value is PiChecklistNativeTask {
  if (typeof value !== "object" || value === null) return false;
  const task = value as Record<string, unknown>;
  return (
    typeof task.id === "number" &&
    Number.isSafeInteger(task.id) &&
    task.id > 0 &&
    typeof task.identityEntryId === "string" &&
    task.identityEntryId.trim().length > 0 &&
    typeof task.subject === "string" &&
    task.subject.trim().length > 0 &&
    typeof task.status === "string" &&
    TASK_STATUSES.has(task.status)
  );
}

/**
 * The only snapshot-to-source path. Keeps deleted and filter-hidden tasks.
 * A malformed selected match is unavailable; it does not fall back to another snapshot.
 * A ready head with no source entry is a proven empty branch and ignores the snapshot.
 */
export function normalizePiTodoSelection(input: {
  readonly head: PiTodoHeadInput | undefined;
  readonly snapshot: PiTodoSnapshotInput | undefined;
}): PiChecklistCapture["source"] {
  if (input.head === undefined || input.head.state === "unavailable") return unavailable;
  if (input.head.sourceEntryId === null) {
    return { state: "ready", sourceEntryId: null, tasks: [] };
  }
  const tasks = input.snapshot?.tasks;
  if (
    input.snapshot?.format !== PI_TODO_FORMAT ||
    !Array.isArray(tasks) ||
    !tasks.every(isNativeTask) ||
    new Set(tasks.map((task) => task.id)).size !== tasks.length
  ) {
    return unavailable;
  }
  return {
    state: "ready",
    sourceEntryId: input.head.sourceEntryId,
    tasks,
  };
}
