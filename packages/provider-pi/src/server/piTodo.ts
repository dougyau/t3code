import * as Schema from "effect/Schema";
import { piRecordField as field, piRecordString as string } from "./rpc.ts";

const Task = Schema.Struct({
  id: Schema.Int.check(Schema.isGreaterThan(0)),
  identityEntryId: Schema.String,
  subject: Schema.String,
  status: Schema.Literals(["pending", "in_progress", "completed", "deleted"]),
});
const decodeTasks = Schema.decodeUnknownSync(Schema.Array(Task));
export type PiTodoSource =
  | { readonly state: "unavailable"; readonly sourceEntryId: null; readonly tasks: readonly [] }
  | {
      readonly state: "ready";
      readonly sourceEntryId: string | null;
      readonly tasks: ReadonlyArray<typeof Task.Type>;
    };

export const unavailablePiTodos: PiTodoSource = {
  state: "unavailable",
  sourceEntryId: null,
  tasks: [],
};

// rpiv-todo snapshots are full membership, including deleted tasks. A clear ends an incarnation.
export function piTodoSnapshot(
  details: unknown,
  sourceEntryId: string,
  previous?: PiTodoSource,
): PiTodoSource {
  const raw = field(details, "tasks");
  if (!Array.isArray(raw) || raw.length > 1000 || typeof field(details, "nextId") !== "number")
    return unavailablePiTodos;
  const identities = new Map(previous?.tasks.map((task) => [task.id, task.identityEntryId]));
  try {
    const tasks = decodeTasks(
      raw.map((task) => ({
        id: field(task, "id"),
        identityEntryId: identities.get(Number(field(task, "id"))) ?? sourceEntryId,
        subject: field(task, "subject"),
        status: field(task, "status"),
      })),
    );
    if (
      new Set(tasks.map((task) => task.id)).size !== tasks.length ||
      tasks.some((task) => !task.subject.trim() || task.subject.length > 2000)
    )
      return unavailablePiTodos;
    return { state: "ready", sourceEntryId, tasks };
  } catch {
    return unavailablePiTodos;
  }
}

/** The caller has walked the selected leaf to its root, oldest entry first. */
export function piTodoBranch(
  branch: ReadonlyArray<{ readonly id: string; readonly message: unknown }> | null,
): PiTodoSource {
  if (branch === null || branch.length > 20_000) return unavailablePiTodos;
  let source: PiTodoSource = { state: "ready", sourceEntryId: null, tasks: [] };
  for (const entry of branch) {
    if (
      string(entry.message, "role") !== "toolResult" ||
      string(entry.message, "toolName") !== "todo"
    )
      continue;
    const details = field(entry.message, "details");
    // Match the native replay predicate before validating the selected snapshot.
    if (Array.isArray(field(details, "tasks")) && typeof field(details, "nextId") === "number")
      source = piTodoSnapshot(details, entry.id, source);
  }
  return source;
}
