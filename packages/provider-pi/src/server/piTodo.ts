import * as HashSet from "effect/HashSet";
import * as Schema from "effect/Schema";
import { piRecordField as field, piRecordString as string } from "./rpc.ts";

const Task = Schema.Struct({
  id: Schema.Int.check(Schema.isGreaterThan(0)),
  identityEntryId: Schema.String,
  subject: Schema.String,
  parentId: Schema.optional(Schema.Int.check(Schema.isGreaterThan(0))),
  status: Schema.Literals(["pending", "in_progress", "completed", "cancelled", "deleted"]),
});
const decodeTasks = Schema.decodeUnknownSync(Schema.Array(Task));
export type PiTodoSource =
  | { readonly state: "unavailable"; readonly sourceEntryId: null; readonly tasks: readonly [] }
  | {
      readonly state: "ready";
      readonly sourceEntryId: string | null;
      readonly tasks: ReadonlyArray<typeof Task.Type>;
    };

const unavailablePiTodos: PiTodoSource = {
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
      raw.map((task) => {
        const parentId = field(task, "parentId");
        return {
          id: field(task, "id"),
          identityEntryId: identities.get(Number(field(task, "id"))) ?? sourceEntryId,
          subject: field(task, "subject"),
          status: field(task, "status"),
          parentId:
            typeof parentId === "number" && Number.isInteger(parentId) && parentId > 0
              ? parentId
              : undefined,
        };
      }),
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

export type PiTodoFold = {
  readonly chain: Extract<PiTodoSource, { state: "ready" }>;
  readonly winner: PiTodoSource;
  readonly winnerIsState: boolean;
  readonly seen: HashSet.HashSet<string>;
};

type PiTodoEntry = {
  readonly id: string;
  readonly type?: unknown;
  readonly customType?: unknown;
  readonly data?: unknown;
  readonly message?: unknown;
};

export function piTodoEmptyFold(): PiTodoFold {
  const chain = { state: "ready" as const, sourceEntryId: null, tasks: [] };
  return { chain, winner: chain, winnerIsState: false, seen: HashSet.empty() };
}

export function piTodoStep(fold: PiTodoFold, entry: PiTodoEntry): PiTodoFold {
  if (HashSet.has(fold.seen, entry.id)) return fold;
  const isState =
    entry.type === "custom" &&
    (entry.customType === "rpiv-todo-state" ||
      (entry.customType === "pitako-todo-state" && field(entry.data, "version") === 1));
  let payload: unknown;
  if (isState) payload = entry.data;
  else if (
    string(entry.message, "role") === "toolResult" &&
    string(entry.message, "toolName") === "todo"
  )
    payload = field(entry.message, "details");
  else return fold;
  if (!Array.isArray(field(payload, "tasks")) || typeof field(payload, "nextId") !== "number")
    return fold;
  let snap: PiTodoSource;
  try {
    snap =
      isState && Buffer.byteLength(JSON.stringify(payload), "utf8") > 262144
        ? unavailablePiTodos
        : piTodoSnapshot(payload, entry.id, fold.chain);
  } catch {
    snap = unavailablePiTodos;
  }
  return {
    chain: snap.state === "ready" ? snap : fold.chain,
    winner: isState || !fold.winnerIsState ? snap : fold.winner,
    winnerIsState: fold.winnerIsState || isState,
    seen: HashSet.add(fold.seen, entry.id),
  };
}

/** The caller has walked the selected leaf to its root, oldest entry first. */
export function piTodoBranch(branch: ReadonlyArray<PiTodoEntry> | null): PiTodoFold {
  if (branch === null || branch.length > 20_000)
    return { ...piTodoEmptyFold(), winner: unavailablePiTodos };
  return branch.reduce(piTodoStep, piTodoEmptyFold());
}

export function piTodoSteps(tasks: PiTodoSource["tasks"]) {
  const byId = new Map(tasks.map((task) => [task.id, task]));
  const children = new Map<number, Array<typeof Task.Type>>();
  const roots: Array<typeof Task.Type> = [];
  for (const task of tasks) {
    if (task.status === "deleted") continue;
    const parent = task.parentId === undefined ? undefined : byId.get(task.parentId);
    if (
      parent &&
      parent.id !== task.id &&
      parent.status !== "deleted" &&
      parent.parentId === undefined
    ) {
      const siblings = children.get(parent.id);
      if (siblings) siblings.push(task);
      else children.set(parent.id, [task]);
    } else roots.push(task);
  }
  const step = (task: typeof Task.Type, child: boolean) => ({
    id: `${task.identityEntryId}:${task.id}`,
    // ↳ marks children; ⊘ marks cancellation without changing the clients' three step statuses.
    text: `${child ? "↳ " : ""}${task.status === "cancelled" ? "⊘ " : ""}${task.subject.trim()}`,
    status:
      task.status === "in_progress"
        ? ("running" as const)
        : task.status === "completed" || task.status === "cancelled"
          ? ("completed" as const)
          : ("pending" as const),
  });
  return roots.flatMap((task) => [
    step(task, false),
    ...(children.get(task.id) ?? []).map((child) => step(child, true)),
  ]);
}
