import { piRecordField, piRecordString } from "./rpc.ts";
import type { PiChecklistCapture } from "@t3tools/contracts";
import { normalizePiTodoSelection, PI_TODO_FORMAT } from "@t3tools/shared/piTodo";

export interface PiSessionEntry {
  readonly id: string;
  readonly parentId: string | null;
  readonly type: string;
  readonly message?: {
    readonly role: string;
    readonly toolName?: string;
    readonly toolCallId?: string;
    readonly details?: unknown;
  };
}

/** Index all entry kinds: non-message entries are part of the native ancestry. */
export function piSessionEntries(entries: unknown): ReadonlyMap<string, PiSessionEntry> | null {
  if (!Array.isArray(entries)) return null;
  const byId = new Map<string, PiSessionEntry>();
  for (const entry of entries) {
    const id = piRecordString(entry, "id");
    const parentId = piRecordField(entry, "parentId");
    const type = piRecordString(entry, "type");
    if (
      id === undefined ||
      byId.has(id) ||
      type === undefined ||
      (parentId !== null && typeof parentId !== "string")
    )
      return null;
    const message = piRecordField(entry, "message");
    const role = piRecordString(message, "role");
    const toolName = piRecordString(message, "toolName");
    const toolCallId = piRecordString(message, "toolCallId");
    byId.set(id, {
      id,
      parentId,
      type,
      ...(role === undefined
        ? {}
        : {
            message: {
              role,
              ...(toolName === undefined ? {} : { toolName }),
              ...(toolCallId === undefined ? {} : { toolCallId }),
              details: piRecordField(message, "details"),
            },
          }),
    });
  }
  return byId;
}

/**
 * Walk newest first. A partial `since` listing may end before stopAt; only
 * reachedStop proves a complete branch. Cycles are never valid ancestry.
 */
export function walkPiBranch(
  entries: ReadonlyMap<string, PiSessionEntry>,
  leafId: string | null,
  stopAt: string | null,
): {
  readonly entries: ReadonlyArray<PiSessionEntry>;
  readonly entryCount: number;
  readonly userEntryIds: ReadonlyArray<string>;
  readonly reachedStop: boolean;
} | null {
  const visited = new Set<string>();
  const branch: Array<PiSessionEntry> = [];
  const userEntryIds: Array<string> = [];
  let current = leafId;
  while (current !== null && current !== stopAt) {
    const entry = entries.get(current);
    if (entry === undefined) break;
    if (visited.has(current)) return null;
    visited.add(current);
    branch.push(entry);
    if (entry.type === "message" && entry.message?.role === "user") userEntryIds.push(current);
    current = entry.parentId;
  }
  return {
    entries: branch,
    entryCount: visited.size,
    userEntryIds,
    reachedStop: current === stopAt,
  };
}

/** Replay native full membership, then use the shared public/native selection validator. */
export function normalizePiTodoBranch(
  entries: ReadonlyMap<string, PiSessionEntry> | null,
  leafId: string | null | undefined,
): PiChecklistCapture["source"] {
  const branch =
    entries === null || leafId === undefined ? null : walkPiBranch(entries, leafId, null);
  if (branch === null || !branch.reachedStop) {
    return normalizePiTodoSelection({ head: { state: "unavailable" }, snapshot: undefined });
  }
  let identities = new Map<number, string>();
  let sourceEntryId: string | null = null;
  let tasks: ReadonlyArray<unknown> = [];
  for (const entry of branch.entries.toReversed()) {
    const message = entry.message;
    if (entry.type !== "message" || message?.role !== "toolResult" || message.toolName !== "todo")
      continue;
    const nativeTasks = piRecordField(message.details, "tasks");
    // This is replayFromBranch's matching predicate, not task validity.
    if (!Array.isArray(nativeTasks) || typeof piRecordField(message.details, "nextId") !== "number")
      continue;
    const nextIdentities = new Map<number, string>();
    tasks = nativeTasks.map((task) => {
      const id = piRecordField(task, "id");
      const identityEntryId =
        typeof id === "number" && Number.isInteger(id) && id >= 0
          ? (identities.get(id) ?? entry.id)
          : undefined;
      if (typeof id === "number" && identityEntryId !== undefined)
        nextIdentities.set(id, identityEntryId);
      return {
        id,
        identityEntryId,
        subject: piRecordField(task, "subject"),
        status: piRecordField(task, "status"),
      };
    });
    identities = nextIdentities;
    sourceEntryId = entry.id;
  }
  return normalizePiTodoSelection({
    head: { state: "ready", sourceEntryId },
    snapshot: { format: PI_TODO_FORMAT, tasks },
  });
}
