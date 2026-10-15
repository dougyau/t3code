import { describe, expect, it } from "@effect/vitest";
import { piSessionEntries, walkPiBranch } from "./piSessionEntries.ts";

describe("Pi native ancestry", () => {
  it("keeps tool details across custom and compaction ancestors, excluding abandoned branches", () => {
    const details = { tasks: [{ id: 1, subject: "Task", status: "pending" }], nextId: 2 };
    const entries = piSessionEntries([
      { id: "user", parentId: null, type: "message", message: { role: "user" } },
      {
        id: "todo",
        parentId: "user",
        type: "message",
        message: { role: "toolResult", toolName: "todo", toolCallId: "call", details },
      },
      { id: "marker", parentId: "todo", type: "custom" },
      { id: "compact", parentId: "marker", type: "compaction", firstKeptEntryId: "marker" },
      {
        id: "abandoned",
        parentId: "user",
        type: "message",
        message: { role: "toolResult", toolName: "todo", details: { tasks: [], nextId: 1 } },
      },
    ]);
    expect(entries).not.toBeNull();
    const branch = walkPiBranch(entries!, "compact", null);
    expect(branch?.reachedStop).toBe(true);
    expect(branch?.entries.map((entry) => entry.id)).toEqual(["compact", "marker", "todo", "user"]);
    expect(branch?.entries[2]?.message).toEqual({
      role: "toolResult",
      toolName: "todo",
      toolCallId: "call",
      details,
    });
    expect(branch?.userEntryIds).toEqual(["user"]);
  });

  it("distinguishes a partial since window from a complete empty branch", () => {
    const entries = piSessionEntries([{ id: "latest", parentId: "missing", type: "custom" }])!;
    expect(walkPiBranch(entries, "latest", null)?.reachedStop).toBe(false);
    expect(walkPiBranch(entries, "latest", "missing")?.reachedStop).toBe(true);
    expect(walkPiBranch(entries, null, null)).toEqual({
      entries: [],
      entryCount: 0,
      userEntryIds: [],
      reachedStop: true,
    });
  });

  it("rejects duplicate IDs, malformed parents and cyclic branches", () => {
    const entry = { id: "a", parentId: null, type: "custom" };
    expect(piSessionEntries([entry, entry])).toBeNull();
    expect(piSessionEntries([{ ...entry, parentId: 4 }])).toBeNull();
    const entries = piSessionEntries([
      { ...entry, parentId: "b" },
      { id: "b", parentId: "a", type: "compaction" },
    ])!;
    expect(walkPiBranch(entries, "a", null)).toBeNull();
  });
});
