import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import {
  nativeTodoBranch,
  withNativeTodo,
  type NativeTodoSnapshot,
} from "./piTodo.native.fixture.ts";

const Details = Schema.Struct({
  action: Schema.String,
  nextId: Schema.Number,
  tasks: Schema.Array(
    Schema.Struct({ id: Schema.Number, subject: Schema.String, status: Schema.String }),
  ),
});
const decodeDetails = Schema.decodeUnknownSync(Details);
const decodeLiveResult = Schema.decodeUnknownSync(Schema.Struct({ details: Schema.Unknown }));
const decodeMessages = Schema.decodeUnknownSync(
  Schema.Struct({
    messages: Schema.Array(
      Schema.Struct({
        role: Schema.String,
        toolName: Schema.optional(Schema.String),
      }),
    ),
  }),
);
const latestTodo = (snapshot: NativeTodoSnapshot) =>
  nativeTodoBranch(snapshot).findLast(
    (entry) => entry.message?.role === "toolResult" && entry.message.toolName === "todo",
  );
const membership = (snapshot: NativeTodoSnapshot) =>
  decodeDetails(latestTodo(snapshot)?.message?.details).tasks;

const configured =
  process.env.T3_PI_TODO_NATIVE_PI_ROOT !== undefined &&
  process.env.T3_PI_TODO_NATIVE_TODO_ROOT !== undefined;

describe.skipIf(!configured)("installed Pi / rpiv-todo native lineage (offline)", () => {
  it.live("proves append timing, full membership, clear reuse, and lifecycle ancestry", () =>
    withNativeTodo((fixture) =>
      Effect.gen(function* () {
        yield* fixture.prompt("Native empty baseline");
        const empty = yield* fixture.capture("empty baseline");
        assert.isUndefined(latestTodo(empty));
        const emptyLeaf = empty.leafId!;

        const first = yield* fixture.todo({ action: "create", subject: "Original identity" });
        assert.equal(first.before.toolCallId, first.live["toolCallId"]);
        assert.equal(first.before.sessionId, first.snapshot.sessionId);
        assert.equal(first.before.sessionFile, first.snapshot.sessionFile);
        assert.notEqual(first.before.leafId, first.result.id);
        for (const entries of [first.before.entries, first.before.persisted]) {
          assert.isFalse(
            entries.some(
              (entry) =>
                entry.message?.toolCallId === first.live["toolCallId"] &&
                entry.message?.role === "toolResult",
            ),
          );
        }
        assert.equal(first.result.parentId, first.before.leafId);
        assert.deepEqual(
          first.result.message?.details,
          decodeLiveResult(first.live["result"]).details,
        );
        assert.equal(membership(first.snapshot)[0]?.id, 1);
        assert.equal(membership(first.snapshot)[0]?.subject, "Original identity");

        yield* fixture.todo({ action: "create", subject: "Hidden pending task" });
        yield* fixture.todo({ action: "update", id: 1, status: "in_progress" });
        yield* fixture.todo({ action: "update", id: 1, status: "completed" });
        const filtered = yield* fixture.todo({ action: "list", status: "completed" });
        assert.deepEqual(
          membership(filtered.snapshot).map((task) => task.id),
          [1, 2],
        );
        assert.include(JSON.stringify(filtered.result.message?.content), "Original identity");
        assert.notInclude(JSON.stringify(filtered.result.message?.content), "Hidden pending task");
        const singleton = yield* fixture.todo({ action: "get", id: 1 });
        assert.deepEqual(
          membership(singleton.snapshot).map((task) => task.id),
          [1, 2],
        );
        assert.notInclude(JSON.stringify(singleton.result.message?.content), "Hidden pending task");
        const deleted = yield* fixture.todo({ action: "delete", id: 1 });
        assert.deepEqual(
          membership(deleted.snapshot).map((task) => task.status),
          ["deleted", "pending"],
        );
        const visibleList = yield* fixture.todo({ action: "list" });
        assert.deepEqual(
          membership(visibleList.snapshot).map((task) => task.id),
          [1, 2],
        );
        assert.notInclude(JSON.stringify(visibleList.result.message?.content), "Original identity");
        const tombstones = yield* fixture.todo({ action: "list", includeDeleted: true });
        assert.include(JSON.stringify(tombstones.result.message?.content), "[deleted]");
        const cleared = yield* fixture.todo({ action: "clear" });
        assert.deepEqual(membership(cleared.snapshot), []);
        assert.equal(decodeDetails(cleared.result.message?.details).nextId, 1);
        const reused = yield* fixture.todo({ action: "create", subject: "New identity" });
        assert.deepEqual(
          membership(reused.snapshot).map((task) => [task.id, task.subject]),
          [[1, "New identity"]],
        );
        assert.notEqual(first.result.id, reused.result.id);
        const populated = reused.snapshot;

        // Native tree navigation preserves abandoned entries in get_entries.
        yield* fixture.request({ type: "prompt", message: `/fixture-tree ${emptyLeaf}` });
        const noTodo = yield* fixture.capture("active branch without todo");
        assert.equal(noTodo.sessionId, populated.sessionId);
        assert.isTrue(noTodo.entries.some((entry) => entry.id === reused.result.id));
        assert.isUndefined(latestTodo(noTodo));
        const emptyList = yield* fixture.todo({ action: "list" });
        assert.deepEqual(membership(emptyList.snapshot), []);
        assert.notEqual(emptyList.result.id, reused.result.id);
        yield* fixture.request({ type: "prompt", message: `/fixture-tree ${populated.leafId}` });
        const restored = yield* fixture.todo({ action: "list" });
        assert.deepEqual(
          membership(restored.snapshot).map((task) => task.subject),
          ["New identity"],
        );
        const originalFile = restored.snapshot.sessionFile;

        yield* fixture.reopen(originalFile);
        const reopened = yield* fixture.capture("process reopen");
        assert.equal(reopened.sessionId, populated.sessionId);
        assert.equal(latestTodo(reopened)?.id, restored.result.id);
        const afterReopen = yield* fixture.todo({ action: "list" });
        assert.deepEqual(
          membership(afterReopen.snapshot).map((task) => task.subject),
          ["New identity"],
        );

        yield* fixture.request({ type: "new_session" });
        yield* fixture.prompt("Independent new native session");
        const fresh = yield* fixture.capture("new session has no inherited checklist");
        assert.notEqual(fresh.sessionId, reopened.sessionId);
        assert.isUndefined(latestTodo(fresh));
        assert.deepEqual(membership((yield* fixture.todo({ action: "list" })).snapshot), []);
        yield* fixture.request({ type: "switch_session", sessionPath: originalFile });
        const resumed = yield* fixture.todo({ action: "list" });
        assert.equal(resumed.snapshot.sessionId, populated.sessionId);
        assert.deepEqual(
          membership(resumed.snapshot).map((task) => task.subject),
          ["New identity"],
        );

        // Pi fork(entryId) selects BEFORE a user message: also T3's native rollback primitive.
        const rollbackTurn = yield* fixture.todo({
          action: "create",
          subject: "Discard on rollback",
        });
        const branch = nativeTodoBranch(rollbackTurn.snapshot);
        const user = branch.findLast((entry) => entry.message?.role === "user")!;
        yield* fixture.request({ type: "fork", entryId: user.id });
        const forked = yield* fixture.capture(
          "native fork before discarded user / rollback boundary",
        );
        assert.notEqual(forked.sessionId, resumed.snapshot.sessionId);
        assert.notEqual(forked.sessionFile, originalFile);
        assert.isFalse(
          nativeTodoBranch(forked).some((entry) => entry.id === rollbackTurn.result.id),
        );
        assert.deepEqual(
          membership(forked).map((task) => task.subject),
          ["New identity"],
        );
        assert.deepEqual(
          membership((yield* fixture.todo({ action: "list" })).snapshot).map(
            (task) => task.subject,
          ),
          ["New identity"],
        );
        const forkReuse = yield* fixture.todo({ action: "create", subject: "Fork new identity" });
        assert.equal(membership(rollbackTurn.snapshot)[1]?.id, 2);
        assert.equal(membership(forkReuse.snapshot)[1]?.id, 2);
        assert.notEqual(rollbackTurn.result.id, forkReuse.result.id);
        const forkSubjects = ["New identity", "Fork new identity"];

        yield* fixture.request({ type: "prompt", message: "/fixture-marker" });
        const marker = yield* fixture.capture("non-message active leaf");
        assert.equal(marker.entries.find((entry) => entry.id === marker.leafId)?.type, "custom");
        assert.deepEqual(
          membership(marker).map((task) => task.subject),
          forkSubjects,
        );
        yield* fixture.request({ type: "compact" });
        const compacted = yield* fixture.capture("native compaction active leaf");
        const compaction = compacted.entries.find((entry) => entry.id === compacted.leafId)!;
        assert.equal(compaction.type, "compaction");
        assert.equal(compaction.parentId, marker.leafId);
        assert.equal(compaction.firstKeptEntryId, marker.leafId);
        assert.deepEqual(
          membership(compacted).map((task) => task.subject),
          forkSubjects,
        );
        const compactedMessages = decodeMessages(yield* fixture.request({ type: "get_messages" }));
        assert.isFalse(
          compactedMessages.messages.some(
            (message) => message.role === "toolResult" && message.toolName === "todo",
          ),
        );
        yield* fixture.reopen(compacted.sessionFile);
        const compactedReopen = yield* fixture.capture("reopen compaction ancestry");
        assert.equal(compactedReopen.leafId, compacted.leafId);
        const postCompaction = yield* fixture.todo({ action: "list" });
        assert.deepEqual(
          membership(postCompaction.snapshot).map((task) => task.subject),
          forkSubjects,
        );

        // Re-fork the original branch before its very first todo: no cross-session carry.
        yield* fixture.request({ type: "switch_session", sessionPath: originalFile });
        const untouchedOriginal = yield* fixture.capture(
          "original remains independent of rollback fork",
        );
        assert.deepEqual(
          membership(untouchedOriginal).map((task) => task.subject),
          ["New identity", "Discard on rollback"],
        );
        const firstUser = nativeTodoBranch(first.snapshot).findLast(
          (entry) => entry.message?.role === "user",
        )!;
        yield* fixture.request({ type: "fork", entryId: firstUser.id });
        const emptyFork = yield* fixture.capture("fork without todo ancestry");
        assert.isUndefined(latestTodo(emptyFork));
        assert.deepEqual(membership((yield* fixture.todo({ action: "list" })).snapshot), []);
      }),
    ),
  );
});
