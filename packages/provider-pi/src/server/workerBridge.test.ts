// @effect-diagnostics nodeBuiltinImport:off -- Private native directory ownership is the boundary under test.
import * as NodeFS from "node:fs";
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Cause from "effect/Cause";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";
import {
  PiWorkerBridge,
  WorkerSourceError,
  NativeRecordAssembler,
  classifyPiWorkerFailure,
} from "./workerBridge.ts";
import { BridgeError } from "./pitako/bridge/client.js";
import {
  errors,
  capabilities,
  type PublicWorkerDetail,
  type PublicRecord,
  type PublicConversationPage,
  type PublicFragment,
} from "./pitako/bridge/protocol.js";
import { workerWireFixture } from "./workerBridge.wire.fixture.ts";
import {
  handoffWorkerRows,
  invalidHandoffWorkerRows,
  legacyWorkerRow,
} from "../../../contracts/src/providerWorkers.fixture.ts";
import { PiNativeWorkerLaunch, PiNativeWorkerCheckpoint } from "@t3tools/contracts";

describe("passive worker handoff wire boundary", () => {
  const owner = { processId: "process", coordinatorSessionId: "session", epoch: "epoch" };
  it.effect("conserves every public combination through real list and subscription replies", () =>
    Effect.gen(function* () {
      const f = yield* workerWireFixture(owner);
      for (const row of handoffWorkerRows) {
        const roster = { owner, revision: 1, cursor: null, items: [row] };
        f.policy.roster = roster;
        assert.deepEqual(yield* Effect.promise(() => f.client.list()), roster);
        const changed = Promise.withResolvers<unknown>();
        const subscription = yield* Effect.promise(() => f.client.subscribe(changed.resolve));
        assert.deepEqual(subscription.snapshot, roster);
        f.policy.roster = { ...roster, revision: 2 };
        f.notify(2);
        assert.deepEqual(yield* Effect.promise(() => changed.promise), {
          version: 2,
          method: "changed",
          owner,
          clientId: f.client.lease.clientId,
          subscriptionId: "subscription",
          revision: 2,
          refreshRequired: true,
          gap: false,
        });
        assert.deepEqual(yield* Effect.promise(() => subscription.refresh()), f.policy.roster);
        yield* Effect.promise(() => subscription.dispose());
      }
      assert.deepEqual(f.calls, [
        "connect",
        ...handoffWorkerRows.flatMap(() => [
          "workers.list",
          "subscribe",
          "workers.list",
          "unsubscribe",
        ]),
      ]);
      yield* Effect.promise(() => f.client.disconnect());
    }).pipe(Effect.scoped),
  );

  it.effect("returns observable transport errors for invalid list and subscription payloads", () =>
    Effect.gen(function* () {
      const f = yield* workerWireFixture(owner);
      const badPages = [
        ...invalidHandoffWorkerRows.map((row) => ({
          owner,
          revision: 1,
          cursor: null,
          items: [row],
        })),
        { owner, revision: 1, cursor: null, items: [{ ...legacyWorkerRow, futureField: true }] },
        {
          owner,
          revision: 1,
          cursor: null,
          items: [
            {
              ...handoffWorkerRows[3],
              handoff: {
                collection: "collected",
                bookkeeping: "pending",
                delivery: "offered",
                continuation: "requested",
                futureField: true,
              },
            },
          ],
        },
        {
          owner,
          revision: 1,
          cursor: null,
          items: Array.from({ length: 51 }, () => handoffWorkerRows[3]),
        },
        {
          owner: { ...owner, epoch: "foreign" },
          revision: 1,
          cursor: null,
          items: [legacyWorkerRow],
        },
      ];
      for (const page of badPages) {
        for (const method of ["list", "subscribe"] as const) {
          const client = yield* Effect.promise(() => f.open());
          f.policy.roster = page;
          const error = yield* Effect.tryPromise(async () =>
            method === "list"
              ? client.list()
              : client.subscribe(() => assert.fail("Invalid subscription must not notify")),
          ).pipe(Effect.flip);
          assert.deepEqual(classifyPiWorkerFailure(error.cause), {
            kind: "transport",
            code: "invalid_reply",
          });
          yield* Effect.promise(() => client.disconnect());
        }
      }
      assert.isTrue(
        f.calls.every((call) => ["connect", "workers.list", "subscribe"].includes(call)),
      );
    }).pipe(Effect.scoped),
  );

  it.effect(
    "keeps bounded pagination revisions and stale cursor errors on the passive client",
    () =>
      Effect.gen(function* () {
        const f = yield* workerWireFixture(owner);
        const first = {
          owner,
          revision: 7,
          cursor: "next-page",
          items: handoffWorkerRows.slice(0, 50),
        };
        f.policy.roster = first;
        assert.deepEqual(yield* Effect.promise(() => f.client.list()), first);
        const second = { ...first, cursor: null, items: handoffWorkerRows.slice(50) };
        f.policy.roster = second;
        assert.deepEqual(
          yield* Effect.promise(() => f.client.list(first.cursor, first.revision)),
          second,
        );
        assert.deepEqual(f.requests.at(-1)?.params, { after: "next-page", revision: 7 });
        f.policy.pageError = "stale_cursor";
        const error = yield* Effect.tryPromise(() =>
          f.client.list(first.cursor, first.revision),
        ).pipe(Effect.flip);
        assert.deepEqual(classifyPiWorkerFailure(error.cause), {
          kind: "coded",
          code: "stale_cursor",
        });
      }).pipe(Effect.scoped),
  );
});

describe("Pi worker failure provenance", () => {
  it.effect("rejects protocol 1 before connecting without downgrading the public client", () =>
    Effect.gen(function* () {
      const owner = { processId: "process", coordinatorSessionId: "session", epoch: "epoch" };
      const f = yield* workerWireFixture(owner);
      const descriptor: Record<string, unknown> = JSON.parse(
        NodeFS.readFileSync(f.descriptor, "utf8"),
      );
      NodeFS.writeFileSync(f.descriptor, JSON.stringify({ ...descriptor, version: 1 }));
      const error = yield* Effect.tryPromise(() => f.open()).pipe(Effect.flip);
      assert.instanceOf(error.cause, BridgeError);
      assert.equal(error.cause instanceof BridgeError && error.cause.code, "invalid_descriptor");
      assert.deepEqual(f.calls, ["connect"]);
      assert.deepEqual(yield* Effect.promise(() => f.client.list()), {
        owner,
        revision: 1,
        items: [],
        cursor: null,
      });
    }).pipe(Effect.scoped),
  );
  it("classifies the authoritative native vocabulary, not arbitrary code-shaped errors", () => {
    for (const code of Object.keys(errors)) {
      const request = classifyPiWorkerFailure(new BridgeError(code, "native"));
      assert.isTrue(request.kind === "coded" && request.code === code);
      const acquisition = classifyPiWorkerFailure(new WorkerSourceError({ code }), "acquisition");
      assert.isTrue(acquisition.kind === "coded" && acquisition.code === code);
      assert.deepEqual(classifyPiWorkerFailure(new WorkerSourceError({ code })), {
        kind: "unclassified",
      });
      assert.deepEqual(classifyPiWorkerFailure({ code }), { kind: "unclassified" });
      assert.deepEqual(
        classifyPiWorkerFailure({ _tag: "WorkerSourceError", code }, "acquisition"),
        { kind: "unclassified" },
      );
    }
    for (const code of ["invalid_reply", "disconnected"] as const)
      assert.deepEqual(classifyPiWorkerFailure(new BridgeError(code, "transport")), {
        kind: "transport",
        code,
      });
    for (const cause of [
      new BridgeError("new_native_code", "unknown"),
      new Error("ordinary"),
      new WorkerSourceError({ code: "source_failed" }),
    ])
      assert.deepEqual(classifyPiWorkerFailure(cause, "acquisition"), { kind: "unclassified" });
  });
  it.effect(
    "observes real passive-client local rejection before write, and framing transport failure",
    () =>
      Effect.gen(function* () {
        const owner = { processId: "process", coordinatorSessionId: "session", epoch: "epoch" };
        const f = yield* workerWireFixture(owner);
        const ref = {
          ...owner,
          kind: "read" as const,
          instanceId: "worker",
          source: "agent_spawn" as const,
        };
        for (const cursor of ["界".repeat(6000), ""]) {
          const error = yield* Effect.tryPromise(() => f.client.publicActivity(ref, cursor)).pipe(
            Effect.flip,
          );
          assert.deepEqual(classifyPiWorkerFailure(error.cause), {
            kind: "coded",
            code: "invalid_request",
          });
        }
        assert.deepEqual(f.calls, ["connect"]);
        assert.deepEqual(yield* Effect.promise(() => f.client.list()), {
          owner,
          revision: 1,
          items: [],
          cursor: null,
        });
        assert.equal(f.sockets.size, 1);
        f.policy.malformed = true;
        const error = yield* Effect.tryPromise(() => f.client.list()).pipe(Effect.flip);
        assert.deepEqual(classifyPiWorkerFailure(error.cause), {
          kind: "transport",
          code: "invalid_reply",
        });
        yield* Effect.promise(() => f.client.disconnect());
      }).pipe(Effect.scoped),
  );
});

const decodeCheckpoint = Schema.decodeUnknownSync(PiNativeWorkerCheckpoint);
const owner = { processId: "process", coordinatorSessionId: "session", epoch: "owner-epoch" };
const launch = Schema.decodeUnknownSync(PiNativeWorkerLaunch)({
  source: "agent_spawn",
  instanceId: "worker",
  providerInstanceId: "pi",
  providerSessionId: "provider-session",
  parentThreadId: "thread",
  parentProviderThreadId: "provider-thread",
  parentProviderTurnId: "provider-turn",
  runId: "run",
  runAttemptId: "attempt",
  toolNodeId: "tool-node",
  coordinatorSessionId: "session",
  toolCallId: "launch",
});
const ref = {
  ...owner,
  kind: "read" as const,
  instanceId: "worker",
  source: "agent_spawn" as const,
};
const binding = {
  generation: 0,
  historyId: "history",
  sessionId: "child-session",
  state: "active" as const,
  headEntryId: "entry",
  capture: "open" as const,
  attemptIds: ["child-attempt"],
  attempts: [],
  gaps: [],
  todoHead: { state: "ready" as const, sourceEntryId: "todo-entry" },
};
const bindingRef = {
  ...ref,
  native: { generation: 0, historyId: "history", sessionId: "child-session" },
};
const detail: PublicWorkerDetail = {
  format: "public-v1",
  ref,
  worker: {
    source: "agent_spawn",
    instanceId: "worker",
    roleId: "researcher",
    coordinatorSessionId: "session",
    toolCallId: "launch",
    acceptedAt: 10,
    linkage: "tool",
  },
  lifecycle: { execution: "running", result: "pending", capture: "open" },
  captureEpoch: "capture-epoch",
  bindings: [{ ...binding, ref: bindingRef }],
  workbrief: { text: "native brief", offset: 0, nextOffset: null, source: "invocation task" },
  nextBrief: null,
  selected: { model: "requested" },
  activated: null,
};
const record: PublicRecord = {
  id: "record",
  binding: 0,
  revision: 1,
  order: 1,
  nativeOrder: 2,
  occurredAt: 15,
  observedAt: 16,
  nativeTime: "native-time",
  nativeEntryId: "todo-entry",
  parentEntryId: "entry",
  toolCallId: "todo-call",
  kind: "tool",
  payload: {
    status: "completed",
    name: "todo",
    argsState: "not_captured",
    result: null,
    isError: false,
    todoSnapshot: {
      format: "rpiv-todo-v1",
      tasks: [{ id: 1, identityEntryId: "todo-entry", subject: "native task", status: "pending" }],
    },
  },
};
const page: PublicConversationPage = {
  format: "public-v1",
  state: "ready",
  ref: bindingRef,
  epoch: "capture-epoch",
  cut: 1,
  changes: [{ sequence: 1, op: "upsert", record }],
  next: "next",
  more: false,
  gaps: [],
  binding,
};
const fragments = (value: PublicRecord = record) => {
  const bytes = Buffer.from(JSON.stringify({ sequence: 1, op: "upsert", record: value }));
  const cut = Math.floor(bytes.length / 2);
  const start: PublicFragment = {
    kind: "fragment",
    id: value.id,
    binding: value.binding,
    revision: value.revision,
    sequence: 1,
    encoding: "base64-json",
    byteOffset: 0,
    totalBytes: bytes.length,
    data: bytes.subarray(0, cut).toString("base64"),
  };
  const end: PublicFragment = {
    ...start,
    byteOffset: cut,
    data: bytes.subarray(cut).toString("base64"),
  };
  return { start, end };
};

describe("native record assembly", () => {
  it("bounds direct records and rejects malformed complete payload scope", () => {
    const assembler = new NativeRecordAssembler();
    const direct = (value: unknown) => ({ sequence: 1, op: "upsert", record: value });
    assert.throws(
      () =>
        assembler.accept(
          direct({
            ...record,
            payload: {
              ...record.payload,
              name: "x".repeat(4 * 1024 * 1024),
            },
          }) as PublicConversationPage["changes"][number],
          page,
          null,
        ),
      "exceeds capture bound",
    );
    assert.throws(() =>
      assembler.accept(
        direct({
          ...record,
          kind: "message",
          payload: {
            role: "system",
            blocks: [],
          },
        }) as PublicConversationPage["changes"][number],
        page,
        null,
      ),
    );
    assert.throws(() =>
      assembler.accept(
        direct({ ...record, binding: -1 }) as PublicConversationPage["changes"][number],
        page,
        null,
      ),
    );
  });
  it("preserves complete typed TODO provenance and replays safely from fragment start", () => {
    const { start, end } = fragments();
    const assembler = new NativeRecordAssembler();
    assert.isUndefined(assembler.accept(start, page, "start-cursor"));
    const manifest = assembler.fragment;
    assert.deepEqual(manifest, {
      id: "record",
      binding: 0,
      revision: 1,
      epoch: "capture-epoch",
      sequence: 1,
      totalBytes: start.totalBytes,
      cursor: "start-cursor",
    });
    assert.notInclude(JSON.stringify(manifest), "native task");
    const replay = new NativeRecordAssembler();
    replay.accept(start, page, manifest!.cursor);
    const complete = replay.accept(end, page, "continuation");
    assert.equal(complete?.provenance.toolCallId, "todo-call");
    assert.equal(
      complete?.kind === "tool" && complete.payload.todoSnapshot?.tasks[0]?.subject,
      "native task",
    );
    assert.isUndefined(replay.fragment);
    assert.deepEqual(complete, assembler.accept(end, page, "continuation"));
  });
  it("rejects overlap, gaps, identity/revision/epoch/binding/sequence/size drift and interleaving", () => {
    const { start, end } = fragments();
    for (const changed of [
      { ...end, byteOffset: end.byteOffset - 1 },
      { ...end, byteOffset: end.byteOffset + 1 },
      { ...end, id: "other" },
      { ...end, binding: 2 },
      { ...end, revision: 2 },
      { ...end, sequence: 2 },
      { ...end, totalBytes: end.totalBytes + 1 },
      { ...end, data: "not base64" },
    ]) {
      const assembler = new NativeRecordAssembler();
      assembler.accept(start, page, null);
      assert.throws(() => assembler.accept(changed, page, "next"));
    }
    const assembler = new NativeRecordAssembler();
    assembler.accept(start, page, null);
    assert.throws(() => assembler.accept(end, { ...page, epoch: "new-epoch" }, "next"));
    assert.throws(() => assembler.accept(page.changes[0]!, page, "next"));
    assert.throws(() => new NativeRecordAssembler().accept(end, page, null));
    assert.throws(() =>
      new NativeRecordAssembler().accept({ ...start, totalBytes: 4 * 1024 * 1024 + 1 }, page, null),
    );
  });
  it("rejects invalid JSON and decoded record identity instead of persisting fragments", () => {
    const { start, end } = fragments({ ...record, id: "changed" });
    const assembler = new NativeRecordAssembler();
    assembler.accept({ ...start, id: "claimed" }, page, null);
    assert.throws(() => assembler.accept({ ...end, id: "claimed" }, page, "next"));
    const invalid = Buffer.from("{");
    assert.throws(() =>
      new NativeRecordAssembler().accept(
        { ...start, totalBytes: 1, data: invalid.toString("base64") },
        page,
        null,
      ),
    );
  });
});

describe("Pi native source ownership", () => {
  it.effect(
    "keeps a flag-free replacement unavailable across all inspection and retired-owner callbacks",
    () =>
      Effect.gen(function* () {
        const rejected = new PiWorkerBridge("linux");
        const directory = rejected.directory!;
        const replacement = new PiWorkerBridge("linux", true);
        try {
          assert.deepEqual(replacement.launchArgs, []);
          assert.isUndefined(replacement.directory);
          yield* replacement.bind("requested-winner");
          assert.deepEqual((yield* replacement.reconcile(launch)).gaps, ["unavailable"]);
          const error = yield* replacement.read(launch).pipe(Effect.flip);
          assert.equal(error.code, "bridge_launch_failed");
          // Late retirement of the first port has no authority over the winner.
          yield* rejected.invalidate;
          rejected.cleanupDirectory(false);
          yield* replacement.invalidate;
          yield* replacement.bind("late-callback");
          yield* replacement.close;
          replacement.cleanupDirectory(true);
          assert.isFalse(yield* replacement.hasPendingWork([launch]));
          assert.isTrue(NodeFS.existsSync(directory));
        } finally {
          rejected.cleanupDirectory(true);
        }
      }),
  );
  it.effect(
    "launches automatically without worker settings, retaining private directory ownership",
    () =>
      Effect.gen(function* () {
        const bridge = new PiWorkerBridge("linux");
        assert.isDefined(bridge.directory);
        assert.deepEqual(bridge.launchArgs, [`--pitako-agent-bridge=${bridge.directory}`]);
        yield* bridge.close;
        bridge.cleanupDirectory(true);
        assert.isFalse(NodeFS.existsSync(bridge.directory!));
      }),
  );
  it.effect(
    "creates an empty canonical private directory and does not bind an invented session ID",
    () =>
      Effect.gen(function* () {
        const bridge = new PiWorkerBridge("linux");
        const directory = bridge.directory!;
        try {
          assert.equal(NodeFS.realpathSync(directory), directory);
          assert.equal(NodeFS.statSync(directory).mode & 0o777, 0o700);
          assert.deepEqual(NodeFS.readdirSync(directory), []);
          assert.isAtMost(Buffer.byteLength(`${directory}/b-000000000000.sock`), 103);
          assert.deepEqual(bridge.launchArgs, [`--pitako-agent-bridge=${directory}`]);
          yield* bridge.bind(undefined);
          assert.deepEqual((yield* bridge.reconcile(launch)).gaps, ["unavailable"]);
          assert.isFalse(yield* bridge.hasPendingWork([launch]));
        } finally {
          bridge.cleanupDirectory(true);
        }
      }),
  );
  it.effect("represents unsupported transport without passing bridge flags", () =>
    Effect.gen(function* () {
      const bridge = new PiWorkerBridge("win32");
      assert.deepEqual(bridge.launchArgs, []);
      assert.deepEqual((yield* bridge.reconcile(launch)).gaps, ["unavailable"]);
    }),
  );
  it.effect("retains diagnostic directories on uncertain exit", () =>
    Effect.sync(() => {
      const bridge = new PiWorkerBridge("linux");
      const directory = bridge.directory!;
      bridge.cleanupDirectory(false);
      assert.isTrue(NodeFS.existsSync(directory));
      NodeFS.writeFileSync(`${directory}/diagnostic`, "uncertain native state");
      bridge.cleanupDirectory(true);
      assert.isTrue(NodeFS.existsSync(`${directory}/diagnostic`));
      NodeFS.unlinkSync(`${directory}/diagnostic`);
      bridge.cleanupDirectory(true);
      assert.isFalse(NodeFS.existsSync(directory));
    }),
  );
  it.effect(
    "reads public provenance without changed hints, assembles records across pages, and preserves selections",
    () =>
      Effect.gen(function* () {
        const bridge = new PiWorkerBridge("linux");
        const wire = yield* workerWireFixture(owner, bridge.directory);
        wire.policy.subscribeError = true;
        const { start, end } = fragments();
        wire.policy.reply = (request) => {
          if (request.method === "workers.detail") return detail;
          if (request.method === "workers.activity")
            return request.params.cursor === undefined
              ? { ...page, changes: [start], more: true, next: "fragment-next" }
              : { ...page, changes: [end] };
          throw new Error("Unexpected passive operation");
        };
        yield* Effect.addFinalizer(() => bridge.close);
        yield* bridge.bind("session");
        assert.deepEqual(bridge.owner, owner);
        const summary = yield* bridge.reconcile(launch);
        assert.equal(summary.brief, "native brief");
        const batch = yield* bridge.read(launch);
        assert.equal(batch.records.length, 1);
        assert.deepEqual(batch.bindings[0]?.todoHead, binding.todoHead);
        assert.equal(batch.checkpoint.cursor, "next");
        assert.isUndefined(batch.checkpoint.fragment);
        assert.deepEqual(batch.records[0]?.provenance.owner, owner);
        assert.deepEqual(wire.calls, [
          "connect",
          "connect",
          "subscribe",
          "workers.detail",
          "workers.detail",
          "workers.activity",
          "workers.activity",
        ]);
      }).pipe(Effect.scoped),
  );
  it.effect.each(["message", "tool"] as const)(
    "retains accepted %s omissions through empty reads and restored checkpoints, only for the same launch and owner",
    (kind) =>
      Effect.gen(function* () {
        const bridge = new PiWorkerBridge("linux");
        yield* Effect.addFinalizer(() => Effect.sync(() => bridge.cleanupDirectory(true)));
        const wire = yield* workerWireFixture(owner, bridge.directory);
        yield* Effect.addFinalizer(() => bridge.close);
        yield* bridge.bind("session");
        const omitted: PublicRecord =
          kind === "message"
            ? {
                ...record,
                toolCallId: null,
                kind: "message",
                payload: {
                  role: "assistant",
                  blocks: [
                    { type: "text", text: "Authentic text" },
                    { type: "omitted", reason: "unsupported_content" },
                  ],
                },
              }
            : {
                ...record,
                kind: "tool",
                payload: {
                  status: "completed",
                  name: "read",
                  argsState: "not_captured",
                  result: [{ type: "omitted", reason: "unsupported_content" }],
                  isError: false,
                },
              };
        wire.policy.reply = (request) =>
          request.method === "workers.detail"
            ? detail
            : {
                ...page,
                changes: [{ sequence: 1, op: "upsert", record: omitted }],
              };
        const accepted = yield* bridge.read(launch);
        assert.lengthOf(accepted.records, 1);
        assert.deepEqual(accepted.checkpoint.gaps, ["unsupported_content"]);
        assert.deepEqual(accepted.summary.gaps, ["unsupported_content"]);
        assert.equal(accepted.summary.execution, "running");
        assert.equal(accepted.summary.capture, "open");
        assert.equal(accepted.summary.availability, "ready");
        assert.isUndefined(accepted.checkpoint.bindingCursors[0]?.omission);

        wire.policy.reply = (request) =>
          request.method === "workers.detail" ? detail : { ...page, changes: [] };
        const empty = yield* bridge.read(launch, accepted.checkpoint);
        assert.deepEqual(empty.records, []);
        assert.deepEqual(empty.summary.gaps, ["unsupported_content"]);
        const restored = decodeCheckpoint(
          JSON.parse(
            JSON.stringify({
              ...empty.checkpoint,
              gaps: ["unsupported_content", "unavailable", "unsupported_content", "not_captured"],
            }),
          ),
        );
        const replacement = new PiWorkerBridge("linux");
        yield* Effect.addFinalizer(() => Effect.sync(() => replacement.cleanupDirectory(true)));
        const restoredWire = yield* workerWireFixture(owner, replacement.directory);
        yield* Effect.addFinalizer(() => replacement.close);
        yield* replacement.bind("session");
        restoredWire.policy.reply = wire.policy.reply;
        const resumed = yield* replacement.read(launch, restored);
        assert.deepEqual(resumed.checkpoint.gaps, ["unsupported_content"]);
        assert.deepEqual(resumed.summary.gaps, ["unsupported_content"]);
        assert.isUndefined(resumed.checkpoint.bindingCursors[0]?.omission);
        // The content finding outlives capture resync, but not another owner or launch.
        const resynced = yield* replacement.read(launch, {
          ...restored,
          captureEpoch: "old-capture",
        });
        assert.deepEqual(resynced.summary.gaps, ["unsupported_content", "resync_required"]);
        for (const foreign of [
          { ...restored, owner: { ...owner, epoch: "other-owner" } },
          { ...restored, owner: { ...owner, processId: "other-process" } },
          { ...restored, owner: { ...owner, coordinatorSessionId: "other-session" } },
          { ...restored, launch: { ...launch, instanceId: "another-worker" } },
        ]) {
          const isolated = yield* replacement.read(launch, foreign);
          assert.notInclude(isolated.checkpoint.gaps, "unsupported_content");
          assert.notInclude(isolated.summary.gaps, "unsupported_content");
        }
      }).pipe(Effect.scoped),
  );

  it.effect(
    "derives omissions only after fragment assembly and rejects invalid omitted records",
    () =>
      Effect.gen(function* () {
        const bridge = new PiWorkerBridge("linux");
        yield* Effect.addFinalizer(() => Effect.sync(() => bridge.cleanupDirectory(true)));
        const wire = yield* workerWireFixture(owner, bridge.directory);
        yield* Effect.addFinalizer(() => bridge.close);
        yield* bridge.bind("session");
        const omitted: PublicRecord = {
          ...record,
          toolCallId: null,
          kind: "message",
          payload: {
            role: "assistant",
            blocks: [{ type: "omitted", reason: "unsupported_content" }],
          },
        };
        const { start, end } = fragments(omitted);
        wire.policy.reply = (request) =>
          request.method === "workers.detail"
            ? detail
            : {
                ...page,
                changes: [start],
                more: false,
                next: null,
              };
        const partial = yield* bridge.read(launch);
        assert.deepEqual(partial.records, []);
        assert.isDefined(partial.checkpoint.fragment);
        assert.notInclude(partial.checkpoint.gaps, "unsupported_content");
        wire.policy.reply = (request) => {
          if (request.method === "workers.detail") return detail;
          if (request.method === "workers.activity")
            return request.params.cursor === undefined
              ? { ...page, changes: [start], more: true, next: "fragment-next" }
              : { ...page, changes: [end] };
          throw new Error("Unexpected passive operation");
        };
        const complete = yield* bridge.read(launch, partial.checkpoint);
        assert.lengthOf(complete.records, 1);
        assert.deepEqual(complete.summary.gaps, ["unsupported_content"]);
        assert.isUndefined(complete.checkpoint.fragment);
        assert.equal(complete.summary.capture, "open");
        // Valid omitted blocks do not rescue a malformed payload or wrong binding.
        const malformed = Buffer.from(
          JSON.stringify({
            sequence: 1,
            op: "upsert",
            record: {
              ...omitted,
              payload: {
                role: "system",
                blocks: [{ type: "omitted", reason: "unsupported_content" }],
              },
            },
          }),
        );
        const rejectedChanges: PublicConversationPage["changes"][] = [
          [{ ...start, totalBytes: malformed.length, data: malformed.toString("base64") }],
          [{ sequence: 1, op: "upsert", record: { ...omitted, binding: 5 } }],
        ];
        for (const changes of rejectedChanges) {
          wire.policy.reply = (request) =>
            request.method === "workers.detail" ? detail : { ...page, changes };
          const invalid = yield* bridge.read(launch);
          assert.deepEqual(invalid.records, []);
          assert.include(invalid.checkpoint.gaps, "invalid_record");
          assert.notInclude(invalid.checkpoint.gaps, "unsupported_content");
        }
      }).pipe(Effect.scoped),
  );

  it.effect(
    "bounds repeated resync responses without rejecting an authoritative epoch change",
    () =>
      Effect.gen(function* () {
        const bridge = new PiWorkerBridge("linux");
        const wire = yield* workerWireFixture(owner, bridge.directory);
        yield* Effect.addFinalizer(() => bridge.close);
        yield* bridge.bind("session");
        wire.policy.reply = (request) =>
          request.method === "workers.detail"
            ? { ...detail, captureEpoch: "replacement-epoch" }
            : { ...page, state: "resync_required", epoch: "replacement-epoch", changes: [] };
        const first = yield* bridge.read(launch);
        assert.isTrue(first.more);
        assert.notInclude(first.checkpoint.gaps, "invalid_record");
        const repeated = yield* bridge.read(launch, first.checkpoint);
        assert.isFalse(repeated.more);
        assert.equal(repeated.summary.capture, "incomplete");
        assert.include(repeated.summary.gaps, "resync_required");
      }).pipe(Effect.scoped),
  );
  it.effect(
    "retains historical binding metadata when resync no longer exposes that source binding",
    () =>
      Effect.gen(function* () {
        const bridge = new PiWorkerBridge("linux");
        const wire = yield* workerWireFixture(owner, bridge.directory);
        yield* Effect.addFinalizer(() => bridge.close);
        yield* bridge.bind("session");
        wire.policy.reply = (request) => (request.method === "workers.detail" ? detail : page);
        const first = yield* bridge.read(launch);
        const nextBinding = {
          ...binding,
          generation: 1,
          historyId: "new-history",
          sessionId: "new-session",
        };
        const nextRef = {
          ...ref,
          native: { generation: 1, historyId: "new-history", sessionId: "new-session" },
        };
        wire.policy.reply = (request) =>
          request.method === "workers.detail"
            ? { ...detail, bindings: [{ ...nextBinding, ref: nextRef }] }
            : {
                ...page,
                ref: nextRef,
                binding: nextBinding,
                changes: [{ sequence: 1, op: "upsert", record: { ...record, binding: 1 } }],
              };
        const resynced = yield* bridge.read(launch, first.checkpoint);
        assert.deepEqual(
          resynced.bindings.map((value) => [value.generation, value.state]),
          [
            [1, "active"],
            [0, "historical"],
          ],
        );
        assert.deepEqual(resynced.bindings[1]?.todoHead, binding.todoHead);
        assert.include(resynced.checkpoint.gaps, "pruned");
        assert.equal(resynced.records[0]?.provenance.binding, 1);
        assert.isFalse(resynced.more);
      }).pipe(Effect.scoped),
  );
  it.effect(
    "rejects mismatched/unlinked launch metadata while independent valid reads still progress",
    () =>
      Effect.gen(function* () {
        const bridge = new PiWorkerBridge("linux");
        const wire = yield* workerWireFixture(owner, bridge.directory);
        yield* Effect.addFinalizer(() => bridge.close);
        yield* bridge.bind("session");
        for (const worker of [
          { ...detail.worker, linkage: "unlinked", toolCallId: null },
          { ...detail.worker, toolCallId: "other" },
        ]) {
          wire.policy.reply = () => ({ ...detail, worker });
          assert.deepEqual((yield* bridge.reconcile(launch)).gaps, ["provenance_mismatch"]);
        }
        wire.policy.reply = (request) => (request.method === "workers.detail" ? detail : page);
        assert.deepEqual(
          (yield* bridge.reconcile({ ...launch, coordinatorSessionId: "other" })).gaps,
          ["provenance_mismatch"],
        );
        assert.equal((yield* bridge.read(launch)).records.length, 1);
        yield* bridge.invalidate;
        assert.equal(bridge.ownerGeneration, 1);
        assert.isUndefined(bridge.owner);
        assert.deepEqual((yield* bridge.reconcile(launch)).gaps, ["stale_owner"]);
      }).pipe(Effect.scoped),
  );
  it.effect(
    "returns a safe restart manifest and explicit gap for interrupted or oversized fragments",
    () =>
      Effect.gen(function* () {
        const bridge = new PiWorkerBridge("linux");
        const wire = yield* workerWireFixture(owner, bridge.directory);
        yield* Effect.addFinalizer(() => bridge.close);
        yield* bridge.bind("session");
        const { start, end } = fragments();
        wire.policy.reply = (request) => {
          if (request.method === "workers.detail") return detail;
          if (request.method === "workers.activity")
            return request.params.cursor === undefined
              ? { ...page, changes: [start], more: true, next: "fragment-next" }
              : { state: "unavailable", reason: "capture_failed" };
          throw new Error("Unexpected native operation");
        };
        const partial = yield* bridge.read(launch);
        assert.deepEqual(partial.records, []);
        assert.isDefined(partial.checkpoint.fragment);
        assert.equal(partial.summary.capture, "incomplete");
        assert.include(partial.summary.gaps, "unavailable");
        assert.isNull(partial.checkpoint.cursor);
        assert.include(partial.checkpoint.gaps, "unavailable");
        assert.equal(partial.checkpoint.capture, "incomplete");
        wire.policy.reply = (request) => {
          if (request.method === "workers.detail") return detail;
          if (request.method === "workers.activity")
            return request.params.cursor === undefined
              ? { ...page, changes: [start], more: true, next: "fragment-next" }
              : { ...page, changes: [end] };
          throw new Error("Unexpected native operation");
        };
        const replay = yield* bridge.read(launch, partial.checkpoint);
        assert.equal(replay.records.length, 1);
        assert.isUndefined(replay.checkpoint.fragment);
        wire.policy.reply = (request) =>
          request.method === "workers.detail"
            ? detail
            : { ...page, changes: [{ ...start, totalBytes: 4 * 1024 * 1024 + 1 }] };
        const oversized = yield* bridge.read(launch);
        assert.deepEqual(oversized.records, []);
        assert.include(oversized.checkpoint.gaps, "record_oversized");
        const reads = wire.calls.filter((method) => method === "workers.activity").length;
        const omitted = yield* bridge.read(launch, oversized.checkpoint);
        assert.equal(omitted.checkpoint.capture, "incomplete");
        assert.isFalse(omitted.more);
        assert.equal(wire.calls.filter((method) => method === "workers.activity").length, reads);
      }).pipe(Effect.scoped),
  );
  it.effect(
    "marks generation/reference drift incomplete without blocking an independent worker",
    () =>
      Effect.gen(function* () {
        const bridge = new PiWorkerBridge("linux");
        const wire = yield* workerWireFixture(owner, bridge.directory);
        yield* Effect.addFinalizer(() => bridge.close);
        yield* bridge.bind("session");
        wire.policy.reply = (request) =>
          request.method === "workers.detail"
            ? detail
            : { ...page, binding: { ...binding, historyId: "other-history" } };
        const drift = yield* bridge.read(launch);
        assert.deepEqual(drift.records, []);
        assert.include(drift.checkpoint.gaps, "invalid_record");
        assert.equal(drift.checkpoint.capture, "incomplete");
        const independent = new PiWorkerBridge("linux");
        const independentWire = yield* workerWireFixture(owner, independent.directory);
        yield* Effect.addFinalizer(() => independent.close);
        yield* independent.bind("session");
        independentWire.policy.reply = (request) =>
          request.method === "workers.detail" ? detail : page;
        assert.equal((yield* independent.read(launch)).records.length, 1);
        independentWire.policy.reply = () => ({
          ...detail,
          bindings: [
            { ...binding, ref: { ...bindingRef, native: { ...bindingRef.native, generation: 2 } } },
          ],
        });
        const rejected = yield* independent.read(launch).pipe(Effect.flip);
        assert.equal(rejected.code, "invalid_reply");
      }).pipe(Effect.scoped),
  );
  it.effect("derives pending work only from committed exact launches", () =>
    Effect.gen(function* () {
      const bridge = new PiWorkerBridge("linux");
      const wire = yield* workerWireFixture(owner, bridge.directory);
      yield* Effect.addFinalizer(() => bridge.close);
      yield* bridge.bind("session");
      wire.policy.reply = () => detail;
      assert.isFalse(yield* bridge.hasPendingWork([]));
      assert.isTrue(yield* bridge.hasPendingWork([launch]));
      wire.policy.reply = () => ({
        ...detail,
        worker: { ...detail.worker, toolCallId: "unlinked" },
      });
      assert.isFalse(yield* bridge.hasPendingWork([launch]));
      assert.notInclude(wire.calls, "list");
    }).pipe(Effect.scoped),
  );
  it.effect(
    "reconciles team assignment ID and role, and reads complete result pages without consuming them",
    () =>
      Effect.gen(function* () {
        const bridge = new PiWorkerBridge("linux");
        const wire = yield* workerWireFixture(owner, bridge.directory);
        yield* Effect.addFinalizer(() => bridge.close);
        yield* bridge.bind("session");
        const assignment = {
          ...launch,
          source: "team_assign" as const,
          assignmentId: "assignment",
          roleId: "researcher",
        };
        const teamRef = { ...ref, source: "team_assign" as const, assignmentId: "assignment" };
        const teamBindingRef = {
          ...bindingRef,
          source: "team_assign" as const,
          assignmentId: "assignment",
        };
        const teamDetail = {
          ...detail,
          ref: teamRef,
          worker: { ...detail.worker, source: "team_assign" as const, assignmentId: "assignment" },
          bindings: [{ ...binding, ref: teamBindingRef }],
          lifecycle: {
            execution: "completed",
            result: "available",
            capture: "closed",
            finalSequence: 1,
          },
        };
        wire.policy.reply = (request) => {
          if (request.method === "workers.detail") {
            assert.equal(request.params.id, assignment.instanceId);
            return teamDetail;
          }
          if (request.method === "workers.activity") return { ...page, ref: teamBindingRef };
          if (request.method === "workers.result")
            return {
              format: "public-v1",
              state: "ready",
              ref: teamRef,
              status: "completed",
              text:
                request.params.cursor === undefined
                  ? { text: "first😀", offset: 0, nextOffset: 7 }
                  : { text: "last", offset: 7, nextOffset: null },
              nextText:
                request.params.cursor === undefined
                  ? { ref: teamRef, section: "result_text", offset: 7 }
                  : null,
            };
          throw Error("Unexpected operation");
        };
        assert.equal((yield* bridge.reconcile(assignment)).availability, "ready");
        assert.deepEqual((yield* bridge.reconcile({ ...assignment, assignmentId: "other" })).gaps, [
          "provenance_mismatch",
        ]);
        assert.deepEqual((yield* bridge.reconcile({ ...assignment, roleId: "other" })).gaps, [
          "provenance_mismatch",
        ]);
        const batch = yield* bridge.read(assignment);
        assert.deepEqual(batch.result, {
          status: "completed",
          text: "first😀last",
          complete: true,
        });
        assert.deepEqual((yield* bridge.read(assignment, batch.checkpoint)).result, batch.result);
        assert.isTrue(
          wire.calls.every((method) =>
            [
              "connect",
              "subscribe",
              "workers.detail",
              "workers.activity",
              "workers.result",
            ].includes(method),
          ),
        );
      }).pipe(Effect.scoped),
  );
});

describe("owner-global background pending", () => {
  it.effect.each([13, 16, 17])(
    "negotiates canonical %s capabilities without probing old producers",
    (count) =>
      Effect.gen(function* () {
        const bridge = new PiWorkerBridge("linux");
        yield* Effect.addFinalizer(() => Effect.sync(() => bridge.cleanupDirectory(true)));
        const wire = yield* workerWireFixture(
          owner,
          bridge.directory,
          capabilities.slice(0, count),
        );
        yield* Effect.addFinalizer(() => bridge.close);
        yield* bridge.bind("session");
        for (const reply of [
          { state: "pending" },
          { state: "none" },
          ...["unsupported", "stale_owner", "source_unavailable"].map((reason) => ({
            state: "unavailable",
            reason,
          })),
        ]) {
          wire.policy.reply = () => reply;
          assert.deepEqual(
            yield* bridge.readBackgroundPending(),
            count === 17
              ? reply
              : {
                  state: "unavailable",
                  reason: "unsupported",
                },
          );
        }
        assert.equal(
          wire.calls.filter((method) => method === "background.pending").length,
          count === 17 ? 5 : 0,
        );
        assert.deepEqual(bridge.owner, owner);
        assert.isFalse(yield* bridge.hasPendingWork([]));
      }).pipe(Effect.scoped),
  );

  it.effect("does not mistake incompatible descriptors or acquisition failure for absence", () =>
    Effect.gen(function* () {
      for (const advertised of [
        capabilities.slice(0, 14),
        [...capabilities, "future"],
        capabilities.toReversed(),
      ]) {
        yield* Effect.gen(function* () {
          const bridge = new PiWorkerBridge("linux");
          yield* Effect.addFinalizer(() => Effect.sync(() => bridge.cleanupDirectory(true)));
          const wire = yield* workerWireFixture(owner, bridge.directory);
          yield* Effect.addFinalizer(() => bridge.close);
          const descriptor = JSON.parse(NodeFS.readFileSync(wire.descriptor, "utf8"));
          NodeFS.writeFileSync(
            wire.descriptor,
            JSON.stringify({ ...descriptor, capabilities: advertised }),
          );
          yield* bridge.bind("session");
          assert.deepEqual(yield* bridge.readBackgroundPending(), {
            state: "unavailable",
            reason: "source_unavailable",
          });
          assert.deepEqual(wire.calls, ["connect"]);
        }).pipe(Effect.scoped);
      }
      const absent = new PiWorkerBridge("win32");
      assert.deepEqual(yield* absent.readBackgroundPending(), {
        state: "unavailable",
        reason: "source_unavailable",
      });
    }).pipe(Effect.scoped),
  );

  it.effect("rejects invalid replies without claiming none", () =>
    Effect.gen(function* () {
      for (const reply of [
        { state: "unknown" },
        { state: "none", extra: true },
        { state: "unavailable", reason: "unknown" },
        null,
        false,
      ]) {
        yield* Effect.gen(function* () {
          const bridge = new PiWorkerBridge("linux");
          yield* Effect.addFinalizer(() => Effect.sync(() => bridge.cleanupDirectory(true)));
          const wire = yield* workerWireFixture(owner, bridge.directory);
          yield* Effect.addFinalizer(() => bridge.close);
          yield* bridge.bind("session");
          wire.policy.reply = () => reply;
          assert.deepEqual(yield* bridge.readBackgroundPending(), {
            state: "unavailable",
            reason: "source_unavailable",
          });
        }).pipe(Effect.scoped);
      }
    }).pipe(Effect.scoped),
  );

  it.effect(
    "preserves a healthy connection after operation rejection but not transport failure",
    () =>
      Effect.gen(function* () {
        const bridge = new PiWorkerBridge("linux");
        yield* Effect.addFinalizer(() => Effect.sync(() => bridge.cleanupDirectory(true)));
        const wire = yield* workerWireFixture(owner, bridge.directory);
        yield* Effect.addFinalizer(() => bridge.close);
        yield* bridge.bind("session");
        const generation = bridge.ownerGeneration;
        wire.policy.pendingError = "busy";
        assert.deepEqual(yield* bridge.readBackgroundPending(), {
          state: "unavailable",
          reason: "source_unavailable",
        });
        wire.policy.pendingError = undefined;
        wire.policy.reply = () => ({ state: "pending" });
        assert.deepEqual(yield* bridge.readBackgroundPending(), { state: "pending" });
        assert.equal(bridge.ownerGeneration, generation);
        assert.equal(wire.calls.filter((method) => method === "connect").length, 2);
        wire.policy.malformed = true;
        assert.deepEqual(yield* bridge.readBackgroundPending(), {
          state: "unavailable",
          reason: "source_unavailable",
        });
        wire.policy.malformed = false;
        assert.deepEqual(yield* bridge.readBackgroundPending(), {
          state: "unavailable",
          reason: "source_unavailable",
        });
      }).pipe(Effect.scoped),
  );

  it.effect("fences a held response during retirement and permits an independent replacement", () =>
    Effect.gen(function* () {
      const bridge = new PiWorkerBridge("linux");
      yield* Effect.addFinalizer(() => Effect.sync(() => bridge.cleanupDirectory(true)));
      const wire = yield* workerWireFixture(owner, bridge.directory);
      yield* Effect.addFinalizer(() => bridge.close);
      yield* bridge.bind("session");
      const entered = Promise.withResolvers<void>();
      const reply = Promise.withResolvers<unknown>();
      wire.policy.reply = () => {
        entered.resolve();
        return reply.promise;
      };
      const reading = yield* bridge.readBackgroundPending().pipe(Effect.forkChild);
      yield* Effect.promise(() => entered.promise);
      const generation = bridge.ownerGeneration;
      yield* bridge.invalidate;
      reply.resolve({ state: "none" });
      assert.deepEqual(yield* Fiber.join(reading), { state: "unavailable", reason: "stale_owner" });
      assert.equal(bridge.ownerGeneration, generation + 1);
      assert.deepEqual(yield* bridge.readBackgroundPending(), {
        state: "unavailable",
        reason: "stale_owner",
      });
      wire.policy.reply = () => ({ state: "pending" });
      yield* bridge.bind("session");
      assert.deepEqual(yield* bridge.readBackgroundPending(), { state: "pending" });
      assert.deepEqual(bridge.owner, owner);
      yield* bridge.close;
      assert.deepEqual(yield* bridge.readBackgroundPending(), {
        state: "unavailable",
        reason: "stale_owner",
      });
    }).pipe(Effect.scoped),
  );

  it.effect("keeps Effect interruption as cancellation, joining tracked work on close", () =>
    Effect.gen(function* () {
      const bridge = new PiWorkerBridge("linux");
      yield* Effect.addFinalizer(() => Effect.sync(() => bridge.cleanupDirectory(true)));
      const wire = yield* workerWireFixture(owner, bridge.directory);
      yield* Effect.addFinalizer(() => bridge.close);
      yield* bridge.bind("session");
      const entered = Promise.withResolvers<void>();
      const reply = Promise.withResolvers<unknown>();
      wire.policy.reply = () => {
        entered.resolve();
        return reply.promise;
      };
      const reading = yield* bridge.readBackgroundPending().pipe(Effect.exit, Effect.forkChild);
      yield* Effect.promise(() => entered.promise);
      yield* Fiber.interrupt(reading);
      const exit = yield* Fiber.await(reading);
      assert.isTrue(Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause));
      reply.resolve({ state: "none" });
      yield* bridge.close;
      assert.isUndefined(bridge.owner);
    }).pipe(Effect.scoped),
  );
});
