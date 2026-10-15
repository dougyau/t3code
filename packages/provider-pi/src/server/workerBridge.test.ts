// @effect-diagnostics nodeBuiltinImport:off -- Private native directory ownership is the boundary under test.
import * as NodeFS from "node:fs";
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { PiWorkerBridge, WorkerSourceError, NativeRecordAssembler, classifyPiWorkerFailure } from "./workerBridge.ts";
import { BridgeError } from "./pitako/bridge/client.js";
import { errors, type PublicWorkerDetail, type PublicRecord, type PublicConversationPage, type PublicFragment } from "./pitako/bridge/protocol.js";
import { workerWireFixture } from "./workerBridge.wire.fixture.ts";
import { PiNativeWorkerLaunch } from "@t3tools/contracts";

describe("Pi worker failure provenance", () => {
  it.effect("rejects protocol 1 before connecting without downgrading the public client", () =>
    Effect.gen(function* () {
      const owner = { processId: "process", coordinatorSessionId: "session", epoch: "epoch" };
      const f = yield* workerWireFixture(owner);
      const descriptor: Record<string, unknown> = JSON.parse(NodeFS.readFileSync(f.descriptor, "utf8"));
      NodeFS.writeFileSync(f.descriptor, JSON.stringify({ ...descriptor, version: 1 }));
      const error = yield* Effect.tryPromise(() => f.open()).pipe(Effect.flip);
      assert.instanceOf(error.cause, BridgeError);
      assert.equal(error.cause instanceof BridgeError && error.cause.code, "invalid_descriptor");
      assert.deepEqual(f.calls, ["connect"]);
      assert.deepEqual(yield* Effect.promise(() => f.client.list()), { owner, revision: 1, items: [], cursor: null });
    }).pipe(Effect.scoped));
  it("classifies the authoritative native vocabulary, not arbitrary code-shaped errors", () => {
    for (const code of Object.keys(errors)) {
      const request = classifyPiWorkerFailure(new BridgeError(code, "native"));
      assert.isTrue(request.kind === "coded" && request.code === code);
      const acquisition = classifyPiWorkerFailure(new WorkerSourceError({ code }), "acquisition");
      assert.isTrue(acquisition.kind === "coded" && acquisition.code === code);
      assert.deepEqual(classifyPiWorkerFailure(new WorkerSourceError({ code })), { kind: "unclassified" });
      assert.deepEqual(classifyPiWorkerFailure({ code }), { kind: "unclassified" });
      assert.deepEqual(classifyPiWorkerFailure({ _tag: "WorkerSourceError", code }, "acquisition"), { kind: "unclassified" });
    }
    for (const code of ["invalid_reply", "disconnected"] as const)
      assert.deepEqual(classifyPiWorkerFailure(new BridgeError(code, "transport")), { kind: "transport", code });
    for (const cause of [new BridgeError("new_native_code", "unknown"), new Error("ordinary"),
      new WorkerSourceError({ code: "source_failed" })])
      assert.deepEqual(classifyPiWorkerFailure(cause, "acquisition"), { kind: "unclassified" });
  });
  it.effect("observes real passive-client local rejection before write, and framing transport failure", () =>
    Effect.gen(function* () {
      const owner = { processId: "process", coordinatorSessionId: "session", epoch: "epoch" };
      const f = yield* workerWireFixture(owner);
      const ref = { ...owner, kind: "read" as const, instanceId: "worker", source: "agent_spawn" as const };
      for (const cursor of ["界".repeat(6000), ""]) {
        const error = yield* Effect.tryPromise(() => f.client.publicActivity(ref, cursor)).pipe(Effect.flip);
        assert.deepEqual(classifyPiWorkerFailure(error.cause), { kind: "coded", code: "invalid_request" });
      }
      assert.deepEqual(f.calls, ["connect"]);
      assert.deepEqual(yield* Effect.promise(() => f.client.list()), { owner, revision: 1, items: [], cursor: null });
      assert.equal(f.sockets.size, 1);
      f.policy.malformed = true;
      const error = yield* Effect.tryPromise(() => f.client.list()).pipe(Effect.flip);
      assert.deepEqual(classifyPiWorkerFailure(error.cause), { kind: "transport", code: "invalid_reply" });
      yield* Effect.promise(() => f.client.disconnect());
    }).pipe(Effect.scoped));
});

const owner = { processId: "process", coordinatorSessionId: "session", epoch: "owner-epoch" };
const launch = Schema.decodeUnknownSync(PiNativeWorkerLaunch)({
  source: "agent_spawn", instanceId: "worker",
  providerInstanceId: "pi", providerSessionId: "provider-session",
  parentThreadId: "thread", parentProviderThreadId: "provider-thread", parentProviderTurnId: "provider-turn",
  runId: "run", runAttemptId: "attempt", toolNodeId: "tool-node", coordinatorSessionId: "session", toolCallId: "launch",
});
const ref = { ...owner, kind: "read" as const, instanceId: "worker", source: "agent_spawn" as const };
const binding = {
  generation: 0, historyId: "history", sessionId: "child-session", state: "active" as const,
  headEntryId: "entry", capture: "open" as const, attemptIds: ["child-attempt"], attempts: [], gaps: [],
  todoHead: { state: "ready" as const, sourceEntryId: "todo-entry" },
};
const bindingRef = { ...ref, native: { generation: 0, historyId: "history", sessionId: "child-session" } };
const detail: PublicWorkerDetail = {
  format: "public-v1", ref,
  worker: { source: "agent_spawn", instanceId: "worker", roleId: "researcher", coordinatorSessionId: "session",
    toolCallId: "launch", acceptedAt: 10, linkage: "tool" },
  lifecycle: { execution: "running", result: "pending", capture: "open" },
  captureEpoch: "capture-epoch", bindings: [{ ...binding, ref: bindingRef }],
  workbrief: { text: "native brief", offset: 0, nextOffset: null, source: "invocation task" },
  nextBrief: null, selected: { model: "requested" }, activated: null,
};
const record: PublicRecord = {
  id: "record", binding: 0, revision: 1, order: 1, nativeOrder: 2, occurredAt: 15, observedAt: 16,
  nativeTime: "native-time", nativeEntryId: "todo-entry", parentEntryId: "entry", toolCallId: "todo-call",
  kind: "tool", payload: { status: "completed", name: "todo", argsState: "not_captured", result: null,
    isError: false, todoSnapshot: { format: "rpiv-todo-v1", tasks: [
      { id: 1, identityEntryId: "todo-entry", subject: "native task", status: "pending" },
    ] } },
};
const page: PublicConversationPage = {
  format: "public-v1", state: "ready", ref: bindingRef, epoch: "capture-epoch", cut: 1,
  changes: [{ sequence: 1, op: "upsert", record }], next: "next", more: false, gaps: [], binding,
};
const fragments = (value = record) => {
  const bytes = Buffer.from(JSON.stringify({ sequence: 1, op: "upsert", record: value }));
  const cut = Math.floor(bytes.length / 2);
  const start: PublicFragment = { kind: "fragment", id: value.id, binding: value.binding, revision: value.revision,
    sequence: 1, encoding: "base64-json", byteOffset: 0, totalBytes: bytes.length, data: bytes.subarray(0, cut).toString("base64") };
  const end: PublicFragment = { ...start, byteOffset: cut, data: bytes.subarray(cut).toString("base64") };
  return { start, end };
};

describe("native record assembly", () => {
  it("bounds direct records and rejects malformed complete payload scope", () => {
    const assembler = new NativeRecordAssembler();
    const direct = (value: unknown) => ({ sequence: 1, op: "upsert", record: value });
    assert.throws(() => assembler.accept(direct({ ...record, payload: {
      ...record.payload, name: "x".repeat(4 * 1024 * 1024),
    } }) as PublicConversationPage["changes"][number], page, null), "exceeds capture bound");
    assert.throws(() => assembler.accept(direct({ ...record, kind: "message", payload: {
      role: "system", blocks: [],
    } }) as PublicConversationPage["changes"][number], page, null));
    assert.throws(() => assembler.accept(direct({ ...record, binding: -1 }) as PublicConversationPage["changes"][number], page, null));
  });
  it("preserves complete typed TODO provenance and replays safely from fragment start", () => {
    const { start, end } = fragments();
    const assembler = new NativeRecordAssembler();
    assert.isUndefined(assembler.accept(start, page, "start-cursor"));
    const manifest = assembler.fragment;
    assert.deepEqual(manifest, { id: "record", binding: 0, revision: 1, epoch: "capture-epoch", sequence: 1,
      totalBytes: start.totalBytes, cursor: "start-cursor" });
    assert.notInclude(JSON.stringify(manifest), "native task");
    const replay = new NativeRecordAssembler();
    replay.accept(start, page, manifest!.cursor);
    const complete = replay.accept(end, page, "continuation");
    assert.equal(complete?.provenance.toolCallId, "todo-call");
    assert.equal(complete?.kind === "tool" && complete.payload.todoSnapshot?.tasks[0]?.subject, "native task");
    assert.isUndefined(replay.fragment);
    assert.deepEqual(complete, assembler.accept(end, page, "continuation"));
  });
  it("rejects overlap, gaps, identity/revision/epoch/binding/sequence/size drift and interleaving", () => {
    const { start, end } = fragments();
    for (const changed of [
      { ...end, byteOffset: end.byteOffset - 1 }, { ...end, byteOffset: end.byteOffset + 1 },
      { ...end, id: "other" }, { ...end, binding: 2 }, { ...end, revision: 2 },
      { ...end, sequence: 2 }, { ...end, totalBytes: end.totalBytes + 1 }, { ...end, data: "not base64" },
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
    assert.throws(() => new NativeRecordAssembler().accept({ ...start, totalBytes: 4 * 1024 * 1024 + 1 }, page, null));
  });
  it("rejects invalid JSON and decoded record identity instead of persisting fragments", () => {
    const { start, end } = fragments({ ...record, id: "changed" });
    const assembler = new NativeRecordAssembler();
    assembler.accept({ ...start, id: "claimed" }, page, null);
    assert.throws(() => assembler.accept({ ...end, id: "claimed" }, page, "next"));
    const invalid = Buffer.from("{");
    assert.throws(() => new NativeRecordAssembler().accept({ ...start, totalBytes: 1,
      data: invalid.toString("base64") }, page, null));
  });
});

describe("Pi native source ownership", () => {
  it.effect("keeps a flag-free replacement unavailable across all inspection and retired-owner callbacks", () =>
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
  it.effect("launches automatically without worker settings, retaining private directory ownership", () =>
    Effect.gen(function* () {
      const bridge = new PiWorkerBridge("linux");
      assert.isDefined(bridge.directory);
      assert.deepEqual(bridge.launchArgs, [`--pitako-agent-bridge=${bridge.directory}`]);
      yield* bridge.close;
      bridge.cleanupDirectory(true);
      assert.isFalse(NodeFS.existsSync(bridge.directory!));
    }),
  );
  it.effect("creates an empty canonical private directory and does not bind an invented session ID", () =>
    Effect.gen(function* () {
        const bridge = new PiWorkerBridge("linux");
        const directory = bridge.directory!;
        try {
          assert.equal(NodeFS.realpathSync(directory), directory);
          assert.equal(NodeFS.statSync(directory).mode & 0o777, 0o700);
          assert.deepEqual(NodeFS.readdirSync(directory), []);
          assert.isAtMost(Buffer.byteLength(`${directory}/b-000000000000.sock`), 103);
          assert.deepEqual(bridge.launchArgs, [
            `--pitako-agent-bridge=${directory}`,
          ]);
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
  it.effect("reads public provenance without changed hints, assembles records across pages, and preserves selections", () =>
    Effect.gen(function* () {
      const bridge = new PiWorkerBridge("linux");
      const wire = yield* workerWireFixture(owner, bridge.directory);
      wire.policy.subscribeError = true;
      const { start, end } = fragments();
      wire.policy.reply = (request) => {
        if (request.method === "workers.detail") return detail;
        if (request.method === "workers.activity") return request.params.cursor === undefined
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
      assert.deepEqual(wire.calls, ["connect", "connect", "subscribe", "workers.detail",
        "workers.detail", "workers.activity", "workers.activity"]);
    }).pipe(Effect.scoped));
  it.effect("bounds repeated resync responses without rejecting an authoritative epoch change", () =>
    Effect.gen(function* () {
      const bridge = new PiWorkerBridge("linux");
      const wire = yield* workerWireFixture(owner, bridge.directory);
      yield* Effect.addFinalizer(() => bridge.close);
      yield* bridge.bind("session");
      wire.policy.reply = (request) => request.method === "workers.detail"
        ? { ...detail, captureEpoch: "replacement-epoch" }
        : { ...page, state: "resync_required", epoch: "replacement-epoch", changes: [] };
      const first = yield* bridge.read(launch);
      assert.isTrue(first.more);
      assert.notInclude(first.checkpoint.gaps, "invalid_record");
      const repeated = yield* bridge.read(launch, first.checkpoint);
      assert.isFalse(repeated.more);
      assert.equal(repeated.summary.capture, "incomplete");
      assert.include(repeated.summary.gaps, "resync_required");
    }).pipe(Effect.scoped));
  it.effect("retains historical binding metadata when resync no longer exposes that source binding", () =>
    Effect.gen(function* () {
      const bridge = new PiWorkerBridge("linux");
      const wire = yield* workerWireFixture(owner, bridge.directory);
      yield* Effect.addFinalizer(() => bridge.close);
      yield* bridge.bind("session");
      wire.policy.reply = (request) => request.method === "workers.detail" ? detail : page;
      const first = yield* bridge.read(launch);
      const nextBinding = { ...binding, generation: 1, historyId: "new-history", sessionId: "new-session" };
      const nextRef = { ...ref, native: { generation: 1, historyId: "new-history", sessionId: "new-session" } };
      wire.policy.reply = (request) => request.method === "workers.detail"
        ? { ...detail, bindings: [{ ...nextBinding, ref: nextRef }] }
        : { ...page, ref: nextRef, binding: nextBinding,
            changes: [{ sequence: 1, op: "upsert", record: { ...record, binding: 1 } }] };
      const resynced = yield* bridge.read(launch, first.checkpoint);
      assert.deepEqual(resynced.bindings.map((value) => [value.generation, value.state]),
        [[1, "active"], [0, "historical"]]);
      assert.deepEqual(resynced.bindings[1]?.todoHead, binding.todoHead);
      assert.include(resynced.checkpoint.gaps, "pruned");
      assert.equal(resynced.records[0]?.provenance.binding, 1);
      assert.isFalse(resynced.more);
    }).pipe(Effect.scoped));
  it.effect("rejects mismatched/unlinked launch metadata while independent valid reads still progress", () =>
    Effect.gen(function* () {
      const bridge = new PiWorkerBridge("linux");
      const wire = yield* workerWireFixture(owner, bridge.directory);
      yield* Effect.addFinalizer(() => bridge.close);
      yield* bridge.bind("session");
      for (const worker of [
        { ...detail.worker, linkage: "unlinked", toolCallId: null }, { ...detail.worker, toolCallId: "other" },
      ]) {
        wire.policy.reply = () => ({ ...detail, worker });
        assert.deepEqual((yield* bridge.reconcile(launch)).gaps, ["provenance_mismatch"]);
      }
      wire.policy.reply = (request) => request.method === "workers.detail" ? detail : page;
      assert.deepEqual((yield* bridge.reconcile({ ...launch, coordinatorSessionId: "other" })).gaps, ["provenance_mismatch"]);
      assert.equal((yield* bridge.read(launch)).records.length, 1);
      yield* bridge.invalidate;
      assert.equal(bridge.ownerGeneration, 1);
      assert.isUndefined(bridge.owner);
      assert.deepEqual((yield* bridge.reconcile(launch)).gaps, ["stale_owner"]);
    }).pipe(Effect.scoped));
  it.effect("returns a safe restart manifest and explicit gap for interrupted or oversized fragments", () =>
    Effect.gen(function* () {
      const bridge = new PiWorkerBridge("linux");
      const wire = yield* workerWireFixture(owner, bridge.directory);
      yield* Effect.addFinalizer(() => bridge.close);
      yield* bridge.bind("session");
      const { start, end } = fragments();
      wire.policy.reply = (request) => {
        if (request.method === "workers.detail") return detail;
        if (request.method === "workers.activity") return request.params.cursor === undefined
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
        if (request.method === "workers.activity") return request.params.cursor === undefined
          ? { ...page, changes: [start], more: true, next: "fragment-next" } : { ...page, changes: [end] };
        throw new Error("Unexpected native operation");
      };
      const replay = yield* bridge.read(launch, partial.checkpoint);
      assert.equal(replay.records.length, 1);
      assert.isUndefined(replay.checkpoint.fragment);
      wire.policy.reply = (request) => request.method === "workers.detail" ? detail :
        { ...page, changes: [{ ...start, totalBytes: 4 * 1024 * 1024 + 1 }] };
      const oversized = yield* bridge.read(launch);
      assert.deepEqual(oversized.records, []);
      assert.include(oversized.checkpoint.gaps, "record_oversized");
      const reads = wire.calls.filter((method) => method === "workers.activity").length;
      const omitted = yield* bridge.read(launch, oversized.checkpoint);
      assert.equal(omitted.checkpoint.capture, "incomplete");
      assert.isFalse(omitted.more);
      assert.equal(wire.calls.filter((method) => method === "workers.activity").length, reads);
    }).pipe(Effect.scoped));
  it.effect("marks generation/reference drift incomplete without blocking an independent worker", () =>
    Effect.gen(function* () {
      const bridge = new PiWorkerBridge("linux");
      const wire = yield* workerWireFixture(owner, bridge.directory);
      yield* Effect.addFinalizer(() => bridge.close);
      yield* bridge.bind("session");
      wire.policy.reply = (request) => request.method === "workers.detail" ? detail :
        { ...page, binding: { ...binding, historyId: "other-history" } };
      const drift = yield* bridge.read(launch);
      assert.deepEqual(drift.records, []);
      assert.include(drift.checkpoint.gaps, "invalid_record");
      assert.equal(drift.checkpoint.capture, "incomplete");
      const independent = new PiWorkerBridge("linux");
      const independentWire = yield* workerWireFixture(owner, independent.directory);
      yield* Effect.addFinalizer(() => independent.close);
      yield* independent.bind("session");
      independentWire.policy.reply = (request) => request.method === "workers.detail" ? detail : page;
      assert.equal((yield* independent.read(launch)).records.length, 1);
      independentWire.policy.reply = () => ({
        ...detail, bindings: [{ ...binding, ref: { ...bindingRef, native: { ...bindingRef.native, generation: 2 } } }],
      });
      const rejected = yield* independent.read(launch).pipe(Effect.flip);
      assert.equal(rejected.code, "invalid_reply");
    }).pipe(Effect.scoped));
  it.effect("derives pending work only from committed exact launches", () =>
    Effect.gen(function* () {
      const bridge = new PiWorkerBridge("linux");
      const wire = yield* workerWireFixture(owner, bridge.directory);
      yield* Effect.addFinalizer(() => bridge.close);
      yield* bridge.bind("session");
      wire.policy.reply = () => detail;
      assert.isFalse(yield* bridge.hasPendingWork([]));
      assert.isTrue(yield* bridge.hasPendingWork([launch]));
      wire.policy.reply = () => ({ ...detail, worker: { ...detail.worker, toolCallId: "unlinked" } });
      assert.isFalse(yield* bridge.hasPendingWork([launch]));
      assert.notInclude(wire.calls, "list");
    }).pipe(Effect.scoped));
  it.effect("reconciles team assignment ID and role, and reads complete result pages without consuming them", () =>
    Effect.gen(function* () {
      const bridge = new PiWorkerBridge("linux");
      const wire = yield* workerWireFixture(owner, bridge.directory);
      yield* Effect.addFinalizer(() => bridge.close);
      yield* bridge.bind("session");
      const assignment = { ...launch, source: "team_assign" as const, assignmentId: "assignment", roleId: "researcher" };
      const teamRef = { ...ref, source: "team_assign" as const, assignmentId: "assignment" };
      const teamBindingRef = { ...bindingRef, source: "team_assign" as const, assignmentId: "assignment" };
      const teamDetail = { ...detail, ref: teamRef,
        worker: { ...detail.worker, source: "team_assign" as const, assignmentId: "assignment" },
        bindings: [{ ...binding, ref: teamBindingRef }],
        lifecycle: { execution: "completed", result: "available", capture: "closed", finalSequence: 1 } };
      wire.policy.reply = (request) => {
        if (request.method === "workers.detail") {
          assert.equal(request.params.id, assignment.instanceId);
          return teamDetail;
        }
        if (request.method === "workers.activity") return { ...page, ref: teamBindingRef };
        if (request.method === "workers.result") return {
          format: "public-v1", state: "ready", ref: teamRef, status: "completed",
          text: request.params.cursor === undefined
            ? { text: "first😀", offset: 0, nextOffset: 7 }
            : { text: "last", offset: 7, nextOffset: null },
          nextText: request.params.cursor === undefined ? { ref: teamRef, section: "result_text", offset: 7 } : null,
        };
        throw Error("Unexpected operation");
      };
      assert.equal((yield* bridge.reconcile(assignment)).availability, "ready");
      assert.deepEqual((yield* bridge.reconcile({ ...assignment, assignmentId: "other" })).gaps, ["provenance_mismatch"]);
      assert.deepEqual((yield* bridge.reconcile({ ...assignment, roleId: "other" })).gaps, ["provenance_mismatch"]);
      const batch = yield* bridge.read(assignment);
      assert.deepEqual(batch.result, { status: "completed", text: "first😀last", complete: true });
      assert.deepEqual((yield* bridge.read(assignment, batch.checkpoint)).result, batch.result);
      assert.isTrue(wire.calls.every((method) => ["connect", "subscribe", "workers.detail", "workers.activity", "workers.result"].includes(method)));
    }).pipe(Effect.scoped));
});
