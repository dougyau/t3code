import { assert, it } from "@effect/vitest";
import {
  CheckpointId,
  CheckpointRef,
  CheckpointScopeId,
  CommandId,
  MessageId,
  type ModelSelection,
  NodeId,
  type OrchestrationV2AppThread,
  type OrchestrationV2CheckpointScope,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2PlanArtifact,
  type OrchestrationV2RuntimeRequest,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2Run,
  type OrchestrationV2TurnItem,
  type OrchestrationV2ProjectedTurnItem,
  type OrchestrationV2ThreadProjection,
  type PiNativeWorkerLaunch,
  type PiNativeWorkerCheckpoint,
  type PiNativeWorkerSummary,
  type PiNativeWorkerRecord,
  ProviderDriverKind,
  ProviderInstanceId,
  PlanId,
  type ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  RuntimeRequestId,
  TurnItemId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { vi } from "vite-plus/test";

import { toolOutputImages } from "@t3tools/shared/toolOutput";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as EventSink from "./EventSink.ts";
import * as EventStore from "./EventStore.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderEventIngestor from "./ProviderEventIngestor.ts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ThreadCommandExecutor from "./ThreadCommandExecutor.ts";
import * as RunExecutionService from "./RunExecutionService.ts";
import * as CheckpointService from "./CheckpointService.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as McpAppModelContext from "../mcpApps/McpAppModelContext.ts";
import * as RunFinalizationService from "./RunFinalizationService.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import { makeProviderFailure } from "@t3tools/provider-core/server/failure";
import {
  makeProviderEventRoutingState,
  type ProviderEventRouteIdentity,
  routeProviderEvent,
  selectInheritedBackgroundTurnItems,
} from "./RunExecutionService.ts";

const layerTestDatabase = SqlitePersistence.layerMemory;
const layerTestStores = Layer.merge(EventStore.layer, ProjectionStore.layer).pipe(
  Layer.provide(layerTestDatabase),
);

const layerTestEventSink = EventSink.layer.pipe(
  Layer.provide(Layer.mergeAll(layerTestStores, layerTestDatabase)),
);

const layerTest = Layer.mergeAll(
  layerTestDatabase,
  layerTestStores,
  layerTestEventSink,
  IdAllocator.layer,
  ThreadCommandExecutor.layer,
  ProviderEventIngestor.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        layerTestStores,
        layerTestEventSink,
        IdAllocator.layer,
        ThreadCommandExecutor.layer,
      ),
    ),
  ),
);
const modelSelection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "gpt-5.4",
} satisfies ModelSelection;
const CODEX_DRIVER = ProviderDriverKind.make("codex");

function threadCreatedEvent(
  now: DateTime.Utc,
): Effect.Effect<
  OrchestrationV2DomainEvent,
  IdAllocator.IdAllocatorV2Error,
  IdAllocator.IdAllocatorV2
> {
  return Effect.gen(function* () {
    const idAllocator = yield* IdAllocator.IdAllocatorV2;
    const projectId = yield* idAllocator.allocate.project({
      fixtureName: "provider-event-ingestor",
    });
    const threadId = yield* idAllocator.allocate.thread({
      fixtureName: "provider-event-ingestor",
      projectId,
    });
    const providerThreadId = idAllocator.derive.providerThread({
      driver: CODEX_DRIVER,
      nativeThreadId: "native-thread",
    });
    const thread: OrchestrationV2AppThread = {
      createdBy: "user",
      creationSource: "web",
      id: threadId,
      projectId,
      title: "Provider event ingestor",
      providerInstanceId: modelSelection.instanceId,
      modelSelection: modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      branchPullRequest: null,
      activeOrderKey: null,
      activeProviderThreadId: providerThreadId,
      lineage: {
        parentThreadId: null,
        relationshipToParent: null,
        rootThreadId: threadId,
      },
      forkedFrom: null,
      createdAt: now,
      updatedAt: now,
      archivedAt: null,
      settledOverride: null,
      settledAt: null,
      lastVisitedAt: null,
      deletedAt: null,
    };

    return {
      id: yield* idAllocator.allocate.event({ threadId }),
      type: "thread.created",
      threadId,
      occurredAt: now,
      payload: thread,
    };
  });
}

const layer = it.layer(layerTest);

const seedNativeWorker = Effect.fnUntraced(function* (admit = true) {
  const driver = ProviderDriverKind.make("pi");
  const selection: ModelSelection = { instanceId: ProviderInstanceId.make("pi"), model: "parent-model" };
  const now = yield* DateTime.now;
  const sink = yield* EventSink.EventSinkV2;
  const ingestor = yield* ProviderEventIngestor.ProviderEventIngestorV2;
  const ids = yield* IdAllocator.IdAllocatorV2;
  const parentSeed = yield* threadCreatedEvent(now);
  if (parentSeed.type !== "thread.created") return yield* Effect.die(new Error("Expected parent thread."));
  const parent = { ...parentSeed, payload: {
    ...parentSeed.payload,
    providerInstanceId: selection.instanceId, modelSelection: selection,
  } };
  const providerSessionId = yield* ids.allocate.providerSession({
    providerInstanceId: selection.instanceId, threadId: parent.threadId,
  });
  const launch: PiNativeWorkerLaunch = {
    source: "team_assign", instanceId: `worker-${parent.threadId}`,
    assignmentId: `assignment-${parent.threadId}`, roleId: "developer",
    providerInstanceId: selection.instanceId, providerSessionId, parentThreadId: parent.threadId,
    parentProviderThreadId: ids.derive.providerThread({ driver, nativeThreadId: "native-thread" }),
    parentProviderTurnId: ProviderTurnId.make("native-parent-turn"),
    runId: RunId.make("native-parent-run"), runAttemptId: RunAttemptId.make("native-parent-attempt"),
    toolNodeId: NodeId.make("native-launch-tool"), coordinatorSessionId: "coordinator-1",
    toolCallId: "assign-call",
  };
  const checkpoint: PiNativeWorkerCheckpoint = {
    launch, owner: { processId: "process-1", coordinatorSessionId: "coordinator-1", epoch: "owner-epoch" },
    ownerGeneration: 1, captureEpoch: null, bindings: [], bindingGeneration: null,
    bindingCursors: [], cursor: null, cut: 0, recovery: false,
    capture: "opening", finalSequence: null, gaps: [],
  };
  const summary: PiNativeWorkerSummary = {
    launch, roleId: "developer", brief: "Implement", execution: "accepted",
    selected: { model: "selected-model", reasoning: "high" },
    capture: "opening", availability: "ready", gaps: [],
  };
  const run: OrchestrationV2Run = {
    id: launch.runId, threadId: parent.threadId, ordinal: 1, providerInstanceId: selection.instanceId,
    modelSelection: selection, providerThreadId: launch.parentProviderThreadId,
    userMessageId: MessageId.make("native-parent-message"), rootNodeId: NodeId.make("native-root"),
    activeAttemptId: launch.runAttemptId, status: "running", requestedAt: now, startedAt: now,
    completedAt: null, checkpointId: null, contextHandoffId: null,
  };
  yield* sink.write({ events: [parent, {
    id: yield* ids.allocate.event({ threadId: parent.threadId }), threadId: parent.threadId,
    type: "run.created", payload: run, occurredAt: now,
  }] });
  const turnItem: OrchestrationV2TurnItem = {
    id: TurnItemId.make("native-launch-item"), threadId: parent.threadId, runId: launch.runId,
    nodeId: launch.toolNodeId, providerThreadId: launch.parentProviderThreadId,
    providerTurnId: launch.parentProviderTurnId, nativeItemRef: null, parentItemId: null,
    ordinal: 1, status: "completed", title: "team_assign", startedAt: now, completedAt: now, updatedAt: now,
    type: "dynamic_tool", toolName: "team_assign", input: { roleId: "developer" }, output: { instanceId: launch.instanceId },
    nativeWorkerLaunch: launch,
  };
  const stored = admit ? yield* ingestor.ingestNormalized({
    providerSessionId, providerInstanceId: selection.instanceId, threadId: parent.threadId,
    runId: run.id, event: { type: "turn_item.updated", driver, turnItem },
    nativeWorkerAdmission: { summary, checkpoint },
    writeIfRunCurrent: { runId: run.id, activeAttemptId: launch.runAttemptId, expectedStatus: "running" },
  }) : [];
  const member = (yield* sink.nativeWorkers(providerSessionId))[0]!;
  const record = (entry: string, revision: number, tasks: string, argsState: "available" | "not_captured" = "available"): PiNativeWorkerRecord => ({
    kind: "tool",
    provenance: {
      instanceId: launch.instanceId, owner: checkpoint.owner, captureEpoch: "capture-epoch",
      id: entry, binding: 1, revision, sequence: revision, order: entry === "A" ? 1 : 2,
      nativeOrder: 1, occurredAt: null, observedAt: 1, nativeTime: null,
      nativeEntryId: entry, parentEntryId: null, toolCallId: `todo-${entry}`,
      ...(argsState === "not_captured" ? { recovered: true as const } : {}),
    },
    payload: {
      status: "completed", name: "rpiv_todo", argsState,
      ...(argsState === "available" ? { args: { subject: tasks } } : {}),
      result: null, isError: false,
      todoSnapshot: { format: "rpiv-todo-v1", tasks: [
        { id: 1, identityEntryId: `task-${entry}`, subject: tasks, status: "pending" },
      ] },
    },
  });
  const batch = (expected: PiNativeWorkerCheckpoint, cut: number, records: ReadonlyArray<PiNativeWorkerRecord>,
    head: string | null = "B"): ProviderAdapter.ProviderNativeWorkerBatch => {
    const bindings: PiNativeWorkerCheckpoint["bindings"] = [{
      generation: 1, historyId: "history-1", sessionId: "worker-session-1", state: "active",
      headEntryId: head, capture: "open", attemptIds: [], attempts: [], gaps: [],
      todoHead: { state: "ready", sourceEntryId: head },
    }];
    return { launch, summary: { ...summary, execution: "running", capture: "open",
      activated: { attemptId: "worker-attempt", model: "actual-model", reasoning: "low" } },
      expectedCheckpoint: expected, checkpoint: {
        ...expected, bindings, bindingGeneration: 1, cursor: `cursor-${cut}`, cut,
        captureEpoch: "capture-epoch", capture: "open",
      }, bindings, records, more: false };
  };
  return { now, sink, ingestor, ids, launch, checkpoint, summary, run, parent, member, stored, record, batch, turnItem };
});

layer("native worker persisted import", (it) => {
  it.effect("persists native message diagnostics and presents tool-only turns without empty rows", () =>
    Effect.gen(function* () {
      // Load the real client builders through Vite: mobile is a CommonJS package
      // and both clients use Bundler resolution, unlike this server project.
      type MessageRow = { message: { text: string } };
      type WebEntry = ({ kind: "message" } & MessageRow) | { kind: "work" };
      type WebInput = { visibleTurnItems: ReadonlyArray<OrchestrationV2ProjectedTurnItem>; optimisticMessages: [] };
      type WebState = { entries: ReadonlyArray<WebEntry> };
      const { deriveTimelineEntriesFromVisibleTurnItems, deriveTimelineEntriesFromVisibleTurnItemsWithState } =
        yield* Effect.promise(() => vi.importActual<{
          deriveTimelineEntriesFromVisibleTurnItems: (input: WebInput) => ReadonlyArray<WebEntry>;
          deriveTimelineEntriesFromVisibleTurnItemsWithState: (input: WebInput, previous?: WebState) => WebState;
        }>("../../../web/src/session-logic.ts"));
      const { buildThreadFeed } = yield* Effect.promise(() => vi.importActual<{
        buildThreadFeed: (rows: ReadonlyArray<OrchestrationV2ProjectedTurnItem>) =>
          ReadonlyArray<({ type: "message" } & MessageRow) | { type: "activity-group" }>;
      }>("../../../mobile/src/lib/threadActivity.ts"));
      const f = yield* seedNativeWorker();
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const message = (id: string, order: number,
        payload: Extract<PiNativeWorkerRecord, { kind: "message" }>["payload"]): PiNativeWorkerRecord => ({
        kind: "message", provenance: { ...f.record(id, 1, "").provenance,
          order, nativeOrder: order, toolCallId: null }, payload,
      });
      const toolCall = { type: "toolCall", toolCallId: "read-call", name: "read", arguments: {} } as const;
      const first = message("tool-only", 0, { role: "assistant", blocks: [toolCall], stopReason: "toolUse" });
      const tool: PiNativeWorkerRecord = { ...f.record("read", 1, ""), kind: "tool",
        provenance: { ...f.record("read", 1, "").provenance, order: 1, toolCallId: "read-call" },
        payload: { name: "read", status: "completed", argsState: "available", args: {},
          result: [{ type: "omitted", reason: "unsupported_content" }], isError: false } };
      const final = message("final", 2, { role: "assistant",
        blocks: [{ type: "text", text: "Final response" }], stopReason: "stop" });
      const page = f.batch(f.checkpoint, 1, [first, tool, final], null);
      assert.isTrue((yield* f.ingestor.ingestNativeWorker(page)).sourceProgress);
      const child = yield* projections.getThreadProjection(f.member.childThreadId!);
      assert.lengthOf(child.turnItems, 4);
      assert.lengthOf(child.messages, 2);
      const nativeItems = child.turnItems.filter((item) => item.nativeWorkerRecord !== undefined);
      assert.deepEqual(nativeItems.map((item) => item.nativeWorkerRecords),
        [first, tool, final].map((record) => [record.provenance]));
      assert.deepEqual(child.providerThreads[0]?.nativeMetadata?.nativeWorkerCheckpoint, page.checkpoint);
      const rows = nativeItems.map((item, position) => ({
        item, position, visibility: "local" as const, sourceThreadId: item.threadId, sourceItemId: item.id,
      }));
      const input: WebInput = { visibleTurnItems: rows, optimisticMessages: [] };
      const web = deriveTimelineEntriesFromVisibleTurnItemsWithState(input);
      assert.deepEqual(web.entries.map((entry) => entry.kind), ["work", "message"]);
      assert.deepEqual(deriveTimelineEntriesFromVisibleTurnItems(input), web.entries);
      const mobile = buildThreadFeed(rows);
      assert.deepEqual(mobile.map((entry) => entry.type), ["activity-group", "message"]);
      assert.deepEqual(mobile.flatMap((entry) => entry.type === "message" ? [entry.message.text] : []), ["Final response"]);
      const sequence = yield* f.sink.latestSequence();
      assert.isFalse((yield* f.ingestor.ingestNativeWorker(page)).committed);
      assert.isFalse((yield* f.ingestor.ingestNativeWorker({ ...page, expectedCheckpoint: page.checkpoint })).sourceProgress);
      assert.equal(yield* f.sink.latestSequence(), sequence);
      assert.deepEqual(yield* projections.getThreadProjection(f.member.childThreadId!), child);

      const revised = { kind: "message" as const, provenance: { ...first.provenance, revision: 2, sequence: 4 },
        payload: { role: "assistant" as const, blocks: [{ type: "text" as const, text: "Now visible" }] } };
      const next = f.batch(page.checkpoint, 2, [revised], null);
      yield* f.ingestor.ingestNativeWorker(next);
      const updated = yield* projections.getThreadProjection(f.member.childThreadId!);
      assert.lengthOf(updated.turnItems, 4);
      assert.lengthOf(updated.messages, 2);
      assert.deepEqual(updated.turnItems.map((item) => [item.id, item.ordinal]),
        child.turnItems.map((item) => [item.id, item.ordinal]));
      assert.deepEqual(updated.turnItems.find((item) => item.id === nativeItems[0]?.id)?.nativeWorkerRecords, [revised.provenance]);
      assert.deepEqual(updated.providerThreads[0]?.nativeMetadata?.nativeWorkerCheckpoint, next.checkpoint);
      const updatedRows = updated.turnItems.filter((item) => item.nativeWorkerRecord !== undefined).map((item, position) => ({
        ...rows[position]!, item,
      }));
      const updatedInput = { ...input, visibleTurnItems: updatedRows };
      const updatedWeb = deriveTimelineEntriesFromVisibleTurnItemsWithState(updatedInput, web);
      assert.deepEqual(updatedWeb.entries, deriveTimelineEntriesFromVisibleTurnItems(updatedInput));
      assert.deepEqual(updatedWeb.entries.map((entry) => entry.kind), ["message", "work", "message"]);
      assert.deepEqual(buildThreadFeed(updatedRows).map((entry) => entry.type), ["message", "activity-group", "message"]);
      assert.isFalse((yield* f.ingestor.ingestNativeWorker(next)).committed);

      const cases = [
        { payload: { role: "assistant", blocks: [{ type: "text", text: "one" }, toolCall, { type: "text", text: "two" }] },
          text: "one\ntwo" },
        { payload: { role: "assistant", blocks: [{ type: "omitted", reason: "unsupported_content" }] },
          text: "[T3 notice] Some content is unavailable." },
        { payload: { role: "assistant", blocks: [toolCall], stopReason: "error", errorMessage: "Provider failed" },
          text: "[T3 notice] Provider failed" },
        { payload: { role: "assistant", blocks: [{ type: "text", text: "Partial" }, toolCall],
          stopReason: "stop", errorMessage: "Still report this" },
          text: "Partial\n\n[T3 notice] Still report this" },
        { payload: { role: "assistant", blocks: [], stopReason: "error" },
          text: "[T3 notice] The response failed." },
        { payload: { role: "assistant", blocks: [], stopReason: "error", errorMessage: " \n" },
          text: "[T3 notice] The response failed." },
        { payload: { role: "assistant", blocks: [{ type: "text", text: "Partial" }, toolCall],
          stopReason: "aborted", errorMessage: "Disconnected" },
          text: "Partial\n\n[T3 notice] Disconnected\n\n[T3 notice] The response was aborted." },
        { payload: { role: "assistant", blocks: [{ type: "text", text: "Partial" }], stopReason: "length" },
          text: "Partial\n\n[T3 notice] The response reached its length limit." },
        ...(["pending", "stop", "toolUse", "deferred"] as const).map((stopReason) => ({
          payload: { role: "assistant" as const, blocks: [toolCall], stopReason }, text: "",
        })),
        { payload: { role: "user", blocks: [{ type: "text", text: "  user" },
          { type: "omitted", reason: "unsupported_content" }, { type: "text", text: "text  " }],
          stopReason: "error", errorMessage: "Do not add" }, text: "  user\ntext  " },
      ] satisfies ReadonlyArray<{ payload: Extract<PiNativeWorkerRecord, { kind: "message" }>["payload"]; text: string }>;
      const diagnostics = f.batch(next.checkpoint, 3,
        cases.map((test, index) => message(`diagnostic-${index}`, index + 3, test.payload)), null);
      yield* f.ingestor.ingestNativeWorker(diagnostics);
      const persisted = yield* projections.getThreadProjection(f.member.childThreadId!);
      assert.deepEqual(persisted.messages.slice(2).map((entry) => entry.text), cases.map((test) => test.text));
      const diagnosticItems = persisted.turnItems.filter((item) => item.nativeWorkerRecord?.id.startsWith("diagnostic-"));
      assert.deepEqual(diagnosticItems.map((item) => "text" in item ? item.text : null), cases.map((test) => test.text));
      const diagnosticRows = diagnosticItems.map((item, position) => ({
        item, position, visibility: "local" as const, sourceThreadId: item.threadId, sourceItemId: item.id,
      }));
      const expectedTexts = cases.map((test) => test.text).filter((text) => text.trim().length > 0);
      assert.deepEqual(deriveTimelineEntriesFromVisibleTurnItems({
        visibleTurnItems: diagnosticRows, optimisticMessages: [],
      }).flatMap((entry) => entry.kind === "message" ? [entry.message.text] : []), expectedTexts);
      assert.deepEqual(buildThreadFeed(diagnosticRows).flatMap((entry) =>
        entry.type === "message" ? [entry.message.text] : []), expectedTexts);
      assert.lengthOf(persisted.turnItems.filter((item) => item.type === "dynamic_tool"), 1);
    }));

  it.effect("keeps one child and immutable admission when the same worker is replayed in another attempt", () =>
    Effect.gen(function* () {
      const f = yield* seedNativeWorker();
      const launch = { ...f.launch, runAttemptId: RunAttemptId.make("retried-attempt"),
        parentProviderTurnId: ProviderTurnId.make("retried-turn"), toolNodeId: NodeId.make("retried-tool") };
      yield* f.ingestor.ingestNormalized({
        providerSessionId: launch.providerSessionId, providerInstanceId: launch.providerInstanceId,
        threadId: launch.parentThreadId, runId: launch.runId,
        event: { type: "turn_item.updated", driver: ProviderDriverKind.make("pi"),
          turnItem: { ...f.turnItem, id: TurnItemId.make("retried-launch-item"),
            nodeId: launch.toolNodeId, providerTurnId: launch.parentProviderTurnId,
            nativeWorkerLaunch: launch } },
        nativeWorkerAdmission: { summary: { ...f.summary, launch },
          checkpoint: { ...f.checkpoint, launch } },
      });
      const members = yield* f.sink.nativeWorkers(launch.providerSessionId);
      assert.lengthOf(members, 1);
      assert.equal(members[0]?.childThreadId, f.member.childThreadId);
      assert.deepEqual(members[0]?.nativeWorker?.launch, f.launch);
    }),
  );
  it.effect.each(["linked", "late owner", "missing owner", "mismatched detail", "limited detail",
    "agent_run", "owner lost", "changed launch", "transaction owner lost",
    "transaction generation changed", "repeated transaction loss", "replayed launch",
    "stale attempt"] as const)(
    "the ordered run subscriber persists before terminal: %s", (scenario) =>
    Effect.gen(function* () {
      const f = yield* seedNativeWorker(false);
      let ownerReady = scenario !== "late owner" && scenario !== "missing owner";
      let ownerGeneration = 1;
      let admissionChecks = 0;
      const admissionChecked = yield* Deferred.make<void>();
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      let beforeTerminal: OrchestrationV2ThreadProjection | undefined;
      const finalized = yield* Deferred.make<void>();
      // EventSink executes this connection check inside writeIfRunCurrent's transaction,
      // after the subscriber has reconciled the owner and constructed the admission.
      const fencedSink: EventSink.EventSinkV2Shape = {
        ...f.sink,
        writeIfRunCurrent: (input) => f.sink.writeIfRunCurrent({
          ...input,
          ...(input.isConnected === undefined ? {} : {
            isConnected: Effect.gen(function* () {
              admissionChecks++;
              if (scenario === "transaction owner lost") ownerReady = false;
              if (scenario === "repeated transaction loss" ||
                (scenario === "transaction generation changed" && admissionChecks === 1)) {
                ownerGeneration++;
              }
              yield* Deferred.succeed(admissionChecked, undefined);
              return yield* input.isConnected!;
            }),
          }),
        }),
      };
      const fencedIngestor = yield* ProviderEventIngestor.ProviderEventIngestorV2.pipe(
        Effect.provide(Layer.fresh(ProviderEventIngestor.layer)),
        Effect.provideService(EventSink.EventSinkV2, fencedSink),
      );
      const providerThread: OrchestrationV2ProviderThread = {
        id: f.launch.parentProviderThreadId, driver: ProviderDriverKind.make("pi"),
        providerInstanceId: f.launch.providerInstanceId, providerSessionId: f.launch.providerSessionId,
        appThreadId: f.parent.threadId, ownerNodeId: null, nativeThreadRef: null,
        nativeConversationHeadRef: null, status: "active", firstRunOrdinal: 1, lastRunOrdinal: 1,
        handoffIds: [], forkedFrom: null, createdAt: f.now, updatedAt: f.now,
      };
      assert.isNotNull(f.run.rootNodeId);
      const rootNodeId = f.run.rootNodeId!;
      const scope: OrchestrationV2CheckpointScope = {
        id: CheckpointScopeId.make("native-root-scope"), threadId: f.parent.threadId,
        runId: f.run.id, nodeId: rootNodeId, parentScopeId: null,
        providerThreadId: providerThread.id, kind: "root_run", ordinalWithinParent: 1,
        advancesAppRunCount: true, cwd: process.cwd(), createdAt: f.now,
      };
      const node: OrchestrationV2ExecutionNode = {
        id: rootNodeId, threadId: f.parent.threadId, runId: f.run.id,
        parentNodeId: null, rootNodeId, kind: "root_turn", status: "running",
        countsForRun: true, providerThreadId: providerThread.id, providerTurnId: f.launch.parentProviderTurnId,
        nativeItemRef: null, runtimeRequestId: null, checkpointScopeId: scope.id,
        startedAt: f.now, completedAt: null,
      };
      yield* Effect.gen(function* () {
        const execution = yield* RunExecutionService.RunExecutionServiceV2;
        yield* execution.startRootRun({
          commandId: CommandId.make("native-admission"), appThread: f.parent.payload,
          providerSessionId: f.launch.providerSessionId, run: f.run, rootNode: node,
          checkpointScope: scope, providerThread,
          attempt: { id: f.launch.runAttemptId, runId: f.run.id, attemptOrdinal: 1,
            rootNodeId: node.id, providerInstanceId: f.launch.providerInstanceId,
            providerThreadId: providerThread.id, providerTurnId: f.launch.parentProviderTurnId,
            reason: "initial", status: "running", startedAt: f.now, completedAt: null },
          attemptId: f.launch.runAttemptId, providerTurnOrdinal: 1,
          shouldFinalizeRun: () => Effect.gen(function* () {
            const projection = yield* projections.getThreadProjection(f.parent.threadId);
            const current = projection.runs.find(run => run.id === f.run.id)?.activeAttemptId ===
              f.launch.runAttemptId;
            if (!current) yield* Deferred.succeed(finalized, undefined);
            return current;
          }),
          message: { messageId: f.run.userMessageId, text: "Delegate", attachments: [],
            createdBy: "user", creationSource: "web" },
          modelSelection: f.run.modelSelection!,
          runtimePolicy: { runtimeMode: "full-access", interactionMode: "default", cwd: process.cwd(),
            approvalPolicy: "never", sandboxPolicy: { type: "readOnly", access: { type: "fullAccess" },
              networkAccess: false } },
          session: {
            instanceId: f.launch.providerInstanceId, providerSessionId: f.launch.providerSessionId,
            providerSession: {
              id: f.launch.providerSessionId, driver: providerThread.driver,
              providerInstanceId: f.launch.providerInstanceId, status: "ready", cwd: process.cwd(),
              model: f.run.modelSelection!.model, capabilities: CodexProviderCapabilitiesV2,
              createdAt: f.now, updatedAt: f.now, lastError: null,
            },
            driver: providerThread.driver,
            startTurn: () => Effect.sync(() => { if (scenario === "late owner") ownerReady = true; }),
            ensureThread: () => Effect.die("Passive shell must not open"),
            resumeThread: () => Effect.die("Passive shell must not resume"),
            steerTurn: () => Effect.die("Passive shell must not steer"),
            interruptTurn: () => Effect.die("Unused"),
            respondToRuntimeRequest: () => Effect.die("Unused"),
            readThreadSnapshot: () => Effect.die("Passive shell must not read ordinary snapshot"),
            rollbackThread: () => Effect.die("Unused"), forkThread: () => Effect.die("Unused"),
            nativeWorkers: { get owner() { return ownerReady ? f.checkpoint.owner : undefined; },
              get ownerGeneration() { return ownerGeneration; }, hints: Stream.empty,
              reconcile: () => Effect.sync(() => {
                if (scenario === "owner lost") ownerReady = false;
                return scenario === "changed launch" ? { ...f.summary,
                  launch: { ...f.launch, toolCallId: "other-launch" } }
                : scenario === "mismatched detail"
                ? { ...f.summary, gaps: ["provenance_mismatch"] }
                : scenario === "limited detail" ? { ...f.summary, availability: "unavailable", gaps: ["unavailable"] }
                : f.summary;
              }),
              read: () => Effect.die("Importer only"),
              hasPendingWork: () => Effect.succeed(false) },
            events: Stream.fromIterable([
              { type: "turn_item.updated", driver: providerThread.driver, turnItem: scenario === "agent_run"
                ? { ...f.turnItem, type: "dynamic_tool", toolName: "agent_run" } : f.turnItem },
              ...(scenario === "replayed launch" ? [{
                type: "turn_item.updated" as const, driver: providerThread.driver, turnItem: f.turnItem,
              }] : []),
              { type: "turn.terminal", driver: providerThread.driver,
                providerThreadId: providerThread.id, providerTurnId: f.launch.parentProviderTurnId,
                runOrdinal: 1, status: "completed", failure: null, threadDisposition: "reusable" },
            ] satisfies ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event>).pipe(
              Stream.tap((event) => Effect.gen(function* () {
                if (scenario === "stale attempt" && event.type === "turn_item.updated") {
                  yield* f.sink.write({ events: [{
                    id: yield* f.ids.allocate.event({ threadId: f.parent.threadId }),
                    threadId: f.parent.threadId, type: "run.updated", occurredAt: f.now,
                    payload: { ...f.run, activeAttemptId: RunAttemptId.make("newer-attempt") },
                  }] });
                }
                if (event.type !== "turn.terminal") return;
                if (scenario.startsWith("transaction") || scenario === "repeated transaction loss") {
                  yield* Deferred.await(admissionChecked);
                }
                beforeTerminal = yield* projections.getThreadProjection(f.parent.threadId);
              })),
              Stream.orDie,
            ),
          },
        });
        yield* Deferred.await(finalized);
      }).pipe(
        Effect.provide(RunExecutionService.layer.pipe(Layer.provide(Layer.mergeAll(
          ServerSettings.layerTest(), McpAppModelContext.layerEmpty,
          Layer.mock(CheckpointService.CheckpointServiceV2)({ captureBaseline: () => Effect.void }),
        )))),
        Effect.provideService(RunFinalizationService.RunFinalizationObserver, {
          refreshAfterTurn: () => Deferred.succeed(finalized, undefined).pipe(Effect.asVoid),
          refresh: () => Effect.void,
        }),
        Effect.provideService(ProviderEventIngestor.ProviderEventIngestorV2, fencedIngestor),
      );
      const parent = yield* projections.getThreadProjection(f.parent.threadId);
      assert.isDefined(beforeTerminal);
      assert.lengthOf(beforeTerminal!.turnItems.filter(item => item.id === f.turnItem.id),
        scenario === "stale attempt" ? 0 : 1);
      assert.equal(beforeTerminal!.runs[0]?.status, "running");
      if (scenario === "transaction owner lost" || scenario === "repeated transaction loss" ||
        scenario === "stale attempt") assert.deepEqual(beforeTerminal!.subagents, []);
      if (scenario === "transaction generation changed" || scenario === "replayed launch") {
        assert.lengthOf(beforeTerminal!.subagents, 1);
      }
      const store = yield* EventStore.EventStoreV2;
      const history = yield* store.read({ threadId: f.parent.threadId }).pipe(Stream.runCollect);
      const tools = history.filter(({ event }) =>
        event.type === "turn-item.updated" && event.payload.id === f.turnItem.id);
      assert.lengthOf(tools, scenario === "stale attempt" ? 0 : scenario === "replayed launch" ? 2 : 1);
      if (scenario === "stale attempt") {
        assert.equal(parent.runs[0]?.activeAttemptId, "newer-attempt");
        assert.deepEqual(parent.turnItems, []);
        assert.deepEqual(parent.subagents, []);
        return;
      }
      assert.equal(parent.runs[0]?.status, "waiting"); // Normal checkpoint effect remains queued.
      assert.equal(parent.attempts[0]?.status, "completed");
      const terminal = history.find(({ event }) => event.type === "run-attempt.updated" &&
        event.payload.id === f.launch.runAttemptId && event.payload.status === "completed");
      assert.isDefined(terminal);
      assert.isTrue(tools.every(tool => tool.sequence < terminal!.sequence));
      assert.equal(parent.turnItems.find((item) => item.id === f.turnItem.id)?.type, "dynamic_tool");
      if (scenario === "missing owner" || scenario === "mismatched detail" || scenario === "agent_run" ||
        scenario === "owner lost" || scenario === "changed launch" ||
        scenario === "transaction owner lost" || scenario === "repeated transaction loss") {
        assert.deepEqual(parent.subagents, []);
        if (scenario === "transaction owner lost") assert.equal(admissionChecks, 1);
        if (scenario === "repeated transaction loss") assert.equal(admissionChecks, 2);
        return;
      }
      const member = parent.subagents[0]!;
      const child = yield* projections.getThreadProjection(member.childThreadId!);
      assert.deepEqual(member.nativeWorker?.launch, f.launch);
      assert.equal(child.providerThreads[0]?.nativeThreadRef, null);
      assert.equal(child.thread.modelSelection.model, "selected-model");
      assert.deepEqual(child.runs, []);
      if (scenario === "transaction generation changed") {
        assert.equal(admissionChecks, 2);
        const checkpoint = child.providerThreads[0]?.nativeMetadata?.nativeWorkerCheckpoint;
        assert.equal(checkpoint?.ownerGeneration, 2);
      }
    }),
  );

  it.effect("rolls back appended capture events and projections on a pre-commit store fault", () =>
    Effect.gen(function* () {
      const f = yield* seedNativeWorker();
      const store = yield* EventStore.EventStoreV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const before = yield* f.sink.latestSequence();
      const page = f.batch(f.checkpoint, 1, [f.record("B", 1, "Atomic B")]);
      const faultySink = yield* EventSink.EventSinkV2.pipe(Effect.provide(Layer.fresh(EventSink.layer)),
        Effect.provideService(EventStore.EventStoreV2, {
          ...store, append: (input) => store.append(input).pipe(Effect.andThen(Effect.fail(
            new EventStore.EventStoreAppendEventsError({ eventCount: input.events.length, cause: "before-commit" }),
          ))),
        }));
      const faultyIngestor = yield* ProviderEventIngestor.ProviderEventIngestorV2.pipe(
        Effect.provide(Layer.fresh(ProviderEventIngestor.layer)),
        Effect.provideService(EventSink.EventSinkV2, faultySink),
      );
      assert.equal((yield* Effect.exit(faultyIngestor.ingestNativeWorker(page)))._tag, "Failure");
      assert.equal(yield* f.sink.latestSequence(), before);
      const unchanged = yield* projections.getThreadProjection(f.member.childThreadId!);
      assert.deepEqual(unchanged.turnItems, []);
      assert.deepEqual(unchanged.plans, []);
      assert.equal(unchanged.thread.modelSelection.model, "selected-model");
      assert.deepEqual(unchanged.providerThreads[0]?.nativeMetadata?.nativeWorkerCheckpoint, f.checkpoint);
      assert.isTrue((yield* f.ingestor.ingestNativeWorker(page)).committed);
      assert.equal((yield* projections.getThreadProjection(f.member.childThreadId!)).turnItems.length, 2);
    }),
  );
  it.effect("replays safely when the caller loses a committed capture receipt", () =>
    Effect.gen(function* () {
      const f = yield* seedNativeWorker();
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const page = f.batch(f.checkpoint, 1, [f.record("B", 1, "Committed B")]);
      const lostReceipt = yield* Effect.exit(f.ingestor.ingestNativeWorker(page).pipe(
        Effect.andThen(Effect.die(new Error("lost capture receipt"))),
      ));
      assert.equal(lostReceipt._tag, "Failure");
      const captured = yield* projections.getThreadProjection(f.member.childThreadId!);
      assert.deepEqual(captured.providerThreads[0]?.nativeMetadata?.nativeWorkerCheckpoint, page.checkpoint);
      assert.equal(captured.thread.modelSelection.model, "actual-model");
      assert.equal(captured.providerThreads[0]?.nativeMetadata?.checklistSelection?.sourceEntryId, "B");
      const sequence = yield* f.sink.latestSequence();
      assert.isFalse((yield* f.ingestor.ingestNativeWorker(page)).committed);
      assert.isFalse((yield* f.ingestor.ingestNativeWorker({
        ...page, expectedCheckpoint: page.checkpoint,
      })).sourceProgress);
      assert.equal(yield* f.sink.latestSequence(), sequence);
      assert.deepEqual(yield* projections.getThreadProjection(f.member.childThreadId!), captured);
    }));
  it.effect("persists complete content, explicit omissions and only a safe fragment restart manifest", () =>
    Effect.gen(function* () {
      const f = yield* seedNativeWorker();
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const message: PiNativeWorkerRecord = {
        kind: "message", provenance: { ...f.record("complete-message", 1, "").provenance,
          toolCallId: null },
        payload: { role: "assistant", blocks: [
          { type: "text", text: "complete message" },
          { type: "omitted", reason: "unsupported_content" },
        ] },
      };
      const tool = f.record("omitted-tool", 1, "");
      const page = f.batch(f.checkpoint, 1, [message, {
        ...tool, kind: "tool", payload: { name: "read_image", status: "completed",
          argsState: "available", args: { path: "image.png" },
          result: [{ type: "omitted", reason: "unsupported_content" }], isError: false },
      }], null);
      const restart = { id: "incomplete-record", binding: 1, revision: 1, epoch: "capture-epoch",
        sequence: 2, totalBytes: 4 * 1024 * 1024, cursor: null };
      assert.isTrue((yield* f.ingestor.ingestNativeWorker({
        ...page, checkpoint: { ...page.checkpoint, fragment: restart, gaps: ["unsupported_content"] },
      })).committed);
      const child = yield* projections.getThreadProjection(f.member.childThreadId!);
      assert.deepEqual(child.messages.map((entry) => entry.text),
        ["complete message\n\n[T3 notice] Some content is unavailable."]);
      const storedTool = child.turnItems.find((entry) => entry.type === "dynamic_tool");
      assert.equal(storedTool?.type, "dynamic_tool");
      if (storedTool?.type !== "dynamic_tool") return;
      assert.deepEqual(storedTool.output, [{ type: "omitted", reason: "unsupported_content" }]);
      const checkpoint = child.providerThreads[0]!.nativeMetadata!.nativeWorkerCheckpoint!;
      assert.deepEqual(checkpoint.fragment, restart);
      assert.deepEqual(checkpoint.gaps, ["unsupported_content"]);
      assert.lengthOf(child.turnItems.filter((item) => item.nativeWorkerRecord !== undefined), 2);
    }));
  it.effect("admits the ordinary tool and passive shell atomically, then imports after a newer parent run", () =>
    Effect.gen(function* () {
      const f = yield* seedNativeWorker();
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      assert.deepEqual(f.stored.map((stored) => stored.event.type),
        ["turn-item.updated", "thread.created", "provider-thread.updated", "subagent.updated"]);
      const shell = yield* projections.getThreadProjection(f.member.childThreadId!);
      assert.equal(shell.providerThreads[0]?.nativeThreadRef, null);
      assert.equal(shell.runs.length, 0);
      assert.equal(shell.thread.lineage.relationshipToParent, "subagent");
      assert.equal(shell.thread.modelSelection.model, "selected-model");
      // Completing the original run and beginning another is valid; native
      // ownership belongs to its original launch attempt, not lastRunOrdinal.
      yield* f.sink.write({ events: [{
        id: yield* f.ids.allocate.event({ threadId: f.parent.threadId }),
        type: "run.updated", threadId: f.parent.threadId, occurredAt: f.now,
        payload: { ...f.run, status: "completed", completedAt: f.now },
      }, {
        id: yield* f.ids.allocate.event({ threadId: f.parent.threadId }),
        type: "run.created", threadId: f.parent.threadId, occurredAt: f.now,
        payload: { ...f.run, id: RunId.make("new-parent-run"), ordinal: 2,
          activeAttemptId: RunAttemptId.make("new-parent-attempt") },
      }, {
        id: yield* f.ids.allocate.event({ threadId: f.parent.threadId }),
        type: "provider-thread.updated", threadId: f.parent.threadId, occurredAt: f.now,
        payload: {
          id: f.launch.parentProviderThreadId, driver: ProviderDriverKind.make("pi"),
          providerInstanceId: f.launch.providerInstanceId, providerSessionId: f.launch.providerSessionId,
          appThreadId: f.parent.threadId, ownerNodeId: null, nativeThreadRef: null,
          nativeConversationHeadRef: null, status: "active", firstRunOrdinal: 1, lastRunOrdinal: 2,
          handoffIds: [], forkedFrom: null, createdAt: f.now, updatedAt: f.now,
        },
      }] });
      const page = f.batch(f.checkpoint, 2, [f.record("A", 1, "Older A"), f.record("B", 1, "Selected B")]);
      const receipt = yield* f.ingestor.ingestNativeWorker(page);
      assert.isTrue(receipt.committed);
      const captured = yield* projections.getThreadProjection(f.member.childThreadId!);
      assert.equal(captured.thread.modelSelection.model, "actual-model");
      assert.equal(captured.providerThreads[0]?.nativeMetadata?.checklistSelection?.sourceEntryId, "B");
      assert.equal(captured.plans.find((plan) => plan.kind === "todo_list")?.kind, "todo_list");
      const checkpoint = captured.providerThreads[0]!.nativeMetadata!.nativeWorkerCheckpoint!;
      // A recovered A has a higher local revision but is not selected B.
      const recovered = f.batch(checkpoint, 3, [f.record("A", 9, "Recovered A", "not_captured")]);
      yield* f.ingestor.ingestNativeWorker(recovered);
      const reread = yield* projections.getThreadProjection(f.member.childThreadId!);
      const toolA = reread.turnItems.find((item) => item.nativeWorkerRecord?.nativeEntryId === "A");
      assert.equal(toolA?.type, "dynamic_tool");
      assert.deepEqual(toolA?.type === "dynamic_tool" ? toolA.input : null, { subject: "Older A" });
      assert.equal(reread.providerThreads[0]?.nativeMetadata?.checklistSelection?.sourceEntryId, "B");
      const todo = reread.plans.find((plan) => plan.kind === "todo_list");
      assert.equal(todo?.kind === "todo_list" ? todo.steps[0]?.text : null, "Selected B");
      const sequence = yield* f.sink.latestSequence();
      assert.isFalse((yield* f.ingestor.ingestNativeWorker(recovered)).committed); // lost receipt retry
      assert.equal(yield* f.sink.latestSequence(), sequence);
      const replay = { ...recovered, expectedCheckpoint: recovered.checkpoint };
      assert.isFalse((yield* f.ingestor.ingestNativeWorker(replay)).sourceProgress);
      assert.equal(yield* f.sink.latestSequence(), sequence);
      // A selected record outside the page is unknown, never fallback/clear.
      const absent = f.batch(recovered.checkpoint, 4, [], "C");
      yield* f.ingestor.ingestNativeWorker(absent);
      assert.equal((yield* projections.getThreadProjection(f.member.childThreadId!))
        .providerThreads[0]?.nativeMetadata?.checklistSelection?.state, "unavailable");
      const arrived = f.batch(absent.checkpoint, 5, [f.record("C", 1, "Selected C")], "C");
      yield* f.ingestor.ingestNativeWorker(arrived);
      assert.equal((yield* projections.getThreadProjection(f.member.childThreadId!))
        .providerThreads[0]?.nativeMetadata?.checklistSelection?.sourceEntryId, "C");
      const cleared = f.batch(arrived.checkpoint, 6, [], null);
      yield* f.ingestor.ingestNativeWorker(cleared);
      assert.equal((yield* projections.getThreadProjection(f.member.childThreadId!))
        .providerThreads[0]?.nativeMetadata?.checklistSelection?.sourceEntryId, null);
    }),
  );

  it.effect("imports full native checklist membership, deleted lifetimes and clear without Pi durations", () =>
    Effect.gen(function* () {
      const f = yield* seedNativeWorker();
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const record = f.record("B", 1, "Visible");
      if (record.kind !== "tool") return yield* Effect.die("Expected native tool.");
      const snapshot = { format: "rpiv-todo-v1", tasks: [
        { id: 1, identityEntryId: "first-life", subject: "Visible", status: "pending" },
        { id: 2, identityEntryId: "hidden-life", subject: "Hidden by filtered list", status: "completed" },
        { id: 3, identityEntryId: "deleted-life", subject: "Deleted", status: "deleted" },
      ] } as const;
      const page = f.batch(f.checkpoint, 1, [{ ...record, payload: {
        ...record.payload, args: { action: "list", status: "pending" }, todoSnapshot: snapshot,
      } }]);
      yield* f.ingestor.ingestNativeWorker(page);
      const child = yield* projections.getThreadProjection(f.member.childThreadId!);
      const plan = child.plans.find((row) => row.kind === "todo_list");
      if (plan?.kind !== "todo_list") return yield* Effect.die("Missing selected native checklist.");
      assert.deepEqual(plan.steps.map((step) => step.text), ["Visible", "Hidden by filtered list"]);
      assert.isTrue(plan.steps.every((step) => step.durationMs === undefined));
      assert.isNull(plan.runId);
      const tool = child.turnItems.find((row) => row.type === "dynamic_tool");
      assert.deepEqual(tool?.type === "dynamic_tool" ? tool.output : undefined, snapshot);
      const cleared = f.batch(page.checkpoint, 2, [], null);
      yield* f.ingestor.ingestNativeWorker(cleared);
      const empty = yield* projections.getThreadProjection(f.member.childThreadId!);
      assert.equal(empty.providerThreads[0]?.nativeMetadata?.checklistSelection?.state, "ready");
      assert.isNull(empty.providerThreads[0]?.nativeMetadata?.checklistSelection?.sourceEntryId);
      const reused = f.record("B", 2, "New lifespan");
      if (reused.kind !== "tool") return yield* Effect.die("Expected native tool.");
      yield* f.ingestor.ingestNativeWorker(f.batch(cleared.checkpoint, 3, [{ ...reused,
        payload: { ...reused.payload, todoSnapshot: { format: "rpiv-todo-v1", tasks: [
          { id: 1, identityEntryId: "second-life", subject: "New lifespan", status: "pending" },
        ] } },
      }]));
      // The current pointer, not a historical plan's array position, owns the new lifespan.
      const current = yield* projections.getThreadProjection(f.member.childThreadId!);
      const currentPlan = current.plans.find((row) =>
        row.id === current.providerThreads[0]?.nativeMetadata?.checklistSelection?.planId);
      if (currentPlan?.kind !== "todo_list") return yield* Effect.die("Missing reused task.");
      assert.notEqual(currentPlan.steps[0]?.id, plan.steps[0]?.id);
      assert.equal(currentPlan.steps[0]?.text, "New lifespan");
    }),
  );

  it.effect("collapses tool records without comparing their independent revisions or erasing captured arguments", () =>
    Effect.gen(function* () {
      const f = yield* seedNativeWorker();
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const start: PiNativeWorkerRecord = {
        kind: "tool", provenance: {
          ...f.record("A", 10, "").provenance, id: "start", nativeEntryId: "start",
          toolCallId: "same-call", order: 1,
        }, payload: {
          status: "started", name: "shell", argsState: "available",
          args: { command: "pwd" }, result: null, isError: false,
        },
      };
      const result: PiNativeWorkerRecord = {
        kind: "tool", provenance: {
          ...start.provenance, id: "result", nativeEntryId: "result", order: 2, revision: 1,
        }, payload: {
          status: "completed", name: "shell", argsState: "not_captured",
          result: [{ type: "text", text: "complete output" }], isError: false,
        },
      };
      const first = f.batch(f.checkpoint, 1, [start], null);
      yield* f.ingestor.ingestNativeWorker(first);
      const second = f.batch(first.checkpoint, 2, [result], null);
      yield* f.ingestor.ingestNativeWorker(second);
      const recovery = f.batch(second.checkpoint, 3, [{
        ...start, provenance: { ...start.provenance, revision: 11, recovered: true },
        payload: { ...start.payload, argsState: "not_captured", args: undefined },
      }], null);
      yield* f.ingestor.ingestNativeWorker(recovery);
      const child = yield* projections.getThreadProjection(f.member.childThreadId!);
      const tools = child.turnItems.filter((item) => item.type === "dynamic_tool");
      assert.lengthOf(tools, 1);
      assert.equal(tools[0]?.status, "completed");
      assert.deepEqual(tools[0]?.output, [{ type: "text", text: "complete output" }]);
      assert.deepEqual(tools[0]?.input, { command: "pwd" });
      assert.deepEqual(tools[0]?.nativeWorkerRecords?.map((entry) => [entry.id, entry.revision]),
        [["result", 1], ["start", 11]]);
      const before = yield* f.sink.latestSequence();
      yield* f.ingestor.ingestNativeWorker({ ...recovery, expectedCheckpoint: recovery.checkpoint });
      assert.equal(yield* f.sink.latestSequence(), before);
    }),
  );

  it.effect("preserves historical binding copies and gaps without selecting an unproven new head", () =>
    Effect.gen(function* () {
      const f = yield* seedNativeWorker();
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const first = f.batch(f.checkpoint, 1, [f.record("B", 1, "First binding B")]);
      yield* f.ingestor.ingestNativeWorker(first);
      const bindings: PiNativeWorkerCheckpoint["bindings"] = [
        { ...first.bindings[0]!, state: "historical" },
        { ...first.bindings[0]!, generation: 2, historyId: "history-2", sessionId: "session-2",
          todoHead: undefined, gaps: ["resync_required"] },
      ];
      const unknown = {
        ...f.batch(first.checkpoint, 2, []), bindings,
        checkpoint: { ...first.checkpoint, bindings, bindingGeneration: 2, cursor: null,
          cut: 2, recovery: true, gaps: ["resync_required"] as const },
      };
      assert.isTrue((yield* f.ingestor.ingestNativeWorker(unknown)).committed);
      const beforeCopy = yield* projections.getThreadProjection(f.member.childThreadId!);
      assert.equal(beforeCopy.providerThreads[0]?.nativeMetadata?.checklistSelection?.list.nativeSessionId,
        "session-2");
      assert.equal(beforeCopy.providerThreads[0]?.nativeMetadata?.checklistSelection?.state, "unavailable");
      const knownBindings = bindings.map((binding) => binding.generation === 2
        ? { ...binding, todoHead: { state: "ready" as const, sourceEntryId: "B" } } : binding);
      const copy = f.record("B", 1, "Second binding B");
      const known = { ...unknown,
        expectedCheckpoint: beforeCopy.providerThreads[0]!.nativeMetadata!.nativeWorkerCheckpoint!,
        bindings: knownBindings,
        records: [{ ...copy, provenance: { ...copy.provenance, binding: 2 } }],
        checkpoint: { ...unknown.checkpoint, bindings: knownBindings, cut: 3 } };
      assert.isTrue((yield* f.ingestor.ingestNativeWorker(known)).committed);
      const child = yield* projections.getThreadProjection(f.member.childThreadId!);
      assert.equal(child.turnItems.filter((item) => item.type === "dynamic_tool").length, 2);
      assert.equal(child.providerThreads[0]?.nativeMetadata?.checklistSelection?.list.nativeSessionId, "session-2");
      assert.equal(child.providerThreads[0]?.nativeMetadata?.checklistSelection?.sessionGeneration, 2);
      assert.deepEqual(child.providerThreads[0]?.nativeMetadata?.nativeWorkerCheckpoint?.gaps, ["resync_required"]);
      const todo = child.plans.find((plan) => plan.kind === "todo_list");
      assert.equal(todo?.kind === "todo_list" ? todo.steps[0]?.text : null, "Second binding B");
    }),
  );

  it.effect("rejects a batch before commit, a replaced launch attempt, and ordinary late capture writes", () =>
    Effect.gen(function* () {
      const f = yield* seedNativeWorker();
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const page = f.batch(f.checkpoint, 1, [f.record("B", 1, "B")]);
      const wrong = { ...f.record("A", 1, "A"), provenance: {
        ...f.record("A", 1, "A").provenance, instanceId: "other-worker",
      } };
      const before = yield* f.sink.latestSequence();
      const fault = yield* Effect.exit(f.ingestor.ingestNativeWorker({ ...page, records: [page.records[0]!, wrong] }));
      assert.equal(fault._tag, "Failure");
      assert.equal(yield* f.sink.latestSequence(), before);
      assert.deepEqual((yield* projections.getThreadProjection(f.member.childThreadId!)).turnItems, []);
      assert.isTrue((yield* f.ingestor.ingestNativeWorker(page)).committed);
      const child = yield* projections.getThreadProjection(f.member.childThreadId!);
      const nativeTool = child.turnItems.find((item) => item.type === "dynamic_tool");
      if (nativeTool === undefined) return yield* Effect.die("Missing captured native tool.");
      yield* f.sink.write({ events: [{
        id: yield* f.ids.allocate.event({ threadId: f.member.childThreadId! }),
        type: "provider-thread.updated", threadId: f.member.childThreadId!, occurredAt: f.now,
        payload: { ...child.providerThreads[0]!, nativeMetadata: {}, nativeThreadRef: null },
      }, {
        id: yield* f.ids.allocate.event({ threadId: child.thread.id }),
        type: "thread.model-selection-updated", threadId: child.thread.id, occurredAt: f.now,
        payload: { ...child.thread,
          modelSelection: { ...child.thread.modelSelection, model: "late-ordinary-model" } },
      }, {
        id: yield* f.ids.allocate.event({ threadId: child.thread.id }),
        type: "plan.updated", threadId: child.thread.id, occurredAt: f.now,
        payload: { ...child.plans[0]!, status: "superseded" },
      }, {
        id: yield* f.ids.allocate.event({ threadId: child.thread.id }),
        type: "turn-item.updated", threadId: child.thread.id, occurredAt: f.now,
        payload: { ...nativeTool, providerThreadId: null },
      }] });
      const protectedChild = yield* projections.getThreadProjection(f.member.childThreadId!);
      assert.deepEqual(protectedChild.providerThreads, child.providerThreads);
      assert.deepEqual(protectedChild.thread.modelSelection, child.thread.modelSelection);
      assert.deepEqual(protectedChild.plans, child.plans);
      assert.deepEqual(protectedChild.turnItems, child.turnItems);
      yield* f.sink.write({ events: [{
        id: yield* f.ids.allocate.event({ threadId: f.parent.threadId }),
        type: "run.updated", threadId: f.parent.threadId, occurredAt: f.now,
        payload: { ...f.run, activeAttemptId: RunAttemptId.make("replacement-attempt") },
      }] });
      assert.isFalse((yield* f.ingestor.ingestNativeWorker(
        f.batch(page.checkpoint, 2, [f.record("B", 2, "Rejected")]),
      )).committed);
      assert.deepEqual((yield* projections.getThreadProjection(f.member.childThreadId!)).providerThreads,
        child.providerThreads);
    }),
  );
});

layer("native admission connection fence", (it) => {
  it.effect("admission connection loss rolls back the ordinary tool and shell, then valid retry commits", () =>
    Effect.gen(function* () {
      const f = yield* seedNativeWorker(false);
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const input = {
        providerSessionId: f.launch.providerSessionId, providerInstanceId: f.launch.providerInstanceId,
        threadId: f.parent.threadId, runId: f.run.id,
        event: { type: "turn_item.updated" as const, driver: ProviderDriverKind.make("pi"), turnItem: f.turnItem },
        writeIfRunCurrent: { runId: f.run.id, activeAttemptId: f.launch.runAttemptId, expectedStatus: "running" as const },
      };
      assert.deepEqual(yield* f.ingestor.ingestNormalized({
        ...input, nativeWorkerAdmission: { summary: f.summary, checkpoint: f.checkpoint,
          isConnected: Effect.succeed(false) },
      }), []);
      const rejected = yield* projections.getThreadProjection(f.parent.threadId);
      assert.deepEqual(rejected.turnItems, []);
      assert.deepEqual(rejected.subagents, []);
      yield* f.ingestor.ingestNormalized({
        ...input, nativeWorkerAdmission: { summary: f.summary, checkpoint: f.checkpoint,
          isConnected: Effect.succeed(true) },
      });
      const accepted = yield* projections.getThreadProjection(f.parent.threadId);
      assert.lengthOf(accepted.turnItems, 1);
      assert.lengthOf(accepted.subagents, 1);
      const child = yield* projections.getThreadProjection(accepted.subagents[0]!.childThreadId!);
      assert.equal(child.providerThreads[0]?.nativeThreadRef, null);
      assert.deepEqual(child.runs, []);
    }));
});
it.effect("records accepted billed turn usage once without billing the context window", () => {
  const recorded: Array<Readonly<Record<string, unknown>>> = [];
  const layerAnalytics = Layer.succeed(ProviderEventIngestor.ProviderTurnAnalytics, {
    record: (properties: Readonly<Record<string, unknown>>) =>
      Effect.sync(() => {
        recorded.push(properties);
      }),
  });
  return Effect.gen(function* () {
    const now = yield* DateTime.now;
    const eventSink = yield* EventSink.EventSinkV2;
    const ingestor = yield* ProviderEventIngestor.ProviderEventIngestorV2;
    const idAllocator = yield* IdAllocator.IdAllocatorV2;
    const threadEvent = yield* threadCreatedEvent(now);
    yield* eventSink.write({ events: [threadEvent] });
    const providerSessionId = yield* idAllocator.allocate.providerSession({
      providerInstanceId: modelSelection.instanceId,
      threadId: threadEvent.threadId,
    });
    const providerThreadId = idAllocator.derive.providerThread({
      driver: CODEX_DRIVER,
      nativeThreadId: "billing-thread",
    });
    const providerTurn = {
      id: idAllocator.derive.providerTurn({ driver: CODEX_DRIVER, nativeTurnId: "billing-turn" }),
      providerThreadId,
      nodeId: NodeId.make("node:billing-turn"),
      runAttemptId: null,
      nativeTurnRef: null,
      ordinal: 1,
      status: "completed" as const,
      startedAt: now,
      completedAt: DateTime.makeUnsafe(DateTime.toEpochMillis(now) + 120),
      tokenUsage: {
        inputTokens: 4000,
        cachedInputTokens: 3000,
        outputTokens: 100,
        usedTokens: 4100,
        updatedAt: DateTime.formatIso(now),
      },
      turnTokenUsage: {
        usageStatus: "complete" as const,
        usageScope: "main_agent" as const,
        hasSubagents: false,
        inputTokens: 40,
        cachedInputTokens: 30,
        outputTokens: 10,
      },
    };
    const input = {
      providerSessionId,
      providerInstanceId: modelSelection.instanceId,
      threadId: threadEvent.threadId,
      analyticsContext: {
        modelSelection,
        runtimeMode: "full-access" as const,
        interactionMode: "default" as const,
      },
      event: { type: "provider_turn.updated" as const, driver: CODEX_DRIVER, providerTurn },
    };
    yield* ingestor.ingestNormalized(input);
    yield* ingestor.ingestNormalized(input);
    const ignored = yield* ingestor.ingestNormalized({
      ...input,
      event: {
        ...input.event,
        providerTurn: {
          ...providerTurn,
          id: idAllocator.derive.providerTurn({
            driver: CODEX_DRIVER,
            nativeTurnId: "stale-billing-turn",
          }),
        },
      },
      writeIfRunCurrent: {
        runId: RunId.make("missing-run"),
        activeAttemptId: RunAttemptId.make("stale-attempt"),
        expectedStatus: "running",
      },
    });
    assert.isEmpty(ignored);
    assert.lengthOf(recorded, 1);
    assert.deepEqual(recorded[0], {
      provider: CODEX_DRIVER,
      terminalStatus: "completed",
      usageStatus: "complete",
      usageScope: "main_agent",
      hasSubagents: false,
      inputTokens: 40,
      cachedInputTokens: 30,
      outputTokens: 10,
      model: modelSelection.model,
      mixedModels: false,
      runtimeMode: "full-access",
      interactionMode: "default",
      durationMs: 120,
    });
  }).pipe(Effect.provide(layerTest.pipe(Layer.provide(layerAnalytics))));
});

layer("ProviderEventIngestorV2", (it) => {
  it.effect("normalizes provider events through the real event log and projection store", () =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const eventSink = yield* EventSink.EventSinkV2;
      const eventStore = yield* EventStore.EventStoreV2;
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const ingestor = yield* ProviderEventIngestor.ProviderEventIngestorV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const threadEvent = yield* threadCreatedEvent(now);
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId: threadEvent.threadId,
      });
      const providerThread: OrchestrationV2ProviderThread = {
        id: idAllocator.derive.providerThread({
          driver: CODEX_DRIVER,
          nativeThreadId: "native-thread",
        }),
        driver: CODEX_DRIVER,
        providerInstanceId: modelSelection.instanceId,
        providerSessionId,
        appThreadId: threadEvent.threadId,
        ownerNodeId: null,
        nativeThreadRef: {
          driver: CODEX_DRIVER,
          nativeId: "native-thread",
          strength: "strong",
        },
        nativeConversationHeadRef: null,
        status: "idle",
        firstRunOrdinal: null,
        lastRunOrdinal: null,
        handoffIds: [],
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
      };

      yield* eventSink.write({ events: [threadEvent] });
      const storedEvents = yield* ingestor.ingestNormalized({
        providerSessionId,
        providerInstanceId: modelSelection.instanceId,
        threadId: threadEvent.threadId,
        event: {
          type: "provider_thread.updated",
          driver: CODEX_DRIVER,
          providerThread,
        },
      });

      const projection = yield* projectionStore.getThreadProjection(threadEvent.threadId);
      const storedDomainEvents = yield* eventStore.read({}).pipe(Stream.runCollect);
      const afterFirstEvent = yield* eventStore
        .read({ afterSequence: 1, threadId: threadEvent.threadId })
        .pipe(Stream.runCollect);
      const latestThreadSequence = yield* eventStore.latestSequence({
        threadId: threadEvent.threadId,
      });

      assert.equal(storedEvents.length, 1);
      assert.equal(storedEvents[0]?.event.type, "provider-thread.updated");
      assert.deepEqual(
        projection.providerThreads.map((thread) => thread.id),
        [providerThread.id],
      );
      assert.deepEqual(
        Array.from(storedDomainEvents).map((stored) => stored.event.type),
        ["thread.created", "provider-thread.updated"],
      );
      assert.deepEqual(
        Array.from(storedDomainEvents).map((stored) => stored.sequence),
        [1, 2],
      );
      assert.deepEqual(
        Array.from(afterFirstEvent).map((stored) => stored.event.type),
        ["provider-thread.updated"],
      );
      assert.equal(latestThreadSequence, 2);
    }),
  );

  it.effect("rolls back runs a provider rewound off its active branch", () =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const eventSink = yield* EventSink.EventSinkV2;
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const ingestor = yield* ProviderEventIngestor.ProviderEventIngestorV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const threadEvent = yield* threadCreatedEvent(now);
      const threadId = threadEvent.threadId;
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });
      const rewoundThreadId = idAllocator.derive.providerThread({
        driver: CODEX_DRIVER,
        nativeThreadId: "rewound-thread",
      });
      const otherThreadId = idAllocator.derive.providerThread({
        driver: CODEX_DRIVER,
        nativeThreadId: "other-thread",
      });
      const scopeId = CheckpointScopeId.make("checkpoint-scope:rewound");
      // A run as execution leaves it: attempt, provider turn, root node, and a
      // checkpoint once completed. A null native turn id is a weak ref.
      const seedRun = Effect.fnUntraced(function* (input: {
        readonly ordinal: number;
        readonly providerThreadId: ProviderThreadId;
        readonly status: "completed" | "interrupted" | "running";
        readonly nativeTurnId: string | null;
      }) {
        const runId = RunId.make(`run:rewound:${input.ordinal}`);
        const attemptId = RunAttemptId.make(`attempt:rewound:${input.ordinal}`);
        const nodeId = NodeId.make(`node:rewound:${input.ordinal}`);
        const providerTurnId = ProviderTurnId.make(`provider-turn:rewound:${input.ordinal}`);
        const completedAt = input.status === "running" ? null : now;
        const eventId = () => idAllocator.allocate.event({ threadId });
        const events: Array<OrchestrationV2DomainEvent> = [
          {
            id: yield* eventId(),
            type: "run.created",
            threadId,
            occurredAt: now,
            payload: {
              id: runId,
              threadId,
              ordinal: input.ordinal,
              providerInstanceId: modelSelection.instanceId,
              modelSelection,
              providerThreadId: input.providerThreadId,
              userMessageId: MessageId.make(`message:rewound:${input.ordinal}`),
              rootNodeId: nodeId,
              activeAttemptId: attemptId,
              status: input.status,
              requestedAt: now,
              startedAt: now,
              completedAt,
              checkpointId: null,
              contextHandoffId: null,
            },
          },
          {
            id: yield* eventId(),
            type: "run-attempt.created",
            threadId,
            occurredAt: now,
            payload: {
              id: attemptId,
              runId,
              attemptOrdinal: 1,
              rootNodeId: nodeId,
              providerInstanceId: modelSelection.instanceId,
              providerThreadId: input.providerThreadId,
              providerTurnId,
              reason: "initial",
              status: input.status,
              startedAt: now,
              completedAt,
            },
          },
          {
            id: yield* eventId(),
            type: "provider-turn.updated",
            threadId,
            occurredAt: now,
            payload: {
              id: providerTurnId,
              providerThreadId: input.providerThreadId,
              nodeId,
              runAttemptId: attemptId,
              nativeTurnRef:
                input.nativeTurnId === null
                  ? {
                      driver: CODEX_DRIVER,
                      nativeId: `synthetic:${input.ordinal}`,
                      strength: "weak",
                    }
                  : { driver: CODEX_DRIVER, nativeId: input.nativeTurnId, strength: "strong" },
              ordinal: input.ordinal,
              status: input.status,
              startedAt: now,
              completedAt,
            },
          },
          {
            id: yield* eventId(),
            type: "node.updated",
            threadId,
            occurredAt: now,
            payload: {
              id: nodeId,
              threadId,
              runId,
              parentNodeId: null,
              rootNodeId: nodeId,
              kind: "root_turn",
              status: input.status,
              countsForRun: true,
              providerThreadId: input.providerThreadId,
              providerTurnId,
              nativeItemRef: null,
              runtimeRequestId: null,
              checkpointScopeId: scopeId,
              startedAt: now,
              completedAt,
            },
          },
          ...(input.status === "completed"
            ? [
                {
                  id: yield* eventId(),
                  type: "checkpoint.captured" as const,
                  threadId,
                  occurredAt: now,
                  payload: {
                    id: CheckpointId.make(`checkpoint:rewound:${input.ordinal}`),
                    threadId,
                    scopeId,
                    runId,
                    nodeId,
                    parentCheckpointId: null,
                    ordinalWithinScope: input.ordinal,
                    appRunOrdinal: input.ordinal,
                    ref: CheckpointRef.make(`refs/t3/checkpoints/rewound/${input.ordinal}`),
                    status: "ready" as const,
                    files: [],
                    capturedAt: now,
                  },
                },
              ]
            : []),
        ];
        yield* eventSink.write({ events });
        return runId;
      });

      yield* eventSink.write({ events: [threadEvent] });
      const otherProviderRun = yield* seedRun({
        ordinal: 1,
        providerThreadId: otherThreadId,
        status: "completed",
        nativeTurnId: "other-turn",
      });
      const keptRun = yield* seedRun({
        ordinal: 2,
        providerThreadId: rewoundThreadId,
        status: "completed",
        nativeTurnId: "kept-turn",
      });
      const abandonedRun = yield* seedRun({
        ordinal: 3,
        providerThreadId: rewoundThreadId,
        status: "completed",
        nativeTurnId: "abandoned-turn",
      });
      const unlocatedRun = yield* seedRun({
        ordinal: 4,
        providerThreadId: rewoundThreadId,
        status: "interrupted",
        nativeTurnId: null,
      });
      const rewindingRun = yield* seedRun({
        ordinal: 5,
        providerThreadId: rewoundThreadId,
        status: "running",
        nativeTurnId: null,
      });

      yield* ingestor.ingestNormalized({
        providerSessionId,
        providerInstanceId: modelSelection.instanceId,
        threadId,
        runId: rewindingRun,
        event: {
          type: "provider_thread.updated",
          driver: CODEX_DRIVER,
          providerThread: {
            id: rewoundThreadId,
            driver: CODEX_DRIVER,
            providerInstanceId: modelSelection.instanceId,
            providerSessionId,
            appThreadId: threadId,
            ownerNodeId: null,
            nativeThreadRef: {
              driver: CODEX_DRIVER,
              nativeId: "rewound-thread",
              strength: "strong",
            },
            nativeConversationHeadRef: {
              driver: CODEX_DRIVER,
              nativeId: "kept-turn-reply",
              strength: "strong",
            },
            status: "idle",
            firstRunOrdinal: 2,
            lastRunOrdinal: 5,
            handoffIds: [],
            forkedFrom: null,
            createdAt: now,
            updatedAt: now,
          },
          retainedNativeTurnIds: ["turn-started-outside-t3", "kept-turn"],
        },
      });

      const projection = yield* projectionStore.getThreadProjection(threadId);
      const runIds = [otherProviderRun, keptRun, abandonedRun, unlocatedRun, rewindingRun];
      assert.deepEqual(
        runIds.map((runId) => projection.runs.find((run) => run.id === runId)?.status),
        ["completed", "completed", "rolled_back", "rolled_back", "running"],
      );
      assert.deepEqual(
        runIds.map((runId) => projection.nodes.find((node) => node.runId === runId)?.status),
        ["completed", "completed", "rolled_back", "rolled_back", "running"],
      );
      assert.deepEqual(
        [otherProviderRun, keptRun, abandonedRun].map(
          (runId) =>
            projection.checkpoints.find((checkpoint) => checkpoint.runId === runId)?.status,
        ),
        ["ready", "ready", "stale"],
      );
      assert.equal(
        projection.providerThreads.find((thread) => thread.id === rewoundThreadId)?.lastRunOrdinal,
        5,
      );
    }),
  );

  it.effect("carries plan-step durations through consecutive and restarted ingestion", () =>
    Effect.gen(function* () {
      yield* TestClock.setTime(Date.parse("2026-09-07T00:00:00.000Z"));
      const now = yield* DateTime.now;
      const eventSink = yield* EventSink.EventSinkV2;
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const ingestor = yield* ProviderEventIngestor.ProviderEventIngestorV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const threadEvent = yield* threadCreatedEvent(now);
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId: threadEvent.threadId,
      });
      const planId = PlanId.make("plan:provider-event-duration");
      const nodeId = NodeId.make("node:provider-event-duration");
      type TodoListPlan = Extract<OrchestrationV2PlanArtifact, { readonly kind: "todo_list" }>;
      const plan = (steps: TodoListPlan["steps"]): TodoListPlan => ({
        id: planId,
        threadId: threadEvent.threadId,
        runId: null,
        nodeId,
        kind: "todo_list",
        status: "active",
        steps,
      });
      const ingest = (
        service: ProviderEventIngestor.ProviderEventIngestorV2["Service"],
        steps: TodoListPlan["steps"],
      ) =>
        service.ingestNormalized({
          providerSessionId,
          providerInstanceId: modelSelection.instanceId,
          threadId: threadEvent.threadId,
          event: { type: "plan.updated", driver: CODEX_DRIVER, plan: plan(steps) },
        });

      yield* eventSink.write({ events: [threadEvent] });
      yield* ingest(ingestor, [
        { id: "duplicate-a", text: "Verify", status: "running" },
        { id: "duplicate-b", text: "Verify", status: "pending" },
        { id: "fallback", text: "Report", status: "pending" },
      ]);
      yield* TestClock.adjust("3 seconds");
      yield* ingest(ingestor, [
        { id: "duplicate-a", text: "Verify", status: "completed" },
        { id: "duplicate-b", text: "Verify", status: "pending" },
        { id: "fallback", text: "Report", status: "pending" },
      ]);

      const restartedIngestor = yield* ProviderEventIngestor.ProviderEventIngestorV2.pipe(
        Effect.provide(
          Layer.fresh(ProviderEventIngestor.layer).pipe(
            Layer.provide(
              Layer.succeed(ProjectionStore.ProjectionStoreV2, {
                ...projectionStore,
                getThreadProjection: () => Effect.die("Plan timing must not load thread history"),
              }),
            ),
          ),
        ),
      );
      yield* TestClock.adjust("4 seconds");
      yield* ingest(restartedIngestor, [
        { id: "duplicate-a", text: "Verify", status: "completed" },
        { id: "duplicate-b", text: "Verify", status: "completed" },
        { id: "fallback", text: "Report", status: "pending" },
      ]);
      yield* TestClock.adjust("5 seconds");
      yield* ingest(restartedIngestor, [
        { id: "duplicate-a", text: "Verify", status: "completed" },
        { id: "duplicate-b", text: "Verify", status: "completed" },
        { id: "fallback", text: "Report", status: "completed" },
      ]);

      const projection = yield* projectionStore.getThreadProjection(threadEvent.threadId);
      const persisted = projection.plans.find(
        (candidate): candidate is TodoListPlan =>
          candidate.kind === "todo_list" && candidate.id === planId,
      );
      assert.deepEqual(
        persisted?.steps.map(({ id, text, status, durationMs }) => ({
          id,
          text,
          status,
          durationMs,
        })),
        [
          { id: "duplicate-a", text: "Verify", status: "completed", durationMs: 3_000 },
          { id: "duplicate-b", text: "Verify", status: "completed", durationMs: 4_000 },
          { id: "fallback", text: "Report", status: "completed", durationMs: 5_000 },
        ],
      );

      yield* ingest(restartedIngestor, [
        { id: "duplicate-a", text: "Inserted task", status: "running" },
        { id: "duplicate-b", text: "Verify", status: "completed" },
        { id: "fallback", text: "Different completed task", status: "completed" },
      ]);
      yield* TestClock.adjust("2 seconds");
      yield* ingest(restartedIngestor, [
        { id: "duplicate-a", text: "Inserted task", status: "completed" },
        { id: "duplicate-b", text: "Verify", status: "completed" },
        { id: "fallback", text: "Different completed task", status: "completed" },
      ]);
      const updated = yield* projectionStore.getThreadProjection(threadEvent.threadId);
      const changedPlan = updated.plans.find(
        (candidate): candidate is TodoListPlan =>
          candidate.kind === "todo_list" && candidate.id === planId,
      );
      assert.deepEqual(
        changedPlan?.steps.map((step) => step.durationMs),
        [2_000, 4_000, undefined],
      );
    }),
  );

  it.effect(
    "treats successful provider terminal markers as non-persisted orchestration control signals",
    () =>
      Effect.gen(function* () {
        const ingestor = yield* ProviderEventIngestor.ProviderEventIngestorV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const projectId = yield* idAllocator.allocate.project({
          fixtureName: "provider-event-terminal",
        });
        const threadId = yield* idAllocator.allocate.thread({
          fixtureName: "provider-event-terminal",
          projectId,
        });
        const providerSessionId = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });
        const normalized = yield* ingestor.normalize({
          providerSessionId,
          providerInstanceId: modelSelection.instanceId,
          threadId,
          event: {
            type: "turn.terminal",
            driver: CODEX_DRIVER,
            providerThreadId: idAllocator.derive.providerThread({
              driver: CODEX_DRIVER,
              nativeThreadId: "native-thread",
            }),
            providerTurnId: idAllocator.derive.providerTurn({
              driver: CODEX_DRIVER,
              nativeTurnId: "native-turn",
            }),
            runOrdinal: 1,
            status: "completed",
            failure: null,
            threadDisposition: "reusable",
          },
        });

        assert.deepEqual(normalized, []);
      }),
  );

  it.effect("persists an interrupted run's inherited terminal through the live run router", () =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const eventSink = yield* EventSink.EventSinkV2;
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const ingestor = yield* ProviderEventIngestor.ProviderEventIngestorV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const threadEvent = yield* threadCreatedEvent(now);
      const priorRunId = RunId.make("run:provider-event-inherited:prior");
      const currentRunId = RunId.make("run:provider-event-inherited:current");
      const itemId = TurnItemId.make("turn-item:provider-event-inherited");
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId: threadEvent.threadId,
      });
      const providerThreadId = idAllocator.derive.providerThread({
        driver: CODEX_DRIVER,
        nativeThreadId: "native-thread-inherited",
      });
      const providerTurnId = idAllocator.derive.providerTurn({
        driver: CODEX_DRIVER,
        nativeTurnId: "native-turn-inherited",
      });
      const runningItem = {
        id: itemId,
        threadId: threadEvent.threadId,
        runId: priorRunId,
        nodeId: NodeId.make("node:provider-event-inherited"),
        providerThreadId,
        providerTurnId,
        nativeItemRef: null,
        parentItemId: null,
        ordinal: 101,
        status: "running",
        title: "Inherited background command",
        startedAt: now,
        completedAt: null,
        updatedAt: now,
        type: "command_execution",
        input: "sleep 60",
      } satisfies OrchestrationV2TurnItem;
      const terminalItem = {
        ...runningItem,
        status: "completed" as const,
        completedAt: now,
        updatedAt: now,
      };

      yield* eventSink.write({ events: [threadEvent] });
      yield* ingestor.ingestNormalized({
        providerSessionId,
        providerInstanceId: modelSelection.instanceId,
        threadId: threadEvent.threadId,
        runId: priorRunId,
        event: { type: "turn_item.updated", driver: CODEX_DRIVER, turnItem: runningItem },
      });

      const identity: ProviderEventRouteIdentity = {
        threadId: threadEvent.threadId,
        runId: currentRunId,
        attemptId: RunAttemptId.make("attempt:provider-event-inherited:current"),
        providerThreadId,
      };
      const inheritedBackgroundTurnItems = selectInheritedBackgroundTurnItems({
        threadId: threadEvent.threadId,
        currentProviderThreadId: providerThreadId,
        currentRunOrdinal: 2,
        runs: [
          {
            id: priorRunId,
            threadId: threadEvent.threadId,
            ordinal: 1,
            status: "interrupted",
          } as OrchestrationV2Run,
          {
            id: currentRunId,
            threadId: threadEvent.threadId,
            ordinal: 2,
            status: "running",
          } as OrchestrationV2Run,
        ],
        turnItems: [runningItem],
      });
      const routeState = makeProviderEventRoutingState({
        identity,
        inheritedBackgroundTurnItems,
        providerTurnId: null,
      });
      const terminalEvent = {
        type: "turn_item.updated",
        driver: CODEX_DRIVER,
        turnItem: terminalItem,
      } as const;
      const [accepted] = routeProviderEvent(terminalEvent, identity, routeState);
      assert.isTrue(accepted);

      const stored = yield* ingestor.ingestNormalized({
        providerSessionId,
        providerInstanceId: modelSelection.instanceId,
        threadId: threadEvent.threadId,
        runId: currentRunId,
        event: terminalEvent,
      });
      const projection = yield* projectionStore.getThreadProjection(threadEvent.threadId);
      const persisted = projection.turnItems.find((item) => item.id === itemId);

      assert.equal(stored.length, 1);
      assert.equal(stored[0]?.event.type, "turn-item.updated");
      assert.equal(persisted?.runId, priorRunId);
      assert.equal(persisted?.threadId, threadEvent.threadId);
      assert.equal(persisted?.status, "completed");
    }),
  );

  it.effect("persists a completed run's late background terminal exactly once", () =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const eventSink = yield* EventSink.EventSinkV2;
      const eventStore = yield* EventStore.EventStoreV2;
      const ingestor = yield* ProviderEventIngestor.ProviderEventIngestorV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const threadEvent = yield* threadCreatedEvent(now);
      const priorRunId = RunId.make("run:provider-event-completed:prior");
      const currentRunId = RunId.make("run:provider-event-completed:current");
      const itemId = TurnItemId.make("turn-item:provider-event-completed");
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId: threadEvent.threadId,
      });
      const providerThreadId = idAllocator.derive.providerThread({
        driver: CODEX_DRIVER,
        nativeThreadId: "native-thread-completed",
      });
      const providerTurnId = idAllocator.derive.providerTurn({
        driver: CODEX_DRIVER,
        nativeTurnId: "native-turn-completed",
      });
      const runningItem = {
        id: itemId,
        threadId: threadEvent.threadId,
        runId: priorRunId,
        nodeId: NodeId.make("node:provider-event-completed"),
        providerThreadId,
        providerTurnId,
        nativeItemRef: null,
        parentItemId: null,
        ordinal: 101,
        status: "running",
        title: "Completed run background command",
        startedAt: now,
        completedAt: null,
        updatedAt: now,
        type: "command_execution",
        input: "sleep 60",
      } satisfies OrchestrationV2TurnItem;
      const terminalEvent = {
        type: "turn_item.updated",
        driver: CODEX_DRIVER,
        turnItem: {
          ...runningItem,
          status: "completed" as const,
          completedAt: now,
          updatedAt: now,
        },
      } as const;

      yield* eventSink.write({ events: [threadEvent] });
      yield* ingestor.ingestNormalized({
        providerSessionId,
        providerInstanceId: modelSelection.instanceId,
        threadId: threadEvent.threadId,
        runId: priorRunId,
        event: { type: "turn_item.updated", driver: CODEX_DRIVER, turnItem: runningItem },
      });

      const priorIdentity: ProviderEventRouteIdentity = {
        threadId: threadEvent.threadId,
        runId: priorRunId,
        attemptId: RunAttemptId.make("attempt:provider-event-completed:prior"),
        providerThreadId,
      };
      const currentIdentity: ProviderEventRouteIdentity = {
        threadId: threadEvent.threadId,
        runId: currentRunId,
        attemptId: RunAttemptId.make("attempt:provider-event-completed:current"),
        providerThreadId,
      };
      const inheritedBackgroundTurnItems = selectInheritedBackgroundTurnItems({
        threadId: threadEvent.threadId,
        currentProviderThreadId: providerThreadId,
        currentRunOrdinal: 2,
        runs: [
          {
            id: priorRunId,
            threadId: threadEvent.threadId,
            ordinal: 1,
            status: "completed",
          } as OrchestrationV2Run,
          {
            id: currentRunId,
            threadId: threadEvent.threadId,
            ordinal: 2,
            status: "running",
          } as OrchestrationV2Run,
        ],
        turnItems: [runningItem],
      });
      const routers = [
        {
          identity: priorIdentity,
          state: makeProviderEventRoutingState({
            identity: priorIdentity,
            providerTurnId: providerTurnId,
          }),
        },
        {
          identity: currentIdentity,
          state: makeProviderEventRoutingState({
            identity: currentIdentity,
            inheritedBackgroundTurnItems,
            providerTurnId: null,
          }),
        },
      ];
      const acceptedRouters = routers.filter(
        ({ identity, state }) => routeProviderEvent(terminalEvent, identity, state)[0],
      );

      yield* Effect.forEach(
        acceptedRouters,
        ({ identity }) =>
          ingestor.ingestNormalized({
            providerSessionId,
            providerInstanceId: modelSelection.instanceId,
            threadId: threadEvent.threadId,
            runId: identity.runId,
            event: terminalEvent,
          }),
        { concurrency: 1 },
      );

      const storedEvents = yield* eventStore
        .read({ threadId: threadEvent.threadId })
        .pipe(Stream.runCollect);
      const storedTerminals = Array.from(storedEvents).filter(
        (stored) =>
          stored.event.type === "turn-item.updated" &&
          stored.event.payload.id === itemId &&
          stored.event.payload.status === "completed",
      );

      assert.equal(storedTerminals.length, 1);
      assert.equal(acceptedRouters.length, 1);
      assert.equal(acceptedRouters[0]?.identity.runId, priorRunId);
    }),
  );

  it.effect.each(["completed", "interrupted", "failed", "cancelled", "control"] as const)(
    "dismisses only native questions when a provider turn ends with %s",
    (terminal) =>
      Effect.gen(function* () {
        const now = yield* DateTime.now;
        const eventSink = yield* EventSink.EventSinkV2;
        const eventStore = yield* EventStore.EventStoreV2;
        const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
        const ingestor = yield* ProviderEventIngestor.ProviderEventIngestorV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const threadEvent = yield* threadCreatedEvent(now);
        const threadId = threadEvent.threadId;
        const providerSessionId = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });
        const providerThreadId = idAllocator.derive.providerThread({
          driver: CODEX_DRIVER,
          nativeThreadId: `${threadId}:questions`,
        });
        const providerTurnId = idAllocator.derive.providerTurn({
          driver: CODEX_DRIVER,
          nativeTurnId: `${threadId}:questions`,
        });
        const otherTurnId = idAllocator.derive.providerTurn({
          driver: CODEX_DRIVER,
          nativeTurnId: `${threadId}:other-turn`,
        });
        const specs = [
          { key: "native", responseCapability: { type: "live", providerSessionId } },
          {
            key: "unavailable",
            responseCapability: { type: "not_resumable", reason: "Turn ended" },
          },
          { key: "message", responseCapability: { type: "message" } },
          { key: "answered", responseCapability: { type: "live", providerSessionId } },
          { key: "other-turn", responseCapability: { type: "live", providerSessionId } },
          { key: "approval", responseCapability: { type: "live", providerSessionId } },
        ] as const;
        const fixtures = specs.map((spec, ordinal) => {
          const nodeId = NodeId.make(`${threadId}:${spec.key}`);
          const resolved = spec.key === "answered";
          const request: OrchestrationV2RuntimeRequest = {
            id: RuntimeRequestId.make(`${threadId}:${spec.key}`),
            nodeId,
            providerTurnId: spec.key === "other-turn" ? otherTurnId : providerTurnId,
            nativeRequestRef: null,
            kind: spec.key === "approval" ? "command" : "user_input",
            status: resolved ? "resolved" : "pending",
            responseCapability: spec.responseCapability,
            createdAt: now,
            resolvedAt: resolved ? now : null,
          };
          const node: OrchestrationV2ExecutionNode = {
            id: nodeId,
            threadId,
            runId: null,
            parentNodeId: null,
            rootNodeId: nodeId,
            kind: spec.key === "approval" ? "approval_request" : "user_input_request",
            status: resolved ? "completed" : "waiting",
            countsForRun: false,
            providerThreadId,
            providerTurnId: request.providerTurnId,
            nativeItemRef: null,
            runtimeRequestId: request.id,
            checkpointScopeId: null,
            startedAt: now,
            completedAt: resolved ? now : null,
          };
          const item: OrchestrationV2TurnItem = {
            id: TurnItemId.make(`${threadId}:${spec.key}`),
            threadId,
            runId: null,
            nodeId,
            providerThreadId,
            providerTurnId: request.providerTurnId,
            nativeItemRef: null,
            parentItemId: null,
            ordinal,
            status: resolved ? "completed" : "waiting",
            title: "Which option?",
            startedAt: now,
            completedAt: resolved ? now : null,
            updatedAt: now,
            ...(spec.key === "approval"
              ? { type: "approval_request", requestId: request.id, requestKind: "command" }
              : {
                  type: "user_input_request",
                  requestId: request.id,
                  questions: [],
                  ...(spec.key === "message" ? { responseMode: "message" as const } : {}),
                }),
          };
          return { key: spec.key, request, node, item };
        });
        const seedEvents: Array<OrchestrationV2DomainEvent> = [threadEvent];
        for (const fixture of fixtures) {
          for (const payload of [
            { type: "runtime-request.updated" as const, payload: fixture.request },
            { type: "node.updated" as const, payload: fixture.node },
            { type: "turn-item.updated" as const, payload: fixture.item },
          ]) {
            seedEvents.push({
              id: yield* idAllocator.allocate.event({ threadId }),
              threadId,
              occurredAt: now,
              ...payload,
            });
          }
        }
        yield* eventSink.write({ events: seedEvents });
        const input = {
          providerSessionId,
          providerInstanceId: modelSelection.instanceId,
          threadId,
          event:
            terminal === "control"
              ? {
                  type: "turn.terminal" as const,
                  driver: CODEX_DRIVER,
                  providerThreadId,
                  providerTurnId,
                  runOrdinal: 1,
                  status: "completed" as const,
                  failure: null,
                  threadDisposition: "reusable" as const,
                }
              : {
                  type: "provider_turn.updated" as const,
                  driver: CODEX_DRIVER,
                  providerTurn: {
                    id: providerTurnId,
                    providerThreadId,
                    nodeId: NodeId.make(`${threadId}:root`),
                    runAttemptId: null,
                    nativeTurnRef: null,
                    ordinal: 1,
                    status: terminal,
                    startedAt: now,
                    completedAt: now,
                  },
                },
        };
        const stored = yield* ingestor.ingestNormalized(input);
        const projection = yield* projectionStore.getThreadProjection(threadId);
        for (const fixture of fixtures) {
          const closed = fixture.key === "native" || fixture.key === "unavailable";
          const request = projection.runtimeRequests.find(
            (item) => item.id === fixture.request.id,
          )!;
          const node = projection.nodes.find((item) => item.id === fixture.node.id)!;
          const item = projection.turnItems.find((item) => item.id === fixture.item.id)!;
          if (closed) {
            assert.equal(request.status, "cancelled");
            assert.isNotNull(request.resolvedAt);
            assert.equal(node.status, "cancelled");
            assert.isNotNull(node.completedAt);
            assert.equal(item.status, "cancelled");
            assert.isNotNull(item.completedAt);
          } else {
            assert.equal(request.status, fixture.request.status);
            assert.equal(node.status, fixture.node.status);
            assert.equal(item.status, fixture.item.status);
          }
        }
        assert.equal(
          stored.filter((entry) => entry.event.type === "runtime-request.updated").length,
          2,
        );
        assert.isEmpty(
          (yield* projectionStore.getPendingNativeUserInputs(threadId, providerTurnId))
            .runtimeRequests,
        );
        const replayed = yield* eventStore.read({ threadId }).pipe(Stream.runCollect);
        assert.equal(
          replayed.filter(
            (entry) =>
              entry.event.type === "runtime-request.updated" &&
              entry.event.payload.status === "cancelled",
          ).length,
          2,
        );
        const repeated = yield* ingestor.ingestNormalized(input);
        assert.isFalse(repeated.some((entry) => entry.event.type === "runtime-request.updated"));
      }),
  );

  it.effect(
    "preserves an answer committed after terminal normalization reads a pending question",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const eventSink = yield* EventSink.EventSinkV2;
          const eventStore = yield* EventStore.EventStoreV2;
          const projections = yield* ProjectionStore.ProjectionStoreV2;
          const idAllocator = yield* IdAllocator.IdAllocatorV2;
          const now = yield* DateTime.now;
          const threadEvent = yield* threadCreatedEvent(now);
          const threadId = threadEvent.threadId;
          const providerSessionId = yield* idAllocator.allocate.providerSession({
            providerInstanceId: modelSelection.instanceId,
            threadId,
          });
          const providerThreadId = idAllocator.derive.providerThread({
            driver: CODEX_DRIVER,
            nativeThreadId: `${threadId}:race`,
          });
          const providerTurnId = idAllocator.derive.providerTurn({
            driver: CODEX_DRIVER,
            nativeTurnId: `${threadId}:race`,
          });
          const nodeId = NodeId.make(`${threadId}:question`);
          const request: OrchestrationV2RuntimeRequest = {
            id: RuntimeRequestId.make(`${threadId}:question`),
            nodeId,
            providerTurnId,
            nativeRequestRef: null,
            kind: "user_input",
            status: "pending",
            responseCapability: { type: "live", providerSessionId },
            createdAt: now,
            resolvedAt: null,
          };
          const node: OrchestrationV2ExecutionNode = {
            id: nodeId,
            threadId,
            runId: null,
            parentNodeId: null,
            rootNodeId: nodeId,
            kind: "user_input_request",
            status: "waiting",
            countsForRun: false,
            providerThreadId,
            providerTurnId,
            nativeItemRef: null,
            runtimeRequestId: request.id,
            checkpointScopeId: null,
            startedAt: now,
            completedAt: null,
          };
          const item: OrchestrationV2TurnItem = {
            id: TurnItemId.make(`${threadId}:question`),
            threadId,
            runId: null,
            nodeId,
            providerThreadId,
            providerTurnId,
            nativeItemRef: null,
            parentItemId: null,
            ordinal: 1,
            status: "waiting",
            title: "Which option?",
            startedAt: now,
            completedAt: null,
            updatedAt: now,
            type: "user_input_request",
            requestId: request.id,
            questions: [],
          };
          const seedEvents: Array<OrchestrationV2DomainEvent> = [threadEvent];
          for (const payload of [
            { type: "runtime-request.updated" as const, payload: request },
            { type: "node.updated" as const, payload: node },
            { type: "turn-item.updated" as const, payload: item },
          ])
            seedEvents.push({
              id: yield* idAllocator.allocate.event({ threadId }),
              threadId,
              occurredAt: now,
              ...payload,
            });
          yield* eventSink.write({ events: seedEvents });
          const normalized = yield* Deferred.make<void>();
          const releaseTerminalWrite = yield* Deferred.make<void>();
          const gatedSink = EventSink.EventSinkV2.of({
            ...eventSink,
            write: (input) =>
              Deferred.succeed(normalized, undefined).pipe(
                Effect.andThen(Deferred.await(releaseTerminalWrite)),
                Effect.andThen(eventSink.write(input)),
              ),
          });
          const ingestor = yield* ProviderEventIngestor.ProviderEventIngestorV2.pipe(
            Effect.provide(Layer.fresh(ProviderEventIngestor.layer)),
            Effect.provideService(EventSink.EventSinkV2, gatedSink),
          );
          const terminal = yield* ingestor
            .ingestNormalized({
              providerSessionId,
              providerInstanceId: modelSelection.instanceId,
              threadId,
              event: {
                type: "provider_turn.updated",
                driver: CODEX_DRIVER,
                providerTurn: {
                  id: providerTurnId,
                  providerThreadId,
                  nodeId,
                  runAttemptId: null,
                  nativeTurnRef: null,
                  ordinal: 1,
                  status: "completed",
                  startedAt: now,
                  completedAt: now,
                },
              },
            })
            .pipe(Effect.forkScoped);
          yield* Deferred.await(normalized);
          const answers = { decision: "Use the existing workspace" };
          const responseEvents: Array<OrchestrationV2DomainEvent> = [];
          for (const payload of [
            {
              type: "runtime-request.updated" as const,
              payload: { ...request, status: "resolved" as const, answers, resolvedAt: now },
            },
            {
              type: "node.updated" as const,
              payload: { ...node, status: "completed" as const, completedAt: now },
            },
            {
              type: "turn-item.updated" as const,
              payload: { ...item, status: "completed" as const, completedAt: now },
            },
          ])
            responseEvents.push({
              id: yield* idAllocator.allocate.event({ threadId }),
              threadId,
              occurredAt: now,
              ...payload,
            });
          yield* eventSink.write({ events: responseEvents });
          yield* Deferred.succeed(releaseTerminalWrite, undefined);
          const committedTerminal = yield* Fiber.join(terminal);
          assert.deepEqual(
            committedTerminal.map((stored) => stored.event.type),
            ["provider-turn.updated"],
          );
          const projection = yield* projections.getThreadProjection(threadId);
          const answered = projection.runtimeRequests.find((entry) => entry.id === request.id)!;
          assert.equal(answered.status, "resolved");
          assert.deepEqual(answered.answers, answers);
          assert.equal(projection.nodes.find((entry) => entry.id === nodeId)!.status, "completed");
          assert.equal(
            projection.turnItems.find((entry) => entry.id === item.id)!.status,
            "completed",
          );
          const history = yield* eventStore.read({ threadId }).pipe(Stream.runCollect);
          assert.isFalse(
            history.some(
              (stored) =>
                stored.event.type === "runtime-request.updated" &&
                stored.event.payload.status === "cancelled",
            ),
          );
        }),
      ),
  );

  it.effect("persists a failed provider terminal as one expected error item", () =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const retryStartedAt = DateTime.makeUnsafe(DateTime.toEpochMillis(now) - 5_000);
      const eventSink = yield* EventSink.EventSinkV2;
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const ingestor = yield* ProviderEventIngestor.ProviderEventIngestorV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const threadEvent = yield* threadCreatedEvent(now);
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId: threadEvent.threadId,
      });
      const providerThreadId = idAllocator.derive.providerThread({
        driver: CODEX_DRIVER,
        nativeThreadId: "native-thread-failed",
      });
      const providerTurnId = idAllocator.derive.providerTurn({
        driver: CODEX_DRIVER,
        nativeTurnId: "native-turn-failed",
      });

      yield* eventSink.write({ events: [threadEvent] });
      const stored = yield* ingestor.ingestNormalized({
        providerSessionId,
        providerInstanceId: modelSelection.instanceId,
        threadId: threadEvent.threadId,
        event: {
          type: "turn.terminal",
          driver: CODEX_DRIVER,
          providerThreadId,
          providerTurnId,
          runOrdinal: 1,
          failureItemOrdinal: 102,
          status: "failed",
          failure: makeProviderFailure({
            message: "Invalid reasoning effort.",
            code: "invalid_request",
            class: "validation_error",
          }),
          retry: {
            attempt: 3,
            maxAttempts: 3,
            retryDelayMs: 2_000,
          },
          retryStartedAt,
          threadDisposition: "reusable",
        },
      });

      const projection = yield* projectionStore.getThreadProjection(threadEvent.threadId);
      const errorItems = projection.visibleTurnItems.filter(
        (candidate) => candidate.item.type === "error",
      );

      assert.equal(stored.length, 1);
      assert.equal(stored[0]?.event.type, "turn-item.updated");
      assert.equal(errorItems.length, 1);
      const errorItem = errorItems[0]?.item;
      assert.equal(errorItem?.type, "error");
      if (errorItem?.type !== "error") return;
      assert.equal(errorItem.failure.message, "Invalid reasoning effort.");
      assert.equal(errorItem.failure.code, "invalid_request");
      assert.deepEqual(errorItem.retry, {
        attempt: 3,
        maxAttempts: 3,
        retryDelayMs: 2_000,
      });
      const errorStartedAt = errorItem.startedAt;
      assert.ok(errorStartedAt);
      assert.equal(DateTime.toEpochMillis(errorStartedAt), DateTime.toEpochMillis(retryStartedAt));
      assert.equal(errorItem.providerThreadId, providerThreadId);
      assert.equal(errorItem.providerTurnId, providerTurnId);
    }),
  );

  it.effect("stores tool image bytes only where a tool-output-image asset serves them", () =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const eventSink = yield* EventSink.EventSinkV2;
      const eventStore = yield* EventStore.EventStoreV2;
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const ingestor = yield* ProviderEventIngestor.ProviderEventIngestorV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const threadEvent = yield* threadCreatedEvent(now);
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId: threadEvent.threadId,
      });
      const readBase64 = Buffer.alloc(30_000, 7).toString("base64");
      const screenshotBase64 = Buffer.alloc(20_000, 9).toString("base64");
      const toolItem = (
        id: string,
        ordinal: number,
        toolName: string,
        output: unknown,
      ): OrchestrationV2TurnItem => ({
        id: TurnItemId.make(id),
        threadId: threadEvent.threadId,
        runId: null,
        nodeId: null,
        providerThreadId: null,
        providerTurnId: null,
        nativeItemRef: null,
        parentItemId: null,
        ordinal,
        status: "completed",
        title: toolName,
        startedAt: now,
        completedAt: now,
        updatedAt: now,
        type: "dynamic_tool",
        toolName,
        input: {},
        output,
      });
      const read = toolItem("turn-item:read-image", 1, "Read", {
        type: "image",
        file: { base64: readBase64, type: "image/png", originalSize: 30_000 },
      });
      const screenshot = toolItem("turn-item:screenshot", 2, "mcp__t3-code__device_screenshot", {
        content: [
          {
            type: "image",
            source: { type: "base64", media_type: "image/png", data: screenshotBase64 },
          },
        ],
      });

      yield* eventSink.write({ events: [threadEvent] });
      for (const turnItem of [read, screenshot]) {
        yield* ingestor.ingestNormalized({
          providerSessionId,
          providerInstanceId: modelSelection.instanceId,
          threadId: threadEvent.threadId,
          event: { type: "turn_item.updated", driver: CODEX_DRIVER, turnItem },
        });
      }

      const storedEvents = yield* eventStore
        .read({ threadId: threadEvent.threadId, eventType: "turn-item.updated" })
        .pipe(Stream.runCollect);
      const storedJson = JSON.stringify(Array.from(storedEvents, (stored) => stored.event));
      const projectedRead = yield* projectionStore.getTurnItem({
        threadId: threadEvent.threadId,
        itemId: read.id,
      });
      const projectedScreenshot = yield* projectionStore.getTurnItem({
        threadId: threadEvent.threadId,
        itemId: screenshot.id,
      });

      assert.equal(storedJson.includes(readBase64), false);
      assert.equal(storedJson.includes(screenshotBase64), true);
      assert.deepEqual(projectedRead?.type === "dynamic_tool" ? projectedRead.output : null, {
        type: "image",
        file: { type: "image/png", originalSize: 30_000, sizeBytes: 30_000 },
      });
      assert.deepEqual(
        toolOutputImages(
          projectedScreenshot?.type === "dynamic_tool" ? projectedScreenshot.output : null,
        ),
        [{ mimeType: "image/png", data: screenshotBase64 }],
      );
    }),
  );

  it.effect("routes provider-owned child artifacts to their child app thread", () =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const ingestor = yield* ProviderEventIngestor.ProviderEventIngestorV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const rootEvent = yield* threadCreatedEvent(now);
      if (rootEvent.type !== "thread.created") {
        throw new Error("Expected a thread.created fixture event");
      }
      const childThreadId = idAllocator.derive.threadFromProviderThread({
        driver: CODEX_DRIVER,
        nativeThreadId: "native-subagent-thread",
      });
      const childRootNodeId = NodeId.make("node:subagent-root");
      const childThread: OrchestrationV2AppThread = {
        ...rootEvent.payload,
        id: childThreadId,
        title: "inspect package",
        activeProviderThreadId: null,
        lineage: {
          parentThreadId: rootEvent.threadId,
          relationshipToParent: "subagent",
          rootThreadId: rootEvent.threadId,
        },
        forkedFrom: {
          type: "node",
          nodeId: NodeId.make("node:parent-subagent"),
        },
      };
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId: rootEvent.threadId,
      });

      const threadEvents = yield* ingestor.normalize({
        providerSessionId,
        providerInstanceId: modelSelection.instanceId,
        threadId: rootEvent.threadId,
        event: {
          type: "app_thread.created",
          driver: CODEX_DRIVER,
          appThread: childThread,
        },
      });
      const messageEvents = yield* ingestor.normalize({
        providerSessionId,
        providerInstanceId: modelSelection.instanceId,
        threadId: rootEvent.threadId,
        event: {
          type: "message.updated",
          driver: CODEX_DRIVER,
          message: {
            createdBy: "agent",
            creationSource: "provider",
            id: MessageId.make("message:subagent-response"),
            threadId: childThreadId,
            runId: null,
            nodeId: childRootNodeId,
            role: "assistant",
            text: "Subagent result",
            attachments: [],
            streaming: false,
            createdAt: now,
            updatedAt: now,
          },
        },
      });

      assert.equal(threadEvents[0]?.type, "thread.created");
      assert.equal(threadEvents[0]?.threadId, childThreadId);
      assert.equal(messageEvents[0]?.type, "message.updated");
      assert.equal(messageEvents[0]?.threadId, childThreadId);
    }),
  );

  it.effect("updates provider child metadata without recreating its saved thread", () =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const eventSink = yield* EventSink.EventSinkV2;
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const ingestor = yield* ProviderEventIngestor.ProviderEventIngestorV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const rootEvent = yield* threadCreatedEvent(now);
      if (rootEvent.type !== "thread.created") throw new Error("Expected thread fixture");
      const childThreadId = idAllocator.derive.threadFromProviderThread({
        driver: CODEX_DRIVER,
        nativeThreadId: "workflow-member-metadata",
      });
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId: rootEvent.threadId,
      });
      const offered = {
        ...rootEvent.payload,
        id: childThreadId,
        title: "Inspect checkout",
        activeProviderThreadId: null,
        lineage: {
          parentThreadId: rootEvent.threadId,
          relationshipToParent: "subagent" as const,
          rootThreadId: rootEvent.threadId,
        },
      };
      const ingest = (appThread: OrchestrationV2AppThread) =>
        ingestor.ingestNormalized({
          providerSessionId,
          providerInstanceId: modelSelection.instanceId,
          threadId: rootEvent.threadId,
          event: { type: "app_thread.created", driver: CODEX_DRIVER, appThread },
        });
      yield* eventSink.write({ events: [rootEvent] });
      const created = yield* Effect.all([ingest(offered), ingest(offered)], {
        concurrency: "unbounded",
      });
      assert.deepEqual(
        created.flat().map((stored) => stored.event.type),
        ["thread.created"],
      );
      const saved = {
        ...offered,
        archivedAt: now,
        deletedAt: now,
        activeProviderThreadId: rootEvent.payload.activeProviderThreadId,
        branch: "user/checkout",
      };
      yield* eventSink.write({
        events: [
          {
            ...rootEvent,
            id: yield* idAllocator.allocate.event({ threadId: childThreadId }),
            threadId: childThreadId,
            type: "thread.deleted",
            payload: saved,
          },
        ],
      });
      for (const metadata of [
        { title: "Review checkout", modelSelection },
        { title: "Review checkout", modelSelection: { ...modelSelection, model: "gpt-6.1-sol" } },
      ]) {
        const updated = yield* ingest({ ...offered, ...metadata });
        assert.deepEqual(
          updated.map((stored) => stored.event.type),
          ["thread.metadata-updated"],
        );
        const thread = yield* projectionStore.getThread(childThreadId);
        assert.deepEqual(thread, { ...saved, ...metadata, updatedAt: thread.updatedAt });
      }
      assert.deepEqual(
        yield* ingest({
          ...offered,
          title: "Review checkout",
          modelSelection: { ...modelSelection, model: "gpt-6.1-sol" },
        }),
        [],
      );
    }),
  );

  it.effect("moves a native subagent's thread to the model its provider reports later", () =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const eventSink = yield* EventSink.EventSinkV2;
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const ingestor = yield* ProviderEventIngestor.ProviderEventIngestorV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const rootEvent = yield* threadCreatedEvent(now);
      if (rootEvent.type !== "thread.created") {
        throw new Error("Expected a thread.created fixture event");
      }
      const childThreadId = idAllocator.derive.threadFromProviderThread({
        driver: CODEX_DRIVER,
        nativeThreadId: "native-late-model-subagent",
      });
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId: rootEvent.threadId,
      });
      const ingest = (event: ProviderEventIngestor.ProviderEventIngestInput["event"]) =>
        ingestor.ingestNormalized({
          providerSessionId,
          providerInstanceId: modelSelection.instanceId,
          threadId: rootEvent.threadId,
          event,
        });
      yield* eventSink.write({ events: [rootEvent] });
      // The subagent's thread starts on the parent's model and options.
      yield* ingest({
        type: "app_thread.created",
        driver: CODEX_DRIVER,
        appThread: {
          ...rootEvent.payload,
          id: childThreadId,
          title: "review design",
          modelSelection: {
            ...modelSelection,
            options: [{ id: "reasoningEffort", value: "xhigh" }],
          },
          activeProviderThreadId: null,
          lineage: {
            parentThreadId: rootEvent.threadId,
            relationshipToParent: "subagent",
            rootThreadId: rootEvent.threadId,
          },
        },
      });
      const subagentUpdated = {
        type: "subagent.updated",
        driver: CODEX_DRIVER,
        subagent: {
          id: NodeId.make("node:late-model-subagent"),
          threadId: rootEvent.threadId,
          runId: null,
          parentNodeId: NodeId.make("node:root"),
          origin: "provider_native",
          createdBy: "agent",
          driver: CODEX_DRIVER,
          providerInstanceId: modelSelection.instanceId,
          providerThreadId: null,
          childThreadId,
          nativeTaskRef: null,
          prompt: "Review the design",
          title: "review design",
          model: "gpt-6.1-sol",
          status: "running",
          result: null,
          startedAt: now,
          completedAt: null,
          updatedAt: now,
        },
      } satisfies ProviderEventIngestor.ProviderEventIngestInput["event"];

      const first = yield* ingest(subagentUpdated);
      const repeated = yield* ingest(subagentUpdated);
      const childThread = yield* projectionStore.getThread(childThreadId);

      assert.deepEqual(
        first.map((stored) => [stored.event.type, stored.event.threadId]),
        [
          ["subagent.updated", rootEvent.threadId],
          ["thread.model-selection-updated", childThreadId],
        ],
      );
      assert.deepEqual(
        repeated.map((stored) => stored.event.type),
        ["subagent.updated"],
      );
      assert.deepEqual(childThread.modelSelection, {
        instanceId: modelSelection.instanceId,
        model: "gpt-6.1-sol",
      });

      // A selection the provider reports, effort included, replaces the thread's.
      const reportedSelection = {
        instanceId: modelSelection.instanceId,
        model: "gpt-6.1-sol",
        options: [{ id: "reasoningEffort", value: "low" }],
      };
      const reported = yield* ingest({
        ...subagentUpdated,
        subagent: { ...subagentUpdated.subagent, modelSelection: reportedSelection },
      });
      assert.deepEqual(
        reported.map((stored) => stored.event.type),
        ["subagent.updated", "thread.model-selection-updated"],
      );
      assert.deepEqual(
        (yield* projectionStore.getThread(childThreadId)).modelSelection,
        reportedSelection,
      );

      // A subagent recovered after a restart reports its model but no
      // selection; the same model keeps the effort the thread holds.
      const recovered = yield* ingest(subagentUpdated);
      assert.deepEqual(
        recovered.map((stored) => stored.event.type),
        ["subagent.updated"],
      );
      assert.deepEqual(
        (yield* projectionStore.getThread(childThreadId)).modelSelection,
        reportedSelection,
      );
    }),
  );
});
