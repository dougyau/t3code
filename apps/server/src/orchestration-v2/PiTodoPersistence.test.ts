import { assert, it } from "@effect/vitest";
import {
  CheckpointId,
  CheckpointRef,
  CheckpointScopeId,
  CommandId,
  ContextTransferId,
  EventId,
  MessageId,
  NodeId,
  PlanId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
  TurnItemId,
  ProjectId,
  pointerFromCapture,
  type PiChecklistCapture,
  type PiChecklistSelectionPointer,
  type OrchestrationV2AppThread,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2Run,
  type OrchestrationV2RunAttempt,
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2CheckpointScope,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";

import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import { selectCurrentChecklistPlan } from "@t3tools/shared/checklist";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as EventSink from "./EventSink.ts";
import * as EventStore from "./EventStore.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderEventIngestor from "./ProviderEventIngestor.ts";
import * as ProviderTurnStart from "./ProviderTurnStartService.ts";
import * as CheckpointRollback from "./CheckpointRollbackService.ts";
import * as CheckpointService from "./CheckpointService.ts";
import * as ContextHandoff from "./ContextHandoffService.ts";
import * as ThreadCommands from "./ThreadCommandExecutor.ts";
import * as RuntimePolicy from "./RuntimePolicy.ts";
import * as ProviderSessions from "./ProviderSessionManager.ts";
import * as ProviderAuth from "../provider/ProviderAuthService.ts";
import * as ProjectStore from "./ProjectStore.ts";
import * as RunExecution from "./RunExecutionService.ts";
import * as RunFinalization from "./RunFinalizationService.ts";
import * as McpAppModelContext from "../mcpApps/McpAppModelContext.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as StartupTestkit from "./ProviderTurnStartService.testkit.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";

const stores = Layer.merge(EventStore.layer, ProjectionStore.layer).pipe(
  Layer.provide(SqlitePersistence.layerMemory),
);
const sinkLayer = EventSink.layer.pipe(
  Layer.provide(Layer.merge(stores, SqlitePersistence.layerMemory)),
);
const baseLayer = Layer.mergeAll(
  stores,
  sinkLayer,
  IdAllocator.layer,
  ThreadCommands.layer,
  ProviderEventIngestor.layer.pipe(
    Layer.provide(Layer.mergeAll(stores, sinkLayer, IdAllocator.layer, ThreadCommands.layer)),
  ),
  SqlitePersistence.layerMemory,
);
const driver = ProviderDriverKind.make("pi");
const instanceId = ProviderInstanceId.make("pi-personal");
const modelSelection = { instanceId, model: "offline-test" };
const policy = {
  runtimeMode: "full-access",
  interactionMode: "default",
  cwd: "/tmp/t1-private-test",
  approvalPolicy: "never",
  sandboxPolicy: { type: "readOnly", access: { type: "fullAccess" }, networkAccess: false },
} as const satisfies ProviderAdapter.ProviderAdapterV2RuntimePolicy;
const policyLayer = Layer.mock(RuntimePolicy.RuntimePolicyV2)({
  resolve: () => Effect.succeed(policy),
});

const seed = Effect.fnUntraced(function* (label: string, nativeId: string | null = "session-a") {
  const sink = yield* EventSink.EventSinkV2;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const eventStore = yield* EventStore.EventStoreV2;
  const ingestor = yield* ProviderEventIngestor.ProviderEventIngestorV2;
  const now = yield* DateTime.now;
  const threadId = ThreadId.make(`thread:${label}`);
  const runId = RunId.make(`run:${label}`);
  const providerThreadId = ProviderThreadId.make(`provider-thread:${label}`);
  const sessionId = ProviderSessionId.make(`session:${label}`);
  const nodeId = NodeId.make(`node:${label}`);
  const attemptId = RunAttemptId.make(`attempt:${label}`);
  const scopeId = CheckpointScopeId.make(`scope:${label}`);
  const messageId = MessageId.make(`message:${label}`);
  let serial = 0;
  const envelope = () => ({
    id: EventId.make(`event:${label}:${++serial}`),
    threadId,
    occurredAt: now,
    driver,
    providerInstanceId: instanceId,
  });
  const thread: OrchestrationV2AppThread = {
    id: threadId,
    projectId: ProjectId.make(`project:${label}`),
    title: "Pi checklist",
    createdBy: "user",
    creationSource: "web",
    providerInstanceId: instanceId,
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: providerThreadId,
    activeOrderKey: null,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  };
  const providerThread: OrchestrationV2ProviderThread = {
    id: providerThreadId,
    driver,
    providerInstanceId: instanceId,
    providerSessionId: sessionId,
    appThreadId: threadId,
    ownerNodeId: null,
    nativeThreadRef: nativeId === null ? null : { driver, nativeId, strength: "strong" },
    nativeConversationHeadRef: null,
    status: "idle",
    firstRunOrdinal: 1,
    lastRunOrdinal: 1,
    handoffIds: [],
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
    nativeMetadata: { title: "Provider title", itemIdentityVersion: 2 },
  };
  const run: OrchestrationV2Run = {
    id: runId,
    threadId,
    ordinal: 1,
    providerInstanceId: instanceId,
    modelSelection,
    providerThreadId,
    userMessageId: messageId,
    rootNodeId: nodeId,
    activeAttemptId: attemptId,
    status: "starting",
    requestedAt: now,
    startedAt: null,
    completedAt: null,
    checkpointId: null,
    contextHandoffId: null,
  };
  const attempt: OrchestrationV2RunAttempt = {
    id: attemptId,
    runId,
    rootNodeId: nodeId,
    attemptOrdinal: 1,
    providerInstanceId: instanceId,
    providerThreadId,
    providerTurnId: null,
    reason: "initial",
    status: "pending",
    startedAt: null,
    completedAt: null,
  };
  const node: OrchestrationV2ExecutionNode = {
    id: nodeId,
    threadId,
    runId,
    parentNodeId: null,
    rootNodeId: nodeId,
    kind: "root_turn",
    status: "pending",
    countsForRun: true,
    providerThreadId,
    providerTurnId: null,
    nativeItemRef: null,
    runtimeRequestId: null,
    checkpointScopeId: scopeId,
    startedAt: null,
    completedAt: null,
  };
  const scope: OrchestrationV2CheckpointScope = {
    id: scopeId,
    threadId,
    runId,
    nodeId,
    parentScopeId: null,
    providerThreadId,
    kind: "root_run",
    ordinalWithinParent: 0,
    advancesAppRunCount: true,
    cwd: "/tmp/t1-private-test",
    createdAt: now,
  };
  yield* sink.write({
    events: [
      { ...envelope(), type: "thread.created", payload: thread },
      { ...envelope(), type: "provider-thread.updated", payload: providerThread },
      { ...envelope(), type: "run.created", payload: run },
      { ...envelope(), type: "run-attempt.updated", payload: attempt },
      { ...envelope(), type: "node.updated", payload: node },
      { ...envelope(), type: "checkpoint-scope.created", payload: scope },
      {
        ...envelope(),
        type: "message.updated",
        payload: {
          id: messageId,
          threadId,
          runId,
          nodeId,
          role: "user",
          text: "Continue",
          attachments: [],
          streaming: false,
          createdBy: "user",
          creationSource: "web",
          createdAt: now,
          updatedAt: now,
        },
      },
    ],
  });
  const capture = (version = 1, expected?: PiChecklistSelectionPointer): PiChecklistCapture => ({
    list: { threadId, providerInstanceId: instanceId, nativeSessionId: nativeId ?? "session-a" },
    ownerGeneration: 1,
    sessionGeneration: 0,
    captureVersion: version,
    authority: "capture",
    expectedPointer:
      expected === undefined ? { kind: "absent" } : { kind: "current", pointer: expected },
    source: {
      state: "ready",
      sourceEntryId: `entry:${version}`,
      tasks: [
        { id: 1, identityEntryId: "identity:1", subject: "First task", status: "in_progress" },
        {
          id: 2,
          identityEntryId: "identity:2",
          subject: "Hidden by a filtered list",
          status: "pending",
        },
        { id: 3, identityEntryId: "identity:3", subject: "Deleted task", status: "deleted" },
      ],
    },
  });
  const tool = (current: PiChecklistCapture, toolRunId = runId) =>
    ({
      type: "turn_item.updated",
      driver,
      checklistCapture: current,
      turnItem: {
        id: TurnItemId.make(`tool:${label}:${current.captureVersion}`),
        threadId,
        runId: toolRunId,
        nodeId,
        providerThreadId,
        providerTurnId: null,
        nativeItemRef: null,
        parentItemId: null,
        ordinal: current.captureVersion,
        status: "completed",
        title: "todo",
        startedAt: now,
        completedAt: now,
        updatedAt: now,
        type: "dynamic_tool",
        toolName: "todo",
        input: {},
        output: { tasks: current.source.tasks },
      },
    }) satisfies ProviderAdapter.ProviderAdapterV2Event;
  const ingest = (current: PiChecklistCapture, toolRunId = runId) =>
    ingestor.ingestNormalized({
      providerSessionId: sessionId,
      providerInstanceId: instanceId,
      threadId,
      runId: toolRunId,
      event: tool(current, toolRunId),
    });
  const read = () => projections.getThreadProjection(threadId);
  return {
    sink,
    projections,
    eventStore,
    ingestor,
    now,
    threadId,
    runId,
    providerThreadId,
    sessionId,
    attemptId,
    nodeId,
    scopeId,
    envelope,
    thread,
    providerThread,
    run,
    attempt,
    node,
    scope,
    capture,
    tool,
    ingest,
    read,
  };
});
type Fixture = Effect.Success<ReturnType<typeof seed>>;

function session(
  f: Fixture,
  overrides: Partial<ProviderAdapter.ProviderAdapterV2SessionRuntime> = {},
) {
  return {
    instanceId,
    driver,
    providerSessionId: f.sessionId,
    providerSession: {
      id: f.sessionId,
      driver,
      providerInstanceId: instanceId,
      status: "ready",
      cwd: policy.cwd,
      model: null,
      capabilities: CodexProviderCapabilitiesV2,
      createdAt: f.now,
      updatedAt: f.now,
      lastError: null,
    },
    events: Stream.never,
    ensureThread: () => Effect.die("unexpected ensure"),
    resumeThread: () => Effect.die("unexpected resume"),
    forkThread: () => Effect.die("unexpected fork"),
    rollbackThread: () => Effect.die("unexpected rollback"),
    readThreadSnapshot: () => Effect.die("unexpected snapshot"),
    startTurn: () => Effect.void,
    steerTurn: () => Effect.void,
    interruptTurn: () => Effect.void,
    respondToRuntimeRequest: () => Effect.void,
    ...overrides,
  } satisfies ProviderAdapter.ProviderAdapterV2SessionRuntime;
}

function start(
  f: Fixture,
  runtime: ProviderAdapter.ProviderAdapterV2SessionRuntime,
  willRetry = false,
) {
  return Effect.gen(function* () {
    yield* (yield* ProviderTurnStart.ProviderTurnStartServiceV2).start({
      threadId: f.threadId,
      runId: f.runId,
      willRetry,
    });
  }).pipe(
    Effect.provide(
      ProviderTurnStart.layer.pipe(
        Layer.provide(
          Layer.mergeAll(
            Layer.succeed(EventSink.EventSinkV2, f.sink),
            Layer.succeed(ProjectionStore.ProjectionStoreV2, f.projections),
            ContextHandoff.layer.pipe(Layer.provide(IdAllocator.layer)),
            IdAllocator.layer,
            FileSystem.layerNoop({}),
            StartupTestkit.layer,
            policyLayer,
            Layer.mock(ProviderSessions.ProviderSessionManagerV2)({
              open: () => Effect.succeed(runtime),
            }),
            Layer.mock(ProviderAuth.ProviderAuthService)({
              tryHandlePromptCommand: () => Effect.succeed(false),
            }),
            Layer.mock(RunExecution.RunExecutionServiceV2)({ startRootRun: () => Effect.void }),
          ),
        ),
      ),
    ),
  );
}

it.layer(baseLayer)("Pi checklist persistence", (it) => {
  it.effect("overlapping root subscribers commit only their accepted tools and reject stale selectors", () =>
    Effect.gen(function* () {
      const f = yield* seed("overlapping-subscribers");
      const olderEvents = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
      const newerEvents = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
      let subscriptions = 0;
      const runtime = session(f, {
        subscribeEvents: Effect.sync(() => ({
          events: Stream.fromQueue(subscriptions++ === 0 ? olderEvents : newerEvents),
          close: Effect.void,
        })),
      });
      const old = {
        run: { ...f.run, status: "running" as const, startedAt: f.now },
        attempt: { ...f.attempt, status: "running" as const, startedAt: f.now },
        node: { ...f.node, status: "running" as const, startedAt: f.now },
        scope: f.scope,
      };
      const runId = RunId.make("run:overlapping:newer");
      const attemptId = RunAttemptId.make("attempt:overlapping:newer");
      const nodeId = NodeId.make("node:overlapping:newer");
      const scopeId = CheckpointScopeId.make("scope:overlapping:newer");
      const newer = {
        run: { ...old.run, id: runId, ordinal: 2, activeAttemptId: attemptId, rootNodeId: nodeId },
        attempt: { ...old.attempt, id: attemptId, runId, rootNodeId: nodeId },
        node: { ...old.node, id: nodeId, runId, rootNodeId: nodeId, checkpointScopeId: scopeId },
        scope: { ...f.scope, id: scopeId, runId, nodeId },
      };
      yield* f.sink.write({
        events: [
          { ...f.envelope(), type: "run.updated", payload: old.run },
          { ...f.envelope(), type: "run-attempt.updated", payload: old.attempt },
          { ...f.envelope(), type: "node.updated", payload: old.node },
          { ...f.envelope(), type: "run.created", payload: newer.run },
          { ...f.envelope(), type: "run-attempt.updated", payload: newer.attempt },
          { ...f.envelope(), type: "node.updated", payload: newer.node },
          { ...f.envelope(), type: "checkpoint-scope.created", payload: newer.scope },
          { ...f.envelope(), type: "provider-thread.updated", payload: { ...f.providerThread, lastRunOrdinal: 2 } },
        ],
      });
      const first = f.capture();
      const second = f.capture(2, pointerFromCapture(first));
      const stale = f.capture(3, pointerFromCapture(first));
      const continued = f.capture(4, pointerFromCapture(second));
      yield* Effect.gen(function* () {
        const executor = yield* RunExecution.RunExecutionServiceV2;
        for (const root of [old, newer]) {
          yield* executor.startRootRun({
            commandId: CommandId.make(`command:${root.run.id}`),
            appThread: f.thread,
            providerSessionId: f.sessionId,
            session: runtime,
            run: root.run,
            rootNode: root.node,
            checkpointScope: root.scope,
            providerThread: f.providerThread,
            attempt: root.attempt,
            attemptId: root.attempt.id,
            providerTurnOrdinal: root.run.ordinal,
            message: {
              messageId: f.run.userMessageId, text: "Continue", attachments: [],
              createdBy: "user", creationSource: "web",
            },
            modelSelection,
            runtimePolicy: policy,
          });
        }
        assert.equal(subscriptions, 2);
        for (const [capture, owner] of [
          [first, old], [second, newer], [stale, old], [continued, old],
        ] as const) {
          const event = f.tool(capture, owner.run.id);
          if (event.type !== "turn_item.updated") return assert.fail("Expected tool result");
          const delivered = { ...event, turnItem: { ...event.turnItem, nodeId: owner.node.id } };
          // A manager broadcasts each native event to both actual subscribers.
          yield* Queue.offer(olderEvents, delivered);
          yield* Queue.offer(newerEvents, delivered);
          yield* f.sink.stream({ threadId: f.threadId, eventType: "turn-item.updated" }).pipe(
            Stream.filter((stored) => stored.event.type === "turn-item.updated" &&
              stored.event.payload.id === delivered.turnItem.id),
            Stream.take(1),
            Stream.runDrain,
          );
          const projection = yield* f.read();
          assert.deepEqual(
            projection.providerThreads[0]?.nativeMetadata?.checklistSelection,
            pointerFromCapture(capture === stale ? second : capture),
          );
        }
      }).pipe(Effect.provide(RunExecution.layer.pipe(Layer.provide(Layer.mergeAll(
        Layer.succeed(EventSink.EventSinkV2, f.sink),
        Layer.succeed(ProjectionStore.ProjectionStoreV2, f.projections),
        Layer.succeed(ProviderEventIngestor.ProviderEventIngestorV2, f.ingestor),
        Layer.mock(CheckpointService.CheckpointServiceV2)({ captureBaseline: () => Effect.void }),
        Layer.succeed(RunFinalization.RunFinalizationObserver, {
          refresh: () => Effect.void, refreshAfterTurn: () => Effect.void,
        }),
        McpAppModelContext.layerEmpty, ServerSettings.layerTest(), IdAllocator.layer,
      )))));
      const projection = yield* f.read();
      assert.equal(projection.turnItems.filter((item) => item.type === "dynamic_tool").length, 4);
      assert.equal(projection.turnItems.filter((item) => item.type === "todo_list").length, 1);
      assert.equal(projection.turnItems.find((item) => item.type === "todo_list")?.runId, null);
      const plans = yield* f.eventStore.read({ threadId: f.threadId, eventType: "plan.updated" }).pipe(Stream.runCollect);
      assert.equal(plans.length, 3, "the stale selector and the non-owning subscriber add no plan write");
    }),
  );

  it.effect(
    "uses the real root subscriber and preserves its pointer through actual finalization",
    () =>
      Effect.gen(function* () {
        const f = yield* seed("root-subscriber");
        const completed = yield* Deferred.make<void>();
        const runningRun = { ...f.run, status: "running" as const, startedAt: f.now };
        const runningAttempt = { ...f.attempt, status: "running" as const, startedAt: f.now };
        const runningNode = { ...f.node, status: "running" as const, startedAt: f.now };
        yield* f.sink.write({
          events: [
            { ...f.envelope(), type: "run.updated", payload: runningRun },
            { ...f.envelope(), type: "run-attempt.updated", payload: runningAttempt },
            { ...f.envelope(), type: "node.updated", payload: runningNode },
          ],
        });
        const providerTurnId = ProviderTurnId.make("turn:root-subscriber");
        const capture = f.capture();
        const events: ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event> = [
          {
            type: "provider_turn.updated",
            driver,
            threadId: f.threadId,
            providerTurn: {
              id: providerTurnId,
              providerThreadId: f.providerThreadId,
              nodeId: f.nodeId,
              runAttemptId: f.attemptId,
              nativeTurnRef: null,
              ordinal: 1,
              status: "running",
              startedAt: f.now,
              completedAt: null,
            },
          },
          f.tool(capture),
          f.tool(capture),
          {
            type: "turn.terminal",
            driver,
            providerThreadId: f.providerThreadId,
            providerTurnId,
            runOrdinal: 1,
            status: "completed",
            failure: null,
            threadDisposition: "reusable",
          },
        ];
        yield* Effect.gen(function* () {
          yield* (yield* RunExecution.RunExecutionServiceV2).startRootRun({
            commandId: CommandId.make("command:root-subscriber"),
            appThread: f.thread,
            providerSessionId: f.sessionId,
            session: session(f, { events: Stream.fromIterable(events) }),
            run: runningRun,
            rootNode: runningNode,
            checkpointScope: f.scope,
            providerThread: f.providerThread,
            attempt: runningAttempt,
            attemptId: f.attemptId,
            providerTurnOrdinal: 1,
            message: {
              messageId: f.run.userMessageId,
              text: "Continue",
              attachments: [],
              createdBy: "user",
              creationSource: "web",
            },
            modelSelection,
            runtimePolicy: policy,
          });
          // refreshAfterTurn happens after the persisted finalization batch, not a timer.
          yield* Deferred.await(completed);
        }).pipe(
          Effect.provide(
            RunExecution.layer.pipe(
              Layer.provide(
                Layer.mergeAll(
                  Layer.succeed(EventSink.EventSinkV2, f.sink),
                  Layer.succeed(ProjectionStore.ProjectionStoreV2, f.projections),
                  Layer.succeed(ProviderEventIngestor.ProviderEventIngestorV2, f.ingestor),
                  Layer.mock(CheckpointService.CheckpointServiceV2)({
                    captureBaseline: () => Effect.void,
                  }),
                  Layer.succeed(RunFinalization.RunFinalizationObserver, {
                    refresh: () => Effect.void,
                    refreshAfterTurn: () =>
                      Deferred.succeed(completed, undefined).pipe(Effect.asVoid),
                  }),
                  McpAppModelContext.layerEmpty,
                  ServerSettings.layerTest(),
                  IdAllocator.layer,
                ),
              ),
            ),
          ),
        );
        const projection = yield* f.read();
        assert.equal(projection.runs[0]?.status, "waiting");
        assert.equal(projection.providerThreads[0]?.status, "idle");
        assert.deepEqual(
          projection.providerThreads[0]?.nativeMetadata?.checklistSelection,
          pointerFromCapture(capture),
        );
        assert.equal(projection.plans.length, 1);
        assert.equal(projection.turnItems.filter((item) => item.type === "dynamic_tool").length, 1);
        assert.equal(
          projection.turnItems.find((item) => item.type === "dynamic_tool")?.runId,
          f.runId,
        );
        assert.equal(projection.turnItems.find((item) => item.type === "todo_list")?.runId, null);
        const listEvents = yield* f.eventStore
          .read({ threadId: f.threadId, eventType: "plan.updated" })
          .pipe(Stream.runCollect);
        assert.equal(
          listEvents.length,
          1,
          "a duplicated accepted tool capture cannot create another artifact",
        );
      }),
  );

  it.effect("rolls back the live tool and selector if checklist projection fails", () =>
    Effect.gen(function* () {
      const f = yield* seed("atomic-failure");
      const sql = yield* SqlClient.SqlClient;
      yield* sql`CREATE TEMP TRIGGER fail_pi_plan BEFORE INSERT ON orchestration_v2_projection_plans
        WHEN NEW.thread_id = 'thread:atomic-failure'
        BEGIN SELECT RAISE(ABORT, 'test checklist projection failure'); END`;
      const before = yield* f.eventStore.latestSequence({ threadId: f.threadId });
      assert.isTrue(Option.isSome(yield* f.ingest(f.capture()).pipe(Effect.flip, Effect.option)));
      const projection = yield* f.read();
      assert.equal(projection.plans.length, 0);
      assert.equal(projection.turnItems.length, 0);
      assert.equal(projection.providerThreads[0]?.nativeMetadata?.checklistSelection, undefined);
      assert.equal(yield* f.eventStore.latestSequence({ threadId: f.threadId }), before);
      yield* sql`DROP TRIGGER fail_pi_plan`;
      yield* f.ingest(f.capture());
      assert.equal((yield* f.read()).plans.length, 1);
    }),
  );

  it.effect(
    "atomically persists accepted live tool attribution, list, item, selector and replay",
    () =>
      Effect.gen(function* () {
        const f = yield* seed("live");
        const capture = f.capture();
        const events = yield* f.ingest(capture);
        assert.equal(events.length, 4);
        const projection = yield* f.read();
        assert.equal(
          projection.turnItems.find((item) => item.type === "dynamic_tool")?.runId,
          f.runId,
        );
        const pointer = projection.providerThreads[0]?.nativeMetadata?.checklistSelection;
        assert.deepEqual(pointer, pointerFromCapture(capture));
        const plan = projection.plans[0];
        assert.equal(plan?.runId, null);
        assert.equal(plan?.kind, "todo_list");
        if (plan?.kind !== "todo_list") return assert.fail("Missing persisted checklist");
        assert.deepEqual(
          plan.steps.map((step) => step.text),
          ["First task", "Hidden by a filtered list"],
        );
        assert.isTrue(plan.steps.every((step) => step.durationMs === undefined));
        const item = projection.turnItems.find((item) => item.type === "todo_list");
        assert.equal(item?.runId, null);
        assert.equal(item?.type === "todo_list" ? item.planId : undefined, plan.id);
        const stored = yield* f.eventStore.read({ threadId: f.threadId }).pipe(Stream.runCollect);
        const replay = yield* ProjectionStore.ProjectionStoreV2.pipe(
          Effect.provide(ProjectionStore.layerMemory),
        );
        for (const event of stored) yield* replay.apply(event.event);
        const replayed = yield* replay.getThreadProjection(f.threadId);
        assert.deepEqual(replayed.plans, projection.plans);
        assert.deepEqual(
          [...replayed.turnItems].sort((a, b) => a.ordinal - b.ordinal),
          projection.turnItems,
        );
        assert.deepEqual(replayed.providerThreads, projection.providerThreads);
      }),
  );

  it.effect(
    "distinguishes a valid empty checklist from unavailable without selecting historical content",
    () =>
      Effect.gen(function* () {
        const f = yield* seed("empty-unavailable");
        const first = f.capture();
        yield* f.ingest(first);
        const empty: PiChecklistCapture = {
          ...f.capture(2, pointerFromCapture(first)),
          source: { state: "ready", sourceEntryId: "empty-entry", tasks: [] },
        };
        yield* f.ingest(empty);
        const cleared = yield* f.read();
        const plan = selectCurrentChecklistPlan({
          plans: cleared.plans,
          activeProviderThread: cleared.providerThreads[0] ?? null,
        });
        assert.equal(plan?.kind === "todo_list" ? plan.steps.length : undefined, 0);
        const unavailable: PiChecklistCapture = {
          ...f.capture(3, pointerFromCapture(empty)),
          source: { state: "unavailable", sourceEntryId: null, tasks: [] },
        };
        assert.equal((yield* f.ingest(unavailable)).length, 2);
        const lost = yield* f.read();
        assert.equal(
          lost.providerThreads[0]?.nativeMetadata?.checklistSelection?.state,
          "unavailable",
        );
        assert.equal(
          selectCurrentChecklistPlan({
            plans: lost.plans,
            activeProviderThread: lost.providerThreads[0] ?? null,
          }),
          null,
        );
      }),
  );

  it.effect(
    "retains the pointed completed list and artifact pair outside a bounded timeline window",
    () =>
      Effect.gen(function* () {
        const f = yield* seed("bounded-list");
        const capture: PiChecklistCapture = {
          ...f.capture(),
          source: {
            state: "ready",
            sourceEntryId: "completed-entry",
            tasks: [
              {
                id: 1,
                identityEntryId: "identity:1",
                subject: "Completed task",
                status: "completed",
              },
            ],
          },
        };
        yield* f.ingest(capture);
        for (const version of [10, 20]) {
          const { checklistCapture: _capture, ...event } = f.tool(f.capture(version));
          yield* f.ingestor.ingestNormalized({
            providerSessionId: f.sessionId,
            providerInstanceId: instanceId,
            threadId: f.threadId,
            runId: f.runId,
            event,
          });
        }
        const full = yield* f.read();
        const bounded = yield* f.projections.getThreadSnapshotWindow(f.threadId, { rowLimit: 1 });
        assert.equal(bounded.projection.providerThreads[0]?.status, "idle");
        assert.deepEqual(bounded.projection.plans, full.plans);
        assert.equal(bounded.projection.plans[0]?.status, "completed");
        assert.deepEqual(
          bounded.projection.turnItems.filter((item) => item.type === "todo_list"),
          full.turnItems.filter((item) => item.type === "todo_list"),
        );
        assert.equal(
          bounded.projection.turnItems.filter((item) => item.type === "dynamic_tool").length,
          1,
        );
        assert.equal(
          selectCurrentChecklistPlan({
            plans: bounded.projection.plans,
            activeProviderThread: bounded.projection.providerThreads[0] ?? null,
          })?.id,
          full.plans[0]?.id,
        );
      }),
  );

  it.effect(
    "rejects stale expected pointer, lower generation and wrong native session without fencing known runs",
    () =>
      Effect.gen(function* () {
        const f = yield* seed("overlap");
        const first = f.capture();
        yield* f.ingest(first);
        const second = f.capture(2, pointerFromCapture(first));
        const newerRunId = RunId.make("run:overlap:newer");
        yield* f.sink.write({
          events: [
            {
              ...f.envelope(),
              type: "run.created",
              payload: { ...f.run, id: newerRunId, ordinal: 2 },
            },
          ],
        });
        yield* f.ingest(second, newerRunId);
        const before = yield* f.read();
        for (const stale of [
          { ...f.capture(3), ownerGeneration: 0 },
          { ...f.capture(4, pointerFromCapture(second)), ownerGeneration: 0 },
          {
            ...f.capture(5, pointerFromCapture(second)),
            list: { ...second.list, nativeSessionId: "other-session" },
          },
        ]) {
          assert.equal((yield* f.ingest(stale)).length, 1);
        }
        const after = yield* f.read();
        assert.deepEqual(after.plans, before.plans);
        assert.deepEqual(
          after.providerThreads[0]?.nativeMetadata?.checklistSelection,
          pointerFromCapture(second),
        );
        assert.equal(after.turnItems.filter((item) => item.type === "todo_list").length, 1);
        assert.equal(
          after.turnItems.find((item) => item.id === TurnItemId.make("tool:overlap:2"))?.runId,
          "run:overlap:newer",
        );
        // The row captured before the live tool (as in run finalization) cannot erase or authorize a pointer.
        yield* f.sink.write({
          events: [
            {
              ...f.envelope(),
              type: "provider-thread.updated",
              payload: {
                ...f.providerThread,
                status: "idle",
                nativeMetadata: {
                  title: "Finalized provider",
                  checklistSelection: pointerFromCapture(first),
                },
              },
            },
          ],
        });
        const finalized = yield* f.read();
        assert.deepEqual(
          finalized.providerThreads[0]?.nativeMetadata?.checklistSelection,
          pointerFromCapture(second),
        );
        assert.equal(finalized.providerThreads[0]?.nativeMetadata?.title, "Finalized provider");
        const third = f.capture(6, pointerFromCapture(second));
        yield* f.ingest(third, f.runId);
        const continued = yield* f.read();
        assert.deepEqual(
          continued.providerThreads[0]?.nativeMetadata?.checklistSelection,
          pointerFromCapture(third),
        );
        assert.equal(
          continued.turnItems.find((item) => item.id === TurnItemId.make("tool:overlap:6"))?.runId,
          f.runId,
          "a known older run can still produce the conversation's current list",
        );
      }),
  );

  it.effect("lowers a fork supplement only into the guarded target thread", () =>
    Effect.gen(function* () {
      const source = yield* seed("fork-source");
      const f = yield* seed("fork-target", null);
      const sourceCapture = source.capture();
      yield* source.ingest(sourceCapture);
      const capture = f.capture();
      yield* f.sink.write({
        events: [
          {
            ...f.envelope(),
            type: "context-transfer.updated",
            payload: {
              id: ContextTransferId.make("transfer:fork-target"),
              type: "fork",
              sourceThreadId: source.threadId,
              targetThreadId: f.threadId,
              sourcePoint: { threadId: source.threadId, runId: source.runId },
              basePoint: null,
              sourceProviderInstanceId: instanceId,
              targetProviderInstanceId: instanceId,
              targetRunId: f.runId,
              status: "pending",
              resolution: null,
              createdBy: "user",
              error: null,
              createdAt: f.now,
              updatedAt: f.now,
              consumedAt: null,
            },
          },
        ],
      });
      yield* start(
        f,
        session(f, {
          forkThread: (input) => {
            assert.equal(input.sourceProviderThread.id, source.providerThreadId);
            assert.equal(input.targetThreadId, f.threadId);
            return Effect.succeed({
              ...f.providerThread,
              nativeThreadRef: { driver, nativeId: "session-a", strength: "strong" },
              checklistCapture: capture,
            });
          },
        }),
      );
      const target = yield* f.read();
      assert.equal(target.contextTransfers[0]?.status, "consumed");
      assert.equal(target.plans[0]?.threadId, f.threadId);
      assert.deepEqual(
        target.providerThreads[0]?.nativeMetadata?.checklistSelection,
        pointerFromCapture(capture),
      );
      assert.deepEqual(
        (yield* source.read()).providerThreads[0]?.nativeMetadata?.checklistSelection,
        pointerFromCapture(sourceCapture),
      );
    }),
  );

  it.effect.each(["ensure", "resume"] as const)(
    "lowers %s supplement in the real guarded startup batch",
    (method) =>
      Effect.gen(function* () {
        const f = yield* seed(`startup-${method}`, method === "ensure" ? null : "session-a");
        const capture = f.capture();
        const loaded = {
          ...f.providerThread,
          nativeThreadRef: { driver, nativeId: "session-a", strength: "strong" as const },
          checklistCapture: capture,
        };
        yield* start(
          f,
          session(f, {
            [method === "ensure" ? "ensureThread" : "resumeThread"]: () => Effect.succeed(loaded),
          }),
        );
        const projection = yield* f.read();
        assert.equal(projection.runs[0]?.status, "running");
        assert.deepEqual(
          projection.providerThreads[0]?.nativeMetadata?.checklistSelection,
          pointerFromCapture(capture),
        );
        assert.equal(projection.providerThreads[0]?.nativeMetadata?.title, "Provider title");
        assert.equal(projection.plans.length, 1);
        assert.equal(projection.turnItems.filter((item) => item.type === "todo_list").length, 1);
        assert.isFalse("checklistCapture" in (projection.providerThreads[0] ?? {}));
      }),
  );

  it.effect(
    "rejects a stale startup attempt at the transaction cut and permits independent progress",
    () =>
      Effect.gen(function* () {
        const f = yield* seed("startup-guard", null);
        const capture = f.capture();
        // The first guard reads current. Replacement happens at the EventSink write cut.
        const cutSink = {
          ...f.sink,
          writeIfRunCurrent: (
            input: Parameters<EventSink.EventSinkV2Shape["writeIfRunCurrent"]>[0],
          ) =>
            Effect.gen(function* () {
              yield* f.sink.write({
                events: [
                  {
                    ...f.envelope(),
                    type: "run.updated",
                    payload: {
                      ...f.run,
                      activeAttemptId: RunAttemptId.make("replacement-attempt"),
                    },
                  },
                ],
              });
              return yield* f.sink.writeIfRunCurrent(input);
            }),
        };
        yield* start(
          { ...f, sink: cutSink },
          session(f, {
            ensureThread: () =>
              Effect.succeed({
                ...f.providerThread,
                nativeThreadRef: { driver, nativeId: "session-a", strength: "strong" },
                checklistCapture: capture,
              }),
          }),
        );
        const rejected = yield* f.read();
        assert.equal(rejected.plans.length, 0);
        assert.equal(rejected.turnItems.length, 0);
        assert.equal(rejected.providerThreads[0]?.nativeMetadata?.checklistSelection, undefined);
        assert.equal(rejected.runs[0]?.activeAttemptId, "replacement-attempt");
        const valid = yield* seed("startup-independent", null);
        yield* start(
          valid,
          session(valid, {
            ensureThread: () =>
              Effect.succeed({
                ...valid.providerThread,
                nativeThreadRef: { driver, nativeId: "session-a", strength: "strong" },
                checklistCapture: valid.capture(),
              }),
          }),
        );
        assert.equal((yield* valid.read()).plans.length, 1);
      }),
  );

  it.effect.each([true, false])(
    "rejects a stale session-reset before committing its native session with willRetry=%s",
    (willRetry) =>
      Effect.gen(function* () {
        const f = yield* seed(`stale-reset-${willRetry}`);
        const old = f.capture();
        yield* f.ingest(old);
        const newer = { ...f.capture(2, pointerFromCapture(old)), ownerGeneration: 2 };
        const stale: PiChecklistCapture = {
          ...f.capture(3, pointerFromCapture(old)),
          list: { ...old.list, nativeSessionId: "session-b" },
          authority: "session_reset",
          ownerGeneration: 2,
          sessionGeneration: 1,
        };
        const cutSink = {
          ...f.sink,
          writeIfRunCurrent: (
            input: Parameters<EventSink.EventSinkV2Shape["writeIfRunCurrent"]>[0],
          ) =>
            Effect.gen(function* () {
              yield* f.ingest(newer).pipe(Effect.orDie);
              return yield* f.sink.writeIfRunCurrent(input);
            }),
        };
        const resumeThread = () =>
          Effect.fail(
            new ProviderAdapter.ProviderAdapterTurnStartError({
              driver,
              threadId: f.threadId,
              providerThreadId: f.providerThreadId,
              runId: f.runId,
              cause: "native session gone",
            }),
          );
        const attempted = yield* start(
          { ...f, sink: cutSink },
          session(f, {
            resumeThread,
            ensureThread: () =>
              Effect.succeed({
                ...f.providerThread,
                nativeThreadRef: { driver, nativeId: "session-b", strength: "strong" },
                checklistCapture: stale,
              }),
          }),
          willRetry,
        ).pipe(Effect.result);
        assert.equal(attempted._tag, willRetry ? "Failure" : "Success");
        const rejected = yield* f.read();
        assert.equal(rejected.runs[0]?.status, willRetry ? "starting" : "failed");
        assert.equal(rejected.providerThreads[0]?.nativeThreadRef?.nativeId, "session-a");
        assert.deepEqual(
          rejected.providerThreads[0]?.nativeMetadata?.checklistSelection,
          pointerFromCapture(newer),
        );
        assert.equal(rejected.plans.length, 1);
        const listEvents = yield* f.eventStore
          .read({
            threadId: f.threadId,
            eventType: "plan.updated",
          })
          .pipe(Stream.runCollect);
        assert.equal(
          listEvents.length,
          2,
          "only the two independently admitted captures are committed",
        );
        if (!willRetry) return;
        const fresh = {
          ...stale,
          ownerGeneration: 3,
          expectedPointer: { kind: "current" as const, pointer: pointerFromCapture(newer) },
        };
        yield* start(
          f,
          session(f, {
            resumeThread,
            ensureThread: () =>
              Effect.succeed({
                ...f.providerThread,
                nativeThreadRef: { driver, nativeId: "session-b", strength: "strong" },
                checklistCapture: fresh,
              }),
          }),
          true,
        );
        const retried = yield* f.read();
        assert.equal(retried.runs[0]?.status, "running");
        assert.equal(retried.providerThreads[0]?.nativeThreadRef?.nativeId, "session-b");
        assert.deepEqual(
          retried.providerThreads[0]?.nativeMetadata?.checklistSelection,
          pointerFromCapture(fresh),
        );
      }),
  );

  it.effect.each([true, false])(
    "retires the old selector after failed resume with supplement=%s",
    (supplement) =>
      Effect.gen(function* () {
        const f = yield* seed(`fallback-${supplement}`);
        const old = f.capture();
        yield* f.ingest(old);
        const next: PiChecklistCapture = {
          ...f.capture(2, pointerFromCapture(old)),
          list: { ...old.list, nativeSessionId: "session-b" },
          authority: "session_reset",
          ownerGeneration: 2,
          sessionGeneration: 1,
          source: { state: "ready", sourceEntryId: null, tasks: [] },
        };
        yield* start(
          f,
          session(f, {
            resumeThread: () =>
              Effect.fail(
                new ProviderAdapter.ProviderAdapterTurnStartError({
                  driver,
                  threadId: f.threadId,
                  providerThreadId: f.providerThreadId,
                  runId: f.runId,
                  cause: "native session gone",
                }),
              ),
            ensureThread: (input) => {
              assert.equal(input.existingProviderThread?.id, f.providerThreadId);
              assert.equal(input.existingProviderThread?.nativeThreadRef, null);
              return Effect.succeed({
                ...f.providerThread,
                nativeThreadRef: { driver, nativeId: "session-b", strength: "strong" },
                ...(supplement ? { checklistCapture: next } : {}),
              });
            },
          }),
        );
        const projection = yield* f.read();
        const row = projection.providerThreads[0];
        assert.equal(row?.id, f.providerThreadId);
        assert.equal(row?.nativeThreadRef?.nativeId, "session-b");
        assert.equal(row?.nativeMetadata?.checklistSelection?.list.nativeSessionId, "session-b");
        assert.equal(
          row?.nativeMetadata?.checklistSelection?.state,
          supplement ? "ready" : "unavailable",
        );
        const selected = selectCurrentChecklistPlan({
          plans: projection.plans,
          activeProviderThread: row ?? null,
        });
        assert.equal(selected, null, "a proven no-todo head cannot select the historical list");
        const handoffEvents = yield* f.eventStore
          .read({ threadId: f.threadId, eventType: "context-handoff.updated" })
          .pipe(Stream.runCollect);
        assert.equal(handoffEvents.length, 1);
      }),
  );

  it.effect.each([true, false])(
    "changes checklist only when actual rollbackThread is called: %s",
    (rollback) =>
      Effect.gen(function* () {
        const f = yield* seed(`rollback-${rollback}`);
        const old = f.capture();
        yield* f.ingest(old);
        const checkpointId = CheckpointId.make(`checkpoint:rollback-${rollback}`);
        yield* f.sink.write({
          events: [
            {
              ...f.envelope(),
              type: "run.updated",
              payload: { ...f.run, status: rollback ? "completed" : "rolled_back" },
            },
            {
              ...f.envelope(),
              type: "checkpoint.captured",
              payload: {
                id: checkpointId,
                threadId: f.threadId,
                runId: f.runId,
                nodeId: f.nodeId,
                scopeId: f.scopeId,
                ref: CheckpointRef.make("refs/t3-test/baseline"),
                ordinalWithinScope: 0,
                appRunOrdinal: null,
                parentCheckpointId: null,
                status: "ready",
                files: [],
                capturedAt: f.now,
              },
            },
          ],
        });
        const capture: PiChecklistCapture = {
          ...f.capture(2, pointerFromCapture(old)),
          source: { state: "ready", sourceEntryId: null, tasks: [] },
        };
        let calls = 0;
        const runtime = session(f, {
          rollbackThread: ({ providerThread }) => {
            calls++;
            return Effect.succeed({
              providerThread,
              checklistCapture: capture,
              providerTurns: [],
              messages: [],
              runtimeRequests: [],
            });
          },
        });
        yield* Effect.gen(function* () {
          yield* (yield* CheckpointRollback.CheckpointRollbackServiceV2).execute({
            threadId: f.threadId,
            providerThreadId: f.providerThreadId,
            checkpointId,
            scopeId: f.scopeId,
            restoreFiles: false,
          });
        }).pipe(
          Effect.provide(
            CheckpointRollback.layer.pipe(
              Layer.provide(
                Layer.mergeAll(
                  Layer.succeed(EventSink.EventSinkV2, f.sink),
                  Layer.succeed(ProjectionStore.ProjectionStoreV2, f.projections),
                  Layer.mock(CheckpointService.CheckpointServiceV2)({
                    deleteStaleRefs: () => Effect.void,
                  }),
                  Layer.mock(ProviderSessions.ProviderSessionManagerV2)({
                    open: () => Effect.succeed(runtime),
                  }),
                  Layer.mock(ProjectStore.ProjectStoreV2)({
                    get: () => Effect.succeed(Option.none()),
                  }),
                  IdAllocator.layer,
                  ThreadCommands.layer,
                  policyLayer,
                  FileSystem.layerNoop({}),
                  Path.layer,
                ),
              ),
            ),
          ),
        );
        const projection = yield* f.read();
        assert.equal(calls, rollback ? 1 : 0);
        assert.deepEqual(
          projection.providerThreads[0]?.nativeMetadata?.checklistSelection,
          pointerFromCapture(rollback ? capture : old),
        );
        assert.equal(
          projection.plans[0]?.kind === "todo_list" ? projection.plans[0].steps.length : undefined,
          rollback ? 0 : 2,
        );
      }),
  );

  it.effect("does not carry Pi durations but leaves non-Pi checklist timing intact", () =>
    Effect.gen(function* () {
      const f = yield* seed("durations");
      const planId = PlanId.make("plan:legacy-pi-duration");
      const plan = {
        id: planId,
        threadId: f.threadId,
        runId: null,
        nodeId: f.nodeId,
        kind: "todo_list" as const,
        status: "active" as const,
        steps: [{ id: "step", text: "Timed task", status: "running" as const }],
      };
      for (const pi of [false, true]) {
        const source = {
          ...plan,
          id: PlanId.make(`${planId}:${pi}`),
          ...(pi ? { piChecklistProvenance: "pi_native" as const } : {}),
        };
        yield* f.ingestor.ingestNormalized({
          providerSessionId: f.sessionId,
          providerInstanceId: instanceId,
          threadId: f.threadId,
          event: {
            type: "plan.updated",
            driver: pi ? driver : ProviderDriverKind.make("codex"),
            plan: source,
          },
        });
        yield* TestClock.adjust("2 seconds");
        yield* f.ingestor.ingestNormalized({
          providerSessionId: f.sessionId,
          providerInstanceId: instanceId,
          threadId: f.threadId,
          event: {
            type: "plan.updated",
            driver: pi ? driver : ProviderDriverKind.make("codex"),
            plan: { ...source, steps: [{ ...source.steps[0]!, status: "completed" }] },
          },
        });
        const persisted = yield* f.projections.getPlan(f.threadId, source.id);
        assert.equal(
          persisted?.kind === "todo_list" ? persisted.steps[0]?.durationMs : undefined,
          pi ? undefined : 2000,
        );
      }
    }),
  );
});
