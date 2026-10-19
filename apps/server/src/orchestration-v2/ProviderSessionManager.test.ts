import * as NetAddress from "effect/net/NetAddress";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import { AgentScope } from "@t3tools/shared/AgentScope";
import {
  EnvironmentId,
  type ModelSelection,
  type OrchestrationV2AppThread,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ProviderCapabilities,
  type OrchestrationV2ProviderSession,
  type OrchestrationV2ProviderThread,
  type Project,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  type ProviderSessionId,
  type PiNativeWorkerLaunch,
  type PiNativeWorkerCheckpoint,
  type PiNativeWorkerSummary,
  NodeId,
  MessageId,
  RunId,
  RunAttemptId,
  ProviderTurnId,
  TurnItemId,
  ThreadId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Schema from "effect/Schema";
import * as Layer from "effect/Layer";
import * as Metric from "effect/Metric";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import { TestClock } from "effect/testing";
import { HttpServer } from "effect/http";

import { ProviderWorkspaceMissingError } from "../provider/Errors.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as McpProviderSession from "@t3tools/provider-core/server/mcpSession";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as McpSessionRegistry from "../mcp/McpSessionRegistry.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ServerSettings from "../serverSettings.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as EventSink from "./EventSink.ts";
import * as EventStore from "./EventStore.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProviderEventIngestor from "./ProviderEventIngestor.ts";
import * as ThreadCommandExecutor from "./ThreadCommandExecutor.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { readArchitectures } from "../../../../packages/provider-pi/src/server/architectureBridge.ts";
import * as ArchitectureQuery from "./ArchitectureQuery.ts";

const layerTestDatabase = SqlitePersistence.layerMemory;
const isArchitectureCatalogLoss = Schema.is(ProviderAdapter.ProviderArchitectureCatalogLoss);
const layerTestStores = Layer.merge(EventStore.layer, ProjectionStore.layer).pipe(
  Layer.provide(layerTestDatabase),
);
const layerTestEventSink = EventSink.layer.pipe(
  Layer.provide(Layer.mergeAll(layerTestStores, layerTestDatabase)),
);
const layerFailingReleaseEventSink = Layer.effect(
  EventSink.EventSinkV2,
  Effect.gen(function* () {
    const delegate = yield* EventSink.EventSinkV2;
    return EventSink.EventSinkV2.of({
      ...delegate,
      write: (input) =>
        input.events.some(
          (event) =>
            event.type === "provider-session.updated" &&
            (event.payload.status === "stopped" || event.payload.status === "error"),
        )
          ? Effect.fail(new EventSink.EventSinkWriteError({ eventCount: input.events.length }))
          : delegate.write(input),
    });
  }),
).pipe(Layer.provide(layerTestEventSink));

interface FlakyReleaseWrites {
  /** Which release writes fail right now. */
  readonly failing: Ref.Ref<"none" | "session" | "session-and-requests">;
  readonly failNativeRetirement?: Ref.Ref<boolean>;
  readonly rejectNativeRetirement?: boolean;
  /** Receives one item per failed write. */
  readonly failures: Queue.Queue<void>;
  /** Holds runtime request writes: completes `paused`, then waits for `resume`. */
  readonly pauseRequestWrites?: {
    readonly paused: Deferred.Deferred<void>;
    readonly resume: Deferred.Deferred<void>;
  };
}

// Fails release writes with a defect, the way a failed SQL commit surfaces.
const layerFlakyReleaseEventSink = (flaky: FlakyReleaseWrites) =>
  Layer.effect(
    EventSink.EventSinkV2,
    Effect.gen(function* () {
      const delegate = yield* EventSink.EventSinkV2;
      return EventSink.EventSinkV2.of({
        ...delegate,
        write: (input) =>
          Effect.gen(function* () {
            const failing = yield* Ref.get(flaky.failing);
            const fails = input.events.some(
              (event) =>
                (failing !== "none" &&
                  event.type === "provider-session.updated" &&
                  (event.payload.status === "stopped" || event.payload.status === "error")) ||
                (failing === "session-and-requests" && event.type === "runtime-request.updated"),
            );
            const pause = flaky.pauseRequestWrites;
            if (
              pause !== undefined &&
              input.events.some((event) => event.type === "runtime-request.updated")
            ) {
              yield* Deferred.succeed(pause.paused, undefined);
              yield* Deferred.await(pause.resume);
            }
            if (!fails) return yield* delegate.write(input);
            yield* Queue.offer(flaky.failures, undefined);
            return yield* Effect.die(new Error("simulated commit failure"));
          }),
        writeNativeWorker: (input) =>
          Effect.gen(function* () {
            if (
              input.retire === true &&
              flaky.failNativeRetirement !== undefined &&
              (yield* Ref.get(flaky.failNativeRetirement))
            ) {
              yield* Queue.offer(flaky.failures, undefined);
              if (flaky.rejectNativeRetirement)
                return {
                  committed: false,
                  sourceProgress: false,
                  storedEvents: [],
                };
              return yield* Effect.fail(
                new EventSink.EventSinkWriteError({ eventCount: 0, cause: "retirement-commit" }),
              );
            }
            return yield* delegate.writeNativeWorker(input);
          }),
      });
    }),
  ).pipe(Layer.provide(layerTestEventSink));

// Once armed, holds the next attach write until the writer is interrupted.
const layerPausingAttachEventSink = (pause: {
  readonly armed: Ref.Ref<boolean>;
  readonly paused: Deferred.Deferred<void>;
}) =>
  Layer.effect(
    EventSink.EventSinkV2,
    Effect.gen(function* () {
      const delegate = yield* EventSink.EventSinkV2;
      return EventSink.EventSinkV2.of({
        ...delegate,
        write: (input) =>
          Effect.gen(function* () {
            const attach = input.events.some((event) => event.type === "provider-session.attached");
            if (attach && (yield* Ref.getAndSet(pause.armed, false))) {
              yield* Deferred.succeed(pause.paused, undefined);
              return yield* Effect.never;
            }
            return yield* delegate.write(input);
          }),
      });
    }),
  ).pipe(Layer.provide(layerTestEventSink));

const CodexCapabilities: OrchestrationV2ProviderCapabilities = CodexProviderCapabilitiesV2;
const ExclusiveCapabilities: OrchestrationV2ProviderCapabilities = {
  ...CodexCapabilities,
  sessions: {
    ...CodexCapabilities.sessions,
    supportsMultipleProviderThreadsPerSession: false,
  },
};

interface TestProviderRuntimeState {
  readonly openCount: number;
  readonly closeCount: number;
  readonly interruptCount: number;
  readonly resumeCount: number;
  readonly unloadedNativeThreadIds: ReadonlyArray<string>;
  readonly eventQueues: ReadonlyMap<
    string,
    Queue.Queue<ProviderAdapter.ProviderAdapterV2Event, Cause.Done>
  >;
}

const emptyState: TestProviderRuntimeState = {
  openCount: 0,
  closeCount: 0,
  interruptCount: 0,
  resumeCount: 0,
  unloadedNativeThreadIds: [],
  eventQueues: new Map(),
};

const modelSelection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "gpt-5.4",
} satisfies ModelSelection;
const CODEX_DRIVER = ProviderDriverKind.make("codex");

const runtimePolicy = {
  runtimeMode: "full-access",
  interactionMode: "default",
  cwd: process.cwd(),
} satisfies ProviderAdapter.ProviderAdapterV2RuntimePolicy;

function makeProviderSession(input: {
  readonly providerSessionId: ProviderSessionId;
  readonly now: DateTime.Utc;
  readonly capabilities?: OrchestrationV2ProviderCapabilities;
}): OrchestrationV2ProviderSession {
  return {
    id: input.providerSessionId,
    driver: CODEX_DRIVER,
    providerInstanceId: modelSelection.instanceId,
    status: "ready",
    cwd: process.cwd(),
    model: "gpt-5.4",
    capabilities: input.capabilities ?? CodexCapabilities,
    createdAt: input.now,
    updatedAt: input.now,
    lastError: null,
  };
}

function makeThreadCreatedEvent(input: {
  readonly idAllocator: IdAllocator.IdAllocatorV2["Service"];
  readonly threadId: ThreadId;
  readonly now: DateTime.Utc;
  readonly projectId?: ProjectId;
}) {
  return Effect.gen(function* () {
    const projectId =
      input.projectId ??
      (yield* input.idAllocator.allocate.project({
        fixtureName: "provider-session-manager",
      }));
    const providerThreadId = input.idAllocator.derive.providerThread({
      driver: CODEX_DRIVER,
      nativeThreadId: "native-thread",
    });
    const thread: OrchestrationV2AppThread = {
      createdBy: "user",
      creationSource: "web",
      id: input.threadId,
      projectId,
      title: "Provider session manager",
      providerInstanceId: modelSelection.instanceId,
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      activeProviderThreadId: providerThreadId,
      lineage: {
        parentThreadId: null,
        relationshipToParent: null,
        rootThreadId: input.threadId,
      },
      forkedFrom: null,
      createdAt: input.now,
      updatedAt: input.now,
      archivedAt: null,
      settledOverride: null,
      settledAt: null,
      lastVisitedAt: null,
      deletedAt: null,
    };
    return {
      id: yield* input.idAllocator.allocate.event({ threadId: input.threadId }),
      type: "thread.created" as const,
      threadId: input.threadId,
      occurredAt: input.now,
      payload: thread,
    };
  });
}

function makeProviderThread(input: {
  readonly idAllocator: IdAllocator.IdAllocatorV2["Service"];
  readonly threadId: ThreadId;
  readonly providerSessionId: ProviderSessionId;
  readonly now: DateTime.Utc;
  readonly nativeThreadId?: string;
}): OrchestrationV2ProviderThread {
  const nativeThreadId = input.nativeThreadId ?? "native-thread";
  return {
    id: input.idAllocator.derive.providerThread({
      driver: CODEX_DRIVER,
      nativeThreadId,
    }),
    driver: CODEX_DRIVER,
    providerInstanceId: modelSelection.instanceId,
    providerSessionId: input.providerSessionId,
    appThreadId: input.threadId,
    ownerNodeId: null,
    nativeThreadRef: {
      driver: CODEX_DRIVER,
      nativeId: nativeThreadId,
      strength: "strong",
    },
    nativeConversationHeadRef: null,
    status: "idle",
    firstRunOrdinal: null,
    lastRunOrdinal: null,
    handoffIds: [],
    forkedFrom: null,
    createdAt: input.now,
    updatedAt: input.now,
  };
}

function unimplemented(detail: string) {
  return Effect.fail(
    new ProviderAdapter.ProviderAdapterProtocolError({
      driver: CODEX_DRIVER,
      detail,
    }),
  );
}

const makeProviderAdapter = Effect.fnUntraced(function* (
  state: Ref.Ref<TestProviderRuntimeState>,
  options: {
    readonly failEventStream?: boolean;
    readonly capabilities?: OrchestrationV2ProviderCapabilities;
    readonly mcpConfigs?: Ref.Ref<
      ReadonlyArray<McpProviderSession.McpProviderSessionConfig | undefined>
    >;
    readonly beforeOpen?: (input: {
      readonly providerSessionId: ProviderSessionId;
      readonly initialProviderItemIdentityVersion?: 2;
    }) => Effect.Effect<void>;
    readonly hasPendingBackgroundWork?: Effect.Effect<boolean>;
    readonly hasPendingBackgroundWorkForThread?: Effect.Effect<boolean>;
    readonly hangSessionScopeClose?: boolean;
    readonly startTurn?: Effect.Effect<void>;
    readonly beforeUnload?: Effect.Effect<void>;
    /** Registers the process's closeCount finalizer before `beforeOpen` runs. */
    readonly spawnBeforeOpen?: boolean;
    /** Completed when a hanging scope close reaches its wedged finalizer. */
    readonly scopeCloseReached?: Deferred.Deferred<void>;
    readonly nativeWorkers?: (
      providerSessionId: ProviderSessionId,
    ) => ProviderAdapter.ProviderNativeWorkersPort;
    readonly onScopeClose?: Effect.Effect<void>;
    readonly providerEvents?: Queue.Queue<ProviderAdapter.ProviderAdapterV2Event, Cause.Done>;
  } = {},
) {
  const mcpSessions = yield* McpProviderSessions.McpProviderSessions;
  const countClose = Effect.addFinalizer(() =>
    Ref.update(state, (current) => ({
      ...current,
      closeCount: current.closeCount + 1,
    })),
  );
  // Registered after countClose so it runs first on scope close, wedging the
  // close before the closeCount finalizer, like a provider process that never
  // yields its message stream.
  const hangClose = Effect.addFinalizer(() =>
    (options.scopeCloseReached === undefined
      ? Effect.void
      : Deferred.succeed(options.scopeCloseReached, undefined)
    ).pipe(Effect.andThen(Effect.never)),
  );
  return ProviderAdapter.ProviderAdapterV2.of({
    instanceId: ProviderInstanceId.make("codex"),
    driver: CODEX_DRIVER,
    getCapabilities: () => Effect.succeed(options.capabilities ?? CodexCapabilities),
    planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
    openSession: (input) =>
      Effect.gen(function* () {
        if (options.spawnBeforeOpen === true) {
          yield* countClose;
          if (options.hangSessionScopeClose === true) yield* hangClose;
        }
        if (options.mcpConfigs !== undefined && options.spawnBeforeOpen === true) {
          const mcpConfig = yield* mcpSessions.read(input.threadId);
          yield* Ref.update(options.mcpConfigs, (configs) => [...configs, mcpConfig]);
        }
        if (options.beforeOpen !== undefined) {
          yield* options.beforeOpen(input);
        }
        if (options.mcpConfigs !== undefined && options.spawnBeforeOpen !== true) {
          const mcpConfig = yield* mcpSessions.read(input.threadId);
          yield* Ref.update(options.mcpConfigs, (configs) => [...configs, mcpConfig]);
        }
        const now = yield* DateTime.now;
        const events =
          options.providerEvents ??
          (yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event, Cause.Done>());
        const session = makeProviderSession({
          providerSessionId: input.providerSessionId,
          now,
          ...(options.capabilities === undefined ? {} : { capabilities: options.capabilities }),
        });
        yield* Ref.update(state, (current) => {
          const eventQueues = new Map(current.eventQueues);
          eventQueues.set(String(input.providerSessionId), events);
          return {
            ...current,
            openCount: current.openCount + 1,
            eventQueues,
          };
        });
        if (options.spawnBeforeOpen !== true) {
          yield* countClose;
          if (options.hangSessionScopeClose === true) yield* hangClose;
        }
        if (options.onScopeClose !== undefined)
          yield* Effect.addFinalizer(() => options.onScopeClose!);

        return {
          instanceId: ProviderInstanceId.make("codex"),
          driver: CODEX_DRIVER,
          providerSessionId: input.providerSessionId,
          providerSession: session,
          events: options.failEventStream
            ? Stream.fail(
                new ProviderAdapter.ProviderAdapterEventStreamError({
                  driver: CODEX_DRIVER,
                  providerSessionId: input.providerSessionId,
                  cause: "process exited",
                }),
              )
            : Stream.fromQueue(events),
          ...(options.nativeWorkers === undefined
            ? {}
            : { nativeWorkers: options.nativeWorkers(input.providerSessionId) }),
          ...(options.hasPendingBackgroundWork === undefined
            ? {}
            : { hasPendingBackgroundWork: options.hasPendingBackgroundWork }),
          ...(options.hasPendingBackgroundWorkForThread === undefined
            ? {}
            : {
                hasPendingBackgroundWorkForThread: () => options.hasPendingBackgroundWorkForThread!,
              }),
          ensureThread: () => unimplemented("ensureThread unused in test"),
          resumeThread: (threadInput) =>
            Ref.update(state, (current) => ({
              ...current,
              resumeCount: current.resumeCount + 1,
            })).pipe(Effect.as(threadInput.providerThread)),
          startTurn: () => options.startTurn ?? Effect.void,
          steerTurn: () => Effect.void,
          interruptTurn: () =>
            Ref.update(state, (current) => ({
              ...current,
              interruptCount: current.interruptCount + 1,
            })),
          unloadThread: ({ providerThread }) =>
            (options.beforeUnload ?? Effect.void).pipe(
              Effect.andThen(
                Ref.update(state, (current) => ({
                  ...current,
                  unloadedNativeThreadIds: [
                    ...current.unloadedNativeThreadIds,
                    providerThread.nativeThreadRef?.nativeId ?? "",
                  ],
                })),
              ),
            ),
          respondToRuntimeRequest: () => Effect.void,
          readThreadSnapshot: () => unimplemented("readThreadSnapshot unused in test"),
          rollbackThread: () => unimplemented("rollbackThread unused in test"),
          forkThread: () => unimplemented("forkThread unused in test"),
        } satisfies ProviderAdapter.ProviderAdapterV2SessionRuntime;
      }),
  });
});

function layerTest(input: {
  readonly state: Ref.Ref<TestProviderRuntimeState>;
  readonly residencyFailure?: Ref.Ref<"none" | "membership" | "checkpoint">;
  readonly nativeMembership?: EventSink.EventSinkV2["Service"]["nativeWorkers"];
  readonly unreadableCheckpoint?: Ref.Ref<ThreadId | null>;
  readonly idleTimeoutMs: number;
  readonly maxIdlePinMs?: number;
  readonly failEventStream?: boolean;
  readonly capabilities?: OrchestrationV2ProviderCapabilities;
  readonly mcpConfigs?: Ref.Ref<
    ReadonlyArray<McpProviderSession.McpProviderSessionConfig | undefined>
  >;
  readonly beforeOpen?: (input: {
    readonly providerSessionId: ProviderSessionId;
    readonly initialProviderItemIdentityVersion?: 2;
  }) => Effect.Effect<void>;
  readonly failReleaseEventWrites?: boolean;
  readonly flakyReleaseWrites?: FlakyReleaseWrites;
  readonly pauseAttachWrite?: Parameters<typeof layerPausingAttachEventSink>[0];
  /** Once armed, holds the next credential lookup until it is interrupted. */
  readonly pauseResolve?: {
    readonly armed: Ref.Ref<boolean>;
    readonly paused: Deferred.Deferred<void>;
  };
  readonly hasPendingBackgroundWork?: Effect.Effect<boolean>;
  readonly hasPendingBackgroundWorkForThread?: Effect.Effect<boolean>;
  readonly hangSessionScopeClose?: boolean;
  readonly startTurn?: Effect.Effect<void>;
  readonly beforeUnload?: Effect.Effect<void>;
  readonly spawnBeforeOpen?: boolean;
  readonly scopeCloseReached?: Deferred.Deferred<void>;
  readonly nativeWorkers?: (
    providerSessionId: ProviderSessionId,
  ) => ProviderAdapter.ProviderNativeWorkersPort;
  readonly onScopeClose?: Effect.Effect<void>;
  readonly providerEvents?: Queue.Queue<ProviderAdapter.ProviderAdapterV2Event, Cause.Done>;
  readonly serverSettingsLayer?: ReturnType<typeof ServerSettings.layerTest>;
  readonly projectServiceLayer?: Layer.Layer<ProjectService.ProjectService>;
}) {
  const testStores =
    input.residencyFailure === undefined && input.unreadableCheckpoint === undefined
      ? layerTestStores
      : Layer.effect(
          ProjectionStore.ProjectionStoreV2,
          Effect.gen(function* () {
            const delegate = yield* ProjectionStore.ProjectionStoreV2;
            return ProjectionStore.ProjectionStoreV2.of({
              ...delegate,
              getThreadRecords: (...args) =>
                Effect.gen(function* () {
                  if (
                    (input.residencyFailure !== undefined &&
                      (yield* Ref.get(input.residencyFailure)) === "checkpoint") ||
                    (input.unreadableCheckpoint !== undefined &&
                      (yield* Ref.get(input.unreadableCheckpoint)) === args[0])
                  ) {
                    return yield* Effect.die("checkpoint unreadable");
                  }
                  return yield* delegate.getThreadRecords(...args);
                }),
            });
          }),
        ).pipe(Layer.provideMerge(layerTestStores));
  const baseEventSink =
    input.flakyReleaseWrites !== undefined
      ? layerFlakyReleaseEventSink(input.flakyReleaseWrites)
      : input.pauseAttachWrite !== undefined
        ? layerPausingAttachEventSink(input.pauseAttachWrite)
        : input.failReleaseEventWrites
          ? layerFailingReleaseEventSink
          : layerTestEventSink;
  const layerConfiguredEventSink =
    input.residencyFailure === undefined && input.nativeMembership === undefined
      ? baseEventSink
      : Layer.effect(
          EventSink.EventSinkV2,
          Effect.gen(function* () {
            const delegate = yield* EventSink.EventSinkV2;
            return EventSink.EventSinkV2.of({
              ...delegate,
              nativeWorkers: (...args) =>
                Effect.gen(function* () {
                  if (
                    input.residencyFailure !== undefined &&
                    (yield* Ref.get(input.residencyFailure)) === "membership"
                  )
                    return yield* Effect.die("membership unreadable");
                  return yield* (input.nativeMembership ?? delegate.nativeWorkers)(...args);
                }),
            });
          }),
        ).pipe(Layer.provide(baseEventSink));
  const layerRegistry = Layer.unwrap(
    makeProviderAdapter(input.state, {
      failEventStream: input.failEventStream ?? false,
      ...(input.capabilities === undefined ? {} : { capabilities: input.capabilities }),
      ...(input.mcpConfigs === undefined ? {} : { mcpConfigs: input.mcpConfigs }),
      ...(input.beforeOpen === undefined ? {} : { beforeOpen: input.beforeOpen }),
      ...(input.hasPendingBackgroundWork === undefined
        ? {}
        : { hasPendingBackgroundWork: input.hasPendingBackgroundWork }),
      ...(input.hasPendingBackgroundWorkForThread === undefined
        ? {}
        : { hasPendingBackgroundWorkForThread: input.hasPendingBackgroundWorkForThread }),
      ...(input.hangSessionScopeClose === undefined
        ? {}
        : { hangSessionScopeClose: input.hangSessionScopeClose }),
      ...(input.startTurn === undefined ? {} : { startTurn: input.startTurn }),
      ...(input.beforeUnload === undefined ? {} : { beforeUnload: input.beforeUnload }),
      ...(input.spawnBeforeOpen === undefined ? {} : { spawnBeforeOpen: input.spawnBeforeOpen }),
      ...(input.scopeCloseReached === undefined
        ? {}
        : { scopeCloseReached: input.scopeCloseReached }),
      ...(input.nativeWorkers === undefined ? {} : { nativeWorkers: input.nativeWorkers }),
      ...(input.onScopeClose === undefined ? {} : { onScopeClose: input.onScopeClose }),
      ...(input.providerEvents === undefined ? {} : { providerEvents: input.providerEvents }),
    }).pipe(Effect.map(ProviderAdapterRegistry.layerSingle)),
  );
  const layerConfiguredMcpRegistry =
    input.pauseResolve === undefined
      ? layerTestMcpRegistry
      : layerPausingMcpRegistry(input.pauseResolve);
  const layerProviderEventIngestorTest = ProviderEventIngestor.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        layerConfiguredEventSink,
        IdAllocator.layer,
        testStores,
        ThreadCommandExecutor.layer,
      ),
    ),
  );
  return Layer.mergeAll(
    testStores,
    layerConfiguredEventSink,
    layerProviderEventIngestorTest,
    IdAllocator.layer,
    layerConfiguredMcpRegistry,
    McpProviderSessions.layer,
    ProviderSessionManager.layerWithOptions({
      idleTimeoutMs: input.idleTimeoutMs,
      ...(input.maxIdlePinMs === undefined ? {} : { maxIdlePinMs: input.maxIdlePinMs }),
    }).pipe(
      Layer.provide(
        Layer.mergeAll(
          layerRegistry.pipe(Layer.provideMerge(McpProviderSessions.layer)),
          layerConfiguredEventSink,
          IdAllocator.layer,
          layerProviderEventIngestorTest,
          layerConfiguredMcpRegistry,
          testStores,
          ...(input.serverSettingsLayer === undefined ? [] : [input.serverSettingsLayer]),
          ...(input.projectServiceLayer === undefined ? [] : [input.projectServiceLayer]),
        ),
      ),
    ),
  ).pipe(Layer.provide(NodeServices.layer));
}

const fakeHttpServer = HttpServer.HttpServer.of({
  address: NetAddress.inetAddressFromIpStringUnsafe("127.0.0.1", 43123),
  serve: (() => Effect.void) as HttpServer.HttpServer["Service"]["serve"],
});

class NativeSourceUnavailable extends Schema.TaggedError<NativeSourceUnavailable>()(
  "NativeSourceUnavailable",
  {},
) {}

const residencyOwner = {
  processId: "residency-process",
  coordinatorSessionId: "residency-owner",
  epoch: "epoch-1",
};

function residencyPort(
  readBackgroundPending: ProviderAdapter.ProviderNativeWorkersPort["readBackgroundPending"],
) {
  return {
    owner: residencyOwner,
    ownerGeneration: 1,
    hints: Stream.never,
    readBackgroundPending,
    reconcile: () => Effect.die("No native membership admitted"),
    read: () => Effect.die("No native membership admitted"),
    hasPendingWork: () => Effect.die("Residency must not depend on admitted launches"),
  } satisfies ProviderAdapter.ProviderNativeWorkersPort;
}

const openResidencySession = Effect.fnUntraced(function* () {
  const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
  const sink = yield* EventSink.EventSinkV2;
  const ids = yield* IdAllocator.IdAllocatorV2;
  const now = yield* DateTime.now;
  const threadId = ThreadId.make("residency-thread");
  const providerSessionId = yield* ids.allocate.providerSession({
    threadId,
    providerInstanceId: modelSelection.instanceId,
  });
  yield* sink.write({
    events: [yield* makeThreadCreatedEvent({ idAllocator: ids, threadId, now })],
  });
  const runtime = yield* manager.open({
    threadId,
    providerSessionId,
    modelSelection,
    runtimePolicy,
  });
  const providerThread = makeProviderThread({ idAllocator: ids, threadId, providerSessionId, now });
  return { manager, sink, ids, now, threadId, providerSessionId, runtime, providerThread };
});

const startResidencyTurn = Effect.fnUntraced(function* (
  fixture: Effect.Success<ReturnType<typeof openResidencySession>>,
) {
  const { runtime, ids, threadId, providerThread } = fixture;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const runId = ids.derive.run({ threadId, ordinal: 1 });
  yield* runtime.startTurn({
    appThread: (yield* projections.getThreadProjection(threadId)).thread,
    threadId,
    providerThread,
    runId,
    runOrdinal: 1,
    providerTurnOrdinal: 1,
    attemptId: ids.derive.runAttempt({ runId, attemptOrdinal: 1 }),
    rootNodeId: ids.derive.rootNode({ runId }),
    message: {
      createdBy: "user",
      creationSource: "web",
      messageId: MessageId.make("residency-message"),
      text: "continue",
      attachments: [],
    },
    modelSelection,
    runtimePolicy,
  });
});

const terminalResidencyTurn = Effect.fnUntraced(function* (
  fixture: Effect.Success<ReturnType<typeof openResidencySession>>,
  runtimeState: Ref.Ref<TestProviderRuntimeState>,
) {
  const state = yield* fixture.runtime.subscribeEvents!;
  const terminal = yield* state.events.pipe(
    Stream.filter((event) => event.type === "turn.terminal"),
    Stream.take(1),
    Stream.runDrain,
    Effect.forkScoped,
  );
  // The fixture provider sends through the same broadcast the manager consumes.
  const providerEvents = yield* Ref.get(runtimeState);
  yield* Queue.offer(providerEvents.eventQueues.get(String(fixture.providerSessionId))!, {
    type: "turn.terminal",
    driver: CODEX_DRIVER,
    providerThreadId: fixture.providerThread.id,
    providerTurnId: ProviderTurnId.make(`terminal-${fixture.threadId}`),
    runOrdinal: 1,
    status: "completed",
    failure: null,
    threadDisposition: "reusable",
  });
  yield* Fiber.join(terminal);
});

const attachResidencySibling = Effect.fnUntraced(function* (
  fixture: Effect.Success<ReturnType<typeof openResidencySession>>,
  name: string,
) {
  const { sink, ids, now, manager, providerSessionId } = fixture;
  const threadId = ThreadId.make(name);
  yield* sink.write({
    events: [yield* makeThreadCreatedEvent({ idAllocator: ids, threadId, now })],
  });
  const runtime = yield* manager.open({
    threadId,
    providerSessionId,
    modelSelection,
    runtimePolicy,
  });
  const providerThread = makeProviderThread({
    idAllocator: ids,
    threadId,
    providerSessionId,
    now,
    nativeThreadId: name,
  });
  return { ...fixture, threadId, runtime, providerThread };
});

const admitResidencyCapture = Effect.fnUntraced(function* (
  fixture: Effect.Success<ReturnType<typeof openResidencySession>>,
  name: string,
  changes: Partial<PiNativeWorkerCheckpoint> = {},
) {
  const { sink, ids, now, threadId, providerSessionId, providerThread } = fixture;
  const ingestor = yield* ProviderEventIngestor.ProviderEventIngestorV2;
  const runId = ids.derive.run({ threadId, ordinal: 1 });
  const runAttemptId = ids.derive.runAttempt({ runId, attemptOrdinal: 1 });
  yield* sink.write({
    events: [
      {
        id: yield* ids.allocate.event({ threadId }),
        type: "run.created",
        threadId,
        occurredAt: now,
        payload: {
          id: runId,
          threadId,
          ordinal: 1,
          providerInstanceId: modelSelection.instanceId,
          modelSelection,
          providerThreadId: providerThread.id,
          userMessageId: MessageId.make("residency-user-message"),
          rootNodeId: ids.derive.rootNode({ runId }),
          activeAttemptId: runAttemptId,
          status: "running",
          requestedAt: now,
          startedAt: now,
          completedAt: null,
          checkpointId: null,
          contextHandoffId: null,
        },
      },
    ],
  });
  const launch: PiNativeWorkerLaunch = {
    source: "agent_spawn",
    instanceId: name,
    providerInstanceId: modelSelection.instanceId,
    providerSessionId,
    parentThreadId: threadId,
    parentProviderThreadId: providerThread.id,
    parentProviderTurnId: ProviderTurnId.make("residency-parent-turn"),
    runId,
    runAttemptId,
    toolNodeId: NodeId.make(`residency-tool-${name}`),
    coordinatorSessionId: residencyOwner.coordinatorSessionId,
    toolCallId: `spawn-${name}`,
  };
  const checkpoint: PiNativeWorkerCheckpoint = {
    launch,
    owner: residencyOwner,
    ownerGeneration: 1,
    captureEpoch: null,
    bindings: [],
    bindingGeneration: null,
    bindingCursors: [],
    cursor: null,
    cut: 0,
    recovery: false,
    capture: "opening",
    finalSequence: null,
    gaps: [],
    ...changes,
  };
  yield* ingestor.ingestNormalized({
    providerSessionId,
    providerInstanceId: modelSelection.instanceId,
    threadId,
    event: {
      type: "turn_item.updated",
      driver: CODEX_DRIVER,
      turnItem: {
        id: TurnItemId.make(`residency-launch-${name}`),
        threadId,
        runId,
        nodeId: launch.toolNodeId,
        providerThreadId: providerThread.id,
        providerTurnId: launch.parentProviderTurnId,
        nativeItemRef: null,
        parentItemId: null,
        ordinal: 1,
        status: "completed",
        title: null,
        startedAt: now,
        completedAt: now,
        updatedAt: now,
        type: "dynamic_tool",
        toolName: "agent_spawn",
        input: {},
        output: {},
        nativeWorkerLaunch: launch,
      },
    },
    nativeWorkerAdmission: {
      checkpoint,
      summary: {
        launch,
        execution: "completed",
        result: "available",
        capture: checkpoint.capture,
        availability: "ready",
        gaps: checkpoint.gaps,
      },
    },
  });
  return (yield* sink.nativeWorkers(providerSessionId)).find(
    (member) => member.nativeWorker?.launch.instanceId === name,
  )!;
});

const writeResidencyCheckpoint = Effect.fnUntraced(function* (
  fixture: Effect.Success<ReturnType<typeof openResidencySession>>,
  member: Effect.Success<ReturnType<typeof admitResidencyCapture>>,
  capture: "missing" | "closed",
) {
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const child = yield* projections.getThreadRecords(member.childThreadId!, ["providerThreads"]);
  const row = child.providerThreads.find((candidate) => candidate.id === member.providerThreadId)!;
  const previous = row.nativeMetadata!.nativeWorkerCheckpoint!;
  const checkpoint = { ...previous, capture: "closed" as const, finalSequence: 1 };
  // Ordinary writes correctly cannot overwrite native capture. Use its CAS
  // boundary, including for the deliberately missing-checkpoint recovery fixture.
  const eventId = yield* fixture.ids.allocate.event({ threadId: member.childThreadId! });
  const receipt = yield* fixture.sink.writeNativeWorker({
    launch: previous.launch,
    ownerGeneration: previous.ownerGeneration,
    expectedCheckpoint: previous,
    checkpoint,
    build: () =>
      Effect.succeed({
        sourceProgress: false,
        events: [
          {
            id: eventId,
            type: "provider-thread.updated",
            threadId: member.childThreadId!,
            occurredAt: fixture.now,
            payload: {
              ...row,
              nativeMetadata:
                capture === "missing"
                  ? {}
                  : { ...row.nativeMetadata, nativeWorkerCheckpoint: checkpoint },
            },
          },
        ],
      }),
  });
  assert.isTrue(receipt.committed);
});

it.effect.each(["completed-root", "owned-node", "other-provider-thread", "missing-root"] as const)(
  "collects Architecture only with a legitimate ordinary coordinator anchor: %s",
  (anchor) =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const owner = { ...residencyOwner, coordinatorSessionId: "native-thread" };
      const batch: ProviderAdapter.ProviderArchitectureBatch = {
        roomId: "root-room",
        checkpoint: { owner, ownerGeneration: 1, snapshotId: "root-cut", cut: 1 },
        capture: "open",
        gaps: [],
        records: [
          {
            kind: "room",
            id: "root-room",
            mode: "table",
            origin: {
              coordinatorSessionId: owner.coordinatorSessionId,
              tool: "architecture_start",
              toolCallId: "root-start",
              memberId: "director",
              instanceId: "director",
              assignmentId: "director-assignment",
            },
            goalSource: { kind: "worker-workbrief", instanceId: "director" },
            lifecycle: {
              stage: "preparation",
              round: 1,
              protocolState: "running",
              outcome: "pending",
              closure: { state: "pending", members: "pending", director: "pending" },
            },
            pendingDoubtIds: [],
          },
          {
            kind: "member",
            id: "director",
            assignmentId: "director-assignment",
            instanceId: "director",
            roleId: "architect",
            function: "director",
            ordinal: 0,
            admittedAt: 1,
            mandate: { source: "root-workbrief", instanceId: "director" },
            execution: "running",
            conversationRefs: [],
          },
        ],
      };
      yield* Effect.gen(function* () {
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const sink = yield* EventSink.EventSinkV2;
        const ids = yield* IdAllocator.IdAllocatorV2;
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        const now = yield* DateTime.now;
        const threadId = ThreadId.make("root-coordinator-thread");
        const providerSessionId = yield* ids.allocate.providerSession({
          threadId,
          providerInstanceId: modelSelection.instanceId,
        });
        const parent = {
          ...makeProviderThread({ idAllocator: ids, threadId, providerSessionId, now }),
          ownerNodeId: anchor === "owned-node" ? NodeId.make("owned-provider-node") : null,
        };
        if (anchor !== "owned-node") assert.isNull(parent.ownerNodeId);
        const runId = ids.derive.run({ threadId, ordinal: 1 });
        const rootNodeId = ids.derive.rootNode({ runId });
        const run = {
          id: runId,
          threadId,
          ordinal: 1,
          providerInstanceId: modelSelection.instanceId,
          modelSelection,
          providerThreadId:
            anchor === "other-provider-thread" ? ProviderThreadId.make("unrelated") : parent.id,
          userMessageId: MessageId.make("root-user-message"),
          rootNodeId: anchor === "missing-root" ? null : rootNodeId,
          activeAttemptId: ids.derive.runAttempt({ runId, attemptOrdinal: 1 }),
          status: "completed" as const,
          requestedAt: now,
          startedAt: now,
          completedAt: now,
          checkpointId: null,
          contextHandoffId: null,
        };
        yield* sink.write({
          events: [
            yield* makeThreadCreatedEvent({ idAllocator: ids, threadId, now }),
            {
              id: yield* ids.allocate.event({ threadId }),
              type: "provider-thread.updated",
              threadId,
              occurredAt: now,
              payload: parent,
            },
            {
              id: yield* ids.allocate.event({ threadId }),
              type: "run.created",
              threadId,
              occurredAt: now,
              payload: run,
            },
            // A newer unrelated run must never supply the coordinator's root.
            {
              id: yield* ids.allocate.event({ threadId }),
              type: "run.created",
              threadId,
              occurredAt: now,
              payload: {
                ...run,
                id: ids.derive.run({ threadId, ordinal: 2 }),
                ordinal: 2,
                providerThreadId: ProviderThreadId.make("newer-unrelated"),
                rootNodeId: NodeId.make("wrong-root"),
              },
            },
          ],
        });
        // open awaits the real collector's initial pass; no timer or polling is needed.
        yield* manager.open({ threadId, providerSessionId, modelSelection, runtimePolicy });
        const archives = yield* sink.architectures(threadId);
        const thread = yield* projections.getThread(threadId);
        const members = yield* sink.nativeWorkers(providerSessionId);
        if (anchor === "completed-root" || anchor === "owned-node") {
          assert.lengthOf(archives, 1);
          assert.equal(archives[0]?.parentProviderThreadId, parent.id);
          assert.deepEqual(archives[0]?.records, batch.records);
          assert.equal(thread.nativeTables?.[0]?.roomId, batch.roomId);
          assert.lengthOf(members, 1);
          assert.deepEqual(members[0]?.nativeWorker?.launch, {
            source: "architecture_roster",
            providerInstanceId: modelSelection.instanceId,
            providerSessionId,
            parentThreadId: threadId,
            parentProviderThreadId: parent.id,
            parentNodeId: parent.ownerNodeId ?? rootNodeId,
            coordinatorSessionId: owner.coordinatorSessionId,
            roomId: batch.roomId,
            memberId: "director",
            assignmentId: "director-assignment",
            instanceId: "director",
            roleId: "architect",
          });
        } else {
          assert.isEmpty(archives);
          assert.isEmpty(thread.nativeTables ?? []);
          assert.isEmpty(members);
        }
        yield* manager.close(providerSessionId);
      }).pipe(
        Effect.provide(
          layerTest({
            state,
            idleTimeoutMs: 30 * 60_000,
            nativeWorkers: () => ({
              ...residencyPort(() => Effect.succeed({ state: "pending" })),
              owner,
              readArchitectures: () => Effect.succeed([batch]),
              read: (launch, expected) => {
                if (expected === undefined) return Effect.die("Expected admitted checkpoint");
                return Effect.succeed({
                  launch,
                  expectedCheckpoint: expected,
                  checkpoint: expected,
                  records: [],
                  bindings: [],
                  more: false,
                  summary: {
                    launch,
                    execution: "running",
                    result: "pending",
                    capture: "opening",
                    availability: "ready",
                    gaps: [],
                  },
                });
              },
            }),
          }),
        ),
      );
    }),
);

it.effect(
  "observes Architecture while foreground is idle and fences the retired owner on close",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const hints = yield* Queue.unbounded<void>();
      const owner = { ...residencyOwner, coordinatorSessionId: "native-thread" };
      let cut = 1;
      const batch = (): ProviderAdapter.ProviderArchitectureBatch => ({
        roomId: "idle-room",
        checkpoint: { owner, ownerGeneration: 1, snapshotId: `cut-${cut}`, cut },
        capture: "open",
        gaps: [],
        records: [
          {
            kind: "room",
            id: "idle-room",
            mode: "table",
            origin: {
              coordinatorSessionId: owner.coordinatorSessionId,
              tool: "architecture_start",
              toolCallId: "start",
              memberId: "director",
              instanceId: "director",
              assignmentId: "director",
            },
            goalSource: { kind: "worker-workbrief", instanceId: "director" },
            lifecycle: {
              stage: "waiting",
              round: cut,
              protocolState: "waiting",
              outcome: "pending",
              closure: { state: "pending", members: "pending", director: "pending" },
            },
            pendingDoubtIds: [],
          },
        ],
      });
      yield* Effect.gen(function* () {
        const fixture = yield* openResidencySession();
        yield* fixture.sink.write({
          events: [
            {
              id: yield* fixture.ids.allocate.event({ threadId: fixture.threadId }),
              threadId: fixture.threadId,
              type: "provider-thread.updated",
              occurredAt: fixture.now,
              payload: { ...fixture.providerThread, ownerNodeId: NodeId.make("root") },
            },
          ],
        });
        yield* startResidencyTurn(fixture);
        yield* terminalResidencyTurn(fixture, state);
        const imported = yield* fixture.sink.stream({ eventType: "thread.metadata-updated" }).pipe(
          Stream.filter(
            (stored) =>
              stored.event.type === "thread.metadata-updated" &&
              stored.event.payload.nativeTables?.some(
                (table) => table.roomId === "idle-room" && table.lifecycle.round === 2,
              ) === true,
          ),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped,
        );
        cut = 2;
        yield* Queue.offer(hints, undefined);
        yield* Fiber.join(imported);
        const saved = (yield* fixture.sink.architectures(fixture.threadId))[0]!;
        assert.equal(saved.checkpoint.cut, 2);
        yield* fixture.manager.close(fixture.providerSessionId);
        const retired = (yield* fixture.sink.architectures(fixture.threadId))[0]!;
        assert.isTrue(retired.retired);
        assert.equal(retired.summary.observation, "stale");
        assert.equal(retired.summary.capture, "incomplete");
        assert.equal(retired.summary.lifecycle.closure.state, "pending");
        const ingestor = yield* ProviderEventIngestor.ProviderEventIngestorV2;
        cut = 3;
        yield* ingestor.ingestArchitecture({
          threadId: fixture.threadId,
          providerInstanceId: modelSelection.instanceId,
          providerSessionId: fixture.providerSessionId,
          parentProviderThreadId: fixture.providerThread.id,
          parentNodeId: NodeId.make("root"),
          batch: batch(),
          isConnected: Effect.succeed(true),
        });
        assert.equal((yield* fixture.sink.architectures(fixture.threadId))[0]?.checkpoint.cut, 2);
      }).pipe(
        Effect.provide(
          layerTest({
            state,
            idleTimeoutMs: 30 * 60_000,
            nativeWorkers: () => ({
              ...residencyPort(() => Effect.succeed({ state: "pending" })),
              owner,
              hints: Stream.fromQueue(hints),
              readArchitectures: () => Effect.sync(() => [batch()]),
            }),
          }),
        ),
      );
    }),
);

it.effect.each(["first-page", "later-page"] as const)(
  "retains confirmed Architecture on %s catalog loss while admitted workers progress",
  (failurePage) =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const hints = yield* Queue.unbounded<void>();
      const owner = { ...residencyOwner, coordinatorSessionId: "native-thread" };
      let failing = false;
      let throwCatalog = false;
      let workerCut = 0;
      const source: Parameters<typeof readArchitectures>[0] = {
        owner,
        capabilities: ["architecture.list", "architecture.read", "architecture.public-v1"],
        listArchitectures: async (cursor) => {
          if (throwCatalog) throw Error("controlled catalog transport failure");
          if (failing && (failurePage === "first-page" || cursor))
            return { state: "unavailable", reason: "snapshot_oversized" };
          return {
            state: "ready",
            format: "architecture-public-v1",
            owner,
            snapshotId: "catalog",
            cut: 5,
            next: failing ? "next-catalog-page" : null,
            items: failing
              ? [
                  {
                    ref: { ...owner, kind: "architecture", roomId: "unconfirmed-room" },
                    mode: "table",
                    director: {
                      instanceId: "unknown",
                      memberId: "unknown",
                      assignmentId: "unknown",
                    },
                    lifecycle: {
                      stage: null,
                      round: 0,
                      protocolState: "running",
                      outcome: "pending",
                      closure: { state: "pending", members: "pending", director: "pending" },
                    },
                  },
                ]
              : [],
          };
        },
        readArchitecture: async () => {
          throw Error("Must finish the catalog before reading any discovered room");
        },
        publicDetail: async () => {
          throw Error("No original may be discovered through an incomplete catalog");
        },
      };
      const batch: ProviderAdapter.ProviderArchitectureBatch = {
        roomId: "confirmed-room",
        checkpoint: { owner, ownerGeneration: 1, snapshotId: "confirmed-cut", cut: 4 },
        capture: "open",
        gaps: [],
        original: {
          id: "original",
          text: "  # Original 🧭\r\nLiteral unchanged goal",
          source: "original WorkBrief",
          complete: true,
        },
        records: [
          {
            kind: "room",
            id: "confirmed-room",
            mode: "table",
            origin: {
              coordinatorSessionId: owner.coordinatorSessionId,
              tool: "architecture_start",
              toolCallId: "confirmed-start",
              memberId: "director",
              instanceId: "director",
              assignmentId: "director-assignment",
            },
            goalSource: { kind: "worker-workbrief", instanceId: "director" },
            lifecycle: {
              stage: "preparation",
              round: 1,
              protocolState: "running",
              outcome: "pending",
              closure: { state: "pending", members: "pending", director: "pending" },
            },
            pendingDoubtIds: [],
          },
          {
            kind: "member",
            id: "director",
            assignmentId: "director-assignment",
            instanceId: "director",
            roleId: "architect",
            function: "director",
            ordinal: 0,
            admittedAt: 1,
            mandate: { source: "root-workbrief", instanceId: "director" },
            execution: "running",
            conversationRefs: [],
          },
        ],
      };
      yield* Effect.gen(function* () {
        const fixture = yield* openResidencySession();
        yield* fixture.sink.write({
          events: [
            {
              id: yield* fixture.ids.allocate.event({ threadId: fixture.threadId }),
              threadId: fixture.threadId,
              type: "provider-thread.updated",
              occurredAt: fixture.now,
              payload: { ...fixture.providerThread, ownerNodeId: NodeId.make("root") },
            },
          ],
        });
        const ingestor = yield* ProviderEventIngestor.ProviderEventIngestorV2;
        const input = {
          threadId: fixture.threadId,
          providerInstanceId: modelSelection.instanceId,
          providerSessionId: fixture.providerSessionId,
          parentProviderThreadId: fixture.providerThread.id,
          parentNodeId: NodeId.make("root"),
          isConnected: Effect.succeed(true),
        };
        yield* ingestor.ingestArchitecture({ ...input, batch });
        const room = batch.records.find((record) => record.kind === "room")!;
        for (const fence of ["owner", "generation"] as const) {
          yield* ingestor.ingestArchitecture({
            ...input,
            batch: {
              ...batch,
              roomId: fence,
              records: [{ ...room, id: fence }],
              checkpoint: {
                ...batch.checkpoint,
                ...(fence === "owner"
                  ? { owner: { ...owner, epoch: "different-owner" } }
                  : { ownerGeneration: 2 }),
              },
            },
          });
        }
        const before = yield* fixture.sink.architectures(fixture.threadId);
        const saved = before.find((archive) => archive.summary.roomId === batch.roomId)!;
        const worker = (yield* fixture.sink.nativeWorkers(fixture.providerSessionId))[0]!;
        assert.isDefined(worker.childThreadId);
        const advance = Effect.fnUntraced(function* () {
          workerCut++;
          const committed = yield* fixture.sink
            .stream({ eventType: "provider-thread.updated" })
            .pipe(
              Stream.filter(
                (stored) =>
                  stored.event.type === "provider-thread.updated" &&
                  stored.event.payload.nativeMetadata?.nativeWorkerCheckpoint?.cut === workerCut,
              ),
              Stream.take(1),
              Stream.runDrain,
              Effect.forkScoped,
            );
          yield* Queue.offer(hints, undefined);
          yield* Fiber.join(committed);
        });
        failing = true;
        yield* advance();
        const archives = yield* fixture.sink.architectures(fixture.threadId);
        assert.equal(archives.length, 3);
        const partial = archives.find((archive) => archive.summary.roomId === batch.roomId)!;
        for (const archive of archives.filter((archive) => archive !== partial))
          assert.deepEqual(
            archive,
            before.find((previous) => previous.summary.tableId === archive.summary.tableId),
          );
        assert.deepEqual(partial.records, saved.records);
        assert.deepEqual(partial.original, saved.original);
        assert.deepEqual(partial.checkpoint, saved.checkpoint);
        assert.deepEqual(partial.summary.lifecycle, saved.summary.lifecycle);
        assert.equal(partial.summary.participants, 1);
        assert.equal(partial.summary.capture, "incomplete");
        assert.equal(partial.summary.observation, "partial");
        const gaps = ["catalog_unavailable:snapshot_oversized"];
        const tables = yield* ArchitectureQuery.read(
          { kind: "tables", threadId: fixture.threadId },
          archives,
          [],
        );
        if (tables.kind !== "tables") throw Error("Expected tables");
        assert.deepEqual(tables.items.find((table) => table.roomId === batch.roomId)?.gaps, gaps);
        const records = yield* ArchitectureQuery.read(
          { kind: "records", threadId: fixture.threadId, tableId: partial.summary.tableId },
          archives,
          [],
        );
        if (records.kind !== "records") throw Error("Expected records");
        assert.equal(records.items.length, saved.records.length);
        assert.deepEqual(records.summary.gaps, gaps);
        const original = yield* ArchitectureQuery.read(
          {
            kind: "text",
            threadId: fixture.threadId,
            tableId: partial.summary.tableId,
            textId: "original",
          },
          archives,
          [],
        );
        if (original.kind !== "text") throw Error("Expected original");
        assert.equal(original.text, batch.original?.text);
        assert.isTrue(original.complete);
        yield* advance();
        assert.deepEqual(yield* fixture.sink.architectures(fixture.threadId), archives);
        // Unexpected optional-source failures must not gate a still-valid worker source either.
        throwCatalog = true;
        yield* advance();
        assert.deepEqual(yield* fixture.sink.architectures(fixture.threadId), archives);
        const latest = (yield* fixture.sink.nativeWorkers(fixture.providerSessionId))[0]!;
        assert.equal(latest.result, "worker result 3");
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        const child = yield* projections.getThreadRecords(worker.childThreadId!, [
          "providerThreads",
        ]);
        assert.equal(child.providerThreads[0]?.nativeMetadata?.nativeWorkerCheckpoint?.cut, 3);
      }).pipe(
        Effect.provide(
          layerTest({
            state,
            idleTimeoutMs: 30 * 60_000,
            nativeWorkers: () => ({
              ...residencyPort(() => Effect.succeed({ state: "pending" })),
              owner,
              hints: Stream.fromQueue(hints),
              readArchitectures: () =>
                Effect.tryPromise(() => readArchitectures(source, 1)).pipe(
                  Effect.mapError((error) =>
                    isArchitectureCatalogLoss(error.cause) ? error.cause : error,
                  ),
                ),
              read: (launch, expected) =>
                Effect.succeed({
                  launch,
                  expectedCheckpoint: expected,
                  checkpoint: { ...expected!, cursor: `worker-${workerCut}`, cut: workerCut },
                  records: [],
                  bindings: [],
                  more: false,
                  summary: {
                    launch,
                    execution: "completed",
                    result: "available",
                    capture: "open",
                    availability: "ready",
                    gaps: [],
                  },
                  result: {
                    status: "completed",
                    text: `worker result ${workerCut}`,
                    complete: true,
                  },
                }),
            }),
          }),
        ),
      );
    }),
);

it.effect.each([
  "opening",
  "open",
  "binding",
  "more",
  "fragment",
  "omitted",
  "terminal gap",
  "old owner",
] as const)(
  "native committed capture %s observes the current owner and actual capture obligations",
  (capture) =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      yield* Effect.gen(function* () {
        const fixture = yield* openResidencySession();
        const binding: PiNativeWorkerCheckpoint["bindings"][number] = {
          generation: 1,
          historyId: "history",
          sessionId: "session",
          state: "historical",
          headEntryId: null,
          capture: "closed",
          attemptIds: [],
          attempts: [],
          gaps: [],
        };
        yield* admitResidencyCapture(fixture, "local-worker", {
          capture: capture === "opening" || capture === "open" ? capture : "incomplete",
          ...(capture === "old owner"
            ? { capture: "open", owner: { ...residencyOwner, epoch: "old-epoch" } }
            : {}),
          bindings: ["binding", "more", "fragment", "omitted"].includes(capture) ? [binding] : [],
          bindingCursors: ["more", "fragment", "omitted"].includes(capture)
            ? [
                {
                  generation: 1,
                  cursor: "cursor",
                  cut: 1,
                  recovery: false,
                  more: capture !== "fragment",
                  ...(capture === "omitted" ? { omission: "invalid_record" } : {}),
                  ...(capture === "fragment"
                    ? {
                        fragment: {
                          id: "fragment",
                          binding: 1,
                          revision: 1,
                          epoch: "capture",
                          sequence: 1,
                          totalBytes: 10,
                          cursor: "cursor",
                        },
                      }
                    : {}),
                },
              ]
            : [],
          gaps: capture === "terminal gap" ? ["unavailable"] : [],
        });
        yield* TestClock.adjust("30 minutes");
        yield* TestClock.adjust("5 hours");
        const protects = !["omitted", "terminal gap", "old owner"].includes(capture);
        assert.equal(
          Option.isSome(yield* fixture.manager.getResident(fixture.providerSessionId)),
          protects,
        );
        if (protects) yield* fixture.manager.close(fixture.providerSessionId);
      }).pipe(
        Effect.provide(
          layerTest({
            state,
            idleTimeoutMs: 30 * 60_000,
            hasPendingBackgroundWork: Effect.die("ordinary unreadable"),
            // A terminal gap/old owner alone must not protect; those cases need all
            // applicable sources absent, not an unrelated ordinary-read failure.
            ...(["omitted", "terminal gap", "old owner"].includes(capture)
              ? { hasPendingBackgroundWork: Effect.succeed(false) }
              : {}),
            nativeWorkers: () => residencyPort(() => Effect.succeed({ state: "none" })),
          }),
        ),
      );
    }),
);

it.effect("native failed checkpoint does not hide a later committed positive", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const unreadableCheckpoint = yield* Ref.make<ThreadId | null>(null);
    yield* Effect.gen(function* () {
      const fixture = yield* openResidencySession();
      yield* admitResidencyCapture(fixture, "first-worker");
      yield* admitResidencyCapture(fixture, "positive-worker");
      const first = (yield* fixture.sink.nativeWorkers(fixture.providerSessionId))[0]!;
      yield* Ref.set(unreadableCheckpoint, first.childThreadId);
      yield* TestClock.adjust("30 minutes");
      yield* TestClock.adjust("5 hours");
      assert.isTrue(Option.isSome(yield* fixture.manager.getResident(fixture.providerSessionId)));
      yield* Ref.set(unreadableCheckpoint, null);
      yield* fixture.manager.close(fixture.providerSessionId);
    }).pipe(
      Effect.provide(
        layerTest({
          state,
          idleTimeoutMs: 30 * 60_000,
          unreadableCheckpoint,
          nativeWorkers: () =>
            residencyPort(() => Effect.succeed({ state: "unavailable", reason: "unsupported" })),
        }),
      ),
    );
  }),
);

it.effect("native unreadable required checkpoint consumes bounded uncertainty, not absence", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const failure = yield* Ref.make<"none" | "membership" | "checkpoint">("none");
    yield* Effect.gen(function* () {
      const fixture = yield* openResidencySession();
      yield* admitResidencyCapture(fixture, "unreadable-worker");
      yield* Ref.set(failure, "checkpoint");
      yield* TestClock.adjust("30 minutes");
      yield* TestClock.adjust("64 minutes");
      assert.isTrue(Option.isSome(yield* fixture.manager.getResident(fixture.providerSessionId)));
      yield* TestClock.adjust("1 minute");
      assert.isTrue(Option.isNone(yield* fixture.manager.getResident(fixture.providerSessionId)));
      yield* Ref.set(failure, "none");
      yield* TestClock.adjust("1 second"); // Existing record-write retry, not a residency retry.
    }).pipe(
      Effect.provide(
        layerTest({
          state,
          idleTimeoutMs: 30 * 60_000,
          maxIdlePinMs: 65 * 60_000,
          residencyFailure: failure,
          nativeWorkers: () => residencyPort(() => Effect.succeed({ state: "none" })),
        }),
      ),
    );
  }),
);

it.effect("native expired final round rereads newly committed local capture", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const entered = yield* Deferred.make<void>();
    const resume = yield* Deferred.make<void>();
    let calls = 0;
    yield* Effect.gen(function* () {
      const fixture = yield* openResidencySession();
      yield* TestClock.adjust("30 minutes");
      yield* TestClock.adjust("65 minutes");
      yield* Deferred.await(entered);
      yield* admitResidencyCapture(fixture, "late-worker");
      yield* Deferred.succeed(resume, undefined);
      yield* TestClock.adjust("5 hours");
      assert.isTrue(Option.isSome(yield* fixture.manager.getResident(fixture.providerSessionId)));
      yield* fixture.manager.close(fixture.providerSessionId);
    }).pipe(
      Effect.provide(
        layerTest({
          state,
          idleTimeoutMs: 30 * 60_000,
          maxIdlePinMs: 65 * 60_000,
          nativeWorkers: () =>
            residencyPort(() =>
              Effect.gen(function* () {
                calls += 1;
                if (calls === 4) {
                  yield* Deferred.succeed(entered, undefined);
                  yield* Deferred.await(resume);
                }
                return { state: "unavailable", reason: "source_unavailable" } as const;
              }),
            ),
        }),
      ),
    );
  }),
);

it.effect("native missing committed checkpoint is unknown until the bounded deadline", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    yield* Effect.gen(function* () {
      const fixture = yield* openResidencySession();
      const member = yield* admitResidencyCapture(fixture, "missing-checkpoint");
      yield* writeResidencyCheckpoint(fixture, member, "missing");
      yield* TestClock.adjust("30 minutes");
      yield* TestClock.adjust("64 minutes");
      assert.isTrue(Option.isSome(yield* fixture.manager.getResident(fixture.providerSessionId)));
      yield* TestClock.adjust("1 minute");
      assert.isTrue(Option.isNone(yield* fixture.manager.getResident(fixture.providerSessionId)));
    }).pipe(
      Effect.provide(
        layerTest({
          state,
          idleTimeoutMs: 30 * 60_000,
          maxIdlePinMs: 65 * 60_000,
          nativeWorkers: () => residencyPort(() => Effect.succeed({ state: "none" })),
        }),
      ),
    );
  }),
);

it.effect.each([
  "native",
  "ordinary",
  "ordinary unknown",
  "ordinary failure",
  "membership failure",
] as const)("native residency retains known %s work beyond four hours without Lineage", (source) =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const pending = yield* Ref.make(true);
    const failure = yield* Ref.make<"none" | "membership" | "checkpoint">(
      source === "membership failure" ? "membership" : "none",
    );
    const ordinary = Ref.get(pending).pipe(
      Effect.flatMap((value) =>
        source === "ordinary failure" && value
          ? Effect.die("ordinary unavailable")
          : Effect.succeed(value && source.startsWith("ordinary") && source !== "ordinary failure"),
      ),
    );
    yield* Effect.gen(function* () {
      const { manager, providerSessionId, sink } = yield* openResidencySession();
      if (source !== "membership failure")
        assert.lengthOf(yield* sink.nativeWorkers(providerSessionId), 0);
      yield* TestClock.adjust("30 minutes");
      assert.isTrue(Option.isSome(yield* manager.getResident(providerSessionId)));
      yield* TestClock.adjust("5 hours");
      assert.isTrue(Option.isSome(yield* manager.getResident(providerSessionId)));
      assert.equal((yield* Ref.get(state)).closeCount, 0);
      yield* Ref.set(pending, false);
      yield* Ref.set(failure, "none");
      yield* TestClock.adjust("30 minutes");
      assert.isTrue(Option.isNone(yield* manager.getResident(providerSessionId)));
    }).pipe(
      Effect.provide(
        layerTest({
          state,
          idleTimeoutMs: 30 * 60_000,
          residencyFailure: failure,
          hasPendingBackgroundWork: ordinary,
          nativeWorkers: () =>
            residencyPort(() =>
              Ref.get(pending).pipe(
                Effect.map((value) =>
                  value && source === "ordinary unknown"
                    ? ({ state: "unavailable", reason: "source_unavailable" } as const)
                    : { state: value && source !== "ordinary" ? "pending" : "none" },
                ),
              ),
            ),
        }),
      ),
    );
  }),
);

it.effect.each([
  "unsupported",
  "stale_owner",
  "source_unavailable",
  "error",
  "membership failure",
] as const)("native unknown-only %s uses one bounded episode and one final round", (source) =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const calls = yield* Ref.make(0);
    const failure = yield* Ref.make<"none" | "membership" | "checkpoint">(
      source === "membership failure" ? "membership" : "none",
    );
    yield* Effect.gen(function* () {
      const { manager, providerSessionId } = yield* openResidencySession();
      yield* TestClock.adjust("30 minutes");
      assert.isTrue(Option.isSome(yield* manager.getResident(providerSessionId)));
      yield* TestClock.adjust("64 minutes");
      assert.isTrue(Option.isSome(yield* manager.getResident(providerSessionId)));
      yield* TestClock.adjust("1 minute");
      assert.isTrue(Option.isNone(yield* manager.getResident(providerSessionId)));
      assert.equal(
        yield* Ref.get(calls),
        4,
        "expiry reuses the current round and rearm respects remaining budget",
      );
    }).pipe(
      Effect.provide(
        layerTest({
          state,
          idleTimeoutMs: 30 * 60_000,
          maxIdlePinMs: 65 * 60_000,
          residencyFailure: failure,
          nativeWorkers: () =>
            residencyPort(() =>
              Effect.gen(function* () {
                yield* Ref.update(calls, (n) => n + 1);
                if (source === "error") return yield* Effect.die("native read unavailable");
                return source === "membership failure"
                  ? ({ state: "none" } as const)
                  : ({ state: "unavailable", reason: source } as const);
              }),
            ),
        }),
      ),
    );
  }),
);

it.effect("native unknown/pending/unknown and long pending start fresh uncertainty episodes", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const observation = yield* Ref.make<ProviderAdapter.ProviderBackgroundPending>({
      state: "unavailable",
      reason: "unsupported",
    });
    yield* Effect.gen(function* () {
      const { manager, providerSessionId } = yield* openResidencySession();
      yield* TestClock.adjust("30 minutes");
      yield* Ref.set(observation, { state: "pending" });
      yield* TestClock.adjust("30 minutes");
      yield* TestClock.adjust("5 hours");
      assert.isTrue(Option.isSome(yield* manager.getResident(providerSessionId)));
      yield* Ref.set(observation, { state: "unavailable", reason: "source_unavailable" });
      yield* TestClock.adjust("30 minutes");
      yield* TestClock.adjust("64 minutes");
      assert.isTrue(Option.isSome(yield* manager.getResident(providerSessionId)));
      yield* TestClock.adjust("1 minute");
      assert.isTrue(Option.isNone(yield* manager.getResident(providerSessionId)));
    }).pipe(
      Effect.provide(
        layerTest({
          state,
          idleTimeoutMs: 30 * 60_000,
          maxIdlePinMs: 65 * 60_000,
          nativeWorkers: () => residencyPort(() => Ref.get(observation)),
        }),
      ),
    );
  }),
);

it.effect.each(["pending", "none", "unavailable"] as const)(
  "native late %s reobserves a changed owner without renewing uncertainty",
  (late) =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const entered = yield* Deferred.make<void>();
      const resume = yield* Deferred.make<void>();
      let owner = residencyOwner;
      let calls = 0;
      yield* Effect.gen(function* () {
        const { manager, providerSessionId } = yield* openResidencySession();
        yield* TestClock.adjust("30 minutes");
        yield* TestClock.adjust("60 minutes");
        yield* TestClock.adjust("5 minutes");
        yield* Deferred.await(entered);
        owner = { ...residencyOwner, epoch: "successor-epoch" };
        yield* Deferred.succeed(resume, undefined);
        yield* TestClock.adjust("0 millis");
        assert.isTrue(Option.isNone(yield* manager.getResident(providerSessionId)));
        assert.equal(
          calls,
          5,
          "changed same-runtime owner gets a responsible current round, not a fresh budget",
        );
      }).pipe(
        Effect.provide(
          layerTest({
            state,
            idleTimeoutMs: 30 * 60_000,
            maxIdlePinMs: 65 * 60_000,
            nativeWorkers: () => ({
              ...residencyPort(() =>
                Effect.gen(function* () {
                  calls += 1;
                  if (calls === 4) {
                    yield* Deferred.succeed(entered, undefined);
                    yield* Deferred.await(resume);
                    return late === "unavailable"
                      ? ({ state: late, reason: "source_unavailable" } as const)
                      : { state: late };
                  }
                  return { state: "unavailable", reason: "unsupported" } as const;
                }),
              ),
              get owner() {
                return owner;
              },
            }),
          }),
        ),
      );
    }),
);

it.effect(
  "native final round rechecks pendingWake after waiting, then releases after continuation terminal",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const pendingWake = yield* Ref.make(false);
      const entered = yield* Deferred.make<void>();
      const resume = yield* Deferred.make<void>();
      let calls = 0;
      yield* Effect.gen(function* () {
        const fixture = yield* openResidencySession();
        const { manager, providerSessionId } = fixture;
        yield* TestClock.adjust("30 minutes");
        yield* TestClock.adjust("65 minutes");
        yield* Deferred.await(entered);
        yield* Ref.set(pendingWake, true);
        yield* Deferred.succeed(resume, undefined);
        yield* TestClock.adjust("5 hours");
        assert.isTrue(Option.isSome(yield* manager.getResident(providerSessionId)));
        yield* startResidencyTurn(fixture);
        yield* Ref.set(pendingWake, false);
        const subscription = yield* fixture.runtime.subscribeEvents!;
        const terminal = yield* subscription.events.pipe(
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped,
        );
        yield* Queue.offer((yield* Ref.get(state)).eventQueues.get(String(providerSessionId))!, {
          type: "turn.terminal",
          driver: CODEX_DRIVER,
          providerThreadId: fixture.providerThread.id,
          providerTurnId: ProviderTurnId.make("continuation-terminal"),
          runOrdinal: 1,
          status: "completed",
          failure: null,
          threadDisposition: "reusable",
        });
        yield* Fiber.join(terminal);
        yield* TestClock.adjust("30 minutes");
        assert.isTrue(Option.isNone(yield* manager.getResident(providerSessionId)));
      }).pipe(
        Effect.provide(
          layerTest({
            state,
            idleTimeoutMs: 30 * 60_000,
            maxIdlePinMs: 65 * 60_000,
            hasPendingBackgroundWork: Ref.get(pendingWake),
            nativeWorkers: () =>
              residencyPort(() =>
                Effect.gen(function* () {
                  calls += 1;
                  if (calls === 4) {
                    yield* Deferred.succeed(entered, undefined);
                    yield* Deferred.await(resume);
                  }
                  return calls <= 4
                    ? ({ state: "unavailable", reason: "source_unavailable" } as const)
                    : ({ state: "none" } as const);
                }),
              ),
          }),
        ),
      );
    }),
);

it.effect(
  "native interrupted observation neither expires nor pins, and explicit detach stays authoritative",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const entered = yield* Deferred.make<void>();
      const interrupted = yield* Deferred.make<void>();
      yield* Effect.gen(function* () {
        const { manager, providerSessionId, threadId } = yield* openResidencySession();
        yield* TestClock.adjust("30 minutes");
        yield* Deferred.await(entered);
        yield* manager.detach({ providerSessionId, threadId });
        yield* Deferred.await(interrupted);
        assert.isTrue(Option.isNone(yield* manager.getResident(providerSessionId)));
        assert.equal((yield* Ref.get(state)).closeCount, 1);
      }).pipe(
        Effect.provide(
          layerTest({
            state,
            idleTimeoutMs: 30 * 60_000,
            capabilities: ExclusiveCapabilities,
            nativeWorkers: () =>
              residencyPort(() =>
                Deferred.succeed(entered, undefined).pipe(
                  Effect.andThen(Effect.never),
                  Effect.onInterrupt(() => Deferred.succeed(interrupted, undefined)),
                ),
              ),
          }),
        ),
      );
    }),
);

it.effect.each(["pending", "none", "unavailable"] as const)(
  "native late %s cannot mutate or destroy a replaced runtime",
  (late) =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const entered = yield* Deferred.make<void>();
      const resume = yield* Deferred.make<void>();
      let opens = 0;
      yield* Effect.gen(function* () {
        const fixture = yield* openResidencySession();
        const { manager, providerSessionId, threadId } = fixture;
        yield* TestClock.adjust("30 minutes");
        yield* TestClock.adjust("65 minutes");
        yield* Deferred.await(entered);
        const close = yield* manager.close(providerSessionId).pipe(Effect.forkChild);
        yield* TestClock.adjust("0 millis");
        assert.isTrue(
          Option.isNone(yield* manager.getResident(providerSessionId)),
          "old removal precedes waiting for its probe",
        );
        yield* manager.open({ threadId, providerSessionId, modelSelection, runtimePolicy });
        assert.equal((yield* Ref.get(state)).openCount, 2);
        yield* Deferred.succeed(resume, undefined);
        yield* Fiber.join(close);
        assert.isTrue(Option.isSome(yield* manager.getResident(providerSessionId)));
        yield* TestClock.adjust("30 minutes");
        assert.isTrue(Option.isNone(yield* manager.getResident(providerSessionId)));
        assert.equal((yield* Ref.get(state)).closeCount, 2);
      }).pipe(
        Effect.provide(
          layerTest({
            state,
            idleTimeoutMs: 30 * 60_000,
            maxIdlePinMs: 65 * 60_000,
            nativeWorkers: () => {
              const generation = ++opens;
              let calls = 0;
              return residencyPort(() =>
                generation === 1
                  ? Effect.uninterruptible(
                      Effect.gen(function* () {
                        calls += 1;
                        if (calls !== 4)
                          return { state: "unavailable", reason: "unsupported" } as const;
                        yield* Deferred.succeed(entered, undefined);
                        yield* Deferred.await(resume);
                        return late === "unavailable"
                          ? ({ state: late, reason: "unsupported" } as const)
                          : { state: late };
                      }),
                    )
                  : Effect.succeed({ state: "none" }),
              );
            },
          }),
        ),
      );
    }),
);

it.effect.each(["pending", "none", "unavailable"] as const)(
  "native late %s cannot expire or pin a busy turn",
  (late) =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const entered = yield* Deferred.make<void>();
      const resume = yield* Deferred.make<void>();
      let calls = 0;
      yield* Effect.gen(function* () {
        const fixture = yield* openResidencySession();
        yield* TestClock.adjust("30 minutes");
        yield* TestClock.adjust("65 minutes");
        yield* Deferred.await(entered);
        const starting = yield* startResidencyTurn(fixture).pipe(Effect.forkChild);
        yield* TestClock.adjust("0 millis");
        yield* Deferred.succeed(resume, undefined);
        yield* Fiber.join(starting);
        yield* TestClock.adjust("5 hours");
        assert.isTrue(Option.isSome(yield* fixture.manager.getResident(fixture.providerSessionId)));
        yield* terminalResidencyTurn(fixture, state);
        yield* TestClock.adjust("30 minutes");
        assert.isTrue(Option.isNone(yield* fixture.manager.getResident(fixture.providerSessionId)));
      }).pipe(
        Effect.provide(
          layerTest({
            state,
            idleTimeoutMs: 30 * 60_000,
            maxIdlePinMs: 65 * 60_000,
            nativeWorkers: () =>
              residencyPort(() =>
                Effect.uninterruptible(
                  Effect.gen(function* () {
                    calls += 1;
                    if (calls < 4) return { state: "unavailable", reason: "unsupported" } as const;
                    if (calls > 4) return { state: "none" } as const;
                    yield* Deferred.succeed(entered, undefined);
                    yield* Deferred.await(resume);
                    return late === "unavailable"
                      ? ({ state: late, reason: "unsupported" } as const)
                      : { state: late };
                  }),
                ),
              ),
          }),
        ),
      );
    }),
);

it.effect.each(["none", "turn", "source progress"] as const)(
  "native sibling unloads share one uncertainty episode; activity reset=%s",
  (activity) =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const hints = yield* Queue.unbounded<void>();
      let resultText = "initial result";
      yield* Effect.gen(function* () {
        const a = yield* openResidencySession();
        const b = yield* attachResidencySibling(a, "sibling-b");
        const busy = yield* attachResidencySibling(a, "busy-sibling");
        if (activity === "source progress") {
          const initial = yield* a.sink.stream({ eventType: "subagent.updated" }).pipe(
            Stream.filter(
              (stored) =>
                stored.event.type === "subagent.updated" &&
                stored.event.payload.result === resultText,
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
          );
          yield* admitResidencyCapture(a, "closed-progress-worker", {
            capture: "closed",
            finalSequence: 1,
          });
          yield* Fiber.join(initial);
        }
        yield* startResidencyTurn(a);
        yield* startResidencyTurn(b);
        yield* startResidencyTurn(busy);
        yield* terminalResidencyTurn(a, state);
        yield* TestClock.adjust("20 minutes");
        yield* terminalResidencyTurn(b, state);
        yield* TestClock.adjust("50 minutes"); // First unknown at 30m, sibling's at 50m.
        if (activity === "turn") yield* startResidencyTurn(busy);
        if (activity === "source progress") {
          const progressed = yield* a.sink.stream({ eventType: "subagent.updated" }).pipe(
            Stream.filter(
              (stored) =>
                stored.event.type === "subagent.updated" &&
                stored.event.payload.result === "new result",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
          );
          resultText = "new result";
          yield* Queue.offer(hints, undefined);
          yield* Fiber.join(progressed);
        }
        yield* TestClock.adjust("24 minutes");
        assert.deepEqual((yield* Ref.get(state)).unloadedNativeThreadIds, []);
        yield* TestClock.adjust("1 minute");
        if (activity !== "none") {
          assert.deepEqual((yield* Ref.get(state)).unloadedNativeThreadIds, []);
          yield* TestClock.adjust("49 minutes");
          assert.deepEqual((yield* Ref.get(state)).unloadedNativeThreadIds, []);
          yield* TestClock.adjust("1 minute");
        }
        assert.sameMembers(
          [...(yield* Ref.get(state)).unloadedNativeThreadIds],
          ["native-thread", "sibling-b"],
        );
        assert.isTrue(
          Option.isSome(yield* a.manager.getResident(a.providerSessionId)),
          "busy sibling remains independent",
        );
        assert.equal((yield* Ref.get(state)).closeCount, 0);
      }).pipe(
        Effect.provide(
          layerTest({
            state,
            idleTimeoutMs: 30 * 60_000,
            maxIdlePinMs: 65 * 60_000,
            nativeWorkers: () => ({
              ...residencyPort(() =>
                Effect.succeed({ state: "unavailable", reason: "source_unavailable" }),
              ),
              hints: Stream.fromQueue(hints),
              read: (launch, expected) =>
                Effect.succeed({
                  launch,
                  expectedCheckpoint: expected,
                  checkpoint: expected!,
                  records: [],
                  bindings: [],
                  more: false,
                  result: { status: "completed", text: resultText, complete: true },
                  summary: {
                    launch,
                    execution: "completed",
                    result: "available",
                    capture: "closed",
                    availability: "ready",
                    gaps: [],
                  },
                }),
            }),
          }),
        ),
      );
    }),
);

it.effect.each(["native", "ordinary", "capture"] as const)(
  "native %s work protects unrelated sibling unloads without a progress deadline",
  (source) =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const pending = yield* Ref.make(true);
      yield* Effect.gen(function* () {
        const a = yield* openResidencySession();
        const b = yield* attachResidencySibling(a, "sibling-b");
        const busy = yield* attachResidencySibling(a, "busy-sibling");
        yield* startResidencyTurn(a);
        yield* startResidencyTurn(b);
        yield* startResidencyTurn(busy);
        yield* terminalResidencyTurn(a, state);
        yield* terminalResidencyTurn(b, state);
        const member =
          source === "capture" ? yield* admitResidencyCapture(a, "sibling-capture") : undefined;
        yield* TestClock.adjust("6 hours");
        assert.deepEqual((yield* Ref.get(state)).unloadedNativeThreadIds, []);
        yield* Ref.set(pending, false);
        if (member !== undefined) yield* writeResidencyCheckpoint(a, member, "closed");
        yield* TestClock.adjust("30 minutes");
        assert.sameMembers(
          [...(yield* Ref.get(state)).unloadedNativeThreadIds],
          ["native-thread", "sibling-b"],
        );
        assert.isTrue(Option.isSome(yield* a.manager.getResident(a.providerSessionId)));
      }).pipe(
        Effect.provide(
          layerTest({
            state,
            idleTimeoutMs: 30 * 60_000,
            hasPendingBackgroundWorkForThread: Effect.succeed(false),
            hasPendingBackgroundWork: Ref.get(pending).pipe(
              Effect.map((value) => value && source === "ordinary"),
            ),
            nativeWorkers: () =>
              residencyPort(() =>
                Ref.get(pending).pipe(
                  Effect.map((value) =>
                    source === "native" || !value
                      ? { state: value ? "pending" : "none" }
                      : { state: "unavailable", reason: "unsupported" },
                  ),
                ),
              ),
          }),
        ),
      );
    }),
);

it.effect(
  "native release and unload compare the live pin episode after a concurrent certainty reset",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const entered = yield* Deferred.make<void>();
      const reset = yield* Deferred.make<void>();
      const resume = yield* Deferred.make<void>();
      let finalRound = false;
      let waiting = false;
      let cleared = false;
      yield* Effect.gen(function* () {
        const fixture = yield* openResidencySession();
        yield* startResidencyTurn(fixture);
        yield* terminalResidencyTurn(fixture, state); // Both destructive timers now run for this same owner.
        yield* TestClock.adjust("90 minutes");
        finalRound = true;
        yield* TestClock.adjust("5 minutes");
        yield* Deferred.await(entered);
        yield* Deferred.await(reset);
        yield* Deferred.succeed(resume, undefined);
        yield* TestClock.adjust("0 millis");
        assert.isTrue(Option.isSome(yield* fixture.manager.getResident(fixture.providerSessionId)));
        assert.deepEqual((yield* Ref.get(state)).unloadedNativeThreadIds, []);
        yield* TestClock.adjust("64 minutes");
        assert.isTrue(Option.isSome(yield* fixture.manager.getResident(fixture.providerSessionId)));
        yield* TestClock.adjust("1 minute");
        assert.isTrue(Option.isNone(yield* fixture.manager.getResident(fixture.providerSessionId)));
      }).pipe(
        Effect.provide(
          layerTest({
            state,
            idleTimeoutMs: 30 * 60_000,
            maxIdlePinMs: 65 * 60_000,
            nativeWorkers: () =>
              residencyPort(() =>
                Effect.gen(function* () {
                  if (finalRound && !waiting) {
                    waiting = true;
                    yield* Deferred.succeed(entered, undefined);
                    yield* Deferred.await(resume);
                  } else if (finalRound && !cleared) {
                    cleared = true;
                    // Signal only after the pending observer has had a chance to commit its
                    // decision, using the next scheduled clock boundary in the test.
                    yield* Deferred.succeed(reset, undefined);
                    return { state: "pending" } as const;
                  }
                  return { state: "unavailable", reason: "unsupported" } as const;
                }),
              ),
          }),
        ),
      );
    }),
);

it.effect("native cursor commits, hints and provider-session replay do not renew uncertainty", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const hints = yield* Queue.unbounded<void>();
    let cursor = 1;
    let imported = false;
    yield* Effect.gen(function* () {
      const fixture = yield* openResidencySession();
      yield* admitResidencyCapture(fixture, "terminal-worker", {
        capture: "closed",
        finalSequence: 1,
        cut: 1,
      });
      imported = true;
      yield* TestClock.adjust("30 minutes");
      yield* TestClock.adjust("30 minutes");
      const committed = yield* fixture.sink.stream({ eventType: "provider-thread.updated" }).pipe(
        Stream.filter(
          (stored) =>
            stored.event.type === "provider-thread.updated" &&
            stored.event.payload.nativeMetadata?.nativeWorkerCheckpoint?.cut === 2,
        ),
        Stream.take(1),
        Stream.runDrain,
        Effect.forkScoped,
      );
      cursor = 2;
      yield* Queue.offer(hints, undefined);
      yield* Fiber.join(committed);
      const subscription = yield* fixture.runtime.subscribeEvents!;
      const replayed = yield* subscription.events.pipe(
        Stream.take(1),
        Stream.runDrain,
        Effect.forkScoped,
      );
      yield* Queue.offer(
        (yield* Ref.get(state)).eventQueues.get(String(fixture.providerSessionId))!,
        {
          type: "provider_session.updated",
          driver: CODEX_DRIVER,
          providerSession: fixture.runtime.providerSession,
        },
      );
      yield* Fiber.join(replayed);
      yield* TestClock.adjust("34 minutes");
      assert.isTrue(Option.isSome(yield* fixture.manager.getResident(fixture.providerSessionId)));
      yield* TestClock.adjust("1 minute");
      assert.isTrue(Option.isNone(yield* fixture.manager.getResident(fixture.providerSessionId)));
    }).pipe(
      Effect.provide(
        layerTest({
          state,
          idleTimeoutMs: 30 * 60_000,
          maxIdlePinMs: 65 * 60_000,
          nativeWorkers: () => ({
            ...residencyPort(() => Effect.succeed({ state: "unavailable", reason: "unsupported" })),
            hints: Stream.fromQueue(hints),
            read: (launch, expected) =>
              imported
                ? Effect.succeed({
                    launch,
                    expectedCheckpoint: expected,
                    checkpoint: { ...expected!, cursor: `cursor-${cursor}`, cut: cursor },
                    records: [],
                    bindings: [],
                    more: false,
                    summary: {
                      launch,
                      execution: "completed",
                      result: "available",
                      capture: "closed",
                      availability: "ready",
                      gaps: [],
                    },
                  })
                : Effect.die("not ready to replay"),
          }),
        }),
      ),
    );
  }),
);

it.effect.each(["owner", "busy"] as const)(
  "native idle removal revalidates %s after the request permit wait",
  (change) =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const pause = { paused: yield* Deferred.make<void>(), resume: yield* Deferred.make<void>() };
      const flaky: FlakyReleaseWrites = {
        failing: yield* Ref.make<"none" | "session" | "session-and-requests">("none"),
        failures: yield* Queue.unbounded<void>(),
        pauseRequestWrites: pause,
      };
      let owner = residencyOwner;
      let pending = false;
      yield* Effect.gen(function* () {
        const fixture = yield* openResidencySession();
        const request = yield* makePendingRuntimeRequestEvents({
          idAllocator: fixture.ids,
          threadId: fixture.threadId,
          providerSessionId: fixture.providerSessionId,
          providerThread: fixture.providerThread,
          now: fixture.now,
        });
        yield* Queue.offerAll(
          (yield* Ref.get(state)).eventQueues.get(String(fixture.providerSessionId))!,
          request.providerEvents,
        );
        yield* Deferred.await(pause.paused);
        yield* TestClock.adjust("30 minutes"); // Native none has returned; removal is blocked on the permit.
        assert.isTrue(Option.isSome(yield* fixture.manager.getResident(fixture.providerSessionId)));
        if (change === "owner") {
          owner = { ...owner, epoch: "changed-during-permit" };
          pending = true;
          yield* Deferred.succeed(pause.resume, undefined);
        } else {
          const starting = yield* startResidencyTurn(fixture).pipe(Effect.forkChild);
          yield* TestClock.adjust("0 millis");
          yield* Deferred.succeed(pause.resume, undefined);
          yield* Fiber.join(starting);
        }
        yield* TestClock.adjust("5 hours");
        assert.isTrue(Option.isSome(yield* fixture.manager.getResident(fixture.providerSessionId)));
        assert.equal((yield* Ref.get(state)).closeCount, 0);
        pending = false;
        if (change === "busy") yield* terminalResidencyTurn(fixture, state);
        yield* TestClock.adjust("30 minutes");
        assert.isTrue(Option.isNone(yield* fixture.manager.getResident(fixture.providerSessionId)));
      }).pipe(
        Effect.provide(
          layerTest({
            state,
            idleTimeoutMs: 30 * 60_000,
            flakyReleaseWrites: flaky,
            nativeWorkers: () => ({
              ...residencyPort(() => Effect.sync(() => ({ state: pending ? "pending" : "none" }))),
              get owner() {
                return owner;
              },
            }),
          }),
        ),
      );
    }),
);

it.effect.each(["sync", "async held followup"] as const)(
  "native idle activity cancels permit-wait release without %s reobservation",
  (reader) =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const pause = { paused: yield* Deferred.make<void>(), resume: yield* Deferred.make<void>() };
      const flaky: FlakyReleaseWrites = {
        failing: yield* Ref.make<"none" | "session" | "session-and-requests">("none"),
        failures: yield* Queue.unbounded<void>(),
        pauseRequestWrites: pause,
      };
      let calls = 0;
      let successorArmed = false;
      yield* Effect.gen(function* () {
        const fixture = yield* openResidencySession();
        const { manager, providerSessionId } = fixture;
        const request = yield* makePendingRuntimeRequestEvents({
          idAllocator: fixture.ids,
          threadId: fixture.threadId,
          providerSessionId,
          providerThread: fixture.providerThread,
          now: fixture.now,
        });
        // Only enqueue the request: later item events would trigger another cancellation.
        yield* Queue.offer(
          (yield* Ref.get(state)).eventQueues.get(String(providerSessionId))!,
          request.providerEvents[0]!,
        );
        yield* Deferred.await(pause.paused);
        yield* TestClock.adjust("30 minutes");
        assert.equal(
          calls,
          1,
          "native none has completed and removal is waiting for the request permit",
        );
        // get is idle activity, not markBusy: it must join the interrupted cycle
        // before publishing its successor. Let that join start while the permit is held.
        const activity = yield* manager
          .get(providerSessionId)
          .pipe(Effect.forkChild({ startImmediately: true }));
        yield* TestClock.adjust("0 millis");
        yield* Deferred.succeed(pause.resume, undefined);
        assert.isTrue(
          Option.isSome(yield* Fiber.join(activity)),
          "idle activity must finish its cancellation join",
        );
        assert.equal(calls, 1, "the cancelled cycle must not read again or change the native pin");
        assert.equal((yield* Ref.get(state)).closeCount, 0);
        assert.isTrue(Option.isSome(yield* manager.getResident(providerSessionId)));
        successorArmed = true;
        yield* TestClock.adjust("29 minutes");
        assert.equal(calls, 1);
        yield* TestClock.adjust("1 minute");
        assert.equal(calls, 2, "the successor idle cycle still owns its evaluation");
        assert.isTrue(Option.isNone(yield* manager.getResident(providerSessionId)));
        assert.equal((yield* Ref.get(state)).closeCount, 1);
      }).pipe(
        Effect.provide(
          layerTest({
            state,
            idleTimeoutMs: 30 * 60_000,
            flakyReleaseWrites: flaky,
            // Keep the empty committed-membership source synchronous so it cannot
            // accidentally terminate the interrupted fiber before the native reader.
            nativeMembership: () => Effect.succeed([]),
            nativeWorkers: () =>
              residencyPort(() =>
                Effect.suspend(() => {
                  calls += 1;
                  return reader === "async held followup" && calls > 1 && !successorArmed
                    ? Effect.promise(
                        () => new Promise<ProviderAdapter.ProviderBackgroundPending>(() => {}),
                      )
                    : Effect.succeed({ state: "none" } as const);
                }),
              ),
          }),
        ),
      );
    }),
);

it.effect.each(["pending", "none", "unavailable"] as const)(
  "native unload discards late %s across owner change",
  (late) =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const entered = yield* Deferred.make<void>();
      const resume = yield* Deferred.make<void>();
      let owner = residencyOwner;
      let pending = true;
      let first = true;
      yield* Effect.gen(function* () {
        const a = yield* openResidencySession();
        const busy = yield* attachResidencySibling(a, "busy-sibling");
        yield* startResidencyTurn(a);
        yield* startResidencyTurn(busy);
        yield* terminalResidencyTurn(a, state);
        yield* TestClock.adjust("30 minutes");
        yield* Deferred.await(entered);
        owner = { ...owner, coordinatorSessionId: "changed-coordinator" };
        yield* Deferred.succeed(resume, undefined);
        yield* TestClock.adjust("5 hours");
        assert.deepEqual((yield* Ref.get(state)).unloadedNativeThreadIds, []);
        pending = false;
        yield* TestClock.adjust("30 minutes");
        assert.deepEqual((yield* Ref.get(state)).unloadedNativeThreadIds, ["native-thread"]);
        assert.isTrue(Option.isSome(yield* a.manager.getResident(a.providerSessionId)));
      }).pipe(
        Effect.provide(
          layerTest({
            state,
            idleTimeoutMs: 30 * 60_000,
            nativeWorkers: () => ({
              ...residencyPort(() =>
                Effect.gen(function* () {
                  if (!first) return { state: pending ? "pending" : "none" } as const;
                  first = false;
                  yield* Deferred.succeed(entered, undefined);
                  yield* Deferred.await(resume);
                  return late === "unavailable"
                    ? ({ state: late, reason: "unsupported" } as const)
                    : { state: late };
                }),
              ),
              get owner() {
                return owner;
              },
            }),
          }),
        ),
      );
    }),
);

it.effect.each(["pending", "none", "unavailable"] as const)(
  "native unload ignores late %s from a replaced runtime",
  (late) =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const entered = yield* Deferred.make<void>();
      const resume = yield* Deferred.make<void>();
      let opens = 0;
      yield* Effect.gen(function* () {
        const a = yield* openResidencySession();
        const busy = yield* attachResidencySibling(a, "busy-sibling");
        yield* startResidencyTurn(a);
        yield* startResidencyTurn(busy);
        yield* terminalResidencyTurn(a, state);
        yield* TestClock.adjust("30 minutes");
        yield* Deferred.await(entered);
        const closing = yield* a.manager.close(a.providerSessionId).pipe(Effect.forkChild);
        yield* TestClock.adjust("0 millis");
        assert.isTrue(Option.isNone(yield* a.manager.getResident(a.providerSessionId)));
        yield* a.manager.open({
          threadId: a.threadId,
          providerSessionId: a.providerSessionId,
          modelSelection,
          runtimePolicy,
        });
        yield* Deferred.succeed(resume, undefined);
        yield* Fiber.join(closing);
        assert.deepEqual((yield* Ref.get(state)).unloadedNativeThreadIds, []);
        assert.isTrue(Option.isSome(yield* a.manager.getResident(a.providerSessionId)));
        yield* TestClock.adjust("30 minutes");
        assert.isTrue(Option.isNone(yield* a.manager.getResident(a.providerSessionId)));
        assert.equal((yield* Ref.get(state)).closeCount, 2);
      }).pipe(
        Effect.provide(
          layerTest({
            state,
            idleTimeoutMs: 30 * 60_000,
            nativeWorkers: () => {
              const generation = ++opens;
              return residencyPort(() =>
                generation === 1
                  ? Effect.uninterruptible(
                      Effect.gen(function* () {
                        yield* Deferred.succeed(entered, undefined);
                        yield* Deferred.await(resume);
                        return late === "unavailable"
                          ? ({ state: late, reason: "unsupported" } as const)
                          : { state: late };
                      }),
                    )
                  : Effect.succeed({ state: "none" }),
              );
            },
          }),
        ),
      );
    }),
);

it.effect("native source interruption is cancellation, not a successful unknown pin", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const interrupted = yield* Deferred.make<void>();
    let calls = 0;
    yield* Effect.gen(function* () {
      const fixture = yield* openResidencySession();
      yield* TestClock.adjust("30 minutes");
      yield* Deferred.await(interrupted);
      yield* TestClock.adjust("5 hours");
      assert.equal(
        calls,
        1,
        "interruption cancels the evaluation rather than rearming uncertainty",
      );
      assert.isTrue(Option.isSome(yield* fixture.manager.getResident(fixture.providerSessionId)));
      yield* startResidencyTurn(fixture);
      yield* terminalResidencyTurn(fixture, state);
      yield* TestClock.adjust("30 minutes");
      yield* TestClock.adjust("64 minutes");
      assert.isTrue(Option.isSome(yield* fixture.manager.getResident(fixture.providerSessionId)));
      yield* TestClock.adjust("1 minute");
      assert.isTrue(Option.isNone(yield* fixture.manager.getResident(fixture.providerSessionId)));
    }).pipe(
      Effect.provide(
        layerTest({
          state,
          idleTimeoutMs: 30 * 60_000,
          maxIdlePinMs: 65 * 60_000,
          nativeWorkers: () =>
            residencyPort(() =>
              Effect.gen(function* () {
                calls += 1;
                if (calls === 1) {
                  yield* Deferred.succeed(interrupted, undefined);
                  return yield* Effect.interrupt;
                }
                return { state: "unavailable", reason: "unsupported" } as const;
              }),
            ),
        }),
      ),
    );
  }),
);

it.effect.each([
  "silent",
  "ordinary replay",
  "result renewal",
  "retirement retry",
  "retirement CAS",
  "closed capture",
  "timed-out close",
  "replacement owner",
  "unavailable peer",
] as const)("imports committed membership: %s", (scenario) =>
  Effect.gen(function* () {
    const renew = scenario === "result renewal";
    const replacementOwner = scenario === "replacement owner";
    const failRetirement =
      scenario === "retirement retry" || scenario === "retirement CAS" || replacementOwner;
    const closeCapture = scenario === "closed capture";
    const hangClose = scenario === "timed-out close";
    const state = yield* Ref.make(emptyState);
    const hints = yield* Queue.unbounded<void>();
    const providerEvents = yield* Queue.unbounded<
      ProviderAdapter.ProviderAdapterV2Event,
      Cause.Done
    >();
    const reads = yield* Ref.make(0);
    const ordinaryPending = yield* Ref.make(false);
    const residencyFailure = yield* Ref.make<"none" | "membership" | "checkpoint">("none");
    const backgroundPending = yield* Ref.make<ProviderAdapter.ProviderBackgroundPending>({
      state: "none",
    });
    const owner = {
      processId: "native-process",
      coordinatorSessionId: "native-coordinator",
      epoch: "epoch-1",
    };
    let currentOwner = owner;
    let ready = true;
    let resultText = "public result only";
    let failedWorker: string | undefined;
    const failing = yield* Ref.make<"none" | "session" | "session-and-requests">("none");
    const failNativeRetirement = yield* Ref.make(failRetirement);
    const failures = yield* Queue.unbounded<void>();
    const scopeCloseReached = yield* Deferred.make<void>();
    const cut = yield* Ref.make(1);
    const nativeWorkers = (): ProviderAdapter.ProviderNativeWorkersPort => {
      const sessionOwner = currentOwner;
      return {
        get owner() {
          return ready ? sessionOwner : undefined;
        },
        ownerGeneration: 1,
        hints: Stream.fromQueue(hints),
        reconcile: (launch) =>
          Effect.succeed({
            launch,
            execution: "accepted",
            result: "pending",
            capture: "opening",
            availability: "ready",
            gaps: [],
          }),
        readBackgroundPending: () => Ref.get(backgroundPending),
        hasPendingWork: (launches) =>
          Ref.get(cut).pipe(
            Effect.map((sequence) => launches.length > 0 && !(closeCapture && sequence >= 2)),
          ),
        read: (launch, expected) =>
          Effect.gen(function* () {
            if (launch.instanceId === failedWorker) return yield* new NativeSourceUnavailable();
            assert.isDefined(expected);
            yield* Ref.update(reads, (count) => count + 1);
            const sequence = yield* Ref.get(cut);
            const capture = closeCapture && sequence >= 2 ? ("closed" as const) : ("open" as const);
            const bindings: PiNativeWorkerCheckpoint["bindings"] = [
              {
                generation: 1,
                historyId: "history",
                sessionId: "worker-session",
                state: "active",
                headEntryId: null,
                capture,
                attemptIds: [],
                attempts: [],
                gaps: [],
              },
            ];
            return {
              launch,
              expectedCheckpoint: expected,
              checkpoint: {
                ...expected!,
                launch,
                owner: sessionOwner,
                ownerGeneration: 1,
                captureEpoch: "capture-epoch",
                bindings,
                bindingGeneration: 1,
                bindingCursors: closeCapture
                  ? [
                      {
                        generation: 1,
                        cursor: `cursor-${sequence}`,
                        cut: sequence,
                        recovery: false,
                        more: false,
                      },
                    ]
                  : [],
                cursor: `cursor-${sequence}`,
                cut: sequence,
                capture,
                finalSequence: capture === "closed" ? sequence : null,
              },
              summary: {
                launch,
                execution: "completed" as const,
                result: "available" as const,
                capture,
                availability: "ready" as const,
                gaps: [],
              },
              records: [],
              bindings,
              more: false,
              result: { status: "completed", text: resultText, complete: true },
            };
          }),
      };
    };
    yield* Effect.gen(function* () {
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const sink = yield* EventSink.EventSinkV2;
      const ingestor = yield* ProviderEventIngestor.ProviderEventIngestorV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const ids = yield* IdAllocator.IdAllocatorV2;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("native-manager-parent");
      const providerSessionId = yield* ids.allocate.providerSession({
        threadId,
        providerInstanceId: modelSelection.instanceId,
      });
      yield* sink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator: ids, threadId, now })],
      });
      const runtime = yield* manager.open({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });
      assert.equal(yield* Ref.get(reads), 0);
      assert.isFalse(yield* runtime.hasPendingBackgroundWork!);
      const providerThread = makeProviderThread({
        idAllocator: ids,
        threadId,
        providerSessionId,
        now,
      });
      yield* Ref.set(ordinaryPending, true);
      assert.isTrue(yield* runtime.hasPendingBackgroundWork!);
      assert.isTrue(yield* runtime.hasPendingBackgroundWorkForThread!(providerThread));
      yield* Ref.set(ordinaryPending, false);
      const launch: PiNativeWorkerLaunch = {
        source: "agent_spawn",
        instanceId: "worker",
        providerInstanceId: modelSelection.instanceId,
        providerSessionId,
        parentThreadId: threadId,
        parentProviderThreadId: providerThread.id,
        parentProviderTurnId: ProviderTurnId.make("native-manager-turn"),
        runId: RunId.make("native-manager-run"),
        runAttemptId: RunAttemptId.make("native-manager-attempt"),
        toolNodeId: NodeId.make("native-manager-tool"),
        coordinatorSessionId: owner.coordinatorSessionId,
        toolCallId: "spawn-call",
      };
      yield* sink.write({
        events: [
          {
            id: yield* ids.allocate.event({ threadId }),
            type: "run.created",
            threadId,
            occurredAt: now,
            payload: {
              id: launch.runId,
              threadId,
              ordinal: 1,
              providerInstanceId: modelSelection.instanceId,
              modelSelection,
              providerThreadId: providerThread.id,
              userMessageId: MessageId.make("native-manager-message"),
              rootNodeId: NodeId.make("native-manager-root"),
              activeAttemptId: launch.runAttemptId,
              status: "running",
              requestedAt: now,
              startedAt: now,
              completedAt: null,
              checkpointId: null,
              contextHandoffId: null,
            },
          },
        ],
      });
      const checkpoint: PiNativeWorkerCheckpoint = {
        launch,
        owner,
        ownerGeneration: 1,
        captureEpoch: null,
        bindings: [],
        bindingGeneration: null,
        bindingCursors: [],
        cursor: null,
        cut: 0,
        recovery: false,
        capture: "opening",
        finalSequence: null,
        gaps: [],
      };
      const summary: PiNativeWorkerSummary = {
        launch,
        execution: "accepted",
        result: "pending",
        capture: "opening",
        availability: "ready",
        gaps: [],
      };
      const imported = yield* sink.stream({ eventType: "provider-thread.updated" }).pipe(
        Stream.filter(
          (stored) =>
            stored.event.type === "provider-thread.updated" &&
            stored.event.payload.nativeMetadata?.nativeWorkerCheckpoint?.cut === 1,
        ),
        Stream.take(1),
        Stream.runDrain,
        Effect.forkScoped,
      );
      yield* ingestor.ingestNormalized({
        providerSessionId,
        providerInstanceId: modelSelection.instanceId,
        threadId,
        event: {
          type: "turn_item.updated",
          driver: ProviderDriverKind.make("pi"),
          turnItem: {
            id: TurnItemId.make("native-manager-launch"),
            threadId,
            runId: launch.runId,
            nodeId: launch.toolNodeId,
            providerThreadId: providerThread.id,
            providerTurnId: launch.parentProviderTurnId,
            nativeItemRef: null,
            parentItemId: null,
            ordinal: 1,
            status: "completed",
            title: null,
            startedAt: now,
            completedAt: now,
            updatedAt: now,
            type: "dynamic_tool",
            toolName: "agent_spawn",
            input: {},
            output: {},
            nativeWorkerLaunch: launch,
          },
        },
        nativeWorkerAdmission: { summary, checkpoint },
      });
      yield* Fiber.join(imported);
      let member = (yield* sink.nativeWorkers(providerSessionId))[0]!;
      const child = yield* projections.getThreadProjection(member.childThreadId!);
      assert.equal(child.messages.length, 0); // Public result never fabricates transcript text.
      assert.equal(
        child.providerThreads[0]?.nativeMetadata?.nativeWorkerCheckpoint?.capture,
        "open",
      );
      assert.equal((yield* sink.nativeWorkers(providerSessionId))[0]?.result, "public result only");
      assert.isTrue(yield* runtime.hasPendingBackgroundWork!);
      assert.isFalse(
        yield* runtime.hasPendingBackgroundWorkForThread!({
          ...providerThread,
          id: ids.derive.providerThread({ driver: CODEX_DRIVER, nativeThreadId: "unrelated" }),
        }),
      );
      if (scenario === "unavailable peer") {
        const peerLaunch = {
          ...launch,
          instanceId: "peer-worker",
          toolNodeId: NodeId.make("peer-tool"),
          toolCallId: "peer-spawn-call",
        };
        yield* ingestor.ingestNormalized({
          providerSessionId,
          providerInstanceId: modelSelection.instanceId,
          threadId,
          event: {
            type: "turn_item.updated",
            driver: ProviderDriverKind.make("pi"),
            turnItem: {
              id: TurnItemId.make("peer-launch"),
              threadId,
              runId: launch.runId,
              nodeId: peerLaunch.toolNodeId,
              providerThreadId: providerThread.id,
              providerTurnId: launch.parentProviderTurnId,
              nativeItemRef: null,
              parentItemId: null,
              ordinal: 2,
              status: "completed",
              title: null,
              startedAt: now,
              completedAt: now,
              updatedAt: now,
              type: "dynamic_tool",
              toolName: "agent_spawn",
              input: {},
              output: {},
              nativeWorkerLaunch: peerLaunch,
            },
          },
          nativeWorkerAdmission: {
            summary: { ...summary, launch: peerLaunch },
            checkpoint: { ...checkpoint, launch: peerLaunch },
          },
        });
        const members = yield* sink.nativeWorkers(providerSessionId);
        assert.lengthOf(members, 2);
        // Fail the first read in durable traversal order, not a guessed ID order.
        failedWorker = members[0]!.nativeWorker!.launch.instanceId;
        member = members[1]!;
      }
      const released = yield* sink.stream({ eventType: "provider-thread.updated" }).pipe(
        Stream.filter(
          (stored) =>
            stored.event.type === "provider-thread.updated" &&
            stored.event.threadId === member.childThreadId &&
            stored.event.payload.nativeMetadata?.nativeWorkerCheckpoint?.ownerGeneration === 2,
        ),
        Stream.take(1),
        Stream.runDrain,
        Effect.forkScoped,
      );
      yield* TestClock.adjust("30 minutes"); // Open committed capture protects residency.
      const cursorCommitted = yield* sink.stream({ eventType: "provider-thread.updated" }).pipe(
        Stream.filter(
          (stored) =>
            stored.event.type === "provider-thread.updated" &&
            stored.event.payload.nativeMetadata?.nativeWorkerCheckpoint?.cut === 2,
        ),
        Stream.take(1),
        Stream.runDrain,
        Effect.forkScoped,
      );
      yield* Ref.set(cut, 2);
      yield* Queue.offer(hints, undefined); // Cursor-only progress must not renew.
      yield* Fiber.join(cursorCommitted);
      if (scenario === "ordinary replay") {
        yield* TestClock.adjust("2 hours");
        const subscription = yield* runtime.subscribeEvents!;
        const replayed = yield* subscription.events.pipe(
          Stream.filter((event) => event.type === "provider_session.updated"),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped,
        );
        yield* Queue.offer(providerEvents, {
          type: "provider_session.updated",
          driver: CODEX_DRIVER,
          providerSession: runtime.providerSession,
        });
        yield* Fiber.join(replayed);
      }
      if (closeCapture) {
        assert.isFalse(yield* runtime.hasPendingBackgroundWork!);
        yield* TestClock.adjust("30 minutes");
      } else if (renew) {
        yield* TestClock.adjust("2 hours");
        yield* Ref.set(backgroundPending, { state: "unavailable", reason: "source_unavailable" });
        yield* Ref.set(residencyFailure, "checkpoint");
        yield* TestClock.adjust("30 minutes"); // Unknown episode begins at 3h.
        yield* TestClock.adjust("2 hours");
        yield* Ref.set(residencyFailure, "none");
        const resultCommitted = yield* sink.stream({ eventType: "subagent.updated" }).pipe(
          Stream.filter(
            (stored) =>
              stored.event.type === "subagent.updated" &&
              stored.event.payload.result === "new committed result",
          ),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped,
        );
        resultText = "new committed result";
        yield* Ref.set(cut, 3);
        yield* Queue.offer(hints, undefined);
        yield* Fiber.join(resultCommitted);
        yield* Ref.set(residencyFailure, "checkpoint");
        yield* TestClock.adjust("210 minutes"); // Beyond old 7h deadline, before renewed 9.5h deadline.
        assert.equal((yield* manager.getResident(providerSessionId))._tag, "Some");
        yield* Ref.set(residencyFailure, "none");
      } else {
        yield* TestClock.adjust(scenario === "ordinary replay" ? "2 hours" : "4 hours");
      }
      if (!closeCapture) {
        // Open capture is positive evidence, not an expiring uncertainty pin.
        assert.equal((yield* manager.getResident(providerSessionId))._tag, "Some");
        yield* manager.close(providerSessionId).pipe(Effect.forkScoped);
      }
      if (hangClose) {
        yield* Deferred.await(scopeCloseReached);
        yield* TestClock.adjust("30 seconds");
      }
      if (failRetirement) {
        yield* Queue.take(failures);
        yield* Ref.set(failNativeRetirement, false);
        if (replacementOwner) {
          currentOwner = { ...owner, epoch: "epoch-replacement" };
          ready = true;
          yield* manager.open({ threadId, providerSessionId, modelSelection, runtimePolicy });
          const newLaunch = {
            ...launch,
            instanceId: "new-worker",
            toolNodeId: NodeId.make("new-tool"),
            toolCallId: "new-spawn-call",
          };
          yield* ingestor.ingestNormalized({
            providerSessionId,
            providerInstanceId: modelSelection.instanceId,
            threadId,
            event: {
              type: "turn_item.updated",
              driver: ProviderDriverKind.make("pi"),
              turnItem: {
                id: TurnItemId.make("replacement-launch"),
                threadId,
                runId: launch.runId,
                nodeId: newLaunch.toolNodeId,
                providerThreadId: providerThread.id,
                providerTurnId: launch.parentProviderTurnId,
                nativeItemRef: null,
                parentItemId: null,
                ordinal: 2,
                status: "completed",
                title: null,
                startedAt: now,
                completedAt: now,
                updatedAt: now,
                type: "dynamic_tool",
                toolName: "agent_spawn",
                input: {},
                output: {},
                nativeWorkerLaunch: newLaunch,
              },
            },
            nativeWorkerAdmission: {
              summary: { ...summary, launch: newLaunch },
              checkpoint: { ...checkpoint, launch: newLaunch, owner: currentOwner },
            },
          });
        }
        yield* TestClock.adjust("1 second");
      }
      yield* Fiber.join(released);
      assert.isUndefined(runtime.nativeWorkers?.owner);
      assert.equal(
        (yield* manager.getResident(providerSessionId))._tag,
        replacementOwner ? "Some" : "None",
      );
      const retired = yield* projections.getThreadProjection(member.childThreadId!);
      const finalCheckpoint = retired.providerThreads[0]!.nativeMetadata!.nativeWorkerCheckpoint!;
      assert.equal(finalCheckpoint.ownerGeneration, 2);
      assert.equal(finalCheckpoint.cut, renew ? 3 : 2); // Release reread the last committed checkpoint.
      assert.equal(finalCheckpoint.capture, closeCapture ? "closed" : "incomplete");
      assert.deepEqual(finalCheckpoint.gaps, closeCapture ? [] : ["stale_owner"]);
      assert.equal((yield* Ref.get(state)).closeCount, hangClose ? 0 : 1);
      if (replacementOwner) {
        const newMember = (yield* sink.nativeWorkers(providerSessionId)).find(
          (candidate) => candidate.nativeWorker?.launch.instanceId === "new-worker",
        )!;
        const newer = yield* projections.getThreadProjection(newMember.childThreadId!);
        const newerCheckpoint = newer.providerThreads[0]!.nativeMetadata!.nativeWorkerCheckpoint!;
        assert.equal(newerCheckpoint.owner.epoch, "epoch-replacement");
        assert.equal(newerCheckpoint.ownerGeneration, 1);
        assert.notEqual(newerCheckpoint.capture, "incomplete");
        assert.equal(
          (yield* projections.getThreadProjection(threadId)).providerSessions.at(-1)?.status,
          "ready",
        );
      }
    }).pipe(
      Effect.provide(
        layerTest({
          state,
          idleTimeoutMs: 30 * 60_000,
          maxIdlePinMs: 4 * 60 * 60_000,
          hasPendingBackgroundWork: Ref.get(ordinaryPending),
          residencyFailure,
          hasPendingBackgroundWorkForThread: Ref.get(ordinaryPending),
          nativeWorkers,
          providerEvents,
          onScopeClose: Effect.sync(() => {
            ready = false;
          }),
          hangSessionScopeClose: hangClose,
          scopeCloseReached,
          ...(failRetirement
            ? {
                flakyReleaseWrites: {
                  failing,
                  failures,
                  failNativeRetirement,
                  rejectNativeRetirement: scenario === "retirement CAS",
                },
              }
            : {}),
        }),
      ),
      Effect.provideService(HttpServer.HttpServer, fakeHttpServer),
    );
  }),
);

const fakeEnvironment = ServerEnvironment.ServerEnvironment.of({
  getEnvironmentId: Effect.succeed(EnvironmentId.make("environment-provider-session-manager")),
  getDescriptor: Effect.die("unused"),
});

const layerTestMcpRegistry = Layer.effect(
  McpSessionRegistry.McpSessionRegistry,
  McpSessionRegistry.__testing.make(),
).pipe(
  Layer.provide(Layer.succeed(HttpServer.HttpServer, fakeHttpServer)),
  Layer.provide(Layer.succeed(ServerEnvironment.ServerEnvironment, fakeEnvironment)),
  Layer.provide(NodeServices.layer),
);

const layerPausingMcpRegistry = (pause: {
  readonly armed: Ref.Ref<boolean>;
  readonly paused: Deferred.Deferred<void>;
}) =>
  Layer.effect(
    McpSessionRegistry.McpSessionRegistry,
    Effect.gen(function* () {
      const delegate = yield* McpSessionRegistry.McpSessionRegistry;
      return McpSessionRegistry.McpSessionRegistry.of({
        ...delegate,
        resolve: (rawToken) =>
          Ref.getAndSet(pause.armed, false).pipe(
            Effect.flatMap((armed) =>
              armed
                ? Deferred.succeed(pause.paused, undefined).pipe(Effect.andThen(Effect.never))
                : delegate.resolve(rawToken),
            ),
          ),
      });
    }),
  ).pipe(Layer.provide(layerTestMcpRegistry));

function makeBrowserAccessProject(projectId: ProjectId): Project {
  return {
    id: projectId,
    title: "Browser access project",
    workspaceRoot: process.cwd(),
    repositoryIdentity: null,
    faviconPath: null,
    projectIcon: null,
    defaultModelSelection: null,
    defaultThreadEnvMode: null,
    autoPull: false,
    scripts: [],
    createdAt: "2026-01-01T00:00:00.000Z",
    updatedAt: "2026-01-01T00:00:00.000Z",
    deletedAt: null,
  };
}

function runBrowserAccessScenario(input: {
  readonly enableAgentBrowserAccess: boolean;
  readonly projectOverride: boolean;
  readonly deviceOverride?: boolean;
  readonly createThread?: boolean;
  readonly projectExists?: boolean;
}) {
  return Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const mcpConfigs = yield* Ref.make<
      ReadonlyArray<McpProviderSession.McpProviderSessionConfig | undefined>
    >([]);
    const projectId = ProjectId.make("project-provider-session-manager-browser-access");
    const threadId = ThreadId.make("thread-provider-session-manager-browser-access");
    const layerProjectService = Layer.mock(ProjectService.ProjectService)({
      getById: (requestedProjectId) =>
        Effect.succeed(
          input.projectExists === false
            ? Option.none()
            : Option.some(makeBrowserAccessProject(requestedProjectId)),
        ),
    });

    yield* Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const now = yield* DateTime.now;
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });
      if (input.createThread !== false) {
        yield* eventSink.write({
          events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now, projectId })],
        });
      }
      yield* manager
        .open({ threadId, providerSessionId, modelSelection, runtimePolicy })
        .pipe(Effect.ignore);
    }).pipe(
      Effect.provide(
        layerTest({
          state,
          idleTimeoutMs: 1_000,
          mcpConfigs,
          projectServiceLayer: layerProjectService,
          serverSettingsLayer: ServerSettings.layerTest({
            enableAgentBrowserAccess: input.enableAgentBrowserAccess,
            projectSettingsOverrides: {
              [projectId]: {
                enableAgentBrowserAccess: input.projectOverride,
                ...(input.deviceOverride === undefined
                  ? {}
                  : { enableAgentDeviceAccess: input.deviceOverride }),
              },
            },
          }),
        }),
      ),
    );

    return (yield* Ref.get(mcpConfigs))[0];
  });
}

function makePendingRuntimeRequestEvents(input: {
  readonly idAllocator: IdAllocator.IdAllocatorV2["Service"];
  readonly threadId: ThreadId;
  readonly providerSessionId: ProviderSessionId;
  readonly providerThread: OrchestrationV2ProviderThread;
  readonly now: DateTime.Utc;
}) {
  return Effect.gen(function* () {
    const requestId = yield* input.idAllocator.allocate.runtimeRequest({
      driver: CODEX_DRIVER,
      nativeRequestId: "pending-approval",
    });
    const nodeId = input.idAllocator.derive.approvalNode({ requestId });
    const node = {
      id: nodeId,
      threadId: input.threadId,
      runId: null,
      parentNodeId: null,
      rootNodeId: nodeId,
      kind: "approval_request" as const,
      status: "waiting" as const,
      countsForRun: false,
      providerThreadId: input.providerThread.id,
      providerTurnId: null,
      nativeItemRef: null,
      runtimeRequestId: requestId,
      checkpointScopeId: null,
      startedAt: input.now,
      completedAt: null,
    };
    const request = {
      id: requestId,
      nodeId,
      providerTurnId: null,
      nativeRequestRef: {
        driver: CODEX_DRIVER,
        nativeId: "pending-approval",
        strength: "strong" as const,
      },
      kind: "command" as const,
      status: "pending" as const,
      responseCapability: {
        type: "live" as const,
        providerSessionId: input.providerSessionId,
      },
      createdAt: input.now,
      resolvedAt: null,
    };
    const turnItem = {
      id: input.idAllocator.derive.approvalTurnItem({ requestId }),
      threadId: input.threadId,
      runId: null,
      nodeId,
      providerThreadId: input.providerThread.id,
      providerTurnId: null,
      nativeItemRef: null,
      parentItemId: null,
      ordinal: 1,
      status: "waiting" as const,
      title: null,
      startedAt: input.now,
      completedAt: null,
      updatedAt: input.now,
      type: "approval_request" as const,
      requestId,
      requestKind: "command" as const,
    };
    const events = [
      {
        id: yield* input.idAllocator.allocate.event({
          threadId: input.threadId,
          providerSessionId: input.providerSessionId,
        }),
        type: "node.updated" as const,
        threadId: input.threadId,
        nodeId,
        driver: CODEX_DRIVER,
        occurredAt: input.now,
        payload: node,
      },
      {
        id: yield* input.idAllocator.allocate.event({
          threadId: input.threadId,
          providerSessionId: input.providerSessionId,
        }),
        type: "runtime-request.updated" as const,
        threadId: input.threadId,
        nodeId,
        driver: CODEX_DRIVER,
        occurredAt: input.now,
        payload: request,
      },
      {
        id: yield* input.idAllocator.allocate.event({
          threadId: input.threadId,
          providerSessionId: input.providerSessionId,
        }),
        type: "turn-item.updated" as const,
        threadId: input.threadId,
        nodeId,
        driver: CODEX_DRIVER,
        occurredAt: input.now,
        payload: turnItem,
      },
    ] satisfies ReadonlyArray<OrchestrationV2DomainEvent>;
    const providerEvents = [
      {
        type: "runtime_request.updated" as const,
        driver: CODEX_DRIVER,
        threadId: input.threadId,
        runtimeRequest: request,
      },
      {
        type: "node.updated" as const,
        driver: CODEX_DRIVER,
        node,
      },
      {
        type: "turn_item.updated" as const,
        driver: CODEX_DRIVER,
        turnItem,
      },
    ] satisfies ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event>;
    return { events, providerEvents, requestId, nodeId };
  });
}

it.effect("ProviderSessionManagerV2 opens independent sessions concurrently", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const openStartedCount = yield* Ref.make(0);
    const firstOpenStarted = yield* Deferred.make<void>();
    const secondOpenStarted = yield* Deferred.make<void>();
    const releaseOpens = yield* Deferred.make<void>();
    const beforeOpen = () =>
      Effect.gen(function* () {
        const openNumber = yield* Ref.modify(openStartedCount, (count) => [count + 1, count + 1]);
        yield* Deferred.succeed(openNumber === 1 ? firstOpenStarted : secondOpenStarted, undefined);
        yield* Deferred.await(releaseOpens);
      });

    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const now = yield* DateTime.now;
      const firstThreadId = ThreadId.make("thread-provider-session-manager-concurrent-a");
      const secondThreadId = ThreadId.make("thread-provider-session-manager-concurrent-b");
      const firstProviderSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId: firstThreadId,
      });
      const secondProviderSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId: secondThreadId,
      });

      yield* eventSink.write({
        events: [
          yield* makeThreadCreatedEvent({ idAllocator, threadId: firstThreadId, now }),
          yield* makeThreadCreatedEvent({ idAllocator, threadId: secondThreadId, now }),
        ],
      });
      const firstFiber = yield* manager
        .open({
          threadId: firstThreadId,
          providerSessionId: firstProviderSessionId,
          modelSelection,
          runtimePolicy,
        })
        .pipe(Effect.forkScoped);
      yield* Deferred.await(firstOpenStarted);
      const secondFiber = yield* manager
        .open({
          threadId: secondThreadId,
          providerSessionId: secondProviderSessionId,
          modelSelection,
          runtimePolicy,
        })
        .pipe(Effect.forkScoped);

      yield* Deferred.await(secondOpenStarted);
      assert.equal(yield* Ref.get(openStartedCount), 2);
      yield* Deferred.succeed(releaseOpens, undefined);
      const [firstRuntime, secondRuntime] = yield* Effect.all([
        Fiber.join(firstFiber),
        Fiber.join(secondFiber),
      ]);
      assert.notStrictEqual(firstRuntime, secondRuntime);
      assert.equal((yield* Ref.get(state)).openCount, 2);
    });

    yield* effect.pipe(
      Effect.provide(
        layerTest({
          state,
          idleTimeoutMs: 60_000,
          beforeOpen,
        }),
      ),
    );
  }),
);

it.effect("ProviderSessionManagerV2 closes every live session for a provider instance", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const now = yield* DateTime.now;
      const firstThreadId = ThreadId.make("thread-provider-session-manager-logout-a");
      const secondThreadId = ThreadId.make("thread-provider-session-manager-logout-b");
      const firstProviderSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId: firstThreadId,
      });
      const secondProviderSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId: secondThreadId,
      });

      yield* eventSink.write({
        events: [
          yield* makeThreadCreatedEvent({ idAllocator, threadId: firstThreadId, now }),
          yield* makeThreadCreatedEvent({ idAllocator, threadId: secondThreadId, now }),
        ],
      });
      yield* manager.open({
        threadId: firstThreadId,
        providerSessionId: firstProviderSessionId,
        modelSelection,
        runtimePolicy,
      });
      yield* manager.open({
        threadId: secondThreadId,
        providerSessionId: secondProviderSessionId,
        modelSelection,
        runtimePolicy,
      });

      yield* manager.closeInstance(modelSelection.instanceId);

      assert.isTrue(Option.isNone(yield* manager.get(firstProviderSessionId)));
      assert.isTrue(Option.isNone(yield* manager.get(secondProviderSessionId)));
      assert.equal((yield* Ref.get(state)).closeCount, 2);
    });

    yield* effect.pipe(
      Effect.provide(
        layerTest({
          state,
          idleTimeoutMs: 60_000,
        }),
      ),
    );
  }),
);

it.effect("ProviderSessionManagerV2 records provider session and turn metrics", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("thread-provider-session-manager-metrics");
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });
      const providerThread = makeProviderThread({ idAllocator, threadId, providerSessionId, now });
      const runId = idAllocator.derive.run({ threadId, ordinal: 1 });
      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });

      const runtime = yield* manager.open({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });
      yield* runtime.startTurn({
        appThread: (yield* projectionStore.getThreadProjection(threadId)).thread,
        threadId,
        runId,
        runOrdinal: 1,
        providerTurnOrdinal: 1,
        attemptId: idAllocator.derive.runAttempt({ runId, attemptOrdinal: 1 }),
        rootNodeId: idAllocator.derive.rootNode({ runId }),
        providerThread,
        message: {
          createdBy: "user",
          creationSource: "web",
          messageId: yield* idAllocator.allocate.message({ threadId, ordinal: 1 }),
          text: "hello",
          attachments: [],
        },
        modelSelection,
        runtimePolicy,
      });
      yield* runtime.interruptTurn({
        providerThread,
        providerTurnId: idAllocator.derive.providerTurn({
          driver: CODEX_DRIVER,
          nativeTurnId: "native-turn",
        }),
      });
      yield* manager.close(providerSessionId);

      const snapshots = yield* Metric.snapshot;
      const has = (id: string, attributes: Readonly<Record<string, string>>) =>
        snapshots.some(
          (snapshot) =>
            snapshot.id === id &&
            Object.entries(attributes).every(
              ([key, value]) => snapshot.attributes?.[key] === value,
            ),
        );
      assert.isTrue(
        has("t3_provider_sessions_total", {
          provider: "codex",
          operation: "open",
          outcome: "success",
        }),
      );
      assert.isTrue(
        has("t3_provider_sessions_total", {
          provider: "codex",
          operation: "release",
          reason: "manual_shutdown",
          outcome: "success",
        }),
      );
      assert.isTrue(
        has("t3_provider_turns_total", {
          provider: "codex",
          operation: "send",
          modelFamily: "gpt",
          outcome: "success",
        }),
      );
      assert.isTrue(has("t3_provider_turn_duration", { provider: "codex", operation: "send" }));
      assert.isTrue(
        has("t3_provider_turns_total", {
          provider: "codex",
          operation: "interrupt",
          outcome: "success",
        }),
      );
    });

    yield* effect.pipe(
      Effect.provide(layerTest({ state, idleTimeoutMs: 60_000 })),
      // A private registry keeps other tests' provider metrics out of the assertions.
      Effect.provideService(Metric.MetricRegistry, new Map()),
    );
  }),
);

it.effect("ProviderSessionManagerV2 opens a duplicate session only once", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const openStartedCount = yield* Ref.make(0);
    const firstOpenStarted = yield* Deferred.make<void>();
    const releaseOpen = yield* Deferred.make<void>();
    const beforeOpen = () =>
      Ref.updateAndGet(openStartedCount, (count) => count + 1).pipe(
        Effect.tap(() => Deferred.succeed(firstOpenStarted, undefined)),
        Effect.andThen(Deferred.await(releaseOpen)),
      );

    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("thread-provider-session-manager-single-flight");
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });
      const open = manager.open({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });

      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      const firstFiber = yield* open.pipe(Effect.forkScoped);
      yield* Deferred.await(firstOpenStarted);
      const secondFiber = yield* open.pipe(Effect.forkScoped);
      yield* Effect.yieldNow;
      yield* Effect.yieldNow;
      assert.equal(yield* Ref.get(openStartedCount), 1);

      yield* Deferred.succeed(releaseOpen, undefined);
      const [firstRuntime, secondRuntime] = yield* Effect.all([
        Fiber.join(firstFiber),
        Fiber.join(secondFiber),
      ]);
      assert.strictEqual(firstRuntime, secondRuntime);
      assert.equal((yield* Ref.get(state)).openCount, 1);
    });

    yield* effect.pipe(
      Effect.provide(
        layerTest({
          state,
          idleTimeoutMs: 60_000,
          beforeOpen,
        }),
      ),
    );
  }),
);

it.effect(
  "ProviderSessionManagerV2 starts a thread's session without its last agent's OOM kill",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const owner = ThreadId.make("thread-provider-session-manager-oom-owner");
      const joiner = ThreadId.make("thread-provider-session-manager-oom-joiner");
      // Both threads' previous agents were OOM-killed. A new session may run
      // without a scope (Cursor, OpenCode, a shared Codex session), so it must
      // not inherit that answer.
      const oomKilledThreads = new Set<string>([owner, joiner]);
      const agentScope = {
        ...AgentScope.defaultValue(),
        oomKilled: (id: string) => Effect.sync(() => oomKilledThreads.has(id)),
        clear: (id: string) => Effect.sync(() => void oomKilledThreads.delete(id)),
      };

      const effect = Effect.gen(function* () {
        const eventSink = yield* EventSink.EventSinkV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const now = yield* DateTime.now;
        const providerSessionId = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId: owner,
        });
        yield* eventSink.write({
          events: [
            yield* makeThreadCreatedEvent({ idAllocator, threadId: owner, now }),
            yield* makeThreadCreatedEvent({ idAllocator, threadId: joiner, now }),
          ],
        });
        yield* manager.open({ threadId: owner, providerSessionId, modelSelection, runtimePolicy });
        assert.isFalse(yield* agentScope.oomKilled(owner));

        yield* manager.open({ threadId: joiner, providerSessionId, modelSelection, runtimePolicy });
        assert.isFalse(yield* agentScope.oomKilled(joiner));

        // Reopening a session the thread already uses keeps its answer.
        oomKilledThreads.add(owner);
        yield* manager.open({ threadId: owner, providerSessionId, modelSelection, runtimePolicy });
        assert.isTrue(yield* agentScope.oomKilled(owner));
        assert.equal((yield* Ref.get(state)).openCount, 1);
      });

      yield* effect.pipe(
        Effect.provide(layerTest({ state, idleTimeoutMs: 60_000 })),
        Effect.provideService(AgentScope, agentScope),
      );
    }),
);

it.effect("ProviderSessionManagerV2 cleans up an open interrupted mid-handshake", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const mcpConfigs = yield* Ref.make<
      ReadonlyArray<McpProviderSession.McpProviderSessionConfig | undefined>
    >([]);
    const handshakeStarted = yield* Deferred.make<void>();
    const holdHandshake = yield* Ref.make(true);
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const registry = yield* McpSessionRegistry.McpSessionRegistry;
      const mcpSessions = yield* McpProviderSessions.McpProviderSessions;
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("thread-provider-session-manager-interrupted-open");
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });
      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });

      const opening = yield* manager
        .open({ threadId, providerSessionId, modelSelection, runtimePolicy })
        .pipe(Effect.forkScoped);
      yield* Deferred.await(handshakeStarted);
      const issued = (yield* Ref.get(mcpConfigs)).at(-1);
      const token = issued?.authorizationHeader.replace(/^Bearer\s+/, "");
      assert.isDefined(token);
      assert.isDefined(yield* registry.resolve(token!));

      // A Stop while the provider is still starting.
      yield* Fiber.interrupt(opening);

      // The process started for this open is stopped, and the credential minted
      // for it revoked.
      assert.equal((yield* Ref.get(state)).closeCount, 1);
      assert.isUndefined(yield* registry.resolve(token!));
      assert.isUndefined(yield* mcpSessions.read(threadId));
      assert.isTrue(Option.isNone(yield* manager.get(providerSessionId)));

      // Nothing of the interrupted open is left behind: the next open starts a
      // fresh process with a fresh credential that a later release revokes,
      // which a leaked reservation would prevent.
      yield* Ref.set(holdHandshake, false);
      yield* manager.open({ threadId, providerSessionId, modelSelection, runtimePolicy });
      const replacement = (yield* Ref.get(mcpConfigs)).at(-1);
      const replacementToken = replacement?.authorizationHeader.replace(/^Bearer\s+/, "");
      assert.isDefined(replacementToken);
      assert.notEqual(replacementToken, token);
      const projection = yield* projectionStore.getThreadProjection(threadId);
      assert.equal(projection.providerSessions.at(-1)?.status, "ready");

      yield* manager.close(providerSessionId);
      assert.isUndefined(yield* registry.resolve(replacementToken!));
    });

    yield* effect.pipe(
      Effect.provide(
        layerTest({
          state,
          idleTimeoutMs: 60_000,
          mcpConfigs,
          beforeOpen: () =>
            Ref.get(holdHandshake).pipe(
              Effect.flatMap((hold) =>
                hold
                  ? Deferred.succeed(handshakeStarted, undefined).pipe(Effect.andThen(Effect.never))
                  : Effect.void,
              ),
            ),
          // The process is spawned before the handshake that is interrupted.
          spawnBeforeOpen: true,
        }),
      ),
    );
  }),
);

it.effect("ProviderSessionManagerV2 cleans up an interrupted open whose scope close hangs", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const mcpConfigs = yield* Ref.make<
      ReadonlyArray<McpProviderSession.McpProviderSessionConfig | undefined>
    >([]);
    const handshakeStarted = yield* Deferred.make<void>();
    const scopeCloseReached = yield* Deferred.make<void>();
    const releaseRetry = yield* Deferred.make<void>();
    yield* Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const registry = yield* McpSessionRegistry.McpSessionRegistry;
      const mcpSessions = yield* McpProviderSessions.McpProviderSessions;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("thread-provider-session-manager-interrupted-hung-open");
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });
      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });

      const opening = yield* manager
        .open({ threadId, providerSessionId, modelSelection, runtimePolicy })
        .pipe(Effect.forkChild);
      yield* Deferred.await(handshakeStarted);
      const issued = (yield* Ref.get(mcpConfigs)).at(-1);
      const token = issued?.authorizationHeader.replace(/^Bearer\s+/, "");
      assert.isDefined(token);

      // The interrupt starts cleanup; the scope close then never finishes.
      const interrupter = yield* Fiber.interrupt(opening).pipe(Effect.forkChild);
      yield* Deferred.await(scopeCloseReached);

      // The session cleanup already ran, ahead of the stuck close.
      assert.isUndefined(yield* registry.resolve(token!));
      assert.isUndefined(yield* mcpSessions.read(threadId));
      assert.equal((yield* Ref.get(state)).closeCount, 0);

      // The close is time-boxed, so the interrupter returns and the session's
      // open lock is free for the next open.
      yield* TestClock.adjust("30 seconds");
      yield* Fiber.join(interrupter);
      yield* Deferred.succeed(releaseRetry, undefined);
      yield* manager.open({ threadId, providerSessionId, modelSelection, runtimePolicy });
      assert.equal((yield* Ref.get(state)).openCount, 1);

      // Its close hangs as well; release it while the test clock can still move.
      const stopping = yield* manager.shutdown.pipe(Effect.forkChild);
      yield* TestClock.adjust("30 seconds");
      yield* Fiber.join(stopping);
    }).pipe(
      Effect.provide(
        layerTest({
          state,
          idleTimeoutMs: 60_000,
          mcpConfigs,
          // The first open hangs in its handshake; the retry completes.
          beforeOpen: () =>
            Deferred.isDone(handshakeStarted).pipe(
              Effect.flatMap((retry) =>
                retry
                  ? Deferred.await(releaseRetry)
                  : Deferred.succeed(handshakeStarted, undefined).pipe(
                      Effect.andThen(Effect.never),
                    ),
              ),
            ),
          spawnBeforeOpen: true,
          hangSessionScopeClose: true,
          scopeCloseReached,
        }),
      ),
    );
  }),
);

it.effect("ProviderSessionManagerV2 releases an idle session whose turn start was stopped", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const startTurnReached = yield* Deferred.make<void>();
    yield* Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("thread-provider-session-manager-stopped-turn-start");
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });
      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      const runtime = yield* manager.open({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });
      const runId = idAllocator.derive.run({ threadId, ordinal: 1 });
      const starting = yield* runtime
        .startTurn({
          appThread: (yield* projectionStore.getThreadProjection(threadId)).thread,
          threadId,
          runId,
          runOrdinal: 1,
          providerTurnOrdinal: 1,
          attemptId: idAllocator.derive.runAttempt({ runId, attemptOrdinal: 1 }),
          rootNodeId: idAllocator.derive.rootNode({ runId }),
          providerThread: makeProviderThread({ idAllocator, threadId, providerSessionId, now }),
          message: {
            createdBy: "user",
            creationSource: "web",
            messageId: yield* idAllocator.allocate.message({ threadId, ordinal: 1 }),
            text: "stopped before the provider accepted it",
            attachments: [],
          },
          modelSelection,
          runtimePolicy,
        })
        .pipe(Effect.forkChild);
      yield* Deferred.await(startTurnReached);

      // Stop lands while the provider is still accepting the turn. No terminal
      // event follows, so the session must count itself idle again.
      yield* Fiber.interrupt(starting);
      yield* TestClock.adjust("2 seconds");
      yield* Effect.yieldNow;
      assert.isTrue(Option.isNone(yield* manager.get(providerSessionId)));
      assert.equal((yield* Ref.get(state)).closeCount, 1);
    }).pipe(
      Effect.provide(
        layerTest({
          state,
          idleTimeoutMs: 1000,
          startTurn: Deferred.succeed(startTurnReached, undefined).pipe(
            Effect.andThen(Effect.never),
          ),
        }),
      ),
    );
  }),
);

it.effect(
  "ProviderSessionManagerV2 keeps a shared session busy when another thread's start is stopped early",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const projectId = ProjectId.make("project-provider-session-manager-shared-stopped-start");
      // Blocks the next project read, so a re-attach can be stopped before its
      // start marks the session busy.
      const holdProjectRead = yield* Ref.make(false);
      const projectReadHeld = yield* Deferred.make<void>();
      const projectServiceLayer = Layer.mock(ProjectService.ProjectService)({
        getById: (requestedProjectId) =>
          Ref.get(holdProjectRead).pipe(
            Effect.flatMap((hold) =>
              hold
                ? Deferred.succeed(projectReadHeld, undefined).pipe(Effect.andThen(Effect.never))
                : Effect.succeed(Option.some(makeBrowserAccessProject(requestedProjectId))),
            ),
          ),
      });
      yield* Effect.gen(function* () {
        const eventSink = yield* EventSink.EventSinkV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
        const now = yield* DateTime.now;
        const threadA = ThreadId.make("thread-provider-session-manager-shared-stopped-a");
        const threadB = ThreadId.make("thread-provider-session-manager-shared-stopped-b");
        const providerSessionId = idAllocator.derive.providerSession({
          providerInstanceId: modelSelection.instanceId,
        });
        yield* eventSink.write({
          events: [
            yield* makeThreadCreatedEvent({ idAllocator, threadId: threadA, now, projectId }),
            yield* makeThreadCreatedEvent({ idAllocator, threadId: threadB, now, projectId }),
          ],
        });
        const runtime = yield* manager.open({
          threadId: threadA,
          providerSessionId,
          modelSelection,
          runtimePolicy,
        });
        yield* manager.open({
          threadId: threadB,
          providerSessionId,
          modelSelection,
          runtimePolicy,
        });
        const startTurn = (threadId: ThreadId) =>
          Effect.gen(function* () {
            const runId = idAllocator.derive.run({ threadId, ordinal: 1 });
            return yield* runtime.startTurn({
              appThread: (yield* projectionStore.getThreadProjection(threadId)).thread,
              threadId,
              runId,
              runOrdinal: 1,
              providerTurnOrdinal: 1,
              attemptId: idAllocator.derive.runAttempt({ runId, attemptOrdinal: 1 }),
              rootNodeId: idAllocator.derive.rootNode({ runId }),
              providerThread: makeProviderThread({ idAllocator, threadId, providerSessionId, now }),
              message: {
                createdBy: "user",
                creationSource: "web",
                messageId: yield* idAllocator.allocate.message({ threadId, ordinal: 1 }),
                text: "turn",
                attachments: [],
              },
              modelSelection,
              runtimePolicy,
            });
          });

        // B's turn is accepted and still running.
        yield* startTurn(threadB);

        // A detaches, and its next start is stopped while re-attaching, before
        // it marks the session busy.
        yield* manager.detach({ providerSessionId, threadId: threadA });
        yield* Ref.set(holdProjectRead, true);
        const startingA = yield* startTurn(threadA).pipe(Effect.forkChild);
        yield* Deferred.await(projectReadHeld);
        yield* Fiber.interrupt(startingA);

        // B's running turn keeps the session busy past the idle timeout.
        yield* TestClock.adjust("2 seconds");
        yield* Effect.yieldNow;
        assert.isTrue(Option.isSome(yield* manager.get(providerSessionId)));
        assert.equal((yield* Ref.get(state)).closeCount, 0);
      }).pipe(
        Effect.provide(
          layerTest({
            state,
            idleTimeoutMs: 1000,
            projectServiceLayer,
            serverSettingsLayer: ServerSettings.layerTest({
              projectSettingsOverrides: { [projectId]: { enableAgentBrowserAccess: true } },
            }),
          }),
        ),
      );
    }),
);

it.effect(
  "ProviderSessionManagerV2 keeps a shared session busy when a stopped start still ends its turn",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      // A's adapter start installs its turn, then is stopped mid-request; like
      // OpenCode2, the adapter still ends that turn with turn.terminal later.
      const holdStart = yield* Ref.make(false);
      const startHeld = yield* Deferred.make<void>();
      yield* Effect.gen(function* () {
        const eventSink = yield* EventSink.EventSinkV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
        const now = yield* DateTime.now;
        const threadA = ThreadId.make("thread-provider-session-manager-stopped-ended-a");
        const threadB = ThreadId.make("thread-provider-session-manager-stopped-ended-b");
        const providerSessionId = idAllocator.derive.providerSession({
          providerInstanceId: modelSelection.instanceId,
        });
        yield* eventSink.write({
          events: [
            yield* makeThreadCreatedEvent({ idAllocator, threadId: threadA, now }),
            yield* makeThreadCreatedEvent({ idAllocator, threadId: threadB, now }),
          ],
        });
        const runtime = yield* manager.open({
          threadId: threadA,
          providerSessionId,
          modelSelection,
          runtimePolicy,
        });
        yield* manager.open({
          threadId: threadB,
          providerSessionId,
          modelSelection,
          runtimePolicy,
        });
        const providerThreadOf = (threadId: ThreadId) =>
          makeProviderThread({
            idAllocator,
            threadId,
            providerSessionId,
            now,
            nativeThreadId: `native-${threadId}`,
          });
        const startTurn = (threadId: ThreadId) =>
          Effect.gen(function* () {
            const runId = idAllocator.derive.run({ threadId, ordinal: 1 });
            return yield* runtime.startTurn({
              appThread: (yield* projectionStore.getThreadProjection(threadId)).thread,
              threadId,
              runId,
              runOrdinal: 1,
              providerTurnOrdinal: 1,
              attemptId: idAllocator.derive.runAttempt({ runId, attemptOrdinal: 1 }),
              rootNodeId: idAllocator.derive.rootNode({ runId }),
              providerThread: providerThreadOf(threadId),
              message: {
                createdBy: "user",
                creationSource: "web",
                messageId: yield* idAllocator.allocate.message({ threadId, ordinal: 1 }),
                text: "turn",
                attachments: [],
              },
              modelSelection,
              runtimePolicy,
            });
          });

        // B's turn is accepted and still running.
        yield* startTurn(threadB);

        // A's start is stopped after the adapter began the turn.
        yield* Ref.set(holdStart, true);
        const startingA = yield* startTurn(threadA).pipe(Effect.forkChild);
        yield* Deferred.await(startHeld);
        yield* Fiber.interrupt(startingA);

        // The adapter ends A's turn anyway.
        const queue = (yield* Ref.get(state)).eventQueues.get(String(providerSessionId));
        assert.isDefined(queue);
        yield* Queue.offer(queue!, {
          type: "turn.terminal",
          driver: CODEX_DRIVER,
          providerThreadId: providerThreadOf(threadA).id,
          providerTurnId: idAllocator.derive.providerTurn({
            driver: CODEX_DRIVER,
            nativeTurnId: "native-turn-stopped-a",
          }),
          runOrdinal: 1,
          status: "completed",
          failure: null,
          threadDisposition: "reusable",
        });

        // B's running turn keeps the session busy past the idle timeout.
        yield* TestClock.adjust("2 seconds");
        yield* Effect.yieldNow;
        assert.isTrue(Option.isSome(yield* manager.get(providerSessionId)));
        assert.equal((yield* Ref.get(state)).closeCount, 0);
      }).pipe(
        Effect.provide(
          layerTest({
            state,
            idleTimeoutMs: 1000,
            startTurn: Ref.get(holdStart).pipe(
              Effect.flatMap((hold) =>
                hold
                  ? Deferred.succeed(startHeld, undefined).pipe(Effect.andThen(Effect.never))
                  : Effect.void,
              ),
            ),
          }),
        ),
      );
    }),
);

it.effect("ProviderSessionManagerV2 stops a session still opening when its layer shuts down", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const handshakeStarted = yield* Deferred.make<void>();
    yield* Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("thread-provider-session-manager-shutdown-during-open");
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });
      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      // Detached, like an open the layer does not own: only the session
      // scope's parent can stop its process when the layer closes.
      yield* manager
        .open({ threadId, providerSessionId, modelSelection, runtimePolicy })
        .pipe(Effect.forkDetach);
      yield* Deferred.await(handshakeStarted);
      assert.equal((yield* Ref.get(state)).closeCount, 0);
    }).pipe(
      Effect.provide(
        layerTest({
          state,
          idleTimeoutMs: 60_000,
          beforeOpen: () =>
            Deferred.succeed(handshakeStarted, undefined).pipe(Effect.andThen(Effect.never)),
          spawnBeforeOpen: true,
        }),
      ),
    );

    assert.equal((yield* Ref.get(state)).closeCount, 1);
  }),
);

it.effect("ProviderSessionManagerV2 releases live sessions when its layer shuts down", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("thread-provider-session-manager-shutdown");
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });

      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      yield* manager.open({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });

      const liveState = yield* Ref.get(state);
      assert.equal(liveState.openCount, 1);
      assert.equal(liveState.closeCount, 0);
    });

    yield* effect.pipe(
      Effect.provide(
        layerTest({
          state,
          idleTimeoutMs: 60_000,
        }),
      ),
    );

    assert.equal((yield* Ref.get(state)).closeCount, 1);
  }),
);

it.effect("ProviderSessionManagerV2 closes event subscriptions normally on server shutdown", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("thread-provider-session-manager-shutdown-subscription");
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });
      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      const runtime = yield* manager.open({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });
      const bufferedSubscription = yield* runtime.subscribeEvents!;
      const activeSubscription = yield* runtime.subscribeEvents!;
      const adapterQueue = (yield* Ref.get(state)).eventQueues.get(String(providerSessionId));
      assert.isDefined(adapterQueue);
      yield* Queue.offer(adapterQueue!, {
        type: "provider_session.updated",
        driver: CODEX_DRIVER,
        providerSession: runtime.providerSession,
      });
      assert.isTrue(Option.isSome(yield* activeSubscription.events.pipe(Stream.runHead)));

      yield* manager.shutdown;

      assert.isEmpty(yield* bufferedSubscription.events.pipe(Stream.runCollect));
    });

    yield* effect.pipe(Effect.provide(layerTest({ state, idleTimeoutMs: 60_000 })));
  }),
);

it.effect("ProviderSessionManagerV2 drains subscribers when the provider stops", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("thread-provider-session-manager-provider-stop");
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });
      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      const runtime = yield* manager.open({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });
      const subscription = yield* runtime.subscribeEvents!;
      const collected = yield* subscription.events.pipe(Stream.runCollect, Effect.forkScoped);
      const adapterQueue = (yield* Ref.get(state)).eventQueues.get(String(providerSessionId));
      assert.isDefined(adapterQueue);
      const providerThreadId = idAllocator.derive.providerThread({
        driver: CODEX_DRIVER,
        nativeThreadId: "provider-stop-thread",
      });
      const providerTurnId = idAllocator.derive.providerTurn({
        driver: CODEX_DRIVER,
        nativeTurnId: "provider-stop-turn",
      });
      yield* Queue.offer(adapterQueue!, {
        type: "turn.terminal",
        driver: CODEX_DRIVER,
        providerThreadId,
        providerTurnId,
        runOrdinal: 1,
        status: "completed",
        failure: null,
        threadDisposition: "reusable",
      });
      yield* Queue.offer(adapterQueue!, {
        type: "provider_session.updated",
        driver: CODEX_DRIVER,
        providerSession: {
          ...runtime.providerSession,
          status: "stopped",
          updatedAt: now,
        },
      });
      yield* Queue.end(adapterQueue!);

      const events = Array.from(yield* Fiber.join(collected));
      assert.deepEqual(
        events.map((event) => event.type),
        ["turn.terminal", "provider_session.updated"],
      );
      assert.isTrue(Option.isNone(yield* manager.get(providerSessionId)));
      assert.equal((yield* Ref.get(state)).closeCount, 1);
    });

    yield* effect.pipe(Effect.provide(layerTest({ state, idleTimeoutMs: 60_000 })));
  }),
);

it.effect(
  "ProviderSessionManagerV2 issues MCP credentials before opening and revokes them on close",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const mcpConfigs = yield* Ref.make<
        ReadonlyArray<McpProviderSession.McpProviderSessionConfig | undefined>
      >([]);
      const effect = Effect.gen(function* () {
        const eventSink = yield* EventSink.EventSinkV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const registry = yield* McpSessionRegistry.McpSessionRegistry;
        const mcpSessions = yield* McpProviderSessions.McpProviderSessions;
        const now = yield* DateTime.now;
        const threadId = ThreadId.make("thread-provider-session-manager-mcp");
        const providerSessionId = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });

        yield* eventSink.write({
          events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
        });
        yield* manager.open({
          threadId,
          providerSessionId,
          modelSelection,
          runtimePolicy,
        });

        const captured = (yield* Ref.get(mcpConfigs))[0];
        assert.isDefined(captured);
        assert.equal(captured?.threadId, threadId);
        assert.equal(captured?.providerInstanceId, modelSelection.instanceId);
        assert.equal(captured?.endpoint, "http://127.0.0.1:43123/mcp");
        const token = captured?.authorizationHeader.replace(/^Bearer\s+/, "");
        assert.isDefined(token);
        const resolved = yield* registry.resolve(token!);
        assert.equal(resolved?.thread.threadId, threadId);
        assert.deepEqual(
          resolved?.capabilities,
          new Set(["preview", "orchestration", "worktree", "pull-requests"]),
        );

        yield* manager.close(providerSessionId);
        assert.isUndefined(yield* mcpSessions.read(threadId));
        assert.isUndefined(yield* registry.resolve(token!));
      });

      yield* effect.pipe(
        Effect.provide(
          layerTest({
            state,
            idleTimeoutMs: 1_000,
            mcpConfigs,
          }),
        ),
      );
    }),
);

it.effect(
  "ProviderSessionManagerV2 withholds the preview capability when agent browser access is off",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const mcpConfigs = yield* Ref.make<
        ReadonlyArray<McpProviderSession.McpProviderSessionConfig | undefined>
      >([]);
      const effect = Effect.gen(function* () {
        const eventSink = yield* EventSink.EventSinkV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const registry = yield* McpSessionRegistry.McpSessionRegistry;
        const now = yield* DateTime.now;
        const threadId = ThreadId.make("thread-provider-session-manager-no-browser");
        const providerSessionId = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });

        yield* eventSink.write({
          events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
        });
        yield* manager.open({
          threadId,
          providerSessionId,
          modelSelection,
          runtimePolicy,
        });

        const captured = (yield* Ref.get(mcpConfigs))[0];
        assert.isDefined(captured);
        assert.equal(captured?.browserToolsAvailable, false);
        const token = captured?.authorizationHeader.replace(/^Bearer\s+/, "");
        const resolved = yield* registry.resolve(token!);
        assert.deepEqual(
          resolved?.capabilities,
          new Set(["orchestration", "worktree", "pull-requests"]),
        );

        yield* manager.close(providerSessionId);
      });

      yield* effect.pipe(
        Effect.provide(
          layerTest({
            state,
            idleTimeoutMs: 1_000,
            mcpConfigs,
            // orDie: the test layer's settings-normalization error cannot
            // occur for a literal override and the slot requires error never.
            serverSettingsLayer: ServerSettings.layerTest({
              enableAgentBrowserAccess: false,
            }).pipe(Layer.orDie),
          }),
        ),
      );
    }),
);

it.effect("ProviderSessionManagerV2 honors a project browser-access opt-out", () =>
  Effect.gen(function* () {
    const captured = yield* runBrowserAccessScenario({
      enableAgentBrowserAccess: true,
      projectOverride: false,
    });
    assert.isDefined(captured);
    assert.equal(captured?.browserToolsAvailable, false);
  }),
);

it.effect("ProviderSessionManagerV2 honors a project browser-access opt-in", () =>
  Effect.gen(function* () {
    const captured = yield* runBrowserAccessScenario({
      enableAgentBrowserAccess: false,
      projectOverride: true,
    });
    assert.isDefined(captured);
    assert.equal(captured?.browserToolsAvailable, true);
  }),
);

it.effect("ProviderSessionManagerV2 fails browser access closed for a missing project", () =>
  Effect.gen(function* () {
    const captured = yield* runBrowserAccessScenario({
      enableAgentBrowserAccess: true,
      projectOverride: true,
      projectExists: false,
    });
    assert.isDefined(captured);
    assert.equal(captured?.browserToolsAvailable, false);
  }),
);

it.effect("ProviderSessionManagerV2 fails browser access closed for a missing thread", () =>
  Effect.gen(function* () {
    const captured = yield* runBrowserAccessScenario({
      enableAgentBrowserAccess: true,
      projectOverride: true,
      createThread: false,
    });
    assert.isDefined(captured);
    assert.equal(captured?.browserToolsAvailable, false);
  }),
);

it.effect("ProviderSessionManagerV2 revokes MCP credentials when release persistence fails", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const mcpConfigs = yield* Ref.make<
      ReadonlyArray<McpProviderSession.McpProviderSessionConfig | undefined>
    >([]);
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const registry = yield* McpSessionRegistry.McpSessionRegistry;
      const mcpSessions = yield* McpProviderSessions.McpProviderSessions;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("thread-provider-session-manager-mcp-release-failure");
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });

      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      yield* manager.open({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });

      const captured = (yield* Ref.get(mcpConfigs))[0];
      const token = captured?.authorizationHeader.replace(/^Bearer\s+/, "");
      assert.isDefined(token);
      assert.isDefined(yield* registry.resolve(token!));

      const closeError = yield* manager.close(providerSessionId).pipe(Effect.flip);
      assert.equal(closeError._tag, "ProviderSessionCloseError");
      assert.isUndefined(yield* mcpSessions.read(threadId));
      assert.isUndefined(yield* registry.resolve(token!));
    });

    yield* effect.pipe(
      Effect.provide(
        layerTest({
          state,
          idleTimeoutMs: 1_000,
          mcpConfigs,
          failReleaseEventWrites: true,
        }),
      ),
    );
  }),
);

it.effect(
  "ProviderSessionManagerV2 keeps a thread attached when an earlier attach of it is interrupted",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const armed = yield* Ref.make(false);
      const paused = yield* Deferred.make<void>();
      yield* Effect.gen(function* () {
        const eventSink = yield* EventSink.EventSinkV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const registry = yield* McpSessionRegistry.McpSessionRegistry;
        const mcpSessions = yield* McpProviderSessions.McpProviderSessions;
        const now = yield* DateTime.now;
        const owner = ThreadId.make("thread-provider-session-manager-attach-race-owner");
        const threadId = ThreadId.make("thread-provider-session-manager-attach-race");
        const providerSessionId = idAllocator.derive.providerSession({
          providerInstanceId: modelSelection.instanceId,
        });
        yield* eventSink.write({
          events: [
            yield* makeThreadCreatedEvent({ idAllocator, threadId: owner, now }),
            yield* makeThreadCreatedEvent({ idAllocator, threadId, now }),
          ],
        });
        const runtime = yield* manager.open({
          threadId: owner,
          providerSessionId,
          modelSelection,
          runtimePolicy,
        });
        const resume = runtime.resumeThread({
          threadId,
          providerThread: makeProviderThread({
            idAllocator,
            threadId,
            providerSessionId,
            now,
            nativeThreadId: "native-attach-race",
          }),
        });

        // The first attach of the thread is stopped after attaching it, while
        // a second attach of the same thread is already on its way.
        yield* Ref.set(armed, true);
        const first = yield* resume.pipe(Effect.forkChild({ startImmediately: true }));
        yield* Deferred.await(paused);
        const second = yield* resume.pipe(Effect.forkChild({ startImmediately: true }));
        yield* Fiber.interrupt(first);
        yield* Fiber.join(second);

        // The second attach owns the thread's attachment and credential.
        const config = yield* mcpSessions.read(threadId);
        assert.isDefined(config);
        const token = config!.authorizationHeader.replace(/^Bearer\s+/, "");
        assert.equal((yield* registry.resolve(token))?.thread.threadId, threadId);
      }).pipe(
        Effect.provide(
          layerTest({ state, idleTimeoutMs: 60_000, pauseAttachWrite: { armed, paused } }),
        ),
      );
    }),
);

it.effect(
  "ProviderSessionManagerV2 keeps a replacement session's attachment when a stale attach is interrupted",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const armed = yield* Ref.make(false);
      const paused = yield* Deferred.make<void>();
      yield* Effect.gen(function* () {
        const eventSink = yield* EventSink.EventSinkV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const registry = yield* McpSessionRegistry.McpSessionRegistry;
        const mcpSessions = yield* McpProviderSessions.McpProviderSessions;
        const eventStore = yield* EventStore.EventStoreV2;
        const now = yield* DateTime.now;
        const owner = ThreadId.make("thread-provider-session-manager-stale-attach-owner");
        const threadId = ThreadId.make("thread-provider-session-manager-stale-attach");
        const providerSessionId = idAllocator.derive.providerSession({
          providerInstanceId: modelSelection.instanceId,
        });
        yield* eventSink.write({
          events: [
            yield* makeThreadCreatedEvent({ idAllocator, threadId: owner, now }),
            yield* makeThreadCreatedEvent({ idAllocator, threadId, now }),
          ],
        });
        const runtime = yield* manager.open({
          threadId: owner,
          providerSessionId,
          modelSelection,
          runtimePolicy,
        });

        // An attach of the thread to this session stalls mid-write...
        yield* Ref.set(armed, true);
        const stale = yield* runtime
          .resumeThread({
            threadId,
            providerThread: makeProviderThread({
              idAllocator,
              threadId,
              providerSessionId,
              now,
              nativeThreadId: "native-stale-attach",
            }),
          })
          .pipe(Effect.forkChild({ startImmediately: true }));
        yield* Deferred.await(paused);

        // ...while the session is replaced and the thread opens the new one.
        yield* manager.release({ providerSessionId, reason: "runtime_error" });
        const replacement = yield* manager.open({
          threadId,
          providerSessionId,
          modelSelection,
          runtimePolicy,
        });
        const config = yield* mcpSessions.read(threadId);
        assert.isDefined(config);

        yield* Fiber.interrupt(stale);

        // The replacement keeps the thread and its credential.
        assert.equal(
          (yield* mcpSessions.read(threadId))?.providerSessionId,
          config!.providerSessionId,
        );
        const token = config!.authorizationHeader.replace(/^Bearer\s+/, "");
        assert.equal((yield* registry.resolve(token))?.thread.threadId, threadId);
        // Still attached: resuming on the replacement does not attach the thread again.
        const attachedEvents = eventStore
          .read({ threadId, eventType: "provider-session.attached" })
          .pipe(
            Stream.runCollect,
            Effect.map((events) => events.length),
          );
        const attachedBefore = yield* attachedEvents;
        yield* replacement.resumeThread({
          threadId,
          providerThread: makeProviderThread({
            idAllocator,
            threadId,
            providerSessionId,
            now,
            nativeThreadId: "native-stale-attach",
          }),
        });
        assert.equal(yield* attachedEvents, attachedBefore);
      }).pipe(
        Effect.provide(
          layerTest({ state, idleTimeoutMs: 60_000, pauseAttachWrite: { armed, paused } }),
        ),
      );
    }),
);

it.effect(
  "ProviderSessionManagerV2 revokes a reused credential after a resume stopped while checking it",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const armed = yield* Ref.make(false);
      const paused = yield* Deferred.make<void>();
      yield* Effect.gen(function* () {
        const eventSink = yield* EventSink.EventSinkV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const registry = yield* McpSessionRegistry.McpSessionRegistry;
        const mcpSessions = yield* McpProviderSessions.McpProviderSessions;
        const now = yield* DateTime.now;
        const owner = ThreadId.make("thread-provider-session-manager-resolve-stop-owner");
        const threadId = ThreadId.make("thread-provider-session-manager-resolve-stop");
        const providerSessionId = idAllocator.derive.providerSession({
          providerInstanceId: modelSelection.instanceId,
        });
        yield* eventSink.write({
          events: [
            yield* makeThreadCreatedEvent({ idAllocator, threadId: owner, now }),
            yield* makeThreadCreatedEvent({ idAllocator, threadId, now }),
          ],
        });
        const runtime = yield* manager.open({
          threadId: owner,
          providerSessionId,
          modelSelection,
          runtimePolicy,
        });
        const resume = runtime.resumeThread({
          threadId,
          providerThread: makeProviderThread({
            idAllocator,
            threadId,
            providerSessionId,
            now,
            nativeThreadId: "native-resolve-stop",
          }),
        });
        // The thread gets a credential, then detaches and keeps it for a re-attach.
        yield* resume;
        yield* manager.detach({ providerSessionId, threadId });
        const config = yield* mcpSessions.read(threadId);
        assert.isDefined(config);
        const token = config!.authorizationHeader.replace(/^Bearer\s+/, "");

        // A re-attach is stopped while it checks whether that credential is reusable.
        yield* Ref.set(armed, true);
        const stopped = yield* resume.pipe(Effect.forkChild({ startImmediately: true }));
        yield* Deferred.await(paused);
        yield* Fiber.interrupt(stopped);

        // Nothing holds the credential now, so a terminal release revokes it.
        yield* manager.release({ providerSessionId, reason: "manual_shutdown" });
        assert.isUndefined(yield* registry.resolve(token));
      }).pipe(
        Effect.provide(
          layerTest({ state, idleTimeoutMs: 60_000, pauseResolve: { armed, paused } }),
        ),
      );
    }),
);

it.effect("ProviderSessionManagerV2 duplicate detach preserves replacement MCP credentials", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const mcpConfigs = yield* Ref.make<
      ReadonlyArray<McpProviderSession.McpProviderSessionConfig | undefined>
    >([]);
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const registry = yield* McpSessionRegistry.McpSessionRegistry;
      const mcpSessions = yield* McpProviderSessions.McpProviderSessions;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("thread-provider-session-manager-replacement-mcp");
      const oldSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });
      const replacementSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });

      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      yield* manager.open({
        threadId,
        providerSessionId: oldSessionId,
        modelSelection,
        runtimePolicy,
      });
      yield* manager.detach({ providerSessionId: oldSessionId, threadId });
      yield* manager.open({
        threadId,
        providerSessionId: replacementSessionId,
        modelSelection,
        runtimePolicy,
      });

      const replacement = (yield* Ref.get(mcpConfigs)).at(-1);
      assert.isDefined(replacement);
      const replacementToken = replacement?.authorizationHeader.replace(/^Bearer\s+/, "");
      assert.isDefined(replacementToken);
      assert.equal(
        (yield* mcpSessions.read(threadId))?.providerSessionId,
        replacement?.providerSessionId,
      );

      yield* manager.detach({ providerSessionId: oldSessionId, threadId });

      assert.equal(
        (yield* mcpSessions.read(threadId))?.providerSessionId,
        replacement?.providerSessionId,
      );
      assert.equal((yield* registry.resolve(replacementToken!))?.thread.threadId, threadId);
    });

    yield* effect.pipe(
      Effect.provide(
        layerTest({
          state,
          idleTimeoutMs: 1_000,
          capabilities: ExclusiveCapabilities,
          mcpConfigs,
        }),
      ),
    );
  }),
);

it.effect(
  "ProviderSessionManagerV2 detach of a superseded live session preserves replacement MCP credentials",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const mcpConfigs = yield* Ref.make<
        ReadonlyArray<McpProviderSession.McpProviderSessionConfig | undefined>
      >([]);
      const effect = Effect.gen(function* () {
        const eventSink = yield* EventSink.EventSinkV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const registry = yield* McpSessionRegistry.McpSessionRegistry;
        const mcpSessions = yield* McpProviderSessions.McpProviderSessions;
        const now = yield* DateTime.now;
        const threadId = ThreadId.make("thread-provider-session-manager-superseded-mcp");
        const oldSessionId = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });
        const replacementSessionId = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });

        yield* eventSink.write({
          events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
        });
        yield* manager.open({
          threadId,
          providerSessionId: oldSessionId,
          modelSelection,
          runtimePolicy,
        });
        // The replacement opens while the old session is still attached: this is
        // the workspace-handoff sequence, where the queued continuation run can
        // start its session before the outbox executes the old session's detach.
        yield* manager.open({
          threadId,
          providerSessionId: replacementSessionId,
          modelSelection,
          runtimePolicy,
        });

        const replacement = (yield* Ref.get(mcpConfigs)).at(-1);
        assert.isDefined(replacement);
        const replacementToken = replacement?.authorizationHeader.replace(/^Bearer\s+/, "");
        assert.isDefined(replacementToken);
        assert.equal(
          (yield* mcpSessions.read(threadId))?.providerSessionId,
          replacement?.providerSessionId,
        );

        // First (non-duplicate) detach of the superseded session must not revoke
        // the replacement's credential or clear its config slot.
        yield* manager.detach({ providerSessionId: oldSessionId, threadId });

        assert.equal(
          (yield* mcpSessions.read(threadId))?.providerSessionId,
          replacement?.providerSessionId,
        );
        assert.equal((yield* registry.resolve(replacementToken!))?.thread.threadId, threadId);
      });

      yield* effect.pipe(
        Effect.provide(
          layerTest({
            state,
            idleTimeoutMs: 1_000,
            capabilities: ExclusiveCapabilities,
            mcpConfigs,
          }),
        ),
      );
    }),
);

it.effect(
  "ProviderSessionManagerV2 keeps a thread's MCP credential stable across detach and re-attach on a shared session",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const mcpConfigs = yield* Ref.make<
        ReadonlyArray<McpProviderSession.McpProviderSessionConfig | undefined>
      >([]);
      const effect = Effect.gen(function* () {
        const eventSink = yield* EventSink.EventSinkV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const registry = yield* McpSessionRegistry.McpSessionRegistry;
        const mcpSessions = yield* McpProviderSessions.McpProviderSessions;
        const now = yield* DateTime.now;
        const threadId = ThreadId.make("thread-provider-session-manager-stable-mcp");
        const providerSessionId = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });

        yield* eventSink.write({
          events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
        });
        yield* manager.open({
          threadId,
          providerSessionId,
          modelSelection,
          runtimePolicy,
        });

        const original = (yield* Ref.get(mcpConfigs)).at(-1);
        assert.isDefined(original);
        const originalToken = original?.authorizationHeader.replace(/^Bearer\s+/, "");
        assert.isDefined(originalToken);

        // Workspace-change handoff on a shared multi-thread session (codex):
        // the thread detaches while the provider process keeps running, and the
        // process's MCP client keeps using the credential it was started with.
        yield* manager.detach({ providerSessionId, threadId, detail: "Workspace changed." });
        assert.equal(
          (yield* registry.resolve(originalToken!))?.thread.threadId,
          threadId,
          "detach must not revoke the credential the live provider process still holds",
        );

        // The continuation run re-attaches the same thread to the same session;
        // the credential must be reused, not rotated, so the provider process's
        // long-lived MCP client stays authorized.
        yield* manager.open({
          threadId,
          providerSessionId,
          modelSelection,
          runtimePolicy,
        });
        assert.equal(
          (yield* mcpSessions.read(threadId))?.providerSessionId,
          original?.providerSessionId,
          "re-attach must reuse the existing credential, not rotate it",
        );
        assert.equal((yield* registry.resolve(originalToken!))?.thread.threadId, threadId);

        // Releasing the session (provider process gone) still revokes.
        yield* manager.close(providerSessionId);
        assert.isUndefined(yield* registry.resolve(originalToken!));
      });

      yield* effect.pipe(
        Effect.provide(
          layerTest({
            state,
            idleTimeoutMs: 1_000,
            mcpConfigs,
          }),
        ),
      );
    }),
);

it.effect(
  "ProviderSessionManagerV2 revokes a rotated credential despite a stale record on another live session",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const mcpConfigs = yield* Ref.make<
        ReadonlyArray<McpProviderSession.McpProviderSessionConfig | undefined>
      >([]);
      const effect = Effect.gen(function* () {
        const eventSink = yield* EventSink.EventSinkV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const registry = yield* McpSessionRegistry.McpSessionRegistry;
        const mcpSessions = yield* McpProviderSessions.McpProviderSessions;
        const now = yield* DateTime.now;
        const threadId = ThreadId.make("thread-provider-session-manager-stale-record");
        const s1 = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });
        const s2 = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });

        yield* eventSink.write({
          events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
        });
        // S1 (shared session) records credential C1 for the thread, then the
        // thread detaches; S1 stays alive with the stale record.
        yield* manager.open({ threadId, providerSessionId: s1, modelSelection, runtimePolicy });
        yield* manager.detach({ providerSessionId: s1, threadId });

        // The credential dies externally, so S2's attach must rotate to C2.
        yield* registry.revokeThread(threadId);
        yield* manager.open({ threadId, providerSessionId: s2, modelSelection, runtimePolicy });
        const rotated = yield* mcpSessions.read(threadId);
        assert.isDefined(rotated);
        const rotatedToken = rotated?.authorizationHeader.replace(/^Bearer\s+/, "");
        assert.isDefined(yield* registry.resolve(rotatedToken!));

        // Releasing S2 must revoke C2 even though S1 still carries a stale
        // record (of dead C1) for the same thread.
        yield* manager.close(s2);
        assert.isUndefined(
          yield* registry.resolve(rotatedToken!),
          "stale record on S1 must not veto revoking S2's rotated credential",
        );
        yield* manager.close(s1);
      });

      yield* effect.pipe(Effect.provide(layerTest({ state, idleTimeoutMs: 1_000, mcpConfigs })));
    }),
);

it.effect(
  "ProviderSessionManagerV2 protects a reused credential from a predecessor release during open",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const mcpConfigs = yield* Ref.make<
        ReadonlyArray<McpProviderSession.McpProviderSessionConfig | undefined>
      >([]);
      const duringOpen = yield* Ref.make<Effect.Effect<void>>(Effect.void);
      const effect = Effect.gen(function* () {
        const eventSink = yield* EventSink.EventSinkV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const registry = yield* McpSessionRegistry.McpSessionRegistry;
        const mcpSessions = yield* McpProviderSessions.McpProviderSessions;
        const now = yield* DateTime.now;
        const threadId = ThreadId.make("thread-provider-session-manager-open-race");
        const s1 = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });
        const s2 = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });

        yield* eventSink.write({
          events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
        });
        yield* manager.open({ threadId, providerSessionId: s1, modelSelection, runtimePolicy });
        const original = (yield* Ref.get(mcpConfigs)).at(-1);
        const originalToken = original?.authorizationHeader.replace(/^Bearer\s+/, "");
        assert.isDefined(originalToken);
        yield* manager.detach({ providerSessionId: s1, threadId });

        // While S2's provider process is spawning (after prepare reused the
        // credential, before the entry is visible), the predecessor session
        // releases. Eager adapters (ACP, OpenCode) bake the credential into
        // the process during openSession, so the release must not revoke it;
        // rotating afterwards cannot repair those adapters.
        yield* Ref.set(duringOpen, manager.close(s1).pipe(Effect.orDie));
        yield* manager.open({ threadId, providerSessionId: s2, modelSelection, runtimePolicy });

        const slot = yield* mcpSessions.read(threadId);
        assert.equal(
          slot?.providerSessionId,
          original?.providerSessionId,
          "the credential the adapter was configured with must remain current",
        );
        assert.equal(
          (yield* registry.resolve(originalToken!))?.thread.threadId,
          threadId,
          "the predecessor release must not revoke a credential reserved by an in-flight open",
        );
        yield* manager.close(s2);
      });

      yield* effect.pipe(
        Effect.provide(
          layerTest({
            state,
            idleTimeoutMs: 1_000,
            mcpConfigs,
            beforeOpen: (input) =>
              input.providerSessionId === undefined
                ? Effect.void
                : Ref.get(duringOpen).pipe(
                    Effect.flatten,
                    Effect.tap(() => Ref.set(duringOpen, Effect.void)),
                  ),
          }),
        ),
      );
    }),
);

it.effect("ProviderSessionManagerV2 terminal detach revokes the thread's MCP credential", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const mcpConfigs = yield* Ref.make<
      ReadonlyArray<McpProviderSession.McpProviderSessionConfig | undefined>
    >([]);
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const registry = yield* McpSessionRegistry.McpSessionRegistry;
      const mcpSessions = yield* McpProviderSessions.McpProviderSessions;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("thread-provider-session-manager-terminal-detach");
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });

      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      yield* manager.open({ threadId, providerSessionId, modelSelection, runtimePolicy });
      const issued = (yield* Ref.get(mcpConfigs)).at(-1);
      const token = issued?.authorizationHeader.replace(/^Bearer\s+/, "");
      assert.isDefined(yield* registry.resolve(token!));

      // Archive/delete detaches carry revokeMcpCredential: the token must die
      // with the thread even though the shared provider process lives on.
      yield* manager.detach({
        providerSessionId,
        threadId,
        detail: "Thread deleted.",
        revokeMcpCredential: true,
      });
      assert.isUndefined(yield* registry.resolve(token!));
      assert.isUndefined(yield* mcpSessions.read(threadId));
    });

    yield* effect.pipe(Effect.provide(layerTest({ state, idleTimeoutMs: 1_000, mcpConfigs })));
  }),
);

it.effect("ProviderSessionManagerV2 resident inspection does not renew idle residency", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const now = yield* DateTime.now;
      const projectId = yield* idAllocator.allocate.project({
        fixtureName: "provider-session-manager-idle",
      });
      const threadId = yield* idAllocator.allocate.thread({
        fixtureName: "provider-session-manager-idle",
        projectId,
      });
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });

      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      yield* manager.open({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });

      yield* TestClock.adjust("500 millis");
      const resident = yield* manager.getResident(providerSessionId);
      assert.isTrue(Option.isSome(resident));
      yield* manager.getResident(providerSessionId);
      yield* TestClock.adjust("500 millis");
      yield* Effect.yieldNow;

      const liveSession = yield* manager.getResident(providerSessionId);
      const runtimeState = yield* Ref.get(state);
      const projection = yield* projectionStore.getThreadProjection(threadId);

      assert.isTrue(Option.isNone(liveSession));
      assert.equal(runtimeState.openCount, 1);
      assert.equal(runtimeState.closeCount, 1);
      assert.equal(projection.providerSessions.at(-1)?.status, "stopped");
    });

    yield* effect.pipe(Effect.provide(layerTest({ state, idleTimeoutMs: 1000 })));
  }),
);

it.effect("ProviderSessionManagerV2 persists release when session scope close hangs", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const now = yield* DateTime.now;
      const threadId = yield* idAllocator.allocate.thread({
        fixtureName: "provider-session-manager-hung-close",
        projectId: yield* idAllocator.allocate.project({
          fixtureName: "provider-session-manager-hung-close",
        }),
      });
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });

      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      yield* manager.open({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });

      yield* TestClock.adjust("1 second");
      yield* Effect.yieldNow;
      assert.isTrue(Option.isNone(yield* manager.get(providerSessionId)));

      yield* TestClock.adjust("30 seconds");
      yield* Effect.yieldNow;
      const projection = yield* projectionStore.getThreadProjection(threadId);
      assert.equal(projection.providerSessions.at(-1)?.status, "stopped");
      assert.equal((yield* Ref.get(state)).closeCount, 0);
    });

    yield* effect.pipe(
      Effect.provide(layerTest({ state, idleTimeoutMs: 1000, hangSessionScopeClose: true })),
    );
  }),
);

it.effect("ProviderSessionManagerV2 defers idle release while background work is pending", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const pendingWork = yield* Ref.make(true);
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const now = yield* DateTime.now;
      const threadId = yield* idAllocator.allocate.thread({
        fixtureName: "provider-session-manager-idle-pin",
        projectId: yield* idAllocator.allocate.project({
          fixtureName: "provider-session-manager-idle-pin",
        }),
      });
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });

      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      yield* manager.open({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });

      yield* TestClock.adjust("3 seconds");
      yield* Effect.yieldNow;
      assert.isTrue(Option.isSome(yield* manager.getResident(providerSessionId)));
      assert.equal((yield* Ref.get(state)).closeCount, 0);

      yield* Ref.set(pendingWork, false);
      yield* TestClock.adjust("1 second");
      yield* Effect.yieldNow;
      assert.isTrue(Option.isNone(yield* manager.get(providerSessionId)));
      assert.equal((yield* Ref.get(state)).closeCount, 1);
    });

    yield* effect.pipe(
      Effect.provide(
        layerTest({
          state,
          idleTimeoutMs: 1000,
          hasPendingBackgroundWork: Ref.get(pendingWork),
        }),
      ),
    );
  }),
);

it.effect("ProviderSessionManagerV2 releases pinned idle sessions once the pin cap expires", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const now = yield* DateTime.now;
      const threadId = yield* idAllocator.allocate.thread({
        fixtureName: "provider-session-manager-pin-cap",
        projectId: yield* idAllocator.allocate.project({
          fixtureName: "provider-session-manager-pin-cap",
        }),
      });
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });

      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      yield* manager.open({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });

      yield* TestClock.adjust("3 seconds");
      yield* Effect.yieldNow;
      assert.isTrue(Option.isSome(yield* manager.getResident(providerSessionId)));
      yield* manager.getResident(providerSessionId);

      yield* TestClock.adjust("1 second");
      yield* Effect.yieldNow;
      assert.isTrue(Option.isNone(yield* manager.get(providerSessionId)));
      assert.equal((yield* Ref.get(state)).closeCount, 1);
    });

    yield* effect.pipe(
      Effect.provide(
        layerTest({
          state,
          idleTimeoutMs: 1000,
          maxIdlePinMs: 3000,
          hasPendingBackgroundWork: Effect.succeed(true),
        }),
      ),
    );
  }),
);

it.effect("ProviderSessionManagerV2 keeps pinned work that reports progress", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const now = yield* DateTime.now;
      const threadId = yield* idAllocator.allocate.thread({
        fixtureName: "provider-session-manager-pin-progress",
        projectId: yield* idAllocator.allocate.project({
          fixtureName: "provider-session-manager-pin-progress",
        }),
      });
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });
      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      const runtime = yield* manager.open({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });
      const adapterQueue = (yield* Ref.get(state)).eventQueues.get(String(providerSessionId));
      assert.isDefined(adapterQueue);
      const step = (duration: Parameters<typeof TestClock.adjust>[0]) =>
        Effect.gen(function* () {
          yield* TestClock.adjust(duration);
          for (let i = 0; i < 20; i += 1) yield* Effect.yieldNow;
        });

      // A long workflow keeps reporting progress well past the pin cap.
      for (let tick = 0; tick < 12; tick += 1) {
        yield* step("1500 millis");
        yield* Queue.offer(adapterQueue!, {
          type: "provider_session.updated",
          driver: CODEX_DRIVER,
          providerSession: runtime.providerSession,
        });
        for (let i = 0; i < 20; i += 1) yield* Effect.yieldNow;
      }
      assert.equal((yield* Ref.get(state)).closeCount, 0);

      // Work that goes silent past the cap is still stopped.
      for (let tick = 0; tick < 6; tick += 1) {
        yield* step("1 second");
      }
      assert.equal((yield* Ref.get(state)).closeCount, 1);
    });

    yield* effect.pipe(
      Effect.provide(
        layerTest({
          state,
          idleTimeoutMs: 1000,
          maxIdlePinMs: 3000,
          hasPendingBackgroundWork: Effect.succeed(true),
        }),
      ),
    );
  }),
);

it.effect(
  "ProviderSessionManagerV2 does not idle-release a session that turns busy during the pending-work check",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const firstCheck = yield* Ref.make(true);
      const checkEntered = yield* Deferred.make<void>();
      const checkGate = yield* Deferred.make<void>();
      const effect = Effect.gen(function* () {
        const eventSink = yield* EventSink.EventSinkV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
        const now = yield* DateTime.now;
        const projectId = yield* idAllocator.allocate.project({
          fixtureName: "provider-session-manager-busy-during-check",
        });
        const threadId = yield* idAllocator.allocate.thread({
          fixtureName: "provider-session-manager-busy-during-check",
          projectId,
        });
        const providerSessionId = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });
        const providerThread = makeProviderThread({
          idAllocator,
          threadId,
          providerSessionId,
          now,
        });
        const runId = idAllocator.derive.run({ threadId, ordinal: 1 });
        const attemptId = idAllocator.derive.runAttempt({ runId, attemptOrdinal: 1 });
        const rootNodeId = idAllocator.derive.rootNode({ runId });
        const providerTurnId = idAllocator.derive.providerTurn({
          driver: CODEX_DRIVER,
          nativeTurnId: "native-turn-busy-during-check",
        });

        yield* eventSink.write({
          events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
        });
        const runtime = yield* manager.open({
          threadId,
          providerSessionId,
          modelSelection,
          runtimePolicy,
        });
        yield* runtime.events.pipe(Stream.runDrain, Effect.forkScoped);
        const appThread = (yield* projectionStore.getThreadProjection(threadId)).thread;

        yield* TestClock.adjust("1 second");
        yield* Deferred.await(checkEntered);

        // The release fiber is parked inside the pending-work check, so the
        // idle decision it already made is stale once this turn marks the
        // session busy.
        const turnFiber = yield* runtime
          .startTurn({
            appThread,
            threadId,
            runId,
            runOrdinal: 1,
            providerTurnOrdinal: 1,
            attemptId,
            rootNodeId,
            providerThread,
            message: {
              createdBy: "user",
              creationSource: "web",
              messageId: yield* idAllocator.allocate.message({ threadId, ordinal: 1 }),
              text: "hello",
              attachments: [],
            },
            modelSelection,
            runtimePolicy,
          })
          .pipe(Effect.forkDetach);
        for (let i = 0; i < 10; i += 1) {
          yield* Effect.yieldNow;
        }
        yield* Deferred.succeed(checkGate, undefined);
        yield* Fiber.join(turnFiber);
        yield* Effect.yieldNow;

        assert.isTrue(Option.isSome(yield* manager.get(providerSessionId)));
        assert.equal((yield* Ref.get(state)).closeCount, 0);

        const queue = (yield* Ref.get(state)).eventQueues.get(String(providerSessionId));
        assert.isDefined(queue);
        yield* Queue.offer(queue!, {
          type: "turn.terminal",
          driver: CODEX_DRIVER,
          providerThreadId: providerThread.id,
          providerTurnId,
          runOrdinal: 1,
          status: "completed",
          failure: null,
          threadDisposition: "reusable",
        });
        yield* TestClock.adjust("1 second");
        yield* Effect.yieldNow;
        assert.isTrue(Option.isNone(yield* manager.get(providerSessionId)));
        assert.equal((yield* Ref.get(state)).closeCount, 1);
      });

      yield* effect.pipe(
        Effect.provide(
          layerTest({
            state,
            idleTimeoutMs: 1000,
            // Uninterruptible so the markBusy-triggered interrupt cannot land
            // inside the check, mirroring an adapter that masks interruption
            // while inspecting its own state.
            hasPendingBackgroundWork: Effect.uninterruptible(
              Effect.gen(function* () {
                if (yield* Ref.getAndSet(firstCheck, false)) {
                  yield* Deferred.succeed(checkEntered, undefined);
                  yield* Deferred.await(checkGate);
                }
                return false;
              }),
            ),
          }),
        ),
      );
    }),
);

it.effect("ProviderSessionManagerV2 does not apply a stale idle pin to a replacement session", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const firstCheck = yield* Ref.make(true);
    const checkEntered = yield* Deferred.make<void>();
    const checkGate = yield* Deferred.make<void>();
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const now = yield* DateTime.now;
      const threadId = yield* idAllocator.allocate.thread({
        fixtureName: "provider-session-manager-stale-pin",
        projectId: yield* idAllocator.allocate.project({
          fixtureName: "provider-session-manager-stale-pin",
        }),
      });
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });

      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      yield* manager.open({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });

      // Park the first idle fiber inside an uninterruptible pending-work probe.
      yield* TestClock.adjust("1 second");
      yield* Deferred.await(checkEntered);

      // close removes the map entry first, then waits to interrupt the idle
      // fiber (still uninterruptible). That window lets a replacement open
      // under the same providerSessionId before the stale probe finishes.
      const closeFiber = yield* manager.close(providerSessionId).pipe(Effect.forkDetach);
      for (let i = 0; i < 20; i += 1) {
        yield* Effect.yieldNow;
      }
      yield* manager.open({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });
      assert.equal((yield* Ref.get(state)).openCount, 2);

      // Stale probe reports pending work against the old runtime; the pin
      // stamp must no-op on the replacement (runtime / generation mismatch).
      yield* Deferred.succeed(checkGate, undefined);
      yield* Fiber.join(closeFiber);
      for (let i = 0; i < 10; i += 1) {
        yield* Effect.yieldNow;
      }

      assert.isTrue(Option.isSome(yield* manager.get(providerSessionId)));
      assert.equal((yield* Ref.get(state)).closeCount, 1);

      // Replacement has no pending background work. After one idle window it
      // must release. A stale pin stamp would have deferred release until
      // maxIdlePinMs.
      yield* TestClock.adjust("1 second");
      yield* Effect.yieldNow;
      assert.isTrue(Option.isNone(yield* manager.get(providerSessionId)));
      assert.equal((yield* Ref.get(state)).closeCount, 2);
    });

    yield* effect.pipe(
      Effect.provide(
        layerTest({
          state,
          idleTimeoutMs: 1000,
          maxIdlePinMs: 60_000,
          hasPendingBackgroundWork: Effect.uninterruptible(
            Effect.gen(function* () {
              if (yield* Ref.getAndSet(firstCheck, false)) {
                yield* Deferred.succeed(checkEntered, undefined);
                yield* Deferred.await(checkGate);
                return true;
              }
              return false;
            }),
          ),
        }),
      ),
    );
  }),
);

it.effect(
  "ProviderSessionManagerV2 keeps active sessions alive until the provider turn terminates",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const effect = Effect.gen(function* () {
        const eventSink = yield* EventSink.EventSinkV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
        const now = yield* DateTime.now;
        const projectId = yield* idAllocator.allocate.project({
          fixtureName: "provider-session-manager-active",
        });
        const threadId = yield* idAllocator.allocate.thread({
          fixtureName: "provider-session-manager-active",
          projectId,
        });
        const providerSessionId = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });
        const providerThread = makeProviderThread({
          idAllocator,
          threadId,
          providerSessionId,
          now,
        });
        const runId = idAllocator.derive.run({ threadId, ordinal: 1 });
        const attemptId = idAllocator.derive.runAttempt({ runId, attemptOrdinal: 1 });
        const rootNodeId = idAllocator.derive.rootNode({ runId });
        const providerTurnId = idAllocator.derive.providerTurn({
          driver: CODEX_DRIVER,
          nativeTurnId: "native-turn",
        });

        yield* eventSink.write({
          events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
        });
        const runtime = yield* manager.open({
          threadId,
          providerSessionId,
          modelSelection,
          runtimePolicy,
        });
        yield* runtime.events.pipe(Stream.runDrain, Effect.forkScoped);
        const appThread = (yield* projectionStore.getThreadProjection(threadId)).thread;
        yield* runtime.startTurn({
          appThread,
          threadId,
          runId,
          runOrdinal: 1,
          providerTurnOrdinal: 1,
          attemptId,
          rootNodeId,
          providerThread,
          message: {
            createdBy: "user",
            creationSource: "web",
            messageId: yield* idAllocator.allocate.message({ threadId, ordinal: 1 }),
            text: "hello",
            attachments: [],
          },
          modelSelection,
          runtimePolicy,
        });

        yield* TestClock.adjust("2 seconds");
        yield* Effect.yieldNow;
        assert.equal((yield* Ref.get(state)).closeCount, 0);

        const queue = (yield* Ref.get(state)).eventQueues.get(String(providerSessionId));
        assert.isDefined(queue);
        yield* Queue.offer(queue!, {
          type: "turn.terminal",
          driver: CODEX_DRIVER,
          providerThreadId: providerThread.id,
          providerTurnId,
          runOrdinal: 1,
          status: "completed",
          failure: null,
          threadDisposition: "reusable",
        });
        yield* TestClock.adjust("1 second");
        yield* Effect.yieldNow;

        const liveSession = yield* manager.get(providerSessionId);
        const projection = yield* projectionStore.getThreadProjection(threadId);
        assert.isTrue(Option.isNone(liveSession));
        assert.equal((yield* Ref.get(state)).closeCount, 1);
        assert.equal(projection.providerSessions.at(-1)?.status, "stopped");
      });

      yield* effect.pipe(Effect.provide(layerTest({ state, idleTimeoutMs: 1000 })));
    }),
);

it.effect("ProviderSessionManagerV2 uses the same release path for runtime failures", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const now = yield* DateTime.now;
      const projectId = yield* idAllocator.allocate.project({
        fixtureName: "provider-session-manager-runtime-error",
      });
      const threadId = yield* idAllocator.allocate.thread({
        fixtureName: "provider-session-manager-runtime-error",
        projectId,
      });
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });

      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      yield* manager.open({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });
      yield* manager.release({
        providerSessionId,
        reason: "runtime_error",
        detail: "process exited",
      });

      const liveSession = yield* manager.get(providerSessionId);
      const runtimeState = yield* Ref.get(state);
      const projection = yield* projectionStore.getThreadProjection(threadId);

      assert.isTrue(Option.isNone(liveSession));
      assert.equal(runtimeState.closeCount, 1);
      assert.equal(projection.providerSessions.at(-1)?.status, "error");
      assert.equal(projection.providerSessions.at(-1)?.lastError, "process exited");
    });

    yield* effect.pipe(Effect.provide(layerTest({ state, idleTimeoutMs: 1000 })));
  }),
);

it.effect("ProviderSessionManagerV2 releases sessions when provider event streams fail", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const now = yield* DateTime.now;
      const projectId = yield* idAllocator.allocate.project({
        fixtureName: "provider-session-manager-stream-error",
      });
      const threadId = yield* idAllocator.allocate.thread({
        fixtureName: "provider-session-manager-stream-error",
        projectId,
      });
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });

      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      const runtime = yield* manager.open({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });
      yield* runtime.events.pipe(Stream.runDrain, Effect.ignore, Effect.forkScoped);
      yield* Effect.yieldNow;

      const liveSession = yield* manager.get(providerSessionId);
      const runtimeState = yield* Ref.get(state);
      const projection = yield* projectionStore.getThreadProjection(threadId);

      assert.isTrue(Option.isNone(liveSession));
      assert.equal(runtimeState.closeCount, 1);
      assert.equal(projection.providerSessions.at(-1)?.status, "error");
    });

    yield* effect.pipe(
      Effect.provide(
        layerTest({
          state,
          idleTimeoutMs: 1000,
          failEventStream: true,
        }),
      ),
    );
  }),
);

it.effect("ProviderSessionManagerV2 marks pending runtime requests non-live on release", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const now = yield* DateTime.now;
      const projectId = yield* idAllocator.allocate.project({
        fixtureName: "provider-session-manager-request-expire",
      });
      const threadId = yield* idAllocator.allocate.thread({
        fixtureName: "provider-session-manager-request-expire",
        projectId,
      });
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });
      const providerThread = makeProviderThread({
        idAllocator,
        threadId,
        providerSessionId,
        now,
      });

      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      const pendingRequest = yield* makePendingRuntimeRequestEvents({
        idAllocator,
        threadId,
        providerSessionId,
        providerThread,
        now,
      });
      yield* eventSink.write({ events: pendingRequest.events });
      yield* manager.open({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });
      yield* manager.release({
        providerSessionId,
        reason: "runtime_error",
        detail: "process exited",
      });

      const projection = yield* projectionStore.getThreadProjection(threadId);
      const request = projection.runtimeRequests.at(-1);
      const requestNode = projection.nodes.find((node) => node.id === request?.nodeId);
      const requestTurnItem = projection.turnItems.find(
        (item) => item.type === "approval_request" && item.requestId === request?.id,
      );

      assert.equal(request?.status, "expired");
      assert.equal(request?.responseCapability.type, "not_resumable");
      assert.equal(requestNode?.status, "failed");
      assert.equal(requestTurnItem?.status, "failed");
    });

    yield* effect.pipe(Effect.provide(layerTest({ state, idleTimeoutMs: 1000 })));
  }),
);
it.effect("ProviderSessionManagerV2 retries release records that failed to persist", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const flaky: FlakyReleaseWrites = {
      failing: yield* Ref.make<"none" | "session" | "session-and-requests">("session"),
      failures: yield* Queue.unbounded<void>(),
    };
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("thread-provider-session-manager-release-retry");
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });
      const providerThread = makeProviderThread({ idAllocator, threadId, providerSessionId, now });
      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      const pendingRequest = yield* makePendingRuntimeRequestEvents({
        idAllocator,
        threadId,
        providerSessionId,
        providerThread,
        now,
      });
      yield* eventSink.write({ events: pendingRequest.events });
      yield* manager.open({ threadId, providerSessionId, modelSelection, runtimePolicy });

      assert.isTrue(Exit.isFailure(yield* Effect.exit(manager.close(providerSessionId))));
      yield* Queue.take(flaky.failures);
      // The failed session write does not keep the approval answerable.
      const afterClose = yield* projectionStore.getThreadProjection(threadId);
      assert.equal(afterClose.runtimeRequests.at(-1)?.responseCapability.type, "not_resumable");
      assert.equal(afterClose.providerSessions.at(-1)?.status, "ready");

      // The first retry fails as well, and the retries continue.
      yield* TestClock.adjust("1 second");
      yield* Queue.take(flaky.failures);
      yield* Ref.set(flaky.failing, "none");
      const stopped = yield* eventSink
        .stream({
          threadId,
          afterSequence: yield* eventSink.latestSequence({ threadId }),
          eventType: "provider-session.updated",
        })
        .pipe(Stream.runHead, Effect.forkScoped);
      yield* TestClock.adjust("1 second");
      yield* Fiber.join(stopped);

      const afterRetry = yield* projectionStore.getThreadProjection(threadId);
      assert.equal(afterRetry.providerSessions.at(-1)?.status, "stopped");
    });

    yield* effect.pipe(
      Effect.provide(layerTest({ state, idleTimeoutMs: 60_000, flakyReleaseWrites: flaky })),
    );
  }),
);

it.effect("ProviderSessionManagerV2 release retries leave a replacement session alone", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const flaky: FlakyReleaseWrites = {
      failing: yield* Ref.make<"none" | "session" | "session-and-requests">("none"),
      failures: yield* Queue.unbounded<void>(),
    };
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const threadId = ThreadId.make("thread-provider-session-manager-release-replacement");
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });
      const writePendingRequest = Effect.gen(function* () {
        const now = yield* DateTime.now;
        const request = yield* makePendingRuntimeRequestEvents({
          idAllocator,
          threadId,
          providerSessionId,
          providerThread: makeProviderThread({ idAllocator, threadId, providerSessionId, now }),
          now,
        });
        yield* eventSink.write({ events: request.events });
        return request.requestId;
      });
      yield* eventSink.write({
        events: [
          yield* makeThreadCreatedEvent({ idAllocator, threadId, now: yield* DateTime.now }),
        ],
      });
      const oldRequestId = yield* writePendingRequest;
      yield* manager.open({ threadId, providerSessionId, modelSelection, runtimePolicy });
      yield* Ref.set(flaky.failing, "session-and-requests");
      assert.isTrue(Exit.isFailure(yield* Effect.exit(manager.close(providerSessionId))));
      yield* Queue.take(flaky.failures);
      yield* Queue.take(flaky.failures);
      yield* Ref.set(flaky.failing, "none");

      // A replacement opens with the same id before the retry runs.
      yield* TestClock.adjust("500 millis");
      yield* manager.open({ threadId, providerSessionId, modelSelection, runtimePolicy });
      const newRequestId = yield* writePendingRequest;
      const replacementStatus = (yield* projectionStore.getThreadProjection(
        threadId,
      )).providerSessions.at(-1)?.status;
      const settled = yield* eventSink
        .stream({
          threadId,
          afterSequence: yield* eventSink.latestSequence({ threadId }),
          eventType: "runtime-request.updated",
        })
        .pipe(Stream.runHead, Effect.forkScoped);
      yield* TestClock.adjust("500 millis");
      yield* Fiber.join(settled);

      const projection = yield* projectionStore.getThreadProjection(threadId);
      const request = (id: typeof oldRequestId) =>
        projection.runtimeRequests.find((candidate) => candidate.id === id);
      assert.equal(request(oldRequestId)?.responseCapability.type, "not_resumable");
      assert.equal(request(newRequestId)?.responseCapability.type, "live");
      assert.equal(projection.providerSessions.at(-1)?.status, replacementStatus);
    });

    yield* effect.pipe(
      Effect.provide(layerTest({ state, idleTimeoutMs: 60_000, flakyReleaseWrites: flaky })),
    );
  }),
);

it.effect("ProviderSessionManagerV2 keeps each failed release's cleanup", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const flaky: FlakyReleaseWrites = {
      failing: yield* Ref.make<"none" | "session" | "session-and-requests">("none"),
      failures: yield* Queue.unbounded<void>(),
    };
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const now = yield* DateTime.now;
      const firstThreadId = ThreadId.make("thread-provider-session-manager-release-each-a");
      const secondThreadId = ThreadId.make("thread-provider-session-manager-release-each-b");
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId: firstThreadId,
      });
      yield* eventSink.write({
        events: [
          yield* makeThreadCreatedEvent({ idAllocator, threadId: firstThreadId, now }),
          yield* makeThreadCreatedEvent({ idAllocator, threadId: secondThreadId, now }),
        ],
      });
      const secondThreadRequest = yield* makePendingRuntimeRequestEvents({
        idAllocator,
        threadId: secondThreadId,
        providerSessionId,
        providerThread: makeProviderThread({
          idAllocator,
          threadId: secondThreadId,
          providerSessionId,
          now,
        }),
        now,
      });
      yield* eventSink.write({ events: secondThreadRequest.events });
      const failRelease = (failedWrites: number) =>
        Effect.gen(function* () {
          yield* Ref.set(flaky.failing, "session-and-requests");
          assert.isTrue(Exit.isFailure(yield* Effect.exit(manager.close(providerSessionId))));
          yield* Effect.repeat(Queue.take(flaky.failures), { times: failedWrites - 1 });
          yield* Ref.set(flaky.failing, "none");
        });

      // The first session serves both threads. Its replacement serves one.
      yield* manager.open({
        threadId: firstThreadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });
      yield* manager.open({
        threadId: secondThreadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });
      // Both the session write and the second thread's request write fail.
      yield* failRelease(2);
      yield* TestClock.adjust("500 millis");
      yield* manager.open({
        threadId: firstThreadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });
      // Only the session write fails: the first thread has no requests.
      yield* failRelease(1);

      const settled = yield* eventSink
        .stream({
          threadId: secondThreadId,
          afterSequence: yield* eventSink.latestSequence({ threadId: secondThreadId }),
          eventType: "runtime-request.updated",
        })
        .pipe(Stream.runHead, Effect.forkScoped);
      yield* TestClock.adjust("500 millis");
      yield* Fiber.join(settled);

      const projection = yield* projectionStore.getThreadProjection(secondThreadId);
      assert.equal(projection.runtimeRequests.at(-1)?.responseCapability.type, "not_resumable");
    });

    yield* effect.pipe(
      Effect.provide(layerTest({ state, idleTimeoutMs: 60_000, flakyReleaseWrites: flaky })),
    );
  }),
);

it.effect("ProviderSessionManagerV2 settles a request the event pump persists during release", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const flaky: FlakyReleaseWrites = {
      failing: yield* Ref.make<"none" | "session" | "session-and-requests">("none"),
      failures: yield* Queue.unbounded<void>(),
      pauseRequestWrites: {
        paused: yield* Deferred.make<void>(),
        resume: yield* Deferred.make<void>(),
      },
    };
    const pause = flaky.pauseRequestWrites!;
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("thread-provider-session-manager-release-pump");
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });
      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      yield* manager.open({ threadId, providerSessionId, modelSelection, runtimePolicy });
      // The runtime creates the request a moment after the release starts.
      const createdAt = DateTime.add(now, { seconds: 1 });
      const pendingRequest = yield* makePendingRuntimeRequestEvents({
        idAllocator,
        threadId,
        providerSessionId,
        providerThread: makeProviderThread({
          idAllocator,
          threadId,
          providerSessionId,
          now: createdAt,
        }),
        now: createdAt,
      });
      const adapterEvents = (yield* Ref.get(state)).eventQueues.get(String(providerSessionId));
      assert.isDefined(adapterEvents);
      yield* Queue.offerAll(adapterEvents!, pendingRequest.providerEvents);
      // The event pump holds the request permit while it persists the request.
      yield* Deferred.await(pause.paused);
      const closed = yield* manager
        .close(providerSessionId)
        .pipe(Effect.forkScoped({ startImmediately: true }));
      yield* TestClock.adjust("1 second");
      yield* Deferred.succeed(pause.resume, undefined);
      yield* Fiber.join(closed);

      const projection = yield* projectionStore.getThreadProjection(threadId);
      const request = projection.runtimeRequests.find(
        (candidate) => candidate.id === pendingRequest.requestId,
      );
      assert.equal(request?.responseCapability.type, "not_resumable");
    });

    yield* effect.pipe(
      Effect.provide(layerTest({ state, idleTimeoutMs: 60_000, flakyReleaseWrites: flaky })),
    );
  }),
);

it.effect("ProviderSessionManagerV2 terminalizes a pending input transcript item on release", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const now = yield* DateTime.now;
      const projectId = yield* idAllocator.allocate.project({
        fixtureName: "provider-session-manager-request-expire",
      });
      const threadId = yield* idAllocator.allocate.thread({
        fixtureName: "provider-session-manager-request-expire",
        projectId,
      });
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });
      const providerThread = makeProviderThread({
        idAllocator,
        threadId,
        providerSessionId,
        now,
      });

      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      const pendingRequest = yield* makePendingRuntimeRequestEvents({
        idAllocator,
        threadId,
        providerSessionId,
        providerThread,
        now,
      });
      yield* eventSink.write({
        events: pendingRequest.events.map((event) =>
          event.type === "turn-item.updated"
            ? { ...event, payload: { ...event.payload, type: "user_input_request", questions: [] } }
            : event,
        ),
      });
      yield* manager.open({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });
      yield* manager.release({
        providerSessionId,
        reason: "runtime_error",
        detail: "process exited",
      });

      const projection = yield* projectionStore.getThreadProjection(threadId);
      const request = projection.runtimeRequests.at(-1);
      const requestNode = projection.nodes.find((node) => node.id === request?.nodeId);
      const requestTurnItem = projection.turnItems.find(
        (item) => item.type === "user_input_request" && item.requestId === request?.id,
      );

      assert.equal(request?.status, "expired");
      assert.equal(request?.responseCapability.type, "not_resumable");
      assert.equal(requestNode?.status, "failed");
      assert.equal(requestTurnItem?.status, "failed");
    });

    yield* effect.pipe(Effect.provide(layerTest({ state, idleTimeoutMs: 1000 })));
  }),
);

it.effect("ProviderSessionManagerV2 persists session-scoped runtime requests without a run", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const now = yield* DateTime.now;
      const projectId = yield* idAllocator.allocate.project({
        fixtureName: "provider-session-manager-session-request",
      });
      const threadId = yield* idAllocator.allocate.thread({
        fixtureName: "provider-session-manager-session-request",
        projectId,
      });
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });
      const providerThread = makeProviderThread({
        idAllocator,
        threadId,
        providerSessionId,
        now,
      });

      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      yield* manager.open({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });
      const pendingRequest = yield* makePendingRuntimeRequestEvents({
        idAllocator,
        threadId,
        providerSessionId,
        providerThread,
        now,
      });
      const afterSequence = yield* eventSink.latestSequence({ threadId });
      const persistedFiber = yield* eventSink.stream({ threadId, afterSequence }).pipe(
        Stream.filter(
          (stored) =>
            stored.event.type === "runtime-request.updated" ||
            stored.event.type === "node.updated" ||
            stored.event.type === "turn-item.updated",
        ),
        Stream.take(3),
        Stream.runCollect,
        Effect.forkScoped,
      );
      const adapterEvents = (yield* Ref.get(state)).eventQueues.get(String(providerSessionId));
      assert.isDefined(adapterEvents);
      yield* Queue.offerAll(adapterEvents!, pendingRequest.providerEvents);
      const persisted = Array.from(yield* Fiber.join(persistedFiber));

      assert.sameMembers(
        persisted.map((stored) => stored.event.type),
        ["runtime-request.updated", "node.updated", "turn-item.updated"],
      );
      const projection = yield* projectionStore.getThreadProjection(threadId);
      const request = projection.runtimeRequests.find(
        (candidate) => candidate.id === pendingRequest.requestId,
      );
      const node = projection.nodes.find((candidate) => candidate.id === pendingRequest.nodeId);
      const turnItem = projection.turnItems.find(
        (candidate) =>
          candidate.type === "approval_request" && candidate.requestId === pendingRequest.requestId,
      );
      assert.equal(request?.status, "pending");
      assert.equal(request?.providerTurnId, null);
      assert.equal(node?.runId, null);
      assert.equal(node?.status, "waiting");
      assert.equal(turnItem?.runId, null);
      assert.equal(turnItem?.status, "waiting");
    });

    yield* effect.pipe(Effect.provide(layerTest({ state, idleTimeoutMs: 1000 })));
  }),
);

it.effect(
  "ProviderSessionManagerV2 preserves item identity during eager native session activation",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const effect = Effect.gen(function* () {
        const eventSink = yield* EventSink.EventSinkV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
        const now = yield* DateTime.now;
        const projectId = yield* idAllocator.allocate.project({
          fixtureName: "provider-session-manager-request-expire",
        });
        const threadId = yield* idAllocator.allocate.thread({
          fixtureName: "provider-session-manager-request-expire",
          projectId,
        });
        const providerSessionId = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });
        const providerThread = makeProviderThread({
          idAllocator,
          threadId,
          providerSessionId,
          now,
        });

        yield* eventSink.write({
          events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
        });
        yield* eventSink.write({
          events: (yield* makePendingRuntimeRequestEvents({
            idAllocator,
            threadId,
            providerSessionId,
            providerThread,
            now,
          })).events,
        });
        yield* manager.open({
          threadId,
          providerSessionId,
          modelSelection,
          runtimePolicy,
          initialNativeThreadId: "native-import",
          initialProviderItemIdentityVersion: 2,
        });
        yield* manager.release({
          providerSessionId,
          reason: "runtime_error",
          detail: "process exited",
        });

        const projection = yield* projectionStore.getThreadProjection(threadId);
        const request = projection.runtimeRequests.at(-1);
        const requestNode = projection.nodes.find((node) => node.id === request?.nodeId);
        const requestTurnItem = projection.turnItems.find(
          (item) => item.type === "approval_request" && item.requestId === request?.id,
        );

        assert.equal(request?.status, "expired");
        assert.equal(request?.responseCapability.type, "not_resumable");
        assert.equal(requestNode?.status, "failed");
        assert.equal(requestTurnItem?.status, "failed");
      });

      yield* effect.pipe(
        Effect.provide(
          layerTest({
            state,
            idleTimeoutMs: 1000,
            beforeOpen: (input) =>
              Effect.sync(() => assert.equal(input.initialProviderItemIdentityVersion, 2)),
          }),
        ),
      );
    }),
);

it.effect(
  "ProviderSessionManagerV2 keeps a multi-thread session alive until all turns finish",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const effect = Effect.gen(function* () {
        const eventSink = yield* EventSink.EventSinkV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
        const now = yield* DateTime.now;
        const projectId = yield* idAllocator.allocate.project({
          fixtureName: "provider-session-manager-multi-thread-active",
        });
        const firstThreadId = yield* idAllocator.allocate.thread({
          fixtureName: "provider-session-manager-multi-thread-active-a",
          projectId,
        });
        const secondThreadId = yield* idAllocator.allocate.thread({
          fixtureName: "provider-session-manager-multi-thread-active-b",
          projectId,
        });
        const providerSessionId = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId: firstThreadId,
        });
        // Each thread has its own native thread on the shared session.
        const firstProviderThread = makeProviderThread({
          idAllocator,
          threadId: firstThreadId,
          providerSessionId,
          now,
          nativeThreadId: "native-thread-a",
        });
        const secondProviderThread = makeProviderThread({
          idAllocator,
          threadId: secondThreadId,
          providerSessionId,
          now,
          nativeThreadId: "native-thread-b",
        });
        const firstRunId = idAllocator.derive.run({ threadId: firstThreadId, ordinal: 1 });
        const secondRunId = idAllocator.derive.run({ threadId: secondThreadId, ordinal: 1 });
        const firstProviderTurnId = idAllocator.derive.providerTurn({
          driver: CODEX_DRIVER,
          nativeTurnId: "native-turn-a",
        });
        const secondProviderTurnId = idAllocator.derive.providerTurn({
          driver: CODEX_DRIVER,
          nativeTurnId: "native-turn-b",
        });

        yield* eventSink.write({
          events: [
            yield* makeThreadCreatedEvent({ idAllocator, threadId: firstThreadId, now }),
            yield* makeThreadCreatedEvent({ idAllocator, threadId: secondThreadId, now }),
          ],
        });
        const runtime = yield* manager.open({
          threadId: firstThreadId,
          providerSessionId,
          modelSelection,
          runtimePolicy,
        });
        yield* manager.open({
          threadId: secondThreadId,
          providerSessionId,
          modelSelection,
          runtimePolicy,
        });
        yield* runtime.events.pipe(Stream.runDrain, Effect.forkScoped);
        const firstAppThread = (yield* projectionStore.getThreadProjection(firstThreadId)).thread;
        const secondAppThread = (yield* projectionStore.getThreadProjection(secondThreadId)).thread;
        yield* runtime.startTurn({
          appThread: firstAppThread,
          threadId: firstThreadId,
          runId: firstRunId,
          runOrdinal: 1,
          providerTurnOrdinal: 1,
          attemptId: idAllocator.derive.runAttempt({ runId: firstRunId, attemptOrdinal: 1 }),
          rootNodeId: idAllocator.derive.rootNode({ runId: firstRunId }),
          providerThread: firstProviderThread,
          message: {
            createdBy: "user",
            creationSource: "web",
            messageId: yield* idAllocator.allocate.message({ threadId: firstThreadId, ordinal: 1 }),
            text: "first",
            attachments: [],
          },
          modelSelection,
          runtimePolicy,
        });
        yield* runtime.startTurn({
          appThread: secondAppThread,
          threadId: secondThreadId,
          runId: secondRunId,
          runOrdinal: 1,
          providerTurnOrdinal: 1,
          attemptId: idAllocator.derive.runAttempt({ runId: secondRunId, attemptOrdinal: 1 }),
          rootNodeId: idAllocator.derive.rootNode({ runId: secondRunId }),
          providerThread: secondProviderThread,
          message: {
            createdBy: "user",
            creationSource: "web",
            messageId: yield* idAllocator.allocate.message({
              threadId: secondThreadId,
              ordinal: 1,
            }),
            text: "second",
            attachments: [],
          },
          modelSelection,
          runtimePolicy,
        });

        const queue = (yield* Ref.get(state)).eventQueues.get(String(providerSessionId));
        assert.isDefined(queue);
        yield* Queue.offer(queue!, {
          type: "turn.terminal",
          driver: CODEX_DRIVER,
          providerThreadId: firstProviderThread.id,
          providerTurnId: firstProviderTurnId,
          runOrdinal: 1,
          status: "completed",
          failure: null,
          threadDisposition: "reusable",
        });
        yield* TestClock.adjust("2 seconds");
        yield* Effect.yieldNow;
        assert.equal((yield* Ref.get(state)).closeCount, 0);

        yield* Queue.offer(queue!, {
          type: "turn.terminal",
          driver: CODEX_DRIVER,
          providerThreadId: secondProviderThread.id,
          providerTurnId: secondProviderTurnId,
          runOrdinal: 1,
          status: "completed",
          failure: null,
          threadDisposition: "reusable",
        });
        yield* TestClock.adjust("1 second");
        yield* Effect.yieldNow;
        assert.equal((yield* Ref.get(state)).closeCount, 1);
      });

      yield* effect.pipe(Effect.provide(layerTest({ state, idleTimeoutMs: 1000 })));
    }),
);

it.effect(
  "ProviderSessionManagerV2 opens one shared runtime, broadcasts events, and detaches threads independently",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const effect = Effect.gen(function* () {
        const eventSink = yield* EventSink.EventSinkV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const now = yield* DateTime.now;
        const projectId = yield* idAllocator.allocate.project({
          fixtureName: "provider-session-manager-shared-runtime",
        });
        const firstThreadId = yield* idAllocator.allocate.thread({
          fixtureName: "provider-session-manager-shared-runtime-a",
          projectId,
        });
        const secondThreadId = yield* idAllocator.allocate.thread({
          fixtureName: "provider-session-manager-shared-runtime-b",
          projectId,
        });
        const providerSessionId = idAllocator.derive.providerSession({
          providerInstanceId: modelSelection.instanceId,
        });

        yield* eventSink.write({
          events: [
            yield* makeThreadCreatedEvent({ idAllocator, threadId: firstThreadId, now }),
            yield* makeThreadCreatedEvent({ idAllocator, threadId: secondThreadId, now }),
          ],
        });
        const firstProviderThread = makeProviderThread({
          idAllocator,
          threadId: firstThreadId,
          providerSessionId,
          now,
        });
        const secondProviderThread = makeProviderThread({
          idAllocator,
          threadId: secondThreadId,
          providerSessionId,
          now,
        });
        const firstRunId = idAllocator.derive.run({ threadId: firstThreadId, ordinal: 1 });
        yield* eventSink.write({
          events: [
            {
              id: yield* idAllocator.allocate.event({ threadId: firstThreadId }),
              type: "provider-thread.updated",
              threadId: firstThreadId,
              driver: CODEX_DRIVER,
              occurredAt: now,
              payload: firstProviderThread,
            },
            {
              id: yield* idAllocator.allocate.event({ threadId: firstThreadId }),
              type: "provider-turn.updated",
              threadId: firstThreadId,
              runId: firstRunId,
              driver: CODEX_DRIVER,
              occurredAt: now,
              payload: {
                id: idAllocator.derive.providerTurn({
                  driver: CODEX_DRIVER,
                  nativeTurnId: "native-turn-shared-runtime-a",
                }),
                providerThreadId: firstProviderThread.id,
                nodeId: idAllocator.derive.rootNode({ runId: firstRunId }),
                runAttemptId: null,
                nativeTurnRef: null,
                ordinal: 1,
                status: "running",
                startedAt: now,
                completedAt: null,
              },
            },
          ],
        });
        const firstRuntime = yield* manager.open({
          threadId: firstThreadId,
          providerSessionId,
          modelSelection,
          runtimePolicy,
        });
        const secondRuntime = yield* manager.open({
          threadId: secondThreadId,
          providerSessionId,
          modelSelection,
          runtimePolicy,
        });

        assert.strictEqual(firstRuntime, secondRuntime);
        assert.equal((yield* Ref.get(state)).openCount, 1);
        const resumeSecondThread = secondRuntime.resumeThread({
          providerThread: secondProviderThread,
          threadId: secondThreadId,
          modelSelection,
          runtimePolicy,
        });
        yield* resumeSecondThread;
        yield* resumeSecondThread;
        assert.equal((yield* Ref.get(state)).resumeCount, 1);
        yield* secondRuntime.resumeThread({
          providerThread: secondProviderThread,
          threadId: secondThreadId,
          modelSelection: { ...modelSelection, model: "gpt-5.4-mini" },
          runtimePolicy,
        });
        assert.equal((yield* Ref.get(state)).resumeCount, 2);
        yield* resumeSecondThread;
        assert.equal((yield* Ref.get(state)).resumeCount, 3);
        const subscribe = firstRuntime.subscribeEvents;
        assert.isDefined(subscribe);
        if (subscribe === undefined) return;
        const firstSubscription = yield* subscribe;
        const secondSubscription = yield* subscribe;
        const queue = (yield* Ref.get(state)).eventQueues.get(String(providerSessionId));
        assert.isDefined(queue);
        yield* Queue.offer(queue!, {
          type: "provider_session.updated",
          driver: CODEX_DRIVER,
          providerSession: firstRuntime.providerSession,
        });
        const received = yield* Effect.all([
          firstSubscription.events.pipe(Stream.runHead),
          secondSubscription.events.pipe(Stream.runHead),
        ]);
        assert.isTrue(received.every(Option.isSome));
        assert.isTrue(
          received.every(
            (event) => Option.isSome(event) && event.value.type === "provider_session.updated",
          ),
        );

        yield* manager.detach({ providerSessionId, threadId: secondThreadId });
        yield* manager.open({
          threadId: secondThreadId,
          providerSessionId,
          modelSelection,
          runtimePolicy,
        });
        yield* resumeSecondThread;
        assert.equal((yield* Ref.get(state)).resumeCount, 4);

        // The second thread has no persisted provider thread, so nothing is unloaded.
        assert.deepEqual((yield* Ref.get(state)).unloadedNativeThreadIds, []);

        yield* manager.detach({ providerSessionId, threadId: firstThreadId });
        assert.isTrue(Option.isSome(yield* manager.get(providerSessionId)));
        assert.equal((yield* Ref.get(state)).closeCount, 0);
        assert.equal((yield* Ref.get(state)).interruptCount, 1);
        // The runtime stays up for the second thread; the first thread's
        // native state is unloaded after its turn is interrupted.
        assert.deepEqual((yield* Ref.get(state)).unloadedNativeThreadIds, ["native-thread"]);

        yield* manager.detach({ providerSessionId, threadId: secondThreadId });
        yield* TestClock.adjust("1 second");
        yield* Effect.yieldNow;
        assert.equal((yield* Ref.get(state)).closeCount, 1);
      });

      yield* effect.pipe(Effect.provide(layerTest({ state, idleTimeoutMs: 1000 })));
    }),
);

it.effect(
  "ProviderSessionManagerV2 re-attaching a thread waits for its in-flight unload, then reloads it",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const unloadStarted = yield* Deferred.make<void>();
      const releaseUnload = yield* Deferred.make<void>();
      // Resumes the provider had served when the unload actually reached it.
      let resumesBeforeUnload: number | undefined;
      // The unload parks after detach removed the attachment, leaving the
      // window in which the same thread's next turn re-attaches it.
      const beforeUnload = Effect.gen(function* () {
        yield* Deferred.succeed(unloadStarted, undefined);
        yield* Deferred.await(releaseUnload);
        resumesBeforeUnload = (yield* Ref.get(state)).resumeCount;
      });
      const effect = Effect.gen(function* () {
        const eventSink = yield* EventSink.EventSinkV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const now = yield* DateTime.now;
        const projectId = yield* idAllocator.allocate.project({
          fixtureName: "provider-session-manager-unload-race",
        });
        const threadId = yield* idAllocator.allocate.thread({
          fixtureName: "provider-session-manager-unload-race-a",
          projectId,
        });
        const otherThreadId = yield* idAllocator.allocate.thread({
          fixtureName: "provider-session-manager-unload-race-b",
          projectId,
        });
        const providerSessionId = idAllocator.derive.providerSession({
          providerInstanceId: modelSelection.instanceId,
        });
        const providerThread = makeProviderThread({
          idAllocator,
          threadId,
          providerSessionId,
          now,
        });
        yield* eventSink.write({
          events: [
            yield* makeThreadCreatedEvent({ idAllocator, threadId, now }),
            yield* makeThreadCreatedEvent({ idAllocator, threadId: otherThreadId, now }),
            {
              id: yield* idAllocator.allocate.event({ threadId }),
              type: "provider-thread.updated",
              threadId,
              driver: CODEX_DRIVER,
              occurredAt: now,
              payload: providerThread,
            },
          ],
        });
        const runtime = yield* manager.open({
          threadId,
          providerSessionId,
          modelSelection,
          runtimePolicy,
        });
        // A second thread keeps the shared runtime up after the detach.
        yield* manager.open({
          threadId: otherThreadId,
          providerSessionId,
          modelSelection,
          runtimePolicy,
        });
        // Resuming re-attaches the thread to the shared runtime.
        const resume = runtime.resumeThread({
          providerThread,
          threadId,
          modelSelection,
          runtimePolicy,
        });
        yield* resume;

        const detach = yield* manager
          .detach({ providerSessionId, threadId })
          .pipe(Effect.forkScoped);
        yield* Deferred.await(unloadStarted);
        // The same thread's next turn re-attaches while the unload is parked.
        // Give it room to run: unfixed, it reaches the provider's resume
        // here; serialized, it waits for the unload.
        const reattach = yield* resume.pipe(Effect.forkScoped);
        yield* Effect.yieldNow;
        yield* Effect.yieldNow;
        yield* Deferred.succeed(releaseUnload, undefined);
        yield* Fiber.join(detach);
        yield* Fiber.join(reattach);

        // The unload reached the provider before the re-attached resume, so
        // that resume reloads the thread instead of being torn down after it.
        assert.equal(resumesBeforeUnload, 1);
        assert.equal((yield* Ref.get(state)).resumeCount, 2);
        assert.deepEqual((yield* Ref.get(state)).unloadedNativeThreadIds, ["native-thread"]);
        assert.isTrue(Option.isSome(yield* manager.get(providerSessionId)));
      });

      yield* effect.pipe(
        Effect.provide(layerTest({ state, idleTimeoutMs: 1000, beforeUnload })),
        Effect.scoped,
      );
    }),
);

function runIdleThreadUnloadScenario(
  name: string,
  scenario: (input: {
    readonly state: Ref.Ref<TestProviderRuntimeState>;
    readonly manager: ProviderSessionManager.ProviderSessionManagerV2Shape;
    readonly providerSessionId: ProviderSessionId;
    readonly threadA: ThreadId;
    /** Starts run `ordinal` on a thread and returns once the provider accepted it. */
    readonly startTurn: (threadId: ThreadId, ordinal: number) => Effect.Effect<void>;
    /** Ends run `ordinal` on a thread and waits for the session to process it. */
    readonly endTurn: (threadId: ThreadId, ordinal: number) => Effect.Effect<void>;
    readonly resume: (threadId: ThreadId) => Effect.Effect<void>;
  }) => Effect.Effect<void, ProviderSessionManager.ProviderSessionManagerV2Error>,
  options: { readonly hasPendingBackgroundWorkForThread?: Effect.Effect<boolean> } = {},
) {
  return Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    yield* Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const now = yield* DateTime.now;
      const threadA = ThreadId.make(`thread-provider-session-manager-${name}-a`);
      const threadB = ThreadId.make(`thread-provider-session-manager-${name}-b`);
      const providerSessionId = idAllocator.derive.providerSession({
        providerInstanceId: modelSelection.instanceId,
      });
      yield* eventSink.write({
        events: [
          yield* makeThreadCreatedEvent({ idAllocator, threadId: threadA, now }),
          yield* makeThreadCreatedEvent({ idAllocator, threadId: threadB, now }),
        ],
      });
      const runtime = yield* manager.open({
        threadId: threadA,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });
      yield* manager.open({ threadId: threadB, providerSessionId, modelSelection, runtimePolicy });
      const providerThreadOf = (threadId: ThreadId) =>
        makeProviderThread({
          idAllocator,
          threadId,
          providerSessionId,
          now,
          nativeThreadId: `native-${threadId}`,
        });
      const resume = (threadId: ThreadId) =>
        runtime
          .resumeThread({
            providerThread: providerThreadOf(threadId),
            threadId,
            modelSelection,
            runtimePolicy,
          })
          .pipe(Effect.asVoid, Effect.orDie);
      const startTurn = (threadId: ThreadId, ordinal: number) =>
        Effect.gen(function* () {
          yield* resume(threadId);
          const runId = idAllocator.derive.run({ threadId, ordinal });
          yield* runtime.startTurn({
            appThread: (yield* projectionStore.getThreadProjection(threadId)).thread,
            threadId,
            runId,
            runOrdinal: ordinal,
            providerTurnOrdinal: ordinal,
            attemptId: idAllocator.derive.runAttempt({ runId, attemptOrdinal: 1 }),
            rootNodeId: idAllocator.derive.rootNode({ runId }),
            providerThread: providerThreadOf(threadId),
            message: {
              createdBy: "user",
              creationSource: "web",
              messageId: yield* idAllocator.allocate.message({ threadId, ordinal }),
              text: "turn",
              attachments: [],
            },
            modelSelection,
            runtimePolicy,
          });
        }).pipe(Effect.orDie);
      const subscribe = runtime.subscribeEvents;
      assert.isDefined(subscribe);
      const endTurn = (threadId: ThreadId, ordinal: number) =>
        Effect.scoped(
          Effect.gen(function* () {
            // The pump hands an event to subscribers only after the session
            // has processed it, so receiving it is the receipt.
            const subscription = yield* Effect.acquireRelease(subscribe!, (sub) => sub.close);
            const received = yield* subscription.events.pipe(
              Stream.filter((event) => event.type === "turn.terminal"),
              Stream.runHead,
              Effect.forkScoped,
            );
            const queue = (yield* Ref.get(state)).eventQueues.get(String(providerSessionId));
            assert.isDefined(queue);
            yield* Queue.offer(queue!, {
              type: "turn.terminal",
              driver: CODEX_DRIVER,
              providerThreadId: providerThreadOf(threadId).id,
              providerTurnId: idAllocator.derive.providerTurn({
                driver: CODEX_DRIVER,
                nativeTurnId: `native-turn-${threadId}-${ordinal}`,
              }),
              runOrdinal: ordinal,
              status: "completed",
              failure: null,
              threadDisposition: "reusable",
            });
            yield* Fiber.join(received);
          }),
        ).pipe(Effect.orDie);

      // B's running turn keeps the shared runtime itself busy throughout.
      yield* startTurn(threadB, 1);
      yield* scenario({
        state,
        manager,
        providerSessionId,
        threadA,
        startTurn,
        endTurn,
        resume,
      });
    }).pipe(
      Effect.provide(
        layerTest({
          state,
          idleTimeoutMs: 1000,
          ...(options.hasPendingBackgroundWorkForThread === undefined
            ? {}
            : { hasPendingBackgroundWorkForThread: options.hasPendingBackgroundWorkForThread }),
        }),
      ),
    );
  });
}

it.effect(
  "ProviderSessionManagerV2 unloads a shared-runtime thread left idle, and reloads it on its next turn",
  () =>
    runIdleThreadUnloadScenario(
      "idle-unload",
      ({ state, manager, providerSessionId, threadA, startTurn, endTurn, resume }) =>
        Effect.gen(function* () {
          yield* startTurn(threadA, 1);
          assert.equal((yield* Ref.get(state)).resumeCount, 2);
          yield* endTurn(threadA, 1);

          // A follow-up before the timeout keeps the thread loaded.
          yield* TestClock.adjust("500 millis");
          yield* startTurn(threadA, 2);
          yield* TestClock.adjust("1 second");
          assert.deepEqual((yield* Ref.get(state)).unloadedNativeThreadIds, []);

          // Idle for the full timeout after its last turn, A is unloaded while
          // the runtime stays up for B.
          yield* endTurn(threadA, 2);
          yield* TestClock.adjust("1 second");
          assert.deepEqual((yield* Ref.get(state)).unloadedNativeThreadIds, [`native-${threadA}`]);
          assert.isTrue(Option.isSome(yield* manager.get(providerSessionId)));
          assert.equal((yield* Ref.get(state)).closeCount, 0);

          // The next turn's resume reaches the provider and reloads A.
          const resumes = (yield* Ref.get(state)).resumeCount;
          yield* resume(threadA);
          assert.equal((yield* Ref.get(state)).resumeCount, resumes + 1);
        }),
    ),
);

it.effect(
  "ProviderSessionManagerV2 keeps an idle shared-runtime thread loaded while its background work runs",
  () =>
    Effect.gen(function* () {
      const pendingWork = yield* Ref.make(true);
      yield* runIdleThreadUnloadScenario(
        "idle-unload-pinned",
        ({ state, threadA, startTurn, endTurn }) =>
          Effect.gen(function* () {
            yield* startTurn(threadA, 1);
            yield* endTurn(threadA, 1);
            yield* TestClock.adjust("3 seconds");
            assert.deepEqual((yield* Ref.get(state)).unloadedNativeThreadIds, []);

            yield* Ref.set(pendingWork, false);
            yield* TestClock.adjust("1 second");
            assert.deepEqual((yield* Ref.get(state)).unloadedNativeThreadIds, [
              `native-${threadA}`,
            ]);
          }),
        { hasPendingBackgroundWorkForThread: Ref.get(pendingWork) },
      );
    }),
);

it.effect(
  "ProviderSessionManagerV2 rejects a second thread when the provider runtime is exclusive",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const effect = Effect.gen(function* () {
        const eventSink = yield* EventSink.EventSinkV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const now = yield* DateTime.now;
        const projectId = yield* idAllocator.allocate.project({
          fixtureName: "provider-session-manager-exclusive-runtime",
        });
        const firstThreadId = yield* idAllocator.allocate.thread({
          fixtureName: "provider-session-manager-exclusive-runtime-a",
          projectId,
        });
        const secondThreadId = yield* idAllocator.allocate.thread({
          fixtureName: "provider-session-manager-exclusive-runtime-b",
          projectId,
        });
        const providerSessionId = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId: firstThreadId,
        });
        yield* eventSink.write({
          events: [
            yield* makeThreadCreatedEvent({ idAllocator, threadId: firstThreadId, now }),
            yield* makeThreadCreatedEvent({ idAllocator, threadId: secondThreadId, now }),
          ],
        });

        yield* manager.open({
          threadId: firstThreadId,
          providerSessionId,
          modelSelection,
          runtimePolicy,
        });
        const error = yield* manager
          .open({
            threadId: secondThreadId,
            providerSessionId,
            modelSelection,
            runtimePolicy,
          })
          .pipe(Effect.flip);

        assert.equal(error._tag, "ProviderSessionOpenError");
        assert.equal((yield* Ref.get(state)).openCount, 1);
      });

      yield* effect.pipe(
        Effect.provide(
          layerTest({ state, idleTimeoutMs: 1000, capabilities: ExclusiveCapabilities }),
        ),
      );
    }),
);

it.effect.each(["missing", "file"] as const)(
  "rejects a %s workspace before opening a provider session",
  (workspaceState) =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const root = yield* fileSystem.makeTempDirectoryScoped();
      const cwd = `${root}/workspace`;
      if (workspaceState === "file") yield* fileSystem.writeFileString(cwd, "not a directory");
      const state = yield* Ref.make(emptyState);
      yield* Effect.gen(function* () {
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const eventSink = yield* EventSink.EventSinkV2;
        const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const threadId = ThreadId.make(`thread-${workspaceState}-workspace`);
        const providerSessionId = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });
        yield* eventSink.write({
          events: [
            yield* makeThreadCreatedEvent({
              idAllocator,
              threadId,
              now: yield* DateTime.now,
            }),
          ],
        });
        const error = yield* manager
          .open({
            threadId,
            providerSessionId,
            modelSelection,
            runtimePolicy: { ...runtimePolicy, cwd },
          })
          .pipe(Effect.flip);
        assert.instanceOf(error, ProviderWorkspaceMissingError);
        assert.include(error.message, cwd);
        assert.include(error.message, "Restore the folder at this path before retrying.");
        assert.equal((yield* Ref.get(state)).openCount, 0);
        assert.isTrue(Option.isNone(yield* manager.get(providerSessionId)));
        assert.deepEqual(
          (yield* projectionStore.getThreadProjection(threadId)).providerSessions,
          [],
        );
      }).pipe(Effect.provide(layerTest({ state, idleTimeoutMs: 60_000 })));
    }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect(
  "rejects a deleted workspace before reusing a live session without changing its state",
  () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const root = yield* fileSystem.makeTempDirectoryScoped();
      const cwd = `${root}/workspace`;
      yield* fileSystem.makeDirectory(cwd);
      const state = yield* Ref.make(emptyState);
      yield* Effect.gen(function* () {
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const eventSink = yield* EventSink.EventSinkV2;
        const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const threadId = ThreadId.make("thread-deleted-live-workspace");
        const providerSessionId = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });
        yield* eventSink.write({
          events: [
            yield* makeThreadCreatedEvent({
              idAllocator,
              threadId,
              now: yield* DateTime.now,
            }),
          ],
        });
        const input = {
          threadId,
          providerSessionId,
          modelSelection,
          runtimePolicy: { ...runtimePolicy, cwd },
        };
        const runtime = yield* manager.open(input);
        const before = yield* projectionStore.getThreadProjection(threadId);
        yield* fileSystem.remove(cwd, { recursive: true });
        const error = yield* manager.open(input).pipe(Effect.flip);
        assert.instanceOf(error, ProviderWorkspaceMissingError);
        assert.equal((yield* Ref.get(state)).openCount, 1);
        assert.equal((yield* Ref.get(state)).closeCount, 0);
        assert.strictEqual(Option.getOrThrow(yield* manager.get(providerSessionId)), runtime);
        assert.deepEqual(
          (yield* projectionStore.getThreadProjection(threadId)).providerSessions,
          before.providerSessions,
        );
      }).pipe(Effect.provide(layerTest({ state, idleTimeoutMs: 60_000 })));
    }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect(
  "ProviderSessionManagerV2 applies project device access independently of browser access",
  () =>
    Effect.gen(function* () {
      const enabled = yield* runBrowserAccessScenario({
        enableAgentBrowserAccess: false,
        projectOverride: false,
        deviceOverride: true,
      });
      assert.isTrue(enabled?.capabilities?.has("device"));
      assert.isFalse(enabled?.browserToolsAvailable);
      const denied = yield* runBrowserAccessScenario({
        enableAgentBrowserAccess: false,
        projectOverride: false,
        deviceOverride: true,
        projectExists: false,
      });
      assert.isFalse(denied?.capabilities?.has("device"));
    }),
);
