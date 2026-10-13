import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as C from "@t3tools/contracts";
import type { PiWorkersPort } from "@t3tools/provider-pi/server";
import { BridgeError } from "../../../../packages/provider-pi/src/server/pitako/bridge/client.js";
import type { ProviderAdapterV2SessionRuntime } from "@t3tools/provider-core/server/ProviderAdapter";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as Option from "effect/Option";
import * as RpcTest from "effect/rpc/RpcTest";
import { Atom, AtomRegistry } from "effect/reactivity";
import { EnvironmentRegistry } from "../../../../packages/client-runtime/src/connection/registry.ts";
import { EnvironmentSupervisor } from "../../../../packages/client-runtime/src/connection/supervisor.ts";
import {
  PrimaryConnectionTarget,
  type NetworkStatus,
} from "../../../../packages/client-runtime/src/connection/model.ts";
import type { ConnectionCatalogEntry } from "../../../../packages/client-runtime/src/connection/catalog.ts";
import type { RpcSession } from "../../../../packages/client-runtime/src/rpc/session.ts";
import { createProviderWorkersEnvironmentAtoms } from "@t3tools/client-runtime/state/provider-workers";
import * as Workers from "../../../server/src/providerWorkers/ProviderWorkers.ts";
import * as WorkerRpc from "../../../server/src/providerWorkers/rpc.ts";
import * as RpcAuthorization from "../../../server/src/auth/RpcAuthorization.ts";
import * as SessionStore from "../../../server/src/auth/SessionStore.ts";
import * as ServerEnvironment from "../../../server/src/environment/ServerEnvironment.ts";
import * as ProjectionStore from "../../../server/src/orchestration-v2/ProjectionStore.ts";
import * as ProviderSessionManager from "../../../server/src/orchestration-v2/ProviderSessionManager.ts";
const isWorkerFailure = Schema.is(C.ProviderWorkersError);

/** Real service and RPC handlers, only the resident provider port and connection are fixtures. */
export const workerPanelFixture = Effect.gen(function* () {
  const target = {
    environmentId: C.EnvironmentId.make("panel-env"),
    threadId: C.ThreadId.make("panel-thread"),
  };
  const providerInstanceId = C.ProviderInstanceId.make("pi");
  const providerSessionId = C.ProviderSessionId.make("panel-provider-session");
  const now = yield* DateTime.now;
  const owner = {
    processId: "panel-process",
    coordinatorSessionId: "panel-coordinator",
    epoch: "epoch",
  };
  const ref = { kind: "read", ...owner, instanceId: "worker", source: "agent_spawn" } as const;
  const selection: typeof C.WorkerSelection.Type = {
    ...owner,
    instanceId: "worker",
    generation: 1,
    historyId: "history",
    sessionId: "child",
    target: { kind: "background", instanceId: "worker" },
  };
  const calls = {
    opens: 0,
    disconnects: 0,
    liveObservations: 0,
    list: 0,
    detail: [] as unknown[],
    grants: [] as string[],
    history: 0,
    route: 0,
    effects: [] as unknown[],
    receipts: [] as unknown[],
    result: [] as unknown[],
    requests: 0,
    activity: 0,
    historyList: 0,
  };
  const policy = {
    mode: "full-access" as PiWorkersPort["runtimeMode"],
    auth: true,
    invalidCursor: false,
    actionBusy: false,
    detailBusy: false,
    actionError: undefined as string | undefined,
    recordedError: undefined as "stale_selection" | undefined,
  };
  const cursor = (section: (typeof C.WorkerCursor.Type)["section"]) =>
    ({
      ref,
      section: section as "brief" | "detail_metadata" | "result_text" | "result_metadata",
      offset: 1024,
    }) as const;
  const detail = {
    state: "ready",
    ref,
    status: "running",
    observedStatus: "running",
    result: "pending",
    activity: "observed_binding",
    workbrief: { text: "worker brief", offset: 0, nextOffset: 1024, source: "invocation task" },
    metadata: { text: '{"requested":"fixture', offset: 0, nextOffset: 1024 },
    nextBrief: cursor("brief"),
    nextMetadata: cursor("detail_metadata"),
    controls: { grant: "denied", selection },
  } as const;
  type NativeClient = Effect.Success<ReturnType<PiWorkersPort["openView"]>>;
  type NativeStatus = Awaited<ReturnType<NativeClient["requestStatus"]>>;
  type Roster = Awaited<ReturnType<NativeClient["list"]>>;
  const data: { roster: Roster; detail: typeof C.WorkerDetail.Type } = {
    roster: {
      owner, revision: 1, cursor: null,
      items: [{
        id: "worker", kind: "background", roleId: "developer", status: "running",
        task: "fixture task", readRef: ref, source: "agent_spawn",
        selectedModel: "selected", requestedModel: "requested", requestedReasoning: "high",
        sdkRequest: null, acceptedAt: 1, activity: "observed_binding", phase: "working",
      }],
    },
    detail,
  };
  let observation: typeof C.WorkerObservation.Type = {
    state: "last_observation", owner, revision: 1, observedAt: 1,
  };
  const observations = yield* SubscriptionRef.make<typeof C.WorkerObservation.Type>(observation);
  const holds = new Map<"list" | "detail" | "action", {
    started: Deferred.Deferred<void>;
    reply: Deferred.Deferred<void, C.ProviderWorkersError>;
  }>();
  const holdNext = Effect.fn(function* (kind: "list" | "detail" | "action") {
    const started = yield* Deferred.make<void>();
    const reply = yield* Deferred.make<void, C.ProviderWorkersError>();
    holds.set(kind, { started, reply });
    return { started, reply };
  });
  const hold = async (kind: "list" | "detail" | "action") => {
    const gate = holds.get(kind);
    if (!gate) return;
    holds.delete(kind);
    // Reject with the public error, rather than runPromise's FiberFailure wrapper.
    const exit = await Effect.runPromise(
      Deferred.succeed(gate.started, undefined).pipe(Effect.andThen(Deferred.await(gate.reply)), Effect.result),
    );
    if (exit._tag === "Failure") throw exit.failure;
  };
  const receipts = new Map<number, NativeStatus>();
  const client: NativeClient = {
    owner,
    capabilities: [],
    lease: { clientId: "lease", resumeToken: "private-test-only", highWater: 0 },
    list: async () => {
      calls.list++;
      const snapshot = data.roster;
      await hold("list");
      return snapshot;
    },
    detail: async (_id, next) => {
      calls.detail.push(next);
      const snapshot = data.detail;
      await hold("detail");
      if (policy.detailBusy) throw new BridgeError("busy", "fixture pre-admission detail busy");
      if (next && policy.invalidCursor)
        throw new C.ProviderWorkersError({ code: "invalid_cursor" });
      return next && "section" in next && next.section === "detail_metadata"
        ? {
            ...snapshot,
            metadata: { text: ' continuation"}', offset: 1024, nextOffset: null },
            nextMetadata: null,
          }
        : next
          ? {
              ...snapshot,
              workbrief: {
                ...snapshot.workbrief,
                source: "invocation task",
                text: "brief continuation",
                offset: 1024,
                nextOffset: null,
              },
              nextBrief: null,
            }
          : { ...snapshot, controls: { ...snapshot.controls, selection } };
    },
    requests: async () => {
      calls.requests++;
      return ({
      state: "ready",
      ref,
      page: { text: "{}", offset: 0, nextOffset: null },
      next: null,
      });
    },
    activity: async () => {
      calls.activity++;
      return { state: "unavailable", reason: "not_captured" };
    },
    result: async (_ref, next) => {
      calls.result.push(next);
      return {
        state: "ready",
        ref,
        instanceId: "worker",
        generation: 1,
        status: "completed",
        text: {
          text: next ? "next result" : "first result",
          offset: next ? 1024 : 0,
          nextOffset: next ? null : 1024,
        },
        metadata: {
          text:
            next && "section" in next && next.section === "result_metadata"
              ? "true}"
              : '{"result":',
          offset: next && "section" in next && next.section === "result_metadata" ? 1024 : 0,
          nextOffset: next && "section" in next && next.section === "result_metadata" ? null : 1024,
        },
        nextText: next ? null : cursor("result_text"),
        nextMetadata:
          next && "section" in next && next.section === "result_metadata"
            ? null
            : cursor("result_metadata"),
      };
    },
    historyList: async () => {
      calls.historyList++;
      return ({
      items: [
        {
          kind: "member",
          groupId: "group",
          historyId: "history",
          roleId: "developer",
          coordinatorSessionId: owner.coordinatorSessionId,
          native: { state: "allocated", sessionId: "child", disposition: { state: "pending" } },
        },
      ],
      diagnostics: [],
      cursor: null,
      });
    },
    historyRead: async () => {
      calls.history++;
      return {
        items: [
          {
            historyId: "history",
            source: "worker",
            entryOffset: 0,
            byteOffset: 0,
            endsEntry: true,
            encoding: "base64",
            data: "eyJzZWNyZXQiOnRydWV9",
            validation: "json",
          },
        ],
        diagnostics: [],
        cursor: null,
      };
    },
    routeRead: async () => {
      calls.route++;
      return {
        state: "captured",
        page: { text: "private router capture", offset: 0, nextOffset: null },
      };
    },
    grant: async (grant) => {
      calls.grants.push(grant);
      return { grant, acknowledged: true };
    },
    input: async (_selection, input, sequence) => {
      calls.effects.push(input);
      await hold("action");
      if (policy.actionBusy) throw new BridgeError("busy", "fixture native envelope busy");
      if (policy.actionError) throw new BridgeError(policy.actionError, "fixture native request rejection");
      const status: NativeStatus = {
        state: "recorded",
        sequence,
        receipt: policy.recordedError ? { kind: "error", code: policy.recordedError } : {
          kind: "input",
          native: {
            interactionId: `input-${sequence}`,
            historyId: "history",
            sessionId: "child",
            intent: input.intent,
            status: "queued",
          },
        },
      };
      receipts.set(sequence, status);
      return status;
    },
    cancel: async (_selection, sequence) => {
      calls.effects.push("cancel");
      await hold("action");
      const status: NativeStatus = {
        state: "recorded",
        sequence,
        receipt: {
          kind: "cancel",
          native: {
            status: "requested",
            instanceId: "worker",
            historyId: "history",
            sessionId: "child",
          },
        },
      };
      receipts.set(sequence, status);
      return status;
    },
    requestStatus: async (sequence) => {
      calls.receipts.push(sequence);
      return receipts.get(sequence) ?? { state: "not_admitted", sequence };
    },
    subscribe: () => {
      throw new Error("No private native subscription");
    },
    disconnect: async () => {
      calls.disconnects++;
    },
  };
  const port: PiWorkersPort = {
    driver: "pi",
    get runtimeMode() {
      return policy.mode;
    },
    pending: false,
    ceilings: { history: true, route: true, control: true },
    get observation() { return observation; },
    observations: Stream.unwrap(
      Effect.acquireRelease(
        Effect.sync(() => {
          calls.liveObservations++;
          return SubscriptionRef.changes(observations);
        }),
        () =>
          Effect.sync(() => {
            calls.liveObservations--;
          }),
      ),
    ),
    refresh: Effect.void,
    read: (use) =>
      Effect.tryPromise({
        try: () => use(client),
        catch: (error) => isWorkerFailure(error)
          ? error
          : new C.ProviderWorkersError({ code: "fixture_failure" }),
      }),
    disconnectView: (view) => Effect.promise(() => view.disconnect()),
    openView: () =>
      Effect.sync(() => {
        calls.opens++;
        return client;
      }),
  };
  const unexpected = () => {
    throw new Error("Inspection must not start provider work");
  };
  const runtime: ProviderAdapterV2SessionRuntime = {
    instanceId: providerInstanceId,
    driver: C.ProviderDriverKind.make("pi"),
    providerSessionId,
    workers: port,
    get providerSession() {
      return unexpected();
    },
    events: Stream.empty,
    ensureThread: unexpected,
    resumeThread: unexpected,
    startTurn: unexpected,
    steerTurn: unexpected,
    interruptTurn: unexpected,
    respondToRuntimeRequest: unexpected,
    readThreadSnapshot: unexpected,
    rollbackThread: unexpected,
    forkThread: unexpected,
  };
  const thread: C.OrchestrationV2AppThread = {
    id: target.threadId,
    projectId: C.ProjectId.make("project"),
    title: "idle coordinator",
    createdBy: "user",
    creationSource: "web",
    providerInstanceId,
    modelSelection: { instanceId: providerInstanceId, model: "fixture" },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: C.ProviderThreadId.make("provider-thread"),
    forkedFrom: null,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: target.threadId },
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  };
  const providerThread: C.OrchestrationV2ProviderThread = {
    id: thread.activeProviderThreadId!,
    driver: C.ProviderDriverKind.make("pi"),
    providerInstanceId,
    providerSessionId,
    appThreadId: target.threadId,
    ownerNodeId: null,
    nativeThreadRef: null,
    nativeConversationHeadRef: null,
    status: "idle",
    firstRunOrdinal: null,
    lastRunOrdinal: null,
    handoffIds: [],
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
  };
  const service = yield* Workers.make.pipe(
    Effect.provide(
      Layer.mergeAll(
        NodeCrypto.layer,
        Layer.mock(SessionStore.SessionStore)({
          cookieName: "fixture",
          legacyCookieName: undefined,
          isActive: () => Effect.succeed(policy.auth),
        }),
        Layer.mock(ServerEnvironment.ServerEnvironment)({
          getEnvironmentId: Effect.succeed(target.environmentId),
        }),
        Layer.mock(ProjectionStore.ProjectionStoreV2)({
          getThreadShell: () =>
            Effect.succeed({
              ...thread,
              latestRunId: null,
              activeRunId: null,
              status: "idle",
              pendingRuntimeRequest: null,
              latestVisibleMessage: null,
              latestUserMessageAt: null,
              hasActionableProposedPlan: false,
              itemCount: 0,
              visibleItemCount: 0,
            }),
          getThreadProviderContext: () =>
            Effect.succeed({ thread, providerThreads: [providerThread], providerSessions: [] }),
        }),
        Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
          getResident: () => Effect.succeedSome(runtime),
        }),
      ),
    ),
  );
  const scopes = [
    C.AuthOrchestrationReadScope,
    C.AuthDiagnosticsReadScope,
    C.AuthOrchestrationOperateScope,
  ];
  const rpc = yield* RpcTest.makeClient(C.ProviderWorkersRpcGroup).pipe(
    Effect.provide(
      C.ProviderWorkersRpcGroup.toLayer(
        WorkerRpc.makeHandlers({
          sessionId: C.AuthSessionId.make("panel-auth"),
          subject: "fixture",
          method: "bearer-access-token",
          scopes,
        }),
      ).pipe(
        Layer.provideMerge(
          Layer.mergeAll(
            Layer.succeed(Workers.ProviderWorkers, service),
            RpcAuthorization.layer(scopes),
            NodeCrypto.layer,
          ),
        ),
      ),
    ),
  );
  const session = { client: rpc } as unknown as RpcSession;
  const sessionRef = yield* SubscriptionRef.make(Option.some(session));
  const supervisor = {
    session: sessionRef,
    target: new PrimaryConnectionTarget({
      ...target,
      label: "fixture",
      httpBaseUrl: "http://fixture",
      wsBaseUrl: "ws://fixture",
    }),
  } as unknown as EnvironmentSupervisor["Service"];
  const atomRuntime = Atom.runtime(
    Layer.mock(EnvironmentRegistry)({
      entries: yield* SubscriptionRef.make<ReadonlyMap<C.EnvironmentId, ConnectionCatalogEntry>>(
        new Map(),
      ),
      networkStatus: yield* SubscriptionRef.make<NetworkStatus>("online"),
      run: (_id, effect) => Effect.provideService(effect, EnvironmentSupervisor, supervisor),
      followStream: (_id, stream) =>
        Stream.provideService(stream, EnvironmentSupervisor, supervisor),
    }),
  );
  let nonce = 0;
  const workers = createProviderWorkersEnvironmentAtoms(
    atomRuntime,
    () => `00000000-0000-0000-0000-${String(++nonce).padStart(12, "0")}`,
  );
  const registry = AtomRegistry.make();
  yield* Effect.addFinalizer(() => Effect.sync(() => registry.dispose()));
  return { target, calls, policy, workers, registry, sessionRef, data, owner, holdNext,
    notify: (next: typeof C.WorkerObservation.Type) => Effect.sync(() => { observation = next; })
      .pipe(Effect.andThen(SubscriptionRef.set(observations, next))),
  };
});
