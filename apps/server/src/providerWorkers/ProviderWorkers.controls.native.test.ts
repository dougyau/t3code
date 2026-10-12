// @effect-diagnostics nodeBuiltinImport:off -- Offline fixture owns the process, frontier and history.
import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import * as C from "@t3tools/contracts";
import type { PiWorkersPort } from "@t3tools/provider-pi/server";
import { nativeIntegration } from "../../../../packages/provider-pi/src/server/workerBridge.native.fixture.ts";
import { BridgeError, type BridgeClient, type AgentSelection, type SensitiveGrant } from "../../../../packages/provider-pi/src/server/pitako/bridge/client.js";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as HttpServer from "effect/http/HttpServer";
import * as HttpServerRequest from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import * as Socket from "effect/socket/Socket";
import { RpcClient, RpcServer, RpcSerialization } from "effect/rpc";
import * as ServerConfig from "../config.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as SessionStore from "../auth/SessionStore.ts";
import * as RpcAuthorization from "../auth/RpcAuthorization.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ProviderSessionManager from "../orchestration-v2/ProviderSessionManager.ts";
import * as Workers from "./ProviderWorkers.ts";
import * as WorkerRpc from "./rpc.ts";

const source = process.env.T3_PITAKO_NATIVE_TEST_ROOT;
const authLayer = EnvironmentAuth.layer.pipe(
  Layer.provideMerge(SqlitePersistence.layerMemory),
  Layer.provide(ServerSecretStore.layer),
  Layer.provide(ServerEnvironment.layerIdentity),
  Layer.provide(ServerConfig.layerTest(process.cwd(), { prefix: "t3-workers-controls-" })),
  Layer.provide(NodeServices.layer),
);

const integration = (mode: "full-access" | "approval-required" | "auto") =>
  nativeIntegration(false, ({ workers, runtime, providerThread, threadId,
    appThread, releaseFrontier, releaseSettlement, consumed }) => Effect.gen(function* () {
    const environmentId = C.EnvironmentId.make("control-native-environment");
    const target = { environmentId, threadId };
    const shell: C.OrchestrationV2ThreadShell = {
      ...appThread, runtimeMode: "full-access", activeProviderThreadId: providerThread.id,
      latestRunId: null, activeRunId: null, status: "idle", pendingRuntimeRequest: null,
      latestVisibleMessage: null, latestUserMessageAt: null, hasActionableProposedPlan: false,
      pendingBackgroundTasks: [], providerInstanceHistory: [], itemCount: 0, visibleItemCount: 0,
    };
    const admitted = yield* Queue.unbounded<typeof C.WorkerMutationStatus.Type>();
    const clients = new WeakMap<BridgeClient, BridgeClient>();
    const counters = { effects: 0, grant: 0, status: 0, opens: 0, resumes: 0, joins: 0 };
    let holdReply = false;
    let failSocket = false;
    let releaseReply: (() => void) | undefined;
    let lease: BridgeClient["lease"] | undefined;
    let stage = "open";
    const tracked: PiWorkersPort = {
      driver: workers.driver,
      ceilings: workers.ceilings,
      get pending() { return workers.pending; },
      get runtimeMode() { return workers.runtimeMode; },
      get observation() { return workers.observation; },
      observations: workers.observations,
      refresh: workers.refresh,
      read: (use) => workers.read(use),
      openView: (resume) => workers.openView(resume).pipe(Effect.map((client) => {
        counters.opens++;
        if (resume) {
          counters.resumes++;
          assert.isAbove(counters.joins, 0);
          assert.equal(resume.clientId, lease?.clientId);
          assert.equal(resume.resumeToken, lease?.resumeToken);
        } else lease = client.lease;
        const wrapped: BridgeClient = {
          ...client,
          get lease() { return client.lease; },
          grant: (grant: SensitiveGrant | "control", acknowledged: true, selection?: AgentSelection) => {
            counters.grant++;
            return grant === "control"
              ? client.grant(grant, acknowledged, selection!)
              : client.grant(grant, acknowledged);
          },
          input: async (...args) => {
            counters.effects++;
            const receipt = await client.input(...args);
            Queue.offerUnsafe(admitted, receipt);
            if (failSocket) {
              failSocket = false;
              await client.disconnect();
              throw new BridgeError("disconnected", "Fixture lost native response after real admission");
            }
            if (holdReply) await new Promise<void>((resolve) => { releaseReply = resolve; });
            return receipt;
          },
          cancel: (...args) => { counters.effects++; return client.cancel(...args); },
          requestStatus: async (sequence) => {
            counters.status++;
            const status = await client.requestStatus(sequence);
            releaseReply?.();
            holdReply = false;
            return status;
          },
        };
        clients.set(wrapped, client);
        return wrapped;
      })),
      disconnectView: (client) => {
        counters.joins++;
        return workers.disconnectView(clients.get(client) ?? client);
      },
    };
    const workerLayer = Workers.layer.pipe(Layer.provide(Layer.mergeAll(
      Layer.mock(ServerEnvironment.ServerEnvironment)({ getEnvironmentId: Effect.succeed(environmentId) }),
      Layer.mock(ProjectionStore.ProjectionStoreV2)({
        getThreadShell: () => Effect.succeed(shell),
        getThreadProviderContext: () => Effect.succeed({
          thread: { ...appThread, runtimeMode: "full-access" },
          providerSessions: [runtime.providerSession], providerThreads: [providerThread],
        }),
      }),
      Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
        getResident: () => Effect.succeedSome({ ...runtime, workers: tracked }),
      }),
    )));
    yield* Effect.gen(function* () {
      const auth = yield* EnvironmentAuth.EnvironmentAuth;
      const sessions = yield* SessionStore.SessionStore;
      const issued = yield* auth.issueSession({
        scopes: [C.AuthOrchestrationReadScope, C.AuthOrchestrationOperateScope, C.AuthDiagnosticsReadScope],
      });
      const verified = yield* sessions.verify(issued.token);
      const route = Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest;
        const session = yield* auth.authenticateWebSocketUpgrade(request);
        return yield* Effect.scoped(Effect.gen(function* () {
          const { protocol, httpEffect } = yield* RpcServer.makeProtocolWithHttpEffectWebsocket;
          yield* RpcServer.make(C.ProviderWorkersRpcGroup).pipe(
            Effect.provideService(RpcServer.Protocol, protocol),
            Effect.provide(Layer.mergeAll(
              C.ProviderWorkersRpcGroup.toLayer(WorkerRpc.makeHandlers(session)),
              RpcAuthorization.layer(session.scopes), RpcSerialization.layerJson,
            )), Effect.forkScoped,
          );
          return yield* httpEffect;
        }));
      }).pipe(Effect.orElseSucceed(() => HttpServerResponse.empty({ status: 401 })));
      yield* Layer.build(HttpServer.serve(route));
      const http = yield* HttpServer.HttpServer;
      if (http.address._tag === "UnixPathAddress") throw new Error("Expected TCP fixture server");
      const port = http.address.port;
      const connect = Effect.gen(function* () {
        const ticket = yield* auth.issueWebSocketTicket(verified);
        const context = yield* Layer.build(RpcClient.layerProtocolSocket().pipe(
          Layer.provide(Socket.layerWebSocket(`ws://127.0.0.1:${port}/ws?wsTicket=${encodeURIComponent(ticket.ticket)}`)),
          Layer.provide(Socket.layerWebSocketConstructorGlobal), Layer.provide(RpcSerialization.layerJson),
        ));
        return yield* RpcClient.make(C.ProviderWorkersRpcGroup).pipe(Effect.provide(context));
      });
      const a = yield* connect;
      const roster = yield* a[C.WS_METHODS.workerList](target);
      const worker = roster.items.find((item) => item.source === "agent_spawn");
      if (!worker) throw new Error("Expected fixture-owned worker");
      const events = yield* Queue.unbounded<typeof C.WorkerViewEvent.Type>();
      const stream = yield* a[C.WS_METHODS.workerOpenView](target).pipe(
        Stream.runForEach((event) => Queue.offer(events, event)), Effect.forkScoped,
      );
      const opened = yield* Queue.take(events);
      if (opened.type !== "opened") throw new Error("Expected private opening");
      const view = { ...target, viewId: opened.viewId };
      const decisionId = (ordinal: number) =>
        C.makeWorkerActionId(view.viewId, ordinal, "00000000-0000-0000-0000-000000000001");
      const detail = yield* a[C.WS_METHODS.workerControlDetail]({ ...view, id: worker.id });
      const selection = detail.controls.selection;
      if (!selection) throw new Error("Expected native controllable selection");
      yield* a[C.WS_METHODS.workerActivate]({ ...view, selection, acknowledged: true });
      const histories = yield* a[C.WS_METHODS.workerHistoryList]({ ...target, workerId: worker.id });
      assert.isTrue(histories.items.some((history) =>
        history.historyId === selection.historyId && history.native.state === "allocated" &&
        history.native.sessionId === selection.sessionId));
      const query: typeof C.WorkerActionInput.Type = {
        ...view, actionId: decisionId(1), selection,
        operation: { kind: "input", input: { intent: "query", text: "/literal-native-note", decisionId: "decision" } },
      };
      const steer = { ...query, actionId: decisionId(2), operation: {
        kind: "input" as const, input: { intent: "steer" as const, text: "native frontier steering note" },
      } };
      const lookup = { ...target, actionId: query.actionId, access: { viewId: view.viewId } };
      const inputReceipt = (outcome: typeof C.WorkerActionOutcome.Type) => {
        assert.equal(outcome.status.state, "recorded");
        if (outcome.status.state !== "recorded" || outcome.status.receipt.kind !== "input")
          throw new Error(`Expected actual native input receipt: ${JSON.stringify(outcome)}`);
        assert.equal(outcome.status.receipt.native.historyId, selection.historyId);
        assert.equal(outcome.status.receipt.native.sessionId, selection.sessionId);
        return outcome.status.receipt.native;
      };
      if (mode !== "full-access") {
        assert.equal(workers.runtimeMode, "approval-required");
        for (const input of [query, steer]) {
          const error = yield* a[C.WS_METHODS.workerAction](input).pipe(Effect.flip);
          assert.equal(error._tag === "ProviderWorkersError" && error.code, "full_access_required");
        }
        assert.equal(counters.effects, 0);
      } else {
        // The reply is held only AFTER the real broker has admitted native input.
        holdReply = true;
        stage = "await actual admission";
        const lostReply = yield* a[C.WS_METHODS.workerAction](query).pipe(Effect.forkScoped);
        const realAdmission = yield* Queue.take(admitted);
        assert.equal(realAdmission.state, "recorded");
        assert.equal((yield* a[C.WS_METHODS.workerAction](steer).pipe(Effect.flip))._tag, "ProviderWorkersError");
        assert.equal(counters.effects, 1);
        stage = "discard reply";
        yield* Fiber.interrupt(lostReply);
        stage = "alive lookup";
        const recovered = yield* a[C.WS_METHODS.workerReceipt](lookup);
        const nativeQuery = inputReceipt(recovered);
        assert.equal(nativeQuery.intent, "query");
        assert.equal(nativeQuery.status, "queued");
        assert.equal(counters.opens, 1);
        assert.equal(counters.joins, 0);
        assert.equal(counters.grant, 1);
        assert.deepEqual(yield* a[C.WS_METHODS.workerAction](query), recovered);
        stage = "steer";
        const nativeSteer = inputReceipt(yield* a[C.WS_METHODS.workerAction](steer));
        assert.equal(nativeSteer.intent, "steer");
        assert.equal(counters.effects, 2);
        const consumedInput: typeof C.WorkerActionInput.Type = { ...steer, actionId: decisionId(3) };
        failSocket = true;
        stage = "native loss";
        const failed = yield* a[C.WS_METHODS.workerAction](consumedInput);
        assert.deepEqual(failed.status, { state: "outcome_unknown", reason: "disconnected" });
        assert.equal(counters.effects, 3);
        // A new T3 upgrade of the same active AuthSession can use the opaque handle, not view authority.
        const b = yield* connect;
        stage = "recovery";
        assert.equal((yield* b[C.WS_METHODS.workerControlDetail]({ ...view, id: worker.id }).pipe(Effect.flip))._tag, "ProviderWorkersError");
        const resumed = yield* b[C.WS_METHODS.workerReceipt]({
          ...target, actionId: consumedInput.actionId, access: { recoveryHandle: opened.recoveryHandle },
        });
        const nativeResumed = inputReceipt(resumed);
        assert.equal(counters.resumes, 1);
        assert.equal(counters.grant, 1);
        assert.equal(counters.effects, 3);
        assert.deepEqual((yield* b[C.WS_METHODS.workerReceipt]({
          ...target, actionId: decisionId(4), access: { recoveryHandle: opened.recoveryHandle },
        })).status, { state: "outcome_unknown", reason: "mapping_evicted" });
        releaseFrontier();
        stage = "later frontier consumption";
        yield* consumed([nativeQuery.interactionId, nativeSteer.interactionId, nativeResumed.interactionId]);
        const activity = yield* a[C.WS_METHODS.workerActivity]({ ...target, ref: detail.ref });
        const activityText = JSON.stringify(activity);
        assert.include(activityText, nativeResumed.interactionId);
        assert.equal(counters.effects, 3);
        // No native grants are restored during a resumed lookup.
        assert.equal((yield* a[C.WS_METHODS.workerControlDetail]({ ...view, id: worker.id }).pipe(Effect.flip))._tag, "ProviderWorkersError");
      }
      // Cancel is separately admitted in supervised modes; its requested receipt is not settlement proof.
      if (mode !== "full-access") {
        const cancel = yield* a[C.WS_METHODS.workerAction]({ ...query, actionId: decisionId(3), operation: { kind: "cancel" } });
        assert.equal(cancel.status.state, "recorded");
        if (cancel.status.state !== "recorded") throw new Error("Expected cancel");
        assert.deepEqual(cancel.status.receipt, { kind: "cancel", native: {
          status: "requested", instanceId: selection.instanceId,
          historyId: selection.historyId, sessionId: selection.sessionId,
        } });
        assert.equal(counters.effects, 1);
      }
      releaseFrontier();
      releaseSettlement();
      stage = "close";
      yield* a[C.WS_METHODS.workerCloseView](view);
      yield* Fiber.join(stream);
      yield* sessions.revoke(verified.sessionId);
      const expired = yield* a[C.WS_METHODS.workerReceipt]({
        ...target, actionId: query.actionId, access: { recoveryHandle: opened.recoveryHandle },
      }).pipe(Effect.flip);
      assert.equal(expired._tag === "ProviderWorkersError" && expired.code, "forbidden");
    }).pipe(
      Effect.onInterrupt(() => Effect.logWarning(`Control native stage: ${stage}; counters: ${JSON.stringify(counters)}`)),
      Effect.provide(Layer.mergeAll(workerLayer, RpcSerialization.layerJson, NodeHttpServer.layerTest)
        .pipe(Layer.provideMerge(authLayer), Layer.provideMerge(NodeServices.layer))),
      Effect.scoped,
    );
  }).pipe(Effect.orDie), { runtimeMode: mode });

describe.skipIf(source === undefined)("native worker controls over authenticated T3 RPC", () => {
  it.live("admits notes once, consumes them at the later native frontier, and recovers without resending", () => integration("full-access"));
  it.live.each(["approval-required", "auto"] as const)(
    "rejects query/steer under immutable %s launch despite full-access thread preferences; cancel remains",
    (mode) => integration(mode),
  );
});
