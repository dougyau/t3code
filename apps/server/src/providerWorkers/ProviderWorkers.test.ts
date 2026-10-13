import { assert, describe, it } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as C from "@t3tools/contracts";
import type { ProviderAdapterV2SessionRuntime } from "@t3tools/provider-core/server/ProviderAdapter";
import type { PiWorkersPort } from "@t3tools/provider-pi/server";
import { WorkerInspectionError } from "../../../../packages/provider-pi/src/server/workerBridge.ts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as RpcTest from "effect/rpc/RpcTest";
import * as RpcAuthorization from "../auth/RpcAuthorization.ts";
import * as SessionStore from "../auth/SessionStore.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import type { McpInvocationScope } from "../mcp/McpInvocationContext.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ProviderSessionManager from "../orchestration-v2/ProviderSessionManager.ts";
import * as Workers from "./ProviderWorkers.ts";
import * as WorkerRpc from "./rpc.ts";
import { handoffWorkerRows, invalidHandoffWorkerRows } from "../../../../packages/contracts/src/providerWorkers.fixture.ts";
import { workerWireFixture } from "../../../../packages/provider-pi/src/server/workerBridge.wire.fixture.ts";
import { McpSchema, McpServer } from "effect/ai";
import * as McpHttpServer from "../mcp/McpHttpServer.ts";
import * as McpInvocationContext from "../mcp/McpInvocationContext.ts";
import * as McpToolAccessTestkit from "../mcp/McpToolAccess.testkit.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as ProviderRegistry from "../provider/ProviderRegistry.ts";
import * as ProviderAdapterRegistry from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ScheduledTaskService from "../scheduledTasks/ScheduledTaskService.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as SecretRequests from "../secrets/SecretRequests.ts";

describe("worker caller authority and view admission", () => {
  it.effect("conserves passive handoff pages through native decoder, service, RPC and registered MCP", () =>
    Effect.gen(function* () {
      const environmentId = C.EnvironmentId.make("handoff-environment");
      const threadId = C.ThreadId.make("handoff-thread");
      const providerSessionId = C.ProviderSessionId.make("handoff-session");
      const providerInstanceId = C.ProviderInstanceId.make("pi");
      const providerThreadId = C.ProviderThreadId.make("handoff-provider-thread");
      const shell = { ...McpToolAccessTestkit.liveThreadShell(threadId),
        providerInstanceId, activeProviderThreadId: providerThreadId, lastVisitedAt: null };
      const owner = { processId: "process", coordinatorSessionId: "session", epoch: "epoch" };
      const f = yield* workerWireFixture(owner);
      let client = f.client;
      const unexpected = () => { throw new Error("Passive inspection invoked private view or provider work"); };
      const port: PiWorkersPort = {
        driver: "pi", runtimeMode: "full-access", pending: false,
        ceilings: { history: true, route: true, control: true },
        observation: { state: "last_observation", owner, revision: 1, observedAt: 1 },
        observations: Stream.never, refresh: Effect.die("Unexpected passive refresh"),
        openView: unexpected, disconnectView: unexpected,
        read: (use) => Effect.tryPromise({ try: () => use(client),
          catch: (cause) => new WorkerInspectionError({ code: cause instanceof Error && "code" in cause
            && typeof cause.code === "string" ? cause.code : "inspection_failed" }) }),
      };
      const runtime: ProviderAdapterV2SessionRuntime = {
        instanceId: providerInstanceId, driver: C.ProviderDriverKind.make("pi"), providerSessionId, workers: port,
        get providerSession() { return unexpected(); },
        events: Stream.empty, ensureThread: unexpected, resumeThread: unexpected,
        startTurn: unexpected, steerTurn: unexpected, interruptTurn: unexpected,
        respondToRuntimeRequest: unexpected, readThreadSnapshot: unexpected,
        rollbackThread: unexpected, forkThread: unexpected,
      };
      const workersLayer = Workers.layer.pipe(Layer.provide(Layer.mergeAll(
        Layer.mock(SessionStore.SessionStore)({ cookieName: "test", legacyCookieName: undefined, isActive: unexpected }),
        Layer.mock(ServerEnvironment.ServerEnvironment)({ getEnvironmentId: Effect.succeed(environmentId) }),
        Layer.mock(ProjectionStore.ProjectionStoreV2)({
          getThreadShell: () => Effect.succeed(shell),
          getThreadProviderContext: () => Effect.succeed({
            thread: shell, providerSessions: [], providerThreads: [{
              id: providerThreadId, driver: C.ProviderDriverKind.make("pi"), providerInstanceId,
              providerSessionId, appThreadId: threadId, ownerNodeId: null, nativeThreadRef: null,
              nativeConversationHeadRef: null, status: "idle", firstRunOrdinal: null, lastRunOrdinal: null,
              handoffIds: [], forkedFrom: null, createdAt: shell.createdAt, updatedAt: shell.updatedAt,
            }],
          }),
        }),
        Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
          getResident: () => Effect.succeedSome(runtime),
        }),
      )));
      const session = { sessionId: C.AuthSessionId.make("handoff-auth"), subject: "fixture",
        method: "bearer-access-token" as const, scopes: [C.AuthOrchestrationReadScope] };
      const target = { environmentId, threadId };
      const viewer = { session, connectionId: "fixture" };
      const invocation: McpInvocationScope = {
        environmentId, issuedAt: 0, requestNamespace: "handoff-mcp",
        thread: { threadId, providerSessionId, providerInstanceId },
        client: undefined, capabilities: new Set(["orchestration"]),
      };
      const mcpClient = McpSchema.McpServerClient.of({
        clientId: 1, protocolVersion: "2025-06-18", clientCapabilities: {},
        clientInfo: { name: "handoff-fixture", version: "1" },
        initializePayload: { protocolVersion: "2025-06-18", capabilities: {},
          clientInfo: { name: "handoff-fixture", version: "1" } },
        getClient: Effect.die("Unexpected MCP client access"),
      });
      const mcpLayer = McpHttpServer.layerOrchestratorToolkit.pipe(
        Layer.provideMerge(McpServer.McpServer.layer),
        Layer.provide(McpToolAccessTestkit.liveThreadsLayer),
        Layer.provide(ServerSettings.layerTest()),
        Layer.provide(Layer.mock(ProviderRegistry.ProviderRegistry)({})),
        Layer.provide(Layer.mock(ProviderAdapterRegistry.ProviderAdapterRegistryV2)({})),
        Layer.provide(Layer.mock(ScheduledTaskService.ScheduledTaskService)({})),
        Layer.provide(Layer.mock(ProjectService.ProjectService)({})),
        Layer.provide(Layer.mock(SecretRequests.SecretRequests)({})),
      );
      const ref = { kind: "read" as const, ...owner, instanceId: "worker", source: "agent_run" as const };
      const detail = { state: "ready", ref, observedStatus: "completed", status: "completed",
        result: "unavailable", activity: "unavailable",
        workbrief: { text: "sync worker", offset: 0, nextOffset: null, source: "invocation task" },
        metadata: { text: "{}", offset: 0, nextOffset: null }, nextMetadata: null, nextBrief: null,
        controls: { grant: "denied" } } satisfies typeof C.WorkerDetail.Type;
      const result = { state: "unavailable", reason: "unsupported_kind" } satisfies typeof C.WorkerResult.Type;
      f.policy.detail = detail;
      f.policy.result = result;
      yield* Effect.gen(function* () {
        const service = yield* Workers.ProviderWorkers;
        const rpc = yield* RpcTest.makeClient(C.ProviderWorkersRpcGroup);
        const mcp = yield* McpServer.McpServer;
        const callMcp = (request: typeof C.WorkerMcpReadInput.Type["request"]) =>
          mcp.callTool({ name: "t3_workers_read", arguments: { request } }).pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
            Effect.provideService(McpSchema.McpServerClient, mcpClient),
          );
        for (const [index, items] of [handoffWorkerRows.slice(0, 50), handoffWorkerRows.slice(50)].entries()) {
          const roster = { owner, revision: 7, cursor: index === 0 ? "page-2" : null, items };
          const input = index === 0 ? target : { ...target, after: "page-2", revision: 7 };
          f.policy.roster = roster;
          assert.deepEqual(yield* service.list(viewer, input), roster);
          assert.deepEqual(yield* rpc[C.WS_METHODS.workerList](input), roster);
          const response = yield* callMcp({ operation: "list", threadId,
            ...(index === 0 ? {} : { after: "page-2", revision: 7 }) });
          assert.isFalse(response.isError ?? false);
          assert.deepEqual(response.structuredContent, roster);
          assert.deepEqual(f.requests.at(-1)?.params, index === 0 ? {} : { after: "page-2", revision: 7 });
        }
        assert.deepEqual(yield* service.detail(viewer, { ...target, id: "worker" }), detail);
        assert.deepEqual(yield* service.result(viewer, { ...target, ref }), result);
        assert.deepEqual(yield* rpc[C.WS_METHODS.workerDetail]({ ...target, id: "worker" }), detail);
        assert.deepEqual(yield* rpc[C.WS_METHODS.workerResult]({ ...target, ref }), result);
        assert.deepEqual((yield* callMcp({ operation: "detail", threadId, id: "worker" })).structuredContent, detail);
        assert.deepEqual((yield* callMcp({ operation: "result", threadId, ref })).structuredContent, result);
        for (const row of invalidHandoffWorkerRows) {
          f.policy.roster = { owner, revision: 7, cursor: null, items: [row] };
          const error = yield* service.list(viewer, target).pipe(Effect.flip);
          assert.equal(error.code, "invalid_reply");
          client = yield* Effect.promise(() => f.open());
          const rpcError = yield* rpc[C.WS_METHODS.workerList](target).pipe(Effect.flip);
          if (rpcError._tag !== "ProviderWorkersError") throw rpcError;
          assert.equal(rpcError.code, "invalid_reply");
          client = yield* Effect.promise(() => f.open());
          const mcpError = yield* callMcp({ operation: "list", threadId });
          assert.isTrue(mcpError.isError);
          assert.isUndefined(mcpError.structuredContent);
          const content = mcpError.content[0];
          if (content?.type !== "text") throw new Error("Expected declared MCP failure");
          assert.deepEqual(JSON.parse(content.text), {
            _tag: "OrchestratorMcpFailure", code: "provider_unavailable",
            message: "Worker inspection unavailable: invalid_reply.",
          });
          client = yield* Effect.promise(() => f.open());
        }
        f.policy.roster = { owner, revision: 8, cursor: null, items: handoffWorkerRows.slice(0, 1) };
        f.policy.pageError = "stale_cursor";
        assert.equal((yield* service.list(viewer, { ...target, after: "page-2", revision: 7 }).pipe(Effect.flip)).code, "stale_cursor");
        f.policy.pageError = undefined;
        assert.deepEqual(yield* service.list(viewer, target), f.policy.roster);
        assert.deepEqual(f.calls.slice(0, 13), ["connect",
          ...Array.from({ length: 6 }, () => "workers.list"),
          "workers.detail", "workers.result", "workers.detail", "workers.result", "workers.detail", "workers.result"]);
        assert.isTrue(f.calls.every((method) => ["connect", "workers.list", "workers.detail", "workers.result"].includes(method)));
      }).pipe(Effect.provide(Layer.mergeAll(
        C.ProviderWorkersRpcGroup.toLayer(WorkerRpc.makeHandlers(session)),
        RpcAuthorization.layer(session.scopes), mcpLayer,
      ).pipe(Layer.provideMerge(workersLayer))));
    }).pipe(Effect.scoped, Effect.provide(NodeCrypto.layer)),
  );
  it.effect("requires thread-scoped MCP reads and preserves ordinary RPC access and structured busy", () =>
    Effect.gen(function* () {
      const environmentId = C.EnvironmentId.make("workers-busy-environment");
      const threadId = C.ThreadId.make("workers-busy-thread");
      const providerSessionId = C.ProviderSessionId.make("workers-busy-session");
      const providerInstanceId = C.ProviderInstanceId.make("pi");
      const now = yield* DateTime.now;
      const providerThread: C.OrchestrationV2ProviderThread = {
        id: C.ProviderThreadId.make("workers-busy-provider-thread"),
        driver: C.ProviderDriverKind.make("pi"), providerInstanceId, providerSessionId, appThreadId: threadId,
        ownerNodeId: null, nativeThreadRef: null, nativeConversationHeadRef: null,
        status: "idle", firstRunOrdinal: null, lastRunOrdinal: null, handoffIds: [],
        forkedFrom: null, createdAt: now, updatedAt: now,
      };
      const thread: C.OrchestrationV2AppThread = {
        id: threadId, projectId: C.ProjectId.make("workers-busy-project"), title: "busy fixture",
        createdBy: "user", creationSource: "web", providerInstanceId,
        modelSelection: { instanceId: providerInstanceId, model: "fixture" }, runtimeMode: "full-access",
        interactionMode: "default", branch: null, worktreePath: null,
        activeProviderThreadId: providerThread.id,
        lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
        forkedFrom: null, createdAt: now, updatedAt: now, archivedAt: null,
        settledOverride: null, settledAt: null, lastVisitedAt: null, deletedAt: null,
      };
      const shell: C.OrchestrationV2ThreadShell = {
        ...thread, latestRunId: null, activeRunId: null, status: "idle",
        pendingRuntimeRequest: null, latestVisibleMessage: null, latestUserMessageAt: null,
        hasActionableProposedPlan: false, itemCount: 0, visibleItemCount: 0,
      };
      const owner = { processId: "fixture-process", coordinatorSessionId: "fixture-session", epoch: "1" };
      let opens = 0;
      let openingResources = 0;
      let disconnects = 0;
      const reads = { projection: 0, resident: 0, port: 0, list: 0 };
      const roster = { owner, revision: 1, cursor: null, items: [] };
      const unexpected = () => { throw new Error("Unexpected runtime or native operation"); };
      const client: Effect.Success<ReturnType<PiWorkersPort["openView"]>> = {
        owner, capabilities: [],
        get lease() { return unexpected(); },
        list: async () => { reads.list++; return roster; },
        subscribe: unexpected, detail: unexpected, requests: unexpected,
        activity: unexpected, result: unexpected, historyList: unexpected,
        historyRead: unexpected, routeRead: unexpected, grant: unexpected,
        input: unexpected, cancel: unexpected, requestStatus: unexpected,
        disconnect: async () => { disconnects++; },
      };
      const port: PiWorkersPort = {
        driver: "pi", runtimeMode: "full-access", pending: false,
        ceilings: { history: true, route: true, control: false },
        observation: { state: "last_observation", owner, observedAt: 1, revision: 1 },
        observations: Stream.never, refresh: Effect.void,
        disconnectView: (client) => Effect.promise(() => client.disconnect()),
        read: (use) => Effect.promise(() => { reads.port++; return use(client); }),
        openView: () => Effect.gen(function* () {
          opens++;
          if (opens === 1) {
            yield* Effect.acquireRelease(
              Effect.sync(() => { openingResources++; }),
              () => Effect.sync(() => { openingResources--; }),
            );
            return yield* new WorkerInspectionError({ code: "busy" });
          }
          return client;
        }),
      };
      // The resident lookup supplies the port only. Fail if inspection starts provider work.
      const runtime: ProviderAdapterV2SessionRuntime = {
        instanceId: providerInstanceId, driver: C.ProviderDriverKind.make("pi"), providerSessionId, workers: port,
        get providerSession() { return unexpected(); },
        events: Stream.empty, ensureThread: unexpected, resumeThread: unexpected,
        startTurn: unexpected, steerTurn: unexpected, interruptTurn: unexpected,
        respondToRuntimeRequest: unexpected, readThreadSnapshot: unexpected,
        rollbackThread: unexpected, forkThread: unexpected,
      };
      const workersLayer = Workers.layer.pipe(Layer.provide(Layer.mergeAll(
        Layer.mock(SessionStore.SessionStore)({ cookieName: "test", legacyCookieName: undefined,
          isActive: () => Effect.succeed(true) }),
        Layer.mock(ServerEnvironment.ServerEnvironment)({ getEnvironmentId: Effect.succeed(environmentId) }),
        Layer.mock(ProjectionStore.ProjectionStoreV2)({
          getThreadShell: () => Effect.sync(() => { reads.projection++; return shell; }),
          getThreadProviderContext: () => Effect.succeed({
            thread, providerThreads: [providerThread], providerSessions: [],
          }),
        }),
        Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
          getResident: () => Effect.sync(() => { reads.resident++; return runtime; }).pipe(Effect.asSome),
        }),
      )));
      const session = {
        sessionId: C.AuthSessionId.make("workers-busy-auth"), subject: "fixture",
        method: "bearer-access-token" as const, scopes: [C.AuthOrchestrationReadScope],
      };
      yield* Effect.gen(function* () {
        const service = yield* Workers.ProviderWorkers;
        const callerThread = { threadId, providerSessionId, providerInstanceId };
        const invocation: McpInvocationScope = {
          environmentId, issuedAt: 0, requestNamespace: "workers-mcp",
          thread: callerThread,
          client: undefined, capabilities: new Set(["orchestration"]),
        };
        const ref: typeof C.WorkerReference.Type = {
          kind: "read", ...owner, instanceId: "fixture-worker", source: "agent_spawn",
        };
        const requests: Array<typeof C.WorkerMcpReadInput.Type["request"]> = [
          { operation: "list", threadId },
          { operation: "detail", threadId, id: "fixture-worker" },
          { operation: "requests", threadId, id: "fixture-worker" },
          { operation: "activity", threadId, ref },
          { operation: "result", threadId, ref },
          { operation: "historyMetadata", threadId },
        ];
        for (const access of ["read-only", "full-access"] as const) {
          for (const request of requests) {
            const error = yield* service.readForMcp({
              ...invocation, thread: undefined,
              client: { sessionId: "oauth-fixture", label: "OAuth fixture", access },
            }, { request }).pipe(Effect.flip);
            assert.equal(error.code, "thread_credential_required");
          }
        }
        for (const request of requests) {
          for (const caller of [
            { ...invocation, thread: { ...callerThread, threadId: C.ThreadId.make("foreign-thread") } },
            { ...invocation, environmentId: C.EnvironmentId.make("wrong-environment") },
            { ...invocation, capabilities: new Set<never>() },
          ]) {
            const error = yield* service.readForMcp(caller, { request }).pipe(Effect.flip);
            assert.equal(error.code, caller.environmentId !== environmentId ? "provider_unavailable" : "capability_denied");
          }
        }
        assert.deepEqual(reads, { projection: 0, resident: 0, port: 0, list: 0 });
        assert.equal(opens, 0);
        assert.deepEqual(yield* service.readForMcp(invocation, { request: { operation: "list", threadId } }), roster);
        assert.deepEqual(reads, { projection: 1, resident: 1, port: 1, list: 1 });
      }).pipe(Effect.provide(workersLayer));
      yield* Effect.gen(function* () {
        const rpc = yield* RpcTest.makeClient(C.ProviderWorkersRpcGroup);
        const target = { environmentId, threadId };
        assert.deepEqual(yield* rpc[C.WS_METHODS.workerList](target), roster);
        assert.deepEqual(reads, { projection: 2, resident: 2, port: 2, list: 2 });
        const failedEvents: Array<typeof C.WorkerViewEvent.Type> = [];
        const error = yield* rpc[C.WS_METHODS.workerOpenView](target).pipe(
          Stream.runForEach((event) => Effect.sync(() => { failedEvents.push(event); })),
          Effect.flip,
        );
        assert.instanceOf(error, C.ProviderWorkersError);
        assert.equal(error.code, "busy");
        assert.deepEqual(failedEvents, []);
        assert.equal(opens, 1); // Reached the port with no local views occupying capacity.
        assert.equal(openingResources, 0);

        // Keep all three allowed streams alive: a leaked reservation would reject the third.
        const opened = yield* Queue.unbounded<typeof C.WorkerViewEvent.Type>();
        const fibers = [];
        const ids = new Set<string>();
        for (let index = 0; index < 3; index++) {
          fibers.push(yield* rpc[C.WS_METHODS.workerOpenView](target).pipe(
            Stream.runForEach((event) => Queue.offer(opened, event)), Effect.forkScoped,
          ));
          const event = yield* Queue.take(opened);
          if (event.type !== "opened") throw new Error("Expected opened view");
          assert.deepEqual(event.grants, []);
          ids.add(event.viewId);
        }
        assert.equal(ids.size, 3);
        assert.equal(opens, 4);
        assert.equal(disconnects, 0);
        for (const viewId of ids) {
          yield* rpc[C.WS_METHODS.workerCloseView]({ ...target, viewId });
        }
        for (const fiber of fibers) yield* Fiber.join(fiber);
        assert.equal(disconnects, 3);
      }).pipe(
        Effect.provide(Layer.mergeAll(
          C.ProviderWorkersRpcGroup.toLayer(WorkerRpc.makeHandlers(session)).pipe(
            Layer.provide(workersLayer),
          ),
          RpcAuthorization.layer(session.scopes),
        )),
      );
    }).pipe(Effect.scoped, Effect.provide(NodeCrypto.layer)),
  );
});
