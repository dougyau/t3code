// @effect-diagnostics nodeBuiltinImport:off -- The offline fixture owns its histories and native process.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import * as C from "@t3tools/contracts";
import type { PiWorkersPort } from "@t3tools/provider-pi/server";
import { nativeIntegration } from "../../../../packages/provider-pi/src/server/workerBridge.native.fixture.ts";
import { connect as connectNative } from "../../../../packages/provider-pi/src/server/pitako/bridge/client.js";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
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
import type { McpInvocationScope } from "../mcp/McpInvocationContext.ts";

const source = process.env.T3_PITAKO_NATIVE_TEST_ROOT;
const authLayer = EnvironmentAuth.layer.pipe(
  Layer.provideMerge(SqlitePersistence.layerMemory),
  Layer.provide(ServerSecretStore.layer),
  Layer.provide(ServerEnvironment.layerIdentity),
  Layer.provide(ServerConfig.layerTest(process.cwd(), { prefix: "t3-workers-auth-" })),
  Layer.provide(NodeServices.layer),
);

describe.skipIf(source === undefined)("worker views over authenticated WebSockets and actual Pitako", () => {
  it.live("isolates grants, preserves native bytes, and releases private slots without starting work", () =>
    nativeIntegration(false, ({ workers, runtime, providerThread, providerSessionId, threadId, appThread, root }) =>
      Effect.gen(function* () {
        const environmentId = C.EnvironmentId.make("workers-native-environment");
        const target = { environmentId, threadId };
        let stage = "auth";
        const shell: C.OrchestrationV2ThreadShell = {
          ...appThread, activeProviderThreadId: providerThread.id,
          latestRunId: null, activeRunId: null, status: "idle", pendingRuntimeRequest: null,
          latestVisibleMessage: null, latestUserMessageAt: null, hasActionableProposedPlan: false,
          pendingBackgroundTasks: [], providerInstanceHistory: [], itemCount: 0, visibleItemCount: 0,
        };
        const foreignThreadId = C.ThreadId.make("foreign");
        const foreignProviderThreadId = C.ProviderThreadId.make("foreign-provider-thread");
        const foreignThread = { ...appThread, id: foreignThreadId, activeProviderThreadId: foreignProviderThreadId };
        const foreignProviderThread = { ...providerThread, id: foreignProviderThreadId, appThreadId: foreignThreadId };
        const nativeReads = { history: 0, route: 0 };
        let ceilings = workers.ceilings;
        let residentAvailable = true;
        let workerSupported = true;
        const { workers: _workers, ...runtimeWithoutWorkers } = runtime;
        const trackedWorkers: PiWorkersPort = {
          ...workers,
          get observation() { return workers.observation; },
          get ceilings() { return ceilings; },
          openView: (resume) => workers.openView(resume).pipe(Effect.map((client) => ({
            ...client,
            historyRead: (options) => { nativeReads.history++; return client.historyRead(options); },
            routeRead: (id, offset) => { nativeReads.route++; return client.routeRead(id, offset); },
          }))),
        };
        const workerLayer = Workers.layer.pipe(Layer.provide(Layer.mergeAll(
          Layer.mock(ServerEnvironment.ServerEnvironment)({ getEnvironmentId: Effect.succeed(environmentId) }),
          Layer.mock(ProjectionStore.ProjectionStoreV2)({
            getThreadShell: (id) => Effect.succeed(id === threadId ? shell : id === foreignThreadId ? {
              ...shell, id, activeProviderThreadId: foreignProviderThreadId,
            } : null),
            getThreadProviderContext: (id) => Effect.succeed({
              thread: id === foreignThreadId ? foreignThread : appThread,
              providerSessions: [runtime.providerSession],
              providerThreads: [id === foreignThreadId ? foreignProviderThread : providerThread],
            }),
          }),
          Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
            getResident: (id) => Effect.succeed(id === providerSessionId && residentAvailable
              ? Option.some(workerSupported ? { ...runtime, workers: trackedWorkers } : runtimeWithoutWorkers)
              : Option.none()),
          }),
        )));
        yield* Effect.gen(function* () {
          const auth = yield* EnvironmentAuth.EnvironmentAuth;
          const sessions = yield* SessionStore.SessionStore;
          const issued = yield* auth.issueSession({ scopes: [C.AuthOrchestrationReadScope, C.AuthDiagnosticsReadScope] });
          const verified = yield* sessions.verify(issued.token);
          const service = yield* Workers.ProviderWorkers;
          const upgraded = yield* Queue.unbounded<C.AuthSessionId>();
          const route = Effect.gen(function* () {
            const request = yield* HttpServerRequest.HttpServerRequest;
            if (!request.url.startsWith("/ws?")) return HttpServerResponse.empty({ status: 404 });
            const session = yield* auth.authenticateWebSocketUpgrade(request);
            yield* Queue.offer(upgraded, session.sessionId);
            return yield* Effect.scoped(Effect.gen(function* () {
              const { protocol, httpEffect } = yield* RpcServer.makeProtocolWithHttpEffectWebsocket;
              yield* RpcServer.make(C.ProviderWorkersRpcGroup).pipe(
                Effect.provideService(RpcServer.Protocol, protocol),
                Effect.provide(Layer.mergeAll(
                  C.ProviderWorkersRpcGroup.toLayer(WorkerRpc.makeHandlers(session)),
                  RpcAuthorization.layer(session.scopes),
                  RpcSerialization.layerJson,
                )),
                Effect.forkScoped,
              );
              return yield* Effect.raceFirst(httpEffect, sessions.awaitInvalidation(session.sessionId).pipe(
                Effect.as(HttpServerResponse.empty()),
              ));
            }));
          }).pipe(Effect.orElseSucceed(() => HttpServerResponse.empty({ status: 401 })));
          yield* Layer.build(HttpServer.serve(route));
          const http = yield* HttpServer.HttpServer;
          if (http.address._tag === "UnixPathAddress") return yield* Effect.die("Expected TCP test listener");
          const port = http.address.port;
          const connect = (session = verified) => Effect.gen(function* () {
            const ticket = yield* auth.issueWebSocketTicket(session);
            const context = yield* Layer.build(RpcClient.layerProtocolSocket().pipe(
                Layer.provide(Socket.layerWebSocket(`ws://127.0.0.1:${port}/ws?wsTicket=${encodeURIComponent(ticket.ticket)}`)),
                Layer.provide(Socket.layerWebSocketConstructorGlobal),
                Layer.provide(RpcSerialization.layerJson),
              ));
            return yield* RpcClient.make(C.ProviderWorkersRpcGroup).pipe(Effect.provide(context));
          });
          const a = yield* connect();
          const b = yield* connect();
          const roster = yield* a[C.WS_METHODS.workerList](target);
          assert.deepEqual(yield* b[C.WS_METHODS.workerList](target), roster);
          assert.equal(yield* Queue.take(upgraded), verified.sessionId);
          assert.equal(yield* Queue.take(upgraded), verified.sessionId);
          assert.equal(roster.items.length, 2);
          const worker = roster.items.find((item) => item.source === "agent_spawn");
          if (!worker) throw new Error("Expected fixture-owned background worker");
          residentAvailable = false;
          const absent = yield* a[C.WS_METHODS.workerList](target).pipe(Effect.flip);
          assert.equal(absent._tag === "ProviderWorkersError" && absent.code, "runtime_absent");
          assert.deepEqual(yield* a[C.WS_METHODS.workerObserve](target).pipe(Stream.runHead), Option.some({ state: "runtime_absent" }));
          residentAvailable = true;
          workerSupported = false;
          const unsupported = yield* a[C.WS_METHODS.workerList](target).pipe(Effect.flip);
          assert.equal(unsupported._tag === "ProviderWorkersError" && unsupported.code, "unsupported");
          workerSupported = true;
          const invocation: McpInvocationScope = {
            environmentId, issuedAt: 0, requestNamespace: "native-workers-test", capabilities: new Set(["orchestration"]),
            thread: { threadId, providerSessionId, providerInstanceId: runtime.providerSession.providerInstanceId }, client: undefined,
          };
          assert.deepEqual(yield* service.readForMcp(invocation, { request: { operation: "list", threadId } }), roster);
          assert.equal((yield* service.readForMcp(invocation, {
            request: { operation: "list", threadId: C.ThreadId.make("foreign") },
          }).pipe(Effect.flip)).code, "capability_denied");
          assert.equal((yield* service.readForMcp({ ...invocation, capabilities: new Set() }, {
            request: { operation: "list", threadId },
          }).pipe(Effect.flip)).code, "capability_denied");
          stage = "ordinary pages";
          const detail = yield* a[C.WS_METHODS.workerDetail]({ ...target, id: worker.id });
          assert.isNotNull(detail.nextBrief);
          const brief = yield* a[C.WS_METHODS.workerDetail]({
            ...target, id: worker.id, cursor: detail.nextBrief!,
          });
          assert.isUndefined(brief.metadata);
          assert.isUndefined(brief.nextMetadata);
          assert.equal(brief.workbrief!.offset, detail.workbrief!.nextOffset);
          assert.equal((yield* a[C.WS_METHODS.workerRequests]({ ...target, id: worker.id })).state, "ready");
          assert.equal((yield* a[C.WS_METHODS.workerActivity]({ ...target, ref: detail.ref })).state, "ready");
          const firstResult = yield* a[C.WS_METHODS.workerResult]({ ...target, ref: detail.ref });
          if (firstResult.state !== "ready") throw new Error("Expected captured result");
          let result = firstResult;
          let resultText = result.text!.text;
          assert.isNotNull(result.nextText);
          while (result.nextText) {
            const continuation = yield* a[C.WS_METHODS.workerResult]({ ...target, ref: detail.ref, cursor: result.nextText });
            if (continuation.state !== "ready") throw new Error("Expected result continuation");
            assert.isUndefined(continuation.metadata);
            assert.isUndefined(continuation.nextMetadata);
            assert.equal(continuation.text!.offset, result.text!.nextOffset);
            resultText += continuation.text!.text;
            result = continuation;
          }
          assert.equal(resultText, "native result 😀".repeat(200));
          const start = (client: typeof a) => Effect.gen(function* () {
            const events = yield* Queue.unbounded<typeof C.WorkerViewEvent.Type>();
            const fiber = yield* client[C.WS_METHODS.workerOpenView](target).pipe(
              Stream.runForEach((event) => Queue.offer(events, event)), Effect.forkScoped,
            );
            const opened = yield* Effect.raceFirst(
              Queue.take(events),
              Fiber.join(fiber).pipe(Effect.andThen(Effect.die("Private stream ended before opening"))),
            );
            if (opened.type !== "opened") throw new Error("Expected private view opening");
            assert.notEqual(opened.viewId, opened.recoveryHandle);
            assert.deepEqual(opened.grants, []);
            return { opened, events, fiber };
          });
          const one = yield* start(a);
          const two = yield* start(b);
          const input = { ...target, viewId: one.opened.viewId };
          const other = { ...target, viewId: two.opened.viewId };
          const histories = yield* a[C.WS_METHODS.workerHistoryList]({ ...target, workerId: worker.id });
          assert.isAbove(histories.items.length, 0);
          const history = histories.items[0]!;
          const read = { ...input, historyId: history.historyId, groupId: history.groupId, workerId: worker.id };
          const denied = yield* a[C.WS_METHODS.workerHistoryRead](read).pipe(Effect.flip);
          assert.equal(denied._tag === "ProviderWorkersError" && denied.code, "acknowledgement_required");
          assert.equal(nativeReads.history, 0);
          ceilings = { history: false, route: false, control: false };
          for (const grant of ["history", "route"] as const) {
            const ceilingDenied = yield* a[C.WS_METHODS.workerAcknowledge]({
              ...input, grant, acknowledged: true,
            }).pipe(Effect.flip);
            assert.equal(ceilingDenied._tag === "ProviderWorkersError" && ceilingDenied.code, "ceiling_denied");
          }
          ceilings = workers.ceilings;
          yield* a[C.WS_METHODS.workerAcknowledge]({ ...input, grant: "history", acknowledged: true });
          const otherDenied = yield* b[C.WS_METHODS.workerHistoryRead]({ ...read, ...other }).pipe(Effect.flip);
          assert.equal(otherDenied._tag === "ProviderWorkersError" && otherDenied.code, "acknowledgement_required");
          for (const wrong of [
            { ...read, viewId: one.opened.recoveryHandle },
            { ...read, environmentId: C.EnvironmentId.make("foreign") },
            // A valid different thread sharing this resident native owner still cannot borrow the view.
            { ...read, threadId: foreignThreadId },
          ]) assert.equal((yield* a[C.WS_METHODS.workerHistoryRead](wrong).pipe(Effect.exit))._tag, "Failure");
          const stolen = yield* b[C.WS_METHODS.workerHistoryRead](read).pipe(Effect.flip);
          assert.equal(stolen._tag === "ProviderWorkersError" && stolen.code, "foreign_view");
          stage = "unauthorized socket";
          const unauthorized = yield* auth.issueSession({ scopes: [] }).pipe(Effect.flatMap((issued) => sessions.verify(issued.token)), Effect.flatMap(connect));
          assert.equal((yield* unauthorized[C.WS_METHODS.workerHistoryRead](read).pipe(Effect.exit))._tag, "Failure");
          assert.equal((yield* unauthorized[C.WS_METHODS.workerRouteRead]({ ...input, id: worker.id }).pipe(Effect.exit))._tag, "Failure");
          assert.deepEqual(nativeReads, { history: 0, route: 0 });
          stage = "raw history";
          const bytes: Uint8Array[] = [];
          let cursor: string | undefined;
          do {
            const page = yield* a[C.WS_METHODS.workerHistoryRead]({ ...read, limit: 1, ...(cursor ? { cursor } : {}) });
            for (const row of page.items) bytes.push(Buffer.from(row.data, "base64"));
            cursor = page.cursor ?? undefined;
          } while (cursor);
          const reconstructed = Buffer.concat(bytes);
          assert.include(reconstructed.toString("utf8"), "native result 😀");
          assert.isAbove(reconstructed.length, 0);
          const nativeFile = NodeFS.readdirSync(root, { recursive: true })
            .filter((file): file is string => typeof file === "string" && file.endsWith(".jsonl"))
            .map((file) => NodePath.join(root, file))
            .find((file) => NodeFS.readFileSync(file).equals(reconstructed));
          assert.isDefined(nativeFile);
          const firstPage = yield* a[C.WS_METHODS.workerHistoryRead]({ ...read, limit: 1 });
          assert.isNotNull(firstPage.cursor);
          const additions = Buffer.concat([
            Buffer.from(JSON.stringify({ text: "😀".repeat(4000) }) + "\n"),
            Buffer.from([0xff, 0x0a]),
          ]);
          const modified = Buffer.from(reconstructed);
          modified[0] = 0xff;
          NodeFS.writeFileSync(nativeFile!, Buffer.concat([modified, additions]));
          stage = "stale history";
          const stale = yield* a[C.WS_METHODS.workerHistoryRead]({ ...read, cursor: firstPage.cursor!, limit: 1 });
          assert.deepEqual(stale.items, []);
          assert.isTrue(stale.diagnostics.some((item) => item.code === "stale_cursor"));
          let next: string | undefined;
          const fragments: Buffer[] = [];
          let offset = 0;
          let corrupt = false;
          let splitEntry = false;
          let corruptDiagnostic = false;
          do {
            const page = yield* a[C.WS_METHODS.workerHistoryRead]({ ...read, limit: 1, ...(next ? { cursor: next } : {}) });
            corruptDiagnostic ||= page.diagnostics.some((item) => item.code === "corrupt_entry");
            for (const row of page.items) {
              assert.equal(row.byteOffset, offset);
              assert.isAtMost(row.entryOffset, row.byteOffset);
              const fragment = Buffer.from(row.data, "base64");
              fragments.push(fragment);
              offset += fragment.length;
              if (row.validation === "corrupt") corrupt = true;
              if (!row.endsEntry) splitEntry = true;
            }
            next = page.cursor ?? undefined;
          } while (next);
          assert.deepEqual(Buffer.concat(fragments), Buffer.concat([modified, additions]));
          assert.isTrue(corrupt);
          assert.isTrue(splitEntry);
          assert.isTrue(corruptDiagnostic);
          stage = "routing";
          const routeInput = { ...input, id: worker.id };
          assert.equal((yield* a[C.WS_METHODS.workerRouteRead](routeInput).pipe(Effect.flip))._tag, "ProviderWorkersError");
          yield* a[C.WS_METHODS.workerAcknowledge]({ ...input, grant: "route", acknowledged: true });
          const routing = yield* a[C.WS_METHODS.workerRouteRead](routeInput);
          assert.equal(routing.state, "captured");
          assert.isAtMost(routing.page.text.length, 1024);
          let routeOffset = routing.page.nextOffset;
          while (routeOffset !== null) {
            const continuation = yield* a[C.WS_METHODS.workerRouteRead]({ ...routeInput, offset: routeOffset });
            if (continuation.state !== "captured") throw new Error("Expected retained captured route");
            assert.equal(continuation.page.offset, routeOffset);
            routeOffset = continuation.page.nextOffset;
          }
          assert.equal((yield* b[C.WS_METHODS.workerRouteRead]({ ...routeInput, ...other }).pipe(Effect.flip))._tag, "ProviderWorkersError");
          const three = yield* start(a);
          const fourth = yield* a[C.WS_METHODS.workerOpenView](target).pipe(Stream.runHead, Effect.flip);
          assert.equal(fourth._tag === "ProviderWorkersError" && fourth.code, "busy");
          assert.deepEqual(yield* a[C.WS_METHODS.workerList](target), roster);
          yield* Fiber.interrupt(three.fiber);
          yield* a[C.WS_METHODS.workerCloseView](input);
          yield* Fiber.join(one.fiber);
          assert.equal((yield* a[C.WS_METHODS.workerHistoryRead](read).pipe(Effect.flip))._tag, "ProviderWorkersError");
          const fresh = yield* start(a);
          assert.deepEqual(fresh.opened.grants, []);
          assert.equal((yield* a[C.WS_METHODS.workerHistoryRead]({ ...read, viewId: fresh.opened.viewId }).pipe(Effect.flip))._tag, "ProviderWorkersError");
          const owner = fresh.opened.owner.coordinatorSessionId;
          stage = "owner invalidation";
          yield* workers.invalidate;
          yield* Fiber.await(two.fiber);
          yield* Fiber.await(fresh.fiber);
          assert.equal((yield* a[C.WS_METHODS.workerHistoryRead]({ ...read, viewId: fresh.opened.viewId }).pipe(Effect.flip))._tag, "ProviderWorkersError");
          yield* workers.bind(owner);
          stage = "external slots";
          yield* Effect.gen(function* () {
            for (let index = 0; index < 3; index++) {
              yield* Effect.acquireRelease(
                Effect.tryPromise({
                  try: () => connectNative(NodePath.join(workers.directory!, "descriptor.json"), { expectedSessionId: owner }),
                  catch: () => new C.ProviderWorkersError({ code: "fixture_connect_failed" }),
                }),
                (client) => Effect.promise(() => client.disconnect()),
              );
            }
            const blocked = yield* a[C.WS_METHODS.workerOpenView](target).pipe(Stream.runHead, Effect.flip);
            // The current authoritative broker closes excess sockets before connect;
            // it does not send busy. Preserve that error rather than guessing its cause.
            assert.equal(blocked._tag === "ProviderWorkersError" && blocked.code, "disconnected");
            assert.deepEqual(yield* a[C.WS_METHODS.workerList](target), roster);
          }).pipe(Effect.scoped);
          stage = "lost transport and reconnect";
          yield* Effect.scoped(Effect.gen(function* () {
            const transport = yield* connect();
            const view = yield* start(transport);
            yield* transport[C.WS_METHODS.workerAcknowledge]({
              ...target, viewId: view.opened.viewId, grant: "history", acknowledged: true,
            });
          }));
          const reconnected = yield* connect();
          const reconnectedView = yield* start(reconnected);
          assert.deepEqual(reconnectedView.opened.grants, []);
          assert.equal((yield* reconnected[C.WS_METHODS.workerHistoryRead]({
            ...read, viewId: reconnectedView.opened.viewId,
          }).pipe(Effect.flip))._tag, "ProviderWorkersError");
          yield* Fiber.interrupt(reconnectedView.fiber);
          const revoked = yield* start(a);
          stage = "auth revocation";
          yield* sessions.revoke(verified.sessionId);
          yield* Fiber.await(revoked.fiber);
          // Direct service reads cannot use a completed transport's view, even with its old session identity.
          const viewer = { session: { ...verified, method: verified.method }, connectionId: "not-the-opening-socket" };
          assert.equal((yield* service.historyRead(viewer, read).pipe(Effect.flip)).code, "foreign_view");
          assert.isFalse(yield* runtime.hasPendingBackgroundWork!);
          // Only fixture-owned files are inspected; no coordinator or global transcript is readable.
          assert.isTrue(NodeFS.existsSync(root));
          assert.isTrue(NodePath.isAbsolute(root));
          stage = "complete";
        }).pipe(
          Effect.onInterrupt(() => Effect.logWarning(`Worker native stage: ${stage}`)),
          Effect.provide(Layer.mergeAll(
            workerLayer, RpcSerialization.layerJson, NodeHttpServer.layerTest,
          ).pipe(Layer.provideMerge(authLayer), Layer.provideMerge(NodeServices.layer))),
          Effect.scoped,
        );
      }).pipe(Effect.orDie),
    ),
  );
});
