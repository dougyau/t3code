import { assert, describe, it } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as C from "@t3tools/contracts";
import type { PiWorkersPort } from "@t3tools/provider-pi/server";
import { WorkerInspectionError } from "../../../../packages/provider-pi/src/server/workerBridge.ts";
import { workerWireFixture } from "../../../../packages/provider-pi/src/server/workerBridge.wire.fixture.ts";
import { BridgeError, type MutationStatus } from "../../../../packages/provider-pi/src/server/pitako/bridge/client.js";
import type { ErrorCode } from "../../../../packages/provider-pi/src/server/pitako/bridge/protocol.js";
import type { ProviderAdapterV2SessionRuntime } from "@t3tools/provider-core/server/ProviderAdapter";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as RpcTest from "effect/rpc/RpcTest";
import * as SessionStore from "../auth/SessionStore.ts";
import * as RpcAuthorization from "../auth/RpcAuthorization.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ProviderSessionManager from "../orchestration-v2/ProviderSessionManager.ts";
import * as Workers from "./ProviderWorkers.ts";
import * as WorkerRpc from "./rpc.ts";

const workerError = (error: C.ProviderWorkersError | C.EnvironmentAuthorizationError) => {
  if (error._tag !== "ProviderWorkersError") throw new Error(`Expected worker error: ${error._tag}`);
  return error;
};
const id = (view: { viewId: string }, ordinal: number, nonce = "00000000-0000-0000-0000-000000000001") =>
  C.makeWorkerActionId(view.viewId, ordinal, nonce);
const fixtureWithWire = (wire?: Effect.Success<ReturnType<typeof workerWireFixture>>) => Effect.gen(function* () {
  const environmentId = C.EnvironmentId.make("control-environment");
  const threadId = C.ThreadId.make("control-thread");
  const providerSessionId = C.ProviderSessionId.make("control-session");
  const providerInstanceId = C.ProviderInstanceId.make("pi");
  const now = yield* DateTime.now;
  const providerThread: C.OrchestrationV2ProviderThread = {
    id: C.ProviderThreadId.make("control-provider-thread"),
    driver: C.ProviderDriverKind.make("pi"), providerInstanceId, providerSessionId, appThreadId: threadId,
    ownerNodeId: null, nativeThreadRef: null, nativeConversationHeadRef: null,
    status: "idle", firstRunOrdinal: null, lastRunOrdinal: null, handoffIds: [],
    forkedFrom: null, createdAt: now, updatedAt: now,
  };
  const thread: C.OrchestrationV2AppThread = {
    id: threadId, projectId: C.ProjectId.make("project"), title: "control fixture",
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
  const owner = { processId: "process", coordinatorSessionId: "session", epoch: "epoch" };
  const selection: typeof C.WorkerSelection.Type = {
    ...owner, target: { kind: "background", instanceId: "worker" },
    instanceId: "worker", generation: 1, historyId: "history", sessionId: "child-session",
  };
  const state = {
    mode: "full-access" as PiWorkersPort["runtimeMode"], activeAuth: true,
    ceiling: true, current: selection as typeof C.WorkerSelection.Type | undefined,
    terminal: false, retired: false, disconnectFailure: false, leaseExpired: false,
    hold: false, nativeBusy: false, statusBusy: false, detailBusy: false, evicted: false,
    actionError: undefined as string | undefined,
    statusError: undefined as string | undefined,
    recordedError: undefined as ErrorCode | undefined,
    pageError: undefined as "invalid_page" | "oversized" | undefined,
    resolve: undefined as (() => void) | undefined,
    duringDetail: undefined as (() => void) | undefined,
    duringDisconnect: undefined as (() => void) | undefined,
    highWater: 7,
  };
  const calls = { effects: [] as number[], status: [] as number[], grant: 0,
    details: 0, leaseReads: 0, disconnect: 0, opens: 0, resumes: 0 };
  const admissions = yield* Queue.unbounded<number>();
  const completions = yield* Queue.unbounded<number>();
  const receipts = new Map<number, MutationStatus>();
  const client: Effect.Success<ReturnType<PiWorkersPort["openView"]>> = {
    owner, capabilities: [],
    get lease() { calls.leaseReads++; return wire?.client.lease ??
      { clientId: "lease", resumeToken: "private", highWater: state.highWater }; },
    detail: async () => {
      calls.details++;
      if (state.detailBusy) throw new BridgeError("busy", "fixture coded detail busy");
      state.duringDetail?.();
      return {
        state: "ready", ref: { kind: "read", ...owner, instanceId: "worker", source: "agent_spawn" },
        status: state.terminal ? "completed" : "running", observedStatus: "running",
        result: "pending", activity: "observed_binding",
        metadata: { text: "{}", offset: 0, nextOffset: null },
        workbrief: { text: "brief", offset: 0, nextOffset: null, source: "invocation task" },
        nextBrief: null, nextMetadata: null,
        controls: { ...(state.current ? { selection: state.current } : {}), grant: "denied" },
      };
    },
    grant: async (grant) => { calls.grant++; return { grant, acknowledged: true }; },
    input: async (_selection, input, sequence) => {
      calls.effects.push(sequence);
      if (wire) return wire.client.input(_selection, input, sequence);
      if (state.actionError === "uncoded") throw new Error("fixture native transport failure");
      if (state.actionError === "plain_busy") throw { code: "busy" };
      if (state.actionError) throw new BridgeError(state.actionError, "fixture native action error");
      receipts.set(sequence, { state: "pending", sequence });
      Queue.offerUnsafe(admissions, sequence);
      if (state.hold) await new Promise<void>((resolve) => { state.resolve = resolve; });
      const status: MutationStatus = {
        state: "recorded", sequence, receipt: state.recordedError || state.nativeBusy
          ? { kind: "error", code: state.recordedError ?? "busy" } : {
          kind: "input", native: {
            interactionId: `interaction-${sequence}`, historyId: selection.historyId,
            sessionId: selection.sessionId, intent: input.intent, status: "queued",
          },
        },
      };
      receipts.set(sequence, status);
      Queue.offerUnsafe(completions, sequence);
      if (state.pageError) return { state: "recorded", sequence, receipt: {
        kind: "input", native: {
          interactionId: state.pageError === "invalid_page" ? "" : "x".repeat(48 * 1024),
          historyId: selection.historyId, sessionId: selection.sessionId, intent: "query", status: "queued",
        },
      } };
      return status;
    },
    cancel: async (_selection, sequence) => {
      calls.effects.push(sequence);
      if (wire) return wire.client.cancel(_selection, sequence);
      if (state.actionError === "uncoded") throw new Error("fixture native transport failure");
      if (state.actionError === "plain_busy") throw { code: "busy" };
      if (state.actionError) throw new BridgeError(state.actionError, "fixture native action error");
      const status: MutationStatus = { state: "recorded", sequence, receipt: {
        kind: "cancel", native: {
          status: "requested", instanceId: selection.instanceId,
          historyId: selection.historyId, sessionId: selection.sessionId,
        },
      } };
      receipts.set(sequence, status);
      return status;
    },
    requestStatus: async (sequence) => {
      calls.status.push(sequence);
      if (wire) return wire.client.requestStatus(sequence);
      if (state.statusError) throw new BridgeError(state.statusError, "fixture status rejection");
      if (state.statusBusy) throw new BridgeError("busy", "fixture coded status busy");
      if (state.pageError) return { state: "recorded", sequence, receipt: {
        kind: "input", native: {
          interactionId: state.pageError === "invalid_page" ? "" : "x".repeat(48 * 1024),
          historyId: selection.historyId, sessionId: selection.sessionId, intent: "query", status: "queued",
        },
      } };
      if (state.disconnectFailure) { state.disconnectFailure = false; throw { code: "disconnected" }; }
      if (state.evicted) return { state: "outcome_unknown", sequence };
      return receipts.get(sequence) ?? { state: "not_admitted", sequence };
    },
    disconnect: async () => { calls.disconnect++; state.duringDisconnect?.(); await wire?.client.disconnect(); },
    list: async () => ({ owner, items: [], revision: 1, cursor: null }),
    subscribe: () => { throw new Error("No private subscription"); },
    requests: () => { throw new Error("No requests"); },
    activity: () => { throw new Error("No activity"); },
    result: () => { throw new Error("No result"); },
    historyList: () => { throw new Error("No history"); },
    historyRead: async () => ({ items: [], diagnostics: [], cursor: null }),
    routeRead: async () => ({ state: "captured", page: { text: "private", offset: 0, nextOffset: null } }),
  };
  const port: PiWorkersPort = {
    driver: "pi", get runtimeMode() { return state.mode; }, pending: true,
    get ceilings() { return { history: true, route: true, control: state.ceiling }; },
    get observation(): PiWorkersPort["observation"] { return state.retired ? { state: "stale_owner" } :
      { state: "last_observation", owner, observedAt: 1, revision: 1 }; },
    observations: Stream.never, refresh: Effect.void, read: (use) => Effect.promise(() => use(client)),
    disconnectView: (client) => Effect.promise(() => client.disconnect()),
    openView: (resume) => Effect.gen(function* () {
      calls.opens++;
      if (resume) {
        calls.resumes++;
        assert.equal(resume.clientId, wire?.client.lease.clientId ?? "lease");
        assert.isAbove(calls.disconnect, 0);
        if (wire) return yield* Effect.acquireRelease(
          Effect.tryPromise({ try: () => wire.open(resume), catch: (cause) =>
            new WorkerInspectionError({ code: cause instanceof BridgeError ? cause.code : "inspection_failed" }) }),
          (client) => Effect.promise(() => client.disconnect()),
        );
        if (state.leaseExpired) return yield* Effect.fail(new WorkerInspectionError({ code: "lease_expired" }));
      }
      return client;
    }),
  };
  // Only the port is reachable. Starting/resuming provider work is not a fixture operation.
  const runtime: ProviderAdapterV2SessionRuntime = {
    instanceId: providerInstanceId, driver: C.ProviderDriverKind.make("pi"), providerSessionId,
    workers: port, get providerSession(): C.OrchestrationV2ProviderSession { throw new Error("No residence touch"); },
    events: Stream.empty,
    ensureThread: () => { throw new Error("No start"); }, resumeThread: () => { throw new Error("No resume"); },
    startTurn: () => { throw new Error("No prompt"); }, steerTurn: () => { throw new Error("No steer"); },
    interruptTurn: () => { throw new Error("No interrupt"); }, respondToRuntimeRequest: () => { throw new Error("No approval"); },
    readThreadSnapshot: () => { throw new Error("No snapshot"); }, rollbackThread: () => { throw new Error("No rollback"); },
    forkThread: () => { throw new Error("No fork"); },
  };
  const dependencies = Layer.mergeAll(
    Layer.mock(SessionStore.SessionStore)({ cookieName: "fixture", legacyCookieName: undefined,
      isActive: () => Effect.sync(() => state.activeAuth) }),
    Layer.mock(ServerEnvironment.ServerEnvironment)({ getEnvironmentId: Effect.succeed(environmentId) }),
    Layer.mock(ProjectionStore.ProjectionStoreV2)({
      getThreadShell: () => Effect.succeed(shell),
      getThreadProviderContext: () => Effect.succeed({ thread, providerThreads: [providerThread], providerSessions: [] }),
    }),
    Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({ getResident: () => Effect.succeedSome(runtime) }),
  );
  const service = yield* Workers.make.pipe(Effect.provide(dependencies));
  const session = { sessionId: C.AuthSessionId.make("auth"), subject: "fixture",
    method: "bearer-access-token" as const,
    scopes: [C.AuthOrchestrationReadScope, C.AuthOrchestrationOperateScope, C.AuthDiagnosticsReadScope] };
  const rpc = (scopes = session.scopes, sessionId = session.sessionId, workers = service) =>
    RpcTest.makeClient(C.ProviderWorkersRpcGroup).pipe(Effect.provide(Layer.mergeAll(
      C.ProviderWorkersRpcGroup.toLayer(WorkerRpc.makeHandlers({ ...session, sessionId, scopes })).pipe(
        Layer.provide(Layer.succeed(Workers.ProviderWorkers, workers))),
      RpcAuthorization.layer(scopes),
    )));
  const a = yield* rpc();
  const b = yield* rpc();
  const target = { environmentId, threadId };
  const start = (client = a) => Effect.gen(function* () {
    const opened = yield* Queue.unbounded<typeof C.WorkerViewEvent.Type>();
    const fiber = yield* client[C.WS_METHODS.workerOpenView](target).pipe(
      Stream.runForEach((event) => Queue.offer(opened, event)), Effect.forkScoped);
    const event = yield* Queue.take(opened);
    if (event.type !== "opened") throw new Error("Expected opened");
    return { ...target, viewId: event.viewId, recoveryHandle: event.recoveryHandle, fiber, events: opened };
  });
  const activate = (view: typeof C.WorkerViewInput.Type) => Effect.gen(function* () {
    const detail = yield* a[C.WS_METHODS.workerControlDetail]({ ...view, id: "worker" });
    assert.deepEqual(detail.controls.selection, state.current);
    yield* a[C.WS_METHODS.workerActivate]({ ...view, selection, acknowledged: true });
  });
  return { state, port, calls, receipts, admissions, completions, rpc, a, b, target, selection, start, activate, service, dependencies, session };
});
const fixture = fixtureWithWire();

describe("worker controls and receipt authority", () => {
  it.effect.each(["界".repeat(6000), "   "])(
    "preserves T3-admitted input rejected locally by the actual passive native client", (text) =>
      Effect.gen(function* () {
        const wire = yield* workerWireFixture({ processId: "process", coordinatorSessionId: "session", epoch: "epoch" });
        const f = yield* fixtureWithWire(wire);
        const view = yield* f.start();
        yield* f.activate(view);
        yield* f.a[C.WS_METHODS.workerAcknowledge]({ ...view, grant: "history", acknowledged: true });
        yield* Queue.take(view.events);
        const action: typeof C.WorkerActionInput.Type = {
          ...view, actionId: id(view, 1), selection: f.selection,
          operation: { kind: "input", input: { intent: "query", text } },
        };
        const error = workerError(yield* f.a[C.WS_METHODS.workerAction](action).pipe(Effect.flip));
        assert.equal(error.code, "invalid_request");
        assert.equal(error.actionAdmitted, true);
        assert.deepEqual(wire.calls, ["connect"]); // No native mutation write or receipt admission.
        const lookup = { ...view, actionId: action.actionId, access: { recoveryHandle: view.recoveryHandle } };
        assert.deepEqual((yield* f.a[C.WS_METHODS.workerReceipt](lookup)).status, { state: "not_admitted", sequence: 8 });
        yield* f.a[C.WS_METHODS.workerAction](action);
        assert.deepEqual(wire.calls, ["connect", "request.status", "request.status"]);
        assert.deepEqual(f.calls.effects, [8]);
        yield* f.a[C.WS_METHODS.workerHistoryRead]({ ...view, historyId: "history" });
        yield* f.a[C.WS_METHODS.workerAction]({ ...action, actionId: id(view, 2),
          operation: { kind: "cancel" } });
        assert.deepEqual(f.calls.effects, [8, 9]);
        assert.equal(wire.sockets.size, 1);
        assert.equal(f.calls.disconnect, 0);
        assert.equal(f.calls.resumes, 0);
        assert.equal(yield* Queue.size(view.events), 0);
      }).pipe(Effect.scoped, Effect.provide(NodeCrypto.layer)));

  it.effect.each((["stale_selection", "stale_reference", "unknown_target"] as const).flatMap((code) =>
    [{ code, channel: "recorded" }, { code, channel: "rejected" }]))(
    "invalidates only the matching control identity for $channel $code", ({ code, channel }) =>
      Effect.gen(function* () {
        const f = yield* fixture;
        const view = yield* f.start();
        yield* f.activate(view);
        for (const grant of ["history", "route"] as const) {
          yield* f.a[C.WS_METHODS.workerAcknowledge]({ ...view, grant, acknowledged: true });
          yield* Queue.take(view.events);
        }
        const action: typeof C.WorkerActionInput.Type = { ...view, actionId: id(view, 1),
          selection: f.selection, operation: { kind: "input", input: { intent: "query", text: "once" } } };
        f.state.recordedError = channel === "recorded" ? code : undefined;
        f.state.actionError = channel === "rejected" ? code : undefined;
        let outcome: typeof C.WorkerActionOutcome.Type;
        if (channel === "rejected") {
          const error = workerError(yield* f.a[C.WS_METHODS.workerAction](action).pipe(Effect.flip));
          assert.equal(error.code, code);
          assert.equal(error.actionAdmitted, true);
          const status: MutationStatus = { state: "recorded", sequence: 8, receipt: { kind: "error", code } };
          outcome = { actionId: action.actionId, status };
          f.receipts.set(8, status);
          f.state.actionError = undefined;
        } else outcome = yield* f.a[C.WS_METHODS.workerAction](action);
        assert.deepEqual(outcome.status, { state: "recorded", sequence: 8, receipt: { kind: "error", code } });
        assert.deepEqual(yield* Queue.take(view.events), { type: "invalidated", reason: "stale_selection" });
        const lookup = { ...view, actionId: action.actionId, access: { recoveryHandle: view.recoveryHandle } };
        assert.deepEqual(yield* f.a[C.WS_METHODS.workerReceipt](lookup), outcome);
        assert.equal(yield* Queue.size(view.events), 0);
        assert.equal(workerError(yield* f.a[C.WS_METHODS.workerAction]({
          ...action, actionId: id(view, 2),
        }).pipe(Effect.flip)).code, "acknowledgement_required");
        assert.equal(workerError(yield* f.a[C.WS_METHODS.workerActivate]({
          ...view, selection: f.selection, acknowledged: true,
        }).pipe(Effect.flip)).code, "stale_selection"); // Observation was cleared too.
        yield* f.a[C.WS_METHODS.workerHistoryRead]({ ...view, historyId: "history" });
        yield* f.a[C.WS_METHODS.workerRouteRead]({ ...view, id: "worker" });
        const next = { ...f.selection, generation: 2, sessionId: "replacement" };
        f.state.current = next;
        f.state.recordedError = undefined;
        yield* f.a[C.WS_METHODS.workerControlDetail]({ ...view, id: "worker" });
        yield* f.a[C.WS_METHODS.workerActivate]({ ...view, selection: next, acknowledged: true });
        assert.deepEqual(yield* f.a[C.WS_METHODS.workerReceipt](lookup), outcome);
        assert.equal(yield* Queue.size(view.events), 0); // Old receipt cannot clear the new activation.
        yield* f.a[C.WS_METHODS.workerAction]({ ...action, actionId: id(view, 2), selection: next });
        assert.deepEqual(f.calls.effects, [8, 9]);
        assert.equal(f.calls.disconnect, 0);
        assert.equal(f.calls.resumes, 0);
        assert.equal(f.calls.grant, 4);
      }).pipe(Effect.scoped, Effect.provide(NodeCrypto.layer)));

  it.effect("poisons only rejected sequence correlation, never reading a different payload's receipt", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const view = yield* f.start();
      yield* f.activate(view);
      const foreign: MutationStatus = { state: "recorded", sequence: 8, receipt: {
        kind: "cancel", native: { status: "requested", instanceId: "foreign", historyId: "foreign", sessionId: "foreign" },
      } };
      f.receipts.set(8, foreign);
      f.state.actionError = "sequence_conflict";
      const action: typeof C.WorkerActionInput.Type = { ...view, actionId: id(view, 1),
        selection: f.selection, operation: { kind: "cancel" } };
      const error = workerError(yield* f.a[C.WS_METHODS.workerAction](action).pipe(Effect.flip));
      assert.equal(error.code, "sequence_conflict");
      assert.equal(error.actionAdmitted, true);
      f.state.actionError = undefined;
      const lookup = { ...view, actionId: action.actionId, access: { recoveryHandle: view.recoveryHandle } };
      for (const call of [f.a[C.WS_METHODS.workerReceipt](lookup), f.a[C.WS_METHODS.workerAction](action),
        f.a[C.WS_METHODS.workerReceipt](lookup)])
        assert.deepEqual((yield* call).status, { state: "outcome_unknown", reason: "unavailable" });
      assert.deepEqual(f.calls.status, []);
      assert.deepEqual(f.calls.effects, [8]); // Native invocation rejects; never a second effect.
      assert.deepEqual(f.receipts.get(8), foreign);
      // Recorded error receipts are a separate channel, not poisoned correlation.
      f.state.recordedError = "sequence_conflict";
      const recorded = { ...action, actionId: id(view, 2),
        operation: { kind: "input" as const, input: { intent: "query" as const, text: "new decision" } } };
      const outcome = yield* f.a[C.WS_METHODS.workerAction](recorded);
      assert.deepEqual(outcome.status, { state: "recorded", sequence: 9, receipt: { kind: "error", code: "sequence_conflict" } });
      assert.deepEqual(yield* f.a[C.WS_METHODS.workerAction](recorded), outcome);
      assert.deepEqual(f.calls.status, [9]);
      assert.deepEqual(f.calls.effects, [8, 9]);
      assert.equal(f.calls.resumes, 0);
    }).pipe(Effect.scoped, Effect.provide(NodeCrypto.layer)));

  it.effect.each(["lease_expired", "stale_owner", "unauthorized"] as const)(
    "retires %s authority and never resumes it again", (code) =>
      Effect.gen(function* () {
        const f = yield* fixture;
        const view = yield* f.start();
        yield* f.activate(view);
        const action: typeof C.WorkerActionInput.Type = { ...view, actionId: id(view, 1),
          selection: f.selection, operation: { kind: "cancel" } };
        f.state.actionError = code;
        if (code === "unauthorized") {
          const error = workerError(yield* f.a[C.WS_METHODS.workerAction](action).pipe(Effect.flip));
          assert.equal(error.code, code);
          assert.equal(error.actionAdmitted, true);
        } else assert.deepEqual((yield* f.a[C.WS_METHODS.workerAction](action)).status, {
          state: "outcome_unknown", reason: code === "lease_expired" ? code : "owner_retired",
        });
        yield* Fiber.join(view.fiber);
        const lookup = { ...view, actionId: action.actionId, access: { recoveryHandle: view.recoveryHandle } };
        for (let i = 0; i < 2; i++) {
          if (code === "unauthorized")
            assert.equal(workerError(yield* f.a[C.WS_METHODS.workerReceipt](lookup).pipe(Effect.flip)).code, code);
          else assert.deepEqual((yield* f.a[C.WS_METHODS.workerReceipt](lookup)).status, {
            state: "outcome_unknown", reason: code === "lease_expired" ? code : "owner_retired",
          });
        }
        assert.equal(f.calls.disconnect, 1);
        assert.equal(f.calls.resumes, 0);
        assert.deepEqual(f.calls.status, []);
        assert.deepEqual(f.calls.effects, [8]);
      }).pipe(Effect.scoped, Effect.provide(NodeCrypto.layer)));

  it.effect("joins the old socket and an actual expired native resume admission without fresh-lease repair", () =>
    Effect.gen(function* () {
      const wire = yield* workerWireFixture({ processId: "process", coordinatorSessionId: "session", epoch: "epoch" });
      const f = yield* fixtureWithWire(wire);
      const view = yield* f.start();
      yield* f.activate(view);
      const action: typeof C.WorkerActionInput.Type = { ...view, actionId: id(view, 1),
        selection: f.selection, operation: { kind: "cancel" } };
      yield* f.a[C.WS_METHODS.workerAction](action);
      yield* f.a[C.WS_METHODS.workerCloseView](view);
      yield* Fiber.join(view.fiber);
      yield* Effect.promise(() => wire.joinSockets());
      assert.equal(wire.sockets.size, 0);
      wire.policy.resumeError = "lease_expired";
      const lookup = { ...view, actionId: action.actionId, access: { recoveryHandle: view.recoveryHandle } };
      for (let i = 0; i < 2; i++)
        assert.deepEqual((yield* f.a[C.WS_METHODS.workerReceipt](lookup)).status, {
          state: "outcome_unknown", reason: "lease_expired",
        });
      assert.deepEqual(wire.calls, ["connect", "cancel", "connect"]);
      yield* Effect.promise(() => wire.joinSockets());
      assert.equal(wire.sockets.size, 0); // connect() closes and joins the rejected attempt.
      assert.equal(f.calls.resumes, 1);
      assert.deepEqual(f.calls.status, []);
      assert.equal(f.calls.grant, 1);
    }).pipe(Effect.scoped, Effect.provide(NodeCrypto.layer)));

  it.effect.each(["busy", "forbidden", "invalid_request", "unsupported_method",
    "invalid_cursor", "stale_cursor", "oversized"].flatMap((code) =>
      [{ kind: "input" as const, code }, { kind: "cancel" as const, code }]))(
    "preserves admitted $kind $code classification and healthy grants without replaying", ({ kind, code }) =>
      Effect.gen(function* () {
        const f = yield* fixture;
        const view = yield* f.start();
        yield* f.activate(view);
        yield* f.a[C.WS_METHODS.workerAcknowledge]({ ...view, grant: "history", acknowledged: true });
        const granted = yield* Queue.take(view.events);
        assert.equal(granted.type, "opened");
        const action: typeof C.WorkerActionInput.Type = {
          ...view, actionId: id(view, 1), selection: f.selection,
          operation: kind === "input" ? { kind, input: { intent: "query", text: "exact decision" } } : { kind },
        };
        f.state.actionError = code;
        const rejected = workerError(yield* Effect.flip(f.a[C.WS_METHODS.workerAction](action)));
        assert.equal(rejected.code, code);
        assert.deepEqual(JSON.parse(JSON.stringify(rejected)), {
          _tag: "ProviderWorkersError", code, actionAdmitted: true,
        });
        f.state.actionError = undefined;
        const receipt = yield* f.a[C.WS_METHODS.workerReceipt]({
          ...view, actionId: action.actionId, access: { recoveryHandle: view.recoveryHandle },
        });
        assert.deepEqual(receipt.status, { state: "not_admitted", sequence: 8 });
        const repeated = yield* f.a[C.WS_METHODS.workerAction](action);
        assert.deepEqual(repeated, receipt);
        assert.deepEqual(f.calls.effects, [8]);
        assert.deepEqual(f.calls.status, [8, 8]);
        f.state.statusBusy = true;
        const lookupBusy = workerError(yield* Effect.flip(f.a[C.WS_METHODS.workerAction](action)));
        assert.equal(lookupBusy.code, "busy");
        assert.equal(JSON.parse(JSON.stringify(lookupBusy)).actionAdmitted, true);
        f.state.statusBusy = false;
        f.state.statusError = code;
        const lookupError = workerError(yield* Effect.flip(f.a[C.WS_METHODS.workerReceipt]({
          ...view, actionId: action.actionId, access: { recoveryHandle: view.recoveryHandle },
        })));
        assert.equal(lookupError.code, code);
        assert.equal(lookupError.actionAdmitted, true);
        f.state.statusError = undefined;
        yield* f.a[C.WS_METHODS.workerHistoryRead]({ ...view, historyId: "history" });
        assert.equal(f.calls.grant, 2);
        assert.equal(f.calls.disconnect, 0);
        assert.equal(f.calls.resumes, 0);
        assert.equal(yield* Queue.size(view.events), 0);
        // The old admission still supersedes older IDs, but does not hold pending or remove selection.
        const superseded = yield* f.a[C.WS_METHODS.workerAction]({ ...action, actionId: id(view, 1,
          "00000000-0000-0000-0000-000000000002") });
        assert.deepEqual(superseded.status, { state: "outcome_unknown", reason: "mapping_evicted" });
        yield* f.a[C.WS_METHODS.workerAction]({ ...action, actionId: id(view, 2) });
        assert.deepEqual(f.calls.effects, [8, 9]);
      }).pipe(Effect.scoped, Effect.provide(NodeCrypto.layer)));
  it.effect.each(["invalid_page", "oversized"] as const)(
    "settles %s completed replies as unknown without native recovery", (code) =>
      Effect.gen(function* () {
        const f = yield* fixture;
        const view = yield* f.start();
        yield* f.activate(view);
        yield* f.a[C.WS_METHODS.workerAcknowledge]({ ...view, grant: "history", acknowledged: true });
        yield* Queue.take(view.events);
        const action: typeof C.WorkerActionInput.Type = { ...view, actionId: id(view, 1),
          selection: f.selection, operation: { kind: "input", input: { intent: "steer", text: "once" } } };
        f.state.pageError = code;
        const outcome = yield* f.a[C.WS_METHODS.workerAction](action);
        assert.deepEqual(outcome.status, { state: "outcome_unknown", reason: "unavailable" });
        const malformedLookup = yield* f.a[C.WS_METHODS.workerReceipt]({
          ...view, actionId: action.actionId, access: { recoveryHandle: view.recoveryHandle },
        });
        assert.deepEqual(malformedLookup.status, { state: "outcome_unknown", reason: "unavailable" });
        f.state.pageError = undefined;
        const receipt = yield* f.a[C.WS_METHODS.workerReceipt]({
          ...view, actionId: action.actionId, access: { recoveryHandle: view.recoveryHandle },
        });
        assert.equal(receipt.status.state, "recorded");
        assert.deepEqual(yield* f.a[C.WS_METHODS.workerAction](action), receipt);
        assert.deepEqual(f.calls.effects, [8]);
        yield* f.a[C.WS_METHODS.workerHistoryRead]({ ...view, historyId: "history" });
        yield* f.a[C.WS_METHODS.workerAction]({ ...action, actionId: id(view, 2) });
        assert.deepEqual(f.calls.effects, [8, 9]);
        assert.equal(f.calls.disconnect, 0);
        assert.equal(f.calls.resumes, 0);
        assert.equal(f.calls.grant, 2);
        assert.equal(yield* Queue.size(view.events), 0);
      }).pipe(Effect.scoped, Effect.provide(NodeCrypto.layer)));
  it.effect.each(["invalid_reply", "disconnected", "uncoded", "plain_busy"])(
    "keeps %s native action failure uncertain and joins before explicit recovery", (code) =>
      Effect.gen(function* () {
        const f = yield* fixture;
        const view = yield* f.start();
        yield* f.activate(view);
        f.state.actionError = code;
        const action: typeof C.WorkerActionInput.Type = { ...view, actionId: id(view, 1),
          selection: f.selection, operation: { kind: "cancel" } };
        const outcome = yield* f.a[C.WS_METHODS.workerAction](action);
        assert.deepEqual(outcome.status, { state: "outcome_unknown", reason: "disconnected" });
        assert.deepEqual(yield* Queue.take(view.events), { type: "invalidated", reason: "disconnected" });
        const blocked = workerError(yield* Effect.flip(f.a[C.WS_METHODS.workerAction]({
          ...action, actionId: id(view, 2),
        })));
        assert.equal(blocked.code, "disconnected");
        assert.equal(f.calls.resumes, 0);
        const grants = f.calls.grant;
        const recovered = yield* f.a[C.WS_METHODS.workerReceipt]({
          ...view, actionId: action.actionId, access: { recoveryHandle: view.recoveryHandle },
        });
        assert.deepEqual(recovered.status, { state: "not_admitted", sequence: 8 });
        assert.equal(f.calls.disconnect, 1);
        assert.equal(f.calls.resumes, 1);
        assert.equal(f.calls.grant, grants);
        assert.deepEqual(f.calls.effects, [8]);
      }).pipe(Effect.scoped, Effect.provide(NodeCrypto.layer)));
  it.effect("rechecks session revocation after selection reads and before sequence/effect admission", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const view = yield* f.start();
      yield* f.activate(view);
      f.state.duringDetail = () => { f.state.activeAuth = false; };
      const action: typeof C.WorkerActionInput.Type = { ...view, actionId: id(view, 1), selection: f.selection,
        operation: { kind: "cancel" } };
      assert.equal((yield* f.a[C.WS_METHODS.workerAction](action).pipe(Effect.flip, Effect.map(workerError))).code, "forbidden");
      assert.deepEqual(f.calls.effects, []);
      assert.equal(f.calls.leaseReads, 0);
      f.state.duringDetail = undefined;
      f.state.activeAuth = true;
      const outcome = yield* f.a[C.WS_METHODS.workerAction](action);
      assert.equal(outcome.status.state, "recorded");
      assert.deepEqual(f.calls.effects, [8]);
    }).pipe(Effect.scoped, Effect.provide(NodeCrypto.layer)),
  );
  it.effect("binds current private selection, scopes, ceiling and actual launch policy without changing cancel semantics", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const view = yield* f.start();
      const action: typeof C.WorkerActionInput.Type = { ...view, actionId: id(view, 1), selection: f.selection,
        operation: { kind: "input", input: { intent: "query", text: "/not-a-command" } } };
      const readOnly = yield* f.rpc([C.AuthOrchestrationReadScope]);
      for (const call of [
        readOnly[C.WS_METHODS.workerControlDetail]({ ...view, id: "worker" }),
        readOnly[C.WS_METHODS.workerActivate]({ ...view, selection: f.selection, acknowledged: true }),
        readOnly[C.WS_METHODS.workerAction](action),
        readOnly[C.WS_METHODS.workerReceipt]({ ...f.target, actionId: action.actionId, access: { recoveryHandle: view.recoveryHandle } }),
      ]) assert.equal((yield* call.pipe(Effect.flip))._tag, "EnvironmentAuthorizationError");
      assert.equal((yield* f.b[C.WS_METHODS.workerControlDetail]({ ...view, id: "worker" }).pipe(Effect.flip, Effect.map(workerError))).code, "foreign_view");
      assert.equal((yield* f.a[C.WS_METHODS.workerAction](action).pipe(Effect.flip, Effect.map(workerError))).code, "acknowledgement_required");
      assert.equal((yield* f.a[C.WS_METHODS.workerActivate]({ ...view, selection: f.selection, acknowledged: true }).pipe(Effect.flip, Effect.map(workerError))).code, "stale_selection");
      yield* f.a[C.WS_METHODS.workerControlDetail]({ ...view, id: "worker" });
      f.state.ceiling = false;
      assert.equal((yield* f.a[C.WS_METHODS.workerActivate]({ ...view, selection: f.selection, acknowledged: true }).pipe(Effect.flip, Effect.map(workerError))).code, "ceiling_denied");
      f.state.ceiling = true;
      yield* f.activate(view);
      f.state.mode = "approval-required";
      assert.equal((yield* f.a[C.WS_METHODS.workerAction](action).pipe(Effect.flip, Effect.map(workerError))).code, "full_access_required");
      assert.equal((yield* f.a[C.WS_METHODS.workerAction]({ ...action, operation: {
        kind: "input", input: { intent: "steer", text: "note" } },
      }).pipe(Effect.flip, Effect.map(workerError))).code, "full_access_required");
      assert.deepEqual(f.calls.effects, []);
      f.state.current = { ...f.selection, generation: 2, sessionId: "replacement" };
      assert.equal((yield* f.a[C.WS_METHODS.workerAction]({ ...action, operation: { kind: "cancel" } }).pipe(Effect.flip, Effect.map(workerError))).code, "stale_selection");
      f.state.current = undefined; // Synchronous/non-controllable detail has no selection.
      assert.equal((yield* f.a[C.WS_METHODS.workerAction]({ ...action, operation: { kind: "cancel" } }).pipe(Effect.flip, Effect.map(workerError))).code, "stale_selection");
      f.state.current = f.selection;
      f.state.terminal = true;
      assert.equal((yield* f.a[C.WS_METHODS.workerAction]({ ...action, operation: { kind: "cancel" } }).pipe(Effect.flip, Effect.map(workerError))).code, "stale_selection");
      f.state.terminal = false;
      const cancel = yield* f.a[C.WS_METHODS.workerAction]({ ...action, actionId: id(view, 2), operation: { kind: "cancel" } });
      assert.equal(cancel.status.state, "recorded");
      if (cancel.status.state !== "recorded") throw new Error("Expected receipt");
      assert.equal(cancel.status.receipt.kind, "cancel");
      assert.deepEqual(f.calls.effects, [8]);
      assert.isTrue(f.port.pending);
      f.state.activeAuth = false;
      assert.equal((yield* f.a[C.WS_METHODS.workerReceipt]({ ...f.target, actionId: id(view, 2), access: { viewId: view.viewId } }).pipe(Effect.flip, Effect.map(workerError))).code, "forbidden");
    }).pipe(Effect.scoped, Effect.provide(NodeCrypto.layer)),
  );
  it.effect("registers one sequence before effect, rejects local busy without consuming an ID, and owns work after callers stop", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const view = yield* f.start();
      yield* f.activate(view);
      const action: typeof C.WorkerActionInput.Type = { ...view, actionId: id(view, 1), selection: f.selection,
        operation: { kind: "input", input: { intent: "steer", text: "note", decisionId: "decision" } } };
      f.state.hold = true;
      const first = yield* f.a[C.WS_METHODS.workerAction](action).pipe(Effect.forkScoped);
      assert.equal(yield* Queue.take(f.admissions), 8);
      const before = { ...f.calls };
      assert.equal((yield* f.a[C.WS_METHODS.workerAction]({ ...action, actionId: id(view, 2) }).pipe(Effect.flip, Effect.map(workerError))).code, "busy");
      assert.equal((yield* f.a[C.WS_METHODS.workerAction]({ ...action, actionId: id(view, 99) }).pipe(Effect.flip, Effect.map(workerError))).code, "busy");
      assert.equal(f.calls.details, before.details);
      assert.equal(f.calls.leaseReads, before.leaseReads);
      assert.deepEqual(f.calls.effects, [8]);
      assert.deepEqual((yield* f.a[C.WS_METHODS.workerAction](action)).status, { state: "pending", sequence: 8 });
      assert.equal((yield* f.a[C.WS_METHODS.workerAction]({ ...action, operation: { kind: "cancel" } }).pipe(Effect.flip, Effect.map(workerError))).code, "action_conflict");
      yield* Fiber.interrupt(first);
      assert.deepEqual((yield* f.a[C.WS_METHODS.workerReceipt]({ ...f.target, actionId: action.actionId, access: { viewId: view.viewId } })).status, { state: "pending", sequence: 8 });
      f.state.hold = false;
      f.state.resolve!();
      assert.equal(yield* Queue.take(f.completions), 8);
      // The same native milestone, not a timeout, joins the detached service-owned effect.
      const done = yield* f.a[C.WS_METHODS.workerReceipt]({ ...f.target, actionId: action.actionId, access: { viewId: view.viewId } });
      assert.equal(done.status.state, "recorded");
      const uninvoked = yield* f.a[C.WS_METHODS.workerAction]({ ...action, actionId: id(view, 2) });
      assert.equal(uninvoked.status.state, "recorded");
      if (uninvoked.status.state !== "recorded") throw new Error("Expected independent B receipt");
      assert.equal(uninvoked.status.receipt.kind, "input");
      assert.deepEqual(f.calls.effects, [8, 9]); // Busy ordinal 99 did not supersede B.
      f.state.nativeBusy = true;
      const busy = yield* f.a[C.WS_METHODS.workerAction]({ ...action, actionId: id(view, 3) });
      assert.equal(busy.status.state, "recorded");
      if (busy.status.state !== "recorded") throw new Error("Expected busy receipt");
      assert.deepEqual(busy.status.receipt, { kind: "error", code: "busy" });
      f.state.nativeBusy = false;
      assert.deepEqual(yield* f.a[C.WS_METHODS.workerAction]({ ...action, actionId: id(view, 3) }), busy);
      assert.deepEqual(f.calls.effects, [8, 9, 10]);
      assert.equal(f.calls.opens, 1);
      assert.equal(f.calls.disconnect, 0);
      assert.equal(f.calls.grant, 1);
    }).pipe(Effect.scoped, Effect.provide(NodeCrypto.layer)),
  );
  it.effect("looks up alive clients without regranting, resumes only old leases after joined disconnect, and reports lost memory truthfully", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const view = yield* f.start();
      yield* f.activate(view);
      const action: typeof C.WorkerActionInput.Type = { ...view, actionId: id(view, 1), selection: f.selection,
        operation: { kind: "input", input: { intent: "query", text: "question" } } };
      const outcome = yield* f.a[C.WS_METHODS.workerAction](action);
      const lookup = { ...f.target, actionId: action.actionId, access: { recoveryHandle: view.recoveryHandle } };
      assert.deepEqual(yield* f.b[C.WS_METHODS.workerReceipt](lookup), outcome);
      assert.equal(f.calls.opens, 1);
      assert.equal(f.calls.disconnect, 0);
      assert.equal(f.calls.grant, 1);
      f.state.detailBusy = true;
      assert.equal((yield* f.a[C.WS_METHODS.workerControlDetail]({ ...view, id: "worker" }).pipe(
        Effect.flip, Effect.map(workerError),
      )).code, "busy");
      f.state.detailBusy = false;
      assert.deepEqual(yield* f.b[C.WS_METHODS.workerReceipt](lookup), outcome);
      assert.equal(f.calls.opens, 1);
      assert.equal(f.calls.disconnect, 0);
      assert.equal(f.calls.grant, 1);
      f.state.statusBusy = true;
      assert.equal((yield* f.b[C.WS_METHODS.workerReceipt](lookup).pipe(Effect.flip, Effect.map(workerError))).code, "busy");
      f.state.statusBusy = false;
      assert.deepEqual(yield* f.b[C.WS_METHODS.workerReceipt](lookup), outcome);
      assert.equal(f.calls.opens, 1);
      assert.equal(f.calls.disconnect, 0);
      assert.equal(f.calls.grant, 1);
      const foreign = yield* f.rpc(undefined, C.AuthSessionId.make("foreign"));
      assert.equal((yield* foreign[C.WS_METHODS.workerReceipt](lookup).pipe(Effect.flip, Effect.map(workerError))).code, "foreign_view");
      assert.equal((yield* f.b[C.WS_METHODS.workerAction](action).pipe(Effect.flip, Effect.map(workerError))).code, "foreign_view");
      f.state.disconnectFailure = true;
      assert.deepEqual((yield* f.a[C.WS_METHODS.workerReceipt](lookup)).status, { state: "outcome_unknown", reason: "disconnected" });
      assert.equal((yield* f.a[C.WS_METHODS.workerAction]({
        ...action, operation: { kind: "cancel" },
      }).pipe(Effect.flip, Effect.map(workerError))).code, "action_conflict");
      assert.equal((yield* f.a[C.WS_METHODS.workerAction]({
        ...action, actionId: id(view, 2),
      }).pipe(Effect.flip, Effect.map(workerError))).code, "disconnected");
      assert.deepEqual(yield* f.a[C.WS_METHODS.workerAction](action), outcome);
      assert.equal(f.calls.resumes, 1);
      assert.deepEqual(yield* f.b[C.WS_METHODS.workerReceipt](lookup), outcome);
      assert.equal(f.calls.resumes, 2);
      assert.equal(f.calls.grant, 1);
      assert.deepEqual(f.calls.effects, [8]);
      f.state.evicted = true;
      assert.deepEqual((yield* f.b[C.WS_METHODS.workerReceipt](lookup)).status, { state: "outcome_unknown", sequence: 8 });
      f.state.retired = true;
      assert.deepEqual((yield* f.b[C.WS_METHODS.workerReceipt](lookup)).status, { state: "outcome_unknown", reason: "owner_retired" });
      f.state.retired = false;
      const restarted = yield* Workers.make.pipe(Effect.provide(f.dependencies));
      const freshRpc = yield* f.rpc(undefined, undefined, restarted);
      assert.deepEqual((yield* freshRpc[C.WS_METHODS.workerReceipt](lookup)).status, { state: "outcome_unknown", reason: "unavailable" });
      yield* f.a[C.WS_METHODS.workerCloseView](view);
      yield* Fiber.join(view.fiber);
      yield* TestClock.adjust("5 minutes");
      assert.deepEqual((yield* f.b[C.WS_METHODS.workerReceipt](lookup)).status, { state: "outcome_unknown", reason: "unavailable" });
    }).pipe(Effect.scoped, Effect.provide(NodeCrypto.layer)),
  );
  it.effect("keeps old-lease expiry uncertain and never asks a new lease for not_admitted", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const view = yield* f.start();
      yield* f.activate(view);
      yield* f.a[C.WS_METHODS.workerAction]({ ...view, actionId: id(view, 1), selection: f.selection,
        operation: { kind: "cancel" } });
      yield* f.a[C.WS_METHODS.workerCloseView](view);
      yield* Fiber.join(view.fiber);
      f.state.leaseExpired = true;
      const outcome = yield* f.b[C.WS_METHODS.workerReceipt]({
        ...f.target, actionId: id(view, 1), access: { recoveryHandle: view.recoveryHandle },
      });
      assert.deepEqual(outcome.status, { state: "outcome_unknown", reason: "lease_expired" });
      assert.equal(f.calls.resumes, 1);
      assert.deepEqual(f.calls.status, []);
      assert.deepEqual(f.calls.effects, [8]);
      assert.equal(f.calls.grant, 1);
    }).pipe(Effect.scoped, Effect.provide(NodeCrypto.layer)),
  );
  it.effect("denies receipt lookup if auth expires while joining a failed native socket", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const view = yield* f.start();
      yield* f.activate(view);
      yield* f.a[C.WS_METHODS.workerAction]({ ...view, selection: f.selection, actionId: id(view, 1),
        operation: { kind: "cancel" } });
      const lookup = { ...f.target, actionId: id(view, 1), access: { viewId: view.viewId } };
      f.state.disconnectFailure = true;
      assert.deepEqual((yield* f.a[C.WS_METHODS.workerReceipt](lookup)).status, {
        state: "outcome_unknown", reason: "disconnected",
      });
      const statuses = f.calls.status.length;
      f.state.duringDisconnect = () => { f.state.activeAuth = false; };
      assert.equal((yield* f.a[C.WS_METHODS.workerReceipt](lookup).pipe(Effect.flip, Effect.map(workerError))).code, "forbidden");
      assert.equal(f.calls.status.length, statuses);
      assert.deepEqual(f.calls.effects, [8]);
    }).pipe(Effect.scoped, Effect.provide(NodeCrypto.layer)),
  );
  it.effect("supersedes busy B only when later C is admitted, retaining exactly the latest 32 mappings", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const view = yield* f.start();
      yield* f.activate(view);
      const action: typeof C.WorkerActionInput.Type = { ...view, actionId: id(view, 1), selection: f.selection,
        operation: { kind: "input", input: { intent: "query", text: "A" } } };
      const b = { ...action, actionId: id(view, 2) };
      const c = { ...action, actionId: id(view, 3) };
      f.state.hold = true;
      const pending = yield* f.a[C.WS_METHODS.workerAction](action).pipe(Effect.forkScoped);
      assert.equal(yield* Queue.take(f.admissions), 8);
      const before = { details: f.calls.details, leaseReads: f.calls.leaseReads };
      for (const decision of [b, c])
        assert.equal((yield* f.a[C.WS_METHODS.workerAction](decision).pipe(Effect.flip, Effect.map(workerError))).code, "busy");
      assert.deepEqual({ details: f.calls.details, leaseReads: f.calls.leaseReads }, before);
      assert.deepEqual(f.calls.effects, [8]);
      f.state.hold = false;
      f.state.resolve!();
      yield* Fiber.join(pending);
      // Explicit C admission, not its earlier busy rejection, retires B.
      yield* f.a[C.WS_METHODS.workerAction](c);
      assert.deepEqual((yield* f.a[C.WS_METHODS.workerAction](b)).status, {
        state: "outcome_unknown", reason: "mapping_evicted",
      });
      for (let ordinal = 4; ordinal <= 40; ordinal++)
        yield* f.a[C.WS_METHODS.workerAction]({ ...action, actionId: id(view, ordinal) });
      assert.equal(f.calls.effects.length, 39);
      const statuses = f.calls.status.length;
      // Membership/comparison evidence is gone. Neither original nor altered IDs may re-effect.
      for (const retired of [action, b, { ...action, operation: { kind: "cancel" } as const }])
        assert.deepEqual((yield* f.a[C.WS_METHODS.workerAction](retired)).status, {
          state: "outcome_unknown", reason: "mapping_evicted",
        });
      for (let ordinal = 1; ordinal <= 40; ordinal++) {
        const receipt = yield* f.a[C.WS_METHODS.workerReceipt]({
          ...f.target, actionId: id(view, ordinal), access: { viewId: view.viewId },
        });
        assert.equal(receipt.status.state, ordinal >= 9 ? "recorded" : "outcome_unknown");
      }
      assert.equal(f.calls.status.length - statuses, 32);
      const retained = { ...action, actionId: id(view, 9) };
      yield* f.a[C.WS_METHODS.workerAction](retained); // A duplicate must not refresh insertion order.
      assert.equal((yield* f.a[C.WS_METHODS.workerAction]({ ...retained, operation: { kind: "cancel" } }).pipe(
        Effect.flip, Effect.map(workerError),
      )).code, "action_conflict");
      yield* f.a[C.WS_METHODS.workerAction]({ ...action, actionId: id(view, 41) });
      assert.deepEqual((yield* f.a[C.WS_METHODS.workerAction](retained)).status, {
        state: "outcome_unknown", reason: "mapping_evicted",
      });
      assert.equal(f.calls.effects.length, 40);
      assert.equal(f.calls.opens, 1);
      assert.equal(f.calls.grant, 1);
      assert.equal(f.calls.disconnect, 0);
    }).pipe(Effect.scoped, Effect.provide(NodeCrypto.layer)),
  );
  it.effect("validates canonical view identity and rejects retired ordinal aliases without using native high-water as membership", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const viewer: Workers.WorkerViewer = { session: f.session, connectionId: "direct" };
      const events = yield* Queue.unbounded<typeof C.WorkerViewEvent.Type>();
      yield* f.service.openView(viewer, f.target).pipe(
        Stream.runForEach((event) => Queue.offer(events, event)), Effect.forkScoped);
      const event = yield* Queue.take(events);
      if (event.type !== "opened") throw new Error("Expected view");
      const view = { ...f.target, viewId: event.viewId };
      yield* f.service.controlDetail(viewer, { ...view, id: "worker" });
      yield* f.service.activate(viewer, { ...view, selection: f.selection, acknowledged: true });
      const action: typeof C.WorkerActionInput.Type = { ...view, actionId: id(view, 10), selection: f.selection,
        operation: { kind: "cancel" } };
      for (const malformed of ["arbitrary", id({ viewId: "another-view" }, 10),
        action.actionId.replace(":10:", ":010:"), action.actionId.replace("w1:", "w2:")]) {
        assert.equal((yield* f.service.action(viewer, { ...action, actionId: malformed }).pipe(Effect.flip)).code, "invalid_action_id");
      }
      assert.deepEqual(f.calls.effects, []);
      assert.equal(f.calls.leaseReads, 0);
      yield* f.service.action(viewer, action);
      for (const retired of [id(view, 10, "00000000-0000-0000-0000-000000000002"), id(view, 9), id(view, 1)])
        assert.deepEqual((yield* f.service.action(viewer, { ...action, actionId: retired })).status, {
          state: "outcome_unknown", reason: "mapping_evicted",
        });
      assert.deepEqual(f.calls.effects, [8]); // Ordinal 10 did not choose native sequence 10.
      f.state.highWater = Number.MAX_SAFE_INTEGER;
      const last = { ...action, actionId: id(view, Number.MAX_SAFE_INTEGER) };
      assert.equal((yield* f.service.action(viewer, last).pipe(Effect.flip)).code, "action_limit");
      assert.deepEqual(f.calls.effects, [8]);
      f.state.highWater = 7;
      // Failed native exhaustion did not register the ID or advance either frontier.
      yield* f.service.action(viewer, { ...action, actionId: id(view, 11) });
      yield* f.service.action(viewer, last);
      assert.deepEqual(f.calls.effects, [8, 9, 10]);
      assert.deepEqual((yield* f.service.action(viewer, { ...action, actionId: id(view, 12) })).status, {
        state: "outcome_unknown", reason: "mapping_evicted",
      });
      assert.equal((yield* f.service.action(viewer, { ...action,
        actionId: `w1:${encodeURIComponent(view.viewId)}:9007199254740992:00000000-0000-0000-0000-000000000001`,
      }).pipe(Effect.flip)).code, "invalid_action_id");
      assert.deepEqual(f.calls.effects, [8, 9, 10]);
    }).pipe(Effect.scoped, Effect.provide(NodeCrypto.layer)),
  );
});
