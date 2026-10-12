import { describe, expect, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import * as C from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as Option from "effect/Option";
import { Atom, AtomRegistry, AsyncResult } from "effect/reactivity";
import { EnvironmentRegistry } from "../connection/registry.ts";
import { EnvironmentSupervisor } from "../connection/supervisor.ts";
import { PrimaryConnectionTarget } from "../connection/model.ts";
import type { RpcSession } from "../rpc/session.ts";
import { createProviderWorkersEnvironmentAtoms, type WorkerPrivateState } from "./providerWorkers.ts";

vi.mock("./session.ts", () => ({ createEnvironmentSessionAtoms: () => ({ sessionStateAtom: sessions }) }));
const sessions = Atom.family((_id: C.EnvironmentId) => Atom.make(AsyncResult.success<C.AuthSessionState>({
  authenticated: true, scopes: [C.AuthOrchestrationReadScope, C.AuthDiagnosticsReadScope, C.AuthOrchestrationOperateScope],
  permissions: [C.AuthOrchestrationReadScope, C.AuthDiagnosticsReadScope, C.AuthOrchestrationOperateScope],
  auth: { policy: "remote-reachable", bootstrapMethods: [], sessionMethods: [], sessionCookieName: "test" },
})));

describe("worker private state", () => {
  it.effect("keeps bounded pages per environment and clears them on grant, owner, stream and stale-page changes", () =>
    Effect.scoped(Effect.gen(function* () {
      const env = C.EnvironmentId.make("env");
      const other = C.EnvironmentId.make("other");
      const target = { environmentId: env, threadId: C.ThreadId.make("thread") };
      const opened: Extract<typeof C.WorkerViewEvent.Type, { type: "opened" }> = {
        type: "opened", viewId: "view", recoveryHandle: "recovery",
        owner: { processId: "pid", coordinatorSessionId: "session", epoch: "epoch" }, grants: [],
      };
      const events = yield* SubscriptionRef.make<typeof C.WorkerViewEvent.Type>(opened);
      const observation = { state: "last_observation", owner: opened.owner, revision: 1, observedAt: 100 } as const;
      const observations = yield* SubscriptionRef.make<typeof C.WorkerObservation.Type>(observation);
      const roster: typeof C.WorkerRoster.Type = { owner: opened.owner, revision: 1, items: [], cursor: null };
      let listReads = 0;
      const page = { items: [], diagnostics: [{ code: "native_missing" }], cursor: null };
      const routePage: typeof C.WorkerRoute.Type = {
        state: "captured", page: { text: "private route", offset: 0, nextOffset: null },
      };
      const selection: typeof C.WorkerSelection.Type = {
        ...opened.owner, target: { kind: "background", instanceId: "worker" },
        instanceId: "worker", generation: 1, historyId: "history", sessionId: "child",
      };
      const control: typeof C.WorkerControlDetail.Type = {
        state: "ready", ref: { kind: "read", ...opened.owner, instanceId: "worker", source: "agent_spawn" },
        status: "running", observedStatus: "running", result: "pending", activity: "observed_binding",
        controls: { grant: "denied", selection },
      };
      let effects = 0;
      let lookups = 0;
      const reply = yield* Deferred.make<typeof C.WorkerActionOutcome.Type, C.ProviderWorkersError>();
      const sentIds: string[] = [];
      const sentTexts: Array<string | undefined> = [];
      let actionResponse: Effect.Effect<typeof C.WorkerActionOutcome.Type, C.ProviderWorkersError> = Deferred.await(reply);
      let actionId = "";
      const outcome = (id = actionId, sequence = 1): typeof C.WorkerActionOutcome.Type => ({ actionId: id, status: {
        state: "recorded", sequence, receipt: { kind: "input", native: {
          interactionId: "interaction", historyId: "history", sessionId: "child", intent: "query", status: "queued",
        } },
      } });
      const handles = new Map([["view", "recovery"], ["view2", "recovery2"], ["view3", "recovery3"]]);
      const records = new Map<string, { viewId: string; recoveryHandle: string; outcome: typeof C.WorkerActionOutcome.Type }>();
      const lookupInputs: Array<typeof C.WorkerReceiptInput.Type> = [];
      let expired = false;
      let historyResponse: Effect.Effect<typeof C.WorkerHistoryRead.Type, C.ProviderWorkersError> = Effect.succeed(page);
      const session = {
        client: {
          [C.WS_METHODS.workerOpenView]: () => SubscriptionRef.changes(events),
          [C.WS_METHODS.workerObserve]: () => SubscriptionRef.changes(observations),
          [C.WS_METHODS.workerList]: () => Effect.sync(() => { listReads++; return roster; }),
          [C.WS_METHODS.workerHistoryRead]: () => historyResponse,
          [C.WS_METHODS.workerRouteRead]: () => Effect.succeed(routePage),
          [C.WS_METHODS.workerControlDetail]: () => Effect.succeed(control),
          [C.WS_METHODS.workerActivate]: () => Effect.void,
          [C.WS_METHODS.workerAction]: (input: typeof C.WorkerActionInput.Type) =>
            Effect.sync(() => {
              effects++;
              sentIds.push(input.actionId);
              sentTexts.push(input.operation.kind === "input" ? input.operation.input.text : undefined);
              records.set(input.actionId, {
                viewId: input.viewId, recoveryHandle: handles.get(input.viewId)!,
                outcome: outcome(input.actionId, effects),
              });
            }).pipe(Effect.andThen(() => actionResponse)),
          [C.WS_METHODS.workerReceipt]: (input: typeof C.WorkerReceiptInput.Type) => Effect.gen(function* () {
            lookups++;
            lookupInputs.push(input);
            const record = records.get(input.actionId);
            if (!record || ("viewId" in input.access
              ? input.access.viewId !== record.viewId
              : input.access.recoveryHandle !== record.recoveryHandle))
              return yield* new C.ProviderWorkersError({ code: "view_closed" });
            return expired
              ? { actionId: input.actionId, status: { state: "outcome_unknown" as const, reason: "lease_expired" as const } }
              : record.outcome;
          }),
        },
      } as unknown as RpcSession;
      const sessionRef = yield* SubscriptionRef.make(Option.some(session));
      const supervisor = {
        session: sessionRef,
        target: new PrimaryConnectionTarget({
          environmentId: env, label: "test", httpBaseUrl: "http://fixture", wsBaseUrl: "ws://fixture",
        }),
      } as unknown as EnvironmentSupervisor["Service"];
      const entries = yield* SubscriptionRef.make<ReadonlyMap<C.EnvironmentId, import("../connection/catalog.ts").ConnectionCatalogEntry>>(new Map());
      const networkStatus = yield* SubscriptionRef.make<"online" | "offline" | "unknown">("online");
      const runtime = Atom.runtime(Layer.mock(EnvironmentRegistry)({
        entries, networkStatus,
        run: (_id, effect) => Effect.provideService(effect, EnvironmentSupervisor, supervisor),
        followStream: (_id, stream) => Stream.provideService(stream, EnvironmentSupervisor, supervisor),
      }));
      let nonceCalls = 0;
      const atoms = createProviderWorkersEnvironmentAtoms(runtime, () => {
        nonceCalls++;
        return `00000000-0000-0000-0000-${String(nonceCalls).padStart(12, "0")}`;
      });
      const registry = AtomRegistry.make();
      yield* Effect.addFinalizer(() => Effect.sync(() => registry.dispose()));
      const state = atoms.privateState(target);
      const view = atoms.view({ environmentId: env, input: target });
      const unmount = registry.mount(view);
      yield* AtomRegistry.getResult(registry, view);
      const observe = atoms.observe({ environmentId: env, input: target });
      const unmountObserve = registry.mount(observe);
      yield* AtomRegistry.getResult(registry, observe);
      yield* Effect.promise(() => atoms.list.run(registry, { environmentId: env, input: target }));
      const cached = atoms.pages(target);
      expect(registry.get(cached).list).toEqual(roster);
      expect(registry.get(atoms.pages({ ...target, environmentId: other })).list).toBeNull();
      expect(registry.get(atoms.pages({ ...target, threadId: C.ThreadId.make("other-thread") })).list).toBeNull();
      yield* SubscriptionRef.set(observations, { ...observation, revision: 2 });
      yield* Effect.callback<void>((resume) => {
        const unsubscribe = registry.subscribe(cached, (value) => {
          if (value.list === null) resume(Effect.void);
        }, { immediate: true });
        return Effect.sync(unsubscribe);
      });
      expect(registry.get(cached).list).toBeNull();
      expect(listReads).toBe(1);
      // A late page from the previous revision cannot refill the invalidated traversal.
      yield* Effect.promise(() => atoms.list.run(registry, { environmentId: env, input: target }));
      expect(registry.get(cached).list).toBeNull();
      expect(registry.get(state).view?.grants).toEqual([]);
      const waitFor = (predicate: (value: WorkerPrivateState) => boolean) => Effect.callback<void>((resume) => {
        const unsubscribe = registry.subscribe(state, (value) => {
          if (predicate(value)) resume(Effect.void);
        }, { immediate: true });
        return Effect.sync(unsubscribe);
      });
      yield* SubscriptionRef.set(events, { ...opened, grants: ["history", "route"] });
      yield* waitFor((value) => value.view?.grants.length === 2);
      yield* Effect.promise(() => atoms.historyRead.run(registry, {
        environmentId: env, input: { ...target, viewId: "view", historyId: "history" },
      }));
      expect(registry.get(state).history).toEqual(page);
      yield* Effect.promise(() => atoms.routeRead.run(registry, {
        environmentId: env, input: { ...target, viewId: "view", id: "worker" },
      }));
      expect(registry.get(state).route).toEqual(routePage);
      const empty = { view: null, history: null, route: null, control: null, selection: null, recovery: null,
        decisions: [], decisionOrdinal: 0, pending: false, outcomes: [] };
      expect(registry.get(atoms.privateState({ ...target, environmentId: other }))).toEqual(empty);
      expect(registry.get(atoms.privateState({ ...target, threadId: C.ThreadId.make("other-thread") }))).toEqual(empty);
      yield* SubscriptionRef.set(events, { ...opened, grants: ["route"] });
      yield* waitFor((value) => value.view?.grants.length === 1);
      expect(registry.get(state).history).toBeNull();
      expect(registry.get(state).route).toEqual(routePage);
      yield* SubscriptionRef.set(events, { ...opened, grants: ["history", "route"] });
      yield* waitFor((value) => value.view?.grants.length === 2);
      yield* Effect.promise(() => atoms.historyRead.run(registry, {
        environmentId: env, input: { ...target, viewId: "view", historyId: "history" },
      }));
      historyResponse = Effect.fail(new C.ProviderWorkersError({ code: "stale_cursor" }));
      yield* Effect.promise(() => atoms.historyRead.run(registry, {
        environmentId: env, input: { ...target, viewId: "view", historyId: "history", cursor: "stale" },
      }));
      expect(registry.get(state).history).toBeNull();
      expect(registry.get(state).route).toEqual(routePage);
      const privateTarget = { environmentId: env, input: { ...target, viewId: "view" } };
      yield* Effect.promise(() => atoms.controlDetail.run(registry, {
        ...privateTarget, input: { ...privateTarget.input, id: "worker" },
      }));
      expect(registry.get(state).selection).toBeNull();
      yield* Effect.promise(() => atoms.activate.run(registry, {
        ...privateTarget, input: { ...privateTarget.input, selection, acknowledged: true },
      }));
      expect(registry.get(state).selection).toEqual(selection);
      const input = { intent: "query" as const, text: "a native later-frontier note" };
      const deliberate = { ...privateTarget, input: { ...privateTarget.input, selection,
        operation: { kind: "input" as const, input } } };
      const decision = atoms.createDecision(registry, deliberate);
      input.text = "a changed draft, not the issued decision";
      actionId = decision.actionId;
      expect(C.parseWorkerActionId(actionId)?.ordinal).toBe(1);
      const action = yield* Effect.promise(() => atoms.action.run(registry, {
        ...privateTarget, input: decision,
      })).pipe(Effect.forkScoped);
      yield* waitFor((value) => value.pending);
      expect(() => atoms.createDecision(registry, deliberate)).toThrow(C.ProviderWorkersError);
      expect(registry.get(state).decisionOrdinal).toBe(1);
      expect(nonceCalls).toBe(1);
      expect(registry.get(state).decisions).toEqual([{ decision, recoveryHandle: "recovery" }]);
      const localBusy = yield* Effect.promise(() => atoms.action.run(registry, { ...privateTarget, input: decision }));
      expect(localBusy._tag).toBe("Failure");
      expect(registry.get(state).outcomes).toEqual([]);
      historyResponse = Effect.succeed(page);
      yield* Effect.promise(() => atoms.historyRead.run(registry, {
        environmentId: env, input: { ...target, viewId: "view", historyId: "history" },
      }));
      const beforeTargetInvalidation = registry.get(state);
      yield* SubscriptionRef.set(events, { type: "invalidated", reason: "stale_selection" });
      yield* waitFor((value) => value.control === null && value.selection === null);
      const afterTargetInvalidation = registry.get(state);
      expect(afterTargetInvalidation.view).toBe(beforeTargetInvalidation.view);
      expect(afterTargetInvalidation.history).toBe(beforeTargetInvalidation.history);
      expect(afterTargetInvalidation.route).toBe(beforeTargetInvalidation.route);
      expect(afterTargetInvalidation.pending).toBe(true);
      expect(afterTargetInvalidation.decisions).toBe(beforeTargetInvalidation.decisions);
      expect(afterTargetInvalidation.outcomes).toBe(beforeTargetInvalidation.outcomes);
      expect(afterTargetInvalidation.recovery).toBe(beforeTargetInvalidation.recovery);
      expect(afterTargetInvalidation.decisionOrdinal).toBe(1);
      yield* Deferred.fail(reply, new C.ProviderWorkersError({ code: "disconnected" }));
      yield* Fiber.join(action);
      expect(registry.get(state).decisions).toEqual([{ decision, recoveryHandle: "recovery" }]);
      expect(registry.get(state).pending).toBe(false);
      expect(sentIds).toEqual([actionId]);
      expect(sentTexts).toEqual(["a native later-frontier note"]);
      yield* SubscriptionRef.set(events, { type: "invalidated", reason: "disconnected" });
      yield* waitFor((value) => value.view === null);
      expect(registry.get(state).control).toBeNull();
      expect(registry.get(state).selection).toBeNull();
      expect(registry.get(state).history).toBeNull();
      expect(registry.get(state).route).toBeNull();
      expect(registry.get(state).recovery?.recoveryHandle).toBe("recovery");
      expect(registry.get(state).decisionOrdinal).toBe(1);
      yield* SubscriptionRef.set(events, opened);
      yield* waitFor((value) => value.view?.viewId === opened.viewId);
      expect(registry.get(state).decisionOrdinal).toBe(1);
      expect(registry.get(state).decisions).toEqual([{ decision, recoveryHandle: "recovery" }]);
      expect(nonceCalls).toBe(1);
      yield* SubscriptionRef.set(events, { type: "invalidated", reason: "disconnected" });
      yield* waitFor((value) => value.view === null);
      // Reopening after transport loss must not overwrite the deliberate action's recovery authority.
      yield* SubscriptionRef.set(events, { ...opened, viewId: "view2", recoveryHandle: "recovery2" });
      yield* waitFor((value) => value.view?.viewId === "view2");
      expect(registry.get(state).selection).toBeNull();
      expect(registry.get(state).view?.grants).toEqual([]);
      expect(registry.get(state).recovery?.recoveryHandle).toBe("recovery2");
      expect(registry.get(state).decisions).toEqual([{ decision, recoveryHandle: "recovery" }]);
      expect(effects).toBe(1);
      expect(lookups).toBe(0);
      expect(nonceCalls).toBe(1);
      expect(registry.get(state).decisionOrdinal).toBe(0);
      // Server-local busy can outlive the waiting client. It does not consume an issued decision.
      const nextTarget = { ...deliberate, input: { ...deliberate.input, viewId: "view2" } };
      const b = atoms.createDecision(registry, nextTarget);
      actionResponse = Effect.fail(new C.ProviderWorkersError({ code: "busy" }));
      const serverBusy = yield* Effect.promise(() => atoms.action.run(registry, { ...privateTarget, input: b }));
      expect(serverBusy._tag).toBe("Failure");
      expect(registry.get(state).decisions).toEqual([
        { decision, recoveryHandle: "recovery" }, { decision: b, recoveryHandle: "recovery2" },
      ]);
      expect(registry.get(state).decisionOrdinal).toBe(1);
      expect(nonceCalls).toBe(2);
      actionId = b.actionId;
      actionResponse = Effect.sync(() => outcome(b.actionId, effects));
      yield* Effect.promise(() => atoms.action.run(registry, { ...privateTarget, input: b }));
      expect(sentIds).toEqual([decision.actionId, b.actionId, b.actionId]);
      expect(nonceCalls).toBe(2); // Explicit submission reused the exact ID, not a renamed retry.
      const retained = registry.get(state).decisions;
      const old = retained.find((item) => item.decision === decision)!;
      const current = retained.find((item) => item.decision === b)!;
      expect(C.parseWorkerActionId(b.actionId)?.ordinal).toBe(1);
      const oldAction = yield* Effect.promise(() => atoms.action.run(registry, { ...privateTarget, input: old.decision }));
      expect(oldAction._tag).toBe("Failure");
      expect(effects).toBe(3);
      yield* Effect.promise(() => atoms.receipt.run(registry, {
        environmentId: env, input: { ...target, actionId: old.decision.actionId, access: { recoveryHandle: old.recoveryHandle } },
      }));
      expect(registry.get(state).outcomes.map((item) => item.actionId)).toEqual([b.actionId, decision.actionId]);
      yield* Effect.promise(() => atoms.receipt.run(registry, {
        environmentId: env, input: { ...target, actionId: current.decision.actionId, access: { recoveryHandle: current.recoveryHandle } },
      }));
      expect(registry.get(state).outcomes).toEqual([outcome(decision.actionId, 1), outcome(b.actionId, 3)]);
      expect(lookupInputs.map((input) => [input.actionId, input.access])).toEqual([
        [decision.actionId, { recoveryHandle: "recovery" }], [b.actionId, { recoveryHandle: "recovery2" }],
      ]);
      expect(registry.get(state).selection).toBeNull();
      expect(registry.get(state).view?.grants).toEqual([]);
      expect(effects).toBe(3);
      const beforeExhaustion = registry.get(state);
      registry.set(state, { ...beforeExhaustion, decisionOrdinal: Number.MAX_SAFE_INTEGER });
      expect(() => atoms.createDecision(registry, nextTarget)).toThrow(C.ProviderWorkersError);
      expect(nonceCalls).toBe(2);
      registry.set(state, beforeExhaustion);
      for (let i = 0; i < 35; i++) atoms.createDecision(registry, nextTarget);
      expect(registry.get(state).decisions).toHaveLength(32);
      expect(registry.get(state).decisionOrdinal).toBe(36);
      expect(nonceCalls).toBe(37);
      expect(effects).toBe(3); // Creation/eviction never dispatches.
      const forgotten = yield* Effect.promise(() => atoms.action.run(registry, { ...privateTarget, input: b }));
      expect(forgotten._tag).toBe("Failure");
      expect(effects).toBe(3);
      expect(registry.get(state).outcomes).toEqual([]); // Eviction drops the matching outcomes and credentials too.
      expect(registry.get(state).decisions.every((item) => item.recoveryHandle === "recovery2")).toBe(true);

      // A response already in flight belongs to its old decision, even while the replacement action is pending.
      const lateReply = yield* Deferred.make<typeof C.WorkerActionOutcome.Type, C.ProviderWorkersError>();
      const lateStarted = yield* Deferred.make<void>();
      actionResponse = Deferred.succeed(lateStarted, undefined).pipe(Effect.andThen(Deferred.await(lateReply)));
      const c = atoms.createDecision(registry, nextTarget);
      const late = yield* Effect.promise(() => atoms.action.run(registry, { ...privateTarget, input: c })).pipe(Effect.forkScoped);
      yield* Deferred.await(lateStarted);
      yield* SubscriptionRef.set(events, { type: "invalidated", reason: "disconnected" });
      yield* waitFor((value) => value.view === null);
      yield* SubscriptionRef.set(events, { ...opened, viewId: "view3", recoveryHandle: "recovery3" });
      yield* waitFor((value) => value.view?.viewId === "view3");
      const newReply = yield* Deferred.make<typeof C.WorkerActionOutcome.Type, C.ProviderWorkersError>();
      const newStarted = yield* Deferred.make<void>();
      actionResponse = Deferred.succeed(newStarted, undefined).pipe(Effect.andThen(Deferred.await(newReply)));
      const d = atoms.createDecision(registry, { ...nextTarget, input: { ...nextTarget.input, viewId: "view3" } });
      const pending = yield* Effect.promise(() => atoms.action.run(registry, { ...privateTarget, input: d })).pipe(Effect.forkScoped);
      yield* Deferred.await(newStarted);
      yield* Deferred.succeed(lateReply, outcome(c.actionId, 4));
      yield* Fiber.join(late);
      expect(registry.get(state).pending).toBe(true);
      expect(registry.get(state).recovery?.recoveryHandle).toBe("recovery3");
      expect(registry.get(state).view?.viewId).toBe("view3");
      expect(registry.get(state).view?.grants).toEqual([]);
      expect(registry.get(state).selection).toBeNull();
      expect(registry.get(state).outcomes).toEqual([outcome(c.actionId, 4)]);
      const wrongHandle = yield* Effect.promise(() => atoms.receipt.run(registry, {
        environmentId: env, input: { ...target, actionId: c.actionId, access: { recoveryHandle: "recovery3" } },
      }));
      expect(wrongHandle._tag).toBe("Failure");
      expect(registry.get(state).outcomes).toEqual([outcome(c.actionId, 4)]);
      yield* Effect.promise(() => atoms.receipt.run(registry, {
        environmentId: env, input: { ...target, actionId: c.actionId, access: { recoveryHandle: "recovery2" } },
      }));
      expect(registry.get(state).pending).toBe(true);
      yield* Deferred.succeed(newReply, outcome(d.actionId, 5));
      yield* Fiber.join(pending);
      expect(registry.get(state).pending).toBe(false);
      yield* Effect.promise(() => atoms.receipt.run(registry, {
        environmentId: env, input: { ...target, actionId: d.actionId, access: { viewId: "view3" } },
      }));
      expect(registry.get(state).outcomes).toEqual([outcome(c.actionId, 4), outcome(d.actionId, 5)]);
      expired = true;
      yield* Effect.promise(() => atoms.receipt.run(registry, {
        environmentId: env, input: { ...target, actionId: c.actionId, access: { recoveryHandle: "recovery2" } },
      }));
      expect(registry.get(state).outcomes).toEqual([
        outcome(d.actionId, 5), { actionId: c.actionId, status: { state: "outcome_unknown", reason: "lease_expired" } },
      ]);
      expect(effects).toBe(5); // Explicit lookup, including uncertainty, never dispatches.
      expired = false;

      // Repeated fresh views cannot accumulate an unbounded collection of receipt credentials.
      for (let i = 0; i < 35; i++) {
        const viewId = `rolling-view-${i}`;
        const recoveryHandle = `rolling-recovery-${i}`;
        handles.set(viewId, recoveryHandle);
        yield* SubscriptionRef.set(events, { ...opened, viewId, recoveryHandle });
        yield* waitFor((value) => value.view?.viewId === viewId);
        const rolling = atoms.createDecision(registry, { ...nextTarget, input: { ...nextTarget.input, viewId } });
        expect(C.parseWorkerActionId(rolling.actionId)?.ordinal).toBe(1);
        actionResponse = Effect.sync(() => outcome(rolling.actionId, effects));
        yield* Effect.promise(() => atoms.action.run(registry, { ...privateTarget, input: rolling }));
      }
      expect(registry.get(state).decisions).toHaveLength(32);
      expect(registry.get(state).outcomes).toHaveLength(32);
      expect(registry.get(state).decisions.map((item) => item.recoveryHandle)).toEqual(
        Array.from({ length: 32 }, (_, i) => `rolling-recovery-${i + 3}`),
      );
      expect(registry.get(state).outcomes.map((item) => item.actionId)).toEqual(
        registry.get(state).decisions.map((item) => item.decision.actionId),
      );
      yield* Effect.promise(() => atoms.receipt.run(registry, {
        environmentId: env, input: { ...target, actionId: d.actionId, access: { recoveryHandle: "recovery3" } },
      }));
      expect(registry.get(state).outcomes).toHaveLength(32);
      expect(registry.get(state).outcomes.some((item) => item.actionId === d.actionId)).toBe(false);
      expect(effects).toBe(40);
      yield* SubscriptionRef.set(events, { ...opened, owner: { ...opened.owner, epoch: "next" } });
      yield* waitFor((value) => value.view?.owner.epoch === "next");
      expect(registry.get(state).history).toBeNull();
      expect(registry.get(state).route).toBeNull();
      expect(registry.get(state).decisions).toEqual([]);
      expect(registry.get(state).outcomes).toEqual([]);
      yield* Effect.promise(() => atoms.receipt.run(registry, {
        environmentId: env, input: { ...target, actionId: d.actionId, access: { recoveryHandle: "recovery3" } },
      }));
      expect(registry.get(state).outcomes).toEqual([]);
      const foreignAction = yield* Effect.promise(() => atoms.action.run(registry, { ...privateTarget, input: d }));
      expect(foreignAction._tag).toBe("Failure");
      expect(effects).toBe(40);
      yield* SubscriptionRef.set(sessionRef, Option.none());
      yield* waitFor((value) => value.view === null);
      yield* SubscriptionRef.set(sessionRef, Option.some(session));
      yield* waitFor((value) => value.view?.owner.epoch === "next");
      expect(registry.get(state).view?.grants).toEqual([]);
      unmount();
      unmountObserve();
      yield* waitFor((value) => value.view === null);
    })),
  );
});
