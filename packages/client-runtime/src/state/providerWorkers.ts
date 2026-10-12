import * as C from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Cause from "effect/Cause";
import * as Stream from "effect/Stream";
import { Atom, AtomRegistry, AsyncResult } from "effect/reactivity";
import type { EnvironmentRegistry } from "../connection/registry.ts";
import { createEnvironmentRpcCommand, createEnvironmentRpcSubscriptionAtomFamily, environmentRpcKey } from "./runtime.ts";

type OpenedView = Extract<typeof C.WorkerViewEvent.Type, { type: "opened" }>;
const decisionBrand: unique symbol = Symbol("WorkerDecision");
export type WorkerDecision = typeof C.WorkerActionInput.Type & { readonly [decisionBrand]: true };
export interface WorkerPrivateState {
  readonly view: OpenedView | null;
  readonly history: typeof C.WorkerHistoryRead.Type | null;
  readonly route: typeof C.WorkerRoute.Type | null;
  readonly control: typeof C.WorkerControlDetail.Type | null;
  readonly selection: typeof C.WorkerSelection.Type | null;
  /** Last operational view identity, including while disconnected. */
  readonly recovery: Pick<OpenedView, "viewId" | "recoveryHandle" | "owner"> | null;
  /** Each retained decision keeps its receipt-only credential, never operational authority. */
  readonly decisions: ReadonlyArray<{ readonly decision: WorkerDecision; readonly recoveryHandle: string }>;
  readonly decisionOrdinal: number;
  readonly pending: boolean;
  readonly outcomes: ReadonlyArray<typeof C.WorkerActionOutcome.Type>;
}
const empty: WorkerPrivateState = {
  view: null, history: null, route: null, control: null, selection: null, recovery: null,
  decisions: [], decisionOrdinal: 0, pending: false, outcomes: [],
};
const endView = (state: WorkerPrivateState): WorkerPrivateState => ({
  ...empty, recovery: state.recovery, decisions: state.decisions, decisionOrdinal: state.decisionOrdinal,
  outcomes: state.outcomes,
});
export interface WorkerPages {
  readonly observation: typeof C.WorkerObservation.Type | null;
  readonly list: typeof C.WorkerRoster.Type | null;
  readonly detail: typeof C.WorkerDetail.Type | null;
  readonly requests: typeof C.WorkerRequests.Type | null;
  readonly activity: typeof C.WorkerActivity.Type | null;
  readonly result: typeof C.WorkerResult.Type | null;
  readonly historyList: typeof C.WorkerHistoryList.Type | null;
}
const emptyPages: WorkerPages = {
  observation: null, list: null, detail: null, requests: null, activity: null, result: null, historyList: null,
};

/** Only the latest bounded page is retained. Authority changes discard it, never merge traversals. */
export function workerPrivateStateForEvent(
  state: WorkerPrivateState, event: typeof C.WorkerViewEvent.Type,
): WorkerPrivateState {
  if (event.type === "invalidated") return event.reason === "stale_selection"
    ? { ...state, control: null, selection: null } : endView(state);
  const same = state.view?.viewId === event.viewId &&
    state.view.owner.processId === event.owner.processId &&
    state.view.owner.coordinatorSessionId === event.owner.coordinatorSessionId &&
    state.view.owner.epoch === event.owner.epoch;
  // Replacement views retain same-owner decisions, not their grants or selection.
  const recover = state.recovery?.owner.processId === event.owner.processId &&
    state.recovery.owner.coordinatorSessionId === event.owner.coordinatorSessionId &&
    state.recovery.owner.epoch === event.owner.epoch;
  return {
    view: event,
    history: same && event.grants.includes("history") ? state.history : null,
    route: same && event.grants.includes("route") ? state.route : null,
    control: same ? state.control : null,
    selection: same ? state.selection : null,
    recovery: { viewId: event.viewId, recoveryHandle: event.recoveryHandle, owner: event.owner },
    decisions: same || recover ? state.decisions : [],
    decisionOrdinal: same || (recover && state.recovery?.viewId === event.viewId) ? state.decisionOrdinal : 0,
    pending: same ? state.pending : false,
    outcomes: same || recover ? state.outcomes : [],
  };
}

export function createProviderWorkersEnvironmentAtoms<R, E>(
  runtime: Atom.AtomRuntime<EnvironmentRegistry | R, E>,
  randomUUID: () => string,
) {
  const privateStates = Atom.family((_key: string) => Atom.make<WorkerPrivateState>(empty));
  const pageStates = Atom.family((_key: string) => Atom.make<WorkerPages>(emptyPages));
  const pages = (target: typeof C.WorkerTarget.Type) => pageStates(environmentRpcKey({
    environmentId: target.environmentId, input: { threadId: target.threadId },
  }));
  const storePage = (
    target: { environmentId: C.EnvironmentId; input: { threadId: C.ThreadId } },
    registry: AtomRegistry.AtomRegistry, page: Partial<WorkerPages>,
  ) => Effect.sync(() => {
    const atom = pages({ environmentId: target.environmentId, threadId: target.input.threadId });
    const current = registry.get(atom);
    const observation = current.observation;
    if (observation && observation.state !== "last_observation") return;
    if (page.list && observation?.state === "last_observation" &&
      (page.list.revision !== observation.revision ||
        page.list.owner.processId !== observation.owner.processId ||
        page.list.owner.coordinatorSessionId !== observation.owner.coordinatorSessionId ||
        page.list.owner.epoch !== observation.owner.epoch)) return;
    registry.set(atom, { ...current, ...page });
  });
  const clearPages = (
    target: { environmentId: C.EnvironmentId; input: { threadId: C.ThreadId } },
    registry: AtomRegistry.AtomRegistry,
  ) => Effect.sync(() => {
    const atom = pages({ environmentId: target.environmentId, threadId: target.input.threadId });
    registry.set(atom, { ...emptyPages, observation: registry.get(atom).observation });
  });
  const privateState = (target: typeof C.WorkerTarget.Type) => privateStates(environmentRpcKey({
    environmentId: target.environmentId, input: { threadId: target.threadId },
  }));
  const view = createEnvironmentRpcSubscriptionAtomFamily(runtime, {
    label: "environment-data:workers:view", tag: C.WS_METHODS.workerOpenView, idleTtlMs: 0,
    onEnd: (input, registry) => registry.set(privateState(input), endView(registry.get(privateState(input)))),
    transform: (stream, input) => Stream.unwrap(Effect.gen(function* () {
      const registry = yield* AtomRegistry.AtomRegistry;
      const atom = privateState(input);
      let opened: OpenedView | undefined;
      yield* Effect.addFinalizer(() => Effect.sync(() => {
        if (opened && registry.get(atom).view?.viewId === opened.viewId) registry.set(atom, endView(registry.get(atom)));
      }));
      return stream.pipe(Stream.tap((event) => Effect.sync(() => {
        if (event.type === "opened") opened = event;
        registry.set(atom, workerPrivateStateForEvent(registry.get(atom), event));
      })));
    })),
  });
  const action = createEnvironmentRpcCommand(runtime, {
    label: "workers:action", tag: C.WS_METHODS.workerAction,
    onSuccess: (target, registry, outcome) => Effect.sync(() => {
      const atom = privateState({ ...target.input, environmentId: target.environmentId });
      const state = registry.get(atom);
      if (outcome.actionId === target.input.actionId && state.decisions.some(({ decision }) =>
        decision.actionId === target.input.actionId && decision.viewId === target.input.viewId)) registry.set(atom, {
        ...state, outcomes: [...state.outcomes.filter((item) => item.actionId !== outcome.actionId), outcome].slice(-32),
      });
    }),
  });
  return {
    privateState,
    pages,
    view,
    observe: createEnvironmentRpcSubscriptionAtomFamily(runtime, {
      label: "environment-data:workers:observe", tag: C.WS_METHODS.workerObserve, idleTtlMs: 0,
      onEnd: (input, registry) => registry.set(pages(input), emptyPages),
      transform: (stream, input) => Stream.unwrap(Effect.gen(function* () {
        const registry = yield* AtomRegistry.AtomRegistry;
        let previous: typeof C.WorkerObservation.Type | undefined;
        return stream.pipe(Stream.tap((observation) => Effect.sync(() => {
          if (observation.state !== "last_observation" || previous?.state !== "last_observation" ||
            observation.revision !== previous.revision ||
            observation.owner.processId !== previous.owner.processId ||
            observation.owner.coordinatorSessionId !== previous.owner.coordinatorSessionId ||
            observation.owner.epoch !== previous.owner.epoch)
            registry.set(pages(input), { ...emptyPages, observation });
          else registry.set(pages(input), { ...registry.get(pages(input)), observation });
          previous = observation;
        })));
      })),
    }),
    list: createEnvironmentRpcCommand(runtime, {
      label: "workers:list", tag: C.WS_METHODS.workerList, onFailure: clearPages,
      onSuccess: (target, registry, page) => storePage(target, registry, { list: page }),
    }),
    detail: createEnvironmentRpcCommand(runtime, {
      label: "workers:detail", tag: C.WS_METHODS.workerDetail, onFailure: clearPages,
      onSuccess: (target, registry, page) => storePage(target, registry, { detail: page }),
    }),
    requests: createEnvironmentRpcCommand(runtime, {
      label: "workers:requests", tag: C.WS_METHODS.workerRequests, onFailure: clearPages,
      onSuccess: (target, registry, page) => storePage(target, registry, { requests: page }),
    }),
    activity: createEnvironmentRpcCommand(runtime, {
      label: "workers:activity", tag: C.WS_METHODS.workerActivity, onFailure: clearPages,
      onSuccess: (target, registry, page) => storePage(target, registry, { activity: page }),
    }),
    result: createEnvironmentRpcCommand(runtime, {
      label: "workers:result", tag: C.WS_METHODS.workerResult, onFailure: clearPages,
      onSuccess: (target, registry, page) => storePage(target, registry, { result: page }),
    }),
    historyList: createEnvironmentRpcCommand(runtime, {
      label: "workers:history-list", tag: C.WS_METHODS.workerHistoryList, onFailure: clearPages,
      onSuccess: (target, registry, page) => storePage(target, registry, { historyList: page }),
    }),
    acknowledge: createEnvironmentRpcCommand(runtime, { label: "workers:acknowledge", tag: C.WS_METHODS.workerAcknowledge }),
    closeView: createEnvironmentRpcCommand(runtime, {
      label: "workers:close-view", tag: C.WS_METHODS.workerCloseView,
      onSettled: (target, registry) => Effect.sync(() => {
        const atom = privateState({ ...target.input, environmentId: target.environmentId });
        if (registry.get(atom).view?.viewId === target.input.viewId) registry.set(atom, endView(registry.get(atom)));
      }),
    }),
    controlDetail: createEnvironmentRpcCommand(runtime, {
      label: "workers:control-detail", tag: C.WS_METHODS.workerControlDetail,
      onSuccess: (target, registry, control) => Effect.sync(() => {
        const atom = privateState({ ...target.input, environmentId: target.environmentId });
        const state = registry.get(atom);
        if (state.view?.viewId === target.input.viewId) registry.set(atom, { ...state, control, selection: null });
      }),
    }),
    activate: createEnvironmentRpcCommand(runtime, {
      label: "workers:activate", tag: C.WS_METHODS.workerActivate,
      onSuccess: (target, registry) => Effect.sync(() => {
        const atom = privateState({ ...target.input, environmentId: target.environmentId });
        const state = registry.get(atom);
        if (state.view?.viewId === target.input.viewId)
          registry.set(atom, { ...state, selection: target.input.selection });
      }),
    }),
    /** Call only for an explicit new decision, never from recovery or transport lifecycle hooks. */
    createDecision: (
      registry: AtomRegistry.AtomRegistry,
      target: { environmentId: C.EnvironmentId; input: Omit<typeof C.WorkerActionInput.Type, "actionId"> },
    ): WorkerDecision => {
      const atom = privateState({ ...target.input, environmentId: target.environmentId });
      const state = registry.get(atom);
      if (state.pending) throw new C.ProviderWorkersError({ code: "busy" });
      if (state.view?.viewId !== target.input.viewId) throw new C.ProviderWorkersError({ code: "view_closed" });
      const ordinal = state.decisionOrdinal + 1;
      if (!Number.isSafeInteger(ordinal)) throw new C.ProviderWorkersError({ code: "action_limit" });
      const decision: WorkerDecision = Object.freeze({ ...target.input,
        selection: Object.freeze({ ...target.input.selection,
          target: Object.freeze({ ...target.input.selection.target }),
        }),
        operation: Object.freeze(target.input.operation.kind === "input"
          ? { ...target.input.operation, input: Object.freeze({ ...target.input.operation.input }) }
          : { ...target.input.operation }),
        actionId: C.makeWorkerActionId(state.view.viewId, ordinal, randomUUID()),
        [decisionBrand]: true as const,
      });
      const decisions = [...state.decisions, { decision, recoveryHandle: state.view.recoveryHandle }].slice(-32);
      registry.set(atom, { ...state, decisionOrdinal: ordinal,
        recovery: { viewId: state.view.viewId, recoveryHandle: state.view.recoveryHandle, owner: state.view.owner },
        decisions,
        outcomes: state.outcomes.filter((outcome) => decisions.some(({ decision }) => decision.actionId === outcome.actionId)),
      });
      return decision;
    },
    action: {
      ...action,
      run: async (
        registry: AtomRegistry.AtomRegistry,
        target: { environmentId: C.EnvironmentId; input: WorkerDecision },
      ) => {
        const atom = privateState({ ...target.input, environmentId: target.environmentId });
        const state = registry.get(atom);
        const code = state.pending ? "busy" :
          !state.decisions.some(({ decision }) => decision === target.input) ||
            state.view?.viewId !== target.input.viewId ? "invalid_action_id" : undefined;
        if (code) return AsyncResult.failure(Cause.fail(new C.ProviderWorkersError({ code })));
        registry.set(atom, { ...state, pending: true });
        try { return await action.run(registry, target); }
        finally {
          const current = registry.get(atom);
          if (current.view?.viewId === target.input.viewId) registry.set(atom, { ...current, pending: false });
        }
      },
    },
    receipt: createEnvironmentRpcCommand(runtime, {
      label: "workers:receipt", tag: C.WS_METHODS.workerReceipt,
      onSuccess: (target, registry, outcome) => Effect.sync(() => {
        const atom = privateState({ ...target.input, environmentId: target.environmentId });
        const state = registry.get(atom);
        const access = target.input.access;
        if (outcome.actionId === target.input.actionId && state.decisions.some(({ decision, recoveryHandle }) =>
          decision.actionId === target.input.actionId &&
          ("viewId" in access ? decision.viewId === access.viewId : recoveryHandle === access.recoveryHandle)))
          registry.set(atom, { ...state,
            outcomes: [...state.outcomes.filter((item) => item.actionId !== outcome.actionId), outcome].slice(-32),
          });
      }),
    }),
    historyRead: createEnvironmentRpcCommand(runtime, {
      label: "workers:history-read", tag: C.WS_METHODS.workerHistoryRead,
      onSuccess: (target, registry, page) => Effect.sync(() => {
        const atom = privateState({ ...target.input, environmentId: target.environmentId });
        const state = registry.get(atom);
        if (state.view?.viewId === target.input.viewId && state.view.grants.includes("history"))
          registry.set(atom, { ...state, history: page });
      }),
      onFailure: (target, registry) => Effect.sync(() => {
        const atom = privateState({ ...target.input, environmentId: target.environmentId });
        const state = registry.get(atom);
        if (state.view?.viewId === target.input.viewId) registry.set(atom, { ...state, history: null });
      }),
    }),
    routeRead: createEnvironmentRpcCommand(runtime, {
      label: "workers:route-read", tag: C.WS_METHODS.workerRouteRead,
      onSuccess: (target, registry, page) => Effect.sync(() => {
        const atom = privateState({ ...target.input, environmentId: target.environmentId });
        const state = registry.get(atom);
        if (state.view?.viewId === target.input.viewId && state.view.grants.includes("route"))
          registry.set(atom, { ...state, route: page });
      }),
      onFailure: (target, registry) => Effect.sync(() => {
        const atom = privateState({ ...target.input, environmentId: target.environmentId });
        const state = registry.get(atom);
        if (state.view?.viewId === target.input.viewId) registry.set(atom, { ...state, route: null });
      }),
    }),
  };
}
