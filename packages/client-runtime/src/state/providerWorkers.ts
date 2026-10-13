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

const sameOwner = (left: typeof C.WorkerOwner.Type, right: typeof C.WorkerOwner.Type) =>
  left.processId === right.processId &&
  left.coordinatorSessionId === right.coordinatorSessionId && left.epoch === right.epoch;

/** Native revisions are meaningful only within the complete owner identity. */
export const workerObservationKey = (observation: typeof C.WorkerObservation.Type | null) =>
  observation?.state === "last_observation"
    ? JSON.stringify([observation.owner.processId, observation.owner.coordinatorSessionId,
        observation.owner.epoch, observation.revision])
    : null;

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
  // Request fences live with the registry, not idle atoms that can reset during an RPC.
  const tickets = new WeakMap<AtomRegistry.AtomRegistry, Map<Atom.Atom<WorkerPages>, Map<string, number>>>();
  const invalidate = (
    target: typeof C.WorkerTarget.Type,
    registry: AtomRegistry.AtomRegistry,
    keepRoster = false,
  ) => {
    const atom = pages(target);
    const current = registry.get(atom);
    const pending = tickets.get(registry)?.get(atom);
    pending?.forEach((ticket, field) => { pending.set(field, ticket + 1); });
    registry.set(atom, { ...emptyPages, observation: current.observation,
      list: keepRoster ? current.list : null });
  };
  // Fence command settlement too: a late failure must not clear newer pages.
  const read = <K extends Exclude<keyof WorkerPages, "observation">,
    I extends { threadId: C.ThreadId }, E>(
    field: K,
    command: {
      readonly run: (registry: AtomRegistry.AtomRegistry,
        target: { environmentId: C.EnvironmentId; input: I }) =>
        Promise<AsyncResult.AsyncResult<NonNullable<WorkerPages[K]>, E>>;
      readonly permissionAtom: (id: C.EnvironmentId, input?: unknown) => Atom.Atom<boolean>;
      readonly label: string;
    },
    accepts: (page: NonNullable<WorkerPages[K]>, current: WorkerPages, input: I) => boolean = () => true,
  ) => ({
    ...command,
    run: async (registry: AtomRegistry.AtomRegistry,
      request: { environmentId: C.EnvironmentId; input: I }) => {
      const target = { environmentId: request.environmentId, threadId: request.input.threadId };
      const atom = pages(target);
      const key = workerObservationKey(registry.get(atom).observation);
      const detailRef = JSON.stringify(registry.get(atom).detail?.ref);
      let byPage = tickets.get(registry);
      if (!byPage) {
        byPage = new Map();
        tickets.set(registry, byPage);
      }
      let byField = byPage.get(atom);
      if (!byField) {
        byField = new Map();
        byPage.set(atom, byField);
      }
      const ticket = (byField.get(field) ?? 0) + 1;
      byField.set(field, ticket);
      const result = await command.run(registry, request);
      const current = registry.get(atom);
      if (byField.get(field) !== ticket ||
        key !== workerObservationKey(current.observation) ||
        (field !== "list" && detailRef !== JSON.stringify(current.detail?.ref)) ||
        (current.observation && current.observation.state !== "last_observation")) return result;
      if (AsyncResult.isFailure(result))
        registry.set(atom, { ...emptyPages, observation: current.observation,
          list: field === "list" ? null : current.list });
      else if (AsyncResult.isSuccess(result) && accepts(result.value, current, request.input))
        registry.set(atom, {
          ...(field === "list" ? { ...emptyPages, observation: current.observation } : current),
          [field]: result.value,
        });
      return result;
    },
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
    invalidate,
    view,
    observe: createEnvironmentRpcSubscriptionAtomFamily(runtime, {
      label: "environment-data:workers:observe", tag: C.WS_METHODS.workerObserve, idleTtlMs: 0,
      onEnd: (input, registry) => {
        invalidate(input, registry);
        registry.set(pages(input), emptyPages);
      },
      transform: (stream, input) => Stream.unwrap(Effect.gen(function* () {
        const registry = yield* AtomRegistry.AtomRegistry;
        let previous: typeof C.WorkerObservation.Type | undefined;
        return stream.pipe(Stream.tap((observation) => Effect.sync(() => {
          if (observation.state !== "last_observation" || previous?.state !== "last_observation" ||
            observation.revision !== previous.revision ||
            !sameOwner(observation.owner, previous.owner))
            {
              invalidate(input, registry);
              registry.set(pages(input), { ...emptyPages, observation });
            }
          else registry.set(pages(input), { ...registry.get(pages(input)), observation });
          previous = observation;
        })));
      })),
    }),
    list: read("list", createEnvironmentRpcCommand(runtime, {
      label: "workers:list", tag: C.WS_METHODS.workerList,
    }), (page, current) => !current.observation ||
      (current.observation.state === "last_observation" && current.observation.revision === page.revision &&
        sameOwner(current.observation.owner, page.owner))),
    detail: read("detail", createEnvironmentRpcCommand(runtime, {
      label: "workers:detail", tag: C.WS_METHODS.workerDetail,
    }), (page, current, input) => page.ref.instanceId === input.id &&
      (!current.list || current.list.items.some((row) => row.id === page.ref.instanceId &&
        (!row.source || row.source === page.ref.source) &&
        (!row.assignmentId || row.assignmentId === page.ref.assignmentId) &&
        (!row.activated?.attemptId || !page.ref.attemptId || row.activated.attemptId === page.ref.attemptId))) &&
      (!current.observation ||
        (current.observation.state === "last_observation" && sameOwner(current.observation.owner, page.ref)))),
    requests: read("requests", createEnvironmentRpcCommand(runtime, {
      label: "workers:requests", tag: C.WS_METHODS.workerRequests,
    })),
    activity: read("activity", createEnvironmentRpcCommand(runtime, {
      label: "workers:activity", tag: C.WS_METHODS.workerActivity,
    })),
    result: read("result", createEnvironmentRpcCommand(runtime, {
      label: "workers:result", tag: C.WS_METHODS.workerResult,
    })),
    historyList: read("historyList", createEnvironmentRpcCommand(runtime, {
      label: "workers:history-list", tag: C.WS_METHODS.workerHistoryList,
    })),
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
