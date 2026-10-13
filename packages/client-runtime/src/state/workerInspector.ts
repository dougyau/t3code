import * as C from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import { Atom, AtomRegistry, AsyncResult } from "effect/reactivity";
import { workerObservationKey, type createProviderWorkersEnvironmentAtoms, type WorkerDecision } from "./providerWorkers.ts";

type Workers = ReturnType<typeof createProviderWorkersEnvironmentAtoms>;
type Button = {
  readonly label: string;
  readonly disabled: boolean;
  readonly press: () => Promise<void>;
};
type Section = { readonly title: string; readonly text: string };
const json = (value: unknown) => JSON.stringify(value, null, 2);

/** Separate observations, never a computed success/acceptance state. */
export function workerHandoffSections(row: (typeof C.WorkerRoster.Type)["items"][number]): Section[] {
  return [
    { title: "Engine collection", text: row.handoff?.collection ?? "not reported" },
    { title: "Coordinator delivery", text: row.handoff?.delivery ?? "not reported" },
    { title: "Bookkeeping", text: row.handoff?.bookkeeping ?? "not reported" },
    { title: "Continuation", text: row.handoff?.continuation ?? "not reported" },
    { title: "Composite resultTaken", text: row.resultTaken === undefined ? "not reported" : String(row.resultTaken) },
  ];
}

/** Parse metadata only when this page contains the entire section. Never join different observations. */
export function workerTextSection(
  title: string,
  page: typeof C.WorkerTextPage.Type,
  metadata = false,
): Section {
  let text = page.text;
  let diagnostic = "";
  if (metadata && page.offset === 0 && page.nextOffset === null) {
    try {
      text = json(JSON.parse(page.text));
    } catch {
      diagnostic = "Malformed metadata. Original text follows.\n";
    }
  } else if (metadata) diagnostic = "Incomplete metadata section. Original fragment follows.\n";
  return { title: `${title} · offset ${page.offset}`, text: diagnostic + text };
}

/** The original base64 is always shown, including when decoding fails or an entry is incomplete. */
export function workerHistorySection(
  item: (typeof C.WorkerHistoryRead.Type)["items"][number],
): Section {
  let readable: string;
  try {
    const bytes = Uint8Array.from(atob(item.data), (char) => char.charCodeAt(0));
    readable = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  } catch {
    readable = "Malformed or incomplete UTF-8 bytes. Use the original base64 below.";
  }
  return {
    title: `History ${item.historyId} · entry ${item.entryOffset} · byte ${item.byteOffset}`,
    text: `${item.validation}; ${item.endsEntry ? "entry ends here" : "incomplete entry"}\n${readable}\nOriginal base64:\n${item.data}`,
  };
}

/**
 * One mounted inspector, shared by web and mobile. Renderers invoke buttons only on human presses.
 * Credentials stay in the worker atoms; neither the render model nor error text exposes them.
 */
export function createWorkerInspector(
  registry: AtomRegistry.AtomRegistry,
  workers: Workers,
  target: typeof C.WorkerTarget.Type,
) {
  const initial = {
    id: "",
    text: "",
    cancelConfirmation: false,
    error: "",
    busy: false,
    busyDecision: null as WorkerDecision | null,
    briefCursor: null as typeof C.WorkerCursor.Type | null,
    configurationCursor: null as typeof C.WorkerCursor.Type | null,
    resultTextCursor: null as typeof C.WorkerCursor.Type | null,
    resultMetadataCursor: null as typeof C.WorkerCursor.Type | null,
  };
  const local = Atom.make(initial);
  const update = (patch: Partial<typeof initial>) =>
    registry.set(local, { ...registry.get(local), ...patch });
  const viewAtom = workers.view({ environmentId: target.environmentId, input: target });
  const observationAtom = workers.observe({ environmentId: target.environmentId, input: target });
  const privateAtom = workers.privateState(target);
  const pageAtom = workers.pages(target);
  let releaseViewWait: (() => void) | undefined;
  let mounted = false;
  let generation = 0;
  let invocation = 0;
  let refreshing = false;
  let pendingRefresh: string | null = null;
  const captured = () => ({ generation, key: workerObservationKey(registry.get(pageAtom).observation) });
  const current = (stamp: ReturnType<typeof captured>) =>
    stamp.generation === generation && stamp.key === workerObservationKey(registry.get(pageAtom).observation);
  const clearCursors = () => update({
    briefCursor: null, configurationCursor: null, resultTextCursor: null, resultMetadataCursor: null,
  });
  function reportFailure<A, E>(result: AsyncResult.AsyncResult<A, E>) {
    if (!AsyncResult.isFailure(result)) return;
    const failure = Cause.findError(result.cause);
    const code = failure._tag === "Success" && typeof failure.success === "object" &&
      failure.success !== null && "code" in failure.success ? String(failure.success.code) : "request_failed";
    update({ error: `${code}. No automatic retry. Restart a stale traversal deliberately.` });
  }
  async function drainRefresh() {
    if (!mounted || refreshing || !pendingRefresh || registry.get(local).busy || registry.get(privateAtom).pending) return;
    const intent = pendingRefresh;
    pendingRefresh = null;
    if (intent !== workerObservationKey(registry.get(pageAtom).observation)) return;
    const stamp = captured();
    refreshing = true;
    update({ error: "" });
    try {
      const roster = await workers.list.run(registry, { environmentId: target.environmentId, input: target });
      if (!mounted || !current(stamp)) return;
      reportFailure(roster);
      const id = registry.get(local).id;
      if (AsyncResult.isSuccess(roster) && registry.get(pageAtom).list === roster.value &&
        roster.value.items.some((row) => row.id === id)) {
        if (registry.get(local).busy || registry.get(privateAtom).pending) {
          pendingRefresh = intent;
          return;
        }
        const detailStamp = captured();
        const result = await detailRead.run(registry, { environmentId: target.environmentId, input: { ...target, id } });
        if (mounted && current(detailStamp)) reportFailure(result);
      }
    } catch {
      if (mounted && current(stamp)) update({ error: "request_failed. No automatic retry." });
    } finally {
      refreshing = false;
      void drainRefresh();
    }
  }
  // Commands still check destination permissions at execution; UI availability is not authority.
  async function invoke<A, E>(run: () => Promise<AsyncResult.AsyncResult<A, E>>) {
    if (registry.get(local).busy) return;
    if (refreshing) {
      generation++;
      workers.invalidate(target, registry, true);
      clearCursors();
      pendingRefresh = workerObservationKey(registry.get(pageAtom).observation);
    }
    const stamp = captured();
    const request = ++invocation;
    update({ busy: true, error: "" });
    try {
      const result = await run();
      if (!current(stamp)) return;
      if (AsyncResult.isFailure(result)) {
        const failure = Cause.findError(result.cause);
        const code =
          failure._tag === "Success" &&
          typeof failure.success === "object" &&
          failure.success !== null &&
          "code" in failure.success
            ? String(failure.success.code)
            : "request_failed";
        update({ error: `${code}. No automatic retry. Restart a stale traversal deliberately.` });
        const admitted = failure._tag === "Success" && typeof failure.success === "object" &&
          failure.success !== null && "actionAdmitted" in failure.success && failure.success.actionAdmitted === true;
        if (!admitted && (code === "invalid_cursor" || code === "stale_cursor" || code === "revision_mismatch")) {
          update({
            briefCursor: null,
            configurationCursor: null,
            resultTextCursor: null,
            resultMetadataCursor: null,
          });
          registry.set(pageAtom, {
            ...registry.get(pageAtom),
            detail: null,
            requests: null,
            activity: null,
            result: null,
            historyList: null,
          });
          const state = registry.get(privateAtom);
          registry.set(privateAtom, { ...state, history: null, route: null });
        }
      }
    } catch {
      if (current(stamp)) update({ error: "request_failed. No automatic retry or resend." });
    } finally {
      // Revision changes fence data/errors, but the admitted human action still owns busy.
      if (mounted && request === invocation) update({ busy: false });
      void drainRefresh();
    }
  }
  const detailRead = {
    ...workers.detail,
    run: async (
      registry: AtomRegistry.AtomRegistry,
      args: Parameters<Workers["detail"]["run"]>[1],
    ) => {
      const stamp = captured();
      const result = await workers.detail.run(registry, args);
      if (mounted && current(stamp) && AsyncResult.isSuccess(result) && registry.get(pageAtom).detail === result.value) {
        const cursor = args.input.cursor;
        update({
          ...(!cursor || ("section" in cursor && cursor.section === "brief")
            ? { briefCursor: result.value.nextBrief ?? null }
            : {}),
          ...(!cursor || ("section" in cursor && cursor.section === "detail_metadata")
            ? { configurationCursor: result.value.nextMetadata ?? null }
            : {}),
        });
      }
      return result;
    },
  };
  const resultRead = {
    ...workers.result,
    run: async (
      registry: AtomRegistry.AtomRegistry,
      args: Parameters<Workers["result"]["run"]>[1],
    ) => {
      const stamp = captured();
      const result = await workers.result.run(registry, args);
      if (mounted && current(stamp) && AsyncResult.isSuccess(result) && registry.get(pageAtom).result === result.value) {
        const cursor = args.input.cursor;
        const page = result.value;
        update({
          ...(!cursor || ("section" in cursor && cursor.section === "result_text")
            ? { resultTextCursor: page.state === "ready" ? (page.nextText ?? null) : null }
            : {}),
          ...(!cursor || ("section" in cursor && cursor.section === "result_metadata")
            ? { resultMetadataCursor: page.state === "ready" ? (page.nextMetadata ?? null) : null }
            : {}),
        });
      }
      return result;
    },
  };
  async function revoke() {
    const state = registry.get(privateAtom);
    // Clear rendered sensitive data immediately, including while the close request is in flight.
    registry.set(privateAtom, {
      ...state,
      history: null,
      route: null,
      control: null,
      selection: null,
    });
    update({ text: "", cancelConfirmation: false });
    if (state.view)
      await invoke(() =>
        workers.closeView.run(registry, {
          environmentId: target.environmentId,
          input: { ...target, viewId: state.view!.viewId },
        }),
      );
    await new Promise<void>((resolve) => {
      const previousView = state.view?.viewId;
      let off = () => {};
      const finish = () => {
        off();
        releaseViewWait = undefined;
        resolve();
      };
      releaseViewWait = finish;
      off = registry.subscribe(viewAtom, (value) => {
        if (
          AsyncResult.isFailure(value) ||
          (AsyncResult.isSuccess(value) &&
            !value.waiting &&
            registry.get(privateAtom).view !== null &&
            registry.get(privateAtom).view?.viewId !== previousView)
        ) {
          finish();
        }
      });
      registry.refresh(viewAtom);
    });
  }
  const model = Atom.make((get) => {
    const ui = get(local);
    const pages = get(pageAtom);
    const state = get(privateAtom);
    const view = state.view;
    const viewInput = view ? { ...target, viewId: view.viewId } : null;
    const row = pages.list?.items.find((item) => item.id === ui.id);
    const detail = row && pages.detail?.ref.instanceId === ui.id ? pages.detail : null;
    const buttons: Button[] = [];
    const sections: Section[] = [];
    const add = <I, A, E>(
      label: string,
      command: {
        permissionAtom: (id: C.EnvironmentId, input?: unknown) => Atom.Atom<boolean>;
        run: (
          registry: AtomRegistry.AtomRegistry,
          value: { environmentId: C.EnvironmentId; input: I },
        ) => Promise<AsyncResult.AsyncResult<A, E>>;
      },
      input: I,
      disabled = false,
    ) =>
      buttons.push({
        label,
        disabled: disabled || ui.busy || !get(command.permissionAtom(target.environmentId, input)),
        press: () =>
          invoke(() => command.run(registry, { environmentId: target.environmentId, input })),
      });
    const observed = pages.observation;
    const status =
      observed?.state === "last_observation"
        ? `Last observation ${DateTime.formatIso(DateTime.makeUnsafe(observed.observedAt))} · revision ${observed.revision}. Not a live-connection promise.`
        : observed?.state === "error"
          ? `error: ${observed.code}`
          : (observed?.state ?? "connecting");
    const ready = observed?.state === "last_observation";
    add("Load roster", workers.list, target, !ready);
    if (pages.list?.cursor)
      add(
        "More workers",
        workers.list,
        {
          ...target,
          after: pages.list.cursor,
          revision: pages.list.revision,
        },
        !ready,
      );
    const rows = (pages.list?.items ?? []).map((row) => ({
      id: row.id,
      label: `${row.roleId} · ${row.status} · ${row.source ?? "worker"} · ${row.task}`,
      sections: [{ title: "Execution", text: row.status },
        { title: "Result read availability", text: "not consulted" }, ...workerHandoffSections(row)],
      select: async () => {
        if (ui.busy) return;
        generation++;
        workers.invalidate(target, registry, true);
        update({
          id: "",
          text: "",
          cancelConfirmation: false,
          briefCursor: null,
          configurationCursor: null,
          resultTextCursor: null,
          resultMetadataCursor: null,
        });
        const privateState = registry.get(privateAtom);
        const stamp = captured();
        if (
          privateState.history ||
          privateState.route ||
          privateState.control ||
          privateState.selection ||
          privateState.view?.grants.length
        )
          await revoke();
        if (!mounted || !current(stamp)) return;
        update({ id: row.id });
        await invoke(() =>
          detailRead.run(registry, {
            environmentId: target.environmentId,
            input: { ...target, id: row.id },
          }),
        );
      },
    }));
    if (row)
      sections.push({
        title: "Chosen, activated, and observed requests",
        text: json({
          selectedModel: row.selectedModel,
          requestedModel: row.requestedModel,
          requestedReasoning: row.requestedReasoning,
          activated: row.activated,
          sdkRequest: row.sdkRequest,
          returned_service_tier: "unavailable",
        }),
      });
    if (ui.id && !row) sections.push({
      title: "Selection", text: "Selected worker is outside current page. Load more workers deliberately.",
    });
    if (row) {
      sections.push(...workerHandoffSections(row));
      add("Reload detail", detailRead, { ...target, id: ui.id }, !ready);
    }
    if (detail) {
      sections.push({
        title: "Worker state",
        text: `Execution observed ${detail.observedStatus}; registered ${detail.status}; result read availability ${detail.result}. ${detail.controls.reason ?? ""} Detail is a separate observation from roster revision ${pages.list?.revision}.`,
      });
      if (detail.workbrief)
        sections.push(workerTextSection(detail.workbrief.source, detail.workbrief));
      if (detail.metadata) sections.push(workerTextSection("Configuration", detail.metadata, true));
      if (ui.briefCursor)
        add("More brief", detailRead, { ...target, id: ui.id, cursor: ui.briefCursor }, !ready);
      if (ui.configurationCursor)
        add(
          "More configuration",
          detailRead,
          { ...target, id: ui.id, cursor: ui.configurationCursor },
          !ready,
        );
      add("Observed requests", workers.requests, { ...target, id: ui.id }, !ready);
      add("Activity", workers.activity, { ...target, ref: detail.ref }, !ready);
      add("Result", resultRead, { ...target, ref: detail.ref }, !ready);
      add("History metadata", workers.historyList, { ...target, workerId: ui.id }, !ready);
      if (viewInput)
        add("Inspect control target", workers.controlDetail, { ...viewInput, id: ui.id }, !ready);
    }
    if (pages.requests && detail) {
      sections.push(workerTextSection("Observed requests", pages.requests.page, true));
      if (pages.requests.next)
        add(
          "More requests",
          workers.requests,
          { ...target, id: ui.id, cursor: pages.requests.next },
          !ready,
        );
    }
    if (pages.activity && detail) {
      const activity = pages.activity;
      sections.push({
        title: "Activity",
        text: activity.state === "ready" ? json(activity.page) : json(activity),
      });
      if (activity.state === "ready" && activity.page.more)
        add(
          "More activity",
          workers.activity,
          { ...target, ref: detail.ref, cursor: activity.next },
          !ready,
        );
    }
    if (pages.result && detail) {
      const result = pages.result;
      if (result.state !== "ready") sections.push({ title: "Result", text: json(result) });
      else {
        sections.push({
          title: "Result readiness",
          text: `ready · ${result.status}. Cancellation requested is not settlement or disposal.`,
        });
        if (result.text) sections.push(workerTextSection("Result text", result.text));
        if (result.metadata)
          sections.push(workerTextSection("Result metadata", result.metadata, true));
        if (ui.resultTextCursor)
          add(
            "More result text",
            resultRead,
            { ...target, ref: detail.ref, cursor: ui.resultTextCursor },
            !ready,
          );
        if (ui.resultMetadataCursor)
          add(
            "More result metadata",
            resultRead,
            { ...target, ref: detail.ref, cursor: ui.resultMetadataCursor },
            !ready,
          );
      }
    }
    if (pages.historyList && detail) {
      sections.push({ title: "History metadata and disposal", text: json(pages.historyList) });
      if (pages.historyList.cursor)
        add(
          "More history metadata",
          workers.historyList,
          {
            ...target,
            workerId: ui.id,
            cursor: pages.historyList.cursor,
          },
          !ready,
        );
      if (viewInput && view?.grants.includes("history")) {
        for (const item of pages.historyList.items)
          add(
            `Read history ${item.historyId}`,
            workers.historyRead,
            {
              ...viewInput,
              workerId: ui.id,
              historyId: item.historyId,
              groupId: item.groupId,
            },
            !ready,
          );
      }
    }
    if (viewInput && ui.id) {
      add(
        "Acknowledge private history",
        workers.acknowledge,
        { ...viewInput, grant: "history", acknowledged: true },
        !ready || view?.grants.includes("history"),
      );
      add(
        "Acknowledge private routing",
        workers.acknowledge,
        { ...viewInput, grant: "route", acknowledged: true },
        !ready || view?.grants.includes("route"),
      );
      if (view?.grants.includes("route"))
        add("Read routing", workers.routeRead, { ...viewInput, id: ui.id }, !ready);
      buttons.push({ label: "Revoke private access", disabled: ui.busy, press: revoke });
    }
    if (state.history && view?.grants.includes("history")) {
      sections.push(...state.history.items.map(workerHistorySection));
      sections.push({ title: "History diagnostics", text: json(state.history.diagnostics) });
      const item = state.history.items[0];
      const meta = pages.historyList?.items.find((entry) => entry.historyId === item?.historyId);
      if (state.history.cursor && item && viewInput)
        add(
          "More raw history",
          workers.historyRead,
          {
            ...viewInput,
            workerId: ui.id,
            historyId: item.historyId,
            ...(meta ? { groupId: meta.groupId } : {}),
            cursor: state.history.cursor,
          },
          !ready,
        );
    }
    if (state.route && view?.grants.includes("route")) {
      sections.push(workerTextSection(`Routing ${state.route.state}`, state.route.page));
      if (state.route.page.nextOffset !== null && viewInput)
        add(
          "More routing",
          workers.routeRead,
          {
            ...viewInput,
            id: ui.id,
            offset: state.route.page.nextOffset,
          },
          !ready,
        );
    }
    const control = state.control?.ref.instanceId === ui.id ? state.control.controls : null;
    const selection = control?.selection;
    if (control)
      sections.push({
        title: "Control eligibility",
        text: `${control.reason ?? "Explicit activation required"}. Effective input policy: ${control.runtimeMode ?? "unavailable"}. Synchronous agent_run targets are not controllable.`,
      });
    if (selection && viewInput)
      add(
        "Activate this exact control target",
        workers.activate,
        {
          ...viewInput,
          selection,
          acknowledged: true,
        },
        !ready || state.selection !== null,
      );
    const activated = selection && state.selection && json(selection) === json(state.selection);
    const inputEnabled = Boolean(
      ready &&
      activated &&
      control?.runtimeMode === "full-access" &&
      get(workers.action.permissionAtom(target.environmentId)) &&
      !ui.busy &&
      !state.pending,
    );
    function decide(operation: WorkerDecision["operation"]) {
      if (!viewInput || !selection || !activated) return Promise.resolve();
      return invoke(() => {
        // One immutable decision per deliberate submission, never from an effect or recovery hook.
        const decision = workers.createDecision(registry, {
          environmentId: target.environmentId,
          input: { ...viewInput, selection, operation },
        });
        update({ text: "", cancelConfirmation: false, busyDecision: null });
        return submit(decision);
      });
    }
    async function submit(decision: WorkerDecision) {
      const stamp = captured();
      const result = await workers.action.run(registry, {
        environmentId: target.environmentId,
        input: decision,
      });
      if (!current(stamp)) return result;
      if (AsyncResult.isFailure(result)) {
        const failure = Cause.findError(result.cause);
        if (
          failure._tag === "Success" &&
          typeof failure.success === "object" &&
          failure.success !== null &&
          "code" in failure.success &&
          failure.success.code === "busy" &&
          !("actionAdmitted" in failure.success && failure.success.actionAdmitted === true)
        )
          update({ busyDecision: decision });
        else update({ busyDecision: null });
      } else update({ busyDecision: null });
      return result;
    }
    for (const intent of ["query", "steer"] as const)
      buttons.push({
        label: intent === "query" ? "Submit query" : "Submit steer",
        disabled: !inputEnabled || ui.text.trim().length === 0,
        press: () => decide({ kind: "input", input: { intent, text: ui.text } }),
      });
    buttons.push({
      label: ui.cancelConfirmation ? "Confirm cancellation request" : "Request cancellation",
      disabled:
        !ready ||
        !activated ||
        ui.busy ||
        state.pending ||
        !get(workers.action.permissionAtom(target.environmentId)),
      press: async () => {
        if (!ui.cancelConfirmation) update({ cancelConfirmation: true });
        else await decide({ kind: "cancel" });
      },
    });
    if (ui.cancelConfirmation) {
      sections.push({
        title: "Confirm cancellation",
        text: "Request cancellation of this exact worker? Requested does not mean settled, disposed, or result-ready.",
      });
      buttons.push({
        label: "Keep worker running",
        disabled: false,
        press: async () => update({ cancelConfirmation: false }),
      });
    }
    for (const { decision, recoveryHandle } of state.decisions) {
      const outcome = state.outcomes.find((entry) => entry.actionId === decision.actionId);
      sections.push({
        title: `Receipt ${decision.actionId}`,
        text: outcome
          ? json(outcome.status)
          : "No receipt observed. Lookup only; unknown is not permission to retry.",
      });
      add(`Lookup receipt ${decision.actionId}`, workers.receipt, {
        ...target,
        actionId: decision.actionId,
        // Always the credential paired with this decision, including after replacement + new action.
        access: { recoveryHandle },
      });
      if (ui.busyDecision === decision)
        buttons.push({
          label: "Submit same locally busy decision",
          disabled:
            ui.busy ||
            state.pending ||
            !ready ||
            !activated ||
            decision.viewId !== view?.viewId ||
            state.decisions.at(-1)?.decision !== decision ||
            !get(workers.action.permissionAtom(target.environmentId)),
          press: () => invoke(() => submit(decision)),
        });
    }
    return {
      status,
      error: ui.error,
      busy: ui.busy,
      rows,
      buttons,
      sections,
      text: ui.text,
      inputEnabled,
      setText: (text: string) => update({ text: text.slice(0, 8192) }),
      notice:
        "Passive inspection. Delivery and continuation do not prove acceptance or verification. Result is a deliberate read, not collection or acknowledgement. Private access is acknowledged separately for history and routing. Controls need explicit activation. Query and steer require effective full-access. Closing this view does not cancel workers. Each section has its own cursor; separately requested pages are not an immutable snapshot. Receipts are not useful answers. Receipt availability is bounded: 16 server records per owner, 32 mappings per lease, five-minute disconnected handles, and native retention. Unknown does not authorize resend.",
    };
  });
  return {
    model,
    viewAtom,
    observationAtom,
    mount: () => {
      mounted = true;
      let previousKey: string | null = null;
      let previousList = registry.get(pageAtom).list;
      const offPages = registry.subscribe(pageAtom, (pages) => {
        if (pages.list !== previousList) {
          previousList = pages.list;
          clearCursors();
        }
        const key = workerObservationKey(pages.observation);
        if (key === previousKey) return;
        previousKey = key;
        generation++;
        clearCursors();
        update({ error: "", text: "", cancelConfirmation: false, busyDecision: null });
        pendingRefresh = key;
        void drainRefresh();
      }, { immediate: true });
      let previous = registry.get(privateAtom).view;
      const offPrivate = registry.subscribe(privateAtom, (state) => {
        if (previous && state.view?.viewId !== previous.viewId)
          update({
            text: "",
            cancelConfirmation: false,
            error: "",
            busyDecision: null,
            briefCursor: null,
            configurationCursor: null,
            resultTextCursor: null,
            resultMetadataCursor: null,
          });
        previous = state.view;
        void drainRefresh();
      });
      return () => {
        mounted = false;
        generation++;
        invocation++;
        pendingRefresh = null;
        offPages();
        offPrivate();
        workers.invalidate(target, registry);
        clearCursors();
        update({ busy: false });
      };
    },
    clearLocal: () =>
      update({
        id: "",
        text: "",
        cancelConfirmation: false,
        error: "",
        busyDecision: null,
        briefCursor: null,
        configurationCursor: null,
        resultTextCursor: null,
        resultMetadataCursor: null,
      }),
    close: async () => {
      releaseViewWait?.();
      const state = registry.get(privateAtom);
      if (state.view)
        await workers.closeView.run(registry, {
          environmentId: target.environmentId,
          input: { ...target, viewId: state.view.viewId },
        });
    },
  };
}
