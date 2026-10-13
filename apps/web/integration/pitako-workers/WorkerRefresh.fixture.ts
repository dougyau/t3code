import { expect } from "@effect/vitest";
import * as C from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import type { Atom } from "effect/reactivity";
import { act, type ReactTestRendererJSON } from "../../src/test/componentRenderer";
import type { workerPanelFixture } from "./PitakoWorkersPanel.fixture";

/** Visible text from the mounted component, not serialized props or static markup. */
export function renderedWorkerText(tree: ReactTestRendererJSON | ReactTestRendererJSON[] | string | null): string {
  if (typeof tree === "string") return tree;
  if (!tree) return "";
  if (Array.isArray(tree)) return tree.map(renderedWorkerText).join("\n");
  return (tree.children ?? []).map(renderedWorkerText).join("");
}

/** Both real renderers exercise the same native port -> service -> RPC -> atom path. */
export const workerRefreshScenario = Effect.fn(function* (
  f: Effect.Success<typeof workerPanelFixture>,
  ui: { text: () => string; click: (label: string) => Promise<void> },
) {
  const pages = f.workers.pages(f.target);
  const wait = <A>(atom: Atom.Atom<A>, predicate: (value: A) => boolean) =>
    new Promise<void>((resolve) => {
      let off = () => {};
      off = f.registry.subscribe(atom, (value) => {
        if (predicate(value)) {
          resolve();
          queueMicrotask(() => off());
        }
      }, { immediate: true });
    });
  const flush = (promise: Promise<unknown>) => Effect.promise(() => act(async () => { await promise; }));
  const notify = (revision: number) => flush(Effect.runPromise(f.notify({
    state: "last_observation", owner: f.owner, revision, observedAt: revision,
  })).then(() => wait(pages, (p) => p.observation?.state === "last_observation" && p.observation.revision === revision)));
  const first = f.data.roster.items[0]!;
  f.data.roster = { ...f.data.roster, items: [{
    ...first, status: "completed", resultTaken: false,
    handoff: { collection: "collected", delivery: "offered", bookkeeping: "blocked", continuation: "blocked-unconfirmed" },
  }] };
  f.data.detail = { ...f.data.detail, status: "completed", observedStatus: "running", result: "ready" };
  yield* Effect.promise(() => ui.click("Inspect Pitako workers"));
  yield* flush(wait(pages, (p) => p.list !== null));
  expect(f.calls.list).toBe(1);
  expect(f.calls.detail).toEqual([]);
  for (const text of ["Execution: completed", "Engine collection: collected", "Coordinator delivery: offered",
    "Bookkeeping: blocked", "Continuation: blocked-unconfirmed", "Composite resultTaken: false",
    "Result read availability: not consulted", "Passive inspection", "do not prove acceptance or verification"])
    expect(ui.text()).toContain(text);
  expect(ui.text()).not.toContain("result to collect");
  yield* Effect.promise(() => ui.click("developer · completed · agent_spawn · fixture task"));
  yield* flush(wait(pages, (p) => p.detail !== null));
  expect(ui.text()).toContain("Execution observed running; registered completed; result read availability ready");
  expect(ui.text()).toContain("Incomplete metadata section");
  const detailReads = f.calls.detail.length;
  // New native observations refresh the first roster and selected first detail without a Result press.
  f.data.roster = { ...f.data.roster, revision: 2, items: [{
    ...first, status: "completed", resultTaken: false,
    handoff: { collection: "collected", delivery: "incorporated", bookkeeping: "inapplicable", continuation: "requested" },
  }] };
  yield* notify(2);
  yield* flush(wait(pages, (p) => p.detail !== null && p.list?.revision === 2));
  expect(f.calls.detail).toHaveLength(detailReads + 1);
  expect(f.calls.detail.at(-1)).toBeUndefined();
  for (const text of ["Coordinator delivery: incorporated", "Bookkeeping: inapplicable", "Continuation: requested",
    "Composite resultTaken: false"]) expect(ui.text()).toContain(text);
  expect(ui.text()).not.toContain("Bookkeeping: reconciled");
  expect(ui.text()).not.toContain("Continuation: observed");
  const listReads = f.calls.list;
  yield* notify(2);
  expect(f.calls.list).toBe(listReads);

  // A held first read plus a burst has exactly one latest pending intent.
  const held = yield* f.holdNext("list");
  f.data.roster = { ...f.data.roster, revision: 3 };
  yield* notify(3);
  yield* Deferred.await(held.started);
  for (const revision of [4, 5, 6]) {
    f.data.roster = { ...f.data.roster, revision };
    yield* notify(revision);
  }
  expect(f.calls.list).toBe(listReads + 1);
  yield* flush(Effect.runPromise(Deferred.succeed(held.reply, undefined)));
  yield* flush(wait(pages, (p) => p.detail !== null && p.list?.revision === 6));
  expect(f.calls.list).toBe(listReads + 2);
  expect(f.calls.detail).toHaveLength(detailReads + 2);
  expect(f.registry.get(pages).list?.revision).toBe(6);

  // A current failure is explicit and does not retry until another notification or human reload.
  const failed = yield* f.holdNext("list");
  f.data.roster = { ...f.data.roster, revision: 7 };
  yield* notify(7);
  yield* Deferred.await(failed.started);
  yield* flush(Effect.runPromise(Deferred.fail(failed.reply,
    new C.ProviderWorkersError({ code: "fixture_failure" }))));
  expect(ui.text()).toContain("fixture_failure");
  expect(ui.text()).toContain("No automatic retry");
  expect(f.calls.list).toBe(listReads + 3);
  const next = yield* f.holdNext("list");
  f.data.roster = { ...f.data.roster, revision: 8, cursor: "deliberate-next-page", items: [] };
  yield* notify(8);
  yield* Deferred.await(next.started);
  yield* flush(Effect.runPromise(Deferred.succeed(next.reply, undefined)));
  yield* flush(wait(pages, (p) => p.list?.revision === 8));
  expect(ui.text()).toContain("outside current page");
  expect(ui.text()).not.toContain("Coordinator delivery: incorporated");
  expect(f.calls.detail).toHaveLength(detailReads + 2);
  // Missing optional fields stay missing, including synchronous agent_run availability.
  f.data.roster = { ...f.data.roster, revision: 9, cursor: null, items: [{ ...first, source: "agent_run" }] };
  f.data.detail = { ...f.data.detail, ref: { ...f.data.detail.ref, source: "agent_run" }, result: "unavailable" };
  yield* notify(9);
  yield* flush(wait(pages, (p) => p.detail !== null && p.list?.revision === 9));
  for (const text of ["Engine collection: not reported", "Coordinator delivery: not reported",
    "Bookkeeping: not reported", "Continuation: not reported", "Composite resultTaken: not reported",
    "result read availability unavailable"]) expect(ui.text()).toContain(text);
  expect(ui.text()).not.toContain("unsupported_kind");
  expect(f.calls.result).toEqual([]);
  expect(f.calls.requests).toBe(0);
  expect(f.calls.activity).toBe(0);
  expect(f.calls.historyList).toBe(0);
  expect(f.calls.history).toBe(0);
  expect(f.calls.route).toBe(0);
  expect(f.calls.grants).toEqual([]);
  expect(f.calls.effects).toEqual([]);
  // Human selection can replace a held passive detail, including at the same native revision.
  const selectedDetail = yield* f.holdNext("detail");
  const originalDetail = f.data.detail;
  f.data.roster = { ...f.data.roster, revision: 10, items: [
    first, { ...first, id: "other-worker", roleId: "reviewer", task: "other task" },
  ] };
  yield* notify(10);
  yield* Deferred.await(selectedDetail.started);
  f.data.detail = {
    ...originalDetail, ref: { ...originalDetail.ref, instanceId: "other-worker", source: "agent_spawn" },
    nextBrief: null, nextMetadata: null,
    workbrief: { text: "new human selection", source: "invocation task", offset: 0, nextOffset: null },
  };
  yield* Effect.promise(() => ui.click("reviewer · running · agent_spawn · other task"));
  yield* flush(wait(pages, (p) => p.detail?.ref.instanceId === "other-worker"));
  yield* flush(Effect.runPromise(Deferred.succeed(selectedDetail.reply, undefined)));
  expect(f.registry.get(pages).detail?.ref.instanceId).toBe("other-worker");
  expect(ui.text()).toContain("new human selection");
  expect(ui.text()).not.toContain("More brief");
  expect(ui.text()).not.toContain("More configuration");
  f.data.detail = { ...originalDetail, ref: { ...originalDetail.ref, source: "agent_spawn" } };
  yield* Effect.promise(() => ui.click("developer · running · agent_spawn · fixture task"));
  yield* flush(wait(pages, (p) => p.detail?.ref.instanceId === first.id));
  // Human control owns admission. A notification burst during it retains only the latest read,
  // without cancelling/retrying the decision or consulting any other passive endpoint.
  f.data.roster = { ...f.data.roster, revision: 11, items: [first] };
  f.data.detail = { ...f.data.detail, ref: { ...f.data.detail.ref, source: "agent_spawn" },
    status: "running", observedStatus: "running", result: "pending" };
  yield* notify(11);
  yield* flush(wait(pages, (p) => p.detail !== null && p.list?.revision === 11));
  const privateState = f.workers.privateState(f.target);
  yield* Effect.promise(() => ui.click("Inspect control target"));
  yield* flush(wait(privateState, (s) => s.control !== null));
  yield* Effect.promise(() => ui.click("Activate this exact control target"));
  yield* flush(wait(privateState, (s) => s.selection !== null));
  const action = yield* f.holdNext("action");
  yield* Effect.promise(() => ui.click("Request cancellation"));
  yield* Effect.promise(() => ui.click("Confirm cancellation request"));
  yield* Deferred.await(action.started);
  const beforeActionReads = f.calls.list;
  for (const revision of [12, 13, 14]) {
    f.data.roster = { ...f.data.roster, revision };
    yield* notify(revision);
  }
  expect(f.calls.list).toBe(beforeActionReads);
  expect(f.calls.effects).toEqual(["cancel"]);
  yield* flush(Effect.runPromise(Deferred.succeed(action.reply, undefined)));
  yield* flush(wait(pages, (p) => p.detail !== null && p.list?.revision === 14));
  expect(f.calls.list).toBe(beforeActionReads + 1);
  expect(f.calls.effects).toEqual(["cancel"]);
  expect(f.calls.receipts).toEqual([]);
  expect(f.calls.result).toEqual([]);
  // Loss and reconnect discard operative pages, not receipts, and only admit fresh passive reads.
  yield* flush(Effect.runPromise(f.notify({ state: "runtime_absent" }))
    .then(() => wait(pages, (p) => p.observation?.state === "runtime_absent")));
  expect(f.registry.get(pages).detail).toBeNull();
  expect(ui.text()).not.toContain("result read availability unavailable");
  f.data.roster = { ...f.data.roster, revision: 15 };
  yield* notify(15);
  yield* flush(wait(pages, (p) => p.detail !== null && p.list?.revision === 15));
  expect(f.calls.effects).toEqual(["cancel"]);
  expect(f.calls.result).toEqual([]);
  // Late detail success and failure cannot revive old local cursors or clear the newer view.
  const detailWithCursors = f.data.detail;
  let revision = 15;
  for (const fail of [false, true]) {
    const heldDetail = yield* f.holdNext("detail");
    f.data.detail = detailWithCursors;
    f.data.roster = { ...f.data.roster, revision: ++revision };
    yield* notify(revision);
    yield* Deferred.await(heldDetail.started);
    f.data.detail = {
      ...detailWithCursors, nextBrief: null, nextMetadata: null,
      workbrief: { text: "current complete brief", source: "invocation task", offset: 0, nextOffset: null },
      metadata: { text: "{}", offset: 0, nextOffset: null },
    };
    f.data.roster = { ...f.data.roster, revision: ++revision };
    yield* notify(revision);
    const result = fail
      ? Deferred.fail(heldDetail.reply, new C.ProviderWorkersError({ code: "stale_detail_failure" }))
      : Deferred.succeed(heldDetail.reply, undefined);
    yield* flush(Effect.runPromise(result));
    yield* flush(wait(pages, (p) => p.detail !== null && p.list?.revision === revision));
    expect(ui.text()).toContain("current complete brief");
    expect(ui.text()).not.toContain("stale_detail_failure");
    expect(ui.text()).not.toContain("More brief");
    expect(ui.text()).not.toContain("More configuration");
    expect(f.calls.result).toEqual([]);
  }
  const closedReads = f.calls.list;
  yield* Effect.promise(() => ui.click("Close Pitako inspection"));
  yield* flush(wait(f.workers.privateState(f.target), (p) => p.view === null));
  yield* flush(Effect.runPromise(f.notify({ state: "last_observation", owner: f.owner, revision: revision + 1, observedAt: revision + 1 })));
  expect(f.calls.list).toBe(closedReads);
});
