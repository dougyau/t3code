import { RegistryContext } from "@effect/atom-react";
import * as C from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import { Atom, AsyncResult } from "effect/reactivity";
import { act, create, type ReactTestRenderer } from "../../src/test/componentRenderer";
import { describe, expect, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import type { createProviderWorkersEnvironmentAtoms } from "@t3tools/client-runtime/state/provider-workers";
import { workerPanelFixture } from "./PitakoWorkersPanel.fixture";
import { workerRefreshScenario, renderedWorkerText } from "./WorkerRefresh.fixture";

const state = vi.hoisted(() => ({
  workers: null as ReturnType<typeof createProviderWorkersEnvironmentAtoms> | null,
}));
vi.mock("../../src/state/provider-workers", () => ({
  get providerWorkers() {
    return state.workers;
  },
}));
vi.mock("../../src/components/ui/button", () => ({ Button: "button" }));
vi.mock("../../src/components/ui/textarea", () => ({ Textarea: "textarea" }));
vi.mock("@tanstack/react-router", () => ({ useNavigate: () => vi.fn() }));
vi.mock("../../src/state/entities", () => ({
  useThreadProjection: () => null,
  useThreadShells: () => [],
  useProjects: () => [],
  useServerConfigs: () => new Map(),
}));
vi.mock("../../src/lib/archivedThreadsState", () => ({
  useArchivedThreadSnapshots: () => ({ snapshots: [] }),
}));
vi.mock("../../src/state/use-atom-command", () => ({ useAtomCommand: () => vi.fn() }));
const sessions = Atom.family((_id: C.EnvironmentId) =>
  Atom.make(
    AsyncResult.success<C.AuthSessionState>({
      authenticated: true,
      scopes: [
        C.AuthOrchestrationReadScope,
        C.AuthDiagnosticsReadScope,
        C.AuthOrchestrationOperateScope,
      ],
      auth: {
        policy: "remote-reachable",
        bootstrapMethods: [],
        sessionMethods: [],
        sessionCookieName: "fixture",
      },
    }),
  ),
);
vi.mock("@t3tools/client-runtime/state/session", () => ({
  createEnvironmentSessionAtoms: () => ({
    sessionStateAtom: (id: C.EnvironmentId) => sessions(id),
  }),
}));
import { ThreadRelationshipsPanel } from "../../src/components/chat/ThreadRelationshipsControl";
import { PitakoWorkersPanel } from "../../src/components/chat/PitakoWorkersPanel";

describe("Pitako worker inspection interaction", () => {
  it.effect("presents independent handoff observations and refreshes only roster/detail on native notifications", () =>
    Effect.scoped(Effect.gen(function* () {
      const f = yield* workerPanelFixture;
      state.workers = f.workers;
      vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
      let renderer: ReactTestRenderer;
      yield* Effect.addFinalizer(() => Effect.sync(() => vi.unstubAllGlobals()));
      yield* Effect.addFinalizer(() => Effect.promise(async () => {
        if (renderer) await act(async () => renderer.unmount());
      }));
      yield* Effect.promise(async () => {
        await act(async () => {
          renderer = create(<RegistryContext.Provider value={f.registry}>
            <PitakoWorkersPanel {...f.target} />
          </RegistryContext.Provider>);
        });
      });
      yield* workerRefreshScenario(f, {
        text: () => renderedWorkerText(renderer.toJSON()),
        click: async (label) => {
          await act(async () => {
            const button = renderer.root.findAllByType("button")
              .find((node) => node.props.children === label);
            expect(button, label).toBeDefined();
            expect(button!.props.disabled).not.toBe(true);
            button!.props.onClick();
          });
        },
      });
    })));
  it.effect(
    "opens with a passive roster, reads bounded ordinary and acknowledged private pages, controls explicitly and looks up an old receipt after replacement + new submission",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const f = yield* workerPanelFixture;
          state.workers = f.workers;
          vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
          let renderer: ReactTestRenderer;
          yield* Effect.addFinalizer(() => Effect.sync(() => vi.unstubAllGlobals()));
          yield* Effect.addFinalizer(() =>
            Effect.promise(async () => {
              if (renderer) await act(async () => renderer.unmount());
            }),
          );
          const wait = <A,>(atom: Atom.Atom<A>, predicate: (value: A) => boolean) =>
            new Promise<void>((resolve) => {
              let off = () => {};
              off = f.registry.subscribe(
                atom,
                (value) => {
                  if (predicate(value)) {
                    resolve();
                    queueMicrotask(() => off());
                  }
                },
                { immediate: true },
              );
            });
          const privateAtom = f.workers.privateState(f.target);
          const pages = f.workers.pages(f.target);
          const click = async (label: string, settled?: Promise<void>) => {
            await act(async () => {
              const button = renderer.root
                .findAllByType("button")
                .find((node) => node.props.children === label)!;
              expect(button, label).toBeDefined();
              expect(button.props.disabled).not.toBe(true);
              button.props.onClick();
            });
            if (settled)
              await act(async () => {
                await settled;
              });
          };
          yield* Effect.promise(async () => {
            await act(async () => {
              renderer = create(
                <RegistryContext.Provider value={f.registry}>
                  <ThreadRelationshipsPanel {...f.target} />
                </RegistryContext.Provider>,
              );
            });
            expect(f.calls.opens).toBe(0);
            await click(
              "Inspect Pitako workers",
              wait(privateAtom, (s) => s.view !== null),
            );
            await act(async () => {
              await wait(pages, (p) => p.observation?.state === "last_observation");
            });
            await act(async () => { await wait(pages, (p) => p.list !== null); });
            expect(f.calls.list).toBe(1);
            expect(f.calls.grants).toEqual([]);
            expect(f.calls.effects).toEqual([]);
            await click(
              "Load roster",
              wait(pages, (p) => p.list !== null),
            );
            await click(
              "developer · running · agent_spawn · fixture task",
              wait(pages, (p) => p.detail !== null),
            );
            expect(JSON.stringify(renderer.toJSON())).toContain("worker brief");
            expect(JSON.stringify(renderer.toJSON())).toContain("returned_service_tier");
            expect(f.calls.history).toBe(0);
            expect(f.calls.route).toBe(0);
            await click(
              "More brief",
              wait(pages, (p) => p.detail?.workbrief?.offset === 1024),
            );
            expect(f.registry.get(pages).detail?.metadata).toBeUndefined();
            await click(
              "More configuration",
              wait(pages, (p) => p.detail?.metadata?.offset === 1024),
            );
            expect(f.calls.detail.at(-1)).toMatchObject({
              section: "detail_metadata",
              offset: 1024,
            });
            expect(f.registry.get(pages).detail?.workbrief).toBeUndefined();
            await click(
              "Observed requests",
              wait(pages, (p) => p.requests !== null),
            );
            await click(
              "Activity",
              wait(pages, (p) => p.activity !== null),
            );
            await click(
              "Result",
              wait(pages, (p) => p.result !== null),
            );
            await click(
              "More result text",
              wait(pages, (p) => p.result?.state === "ready" && p.result.text?.offset === 1024),
            );
            const result = f.registry.get(pages).result;
            expect(result?.state === "ready" && result.metadata).toBeUndefined();
            expect(f.calls.result[1]).toMatchObject({ section: "result_text", offset: 1024 });
            await click(
              "More result metadata",
              wait(pages, (p) => p.result?.state === "ready" && p.result.metadata?.offset === 1024),
            );
            expect(f.calls.result.at(-1)).toMatchObject({
              section: "result_metadata",
              offset: 1024,
            });
            const metadataPage = f.registry.get(pages).result;
            expect(metadataPage?.state === "ready" && metadataPage.text).toBeUndefined();
            await click(
              "Reload detail",
              wait(pages, (p) => p.detail?.workbrief?.offset === 0),
            );
            f.policy.invalidCursor = true;
            await click(
              "More brief",
              wait(pages, (p) => p.detail === null),
            );
            expect(JSON.stringify(renderer.toJSON())).toContain("invalid_cursor");
            expect(f.registry.get(pages).result).toBeNull();
            f.policy.invalidCursor = false;
            await click(
              "Reload detail",
              wait(pages, (p) => p.detail?.workbrief?.offset === 0),
            );
            await click(
              "History metadata",
              wait(pages, (p) => p.historyList !== null),
            );
            expect(f.calls.history).toBe(0);
            await click(
              "Acknowledge private history",
              wait(privateAtom, (s) => s.view?.grants.includes("history") === true),
            );
            expect(f.calls.history).toBe(0);
            await click(
              "Read history history",
              wait(privateAtom, (s) => s.history !== null),
            );
            expect(JSON.stringify(renderer.toJSON())).toContain("eyJzZWNyZXQiOnRydWV9");
            await click(
              "Acknowledge private routing",
              wait(privateAtom, (s) => s.view?.grants.includes("route") === true),
            );
            expect(f.calls.route).toBe(0);
            await click(
              "Read routing",
              wait(privateAtom, (s) => s.route !== null),
            );
            await click(
              "Inspect control target",
              wait(privateAtom, (s) => s.control !== null),
            );
            expect(f.registry.get(privateAtom).selection).toBeNull();
            await click(
              "Activate this exact control target",
              wait(privateAtom, (s) => s.selection !== null),
            );
            const submit = async (label: string, text: string, count: number) => {
              await act(async () =>
                renderer.root.findByType("textarea").props.onChange({ target: { value: text } }),
              );
              await click(
                label,
                wait(privateAtom, (s) => s.outcomes.length === count && !s.pending),
              );
            };
            await submit("Submit query", "deliberate query", 1);
            await submit("Submit steer", "deliberate steer", 2);
            expect(f.calls.effects).toEqual([
              { intent: "query", text: "deliberate query" },
              { intent: "steer", text: "deliberate steer" },
            ]);
            await click("Request cancellation");
            expect(f.calls.effects).toHaveLength(2);
            await click(
              "Confirm cancellation request",
              wait(privateAtom, (s) => s.outcomes.length === 3 && !s.pending),
            );
            expect(f.calls.effects[2]).toBe("cancel");
            expect(JSON.stringify(renderer.toJSON())).toContain("requested");
            const old = f.registry.get(privateAtom).decisions[0]!;
            const oldView = f.registry.get(privateAtom).view!.viewId;
            await click(
              "Revoke private access",
              wait(privateAtom, (s) => s.view !== null && s.view.viewId !== oldView),
            );
            expect(f.registry.get(privateAtom).history).toBeNull();
            expect(f.registry.get(privateAtom).route).toBeNull();
            expect(f.registry.get(privateAtom).selection).toBeNull();
            expect(f.registry.get(privateAtom).view?.grants).toEqual([]);
            expect(JSON.stringify(renderer.toJSON())).not.toContain("private router capture");
            await act(async () => {
              await wait(pages, (p) => p.observation?.state === "last_observation");
            });
            await click(
              "Load roster",
              wait(pages, (p) => p.list !== null),
            );
            await click(
              "developer · running · agent_spawn · fixture task",
              wait(pages, (p) => p.detail !== null),
            );
            await click(
              "Inspect control target",
              wait(privateAtom, (s) => s.control !== null),
            );
            await click(
              "Activate this exact control target",
              wait(privateAtom, (s) => s.selection !== null),
            );
            await submit("Submit query", "new view query", 4);
            const next = f.registry.get(privateAtom).decisions.at(-1)!;
            expect(next.recoveryHandle).not.toBe(old.recoveryHandle);
            const effects = f.calls.effects.length;
            const previousOutcome = f.registry
              .get(privateAtom)
              .outcomes.find((o) => o.actionId === old.decision.actionId);
            await click(
              `Lookup receipt ${old.decision.actionId}`,
              wait(
                privateAtom,
                (s) =>
                  s.outcomes.find((o) => o.actionId === old.decision.actionId) !== previousOutcome,
              ),
            );
            expect(f.calls.effects).toHaveLength(effects);
            expect(f.registry.get(privateAtom).view?.viewId).toBe(next.decision.viewId);
            expect(
              f.registry.get(privateAtom).outcomes.find((o) => o.actionId === old.decision.actionId)
                ?.status.state,
            ).toBe("recorded");
            // Native busy after T3 admission is not the local uninvoked-ID retry affordance.
            const submitAndSettle = async (label: string) => {
              await act(async () => {
                const button = renderer.root.findAllByType("button")
                  .find((node) => node.props.children === label)!;
                expect(button).toBeDefined();
                expect(button.props.disabled).not.toBe(true);
                await button.props.onClick();
              });
            };
            await act(async () =>
              renderer.root.findByType("textarea").props.onChange({ target: { value: "native busy query" } }),
            );
            f.policy.actionBusy = true;
            const healthyView = f.registry.get(privateAtom).view!.viewId;
            const opens = f.calls.opens;
            const disconnects = f.calls.disconnects;
            await submitAndSettle("Submit query");
            const admittedBusy = f.registry.get(privateAtom).decisions.at(-1)!;
            expect(JSON.stringify(renderer.toJSON())).toContain("busy");
            expect(renderer.root.findAllByType("button").some(
              (node) => node.props.children === "Submit same locally busy decision",
            )).toBe(false);
            expect(f.registry.get(privateAtom).pending).toBe(false);
            expect(f.registry.get(privateAtom).view?.viewId).toBe(healthyView);
            expect(f.registry.get(privateAtom).selection).not.toBeNull();
            expect(f.calls.effects).toHaveLength(effects + 1);
            f.policy.actionBusy = false;
            await submitAndSettle(`Lookup receipt ${admittedBusy.decision.actionId}`);
            expect(f.registry.get(privateAtom).outcomes.find(
              (outcome) => outcome.actionId === admittedBusy.decision.actionId,
            )).toBeDefined();
            expect(f.calls.receipts.at(-1)).toBe(2);
            expect(f.calls.effects).toHaveLength(effects + 1);
            expect(f.calls.opens).toBe(opens);
            expect(f.calls.disconnects).toBe(disconnects);
            // Pre-admission busy still permits a deliberate submission of that same immutable ID.
            await act(async () =>
              renderer.root.findByType("textarea").props.onChange({ target: { value: "locally busy query" } }),
            );
            f.policy.detailBusy = true;
            await submitAndSettle("Submit query");
            const localBusy = f.registry.get(privateAtom).decisions.at(-1)!;
            expect(f.calls.effects).toHaveLength(effects + 1);
            f.policy.detailBusy = false;
            await submitAndSettle("Submit same locally busy decision");
            expect(f.registry.get(privateAtom).decisions.at(-1)).toBe(localBusy);
            expect(f.registry.get(privateAtom).outcomes.find(
              (outcome) => outcome.actionId === localBusy.decision.actionId,
            )?.status).toMatchObject({ state: "recorded", sequence: 3 });
            expect(f.calls.effects).toHaveLength(effects + 2);
            await click("Acknowledge private history",
              wait(privateAtom, (s) => s.view?.grants.includes("history") === true));
            await click("History metadata", wait(pages, (s) => s.historyList !== null));
            await click("Read history history", wait(privateAtom, (s) => s.history !== null));
            await click("Acknowledge private routing",
              wait(privateAtom, (s) => s.view?.grants.includes("route") === true));
            await click("Read routing", wait(privateAtom, (s) => s.route !== null));
            const independent = f.registry.get(privateAtom);
            // Admitted request/cursor failures must not use ordinary traversal-error clearing.
            for (const code of ["invalid_request", "invalid_cursor", "stale_cursor", "oversized"]) {
              f.policy.actionError = code;
              await act(async () =>
                renderer.root.findByType("textarea").props.onChange({ target: { value: "deliberate native rejection" } }));
              await submitAndSettle("Submit query");
              expect(JSON.stringify(renderer.toJSON())).toContain(code);
              const after = f.registry.get(privateAtom);
              expect(after.pending).toBe(false);
              expect(after.view).toBe(independent.view);
              expect(after.history).toBe(independent.history);
              expect(after.route).toBe(independent.route);
              expect(after.selection).toBe(independent.selection);
              expect(renderer.root.findAllByType("button").some(
                (node) => node.props.children === "Submit same locally busy decision")).toBe(false);
            }
            f.policy.actionError = undefined;
            f.policy.recordedError = "stale_selection";
            await act(async () =>
              renderer.root.findByType("textarea").props.onChange({ target: { value: "stale target" } }));
            await click("Submit query", wait(privateAtom, (s) => s.control === null && s.selection === null && !s.pending));
            const stale = f.registry.get(privateAtom);
            expect(stale.view).toBe(independent.view);
            expect(stale.history).toBe(independent.history);
            expect(stale.route).toBe(independent.route);
            expect(stale.decisions).toHaveLength(independent.decisions.length + 5);
            expect(stale.outcomes.at(-1)?.status).toMatchObject({
              state: "recorded", receipt: { kind: "error", code: "stale_selection" },
            });
            expect(renderer.root.findAllByType("button").find(
              (node) => node.props.children === "Submit query")?.props.disabled).toBe(true);
            const staleEffects = f.calls.effects.length;
            await submitAndSettle(`Lookup receipt ${stale.decisions.at(-1)!.decision.actionId}`);
            expect(f.calls.effects).toHaveLength(staleEffects);
            expect(f.registry.get(privateAtom).selection).toBeNull();
            f.policy.recordedError = undefined;
            await click("Inspect control target", wait(privateAtom, (s) => s.control !== null));
            await click("Activate this exact control target", wait(privateAtom, (s) => s.selection !== null));
            await act(async () =>
              renderer.root.findByType("textarea").props.onChange({ target: { value: "fresh explicit decision" } }));
            await submitAndSettle("Submit query");
            expect(f.calls.effects).toHaveLength(staleEffects + 1);
            expect(f.calls.opens).toBe(opens);
            expect(f.calls.disconnects).toBe(disconnects);
            await click(
              "Close Pitako inspection",
              wait(privateAtom, (s) => s.view === null),
            );
            expect(f.calls.effects).toHaveLength(staleEffects + 1);
            expect(f.registry.get(privateAtom).selection).toBeNull();
            await act(async () => renderer.unmount());
          });
          // The service scope joins all native observation and private stream finalizers.
          return f.calls;
        }),
      ).pipe(
        Effect.tap((calls) =>
          Effect.sync(() => {
            expect(calls.disconnects).toBe(calls.opens);
            expect(calls.liveObservations).toBe(0);
          }),
        ),
      ),
  );
});
