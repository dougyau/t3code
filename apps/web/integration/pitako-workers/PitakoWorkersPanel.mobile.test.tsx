import { RegistryContext } from "@effect/atom-react";
import * as C from "@t3tools/contracts";
import type { createProviderWorkersEnvironmentAtoms } from "@t3tools/client-runtime/state/provider-workers";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as Option from "effect/Option";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { Atom, AsyncResult } from "effect/reactivity";
import { act, create, type ReactTestRenderer } from "../../src/test/componentRenderer";
import { describe, expect, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import { workerPanelFixture } from "./PitakoWorkersPanel.fixture";

const state = vi.hoisted(() => ({
  workers: null as ReturnType<typeof createProviderWorkersEnvironmentAtoms> | null,
  turn: null as Atom.Atom<unknown> | null,
  navigations: [] as unknown[],
}));
// Exercise native components with the prepared web renderer's React version, not Expo's React.
vi.mock(
  "../../../mobile/node_modules/react",
  async () => (await import("../../src/test/componentRenderer")).React,
);
vi.mock(
  "../../../mobile/node_modules/react/compiler-runtime",
  async () => (await import("../../src/test/componentRenderer")).ReactCompilerRuntime,
);
vi.mock(
  "../../../mobile/node_modules/@effect/atom-react",
  async () => (await import("../../src/test/componentRenderer")).AtomReact,
);
vi.mock("../../../mobile/src/state/provider-workers", () => ({
  get providerWorkers() {
    return state.workers;
  },
}));
vi.mock("../../../mobile/src/state/threads", () => ({
  environmentThreadDetails: { turnSubagentsAtom: () => state.turn },
}));
vi.mock("../../../mobile/node_modules/react-native", () => ({
  View: "view",
  ScrollView: "scroll",
  Pressable: "button",
  TextInput: "input",
  Platform: { OS: "android" },
}));
vi.mock("../../../mobile/src/components/AppText", () => ({ AppText: "text" }));
vi.mock("../../../mobile/src/components/AndroidScreenHeader", () => ({
  AndroidSheetHeader: () => null,
}));
vi.mock("../../../mobile/src/lib/useUniwindTheme", () => ({ useUniwindTheme: () => ({}) }));
vi.mock("../../../mobile/src/native/StackHeader", () => ({
  nativeHeaderScrollEdgeEffects: () => ({}),
}));
vi.mock("../../../mobile/node_modules/react-native-safe-area-context", () => ({
  useSafeAreaInsets: () => ({ bottom: 0 }),
}));
vi.mock("../../../mobile/node_modules/react-native-screens", () => ({
  Screen: "screen",
  ScreenStack: "stack",
  ScreenStackHeaderConfig: "header",
}));
vi.mock("../../../mobile/node_modules/expo-haptics", () => ({
  selectionAsync: () => Promise.resolve(),
}));
vi.mock("../../../mobile/node_modules/@react-navigation/native", () => ({
  useNavigation: () => ({
    dispatch: (action: unknown) => state.navigations.push(action),
    goBack: () => {},
  }),
  StackActions: { replace: (name: string, params: unknown) => ({ name, params }) },
}));
vi.mock("../../../mobile/src/features/threads/SubagentRow", () => ({
  SubagentRow: () => <text>Existing subagent</text>,
}));
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
  createEnvironmentSessionAtoms: () => ({ sessionStateAtom: sessions }),
}));
import { ThreadAgentsSheet } from "../../../mobile/src/features/threads/ThreadAgentsSheet";

describe("mobile Pitako agents entry", () => {
  it.effect(
    "coexists with subagent navigation, forbids auto input, permits explicit cancel, and clears private data on transport loss",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const f = yield* workerPanelFixture;
          f.policy.mode = "auto-accept-edits";
          state.workers = f.workers;
          state.navigations = [];
          const childThreadId = C.ThreadId.make("existing-child");
          const subagent: C.OrchestrationV2Subagent = {
            id: C.NodeId.make("existing"),
            threadId: f.target.threadId,
            runId: null,
            parentNodeId: C.NodeId.make("parent"),
            origin: "app_owned",
            createdBy: "agent",
            driver: C.ProviderDriverKind.make("codex"),
            providerInstanceId: C.ProviderInstanceId.make("codex"),
            providerThreadId: null,
            childThreadId,
            nativeTaskRef: null,
            prompt: "Existing task",
            title: null,
            model: null,
            status: "completed",
            result: "done",
            startedAt: null,
            completedAt: null,
            updatedAt: DateTime.makeUnsafe(1),
          };
          state.turn = Atom.make({ subagents: [subagent], liveCount: 0 });
          vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
          let renderer: ReactTestRenderer;
          yield* Effect.addFinalizer(() => Effect.sync(() => vi.unstubAllGlobals()));
          yield* Effect.addFinalizer(() =>
            Effect.promise(async () => {
              if (renderer) await act(async () => renderer.unmount());
            }),
          );
          const privateAtom = f.workers.privateState(f.target);
          const pages = f.workers.pages(f.target);
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
          const press = async (label: string, settled?: Promise<void>) => {
            await act(async () => {
              const button = renderer.root
                .findAllByType("button")
                .find((node) => node.props.accessibilityLabel === label)!;
              expect(button, label).toBeDefined();
              expect(button.props.disabled).not.toBe(true);
              button.props.onPress();
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
                  <ThreadAgentsSheet route={{ params: f.target }} />
                </RegistryContext.Provider>,
              );
            });
            expect(f.calls.opens).toBe(0);
            await act(async () =>
              renderer.root
                .findAllByType("button")
                .find((node) => node.props.accessibilityRole === "link")!
                .props.onPress(),
            );
            expect(state.navigations).toEqual([
              { name: "Thread", params: { ...f.target, threadId: childThreadId } },
            ]);
            await press(
              "Inspect Pitako workers",
              wait(privateAtom, (s) => s.view !== null),
            );
            await act(async () => {
              await wait(pages, (p) => p.observation?.state === "last_observation");
            });
            await press(
              "Load roster",
              wait(pages, (p) => p.list !== null),
            );
            await press(
              "developer · running · agent_spawn · fixture task",
              wait(pages, (p) => p.detail !== null),
            );
            await press(
              "Inspect control target",
              wait(privateAtom, (s) => s.control !== null),
            );
            await press(
              "Activate this exact control target",
              wait(privateAtom, (s) => s.selection !== null),
            );
            for (const label of ["Submit query", "Submit steer"])
              expect(
                renderer.root
                  .findAllByType("button")
                  .find((node) => node.props.accessibilityLabel === label)!.props.disabled,
              ).toBe(true);
            expect(renderer.root.findByType("input").props.editable).toBe(false);
            expect(f.calls.effects).toEqual([]);
            await press("Request cancellation");
            expect(f.calls.effects).toEqual([]);
            await press(
              "Confirm cancellation request",
              wait(privateAtom, (s) => s.outcomes.length === 1),
            );
            expect(f.calls.effects).toEqual(["cancel"]);
            await press(
              "Acknowledge private routing",
              wait(privateAtom, (s) => s.view?.grants.includes("route") === true),
            );
            await press(
              "Read routing",
              wait(privateAtom, (s) => s.route !== null),
            );
          });
          const lost = wait(privateAtom, (s) => s.view === null);
          yield* SubscriptionRef.set(f.sessionRef, Option.none());
          yield* Effect.promise(async () => {
            await act(async () => {
              await lost;
            });
            expect(f.registry.get(privateAtom).route).toBeNull();
            expect(f.registry.get(privateAtom).selection).toBeNull();
            expect(JSON.stringify(renderer.toJSON())).not.toContain("private router capture");
            expect(f.calls.effects).toEqual(["cancel"]);
            await act(async () => renderer.unmount());
          });
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
