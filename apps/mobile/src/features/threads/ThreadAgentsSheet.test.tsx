import {
  EffectAtomReact,
  act,
  create,
  type ReactTestRenderer,
} from "../../../../web/test/component-renderer";
import { EnvironmentId, NodeId, RunId, ThreadId } from "@t3tools/contracts";
import { AtomRegistry } from "effect/reactivity";
import { afterEach, expect, it, vi } from "vite-plus/test";
import {
  nativeSubagentFixture,
  nativeWorkerFixture,
} from "../../../../../packages/client-runtime/src/testing/nativeWorker";

const { RegistryContext } = EffectAtomReact;
const state = vi.hoisted(() => ({ dispatch: vi.fn(), parent: vi.fn(), noCurrent: false }));
vi.mock("react", async () => (await import("../../../../web/test/component-renderer")).React);
vi.mock(
  "@effect/atom-react",
  async () => (await import("../../../../web/test/component-renderer")).EffectAtomReact,
);
// Resolve native host shims from the mobile package, not web's dependency graph.
vi.mock("react-native", () => ({
  Platform: { OS: "android", Version: 35 },
  View: "View",
  Pressable: "Pressable",
  ScrollView: "ScrollView",
}));
vi.mock("@react-navigation/native", () => ({
  useNavigation: () => ({ dispatch: state.dispatch, goBack: vi.fn() }),
  StackActions: {
    replace: (name: string, params: unknown) => ({ type: "REPLACE", payload: { name, params } }),
  },
}));
vi.mock("expo-haptics", () => ({ selectionAsync: vi.fn() }));
vi.mock("react-native-screens", () => ({
  Screen: "Screen",
  ScreenStack: "ScreenStack",
  ScreenStackHeaderConfig: "Header",
}));
vi.mock("react-native-safe-area-context", () => ({ useSafeAreaInsets: () => ({ bottom: 0 }) }));
vi.mock("../../components/AndroidScreenHeader", () => ({ AndroidSheetHeader: "Header" }));
vi.mock("../../components/AppText", () => ({ AppText: "Text" }));
vi.mock("../../components/ScreenHeader", () => ({ ScreenHeader: "Header" }));
vi.mock("../../native/SelectableMarkdownText", () => ({ SelectableMarkdownText: "Markdown" }));
vi.mock("../files/FileMarkdownPreview", () => ({
  useMarkdownPreviewStyles: () => ({ nativeTextStyle: {} }),
}));
vi.mock("../../connection/runtime", async () => {
  const { Atom } = await import("effect/reactivity");
  const Layer = await import("effect/Layer");
  return { connectionAtomRuntime: Atom.runtime(Layer.empty) };
});
vi.mock("../../components/AppSymbol", () => ({ SymbolView: "Symbol" }));
vi.mock("../../components/ProviderIcon", () => ({ ProviderIcon: "Icon" }));
vi.mock("../../components/MaterialScreenContent", () => ({ MaterialScreenContent: "Material" }));
vi.mock("../../lib/useUniwindTheme", () => ({ useUniwindTheme: () => ({}) }));
vi.mock("../../native/StackHeader", () => ({
  NativeStackScreenOptions: "Options",
  nativeHeaderScrollEdgeEffects: () => ({}),
}));
vi.mock("../layout/AdaptiveWorkspaceLayout", () => ({
  useAdaptiveWorkspaceLayout: () => ({ layout: { usesSplitView: false } }),
}));
vi.mock("./RequestActionButton", () => ({
  RequestActionButton: ({ label, onPress }: { label: string; onPress: () => void }) => (
    <button type="button" onClick={onPress}>
      {label}
    </button>
  ),
}));
vi.mock("./use-visible-second-clock", () => ({ useVisibleSecondClock: () => 0 }));
vi.mock("../../state/entities", () => ({
  useEnvironmentServerConfig: () => null,
  useProject: () => null,
  useThreadShell: () => null,
}));
vi.mock("../../state/threads", async () => {
  const { Atom, AsyncResult } = await import("effect/reactivity");
  const Option = await import("effect/Option");
  const { createEnvironmentThreadDetailAtoms, EMPTY_ENVIRONMENT_THREAD_STATE } =
    await import("@t3tools/client-runtime/state/threads");
  const { v2Projection } =
    await import("../../../../../packages/client-runtime/src/state/orchestrationV2TestFixtures");
  const team = nativeSubagentFixture();
  const launch = team.nativeWorker!.launch;
  if (launch.source !== "team_assign") throw new Error("Expected tool-launched fixture");
  const independent = nativeSubagentFixture({
    id: NodeId.make("independent"),
    childThreadId: ThreadId.make("independent-chat"),
    title: "Independent worker",
    nativeWorker: nativeWorkerFixture({
      launch: { ...launch, source: "agent_spawn", instanceId: "independent" },
    }),
  });
  const closed = nativeSubagentFixture({
    id: NodeId.make("closed"),
    childThreadId: ThreadId.make("closed-chat"),
    title: "Earlier closed worker",
    runId: RunId.make("earlier-run"),
    nativeWorker: nativeWorkerFixture({ capture: "closed" }),
  });
  const source = Atom.make(
    AsyncResult.success({
      ...EMPTY_ENVIRONMENT_THREAD_STATE,
      data: Option.some({ ...v2Projection, runs: [], subagents: [team, independent, closed] }),
    }),
  );
  const details = createEnvironmentThreadDetailAtoms(() => source);
  const noCurrent = Atom.make(null);
  return {
    environmentThreadDetails: {
      ...details,
      turnSubagentsAtom: (target: Parameters<typeof details.turnSubagentsAtom>[0]) =>
        state.noCurrent ? noCurrent : details.turnSubagentsAtom(target),
    },
  };
});

import { ThreadAgentsSheet } from "./ThreadAgentsSheet";
import { ProviderSubagentBar } from "./ProviderSubagentBar";

let renderer: ReactTestRenderer;
afterEach(async () => {
  await act(async () => renderer?.unmount());
  vi.unstubAllGlobals();
  state.dispatch.mockClear();
  state.parent.mockClear();
  state.noCurrent = false;
});

it("uses the shared native roster and opens current and persisted earlier public children in the same environment", async () => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  const registry = AtomRegistry.make();
  try {
    await act(async () => {
      renderer = create(
        <RegistryContext.Provider value={registry}>
          <ThreadAgentsSheet
            route={{
              params: {
                environmentId: EnvironmentId.make("remote"),
                threadId: ThreadId.make("parent"),
              },
            }}
          />
        </RegistryContext.Provider>,
      );
    });
    const text = JSON.stringify(renderer.toJSON());
    expect(text).toContain("Team assignments");
    expect(text).toContain("reviewer · Assignment assignment-review");
    expect(text).toContain(
      "Execution completed · Capture open · Availability unavailable · Result available",
    );
    expect(text).toContain("Effective model: ");
    expect(text).toContain("gpt-effective");
    expect(text).not.toContain("requested-not-effective");
    expect(text).toContain("Lineage · earlier assignments");
    const links = renderer.root.findAll((node) => String(node.type) === "Pressable");
    expect(links).toHaveLength(3);
    for (const link of links) await act(async () => link.props.onPress());
    expect(state.dispatch.mock.calls.map(([action]) => action.payload.params)).toEqual([
      { environmentId: "remote", threadId: "independent-chat" },
      { environmentId: "remote", threadId: "worker-chat" },
      { environmentId: "remote", threadId: "closed-chat" },
    ]);
  } finally {
    await act(async () => renderer.unmount());
    registry.dispose();
  }
});

it("keeps persisted Lineage navigable when the selected turn has no roster", async () => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  state.noCurrent = true;
  const registry = AtomRegistry.make();
  try {
    await act(async () => {
      renderer = create(
        <RegistryContext.Provider value={registry}>
          <ThreadAgentsSheet
            route={{
              params: {
                environmentId: EnvironmentId.make("remote"),
                threadId: ThreadId.make("parent"),
              },
            }}
          />
        </RegistryContext.Provider>,
      );
    });
    expect(JSON.stringify(renderer.toJSON())).toContain("Lineage · earlier assignments");
    expect(JSON.stringify(renderer.toJSON())).not.toContain("No agents in this turn.");
    // Native host children are rendered by AgentRow; choose its ordinary open action.
    const links = renderer.root.findAll((node) => String(node.type) === "Pressable");
    expect(links).toHaveLength(3);
    await act(async () => links[2]!.props.onPress());
    expect(state.dispatch).toHaveBeenCalledWith({
      type: "REPLACE",
      payload: { name: "Thread", params: { environmentId: "remote", threadId: "closed-chat" } },
    });
  } finally {
    await act(async () => renderer.unmount());
    registry.dispose();
  }
});

it("keeps the native footer compact and read-only while preserving the parent return", async () => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  await act(async () => {
    renderer = create(
      <ProviderSubagentBar
        provider={null}
        modelLabel="requested"
        effortLabel={null}
        status={null}
        nativeWorker={nativeWorkerFixture()}
        onOpenParent={state.parent}
      />,
    );
  });
  const text = JSON.stringify(renderer.toJSON());
  expect(text).not.toContain("Availability unavailable");
  expect(text).not.toContain("Capture open");
  expect(text).not.toContain("assignment-review");
  expect(text).not.toContain("Review the public changes");
  expect(text).toContain("reviewer");
  expect(text).toContain("gpt-effective");
  expect(text).toContain("Execution completed");
  expect(text).not.toContain("Execution failed");
  expect(renderer.root.findAll((node) => String(node.type) === "TextInput")).toHaveLength(0);
  expect(renderer.root.findAllByType("button")).toHaveLength(1);
  await act(async () => renderer.root.findByType("button").props.onClick());
  expect(state.parent).toHaveBeenCalledOnce();
});
