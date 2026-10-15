import * as NodeModule from "node:module";
import {
  EffectAtomReact,
  React,
  act,
  create,
  type ReactTestRenderer,
} from "../../../../web/test/component-renderer";
import type { ParamListBase, StackNavigationState } from "@react-navigation/native";
import { EnvironmentId, RunId, ThreadId, type PiNativeWorkerSummary } from "@t3tools/contracts";
import { AtomRegistry } from "effect/reactivity";
import { afterEach, expect, it, vi } from "vite-plus/test";
import {
  nativeSubagentFixture,
  nativeWorkerFixture,
} from "../../../../../packages/client-runtime/src/testing/nativeWorker";

const { RegistryContext } = EffectAtomReact;
const state = vi.hoisted(() => ({
  navigate: vi.fn<(name: string, params: object) => void>(),
  goBack: vi.fn<() => void>(),
  update:
    vi.fn<
      (
        registry: AtomRegistry.AtomRegistry,
        worker: PiNativeWorkerSummary | null,
        parentMissing?: boolean,
      ) => void
    >(),
}));
vi.mock("react", async () => (await import("../../../../web/test/component-renderer")).React);
vi.mock(
  "@effect/atom-react",
  async () => (await import("../../../../web/test/component-renderer")).EffectAtomReact,
);
vi.mock("react-native", () => ({
  Platform: { OS: "android", Version: 35 },
  View: "View",
  ScrollView: "ScrollView",
}));
vi.mock("@react-navigation/native", () => ({
  useNavigation: () => ({ navigate: state.navigate, goBack: state.goBack }),
}));
vi.mock("react-native-screens", () => ({
  Screen: "Screen",
  ScreenStack: "ScreenStack",
  ScreenStackHeaderConfig: "Header",
}));
vi.mock("react-native-safe-area-context", () => ({ useSafeAreaInsets: () => ({ bottom: 0 }) }));
vi.mock("../../components/AndroidScreenHeader", () => ({ AndroidSheetHeader: "Header" }));
vi.mock("../../components/AppText", () => ({ AppText: "Text" }));
vi.mock("../../components/ProviderIcon", () => ({ ProviderIcon: "Icon" }));
vi.mock("../../components/MaterialScreenContent", () => ({ MaterialScreenContent: "Material" }));
vi.mock("uniwind", () => ({ withUniwind: (component: unknown) => component }));
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
vi.mock("../../state/threads", async () => {
  const { Atom, AsyncResult } = await import("effect/reactivity");
  const Option = await import("effect/Option");
  const { createEnvironmentThreadDetailAtoms, EMPTY_ENVIRONMENT_THREAD_STATE } =
    await import("@t3tools/client-runtime/state/threads");
  const { v2Projection } =
    await import("../../../../../packages/client-runtime/src/state/orchestrationV2TestFixtures");
  const projection = (worker: PiNativeWorkerSummary | null) => ({
    ...v2Projection,
    // The parent has moved on. The selected worker is not in its current turn.
    runs: [],
    subagents: worker
      ? [
          nativeSubagentFixture({
            childThreadId: ThreadId.make("other-child"),
            nativeWorker: nativeWorkerFixture({ brief: "WRONG CHILD" }),
          }),
          nativeSubagentFixture({ runId: RunId.make("earlier-run"), nativeWorker: worker }),
        ]
      : [],
  });
  const source = Atom.make(
    AsyncResult.success({
      ...EMPTY_ENVIRONMENT_THREAD_STATE,
      data: Option.some(projection(nativeWorkerFixture())),
    }),
  );
  const otherEnvironment = Atom.make(
    AsyncResult.success({
      ...EMPTY_ENVIRONMENT_THREAD_STATE,
      data: Option.some(projection(nativeWorkerFixture({ brief: "WRONG ENVIRONMENT" }))),
    }),
  );
  state.update.mockImplementation((registry, worker, parentMissing = false) => {
    registry.set(
      source,
      AsyncResult.success({
        ...EMPTY_ENVIRONMENT_THREAD_STATE,
        data: parentMissing ? Option.none() : Option.some(projection(worker)),
      }),
    );
  });
  return {
    environmentThreadDetails: createEnvironmentThreadDetailAtoms((environmentId, threadId) =>
      environmentId === "remote" && threadId === "parent" ? source : otherEnvironment,
    ),
  };
});

import { ThreadNativeWorkerDetailsSheet } from "./ThreadNativeWorkerDetailsSheet";
import { ProviderSubagentBar } from "./ProviderSubagentBar";

// Same pure router used by the native stack; only the platform navigation boundary is doubled.
const requireNavigation = NodeModule.createRequire(
  NodeModule.createRequire(import.meta.url).resolve("@react-navigation/native/package.json"),
);
const { StackRouter, CommonActions } = requireNavigation("@react-navigation/routers") as {
  StackRouter: typeof import("@react-navigation/native").StackRouter;
  CommonActions: typeof import("@react-navigation/native").CommonActions;
};
const target = {
  environmentId: EnvironmentId.make("remote"),
  parentThreadId: ThreadId.make("parent"),
  childThreadId: ThreadId.make("worker-chat"),
};
let renderer: ReactTestRenderer;
afterEach(async () => {
  await act(async () => renderer?.unmount());
  vi.unstubAllGlobals();
  state.navigate.mockReset();
  state.goBack.mockReset();
});

function texts() {
  return renderer.root
    .findAll((node) => String(node.type) === "Text")
    .map((node) => node.children.join(""));
}
function button(label: string) {
  return renderer.root.findAllByType("button").find((node) => node.children.join("") === label)!;
}
async function mountReader(registry: AtomRegistry.AtomRegistry) {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  await act(async () => {
    renderer = create(
      <RegistryContext.Provider value={registry}>
        <ThreadNativeWorkerDetailsSheet route={{ params: target }} />
      </RegistryContext.Provider>,
    );
  });
}

it("reads exact original text and independent observations from the same environment, including earlier turns", async () => {
  const registry = AtomRegistry.make();
  const brief = "  first line\n\n<b>literal</b> & \tlast  \n" + "long wrapped text ".repeat(500);
  const worker = nativeWorkerFixture({
    brief,
    briefComplete: false,
    selected: { requestedModel: "requested-only", model: "selected-only", reasoning: "low" },
    gaps: ["unsupported_content"],
  });
  state.update(registry, worker);
  try {
    await mountReader(registry);
    expect(texts()).toContain(brief);
    expect(texts()).toContain("Partial brief — only the first page is available.");
    for (const value of [
      "completed",
      "open",
      "unavailable",
      "available",
      "reviewer-instance",
      "assignment-review",
      "requested-only",
      "selected-only",
      "gpt-effective",
      "unsupported_content",
    ]) {
      expect(texts()).toContain(value);
    }
    expect(texts().join(" ")).not.toContain("WRONG ENVIRONMENT");
    expect(texts().join(" ")).not.toContain("WRONG CHILD");
    expect(texts().join("\n")).toContain(
      "Execution\ncompleted\nCapture\nopen\nAvailability\nunavailable\nResult\navailable",
    );
    expect(texts().join(" ")).toContain("Gaps do not determine execution status");
    expect(renderer.root.findAllByType("button").map((node) => node.children.join(""))).toEqual([
      "Close",
    ]);
    expect(renderer.root.findAll((node) => String(node.type) === "TextInput")).toHaveLength(0);

    const { execution: _execution, result: _result, activated: _activated, ...unreported } = worker;
    await act(async () =>
      state.update(registry, { ...unreported, brief: "new brief", briefComplete: true, gaps: [] }),
    );
    expect(texts()).toContain("new brief");
    expect(texts()).not.toContain(brief);
    expect(texts()).toContain("Complete brief");
    expect(texts()).toContain("unknown");
    expect(texts()).toContain("Not reported");
    expect(texts().join("\n")).toContain("Result\nNot reported");
    expect(texts().join("\n")).toContain("Activated model\nNot reported");
    expect(texts()).toContain("No gaps reported.");
    expect(texts().join(" ")).toContain("No reported gaps does not establish complete capture.");

    await act(async () => state.update(registry, null));
    expect(texts()).toContain("Worker details unavailable");
    expect(texts()).not.toContain("new brief");
    expect(texts()).not.toContain("reviewer-instance");
    await act(async () => state.update(registry, worker, true));
    expect(texts()).toContain("Worker details unavailable");
    expect(texts()).not.toContain(brief);
  } finally {
    await act(async () => renderer.unmount());
    registry.dispose();
  }
});

it("distinguishes missing, empty and unknown-completeness originals without leaking another launch's assignment ID", async () => {
  const registry = AtomRegistry.make();
  const { brief: _brief, briefComplete: _complete, ...withoutBrief } = nativeWorkerFixture();
  state.update(registry, withoutBrief);
  try {
    await mountReader(registry);
    expect(texts()).toContain("Brief unavailable");
    await act(async () => state.update(registry, { ...withoutBrief, brief: "" }));
    expect(texts()).not.toContain("Brief unavailable");
    expect(texts()).toContain("Brief completeness not reported.");
    expect(texts()).not.toContain("Complete brief");
    const launch = { ...withoutBrief.launch, source: "agent_spawn" as const };
    await act(async () => state.update(registry, { ...withoutBrief, launch, brief: " \n\t " }));
    expect(texts()).toContain(" \n\t ");
    expect(texts()).toContain("agent_spawn");
    expect(texts()).not.toContain("Assignment ID");
    expect(texts()).not.toContain("assignment-review");
  } finally {
    await act(async () => renderer.unmount());
    registry.dispose();
  }
});

it("opens the ID-only reader above the observed child and returns to that exact thread on close or Back", async () => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  const registry = AtomRegistry.make();
  state.update(registry, nativeWorkerFixture());
  const router = StackRouter({});
  const options = {
    routeNames: ["Thread", "ThreadNativeWorkerDetails"],
    routeParamList: {},
    routeGetIdList: {},
  };
  const observed = {
    key: "observed-child",
    name: "Thread",
    params: { environmentId: "remote", threadId: "worker-chat" },
  };
  const initialHistory: StackNavigationState<ParamListBase> = {
    stale: false,
    type: "stack",
    key: "stack",
    index: 0,
    routeNames: options.routeNames,
    routes: [observed],
    preloadedRoutes: [],
  };
  const onStateChange = vi.fn<(current: StackNavigationState<ParamListBase>) => void>();
  function Workspace() {
    const [current, setCurrent] = React.useState(initialHistory);
    React.useEffect(() => {
      onStateChange(current);
    }, [current]);
    const dispatch = (action: Parameters<typeof router.getStateForAction>[1]) => {
      setCurrent((previous) => {
        const next = router.getStateForAction(previous, action, options);
        if (!next || next.stale !== false) throw new Error("Router rejected navigation");
        return next;
      });
    };
    state.navigate.mockImplementation((name, params) =>
      dispatch(CommonActions.navigate(name, params)),
    );
    state.goBack.mockImplementation(() => dispatch(CommonActions.goBack()));
    const route = current.routes[current.index]!;
    return route.name === "Thread" ? (
      <>
        <span>{JSON.stringify(route.params)}</span>
        <ProviderSubagentBar
          provider={null}
          modelLabel="requested"
          effortLabel={null}
          status={null}
          nativeWorker={nativeWorkerFixture()}
          onOpenParent={null}
          onOpenAssignment={() => state.navigate("ThreadNativeWorkerDetails", target)}
        />
      </>
    ) : (
      <ThreadNativeWorkerDetailsSheet route={{ params: target }} />
    );
  }
  try {
    await act(async () => {
      renderer = create(
        <RegistryContext.Provider value={registry}>
          <Workspace />
        </RegistryContext.Provider>,
      );
    });
    for (const dismiss of [
      () => button("Close").props.onClick(),
      () => renderer.root.find((node) => String(node.type) === "Header").props.onBack(),
    ]) {
      await act(async () => button("Assignment").props.onClick());
      const history = onStateChange.mock.lastCall![0];
      expect(history.routes).toHaveLength(2);
      expect(history.routes[0]).toBe(observed);
      expect(history.routes[1]?.params).toEqual(target);
      expect(Object.keys(history.routes[1]!.params!).sort()).toEqual([
        "childThreadId",
        "environmentId",
        "parentThreadId",
      ]);
      expect(texts()).toContain("Review the public changes");
      await act(async () => dismiss());
      expect(onStateChange.mock.lastCall![0].routes).toEqual([observed]);
      expect(renderer.root.findByType("span").children.join("")).toBe(
        JSON.stringify(observed.params),
      );
      expect(button("Assignment")).toBeDefined();
      expect(texts()).not.toContain("Original brief");
    }
  } finally {
    await act(async () => renderer.unmount());
    registry.dispose();
  }
});

it("uses execution only in visible and spoken footer text, keeps unreported activated models explicit, and leaves non-native presentation alone", async () => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  const base = nativeWorkerFixture();
  const { activated: _activated, execution: _execution, roleId: _role, ...unreported } = base;
  await act(async () => {
    renderer = create(
      <ProviderSubagentBar
        provider={null}
        modelLabel="selected-only"
        effortLabel="high"
        status={null}
        nativeWorker={{
          ...unreported,
          launch: { ...base.launch, source: "agent_spawn" },
          selected: { model: "selected-only" },
        }}
        onOpenParent={null}
        onOpenAssignment={() => {}}
      />,
    );
  });
  expect(texts()).toContain("agent_spawn");
  expect(texts()).toContain("Not reported");
  expect(texts().join(" ")).toContain("Execution unknown");
  const spoken = renderer.root
    .findAll((node) => node.props.accessible === true)
    .map((node) => node.props.accessibilityLabel)
    .join(" ");
  for (const dump of [
    "reviewer-instance",
    "assignment-review",
    "Review the public changes",
    "Capture",
    "Availability",
    "Result",
    "selected-only",
  ]) {
    expect(texts().join(" ")).not.toContain(dump);
    expect(spoken).not.toContain(dump);
  }
  expect(spoken).toContain("Execution unknown");
  expect(button("Assignment")).toBeDefined();
  await act(async () =>
    renderer.update(
      <ProviderSubagentBar
        provider={null}
        modelLabel="ordinary-model"
        effortLabel="high"
        status={{
          status: "completed",
          startedAt: "2026-06-20T00:00:00Z",
          completedAt: "2026-06-20T00:00:12Z",
        }}
        onOpenParent={null}
        onOpenAssignment={() => {}}
      />,
    ),
  );
  expect(texts().join(" ")).toContain("Completed in 12s");
  expect(texts()).toContain("ordinary-model");
  expect(texts()).toContain("high");
  expect(renderer.root.findAllByType("button")).toHaveLength(0);
});
