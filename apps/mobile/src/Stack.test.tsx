import * as NodeModule from "node:module";
import {
  React,
  EffectAtomReact,
  act,
  create,
  type ReactTestRenderer,
} from "../../web/test/component-renderer";
import type { ParamListBase, StackNavigationState } from "@react-navigation/native";
import type { ReactNode } from "react";
import { EnvironmentId, ThreadId, type PiArchitectureReadResult } from "@t3tools/contracts";
import { tablePresentationAtom } from "@t3tools/client-runtime/state/piArchitecture/model";
import { presentThreadShell } from "@t3tools/client-runtime/state/shell";
import { v2ThreadShell } from "../../../packages/client-runtime/src/state/orchestrationV2TestFixtures";
import { Atom, AtomRegistry, AsyncResult } from "effect/reactivity";
import { scopedThreadKey } from "./lib/scopedEntities";
import {
  readThreadReadingPosition,
  rememberThreadReadingPosition,
  type ThreadReadingPosition,
} from "./features/threads/thread-reading-position";
import { afterEach, expect, it, vi } from "vite-plus/test";

const host = vi.hoisted(() => ({
  useRoute: () => ({ key: "", name: "", params: {} as Record<string, unknown> }),
  useFocused: (): boolean => false,
  navigation: {} as {
    navigate: (name: string, params: object) => void;
    dispatch: (action: unknown) => void;
    goBack: () => void;
    canGoBack: () => boolean;
  },
  back: new Set<() => boolean>(),
  selectTab: vi.fn(),
  query: undefined as unknown,
  mode: "loaded" as "loaded" | "loading" | "unavailable",
  keyboard: false,
  usesSplitView: false,
  dismissKeyboard: vi.fn(),
  shell: undefined as unknown,
}));
vi.mock("react", async () => (await import("../../web/test/component-renderer")).React);
vi.mock(
  "@effect/atom-react",
  async () => (await import("../../web/test/component-renderer")).EffectAtomReact,
);
vi.mock("@react-navigation/native", async () => {
  const { React } = await import("../../web/test/component-renderer");
  return {
    useNavigation: () => host.navigation,
    useRoute: () => host.useRoute(),
    useFocusEffect: (callback: () => void | (() => void)) => {
      const focused = host.useFocused();
      React.useEffect(() => (focused ? callback() : undefined), [callback, focused]);
    },
    StackActions: {
      push: (name: string, params: object) => ({ type: "PUSH", payload: { name, params } }),
      replace: (name: string, params: object) => ({ type: "REPLACE", payload: { name, params } }),
    },
    CommonActions: { reset: (state: unknown) => ({ type: "RESET", payload: state }) },
    createPathConfigForStaticNavigation: () => ({}),
    getPathFromState: () => "/",
  };
});
vi.mock("@react-navigation/native-stack", () => ({
  createNativeStackScreen: (screen: unknown) => screen,
}));
vi.mock("./native/createV5StackNavigator", () => ({
  createV5StackNavigator: (config: unknown) => config,
  createV5SheetStackNavigator: (config: unknown) => config,
}));
vi.mock("./features/layout/createWorkspaceStackNavigator", () => ({
  createWorkspaceStackNavigator: (config: unknown) => ({ with: () => config }),
}));
vi.mock("react-native", () => ({
  Platform: { OS: "android", Version: 35 },
  View: "div",
  ScrollView: "article",
  Pressable: "button",
  StyleSheet: { create: (styles: unknown) => styles },
  BackHandler: {
    addEventListener: (_event: string, callback: () => boolean) => {
      host.back.add(callback);
      return { remove: () => host.back.delete(callback) };
    },
  },
  Keyboard: { isVisible: () => host.keyboard, dismiss: host.dismissKeyboard },
  Alert: { alert: vi.fn() },
}));
vi.mock("react-native-safe-area-context", () => ({
  useSafeAreaInsets: () => ({ top: 0, bottom: 0 }),
}));
vi.mock("uniwind", () => ({ useResolveClassNames: () => ({}) }));
vi.mock("expo-crypto", () => ({ randomUUID: () => "test-id" }));
vi.mock("./components/ScreenHeader", () => ({ ScreenHeader: "header" }));
vi.mock("./components/ScreenHeaderButton", () => ({ ScreenHeaderButton: "HeaderButton" }));
vi.mock("./components/AppText", () => ({ AppText: "span" }));
vi.mock("./components/CompactBrandTitle", () => ({ getCompactBrandHeaderOptions: () => ({}) }));
vi.mock("./components/EmptyState", () => ({ EmptyState: "aside" }));
vi.mock("./components/LoadingScreen", () => ({ LoadingScreen: "output" }));
vi.mock("./components/RenderErrorBoundary", () => ({
  RenderErrorBoundary: "Boundary",
  RenderFailureView: "Failure",
}));
vi.mock("./native/StackHeader", () => ({ nativeHeaderScrollEdgeEffects: () => ({}) }));
vi.mock("./native/sheet-surface", () => ({ FORM_SHEET_PRESENTATION_OPTIONS: {} }));
vi.mock("./native/native-glass", () => ({ NATIVE_LIQUID_GLASS_SUPPORTED: false }));
vi.mock("./native/native-layout-metrics", () => ({ useNativeLayoutMetrics: () => ({}) }));
vi.mock("./native/SelectableMarkdownText", () => ({ SelectableMarkdownText: "Markdown" }));
vi.mock("./native/useOpenPullRequest", () => ({ useOpenPullRequest: () => vi.fn() }));
vi.mock("./features/files/FileMarkdownPreview", () => ({ useMarkdownPreviewStyles: () => ({}) }));
vi.mock("./features/settings/appearance/AppearancePreferencesProvider", () => ({
  useAppearancePreferences: () => ({ themeVariables: {} }),
}));
vi.mock("./features/layout/AdaptiveWorkspaceLayout", () => ({
  AdaptiveWorkspaceLayout: "Layout",
  useAdaptiveWorkspaceLayout: () => ({
    layout: { usesSplitView: host.usesSplitView, variant: "compact" },
    panes: { auxiliaryPaneVisible: false },
    fileInspector: { supported: false },
  }),
  useRegisterWorkspaceInspector: () => {},
  useAdaptiveWorkspacePaneRole: () => {},
}));
vi.mock("./features/workspace/WorkspaceTabsContext", () => ({
  useWorkspaceTabs: () => ({ dispatch: host.selectTab }),
}));
vi.mock("./features/threads/useThreadHeaderOptions", () => ({
  useThreadHeaderOptions: () => ({ options: {} }),
}));
vi.mock("./features/threads/ThreadDetailScreen", () => ({ ThreadDetailScreen: "ThreadBody" }));
vi.mock("./features/threads/GitActionProgressOverlay", () => ({
  GitActionProgressOverlay: "GitProgress",
}));
vi.mock("./features/threads/thread-inspector-content-stack", () => ({
  ThreadInspectorContentStack: "Inspector",
}));
vi.mock("./features/files/thread-file-navigator-pane", () => ({
  ThreadFileNavigatorPane: "Files",
}));
vi.mock("./features/threads/use-worktree-setup", () => ({
  useWorktreeSetup: () => ({ snapshot: null, visible: null }),
}));
vi.mock("./features/prompt-links/useThreadPromptLink", () => ({ useThreadPromptLink: () => {} }));
vi.mock("./state/workspace", () => ({ useConnectionsReady: () => host.mode !== "loading" }));
vi.mock("./state/shell", () => ({
  useEnvironmentShellReadiness: () => ({ status: "ready", hasError: false }),
}));
vi.mock("./state/use-remote-environment-registry", () => ({
  useRemoteConnectionStatus: () => ({ connectionState: "available" }),
  useRemoteEnvironmentRuntime: () => ({ connectionState: "available" }),
  useRemoteConnections: () => ({ onReconnectEnvironment: vi.fn() }),
}));
vi.mock("./state/use-thread-selection", () => ({
  useThreadSelection: () => {
    const params = host.useRoute().params;
    return {
      selectedThread:
        host.mode === "loaded" ? { ...(host.shell as object), id: params.threadId } : null,
      selectedThreadCreation: null,
      selectedThreadProject: null,
      selectedEnvironmentConnection: null,
    };
  },
}));
vi.mock("./state/use-thread-detail", async () => {
  const Option = await import("effect/Option");
  return {
    useSelectedThreadDetailState: () => ({
      data: Option.none(),
      error: Option.none(),
      status: host.mode === "loading" ? "loading" : "ready",
      history: { hasMoreHistory: false, error: null },
    }),
  };
});
vi.mock("./state/use-thread-composer-state", () => ({
  useThreadComposerState: () => ({
    selectedThreadFeed: [],
    selectedThreadActivityRun: null,
    selectedThreadQueueCount: 0,
    activeWorkStartedAt: null,
  }),
}));
vi.mock("./state/use-selected-thread-worktree", () => ({
  useSelectedThreadWorktree: () => ({ selectedThreadCwd: null }),
}));
vi.mock("./state/use-selected-thread-git-state", () => ({ useSelectedThreadGitState: () => ({}) }));
vi.mock("./state/use-selected-thread-git-actions", () => ({
  useSelectedThreadGitActions: () => ({}),
}));
vi.mock("./state/use-selected-thread-requests", () => ({ useSelectedThreadRequests: () => ({}) }));
vi.mock("./state/use-terminal-session", () => ({
  useKnownTerminalSessions: () => ({ sessions: [] }),
}));
vi.mock("./state/use-vcs-action-state", () => ({
  useGitActionProgress: () => null,
  dismissGitActionResult: vi.fn(),
}));
vi.mock("./state/session", () => ({
  useEnvironmentScope: () => false,
  readEnvironmentScope: () => false,
}));
vi.mock("./state/query", () => ({ useEnvironmentQuery: () => ({ data: null }) }));
vi.mock("./state/vcs", () => ({ vcsEnvironment: {} }));
vi.mock("./state/use-atom-command", () => ({ useAtomCommand: () => vi.fn() }));
vi.mock("./state/threads", () => ({ threadEnvironment: {}, environmentThreadDetails: {} }));
vi.mock("./state/new-task-draft-key", () => ({ restoredNewTaskDraftKey: () => "" }));
vi.mock("./state/pending-thread-creation", () => ({ clearPendingThreadCreationOutcome: vi.fn() }));
vi.mock("./state/recover-failed-thread-draft", () => ({ recoverFailedThreadDraft: vi.fn() }));
vi.mock("./connection/runtime", () => ({ connectionAtomRuntime: {} }));
vi.mock("@t3tools/client-runtime/state/piArchitecture/queries", () => ({
  createPiArchitectureAtoms: () => ({ query: () => host.query }),
}));

// Other registered destinations are not rendered. Keep registration itself real.
vi.mock("./features/pull-requests/guide/PullRequestGuideScreen", () => ({
  PullRequestGuideStopScreen: "Other",
}));
vi.mock("./features/pull-requests/PullRequestInlineCommentScreen", () => ({
  PullRequestInlineCommentScreen: "Other",
}));
vi.mock("./features/pull-requests/PullRequestDiscussionScreen", () => ({
  PullRequestDiscussionScreen: "Other",
}));
vi.mock("./features/workspace/WorkspaceRootScreen", () => ({ WorkspaceRootScreen: "Home" }));
vi.mock("./features/workspace/AndroidWorkspaceTabsProvider", () => ({
  AndroidWorkspaceTabsProvider: "Other",
}));
vi.mock("./features/pull-requests/PullRequestRouteScreen", () => ({
  PullRequestRouteScreen: "Other",
  PullRequestFileRouteScreen: "Other",
  PullRequestFilesRouteScreen: "Other",
}));
vi.mock("./features/pull-requests/PullRequestReviewSheet", () => ({
  PullRequestReviewSheet: "Other",
}));
vi.mock("./features/pull-requests/PullRequestReviewersScreen", () => ({
  PullRequestReviewersScreen: "Other",
}));
vi.mock("./features/archive/ArchivedThreadsRouteScreen", () => ({
  ArchivedThreadsRouteScreen: "Other",
}));
vi.mock("./features/agent-awareness/notificationNavigation", () => ({
  useAgentNotificationNavigation: () => {},
}));
vi.mock("./features/cloud/ConnectOnboardingRouteScreen", () => ({
  ConnectOnboardingRouteScreen: "Other",
}));
vi.mock("./features/cloud/connectOnboardingNavigation", () => ({
  useConnectOnboardingNavigation: () => {},
}));
vi.mock("./features/files/AttachmentFileScreen", () => ({ AttachmentFileScreen: "Other" }));
vi.mock("./features/files/ThreadFilesRouteScreen", () => ({
  ThreadFilesTreeScreen: "Other",
  ThreadFileScreen: "Other",
}));
vi.mock("./features/pull-requests/pullRequestSelection", () => ({ selectedPullRequestKey: null }));
vi.mock("./features/keyboard/HardwareKeyboardCommandProvider", () => ({
  HardwareKeyboardCommandOverlay: "Other",
  HardwareKeyboardCommandProvider: "Other",
}));
vi.mock("./features/review/ReviewCommentComposerSheet", () => ({
  ReviewCommentComposerSheet: "Other",
}));
vi.mock("./features/review/ReviewSheet", () => ({ ReviewSheet: "Other" }));
vi.mock("./features/terminal/ThreadTerminalRouteScreen", () => ({
  ThreadTerminalRouteScreen: "Other",
}));
vi.mock("./features/devices/DevicePreviewRouteScreen", () => ({
  DevicePreviewRouteScreen: "Other",
}));
vi.mock("./features/browser/BrowserPreviewRouteScreen", () => ({
  BrowserPreviewRouteScreen: "Other",
}));
vi.mock("./features/threads/git/GitBranchesSheet", () => ({ GitBranchesSheet: "Other" }));
vi.mock("./features/threads/git/GitCommitSheet", () => ({ GitCommitSheet: "Other" }));
vi.mock("./features/threads/git/GitConfirmSheet", () => ({ GitConfirmSheet: "Other" }));
vi.mock("./features/threads/git/GitOverviewSheet", () => ({ GitOverviewSheet: "Other" }));
vi.mock("./features/threads/ThreadAgentsSheet", () => ({ ThreadAgentsSheet: "Other" }));
vi.mock("./features/threads/ThreadNativeWorkerDetailsSheet", () => ({
  ThreadNativeWorkerDetailsSheet: "Other",
}));
vi.mock("./features/threads/ThreadQueueControl", () => ({ ThreadQueueSheet: "Other" }));
vi.mock("./features/threads/McpAppFullscreenScreen", () => ({ McpAppFullscreenScreen: "Other" }));
vi.mock("./features/connection/ConnectionsRouteScreen", () => ({
  ConnectionsRouteScreen: "Other",
}));
vi.mock("./features/connection/ConnectionsNewRouteScreen", () => ({
  ConnectionsNewRouteScreen: "Other",
}));
vi.mock("./features/projects/AddProjectDestinationRoute", () => ({
  AddProjectDestinationRoute: "Other",
}));
vi.mock("./features/projects/AddProjectLocalRoute", () => ({ AddProjectLocalRoute: "Other" }));
vi.mock("./features/projects/AddProjectNewRoute", () => ({ AddProjectNewRoute: "Other" }));
vi.mock("./features/projects/AddProjectRepositoryRoute", () => ({
  AddProjectRepositoryRoute: "Other",
}));
vi.mock("./features/projects/AddProjectSourceRoute", () => ({ AddProjectSourceRoute: "Other" }));
vi.mock("./features/threads/NewTaskDraftRouteScreen", () => ({ NewTaskDraftRouteScreen: "Other" }));
vi.mock("./features/threads/NewTaskContextPickerScreens", () => ({
  NewTaskBranchPickerRouteScreen: "Other",
  NewTaskEnvironmentPickerRouteScreen: "Other",
}));
vi.mock("./features/threads/ThreadSettingsSheet", () => ({
  ExistingThreadSettingsRouteProvider: "Other",
  ExistingThreadSettingsRouteScreen: "Other",
  NewTaskThreadSettingsRouteScreen: "Other",
}));
vi.mock("./features/threads/new-task-flow-provider", () => ({ NewTaskFlowProvider: "Other" }));
vi.mock("./features/threads/NewTaskRouteScreen", () => ({ NewTaskRouteScreen: "Other" }));
vi.mock("./features/settings/SettingsAppearanceRouteScreen", () => ({
  SettingsAppearanceRouteScreen: "Other",
}));
vi.mock("./features/settings/SettingsClientStorageRouteScreen", () => ({
  SettingsClientStorageRouteScreen: "Other",
}));
vi.mock("./features/diagnostics/SettingsDiagnosticsRouteScreen", () => ({
  SettingsDiagnosticsRouteScreen: "Other",
}));
vi.mock("./features/settings/SettingsProviderAccountsRouteScreen", () => ({
  SettingsProviderAccountsRouteScreen: "Other",
}));
vi.mock("./features/settings/SettingsAuthRouteScreen", () => ({
  SettingsAuthRouteScreen: "Other",
}));
vi.mock("./features/settings/SettingsEnvironmentDetailRouteScreen", () => ({
  SettingsEnvironmentDetailRouteScreen: "Other",
}));
vi.mock("./features/settings/SettingsEnvironmentsRouteScreen", () => ({
  SettingsEnvironmentsRouteScreen: "Other",
}));
vi.mock("./features/settings/SettingsFollowUpRouteScreen", () => ({
  SettingsFollowUpRouteScreen: "Other",
}));
vi.mock("./features/settings/SettingsServerControlsRouteScreen", () => ({
  SettingsEnvironmentAgentBehaviorRouteScreen: "Other",
  SettingsEnvironmentMaintenanceRouteScreen: "Other",
  SettingsEnvironmentNewThreadsRouteScreen: "Other",
  SettingsEnvironmentSourceControlRouteScreen: "Other",
}));
vi.mock("./features/settings/SettingsScheduledTasksRouteScreen", () => ({
  SettingsScheduledTasksRouteScreen: "Other",
  SettingsScheduledTaskNewRouteScreen: "Other",
  SettingsScheduledTaskEditRouteScreen: "Other",
}));
vi.mock("./features/settings/ScheduledTaskPickerScreens", () => ({
  ScheduledTaskModelPickerRouteScreen: "Other",
  ScheduledTaskBranchPickerRouteScreen: "Other",
}));
vi.mock("./features/settings/scheduled-task-editor", () => ({
  ScheduledTaskEditorProvider: "Other",
}));
vi.mock("./features/settings/SettingsKeyboardRouteScreen", () => ({
  SettingsKeyboardRouteScreen: "Other",
}));
vi.mock("./features/settings/SettingsLegalRouteScreen", () => ({
  SettingsLegalRouteScreen: "Other",
}));
vi.mock("./features/settings/SettingsMicrophoneRouteScreen", () => ({
  SettingsMicrophoneRouteScreen: "Other",
}));
vi.mock("./features/settings/SettingsOpenSourceLicensesRouteScreen", () => ({
  SettingsOpenSourceLicenseRouteScreen: "Other",
  SettingsOpenSourceLicensesRouteScreen: "Other",
}));
vi.mock("./features/settings/SettingsProjectGroupingRouteScreen", () => ({
  SettingsProjectGroupingRouteScreen: "Other",
}));
vi.mock("./features/settings/SettingsProjectOverviewRouteScreen", () => ({
  SettingsProjectOverviewRouteScreen: "Other",
}));
vi.mock("./features/usage/UsageLimitsPooled", () => ({ UsageLimitAccountScreen: "Other" }));
vi.mock("./features/usage/UsageRouteScreen", () => ({ UsageRouteScreen: "Other" }));
vi.mock("./features/settings/SettingsAboutRouteScreen", () => ({
  SettingsAboutRouteScreen: "Other",
}));
vi.mock("./features/settings/SettingsNotificationsRouteScreen", () => ({
  SettingsNotificationsRouteScreen: "Other",
}));
vi.mock("./features/settings/SettingsRouteScreen", () => ({ SettingsRouteScreen: "Other" }));
vi.mock("./features/settings/SettingsThreadsRouteScreen", () => ({
  SettingsThreadsRouteScreen: "Other",
}));
vi.mock("./features/settings/settings-environment-filter", () => ({
  SettingsEnvironmentFilterProvider: "Other",
}));
vi.mock("./features/showcase/ShowcaseCaptureCoordinator", () => ({
  ShowcaseCaptureCoordinator: "Other",
}));
vi.mock("./features/settings/components/SettingsLegalDocumentRouteScreen", () => ({
  SettingsLegalDocumentCloseHeaderButton: "Other",
  SettingsLegalDocumentExternalHeaderButton: "Other",
}));
vi.mock("./features/shortcuts/useAppShortcuts", () => ({ useAppShortcuts: () => {} }));
vi.mock("./features/sharing/IncomingShareProvider", () => ({ useIncomingShare: () => {} }));
vi.mock("./state/use-thread-outbox-drain", () => ({ useThreadOutboxDrain: () => {} }));
vi.mock("./state/composer-attachment-uploads", () => ({
  useComposerAttachmentUploadWorker: () => {},
}));

import { RootStack } from "./Stack";
import { ThreadRouteScreen } from "./features/threads/ThreadRouteScreen";
import { ThreadArchitectTableScreen } from "./features/threads/ThreadArchitectTableScreen";

const requireNavigation = NodeModule.createRequire(
  NodeModule.createRequire(import.meta.url).resolve("@react-navigation/native/package.json"),
);
const { StackRouter, CommonActions } = requireNavigation("@react-navigation/routers") as {
  StackRouter: typeof import("@react-navigation/native").StackRouter;
  CommonActions: typeof import("@react-navigation/native").CommonActions;
};
const target = {
  environmentId: EnvironmentId.make("navigation-environment"),
  threadId: ThreadId.make("coordinator"),
  tableId: "navigation-table",
};
const RouteContext = React.createContext({
  key: "",
  name: "",
  params: {} as Record<string, unknown>,
});
function RegisteredRoute(props: {
  readonly route: StackNavigationState<ParamListBase>["routes"][number];
  readonly children: ReactNode;
}) {
  const value = React.useMemo(
    () => ({
      ...props.route,
      params: (props.route.params ?? {}) as Record<string, unknown>,
    }),
    [props.route],
  );
  return <RouteContext.Provider value={value}>{props.children}</RouteContext.Provider>;
}
let renderer: ReactTestRenderer | undefined;
let registry: AtomRegistry.AtomRegistry;
afterEach(async () => {
  await act(() => renderer?.unmount());
  renderer = undefined;
  registry?.dispose();
  host.back.clear();
  host.selectTab.mockClear();
  host.dismissKeyboard.mockClear();
  vi.unstubAllGlobals();
});

async function mountStack(usesSplitView = false) {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  registry = AtomRegistry.make();
  host.shell = presentThreadShell(target.environmentId, v2ThreadShell);
  const records: PiArchitectureReadResult = {
    kind: "records",
    next: null,
    summary: {
      ...target,
      providerInstanceId: v2ThreadShell.providerInstanceId,
      coordinatorSessionId: "coordinator-session",
      roomId: "room",
      mode: "table",
      participants: 1,
      lifecycle: {
        stage: "closing",
        round: 2,
        protocolState: "finished",
        outcome: "unavailable",
        closure: { state: "released", members: "released", director: "released" },
      },
      capture: "closed",
      observation: "current",
      gaps: [],
    },
    items: [
      {
        kind: "member",
        id: "specialist",
        assignmentId: "task",
        instanceId: "instance",
        roleId: "architect",
        function: "specialist",
        execution: "completed",
        ordinal: 1,
        mandateSource: "TeamAssignment.task",
        textId: "mandate",
        workerThreadId: ThreadId.make("participant"),
        bindingCount: 1,
      },
    ],
  };
  host.query = Atom.make(AsyncResult.success(records));
  host.mode = "loaded";
  host.keyboard = false;
  host.usesSplitView = usesSplitView;
  const config = RootStack as unknown as {
    screens: Record<
      string,
      { screen: typeof ThreadRouteScreen | typeof ThreadArchitectTableScreen }
    >;
  };
  expect(config.screens.Thread?.screen).toBe(ThreadRouteScreen);
  expect(config.screens.ThreadArchitectTable?.screen).toBe(ThreadArchitectTableScreen);
  const router = StackRouter({ initialRouteName: "Home" });
  const options = {
    routeNames: Object.keys(config.screens),
    routeParamList: {},
    routeGetIdList: {},
  };
  let stack: StackNavigationState<ParamListBase> = router.getInitialState(options);
  let redraw = () => {};
  const dispatch = (input: unknown) => {
    const action = typeof input === "function" ? input(stack) : input;
    const next = router.getStateForAction(stack, action, options);
    if (next) stack = router.getRehydratedState(next, options);
    redraw();
  };
  host.navigation = {
    dispatch,
    navigate: (name, params) => dispatch(CommonActions.navigate(name, params)),
    goBack: () => dispatch(CommonActions.goBack()),
    canGoBack: () => stack.index > 0,
  };
  host.useRoute = () => React.useContext(RouteContext);
  host.useFocused = () => host.useRoute().key === stack.routes[stack.index]?.key;
  function Harness() {
    const [, update] = React.useReducer((value: number) => value + 1, 0);
    React.useLayoutEffect(() => {
      redraw = update;
    }, []);
    return (
      <EffectAtomReact.RegistryContext.Provider value={registry}>
        {stack.routes.map((route) => {
          const Screen = config.screens[route.name]?.screen;
          return (
            <RegisteredRoute key={route.key} route={route}>
              <section data-route={route.key}>
                {route.name === "Thread" ? (
                  <ThreadRouteScreen
                    route={{
                      ...route,
                      params: route.params as { environmentId: string; threadId: string },
                    }}
                  />
                ) : route.name === "ThreadArchitectTable" && Screen ? (
                  <ThreadArchitectTableScreen route={{ ...route, params: target }} />
                ) : null}
              </section>
            </RegisteredRoute>
          );
        })}
      </EffectAtomReact.RegistryContext.Provider>
    );
  }
  await act(() => {
    renderer = create(<Harness />, {
      createNodeMock: () => ({ scrollTo: () => {} }),
    });
  });
  await act(() =>
    host.navigation.navigate("Thread", {
      environmentId: target.environmentId,
      threadId: target.threadId,
    }),
  );
  const coordinator = stack.routes.at(-1)!;
  const coordinatorReading: ThreadReadingPosition = {
    rowId: "history-row",
    offsetWithinRow: 21,
    scrollOffset: 840,
    follow: false,
    disclosures: {
      expandedWorkGroups: {},
      expandedWorkRows: {},
      expandedTurnIds: new Set(),
      workGroupScrollPositions: new Map(),
    },
  };
  rememberThreadReadingPosition(
    scopedThreadKey(target.environmentId, target.threadId),
    coordinatorReading,
  );
  await act(() =>
    host.navigation.dispatch({
      type: "PUSH",
      payload: { name: "ThreadArchitectTable", params: target },
    }),
  );
  const table = stack.routes.at(-1)!;
  await act(() =>
    registry.set(tablePresentationAtom(target), {
      ...registry.get(tablePresentationAtom(target)),
      tab: "Participants",
      source: true,
      expanded: { "Round 1": true },
      reading: { Participants: { id: "member:specialist", offset: 17, fallback: 217 } },
    }),
  );
  const tableSurface = renderer!.root.findByProps({ "data-route": table.key });
  const retained = registry.get(tablePresentationAtom(target));
  const active = () => renderer!.root.findByProps({ "data-route": stack.routes[stack.index]!.key });
  const participantButton = active()
    .findAllByType("button")
    .find((button) =>
      button
        .findAllByType("span")
        .some((text) => text.children.join("").includes("Read conversation")),
    );
  expect(participantButton).toBeDefined();
  await act(() => participantButton!.props.onPress());
  return {
    active,
    coordinator,
    table,
    tableSurface,
    coordinatorReading,
    state: () => stack,
    refresh: () => act(() => redraw()),
    retained,
    hardwareBack: () =>
      act(() => {
        const handled = [...host.back].toReversed().some((handler) => handler());
        if (!handled) host.navigation.goBack();
      }),
  };
}

it.each([
  "header Back",
  "split-view header Back",
  "Return-to-chat action",
  "hardware Back",
] as const)(
  "registered participant %s restores the table without a workspace reset",
  async (method) => {
    const stack = await mountStack(method === "split-view header Back");
    const before = stack.state().routes.slice(0, -1);
    expect(stack.state().routes.at(-1)?.params).toMatchObject({
      architectParticipant: true,
      threadId: "participant",
    });
    expect(host.back.size).toBe(0);
    host.selectTab.mockClear();
    if (method === "header Back" || method === "split-view header Back")
      await act(() => stack.active().findByType("header").props.onBack());
    else if (method === "Return-to-chat action") {
      const action = stack
        .active()
        .findByType("header")
        .props.actions.find(
          (item: { accessibilityLabel: string }) => item.accessibilityLabel === "Return to chat",
        );
      expect(action).toBeDefined();
      await act(() => action.onPress());
    } else await stack.hardwareBack();
    expect(stack.state().routes).toEqual(before);
    expect(stack.state().routes.at(-1)?.key).toBe(stack.table.key);
    expect(stack.active()).toBe(stack.tableSurface);
    expect(registry.get(tablePresentationAtom(target))).toEqual(stack.retained);
    expect(readThreadReadingPosition(scopedThreadKey(target.environmentId, target.threadId))).toBe(
      stack.coordinatorReading,
    );
    expect(host.selectTab).not.toHaveBeenCalled();
  },
);

it.each(["loading", "unavailable"] as const)(
  "participant fallback %s header returns to the table",
  async (mode) => {
    const stack = await mountStack();
    host.mode = mode;
    await stack.refresh();
    expect(stack.active().findAllByType(mode === "loading" ? "output" : "aside")).toHaveLength(1);
    const before = stack.state().routes.slice(0, -1);
    await act(() => stack.active().findByType("header").props.onBack());
    expect(stack.state().routes).toEqual(before);
    expect(registry.get(tablePresentationAtom(target))).toEqual(stack.retained);
  },
);

it("unrelated thread Android Back still dismisses keyboard, then returns to the workspace", async () => {
  const stack = await mountStack();
  await act(() => host.navigation.goBack());
  await act(() => host.navigation.goBack());
  expect(stack.state().routes.at(-1)?.key).toBe(stack.coordinator.key);
  expect(host.back.size).toBe(1);
  host.keyboard = true;
  await stack.hardwareBack();
  expect(host.dismissKeyboard).toHaveBeenCalledOnce();
  expect(stack.state().routes.at(-1)?.key).toBe(stack.coordinator.key);
  host.keyboard = false;
  await stack.hardwareBack();
  expect(stack.state().routes.map((route) => route.name)).toEqual(["Home"]);
});

it.each([false, true])(
  "unrelated loaded header preserves workspace Back policy with split view %s",
  async (usesSplitView) => {
    const stack = await mountStack(usesSplitView);
    await act(() => host.navigation.goBack());
    await act(() => host.navigation.goBack());
    expect(stack.state().routes.at(-1)?.key).toBe(stack.coordinator.key);
    const header = stack.active().findByType("header");
    host.selectTab.mockClear();
    if (usesSplitView) {
      expect(header.props.onBack).toBeUndefined();
      expect(stack.state().routes.map((route) => route.name)).toEqual(["Home", "Thread"]);
      await stack.hardwareBack();
    } else {
      await act(() => header.props.onBack());
    }
    expect(stack.state().routes.map((route) => route.name)).toEqual(["Home"]);
    expect(host.selectTab).toHaveBeenCalledWith({ type: "select", tab: "threads" });
  },
);
