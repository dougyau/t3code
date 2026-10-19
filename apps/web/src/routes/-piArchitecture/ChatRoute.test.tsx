// @vitest-environment jsdom

import { RegistryContext } from "@effect/atom-react";
import { scopeThreadRef, scopedThreadKey } from "@t3tools/client-runtime/environment";
import { tablePresentationAtom } from "@t3tools/client-runtime/state/piArchitecture/model";
import {
  EnvironmentId,
  MessageId,
  ProviderInstanceId,
  ThreadId,
  type PiArchitectureReadResult,
  type PiArchitectureSummary,
} from "@t3tools/contracts";
import {
  createMemoryHistory,
  createRootRoute,
  createRoute,
  createRouter,
  RouterProvider,
} from "@tanstack/react-router";
import { AsyncResult, Atom, AtomRegistry } from "effect/reactivity";
import {
  act,
  Fragment,
  useImperativeHandle,
  useRef,
  type ComponentProps,
  type ReactNode,
  type Ref,
} from "react";
import { create, type ReactTestRenderer } from "react-test-renderer";
import type { LegendListRef } from "@legendapp/list/react";
import { expect, it, vi } from "vite-plus/test";
import { useComposerDraftStore, useComposerThreadDraft } from "../../composerDraftStore";
import { ThreadRouteOutlet } from "../../components/ThreadRouteView";
import { MessagesTimeline } from "../../components/chat/MessagesTimeline";
import { readTimelinePosition } from "../../components/chat/timelineScrollAnchoring";
import { ArchitectLineage } from "./ArchitectLineage";
import { Route as tableRoute } from "../_chat.architect.$environmentId.$threadId.$tableId";

const host = vi.hoisted(() => ({
  query: undefined as unknown,
  permission: undefined as unknown,
  rows: [] as readonly { id: string }[],
  scroll: 0,
  tableScroll: 0,
  frames: new Map<number, FrameRequestCallback>(),
  nextFrame: 0,
}));
vi.mock("./state", () => ({
  piArchitecture: {
    query: () => host.query,
    read: { permissionAtom: () => host.permission },
  },
}));
// The server shell is a seam. Keep the actual route owner, draft store,
// table presentation atoms and timeline restoration; omit unrelated chat chrome.
vi.mock("../../state/entities", async (original) => ({
  ...(await original<typeof import("../../state/entities")>()),
  useThreadRefs: () => [],
  useEnvironmentThreadRefs: () => [],
  useThreadShell: () => ({ latestRun: null, itemCount: 1, deletedAt: null }),
}));
vi.mock("../../state/query", async (original) => ({
  ...(await original<typeof import("../../state/query")>()),
  useEnvironmentQuery: () => ({ data: { status: "ready" } }),
}));
vi.mock("../../components/ui/sidebar", () => ({
  SidebarInset: ({ children }: { children: ReactNode }) => children,
}));
vi.mock("../../components/ChatView", () => ({
  default: ChatSurface,
}));
vi.mock("../../components/DiffWorkerPoolProvider", () => ({
  DiffWorkerPoolProvider: ({ children }: { children: ReactNode }) => children,
}));
vi.mock("@legendapp/list/react", () => ({
  LegendList: (props: {
    ref: Ref<LegendListRef>;
    data: readonly { id: string }[];
    renderItem: (input: { item: { id: string } }) => ReactNode;
  }) => {
    useImperativeHandle(props.ref, timelineHost, []);
    host.rows = props.data;
    return props.data.map((item) => (
      <Fragment key={item.id}>{props.renderItem({ item })}</Fragment>
    ));
  },
}));

const environmentId = EnvironmentId.make("navigation-environment");
const threadId = ThreadId.make("coordinator");
const childId = ThreadId.make("participant");
const target = { environmentId, threadId, tableId: "navigation-table" };
const coordinator = scopeThreadRef(environmentId, threadId);
const summary: PiArchitectureSummary = {
  tableId: target.tableId,
  threadId,
  providerInstanceId: ProviderInstanceId.make("pi"),
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
};
const records: PiArchitectureReadResult = {
  kind: "records",
  summary,
  next: null,
  items: [
    ...[1, 2].map((round) => ({
      kind: "publication" as const,
      id: `round-${round}`,
      ordinal: round,
      memberId: "specialist",
      assignmentId: "task",
      stage: "contribution" as const,
      round,
      turnId: `turn-${round}`,
      attemptId: `attempt-${round}`,
      snapshotId: `snapshot-${round}`,
      messageId: `message-${round}`,
      textId: `publication-${round}`,
    })),
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
      workerThreadId: childId,
      bindingCount: 1,
    },
  ],
};

function timelineHost(): LegendListRef {
  host.scroll = 0;
  return {
    getScrollableNode: () => ({
      get scrollTop() {
        return host.scroll;
      },
      scrollHeight: 2000,
      clientHeight: 600,
      addEventListener() {},
      removeEventListener() {},
      ownerDocument: { addEventListener() {}, removeEventListener() {} },
      getBoundingClientRect: () => ({ top: 0 }),
    }),
    getState: () => ({
      data: host.rows,
      scroll: host.scroll,
      scrollLength: 600,
      contentLength: 2000,
      indexByKey: () => 0,
      positionAtIndex: () => 0,
      sizeAtIndex: () => 2000,
      elementAtIndex: () => ({ getBoundingClientRect: () => ({ top: -host.scroll }) }),
    }),
    scrollToIndex: async ({ viewOffset }: { viewOffset: number }) => {
      host.scroll = -viewOffset;
    },
    scrollToOffset: async ({ offset }: { offset: number }) => {
      host.scroll = offset;
    },
  } as unknown as LegendListRef;
}

function ChatSurface(props: { environmentId: EnvironmentId; threadId: ThreadId }) {
  const ref = scopeThreadRef(props.environmentId, props.threadId);
  const draft = useComposerThreadDraft(ref);
  const listRef = useRef<LegendListRef | null>(null);
  const timeline: ComponentProps<typeof MessagesTimeline> = {
    listRef,
    routeThreadKey: scopedThreadKey(ref),
    timelineEntries: [
      {
        id: "prompt",
        kind: "message",
        createdAt: "2026-10-19T00:00:00Z",
        message: {
          id: MessageId.make("prompt"),
          role: "user",
          text: "Coordinator history",
          createdAt: "2026-10-19T00:00:00Z",
          updatedAt: "2026-10-19T00:00:00Z",
          runId: null,
          streaming: false,
        },
      },
    ],
    isWorking: false,
    activeTurnInProgress: false,
    latestRun: null,
    turnDiffSummaries: [],
    providerStatuses: [],
    runs: [],
    onOpenTurnDiff: () => {},
    onOpenThread: () => {},
    onForkFromRun: async () => {},
    onRollbackCheckpoint: () => {},
    supportsConversationRollback: false,
    onRevertToTurnCount: () => {},
    isRevertingCheckpoint: false,
    onImageExpand: () => {},
    activeThreadEnvironmentId: props.environmentId,
    markdownCwd: undefined,
    resolvedTheme: "light",
    timestampFormat: "locale",
    workspaceRoot: undefined,
    anchorMessageId: null,
    onAnchorReady: () => {},
    onAnchorSizeChanged: () => {},
    contentInsetEndAdjustment: 0,
    liveFollowEnabled: false,
    onIsAtEndChange: () => {},
    onManualNavigation: () => {},
  };
  return (
    <section aria-label={`Chat ${props.threadId}`}>
      <output aria-label="Draft">{draft.prompt}</output>
      <output aria-label="Unsent attachments">
        {draft.images.map((image) => image.name).join(",")}
      </output>
      <MessagesTimeline {...timeline} />
      {props.threadId === threadId ? (
        <ArchitectLineage environmentId={environmentId} tables={[summary]} />
      ) : null}
    </section>
  );
}

it("restores coordinator and table state across production route and participant swaps", async () => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  vi.stubGlobal("requestAnimationFrame", (callback: FrameRequestCallback) => {
    host.frames.set(++host.nextFrame, callback);
    return host.nextFrame;
  });
  vi.stubGlobal("cancelAnimationFrame", (id: number) => host.frames.delete(id));
  Object.defineProperty(window, "matchMedia", {
    configurable: true,
    value: () => ({ matches: false, addEventListener() {}, removeEventListener() {} }),
  });
  host.query = Atom.make(AsyncResult.success(records));
  host.permission = Atom.make(true);
  const registry = AtomRegistry.make();
  const root = createRootRoute();
  const layout = createRoute({
    getParentRoute: () => root,
    id: "_chat",
    component: ThreadRouteOutlet,
  });
  const chat = createRoute({
    getParentRoute: () => layout,
    path: "$environmentId/$threadId",
  });
  const table = createRoute({
    getParentRoute: () => layout,
    path: "architect/$environmentId/$threadId/$tableId",
    ...(tableRoute.options.component && { component: tableRoute.options.component }),
    ...(tableRoute.options.validateSearch && { validateSearch: tableRoute.options.validateSearch }),
  });
  const router = createRouter({
    routeTree: root.addChildren([layout.addChildren([chat, table])]),
    history: createMemoryHistory({ initialEntries: [`/${environmentId}/${threadId}`] }),
  });
  const store = useComposerDraftStore.getState();
  store.setPrompt(coordinator, "Actual unsent coordinator draft 🧭");
  store.addImage(coordinator, {
    id: "unsent-image",
    type: "image",
    name: "diagram.png",
    mimeType: "image/png",
    sizeBytes: 5,
    file: new File(["image"], "diagram.png", { type: "image/png" }),
    previewUrl: "blob:offline-diagram",
  });
  host.scroll = 840;
  let renderer: ReactTestRenderer | undefined;
  const flush = async () =>
    act(() => {
      const frames = [...host.frames.values()];
      host.frames.clear();
      frames.forEach((frame) => frame(0));
    });
  const press = async (label: string) => {
    const button = renderer!.root
      .findAllByType("button")
      .find((node) => node.children.some((text) => text === label));
    if (!button) throw new Error(`Missing button ${label}`);
    await act(async () => {
      await button.props.onClick();
    });
    await act(async () => {
      await router.load();
    });
  };
  try {
    await act(async () => {
      await router.load();
      renderer = create(
        <RegistryContext.Provider value={registry}>
          <RouterProvider router={router} />
        </RegistryContext.Provider>,
        {
          createNodeMock: (element) => {
            if (
              !(
                typeof element.props === "object" &&
                element.props !== null &&
                "id" in element.props &&
                element.props.id === "architect-reading-pane"
              )
            )
              return null;
            host.tableScroll = 0;
            return {
              get scrollTop() {
                return host.tableScroll;
              },
              set scrollTop(value: number) {
                host.tableScroll = value;
              },
              getBoundingClientRect: () => ({ top: 0 }),
              querySelectorAll: () => [
                {
                  dataset: { readingId: "member:specialist" },
                  getBoundingClientRect: () => ({ top: 200 - host.tableScroll }),
                },
              ],
            };
          },
        },
      );
    });
    await flush();
    host.scroll = 840;
    await press("Open Architect table");
    expect(readTimelinePosition(scopedThreadKey(coordinator))?.scrollOffset).toBe(840);
    expect(renderer!.root.findAllByProps({ "aria-label": `Chat ${threadId}` })).toHaveLength(0);
    // The table is not another thread; its identity and reading state live in the
    // existing environment/thread/table-scoped owner.
    await act(() =>
      registry.set(tablePresentationAtom(target), {
        ...registry.get(tablePresentationAtom(target)),
        tab: "Participants",
        source: true,
        expanded: { "Round 1": true, "Round 2": true },
        reading: { Participants: { id: "member:specialist", offset: 17, fallback: 217 } },
      }),
    );
    const saved = registry.get(tablePresentationAtom(target));
    const conversation = renderer!.root
      .findAllByType("button")
      .find((node) => node.children.join("") === "Read conversation: architect · specialist");
    expect(conversation).toBeDefined();
    await act(async () => {
      await conversation!.props.onClick();
      await router.load();
    });
    expect(router.state.location.search).toEqual({ participant: "specialist" });
    expect(renderer!.root.findAllByProps({ "aria-label": `Chat ${childId}` })).toHaveLength(1);
    await press("Back to Architect table");
    expect(registry.get(tablePresentationAtom(target))).toEqual(saved);
    expect(host.tableScroll).toBe(217);
    await press("Return to coordinator");
    await flush();
    await flush();
    expect(renderer!.root.findByProps({ "aria-label": "Draft" }).children).toEqual([
      "Actual unsent coordinator draft 🧭",
    ]);
    expect(renderer!.root.findByProps({ "aria-label": "Unsent attachments" }).children).toEqual([
      "diagram.png",
    ]);
    expect(host.scroll).toBe(840);
    expect(store.getComposerDraft(coordinator)?.images[0]?.file.name).toBe("diagram.png");
  } finally {
    await act(() => renderer?.unmount());
    registry.dispose();
    vi.unstubAllGlobals();
  }
}, 120_000);
