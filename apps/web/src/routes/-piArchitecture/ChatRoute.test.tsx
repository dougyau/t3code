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
import { createRoot } from "react-dom/client";
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
  reads: [] as unknown[],
  rows: [] as readonly { id: string }[],
  scroll: 0,
  tableScroll: 0,
  frames: new Map<number, FrameRequestCallback>(),
  nextFrame: 0,
  texts: new Map<string, unknown>(),
}));
vi.mock("./state", () => ({
  piArchitecture: {
    query: (target: { input: { kind: string; textId?: string } }) => {
      host.reads.push(target);
      if (target.input.kind === "text" && target.input.textId)
        return host.texts.get(target.input.textId) ?? host.query;
      return host.query;
    },
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

function ChatSurface(props: {
  environmentId: EnvironmentId;
  threadId: ThreadId;
  mainContent?: ReactNode;
}) {
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
      {props.mainContent === undefined ? (
        <>
          <output aria-label="Draft">{draft.prompt}</output>
          <output aria-label="Unsent attachments">
            {draft.images.map((image) => image.name).join(",")}
          </output>
          <MessagesTimeline {...timeline} />
        </>
      ) : (
        props.mainContent
      )}
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
      .find(
        (node) =>
          node.props["aria-label"] === label || node.children.some((text) => text === label),
      );
    if (!button) throw new Error(`Missing button ${label}`);
    await act(async () => {
      await button.props.onClick(new MouseEvent("click"));
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
    expect(
      renderer!.root.findAllByProps({ "aria-label": "Collapse Architect participants" }),
    ).toHaveLength(0);
    expect(readTimelinePosition(scopedThreadKey(coordinator))?.scrollOffset).toBe(840);
    // The workspace owner survives, but its conversation (not a hidden composer)
    // is replaced by the table. Actual chrome is exercised by ChatView.workspace.test.
    expect(renderer!.root.findAllByProps({ "aria-label": `Chat ${threadId}` })).toHaveLength(1);
    expect(renderer!.root.findAllByType(MessagesTimeline)).toHaveLength(0);
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

it("keeps participant reads lazy and scoped across disclosure and permission changes", async () => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  const permission = Atom.make(false);
  host.permission = permission;
  host.query = Atom.make(AsyncResult.success(records));
  host.reads = [];
  const registry = AtomRegistry.make();
  const second: PiArchitectureSummary = {
    ...summary,
    tableId: "second-table",
    participants: 2,
    observation: "partial",
    capture: "incomplete",
    lifecycle: { ...summary.lifecycle, protocolState: "running" },
  };
  const root = createRootRoute({
    component: () => <ArchitectLineage environmentId={environmentId} tables={[summary, second]} />,
  });
  const router = createRouter({
    routeTree: root,
    history: createMemoryHistory({ initialEntries: ["/"] }),
  });
  let renderer: ReactTestRenderer | undefined;
  const buttons = (label: string) =>
    renderer!.root.findAllByType("button").filter((node) => node.props["aria-label"] === label);
  const content = () => JSON.stringify(renderer!.toJSON());
  const toggle = async (label: string) => {
    await act(() => buttons(label)[0]!.props.onClick(new MouseEvent("click")));
  };
  try {
    await act(async () => {
      await router.load();
      renderer = create(
        <RegistryContext.Provider value={registry}>
          <RouterProvider router={router} />
        </RegistryContext.Provider>,
      );
    });
    expect(buttons("Open Architect table").every((button) => button.props.disabled)).toBe(true);
    expect(buttons("Expand Architect participants").every((button) => button.props.disabled)).toBe(
      true,
    );
    expect(host.reads).toEqual([]);
    const counts = renderer!.root.findAllByType("span").map((node) => node.children.join(""));
    expect(counts).toContain("1 participant · Round 2");
    expect(counts).toContain("2 participants · Round 2");
    expect(content()).toContain("Finished");
    expect(content()).toContain("Result unavailable");
    expect(content()).toContain("Last known: Running");
    expect(content()).toContain("Partial view");
    expect(content()).toContain("Only part of this table was observed.");
    expect(content()).toContain("Capture is incomplete.");
    expect(content()).not.toContain("Specialist · architect");
    await act(() => registry.set(permission, true));
    expect(buttons("Open Architect table").every((button) => !button.props.disabled)).toBe(true);
    expect(host.reads).toEqual([]);

    await toggle("Expand Architect participants");
    expect(router.state.location.pathname).toBe("/");
    expect(content()).toContain("Specialist · architect · #1");
    expect(content()).toContain("Completed");
    expect(buttons("Collapse Architect participants")[0]!.props["aria-expanded"]).toBe(true);
    expect(host.reads.length).toBeGreaterThan(0);
    expect(host.reads).toEqual(
      expect.arrayContaining([
        { environmentId, input: { kind: "records", threadId, tableId: summary.tableId } },
      ]),
    );
    for (const read of host.reads) {
      expect(read).toEqual({
        environmentId,
        input: { kind: "records", threadId, tableId: summary.tableId },
      });
    }
    await toggle("Collapse Architect participants");
    expect(content()).not.toContain("Specialist · architect");
    host.reads = [];
    await toggle("Expand Architect participants");
    expect(content()).toContain("Specialist · architect · #1");

    await act(() => registry.set(permission, false));
    expect(content()).not.toContain("Specialist · architect");
    host.reads = [];
    expect(buttons("Collapse Architect participants")[0]!.props.disabled).toBe(true);
    await act(() => registry.set(permission, true));
    expect(content()).toContain("Specialist · architect · #1");
    expect(host.reads).toEqual(
      expect.arrayContaining([
        { environmentId, input: { kind: "records", threadId, tableId: summary.tableId } },
      ]),
    );
  } finally {
    await act(() => renderer?.unmount());
    registry.dispose();
    vi.unstubAllGlobals();
  }
});

it("retains full contributions and reading in the mounted route as publications arrive", async () => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  Object.defineProperty(window, "matchMedia", {
    configurable: true,
    value: () => ({ matches: false, addEventListener() {}, removeEventListener() {} }),
  });
  const recordAtom = Atom.make(AsyncResult.success(records));
  host.query = recordAtom;
  host.permission = Atom.make(true);
  const excerpt = "Exact proposal " + "🧠".repeat(584) + "👩🏽‍💻";
  const proposal = excerpt + "Café e\u0301 中文 👩🏽‍💻 ".repeat(110) + "FULL END 🦉";
  const captured = (textId: string, proposal: string, complete = true) =>
    Atom.make(
      AsyncResult.success<PiArchitectureReadResult>({
        kind: "text",
        textId,
        text: JSON.stringify({
          body: {
            kind: "contribution",
            proposal,
            arguments: ["Secondary section [original link](https://example.com/source)"],
            objections: [],
            doubts: [{ question: "Original question?", purpose: "Original purpose 🔎" }],
          },
        }),
        source: "native publication",
        format: "json",
        complete,
        revision: "revision",
        offset: 0,
        nextOffset: null,
      }),
    );
  host.texts.set("publication-2", captured("publication-2", proposal, false));
  host.texts.set("publication-3", captured("publication-3", "New short contribution"));
  host.texts.set("publication-4", captured("publication-4", "Another short contribution"));
  const registry = AtomRegistry.make();
  registry.set(tablePresentationAtom(target), {
    ...registry.get(tablePresentationAtom(target)),
    tab: "Debate",
  });
  const root = createRootRoute();
  const layout = createRoute({
    getParentRoute: () => root,
    id: "_chat",
    component: ThreadRouteOutlet,
  });
  const table = createRoute({
    getParentRoute: () => layout,
    path: "architect/$environmentId/$threadId/$tableId",
    ...(tableRoute.options.component && { component: tableRoute.options.component }),
    ...(tableRoute.options.validateSearch && { validateSearch: tableRoute.options.validateSearch }),
  });
  const router = createRouter({
    routeTree: root.addChildren([layout.addChildren([table])]),
    history: createMemoryHistory({
      initialEntries: [`/architect/${environmentId}/${threadId}/${target.tableId}`],
    }),
  });
  const container = document.createElement("div");
  document.body.append(container);
  const mounted = createRoot(container);
  const click = async (scope: ParentNode, label: string) => {
    const button = [...scope.querySelectorAll("button")].find(
      (button) => button.textContent === label,
    );
    if (!button) throw new Error(`Missing button: ${label}`);
    await act(async () => button.click());
  };
  try {
    await act(async () => {
      await router.load();
      mounted.render(
        <RegistryContext.Provider value={registry}>
          <RouterProvider router={router} />
        </RegistryContext.Provider>,
      );
    });
    const article = container.querySelector<HTMLElement>("#publication-message-2")!;
    const pane = container.querySelector<HTMLElement>("#architect-reading-pane")!;
    const toolbar = container.querySelector('[aria-label="Debate actions"]')!;
    expect([...toolbar.querySelectorAll("button")].map((button) => button.textContent)).toEqual([
      "Read original brief",
      "Go to latest contributions",
    ]);
    expect(article.textContent).toContain("Specialist · architect · #1 · Contribution");
    expect(article.textContent).toContain("Partial native capture");
    expect(article.textContent).toContain("Contribution excerpt · Proposal");
    expect(article.querySelector("h4")?.nextElementSibling?.textContent).toBe(excerpt + "…");
    expect(article.textContent).not.toContain("FULL END 🦉");
    expect(article.querySelectorAll("a")).toHaveLength(0);
    const technical = article.querySelector("details")!;
    expect(technical.open).toBe(false);
    expect(article.textContent).not.toContain("snapshot-2");
    await act(async () => {
      technical.open = true;
      technical.dispatchEvent(new Event("toggle"));
    });
    expect(technical.textContent).toContain("Snapshot snapshot-2");
    expect(technical.textContent).toContain("Member ID: specialist");
    await click(article, "Read full contribution");
    expect(article.textContent).toContain(proposal);
    expect(article.textContent).toContain("Original question?");
    expect(article.textContent).toContain("Original purpose 🔎");
    expect(article.querySelector("a")?.getAttribute("href")).toBe("https://example.com/source");
    expect(article.textContent).not.toContain('"body"');

    // Geometry is a deterministic host boundary; this checks state/scroll retention, not pixels.
    for (const anchor of pane.querySelectorAll<HTMLElement>("[data-reading-id]"))
      anchor.getBoundingClientRect = () => ({ top: 200 }) as DOMRect;
    article.getBoundingClientRect = () => ({ top: -45 }) as DOMRect;
    pane.scrollTop = 270;
    await act(async () => pane.dispatchEvent(new Event("scroll", { bubbles: true })));
    const reading = registry.get(tablePresentationAtom(target)).reading.Debate;
    expect(reading).toMatchObject({ id: "publication:message-2", offset: 45, fallback: 270 });
    if (records.kind !== "records") throw new Error("Expected records fixture");
    const prior = records.items.find((record) => record.kind === "publication")!;
    if (prior.kind !== "publication") throw new Error("Expected publication fixture");
    const arrival = (round: number) => ({
      ...prior,
      id: `round-${round}`,
      ordinal: round,
      round,
      messageId: `message-${round}`,
      textId: `publication-${round}`,
    });
    await act(async () =>
      registry.set(
        recordAtom,
        AsyncResult.success({ ...records, items: [...records.items, arrival(3)] }),
      ),
    );
    expect(container.querySelector("#publication-message-2")).toBe(article);
    expect(container.querySelector("#architect-reading-pane")).toBe(pane);
    expect(article.textContent).toContain(proposal);
    expect(container.textContent).toContain("New short contribution");
    expect(container.querySelector("#publication-message-3")?.textContent).not.toContain(
      "Read full contribution",
    );
    expect(pane.scrollTop).toBe(270);
    expect(registry.get(tablePresentationAtom(target)).reading.Debate).toEqual(reading);
    await click(article, "Show less");
    expect(article.textContent).not.toContain("FULL END 🦉");
    await act(async () =>
      registry.set(
        recordAtom,
        AsyncResult.success({ ...records, items: [...records.items, arrival(3), arrival(4)] }),
      ),
    );
    expect(container.textContent).toContain("Another short contribution");
    expect(article.textContent).toContain("Contribution excerpt · Proposal");
    expect(article.textContent).not.toContain("FULL END 🦉");
    expect(pane.scrollTop).toBe(270);
    expect(registry.get(tablePresentationAtom(target)).reading.Debate).toEqual(reading);
    await click(article, "Read full contribution");
    expect(article.textContent).toContain(proposal);
  } finally {
    await act(async () => mounted.unmount());
    container.remove();
    registry.dispose();
    host.texts.clear();
    vi.unstubAllGlobals();
  }
});
