// @vitest-environment jsdom

import { RegistryContext } from "@effect/atom-react";
import { scopeThreadRef, scopedThreadKey } from "@t3tools/client-runtime/environment";
import {
  EnvironmentId,
  ProviderInstanceId,
  ThreadId,
  type PiArchitectureSummary,
} from "@t3tools/contracts";
import {
  createMemoryHistory,
  createRootRoute,
  createRoute,
  createRouter,
  RouterProvider,
} from "@tanstack/react-router";
import { Atom, AtomRegistry, AsyncResult } from "effect/reactivity";
import { act } from "react";
import { createRoot } from "react-dom/client";
import { expect, it, vi } from "vite-plus/test";
import { ThreadRouteOutlet } from "./ThreadRouteView";
import { Route as tableRoute } from "../routes/_chat.architect.$environmentId.$threadId.$tableId";
import { makeThreadFixture, makeThreadProjectionFixture } from "../test-fixtures";
import { useRightPanelStore } from "../rightPanelStore";
import { useComposerDraftStore } from "../composerDraftStore";

const host = vi.hoisted(() => ({
  shell: undefined as unknown,
  projection: undefined as unknown,
  records: undefined as unknown,
  permission: undefined as unknown,
  empty: [],
  query: { data: null },
  sidebar: { state: "expanded", isMobile: false, open: true },
}));
// Substitute connection/query data and missing browser measurements, not the
// workspace, layout controls, details card, Lineage, composer or their stores.
vi.mock("../state/environments", async (original) => ({
  ...(await original<typeof import("../state/environments")>()),
  useEnvironments: () => ({ environments: host.empty }),
}));
vi.mock("./chat/useAutoBalanceUpdateBanner", () => ({
  useAutoBalanceUpdateBanner: () => null,
}));
vi.mock("../hooks/useLoadBalancedEnvironment", () => ({
  useLoadBalancedEnvironment: () => ({ environmentId: null, pending: false }),
}));
vi.mock("../lib/observeResize", () => ({
  observeResize: () => () => {},
}));
vi.mock("../state/entities", async (original) => ({
  ...(await original<typeof import("../state/entities")>()),
  useThreadRefs: () => host.empty,
  useEnvironmentThreadRefs: () => host.empty,
  useThreadShell: () => host.shell,
  useThreadProjection: () => host.projection,
  useThreadVisibleTurnItems: () => host.empty,
  useThreadStatus: () => "live",
  useProject: () => null,
  useProjects: () => host.empty,
}));
vi.mock("../state/query", async (original) => ({
  ...(await original<typeof import("../state/query")>()),
  useEnvironmentQuery: () => host.query,
}));
vi.mock("../routes/-piArchitecture/state", () => ({
  piArchitecture: {
    query: () => host.records,
    read: { permissionAtom: () => host.permission },
  },
}));
vi.mock("./ui/sidebar", async (original) => ({
  ...(await original<typeof import("./ui/sidebar")>()),
  SidebarInset: ({ children }: { children: React.ReactNode }) => children,
  useSidebar: () => host.sidebar,
}));

it("keeps the real workspace details control and card across coordinator/table navigation", async () => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  vi.spyOn(window, "scrollTo").mockImplementation(() => {});
  Object.defineProperty(document, "fonts", { configurable: true, value: new EventTarget() });
  Object.defineProperty(Range.prototype, "getBoundingClientRect", {
    configurable: true,
    value: () => new DOMRect(0, 0, 100, 20),
  });
  Object.defineProperty(Range.prototype, "getClientRects", {
    configurable: true,
    value: () => [],
  });
  Object.defineProperty(window, "matchMedia", {
    configurable: true,
    value: () => ({ matches: false, addEventListener() {}, removeEventListener() {} }),
  });
  vi.spyOn(HTMLElement.prototype, "clientWidth", "get").mockReturnValue(1800);
  vi.spyOn(HTMLElement.prototype, "clientHeight", "get").mockReturnValue(1000);
  vi.spyOn(HTMLElement.prototype, "offsetHeight", "get").mockImplementation(function (
    this: HTMLElement,
  ) {
    return this.hasAttribute("data-thread-details-card") ? 1000 : 260;
  });
  vi.spyOn(HTMLElement.prototype, "getBoundingClientRect").mockReturnValue(
    new DOMRect(0, 0, 1800, 1000),
  );
  const computedStyle = window.getComputedStyle;
  vi.spyOn(window, "getComputedStyle").mockImplementation((element) =>
    element.className.toString().includes("min-w-[40rem]")
      ? ({ paddingLeft: "48px", width: "768px", minWidth: "640px" } as CSSStyleDeclaration)
      : computedStyle(element),
  );
  const environmentId = EnvironmentId.make("workspace-environment");
  const threadId = ThreadId.make("workspace-coordinator");
  const ref = scopeThreadRef(environmentId, threadId);
  host.shell = makeThreadFixture({ environmentId, id: threadId, title: "Coordinator workspace" });
  const projection = makeThreadProjectionFixture();
  const summary: PiArchitectureSummary = {
    tableId: "workspace-table",
    threadId,
    providerInstanceId: ProviderInstanceId.make("pi"),
    coordinatorSessionId: "session",
    roomId: "room",
    mode: "table",
    participants: 0,
    lifecycle: {
      stage: "closing",
      round: 1,
      protocolState: "finished",
      outcome: "available",
      closure: { state: "released", members: "released", director: "released" },
    },
    observation: "stale",
    capture: "closed",
    gaps: [],
  };
  host.projection = {
    projection: {
      ...projection,
      thread: {
        ...projection.thread,
        id: threadId,
        title: "Coordinator workspace",
        nativeTables: [summary],
      },
    },
  };
  host.records = Atom.make(
    AsyncResult.success({
      kind: "records",
      summary,
      items: [],
      next: null,
    }),
  );
  const permission = Atom.make(true);
  host.permission = permission;
  const registry = AtomRegistry.make();
  const file = new File(["image"], "diagram.png", { type: "image/png" });
  useComposerDraftStore.getState().setPrompt(ref, "Unsent coordinator plan");
  useComposerDraftStore.getState().addImage(ref, {
    id: "workspace-image",
    type: "image",
    name: file.name,
    mimeType: file.type,
    sizeBytes: file.size,
    file,
    previewUrl: "blob:workspace-image",
  });
  const rootRoute = createRootRoute();
  const layout = createRoute({
    getParentRoute: () => rootRoute,
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
    // Keep this minimal router host's coordinator params stable across child updates.
    defaultStructuralSharing: true,
    routeTree: rootRoute.addChildren([layout.addChildren([chat, table])]),
    history: createMemoryHistory({
      initialEntries: [`/architect/${environmentId}/${threadId}/workspace-table`],
    }),
  });
  const container = document.createElement("div");
  document.body.append(container);
  const root = createRoot(container);
  const pressDetails = async () => {
    const button = container.querySelector<HTMLButtonElement>(
      '[aria-label="Toggle thread details panel"]',
    );
    if (!button) throw new Error("Missing workspace details control");
    await act(() => button.click());
  };
  try {
    await act(async () => {
      await router.load();
      root.render(
        <RegistryContext.Provider value={registry}>
          <RouterProvider router={router} />
        </RegistryContext.Provider>,
      );
    });
    expect(container.textContent).toContain("Coordinator workspace");
    expect(container.textContent).toContain("Architect table");
    expect(container.querySelector("[data-chat-header]")).not.toBeNull();
    expect(container.querySelector('aside[aria-label="Thread details"]')).not.toBeNull();
    await pressDetails();
    expect(container.querySelector('aside[aria-label="Thread details"]')).toBeNull();
    expect(
      useRightPanelStore.getState().threadPanelVisibilityByThreadKey[scopedThreadKey(ref)]
        ?.inlineOpen,
    ).toBe(false);
    await pressDetails();
    expect(container.querySelector('aside[aria-label="Thread details"]')).not.toBeNull();
    await act(async () => {
      await router.navigate({ to: `/${environmentId}/${threadId}` });
    });
    expect(container.querySelector('[aria-label="Architect table"]')).toBeNull();
    expect(container.querySelector('aside[aria-label="Thread details"]')).not.toBeNull();
    expect(useComposerDraftStore.getState().getComposerDraft(ref)?.prompt).toBe(
      "Unsent coordinator plan",
    );
    expect(useComposerDraftStore.getState().getComposerDraft(ref)?.images[0]?.file).toBe(file);
    expect(container.querySelector('[contenteditable="true"]')?.textContent).toContain(
      "Unsent coordinator plan",
    );
    const openTable = container.querySelector<HTMLButtonElement>(
      'button[aria-label="Open Architect table"]',
    );
    if (!openTable) throw new Error("Missing actual Lineage table action");
    const coordinatorPath = router.state.location.pathname;
    await act(() => registry.set(permission, false));
    await act(() => openTable.click());
    expect(router.state.location.pathname).toBe(coordinatorPath);
    expect(container.querySelector('[contenteditable="true"]')).not.toBeNull();
    await act(() => registry.set(permission, true));
    const participants = container.querySelector<HTMLButtonElement>(
      'button[aria-label="Expand Architect participants"]',
    );
    if (!participants) throw new Error("Missing independent participant disclosure");
    participants.focus();
    expect(document.activeElement).toBe(participants);
    await act(() => participants.click());
    expect(router.state.location.pathname).toBe(coordinatorPath);
    expect(participants.getAttribute("aria-expanded")).toBe("true");
    await act(() => participants.click());
    expect(participants.getAttribute("aria-expanded")).toBe("false");
    openTable.focus();
    expect(document.activeElement).toBe(openTable);
    await act(async () => openTable.click());
    expect(participants.getAttribute("aria-expanded")).toBe("false");
    expect(container.querySelector('[aria-label="Architect table"]')).not.toBeNull();
    expect(container.querySelector('[contenteditable="true"]')).toBeNull();
    await act(() => {
      window.dispatchEvent(new KeyboardEvent("keydown", { key: "x", bubbles: true }));
      window.dispatchEvent(
        new KeyboardEvent("keydown", { key: "Enter", ctrlKey: true, bubbles: true }),
      );
    });
    expect(useComposerDraftStore.getState().getComposerDraft(ref)?.prompt).toBe(
      "Unsent coordinator plan",
    );
    await pressDetails();
    expect(container.querySelector('aside[aria-label="Thread details"]')).toBeNull();
    await act(async () => {
      await router.navigate({ to: `/${environmentId}/${threadId}` });
    });
    expect(container.querySelector('aside[aria-label="Thread details"]')).toBeNull();
    await pressDetails();
    expect(container.querySelector('aside[aria-label="Thread details"]')).not.toBeNull();
  } finally {
    await act(() => root.unmount());
    container.remove();
    registry.dispose();
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  }
});
