import type { ReactNode, ReactElement, ComponentProps } from "react";
import type { ReactTestInstance } from "react-test-renderer";
import { act, create, type ReactTestRenderer } from "../../../test/component-renderer";
import { afterEach, expect, it, vi } from "vite-plus/test";
import { nativeWorkerFixture } from "@t3tools/client-runtime/testing/nativeWorker";
import type { PiNativeWorkerSummary } from "@t3tools/contracts";

// Portal/focus need a browser; this boundary double exercises reader state and content only.
vi.mock("../ui/dialog", async () => {
  const { React, createContext } = await import("../../../test/component-renderer");
  const Context = createContext({ open: false, change: (_open: boolean) => {} });
  return {
    Dialog: ({
      open,
      onOpenChange,
      children,
    }: {
      open: boolean;
      onOpenChange: (open: boolean) => void;
      children: ReactNode;
    }) => <Context value={{ open, change: onOpenChange }}>{children}</Context>,
    DialogTrigger: ({
      render,
      children,
    }: {
      render: ReactElement<ComponentProps<"button">>;
      children: ReactNode;
    }) => {
      const dialog = React.use(Context);
      return React.cloneElement(render, { onClick: () => dialog.change(true) }, children);
    },
    DialogPopup: ({ children }: { children: ReactNode }) => {
      const dialog = React.use(Context);
      return dialog.open ? (
        <div role="dialog">
          {children}
          <button type="button" onClick={() => dialog.change(false)}>
            Close
          </button>
        </div>
      ) : null;
    },
    DialogHeader: "header",
    DialogTitle: "h1",
    DialogPanel: "main",
  };
});

import { ProviderSubagentBar } from "./ProviderSubagentBar";

let renderer: ReactTestRenderer;
const visibleStatus = { textContent: "" };
const props = {
  provider: null,
  showInstanceBadge: false,
  modelLabel: "fallback-model",
  effortLabel: "high",
  status: null,
  onOpenParent: null,
};
function text(node: ReactTestInstance): string {
  return node.children.map((child) => (typeof child === "string" ? child : text(child))).join("");
}
const renderedText = () => text(renderer.root);
const button = (label: string) =>
  renderer.root.findAllByType("button").find((node) => text(node) === label);
const click = async (label: string) => {
  const target = button(label);
  if (!target) throw new Error(`Missing button: ${label}`);
  await act(async () =>
    target.props.onClick({
      nativeEvent: new Event("click"),
      preventDefault() {},
      defaultPrevented: false,
    }),
  );
};
async function mount(worker: PiNativeWorkerSummary | undefined = nativeWorkerFixture()) {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  await act(async () => {
    renderer = create(<ProviderSubagentBar {...props} nativeWorker={worker} />, {
      createNodeMock: (element) => (element.type === "span" ? visibleStatus : null),
    });
  });
}
async function update(worker: PiNativeWorkerSummary | undefined) {
  await act(async () => renderer.update(<ProviderSubagentBar {...props} nativeWorker={worker} />));
}
afterEach(async () => {
  await act(async () => renderer?.unmount());
  vi.unstubAllGlobals();
});

it("keeps native detail out of the compact bar and its announcement", async () => {
  const worker = nativeWorkerFixture({ brief: "Private original brief" });
  await mount(worker);
  expect(renderedText()).toContain("reviewer");
  expect(renderedText()).toContain("gpt-effective");
  expect(renderedText()).toContain("Execution completed");
  expect(visibleStatus.textContent).toBe("Execution completed");
  for (const detail of [
    worker.launch.instanceId,
    "assignment-review",
    "Private original brief",
    "Capture",
    "Availability",
    "Result",
    "requested-not-effective",
  ]) {
    expect(renderedText()).not.toContain(detail);
  }
  expect(text(renderer.root.findByProps({ role: "status" }))).toBe(
    "reviewer, gpt-effective subagent: Execution completed",
  );
  expect(renderer.root.findAllByType("input")).toHaveLength(0);
  expect(renderer.root.findAllByType("textarea")).toHaveLength(0);
  expect(renderer.root.findAllByType("button").map(text)).toEqual(["Assignment"]);
});

it("reads exact long partial text, switches observation, follows updates and never retains a removed summary", async () => {
  const brief = `  <b>Original, not HTML</b>\n\t${"long unbroken label ".repeat(500)}\n  `;
  const worker = nativeWorkerFixture({
    brief,
    briefComplete: false,
    gaps: ["unsupported_content"],
    selected: {
      model: "selected-only",
      requestedModel: "requested-not-effective",
      reasoning: "medium",
    },
  });
  await mount(worker);
  await click("Assignment");
  expect(renderedText()).toContain("Partial brief — only the first page is available.");
  expect(
    renderer.root
      .findAllByType("div")
      .some((node) => node.children.length === 1 && node.children[0] === brief),
  ).toBe(true);
  expect(renderer.root.findAllByType("b")).toHaveLength(0);
  await click("Observation");
  expect(renderedText()).not.toContain(brief);
  expect(renderedText()).toContain(
    "ExecutioncompletedCaptureopenAvailabilityunavailableResultavailable",
  );
  expect(renderedText()).toContain(`Instance ID${worker.launch.instanceId}`);
  expect(renderedText()).toContain("Assignment IDassignment-review");
  expect(renderedText()).toContain("Requested modelrequested-not-effective");
  expect(renderedText()).toContain("Selected modelselected-only");
  expect(renderedText()).toContain("Selected reasoningmedium");
  expect(renderedText()).toContain("Activated modelgpt-effective");
  expect(renderedText()).toContain("unsupported_content");
  expect(renderedText()).toContain("Gaps do not determine");
  if (worker.launch.source !== "team_assign") throw new Error("Expected team fixture");
  const next = nativeWorkerFixture({
    brief: "new worker text",
    briefComplete: true,
    launch: { ...worker.launch, source: "agent_spawn", instanceId: "different-worker" },
    activated: null,
    gaps: [],
  });
  await update(next);
  expect(renderedText()).not.toContain(worker.launch.instanceId);
  expect(renderedText()).not.toContain("Assignment ID");
  expect(renderedText()).toContain("Activated modelNot reported");
  expect(renderedText()).toContain("No gaps reported.");
  await click("Original brief");
  expect(renderedText()).toContain("Complete briefnew worker text");
  expect(renderedText()).not.toContain(brief);
  await update({ ...next, brief: "updated current text" });
  expect(renderedText()).toContain("updated current text");
  expect(renderedText()).not.toContain("new worker text");
  await update(undefined);
  expect(renderedText()).toContain("Worker details unavailable");
  expect(renderedText()).not.toContain("updated current text");
  await click("Close");
  expect(renderer.root.findAllByProps({ role: "dialog" })).toHaveLength(0);
});

it.each([
  [undefined, undefined, "Brief unavailable"],
  ["", true, "Complete brief"],
  ["  \n\t", undefined, "Brief completeness not reported."],
] as const)(
  "distinguishes original brief %j with completeness %j",
  async (brief, briefComplete, notice) => {
    await mount(nativeWorkerFixture({ brief, briefComplete }));
    await click("Assignment");
    expect(renderedText()).toContain(notice);
    if (brief !== undefined) {
      expect(renderedText()).not.toContain("Brief unavailable");
      expect(
        renderer.root
          .findAllByType("div")
          .some((node) =>
            brief === ""
              ? node.children.length === 0 && node.props.className?.includes("whitespace-pre-wrap")
              : node.children[0] === brief,
          ),
      ).toBe(true);
    }
    await click("Close");
    expect(renderedText()).not.toContain(notice);
  },
);

it("does not imply execution or activation from selected models and observation fields", async () => {
  const worker = nativeWorkerFixture();
  if (worker.launch.source !== "team_assign") throw new Error("Expected team fixture");
  const { execution: _execution, result: _result, roleId: _roleId, ...unknown } = worker;
  await mount({
    ...unknown,
    activated: null,
    selected: { model: "selected-only", requestedModel: "requested-not-effective" },
    launch: { ...worker.launch, source: "agent_spawn" },
  });
  expect(visibleStatus.textContent).toBe("Execution unknown");
  expect(text(renderer.root.findByProps({ role: "status" }))).toBe(
    "agent_spawn, Not reported subagent: Execution unknown",
  );
  expect(renderedText()).not.toContain("requested-not-effective");
  await click("Assignment");
  await click("Observation");
  expect(renderedText()).toContain("Executionunknown");
  expect(renderedText()).toContain("ResultNot reported");
});

it("preserves non-native model/effort and has no Assignment reader", async () => {
  await mount();
  await update(undefined);
  expect(renderedText()).toContain("fallback-model");
  expect(renderedText()).toContain("high");
  expect(button("Assignment")).toBeUndefined();
  expect(renderer.root.findAllByProps({ role: "dialog" })).toHaveLength(0);
});

it("toggles the shared task list with counts, current/completed states and long labels, then removes it", async () => {
  await mount();
  const long = "Review exact checklist label ".repeat(80);
  const tasks = {
    tasksProgress: { step: long, completedSteps: 1, totalSteps: 3 },
    taskSteps: [
      { step: "Inspected public output", status: "completed" as const },
      { step: long, status: "inProgress" as const },
      { step: "Verify remaining output", status: "pending" as const },
    ],
  };
  await act(async () => renderer.update(<ProviderSubagentBar {...props} {...tasks} />));
  expect(renderedText()).toContain("1/3");
  expect(renderedText()).toContain(long);
  expect(renderedText()).not.toContain("Inspected public output");
  const toggle = () => renderer.root.findByType("button");
  await act(async () => toggle().props.onClick());
  const list = renderer.root.findByProps({ role: "list" });
  expect(text(list)).toContain("Completed: Inspected public output");
  expect(text(list)).toContain(`Running: ${long}`);
  expect(text(list)).toContain("Pending: Verify remaining output");
  await act(async () => toggle().props.onClick());
  expect(renderer.root.findAllByProps({ role: "list" })).toHaveLength(0);
  await update(undefined);
  expect(renderedText()).not.toContain(long);
  expect(renderedText()).not.toContain("1/3");
  expect(renderer.root.findAllByType("button")).toHaveLength(0);
});
