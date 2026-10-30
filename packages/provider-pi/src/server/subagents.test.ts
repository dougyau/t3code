import { describe, expect, it } from "vite-plus/test";
import {
  piSubagentResults,
  piSubagentRunStates,
  piSubagentSnapshot,
  piSubagentLifecycle,
  piSubagentWorkflowChildren,
  piSubagentLifecycleHints,
  PI_SUBAGENT_LIFECYCLE_MAX_BYTES,
} from "./subagents.ts";

describe("Pi native subagent observations", () => {
  it("validates the public membership contract independently of lifecycle identity metadata", () => {
    const data = {
      runId: "W",
      toolCallId: "launch",
      mode: "workflow",
      workflowChildren: {
        version: 1,
        workflowRunId: "W",
        parentToolCallId: "launch",
        children: [
          {
            childId: "review-contracts",
            runId: "C",
            label: "Review contracts",
            agent: "reviewer",
            state: "running",
          },
        ],
      },
    };
    const text = JSON.stringify(data);
    expect(piSubagentLifecycle(text, "W")).toBeUndefined();
    expect(piSubagentWorkflowChildren(text, "W", "launch")[0]?.title).toBe("Review contracts");
    const fallback = piSubagentWorkflowChildren(
      JSON.stringify({
        ...data,
        workflowChildren: {
          ...data.workflowChildren,
          children: [{ ...data.workflowChildren.children[0], label: undefined }],
        },
      }),
      "W",
      "launch",
    )[0];
    expect(fallback?.title).toBe("review-contracts");
    expect(fallback?.label).toBeUndefined();
    for (const invalid of [
      { ...data, runId: "foreign" },
      { ...data, workflowChildren: { ...data.workflowChildren, version: 2 } },
      { ...data, workflowChildren: { ...data.workflowChildren, parentToolCallId: "other" } },
    ])
      expect(piSubagentWorkflowChildren(JSON.stringify(invalid), "W", "launch")).toEqual([]);
    expect(
      piSubagentWorkflowChildren(" ".repeat(PI_SUBAGENT_LIFECYCLE_MAX_BYTES + 1), "W", "launch"),
    ).toEqual([]);
  });

  it("preserves bounded foreground nested public steps and their real parents", () => {
    const grandchild = {
      id: "grandchild",
      parentRunId: "child",
      parentStepIndex: 0,
      state: "complete",
      steps: [{ agent: "reviewer", status: "complete", startedAt: 2, endedAt: 3 }],
    };
    const child = {
      id: "child",
      parentRunId: "foreground",
      parentStepIndex: 0,
      state: "complete",
      steps: [
        { agent: "worker", status: "complete", startedAt: 1, endedAt: 3, children: [grandchild] },
      ],
      children: [grandchild],
    };
    const rows = piSubagentResults(
      {
        details: {
          runId: "foreground",
          results: [
            {
              agent: "parent",
              task: "delegate",
              exitCode: 0,
              children: [child],
            },
          ],
        },
      },
      "launch",
      true,
    );
    for (const runId of ["child", "grandchild"]) {
      const descendant = rows.find((row) => row.runId === runId);
      expect(descendant).toBeDefined();
      expect(descendant?.prompt).toBeUndefined();
      expect(descendant?.resultPosition).toBeUndefined();
    }
    const cyclic: { id: string; parentRunId: string; parentStepIndex: number; steps: unknown[] } = {
      id: "cycle",
      parentRunId: "foreground",
      parentStepIndex: 0,
      steps: [],
    };
    cyclic.steps.push({ agent: "cycle", status: "running", startedAt: 1, children: [cyclic] });
    const bounded = piSubagentResults(
      {
        details: {
          runId: "foreground",
          results: [
            {
              agent: "parent",
              task: "delegate",
              exitCode: 0,
              children: [
                cyclic,
                ...Array.from({ length: 600 }, (_, index) => ({ ...child, id: `child-${index}` })),
              ],
            },
          ],
        },
      },
      "launch",
      true,
    );
    expect(bounded.length).toBeLessThanOrEqual(500);
    expect(new Set(bounded.map((row) => row.id)).size).toBe(bounded.length);
  });
  it.each([false, true])(
    "preserves native foreground pause provenance at tool completion=%s",
    (completed) => {
      const [child] = piSubagentResults(
        {
          details: {
            runId: "foreground",
            results: [
              {
                agent: "worker",
                task: "work",
                interrupted: true,
                exitCode: 0,
                progress: { status: "running" },
                finalOutput: "Interrupted. Waiting for explicit next action.",
              },
            ],
          },
        },
        "launch",
        completed,
      );
      expect(child?.status).toBe("idle");
      expect(child?.paused).toBe(true);
    },
  );
  it("lets actual same-run workflow paused membership override the result's zero exit code", () => {
    const rows = piSubagentResults(
      {
        details: {
          mode: "workflow",
          runId: "workflow",
          results: [{ runId: "child", index: 0, agent: "worker", task: "work", exitCode: 0 }],
          workflowChildren: {
            version: 1,
            workflowRunId: "workflow",
            parentToolCallId: "launch",
            children: [{ childId: "lane", runId: "child", agent: "worker", state: "paused" }],
          },
        },
      },
      "launch",
      true,
    );
    expect(rows).toHaveLength(1);
    expect(rows[0]?.status).toBe("idle");
    expect(rows[0]?.paused).toBe(true);
  });
  it("does not settle a detached foreground receipt", () => {
    const [child] = piSubagentResults(
      {
        details: {
          runId: "foreground",
          mode: "single",
          results: [
            {
              index: 0,
              agent: "worker",
              task: "work",
              detached: true,
              exitCode: -2,
              detachedReason: "user request",
              finalOutput: "Detached at user request before task completion.",
            },
          ],
        },
      },
      "launch",
      true,
    );
    expect(child?.status).toBe("running");
    expect(child?.result).toBeUndefined();
    expect(child?.progress).toBe("Detached at user request before task completion.");
  });
  it("reads only supported bounded lifecycle identity metadata", () => {
    const metadata = {
      lifecycleArtifactVersion: 3,
      runId: "C",
      mode: "single",
      parentWorkflowRunId: "W",
      workflowKey: "lane",
      steps: [{ index: 0 }],
      ignored: "future field",
    };
    expect(piSubagentLifecycle(JSON.stringify(metadata), "C")).toEqual({
      runId: "C",
      mode: "single",
      parentWorkflowRunId: "W",
      workflowKey: "lane",
      steps: [{ childId: "step:0", runId: undefined, workflowKey: undefined }],
    });
    for (const value of [
      "{",
      JSON.stringify({ ...metadata, lifecycleArtifactVersion: 4 }),
      JSON.stringify({ ...metadata, runId: "other" }),
      JSON.stringify({ ...metadata, id: "other" }),
      JSON.stringify({ ...metadata, steps: Array.from({ length: 129 }, () => ({})) }),
      JSON.stringify({ ...metadata, ignored: "x".repeat(PI_SUBAGENT_LIFECYCLE_MAX_BYTES) }),
      JSON.stringify({ ...metadata, ignored: "界".repeat(PI_SUBAGENT_LIFECYCLE_MAX_BYTES / 2) }),
      JSON.stringify({ ...metadata, ignored: Array.from({ length: 4097 }, () => 0) }),
      JSON.stringify({
        ...metadata,
        ignored: Array.from({ length: 17 }).reduce<unknown>((value) => ({ child: value }), null),
      }),
    ])
      expect(piSubagentLifecycle(value, "C")).toBeUndefined();
  });

  it("bounds untrusted discovery hints and ignores unusable fields", () => {
    const lines = [
      "PI_SUBAGENT_ASYNC_JSON:" +
        JSON.stringify({
          kind: "pi-subagents.async-status-snapshot",
          version: 1,
          runs: [],
          lifecycleHints: [
            ...Array.from({ length: 70 }, (_, index) => ({
              runId: `run-${index}`,
              asyncDir: "/known",
              childRunId: "x".repeat(257),
            })),
            { runId: "bad", asyncDir: "x".repeat(4097) },
          ],
        }),
    ];
    const hints = piSubagentLifecycleHints(lines);
    expect(hints).toHaveLength(63);
    expect(hints.every((hint) => hint.childRunId === undefined)).toBe(true);
  });
  it.each(["attached-first", "summary-first"])(
    "preserves one supported nested step parent, %s",
    (order) => {
      const nested = {
        id: "N",
        kind: "subagent",
        state: "running",
        children: [
          {
            id: "step:0",
            kind: "step",
            agent: "same label",
            state: "running",
            startedAt: 1,
            updatedAt: 1,
          },
        ],
      };
      const parent = {
        id: "step:0",
        kind: "step",
        agent: "parent",
        state: "running",
        startedAt: 1,
        children: [nested],
      };
      const rows = piSubagentSnapshot([
        "PI_SUBAGENT_ASYNC_JSON:" +
          JSON.stringify({
            kind: "pi-subagents.async-status-snapshot",
            version: 1,
            generatedAt: 10,
            runs: [
              {
                id: "C",
                kind: "subagent",
                state: "running",
                children: order === "attached-first" ? [parent, nested] : [nested, parent],
              },
              { ...nested, id: "different" },
            ],
          }),
      ]);
      const child = rows.filter((row) => row.runId === "N");
      expect(child).toHaveLength(1);
      expect(child[0]?.parentId).toBe(JSON.stringify(["C", "step:0"]));
      expect(rows.findIndex((row) => row.runId === "C")).toBeLessThan(
        rows.findIndex((row) => row.runId === "N"),
      );
      expect(rows.filter((row) => row.title === "same label")).toHaveLength(2);
      expect(rows.find((row) => row.runId === "different")?.parentId).toBeNull();
    },
  );

  it("does not invent a parent from absent or conflicting nested ownership", () => {
    const nested = {
      id: "N",
      kind: "subagent",
      state: "running",
      children: [{ id: "step:0", kind: "step", agent: "nested", state: "running", startedAt: 1 }],
    };
    const decode = (runs: unknown[]) =>
      piSubagentSnapshot([
        "PI_SUBAGENT_ASYNC_JSON:" +
          JSON.stringify({
            kind: "pi-subagents.async-status-snapshot",
            version: 1,
            generatedAt: 10,
            runs,
          }),
      ]);
    expect(decode([nested])[0]?.parentId).toBeNull();
    for (const ids of [
      ["C", "D"],
      ["D", "C"],
    ]) {
      const rows = decode(
        ids.map((id) => ({
          id,
          kind: "subagent",
          state: "running",
          children: [
            {
              id: "step:0",
              kind: "step",
              agent: "parent",
              state: "running",
              startedAt: 1,
              children: [nested],
            },
          ],
        })),
      );
      expect(rows.some((row) => row.runId === "N")).toBe(false);
    }
  });

  it("projects public foreground workflow children before results with final-result identities", () => {
    const progress = piSubagentResults(
      {
        details: {
          mode: "workflow",
          runId: "workflow",
          results: [],
          workflowChildren: {
            version: 1,
            parentToolCallId: "call",
            workflowRunId: "workflow",
            workflowState: "running",
            inventoryComplete: false,
            children: [
              {
                childId: "leaf",
                runId: "leaf-run",
                agent: "worker",
                state: "running",
                activity: { currentTool: "read", toolCount: 1 },
              },
            ],
          },
        },
      },
      "call",
      false,
    );
    const final = piSubagentResults(
      {
        details: {
          mode: "workflow",
          runId: "workflow",
          results: [
            {
              runId: "leaf-run",
              workflowKey: "leaf",
              index: 0,
              agent: "worker",
              task: "work",
              exitCode: 0,
              finalOutput: "done",
            },
          ],
        },
      },
      "call",
      true,
    );
    expect(progress).toHaveLength(1);
    expect(progress[0]?.id).toBe(final[0]?.id);
    expect(progress[0]?.status).toBe("running");
    expect(progress[0]?.progress).toContain("read");
    expect(progress[0]?.prompt).toBeUndefined();
  });

  it("keeps foreground identities stable across reordered progress and final rows", () => {
    const first = { index: 9, agent: "scout", task: "inspect", finished: false };
    const second = { index: 2, agent: "reviewer", task: "review", finished: false };
    const progress = piSubagentResults(
      { details: { runId: "r", results: [first, second] } },
      "call",
      false,
    );
    const done = piSubagentResults(
      {
        details: {
          runId: "r",
          results: [
            { ...second, finished: true, exitCode: 1 },
            { ...first, finished: true, exitCode: 0 },
          ],
        },
      },
      "other-call",
      true,
    );
    expect(done.map((row) => row.id)).toEqual([progress[1]?.id, progress[0]?.id]);
    expect(done.map((row) => row.status)).toEqual(["failed", "completed"]);
  });

  it.each([true, false])(
    "keeps workflow single-child identities across local index collisions and reorder, row runId=%s",
    (hasRunId) => {
      const a = {
        index: 0,
        workflowKey: "scout",
        runId: hasRunId ? "child-a" : undefined,
        agent: "scout",
        task: "inspect",
        finished: false,
      };
      const b = {
        index: 0,
        workflowKey: "review",
        runId: hasRunId ? "child-b" : undefined,
        agent: "reviewer",
        task: "review",
        finished: false,
      };
      const first = piSubagentResults(
        { details: { mode: "workflow", runId: "workflow", results: [a, b] } },
        "call",
        false,
      );
      const last = piSubagentResults(
        {
          details: {
            mode: "workflow",
            runId: "workflow",
            results: [
              { ...b, exitCode: 0, finished: true },
              { ...a, exitCode: 0, finished: true },
            ],
          },
        },
        "call",
        true,
      );
      expect(first[0]?.id).not.toBe(first[1]?.id);
      expect(last.map((row) => row.id)).toEqual([first[1]?.id, first[0]?.id]);
    },
  );

  it("preserves stderr as the result of a failed child", () => {
    const [child] = piSubagentResults(
      {
        details: {
          runId: "failed",
          results: [
            { index: 0, agent: "scout", task: "inspect", exitCode: 1, stderr: "Launch failed" },
          ],
        },
      },
      "call",
      true,
    );
    expect(child?.status).toBe("failed");
    expect(child?.result).toBe("Launch failed");
  });

  it("recognizes ended partial snapshots as inactive without inventing a successful outcome", () => {
    const lines = (ended: boolean) => [
      `PI_SUBAGENT_ASYNC_JSON:${JSON.stringify({ kind: "pi-subagents.async-status-snapshot", version: 1, runs: [{ id: "partial", kind: "subagent", state: "partial", ...(ended ? { endedAt: 2 } : {}), children: [{ id: "step:0", kind: "step", agent: "worker", state: "partial", startedAt: 1, ...(ended ? { endedAt: 2 } : {}) }] }] })}`,
    ];
    expect(piSubagentSnapshot(lines(true))[0]).toMatchObject({
      status: "idle",
      partial: true,
      paused: false,
    });
    expect(piSubagentRunStates(lines(true)).get("partial")).toBe("idle");
    expect(piSubagentSnapshot(lines(false))).toEqual([]);
    expect(piSubagentRunStates(lines(false)).has("partial")).toBe(false);
  });
  it("does not duplicate an async agent when its queued run materializes a self-step", () => {
    const snapshot = (runs: unknown[]) => [
      `PI_SUBAGENT_ASYNC_JSON:${JSON.stringify({
        kind: "pi-subagents.async-status-snapshot",
        version: 1,
        runs,
      })}`,
    ];
    const root = {
      id: "detached",
      kind: "subagent",
      label: "scout",
      state: "queued",
    };
    const queued = piSubagentSnapshot(snapshot([root]));
    const running = piSubagentSnapshot(
      snapshot([
        {
          ...root,
          state: "running",
          children: [
            { id: "step:0", kind: "step", agent: "scout", state: "running", startedAt: 1 },
          ],
        },
      ]),
    );
    expect(queued).toEqual([]);
    expect(running).toHaveLength(1);
    expect(running[0]?.childId).toBe("step:0");
  });

  it("projects agents and actual ancestry, not workflow roots, planned steps or host steps", () => {
    const snapshot = {
      kind: "pi-subagents.async-status-snapshot",
      version: 1,
      generatedAt: 100,
      runs: [
        {
          id: "wrapper",
          kind: "workflow",
          label: "workflow",
          state: "running",
          children: [
            { id: "planned", kind: "step", agent: "scout", state: "queued" },
            { id: "host", kind: "host-step", label: "check", state: "running" },
            {
              id: "real",
              kind: "step",
              agent: "scout",
              state: "running",
              startedAt: 1,
              children: [
                {
                  id: "nested",
                  kind: "subagent",
                  label: "worker",
                  state: "running",
                  children: [
                    { id: "step:0", kind: "step", agent: "worker", state: "running", startedAt: 2 },
                  ],
                },
              ],
            },
          ],
        },
      ],
    };
    const rows = piSubagentSnapshot([`PI_SUBAGENT_ASYNC_JSON:${JSON.stringify(snapshot)}`]);
    expect(rows.map((row) => row.title)).toEqual(["scout", "worker"]);
    expect(rows[1]?.parentId).toBe(rows[0]?.id);
    expect(rows[1]?.runId).toBe("nested");
    expect(piSubagentSnapshot(["PI_SUBAGENT_ASYNC_JSON:invalid"])).toEqual([]);
    expect(
      piSubagentSnapshot([`PI_SUBAGENT_ASYNC_JSON:${JSON.stringify({ ...snapshot, version: 2 })}`]),
    ).toEqual([]);
  });
});
