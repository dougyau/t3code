import { describe, expect, it } from "vite-plus/test";
import {
  NodeId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  type PiNativeWorkerSummary,
  RunId,
  ThreadId,
  type OrchestrationV2Subagent,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";

import { deriveThreadTurnSubagents, resolveSubagentPillSegment } from "./threadSubagents.ts";

const at = (iso: string) => DateTime.makeUnsafe(iso);

function subagent(
  overrides: Omit<Partial<OrchestrationV2Subagent>, "id"> & { readonly id: string },
): OrchestrationV2Subagent {
  const { id, ...rest } = overrides;
  return {
    id: NodeId.make(id),
    threadId: ThreadId.make("thread-1"),
    runId: RunId.make("run-1"),
    parentNodeId: NodeId.make("node-1"),
    origin: "app_owned",
    createdBy: "agent",
    driver: ProviderDriverKind.make("codex"),
    providerInstanceId: ProviderInstanceId.make("codex"),
    providerThreadId: null,
    childThreadId: ThreadId.make("thread-child"),
    nativeTaskRef: null,
    prompt: "Do the thing",
    title: "Worker",
    model: "gpt-5.4",
    status: "running",
    result: null,
    startedAt: at("2026-06-20T00:00:00.000Z"),
    completedAt: null,
    updatedAt: at("2026-06-20T00:00:01.000Z"),
    ...rest,
  };
}

const run = (id: string, status: "running" | "completed") =>
  ({ id: RunId.make(id), status }) as never;
const activeRun = run("run-1", "running");
const finishedRun = run("run-1", "completed");

function nativeWorker(overrides: Partial<PiNativeWorkerSummary> = {}): PiNativeWorkerSummary {
  return {
    launch: {
      source: "agent_spawn",
      instanceId: "independent-worker",
      providerInstanceId: ProviderInstanceId.make("pi"),
      providerSessionId: ProviderSessionId.make("session"),
      parentThreadId: ThreadId.make("thread-1"),
      parentProviderThreadId: ProviderThreadId.make("parent-provider"),
      parentProviderTurnId: ProviderTurnId.make("parent-turn"),
      runId: RunId.make("run-0"),
      runAttemptId: RunAttemptId.make("attempt"),
      toolNodeId: NodeId.make("launch"),
      coordinatorSessionId: "coordinator",
      toolCallId: "spawn",
    },
    execution: "completed",
    capture: "closed",
    availability: "ready",
    gaps: [],
    ...overrides,
  };
}

describe("deriveThreadTurnSubagents", () => {
  it.each([
    ["running", "closed", true],
    ["accepted", "incomplete", true],
    ["completed", "open", true],
    ["failed", "opening", true],
    ["completed", "closed", false],
    ["failed", "incomplete", false],
    ["cancelled", "closed", false],
  ] as const)(
    "retains earlier native %s/%s: %s without pulling in other providers",
    (execution, capture, included) => {
      const turn = deriveThreadTurnSubagents({
        runs: [run("run-0", "completed"), activeRun],
        subagents: [
          subagent({ id: "selected" }),
          subagent({
            id: "earlier-native",
            runId: RunId.make("run-0"),
            status: "completed",
            nativeWorker: nativeWorker({ execution, capture, availability: "unavailable" }),
          }),
          subagent({ id: "earlier-app-owned", runId: RunId.make("run-0") }),
          subagent({ id: "earlier-claude", runId: RunId.make("run-0"), origin: "provider_native" }),
        ],
      });
      expect(turn?.subagents.map((agent) => agent.id)).toEqual(
        included ? ["earlier-native", "selected"] : ["selected"],
      );
    },
  );

  it("keeps the entry point visible for a finished result whose capture is still open", () => {
    const turn = deriveThreadTurnSubagents({
      runs: [finishedRun],
      subagents: [
        subagent({
          id: "capturing",
          status: "completed",
          nativeWorker: nativeWorker({
            capture: "open",
            result: "available",
            availability: "unavailable",
          }),
        }),
      ],
    });
    expect(turn?.liveCount).toBe(0);
    expect(resolveSubagentPillSegment(turn)).toEqual({
      label: "1 capturing",
      accessibilityLabel: "1 agent still capturing",
    });
  });
  it("returns null when the thread has never spawned an agent", () => {
    expect(deriveThreadTurnSubagents({ runs: [activeRun], subagents: [] })).toBeNull();
  });

  it("scopes the roster to the active run and orders it by start time", () => {
    const turn = deriveThreadTurnSubagents({
      runs: [run("run-0", "completed"), activeRun],
      subagents: [
        subagent({ id: "b", startedAt: at("2026-06-20T00:00:05.000Z") }),
        subagent({ id: "old", runId: RunId.make("run-0") }),
        subagent({ id: "a", startedAt: at("2026-06-20T00:00:02.000Z") }),
      ],
    });

    expect(turn?.runId).toBe("run-1");
    expect(turn?.subagents.map((entry) => entry.id)).toEqual(["a", "b"]);
    expect(turn?.turnActive).toBe(true);
  });

  it("falls back to the most recently updated agent's run once the turn settles", () => {
    const turn = deriveThreadTurnSubagents({
      runs: [finishedRun, run("run-2", "completed")],
      subagents: [
        subagent({ id: "old", status: "completed", updatedAt: at("2026-06-20T00:00:01.000Z") }),
        subagent({
          id: "recent",
          runId: RunId.make("run-2"),
          status: "completed",
          updatedAt: at("2026-06-20T00:00:09.000Z"),
        }),
      ],
    });

    expect(turn?.runId).toBe("run-2");
    expect(turn?.turnActive).toBe(false);
    expect(turn?.subagents.map((entry) => entry.id)).toEqual(["recent"]);
  });

  it("counts idle as neither working nor done", () => {
    const turn = deriveThreadTurnSubagents({
      runs: [activeRun],
      subagents: [
        subagent({ id: "a", status: "running" }),
        subagent({ id: "b", status: "idle" }),
        subagent({ id: "c", status: "failed" }),
      ],
    });

    expect(turn?.liveCount).toBe(1);
    expect(turn?.settledCount).toBe(1);
    expect(turn?.subagents).toHaveLength(3);
  });
});

describe("resolveSubagentPillSegment", () => {
  it("shows how many of the turn's agents are still working", () => {
    const turn = deriveThreadTurnSubagents({
      runs: [activeRun],
      subagents: [
        subagent({ id: "a", status: "running" }),
        subagent({ id: "b", status: "waiting" }),
        subagent({ id: "c", status: "completed" }),
      ],
    });

    expect(resolveSubagentPillSegment(turn)).toEqual({
      label: "2/3",
      accessibilityLabel: "2 of 3 agents working",
    });
  });

  it("reports the finished roster while the turn is still running", () => {
    const turn = deriveThreadTurnSubagents({
      runs: [activeRun],
      subagents: [subagent({ id: "a", status: "completed" })],
    });

    expect(resolveSubagentPillSegment(turn)).toEqual({
      label: "1 done",
      accessibilityLabel: "1 agent done",
    });
  });

  it("hides itself once the turn is over and nothing is working", () => {
    const turn = deriveThreadTurnSubagents({
      runs: [finishedRun],
      subagents: [subagent({ id: "a", status: "completed" })],
    });

    expect(resolveSubagentPillSegment(turn)).toBeNull();
    expect(resolveSubagentPillSegment(null)).toBeNull();
  });

  it("stays visible after the turn ends while an agent is still working", () => {
    const turn = deriveThreadTurnSubagents({
      runs: [finishedRun],
      subagents: [subagent({ id: "a", status: "running" })],
    });

    expect(resolveSubagentPillSegment(turn)?.label).toBe("1/1");
  });
});
