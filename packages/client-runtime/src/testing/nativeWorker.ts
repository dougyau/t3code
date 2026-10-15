import {
  NodeId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
  type OrchestrationV2Subagent,
  type PiNativeWorkerSummary,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";

export function nativeWorkerFixture(
  overrides: Partial<PiNativeWorkerSummary> = {},
): PiNativeWorkerSummary {
  return {
    launch: {
      source: "team_assign",
      instanceId: "reviewer-instance",
      roleId: "reviewer",
      assignmentId: "assignment-review",
      providerInstanceId: ProviderInstanceId.make("pi"),
      providerSessionId: ProviderSessionId.make("session"),
      parentThreadId: ThreadId.make("parent"),
      parentProviderThreadId: ProviderThreadId.make("parent-provider"),
      parentProviderTurnId: ProviderTurnId.make("parent-turn"),
      runId: RunId.make("run-1"),
      runAttemptId: RunAttemptId.make("attempt"),
      toolNodeId: NodeId.make("launch"),
      coordinatorSessionId: "coordinator",
      toolCallId: "assign",
    },
    roleId: "reviewer",
    brief: "Review the public changes",
    activated: { attemptId: "attempt-1", model: "gpt-effective", reasoning: "high" },
    execution: "completed",
    capture: "open",
    availability: "unavailable",
    result: "available",
    gaps: [],
    ...overrides,
  };
}

export function nativeSubagentFixture(
  overrides: Partial<OrchestrationV2Subagent> = {},
): OrchestrationV2Subagent {
  return {
    id: NodeId.make("worker"),
    threadId: ThreadId.make("parent"),
    runId: RunId.make("run-1"),
    parentNodeId: NodeId.make("launch"),
    origin: "provider_native",
    createdBy: "agent",
    driver: ProviderDriverKind.make("pi"),
    providerInstanceId: ProviderInstanceId.make("pi"),
    providerThreadId: null,
    childThreadId: ThreadId.make("worker-chat"),
    nativeTaskRef: null,
    prompt: "Review the public changes",
    title: "Review worker",
    model: "requested-not-effective",
    status: "completed",
    result: null,
    startedAt: DateTime.makeUnsafe("2026-06-20T00:00:00Z"),
    completedAt: DateTime.makeUnsafe("2026-06-20T00:01:00Z"),
    updatedAt: DateTime.makeUnsafe("2026-06-20T00:01:01Z"),
    nativeWorker: nativeWorkerFixture(),
    ...overrides,
  };
}
