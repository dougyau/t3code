import {
  NodeId,
  PI_CHECKLIST_PROVENANCE,
  piChecklistStepId,
  piChecklistTurnItemId,
  pointerFromCapture,
  type OrchestrationV2PlanArtifact,
  type OrchestrationV2PlanStep,
  type OrchestrationV2TurnItem,
  type PiChecklistCapture,
  type PiChecklistSelectionPointer,
} from "@t3tools/contracts";
import type * as DateTime from "effect/DateTime";

export type PiChecklistProjection = {
  readonly pointer: PiChecklistSelectionPointer;
  readonly plan: OrchestrationV2PlanArtifact | null;
  readonly turnItem: OrchestrationV2TurnItem | null;
};

function visibleSteps(capture: PiChecklistCapture): ReadonlyArray<OrchestrationV2PlanStep> {
  if (capture.source.state !== "ready" || capture.source.sourceEntryId === null) return [];
  return capture.source.tasks.flatMap((task) => {
    if (task.status === "deleted") return [];
    const status =
      task.status === "in_progress"
        ? "running"
        : task.status === "completed"
          ? "completed"
          : "pending";
    return [
      {
        id: piChecklistStepId(capture.list, task.identityEntryId, task.id),
        text: task.subject,
        status,
      },
    ];
  });
}

/** Projects one capture into the current pointer plus at most one todo_list artifact pair. */
export function projectPiChecklist(input: {
  readonly capture: PiChecklistCapture;
  readonly updatedAt: DateTime.Utc;
}): PiChecklistProjection {
  const pointer = pointerFromCapture(input.capture);
  if (pointer.state !== "ready" || pointer.planId === null) {
    return { pointer, plan: null, turnItem: null };
  }
  const steps = [...visibleSteps(input.capture)];
  const finished = steps.length > 0 && steps.every((step) => step.status === "completed");
  const planId = pointer.planId;
  const nodeId = NodeId.make(`pi-checklist-node:${planId}`);
  const plan: OrchestrationV2PlanArtifact = {
    id: planId,
    threadId: input.capture.list.threadId,
    runId: null,
    nodeId,
    kind: "todo_list",
    status: finished ? "completed" : "active",
    steps,
    piChecklistProvenance: PI_CHECKLIST_PROVENANCE,
  };
  const turnItem: OrchestrationV2TurnItem = {
    id: piChecklistTurnItemId(input.capture.list),
    threadId: input.capture.list.threadId,
    runId: null,
    nodeId,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal: 0,
    status: finished ? "completed" : "running",
    title: null,
    startedAt: null,
    completedAt: null,
    updatedAt: input.updatedAt,
    type: "todo_list",
    planId,
    steps,
    piChecklistProvenance: PI_CHECKLIST_PROVENANCE,
  };
  return { pointer, plan, turnItem };
}

type ChecklistPlan = {
  readonly id: string;
  readonly kind: string;
  readonly piChecklistProvenance?: string | undefined;
};

/**
 * `undefined` means this thread is not a Pi checklist owner; the caller keeps its
 * existing selection. `null` means Pi has no current list. Never the latest array entry.
 */
export function selectCurrentChecklistPlan<T extends ChecklistPlan>(input: {
  readonly plans: ReadonlyArray<T>;
  readonly activeProviderThread: {
    readonly driver: string;
    readonly nativeMetadata?:
      | {
          readonly checklistSelection?: PiChecklistSelectionPointer | undefined;
        }
      | null
      | undefined;
  } | null;
}): T | null | undefined {
  const thread = input.activeProviderThread;
  if (thread === null || thread.driver !== "pi") return undefined;
  const pointer = thread.nativeMetadata?.checklistSelection;
  if (
    pointer === undefined ||
    pointer.state !== "ready" ||
    pointer.sourceEntryId === null ||
    pointer.planId === null
  )
    return null;
  return (
    input.plans.find(
      (plan) => plan.id === pointer.planId && plan.kind === "todo_list" && isPiChecklistPlan(plan),
    ) ?? null
  );
}

export function isPiChecklistPlan(plan: {
  readonly piChecklistProvenance?: string | undefined;
}): boolean {
  return plan.piChecklistProvenance === PI_CHECKLIST_PROVENANCE;
}
