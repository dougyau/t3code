import type {
  OrchestrationV2PlanArtifact,
  OrchestrationV2ProviderThread,
} from "@t3tools/contracts";

export function selectPiChecklist(
  plans: ReadonlyArray<OrchestrationV2PlanArtifact>,
  providerThread: Pick<OrchestrationV2ProviderThread, "driver" | "nativeMetadata"> | undefined,
) {
  if (providerThread?.driver !== "pi") return undefined;
  const selection = providerThread.nativeMetadata?.checklistSelection;
  if (selection?.state !== "ready" || selection.planId === null) return null;
  const plan = plans.find((plan) => plan.id === selection.planId);
  return plan?.kind === "todo_list" ? plan : null;
}
