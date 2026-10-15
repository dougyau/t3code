import type { OrchestrationV2TurnItem } from "@t3tools/contracts";

/** Keep canonical native records, but omit their empty, settled assistant chat rows. */
export function shouldOmitNativeWorkerMessage(item: OrchestrationV2TurnItem): boolean {
  return (
    item.type === "assistant_message" &&
    item.nativeWorkerRecord !== undefined &&
    !item.streaming &&
    item.text.trim().length === 0 &&
    (item.attachments?.length ?? 0) === 0
  );
}
