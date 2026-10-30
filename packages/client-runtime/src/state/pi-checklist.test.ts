import { NodeId, PlanId, ProviderDriverKind, ThreadId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";
import { selectPiChecklist } from "./pi-checklist.ts";

describe("native checklist selection", () => {
  it("uses only the selected Pi plan and does not fall back after clear or observation loss", () => {
    const selected = {
      id: PlanId.make("selected"),
      threadId: ThreadId.make("thread"),
      runId: null,
      nodeId: NodeId.make("node"),
      kind: "todo_list" as const,
      status: "active" as const,
      steps: [],
    };
    const plans = [selected, { ...selected, id: PlanId.make("newer-history") }];
    const owner = {
      driver: ProviderDriverKind.make("pi"),
      nativeMetadata: {
        checklistSelection: {
          state: "ready" as const,
          planId: selected.id,
          sourceEntryId: "entry",
        },
      },
    };
    expect(selectPiChecklist(plans, owner)).toBe(selected);
    for (const state of ["ready", "unavailable"] as const) {
      expect(
        selectPiChecklist(plans, {
          ...owner,
          nativeMetadata: {
            checklistSelection: { state, planId: null, sourceEntryId: null },
          },
        }),
      ).toBeNull();
    }
  });
});
