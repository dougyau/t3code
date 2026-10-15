import { PiChecklistCapture, PiChecklistSelectionPointer } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import { projectPiChecklist, selectCurrentChecklistPlan } from "./checklist.ts";

const decodeCapture = Schema.decodeUnknownSync(PiChecklistCapture);
const decodePointer = Schema.decodeUnknownSync(PiChecklistSelectionPointer);

function capture(
  source: {
    readonly state: "ready" | "unavailable";
    readonly sourceEntryId: string | null;
    readonly tasks: ReadonlyArray<{
      readonly id: number;
      readonly identityEntryId: string;
      readonly subject: string;
      readonly status: "pending" | "in_progress" | "completed" | "deleted";
    }>;
  },
  captureVersion = 1,
): PiChecklistCapture {
  return decodeCapture({
    list: {
      threadId: "thread-1",
      providerInstanceId: "pi",
      nativeSessionId: "session-a",
    },
    ownerGeneration: 1,
    sessionGeneration: 1,
    captureVersion,
    expectedPointer: { kind: "absent" },
    authority: "capture",
    source,
  });
}

describe("projectPiChecklist", () => {
  it("keeps identity through status changes but gives a reused number a new lifespan", () => {
    const project = (identityEntryId: string, status: "pending" | "completed") =>
      projectPiChecklist({
        capture: capture({
          state: "ready",
          sourceEntryId: "selected",
          tasks: [{ id: 1, identityEntryId, subject: "Task", status }],
        }),
        updatedAt: DateTime.makeUnsafe("2026-09-04T00:00:00.000Z"),
      }).plan;
    const first = project("first-life", "pending");
    const finished = project("first-life", "completed");
    const reused = project("after-clear", "pending");
    if (
      first?.kind !== "todo_list" ||
      finished?.kind !== "todo_list" ||
      reused?.kind !== "todo_list"
    )
      throw new Error("expected checklist plans");
    expect(first.id).toBe(reused.id);
    expect(first.steps[0]?.id).toBe(finished.steps[0]?.id);
    expect(first.steps[0]?.id).not.toBe(reused.steps[0]?.id);
  });

  it("projects visible steps without a run id or Pi durations and drops deleted rows", () => {
    const projected = projectPiChecklist({
      capture: capture({
        state: "ready",
        sourceEntryId: "entry-1",
        tasks: [
          { id: 1, identityEntryId: "id-1", subject: "Ship", status: "in_progress" },
          { id: 2, identityEntryId: "id-2", subject: "Hidden", status: "deleted" },
        ],
      }),
      updatedAt: DateTime.makeUnsafe("2026-09-04T00:00:00.000Z"),
    });

    expect(projected.plan).toMatchObject({
      runId: null,
      kind: "todo_list",
      piChecklistProvenance: "pi_native",
      steps: [{ text: "Ship", status: "running" }],
    });
    if (projected.plan?.kind !== "todo_list") throw new Error("expected a todo_list");
    expect(projected.plan.steps[0]).not.toHaveProperty("durationMs");
    expect(projected.turnItem).toMatchObject({
      runId: null,
      type: "todo_list",
      startedAt: null,
      completedAt: null,
    });
    expect(projected.pointer).toMatchObject({ state: "ready", sourceEntryId: "entry-1" });
  });

  it("projects a proven empty branch as an empty plan, not unavailable", () => {
    const projected = projectPiChecklist({
      capture: capture({ state: "ready", sourceEntryId: null, tasks: [] }),
      updatedAt: DateTime.makeUnsafe("2026-09-04T00:00:00.000Z"),
    });

    expect(projected.pointer).toMatchObject({
      state: "ready",
      sourceEntryId: null,
      planId: expect.any(String),
    });
    if (projected.plan?.kind !== "todo_list") throw new Error("expected a todo_list");
    expect(projected.plan.steps).toEqual([]);
  });

  it("projects a malformed selection as unavailable with no artifact", () => {
    const projected = projectPiChecklist({
      capture: capture({ state: "unavailable", sourceEntryId: "entry-1", tasks: [] }),
      updatedAt: DateTime.makeUnsafe("2026-09-04T00:00:00.000Z"),
    });

    expect(projected.pointer).toMatchObject({
      state: "unavailable",
      sourceEntryId: null,
      planId: null,
    });
    expect(projected.plan).toBeNull();
    expect(projected.turnItem).toBeNull();
  });
});

describe("selectCurrentChecklistPlan", () => {
  const pointer = decodePointer({
    state: "ready",
    list: { threadId: "thread-1", providerInstanceId: "pi", nativeSessionId: "session-a" },
    ownerGeneration: 1,
    sessionGeneration: 1,
    captureVersion: 2,
    sourceEntryId: "entry-1",
    planId: "plan-current",
  });

  it("returns the pointed plan and ignores a later array entry", () => {
    const pointed = { id: "plan-current", kind: "todo_list", piChecklistProvenance: "pi_native" };
    const later = { id: "plan-later", kind: "todo_list" };
    expect(
      selectCurrentChecklistPlan({
        plans: [pointed, later],
        activeProviderThread: {
          driver: "pi",
          nativeMetadata: { checklistSelection: pointer },
        },
      }),
    ).toBe(pointed);
  });

  it("does not fall back when the Pi pointer is unavailable", () => {
    expect(
      selectCurrentChecklistPlan({
        plans: [{ id: "plan-later", kind: "todo_list" }],
        activeProviderThread: {
          driver: "pi",
          nativeMetadata: {
            checklistSelection: decodePointer({
              ...pointer,
              state: "unavailable",
              sourceEntryId: null,
              planId: null,
            }),
          },
        },
      }),
    ).toBeNull();
  });

  it("leaves non-Pi threads to their existing reader", () => {
    expect(
      selectCurrentChecklistPlan({
        plans: [{ id: "plan-codex", kind: "todo_list" }],
        activeProviderThread: { driver: "codex", nativeMetadata: null },
      }),
    ).toBeUndefined();
  });

  it("does not accept a pointed historical plan without Pi checklist provenance", () => {
    for (const plan of [
      { id: "plan-current", kind: "todo_list" },
      { id: "plan-current", kind: "proposed_plan", piChecklistProvenance: "pi_native" },
    ]) {
      expect(
        selectCurrentChecklistPlan({
          plans: [plan],
          activeProviderThread: { driver: "pi", nativeMetadata: { checklistSelection: pointer } },
        }),
      ).toBeNull();
    }
    expect(
      selectCurrentChecklistPlan({
        plans: [{ id: "plan-current", kind: "todo_list", piChecklistProvenance: "pi_native" }],
        activeProviderThread: { driver: "pi", nativeMetadata: null },
      }),
    ).toBeNull();
    expect(
      selectCurrentChecklistPlan({
        plans: [{ id: "plan-current", kind: "todo_list", piChecklistProvenance: "pi_native" }],
        activeProviderThread: {
          driver: "pi",
          nativeMetadata: { checklistSelection: { ...pointer, sourceEntryId: null } },
        },
      }),
    ).toBeNull();
  });
});
