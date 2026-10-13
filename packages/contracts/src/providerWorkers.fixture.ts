export const legacyWorkerRow = {
  id: "worker", roleId: "developer", status: "completed" as const,
  source: "agent_spawn" as const, task: "passive inspection", selectedModel: "fixture",
  sdkRequest: null, acceptedAt: 1, activity: "finished", phase: "idle" as const,
};

/** Independent dimensions include combinations T3 must preserve, not infer or correct. */
export const handoffWorkerRows = [
  legacyWorkerRow,
  { ...legacyWorkerRow, resultTaken: false },
  { ...legacyWorkerRow, resultTaken: true },
  ...(["pending", "reconciled", "inapplicable", "blocked"] as const).flatMap((bookkeeping) =>
    (["pending", "offered", "incorporated", "blocked", "dropped"] as const).flatMap((delivery) =>
      (["not-requested", "requested", "observed", "blocked-unconfirmed"] as const).map((continuation) => ({
        ...legacyWorkerRow, resultTaken: false,
        handoff: { collection: "collected" as const, bookkeeping, delivery, continuation },
      })),
    ),
  ),
];

export const invalidHandoffWorkerRows = [
  { ...legacyWorkerRow, resultTaken: "false" },
  { ...legacyWorkerRow, handoff: null },
  { ...legacyWorkerRow, handoff: {} },
  ...["collection", "bookkeeping", "delivery", "continuation"].flatMap((key) => {
    const handoff = {
      collection: "collected", bookkeeping: "pending", delivery: "offered", continuation: "requested",
    };
    return [
      { ...legacyWorkerRow, handoff: Object.fromEntries(Object.entries(handoff).filter(([name]) => name !== key)) },
      { ...legacyWorkerRow, handoff: { ...handoff, [key]: "future-state" } },
    ];
  }),
];
