import {
  EnvironmentId,
  ProviderInstanceId,
  ThreadId,
  type PiArchitectureDetailRecord,
  type PiArchitectureSummary,
} from "@t3tools/contracts";
import { AtomRegistry } from "effect/reactivity";
import { expect, it } from "vite-plus/test";
import {
  debateGroups,
  recordSections,
  tablePresentationAtom,
  tableStatus,
  tableTabs,
} from "./model.ts";

const historical: PiArchitectureSummary = {
  tableId: "opaque-table-id",
  threadId: ThreadId.make("coordinator"),
  providerInstanceId: ProviderInstanceId.make("pi"),
  coordinatorSessionId: "session",
  roomId: "room",
  mode: "table",
  participants: 2,
  lifecycle: {
    stage: "closing",
    round: 2,
    protocolState: "finished",
    outcome: "available",
    closure: { state: "released", members: "released", director: "released" },
  },
  capture: "closed",
  observation: "stale",
  gaps: [],
};

it("presents a finished historical table without claiming success or exposing diagnostic enums", () => {
  expect(tableStatus(historical)).toEqual({
    state: "Finished",
    result: "Result available",
    context: "Historical view",
    warnings: [],
  });
});

it("keeps active work last-known when stale and never infers completion from other fields", () => {
  const running = {
    ...historical,
    lifecycle: { ...historical.lifecycle, protocolState: "running" as const },
  };
  expect(tableStatus({ ...running, observation: "current" }).state).toBe("Running");
  expect(tableStatus(running)).toMatchObject({
    state: "Last known: Running",
    context: "Historical view",
  });
  expect(
    tableStatus({
      ...running,
      lifecycle: { ...running.lifecycle, protocolState: "waiting", outcome: "pending" },
    }),
  ).toMatchObject({ state: "Last known: Waiting", result: "Result pending" });
});

it("does not let available results or closed capture conceal failure, cancellation or uncertainty", () => {
  for (const [protocolState, state] of [
    ["failed", "Failed"],
    ["cancelled", "Cancelled"],
    ["closure-unknown", "Completion unconfirmed"],
  ] as const) {
    expect(
      tableStatus({ ...historical, lifecycle: { ...historical.lifecycle, protocolState } }).state,
    ).toBe(state);
  }
  const uncertain = tableStatus({
    ...historical,
    observation: "partial",
    capture: "incomplete",
    gaps: ["Missing final interval"],
    lifecycle: {
      ...historical.lifecycle,
      closure: { state: "unknown", members: "released", director: "unknown" },
    },
  });
  expect(uncertain).toMatchObject({
    state: "Completion unconfirmed",
    context: "Partial view",
  });
  expect(uncertain.warnings.join(" ")).toContain("Only part of this table was observed.");
  expect(uncertain.warnings.join(" ")).toContain("Capture is incomplete.");
  expect(uncertain.warnings.join(" ")).toContain("Missing final interval");
  expect(uncertain.warnings.join(" ")).toContain("Native closure could not be confirmed.");
});

it("isolates reading anchors by environment, coordinator and table", () => {
  const registry = AtomRegistry.make();
  const target = {
    environmentId: EnvironmentId.make("remote"),
    threadId: ThreadId.make("parent"),
    tableId: "table",
  };
  try {
    registry.update(tablePresentationAtom(target), (state) => ({
      ...state,
      tab: tableTabs[1],
      source: true,
      expanded: { "Round 1": true },
      reading: { Debate: { id: "publication:original-id", offset: 12, fallback: 300 } },
    }));
    expect(registry.get(tablePresentationAtom(target))).toMatchObject({
      tab: "Debate",
      source: true,
      reading: { Debate: { id: "publication:original-id", offset: 12 } },
    });
    for (const other of [
      { ...target, environmentId: EnvironmentId.make("other") },
      { ...target, threadId: ThreadId.make("other") },
      { ...target, tableId: "other" },
    ])
      expect(registry.get(tablePresentationAtom(other))).toMatchObject({
        tab: "Summary",
        reading: {},
        expanded: {},
      });
  } finally {
    registry.dispose();
  }
});

it("groups only native rounds, orders publications, and preserves explicit objection identities", () => {
  const publication = (
    id: string,
    ordinal: number,
    round?: number,
  ): PiArchitectureDetailRecord => ({
    kind: "publication",
    id,
    ordinal,
    memberId: "member",
    assignmentId: "assignment",
    messageId: id,
    textId: `publication:${id}`,
    stage: "preparation",
    snapshotId: "snapshot",
    turnId: "turn",
    attemptId: "attempt",
    ...(round === undefined ? {} : { round }),
  });
  const first = publication("first", 1);
  const second = publication("second", 3, 1);
  const groups = debateGroups([first, second, publication("earlier", 2, 1)]);
  expect(
    groups.map((group) => [group.id, group.publications.map((record) => record.messageId)]),
  ).toEqual([
    ["Preparation", ["first"]],
    ["Round 1", ["earlier", "second"]],
  ]);
  const sections = recordSections(
    second,
    JSON.stringify({
      body: {
        kind: "contribution",
        proposal: "# Round 999 🧭",
        arguments: [],
        objections: [
          { memberId: "other-member", messageId: "native-message", argument: "literal" },
        ],
        doubts: [],
      },
    }),
  );
  expect(sections).toContainEqual({
    title: "Objection",
    text: "literal",
    reference: { memberId: "other-member", messageId: "native-message" },
  });
  expect(groups).toHaveLength(2);
});

it("labels structured confirmed outcomes without inventing a roster author or text field", () => {
  const sections = recordSections(
    { kind: "outcome", id: "outcome", textId: "outcome:outcome", status: "completed" },
    JSON.stringify({
      kind: "synthesis",
      body: {
        kind: "synthesis",
        status: "complete",
        recommendation: "Exact recommendation 🧠",
        alternatives: [],
        evidence: [],
        assumptions: [],
        disagreements: ["Separate disagreement"],
        pending: ["Open obligation"],
      },
    }),
  );
  expect(sections[0]).toEqual({
    title: "Confirmed outcome · completed · Synthesis · complete",
    text: "Exact recommendation 🧠",
  });
  expect(sections).toContainEqual({ title: "Pending", text: "Open obligation" });
  expect(sections).toContainEqual({ title: "Disagreement", text: "Separate disagreement" });
});

it.each([
  {
    body: {
      kind: "composition",
      snapshotId: "s",
      reason: "Director-only is sufficient",
      specialists: [],
    },
    sections: [{ title: "Composition", text: "Director-only is sufficient" }],
  },
  {
    body: {
      kind: "composition",
      snapshotId: "s",
      reason: "Continuity and new evidence",
      specialists: [
        { kind: "retain", memberId: "existing", mandate: "Keep focus", reason: "Continuity" },
        { kind: "add", candidateRef: "candidate", mandate: "Check evidence", reason: "New angle" },
      ],
    },
    sections: [
      { title: "Composition", text: "Continuity and new evidence" },
      { title: "Retain · existing", text: "Keep focus\n\nContinuity" },
      { title: "Add · candidate", text: "Check evidence\n\nNew angle" },
    ],
  },
  {
    body: {
      kind: "composition",
      snapshotId: "old",
      specialists: [
        { candidateRef: "old-candidate", mandate: "Old mandate", reason: "Old reason" },
      ],
    },
    sections: [{ title: "old-candidate", text: "Old mandate\n\nOld reason" }],
  },
  {
    body: {
      kind: "answer",
      answer: "Historical answer",
      evidence: ["Proof"],
      limits: [],
      doubts: [],
    },
    sections: [
      { title: "Answer", text: "Historical answer" },
      { title: "Evidence", text: "Proof" },
    ],
  },
  {
    body: {
      kind: "consultation-plan",
      resolutions: [],
      support: [{ role: "scout", brief: "Historical support", doubtIds: ["d"], prerequisites: [] }],
    },
    sections: [{ title: "scout", text: "Historical support" }],
  },
])(
  "presents current composition and historical bodies without losing their content: %j",
  ({ body, sections }) => {
    expect(
      recordSections(
        {
          kind: "publication",
          id: "p",
          ordinal: 1,
          memberId: "director",
          assignmentId: "assignment",
          messageId: "p",
          textId: "publication:p",
          stage: "composition",
          snapshotId: "s",
          turnId: "t",
          attemptId: "a",
        },
        JSON.stringify({ body }),
      ),
    ).toEqual(sections);
  },
);

it.each(["composition", "TeamAssignment.task", "root-workbrief"] as const)(
  "presents truthful individual mandate source: %s",
  (source) => {
    expect(
      recordSections(
        {
          kind: "member",
          id: "member",
          assignmentId: "assignment",
          instanceId: "instance",
          roleId: "architect",
          function: "specialist",
          execution: "running",
          ordinal: 1,
          mandateSource: source,
          textId: "member:member",
          workerThreadId: null,
          bindingCount: 0,
        },
        JSON.stringify({
          mandate:
            source === "root-workbrief"
              ? { source, instanceId: "instance" }
              : { source, text: "Individual task, not the root goal" },
        }),
      ),
    ).toEqual([
      {
        title: `Individual mandate · ${source}`,
        text:
          source === "root-workbrief" ? "See original brief" : "Individual task, not the root goal",
      },
    ]);
  },
);

it("keeps later instruction author/order/application separate and reports corrupt capture", () => {
  const control = {
    kind: "control" as const,
    id: "control",
    textId: "control:control",
    action: "context" as const,
    order: 7,
    author: { coordinatorSessionId: "coordinator", toolCallId: "call" },
  };
  expect(
    recordSections(
      control,
      JSON.stringify({ text: "Later instruction", application: { state: "pending" } }),
    ),
  ).toEqual([
    { title: "context · pending · coordinator · instruction 7", text: "Later instruction" },
  ]);
  expect(recordSections(control, "{broken")).toEqual([
    {
      title: "Content unavailable",
      text: "The captured structured content could not be decoded.",
    },
  ]);
});
