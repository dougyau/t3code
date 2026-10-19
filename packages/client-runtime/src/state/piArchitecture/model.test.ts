import { EnvironmentId, ThreadId, type PiArchitectureDetailRecord } from "@t3tools/contracts";
import { AtomRegistry } from "effect/reactivity";
import { expect, it } from "vite-plus/test";
import { debateGroups, recordSections, tablePresentationAtom, tableTabs } from "./model.ts";

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
