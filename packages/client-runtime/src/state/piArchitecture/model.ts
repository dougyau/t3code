import {
  PiArchitectureBody,
  PiArchitectureRecord,
  type EnvironmentId,
  type PiArchitectureDetailRecord,
  type PiArchitectureSummary,
  type OrchestrationV2Subagent,
  type ThreadId,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { Atom } from "effect/reactivity";

export type TableTarget = {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
  readonly tableId: string;
};
export const tableTabs = ["Summary", "Debate", "Participants"] as const;
export type TableTab = (typeof tableTabs)[number];
export const tableKey = (target: TableTarget) =>
  JSON.stringify([target.environmentId, target.threadId, target.tableId]);
export interface ReadingPoint {
  readonly id: string;
  readonly offset: number;
  readonly fallback: number;
}
export interface TablePresentation {
  readonly tab: TableTab;
  readonly source: boolean;
  readonly briefOpen: boolean;
  readonly expanded: Readonly<Record<string, boolean>>;
  readonly reading: Partial<Record<TableTab | "brief", ReadingPoint>>;
}
const presentation = Atom.family((_key: string) =>
  Atom.make<TablePresentation>({
    tab: "Summary",
    source: false,
    briefOpen: false,
    expanded: {},
    reading: {},
  }).pipe(Atom.keepAlive),
);
export const tablePresentationAtom = (target: TableTarget) => presentation(tableKey(target));
export function isTableWorker(table: PiArchitectureSummary, worker: OrchestrationV2Subagent) {
  const launch = worker.nativeWorker?.launch;
  return (
    launch?.coordinatorSessionId === table.coordinatorSessionId &&
    launch.providerInstanceId === table.providerInstanceId &&
    ((launch.source === "architecture_roster" && launch.roomId === table.roomId) ||
      (launch.source === "team_assign" && launch.entrypoint === "architecture_start"))
  );
}
export type Member = Extract<PiArchitectureDetailRecord, { kind: "member" }>;
export type Publication = Extract<PiArchitectureDetailRecord, { kind: "publication" }>;

export function participantModel(member: Member) {
  return `Selected model: ${member.model?.selected ?? "unavailable"} · Activated model: ${member.model?.activated ?? "unavailable"}`;
}

export function debateGroups(records: readonly PiArchitectureDetailRecord[]) {
  const groups = new Map<string, Publication[]>();
  for (const record of records) {
    if (record.kind !== "publication") continue;
    const key = record.round === undefined ? "Preparation" : `Round ${record.round}`;
    const group = groups.get(key) ?? [];
    group.push(record);
    groups.set(key, group);
  }
  return [...groups]
    .map(([id, publications]) => ({
      id,
      publications: [...publications].sort((a, b) => a.ordinal - b.ordinal),
    }))
    .sort((a, b) => (a.publications[0]?.round ?? -1) - (b.publications[0]?.round ?? -1));
}
export function tableObservation(summary: PiArchitectureSummary) {
  return [
    `Observation ${summary.observation}`,
    `Capture ${summary.capture}`,
    `Protocol ${summary.lifecycle.protocolState}`,
    `Outcome ${summary.lifecycle.outcome}`,
    `Native closure ${summary.lifecycle.closure.state}`,
    `Members ${summary.lifecycle.closure.members}`,
    `Director ${summary.lifecycle.closure.director}`,
  ].join(" · ");
}
const decodePublication = Schema.decodeUnknownSync(
  Schema.fromJsonString(Schema.Struct({ body: PiArchitectureBody })),
);
const decodeOutcome = Schema.decodeUnknownSync(
  Schema.fromJsonString(PiArchitectureRecord.members[5].fields.content),
);
const decodeMandate = Schema.decodeUnknownSync(
  Schema.fromJsonString(Schema.Struct({ mandate: PiArchitectureRecord.members[1].fields.mandate })),
);
const decodeControl = Schema.decodeUnknownSync(
  Schema.fromJsonString(
    Schema.Struct({
      text: Schema.optional(Schema.String),
      application: PiArchitectureRecord.members[3].fields.application,
    }),
  ),
);
export interface ContentSection {
  readonly title: string;
  readonly text: string;
  readonly reference?: { readonly memberId: string; readonly messageId: string };
}
/** Public structured bodies supply relations; Markdown never supplies identity. */
export function bodySections(body: PiArchitectureBody): readonly ContentSection[] {
  const list = (title: string, values: readonly string[]) =>
    values.map((text) => ({ title, text }));
  const doubts = (values: readonly { question: string; purpose: string }[]) =>
    values.map((doubt) => ({ title: doubt.question, text: doubt.purpose }));
  switch (body.kind) {
    case "composition":
      return body.specialists.map((member) => ({
        title: member.candidateRef,
        text: `${member.mandate}\n\n${member.reason}`,
      }));
    case "preparation":
      return [{ title: "Analysis", text: body.analysis }, ...doubts(body.doubts)];
    case "contribution":
      return [
        { title: "Proposal", text: body.proposal },
        ...list("Argument", body.arguments),
        ...body.objections.map((objection) => ({
          title: "Objection",
          text: objection.argument,
          reference: { memberId: objection.memberId, messageId: objection.messageId },
        })),
        ...doubts(body.doubts),
      ];
    case "decision":
      return [{ title: `Director decision · ${body.action}`, text: body.reason }];
    case "answer":
    case "consultation-response":
      return [
        { title: "Answer", text: body.answer },
        ...list("Evidence", body.evidence),
        ...list("Limit", body.limits),
        ...doubts(body.doubts),
      ];
    case "consultation-plan":
      return [
        ...body.resolutions.map((resolution) => ({
          title: `${resolution.doubtId} · ${resolution.status}`,
          text: resolution.answer,
        })),
        ...body.support.map((support) => ({ title: support.role, text: support.brief })),
      ];
    case "knowledge":
      return body.resolutions.map((resolution) => ({
        title: `${resolution.doubtId} · ${resolution.status}`,
        text: resolution.answer,
      }));
    case "synthesis":
      return [
        { title: `Synthesis · ${body.status}`, text: body.recommendation },
        ...list("Alternative", body.alternatives),
        ...list("Evidence", body.evidence),
        ...list("Assumption", body.assumptions),
        ...list("Disagreement", body.disagreements),
        ...list("Pending", body.pending),
      ];
  }
}
export function recordSections(
  record: PiArchitectureDetailRecord,
  text: string,
): readonly ContentSection[] {
  try {
    switch (record.kind) {
      case "publication":
        return bodySections(decodePublication(text).body);
      case "outcome": {
        const outcome = decodeOutcome(text);
        return outcome.kind === "text"
          ? [{ title: `Confirmed outcome · ${record.status}`, text: outcome.text }]
          : bodySections(outcome.body).map((section, index) =>
              index === 0
                ? { ...section, title: `Confirmed outcome · ${record.status} · ${section.title}` }
                : section,
            );
      }
      case "member": {
        const { mandate } = decodeMandate(text);
        return [
          {
            title: `Individual mandate · ${mandate.source}`,
            text: mandate.source === "TeamAssignment.task" ? mandate.text : "See original brief",
          },
        ];
      }
      case "control": {
        const control = decodeControl(text);
        return [
          {
            title: `${record.action} · ${control.application.state} · ${record.author.coordinatorSessionId} · instruction ${record.order}`,
            text: control.text ?? "No instruction text captured",
          },
        ];
      }
      case "failure":
        return [
          {
            title: `Failure · ${record.phase} · ${record.memberId ?? record.assignmentId ?? "table"}${record.stage === undefined ? "" : ` · ${record.stage}`}${record.round === undefined ? "" : ` · round ${record.round}`}`,
            text,
          },
        ];
      default:
        return [];
    }
  } catch {
    return [
      {
        title: "Content unavailable",
        text: "The captured structured content could not be decoded.",
      },
    ];
  }
}
