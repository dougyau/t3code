import * as Schema from "effect/Schema";
import { NonNegativeInt, ThreadId, TrimmedNonEmptyString } from "./baseSchemas.ts";
import { ProviderInstanceId } from "./providerInstance.ts";

const Strings = Schema.Array(Schema.String);
const Doubt = Schema.Struct({ question: Schema.String, purpose: Schema.String });
const Doubts = Schema.Array(Doubt);
const Resolution = Schema.Struct({
  doubtId: Schema.String,
  status: Schema.Literals(["answered", "partial", "rejected", "failed"]),
  answer: Schema.String,
  supportIds: Strings,
});
export const PiArchitectureSynthesis = Schema.Struct({
  kind: Schema.Literal("synthesis"),
  status: Schema.Literals(["complete", "partial", "failed"]),
  recommendation: Schema.String,
  alternatives: Strings,
  evidence: Strings,
  assumptions: Strings,
  disagreements: Strings,
  pending: Strings,
});
export const PiArchitectureBody = Schema.Union([
  Schema.Struct({
    kind: Schema.Literal("composition"),
    snapshotId: Schema.String,
    specialists: Schema.Array(
      Schema.Struct({
        candidateRef: Schema.String,
        mandate: Schema.String,
        reason: Schema.String,
      }),
    ),
  }),
  Schema.Struct({ kind: Schema.Literal("preparation"), analysis: Schema.String, doubts: Doubts }),
  Schema.Struct({
    kind: Schema.Literal("contribution"),
    proposal: Schema.String,
    arguments: Strings,
    objections: Schema.Array(
      Schema.Struct({
        memberId: Schema.String,
        messageId: Schema.String,
        argument: Schema.String,
      }),
    ),
    doubts: Doubts,
  }),
  Schema.Struct({
    kind: Schema.Literal("decision"),
    action: Schema.Literals(["continue", "consult", "synthesize", "recompose"]),
    reason: Schema.String,
  }),
  Schema.Struct({
    kind: Schema.Literal("answer"),
    answer: Schema.String,
    evidence: Strings,
    limits: Strings,
    doubts: Doubts,
  }),
  Schema.Struct({
    kind: Schema.Literal("consultation-plan"),
    resolutions: Schema.Array(
      Schema.Struct({
        doubtId: Schema.String,
        status: Schema.Literals(["answered", "rejected"]),
        answer: Schema.String,
      }),
    ),
    support: Schema.Array(
      Schema.Struct({
        role: Schema.Literals(["scout", "researcher", "oracle"]),
        brief: Schema.String,
        doubtIds: Strings,
        prerequisites: Schema.Array(NonNegativeInt),
      }),
    ),
  }),
  Schema.Struct({
    kind: Schema.Literal("consultation-response"),
    doubtIds: Strings,
    answer: Schema.String,
    evidence: Strings,
    limits: Strings,
    doubts: Doubts,
  }),
  Schema.Struct({ kind: Schema.Literal("knowledge"), resolutions: Schema.Array(Resolution) }),
  PiArchitectureSynthesis,
]);
export type PiArchitectureBody = typeof PiArchitectureBody.Type;
const Stage = Schema.Literals([
  "composition",
  "preparation",
  "contribution",
  "decision",
  "answer",
  "consultation-plan",
  "consultation-response",
  "knowledge",
  "synthesis",
]);
const Closure = Schema.Literals(["not-created", "pending", "released", "unknown"]);
export const PiArchitectureLifecycle = Schema.Struct({
  stage: Schema.NullOr(Schema.Union([Stage, Schema.Literals(["waiting", "closing"])])),
  round: NonNegativeInt,
  protocolState: Schema.Literals([
    "starting",
    "running",
    "waiting",
    "finished",
    "failed",
    "cancelled",
    "closure-unknown",
  ]),
  outcome: Schema.Literals(["pending", "available", "unavailable"]),
  closure: Schema.Struct({
    state: Schema.Literals(["pending", "released", "unknown"]),
    members: Closure,
    director: Closure,
  }),
});
export const PiArchitectureOwner = Schema.Struct({
  processId: TrimmedNonEmptyString,
  coordinatorSessionId: TrimmedNonEmptyString,
  epoch: TrimmedNonEmptyString,
});
export const PiArchitectureWorkerRef = Schema.Struct({
  ...PiArchitectureOwner.fields,
  kind: Schema.Literal("read"),
  instanceId: TrimmedNonEmptyString,
  source: Schema.Literals(["agent_run", "agent_spawn", "team_assign"]),
  assignmentId: Schema.optional(Schema.String),
  evaluationId: Schema.optional(Schema.String),
  attemptId: Schema.optional(Schema.String),
  native: Schema.optional(
    Schema.Struct({
      generation: NonNegativeInt,
      historyId: Schema.String,
      sessionId: Schema.String,
    }),
  ),
});
export const PiArchitectureMember = Schema.Struct({
  kind: Schema.Literal("member"),
  id: TrimmedNonEmptyString,
  assignmentId: TrimmedNonEmptyString,
  instanceId: Schema.String,
  roleId: Schema.String,
  function: Schema.Literals(["director", "specialist", "support"]),
  ordinal: NonNegativeInt,
  admittedAt: Schema.Number,
  mandate: Schema.Union([
    Schema.Struct({ source: Schema.Literal("root-workbrief"), instanceId: Schema.String }),
    Schema.Struct({ source: Schema.Literal("TeamAssignment.task"), text: Schema.String }),
  ]),
  execution: Schema.Literals(["accepted", "running", "completed", "failed", "cancelled"]),
  workerRef: Schema.optional(PiArchitectureWorkerRef),
  conversationRefs: Schema.Array(PiArchitectureWorkerRef),
  compositionMessageId: Schema.optional(Schema.String),
  supersededByCompositionId: Schema.optional(Schema.String),
});
export type PiArchitectureMember = typeof PiArchitectureMember.Type;
export const PiArchitectureRecord = Schema.Union([
  Schema.Struct({
    kind: Schema.Literal("room"),
    id: Schema.String,
    mode: Schema.Literals(["table", "consultation"]),
    origin: Schema.Struct({
      coordinatorSessionId: Schema.String,
      tool: Schema.Literal("architecture_start"),
      toolCallId: Schema.String,
      memberId: Schema.String,
      assignmentId: Schema.String,
      instanceId: Schema.String,
    }),
    goalSource: Schema.Struct({
      kind: Schema.Literal("worker-workbrief"),
      instanceId: Schema.String,
    }),
    lifecycle: PiArchitectureLifecycle,
    pendingDoubtIds: Strings,
  }),
  PiArchitectureMember,
  Schema.Struct({
    kind: Schema.Literal("publication"),
    id: Schema.String,
    message: Schema.Struct({
      id: Schema.String,
      ordinal: NonNegativeInt,
      roomId: Schema.String,
      assignmentId: Schema.String,
      memberId: Schema.String,
      author: Schema.String,
      stage: Stage,
      snapshotId: Schema.String,
      round: Schema.optional(NonNegativeInt),
      turnId: Schema.String,
      attemptId: Schema.String,
      body: PiArchitectureBody,
      native: Schema.Struct({
        historyId: Schema.optional(Schema.String),
        sessionId: Schema.String,
        toolCallId: Schema.String,
        persistence: Schema.Literal("unconfirmed"),
      }),
      doubts: Schema.Array(Schema.Struct({ id: Schema.String, ...Doubt.fields })),
    }),
  }),
  Schema.Struct({
    kind: Schema.Literal("control"),
    id: Schema.String,
    order: NonNegativeInt,
    action: Schema.Literals(["context", "priorities", "finish", "stop"]),
    text: Schema.optional(Schema.String),
    author: Schema.Struct({ coordinatorSessionId: Schema.String, toolCallId: Schema.String }),
    application: Schema.Union([
      Schema.Struct({ state: Schema.Literal("pending") }),
      Schema.Struct({
        state: Schema.Literal("applied"),
        stage: PiArchitectureLifecycle.fields.stage,
        round: NonNegativeInt,
      }),
    ]),
  }),
  Schema.Struct({
    kind: Schema.Literal("failure"),
    id: Schema.String,
    phase: Schema.Literals(["preparation", "admission", "turn", "disband", "director-close"]),
    error: Schema.String,
    memberId: Schema.optional(Schema.String),
    assignmentId: Schema.optional(Schema.String),
    stage: Schema.optional(Stage),
    round: Schema.optional(NonNegativeInt),
  }),
  Schema.Struct({
    kind: Schema.Literal("outcome"),
    id: Schema.Literal("final"),
    source: Schema.Literal("background-settlement"),
    status: Schema.Literals(["completed", "failed", "cancelled"]),
    content: Schema.Union([
      Schema.Struct({ kind: Schema.Literal("synthesis"), body: PiArchitectureSynthesis }),
      Schema.Struct({ kind: Schema.Literal("text"), text: Schema.String }),
    ]),
  }),
  Schema.Struct({
    kind: Schema.Literal("knowledge-boundary"),
    id: Schema.String,
    ordinal: NonNegativeInt,
    roomId: Schema.String,
    cycleId: Schema.String,
    planMessageId: Schema.String,
    knowledgeMessageId: Schema.NullOr(Schema.String),
    resolutions: Schema.Array(
      Schema.Struct({
        ...Resolution.fields,
        provenance: Schema.Union([
          Schema.Struct({ kind: Schema.Literal("direct"), planMessageId: Schema.String }),
          Schema.Struct({
            kind: Schema.Literal("delegated"),
            knowledgeMessageId: Schema.String,
            consultationId: Schema.String,
            responses: Schema.Array(
              Schema.Struct({
                consultationId: Schema.String,
                assignmentId: Schema.optional(Schema.String),
                messageId: Schema.optional(Schema.String),
                status: Schema.Literals(["completed", "failed"]),
              }),
            ),
          }),
        ]),
      }),
    ),
  }),
]);
export type PiArchitectureRecord = typeof PiArchitectureRecord.Type;

export const PiArchitectureAnchor = Schema.Struct({
  threadId: ThreadId,
  providerInstanceId: ProviderInstanceId,
  coordinatorSessionId: TrimmedNonEmptyString,
  roomId: TrimmedNonEmptyString,
});
export type PiArchitectureAnchor = typeof PiArchitectureAnchor.Type;
export const PiArchitectureText = Schema.Struct({
  id: Schema.String,
  text: Schema.String,
  source: Schema.Literals(["original WorkBrief", "invocation task", "unavailable"]),
  complete: Schema.Boolean,
});
export type PiArchitectureText = typeof PiArchitectureText.Type;
export const PiArchitectureSummary = Schema.Struct({
  tableId: TrimmedNonEmptyString,
  ...PiArchitectureAnchor.fields,
  mode: Schema.Literals(["table", "consultation"]),
  participants: NonNegativeInt,
  lifecycle: PiArchitectureLifecycle,
  capture: Schema.Literals(["open", "closed", "incomplete"]),
  observation: Schema.Literals(["current", "stale", "partial"]),
  gaps: Strings,
});
export type PiArchitectureSummary = typeof PiArchitectureSummary.Type;
export const PiArchitectureCheckpoint = Schema.Struct({
  owner: PiArchitectureOwner,
  ownerGeneration: NonNegativeInt,
  snapshotId: Schema.String,
  cut: NonNegativeInt,
});
export type PiArchitectureCheckpoint = typeof PiArchitectureCheckpoint.Type;

const Table = { threadId: ThreadId, tableId: TrimmedNonEmptyString };
export const PiArchitectureReadInput = Schema.Union([
  Schema.Struct({
    kind: Schema.Literal("tables"),
    threadId: ThreadId,
    cursor: Schema.optional(Schema.String),
  }),
  Schema.Struct({
    kind: Schema.Literal("records"),
    ...Table,
    cursor: Schema.optional(Schema.String),
  }),
  Schema.Struct({
    kind: Schema.Literal("text"),
    ...Table,
    textId: TrimmedNonEmptyString,
    offset: Schema.optional(NonNegativeInt),
    revision: Schema.optional(Schema.String),
  }),
]);
export type PiArchitectureReadInput = typeof PiArchitectureReadInput.Type;
export const PiArchitectureDetailRecord = Schema.Union([
  Schema.Struct({
    kind: Schema.Literal("room"),
    id: Schema.String,
    mode: Schema.Literals(["table", "consultation"]),
    lifecycle: PiArchitectureLifecycle,
    pendingDoubtIds: Strings,
    originalTextId: Schema.Literal("original"),
  }),
  Schema.Struct({
    kind: Schema.Literal("member"),
    id: Schema.String,
    assignmentId: Schema.String,
    instanceId: Schema.String,
    roleId: Schema.String,
    function: PiArchitectureMember.fields.function,
    execution: PiArchitectureMember.fields.execution,
    ordinal: NonNegativeInt,
    mandateSource: Schema.Literals(["root-workbrief", "TeamAssignment.task"]),
    textId: Schema.String,
    workerThreadId: Schema.NullOr(ThreadId),
    model: Schema.optional(
      Schema.Struct({
        selected: Schema.optional(Schema.String),
        activated: Schema.optional(Schema.String),
      }),
    ),
    bindingCount: NonNegativeInt,
    supersededByCompositionId: Schema.optional(Schema.String),
  }),
  Schema.Struct({
    kind: Schema.Literal("publication"),
    id: Schema.String,
    ordinal: NonNegativeInt,
    memberId: Schema.String,
    assignmentId: Schema.String,
    stage: Stage,
    round: Schema.optional(NonNegativeInt),
    turnId: Schema.String,
    attemptId: Schema.String,
    snapshotId: Schema.String,
    messageId: Schema.String,
    textId: Schema.String,
  }),
  Schema.Struct({
    kind: Schema.Literal("control"),
    id: Schema.String,
    action: Schema.String,
    order: NonNegativeInt,
    author: Schema.Struct({ coordinatorSessionId: Schema.String, toolCallId: Schema.String }),
    textId: Schema.String,
  }),
  Schema.Struct({
    kind: Schema.Literal("failure"),
    id: Schema.String,
    phase: Schema.String,
    memberId: Schema.optional(Schema.String),
    assignmentId: Schema.optional(Schema.String),
    stage: Schema.optional(Stage),
    round: Schema.optional(NonNegativeInt),
    textId: Schema.String,
  }),
  Schema.Struct({
    kind: Schema.Literal("outcome"),
    id: Schema.String,
    status: Schema.Literals(["completed", "failed", "cancelled"]),
    textId: Schema.String,
  }),
  Schema.Struct({
    kind: Schema.Literal("knowledge-boundary"),
    id: Schema.String,
    ordinal: NonNegativeInt,
    textId: Schema.String,
  }),
]);
export type PiArchitectureDetailRecord = typeof PiArchitectureDetailRecord.Type;
export const PiArchitectureReadResult = Schema.Union([
  Schema.Struct({
    kind: Schema.Literal("tables"),
    items: Schema.Array(PiArchitectureSummary),
    next: Schema.NullOr(Schema.String),
  }),
  Schema.Struct({
    kind: Schema.Literal("records"),
    summary: PiArchitectureSummary,
    items: Schema.Array(PiArchitectureDetailRecord),
    next: Schema.NullOr(Schema.String),
  }),
  Schema.Struct({
    kind: Schema.Literal("text"),
    textId: Schema.String,
    revision: Schema.String,
    offset: NonNegativeInt,
    text: Schema.String,
    nextOffset: Schema.NullOr(NonNegativeInt),
    format: Schema.Literals(["text", "json"]),
    complete: Schema.Boolean,
    source: Schema.String,
  }),
]);
export type PiArchitectureReadResult = typeof PiArchitectureReadResult.Type;
