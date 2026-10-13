import * as Schema from "effect/Schema";
import { EnvironmentId, ThreadId, NonNegativeInt } from "./baseSchemas.ts";
import { RuntimeMode } from "./providerPolicy.ts";

const Id = Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(512));
const Cursor = Schema.String.check(Schema.isMaxLength(8192));
const Text = Schema.String.check(Schema.isMaxLength(1024));
const Rows = <S extends Schema.Top>(schema: S) => Schema.Array(schema).check(Schema.isMaxLength(50));
/** Canonical decision identity; its ordinal is never a native sequence or a capability. */
export function parseWorkerActionId(value: string) {
  if (value.length > 2048) return undefined;
  const [version, namespace, counter, nonce, extra] = value.split(":");
  if (version !== "w1" || !namespace || !counter || !nonce || extra !== undefined ||
    !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(nonce)) return undefined;
  const ordinal = Number(counter);
  if (!Number.isSafeInteger(ordinal) || ordinal < 1 || String(ordinal) !== counter) return undefined;
  let viewId: string;
  try { viewId = decodeURIComponent(namespace); } catch { return undefined; }
  if (!viewId || viewId.length > 512 || encodeURIComponent(viewId) !== namespace) return undefined;
  return { viewId, ordinal, nonce };
}
export function makeWorkerActionId(viewId: string, ordinal: number, nonce: string) {
  const value = `w1:${encodeURIComponent(viewId)}:${ordinal}:${nonce}`;
  if (!parseWorkerActionId(value)) throw new RangeError("Invalid worker decision identity");
  return value;
}
export const WorkerActionId = Schema.String.check(
  Schema.isMaxLength(2048), Schema.makeFilter((value) => parseWorkerActionId(value) !== undefined),
);
export const WorkerOwner = Schema.Struct({ processId: Id, coordinatorSessionId: Id, epoch: Id });
export const WorkerSelection = Schema.Struct({
  ...WorkerOwner.fields,
  target: Schema.Union([
    Schema.Struct({ kind: Schema.Literal("background"), instanceId: Id }),
    Schema.Struct({ kind: Schema.Literal("team"), assignmentId: Id }),
  ]),
  instanceId: Id, generation: NonNegativeInt, historyId: Id, sessionId: Id,
});
// Native cursor validation compares serialized refs; preserve its field order through RPC codecs.
export const WorkerReference = Schema.Struct({
  kind: Schema.Literal("read"), ...WorkerOwner.fields, instanceId: Id,
  source: Schema.Literals(["agent_run", "agent_spawn", "team_assign"]),
  assignmentId: Schema.optionalKey(Id), evaluationId: Schema.optionalKey(Id), attemptId: Schema.optionalKey(Id),
  native: Schema.optionalKey(Schema.Struct({ generation: NonNegativeInt, historyId: Id, sessionId: Id })),
});
export const WorkerCursor = Schema.Union([
  Schema.Struct({ ref: WorkerReference, section: Schema.Literals(["brief", "detail_metadata", "result_text", "result_metadata"]), offset: NonNegativeInt }),
  Schema.Struct({ ref: WorkerReference, section: Schema.Literal("requests"), offset: NonNegativeInt, digest: Id }),
  Schema.Struct({ ref: WorkerReference, section: Schema.Literal("activity"), after: NonNegativeInt }),
]);
export const WorkerTextPage = Schema.Struct({ text: Text, offset: NonNegativeInt, nextOffset: Schema.NullOr(NonNegativeInt) });
const Status = Schema.Literals(["created", "running", "completed", "failed", "cancelled"]);
const SdkRequest = Schema.Struct({
  attemptId: Schema.optional(Id), historyId: Schema.optional(Id), sessionId: Schema.optional(Id),
  sdkOptions: Schema.optional(Schema.Struct({ reasoning: Schema.optional(Text), temperature: Schema.optional(Schema.Number), maxTokens: Schema.optional(Schema.Number), serviceTier: Schema.optional(Text) })),
  model: Text, provider: Schema.optional(Text), requestId: Schema.optional(Id),
  inputTokens: Schema.optional(Schema.NullOr(Schema.Number)), outputTokens: Schema.optional(Schema.NullOr(Schema.Number)),
  reasoning: Schema.optional(Text), fast_requested: Schema.Boolean,
  requested_service_tier: Schema.optional(Schema.Literals(["fast", "priority"])),
  returned_service_tier: Schema.Literal("unavailable"),
  time_to_first_model_output_ms: Schema.Union([Schema.Number, Schema.Literal("unavailable")]),
  time_to_first_model_output_unavailable_reason: Schema.optional(Schema.Literals(["cancelled", "failed"])),
});
export const WorkerRoster = Schema.Struct({
  owner: WorkerOwner, revision: NonNegativeInt, cursor: Schema.NullOr(Cursor),
  items: Rows(Schema.Struct({
    id: Id, roleId: Id, status: Status,
    source: Schema.optional(Schema.Literals(["agent_run", "agent_spawn", "team_assign"])),
    assignmentId: Schema.optional(Id), task: Text, selectedModel: Text,
    requestedModel: Schema.optional(Text), requestedReasoning: Schema.optional(Text),
    activated: Schema.optional(Schema.Struct({ attemptId: Id, model: Text, reasoning: Text })),
    sdkRequest: Schema.NullOr(SdkRequest), acceptedAt: Schema.Number, terminalAt: Schema.optional(Schema.Number),
    activity: Text, phase: Schema.Literals(["working", "idle", "suspected_stall", "stalled"]), profile: Schema.optional(Text),
    resultTaken: Schema.optionalKey(Schema.Boolean),
    handoff: Schema.optionalKey(Schema.Struct({
      collection: Schema.Literal("collected"),
      bookkeeping: Schema.Literals(["pending", "reconciled", "inapplicable", "blocked"]),
      delivery: Schema.Literals(["pending", "offered", "incorporated", "blocked", "dropped"]),
      continuation: Schema.Literals(["not-requested", "requested", "observed", "blocked-unconfirmed"]),
    })),
  })),
});
export const WorkerAvailability = Schema.Union([
  Schema.Struct({ state: Schema.Literal("pending"), instanceId: Id, generation: NonNegativeInt }),
  Schema.Struct({ state: Schema.Literal("unavailable"), reason: Schema.Literals(["unknown_target", "outcome_missing", "unsupported_kind", "not_captured", "close_failed", "capture_failed", "snapshot_oversized"]) }),
  Schema.Struct({ state: Schema.Literals(["evicted", "stale_reference", "stale_owner", "invalid_cursor"]) }),
]);
const Brief = Schema.Struct({ ...WorkerTextPage.fields, source: Schema.Literals(["original WorkBrief", "invocation task"]) });
export const WorkerDetail = Schema.Struct({
  state: Schema.Literal("ready"), ref: WorkerReference, status: Status, observedStatus: Status,
  result: Schema.Literals(["ready", "pending", "unavailable", "evicted", "stale_reference", "stale_owner", "invalid_cursor"]),
  activity: Schema.Literals(["observed_binding", "unavailable"]),
  metadata: Schema.optionalKey(WorkerTextPage), workbrief: Schema.optionalKey(Brief),
  nextMetadata: Schema.optionalKey(Schema.NullOr(WorkerCursor)), nextBrief: Schema.optionalKey(Schema.NullOr(WorkerCursor)),
  // Control selections are not ordinary read authority.
  controls: Schema.Struct({ reason: Schema.optional(Text), grant: Schema.Literals(["denied", "enabled"]) }),
});
export const WorkerRequests = Schema.Struct({ state: Schema.Literal("ready"), ref: WorkerReference, page: WorkerTextPage, next: Schema.NullOr(WorkerCursor) });
export const WorkerActivity = Schema.Union([WorkerAvailability, Schema.Struct({
  state: Schema.Literal("ready"), ref: WorkerReference, next: WorkerCursor,
  page: Schema.Struct({
    historyId: Id, sessionId: Id, active: Schema.Boolean,
    native: Schema.Struct({ sessionId: Id, historyId: Id, leafId: Schema.NullOr(Id), persisted: Schema.Boolean }),
    workbrief: Brief, teamWrapper: Schema.optional(Text),
    activity: Rows(Schema.Struct({ sequence: NonNegativeInt, kind: Text, text: Text, source: Schema.Literal("event-only"), nativeLeafId: Schema.NullOr(Id) })),
    interactions: Rows(Schema.Record(Schema.String, Schema.Unknown)), gaps: Rows(Text), cursor: NonNegativeInt, more: Schema.Boolean,
  }),
})]);
export const WorkerResult = Schema.Union([WorkerAvailability, Schema.Struct({
  state: Schema.Literal("ready"), ref: WorkerReference, instanceId: Id, generation: NonNegativeInt,
  status: Schema.Literals(["completed", "failed", "cancelled"]),
  text: Schema.optionalKey(WorkerTextPage), metadata: Schema.optionalKey(WorkerTextPage),
  nextText: Schema.optionalKey(Schema.NullOr(WorkerCursor)), nextMetadata: Schema.optionalKey(Schema.NullOr(WorkerCursor)),
})]);
export const WorkerHistoryMetadata = Schema.Struct({
  kind: Schema.Literal("member"), groupId: Id, historyId: Id, roleId: Id, coordinatorSessionId: Id,
  instanceId: Schema.optional(Id), assignmentId: Schema.optional(Id), attemptId: Schema.optional(Id),
  native: Schema.Union([
    Schema.Struct({ state: Schema.Literals(["not-created", "pruned"]) }),
    Schema.Struct({ state: Schema.Literal("allocated"), sessionId: Id, disposition: Schema.Union([
      Schema.Struct({ state: Schema.Literal("pending") }), Schema.Struct({ state: Schema.Literal("disposed"), at: Id }),
      Schema.Struct({ state: Schema.Literal("unknown"), reason: Text }),
    ]) }),
  ]),
});
const HistoryPage = <S extends Schema.Top>(schema: S) => Schema.Struct({
  items: Rows(schema), diagnostics: Rows(Schema.Struct({ code: Id, detail: Schema.optional(Text) })), cursor: Schema.NullOr(Cursor),
});
export const WorkerHistoryList = HistoryPage(WorkerHistoryMetadata);
export const WorkerHistoryRead = HistoryPage(Schema.Struct({
  historyId: Id, source: Schema.Literal("worker"), entryOffset: NonNegativeInt, byteOffset: NonNegativeInt,
  endsEntry: Schema.Boolean, encoding: Schema.Literal("base64"), data: Schema.String.check(Schema.isMaxLength(48 * 1024)),
  validation: Schema.Literals(["validation_limit", "corrupt", "json", "fragment"]),
}));
export const WorkerRoute = Schema.Struct({ state: Schema.Literals(["captured", "not-routed", "unavailable"]), page: WorkerTextPage });
export const WorkerObservation = Schema.Union([
  Schema.Struct({ state: Schema.Literals(["disabled", "unsupported", "runtime_absent", "connecting", "stale_owner"]) }),
  Schema.Struct({ state: Schema.Literal("error"), code: Id }),
  Schema.Struct({ state: Schema.Literal("last_observation"), owner: WorkerOwner, revision: NonNegativeInt, observedAt: Schema.Number }),
]);
export const WorkerTarget = Schema.Struct({ environmentId: EnvironmentId, threadId: ThreadId });
export const WorkerListInput = Schema.Struct({ ...WorkerTarget.fields, after: Schema.optional(Cursor), revision: Schema.optional(NonNegativeInt) });
export const WorkerIdInput = Schema.Struct({ ...WorkerTarget.fields, id: Id, cursor: Schema.optional(WorkerCursor) });
export const WorkerRefInput = Schema.Struct({ ...WorkerTarget.fields, ref: WorkerReference, cursor: Schema.optional(WorkerCursor) });
export const WorkerHistoryListInput = Schema.Struct({
  ...WorkerTarget.fields, workerId: Schema.optional(Id), cursor: Schema.optional(Cursor),
  limit: Schema.optionalKey(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 50 }))),
});
export const WorkerViewInput = Schema.Struct({ ...WorkerTarget.fields, viewId: Id });
export const WorkerSensitiveGrant = Schema.Literals(["history", "route"]);
export const WorkerAcknowledgeInput = Schema.Struct({ ...WorkerViewInput.fields, grant: WorkerSensitiveGrant, acknowledged: Schema.Literal(true) });
export const WorkerHistoryReadInput = Schema.Struct({ ...WorkerHistoryListInput.fields, viewId: Id, historyId: Id, groupId: Schema.optionalKey(Id) });
export const WorkerRouteInput = Schema.Struct({ ...WorkerViewInput.fields, id: Id, offset: Schema.optional(NonNegativeInt) });
export const WorkerControlDetailInput = Schema.Struct({ ...WorkerViewInput.fields, id: Id });
export const WorkerControlDetail = Schema.Struct({ ...WorkerDetail.fields, controls: Schema.Struct({
  selection: Schema.optionalKey(WorkerSelection), reason: Schema.optional(Text),
  runtimeMode: Schema.optionalKey(RuntimeMode),
  grant: Schema.Literals(["denied", "enabled"]),
}) });
export const WorkerActivateInput = Schema.Struct({
  ...WorkerViewInput.fields, selection: WorkerSelection, acknowledged: Schema.Literal(true),
});
export const WorkerNativeInput = Schema.Struct({
  intent: Schema.Literals(["query", "steer"]),
  text: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(8192)),
  decisionId: Schema.optionalKey(Id),
});
export const WorkerActionInput = Schema.Struct({
  ...WorkerViewInput.fields, actionId: WorkerActionId, selection: WorkerSelection,
  operation: Schema.Union([
    Schema.Struct({ kind: Schema.Literal("input"), input: WorkerNativeInput }),
    Schema.Struct({ kind: Schema.Literal("cancel") }),
  ]),
});
const Sequence = Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: Number.MAX_SAFE_INTEGER }));
const NativeReceipt = Schema.Union([
  Schema.Struct({ kind: Schema.Literal("input"), native: Schema.Struct({
    interactionId: Id, historyId: Id, sessionId: Id, intent: Schema.Literals(["query", "steer"]),
    decisionId: Schema.optionalKey(Id), status: Schema.Literals(["queued", "handled", "rejected", "unconfirmed"]),
    nativeEntryId: Schema.optionalKey(Schema.NullOr(Id)), delivery: Schema.optionalKey(Text),
    reason: Schema.optionalKey(Text),
    candidateAnswer: Schema.optionalKey(Schema.Struct({ sequence: NonNegativeInt, basis: Text })),
  }) }),
  Schema.Struct({ kind: Schema.Literal("cancel"), native: Schema.Struct({
    status: Schema.Literal("requested"), instanceId: Id, historyId: Id, sessionId: Id,
  }) }),
  Schema.Struct({ kind: Schema.Literal("error"), code: Id }),
]);
export const WorkerMutationStatus = Schema.Union([
  Schema.Struct({ state: Schema.Literals(["pending", "outcome_unknown", "not_admitted"]), sequence: Sequence }),
  Schema.Struct({ state: Schema.Literal("recorded"), sequence: Sequence, receipt: NativeReceipt }),
]);
export const WorkerActionOutcome = Schema.Struct({
  actionId: WorkerActionId,
  status: Schema.Union([
    WorkerMutationStatus,
    Schema.Struct({
      state: Schema.Literal("outcome_unknown"),
      reason: Schema.Literals(["unavailable", "mapping_evicted", "owner_retired", "lease_expired", "disconnected"]),
    }),
  ]),
});
export const WorkerReceiptInput = Schema.Struct({
  ...WorkerTarget.fields, actionId: WorkerActionId,
  access: Schema.Union([
    Schema.Struct({ viewId: Id }),
    Schema.Struct({ recoveryHandle: Id }),
  ]),
});
export const WorkerViewEvent = Schema.Union([
  Schema.Struct({ type: Schema.Literal("opened"), viewId: Id, recoveryHandle: Id, owner: WorkerOwner, grants: Schema.Array(WorkerSensitiveGrant).check(Schema.isMaxLength(2)) }),
  Schema.Struct({ type: Schema.Literal("invalidated"), reason: Id }),
]);
export class ProviderWorkersError extends Schema.TaggedError<ProviderWorkersError>()("ProviderWorkersError", {
  code: Id,
  /** This decision already has a T3 sequence/mapping; a coded rejection is not permission to resend. */
  actionAdmitted: Schema.optionalKey(Schema.Literal(true)),
}) {}
const WorkerMcpReadRequest = Schema.Union([
  Schema.Struct({ operation: Schema.Literal("list"), threadId: ThreadId, after: Schema.optional(Cursor), revision: Schema.optional(NonNegativeInt) }),
  Schema.Struct({ operation: Schema.Literal("detail"), threadId: ThreadId, id: Id, cursor: Schema.optional(WorkerCursor) }),
  Schema.Struct({ operation: Schema.Literal("requests"), threadId: ThreadId, id: Id, cursor: Schema.optional(WorkerCursor) }),
  Schema.Struct({ operation: Schema.Literal("activity"), threadId: ThreadId, ref: WorkerReference, cursor: Schema.optional(WorkerCursor) }),
  Schema.Struct({ operation: Schema.Literal("result"), threadId: ThreadId, ref: WorkerReference, cursor: Schema.optional(WorkerCursor) }),
  Schema.Struct({ operation: Schema.Literal("historyMetadata"), threadId: ThreadId, workerId: Schema.optional(Id), cursor: Schema.optional(Cursor) }),
]);
// MCP tools require an object-root input schema; the operation remains a discriminated request.
export const WorkerMcpReadInput = Schema.Struct({ request: WorkerMcpReadRequest });
export const WorkerMcpReadResult = Schema.Union([WorkerRoster, WorkerDetail, WorkerRequests, WorkerActivity, WorkerResult, WorkerHistoryList]);
