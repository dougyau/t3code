export interface Owner { processId: string; coordinatorSessionId: string; epoch: string }
export type Capability = "workers.list" | "workers.detail" | "workers.requests" | "workers.activity" | "workers.result" | "subscribe" | "unsubscribe" |
  "history.list" | "history.read" | "route.read" | "workers.detail.public-v1" | "workers.activity.public-v1" | "workers.result.public-v1";
export interface ClientLease { clientId: string; resumeToken: string }
export type ResumeLease = Pick<ClientLease, "clientId" | "resumeToken">;
export interface Descriptor {
  version: 2; transport: "unix"; socket: string; token: string; owner: Owner;
  capabilities: readonly Capability[];
}
export interface PublicLaunch {
  source: "agent_spawn" | "team_assign"; instanceId: string; roleId: string;
  coordinatorSessionId: string | null; toolCallId: string | null; assignmentId?: string;
  acceptedAt: number; linkage: "tool" | "unlinked";
}
export type PublicGap = "pending" | "not_captured" | "not_persisted" | "pruned" | "evicted" |
  "unsupported_content" | "capture_failed" | "stale_owner" | "resync_required";
export type PublicContent = { type: "text"; text: string } |
  { type: "toolCall"; toolCallId: string; name: string; arguments?: unknown } |
  { type: "omitted"; reason: "unsupported_content" };
export type PublicTodoSnapshot = {
  format: "rpiv-todo-v1";
  tasks: { id: number; identityEntryId: string; subject: string;
    status: "pending" | "in_progress" | "completed" | "deleted" }[];
};
export type PublicTodoHead = { state: "ready"; sourceEntryId: string | null } | { state: "unavailable" };
export interface PublicBinding {
  generation: number; historyId: string; sessionId: string; state: "active" | "historical";
  headEntryId: string | null; capture: "open" | "closed" | "incomplete"; attemptIds: string[];
  attempts: { attemptId: string; activated?: { model: string; reasoning: string };
    selected?: { model: string; requestedModel?: string; reasoning?: string } }[];
  gaps: PublicGap[]; todoHead?: PublicTodoHead;
}
export type PublicPayload = { role: "user" | "assistant"; blocks: PublicContent[];
  stopReason?: "pending" | "stop" | "length" | "toolUse" | "error" | "aborted" | "deferred"; errorMessage?: string } |
  { status: "started" | "updated" | "completed" | "error"; name: string; args?: unknown;
    argsState: "available" | "not_captured"; result: PublicContent[] | null; isError: boolean | null;
    todoSnapshot?: PublicTodoSnapshot } |
  { headEntryId: string | null; previousHeadEntryId: string };
interface PublicRecordFields {
  id: string; binding: number; revision: number;
  order: number; nativeOrder: number | null; occurredAt: number | null;
  observedAt: number; nativeTime: string | null;
  nativeEntryId: string | null; parentEntryId: string | null; toolCallId: string | null;
  recovered?: true;
}
export type PublicRecord = PublicRecordFields & (
  { kind: "message"; payload: Extract<PublicPayload, { role: unknown }> } |
  { kind: "tool"; parentToolCallId?: string; payload: Extract<PublicPayload, { status: unknown }> } |
  { kind: "branch_changed"; payload: Extract<PublicPayload, { headEntryId: unknown }> });
export interface PublicChange { sequence: number | null; op: "upsert"; record: PublicRecord }
export interface PublicFragment {
  kind: "fragment"; id: string; binding: number; revision: number; sequence: number | null;
  encoding: "base64-json"; byteOffset: number; totalBytes: number; data: string;
}
export interface PublicConversationPage {
  format: "public-v1"; state: "ready" | "resync_required"; ref: WorkerReadReference;
  epoch: string; cut: number; changes: (PublicChange | PublicFragment)[];
  next: string | null; more: boolean; lost?: { from: number; through: number }; gaps: PublicGap[];
  binding?: PublicBinding; capture?: { state: "opening" | "open" | "closed" | "incomplete"; finalSequence: number | null };
}
export interface PublicWorkerDetail {
  format: "public-v1"; ref: WorkerReadReference; worker: PublicLaunch;
  lifecycle: {
    execution: "accepted" | "running" | "completed" | "failed" | "cancelled";
    result: "pending" | "available" | "unavailable";
    capture: "opening" | "open" | "closed" | "incomplete"; captureReason?: "not_captured" | "capture_failed";
    finalSequence?: number;
  };
  captureEpoch?: string; conversationSequence?: number;
  captureGaps?: PublicGap[];
  bindings: (PublicBinding & { ref: WorkerReadReference })[];
  nextBindings?: WorkerReadCursor | null;
  workbrief: TextPage & { source: "original WorkBrief" | "invocation task" };
  nextBrief: WorkerReadCursor | null;
  selected: { model: string; requestedModel?: string; reasoning?: string };
  activated: { attemptId: string; model: string; reasoning: string } | null;
}
/** Wire metadata only; no Pi or controller dependency in native consumer types. */
export interface SdkRequest {
  attemptId?: string; historyId?: string; sessionId?: string;
  sdkOptions?: { reasoning?: string; temperature?: number; maxTokens?: number; serviceTier?: string };
  model: string; provider?: string; requestId?: string; inputTokens?: number | null; outputTokens?: number | null;
  reasoning?: string; fast_requested: boolean; requested_service_tier?: "fast" | "priority";
  returned_service_tier: "unavailable"; time_to_first_model_output_ms: number | "unavailable";
  time_to_first_model_output_unavailable_reason?: "cancelled" | "failed";
}
export interface WorkerListItem {
  id: string; roleId: string; status: "created" | "running" | "completed" | "failed" | "cancelled";
  source?: "agent_run" | "agent_spawn" | "team_assign"; assignmentId?: string;
  task: string; selectedModel: string; requestedModel?: string; requestedReasoning?: string;
  activated?: { attemptId: string; model: string; reasoning: string }; sdkRequest: SdkRequest | null;
  acceptedAt: number; terminalAt?: number; activity: string;
  phase: "working" | "idle" | "suspected_stall" | "stalled"; profile?: string;
  resultTaken?: boolean;
  handoff?: {
    collection: "collected";
    bookkeeping: "pending" | "reconciled" | "inapplicable" | "blocked";
    delivery: "pending" | "offered" | "incorporated" | "blocked" | "dropped";
    continuation: "not-requested" | "requested" | "observed" | "blocked-unconfirmed";
  };
}
export interface WorkerList { owner: Owner; revision: number; items: WorkerListItem[]; cursor: string | null }
export interface Subscription { subscriptionId: string; snapshot: WorkerList }
export interface Notification {
  version: 2; method: "changed"; clientId: string; owner: Owner;
  subscriptionId: string; revision: number; refreshRequired: true;
}
export interface WorkerReadReference extends Owner {
  kind: "read"; instanceId: string; source: "agent_run" | "agent_spawn" | "team_assign";
  assignmentId?: string; evaluationId?: string; attemptId?: string;
  native?: { generation: number; historyId: string; sessionId: string };
}
export type WorkerReadCursor =
  | { ref: WorkerReadReference; section: "brief" | "public_bindings" | "detail_metadata" | "result_text" | "result_metadata"; offset: number }
  | { ref: WorkerReadReference; section: "requests"; offset: number; digest: string }
  | { ref: WorkerReadReference; section: "activity"; after: number };
export interface PublicWorkerResult {
  format: "public-v1"; state: "ready"; ref: WorkerReadReference; status: "completed" | "failed" | "cancelled";
  text: TextPage; nextText: WorkerReadCursor | null;
}
export interface TextPage { text: string; offset: number; nextOffset: number | null }
export type Availability =
  | { state: "pending"; instanceId: string; generation: number }
  | { state: "unavailable"; reason: "unknown_target" | "outcome_missing" | "unsupported_kind" | "not_captured" |
      "close_failed" | "capture_failed" | "snapshot_oversized" }
  | { state: "evicted" | "stale_reference" | "stale_owner" | "invalid_cursor" };
export interface WorkerDetail {
  state: "ready"; ref: WorkerReadReference; status: WorkerListItem["status"]; observedStatus: WorkerListItem["status"];
  result: "ready" | Availability["state"]; activity: "observed_binding" | "unavailable";
  metadata: TextPage; workbrief: TextPage & { source: "original WorkBrief" | "invocation task" };
  nextMetadata: WorkerReadCursor | null; nextBrief: WorkerReadCursor | null;
}
export interface WorkerRequests {
  state: "ready"; ref: WorkerReadReference; page: TextPage; next: WorkerReadCursor | null;
}
export interface ActivityPage {
  historyId: string; sessionId: string; active: boolean;
  native: { sessionId: string; historyId: string; leafId: string | null; persisted: boolean };
  workbrief: TextPage & { source: "original WorkBrief" | "invocation task" };
  teamWrapper?: string; activity: { sequence: number; kind: string; text: string; source: "event-only"; nativeLeafId: string | null }[];
  interactions: Record<string, unknown>[]; gaps: string[]; cursor: number; more: boolean;
}
export type WorkerActivity = Availability | { state: "ready"; ref: WorkerReadReference; page: ActivityPage; next: WorkerReadCursor };
export type WorkerResult = Availability | { state: "ready"; ref: WorkerReadReference; instanceId: string; generation: number;
  status: "completed" | "failed" | "cancelled"; text: TextPage; metadata: TextPage;
  nextText: WorkerReadCursor | null; nextMetadata: WorkerReadCursor | null };
export type Envelope = { version: 2; id: string; clientId: string; owner: Owner; method: string; params: Record<string, unknown> };
export type HistoryListOptions = { workerId?: string; cursor?: string; limit?: number };
export type HistoryReadOptions = HistoryListOptions & { historyId: string; groupId?: string };
/** Metadata is provenance, not a filesystem capability. Extra catalog fields remain opaque. */
export type HistoryMetadata = {
  kind: "member"; groupId: string; historyId: string; roleId: string; coordinatorSessionId: string;
  instanceId?: string; assignmentId?: string; attemptId?: string;
  native: { state: "not-created" | "pruned" } |
    { state: "allocated"; sessionId: string; disposition: { state: "pending" } |
      { state: "disposed"; at: string } | { state: "unknown"; reason: string } };
  [field: string]: unknown;
};
export type HistoryFragment = {
  historyId: string; source: "worker"; entryOffset: number; byteOffset: number; endsEntry: boolean;
  encoding: "base64"; data: string; validation: "validation_limit" | "corrupt" | "json" | "fragment";
};
export type HistoryPage<T = HistoryFragment> = {
  items: T[]; diagnostics: Array<{ code: string; detail?: string }>; cursor: string | null;
};
export type RoutePage = { state: "captured" | "not-routed" | "unavailable"; page: TextPage };
export type Request = { version: 2; id: string; clientId: string; owner: Owner } & (
  { method: "connect"; params: { token: string; expectedSessionId: string; resumeToken?: string } } |
  { method: "workers.list"; params: { after?: string; revision?: number } } |
  { method: "subscribe"; params: Record<string, never> } |
  { method: "unsubscribe"; params: { subscriptionId: string } } |
  { method: "workers.detail"; params: { id: string; cursor?: WorkerReadCursor; format?: "public-v1" } } |
  { method: "workers.requests"; params: { id: string; cursor?: WorkerReadCursor } } |
  { method: "workers.activity"; params: { ref: WorkerReadReference; cursor?: WorkerReadCursor | string; format?: "public-v1"; recovery?: boolean } } |
  { method: "workers.result"; params: { ref: WorkerReadReference; cursor?: WorkerReadCursor; format?: "public-v1" } } |
  { method: "history.list"; params: HistoryListOptions } |
  { method: "history.read"; params: HistoryReadOptions } |
  { method: "route.read"; params: { id: string; offset?: number } });
export const version: 2;
export const replyBytes: number;
export const outstandingRequests: number;
export const queueBytes: number;
export const capabilities: readonly Capability[];
export const errors: Readonly<Record<"invalid_request" | "unsupported_method" | "unauthorized" | "stale_owner" |
  "invalid_cursor" | "stale_cursor" | "stale_reference" | "unknown_target" | "busy" | "oversized" | "lease_expired", string>>;
export type ErrorCode = keyof typeof errors;
export type Reply = { version: 2; id: string; clientId: string; owner: Owner } & (
  { ok: true; result: WorkerList | WorkerDetail | WorkerRequests | WorkerActivity | WorkerResult | Subscription |
    HistoryPage | HistoryPage<HistoryMetadata> | RoutePage | PublicWorkerDetail | PublicConversationPage | PublicWorkerResult |
    { subscriptionId: string } | { capabilities: readonly Capability[]; lease: ClientLease } } |
  { ok: false; error: { code: ErrorCode; message: string } });
export function object(value: unknown): value is Record<string, unknown>;
export function string(value: unknown, max?: number): value is string;
export function isOwner(value: unknown): value is Owner;
export function sameOwner(a: Owner, b: Owner): boolean;
export function isDescriptor(value: unknown): value is Descriptor;
export function isEnvelope(value: unknown): value is Envelope;
export function isRequest(value: unknown): value is Request;
export function isReadReference(value: unknown): value is WorkerReadReference;
export function isReadCursor(value: unknown): value is WorkerReadCursor;
export function isReply(value: unknown, method: Request["method"], format?: "public-v1"): value is Reply;
export function isNotification(value: unknown): value is Notification;
export function failure(request: Envelope, code: ErrorCode): Reply;
export function success(request: Request, result: unknown): { version: 2; id: string; clientId: string; owner: Owner; ok: true; result: unknown };
