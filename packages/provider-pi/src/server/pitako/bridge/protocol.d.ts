export interface Owner { processId: string; coordinatorSessionId: string; epoch: string }
export type Grant = "history" | "route" | "control";
export type Capability = "workers.list" | "workers.detail" | "workers.requests" | "workers.activity" | "workers.result" | "subscribe" | "unsubscribe" |
  "history.list" | "history.read" | "route.read" | "view.grant" | "input" | "cancel" | "request.status";
export interface AgentSelection extends Owner {
  target: { kind: "background"; instanceId: string } | { kind: "team"; assignmentId: string };
  instanceId: string; generation: number; historyId: string; sessionId: string;
}
export interface ClientLease { clientId: string; resumeToken: string; highWater: number }
export type ResumeLease = Pick<ClientLease, "clientId" | "resumeToken">;
export interface NativeInput { intent: "query" | "steer"; text: string; decisionId?: string }
export interface NativeInputReceipt {
  interactionId: string; historyId: string; sessionId: string; intent: NativeInput["intent"]; decisionId?: string;
  status: "queued" | "handled" | "rejected" | "unconfirmed"; nativeEntryId?: string | null; delivery?: string; reason?: string;
  candidateAnswer?: { sequence: number; basis: string };
}
export type MutationStatus =
  | { state: "pending" | "outcome_unknown" | "not_admitted"; sequence: number }
  | { state: "recorded"; sequence: number; receipt:
      { kind: "input"; native: NativeInputReceipt } |
      { kind: "cancel"; native: { status: "requested"; instanceId: string; historyId: string; sessionId: string } } |
      { kind: "error"; code: ErrorCode } };
export interface Descriptor {
  version: 1; transport: "unix"; socket: string; token: string; owner: Owner;
  grants: Grant[]; capabilities: readonly Capability[];
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
}
export interface WorkerList { owner: Owner; revision: number; items: WorkerListItem[]; cursor: string | null }
export interface Subscription { subscriptionId: string; snapshot: WorkerList }
export interface Notification {
  version: 1; method: "changed"; clientId: string; owner: Owner;
  subscriptionId: string; revision: number; refreshRequired: true;
}
export interface WorkerReadReference extends Owner {
  kind: "read"; instanceId: string; source: "agent_run" | "agent_spawn" | "team_assign";
  assignmentId?: string; evaluationId?: string; attemptId?: string;
  native?: { generation: number; historyId: string; sessionId: string };
}
export type WorkerReadCursor =
  | { ref: WorkerReadReference; section: "brief" | "detail_metadata" | "result_text" | "result_metadata"; offset: number }
  | { ref: WorkerReadReference; section: "requests"; offset: number; digest: string }
  | { ref: WorkerReadReference; section: "activity"; after: number };
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
  controls: { selection?: AgentSelection; reason?: string; grant: "denied" | "enabled" };
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
export type Envelope = { version: 1; id: string; clientId: string; owner: Owner; method: string; params: Record<string, unknown> };
export type SensitiveGrant = "history" | "route";
export type ViewAcknowledgement = { grant: Grant; acknowledged: true };
export type ViewGrant = { grant: SensitiveGrant; acknowledged: true } |
  { grant: "control"; acknowledged: true; selection: AgentSelection };
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
export type Request = { version: 1; id: string; clientId: string; owner: Owner } & (
  { method: "connect"; params: { token: string; expectedSessionId: string; resumeToken?: string } } |
  { method: "input"; params: { sequence: number; selection: AgentSelection; input: NativeInput } } |
  { method: "cancel"; params: { sequence: number; selection: AgentSelection } } |
  { method: "request.status"; params: { sequence: number } } |
  { method: "workers.list"; params: { after?: string; revision?: number } } |
  { method: "subscribe"; params: Record<string, never> } |
  { method: "unsubscribe"; params: { subscriptionId: string } } |
  { method: "workers.detail" | "workers.requests"; params: { id: string; cursor?: WorkerReadCursor } } |
  { method: "workers.activity" | "workers.result"; params: { ref: WorkerReadReference; cursor?: WorkerReadCursor } } |
  { method: "history.list"; params: HistoryListOptions } |
  { method: "history.read"; params: HistoryReadOptions } |
  { method: "route.read"; params: { id: string; offset?: number } } |
  { method: "view.grant"; params: ViewGrant });
export const version: 1;
export const replyBytes: number;
export const outstandingRequests: number;
export const queueBytes: number;
export const capabilities: readonly Capability[];
export const errors: Readonly<Record<"invalid_request" | "unsupported_method" | "unauthorized" | "stale_owner" |
  "invalid_cursor" | "stale_cursor" | "stale_reference" | "unknown_target" | "busy" | "oversized" | "forbidden" |
  "sequence_conflict" | "lease_expired" | "stale_selection", string>>;
export type ErrorCode = keyof typeof errors;
export type Reply = { version: 1; id: string; clientId: string; owner: Owner } & (
  { ok: true; result: WorkerList | WorkerDetail | WorkerRequests | WorkerActivity | WorkerResult | Subscription |
    HistoryPage | HistoryPage<HistoryMetadata> | RoutePage | ViewAcknowledgement | MutationStatus |
    { subscriptionId: string } | { capabilities: readonly Capability[]; grants: readonly []; lease: ClientLease } } |
  { ok: false; error: { code: ErrorCode; message: string } });
export function object(value: unknown): value is Record<string, unknown>;
export function string(value: unknown, max?: number): value is string;
export function isOwner(value: unknown): value is Owner;
export function sameOwner(a: Owner, b: Owner): boolean;
export function isGrants(value: unknown): value is Grant[];
export function isDescriptor(value: unknown): value is Descriptor;
export function isEnvelope(value: unknown): value is Envelope;
export function isRequest(value: unknown): value is Request;
export function isReadReference(value: unknown): value is WorkerReadReference;
export function isReadCursor(value: unknown): value is WorkerReadCursor;
export function isSelection(value: unknown): value is AgentSelection;
export function isMutationStatus(value: unknown): value is MutationStatus;
export function isReply(value: unknown, method: Request["method"]): value is Reply;
export function isNotification(value: unknown): value is Notification;
export function failure(request: Envelope, code: ErrorCode): Reply;
export function success(request: Request, result: unknown): { version: 1; id: string; clientId: string; owner: Owner; ok: true; result: unknown };
