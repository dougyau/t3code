/** Authoritative v2 wire validation. Passive: no Pi, discovery, or service initialization. */
export const version = 2;
export const replyBytes = 48 * 1024;
export const outstandingRequests = 8;
export const queueBytes = 256 * 1024;
export const capabilities = Object.freeze(["workers.list", "workers.detail", "workers.requests", "workers.activity", "workers.result", "subscribe", "unsubscribe",
  "history.list", "history.read", "route.read", "workers.detail.public-v1", "workers.activity.public-v1", "workers.result.public-v1", "architecture.list", "architecture.read", "architecture.public-v1", "background.pending"]);
export const object = value => !!value && typeof value === "object" && !Array.isArray(value);
const keys = (value, allowed) => Object.keys(value).every(key => allowed.includes(key));
export const string = (value, max = 1024) =>
  typeof value === "string" && value.length > 0 && Buffer.byteLength(value) <= max && !value.includes("\0");
export const isOwner = value => object(value) && keys(value, ["processId", "coordinatorSessionId", "epoch"]) &&
  string(value.processId) && string(value.coordinatorSessionId) && string(value.epoch);
export const sameOwner = (a, b) => a.processId === b.processId &&
  a.coordinatorSessionId === b.coordinatorSessionId && a.epoch === b.epoch;
// Only the three published snapshots, not arbitrary subsets or future extensions.
const isCapabilities = value => Array.isArray(value) && [13, 16, 17].includes(value.length) &&
  value.every((item, index) => item === capabilities[index]);
const integer = value => Number.isSafeInteger(value) && value >= 0;
const token = value => typeof value === "string" && /^[a-f0-9]{64}$/.test(value);
const uuid = value => typeof value === "string" && /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value);
const historyOptions = value => (value.cursor === undefined || typeof value.cursor === "string" &&
  value.cursor.length <= 2048 && /^[\w-]+$/.test(value.cursor)) &&
  (value.limit === undefined || integer(value.limit) && value.limit >= 1 && value.limit <= 200);
const readReference = (value, publicFormat) => object(value) && Buffer.byteLength(JSON.stringify(value)) <= 4096 &&
  keys(value, ["kind", "processId", "coordinatorSessionId", "epoch", "instanceId", "source", "assignmentId", "evaluationId", "attemptId", "native"]) &&
  value.kind === "read" && isOwner({ processId: value.processId, coordinatorSessionId: value.coordinatorSessionId, epoch: value.epoch }) &&
  string(value.instanceId) && ["agent_run", "agent_spawn", "team_assign"].includes(value.source) &&
  (value.source === "team_assign" ? string(value.assignmentId) && (publicFormat ? value.evaluationId === undefined : string(value.evaluationId))
    : value.assignmentId === undefined && value.evaluationId === undefined) &&
  (publicFormat ? value.attemptId === undefined : value.attemptId === undefined || string(value.attemptId)) &&
  (value.native === undefined || object(value.native) && keys(value.native, ["generation", "historyId", "sessionId"]) &&
    integer(value.native.generation) && string(value.native.historyId) && string(value.native.sessionId) &&
    (publicFormat || string(value.attemptId)));
export const isReadReference = value => readReference(value, false);
const readCursor = (value, publicFormat) => object(value) && readReference(value.ref, publicFormat) &&
  (value.section === "activity" ? keys(value, ["ref", "section", "after"]) && integer(value.after)
    : ["brief", "public_bindings", "detail_metadata", "result_text", "result_metadata", "requests"].includes(value.section) &&
      keys(value, value.section === "requests" ? ["ref", "section", "offset", "digest"] : ["ref", "section", "offset"]) &&
      integer(value.offset) && (value.section !== "requests" || typeof value.digest === "string" && /^[a-f0-9]{64}$/.test(value.digest)));
export const isReadCursor = value => readCursor(value, false);
export const isDescriptor = value => object(value) &&
  keys(value, ["version", "transport", "socket", "token", "owner", "capabilities"]) &&
  value.version === version && value.transport === "unix" && string(value.socket, 103) && value.socket.startsWith("/") &&
  typeof value.token === "string" && /^[a-f0-9]{64}$/.test(value.token) &&
  isOwner(value.owner) && isCapabilities(value.capabilities);
export const isEnvelope = value => object(value) && value.version === version &&
  string(value.id, 128) && string(value.clientId, 128) && isOwner(value.owner) &&
  string(value.method, 128) && object(value.params) &&
  keys(value, ["version", "id", "clientId", "owner", "method", "params"]);
export function isRequest(value) {
  if (!isEnvelope(value)) return false;
  switch (value.method) {
    case "connect": return keys(value.params, ["token", "expectedSessionId", "resumeToken"]) &&
      token(value.params.token) && string(value.params.expectedSessionId) &&
      (value.params.resumeToken === undefined || token(value.params.resumeToken));
    case "background.pending": return keys(value.params, []);
    case "architecture.list": return keys(value.params, ["cursor"]) && optional(value.params, "cursor", architectureCursor);
    case "architecture.read": return keys(value.params, ["ref", "cursor"]) && isArchitectureRef(value.params.ref) && optional(value.params, "cursor", architectureCursor);
    case "workers.list": return keys(value.params, ["after", "revision"]) &&
      (value.params.after === undefined || string(value.params.after) && integer(value.params.revision)) &&
      (value.params.revision === undefined || integer(value.params.revision));
    case "subscribe": return keys(value.params, []);
    case "unsubscribe": return keys(value.params, ["subscriptionId"]) && string(value.params.subscriptionId, 128);
    case "workers.detail": return keys(value.params, ["id", "cursor", "format"]) && string(value.params.id) &&
      (value.params.format === undefined || value.params.format === "public-v1") &&
      (value.params.cursor === undefined || readCursor(value.params.cursor, value.params.format === "public-v1") &&
        (value.params.format === "public-v1" ? ["brief", "public_bindings"].includes(value.params.cursor.section)
          : ["brief", "detail_metadata"].includes(value.params.cursor.section)));
    case "workers.requests": return keys(value.params, ["id", "cursor"]) && string(value.params.id) &&
      (value.params.cursor === undefined || isReadCursor(value.params.cursor) && value.params.cursor.section === "requests");
    case "workers.activity":
      if (value.params.format === "public-v1") return keys(value.params, ["ref", "cursor", "format", "recovery"]) &&
        readReference(value.params.ref, value.params.ref?.source !== "agent_run") &&
        (value.params.cursor === undefined || string(value.params.cursor, 16 * 1024)) &&
        (value.params.recovery === undefined || typeof value.params.recovery === "boolean");
      // Fall through for legacy coalesced activity, still read-only.
    case "workers.result":
      if (value.params.format === "public-v1") return keys(value.params, ["ref", "cursor", "format"]) &&
        readReference(value.params.ref, value.params.ref?.source !== "agent_run") && (value.params.cursor === undefined || readCursor(value.params.cursor, true) &&
          value.params.cursor.section === "result_text");
      return keys(value.params, ["ref", "cursor"]) && isReadReference(value.params.ref) &&
      (value.params.cursor === undefined || isReadCursor(value.params.cursor) &&
        (value.method === "workers.activity" ? value.params.cursor.section === "activity"
          : ["result_text", "result_metadata"].includes(value.params.cursor.section)));
    case "history.list": return keys(value.params, ["workerId", "cursor", "limit"]) && historyOptions(value.params) &&
      (value.params.workerId === undefined || string(value.params.workerId));
    case "history.read": return keys(value.params, ["workerId", "historyId", "groupId", "cursor", "limit"]) &&
      historyOptions(value.params) && uuid(value.params.historyId) && (value.params.groupId === undefined || uuid(value.params.groupId)) &&
      (value.params.workerId === undefined || string(value.params.workerId));
    case "route.read": return keys(value.params, ["id", "offset"]) && string(value.params.id) &&
      (value.params.offset === undefined || integer(value.params.offset));
    default: return false;
  }
}
export const errors = Object.freeze({
  invalid_request: "Invalid bridge request",
  unsupported_method: "Unsupported bridge method",
  unauthorized: "Bridge authentication failed",
  lease_expired: "Logical client lease is expired or unknown; connect as a new client",
  stale_owner: "Bridge owner is retired or mismatched",
  invalid_cursor: "Invalid worker cursor",
  stale_cursor: "Observed projection changed; refresh the first page",
  stale_reference: "Worker read identity changed; refresh detail",
  unknown_target: "Unknown scoped worker",
  busy: "Bridge request limit exceeded",
  oversized: "Bridge reply exceeds its byte budget",
});
export const failure = (request, code) => ({ version, id: request.id, clientId: request.clientId,
  owner: request.owner, ok: false, error: { code, message: errors[code] } });
export const success = (request, result) => ({ version, id: request.id, clientId: request.clientId,
  owner: request.owner, ok: true, result });
const text = value => typeof value === "string" && Buffer.byteLength(value) <= replyBytes;
const optional = (value, key, validate) => value[key] === undefined || validate(value[key]);
function isSdkRequest(value) {
  return object(value) && keys(value, ["turnId", "attemptId", "historyId", "sessionId", "sdkOptions", "model", "provider",
    "requestId", "inputTokens", "outputTokens", "reasoning", "fast_requested", "requested_service_tier",
    "returned_service_tier", "time_to_first_model_output_ms", "time_to_first_model_output_unavailable_reason"]) &&
    ["turnId", "attemptId", "historyId", "sessionId", "provider", "requestId", "reasoning"].every(key => optional(value, key, text)) &&
    text(value.model) && typeof value.fast_requested === "boolean" &&
    optional(value, "sdkOptions", options => object(options) &&
      keys(options, ["reasoning", "temperature", "maxTokens", "serviceTier"]) &&
      ["reasoning", "serviceTier"].every(key => optional(options, key, text)) &&
      ["temperature", "maxTokens"].every(key => optional(options, key, Number.isFinite))) &&
    ["inputTokens", "outputTokens"].every(key => optional(value, key, item => item === null || Number.isFinite(item))) &&
    optional(value, "requested_service_tier", item => ["fast", "priority"].includes(item)) &&
    value.returned_service_tier === "unavailable" &&
    (value.time_to_first_model_output_ms === "unavailable" || Number.isFinite(value.time_to_first_model_output_ms)) &&
    optional(value, "time_to_first_model_output_unavailable_reason", item => ["cancelled", "failed"].includes(item));
}
function isList(value, owner) {
  return object(value) && keys(value, ["owner", "items", "cursor", "revision"]) && integer(value.revision) && isOwner(value.owner) &&
    sameOwner(value.owner, owner) && Array.isArray(value.items) && value.items.length <= 50 &&
    (value.cursor === null || string(value.cursor)) &&
    value.items.every(row => object(row) &&
      keys(row, ["id", "roleId", "status", "source", "teamId", "memberId", "turnId", "assignmentId", "task", "selectedModel", "requestedModel",
        "requestedReasoning", "activated", "sdkRequest", "acceptedAt", "terminalAt", "activity", "phase", "profile",
        "resultTaken", "handoff"]) &&
      string(row.id) && string(row.roleId) &&
      ["created", "running", "completed", "failed", "cancelled"].includes(row.status) &&
      optional(row, "source", source => ["agent_run", "agent_spawn", "team_assign"].includes(source)) &&
      ["teamId", "memberId", "turnId", "assignmentId", "requestedModel", "requestedReasoning", "profile"].every(key => optional(row, key, text)) &&
      text(row.task) && text(row.selectedModel) && optional(row, "terminalAt", Number.isFinite) &&
      optional(row, "activated", item => object(item) && keys(item, ["attemptId", "model", "reasoning"]) &&
        text(item.attemptId) && text(item.model) && text(item.reasoning)) &&
      (row.sdkRequest === null || isSdkRequest(row.sdkRequest)) &&
      optional(row, "resultTaken", item => typeof item === "boolean") &&
      optional(row, "handoff", item => object(item) && keys(item, ["collection", "bookkeeping", "delivery", "continuation"]) &&
        item.collection === "collected" && ["pending", "reconciled", "inapplicable", "blocked"].includes(item.bookkeeping) &&
        ["pending", "offered", "incorporated", "blocked", "dropped"].includes(item.delivery) &&
        ["not-requested", "requested", "observed", "blocked-unconfirmed"].includes(item.continuation)) &&
      Number.isFinite(row.acceptedAt) && text(row.activity) &&
      ["working", "idle", "suspected_stall", "stalled"].includes(row.phase));
}
const status = value => ["created", "running", "completed", "failed", "cancelled"].includes(value);
const page = value => object(value) && keys(value, ["text", "offset", "nextOffset", "source"]) &&
  typeof value.text === "string" && value.text.length <= 1024 && integer(value.offset) &&
  (value.nextOffset === null || value.nextOffset === value.offset + value.text.length && value.nextOffset > value.offset);
const brief = value => page(value) && ["original WorkBrief", "invocation task"].includes(value.source);
const boundRef = (value, owner) => isReadReference(value) && sameOwner(value, owner);
const publicBoundRef = (value, owner) => readReference(value, true) && sameOwner(value, owner);
const cursor = (value, ref, section, publicFormat = false) => value === null || readCursor(value, publicFormat) && value.section === section &&
  JSON.stringify(value.ref) === JSON.stringify(ref);
function available(value) {
  if (value.state === "pending") return keys(value, ["state", "instanceId", "generation"]) && string(value.instanceId) && integer(value.generation);
  if (value.state === "unavailable") return keys(value, ["state", "reason"]) &&
    ["unknown_target", "outcome_missing", "unsupported_kind", "not_captured", "close_failed", "capture_failed", "snapshot_oversized"].includes(value.reason);
  return ["evicted", "stale_reference", "stale_owner", "invalid_cursor"].includes(value.state) && keys(value, ["state"]);
}
function readReply(value, method, owner) {
  if (!object(value)) return false;
  if (value.state !== "ready") return ["workers.activity", "workers.result"].includes(method) && available(value);
  if (!boundRef(value.ref, owner)) return false;
  switch (method) {
    case "workers.detail": return keys(value, ["state", "ref", "status", "observedStatus", "result", "activity",
      "metadata", "workbrief", "nextMetadata", "nextBrief"]) && status(value.status) && status(value.observedStatus) &&
      ["ready", "pending", "unavailable", "evicted", "stale_reference", "stale_owner", "invalid_cursor"].includes(value.result) &&
      ["observed_binding", "unavailable"].includes(value.activity) && page(value.metadata) && brief(value.workbrief) &&
      cursor(value.nextMetadata, value.ref, "detail_metadata") && cursor(value.nextBrief, value.ref, "brief");
    case "workers.requests": return keys(value, ["state", "ref", "page", "next"]) &&
      page(value.page) && cursor(value.next, value.ref, "requests");
    case "workers.result": return keys(value, ["state", "ref", "instanceId", "generation", "status", "text", "metadata",
      "nextText", "nextMetadata"]) && value.instanceId === value.ref.instanceId && integer(value.generation) &&
      ["completed", "failed", "cancelled"].includes(value.status) && page(value.text) && page(value.metadata) &&
      cursor(value.nextText, value.ref, "result_text") && cursor(value.nextMetadata, value.ref, "result_metadata");
    case "workers.activity": {
      const p = value.page;
      return keys(value, ["state", "ref", "page", "next"]) && object(p) &&
        keys(p, ["historyId", "sessionId", "active", "native", "workbrief", "teamWrapper", "activity", "interactions", "gaps", "cursor", "more"]) &&
        value.ref.native?.historyId === p.historyId && value.ref.native?.sessionId === p.sessionId &&
        typeof p.active === "boolean" && object(p.native) && keys(p.native, ["historyId", "sessionId", "leafId", "persisted"]) &&
        p.native.historyId === p.historyId && p.native.sessionId === p.sessionId &&
        (p.native.leafId === null || string(p.native.leafId)) && typeof p.native.persisted === "boolean" &&
        brief(p.workbrief) && optional(p, "teamWrapper", text) && Array.isArray(p.activity) && p.activity.length <= 200 &&
        p.activity.every(event => object(event) && keys(event, ["sequence", "kind", "text", "source", "nativeLeafId"]) &&
          integer(event.sequence) && text(event.kind) && text(event.text) && event.source === "event-only" &&
          (event.nativeLeafId === null || string(event.nativeLeafId))) &&
        Array.isArray(p.interactions) && p.interactions.length <= 200 && p.interactions.every(object) &&
        Array.isArray(p.gaps) && p.gaps.every(text) && integer(p.cursor) && typeof p.more === "boolean" &&
        value.next !== null && cursor(value.next, value.ref, "activity") && value.next.after === p.cursor;
    }
    default: return false;
  }
}
function historyReply(value, method, owner) {
  return object(value) && keys(value, ["items", "diagnostics", "cursor"]) &&
    historyOptions({ cursor: value.cursor === null ? undefined : value.cursor }) &&
    (value.cursor === null || typeof value.cursor === "string") &&
    Array.isArray(value.diagnostics) && value.diagnostics.length <= 36 &&
    value.diagnostics.every(item => object(item) && keys(item, ["code", "detail"]) &&
      string(item.code, 200) && optional(item, "detail", text)) &&
    Array.isArray(value.items) && value.items.length <= 200 && value.items.every(item => object(item) &&
      (method === "history.list" ? item.kind === "member" && uuid(item.groupId) && uuid(item.historyId) &&
        string(item.roleId) && item.coordinatorSessionId === owner.coordinatorSessionId && text(item.admittedAt) &&
        optional(item, "instanceId", string) && optional(item, "assignmentId", string) && optional(item, "attemptId", string) &&
        Array.isArray(item.gaps) && item.gaps.every(text) && object(item.native) &&
        ["allocated", "pruned", "not-created"].includes(item.native.state) && item.native.path === undefined &&
        (item.native.state !== "allocated" || string(item.native.sessionId) && object(item.native.disposition) &&
          (item.native.disposition.state === "pending" || item.native.disposition.state === "disposed" && text(item.native.disposition.at) ||
            item.native.disposition.state === "unknown" && text(item.native.disposition.reason))) &&
        item.coordinatorSessionFile === undefined && object(item.group) && item.group.groupId === item.groupId
        : keys(item, ["historyId", "source", "entryOffset", "byteOffset", "endsEntry", "encoding", "data", "validation"]) &&
          uuid(item.historyId) && item.source === "worker" && integer(item.entryOffset) && integer(item.byteOffset) &&
          item.entryOffset <= item.byteOffset && typeof item.endsEntry === "boolean" && item.encoding === "base64" &&
          typeof item.data === "string" && item.data.length <= 10924 &&
          /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(item.data) &&
          ["validation_limit", "corrupt", "json", "fragment"].includes(item.validation)));
}
function publicDetail(value, owner) {
  if (!object(value)) return false;
  if (value.state === "unavailable") return keys(value, ["state", "reason"]) && value.reason === "unsupported_kind";
  const worker = value.worker, lifecycle = value.lifecycle;
  return keys(value, ["format", "ref", "worker", "lifecycle", "bindings", "workbrief", "nextBrief", "selected", "activated",
      "captureEpoch", "conversationSequence", "captureGaps", "nextBindings"]) &&
    value.format === "public-v1" && publicBoundRef(value.ref, owner) && ["agent_spawn", "team_assign"].includes(value.ref.source) &&
    object(worker) && keys(worker, ["source", "instanceId", "roleId", "teamId", "memberId", "turnId", "coordinatorSessionId", "toolCallId", "assignmentId", "acceptedAt", "linkage"]) &&
    ["teamId", "memberId", "turnId"].every(key => optional(worker, key, string)) &&
    worker.source === value.ref.source && worker.instanceId === value.ref.instanceId && string(worker.roleId) &&
    worker.coordinatorSessionId === owner.coordinatorSessionId &&
    (worker.toolCallId === null || string(worker.toolCallId)) &&
    worker.linkage === (worker.toolCallId === null ? "unlinked" : "tool") &&
    worker.assignmentId === value.ref.assignmentId && Number.isFinite(worker.acceptedAt) &&
    object(lifecycle) && keys(lifecycle, ["execution", "result", "capture", "captureReason", "finalSequence"]) &&
    ["accepted", "running", "completed", "failed", "cancelled"].includes(lifecycle.execution) &&
    ["pending", "available", "unavailable"].includes(lifecycle.result) &&
    ["opening", "open", "closed", "incomplete"].includes(lifecycle.capture) &&
    optional(lifecycle, "finalSequence", integer) &&
    optional(lifecycle, "captureReason", reason => ["not_captured", "capture_failed"].includes(reason)) &&
    optional(value, "captureEpoch", string) && optional(value, "conversationSequence", integer) &&
    optional(value, "captureGaps", item => Array.isArray(item) && item.every(publicGap)) &&
    optional(value, "nextBindings", item => cursor(item, value.ref, "public_bindings", true)) &&
    Array.isArray(value.bindings) && value.bindings.length <= 50 && value.bindings.every(binding =>
      object(binding) && keys(binding, ["generation", "historyId", "sessionId", "state", "headEntryId", "capture", "attemptIds", "attempts", "gaps", "ref", "todoHead"]) &&
      optional(binding, "todoHead", publicTodoHead) &&
      integer(binding.generation) && string(binding.historyId) && string(binding.sessionId) &&
      ["active", "historical"].includes(binding.state) && Array.isArray(binding.attemptIds) && binding.attemptIds.every(item => string(item)) &&
      (binding.headEntryId === null || string(binding.headEntryId)) && ["open", "closed", "incomplete"].includes(binding.capture) &&
      publicBoundRef(binding.ref, owner) && binding.ref.instanceId === value.ref.instanceId &&
      binding.ref.native?.generation === binding.generation && binding.ref.native?.sessionId === binding.sessionId &&
      binding.ref.native?.historyId === binding.historyId && Array.isArray(binding.attempts) &&
      binding.attempts.every(attempt => object(attempt) && keys(attempt, ["attemptId", "activated", "selected"]) &&
        string(attempt.attemptId) && optional(attempt, "activated", activation => object(activation) &&
          keys(activation, ["model", "reasoning"]) && text(activation.model) && text(activation.reasoning)) &&
        optional(attempt, "selected", publicSelection)) &&
      Array.isArray(binding.gaps) && binding.gaps.every(publicGap)) &&
    object(value.selected) && keys(value.selected, ["model", "requestedModel", "reasoning"]) && text(value.selected.model) &&
    ["requestedModel", "reasoning"].every(key => optional(value.selected, key, text)) &&
    (value.activated === null || object(value.activated) && keys(value.activated, ["attemptId", "model", "reasoning"]) &&
      ["attemptId", "model", "reasoning"].every(key => text(value.activated[key]))) &&
    brief(value.workbrief) && cursor(value.nextBrief, value.ref, "brief", true);
}
const publicGap = value => ["pending", "not_captured", "not_persisted", "pruned", "evicted", "unsupported_content",
  "capture_failed", "stale_owner", "resync_required"].includes(value);
function publicSelection(value) {
  return object(value) && keys(value, ["model", "requestedModel", "reasoning"]) && text(value.model) &&
    ["requestedModel", "reasoning"].every(key => optional(value, key, text));
}
function publicTodoHead(value) {
  return object(value) && (value.state === "unavailable" ? keys(value, ["state"]) :
    value.state === "ready" && keys(value, ["state", "sourceEntryId"]) &&
    (value.sourceEntryId === null || string(value.sourceEntryId)));
}
function publicTodoSnapshot(value) {
  if (!object(value) || !keys(value, ["format", "tasks"]) || value.format !== "rpiv-todo-v1" ||
      !Array.isArray(value.tasks)) return false;
  const ids = new Set();
  return value.tasks.every(task => {
    if (!object(task) || !keys(task, ["id", "identityEntryId", "subject", "status"]) ||
        !Number.isSafeInteger(task.id) || task.id <= 0 || ids.has(task.id) ||
        !string(task.identityEntryId) || typeof task.subject !== "string" ||
        !["pending", "in_progress", "completed", "deleted"].includes(task.status)) return false;
    ids.add(task.id);
    return true;
  });
}
function publicContent(value) {
  return Array.isArray(value) && value.every(block => object(block) &&
    (block.type === "text" ? keys(block, ["type", "text"]) && text(block.text) :
      block.type === "toolCall" ? keys(block, ["type", "toolCallId", "name", "arguments"]) &&
        string(block.toolCallId) && string(block.name) :
        block.type === "omitted" && keys(block, ["type", "reason"]) && block.reason === "unsupported_content"));
}
function publicChange(change) {
  const record = change?.kind === "fragment" ? change : change?.record;
  if (!object(record) || !string(record.id) || !integer(record.binding) || !integer(record.revision) ||
      record.kind === "fragment" && record.sequence !== null && !integer(record.sequence)) return false;
  if (record.kind === "fragment") return keys(record, ["kind", "id", "binding", "revision", "sequence", "encoding", "byteOffset", "totalBytes", "data"]) &&
    record.encoding === "base64-json" && integer(record.byteOffset) && integer(record.totalBytes) &&
    record.byteOffset < record.totalBytes && text(record.data) &&
    /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(record.data);
  if (!object(change) || !keys(change, ["sequence", "op", "record"]) || change.op !== "upsert" ||
      !(integer(change.sequence) || change.sequence === null && record.recovered === true)) return false;
  const payload = record.payload;
  const validPayload = record.kind === "message" ? object(payload) && keys(payload, ["role", "blocks", "stopReason", "errorMessage"]) &&
      ["user", "assistant"].includes(payload.role) && publicContent(payload.blocks) &&
      optional(payload, "stopReason", reason => ["pending", "stop", "length", "toolUse", "error", "aborted", "deferred"].includes(reason)) &&
      optional(payload, "errorMessage", text) && (payload.role === "assistant" || payload.stopReason === undefined && payload.errorMessage === undefined) &&
      string(record.nativeEntryId) && record.toolCallId === null
    : record.kind === "tool" ? object(payload) && keys(payload, ["status", "name", "args", "argsState", "result", "isError", "todoSnapshot"]) &&
      optional(payload, "todoSnapshot", publicTodoSnapshot) &&
      ["started", "updated", "completed", "error"].includes(payload.status) && string(payload.name) &&
      ["available", "not_captured"].includes(payload.argsState) && (payload.result === null || publicContent(payload.result)) &&
      (payload.isError === null || typeof payload.isError === "boolean") && string(record.toolCallId) &&
      optional(record, "parentToolCallId", string)
    : record.kind === "branch_changed" && object(payload) && keys(payload, ["headEntryId", "previousHeadEntryId"]) &&
      (payload.headEntryId === null || string(payload.headEntryId)) && string(payload.previousHeadEntryId) && record.toolCallId === null;
  return validPayload && keys(record, ["id", "binding", "revision", "kind", "order", "nativeOrder", "occurredAt", "observedAt", "nativeTime", "nativeEntryId",
      "parentEntryId", "toolCallId", "parentToolCallId", "payload", "recovered"]) &&
    ["message", "tool", "branch_changed"].includes(record.kind) && Number.isFinite(record.observedAt) &&
    integer(record.order) && (record.nativeOrder === null || integer(record.nativeOrder)) &&
    (record.occurredAt === null || Number.isFinite(record.occurredAt)) &&
    (record.nativeTime === null || text(record.nativeTime)) &&
    ["nativeEntryId", "parentEntryId", "toolCallId"].every(key => record[key] === null || string(record[key])) &&
    optional(record, "recovered", value => value === true);
}
function publicActivity(value, owner) {
  return object(value) && keys(value, ["format", "state", "ref", "epoch", "cut", "changes", "next", "more", "lost", "gaps", "binding", "capture"]) &&
    value.format === "public-v1" && ["ready", "resync_required"].includes(value.state) &&
    publicBoundRef(value.ref, owner) && string(value.epoch) && integer(value.cut) &&
    (value.next === null || string(value.next, 16 * 1024)) &&
    typeof value.more === "boolean" &&
    optional(value, "lost", item => object(item) && keys(item, ["from", "through"]) && integer(item.from) && integer(item.through)) &&
    optional(value, "binding", item => object(item) && keys(item, ["generation", "historyId", "sessionId", "state", "headEntryId", "capture", "attemptIds", "attempts", "gaps", "todoHead"]) &&
      optional(item, "todoHead", publicTodoHead) &&
      item.generation === value.ref.native?.generation && item.sessionId === value.ref.native?.sessionId &&
      item.historyId === value.ref.native?.historyId && ["active", "historical"].includes(item.state) &&
      (item.headEntryId === null || string(item.headEntryId)) && ["open", "closed", "incomplete"].includes(item.capture) &&
      Array.isArray(item.attemptIds) && item.attemptIds.every(id => string(id)) &&
      Array.isArray(item.attempts) && item.attempts.every(attempt => object(attempt) &&
        keys(attempt, ["attemptId", "activated", "selected"]) && string(attempt.attemptId) &&
        optional(attempt, "activated", activation => object(activation) && keys(activation, ["model", "reasoning"]) &&
          text(activation.model) && text(activation.reasoning)) && optional(attempt, "selected", publicSelection)) &&
      Array.isArray(item.gaps) && item.gaps.every(publicGap)) &&
    optional(value, "capture", item => object(item) && keys(item, ["state", "finalSequence"]) &&
      ["opening", "open", "closed", "incomplete"].includes(item.state) && (item.finalSequence === null || integer(item.finalSequence))) &&
    Array.isArray(value.changes) && value.changes.length <= 50 && value.changes.every(publicChange) &&
    Array.isArray(value.gaps) && value.gaps.every(publicGap);
}
export function isReply(value, method, format) {
  if (!object(value) || value.version !== version || !string(value.id, 128) ||
      !string(value.clientId, 128) || !isOwner(value.owner)) return false;
  if (value.ok === false) return keys(value, ["version", "id", "clientId", "owner", "ok", "error"]) &&
    object(value.error) && keys(value.error, ["code", "message"]) &&
    Object.hasOwn(errors, value.error.code) && value.error.message === errors[value.error.code];
  if (value.ok !== true || !keys(value, ["version", "id", "clientId", "owner", "ok", "result"])) return false;
  if (method === "background.pending") return object(value.result) &&
    (["pending", "none"].includes(value.result.state) ? keys(value.result, ["state"])
      : value.result.state === "unavailable" && keys(value.result, ["state", "reason"]) &&
        ["unsupported", "stale_owner", "source_unavailable"].includes(value.result.reason));
  if (method === "architecture.list" || method === "architecture.read") return architectureReply(value.result, method, value.owner);
  if (method === "connect") return object(value.result) && keys(value.result, ["capabilities", "lease"]) &&
    isCapabilities(value.result.capabilities) &&
    object(value.result.lease) && keys(value.result.lease, ["clientId", "resumeToken"]) &&
    value.result.lease.clientId === value.clientId && token(value.result.lease.resumeToken);
  if (method === "subscribe") return object(value.result) && keys(value.result, ["subscriptionId", "snapshot"]) &&
    string(value.result.subscriptionId, 128) && isList(value.result.snapshot, value.owner);
  if (method === "unsubscribe") return object(value.result) && keys(value.result, ["subscriptionId"]) &&
    string(value.result.subscriptionId, 128);
  if (method === "route.read") return object(value.result) && keys(value.result, ["state", "page"]) &&
    ["captured", "not-routed", "unavailable"].includes(value.result.state) && page(value.result.page);
  if (method === "history.list" || method === "history.read") return historyReply(value.result, method, value.owner);
  if (method === "workers.detail" && format === "public-v1")
    return publicDetail(value.result, value.owner);
  if (format === "public-v1" && method === "workers.activity")
    return available(value.result) || publicActivity(value.result, value.owner);
  if (format === "public-v1" && method === "workers.result")
    return available(value.result) || object(value.result) &&
      keys(value.result, ["format", "state", "ref", "status", "text", "nextText"]) &&
      value.result.format === "public-v1" && value.result.state === "ready" && publicBoundRef(value.result.ref, value.owner) &&
      ["completed", "failed", "cancelled"].includes(value.result.status) && page(value.result.text) &&
      cursor(value.result.nextText, value.result.ref, "result_text", true);
  return method === "workers.list" ? isList(value.result, value.owner) : readReply(value.result, method, value.owner);
}

/** Content-free coalescible hint; refresh is mandatory, never replay. */
export function isNotification(value) {
  return object(value) && keys(value, ["version", "method", "clientId", "owner", "subscriptionId", "revision", "refreshRequired"]) &&
    value.version === version && value.method === "changed" && string(value.clientId, 128) && isOwner(value.owner) &&
    string(value.subscriptionId, 128) && integer(value.revision) && value.refreshRequired === true;
}

const architectureCursor = value => string(value, 16 * 1024);
const unboundedText = value => typeof value === "string";
const nonempty = value => typeof value === "string" && value.length > 0;
const array = (value, validate) => Array.isArray(value) && value.every(validate);
const fields = (value, names) => object(value) && keys(value, names);
const strings = value => array(value, nonempty);
const doubt = value => fields(value, ["question", "purpose"]) && nonempty(value.question) && nonempty(value.purpose);
const resolution = (value, statuses, extra) => fields(value, ["doubtId", "status", "answer", ...extra]) &&
  nonempty(value.doubtId) && statuses.includes(value.status) && nonempty(value.answer);
export function isPublicArchitectureBody(body) {
  if (!object(body)) return false;
  switch (body.kind) {
    case "composition": return keys(body, ["kind", "snapshotId", "specialists"]) && nonempty(body.snapshotId) &&
      Array.isArray(body.specialists) && body.specialists.length >= 1 && body.specialists.length <= 5 && body.specialists.every(item =>
        fields(item, ["candidateRef", "mandate", "reason"]) && [item.candidateRef, item.mandate, item.reason].every(nonempty));
    case "preparation": return keys(body, ["kind", "analysis", "doubts"]) && nonempty(body.analysis) && array(body.doubts, doubt);
    case "contribution": return keys(body, ["kind", "proposal", "arguments", "objections", "doubts"]) && nonempty(body.proposal) && strings(body.arguments) &&
      array(body.objections, item => fields(item, ["memberId", "messageId", "argument"]) && [item.memberId, item.messageId, item.argument].every(nonempty)) && array(body.doubts, doubt);
    case "decision": return keys(body, ["kind", "action", "reason"]) && ["continue", "consult", "synthesize", "recompose"].includes(body.action) && nonempty(body.reason);
    case "answer":
    case "consultation-response": return keys(body, ["kind", "answer", "evidence", "limits", "doubts", ...(body.kind === "answer" ? [] : ["doubtIds"])]) &&
      nonempty(body.answer) && strings(body.evidence) && strings(body.limits) && array(body.doubts, doubt) && (body.kind === "answer" || strings(body.doubtIds));
    case "consultation-plan": return keys(body, ["kind", "resolutions", "support"]) &&
      array(body.resolutions, item => resolution(item, ["answered", "rejected"], [])) && array(body.support, item =>
        fields(item, ["role", "brief", "doubtIds", "prerequisites"]) && ["scout", "researcher", "oracle"].includes(item.role) && nonempty(item.brief) &&
        strings(item.doubtIds) && array(item.prerequisites, integer));
    case "knowledge": return keys(body, ["kind", "resolutions"]) && array(body.resolutions, item =>
      resolution(item, ["answered", "partial", "rejected", "failed"], ["supportIds"]) && strings(item.supportIds));
    case "synthesis": return keys(body, ["kind", "status", "recommendation", "alternatives", "evidence", "assumptions", "disagreements", "pending"]) &&
      ["complete", "partial", "failed"].includes(body.status) && nonempty(body.recommendation) && ["alternatives", "evidence", "assumptions", "disagreements", "pending"].every(key => strings(body[key]));
    default: return false;
  }
}
const architectureStages = ["composition", "preparation", "contribution", "decision", "answer", "consultation-plan", "consultation-response", "knowledge", "synthesis"];
const architectureStage = value => value === null || [...architectureStages, "waiting", "closing"].includes(value);
export const isArchitectureRef = value => fields(value, ["kind", "processId", "coordinatorSessionId", "epoch", "roomId"]) && value.kind === "architecture" &&
  isOwner({ processId: value.processId, coordinatorSessionId: value.coordinatorSessionId, epoch: value.epoch }) && string(value.roomId);
function architectureLifecycle(value) {
  return fields(value, ["stage", "round", "protocolState", "outcome", "closure"]) && architectureStage(value.stage) && integer(value.round) &&
    ["starting", "running", "waiting", "finished", "failed", "cancelled", "closure-unknown"].includes(value.protocolState) && ["pending", "available", "unavailable"].includes(value.outcome) &&
    fields(value.closure, ["state", "members", "director"]) && ["pending", "released", "unknown"].includes(value.closure.state) &&
    ["members", "director"].every(key => ["not-created", "pending", "released", "unknown"].includes(value.closure[key]));
}
const architectureIdentity = value => fields(value, ["memberId", "assignmentId", "instanceId"]) && [value.memberId, value.assignmentId, value.instanceId].every(item => string(item));
function architectureMessage(value) {
  return fields(value, ["id", "ordinal", "roomId", "assignmentId", "memberId", "author", "stage", "snapshotId", "round", "turnId", "attemptId", "body", "native", "doubts"]) &&
    ["id", "roomId", "assignmentId", "memberId", "author", "snapshotId", "turnId", "attemptId"].every(key => string(value[key])) && integer(value.ordinal) &&
    optional(value, "round", integer) && architectureStages.includes(value.stage) && isPublicArchitectureBody(value.body) && value.body.kind === value.stage &&
    fields(value.native, ["historyId", "sessionId", "toolCallId", "persistence"]) && optional(value.native, "historyId", string) && string(value.native.sessionId) &&
    string(value.native.toolCallId) && value.native.persistence === "unconfirmed" && array(value.doubts, item => fields(item, ["id", "question", "purpose"]) &&
      string(item.id) && nonempty(item.question) && nonempty(item.purpose));
}
export function isArchitectureRecord(value) {
  if (!object(value) || !string(value.id)) return false;
  switch (value.kind) {
    case "room": return keys(value, ["kind", "id", "mode", "origin", "goalSource", "lifecycle", "pendingDoubtIds"]) && ["table", "consultation"].includes(value.mode) &&
      fields(value.origin, ["coordinatorSessionId", "tool", "toolCallId", "memberId", "assignmentId", "instanceId"]) && value.origin.tool === "architecture_start" &&
      ["coordinatorSessionId", "toolCallId", "memberId", "assignmentId", "instanceId"].every(key => string(value.origin[key])) &&
      fields(value.goalSource, ["kind", "instanceId"]) && value.goalSource.kind === "worker-workbrief" && string(value.goalSource.instanceId) &&
      architectureLifecycle(value.lifecycle) && strings(value.pendingDoubtIds);
    case "member": return keys(value, ["kind", "id", "assignmentId", "instanceId", "roleId", "function", "ordinal", "admittedAt", "mandate", "execution", "workerRef", "conversationRefs", "compositionMessageId", "supersededByCompositionId"]) &&
      ["assignmentId", "instanceId", "roleId"].every(key => string(value[key])) && ["director", "specialist", "support"].includes(value.function) && integer(value.ordinal) && Number.isFinite(value.admittedAt) &&
      object(value.mandate) && (value.mandate.source === "root-workbrief" ? keys(value.mandate, ["source", "instanceId"]) && string(value.mandate.instanceId) :
        value.mandate.source === "TeamAssignment.task" && keys(value.mandate, ["source", "text"]) && unboundedText(value.mandate.text)) &&
      ["accepted", "running", "completed", "failed", "cancelled"].includes(value.execution) && optional(value, "workerRef", item => readReference(item, true)) &&
      array(value.conversationRefs, item => readReference(item, true)) && ["compositionMessageId", "supersededByCompositionId"].every(key => optional(value, key, string));
    case "publication": return keys(value, ["kind", "id", "message"]) && architectureMessage(value.message) && value.id === value.message.id;
    case "knowledge-boundary": return keys(value, ["kind", "id", "ordinal", "roomId", "cycleId", "planMessageId", "knowledgeMessageId", "resolutions"]) &&
      integer(value.ordinal) && ["roomId", "cycleId", "planMessageId"].every(key => string(value[key])) &&
      (value.knowledgeMessageId === null || string(value.knowledgeMessageId)) && array(value.resolutions, item =>
        resolution(item, ["answered", "partial", "rejected", "failed"], ["supportIds", "provenance"]) && strings(item.supportIds) && object(item.provenance) &&
        (item.provenance.kind === "direct" ? fields(item.provenance, ["kind", "planMessageId"]) && string(item.provenance.planMessageId) && item.supportIds.length === 0 :
          item.provenance.kind === "delegated" && fields(item.provenance, ["kind", "knowledgeMessageId", "consultationId", "responses"]) &&
          string(item.provenance.knowledgeMessageId) && string(item.provenance.consultationId) && array(item.provenance.responses, response =>
            fields(response, ["consultationId", "assignmentId", "messageId", "status"]) && string(response.consultationId) &&
            optional(response, "assignmentId", string) && optional(response, "messageId", string) && ["completed", "failed"].includes(response.status))));
    case "control": return keys(value, ["kind", "id", "order", "action", "text", "author", "application"]) && integer(value.order) &&
      ["context", "priorities", "finish", "stop"].includes(value.action) && optional(value, "text", unboundedText) &&
      fields(value.author, ["coordinatorSessionId", "toolCallId"]) && string(value.author.coordinatorSessionId) && string(value.author.toolCallId) && object(value.application) &&
      (value.application.state === "pending" ? keys(value.application, ["state"]) : value.application.state === "applied" && keys(value.application, ["state", "stage", "round"]) && architectureStage(value.application.stage) && integer(value.application.round));
    case "failure": return keys(value, ["kind", "id", "phase", "error", "memberId", "assignmentId", "stage", "round"]) &&
      ["preparation", "admission", "turn", "disband", "director-close"].includes(value.phase) && unboundedText(value.error) &&
      ["memberId", "assignmentId"].every(key => optional(value, key, string)) && optional(value, "stage", item => architectureStages.includes(item)) && optional(value, "round", integer);
    case "outcome": return keys(value, ["kind", "id", "source", "status", "content"]) && value.id === "final" && value.source === "background-settlement" &&
      ["completed", "failed", "cancelled"].includes(value.status) && object(value.content) && (value.content.kind === "text" ? keys(value.content, ["kind", "text"]) && unboundedText(value.content.text) :
        value.content.kind === "synthesis" && keys(value.content, ["kind", "body"]) && value.content.body?.kind === "synthesis" && isPublicArchitectureBody(value.content.body));
    default: return false;
  }
}
function architectureFragment(value) {
  return fields(value, ["kind", "recordId", "encoding", "byteOffset", "totalBytes", "data"]) && value.kind === "fragment" && string(value.recordId) &&
    value.encoding === "base64-json" && integer(value.byteOffset) && integer(value.totalBytes) && value.byteOffset < value.totalBytes &&
    typeof value.data === "string" && /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(value.data) &&
    Buffer.from(value.data, "base64").length > 0 && Buffer.from(value.data, "base64").length <= 12 * 1024 && value.byteOffset + Buffer.from(value.data, "base64").length <= value.totalBytes;
}
function architectureReply(value, method, owner) {
  if (!object(value)) return false;
  if (value.state !== "ready") return value.state === "unavailable" ? keys(value, ["state", "reason"]) &&
    ["not_captured", "snapshot_oversized", ...(method === "architecture.read" ? ["unknown_room"] : [])].includes(value.reason) :
    keys(value, ["state"]) && ["stale_owner", "stale_cursor", "invalid_cursor"].includes(value.state);
  if (Buffer.byteLength(JSON.stringify(value)) > 40 * 1024 || value.format !== "architecture-public-v1" || !string(value.snapshotId) || !integer(value.cut) ||
      !(value.next === null || architectureCursor(value.next))) return false;
  if (method === "architecture.list") return keys(value, ["format", "state", "owner", "snapshotId", "cut", "items", "next"]) && isOwner(value.owner) && sameOwner(value.owner, owner) &&
    array(value.items, item => fields(item, ["ref", "mode", "director", "lifecycle"]) && isArchitectureRef(item.ref) && sameOwner(item.ref, owner) &&
      ["table", "consultation"].includes(item.mode) && architectureIdentity(item.director) && architectureLifecycle(item.lifecycle));
  return keys(value, ["format", "state", "ref", "snapshotId", "cut", "capture", "records", "next"]) && isArchitectureRef(value.ref) && sameOwner(value.ref, owner) &&
    fields(value.capture, ["state", "finalCut", "gaps"]) && ["open", "closed", "incomplete"].includes(value.capture.state) &&
    (value.capture.finalCut === null || integer(value.capture.finalCut) && value.capture.finalCut <= value.cut) && array(value.capture.gaps, item => ["not_captured", "capture_failed"].includes(item)) &&
    array(value.records, item => isArchitectureRecord(item) || architectureFragment(item));
}
