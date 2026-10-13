/** Authoritative v1 wire validation. Passive: no Pi, discovery, or service initialization. */
export const version = 1;
export const replyBytes = 48 * 1024;
export const outstandingRequests = 8;
export const queueBytes = 256 * 1024;
export const capabilities = Object.freeze(["workers.list", "workers.detail", "workers.requests", "workers.activity", "workers.result", "subscribe", "unsubscribe",
  "history.list", "history.read", "route.read", "view.grant", "input", "cancel", "request.status"]);
export const object = value => !!value && typeof value === "object" && !Array.isArray(value);
const keys = (value, allowed) => Object.keys(value).every(key => allowed.includes(key));
export const string = (value, max = 1024) =>
  typeof value === "string" && value.length > 0 && Buffer.byteLength(value) <= max && !value.includes("\0");
export const isOwner = value => object(value) && keys(value, ["processId", "coordinatorSessionId", "epoch"]) &&
  string(value.processId) && string(value.coordinatorSessionId) && string(value.epoch);
export const sameOwner = (a, b) => a.processId === b.processId &&
  a.coordinatorSessionId === b.coordinatorSessionId && a.epoch === b.epoch;
export const isGrants = value => Array.isArray(value) && value.length <= 3 &&
  new Set(value).size === value.length && value.every(item => ["history", "route", "control"].includes(item));
const isCapabilities = value => Array.isArray(value) && value.length === capabilities.length &&
  value.every((item, index) => item === capabilities[index]);
const integer = value => Number.isSafeInteger(value) && value >= 0;
const mutationSequence = value => integer(value) && value > 0;
const token = value => typeof value === "string" && /^[a-f0-9]{64}$/.test(value);
const sensitiveGrant = value => ["history", "route"].includes(value);
const uuid = value => typeof value === "string" && /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value);
const historyOptions = value => (value.cursor === undefined || typeof value.cursor === "string" &&
  value.cursor.length <= 2048 && /^[\w-]+$/.test(value.cursor)) &&
  (value.limit === undefined || integer(value.limit) && value.limit >= 1 && value.limit <= 200);
export const isReadReference = value => object(value) && Buffer.byteLength(JSON.stringify(value)) <= 4096 &&
  keys(value, ["kind", "processId", "coordinatorSessionId", "epoch", "instanceId", "source", "assignmentId", "evaluationId", "attemptId", "native"]) &&
  value.kind === "read" && isOwner({ processId: value.processId, coordinatorSessionId: value.coordinatorSessionId, epoch: value.epoch }) &&
  string(value.instanceId) && ["agent_run", "agent_spawn", "team_assign"].includes(value.source) &&
  (value.source === "team_assign" ? string(value.assignmentId) && string(value.evaluationId)
    : value.assignmentId === undefined && value.evaluationId === undefined) &&
  (value.attemptId === undefined || string(value.attemptId)) &&
  (value.native === undefined || object(value.native) && keys(value.native, ["generation", "historyId", "sessionId"]) &&
    integer(value.native.generation) && string(value.native.historyId) && string(value.native.sessionId) && string(value.attemptId));
export const isReadCursor = value => object(value) && isReadReference(value.ref) &&
  (value.section === "activity" ? keys(value, ["ref", "section", "after"]) && integer(value.after)
    : ["brief", "detail_metadata", "result_text", "result_metadata", "requests"].includes(value.section) &&
      keys(value, value.section === "requests" ? ["ref", "section", "offset", "digest"] : ["ref", "section", "offset"]) &&
      integer(value.offset) && (value.section !== "requests" || typeof value.digest === "string" && /^[a-f0-9]{64}$/.test(value.digest)));
export const isSelection = value => object(value) && Buffer.byteLength(JSON.stringify(value)) <= 8192 &&
  keys(value, ["processId", "coordinatorSessionId", "epoch", "target", "instanceId", "generation", "historyId", "sessionId"]) &&
  isOwner({ processId: value.processId, coordinatorSessionId: value.coordinatorSessionId, epoch: value.epoch }) &&
  ["instanceId", "historyId", "sessionId"].every(key => string(value[key])) && integer(value.generation) &&
  object(value.target) && (value.target.kind === "background"
    ? keys(value.target, ["kind", "instanceId"]) && value.target.instanceId === value.instanceId
    : value.target.kind === "team" && keys(value.target, ["kind", "assignmentId"]) && string(value.target.assignmentId));
export const isDescriptor = value => object(value) &&
  keys(value, ["version", "transport", "socket", "token", "owner", "grants", "capabilities"]) &&
  value.version === version && value.transport === "unix" && string(value.socket, 103) && value.socket.startsWith("/") &&
  typeof value.token === "string" && /^[a-f0-9]{64}$/.test(value.token) &&
  isOwner(value.owner) && isGrants(value.grants) && isCapabilities(value.capabilities);
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
    case "input": return keys(value.params, ["sequence", "selection", "input"]) &&
      mutationSequence(value.params.sequence) && isSelection(value.params.selection) &&
      object(value.params.input) && keys(value.params.input, ["intent", "text", "decisionId"]) &&
      ["query", "steer"].includes(value.params.input.intent) && typeof value.params.input.text === "string" &&
      value.params.input.text.trim().length > 0 && Buffer.byteLength(value.params.input.text) <= 16 * 1024 &&
      (value.params.input.decisionId === undefined || string(value.params.input.decisionId, 1024));
    case "cancel": return keys(value.params, ["sequence", "selection"]) &&
      mutationSequence(value.params.sequence) && isSelection(value.params.selection);
    case "request.status": return keys(value.params, ["sequence"]) && mutationSequence(value.params.sequence);
    case "workers.list": return keys(value.params, ["after", "revision"]) &&
      (value.params.after === undefined || string(value.params.after) && integer(value.params.revision)) &&
      (value.params.revision === undefined || integer(value.params.revision));
    case "subscribe": return keys(value.params, []);
    case "unsubscribe": return keys(value.params, ["subscriptionId"]) && string(value.params.subscriptionId, 128);
    case "workers.detail":
    case "workers.requests": return keys(value.params, ["id", "cursor"]) && string(value.params.id) &&
      (value.params.cursor === undefined || isReadCursor(value.params.cursor) &&
        (value.method === "workers.requests" ? value.params.cursor.section === "requests"
          : ["brief", "detail_metadata"].includes(value.params.cursor.section)));
    case "workers.activity":
    case "workers.result": return keys(value.params, ["ref", "cursor"]) && isReadReference(value.params.ref) &&
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
    case "view.grant": return value.params.acknowledged === true &&
      (value.params.grant === "control" ? keys(value.params, ["grant", "acknowledged", "selection"]) &&
        isSelection(value.params.selection) : keys(value.params, ["grant", "acknowledged"]) && sensitiveGrant(value.params.grant));
    default: return false;
  }
}
export const errors = Object.freeze({
  invalid_request: "Invalid bridge request",
  unsupported_method: "Unsupported bridge method",
  unauthorized: "Bridge authentication failed",
  forbidden: "Operation requires a startup ceiling and explicit current-view activation",
  sequence_conflict: "Mutation sequence payload differs from its admitted request",
  lease_expired: "Logical client lease is expired or unknown; outcome is unknown, never replay",
  stale_selection: "Worker control identity changed; refresh detail",
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
  return object(value) && keys(value, ["attemptId", "historyId", "sessionId", "sdkOptions", "model", "provider",
    "requestId", "inputTokens", "outputTokens", "reasoning", "fast_requested", "requested_service_tier",
    "returned_service_tier", "time_to_first_model_output_ms", "time_to_first_model_output_unavailable_reason"]) &&
    ["attemptId", "historyId", "sessionId", "provider", "requestId", "reasoning"].every(key => optional(value, key, text)) &&
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
      keys(row, ["id", "roleId", "status", "source", "assignmentId", "task", "selectedModel", "requestedModel",
        "requestedReasoning", "activated", "sdkRequest", "acceptedAt", "terminalAt", "activity", "phase", "profile",
        "resultTaken", "handoff"]) &&
      string(row.id) && string(row.roleId) &&
      ["created", "running", "completed", "failed", "cancelled"].includes(row.status) &&
      optional(row, "source", source => ["agent_run", "agent_spawn", "team_assign"].includes(source)) &&
      ["assignmentId", "requestedModel", "requestedReasoning", "profile"].every(key => optional(row, key, text)) &&
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
const cursor = (value, ref, section) => value === null || isReadCursor(value) && value.section === section &&
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
      "metadata", "workbrief", "nextMetadata", "nextBrief", "controls"]) && status(value.status) && status(value.observedStatus) &&
      ["ready", "pending", "unavailable", "evicted", "stale_reference", "stale_owner", "invalid_cursor"].includes(value.result) &&
      ["observed_binding", "unavailable"].includes(value.activity) && page(value.metadata) && brief(value.workbrief) &&
      cursor(value.nextMetadata, value.ref, "detail_metadata") && cursor(value.nextBrief, value.ref, "brief") &&
      object(value.controls) && keys(value.controls, ["selection", "reason", "grant"]) &&
      ["denied", "enabled"].includes(value.controls.grant) && optional(value.controls, "reason", text) &&
      optional(value.controls, "selection", item => isSelection(item) && sameOwner(item, owner));
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
export function isReply(value, method) {
  if (!object(value) || value.version !== version || !string(value.id, 128) ||
      !string(value.clientId, 128) || !isOwner(value.owner)) return false;
  if (value.ok === false) return keys(value, ["version", "id", "clientId", "owner", "ok", "error"]) &&
    object(value.error) && keys(value.error, ["code", "message"]) &&
    Object.hasOwn(errors, value.error.code) && value.error.message === errors[value.error.code];
  if (value.ok !== true || !keys(value, ["version", "id", "clientId", "owner", "ok", "result"])) return false;
  if (method === "connect") return object(value.result) && keys(value.result, ["capabilities", "grants", "lease"]) &&
    isCapabilities(value.result.capabilities) && Array.isArray(value.result.grants) && value.result.grants.length === 0 &&
    object(value.result.lease) && keys(value.result.lease, ["clientId", "resumeToken", "highWater"]) &&
    value.result.lease.clientId === value.clientId && token(value.result.lease.resumeToken) && integer(value.result.lease.highWater);
  if (["input", "cancel", "request.status"].includes(method)) return isMutationStatus(value.result);
  if (method === "subscribe") return object(value.result) && keys(value.result, ["subscriptionId", "snapshot"]) &&
    string(value.result.subscriptionId, 128) && isList(value.result.snapshot, value.owner);
  if (method === "unsubscribe") return object(value.result) && keys(value.result, ["subscriptionId"]) &&
    string(value.result.subscriptionId, 128);
  if (method === "view.grant") return object(value.result) && keys(value.result, ["grant", "acknowledged"]) &&
    ["history", "route", "control"].includes(value.result.grant) && value.result.acknowledged === true;
  if (method === "route.read") return object(value.result) && keys(value.result, ["state", "page"]) &&
    ["captured", "not-routed", "unavailable"].includes(value.result.state) && page(value.result.page);
  if (method === "history.list" || method === "history.read") return historyReply(value.result, method, value.owner);
  return method === "workers.list" ? isList(value.result, value.owner) : readReply(value.result, method, value.owner);
}

export function isMutationStatus(value) {
  if (!object(value) || !mutationSequence(value.sequence)) return false;
  if (["pending", "outcome_unknown", "not_admitted"].includes(value.state)) return keys(value, ["state", "sequence"]);
  if (value.state !== "recorded" || !keys(value, ["state", "sequence", "receipt"]) || !object(value.receipt)) return false;
  const receipt = value.receipt, native = receipt.native;
  if (receipt.kind === "error") return keys(receipt, ["kind", "code"]) && Object.hasOwn(errors, receipt.code);
  if (!keys(receipt, ["kind", "native"]) || !object(native)) return false;
  if (receipt.kind === "cancel") return keys(native, ["status", "instanceId", "historyId", "sessionId"]) &&
    native.status === "requested" && ["instanceId", "historyId", "sessionId"].every(key => string(native[key]));
  return receipt.kind === "input" &&
    keys(native, ["interactionId", "historyId", "sessionId", "intent", "decisionId", "status", "nativeEntryId", "delivery", "reason", "candidateAnswer"]) &&
    string(native.interactionId) && string(native.historyId) && string(native.sessionId) &&
    ["query", "steer"].includes(native.intent) && ["queued", "handled", "rejected", "unconfirmed"].includes(native.status) &&
    optional(native, "decisionId", item => string(item, 1024)) && optional(native, "nativeEntryId", item => item === null || string(item)) &&
    optional(native, "delivery", text) && optional(native, "reason", text) &&
    optional(native, "candidateAnswer", item => object(item) && keys(item, ["sequence", "basis"]) && integer(item.sequence) && text(item.basis));
}

/** Content-free coalescible hint; refresh is mandatory, never replay. */
export function isNotification(value) {
  return object(value) && keys(value, ["version", "method", "clientId", "owner", "subscriptionId", "revision", "refreshRequired"]) &&
    value.version === version && value.method === "changed" && string(value.clientId, 128) && isOwner(value.owner) &&
    string(value.subscriptionId, 128) && integer(value.revision) && value.refreshRequired === true;
}
