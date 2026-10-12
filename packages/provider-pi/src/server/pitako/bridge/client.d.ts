import type { Owner, WorkerList, Capability, Notification, WorkerReadReference, WorkerReadCursor, WorkerDetail, WorkerRequests, WorkerActivity, WorkerResult } from "./protocol.js";
import type { SensitiveGrant, ViewAcknowledgement, HistoryListOptions, HistoryReadOptions, HistoryPage, HistoryMetadata, RoutePage } from "./protocol.js";
import type { AgentSelection, ClientLease, ResumeLease, NativeInput, MutationStatus } from "./protocol.js";
export type { AgentSelection, ClientLease, ResumeLease, NativeInput, NativeInputReceipt, MutationStatus } from "./protocol.js";
export type { WorkerReadReference, WorkerReadCursor, WorkerDetail, WorkerRequests, WorkerActivity, WorkerResult } from "./protocol.js";
export type { SensitiveGrant, ViewAcknowledgement, HistoryListOptions, HistoryReadOptions, HistoryPage, HistoryMetadata, HistoryFragment, RoutePage } from "./protocol.js";
export class BridgeError extends Error { readonly code: string; constructor(code: string, message: string); }
/** Every hint requires refresh. Gaps are not replayed; refresh again on reconnect. */
export type ChangeHint = Readonly<Notification & { gap: boolean }>;
export interface BridgeSubscription {
  readonly subscriptionId: string;
  readonly snapshot: WorkerList;
  readonly active: boolean;
  refresh(): Promise<WorkerList>;
  /** Synchronously silences callbacks, then joins server disposal. Idempotent. */
  dispose(): Promise<void>;
}
export interface BridgeClient {
  readonly owner: Readonly<Owner>;
  readonly capabilities: readonly Capability[];
  /** Private reconnect credential plus the server's high-water mark at this connection. Never put in model context. */
  readonly lease: Readonly<ClientLease>;
  /** Carry the first page revision on continuation; stale_cursor requires a fresh first page. */
  list(after?: string, revision?: number): Promise<WorkerList>;
  subscribe(listener: (hint: ChangeHint) => void): Promise<BridgeSubscription>;
  detail(id: string, cursor?: WorkerReadCursor): Promise<WorkerDetail>;
  requests(id: string, cursor?: WorkerReadCursor): Promise<WorkerRequests>;
  activity(ref: WorkerReadReference, cursor?: WorkerReadCursor): Promise<WorkerActivity>;
  result(ref: WorkerReadReference, cursor?: WorkerReadCursor): Promise<WorkerResult>;
  historyList(options?: HistoryListOptions): Promise<HistoryPage<HistoryMetadata>>;
  historyRead(options: HistoryReadOptions): Promise<HistoryPage>;
  routeRead(id: string, offset?: number): Promise<RoutePage>;
  /** Explicit fresh acknowledgement/activation. Never restored on connect/refresh/reconnect. */
  grant(grant: SensitiveGrant, acknowledged: true): Promise<ViewAcknowledgement>;
  grant(grant: "control", acknowledged: true, selection: AgentSelection): Promise<ViewAcknowledgement>;
  /** Sequences are explicit new client decisions, never automatic transport retries. Native receipts are not answer proof. */
  input(selection: AgentSelection, input: NativeInput, sequence: number): Promise<MutationStatus>;
  cancel(selection: AgentSelection, sequence: number): Promise<MutationStatus>;
  requestStatus(sequence: number): Promise<MutationStatus>;
  disconnect(): Promise<void>;
}
/** Explicit host entry: import from "pitako/extensions/agent/bridge/client.js". */
export function connect(descriptorPath: string, options: { expectedSessionId: string; resume?: ResumeLease }): Promise<BridgeClient>;
