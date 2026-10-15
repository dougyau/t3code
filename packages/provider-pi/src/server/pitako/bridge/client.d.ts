import type { Owner, WorkerList, Capability, Notification, WorkerReadReference, WorkerReadCursor, WorkerDetail, WorkerRequests, WorkerActivity, WorkerResult } from "./protocol.js";
import type { HistoryListOptions, HistoryReadOptions, HistoryPage, HistoryMetadata, RoutePage, PublicWorkerDetail } from "./protocol.js";
import type { ClientLease, ResumeLease } from "./protocol.js";
import type { PublicConversationPage, PublicWorkerResult, Availability } from "./protocol.js";
export type { PublicConversationPage, PublicWorkerResult, PublicRecord, PublicFragment, PublicBinding, PublicGap, PublicContent } from "./protocol.js";
export type { ClientLease, ResumeLease, PublicLaunch, PublicWorkerDetail } from "./protocol.js";
export type { WorkerReadReference, WorkerReadCursor, WorkerDetail, WorkerRequests, WorkerActivity, WorkerResult } from "./protocol.js";
export type { HistoryListOptions, HistoryReadOptions, HistoryPage, HistoryMetadata, HistoryFragment, RoutePage } from "./protocol.js";
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
  /** Private reconnect credential. Never put in model context. */
  readonly lease: Readonly<ClientLease>;
  /** Carry the first page revision on continuation; stale_cursor requires a fresh first page. */
  list(after?: string, revision?: number): Promise<WorkerList>;
  subscribe(listener: (hint: ChangeHint) => void): Promise<BridgeSubscription>;
  detail(id: string, cursor?: WorkerReadCursor): Promise<WorkerDetail>;
  publicDetail(id: string, cursor?: WorkerReadCursor): Promise<PublicWorkerDetail | { state: "unavailable"; reason: "unsupported_kind" }>;
  publicActivity(ref: WorkerReadReference, cursor?: string, recovery?: boolean): Promise<PublicConversationPage | Availability>;
  publicResult(ref: WorkerReadReference, cursor?: WorkerReadCursor): Promise<PublicWorkerResult | Availability>;
  requests(id: string, cursor?: WorkerReadCursor): Promise<WorkerRequests>;
  activity(ref: WorkerReadReference, cursor?: WorkerReadCursor): Promise<WorkerActivity>;
  result(ref: WorkerReadReference, cursor?: WorkerReadCursor): Promise<WorkerResult>;
  historyList(options?: HistoryListOptions): Promise<HistoryPage<HistoryMetadata>>;
  historyRead(options: HistoryReadOptions): Promise<HistoryPage>;
  routeRead(id: string, offset?: number): Promise<RoutePage>;
  disconnect(): Promise<void>;
}
/** Explicit host entry: import from "pitako/extensions/agent/bridge/client.js". */
export function connect(descriptorPath: string, options: { expectedSessionId: string; resume?: ResumeLease }): Promise<BridgeClient>;
