import type { RunId } from "@t3tools/contracts";
import type { ThreadWorkGroupScrollPosition } from "./thread-feed-live-follow";

export interface ThreadReadingPosition {
  readonly rowId: string;
  readonly offsetWithinRow: number;
  readonly scrollOffset: number;
  readonly follow: boolean;
  readonly disclosures: {
    readonly expandedWorkGroups: Record<string, boolean>;
    readonly expandedWorkRows: Record<string, boolean>;
    readonly expandedTurnIds: ReadonlySet<RunId>;
    readonly workGroupScrollPositions: Map<string, ThreadWorkGroupScrollPosition>;
  };
}

// Session-only, environment/thread-scoped. Composer drafts remain in their existing store.
const positions = new Map<string, ThreadReadingPosition>();

export function readThreadReadingPosition(threadKey: string) {
  return positions.get(threadKey);
}

export function rememberThreadReadingPosition(threadKey: string, position: ThreadReadingPosition) {
  positions.delete(threadKey);
  positions.set(threadKey, position);
  if (positions.size > 100) {
    const oldest = positions.keys().next().value;
    if (oldest !== undefined) positions.delete(oldest);
  }
}
