import type * as Effect from "effect/Effect";
import type * as Stream from "effect/Stream";

/** Native worker observations are independent of provider events and root-run ancestry. */
export interface ProviderWorkerOwner {
  readonly processId: string;
  readonly coordinatorSessionId: string;
  readonly epoch: string;
}

export type ProviderWorkersObservation =
  | { readonly state: "disabled" | "unsupported" | "connecting" | "stale_owner" }
  | {
      readonly state: "last_observation";
      readonly owner: ProviderWorkerOwner;
      readonly observedAt: number;
      readonly revision: number;
    }
  | {
      readonly state: "error";
      readonly code: string;
      readonly lastObservation?: {
        readonly owner: ProviderWorkerOwner;
        readonly observedAt: number;
        readonly revision: number;
      };
    };

/** Providers refine this port with their native typed pages and scoped private views. */
export interface ProviderWorkersPort {
  readonly driver: string;
  readonly runtimeMode: "approval-required" | "auto-accept-edits" | "full-access";
  readonly observation: ProviderWorkersObservation;
  readonly observations: Stream.Stream<ProviderWorkersObservation>;
  readonly refresh: Effect.Effect<void, Error>;
}
