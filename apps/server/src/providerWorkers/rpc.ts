import { ProviderWorkersRpcGroup, WS_METHODS } from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import type { AuthenticatedSession } from "../auth/EnvironmentAuth.ts";
import { ProviderWorkers } from "./ProviderWorkers.ts";

/** Called once for each authenticated WebSocket upgrade, never keyed by a client-supplied id. */
export const makeHandlers = (session: AuthenticatedSession) => Effect.gen(function* () {
  const workers = yield* ProviderWorkers;
  const crypto = yield* Crypto.Crypto;
  const viewer = { session, connectionId: yield* crypto.randomUUIDv4.pipe(Effect.orDie) };
  return ProviderWorkersRpcGroup.of({
    [WS_METHODS.workerObserve]: (input) => workers.observe(viewer, input),
    [WS_METHODS.workerList]: (input) => workers.list(viewer, input),
    [WS_METHODS.workerDetail]: (input) => workers.detail(viewer, input),
    [WS_METHODS.workerRequests]: (input) => workers.requests(viewer, input),
    [WS_METHODS.workerActivity]: (input) => workers.activity(viewer, input),
    [WS_METHODS.workerResult]: (input) => workers.result(viewer, input),
    [WS_METHODS.workerHistoryList]: (input) => workers.historyList(viewer, input),
    [WS_METHODS.workerOpenView]: (input) => workers.openView(viewer, input),
    [WS_METHODS.workerCloseView]: (input) => workers.closeView(viewer, input),
    [WS_METHODS.workerAcknowledge]: (input) => workers.acknowledge(viewer, input),
    [WS_METHODS.workerHistoryRead]: (input) => workers.historyRead(viewer, input),
    [WS_METHODS.workerRouteRead]: (input) => workers.routeRead(viewer, input),
    [WS_METHODS.workerControlDetail]: (input) => workers.controlDetail(viewer, input),
    [WS_METHODS.workerActivate]: (input) => workers.activate(viewer, input),
    [WS_METHODS.workerAction]: (input) => workers.action(viewer, input),
    [WS_METHODS.workerReceipt]: (input) => workers.receipt(viewer, input),
  });
});
