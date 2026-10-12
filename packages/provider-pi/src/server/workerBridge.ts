// @effect-diagnostics nodeBuiltinImport:off globalTimers:off -- Native bridge boundary owns synchronous inode checks and a single bounded fs-watch readiness deadline; no polling.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as Effect from "effect/Effect";
import * as Clock from "effect/Clock";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import type {
  ProviderWorkersObservation,
  ProviderWorkersPort,
} from "@t3tools/provider-core/server/ProviderWorkers";
import {
  connect,
  BridgeError,
  type BridgeClient,
  type BridgeSubscription,
} from "./pitako/bridge/client.js";
import type { ResumeLease, WorkerList } from "./pitako/bridge/protocol.js";
import { errors as nativeErrors } from "./pitako/bridge/protocol.js";
import type { PiSettings } from "../settings.ts";

/** Native credentials stay inside this server-only port, never provider events or projections. */
export interface PiWorkersPort extends ProviderWorkersPort {
  readonly driver: "pi";
  readonly pending: boolean;
  readonly ceilings: Readonly<{ history: boolean; route: boolean; control: boolean }>;
  readonly read: <A>(use: (client: BridgeClient) => Promise<A>) => Effect.Effect<A, Error>;
  readonly openView: (
    resume?: ResumeLease,
  ) => Effect.Effect<BridgeClient, Error, import("effect/Scope").Scope>;
  /** Join the old socket and release its reservation before explicit same-lease status recovery. */
  readonly disconnectView: (client: BridgeClient) => Effect.Effect<void>;
}

export function isPiWorkersPort(port: ProviderWorkersPort): port is PiWorkersPort {
  return port.driver === "pi" && "read" in port && "openView" in port;
}

export class WorkerInspectionError extends Schema.TaggedError<WorkerInspectionError>()(
  "WorkerInspectionError",
  { code: Schema.String },
) {}
const errorOf = (cause: unknown) =>
  new WorkerInspectionError({
    code: cause instanceof BridgeError ? cause.code : "inspection_failed",
  });

export type PiWorkerFailure =
  | { readonly kind: "coded"; readonly code: keyof typeof nativeErrors }
  | { readonly kind: "transport"; readonly code: "disconnected" | "invalid_reply" }
  | { readonly kind: "unclassified" };
const nativeCode = (code: string): code is keyof typeof nativeErrors => Object.hasOwn(nativeErrors, code);

/** Only acquisition errors pass through the port's locally minted inspection carrier. */
export function classifyPiWorkerFailure(
  cause: unknown, phase: "request" | "acquisition" = "request",
): PiWorkerFailure {
  if (!(cause instanceof BridgeError)) {
    // @effect-diagnostics-next-line instanceOfSchema:off -- Carrier identity is provenance; a structural schema check would admit forged objects.
    if (phase !== "acquisition" || !(cause instanceof WorkerInspectionError)) return { kind: "unclassified" };
  }
  const code = cause.code;
  if (nativeCode(code)) return { kind: "coded", code };
  if (code === "disconnected" || code === "invalid_reply") return { kind: "transport", code };
  return { kind: "unclassified" };
}

export class PiWorkerBridge implements PiWorkersPort {
  readonly driver = "pi";
  readonly runtimeMode: PiWorkersPort["runtimeMode"];
  readonly ceilings: PiWorkersPort["ceilings"];
  readonly directory: string | undefined;
  readonly launchArgs: ReadonlyArray<string>;
  observation: ProviderWorkersObservation;
  pending: boolean;
  private readonly directoryIdentity: NodeFS.Stats | undefined;
  private client: BridgeClient | undefined;
  private subscription: BridgeSubscription | undefined;
  private generation = 0;
  private closed = false;
  private dirty = false;
  private refreshing: Promise<void> | undefined;
  private readonly work = new Set<Promise<unknown>>();
  private readonly views = new Set<BridgeClient>();
  private openingViews = 0;
  private readonly unsettled = new Set<string>();
  private settlementOverflow = false;
  private readonly listeners = new Set<(state: ProviderWorkersObservation) => void>();
  private readinessCancel: (() => void) | undefined;
  private readonly launchFailed: boolean;

  constructor(
    settings: PiSettings,
    runtimeMode: PiWorkersPort["runtimeMode"],
    platform: string,
    launchFailed = false,
  ) {
    this.launchFailed = launchFailed;
    this.runtimeMode = runtimeMode;
    this.ceilings = {
      history: settings.workerHistory === true,
      route: settings.workerRoute === true,
      control: settings.workerControl === true,
    };
    this.pending = settings.workerBridge === true;
    this.observation = launchFailed
      ? { state: "error", code: "bridge_launch_failed" }
      : !settings.workerBridge
      ? { state: "disabled" }
      : platform === "win32"
        ? { state: "unsupported" }
        : { state: "connecting" };
    let directory: string | undefined;
    if (this.observation.state === "connecting") {
      try {
        // realpath also canonicalizes macOS /var. Never use the host's potentially
        // long cache/worktree directory for Unix sockets.
        directory = NodeFS.mkdtempSync(
          NodePath.join(NodeFS.realpathSync(NodeOS.tmpdir()), "t3-pw-"),
        );
        NodeFS.chmodSync(directory, 0o700);
        if (Buffer.byteLength(NodePath.join(directory, "b-000000000000.sock")) > 103) {
          NodeFS.rmdirSync(directory);
          directory = undefined;
          this.observation = { state: "error", code: "socket_path_limit" };
        }
      } catch {
        this.observation = { state: "error", code: "private_directory_failed" };
      }
    }
    this.directory = directory;
    this.directoryIdentity = directory === undefined ? undefined : NodeFS.lstatSync(directory);
    const grants = [
      ...(settings.workerHistory ? ["history"] : []),
      ...(settings.workerRoute ? ["route"] : []),
      ...(settings.workerControl ? ["control"] : []),
    ];
    this.launchArgs =
      directory === undefined
        ? []
        : [
            `--pitako-agent-bridge=${directory}`,
            ...(grants.length === 0 ? [] : [`--pitako-agent-bridge-grants=${grants.join(",")}`]),
          ];
  }

  readonly observations = Stream.callback<ProviderWorkersObservation>(
    (queue) =>
      Effect.acquireRelease(
        Effect.sync(() => {
          const listener = (state: ProviderWorkersObservation) => {
            Queue.offerUnsafe(queue, state);
          };
          this.listeners.add(listener);
          listener(this.observation);
          return listener;
        }),
        (listener) =>
          Effect.sync(() => {
            this.listeners.delete(listener);
          }),
      ),
    { bufferSize: 1, strategy: "sliding" },
  );

  private publish(state: ProviderWorkersObservation) {
    if (this.launchFailed) return;
    this.observation = state;
    for (const listener of this.listeners) listener(state);
  }

  private failed(cause: unknown) {
    this.pending = true;
    const previous = this.observation;
    const lastObservation =
      previous.state === "last_observation"
        ? { owner: previous.owner, observedAt: previous.observedAt, revision: previous.revision }
        : previous.state === "error"
          ? previous.lastObservation
          : undefined;
    this.publish({
      state: "error",
      code: cause instanceof BridgeError ? cause.code : "inspection_failed",
      ...(lastObservation === undefined ? {} : { lastObservation }),
    });
  }

  private track<A>(promise: Promise<A>): Promise<A> {
    this.work.add(promise);
    void promise.then(
      () => this.work.delete(promise),
      () => this.work.delete(promise),
    );
    return promise;
  }

  private async descriptorReady(): Promise<void> {
    const directory = this.directory;
    if (directory === undefined) throw new Error("Bridge unavailable");
    await new Promise<void>((resolve, reject) => {
      const watcher = NodeFS.watch(directory, () => check());
      const timer = setTimeout(
        () =>
          finish(
            new BridgeError("bridge_not_ready", "Compatible Pitako bridge did not become ready"),
          ),
        3_000,
      );
      const finish = (error?: Error) => {
        clearTimeout(timer);
        watcher.close();
        this.readinessCancel = undefined;
        if (error === undefined) resolve();
        else reject(error);
      };
      const check = () => {
        if (NodeFS.existsSync(NodePath.join(directory, "descriptor.json"))) finish();
      };
      this.readinessCancel = () => finish(new Error("Bridge owner invalidated"));
      watcher.on("error", () => finish(new Error("Bridge readiness failed")));
      check();
    });
  }

  /** sessionId comes only from the correlated stdio get_state, never the descriptor/sessionFile. */
  readonly bind = (sessionId: string | undefined) =>
    Effect.tryPromise({
      try: async () => {
        if (this.launchFailed) return;
        // Join the previous owner's reads before tracking the new bind itself.
        if (this.client !== undefined && this.client.owner.coordinatorSessionId !== sessionId)
          await this.invalidateNative();
        return this.track(this.bindNative(sessionId));
      },
      catch: errorOf,
    }).pipe(Effect.ignore);

  private async bindNative(sessionId: string | undefined) {
    if (this.directory === undefined || this.closed) return;
    if (this.client !== undefined && this.client.owner.coordinatorSessionId === sessionId) return;
    const generation = this.generation;
    try {
      if (!sessionId)
        throw new BridgeError("missing_session_id", "Pi returned no native session ID");
      await this.descriptorReady();
      const client = await connect(NodePath.join(this.directory, "descriptor.json"), {
        expectedSessionId: sessionId,
      });
      if (generation !== this.generation || this.closed) {
        await client.disconnect();
        return;
      }
      this.client = client;
      this.subscription = await client.subscribe(() => {
        void this.refreshNative().catch(() => {});
      });
      await this.refreshNative();
    } catch (cause) {
      if (generation === this.generation && !this.closed) this.failed(cause);
    }
  }

  /** Coalescing retains a dirty bit while a read is in flight, including gap hints. */
  private refreshNative(): Promise<void> {
    this.dirty = true;
    if (this.refreshing !== undefined) return this.refreshing;
    const generation = this.generation;
    const run = async () => {
      let staleRestarts = 0;
      while (this.dirty && !this.closed && generation === this.generation) {
        this.dirty = false;
        const unsettled = new Set(this.unsettled);
        let settlementOverflow = this.settlementOverflow;
        try {
          const client = this.client;
          if (!client) throw new BridgeError("bridge_unavailable", "Worker inspection unavailable");
          let first: WorkerList | undefined;
          let cursor: string | undefined;
          let pending = false;
          let pages = 0;
          do {
            const page = await client.list(cursor, first?.revision);
            first ??= page;
            for (const worker of page.items) {
              if (worker.source === "agent_run") continue;
              if (worker.status === "created" || worker.status === "running") {
                if (unsettled.size < 3_200) unsettled.add(worker.id);
                else settlementOverflow = true;
                pending = true;
                continue;
              }
              // Terminal/cancelled is not physical settlement. Native activity
              // becomes inactive only after close and terminal capture joined.
              const detail = await client.detail(worker.id);
              const activity = await client.activity(detail.ref);
              if (activity.state !== "ready" || activity.page.active) {
                if (unsettled.size < 3_200) unsettled.add(worker.id);
                else settlementOverflow = true;
                pending = true;
              } else unsettled.delete(worker.id);
            }
            cursor = page.cursor ?? undefined;
            if (++pages >= 64 && cursor !== undefined) {
              pending = true;
              break;
            }
          } while (cursor !== undefined);
          if (generation !== this.generation || this.closed) return;
          this.unsettled.clear();
          for (const id of unsettled) this.unsettled.add(id);
          this.settlementOverflow = settlementOverflow;
          this.pending = pending || this.unsettled.size > 0 || this.settlementOverflow;
          this.publish({
            state: "last_observation",
            owner: client.owner,
            revision: first.revision,
            observedAt: Clock.Clock.defaultValue().currentTimeMillisUnsafe(),
          });
        } catch (cause) {
          if (cause instanceof BridgeError && cause.code === "stale_cursor") {
            // Partial traversal never contributes a settlement decision.
            this.pending = true;
            this.dirty = true;
            if (++staleRestarts < 3) continue;
            // Do not spin forever under continuous native changes.
          }
          if (generation === this.generation && !this.closed) this.failed(cause);
          throw cause;
        }
      }
    };
    this.refreshing = this.track(run()).finally(() => {
      this.refreshing = undefined;
    });
    return this.refreshing;
  }

  readonly refresh = Effect.suspend(() =>
    this.launchFailed
      ? Effect.fail(new WorkerInspectionError({ code: "bridge_launch_failed" }))
      : this.observation.state === "disabled" || this.observation.state === "unsupported"
      ? Effect.void
      : Effect.tryPromise({ try: () => this.refreshNative(), catch: errorOf }),
  );

  readonly read = <A>(use: (client: BridgeClient) => Promise<A>) =>
    Effect.tryPromise({
      try: () =>
        this.track(
          (async () => {
            if (this.launchFailed)
              throw new BridgeError("bridge_launch_failed", "Worker bridge launch failed");
            const client = this.client;
            const generation = this.generation;
            if (!client || this.closed)
              throw new BridgeError("bridge_unavailable", "Worker inspection unavailable");
            try {
              const result = await use(client);
              if (generation !== this.generation || this.closed)
                throw new BridgeError("stale_owner", "Worker owner changed");
              return result;
            } catch (cause) {
              if (generation === this.generation && !this.closed) this.failed(cause);
              throw cause;
            }
          })(),
        ),
      catch: errorOf,
    });

  readonly openView = (resume?: ResumeLease) =>
    Effect.acquireRelease(
      Effect.tryPromise({
        try: () =>
          this.track(
            (async () => {
              if (this.launchFailed)
                throw new BridgeError("bridge_launch_failed", "Worker bridge launch failed");
              const ordinary = this.client;
              const generation = this.generation;
              if (!ordinary || !this.directory || this.closed)
                throw new BridgeError("bridge_unavailable", "Worker inspection unavailable");
              if (this.views.size + this.openingViews >= 3)
                throw new BridgeError("busy", "Private worker view limit reached");
              this.openingViews++;
              try {
                const client = await connect(NodePath.join(this.directory, "descriptor.json"), {
                  expectedSessionId: ordinary.owner.coordinatorSessionId,
                  ...(resume === undefined ? {} : { resume }),
                });
                if (
                  generation !== this.generation ||
                  this.closed ||
                  client.owner.epoch !== ordinary.owner.epoch ||
                  client.owner.processId !== ordinary.owner.processId
                ) {
                  await client.disconnect();
                  throw new BridgeError("stale_owner", "Worker owner changed");
                }
                this.views.add(client);
                return client;
              } finally {
                this.openingViews--;
              }
            })(),
          ),
        catch: errorOf,
      }),
      (client) => this.disconnectView(client),
    );

  readonly disconnectView = (client: BridgeClient) =>
    Effect.promise(async () => {
      await client.disconnect();
      this.views.delete(client);
    });

  /** Silences callbacks first, then joins clients and all known admitted reads. */
  readonly invalidate = Effect.promise(() => this.invalidateNative());

  private async invalidateNative() {
    if (this.observation.state === "disabled") return;
    this.generation++;
    this.pending = true;
    this.publish({ state: "stale_owner" });
    this.readinessCancel?.();
    const subscription = this.subscription;
    this.subscription = undefined;
    // Disposal may reject as soon as disconnect tears down an outstanding unsubscribe.
    // Handle that rejection now, while still joining its settlement below.
    const disposal = subscription?.dispose().catch(() => undefined);
    const clients = [...this.views, ...(this.client === undefined ? [] : [this.client])];
    this.client = undefined;
    this.views.clear();
    await Promise.allSettled(clients.map((client) => client.disconnect()));
    await Promise.allSettled([...(disposal === undefined ? [] : [disposal]), ...this.work]);
    this.unsettled.clear();
    this.settlementOverflow = false;
  }

  readonly close = Effect.sync(() => {
    this.closed = true;
  }).pipe(Effect.andThen(this.invalidate));

  /** Only an orderly process exit and unchanged, now-empty owned directory permit removal. */
  cleanupDirectory(orderlyExit: boolean) {
    if (!orderlyExit || !this.directory || !this.directoryIdentity) return;
    try {
      const current = NodeFS.lstatSync(this.directory);
      if (
        current.dev === this.directoryIdentity.dev &&
        current.ino === this.directoryIdentity.ino &&
        !current.isSymbolicLink() &&
        NodeFS.readdirSync(this.directory).length === 0
      )
        NodeFS.rmdirSync(this.directory);
    } catch {
      /* Retain uncertain resources for diagnosis; never remove a replacement owner. */
    }
  }
}
