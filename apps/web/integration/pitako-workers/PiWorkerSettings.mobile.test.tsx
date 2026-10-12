import * as C from "@t3tools/contracts";
import { RegistryContext, useAtomValue } from "@effect/atom-react";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as RpcTest from "effect/rpc/RpcTest";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { Atom, AtomRegistry, AsyncResult } from "effect/reactivity";
import { vi } from "vite-plus/test";
import { act, create, type ReactTestRenderer } from "../../src/test/componentRenderer";
import { createServerEnvironmentAtoms } from "@t3tools/client-runtime/state/server";
import { EnvironmentRegistry } from "../../../../packages/client-runtime/src/connection/registry";
import type { ConnectionCatalogEntry } from "../../../../packages/client-runtime/src/connection/catalog";
import type { NetworkStatus } from "../../../../packages/client-runtime/src/connection/model";
import { EnvironmentSupervisor } from "../../../../packages/client-runtime/src/connection/supervisor";
import { PrimaryConnectionTarget } from "../../../../packages/client-runtime/src/connection/model";
import { EnvironmentCacheStore } from "../../../../packages/client-runtime/src/platform/persistence";
import type { RpcSession } from "../../../../packages/client-runtime/src/rpc/session";
import { ServerSettingsService, layerTest } from "../../../server/src/serverSettings";
import * as RpcAuthorization from "../../../server/src/auth/RpcAuthorization";
import { piClient } from "@t3tools/provider-pi/client";
import { ProviderSettingsForm } from "../../src/components/settings/ProviderSettingsForm";

const state = vi.hoisted(() => ({
  server: null as ReturnType<typeof createServerEnvironmentAtoms> | null,
}));
vi.mock(
  "../../../mobile/node_modules/react",
  async () => (await import("../../src/test/componentRenderer")).React,
);
vi.mock(
  "../../../mobile/node_modules/react/compiler-runtime",
  async () => (await import("../../src/test/componentRenderer")).ReactCompilerRuntime,
);
vi.mock(
  "../../../mobile/node_modules/@effect/atom-react",
  async () => (await import("../../src/test/componentRenderer")).AtomReact,
);
vi.mock("../../../mobile/node_modules/react-native", () => ({ View: "view" }));
vi.mock("../../../mobile/src/components/AppText", () => ({ AppText: "text" }));
vi.mock("../../../mobile/src/state/server", () => ({
  get serverEnvironment() {
    return state.server;
  },
}));
vi.mock("../../../mobile/src/features/settings/components/SettingsSwitchRow", () => ({
  SettingsSwitchRow: "switch",
}));
vi.mock("../../src/components/ui/switch", () => ({ Switch: "web-switch" }));
const sessions = Atom.family((_id: C.EnvironmentId) =>
  Atom.make(
    AsyncResult.success<C.AuthSessionState>({
      authenticated: true,
      scopes: [],
      auth: {
        policy: "remote-reachable",
        bootstrapMethods: [],
        sessionMethods: [],
        sessionCookieName: "fixture",
      },
    }),
  ).pipe(Atom.keepAlive),
);
vi.mock("@t3tools/client-runtime/state/session", () => ({
  createEnvironmentSessionAtoms: () => ({
    sessionStateAtom: (environmentId: C.EnvironmentId) => sessions(environmentId),
  }),
}));
import { PiWorkerSettings } from "../../../mobile/src/features/settings/PiWorkerSettings";

describe("Pi process opt-in settings", () => {
  it.effect(
    "requires destination provider administration, saves all four ceilings, and retains settings on denial",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const environmentId = C.EnvironmentId.make("settings-env");
          const instanceId = C.ProviderInstanceId.make("pi");
          const service = yield* ServerSettingsService.pipe(
            Effect.provide(
              layerTest({
                providerInstances: {
                  [instanceId]: { driver: C.ProviderDriverKind.make("pi"), config: {} },
                },
              }),
            ),
          );
          const settings = Atom.make(yield* service.getSettings).pipe(Atom.keepAlive);
          let writes = 0;
          const group = C.WsRpcGroup.omit(
            ...Array.from(C.WsRpcGroup.requests.values(), (rpc) => rpc._tag).filter(
              (tag) => tag !== C.WS_METHODS.serverUpdateSettings,
            ),
          );
          const client = yield* RpcTest.makeClient(group).pipe(
            Effect.provide(
              Layer.mergeAll(
                group.toLayerHandler(
                  C.WS_METHODS.serverUpdateSettings,
                  ({ patch, providerInstanceMutation }) =>
                    Effect.gen(function* () {
                      writes++;
                      expect(patch).toEqual({});
                      expect(providerInstanceMutation?.operation).toBe("upsert");
                      const next = yield* service.updateProviderInstance(
                        providerInstanceMutation!,
                        patch,
                      );
                      registry.set(settings, next);
                      return next;
                    }),
                ),
                RpcAuthorization.layer([C.AuthProvidersManageScope]),
              ),
            ),
          );
          const session = { client } as unknown as RpcSession;
          const sessionRef = yield* SubscriptionRef.make(Option.some(session));
          const supervisor = {
            session: sessionRef,
            target: new PrimaryConnectionTarget({
              environmentId,
              label: "fixture",
              httpBaseUrl: "http://fixture",
              wsBaseUrl: "ws://fixture",
            }),
          } as unknown as EnvironmentSupervisor["Service"];
          const runtime = Atom.runtime(
            Layer.mergeAll(
              Layer.mock(EnvironmentRegistry)({
                entries: yield* SubscriptionRef.make<
                  ReadonlyMap<C.EnvironmentId, ConnectionCatalogEntry>
                >(new Map()),
                networkStatus: yield* SubscriptionRef.make<NetworkStatus>("online"),
                run: (_id, effect) =>
                  Effect.provideService(effect, EnvironmentSupervisor, supervisor),
              }),
              Layer.mock(EnvironmentCacheStore)({}),
            ),
          );
          const server = createServerEnvironmentAtoms(runtime, {
            initialConfigValueAtom: () => Atom.make(null),
          });
          state.server = { ...server, settingsValueAtom: () => settings };
          const registry = AtomRegistry.make();
          yield* Effect.addFinalizer(() => Effect.sync(() => registry.dispose()));
          vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
          yield* Effect.addFinalizer(() => Effect.sync(() => vi.unstubAllGlobals()));
          let renderer: ReactTestRenderer;
          yield* Effect.addFinalizer(() =>
            Effect.promise(async () => {
              if (renderer) await act(async () => renderer.unmount());
            }),
          );
          yield* Effect.promise(async () => {
            await act(async () => {
              renderer = create(
                <RegistryContext.Provider value={registry}>
                  <PiWorkerSettings environmentId={environmentId} instanceId={instanceId} />
                </RegistryContext.Provider>,
              );
            });
            expect(writes).toBe(0);
            const row = (label: string) =>
              renderer.root.findAllByType("switch").find((node) => node.props.label === label)!;
            expect(row("Pitako worker bridge").props.disabled).toBe(true);
            const before = registry.get(settings);
            const denied = await server.mutateProviderInstance.run(registry, {
              environmentId,
              input: {
                patch: {},
                providerInstanceMutation: {
                  operation: "upsert",
                  instanceId,
                  instance: {
                    driver: C.ProviderDriverKind.make("pi"),
                    config: { workerBridge: true },
                  },
                },
              },
            });
            expect(AsyncResult.isFailure(denied)).toBe(true);
            expect(writes).toBe(0);
            expect(registry.get(settings)).toBe(before);
            await act(async () =>
              registry.set(
                sessions(environmentId),
                AsyncResult.success<C.AuthSessionState>({
                  authenticated: true,
                  scopes: [C.AuthProvidersManageScope],
                  auth: {
                    policy: "remote-reachable",
                    bootstrapMethods: [],
                    sessionMethods: [],
                    sessionCookieName: "fixture",
                  },
                }),
              ),
            );
            for (const label of [
              "Pitako worker bridge",
              "Allow worker history acknowledgement",
              "Allow worker routing acknowledgement",
              "Allow worker control activation",
            ] as const) {
              expect(row(label).props.value).toBe(false);
              expect(row(label).props.disabled).toBe(false);
              const expectedWrites = writes + 1;
              const saved = new Promise<void>((resolve) => {
                let off = () => {};
                off = registry.subscribe(settings, () => {
                  if (writes === expectedWrites) {
                    resolve();
                    queueMicrotask(() => off());
                  }
                });
              });
              await act(async () => {
                row(label).props.onValueChange(true);
                await saved;
              });
            }
            expect(writes).toBe(4);
            expect(registry.get(settings).providerInstances[instanceId]?.config).toEqual({
              workerBridge: true,
              workerHistory: true,
              workerRoute: true,
              workerControl: true,
            });
            expect(JSON.stringify(renderer.toJSON())).toContain("next Pi process");
            expect(JSON.stringify(renderer.toJSON())).toContain(
              "current process was not restarted",
            );
            await act(async () => renderer.unmount());
            // The web/desktop card reuses this generated form, not another set of worker controls.
            // Bind its output to the same protected command and settings service as mobile.
            function WebForm() {
              const value = useAtomValue(settings).providerInstances[instanceId]?.config;
              return (
                <ProviderSettingsForm
                  definition={piClient}
                  value={value}
                  idPrefix="pi-worker-fixture"
                  variant="settings"
                  onChange={(config) => {
                    void server.mutateProviderInstance.run(registry, {
                      environmentId,
                      input: {
                        patch: {},
                        providerInstanceMutation: {
                          operation: "upsert",
                          instanceId,
                          instance: { driver: C.ProviderDriverKind.make("pi"), config },
                        },
                      },
                    });
                  }}
                />
              );
            }
            await act(async () => {
              renderer = create(
                <RegistryContext.Provider value={registry}>
                  <WebForm />
                </RegistryContext.Provider>,
              );
            });
            for (const [field, label] of [
              ["workerBridge", "Pitako worker bridge"],
              ["workerHistory", "Allow worker history acknowledgement"],
              ["workerRoute", "Allow worker routing acknowledgement"],
              ["workerControl", "Allow worker control activation"],
            ] as const) {
              const checkbox = renderer.root
                .findAllByType("web-switch")
                .find((node) => node.props["aria-label"] === label)!;
              expect(checkbox.props.checked).toBe(true);
              const expectedWrites = writes + 1;
              const saved = new Promise<void>((resolve) => {
                let off = () => {};
                off = registry.subscribe(settings, () => {
                  if (writes === expectedWrites) {
                    resolve();
                    queueMicrotask(() => off());
                  }
                });
              });
              await act(async () => {
                checkbox.props.onCheckedChange(false);
                await saved;
              });
              expect(
                registry.get(settings).providerInstances[instanceId]?.config?.[field] ?? false,
              ).toBe(false);
            }
            expect(writes).toBe(8);
            const finalConfig = registry.get(settings).providerInstances[instanceId]?.config;
            for (const field of ["workerBridge", "workerHistory", "workerRoute", "workerControl"]) {
              expect(finalConfig?.[field] ?? false).toBe(false);
            }
          });
          const persisted = yield* service.getSettings;
          for (const field of ["workerBridge", "workerHistory", "workerRoute", "workerControl"]) {
            expect(persisted.providerInstances[instanceId]?.config?.[field] ?? false).toBe(false);
          }
        }),
      ),
  );
});
