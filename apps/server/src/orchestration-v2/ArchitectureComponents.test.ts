import { expect, vi } from "vite-plus/test";
import { it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  AuthOrchestrationReadScope,
  EnvironmentId,
  ORCHESTRATION_V2_WS_METHODS,
  OrchestrationV2GetThreadProjectionError,
  PiArchitectureReadInput,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ThreadId,
  WsRpcGroup,
  type PiArchitectureRecord,
  type PiArchitectureSummary,
  type PiArchitectureReadResult,
} from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Context from "effect/Context";
import * as Cause from "effect/Cause";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as SqlClient from "effect/sql/SqlClient";
import * as RpcClient from "effect/rpc/RpcClient";
import type * as RpcGroup from "effect/rpc/RpcGroup";
import type * as RpcMessage from "effect/rpc/RpcMessage";
import * as RpcTest from "effect/rpc/RpcTest";
import type * as Exit from "effect/Exit";
import { AsyncResult, Atom, AtomRegistry } from "effect/reactivity";
import {
  AVAILABLE_CONNECTION_STATE,
  PrimaryConnectionTarget,
  type PreparedConnection,
  type SupervisorConnectionState,
  type NetworkStatus,
} from "../../../../packages/client-runtime/src/connection/model.ts";
import type { ConnectionCatalogEntry } from "../../../../packages/client-runtime/src/connection/catalog.ts";
import {
  tablePresentationAtom,
  type TableTarget,
} from "../../../../packages/client-runtime/src/state/piArchitecture/model.ts";
import {
  nativeSubagentFixture,
  nativeWorkerFixture,
} from "../../../../packages/client-runtime/src/testing/nativeWorker.ts";
import * as ArchitectureArchive from "./ArchitectureArchive.ts";
import * as ArchitectureQuery from "./ArchitectureQuery.ts";
import * as RpcAuthorization from "../auth/RpcAuthorization.ts";

// Narrow runtime import APIs keep the server's NodeNext context separate from
// client Bundler/DOM/native declarations. These are real modules, not doubles.
type Component<P> = (props: P) => unknown;
interface Instance {
  type: unknown;
  props: Record<string, unknown>;
  children: Array<Instance | string>;
  findAll(predicate: (node: Instance) => boolean): Instance[];
  findAllByType(type: string): Instance[];
  findByType(type: string): Instance;
}
interface ReactTestRenderer {
  root: Instance;
  update(element: unknown): void;
  unmount(): void;
}
interface RendererApi {
  React: { createElement(type: unknown, props: unknown, ...children: unknown[]): unknown };
  EffectAtomReact: { RegistryContext: { Provider: unknown } };
  act(run: () => Promise<unknown>): Promise<void>;
  create(
    element: unknown,
    options?: {
      createNodeMock: (element: { type: unknown; props: Record<string, unknown> }) => unknown;
    },
  ): ReactTestRenderer;
}
const { React, EffectAtomReact, act, create } = await vi.importActual<RendererApi>(
  "../../../web/test/component-renderer.ts",
);
type ReadTarget = { environmentId: EnvironmentId; input: PiArchitectureReadInput };
type QueryError = { readonly _tag: string };
interface PiArchitectureAtoms {
  query(
    target: ReadTarget,
  ): Atom.Atom<AsyncResult.AsyncResult<PiArchitectureReadResult, QueryError>>;
  read: {
    permissionAtom(id: EnvironmentId, input?: PiArchitectureReadInput): Atom.Atom<boolean>;
    run(
      registry: AtomRegistry.AtomRegistry,
      target: ReadTarget,
    ): Promise<Exit.Exit<PiArchitectureReadResult, QueryError>>;
  };
}
interface Session {
  client: RpcClient.RpcClient<RpcGroup.Rpcs<typeof WsRpcGroup>>;
  ready: Effect.Effect<void>;
  probe: Effect.Effect<void>;
  closed: Effect.Effect<void>;
  initialConfig: Effect.Effect<never>;
  subscribeServerConfig(): Stream.Stream<never>;
}
interface Supervisor {
  target: PrimaryConnectionTarget;
  state: SubscriptionRef.SubscriptionRef<SupervisorConnectionState>;
  session: SubscriptionRef.SubscriptionRef<Option.Option<Session>>;
  prepared: SubscriptionRef.SubscriptionRef<Option.Option<PreparedConnection>>;
  connect: Effect.Effect<void>;
  disconnect: Effect.Effect<void>;
  retryNow: Effect.Effect<void>;
}
interface Registry {
  run<A, E, R>(
    id: EnvironmentId,
    effect: Effect.Effect<A, E, R>,
  ): Effect.Effect<A, E, Exclude<R, Supervisor>>;
  followStream<A, E, R>(
    id: EnvironmentId,
    stream: Stream.Stream<A, E, R>,
  ): Stream.Stream<A, E, Exclude<R, Supervisor>>;
  stateChanges(id: EnvironmentId): Stream.Stream<SupervisorConnectionState>;
  entries: SubscriptionRef.SubscriptionRef<ReadonlyMap<EnvironmentId, ConnectionCatalogEntry>>;
  networkStatus: SubscriptionRef.SubscriptionRef<NetworkStatus>;
}
const { EnvironmentRegistry } = await vi.importActual<{
  EnvironmentRegistry: Context.Key<Registry, Registry>;
}>("../../../../packages/client-runtime/src/connection/registry.ts");
const { EnvironmentSupervisor } = await vi.importActual<{
  EnvironmentSupervisor: Context.Key<Supervisor, Supervisor>;
}>("../../../../packages/client-runtime/src/connection/supervisor.ts");
type Runtime = Atom.AtomRuntime<Registry, never>;
const { createPiArchitectureAtoms } = await vi.importActual<{
  createPiArchitectureAtoms(
    runtime: Runtime,
    refresh: () => Atom.Atom<number>,
  ): PiArchitectureAtoms;
}>("../../../../packages/client-runtime/src/state/piArchitecture/queries.ts");
const { createEnvironmentSessionAtoms } = await vi.importActual<{
  createEnvironmentSessionAtoms(runtime: Runtime): {
    sessionStateAtom(
      id: EnvironmentId,
    ): Atom.Writable<
      AsyncResult.AsyncResult<{ authenticated: boolean; scopes: readonly string[] }>
    >;
  };
}>("../../../../packages/client-runtime/src/state/session.ts");

// Only connection, navigation and native host boundaries are substituted. Catalog,
// detail, paged text, presentation and permission atoms are the production versions.
const host = vi.hoisted(() => {
  let atoms: PiArchitectureAtoms;
  let runtime: Runtime;
  let tables: readonly PiArchitectureSummary[] = [];
  let refreshThread: (registry: AtomRegistry.AtomRegistry) => void = () => {};
  return {
    setAtoms(value: PiArchitectureAtoms) {
      atoms = value;
    },
    getAtoms: () => atoms,
    setRuntime(value: typeof runtime) {
      runtime = value;
    },
    getRuntime: () => runtime,
    setTables(value: typeof tables) {
      tables = value;
    },
    getTables: () => tables,
    setRefreshThread(value: typeof refreshThread) {
      refreshThread = value;
    },
    refreshThread(registry: AtomRegistry.AtomRegistry) {
      refreshThread(registry);
    },
    navigate: vi.fn<(input: unknown) => void>(),
    dispatch: vi.fn<(input: unknown) => void>(),
    back: vi.fn(),
  };
});
vi.mock("../../../web/src/routes/-piArchitecture/state.ts", () => ({
  piArchitecture: host.getAtoms(),
}));
vi.mock("../../../mobile/src/connection/runtime.ts", () => ({
  connectionAtomRuntime: host.getRuntime(),
}));
vi.mock("../../../mobile/src/lib/openExternalUrl.ts", () => ({ tryOpenExternalUrl: vi.fn() }));
vi.mock("../../../web/node_modules/@tanstack/react-router/dist/esm/index.dev.js", () => ({
  useNavigate: () => host.navigate,
}));
// Browser palette/media-query application has no host in react-test-renderer.
vi.mock("../../../web/src/hooks/useTheme.ts", async () => ({
  ...(await vi.importActual<Record<string, unknown>>("../../../web/src/hooks/useTheme.ts")),
  useTheme: () => ({ resolvedTheme: "light" }),
}));
vi.mock(
  "../../../mobile/node_modules/react",
  async () => (await vi.importActual<RendererApi>("../../../web/test/component-renderer.ts")).React,
);
vi.mock(
  "../../../mobile/node_modules/@effect/atom-react",
  async () =>
    (await vi.importActual<RendererApi>("../../../web/test/component-renderer.ts")).EffectAtomReact,
);
vi.mock("../../../mobile/node_modules/react-native", () => ({
  Platform: { OS: "android", Version: 35 },
  View: "View",
  Pressable: "Pressable",
  ScrollView: "ScrollView",
}));
vi.mock("../../../mobile/node_modules/@react-navigation/native", () => ({
  useNavigation: () => ({ dispatch: host.dispatch, goBack: host.back }),
  StackActions: {
    push: (name: string, params: unknown) => ({ type: "PUSH", payload: { name, params } }),
    replace: (name: string, params: unknown) => ({ type: "REPLACE", payload: { name, params } }),
  },
}));
vi.mock("../../../mobile/node_modules/expo-haptics", () => ({ selectionAsync: vi.fn() }));
vi.mock("../../../mobile/node_modules/react-native-screens", () => ({
  Screen: "Screen",
  ScreenStack: "ScreenStack",
  ScreenStackHeaderConfig: "Header",
}));
vi.mock("../../../mobile/node_modules/react-native-safe-area-context", () => ({
  useSafeAreaInsets: () => ({ bottom: 0 }),
}));
vi.mock("../../../mobile/src/components/AndroidScreenHeader.tsx", () => ({
  AndroidSheetHeader: "Header",
}));
vi.mock("../../../mobile/src/components/ScreenHeader.tsx", () => ({ ScreenHeader: "Header" }));
vi.mock("../../../mobile/src/components/AppText.tsx", () => ({ AppText: "Text" }));
vi.mock("../../../mobile/src/components/AppSymbol.tsx", () => ({ SymbolView: "Symbol" }));
vi.mock("../../../mobile/src/components/ProviderIcon.tsx", () => ({ ProviderIcon: "Icon" }));
vi.mock("../../../mobile/src/components/MaterialScreenContent.tsx", () => ({
  MaterialScreenContent: "Material",
}));
vi.mock("../../../mobile/src/lib/useUniwindTheme.ts", () => ({ useUniwindTheme: () => ({}) }));
vi.mock("../../../mobile/src/native/StackHeader.tsx", () => ({
  NativeStackScreenOptions: "Options",
  nativeHeaderScrollEdgeEffects: () => ({}),
}));
vi.mock("../../../mobile/src/native/SelectableMarkdownText.tsx", async () => {
  const { React } = await vi.importActual<RendererApi>("../../../web/test/component-renderer.ts");
  return {
    SelectableMarkdownText: ({ markdown }: { markdown: string }) =>
      React.createElement("Markdown", null, markdown),
  };
});
vi.mock("../../../mobile/src/features/files/FileMarkdownPreview.tsx", () => ({
  useMarkdownPreviewStyles: () => ({ nativeTextStyle: {} }),
}));
vi.mock("../../../mobile/src/features/layout/AdaptiveWorkspaceLayout.tsx", () => ({
  useAdaptiveWorkspaceLayout: () => ({ layout: { usesSplitView: false } }),
}));
vi.mock("../../../mobile/src/features/threads/use-visible-second-clock.ts", () => ({
  useVisibleSecondClock: () => 0,
}));
vi.mock("../../../mobile/src/state/entities.ts", () => ({
  useEnvironmentServerConfig: () => null,
  useProject: () => null,
  useThreadShell: () => null,
}));
vi.mock("../../../mobile/src/state/threads.ts", async () => {
  const { createEnvironmentThreadDetailAtoms, EMPTY_ENVIRONMENT_THREAD_STATE } =
    await vi.importActual<{
      createEnvironmentThreadDetailAtoms(source: () => Atom.Atom<unknown>): unknown;
      EMPTY_ENVIRONMENT_THREAD_STATE: Record<string, unknown>;
    }>("../../../../packages/client-runtime/src/state/threads.ts");
  const { v2Projection } =
    await import("../../../../packages/client-runtime/src/state/orchestrationV2TestFixtures.ts");
  const state = () =>
    AsyncResult.success({
      ...EMPTY_ENVIRONMENT_THREAD_STATE,
      data: Option.some({
        ...v2Projection,
        thread: { ...v2Projection.thread, nativeTables: host.getTables() },
        runs: [],
        subagents: [],
      }),
    });
  const source = Atom.make(state());
  host.setRefreshThread((registry) => registry.set(source, state()));
  return { environmentThreadDetails: createEnvironmentThreadDetailAtoms(() => source) };
});

const environmentId = EnvironmentId.make("archive-environment");
const deniedEnvironment = EnvironmentId.make("denied-environment");
const otherEnvironment = EnvironmentId.make("other-environment");
const threadId = ThreadId.make("historical-coordinator");
const target: TableTarget = { environmentId, threadId, tableId: "historical-table" };
const original =
  "  # Not an identity: director / Round 999\n🧭 café e\u0301 中文\n".repeat(1800) +
  "\nEXACT END 🦉\n";
const owner = {
  processId: "stopped-process",
  coordinatorSessionId: "coordinator-session",
  epoch: "epoch",
};
const teamLaunch = nativeWorkerFixture().launch;
if (teamLaunch.source !== "team_assign") throw new Error("Expected team fixture");
const worker = nativeWorkerFixture({
  selected: { model: "selected/model", requestedModel: "requested/model", reasoning: "low" },
  activated: { attemptId: "observed-attempt", model: "activated/model", reasoning: "high" },
  launch: {
    ...teamLaunch,
    ...owner,
    providerInstanceId: ProviderInstanceId.make("pi"),
    toolCallId: "start",
    source: "team_assign",
    instanceId: "specialist-instance",
    assignmentId: "specialist-task",
    roleId: "architect",
  },
});
const child = nativeSubagentFixture({
  childThreadId: ThreadId.make("public-child"),
  nativeWorker: worker,
});
const member = {
  kind: "member",
  id: "specialist",
  assignmentId: "specialist-task",
  instanceId: "specialist-instance",
  roleId: "architect",
  function: "specialist",
  ordinal: 1,
  admittedAt: 1,
  mandate: { source: "TeamAssignment.task", text: "Separate specialist mandate 🏗️" },
  execution: "completed",
  conversationRefs: [
    {
      ...owner,
      kind: "read",
      source: "team_assign",
      instanceId: "specialist-instance",
      native: { generation: 1, historyId: "native-history", sessionId: "native-child" },
    },
  ],
} satisfies PiArchitectureRecord;
const longProposal =
  "Proposal earlier 🧠 " + "Café e\u0301 中文 👩🏽‍💻 🧭 ".repeat(100) + "EXACT CONTRIBUTION END 🦉";
function publication(id: string, ordinal: number, round: number): PiArchitectureRecord {
  return {
    kind: "publication",
    id,
    message: {
      id,
      ordinal,
      roomId: "room",
      assignmentId: "specialist-task",
      memberId: "specialist",
      author: "not-the-roster-author",
      stage: "contribution",
      snapshotId: `snapshot-${id}`,
      round,
      turnId: `turn-${id}`,
      attemptId: `attempt-${id}`,
      body: {
        kind: "contribution",
        proposal: id === "earlier" ? longProposal : `Proposal ${id} 🧠`,
        arguments: [
          id === "earlier" ? "Secondary argument remains verbatim 🏗️" : "Explicit argument",
        ],
        objections: [
          { memberId: "absent-member", messageId: "absent-message", argument: "Missing reference" },
          ...(id === "latest"
            ? [
                {
                  memberId: "specialist",
                  messageId: "earlier",
                  argument: "Compare earlier proposal",
                },
              ]
            : []),
        ],
        doubts:
          id === "earlier"
            ? [{ question: "Exact doubt?", purpose: "Original doubt purpose 🔎" }]
            : [],
      },
      native: { sessionId: "native-child", toolCallId: id, persistence: "unconfirmed" },
      doubts: [],
    },
  };
}
function archive(
  tableId = target.tableId,
  label = "Exact recommendation 🧠",
): ArchitectureArchive.ArchitectureArchive {
  return {
    summary: {
      tableId,
      threadId,
      providerInstanceId: ProviderInstanceId.make("pi"),
      coordinatorSessionId: owner.coordinatorSessionId,
      roomId: "room",
      mode: "table",
      participants: 2,
      lifecycle: {
        stage: "closing",
        round: 2,
        protocolState: "finished",
        outcome: "available",
        closure: { state: "unknown", members: "released", director: "unknown" },
      },
      capture: "incomplete",
      observation: "stale",
      gaps: ["missed native interval"],
    },
    checkpoint: { owner, ownerGeneration: 1, snapshotId: "captured-snapshot", cut: 20 },
    providerSessionId: ProviderSessionId.make("provider-session"),
    parentProviderThreadId: ProviderThreadId.make("provider-parent"),
    original: { id: "original", text: original, source: "original WorkBrief", complete: false },
    records: [
      member,
      {
        ...member,
        id: "unbound",
        instanceId: "unbound-instance",
        assignmentId: "unbound-task",
        roleId: "unbound-role",
        ordinal: 2,
        conversationRefs: [],
      },
      publication("earlier", 1, 1),
      publication("latest", 2, 2),
      {
        kind: "control",
        id: "later",
        order: 9,
        action: "priorities",
        text: "Later instruction is not the original",
        author: { coordinatorSessionId: "instruction-author", toolCallId: "control-call" },
        application: { state: "pending" },
      },
      {
        kind: "control",
        id: "first",
        order: 2,
        action: "context",
        text: "First instruction",
        author: { coordinatorSessionId: "first-author", toolCallId: "first-call" },
        application: { state: "applied", stage: "contribution", round: 1 },
      },
      {
        kind: "failure",
        id: "failed-turn",
        phase: "turn",
        error: "Captured participant failure",
        memberId: "specialist",
        round: 1,
      },
      {
        kind: "outcome",
        id: "final",
        source: "background-settlement",
        status: "completed",
        content: {
          kind: "synthesis",
          body: {
            kind: "synthesis",
            status: "partial",
            recommendation: label,
            alternatives: [],
            evidence: [],
            assumptions: [],
            disagreements: ["Separate disagreement"],
            pending: ["Open obligation"],
          },
        },
      },
      ...Array.from({ length: 16 }, (_, i): PiArchitectureRecord => ({
        kind: "failure",
        id: `historical-failure-${i}`,
        phase: "turn",
        error: `Historical failure ${i}`,
      })),
    ],
  };
}

type Surface = Component<{
  target: TableTarget;
  atoms: PiArchitectureAtoms;
  onCoordinator: () => void;
  onParticipant: (id: string) => void;
}>;
type NativeSurface = Component<{ route: { params: TableTarget } }>;
function text(node: Instance): string {
  return node.children.map((part) => (typeof part === "string" ? part : text(part))).join("");
}
function control(renderer: ReactTestRenderer, label: string) {
  const found = renderer.root.findAll(
    (node) =>
      (node.type === "button" || String(node.type) === "Pressable") &&
      (text(node) === label ||
        node.props["aria-label"] === label ||
        node.props.accessibilityLabel === label),
  )[0];
  if (!found) throw new Error(`Missing control: ${label}\n${text(renderer.root).slice(0, 3000)}`);
  return found;
}
async function press(renderer: ReactTestRenderer, label: string) {
  const node = control(renderer, label);
  await act(async () =>
    invoke(node, node.props.onPress ? "onPress" : "onClick", {
      nativeEvent: new Event("click"),
      preventDefault() {},
      defaultPrevented: false,
    }),
  );
}
function invoke(node: Instance, handler: string, event: unknown) {
  const callback = node.props[handler];
  if (typeof callback !== "function") throw new Error(`Missing host callback ${handler}`);
  return callback(event);
}
function settled<A, E>(
  registry: AtomRegistry.AtomRegistry,
  atom: Atom.Atom<AsyncResult.AsyncResult<A, E>>,
) {
  return AtomRegistry.getResult(registry, atom, { suspendOnWaiting: true });
}
function rendering<A, E, R>(effect: Effect.Effect<A, E, R>) {
  return Effect.gen(function* () {
    const done = Promise.withResolvers<void>();
    const pending = act(() => done.promise);
    return yield* effect.pipe(
      Effect.ensuring(
        Effect.promise(() => {
          done.resolve();
          return pending;
        }),
      ),
    );
  });
}
const decodeReadInput = Schema.decodeUnknownEffect(PiArchitectureReadInput);

it.effect(
  "drives archived paged queries through guarded commands into real web and Android table interactions",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const directory = yield* fs.makeTempDirectoryScoped({
          prefix: "t3-architecture-components-",
        });
        const sqlite = () => NodeSqliteClient.layer({ filename: `${directory}/archive.sqlite` });
        yield* Effect.scoped(
          Effect.gen(function* () {
            const sql = yield* SqlClient.SqlClient;
            yield* ArchitectureArchive.setup(sql);
            yield* ArchitectureArchive.write(sql, archive());
            yield* ArchitectureArchive.write(sql, archive("second-table", "Other table outcome"));
          }).pipe(Effect.provide(sqlite())),
        );
        // Reopen persisted data. No native runtime, architecture producer or current turn exists.
        const sql = Context.get(yield* Layer.build(sqlite()), SqlClient.SqlClient);
        const insert = () =>
          ArchitectureArchive.write(sql, {
            ...archive(),
            records: [
              ...archive().records.map((record) =>
                record.kind === "control" && record.id === "later"
                  ? ({
                      ...record,
                      text: "Updated later instruction",
                      application: { state: "applied", stage: "synthesis", round: 3 },
                    } satisfies PiArchitectureRecord)
                  : record,
              ),
              publication("inserted", 3, 3),
            ],
          });
        const method = ORCHESTRATION_V2_WS_METHODS.getPiArchitecture;
        const group = WsRpcGroup.omit(
          ...[...WsRpcGroup.requests.keys()].filter(
            (tag): tag is Exclude<RpcGroup.Rpcs<typeof WsRpcGroup>["_tag"], typeof method> =>
              tag !== method,
          ),
        );
        const calls: Array<{
          environmentId: EnvironmentId;
          input: typeof PiArchitectureReadInput.Type;
        }> = [];
        let wireFault: "gap" | "revision" | undefined;
        let faultEnvironment = environmentId;
        let recaptures = 0;
        let activeChild = child;
        const makeSession = (id: EnvironmentId) =>
          Effect.gen(function* () {
            const rpc = yield* RpcTest.makeClient(group).pipe(
              Effect.provide(
                Layer.mergeAll(
                  group.toLayerHandler(method, (input) =>
                    Effect.gen(function* () {
                      calls.push({ environmentId: id, input });
                      if (
                        input.kind !== "tables" &&
                        input.tableId === target.tableId &&
                        ((input.kind === "records" && input.cursor) ||
                          (input.kind === "text" && input.offset))
                      ) {
                        const [previous] = yield* ArchitectureArchive.read(
                          sql,
                          input.threadId,
                          input.tableId,
                        );
                        if (!previous) return yield* Effect.die("Missing persisted table");
                        recaptures++;
                        yield* ArchitectureArchive.write(sql, {
                          ...previous,
                          checkpoint: {
                            ...previous.checkpoint,
                            snapshotId: `native-recapture-${recaptures}`,
                            cut: previous.checkpoint.cut + 1,
                          },
                        });
                      }
                      const rows = yield* ArchitectureArchive.read(sql, input.threadId);
                      const result = yield* ArchitectureQuery.read(input, rows, [activeChild]);
                      // Fault only the transport page metadata after the real persisted query.
                      if (
                        wireFault &&
                        id === faultEnvironment &&
                        input.kind === "text" &&
                        input.tableId === "second-table" &&
                        result.kind === "text" &&
                        result.offset > 0
                      ) {
                        return wireFault === "gap"
                          ? { ...result, offset: result.offset + 1 }
                          : { ...result, revision: `${result.revision}-changed` };
                      }
                      return result;
                    }).pipe(
                      Effect.mapError(
                        (cause) =>
                          new OrchestrationV2GetThreadProjectionError({
                            threadId: input.threadId,
                            message: "Failed to read Pi architecture archive",
                            cause,
                          }),
                      ),
                    ),
                  ),
                  RpcAuthorization.layer([AuthOrchestrationReadScope]),
                ),
              ),
            );
            // Adapt the in-memory single-method server to the full client transport interface.
            // Unexpected RPCs fail; no UI/domain result is fabricated.
            let write!: (
              message: RpcMessage.FromServer<RpcGroup.Rpcs<typeof WsRpcGroup>>,
            ) => Effect.Effect<void>;
            const transport = yield* RpcClient.makeNoSerialization<
              RpcGroup.Rpcs<typeof WsRpcGroup>,
              never
            >(WsRpcGroup, {
              onFromClient: ({ message }) =>
                Effect.gen(function* () {
                  if (message._tag !== "Request") return;
                  if (message.tag !== method)
                    return yield* Effect.die(`Unexpected RPC ${message.tag}`);
                  const input = yield* decodeReadInput(message.payload).pipe(Effect.orDie);
                  const exit = yield* Effect.exit(rpc[method](input));
                  yield* write({ _tag: "Exit", clientId: 0, requestId: message.id, exit });
                }),
            });
            write = transport.write;
            const session: Session = {
              client: transport.client,
              ready: Effect.void,
              probe: Effect.void,
              closed: Effect.never,
              initialConfig: Effect.die("No bootstrap config needed"),
              subscribeServerConfig: () => Stream.never,
            };
            const supervisor: Supervisor = {
              target: new PrimaryConnectionTarget({
                environmentId: id,
                label: id,
                httpBaseUrl: "https://archive.test",
                wsBaseUrl: "wss://archive.test",
              }),
              state: yield* SubscriptionRef.make<SupervisorConnectionState>({
                ...AVAILABLE_CONNECTION_STATE,
                desired: true,
                network: "online",
                phase: "connected",
                attempt: 1,
                generation: 1,
              }),
              session: yield* SubscriptionRef.make(Option.some(session)),
              prepared: yield* SubscriptionRef.make(Option.none<PreparedConnection>()),
              connect: Effect.void,
              disconnect: Effect.void,
              retryNow: Effect.void,
            };
            return supervisor;
          });
        const primary = yield* makeSession(environmentId);
        const denied = yield* makeSession(deniedEnvironment);
        const other = yield* makeSession(otherEnvironment);
        let routed = false;
        const supervisorFor = (id: EnvironmentId) =>
          id === deniedEnvironment
            ? denied
            : id === otherEnvironment
              ? other
              : routed
                ? denied
                : primary;
        const run: Registry["run"] = (id, effect) =>
          Effect.provideService(effect, EnvironmentSupervisor, supervisorFor(id));
        const followStream: Registry["followStream"] = (id, stream) =>
          Stream.provideService(stream, EnvironmentSupervisor, supervisorFor(id));
        const runtime = Atom.runtime(
          Layer.mock(EnvironmentRegistry, {
            run,
            followStream,
            stateChanges: (id) => SubscriptionRef.changes(supervisorFor(id).state),
            entries: yield* SubscriptionRef.make<
              ReadonlyMap<EnvironmentId, ConnectionCatalogEntry>
            >(new Map()),
            networkStatus: yield* SubscriptionRef.make<NetworkStatus>("online"),
          }),
        );
        const refresh = Atom.make(0);
        const atoms = createPiArchitectureAtoms(runtime, () => refresh);
        host.setAtoms(atoms);
        host.setRuntime(runtime);
        const sessions = createEnvironmentSessionAtoms(runtime);
        const registry = AtomRegistry.make({
          initialValues: [
            [
              sessions.sessionStateAtom(environmentId),
              AsyncResult.success({ authenticated: true, scopes: [AuthOrchestrationReadScope] }),
            ],
            [
              sessions.sessionStateAtom(otherEnvironment),
              AsyncResult.success({ authenticated: true, scopes: [AuthOrchestrationReadScope] }),
            ],
            [
              sessions.sessionStateAtom(deniedEnvironment),
              AsyncResult.success({ authenticated: true, scopes: [] }),
            ],
          ],
        });
        const releases: Array<() => void> = [];
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            releases.forEach((release) => release());
            registry.dispose();
            host.setRefreshThread(() => {});
          }),
        );
        for (const id of [environmentId, deniedEnvironment, otherEnvironment]) {
          releases.push(registry.mount(sessions.sessionStateAtom(id)));
        }
        expect(registry.get(atoms.read.permissionAtom(environmentId))).toBe(true);
        expect(registry.get(atoms.read.permissionAtom(deniedEnvironment))).toBe(false);
        const query = (input: typeof PiArchitectureReadInput.Type, id = environmentId) => {
          const atom = atoms.query({ environmentId: id, input });
          releases.push(registry.mount(atom));
          return AtomRegistry.getResult(registry, atom, { suspendOnWaiting: true }).pipe(
            Effect.tap(() => Effect.yieldNow),
          );
        };
        const catalog = yield* query({ kind: "tables", threadId });
        expect(catalog.kind).toBe("tables");
        if (catalog.kind !== "tables") return yield* Effect.die("Expected persisted catalog");
        expect(catalog.items.map((item) => item.tableId)).toEqual([
          "historical-table",
          "second-table",
        ]);
        host.setTables(catalog.items);
        const records = yield* query({ kind: "records", threadId, tableId: target.tableId });
        expect(records.kind).toBe("records");
        expect(records.kind === "records" && records.items.length).toBe(24);
        expect(calls.some(({ input }) => input.kind === "records" && Boolean(input.cursor))).toBe(
          true,
        );
        const captured = yield* query({
          kind: "text",
          threadId,
          tableId: target.tableId,
          textId: "original",
        });
        expect(captured.kind === "text" && captured.text).toBe(original);
        expect(recaptures).toBeGreaterThan(2);
        expect(
          calls.filter(({ input }) => input.kind === "text" && input.textId === "original").length,
        ).toBeGreaterThan(2);
        yield* query({ kind: "text", threadId, tableId: target.tableId, textId: "control:later" });
        let web: ReactTestRenderer | undefined;
        let native: ReactTestRenderer | undefined;
        try {
          yield* Effect.gen(function* () {
            vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
            vi.stubGlobal("requestAnimationFrame", (fn: () => void) => {
              fn();
              return 0;
            });
            const storage = new Map<string, string>();
            const localStorage = {
              getItem: (key: string) => storage.get(key) ?? null,
              setItem: (key: string, value: string) => {
                storage.set(key, value);
              },
              removeItem: (key: string) => {
                storage.delete(key);
              },
            };
            vi.stubGlobal("localStorage", localStorage);
            const { ArchitectLineage } = yield* Effect.promise(() =>
              vi.importActual<{
                ArchitectLineage: Component<{
                  environmentId: EnvironmentId;
                  tables: typeof catalog.items;
                }>;
              }>("../../../web/src/routes/-piArchitecture/ArchitectLineage.tsx"),
            );
            const { ArchitectTable } = yield* Effect.promise(() =>
              vi.importActual<{ ArchitectTable: Surface }>(
                "../../../web/src/routes/-piArchitecture/ArchitectTable.tsx",
              ),
            );
            const { ThreadArchitectTableScreen, mobilePiArchitecture } = yield* Effect.promise(() =>
              vi.importActual<{
                ThreadArchitectTableScreen: NativeSurface;
                mobilePiArchitecture: PiArchitectureAtoms;
              }>("../../../mobile/src/features/threads/ThreadArchitectTableScreen.tsx"),
            );
            const { ThreadAgentsSheet } = yield* Effect.promise(() =>
              vi.importActual<{
                ThreadAgentsSheet: Component<{
                  route: { params: { environmentId: EnvironmentId; threadId: ThreadId } };
                }>;
              }>("../../../mobile/src/features/threads/ThreadAgentsSheet.tsx"),
            );
            let focusedTab: string | null = null;
            const referenceScroll = vi.fn(() => {
              // Reference navigation must expose the real full target before scrolling.
              expect(text(web!.root)).toContain(longProposal);
              expect(text(web!.root)).toContain("Secondary argument remains verbatim 🏗️");
            });
            const referenceFocus = vi.fn();
            vi.stubGlobal("document", {
              getElementById: (id: string) =>
                web?.root.findAll((node) => node.props.role === "tab" && node.props.id === id)
                  .length
                  ? {
                      focus() {
                        focusedTab = id;
                      },
                    }
                  : id === "publication-earlier"
                    ? { scrollIntoView: referenceScroll, focus: referenceFocus }
                    : null,
            });
            vi.stubGlobal("window", Object.assign(new EventTarget(), { localStorage }));
            // Layout inputs are host events, not physical geometry evidence.
            let shift = 0;
            const pane = {
              scrollTop: 0,
              getBoundingClientRect: () => ({ top: 0 }),
              querySelectorAll: () =>
                web?.root
                  .findAll((node) => typeof node.props["data-reading-id"] === "string")
                  .map((node) => {
                    const id = node.props["data-reading-id"];
                    const position =
                      id === "publication:earlier"
                        ? 500 + shift
                        : id === "publication:latest"
                          ? 900 + shift
                          : 0;
                    return {
                      dataset: { readingId: id },
                      getBoundingClientRect: () => ({ top: position - pane.scrollTop }),
                    };
                  }) ?? [],
            };
            let emitResize = () => {};
            class LayoutObserver {
              targets = new Set<object>();
              readonly callback: (
                entries: Array<{ target: object; contentRect: { width: number; height: number } }>,
              ) => void;
              constructor(callback: LayoutObserver["callback"]) {
                this.callback = callback;
                emitResize = () => this.emit();
              }
              observe(value: object) {
                this.targets.add(value);
              }
              unobserve(value: object) {
                this.targets.delete(value);
              }
              disconnect() {
                this.targets.clear();
              }
              emit() {
                this.callback(
                  [...this.targets].map((value) => ({
                    target: value,
                    contentRect: { width: 800, height: 1000 + shift },
                  })),
                );
              }
            }
            vi.stubGlobal("ResizeObserver", LayoutObserver);
            const provider = (child: unknown) =>
              React.createElement(
                EffectAtomReact.RegistryContext.Provider,
                { value: registry },
                child,
              );
            const onCoordinator = vi.fn();
            const onParticipant = vi.fn();
            const table = (selected = target) =>
              provider(
                React.createElement(ArchitectTable, {
                  target: selected,
                  atoms,
                  onCoordinator,
                  onParticipant,
                }),
              );
            yield* Effect.promise(() =>
              act(async () => {
                web = create(
                  provider(
                    React.createElement(ArchitectLineage, { environmentId, tables: catalog.items }),
                  ),
                  {
                    createNodeMock: (element) =>
                      element.props.id === "architect-reading-pane" ? pane : { focus() {} },
                  },
                );
              }),
            );
            expect(text(web!.root)).toContain("Completion unconfirmed");
            expect(text(web!.root)).toContain("Result available");
            expect(text(web!.root)).toContain("Historical view");
            expect(text(web!.root)).toContain("Capture is incomplete.");
            expect(text(web!.root)).toContain("missed native interval");
            expect(text(web!.root)).not.toContain("historical-table");
            expect(text(web!.root)).not.toContain("Observation stale");
            expect(text(web!.root)).toContain("2 participants");
            expect(text(web!.root)).not.toContain("Technical details");
            expect(text(web!.root)).toContain("Native closure could not be confirmed.");
            host.navigate.mockClear();
            yield* Effect.promise(() => press(web!, "Expand Architect participants"));
            expect(host.navigate).not.toHaveBeenCalled();
            expect(text(web!.root)).toContain("Specialist · architect · #1");
            expect(text(web!.root)).toContain("Completed");
            const identifiers = web!.root.findAllByType("details")[0]!;
            expect(identifiers.props.open).not.toBe(true);
            expect(text(identifiers)).toContain("Member ID: specialist");
            yield* Effect.promise(() => press(web!, "Collapse Architect participants"));
            expect(text(web!.root)).not.toContain("Specialist · architect · #1");
            yield* Effect.promise(() => press(web!, "Expand Architect participants"));
            expect(text(web!.root)).toContain("Specialist · architect · #1");
            yield* Effect.promise(() => press(web!, "Open Architect table"));
            expect(host.navigate).toHaveBeenCalledWith({
              to: "/architect/$environmentId/$threadId/$tableId",
              params: target,
            });
            yield* Effect.promise(() => act(async () => web!.update(table())));
            expect(text(web!.root)).toContain("missed native interval");
            expect(text(web!.root)).not.toContain("Observation stale");
            const technical = web!.root.findAllByType("details")[0]!;
            expect(technical.props.open).toBe(false);
            yield* Effect.promise(() =>
              act(async () => invoke(technical, "onToggle", { currentTarget: { open: true } })),
            );
            expect(technical.props.open).toBe(true);
            expect(text(technical)).toContain("Table ID: historical-table");
            expect(text(technical)).toContain(
              "Observation stale · Capture incomplete · Protocol finished · Outcome available · Native closure unknown · Members released · Director unknown",
            );
            yield* Effect.promise(() =>
              act(async () => invoke(technical, "onToggle", { currentTarget: { open: false } })),
            );
            expect(technical.props.open).toBe(false);
            expect(text(technical)).not.toContain("Observation stale");
            yield* Effect.promise(() => press(web!, "Read original brief"));
            const expectBriefMode = (selected: "Reading" | "Source") => {
              for (const label of ["Reading", "Source"]) {
                const button = control(web!, label);
                const active = label === selected;
                expect(button.props["aria-pressed"]).toBe(active);
                expect(button.props.className).toContain(
                  active ? "bg-primary" : "border-transparent",
                );
                expect(button.props.className).not.toContain(
                  active ? "border-transparent" : "bg-primary",
                );
                expect(button.findByType("span").props.className).toBe(
                  active ? "font-semibold" : "font-normal",
                );
              }
            };
            expectBriefMode("Reading");
            const readingText = text(web!.root);
            expect(readingText).toContain("EXACT END 🦉");
            yield* Effect.promise(() => press(web!, "Source"));
            expectBriefMode("Source");
            expect(web!.root.findByType("pre").children.join("")).toBe(original);
            yield* Effect.promise(() => press(web!, "Reading"));
            expectBriefMode("Reading");
            expect(text(web!.root)).toBe(readingText);
            expect(text(web!.root)).toContain("EXACT END 🦉");
            yield* Effect.promise(() => press(web!, "Back to Summary"));
            // Host focus and class checks do not establish browser painting or contrast.
            const expectMainTabFocus = () => {
              for (const label of ["Summary", "Debate", "Participants"]) {
                const button = control(web!, label);
                const classes = String(button.props.className).split(" ");
                expect(classes).toContain("focus-visible:outline-2");
                expect(classes).toContain("focus-visible:-outline-offset-2");
                expect(classes).toContain(
                  button.props["aria-selected"]
                    ? "focus-visible:outline-primary-foreground"
                    : "focus-visible:outline-ring",
                );
                expect(classes).not.toContain(
                  button.props["aria-selected"]
                    ? "focus-visible:outline-ring"
                    : "focus-visible:outline-primary-foreground",
                );
              }
            };
            expectMainTabFocus();
            // Keyboard navigation changes the actual surface rather than only checking attributes.
            yield* Effect.promise(() =>
              act(async () =>
                invoke(control(web!, "Summary"), "onKeyDown", {
                  key: "ArrowRight",
                  preventDefault() {},
                }),
              ),
            );
            expect(text(web!.root)).toContain("Proposal latest 🧠");
            expect(focusedTab).toBe("architect-tab-Debate");
            expectMainTabFocus();
            expect(control(web!, "Debate").props).toMatchObject({
              "aria-selected": true,
              tabIndex: 0,
            });
            expect(control(web!, "Debate").props.className).toContain("bg-primary");
            expect(control(web!, "Debate").props.className).toContain("font-semibold");
            expect(control(web!, "Summary").props.className).toContain("text-muted-foreground");
            expect(control(web!, "Summary").props.className).not.toContain("bg-primary");
            yield* Effect.promise(() =>
              act(async () =>
                invoke(control(web!, "Debate"), "onKeyDown", {
                  key: "End",
                  preventDefault() {},
                }),
              ),
            );
            expect(text(web!.root)).toContain("Separate specialist mandate 🏗️");
            expect(focusedTab).toBe("architect-tab-Participants");
            expectMainTabFocus();
            expect(control(web!, "Participants").props.className).toContain("bg-primary");
            yield* Effect.promise(() =>
              act(async () =>
                invoke(control(web!, "Participants"), "onKeyDown", {
                  key: "Home",
                  preventDefault() {},
                }),
              ),
            );
            expect(text(web!.root)).toContain("Exact recommendation 🧠");
            expect(focusedTab).toBe("architect-tab-Summary");
            expectMainTabFocus();
            expect(control(web!, "Summary").props.className).toContain("font-semibold");
            yield* Effect.promise(() => press(web!, "Debate"));
            yield* Effect.promise(() => press(web!, "Round 1"));
            expect(text(web!.root)).toContain("Proposal earlier 🧠");
            expect(text(web!.root)).toContain("Specialist · architect · #1 · Contribution");
            expect(text(web!.root)).toContain("Contribution excerpt · Proposal");
            expect(text(web!.root)).not.toContain("EXACT CONTRIBUTION END 🦉");
            expect(text(web!.root)).not.toContain("Secondary argument remains verbatim 🏗️");
            expect(text(web!.root)).not.toContain("snapshot-earlier");
            const publicationTechnical = web!.root
              .findAllByType("details")
              .find((node) => text(node).includes("Contribution technical details"))!;
            expect(publicationTechnical.props.open).toBe(false);
            yield* Effect.promise(() =>
              act(async () =>
                invoke(publicationTechnical, "onToggle", { currentTarget: { open: true } }),
              ),
            );
            expect(text(web!.root)).toContain(
              "Snapshot snapshot-earlier · turn turn-earlier · attempt attempt-earlier",
            );
            expect(text(publicationTechnical)).toContain("Member ID: specialist");
            expect(text(publicationTechnical)).toContain("Message ID: earlier");
            yield* Effect.promise(() => press(web!, "Read full contribution"));
            expect(text(web!.root)).toContain(longProposal);
            expect(text(web!.root)).toContain("Secondary argument remains verbatim 🏗️");
            expect(text(web!.root)).toContain("Exact doubt?");
            expect(text(web!.root)).toContain("Original doubt purpose 🔎");
            const earlierContribution = web!.root.findAll(
              (node) => node.type === "article" && node.props.id === "publication-earlier",
            )[0]!;
            expect(earlierContribution).toBeDefined();
            expect(text(earlierContribution)).toContain("Reference unavailable: absent-member");
            expect(text(earlierContribution)).toContain("absent-message");
            expect(text(earlierContribution)).toContain("Missing reference");
            yield* Effect.promise(() => press(web!, "Show less"));
            expect(text(web!.root)).not.toContain("EXACT CONTRIBUTION END 🦉");
            yield* Effect.promise(() => press(web!, "Round 1"));
            const referenceFrames: Array<() => void> = [];
            vi.stubGlobal("requestAnimationFrame", (fn: () => void) => {
              referenceFrames.push(fn);
              return 0;
            });
            yield* Effect.promise(() => press(web!, "Read referenced contribution"));
            yield* Effect.promise(() =>
              act(async () => referenceFrames.splice(0).forEach((frame) => frame())),
            );
            vi.stubGlobal("requestAnimationFrame", (fn: () => void) => {
              fn();
              return 0;
            });
            expect(referenceScroll).toHaveBeenCalledOnce();
            expect(referenceFocus).toHaveBeenCalledOnce();
            expect(text(web!.root)).toContain(longProposal);
            expect(registry.get(tablePresentationAtom(target)).expanded).toMatchObject({
              "Round 1": true,
              "contribution:earlier": true,
            });
            pane.scrollTop = 532;
            yield* Effect.promise(() =>
              act(async () =>
                invoke(
                  web!.root.findAll((node) => node.props.id === "architect-reading-pane")[0]!,
                  "onScroll",
                  {},
                ),
              ),
            );
            expect(registry.get(tablePresentationAtom(target)).reading.Debate).toEqual({
              id: "publication:earlier",
              offset: 32,
              fallback: 532,
            });
            expect(text(web!.root)).toContain("Reference unavailable: absent-member");
            expect(
              web!.root.findAll(
                (node) => node.type === "button" && text(node) === "Read referenced contribution",
              ),
            ).toHaveLength(1);
            yield* Effect.promise(() => press(web!, "Participants"));
            expect(text(web!.root)).toContain(
              "Selected model: selected/model · Activated model: activated/model",
            );
            expect(text(web!.root)).toContain(
              "Selected model: unavailable · Activated model: unavailable",
            );
            expect(text(web!.root)).not.toContain("requested/model");
            expect(text(web!.root)).toContain("Separate specialist mandate 🏗️");
            expect(text(web!.root)).toContain("Conversation unavailable · 0 confirmed bindings");
            yield* Effect.promise(() => press(web!, "Read conversation: architect · specialist"));
            expect(onParticipant).toHaveBeenCalledWith("specialist");
            // Participant route unmount/remount retains the production presentation atom.
            yield* Effect.promise(() =>
              act(async () =>
                web!.update(provider(React.createElement("div", null, "public child"))),
              ),
            );
            yield* Effect.promise(() => act(async () => web!.update(table())));
            expect(text(web!.root)).toContain("Separate specialist mandate 🏗️");
            expect(registry.get(tablePresentationAtom(target))).toMatchObject({
              tab: "Participants",
              expanded: { "Round 1": true },
            });
            yield* Effect.promise(() => press(web!, "Debate"));
            expect(pane.scrollTop).toBe(532);
            yield* Effect.promise(() => press(web!, "Return to coordinator"));
            expect(onCoordinator).toHaveBeenCalledOnce();
            yield* Effect.promise(() => press(web!, "Summary"));
            expect(text(web!.root)).toContain("Exact recommendation 🧠");
            expect(text(web!.root)).toContain(
              "Confirmed outcome · completed · Synthesis · partial",
            );
            expect(text(web!.root)).toContain("Captured participant failure");
            expect(text(web!.root).indexOf("First instruction")).toBeLessThan(
              text(web!.root).indexOf("Later instruction is not the original"),
            );
            expect(text(web!.root)).toContain("instruction-author · instruction 9");
            expect(text(web!.root)).not.toContain("not-the-roster-author");
            expect(
              registry.get(tablePresentationAtom({ ...target, tableId: "second-table" })).tab,
            ).toBe("Summary");
            expect(
              registry.get(tablePresentationAtom({ ...target, environmentId: otherEnvironment }))
                .reading,
            ).toEqual({});
            const nativeCatalog = mobilePiArchitecture.query({
              environmentId,
              input: { kind: "tables", threadId },
            });
            releases.push(registry.mount(nativeCatalog));
            yield* settled(registry, nativeCatalog);
            const scrollTo = vi.fn();
            yield* Effect.promise(() =>
              act(async () => {
                native = create(
                  provider(
                    React.createElement(ThreadAgentsSheet, {
                      route: { params: { environmentId, threadId } },
                    }),
                  ),
                  {
                    createNodeMock: (element) =>
                      String(element.type) === "ScrollView" ? { scrollTo } : {},
                  },
                );
              }),
            );
            expect(text(native!.root)).toContain("Architect table");
            expect(text(native!.root)).toContain("Completion unconfirmed");
            expect(text(native!.root)).toContain("missed native interval");
            expect(text(native!.root)).not.toContain("Observation stale");
            yield* Effect.promise(() => press(native!, "Technical details"));
            expect(control(native!, "Technical details").props.accessibilityState).toMatchObject({
              expanded: true,
            });
            expect(text(native!.root)).toContain("Table ID: historical-table");
            expect(text(native!.root)).toContain("Native closure unknown");
            yield* Effect.promise(() => press(native!, "Technical details"));
            expect(control(native!, "Technical details").props.accessibilityState).toMatchObject({
              expanded: false,
            });
            expect(text(native!.root)).not.toContain("Observation stale");
            expect(text(native!.root)).toContain("Native closure could not be confirmed.");
            yield* Effect.promise(() => press(native!, "Participants"));
            expect(text(native!.root)).toContain("Specialist · architect · #1");
            expect(text(native!.root)).toContain("Completed");
            expect(text(native!.root)).not.toContain("Member ID: specialist");
            yield* Effect.promise(() => press(native!, "Technical identifiers"));
            expect(text(native!.root)).toContain("Member ID: specialist");
            yield* Effect.promise(() => press(native!, "Participants"));
            expect(text(native!.root)).not.toContain("Specialist · architect · #1");
            // Real app focus state, with native hosts substituted: not a device paint check.
            const focusNativeAction = async (label: string, filled: boolean) => {
              const before = control(native!, label);
              const classes = before.props.className;
              const selection = before.props.accessibilityState;
              const presentation = registry.get(tablePresentationAtom(target));
              const content = text(native!.root);
              expect(String(classes).split(" ")).not.toContain("outline-2");
              await act(async () => invoke(before, "onFocus", {}));
              const focused = control(native!, label);
              const cue = String(focused.props.className).split(" ");
              expect(cue).toContain("outline-2");
              expect(cue).toContain("-outline-offset-2");
              expect(cue).toContain(filled ? "outline-primary-foreground" : "outline-primary");
              expect(cue).not.toContain(filled ? "outline-primary" : "outline-primary-foreground");
              expect(cue.includes("bg-primary")).toBe(filled);
              expect(focused.props.accessibilityState).toEqual(selection);
              expect(registry.get(tablePresentationAtom(target))).toBe(presentation);
              expect(text(native!.root)).toBe(content);
              await act(async () => invoke(focused, "onBlur", {}));
              expect(control(native!, label).props.className).toBe(classes);
              expect(control(native!, label).props.accessibilityState).toEqual(selection);
              expect(registry.get(tablePresentationAtom(target))).toBe(presentation);
              expect(text(native!.root)).toBe(content);
            };
            yield* Effect.promise(() => focusNativeAction("Open Architect table", true));
            yield* Effect.promise(() => press(native!, "Open Architect table"));
            expect(host.dispatch).toHaveBeenLastCalledWith({
              type: "REPLACE",
              payload: { name: "ThreadArchitectTable", params: target },
            });
            const nativeStack = (participant = false) =>
              provider(
                React.createElement(
                  "ScreenStack",
                  null,
                  React.createElement(ThreadArchitectTableScreen, {
                    key: "table",
                    route: { params: target },
                  }),
                  participant
                    ? React.createElement(
                        "Screen",
                        { key: "participant" },
                        "public child conversation",
                      )
                    : null,
                ),
              );
            yield* Effect.promise(() => act(async () => native!.update(nativeStack())));
            yield* Effect.promise(() => press(native!, "Read original brief"));
            yield* Effect.promise(() => press(native!, "Source"));
            expect(text(native!.root)).toContain(original);
            expect(control(native!, "Source").props.className).toContain("bg-primary");
            expect(control(native!, "Reading").props.className).not.toContain("bg-primary");
            yield* Effect.promise(() => focusNativeAction("Source", true));
            yield* Effect.promise(() => focusNativeAction("Reading", false));
            yield* Effect.promise(() => press(native!, "Reading"));
            expect(text(native!.root)).toContain("EXACT END 🦉");
            yield* Effect.promise(() => press(native!, "Back to Summary"));
            yield* Effect.promise(() => press(native!, "Debate"));
            expect(text(native!.root)).toContain("Proposal earlier 🧠");
            expect(control(native!, "Debate").props).toMatchObject({
              accessibilityRole: "tab",
              accessibilityState: { selected: true },
            });
            expect(control(native!, "Debate").props.className).toContain("bg-primary");
            expect(control(native!, "Debate").findByType("Text").props.className).toContain(
              "font-t3-bold",
            );
            expect(control(native!, "Summary").props.className).not.toContain("bg-primary");
            expect(control(native!, "Summary").findByType("Text").props.className).toContain(
              "text-foreground-muted",
            );
            yield* Effect.promise(() => focusNativeAction("Debate", true));
            yield* Effect.promise(() => focusNativeAction("Summary", false));
            const nativeEarlier = () =>
              native!.root
                .findAll(
                  (node) =>
                    String(node.type) === "View" &&
                    typeof node.props.onLayout === "function" &&
                    text(node).includes("Proposal earlier 🧠"),
                )
                .at(-1)!;
            yield* Effect.promise(() =>
              act(async () =>
                invoke(nativeEarlier(), "onLayout", {
                  nativeEvent: { layout: { y: 500, height: 200 } },
                }),
              ),
            );
            // Both Debate groups are already expanded and the target is measured.
            // No state-key change or second onLayout can rescue this press.
            scrollTo.mockClear();
            yield* Effect.promise(() => press(native!, "Read referenced contribution"));
            expect(scrollTo).toHaveBeenLastCalledWith({ y: 500, animated: false });
            // A collapsed target must wait for its new layout, not use a stale box.
            yield* Effect.promise(() => press(native!, "Round 1"));
            scrollTo.mockClear();
            yield* Effect.promise(() => press(native!, "Read referenced contribution"));
            expect(scrollTo).not.toHaveBeenCalled();
            yield* Effect.promise(() =>
              act(async () =>
                invoke(nativeEarlier(), "onLayout", {
                  nativeEvent: { layout: { y: 500, height: 200 } },
                }),
              ),
            );
            expect(scrollTo).toHaveBeenLastCalledWith({ y: 500, animated: false });
            expect(text(native!.root)).toContain("Reference unavailable: absent-member");
            yield* Effect.promise(() =>
              act(async () =>
                invoke(native!.root.findByType("ScrollView"), "onScroll", {
                  nativeEvent: { contentOffset: { y: 550 } },
                }),
              ),
            );
            expect(registry.get(tablePresentationAtom(target)).reading.Debate).toEqual({
              id: "publication:earlier",
              offset: 50,
              fallback: 550,
            });
            const immutableCalls = calls.filter(
              ({ input }) => input.kind === "text" && input.textId === "original",
            ).length;
            const controlCalls = calls.filter(
              ({ input }) => input.kind === "text" && input.textId === "control:later",
            ).length;
            yield* insert();
            yield* rendering(
              Effect.gen(function* () {
                registry.set(refresh, 1);
                host.refreshThread(registry);
                yield* settled(
                  registry,
                  atoms.query({
                    environmentId,
                    input: { kind: "records", threadId, tableId: target.tableId },
                  }),
                );
                yield* settled(
                  registry,
                  mobilePiArchitecture.query({
                    environmentId,
                    input: { kind: "records", threadId, tableId: target.tableId },
                  }),
                );
                yield* settled(
                  registry,
                  atoms.query({
                    environmentId,
                    input: {
                      kind: "text",
                      threadId,
                      tableId: target.tableId,
                      textId: "control:later",
                    },
                  }),
                );
              }),
            );
            expect(
              calls.filter(({ input }) => input.kind === "text" && input.textId === "original"),
            ).toHaveLength(immutableCalls);
            expect(
              calls.filter(({ input }) => input.kind === "text" && input.textId === "control:later")
                .length,
            ).toBeGreaterThan(controlCalls);
            expect(registry.get(tablePresentationAtom(target))).toMatchObject({
              tab: "Debate",
              expanded: { "Round 1": true, "Round 3": true },
              reading: { Debate: { id: "publication:earlier", offset: 50 } },
            });
            expect(text(web!.root)).toContain("Proposal inserted 🧠");
            expect(text(web!.root)).toContain(longProposal);
            expect(text(web!.root)).toContain("Secondary argument remains verbatim 🏗️");
            expect(
              registry.get(tablePresentationAtom(target)).expanded["contribution:earlier"],
            ).toBe(true);
            expect(pane.scrollTop).toBe(532);
            expect(referenceScroll).toHaveBeenCalledOnce();
            expect(text(native!.root)).toContain("Proposal inserted 🧠");
            shift = 300;
            yield* Effect.promise(() => act(async () => emitResize()));
            expect(pane.scrollTop).toBe(832);
            yield* Effect.promise(() =>
              act(async () =>
                invoke(nativeEarlier(), "onLayout", {
                  nativeEvent: { layout: { y: 800, height: 250 } },
                }),
              ),
            );
            expect(scrollTo).toHaveBeenLastCalledWith({ y: 850, animated: false });
            yield* Effect.promise(() => press(native!, "Participants"));
            expect(text(native!.root)).toContain(
              "Selected model: selected/model · Activated model: activated/model",
            );
            expect(text(native!.root)).toContain(
              "Selected model: unavailable · Activated model: unavailable",
            );
            expect(text(native!.root)).not.toContain("requested/model");
            activeChild = {
              ...child,
              nativeWorker: { ...worker, activated: null },
            };
            yield* rendering(
              Effect.gen(function* () {
                registry.set(refresh, 2);
                host.refreshThread(registry);
                for (const source of [atoms, mobilePiArchitecture]) {
                  yield* settled(
                    registry,
                    source.query({
                      environmentId,
                      input: { kind: "records", threadId, tableId: target.tableId },
                    }),
                  );
                }
              }),
            );
            for (const surface of [web!, native!]) {
              expect(text(surface.root)).toContain(
                "Selected model: selected/model · Activated model: unavailable",
              );
              expect(text(surface.root)).not.toContain("Activated model: activated/model");
              expect(text(surface.root)).not.toContain("requested/model");
            }
            yield* Effect.promise(() =>
              press(native!, "Read conversation: architect · specialist"),
            );
            expect(host.dispatch).toHaveBeenLastCalledWith({
              type: "PUSH",
              payload: {
                name: "Thread",
                params: {
                  environmentId,
                  threadId: child.childThreadId,
                  architectParticipant: true,
                },
              },
            });
            const scrolls = scrollTo.mock.calls.length;
            yield* Effect.promise(() => act(async () => native!.update(nativeStack(true))));
            expect(text(native!.root)).toContain("public child conversation");
            yield* Effect.promise(() => act(async () => native!.update(nativeStack())));
            expect(scrollTo).toHaveBeenCalledTimes(scrolls);
            yield* Effect.promise(() => press(native!, "Debate"));
            yield* Effect.promise(() =>
              act(async () =>
                invoke(nativeEarlier(), "onLayout", {
                  nativeEvent: { layout: { y: 800, height: 250 } },
                }),
              ),
            );
            expect(scrollTo).toHaveBeenLastCalledWith({ y: 850, animated: false });
            expect(registry.get(tablePresentationAtom(target)).expanded["Round 1"]).toBe(true);
            yield* Effect.promise(() => press(native!, "Summary"));
            expect(text(native!.root)).toContain("Updated later instruction");
            expect(text(native!.root)).not.toContain(original);
            yield* Effect.promise(() =>
              press(native!, "Return to coordinator to give instructions"),
            );
            expect(host.back).toHaveBeenCalled();
            // Same rendered table, new table/environment identities: presentation and
            // captured failures cannot leak across destinations.
            const second = { ...target, tableId: "second-table" };
            yield* rendering(
              Effect.gen(function* () {
                web!.update(table(second));
                yield* settled(
                  registry,
                  atoms.query({
                    environmentId,
                    input: { kind: "records", threadId, tableId: second.tableId },
                  }),
                );
              }),
            );
            expect(text(web!.root)).toContain("Other table outcome");
            expect(registry.get(tablePresentationAtom(second)).reading).toEqual({});
            wireFault = "gap";
            yield* Effect.promise(() => press(web!, "Read original brief"));
            yield* rendering(
              Effect.exit(
                settled(
                  registry,
                  atoms.query({
                    environmentId,
                    input: { kind: "text", threadId, tableId: second.tableId, textId: "original" },
                  }),
                ),
              ),
            );
            expect(text(web!.root)).toContain("No pages have been combined across revisions.");
            expect(text(web!.root)).not.toContain("EXACT END 🦉");
            yield* rendering(
              Effect.gen(function* () {
                native!.update(
                  provider(
                    React.createElement(ThreadArchitectTableScreen, { route: { params: second } }),
                  ),
                );
                yield* Effect.exit(
                  settled(
                    registry,
                    mobilePiArchitecture.query({
                      environmentId,
                      input: {
                        kind: "text",
                        threadId,
                        tableId: second.tableId,
                        textId: "original",
                      },
                    }),
                  ),
                );
              }),
            );
            expect(text(native!.root)).toContain("No pages have been combined across revisions.");
            const remote = { ...second, environmentId: otherEnvironment };
            faultEnvironment = otherEnvironment;
            wireFault = "revision";
            yield* rendering(
              Effect.gen(function* () {
                web!.update(table(remote));
                yield* settled(
                  registry,
                  atoms.query({
                    environmentId: otherEnvironment,
                    input: { kind: "records", threadId, tableId: second.tableId },
                  }),
                );
              }),
            );
            expect(text(web!.root)).toContain("Other table outcome");
            expect(text(web!.root)).not.toContain("No pages have been combined across revisions.");
            yield* Effect.promise(() => press(web!, "Read original brief"));
            yield* rendering(
              Effect.exit(
                settled(
                  registry,
                  atoms.query({
                    environmentId: otherEnvironment,
                    input: { kind: "text", threadId, tableId: second.tableId, textId: "original" },
                  }),
                ),
              ),
            );
            expect(text(web!.root)).toContain("No pages have been combined across revisions.");
            wireFault = undefined;
            yield* Effect.promise(() => act(async () => web!.update(table())));
            yield* Effect.promise(() => press(web!, "Read original brief"));
            yield* Effect.promise(() => press(web!, "Source"));
            expect(web!.root.findByType("pre").children.join("")).toBe(original);
            yield* Effect.promise(() => press(web!, "Back to Summary"));
            yield* Effect.promise(() => press(web!, "Debate"));
            expect(registry.get(tablePresentationAtom(target))).toMatchObject({
              expanded: { "Round 1": true },
              reading: { Debate: { id: "publication:earlier", offset: 50 } },
            });
            expect(pane.scrollTop).toBe(850);
          });
          // Origin rejection and destination rejection must not reach the query service;
          // the independent permitted environment remains usable.
          const count = calls.length;
          const originDenied = yield* Effect.promise(() =>
            atoms.read.run(registry, {
              environmentId: deniedEnvironment,
              input: { kind: "tables", threadId },
            }),
          );
          expect(originDenied._tag).toBe("Failure");
          if (originDenied._tag === "Failure") {
            expect(Option.getOrNull(Cause.findErrorOption(originDenied.cause))).toMatchObject({
              _tag: "EnvironmentAuthorizationError",
              requiredScope: AuthOrchestrationReadScope,
            });
          }
          expect(calls).toHaveLength(count);
          routed = true;
          const destinationDenied = yield* Effect.promise(() =>
            atoms.read.run(registry, { environmentId, input: { kind: "tables", threadId } }),
          );
          expect(destinationDenied._tag).toBe("Failure");
          if (destinationDenied._tag === "Failure") {
            expect(Option.getOrNull(Cause.findErrorOption(destinationDenied.cause))).toMatchObject({
              _tag: "EnvironmentAuthorizationError",
              requiredScope: AuthOrchestrationReadScope,
            });
          }
          expect(calls).toHaveLength(count);
          routed = false;
          const independent = yield* query({ kind: "tables", threadId }, otherEnvironment);
          expect(independent.kind).toBe("tables");
          expect(calls.at(-1)?.environmentId).toBe(otherEnvironment);
        } finally {
          yield* Effect.promise(async () => {
            await act(async () => {
              web?.unmount();
              native?.unmount();
            });
          });
          vi.unstubAllGlobals();
        }
      }).pipe(Effect.provide(NodeServices.layer)),
    ),
);
