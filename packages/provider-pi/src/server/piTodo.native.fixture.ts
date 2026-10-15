// @effect-diagnostics nodeBuiltinImport:off preferSchemaOverJson:off -- Private native fixture; JSONL is the installed producer's protocol.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeCrypto from "node:crypto";
import * as NodeProcess from "node:process";
import * as NodeUtil from "node:util";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import type * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import {
  makePiRpcConnection,
  type PiRpcRecord,
  type PiRpcError,
  type PiRpcTimeoutError,
} from "./rpc.ts";

const Message = Schema.Struct({
  role: Schema.String,
  toolName: Schema.optional(Schema.String),
  toolCallId: Schema.optional(Schema.String),
  content: Schema.Unknown,
  details: Schema.optional(Schema.Unknown),
});
export const NativeTodoEntry = Schema.Struct({
  id: Schema.String,
  parentId: Schema.NullOr(Schema.String),
  type: Schema.String,
  timestamp: Schema.String,
  message: Schema.optional(Message),
  firstKeptEntryId: Schema.optional(Schema.NullOr(Schema.String)),
  summary: Schema.optional(Schema.String),
  customType: Schema.optional(Schema.String),
});
const Entries = Schema.Struct({
  entries: Schema.Array(NativeTodoEntry),
  leafId: Schema.NullOr(Schema.String),
});
const State = Schema.Struct({ sessionId: Schema.String, sessionFile: Schema.String });
const BeforeAppend = Schema.Struct({
  toolCallId: Schema.String,
  sessionId: Schema.String,
  sessionFile: Schema.String,
  leafId: Schema.NullOr(Schema.String),
  entries: Schema.Array(NativeTodoEntry),
  persisted: Schema.Array(NativeTodoEntry),
});
const decodeState = Schema.decodeUnknownEffect(State);
const decodeEntries = Schema.decodeUnknownEffect(Entries);
const decodeBeforeAppend = Schema.decodeUnknownEffect(BeforeAppend);
const decodeRawEntries = Schema.decodeUnknownEffect(
  Schema.Struct({
    entries: Schema.Array(Schema.Unknown),
  }),
);
type NativeTodoError = PiRpcError | PiRpcTimeoutError | Schema.SchemaError;
type NativeTodoServices = Scope.Scope | ChildProcessSpawner.ChildProcessSpawner;
export type NativeTodoSnapshot = Schema.Schema.Type<typeof Entries> &
  Schema.Schema.Type<typeof State>;

/** Follow every parent, including custom/compaction entries, not file order. */
export function nativeTodoBranch(snapshot: NativeTodoSnapshot) {
  const byId = new Map(snapshot.entries.map((entry) => [entry.id, entry]));
  const branch: Array<Schema.Schema.Type<typeof NativeTodoEntry>> = [];
  let id = snapshot.leafId;
  while (id !== null) {
    const entry = byId.get(id);
    if (!entry || branch.some((ancestor) => ancestor.id === id)) {
      throw new Error(`Broken native ancestry at ${id}`);
    }
    branch.unshift(entry);
    id = entry.parentId;
  }
  return branch;
}

function extensionSource(piRoot: string) {
  return `
import { readFileSync } from "node:fs";
import { randomUUID } from "node:crypto";
import { createAssistantMessageEventStream } from ${JSON.stringify(NodePath.join(piRoot, "../pi-ai/dist/utils/event-stream.js"))};
export default function(pi) {
  // Fail closed: no accidental real provider or network fallback.
  globalThis.fetch = () => { throw Error("Native todo fixture forbids network"); };
  pi.registerProvider("t3-todo-fixture", {
    api: "openai-responses", baseUrl: "http://127.0.0.1", apiKey: "offline-fixture",
    models: [{ id: "controlled", name: "Offline todo", reasoning: false, input: ["text"],
      contextWindow: 1000000, maxTokens: 128,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } }],
    streamSimple(model, context) {
      const last = context.messages.at(-1);
      const input = last?.role === "user" ? last.content : undefined;
      const text = typeof input === "string" ? input : input?.find(block => block.type === "text")?.text;
      const params = text?.startsWith("TODO ") ? JSON.parse(text.slice(5)) : undefined;
      const content = params
        ? [{ type: "toolCall", id: randomUUID(), name: "todo", arguments: params }]
        : [{ type: "text", text: "Offline boundary complete." }];
      const message = { role: "assistant", content, api: model.api, provider: model.provider, model: model.id,
        stopReason: params ? "toolUse" : "stop", timestamp: Date.now(),
        usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, totalTokens: 2,
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } } };
      const stream = createAssistantMessageEventStream();
      queueMicrotask(() => {
        stream.push({ type: "start", partial: { ...message, content: [] } });
        if (params) {
          stream.push({ type: "toolcall_start", contentIndex: 0, partial: message });
          stream.push({ type: "toolcall_end", contentIndex: 0, toolCall: content[0], partial: message });
        } else {
          stream.push({ type: "text_start", contentIndex: 0, partial: message });
          stream.push({ type: "text_delta", contentIndex: 0, delta: content[0].text, partial: message });
          stream.push({ type: "text_end", contentIndex: 0, content: content[0].text, partial: message });
        }
        stream.push({ type: "done", reason: message.stopReason, message }); stream.end(message);
      });
      return stream;
    }
  });
  pi.on("tool_execution_end", (event, ctx) => {
    if (event.toolName !== "todo") return;
    const manager = ctx.sessionManager;
    const file = manager.getSessionFile();
    const persisted = readFileSync(file, "utf8").trim().split("\\n").map(line => JSON.parse(line))
      .filter(entry => entry.type !== "session");
    ctx.ui.notify(JSON.stringify({ kind: "native-before-append", toolCallId: event.toolCallId,
      sessionId: manager.getSessionId(), sessionFile: file, entries: manager.getEntries(),
      leafId: manager.getLeafId(), persisted }));
  });
  pi.registerCommand("fixture-tree", {
    description: "Select a real native branch without an LLM summary",
    async handler(id, ctx) {
      const result = await ctx.navigateTree(id, { summarize: false });
      if (result.cancelled) throw Error("Native tree navigation cancelled");
    }
  });
  pi.registerCommand("fixture-marker", {
    description: "Append a native non-message ancestor",
    handler() { pi.appendEntry("native-todo-marker", { fixture: true }); }
  });
  pi.on("session_start", (_event, ctx) => ctx.ui.notify(JSON.stringify({
    kind: "native-process", pid: process.pid, node: process.version, argv: process.argv,
    sessionId: ctx.sessionManager.getSessionId(), sessionFile: ctx.sessionManager.getSessionFile()
  })));
  pi.on("session_before_compact", event => ({
    compaction: { summary: "Offline native compaction", tokensBefore: event.preparation.tokensBefore,
      firstKeptEntryId: event.branchEntries.at(-1).id }
  }));
}
`;
}

/**
 * Real CLI, real installed rpiv-todo, real private JSONL. Opt-in paths avoid
 * requiring Pi on ordinary CI. The callback may reuse captures in adapter tests.
 */
export const withNativeTodo = <A, E>(
  check: (fixture: {
    capture: (label: string) => Effect.Effect<NativeTodoSnapshot, NativeTodoError>;
    todo: (params: PiRpcRecord) => Effect.Effect<
      {
        snapshot: NativeTodoSnapshot;
        before: Schema.Schema.Type<typeof BeforeAppend>;
        live: PiRpcRecord;
        result: Schema.Schema.Type<typeof NativeTodoEntry>;
      },
      NativeTodoError
    >;
    prompt: (message: string) => Effect.Effect<ReadonlyArray<PiRpcRecord>, NativeTodoError>;
    request: (record: PiRpcRecord) => Effect.Effect<unknown, NativeTodoError>;
    reopen: (sessionFile: string) => Effect.Effect<void, NativeTodoError, NativeTodoServices>;
  }) => Effect.Effect<A, E, NativeTodoServices>,
) =>
  Effect.gen(function* () {
    const piRoot = NodeFS.realpathSync(NodeProcess.env.T3_PI_TODO_NATIVE_PI_ROOT ?? "");
    const todoRoot = NodeFS.realpathSync(NodeProcess.env.T3_PI_TODO_NATIVE_TODO_ROOT ?? "");
    const cli = NodePath.join(piRoot, "dist/cli.js");
    const producer = NodePath.join(todoRoot, "index.ts");
    const root = yield* Effect.acquireRelease(
      Effect.sync(() => NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-native-todo-"))),
      (directory) => Effect.sync(() => NodeFS.rmSync(directory, { recursive: true, force: true })),
    );
    const cwd = NodePath.join(root, "workspace");
    const agentDir = NodePath.join(root, "agent");
    for (const dir of [cwd, agentDir, NodePath.join(root, "temp"), NodePath.join(root, "sessions")])
      NodeFS.mkdirSync(dir, { mode: 0o700 });
    const settings = {
      enableInstallTelemetry: false,
      compaction: { enabled: false, keepRecentTokens: 1 },
      cacheWarming: "off",
    };
    NodeFS.writeFileSync(NodePath.join(agentDir, "settings.json"), JSON.stringify(settings));
    NodeFS.writeFileSync(NodePath.join(agentDir, "auth.json"), "{}");
    const extension = NodePath.join(root, "offline.mjs");
    NodeFS.writeFileSync(extension, extensionSource(piRoot));
    const record = (value: unknown) => {
      NodeProcess.stdout.write(`NATIVE_TODO ${JSON.stringify(value)}\n`);
    };
    record({
      kind: "producer",
      node: process.version,
      cli,
      producer,
      hashes: Object.fromEntries(
        [
          cli,
          producer,
          NodePath.join(piRoot, "package.json"),
          NodePath.join(piRoot, "dist/core/agent-session.js"),
          NodePath.join(piRoot, "dist/modes/rpc/rpc-mode.js"),
          NodePath.join(todoRoot, "package.json"),
          NodePath.join(todoRoot, "state/replay.ts"),
          NodePath.join(todoRoot, "tool/response-envelope.ts"),
          extension,
        ].map((file) => [
          file,
          NodeCrypto.createHash("sha256").update(NodeFS.readFileSync(file)).digest("hex"),
        ]),
      ),
      settings,
    });
    const args = [
      cli,
      "--mode",
      "rpc",
      "--offline",
      "--no-extensions",
      "--no-skills",
      "--no-prompt-templates",
      "--no-themes",
      "--no-context-files",
      "--no-mcp",
      "--no-builtin-tools",
      "-e",
      producer,
      "-e",
      extension,
      "--provider",
      "t3-todo-fixture",
      "--model",
      "controlled",
      "--session-dir",
      NodePath.join(root, "sessions"),
    ];
    const spawn = (sessionFile?: string) => {
      const launch = {
        command: process.execPath,
        args: [...args, ...(sessionFile ? ["--session", sessionFile] : [])],
        cwd,
        env: {
          PATH: `${NodePath.dirname(process.execPath)}:/usr/bin:/bin`,
          HOME: root,
          TMPDIR: NodePath.join(root, "temp"),
          XDG_CONFIG_HOME: NodePath.join(root, "config"),
          PI_CODING_AGENT_DIR: agentDir,
          PI_OFFLINE: "1",
        },
      };
      record({ kind: "launch", ...launch });
      return makePiRpcConnection(launch);
    };
    const program = Effect.gen(function* () {
      let connection = yield* spawn();
      const request = (command: PiRpcRecord) =>
        connection
          .request(command)
          .pipe(
            Effect.tap((response) => Effect.sync(() => record({ kind: "rpc", command, response }))),
          );
      const capture = (label: string) =>
        Effect.gen(function* () {
          const state = yield* decodeState(yield* request({ type: "get_state" }));
          const response = yield* request({ type: "get_entries" });
          const entries = yield* decodeEntries(response);
          const raw = NodeFS.readFileSync(state.sessionFile, "utf8");
          record({ kind: "capture", label, ...state, ...entries, persistedJsonl: raw });
          const persisted = raw
            .trim()
            .split("\n")
            .map((line) => JSON.parse(line))
            .filter((entry) => entry.type !== "session");
          const native = yield* decodeRawEntries(response);
          if (!NodeUtil.isDeepStrictEqual(persisted, native.entries))
            throw new Error(`get_entries differs from private persisted tree at ${label}`);
          return { ...state, ...entries };
        });
      const prompt = (message: string) =>
        Effect.gen(function* () {
          const events: Array<PiRpcRecord> = [];
          yield* request({ type: "prompt", message });
          while (true) {
            const event = yield* Queue.take(connection.events);
            events.push(event);
            record({ kind: "event", event });
            if (event["type"] === "extension_error") throw new Error(JSON.stringify(event));
            if (event["type"] === "agent_settled") return events;
          }
        });
      const todo = (params: PiRpcRecord) =>
        Effect.gen(function* () {
          const events = yield* prompt(`TODO ${JSON.stringify(params)}`);
          const snapshot = yield* capture(String(params["action"]));
          const live = events.find(
            (event) => event["type"] === "tool_execution_end" && event["toolName"] === "todo",
          );
          const beforeEvent = events.find(
            (event) =>
              event["type"] === "extension_ui_request" &&
              typeof event["message"] === "string" &&
              event["message"].includes('"kind":"native-before-append"'),
          );
          if (!live || !beforeEvent)
            throw new Error("Missing native pre-append/live tool observation");
          const before = yield* decodeBeforeAppend(JSON.parse(String(beforeEvent["message"])));
          const result = nativeTodoBranch(snapshot).find(
            (entry) =>
              entry.message?.toolCallId === live["toolCallId"] &&
              entry.message?.role === "toolResult",
          );
          if (!result) throw new Error("Live tool has no appended active-branch result");
          if (
            before.sessionId !== snapshot.sessionId ||
            before.sessionFile !== snapshot.sessionFile ||
            before.toolCallId !== live["toolCallId"] ||
            result.parentId !== before.leafId ||
            [...before.entries, ...before.persisted].some(
              (entry) =>
                entry.message?.role === "toolResult" &&
                entry.message.toolCallId === before.toolCallId,
            )
          ) {
            throw new Error("Incoherent native append observation");
          }
          record({
            kind: "proven-append",
            sessionId: snapshot.sessionId,
            leafId: snapshot.leafId,
            toolCallId: before.toolCallId,
            entryId: result.id,
            parentId: result.parentId,
            beforeLeafId: before.leafId,
          });
          return { snapshot, before, live, result };
        });
      const value = yield* check({
        capture,
        todo,
        prompt,
        request,
        reopen: (sessionFile) =>
          Effect.gen(function* () {
            const orderlyExit = yield* connection.shutdown;
            record({ kind: "shutdown", sessionFile, orderlyExit });
            if (!orderlyExit) throw new Error("Native Pi did not exit cleanly before reopen");
            connection = yield* spawn(sessionFile);
            yield* request({ type: "get_state" });
          }),
      });
      const orderlyExit = yield* connection.shutdown;
      record({ kind: "shutdown", orderlyExit });
      if (!orderlyExit) throw new Error("Native Pi did not exit cleanly");
      return value;
    }).pipe(Effect.scoped);
    return yield* program;
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer));
