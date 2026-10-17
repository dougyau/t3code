// @effect-diagnostics nodeBuiltinImport:off preferSchemaOverJson:off -- Private offline native test setup and atomic child publications.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeProcess from "node:process";
import { assert } from "@effect/vitest";
import * as Schema from "effect/Schema";

/** Atomic file publication, not timing/polling, drives every native milestone. */
export function publication(
  root: string,
  publications: { signal: AbortSignal; closed: Set<Promise<void>> },
  file: string,
  accept: (value: unknown) => boolean,
  fresh = false,
) {
  const { signal, closed } = publications;
  return new Promise<unknown>((resolve, reject) => {
    const watcher = NodeFS.watch(root, { signal }, (_event, changed) => {
      if (signal.aborted) return;
      if (String(changed) === file) check();
    });
    const joined = new Promise<void>((done) => watcher.once("close", done));
    closed.add(joined);
    void joined.then(() => { closed.delete(joined); });
    const check = () => {
      try {
        const value: unknown = JSON.parse(NodeFS.readFileSync(NodePath.join(root, file), "utf8"));
        if (accept(value)) {
          watcher.close();
          resolve(value);
        }
      } catch {
        /* The producer has not published yet. */
      }
    };
    watcher.once("error", (error) => {
      watcher.close();
      reject(error);
    });
    if (!fresh) check();
  });
}

const Probe = Schema.Struct({
  coordinatorCalls: Schema.Number,
  childCalls: Schema.Number,
  frontierEntered: Schema.Number,
  sessionId: Schema.String,
  sessionFile: Schema.String,
  argv: Schema.Array(Schema.String),
  pid: Schema.Number,
  rows: Schema.Array(
    Schema.Struct({
      id: Schema.String,
      outcome: Schema.Boolean,
      resultTaken: Schema.Boolean,
      live: Schema.Boolean,
    }),
  ),
  roles: Schema.Array(Schema.Array(Schema.String)),
});
export const decodeProbe = Schema.decodeUnknownSync(Probe);

export function extensionSource(root: string, pitako: string, spawnWorkers = true, extendedReads = false, publicMode = false) {
  const literal = JSON.stringify;
  return `
import { writeFileSync, renameSync, readFileSync, existsSync, watch } from "node:fs";
import { createAssistantMessageEventStream } from ${literal(NodePath.join(pitako, "node_modules/@earendil-works/pi-ai/dist/utils/event-stream.js"))};
import { listObservations, subscribeChanges } from ${literal(NodePath.join(pitako, "extensions/agent/observe.ts"))};
import { teamEvaluationForSession } from ${literal(NodePath.join(pitako, "extensions/team.ts"))};
${publicMode ? `
import { createPiExecutor } from ${literal(NodePath.join(pitako, "extensions/agent/pi.ts"))};
import { setBackgroundExecutor } from ${literal(NodePath.join(pitako, "extensions/agent/background.ts"))};
import { createAgentSession, ModelRuntime } from ${literal(NodePath.join(pitako, "node_modules/@earendil-works/pi-coding-agent/dist/index.js"))};
` : ""}
const root = ${literal(root)};
let coordinatorCalls = 0, childCalls = 0, frontierEntered = 0, context, watcher, unsubscribe, release, releaseFrontier;
const released = new Promise(resolve => { release = resolve; });
const frontier = new Promise(resolve => { releaseFrontier = resolve; });
const consumed = [];
${publicMode ? `
const gates = new Map(), sessions = [];
let operations = Promise.resolve(), update;
function gate(name) {
  if (existsSync(root + "/" + name) || existsSync(root + "/release")) return Promise.resolve();
  if (!gates.has(name)) {
    let resolve;
    const promise = new Promise(done => { resolve = done; });
    gates.set(name, { promise, resolve });
  }
  return gates.get(name).promise;
}
function witness(name) {
  const value = sessions.map(session => ({ sessionId: session.sessionId, file: session.sessionManager.getSessionFile(),
    head: session.sessionManager.getLeafId(), entries: session.sessionManager.getEntries() }));
  writeFileSync(root + "/" + name + ".tmp", JSON.stringify(value));
  renameSync(root + "/" + name + ".tmp", root + "/" + name);
}
setBackgroundExecutor(createPiExecutor({ factories: {
  createRuntime: async options => { await gate("launch"); return ModelRuntime.create(options); },
  createSession: async options => {
    const created = await createAgentSession(options);
    sessions.push(created.session);
    const dispose = created.session.dispose.bind(created.session);
    created.session.dispose = async () => {
      witness("disposal.json"); await gate("close-capture"); await dispose();
      witness("closed.json");
    };
    return created;
  },
} }));
` : ""}
function publish() {
  if (!context) return;
  const sessionId = context.sessionManager.getSessionId();
  const evaluation = teamEvaluationForSession(sessionId, false);
  const observations = listObservations(evaluation?.token, sessionId);
  const bag = globalThis[Symbol.for("pitako.backgroundWorkers")];
  const rows = observations.map(item => {
    const row = bag?.rows.get(item.id);
    return { id: item.id, outcome: !!row?.outcome, live: !!row?.live, resultTaken: !!item.resultTaken };
  });
  const roles = [...(globalThis[Symbol.for("pitako.teamRegistry")]?.evaluations.get(sessionId)?.roles ?? [])];
  const state = { coordinatorCalls, childCalls, frontierEntered, sessionId, sessionFile: context.sessionManager.getSessionFile(),
    argv: process.argv, pid: process.pid, rows, roles };
  writeFileSync(root + "/state.json.tmp", JSON.stringify(state));
  renameSync(root + "/state.json.tmp", root + "/state.json");
}
globalThis[Symbol.for("t3.pitako.fixture")] = { released, frontier, publish,
  ${publicMode ? `gate, witness, update: fn => { update = fn; },` : ""}
  child: () => { childCalls++; publish(); },
  frontierEnter: () => { frontierEntered++; publish(); },
  consumed: (messages) => {
    consumed.push(messages);
    writeFileSync(root + "/consumed.json.tmp", JSON.stringify(consumed));
    renameSync(root + "/consumed.json.tmp", root + "/consumed.json");
  } };
export default function(pi) {
  const model = { id: "controlled", name: "Offline controlled", compat: { supportsOpenAIGrammarTools: true },
    reasoning: false, input: ["text"], contextWindow: 1000000, maxTokens: 128,
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } };
  pi.registerProvider("t3-fixture", {
    api: "openai-responses", baseUrl: "http://127.0.0.1", apiKey: "fixture", models: [model],
    streamSimple(model) {
      coordinatorCalls++; publish();
      const stream = createAssistantMessageEventStream();
      const content = ${spawnWorkers} && coordinatorCalls === 1 ? [
        { type: "toolCall", id: "spawn-background", name: "agent_spawn", arguments: { role: "researcher", task: ${JSON.stringify(`Offline background fixture${extendedReads ? " brief 😀".repeat(200) : ""}`)} } },
        { type: "toolCall", id: "assign-team", name: "team_assign", arguments: { role: "researcher", task: "Offline Team fixture" } }
      ] : [{ type: "text", text: "Coordinator idle; native children continue." }];
      const message = { role: "assistant", content, api: model.api, provider: model.provider, model: model.id,
        stopReason: ${spawnWorkers} && coordinatorCalls === 1 ? "toolUse" : "stop", timestamp: Date.now(),
        usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, totalTokens: 2,
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } } };
      queueMicrotask(async () => {
        ${publicMode ? `if (coordinatorCalls > 1) { witness("coordinator-waiting.json"); await gate("coordinator"); }` : ""}
        stream.push({ type: "start", partial: { ...message, content: [] } });
        for (const [contentIndex, block] of content.entries()) {
          if (block.type === "toolCall") {
            stream.push({ type: "toolcall_start", contentIndex, partial: message });
            stream.push({ type: "toolcall_end", contentIndex, toolCall: block, partial: message });
          } else {
            stream.push({ type: "text_start", contentIndex, partial: message });
            stream.push({ type: "text_delta", contentIndex, delta: block.text, partial: message });
            stream.push({ type: "text_end", contentIndex, content: block.text, partial: message });
          }
        }
        stream.push({ type: "done", reason: message.stopReason, message }); stream.end(message);
      });
      return stream;
    }
  });
  pi.on("session_start", (_event, ctx) => {
    context = ctx;
    unsubscribe = subscribeChanges(publish);
    watcher = watch(root, (_event, file) => {
      ${publicMode ? `
      gates.get(String(file))?.resolve();
      if (String(file) === "release") for (const pending of gates.values()) pending.resolve();
      if (String(file) === "command.json") operations = operations.then(async () => {
        const command = JSON.parse(readFileSync(root + "/command.json", "utf8"));
        const session = sessions[0];
        if (command.action === "branch") await session.navigateTree(command.head);
        if (command.action === "burst") {
          if (!update) throw Error("No live native update callback");
          for (let i = 0; i < 40; i++) update({ content: [{ type: "text", text: String(i) + "漢😀".repeat(30000) }] });
        }
        witness(command.id + ".json");
      }).catch(error => {
        writeFileSync(root + "/command-error.json", JSON.stringify({ message: String(error) }));
      });
      ` : ""}
      if (String(file) === "release") release();
      if (String(file) === "frontier") releaseFrontier();
      if (String(file) === "snapshot") publish();
    });
    publish();
  });
  pi.on("agent_settled", publish);
  pi.on("session_shutdown", async () => {
    release(); releaseFrontier();
    ${publicMode ? `for (const pending of gates.values()) pending.resolve(); await operations;` : ""}
    watcher?.close(); unsubscribe?.();
  });
}
`;
}

export function workerSource(pitako: string, controls = false, publicMode = false) {
  if (publicMode) return `
import { createAssistantMessageEventStream } from ${JSON.stringify(NodePath.join(pitako, "node_modules/@earendil-works/pi-ai/dist/utils/event-stream.js"))};
export default function(pi) {
  let calls = 0;
  const fixture = () => globalThis[Symbol.for("t3.pitako.fixture")];
  pi.registerTool({ name: "fixture_large", label: "Offline live tool", description: "Controlled native updates",
    parameters: { type: "object", properties: { value: { type: "string" } }, required: ["value"] },
    async execute(id, _args, _signal, update) {
      fixture().update(update);
      update({ content: [{ type: "text", text: "native intermediate update" }] });
      fixture().witness(id === "native-tool-7" ? "tool-entered.json" : "evict-tool-entered.json");
      await fixture().gate(id === "native-tool-7" ? "tool-end" : "evict-tool-end");
      return { content: [{ type: "text", text: "native final tool result" }] };
    } });
  pi.registerProvider("t3-worker", {
    api: "openai-responses", baseUrl: "http://127.0.0.1", apiKey: "fixture",
    models: [{ id: "controlled", name: "Offline child", compat: { supportsOpenAIGrammarTools: true },
      reasoning: false, input: ["text"], contextWindow: 1000000, maxTokens: 128,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } }],
    streamSimple(model, context) {
      const f = fixture();
      if (!f) throw Error("Only fixture-owned workers may run");
      const call = ++calls;
      if (call === 1) f.child();
      const team = JSON.stringify(context.messages).includes("Team assignment header:");
      const actions = [
        { action: "create", subject: "first native task" },
        { action: "create", subject: "second native task" },
        { action: "delete", id: 1 },
        { action: "list", status: "pending" },
        { action: "clear" },
        { action: "create", subject: "reused native id" },
      ];
      const tool = !team && call <= actions.length + 2;
      const content = tool ? [{ type: "toolCall", id: "native-tool-" + call,
        name: call <= actions.length ? "todo" : "fixture_large",
        arguments: call <= actions.length ? actions[call - 1] : { value: call === 7 ? '漢😀"\\\\\\n'.repeat(9000) : "evict prior tool" } }]
        : [{ type: "text", text: "native result 😀".repeat(200) }];
      const message = { role: "assistant", content, api: model.api, provider: model.provider, model: model.id,
        stopReason: tool ? "toolUse" : "stop", timestamp: Date.now(),
        usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, totalTokens: 2,
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } } };
      const stream = createAssistantMessageEventStream();
      queueMicrotask(async () => {
        f.witness(team ? "team-waiting.json" : "step-" + call + ".json");
        await f.gate(team ? "team-end" : "step-" + call);
        stream.push({ type: "start", partial: { ...message, content: [] } });
        if (tool) {
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
}
`;
  return `
import { createAssistantMessageEventStream } from ${JSON.stringify(NodePath.join(pitako, "node_modules/@earendil-works/pi-ai/dist/utils/event-stream.js"))};
export default function(pi) {
  let calls = 0;
  pi.registerTool({ name: "fixture_frontier", label: "Offline frontier", description: "Controlled offline tool boundary",
    parameters: { type: "object", properties: {} },
    async execute() {
      const fixture = globalThis[Symbol.for("t3.pitako.fixture")];
      fixture.frontierEnter();
      await fixture.frontier;
      return { content: [{ type: "text", text: "frontier released" }] };
    } });
  pi.registerProvider("t3-worker", {
    api: "openai-responses", baseUrl: "http://127.0.0.1", apiKey: "fixture",
    models: [{ id: "controlled", name: "Offline child", compat: { supportsOpenAIGrammarTools: true },
      reasoning: false, input: ["text"], contextWindow: 1000000, maxTokens: 128,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } }],
    streamSimple(model, context) {
      const fixture = globalThis[Symbol.for("t3.pitako.fixture")];
      if (!fixture) throw Error("Only fixture-owned workers may run");
      fixture.child();
      calls++;
      // Native steering defaults to one queued note per boundary. Keep later
      // tool frontiers available for all three deliberately admitted notes.
      const atFrontier = ${controls} && calls <= 3;
      if (${controls} && calls > 1) fixture.consumed(context.messages);
      const stream = createAssistantMessageEventStream();
      const message = { role: "assistant", content: atFrontier
          ? [{ type: "toolCall", id: "frontier", name: "fixture_frontier", arguments: {} }]
          : [{ type: "text", text: "native result 😀".repeat(200) }],
        api: model.api, provider: model.provider, model: model.id, stopReason: atFrontier ? "toolUse" : "stop", timestamp: Date.now(),
        usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, totalTokens: 2,
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } } };
      queueMicrotask(async () => {
        stream.push({ type: "start", partial: { ...message, content: [] } });
        if (!atFrontier) await fixture.released;
        if (atFrontier) {
          stream.push({ type: "toolcall_start", contentIndex: 0, partial: message });
          stream.push({ type: "toolcall_end", contentIndex: 0, toolCall: message.content[0], partial: message });
        } else {
          stream.push({ type: "text_start", contentIndex: 0, partial: message });
          stream.push({ type: "text_delta", contentIndex: 0, delta: message.content[0].text, partial: message });
          stream.push({ type: "text_end", contentIndex: 0, content: message.content[0].text, partial: message });
        }
        stream.push({ type: "done", reason: message.stopReason, message }); stream.end(message);
      });
      return stream;
    }
  });
}
`;
}

/** Shared private setup only; each consumer owns its process scope and closure. */
export function prepareNativeFixture(spawnWorkers = true) {
    const source = NodeProcess.env.T3_PITAKO_NATIVE_TEST_ROOT;
    assert.isDefined(source);
    const pitako = NodeFS.realpathSync(source!);
    const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-native-pw-"));
    const cancellation = new AbortController();
    const publications = { signal: cancellation.signal, closed: new Set<Promise<void>>() };
    const agentDir = NodePath.join(root, "agent");
    const cwd = NodePath.join(root, "workspace");
    for (const dir of [
      cwd,
      agentDir,
      NodePath.join(root, "temp"),
      NodePath.join(agentDir, "extensions"),
      NodePath.join(agentDir, "pitako"),
    ])
      NodeFS.mkdirSync(dir, { recursive: true, mode: 0o700 });
    NodeFS.writeFileSync(
      NodePath.join(agentDir, "settings.json"),
      JSON.stringify({
        enableInstallTelemetry: false,
        compaction: { enabled: false },
        cacheWarming: "off",
      }),
    );
    NodeFS.writeFileSync(
      NodePath.join(agentDir, "pitako/config.toml"),
      '[model_policies.researcher.primary]\nmodel = "t3-worker/controlled"\nreasoning = "off"\n' +
      "",
    );
    const fixture = NodePath.join(root, "fixture.mjs");
    NodeFS.writeFileSync(fixture, extensionSource(root, pitako, spawnWorkers, false, true));
    NodeFS.writeFileSync(NodePath.join(agentDir, "extensions/worker.js"), workerSource(pitako, false, true));
    const binary = NodePath.join(root, "pi");
    const cli = NodeFS.realpathSync(
      NodePath.join(pitako, "node_modules/@earendil-works/pi-coding-agent/dist/cli.js"),
    );
    NodeFS.writeFileSync(
      binary,
      `#!/bin/sh\nexec ${JSON.stringify(process.execPath)} ${JSON.stringify(cli)} "$@"\n`,
      { mode: 0o700 },
    );
    return { pitako, root, cancellation, publications, agentDir, cwd, fixture, binary, cli };
}
