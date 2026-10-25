// @effect-diagnostics nodeBuiltinImport:off -- Exercises the shipped extension inside its native JavaScript boundary.
import * as NodeModule from "node:module";
import * as NodeVM from "node:vm";
import { describe, expect, it } from "vite-plus/test";
import { PI_SUBAGENT_OBSERVATION_SOURCE } from "./observationExtensionSource.ts";
import { piSubagentSnapshot } from "./subagents.ts";

function setup() {
  const listeners = new Map<string, Set<(value: unknown) => void>>();
  const widgets = new Map<string, string[]>();
  const hooks = new Map<string, (event: unknown, ctx: typeof context) => void>();
  const commands = new Map<string, { handler(id: string, ctx: typeof context): Promise<void> }>();
  const context = {
    sessionManager: { getSessionId: () => "session", getSessionFile: () => "/session.jsonl" },
    ui: {
      setWidget: (key: string, lines: string[]) => {
        widgets.set(key, lines);
      },
    },
  };
  const emit = (name: string, value: unknown) => {
    for (const listener of listeners.get(name) ?? []) listener(value);
  };
  const requests: string[] = [];
  const pi = {
    on: (name: string, handler: (event: unknown, ctx: typeof context) => void) => {
      hooks.set(name, handler);
    },
    registerCommand: (
      name: string,
      command: { handler(id: string, ctx: typeof context): Promise<void> },
    ) => {
      commands.set(name, command);
    },
    events: {
      on: (name: string, listener: (value: unknown) => void) => {
        const set = listeners.get(name) ?? new Set();
        set.add(listener);
        listeners.set(name, set);
        return () => {
          set.delete(listener);
        };
      },
      emit,
    },
  };
  pi.events.on("subagents:rpc:v1:request", (value) => {
    const request = value as { requestId: string; method: string; params: { id: string } };
    requests.push(request.method);
    emit(`subagents:rpc:v1:reply:${request.requestId}`, {
      version: 1,
      success: true,
      data: {
        asyncSnapshot: {
          kind: "pi-subagents.async-status-snapshot",
          version: 1,
          runs: [
            {
              id: "run",
              kind: "workflow",
              state: "running",
              children: [
                {
                  id: "step:0",
                  kind: "step",
                  state: "running",
                  agent: "scout",
                  startedAt: 1,
                },
              ],
            },
          ],
        },
      },
    });
  });
  const realm = NodeVM.createContext({
    pi,
    TextEncoder,
    setTimeout: () => 0,
    clearTimeout: () => {},
  });
  NodeVM.runInContext(
    NodeModule.stripTypeScriptTypes(
      `${PI_SUBAGENT_OBSERVATION_SOURCE}\ninstallSubagentObservation(pi);`,
    ),
    realm,
  );
  const registry = NodeVM.runInContext(
    'globalThis[Symbol.for("@agegr/pi-web/session-liveness/v1")]',
    realm,
  ) as { register(provider: { name: string; sessionId: string; isActive(): boolean }): () => void };
  return { hooks, commands, context, registry, widgets, requests, emit };
}

describe("Pi public subagent observation bridge", () => {
  it("reconciles public status into agent observations without the stock widget or any control method", async () => {
    const bridge = setup();
    bridge.hooks.get("session_start")!({}, bridge.context);
    await bridge.commands.get("t3-subagent-liveness")!.handler("query", bridge.context);
    expect(piSubagentSnapshot(bridge.widgets.get("t3-subagents")).map((row) => row.title)).toEqual([
      "scout",
    ]);
    expect(bridge.requests.length).toBeGreaterThan(0);
    expect(bridge.requests.every((method) => method === "status")).toBe(true);
  });

  it("forwards only bounded discovery fields and clears hints with the session", async () => {
    const bridge = setup();
    bridge.hooks.get("session_start")!({}, bridge.context);
    bridge.emit("subagent:child-status", {
      type: "subagent.child-status",
      version: 1,
      runId: "W",
      childId: "lane",
      childRunId: "C",
      workflowKey: "lane",
      asyncDir: "/discovered/W",
      secret: "not forwarded",
      task: "not forwarded",
    });
    await bridge.commands.get("t3-subagent-liveness")!.handler("query", bridge.context);
    const snapshot = () =>
      JSON.parse(bridge.widgets.get("t3-subagents")![0]!.slice("PI_SUBAGENT_ASYNC_JSON:".length));
    expect(snapshot().lifecycleHints).toEqual([
      {
        runId: "W",
        childId: "lane",
        childRunId: "C",
        workflowKey: "lane",
        asyncDir: "/discovered/W",
      },
    ]);
    bridge.emit("subagent:async-started", {
      id: "C",
      asyncDir: "/discovered/C",
      parentWorkflowRunId: "W",
      workflowKey: "lane",
      task: "not forwarded",
    });
    await bridge.commands.get("t3-subagent-liveness")!.handler("child-dir", bridge.context);
    expect(snapshot().lifecycleHints[1]).toEqual({
      runId: "C",
      asyncDir: "/discovered/C",
      workflowKey: "lane",
    });
    for (let index = 0; index < 70; index++)
      bridge.emit("subagent:async-started", {
        id: `run-${index}`,
        asyncDir: `/d/${index}`,
        task: "not forwarded",
      });
    bridge.emit("subagent:async-started", { id: "oversized", asyncDir: "x".repeat(4097) });
    await bridge.commands.get("t3-subagent-liveness")!.handler("bounded", bridge.context);
    expect(snapshot().lifecycleHints).toHaveLength(64);
    expect(
      snapshot().lifecycleHints.some((hint: { runId: string }) => hint.runId === "oversized"),
    ).toBe(false);
    bridge.hooks.get("session_shutdown")!({}, bridge.context);
    bridge.hooks.get("session_start")!({}, bridge.context);
    await bridge.commands.get("t3-subagent-liveness")!.handler("new-session", bridge.context);
    expect(snapshot().lifecycleHints).toEqual([]);
  });

  it("reports fresh session-scoped residency, and unavailable after provider release", async () => {
    const bridge = setup();
    let active = true;
    const release = bridge.registry.register({
      name: "pi-subagents",
      sessionId: "session",
      isActive: () => active,
    });
    bridge.registry.register({ name: "other", sessionId: "another-session", isActive: () => true });
    const command = bridge.commands.get("t3-subagent-liveness")!;
    const status = () => JSON.parse(bridge.widgets.get("t3-subagent-liveness")![0]!).status;
    await command.handler("first", bridge.context);
    expect(status()).toBe("active");
    active = false;
    await command.handler("second", bridge.context);
    expect(status()).toBe("inactive");
    release();
    await command.handler("third", bridge.context);
    expect(status()).toBe("unavailable");
  });
});
