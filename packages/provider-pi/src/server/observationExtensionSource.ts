// Runs inside Pi: only public extension events and the versioned host registry cross this boundary.
export const PI_SUBAGENT_OBSERVATION_SOURCE = String.raw`
function installSubagentObservation(pi: ExtensionAPI) {
  type Provider = { name: string; sessionId: string; sessionFile?: string; isActive(): boolean };
  const providers = new Set<Provider>();
  const registryKey = Symbol.for("@agegr/pi-web/session-liveness/v1");
  const globals = globalThis as unknown as { [key: symbol]: unknown };
  const registry = {
    version: 1,
    register(provider: Provider) {
      providers.add(provider);
      return () => { providers.delete(provider); };
    },
  };
  const ownsRegistry = globals[registryKey] === undefined;
  if (ownsRegistry) globals[registryKey] = registry;
  let context: ExtensionContext | undefined;
  let sequence = 0;
  let refreshing = false;
  let refreshAgain = false;
  const lifecycleHints = new Map<string, Record<string, string>>();
  const rememberLifecycleHint = (value: unknown, child: boolean) => {
    if (context === undefined || !value || typeof value !== "object") return;
    const source = value as Record<string, unknown>;
    if (child && (source.type !== "subagent.child-status" || source.version !== 1)) return;
    const hint: Record<string, string> = {};
    for (const field of ["runId", "childId", "childRunId", "workflowKey", "asyncDir"]) {
      const text = field === "runId" && !child ? source.id : source[field];
      if (typeof text === "string" && text.length > 0 && text.length <= (field === "asyncDir" ? 4096 : 256) && !/[\u0000-\u001f]/.test(text)) hint[field] = text;
    }
    if (!hint.runId || !hint.asyncDir || (child && (!hint.childId || !hint.childRunId || !hint.workflowKey))) return;
    const key = JSON.stringify([hint.runId, hint.childRunId]);
    lifecycleHints.delete(key);
    lifecycleHints.set(key, hint);
    while (lifecycleHints.size > 64 || new TextEncoder().encode(JSON.stringify([...lifecycleHints.values()])).byteLength > 32 * 1024) lifecycleHints.delete(lifecycleHints.keys().next().value!);
  };

  const liveness = () => {
    if (!ownsRegistry || context === undefined) return "unavailable";
    const ids = new Set([context.sessionManager.getSessionId(), context.sessionManager.getSessionFile()]);
    const scoped = [...providers].filter((provider) =>
      ids.has(provider.sessionId) || (provider.sessionFile !== undefined && ids.has(provider.sessionFile)));
    if (scoped.length === 0) return "unavailable";
    try {
      return scoped.some((provider) => provider.isActive()) ? "active" : "inactive";
    } catch {
      return "unavailable";
    }
  };

  const refresh = async () => {
    if (context === undefined) return;
    if (refreshing) { refreshAgain = true; return; }
    refreshing = true;
    do {
      refreshAgain = false;
      const owner = context;
      const requestId = "t3-observe-" + (++sequence);
      await new Promise<void>((resolve) => {
        const finish = () => { clearTimeout(timer); unsubscribe(); resolve(); };
        const unsubscribe = pi.events.on("subagents:rpc:v1:reply:" + requestId, (value: unknown) => {
          const reply = value as { version?: number; success?: boolean; data?: { asyncSnapshot?: Record<string, unknown> } };
          if (context === owner && reply.version === 1 && reply.success && reply.data?.asyncSnapshot) {
            const snapshot = { ...reply.data.asyncSnapshot, lifecycleHints: [...lifecycleHints.values()] };
            let serialized = JSON.stringify(snapshot);
            if (new TextEncoder().encode(serialized).byteLength > 256 * 1024 - "PI_SUBAGENT_ASYNC_JSON:".length) serialized = JSON.stringify({ ...snapshot, lifecycleHints: [] });
            owner.ui.setWidget("t3-subagents", ["PI_SUBAGENT_ASYNC_JSON:" + serialized]);
          }
          finish();
        });
        const timer = setTimeout(finish, 5_000);
        pi.events.emit("subagents:rpc:v1:request", { version: 1, requestId, method: "status", params: {} });
      });
    } while (refreshAgain && context !== undefined);
    refreshing = false;
  };

  pi.on("session_start", (_event, ctx) => {
    lifecycleHints.clear();
    context = ctx;
    ctx.ui.setWidget("t3-subagent-liveness", [JSON.stringify({ version: 1, ready: true })]);
    void refresh();
  });
  pi.on("session_shutdown", () => { context = undefined; refreshAgain = false; lifecycleHints.clear(); });
  pi.on("agent_settled", () => { void refresh(); });
  pi.on("tool_result", (event) => { if (event.toolName === "subagent") void refresh(); });
  for (const name of ["subagents:rpc:v1:ready", "subagent:child-status", "subagent:async-started", "subagent:async-complete"]) {
    pi.events.on(name, (value: unknown) => {
      if (name === "subagent:child-status" || name === "subagent:async-started") rememberLifecycleHint(value, name === "subagent:child-status");
      void refresh();
    });
  }
  pi.registerCommand("t3-subagent-liveness", {
    description: "Read T3's native subagent residency observation without a model turn.",
    handler: async (requestId, ctx) => {
      context = ctx;
      ctx.ui.setWidget("t3-subagent-liveness", [JSON.stringify({
        version: 1, requestId, status: liveness(),
      })]);
      void refresh();
    },
  });
}
`;
