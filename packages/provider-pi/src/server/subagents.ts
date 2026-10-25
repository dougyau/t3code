import type { OrchestrationV2Subagent } from "@t3tools/contracts";
import {
  piRecordField as recordField,
  piRecordNumber as recordNumber,
  piRecordString as recordString,
} from "./rpc.ts";

export interface PiSubagentObservation {
  readonly id: string;
  readonly runId: string;
  readonly childId: string;
  readonly resultPosition?: number;
  readonly inspectionTarget?: { readonly runId: string; readonly childId: string };
  readonly parentId: string | null;
  readonly title: string;
  readonly prompt?: string | undefined;
  readonly model?: string | undefined;
  readonly status: OrchestrationV2Subagent["status"];
  readonly paused?: boolean;
  readonly partial?: boolean;
  readonly result?: string | undefined;
  readonly progress?: string | undefined;
  readonly updatedAt?: number | undefined;
  readonly observedAt?: number | undefined;
}

export const piSubagentObservationId = (runId: string, childId: string) =>
  JSON.stringify([runId, childId]);

export const PI_SUBAGENT_LIFECYCLE_MAX_BYTES = 64 * 1024;

export function piSubagentMetadataString(value: unknown, field: string, limit = 256) {
  const text = recordString(value, field);
  return text && text.length <= limit && !/[\u0000-\u001f]/.test(text) ? text : undefined;
}

export interface PiSubagentLifecycle {
  readonly runId: string;
  readonly mode: "single" | "workflow" | "chain" | "parallel";
  readonly parentWorkflowRunId?: string | undefined;
  readonly workflowKey?: string | undefined;
  readonly steps: ReadonlyArray<{
    runId?: string | undefined;
    workflowKey?: string | undefined;
    childId: string;
  }>;
}

/** Lifecycle metadata proves identity only; partial files never prove absence or settlement. */
export function piSubagentLifecycle(text: string, runId: string): PiSubagentLifecycle | undefined {
  if (
    text.length > PI_SUBAGENT_LIFECYCLE_MAX_BYTES ||
    new TextEncoder().encode(text).byteLength > PI_SUBAGENT_LIFECYCLE_MAX_BYTES
  )
    return undefined;
  let data: unknown;
  try {
    data = JSON.parse(text);
  } catch {
    return undefined;
  }
  let remaining = 4096;
  const bounded = (value: unknown, depth: number): boolean => {
    if (depth > 16 || --remaining < 0) return false;
    return (
      value === null ||
      typeof value !== "object" ||
      Object.values(value).every((child) => bounded(child, depth + 1))
    );
  };
  if (
    !bounded(data, 0) ||
    recordField(data, "lifecycleArtifactVersion") !== 3 ||
    recordString(data, "runId") !== runId ||
    (recordString(data, "id") !== undefined && recordString(data, "id") !== runId)
  )
    return undefined;
  const mode = recordString(data, "mode");
  const steps = recordField(data, "steps");
  if (
    (mode !== "single" && mode !== "workflow" && mode !== "chain" && mode !== "parallel") ||
    !Array.isArray(steps) ||
    steps.length > 128
  )
    return undefined;
  return {
    runId,
    mode,
    parentWorkflowRunId: piSubagentMetadataString(data, "parentWorkflowRunId"),
    workflowKey: piSubagentMetadataString(data, "workflowKey"),
    steps: steps.map((step, position) => ({
      runId: piSubagentMetadataString(step, "runId"),
      workflowKey: piSubagentMetadataString(step, "workflowKey"),
      childId:
        piSubagentMetadataString(step, "workflowKey") ??
        piSubagentMetadataString(step, "runId") ??
        `step:${recordNumber(step, "index") ?? position}`,
    })),
  };
}

export function piSubagentLifecycleHints(lines: unknown) {
  const hints = recordField(decodeSubagentSnapshot(lines)?.snapshot, "lifecycleHints");
  if (!Array.isArray(hints)) return [];
  return hints.slice(-64).flatMap((hint) => {
    const runId = piSubagentMetadataString(hint, "runId");
    const asyncDir = piSubagentMetadataString(hint, "asyncDir", 4096);
    if (runId === undefined || asyncDir === undefined) return [];
    return [
      {
        runId,
        asyncDir,
        childRunId: piSubagentMetadataString(hint, "childRunId"),
        childId: piSubagentMetadataString(hint, "childId"),
        workflowKey: piSubagentMetadataString(hint, "workflowKey"),
      },
    ];
  });
}

function resultText(result: unknown): string | undefined {
  const error = recordString(result, "errorMessage");
  if (error) return error.slice(0, 10_000);
  const stopReason = recordString(result, "stopReason");
  const stderr = recordString(result, "stderr");
  if (
    stderr &&
    ((recordNumber(result, "exitCode") ?? 0) !== 0 ||
      stopReason === "error" ||
      stopReason === "aborted")
  )
    return stderr.slice(0, 10_000);
  const finalOutput = recordString(result, "finalOutput");
  if (finalOutput !== undefined) return finalOutput.slice(0, 10_000);
  const messages = recordField(result, "messages");
  if (Array.isArray(messages)) {
    for (let index = messages.length - 1; index >= 0; index -= 1) {
      const message = messages[index];
      if (recordString(message, "role") !== "assistant") continue;
      const content = recordField(message, "content");
      const text = Array.isArray(content)
        ? content
            .filter((part) => recordField(part, "type") === "text")
            .map((part) => recordString(part, "text") ?? "")
            .join("\n")
        : "";
      if (text) return text.slice(0, 10_000);
    }
  }
  return recordString(result, "output")?.slice(0, 10_000);
}

export function piSubagentResults(
  result: unknown,
  fallbackRunId: string,
  completed: boolean,
): ReadonlyArray<PiSubagentObservation> {
  const details = recordField(result, "details");
  const results = recordField(details, "results");
  if (!Array.isArray(results)) return [];
  const outerRunId =
    recordString(details, "runId") ?? recordString(details, "asyncId") ?? fallbackRunId;
  const observations = results.slice(0, 500).flatMap((row, position): PiSubagentObservation[] => {
    const title = recordString(row, "agent");
    const prompt = recordString(row, "task");
    if (title === undefined || prompt === undefined) return [];
    const index = recordNumber(row, "index") ?? recordNumber(row, "step") ?? position;
    const runId = recordString(row, "runId") ?? outerRunId;
    const workflowKey = recordString(row, "workflowKey");
    const childId =
      workflowKey !== undefined && runId === outerRunId
        ? JSON.stringify(["workflow", workflowKey, index])
        : `step:${index}`;
    const progressStatus = recordString(recordField(row, "progress"), "status");
    // Detached foreground completion arrives through onUpdate, not a second tool end.
    const finished =
      recordField(row, "detached") !== true &&
      (progressStatus === "completed" ||
        progressStatus === "failed" ||
        recordField(row, "finished") === true ||
        (recordField(row, "finished") !== false &&
          completed &&
          (recordNumber(row, "exitCode") !== undefined ||
            recordString(row, "stopReason") !== undefined)));
    const stopReason = recordString(row, "stopReason");
    const paused = recordField(row, "interrupted") === true;
    const status = paused
      ? "idle"
      : !finished
        ? "running"
        : stopReason === "aborted"
          ? "interrupted"
          : progressStatus === "failed" ||
              stopReason === "error" ||
              (recordNumber(row, "exitCode") ?? 0) !== 0
            ? "failed"
            : "completed";
    const text = resultText(row);
    return [
      {
        id: piSubagentObservationId(runId, childId),
        runId,
        childId,
        resultPosition: position,
        parentId: null,
        title,
        prompt,
        model: recordString(row, "model"),
        status,
        paused,
        ...(finished || paused ? { result: text } : { progress: text?.slice(0, 200) }),
      },
    ];
  });
  const seen = new Set<unknown>();
  const ids = new Set(observations.map((observation) => observation.id));
  let remaining = 500 - observations.length;
  const visit = (children: unknown, parent: PiSubagentObservation, depth: number) => {
    if (!Array.isArray(children) || depth > 16) return;
    for (const child of children.slice(0, 500)) {
      if (remaining-- <= 0) return;
      const runId = piSubagentMetadataString(child, "id");
      const parentStep = recordNumber(child, "parentStepIndex");
      if (
        seen.has(child) ||
        runId === undefined ||
        recordString(child, "parentRunId") !== parent.runId ||
        parent.childId !== `step:${parentStep}`
      )
        continue;
      seen.add(child);
      const steps = recordField(child, "steps");
      if (!Array.isArray(steps)) continue;
      for (const [index, step] of steps.slice(0, 128).entries()) {
        if (remaining-- <= 0) return;
        const title = piSubagentMetadataString(step, "agent");
        const status = snapshotStatus(recordField(step, "status"), recordNumber(step, "endedAt"));
        if (title === undefined || status === undefined) continue;
        const childId = `step:${index}`;
        const id = piSubagentObservationId(runId, childId);
        if (ids.has(id)) continue;
        ids.add(id);
        const observation: PiSubagentObservation = {
          id,
          runId,
          childId,
          parentId: parent.id,
          title,
          status,
          model: recordString(step, "model"),
          paused: recordField(step, "status") === "paused",
          partial: recordField(step, "status") === "partial",
          progress: recordString(step, "currentTool"),
          updatedAt: recordNumber(step, "endedAt") ?? recordNumber(step, "lastActivityAt"),
        };
        observations.push(observation);
        visit(recordField(step, "children"), observation, depth + 1);
        // The producer may also publish the same descendants on the run summary.
        visit(recordField(child, "children"), observation, depth + 1);
      }
    }
  };
  for (const observation of [...observations]) {
    if (observation.resultPosition !== undefined)
      visit(recordField(results[observation.resultPosition], "children"), observation, 0);
  }
  const summary = recordField(details, "workflowChildren");
  const children = recordField(summary, "children");
  if (
    recordString(details, "mode") === "workflow" &&
    recordField(summary, "version") === 1 &&
    recordString(summary, "workflowRunId") === outerRunId &&
    recordString(summary, "parentToolCallId") === fallbackRunId &&
    Array.isArray(children)
  ) {
    for (const child of children.slice(0, 128)) {
      const runId = piSubagentMetadataString(child, "runId");
      const title = piSubagentMetadataString(child, "agent");
      const key = piSubagentMetadataString(child, "childId", 128);
      const state = recordString(child, "state");
      if (
        runId === undefined ||
        title === undefined ||
        key === undefined ||
        !/^[A-Za-z0-9][A-Za-z0-9._-]*$/.test(key)
      )
        continue;
      const status = snapshotStatus(state === "completed" ? "complete" : state);
      if (status === undefined) continue;
      // Public workflow members with an agent are individual runs; their final result uses local index zero.
      const childId = "step:0";
      const id = piSubagentObservationId(runId, childId);
      const resultIndex = observations.findIndex((observation) => observation.runId === runId);
      if (resultIndex !== -1) {
        if (state === "paused")
          observations[resultIndex] = { ...observations[resultIndex]!, status, paused: true };
        continue;
      }
      const currentTool = piSubagentMetadataString(recordField(child, "activity"), "currentTool");
      observations.push({
        id,
        runId,
        childId,
        parentId: null,
        title,
        status,
        paused: state === "paused",
        model: piSubagentMetadataString(child, "model"),
        ...(currentTool === undefined ? {} : { progress: currentTool }),
      });
    }
  }
  return observations;
}

function snapshotStatus(
  value: unknown,
  endedAt?: number,
): PiSubagentObservation["status"] | undefined {
  switch (value) {
    case "queued":
      return "pending";
    case "running":
      return "running";
    case "complete":
      return "completed";
    case "failed":
    case "rejected":
      return "failed";
    case "stopped":
      return "interrupted";
    case "paused":
      // Native paused runs have settled; any live descendants are observed separately.
      return "idle";
    case "partial":
      // Ended partial work is inactive, but neither success nor physical exit is proved.
      return endedAt === undefined ? undefined : "idle";
    default:
      return undefined;
  }
}

function decodeSubagentSnapshot(lines: unknown) {
  if (!Array.isArray(lines)) return undefined;
  const line = lines.find(
    (value): value is string =>
      typeof value === "string" && value.startsWith("PI_SUBAGENT_ASYNC_JSON:"),
  );
  if (
    line === undefined ||
    line.length > 256 * 1024 ||
    new TextEncoder().encode(line).byteLength > 256 * 1024
  )
    return undefined;
  let snapshot: unknown;
  try {
    snapshot = JSON.parse(line.slice("PI_SUBAGENT_ASYNC_JSON:".length));
  } catch {
    return undefined;
  }
  if (
    recordField(snapshot, "kind") !== "pi-subagents.async-status-snapshot" ||
    recordField(snapshot, "version") !== 1
  )
    return undefined;
  const runs = recordField(snapshot, "runs");
  return Array.isArray(runs) ? { snapshot, runs } : undefined;
}

export function piSubagentWorkflowRunIds(lines: unknown): ReadonlySet<string> {
  const workflows = new Set<string>();
  let remaining = 500;
  const visit = (node: unknown, depth: number) => {
    if (depth > 16 || remaining-- <= 0) return;
    const id = recordString(node, "id");
    if (recordString(node, "kind") === "workflow" && id !== undefined) workflows.add(id);
    const children = recordField(node, "children");
    if (Array.isArray(children)) for (const child of children) visit(child, depth + 1);
  };
  for (const run of decodeSubagentSnapshot(lines)?.runs ?? []) visit(run, 0);
  return workflows;
}

export function piSubagentRunStates(
  lines: unknown,
): ReadonlyMap<string, PiSubagentObservation["status"]> {
  const decoded = decodeSubagentSnapshot(lines);
  const states = new Map<string, PiSubagentObservation["status"]>();
  if (decoded === undefined) return states;
  let remaining = 500;
  const visit = (node: unknown, depth: number) => {
    if (depth > 16 || remaining-- <= 0) return;
    const kind = recordString(node, "kind");
    if (kind === "host-step") return;
    const id = recordString(node, "id");
    const status = snapshotStatus(recordField(node, "state"), recordNumber(node, "endedAt"));
    if ((kind === "subagent" || kind === "workflow") && id !== undefined && status !== undefined)
      states.set(id, status);
    const children = recordField(node, "children");
    if (Array.isArray(children)) for (const child of children) visit(child, depth + 1);
  };
  for (const run of decoded.runs) visit(run, 0);
  return states;
}

/** Widgets are bounded, partial observations. Missing rows never terminalize an agent. */
export function piSubagentSnapshot(lines: unknown): ReadonlyArray<PiSubagentObservation> {
  const decoded = decodeSubagentSnapshot(lines);
  if (decoded === undefined) return [];
  const { snapshot, runs } = decoded;
  const observations: PiSubagentObservation[] = [];
  let remaining = 500;
  const visit = (node: unknown, runId: string, parentId: string | null, depth: number) => {
    if (depth > 16 || remaining-- <= 0) return;
    const kind = recordString(node, "kind");
    if (kind === "host-step") return;
    const childId = recordString(node, "id");
    if (childId === undefined) return;
    const children = recordField(node, "children");
    // Run roots precede their self-steps and may also group multiple agents.
    const isAgent = kind === "step";
    const status = snapshotStatus(recordField(node, "state"), recordNumber(node, "endedAt"));
    let nextParent = parentId;
    if (
      isAgent &&
      status !== undefined &&
      (kind !== "step" || recordNumber(node, "startedAt") !== undefined || status === "running")
    ) {
      const id = piSubagentObservationId(runId, childId);
      observations.push({
        id,
        runId,
        childId,
        parentId,
        title: recordString(node, "agent") ?? recordString(node, "label") ?? "Subagent",
        model: recordString(node, "model"),
        status,
        paused: recordField(node, "state") === "paused",
        partial: recordField(node, "state") === "partial",
        progress: recordString(recordField(node, "activity"), "currentTool"),
        updatedAt: recordNumber(node, "updatedAt"),
        observedAt: recordNumber(snapshot, "generatedAt"),
      });
      nextParent = id;
    }
    if (!Array.isArray(children)) return;
    for (const child of children) {
      const childKind = recordString(child, "kind");
      const nestedRunId =
        childKind === "subagent" || childKind === "workflow"
          ? (recordString(child, "id") ?? runId)
          : runId;
      visit(child, nestedRunId, nextParent, depth + 1);
    }
  };
  for (const run of runs) {
    const runId = recordString(run, "id");
    if (runId !== undefined) visit(run, runId, null, 0);
  }
  const byId = new Map<string, PiSubagentObservation | null>();
  for (const observation of observations) {
    const prior = byId.get(observation.id);
    if (prior === null) continue;
    if (prior === undefined || prior.parentId === null || prior.parentId === observation.parentId) {
      byId.set(observation.id, observation);
    } else if (observation.parentId !== null) {
      // Conflicting explicit parents cannot establish ownership.
      byId.set(observation.id, null);
    }
  }
  const ordered: PiSubagentObservation[] = [];
  const visited = new Set<string>();
  const append = (observation: PiSubagentObservation) => {
    if (visited.has(observation.id)) return;
    visited.add(observation.id);
    const parent = observation.parentId === null ? undefined : byId.get(observation.parentId);
    if (parent) append(parent);
    ordered.push(observation);
  };
  // A summary can precede the owning step; emit its supported parent before creating the child.
  for (const observation of byId.values()) if (observation !== null) append(observation);
  return ordered;
}
