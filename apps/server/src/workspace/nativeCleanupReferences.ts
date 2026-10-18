import * as NodeUtil from "node:util";
import {
  OrchestrationV2ProviderThreadJson,
  OrchestrationV2RunJson,
  OrchestrationV2SubagentJson,
  type OrchestrationV2AppThread,
  type ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as SqlClient from "effect/sql/SqlClient";

const decodeMember = Schema.decodeUnknownEffect(Schema.fromJsonString(OrchestrationV2SubagentJson));
const decodeProviderThread = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationV2ProviderThreadJson),
);
const decodeRun = Schema.decodeUnknownEffect(Schema.fromJsonString(OrchestrationV2RunJson));

/**
 * Read only native membership and its descendants, not conversation history.
 * The stored child checkpoint is the current owner generation, including retirement
 * writes made by EventSink.writeNativeWorker.
 */
export const readNativeCleanupReferences = Effect.fnUntraced(function* (
  sql: SqlClient.SqlClient,
  threads: ReadonlyArray<
    Pick<
      OrchestrationV2AppThread,
      | "id"
      | "projectId"
      | "archivedAt"
      | "providerInstanceId"
      | "activeProviderThreadId"
      | "lineage"
      | "forkedFrom"
    >
  >,
) {
  const memberRows = yield* sql<{ payload_json: string }>`
    WITH RECURSIVE children(thread_id) AS (
      SELECT child_thread_id FROM orchestration_v2_projection_subagents
      WHERE json_extract(payload_json, '$.nativeWorker') IS NOT NULL
        OR (origin = 'provider_native' AND json_extract(payload_json, '$.driver') = 'pi')
      UNION
      SELECT s.child_thread_id FROM orchestration_v2_projection_subagents s
      JOIN children c ON s.thread_id = c.thread_id
    )
    SELECT payload_json FROM orchestration_v2_projection_subagents
    WHERE json_extract(payload_json, '$.nativeWorker') IS NOT NULL
      OR (origin = 'provider_native' AND json_extract(payload_json, '$.driver') = 'pi')
      OR thread_id IN (SELECT thread_id FROM children)
    ORDER BY subagent_id
  `;
  const providerRows = yield* sql<{ payload_json: string }>`
    SELECT payload_json FROM orchestration_v2_projection_provider_threads
    WHERE json_extract(payload_json, '$.nativeMetadata.nativeWorkerCheckpoint') IS NOT NULL
      OR provider_thread_id IN (
        SELECT json_extract(payload_json, '$.nativeWorker.launch.parentProviderThreadId')
        FROM orchestration_v2_projection_subagents
      )
    ORDER BY provider_thread_id
  `;
  const runRows = yield* sql<{ payload_json: string }>`
    SELECT payload_json FROM orchestration_v2_projection_runs
    WHERE run_id IN (
      SELECT json_extract(payload_json, '$.nativeWorker.launch.runId')
      FROM orchestration_v2_projection_subagents
    )
    ORDER BY run_id
  `;
  const members = yield* Effect.forEach(memberRows, (row) => decodeMember(row.payload_json));
  const providers = yield* Effect.forEach(providerRows, (row) =>
    decodeProviderThread(row.payload_json),
  );
  const runs = yield* Effect.forEach(runRows, (row) => decodeRun(row.payload_json));
  const byThread = new Map(threads.map((thread) => [thread.id, thread]));

  const inspect = (
    threadId: ThreadId,
    ancestors = new Set<ThreadId>(),
  ): {
    readonly observation: boolean;
    readonly reason: string | null;
    readonly evidence: ReadonlyArray<unknown>;
    readonly threadIds: ReadonlyArray<ThreadId>;
  } => {
    const memberships = members.filter((member) => member.childThreadId === threadId);
    const child = byThread.get(threadId);
    const checkpoints = providers.filter(
      (provider) =>
        provider.appThreadId === threadId &&
        provider.nativeMetadata?.nativeWorkerCheckpoint !== undefined,
    );
    if (
      !memberships.some(
        (member) =>
          member.nativeWorker !== undefined ||
          (member.origin === "provider_native" && member.driver === "pi"),
      ) &&
      checkpoints.length === 0
    )
      return { observation: false, reason: null, evidence: [], threadIds: [threadId] };
    const evidence: unknown[] = [memberships, checkpoints];
    const threadIds = [threadId];
    const blocked = (reason: string) => ({ observation: true, reason, evidence, threadIds });
    if (ancestors.has(threadId)) return blocked("native observation has cyclic descendants");
    if (memberships.length !== 1 || checkpoints.length !== 1)
      return blocked("native observation identity is incomplete or contradictory");
    const member = memberships[0]!;
    const provider = checkpoints[0]!;
    const summary = member.nativeWorker;
    const checkpoint = provider.nativeMetadata!.nativeWorkerCheckpoint!;
    const launch = summary?.launch;
    const parent = launch === undefined ? undefined : byThread.get(launch.parentThreadId);
    const parentProvider = providers.find((row) => row.id === launch?.parentProviderThreadId);
    const run = runs.find(
      (row) => row.id === launch?.runId && row.threadId === launch?.parentThreadId,
    );
    evidence.push(child, parent?.id, parentProvider, run);
    if (
      summary === undefined ||
      launch === undefined ||
      child === undefined ||
      parent === undefined ||
      member.origin !== "provider_native" ||
      member.driver !== "pi" ||
      provider.driver !== "pi" ||
      !NodeUtil.isDeepStrictEqual(checkpoint.launch, launch) ||
      member.threadId !== launch.parentThreadId ||
      member.runId !== launch.runId ||
      member.parentNodeId !== launch.toolNodeId ||
      member.providerThreadId !== provider.id ||
      provider.ownerNodeId !== member.id ||
      provider.appThreadId !== member.childThreadId ||
      provider.providerSessionId !== launch.providerSessionId ||
      provider.providerInstanceId !== launch.providerInstanceId ||
      member.providerInstanceId !== launch.providerInstanceId ||
      child.providerInstanceId !== launch.providerInstanceId ||
      child.projectId !== parent.projectId ||
      child.activeProviderThreadId !== provider.id ||
      child.lineage.parentThreadId !== parent.id ||
      child.lineage.rootThreadId !== parent.lineage.rootThreadId ||
      child.lineage.relationshipToParent !== "subagent" ||
      child.forkedFrom?.type !== "node" ||
      child.forkedFrom.nodeId !== member.id ||
      parentProvider?.appThreadId !== parent.id ||
      parentProvider.driver !== "pi" ||
      parentProvider.providerSessionId !== launch.providerSessionId ||
      parentProvider.providerInstanceId !== launch.providerInstanceId ||
      run?.providerThreadId !== launch.parentProviderThreadId ||
      run.providerInstanceId !== launch.providerInstanceId ||
      run.activeAttemptId !== launch.runAttemptId ||
      checkpoint.owner.coordinatorSessionId !== launch.coordinatorSessionId
    )
      return blocked("native observation identity is incomplete or contradictory");
    if (
      !["completed", "failed", "cancelled"].includes(summary.execution ?? "") ||
      summary.capture !== "closed" ||
      summary.availability !== "ready" ||
      summary.gaps.length > 0 ||
      checkpoint.capture !== "closed" ||
      checkpoint.gaps.length > 0 ||
      checkpoint.fragment !== undefined ||
      checkpoint.recovery ||
      checkpoint.captureEpoch === null ||
      checkpoint.finalSequence === null ||
      checkpoint.bindings.length === 0 ||
      new Set(checkpoint.bindings.map((binding) => binding.generation)).size !==
        checkpoint.bindings.length ||
      checkpoint.bindings.some(
        (binding) =>
          binding.state !== "historical" ||
          binding.capture !== "closed" ||
          binding.gaps.length > 0 ||
          checkpoint.bindingCursors.filter((cursor) => cursor.generation === binding.generation)
            .length !== 1,
      ) ||
      checkpoint.bindingCursors.some(
        (cursor) =>
          cursor.more ||
          cursor.recovery ||
          cursor.fragment !== undefined ||
          cursor.omission !== undefined ||
          !checkpoint.bindings.some((binding) => binding.generation === cursor.generation),
      ) ||
      !checkpoint.bindingCursors.some(
        (cursor) =>
          cursor.generation === checkpoint.bindingGeneration &&
          cursor.cursor === checkpoint.cursor &&
          cursor.cut === checkpoint.cut,
      )
    )
      return blocked("native observation capture or execution is not settled");
    const descendants = members.filter((row) => row.threadId === threadId);
    const childThreads = threads.filter(
      (row) =>
        row.lineage.parentThreadId === threadId && row.lineage.relationshipToParent === "subagent",
    );
    evidence.push(descendants, childThreads);
    if (descendants.some((descendant) => descendant.childThreadId === null))
      return blocked("native observation descendant is incomplete");
    const descendantIds = new Set([
      ...childThreads.map((row) => row.id),
      ...descendants.flatMap((row) => (row.childThreadId === null ? [] : [row.childThreadId])),
    ]);
    const next = new Set([...ancestors, threadId]);
    for (const descendantId of descendantIds) {
      const result = inspect(descendantId, next);
      evidence.push(result.evidence);
      threadIds.push(...result.threadIds);
      if (!result.observation || result.reason !== null)
        return blocked(result.reason ?? "native observation descendant is an independent owner");
    }
    return { observation: true, reason: null, evidence, threadIds };
  };
  return inspect;
});
