import * as NodeCrypto from "@effect/platform-node-shared/NodeCrypto";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Hex from "effect/encoding/Hex";
import type {
  PiArchitectureDetailRecord,
  PiArchitectureReadInput,
  PiArchitectureReadResult,
  PiArchitectureRecord,
  OrchestrationV2Subagent,
} from "@t3tools/contracts";
import type { ArchitectureArchive } from "./ArchitectureArchive.ts";

export class ArchitectureQueryError extends Schema.TaggedError<ArchitectureQueryError>()(
  "ArchitectureQueryError",
  { cause: Schema.Defect() },
) {}
const isQueryError = Schema.is(ArchitectureQueryError);
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const textId = (record: PiArchitectureRecord) => `${record.kind}:${record.id}`;
const bindings = (record: Extract<PiArchitectureRecord, { kind: "member" }>) =>
  record.conversationRefs.flatMap((ref) => (ref.native ? [ref.native] : []));

function detail(
  record: PiArchitectureRecord,
  archive: ArchitectureArchive,
  workers: ReadonlyArray<OrchestrationV2Subagent>,
): PiArchitectureDetailRecord {
  const id = record.id;
  switch (record.kind) {
    case "room":
      return {
        kind: record.kind,
        id,
        mode: record.mode,
        lifecycle: record.lifecycle,
        pendingDoubtIds: record.pendingDoubtIds,
        originalTextId: "original",
      };
    case "member": {
      const worker = workers.find(
        (worker) =>
          worker.nativeWorker?.launch.providerInstanceId === archive.summary.providerInstanceId &&
          worker.nativeWorker.launch.coordinatorSessionId ===
            archive.summary.coordinatorSessionId &&
          worker.nativeWorker.launch.instanceId === record.instanceId,
      );
      const native = worker?.nativeWorker;
      return {
        kind: record.kind,
        id,
        assignmentId: record.assignmentId,
        instanceId: record.instanceId,
        roleId: record.roleId,
        function: record.function,
        ordinal: record.ordinal,
        execution: record.execution,
        mandateSource: record.mandate.source,
        textId: textId(record),
        bindingCount: bindings(record).length,
        workerThreadId: worker?.childThreadId ?? null,
        ...(native?.selected || native?.activated
          ? {
              model: {
                ...(native.selected ? { selected: native.selected.model } : {}),
                ...(native.activated ? { activated: native.activated.model } : {}),
              },
            }
          : {}),
        ...(record.supersededByCompositionId
          ? { supersededByCompositionId: record.supersededByCompositionId }
          : {}),
      };
    }
    case "publication":
      return {
        kind: record.kind,
        id,
        ordinal: record.message.ordinal,
        memberId: record.message.memberId,
        assignmentId: record.message.assignmentId,
        stage: record.message.stage,
        ...(record.message.round === undefined ? {} : { round: record.message.round }),
        turnId: record.message.turnId,
        attemptId: record.message.attemptId,
        snapshotId: record.message.snapshotId,
        messageId: record.message.id,
        textId: textId(record),
      };
    case "control":
      return {
        kind: record.kind,
        id,
        action: record.action,
        order: record.order,
        author: record.author,
        textId: textId(record),
      };
    case "failure":
      return {
        kind: record.kind,
        id,
        phase: record.phase,
        ...(record.memberId === undefined ? {} : { memberId: record.memberId }),
        ...(record.assignmentId === undefined ? {} : { assignmentId: record.assignmentId }),
        ...(record.stage === undefined ? {} : { stage: record.stage }),
        ...(record.round === undefined ? {} : { round: record.round }),
        textId: textId(record),
      };
    case "outcome":
      return { kind: record.kind, id, status: record.status, textId: textId(record) };
    case "knowledge-boundary":
      return { kind: record.kind, id, ordinal: record.ordinal, textId: textId(record) };
  }
}

function content(record: PiArchitectureRecord): {
  text: string;
  format: "text" | "json";
  source: string;
  complete: boolean;
} {
  switch (record.kind) {
    case "member":
      return {
        text: JSON.stringify({ mandate: record.mandate, bindings: bindings(record) }),
        format: "json",
        source: record.mandate.source,
        complete: true,
      };
    case "publication":
      return {
        text: JSON.stringify({
          body: record.message.body,
          doubts: record.message.doubts,
          native: record.message.native,
        }),
        format: "json",
        source: "settled publication",
        complete: true,
      };
    case "outcome":
      return {
        text: JSON.stringify(record.content),
        format: "json",
        source: record.source,
        complete: true,
      };
    case "control":
      return {
        text: JSON.stringify({
          action: record.action,
          text: record.text,
          application: record.application,
        }),
        format: "json",
        source: "subsequent instruction",
        complete: true,
      };
    case "failure":
      return { text: record.error, format: "text", source: record.phase, complete: true };
    case "knowledge-boundary":
      return { text: JSON.stringify(record), format: "json", source: record.kind, complete: true };
    case "room":
      return { text: "", format: "text", source: "unavailable", complete: false };
  }
}

function page<A>(items: ReadonlyArray<A>, cursor: string | undefined, revision: string) {
  const prefix = `${revision}:`;
  if (
    cursor !== undefined &&
    (!cursor.startsWith(prefix) || !/^\d+$/.test(cursor.slice(prefix.length)))
  )
    throw Error("stale architecture cursor");
  const offset = cursor ? Number(cursor.slice(prefix.length)) : 0;
  if (!Number.isSafeInteger(offset) || offset > items.length)
    throw Error("invalid architecture cursor");
  let bytes = 0;
  let end = offset;
  while (end < items.length && end < offset + 20) {
    const size = new TextEncoder().encode(JSON.stringify(items[end])).length;
    if (bytes + size > 16384) break;
    bytes += size;
    end++;
  }
  if (end === offset && end < items.length) throw Error("Architecture metadata exceeds page limit");
  return { items: items.slice(offset, end), next: end < items.length ? `${prefix}${end}` : null };
}

/** Reads only the committed T3 archive. No native bridge or runtime recovery. */
export const read = (
  input: PiArchitectureReadInput,
  archives: ReadonlyArray<ArchitectureArchive>,
  workers: ReadonlyArray<OrchestrationV2Subagent>,
): Effect.Effect<PiArchitectureReadResult, ArchitectureQueryError> =>
  Effect.gen(function* () {
    const archive =
      input.kind === "tables"
        ? undefined
        : archives.find(
            (row) =>
              row.summary.threadId === input.threadId && row.summary.tableId === input.tableId,
          );
    if (input.kind !== "tables" && !archive)
      return yield* new ArchitectureQueryError({ cause: "Architecture table not found" });
    const tables =
      input.kind === "tables"
        ? archives
            .filter((row) => row.summary.threadId === input.threadId)
            .map((row) => row.summary)
        : [];
    const records =
      input.kind === "records" && archive
        ? archive.records.map((record) => detail(record, archive, workers))
        : [];
    const record =
      input.kind === "text"
        ? archive?.records.find((record) => textId(record) === input.textId)
        : undefined;
    const value =
      input.kind !== "text"
        ? undefined
        : input.textId === "original"
          ? {
              text: archive?.original?.text ?? "",
              format: "text" as const,
              source: archive?.original?.source ?? "unavailable",
              complete: archive?.original?.complete ?? false,
            }
          : record
            ? content(record)
            : undefined;
    // Bind each token to its query resource and exposed value, not owner recapture bookkeeping.
    // Records include the worker join; text includes its own source/completeness, not other texts.
    const revision = Hex.encode(
      yield* NodeCrypto.make.digest(
        "SHA-256",
        new TextEncoder().encode(
          encodeJson([
            input.kind,
            input.threadId,
            input.kind === "tables" ? null : input.tableId,
            input.kind === "text"
              ? [
                  input.textId,
                  archive?.summary.providerInstanceId,
                  archive?.summary.coordinatorSessionId,
                  archive?.summary.roomId,
                  value,
                ]
              : input.kind === "tables"
                ? tables
                : [archive?.summary, records],
          ]),
        ),
      ),
    );
    return yield* Effect.try({
      try: (): PiArchitectureReadResult => {
        if (input.kind === "tables")
          return {
            kind: "tables",
            ...page(tables, input.cursor, revision),
          };
        if (!archive) throw Error("Architecture table not found");
        if (input.kind === "records")
          return {
            kind: "records",
            summary: archive.summary,
            ...page(records, input.cursor, revision),
          };
        if (input.revision !== undefined && input.revision !== revision)
          throw Error("stale architecture text");
        if (!value) throw Error("Architecture text not found");
        const offset = input.offset ?? 0;
        if (offset > value.text.length || (offset > 0 && !input.revision))
          throw Error("Invalid architecture text offset");
        let end = Math.min(value.text.length, offset + 4096);
        const last = value.text.charCodeAt(end - 1);
        if (end < value.text.length && last >= 0xd800 && last <= 0xdbff) end--;
        return {
          kind: "text",
          textId: input.textId,
          revision,
          offset,
          text: value.text.slice(offset, end),
          nextOffset: end < value.text.length ? end : null,
          format: value.format,
          complete: value.complete,
          source: value.source,
        };
      },
      catch: (cause) => new ArchitectureQueryError({ cause }),
    });
  }).pipe(
    Effect.mapError((cause) =>
      isQueryError(cause) ? cause : new ArchitectureQueryError({ cause }),
    ),
  );
