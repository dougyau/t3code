import * as Schema from "effect/Schema";
import { PiArchitectureRecord, type PiArchitectureText } from "@t3tools/contracts";
import {
  ProviderArchitectureCatalogLoss,
  type ProviderArchitectureBatch,
} from "@t3tools/provider-core/server/ProviderAdapter";
import { BridgeError, type BridgeClient } from "./pitako/bridge/client.js";
import {
  sameOwner,
  type ArchitectureRef,
  type WorkerReadCursor,
} from "./pitako/bridge/protocol.js";

const decodeRecord = Schema.decodeUnknownSync(PiArchitectureRecord);
const decodeJson = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
const limit = 4 * 1024 * 1024;
type ArchitectureClient = Pick<
  BridgeClient,
  "capabilities" | "owner" | "listArchitectures" | "readArchitecture" | "publicDetail"
>;

/** One view has only one room cut: finish it before requesting another room. */
export async function readArchitectures(
  client: ArchitectureClient,
  ownerGeneration: number,
): Promise<ProviderArchitectureBatch[]> {
  if (
    !["architecture.list", "architecture.read", "architecture.public-v1"].every((capability) =>
      client.capabilities.some((value) => value === capability),
    )
  )
    return [];
  const refs: ArchitectureRef[] = [];
  const cursors = new Set<string>();
  let cursor: string | undefined;
  let snapshotId: string | undefined;
  let cut: number | undefined;
  do {
    const page = await client.listArchitectures(cursor);
    if (page.state !== "ready") {
      // An incomplete catalog establishes no room cut, including for refs on earlier pages.
      throw new ProviderArchitectureCatalogLoss({
        catalogGap: `catalog_${page.state}${page.state === "unavailable" ? `:${page.reason}` : ""}`,
        owner: { ...client.owner },
        ownerGeneration,
      });
    }
    snapshotId ??= page.snapshotId;
    cut ??= page.cut;
    if (!sameOwner(page.owner, client.owner) || snapshotId !== page.snapshotId || cut !== page.cut)
      throw new BridgeError("provenance_mismatch", "Architecture catalog cut changed");
    for (const item of page.items) {
      if (!sameOwner(item.ref, client.owner))
        throw new BridgeError("stale_owner", "Catalog owner differs");
      if (!refs.some((ref) => ref.roomId === item.ref.roomId)) refs.push(item.ref);
    }
    cursor = page.next ?? undefined;
    if (cursor && cursors.has(cursor))
      throw new BridgeError("invalid_cursor", "Repeated catalog cursor");
    if (cursor) cursors.add(cursor);
  } while (cursor);
  const result: ProviderArchitectureBatch[] = [];
  for (const ref of refs) result.push(await readRoom(client, ref, ownerGeneration));
  return result;
}

async function readRoom(
  client: ArchitectureClient,
  ref: ArchitectureRef,
  ownerGeneration: number,
): Promise<ProviderArchitectureBatch> {
  const records: PiArchitectureRecord[] = [];
  const gaps: string[] = [];
  const cursors = new Set<string>();
  let cursor: string | undefined;
  let snapshotId: string | undefined;
  let cut = 0;
  let capture: ProviderArchitectureBatch["capture"] = "incomplete";
  let fragment: { id: string; total: number; bytes: Buffer[]; offset: number } | undefined;
  let bytes = 0;
  try {
    do {
      const page = await client.readArchitecture(ref, cursor);
      if (page.state !== "ready")
        throw new BridgeError(page.state, "Architecture room unavailable");
      snapshotId ??= page.snapshotId;
      if (
        snapshotId !== page.snapshotId ||
        (cursor && cut !== page.cut) ||
        !sameOwner(page.ref, ref) ||
        page.ref.roomId !== ref.roomId
      )
        throw new BridgeError("provenance_mismatch", "Architecture room cut changed");
      cut = page.cut;
      capture = page.capture.state;
      gaps.push(...page.capture.gaps);
      for (const wire of page.records) {
        if (wire.kind !== "fragment") {
          if (fragment)
            throw new BridgeError("invalid_record", "Interleaved Architecture fragment");
          records.push(decodeRecord(wire));
          continue;
        }
        if (!fragment)
          fragment = { id: wire.recordId, total: wire.totalBytes, bytes: [], offset: 0 };
        if (
          fragment.id !== wire.recordId ||
          fragment.offset !== wire.byteOffset ||
          fragment.total !== wire.totalBytes ||
          wire.totalBytes > limit
        )
          throw new BridgeError("invalid_record", "Architecture fragment identity differs");
        const part = Buffer.from(wire.data, "base64");
        fragment.bytes.push(part);
        fragment.offset += part.length;
        bytes += part.length;
        if (bytes > limit || fragment.offset > fragment.total)
          throw new BridgeError("record_oversized", "Architecture snapshot exceeds limit");
        if (fragment.offset === fragment.total) {
          const restored = decodeRecord(
            decodeJson(
              new TextDecoder("utf-8", { fatal: true }).decode(Buffer.concat(fragment.bytes)),
            ),
          );
          if (restored.id !== fragment.id)
            throw new BridgeError("invalid_record", "Fragment record differs");
          records.push(restored);
          fragment = undefined;
        }
      }
      cursor = page.next ?? undefined;
      if (cursor && cursors.has(cursor))
        throw new BridgeError("invalid_cursor", "Repeated room cursor");
      if (cursor) cursors.add(cursor);
    } while (cursor);
    if (fragment) throw new BridgeError("invalid_record", "Incomplete Architecture fragment");
  } catch (error) {
    capture = "incomplete";
    gaps.push(error instanceof BridgeError ? error.code : "invalid_record");
  }
  const room = records.find((record) => record.kind === "room");
  const original = room ? await readOriginal(client, room.goalSource.instanceId) : undefined;
  return {
    roomId: ref.roomId,
    checkpoint: { owner: { ...client.owner }, ownerGeneration, snapshotId: snapshotId ?? "", cut },
    records,
    capture,
    gaps: [...new Set(gaps)],
    ...(original ? { original } : {}),
  };
}

async function readOriginal(
  client: ArchitectureClient,
  instanceId: string,
): Promise<PiArchitectureText> {
  let text = "";
  let source: PiArchitectureText["source"] = "unavailable";
  let cursor: WorkerReadCursor | undefined;
  const cursors = new Set<string>();
  try {
    do {
      const page = await client.publicDetail(instanceId, cursor);
      if (
        "state" in page ||
        !sameOwner(page.ref, client.owner) ||
        page.ref.instanceId !== instanceId ||
        page.workbrief.offset !== text.length ||
        (source !== "unavailable" && source !== page.workbrief.source)
      )
        throw Error("Original WorkBrief provenance differs");
      source = page.workbrief.source;
      text += page.workbrief.text;
      if (Buffer.byteLength(text) > limit) throw Error("Original WorkBrief exceeds limit");
      cursor = page.nextBrief ?? undefined;
      const key = JSON.stringify(cursor);
      if (cursor && cursors.has(key)) throw Error("Repeated WorkBrief cursor");
      if (cursor) cursors.add(key);
    } while (cursor);
    return { id: "original", text, source, complete: true };
  } catch {
    return { id: "original", text, source, complete: false };
  }
}
