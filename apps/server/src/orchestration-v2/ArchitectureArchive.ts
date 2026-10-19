import * as Schema from "effect/Schema";
import * as Effect from "effect/Effect";
import type * as SqlClient from "effect/sql/SqlClient";
import {
  PiArchitectureCheckpoint,
  PiArchitectureRecord,
  PiArchitectureSummary,
  PiArchitectureText,
  ProviderSessionId,
  ProviderThreadId,
  ThreadId,
} from "@t3tools/contracts";

export const ArchitectureArchive = Schema.Struct({
  summary: PiArchitectureSummary,
  providerSessionId: ProviderSessionId,
  parentProviderThreadId: ProviderThreadId,
  checkpoint: PiArchitectureCheckpoint,
  records: Schema.Array(PiArchitectureRecord),
  original: Schema.optional(PiArchitectureText),
  retired: Schema.optional(Schema.Boolean),
});
export type ArchitectureArchive = typeof ArchitectureArchive.Type;
const decode = Schema.decodeUnknownSync(Schema.fromJsonString(ArchitectureArchive));
const encode = Schema.encodeSync(Schema.fromJsonString(ArchitectureArchive));

export const setup = (sql: SqlClient.SqlClient) => sql`
  CREATE TABLE IF NOT EXISTS orchestration_v2_architecture_archive (
    thread_id TEXT NOT NULL,
    table_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    PRIMARY KEY (thread_id, table_id)
  )
`;
export const read = (sql: SqlClient.SqlClient, threadId: ThreadId, tableId?: string) =>
  (tableId === undefined
    ? sql<{
        payload_json: string;
      }>`SELECT payload_json FROM orchestration_v2_architecture_archive WHERE thread_id = ${threadId} ORDER BY table_id`
    : sql<{
        payload_json: string;
      }>`SELECT payload_json FROM orchestration_v2_architecture_archive WHERE thread_id = ${threadId} AND table_id = ${tableId}`
  ).pipe(Effect.map((rows) => rows.map((row) => decode(row.payload_json))));
export const write = (sql: SqlClient.SqlClient, archive: ArchitectureArchive) => sql`
  INSERT INTO orchestration_v2_architecture_archive (thread_id, table_id, payload_json)
  VALUES (${archive.summary.threadId}, ${archive.summary.tableId}, ${encode(archive)})
  ON CONFLICT(thread_id, table_id) DO UPDATE SET payload_json = excluded.payload_json
`;
