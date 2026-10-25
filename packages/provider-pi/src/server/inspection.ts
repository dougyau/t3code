import * as Schema from "effect/Schema";
import { piRecordField as field, piRecordString as string } from "./rpc.ts";

export const PiSubagentMessage = Schema.Struct({
  role: Schema.String,
  kind: Schema.Literals(["text", "toolCall", "toolResult"]),
  text: Schema.String,
  name: Schema.optional(Schema.String),
  isError: Schema.optional(Schema.Boolean),
});
export type PiSubagentMessage = typeof PiSubagentMessage.Type;

export function piSubagentMessages(input: unknown): ReadonlyArray<PiSubagentMessage> {
  if (!Array.isArray(input)) return [];
  return input.flatMap((message): PiSubagentMessage[] => {
    const role = string(message, "role") ?? "assistant";
    const content = field(message, "content");
    const textFields =
      role === "toolResult"
        ? {
            role,
            kind: "toolResult" as const,
            name: string(message, "toolName"),
            isError: field(message, "isError") === true,
          }
        : { role, kind: "text" as const };
    if (typeof content === "string") return [{ ...textFields, text: content }];
    if (!Array.isArray(content)) return [];
    return content.flatMap((block): PiSubagentMessage[] => {
      const type = string(block, "type");
      if (type === "text") {
        return [{ ...textFields, text: string(block, "text") ?? "" }];
      }
      if (type === "toolCall") {
        return [
          {
            role,
            kind: "toolCall",
            name: string(block, "name"),
            text: JSON.stringify(field(block, "arguments") ?? null),
          },
        ];
      }
      return [];
    });
  });
}

export function piSubagentToolSummaries(input: unknown): ReadonlyArray<PiSubagentMessage> {
  if (!Array.isArray(input)) return [];
  return input.slice(-64).flatMap((summary): PiSubagentMessage[] => {
    const text = string(summary, "expandedText") ?? string(summary, "text");
    return text
      ? [
          {
            role: "system",
            kind: "text",
            text: `Native tool activity (summary only; outcome unknown): ${text.slice(0, 2000)}`,
          },
        ]
      : [];
  });
}

export const PiSubagentInspectReply = Schema.Struct({
  kind: Schema.Literal("pi-subagents.inspect-reply"),
  version: Schema.Literal(1),
  requestId: Schema.String,
  asyncId: Schema.optional(Schema.String),
  childId: Schema.optional(Schema.String),
  task: Schema.optional(Schema.String),
  finalOutput: Schema.optional(Schema.String),
  messages: Schema.optional(Schema.Array(PiSubagentMessage)),
  truncated: Schema.optional(
    Schema.Struct({
      task: Schema.Boolean,
      messages: Schema.Number,
      finalOutput: Schema.Boolean,
    }),
  ),
  error: Schema.optional(Schema.Struct({ code: Schema.String, message: Schema.String })),
});
