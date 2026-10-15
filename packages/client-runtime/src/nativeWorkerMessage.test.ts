import { MessageId, ThreadId, TurnItemId, type OrchestrationV2TurnItem } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { expect, it } from "vite-plus/test";
import { shouldOmitNativeWorkerMessage } from "./nativeWorkerMessage.ts";
import { nativeWorkerRecordFixture } from "./testing/nativeWorker.ts";

it("only omits settled native assistant messages without text or attachments", () => {
  const timestamp = DateTime.makeUnsafe("2026-06-20T00:00:03Z");
  const item: OrchestrationV2TurnItem = {
    id: TurnItemId.make("item"),
    threadId: ThreadId.make("thread"),
    runId: null,
    nodeId: null,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal: 0,
    status: "completed",
    title: null,
    startedAt: timestamp,
    completedAt: timestamp,
    updatedAt: timestamp,
    type: "assistant_message",
    messageId: MessageId.make("message"),
    text: " \n ",
    streaming: false,
    nativeWorkerRecord: nativeWorkerRecordFixture(),
  };
  expect(shouldOmitNativeWorkerMessage(item)).toBe(true);
  expect(shouldOmitNativeWorkerMessage({ ...item, text: "" })).toBe(true);
  for (const retained of [
    { ...item, nativeWorkerRecord: undefined },
    { ...item, streaming: true },
    { ...item, text: "[T3 notice] The response failed." },
    // Historical notices lost their synthetic provenance. Never classify their text.
    { ...item, text: "[T3 notice] Some content is unavailable." },
    { ...item, text: "Actual response\n\n[T3 notice] Some content is unavailable." },
    {
      ...item,
      text: "[T3 notice] Some content is unavailable.",
      attachments: [
        {
          id: "historical-image",
          type: "image" as const,
          name: "image.png",
          mimeType: "image/png",
          sizeBytes: 1,
        },
      ],
    },
    {
      ...item,
      attachments: [
        {
          id: "image",
          type: "image" as const,
          name: "image.png",
          mimeType: "image/png",
          sizeBytes: 1,
        },
      ],
    },
    {
      ...item,
      type: "user_message" as const,
      createdBy: "user" as const,
      creationSource: "mobile" as const,
      inputIntent: "turn_start" as const,
      attachments: [],
    },
  ]) {
    expect(shouldOmitNativeWorkerMessage(retained)).toBe(false);
  }
});
