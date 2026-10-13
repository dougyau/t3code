import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import { handoffWorkerRows, invalidHandoffWorkerRows } from "./providerWorkers.fixture.ts";
import { WorkerAcknowledgeInput, WorkerHistoryRead, WorkerHistoryReadInput, WorkerMcpReadInput, WorkerRoster, WorkerTextPage, WorkerActivateInput, WorkerActionOutcome, WorkerReceiptInput, WorkerActionId, makeWorkerActionId, parseWorkerActionId } from "./providerWorkers.ts";

const decodeText = Schema.decodeUnknownSync(WorkerTextPage);
const decodeHistory = Schema.decodeUnknownSync(WorkerHistoryRead);
const decodeAcknowledge = Schema.decodeUnknownSync(WorkerAcknowledgeInput);
const decodeHistoryInput = Schema.decodeUnknownSync(WorkerHistoryReadInput);
const decodeRoster = Schema.decodeUnknownSync(WorkerRoster);
const decodeMcp = Schema.decodeUnknownSync(WorkerMcpReadInput);
const decodeActivate = Schema.decodeUnknownSync(WorkerActivateInput);
const decodeReceipt = Schema.decodeUnknownSync(WorkerReceiptInput);
const decodeOutcome = Schema.decodeUnknownSync(WorkerActionOutcome);
const decodeActionId = Schema.decodeUnknownSync(WorkerActionId);
const actionId = makeWorkerActionId("view", 1, "00000000-0000-0000-0000-000000000001");

describe("browser-safe worker pages", () => {
  it("preserves optional handoff observations and rejects incomplete or unknown states", () => {
    const page = { owner: { processId: "process", coordinatorSessionId: "session", epoch: "1" }, revision: 1, cursor: null };
    for (const row of handoffWorkerRows) {
      expect(decodeRoster({ ...page, items: [row] })).toEqual({ ...page, items: [row] });
    }
    for (const row of invalidHandoffWorkerRows) {
      expect(() => decodeRoster({ ...page, items: [row] })).toThrow();
    }
    expect(() => decodeRoster({ ...page, items: Array.from({ length: 51 }, () => handoffWorkerRows[3]) })).toThrow();
  });
  it("offers only ordinary discriminated read requests to MCP", () => {
    expect(decodeMcp({ request: { operation: "list", threadId: "thread" } })).toEqual({
      request: { operation: "list", threadId: "thread" },
    });
    expect(() => decodeMcp({ request: { operation: "acknowledge", threadId: "thread", viewId: "view", acknowledged: true } })).toThrow();
    expect(() => decodeMcp({ request: { operation: "historyRead", threadId: "thread", viewId: "view", historyId: "history" } })).toThrow();
    for (const operation of ["activate", "action", "receipt"])
      expect(() => decodeMcp({ request: { operation, threadId: "thread", actionId: "action" } })).toThrow();
  });
  it("requires exact control identity and receipt authority without conflating transport uncertainty or native states", () => {
    const target = { environmentId: "env", threadId: "thread", viewId: "view" };
    expect(() => decodeActivate({ ...target, acknowledged: true, selection: { instanceId: "worker" } })).toThrow();
    expect(() => decodeReceipt({ ...target, actionId: "action" })).toThrow();
    const outcome = decodeOutcome;
    for (const state of ["pending", "outcome_unknown", "not_admitted"])
      expect(outcome({ actionId, status: { state, sequence: 1 } }).status.state).toBe(state);
    for (const status of ["queued", "handled", "rejected", "unconfirmed"]) {
      const page = { actionId, status: { state: "recorded", sequence: 1, receipt: { kind: "input", native: {
        interactionId: "interaction", historyId: "history", sessionId: "session", intent: "query", status,
      } } } };
      expect(outcome(page)).toEqual(page);
    }
    expect(outcome({ actionId, status: { state: "outcome_unknown", reason: "unavailable" } }).status.state).toBe("outcome_unknown");
    expect(() => outcome({ actionId, status: { state: "pending", sequence: 0 } })).toThrow();
  });
  it("accepts only bounded canonical view/positive-safe-ordinal/nonce decision IDs", () => {
    const view = "view:one";
    const nonce = "00000000-0000-0000-0000-000000000001";
    const value = makeWorkerActionId(view, Number.MAX_SAFE_INTEGER, nonce);
    expect(decodeActionId(value)).toBe(value);
    expect(parseWorkerActionId(value)).toEqual({ viewId: view, ordinal: Number.MAX_SAFE_INTEGER, nonce });
    for (const invalid of [
      "arbitrary", value.replace("w1:", "w2:"), value.replace("%3A", "%3a"),
      `w1:view:0:${nonce}`, `w1:view:01:${nonce}`, `w1:view:1e2:${nonce}`,
      `w1:view:9007199254740992:${nonce}`, `w1:%zz:1:${nonce}`,
      `w1:view:1:${nonce}:extra`, `w1:view:1:${nonce.toUpperCase()}X`,
      `w1:${"a".repeat(513)}:1:${nonce}`,
    ]) {
      expect(parseWorkerActionId(invalid)).toBeUndefined();
      expect(() => decodeActionId(invalid)).toThrow();
    }
    expect(() => makeWorkerActionId(view, Number.MAX_SAFE_INTEGER + 1, nonce)).toThrow(RangeError);
  });
  it("bounds rows and UTF-16 text without decoding native history bytes", () => {
    const text = "😀".repeat(512);
    expect(decodeText({ text, offset: 0, nextOffset: null }).text).toBe(text);
    expect(() => decodeText({ text: text + "a", offset: 0, nextOffset: null })).toThrow();
    const item = { historyId: "history", source: "worker", byteOffset: 0, entryOffset: 0, endsEntry: true, encoding: "base64", data: "/wo=", validation: "corrupt" };
    const page = { items: [item], diagnostics: [{ code: "corrupt_entry" }], cursor: null };
    expect(decodeHistory(page)).toEqual(page);
    expect(() => decodeHistory({ ...page, items: Array.from({ length: 51 }, () => item) })).toThrow();
    expect(() => decodeHistory({ ...page, items: [{ ...item, source: "coordinator" }] })).toThrow();
  });
  it("requires a fresh acknowledgement and never accepts coordinator/path selectors", () => {
    const target = { environmentId: "env", threadId: "thread", viewId: "view" };
    const ack = decodeAcknowledge;
    expect(ack({ ...target, grant: "history", acknowledged: true }).grant).toBe("history");
    expect(() => ack({ ...target, grant: "history", acknowledged: false })).toThrow();
    expect(() => ack({ ...target, grant: "control", acknowledged: true })).toThrow();
    const read = decodeHistoryInput({
      ...target, historyId: "history", source: "coordinator", path: "/private",
    });
    expect(read).not.toHaveProperty("source");
    expect(read).not.toHaveProperty("path");
    expect(() => decodeRoster({ owner: {}, items: [], revision: 1, cursor: null })).toThrow();
  });
});
