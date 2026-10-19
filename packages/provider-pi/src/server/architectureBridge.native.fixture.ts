// @effect-diagnostics nodeBuiltinImport:off -- Paths for the isolated native fixture.
import * as NodePath from "node:path";
import { extensionSource } from "./workerBridge.native.shared.ts";

export const architectureGoal = "# Encargo 🧠\r\n\n" + 'Literal "quote" \\ ruta\n'.repeat(500);

export function architectureCoordinator(root: string, pitako: string) {
  return extensionSource(root, pitako, true, false, true)
    .replace("= { released, frontier, publish,", "= { root, released, frontier, publish,")
    .replace(
      'witness("disposal.json"); await gate("close-capture"); await dispose();',
      `const synthesizing = created.session.sessionManager.getEntries().some(entry =>
      entry.type === "message" && entry.message.role === "user" &&
      entry.message.content.some(block => block.type === "text" && block.text.includes('"stage":"synthesis"')));
      if (synthesizing) { witness("director-disposal.json"); await gate("close-capture"); }
      await dispose();`,
    )
    .replace(
      "import { teamEvaluationForSession }",
      "import { teamAssignments, teamEvaluationForSession }",
    )
    .replace(
      "const root = ",
      `import { architectureRooms, subscribeArchitectureChanges } from ${JSON.stringify(NodePath.join(pitako, "extensions/architecture/rooms.ts"))};
const root = `,
    )
    .replace(
      "const state = { coordinatorCalls",
      `const assignments = evaluation ? teamAssignments(evaluation).map(a => ({
      id: a.id, memberId: a.memberId, instanceId: a.instanceId, teamId: a.teamId, task: a.task, ordinal: a.ordinal
    })) : [];
    const state = { assignments, coordinatorCalls`,
    )
    .replace(
      'createRuntime: async options => { await gate("launch"); return ModelRuntime.create(options); },',
      `createRuntime: async options => {
      writeFileSync(root + "/runtime-entered.json", "{}");
      await gate("launch");
      try {
        const runtime = await ModelRuntime.create(options);
        writeFileSync(root + "/runtime-created.json", "{}");
        return runtime;
      } catch (error) {
        writeFileSync(root + "/runtime-error.json", JSON.stringify({ error: String(error) })); throw error;
      }
    },`,
    )
    .replace(
      "streamSimple(model) {",
      `streamSimple(model, input) {
      writeFileSync(root + "/coordinator-input.json.tmp", JSON.stringify({ messages: input.messages.filter(m => m.role === "toolResult") }));
      renameSync(root + "/coordinator-input.json.tmp", root + "/coordinator-input.json");`,
    )
    .replace(
      /const content = true && coordinatorCalls === 1 \? \[[\s\S]*?\] : \[/,
      `const content = coordinatorCalls === 1 ? [
        { type: "toolCall", id: "architecture-one", name: "architecture_start", arguments: { mode: "table", goal: ${JSON.stringify(architectureGoal)} } },
        { type: "toolCall", id: "architecture-two", name: "architecture_start", arguments: { mode: "table", goal: "Separate room" } }
      ] : [`,
    )
    .replace(
      "unsubscribe = subscribeChanges(publish);",
      `const workerChanges = subscribeChanges(publish);
    const roomChanges = subscribeArchitectureChanges(evaluation => {
      const rooms = architectureRooms(evaluation);
      if (rooms.length === 2 && rooms.every(room => room.capture.state === "closed")) {
        publish();
        witness("capture-closed.json");
      }
    });
    unsubscribe = () => { workerChanges(); roomChanges(); };`,
    );
}

// Only the model boundary is controlled. Admission, sessions, submit validation,
// settlement, publication, roster and bridge all run in the real extensions.
export function architectureWorker(pitako: string) {
  return `
import { createAssistantMessageEventStream } from ${JSON.stringify(NodePath.join(pitako, "node_modules/@earendil-works/pi-ai/dist/utils/event-stream.js"))};
import { writeFileSync } from "node:fs";
export default function(pi) {
  const fixture = () => globalThis[Symbol.for("t3.pitako.fixture")];
  pi.registerProvider("t3-worker", {
    api: "openai-responses", baseUrl: "http://127.0.0.1", apiKey: "fixture",
    models: [{ id: "controlled", name: "Offline architecture", compat: { supportsOpenAIGrammarTools: true },
      reasoning: false, input: ["text"], contextWindow: 1000000, maxTokens: 128,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } }],
    streamSimple(model, input) {
      const f = fixture();
      if (!f) throw Error("Only fixture-owned workers may run");
      writeFileSync(f.root + "/worker-input.json", JSON.stringify({ messages: input.messages }));
      f.witness("worker-first.json");
      const requests = input.messages.flatMap(message => {
        if (message.role !== "user") return [];
        const text = typeof message.content === "string" ? message.content :
          message.content.filter(block => block.type === "text").map(block => block.text).join("");
        try { const request = JSON.parse(text.split("\\n")[0]); return request.protocol === "Pitako architecture" ? [request] : []; }
        catch { return []; }
      });
      const request = requests.at(-1);
      if (!request) throw Error("Missing real architecture prompt");
      const results = input.messages.filter(message => message.role === "toolResult");
      const submitted = input.messages.at(-1)?.role === "toolResult";
      const failed = request.stage === "preparation" && request.context.mandate === "Fails before contribution";
      const other = request.goal === "Separate room";
      const stage = request.stage;
      let body;
      if (stage === "composition") body = { kind: stage, snapshotId: request.snapshot.id,
        specialists: (other ? ["Other specialist"] :
          request.round ? ["Replacement"] : ["Fails before contribution", "Healthy one", "Healthy two"])
          .map(mandate => ({ candidateRef: request.snapshot.general[0].ref, mandate, reason: "Known native composition" })) };
      if (stage === "preparation") body = { kind: stage, analysis: request.context.mandate,
        doubts: other ? [] : [{ question: "Is the evidence sufficient?", purpose: "Real support path" }] };
      if (stage === "answer") body = { kind: stage, answer: "Direct answer", evidence: [], limits: [], doubts: [] };
      if (stage === "consultation-plan") body = { kind: stage, resolutions: [], support: request.context.doubts.length ?
        [{ role: "scout", brief: "Independent support mandate", doubtIds: request.context.doubts.map(d => d.id), prerequisites: [] }] : [] };
      if (stage === "consultation-response") body = { kind: stage, doubtIds: request.context.request.doubtIds,
        answer: "Observed support evidence", evidence: [], limits: [], doubts: [] };
      if (stage === "knowledge") body = { kind: stage, resolutions: request.context.delegatedDoubts.map(d => ({
        doubtId: d.id, status: "answered", answer: "Known support answer",
        supportIds: request.context.responses.filter(response => response.status === "completed").map(response => response.id) })) };
      if (stage === "contribution") {
        const prior = request.authorization.objections.find(reference =>
          request.context.shared.messages.some(message => message.author === "specialist" && message.id === reference.messageId));
        body = { kind: stage, proposal: request.context.mandate, arguments: ["Evidence"],
          objections: prior ? [{ memberId: prior.memberId, messageId: prior.messageId, argument: "A genuine dissent" }] : [], doubts: [] };
      }
      if (stage === "decision") body = { kind: stage, action: !other && request.round === 1 ? "recompose" : "synthesize", reason: "Known replacement" };
      if (stage === "synthesis") body = { kind: stage, status: other ? "complete" : "partial",
        recommendation: "Bounded recommendation 🧠".repeat(300), alternatives: [], evidence: [], assumptions: [],
        disagreements: [], pending: other ? [] : ["Earlier member failed"] };
      if (!body && !failed) throw Error("Unsupported native stage: " + stage);
      const content = submitted || failed ? [{ type: "text", text: "Settled offline turn" }] :
        [{ type: "toolCall", id: "submit-" + requests.length, name: "architecture_submit",
          arguments: { submissionId: "submit-" + requests.length, body } }];
      const message = { role: "assistant", content, api: model.api, provider: model.provider, model: model.id,
        stopReason: submitted || failed ? "stop" : "toolUse", timestamp: Date.now(),
        usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, totalTokens: 2,
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } } };
      const stream = createAssistantMessageEventStream();
      for (const result of results) if (result.isError) {
        f.witness("submission-error.json"); throw Error("Native submission rejected: " + JSON.stringify(result));
      }
      queueMicrotask(async () => {
        if (!other && stage === "contribution" && request.round === 1 && !submitted) {
          f.witness("frontier.json"); await f.gate("contributions");
        }
        if (stage === "synthesis" && submitted) {
          f.witness("provisional-" + (other ? "other" : "table") + ".json"); await f.gate("synthesis");
        }
        stream.push({ type: "start", partial: { ...message, content: [] } });
        for (const [contentIndex, block] of content.entries()) {
          if (block.type === "toolCall") {
            stream.push({ type: "toolcall_start", contentIndex, partial: message });
            stream.push({ type: "toolcall_end", contentIndex, toolCall: block, partial: message });
          } else {
            stream.push({ type: "text_start", contentIndex, partial: message });
            stream.push({ type: "text_delta", contentIndex, delta: block.text, partial: message });
            stream.push({ type: "text_end", contentIndex, content: block.text, partial: message });
          }
        }
        stream.push({ type: "done", reason: message.stopReason, message }); stream.end(message);
      });
      return stream;
    }
  });
}
`;
}

export const architectureConfig =
  ["architect", "scout"]
    .map(
      (role) =>
        `[model_policies.${role}.primary]\nmodel = "t3-worker/controlled"\nreasoning = "off"\n`,
    )
    .join("") + `[architecture]\ncandidates = [{ model = "t3-worker/controlled" }]\n`;
