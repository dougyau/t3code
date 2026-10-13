import { RegistryContext, useAtomValue } from "@effect/atom-react";
import { createWorkerInspector } from "@t3tools/client-runtime/state/worker-inspector";
import type { WorkerTarget } from "@t3tools/contracts";
import { AsyncResult } from "effect/reactivity";
import { useContext, useEffect, useState } from "react";
import { providerWorkers } from "../../state/provider-workers";
import { Button } from "../ui/button";
import { Textarea } from "../ui/textarea";

/** Separate from T3 subagents: opening never grants private access or sends native input. */
export function PitakoWorkersPanel(props: typeof WorkerTarget.Type) {
  const [open, setOpen] = useState(false);
  return (
    <section aria-label="Pitako workers" className="space-y-2 p-3">
      <h3 className="text-sm font-medium">Pitako workers</h3>
      <Button size="sm" variant="outline" onClick={() => setOpen(!open)}>
        {open ? "Close Pitako inspection" : "Inspect Pitako workers"}
      </Button>
      {open ? (
        <WorkerInspection key={`${props.environmentId}:${props.threadId}`} {...props} />
      ) : null}
    </section>
  );
}

function WorkerInspection(target: typeof WorkerTarget.Type) {
  const registry = useContext(RegistryContext);
  const [inspector] = useState(() => createWorkerInspector(registry, providerWorkers, target));
  const observation = useAtomValue(inspector.observationAtom);
  const view = useAtomValue(inspector.viewAtom);
  const model = useAtomValue(inspector.model);
  useEffect(() => {
    const unsubscribe = inspector.mount();
    return () => {
      unsubscribe();
      inspector.clearLocal();
      void inspector.close();
    };
  }, [inspector]);
  return (
    <div className="space-y-3 text-sm">
      <p>{model.status}</p>
      {AsyncResult.isFailure(observation) || AsyncResult.isFailure(view) ? (
        <p role="alert">Inspection unavailable or permission denied. No automatic retry.</p>
      ) : null}
      <p className="text-xs text-muted-foreground">{model.notice}</p>
      {model.error ? <p role="alert">{model.error}</p> : null}
      {model.busy ? <p role="status">Request in progress. No automatic resend.</p> : null}
      <div className="flex flex-wrap gap-2">
        {model.rows.map((row) => (
          <section key={row.id} aria-label={row.label} className="space-y-1">
            <Button variant="outline" size="sm" disabled={model.busy} onClick={() => void row.select()}>
              {row.label}
            </Button>
            <dl>
              {row.sections.map((section) => (
                <div key={section.title}>
                  <dt className="inline font-medium">{section.title}: </dt>
                  <dd className="inline">{section.text}</dd>
                </div>
              ))}
            </dl>
          </section>
        ))}
      </div>
      <label className="block">
        Deliberate worker input
        <Textarea
          aria-label="Deliberate worker input"
          value={model.text}
          disabled={!model.inputEnabled}
          maxLength={8192}
          onChange={(event) => model.setText(event.target.value)}
        />
      </label>
      <div className="flex flex-wrap gap-2">
        {model.buttons.map((button) => (
          <Button
            key={button.label}
            variant="outline"
            size="sm"
            disabled={button.disabled}
            onClick={() => void button.press()}
          >
            {button.label}
          </Button>
        ))}
      </div>
      {model.sections.map((section) => (
        <section key={section.title} className="space-y-1">
          <h4 className="font-medium">{section.title}</h4>
          <pre className="max-h-64 overflow-auto whitespace-pre-wrap break-all text-xs">
            {section.text}
          </pre>
        </section>
      ))}
    </div>
  );
}
