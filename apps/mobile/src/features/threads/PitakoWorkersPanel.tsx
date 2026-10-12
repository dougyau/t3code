import { RegistryContext, useAtomValue } from "@effect/atom-react";
import { createWorkerInspector } from "@t3tools/client-runtime/state/worker-inspector";
import type { WorkerTarget } from "@t3tools/contracts";
import { AsyncResult } from "effect/reactivity";
import { useContext, useEffect, useState } from "react";
import { Pressable, TextInput, View } from "react-native";
import { AppText as Text } from "../../components/AppText";
import { providerWorkers } from "../../state/provider-workers";

export function PitakoWorkersPanel(target: typeof WorkerTarget.Type) {
  const [open, setOpen] = useState(false);
  return (
    <View className="gap-3 py-4" accessibilityLabel="Pitako workers">
      <Text className="text-base font-semibold">Pitako workers</Text>
      <WorkerButton
        label={open ? "Close Pitako inspection" : "Inspect Pitako workers"}
        press={() => setOpen(!open)}
      />
      {open ? (
        <WorkerInspection key={`${target.environmentId}:${target.threadId}`} {...target} />
      ) : null}
    </View>
  );
}

function WorkerButton(props: { label: string; disabled?: boolean; press: () => void }) {
  return (
    <Pressable
      accessibilityRole="button"
      accessibilityLabel={props.label}
      accessibilityState={{ disabled: props.disabled }}
      disabled={props.disabled}
      onPress={props.press}
      className="rounded-lg border border-border p-3 disabled:opacity-50"
    >
      <Text className="text-sm text-foreground">{props.label}</Text>
    </Pressable>
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
    <View className="gap-3">
      <Text>{model.status}</Text>
      {AsyncResult.isFailure(observation) || AsyncResult.isFailure(view) ? (
        <Text accessibilityRole="alert">
          Inspection unavailable or permission denied. No automatic retry.
        </Text>
      ) : null}
      <Text className="text-xs text-foreground-muted">{model.notice}</Text>
      {model.error ? <Text accessibilityRole="alert">{model.error}</Text> : null}
      {model.busy ? <Text>Request in progress. No automatic resend.</Text> : null}
      {model.rows.map((row) => (
        <WorkerButton
          key={row.id}
          label={row.label}
          disabled={model.busy}
          press={() => void row.select()}
        />
      ))}
      <Text>Deliberate worker input</Text>
      <TextInput
        accessibilityLabel="Deliberate worker input"
        multiline
        value={model.text}
        editable={model.inputEnabled}
        maxLength={8192}
        onChangeText={model.setText}
        className="min-h-20 rounded-lg border border-border p-3 text-foreground"
      />
      {model.buttons.map((button) => (
        <WorkerButton
          key={button.label}
          label={button.label}
          disabled={button.disabled}
          press={() => void button.press()}
        />
      ))}
      {model.sections.map((section) => (
        <View key={section.title} className="gap-1">
          <Text className="font-semibold">{section.title}</Text>
          <Text selectable className="text-xs">
            {section.text}
          </Text>
        </View>
      ))}
    </View>
  );
}
