import { useState } from "react";
import { useAtomValue } from "@effect/atom-react";
import { Pressable, ScrollView, Text, View } from "react-native";
import type { ScopedThreadRef } from "@t3tools/contracts";
import { selectPiChecklist } from "@t3tools/client-runtime/state/pi-checklist";
import { environmentThreadDetails } from "../../state/threads";

export function NativeChecklist({ threadRef }: { readonly threadRef: ScopedThreadRef }) {
  const thread = useAtomValue(environmentThreadDetails.quietThreadAtom(threadRef));
  const [expanded, setExpanded] = useState(false);
  const projection = thread?.projection;
  if (!projection) return null;
  const plan = selectPiChecklist(
    projection.plans,
    projection.providerThreads.find(
      (provider) => provider.id === projection.thread.activeProviderThreadId,
    ),
  );
  if (plan?.kind !== "todo_list" || plan.steps.length === 0) return null;
  const done = plan.steps.filter((step) => step.status === "completed").length;
  return (
    <View className="rounded-xl border border-border bg-surface p-3 mb-2">
      <Pressable
        accessibilityRole="button"
        accessibilityState={{ expanded }}
        accessibilityLabel={`${done} of ${plan.steps.length} tasks complete`}
        onPress={() => setExpanded((value) => !value)}
      >
        <Text className="text-foreground">
          Tasks · {done}/{plan.steps.length} {expanded ? "▾" : "▸"}
        </Text>
      </Pressable>
      {expanded ? (
        <ScrollView style={{ maxHeight: 180 }}>
          {plan.steps.map((step) => (
            <Text key={step.id} className="text-foreground py-1" selectable>
              {step.status === "completed" ? "✓" : step.status === "running" ? "→" : "○"}{" "}
              {step.text}
            </Text>
          ))}
        </ScrollView>
      ) : null}
    </View>
  );
}
