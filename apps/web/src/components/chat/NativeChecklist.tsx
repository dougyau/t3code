import type { OrchestrationV2PlanArtifact } from "@t3tools/contracts";
import { CheckIcon, CircleDotIcon, CircleIcon, ListTodoIcon } from "lucide-react";
import { useState } from "react";

import { ComposerBanner } from "./ComposerBanner";

export function NativeChecklist({
  plan,
}: {
  readonly plan: Extract<OrchestrationV2PlanArtifact, { readonly kind: "todo_list" }>;
}) {
  const [expanded, setExpanded] = useState(false);
  if (plan.steps.length === 0) return null;
  const completed = plan.steps.filter((step) => step.status === "completed").length;
  const current =
    plan.steps.find((step) => step.status === "running") ??
    plan.steps.find((step) => step.status === "pending") ??
    plan.steps.at(-1)!;
  return (
    <ComposerBanner.Attachment>
      <ComposerBanner.Root>
        <ComposerBanner.Row
          render={<button type="button" />}
          aria-expanded={expanded}
          aria-label={`Tasks: ${completed} of ${plan.steps.length} complete`}
          onClick={() => setExpanded((value) => !value)}
          data-composer-tasks-badge="true"
        >
          <ComposerBanner.Icon>
            <ListTodoIcon />
          </ComposerBanner.Icon>
          <ComposerBanner.Content>
            <span className="shrink-0">Tasks</span>
            <span className="min-w-0 truncate text-foreground">{current.text}</span>
          </ComposerBanner.Content>
          <ComposerBanner.Actions>
            <ComposerBanner.Count>
              {completed}/{plan.steps.length}
            </ComposerBanner.Count>
            <ComposerBanner.ToggleIcon expanded={expanded} />
          </ComposerBanner.Actions>
        </ComposerBanner.Row>
        {expanded ? (
          <ComposerBanner.Scroll>
            <ComposerBanner.Children render={<ul aria-label="Task list" />}>
              {plan.steps.map((step) => (
                <ComposerBanner.Row key={step.id} render={<li />} className="items-start py-1">
                  <ComposerBanner.Icon>
                    {step.status === "completed" ? (
                      <CheckIcon />
                    ) : step.status === "running" ? (
                      <CircleDotIcon />
                    ) : (
                      <CircleIcon />
                    )}
                  </ComposerBanner.Icon>
                  <ComposerBanner.Content className="block wrap-anywhere">
                    <span className="sr-only">{step.status}: </span>
                    {step.text}
                  </ComposerBanner.Content>
                </ComposerBanner.Row>
              ))}
            </ComposerBanner.Children>
          </ComposerBanner.Scroll>
        ) : null}
      </ComposerBanner.Root>
    </ComposerBanner.Attachment>
  );
}
