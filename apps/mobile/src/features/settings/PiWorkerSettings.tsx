import { useAtomValue } from "@effect/atom-react";
import {
  ProviderDriverKind,
  type EnvironmentId,
  type ProviderInstanceId,
} from "@t3tools/contracts";
import { AsyncResult } from "effect/reactivity";
import { useRef, useState } from "react";
import { View } from "react-native";
import { AppText as Text } from "../../components/AppText";
import { serverEnvironment } from "../../state/server";
import { useAtomCommand } from "../../state/use-atom-command";
import { SettingsSwitchRow } from "./components/SettingsSwitchRow";

const fields = [
  ["workerBridge", "Pitako worker bridge"],
  ["workerHistory", "Allow worker history acknowledgement"],
  ["workerRoute", "Allow worker routing acknowledgement"],
  ["workerControl", "Allow worker control activation"],
] as const;

/** Environment-local provider administration, never a grant or a process restart. */
export function PiWorkerSettings({
  environmentId,
  instanceId,
}: {
  environmentId: EnvironmentId;
  instanceId: ProviderInstanceId;
}) {
  const settings = useAtomValue(serverEnvironment.settingsValueAtom(environmentId));
  const allowed = useAtomValue(
    serverEnvironment.mutateProviderInstance.permissionAtom(environmentId, {
      patch: {},
      providerInstanceMutation: {
        operation: "upsert",
        instanceId,
        instance: settings?.providerInstances[instanceId] ?? {
          driver: ProviderDriverKind.make("pi"),
        },
      },
    }),
  );
  const mutate = useAtomCommand(serverEnvironment.mutateProviderInstance, { reportFailure: false });
  const [pending, setPending] = useState(false);
  const pendingRef = useRef(false);
  const [message, setMessage] = useState("");
  const instance = settings?.providerInstances[instanceId];
  const config =
    instance?.config && typeof instance.config === "object" && !Array.isArray(instance.config)
      ? (instance.config as Record<string, unknown>)
      : {};
  async function change(key: (typeof fields)[number][0], value: boolean) {
    if (!allowed || !instance || pendingRef.current) return;
    pendingRef.current = true;
    setPending(true);
    setMessage("");
    try {
      const result = await mutate({
        environmentId,
        input: {
          patch: {},
          providerInstanceMutation: {
            operation: "upsert",
            instanceId,
            instance: { ...instance, config: { ...config, [key]: value } },
          },
        },
      });
      setMessage(
        AsyncResult.isFailure(result)
          ? "Settings were not saved. Permission or connection may have changed."
          : "Saved. Applies to the next Pi process. The current process was not restarted.",
      );
    } finally {
      pendingRef.current = false;
      setPending(false);
    }
  }
  return (
    <View className="gap-2">
      <Text className="px-4 text-sm text-foreground-muted">
        Pitako opt-ins apply to the next Pi process. They set ceilings, not private access grants.
        No process is restarted. All are off by default.
      </Text>
      {fields.map(([key, label]) => (
        <SettingsSwitchRow
          key={key}
          icon="person.2"
          label={label}
          value={key in config && config[key] === true}
          disabled={!allowed || !instance || pending}
          onValueChange={(value) => void change(key, value)}
        />
      ))}
      {message ? <Text className="px-4 text-sm text-foreground-muted">{message}</Text> : null}
    </View>
  );
}
