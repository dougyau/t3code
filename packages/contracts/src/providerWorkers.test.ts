import { describe, expect, it } from "vite-plus/test";

import {
  AuthOrchestrationOperateScope,
  AuthProvidersManageScope,
  AuthSettingsWriteScope,
  AuthSourceControlWriteScope,
} from "./auth.ts";
import { CLIENT_GUARDED_RPC_SCOPES, clientRpcRequiredScopes } from "./clientRpcPermissions.ts";
import { RpcScopeAuthorization, WS_METHODS, WsRpcGroup } from "./rpc.ts";

describe("retired worker inspector transport", () => {
  it("does not register inspector reads, grants, controls or receipt recovery", () => {
    const operations = [
      "observe",
      "list",
      "detail",
      "requests",
      "activity",
      "result",
      "historyList",
      "openView",
      "closeView",
      "acknowledge",
      "historyRead",
      "routeRead",
      "controlDetail",
      "activate",
      "action",
      "receipt",
    ];
    const methods: readonly string[] = Object.values(WS_METHODS);
    for (const operation of operations) {
      const method = `providerWorkers.${operation}`;
      expect(methods).not.toContain(method);
      expect(WsRpcGroup.requests.has(method)).toBe(false);
      expect(Object.hasOwn(CLIENT_GUARDED_RPC_SCOPES, method)).toBe(false);
    }
    expect(
      [...WsRpcGroup.requests.keys()].filter((tag) => tag.startsWith("providerWorkers.")),
    ).toEqual([]);
  });

  it("keeps ordinary provider and orchestration requests scope-authorized", () => {
    for (const method of [
      WS_METHODS.providerAuthStart,
      WS_METHODS.providerAuthComplete,
      WS_METHODS.providerAuthRespond,
      WS_METHODS.providerAuthCancel,
      WS_METHODS.providerAuthLogout,
      WS_METHODS.providerAuthSubscribe,
      WS_METHODS.scheduledTasksRunNow,
    ]) {
      const rpc = WsRpcGroup.requests.get(method);
      expect(rpc).toBeDefined();
      expect(rpc?.middlewares.has(RpcScopeAuthorization)).toBe(true);
    }
    expect(clientRpcRequiredScopes(WS_METHODS.scheduledTasksRunNow, undefined)).toEqual([
      AuthOrchestrationOperateScope,
    ]);
    expect(clientRpcRequiredScopes(WS_METHODS.vcsPull, undefined)).toEqual([
      AuthSourceControlWriteScope,
    ]);
  });

  it("preserves provider administration independently of ordinary settings writes", () => {
    const providerInstanceMutation = {
      operation: "upsert",
      instanceId: "pi",
      instance: { driver: "pi", config: {} },
    };
    expect(clientRpcRequiredScopes(WS_METHODS.serverUpdateSettings, {
      patch: {},
      providerInstanceMutation,
    })).toEqual([AuthProvidersManageScope]);
    expect(clientRpcRequiredScopes(WS_METHODS.serverUpdateSettings, {
      patch: { defaultRuntimeMode: "full-access" },
    })).toEqual([AuthSettingsWriteScope]);
    expect(clientRpcRequiredScopes(WS_METHODS.serverUpdateSettings, {
      patch: { defaultRuntimeMode: "full-access" },
      providerInstanceMutation,
    })).toEqual([AuthSettingsWriteScope, AuthProvidersManageScope]);
    expect(() => clientRpcRequiredScopes(WS_METHODS.serverUpdateSettings, {
      patch: { defaultRuntimeMode: "invalid" },
    })).toThrow();
  });
});
