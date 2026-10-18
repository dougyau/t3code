import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";

import {
  navigateAfterThreadDeletion,
  requestThreadUnpinConfirmation,
  ThreadArchiveBlockedError,
  requestThreadWorktreeCleanup,
  reportThreadWorktreeCleanup,
} from "./useThreadActions";
import { toastManager } from "../components/ui/toast";

describe("manual checkout cleanup", () => {
  afterEach(() => vi.restoreAllMocks());

  it("offers the server-selected checkout and respects declining", async () => {
    let prompt = "";
    const eligibility = { eligible: true as const, worktreePath: "/checkout/feature" };
    expect(
      await requestThreadWorktreeCleanup({
        eligibility,
        canRemove: true,
        confirm: async (message) => {
          prompt = message;
          return false;
        },
      }),
    ).toBeNull();
    expect(prompt).toContain("\nfeature\n");
    expect(prompt).toContain("Safety checks may keep it");
    expect(
      await requestThreadWorktreeCleanup({
        eligibility,
        canRemove: true,
        confirm: async () => true,
      }),
    ).toBe("/checkout/feature");
  });

  it.each([
    { eligible: false as const, reason: "Automatic worktree cleanup is enabled" },
    { eligible: false as const, reason: "Checkout has another owning thread" },
  ])("does not offer an ineligible checkout: $reason", async (eligibility) => {
    const confirm = vi.fn(async () => true);
    expect(
      await requestThreadWorktreeCleanup({
        eligibility,
        canRemove: true,
        confirm,
      }),
    ).toBeNull();
    expect(confirm).not.toHaveBeenCalled();
  });

  it("does not offer deletion without the destination permission", async () => {
    const confirm = vi.fn(async () => true);
    expect(
      await requestThreadWorktreeCleanup({
        eligibility: { eligible: true, worktreePath: "/checkout/feature" },
        canRemove: false,
        confirm,
      }),
    ).toBeNull();
    expect(confirm).not.toHaveBeenCalled();
  });

  it.each(["kept", "failed"] as const)(
    "reports a %s checkout separately from successful thread deletion",
    (outcome) => {
      const addToast = vi.spyOn(toastManager, "add").mockReturnValue("cleanup-result");
      reportThreadWorktreeCleanup({
        kind: "worktree",
        outcome,
        reason: "Another owner exists",
        path: "/checkout/feature",
        threadId: ThreadId.make("deleted"),
        threadTitle: "Deleted",
        bytes: null,
        files: null,
      });
      expect(addToast).toHaveBeenCalledWith(
        expect.objectContaining({
          title:
            outcome === "kept" ? "Worktree kept" : "Thread deleted, but worktree cleanup failed",
          description: "Another owner exists",
        }),
      );
    },
  );
});

describe("navigateAfterThreadDeletion", () => {
  afterEach(() => vi.restoreAllMocks());

  it("reports a rejected navigation without failing the completed deletion", async () => {
    const addToast = vi.spyOn(toastManager, "add").mockReturnValue("navigation-error");

    await expect(
      navigateAfterThreadDeletion(() => Promise.reject(new Error("route unavailable"))),
    ).resolves.toBeUndefined();

    expect(addToast).toHaveBeenCalledOnce();
    expect(addToast).toHaveBeenCalledWith(
      expect.objectContaining({
        title: "Thread deleted, but navigation failed",
        description: "route unavailable",
      }),
    );
  });

  it("does not report an error after successful navigation", async () => {
    const addToast = vi.spyOn(toastManager, "add");

    await navigateAfterThreadDeletion(() => Promise.resolve());

    expect(addToast).not.toHaveBeenCalled();
  });
});

describe("ThreadArchiveBlockedError", () => {
  it("keeps the blocked thread context with the fixed message", () => {
    const error = new ThreadArchiveBlockedError({
      environmentId: EnvironmentId.make("environment-1"),
      threadId: ThreadId.make("thread-1"),
    });

    expect(error).toMatchObject({
      environmentId: "environment-1",
      threadId: "thread-1",
    });
    expect(error.message).toBe("Cannot archive while the provider is active.");
  });
});

describe("requestThreadUnpinConfirmation", () => {
  it("skips the dialog when confirmation is disabled", async () => {
    let callCount = 0;
    const result = await requestThreadUnpinConfirmation({
      enabled: false,
      title: "Pinned thread",
      confirm: async () => {
        callCount += 1;
        return false;
      },
    });

    expect(result).toMatchObject({ _tag: "Success", value: true });
    expect(callCount).toBe(0);
  });

  it("degrades gracefully when dialogs are unavailable", async () => {
    const result = await requestThreadUnpinConfirmation({
      enabled: true,
      title: "Pinned thread",
      confirm: null,
    });

    expect(result).toMatchObject({ _tag: "Success", value: true });
  });

  it("uses the thread title and returns the user's decision", async () => {
    let message = "";
    const result = await requestThreadUnpinConfirmation({
      enabled: true,
      title: "Release prep",
      confirm: async (nextMessage) => {
        message = nextMessage;
        return false;
      },
    });

    expect(message).toBe(
      'Unpin thread "Release prep"?\nThis will move the thread out of your pinned section.',
    );
    expect(result).toMatchObject({ _tag: "Success", value: false });
  });

  it("keeps dialog failures observable", async () => {
    const result = await requestThreadUnpinConfirmation({
      enabled: true,
      title: "Pinned thread",
      confirm: () => Promise.reject(new Error("dialog unavailable")),
    });

    expect(result._tag).toBe("Failure");
  });
});
