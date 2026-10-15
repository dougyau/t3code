import {
  createRef,
  act,
  createContext,
  create,
  type ReactTestRenderer,
} from "../../../../web/test/component-renderer";
import { EnvironmentId, MessageId, ThreadId } from "@t3tools/contracts";
import type { LegendListRef } from "@legendapp/list/react-native";
import { afterEach, expect, it, vi } from "vite-plus/test";

// Keep the real feed and its reading/follow lifecycle; substitute native hosts and
// unrelated media/network boundaries. The list exposes the same imperative state.
interface ListProps {
  data: readonly { id: string }[];
  maintainScrollAtEnd?: unknown;
  initialScrollAtEnd?: boolean;
  initialScrollIndex?: unknown;
  onScrollBeginDrag: () => void;
  onScroll: (event: {
    nativeEvent: {
      contentOffset: { y: number };
      contentSize: { height: number };
      layoutMeasurement: { height: number };
    };
  }) => void;
}
const list = vi.hoisted(() => ({
  props: {} as ListProps,
  blur: undefined as (() => void) | undefined,
}));
vi.mock("react", async () => (await import("../../../../web/test/component-renderer")).React);
vi.mock("react-native", () => ({
  Platform: { OS: "android", Version: 35 },
  View: "View",
  Text: "Text",
  Pressable: "Pressable",
  ScrollView: "ScrollView",
  Image: "Image",
  ActivityIndicator: "Spinner",
  Alert: { alert: vi.fn() },
  StyleSheet: { create: (value: unknown) => value },
  useWindowDimensions: () => ({ width: 400, fontScale: 1 }),
}));
vi.mock("@legendapp/list/keyboard", () => ({
  KeyboardAwareLegendList: (props: ListProps) => {
    list.props = props;
    return <div />;
  },
}));
vi.mock("@legendapp/list/react-native", () => ({ useViewabilityAmount: () => 1 }));
vi.mock("@react-navigation/native", async () => {
  const { useEffect } = (await import("../../../../web/test/component-renderer")).React;
  return {
    useNavigation: () => ({}),
    useFocusEffect: (effect: () => () => void) =>
      useEffect(() => {
        list.blur = effect();
        return () => list.blur?.();
      }, [effect]),
  };
});
vi.mock("@react-navigation/elements", async () => ({
  HeaderHeightContext: (
    await import("../../../../web/test/component-renderer")
  ).React.createContext(0),
}));
vi.mock("react-native-safe-area-context", () => ({
  useSafeAreaInsets: () => ({ top: 0, bottom: 0 }),
}));
vi.mock("react-native-reanimated", () => ({
  default: { View: "View" },
  FadeIn: { delay: () => ({ duration: () => undefined }) },
  FadeInUp: undefined,
}));
vi.mock("expo-haptics", () => ({ selectionAsync: vi.fn() }));

let renderer: ReactTestRenderer | undefined;
afterEach(async () => {
  await act(() => renderer?.unmount());
  vi.unstubAllGlobals();
});
const sharedValue = <T,>(value: T) => ({
  value,
  get: () => value,
  set: vi.fn(),
  addListener: vi.fn(),
  removeListener: vi.fn(),
  modify: vi.fn(),
});

it.each([false, true])(
  "captures on focus departure and restores the mobile feed reading intent (follow %s)",
  async (follow) => {
    vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
    vi.stubGlobal("requestAnimationFrame", () => 1);
    vi.stubGlobal("cancelAnimationFrame", vi.fn());
    const mobile = "../../";
    // These components are not the reading/navigation behavior under test.
    const components: Record<string, string[]> = {
      "features/threads/ThreadMarkdownVideo": ["ThreadMarkdownVideo", "ThreadMediaVisibleContext"],
      "features/threads/thread-context-divider": ["ThreadContextDivider"],
      "features/threads/thread-handoff-row": ["ThreadHandoffRow"],
      "features/threads/SecretRequestCard": ["SecretRequestCard"],
      "features/threads/worktree-setup-card": ["WorktreeWorkingHeader", "WorktreeSetupCard"],
      "components/ComposerContextSheet": ["ComposerContextSheet"],
      "components/AppSymbol": ["SymbolView"],
      "components/AppActivityIndicator": ["AppActivityIndicator"],
      "components/FilePreviewModal": ["FilePreviewModal"],
      "components/VideoPreviewModal": ["VideoPreviewModal"],
      "components/VideoAttachmentTile": ["VideoAttachmentTile"],
      "components/MediaVideoPlayer": ["MediaVideoPlayer"],
      "components/NativePresentation": ["PresentationSource"],
      "components/CopyTextButton": ["CopyTextButton"],
      "components/ControlPill": ["ControlPillMenu"],
      "components/PierreEntryIcon": ["PierreEntryIcon"],
      "components/AppText": ["AppText"],
      "features/threads/HtmlRenderWebView": ["ThreadHtmlRender", "htmlRenderRowHeight"],
      "features/threads/McpAppWebView": ["ThreadMcpApp"],
      "native/SelectableMarkdownText": ["SelectableMarkdownText"],
      "features/threads/ThreadMarkdownImage": [
        "MarkdownImageAvailableWidthContext",
        "ThreadMarkdownImage",
        "ThreadMarkdownImageUnavailable",
        "ThreadMarkdownImageView",
      ],
    };
    for (const [path, names] of Object.entries(components)) {
      vi.doMock(mobile + path, () =>
        Object.fromEntries(
          names.map((name) => [name, name.endsWith("Context") ? createContext(null) : name]),
        ),
      );
    }
    vi.doMock(mobile + "components/FilePreviewGallery", () => ({
      useFilePreviewGallery: () => (value: unknown) => value,
      FilePreviewGallery: "Gallery",
    }));
    vi.doMock(mobile + "native/NativeWorkspaceColumns", () => ({
      useNativeWorkspaceColumnsSupported: () => false,
    }));
    vi.doMock(mobile + "native/useOpenPullRequest", () => ({ useOpenPullRequest: () => vi.fn() }));
    vi.doMock(mobile + "native/useMarkdownImageSource", () => ({
      useMarkdownImageSource: () => vi.fn(),
    }));
    vi.doMock(mobile + "lib/useUniwindTheme", () => ({
      useUniwindTheme: () => new Proxy({}, { get: () => "#000000" }),
    }));
    vi.doMock(mobile + "lib/useFontFamily", () => ({ useFontFamily: () => "system" }));
    vi.doMock(mobile + "features/settings/appearance/AppearancePreferencesProvider", () => ({
      useAppearancePreferences: () => ({
        appearance: { baseFontSize: 14 },
        themeAppearance: "dark",
      }),
    }));
    vi.doMock(mobile + "features/review/ReviewCommentCard", () => ({
      ReviewCommentCard: "Card",
      useReviewCommentColors: () => ({}),
    }));
    vi.doMock(mobile + "features/threads/thread-work-log", () => ({
      collapsedWorkLogHeight: () => 50,
      ThreadDisclosureChevron: "Chevron",
      ThreadWorkGroupToggle: "Toggle",
      ThreadThinkingRow: "Thinking",
      ThreadWorkLog: "Work",
      WORK_GROUP_TOGGLE_HEIGHT: 40,
      THREAD_DISCLOSURE_TRANSITION_MS: 0,
    }));
    vi.doMock(mobile + "features/threads/markdownCodeHighlightState", () => ({
      useMarkdownCodeHighlight: () => ({}),
    }));
    vi.doMock(mobile + "features/threads/useFileChipShare", () => ({
      useFileChipShare: () => vi.fn(),
    }));
    vi.doMock(mobile + "state/assets", () => ({
      useAssetUrl: () => null,
      useAssetUrlState: () => null,
      useRefreshAssetUrl: () => vi.fn(),
      assetEnvironment: {},
    }));
    vi.doMock(mobile + "state/use-atom-query-runner", () => ({
      useAtomQueryRunner: () => vi.fn(),
    }));
    vi.doMock(mobile + "state/session", () => ({ usePreparedConnection: () => null }));
    vi.doMock(mobile + "state/entities", () => ({ useLiveThreadLinkLabels: () => ({}) }));
    vi.doMock(mobile + "state/use-thread-selection", () => ({ useThreadSelection: () => ({}) }));
    vi.doMock(mobile + "state/atom-registry", () => ({ appAtomRegistry: {} }));
    vi.doMock(mobile + "state/threads", () => ({
      environmentThreadShells: {},
      threadEnvironment: {},
    }));
    vi.doMock(mobile + "state/use-atom-command", () => ({ useAtomCommand: () => vi.fn() }));
    vi.doMock(mobile + "state/v2-item-support", () => ({ useV2ItemSupport: () => ({}) }));
    vi.doMock(mobile + "features/threads/threadForkNavigation", () => ({
      waitForThreadShellReady: vi.fn(),
    }));
    vi.doMock(mobile + "features/prompt-links/prompt-link-actions", () => ({
      copyPromptLink: vi.fn(),
    }));
    vi.doMock(mobile + "lib/attachmentDownload", () => ({ downloadAndShareAttachment: vi.fn() }));
    vi.doMock(mobile + "lib/composerContextClipboard", () => ({
      writeComposerContextClipboard: vi.fn(),
    }));
    vi.doMock(mobile + "lib/uuid", () => ({ uuidv4: () => "unused" }));
    vi.doMock(mobile + "lib/copyTextWithHaptic", () => ({ copyTextWithHaptic: vi.fn() }));
    vi.doMock(mobile + "lib/markdownFileIcons", () => ({ markdownFileIconSource: vi.fn() }));
    vi.doMock(mobile + "lib/markdownLinkIcons", () => ({ markdownLinkIconSource: vi.fn() }));
    vi.doMock(mobile + "features/threads/fileChipMenu", () => ({
      fileChipMenu: vi.fn(),
      resolveFileChipTarget: vi.fn(),
    }));

    const { ThreadFeed } = await import("./ThreadFeed");
    const ref = createRef<LegendListRef>();
    const row = {
      id: "public-message",
      createdAt: "2026-06-01T00:00:00Z",
      type: "message" as const,
      message: {
        id: MessageId.make("public-message"),
        role: "user" as const,
        text: "Coordinator prompt",
        createdAt: "2026-06-01T00:00:00Z",
        attachments: [],
        runId: null,
        streaming: false,
        visibility: "local" as const,
        sourceThreadId: ThreadId.make(`reading-parent-${follow}`),
        updatedAt: "2026-06-01T00:00:00Z",
      },
    };
    let scroll = 340;
    ref.current = {
      getState: () => ({
        scroll,
        start: 0,
        contentLength: 2000,
        scrollLength: 600,
        data: list.props.data,
        positionAtIndex: () => 0,
        indexByKey: () => 0,
        sizeAtIndex: () => 2000,
      }),
    } as unknown as LegendListRef;
    const props = {
      environmentId: EnvironmentId.make("remote"),
      threadId: ThreadId.make(`reading-parent-${follow}`),
      threadTitle: "Coordinator",
      contentPresentation: { kind: "ready" as const },
      feed: [row],
      queuedMessages: [],
      latestRun: null,
      listRef: ref,
      layoutVariant: "compact" as const,
      contentInsetEndAdjustment: sharedValue(0),
      workspaceRoot: null,
      onUseArtifactTemplate: vi.fn(),
      onEditPendingMessage: vi.fn(),
      dispatchingMessageId: null,
      submittedMessageId: null,
      anchorMessageId: null,
      activeWorkStartedAt: null,
      agentLabel: "Pi",
      freeze: sharedValue(false),
    };
    await act(() => {
      renderer = create(<ThreadFeed {...props} />);
    });
    if (!follow)
      await act(() => {
        list.props.onScrollBeginDrag();
        list.props.onScroll({
          nativeEvent: {
            contentOffset: { y: scroll },
            contentSize: { height: 2000 },
            layoutMeasurement: { height: 600 },
          },
        });
      });
    expect(Boolean(list.props.maintainScrollAtEnd)).toBe(follow);
    // The native navigator calls focus cleanup before moving to the worker.
    list.blur?.();
    await act(() => renderer?.unmount());
    scroll = 0;
    await act(() => {
      renderer = create(<ThreadFeed {...props} />);
    });
    expect(list.props.initialScrollAtEnd).toBe(follow);
    expect(list.props.initialScrollIndex).toEqual(
      follow ? undefined : { index: 0, viewOffset: -340 },
    );
    await act(() =>
      renderer?.update(<ThreadFeed {...props} feed={[row, { ...row, id: "later-event" }]} />),
    );
    expect(Boolean(list.props.maintainScrollAtEnd)).toBe(follow);
    // An identically named thread in a different environment has no saved anchor.
    await act(() => renderer?.unmount());
    await act(() => {
      renderer = create(<ThreadFeed {...props} environmentId={EnvironmentId.make("other")} />);
    });
    expect(list.props.initialScrollAtEnd).toBe(true);
    expect(list.props.initialScrollIndex).toBeUndefined();
    expect(Boolean(list.props.maintainScrollAtEnd)).toBe(true);
  },
);
