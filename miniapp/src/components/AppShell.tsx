import { Banner, Button, Tabbar } from "@telegram-apps/telegram-ui";
import { settingsButton } from "@tma.js/sdk";
import {
  addToHomeScreen,
  backButton,
  openLink,
  openTelegramLink,
} from "@tma.js/sdk-react";
import {
  Suspense,
  lazy,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { AppBrand } from "../components/AppBrand";
import { SkeletonPanel } from "../components/Skeleton";
import { ApiError, apiFetch, getApiBase, pingApi } from "../lib/api";
import { t } from "../lib/i18n";
import { canAccessRoute, resolveRoleTier, type RoleTier } from "../lib/roles";
import {
  getHashPath,
  matchRoute,
  navigate,
  type RouteMatch,
} from "../lib/router";
import { setTelemetryContext, trackEvent } from "../lib/telemetry";
import { loadUiState, saveUiState } from "../lib/uiState";
import { CallConsole } from "../routes/CallConsole";
import { Calls } from "../routes/Calls";
import { Dashboard } from "../routes/Dashboard";
import { Inbox } from "../routes/Inbox";
import { Settings } from "../routes/Settings";
import { CallsProvider, useCalls } from "../state/calls";
import { useUser } from "../state/user";

const Sms = lazy(() =>
  import("../routes/Sms").then((module) => ({ default: module.Sms })),
);
const Users = lazy(() =>
  import("../routes/Users").then((module) => ({ default: module.Users })),
);
const Transcripts = lazy(() =>
  import("../routes/Transcripts").then((module) => ({
    default: module.Transcripts,
  })),
);
const TranscriptDetail = lazy(() =>
  import("../routes/TranscriptDetail").then((module) => ({
    default: module.TranscriptDetail,
  })),
);
const Logs = lazy(() =>
  import("../routes/Logs").then((module) => ({ default: module.Logs })),
);
const ProviderStatus = lazy(() =>
  import("../routes/Provider").then((module) => ({
    default: module.Provider,
  })),
);

type TabItem = {
  label: string;
  path: string;
  icon: JSX.Element;
};

const baseTabs: TabItem[] = [
  {
    label: "Dashboard",
    path: "/",
    icon: (
      <svg className="tab-icon" viewBox="0 0 24 24" aria-hidden="true">
        <path
          d="M4 4h7v7H4zM13 4h7v4h-7zM13 10h7v10h-7zM4 13h7v7H4z"
          fill="currentColor"
        />
      </svg>
    ),
  },
  {
    label: "Call",
    path: "/inbox",
    icon: (
      <svg className="tab-icon" viewBox="0 0 24 24" aria-hidden="true">
        <path
          d="M6 4h12v16H6z"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.6"
        />
        <path
          d="M9 8h6M9 12h6M9 16h4"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.6"
          strokeLinecap="round"
        />
      </svg>
    ),
  },
  {
    label: "SMS",
    path: "/sms",
    icon: (
      <svg className="tab-icon" viewBox="0 0 24 24" aria-hidden="true">
        <path
          d="M3 8a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2v10a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8z"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.6"
          strokeLinejoin="round"
        />
        <path
          d="M3 8l9 6 9-6"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.6"
          strokeLinejoin="round"
        />
      </svg>
    ),
  },
  {
    label: "Settings",
    path: "/settings",
    icon: (
      <svg className="tab-icon" viewBox="0 0 24 24" aria-hidden="true">
        <path
          d="M12 8a4 4 0 1 1 0 8 4 4 0 0 1 0-8z"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.6"
        />
        <path
          d="M4 12h2m12 0h2M6.5 6.5l1.4 1.4m8.2 8.2 1.4 1.4M17.5 6.5l-1.4 1.4M8.1 16.1l-1.6 1.6"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.6"
          strokeLinecap="round"
        />
      </svg>
    ),
  },
];

type DrawerItem = {
  label: string;
  path: string;
  icon: JSX.Element;
  adminOnly?: boolean;
  matchRoutes?: string[];
};

const drawerNavItems: DrawerItem[] = [
  {
    label: "Calls history",
    path: "/calls",
    matchRoutes: ["calls", "callConsole"],
    icon: (
      <svg className="tab-icon" viewBox="0 0 24 24" aria-hidden="true">
        <path
          d="M6 4h12v16H6z"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.6"
        />
        <path
          d="M9 8h6M9 12h6M9 16h4"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.6"
          strokeLinecap="round"
        />
      </svg>
    ),
  },
  {
    label: "Transcripts",
    path: "/transcripts",
    matchRoutes: ["transcripts", "transcriptDetail"],
    icon: (
      <svg className="tab-icon" viewBox="0 0 24 24" aria-hidden="true">
        <path
          d="M4 6h16v9H7l-3 3V6z"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.6"
          strokeLinejoin="round"
        />
        <path
          d="M8 10h8M8 13h5"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.6"
          strokeLinecap="round"
        />
      </svg>
    ),
  },
  {
    label: "Provider status",
    path: "/provider",
    matchRoutes: ["provider"],
    adminOnly: true,
    icon: (
      <svg className="tab-icon" viewBox="0 0 24 24" aria-hidden="true">
        <path
          d="M4 12a8 8 0 1 1 16 0 8 8 0 0 1-16 0z"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.6"
        />
        <path
          d="M12 8v4l3 2"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.6"
          strokeLinecap="round"
        />
      </svg>
    ),
  },
  {
    label: "Users",
    path: "/users",
    matchRoutes: ["users"],
    adminOnly: true,
    icon: (
      <svg className="tab-icon" viewBox="0 0 24 24" aria-hidden="true">
        <path
          d="M7 18c0-2.2 2.2-4 5-4s5 1.8 5 4"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.6"
          strokeLinecap="round"
        />
        <circle
          cx="12"
          cy="9"
          r="3.2"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.6"
        />
      </svg>
    ),
  },
  {
    label: "Logs",
    path: "/logs",
    matchRoutes: ["logs"],
    adminOnly: true,
    icon: (
      <svg className="tab-icon" viewBox="0 0 24 24" aria-hidden="true">
        <path
          d="M4 4h16v16H4z"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.6"
        />
        <path
          d="M8 9h8M8 13h8M8 17h5"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.6"
          strokeLinecap="round"
        />
      </svg>
    ),
  },
];

function AccessDenied({ message }: { message: string }) {
  return <div className="panel">{message}</div>;
}

type ApiConnectivityState = "checking" | "ok" | "error" | "missing";

type ApiConnectivity = {
  state: ApiConnectivityState;
  latencyMs?: number | null;
  error?: { message: string; status?: number; code?: string } | null;
};

function ApiUnavailable({
  title,
  description,
  onRetry,
}: {
  title: string;
  description: string;
  onRetry: () => void;
}) {
  return (
    <div className="panel">
      <div className="skeleton-title">{title}</div>
      <div className="panel-subtitle">{description}</div>
      <div className="skeleton-line" />
      <div className="skeleton-line" />
      <div className="section-actions">
        <Button size="s" mode="bezeled" onClick={onRetry}>
          Retry
        </Button>
      </div>
    </div>
  );
}

function describeApiConnectivityError(error: unknown) {
  if (error instanceof ApiError) {
    if (error.code === "no_api_base") {
      return {
        message: "API URL not configured. Set VITE_API_URL in your environment.",
        status: 0,
        code: error.code,
      };
    }
    if (error.code === "timeout") {
      return {
        message: "API request timed out. Check your network connection.",
        status: 0,
        code: error.code,
      };
    }
    if (error.code === "network_error") {
      return {
        message: "CORS blocked or network error. Verify API URL and allowlist.",
        status: 0,
        code: error.code,
      };
    }
    return {
      message: error.message || "API request failed.",
      status: error.status,
      code: error.code,
    };
  }
  return {
    message: error instanceof Error ? error.message : "API request failed.",
  };
}

function RouteRenderer({
  route,
  role,
  status,
  errorMessage,
  errorKind,
  apiConnectivity,
  onRetry,
}: {
  route: RouteMatch;
  role: RoleTier;
  status: "idle" | "loading" | "ready" | "error";
  errorMessage?: string | null;
  errorKind?: string | null;
  apiConnectivity: ApiConnectivity;
  onRetry: () => void;
}) {
  const isSettingsRoute = route.name === "settings";

  if (apiConnectivity.state === "missing" && !isSettingsRoute) {
    return (
      <ApiUnavailable
        title="API URL not configured"
        description="Set VITE_API_URL for this Mini App."
        onRetry={onRetry}
      />
    );
  }

  if (apiConnectivity.state === "error" && !isSettingsRoute) {
    return (
      <ApiUnavailable
        title="Cannot reach API"
        description={apiConnectivity.error?.message ?? "Check connectivity."}
        onRetry={onRetry}
      />
    );
  }

  if ((status === "loading" || status === "idle") && !isSettingsRoute) {
    return <SkeletonPanel title="Authorizing..." />;
  }

  if (status === "error" && !isSettingsRoute) {
    if (errorKind === "offline") {
      return (
        <ApiUnavailable
          title="Cannot reach API"
          description={errorMessage ?? "Check connectivity."}
          onRetry={onRetry}
        />
      );
    }
    if (errorKind === "unauthorized") {
      return (
        <AccessDenied message="Not authorized for this Mini App." />
      );
    }
    return (
      <AccessDenied
        message={errorMessage ?? "Authentication failed. Please retry."}
      />
    );
  }

  if (!canAccessRoute(role, route.name) && !isSettingsRoute) {
    return (
      <AccessDenied
        message={t("banner.auth.unauthorized.body", "Access denied.")}
      />
    );
  }

  switch (route.name) {
    case "dashboard":
      return <Dashboard />;
    case "inbox":
      return <Inbox />;
    case "calls":
      return <Calls />;
    case "callConsole":
      return <CallConsole callSid={route.params.callSid} />;
    case "transcripts":
      return (
        <Suspense fallback={<SkeletonPanel title="Loading transcripts" />}>
          <Transcripts />
        </Suspense>
      );
    case "transcriptDetail":
      return (
        <Suspense fallback={<SkeletonPanel title="Loading transcript" />}>
          <TranscriptDetail callSid={route.params.callSid} />
        </Suspense>
      );
    case "sms":
      return (
        <Suspense fallback={<SkeletonPanel title="Loading SMS center" />}>
          <Sms />
        </Suspense>
      );
    case "provider":
      return (
        <Suspense fallback={<SkeletonPanel title="Loading provider status" />}>
          <ProviderStatus />
        </Suspense>
      );
    case "users":
      return (
        <Suspense fallback={<SkeletonPanel title="Loading users" />}>
          <Users />
        </Suspense>
      );
    case "logs":
      return (
        <Suspense fallback={<SkeletonPanel title="Loading logs" />}>
          <Logs />
        </Suspense>
      );
    case "settings":
      return <Settings />;
    default:
      return <AccessDenied message="Route not found." />;
  }
}

function CallsBootstrap({ activeCallSid }: { activeCallSid: string | null }) {
  const { fetchCalls, fetchInboundQueue, fetchCall } = useCalls();
  const { status } = useUser();

  useEffect(() => {
    if (status !== "ready") return;
    fetchCalls({ limit: 10 }).catch(() => {});
    fetchInboundQueue().catch(() => {});
    if (activeCallSid !== null && activeCallSid !== "") {
      fetchCall(activeCallSid).catch(() => {});
    }
  }, [status, activeCallSid, fetchCalls, fetchInboundQueue, fetchCall]);

  return null;
}

function InboundPoller() {
  const { fetchInboundQueue } = useCalls();
  const { status } = useUser();

  useEffect(() => {
    if (typeof window === "undefined" || typeof document === "undefined") {
      return undefined;
    }
    if (status !== "ready") return undefined;
    const baseDelay = 5000;
    const maxDelay = 60000;
    let delay = baseDelay;
    let timer: number | null = null;
    let cancelled = false;

    const schedule = (nextDelay: number) => {
      if (timer !== null) window.clearTimeout(timer);
      timer = window.setTimeout(() => {
        void run();
      }, nextDelay);
    };

    const run = async () => {
      if (cancelled) return;
      const isVisible =
        typeof document === "undefined" ||
        document.visibilityState === "visible";
      if (!isVisible) {
        schedule(baseDelay);
        return;
      }
      try {
        await fetchInboundQueue();
        delay = baseDelay;
      } catch {
        delay = Math.min(delay * 2, maxDelay);
      }
      schedule(delay);
    };

    void run();

    const handleResume = () => {
      if (document.visibilityState === "visible") {
        delay = baseDelay;
        void run();
      }
    };
    window.addEventListener("focus", handleResume);
    document.addEventListener("visibilitychange", handleResume);

    return () => {
      cancelled = true;
      if (timer !== null) window.clearTimeout(timer);
      window.removeEventListener("focus", handleResume);
      document.removeEventListener("visibilitychange", handleResume);
    };
  }, [status, fetchInboundQueue]);

  return null;
}

export function AppShell() {
  const {
    status,
    user,
    roles,
    error,
    errorKind,
    environment,
    tenantId,
    refresh,
  } = useUser();
  const roleTier = useMemo(() => resolveRoleTier(roles), [roles]);
  const isAdmin = roleTier === "admin";
  const apiBase = getApiBase();
  const [isOnline, setIsOnline] = useState(() =>
    typeof navigator !== "undefined" ? navigator.onLine : true,
  );
  const [path, setPath] = useState(getHashPath());
  const [drawerOpen, setDrawerOpen] = useState(false);
  const restoredRef = useRef(false);
  const [health, setHealth] = useState<{
    degraded: boolean;
    lastErrorAt?: string | null;
  } | null>(null);
  const [apiConnectivity, setApiConnectivity] = useState<ApiConnectivity>(() =>
    apiBase ? { state: "checking" } : { state: "missing" },
  );

  useEffect(() => {
    const handler = () => setPath(getHashPath());
    window.addEventListener("hashchange", handler);
    return () => window.removeEventListener("hashchange", handler);
  }, []);

  const route = matchRoute(path);
  const navItems = useMemo(() => baseTabs, []);
  const tabPaths = navItems.map((item) => item.path);
  const showBack = !tabPaths.includes(route.path);
  const activeCallSid =
    route.name === "callConsole" ? route.params.callSid : null;

  const runPing = useCallback(async () => {
    if (!apiBase) {
      setApiConnectivity({ state: "missing" });
      return;
    }
    setApiConnectivity((prev) => ({
      ...prev,
      state: "checking",
    }));
    try {
      const { payload, latencyMs } = await pingApi({ timeoutMs: 5000 });
      if (payload.ok) {
        setApiConnectivity({ state: "ok", latencyMs });
      } else {
        setApiConnectivity({
          state: "error",
          latencyMs,
          error: { message: "Ping failed. API returned not ok." },
        });
      }
    } catch (err) {
      const detail = describeApiConnectivityError(err);
      setApiConnectivity({
        state: detail.code === "no_api_base" ? "missing" : "error",
        error: detail,
      });
    }
  }, [apiBase]);

  const handleRetry = useCallback(() => {
    void runPing();
    if (apiBase) {
      void refresh();
    }
  }, [runPing, refresh, apiBase]);

  useEffect(() => {
    const handleBack = () => {
      if (window.history.length > 1) {
        window.history.back();
      } else {
        navigate("/");
      }
    };

    if (showBack) {
      backButton.show();
      const off = backButton.onClick(handleBack);
      return () => {
        off();
        backButton.hide();
      };
    }

    backButton.hide();
    return undefined;
  }, [showBack]);

  useEffect(() => {
    const handleOnline = () => {
      setIsOnline(true);
      void runPing();
    };
    const handleOffline = () => setIsOnline(false);
    window.addEventListener("online", handleOnline);
    window.addEventListener("offline", handleOffline);
    return () => {
      window.removeEventListener("online", handleOnline);
      window.removeEventListener("offline", handleOffline);
    };
  }, [runPing]);

  useEffect(() => {
    void runPing();
  }, [runPing]);

  useEffect(() => {
    saveUiState({ path, activeCallSid });
  }, [path, activeCallSid]);

  useEffect(() => {
    if (status !== "ready" || restoredRef.current) return;
    const saved = loadUiState();
    const savedPath = saved.path ?? "";
    if (savedPath !== "" && savedPath !== path) {
      const savedRoute = matchRoute(savedPath);
      if (canAccessRoute(roleTier, savedRoute.name)) {
        navigate(savedPath);
      }
    }
    restoredRef.current = true;
  }, [status, path, roleTier]);

  const headerSubtitle = useMemo(() => {
    if (status === "loading") return "Authorizing...";
    if (status === "error") return error ?? "Auth failed";
    if (!user) return "Not connected";
    const roleLabel = isAdmin
      ? "Admin"
      : roleTier === "operator"
        ? "Operator"
        : "Read-only";
    const displayName =
      user.username ?? user.first_name ?? String(user.id ?? "");
    return `${roleLabel} • ${displayName}`;
  }, [status, error, user, isAdmin, roleTier]);

  const botUsername = String(import.meta.env.VITE_BOT_USERNAME ?? "").trim();
  const botUrl = botUsername !== "" ? `https://t.me/${botUsername}` : "";
  const termsUrl = String(import.meta.env.VITE_TERMS_URL ?? "").trim();
  const privacyUrl = String(import.meta.env.VITE_PRIVACY_URL ?? "").trim();
  const addToHomeSupported = addToHomeScreen?.isAvailable?.() === true;

  const handleOpenBot = useCallback(() => {
    if (!botUrl) return;
    if (openTelegramLink.ifAvailable !== undefined) {
      openTelegramLink.ifAvailable(botUrl);
    } else {
      openLink(botUrl);
    }
  }, [botUrl]);

  const handleReload = useCallback(() => window.location.reload(), []);

  const handleOpenUrl = useCallback((url: string) => {
    if (!url) return;
    openLink(url);
  }, []);

  const handleAddToHome = useCallback(() => {
    if (addToHomeSupported) {
      addToHomeScreen();
    }
  }, [addToHomeSupported]);

  const closeDrawer = useCallback(() => setDrawerOpen(false), []);
  const toggleDrawer = useCallback(
    () => setDrawerOpen((prev) => !prev),
    [],
  );

  useEffect(() => {
    if (!settingsButton?.show?.isAvailable?.()) return undefined;
    settingsButton.mount?.ifAvailable?.();
    settingsButton.show();
    const off = settingsButton.onClick(() => {
      toggleDrawer();
    });
    return () => {
      off?.();
      settingsButton.hide?.ifAvailable?.();
    };
  }, [toggleDrawer]);

  useEffect(() => {
    if (!drawerOpen) return undefined;
    const original = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => {
      document.body.style.overflow = original;
    };
  }, [drawerOpen]);

  useEffect(() => {
    if (!drawerOpen) return;
    setDrawerOpen(false);
  }, [path, drawerOpen]);

  useEffect(() => {
    if (status !== "ready") return;
    setTelemetryContext({
      role: roleTier,
      environment: environment ?? null,
      tenant_id: tenantId ?? null,
    });
    trackEvent("console_opened");
  }, [status, roleTier, environment, tenantId]);

  useEffect(() => {
    if (status !== "ready") return;
    let cancelled = false;
    const fetchHealth = async () => {
      try {
        const response = await apiFetch<{
          provider: { degraded: boolean; last_error_at?: string | null };
        }>("/webapp/health");
        if (!cancelled) {
          setHealth({
            degraded: response.provider.degraded,
            lastErrorAt: response.provider.last_error_at ?? null,
          });
        }
      } catch {
        if (!cancelled) setHealth(null);
      }
    };
    void fetchHealth();
    const timer = window.setInterval(() => {
      void fetchHealth();
    }, 60000);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [status]);

  const authBanner = useMemo(() => {
    if (status !== "error" || error === null || error === "") return null;
    if (errorKind === "offline") {
      return {
        header: t("banner.offline.header", "You're offline"),
        body: t(
          "banner.offline.body",
          "Some data may be outdated. Reconnect to refresh.",
        ),
      };
    }
    if (errorKind === "unauthorized") {
      return {
        header: t("banner.auth.unauthorized.header", "Not authorized"),
        body: t(
          "banner.auth.unauthorized.body",
          "Please reopen the Mini App from the bot to sign in.",
        ),
      };
    }
    if (errorKind === "initdata") {
      return {
        header: t("banner.auth.initdata.header", "Session expired"),
        body: t("banner.auth.initdata.body", "Close and reopen the Mini App."),
      };
    }
    if (errorKind === "server") {
      return {
        header: t("banner.auth.server.header", "Server unavailable"),
        body: t("banner.auth.server.body", "Try again soon."),
      };
    }
    return {
      header: t("banner.error.header", "Something went wrong"),
      body: error,
    };
  }, [status, error, errorKind]);

  const apiBanner = useMemo(() => {
    if (apiConnectivity.state === "missing") {
      return {
        header: "API URL not configured",
        body: "Set VITE_API_URL for this Mini App.",
      };
    }
    if (apiConnectivity.state === "error") {
      const detail = apiConnectivity.error?.message ?? "Cannot reach API.";
      const statusLabel =
        apiConnectivity.error?.status && apiConnectivity.error?.status > 0
          ? ` (status ${apiConnectivity.error.status})`
          : "";
      return {
        header: "Cannot reach API",
        body: `${detail}${statusLabel}`,
      };
    }
    return null;
  }, [apiConnectivity]);

  const showAuthBanner =
    authBanner !== null &&
    !(errorKind === "offline" && apiConnectivity.state !== "ok");

  const envLabel = environment?.toLowerCase() ?? "";
  const environmentLabel = environment?.toUpperCase() ?? "";

  const activeTabPath =
    route.name === "callConsole" ? "/inbox" : route.path;

  const primaryDrawerItems = useMemo(
    () => drawerNavItems.filter((item) => item.adminOnly !== true),
    [],
  );
  const adminDrawerItems = useMemo(
    () => drawerNavItems.filter((item) => item.adminOnly === true),
    [],
  );

  const hasEnvironment = environmentLabel !== "";
  const hasBotUrl = botUrl !== "";
  const hasTermsUrl = termsUrl !== "";
  const hasPrivacyUrl = privacyUrl !== "";

  return (
    <CallsProvider>
      <div className="app-shell">
        <CallsBootstrap activeCallSid={activeCallSid} />
        <InboundPoller />
        <header className="wallet-topbar">
          <div className="topbar-spacer" aria-hidden="true" />
          <AppBrand subtitle="mini app" meta={headerSubtitle} className="topbar-brand" />
          <div className="topbar-actions">
            <button
              type="button"
              className="menu-button"
              onClick={toggleDrawer}
              aria-label="Open menu"
              aria-expanded={drawerOpen}
            >
              <svg className="tab-icon" viewBox="0 0 24 24" aria-hidden="true">
                <path
                  d="M3 6h18M3 12h18M3 18h18"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="2"
                  strokeLinecap="round"
                />
              </svg>
            </button>
          </div>
        </header>

        <div
          className={`drawer-backdrop ${drawerOpen ? "open" : ""}`}
          role="presentation"
          onClick={closeDrawer}
        />
        <aside
          className={`drawer-panel ${drawerOpen ? "open" : ""}`}
          aria-hidden={!drawerOpen}
        >
          <div className="drawer-header">
            <div>
              <div className="drawer-title">Menu</div>
              <div className="drawer-subtitle">{headerSubtitle}</div>
            </div>
            {isAdmin && <span className="admin-badge">Admin</span>}
          </div>

          <div className="drawer-section">
            <div className="drawer-section-title">Activity</div>
            {primaryDrawerItems.map((item) => {
              const matches = item.matchRoutes?.includes(route.name) === true;
              const active = matches || route.path === item.path;
              return (
                <button
                  key={item.path}
                  type="button"
                  className={`drawer-item ${active ? "active" : ""}`}
                  onClick={() => {
                    navigate(item.path);
                    closeDrawer();
                  }}
                >
                  <span className="drawer-icon">{item.icon}</span>
                  <span className="drawer-label">{item.label}</span>
                </button>
              );
            })}
          </div>

          {isAdmin && (
            <div className="drawer-section">
              <div className="drawer-section-title">
                Admin tools <span className="drawer-badge">Admin</span>
              </div>
              {adminDrawerItems.map((item) => {
                const matches = item.matchRoutes?.includes(route.name) === true;
                const active = matches || route.path === item.path;
                return (
                  <button
                    key={item.path}
                    type="button"
                    className={`drawer-item ${active ? "active" : ""}`}
                    onClick={() => {
                      navigate(item.path);
                      closeDrawer();
                    }}
                  >
                    <span className="drawer-icon">{item.icon}</span>
                    <span className="drawer-label">{item.label}</span>
                  </button>
                );
              })}
            </div>
          )}

          <div className="drawer-section">
            <div className="drawer-section-title">App</div>
            {hasBotUrl && (
              <button
                type="button"
                className="drawer-item"
                onClick={() => {
                  handleOpenBot();
                  closeDrawer();
                }}
              >
                <span className="drawer-label">Open bot</span>
              </button>
            )}
            {addToHomeSupported && (
              <button
                type="button"
                className="drawer-item"
                onClick={() => {
                  handleAddToHome();
                  closeDrawer();
                }}
              >
                <span className="drawer-label">Add to Home</span>
              </button>
            )}
            {hasTermsUrl && (
              <button
                type="button"
                className="drawer-item"
                onClick={() => {
                  handleOpenUrl(termsUrl);
                  closeDrawer();
                }}
              >
                <span className="drawer-label">Terms</span>
              </button>
            )}
            {hasPrivacyUrl && (
              <button
                type="button"
                className="drawer-item"
                onClick={() => {
                  handleOpenUrl(privacyUrl);
                  closeDrawer();
                }}
              >
                <span className="drawer-label">Privacy</span>
              </button>
            )}
            <button
              type="button"
              className="drawer-item"
              onClick={() => {
                handleReload();
                closeDrawer();
              }}
            >
              <span className="drawer-label">Reload</span>
            </button>
          </div>
        </aside>

        {hasEnvironment && (
          <div className={`env-ribbon env-${envLabel || "unknown"}`}>
            {t(`env.${envLabel}`, environmentLabel)}
          </div>
        )}

        {apiBanner && (
          <div className="wallet-banner-group">
            <Banner
              type="inline"
              header={apiBanner.header}
              description={apiBanner.body}
              className="wallet-banner"
            />
            <div className="banner-actions">
              <Button size="s" mode="bezeled" onClick={handleRetry}>
                Retry
              </Button>
            </div>
          </div>
        )}

        {showAuthBanner && (
          <Banner
            type="inline"
            header={authBanner?.header}
            description={authBanner?.body}
            className="wallet-banner"
          />
        )}

        {health?.degraded === true && roleTier === "admin" && (
          <Banner
            type="inline"
            header="Degraded service"
            description={
              health.lastErrorAt !== null && health.lastErrorAt !== ""
                ? `Provider errors detected. Last error at ${health.lastErrorAt}.`
                : "Provider errors detected."
            }
            className="wallet-banner"
          />
        )}

        {!isOnline && !showAuthBanner && apiBanner === null && (
          <Banner
            type="inline"
            header={t("banner.offline.header", "You're offline")}
            description={t(
              "banner.offline.body",
              "Some data may be outdated. Reconnect to refresh.",
            )}
            className="wallet-banner"
          />
        )}

        <main key={route.path} className="content" data-route={route.name}>
          <RouteRenderer
            route={route}
            role={roleTier}
            status={status}
            errorMessage={error}
            errorKind={errorKind}
            apiConnectivity={apiConnectivity}
            onRetry={handleRetry}
          />
        </main>

        <Tabbar className="vn-tabbar">
          {navItems.map((item) => {
            const isActive = activeTabPath === item.path;
            return (
              <Tabbar.Item
                key={item.path}
                text={item.label}
                selected={isActive}
                className={`vn-tab ${isActive ? "is-active" : ""}`}
                onClick={() => navigate(item.path)}
              >
                {item.icon}
              </Tabbar.Item>
            );
          })}
        </Tabbar>
      </div>
    </CallsProvider>
  );
}
