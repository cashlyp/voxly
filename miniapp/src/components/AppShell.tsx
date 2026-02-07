import { Banner, Tabbar } from "@telegram-apps/telegram-ui";
import { settingsButton } from "@tma.js/sdk";
import {
  addToHomeScreen,
  backButton,
  openLink,
  openTelegramLink,
  popup,
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
import { apiFetch } from "../lib/api";
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

const Scripts = lazy(() =>
  import("../routes/Scripts").then((module) => ({ default: module.Scripts })),
);
const Personas = lazy(() =>
  import("../routes/Personas").then((module) => ({ default: module.Personas })),
);
const CallerFlags = lazy(() =>
  import("../routes/CallerFlags").then((module) => ({
    default: module.CallerFlags,
  })),
);
const Sms = lazy(() =>
  import("../routes/Sms").then((module) => ({ default: module.Sms })),
);
const Email = lazy(() =>
  import("../routes/Email").then((module) => ({ default: module.Email })),
);
const Health = lazy(() =>
  import("../routes/Health").then((module) => ({ default: module.Health })),
);
const Users = lazy(() =>
  import("../routes/Users").then((module) => ({ default: module.Users })),
);

type TabItem = {
  label: string;
  path: string;
  icon: JSX.Element;
};

type PopupButtons = Parameters<NonNullable<typeof popup.show>>[0]["buttons"];

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
    label: "Calls",
    path: "/calls",
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

const adminMenuItems: TabItem[] = [
  {
    label: "Scripts",
    path: "/scripts",
    icon: (
      <svg className="tab-icon" viewBox="0 0 24 24" aria-hidden="true">
        <path
          d="M6 4h9l3 3v13H6z"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.6"
        />
        <path
          d="M9 12h6M9 16h4M9 8h3"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.6"
          strokeLinecap="round"
        />
      </svg>
    ),
  },
  {
    label: "Personas",
    path: "/personas",
    icon: (
      <svg className="tab-icon" viewBox="0 0 24 24" aria-hidden="true">
        <path
          d="M12 2c5.5 0 10 4.5 10 10s-4.5 10-10 10S2 17.5 2 12 6.5 2 12 2z"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.6"
        />
        <path
          d="M8 10a4 4 0 0 1 8 0"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.6"
          strokeLinecap="round"
        />
        <circle cx="12" cy="7" r="1.5" fill="currentColor" />
      </svg>
    ),
  },
  {
    label: "Flags",
    path: "/caller-flags",
    icon: (
      <svg className="tab-icon" viewBox="0 0 24 24" aria-hidden="true">
        <path
          d="M4 4h16v16H4z"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.6"
        />
        <path
          d="M7 8h10M7 12h10M7 16h6"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.6"
          strokeLinecap="round"
        />
      </svg>
    ),
  },
  {
    label: "Email",
    path: "/email",
    icon: (
      <svg className="tab-icon" viewBox="0 0 24 24" aria-hidden="true">
        <path
          d="M4 4h16l-2 8-14 0 2-8z"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.6"
          strokeLinejoin="round"
        />
        <path
          d="M4 4c-2 2-2 8-2 8 0 4 2 6 10 6s10-2 10-6c0 0 0-6-2-8"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.6"
          strokeLinecap="round"
        />
      </svg>
    ),
  },
  {
    label: "Health",
    path: "/health",
    icon: (
      <svg className="tab-icon" viewBox="0 0 24 24" aria-hidden="true">
        <path
          d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2z"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.6"
        />
        <path
          d="M12 6v6M15 12h-6"
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
];

const menuIcon = (
  <svg className="tab-icon" viewBox="0 0 24 24" aria-hidden="true">
    <path
      d="M3 6h18M3 12h18M3 18h18"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
    />
  </svg>
);

function AccessDenied({ message }: { message: string }) {
  return <div className="panel">{message}</div>;
}

function RouteRenderer({ route, role }: { route: RouteMatch; role: RoleTier }) {
  if (!canAccessRoute(role, route.name)) {
    return (
      <AccessDenied
        message={t("banner.auth.unauthorized.body", "Admin access required.")}
      />
    );
  }
  const { roles } = useUser();
  const isAdmin = roles.includes("admin");

  switch (route.name) {
    case "dashboard":
      return <Dashboard />;
    case "inbox":
      return <Inbox />;
    case "calls":
      return <Calls />;
    case "callConsole":
      return <CallConsole callSid={route.params.callSid} />;
    case "scripts":
      return (
        <Suspense fallback={<SkeletonPanel title="Loading scripts" />}>
          <Scripts />
        </Suspense>
      );
    case "personas":
      return (
        <Suspense fallback={<SkeletonPanel title="Loading personas" />}>
          <Personas />
        </Suspense>
      );
    case "callerFlags":
      return (
        <Suspense fallback={<SkeletonPanel title="Loading caller flags" />}>
          <CallerFlags />
        </Suspense>
      );
    case "sms":
      return (
        <Suspense fallback={<SkeletonPanel title="Loading SMS center" />}>
          <Sms />
        </Suspense>
      );
    case "email":
      return (
        <Suspense fallback={<SkeletonPanel title="Loading email center" />}>
          <Email />
        </Suspense>
      );
    case "health":
      return (
        <Suspense fallback={<SkeletonPanel title="Loading health status" />}>
          <Health />
        </Suspense>
      );
    case "users":
      return (
        <Suspense fallback={<SkeletonPanel title="Loading users" />}>
          {isAdmin ? (
            <Users />
          ) : (
            <AccessDenied message="Admin access required." />
          )}
        </Suspense>
      );
    case "settings":
      return isAdmin ? (
        <Settings />
      ) : (
        <AccessDenied message="Admin access required." />
      );
    default:
      return <AccessDenied message="Route not found." />;
  }
}

function CallsBootstrap({ activeCallSid }: { activeCallSid?: string | null }) {
  const { fetchCalls, fetchInboundQueue, fetchCall } = useCalls();
  const { status } = useUser();

  useEffect(() => {
    if (status !== "ready") return;
    fetchCalls({ limit: 10 }).catch(() => {});
    fetchInboundQueue().catch(() => {});
    if (activeCallSid) {
      fetchCall(activeCallSid).catch(() => {});
    }
  }, [status, activeCallSid, fetchCalls, fetchInboundQueue, fetchCall]);

  return null;
}

export function AppShell() {
  const { status, user, roles, error, errorKind, environment, tenantId } =
    useUser();
  const roleTier = useMemo(() => resolveRoleTier(roles), [roles]);
  const isAdmin = roleTier === "admin";
  const [isOnline, setIsOnline] = useState(() =>
    typeof navigator !== "undefined" ? navigator.onLine : true,
  );
  const [path, setPath] = useState(getHashPath());
  const menuBusyRef = useRef(false);
  const restoredRef = useRef(false);
  const [health, setHealth] = useState<{
    degraded: boolean;
    lastErrorAt?: string | null;
  } | null>(null);

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
    const handleOnline = () => setIsOnline(true);
    const handleOffline = () => setIsOnline(false);
    window.addEventListener("online", handleOnline);
    window.addEventListener("offline", handleOffline);
    return () => {
      window.removeEventListener("online", handleOnline);
      window.removeEventListener("offline", handleOffline);
    };
  }, []);

  useEffect(() => {
    saveUiState({ path, activeCallSid });
  }, [path, activeCallSid]);

  useEffect(() => {
    if (status !== "ready" || restoredRef.current) return;
    const saved = loadUiState();
    if (saved.path && saved.path !== path) {
      const savedRoute = matchRoute(saved.path);
      if (canAccessRoute(roleTier, savedRoute.name)) {
        navigate(saved.path);
      }
    }
    restoredRef.current = true;
  }, [status, path, roleTier]);

  const headerSubtitle = useMemo(() => {
    if (status === "loading") return "Authorizing...";
    if (status === "error") return error || "Auth failed";
    if (!user) return "Not connected";
    const roleLabel = isAdmin
      ? "Admin"
      : roleTier === "operator"
        ? "Operator"
        : "Read-only";
    return `${roleLabel} • ${user.username || user.first_name || user.id}`;
  }, [status, error, user, isAdmin, roleTier]);

  const botUsername = import.meta.env.VITE_BOT_USERNAME || "";
  const botUrl = botUsername ? `https://t.me/${botUsername}` : "";
  const termsUrl = import.meta.env.VITE_TERMS_URL || "";
  const privacyUrl = import.meta.env.VITE_PRIVACY_URL || "";
  const addToHomeSupported = addToHomeScreen?.isAvailable?.() ?? false;

  const handleOpenBot = useCallback(() => {
    if (!botUrl) return;
    if (openTelegramLink.ifAvailable !== undefined) {
      openTelegramLink.ifAvailable(botUrl);
    } else {
      openLink(botUrl);
    }
  }, [botUrl]);

  const handleOpenSettings = useCallback(() => navigate("/settings"), []);

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

  const showAdminMenu = useCallback(async () => {
    if (!popup.show?.isAvailable?.()) return;
    if (menuBusyRef.current === true) return;
    menuBusyRef.current = true;
    try {
      const buttons: PopupButtons = adminMenuItems.map((item) => ({
        id: item.path,
        type: "default" as const,
        text: item.label,
      }));
      buttons.push({ type: "close" });
      const result = await popup.show({
        title: "Admin Tools",
        message: "Select a tool",
        buttons,
      });
      if (result && result !== "close") {
        navigate(result);
      }
    } finally {
      menuBusyRef.current = false;
    }
  }, [adminMenuItems]);

  const showLegalMenu = useCallback(async () => {
    if (!popup.show?.isAvailable?.()) return;
    const result = await popup.show({
      title: "Legal",
      message: "View policies",
      buttons: [
        { id: "terms", type: "default", text: "Terms" },
        { id: "privacy", type: "default", text: "Privacy" },
        { type: "close" },
      ],
    });
    if (result === "terms") handleOpenUrl(termsUrl);
    if (result === "privacy") handleOpenUrl(privacyUrl);
  }, [handleOpenUrl, privacyUrl, termsUrl]);

  const showMoreMenu = useCallback(async () => {
    if (!popup.show?.isAvailable?.()) return;
    const buttons: PopupButtons = [];
    if (botUrl) {
      buttons.push({ id: "reload", type: "default", text: "Reload" });
    }
    if (addToHomeSupported) {
      buttons.push({ id: "add_home", type: "default", text: "Add to Home" });
    }
    buttons.push({ id: "legal", type: "default", text: "Legal" });
    const result = await popup.show({
      title: "More actions",
      message: "Extra options",
      buttons,
    });
    if (result === "reload") handleReload();
    if (result === "add_home") handleAddToHome();
    if (result === "legal") await showLegalMenu();
  }, [
    addToHomeSupported,
    botUrl,
    handleAddToHome,
    handleReload,
    showLegalMenu,
  ]);

  const showSettingsMenu = useCallback(async () => {
    if (!popup.show?.isAvailable?.()) return;
    if (menuBusyRef.current === true) return;
    menuBusyRef.current = true;
    try {
      const buttons: PopupButtons = [
        { id: "settings", type: "default", text: "Settings" },
      ];
      if (isAdmin) {
        buttons.push({ id: "admin", type: "default", text: "Admin Tools" });
      }
      buttons.push(
        botUrl
          ? { id: "bot", type: "default", text: "Open Bot" }
          : { id: "reload", type: "default", text: "Reload" },
        { id: "more", type: "default", text: "More" },
      );
      const result = await popup.show({
        title: "Menu",
        message: "Choose an action",
        buttons,
      });
      if (result === "settings") handleOpenSettings();
      if (result === "admin") await showAdminMenu();
      if (result === "bot") handleOpenBot();
      if (result === "reload") handleReload();
      if (result === "more") await showMoreMenu();
    } finally {
      menuBusyRef.current = false;
    }
  }, [
    botUrl,
    handleOpenBot,
    handleOpenSettings,
    handleReload,
    isAdmin,
    showAdminMenu,
    showMoreMenu,
  ]);

  useEffect(() => {
    if (!settingsButton?.show?.isAvailable?.()) return undefined;
    settingsButton.mount?.ifAvailable?.();
    settingsButton.show();
    const off = settingsButton.onClick(() => {
      void showSettingsMenu();
    });
    return () => {
      off?.();
      settingsButton.hide?.ifAvailable?.();
    };
  }, [showSettingsMenu]);

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
        }>("/webapp/ping");
        if (!cancelled) {
          setHealth({
            degraded: response.provider.degraded,
            lastErrorAt: response.provider.last_error_at || null,
          });
        }
      } catch {
        if (!cancelled) setHealth(null);
      }
    };
    fetchHealth();
    const timer = window.setInterval(fetchHealth, 60000);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [status]);

  const authBanner = useMemo(() => {
    if (status !== "error" || !error) return null;
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

  const envLabel = environment ? environment.toLowerCase() : "";

  const allNavItems = useMemo(() => {
    const items = [...navItems];
    if (isAdmin) {
      items.push({
        label: "Menu",
        path: "#menu",
        icon: menuIcon,
      } as TabItem);
    }
    return items;
  }, [navItems, isAdmin]);

  return (
    <CallsProvider>
      <div className="app-shell">
        <CallsBootstrap activeCallSid={activeCallSid} />
        <header className="wallet-topbar">
          <AppBrand subtitle="mini app" meta={headerSubtitle} />
        </header>

        {environment && (
          <div className={`env-ribbon env-${envLabel || "unknown"}`}>
            {t(`env.${envLabel}`, environment.toUpperCase())}
          </div>
        )}

        {authBanner && (
          <Banner
            type="inline"
            header={authBanner.header}
            description={authBanner.body}
            className="wallet-banner"
          />
        )}

        {health?.degraded && roleTier === "admin" && (
          <Banner
            type="inline"
            header="Degraded service"
            description={
              health.lastErrorAt
                ? `Provider errors detected. Last error at ${health.lastErrorAt}.`
                : "Provider errors detected."
            }
            className="wallet-banner"
          />
        )}

        {!isOnline && !authBanner && (
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
          <RouteRenderer route={route} role={roleTier} />
        </main>

        <Tabbar className="vn-tabbar">
          {allNavItems.map((item) => {
            const isActive =
              item.path === "#menu" ? false : route.path === item.path;
            return (
              <Tabbar.Item
                key={item.path}
                text={item.label}
                selected={isActive}
                className={`vn-tab ${isActive ? "is-active" : ""}`}
                onClick={() => {
                  if (item.path === "#menu") {
                    void showAdminMenu();
                  } else {
                    navigate(item.path);
                  }
                }}
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
