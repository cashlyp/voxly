import {
  Button,
  Cell,
  Chip,
  List,
  Placeholder,
  Section,
} from "@telegram-apps/telegram-ui";
import { openLink } from "@tma.js/sdk-react";
import { useCallback, useEffect, useMemo, useState } from "react";
import { AppBrand } from "../components/AppBrand";
import { ApiError, apiFetch, getApiBase, pingApi } from "../lib/api";
import { getInitData } from "../lib/auth";
import { resolveRoleTier } from "../lib/roles";
import { navigate } from "../lib/router";
import { useUser } from "../state/user";

type PingResponse = {
  ok: boolean;
  ts: string;
  version?: string | null;
};

type HealthResponse = {
  ok: boolean;
  server_time: string;
  environment: string;
  provider: {
    current: string;
    readiness: Record<string, boolean>;
    degraded: boolean;
    last_error_at?: string | null;
    last_success_at?: string | null;
  };
  webhook: {
    last_sequence?: number;
  };
};

type AccessResponse = {
  ok: boolean;
  admins: string[];
  operators?: string[];
  viewers: string[];
};

type DiagnosticsError = {
  status?: number;
  message: string;
  code?: string;
} | null;

function maskApiBase(raw: string): string {
  if (!raw) return "-";
  try {
    const parsed = new URL(raw);
    const hostParts = parsed.hostname.split(".").filter(Boolean);
    if (hostParts.length < 2) {
      return `${parsed.protocol}//${parsed.hostname}`;
    }
    const tld = hostParts[hostParts.length - 1];
    const domain = hostParts[hostParts.length - 2];
    const subdomain =
      hostParts.length > 2 ? hostParts.slice(0, -2).join(".") : "";
    const maskedDomain =
      domain.length <= 2
        ? `${domain[0] ?? "*"}*`
        : `${domain[0]}***${domain[domain.length - 1]}`;
    const maskedHost = [subdomain, `${maskedDomain}.${tld}`]
      .filter(Boolean)
      .join(".");
    return `${parsed.protocol}//${maskedHost}`;
  } catch {
    return "-";
  }
}

export function Settings() {
  const { user, roles, refresh, logout } = useUser();
  const roleTier = useMemo(() => resolveRoleTier(roles), [roles]);
  const isAdmin = roleTier === "admin";
  const apiBase = getApiBase();
  const maskedApiBase = useMemo(() => maskApiBase(apiBase), [apiBase]);
  const origin =
    typeof window !== "undefined" ? window.location.origin : "unknown";

  const [ping, setPing] = useState<PingResponse | null>(null);
  const [pingLatency, setPingLatency] = useState<number | null>(null);
  const [health, setHealth] = useState<HealthResponse | null>(null);
  const [healthLatency, setHealthLatency] = useState<number | null>(null);
  const [loading, setLoading] = useState(true);
  const [lastError, setLastError] = useState<DiagnosticsError>(null);
  const [accessLoading, setAccessLoading] = useState(false);
  const [accessError, setAccessError] = useState<string | null>(null);
  const [accessAdmins, setAccessAdmins] = useState<string[]>([]);
  const [accessOperators, setAccessOperators] = useState<string[]>([]);
  const [operatorsInput, setOperatorsInput] = useState("");

  const describeError = useCallback((err: unknown): DiagnosticsError => {
    if (err instanceof ApiError) {
      return {
        status: err.status,
        code: err.code,
        message: err.message,
      };
    }
    return {
      message: err instanceof Error ? err.message : "Failed to load status",
    };
  }, []);

  const loadDiagnostics = useCallback(async () => {
    setLoading(true);
    setLastError(null);
    setPing(null);
    setHealth(null);
    try {
      if (!apiBase) {
        setLastError({
          message: "API URL not configured. Set VITE_API_URL.",
        });
        return;
      }
      const pingResult = await pingApi({ timeoutMs: 5000 });
      setPingLatency(pingResult.latencyMs);
      setPing(pingResult.payload);
      const healthStart = Date.now();
      const healthResponse = await apiFetch<HealthResponse>("/webapp/health");
      setHealthLatency(Date.now() - healthStart);
      setHealth(healthResponse);
    } catch (err) {
      setLastError(describeError(err));
    } finally {
      setLoading(false);
    }
  }, [apiBase, describeError]);

  useEffect(() => {
    void loadDiagnostics();
  }, [loadDiagnostics]);

  const allowlistStatus = useMemo(() => {
    if (lastError?.code === "origin_not_allowed") return "blocked";
    if (ping?.ok) return "allowed";
    if (lastError) return "unknown";
    return "unknown";
  }, [lastError, ping]);

  const loadAccess = useCallback(async () => {
    if (!isAdmin) return;
    setAccessLoading(true);
    setAccessError(null);
    try {
      const initData = getInitData();
      if (!initData) {
        setAccessError("Telegram init data missing.");
        return;
      }
      const response = await apiFetch<AccessResponse>("/miniapp/access", {
        headers: { "X-Telegram-Init-Data": initData },
      });
      setAccessAdmins(response.admins ?? []);
      setAccessOperators(response.operators ?? []);
      setOperatorsInput((response.operators ?? []).join(","));
    } catch (err) {
      const described = describeError(err);
      setAccessError(described?.message ?? "Failed to load access lists.");
    } finally {
      setAccessLoading(false);
    }
  }, [isAdmin, describeError]);

  const saveAccess = useCallback(async () => {
    if (!isAdmin) return;
    setAccessLoading(true);
    setAccessError(null);
    try {
      const initData = getInitData();
      if (!initData) {
        setAccessError("Telegram init data missing.");
        return;
      }
      const response = await apiFetch<AccessResponse>("/miniapp/access", {
        method: "POST",
        headers: { "X-Telegram-Init-Data": initData },
        body: { operators: operatorsInput },
      });
      setAccessAdmins(response.admins ?? []);
      setAccessOperators(response.operators ?? []);
      setOperatorsInput((response.operators ?? []).join(","));
    } catch (err) {
      const described = describeError(err);
      setAccessError(described?.message ?? "Failed to update allowlist.");
    } finally {
      setAccessLoading(false);
    }
  }, [isAdmin, operatorsInput, describeError]);

  useEffect(() => {
    if (isAdmin) {
      void loadAccess();
    }
  }, [isAdmin, loadAccess]);

  const botUsername = String(import.meta.env.VITE_BOT_USERNAME ?? "").trim();
  const botUrl = botUsername !== "" ? `https://t.me/${botUsername}` : "";
  const termsUrl = String(import.meta.env.VITE_TERMS_URL ?? "").trim();
  const privacyUrl = String(import.meta.env.VITE_PRIVACY_URL ?? "").trim();
  const hasBotUrl = botUrl !== "";
  const hasTermsUrl = termsUrl !== "";
  const hasPrivacyUrl = privacyUrl !== "";

  const handleOpenUrl = useCallback((url: string) => {
    if (!url) return;
    openLink(url);
  }, []);

  return (
    <div className="wallet-page">
      <div className="settings-brand-card">
        <AppBrand subtitle="mini app" meta="Settings" />
      </div>

      <List className="wallet-list">
        <Section header="Account" className="wallet-section">
          <Cell subtitle="Telegram user">
            {user?.username ?? user?.first_name ?? String(user?.id ?? "Unknown")}
          </Cell>
          <Cell
            subtitle="User ID"
            after={<Chip mode="mono">{user?.id ?? "-"}</Chip>}
          >
            Session
          </Cell>
          <Cell subtitle="Role" after={<Chip mode="mono">{roleTier}</Chip>}>
            Access level
          </Cell>
          <div className="section-actions">
            <Button size="s" mode="bezeled" onClick={() => void refresh()}>
              Refresh session
            </Button>
            <Button size="s" mode="plain" onClick={logout}>
              Sign out
            </Button>
          </div>
        </Section>

        <Section header="Diagnostics" className="wallet-section">
          <Cell
            subtitle="API base URL"
            after={<Chip mode="mono">{maskedApiBase}</Chip>}
          >
            API configuration
          </Cell>
          <Cell subtitle="Origin" after={<Chip mode="mono">{origin}</Chip>}>
            Mini App origin
          </Cell>
          <Cell
            subtitle="Allowlist status"
            after={
              <Chip mode={allowlistStatus === "allowed" ? "mono" : "outline"}>
                {allowlistStatus}
              </Chip>
            }
          >
            Origin allowlist
          </Cell>
          <Cell
            subtitle="Ping result"
            after={
              <Chip mode={ping?.ok ? "mono" : "outline"}>
                {ping?.ok ? "ok" : "failed"}
              </Chip>
            }
          >
            Connectivity
          </Cell>
          <Cell
            subtitle="Ping latency"
            after={
              <Chip mode="mono">
                {pingLatency !== null ? `${pingLatency}ms` : "-"}
              </Chip>
            }
          >
            Latency
          </Cell>
          <Cell
            subtitle="Health status"
            after={
              <Chip mode={health?.ok ? "mono" : "outline"}>
                {health?.ok ? "ok" : "unknown"}
              </Chip>
            }
          >
            /webapp/health
          </Cell>
          <Cell
            subtitle="Health latency"
            after={
              <Chip mode="mono">
                {healthLatency !== null ? `${healthLatency}ms` : "-"}
              </Chip>
            }
          >
            Health latency
          </Cell>
          <Cell subtitle="Last error">
            {lastError
              ? `${lastError.message}${
                  lastError.status ? ` (status ${lastError.status})` : ""
                }`
              : "-"}
          </Cell>
          <div className="section-actions">
            <Button size="s" mode="bezeled" onClick={loadDiagnostics}>
              Retry diagnostics
            </Button>
          </div>
        </Section>

        <Section header="Service status" className="wallet-section">
          {loading && health === null ? (
            <Placeholder
              header="Loading status"
              description="Checking API health"
            />
          ) : health ? (
            <>
              <Cell
                subtitle="Environment"
                after={<Chip mode="mono">{health.environment}</Chip>}
              >
                API
              </Cell>
              <Cell
                subtitle="Latency"
                after={
                  <Chip mode="mono">
                    {healthLatency !== null ? `${healthLatency}ms` : "-"}
                  </Chip>
                }
              >
                API latency
              </Cell>
              <Cell
                subtitle="Provider"
                after={<Chip mode="mono">{health.provider.current}</Chip>}
              >
                Current provider
              </Cell>
              <Cell
                subtitle="Provider health"
                after={
                  <Chip mode={health.provider.degraded ? "outline" : "mono"}>
                    {health.provider.degraded ? "degraded" : "healthy"}
                  </Chip>
                }
              >
                Provider status
              </Cell>
              <Cell subtitle="Last error">
                {health.provider.last_error_at ?? "-"}
              </Cell>
            </>
          ) : (
            <Placeholder
              header="Status unavailable"
              description="Unable to load status."
            />
          )}
        </Section>

        <Section header="Quick links" className="wallet-section">
          <div className="section-actions">
            {hasBotUrl && (
              <Button size="s" mode="bezeled" onClick={() => handleOpenUrl(botUrl)}>
                Open bot
              </Button>
            )}
            {hasTermsUrl && (
              <Button size="s" mode="plain" onClick={() => handleOpenUrl(termsUrl)}>
                Terms
              </Button>
            )}
            {hasPrivacyUrl && (
              <Button size="s" mode="plain" onClick={() => handleOpenUrl(privacyUrl)}>
                Privacy
              </Button>
            )}
          </div>
        </Section>

        {isAdmin && (
          <Section header="Operator allowlist" className="wallet-section">
            <Cell
              subtitle="Admin IDs"
              after={<Chip mode="mono">{accessAdmins.length}</Chip>}
            >
              Admin users
            </Cell>
            {accessAdmins.length > 0 && (
              <div className="chip-list">
                {accessAdmins.map((id) => (
                  <Chip key={`admin-${id}`} mode="mono">
                    {id}
                  </Chip>
                ))}
              </div>
            )}
            <Cell
              subtitle="Operator IDs"
              after={<Chip mode="mono">{accessOperators.length}</Chip>}
            >
              Operators
            </Cell>
            {accessOperators.length > 0 && (
              <div className="chip-list">
                {accessOperators.map((id) => (
                  <Chip key={`operator-${id}`} mode="mono">
                    {id}
                  </Chip>
                ))}
              </div>
            )}
            <textarea
              className="allowlist-input"
              placeholder="Comma-separated Telegram user IDs"
              value={operatorsInput}
              onChange={(event) => setOperatorsInput(event.target.value)}
              rows={3}
            />
            {accessError && (
              <Cell subtitle="Error">{accessError}</Cell>
            )}
            <div className="section-actions">
              <Button
                size="s"
                mode="plain"
                onClick={() => void loadAccess()}
                disabled={accessLoading}
              >
                Reload
              </Button>
              <Button
                size="s"
                mode="bezeled"
                onClick={() => void saveAccess()}
                disabled={accessLoading}
              >
                Update allowlist
              </Button>
            </div>
          </Section>
        )}

        {isAdmin && (
          <Section header="Admin tools" className="wallet-section">
            <div className="section-actions">
              <Button
                size="s"
                mode="bezeled"
                onClick={() => navigate("/provider")}
              >
                Provider status
              </Button>
              <Button size="s" mode="bezeled" onClick={() => navigate("/users")}>
                Users
              </Button>
              <Button size="s" mode="bezeled" onClick={() => navigate("/logs")}>
                Logs
              </Button>
            </div>
          </Section>
        )}
      </List>
    </div>
  );
}
