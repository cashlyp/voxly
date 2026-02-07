import {
  Banner,
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
import { apiFetch } from "../lib/api";
import { resolveRoleTier } from "../lib/roles";
import { navigate } from "../lib/router";
import { useUser } from "../state/user";

type PingResponse = {
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

export function Settings() {
  const { user, roles, refresh, logout } = useUser();
  const roleTier = useMemo(() => resolveRoleTier(roles), [roles]);
  const isAdmin = roleTier === "admin";

  const [ping, setPing] = useState<PingResponse | null>(null);
  const [pingLatency, setPingLatency] = useState<number | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const loadPing = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const start = Date.now();
      const response = await apiFetch<PingResponse>("/webapp/ping");
      setPingLatency(Date.now() - start);
      setPing(response);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load status");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadPing();
  }, [loadPing]);

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
        {error !== null && error !== "" && (
          <Banner type="inline" header="Error" description={error} />
        )}

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

        <Section header="Service status" className="wallet-section">
          {loading && ping === null ? (
            <Placeholder header="Loading status" description="Checking API health" />
          ) : ping ? (
            <>
              <Cell subtitle="Environment" after={<Chip mode="mono">{ping.environment}</Chip>}>
                API
              </Cell>
              <Cell
                subtitle="Latency"
                after={
                  <Chip mode="mono">
                    {pingLatency !== null ? `${pingLatency}ms` : "-"}
                  </Chip>
                }
              >
                API latency
              </Cell>
              <Cell subtitle="Provider" after={<Chip mode="mono">{ping.provider.current}</Chip>}>
                Current provider
              </Cell>
              <Cell
                subtitle="Provider health"
                after={
                  <Chip mode={ping.provider.degraded ? "outline" : "mono"}>
                    {ping.provider.degraded ? "degraded" : "healthy"}
                  </Chip>
                }
              >
                Provider status
              </Cell>
              <Cell subtitle="Last error">
                {ping.provider.last_error_at ?? "-"}
              </Cell>
            </>
          ) : (
            <Placeholder header="Status unavailable" description="Unable to load status." />
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
