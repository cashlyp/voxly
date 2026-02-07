import {
  Banner,
  Button,
  List,
  Placeholder,
  Section,
} from "@telegram-apps/telegram-ui";
import { useCallback, useEffect, useState } from "react";
import { apiFetch } from "../lib/api";
import { trackEvent } from "../lib/telemetry";
import { useUser } from "../state/user";

type HealthStatus = {
  ok: boolean;
  timestamp: string;
  uptime_seconds: number;
  environment: string;
  provider: {
    current: string;
    readiness: Record<string, boolean>;
    degraded: boolean;
    last_error_at?: string | null;
    last_success_at?: string | null;
  };
  database?: {
    connected: boolean;
    last_checked: string;
  };
  webhook?: {
    last_sequence?: number;
    last_event_at?: string | null;
  };
  memory?: {
    percentage: number;
  };
  api_version: string;
};

export function Health() {
  const { roles } = useUser();
  const isAdmin = roles.includes("admin");

  const [health, setHealth] = useState<HealthStatus | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);

  const loadHealth = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await apiFetch<HealthStatus>("/webapp/ping");
      setHealth(response);
      setLastUpdated(new Date());
      trackEvent("health_check");
    } catch (err) {
      setError(
        err instanceof Error ? err.message : "Failed to load health status",
      );
      trackEvent("health_check_failed");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadHealth();
    const interval = setInterval(loadHealth, 30000); // Refresh every 30 seconds
    return () => clearInterval(interval);
  }, [loadHealth]);

  const formatUptime = (seconds: number) => {
    const days = Math.floor(seconds / 86400);
    const hours = Math.floor((seconds % 86400) / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);

    if (days > 0) return `${days}d ${hours}h ${minutes}m`;
    if (hours > 0) return `${hours}h ${minutes}m`;
    return `${minutes}m`;
  };

  const getStatusColor = (status: boolean) => (status ? "#28a745" : "#dc3545");
  const getStatusText = (status: boolean) =>
    status ? "✓ Operational" : "✗ Down";

  if (loading && !health) {
    return (
      <div className="wallet-page">
        <Placeholder
          header="Loading Health Status"
          description="Please wait..."
        />
      </div>
    );
  }

  if (error || !health) {
    return (
      <div className="wallet-page">
        <Banner
          type="inline"
          header="Error"
          description={error || "Failed to load health status"}
        />
      </div>
    );
  }

  const isDegraded = health.provider?.degraded;

  return (
    <div className="wallet-page">
      <List className="wallet-list">
        {isDegraded && (
          <Banner
            type="inline"
            header="Warning"
            description="Provider Health Degraded - Some services may be impacted."
          />
        )}

        <Section header="System Status">
          <div className="status-grid">
            <div className="status-card">
              <div className="status-label">Uptime</div>
              <div
                className="status-value"
                style={{ color: getStatusColor(true) }}
              >
                {formatUptime(health.uptime_seconds)}
              </div>
            </div>

            <div className="status-card">
              <div className="status-label">Environment</div>
              <div className="status-value">{health.environment}</div>
            </div>

            <div className="status-card">
              <div className="status-label">API Version</div>
              <div className="status-value">{health.api_version}</div>
            </div>

            {health.memory && (
              <div className="status-card">
                <div className="status-label">Memory Usage</div>
                <div
                  className="status-value"
                  style={{
                    color:
                      health.memory.percentage > 80
                        ? "#dc3545"
                        : health.memory.percentage > 60
                          ? "#ffc107"
                          : "#28a745",
                  }}
                >
                  {Math.round(health.memory.percentage)}%
                </div>
              </div>
            )}
          </div>
        </Section>

        <Section header="Voice Provider">
          <div className="provider-section">
            <div className="provider-card">
              <div className="provider-header">
                <div className="provider-label">Current Provider</div>
                <div className="provider-name">
                  {health.provider?.current || "Unknown"}
                </div>
              </div>

              {isDegraded && (
                <div className="degraded-notice">
                  ⚠️ Provider health is degraded
                </div>
              )}

              {health.provider.last_error_at !== null &&
                health.provider.last_error_at !== undefined && (
                  <div className="error-info">
                    Last error:{" "}
                    {new Date(health.provider.last_error_at).toLocaleString()}
                  </div>
                )}

              {health.provider.last_success_at !== null &&
                health.provider.last_success_at !== undefined && (
                  <div className="success-info">
                    Last success:{" "}
                    {new Date(health.provider.last_success_at).toLocaleString()}
                  </div>
                )}
            </div>

            {health.provider?.readiness &&
              Object.keys(health.provider.readiness).length > 0 && (
                <div className="readiness-section">
                  <div className="section-title">Provider Readiness</div>
                  <div className="readiness-list">
                    {Object.entries(health.provider.readiness).map(
                      ([provider, ready]) => (
                        <div key={provider} className="readiness-item">
                          <div
                            className="readiness-icon"
                            style={{ color: getStatusColor(ready) }}
                          >
                            {getStatusText(ready)}
                          </div>
                          <div className="readiness-name">{provider}</div>
                        </div>
                      ),
                    )}
                  </div>
                </div>
              )}
          </div>
        </Section>

        {isAdmin && health.database && (
          <Section header="Database">
            <div className="status-box">
              <div className="status-item">
                <span>Connection</span>
                <span
                  style={{ color: getStatusColor(health.database.connected) }}
                >
                  {getStatusText(health.database.connected)}
                </span>
              </div>
              <div className="status-item">
                <span>Last Checked</span>
                <span>
                  {new Date(health.database.last_checked).toLocaleString()}
                </span>
              </div>
            </div>
          </Section>
        )}

        {isAdmin && health.webhook && (
          <Section header="Webhook">
            <div className="status-box">
              {health.webhook.last_sequence !== undefined && (
                <div className="status-item">
                  <span>Last Sequence</span>
                  <span>#{health.webhook.last_sequence}</span>
                </div>
              )}
              {health.webhook.last_event_at && (
                <div className="status-item">
                  <span>Last Event</span>
                  <span>
                    {new Date(health.webhook.last_event_at).toLocaleString()}
                  </span>
                </div>
              )}
            </div>
          </Section>
        )}

        <Section>
          <div className="section-actions">
            <Button
              size="m"
              mode="filled"
              onClick={() => void loadHealth()}
              disabled={loading}
            >
              {loading ? "Checking..." : "Refresh Status"}
            </Button>
          </div>
          {lastUpdated && (
            <div className="last-updated">
              Last updated: {lastUpdated.toLocaleTimeString()}
            </div>
          )}
        </Section>
      </List>

      <style>{`
        .status-grid {
          display: grid;
          grid-template-columns: 1fr 1fr;
          gap: 12px;
          margin: 12px 0;
        }

        .status-card {
          background: var(--tg-theme-secondary-bg-color, #f3f3f5);
          border-radius: 8px;
          padding: 12px;
          text-align: center;
        }

        .status-label {
          font-size: 12px;
          color: var(--tg-theme-hint-color, #8a8a8e);
          margin-bottom: 4px;
        }

        .status-value {
          font-size: 16px;
          font-weight: 600;
          color: var(--tg-theme-text-color);
        }

        .provider-section {
          display: flex;
          flex-direction: column;
          gap: 16px;
        }

        .provider-card {
          background: var(--tg-theme-secondary-bg-color, #f3f3f5);
          border-radius: 8px;
          padding: 16px;
        }

        .provider-header {
          margin-bottom: 12px;
        }

        .provider-label {
          font-size: 12px;
          color: var(--tg-theme-hint-color, #8a8a8e);
          margin-bottom: 4px;
        }

        .provider-name {
          font-size: 18px;
          font-weight: 600;
          color: var(--tg-theme-text-color);
          text-transform: capitalize;
        }

        .degraded-notice {
          background: rgba(255, 193, 7, 0.1);
          border-left: 3px solid #ffc107;
          padding: 8px 12px;
          margin: 8px 0;
          border-radius: 4px;
          font-size: 12px;
        }

        .error-info,
        .success-info {
          font-size: 12px;
          color: var(--tg-theme-hint-color, #8a8a8e);
          margin-top: 4px;
        }

        .readiness-section {
          margin-top: 12px;
        }

        .section-title {
          font-size: 12px;
          color: var(--tg-theme-hint-color, #8a8a8e);
          margin-bottom: 8px;
          font-weight: 600;
        }

        .readiness-list {
          display: flex;
          flex-direction: column;
          gap: 8px;
        }

        .readiness-item {
          display: flex;
          align-items: center;
          gap: 8px;
          padding: 8px;
          background: var(--tg-theme-bg-color, #fff);
          border-radius: 4px;
        }

        .readiness-icon {
          font-size: 12px;
          font-weight: 600;
          min-width: 80px;
        }

        .readiness-name {
          font-size: 14px;
          color: var(--tg-theme-text-color);
          text-transform: capitalize;
        }

        .status-box {
          background: var(--tg-theme-secondary-bg-color, #f3f3f5);
          border-radius: 8px;
          padding: 12px;
        }

        .status-item {
          display: flex;
          justify-content: space-between;
          padding: 8px 0;
          font-size: 14px;
          border-bottom: 1px solid var(--tg-theme-bg-color, #fff);
        }

        .status-item:last-child {
          border-bottom: none;
        }

        .last-updated {
          text-align: center;
          font-size: 12px;
          color: var(--tg-theme-hint-color, #8a8a8e);
          margin-top: 12px;
        }
      `}</style>
    </div>
  );
}
