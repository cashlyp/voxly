import {
  Banner,
  Button,
  Cell,
  List,
  Placeholder,
  Section,
} from "@telegram-apps/telegram-ui";
import { useCallback, useEffect, useState } from "react";
import { apiFetch, createIdempotencyKey } from "../lib/api";
import { trackEvent } from "../lib/telemetry";
import { confirmAction, hapticError, hapticSuccess } from "../lib/ux";
import { useUser } from "../state/user";

type ProviderStatus = {
  ok: boolean;
  provider: string;
  supported_providers: string[];
  stored_provider: string;
  aws_ready: boolean;
  twilio_ready: boolean;
  vonage_ready: boolean;
  vonage_ready_label?: string;
};

type SwitchResponse = {
  ok: boolean;
  provider: string;
  message?: string;
};

const PROVIDER_INFO: Record<
  string,
  { name: string; emoji: string; description: string }
> = {
  twilio: {
    name: "Twilio",
    emoji: "☁️",
    description: "PSTN calls via Twilio phone numbers",
  },
  aws: {
    name: "AWS Connect",
    emoji: "🏗️",
    description: "Voice service via Amazon Connect",
  },
  vonage: {
    name: "Vonage",
    emoji: "📡",
    description: "Communications platform as a service",
  },
};

export function Provider() {
  const { roles } = useUser();
  const isAdmin = roles.includes("admin");

  const [status, setStatus] = useState<ProviderStatus | null>(null);
  const [loading, setLoading] = useState(true);
  const [switching, setSwitching] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);

  const loadStatus = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await apiFetch<ProviderStatus>("/admin/provider");
      setStatus(response);
    } catch (err) {
      setError(
        err instanceof Error ? err.message : "Failed to load provider status",
      );
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadStatus();
  }, [loadStatus]);

  const handleSwitch = async (provider: string) => {
    if (!isAdmin) return;
    if (status?.provider === provider) return;

    const providerInfo = PROVIDER_INFO[provider] || {};
    const confirmed = await confirmAction({
      title: `Switch to ${providerInfo.name}?`,
      message: `Calls will be routed through ${providerInfo.description}.`,
      confirmText: "Switch",
      destructive: false,
    });
    if (!confirmed) return;

    setSwitching(true);
    setError(null);
    setSuccess(null);
    try {
      await apiFetch<SwitchResponse>("/admin/provider", {
        method: "POST",
        body: { provider },
        idempotencyKey: createIdempotencyKey(),
      });
      hapticSuccess();
      setSuccess(`Switched to ${providerInfo.name}`);
      trackEvent("provider_switched", { provider });
      await loadStatus();
    } catch (err) {
      hapticError();
      setError(
        err instanceof Error ? err.message : "Failed to switch provider",
      );
      trackEvent("provider_switch_failed", { provider });
    } finally {
      setSwitching(false);
    }
  };

  if (!isAdmin) {
    return (
      <div className="wallet-page">
        <Banner
          type="inline"
          header="Access denied"
          description="Only administrators can change providers."
        />
      </div>
    );
  }

  if (loading) {
    return (
      <div className="wallet-page">
        <Placeholder header="Loading..." description="Provider settings" />
      </div>
    );
  }

  if (!status) {
    return (
      <div className="wallet-page">
        <Banner
          type="inline"
          header="Error"
          description="Could not load provider status"
        />
      </div>
    );
  }

  const isReady = (provider: string) => {
    switch (provider) {
      case "twilio":
        return status.twilio_ready;
      case "aws":
        return status.aws_ready;
      case "vonage":
        return status.vonage_ready;
      default:
        return false;
    }
  };

  return (
    <div className="wallet-page">
      {error && <Banner type="inline" header="Error" description={error} />}
      {success && (
        <Banner type="inline" header="Success" description={success} />
      )}

      <List className="wallet-list">
        <Section header="Current provider" className="wallet-section">
          <div className="provider-hero">
            <div className="provider-status-badge active">
              {PROVIDER_INFO[status.provider]?.emoji || "🌐"}
            </div>
            <div className="provider-info">
              <div className="provider-name">
                {PROVIDER_INFO[status.provider]?.name ||
                  status.provider.toUpperCase()}
              </div>
              <div className="provider-description">
                {PROVIDER_INFO[status.provider]?.description ||
                  "Current voice provider"}
              </div>
            </div>
          </div>
        </Section>

        <Section header="Available providers" className="wallet-section">
          {status.supported_providers.map((provider) => {
            const info = PROVIDER_INFO[provider] || {
              name: provider,
              emoji: "🌐",
              description: "",
            };
            const ready = isReady(provider);
            const current = status.provider === provider;

            return (
              <Cell
                key={provider}
                subtitle={
                  current
                    ? "✅ Active"
                    : ready
                      ? "🟢 Ready"
                      : "🔴 Not configured"
                }
                after={
                  !current && (
                    <Button
                      size="s"
                      mode={ready ? "filled" : "outline"}
                      disabled={switching || !ready}
                      title={
                        !ready ? "Provider not fully configured" : undefined
                      }
                      onClick={() => void handleSwitch(provider)}
                    >
                      {ready ? "Switch" : "Configure"}
                    </Button>
                  )
                }
              >
                <div
                  style={{ display: "flex", alignItems: "center", gap: "8px" }}
                >
                  <span>{info.emoji}</span>
                  <div>
                    <div>{info.name}</div>
                  </div>
                </div>
              </Cell>
            );
          })}
        </Section>

        <Section header="Readiness" className="wallet-section">
          <Cell
            subtitle={
              status.twilio_ready ? "✅ Configured" : "⚠️ Missing credentials"
            }
          >
            Twilio
          </Cell>
          <Cell
            subtitle={
              status.aws_ready ? "✅ Configured" : "⚠️ Missing credentials"
            }
          >
            AWS Connect
          </Cell>
          <Cell
            subtitle={
              status.vonage_ready ? "✅ Configured" : "⚠️ Missing credentials"
            }
          >
            Vonage
          </Cell>
        </Section>

        <Section header="Stored default" className="wallet-section">
          <Cell subtitle={status.stored_provider || "Not set"}>
            Fallback provider
          </Cell>
        </Section>

        <Section header="Actions" className="wallet-section">
          <div className="section-actions">
            <Button
              size="s"
              mode="bezeled"
              disabled={loading || switching}
              onClick={() => void loadStatus()}
            >
              🔄 Refresh status
            </Button>
          </div>
        </Section>
      </List>
    </div>
  );
}
