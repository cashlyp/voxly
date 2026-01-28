import { useCallback, useEffect, useState } from 'react';
import { Button, Cell, Chip, List, Placeholder, Section } from '@telegram-apps/telegram-ui';
import { apiFetch, createIdempotencyKey } from '../lib/api';

type SettingsResponse = {
  ok: boolean;
  provider: {
    current: string;
    supported: string[];
    readiness: Record<string, boolean>;
  };
  webhook_health?: {
    last_sequence?: number;
  };
};

type AuditLog = {
  id: number;
  user_id: string;
  action: string;
  call_sid?: string | null;
  metadata?: Record<string, unknown> | null;
  created_at: string;
};

export function Settings() {
  const [settings, setSettings] = useState<SettingsResponse | null>(null);
  const [switching, setSwitching] = useState(false);
  const [auditLogs, setAuditLogs] = useState<AuditLog[]>([]);
  const [auditCursor, setAuditCursor] = useState<number | null>(null);
  const [auditLoading, setAuditLoading] = useState(false);

  const loadSettings = useCallback(async () => {
    const response = await apiFetch<SettingsResponse>('/webapp/settings');
    setSettings(response);
  }, []);

  const loadAudit = useCallback(async (cursor = 0, append = false) => {
    setAuditLoading(true);
    try {
      const params = new URLSearchParams();
      params.set('limit', '20');
      if (cursor) params.set('cursor', String(cursor));
      const response = await apiFetch<{ ok: boolean; logs: AuditLog[]; next_cursor: number | null }>(
        `/webapp/audit?${params.toString()}`,
      );
      setAuditLogs((prev) => (append ? [...prev, ...(response.logs || [])] : (response.logs || [])));
      setAuditCursor(response.next_cursor ?? null);
    } finally {
      setAuditLoading(false);
    }
  }, []);

  useEffect(() => {
    loadSettings();
    loadAudit(0, false);
  }, [loadSettings, loadAudit]);

  const switchProvider = async (provider: string) => {
    setSwitching(true);
    try {
      await apiFetch('/webapp/settings/provider', {
        method: 'POST',
        body: { provider },
        idempotencyKey: createIdempotencyKey(),
      });
      await loadSettings();
    } finally {
      setSwitching(false);
    }
  };

  return (
    <div className="wallet-page">
      <List className="wallet-list">
      <Section header="Provider status" className="wallet-section">
        {!settings ? (
          <Placeholder header="Loading settings" description="Fetching provider status." />
        ) : (
          <>
            <Cell subtitle="Current provider">{settings.provider.current}</Cell>
            {settings.provider.supported.map((provider) => {
              const ready = settings.provider.readiness[provider];
              return (
                <Cell
                  key={provider}
                  subtitle={ready ? 'Ready' : 'Not configured'}
                  after={(
                    <Button
                      size="s"
                      mode="bezeled"
                      disabled={!ready || switching}
                      onClick={() => switchProvider(provider)}
                    >
                      Switch
                    </Button>
                  )}
                >
                  {provider}
                </Cell>
              );
            })}
          </>
        )}
      </Section>

      <Section header="Webhook health" className="wallet-section">
        <Cell
          subtitle="Latest event sequence"
          after={<Chip mode="mono">{settings?.webhook_health?.last_sequence ?? '-'}</Chip>}
        >
          Webhook status
        </Cell>
      </Section>

      <Section header="Audit log" footer="Latest admin actions" className="wallet-section">
        {auditLogs.length === 0 ? (
          <Placeholder header="No audit entries" description="Admin actions will appear here." />
        ) : (
          auditLogs.map((entry) => (
            <Cell
              key={entry.id}
              subtitle={entry.created_at}
              description={entry.call_sid || entry.user_id}
            >
              {entry.action}
            </Cell>
          ))
        )}
        {auditCursor && (
          <div className="section-actions">
            <Button
              size="s"
              mode="bezeled"
              disabled={auditLoading}
              onClick={() => loadAudit(auditCursor, true)}
            >
              {auditLoading ? 'Loading…' : 'Load more'}
            </Button>
          </div>
        )}
      </Section>
      </List>
    </div>
  );
}
