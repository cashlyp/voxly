import { useCallback, useEffect, useState } from 'react';
import { apiFetch } from '../lib/api';

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

export function Settings() {
  const [settings, setSettings] = useState<SettingsResponse | null>(null);
  const [switching, setSwitching] = useState(false);

  const loadSettings = useCallback(async () => {
    const response = await apiFetch<SettingsResponse>('/webapp/settings');
    setSettings(response);
  }, []);

  useEffect(() => {
    loadSettings();
  }, [loadSettings]);

  const switchProvider = async (provider: string) => {
    setSwitching(true);
    try {
      await apiFetch('/webapp/settings/provider', {
        method: 'POST',
        body: { provider },
      });
      await loadSettings();
    } finally {
      setSwitching(false);
    }
  };

  return (
    <section className="stack">
      <div className="panel">
        <h2>Provider status</h2>
        {settings ? (
          <>
            <p className="muted">Current provider: {settings.provider.current}</p>
            <div className="list">
              {settings.provider.supported.map((provider) => (
                <div className="list-item" key={provider}>
                  <div>
                    <strong>{provider}</strong>
                    <p className="muted">
                      {settings.provider.readiness[provider] ? 'Ready' : 'Not configured'}
                    </p>
                  </div>
                  <button
                    type="button"
                    className="btn"
                    disabled={!settings.provider.readiness[provider] || switching}
                    onClick={() => switchProvider(provider)}
                  >
                    Switch
                  </button>
                </div>
              ))}
            </div>
          </>
        ) : (
          <p className="muted">Loading settings...</p>
        )}
      </div>

      <div className="panel">
        <h2>Webhook health</h2>
        <p className="muted">
          Latest event sequence: {settings?.webhook_health?.last_sequence ?? '-'}
        </p>
      </div>
    </section>
  );
}
