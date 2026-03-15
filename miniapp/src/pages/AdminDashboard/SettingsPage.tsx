type SettingsModule = {
  id: string;
  label: string;
};

type SettingsFeatureFlag = {
  key: string;
  enabled: boolean;
  source: 'server' | 'default';
  defaultEnabled: boolean;
  description: string;
};

type SettingsPageProps = {
  userLabel: string;
  sessionRole: string;
  sessionRoleSource: string;
  pollingPaused: boolean;
  loading: boolean;
  busy: boolean;
  settingsStatusLabel: string;
  apiBaseUrl: string;
  visibleModules: SettingsModule[];
  featureFlags: SettingsFeatureFlag[];
  featureFlagsSourceLabel: string;
  featureFlagsUpdatedAtLabel: string;
  onTogglePolling: () => void;
  onSyncNow: () => void;
  onRetrySession: () => void;
  onJumpToModule: (moduleId: string) => void;
};

function moduleBadge(moduleId: string): { token: string; tone: string } {
  switch (moduleId) {
    case 'ops':
      return { token: '◉', tone: 'is-blue' };
    case 'sms':
      return { token: '✉', tone: 'is-green' };
    case 'mailer':
      return { token: '✦', tone: 'is-violet' };
    case 'provider':
      return { token: '⛭', tone: 'is-orange' };
    case 'content':
      return { token: '✎', tone: 'is-cyan' };
    case 'users':
      return { token: '◎', tone: 'is-yellow' };
    case 'audit':
      return { token: '⚑', tone: 'is-red' };
    default:
      return { token: '•', tone: 'is-blue' };
  }
}

export function SettingsPage({
  userLabel,
  sessionRole,
  sessionRoleSource,
  pollingPaused,
  loading,
  busy,
  settingsStatusLabel,
  apiBaseUrl,
  visibleModules,
  featureFlags,
  featureFlagsSourceLabel,
  featureFlagsUpdatedAtLabel,
  onTogglePolling,
  onSyncNow,
  onRetrySession,
  onJumpToModule,
}: SettingsPageProps) {
  const actionsDisabled = loading || busy;

  return (
    <section className="va-settings-page">
      <div className="va-settings-title-wrap">
        <h2 className="va-settings-title">General Settings</h2>
        <p className="va-muted">Wallet-style controls for your admin console.</p>
      </div>

      <div className="va-settings-group">
        <h3>Profile</h3>
        <div className="va-settings-row">
          <span className="va-settings-icon is-cyan">◎</span>
          <div>
            <strong>{userLabel}</strong>
            <p className="va-muted">Role: {sessionRole} ({sessionRoleSource})</p>
          </div>
          <span className="va-settings-meta">Active</span>
        </div>
      </div>

      <div className="va-settings-group">
        <h3>Operations</h3>
        <button type="button" className="va-settings-row va-settings-row-btn" onClick={onTogglePolling}>
          <span className="va-settings-icon is-blue">⟳</span>
          <div>
            <strong>Live Polling</strong>
            <p className="va-muted">Pause or resume background sync jobs.</p>
          </div>
          <span className="va-settings-trail">
            <span className="va-settings-meta">{pollingPaused ? 'Paused' : 'On'}</span>
            <span className="va-settings-arrow" aria-hidden>›</span>
          </span>
        </button>
        <button
          type="button"
          className="va-settings-row va-settings-row-btn"
          onClick={onSyncNow}
          disabled={actionsDisabled}
        >
          <span className="va-settings-icon is-green">↻</span>
          <div>
            <strong>Sync Now</strong>
            <p className="va-muted">Fetch latest bootstrap and queue snapshots.</p>
          </div>
          <span className="va-settings-trail">
            <span className="va-settings-meta">Run</span>
            <span className="va-settings-arrow" aria-hidden>›</span>
          </span>
        </button>
        <button
          type="button"
          className="va-settings-row va-settings-row-btn"
          onClick={onRetrySession}
          disabled={actionsDisabled}
        >
          <span className="va-settings-icon is-orange">⌁</span>
          <div>
            <strong>Retry Session</strong>
            <p className="va-muted">Refresh Mini App session token and authorization.</p>
          </div>
          <span className="va-settings-trail">
            <span className="va-settings-meta">Reset</span>
            <span className="va-settings-arrow" aria-hidden>›</span>
          </span>
        </button>
      </div>

      <div className="va-settings-group">
        <h3>System</h3>
        <div className="va-settings-row">
          <span className="va-settings-icon is-violet">⚙</span>
          <div>
            <strong>Settings Button</strong>
            <p className="va-muted">Telegram native settings trigger state.</p>
          </div>
          <span className="va-settings-meta">{settingsStatusLabel}</span>
        </div>
        <div className="va-settings-row">
          <span className="va-settings-icon is-yellow">⌘</span>
          <div>
            <strong>API Base URL</strong>
            <p className="va-muted">{apiBaseUrl || 'same-origin'}</p>
          </div>
          <span className="va-settings-meta">Bound</span>
        </div>
      </div>

      <div className="va-settings-group">
        <h3>Feature Flags</h3>
        <div className="va-settings-row">
          <span className="va-settings-icon is-blue">⚑</span>
          <div>
            <strong>Flag Snapshot</strong>
            <p className="va-muted">Source: {featureFlagsSourceLabel}</p>
          </div>
          <span className="va-settings-meta">{featureFlagsUpdatedAtLabel}</span>
        </div>
        {featureFlags.length === 0 ? (
          <p className="va-muted va-settings-empty">No feature flags loaded.</p>
        ) : (
          featureFlags.map((flag) => (
            <div key={flag.key} className="va-settings-row">
              <span className={`va-settings-icon ${flag.enabled ? 'is-green' : 'is-orange'}`}>
                {flag.enabled ? '✓' : '—'}
              </span>
              <div>
                <strong>{flag.key}</strong>
                <p className="va-muted">{flag.description}</p>
              </div>
              <span className="va-settings-trail">
                <span className={`va-settings-flag-state ${flag.enabled ? 'is-enabled' : 'is-disabled'}`}>
                  {flag.enabled ? 'Enabled' : 'Disabled'}
                </span>
                <span className="va-settings-meta">
                  {flag.source === 'server' ? 'Server' : `Default ${flag.defaultEnabled ? 'On' : 'Off'}`}
                </span>
              </span>
            </div>
          ))
        )}
      </div>

      <div className="va-settings-group">
        <h3>Modules</h3>
        {visibleModules.length === 0 ? (
          <p className="va-muted">No modules available for this role.</p>
        ) : (
          visibleModules.map((module) => {
            const badge = moduleBadge(module.id);
            return (
              <button
                key={module.id}
                type="button"
                className="va-settings-row va-settings-row-btn"
                onClick={() => onJumpToModule(module.id)}
              >
                <span className={`va-settings-icon ${badge.tone}`}>{badge.token}</span>
                <div>
                  <strong>{module.label}</strong>
                  <p className="va-muted">Open module workspace.</p>
                </div>
                <span className="va-settings-trail">
                  <span className="va-settings-meta">Open</span>
                  <span className="va-settings-arrow" aria-hidden>›</span>
                </span>
              </button>
            );
          })
        )}
      </div>
    </section>
  );
}
