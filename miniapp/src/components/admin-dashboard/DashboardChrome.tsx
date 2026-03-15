type ModuleItem = {
  id: string;
  label: string;
};

type DashboardSettingsHeaderProps = {
  loading: boolean;
  busy: boolean;
  onBack: () => void;
  onSync: () => void;
};

type DashboardMainHeaderProps = {
  userLabel: string;
  sessionRole: string;
  sessionRoleSource: string;
  settingsStatusLabel: string;
  featureFlagsCount: number | string;
  moduleDetail: string;
  activeModuleGlyph: string;
  loading: boolean;
  busy: boolean;
  onOpenSettings: () => void;
  onRefresh: () => void;
};

type DashboardModuleNavProps = {
  modules: ModuleItem[];
  activeModuleId: string;
  onSelectModule: (moduleId: string) => void;
};

type DashboardBottomNavProps = {
  modules: ModuleItem[];
  activeModuleId: string;
  moduleGlyph: (moduleId: string) => string;
  onSelectModule: (moduleId: string) => void;
};

export function DashboardSettingsHeader({
  loading,
  busy,
  onBack,
  onSync,
}: DashboardSettingsHeaderProps) {
  return (
    <header className="va-header is-settings">
      <div className="va-settings-header-grid">
        <button
          type="button"
          className="va-settings-back va-btn va-btn-secondary"
          onClick={onBack}
          disabled={loading}
        >
          Back
        </button>
        <div className="va-settings-header-center">
          <strong>VOICEDNUT</strong>
          <span>mini app</span>
        </div>
        <button
          type="button"
          className="va-settings-header-action va-btn va-btn-secondary"
          onClick={onSync}
          disabled={loading || busy}
        >
          Sync
        </button>
      </div>
    </header>
  );
}

export function DashboardMainHeader({
  userLabel,
  sessionRole,
  sessionRoleSource,
  settingsStatusLabel,
  featureFlagsCount,
  moduleDetail,
  activeModuleGlyph,
  loading,
  busy,
  onOpenSettings,
  onRefresh,
}: DashboardMainHeaderProps) {
  return (
    <header className="va-header">
      <div className="va-header-copy">
        <h1>Voicednut Admin</h1>
        <p className="va-muted">Operational control center for providers, messaging, and incidents.</p>
        <p className="va-module-context-line va-muted">
          <span className="va-module-context-icon" aria-hidden>{activeModuleGlyph}</span>
          <span>{moduleDetail}</span>
        </p>
      </div>
      <div className="va-header-meta">
        <div className="va-meta-chips">
          <span className="va-meta-chip">Admin {userLabel}</span>
          <span className="va-meta-chip">Role {sessionRole}</span>
          <span className="va-meta-chip">Source {sessionRoleSource}</span>
          <span className="va-meta-chip">Settings {settingsStatusLabel}</span>
          <span className="va-meta-chip">Flags {featureFlagsCount}</span>
        </div>
        <div className="va-header-actions">
          <button
            type="button"
            className="va-btn va-btn-secondary"
            onClick={onOpenSettings}
            disabled={loading}
          >
            Settings
          </button>
          <button
            type="button"
            className="va-btn va-btn-primary"
            onClick={onRefresh}
            disabled={loading || busy}
          >
            Refresh
          </button>
        </div>
      </div>
    </header>
  );
}

export function DashboardModuleNav({
  modules,
  activeModuleId,
  onSelectModule,
}: DashboardModuleNavProps) {
  return (
    <section className="va-module-nav">
      {modules.map((module) => (
        <button
          key={module.id}
          type="button"
          className={`va-chip ${activeModuleId === module.id ? 'is-active' : ''}`}
          onClick={() => onSelectModule(module.id)}
        >
          {module.label}
        </button>
      ))}
    </section>
  );
}

export function DashboardBottomNav({
  modules,
  activeModuleId,
  moduleGlyph,
  onSelectModule,
}: DashboardBottomNavProps) {
  return (
    <nav className="va-bottom-nav-wrap" aria-label="Quick module navigation">
      <div className="va-bottom-nav">
        {modules.map((module) => (
          <button
            key={`bottom-${module.id}`}
            type="button"
            className={`va-bottom-nav-item ${activeModuleId === module.id ? 'is-active' : ''}`}
            onClick={() => onSelectModule(module.id)}
          >
            <span className="va-bottom-nav-glyph" aria-hidden>{moduleGlyph(module.id)}</span>
            <span>{module.label}</span>
          </button>
        ))}
      </div>
    </nav>
  );
}
