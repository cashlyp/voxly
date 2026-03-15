type ModuleErrorFallbackCardProps = {
  moduleLabel: string;
  onReload: () => void;
  reloadDisabled: boolean;
};

type SessionBlockedCardProps = {
  errorCode: string;
  onRetrySession: () => void;
  retryDisabled: boolean;
};

type EmptyModulesCardProps = {
  onRefreshAccess: () => void;
  refreshDisabled: boolean;
};

type ModuleSkeletonGridProps = {
  labels?: string[];
};

const DEFAULT_SKELETON_LABELS = ['Loading module', 'Preparing data', 'Syncing controls'];

export function ModuleErrorFallbackCard({
  moduleLabel,
  onReload,
  reloadDisabled,
}: ModuleErrorFallbackCardProps) {
  return (
    <div className="va-card va-module-fallback">
      <h3>{moduleLabel} is temporarily unavailable</h3>
      <p className="va-muted">
        This module hit a render-time error. Refresh data and reopen the module.
      </p>
      <button
        type="button"
        onClick={onReload}
        disabled={reloadDisabled}
      >
        Reload Module Data
      </button>
    </div>
  );
}

export function SessionBlockedCard({
  errorCode,
  onRetrySession,
  retryDisabled,
}: SessionBlockedCardProps) {
  return (
    <section className="va-grid">
      <div className="va-card va-blocked">
        <h3>Mini App Session Blocked</h3>
        <p>
          Code: <strong>{errorCode || 'miniapp_auth_invalid'}</strong>
        </p>
        <p>
          Open this Mini App from the Telegram bot menu, then tap <strong>Retry Session</strong>.
        </p>
        <button
          type="button"
          onClick={onRetrySession}
          disabled={retryDisabled}
        >
          Retry Session
        </button>
      </div>
    </section>
  );
}

export function EmptyModulesCard({
  onRefreshAccess,
  refreshDisabled,
}: EmptyModulesCardProps) {
  return (
    <section className="va-grid">
      <div className="va-card va-empty-state">
        <h3>No Modules Available</h3>
        <p className="va-muted">
          This role has no enabled modules yet. Ask an administrator to grant access, then refresh.
        </p>
        <button
          type="button"
          className="va-btn va-btn-primary"
          onClick={onRefreshAccess}
          disabled={refreshDisabled}
        >
          Refresh Access
        </button>
      </div>
    </section>
  );
}

export function ModuleSkeletonGrid({
  labels = DEFAULT_SKELETON_LABELS,
}: ModuleSkeletonGridProps) {
  return (
    <section className="va-grid va-module-skeleton-grid">
      {labels.map((label) => (
        <div key={label} className="va-card va-module-skeleton-card">
          <div className="va-module-skeleton-title" />
          <div className="va-module-skeleton-line" />
          <div className="va-module-skeleton-line short" />
          <p className="va-muted">{label}...</p>
        </div>
      ))}
    </section>
  );
}
