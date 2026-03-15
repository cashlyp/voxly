import type { ActionTelemetry } from '@/hooks/admin-dashboard/useDashboardActions';

type DashboardStatusRailProps = {
  loading: boolean;
  error: string;
  notice: string;
  busyAction: string;
  syncModeLabel: string;
  pollFailureCount: number;
  streamFailureCount: number;
  bridgeHardFailures: number;
  bridgeSoftFailures: number;
  actionTelemetry: ActionTelemetry;
};

function statusLabel(status: ActionTelemetry['status']): string {
  switch (status) {
    case 'running':
      return 'Running';
    case 'success':
      return 'Healthy';
    case 'error':
      return 'Errored';
    default:
      return 'Idle';
  }
}

export function DashboardStatusRail({
  loading,
  error,
  notice,
  busyAction,
  syncModeLabel,
  pollFailureCount,
  streamFailureCount,
  bridgeHardFailures,
  bridgeSoftFailures,
  actionTelemetry,
}: DashboardStatusRailProps) {
  const hasActionTelemetry = actionTelemetry.action.length > 0;
  return (
    <section className="va-status-rail">
      <div className="va-card va-status-card">
        <div className="va-status-chip-row">
          <span className="va-meta-chip">Sync {syncModeLabel}</span>
          <span className="va-meta-chip">Poll failures {pollFailureCount}</span>
          <span className="va-meta-chip">Stream failures {streamFailureCount}</span>
          <span className="va-meta-chip">Bridge 5xx {bridgeHardFailures}</span>
          <span className="va-meta-chip">Bridge 4xx/5xx {bridgeSoftFailures}</span>
          <span className="va-meta-chip">Busy {busyAction || 'none'}</span>
        </div>
        {hasActionTelemetry ? (
          <p className="va-status-line">
            Action {actionTelemetry.action} • Status {statusLabel(actionTelemetry.status)} • Trace {actionTelemetry.traceHint || 'n/a'} • Latency {actionTelemetry.latencyMs}ms
          </p>
        ) : null}
        {actionTelemetry.status === 'error' && actionTelemetry.error ? (
          <p className="va-error">{actionTelemetry.error}</p>
        ) : null}
      </div>
      {loading ? <p className="va-muted">Preparing dashboard…</p> : null}
      {error ? <p className="va-error">{error}</p> : null}
      {notice ? <p className="va-notice">{notice}</p> : null}
    </section>
  );
}
