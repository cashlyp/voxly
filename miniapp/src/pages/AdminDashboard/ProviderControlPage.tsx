import type { DashboardVm, ProviderMatrixRow } from './types';

type ProviderControlPageProps = {
  visible: boolean;
  vm: DashboardVm;
};

export function ProviderControlPage({ visible, vm }: ProviderControlPageProps) {
  if (!visible) return null;

  const {
    providerReadinessTotals,
    providerDegradedCount,
    providerReadinessPercent,
    textBar,
    busyAction,
    providerPreflightBusy,
    preflightActiveProviders,
    loading,
    handleRefresh,
    providerMatrixRows,
    providerCurrentByChannel,
    providerSupportedByChannel,
    providerSwitchPlanByChannel,
    setProviderSwitchTarget,
    simulateProviderSwitchPlan,
    confirmProviderSwitchPlan,
    applyProviderSwitchPlan,
    resetProviderSwitchPlan,
    renderProviderSection,
  } = vm;

  return (
    <>
      <section className="va-grid">
        <div className="va-card">
          <h3>Provider Preflight Matrix</h3>
          <p>
            Ready providers: <strong>{providerReadinessTotals.ready}/{providerReadinessTotals.total}</strong>
            {' '}| Degraded: <strong>{providerDegradedCount}</strong>
          </p>
          <pre>{textBar(providerReadinessPercent)}</pre>
          <div className="va-inline-tools">
            <button
              type="button"
              disabled={busyAction.length > 0 || providerPreflightBusy.length > 0}
              onClick={() => { void preflightActiveProviders(); }}
            >
              Preflight Active Providers
            </button>
            <button
              type="button"
              disabled={loading || busyAction.length > 0}
              onClick={handleRefresh}
            >
              Refresh Matrix
            </button>
          </div>
          {providerMatrixRows.length === 0 ? (
            <p className="va-muted">Compatibility matrix is warming up.</p>
          ) : (
            <ul className="va-list va-matrix-list">
              {providerMatrixRows.map((row: ProviderMatrixRow) => (
                <li key={`matrix-${row.channel}-${row.provider}`}>
                  <strong>{row.channel.toUpperCase()} · {row.provider}</strong>
                  <span>
                    Ready: <strong>{row.ready ? 'yes' : 'no'}</strong>
                    {' '}| Degraded: <strong>{row.degraded ? 'yes' : 'no'}</strong>
                  </span>
                  <span>
                    Flows: <strong>{row.flowCount}</strong>
                    {' '}| Parity gaps: <strong>{row.parityGapCount}</strong>
                  </span>
                  {row.channel === 'call' ? (
                    <span>Payment mode: <strong>{row.paymentMode}</strong></span>
                  ) : null}
                </li>
              ))}
            </ul>
          )}
        </div>
      </section>
      <section className="va-grid">
        <div className="va-card">
          <h3>Staged Switch Planner</h3>
          <p className="va-muted">
            Safe flow: simulate target readiness, confirm intent, apply switch, then review health check.
          </p>
          <ul className="va-list">
            {(['call', 'sms', 'email'] as const).map((channel) => {
              const current = providerCurrentByChannel[channel] || 'unknown';
              const supported = providerSupportedByChannel[channel] || [];
              const plan = providerSwitchPlanByChannel[channel];
              return (
                <li key={`provider-plan-${channel}`}>
                  <strong>{channel.toUpperCase()} plan</strong>
                  <span>Current: {current}</span>
                  <div className="va-inline-tools">
                    <select
                      className="va-input"
                      value={plan.target}
                      onChange={(event) => setProviderSwitchTarget(channel, event.target.value)}
                    >
                      <option value="">Select provider target</option>
                      {supported.map((provider) => (
                        <option key={`switch-${channel}-${provider}`} value={provider}>{provider}</option>
                      ))}
                    </select>
                    <button
                      type="button"
                      disabled={busyAction.length > 0 || providerPreflightBusy.length > 0 || !plan.target}
                      onClick={() => { void simulateProviderSwitchPlan(channel); }}
                    >
                      Simulate
                    </button>
                    <button
                      type="button"
                      disabled={plan.stage !== 'simulated'}
                      onClick={() => confirmProviderSwitchPlan(channel)}
                    >
                      Confirm
                    </button>
                    <button
                      type="button"
                      disabled={busyAction.length > 0 || plan.stage !== 'confirmed'}
                      onClick={() => { void applyProviderSwitchPlan(channel); }}
                    >
                      Apply
                    </button>
                    <button type="button" onClick={() => resetProviderSwitchPlan(channel)}>
                      Reset
                    </button>
                  </div>
                  <span>Stage: <strong>{plan.stage}</strong> | Post-check: <strong>{plan.postCheck}</strong></span>
                  {plan.rollbackSuggestion ? (
                    <span>Rollback suggestion: <strong>{plan.rollbackSuggestion}</strong></span>
                  ) : null}
                </li>
              );
            })}
          </ul>
        </div>
        {renderProviderSection('call')}
        {renderProviderSection('sms')}
        {renderProviderSection('email')}
      </section>
    </>
  );
}
