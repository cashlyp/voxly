import type {
  ActivityEntry,
  CallLogRow,
  DashboardVm,
  DlqCallRow,
  DlqEmailRow,
  EmailJob,
} from './types';

type OpsDashboardPageProps = {
  visible: boolean;
  vm: DashboardVm;
};

export function OpsDashboardPage({ visible, vm }: OpsDashboardPageProps) {
  if (!visible) return null;

  const {
    isFeatureEnabled,
    isDashboardDegraded,
    syncModeLabel,
    streamModeLabel,
    streamLastEventLabel,
    streamFailureCount,
    pollFailureCount,
    bridgeHardFailures,
    bridgeSoftFailures,
    sloErrorBudgetPercent,
    sloP95ActionLatencyMs,
    pollFreshnessLabel,
    degradedCauses,
    lastPollLabel,
    lastSuccessfulPollLabel,
    nextPollLabel,
    callCompleted,
    callTotal,
    callFailed,
    callFailureRate,
    callSuccessRate,
    queueBacklogTotal,
    providerReadinessTotals,
    providerReadinessPercent,
    textBar,
    runtimeEffectiveMode,
    runtimeModeOverride,
    runtimeCanaryEffective,
    runtimeCanaryOverrideLabel,
    runtimeIsCircuitOpen,
    runtimeForcedLegacyUntil,
    runtimeActiveTotal,
    runtimeActiveLegacy,
    runtimeActiveVoiceAgent,
    busyAction,
    enableRuntimeMaintenance,
    disableRuntimeMaintenance,
    refreshRuntimeStatus,
    runtimeCanaryInput,
    setRuntimeCanaryInput,
    applyRuntimeCanary,
    clearRuntimeCanary,
    activityLog,
    formatTime,
    renderProviderSection,
    smsTotalRecipients,
    smsSuccess,
    smsFailed,
    smsProcessedPercent,
    emailTotalRecipients,
    emailSent,
    emailFailed,
    emailDelivered,
    emailBounced,
    emailComplained,
    emailSuppressed,
    emailProcessedPercent,
    emailDeliveredPercent,
    callLogs,
    callLogsTotal,
    toText,
    toInt,
    emailJobs,
    dlqPayload,
    callDlq,
    emailDlq,
    runAction,
    hasMeaningfulData,
  } = vm;
  const runtimeControlsEnabled = isFeatureEnabled('runtime_controls', true);
  const providerCardsEnabled = isFeatureEnabled('provider_cards', true);

  return (
    <>
      <section className="va-grid">
        <div className={`va-card va-health ${isDashboardDegraded ? 'is-degraded' : 'is-healthy'}`}>
          <h3>Live Sync Health</h3>
          <p>
            Mode: <strong>{syncModeLabel}</strong>
          </p>
          <p>
            Poll failures: <strong>{pollFailureCount}</strong> | Bridge 5xx: <strong>{bridgeHardFailures}</strong>
            {' '}| Bridge 4xx/5xx: <strong>{bridgeSoftFailures}</strong>
          </p>
          <p>
            Data feed: <strong>{streamModeLabel}</strong> | Stream failures: <strong>{streamFailureCount}</strong>
          </p>
          <p>Last poll attempt: <strong>{lastPollLabel}</strong></p>
          <p>Last successful poll: <strong>{lastSuccessfulPollLabel}</strong></p>
          <p>Last stream event: <strong>{streamLastEventLabel}</strong></p>
          <p>Next poll scheduled: <strong>{nextPollLabel}</strong></p>
          <details className="va-drilldown">
            <summary>Degraded causes drill-down</summary>
            {degradedCauses.length === 0 ? (
              <p className="va-muted">No active degradation causes detected.</p>
            ) : (
              <ul className="va-list">
                {degradedCauses.map((cause) => (
                  <li key={`degraded-cause-${cause}`}>{cause}</li>
                ))}
              </ul>
            )}
          </details>
        </div>

        <div className="va-card">
          <h3>SLO Widgets</h3>
          <p>
            Error budget remaining: <strong>{sloErrorBudgetPercent}%</strong>
          </p>
          <pre>{textBar(sloErrorBudgetPercent)}</pre>
          <p>
            p95 action latency: <strong>{sloP95ActionLatencyMs}ms</strong>
          </p>
          <p>
            Poll freshness: <strong>{pollFreshnessLabel}</strong>
          </p>
        </div>

        <div className="va-card">
          <h3>Ops Snapshot</h3>
          <p>
            Calls: <strong>{callCompleted}</strong> completed / <strong>{callTotal}</strong> total
          </p>
          <p>
            Call failures: <strong>{callFailed}</strong> ({callFailureRate}%)
          </p>
          <p>
            Success rate: <strong>{Math.max(0, Math.min(100, Math.round(callSuccessRate)))}%</strong>
          </p>
          <p>
            Queue backlog: <strong>{queueBacklogTotal}</strong>
          </p>
          <p>
            Provider readiness: <strong>{providerReadinessTotals.ready}/{providerReadinessTotals.total}</strong>
          </p>
          <pre>{textBar(providerReadinessPercent)}</pre>
        </div>

        {runtimeControlsEnabled ? (
          <div className="va-card">
            <h3>Voice Runtime Control</h3>
            <p>
              Effective mode: <strong>{runtimeEffectiveMode}</strong>
              {' '}| Override: <strong>{runtimeModeOverride}</strong>
            </p>
            <p>
              Canary effective: <strong>{runtimeCanaryEffective}%</strong>
              {' '}| Override: <strong>{runtimeCanaryOverrideLabel}</strong>
            </p>
            <p>
              Circuit: <strong>{runtimeIsCircuitOpen ? 'Open' : 'Closed'}</strong>
              {' '}| Forced legacy until: <strong>{runtimeForcedLegacyUntil}</strong>
            </p>
            <p>
              Active calls: <strong>{runtimeActiveTotal}</strong>
              {' '}| Legacy: <strong>{runtimeActiveLegacy}</strong>
              {' '}| Voice Agent: <strong>{runtimeActiveVoiceAgent}</strong>
            </p>
            <div className="va-inline-tools">
              <button
                type="button"
                disabled={busyAction.length > 0}
                onClick={() => { void enableRuntimeMaintenance(); }}
              >
                Enable Maintenance
              </button>
              <button
                type="button"
                disabled={busyAction.length > 0}
                onClick={() => { void disableRuntimeMaintenance(); }}
              >
                Disable Maintenance
              </button>
              <button
                type="button"
                disabled={busyAction.length > 0}
                onClick={() => { void refreshRuntimeStatus(); }}
              >
                Refresh Runtime
              </button>
            </div>
            <div className="va-inline-tools">
              <input
                className="va-input"
                inputMode="numeric"
                min={0}
                max={100}
                placeholder="Canary % (0-100)"
                value={runtimeCanaryInput}
                onChange={(event) => setRuntimeCanaryInput(event.target.value)}
              />
              <button
                type="button"
                disabled={busyAction.length > 0}
                onClick={() => { void applyRuntimeCanary(); }}
              >
                Apply Canary
              </button>
              <button
                type="button"
                disabled={busyAction.length > 0}
                onClick={() => { void clearRuntimeCanary(); }}
              >
                Clear Canary
              </button>
            </div>
          </div>
        ) : (
          <div className="va-card">
            <h3>Voice Runtime Control</h3>
            <p className="va-muted">Runtime controls are disabled by feature flag.</p>
          </div>
        )}

        <div className="va-card">
          <h3>Activity Timeline</h3>
          {activityLog.length === 0 ? (
            <p className="va-muted">No activity recorded yet.</p>
          ) : (
            <ul className="va-list va-list-activity">
              {activityLog.map((entry: ActivityEntry) => (
                <li key={entry.id}>
                  <span className={`va-pill va-pill-${entry.status}`}>{entry.status}</span>
                  <strong>{entry.title}</strong>
                  <span>{entry.detail}</span>
                  <span>{formatTime(entry.at)}</span>
                </li>
              ))}
            </ul>
          )}
        </div>
      </section>

      {providerCardsEnabled ? (
        <section className="va-grid">
          {renderProviderSection('call')}
          {renderProviderSection('sms')}
          {renderProviderSection('email')}
        </section>
      ) : null}

      <section className="va-grid">
        <div className="va-card">
          <h3>SMS Bulk Status (24h)</h3>
          <p>Total recipients: <strong>{smsTotalRecipients}</strong></p>
          <p>Successful: <strong>{smsSuccess}</strong> | Failed: <strong>{smsFailed}</strong></p>
          <pre>{textBar(smsProcessedPercent)}</pre>
        </div>

        <div className="va-card">
          <h3>Email Bulk Status (24h)</h3>
          <p>Total recipients: <strong>{emailTotalRecipients}</strong></p>
          <p>Sent: <strong>{emailSent}</strong> | Failed: <strong>{emailFailed}</strong></p>
          <p>Delivered: <strong>{emailDelivered}</strong></p>
          <p>
            Bounced: <strong>{emailBounced}</strong> | Complaints: <strong>{emailComplained}</strong>
            {' '}| Suppressed: <strong>{emailSuppressed}</strong>
          </p>
          <pre>{textBar(emailProcessedPercent)}</pre>
          <pre>{textBar(emailDeliveredPercent)}</pre>
        </div>
      </section>

      <section className="va-grid">
        <div className="va-card">
          <h3>Recent Call Logs</h3>
          <p>
            Showing <strong>{callLogs.length}</strong> of <strong>{callLogsTotal}</strong> recent calls.
          </p>
          {callLogs.length === 0 ? <p className="va-muted">No recent calls available.</p> : null}
          <ul className="va-list">
            {callLogs.slice(0, 10).map((row: CallLogRow, index: number) => {
              const callSid = toText(row.call_sid, `call-${index + 1}`);
              return (
                <li key={`call-log-${callSid}-${index}`}>
                  <strong>{callSid}</strong>
                  <span>
                    {toText(row.direction, 'unknown')} | {toText(row.status_normalized, toText(row.status, 'unknown'))}
                  </span>
                  <span>
                    Runtime: {toText(row.voice_runtime, 'unknown')}
                    {' '}| Duration: {toInt(row.duration)}s
                    {' '}| Transcripts: {toInt(row.transcript_count)}
                  </span>
                  <span>
                    Number: {toText(row.phone_number, 'n/a')}
                    {' '}| Ended: {toText(row.ended_reason, 'n/a')}
                  </span>
                  <span>{formatTime(row.created_at)}</span>
                </li>
              );
            })}
          </ul>
        </div>

        <div className="va-card">
          <h3>Email Jobs</h3>
          {emailJobs.length === 0 ? <p className="va-muted">No recent jobs.</p> : null}
          <ul className="va-list">
            {emailJobs.slice(0, 8).map((job: EmailJob, index: number) => {
              const jobId = toText(job.job_id, `job-${index + 1}`);
              const jobStatus = toText(job.status);
              const jobKey = `email-job-${jobId}-${index}`;
              return (
                <li key={jobKey}>
                  <strong>{jobId}</strong>
                  <span>{jobStatus}</span>
                  <span>{toInt(job.sent)}/{toInt(job.total)} sent</span>
                  <span>
                    Failed: {toInt(job.failed)} | Delivered: {toInt(job.delivered)} | Bounced: {toInt(job.bounced)}
                  </span>
                  <span>{formatTime(job.updated_at || job.created_at)}</span>
                </li>
              );
            })}
          </ul>
        </div>

        <div className="va-card">
          <h3>DLQ: Call Jobs ({toInt(dlqPayload.call_open, callDlq.length)})</h3>
          {callDlq.length === 0 ? <p className="va-muted">No open call DLQ entries.</p> : null}
          <ul className="va-list">
            {callDlq.map((row: DlqCallRow) => {
              const rowId = toInt(row.id);
              const handleReplay = (): void => {
                void runAction(
                  'dlq.call.replay',
                  { id: rowId },
                  {
                    confirmText: `Replay call DLQ #${rowId}?`,
                    successMessage: `Replay requested for call DLQ #${rowId}.`,
                  },
                );
              };
              return (
                <li key={`call-dlq-${rowId}`}>
                  <span>#{rowId} {toText(row.job_type, 'job')}</span>
                  <span>Replays: {toInt(row.replay_count)}</span>
                  <button
                    type="button"
                    disabled={busyAction.length > 0 || rowId <= 0}
                    onClick={handleReplay}
                  >
                    Replay
                  </button>
                </li>
              );
            })}
          </ul>
        </div>

        <div className="va-card">
          <h3>DLQ: Email ({toInt(dlqPayload.email_open, emailDlq.length)})</h3>
          {emailDlq.length === 0 ? <p className="va-muted">No open email DLQ entries.</p> : null}
          <ul className="va-list">
            {emailDlq.map((row: DlqEmailRow) => {
              const rowId = toInt(row.id);
              const handleReplay = (): void => {
                void runAction(
                  'dlq.email.replay',
                  { id: rowId },
                  {
                    confirmText: `Replay email DLQ #${rowId}?`,
                    successMessage: `Replay requested for email DLQ #${rowId}.`,
                  },
                );
              };
              return (
                <li key={`email-dlq-${rowId}`}>
                  <span>#{rowId} msg:{toText(row.message_id)}</span>
                  <span>Reason: {toText(row.reason)}</span>
                  <button
                    type="button"
                    disabled={busyAction.length > 0 || rowId <= 0}
                    onClick={handleReplay}
                  >
                    Replay
                  </button>
                </li>
              );
            })}
          </ul>
        </div>
      </section>

      {!hasMeaningfulData ? (
        <section className="va-grid">
          <div className="va-card">
            <h3>Connected, Waiting for Operational Activity</h3>
            <p className="va-muted">
              Session is healthy. Data cards will populate as provider, SMS, email, and DLQ events arrive.
            </p>
          </div>
        </section>
      ) : null}
    </>
  );
}
