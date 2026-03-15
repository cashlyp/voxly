import type { DashboardVm } from './types';
import { selectSmsPageVm } from './vmSelectors';

type SmsSenderPageProps = {
  visible: boolean;
  vm: DashboardVm;
};

export function SmsSenderPage({ visible, vm }: SmsSenderPageProps) {
  if (!visible) return null;

  const {
    smsRecipientsInput,
    setSmsRecipientsInput,
    handleRecipientsFile,
    smsProviderInput,
    setSmsProviderInput,
    smsMessageInput,
    setSmsMessageInput,
    smsScheduleAt,
    setSmsScheduleAt,
    busyAction,
    sendSmsFromConsole,
    smsRecipientsParsed,
    smsInvalidRecipients,
    smsDuplicateCount,
    smsSegmentEstimate,
    smsCostPerSegment,
    setSmsCostPerSegment,
    smsEstimatedCost,
    smsDryRunMode,
    setSmsDryRunMode,
    smsValidationCategories,
    smsRouteSimulationRows,
    smsTotalRecipients,
    smsSuccess,
    smsFailed,
    smsProcessedPercent,
    textBar,
  } = selectSmsPageVm(vm);

  return (
    <section className="va-grid">
      <div className="va-card">
        <h3>SMS Sender Console</h3>
        <p className="va-muted">
          Upload recipients, estimate segments, schedule delivery, and track bulk job outcomes.
        </p>
        <textarea
          className="va-input va-textarea"
          placeholder="Recipients (+15551230001), separated by comma/newline"
          value={smsRecipientsInput}
          onChange={(event) => setSmsRecipientsInput(event.target.value)}
          rows={5}
        />
        <div className="va-inline-tools">
          <input
            type="file"
            accept=".csv,.txt"
            onChange={(event) => {
              const file = event.target.files?.[0] || null;
              void handleRecipientsFile(file, 'sms');
              event.currentTarget.value = '';
            }}
          />
          <input
            className="va-input"
            placeholder="Provider (optional)"
            value={smsProviderInput}
            onChange={(event) => setSmsProviderInput(event.target.value)}
          />
        </div>
        <textarea
          className="va-input va-textarea"
          placeholder="Message body"
          value={smsMessageInput}
          onChange={(event) => setSmsMessageInput(event.target.value)}
          rows={4}
        />
        <div className="va-inline-tools">
          <label className="va-muted">
            Schedule at:
            <input
              className="va-input"
              type="datetime-local"
              value={smsScheduleAt}
              onChange={(event) => setSmsScheduleAt(event.target.value)}
            />
          </label>
          <label className="va-muted">
            Cost/segment ($):
            <input
              className="va-input"
              inputMode="decimal"
              placeholder="0.0075"
              value={smsCostPerSegment}
              onChange={(event) => setSmsCostPerSegment(event.target.value)}
            />
          </label>
          <label className="va-muted">
            <input
              type="checkbox"
              checked={smsDryRunMode}
              onChange={(event) => setSmsDryRunMode(event.target.checked)}
            />
            {' '}Dry-run only
          </label>
          <button
            type="button"
            disabled={busyAction.length > 0}
            onClick={() => { void sendSmsFromConsole(); }}
          >
            {smsDryRunMode ? 'Run Dry-Run' : smsScheduleAt ? 'Schedule SMS Batch' : 'Send SMS Batch'}
          </button>
        </div>
        <div className="va-inline-tools">
          <div className="va-card va-subcard">
            <h4>Validation Preview</h4>
            <p className="va-muted">Valid: <strong>{smsValidationCategories.valid}</strong></p>
            <p className="va-muted">Invalid: <strong>{smsValidationCategories.invalid}</strong></p>
            <p className="va-muted">Duplicates: <strong>{smsValidationCategories.duplicate}</strong></p>
            <p className="va-muted">Likely landline: <strong>{smsValidationCategories.likelyLandline}</strong></p>
          </div>
          <div className="va-card va-subcard">
            <h4>Cost Estimate</h4>
            <p className="va-muted">Estimated send cost: <strong>${smsEstimatedCost.toFixed(4)}</strong></p>
            <p className="va-muted">
              Based on {smsSegmentEstimate.segments} segment(s) per recipient and your cost/segment.
            </p>
          </div>
        </div>
        <p className="va-muted">
          Recipients: <strong>{smsRecipientsParsed.length}</strong>
          {' '}| Invalid: <strong>{smsInvalidRecipients.length}</strong>
          {' '}| Duplicates removed: <strong>{smsDuplicateCount}</strong>
        </p>
        <p className="va-muted">
          Segment estimate: <strong>{smsSegmentEstimate.segments}</strong>
          {' '}segment(s), {smsSegmentEstimate.perSegment} chars/segment.
        </p>
        <h4>Route Simulation</h4>
        {smsRouteSimulationRows.length === 0 ? (
          <p className="va-muted">Provider route simulation is warming up.</p>
        ) : (
          <ul className="va-list">
            {smsRouteSimulationRows.map((row) => (
              <li key={`sms-route-${row.provider}`}>
                <strong>{row.provider}</strong>
                <span>Ready: {row.ready ? 'yes' : 'no'} | Degraded: {row.degraded ? 'yes' : 'no'}</span>
                <span>Parity gaps: {row.parityGapCount}</span>
              </li>
            ))}
          </ul>
        )}
      </div>
      <div className="va-card">
        <h3>SMS Job Tracker</h3>
        <p>Total recipients (24h): <strong>{smsTotalRecipients}</strong></p>
        <p>Successful: <strong>{smsSuccess}</strong> | Failed: <strong>{smsFailed}</strong></p>
        <pre>{textBar(smsProcessedPercent)}</pre>
        {smsInvalidRecipients.length > 0 ? (
          <p className="va-muted">
            Suppression preview (invalid format): {smsInvalidRecipients.slice(0, 10).join(', ')}
          </p>
        ) : null}
      </div>
    </section>
  );
}
