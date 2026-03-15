import type { DashboardVm, EmailJob } from './types';
import { selectMailerPageVm } from './vmSelectors';

type MailerPageProps = {
  visible: boolean;
  vm: DashboardVm;
};

export function MailerPage({ visible, vm }: MailerPageProps) {
  if (!visible) return null;

  const {
    mailerRecipientsInput,
    setMailerRecipientsInput,
    handleRecipientsFile,
    mailerTemplateIdInput,
    setMailerTemplateIdInput,
    mailerSubjectInput,
    setMailerSubjectInput,
    mailerHtmlInput,
    setMailerHtmlInput,
    mailerTextInput,
    setMailerTextInput,
    mailerVariablesInput,
    setMailerVariablesInput,
    mailerScheduleAt,
    setMailerScheduleAt,
    busyAction,
    sendMailerFromConsole,
    mailerRecipientsParsed,
    mailerInvalidRecipients,
    mailerDuplicateCount,
    mailerVariableKeys,
    mailerTemplatePreviewSubject,
    mailerTemplatePreviewBody,
    mailerTemplatePreviewError,
    mailerDomainHealthStatus,
    mailerDomainHealthDetail,
    mailerTrendBars,
    emailTotalRecipients,
    emailSent,
    emailFailed,
    emailDelivered,
    emailBounced,
    emailComplained,
    emailSuppressed,
    emailProcessedPercent,
    emailDeliveredPercent,
    emailBouncePercent,
    emailComplaintPercent,
    textBar,
    emailJobs,
    toText,
    toInt,
  } = selectMailerPageVm(vm);

  return (
    <section className="va-grid">
      <div className="va-card">
        <h3>Mailer Console</h3>
        <textarea
          className="va-input va-textarea"
          placeholder="Recipient emails separated by comma/newline"
          value={mailerRecipientsInput}
          onChange={(event) => setMailerRecipientsInput(event.target.value)}
          rows={5}
        />
        <div className="va-inline-tools">
          <input
            type="file"
            accept=".csv,.txt"
            onChange={(event) => {
              const file = event.target.files?.[0] || null;
              void handleRecipientsFile(file, 'mailer');
              event.currentTarget.value = '';
            }}
          />
          <input
            className="va-input"
            placeholder="Template ID (optional)"
            value={mailerTemplateIdInput}
            onChange={(event) => setMailerTemplateIdInput(event.target.value)}
          />
        </div>
        <input
          className="va-input"
          placeholder="Subject (supports {{variables}})"
          value={mailerSubjectInput}
          onChange={(event) => setMailerSubjectInput(event.target.value)}
        />
        <textarea
          className="va-input va-textarea"
          placeholder="HTML body (optional)"
          value={mailerHtmlInput}
          onChange={(event) => setMailerHtmlInput(event.target.value)}
          rows={4}
        />
        <textarea
          className="va-input va-textarea"
          placeholder="Text body (optional)"
          value={mailerTextInput}
          onChange={(event) => setMailerTextInput(event.target.value)}
          rows={3}
        />
        <textarea
          className="va-input va-textarea"
          placeholder={'Variables JSON, e.g. {"first_name":"Ada"}'}
          value={mailerVariablesInput}
          onChange={(event) => setMailerVariablesInput(event.target.value)}
          rows={3}
        />
        <div className="va-inline-tools">
          <label className="va-muted">
            Send at:
            <input
              className="va-input"
              type="datetime-local"
              value={mailerScheduleAt}
              onChange={(event) => setMailerScheduleAt(event.target.value)}
            />
          </label>
          <button
            type="button"
            disabled={busyAction.length > 0}
            onClick={() => { void sendMailerFromConsole(); }}
          >
            Queue Mailer Job
          </button>
        </div>
        <p className="va-muted">
          Audience: <strong>{mailerRecipientsParsed.length}</strong>
          {' '}| Invalid: <strong>{mailerInvalidRecipients.length}</strong>
          {' '}| Duplicates removed: <strong>{mailerDuplicateCount}</strong>
        </p>
        <p className="va-muted">
          Template variables detected: {mailerVariableKeys.length ? mailerVariableKeys.join(', ') : 'none'}
        </p>
        <div className="va-card va-subcard">
          <h4>Template Render Preview</h4>
          <p className="va-muted">Subject preview: <strong>{mailerTemplatePreviewSubject}</strong></p>
          <p className="va-muted">Body preview: {mailerTemplatePreviewBody}</p>
          {mailerTemplatePreviewError ? <p className="va-error">{mailerTemplatePreviewError}</p> : null}
        </div>
      </div>
      <div className="va-card">
        <h3>Deliverability Monitor</h3>
        <p>Total recipients (24h): <strong>{emailTotalRecipients}</strong></p>
        <p>Sent: <strong>{emailSent}</strong> | Failed: <strong>{emailFailed}</strong></p>
        <p>Delivered: <strong>{emailDelivered}</strong></p>
        <p>
          Bounced: <strong>{emailBounced}</strong> | Complaints: <strong>{emailComplained}</strong>
          {' '}| Suppressed: <strong>{emailSuppressed}</strong>
        </p>
        <pre>{textBar(emailProcessedPercent)}</pre>
        <pre>{textBar(emailDeliveredPercent)}</pre>
        <pre>{textBar(emailBouncePercent)}</pre>
        <pre>{textBar(emailComplaintPercent)}</pre>
        <div className="va-card va-subcard">
          <h4>Domain Health</h4>
          <p className="va-muted">Status: <strong>{mailerDomainHealthStatus}</strong></p>
          <p className="va-muted">{mailerDomainHealthDetail}</p>
        </div>
        <div className="va-card va-subcard">
          <h4>Bounce/Complaint Trend</h4>
          {mailerTrendBars.length === 0 ? (
            <p className="va-muted">No recent jobs for trend rendering.</p>
          ) : (
            <ul className="va-list">
              {mailerTrendBars.map((bar, index) => (
                <li key={`mailer-trend-${index}`}><pre>{bar}</pre></li>
              ))}
            </ul>
          )}
        </div>
        {emailJobs.length > 0 ? (
          <ul className="va-list">
            {emailJobs.slice(0, 6).map((job: EmailJob, index: number) => (
              <li key={`mailer-job-${index}`}>
                <strong>{toText(job.job_id, `job-${index + 1}`)}</strong>
                <span>{toText(job.status, 'unknown')}</span>
                <span>{toInt(job.sent)}/{toInt(job.total)} sent</span>
                <span>
                  Fail: {toInt(job.failed)} | Deliv: {toInt(job.delivered)} | Bounce: {toInt(job.bounced)}
                </span>
              </li>
            ))}
          </ul>
        ) : <p className="va-muted">No recent mailer jobs.</p>}
      </div>
    </section>
  );
}
