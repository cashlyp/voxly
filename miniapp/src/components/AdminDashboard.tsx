import { initData, useRawInitData, useSignal } from '@tma.js/sdk-react';
import { useCallback, useEffect, useMemo, useState } from 'react';

import '@/components/AdminDashboard.css';

const POLL_BASE_INTERVAL_MS = 10000;
const POLL_MAX_INTERVAL_MS = 60000;
const POLL_BACKOFF_MULTIPLIER = 1.7;
const POLL_JITTER_MS = 1200;
const POLL_DEGRADED_FAILURES = 2;
const SESSION_STORAGE_KEY = 'voxly-miniapp-session';
const API_BASE_URL = String(
  import.meta.env.VITE_API_BASE_URL || import.meta.env.VITE_API_BASE || '',
).trim().replace(/\/+$/, '');
const SESSION_REFRESH_RETRY_COUNT = 1;

type ProviderChannel = 'call' | 'sms' | 'email';

interface SessionStateUser {
  username?: unknown;
  firstName?: unknown;
  first_name?: unknown;
  id?: unknown;
}

interface SessionState {
  user?: SessionStateUser;
}

interface SessionCacheEntry {
  token: string;
  exp: number | null;
}

interface SessionResponse {
  success: boolean;
  token?: string;
  expires_at?: number;
  error?: string;
}

interface ApiErrorPayload {
  error?: string;
  message?: string;
}

interface ProviderChannelData {
  provider?: unknown;
  supported_providers?: unknown;
  readiness?: unknown;
}

interface ProviderPayload {
  providers?: Partial<Record<ProviderChannel, ProviderChannelData>>;
}

interface SmsSummary {
  totalRecipients?: unknown;
  totalSuccessful?: unknown;
  totalFailed?: unknown;
}

interface SmsPayload {
  summary?: SmsSummary;
}

interface EmailStats {
  total_recipients?: unknown;
  sent?: unknown;
  failed?: unknown;
  delivered?: unknown;
}

interface EmailStatsPayload {
  stats?: EmailStats;
}

interface EmailJob {
  job_id?: unknown;
  status?: unknown;
  sent?: unknown;
  total?: unknown;
  updated_at?: unknown;
  created_at?: unknown;
}

interface EmailHistoryPayload {
  jobs?: unknown;
}

interface DlqCallRow {
  id?: unknown;
  job_type?: unknown;
  replay_count?: unknown;
}

interface DlqEmailRow {
  id?: unknown;
  message_id?: unknown;
  reason?: unknown;
}

interface DlqPayload {
  call_open?: unknown;
  email_open?: unknown;
  call_preview?: unknown;
  email_preview?: unknown;
}

interface DashboardPayload {
  provider?: ProviderPayload;
  sms_bulk?: SmsPayload;
  email_bulk_stats?: EmailStatsPayload;
  email_bulk_history?: EmailHistoryPayload;
  dlq?: DlqPayload;
  bridge?: unknown;
}

interface DashboardApiPayload extends DashboardPayload {
  success?: boolean;
  dashboard?: DashboardPayload;
  bridge?: unknown;
  poll_interval_seconds?: unknown;
  poll_at?: unknown;
  server_time?: unknown;
}

type JsonObject = Record<string, unknown>;

function textBar(percent: number, width = 10): string {
  const safePercent = Number.isFinite(percent)
    ? Math.max(0, Math.min(100, Math.round(percent)))
    : 0;
  const filled = Math.round((safePercent / 100) * width);
  return `${'█'.repeat(filled)}${'░'.repeat(Math.max(0, width - filled))} ${safePercent}%`;
}

function toInt(value: unknown, fallback = 0): number {
  const num = Number(value);
  return Number.isFinite(num) ? Math.floor(num) : fallback;
}

function toText(value: unknown, fallback = 'unknown'): string {
  if (typeof value === 'string' && value.trim()) return value;
  if (typeof value === 'number' || typeof value === 'boolean' || typeof value === 'bigint') {
    return String(value);
  }
  return fallback;
}

function formatTime(value: unknown): string {
  if (!value) return '—';
  const valueText = toText(value, '');
  if (!valueText) return '—';
  const parsed = new Date(valueText);
  if (Number.isNaN(parsed.getTime())) return toText(value, '—');
  return parsed.toLocaleString();
}

function asRecord(value: unknown): JsonObject {
  return value && typeof value === 'object' && !Array.isArray(value)
    ? (value as JsonObject)
    : {};
}

function asStringList(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return value
    .map((entry) => toText(entry, ''))
    .map((entry) => entry.trim().toLowerCase())
    .filter(Boolean);
}

function asDlqCallRows(value: unknown): DlqCallRow[] {
  if (!Array.isArray(value)) return [];
  return value.map((entry) => asRecord(entry) as DlqCallRow);
}

function asDlqEmailRows(value: unknown): DlqEmailRow[] {
  if (!Array.isArray(value)) return [];
  return value.map((entry) => asRecord(entry) as DlqEmailRow);
}

function asEmailJobs(value: unknown): EmailJob[] {
  if (!Array.isArray(value)) return [];
  return value.map((entry) => asRecord(entry) as EmailJob);
}

function getPollingDelayMs(baseIntervalMs: number, consecutiveFailures: number): number {
  const safeBase = Math.max(3000, Math.min(POLL_MAX_INTERVAL_MS, Math.floor(baseIntervalMs)));
  const backoff = consecutiveFailures > 0
    ? Math.min(
      POLL_MAX_INTERVAL_MS,
      Math.round(safeBase * Math.pow(POLL_BACKOFF_MULTIPLIER, consecutiveFailures)),
    )
    : safeBase;
  const jitter = Math.floor(Math.random() * (POLL_JITTER_MS + 1));
  return backoff + jitter;
}

function normalizeBridgeStatuses(value: unknown): number[] {
  const bridge = asRecord(value);
  return Object.values(bridge)
    .map((status) => Number(status))
    .filter((status) => Number.isFinite(status) && status >= 100 && status <= 599);
}

async function parseJsonResponse(response: Response): Promise<unknown> {
  return response.json().catch(() => null);
}

function parseApiError(payload: unknown, status: number): string {
  const body = asRecord(payload) as ApiErrorPayload;
  return body.error || body.message || `Request failed (${status})`;
}

function readSessionCache(): SessionCacheEntry | null {
  try {
    const raw = sessionStorage.getItem(SESSION_STORAGE_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as Partial<SessionCacheEntry>;
    if (!parsed || !parsed.token) return null;
    const exp = Number(parsed.exp);
    if (Number.isFinite(exp) && exp <= Math.floor(Date.now() / 1000) + 15) {
      sessionStorage.removeItem(SESSION_STORAGE_KEY);
      return null;
    }
    return {
      token: String(parsed.token),
      exp: Number.isFinite(exp) ? exp : null,
    };
  } catch {
    return null;
  }
}

function writeSessionCache(entry: SessionCacheEntry | null): void {
  if (!entry) {
    sessionStorage.removeItem(SESSION_STORAGE_KEY);
    return;
  }
  sessionStorage.setItem(SESSION_STORAGE_KEY, JSON.stringify(entry));
}

function buildApiUrl(path: string): string {
  const normalizedPath = path.startsWith('/') ? path : `/${path}`;
  return API_BASE_URL ? `${API_BASE_URL}${normalizedPath}` : normalizedPath;
}

export function AdminDashboard() {
  const initDataRawFromHook = useRawInitData();
  const initDataRawFromSignal = useSignal(initData.raw);
  const initDataRaw = initDataRawFromHook || initDataRawFromSignal;
  const initDataState = useSignal(initData.state) as SessionState | undefined;
  const [token, setToken] = useState<string | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string>('');
  const [notice, setNotice] = useState<string>('');
  const [bootstrap, setBootstrap] = useState<DashboardApiPayload | null>(null);
  const [pollPayload, setPollPayload] = useState<DashboardApiPayload | null>(null);
  const [busyAction, setBusyAction] = useState<string>('');
  const [pollFailureCount, setPollFailureCount] = useState<number>(0);
  const [lastPollAt, setLastPollAt] = useState<number | null>(null);
  const [lastSuccessfulPollAt, setLastSuccessfulPollAt] = useState<number | null>(null);
  const [nextPollAt, setNextPollAt] = useState<number | null>(null);

  const userLabel = useMemo(() => {
    const user = asRecord(initDataState?.user);
    const username = user.username;
    const firstName = user.firstName || user.first_name;
    const id = user.id;
    if (typeof username === 'string' && username.length > 0) return `@${username}`;
    if (typeof firstName === 'string' && firstName.length > 0) return String(firstName);
    if (typeof id === 'string' || typeof id === 'number') return `id:${id}`;
    return 'Unknown admin';
  }, [initDataState]);

  const createSession = useCallback(async (): Promise<string> => {
    const cached = readSessionCache();
    if (cached?.token) {
      setToken(cached.token);
      return cached.token;
    }

    if (!initDataRaw) {
      throw new Error('Mini App init data is unavailable. Open this page from Telegram.');
    }

    const response = await fetch(buildApiUrl('/miniapp/session'), {
      method: 'POST',
      headers: {
        Authorization: `tma ${initDataRaw}`,
        'x-telegram-init-data': initDataRaw,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ init_data_raw: initDataRaw }),
    });

    const payload = (await parseJsonResponse(response)) as SessionResponse | null;
    if (!response.ok || !payload?.success || !payload?.token) {
      throw new Error(payload?.error || `Session request failed (${response.status})`);
    }

    const nextToken = payload.token;
    const cacheEntry: SessionCacheEntry = {
      token: nextToken,
      exp: Number.isFinite(Number(payload.expires_at)) ? Number(payload.expires_at) : null,
    };
    writeSessionCache(cacheEntry);
    setToken(nextToken);
    return nextToken;
  }, [initDataRaw]);

  const request = useCallback(async <T,>(path: string, options: RequestInit = {}, retryCount = 0): Promise<T> => {
    const activeToken = token || await createSession();
    const headers = new Headers(options.headers || {});
    headers.set('Authorization', `Bearer ${activeToken}`);
    if (!headers.has('Content-Type') && options.body !== undefined) {
      headers.set('Content-Type', 'application/json');
    }

    const response = await fetch(buildApiUrl(path), {
      ...options,
      headers,
    });

    const payload = await parseJsonResponse(response);
    if (response.status === 401 && retryCount < SESSION_REFRESH_RETRY_COUNT) {
      writeSessionCache(null);
      setToken(null);
      return request<T>(path, options, retryCount + 1);
    }
    if (!response.ok) {
      throw new Error(parseApiError(payload, response.status));
    }
    return payload as T;
  }, [createSession, token]);

  const loadBootstrap = useCallback(async () => {
    setLoading(true);
    setError('');
    try {
      const payload = await request<DashboardApiPayload>('/miniapp/bootstrap');
      const now = Date.now();
      setBootstrap(payload);
      setPollPayload(payload);
      setLastPollAt(now);
      setLastSuccessfulPollAt(now);
      setPollFailureCount(0);
    } catch (err) {
      setPollFailureCount((prev) => prev + 1);
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  }, [request]);

  const loadPoll = useCallback(async (): Promise<boolean> => {
    const startedAt = Date.now();
    setLastPollAt(startedAt);
    try {
      const payload = await request<DashboardApiPayload>('/miniapp/jobs/poll');
      setError('');
      setPollPayload(payload);
      setPollFailureCount(0);
      setLastSuccessfulPollAt(Date.now());
      return true;
    } catch (err) {
      setPollFailureCount((prev) => prev + 1);
      setError(err instanceof Error ? err.message : String(err));
      return false;
    }
  }, [request]);

  const runAction = useCallback(async (action: string, payload: Record<string, unknown>) => {
    setBusyAction(action);
    setNotice('');
    setError('');
    try {
      await request<unknown>('/miniapp/action', {
        method: 'POST',
        body: JSON.stringify({ action, payload }),
      });
      setNotice(`Action completed: ${action}`);
      await loadBootstrap();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusyAction('');
    }
  }, [loadBootstrap, request]);

  useEffect(() => {
    void loadBootstrap();
  }, [loadBootstrap]);

  const serverPollIntervalMs = useMemo(() => {
    const intervalSeconds = Number(bootstrap?.poll_interval_seconds);
    if (!Number.isFinite(intervalSeconds) || intervalSeconds <= 0) {
      return POLL_BASE_INTERVAL_MS;
    }
    return Math.max(3000, Math.min(POLL_MAX_INTERVAL_MS, Math.floor(intervalSeconds * 1000)));
  }, [bootstrap?.poll_interval_seconds]);

  useEffect(() => {
    if (!token) return undefined;
    let disposed = false;
    let timer: ReturnType<typeof setTimeout> | null = null;
    let consecutiveFailures = 0;

    const scheduleNext = (delayMs: number) => {
      if (disposed) return;
      setNextPollAt(Date.now() + delayMs);
      timer = setTimeout(async () => {
        const ok = await loadPoll();
        if (disposed) return;
        consecutiveFailures = ok ? 0 : consecutiveFailures + 1;
        scheduleNext(getPollingDelayMs(serverPollIntervalMs, consecutiveFailures));
      }, delayMs);
    };

    scheduleNext(serverPollIntervalMs);
    return () => {
      disposed = true;
      setNextPollAt(null);
      if (timer) clearTimeout(timer);
    };
  }, [loadPoll, serverPollIntervalMs, token]);

  const dashboard = bootstrap?.dashboard;
  const providerPayload = pollPayload?.provider || dashboard?.provider || {};
  const providersByChannel = providerPayload.providers || {};
  const smsPayload = pollPayload?.sms_bulk || dashboard?.sms_bulk || {};
  const smsSummary = smsPayload.summary || {};
  const emailStatsPayload = pollPayload?.email_bulk_stats || dashboard?.email_bulk_stats || {};
  const emailStats = emailStatsPayload.stats || {};
  const emailHistoryPayload = pollPayload?.email_bulk_history || dashboard?.email_bulk_history || {};
  const emailJobs = asEmailJobs(emailHistoryPayload.jobs);
  const dlqPayload = pollPayload?.dlq || dashboard?.dlq || {};
  const callDlq = asDlqCallRows(dlqPayload.call_preview);
  const emailDlq = asDlqEmailRows(dlqPayload.email_preview);

  const smsTotalRecipients = toInt(smsSummary.totalRecipients);
  const smsSuccess = toInt(smsSummary.totalSuccessful);
  const smsFailed = toInt(smsSummary.totalFailed);
  const smsProcessedPercent = smsTotalRecipients > 0
    ? Math.round(((smsSuccess + smsFailed) / smsTotalRecipients) * 100)
    : 0;

  const emailTotalRecipients = toInt(emailStats.total_recipients);
  const emailSent = toInt(emailStats.sent);
  const emailFailed = toInt(emailStats.failed);
  const emailDelivered = toInt(emailStats.delivered);
  const emailProcessedPercent = emailTotalRecipients > 0
    ? Math.round(((emailSent + emailFailed) / emailTotalRecipients) * 100)
    : 0;
  const emailDeliveredPercent = emailTotalRecipients > 0
    ? Math.round((emailDelivered / emailTotalRecipients) * 100)
    : 0;
  const bridgeStatuses = normalizeBridgeStatuses(pollPayload?.bridge || dashboard?.bridge);
  const bridgeHardFailures = bridgeStatuses.filter((status) => status >= 500).length;
  const bridgeSoftFailures = bridgeStatuses.filter((status) => status >= 400).length;
  const isDashboardDegraded = pollFailureCount >= POLL_DEGRADED_FAILURES || bridgeHardFailures > 0;
  const nextPollLabel = nextPollAt ? formatTime(new Date(nextPollAt).toISOString()) : '—';
  const lastPollLabel = lastPollAt ? formatTime(new Date(lastPollAt).toISOString()) : '—';
  const lastSuccessfulPollLabel = lastSuccessfulPollAt
    ? formatTime(new Date(lastSuccessfulPollAt).toISOString())
    : '—';

  const handleRefresh = (): void => {
    void loadBootstrap();
  };

  const renderProviderSection = (channel: ProviderChannel) => {
    const channelData = providersByChannel[channel] || {};
    const currentProvider = toText(channelData.provider, '').toLowerCase();
    const supported = asStringList(channelData.supported_providers);
    const readiness = asRecord(channelData.readiness);

    return (
      <div className="va-card" key={channel}>
        <h3>{channel.toUpperCase()} Provider</h3>
        <p className="va-muted">Current: <strong>{currentProvider || 'unknown'}</strong></p>
        <div className="va-chip-grid">
          {supported.map((provider) => {
            const normalized = provider.toLowerCase();
            const ready = readiness[normalized] !== false;
            const active = normalized === currentProvider;
            const handleProviderSwitch = (): void => {
              void runAction('provider.set', { channel, provider: normalized });
            };
            return (
              <button
                key={`${channel}-${normalized}`}
                type="button"
                className={`va-chip ${active ? 'is-active' : ''}`}
                disabled={busyAction.length > 0 || !ready || active}
                onClick={handleProviderSwitch}
              >
                {normalized}
                {!ready ? ' (not ready)' : ''}
              </button>
            );
          })}
        </div>
      </div>
    );
  };

  return (
    <main className="va-dashboard">
      <header className="va-header">
        <div>
          <h1>Voxly Admin Console</h1>
          <p className="va-muted">Telegram Mini App dashboard for provider, SMS, email, and DLQ operations.</p>
        </div>
        <div className="va-header-meta">
          <span>Admin: {userLabel}</span>
          <button type="button" onClick={handleRefresh} disabled={loading || busyAction.length > 0}>
            Refresh
          </button>
        </div>
      </header>

      {loading ? <p className="va-muted">Loading dashboard...</p> : null}
      {error ? <p className="va-error">{error}</p> : null}
      {notice ? <p className="va-notice">{notice}</p> : null}

      <section className="va-grid">
        <div className={`va-card va-health ${isDashboardDegraded ? 'is-degraded' : 'is-healthy'}`}>
          <h3>Live Sync Health</h3>
          <p>
            Mode: <strong>{isDashboardDegraded ? 'Degraded' : 'Healthy'}</strong>
          </p>
          <p>
            Poll failures: <strong>{pollFailureCount}</strong> | Bridge 5xx: <strong>{bridgeHardFailures}</strong>
            {' '}| Bridge 4xx/5xx: <strong>{bridgeSoftFailures}</strong>
          </p>
          <p>Last poll attempt: <strong>{lastPollLabel}</strong></p>
          <p>Last successful poll: <strong>{lastSuccessfulPollLabel}</strong></p>
          <p>Next poll scheduled: <strong>{nextPollLabel}</strong></p>
        </div>
      </section>

      <section className="va-grid">
        {renderProviderSection('call')}
        {renderProviderSection('sms')}
        {renderProviderSection('email')}
      </section>

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
          <pre>{textBar(emailProcessedPercent)}</pre>
          <pre>{textBar(emailDeliveredPercent)}</pre>
        </div>
      </section>

      <section className="va-grid">
        <div className="va-card">
          <h3>Email Jobs</h3>
          {emailJobs.length === 0 ? <p className="va-muted">No recent jobs.</p> : null}
          <ul className="va-list">
            {emailJobs.slice(0, 8).map((job, index) => {
              const jobId = toText(job.job_id, `job-${index + 1}`);
              const jobStatus = toText(job.status);
              const jobKey = `email-job-${jobId}-${index}`;
              return (
                <li key={jobKey}>
                  <strong>{jobId}</strong>
                  <span>{jobStatus}</span>
                  <span>{toInt(job.sent)}/{toInt(job.total)} sent</span>
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
            {callDlq.map((row) => {
              const rowId = toInt(row.id);
              const handleReplay = (): void => {
                void runAction('dlq.call.replay', { id: rowId });
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
            {emailDlq.map((row) => {
              const rowId = toInt(row.id);
              const handleReplay = (): void => {
                void runAction('dlq.email.replay', { id: rowId });
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
    </main>
  );
}
