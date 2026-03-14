import { initData, useRawInitData, useSignal } from '@tma.js/sdk-react';
import { useCallback, useEffect, useMemo, useState } from 'react';

import '@/components/AdminDashboard.css';

const POLL_INTERVAL_MS = 10000;
const SESSION_STORAGE_KEY = 'voxly-miniapp-session';
const API_BASE_URL = String(
  import.meta.env.VITE_API_BASE_URL || import.meta.env.VITE_API_BASE || '',
).trim().replace(/\/+$/, '');

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

function formatTime(value: unknown): string {
  if (!value) return '—';
  const parsed = new Date(String(value));
  if (Number.isNaN(parsed.getTime())) return String(value);
  return parsed.toLocaleString();
}

function readSessionCache(): SessionCacheEntry | null {
  try {
    const raw = sessionStorage.getItem(SESSION_STORAGE_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as SessionCacheEntry;
    if (!parsed || !parsed.token) return null;
    if (parsed.exp && parsed.exp <= Math.floor(Date.now() / 1000) + 15) {
      sessionStorage.removeItem(SESSION_STORAGE_KEY);
      return null;
    }
    return parsed;
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
  const initDataState = useSignal(initData.state);
  const [token, setToken] = useState<string | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string>('');
  const [notice, setNotice] = useState<string>('');
  const [bootstrap, setBootstrap] = useState<Record<string, any> | null>(null);
  const [pollPayload, setPollPayload] = useState<Record<string, any> | null>(null);
  const [busyAction, setBusyAction] = useState<string>('');

  const userLabel = useMemo(() => {
    const user = (initDataState?.user || {}) as Record<string, unknown>;
    const username = user.username;
    const firstName = user.firstName || user.first_name;
    const id = user.id;
    if (typeof username === 'string' && username.length > 0) return `@${username}`;
    if (typeof firstName === 'string' && firstName.length > 0) return String(firstName);
    if (id) return `id:${id}`;
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
        'x-telegram-init-data': initDataRaw,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ init_data_raw: initDataRaw }),
    });

    const payload = (await response.json().catch(() => null)) as SessionResponse | null;
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

  const request = useCallback(async (path: string, options: RequestInit = {}, allowRetry = true) => {
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

    const payload = await response.json().catch(() => null);
    if (response.status === 401 && allowRetry) {
      writeSessionCache(null);
      setToken(null);
      const refreshed = await createSession();
      return request(path, {
        ...options,
        headers: {
          ...(options.headers || {}),
          Authorization: `Bearer ${refreshed}`,
        },
      }, false);
    }
    if (!response.ok) {
      const errPayload = (payload || {}) as ApiErrorPayload;
      throw new Error(errPayload.error || errPayload.message || `Request failed (${response.status})`);
    }
    return payload;
  }, [createSession, token]);

  const loadBootstrap = useCallback(async () => {
    setLoading(true);
    setError('');
    try {
      const payload = await request('/miniapp/bootstrap');
      setBootstrap(payload || null);
      setPollPayload(payload || null);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  }, [request]);

  const loadPoll = useCallback(async () => {
    try {
      const payload = await request('/miniapp/jobs/poll');
      setPollPayload(payload || null);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  }, [request]);

  const runAction = useCallback(async (action: string, payload: Record<string, unknown>) => {
    setBusyAction(action);
    setNotice('');
    setError('');
    try {
      await request('/miniapp/action', {
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
    loadBootstrap().catch(() => {});
  }, [loadBootstrap]);

  useEffect(() => {
    if (!token) return undefined;
    const timer = setInterval(() => {
      loadPoll().catch(() => {});
    }, POLL_INTERVAL_MS);
    return () => clearInterval(timer);
  }, [loadPoll, token]);

  const providerPayload = (pollPayload?.provider || bootstrap?.dashboard?.provider || {}) as Record<string, any>;
  const providersByChannel = providerPayload.providers || {};
  const smsPayload = (pollPayload?.sms_bulk || bootstrap?.dashboard?.sms_bulk || {}) as Record<string, any>;
  const smsSummary = smsPayload.summary || {};
  const emailStatsPayload = (pollPayload?.email_bulk_stats || bootstrap?.dashboard?.email_bulk_stats || {}) as Record<string, any>;
  const emailStats = emailStatsPayload.stats || {};
  const emailHistoryPayload = (pollPayload?.email_bulk_history || bootstrap?.dashboard?.email_bulk_history || {}) as Record<string, any>;
  const emailJobs = Array.isArray(emailHistoryPayload.jobs) ? emailHistoryPayload.jobs : [];
  const dlqPayload = (pollPayload?.dlq || bootstrap?.dashboard?.dlq || {}) as Record<string, any>;
  const callDlq = Array.isArray(dlqPayload.call_preview) ? dlqPayload.call_preview : [];
  const emailDlq = Array.isArray(dlqPayload.email_preview) ? dlqPayload.email_preview : [];

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

  const renderProviderSection = (channel: 'call' | 'sms' | 'email') => {
    const channelData = providersByChannel[channel] || {};
    const currentProvider = String(channelData.provider || '').toLowerCase();
    const supported = Array.isArray(channelData.supported_providers)
      ? channelData.supported_providers
      : [];
    const readiness = channelData.readiness || {};

    return (
      <div className="va-card" key={channel}>
        <h3>{channel.toUpperCase()} Provider</h3>
        <p className="va-muted">Current: <strong>{currentProvider || 'unknown'}</strong></p>
        <div className="va-chip-grid">
          {supported.map((provider: string) => {
            const normalized = String(provider).toLowerCase();
            const ready = readiness[normalized] !== false;
            const active = normalized === currentProvider;
            return (
              <button
                key={`${channel}-${normalized}`}
                type="button"
                className={`va-chip ${active ? 'is-active' : ''}`}
                disabled={busyAction.length > 0 || !ready || active}
                onClick={() => runAction('provider.set', { channel, provider: normalized })}
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
          <button type="button" onClick={() => loadBootstrap()} disabled={loading || busyAction.length > 0}>
            Refresh
          </button>
        </div>
      </header>

      {loading ? <p className="va-muted">Loading dashboard...</p> : null}
      {error ? <p className="va-error">{error}</p> : null}
      {notice ? <p className="va-notice">{notice}</p> : null}

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
            {emailJobs.slice(0, 8).map((job: any) => (
              <li key={String(job.job_id || Math.random())}>
                <strong>{String(job.job_id || 'unknown')}</strong>
                <span>{String(job.status || 'unknown')}</span>
                <span>{toInt(job.sent)}/{toInt(job.total)} sent</span>
                <span>{formatTime(job.updated_at || job.created_at)}</span>
              </li>
            ))}
          </ul>
        </div>

        <div className="va-card">
          <h3>DLQ: Call Jobs ({toInt(dlqPayload.call_open, callDlq.length)})</h3>
          {callDlq.length === 0 ? <p className="va-muted">No open call DLQ entries.</p> : null}
          <ul className="va-list">
            {callDlq.map((row: any) => {
              const rowId = toInt(row.id);
              return (
                <li key={`call-dlq-${rowId}`}>
                  <span>#{rowId} {String(row.job_type || 'job')}</span>
                  <span>Replays: {toInt(row.replay_count)}</span>
                  <button
                    type="button"
                    disabled={busyAction.length > 0 || rowId <= 0}
                    onClick={() => runAction('dlq.call.replay', { id: rowId })}
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
            {emailDlq.map((row: any) => {
              const rowId = toInt(row.id);
              return (
                <li key={`email-dlq-${rowId}`}>
                  <span>#{rowId} msg:{String(row.message_id || 'unknown')}</span>
                  <span>Reason: {String(row.reason || 'unknown')}</span>
                  <button
                    type="button"
                    disabled={busyAction.length > 0 || rowId <= 0}
                    onClick={() => runAction('dlq.email.replay', { id: rowId })}
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
