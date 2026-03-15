import { initData, settingsButton, useRawInitData, useSignal } from '@tma.js/sdk-react';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import '@/components/AdminDashboard.css';

const POLL_BASE_INTERVAL_MS = 10000;
const POLL_MAX_INTERVAL_MS = 60000;
const POLL_BACKOFF_MULTIPLIER = 1.7;
const POLL_JITTER_MS = 1200;
const POLL_DEGRADED_FAILURES = 2;
const SESSION_STORAGE_KEY = 'voxly-miniapp-session';
const MAX_ACTIVITY_ITEMS = 18;
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
  code?: string;
}

interface ApiErrorPayload {
  error?: string;
  message?: string;
  code?: string;
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
  bounced?: unknown;
  complained?: unknown;
  suppressed?: unknown;
}

interface EmailStatsPayload {
  stats?: EmailStats;
}

interface EmailJob {
  job_id?: unknown;
  status?: unknown;
  sent?: unknown;
  total?: unknown;
  failed?: unknown;
  delivered?: unknown;
  bounced?: unknown;
  complained?: unknown;
  suppressed?: unknown;
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

interface CallStatsPayload {
  total_calls?: unknown;
  completed_calls?: unknown;
  failed_calls?: unknown;
  success_rate?: unknown;
  recent_calls?: unknown;
  unique_users?: unknown;
}

interface CallLogRow {
  call_sid?: unknown;
  phone_number?: unknown;
  status?: unknown;
  status_normalized?: unknown;
  direction?: unknown;
  duration?: unknown;
  transcript_count?: unknown;
  voice_runtime?: unknown;
  ended_reason?: unknown;
  created_at?: unknown;
  updated_at?: unknown;
}

interface CallLogsPayload {
  rows?: unknown;
  total?: unknown;
  limit?: unknown;
  offset?: unknown;
}

interface VoiceRuntimePayload {
  runtime?: unknown;
  active_calls?: unknown;
  actions?: unknown;
  applied?: unknown;
}

interface CallScriptLifecycle {
  lifecycle_state?: unknown;
  submitted_for_review_at?: unknown;
  reviewed_at?: unknown;
  reviewed_by?: unknown;
  review_note?: unknown;
  live_at?: unknown;
  live_by?: unknown;
}

interface CallScriptRow {
  id?: unknown;
  name?: unknown;
  description?: unknown;
  prompt?: unknown;
  first_message?: unknown;
  default_profile?: unknown;
  objective_tags?: unknown;
  flow_type?: unknown;
  flow_types?: unknown;
  lifecycle_state?: unknown;
  lifecycle?: CallScriptLifecycle;
  version?: unknown;
}

interface CallScriptSimulationPayload {
  simulation?: unknown;
}

interface CallScriptsPayload {
  scripts?: unknown;
  total?: unknown;
  limit?: unknown;
  flow_types?: unknown;
}

interface OpsQueueBacklogPayload {
  total?: unknown;
  dlq_call_open?: unknown;
  dlq_email_open?: unknown;
  sms_failed?: unknown;
  email_failed?: unknown;
}

interface OpsPayload {
  queue_backlog?: OpsQueueBacklogPayload;
  status?: unknown;
  health?: unknown;
}

interface MiniAppUsersPayload {
  rows?: unknown;
  total?: unknown;
}

interface MiniAppAuditPayload {
  rows?: unknown;
  summary?: unknown;
  hours?: unknown;
}

interface MiniAppIncidentsPayload {
  alerts?: unknown;
  total_alerts?: unknown;
  runbooks?: unknown;
  summary?: unknown;
}

interface DashboardPayload {
  session?: unknown;
  provider?: ProviderPayload;
  provider_compatibility?: unknown;
  sms_bulk?: SmsPayload;
  sms_stats?: unknown;
  email_bulk_stats?: EmailStatsPayload;
  email_bulk_history?: EmailHistoryPayload;
  dlq?: DlqPayload;
  call_logs?: CallLogsPayload;
  call_scripts?: CallScriptsPayload;
  call_stats?: CallStatsPayload;
  voice_runtime?: VoiceRuntimePayload;
  users?: MiniAppUsersPayload;
  audit?: MiniAppAuditPayload;
  incidents?: MiniAppIncidentsPayload;
  ops?: OpsPayload;
  bridge?: unknown;
}

interface DashboardApiPayload extends DashboardPayload {
  success?: boolean;
  dashboard?: DashboardPayload;
  session?: unknown;
  bridge?: unknown;
  poll_interval_seconds?: unknown;
  poll_at?: unknown;
  server_time?: unknown;
}

type JsonObject = Record<string, unknown>;
type ActivityStatus = 'info' | 'success' | 'error';
type DashboardModule = 'ops' | 'sms' | 'mailer' | 'provider' | 'content' | 'users' | 'audit';

const MODULE_DEFINITIONS: Array<{ id: DashboardModule; label: string; capability: string }> = [
  { id: 'ops', label: 'Ops Dashboard', capability: 'dashboard_view' },
  { id: 'sms', label: 'SMS Sender', capability: 'sms_bulk_manage' },
  { id: 'mailer', label: 'Mailer Console', capability: 'email_bulk_manage' },
  { id: 'provider', label: 'Provider Control', capability: 'provider_manage' },
  { id: 'content', label: 'Script Studio', capability: 'caller_flags_manage' },
  { id: 'users', label: 'User & Role Admin', capability: 'users_manage' },
  { id: 'audit', label: 'Audit & Incidents', capability: 'dashboard_view' },
];

interface ActivityEntry {
  id: string;
  title: string;
  detail: string;
  status: ActivityStatus;
  at: string;
}

interface MiniAppUserRow {
  telegram_id?: unknown;
  role?: unknown;
  role_source?: unknown;
  total_calls?: unknown;
  successful_calls?: unknown;
  failed_calls?: unknown;
  last_activity?: unknown;
}

interface AuditFeedRow {
  id?: unknown;
  service_name?: unknown;
  status?: unknown;
  details?: unknown;
  timestamp?: unknown;
}

interface IncidentRow {
  id?: unknown;
  service_name?: unknown;
  status?: unknown;
  details?: unknown;
  timestamp?: unknown;
}

interface RunbookRow {
  action?: unknown;
  label?: unknown;
  capability?: unknown;
}

interface MiniAppSessionSummary {
  telegram_id?: unknown;
  role?: unknown;
  role_source?: unknown;
  caps?: unknown;
  exp?: unknown;
}

function textBar(percent: number, width = 10): string {
  const safePercent = Number.isFinite(percent)
    ? Math.max(0, Math.min(100, Math.round(percent)))
    : 0;
  const filled = Math.round((safePercent / 100) * width);
  return `${'█'.repeat(filled)}${'░'.repeat(Math.max(0, width - filled))} ${safePercent}%`;
}

function normalizePhone(value: string): string {
  return value.replace(/[^\d+]/g, '');
}

function parsePhoneList(raw: string): string[] {
  return Array.from(
    new Set(
      String(raw || '')
        .split(/[\n,;\t ]+/g)
        .map((entry) => normalizePhone(entry.trim()))
        .filter(Boolean),
    ),
  );
}

function parseEmailList(raw: string): string[] {
  return Array.from(
    new Set(
      String(raw || '')
        .split(/[\n,;\t ]+/g)
        .map((entry) => entry.trim().toLowerCase())
        .filter(Boolean),
    ),
  );
}

function isValidE164(phone: string): boolean {
  return /^\+[1-9]\d{1,14}$/.test(phone);
}

function isLikelyEmail(email: string): boolean {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
}

function estimateSmsSegments(text: string): { segments: number; perSegment: number } {
  const body = String(text || '');
  if (!body) return { segments: 0, perSegment: 160 };
  const hasUnicode = Array.from(body).some((char) => char.charCodeAt(0) > 127);
  const single = hasUnicode ? 70 : 160;
  const multi = hasUnicode ? 67 : 153;
  if (body.length <= single) return { segments: 1, perSegment: single };
  return { segments: Math.ceil(body.length / multi), perSegment: multi };
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

function asCallLogRows(value: unknown): CallLogRow[] {
  if (!Array.isArray(value)) return [];
  return value.map((entry) => asRecord(entry) as CallLogRow);
}

function asCallScripts(value: unknown): CallScriptRow[] {
  if (!Array.isArray(value)) return [];
  return value.map((entry) => asRecord(entry) as CallScriptRow);
}

function asMiniAppUsers(value: unknown): MiniAppUserRow[] {
  if (!Array.isArray(value)) return [];
  return value.map((entry) => asRecord(entry) as MiniAppUserRow);
}

function asAuditRows(value: unknown): AuditFeedRow[] {
  if (!Array.isArray(value)) return [];
  return value.map((entry) => asRecord(entry) as AuditFeedRow);
}

function asIncidentRows(value: unknown): IncidentRow[] {
  if (!Array.isArray(value)) return [];
  return value.map((entry) => asRecord(entry) as IncidentRow);
}

function asRunbooks(value: unknown): RunbookRow[] {
  if (!Array.isArray(value)) return [];
  return value.map((entry) => asRecord(entry) as RunbookRow);
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

function parseApiErrorCode(payload: unknown): string {
  const body = asRecord(payload) as ApiErrorPayload;
  return toText(body.code, '');
}

function isSessionBootstrapBlockingCode(code: string): boolean {
  return [
    'miniapp_invalid_signature',
    'miniapp_missing_init_data',
    'miniapp_init_data_expired',
    'miniapp_auth_date_future',
    'miniapp_admin_required',
  ].includes(String(code || '').trim());
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
  const initDataRaw = initDataRawFromHook;
  const initDataState = useSignal(initData.state) as SessionState | undefined;
  const settingsButtonSupported = useSignal(settingsButton.isSupported);
  const settingsButtonMounted = useSignal(settingsButton.isMounted);
  const settingsButtonVisible = useSignal(settingsButton.isVisible);
  const [token, setToken] = useState<string | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string>('');
  const [errorCode, setErrorCode] = useState<string>('');
  const [notice, setNotice] = useState<string>('');
  const [bootstrap, setBootstrap] = useState<DashboardApiPayload | null>(null);
  const [pollPayload, setPollPayload] = useState<DashboardApiPayload | null>(null);
  const [busyAction, setBusyAction] = useState<string>('');
  const [pollFailureCount, setPollFailureCount] = useState<number>(0);
  const [lastPollAt, setLastPollAt] = useState<number | null>(null);
  const [lastSuccessfulPollAt, setLastSuccessfulPollAt] = useState<number | null>(null);
  const [nextPollAt, setNextPollAt] = useState<number | null>(null);
  const [sessionBlocked, setSessionBlocked] = useState<boolean>(false);
  const [activityLog, setActivityLog] = useState<ActivityEntry[]>([]);
  const [activeModule, setActiveModule] = useState<DashboardModule>('ops');
  const [settingsOpen, setSettingsOpen] = useState<boolean>(false);
  const [pollingPaused, setPollingPaused] = useState<boolean>(false);
  const [smsRecipientsInput, setSmsRecipientsInput] = useState<string>('');
  const [smsMessageInput, setSmsMessageInput] = useState<string>('');
  const [smsScheduleAt, setSmsScheduleAt] = useState<string>('');
  const [smsProviderInput, setSmsProviderInput] = useState<string>('');
  const [mailerRecipientsInput, setMailerRecipientsInput] = useState<string>('');
  const [mailerSubjectInput, setMailerSubjectInput] = useState<string>('');
  const [mailerHtmlInput, setMailerHtmlInput] = useState<string>('');
  const [mailerTextInput, setMailerTextInput] = useState<string>('');
  const [mailerTemplateIdInput, setMailerTemplateIdInput] = useState<string>('');
  const [mailerVariablesInput, setMailerVariablesInput] = useState<string>('{}');
  const [mailerScheduleAt, setMailerScheduleAt] = useState<string>('');
  const [runtimeCanaryInput, setRuntimeCanaryInput] = useState<string>('');
  const [scriptFlowFilter, setScriptFlowFilter] = useState<string>('');
  const [callScriptsSnapshot, setCallScriptsSnapshot] = useState<CallScriptsPayload | null>(null);
  const [selectedCallScriptId, setSelectedCallScriptId] = useState<number>(0);
  const [scriptNameInput, setScriptNameInput] = useState<string>('');
  const [scriptDescriptionInput, setScriptDescriptionInput] = useState<string>('');
  const [scriptDefaultProfileInput, setScriptDefaultProfileInput] = useState<string>('');
  const [scriptPromptInput, setScriptPromptInput] = useState<string>('');
  const [scriptFirstMessageInput, setScriptFirstMessageInput] = useState<string>('');
  const [scriptObjectiveTagsInput, setScriptObjectiveTagsInput] = useState<string>('');
  const [scriptReviewNoteInput, setScriptReviewNoteInput] = useState<string>('');
  const [scriptSimulationVariablesInput, setScriptSimulationVariablesInput] = useState<string>('{}');
  const [scriptSimulationResult, setScriptSimulationResult] = useState<CallScriptSimulationPayload | null>(null);
  const [providerPreflightBusy, setProviderPreflightBusy] = useState<string>('');
  const [providerPreflightRows, setProviderPreflightRows] = useState<Record<string, string>>({});
  const [providerRollbackByChannel, setProviderRollbackByChannel] = useState<
    Partial<Record<ProviderChannel, string>>
  >({});
  const [userSearch, setUserSearch] = useState<string>('');
  const [userSortBy, setUserSortBy] = useState<string>('last_activity');
  const [userSortDir, setUserSortDir] = useState<string>('desc');
  const [usersSnapshot, setUsersSnapshot] = useState<MiniAppUsersPayload | null>(null);
  const [auditSnapshot, setAuditSnapshot] = useState<MiniAppAuditPayload | null>(null);
  const [incidentsSnapshot, setIncidentsSnapshot] = useState<MiniAppIncidentsPayload | null>(null);
  const sessionRequestRef = useRef<Promise<string> | null>(null);
  const pollFailureNotedRef = useRef<boolean>(false);

  const pushActivity = useCallback((
    status: ActivityStatus,
    title: string,
    detail: string,
  ): void => {
    const entry: ActivityEntry = {
      id: `${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
      title,
      detail,
      status,
      at: new Date().toISOString(),
    };
    setActivityLog((prev) => [entry, ...prev].slice(0, MAX_ACTIVITY_ITEMS));
  }, []);

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
      setSessionBlocked(false);
      return cached.token;
    }

    if (sessionRequestRef.current) {
      return sessionRequestRef.current;
    }

    if (!initDataRaw) {
      setSessionBlocked(true);
      setErrorCode('miniapp_missing_init_data');
      pushActivity(
        'error',
        'Session blocked',
        'Mini App init data is unavailable. Open this page from Telegram.',
      );
      throw new Error('Mini App init data is unavailable. Open this page from Telegram.');
    }

    const sessionRequest = (async (): Promise<string> => {
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
      const code = toText(payload?.code, '');
      if (!response.ok || !payload?.success || !payload?.token) {
        if (isSessionBootstrapBlockingCode(code)) {
          setSessionBlocked(true);
        }
        if (code) {
          setErrorCode(code);
        }
        throw new Error(payload?.error || `Session request failed (${response.status})`);
      }

      const nextToken = payload.token;
      const cacheEntry: SessionCacheEntry = {
        token: nextToken,
        exp: Number.isFinite(Number(payload.expires_at)) ? Number(payload.expires_at) : null,
      };
      writeSessionCache(cacheEntry);
      setToken(nextToken);
      setErrorCode('');
      setSessionBlocked(false);
      pushActivity('success', 'Session established', 'Mini App session token created successfully.');
      return nextToken;
    })();

    sessionRequestRef.current = sessionRequest;
    try {
      return await sessionRequest;
    } finally {
      if (sessionRequestRef.current === sessionRequest) {
        sessionRequestRef.current = null;
      }
    }
  }, [initDataRaw, pushActivity]);

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
      pushActivity('info', 'Session refresh', 'Received 401, refreshing session token.');
      return request<T>(path, options, retryCount + 1);
    }
    if (!response.ok) {
      const code = parseApiErrorCode(payload);
      if (code) {
        setErrorCode(code);
      }
      if (isSessionBootstrapBlockingCode(code)) {
        setSessionBlocked(true);
      }
      throw new Error(parseApiError(payload, response.status));
    }
    setErrorCode('');
    return payload as T;
  }, [createSession, pushActivity, token]);

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
      setUsersSnapshot(payload.dashboard?.users || payload.users || null);
      setAuditSnapshot(payload.dashboard?.audit || payload.audit || null);
      setIncidentsSnapshot(payload.dashboard?.incidents || payload.incidents || null);
      setCallScriptsSnapshot(payload.dashboard?.call_scripts || payload.call_scripts || null);
      const runtimePayload = asRecord(payload.dashboard?.voice_runtime || payload.voice_runtime || {});
      const runtime = asRecord(runtimePayload.runtime);
      const overrideCanary = Number(runtime.canary_percent_override);
      if (Number.isFinite(overrideCanary)) {
        setRuntimeCanaryInput(String(Math.max(0, Math.min(100, Math.round(overrideCanary)))));
      }
      pushActivity('success', 'Dashboard synced', 'Bootstrap data loaded.');
    } catch (err) {
      setPollFailureCount((prev) => prev + 1);
      const detail = err instanceof Error ? err.message : String(err);
      setError(detail);
      pushActivity('error', 'Bootstrap failed', detail);
    } finally {
      setLoading(false);
    }
  }, [pushActivity, request]);

  const loadPoll = useCallback(async (): Promise<boolean> => {
    const startedAt = Date.now();
    setLastPollAt(startedAt);
    try {
      const payload = await request<DashboardApiPayload>('/miniapp/jobs/poll');
      setError('');
      setPollPayload(payload);
      setPollFailureCount(0);
      setLastSuccessfulPollAt(Date.now());
      if (payload.users) {
        setUsersSnapshot(payload.users);
      }
      if (payload.audit) {
        setAuditSnapshot(payload.audit);
      }
      if (payload.incidents) {
        setIncidentsSnapshot(payload.incidents);
      }
      if (pollFailureNotedRef.current) {
        pushActivity('success', 'Live sync recovered', 'Polling resumed successfully.');
      }
      pollFailureNotedRef.current = false;
      return true;
    } catch (err) {
      setPollFailureCount((prev) => prev + 1);
      const detail = err instanceof Error ? err.message : String(err);
      setError(detail);
      if (!pollFailureNotedRef.current) {
        pushActivity('error', 'Live sync degraded', detail);
      }
      pollFailureNotedRef.current = true;
      return false;
    }
  }, [pushActivity, request]);

  const invokeAction = useCallback(async (
    action: string,
    payload: Record<string, unknown>,
  ): Promise<unknown> => {
    const result = await request<{ success?: boolean; data?: unknown; error?: string }>('/miniapp/action', {
      method: 'POST',
      body: JSON.stringify({ action, payload }),
    });
    if (!result?.success) {
      throw new Error(toText((result as Record<string, unknown>)?.error, 'Action failed'));
    }
    return result.data;
  }, [request]);

  const runAction = useCallback(async (
    action: string,
    payload: Record<string, unknown>,
    options: { confirmText?: string; successMessage?: string } = {},
  ) => {
    if (options.confirmText && typeof window !== 'undefined') {
      const allowed = window.confirm(options.confirmText);
      if (!allowed) {
        pushActivity('info', 'Action cancelled', `Cancelled: ${action}`);
        return;
      }
    }
    setBusyAction(action);
    setNotice('');
    setError('');
    try {
      await invokeAction(action, payload);
      const successMessage = options.successMessage || `Action completed: ${action}`;
      setNotice(successMessage);
      pushActivity('success', 'Action completed', successMessage);
      await loadBootstrap();
    } catch (err) {
      const detail = err instanceof Error ? err.message : String(err);
      setError(detail);
      pushActivity('error', `Action failed: ${action}`, detail);
    } finally {
      setBusyAction('');
    }
  }, [invokeAction, loadBootstrap, pushActivity]);

  useEffect(() => {
    void loadBootstrap();
  }, [loadBootstrap]);

  useEffect(() => {
    if (!settingsButton.onClick.isAvailable()) {
      return undefined;
    }
    return settingsButton.onClick(() => {
      setSettingsOpen((prev) => !prev);
    });
  }, []);

  const serverPollIntervalMs = useMemo(() => {
    const intervalSeconds = Number(bootstrap?.poll_interval_seconds);
    if (!Number.isFinite(intervalSeconds) || intervalSeconds <= 0) {
      return POLL_BASE_INTERVAL_MS;
    }
    return Math.max(3000, Math.min(POLL_MAX_INTERVAL_MS, Math.floor(intervalSeconds * 1000)));
  }, [bootstrap?.poll_interval_seconds]);

  useEffect(() => {
    if (!token || sessionBlocked || pollingPaused) return undefined;
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
  }, [loadPoll, pollingPaused, serverPollIntervalMs, sessionBlocked, token]);

  const dashboard = bootstrap?.dashboard;
  const providerPayload = pollPayload?.provider || dashboard?.provider || {};
  const providersByChannel = providerPayload.providers || {};
  const providerCompatibilityPayload =
    pollPayload?.provider_compatibility || dashboard?.provider_compatibility || {};
  const providerCompatibilityRoot = asRecord(
    asRecord(providerCompatibilityPayload).compatibility || providerCompatibilityPayload,
  );
  const providerCompatibilityChannels = asRecord(providerCompatibilityRoot.channels);
  const smsPayload = pollPayload?.sms_bulk || dashboard?.sms_bulk || {};
  const smsSummary = smsPayload.summary || {};
  const emailStatsPayload = pollPayload?.email_bulk_stats || dashboard?.email_bulk_stats || {};
  const emailStats = emailStatsPayload.stats || {};
  const emailHistoryPayload = pollPayload?.email_bulk_history || dashboard?.email_bulk_history || {};
  const emailJobs = asEmailJobs(emailHistoryPayload.jobs);
  const dlqPayload = pollPayload?.dlq || dashboard?.dlq || {};
  const callLogsPayload = pollPayload?.call_logs || dashboard?.call_logs || {};
  const callScriptsPayload = callScriptsSnapshot || pollPayload?.call_scripts || dashboard?.call_scripts || {};
  const callStatsPayload = pollPayload?.call_stats || dashboard?.call_stats || {};
  const voiceRuntimePayload = pollPayload?.voice_runtime || dashboard?.voice_runtime || {};
  const opsPayload = pollPayload?.ops || dashboard?.ops || {};
  const opsQueueBacklog = opsPayload.queue_backlog || {};
  const usersPayload = usersSnapshot || dashboard?.users || {};
  const auditPayload = auditSnapshot || dashboard?.audit || {};
  const incidentsPayload = incidentsSnapshot || dashboard?.incidents || {};
  const sessionPayload = asRecord(
    pollPayload?.session || bootstrap?.session || dashboard?.session || {},
  ) as MiniAppSessionSummary;
  const callDlq = asDlqCallRows(dlqPayload.call_preview);
  const emailDlq = asDlqEmailRows(dlqPayload.email_preview);
  const callLogs = asCallLogRows(callLogsPayload.rows);
  const callScripts = asCallScripts(callScriptsPayload.scripts);
  const usersRows = asMiniAppUsers(usersPayload.rows);
  const auditRows = asAuditRows(auditPayload.rows);
  const incidentRows = asIncidentRows(incidentsPayload.alerts);
  const runbookRows = asRunbooks(incidentsPayload.runbooks);
  const sessionRole = toText(sessionPayload.role, 'viewer').toLowerCase();
  const sessionRoleSource = toText(sessionPayload.role_source, 'inferred');
  const sessionCaps = asStringList(sessionPayload.caps);
  const hasCapability = (capability: string): boolean => sessionCaps.includes(capability);
  const visibleModules = MODULE_DEFINITIONS.filter((module) => hasCapability(module.capability));
  const settingsStatusLabel = !settingsButtonSupported
    ? 'Unsupported'
    : settingsButtonMounted
      ? settingsButtonVisible
        ? 'Visible'
        : 'Mounted'
      : 'Pending';

  useEffect(() => {
    if (visibleModules.length === 0) return;
    if (!visibleModules.some((module) => module.id === activeModule)) {
      setActiveModule(visibleModules[0].id);
    }
  }, [activeModule, visibleModules]);

  useEffect(() => {
    if (callScripts.length === 0) {
      setSelectedCallScriptId(0);
      return;
    }
    if (!callScripts.some((script) => toInt(script.id) === selectedCallScriptId)) {
      setSelectedCallScriptId(toInt(callScripts[0]?.id));
    }
  }, [callScripts, selectedCallScriptId]);

  useEffect(() => {
    const currentScript =
      callScripts.find((script) => toInt(script.id) === selectedCallScriptId) || null;
    if (!currentScript) return;
    setScriptNameInput(toText(currentScript.name, ''));
    setScriptDescriptionInput(toText(currentScript.description, ''));
    setScriptDefaultProfileInput(toText(currentScript.default_profile, ''));
    setScriptPromptInput(toText(currentScript.prompt, ''));
    setScriptFirstMessageInput(toText(currentScript.first_message, ''));
    const tags = asStringList(currentScript.objective_tags);
    setScriptObjectiveTagsInput(tags.join(', '));
  }, [callScripts, selectedCallScriptId]);

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
  const emailBounced = toInt(emailStats.bounced);
  const emailComplained = toInt(emailStats.complained);
  const emailSuppressed = toInt(emailStats.suppressed);
  const emailProcessedPercent = emailTotalRecipients > 0
    ? Math.round(((emailSent + emailFailed) / emailTotalRecipients) * 100)
    : 0;
  const emailDeliveredPercent = emailTotalRecipients > 0
    ? Math.round((emailDelivered / emailTotalRecipients) * 100)
    : 0;
  const emailBouncePercent = emailTotalRecipients > 0
    ? Math.round((emailBounced / emailTotalRecipients) * 100)
    : 0;
  const emailComplaintPercent = emailTotalRecipients > 0
    ? Math.round((emailComplained / emailTotalRecipients) * 100)
    : 0;
  const bridgeStatuses = normalizeBridgeStatuses(pollPayload?.bridge || dashboard?.bridge);
  const bridgeHardFailures = bridgeStatuses.filter((status) => status >= 500).length;
  const bridgeSoftFailures = bridgeStatuses.filter((status) => status >= 400).length;
  const hasBootstrapData = Boolean(
    bootstrap?.success
    || bootstrap?.dashboard
    || pollPayload?.dashboard
    || pollPayload?.provider
    || pollPayload?.sms_bulk
    || pollPayload?.email_bulk_stats
    || pollPayload?.dlq,
  );
  const hasProviderData = Object.keys(asRecord(providersByChannel)).length > 0;
  const hasBulkVolume = smsTotalRecipients > 0 || emailTotalRecipients > 0;
  const hasQueueData = emailJobs.length > 0 || callDlq.length > 0 || emailDlq.length > 0;
  const hasMeaningfulData = hasProviderData || hasBulkVolume || hasQueueData || callLogs.length > 0;
  const isDashboardDegraded = sessionBlocked
    || pollFailureCount >= POLL_DEGRADED_FAILURES
    || bridgeHardFailures > 0
    || (Boolean(error) && !hasBootstrapData);
  const syncModeLabel = pollingPaused
    ? 'Paused'
    : isDashboardDegraded
      ? 'Degraded'
      : 'Healthy';
  const nextPollLabel = pollingPaused
    ? 'Paused'
    : nextPollAt
      ? formatTime(new Date(nextPollAt).toISOString())
      : '—';
  const lastPollLabel = lastPollAt ? formatTime(new Date(lastPollAt).toISOString()) : '—';
  const lastSuccessfulPollLabel = lastSuccessfulPollAt
    ? formatTime(new Date(lastSuccessfulPollAt).toISOString())
    : '—';
  const uptimeScore = Math.max(
    0,
    100 - (pollFailureCount * 12) - (bridgeHardFailures * 18) - (bridgeSoftFailures * 6),
  );
  const callTotal = toInt(callStatsPayload.total_calls);
  const callCompleted = toInt(callStatsPayload.completed_calls);
  const callFailed = toInt(callStatsPayload.failed_calls);
  const callSuccessRateRaw = Number(callStatsPayload.success_rate);
  const callSuccessRate = Number.isFinite(callSuccessRateRaw)
    ? Math.max(0, Math.min(100, Math.round(callSuccessRateRaw)))
    : (callTotal > 0 ? Math.round((callCompleted / callTotal) * 100) : 0);
  const queueBacklogTotal = toInt(
    opsQueueBacklog.total,
    toInt(opsQueueBacklog.dlq_call_open)
      + toInt(opsQueueBacklog.dlq_email_open)
      + toInt(opsQueueBacklog.sms_failed)
      + toInt(opsQueueBacklog.email_failed),
  );
  const callFailureRate = callTotal > 0 ? Math.round((callFailed / callTotal) * 100) : 0;
  const callLogsTotal = toInt(callLogsPayload.total, callLogs.length);
  const voiceRuntime = asRecord(voiceRuntimePayload.runtime);
  const voiceRuntimeCircuit = asRecord(voiceRuntime.circuit);
  const voiceRuntimeActiveCalls = asRecord(voiceRuntimePayload.active_calls);
  const runtimeEffectiveMode = toText(
    voiceRuntime.effective_mode,
    toText(voiceRuntime.configured_mode, 'unknown'),
  );
  const runtimeModeOverride = toText(voiceRuntime.mode_override, 'none');
  const runtimeCanaryEffective = toInt(voiceRuntime.effective_canary_percent);
  const runtimeCanaryOverride = Number(voiceRuntime.canary_percent_override);
  const runtimeCanaryOverrideLabel = Number.isFinite(runtimeCanaryOverride)
    ? `${Math.max(0, Math.min(100, Math.round(runtimeCanaryOverride)))}%`
    : 'none';
  const runtimeIsCircuitOpen = voiceRuntimeCircuit.is_open === true;
  const runtimeForcedLegacyUntil = formatTime(voiceRuntimeCircuit.forced_legacy_until);
  const runtimeActiveTotal = toInt(voiceRuntimeActiveCalls.total);
  const runtimeActiveLegacy = toInt(voiceRuntimeActiveCalls.legacy);
  const runtimeActiveVoiceAgent = toInt(voiceRuntimeActiveCalls.voice_agent);
  const callScriptsTotal = toInt(callScriptsPayload.total, callScripts.length);
  const selectedCallScript =
    callScripts.find((script) => toInt(script.id) === selectedCallScriptId) || null;
  const selectedCallScriptLifecycle = asRecord(selectedCallScript?.lifecycle);
  const selectedCallScriptLifecycleState = toText(
    selectedCallScript?.lifecycle_state || selectedCallScriptLifecycle.lifecycle_state,
    'draft',
  ).toLowerCase();
  const providerMatrixRows = (['call', 'sms', 'email'] as ProviderChannel[])
    .flatMap((channel) => {
      const channelDetails = asRecord(providerCompatibilityChannels[channel]);
      const providers = asRecord(channelDetails.providers);
      const parityGaps = asRecord(channelDetails.parity_gaps);
      const rows = Object.entries(providers).map(([provider, raw]) => {
        const details = asRecord(raw);
        return {
          channel,
          provider,
          ready: details.ready === true,
          degraded: details.degraded === true,
          flowCount: asStringList(details.flows).length,
          parityGapCount: asStringList(parityGaps[provider]).length,
          paymentMode: toText(details.payment_mode, 'n/a'),
        };
      });
      if (rows.length > 0) {
        return rows;
      }
      const channelData = providersByChannel[channel] || {};
      const supported = asStringList(channelData.supported_providers);
      const readiness = asRecord(channelData.readiness);
      return supported.map((provider) => ({
        channel,
        provider,
        ready: readiness[provider] !== false,
        degraded: false,
        flowCount: 0,
        parityGapCount: 0,
        paymentMode: 'n/a',
      }));
    })
    .sort((left, right) => {
      const channelSort = left.channel.localeCompare(right.channel);
      return channelSort === 0 ? left.provider.localeCompare(right.provider) : channelSort;
    });
  const providerReadinessTotals = providerMatrixRows.reduce(
    (acc, row) => ({
      ready: acc.ready + (row.ready ? 1 : 0),
      total: acc.total + 1,
    }),
    { ready: 0, total: 0 },
  );
  const providerReadinessPercent = providerReadinessTotals.total > 0
    ? Math.round((providerReadinessTotals.ready / providerReadinessTotals.total) * 100)
    : 0;
  const providerDegradedCount = providerMatrixRows.filter((row) => row.degraded).length;
  const smsRecipientsParsed = parsePhoneList(smsRecipientsInput);
  const smsInvalidRecipients = smsRecipientsParsed.filter((phone) => !isValidE164(phone));
  const smsDuplicateCount = Math.max(
    0,
    String(smsRecipientsInput || '')
      .split(/[\n,;\t ]+/g)
      .filter(Boolean).length - smsRecipientsParsed.length,
  );
  const smsSegmentEstimate = estimateSmsSegments(smsMessageInput);
  const mailerRecipientsParsed = parseEmailList(mailerRecipientsInput);
  const mailerInvalidRecipients = mailerRecipientsParsed.filter((email) => !isLikelyEmail(email));
  const mailerDuplicateCount = Math.max(
    0,
    String(mailerRecipientsInput || '')
      .split(/[\n,;\t ]+/g)
      .filter(Boolean).length - mailerRecipientsParsed.length,
  );
  const mailerVariableKeys = Array.from(
    new Set([
      ...Array.from(String(mailerSubjectInput || '').matchAll(/{{\s*([\w.-]+)\s*}}/g)).map((m) => m[1]),
      ...Array.from(String(mailerHtmlInput || '').matchAll(/{{\s*([\w.-]+)\s*}}/g)).map((m) => m[1]),
      ...Array.from(String(mailerTextInput || '').matchAll(/{{\s*([\w.-]+)\s*}}/g)).map((m) => m[1]),
    ]),
  );

  const handleRefresh = (): void => {
    pushActivity('info', 'Manual refresh', 'Operator triggered dashboard refresh.');
    void loadBootstrap();
  };

  const resetSession = useCallback((): void => {
    writeSessionCache(null);
    sessionRequestRef.current = null;
    setToken(null);
    setError('');
    setErrorCode('');
    setNotice('');
    setSessionBlocked(false);
    setBootstrap(null);
    setPollPayload(null);
    setLastPollAt(null);
    setLastSuccessfulPollAt(null);
    setNextPollAt(null);
    setPollFailureCount(0);
    setPollingPaused(false);
    setRuntimeCanaryInput('');
    setCallScriptsSnapshot(null);
    setSelectedCallScriptId(0);
    setScriptSimulationResult(null);
    setSettingsOpen(false);
    setActivityLog([]);
    pollFailureNotedRef.current = false;
    void loadBootstrap();
  }, [loadBootstrap]);

  const handleRecipientsFile = useCallback(async (
    file: File | null,
    kind: 'sms' | 'mailer',
  ): Promise<void> => {
    if (!file) return;
    const text = await file.text().catch(() => '');
    if (!text.trim()) return;
    const combined = text.replace(/[,\t;]/g, '\n');
    if (kind === 'sms') {
      setSmsRecipientsInput((prev) => `${prev}${prev ? '\n' : ''}${combined}`.trim());
      pushActivity('info', 'CSV imported', 'SMS recipient list imported from file.');
      return;
    }
    setMailerRecipientsInput((prev) => `${prev}${prev ? '\n' : ''}${combined}`.trim());
    pushActivity('info', 'CSV imported', 'Mailer recipient list imported from file.');
  }, [pushActivity]);

  const refreshUsersModule = useCallback(async (): Promise<void> => {
    try {
      const data = await invokeAction('users.list', {
        limit: 120,
        offset: 0,
        search: userSearch,
        sort_by: userSortBy,
        sort_dir: userSortDir,
      }) as MiniAppUsersPayload;
      setUsersSnapshot(data);
      pushActivity('success', 'Users refreshed', 'User and role list reloaded.');
    } catch (err) {
      const detail = err instanceof Error ? err.message : String(err);
      setError(detail);
      pushActivity('error', 'Users refresh failed', detail);
    }
  }, [invokeAction, pushActivity, userSearch, userSortBy, userSortDir]);

  const refreshAuditModule = useCallback(async (): Promise<void> => {
    try {
      const [auditData, incidentData] = await Promise.all([
        invokeAction('audit.feed', { limit: 80, hours: 24 }),
        invokeAction('incidents.summary', { limit: 80, hours: 24 }),
      ]);
      setAuditSnapshot(auditData as MiniAppAuditPayload);
      setIncidentsSnapshot(incidentData as MiniAppIncidentsPayload);
      pushActivity('success', 'Audit refreshed', 'Audit and incident feeds reloaded.');
    } catch (err) {
      const detail = err instanceof Error ? err.message : String(err);
      setError(detail);
      pushActivity('error', 'Audit refresh failed', detail);
    }
  }, [invokeAction, pushActivity]);

  const handleApplyUserRole = useCallback(async (telegramId: string, role: string): Promise<void> => {
    const reason = typeof window !== 'undefined'
      ? (window.prompt(`Provide audit reason for ${telegramId} -> ${role}`) || '').trim()
      : '';
    if (!reason) {
      pushActivity('info', 'Role update cancelled', `No audit reason supplied for ${telegramId}.`);
      return;
    }
    await runAction(
      'users.role.set',
      { telegram_id: telegramId, role, reason },
      {
        confirmText: `Set role for ${telegramId} to ${role}?`,
        successMessage: `Updated ${telegramId} role to ${role}.`,
      },
    );
    await refreshUsersModule();
  }, [pushActivity, refreshUsersModule, runAction]);

  const runbookAction = useCallback(async (
    action: string,
    payload: Record<string, unknown> = {},
  ): Promise<void> => {
    const normalizedAction = String(action || '').trim().toLowerCase();
    let nextPayload: Record<string, unknown> = { ...payload };
    if (normalizedAction === 'runbook.provider.preflight') {
      const selectedChannel = toText(nextPayload.channel, 'call').toLowerCase();
      const normalizedChannel = (['call', 'sms', 'email'] as ProviderChannel[]).includes(
        selectedChannel as ProviderChannel,
      )
        ? selectedChannel as ProviderChannel
        : 'call';
      const fallbackProvider = toText(
        asRecord(providersByChannel[normalizedChannel]).provider,
        '',
      ).toLowerCase();
      const selectedProvider = toText(nextPayload.provider, fallbackProvider).toLowerCase();
      if (!selectedProvider) {
        pushActivity('error', 'Runbook blocked', 'No active provider available for preflight runbook.');
        return;
      }
      nextPayload = {
        ...nextPayload,
        channel: normalizedChannel,
        provider: selectedProvider,
      };
    }
    await runAction(action, nextPayload, {
      confirmText: `Execute runbook action "${action}"?`,
      successMessage: `Runbook executed: ${action}`,
    });
    await refreshAuditModule();
  }, [providersByChannel, pushActivity, refreshAuditModule, runAction]);

  const preflightActiveProviders = useCallback(async (): Promise<void> => {
    const targets = (['call', 'sms', 'email'] as ProviderChannel[])
      .map((channel) => {
        const provider = toText(asRecord(providersByChannel[channel]).provider, '').toLowerCase();
        return { channel, provider };
      })
      .filter((target) => Boolean(target.provider));
    if (targets.length === 0) {
      setError('No active providers available for preflight.');
      return;
    }
    setProviderPreflightBusy('all');
    setError('');
    setNotice('');
    try {
      for (const target of targets) {
        const key = `${target.channel}:${target.provider}`;
        const result = await invokeAction('provider.preflight', {
          channel: target.channel,
          provider: target.provider,
          network: 1,
          reachability: 1,
        }) as Record<string, unknown>;
        const status = result?.success === true ? 'ok' : toText(result?.error, 'failed');
        setProviderPreflightRows((prev) => ({
          ...prev,
          [key]: status,
        }));
      }
      const message = `Preflight completed for ${targets.length} active provider(s).`;
      setNotice(message);
      pushActivity('success', 'Provider preflight batch', message);
      await loadBootstrap();
    } catch (err) {
      const detail = err instanceof Error ? err.message : String(err);
      setError(detail);
      pushActivity('error', 'Provider preflight batch failed', detail);
    } finally {
      setProviderPreflightBusy('');
    }
  }, [invokeAction, loadBootstrap, providersByChannel, pushActivity]);

  const refreshRuntimeStatus = useCallback(async (): Promise<void> => {
    try {
      const data = await invokeAction('runtime.status', {}) as VoiceRuntimePayload;
      setPollPayload((prev) => ({
        ...(prev || {}),
        voice_runtime: data,
      }));
      const canaryOverride = Number(asRecord(data?.runtime).canary_percent_override);
      if (Number.isFinite(canaryOverride)) {
        setRuntimeCanaryInput(String(Math.max(0, Math.min(100, Math.round(canaryOverride)))));
      }
      pushActivity('success', 'Runtime refreshed', 'Voice runtime status updated.');
    } catch (err) {
      const detail = err instanceof Error ? err.message : String(err);
      setError(detail);
      pushActivity('error', 'Runtime refresh failed', detail);
    }
  }, [invokeAction, pushActivity]);

  const enableRuntimeMaintenance = useCallback(async (): Promise<void> => {
    await runAction(
      'runtime.maintenance.enable',
      { duration_ms: 15 * 60 * 1000 },
      {
        confirmText: 'Enable maintenance mode (legacy-only) for 15 minutes?',
        successMessage: 'Maintenance mode enabled for 15 minutes.',
      },
    );
  }, [runAction]);

  const disableRuntimeMaintenance = useCallback(async (): Promise<void> => {
    await runAction(
      'runtime.maintenance.disable',
      {},
      {
        confirmText: 'Disable maintenance mode and reset runtime circuit now?',
        successMessage: 'Maintenance mode disabled and runtime circuit reset.',
      },
    );
  }, [runAction]);

  const applyRuntimeCanary = useCallback(async (): Promise<void> => {
    const parsed = Number(runtimeCanaryInput);
    if (!Number.isFinite(parsed) || parsed < 0 || parsed > 100) {
      setError('Canary override must be a number between 0 and 100.');
      return;
    }
    await runAction(
      'runtime.canary.set',
      { canary_percent: Math.round(parsed) },
      {
        confirmText: `Set runtime canary override to ${Math.round(parsed)}%?`,
        successMessage: `Runtime canary override set to ${Math.round(parsed)}%.`,
      },
    );
  }, [runAction, runtimeCanaryInput]);

  const clearRuntimeCanary = useCallback(async (): Promise<void> => {
    await runAction(
      'runtime.canary.clear',
      {},
      {
        confirmText: 'Clear runtime canary override and return to configured value?',
        successMessage: 'Runtime canary override cleared.',
      },
    );
    setRuntimeCanaryInput('');
  }, [runAction]);

  const refreshCallScriptsModule = useCallback(async (): Promise<void> => {
    try {
      const data = await invokeAction('callscript.list', {
        limit: 120,
        flow_type: scriptFlowFilter || undefined,
      }) as CallScriptsPayload;
      setCallScriptsSnapshot(data);
      pushActivity('success', 'Scripts refreshed', 'Call script list reloaded.');
    } catch (err) {
      const detail = err instanceof Error ? err.message : String(err);
      setError(detail);
      pushActivity('error', 'Script refresh failed', detail);
    }
  }, [invokeAction, pushActivity, scriptFlowFilter]);

  const saveCallScriptDraft = useCallback(async (): Promise<void> => {
    if (selectedCallScriptId <= 0) {
      setError('Select a script before saving draft changes.');
      return;
    }
    const payload = {
      id: selectedCallScriptId,
      name: scriptNameInput.trim(),
      description: scriptDescriptionInput,
      default_profile: scriptDefaultProfileInput.trim().toLowerCase(),
      prompt: scriptPromptInput,
      first_message: scriptFirstMessageInput,
      objective_tags: scriptObjectiveTagsInput,
    };
    await runAction(
      'callscript.update',
      payload,
      {
        confirmText: `Save draft changes to script #${selectedCallScriptId}?`,
        successMessage: `Draft updated for script #${selectedCallScriptId}.`,
      },
    );
    await refreshCallScriptsModule();
  }, [
    refreshCallScriptsModule,
    runAction,
    scriptDefaultProfileInput,
    scriptDescriptionInput,
    scriptFirstMessageInput,
    scriptNameInput,
    scriptObjectiveTagsInput,
    scriptPromptInput,
    selectedCallScriptId,
  ]);

  const submitCallScriptForReview = useCallback(async (): Promise<void> => {
    if (selectedCallScriptId <= 0) {
      setError('Select a script before submitting for review.');
      return;
    }
    await runAction(
      'callscript.submit_review',
      { id: selectedCallScriptId },
      {
        confirmText: `Submit script #${selectedCallScriptId} for review?`,
        successMessage: `Script #${selectedCallScriptId} submitted for review.`,
      },
    );
    await refreshCallScriptsModule();
  }, [refreshCallScriptsModule, runAction, selectedCallScriptId]);

  const reviewCallScript = useCallback(async (decision: 'approve' | 'reject'): Promise<void> => {
    if (selectedCallScriptId <= 0) {
      setError('Select a script before submitting a review decision.');
      return;
    }
    await runAction(
      'callscript.review',
      {
        id: selectedCallScriptId,
        decision,
        note: scriptReviewNoteInput.trim() || undefined,
      },
      {
        confirmText: `${decision === 'approve' ? 'Approve' : 'Reject'} script #${selectedCallScriptId}?`,
        successMessage: `Review decision recorded for script #${selectedCallScriptId}.`,
      },
    );
    await refreshCallScriptsModule();
  }, [refreshCallScriptsModule, runAction, scriptReviewNoteInput, selectedCallScriptId]);

  const promoteCallScriptLive = useCallback(async (): Promise<void> => {
    if (selectedCallScriptId <= 0) {
      setError('Select a script before promoting to live.');
      return;
    }
    await runAction(
      'callscript.promote_live',
      { id: selectedCallScriptId },
      {
        confirmText: `Promote script #${selectedCallScriptId} to live?`,
        successMessage: `Script #${selectedCallScriptId} promoted to live.`,
      },
    );
    await refreshCallScriptsModule();
  }, [refreshCallScriptsModule, runAction, selectedCallScriptId]);

  const simulateCallScript = useCallback(async (): Promise<void> => {
    if (selectedCallScriptId <= 0) {
      setError('Select a script before running simulation.');
      return;
    }
    let variables: Record<string, unknown> = {};
    if (scriptSimulationVariablesInput.trim()) {
      try {
        variables = asRecord(JSON.parse(scriptSimulationVariablesInput));
      } catch {
        setError('Simulation variables must be valid JSON.');
        return;
      }
    }
    setError('');
    try {
      const data = await invokeAction('callscript.simulate', {
        id: selectedCallScriptId,
        variables,
      }) as CallScriptSimulationPayload;
      setScriptSimulationResult(data);
      pushActivity('success', 'Script simulation complete', `Simulation ready for script #${selectedCallScriptId}.`);
    } catch (err) {
      const detail = err instanceof Error ? err.message : String(err);
      setError(detail);
      pushActivity('error', 'Script simulation failed', detail);
    }
  }, [invokeAction, pushActivity, scriptSimulationVariablesInput, selectedCallScriptId]);

  const sendSmsFromConsole = useCallback(async (): Promise<void> => {
    const recipients = smsRecipientsParsed.filter((phone) => isValidE164(phone));
    if (!recipients.length) {
      setError('Provide at least one valid E.164 recipient.');
      return;
    }
    if (!smsMessageInput.trim()) {
      setError('SMS message is required.');
      return;
    }
    if (smsScheduleAt) {
      setBusyAction('sms.schedule.send');
      setError('');
      setNotice('');
      try {
        const scheduledIso = new Date(smsScheduleAt).toISOString();
        let queued = 0;
        for (const recipient of recipients) {
          await invokeAction('sms.schedule.send', {
            to: recipient,
            message: smsMessageInput,
            scheduled_time: scheduledIso,
            provider: smsProviderInput || undefined,
            options: {
              durable: true,
            },
          });
          queued += 1;
        }
        const msg = `Scheduled ${queued} SMS messages for ${formatTime(scheduledIso)}.`;
        setNotice(msg);
        pushActivity('success', 'SMS scheduled', msg);
      } catch (err) {
        const detail = err instanceof Error ? err.message : String(err);
        setError(detail);
        pushActivity('error', 'SMS scheduling failed', detail);
      } finally {
        setBusyAction('');
      }
      return;
    }
    await runAction(
      'sms.bulk.send',
      {
        recipients,
        message: smsMessageInput,
        provider: smsProviderInput || undefined,
        options: {
          durable: true,
        },
      },
      {
        confirmText: `Send SMS to ${recipients.length} recipients now?`,
        successMessage: `Bulk SMS submitted (${recipients.length} recipients).`,
      },
    );
  }, [
    invokeAction,
    pushActivity,
    runAction,
    smsMessageInput,
    smsProviderInput,
    smsRecipientsParsed,
    smsScheduleAt,
  ]);

  const sendMailerFromConsole = useCallback(async (): Promise<void> => {
    const validRecipients = mailerRecipientsParsed
      .filter((email) => isLikelyEmail(email))
      .map((email) => ({ email }));
    if (!validRecipients.length) {
      setError('Provide at least one valid recipient email.');
      return;
    }
    let parsedVariables: Record<string, unknown> = {};
    if (mailerVariablesInput.trim()) {
      try {
        parsedVariables = asRecord(JSON.parse(mailerVariablesInput));
      } catch {
        setError('Variables must be valid JSON.');
        return;
      }
    }
    const payload: Record<string, unknown> = {
      recipients: validRecipients,
      provider: undefined,
      script_id: mailerTemplateIdInput || undefined,
      subject: mailerSubjectInput || undefined,
      html: mailerHtmlInput || undefined,
      text: mailerTextInput || undefined,
      variables: parsedVariables,
      send_at: mailerScheduleAt ? new Date(mailerScheduleAt).toISOString() : undefined,
    };
    await runAction(
      'email.bulk.send',
      payload,
      {
        confirmText: `Queue bulk email for ${validRecipients.length} recipients?`,
        successMessage: `Bulk email queued (${validRecipients.length} recipients).`,
      },
    );
  }, [
    mailerHtmlInput,
    mailerRecipientsParsed,
    mailerScheduleAt,
    mailerSubjectInput,
    mailerTemplateIdInput,
    mailerTextInput,
    mailerVariablesInput,
    runAction,
  ]);

  const safeSwitchProvider = useCallback(async (
    channel: ProviderChannel,
    targetProvider: string,
    previousProvider: string,
  ): Promise<void> => {
    const normalizedTarget = targetProvider.trim().toLowerCase();
    if (!normalizedTarget) return;
    if (typeof window !== 'undefined') {
      const proceed = window.confirm(
        `Run preflight and switch ${channel.toUpperCase()} provider to "${normalizedTarget}"?`,
      );
      if (!proceed) {
        pushActivity('info', 'Provider switch cancelled', `${channel.toUpperCase()} switch was cancelled.`);
        return;
      }
    }
    const key = `${channel}:${normalizedTarget}`;
    setProviderPreflightBusy(key);
    setError('');
    setNotice('');
    try {
      await invokeAction('provider.preflight', {
        channel,
        provider: normalizedTarget,
        network: 1,
        reachability: 1,
      });
      setProviderPreflightRows((prev) => ({
        ...prev,
        [key]: 'ok',
      }));
      pushActivity('success', 'Preflight completed', `${channel.toUpperCase()} ${normalizedTarget} is ready.`);
      if (previousProvider) {
        setProviderRollbackByChannel((prev) => ({
          ...prev,
          [channel]: previousProvider,
        }));
      }
      await runAction(
        'provider.set',
        { channel, provider: normalizedTarget },
        { successMessage: `${channel.toUpperCase()} provider switched to ${normalizedTarget}.` },
      );
    } catch (err) {
      const detail = err instanceof Error ? err.message : String(err);
      setProviderPreflightRows((prev) => ({
        ...prev,
        [key]: 'failed',
      }));
      setError(detail);
      pushActivity('error', 'Safe switch blocked', `${channel.toUpperCase()} ${normalizedTarget}: ${detail}`);
    } finally {
      setProviderPreflightBusy('');
    }
  }, [invokeAction, pushActivity, runAction]);

  const renderProviderSection = (channel: ProviderChannel) => {
    const channelData = providersByChannel[channel] || {};
    const currentProvider = toText(channelData.provider, '').toLowerCase();
    const supported = asStringList(channelData.supported_providers);
    const readiness = asRecord(channelData.readiness);
    const rollbackTarget = toText(providerRollbackByChannel[channel], '').toLowerCase();

    const runPreflight = async (provider: string): Promise<void> => {
      const key = `${channel}:${provider}`;
      setProviderPreflightBusy(key);
      try {
        const result = await invokeAction('provider.preflight', {
          channel,
          provider,
          network: 1,
          reachability: 1,
        }) as Record<string, unknown>;
        const status = toText(result?.success, '') === 'true' || result?.success === true
          ? 'ok'
          : toText(result?.error, 'failed');
        setProviderPreflightRows((prev) => ({
          ...prev,
          [key]: status,
        }));
        pushActivity('success', 'Preflight completed', `${channel.toUpperCase()} ${provider}: ${status}`);
      } catch (err) {
        const detail = err instanceof Error ? err.message : String(err);
        setProviderPreflightRows((prev) => ({
          ...prev,
          [key]: 'failed',
        }));
        pushActivity('error', 'Preflight failed', `${channel.toUpperCase()} ${provider}: ${detail}`);
      } finally {
        setProviderPreflightBusy('');
      }
    };

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
              void safeSwitchProvider(channel, normalized, currentProvider);
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
        <div className="va-provider-tools">
          {supported.map((provider) => {
            const key = `${channel}:${provider}`;
            const state = providerPreflightRows[key] || 'idle';
            return (
              <button
                key={`${key}:preflight`}
                type="button"
                className="va-chip"
                disabled={providerPreflightBusy.length > 0}
                onClick={() => { void runPreflight(provider); }}
              >
                preflight {provider}: {state}
              </button>
            );
          })}
          <button
            type="button"
            className="va-chip"
            disabled={busyAction.length > 0 || !rollbackTarget || rollbackTarget === currentProvider}
            onClick={() => {
              void runAction(
                'provider.rollback',
                { channel, provider: rollbackTarget },
                {
                  confirmText: `Rollback ${channel.toUpperCase()} provider to "${rollbackTarget}"?`,
                  successMessage: `${channel.toUpperCase()} provider rolled back to ${rollbackTarget}.`,
                },
              );
            }}
          >
            rollback: {rollbackTarget || 'n/a'}
          </button>
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
          <span>Role: {sessionRole} ({sessionRoleSource})</span>
          <span>Settings button: {settingsStatusLabel}</span>
          <button
            type="button"
            onClick={() => setSettingsOpen((prev) => !prev)}
            disabled={loading}
          >
            {settingsOpen ? 'Close Settings' : 'Open Settings'}
          </button>
          <button type="button" onClick={handleRefresh} disabled={loading || busyAction.length > 0}>
            Refresh
          </button>
        </div>
      </header>

      {loading ? <p className="va-muted">Loading dashboard...</p> : null}
      {error ? <p className="va-error">{error}</p> : null}
      {notice ? <p className="va-notice">{notice}</p> : null}
      {settingsOpen ? (
        <section className="va-grid">
          <div className="va-card">
            <h3>Mini App Settings</h3>
            <p className="va-muted">
              Opened via Telegram Settings Button (`settings_button_pressed`) when supported.
            </p>
            <p>
              Live polling: <strong>{pollingPaused ? 'paused' : 'active'}</strong>
            </p>
            <p>
              API base URL: <strong>{API_BASE_URL || 'same-origin'}</strong>
            </p>
            <div className="va-inline-tools">
              <button
                type="button"
                onClick={() => setPollingPaused((prev) => !prev)}
              >
                {pollingPaused ? 'Resume Live Polling' : 'Pause Live Polling'}
              </button>
              <button type="button" onClick={handleRefresh}>
                Sync Now
              </button>
              <button
                type="button"
                onClick={resetSession}
                disabled={loading || busyAction.length > 0}
              >
                Retry Session
              </button>
              <button type="button" onClick={() => setSettingsOpen(false)}>
                Close Settings
              </button>
            </div>
          </div>
        </section>
      ) : null}
      {sessionBlocked ? (
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
              onClick={resetSession}
              disabled={loading || busyAction.length > 0}
            >
              Retry Session
            </button>
          </div>
        </section>
      ) : null}

      <section className="va-grid va-grid-hero">
        <div className="va-card va-hero">
          <div className="va-hero-top">
            <span className="va-kicker">Operations Wallet</span>
            <strong>{uptimeScore}%</strong>
            <p className="va-muted">
              Health score combines poll stability and bridge response quality.
            </p>
          </div>
          <div className="va-hero-stats">
            <article>
              <span>Session</span>
              <strong>{token ? 'Active' : 'Pending'}</strong>
            </article>
            <article>
              <span>Sync Mode</span>
              <strong>{syncModeLabel}</strong>
            </article>
            <article>
              <span>Open DLQ</span>
              <strong>{toInt(dlqPayload.call_open, callDlq.length) + toInt(dlqPayload.email_open, emailDlq.length)}</strong>
            </article>
            <article>
              <span>Data Feed</span>
              <strong>{hasBootstrapData ? 'Online' : 'Warming up'}</strong>
            </article>
          </div>
        </div>
      </section>

      <section className="va-module-nav">
        {visibleModules.map((module) => (
          <button
            key={module.id}
            type="button"
            className={`va-chip ${activeModule === module.id ? 'is-active' : ''}`}
            onClick={() => setActiveModule(module.id)}
          >
            {module.label}
          </button>
        ))}
      </section>
      {visibleModules.length === 0 ? (
        <p className="va-error">No dashboard modules are enabled for this role.</p>
      ) : null}

      {activeModule === 'ops' && hasCapability('dashboard_view') ? (
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
          <p>Last poll attempt: <strong>{lastPollLabel}</strong></p>
          <p>Last successful poll: <strong>{lastSuccessfulPollLabel}</strong></p>
          <p>Next poll scheduled: <strong>{nextPollLabel}</strong></p>
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

        <div className="va-card">
          <h3>Activity Timeline</h3>
          {activityLog.length === 0 ? (
            <p className="va-muted">No activity recorded yet.</p>
          ) : (
            <ul className="va-list va-list-activity">
              {activityLog.map((entry) => (
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
            {callLogs.slice(0, 10).map((row, index) => {
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
            {emailJobs.slice(0, 8).map((job, index) => {
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
            {callDlq.map((row) => {
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
            {emailDlq.map((row) => {
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
      ) : null}

      {activeModule === 'sms' && hasCapability('sms_bulk_manage') ? (
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
              <button
                type="button"
                disabled={busyAction.length > 0}
                onClick={() => { void sendSmsFromConsole(); }}
              >
                {smsScheduleAt ? 'Schedule SMS Batch' : 'Send SMS Batch'}
              </button>
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
      ) : null}

      {activeModule === 'mailer' && hasCapability('email_bulk_manage') ? (
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
            {emailJobs.length > 0 ? (
              <ul className="va-list">
                {emailJobs.slice(0, 6).map((job, index) => (
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
      ) : null}

      {activeModule === 'provider' && hasCapability('provider_manage') ? (
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
                  {providerMatrixRows.map((row) => (
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
            {renderProviderSection('call')}
            {renderProviderSection('sms')}
            {renderProviderSection('email')}
          </section>
        </>
      ) : null}

      {activeModule === 'content' && hasCapability('caller_flags_manage') ? (
        <section className="va-grid">
          <div className="va-card">
            <h3>Script & Persona Studio</h3>
            <p className="va-muted">
              Draft edits, persona/profile tuning, review approvals, and promote-live workflow.
            </p>
            <div className="va-inline-tools">
              <input
                className="va-input"
                placeholder="Flow filter (optional)"
                value={scriptFlowFilter}
                onChange={(event) => setScriptFlowFilter(event.target.value)}
              />
              <button
                type="button"
                onClick={() => { void refreshCallScriptsModule(); }}
              >
                Refresh Scripts
              </button>
            </div>
            <p className="va-muted">
              Scripts available: <strong>{callScriptsTotal}</strong>
            </p>
            <ul className="va-list">
              {callScripts.slice(0, 60).map((script) => {
                const scriptId = toInt(script.id);
                const lifecycle = toText(script.lifecycle_state, 'draft');
                const active = scriptId === selectedCallScriptId;
                return (
                  <li key={`call-script-${scriptId}`}>
                    <strong>{toText(script.name, `script-${scriptId || 'unknown'}`)}</strong>
                    <span>
                      ID: {scriptId} | Flow: {toText(script.flow_type, 'general')} | v{toInt(script.version, 1)}
                    </span>
                    <span>
                      Lifecycle: <strong>{lifecycle}</strong>
                      {' '}| Persona(default_profile): <strong>{toText(script.default_profile, 'general')}</strong>
                    </span>
                    <button
                      type="button"
                      className={active ? 'va-chip is-active' : 'va-chip'}
                      onClick={() => setSelectedCallScriptId(scriptId)}
                    >
                      {active ? 'Selected' : 'Select'}
                    </button>
                  </li>
                );
              })}
            </ul>
          </div>

          <div className="va-card">
            <h3>Draft Editor & Approval</h3>
            {selectedCallScript ? (
              <>
                <p>
                  Editing script: <strong>{toText(selectedCallScript.name, 'unknown')}</strong>
                  {' '}(# {selectedCallScriptId})
                </p>
                <p className="va-muted">
                  Lifecycle: <strong>{selectedCallScriptLifecycleState}</strong>
                  {' '}| Submitted: <strong>{formatTime(selectedCallScriptLifecycle.submitted_for_review_at)}</strong>
                  {' '}| Reviewed: <strong>{formatTime(selectedCallScriptLifecycle.reviewed_at)}</strong>
                </p>
                <input
                  className="va-input"
                  placeholder="Script name"
                  value={scriptNameInput}
                  onChange={(event) => setScriptNameInput(event.target.value)}
                />
                <input
                  className="va-input"
                  placeholder="Persona profile (default_profile)"
                  value={scriptDefaultProfileInput}
                  onChange={(event) => setScriptDefaultProfileInput(event.target.value)}
                />
                <textarea
                  className="va-input va-textarea"
                  placeholder="Description"
                  value={scriptDescriptionInput}
                  onChange={(event) => setScriptDescriptionInput(event.target.value)}
                  rows={2}
                />
                <textarea
                  className="va-input va-textarea"
                  placeholder="Prompt"
                  value={scriptPromptInput}
                  onChange={(event) => setScriptPromptInput(event.target.value)}
                  rows={6}
                />
                <textarea
                  className="va-input va-textarea"
                  placeholder="First message"
                  value={scriptFirstMessageInput}
                  onChange={(event) => setScriptFirstMessageInput(event.target.value)}
                  rows={3}
                />
                <input
                  className="va-input"
                  placeholder="Objective tags (comma-separated)"
                  value={scriptObjectiveTagsInput}
                  onChange={(event) => setScriptObjectiveTagsInput(event.target.value)}
                />
                <div className="va-inline-tools">
                  <button
                    type="button"
                    disabled={busyAction.length > 0}
                    onClick={() => { void saveCallScriptDraft(); }}
                  >
                    Save Draft
                  </button>
                  <button
                    type="button"
                    disabled={busyAction.length > 0}
                    onClick={() => { void submitCallScriptForReview(); }}
                  >
                    Submit Review
                  </button>
                </div>
                <textarea
                  className="va-input va-textarea"
                  placeholder="Review note / approval reason"
                  value={scriptReviewNoteInput}
                  onChange={(event) => setScriptReviewNoteInput(event.target.value)}
                  rows={2}
                />
                <div className="va-inline-tools">
                  <button
                    type="button"
                    disabled={busyAction.length > 0}
                    onClick={() => { void reviewCallScript('approve'); }}
                  >
                    Approve
                  </button>
                  <button
                    type="button"
                    disabled={busyAction.length > 0}
                    onClick={() => { void reviewCallScript('reject'); }}
                  >
                    Reject
                  </button>
                  <button
                    type="button"
                    disabled={busyAction.length > 0}
                    onClick={() => { void promoteCallScriptLive(); }}
                  >
                    Promote Live
                  </button>
                </div>
                <h3>Simulation</h3>
                <textarea
                  className="va-input va-textarea"
                  placeholder={'Variables JSON, e.g. {"customer_name":"Ada"}'}
                  value={scriptSimulationVariablesInput}
                  onChange={(event) => setScriptSimulationVariablesInput(event.target.value)}
                  rows={3}
                />
                <div className="va-inline-tools">
                  <button
                    type="button"
                    disabled={busyAction.length > 0}
                    onClick={() => { void simulateCallScript(); }}
                  >
                    Run Simulation
                  </button>
                </div>
                {scriptSimulationResult ? (
                  <pre>
                    {JSON.stringify(asRecord(scriptSimulationResult).simulation || scriptSimulationResult, null, 2)}
                  </pre>
                ) : null}
              </>
            ) : (
              <p className="va-muted">Select a script from the list to edit and review.</p>
            )}
          </div>
        </section>
      ) : null}

      {activeModule === 'users' && hasCapability('users_manage') ? (
        <section className="va-grid">
          <div className="va-card">
            <h3>User & Role Admin</h3>
            <div className="va-inline-tools">
              <input
                className="va-input"
                placeholder="Search Telegram ID"
                value={userSearch}
                onChange={(event) => setUserSearch(event.target.value)}
              />
              <select
                className="va-input"
                value={userSortBy}
                onChange={(event) => setUserSortBy(event.target.value)}
              >
                <option value="last_activity">Last Activity</option>
                <option value="total_calls">Total Calls</option>
                <option value="role">Role</option>
              </select>
              <select
                className="va-input"
                value={userSortDir}
                onChange={(event) => setUserSortDir(event.target.value)}
              >
                <option value="desc">DESC</option>
                <option value="asc">ASC</option>
              </select>
              <button type="button" onClick={() => { void refreshUsersModule(); }}>
                Refresh Users
              </button>
            </div>
            <p className="va-muted">
              Total users tracked: <strong>{toInt(usersPayload.total, usersRows.length)}</strong>
            </p>
            <ul className="va-list">
              {usersRows.slice(0, 80).map((user) => {
                const telegramId = toText(user.telegram_id, '');
                const role = toText(user.role, 'viewer');
                return (
                  <li key={`user-role-${telegramId}`}>
                    <strong>{telegramId || 'unknown'}</strong>
                    <span>Role: {role} ({toText(user.role_source, 'inferred')})</span>
                    <span>Calls: {toInt(user.total_calls)} | Failed: {toInt(user.failed_calls)}</span>
                    <span>Last activity: {formatTime(user.last_activity)}</span>
                    <div className="va-inline-tools">
                      <button type="button" onClick={() => { void handleApplyUserRole(telegramId, 'admin'); }}>
                        Promote Admin
                      </button>
                      <button type="button" onClick={() => { void handleApplyUserRole(telegramId, 'operator'); }}>
                        Set Operator
                      </button>
                      <button type="button" onClick={() => { void handleApplyUserRole(telegramId, 'viewer'); }}>
                        Demote Viewer
                      </button>
                    </div>
                  </li>
                );
              })}
            </ul>
          </div>
        </section>
      ) : null}

      {activeModule === 'audit' && hasCapability('dashboard_view') ? (
        <section className="va-grid">
          <div className="va-card">
            <h3>Audit & Incident Center</h3>
            <div className="va-inline-tools">
              <button type="button" onClick={() => { void refreshAuditModule(); }}>
                Refresh Alerts
              </button>
              <button
                type="button"
                disabled={!hasCapability('sms_bulk_manage')}
                onClick={() => { void runbookAction('runbook.sms.reconcile'); }}
              >
                Runbook: SMS Reconcile
              </button>
              <button
                type="button"
                disabled={!hasCapability('provider_manage')}
                onClick={() => { void runbookAction('runbook.payment.reconcile'); }}
              >
                Runbook: Payment Reconcile
              </button>
              <button
                type="button"
                disabled={!hasCapability('provider_manage')}
                onClick={() => { void runbookAction('runbook.provider.preflight'); }}
              >
                Runbook: Provider Preflight
              </button>
            </div>
            <p className="va-muted">
              Alert count: <strong>{toInt(incidentsPayload.total_alerts, incidentRows.length)}</strong>
            </p>
            <ul className="va-list">
              {incidentRows.slice(0, 30).map((incident, index) => (
                <li key={`incident-${index}`}>
                  <strong>{toText(incident.service_name, 'service')}</strong>
                  <span>Status: {toText(incident.status, 'unknown')}</span>
                  <span>{toText((asRecord(incident.details).message), toText(incident.details, ''))}</span>
                  <span>{formatTime(incident.timestamp)}</span>
                </li>
              ))}
            </ul>
            {runbookRows.length > 0 ? (
              <>
                <h3>Runbook Actions</h3>
                <ul className="va-list">
                  {runbookRows.map((runbook, index) => {
                    const action = toText(runbook.action, '');
                    const capability = toText(runbook.capability, 'dashboard_view');
                    return (
                      <li key={`runbook-${index}`}>
                        <strong>{toText(runbook.label, 'Runbook')}</strong>
                        <span>{action || 'unknown_action'}</span>
                        <button
                          type="button"
                          disabled={busyAction.length > 0 || !action || !hasCapability(capability)}
                          onClick={() => { void runbookAction(action, {}); }}
                        >
                          Execute
                        </button>
                      </li>
                    );
                  })}
                </ul>
              </>
            ) : null}
          </div>
          <div className="va-card">
            <h3>Immutable Activity Timeline</h3>
            <ul className="va-list">
              {auditRows.slice(0, 40).map((row, index) => (
                <li key={`audit-${index}`}>
                  <strong>{toText(row.service_name, 'service')}</strong>
                  <span>Status: {toText(row.status, 'unknown')}</span>
                  <span>{toText((asRecord(row.details).message), toText(row.details, ''))}</span>
                  <span>{formatTime(row.timestamp)}</span>
                </li>
              ))}
            </ul>
          </div>
        </section>
      ) : null}
    </main>
  );
}
