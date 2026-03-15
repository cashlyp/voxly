import { backButton, hapticFeedback, initData, settingsButton, useRawInitData, useSignal } from '@tma.js/sdk-react';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import '@/pages/AdminDashboard/AdminDashboardPage.css';
import { AuditIncidentsPage } from '@/pages/AdminDashboard/AuditIncidentsPage';
import { MailerPage } from '@/pages/AdminDashboard/MailerPage';
import { OpsDashboardPage } from '@/pages/AdminDashboard/OpsDashboardPage';
import { ProviderControlPage } from '@/pages/AdminDashboard/ProviderControlPage';
import { SettingsPage } from '@/pages/AdminDashboard/SettingsPage';
import { ScriptStudioPage } from '@/pages/AdminDashboard/ScriptStudioPage';
import { SmsSenderPage } from '@/pages/AdminDashboard/SmsSenderPage';
import { UsersRolePage } from '@/pages/AdminDashboard/UsersRolePage';
import { ErrorBoundary } from '@/components/ErrorBoundary';
import type { DashboardVm } from '@/pages/AdminDashboard/types';

const POLL_BASE_INTERVAL_MS = 10000;
const POLL_MAX_INTERVAL_MS = 60000;
const POLL_BACKOFF_MULTIPLIER = 1.7;
const POLL_JITTER_MS = 1200;
const POLL_DEGRADED_FAILURES = 2;
const SESSION_STORAGE_KEY = 'voxly-miniapp-session';
const MAX_ACTIVITY_ITEMS = 18;
const ACTION_REQUEST_TIMEOUT_MS = 15000;
const ACTION_LATENCY_SAMPLE_LIMIT = 40;
const STREAM_RECONNECT_BASE_MS = 2500;
const STREAM_RECONNECT_MAX_MS = 30000;
const STREAM_REFRESH_DEBOUNCE_MS = 350;
const SMS_DEFAULT_COST_PER_SEGMENT = 0.0075;
const API_BASE_URL = String(
  import.meta.env.VITE_API_BASE_URL || import.meta.env.VITE_API_BASE || '',
).trim().replace(/\/+$/, '');
const SESSION_REFRESH_RETRY_COUNT = 1;

type ProviderChannel = 'call' | 'sms' | 'email';

function moduleGlyph(moduleId: string): string {
  switch (moduleId) {
    case 'ops':
      return '◉';
    case 'sms':
      return '✉';
    case 'mailer':
      return '✦';
    case 'provider':
      return '⛭';
    case 'content':
      return '✎';
    case 'users':
      return '◎';
    case 'audit':
      return '⚑';
    default:
      return '•';
  }
}

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
  module_layout?: unknown;
  modules?: unknown;
  feature_flags?: unknown;
  flags?: unknown;
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
  module_layout?: unknown;
  modules?: unknown;
  feature_flags?: unknown;
  flags?: unknown;
  poll_interval_seconds?: unknown;
  poll_at?: unknown;
  server_time?: unknown;
}

type JsonObject = Record<string, unknown>;
type ActivityStatus = 'info' | 'success' | 'error';
type DashboardModule = 'ops' | 'sms' | 'mailer' | 'provider' | 'content' | 'users' | 'audit';
type ModuleConfig = {
  enabled: boolean;
  order: number | null;
  label: string | null;
};
type StreamConnectionMode = 'disabled' | 'connecting' | 'connected' | 'fallback';
type ActionRequestMeta = {
  action_id: string;
  request_id: string;
  idempotency_key: string;
  requested_at: string;
  request_timeout_ms: number;
  source: string;
  ui_module: DashboardModule;
};
type FeatureFlags = Record<string, boolean>;
type FeatureFlagRegistryEntry = {
  key: string;
  defaultEnabled: boolean;
  description: string;
};
type FeatureFlagInspectorItem = {
  key: string;
  enabled: boolean;
  source: 'server' | 'default';
  defaultEnabled: boolean;
  description: string;
};
type FeatureFlagsPayloadSource =
  | 'poll.feature_flags'
  | 'poll.dashboard.feature_flags'
  | 'bootstrap.feature_flags'
  | 'dashboard.feature_flags'
  | 'poll.flags'
  | 'poll.dashboard.flags'
  | 'bootstrap.flags'
  | 'dashboard.flags'
  | 'default';
type ProviderSwitchStage = 'idle' | 'simulated' | 'confirmed' | 'applied' | 'failed';
type ProviderSwitchPostCheck = 'idle' | 'ok' | 'failed';
type ProviderSwitchPlanState = {
  target: string;
  stage: ProviderSwitchStage;
  postCheck: ProviderSwitchPostCheck;
  rollbackSuggestion: string;
};

const MODULE_DEFINITIONS: Array<{ id: DashboardModule; label: string; capability: string }> = [
  { id: 'ops', label: 'Ops Dashboard', capability: 'dashboard_view' },
  { id: 'sms', label: 'SMS Sender', capability: 'sms_bulk_manage' },
  { id: 'mailer', label: 'Mailer Console', capability: 'email_bulk_manage' },
  { id: 'provider', label: 'Provider Control', capability: 'provider_manage' },
  { id: 'content', label: 'Script Studio', capability: 'caller_flags_manage' },
  { id: 'users', label: 'User & Role Admin', capability: 'users_manage' },
  { id: 'audit', label: 'Audit & Incidents', capability: 'dashboard_view' },
];

const MODULE_CONTEXT: Record<DashboardModule, { subtitle: string; detail: string }> = {
  ops: {
    subtitle: 'Operational health, runtime posture, and queue visibility.',
    detail: 'Control plane overview for live operations.',
  },
  sms: {
    subtitle: 'Bulk SMS console for recipients, scheduling, and delivery posture.',
    detail: 'Outbound messaging pipeline.',
  },
  mailer: {
    subtitle: 'Email audience delivery, template variables, and deliverability health.',
    detail: 'Mailer orchestration workspace.',
  },
  provider: {
    subtitle: 'Preflight, provider switching, and rollback safety controls.',
    detail: 'Provider reliability and failover.',
  },
  content: {
    subtitle: 'Call script drafting, review lifecycle, and simulation controls.',
    detail: 'Conversation quality studio.',
  },
  users: {
    subtitle: 'Role assignments, user oversight, and access governance.',
    detail: 'Access and permissions console.',
  },
  audit: {
    subtitle: 'Incident timeline, runbook actions, and immutable audit feed.',
    detail: 'Governance and incident response.',
  },
};
const MODULE_ID_SET = new Set<DashboardModule>(MODULE_DEFINITIONS.map((module) => module.id));
const MODULE_DEFAULT_ORDER: Record<DashboardModule, number> = {
  ops: 0,
  sms: 1,
  mailer: 2,
  provider: 3,
  content: 4,
  users: 5,
  audit: 6,
};
const FEATURE_FLAG_REGISTRY: FeatureFlagRegistryEntry[] = [
  {
    key: 'realtime_stream',
    defaultEnabled: true,
    description: 'Use live stream updates before falling back to polling.',
  },
  {
    key: 'module_skeletons',
    defaultEnabled: true,
    description: 'Render loading skeleton cards while module data hydrates.',
  },
  {
    key: 'module_error_boundaries',
    defaultEnabled: true,
    description: 'Isolate module rendering failures with recovery cards.',
  },
  {
    key: 'runtime_controls',
    defaultEnabled: true,
    description: 'Show runtime maintenance and canary controls.',
  },
  {
    key: 'provider_cards',
    defaultEnabled: true,
    description: 'Expose provider readiness and channel cards in Ops.',
  },
  {
    key: 'advanced_tables',
    defaultEnabled: true,
    description: 'Enable search, filters, and pagination in admin tables.',
  },
  {
    key: 'users_csv_export',
    defaultEnabled: true,
    description: 'Allow CSV export for user and role administration.',
  },
  {
    key: 'runbook_actions',
    defaultEnabled: true,
    description: 'Enable incident runbook quick actions.',
  },
  {
    key: 'incidents_csv_export',
    defaultEnabled: true,
    description: 'Allow CSV export for incident datasets.',
  },
  {
    key: 'audit_csv_export',
    defaultEnabled: true,
    description: 'Allow CSV export for audit timeline records.',
  },
];
const FEATURE_FLAG_REGISTRY_KEYS = new Set<string>(
  FEATURE_FLAG_REGISTRY.map((entry) => entry.key),
);
const USER_ROLE_REASON_TEMPLATES = [
  'Policy update',
  'On-call rotation',
  'Temporary incident response',
  'Compliance request',
  'Access cleanup',
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

function computePercentile(values: number[], percentile: number): number {
  if (!Array.isArray(values) || values.length === 0) return 0;
  const sorted = [...values]
    .map((value) => Number(value))
    .filter((value) => Number.isFinite(value) && value >= 0)
    .sort((a, b) => a - b);
  if (sorted.length === 0) return 0;
  const safePercentile = Math.max(0, Math.min(100, percentile));
  const position = (safePercentile / 100) * (sorted.length - 1);
  const lower = Math.floor(position);
  const upper = Math.ceil(position);
  if (lower === upper) return Math.round(sorted[lower]);
  const weight = position - lower;
  return Math.round((sorted[lower] * (1 - weight)) + (sorted[upper] * weight));
}

function renderTemplateString(template: string, variables: Record<string, unknown>): string {
  if (!template) return '';
  return String(template).replace(/\{\{\s*([a-zA-Z0-9_.-]+)\s*\}\}/g, (_, key: string) => {
    const raw = variables[key];
    if (raw === undefined || raw === null) return `{{${key}}}`;
    if (typeof raw === 'string') return raw;
    if (typeof raw === 'number' || typeof raw === 'boolean' || typeof raw === 'bigint') {
      return String(raw);
    }
    try {
      return JSON.stringify(raw);
    } catch {
      return '[unrenderable]';
    }
  });
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

function parseFlagValue(value: unknown): boolean | null {
  if (typeof value === 'boolean') return value;
  if (typeof value === 'number') {
    if (value === 1) return true;
    if (value === 0) return false;
    return null;
  }
  if (typeof value === 'string') {
    const normalized = value.trim().toLowerCase();
    if (['1', 'true', 'yes', 'on', 'enabled'].includes(normalized)) return true;
    if (['0', 'false', 'no', 'off', 'disabled'].includes(normalized)) return false;
  }
  return null;
}

function parseFeatureFlags(value: unknown): FeatureFlags {
  const flags: FeatureFlags = {};
  if (Array.isArray(value)) {
    value.forEach((entry) => {
      if (typeof entry === 'string' && entry.trim()) {
        flags[entry.trim().toLowerCase()] = true;
        return;
      }
      const record = asRecord(entry);
      const key = toText(record.key ?? record.name ?? record.flag, '').trim().toLowerCase();
      if (!key) return;
      const parsed = parseFlagValue(record.enabled ?? record.value ?? true);
      if (parsed === null) return;
      flags[key] = parsed;
    });
    return flags;
  }
  const record = asRecord(value);
  Object.entries(record).forEach(([key, raw]) => {
    const normalized = String(key || '').trim().toLowerCase();
    if (!normalized) return;
    const parsed = parseFlagValue(raw);
    if (parsed === null) return;
    flags[normalized] = parsed;
  });
  return flags;
}

function parseDashboardModuleId(value: unknown): DashboardModule | null {
  if (typeof value !== 'string') return null;
  const normalized = value.trim().toLowerCase();
  if (MODULE_ID_SET.has(normalized as DashboardModule)) {
    return normalized as DashboardModule;
  }
  return null;
}

function parseModuleLayoutConfig(layout: unknown): Partial<Record<DashboardModule, ModuleConfig>> {
  const root = asRecord(layout);
  const candidates = root.modules ?? layout;
  const rules: Partial<Record<DashboardModule, ModuleConfig>> = {};
  const applyRule = (entry: unknown, keyHint?: string): void => {
    if (typeof entry === 'string') {
      const id = parseDashboardModuleId(entry) || parseDashboardModuleId(keyHint);
      if (!id) return;
      rules[id] = { enabled: true, order: null, label: null };
      return;
    }
    if (typeof entry === 'boolean') {
      const id = parseDashboardModuleId(keyHint);
      if (!id) return;
      rules[id] = { enabled: entry, order: null, label: null };
      return;
    }
    if (typeof entry === 'number') {
      const id = parseDashboardModuleId(keyHint);
      if (!id) return;
      rules[id] = {
        enabled: true,
        order: Number.isFinite(entry) ? Math.floor(entry) : null,
        label: null,
      };
      return;
    }
    const record = asRecord(entry);
    const id = parseDashboardModuleId(record.id ?? record.module ?? record.key ?? keyHint);
    if (!id) return;
    const hidden = record.hidden === true || record.disabled === true;
    const enabled = hidden ? false : record.enabled !== false;
    const orderRaw = Number(record.order);
    const order = Number.isFinite(orderRaw) ? Math.floor(orderRaw) : null;
    const label = typeof record.label === 'string' && record.label.trim() ? record.label.trim() : null;
    rules[id] = { enabled, order, label };
  };

  if (Array.isArray(candidates)) {
    candidates.forEach((entry) => applyRule(entry));
    return rules;
  }
  if (candidates && typeof candidates === 'object') {
    Object.entries(asRecord(candidates)).forEach(([key, value]) => {
      applyRule(value, key);
    });
  }
  return rules;
}

function createActionRequestMeta(action: string, moduleId: DashboardModule): ActionRequestMeta {
  const nonce = Math.random().toString(36).slice(2, 10);
  const ts = Date.now();
  const actionId = `${action}:${ts}:${nonce}`;
  return {
    action_id: actionId,
    request_id: actionId,
    idempotency_key: actionId,
    requested_at: new Date(ts).toISOString(),
    request_timeout_ms: ACTION_REQUEST_TIMEOUT_MS,
    source: 'miniapp_admin_console',
    ui_module: moduleId,
  };
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

function buildEventStreamUrl(path: string, token: string): string {
  const base = buildApiUrl(path);
  const separator = base.includes('?') ? '&' : '?';
  const encodedToken = encodeURIComponent(token);
  return `${base}${separator}token=${encodedToken}&session_token=${encodedToken}&transport=sse`;
}

export function AdminDashboardPage() {
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
  const [streamMode, setStreamMode] = useState<StreamConnectionMode>('disabled');
  const [streamConnected, setStreamConnected] = useState<boolean>(false);
  const [streamFailureCount, setStreamFailureCount] = useState<number>(0);
  const [streamLastEventAt, setStreamLastEventAt] = useState<number | null>(null);
  const [lastPollAt, setLastPollAt] = useState<number | null>(null);
  const [lastSuccessfulPollAt, setLastSuccessfulPollAt] = useState<number | null>(null);
  const [nextPollAt, setNextPollAt] = useState<number | null>(null);
  const [sessionBlocked, setSessionBlocked] = useState<boolean>(false);
  const [activityLog, setActivityLog] = useState<ActivityEntry[]>([]);
  const [actionLatencyMsSamples, setActionLatencyMsSamples] = useState<number[]>([]);
  const [activeModule, setActiveModule] = useState<DashboardModule>('ops');
  const [settingsOpen, setSettingsOpen] = useState<boolean>(false);
  const [pollingPaused, setPollingPaused] = useState<boolean>(false);
  const [smsRecipientsInput, setSmsRecipientsInput] = useState<string>('');
  const [smsMessageInput, setSmsMessageInput] = useState<string>('');
  const [smsScheduleAt, setSmsScheduleAt] = useState<string>('');
  const [smsProviderInput, setSmsProviderInput] = useState<string>('');
  const [smsCostPerSegment, setSmsCostPerSegment] = useState<string>(String(SMS_DEFAULT_COST_PER_SEGMENT));
  const [smsDryRunMode, setSmsDryRunMode] = useState<boolean>(false);
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
  const [providerSwitchPlanByChannel, setProviderSwitchPlanByChannel] = useState<
    Record<ProviderChannel, ProviderSwitchPlanState>
  >({
    call: { target: '', stage: 'idle', postCheck: 'idle', rollbackSuggestion: '' },
    sms: { target: '', stage: 'idle', postCheck: 'idle', rollbackSuggestion: '' },
    email: { target: '', stage: 'idle', postCheck: 'idle', rollbackSuggestion: '' },
  });
  const [userSearch, setUserSearch] = useState<string>('');
  const [userSortBy, setUserSortBy] = useState<string>('last_activity');
  const [userSortDir, setUserSortDir] = useState<string>('desc');
  const [usersSnapshot, setUsersSnapshot] = useState<MiniAppUsersPayload | null>(null);
  const [auditSnapshot, setAuditSnapshot] = useState<MiniAppAuditPayload | null>(null);
  const [incidentsSnapshot, setIncidentsSnapshot] = useState<MiniAppIncidentsPayload | null>(null);
  const sessionRequestRef = useRef<Promise<string> | null>(null);
  const pollFailureNotedRef = useRef<boolean>(false);
  const initialServerModuleAppliedRef = useRef<boolean>(false);
  const streamRefreshTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const triggerHaptic = useCallback((
    mode: 'selection' | 'impact' | 'success' | 'warning' | 'error',
    impactStyle: 'light' | 'medium' | 'heavy' | 'rigid' | 'soft' = 'light',
  ): void => {
    const api = hapticFeedback as unknown as {
      isSupported?: (() => boolean) | boolean;
      selectionChanged?: (() => void) | { ifAvailable?: () => void };
      impactOccurred?: ((style: 'light' | 'medium' | 'heavy' | 'rigid' | 'soft') => void) | {
        ifAvailable?: (style: 'light' | 'medium' | 'heavy' | 'rigid' | 'soft') => void;
      };
      notificationOccurred?: ((state: 'success' | 'warning' | 'error') => void) | {
        ifAvailable?: (state: 'success' | 'warning' | 'error') => void;
      };
    };
    try {
      const supported = typeof api.isSupported === 'function'
        ? Boolean(api.isSupported())
        : api.isSupported !== false;
      if (!supported) return;
      if (mode === 'selection') {
        if (typeof api.selectionChanged === 'function') {
          api.selectionChanged();
          return;
        }
        api.selectionChanged?.ifAvailable?.();
        return;
      }
      if (mode === 'impact') {
        if (typeof api.impactOccurred === 'function') {
          api.impactOccurred(impactStyle);
          return;
        }
        api.impactOccurred?.ifAvailable?.(impactStyle);
        return;
      }
      if (typeof api.notificationOccurred === 'function') {
        api.notificationOccurred(mode);
        return;
      }
      api.notificationOccurred?.ifAvailable?.(mode);
    } catch {
      // Ignore haptic errors to avoid blocking control-path actions.
    }
  }, []);

  const toggleSettings = useCallback((next?: boolean): void => {
    setSettingsOpen((prev) => {
      const target = typeof next === 'boolean' ? next : !prev;
      if (target !== prev) {
        triggerHaptic('selection');
      }
      return target;
    });
  }, [triggerHaptic]);

  const selectModule = useCallback((moduleId: DashboardModule): void => {
    setActiveModule((prev) => {
      if (prev === moduleId) return prev;
      triggerHaptic('selection');
      return moduleId;
    });
  }, [triggerHaptic]);

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
    return (payload ?? {}) as T;
  }, [createSession, pushActivity, token]);

  const loadBootstrap = useCallback(async () => {
    setLoading(true);
    setError('');
    try {
      const payload = asRecord(
        await request<DashboardApiPayload | null>('/miniapp/bootstrap'),
      ) as DashboardApiPayload;
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
      const payload = asRecord(
        await request<DashboardApiPayload | null>('/miniapp/jobs/poll'),
      ) as DashboardApiPayload;
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

  const applyStreamPayload = useCallback((raw: unknown): boolean => {
    const envelope = asRecord(raw);
    const candidate = asRecord(envelope.payload ?? envelope.data ?? raw);
    if (Object.keys(candidate).length === 0) return false;
    const nextPayload = candidate as DashboardApiPayload;
    setPollPayload((prev) => ({
      ...(asRecord(prev) as DashboardApiPayload),
      ...nextPayload,
    }));
    setLastSuccessfulPollAt(Date.now());
    setPollFailureCount(0);
    setError('');

    const dashboardFromPayload = nextPayload.dashboard;
    const usersFromPayload = nextPayload.users || dashboardFromPayload?.users;
    const auditFromPayload = nextPayload.audit || dashboardFromPayload?.audit;
    const incidentsFromPayload = nextPayload.incidents || dashboardFromPayload?.incidents;
    if (usersFromPayload) setUsersSnapshot(usersFromPayload);
    if (auditFromPayload) setAuditSnapshot(auditFromPayload);
    if (incidentsFromPayload) setIncidentsSnapshot(incidentsFromPayload);
    return true;
  }, []);

  const scheduleStreamRefresh = useCallback((): void => {
    if (streamRefreshTimerRef.current) return;
    streamRefreshTimerRef.current = setTimeout(() => {
      streamRefreshTimerRef.current = null;
      void loadPoll();
    }, STREAM_REFRESH_DEBOUNCE_MS);
  }, [loadPoll]);

  const invokeAction = useCallback(async (
    action: string,
    payload: Record<string, unknown>,
    metaOverride?: Partial<ActionRequestMeta>,
  ): Promise<unknown> => {
    const actionMeta: ActionRequestMeta = {
      ...createActionRequestMeta(action, activeModule),
      ...metaOverride,
    };
    const abortController = new AbortController();
    const timeout = setTimeout(() => abortController.abort(), actionMeta.request_timeout_ms);
    try {
      const rawResult = await request<{ success?: boolean; data?: unknown; error?: string } | null>('/miniapp/action', {
        method: 'POST',
        signal: abortController.signal,
        headers: {
          'X-Action-Id': actionMeta.action_id,
          'X-Idempotency-Key': actionMeta.idempotency_key,
        },
        body: JSON.stringify({ action, payload, meta: actionMeta }),
      });
      const result = asRecord(rawResult);
      if (result.success !== true) {
        throw new Error(toText(result.error, 'Action failed'));
      }
      return result.data;
    } catch (err) {
      const isAbortError = err instanceof DOMException && err.name === 'AbortError';
      if (isAbortError) {
        throw new Error(`Action timed out after ${actionMeta.request_timeout_ms}ms`);
      }
      throw err;
    } finally {
      clearTimeout(timeout);
    }
  }, [activeModule, request]);

  const runAction = useCallback(async (
    action: string,
    payload: Record<string, unknown>,
    options: { confirmText?: string; successMessage?: string } = {},
  ) => {
    const actionMeta = createActionRequestMeta(action, activeModule);
    const traceHint = actionMeta.action_id.slice(-8);
    if (options.confirmText && typeof window !== 'undefined') {
      const allowed = window.confirm(options.confirmText);
      if (!allowed) {
        triggerHaptic('warning');
        pushActivity('info', 'Action cancelled', `Cancelled: ${action} (trace:${traceHint})`);
        return;
      }
    }
    setBusyAction(action);
    setNotice('');
    setError('');
    const startedAt = Date.now();
    try {
      await invokeAction(action, payload, actionMeta);
      const successMessage = options.successMessage || `Action completed: ${action}`;
      setNotice(`${successMessage} [${traceHint}]`);
      triggerHaptic('success');
      pushActivity('success', 'Action completed', `${successMessage} (trace:${traceHint})`);
      await loadBootstrap();
    } catch (err) {
      const detail = err instanceof Error ? err.message : String(err);
      setError(detail);
      triggerHaptic('error');
      pushActivity('error', `Action failed: ${action}`, `${detail} (trace:${traceHint})`);
    } finally {
      const latencyMs = Math.max(0, Date.now() - startedAt);
      setActionLatencyMsSamples((prev) => [...prev, latencyMs].slice(-ACTION_LATENCY_SAMPLE_LIMIT));
      setBusyAction('');
    }
  }, [activeModule, invokeAction, loadBootstrap, pushActivity, triggerHaptic]);
  const featureFlagsResolution = useMemo<{
    payload: unknown;
    source: FeatureFlagsPayloadSource;
  }>(() => {
    const candidates: Array<{ source: FeatureFlagsPayloadSource; payload: unknown }> = [
      { source: 'poll.feature_flags', payload: pollPayload?.feature_flags },
      { source: 'poll.dashboard.feature_flags', payload: pollPayload?.dashboard?.feature_flags },
      { source: 'bootstrap.feature_flags', payload: bootstrap?.feature_flags },
      { source: 'dashboard.feature_flags', payload: bootstrap?.dashboard?.feature_flags },
      { source: 'poll.flags', payload: pollPayload?.flags },
      { source: 'poll.dashboard.flags', payload: pollPayload?.dashboard?.flags },
      { source: 'bootstrap.flags', payload: bootstrap?.flags },
      { source: 'dashboard.flags', payload: bootstrap?.dashboard?.flags },
    ];
    const resolved = candidates.find((entry) => entry.payload !== undefined && entry.payload !== null);
    if (!resolved) {
      return {
        payload: {},
        source: 'default',
      };
    }
    return resolved;
  }, [
    bootstrap?.dashboard?.feature_flags,
    bootstrap?.dashboard?.flags,
    bootstrap?.feature_flags,
    bootstrap?.flags,
    pollPayload?.dashboard?.feature_flags,
    pollPayload?.dashboard?.flags,
    pollPayload?.feature_flags,
    pollPayload?.flags,
  ]);
  const featureFlags = useMemo(
    () => parseFeatureFlags(featureFlagsResolution.payload),
    [featureFlagsResolution.payload],
  );
  const featureFlagsSourceLabel = featureFlagsResolution.source;
  const featureFlagsUpdatedAtRaw = pollPayload?.poll_at
    || pollPayload?.server_time
    || asRecord(pollPayload?.dashboard).poll_at
    || asRecord(pollPayload?.dashboard).server_time
    || bootstrap?.poll_at
    || bootstrap?.server_time
    || asRecord(bootstrap?.dashboard).poll_at
    || asRecord(bootstrap?.dashboard).server_time
    || null;
  const featureFlagsUpdatedAtLabel = featureFlagsUpdatedAtRaw
    ? formatTime(featureFlagsUpdatedAtRaw)
    : '—';
  const isFeatureEnabled = useCallback((flag: string, fallback = true): boolean => {
    const normalized = String(flag || '').trim().toLowerCase();
    if (!normalized) return fallback;
    if (!(normalized in featureFlags)) return fallback;
    return featureFlags[normalized];
  }, [featureFlags]);
  const featureFlagInspectorItems = useMemo<FeatureFlagInspectorItem[]>(() => {
    const known = FEATURE_FLAG_REGISTRY.map((entry) => {
      const hasOverride = Object.prototype.hasOwnProperty.call(featureFlags, entry.key);
      const enabled = hasOverride ? featureFlags[entry.key] : entry.defaultEnabled;
      return {
        key: entry.key,
        enabled,
        source: hasOverride ? ('server' as const) : ('default' as const),
        defaultEnabled: entry.defaultEnabled,
        description: entry.description,
      };
    });
    const dynamic = Object.entries(featureFlags)
      .filter(([key]) => !FEATURE_FLAG_REGISTRY_KEYS.has(key))
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([key, enabled]) => ({
        key,
        enabled,
        source: 'server' as const,
        defaultEnabled: Boolean(enabled),
        description: 'Server-defined feature flag.',
      }));
    return [...known, ...dynamic];
  }, [featureFlags]);
  const realtimeStreamEnabled = isFeatureEnabled('realtime_stream', true);
  const moduleSkeletonsEnabled = isFeatureEnabled('module_skeletons', true);
  const moduleErrorBoundariesEnabled = isFeatureEnabled('module_error_boundaries', true);

  useEffect(() => () => {
    if (streamRefreshTimerRef.current) {
      clearTimeout(streamRefreshTimerRef.current);
      streamRefreshTimerRef.current = null;
    }
  }, []);

  useEffect(() => {
    void loadBootstrap();
  }, [loadBootstrap]);

  useEffect(() => {
    if (!realtimeStreamEnabled || !token || sessionBlocked || pollingPaused) {
      setStreamMode('disabled');
      setStreamConnected(false);
      return undefined;
    }
    if (typeof EventSource === 'undefined') {
      setStreamMode('fallback');
      setStreamConnected(false);
      return undefined;
    }

    let disposed = false;
    let reconnectTimer: ReturnType<typeof setTimeout> | null = null;
    let activeStream: EventSource | null = null;
    let attemptCount = 0;
    let endpointIndex = 0;
    const endpoints = ['/miniapp/events', '/miniapp/stream'];

    const closeStream = () => {
      if (activeStream) {
        activeStream.close();
        activeStream = null;
      }
    };

    const connect = () => {
      if (disposed) return;
      setStreamMode('connecting');
      const nextEndpoint = endpoints[Math.max(0, Math.min(endpointIndex, endpoints.length - 1))];
      const streamUrl = buildEventStreamUrl(nextEndpoint, token);
      const source = new EventSource(streamUrl);
      activeStream = source;

      source.onopen = () => {
        if (disposed) return;
        attemptCount = 0;
        setStreamMode('connected');
        setStreamConnected(true);
        setStreamFailureCount(0);
      };

      source.onmessage = (event) => {
        if (disposed) return;
        setStreamLastEventAt(Date.now());
        const eventText = typeof event.data === 'string' ? event.data : String(event.data ?? '');
        let parsed: unknown = eventText;
        try {
          parsed = JSON.parse(eventText);
        } catch {
          // Accept plain text event frames and trigger poll refresh below.
        }
        const applied = applyStreamPayload(parsed);
        if (!applied) {
          scheduleStreamRefresh();
        }
      };

      source.onerror = () => {
        if (disposed) return;
        closeStream();
        setStreamConnected(false);
        if (endpointIndex < endpoints.length - 1) {
          endpointIndex += 1;
          connect();
          return;
        }
        endpointIndex = 0;
        attemptCount += 1;
        setStreamMode('fallback');
        setStreamFailureCount((prev) => prev + 1);
        const delay = Math.min(
          STREAM_RECONNECT_MAX_MS,
          Math.round(STREAM_RECONNECT_BASE_MS * Math.pow(1.5, Math.max(0, attemptCount - 1))),
        );
        reconnectTimer = setTimeout(() => {
          reconnectTimer = null;
          connect();
        }, delay);
      };
    };

    connect();
    return () => {
      disposed = true;
      closeStream();
      if (reconnectTimer) {
        clearTimeout(reconnectTimer);
      }
    };
  }, [applyStreamPayload, pollingPaused, realtimeStreamEnabled, scheduleStreamRefresh, sessionBlocked, token]);

  useEffect(() => {
    if (!settingsButton.onClick.isAvailable()) {
      return undefined;
    }
    return settingsButton.onClick(() => {
      toggleSettings();
    });
  }, [toggleSettings]);

  useEffect(() => {
    if (settingsOpen || activeModule !== 'ops') {
      backButton.show.ifAvailable();
      return;
    }
    backButton.hide.ifAvailable();
  }, [activeModule, settingsOpen]);

  useEffect(() => {
    if (!backButton.onClick.isAvailable()) {
      return undefined;
    }
    return backButton.onClick(() => {
      if (settingsOpen) {
        toggleSettings(false);
        return;
      }
      if (activeModule !== 'ops') {
        selectModule('ops');
        return;
      }
      if (typeof window !== 'undefined' && window.history.length > 1) {
        window.history.back();
      }
    });
  }, [activeModule, selectModule, settingsOpen, toggleSettings]);

  const serverPollIntervalMs = useMemo(() => {
    const intervalSeconds = Number(bootstrap?.poll_interval_seconds);
    if (!Number.isFinite(intervalSeconds) || intervalSeconds <= 0) {
      return POLL_BASE_INTERVAL_MS;
    }
    return Math.max(3000, Math.min(POLL_MAX_INTERVAL_MS, Math.floor(intervalSeconds * 1000)));
  }, [bootstrap?.poll_interval_seconds]);

  useEffect(() => {
    if (!token || sessionBlocked || pollingPaused || streamConnected) return undefined;
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
  }, [loadPoll, pollingPaused, serverPollIntervalMs, sessionBlocked, streamConnected, token]);

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
  const dashboardLayoutPayload = pollPayload?.module_layout
    || bootstrap?.module_layout
    || dashboard?.module_layout
    || pollPayload?.modules
    || bootstrap?.modules
    || dashboard?.modules
    || {};
  const moduleLayoutConfig = parseModuleLayoutConfig(dashboardLayoutPayload);
  const roleAllowedModules = MODULE_DEFINITIONS.filter((module) => hasCapability(module.capability));
  const visibleModules = roleAllowedModules
    .filter((module) => moduleLayoutConfig[module.id]?.enabled !== false)
    .map((module) => ({
      ...module,
      label: moduleLayoutConfig[module.id]?.label || module.label,
    }))
    .sort((a, b) => {
      const orderA = moduleLayoutConfig[a.id]?.order;
      const orderB = moduleLayoutConfig[b.id]?.order;
      const safeA = Number.isFinite(orderA) ? Number(orderA) : MODULE_DEFAULT_ORDER[a.id];
      const safeB = Number.isFinite(orderB) ? Number(orderB) : MODULE_DEFAULT_ORDER[b.id];
      return safeA - safeB;
    });
  const preferredServerModule = parseDashboardModuleId(
    asRecord(dashboardLayoutPayload).active_module || asRecord(dashboardLayoutPayload).default_module,
  );
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
    if (initialServerModuleAppliedRef.current) return;
    if (!preferredServerModule) return;
    if (!visibleModules.some((module) => module.id === preferredServerModule)) {
      initialServerModuleAppliedRef.current = true;
      return;
    }
    initialServerModuleAppliedRef.current = true;
    setActiveModule(preferredServerModule);
  }, [preferredServerModule, visibleModules]);
  const activeModuleMeta = MODULE_CONTEXT[activeModule] || MODULE_CONTEXT.ops;

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
  const streamModeLabel = streamMode === 'connected'
    ? 'Realtime'
    : streamMode === 'connecting'
      ? 'Realtime (connecting)'
      : streamMode === 'fallback'
        ? 'Polling fallback'
        : 'Disabled';
  const streamLastEventLabel = streamLastEventAt
    ? formatTime(new Date(streamLastEventAt).toISOString())
    : '—';
  const syncModeLabel = pollingPaused
    ? 'Paused'
    : streamConnected
      ? isDashboardDegraded
        ? 'Realtime (degraded)'
        : 'Realtime'
    : isDashboardDegraded
      ? 'Degraded'
      : 'Healthy';
  const nextPollLabel = pollingPaused || streamConnected
    ? 'Paused'
    : nextPollAt
      ? formatTime(new Date(nextPollAt).toISOString())
      : '—';
  const lastPollLabel = lastPollAt ? formatTime(new Date(lastPollAt).toISOString()) : '—';
  const lastSuccessfulPollLabel = lastSuccessfulPollAt
    ? formatTime(new Date(lastSuccessfulPollAt).toISOString())
    : '—';
  const pollFreshnessSeconds = lastSuccessfulPollAt
    ? Math.max(0, Math.floor((Date.now() - lastSuccessfulPollAt) / 1000))
    : -1;
  const pollFreshnessLabel = pollFreshnessSeconds < 0 ? 'No successful poll yet' : `${pollFreshnessSeconds}s`;
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
  const sloErrorBudgetPercent = Math.max(
    0,
    Math.min(
      100,
      Math.round(100 - callFailureRate - (pollFailureCount * 2.5) - (bridgeHardFailures * 4)),
    ),
  );
  const sloP95ActionLatencyMs = computePercentile(actionLatencyMsSamples, 95);
  const degradedCauses = [
    sessionBlocked ? 'Session auth blocked' : '',
    pollFailureCount >= POLL_DEGRADED_FAILURES ? `Poll failures ${pollFailureCount}` : '',
    bridgeHardFailures > 0 ? `Bridge hard failures ${bridgeHardFailures}` : '',
    bridgeSoftFailures > 0 ? `Bridge soft failures ${bridgeSoftFailures}` : '',
    (Boolean(error) && !hasBootstrapData) ? 'Bootstrap data unavailable' : '',
  ].filter(Boolean);
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
  const providerCurrentByChannel: Record<ProviderChannel, string> = {
    call: toText(asRecord(providersByChannel.call).provider, '').toLowerCase(),
    sms: toText(asRecord(providersByChannel.sms).provider, '').toLowerCase(),
    email: toText(asRecord(providersByChannel.email).provider, '').toLowerCase(),
  };
  const providerSupportedByChannel: Record<ProviderChannel, string[]> = {
    call: asStringList(asRecord(providersByChannel.call).supported_providers),
    sms: asStringList(asRecord(providersByChannel.sms).supported_providers),
    email: asStringList(asRecord(providersByChannel.email).supported_providers),
  };
  useEffect(() => {
    setProviderSwitchPlanByChannel((prev) => {
      let changed = false;
      const next = { ...prev };
      (['call', 'sms', 'email'] as ProviderChannel[]).forEach((channel) => {
        const current = providerCurrentByChannel[channel];
        const supported = providerSupportedByChannel[channel];
        const fallbackTarget = current || supported[0] || '';
        if (!fallbackTarget || prev[channel].target) return;
        changed = true;
        next[channel] = {
          ...prev[channel],
          target: fallbackTarget,
        };
      });
      return changed ? next : prev;
    });
  }, [providerCurrentByChannel, providerSupportedByChannel]);
  const smsRecipientsParsed = parsePhoneList(smsRecipientsInput);
  const smsInvalidRecipients = smsRecipientsParsed.filter((phone) => !isValidE164(phone));
  const smsDuplicateCount = Math.max(
    0,
    String(smsRecipientsInput || '')
      .split(/[\n,;\t ]+/g)
      .filter(Boolean).length - smsRecipientsParsed.length,
  );
  const smsSegmentEstimate = estimateSmsSegments(smsMessageInput);
  const smsValidRecipients = smsRecipientsParsed.length - smsInvalidRecipients.length;
  const smsLikelyLandlineRecipients = smsRecipientsParsed.filter((phone) => {
    const digits = phone.replace(/\D/g, '');
    return digits.length < 11 || /0000$/.test(digits);
  }).length;
  const smsValidationCategories = {
    valid: smsValidRecipients,
    invalid: smsInvalidRecipients.length,
    duplicate: smsDuplicateCount,
    likelyLandline: smsLikelyLandlineRecipients,
  };
  const smsCostPerSegmentNumber = Number(smsCostPerSegment);
  const smsCostPerSegmentResolved = Number.isFinite(smsCostPerSegmentNumber) && smsCostPerSegmentNumber >= 0
    ? smsCostPerSegmentNumber
    : SMS_DEFAULT_COST_PER_SEGMENT;
  const smsEstimatedCost = Number(
    (smsValidRecipients * Math.max(1, smsSegmentEstimate.segments) * smsCostPerSegmentResolved).toFixed(4),
  );
  const smsRouteSimulationRows = providerMatrixRows
    .filter((row) => row.channel === 'sms')
    .map((row) => ({
      provider: row.provider,
      ready: row.ready,
      degraded: row.degraded,
      parityGapCount: row.parityGapCount,
    }));
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
  const mailerTemplatePreviewContext = useMemo(() => {
    if (!mailerVariablesInput.trim()) return {};
    try {
      return asRecord(JSON.parse(mailerVariablesInput));
    } catch {
      return {};
    }
  }, [mailerVariablesInput]);
  const mailerTemplatePreviewError = useMemo(() => {
    if (!mailerVariablesInput.trim()) return '';
    try {
      JSON.parse(mailerVariablesInput);
      return '';
    } catch {
      return 'Preview variables JSON is invalid.';
    }
  }, [mailerVariablesInput]);
  const mailerTemplatePreviewSubject = renderTemplateString(mailerSubjectInput || '(no subject)', mailerTemplatePreviewContext);
  const mailerTemplatePreviewBody = renderTemplateString(
    mailerTextInput || mailerHtmlInput || '(no body content)',
    mailerTemplatePreviewContext,
  );
  const mailerDomainHealthStatus = emailBouncePercent <= 2 && emailComplaintPercent <= 1
    ? 'Healthy'
    : emailBouncePercent <= 5 && emailComplaintPercent <= 2
      ? 'Watch'
      : 'Critical';
  const mailerDomainHealthDetail = `Bounce ${emailBouncePercent}% · Complaint ${emailComplaintPercent}%`;
  const mailerTrendBars = emailJobs.slice(0, 5).map((job) => {
    const total = Math.max(1, toInt(job.total));
    const delivered = Math.max(0, toInt(job.delivered));
    const deliveryRate = Math.round((delivered / total) * 100);
    return `${toText(job.status, 'job')} ${textBar(deliveryRate, 12)}`;
  });

  const handleRefresh = (): void => {
    triggerHaptic('impact', 'light');
    pushActivity('info', 'Manual refresh', 'Operator triggered dashboard refresh.');
    void loadBootstrap();
  };

  const resetSession = useCallback((): void => {
    triggerHaptic('warning');
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
    setActionLatencyMsSamples([]);
    setRuntimeCanaryInput('');
    setSmsCostPerSegment(String(SMS_DEFAULT_COST_PER_SEGMENT));
    setSmsDryRunMode(false);
    setCallScriptsSnapshot(null);
    setSelectedCallScriptId(0);
    setScriptSimulationResult(null);
    setProviderSwitchPlanByChannel({
      call: { target: '', stage: 'idle', postCheck: 'idle', rollbackSuggestion: '' },
      sms: { target: '', stage: 'idle', postCheck: 'idle', rollbackSuggestion: '' },
      email: { target: '', stage: 'idle', postCheck: 'idle', rollbackSuggestion: '' },
    });
    setSettingsOpen(false);
    setActivityLog([]);
    pollFailureNotedRef.current = false;
    void loadBootstrap();
  }, [loadBootstrap, triggerHaptic]);

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

  const handleApplyUserRole = useCallback(async (
    telegramId: string,
    role: 'admin' | 'operator' | 'viewer',
    reasonHint = '',
  ): Promise<void> => {
    const reasonInput = typeof window !== 'undefined'
      ? (window.prompt(
        `Provide audit reason for ${telegramId} -> ${role}`,
        reasonHint || '',
      ) || '').trim()
      : reasonHint.trim();
    const reason = reasonInput || reasonHint.trim();
    if (!reason) {
      pushActivity('info', 'Role update cancelled', `No audit reason supplied for ${telegramId}.`);
      return;
    }
    if (role === 'admin' && typeof window !== 'undefined') {
      const policyAck = (window.prompt(
        'Two-step approval required. Type "APPROVE ADMIN" to continue.',
        '',
      ) || '').trim().toUpperCase();
      if (policyAck !== 'APPROVE ADMIN') {
        pushActivity('info', 'Admin elevation blocked', `${telegramId} elevation not approved.`);
        return;
      }
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
    if (smsDryRunMode) {
      const previewMsg = `Dry-run complete: ${recipients.length} recipients, ${smsSegmentEstimate.segments} segment(s), est. $${smsEstimatedCost.toFixed(4)}.`;
      setNotice(previewMsg);
      pushActivity('info', 'SMS dry-run simulation', previewMsg);
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
    smsDryRunMode,
    smsEstimatedCost,
    smsMessageInput,
    smsProviderInput,
    smsRecipientsParsed,
    smsSegmentEstimate.segments,
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
        triggerHaptic('warning');
        pushActivity('info', 'Provider switch cancelled', `${channel.toUpperCase()} switch was cancelled.`);
        return;
      }
    }
    triggerHaptic('impact', 'medium');
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
  }, [invokeAction, pushActivity, runAction, triggerHaptic]);

  const setProviderSwitchTarget = useCallback((channel: ProviderChannel, target: string): void => {
    const normalized = String(target || '').trim().toLowerCase();
    setProviderSwitchPlanByChannel((prev) => ({
      ...prev,
      [channel]: {
        ...prev[channel],
        target: normalized,
        stage: normalized ? 'idle' : prev[channel].stage,
        postCheck: 'idle',
      },
    }));
  }, []);

  const simulateProviderSwitchPlan = useCallback(async (channel: ProviderChannel): Promise<void> => {
    const target = toText(providerSwitchPlanByChannel[channel]?.target, '').toLowerCase();
    if (!target) {
      setError(`Select a target provider for ${channel.toUpperCase()} simulation.`);
      return;
    }
    const preflightKey = `${channel}:${target}:plan`;
    setProviderPreflightBusy(preflightKey);
    setError('');
    try {
      const result = await invokeAction('provider.preflight', {
        channel,
        provider: target,
        network: 1,
        reachability: 1,
      }) as Record<string, unknown>;
      const ok = result?.success === true || toText(result?.error, '') === '';
      const rollbackTarget = providerCurrentByChannel[channel];
      setProviderSwitchPlanByChannel((prev) => ({
        ...prev,
        [channel]: {
          target,
          stage: ok ? 'simulated' : 'failed',
          postCheck: 'idle',
          rollbackSuggestion: rollbackTarget || '',
        },
      }));
      if (ok) {
        pushActivity('success', 'Provider switch simulated', `${channel.toUpperCase()} ${target} passed preflight.`);
      } else {
        pushActivity('error', 'Provider simulation failed', `${channel.toUpperCase()} ${target} failed preflight.`);
      }
    } catch (err) {
      const detail = err instanceof Error ? err.message : String(err);
      setError(detail);
      setProviderSwitchPlanByChannel((prev) => ({
        ...prev,
        [channel]: {
          ...prev[channel],
          stage: 'failed',
          postCheck: 'idle',
        },
      }));
      pushActivity('error', 'Provider simulation failed', detail);
    } finally {
      setProviderPreflightBusy('');
    }
  }, [invokeAction, providerCurrentByChannel, providerSwitchPlanByChannel, pushActivity, toText]);

  const confirmProviderSwitchPlan = useCallback((channel: ProviderChannel): void => {
    setProviderSwitchPlanByChannel((prev) => {
      const plan = prev[channel];
      if (!plan.target || plan.stage !== 'simulated') return prev;
      return {
        ...prev,
        [channel]: {
          ...plan,
          stage: 'confirmed',
        },
      };
    });
    pushActivity('info', 'Provider switch confirmed', `${channel.toUpperCase()} switch plan confirmed.`);
  }, [pushActivity]);

  const applyProviderSwitchPlan = useCallback(async (channel: ProviderChannel): Promise<void> => {
    const plan = providerSwitchPlanByChannel[channel];
    const target = toText(plan?.target, '').toLowerCase();
    if (!target || plan?.stage !== 'confirmed') {
      setError(`Simulate and confirm ${channel.toUpperCase()} plan before apply.`);
      return;
    }
    if (typeof window !== 'undefined') {
      const approved = window.confirm(`Apply ${channel.toUpperCase()} provider switch to "${target}" now?`);
      if (!approved) {
        pushActivity('info', 'Provider switch cancelled', `${channel.toUpperCase()} switch apply was cancelled.`);
        return;
      }
    }
    const previousProvider = providerCurrentByChannel[channel];
    await runAction(
      'provider.set',
      { channel, provider: target },
      {
        successMessage: `${channel.toUpperCase()} provider switched to ${target}.`,
      },
    );
    try {
      const postCheck = await invokeAction('provider.preflight', {
        channel,
        provider: target,
        network: 1,
        reachability: 1,
      }) as Record<string, unknown>;
      const healthy = postCheck?.success === true || toText(postCheck?.error, '') === '';
      const rollbackSuggestion = healthy ? '' : previousProvider;
      setProviderSwitchPlanByChannel((prev) => ({
        ...prev,
        [channel]: {
          ...prev[channel],
          stage: healthy ? 'applied' : 'failed',
          postCheck: healthy ? 'ok' : 'failed',
          rollbackSuggestion,
        },
      }));
      if (healthy) {
        pushActivity('success', 'Post-switch health check', `${channel.toUpperCase()} ${target} is healthy.`);
      } else {
        const msg = `${channel.toUpperCase()} ${target} post-check failed.${rollbackSuggestion ? ` Suggested rollback: ${rollbackSuggestion}` : ''}`;
        setNotice(msg);
        pushActivity('error', 'Post-switch health check failed', msg);
      }
    } catch (err) {
      const detail = err instanceof Error ? err.message : String(err);
      setProviderSwitchPlanByChannel((prev) => ({
        ...prev,
        [channel]: {
          ...prev[channel],
          stage: 'failed',
          postCheck: 'failed',
          rollbackSuggestion: previousProvider || '',
        },
      }));
      setError(detail);
      pushActivity('error', 'Post-switch verification failed', detail);
    }
  }, [invokeAction, providerCurrentByChannel, providerSwitchPlanByChannel, pushActivity, runAction, toText]);

  const resetProviderSwitchPlan = useCallback((channel: ProviderChannel): void => {
    setProviderSwitchPlanByChannel((prev) => ({
      ...prev,
      [channel]: {
        target: '',
        stage: 'idle',
        postCheck: 'idle',
        rollbackSuggestion: '',
      },
    }));
  }, []);

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

  const moduleVm: DashboardVm = {
    hasCapability,
    isFeatureEnabled,
    asRecord,
    toInt,
    toText,
    formatTime,
    textBar,
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
    emailBouncePercent,
    emailComplaintPercent,
    callLogs,
    callLogsTotal,
    emailJobs,
    dlqPayload,
    callDlq,
    emailDlq,
    runAction,
    hasMeaningfulData,
    smsRecipientsInput,
    setSmsRecipientsInput,
    handleRecipientsFile,
    smsProviderInput,
    setSmsProviderInput,
    smsMessageInput,
    setSmsMessageInput,
    smsScheduleAt,
    setSmsScheduleAt,
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
    mailerRecipientsInput,
    setMailerRecipientsInput,
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
    providerDegradedCount,
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
    scriptFlowFilter,
    setScriptFlowFilter,
    refreshCallScriptsModule,
    callScriptsTotal,
    callScripts,
    selectedCallScriptId,
    setSelectedCallScriptId,
    selectedCallScript,
    selectedCallScriptLifecycleState,
    selectedCallScriptLifecycle,
    scriptNameInput,
    setScriptNameInput,
    scriptDefaultProfileInput,
    setScriptDefaultProfileInput,
    scriptDescriptionInput,
    setScriptDescriptionInput,
    scriptPromptInput,
    setScriptPromptInput,
    scriptFirstMessageInput,
    setScriptFirstMessageInput,
    scriptObjectiveTagsInput,
    setScriptObjectiveTagsInput,
    saveCallScriptDraft,
    submitCallScriptForReview,
    scriptReviewNoteInput,
    setScriptReviewNoteInput,
    reviewCallScript,
    promoteCallScriptLive,
    scriptSimulationVariablesInput,
    setScriptSimulationVariablesInput,
    simulateCallScript,
    scriptSimulationResult,
    userSearch,
    setUserSearch,
    userSortBy,
    setUserSortBy,
    userSortDir,
    setUserSortDir,
    refreshUsersModule,
    usersPayload,
    usersRows,
    roleReasonTemplates: USER_ROLE_REASON_TEMPLATES,
    handleApplyUserRole,
    refreshAuditModule,
    runbookAction,
    incidentsPayload,
    incidentRows,
    runbookRows,
    auditRows,
  };
  const showModuleSkeleton = moduleSkeletonsEnabled && loading && !hasBootstrapData;
  const moduleBoundaryKeySuffix = `${activeModule}:${lastSuccessfulPollAt ?? 0}`;
  const renderModuleFallback = (moduleLabel: string) => (
    <div className="va-card va-module-fallback">
      <h3>{moduleLabel} is temporarily unavailable</h3>
      <p className="va-muted">
        This module hit a render-time error. Refresh data and reopen the module.
      </p>
      <button
        type="button"
        onClick={handleRefresh}
        disabled={loading || busyAction.length > 0}
      >
        Reload Module Data
      </button>
    </div>
  );
  const wrapModulePane = (moduleKey: DashboardModule, label: string, pane: JSX.Element) => {
    if (!moduleErrorBoundariesEnabled) {
      return <div key={`module-${moduleKey}`}>{pane}</div>;
    }
    return (
      <ErrorBoundary
        key={`${moduleKey}-${moduleBoundaryKeySuffix}`}
        fallback={renderModuleFallback(label)}
      >
        {pane}
      </ErrorBoundary>
    );
  };

  return (
    <main className="va-dashboard">
      {settingsOpen ? (
        <header className="va-header is-settings">
          <div className="va-settings-header-grid">
            <button
              type="button"
              className="va-settings-back"
              onClick={() => toggleSettings(false)}
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
              className="va-settings-header-action"
              onClick={handleRefresh}
              disabled={loading || busyAction.length > 0}
            >
              Sync
            </button>
          </div>
        </header>
      ) : (
        <header className="va-header">
          <div className="va-header-copy">
            <h1>Voicednut Admin Console</h1>
            <p className="va-muted">{activeModuleMeta.subtitle}</p>
            <p className="va-module-context-line">
              <span className="va-module-context-icon" aria-hidden>{moduleGlyph(activeModule)}</span>
              <span>{activeModuleMeta.detail}</span>
            </p>
          </div>
          <div className="va-header-meta">
            <span>Admin: {userLabel}</span>
            <span>Role: {sessionRole} ({sessionRoleSource})</span>
            <span>Settings button: {settingsStatusLabel}</span>
            <span>Feature flags: {Object.keys(featureFlags).length || 'default'}</span>
            <button
              type="button"
              onClick={() => toggleSettings(true)}
              disabled={loading}
            >
              Open Settings
            </button>
            <button type="button" onClick={handleRefresh} disabled={loading || busyAction.length > 0}>
              Refresh
            </button>
          </div>
        </header>
      )}

      {loading ? <p className="va-muted">Loading dashboard...</p> : null}
      {error ? <p className="va-error">{error}</p> : null}
      {notice ? <p className="va-notice">{notice}</p> : null}
      {settingsOpen ? (
        <section className="va-view-stage va-view-stage-settings">
        <SettingsPage
          userLabel={userLabel}
          sessionRole={sessionRole}
          sessionRoleSource={sessionRoleSource}
            pollingPaused={pollingPaused}
            loading={loading}
          busy={busyAction.length > 0}
          settingsStatusLabel={settingsStatusLabel}
          apiBaseUrl={API_BASE_URL || 'same-origin'}
          visibleModules={visibleModules}
          featureFlags={featureFlagInspectorItems}
          featureFlagsSourceLabel={featureFlagsSourceLabel}
          featureFlagsUpdatedAtLabel={featureFlagsUpdatedAtLabel}
          onTogglePolling={() => setPollingPaused((prev) => !prev)}
          onSyncNow={handleRefresh}
          onRetrySession={resetSession}
            onJumpToModule={(moduleId) => {
              if (!MODULE_DEFINITIONS.some((module) => module.id === moduleId)) return;
              selectModule(moduleId as DashboardModule);
              toggleSettings(false);
            }}
          />
        </section>
      ) : null}
      {!settingsOpen ? (
        <section className="va-view-stage va-view-stage-dashboard">
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
              <strong>{streamConnected ? 'Realtime' : hasBootstrapData ? 'Polling' : 'Warming up'}</strong>
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
            onClick={() => selectModule(module.id)}
          >
            {module.label}
          </button>
        ))}
      </section>
      {visibleModules.length === 0 ? (
        <p className="va-error">No dashboard modules are enabled for this role.</p>
      ) : null}
      {showModuleSkeleton ? (
        <section className="va-grid va-module-skeleton-grid">
          {['Loading module', 'Preparing data', 'Syncing controls'].map((label) => (
            <div key={label} className="va-card va-module-skeleton-card">
              <div className="va-module-skeleton-title" />
              <div className="va-module-skeleton-line" />
              <div className="va-module-skeleton-line short" />
              <p className="va-muted">{label}...</p>
            </div>
          ))}
        </section>
      ) : (
        <>
          {wrapModulePane(
            'ops',
            'Ops Dashboard',
            <OpsDashboardPage
              visible={activeModule === 'ops' && hasCapability('dashboard_view')}
              vm={moduleVm}
            />,
          )}
          {wrapModulePane(
            'sms',
            'SMS Sender',
            <SmsSenderPage
              visible={activeModule === 'sms' && hasCapability('sms_bulk_manage')}
              vm={moduleVm}
            />,
          )}
          {wrapModulePane(
            'mailer',
            'Mailer Console',
            <MailerPage
              visible={activeModule === 'mailer' && hasCapability('email_bulk_manage')}
              vm={moduleVm}
            />,
          )}
          {wrapModulePane(
            'provider',
            'Provider Control',
            <ProviderControlPage
              visible={activeModule === 'provider' && hasCapability('provider_manage')}
              vm={moduleVm}
            />,
          )}
          {wrapModulePane(
            'content',
            'Script Studio',
            <ScriptStudioPage
              visible={activeModule === 'content' && hasCapability('caller_flags_manage')}
              vm={moduleVm}
            />,
          )}
          {wrapModulePane(
            'users',
            'User & Role Admin',
            <UsersRolePage
              visible={activeModule === 'users' && hasCapability('users_manage')}
              vm={moduleVm}
            />,
          )}
          {wrapModulePane(
            'audit',
            'Audit & Incidents',
            <AuditIncidentsPage
              visible={activeModule === 'audit' && hasCapability('dashboard_view')}
              vm={moduleVm}
            />,
          )}
        </>
      )}
      {visibleModules.length > 0 ? (
        <nav className="va-bottom-nav-wrap" aria-label="Quick module navigation">
          <div className="va-bottom-nav">
            {visibleModules.map((module) => (
              <button
                key={`bottom-${module.id}`}
                type="button"
                className={`va-bottom-nav-item ${activeModule === module.id ? 'is-active' : ''}`}
                onClick={() => selectModule(module.id)}
              >
                <span className="va-bottom-nav-glyph" aria-hidden>{moduleGlyph(module.id)}</span>
                <span>{module.label}</span>
              </button>
            ))}
          </div>
        </nav>
      ) : null}
        </section>
      ) : null}
    </main>
  );
}
