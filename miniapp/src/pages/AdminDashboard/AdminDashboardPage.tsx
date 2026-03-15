import { backButton, hapticFeedback, initData, settingsButton, useRawInitData, useSignal } from '@tma.js/sdk-react';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import '@/pages/AdminDashboard/AdminDashboardPage.css';
import { AuditIncidentsPage } from '@/pages/AdminDashboard/AuditIncidentsPage';
import { MailerPage } from '@/pages/AdminDashboard/MailerPage';
import { OpsDashboardPage } from '@/pages/AdminDashboard/OpsDashboardPage';
import {
  DashboardBottomNav,
  DashboardMainHeader,
  DashboardModuleNav,
  DashboardSettingsHeader,
} from '@/components/admin-dashboard/DashboardChrome';
import { ProviderChannelCard } from '@/components/admin-dashboard/ProviderChannelCard';
import {
  EmptyModulesCard,
  ModuleErrorFallbackCard,
  ModuleSkeletonGrid,
  SessionBlockedCard,
} from '@/components/admin-dashboard/DashboardStateCards';
import { DashboardStatusRail } from '@/components/admin-dashboard/DashboardStatusRail';
import { ProviderControlPage } from '@/pages/AdminDashboard/ProviderControlPage';
import {
  asRecord,
  formatTime,
  parseApiError,
  parseApiErrorCode,
  parseJsonResponse,
  textBar,
  toInt,
  toText,
} from '@/services/admin-dashboard/dashboardPrimitives';
import {
  buildApiUrl,
  buildEventStreamUrl,
  isSessionBootstrapBlockingCode,
  readSessionCache,
  writeSessionCache,
} from '@/services/admin-dashboard/dashboardTransport';
import {
  validateBootstrapPayload,
  validatePollPayload,
  validateStreamPayload,
} from '@/services/admin-dashboard/dashboardApiContracts';
import { buildDashboardVm } from '@/services/admin-dashboard/dashboardVm/buildDashboardVm';
import { buildGovernanceVmSection } from '@/services/admin-dashboard/dashboardVm/buildGovernanceVmSection';
import { buildMailerVmSection } from '@/services/admin-dashboard/dashboardVm/buildMailerVmSection';
import { buildOpsVmSection } from '@/services/admin-dashboard/dashboardVm/buildOpsVmSection';
import { buildProviderVmSection } from '@/services/admin-dashboard/dashboardVm/buildProviderVmSection';
import { buildSmsVmSection } from '@/services/admin-dashboard/dashboardVm/buildSmsVmSection';
import type { SessionCacheEntry } from '@/services/admin-dashboard/dashboardTransport';
import {
  useDashboardActions,
} from '@/hooks/admin-dashboard/useDashboardActions';
import type { ActionRequestMeta } from '@/hooks/admin-dashboard/useDashboardActions';
import { useDashboardEventStream } from '@/hooks/admin-dashboard/useDashboardEventStream';
import {
  useDashboardFeatureFlags,
} from '@/hooks/admin-dashboard/useDashboardFeatureFlags';
import type { FeatureFlagRegistryEntry } from '@/hooks/admin-dashboard/useDashboardFeatureFlags';
import {
  useDashboardModuleLayout,
} from '@/hooks/admin-dashboard/useDashboardModuleLayout';
import {
  useDashboardMessagingMetrics,
} from '@/hooks/admin-dashboard/useDashboardMessagingMetrics';
import {
  useDashboardMessagingActions,
} from '@/hooks/admin-dashboard/useDashboardMessagingActions';
import {
  useDashboardGovernanceData,
} from '@/hooks/admin-dashboard/useDashboardGovernanceData';
import {
  useDashboardGovernanceActions,
} from '@/hooks/admin-dashboard/useDashboardGovernanceActions';
import {
  useDashboardCallScriptActions,
} from '@/hooks/admin-dashboard/useDashboardCallScriptActions';
import {
  useDashboardRuntimeControls,
} from '@/hooks/admin-dashboard/useDashboardRuntimeControls';
import {
  useDashboardOpsMetrics,
} from '@/hooks/admin-dashboard/useDashboardOpsMetrics';
import {
  useDashboardProviderMetrics,
} from '@/hooks/admin-dashboard/useDashboardProviderMetrics';
import {
  useDashboardProviderActions,
} from '@/hooks/admin-dashboard/useDashboardProviderActions';
import { useDashboardPollingLoop } from '@/hooks/admin-dashboard/useDashboardPollingLoop';
import { SettingsPage } from '@/pages/AdminDashboard/SettingsPage';
import { ScriptStudioPage } from '@/pages/AdminDashboard/ScriptStudioPage';
import { SmsSenderPage } from '@/pages/AdminDashboard/SmsSenderPage';
import { UsersRolePage } from '@/pages/AdminDashboard/UsersRolePage';
import { ErrorBoundary } from '@/components/ErrorBoundary';

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

interface SessionResponse {
  success: boolean;
  token?: string;
  expires_at?: number;
  error?: string;
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

type ActivityStatus = 'info' | 'success' | 'error';
type DashboardModule = 'ops' | 'sms' | 'mailer' | 'provider' | 'content' | 'users' | 'audit';
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

  const resolveEventStreamUrl = useCallback((path: string, sessionToken: string): string => (
    buildEventStreamUrl(path, sessionToken, API_BASE_URL)
  ), []);

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
    const cached = readSessionCache(SESSION_STORAGE_KEY);
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
      const response = await fetch(buildApiUrl('/miniapp/session', API_BASE_URL), {
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
      writeSessionCache(SESSION_STORAGE_KEY, cacheEntry);
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

    const response = await fetch(buildApiUrl(path, API_BASE_URL), {
      ...options,
      headers,
    });

    const payload = await parseJsonResponse(response);
    if (response.status === 401 && retryCount < SESSION_REFRESH_RETRY_COUNT) {
      writeSessionCache(SESSION_STORAGE_KEY, null);
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
      const rawPayload = asRecord(
        await request<DashboardApiPayload | null>('/miniapp/bootstrap'),
      );
      const bootstrapValidation = validateBootstrapPayload(rawPayload);
      if (!bootstrapValidation.ok) {
        throw new Error(bootstrapValidation.error);
      }
      const payload = bootstrapValidation.payload as DashboardApiPayload;
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
      const rawPayload = asRecord(
        await request<DashboardApiPayload | null>('/miniapp/jobs/poll'),
      );
      const pollValidation = validatePollPayload(rawPayload);
      if (!pollValidation.ok) {
        throw new Error(pollValidation.error);
      }
      const payload = pollValidation.payload as DashboardApiPayload;
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
    const streamValidation = validateStreamPayload(candidate);
    if (!streamValidation.ok) {
      return false;
    }
    const nextPayload = streamValidation.payload as DashboardApiPayload;
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

  const createActionMeta = useCallback((action: string): ActionRequestMeta => (
    createActionRequestMeta(action, activeModule)
  ), [activeModule]);

  const { invokeAction, runAction, actionTelemetry } = useDashboardActions({
    createActionMeta,
    request,
    loadBootstrap,
    pushActivity,
    triggerHaptic,
    setBusyAction,
    setNotice,
    setError,
    setActionLatencyMsSamples,
    actionLatencySampleLimit: ACTION_LATENCY_SAMPLE_LIMIT,
  });
  const {
    featureFlags,
    featureFlagsSourceLabel,
    featureFlagsUpdatedAtLabel,
    featureFlagInspectorItems,
    isFeatureEnabled,
    realtimeStreamEnabled,
    moduleSkeletonsEnabled,
    moduleErrorBoundariesEnabled,
  } = useDashboardFeatureFlags({
    pollPayload,
    bootstrap,
    registry: FEATURE_FLAG_REGISTRY,
  });

  useEffect(() => {
    void loadBootstrap();
  }, [loadBootstrap]);

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

  const {
    streamMode,
    streamConnected,
    streamFailureCount,
    streamLastEventAt,
  } = useDashboardEventStream({
    enabled: realtimeStreamEnabled && !sessionBlocked && !pollingPaused,
    token,
    buildEventStreamUrl: resolveEventStreamUrl,
    applyStreamPayload,
    refreshPoll: loadPoll,
    reconnectBaseMs: STREAM_RECONNECT_BASE_MS,
    reconnectMaxMs: STREAM_RECONNECT_MAX_MS,
    refreshDebounceMs: STREAM_REFRESH_DEBOUNCE_MS,
  });

  useDashboardPollingLoop({
    enabled: Boolean(token) && !sessionBlocked && !pollingPaused && !streamConnected,
    baseIntervalMs: serverPollIntervalMs,
    loadPoll,
    setNextPollAt,
    maxIntervalMs: POLL_MAX_INTERVAL_MS,
    backoffMultiplier: POLL_BACKOFF_MULTIPLIER,
    jitterMs: POLL_JITTER_MS,
  });

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
  const dashboardLayoutPayload = pollPayload?.module_layout
    || bootstrap?.module_layout
    || dashboard?.module_layout
    || pollPayload?.modules
    || bootstrap?.modules
    || dashboard?.modules
    || {};
  const {
    hasCapability,
    visibleModules,
    preferredServerModule,
  } = useDashboardModuleLayout<DashboardModule>({
    sessionCaps,
    dashboardLayoutPayload,
    moduleDefinitions: MODULE_DEFINITIONS,
    moduleDefaultOrder: MODULE_DEFAULT_ORDER,
    moduleIdSet: MODULE_ID_SET,
  });
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

  const {
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
    smsRecipientsParsed,
    smsInvalidRecipients,
    smsDuplicateCount,
    smsSegmentEstimate,
    smsValidationCategories,
    smsEstimatedCost,
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
  } = useDashboardMessagingMetrics({
    smsSummary,
    emailStats,
    emailJobs,
    smsRecipientsInput,
    smsMessageInput,
    smsCostPerSegment,
    smsDefaultCostPerSegment: SMS_DEFAULT_COST_PER_SEGMENT,
    mailerRecipientsInput,
    mailerSubjectInput,
    mailerHtmlInput,
    mailerTextInput,
    mailerVariablesInput,
  });
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
  const {
    bridgeHardFailures,
    bridgeSoftFailures,
    isDashboardDegraded,
    streamModeLabel,
    streamLastEventLabel,
    syncModeLabel,
    nextPollLabel,
    lastPollLabel,
    lastSuccessfulPollLabel,
    pollFreshnessLabel,
    uptimeScore,
    callTotal,
    callCompleted,
    callFailed,
    callFailureRate,
    callSuccessRate,
    queueBacklogTotal,
    sloErrorBudgetPercent,
    sloP95ActionLatencyMs,
    degradedCauses,
  } = useDashboardOpsMetrics({
    bridgePayload: pollPayload?.bridge || dashboard?.bridge,
    sessionBlocked,
    pollFailureCount,
    pollDegradedFailures: POLL_DEGRADED_FAILURES,
    error,
    hasBootstrapData,
    streamMode,
    streamConnected,
    streamLastEventAt,
    pollingPaused,
    nextPollAt,
    lastPollAt,
    lastSuccessfulPollAt,
    callStatsPayload,
    opsQueueBacklog,
    actionLatencyMsSamples,
  });
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
  const {
    providerMatrixRows,
    providerReadinessTotals,
    providerReadinessPercent,
    providerDegradedCount,
    providerCurrentByChannel,
    providerSupportedByChannel,
  } = useDashboardProviderMetrics({
    providerCompatibilityChannels,
    providersByChannel: asRecord(providersByChannel),
  });
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
  const smsRouteSimulationRows = providerMatrixRows
    .filter((row) => row.channel === 'sms')
    .map((row) => ({
      provider: row.provider,
      ready: row.ready,
      degraded: row.degraded,
      parityGapCount: row.parityGapCount,
    }));

  const handleRefresh = (): void => {
    triggerHaptic('impact', 'light');
    pushActivity('info', 'Manual refresh', 'Operator triggered dashboard refresh.');
    void loadBootstrap();
  };

  const resetSession = useCallback((): void => {
    triggerHaptic('warning');
    writeSessionCache(SESSION_STORAGE_KEY, null);
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

  const {
    refreshUsersModule,
    refreshAuditModule,
  } = useDashboardGovernanceData<
    MiniAppUsersPayload,
    MiniAppAuditPayload,
    MiniAppIncidentsPayload
  >({
    invokeAction,
    pushActivity,
    setError,
    setUsersSnapshot,
    setAuditSnapshot,
    setIncidentsSnapshot,
    userSearch,
    userSortBy,
    userSortDir,
  });

  const {
    roleReasonTemplates,
    handleApplyUserRole,
    runbookAction,
  } = useDashboardGovernanceActions({
    runAction,
    pushActivity,
    refreshUsersModule,
    refreshAuditModule,
    providersByChannel,
  });

  const {
    refreshRuntimeStatus,
    enableRuntimeMaintenance,
    disableRuntimeMaintenance,
    applyRuntimeCanary,
    clearRuntimeCanary,
  } = useDashboardRuntimeControls({
    invokeAction,
    runAction,
    pushActivity,
    setError,
    setRuntimeCanaryInput,
    runtimeCanaryInput,
    onRuntimeStatusLoaded: (data) => {
      setPollPayload((prev) => ({
        ...(prev || {}),
        voice_runtime: data,
      }));
    },
  });

  const {
    refreshCallScriptsModule,
    saveCallScriptDraft,
    submitCallScriptForReview,
    reviewCallScript,
    promoteCallScriptLive,
    simulateCallScript,
  } = useDashboardCallScriptActions({
    invokeAction,
    runAction,
    pushActivity,
    setError,
    setCallScriptsSnapshot,
    setScriptSimulationResult,
    scriptFlowFilter,
    selectedCallScriptId,
    scriptNameInput,
    scriptDescriptionInput,
    scriptDefaultProfileInput,
    scriptPromptInput,
    scriptFirstMessageInput,
    scriptObjectiveTagsInput,
    scriptReviewNoteInput,
    scriptSimulationVariablesInput,
  });

  const {
    sendSmsFromConsole,
    sendMailerFromConsole,
  } = useDashboardMessagingActions({
    invokeAction,
    runAction,
    pushActivity,
    setError,
    setNotice,
    setBusyAction,
    formatTime,
    smsRecipientsParsed,
    smsMessageInput,
    smsDryRunMode,
    smsSegmentEstimateSegments: smsSegmentEstimate.segments,
    smsEstimatedCost,
    smsScheduleAt,
    smsProviderInput,
    mailerRecipientsParsed,
    mailerTemplateIdInput,
    mailerSubjectInput,
    mailerHtmlInput,
    mailerTextInput,
    mailerVariablesInput,
    mailerScheduleAt,
  });

  const {
    preflightActiveProviders,
    safeSwitchProvider,
    setProviderSwitchTarget,
    simulateProviderSwitchPlan,
    confirmProviderSwitchPlan,
    applyProviderSwitchPlan,
    resetProviderSwitchPlan,
    runProviderPreflight,
  } = useDashboardProviderActions({
    invokeAction,
    runAction,
    pushActivity,
    triggerHaptic,
    loadBootstrap,
    setError,
    setNotice,
    setProviderPreflightBusy,
    setProviderPreflightRows,
    setProviderRollbackByChannel,
    setProviderSwitchPlanByChannel,
    providersByChannel,
    providerCurrentByChannel,
    providerSwitchPlanByChannel,
  });

  const renderProviderSection = (channel: ProviderChannel) => {
    const channelData = providersByChannel[channel] || {};
    const currentProvider = toText(channelData.provider, '').toLowerCase();
    const supported = asStringList(channelData.supported_providers);
    const readiness = asRecord(channelData.readiness);
    const rollbackTarget = toText(providerRollbackByChannel[channel], '').toLowerCase();

    return (
      <ProviderChannelCard
        key={channel}
        channel={channel}
        currentProvider={currentProvider}
        supportedProviders={supported}
        readinessByProvider={readiness}
        busyAction={busyAction}
        providerPreflightBusy={providerPreflightBusy}
        providerPreflightRows={providerPreflightRows}
        rollbackTarget={rollbackTarget}
        onSwitchProvider={safeSwitchProvider}
        onRunPreflight={runProviderPreflight}
        onRollback={async (rollbackChannel, provider) => {
          await runAction(
            'provider.rollback',
            { channel: rollbackChannel, provider },
            {
              confirmText: `Rollback ${rollbackChannel.toUpperCase()} provider to "${provider}"?`,
              successMessage: `${rollbackChannel.toUpperCase()} provider rolled back to ${provider}.`,
            },
          );
        }}
      />
    );
  };

  const opsVmSection = buildOpsVmSection({
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
  });

  const smsVmSection = buildSmsVmSection({
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
  });

  const mailerVmSection = buildMailerVmSection({
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
  });

  const providerVmSection = buildProviderVmSection({
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
  });

  const governanceVmSection = buildGovernanceVmSection({
    hasCapability,
    isFeatureEnabled,
    asRecord,
    toInt,
    toText,
    formatTime,
    busyAction,
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
    roleReasonTemplates,
    handleApplyUserRole,
    refreshAuditModule,
    runbookAction,
    incidentsPayload,
    incidentRows,
    runbookRows,
    auditRows,
  });

  const moduleVm = buildDashboardVm(
    opsVmSection,
    smsVmSection,
    mailerVmSection,
    providerVmSection,
    governanceVmSection,
  );
  const showModuleSkeleton = moduleSkeletonsEnabled && loading && !hasBootstrapData;
  const moduleBoundaryKeySuffix = `${activeModule}:${lastSuccessfulPollAt ?? 0}`;
  const renderModuleFallback = (moduleLabel: string) => (
    <ModuleErrorFallbackCard
      moduleLabel={moduleLabel}
      onReload={handleRefresh}
      reloadDisabled={loading || busyAction.length > 0}
    />
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
        <DashboardSettingsHeader
          loading={loading}
          busy={busyAction.length > 0}
          onBack={() => toggleSettings(false)}
          onSync={handleRefresh}
        />
      ) : (
        <DashboardMainHeader
          userLabel={userLabel}
          sessionRole={sessionRole}
          sessionRoleSource={sessionRoleSource}
          settingsStatusLabel={settingsStatusLabel}
          featureFlagsCount={Object.keys(featureFlags).length || 'default'}
          moduleDetail={activeModuleMeta.detail}
          activeModuleGlyph={moduleGlyph(activeModule)}
          loading={loading}
          busy={busyAction.length > 0}
          onOpenSettings={() => toggleSettings(true)}
          onRefresh={handleRefresh}
        />
      )}

      <DashboardStatusRail
        loading={loading}
        error={error}
        notice={notice}
        busyAction={busyAction}
        syncModeLabel={syncModeLabel}
        pollFailureCount={pollFailureCount}
        streamFailureCount={streamFailureCount}
        bridgeHardFailures={bridgeHardFailures}
        bridgeSoftFailures={bridgeSoftFailures}
        actionTelemetry={actionTelemetry}
      />
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
        <SessionBlockedCard
          errorCode={errorCode}
          onRetrySession={resetSession}
          retryDisabled={loading || busyAction.length > 0}
        />
      ) : null}

      <section className="va-grid va-grid-hero">
        <div className="va-card va-hero">
          <div className="va-hero-top">
            <span className="va-kicker">Operational Health</span>
            <strong>{uptimeScore}%</strong>
            <p className="va-muted">
              Live status from poll stability, bridge responses, and queue pressure.
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
              <strong>{streamConnected ? 'Realtime' : hasBootstrapData ? 'Polling' : 'Initializing'}</strong>
            </article>
          </div>
        </div>
      </section>

      <DashboardModuleNav
        modules={visibleModules}
        activeModuleId={activeModule}
        onSelectModule={(moduleId) => selectModule(moduleId as DashboardModule)}
      />
      {visibleModules.length === 0 ? (
        <EmptyModulesCard
          onRefreshAccess={handleRefresh}
          refreshDisabled={loading || busyAction.length > 0}
        />
      ) : null}
      {showModuleSkeleton ? (
        <ModuleSkeletonGrid />
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
        <DashboardBottomNav
          modules={visibleModules}
          activeModuleId={activeModule}
          moduleGlyph={moduleGlyph}
          onSelectModule={(moduleId) => selectModule(moduleId as DashboardModule)}
        />
      ) : null}
        </section>
      ) : null}
    </main>
  );
}
