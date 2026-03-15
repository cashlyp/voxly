import type { ReactNode } from 'react';

export type ProviderChannel = 'call' | 'sms' | 'email';
export type UserRole = 'admin' | 'operator' | 'viewer';

export interface JsonRecord {
  [key: string]: unknown;
}

export interface ActivityEntry {
  id: string;
  title: string;
  detail: string;
  status: 'info' | 'success' | 'error';
  at: string;
}

export interface ProviderReadinessTotals {
  ready: number;
  total: number;
}

export interface SmsSegmentEstimate {
  segments: number;
  perSegment: number;
}

export interface RunActionOptions {
  confirmText?: string;
  successMessage?: string;
}

export interface EmailJob {
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

export interface CallLogRow {
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

export interface DlqPayload {
  call_open?: unknown;
  email_open?: unknown;
}

export interface DlqCallRow {
  id?: unknown;
  job_type?: unknown;
  replay_count?: unknown;
}

export interface DlqEmailRow {
  id?: unknown;
  message_id?: unknown;
  reason?: unknown;
}

export interface ProviderMatrixRow {
  channel: ProviderChannel;
  provider: string;
  ready: boolean;
  degraded: boolean;
  flowCount: number;
  parityGapCount: number;
  paymentMode: string;
}

export interface CallScriptLifecycle {
  lifecycle_state?: unknown;
  submitted_for_review_at?: unknown;
  reviewed_at?: unknown;
  reviewed_by?: unknown;
  review_note?: unknown;
  live_at?: unknown;
  live_by?: unknown;
}

export interface CallScriptRow {
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

export interface UsersPayload {
  total?: unknown;
}

export interface UserRow {
  telegram_id?: unknown;
  role?: unknown;
  role_source?: unknown;
  total_calls?: unknown;
  failed_calls?: unknown;
  last_activity?: unknown;
}

export interface IncidentsPayload {
  total_alerts?: unknown;
}

export interface IncidentRow {
  service_name?: unknown;
  status?: unknown;
  details?: unknown;
  timestamp?: unknown;
}

export interface RunbookRow {
  label?: unknown;
  action?: unknown;
  capability?: unknown;
}

export interface AuditRow {
  service_name?: unknown;
  status?: unknown;
  details?: unknown;
  timestamp?: unknown;
}

export type StringStateSetter = (value: string | ((prev: string) => string)) => void;
export type NumberStateSetter = (value: number | ((prev: number) => number)) => void;
export type BooleanStateSetter = (value: boolean | ((prev: boolean) => boolean)) => void;

export interface SmsRouteSimulationRow {
  provider: string;
  ready: boolean;
  degraded: boolean;
  parityGapCount: number;
}

export interface ProviderSwitchPlan {
  target: string;
  stage: 'idle' | 'simulated' | 'confirmed' | 'applied' | 'failed';
  postCheck: 'idle' | 'ok' | 'failed';
  rollbackSuggestion: string;
}

export interface DashboardVm {
  hasCapability: (capability: string) => boolean;
  isFeatureEnabled: (flag: string, fallback?: boolean) => boolean;
  asRecord: (value: unknown) => JsonRecord;
  toInt: (value: unknown, fallback?: number) => number;
  toText: (value: unknown, fallback?: string) => string;
  formatTime: (value: unknown) => string;
  textBar: (percentage: number, length?: number) => string;
  isDashboardDegraded: boolean;
  syncModeLabel: string;
  streamModeLabel: string;
  streamLastEventLabel: string;
  streamFailureCount: number;
  pollFailureCount: number;
  bridgeHardFailures: number;
  bridgeSoftFailures: number;
  sloErrorBudgetPercent: number;
  sloP95ActionLatencyMs: number;
  pollFreshnessLabel: string;
  degradedCauses: string[];
  lastPollLabel: string;
  lastSuccessfulPollLabel: string;
  nextPollLabel: string;
  callCompleted: number;
  callTotal: number;
  callFailed: number;
  callFailureRate: number;
  callSuccessRate: number;
  queueBacklogTotal: number;
  providerReadinessTotals: ProviderReadinessTotals;
  providerReadinessPercent: number;
  runtimeEffectiveMode: string;
  runtimeModeOverride: string;
  runtimeCanaryEffective: number;
  runtimeCanaryOverrideLabel: string;
  runtimeIsCircuitOpen: boolean;
  runtimeForcedLegacyUntil: string;
  runtimeActiveTotal: number;
  runtimeActiveLegacy: number;
  runtimeActiveVoiceAgent: number;
  busyAction: string;
  enableRuntimeMaintenance: () => Promise<void>;
  disableRuntimeMaintenance: () => Promise<void>;
  refreshRuntimeStatus: () => Promise<void>;
  runtimeCanaryInput: string;
  setRuntimeCanaryInput: StringStateSetter;
  applyRuntimeCanary: () => Promise<void>;
  clearRuntimeCanary: () => Promise<void>;
  activityLog: ActivityEntry[];
  renderProviderSection: (channel: ProviderChannel) => ReactNode;
  smsTotalRecipients: number;
  smsSuccess: number;
  smsFailed: number;
  smsProcessedPercent: number;
  emailTotalRecipients: number;
  emailSent: number;
  emailFailed: number;
  emailDelivered: number;
  emailBounced: number;
  emailComplained: number;
  emailSuppressed: number;
  emailProcessedPercent: number;
  emailDeliveredPercent: number;
  emailBouncePercent: number;
  emailComplaintPercent: number;
  callLogs: CallLogRow[];
  callLogsTotal: number;
  emailJobs: EmailJob[];
  dlqPayload: DlqPayload;
  callDlq: DlqCallRow[];
  emailDlq: DlqEmailRow[];
  runAction: (
    action: string,
    payload: Record<string, unknown>,
    options?: RunActionOptions,
  ) => Promise<void>;
  hasMeaningfulData: boolean;
  smsRecipientsInput: string;
  setSmsRecipientsInput: StringStateSetter;
  handleRecipientsFile: (file: File | null, kind: 'sms' | 'mailer') => Promise<void>;
  smsProviderInput: string;
  setSmsProviderInput: StringStateSetter;
  smsMessageInput: string;
  setSmsMessageInput: StringStateSetter;
  smsScheduleAt: string;
  setSmsScheduleAt: StringStateSetter;
  sendSmsFromConsole: () => Promise<void>;
  smsRecipientsParsed: string[];
  smsInvalidRecipients: string[];
  smsDuplicateCount: number;
  smsSegmentEstimate: SmsSegmentEstimate;
  smsCostPerSegment: string;
  setSmsCostPerSegment: StringStateSetter;
  smsEstimatedCost: number;
  smsDryRunMode: boolean;
  setSmsDryRunMode: BooleanStateSetter;
  smsValidationCategories: {
    valid: number;
    invalid: number;
    duplicate: number;
    likelyLandline: number;
  };
  smsRouteSimulationRows: SmsRouteSimulationRow[];
  mailerRecipientsInput: string;
  setMailerRecipientsInput: StringStateSetter;
  mailerTemplateIdInput: string;
  setMailerTemplateIdInput: StringStateSetter;
  mailerSubjectInput: string;
  setMailerSubjectInput: StringStateSetter;
  mailerHtmlInput: string;
  setMailerHtmlInput: StringStateSetter;
  mailerTextInput: string;
  setMailerTextInput: StringStateSetter;
  mailerVariablesInput: string;
  setMailerVariablesInput: StringStateSetter;
  mailerScheduleAt: string;
  setMailerScheduleAt: StringStateSetter;
  sendMailerFromConsole: () => Promise<void>;
  mailerRecipientsParsed: string[];
  mailerInvalidRecipients: string[];
  mailerDuplicateCount: number;
  mailerVariableKeys: string[];
  mailerTemplatePreviewSubject: string;
  mailerTemplatePreviewBody: string;
  mailerTemplatePreviewError: string;
  mailerDomainHealthStatus: string;
  mailerDomainHealthDetail: string;
  mailerTrendBars: string[];
  providerDegradedCount: number;
  providerPreflightBusy: string;
  preflightActiveProviders: () => Promise<void>;
  loading: boolean;
  handleRefresh: () => void;
  providerMatrixRows: ProviderMatrixRow[];
  providerCurrentByChannel: Record<ProviderChannel, string>;
  providerSupportedByChannel: Record<ProviderChannel, string[]>;
  providerSwitchPlanByChannel: Record<ProviderChannel, ProviderSwitchPlan>;
  setProviderSwitchTarget: (channel: ProviderChannel, target: string) => void;
  simulateProviderSwitchPlan: (channel: ProviderChannel) => Promise<void>;
  confirmProviderSwitchPlan: (channel: ProviderChannel) => void;
  applyProviderSwitchPlan: (channel: ProviderChannel) => Promise<void>;
  resetProviderSwitchPlan: (channel: ProviderChannel) => void;
  scriptFlowFilter: string;
  setScriptFlowFilter: StringStateSetter;
  refreshCallScriptsModule: () => Promise<void>;
  callScriptsTotal: number;
  callScripts: CallScriptRow[];
  selectedCallScriptId: number;
  setSelectedCallScriptId: NumberStateSetter;
  selectedCallScript: CallScriptRow | null;
  selectedCallScriptLifecycleState: string;
  selectedCallScriptLifecycle: CallScriptLifecycle;
  scriptNameInput: string;
  setScriptNameInput: StringStateSetter;
  scriptDefaultProfileInput: string;
  setScriptDefaultProfileInput: StringStateSetter;
  scriptDescriptionInput: string;
  setScriptDescriptionInput: StringStateSetter;
  scriptPromptInput: string;
  setScriptPromptInput: StringStateSetter;
  scriptFirstMessageInput: string;
  setScriptFirstMessageInput: StringStateSetter;
  scriptObjectiveTagsInput: string;
  setScriptObjectiveTagsInput: StringStateSetter;
  saveCallScriptDraft: () => Promise<void>;
  submitCallScriptForReview: () => Promise<void>;
  scriptReviewNoteInput: string;
  setScriptReviewNoteInput: StringStateSetter;
  reviewCallScript: (decision: 'approve' | 'reject') => Promise<void>;
  promoteCallScriptLive: () => Promise<void>;
  scriptSimulationVariablesInput: string;
  setScriptSimulationVariablesInput: StringStateSetter;
  simulateCallScript: () => Promise<void>;
  scriptSimulationResult: unknown;
  userSearch: string;
  setUserSearch: StringStateSetter;
  userSortBy: string;
  setUserSortBy: StringStateSetter;
  userSortDir: string;
  setUserSortDir: StringStateSetter;
  refreshUsersModule: () => Promise<void>;
  usersPayload: UsersPayload;
  usersRows: UserRow[];
  roleReasonTemplates: string[];
  handleApplyUserRole: (
    telegramId: string,
    nextRole: UserRole,
    reasonHint?: string,
  ) => Promise<void>;
  refreshAuditModule: () => Promise<void>;
  runbookAction: (action: string, payload?: Record<string, unknown>) => Promise<void>;
  incidentsPayload: IncidentsPayload;
  incidentRows: IncidentRow[];
  runbookRows: RunbookRow[];
  auditRows: AuditRow[];
}
