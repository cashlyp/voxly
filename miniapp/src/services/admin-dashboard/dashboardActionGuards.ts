import { toText } from '@/services/admin-dashboard/dashboardPrimitives';

function isNonEmptyString(value: unknown): boolean {
  return typeof value === 'string' && value.trim().length > 0;
}

function isStringArray(value: unknown): value is string[] {
  return Array.isArray(value) && value.every((entry) => typeof entry === 'string');
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

function validateProviderAction(payload: Record<string, unknown>): string | null {
  if (!isNonEmptyString(payload.channel)) return 'channel is required';
  if (!isNonEmptyString(payload.provider)) return 'provider is required';
  return null;
}

function validateSmsBulkSend(payload: Record<string, unknown>): string | null {
  const recipients = payload.recipients;
  if (!isStringArray(recipients) || recipients.length === 0) {
    return 'recipients must be a non-empty string array';
  }
  if (!isNonEmptyString(payload.message)) return 'message is required';
  return null;
}

function validateSmsScheduleSend(payload: Record<string, unknown>): string | null {
  if (!isNonEmptyString(payload.to)) return 'to is required';
  if (!isNonEmptyString(payload.message)) return 'message is required';
  if (!isNonEmptyString(payload.scheduled_time)) return 'scheduled_time is required';
  return null;
}

function validateEmailBulkSend(payload: Record<string, unknown>): string | null {
  if (!Array.isArray(payload.recipients) || payload.recipients.length === 0) {
    return 'recipients must be a non-empty array';
  }
  if (
    payload.script_id === undefined
    && payload.subject === undefined
    && payload.html === undefined
    && payload.text === undefined
  ) {
    return 'provide script_id or subject/html/text content';
  }
  return null;
}

function validateUserRoleSet(payload: Record<string, unknown>): string | null {
  if (!isNonEmptyString(payload.telegram_id)) return 'telegram_id is required';
  const role = toText(payload.role, '').toLowerCase();
  if (!['admin', 'operator', 'viewer'].includes(role)) {
    return 'role must be admin, operator, or viewer';
  }
  if (!isNonEmptyString(payload.reason)) return 'reason is required';
  return null;
}

function validateCallScriptIdPayload(payload: Record<string, unknown>): string | null {
  const id = Number(payload.id);
  if (!Number.isFinite(id) || id <= 0) return 'id must be a positive number';
  return null;
}

const ACTION_GUARDS: Record<string, (payload: Record<string, unknown>) => string | null> = {
  'provider.set': validateProviderAction,
  'provider.rollback': validateProviderAction,
  'provider.preflight': validateProviderAction,
  'sms.bulk.send': validateSmsBulkSend,
  'sms.schedule.send': validateSmsScheduleSend,
  'email.bulk.send': validateEmailBulkSend,
  'users.role.set': validateUserRoleSet,
  'callscript.update': validateCallScriptIdPayload,
  'callscript.submit_review': validateCallScriptIdPayload,
  'callscript.review': validateCallScriptIdPayload,
  'callscript.promote_live': validateCallScriptIdPayload,
  'callscript.simulate': validateCallScriptIdPayload,
};

export function validateDashboardActionPayload(action: string, payload: unknown): string | null {
  if (!isRecord(payload)) {
    return 'payload must be an object';
  }
  const normalizedAction = String(action || '').trim().toLowerCase();
  const guard = ACTION_GUARDS[normalizedAction];
  if (!guard) return null;
  return guard(payload);
}
