function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

const DASHBOARD_PAYLOAD_KEYS = [
  'dashboard',
  'provider',
  'provider_compatibility',
  'module_layout',
  'modules',
  'feature_flags',
  'flags',
  'sms_bulk',
  'sms_stats',
  'email_bulk_stats',
  'email_bulk_history',
  'dlq',
  'call_logs',
  'call_scripts',
  'call_stats',
  'voice_runtime',
  'users',
  'audit',
  'incidents',
  'ops',
  'bridge',
  'session',
  'poll_interval_seconds',
  'poll_at',
  'server_time',
];

function hasKnownDashboardShape(payload: Record<string, unknown>): boolean {
  return DASHBOARD_PAYLOAD_KEYS.some((key) => key in payload);
}

function unwrapDashboardEnvelope(rawPayload: unknown): Record<string, unknown> {
  const payload = asRecord(rawPayload);
  if (Object.keys(payload).length === 0) {
    return payload;
  }

  for (const key of ['payload', 'data', 'result']) {
    const nested = asRecord(payload[key]);
    if (Object.keys(nested).length === 0) {
      continue;
    }
    const nestedDashboard = asRecord(nested.dashboard);
    if (hasKnownDashboardShape(nested) || hasKnownDashboardShape(nestedDashboard)) {
      return {
        ...payload,
        ...nested,
      };
    }
  }

  return payload;
}

function validateDashboardPayloadCommon(
  rawPayload: unknown,
  source: 'bootstrap' | 'poll' | 'stream',
): { ok: true; payload: Record<string, unknown> } | { ok: false; error: string } {
  const payload = unwrapDashboardEnvelope(rawPayload);
  if (Object.keys(payload).length === 0) {
    return { ok: false, error: `${source} payload is empty or not an object` };
  }

  if (payload.success === false) {
    const errorText = typeof payload.error === 'string' && payload.error.trim()
      ? payload.error.trim()
      : typeof payload.message === 'string' && payload.message.trim()
        ? payload.message.trim()
        : `${source} payload indicates an unsuccessful response`;
    return { ok: false, error: errorText };
  }

  const dashboard = asRecord(payload.dashboard);
  if ('dashboard' in payload
    && payload.dashboard !== undefined
    && payload.dashboard !== null
    && (typeof payload.dashboard !== 'object' || Array.isArray(payload.dashboard))) {
    return { ok: false, error: `${source} payload.dashboard is not a valid object` };
  }
  if (!hasKnownDashboardShape(payload) && !hasKnownDashboardShape(dashboard)) {
    return { ok: false, error: `${source} payload did not contain any known dashboard fields` };
  }
  return { ok: true, payload };
}

export function validateBootstrapPayload(
  rawPayload: unknown,
): { ok: true; payload: Record<string, unknown> } | { ok: false; error: string } {
  return validateDashboardPayloadCommon(rawPayload, 'bootstrap');
}

export function validatePollPayload(
  rawPayload: unknown,
): { ok: true; payload: Record<string, unknown> } | { ok: false; error: string } {
  return validateDashboardPayloadCommon(rawPayload, 'poll');
}

export function validateStreamPayload(
  rawPayload: unknown,
): { ok: true; payload: Record<string, unknown> } | { ok: false; error: string } {
  return validateDashboardPayloadCommon(rawPayload, 'stream');
}

export function validateActionEnvelope(
  rawPayload: unknown,
): { ok: true; payload: Record<string, unknown> } | { ok: false; error: string } {
  const payload = asRecord(rawPayload);
  if (Object.keys(payload).length === 0) {
    return { ok: false, error: 'Action response was empty or not an object' };
  }

  if (typeof payload.success !== 'boolean') {
    return { ok: false, error: 'Action response missing boolean success flag' };
  }

  if (payload.success === false) {
    const error = typeof payload.error === 'string' && payload.error.trim()
      ? payload.error.trim()
      : 'Action failed';
    return { ok: false, error };
  }

  return { ok: true, payload };
}
