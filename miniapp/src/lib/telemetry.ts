import { apiFetch } from './api';

type TelemetryEvent = {
  name: string;
  ts: string;
  data?: Record<string, unknown>;
};

type TelemetryContext = {
  role?: string;
  environment?: string | null;
  tenant_id?: string | null;
};

const STORAGE_KEY = 'voicednut.telemetry.session';
const QUEUE: TelemetryEvent[] = [];
let flushTimer: number | null = null;
let context: TelemetryContext = {};

function getSessionId() {
  if (typeof sessionStorage === 'undefined') return 'unknown';
  const existing = sessionStorage.getItem(STORAGE_KEY);
  if (existing) return existing;
  const id = typeof crypto !== 'undefined' && 'randomUUID' in crypto
    ? crypto.randomUUID()
    : `sess_${Date.now()}_${Math.random().toString(36).slice(2)}`;
  sessionStorage.setItem(STORAGE_KEY, id);
  return id;
}

function scheduleFlush() {
  if (flushTimer) return;
  flushTimer = window.setTimeout(() => {
    flushTimer = null;
    void flushTelemetry();
  }, 10000);
}

export function setTelemetryContext(next: TelemetryContext) {
  context = { ...context, ...next };
}

export function trackEvent(name: string, data?: Record<string, unknown>) {
  QUEUE.push({
    name,
    ts: new Date().toISOString(),
    data: {
      ...data,
      route: window.location.hash || '/',
    },
  });
  if (QUEUE.length >= 10) {
    void flushTelemetry();
  } else {
    scheduleFlush();
  }
}

export async function flushTelemetry() {
  if (!QUEUE.length) return;
  const payload = {
    session_id: getSessionId(),
    context,
    events: QUEUE.splice(0, QUEUE.length),
  };
  try {
    await apiFetch('/webapp/telemetry', {
      method: 'POST',
      body: payload,
    });
  } catch {
    // drop telemetry errors
  }
}
