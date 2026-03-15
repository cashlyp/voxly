export type JsonMap = Record<string, unknown>;

export function textBar(percent: number, width = 10): string {
  const safePercent = Number.isFinite(percent)
    ? Math.max(0, Math.min(100, Math.round(percent)))
    : 0;
  const filled = Math.round((safePercent / 100) * width);
  return `${'█'.repeat(filled)}${'░'.repeat(Math.max(0, width - filled))} ${safePercent}%`;
}

export function normalizePhone(value: string): string {
  return value.replace(/[^\d+]/g, '');
}

export function parsePhoneList(raw: string): string[] {
  return Array.from(
    new Set(
      String(raw || '')
        .split(/[\n,;\t ]+/g)
        .map((entry) => normalizePhone(entry.trim()))
        .filter(Boolean),
    ),
  );
}

export function parseEmailList(raw: string): string[] {
  return Array.from(
    new Set(
      String(raw || '')
        .split(/[\n,;\t ]+/g)
        .map((entry) => entry.trim().toLowerCase())
        .filter(Boolean),
    ),
  );
}

export function isValidE164(phone: string): boolean {
  return /^\+[1-9]\d{1,14}$/.test(phone);
}

export function isLikelyEmail(email: string): boolean {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
}

export function estimateSmsSegments(text: string): { segments: number; perSegment: number } {
  const body = String(text || '');
  if (!body) return { segments: 0, perSegment: 160 };
  const hasUnicode = Array.from(body).some((char) => char.charCodeAt(0) > 127);
  const single = hasUnicode ? 70 : 160;
  const multi = hasUnicode ? 67 : 153;
  if (body.length <= single) return { segments: 1, perSegment: single };
  return { segments: Math.ceil(body.length / multi), perSegment: multi };
}

export function computePercentile(values: number[], percentile: number): number {
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

export function renderTemplateString(template: string, variables: Record<string, unknown>): string {
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

export function toInt(value: unknown, fallback = 0): number {
  const num = Number(value);
  return Number.isFinite(num) ? Math.floor(num) : fallback;
}

export function toText(value: unknown, fallback = 'unknown'): string {
  if (typeof value === 'string' && value.trim()) return value;
  if (typeof value === 'number' || typeof value === 'boolean' || typeof value === 'bigint') {
    return String(value);
  }
  return fallback;
}

export function formatTime(value: unknown): string {
  if (!value) return '—';
  const valueText = toText(value, '');
  if (!valueText) return '—';
  const parsed = new Date(valueText);
  if (Number.isNaN(parsed.getTime())) return toText(value, '—');
  return parsed.toLocaleString();
}

export function asRecord(value: unknown): JsonMap {
  return value && typeof value === 'object' && !Array.isArray(value)
    ? (value as JsonMap)
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

export function parseFeatureFlags(value: unknown): Record<string, boolean> {
  const flags: Record<string, boolean> = {};
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

export function getPollingDelayMs(
  baseIntervalMs: number,
  consecutiveFailures: number,
  options: {
    minIntervalMs?: number;
    maxIntervalMs: number;
    backoffMultiplier: number;
    jitterMs: number;
  },
): number {
  const minIntervalMs = Number.isFinite(options.minIntervalMs)
    ? Number(options.minIntervalMs)
    : 3000;
  const safeBase = Math.max(minIntervalMs, Math.min(options.maxIntervalMs, Math.floor(baseIntervalMs)));
  const backoff = consecutiveFailures > 0
    ? Math.min(
      options.maxIntervalMs,
      Math.round(safeBase * Math.pow(options.backoffMultiplier, consecutiveFailures)),
    )
    : safeBase;
  const jitter = Math.floor(Math.random() * (options.jitterMs + 1));
  return backoff + jitter;
}

export function normalizeBridgeStatuses(value: unknown): number[] {
  const bridge = asRecord(value);
  return Object.values(bridge)
    .map((status) => Number(status))
    .filter((status) => Number.isFinite(status) && status >= 100 && status <= 599);
}

export async function parseJsonResponse(response: Response): Promise<unknown> {
  return response.json().catch(() => null);
}

export function parseApiError(payload: unknown, status: number): string {
  const body = asRecord(payload);
  return toText(body.error, '') || toText(body.message, '') || `Request failed (${status})`;
}

export function parseApiErrorCode(payload: unknown): string {
  const body = asRecord(payload);
  return toText(body.code, '');
}
