import { asRecord } from '@/services/admin-dashboard/dashboardPrimitives';

export type ModuleLayoutRule = {
  enabled: boolean;
  order: number | null;
  label: string | null;
};

export function parseModuleId<T extends string>(value: unknown, allowed: Set<T>): T | null {
  if (typeof value !== 'string') return null;
  const normalized = value.trim().toLowerCase() as T;
  if (allowed.has(normalized)) {
    return normalized;
  }
  return null;
}

export function parseModuleLayoutConfig<T extends string>(
  layout: unknown,
  allowed: Set<T>,
): Partial<Record<T, ModuleLayoutRule>> {
  const root = asRecord(layout);
  const candidates = root.modules ?? layout;
  const rules: Partial<Record<T, ModuleLayoutRule>> = {};

  const applyRule = (entry: unknown, keyHint?: string): void => {
    if (typeof entry === 'string') {
      const id = parseModuleId(entry, allowed) || parseModuleId(keyHint, allowed);
      if (!id) return;
      rules[id] = { enabled: true, order: null, label: null };
      return;
    }
    if (typeof entry === 'boolean') {
      const id = parseModuleId(keyHint, allowed);
      if (!id) return;
      rules[id] = { enabled: entry, order: null, label: null };
      return;
    }
    if (typeof entry === 'number') {
      const id = parseModuleId(keyHint, allowed);
      if (!id) return;
      rules[id] = {
        enabled: true,
        order: Number.isFinite(entry) ? Math.floor(entry) : null,
        label: null,
      };
      return;
    }
    const record = asRecord(entry);
    const id = parseModuleId(record.id ?? record.module ?? record.key ?? keyHint, allowed);
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
