import { useCallback, useMemo } from 'react';

import {
  asRecord,
  formatTime,
  parseFeatureFlags,
} from '@/services/admin-dashboard/dashboardPrimitives';

export type FeatureFlagRegistryEntry = {
  key: string;
  defaultEnabled: boolean;
  description: string;
};

export type FeatureFlagInspectorItem = {
  key: string;
  enabled: boolean;
  source: 'server' | 'default';
  defaultEnabled: boolean;
  description: string;
};

export type FeatureFlagsPayloadSource =
  | 'poll.feature_flags'
  | 'poll.dashboard.feature_flags'
  | 'bootstrap.feature_flags'
  | 'dashboard.feature_flags'
  | 'poll.flags'
  | 'poll.dashboard.flags'
  | 'bootstrap.flags'
  | 'dashboard.flags'
  | 'default';

type FeatureFlagsEnvelope = {
  feature_flags?: unknown;
  flags?: unknown;
  poll_at?: unknown;
  server_time?: unknown;
  dashboard?: {
    feature_flags?: unknown;
    flags?: unknown;
    poll_at?: unknown;
    server_time?: unknown;
  };
};

type UseDashboardFeatureFlagsOptions = {
  pollPayload?: FeatureFlagsEnvelope | null;
  bootstrap?: FeatureFlagsEnvelope | null;
  registry: FeatureFlagRegistryEntry[];
};

type UseDashboardFeatureFlagsResult = {
  featureFlags: Record<string, boolean>;
  featureFlagsSourceLabel: FeatureFlagsPayloadSource;
  featureFlagsUpdatedAtLabel: string;
  featureFlagInspectorItems: FeatureFlagInspectorItem[];
  isFeatureEnabled: (flag: string, fallback?: boolean) => boolean;
  realtimeStreamEnabled: boolean;
  moduleSkeletonsEnabled: boolean;
  moduleErrorBoundariesEnabled: boolean;
};

export function useDashboardFeatureFlags({
  pollPayload,
  bootstrap,
  registry,
}: UseDashboardFeatureFlagsOptions): UseDashboardFeatureFlagsResult {
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

  const registryKeys = useMemo(
    () => new Set<string>(registry.map((entry) => entry.key)),
    [registry],
  );

  const featureFlagInspectorItems = useMemo<FeatureFlagInspectorItem[]>(() => {
    const known = registry.map((entry) => {
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
      .filter(([key]) => !registryKeys.has(key))
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([key, enabled]) => ({
        key,
        enabled,
        source: 'server' as const,
        defaultEnabled: Boolean(enabled),
        description: 'Server-defined feature flag.',
      }));
    return [...known, ...dynamic];
  }, [featureFlags, registry, registryKeys]);

  return {
    featureFlags,
    featureFlagsSourceLabel,
    featureFlagsUpdatedAtLabel,
    featureFlagInspectorItems,
    isFeatureEnabled,
    realtimeStreamEnabled: isFeatureEnabled('realtime_stream', true),
    moduleSkeletonsEnabled: isFeatureEnabled('module_skeletons', true),
    moduleErrorBoundariesEnabled: isFeatureEnabled('module_error_boundaries', true),
  };
}
