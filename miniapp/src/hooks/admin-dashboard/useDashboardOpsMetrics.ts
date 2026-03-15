import { useMemo } from 'react';

import {
  computePercentile,
  formatTime,
  normalizeBridgeStatuses,
  toInt,
} from '@/services/admin-dashboard/dashboardPrimitives';

type CallStatsPayload = {
  total_calls?: unknown;
  completed_calls?: unknown;
  failed_calls?: unknown;
  success_rate?: unknown;
};

type OpsQueueBacklogPayload = {
  total?: unknown;
  dlq_call_open?: unknown;
  dlq_email_open?: unknown;
  sms_failed?: unknown;
  email_failed?: unknown;
};

type UseDashboardOpsMetricsOptions = {
  bridgePayload: unknown;
  sessionBlocked: boolean;
  pollFailureCount: number;
  pollDegradedFailures: number;
  error: string;
  hasBootstrapData: boolean;
  streamMode: 'disabled' | 'connecting' | 'connected' | 'fallback';
  streamConnected: boolean;
  streamLastEventAt: number | null;
  pollingPaused: boolean;
  nextPollAt: number | null;
  lastPollAt: number | null;
  lastSuccessfulPollAt: number | null;
  callStatsPayload: CallStatsPayload;
  opsQueueBacklog: OpsQueueBacklogPayload;
  actionLatencyMsSamples: number[];
};

type UseDashboardOpsMetricsResult = {
  bridgeHardFailures: number;
  bridgeSoftFailures: number;
  isDashboardDegraded: boolean;
  streamModeLabel: string;
  streamLastEventLabel: string;
  syncModeLabel: string;
  nextPollLabel: string;
  lastPollLabel: string;
  lastSuccessfulPollLabel: string;
  pollFreshnessLabel: string;
  uptimeScore: number;
  callTotal: number;
  callCompleted: number;
  callFailed: number;
  callFailureRate: number;
  callSuccessRate: number;
  queueBacklogTotal: number;
  sloErrorBudgetPercent: number;
  sloP95ActionLatencyMs: number;
  degradedCauses: string[];
};

export function useDashboardOpsMetrics({
  bridgePayload,
  sessionBlocked,
  pollFailureCount,
  pollDegradedFailures,
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
}: UseDashboardOpsMetricsOptions): UseDashboardOpsMetricsResult {
  return useMemo(() => {
    const bridgeStatuses = normalizeBridgeStatuses(bridgePayload);
    const bridgeHardFailures = bridgeStatuses.filter((status) => status >= 500).length;
    const bridgeSoftFailures = bridgeStatuses.filter((status) => status >= 400).length;

    const isDashboardDegraded = sessionBlocked
      || pollFailureCount >= pollDegradedFailures
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
    const callFailureRate = callTotal > 0 ? Math.round((callFailed / callTotal) * 100) : 0;

    const queueBacklogTotal = toInt(
      opsQueueBacklog.total,
      toInt(opsQueueBacklog.dlq_call_open)
        + toInt(opsQueueBacklog.dlq_email_open)
        + toInt(opsQueueBacklog.sms_failed)
        + toInt(opsQueueBacklog.email_failed),
    );

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
      pollFailureCount >= pollDegradedFailures ? `Poll failures ${pollFailureCount}` : '',
      bridgeHardFailures > 0 ? `Bridge hard failures ${bridgeHardFailures}` : '',
      bridgeSoftFailures > 0 ? `Bridge soft failures ${bridgeSoftFailures}` : '',
      (Boolean(error) && !hasBootstrapData) ? 'Bootstrap data unavailable' : '',
    ].filter(Boolean);

    return {
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
    };
  }, [
    actionLatencyMsSamples,
    bridgePayload,
    callStatsPayload.completed_calls,
    callStatsPayload.failed_calls,
    callStatsPayload.success_rate,
    callStatsPayload.total_calls,
    error,
    hasBootstrapData,
    lastPollAt,
    lastSuccessfulPollAt,
    nextPollAt,
    opsQueueBacklog.dlq_call_open,
    opsQueueBacklog.dlq_email_open,
    opsQueueBacklog.email_failed,
    opsQueueBacklog.sms_failed,
    opsQueueBacklog.total,
    pollDegradedFailures,
    pollFailureCount,
    pollingPaused,
    sessionBlocked,
    streamConnected,
    streamLastEventAt,
    streamMode,
  ]);
}
