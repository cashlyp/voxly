import { useEffect } from 'react';

import { getPollingDelayMs } from '@/services/admin-dashboard/dashboardPrimitives';

type UseDashboardPollingLoopOptions = {
  enabled: boolean;
  baseIntervalMs: number;
  loadPoll: () => Promise<boolean>;
  setNextPollAt: (value: number | null) => void;
  maxIntervalMs: number;
  backoffMultiplier: number;
  jitterMs: number;
};

export function useDashboardPollingLoop({
  enabled,
  baseIntervalMs,
  loadPoll,
  setNextPollAt,
  maxIntervalMs,
  backoffMultiplier,
  jitterMs,
}: UseDashboardPollingLoopOptions): void {
  useEffect(() => {
    if (!enabled) return undefined;

    let disposed = false;
    let timer: ReturnType<typeof setTimeout> | null = null;
    let consecutiveFailures = 0;

    const scheduleNext = (delayMs: number): void => {
      if (disposed) return;
      setNextPollAt(Date.now() + delayMs);
      timer = setTimeout(async () => {
        const ok = await loadPoll();
        if (disposed) return;
        consecutiveFailures = ok ? 0 : consecutiveFailures + 1;
        scheduleNext(getPollingDelayMs(baseIntervalMs, consecutiveFailures, {
          maxIntervalMs,
          backoffMultiplier,
          jitterMs,
        }));
      }, delayMs);
    };

    scheduleNext(baseIntervalMs);
    return () => {
      disposed = true;
      setNextPollAt(null);
      if (timer) {
        clearTimeout(timer);
      }
    };
  }, [
    backoffMultiplier,
    baseIntervalMs,
    enabled,
    jitterMs,
    loadPoll,
    maxIntervalMs,
    setNextPollAt,
  ]);
}
