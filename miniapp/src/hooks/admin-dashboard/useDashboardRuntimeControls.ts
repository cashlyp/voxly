import { useCallback } from 'react';

type ActivityStatus = 'info' | 'success' | 'error';

type InvokeAction = (
  action: string,
  payload: Record<string, unknown>,
  metaOverride?: Record<string, unknown>,
) => Promise<unknown>;

type RunAction = (
  action: string,
  payload: Record<string, unknown>,
  options?: { confirmText?: string; successMessage?: string },
) => Promise<void>;

type VoiceRuntimePayload = {
  runtime?: unknown;
  active_calls?: unknown;
  actions?: unknown;
  applied?: unknown;
};

type UseDashboardRuntimeControlsOptions = {
  invokeAction: InvokeAction;
  runAction: RunAction;
  pushActivity: (status: ActivityStatus, title: string, detail: string) => void;
  setError: (message: string) => void;
  setRuntimeCanaryInput: (value: string) => void;
  runtimeCanaryInput: string;
  onRuntimeStatusLoaded: (payload: VoiceRuntimePayload) => void;
};

type UseDashboardRuntimeControlsResult = {
  refreshRuntimeStatus: () => Promise<void>;
  enableRuntimeMaintenance: () => Promise<void>;
  disableRuntimeMaintenance: () => Promise<void>;
  applyRuntimeCanary: () => Promise<void>;
  clearRuntimeCanary: () => Promise<void>;
};

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

export function useDashboardRuntimeControls({
  invokeAction,
  runAction,
  pushActivity,
  setError,
  setRuntimeCanaryInput,
  runtimeCanaryInput,
  onRuntimeStatusLoaded,
}: UseDashboardRuntimeControlsOptions): UseDashboardRuntimeControlsResult {
  const refreshRuntimeStatus = useCallback(async (): Promise<void> => {
    try {
      const data = await invokeAction('runtime.status', {}) as VoiceRuntimePayload;
      onRuntimeStatusLoaded(data);
      const canaryOverride = Number(asRecord(data?.runtime).canary_percent_override);
      if (Number.isFinite(canaryOverride)) {
        setRuntimeCanaryInput(String(Math.max(0, Math.min(100, Math.round(canaryOverride)))));
      }
      pushActivity('success', 'Runtime refreshed', 'Voice runtime status updated.');
    } catch (err) {
      const detail = err instanceof Error ? err.message : String(err);
      setError(detail);
      pushActivity('error', 'Runtime refresh failed', detail);
    }
  }, [invokeAction, onRuntimeStatusLoaded, pushActivity, setError, setRuntimeCanaryInput]);

  const enableRuntimeMaintenance = useCallback(async (): Promise<void> => {
    await runAction(
      'runtime.maintenance.enable',
      { duration_ms: 15 * 60 * 1000 },
      {
        confirmText: 'Enable maintenance mode (legacy-only) for 15 minutes?',
        successMessage: 'Maintenance mode enabled for 15 minutes.',
      },
    );
  }, [runAction]);

  const disableRuntimeMaintenance = useCallback(async (): Promise<void> => {
    await runAction(
      'runtime.maintenance.disable',
      {},
      {
        confirmText: 'Disable maintenance mode and reset runtime circuit now?',
        successMessage: 'Maintenance mode disabled and runtime circuit reset.',
      },
    );
  }, [runAction]);

  const applyRuntimeCanary = useCallback(async (): Promise<void> => {
    const parsed = Number(runtimeCanaryInput);
    if (!Number.isFinite(parsed) || parsed < 0 || parsed > 100) {
      setError('Canary override must be a number between 0 and 100.');
      return;
    }
    await runAction(
      'runtime.canary.set',
      { canary_percent: Math.round(parsed) },
      {
        confirmText: `Set runtime canary override to ${Math.round(parsed)}%?`,
        successMessage: `Runtime canary override set to ${Math.round(parsed)}%.`,
      },
    );
  }, [runAction, runtimeCanaryInput, setError]);

  const clearRuntimeCanary = useCallback(async (): Promise<void> => {
    await runAction(
      'runtime.canary.clear',
      {},
      {
        confirmText: 'Clear runtime canary override and return to configured value?',
        successMessage: 'Runtime canary override cleared.',
      },
    );
    setRuntimeCanaryInput('');
  }, [runAction, setRuntimeCanaryInput]);

  return {
    refreshRuntimeStatus,
    enableRuntimeMaintenance,
    disableRuntimeMaintenance,
    applyRuntimeCanary,
    clearRuntimeCanary,
  };
}
