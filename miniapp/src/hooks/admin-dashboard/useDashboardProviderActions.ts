import { useCallback } from 'react';

import { asRecord, toText } from '@/services/admin-dashboard/dashboardPrimitives';

export type ProviderChannel = 'call' | 'sms' | 'email';

type ActivityStatus = 'info' | 'success' | 'error';
type ProviderSwitchStage = 'idle' | 'simulated' | 'confirmed' | 'applied' | 'failed';
type ProviderSwitchPostCheck = 'idle' | 'ok' | 'failed';

type ProviderSwitchPlanState = {
  target: string;
  stage: ProviderSwitchStage;
  postCheck: ProviderSwitchPostCheck;
  rollbackSuggestion: string;
};

type ProviderSwitchPlanStateMap = Record<ProviderChannel, ProviderSwitchPlanState>;
type ProviderChannelData = {
  provider?: unknown;
  supported_providers?: unknown;
  readiness?: unknown;
};

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

type UseDashboardProviderActionsOptions = {
  invokeAction: InvokeAction;
  runAction: RunAction;
  pushActivity: (status: ActivityStatus, title: string, detail: string) => void;
  triggerHaptic: (
    mode: 'selection' | 'impact' | 'success' | 'warning' | 'error',
    impactStyle?: 'light' | 'medium' | 'heavy' | 'rigid' | 'soft',
  ) => void;
  loadBootstrap: () => Promise<void>;
  setError: (message: string) => void;
  setNotice: (message: string) => void;
  setProviderPreflightBusy: (value: string) => void;
  setProviderPreflightRows: (
    value: Record<string, string> | ((prev: Record<string, string>) => Record<string, string>),
  ) => void;
  setProviderRollbackByChannel: (
    value:
    | Partial<Record<ProviderChannel, string>>
    | ((prev: Partial<Record<ProviderChannel, string>>) => Partial<Record<ProviderChannel, string>>),
  ) => void;
  setProviderSwitchPlanByChannel: (
    value: ProviderSwitchPlanStateMap | ((prev: ProviderSwitchPlanStateMap) => ProviderSwitchPlanStateMap),
  ) => void;
  providersByChannel: Partial<Record<ProviderChannel, ProviderChannelData>>;
  providerCurrentByChannel: Record<ProviderChannel, string>;
  providerSwitchPlanByChannel: ProviderSwitchPlanStateMap;
};

type UseDashboardProviderActionsResult = {
  preflightActiveProviders: () => Promise<void>;
  safeSwitchProvider: (
    channel: ProviderChannel,
    targetProvider: string,
    previousProvider: string,
  ) => Promise<void>;
  setProviderSwitchTarget: (channel: ProviderChannel, target: string) => void;
  simulateProviderSwitchPlan: (channel: ProviderChannel) => Promise<void>;
  confirmProviderSwitchPlan: (channel: ProviderChannel) => void;
  applyProviderSwitchPlan: (channel: ProviderChannel) => Promise<void>;
  resetProviderSwitchPlan: (channel: ProviderChannel) => void;
  runProviderPreflight: (channel: ProviderChannel, provider: string) => Promise<void>;
};

const CHANNELS: ProviderChannel[] = ['call', 'sms', 'email'];

export function useDashboardProviderActions({
  invokeAction,
  runAction,
  pushActivity,
  triggerHaptic,
  loadBootstrap,
  setError,
  setNotice,
  setProviderPreflightBusy,
  setProviderPreflightRows,
  setProviderRollbackByChannel,
  setProviderSwitchPlanByChannel,
  providersByChannel,
  providerCurrentByChannel,
  providerSwitchPlanByChannel,
}: UseDashboardProviderActionsOptions): UseDashboardProviderActionsResult {
  const runProviderPreflight = useCallback(async (channel: ProviderChannel, provider: string): Promise<void> => {
    const normalizedProvider = toText(provider, '').trim().toLowerCase();
    if (!normalizedProvider) {
      setError(`Select a provider for ${channel.toUpperCase()} preflight.`);
      return;
    }
    const key = `${channel}:${normalizedProvider}`;
    setProviderPreflightBusy(key);
    setError('');
    try {
      const result = await invokeAction('provider.preflight', {
        channel,
        provider: normalizedProvider,
        network: 1,
        reachability: 1,
      }) as Record<string, unknown>;
      const status = result?.success === true ? 'ok' : toText(result?.error, 'failed');
      setProviderPreflightRows((prev) => ({
        ...prev,
        [key]: status,
      }));
      pushActivity('success', 'Preflight completed', `${channel.toUpperCase()} ${normalizedProvider}: ${status}`);
    } catch (err) {
      const detail = err instanceof Error ? err.message : String(err);
      setProviderPreflightRows((prev) => ({
        ...prev,
        [key]: 'failed',
      }));
      pushActivity('error', 'Preflight failed', `${channel.toUpperCase()} ${normalizedProvider}: ${detail}`);
    } finally {
      setProviderPreflightBusy('');
    }
  }, [invokeAction, pushActivity, setError, setProviderPreflightBusy, setProviderPreflightRows]);

  const preflightActiveProviders = useCallback(async (): Promise<void> => {
    const targets = CHANNELS
      .map((channel) => {
        const provider = toText(asRecord(providersByChannel[channel]).provider, '').toLowerCase();
        return { channel, provider };
      })
      .filter((target) => Boolean(target.provider));
    if (targets.length === 0) {
      setError('No active providers available for preflight.');
      return;
    }
    setProviderPreflightBusy('all');
    setError('');
    setNotice('');
    try {
      for (const target of targets) {
        const key = `${target.channel}:${target.provider}`;
        const result = await invokeAction('provider.preflight', {
          channel: target.channel,
          provider: target.provider,
          network: 1,
          reachability: 1,
        }) as Record<string, unknown>;
        const status = result?.success === true ? 'ok' : toText(result?.error, 'failed');
        setProviderPreflightRows((prev) => ({
          ...prev,
          [key]: status,
        }));
      }
      const message = `Preflight completed for ${targets.length} active provider(s).`;
      setNotice(message);
      pushActivity('success', 'Provider preflight batch', message);
      await loadBootstrap();
    } catch (err) {
      const detail = err instanceof Error ? err.message : String(err);
      setError(detail);
      pushActivity('error', 'Provider preflight batch failed', detail);
    } finally {
      setProviderPreflightBusy('');
    }
  }, [
    invokeAction,
    loadBootstrap,
    providersByChannel,
    pushActivity,
    setError,
    setNotice,
    setProviderPreflightBusy,
    setProviderPreflightRows,
  ]);

  const safeSwitchProvider = useCallback(async (
    channel: ProviderChannel,
    targetProvider: string,
    previousProvider: string,
  ): Promise<void> => {
    const normalizedTarget = targetProvider.trim().toLowerCase();
    if (!normalizedTarget) return;
    if (typeof window !== 'undefined') {
      const proceed = window.confirm(
        `Run preflight and switch ${channel.toUpperCase()} provider to "${normalizedTarget}"?`,
      );
      if (!proceed) {
        triggerHaptic('warning');
        pushActivity('info', 'Provider switch cancelled', `${channel.toUpperCase()} switch was cancelled.`);
        return;
      }
    }
    triggerHaptic('impact', 'medium');
    const key = `${channel}:${normalizedTarget}`;
    setProviderPreflightBusy(key);
    setError('');
    setNotice('');
    try {
      await invokeAction('provider.preflight', {
        channel,
        provider: normalizedTarget,
        network: 1,
        reachability: 1,
      });
      setProviderPreflightRows((prev) => ({
        ...prev,
        [key]: 'ok',
      }));
      pushActivity('success', 'Preflight completed', `${channel.toUpperCase()} ${normalizedTarget} is ready.`);
      if (previousProvider) {
        setProviderRollbackByChannel((prev) => ({
          ...prev,
          [channel]: previousProvider,
        }));
      }
      await runAction(
        'provider.set',
        { channel, provider: normalizedTarget },
        { successMessage: `${channel.toUpperCase()} provider switched to ${normalizedTarget}.` },
      );
    } catch (err) {
      const detail = err instanceof Error ? err.message : String(err);
      setProviderPreflightRows((prev) => ({
        ...prev,
        [key]: 'failed',
      }));
      setError(detail);
      pushActivity('error', 'Safe switch blocked', `${channel.toUpperCase()} ${normalizedTarget}: ${detail}`);
    } finally {
      setProviderPreflightBusy('');
    }
  }, [
    invokeAction,
    pushActivity,
    runAction,
    setError,
    setNotice,
    setProviderPreflightBusy,
    setProviderPreflightRows,
    setProviderRollbackByChannel,
    triggerHaptic,
  ]);

  const setProviderSwitchTarget = useCallback((channel: ProviderChannel, target: string): void => {
    const normalized = String(target || '').trim().toLowerCase();
    setProviderSwitchPlanByChannel((prev) => ({
      ...prev,
      [channel]: {
        ...prev[channel],
        target: normalized,
        stage: normalized ? 'idle' : prev[channel].stage,
        postCheck: 'idle',
      },
    }));
  }, [setProviderSwitchPlanByChannel]);

  const simulateProviderSwitchPlan = useCallback(async (channel: ProviderChannel): Promise<void> => {
    const target = toText(providerSwitchPlanByChannel[channel]?.target, '').toLowerCase();
    if (!target) {
      setError(`Select a target provider for ${channel.toUpperCase()} simulation.`);
      return;
    }
    const preflightKey = `${channel}:${target}:plan`;
    setProviderPreflightBusy(preflightKey);
    setError('');
    try {
      const result = await invokeAction('provider.preflight', {
        channel,
        provider: target,
        network: 1,
        reachability: 1,
      }) as Record<string, unknown>;
      const ok = result?.success === true || toText(result?.error, '') === '';
      const rollbackTarget = providerCurrentByChannel[channel];
      setProviderSwitchPlanByChannel((prev) => ({
        ...prev,
        [channel]: {
          target,
          stage: ok ? 'simulated' : 'failed',
          postCheck: 'idle',
          rollbackSuggestion: rollbackTarget || '',
        },
      }));
      if (ok) {
        pushActivity('success', 'Provider switch simulated', `${channel.toUpperCase()} ${target} passed preflight.`);
      } else {
        pushActivity('error', 'Provider simulation failed', `${channel.toUpperCase()} ${target} failed preflight.`);
      }
    } catch (err) {
      const detail = err instanceof Error ? err.message : String(err);
      setError(detail);
      setProviderSwitchPlanByChannel((prev) => ({
        ...prev,
        [channel]: {
          ...prev[channel],
          stage: 'failed',
          postCheck: 'idle',
        },
      }));
      pushActivity('error', 'Provider simulation failed', detail);
    } finally {
      setProviderPreflightBusy('');
    }
  }, [
    invokeAction,
    providerCurrentByChannel,
    providerSwitchPlanByChannel,
    pushActivity,
    setError,
    setProviderPreflightBusy,
    setProviderSwitchPlanByChannel,
  ]);

  const confirmProviderSwitchPlan = useCallback((channel: ProviderChannel): void => {
    setProviderSwitchPlanByChannel((prev) => {
      const plan = prev[channel];
      if (!plan.target || plan.stage !== 'simulated') return prev;
      return {
        ...prev,
        [channel]: {
          ...plan,
          stage: 'confirmed',
        },
      };
    });
    pushActivity('info', 'Provider switch confirmed', `${channel.toUpperCase()} switch plan confirmed.`);
  }, [pushActivity, setProviderSwitchPlanByChannel]);

  const applyProviderSwitchPlan = useCallback(async (channel: ProviderChannel): Promise<void> => {
    const plan = providerSwitchPlanByChannel[channel];
    const target = toText(plan?.target, '').toLowerCase();
    if (!target || plan?.stage !== 'confirmed') {
      setError(`Simulate and confirm ${channel.toUpperCase()} plan before apply.`);
      return;
    }
    if (typeof window !== 'undefined') {
      const approved = window.confirm(`Apply ${channel.toUpperCase()} provider switch to "${target}" now?`);
      if (!approved) {
        pushActivity('info', 'Provider switch cancelled', `${channel.toUpperCase()} switch apply was cancelled.`);
        return;
      }
    }
    const previousProvider = providerCurrentByChannel[channel];
    await runAction(
      'provider.set',
      { channel, provider: target },
      {
        successMessage: `${channel.toUpperCase()} provider switched to ${target}.`,
      },
    );
    try {
      const postCheck = await invokeAction('provider.preflight', {
        channel,
        provider: target,
        network: 1,
        reachability: 1,
      }) as Record<string, unknown>;
      const healthy = postCheck?.success === true || toText(postCheck?.error, '') === '';
      const rollbackSuggestion = healthy ? '' : previousProvider;
      setProviderSwitchPlanByChannel((prev) => ({
        ...prev,
        [channel]: {
          ...prev[channel],
          stage: healthy ? 'applied' : 'failed',
          postCheck: healthy ? 'ok' : 'failed',
          rollbackSuggestion,
        },
      }));
      if (healthy) {
        pushActivity('success', 'Post-switch health check', `${channel.toUpperCase()} ${target} is healthy.`);
      } else {
        const msg = `${channel.toUpperCase()} ${target} post-check failed.${rollbackSuggestion ? ` Suggested rollback: ${rollbackSuggestion}` : ''}`;
        setNotice(msg);
        pushActivity('error', 'Post-switch health check failed', msg);
      }
    } catch (err) {
      const detail = err instanceof Error ? err.message : String(err);
      setProviderSwitchPlanByChannel((prev) => ({
        ...prev,
        [channel]: {
          ...prev[channel],
          stage: 'failed',
          postCheck: 'failed',
          rollbackSuggestion: previousProvider || '',
        },
      }));
      setError(detail);
      pushActivity('error', 'Post-switch verification failed', detail);
    }
  }, [
    invokeAction,
    providerCurrentByChannel,
    providerSwitchPlanByChannel,
    pushActivity,
    runAction,
    setError,
    setNotice,
    setProviderSwitchPlanByChannel,
  ]);

  const resetProviderSwitchPlan = useCallback((channel: ProviderChannel): void => {
    setProviderSwitchPlanByChannel((prev) => ({
      ...prev,
      [channel]: {
        target: '',
        stage: 'idle',
        postCheck: 'idle',
        rollbackSuggestion: '',
      },
    }));
  }, [setProviderSwitchPlanByChannel]);

  return {
    preflightActiveProviders,
    safeSwitchProvider,
    setProviderSwitchTarget,
    simulateProviderSwitchPlan,
    confirmProviderSwitchPlan,
    applyProviderSwitchPlan,
    resetProviderSwitchPlan,
    runProviderPreflight,
  };
}
