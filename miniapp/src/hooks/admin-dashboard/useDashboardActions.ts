import { useCallback, useState } from 'react';
import type { Dispatch, SetStateAction } from 'react';

import { asRecord } from '@/services/admin-dashboard/dashboardPrimitives';
import { validateActionEnvelope } from '@/services/admin-dashboard/dashboardApiContracts';
import { validateDashboardActionPayload } from '@/services/admin-dashboard/dashboardActionGuards';

export type ActionRequestMeta = {
  action_id: string;
  request_id: string;
  idempotency_key: string;
  requested_at: string;
  request_timeout_ms: number;
  source: string;
  ui_module: string;
};

export type ActionTelemetry = {
  traceHint: string;
  action: string;
  status: 'idle' | 'running' | 'success' | 'error';
  at: string;
  latencyMs: number;
  error: string;
};

type ActivityStatus = 'info' | 'success' | 'error';

type RunActionOptions = {
  confirmText?: string;
  successMessage?: string;
};

type UseDashboardActionsOptions = {
  createActionMeta: (action: string) => ActionRequestMeta;
  request: <T>(path: string, options?: RequestInit) => Promise<T>;
  loadBootstrap: () => Promise<void> | void;
  pushActivity: (status: ActivityStatus, title: string, detail: string) => void;
  triggerHaptic: (
    mode: 'selection' | 'impact' | 'success' | 'warning' | 'error',
    impactStyle?: 'light' | 'medium' | 'heavy' | 'rigid' | 'soft',
  ) => void;
  setBusyAction: (value: string) => void;
  setNotice: (value: string) => void;
  setError: (value: string) => void;
  setActionLatencyMsSamples: Dispatch<SetStateAction<number[]>>;
  actionLatencySampleLimit: number;
};

type UseDashboardActionsResult = {
  invokeAction: (
    action: string,
    payload: Record<string, unknown>,
    metaOverride?: Partial<ActionRequestMeta>,
  ) => Promise<unknown>;
  runAction: (
    action: string,
    payload: Record<string, unknown>,
    options?: RunActionOptions,
  ) => Promise<void>;
  actionTelemetry: ActionTelemetry;
};

export function useDashboardActions({
  createActionMeta,
  request,
  loadBootstrap,
  pushActivity,
  triggerHaptic,
  setBusyAction,
  setNotice,
  setError,
  setActionLatencyMsSamples,
  actionLatencySampleLimit,
}: UseDashboardActionsOptions): UseDashboardActionsResult {
  const [actionTelemetry, setActionTelemetry] = useState<ActionTelemetry>({
    traceHint: '',
    action: '',
    status: 'idle',
    at: '',
    latencyMs: 0,
    error: '',
  });

  const invokeAction = useCallback(async (
    action: string,
    payload: Record<string, unknown>,
    metaOverride?: Partial<ActionRequestMeta>,
  ): Promise<unknown> => {
    const actionMeta: ActionRequestMeta = {
      ...createActionMeta(action),
      ...metaOverride,
    };
    const guardError = validateDashboardActionPayload(action, payload);
    if (guardError) {
      throw new Error(`Invalid payload for ${action}: ${guardError}`);
    }

    const startedAt = Date.now();
    const traceHint = actionMeta.action_id.slice(-8);
    setActionTelemetry({
      traceHint,
      action,
      status: 'running',
      at: new Date(startedAt).toISOString(),
      latencyMs: 0,
      error: '',
    });

    const abortController = new AbortController();
    const timeout = setTimeout(() => abortController.abort(), actionMeta.request_timeout_ms);
    try {
      const rawResult = await request<{ success?: boolean; data?: unknown; error?: string } | null>('/miniapp/action', {
        method: 'POST',
        signal: abortController.signal,
        headers: {
          'X-Action-Id': actionMeta.action_id,
          'X-Idempotency-Key': actionMeta.idempotency_key,
        },
        body: JSON.stringify({ action, payload, meta: actionMeta }),
      });
      const envelopeCheck = validateActionEnvelope(rawResult);
      if (!envelopeCheck.ok) {
        throw new Error(envelopeCheck.error);
      }
      const result = asRecord(envelopeCheck.payload);
      const latencyMs = Math.max(0, Date.now() - startedAt);
      setActionTelemetry({
        traceHint,
        action,
        status: 'success',
        at: new Date().toISOString(),
        latencyMs,
        error: '',
      });
      return result.data;
    } catch (err) {
      const isAbortError = err instanceof DOMException && err.name === 'AbortError';
      const detail = isAbortError
        ? `Action timed out after ${actionMeta.request_timeout_ms}ms`
        : err instanceof Error
          ? err.message
          : String(err);
      const latencyMs = Math.max(0, Date.now() - startedAt);
      setActionTelemetry({
        traceHint,
        action,
        status: 'error',
        at: new Date().toISOString(),
        latencyMs,
        error: detail,
      });
      if (isAbortError) {
        throw new Error(detail);
      }
      throw new Error(detail);
    } finally {
      clearTimeout(timeout);
    }
  }, [createActionMeta, request]);

  const runAction = useCallback(async (
    action: string,
    payload: Record<string, unknown>,
    options: RunActionOptions = {},
  ): Promise<void> => {
    const actionMeta = createActionMeta(action);
    const traceHint = actionMeta.action_id.slice(-8);
    if (options.confirmText && typeof window !== 'undefined') {
      const allowed = window.confirm(options.confirmText);
      if (!allowed) {
        triggerHaptic('warning');
        pushActivity('info', 'Action cancelled', `Cancelled: ${action} (trace:${traceHint})`);
        return;
      }
    }

    setBusyAction(action);
    setNotice('');
    setError('');
    const startedAt = Date.now();
    try {
      await invokeAction(action, payload, actionMeta);
      const successMessage = options.successMessage || `Action completed: ${action}`;
      setNotice(`${successMessage} [${traceHint}]`);
      triggerHaptic('success');
      pushActivity('success', 'Action completed', `${successMessage} (trace:${traceHint})`);
      await Promise.resolve(loadBootstrap());
    } catch (err) {
      const detail = err instanceof Error ? err.message : String(err);
      setError(detail);
      triggerHaptic('error');
      pushActivity('error', `Action failed: ${action}`, `${detail} (trace:${traceHint})`);
    } finally {
      const latencyMs = Math.max(0, Date.now() - startedAt);
      setActionLatencyMsSamples((prev) => [...prev, latencyMs].slice(-actionLatencySampleLimit));
      setBusyAction('');
    }
  }, [
    actionLatencySampleLimit,
    createActionMeta,
    invokeAction,
    loadBootstrap,
    pushActivity,
    setActionLatencyMsSamples,
    setBusyAction,
    setError,
    setNotice,
    triggerHaptic,
  ]);

  return {
    invokeAction,
    runAction,
    actionTelemetry,
  };
}
