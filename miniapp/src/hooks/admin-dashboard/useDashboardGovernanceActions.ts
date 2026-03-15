import { useCallback } from 'react';

import { asRecord, toText } from '@/services/admin-dashboard/dashboardPrimitives';

const USER_ROLE_REASON_TEMPLATES = [
  'Policy update',
  'On-call rotation',
  'Temporary escalation',
  'Incident response',
  'Access requested by owner',
] as const;

type ActivityStatus = 'info' | 'success' | 'error';
type ProviderChannel = 'call' | 'sms' | 'email';

type ProviderChannelData = {
  provider?: unknown;
  supported_providers?: unknown;
  readiness?: unknown;
};

type RunAction = (
  action: string,
  payload: Record<string, unknown>,
  options?: { confirmText?: string; successMessage?: string },
) => Promise<void>;

type UseDashboardGovernanceActionsOptions = {
  runAction: RunAction;
  pushActivity: (status: ActivityStatus, title: string, detail: string) => void;
  refreshUsersModule: () => Promise<void>;
  refreshAuditModule: () => Promise<void>;
  providersByChannel: Partial<Record<ProviderChannel, ProviderChannelData>>;
};

type UseDashboardGovernanceActionsResult = {
  roleReasonTemplates: string[];
  handleApplyUserRole: (
    telegramId: string,
    role: 'admin' | 'operator' | 'viewer',
    reasonHint?: string,
  ) => Promise<void>;
  runbookAction: (action: string, payload?: Record<string, unknown>) => Promise<void>;
};

const CHANNELS: ProviderChannel[] = ['call', 'sms', 'email'];

export function useDashboardGovernanceActions({
  runAction,
  pushActivity,
  refreshUsersModule,
  refreshAuditModule,
  providersByChannel,
}: UseDashboardGovernanceActionsOptions): UseDashboardGovernanceActionsResult {
  const handleApplyUserRole = useCallback(async (
    telegramId: string,
    role: 'admin' | 'operator' | 'viewer',
    reasonHint = '',
  ): Promise<void> => {
    const reasonInput = typeof window !== 'undefined'
      ? (window.prompt(
        `Provide audit reason for ${telegramId} -> ${role}`,
        reasonHint || '',
      ) || '').trim()
      : reasonHint.trim();
    const reason = reasonInput || reasonHint.trim();
    if (!reason) {
      pushActivity('info', 'Role update cancelled', `No audit reason supplied for ${telegramId}.`);
      return;
    }
    if (role === 'admin' && typeof window !== 'undefined') {
      const policyAck = (window.prompt(
        'Two-step approval required. Type "APPROVE ADMIN" to continue.',
        '',
      ) || '').trim().toUpperCase();
      if (policyAck !== 'APPROVE ADMIN') {
        pushActivity('info', 'Admin elevation blocked', `${telegramId} elevation not approved.`);
        return;
      }
    }
    await runAction(
      'users.role.set',
      { telegram_id: telegramId, role, reason },
      {
        confirmText: `Set role for ${telegramId} to ${role}?`,
        successMessage: `Updated ${telegramId} role to ${role}.`,
      },
    );
    await refreshUsersModule();
  }, [pushActivity, refreshUsersModule, runAction]);

  const runbookAction = useCallback(async (
    action: string,
    payload: Record<string, unknown> = {},
  ): Promise<void> => {
    const normalizedAction = String(action || '').trim().toLowerCase();
    let nextPayload: Record<string, unknown> = { ...payload };
    if (normalizedAction === 'runbook.provider.preflight') {
      const selectedChannel = toText(nextPayload.channel, 'call').toLowerCase();
      const normalizedChannel = CHANNELS.includes(selectedChannel as ProviderChannel)
        ? selectedChannel as ProviderChannel
        : 'call';
      const fallbackProvider = toText(
        asRecord(providersByChannel[normalizedChannel]).provider,
        '',
      ).toLowerCase();
      const selectedProvider = toText(nextPayload.provider, fallbackProvider).toLowerCase();
      if (!selectedProvider) {
        pushActivity('error', 'Runbook blocked', 'No active provider available for preflight runbook.');
        return;
      }
      nextPayload = {
        ...nextPayload,
        channel: normalizedChannel,
        provider: selectedProvider,
      };
    }
    await runAction(action, nextPayload, {
      confirmText: `Execute runbook action "${action}"?`,
      successMessage: `Runbook executed: ${action}`,
    });
    await refreshAuditModule();
  }, [providersByChannel, pushActivity, refreshAuditModule, runAction]);

  return {
    roleReasonTemplates: [...USER_ROLE_REASON_TEMPLATES],
    handleApplyUserRole,
    runbookAction,
  };
}
