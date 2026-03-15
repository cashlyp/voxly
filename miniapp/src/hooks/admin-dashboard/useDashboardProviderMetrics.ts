import { useMemo } from 'react';

import { asRecord, toText } from '@/services/admin-dashboard/dashboardPrimitives';

export type ProviderChannel = 'call' | 'sms' | 'email';

export type ProviderMatrixRow = {
  channel: ProviderChannel;
  provider: string;
  ready: boolean;
  degraded: boolean;
  flowCount: number;
  parityGapCount: number;
  paymentMode: string;
};

type UseDashboardProviderMetricsOptions = {
  providerCompatibilityChannels: Record<string, unknown>;
  providersByChannel: Record<string, unknown>;
};

type UseDashboardProviderMetricsResult = {
  providerMatrixRows: ProviderMatrixRow[];
  providerReadinessTotals: { ready: number; total: number };
  providerReadinessPercent: number;
  providerDegradedCount: number;
  providerCurrentByChannel: Record<ProviderChannel, string>;
  providerSupportedByChannel: Record<ProviderChannel, string[]>;
};

const CHANNELS: ProviderChannel[] = ['call', 'sms', 'email'];

function asStringList(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return value
    .map((entry) => toText(entry, ''))
    .map((entry) => entry.trim().toLowerCase())
    .filter(Boolean);
}

export function useDashboardProviderMetrics({
  providerCompatibilityChannels,
  providersByChannel,
}: UseDashboardProviderMetricsOptions): UseDashboardProviderMetricsResult {
  return useMemo(() => {
    const providerMatrixRows = CHANNELS
      .flatMap((channel) => {
        const channelDetails = asRecord(providerCompatibilityChannels[channel]);
        const providers = asRecord(channelDetails.providers);
        const parityGaps = asRecord(channelDetails.parity_gaps);
        const rows = Object.entries(providers).map(([provider, raw]) => {
          const details = asRecord(raw);
          return {
            channel,
            provider,
            ready: details.ready === true,
            degraded: details.degraded === true,
            flowCount: asStringList(details.flows).length,
            parityGapCount: asStringList(parityGaps[provider]).length,
            paymentMode: toText(details.payment_mode, 'n/a'),
          };
        });
        if (rows.length > 0) {
          return rows;
        }

        const channelData = asRecord(providersByChannel[channel]);
        const supported = asStringList(channelData.supported_providers);
        const readiness = asRecord(channelData.readiness);
        return supported.map((provider) => ({
          channel,
          provider,
          ready: readiness[provider] !== false,
          degraded: false,
          flowCount: 0,
          parityGapCount: 0,
          paymentMode: 'n/a',
        }));
      })
      .sort((left, right) => {
        const channelSort = left.channel.localeCompare(right.channel);
        return channelSort === 0 ? left.provider.localeCompare(right.provider) : channelSort;
      });

    const providerReadinessTotals = providerMatrixRows.reduce(
      (acc, row) => ({
        ready: acc.ready + (row.ready ? 1 : 0),
        total: acc.total + 1,
      }),
      { ready: 0, total: 0 },
    );
    const providerReadinessPercent = providerReadinessTotals.total > 0
      ? Math.round((providerReadinessTotals.ready / providerReadinessTotals.total) * 100)
      : 0;
    const providerDegradedCount = providerMatrixRows.filter((row) => row.degraded).length;

    const providerCurrentByChannel: Record<ProviderChannel, string> = {
      call: toText(asRecord(providersByChannel.call).provider, '').toLowerCase(),
      sms: toText(asRecord(providersByChannel.sms).provider, '').toLowerCase(),
      email: toText(asRecord(providersByChannel.email).provider, '').toLowerCase(),
    };

    const providerSupportedByChannel: Record<ProviderChannel, string[]> = {
      call: asStringList(asRecord(providersByChannel.call).supported_providers),
      sms: asStringList(asRecord(providersByChannel.sms).supported_providers),
      email: asStringList(asRecord(providersByChannel.email).supported_providers),
    };

    return {
      providerMatrixRows,
      providerReadinessTotals,
      providerReadinessPercent,
      providerDegradedCount,
      providerCurrentByChannel,
      providerSupportedByChannel,
    };
  }, [providerCompatibilityChannels, providersByChannel]);
}
