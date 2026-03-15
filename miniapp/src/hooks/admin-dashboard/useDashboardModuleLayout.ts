import { useCallback, useMemo } from 'react';

import { asRecord } from '@/services/admin-dashboard/dashboardPrimitives';
import {
  parseModuleId,
  parseModuleLayoutConfig,
} from '@/services/admin-dashboard/dashboardLayout';

export type DashboardModuleDefinition<T extends string> = {
  id: T;
  label: string;
  capability: string;
};

type UseDashboardModuleLayoutOptions<T extends string> = {
  sessionCaps: string[];
  dashboardLayoutPayload: unknown;
  moduleDefinitions: DashboardModuleDefinition<T>[];
  moduleDefaultOrder: Record<T, number>;
  moduleIdSet: Set<T>;
};

type UseDashboardModuleLayoutResult<T extends string> = {
  hasCapability: (capability: string) => boolean;
  visibleModules: Array<DashboardModuleDefinition<T> & { label: string }>;
  preferredServerModule: T | null;
};

export function useDashboardModuleLayout<T extends string>({
  sessionCaps,
  dashboardLayoutPayload,
  moduleDefinitions,
  moduleDefaultOrder,
  moduleIdSet,
}: UseDashboardModuleLayoutOptions<T>): UseDashboardModuleLayoutResult<T> {
  const hasCapability = useCallback((capability: string): boolean => (
    sessionCaps.includes(capability)
  ), [sessionCaps]);

  const moduleLayoutConfig = useMemo(
    () => parseModuleLayoutConfig(dashboardLayoutPayload, moduleIdSet),
    [dashboardLayoutPayload, moduleIdSet],
  );

  const visibleModules = useMemo(() => {
    const roleAllowedModules = moduleDefinitions.filter((module) => hasCapability(module.capability));
    return roleAllowedModules
      .filter((module) => moduleLayoutConfig[module.id]?.enabled !== false)
      .map((module) => ({
        ...module,
        label: moduleLayoutConfig[module.id]?.label || module.label,
      }))
      .sort((a, b) => {
        const orderA = moduleLayoutConfig[a.id]?.order;
        const orderB = moduleLayoutConfig[b.id]?.order;
        const safeA = Number.isFinite(orderA) ? Number(orderA) : moduleDefaultOrder[a.id];
        const safeB = Number.isFinite(orderB) ? Number(orderB) : moduleDefaultOrder[b.id];
        return safeA - safeB;
      });
  }, [hasCapability, moduleDefaultOrder, moduleDefinitions, moduleLayoutConfig]);

  const preferredServerModule = useMemo(() => parseModuleId(
    asRecord(dashboardLayoutPayload).active_module || asRecord(dashboardLayoutPayload).default_module,
    moduleIdSet,
  ), [dashboardLayoutPayload, moduleIdSet]);

  return {
    hasCapability,
    visibleModules,
    preferredServerModule,
  };
}
