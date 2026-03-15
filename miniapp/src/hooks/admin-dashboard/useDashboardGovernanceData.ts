import { useCallback } from 'react';

type ActivityStatus = 'info' | 'success' | 'error';

type InvokeAction = (
  action: string,
  payload: Record<string, unknown>,
  metaOverride?: Record<string, unknown>,
) => Promise<unknown>;

type UseDashboardGovernanceDataOptions<UsersPayload, AuditPayload, IncidentsPayload> = {
  invokeAction: InvokeAction;
  pushActivity: (status: ActivityStatus, title: string, detail: string) => void;
  setError: (message: string) => void;
  setUsersSnapshot: (payload: UsersPayload) => void;
  setAuditSnapshot: (payload: AuditPayload) => void;
  setIncidentsSnapshot: (payload: IncidentsPayload) => void;
  userSearch: string;
  userSortBy: string;
  userSortDir: string;
};

type UseDashboardGovernanceDataResult = {
  refreshUsersModule: () => Promise<void>;
  refreshAuditModule: () => Promise<void>;
};

export function useDashboardGovernanceData<UsersPayload, AuditPayload, IncidentsPayload>({
  invokeAction,
  pushActivity,
  setError,
  setUsersSnapshot,
  setAuditSnapshot,
  setIncidentsSnapshot,
  userSearch,
  userSortBy,
  userSortDir,
}: UseDashboardGovernanceDataOptions<UsersPayload, AuditPayload, IncidentsPayload>): UseDashboardGovernanceDataResult {
  const refreshUsersModule = useCallback(async (): Promise<void> => {
    try {
      const data = await invokeAction('users.list', {
        limit: 120,
        offset: 0,
        search: userSearch,
        sort_by: userSortBy,
        sort_dir: userSortDir,
      }) as UsersPayload;
      setUsersSnapshot(data);
      pushActivity('success', 'Users refreshed', 'User and role list reloaded.');
    } catch (err) {
      const detail = err instanceof Error ? err.message : String(err);
      setError(detail);
      pushActivity('error', 'Users refresh failed', detail);
    }
  }, [invokeAction, pushActivity, setError, setUsersSnapshot, userSearch, userSortBy, userSortDir]);

  const refreshAuditModule = useCallback(async (): Promise<void> => {
    try {
      const [auditData, incidentData] = await Promise.all([
        invokeAction('audit.feed', { limit: 80, hours: 24 }),
        invokeAction('incidents.summary', { limit: 80, hours: 24 }),
      ]);
      setAuditSnapshot(auditData as AuditPayload);
      setIncidentsSnapshot(incidentData as IncidentsPayload);
      pushActivity('success', 'Audit refreshed', 'Audit and incident feeds reloaded.');
    } catch (err) {
      const detail = err instanceof Error ? err.message : String(err);
      setError(detail);
      pushActivity('error', 'Audit refresh failed', detail);
    }
  }, [invokeAction, pushActivity, setAuditSnapshot, setError, setIncidentsSnapshot]);

  return {
    refreshUsersModule,
    refreshAuditModule,
  };
}
