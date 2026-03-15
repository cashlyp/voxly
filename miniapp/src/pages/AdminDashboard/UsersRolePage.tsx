import { useEffect, useMemo, useState } from 'react';

import type { DashboardVm, UserRow } from './types';

type UsersRolePageProps = {
  visible: boolean;
  vm: DashboardVm;
};

const PAGE_SIZE_OPTIONS = [10, 20, 40, 80] as const;

function csvEscape(value: string): string {
  if (/[",\n]/.test(value)) {
    return `"${value.replace(/"/g, '""')}"`;
  }
  return value;
}

function downloadCsv(filename: string, headers: string[], rows: string[][]): void {
  if (typeof window === 'undefined' || typeof document === 'undefined') return;
  const lines = [headers.map(csvEscape).join(',')];
  rows.forEach((row) => {
    lines.push(row.map((cell) => csvEscape(String(cell ?? ''))).join(','));
  });
  const csv = `${lines.join('\n')}\n`;
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement('a');
  anchor.href = url;
  anchor.download = filename;
  document.body.appendChild(anchor);
  anchor.click();
  document.body.removeChild(anchor);
  URL.revokeObjectURL(url);
}

export function UsersRolePage({ visible, vm }: UsersRolePageProps) {
  if (!visible) return null;

  const {
    isFeatureEnabled,
    userSearch,
    setUserSearch,
    userSortBy,
    setUserSortBy,
    userSortDir,
    setUserSortDir,
    refreshUsersModule,
    usersPayload,
    usersRows,
    roleReasonTemplates,
    toInt,
    toText,
    formatTime,
    handleApplyUserRole,
  } = vm;
  const advancedTablesEnabled = isFeatureEnabled('advanced_tables', true);
  const usersCsvEnabled = isFeatureEnabled('users_csv_export', advancedTablesEnabled);
  const [roleFilter, setRoleFilter] = useState<'all' | 'admin' | 'operator' | 'viewer'>('all');
  const [pageSize, setPageSize] = useState<number>(20);
  const [page, setPage] = useState<number>(1);
  const [reasonTemplate, setReasonTemplate] = useState<string>(roleReasonTemplates[0] || '');
  const [reasonDetail, setReasonDetail] = useState<string>('');
  const composedReason = [reasonTemplate, reasonDetail.trim()].filter(Boolean).join(' - ');

  const filteredAndSortedUsers = useMemo(() => {
    const query = String(userSearch || '').trim().toLowerCase();
    const filtered = usersRows.filter((user) => {
      const role = toText(user.role, 'viewer').toLowerCase();
      if (roleFilter !== 'all' && role !== roleFilter) return false;
      if (!query) return true;
      const telegramId = toText(user.telegram_id, '').toLowerCase();
      const roleSource = toText(user.role_source, '').toLowerCase();
      return telegramId.includes(query) || role.includes(query) || roleSource.includes(query);
    });
    const direction = userSortDir === 'asc' ? 1 : -1;
    return [...filtered].sort((a, b) => {
      if (userSortBy === 'last_activity') {
        const aTime = Date.parse(toText(a.last_activity, ''));
        const bTime = Date.parse(toText(b.last_activity, ''));
        const safeATime = Number.isFinite(aTime) ? aTime : 0;
        const safeBTime = Number.isFinite(bTime) ? bTime : 0;
        return (safeATime - safeBTime) * direction;
      }
      if (userSortBy === 'total_calls') {
        return (toInt(a.total_calls) - toInt(b.total_calls)) * direction;
      }
      if (userSortBy === 'role') {
        return toText(a.role, 'viewer').localeCompare(toText(b.role, 'viewer')) * direction;
      }
      return toText(a.telegram_id, '').localeCompare(toText(b.telegram_id, '')) * direction;
    });
  }, [roleFilter, toInt, toText, userSearch, userSortBy, userSortDir, usersRows]);

  const totalPages = Math.max(1, Math.ceil(filteredAndSortedUsers.length / pageSize));
  useEffect(() => {
    if (page > totalPages) {
      setPage(totalPages);
    }
  }, [page, totalPages]);
  useEffect(() => {
    setPage(1);
  }, [roleFilter, userSearch, userSortBy, userSortDir, pageSize]);

  const effectivePageSize = advancedTablesEnabled ? pageSize : 80;
  const effectivePage = advancedTablesEnabled ? page : 1;
  const pageStart = (effectivePage - 1) * effectivePageSize;
  const pageUsers = filteredAndSortedUsers.slice(pageStart, pageStart + effectivePageSize);
  const showingStart = filteredAndSortedUsers.length === 0 ? 0 : pageStart + 1;
  const showingEnd = Math.min(filteredAndSortedUsers.length, pageStart + pageUsers.length);
  const exportUsersCsv = (): void => {
    const rows = filteredAndSortedUsers.map((user) => [
      toText(user.telegram_id, ''),
      toText(user.role, 'viewer'),
      toText(user.role_source, 'inferred'),
      String(toInt(user.total_calls)),
      String(toInt(user.failed_calls)),
      formatTime(user.last_activity),
    ]);
    downloadCsv('miniapp-users.csv', [
      'telegram_id',
      'role',
      'role_source',
      'total_calls',
      'failed_calls',
      'last_activity',
    ], rows);
  };

  return (
    <section className="va-grid">
      <div className="va-card">
        <h3>User & Role Admin</h3>
        <div className="va-inline-tools">
          <input
            className="va-input"
            placeholder="Search Telegram ID"
            value={userSearch}
            onChange={(event) => setUserSearch(event.target.value)}
          />
          <select
            className="va-input"
            value={userSortBy}
            onChange={(event) => setUserSortBy(event.target.value)}
          >
            <option value="last_activity">Last Activity</option>
            <option value="total_calls">Total Calls</option>
            <option value="role">Role</option>
          </select>
          <select
            className="va-input"
            value={userSortDir}
            onChange={(event) => setUserSortDir(event.target.value)}
          >
            <option value="desc">DESC</option>
            <option value="asc">ASC</option>
          </select>
          <button type="button" onClick={() => { void refreshUsersModule(); }}>
            Refresh Users
          </button>
        </div>
        {advancedTablesEnabled ? (
          <div className="va-inline-tools">
            <select
              className="va-input"
              value={roleFilter}
              onChange={(event) => setRoleFilter(event.target.value as 'all' | 'admin' | 'operator' | 'viewer')}
            >
              <option value="all">All Roles</option>
              <option value="admin">Admin</option>
              <option value="operator">Operator</option>
              <option value="viewer">Viewer</option>
            </select>
            <select
              className="va-input"
              value={String(pageSize)}
              onChange={(event) => setPageSize(toInt(event.target.value, 20))}
            >
              {PAGE_SIZE_OPTIONS.map((size) => (
                <option key={`users-size-${size}`} value={size}>{size} / page</option>
              ))}
            </select>
            {usersCsvEnabled ? (
              <button type="button" onClick={exportUsersCsv} disabled={filteredAndSortedUsers.length === 0}>
                Export CSV
              </button>
            ) : null}
          </div>
        ) : null}
        <div className="va-inline-tools">
          <select
            className="va-input"
            value={reasonTemplate}
            onChange={(event) => setReasonTemplate(event.target.value)}
          >
            {roleReasonTemplates.map((template) => (
              <option key={`role-reason-${template}`} value={template}>{template}</option>
            ))}
          </select>
          <input
            className="va-input"
            placeholder="Reason details (required for audit)"
            value={reasonDetail}
            onChange={(event) => setReasonDetail(event.target.value)}
          />
        </div>
        <p className="va-muted">
          Policy: admin elevation requires two-step approval phrase after reason confirmation.
        </p>
        <p className="va-muted">
          Total tracked: <strong>{toInt(usersPayload.total, usersRows.length)}</strong>
          {' '}| Filtered: <strong>{filteredAndSortedUsers.length}</strong>
          {' '}| Showing: <strong>{showingStart}-{showingEnd}</strong>
        </p>
        <ul className="va-list">
          {pageUsers.map((user: UserRow, index: number) => {
            const telegramId = toText(user.telegram_id, '');
            const role = toText(user.role, 'viewer');
            return (
              <li key={`user-role-${telegramId || `row-${pageStart + index}`}`}>
                <strong>{telegramId || 'unknown'}</strong>
                <span>Role: {role} ({toText(user.role_source, 'inferred')})</span>
                <span>Calls: {toInt(user.total_calls)} | Failed: {toInt(user.failed_calls)}</span>
                <span>Last activity: {formatTime(user.last_activity)}</span>
                <div className="va-inline-tools">
                  <button
                    type="button"
                    onClick={() => { void handleApplyUserRole(telegramId, 'admin', composedReason); }}
                    disabled={!telegramId}
                  >
                    Promote Admin
                  </button>
                  <button
                    type="button"
                    onClick={() => { void handleApplyUserRole(telegramId, 'operator', composedReason); }}
                    disabled={!telegramId}
                  >
                    Set Operator
                  </button>
                  <button
                    type="button"
                    onClick={() => { void handleApplyUserRole(telegramId, 'viewer', composedReason); }}
                    disabled={!telegramId}
                  >
                    Demote Viewer
                  </button>
                </div>
              </li>
            );
          })}
        </ul>
        {pageUsers.length === 0 ? <p className="va-muted">No users matched your filters.</p> : null}
        {advancedTablesEnabled ? (
          <div className="va-table-pager">
            <button type="button" onClick={() => setPage((prev) => Math.max(1, prev - 1))} disabled={page <= 1}>
              Previous
            </button>
            <span className="va-muted">Page {page} of {totalPages}</span>
            <button
              type="button"
              onClick={() => setPage((prev) => Math.min(totalPages, prev + 1))}
              disabled={page >= totalPages}
            >
              Next
            </button>
          </div>
        ) : null}
      </div>
    </section>
  );
}
