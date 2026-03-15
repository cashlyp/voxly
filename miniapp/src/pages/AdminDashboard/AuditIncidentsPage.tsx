import { useEffect, useMemo, useState } from 'react';

import type { AuditRow, DashboardVm, IncidentRow, RunbookRow } from './types';

type AuditIncidentsPageProps = {
  visible: boolean;
  vm: DashboardVm;
};

const PAGE_SIZE_OPTIONS = [10, 20, 40] as const;
const SAVED_QUERY_STORAGE_KEY = 'voxly-miniapp-audit-saved-queries';

type SavedQuery = {
  id: string;
  label: string;
  incidentQuery: string;
  incidentStatus: string;
  actor: string;
  module: string;
  severity: string;
  auditQuery: string;
  auditActor: string;
  auditModule: string;
  auditSeverity: string;
};

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

export function AuditIncidentsPage({ visible, vm }: AuditIncidentsPageProps) {
  if (!visible) return null;

  const {
    refreshAuditModule,
    hasCapability,
    isFeatureEnabled,
    runbookAction,
    incidentsPayload,
    incidentRows,
    toInt,
    toText,
    asRecord,
    formatTime,
    runbookRows,
    busyAction,
    auditRows,
  } = vm;
  const advancedTablesEnabled = isFeatureEnabled('advanced_tables', true);
  const runbookActionsEnabled = isFeatureEnabled('runbook_actions', true);
  const incidentCsvEnabled = isFeatureEnabled('incidents_csv_export', advancedTablesEnabled);
  const auditCsvEnabled = isFeatureEnabled('audit_csv_export', advancedTablesEnabled);
  const [incidentQuery, setIncidentQuery] = useState<string>('');
  const [incidentStatusFilter, setIncidentStatusFilter] = useState<string>('all');
  const [incidentActorFilter, setIncidentActorFilter] = useState<string>('all');
  const [incidentModuleFilter, setIncidentModuleFilter] = useState<string>('all');
  const [incidentSeverityFilter, setIncidentSeverityFilter] = useState<string>('all');
  const [incidentPage, setIncidentPage] = useState<number>(1);
  const [incidentPageSize, setIncidentPageSize] = useState<number>(20);
  const [auditQuery, setAuditQuery] = useState<string>('');
  const [auditActorFilter, setAuditActorFilter] = useState<string>('all');
  const [auditModuleFilter, setAuditModuleFilter] = useState<string>('all');
  const [auditSeverityFilter, setAuditSeverityFilter] = useState<string>('all');
  const [auditPage, setAuditPage] = useState<number>(1);
  const [auditPageSize, setAuditPageSize] = useState<number>(20);
  const [savedQueries, setSavedQueries] = useState<SavedQuery[]>([]);

  useEffect(() => {
    if (typeof window === 'undefined') return;
    try {
      const raw = window.localStorage.getItem(SAVED_QUERY_STORAGE_KEY);
      if (!raw) return;
      const parsed: unknown = JSON.parse(raw);
      if (!Array.isArray(parsed)) return;
      const rows = parsed
        .map((entry) => asRecord(entry))
        .map((entry) => ({
          id: toText(entry.id, `${Date.now()}`),
          label: toText(entry.label, 'Saved query'),
          incidentQuery: toText(entry.incidentQuery, ''),
          incidentStatus: toText(entry.incidentStatus, 'all'),
          actor: toText(entry.actor, 'all'),
          module: toText(entry.module, 'all'),
          severity: toText(entry.severity, 'all'),
          auditQuery: toText(entry.auditQuery, ''),
          auditActor: toText(entry.auditActor, 'all'),
          auditModule: toText(entry.auditModule, 'all'),
          auditSeverity: toText(entry.auditSeverity, 'all'),
        }));
      setSavedQueries(rows);
    } catch {
      setSavedQueries([]);
    }
  }, [asRecord, toText]);

  useEffect(() => {
    if (typeof window === 'undefined') return;
    try {
      window.localStorage.setItem(SAVED_QUERY_STORAGE_KEY, JSON.stringify(savedQueries.slice(0, 8)));
    } catch {
      // Ignore local storage failures in constrained clients.
    }
  }, [savedQueries]);

  const incidentStatusOptions = useMemo(() => {
    const statuses = Array.from(new Set(incidentRows.map((row) => toText(row.status, 'unknown').toLowerCase())));
    return statuses.filter(Boolean).sort();
  }, [incidentRows, toText]);
  const incidentActorOptions = useMemo(() => {
    const actors = incidentRows.map((row) => toText(asRecord(row.details).actor, '').toLowerCase()).filter(Boolean);
    return Array.from(new Set(actors)).sort();
  }, [asRecord, incidentRows, toText]);
  const incidentModuleOptions = useMemo(() => {
    const modules = incidentRows.map((row) => toText(asRecord(row.details).module, '').toLowerCase()).filter(Boolean);
    return Array.from(new Set(modules)).sort();
  }, [asRecord, incidentRows, toText]);
  const incidentSeverityOptions = useMemo(() => {
    const severities = incidentRows.map((row) => toText(asRecord(row.details).severity, '').toLowerCase()).filter(Boolean);
    return Array.from(new Set(severities)).sort();
  }, [asRecord, incidentRows, toText]);

  const filteredIncidentRows = useMemo(() => {
    const query = incidentQuery.trim().toLowerCase();
    return [...incidentRows]
      .filter((incident) => {
        const status = toText(incident.status, 'unknown').toLowerCase();
        const details = asRecord(incident.details);
        const actor = toText(details.actor, '').toLowerCase();
        const moduleName = toText(details.module, '').toLowerCase();
        const severity = toText(details.severity, '').toLowerCase();
        if (incidentStatusFilter !== 'all' && status !== incidentStatusFilter) return false;
        if (incidentActorFilter !== 'all' && actor !== incidentActorFilter) return false;
        if (incidentModuleFilter !== 'all' && moduleName !== incidentModuleFilter) return false;
        if (incidentSeverityFilter !== 'all' && severity !== incidentSeverityFilter) return false;
        if (!query) return true;
        const service = toText(incident.service_name, 'service').toLowerCase();
        const detail = toText(asRecord(incident.details).message, toText(incident.details, '')).toLowerCase();
        return service.includes(query) || status.includes(query) || detail.includes(query)
          || actor.includes(query) || moduleName.includes(query) || severity.includes(query);
      })
      .sort((a, b) => {
        const aTime = Date.parse(toText(a.timestamp, ''));
        const bTime = Date.parse(toText(b.timestamp, ''));
        return (Number.isFinite(bTime) ? bTime : 0) - (Number.isFinite(aTime) ? aTime : 0);
      });
  }, [
    asRecord,
    incidentActorFilter,
    incidentModuleFilter,
    incidentQuery,
    incidentRows,
    incidentSeverityFilter,
    incidentStatusFilter,
    toText,
  ]);

  const filteredAuditRows = useMemo(() => {
    const query = auditQuery.trim().toLowerCase();
    return [...auditRows]
      .filter((row) => {
        const details = asRecord(row.details);
        const actor = toText(details.actor, 'unknown').toLowerCase();
        const moduleName = toText(details.module, toText(row.service_name, 'unknown')).toLowerCase();
        const severity = toText(details.severity, 'info').toLowerCase();
        if (auditActorFilter !== 'all' && actor !== auditActorFilter) return false;
        if (auditModuleFilter !== 'all' && moduleName !== auditModuleFilter) return false;
        if (auditSeverityFilter !== 'all' && severity !== auditSeverityFilter) return false;
        if (!query) return true;
        const service = toText(row.service_name, 'service').toLowerCase();
        const status = toText(row.status, 'unknown').toLowerCase();
        const detail = toText(details.message, toText(row.details, '')).toLowerCase();
        return service.includes(query) || status.includes(query) || detail.includes(query)
          || actor.includes(query) || moduleName.includes(query) || severity.includes(query);
      })
      .sort((a, b) => {
        const aTime = Date.parse(toText(a.timestamp, ''));
        const bTime = Date.parse(toText(b.timestamp, ''));
        return (Number.isFinite(bTime) ? bTime : 0) - (Number.isFinite(aTime) ? aTime : 0);
      });
  }, [asRecord, auditActorFilter, auditModuleFilter, auditQuery, auditRows, auditSeverityFilter, toText]);
  const auditActorOptions = useMemo(() => {
    const actors = auditRows.map((row) => toText(asRecord(row.details).actor, '').toLowerCase()).filter(Boolean);
    return Array.from(new Set(actors)).sort();
  }, [asRecord, auditRows, toText]);
  const auditModuleOptions = useMemo(() => {
    const modules = auditRows.map((row) => toText(asRecord(row.details).module, '').toLowerCase()).filter(Boolean);
    return Array.from(new Set(modules)).sort();
  }, [asRecord, auditRows, toText]);
  const auditSeverityOptions = useMemo(() => {
    const severities = auditRows.map((row) => toText(asRecord(row.details).severity, '').toLowerCase()).filter(Boolean);
    return Array.from(new Set(severities)).sort();
  }, [asRecord, auditRows, toText]);

  const incidentTotalPages = Math.max(1, Math.ceil(filteredIncidentRows.length / incidentPageSize));
  const auditTotalPages = Math.max(1, Math.ceil(filteredAuditRows.length / auditPageSize));
  useEffect(() => {
    if (incidentPage > incidentTotalPages) setIncidentPage(incidentTotalPages);
  }, [incidentPage, incidentTotalPages]);
  useEffect(() => {
    if (auditPage > auditTotalPages) setAuditPage(auditTotalPages);
  }, [auditPage, auditTotalPages]);
  useEffect(() => {
    setIncidentPage(1);
  }, [incidentActorFilter, incidentModuleFilter, incidentQuery, incidentSeverityFilter, incidentStatusFilter, incidentPageSize]);
  useEffect(() => {
    setAuditPage(1);
  }, [auditActorFilter, auditModuleFilter, auditQuery, auditSeverityFilter, auditPageSize]);

  const effectiveIncidentPageSize = advancedTablesEnabled ? incidentPageSize : 30;
  const effectiveIncidentPage = advancedTablesEnabled ? incidentPage : 1;
  const incidentStart = (effectiveIncidentPage - 1) * effectiveIncidentPageSize;
  const incidentPageRows = filteredIncidentRows.slice(incidentStart, incidentStart + effectiveIncidentPageSize);
  const incidentRangeStart = filteredIncidentRows.length === 0 ? 0 : incidentStart + 1;
  const incidentRangeEnd = Math.min(filteredIncidentRows.length, incidentStart + incidentPageRows.length);
  const effectiveAuditPageSize = advancedTablesEnabled ? auditPageSize : 40;
  const effectiveAuditPage = advancedTablesEnabled ? auditPage : 1;
  const auditStart = (effectiveAuditPage - 1) * effectiveAuditPageSize;
  const auditPageRows = filteredAuditRows.slice(auditStart, auditStart + effectiveAuditPageSize);
  const auditRangeStart = filteredAuditRows.length === 0 ? 0 : auditStart + 1;
  const auditRangeEnd = Math.min(filteredAuditRows.length, auditStart + auditPageRows.length);
  const exportIncidentCsv = (): void => {
    const rows = filteredIncidentRows.map((incident) => [
      toText(incident.service_name, 'service'),
      toText(incident.status, 'unknown'),
      toText(asRecord(incident.details).message, toText(incident.details, '')),
      formatTime(incident.timestamp),
    ]);
    downloadCsv('miniapp-incidents.csv', ['service_name', 'status', 'message', 'timestamp'], rows);
  };
  const exportAuditCsv = (): void => {
    const rows = filteredAuditRows.map((row) => [
      toText(row.service_name, 'service'),
      toText(row.status, 'unknown'),
      toText(asRecord(row.details).message, toText(row.details, '')),
      formatTime(row.timestamp),
    ]);
    downloadCsv('miniapp-audit-feed.csv', ['service_name', 'status', 'message', 'timestamp'], rows);
  };
  const saveCurrentQuery = (): void => {
    const label = `${incidentStatusFilter}/${incidentSeverityFilter} @ ${new Date().toLocaleTimeString()}`;
    const entry: SavedQuery = {
      id: `${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
      label,
      incidentQuery,
      incidentStatus: incidentStatusFilter,
      actor: incidentActorFilter,
      module: incidentModuleFilter,
      severity: incidentSeverityFilter,
      auditQuery,
      auditActor: auditActorFilter,
      auditModule: auditModuleFilter,
      auditSeverity: auditSeverityFilter,
    };
    setSavedQueries((prev) => [entry, ...prev].slice(0, 8));
  };
  const applySavedQuery = (entry: SavedQuery): void => {
    setIncidentQuery(entry.incidentQuery);
    setIncidentStatusFilter(entry.incidentStatus || 'all');
    setIncidentActorFilter(entry.actor || 'all');
    setIncidentModuleFilter(entry.module || 'all');
    setIncidentSeverityFilter(entry.severity || 'all');
    setAuditQuery(entry.auditQuery || '');
    setAuditActorFilter(entry.auditActor || 'all');
    setAuditModuleFilter(entry.auditModule || 'all');
    setAuditSeverityFilter(entry.auditSeverity || 'all');
  };
  const deleteSavedQuery = (id: string): void => {
    setSavedQueries((prev) => prev.filter((query) => query.id !== id));
  };

  return (
    <section className="va-grid">
      <div className="va-card">
        <h3>Audit & Incident Center</h3>
        <div className="va-inline-tools">
          <button type="button" onClick={() => { void refreshAuditModule(); }}>
            Refresh Alerts
          </button>
          <button
            type="button"
            disabled={!hasCapability('sms_bulk_manage')}
            onClick={() => { void runbookAction('runbook.sms.reconcile'); }}
          >
            Runbook: SMS Reconcile
          </button>
          <button
            type="button"
            disabled={!hasCapability('provider_manage')}
            onClick={() => { void runbookAction('runbook.payment.reconcile'); }}
          >
            Runbook: Payment Reconcile
          </button>
          <button
            type="button"
            disabled={!hasCapability('provider_manage')}
            onClick={() => { void runbookAction('runbook.provider.preflight'); }}
          >
            Runbook: Provider Preflight
          </button>
          <button
            type="button"
            disabled={!hasCapability('provider_manage')}
            onClick={() => { void runbookAction('runbook.provider.preflight', { channel: 'call' }); }}
          >
            Playbook: Provider Outage
          </button>
          <button
            type="button"
            disabled={!hasCapability('sms_bulk_manage')}
            onClick={() => { void runbookAction('runbook.sms.reconcile', { scope: 'failure_spike' }); }}
          >
            Playbook: SMS Failure Spike
          </button>
        </div>
        {advancedTablesEnabled ? (
          <div className="va-inline-tools">
            <input
              className="va-input"
              placeholder="Filter incidents (service/status/message)"
              value={incidentQuery}
              onChange={(event) => setIncidentQuery(event.target.value)}
            />
            <select
              className="va-input"
              value={incidentStatusFilter}
              onChange={(event) => setIncidentStatusFilter(event.target.value)}
            >
              <option value="all">All statuses</option>
              {incidentStatusOptions.map((status) => (
                <option key={`incident-status-${status}`} value={status}>{status}</option>
              ))}
            </select>
            <select
              className="va-input"
              value={incidentActorFilter}
              onChange={(event) => setIncidentActorFilter(event.target.value)}
            >
              <option value="all">All actors</option>
              {incidentActorOptions.map((actor) => (
                <option key={`incident-actor-${actor}`} value={actor}>{actor}</option>
              ))}
            </select>
            <select
              className="va-input"
              value={incidentModuleFilter}
              onChange={(event) => setIncidentModuleFilter(event.target.value)}
            >
              <option value="all">All modules</option>
              {incidentModuleOptions.map((moduleName) => (
                <option key={`incident-module-${moduleName}`} value={moduleName}>{moduleName}</option>
              ))}
            </select>
            <select
              className="va-input"
              value={incidentSeverityFilter}
              onChange={(event) => setIncidentSeverityFilter(event.target.value)}
            >
              <option value="all">All severities</option>
              {incidentSeverityOptions.map((severity) => (
                <option key={`incident-severity-${severity}`} value={severity}>{severity}</option>
              ))}
            </select>
            <select
              className="va-input"
              value={String(incidentPageSize)}
              onChange={(event) => setIncidentPageSize(toInt(event.target.value, 20))}
            >
              {PAGE_SIZE_OPTIONS.map((size) => (
                <option key={`incident-page-size-${size}`} value={size}>{size} / page</option>
              ))}
            </select>
            {incidentCsvEnabled ? (
              <button type="button" onClick={exportIncidentCsv} disabled={filteredIncidentRows.length === 0}>
                Export Alerts CSV
              </button>
            ) : null}
            <button type="button" onClick={saveCurrentQuery}>
              Save Query
            </button>
          </div>
        ) : null}
        {savedQueries.length > 0 ? (
          <div className="va-inline-tools">
            {savedQueries.map((query) => (
              <div key={`saved-query-${query.id}`} className="va-saved-query">
                <button type="button" onClick={() => applySavedQuery(query)}>{query.label}</button>
                <button type="button" onClick={() => deleteSavedQuery(query.id)}>✕</button>
              </div>
            ))}
          </div>
        ) : null}
        <p className="va-muted">
          Alert count: <strong>{toInt(incidentsPayload.total_alerts, incidentRows.length)}</strong>
          {' '}| Filtered: <strong>{filteredIncidentRows.length}</strong>
          {' '}| Showing: <strong>{incidentRangeStart}-{incidentRangeEnd}</strong>
        </p>
        <ul className="va-list">
          {incidentPageRows.map((incident: IncidentRow, index: number) => (
            <li key={`incident-${incidentStart + index}`}>
              <strong>{toText(incident.service_name, 'service')}</strong>
              <span>Status: {toText(incident.status, 'unknown')}</span>
              <span>{toText((asRecord(incident.details).message), toText(incident.details, ''))}</span>
              <span>{formatTime(incident.timestamp)}</span>
            </li>
          ))}
        </ul>
        {incidentPageRows.length === 0 ? <p className="va-muted">No incident alerts matched filters.</p> : null}
        {advancedTablesEnabled ? (
          <div className="va-table-pager">
            <button
              type="button"
              onClick={() => setIncidentPage((prev) => Math.max(1, prev - 1))}
              disabled={incidentPage <= 1}
            >
              Previous
            </button>
            <span className="va-muted">Page {incidentPage} of {incidentTotalPages}</span>
            <button
              type="button"
              onClick={() => setIncidentPage((prev) => Math.min(incidentTotalPages, prev + 1))}
              disabled={incidentPage >= incidentTotalPages}
            >
              Next
            </button>
          </div>
        ) : null}
        {runbookActionsEnabled && runbookRows.length > 0 ? (
          <>
            <h3>Runbook Actions</h3>
            <ul className="va-list">
              {runbookRows.map((runbook: RunbookRow, index: number) => {
                const action = toText(runbook.action, '');
                const capability = toText(runbook.capability, 'dashboard_view');
                return (
                  <li key={`runbook-${index}`}>
                    <strong>{toText(runbook.label, 'Runbook')}</strong>
                    <span>{action || 'unknown_action'}</span>
                    <button
                      type="button"
                      disabled={busyAction.length > 0 || !action || !hasCapability(capability)}
                      onClick={() => { void runbookAction(action, {}); }}
                    >
                      Execute
                    </button>
                  </li>
                );
              })}
            </ul>
          </>
        ) : null}
      </div>
      <div className="va-card">
        <h3>Immutable Activity Timeline</h3>
        {advancedTablesEnabled ? (
          <div className="va-inline-tools">
            <input
              className="va-input"
              placeholder="Filter audit timeline"
              value={auditQuery}
              onChange={(event) => setAuditQuery(event.target.value)}
            />
            <select
              className="va-input"
              value={String(auditPageSize)}
              onChange={(event) => setAuditPageSize(toInt(event.target.value, 20))}
            >
              {PAGE_SIZE_OPTIONS.map((size) => (
                <option key={`audit-page-size-${size}`} value={size}>{size} / page</option>
              ))}
            </select>
            <select
              className="va-input"
              value={auditActorFilter}
              onChange={(event) => setAuditActorFilter(event.target.value)}
            >
              <option value="all">Actor: all</option>
              {auditActorOptions.map((actor) => (
                <option key={`audit-actor-${actor}`} value={actor}>{actor}</option>
              ))}
            </select>
            <select
              className="va-input"
              value={auditModuleFilter}
              onChange={(event) => setAuditModuleFilter(event.target.value)}
            >
              <option value="all">Module: all</option>
              {auditModuleOptions.map((moduleName) => (
                <option key={`audit-module-${moduleName}`} value={moduleName}>{moduleName}</option>
              ))}
            </select>
            <select
              className="va-input"
              value={auditSeverityFilter}
              onChange={(event) => setAuditSeverityFilter(event.target.value)}
            >
              <option value="all">Severity: all</option>
              {auditSeverityOptions.map((severity) => (
                <option key={`audit-severity-${severity}`} value={severity}>{severity}</option>
              ))}
            </select>
            {auditCsvEnabled ? (
              <button type="button" onClick={exportAuditCsv} disabled={filteredAuditRows.length === 0}>
                Export Audit CSV
              </button>
            ) : null}
          </div>
        ) : null}
        <p className="va-muted">
          Filtered: <strong>{filteredAuditRows.length}</strong>
          {' '}| Showing: <strong>{auditRangeStart}-{auditRangeEnd}</strong>
        </p>
        <ul className="va-list">
          {auditPageRows.map((row: AuditRow, index: number) => (
            <li key={`audit-${auditStart + index}`}>
              <strong>{toText(row.service_name, 'service')}</strong>
              <span>Status: {toText(row.status, 'unknown')}</span>
              <span>{toText((asRecord(row.details).message), toText(row.details, ''))}</span>
              <span>{formatTime(row.timestamp)}</span>
            </li>
          ))}
        </ul>
        {auditPageRows.length === 0 ? <p className="va-muted">No audit entries matched filters.</p> : null}
        {advancedTablesEnabled ? (
          <div className="va-table-pager">
            <button
              type="button"
              onClick={() => setAuditPage((prev) => Math.max(1, prev - 1))}
              disabled={auditPage <= 1}
            >
              Previous
            </button>
            <span className="va-muted">Page {auditPage} of {auditTotalPages}</span>
            <button
              type="button"
              onClick={() => setAuditPage((prev) => Math.min(auditTotalPages, prev + 1))}
              disabled={auditPage >= auditTotalPages}
            >
              Next
            </button>
          </div>
        ) : null}
      </div>
    </section>
  );
}
