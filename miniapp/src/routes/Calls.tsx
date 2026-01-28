import { useCallback, useEffect, useState } from 'react';
import { useCalls } from '../state/calls';
import { navigate } from '../lib/router';

export function Calls() {
  const { calls, fetchCalls, nextCursor, loading } = useCalls();
  const [statusFilter, setStatusFilter] = useState('');
  const [search, setSearch] = useState('');
  const [cursor, setCursor] = useState(0);

  const loadCalls = useCallback(async (nextCursorValue = 0) => {
    await fetchCalls({
      limit: 20,
      cursor: nextCursorValue,
      status: statusFilter || undefined,
      q: search || undefined,
    });
  }, [fetchCalls, statusFilter, search]);

  useEffect(() => {
    loadCalls(0);
  }, [loadCalls]);

  const handleNext = () => {
    if (nextCursor !== null) {
      setCursor(nextCursor);
      loadCalls(nextCursor);
    }
  };

  const handlePrev = () => {
    const prev = Math.max(0, cursor - 20);
    setCursor(prev);
    loadCalls(prev);
  };

  const handleClear = () => {
    setStatusFilter('');
    setSearch('');
    setCursor(0);
    loadCalls(0);
  };

  return (
    <section className="stack">
      <div className="panel">
        <h2>Call log</h2>
        <div className="filters">
          <input
            type="text"
            placeholder="Search last4 or label..."
            value={search}
            onChange={(event) => setSearch(event.target.value)}
            onKeyDown={(event) => {
              if (event.key === 'Enter') {
                setCursor(0);
                loadCalls(0);
              }
            }}
          />
          <select value={statusFilter} onChange={(event) => setStatusFilter(event.target.value)}>
            <option value="">All statuses</option>
            <option value="ringing">Ringing</option>
            <option value="in-progress">In progress</option>
            <option value="completed">Completed</option>
            <option value="no-answer">No answer</option>
            <option value="failed">Failed</option>
          </select>
          <button
            type="button"
            className="btn"
            onClick={() => {
              setCursor(0);
              loadCalls(0);
            }}
          >
            Apply
          </button>
          <button type="button" className="btn ghost" onClick={handleClear}>
            Clear
          </button>
        </div>
        <p className="muted">
          Showing {calls.length} calls {cursor ? `from ${cursor + 1}` : ''} {statusFilter ? `| ${statusFilter}` : ''} {search ? `| "${search}"` : ''}
        </p>
        {loading && calls.length === 0 ? (
          <p className="muted">Loading calls...</p>
        ) : (
          <div className="list">
            {calls.map((call) => (
              <button
                type="button"
                className="list-item clickable"
                key={call.call_sid}
                onClick={() => navigate(`/calls/${call.call_sid}`)}
              >
                <div>
                  <strong>{call.phone_number || call.call_sid}</strong>
                  <p className="muted">{call.status || 'unknown'} - {call.created_at || '-'}</p>
                </div>
                <span className={`badge ${call.status || 'unknown'}`}>{call.status || 'unknown'}</span>
              </button>
            ))}
          </div>
        )}
        <div className="pager">
          <button type="button" className="btn ghost" onClick={handlePrev} disabled={cursor === 0}>
            Prev
          </button>
          <button type="button" className="btn ghost" onClick={handleNext} disabled={nextCursor === null}>
            Next
          </button>
        </div>
      </div>
    </section>
  );
}
