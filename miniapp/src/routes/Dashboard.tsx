import { useEffect, useMemo } from 'react';
import { useCalls } from '../state/calls';

export function Dashboard() {
  const { calls, inboundQueue, fetchCalls, fetchInboundQueue, loading } = useCalls();

  useEffect(() => {
    fetchCalls({ limit: 10 });
    fetchInboundQueue();
  }, [fetchCalls, fetchInboundQueue]);

  const stats = useMemo(() => {
    const total = calls.length;
    const active = calls.filter((call) => ['in-progress', 'answered', 'ringing'].includes(String(call.status))).length;
    const completed = calls.filter((call) => String(call.status) === 'completed').length;
    return { total, active, completed };
  }, [calls]);

  return (
    <section className="stack">
      <div className="panel">
        <h2>Quick stats</h2>
        <div className="stats-grid">
          <div className="stat-card">
            <span>Total (recent)</span>
            <strong>{stats.total}</strong>
          </div>
          <div className="stat-card">
            <span>Active</span>
            <strong>{stats.active}</strong>
          </div>
          <div className="stat-card">
            <span>Completed</span>
            <strong>{stats.completed}</strong>
          </div>
        </div>
      </div>

      <div className="panel">
        <h2>Inbound queue</h2>
        {inboundQueue.length === 0 ? (
          <p className="muted">No inbound calls waiting.</p>
        ) : (
          <div className="list">
            {inboundQueue.slice(0, 3).map((call) => (
              <div className="list-item" key={call.call_sid}>
                <div>
                  <strong>{call.from || 'Unknown caller'}</strong>
                  <p className="muted">{call.route_label || call.script || 'Inbound'}</p>
                </div>
                <span className={`badge ${call.decision || 'pending'}`}>{call.decision || 'pending'}</span>
              </div>
            ))}
          </div>
        )}
      </div>

      <div className="panel">
        <h2>Recent activity</h2>
        {loading && calls.length === 0 ? (
          <p className="muted">Loading calls...</p>
        ) : (
          <div className="list">
            {calls.slice(0, 5).map((call) => (
              <div className="list-item" key={call.call_sid}>
                <div>
                  <strong>{call.phone_number || call.call_sid}</strong>
                  <p className="muted">{call.status || 'unknown'} - {call.created_at || '-'}</p>
                </div>
                <span className={`badge ${call.status || 'unknown'}`}>{call.status || 'unknown'}</span>
              </div>
            ))}
          </div>
        )}
      </div>
    </section>
  );
}
