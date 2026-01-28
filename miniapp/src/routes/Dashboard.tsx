import { useEffect, useMemo, useState } from 'react';
import { Button } from '@telegram-apps/telegram-ui';
import { useCalls } from '../state/calls';
import { navigate } from '../lib/router';
import { apiFetch } from '../lib/api';
import { useUser } from '../state/user';

export function Dashboard() {
  const { calls, inboundQueue, fetchCalls, fetchInboundQueue, loading } = useCalls();
  const { roles } = useUser();
  const isAdmin = roles.includes('admin');
  const [callbackTasks, setCallbackTasks] = useState<{ id: number; run_at: string; number: string }[]>([]);

  useEffect(() => {
    fetchCalls({ limit: 10 });
    fetchInboundQueue();
    if (isAdmin) {
      apiFetch<{ ok: boolean; tasks: { id: number; run_at: string; number: string }[] }>('/webapp/callbacks?limit=3')
        .then((response) => setCallbackTasks(response.tasks || []))
        .catch(() => {});
    }
  }, [fetchCalls, fetchInboundQueue, isAdmin]);

  const stats = useMemo(() => {
    const total = calls.length;
    const active = calls.filter((call) => ['in-progress', 'answered', 'ringing'].includes(String(call.status))).length;
    const completed = calls.filter((call) => String(call.status) === 'completed').length;
    return { total, active, completed };
  }, [calls]);

  const heroStatus = stats.active > 0 ? 'Live' : 'Idle';

  return (
    <div className="wallet-page">
      <div className="hero-card">
        <div className="hero-header">
          <div>
            <div className="hero-label">Active calls</div>
            <div className="hero-subtitle">Today {stats.total} total</div>
          </div>
          <div className={`hero-status ${heroStatus === 'Live' ? 'live' : 'idle'}`}>
            {heroStatus}
          </div>
        </div>
        <div className="hero-value">{stats.active}</div>
        <div className="hero-meta">
          <span className="hero-pill">{stats.completed} completed</span>
          <span className="hero-pill ghost">{inboundQueue.length} waiting</span>
        </div>
      </div>

      <div className="pill-row">
        <button className="pill-card" type="button" onClick={() => navigate('/calls')}>
          <span className="pill-icon" aria-hidden="true">
            <svg viewBox="0 0 24 24">
              <path d="M7 17a2 2 0 0 1-2-2V9a2 2 0 0 1 2-2h4.5" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" />
              <path d="M15 7h4v10h-4z" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" />
              <path d="M11 12h4" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" />
            </svg>
          </span>
          Live Console
        </button>
        <button className="pill-card" type="button" onClick={() => navigate('/inbox')}>
          <span className="pill-icon" aria-hidden="true">
            <svg viewBox="0 0 24 24">
              <path d="M4 7h16l-2.4 9.6a2 2 0 0 1-2 1.4H8.4a2 2 0 0 1-2-1.4L4 7z" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinejoin="round" />
              <path d="M9 12h6" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" />
            </svg>
          </span>
          Inbox
        </button>
        <button className="pill-card" type="button" onClick={() => navigate('/calls')}>
          <span className="pill-icon" aria-hidden="true">
            <svg viewBox="0 0 24 24">
              <path d="M6 5h12v14H6z" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinejoin="round" />
              <path d="M9 9h6M9 13h6M9 17h4" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" />
            </svg>
          </span>
          Call Logs
        </button>
      </div>

      <div className="banner-card">
        <div className="banner-content">
          <div className="banner-title">Tip: Keep your console open</div>
          <div className="banner-subtitle">Answer in the Mini App to connect incoming calls instantly.</div>
        </div>
        <div className="banner-dots">
          <span className="dot active" />
          <span className="dot" />
          <span className="dot" />
        </div>
      </div>

      <div className="card-section">
        <div className="card-header">
          <span>Inbound queue</span>
          <Button size="s" mode="bezeled" onClick={() => navigate('/inbox')}>
            View all
          </Button>
        </div>
        {inboundQueue.length === 0 ? (
          <div className="empty-card">
            <div className="empty-title">No inbound calls</div>
            <div className="empty-subtitle">You are all caught up.</div>
          </div>
        ) : (
          <div className="card-list">
            {inboundQueue.slice(0, 3).map((call) => (
              <div key={call.call_sid} className="card-item">
                <div className="card-item-main">
                  <div className="card-item-title">Inbound call</div>
                  <div className="card-item-subtitle">{call.route_label || call.script || 'Inbound route'}</div>
                  <div className="card-item-meta">{call.from || 'Unknown caller'}</div>
                </div>
                <span className="tag">{call.decision || 'pending'}</span>
              </div>
            ))}
          </div>
        )}
      </div>

      <div className="card-section">
        <div className="card-header">
          <span>Recent calls</span>
          <span className="card-header-muted">
            {loading && calls.length === 0 ? 'Loading calls...' : `Showing ${calls.slice(0, 5).length}`}
          </span>
        </div>
        <div className="card-list">
          {calls.slice(0, 5).map((call) => (
            <button
              key={call.call_sid}
              type="button"
              className="card-item card-item-button"
              onClick={() => navigate(`/calls/${call.call_sid}`)}
            >
              <div className="card-item-main">
                <div className="card-item-title">{call.phone_number || call.call_sid}</div>
                <div className="card-item-subtitle">{call.created_at || '-'}</div>
              </div>
              <span className="tag outline">{call.status || 'unknown'}</span>
            </button>
          ))}
        </div>
      </div>

      {isAdmin && (
        <div className="card-section">
        <div className="card-header">
          <span>Callbacks</span>
          <span className="card-header-muted">{callbackTasks.length ? `${callbackTasks.length} scheduled` : 'None'}</span>
        </div>
        {callbackTasks.length === 0 ? (
          <div className="empty-card">
            <div className="empty-title">No callbacks queued</div>
            <div className="empty-subtitle">Callback tasks will show up here.</div>
          </div>
        ) : (
          <div className="card-list">
            {callbackTasks.map((task) => (
              <div key={task.id} className="card-item">
                <div className="card-item-main">
                  <div className="card-item-title">{task.number}</div>
                  <div className="card-item-subtitle">Run at {task.run_at}</div>
                </div>
                <span className="tag outline">callback</span>
              </div>
            ))}
          </div>
        )}
      </div>
      )}
    </div>
  );
}
