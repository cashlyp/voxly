import { useEffect, useState } from 'react';
import { apiFetch } from '../lib/api';
import { useCalls } from '../state/calls';
import { navigate } from '../lib/router';

export function Inbox() {
  const { inboundQueue, fetchInboundQueue } = useCalls();
  const [busyCall, setBusyCall] = useState<string | null>(null);

  useEffect(() => {
    fetchInboundQueue();
    const timer = window.setInterval(fetchInboundQueue, 5000);
    return () => window.clearInterval(timer);
  }, [fetchInboundQueue]);

  const handleAction = async (callSid: string, action: 'answer' | 'decline') => {
    setBusyCall(callSid);
    try {
      await apiFetch(`/webapp/inbound/${callSid}/${action}`, { method: 'POST' });
      if (action === 'answer') {
        navigate(`/calls/${callSid}`);
      }
      await fetchInboundQueue();
    } finally {
      setBusyCall(null);
    }
  };

  return (
    <section className="stack">
      <div className="panel">
        <h2>Inbound queue</h2>
        {inboundQueue.length === 0 ? (
          <p className="muted">No inbound calls at the moment.</p>
        ) : (
          <div className="list">
            {inboundQueue.map((call) => (
              <div className="list-item" key={call.call_sid}>
                <div>
                  <strong>{call.from || 'Unknown caller'}</strong>
                  <p className="muted">{call.route_label || call.script || 'Inbound call'}</p>
                </div>
                <div className="actions">
                  {call.decision && <span className={`badge ${call.decision}`}>{call.decision}</span>}
                  <button
                    type="button"
                    className="btn"
                    disabled={busyCall === call.call_sid || call.decision === 'answered' || call.decision === 'declined'}
                    onClick={() => handleAction(call.call_sid, 'answer')}
                  >
                    Answer
                  </button>
                  <button
                    type="button"
                    className="btn danger"
                    disabled={busyCall === call.call_sid || call.decision === 'answered' || call.decision === 'declined'}
                    onClick={() => handleAction(call.call_sid, 'decline')}
                  >
                    Decline
                  </button>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </section>
  );
}
