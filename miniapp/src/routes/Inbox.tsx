import { useEffect, useState } from 'react';
import {
  Banner,
  Button,
  InlineButtons,
} from '@telegram-apps/telegram-ui';
import { apiFetch, createIdempotencyKey } from '../lib/api';
import { useCalls } from '../state/calls';
import { navigate } from '../lib/router';
import { confirmAction, hapticImpact, hapticSuccess, hapticError } from '../lib/ux';

export function Inbox() {
  const { inboundQueue, inboundNotice, fetchInboundQueue } = useCalls();
  const [busyCall, setBusyCall] = useState<string | null>(null);

  useEffect(() => {
    fetchInboundQueue();
    const timer = window.setInterval(fetchInboundQueue, 5000);
    return () => window.clearInterval(timer);
  }, [fetchInboundQueue]);

  const handleAction = async (callSid: string, action: 'answer' | 'decline') => {
    if (action === 'decline') {
      const confirmed = await confirmAction({
        title: 'Decline this call?',
        message: 'The caller will be rejected immediately.',
        confirmText: 'Decline',
        destructive: true,
      });
      if (!confirmed) return;
    }
    setBusyCall(callSid);
    hapticImpact();
    try {
      await apiFetch(`/webapp/inbound/${callSid}/${action}`, {
        method: 'POST',
        idempotencyKey: createIdempotencyKey(),
      });
      hapticSuccess();
      if (action === 'answer') {
        navigate(`/calls/${callSid}`);
      }
      await fetchInboundQueue();
    } catch (error) {
      hapticError();
      throw error;
    } finally {
      setBusyCall(null);
    }
  };

  return (
    <div className="wallet-page">
      {inboundNotice && (
        <Banner
          type="inline"
          header="Incoming call pending"
          description={inboundNotice.message}
          className="wallet-banner"
        />
      )}
      <div className="card-section">
        <div className="card-header">
          <span>Inbound queue</span>
          <Button size="s" mode="bezeled" onClick={() => fetchInboundQueue()}>
            Refresh
          </Button>
        </div>
        {inboundQueue.length === 0 ? (
          <div className="empty-card">
            <div className="empty-title">No inbound calls</div>
            <div className="empty-subtitle">Calls will appear here when ringing.</div>
          </div>
        ) : (
          <div className="card-list">
            {inboundQueue.map((call) => {
              const disabled = busyCall === call.call_sid || call.decision === 'answered' || call.decision === 'declined';
              return (
                <div key={call.call_sid} className="card-item">
                  <div className="card-item-main">
                    <div className="card-item-title">Inbound call</div>
                    <div className="card-item-subtitle">{call.route_label || call.script || 'Inbound route'}</div>
                    <div className="card-item-meta">{call.from || 'Unknown caller'}</div>
                  </div>
                  <InlineButtons mode="bezeled">
                    <InlineButtons.Item
                      text="Answer"
                      disabled={disabled}
                      onClick={() => handleAction(call.call_sid, 'answer')}
                    />
                    <InlineButtons.Item
                      text="Decline"
                      disabled={disabled}
                      onClick={() => handleAction(call.call_sid, 'decline')}
                    />
                  </InlineButtons>
                </div>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );
}
