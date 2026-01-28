import { useEffect, useMemo, useState } from 'react';
import { connectEventStream, type WebappEvent } from '../lib/realtime';
import { ensureAuth } from '../lib/auth';
import { apiFetch } from '../lib/api';
import { useCalls } from '../state/calls';
import { useUser } from '../state/user';

type TranscriptEntry = {
  speaker: string;
  message: string;
  ts: string;
  partial: boolean;
};

export function CallConsole({ callSid }: { callSid: string }) {
  const { activeCall, callEventsById, eventCursorById, fetchCall, fetchCallEvents } = useCalls();
  const { roles } = useUser();
  const isAdmin = roles.includes('admin');
  const [liveEvents, setLiveEvents] = useState<WebappEvent[]>([]);
  const [transcript, setTranscript] = useState<TranscriptEntry[]>([]);
  const [streamHealth, setStreamHealth] = useState<{ latencyMs?: number; jitterMs?: number; packetLossPct?: number; asrConfidence?: number } | null>(null);
  const [connectionStatus, setConnectionStatus] = useState<'connecting' | 'open' | 'error'>('connecting');
  const [actionBusy, setActionBusy] = useState<string | null>(null);
  const [scripts, setScripts] = useState<{ id: number; name: string }[]>([]);
  const [selectedScript, setSelectedScript] = useState<number | null>(null);

  useEffect(() => {
    fetchCall(callSid);
    fetchCallEvents(callSid, 0);
    setLiveEvents([]);
    setTranscript([]);
    setStreamHealth(null);
  }, [callSid, fetchCall, fetchCallEvents]);

  useEffect(() => {
    let stream: { close: () => void } | null = null;
    let cancelled = false;
    ensureAuth()
      .then((session) => {
        if (cancelled) return;
        setConnectionStatus('connecting');
        stream = connectEventStream({
          token: session.token,
          onEvent: (event) => {
            if (event.call_sid !== callSid) return;
            setLiveEvents((prev) => [...prev.slice(-50), event]);
            if (event.type === 'transcript.partial' || event.type === 'transcript.final') {
              const entry: TranscriptEntry = {
                speaker: String(event.data?.speaker || 'unknown'),
                message: String(event.data?.message || ''),
                ts: event.ts,
                partial: event.type === 'transcript.partial',
              };
              setTranscript((prev) => [...prev.slice(-100), entry]);
            }
            if (event.type === 'stream.health') {
              const metrics = event.data?.metrics as { latencyMs?: number; jitterMs?: number; packetLossPct?: number; asrConfidence?: number } | undefined;
              if (metrics) {
                setStreamHealth(metrics);
              }
            }
            if (['call.updated', 'call.ended', 'inbound.ringing'].includes(event.type)) {
              fetchCall(callSid);
            }
          },
          onError: () => setConnectionStatus('error'),
          onOpen: () => setConnectionStatus('open'),
        });
      })
      .catch(() => {
        setConnectionStatus('error');
      });
    return () => {
      cancelled = true;
      if (stream) stream.close();
    };
  }, [callSid, fetchCall]);

  useEffect(() => {
    const interval = window.setInterval(() => {
      const cursor = eventCursorById[callSid] || 0;
      fetchCallEvents(callSid, cursor);
    }, 5000);
    return () => window.clearInterval(interval);
  }, [callSid, eventCursorById, fetchCallEvents]);

  useEffect(() => {
    if (!isAdmin) return;
    apiFetch<{ ok: boolean; scripts: { id: number; name: string }[] }>('/webapp/scripts')
      .then((response) => setScripts(response.scripts || []))
      .catch(() => {});
  }, [isAdmin]);

  const statusLine = useMemo(() => {
    if (!activeCall) return 'Loading call...';
    return `${activeCall.status || 'unknown'} - ${activeCall.direction || 'n/a'}`;
  }, [activeCall]);

  const timeline = callEventsById[callSid] || [];

  const handleInboundAction = async (action: 'answer' | 'decline') => {
    setActionBusy(action);
    try {
      await apiFetch(`/webapp/inbound/${callSid}/${action}`, { method: 'POST' });
      await fetchCall(callSid);
    } finally {
      setActionBusy(null);
    }
  };

  const handleStreamAction = async (action: 'retry' | 'fallback' | 'end') => {
    setActionBusy(action);
    try {
      if (action === 'end') {
        await apiFetch(`/webapp/calls/${callSid}/end`, { method: 'POST' });
      } else {
        await apiFetch(`/webapp/calls/${callSid}/stream/${action}`, { method: 'POST' });
      }
      await fetchCall(callSid);
    } finally {
      setActionBusy(null);
    }
  };

  const handleScriptInject = async () => {
    if (!selectedScript) return;
    setActionBusy('script');
    try {
      await apiFetch(`/webapp/calls/${callSid}/script`, {
        method: 'POST',
        body: { script_id: selectedScript },
      });
    } finally {
      setActionBusy(null);
    }
  };

  return (
    <section className="stack">
      <div className="panel">
        <h2>Live Call Console</h2>
        <p className="muted">{callSid}</p>
        <p className="muted">Status: {statusLine}</p>
        <p className="muted">Realtime: {connectionStatus}</p>
        {streamHealth && (
          <p className="muted">
            Stream health: latency {streamHealth.latencyMs ?? '-'}ms | jitter {streamHealth.jitterMs ?? '-'}ms | loss {streamHealth.packetLossPct ?? '-'}% | asr {streamHealth.asrConfidence ?? '-'}
          </p>
        )}
        <div className="actions">
          <button type="button" className="btn ghost" onClick={() => fetchCallEvents(callSid, 0)}>
            Refresh timeline
          </button>
        </div>
      </div>

      {isAdmin && (
        <div className="panel">
          <h3>Actions</h3>
          {activeCall?.inbound_gate?.status === 'pending' && (
            <div className="actions">
              <button type="button" className="btn" onClick={() => handleInboundAction('answer')} disabled={!!actionBusy}>
                Answer
              </button>
              <button type="button" className="btn danger" onClick={() => handleInboundAction('decline')} disabled={!!actionBusy}>
                Decline
              </button>
            </div>
          )}
          <div className="actions">
            <button type="button" className="btn ghost" onClick={() => handleStreamAction('retry')} disabled={!!actionBusy}>
              Retry stream
            </button>
            <button type="button" className="btn ghost" onClick={() => handleStreamAction('fallback')} disabled={!!actionBusy}>
              Switch to keypad
            </button>
            <button type="button" className="btn danger" onClick={() => handleStreamAction('end')} disabled={!!actionBusy}>
              End call
            </button>
          </div>
          {scripts.length > 0 && (
            <div className="form inline">
              <select
                value={selectedScript ?? ''}
                onChange={(event) => {
                  const value = event.target.value;
                  setSelectedScript(value ? Number(value) : null);
                }}
              >
                <option value="">Select script</option>
                {scripts.map((script) => (
                  <option key={script.id} value={script.id}>{script.name}</option>
                ))}
              </select>
              <button type="button" className="btn" onClick={handleScriptInject} disabled={!selectedScript || !!actionBusy}>
                Inject script
              </button>
            </div>
          )}
        </div>
      )}

      <div className="panel">
        <h3>Timeline</h3>
        {timeline.length === 0 ? (
          <p className="muted">No events yet.</p>
        ) : (
          <div className="list">
            {timeline.map((evt) => (
              <div className="list-item" key={`${evt.sequence_number}-${evt.state}`}>
                <div>
                  <strong>{evt.state}</strong>
                  <p className="muted">{evt.timestamp}</p>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      <div className="panel">
        <h3>Transcript</h3>
        {transcript.length === 0 ? (
          <p className="muted">Waiting for transcript...</p>
        ) : (
          <div className="list">
            {transcript.map((entry, index) => (
              <div className="list-item" key={`${entry.ts}-${index}`}>
                <div>
                  <strong>{entry.speaker}</strong>
                  <p className="muted">{entry.message}</p>
                </div>
                {entry.partial && <span className="badge partial">partial</span>}
              </div>
            ))}
          </div>
        )}
      </div>

      <div className="panel">
        <h3>Live events</h3>
        {liveEvents.length === 0 ? (
          <p className="muted">No realtime updates yet.</p>
        ) : (
          <div className="list">
            {liveEvents.slice(-15).map((event) => (
              <div className="list-item" key={`${event.sequence}-${event.type}`}>
                <div>
                  <strong>{event.type}</strong>
                  <p className="muted">{event.ts}</p>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </section>
  );
}
