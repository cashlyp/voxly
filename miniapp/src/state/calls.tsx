import {
  createContext,
  useCallback,
  useContext,
  useMemo,
  useState,
  type PropsWithChildren,
} from 'react';
import { apiFetch } from '../lib/api';

export type CallRecord = {
  call_sid: string;
  status?: string | null;
  direction?: string | null;
  phone_number?: string | null;
  created_at?: string | null;
  duration?: number | null;
  answered_by?: string | null;
  inbound_gate?: {
    status?: string | null;
    decision_by?: string | null;
    decision_at?: string | null;
  } | null;
  live?: Record<string, unknown> | null;
};

export type LiveCall = {
  call_sid: string;
  inbound?: boolean;
  status?: string | null;
  status_label?: string | null;
  phase?: string | null;
  from?: string | null;
  to?: string | null;
  script?: string | null;
  route_label?: string | null;
  decision?: string;
  decision_by?: string | null;
  decision_at?: string | null;
  priority?: string | null;
  rule_summary?: {
    decision?: string | null;
    label?: string | null;
    risk?: string | null;
    recent_calls?: number | null;
  } | null;
};

export type CallEvent = {
  state: string;
  data: Record<string, unknown> | string | null;
  timestamp: string;
  sequence_number: number;
};

export type InboundNotice = {
  message: string;
  level?: 'info' | 'warning' | 'danger';
  pending_count?: number;
};

type CallsState = {
  calls: CallRecord[];
  inboundQueue: LiveCall[];
  inboundNotice: InboundNotice | null;
  activeCall: CallRecord | null;
  callEvents: CallEvent[];
  callEventsById: Record<string, CallEvent[]>;
  eventCursorById: Record<string, number>;
  nextCursor: number | null;
  loading: boolean;
  error?: string | null;
  fetchCalls: (options?: { limit?: number; cursor?: number; status?: string; q?: string }) => Promise<void>;
  fetchInboundQueue: () => Promise<void>;
  fetchCall: (callSid: string) => Promise<void>;
  fetchCallEvents: (callSid: string, after?: number) => Promise<void>;
  clearActive: () => void;
};

const CallsContext = createContext<CallsState | null>(null);

export function CallsProvider({ children }: PropsWithChildren) {
  const [calls, setCalls] = useState<CallRecord[]>([]);
  const [inboundQueue, setInboundQueue] = useState<LiveCall[]>([]);
  const [inboundNotice, setInboundNotice] = useState<InboundNotice | null>(null);
  const [activeCall, setActiveCall] = useState<CallRecord | null>(null);
  const [callEvents, setCallEvents] = useState<CallEvent[]>([]);
  const [callEventsById, setCallEventsById] = useState<Record<string, CallEvent[]>>({});
  const [eventCursorById, setEventCursorById] = useState<Record<string, number>>({});
  const [nextCursor, setNextCursor] = useState<number | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetchCalls = useCallback(async (options: { limit?: number; cursor?: number; status?: string; q?: string } = {}) => {
    setLoading(true);
    setError(null);
    try {
      const params = new URLSearchParams();
      if (options.limit) params.set('limit', String(options.limit));
      if (options.cursor !== undefined) params.set('cursor', String(options.cursor));
      if (options.status) params.set('status', options.status);
      if (options.q) params.set('q', options.q);
      const response = await apiFetch<{
        ok: boolean;
        calls: CallRecord[];
        next_cursor: number | null;
      }>(`/webapp/calls?${params.toString()}`);
      setCalls(response.calls || []);
      setNextCursor(response.next_cursor ?? null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load calls');
    } finally {
      setLoading(false);
    }
  }, []);

  const fetchInboundQueue = useCallback(async () => {
    setError(null);
    try {
      const response = await apiFetch<{ ok: boolean; calls: LiveCall[]; notice?: InboundNotice | null }>(
        '/webapp/inbound/queue',
      );
      setInboundQueue(response.calls || []);
      setInboundNotice(response.notice ?? null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load inbound queue');
    }
  }, []);

  const fetchCall = useCallback(async (callSid: string) => {
    setLoading(true);
    setError(null);
    try {
      const response = await apiFetch<{ ok: boolean; call: CallRecord; inbound_gate?: CallRecord['inbound_gate']; live?: Record<string, unknown> | null }>(
        `/webapp/calls/${callSid}`,
      );
      const merged = {
        ...response.call,
        inbound_gate: response.inbound_gate ?? response.call.inbound_gate ?? null,
        live: response.live ?? response.call.live ?? null,
      };
      setActiveCall(merged);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load call');
    } finally {
      setLoading(false);
    }
  }, []);

  const fetchCallEvents = useCallback(async (callSid: string, after = 0) => {
    setError(null);
    try {
      const params = new URLSearchParams();
      if (after) params.set('after', String(after));
      const response = await apiFetch<{ ok: boolean; events: CallEvent[]; latest_sequence?: number }>(
        `/webapp/calls/${callSid}/events?${params.toString()}`,
      );
      const incoming = response.events || [];
      setCallEventsById((prev) => {
        const existing = prev[callSid] || [];
        const merged = after > 0 ? [...existing, ...incoming] : incoming;
        const deduped = merged.filter((event, index, arr) => arr.findIndex((item) => item.sequence_number === event.sequence_number) === index);
        return { ...prev, [callSid]: deduped };
      });
      setEventCursorById((prev) => ({
        ...prev,
        [callSid]: response.latest_sequence ?? (incoming.length ? incoming[incoming.length - 1].sequence_number : prev[callSid] || 0),
      }));
      setCallEvents((prev) => {
        if (after > 0) {
          const merged = [...prev, ...incoming];
          return merged.filter((event, index, arr) => arr.findIndex((item) => item.sequence_number === event.sequence_number) === index);
        }
        return incoming;
      });
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load call events');
    }
  }, []);

  const clearActive = useCallback(() => {
    setActiveCall(null);
    setCallEvents([]);
  }, []);

  const value = useMemo<CallsState>(() => ({
    calls,
    inboundQueue,
    inboundNotice,
    activeCall,
    callEvents,
    callEventsById,
    eventCursorById,
    nextCursor,
    loading,
    error,
    fetchCalls,
    fetchInboundQueue,
    fetchCall,
    fetchCallEvents,
    clearActive,
  }), [
    calls,
    inboundQueue,
    inboundNotice,
    activeCall,
    callEvents,
    callEventsById,
    eventCursorById,
    nextCursor,
    loading,
    error,
    fetchCalls,
    fetchInboundQueue,
    fetchCall,
    fetchCallEvents,
    clearActive,
  ]);

  return (
    <CallsContext.Provider value={value}>
      {children}
    </CallsContext.Provider>
  );
}

export function useCalls() {
  const context = useContext(CallsContext);
  if (!context) {
    throw new Error('useCalls must be used within CallsProvider');
  }
  return context;
}
