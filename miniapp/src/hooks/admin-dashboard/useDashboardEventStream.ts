import { useEffect, useRef, useState } from 'react';

export type StreamConnectionMode = 'disabled' | 'connecting' | 'connected' | 'fallback';

type UseDashboardEventStreamOptions = {
  enabled: boolean;
  token: string | null;
  buildEventStreamUrl: (path: string, token: string) => string;
  applyStreamPayload: (raw: unknown) => boolean;
  refreshPoll: () => Promise<boolean>;
  reconnectBaseMs: number;
  reconnectMaxMs: number;
  refreshDebounceMs: number;
};

type UseDashboardEventStreamResult = {
  streamMode: StreamConnectionMode;
  streamConnected: boolean;
  streamFailureCount: number;
  streamLastEventAt: number | null;
};

export function useDashboardEventStream({
  enabled,
  token,
  buildEventStreamUrl,
  applyStreamPayload,
  refreshPoll,
  reconnectBaseMs,
  reconnectMaxMs,
  refreshDebounceMs,
}: UseDashboardEventStreamOptions): UseDashboardEventStreamResult {
  const [streamMode, setStreamMode] = useState<StreamConnectionMode>('disabled');
  const [streamConnected, setStreamConnected] = useState<boolean>(false);
  const [streamFailureCount, setStreamFailureCount] = useState<number>(0);
  const [streamLastEventAt, setStreamLastEventAt] = useState<number | null>(null);
  const refreshTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => () => {
    if (refreshTimerRef.current) {
      clearTimeout(refreshTimerRef.current);
      refreshTimerRef.current = null;
    }
  }, []);

  useEffect(() => {
    if (!enabled || !token) {
      setStreamMode('disabled');
      setStreamConnected(false);
      return undefined;
    }
    if (typeof EventSource === 'undefined') {
      setStreamMode('fallback');
      setStreamConnected(false);
      return undefined;
    }

    let disposed = false;
    let reconnectTimer: ReturnType<typeof setTimeout> | null = null;
    let activeStream: EventSource | null = null;
    let attemptCount = 0;
    let endpointIndex = 0;
    const endpoints = ['/miniapp/events', '/miniapp/stream'];

    const closeStream = (): void => {
      if (activeStream) {
        activeStream.close();
        activeStream = null;
      }
    };

    const scheduleStreamRefresh = (): void => {
      if (refreshTimerRef.current) return;
      refreshTimerRef.current = setTimeout(() => {
        refreshTimerRef.current = null;
        void refreshPoll();
      }, refreshDebounceMs);
    };

    const connect = (): void => {
      if (disposed) return;
      setStreamMode('connecting');
      const nextEndpoint = endpoints[Math.max(0, Math.min(endpointIndex, endpoints.length - 1))];
      const streamUrl = buildEventStreamUrl(nextEndpoint, token);
      const source = new EventSource(streamUrl);
      activeStream = source;

      source.onopen = () => {
        if (disposed) return;
        attemptCount = 0;
        setStreamMode('connected');
        setStreamConnected(true);
        setStreamFailureCount(0);
      };

      source.onmessage = (event) => {
        if (disposed) return;
        setStreamLastEventAt(Date.now());
        const eventText = typeof event.data === 'string' ? event.data : String(event.data ?? '');
        let parsed: unknown = eventText;
        try {
          parsed = JSON.parse(eventText);
        } catch {
          // Accept plain text event frames and trigger poll refresh below.
        }
        const applied = applyStreamPayload(parsed);
        if (!applied) {
          scheduleStreamRefresh();
        }
      };

      source.onerror = () => {
        if (disposed) return;
        closeStream();
        setStreamConnected(false);
        if (endpointIndex < endpoints.length - 1) {
          endpointIndex += 1;
          connect();
          return;
        }
        endpointIndex = 0;
        attemptCount += 1;
        setStreamMode('fallback');
        setStreamFailureCount((prev) => prev + 1);
        const delay = Math.min(
          reconnectMaxMs,
          Math.round(reconnectBaseMs * Math.pow(1.5, Math.max(0, attemptCount - 1))),
        );
        reconnectTimer = setTimeout(() => {
          reconnectTimer = null;
          connect();
        }, delay);
      };
    };

    connect();
    return () => {
      disposed = true;
      closeStream();
      if (reconnectTimer) {
        clearTimeout(reconnectTimer);
      }
    };
  }, [
    applyStreamPayload,
    buildEventStreamUrl,
    enabled,
    reconnectBaseMs,
    reconnectMaxMs,
    refreshDebounceMs,
    refreshPoll,
    token,
  ]);

  return {
    streamMode,
    streamConnected,
    streamFailureCount,
    streamLastEventAt,
  };
}
