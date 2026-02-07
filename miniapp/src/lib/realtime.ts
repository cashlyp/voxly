import { getApiBase } from './api';

export type WebappEvent = {
  sequence: number;
  type: string;
  call_sid: string;
  data: Record<string, unknown>;
  ts: string;
};

export type EventStream = {
  close: () => void;
};

export function connectEventStream(options: {
  token: string;
  since?: number;
  onEvent: (event: WebappEvent) => void;
  onError?: (error: Event) => void;
  onOpen?: () => void;
  onHeartbeat?: (payload: { ts: string }) => void;
}): EventStream {
  const { token, since, onEvent, onError, onOpen, onHeartbeat } = options;
  const query = new URLSearchParams();
  query.set('token', token);
  if (since && Number.isFinite(since)) {
    query.set('since', String(since));
  }
  const apiBase = getApiBase();
  const url = `${apiBase}/webapp/sse?${query.toString()}`;
  const source = new EventSource(url);
  source.onmessage = (message) => {
    try {
      const payload = JSON.parse(message.data) as WebappEvent;
      onEvent(payload);
    } catch {
      // ignore parse errors
    }
  };
  source.onerror = (event) => {
    if (onError) onError(event);
  };
  source.onopen = () => {
    if (onOpen) onOpen();
  };
  source.addEventListener('heartbeat', (message) => {
    if (!onHeartbeat) return;
    try {
      const payload = JSON.parse((message).data) as { ts: string };
      onHeartbeat(payload);
    } catch {
      onHeartbeat({ ts: new Date().toISOString() });
    }
  });
  return {
    close: () => source.close(),
  };
}
