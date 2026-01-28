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
}): EventStream {
  const { token, since, onEvent, onError, onOpen } = options;
  const query = new URLSearchParams();
  query.set('token', token);
  if (since && Number.isFinite(since)) {
    query.set('since', String(since));
  }
  const source = new EventSource(`/webapp/sse?${query.toString()}`);
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
  return {
    close: () => source.close(),
  };
}
