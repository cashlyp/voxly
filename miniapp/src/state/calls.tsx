import {
  createContext,
  useCallback,
  useContext,
  useMemo,
  useState,
  type PropsWithChildren,
} from "react";
import { ApiError, apiFetch } from "../lib/api";

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
  level?: "info" | "warning" | "danger";
  pending_count?: number;
};

type ErrorKind = "offline" | "server" | "unknown";

type FetchMeta = {
  loading: boolean;
  refreshing: boolean;
  stale: boolean;
  updatedAt: number | null;
  error: string | null;
  errorKind: ErrorKind | null;
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
  callsMeta: FetchMeta;
  inboundMeta: FetchMeta;
  activeMeta: FetchMeta;
  fetchCalls: (options?: {
    limit?: number;
    cursor?: number;
    status?: string;
    q?: string;
  }) => Promise<void>;
  fetchInboundQueue: () => Promise<void>;
  fetchCall: (
    callSid: string,
    options?: {
      force?: boolean;
    },
  ) => Promise<void>;
  fetchCallEvents: (callSid: string, after?: number) => Promise<void>;
  clearActive: () => void;
};

const CALLS_CACHE_TTL_MS = 30000;
const CALL_STATUS_CACHE_TTL_MS = 15000;
const INBOUND_CACHE_TTL_MS = 10000;

const callsCache = new Map<
  string,
  { calls: CallRecord[]; nextCursor: number | null; fetchedAt: number }
>();
const callStatusCache = new Map<
  string,
  { call: CallRecord; fetchedAt: number }
>();
const inboundCache: {
  calls: LiveCall[];
  notice: InboundNotice | null;
  fetchedAt: number;
} = {
  calls: [],
  notice: null,
  fetchedAt: 0,
};

const baseMeta: FetchMeta = {
  loading: false,
  refreshing: false,
  stale: false,
  updatedAt: null,
  error: null,
  errorKind: null,
};

function buildCallsCacheKey(options: {
  limit?: number;
  cursor?: number;
  status?: string;
  q?: string;
}) {
  return JSON.stringify({
    limit: options.limit ?? 20,
    cursor: options.cursor ?? 0,
    status: options.status ?? "",
    q: options.q ?? "",
  });
}

function describeError(error: unknown): { message: string; kind: ErrorKind } {
  if (error instanceof ApiError) {
    if (error.status === 0) {
      return {
        message: error.message || "You're offline. Check your connection.",
        kind: "offline",
      };
    }
    return {
      message: error.message || "Server error. Please try again.",
      kind: "server",
    };
  }
  return {
    message: error instanceof Error ? error.message : "Something went wrong.",
    kind: "unknown",
  };
}

const CallsContext = createContext<CallsState | null>(null);

export function CallsProvider({ children }: PropsWithChildren) {
  const [calls, setCalls] = useState<CallRecord[]>([]);
  const [inboundQueue, setInboundQueue] = useState<LiveCall[]>([]);
  const [inboundNotice, setInboundNotice] = useState<InboundNotice | null>(
    null,
  );
  const [activeCall, setActiveCall] = useState<CallRecord | null>(null);
  const [callEvents, setCallEvents] = useState<CallEvent[]>([]);
  const [callEventsById, setCallEventsById] = useState<
    Record<string, CallEvent[]>
  >({});
  const [eventCursorById, setEventCursorById] = useState<
    Record<string, number>
  >({});
  const [nextCursor, setNextCursor] = useState<number | null>(null);
  const [callsMeta, setCallsMeta] = useState<FetchMeta>(baseMeta);
  const [inboundMeta, setInboundMeta] = useState<FetchMeta>(baseMeta);
  const [activeMeta, setActiveMeta] = useState<FetchMeta>(baseMeta);

  const fetchCalls = useCallback(
    async (
      options: {
        limit?: number;
        cursor?: number;
        status?: string;
        q?: string;
      } = {},
    ) => {
      const cacheKey = buildCallsCacheKey(options);
      const cached = callsCache.get(cacheKey);
      const now = Date.now();
      const stale =
        cached && now - cached.fetchedAt > CALLS_CACHE_TTL_MS ? true : false;

      if (cached) {
        setCalls(cached.calls);
        setNextCursor(cached.nextCursor ?? null);
        setCallsMeta((prev) => ({
          ...prev,
          loading: false,
          refreshing: false,
          stale,
          updatedAt: cached.fetchedAt,
          error: null,
          errorKind: null,
        }));
        if (!stale) return;
      }

      setCallsMeta({
        loading: !cached,
        refreshing: !!cached,
        stale: !!cached,
        updatedAt: cached?.fetchedAt ?? null,
        error: null,
        errorKind: null,
      });

      try {
        const params = new URLSearchParams();
        if (options.limit !== undefined)
          params.set("limit", String(options.limit));
        if (options.cursor !== undefined)
          params.set("cursor", String(options.cursor));
        if (options.status !== undefined && options.status !== null)
          params.set("status", options.status);
        if (options.q !== undefined && options.q !== null)
          params.set("q", options.q);
        const response = await apiFetch<{
          ok: boolean;
          calls: CallRecord[];
          next_cursor: number | null;
        }>(`/webapp/calls?${params.toString()}`);
        const fetchedAt = Date.now();
        const next = response.calls;
        callsCache.set(cacheKey, {
          calls: next,
          nextCursor: response.next_cursor ?? null,
          fetchedAt,
        });
        setCalls(next);
        setNextCursor(response.next_cursor ?? null);
        setCallsMeta({
          loading: false,
          refreshing: false,
          stale: false,
          updatedAt: fetchedAt,
          error: null,
          errorKind: null,
        });
      } catch (err) {
        const info = describeError(err);
        setCallsMeta({
          loading: false,
          refreshing: false,
          stale: !!cached,
          updatedAt: cached?.fetchedAt ?? null,
          error: info.message,
          errorKind: info.kind,
        });
      }
    },
    [],
  );

  const fetchInboundQueue = useCallback(async () => {
    const now = Date.now();
    const cached =
      inboundCache.fetchedAt > 0
        ? {
            calls: inboundCache.calls,
            notice: inboundCache.notice,
            fetchedAt: inboundCache.fetchedAt,
          }
        : null;
    const stale =
      cached && now - cached.fetchedAt > INBOUND_CACHE_TTL_MS ? true : false;

    if (cached) {
      setInboundQueue(cached.calls);
      setInboundNotice(cached.notice ?? null);
      setInboundMeta({
        loading: false,
        refreshing: false,
        stale,
        updatedAt: cached.fetchedAt,
        error: null,
        errorKind: null,
      });
      if (!stale) return;
    }

    setInboundMeta({
      loading: !cached,
      refreshing: !!cached,
      stale: !!cached,
      updatedAt: cached?.fetchedAt ?? null,
      error: null,
      errorKind: null,
    });
    try {
      const response = await apiFetch<{
        ok: boolean;
        calls: LiveCall[];
        notice?: InboundNotice | null;
      }>("/webapp/inbound/queue");
      const fetchedAt = Date.now();
      inboundCache.calls = response.calls;
      inboundCache.notice = response.notice ?? null;
      inboundCache.fetchedAt = fetchedAt;
      setInboundQueue(response.calls);
      setInboundNotice(response.notice ?? null);
      setInboundMeta({
        loading: false,
        refreshing: false,
        stale: false,
        updatedAt: fetchedAt,
        error: null,
        errorKind: null,
      });
    } catch (err) {
      const info = describeError(err);
      setInboundMeta({
        loading: false,
        refreshing: false,
        stale: !!cached,
        updatedAt: cached?.fetchedAt ?? null,
        error: info.message,
        errorKind: info.kind,
      });
      throw err;
    }
  }, []);

  const fetchCall = useCallback(
    async (callSid: string, options: { force?: boolean } = {}) => {
      const cached = callStatusCache.get(callSid);
      const now = Date.now();
      const stale =
        cached && now - cached.fetchedAt > CALL_STATUS_CACHE_TTL_MS
          ? true
          : false;

      if (cached) {
        setActiveCall(cached.call);
        setActiveMeta((prev) => ({
          ...prev,
          loading: false,
          refreshing: false,
          stale,
          updatedAt: cached.fetchedAt,
          error: null,
          errorKind: null,
        }));
        const force = options.force === true;
        if (!stale && !force) return;
      }

      setActiveMeta({
        loading: !cached,
        refreshing: !!cached,
        stale: !!cached,
        updatedAt: cached?.fetchedAt ?? null,
        error: null,
        errorKind: null,
      });

      try {
        const response = await apiFetch<{
          ok: boolean;
          call: CallRecord;
          inbound_gate?: CallRecord["inbound_gate"];
          live?: Record<string, unknown> | null;
        }>(`/webapp/calls/${callSid}`);
        const merged = {
          ...response.call,
          inbound_gate:
            response.inbound_gate ?? response.call.inbound_gate ?? null,
          live: response.live ?? response.call.live ?? null,
        };
        const fetchedAt = Date.now();
        callStatusCache.set(callSid, { call: merged, fetchedAt });
        setActiveCall(merged);
        setActiveMeta({
          loading: false,
          refreshing: false,
          stale: false,
          updatedAt: fetchedAt,
          error: null,
          errorKind: null,
        });
      } catch (err) {
        const info = describeError(err);
        setActiveMeta({
          loading: false,
          refreshing: false,
          stale: !!cached,
          updatedAt: cached?.fetchedAt ?? null,
          error: info.message,
          errorKind: info.kind,
        });
      }
    },
    [],
  );

  const fetchCallEvents = useCallback(async (callSid: string, after = 0) => {
    try {
      const params = new URLSearchParams();
      if (after) params.set("after", String(after));
      const response = await apiFetch<{
        ok: boolean;
        events: CallEvent[];
        latest_sequence?: number;
      }>(`/webapp/calls/${callSid}/events?${params.toString()}`);
      const incoming = response.events;
      setCallEventsById((prev) => {
        const existing = prev[callSid] ?? [];
        const merged = after > 0 ? [...existing, ...incoming] : incoming;
        const deduped = merged.filter(
          (event: CallEvent, index: number, arr: CallEvent[]) =>
            arr.findIndex(
              (item: CallEvent) =>
                item.sequence_number === event.sequence_number,
            ) === index,
        );
        return { ...prev, [callSid]: deduped };
      });
      setEventCursorById((prev) => ({
        ...prev,
        [callSid]:
          response.latest_sequence ??
          (incoming.length > 0
            ? incoming[incoming.length - 1].sequence_number
            : prev[callSid] ?? 0),
      }));
      setCallEvents((prev) => {
        if (after > 0) {
          const merged = [...prev, ...incoming];
          return merged.filter(
            (event: CallEvent, index: number, arr: CallEvent[]) =>
              arr.findIndex(
                (item: CallEvent) =>
                  item.sequence_number === event.sequence_number,
              ) === index,
          );
        }
        return incoming;
      });
    } catch (err) {
      const info = describeError(err);
      setActiveMeta((prev) => ({
        ...prev,
        error: info.message,
        errorKind: info.kind,
      }));
    }
  }, []);

  const clearActive = useCallback(() => {
    setActiveCall(null);
    setCallEvents([]);
    setActiveMeta(baseMeta);
  }, []);

  const value = useMemo<CallsState>(
    () => ({
      calls,
      inboundQueue,
      inboundNotice,
      activeCall,
      callEvents,
      callEventsById,
      eventCursorById,
      nextCursor,
      callsMeta,
      inboundMeta,
      activeMeta,
      fetchCalls,
      fetchInboundQueue,
      fetchCall,
      fetchCallEvents,
      clearActive,
    }),
    [
      calls,
      inboundQueue,
      inboundNotice,
      activeCall,
      callEvents,
      callEventsById,
      eventCursorById,
      nextCursor,
      callsMeta,
      inboundMeta,
      activeMeta,
      fetchCalls,
      fetchInboundQueue,
      fetchCall,
      fetchCallEvents,
      clearActive,
    ],
  );

  return (
    <CallsContext.Provider value={value}>{children}</CallsContext.Provider>
  );
}

export function useCalls() {
  const context = useContext(CallsContext);
  if (!context) {
    throw new Error("useCalls must be used within CallsProvider");
  }
  return context;
}
