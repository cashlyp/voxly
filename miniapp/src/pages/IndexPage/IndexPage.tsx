import { initData, useSignal } from "@tma.js/sdk-react";
import { useEffect, useMemo, useRef, useState } from "react";

import { Page } from "@/components/Page.tsx";

import "./IndexPage.css";

const API_TIMEOUT_MS = 12000;
const API_RETRY_MAX = 2;

type MiniappEvent = {
  sequence: number;
  type: string;
  call_sid: string;
  data?: Record<string, unknown> | null;
  ts: string;
};

type CallSnapshot = {
  call_sid: string;
  inbound?: boolean;
  status?: string | null;
  status_label?: string | null;
  phase?: string | null;
  phase_label?: string | null;
  from?: string | null;
  to?: string | null;
  name?: string | null;
  route_label?: string | null;
  script?: string | null;
  last_events?: string[];
  gate_status?: string | null;
};

type CallDetail = {
  call: Record<string, unknown>;
  states: { state: string; timestamp: string }[];
};

function resolveInitialApiBase() {
  const urlParams = new URLSearchParams(window.location.search);
  const override = urlParams.get("api");
  if (override) {
    localStorage.setItem("miniapp_api_base", override);
  }
  return (
    override ||
    localStorage.getItem("miniapp_api_base") ||
    import.meta.env.VITE_API_BASE ||
    window.location.origin
  );
}

export const IndexPage = () => {
  const initDataRaw = useSignal(initData.raw);
  const [apiBase, setApiBase] = useState(resolveInitialApiBase());
  const [apiInput, setApiInput] = useState(apiBase);
  const [sessionToken, setSessionToken] = useState<string | null>(null);
  const [refreshToken, setRefreshToken] = useState<string | null>(null);
  const [status, setStatus] = useState("Connecting…");
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [roles, setRoles] = useState<string[]>(["admin"]);
  const [branding, setBranding] = useState<{ name?: string } | null>(null);
  const [theme, setTheme] = useState<Record<string, string> | null>(null);
  const [tab, setTab] = useState<"dashboard" | "live" | "calllog" | "settings">(
    "dashboard",
  );
  const [calls, setCalls] = useState<Record<string, CallSnapshot>>({});
  const [events, setEvents] = useState<MiniappEvent[]>([]);
  const [activeCallSid, setActiveCallSid] = useState<string | null>(null);
  const [callLog, setCallLog] = useState<Record<string, unknown>[]>([]);
  const [callDetails, setCallDetails] = useState<CallDetail | null>(null);
  const [streamState, setStreamState] = useState<
    "connected" | "reconnecting" | "disconnected"
  >("disconnected");
  const [logFilters, setLogFilters] = useState({
    status: "all",
    direction: "all",
    query: "",
    start: "",
    end: "",
  });
  const [accessAdminInput, setAccessAdminInput] = useState("");
  const [accessViewerInput, setAccessViewerInput] = useState("");
  const [accessEnvAdmins, setAccessEnvAdmins] = useState<string[]>([]);
  const [accessEnvViewers, setAccessEnvViewers] = useState<string[]>([]);
  const [accessEffective, setAccessEffective] = useState<{
    admins: string[];
    viewers: string[];
  }>({ admins: [], viewers: [] });
  const [accessLoaded, setAccessLoaded] = useState(false);
  const [accessNote, setAccessNote] = useState<string | null>(null);
  const [loadingActive, setLoadingActive] = useState(false);
  const [loadingLog, setLoadingLog] = useState(false);
  const [loadingDetails, setLoadingDetails] = useState(false);
  const eventSourceRef = useRef<EventSource | null>(null);
  const sessionTokenRef = useRef<string | null>(null);
  const refreshTokenRef = useRef<string | null>(null);
  const lastSequenceRef = useRef(0);
  const initialCallSidRef = useRef<string | null>(
    new URLSearchParams(window.location.search).get("call"),
  );

  const callList = useMemo(() => Object.values(calls), [calls]);
  const activeCall = activeCallSid ? calls[activeCallSid] : null;
  const isAdmin = useMemo(() => roles.includes("admin"), [roles]);

  function sleep(ms: number) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  function encodeInitData(raw: string) {
    try {
      return btoa(unescape(encodeURIComponent(raw)));
    } catch {
      return "";
    }
  }

  const metrics = useMemo(() => {
    const active = callList.length;
    const pending = callList.filter(
      (c) => c.inbound && ["ringing", "initiated"].includes(String(c.status)),
    ).length;
    const now = Date.now();
    const missed = callLog.filter((c) => {
      const statusValue = String(c.status || "").toLowerCase();
      const ts = c.created_at ? Date.parse(String(c.created_at)) : null;
      if (!ts) return false;
      return (
        now - ts <= 24 * 60 * 60 * 1000 &&
        ["no-answer", "failed", "canceled"].includes(statusValue)
      );
    }).length;
    const completed = callLog.filter(
      (c) => String(c.status || "").toLowerCase() === "completed",
    ).length;
    const total = callLog.length;
    const answerRate = total ? Math.round((completed / total) * 100) : 0;
    const durations = callLog
      .map((c) => Number(c.duration || 0))
      .filter((value) => Number.isFinite(value) && value > 0);
    const avgDuration = durations.length
      ? Math.round(
          durations.reduce((sum, val) => sum + val, 0) / durations.length,
        )
      : 0;
    return { active, pending, missed, answerRate, avgDuration };
  }, [callList, callLog]);

  useEffect(() => {
    setApiInput(apiBase);
  }, [apiBase]);

  useEffect(() => {
    if (tab === "settings" && isAdmin && !accessLoaded) {
      loadAccessControl().catch(() => {});
    }
  }, [tab, isAdmin, accessLoaded]);

  useEffect(() => {
    sessionTokenRef.current = sessionToken;
  }, [sessionToken]);

  useEffect(() => {
    refreshTokenRef.current = refreshToken;
  }, [refreshToken]);

  useEffect(() => {
    if (!theme) return;
    const root = document.documentElement;
    const mapping: Record<string, string> = {
      accent: "--va-accent",
      accent2: "--va-accent-2",
      background: "--va-bg",
      panel: "--va-panel",
      surface: "--va-surface",
      text: "--va-text",
      muted: "--va-muted",
    };
    Object.entries(mapping).forEach(([key, cssVar]) => {
      const value = theme[key];
      if (value) root.style.setProperty(cssVar, value);
    });
  }, [theme]);

  useEffect(() => {
    if (!initDataRaw) {
      setStatus("Missing init data.");
      return;
    }
    bootstrap();
    return () => {
      if (eventSourceRef.current !== null) {
        eventSourceRef.current.close();
      }
    };
  }, [initDataRaw, apiBase]);

  useEffect(() => {
    if (!sessionToken) return;
    connectStream();
  }, [sessionToken, apiBase]);

  async function refreshSession() {
    const refresh = refreshTokenRef.current || refreshToken;
    if (!refresh) return false;
    try {
      const res = await fetch(`${apiBase}/miniapp/refresh`, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${refresh}`,
          ...(initDataRaw ? { "X-Telegram-Init-Data": initDataRaw } : {}),
        },
      });
      if (!res.ok) {
        return false;
      }
      const data = await res.json();
      if (!data?.session_token || data.session_token === "") return false;
      setSessionToken(data.session_token);
      sessionTokenRef.current = data.session_token;
      const nextRefresh = data.refresh_token || refresh;
      setRefreshToken(nextRefresh);
      refreshTokenRef.current = nextRefresh;
      setRoles(data.roles || roles);
      setStatus("Connected");
      return true;
    } catch {
      return false;
    }
  }

  async function apiFetch(
    path: string,
    options: RequestInit = {},
    attempt = 0,
  ) {
    const headers: Record<string, string> = {};
    if (options.headers && typeof options.headers === "object") {
      Object.assign(headers, options.headers as Record<string, string>);
    }
    const token = sessionTokenRef.current || sessionToken;
    if (token !== null && token !== undefined) {
      headers.Authorization = `Bearer ${token}`;
    }
    if (initDataRaw !== null && initDataRaw !== undefined) {
      headers["X-Telegram-Init-Data"] = initDataRaw;
    }
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), API_TIMEOUT_MS);
    try {
      const res = await fetch(`${apiBase}${path}`, {
        ...options,
        headers,
        signal: controller.signal,
      });
      if (
        res.status === 401 &&
        attempt === 0 &&
        (refreshTokenRef.current !== null || refreshToken !== null)
      ) {
        const refreshed = await refreshSession();
        if (refreshed) {
          return apiFetch(path, options, attempt + 1);
        }
        setStatus("Session expired.");
      }
      if (
        (res.status === 429 || res.status >= 500) &&
        attempt < API_RETRY_MAX
      ) {
        const delay = 400 + Math.random() * 600 + attempt * 300;
        await sleep(delay);
        return apiFetch(path, options, attempt + 1);
      }
      if (res.status === 401 || res.status === 403) {
        setErrorMessage("Not authorized. Check admin access.");
      } else if (res.status === 429) {
        setErrorMessage("Rate limited. Please retry shortly.");
      } else if (res.status >= 500) {
        setErrorMessage("API error. Try again.");
      }
      return res;
    } catch (error) {
      if ((error as Error)?.name === "AbortError") {
        setErrorMessage("Request timed out. Check your connection.");
      } else {
        setErrorMessage("Network error. API unreachable.");
      }
      if (attempt < API_RETRY_MAX) {
        await sleep(600 + Math.random() * 800 + attempt * 300);
        return apiFetch(path, options, attempt + 1);
      }
      throw error;
    } finally {
      clearTimeout(timeout);
    }
  }

  async function bootstrap() {
    setStatus("Authorizing…");
    try {
      const res = await fetch(`${apiBase}/miniapp/bootstrap`, {
        method: "POST",
        headers: { Authorization: `tma ${initDataRaw}` },
      });
      if (!res.ok) {
        const payload = await res.json().catch(() => ({}));
        const reason = payload?.error ? ` (${payload.error})` : "";
        setStatus(`Authorization failed${reason}.`);
        setErrorMessage("Not authorized. Check admin access and bot token.");
        return;
      }
      const data = await res.json();
      setSessionToken(data.session_token);
      sessionTokenRef.current = data.session_token;
      const nextRefresh = data.refresh_token || null;
      setRefreshToken(nextRefresh);
      refreshTokenRef.current = nextRefresh;
      setRoles(data.roles || ["admin"]);
      setBranding(data.branding || null);
      setTheme(data.theme || null);
      setStatus("Connected");
      setErrorMessage(null);
      await loadActiveCalls(data.session_token);
      await loadCallLog(data.session_token);
    } catch {
      setStatus("Authorization failed.");
      setErrorMessage("API unreachable. Check network and API base URL.");
    }
  }

  async function loadActiveCalls(tokenOverride?: string) {
    const token = tokenOverride || sessionToken;
    if (!token) return;
    setLoadingActive(true);
    const res = tokenOverride
      ? await fetch(`${apiBase}/miniapp/calls/active`, {
          headers: { Authorization: `Bearer ${token}` },
        })
      : await apiFetch("/miniapp/calls/active", {});
    if (!res.ok) {
      setErrorMessage("Failed to load active calls.");
      setLoadingActive(false);
      return;
    }
    const data = await res.json().catch(() => ({}));
    const next: Record<string, CallSnapshot> = {};
    (data.calls || []).forEach((call: CallSnapshot) => {
      next[call.call_sid] = call;
    });
    setCalls(next);
    setLoadingActive(false);
    const initialSid = initialCallSidRef.current;
    if (initialSid && next[initialSid]) {
      setActiveCallSid(initialSid);
      setTab("live");
      initialCallSidRef.current = null;
    }
  }

  async function loadCallLog(tokenOverride?: string, filters = logFilters) {
    const token = tokenOverride || sessionToken;
    if (!token) return;
    setLoadingLog(true);
    const params = new URLSearchParams();
    params.set("limit", "20");
    if (
      filters.status !== undefined &&
      filters.status !== null &&
      filters.status !== "all"
    )
      params.set("status", filters.status);
    if (
      filters.direction !== undefined &&
      filters.direction !== null &&
      filters.direction !== "all"
    )
      params.set("direction", filters.direction);
    if (filters.query !== undefined && filters.query !== null)
      params.set("q", filters.query);
    if (filters.start !== undefined && filters.start !== null)
      params.set("start", filters.start);
    if (filters.end !== undefined && filters.end !== null)
      params.set("end", filters.end);
    const path = `/miniapp/calls/recent?${params.toString()}`;
    const res = tokenOverride
      ? await fetch(`${apiBase}${path}`, {
          headers: { Authorization: `Bearer ${token}` },
        })
      : await apiFetch(path, {});
    if (!res.ok) {
      setErrorMessage("Failed to load call log.");
      setLoadingLog(false);
      return;
    }
    const data = await res.json().catch(() => ({}));
    setCallLog(data.calls || []);
    setLoadingLog(false);
  }

  async function loadCallDetails(callSid: string) {
    setLoadingDetails(true);
    const res = await apiFetch(`/miniapp/calls/${callSid}`, {});
    if (!res.ok) {
      setErrorMessage("Failed to load call details.");
      setLoadingDetails(false);
      return;
    }
    const data = await res.json().catch(() => ({}));
    const call = data.call;
    if (!call) {
      setCallDetails({ call: { error: "Call not found" }, states: [] });
      setLoadingDetails(false);
      return;
    }
    const statusRes = await apiFetch(`/miniapp/calls/${callSid}/status`, {});
    if (!statusRes.ok) {
      setErrorMessage("Failed to load call status.");
      setLoadingDetails(false);
      return;
    }
    const statusData = await statusRes.json().catch(() => ({}));
    setCallDetails({ call, states: statusData.recent_states || [] });
    setLoadingDetails(false);
  }

  async function handleDecision(callSid: string, action: "answer" | "decline") {
    if (!isAdmin) {
      setErrorMessage("Read-only access: action not allowed.");
      return;
    }
    const res = await apiFetch(`/miniapp/calls/${callSid}/${action}`, {
      method: "POST",
    });
    if (!res.ok) {
      setErrorMessage("Action failed. Try again.");
      return;
    }
    await loadActiveCalls();
  }

  function updateCall(callSid: string, data: Partial<CallSnapshot>) {
    setCalls((prev) => {
      const next = { ...prev };
      next[callSid] = { ...prev[callSid], ...data, call_sid: callSid };
      return next;
    });
  }

  function appendEvent(callSid: string, line: string) {
    setCalls((prev) => {
      const next = { ...prev };
      const current = next[callSid] || { call_sid: callSid };
      const events = [...(current.last_events || [])].slice(-2);
      events.push(line);
      next[callSid] = { ...current, last_events: events.slice(-3) };
      return next;
    });
  }

  function applyMiniappEvent(payload: MiniappEvent) {
    if (!payload || typeof payload.sequence !== "number") return;
    if (payload.sequence <= lastSequenceRef.current) return;
    lastSequenceRef.current = payload.sequence;
    setEvents((prev) => prev.concat(payload).slice(-80));
    if (payload.type === "call.console.opened") {
      updateCall(payload.call_sid, payload.data as CallSnapshot);
      if (
        initialCallSidRef.current !== null &&
        payload.call_sid === initialCallSidRef.current
      ) {
        setActiveCallSid(payload.call_sid);
        setTab("live");
        initialCallSidRef.current = null;
      }
    }
    if (payload.type === "call.status") {
      updateCall(payload.call_sid, {
        status: String(payload.data?.status || payload.data?.label || ""),
        status_label: String(payload.data?.label || ""),
      });
    }
    if (payload.type === "call.phase") {
      updateCall(payload.call_sid, {
        phase: String(payload.data?.phase || ""),
        phase_label: String(payload.data?.label || ""),
      });
    }
    if (payload.type === "call.console.event") {
      appendEvent(payload.call_sid, String(payload.data?.line || ""));
    }
    if (payload.type === "call.inbound_gate") {
      updateCall(payload.call_sid, {
        gate_status: String(payload.data?.status || ""),
      });
    }
  }

  async function resyncStream() {
    const after = lastSequenceRef.current;
    const res = await apiFetch(`/miniapp/resync?after=${after}&limit=200`, {});
    if (!res.ok) return;
    const data = await res.json().catch(() => ({}));
    if (Array.isArray(data.live_calls)) {
      const next: Record<string, CallSnapshot> = {};
      data.live_calls.forEach((call: CallSnapshot) => {
        next[call.call_sid] = call;
      });
      setCalls(next);
    }
    if (Array.isArray(data.events)) {
      data.events.forEach((evt: MiniappEvent) => applyMiniappEvent(evt));
    }
  }

  function connectStream() {
    if (!sessionToken) return;
    if (eventSourceRef.current !== null) {
      eventSourceRef.current.close();
    }
    const since = lastSequenceRef.current;
    const initB64 = initDataRaw ? encodeInitData(initDataRaw) : "";
    const url =
      `${apiBase}/miniapp/stream?token=${encodeURIComponent(sessionToken)}&since=${since}` +
      (initB64 ? `&init_b64=${encodeURIComponent(initB64)}` : "");
    const source = new EventSource(url);
    eventSourceRef.current = source;
    setStreamState("reconnecting");
    source.onopen = () => {
      setStreamState("connected");
      resyncStream().catch(() => {});
    };
    source.onmessage = (event) => {
      if (!event.data) return;
      const payload = JSON.parse(event.data) as MiniappEvent;
      applyMiniappEvent(payload);
    };
    source.onerror = () => {
      setStreamState("reconnecting");
      setStatus("Realtime disconnected. Reconnecting…");
      setTimeout(async () => {
        if (await refreshSession()) {
          connectStream();
        } else {
          connectStream();
        }
      }, 4000);
    };
  }

  function saveApiBase() {
    const value = apiInput.trim();
    if (!value || value === apiBase) return;
    localStorage.setItem("miniapp_api_base", value);
    setApiBase(value);
  }

  function updateFilter(field: string, value: string) {
    setLogFilters((prev) => ({ ...prev, [field]: value }));
  }

  async function applyCallLogFilters() {
    await loadCallLog(undefined, logFilters);
  }

  async function resetCallLogFilters() {
    const defaults = {
      status: "all",
      direction: "all",
      query: "",
      start: "",
      end: "",
    };
    setLogFilters(defaults);
    await loadCallLog(undefined, defaults);
  }

  async function exportCallLog() {
    if (!isAdmin) {
      setErrorMessage("Read-only access: export not allowed.");
      return;
    }
    const params = new URLSearchParams();
    params.set("limit", "1000");
    if (
      logFilters.status !== undefined &&
      logFilters.status !== null &&
      logFilters.status !== "all"
    )
      params.set("status", logFilters.status);
    if (
      logFilters.direction !== undefined &&
      logFilters.direction !== null &&
      logFilters.direction !== "all"
    )
      params.set("direction", logFilters.direction);
    if (logFilters.query !== undefined && logFilters.query !== null)
      params.set("q", logFilters.query);
    if (logFilters.start !== undefined && logFilters.start !== null)
      params.set("start", logFilters.start);
    if (logFilters.end !== undefined && logFilters.end !== null)
      params.set("end", logFilters.end);
    const res = await apiFetch(
      `/miniapp/calls/export?${params.toString()}`,
      {},
    );
    if (!res.ok) {
      setErrorMessage("Export failed. Try again.");
      return;
    }
    const blob = await res.blob();
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = `voxly_calls_${new Date().toISOString().slice(0, 10)}.csv`;
    document.body.appendChild(link);
    link.click();
    link.remove();
    URL.revokeObjectURL(url);
  }

  function formatAccessList(list: string[]) {
    if (!list.length) return "None";
    return list.join(", ");
  }

  async function loadAccessControl() {
    if (!isAdmin) return;
    const res = await apiFetch("/miniapp/access", {});
    if (!res.ok) {
      setErrorMessage("Failed to load access control.");
      return;
    }
    const data = await res.json().catch(() => ({}));
    setAccessEnvAdmins(data.env_admins || []);
    setAccessEnvViewers(data.env_viewers || []);
    setAccessEffective({
      admins: data.admins || [],
      viewers: data.viewers || [],
    });
    const customAdmins = data.custom_admins || [];
    const customViewers = data.custom_viewers || [];
    setAccessAdminInput(customAdmins.join(", "));
    setAccessViewerInput(customViewers.join(", "));
    setAccessLoaded(true);
    setAccessNote(null);
  }

  async function saveAccessControl() {
    if (!isAdmin) return;
    const res = await apiFetch("/miniapp/access", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        admins: accessAdminInput,
        viewers: accessViewerInput,
      }),
    });
    if (!res.ok) {
      setErrorMessage("Failed to update access control.");
      return;
    }
    const data = await res.json().catch(() => ({}));
    setAccessEnvAdmins(data.env_admins || []);
    setAccessEnvViewers(data.env_viewers || []);
    setAccessEffective({
      admins: data.admins || [],
      viewers: data.viewers || [],
    });
    setAccessAdminInput((data.custom_admins || []).join(", "));
    setAccessViewerInput((data.custom_viewers || []).join(", "));
    setAccessNote("Access lists updated.");
  }

  return (
    <Page back={false}>
      <div className="va-shell">
        <header className="va-header">
          <div>
            <div className="va-title">
              {branding?.name || "VOICEDNUT"} Live Ops
            </div>
            <div className="va-subtitle">{status}</div>
          </div>
          <div className="va-badges">
            <div className={`va-pill ${streamState}`}>
              Realtime: {streamState}
            </div>
            <div className="va-badge">{roles.join(", ").toUpperCase()}</div>
          </div>
        </header>
        {errorMessage && <div className="va-alert">{errorMessage}</div>}

        <nav className="va-tabs">
          {(["dashboard", "live", "calllog", "settings"] as const).map(
            (key) => (
              <button
                key={key}
                className={`va-tab ${tab === key ? "active" : ""}`}
                onClick={() => setTab(key)}
              >
                {key === "calllog"
                  ? "Call Log"
                  : key.charAt(0).toUpperCase() + key.slice(1)}
              </button>
            ),
          )}
        </nav>

        {tab === "dashboard" && (
          <section className="va-panel">
            <div className="va-metrics">
              <div className="va-card">
                <div className="va-card-label">Active Calls</div>
                <div className="va-card-value">{metrics.active}</div>
              </div>
              <div className="va-card">
                <div className="va-card-label">Inbound Pending</div>
                <div className="va-card-value">{metrics.pending}</div>
              </div>
              <div className="va-card">
                <div className="va-card-label">Missed (24h)</div>
                <div className="va-card-value">{metrics.missed}</div>
              </div>
              <div className="va-card">
                <div className="va-card-label">Answer Rate</div>
                <div className="va-card-value">{metrics.answerRate}%</div>
              </div>
              <div className="va-card">
                <div className="va-card-label">Avg Duration (s)</div>
                <div className="va-card-value">
                  {metrics.avgDuration || "—"}
                </div>
              </div>
            </div>
            <div className="va-section">
              <h3>Recent Events</h3>
              <div className="va-events">
                {events.length
                  ? events
                      .slice(-6)
                      .map((evt) => `${evt.type} — ${evt.call_sid} — ${evt.ts}`)
                      .join("\n")
                  : "Waiting for events…"}
              </div>
            </div>
          </section>
        )}

        {tab === "live" && (
          <section className="va-panel">
            <div className="va-section">
              <h3>Live Calls</h3>
              <div className="va-list">
                {loadingActive
                  ? "Loading active calls…"
                  : callList.length
                    ? callList.map((call) => (
                        <div className="va-list-item" key={call.call_sid}>
                          <div className="va-list-title">
                            <strong>
                              {call.inbound ? "Inbound" : "Outbound"}
                            </strong>{" "}
                            — {call.name || call.from || "Unknown"}
                          </div>
                          <div className="va-list-meta">
                            Status:{" "}
                            {call.status_label || call.status || "unknown"} |
                            Phase: {call.phase_label || call.phase || "—"}
                          </div>
                          <div className="va-list-meta">
                            Call SID: {call.call_sid}
                          </div>
                          <div className="va-actions">
                            <button
                              className="va-btn secondary"
                              onClick={() => setActiveCallSid(call.call_sid)}
                            >
                              View
                            </button>
                            {call.inbound && isAdmin && (
                              <>
                                <button
                                  className="va-btn"
                                  onClick={() =>
                                    handleDecision(call.call_sid, "answer")
                                  }
                                >
                                  Answer
                                </button>
                                <button
                                  className="va-btn secondary"
                                  onClick={() =>
                                    handleDecision(call.call_sid, "decline")
                                  }
                                >
                                  Decline
                                </button>
                              </>
                            )}
                            {call.inbound && !isAdmin && (
                              <span className="va-muted-label">Read-only</span>
                            )}
                          </div>
                        </div>
                      ))
                    : "No active calls."}
              </div>
            </div>
            <div className="va-section">
              <h3>Call Console</h3>
              <div className="va-console">
                {activeCall ? (
                  <>
                    <div className="va-console-title">
                      <strong>
                        {activeCall.inbound ? "Inbound" : "Outbound"} Call
                      </strong>{" "}
                      — {activeCall.name || activeCall.from || "Unknown"}
                    </div>
                    <div>
                      Status:{" "}
                      {activeCall.status_label ||
                        activeCall.status ||
                        "unknown"}
                    </div>
                    <div>
                      Phase: {activeCall.phase_label || activeCall.phase || "—"}
                    </div>
                    {activeCall.inbound && (
                      <div>Gate: {activeCall.gate_status || "pending"}</div>
                    )}
                    <div>
                      Route: {activeCall.route_label || "—"} | Script:{" "}
                      {activeCall.script || "—"}
                    </div>
                    <div className="va-console-events">Events:</div>
                    <pre>
                      {(activeCall.last_events || []).join("\n") || "—"}
                    </pre>
                  </>
                ) : (
                  "Select a call to view details."
                )}
              </div>
            </div>
          </section>
        )}

        {tab === "calllog" && (
          <section className="va-panel">
            <div className="va-section">
              <h3>Call Log</h3>
              <div className="va-filters">
                <div className="va-field">
                  <label htmlFor="filter-status">Status</label>
                  <select
                    id="filter-status"
                    value={logFilters.status}
                    onChange={(event) =>
                      updateFilter("status", event.target.value)
                    }
                  >
                    <option value="all">All</option>
                    <option value="completed">Completed</option>
                    <option value="in-progress">In progress</option>
                    <option value="ringing">Ringing</option>
                    <option value="failed">Failed</option>
                    <option value="no-answer">No answer</option>
                    <option value="canceled">Canceled</option>
                  </select>
                </div>
                <div className="va-field">
                  <label htmlFor="filter-direction">Direction</label>
                  <select
                    id="filter-direction"
                    value={logFilters.direction}
                    onChange={(event) =>
                      updateFilter("direction", event.target.value)
                    }
                  >
                    <option value="all">All</option>
                    <option value="inbound">Inbound</option>
                    <option value="outbound">Outbound</option>
                  </select>
                </div>
                <div className="va-field">
                  <label htmlFor="filter-query">Search</label>
                  <input
                    id="filter-query"
                    type="text"
                    value={logFilters.query}
                    onChange={(event) =>
                      updateFilter("query", event.target.value)
                    }
                    placeholder="Phone or Call SID"
                  />
                </div>
                <div className="va-field">
                  <label htmlFor="filter-start">From</label>
                  <input
                    id="filter-start"
                    type="date"
                    value={logFilters.start}
                    onChange={(event) =>
                      updateFilter("start", event.target.value)
                    }
                  />
                </div>
                <div className="va-field">
                  <label htmlFor="filter-end">To</label>
                  <input
                    id="filter-end"
                    type="date"
                    value={logFilters.end}
                    onChange={(event) =>
                      updateFilter("end", event.target.value)
                    }
                  />
                </div>
                <div className="va-actions">
                  <button className="va-btn" onClick={applyCallLogFilters}>
                    Apply
                  </button>
                  <button
                    className="va-btn secondary"
                    onClick={resetCallLogFilters}
                  >
                    Reset
                  </button>
                  {isAdmin && (
                    <button
                      className="va-btn secondary"
                      onClick={exportCallLog}
                    >
                      Export CSV
                    </button>
                  )}
                </div>
              </div>
              <div className="va-list">
                {loadingLog
                  ? "Loading call log…"
                  : callLog.length
                    ? callLog.map((call) => (
                        <div
                          className="va-list-item"
                          key={String(call.call_sid)}
                        >
                          <div className="va-list-title">
                            <strong>{String(call.status || "unknown")}</strong>{" "}
                            — {String(call.phone_number || "unknown")}
                          </div>
                          <div className="va-list-meta">
                            Direction: {String(call.direction || "unknown")} |
                            Call SID: {String(call.call_sid)}
                          </div>
                          <button
                            className="va-btn secondary"
                            onClick={() =>
                              loadCallDetails(String(call.call_sid))
                            }
                          >
                            Details
                          </button>
                        </div>
                      ))
                    : "No calls found."}
              </div>
            </div>
            <div className="va-section">
              <h3>Call Details</h3>
              <div className="va-console">
                {loadingDetails ? (
                  "Loading call details…"
                ) : callDetails ? (
                  <>
                    <div className="va-console-title">
                      <strong>
                        {String(callDetails.call.call_sid || "unknown")}
                      </strong>
                    </div>
                    <div>
                      Status: {String(callDetails.call.status || "unknown")}
                    </div>
                    <div>
                      Direction: {String(callDetails.call.direction || "—")}
                    </div>
                    <div>
                      Phone: {String(callDetails.call.phone_number || "—")}
                    </div>
                    <div>
                      Duration: {String(callDetails.call.duration || "—")}
                    </div>
                    <div>
                      Created: {String(callDetails.call.created_at || "—")}
                    </div>
                    <div className="va-console-events">Recent Events:</div>
                    <pre>
                      {callDetails.states.length
                        ? callDetails.states
                            .map((s) => `${s.state} @ ${s.timestamp}`)
                            .join("\n")
                        : "—"}
                    </pre>
                  </>
                ) : (
                  "Select a call to view details."
                )}
              </div>
            </div>
          </section>
        )}

        {tab === "settings" && (
          <section className="va-panel">
            <div className="va-section">
              <h3>Settings</h3>
              <div className="va-field">
                <label htmlFor="api-base">API Base URL</label>
                <input
                  id="api-base"
                  type="text"
                  value={apiInput}
                  onChange={(event) => setApiInput(event.target.value)}
                  placeholder="https://api.example.com"
                />
                <button className="va-btn" onClick={saveApiBase}>
                  Save
                </button>
              </div>
              <div className="va-hint">
                Use this only if the Mini App is hosted on a different domain.
              </div>
            </div>
            {isAdmin && (
              <div className="va-section">
                <h3>Access Control</h3>
                <div className="va-hint">
                  Env admins (read-only): {formatAccessList(accessEnvAdmins)}
                </div>
                <div className="va-hint">
                  Env viewers (read-only): {formatAccessList(accessEnvViewers)}
                </div>
                <div className="va-field">
                  <label htmlFor="access-admins">Custom Admin IDs</label>
                  <input
                    id="access-admins"
                    type="text"
                    value={accessAdminInput}
                    onChange={(event) =>
                      setAccessAdminInput(event.target.value)
                    }
                    placeholder="Comma-separated Telegram user IDs"
                  />
                </div>
                <div className="va-field">
                  <label htmlFor="access-viewers">Custom Viewer IDs</label>
                  <input
                    id="access-viewers"
                    type="text"
                    value={accessViewerInput}
                    onChange={(event) =>
                      setAccessViewerInput(event.target.value)
                    }
                    placeholder="Comma-separated Telegram user IDs"
                  />
                </div>
                <div className="va-actions">
                  <button className="va-btn" onClick={saveAccessControl}>
                    Save Access
                  </button>
                  <button
                    className="va-btn secondary"
                    onClick={loadAccessControl}
                  >
                    Refresh
                  </button>
                </div>
                {accessNote && <div className="va-hint">{accessNote}</div>}
                <div className="va-hint">
                  Effective admins: {formatAccessList(accessEffective.admins)}
                </div>
                <div className="va-hint">
                  Effective viewers: {formatAccessList(accessEffective.viewers)}
                </div>
              </div>
            )}
            {!isAdmin && (
              <div className="va-section">
                <h3>Access Control</h3>
                <div className="va-hint">
                  Read-only access. Contact an admin to change access lists.
                </div>
              </div>
            )}
          </section>
        )}
      </div>
    </Page>
  );
};
