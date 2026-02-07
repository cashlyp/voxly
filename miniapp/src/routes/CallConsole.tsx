import {
  Button,
  Cell,
  Chip,
  InlineButtons,
  List,
  Placeholder,
  Section,
  Select,
} from "@telegram-apps/telegram-ui";
import { useEffect, useMemo, useRef, useState } from "react";
import { apiFetch, createIdempotencyKey } from "../lib/api";
import { ensureAuth } from "../lib/auth";
import { connectEventStream, type WebappEvent } from "../lib/realtime";
import { trackEvent } from "../lib/telemetry";
import {
  confirmAction,
  hapticError,
  hapticImpact,
  hapticSuccess,
} from "../lib/ux";
import { useCalls } from "../state/calls";
import { useUser } from "../state/user";

type TranscriptEntry = {
  speaker: string;
  message: string;
  ts: string;
  partial: boolean;
};

export function CallConsole({ callSid }: { callSid: string }) {
  const {
    activeCall,
    callEventsById,
    eventCursorById,
    fetchCall,
    fetchCallEvents,
  } = useCalls();
  const { roles } = useUser();
  const isAdmin = roles.includes("admin");
  const [liveEvents, setLiveEvents] = useState<WebappEvent[]>([]);
  const [transcript, setTranscript] = useState<TranscriptEntry[]>([]);
  const [streamHealth, setStreamHealth] = useState<{
    latencyMs?: number;
    jitterMs?: number;
    packetLossPct?: number;
    asrConfidence?: number;
  } | null>(null);
  const [connectionStatus, setConnectionStatus] = useState<
    "connecting" | "open" | "error" | "stale" | "reconnecting"
  >("connecting");
  const [actionBusy, setActionBusy] = useState<string | null>(null);
  const [scripts, setScripts] = useState<{ id: number; name: string }[]>([]);
  const [selectedScript, setSelectedScript] = useState<number | null>(null);
  const lastSequenceRef = useRef(0);
  const lastSeenRef = useRef(Date.now());
  const reconnectTimerRef = useRef<number | null>(null);
  const reconnectAttemptRef = useRef(0);
  const [streamEpoch, setStreamEpoch] = useState(0);

  useEffect(() => {
    fetchCall(callSid);
    fetchCallEvents(callSid, 0);
    setLiveEvents([]);
    setTranscript([]);
    setStreamHealth(null);
    lastSequenceRef.current = eventCursorById[callSid] || 0;
    lastSeenRef.current = Date.now();
  }, [callSid, fetchCall, fetchCallEvents]);

  useEffect(() => {
    const cursor = eventCursorById[callSid] || 0;
    if (cursor > lastSequenceRef.current) {
      lastSequenceRef.current = cursor;
    }
  }, [callSid, eventCursorById]);

  useEffect(() => {
    let stream: { close: () => void } | null = null;
    let cancelled = false;
    const since = lastSequenceRef.current;
    const scheduleReconnect = () => {
      if (reconnectTimerRef.current !== null) return;
      const attempt = reconnectAttemptRef.current + 1;
      reconnectAttemptRef.current = attempt;
      const delay = Math.min(30000, 1000 * Math.pow(2, attempt - 1));
      setConnectionStatus("reconnecting");
      reconnectTimerRef.current = window.setTimeout(() => {
        reconnectTimerRef.current = null;
        setStreamEpoch((prev) => prev + 1);
      }, delay);
    };
    ensureAuth()
      .then((session) => {
        if (cancelled) return;
        setConnectionStatus("connecting");
        stream = connectEventStream({
          token: session.token,
          since,
          onEvent: (event) => {
            if (event.call_sid !== callSid) return;
            if (event.sequence && event.sequence <= lastSequenceRef.current)
              return;
            lastSequenceRef.current = Math.max(
              lastSequenceRef.current,
              event.sequence || 0,
            );
            lastSeenRef.current = Date.now();
            setLiveEvents((prev) => [...prev.slice(-50), event]);
            if (
              event.type === "transcript.partial" ||
              event.type === "transcript.final"
            ) {
              const entry: TranscriptEntry = {
                speaker: String(event.data?.speaker || "unknown"),
                message: String(event.data?.message || ""),
                ts: event.ts,
                partial: event.type === "transcript.partial",
              };
              setTranscript((prev) => [...prev.slice(-100), entry]);
            }
            if (event.type === "stream.health") {
              const metrics = event.data?.metrics as
                | {
                    latencyMs?: number;
                    jitterMs?: number;
                    packetLossPct?: number;
                    asrConfidence?: number;
                  }
                | undefined;
              if (metrics !== undefined) {
                setStreamHealth(metrics);
              }
            }
            if (
              ["call.updated", "call.ended", "inbound.ringing"].includes(
                event.type,
              )
            ) {
              fetchCall(callSid);
            }
          },
          onHeartbeat: () => {
            lastSeenRef.current = Date.now();
            setConnectionStatus((prev) => (prev === "stale" ? "open" : prev));
          },
          onError: () => {
            setConnectionStatus("error");
            scheduleReconnect();
          },
          onOpen: () => {
            lastSeenRef.current = Date.now();
            setConnectionStatus("open");
            reconnectAttemptRef.current = 0;
          },
        });
      })
      .catch(() => {
        void setConnectionStatus("error");
        scheduleReconnect();
      });
    return () => {
      cancelled = true;
      if (stream) stream.close();
      if (reconnectTimerRef.current !== null) {
        window.clearTimeout(reconnectTimerRef.current);
        reconnectTimerRef.current = null;
      }
    };
  }, [callSid, fetchCall, streamEpoch]);

  useEffect(() => {
    const timer = window.setInterval(() => {
      const now = Date.now();
      if (now - lastSeenRef.current > 45000) {
        setConnectionStatus((prev) => {
          if (prev !== "stale") {
            setStreamEpoch((epoch) => epoch + 1);
          }
          return "stale";
        });
      }
    }, 15000);
    return () => window.clearInterval(timer);
  }, []);

  useEffect(() => {
    const interval = window.setInterval(() => {
      const cursor = eventCursorById[callSid] || 0;
      fetchCallEvents(callSid, cursor);
    }, 5000);
    return () => window.clearInterval(interval);
  }, [callSid, eventCursorById, fetchCallEvents]);

  useEffect(() => {
    if (!isAdmin) return;
    void apiFetch<{ ok: boolean; scripts: { id: number; name: string }[] }>(
      "/webapp/scripts",
    )
      .then((response) => setScripts(response.scripts || []))
      .catch(() => {});
  }, [isAdmin]);

  const statusLine = useMemo(() => {
    if (!activeCall) return "Loading call...";
    return `${activeCall.status || "unknown"} - ${activeCall.direction || "n/a"}`;
  }, [activeCall]);

  const ruleLabel = useMemo(() => {
    const live = activeCall?.live as Record<string, unknown> | undefined;
    return String(
      live?.route_label ||
        live?.script ||
        (activeCall as Record<string, unknown> | null)?.route_label ||
        "default",
    );
  }, [activeCall]);

  const riskLabel = useMemo(() => {
    const live = activeCall?.live as Record<string, unknown> | undefined;
    return String(live?.risk_level || "normal");
  }, [activeCall]);

  const timeline = callEventsById[callSid] || [];

  const handleInboundAction = async (action: "answer" | "decline") => {
    if (action === "decline") {
      const confirmed = await confirmAction({
        title: "Decline this call?",
        message: "The caller will be rejected immediately.",
        confirmText: "Decline",
        destructive: true,
      });
      if (!confirmed) return;
    }
    setActionBusy(action);
    trackEvent(`console_${action}_clicked`, { call_sid: callSid });
    hapticImpact();
    try {
      await apiFetch(`/webapp/inbound/${callSid}/${action}`, {
        method: "POST",
        idempotencyKey: createIdempotencyKey(),
      });
      hapticSuccess();
      trackEvent(`console_${action}_success`, { call_sid: callSid });
      await fetchCall(callSid);
    } catch (error) {
      hapticError();
      trackEvent(`console_${action}_failed`, { call_sid: callSid });
      throw error;
    } finally {
      setActionBusy(null);
    }
  };

  const handleCallback = async () => {
    if (!isAdmin) return;
    setActionBusy("callback");
    trackEvent("console_callback_clicked", { call_sid: callSid });
    try {
      await apiFetch(`/webapp/inbound/${callSid}/callback`, {
        method: "POST",
        body: { window_minutes: 30 },
        idempotencyKey: createIdempotencyKey(),
      });
      trackEvent("console_callback_scheduled", { call_sid: callSid });
      await fetchCall(callSid);
    } catch (error) {
      trackEvent("console_callback_failed", { call_sid: callSid });
      throw error;
    } finally {
      setActionBusy(null);
    }
  };

  const handleStreamAction = async (action: "retry" | "fallback" | "end") => {
    if (action === "end") {
      const confirmed = await confirmAction({
        title: "End this call?",
        message: "This will immediately stop the live call.",
        confirmText: "End call",
        destructive: true,
      });
      if (!confirmed) return;
    }
    setActionBusy(action);
    trackEvent(`console_stream_${action}_clicked`, { call_sid: callSid });
    hapticImpact();
    try {
      const idempotencyKey = createIdempotencyKey();
      if (action === "end") {
        await apiFetch(`/webapp/calls/${callSid}/end`, {
          method: "POST",
          idempotencyKey,
        });
      } else {
        await apiFetch(`/webapp/calls/${callSid}/stream/${action}`, {
          method: "POST",
          idempotencyKey,
        });
      }
      hapticSuccess();
      trackEvent(`console_stream_${action}_success`, { call_sid: callSid });
      await fetchCall(callSid);
    } catch (error) {
      hapticError();
      trackEvent(`console_stream_${action}_failed`, { call_sid: callSid });
      throw error;
    } finally {
      setActionBusy(null);
    }
  };

  const handleScriptInject = async () => {
    if (!selectedScript) return;
    setActionBusy("script");
    try {
      trackEvent("console_script_inject_clicked", {
        call_sid: callSid,
        script_id: selectedScript,
      });
      await apiFetch(`/webapp/calls/${callSid}/script`, {
        method: "POST",
        body: { script_id: selectedScript },
        idempotencyKey: createIdempotencyKey(),
      });
      trackEvent("console_script_inject_success", {
        call_sid: callSid,
        script_id: selectedScript,
      });
    } finally {
      setActionBusy(null);
    }
  };

  return (
    <div className="wallet-page">
      <List className="wallet-list">
        <Section
          header="Live call console"
          footer={callSid}
          className="wallet-section"
        >
          <Cell subtitle="Status" after={<Chip mode="mono">{statusLine}</Chip>}>
            Call status
          </Cell>
          <Cell
            subtitle="Realtime"
            after={<Chip mode="outline">{connectionStatus}</Chip>}
          >
            Connection
          </Cell>
          {(connectionStatus === "reconnecting" ||
            connectionStatus === "stale") && (
            <Cell subtitle="Reconnecting to live updates...">Reconnecting</Cell>
          )}
          {streamHealth && (
            <Cell
              subtitle={`latency ${streamHealth.latencyMs ?? "-"}ms • jitter ${streamHealth.jitterMs ?? "-"}ms`}
              description={`loss ${streamHealth.packetLossPct ?? "-"}% • asr ${streamHealth.asrConfidence ?? "-"}`}
            >
              Stream health
            </Cell>
          )}
          {activeCall && (
            <Cell
              subtitle={ruleLabel}
              after={<Chip mode="mono">{riskLabel}</Chip>}
            >
              Rule summary
            </Cell>
          )}
          <div className="section-actions">
            <Button
              size="s"
              mode="bezeled"
              onClick={() => void fetchCallEvents(callSid, 0)}
            >
              Refresh timeline
            </Button>
          </div>
        </Section>

        {isAdmin && (
          <Section header="Actions" className="wallet-section">
            {activeCall?.inbound_gate?.status === "pending" && (
              <InlineButtons mode="bezeled">
                <InlineButtons.Item
                  text="Answer"
                  disabled={!!actionBusy}
                  onClick={() => void handleInboundAction("answer")}
                />
                <InlineButtons.Item
                  text="Decline"
                  disabled={!!actionBusy}
                  onClick={() => void handleInboundAction("decline")}
                />
                <InlineButtons.Item
                  text="Callback"
                  disabled={!!actionBusy}
                  onClick={() => void handleCallback()}
                />
              </InlineButtons>
            )}
            <InlineButtons mode="gray">
              <InlineButtons.Item
                text="Retry stream"
                disabled={!!actionBusy}
                onClick={() => void handleStreamAction("retry")}
              />
              <InlineButtons.Item
                text="Switch to keypad"
                disabled={!!actionBusy}
                onClick={() => void handleStreamAction("fallback")}
              />
              <InlineButtons.Item
                text="End call"
                disabled={!!actionBusy}
                onClick={() => void handleStreamAction("end")}
              />
            </InlineButtons>
            {scripts.length > 0 && (
              <>
                <Select
                  header="Inject script"
                  value={selectedScript ?? ""}
                  onChange={(event) => {
                    const value = event.target.value;
                    setSelectedScript(value ? Number(value) : null);
                  }}
                >
                  <option value="">Select script</option>
                  {scripts.map((script) => (
                    <option key={script.id} value={script.id}>
                      {script.name}
                    </option>
                  ))}
                </Select>
                <Button
                  size="s"
                  mode="filled"
                  disabled={!selectedScript || !!actionBusy}
                  onClick={() => void handleScriptInject()}
                >
                  Inject script
                </Button>
              </>
            )}
          </Section>
        )}

        <Section header="Timeline" className="wallet-section">
          {timeline.length === 0 ? (
            <Placeholder
              header="No events yet"
              description="Waiting for new events."
            />
          ) : (
            timeline.map((evt) => (
              <Cell
                key={`${evt.sequence_number}-${evt.state}`}
                subtitle={evt.timestamp}
              >
                {evt.state}
              </Cell>
            ))
          )}
        </Section>

        <Section header="Transcript" className="wallet-section">
          {transcript.length === 0 ? (
            <Placeholder
              header="Waiting for transcript"
              description="Live transcript will appear here."
            />
          ) : (
            transcript.map((entry, index) => (
              <Cell
                key={`${entry.ts}-${index}`}
                subtitle={entry.message}
                after={
                  entry.partial ? <Chip mode="mono">partial</Chip> : undefined
                }
              >
                {entry.speaker}
              </Cell>
            ))
          )}
        </Section>

        <Section header="Live events" className="wallet-section">
          {liveEvents.length === 0 ? (
            <Placeholder
              header="No realtime updates"
              description="Waiting for realtime events."
            />
          ) : (
            liveEvents.slice(-15).map((event) => (
              <Cell key={`${event.sequence}-${event.type}`} subtitle={event.ts}>
                {event.type}
              </Cell>
            ))
          )}
        </Section>
      </List>
    </div>
  );
}
