import { Banner, Button, InlineButtons } from "@telegram-apps/telegram-ui";
import { useEffect, useState } from "react";
import { apiFetch, createIdempotencyKey } from "../lib/api";
import { navigate } from "../lib/router";
import { trackEvent } from "../lib/telemetry";
import {
  confirmAction,
  hapticError,
  hapticImpact,
  hapticSuccess,
} from "../lib/ux";
import { useCalls } from "../state/calls";
import { useUser } from "../state/user";

export function Inbox() {
  const { inboundQueue, inboundNotice, fetchInboundQueue } = useCalls();
  const { roles } = useUser();
  const isAdmin = roles.includes("admin");
  const [busyCall, setBusyCall] = useState<string | null>(null);

  useEffect(() => {
    void fetchInboundQueue();
    const timer = window.setInterval(fetchInboundQueue, 5000);
    return () => window.clearInterval(timer);
  }, [fetchInboundQueue]);

  const handleAction = async (
    callSid: string,
    action: "answer" | "decline",
  ) => {
    if (!isAdmin) return;
    if (action === "decline") {
      const confirmed = await confirmAction({
        title: "Decline this call?",
        message: "The caller will be rejected immediately.",
        confirmText: "Decline",
        destructive: true,
      });
      if (!confirmed) return;
    }
    setBusyCall(callSid);
    trackEvent(`inbound_${action}_clicked`, { call_sid: callSid });
    hapticImpact();
    try {
      await apiFetch(`/webapp/inbound/${callSid}/${action}`, {
        method: "POST",
        idempotencyKey: createIdempotencyKey(),
      });
      hapticSuccess();
      trackEvent(`inbound_${action}_success`, { call_sid: callSid });
      if (action === "answer") {
        navigate(`/calls/${callSid}`);
      }
      await fetchInboundQueue();
    } catch (error) {
      hapticError();
      trackEvent(`inbound_${action}_failed`, { call_sid: callSid });
      throw error;
    } finally {
      setBusyCall(null);
    }
  };

  const handleCallback = async (callSid: string) => {
    if (!isAdmin) return;
    setBusyCall(callSid);
    trackEvent("inbound_callback_clicked", { call_sid: callSid });
    try {
      await apiFetch(`/webapp/inbound/${callSid}/callback`, {
        method: "POST",
        body: { window_minutes: 30 },
        idempotencyKey: createIdempotencyKey(),
      });
      trackEvent("inbound_callback_scheduled", { call_sid: callSid });
      await fetchInboundQueue();
    } catch (error) {
      trackEvent("inbound_callback_failed", { call_sid: callSid });
      throw error;
    } finally {
      setBusyCall(null);
    }
  };

  const priorityLabel = (call: (typeof inboundQueue)[number]) =>
    call.priority || "normal";

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
          <Button
            size="s"
            mode="bezeled"
            onClick={() => void fetchInboundQueue()}
          >
            Refresh
          </Button>
        </div>
        {inboundQueue.length === 0 ? (
          <div className="empty-card">
            <div className="empty-title">No inbound calls</div>
            <div className="empty-subtitle">
              Calls will appear here when ringing.
            </div>
          </div>
        ) : (
          <div className="card-list">
            {inboundQueue.map((call) => {
              const disabled =
                busyCall === call.call_sid ||
                call.decision === "answered" ||
                call.decision === "declined";
              const rule = call.rule_summary;
              return (
                <div key={call.call_sid} className="card-item">
                  <div className="card-item-main">
                    <div className="card-item-title">Inbound call</div>
                    <div className="card-item-subtitle">
                      {call.route_label || call.script || "Inbound route"}
                    </div>
                    <div className="card-item-meta">
                      {call.from || "Unknown caller"}
                    </div>
                    {rule && (
                      <div className="rule-summary">
                        <span
                          className={`priority-badge ${priorityLabel(call)}`}
                        >
                          {priorityLabel(call)}
                        </span>
                        <span className="rule-chip">
                          {rule.decision || "allow"}
                        </span>
                        <span className="rule-chip">
                          {rule.label || "default"}
                        </span>
                        {typeof rule.recent_calls === "number" && (
                          <span className="rule-chip">
                            {rule.recent_calls} recent
                          </span>
                        )}
                        {rule.risk && (
                          <span className={`rule-chip risk-${rule.risk}`}>
                            {rule.risk}
                          </span>
                        )}
                      </div>
                    )}
                  </div>
                  <InlineButtons mode="bezeled">
                    <InlineButtons.Item
                      text="Answer"
                      disabled={disabled || !isAdmin}
                      title={!isAdmin ? "Admin only" : undefined}
                      onClick={() => void handleAction(call.call_sid, "answer")}
                    />
                    <InlineButtons.Item
                      text="Decline"
                      disabled={disabled || !isAdmin}
                      title={!isAdmin ? "Admin only" : undefined}
                      onClick={() =>
                        void handleAction(call.call_sid, "decline")
                      }
                    />
                    <InlineButtons.Item
                      text="Callback"
                      disabled={disabled || !isAdmin}
                      title={!isAdmin ? "Admin only" : undefined}
                      onClick={() => void handleCallback(call.call_sid)}
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
