import {
  Banner,
  Button,
  Input,
  List,
  Section,
  Select,
  Textarea,
} from "@telegram-apps/telegram-ui";
import { useCallback, useEffect, useState } from "react";
import { MaskedPhone } from "../components/MaskedPhone";
import { apiFetch, createIdempotencyKey } from "../lib/api";
import { trackEvent } from "../lib/telemetry";
import { confirmAction, hapticError, hapticSuccess } from "../lib/ux";
import { useUser } from "../state/user";

type SmsMessage = {
  id: number;
  phone_number: string;
  body: string;
  status: "pending" | "sent" | "failed" | "scheduled";
  created_at: string;
  scheduled_for?: string | null;
  error_message?: string | null;
};

type SmsTemplate = {
  id: number;
  name: string;
  body: string;
};

type SendSmsResponse = {
  ok: boolean;
  message_id?: number;
  status?: string;
  error?: string;
};

export function Sms() {
  const { roles } = useUser();
  const isAdmin = roles.includes("admin");

  const [messages, setMessages] = useState<SmsMessage[]>([]);
  const [templates, setTemplates] = useState<SmsTemplate[]>([]);
  const [loading, setLoading] = useState(false);
  const [sending, setSending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<"send" | "history">("send");

  // Form state
  const [phoneNumber, setPhoneNumber] = useState("");
  const [messageBody, setMessageBody] = useState("");
  const [selectedTemplate, setSelectedTemplate] = useState("");
  const [scheduleTime, setScheduleTime] = useState("");
  const [statusFilter, setStatusFilter] = useState("");

  const loadMessages = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const params = new URLSearchParams();
      if (statusFilter !== "") params.set("status", statusFilter);
      params.set("limit", "20");
      const response = await apiFetch<{
        ok: boolean;
        messages?: SmsMessage[];
      }>(
        `/webapp/sms?${params.toString()}`,
      );
      setMessages(response.messages ?? []);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load messages");
      trackEvent("sms_load_failed");
    } finally {
      setLoading(false);
    }
  }, [statusFilter]);

  const loadTemplates = useCallback(async () => {
    try {
      const response = await apiFetch<{
        ok: boolean;
        templates?: SmsTemplate[];
      }>("/webapp/sms/templates");
      setTemplates(response.templates ?? []);
    } catch {
      // Templates are optional
    }
  }, []);

  useEffect(() => {
    void loadMessages();
    void loadTemplates();
  }, [loadMessages, loadTemplates]);

  const handleSendMessage = async () => {
    if (!phoneNumber.trim()) {
      setError("Phone number is required");
      return;
    }

    if (!messageBody.trim()) {
      setError("Message body is required");
      return;
    }

    setSending(true);
    setError(null);
    setSuccess(null);

    try {
      const response = await apiFetch<SendSmsResponse>("/webapp/sms/send", {
        method: "POST",
        body: {
          phone_number: phoneNumber.trim(),
          body: messageBody.trim(),
          scheduled_for: scheduleTime !== "" ? scheduleTime : undefined,
        },
        idempotencyKey: createIdempotencyKey(),
      });

      if (response.ok) {
        hapticSuccess();
        setSuccess(
          scheduleTime
            ? `Message scheduled for ${scheduleTime}`
            : "Message sent successfully!",
        );
        setPhoneNumber("");
        setMessageBody("");
        setScheduleTime("");
        setSelectedTemplate("");
        trackEvent("sms_sent", { scheduled: !!scheduleTime });
        await loadMessages();
      } else {
        throw new Error(response.error ?? "Failed to send message");
      }
    } catch (err) {
      hapticError();
      setError(err instanceof Error ? err.message : "Failed to send message");
      trackEvent("sms_send_failed");
    } finally {
      setSending(false);
    }
  };

  const handleApplyTemplate = (templateId: string) => {
    const template = templates.find((t) => String(t.id) === templateId);
    if (template !== undefined) {
      setMessageBody(template.body);
      setSelectedTemplate(templateId);
    }
  };

  const handleRetry = async (messageId: number) => {
    if (!isAdmin) return;

    const confirmed = await confirmAction({
      title: "Retry Message?",
      message: "Re-send this failed message.",
      confirmText: "Retry",
      destructive: false,
    });

    if (!confirmed) return;

    setSending(true);
    setError(null);

    try {
      await apiFetch(`/webapp/sms/${messageId}/retry`, {
        method: "POST",
        idempotencyKey: createIdempotencyKey(),
      });

      hapticSuccess();
      setSuccess("Message retry queued");
      trackEvent("sms_retry", { message_id: messageId });
      await loadMessages();
    } catch (err) {
      hapticError();
      setError(err instanceof Error ? err.message : "Failed to retry message");
      trackEvent("sms_retry_failed", { message_id: messageId });
    } finally {
      setSending(false);
    }
  };

  return (
    <div className="wallet-page">
      <List className="wallet-list">
        {error !== null && error !== "" && (
          <Banner type="inline" header="Error" description={error} />
        )}
        {success !== null && success !== "" && (
          <Banner type="inline" header="Success" description={success} />
        )}

        <Section header="Message Sender" className="wallet-section">
          <div className="tabs-container">
            <button
              className={`tab-button ${activeTab === "send" ? "active" : ""}`}
              onClick={() => void setActiveTab("send")}
            >
              Send Message
            </button>
            <button
              className={`tab-button ${activeTab === "history" ? "active" : ""}`}
              onClick={() => void setActiveTab("history")}
            >
              History
            </button>
          </div>

          {activeTab === "send" && (
            <>
              <Input
                header="Phone Number"
                placeholder="+1234567890"
                value={phoneNumber}
                onChange={(e) => void setPhoneNumber(e.target.value)}
                disabled={sending}
                type="tel"
              />

              {templates.length > 0 && (
                <Select
                  header="Message Templates"
                  value={selectedTemplate}
                  onChange={(e) => void handleApplyTemplate(e.target.value)}
                >
                  <option value="">Select a template...</option>
                  {templates.map((t) => (
                    <option key={t.id} value={t.id}>
                      {t.name}
                    </option>
                  ))}
                </Select>
              )}

              <Textarea
                header="Message Body"
                placeholder="Type your message here..."
                value={messageBody}
                onChange={(e) => void setMessageBody(e.target.value)}
                disabled={sending}
              />

              <Input
                header="Schedule (optional)"
                type="datetime-local"
                value={scheduleTime}
                onChange={(e) => void setScheduleTime(e.target.value)}
                disabled={sending}
              />

              <div className="section-actions">
                <Button
                  size="m"
                  mode="filled"
                  onClick={() => void handleSendMessage()}
                  disabled={
                    sending || !phoneNumber.trim() || !messageBody.trim()
                  }
                >
                  {sending ? "Sending..." : "Send Message"}
                </Button>
              </div>
            </>
          )}

          {activeTab === "history" && (
            <>
              <Select
                header="Filter by Status"
                value={statusFilter}
                onChange={(e) => void setStatusFilter(e.target.value)}
              >
                <option value="">All messages</option>
                <option value="sent">Sent</option>
                <option value="pending">Pending</option>
                <option value="scheduled">Scheduled</option>
                <option value="failed">Failed</option>
              </Select>

              <div className="card-section">
                <div className="card-header">
                  <span>Message History</span>
                  <span className="card-header-muted">
                    {loading ? "Loading..." : `${messages.length} messages`}
                  </span>
                </div>

                {messages.length === 0 ? (
                  <div className="empty-card">
                    <div className="empty-title">No messages</div>
                    <div className="empty-subtitle">
                      {statusFilter !== ""
                        ? "No messages match the selected filter."
                        : "Your message history will appear here."}
                    </div>
                  </div>
                ) : (
                  <div className="card-list">
                    {messages.map((msg) => (
                      <div key={msg.id} className="card-item">
                        <div className="card-item-main">
                          <div className="card-item-title">
                            <MaskedPhone value={msg.phone_number} />
                          </div>
                          <div className="card-item-subtitle">{msg.body}</div>
                          <div className="card-item-meta">
                            {msg.created_at}
                            {msg.scheduled_for !== null &&
                              msg.scheduled_for !== undefined &&
                              msg.scheduled_for !== "" &&
                              ` • Scheduled: ${msg.scheduled_for}`}
                            {msg.error_message !== null &&
                              msg.error_message !== undefined &&
                              msg.error_message !== "" &&
                              ` • Error: ${msg.error_message}`}
                          </div>
                        </div>
                        <div className="tag-group">
                          <span
                            className={`tag ${
                              msg.status === "sent"
                                ? "success"
                                : msg.status === "failed"
                                  ? "error"
                                  : ""
                            }`}
                          >
                            {msg.status}
                          </span>
                          {msg.status === "failed" && isAdmin && (
                            <Button
                              size="s"
                              mode="plain"
                              onClick={() => void handleRetry(msg.id)}
                              disabled={sending}
                            >
                              Retry
                            </Button>
                          )}
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            </>
          )}
        </Section>
      </List>
    </div>
  );
}
