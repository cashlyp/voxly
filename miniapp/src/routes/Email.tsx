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
import { apiFetch, createIdempotencyKey } from "../lib/api";
import { trackEvent } from "../lib/telemetry";
import { hapticError, hapticSuccess } from "../lib/ux";

type EmailMessage = {
  id: number;
  to: string;
  subject: string;
  status: "pending" | "sent" | "failed" | "bounced";
  created_at: string;
  sent_at?: string | null;
  error_message?: string | null;
};

type EmailTemplate = {
  id: number;
  name: string;
  subject: string;
  body: string;
};

type SendEmailResponse = {
  ok: boolean;
  message_id?: number;
  status?: string;
  error?: string;
};

export function Email() {
  const [messages, setMessages] = useState<EmailMessage[]>([]);
  const [templates, setTemplates] = useState<EmailTemplate[]>([]);
  const [loading, setLoading] = useState(false);
  const [sending, setSending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<"compose" | "history">("compose");

  // Form state
  const [toEmail, setToEmail] = useState("");
  const [subject, setSubject] = useState("");
  const [body, setBody] = useState("");
  const [selectedTemplate, setSelectedTemplate] = useState("");
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
        messages: EmailMessage[];
      }>(`/webapp/emails?${params.toString()}`);
      setMessages(response.messages || []);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load emails");
      trackEvent("email_load_failed");
    } finally {
      setLoading(false);
    }
  }, [statusFilter]);

  const loadTemplates = useCallback(async () => {
    try {
      const response = await apiFetch<{
        ok: boolean;
        templates: EmailTemplate[];
      }>("/webapp/emails/templates");
      setTemplates(response.templates || []);
    } catch {
      // Templates are optional
    }
  }, []);

  useEffect(() => {
    loadMessages();
    loadTemplates();
  }, [loadMessages, loadTemplates]);

  const handleSendEmail = async () => {
    if (!toEmail.trim()) {
      setError("Email address is required");
      return;
    }

    if (!subject.trim()) {
      setError("Subject is required");
      return;
    }

    if (!body.trim()) {
      setError("Message body is required");
      return;
    }

    setSending(true);
    setError(null);
    setSuccess(null);

    try {
      const response = await apiFetch<SendEmailResponse>(
        "/webapp/emails/send",
        {
          method: "POST",
          body: {
            to: toEmail.trim(),
            subject: subject.trim(),
            body: body.trim(),
          },
          idempotencyKey: createIdempotencyKey(),
        },
      );

      if (response.ok) {
        hapticSuccess();
        setSuccess("Email sent successfully!");
        setToEmail("");
        setSubject("");
        setBody("");
        setSelectedTemplate("");
        trackEvent("email_sent");
        void loadMessages();
      } else {
        throw new Error(response.error || "Failed to send email");
      }
    } catch (err) {
      hapticError();
      setError(err instanceof Error ? err.message : "Failed to send email");
      trackEvent("email_send_failed");
    } finally {
      setSending(false);
    }
  };

  const handleApplyTemplate = (templateId: string) => {
    const template = templates.find((t) => String(t.id) === templateId);
    if (template !== undefined) {
      setSubject(template.subject);
      setBody(template.body);
      setSelectedTemplate(templateId);
    }
  };

  return (
    <div className="wallet-page">
      <List className="wallet-list">
        {error && <Banner type="inline" header="Error" description={error} />}
        {success && (
          <Banner type="inline" header="Success" description={success} />
        )}

        <Section header="Email Center" className="wallet-section">
          <div className="tabs-container">
            <button
              className={`tab-button ${activeTab === "compose" ? "active" : ""}`}
              onClick={() => void setActiveTab("compose")}
            >
              Compose
            </button>
            <button
              className={`tab-button ${activeTab === "history" ? "active" : ""}`}
              onClick={() => void setActiveTab("history")}
            >
              History
            </button>
          </div>

          {activeTab === "compose" && (
            <>
              <Input
                header="Recipient Email"
                placeholder="user@example.com"
                value={toEmail}
                onChange={(e) => setToEmail(e.target.value)}
                disabled={sending}
                type="email"
              />

              {templates.length > 0 && (
                <Select
                  header="Email Templates"
                  value={selectedTemplate}
                  onChange={(e) => handleApplyTemplate(e.target.value)}
                >
                  <option value="">Select a template...</option>
                  {templates.map((t) => (
                    <option key={t.id} value={t.id}>
                      {t.name}
                    </option>
                  ))}
                </Select>
              )}

              <Input
                header="Subject"
                placeholder="Email subject"
                value={subject}
                onChange={(e) => setSubject(e.target.value)}
                disabled={sending}
              />

              <Textarea
                header="Message Body"
                placeholder="Type your email message here..."
                value={body}
                onChange={(e) => setBody(e.target.value)}
                disabled={sending}
              />

              <div className="section-actions">
                <Button
                  size="m"
                  mode="filled"
                  onClick={() => void handleSendEmail()}
                  disabled={
                    sending ||
                    !toEmail.trim() ||
                    !subject.trim() ||
                    !body.trim()
                  }
                >
                  {sending ? "Sending..." : "Send Email"}
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
                <option value="">All emails</option>
                <option value="sent">Sent</option>
                <option value="pending">Pending</option>
                <option value="failed">Failed</option>
                <option value="bounced">Bounced</option>
              </Select>

              <div className="card-section">
                <div className="card-header">
                  <span>Email History</span>
                  <span className="card-header-muted">
                    {loading ? "Loading..." : `${messages.length} emails`}
                  </span>
                </div>

                {messages.length === 0 ? (
                  <div className="empty-card">
                    <div className="empty-title">No emails</div>
                    <div className="empty-subtitle">
                      {statusFilter !== ""
                        ? "No emails match the selected filter."
                        : "Your email history will appear here."}
                    </div>
                  </div>
                ) : (
                  <div className="card-list">
                    {messages.map((msg) => (
                      <div key={msg.id} className="card-item">
                        <div className="card-item-main">
                          <div className="card-item-title">{msg.to}</div>
                          <div className="card-item-subtitle">
                            {msg.subject}
                          </div>
                          <div className="card-item-meta">
                            {msg.created_at}
                            {msg.sent_at && ` • Sent: ${msg.sent_at}`}
                            {msg.error_message &&
                              ` • Error: ${msg.error_message}`}
                          </div>
                        </div>
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
