import {
  Banner,
  Button,
  InlineButtons,
  Input,
  Select,
  Textarea,
} from "@telegram-apps/telegram-ui";
import { useEffect, useMemo, useState } from "react";
import { MaskedPhone } from "../components/MaskedPhone";
import { SkeletonList } from "../components/Skeleton";
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

type CallScript = {
  id: number;
  name: string;
  description?: string | null;
  prompt?: string | null;
  first_message?: string | null;
  business_id?: string | null;
  voice_model?: string | null;
};

type OutboundResponse = {
  ok: boolean;
  call_sid?: string;
  status?: string;
  error?: string;
  message?: string;
};

const PLACEHOLDER_REGEX = /\{([a-zA-Z0-9_]+)\}/g;
const TONE_OPTIONS = [
  { id: "auto", label: "Auto (recommended)" },
  { id: "neutral", label: "Neutral / professional" },
  { id: "frustrated", label: "Empathetic troubleshooter" },
  { id: "urgent", label: "Urgent / high-priority" },
  { id: "confused", label: "Patient explainer" },
  { id: "positive", label: "Upbeat / encouraging" },
  { id: "stressed", label: "Reassuring & calming" },
];
const URGENCY_OPTIONS = [
  { id: "auto", label: "Auto (recommended)" },
  { id: "low", label: "Low – casual follow-up" },
  { id: "normal", label: "Normal – timely assistance" },
  { id: "high", label: "High – priority handling" },
  { id: "critical", label: "Critical – emergency protocol" },
];
const TECH_OPTIONS = [
  { id: "auto", label: "Auto (general audience)" },
  { id: "general", label: "General audience" },
  { id: "novice", label: "Beginner-friendly" },
  { id: "advanced", label: "Advanced / technical specialist" },
];

function extractPlaceholders(text?: string | null): string[] {
  if (text === null || text === undefined || text === "") return [];
  const tokens = new Set<string>();
  PLACEHOLDER_REGEX.lastIndex = 0;
  let match = PLACEHOLDER_REGEX.exec(text);
  while (match) {
    if (match[1]) tokens.add(match[1]);
    match = PLACEHOLDER_REGEX.exec(text);
  }
  PLACEHOLDER_REGEX.lastIndex = 0;
  return Array.from(tokens);
}

function replacePlaceholders(
  text: string,
  values: Record<string, string>,
): string {
  return text.replace(PLACEHOLDER_REGEX, (match, token) => {
    const resolvedToken = typeof token === "string" ? token : String(token);
    const value = values[resolvedToken];
    if (value === undefined || value === "") return match;
    return value;
  });
}

export function Inbox() {
  const { inboundQueue, inboundNotice, fetchInboundQueue, inboundMeta } =
    useCalls();
  const { roles } = useUser();
  const isAdmin = roles.includes("admin");
  const [busyCall, setBusyCall] = useState<string | null>(null);

  const [outboundMode, setOutboundMode] = useState<"script" | "custom">(
    "script",
  );
  const [scripts, setScripts] = useState<CallScript[]>([]);
  const [scriptsLoading, setScriptsLoading] = useState(false);
  const [scriptsError, setScriptsError] = useState<string | null>(null);
  const [selectedScriptId, setSelectedScriptId] = useState("");
  const [placeholderValues, setPlaceholderValues] = useState<
    Record<string, string>
  >({});
  const [phoneNumber, setPhoneNumber] = useState("");
  const [customerName, setCustomerName] = useState("");
  const [customPrompt, setCustomPrompt] = useState("");
  const [customFirstMessage, setCustomFirstMessage] = useState("");
  const [voiceModel, setVoiceModel] = useState("");
  const [showAdvanced, setShowAdvanced] = useState(false);
  const [purpose, setPurpose] = useState("");
  const [tone, setTone] = useState("auto");
  const [urgency, setUrgency] = useState("auto");
  const [technicalLevel, setTechnicalLevel] = useState("auto");
  const [outboundBusy, setOutboundBusy] = useState(false);
  const [outboundError, setOutboundError] = useState<string | null>(null);
  const [outboundSuccess, setOutboundSuccess] = useState<string | null>(null);

  useEffect(() => {
    void fetchInboundQueue().catch(() => {});
  }, [fetchInboundQueue]);

  useEffect(() => {
    if (!isAdmin) return;
    setScriptsLoading(true);
    setScriptsError(null);
    void apiFetch<{ ok: boolean; scripts?: CallScript[] }>("/webapp/scripts")
      .then((response) => {
        setScripts(response.scripts ?? []);
      })
      .catch((error) => {
        setScripts([]);
        setScriptsError(
          error instanceof Error ? error.message : "Failed to load scripts",
        );
      })
      .finally(() => {
        setScriptsLoading(false);
      });
  }, [isAdmin]);

  useEffect(() => {
    setPlaceholderValues({});
  }, [selectedScriptId]);

  const selectedScript = useMemo(
    () =>
      scripts.find((script) => String(script.id) === selectedScriptId) ?? null,
    [scripts, selectedScriptId],
  );

  const scriptPlaceholders = useMemo(() => {
    const tokens = new Set<string>();
    extractPlaceholders(selectedScript?.prompt).forEach((token) =>
      tokens.add(token),
    );
    extractPlaceholders(selectedScript?.first_message).forEach((token) =>
      tokens.add(token),
    );
    return Array.from(tokens);
  }, [selectedScript]);

  const scriptsErrorMessage = scriptsError ?? "";
  const scriptDescription =
    typeof selectedScript?.description === "string"
      ? selectedScript.description
      : "";
  const scriptVoiceModel =
    typeof selectedScript?.voice_model === "string"
      ? selectedScript.voice_model
      : "";

  const handleOutboundSubmit = async () => {
    if (!isAdmin || outboundBusy) return;
    setOutboundError(null);
    setOutboundSuccess(null);

    const number = phoneNumber.trim();
    if (number === "") {
      setOutboundError("Enter a phone number in E.164 format.");
      return;
    }
    if (!/^\+[1-9]\d{1,14}$/.test(number)) {
      setOutboundError("Use E.164 format (e.g. +1234567890).");
      return;
    }

    let prompt = "";
    let firstMessage = "";
    let scriptId: number | null = null;
    let scriptName: string | null = null;
    let resolvedVoiceModel = voiceModel.trim();
    let businessId: string | null = null;

    if (outboundMode === "script") {
      if (!selectedScript) {
        setOutboundError("Select a script to continue.");
        return;
      }
      const scriptPrompt =
        typeof selectedScript.prompt === "string" ? selectedScript.prompt : "";
      const scriptFirstMessage =
        typeof selectedScript.first_message === "string"
          ? selectedScript.first_message
          : "";
      if (scriptPrompt.trim() === "" || scriptFirstMessage.trim() === "") {
        setOutboundError("Selected script is missing a prompt or first message.");
        return;
      }
      prompt = replacePlaceholders(scriptPrompt, placeholderValues);
      firstMessage = replacePlaceholders(scriptFirstMessage, placeholderValues);
      scriptId = selectedScript.id;
      scriptName = selectedScript.name;
      const scriptVoice =
        typeof selectedScript.voice_model === "string"
          ? selectedScript.voice_model.trim()
          : "";
      if (resolvedVoiceModel === "" && scriptVoice !== "") {
        resolvedVoiceModel = scriptVoice;
      }
      businessId = selectedScript.business_id ?? null;
    } else {
      prompt = customPrompt.trim();
      firstMessage = customFirstMessage.trim();
    }

    if (prompt === "" || firstMessage === "") {
      setOutboundError("Provide both a prompt and a first message.");
      return;
    }

    setOutboundBusy(true);
    trackEvent("outbound_call_clicked", { mode: outboundMode });
    hapticImpact();

    const body: Record<string, unknown> = {
      number,
      prompt,
      first_message: firstMessage,
    };
    const name = customerName.trim();
    if (name !== "") body.customer_name = name;
    if (scriptId !== null) body.script_id = scriptId;
    if (scriptName !== null) body.script = scriptName;
    if (resolvedVoiceModel !== "") body.voice_model = resolvedVoiceModel;
    if (businessId !== null && businessId !== "") body.business_id = businessId;
    const purposeValue = purpose.trim();
    if (purposeValue !== "") body.purpose = purposeValue;
    if (tone !== "auto") body.emotion = tone;
    if (urgency !== "auto") body.urgency = urgency;
    if (technicalLevel !== "auto") body.technical_level = technicalLevel;

    try {
      const response = await apiFetch<OutboundResponse>("/webapp/outbound-call", {
        method: "POST",
        body,
        idempotencyKey: createIdempotencyKey(),
      });
      const callSid = response.call_sid ?? "";
      const callSuffix = callSid !== "" ? ` • ${callSid}` : "";
      hapticSuccess();
      trackEvent("outbound_call_success", {
        call_sid: callSid !== "" ? callSid : "unknown",
      });
      setOutboundSuccess(
        `Call queued to ${number}${callSuffix}`,
      );
      if (callSid !== "") {
        navigate(`/calls/${callSid}`);
      }
    } catch (error) {
      hapticError();
      trackEvent("outbound_call_failed", { mode: outboundMode });
      setOutboundError(
        error instanceof Error
          ? error.message
          : "Failed to place outbound call.",
      );
    } finally {
      setOutboundBusy(false);
    }
  };

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
      await fetchInboundQueue().catch(() => {});
    } catch {
      hapticError();
      trackEvent(`inbound_${action}_failed`, { call_sid: callSid });
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
      await fetchInboundQueue().catch(() => {});
    } catch {
      trackEvent("inbound_callback_failed", { call_sid: callSid });
    } finally {
      setBusyCall(null);
    }
  };

  const priorityLabel = (call: (typeof inboundQueue)[number]) =>
    call.priority ?? "normal";

  return (
    <div className="wallet-page">
      {inboundMeta.error !== null && inboundMeta.error !== "" && (
        <Banner
          type="inline"
          header={inboundMeta.errorKind === "offline" ? "You're offline" : "Error"}
          description={inboundMeta.error}
          className="wallet-banner"
        />
      )}
      {inboundNotice && (
        <Banner
          type="inline"
          header="Incoming call pending"
          description={inboundNotice.message}
          className="wallet-banner"
        />
      )}
      {outboundError !== null && outboundError !== "" && (
        <Banner
          type="inline"
          header="Outbound call failed"
          description={outboundError}
          className="wallet-banner"
        />
      )}
      {outboundSuccess !== null && outboundSuccess !== "" && (
        <Banner
          type="inline"
          header="Outbound call queued"
          description={outboundSuccess}
          className="wallet-banner"
        />
      )}
      {isAdmin && (
        <div className="card-section">
          <div className="card-header">
            <span>Outbound call</span>
            <span className="card-header-muted">Admin</span>
          </div>
          <div className="card-form">
            <Select
              header="Call type"
              value={outboundMode}
              onChange={(event) =>
                setOutboundMode(event.target.value as "script" | "custom")
              }
            >
              <option value="script">Use script</option>
              <option value="custom">Custom prompt</option>
            </Select>

            <Input
              header="Phone number"
              placeholder="+1234567890"
              value={phoneNumber}
              onChange={(event) => setPhoneNumber(event.target.value)}
              type="tel"
              disabled={outboundBusy}
            />

            <Input
              header="Contact name (optional)"
              placeholder="Customer name"
              value={customerName}
              onChange={(event) => setCustomerName(event.target.value)}
              disabled={outboundBusy}
            />

            {outboundMode === "script" ? (
              <>
                <Select
                  header="Script"
                  value={selectedScriptId}
                  onChange={(event) => setSelectedScriptId(event.target.value)}
                  disabled={outboundBusy || scriptsLoading}
                >
                  <option value="">
                    {scriptsLoading
                      ? "Loading scripts..."
                      : "Select a script"}
                  </option>
                  {scripts.map((script) => (
                    <option key={script.id} value={String(script.id)}>
                      {script.name}
                    </option>
                  ))}
                </Select>
                {scriptsErrorMessage !== "" && (
                  <div className="form-hint">{scriptsErrorMessage}</div>
                )}
                {scriptDescription !== "" && (
                  <div className="form-hint">
                    {scriptDescription}
                  </div>
                )}
                {scriptPlaceholders.length > 0 && (
                  <>
                    <div className="form-hint">
                      Script variables (optional)
                    </div>
                    {scriptPlaceholders.map((token) => (
                      <Input
                        key={token}
                        header={`Value for ${token}`}
                        placeholder={`Enter ${token}`}
                        value={placeholderValues[token] ?? ""}
                        onChange={(event) =>
                          setPlaceholderValues((prev) => ({
                            ...prev,
                            [token]: event.target.value,
                          }))
                        }
                        disabled={outboundBusy}
                      />
                    ))}
                  </>
                )}
              </>
            ) : (
              <>
                <Textarea
                  header="Agent prompt"
                  placeholder="Describe how the AI should behave..."
                  value={customPrompt}
                  onChange={(event) => setCustomPrompt(event.target.value)}
                  disabled={outboundBusy}
                />
                <Textarea
                  header="First message"
                  placeholder="Hello! This is ..."
                  value={customFirstMessage}
                  onChange={(event) =>
                    setCustomFirstMessage(event.target.value)
                  }
                  disabled={outboundBusy}
                />
              </>
            )}

            <Input
              header="Voice model (optional)"
              placeholder={
                scriptVoiceModel !== ""
                  ? `Default: ${scriptVoiceModel}`
                  : "Leave empty to use default"
              }
              value={voiceModel}
              onChange={(event) => setVoiceModel(event.target.value)}
              disabled={outboundBusy}
            />

            <div className="card-actions">
              <Button
                size="s"
                mode="plain"
                onClick={() => setShowAdvanced((prev) => !prev)}
                disabled={outboundBusy}
              >
                {showAdvanced ? "Hide advanced options" : "Show advanced options"}
              </Button>
            </div>

            {showAdvanced && (
              <>
                <Input
                  header="Purpose (optional)"
                  placeholder="security, appointment, follow_up..."
                  value={purpose}
                  onChange={(event) => setPurpose(event.target.value)}
                  disabled={outboundBusy}
                />
                <Select
                  header="Tone"
                  value={tone}
                  onChange={(event) => setTone(event.target.value)}
                  disabled={outboundBusy}
                >
                  {TONE_OPTIONS.map((option) => (
                    <option key={option.id} value={option.id}>
                      {option.label}
                    </option>
                  ))}
                </Select>
                <Select
                  header="Urgency"
                  value={urgency}
                  onChange={(event) => setUrgency(event.target.value)}
                  disabled={outboundBusy}
                >
                  {URGENCY_OPTIONS.map((option) => (
                    <option key={option.id} value={option.id}>
                      {option.label}
                    </option>
                  ))}
                </Select>
                <Select
                  header="Technical level"
                  value={technicalLevel}
                  onChange={(event) => setTechnicalLevel(event.target.value)}
                  disabled={outboundBusy}
                >
                  {TECH_OPTIONS.map((option) => (
                    <option key={option.id} value={option.id}>
                      {option.label}
                    </option>
                  ))}
                </Select>
              </>
            )}

            <div className="card-actions">
              <Button
                size="m"
                mode="filled"
                onClick={() => void handleOutboundSubmit()}
                disabled={
                  outboundBusy ||
                  phoneNumber.trim() === "" ||
                  (outboundMode === "script"
                    ? selectedScriptId === ""
                    : customPrompt.trim() === "" ||
                      customFirstMessage.trim() === "")
                }
              >
                {outboundBusy ? "Placing call..." : "Place outbound call"}
              </Button>
            </div>
          </div>
        </div>
      )}
      <div className="card-section">
        <div className="card-header">
          <span>Inbound queue</span>
          <Button
            size="s"
            mode="bezeled"
            onClick={() => void fetchInboundQueue().catch(() => {})}
          >
            Refresh
          </Button>
        </div>
        {inboundMeta.loading && inboundQueue.length === 0 ? (
          <SkeletonList rows={3} />
        ) : inboundQueue.length === 0 ? (
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
                      {call.route_label ?? call.script ?? "Inbound route"}
                    </div>
                    <div className="card-item-meta">
                      <MaskedPhone value={call.from ?? "Unknown caller"} />
                    </div>
                    {rule && (
                      <div className="rule-summary">
                        <span
                          className={`priority-badge ${priorityLabel(call)}`}
                        >
                          {priorityLabel(call)}
                        </span>
                        <span className="rule-chip">
                          {rule.decision ?? "allow"}
                        </span>
                        <span className="rule-chip">
                          {rule.label ?? "default"}
                        </span>
                        {typeof rule.recent_calls === "number" && (
                          <span className="rule-chip">
                            {rule.recent_calls} recent
                          </span>
                        )}
                        {rule.risk !== null && rule.risk !== undefined && rule.risk !== "" && (
                          <span className={`rule-chip risk-${rule.risk}`}>
                            {rule.risk}
                          </span>
                        )}
                      </div>
                    )}
                  </div>
                  {isAdmin && (
                    <InlineButtons mode="bezeled">
                      <InlineButtons.Item
                        text="Answer"
                        disabled={disabled}
                        onClick={() =>
                          void handleAction(call.call_sid, "answer")
                        }
                      />
                      <InlineButtons.Item
                        text="Decline"
                        disabled={disabled}
                        onClick={() =>
                          void handleAction(call.call_sid, "decline")
                        }
                      />
                      <InlineButtons.Item
                        text="Callback"
                        disabled={disabled}
                        onClick={() => void handleCallback(call.call_sid)}
                      />
                    </InlineButtons>
                  )}
                </div>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );
}
