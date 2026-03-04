const PROVIDER_CHANNELS = Object.freeze({
  CALL: "call",
  SMS: "sms",
  EMAIL: "email",
});

const PROVIDER_FLOW_REGISTRY = Object.freeze({
  [PROVIDER_CHANNELS.CALL]: Object.freeze({
    twilio: Object.freeze({
      label: "Twilio Voice",
      flows: Object.freeze([
        "outbound_voice",
        "inbound_voice",
        "status_webhooks",
        "voice_stream",
        "digit_capture",
        "payment_native",
      ]),
    }),
    vonage: Object.freeze({
      label: "Vonage Voice",
      flows: Object.freeze([
        "outbound_voice",
        "inbound_voice",
        "status_webhooks",
        "voice_stream",
        "digit_capture",
      ]),
    }),
    aws: Object.freeze({
      label: "AWS Connect",
      flows: Object.freeze([
        "outbound_voice",
        "status_webhooks",
        "voice_stream",
      ]),
    }),
  }),
  [PROVIDER_CHANNELS.SMS]: Object.freeze({
    twilio: Object.freeze({
      label: "Twilio SMS",
      flows: Object.freeze([
        "outbound_sms",
        "inbound_sms",
        "delivery_receipts",
        "status_reconcile",
      ]),
    }),
    vonage: Object.freeze({
      label: "Vonage SMS",
      flows: Object.freeze([
        "outbound_sms",
        "inbound_sms",
        "delivery_receipts",
      ]),
    }),
    aws: Object.freeze({
      label: "AWS Pinpoint SMS",
      flows: Object.freeze([
        "outbound_sms",
      ]),
    }),
  }),
  [PROVIDER_CHANNELS.EMAIL]: Object.freeze({
    sendgrid: Object.freeze({
      label: "SendGrid",
      flows: Object.freeze(["outbound_email", "delivery_events"]),
    }),
    mailgun: Object.freeze({
      label: "Mailgun",
      flows: Object.freeze(["outbound_email", "delivery_events"]),
    }),
    ses: Object.freeze({
      label: "AWS SES",
      flows: Object.freeze(["outbound_email", "delivery_events"]),
    }),
  }),
});

const CALL_STATUS_MAP = Object.freeze({
  twilio: Object.freeze({
    queued: "queued",
    initiated: "initiated",
    ringing: "ringing",
    answered: "answered",
    "in-progress": "in-progress",
    in_progress: "in-progress",
    completed: "completed",
    busy: "busy",
    "no-answer": "no-answer",
    no_answer: "no-answer",
    failed: "failed",
    canceled: "canceled",
    cancelled: "canceled",
    voicemail: "no-answer",
  }),
  vonage: Object.freeze({
    started: "initiated",
    ringing: "ringing",
    answered: "answered",
    machine: "answered",
    human: "answered",
    completed: "completed",
    rejected: "canceled",
    canceled: "canceled",
    cancelled: "canceled",
    failed: "failed",
    busy: "busy",
    timeout: "no-answer",
    unanswered: "no-answer",
  }),
  aws: Object.freeze({
    initiated: "initiated",
    connected: "answered",
    ended: "completed",
    failed: "failed",
    no_answer: "no-answer",
    "no-answer": "no-answer",
    busy: "busy",
  }),
});

const CALL_NOTIFICATION_BY_STATUS = Object.freeze({
  queued: "call_initiated",
  initiated: "call_initiated",
  ringing: "call_ringing",
  answered: "call_answered",
  "in-progress": "call_in_progress",
  completed: "call_completed",
  busy: "call_busy",
  "no-answer": "call_no_answer",
  failed: "call_failed",
  canceled: "call_canceled",
});
const CANONICAL_CALL_STATUS_SET = new Set(
  Object.keys(CALL_NOTIFICATION_BY_STATUS),
);

const TWILIO_SMS_STATUS_MAP = Object.freeze({
  accepted: "queued",
  scheduled: "queued",
  queued: "queued",
  sending: "sending",
  sent: "sent",
  delivered: "delivered",
  undelivered: "failed",
  failed: "failed",
  canceled: "canceled",
  cancelled: "canceled",
  read: "read",
});

function normalizeChannel(value) {
  return String(value || "").trim().toLowerCase();
}

function normalizeProviderName(value) {
  return String(value || "").trim().toLowerCase();
}

function normalizeFlowName(value) {
  return String(value || "").trim().toLowerCase();
}

function getRegistryForChannel(channel) {
  return PROVIDER_FLOW_REGISTRY[normalizeChannel(channel)] || null;
}

function getProviderEntry(channel, provider) {
  const registry = getRegistryForChannel(channel);
  if (!registry) return null;
  return registry[normalizeProviderName(provider)] || null;
}

function listSupportedProviders(channel) {
  const registry = getRegistryForChannel(channel);
  return registry ? Object.keys(registry) : [];
}

function listProviderFlows(channel, provider, context = {}) {
  const normalizedChannel = normalizeChannel(channel);
  const normalizedProvider = normalizeProviderName(provider);
  const entry = getProviderEntry(normalizedChannel, normalizedProvider);
  if (!entry) return [];
  const flows = Array.isArray(entry.flows) ? [...entry.flows] : [];
  if (
    normalizedChannel === PROVIDER_CHANNELS.CALL &&
    normalizedProvider === "vonage" &&
    context.vonageDtmfWebhookEnabled !== true
  ) {
    return flows.filter((flow) => flow !== "digit_capture");
  }
  return flows;
}

function providerSupportsFlow(channel, provider, flow, context = {}) {
  const targetFlow = normalizeFlowName(flow);
  if (!targetFlow) return true;
  return listProviderFlows(channel, provider, context).includes(targetFlow);
}

function buildProviderOrder(preferredProvider, providers = []) {
  const preferred = normalizeProviderName(preferredProvider);
  const normalizedProviders = (Array.isArray(providers) ? providers : [])
    .map((provider) => normalizeProviderName(provider))
    .filter(Boolean);
  const order = [];
  if (preferred) {
    order.push(preferred);
  }
  normalizedProviders.forEach((provider) => {
    if (!order.includes(provider)) {
      order.push(provider);
    }
  });
  return order;
}

function resolveProviderExecutionOrder(options = {}) {
  const {
    channel = PROVIDER_CHANNELS.CALL,
    preferredProvider = null,
    providers = [],
    readiness = {},
    requestedFlow = null,
    context = {},
    failoverEnabled = true,
    isProviderDegraded = () => false,
  } = options;

  const normalizedChannel = normalizeChannel(channel);
  const flow = normalizeFlowName(requestedFlow);
  const orderedProviders = buildProviderOrder(preferredProvider, providers);
  const healthy = [];
  const degraded = [];
  const blocked = [];

  orderedProviders.forEach((provider) => {
    if (!provider) return;
    if (readiness && readiness[provider] !== true) {
      blocked.push({
        provider,
        reason: "not_ready",
      });
      return;
    }
    if (flow && !providerSupportsFlow(normalizedChannel, provider, flow, context)) {
      blocked.push({
        provider,
        reason: "flow_unsupported",
        flow,
      });
      return;
    }
    if (failoverEnabled && typeof isProviderDegraded === "function") {
      const degradedState = isProviderDegraded(provider) === true;
      if (degradedState) {
        degraded.push(provider);
        return;
      }
    }
    healthy.push(provider);
  });

  const attemptOrder = healthy.length > 0
    ? [...healthy, ...degraded]
    : [...degraded];

  return {
    channel: normalizedChannel,
    requested_flow: flow || null,
    preferred_provider: normalizeProviderName(preferredProvider) || null,
    ordered_providers: orderedProviders,
    healthy_providers: healthy,
    degraded_providers: degraded,
    blocked_providers: blocked,
    attempt_order: attemptOrder,
    selected_provider: attemptOrder[0] || null,
  };
}

function resolvePaymentExecutionMode(options = {}) {
  const {
    provider,
    featureConfig = {},
    hasNativeAdapter = null,
    smsFallbackEnabled = false,
    smsServiceReady = false,
  } = options;
  const normalizedProvider = normalizeProviderName(provider) || "unknown";

  const enabled = featureConfig?.enabled !== false;
  if (!enabled) {
    return {
      mode: "disabled",
      reason: "feature_disabled",
      provider: normalizedProvider,
      enabled: false,
    };
  }
  if (featureConfig?.kill_switch === true) {
    return {
      mode: "disabled",
      reason: "kill_switch",
      provider: normalizedProvider,
      enabled: false,
    };
  }

  if (normalizedProvider === "twilio") {
    if (featureConfig?.allow_twilio === false) {
      return {
        mode: "disabled",
        reason: "twilio_disabled",
        provider: normalizedProvider,
        enabled: false,
      };
    }
    if (hasNativeAdapter === false) {
      return {
        mode: "disabled",
        reason: "native_adapter_unavailable",
        provider: normalizedProvider,
        enabled: false,
      };
    }
    return {
      mode: "native",
      reason: null,
      provider: normalizedProvider,
      enabled: true,
    };
  }

  if (smsFallbackEnabled !== true) {
    return {
      mode: "disabled",
      reason: "sms_fallback_disabled",
      provider: normalizedProvider,
      enabled: false,
    };
  }
  if (smsServiceReady !== true) {
    return {
      mode: "disabled",
      reason: "sms_service_unavailable",
      provider: normalizedProvider,
      enabled: false,
    };
  }
  return {
    mode: "sms_fallback",
    reason: null,
    provider: normalizedProvider,
    enabled: true,
  };
}

function normalizeCallStatusValue(value = "") {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "-")
    .replace(/_/g, "-");
}

function toCanonicalCallStatus(provider, status) {
  const normalizedProvider = normalizeProviderName(provider);
  const normalizedStatus = normalizeCallStatusValue(status);
  const providerMap = CALL_STATUS_MAP[normalizedProvider] || {};
  return providerMap[normalizedStatus] || normalizedStatus || null;
}

function notificationTypeForCallStatus(status) {
  const normalized = normalizeCallStatusValue(status);
  return CALL_NOTIFICATION_BY_STATUS[normalized] || `call_${normalized || "unknown"}`;
}

function isKnownCanonicalCallStatus(status) {
  return CANONICAL_CALL_STATUS_SET.has(normalizeCallStatusValue(status));
}

function toFiniteNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function maxFiniteNumber(values = []) {
  const numbers = values
    .map((value) => toFiniteNumber(value))
    .filter((value) => Number.isFinite(value));
  if (!numbers.length) return null;
  return Math.max(...numbers);
}

function buildCanonicalCallStatusEvent(provider, payload = {}, options = {}) {
  const normalizedProvider = normalizeProviderName(provider);
  const callSid = String(
    options.callSid
      || payload.CallSid
      || payload.callSid
      || payload.call_sid
      || payload.client_ref
      || "",
  ).trim() || null;
  const rawStatus =
    payload.CallStatus
    || payload.callStatus
    || payload.status
    || payload.event
    || null;
  const mappedStatus = toCanonicalCallStatus(normalizedProvider, rawStatus);
  const status = isKnownCanonicalCallStatus(mappedStatus) ? mappedStatus : null;
  const duration = maxFiniteNumber([
    payload.Duration,
    payload.CallDuration,
    payload.DialCallDuration,
    payload.duration,
    payload.call_duration,
    payload.conversation_duration,
    payload.usage_duration,
  ]);
  const timestamp = String(
    payload.Timestamp
    || payload.timestamp
    || payload.eventTimestamp
    || payload.event_time
    || payload.created_at
    || "",
  ).trim() || null;
  const answeredBy = String(
    payload.AnsweredBy
    || payload.answered_by
    || payload.answeredBy
    || "",
  ).trim() || null;
  return {
    provider: normalizedProvider,
    call_sid: callSid,
    raw_status: rawStatus ? String(rawStatus).trim() : null,
    status,
    notification_type: status ? notificationTypeForCallStatus(status) : null,
    duration,
    answered_by: answeredBy,
    error_code: payload.ErrorCode || payload.errorCode || null,
    error_message: payload.ErrorMessage || payload.errorMessage || null,
    timestamp,
  };
}

function normalizeVonageSmsDeliveryStatus(value = "", errorCode = null) {
  const normalized = String(value || "").trim().toLowerCase();
  if (!normalized) {
    return errorCode ? "failed" : "queued";
  }
  const parsedCode = Number(normalized);
  if (Number.isFinite(parsedCode)) {
    return parsedCode === 0 ? "delivered" : "failed";
  }
  if (["delivered", "read"].includes(normalized)) {
    return normalized;
  }
  if (
    [
      "accepted",
      "submitted",
      "buffered",
      "queued",
      "sent",
      "sending",
      "unknown",
    ].includes(normalized)
  ) {
    return normalized === "submitted" || normalized === "buffered"
      ? "sent"
      : normalized;
  }
  if (
    [
      "failed",
      "rejected",
      "undeliverable",
      "expired",
      "unroutable",
      "deleted",
    ].includes(normalized)
  ) {
    return "failed";
  }
  return normalized;
}

function normalizeSmsStatus(provider, status, errorCode = null) {
  const normalizedProvider = normalizeProviderName(provider);
  const normalizedStatus = String(status || "").trim().toLowerCase();
  if (normalizedProvider === "twilio") {
    return TWILIO_SMS_STATUS_MAP[normalizedStatus] || normalizedStatus || null;
  }
  if (normalizedProvider === "vonage") {
    return normalizeVonageSmsDeliveryStatus(normalizedStatus, errorCode);
  }
  return normalizedStatus || null;
}

function buildCanonicalSmsInboundEvent(provider, payload = {}) {
  const normalizedProvider = normalizeProviderName(provider);
  if (normalizedProvider === "vonage") {
    const rawStatus = String(payload.status || "received").trim() || "received";
    const messageSid = String(
      payload.messageId || payload.message_id || payload["message-id"] || "",
    ).trim() || null;
    return {
      provider: normalizedProvider,
      message_sid: messageSid,
      from: String(payload.msisdn || payload.from || "").trim() || null,
      to: String(payload.to || "").trim() || null,
      body: String(payload.text || payload.body || "").trim(),
      raw_status: rawStatus,
      status: normalizeSmsStatus(normalizedProvider, rawStatus),
      message_type: String(payload.type || "").trim() || null,
      timestamp: String(
        payload["message-timestamp"] || payload.timestamp || payload.scts || "",
      ).trim() || null,
    };
  }
  const messageSid = String(payload.MessageSid || payload.SmsSid || payload.messageSid || "").trim() || null;
  return {
    provider: normalizedProvider,
    message_sid: messageSid,
    from: String(payload.From || payload.from || "").trim() || null,
    to: String(payload.To || payload.to || "").trim() || null,
    body: String(payload.Body || payload.body || "").trim(),
    raw_status: String(payload.SmsStatus || payload.MessageStatus || "received").trim(),
    status: normalizeSmsStatus(normalizedProvider, payload.SmsStatus || payload.MessageStatus || "received"),
    timestamp: String(payload.Timestamp || payload.timestamp || "").trim() || null,
  };
}

function buildCanonicalSmsDeliveryEvent(provider, payload = {}) {
  const normalizedProvider = normalizeProviderName(provider);
  if (normalizedProvider === "vonage") {
    const messageSid = String(
      payload.messageId || payload.message_id || payload["message-id"] || "",
    ).trim() || null;
    const errorCode = String(
      payload["err-code"] ||
      payload.err_code ||
      payload.errorCode ||
      payload.error_code ||
      "",
    ).trim() || null;
    return {
      provider: normalizedProvider,
      message_sid: messageSid,
      raw_status: String(payload.status || "").trim() || null,
      status: normalizeSmsStatus(normalizedProvider, payload.status, errorCode),
      error_code: errorCode,
      error_message: String(
        payload["error-text"] ||
        payload.error_text ||
        payload.errorText ||
        payload.reason ||
        "",
      ).trim() || null,
      timestamp: String(
        payload["message-timestamp"] ||
        payload.timestamp ||
        payload.scts ||
        "",
      ).trim() || null,
    };
  }
  const messageSid = String(
    payload.MessageSid || payload.SmsSid || payload.messageSid || "",
  ).trim() || null;
  return {
    provider: normalizedProvider,
    message_sid: messageSid,
    raw_status: String(payload.MessageStatus || payload.SmsStatus || payload.status || "").trim() || null,
    status: normalizeSmsStatus(normalizedProvider, payload.MessageStatus || payload.SmsStatus || payload.status),
    error_code: payload.ErrorCode || payload.errorCode || null,
    error_message: payload.ErrorMessage || payload.errorMessage || null,
    timestamp: String(payload.Timestamp || payload.timestamp || "").trim() || null,
  };
}

function buildProviderCompatibilityReport(options = {}) {
  const {
    callReadiness = {},
    smsReadiness = {},
    emailReadiness = {},
    activeProviders = {},
    storedProviders = {},
    paymentFeatureConfig = {},
    smsFallbackEnabled = false,
    smsServiceReady = false,
    vonageDtmfWebhookEnabled = false,
    isProviderDegraded = () => false,
  } = options;

  const callProviders = listSupportedProviders(PROVIDER_CHANNELS.CALL);
  const smsProviders = listSupportedProviders(PROVIDER_CHANNELS.SMS);
  const emailProviders = listSupportedProviders(PROVIDER_CHANNELS.EMAIL);
  const callFlowContext = { vonageDtmfWebhookEnabled };

  const callMatrix = {};
  callProviders.forEach((provider) => {
    const flows = listProviderFlows(PROVIDER_CHANNELS.CALL, provider, callFlowContext);
    callMatrix[provider] = {
      ready: callReadiness[provider] === true,
      degraded: isProviderDegraded(provider) === true,
      flows,
      payment_mode: resolvePaymentExecutionMode({
        provider,
        featureConfig: paymentFeatureConfig,
        hasNativeAdapter: provider === "twilio",
        smsFallbackEnabled,
        smsServiceReady,
      }).mode,
    };
  });

  const smsMatrix = {};
  smsProviders.forEach((provider) => {
    smsMatrix[provider] = {
      ready: smsReadiness[provider] === true,
      flows: listProviderFlows(PROVIDER_CHANNELS.SMS, provider),
    };
  });

  const emailMatrix = {};
  emailProviders.forEach((provider) => {
    emailMatrix[provider] = {
      ready: emailReadiness[provider] === true,
      flows: listProviderFlows(PROVIDER_CHANNELS.EMAIL, provider),
    };
  });

  const baselineCallFlows = listProviderFlows(
    PROVIDER_CHANNELS.CALL,
    "twilio",
    callFlowContext,
  );
  const callParityGaps = {};
  Object.entries(callMatrix).forEach(([provider, details]) => {
    callParityGaps[provider] = baselineCallFlows.filter(
      (flow) => !details.flows.includes(flow),
    );
  });

  const baselineSmsFlows = listProviderFlows(PROVIDER_CHANNELS.SMS, "twilio");
  const smsParityGaps = {};
  Object.entries(smsMatrix).forEach(([provider, details]) => {
    smsParityGaps[provider] = baselineSmsFlows.filter(
      (flow) => !details.flows.includes(flow),
    );
  });

  return {
    generated_at: new Date().toISOString(),
    active_providers: {
      call: normalizeProviderName(activeProviders.call) || null,
      sms: normalizeProviderName(activeProviders.sms) || null,
      email: normalizeProviderName(activeProviders.email) || null,
    },
    stored_providers: {
      call: normalizeProviderName(storedProviders.call) || null,
      sms: normalizeProviderName(storedProviders.sms) || null,
      email: normalizeProviderName(storedProviders.email) || null,
    },
    payment: {
      feature_enabled: paymentFeatureConfig?.enabled !== false,
      kill_switch: paymentFeatureConfig?.kill_switch === true,
      sms_fallback_enabled: smsFallbackEnabled === true,
      sms_service_ready: smsServiceReady === true,
    },
    channels: {
      call: {
        providers: callMatrix,
        baseline_provider: "twilio",
        baseline_flows: baselineCallFlows,
        parity_gaps: callParityGaps,
      },
      sms: {
        providers: smsMatrix,
        baseline_provider: "twilio",
        baseline_flows: baselineSmsFlows,
        parity_gaps: smsParityGaps,
      },
      email: {
        providers: emailMatrix,
      },
    },
  };
}

module.exports = {
  PROVIDER_CHANNELS,
  PROVIDER_FLOW_REGISTRY,
  listSupportedProviders,
  listProviderFlows,
  providerSupportsFlow,
  resolveProviderExecutionOrder,
  resolvePaymentExecutionMode,
  toCanonicalCallStatus,
  notificationTypeForCallStatus,
  buildCanonicalCallStatusEvent,
  normalizeSmsStatus,
  buildCanonicalSmsInboundEvent,
  buildCanonicalSmsDeliveryEvent,
  buildProviderCompatibilityReport,
};
