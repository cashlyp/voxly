require("./config/bootstrapLogger");
require("dotenv").config();

const express = require("express");
const fetch = require("node-fetch");
const ExpressWs = require("express-ws");
const path = require("path");
const crypto = require("crypto");
const helmet = require("helmet");
const cors = require("cors");
const compression = require("compression");
const rateLimit = require("express-rate-limit");

const { EnhancedGptService } = require("./routes/gpt");
const { StreamService } = require("./routes/stream");
const { TranscriptionService } = require("./routes/transcription");
const { TextToSpeechService } = require("./routes/tts");
const { recordingService } = require("./routes/recording");
const { EnhancedSmsService } = require("./routes/sms.js");
const { EmailService } = require("./routes/email");
const { createTwilioGatherHandler } = require("./routes/gather");
const { registerCallRoutes } = require("./controllers/callRoutes");
const { registerStatusRoutes } = require("./controllers/statusRoutes");
const { registerWebhookRoutes } = require("./controllers/webhookRoutes");
const Database = require("./db/db");
const { webhookService } = require("./routes/status");
const twilioSignature = require("./middleware/twilioSignature");
const DynamicFunctionEngine = require("./functions/DynamicFunctionEngine");
const { createDigitCollectionService } = require("./functions/Digit");
const { formatDigitCaptureLabel } = require("./functions/Labels");
const config = require("./config");
const {
  PROVIDER_CHANNELS,
  SUPPORTED_CALL_PROVIDERS,
  SUPPORTED_SMS_PROVIDERS,
  SUPPORTED_EMAIL_PROVIDERS,
  getActiveCallProvider,
  getActiveSmsProvider,
  getActiveEmailProvider,
  getStoredCallProvider,
  getStoredSmsProvider,
  getStoredEmailProvider,
  setActiveCallProvider,
  setActiveSmsProvider,
  setActiveEmailProvider,
  setStoredCallProvider,
  setStoredSmsProvider,
  setStoredEmailProvider,
  normalizeProvider,
} = require("./adapters/providerState");
const {
  AwsConnectAdapter,
  AwsTtsAdapter,
  VonageVoiceAdapter,
} = require("./adapters");
const { v4: uuidv4 } = require("uuid");
const { WaveFile } = require("wavefile");

const isProduction = process.env.NODE_ENV === "production";
const appVersion = (() => {
  try {
    return (
      process.env.APP_VERSION ||
      process.env.VERCEL_GIT_COMMIT_SHA ||
      process.env.GIT_SHA ||
      require("./package.json").version ||
      "unknown"
    );
  } catch {
    return "unknown";
  }
})();

const twilio = require("twilio");
const VoiceResponse = twilio.twiml.VoiceResponse;

const DEFAULT_INBOUND_PROMPT =
  "You are an intelligent AI assistant capable of adapting to different business contexts and customer needs. Be professional, helpful, and responsive to customer communication styles. You must add a '•' symbol every 5 to 10 words at natural pauses where your response can be split for text to speech.";
const DEFAULT_INBOUND_FIRST_MESSAGE = "Hello! How can I assist you today?";
const INBOUND_DEFAULT_SETTING_KEY = "inbound_default_script_id";
const INBOUND_DEFAULT_CACHE_MS = 15000;
let inboundDefaultScriptId = null;
let inboundDefaultScript = null;
let inboundDefaultLoadedAt = 0;

const liveConsoleAudioTickMs = Number.isFinite(
  Number(config.liveConsole?.audioTickMs),
)
  ? Number(config.liveConsole?.audioTickMs)
  : 160;
const liveConsoleUserLevelThreshold = Number.isFinite(
  Number(config.liveConsole?.userLevelThreshold),
)
  ? Number(config.liveConsole?.userLevelThreshold)
  : 0.08;
const liveConsoleUserHoldMs = Number.isFinite(
  Number(config.liveConsole?.userHoldMs),
)
  ? Number(config.liveConsole?.userHoldMs)
  : 450;

const HMAC_HEADER_TIMESTAMP = "x-api-timestamp";
const HMAC_HEADER_SIGNATURE = "x-api-signature";
const HMAC_BYPASS_PATH_PREFIXES = [
  "/webhook/",
  "/capture/",
  "/incoming",
  "/aws/transcripts",
  "/connection",
  "/vonage/stream",
  "/aws/stream",
];

let db;
let digitService;
const functionEngine = new DynamicFunctionEngine();
let smsService = new EnhancedSmsService({
  getActiveProvider: () => getActiveSmsProvider(),
});
let emailService;
const sttFallbackCalls = new Set();
const streamTimeoutCalls = new Set();
const inboundRateBuckets = new Map();
const streamStartTimes = new Map();
const sttFailureCounts = new Map();
const activeStreamConnections = new Map();
const streamStartSeen = new Map(); // callSid -> streamSid (dedupe starts)
const streamStopSeen = new Set(); // callSid:streamSid (dedupe stops)
const streamRetryState = new Map(); // callSid -> { attempts, nextDelayMs }
const streamAuthBypass = new Map(); // callSid -> { reason, at }
const streamStatusDedupe = new Map(); // callSid:streamSid:event -> ts
const callStatusDedupe = new Map(); // callSid:status:sequence:timestamp -> ts
const callLifecycle = new Map(); // callSid -> { status, updatedAt }
const streamLastMediaAt = new Map(); // callSid -> timestamp
const sttLastFrameAt = new Map(); // callSid -> timestamp
const streamWatchdogState = new Map(); // callSid -> { noMediaNotifiedAt, noMediaEscalatedAt, sttNotifiedAt }
const providerEventDedupe = new Map(); // source:hash -> ts
const providerHealth = new Map();
const keypadProviderGuardWarnings = new Set(); // provider -> warning emitted
const keypadProviderOverrides = new Map(); // scopeKey -> { provider, expiresAt, ... }
const keypadDtmfSeen = new Map(); // callSid -> { seenAt, source, digitsLength }
const keypadDtmfWatchdogs = new Map(); // callSid -> timeoutId
const vonageWebhookJtiCache = new Map(); // jti -> expiresAtMs
const callRuntimePersistTimers = new Map(); // callSid -> timeoutId
const callRuntimePendingWrites = new Map(); // callSid -> patch
const callToolInFlight = new Map(); // callSid -> { tool, startedAt }
let callJobProcessing = false;
let paymentReconcileRunning = false;
let backgroundWorkersStarted = false;
const outboundRateBuckets = new Map(); // namespace:key -> { count, windowStart }
const callLifecycleCleanupTimers = new Map();
const CALL_STATUS_DEDUPE_MS = 3000;
const CALL_STATUS_DEDUPE_MAX = 5000;
const PROVIDER_EVENT_DEDUPE_MS = 5 * 60 * 1000;
const PROVIDER_EVENT_DEDUPE_MAX = 10000;
const VONAGE_WEBHOOK_JTI_CACHE_MAX = 5000;
const CALL_RUNTIME_PERSIST_DEBOUNCE_MS = 150;
const CALL_RUNTIME_STATE_STALE_MS = 6 * 60 * 60 * 1000;
const TOOL_LOCK_TTL_MS = 20 * 1000;
const KEYPAD_PROVIDER_OVERRIDE_SETTING_KEY = "keypad_provider_overrides_v1";
const CALL_PROVIDER_SETTING_KEY = "call_provider_v1";
const SMS_PROVIDER_SETTING_KEY = "sms_provider_v1";
const EMAIL_PROVIDER_SETTING_KEY = "email_provider_v1";
const PAYMENT_FEATURE_SETTING_KEY = "payment_feature_config_v1";

const defaultPaymentFeatureConfig = Object.freeze({
  enabled: config.payment?.enabled !== false,
  kill_switch: config.payment?.killSwitch === true,
  allow_twilio: config.payment?.allowTwilio !== false,
  require_script_opt_in: config.payment?.requireScriptOptIn === true,
  default_currency: String(config.payment?.defaultCurrency || "USD")
    .trim()
    .toUpperCase()
    .slice(0, 3) || "USD",
  min_amount:
    Number.isFinite(Number(config.payment?.minAmount)) &&
    Number(config.payment?.minAmount) > 0
      ? Number(config.payment?.minAmount)
      : 0,
  max_amount:
    Number.isFinite(Number(config.payment?.maxAmount)) &&
    Number(config.payment?.maxAmount) > 0
      ? Number(config.payment?.maxAmount)
      : 0,
  max_attempts_per_call:
    Number.isFinite(Number(config.payment?.maxAttemptsPerCall)) &&
    Number(config.payment?.maxAttemptsPerCall) > 0
      ? Math.max(1, Math.floor(Number(config.payment?.maxAttemptsPerCall)))
      : 3,
  retry_cooldown_ms:
    Number.isFinite(Number(config.payment?.retryCooldownMs)) &&
    Number(config.payment?.retryCooldownMs) >= 0
      ? Math.max(0, Math.floor(Number(config.payment?.retryCooldownMs)))
      : 20000,
  webhook_idempotency_ttl_ms:
    Number.isFinite(Number(config.payment?.webhookIdempotencyTtlMs)) &&
    Number(config.payment?.webhookIdempotencyTtlMs) > 0
      ? Number(config.payment?.webhookIdempotencyTtlMs)
      : 300000,
});
let paymentFeatureConfig = { ...defaultPaymentFeatureConfig };

function stableStringify(value) {
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    const items = value.map((item) =>
      item === undefined ? "null" : stableStringify(item),
    );
    return `[${items.join(",")}]`;
  }
  const keys = Object.keys(value)
    .filter((key) => value[key] !== undefined)
    .sort();
  const entries = keys.map(
    (key) => `${JSON.stringify(key)}:${stableStringify(value[key])}`,
  );
  return `{${entries.join(",")}}`;
}

const FLOW_STATE_DEFAULTS = Object.freeze({
  normal: { call_mode: "normal", digit_capture_active: false },
  capture_pending: { call_mode: "dtmf_capture", digit_capture_active: true },
  capture_active: { call_mode: "dtmf_capture", digit_capture_active: true },
  payment_active: { call_mode: "payment_capture", digit_capture_active: false },
  ending: { call_mode: "normal", digit_capture_active: false },
});

function normalizeFlowStateKey(value) {
  const raw = String(value || "")
    .trim()
    .toLowerCase();
  if (!raw) return "normal";
  return raw.replace(/\s+/g, "_");
}

function getFlowStateDefaults(flowState) {
  const key = normalizeFlowStateKey(flowState);
  return FLOW_STATE_DEFAULTS[key] || null;
}

function normalizeBooleanFlag(value, fallback = false) {
  if (value === undefined || value === null || value === "") return fallback;
  if (value === true || value === 1) return true;
  const normalized = String(value).trim().toLowerCase();
  return ["1", "true", "yes", "on"].includes(normalized);
}

function sanitizePaymentFeatureConfig(raw = {}, previous = {}) {
  const base = {
    ...defaultPaymentFeatureConfig,
    ...(previous && typeof previous === "object" ? previous : {}),
  };
  const next = { ...base };

  if (raw.enabled !== undefined) {
    next.enabled = normalizeBooleanFlag(raw.enabled, base.enabled);
  }
  if (raw.kill_switch !== undefined || raw.killSwitch !== undefined) {
    next.kill_switch = normalizeBooleanFlag(
      raw.kill_switch ?? raw.killSwitch,
      base.kill_switch,
    );
  }
  if (raw.allow_twilio !== undefined || raw.allowTwilio !== undefined) {
    next.allow_twilio = normalizeBooleanFlag(
      raw.allow_twilio ?? raw.allowTwilio,
      base.allow_twilio,
    );
  }
  if (
    raw.require_script_opt_in !== undefined ||
    raw.requireScriptOptIn !== undefined
  ) {
    next.require_script_opt_in = normalizeBooleanFlag(
      raw.require_script_opt_in ?? raw.requireScriptOptIn,
      base.require_script_opt_in,
    );
  }
  if (raw.default_currency !== undefined || raw.defaultCurrency !== undefined) {
    const candidate = String(
      raw.default_currency ?? raw.defaultCurrency ?? base.default_currency,
    )
      .trim()
      .toUpperCase();
    if (/^[A-Z]{3}$/.test(candidate)) {
      next.default_currency = candidate;
    }
  }
  if (raw.min_amount !== undefined || raw.minAmount !== undefined) {
    const parsed = Number(raw.min_amount ?? raw.minAmount);
    next.min_amount = Number.isFinite(parsed) && parsed > 0 ? parsed : 0;
  }
  if (raw.max_amount !== undefined || raw.maxAmount !== undefined) {
    const parsed = Number(raw.max_amount ?? raw.maxAmount);
    next.max_amount = Number.isFinite(parsed) && parsed > 0 ? parsed : 0;
  }
  if (
    raw.max_attempts_per_call !== undefined ||
    raw.maxAttemptsPerCall !== undefined
  ) {
    const parsed = Number(raw.max_attempts_per_call ?? raw.maxAttemptsPerCall);
    next.max_attempts_per_call =
      Number.isFinite(parsed) && parsed > 0
        ? Math.max(1, Math.floor(parsed))
        : 3;
  }
  if (
    raw.retry_cooldown_ms !== undefined ||
    raw.retryCooldownMs !== undefined
  ) {
    const parsed = Number(raw.retry_cooldown_ms ?? raw.retryCooldownMs);
    next.retry_cooldown_ms =
      Number.isFinite(parsed) && parsed >= 0
        ? Math.max(0, Math.floor(parsed))
        : 20000;
  }
  if (
    next.min_amount > 0 &&
    next.max_amount > 0 &&
    next.min_amount > next.max_amount
  ) {
    const swap = next.min_amount;
    next.min_amount = next.max_amount;
    next.max_amount = swap;
  }
  if (
    raw.webhook_idempotency_ttl_ms !== undefined ||
    raw.webhookIdempotencyTtlMs !== undefined
  ) {
    const parsed = Number(
      raw.webhook_idempotency_ttl_ms ?? raw.webhookIdempotencyTtlMs,
    );
    next.webhook_idempotency_ttl_ms =
      Number.isFinite(parsed) && parsed > 0 ? Math.round(parsed) : 300000;
  }
  return next;
}

function getPaymentFeatureConfig() {
  return sanitizePaymentFeatureConfig(paymentFeatureConfig, {});
}

function isPaymentFeatureEnabledForProvider(provider, options = {}) {
  const cfg = getPaymentFeatureConfig();
  if (cfg.enabled !== true) return false;
  if (cfg.kill_switch === true) return false;
  const normalizedProvider = String(provider || "")
    .trim()
    .toLowerCase();
  if (normalizedProvider === "twilio") {
    if (cfg.allow_twilio !== true) return false;
  } else {
    return false;
  }
  if (
    cfg.require_script_opt_in === true &&
    normalizeBooleanFlag(options.hasScript, false) !== true
  ) {
    return false;
  }
  return true;
}

async function loadPaymentFeatureConfig() {
  paymentFeatureConfig = sanitizePaymentFeatureConfig(paymentFeatureConfig, {});
  if (!db?.getSetting) return paymentFeatureConfig;
  try {
    const raw = await db.getSetting(PAYMENT_FEATURE_SETTING_KEY);
    if (!raw) return paymentFeatureConfig;
    const parsed = JSON.parse(raw);
    paymentFeatureConfig = sanitizePaymentFeatureConfig(parsed, paymentFeatureConfig);
  } catch (error) {
    console.error("Failed to load payment feature config:", error);
  }
  return paymentFeatureConfig;
}

async function persistPaymentFeatureConfig() {
  if (!db?.setSetting) return;
  try {
    await db.setSetting(
      PAYMENT_FEATURE_SETTING_KEY,
      JSON.stringify(getPaymentFeatureConfig()),
    );
  } catch (error) {
    console.error("Failed to persist payment feature config:", error);
  }
}

function normalizePaymentSettings(input = {}, options = {}) {
  const errors = [];
  const warnings = [];
  const featureConfig = getPaymentFeatureConfig();
  const normalizeMessage = (value) => {
    const text = String(value || "").trim();
    return text ? text.slice(0, 240) : null;
  };
  const defaultCurrency = String(
    options.defaultCurrency || featureConfig.default_currency || "USD",
  )
    .trim()
    .toUpperCase();
  const requireConnectorWhenEnabled = options.requireConnectorWhenEnabled === true;
  const hasScript = normalizeBooleanFlag(options.hasScript, false);
  const enforceFeatureGate = options.enforceFeatureGate !== false;
  const provider = String(
    options.provider || input?.provider || currentProvider || "",
  )
    .trim()
    .toLowerCase();

  const normalizedConnector = String(input?.payment_connector || "")
    .trim()
    .slice(0, 120);
  const hasAmountInput =
    input?.payment_amount !== undefined &&
    input?.payment_amount !== null &&
    String(input.payment_amount).trim() !== "";
  const parsedAmount = Number(input?.payment_amount);
  const normalizedAmount = hasAmountInput
    ? Number.isFinite(parsedAmount) && parsedAmount > 0
      ? parsedAmount.toFixed(2)
      : null
    : null;
  if (hasAmountInput && !normalizedAmount) {
    errors.push("payment_amount must be a positive number when provided.");
  }

  const hasCurrencyInput =
    input?.payment_currency !== undefined &&
    input?.payment_currency !== null &&
    String(input.payment_currency).trim() !== "";
  const normalizedCurrencyInput = String(input?.payment_currency || "")
    .trim()
    .toUpperCase();
  if (hasCurrencyInput && !/^[A-Z]{3}$/.test(normalizedCurrencyInput)) {
    errors.push("payment_currency must be a 3-letter currency code.");
  }

  let normalizedEnabled = normalizeBooleanFlag(input?.payment_enabled, false);
  const normalizedCurrency = hasCurrencyInput
    ? normalizedCurrencyInput
    : normalizedEnabled
      ? defaultCurrency
      : null;

  if (normalizedEnabled && provider && provider !== "twilio") {
    normalizedEnabled = false;
    warnings.push(
      `Payment defaults were saved as disabled because active provider is ${provider.toUpperCase()} (Twilio required).`,
    );
  }
  if (
    normalizedEnabled &&
    enforceFeatureGate &&
    !isPaymentFeatureEnabledForProvider(provider, { hasScript })
  ) {
    normalizedEnabled = false;
    warnings.push("Payment was disabled by runtime feature controls.");
  }
  if (normalizedEnabled && requireConnectorWhenEnabled && !normalizedConnector) {
    errors.push("payment_connector is required when payment_enabled is true.");
  }
  if (normalizedAmount) {
    const amountNumber = Number(normalizedAmount);
    if (
      featureConfig.min_amount > 0 &&
      Number.isFinite(amountNumber) &&
      amountNumber < featureConfig.min_amount
    ) {
      errors.push(
        `payment_amount must be at least ${featureConfig.min_amount.toFixed(2)}.`,
      );
    }
    if (
      featureConfig.max_amount > 0 &&
      Number.isFinite(amountNumber) &&
      amountNumber > featureConfig.max_amount
    ) {
      errors.push(
        `payment_amount must be at most ${featureConfig.max_amount.toFixed(2)}.`,
      );
    }
  }

  return {
    normalized: {
      payment_enabled: normalizedEnabled,
      payment_connector: normalizedConnector || null,
      payment_amount: normalizedAmount,
      payment_currency: normalizedCurrency,
      payment_description: String(input?.payment_description || "")
        .trim()
        .slice(0, 240) || null,
      payment_start_message: normalizeMessage(
        input?.payment_start_message ?? input?.paymentStartMessage,
      ),
      payment_success_message: normalizeMessage(
        input?.payment_success_message ?? input?.paymentSuccessMessage,
      ),
      payment_failure_message: normalizeMessage(
        input?.payment_failure_message ?? input?.paymentFailureMessage,
      ),
      payment_retry_message: normalizeMessage(
        input?.payment_retry_message ?? input?.paymentRetryMessage,
      ),
    },
    errors,
    warnings,
  };
}

function normalizePaymentPolicy(input = {}, options = {}) {
  const errors = [];
  const warnings = [];
  const source = input && typeof input === "object" ? input : {};
  const normalized = {};

  const parseNumberRange = (value, { field, min, max, integer = true }) => {
    if (value === undefined || value === null || String(value).trim() === "") {
      return null;
    }
    const parsed = Number(value);
    if (!Number.isFinite(parsed)) {
      errors.push(`${field} must be a number.`);
      return null;
    }
    const candidate = integer ? Math.floor(parsed) : parsed;
    if (candidate < min || candidate > max) {
      errors.push(`${field} must be between ${min} and ${max}.`);
      return null;
    }
    return candidate;
  };

  const maxAttemptsPerCall = parseNumberRange(source.max_attempts_per_call, {
    field: "payment_policy.max_attempts_per_call",
    min: 1,
    max: 10,
  });
  if (maxAttemptsPerCall !== null) {
    normalized.max_attempts_per_call = maxAttemptsPerCall;
  }

  const retryCooldownMs = parseNumberRange(source.retry_cooldown_ms, {
    field: "payment_policy.retry_cooldown_ms",
    min: 0,
    max: 900000,
  });
  if (retryCooldownMs !== null) {
    normalized.retry_cooldown_ms = retryCooldownMs;
  }

  const minInteractions = parseNumberRange(
    source.min_interactions_before_payment,
    {
      field: "payment_policy.min_interactions_before_payment",
      min: 0,
      max: 25,
    },
  );
  if (minInteractions !== null) {
    normalized.min_interactions_before_payment = minInteractions;
  }

  const startHour = parseNumberRange(source.allowed_start_hour_utc, {
    field: "payment_policy.allowed_start_hour_utc",
    min: 0,
    max: 23,
  });
  if (startHour !== null) {
    normalized.allowed_start_hour_utc = startHour;
  }

  const endHour = parseNumberRange(source.allowed_end_hour_utc, {
    field: "payment_policy.allowed_end_hour_utc",
    min: 0,
    max: 23,
  });
  if (endHour !== null) {
    normalized.allowed_end_hour_utc = endHour;
  }
  if (
    startHour !== null &&
    endHour !== null &&
    startHour === endHour
  ) {
    warnings.push(
      "payment_policy.allowed_start_hour_utc equals allowed_end_hour_utc; payment will be allowed 24 hours.",
    );
  }

  if (source.sms_fallback_on_failure !== undefined) {
    normalized.sms_fallback_on_failure = normalizeBooleanFlag(
      source.sms_fallback_on_failure,
      true,
    );
  }
  if (source.sms_fallback_on_timeout !== undefined) {
    normalized.sms_fallback_on_timeout = normalizeBooleanFlag(
      source.sms_fallback_on_timeout,
      true,
    );
  }
  if (source.sms_fallback_message !== undefined) {
    const text = String(source.sms_fallback_message || "").trim();
    normalized.sms_fallback_message = text ? text.slice(0, 240) : null;
  }

  if (source.trigger_mode !== undefined) {
    const mode = String(source.trigger_mode || "")
      .trim()
      .toLowerCase();
    if (["manual", "assisted", "auto"].includes(mode)) {
      normalized.trigger_mode = mode;
    } else if (mode) {
      errors.push(
        "payment_policy.trigger_mode must be one of manual, assisted, or auto.",
      );
    }
  }

  return {
    normalized,
    errors,
    warnings,
  };
}

function parsePaymentPolicy(value) {
  if (!value) return null;
  if (typeof value === "object" && !Array.isArray(value)) {
    return { ...value };
  }
  try {
    const parsed = JSON.parse(String(value));
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      return null;
    }
    return parsed;
  } catch (_) {
    return null;
  }
}

function normalizeScriptTemplateRecord(template = null) {
  if (!template || typeof template !== "object") return template;
  const normalized = { ...template };
  const parsedVersion = Number(normalized.version);
  normalized.version =
    Number.isFinite(parsedVersion) && parsedVersion > 0
      ? Math.max(1, Math.floor(parsedVersion))
      : 1;
  normalized.payment_policy = parsePaymentPolicy(normalized.payment_policy);
  return normalized;
}

const SCRIPT_BOUND_PAYMENT_OPTION_FIELDS = Object.freeze([
  "payment_connector",
  "payment_amount",
  "payment_description",
  "payment_start_message",
  "payment_success_message",
  "payment_failure_message",
  "payment_retry_message",
]);

const SCRIPT_BOUND_PAYMENT_POLICY_FIELDS = Object.freeze(["payment_policy"]);

function hasScriptBoundPaymentOverride(input = {}) {
  const payload = input && typeof input === "object" ? input : {};
  if (normalizeBooleanFlag(payload.payment_enabled, false)) {
    return true;
  }
  return SCRIPT_BOUND_PAYMENT_OPTION_FIELDS.some((field) => {
    if (!Object.prototype.hasOwnProperty.call(payload, field)) {
      return false;
    }
    const value = payload[field];
    if (value === undefined || value === null) {
      return false;
    }
    if (typeof value === "string") {
      return value.trim() !== "";
    }
    return true;
  });
}

function hasScriptBoundPaymentPolicyOverride(input = {}) {
  const payload = input && typeof input === "object" ? input : {};
  return SCRIPT_BOUND_PAYMENT_POLICY_FIELDS.some((field) => {
    if (!Object.prototype.hasOwnProperty.call(payload, field)) {
      return false;
    }
    const value = payload[field];
    if (value === undefined || value === null) {
      return false;
    }
    if (typeof value === "string") {
      return value.trim() !== "";
    }
    if (typeof value === "object") {
      return !Array.isArray(value) && Object.keys(value).length > 0;
    }
    return true;
  });
}

function assertScriptBoundPayment(payload = {}, scriptId = null) {
  if (normalizeScriptId(scriptId)) {
    return;
  }
  if (!hasScriptBoundPaymentOverride(payload)) {
    return;
  }
  const error = new Error("Payment settings require a valid script_id.");
  error.code = "payment_requires_script";
  error.status = 400;
  throw error;
}

function assertScriptBoundPaymentPolicy(payload = {}, scriptId = null) {
  if (normalizeScriptId(scriptId)) {
    return;
  }
  if (!hasScriptBoundPaymentPolicyOverride(payload)) {
    return;
  }
  const error = new Error("Payment policy requires a valid script_id.");
  error.code = "payment_policy_requires_script";
  error.status = 400;
  throw error;
}

function applyTemplateTokens(template = "", values = {}) {
  let rendered = String(template || "");
  Object.entries(values || {}).forEach(([key, value]) => {
    const safeValue = value === undefined || value === null ? "" : String(value);
    rendered = rendered.replace(
      new RegExp(`\\{${String(key).replace(/[.*+?^${}()|[\\]\\\\]/g, "\\$&")}\\}`, "g"),
      safeValue,
    );
  });
  return rendered;
}

function buildPaymentSmsFallbackLink(
  callSid,
  session = {},
  callConfig = {},
  options = {},
) {
  const fallback = config.payment?.smsFallback || {};
  if (fallback.enabled !== true) return null;
  const template = String(fallback.urlTemplate || "").trim();
  if (!template) return null;
  const secret = String(fallback.secret || "").trim();
  if (!secret) return null;

  const ttlSeconds = Number.isFinite(Number(fallback.ttlSeconds))
    ? Math.max(60, Math.floor(Number(fallback.ttlSeconds)))
    : 900;
  const expiresAtMs = Date.now() + ttlSeconds * 1000;
  const expiresAtIso = new Date(expiresAtMs).toISOString();
  const tokenPayload = {
    call_sid: callSid || null,
    payment_id: session?.payment_id || null,
    amount: session?.amount || null,
    currency: session?.currency || null,
    reason: options.reason || null,
    exp: Math.floor(expiresAtMs / 1000),
  };
  const token = Buffer.from(stableStringify(tokenPayload)).toString("base64url");
  const signature = crypto
    .createHmac("sha256", secret)
    .update(token)
    .digest("hex")
    .slice(0, 32);

  const values = {
    call_sid: callSid || "",
    payment_id: session?.payment_id || "",
    amount: session?.amount || "",
    currency: session?.currency || "",
    reason: options.reason || "",
    token,
    signature,
    expires_at: expiresAtIso,
    script_id: callConfig?.script_id || "",
    script_version: callConfig?.script_version || "",
  };
  const rendered = applyTemplateTokens(template, values).trim();
  if (!rendered) return null;

  try {
    const url = new URL(rendered);
    if (!url.searchParams.has("token")) {
      url.searchParams.set("token", token);
    }
    if (!url.searchParams.has("sig")) {
      url.searchParams.set("sig", signature);
    }
    if (!url.searchParams.has("exp")) {
      url.searchParams.set("exp", String(Math.floor(expiresAtMs / 1000)));
    }
    if (!url.searchParams.has("call_sid") && callSid) {
      url.searchParams.set("call_sid", String(callSid));
    }
    if (!url.searchParams.has("payment_id") && session?.payment_id) {
      url.searchParams.set("payment_id", String(session.payment_id));
    }
    return {
      url: url.toString(),
      token,
      signature,
      expires_at: expiresAtIso,
    };
  } catch (_) {
    return null;
  }
}

function buildPaymentSmsFallbackMessage(context = {}) {
  const fallback = config.payment?.smsFallback || {};
  const template = String(
    fallback.messageTemplate || "Complete your payment securely here: {payment_url}",
  ).trim();
  return applyTemplateTokens(template, {
    payment_url: context.payment_url || "",
    amount: context.amount || "",
    currency: context.currency || "",
    payment_id: context.payment_id || "",
  })
    .trim()
    .slice(0, 240);
}

function buildCallCapabilities(callConfig = {}, options = {}) {
  const provider = String(
    options.provider || callConfig?.provider || currentProvider || "",
  )
    .trim()
    .toLowerCase();
  const existing =
    callConfig?.capabilities && typeof callConfig.capabilities === "object"
      ? callConfig.capabilities
      : {};

  const capture =
    existing.capture !== undefined ? existing.capture === true : true;
  const transfer =
    existing.transfer !== undefined ? existing.transfer === true : true;
  const paymentConfigured =
    isPaymentFeatureEnabledForProvider(provider, {
      hasScript:
        options.hasScript !== undefined
          ? options.hasScript
          : Boolean(callConfig?.script_id),
    }) &&
    normalizeBooleanFlag(callConfig?.payment_enabled, false);
  const payment =
    existing.payment !== undefined
      ? existing.payment === true && paymentConfigured
      : paymentConfigured;

  return {
    capture,
    transfer,
    payment,
    provider: provider || null,
  };
}

function buildProviderEventFingerprint(source, dedupePayload = {}) {
  const payload =
    dedupePayload && typeof dedupePayload === "object"
      ? dedupePayload
      : { value: dedupePayload };
  const sourceKey = String(source || "unknown");
  const hash = crypto
    .createHash("sha1")
    .update(stableStringify(payload))
    .digest("hex");
  return {
    source: sourceKey,
    hash,
    key: `${sourceKey}:${hash}`,
  };
}

function buildRuntimeSnapshotPayload(callSid, patch = {}) {
  if (!callSid) return null;
  const callConfig = callConfigurations.get(callSid) || {};
  const session = activeCalls.get(callSid);
  const interactionCount = Number(
    patch.interaction_count ??
      patch.interactionCount ??
      session?.interactionCount ??
      0,
  );
  const payload = {
    call_sid: callSid,
    provider: patch.provider || callConfig.provider || currentProvider || null,
    interaction_count: Number.isFinite(interactionCount)
      ? Math.max(0, Math.floor(interactionCount))
      : 0,
    flow_state:
      patch.flow_state ||
      patch.flowState ||
      callConfig.flow_state ||
      "normal",
    call_mode: patch.call_mode || patch.callMode || callConfig.call_mode || "normal",
    digit_capture_active:
      patch.digit_capture_active !== undefined
        ? Boolean(patch.digit_capture_active)
        : patch.digitCaptureActive !== undefined
          ? Boolean(patch.digitCaptureActive)
          : callConfig.digit_capture_active === true,
  };
  const baseSnapshot =
    patch.snapshot && typeof patch.snapshot === "object"
      ? patch.snapshot
      : {};
  payload.snapshot = {
    flow_state_reason: callConfig.flow_state_reason || null,
    digit_intent_mode: callConfig?.digit_intent?.mode || null,
    tool_in_progress: callConfig?.tool_in_progress || null,
    updated_at: new Date().toISOString(),
    ...baseSnapshot,
  };
  return payload;
}

function queuePersistCallRuntimeState(callSid, patch = {}) {
  if (!callSid || !db?.upsertCallRuntimeState) return;
  const merged = {
    ...(callRuntimePendingWrites.get(callSid) || {}),
    ...(patch && typeof patch === "object" ? patch : {}),
  };
  callRuntimePendingWrites.set(callSid, merged);
  if (callRuntimePersistTimers.has(callSid)) return;
  const timer = setTimeout(async () => {
    callRuntimePersistTimers.delete(callSid);
    const pending = callRuntimePendingWrites.get(callSid) || {};
    callRuntimePendingWrites.delete(callSid);
    const payload = buildRuntimeSnapshotPayload(callSid, pending);
    if (!payload) return;
    try {
      await db.upsertCallRuntimeState(payload);
    } catch (error) {
      console.error("Failed to persist call runtime state:", error);
    }
  }, CALL_RUNTIME_PERSIST_DEBOUNCE_MS);
  callRuntimePersistTimers.set(callSid, timer);
}

async function clearCallRuntimeState(callSid) {
  if (!callSid) return;
  const timer = callRuntimePersistTimers.get(callSid);
  if (timer) {
    clearTimeout(timer);
    callRuntimePersistTimers.delete(callSid);
  }
  callRuntimePendingWrites.delete(callSid);
  if (!db?.deleteCallRuntimeState) return;
  try {
    await db.deleteCallRuntimeState(callSid);
  } catch (_) {
    // ignore cleanup failures
  }
}

async function restoreCallRuntimeState(callSid, callConfig = null) {
  if (!callSid || !db?.getCallRuntimeState) {
    return { restored: false, interactionCount: 0 };
  }
  try {
    const row = await db.getCallRuntimeState(callSid);
    if (!row) return { restored: false, interactionCount: 0 };
    const updatedAt = row.updated_at ? Date.parse(row.updated_at) : NaN;
    if (
      Number.isFinite(updatedAt) &&
      Date.now() - updatedAt > CALL_RUNTIME_STATE_STALE_MS
    ) {
      db.deleteCallRuntimeState?.(callSid).catch(() => {});
      return { restored: false, interactionCount: 0, stale: true };
    }
    const targetConfig = callConfig || callConfigurations.get(callSid);
    if (targetConfig) {
      const shouldRestoreCapture =
        targetConfig.flow_state === "normal" &&
        String(row.flow_state || "").startsWith("capture_");
      const nextState = shouldRestoreCapture
        ? row.flow_state
        : targetConfig.flow_state || row.flow_state || "normal";
      setCallFlowState(
        callSid,
        {
          flow_state: nextState,
          reason: "runtime_restore",
          call_mode: row.call_mode || targetConfig.call_mode,
          digit_capture_active: Number(row.digit_capture_active) === 1,
        },
        { callConfig: targetConfig, skipToolRefresh: true, skipPersist: true },
      );
    }
    const restoredInteraction = Number(row.interaction_count);
    const snapshot = safeJsonParse(row.snapshot, {}) || {};
    return {
      restored: true,
      interactionCount: Number.isFinite(restoredInteraction)
        ? Math.max(0, Math.floor(restoredInteraction))
        : 0,
      snapshot,
      row,
    };
  } catch (error) {
    console.error("Failed to restore call runtime state:", error);
    return { restored: false, interactionCount: 0 };
  }
}

function setCallFlowState(callSid, stateUpdate = {}, options = {}) {
  if (!callSid) return null;
  const callConfig = options.callConfig || callConfigurations.get(callSid);
  if (!callConfig) return null;

  const flowState = normalizeFlowStateKey(
    stateUpdate.flowState || stateUpdate.flow_state || callConfig.flow_state,
  );
  const defaults = getFlowStateDefaults(flowState);
  const explicitMode = stateUpdate.callMode ?? stateUpdate.call_mode;
  const explicitCaptureActive =
    stateUpdate.digitCaptureActive ?? stateUpdate.digit_capture_active;

  const nextCallMode =
    explicitMode ||
    defaults?.call_mode ||
    callConfig.call_mode ||
    "normal";
  const nextDigitCaptureActive =
    explicitCaptureActive !== undefined
      ? Boolean(explicitCaptureActive)
      : defaults
        ? defaults.digit_capture_active === true
        : callConfig.digit_capture_active === true;
  const nextReason =
    stateUpdate.reason ??
    stateUpdate.flow_state_reason ??
    callConfig.flow_state_reason ??
    null;
  const nextUpdatedAt =
    stateUpdate.updatedAt ||
    stateUpdate.flow_state_updated_at ||
    new Date().toISOString();
  const changed =
    callConfig.flow_state !== flowState ||
    callConfig.call_mode !== nextCallMode ||
    callConfig.digit_capture_active !== nextDigitCaptureActive ||
    callConfig.flow_state_reason !== nextReason;

  callConfig.flow_state = flowState;
  callConfig.call_mode = nextCallMode;
  callConfig.digit_capture_active = nextDigitCaptureActive;
  callConfig.flow_state_reason = nextReason;
  callConfig.flow_state_updated_at = nextUpdatedAt;
  callConfigurations.set(callSid, callConfig);

  if (changed && options.skipPersist !== true) {
    queuePersistCallRuntimeState(callSid, {
      flow_state: flowState,
      flow_state_reason: nextReason,
      call_mode: nextCallMode,
      digit_capture_active: nextDigitCaptureActive,
      snapshot: {
        source: options.source || "setCallFlowState",
      },
    });
  }
  if (changed && options.skipToolRefresh !== true) {
    refreshActiveCallTools(callSid);
  }
  return callConfig;
}

function purgeStreamStatusDedupe(callSid) {
  if (!callSid) return;
  const prefix = `${callSid}:`;
  for (const key of streamStatusDedupe.keys()) {
    if (key.startsWith(prefix)) {
      streamStatusDedupe.delete(key);
    }
  }
}

function pruneDedupeMap(map, maxSize) {
  if (map.size <= maxSize) return;
  const entries = [...map.entries()].sort((a, b) => a[1] - b[1]);
  const overflow = entries.length - maxSize;
  for (let i = 0; i < overflow; i += 1) {
    map.delete(entries[i][0]);
  }
}

function buildCallStatusDedupeKey(payload = {}) {
  const callSid = payload.CallSid || payload.callSid || "unknown";
  const status = normalizeCallStatus(
    payload.CallStatus || payload.callStatus || "unknown",
  );
  const sequence =
    payload.SequenceNumber ||
    payload.sequenceNumber ||
    payload.Sequence ||
    payload.sequence ||
    "";
  const timestamp =
    payload.Timestamp ||
    payload.timestamp ||
    payload.EventTimestamp ||
    payload.eventTimestamp ||
    "";
  return `${callSid}:${status}:${sequence}:${timestamp}`;
}

function shouldProcessCallStatusPayload(payload = {}, options = {}) {
  if (options.skipDedupe) return true;
  const callSid = payload.CallSid || payload.callSid;
  if (!callSid) return true;
  const key = buildCallStatusDedupeKey(payload);
  const now = Date.now();
  const lastSeen = callStatusDedupe.get(key);
  if (lastSeen && now - lastSeen < CALL_STATUS_DEDUPE_MS) {
    return false;
  }
  callStatusDedupe.set(key, now);
  pruneDedupeMap(callStatusDedupe, CALL_STATUS_DEDUPE_MAX);
  return true;
}

function purgeCallStatusDedupe(callSid) {
  if (!callSid) return;
  const prefix = `${callSid}:`;
  for (const key of callStatusDedupe.keys()) {
    if (key.startsWith(prefix)) {
      callStatusDedupe.delete(key);
    }
  }
}

function shouldProcessProviderEvent(source, dedupePayload = {}, options = {}) {
  const fingerprint = buildProviderEventFingerprint(source, dedupePayload);
  const key = fingerprint.key;
  const now = Date.now();
  const ttlMs =
    Number.isFinite(Number(options.ttlMs)) && Number(options.ttlMs) > 0
      ? Number(options.ttlMs)
      : PROVIDER_EVENT_DEDUPE_MS;
  const lastSeen = providerEventDedupe.get(key);
  if (lastSeen && now - lastSeen < ttlMs) {
    return false;
  }
  providerEventDedupe.set(key, now);
  pruneDedupeMap(providerEventDedupe, PROVIDER_EVENT_DEDUPE_MAX);
  return true;
}

async function shouldProcessProviderEventAsync(
  source,
  dedupePayload = {},
  options = {},
) {
  if (!shouldProcessProviderEvent(source, dedupePayload, options)) {
    return false;
  }
  if (!db?.reserveProviderEventIdempotency) {
    return true;
  }
  const ttlMs =
    Number.isFinite(Number(options.ttlMs)) && Number(options.ttlMs) > 0
      ? Number(options.ttlMs)
      : PROVIDER_EVENT_DEDUPE_MS;
  const fingerprint = buildProviderEventFingerprint(source, dedupePayload);
  try {
    const reserved = await db.reserveProviderEventIdempotency({
      source: fingerprint.source,
      payload_hash: fingerprint.hash,
      event_key: fingerprint.key,
      ttl_ms: ttlMs,
    });
    return reserved?.reserved !== false;
  } catch (error) {
    console.error("Provider event idempotency persistence failed:", error);
    return true;
  }
}

function recordCallLifecycle(callSid, status, meta = {}) {
  if (!callSid || !status) return false;
  const normalized = normalizeCallStatus(status);
  const prev = callLifecycle.get(callSid)?.status;
  if (prev === normalized) return false;
  const updatedAt = new Date().toISOString();
  callLifecycle.set(callSid, { status: normalized, updatedAt });
  db?.updateCallState?.(callSid, `status_${normalized}`, {
    status: normalized,
    prev_status: prev || null,
    source: meta.source || null,
    raw_status: meta.raw_status || meta.rawStatus || null,
    answered_by: meta.answered_by || meta.answeredBy || null,
    duration: meta.duration || null,
    at: updatedAt,
  }).catch(() => {});
  return true;
}

function scheduleCallLifecycleCleanup(callSid, delayMs = 10 * 60 * 1000) {
  if (!callSid) return;
  if (callLifecycleCleanupTimers.has(callSid)) {
    clearTimeout(callLifecycleCleanupTimers.get(callSid));
  }
  const timer = setTimeout(() => {
    callLifecycleCleanupTimers.delete(callSid);
    purgeCallStatusDedupe(callSid);
    callLifecycle.delete(callSid);
  }, delayMs);
  callLifecycleCleanupTimers.set(callSid, timer);
}

function normalizeBodyForSignature(req) {
  const method = String(req.method || "GET").toUpperCase();
  if (["GET", "HEAD"].includes(method)) {
    return "";
  }
  const contentLength = Number(req.headers["content-length"] || 0);
  const hasBody = Number.isFinite(contentLength) && contentLength > 0;
  if (!req.body || Object.keys(req.body).length === 0) {
    return hasBody ? stableStringify(req.body || {}) : "";
  }
  return stableStringify(req.body);
}

function buildHmacPayload(req, timestamp) {
  const method = String(req.method || "GET").toUpperCase();
  const path = req.originalUrl || req.url || "/";
  const body = normalizeBodyForSignature(req);
  return `${timestamp}.${method}.${path}.${body}`;
}

function normalizePhoneDigits(value) {
  return String(value || "").replace(/\D/g, "");
}

function normalizePhoneForFlag(value) {
  const digits = normalizePhoneDigits(value);
  if (!digits) return null;
  return `+${digits}`;
}

function getInboundRateKey(req, payload = {}) {
  const from =
    payload.From || payload.from || payload.Caller || payload.caller || null;
  const normalized = normalizePhoneForFlag(from);
  if (normalized) return normalized;
  return req?.ip || req?.headers?.["x-forwarded-for"] || "unknown";
}

function shouldRateLimitInbound(req, payload = {}) {
  const max = Number(config.inbound?.rateLimitMax) || 0;
  const windowMs = Number(config.inbound?.rateLimitWindowMs) || 60000;
  if (!Number.isFinite(max) || max <= 0) {
    return { limited: false, key: null };
  }
  const key = getInboundRateKey(req, payload);
  const now = Date.now();
  const bucket = inboundRateBuckets.get(key);
  if (!bucket || now - bucket.windowStart >= windowMs) {
    inboundRateBuckets.set(key, { count: 1, windowStart: now });
    return { limited: false, key };
  }
  bucket.count += 1;
  inboundRateBuckets.set(key, bucket);
  return {
    limited: bucket.count > max,
    key,
    count: bucket.count,
    resetAt: bucket.windowStart + windowMs,
  };
}

function normalizeTwilioDirection(value) {
  return String(value || "")
    .trim()
    .toLowerCase();
}

function isOutboundTwilioDirection(value) {
  const direction = normalizeTwilioDirection(value);
  return direction ? direction.startsWith("outbound") : false;
}

function resolveInboundRoute(toNumber) {
  const routes = config.inbound?.routes || {};
  if (!toNumber || !routes || typeof routes !== "object") return null;
  const normalizedTo = normalizePhoneDigits(toNumber);
  if (!normalizedTo) return routes[toNumber] || null;

  if (routes[toNumber]) return routes[toNumber];
  if (routes[normalizedTo]) return routes[normalizedTo];
  if (routes[`+${normalizedTo}`]) return routes[`+${normalizedTo}`];

  for (const [key, value] of Object.entries(routes)) {
    if (normalizePhoneDigits(key) === normalizedTo) {
      return value;
    }
  }
  return null;
}

function buildInboundDefaults(route = {}) {
  const fallbackPrompt =
    config.inbound?.defaultPrompt || DEFAULT_INBOUND_PROMPT;
  const fallbackFirst =
    config.inbound?.defaultFirstMessage || DEFAULT_INBOUND_FIRST_MESSAGE;
  const prompt = route.prompt || inboundDefaultScript?.prompt || fallbackPrompt;
  const firstMessage =
    route.first_message ||
    route.firstMessage ||
    inboundDefaultScript?.first_message ||
    fallbackFirst;
  return { prompt, firstMessage };
}

function buildInboundCallConfig(callSid, payload = {}, options = {}) {
  const provider = String(options.provider || "twilio").toLowerCase();
  const inbound = options.inbound !== false;
  const route =
    resolveInboundRoute(
      payload.To || payload.to || payload.called || payload.Called,
    ) || {};
  const routeLabel = route.label || route.name || route.route_label || null;
  const { prompt, firstMessage } = buildInboundDefaults(route);
  const functionSystem = functionEngine.generateAdaptiveFunctionSystem(
    prompt,
    firstMessage,
  );
  const createdAt = new Date().toISOString();
  const hasRoutePrompt = Boolean(
    route.prompt || route.first_message || route.firstMessage,
  );
  const fallbackScript = !hasRoutePrompt ? inboundDefaultScript : null;
  const routePaymentPolicy = parsePaymentPolicy(route.payment_policy);
  const effectivePaymentPolicy =
    routePaymentPolicy || fallbackScript?.payment_policy || null;
  const inboundPayment = normalizePaymentSettings(route, {
    provider,
    requireConnectorWhenEnabled: false,
    hasScript: Boolean(route.script_id || fallbackScript?.id),
    enforceFeatureGate: true,
  }).normalized;
  const callConfig = {
    prompt,
    first_message: firstMessage,
    created_at: createdAt,
    user_chat_id: config.telegram?.adminChatId || route.user_chat_id || null,
    customer_name: route.customer_name || null,
    provider,
    provider_metadata: null,
    business_context: route.business_context || functionSystem.context,
    function_count: functionSystem.functions.length,
    purpose: route.purpose || null,
    business_id: route.business_id || null,
    route_label: routeLabel,
    script: route.script || fallbackScript?.name || null,
    script_id: route.script_id || fallbackScript?.id || null,
    script_version:
      normalizeScriptId(route.script_id || fallbackScript?.id) &&
      Number.isFinite(Number(route.script_version || fallbackScript?.version))
        ? Math.max(1, Math.floor(Number(route.script_version || fallbackScript?.version)))
        : null,
    emotion: route.emotion || null,
    urgency: route.urgency || null,
    technical_level: route.technical_level || null,
    voice_model: route.voice_model || null,
    collection_profile: route.collection_profile || null,
    collection_expected_length: route.collection_expected_length || null,
    collection_timeout_s: route.collection_timeout_s || null,
    collection_max_retries: route.collection_max_retries || null,
    collection_mask_for_gpt: route.collection_mask_for_gpt,
    collection_speak_confirmation: route.collection_speak_confirmation,
    payment_enabled: inboundPayment.payment_enabled === true,
    payment_connector: inboundPayment.payment_connector || null,
    payment_amount: inboundPayment.payment_amount || null,
    payment_currency: inboundPayment.payment_currency || null,
    payment_description: inboundPayment.payment_description || null,
    payment_start_message: inboundPayment.payment_start_message || null,
    payment_success_message: inboundPayment.payment_success_message || null,
    payment_failure_message: inboundPayment.payment_failure_message || null,
    payment_retry_message: inboundPayment.payment_retry_message || null,
    payment_policy: effectivePaymentPolicy,
    payment_state: inboundPayment.payment_enabled === true ? "ready" : "disabled",
    payment_state_updated_at: createdAt,
    payment_session: null,
    payment_last_result: null,
    firstMediaTimeoutMs:
      route.first_media_timeout_ms ||
      route.firstMediaTimeoutMs ||
      config.inbound?.firstMediaTimeoutMs ||
      null,
    flow_state: "normal",
    flow_state_updated_at: createdAt,
    call_mode: "normal",
    digit_capture_active: false,
    inbound,
  };
  callConfig.capabilities = buildCallCapabilities(callConfig, { provider });
  return { callConfig, functionSystem };
}

async function refreshInboundDefaultScript(force = false) {
  if (!db) return null;
  const now = Date.now();
  if (
    !force &&
    inboundDefaultLoadedAt &&
    now - inboundDefaultLoadedAt < INBOUND_DEFAULT_CACHE_MS
  ) {
    return inboundDefaultScript;
  }
  inboundDefaultLoadedAt = now;

  let settingValue = null;
  try {
    settingValue = await db.getSetting(INBOUND_DEFAULT_SETTING_KEY);
  } catch (error) {
    console.error("Failed to load inbound default setting:", error);
  }

  if (!settingValue || settingValue === "builtin") {
    inboundDefaultScriptId = null;
    inboundDefaultScript = null;
    return inboundDefaultScript;
  }

  const scriptId = Number(settingValue);
  if (!Number.isFinite(scriptId)) {
    inboundDefaultScriptId = null;
    inboundDefaultScript = null;
    return inboundDefaultScript;
  }

  try {
    const script = normalizeScriptTemplateRecord(
      await db.getCallTemplateById(scriptId),
    );
    if (!script) {
      inboundDefaultScriptId = null;
      inboundDefaultScript = null;
      return inboundDefaultScript;
    }
    inboundDefaultScriptId = scriptId;
    inboundDefaultScript = script;
  } catch (error) {
    console.error("Failed to load inbound default script:", error);
    inboundDefaultScriptId = null;
    inboundDefaultScript = null;
  }
  return inboundDefaultScript;
}

function ensureCallSetup(callSid, payload = {}, options = {}) {
  let callConfig = callConfigurations.get(callSid);
  let functionSystem = callFunctionSystems.get(callSid);
  if (callConfig && functionSystem) {
    return { callConfig, functionSystem, created: false };
  }

  if (!callConfig) {
    const created = buildInboundCallConfig(callSid, payload, options);
    callConfig = created.callConfig;
    functionSystem = functionSystem || created.functionSystem;
  } else if (!functionSystem) {
    const { prompt, first_message } = callConfig;
    const promptValue = prompt || DEFAULT_INBOUND_PROMPT;
    const firstValue = first_message || DEFAULT_INBOUND_FIRST_MESSAGE;
    functionSystem = functionEngine.generateAdaptiveFunctionSystem(
      promptValue,
      firstValue,
    );
  }

  callConfigurations.set(callSid, callConfig);
  callFunctionSystems.set(callSid, functionSystem);
  setCallFlowState(
    callSid,
    {
      flow_state: callConfig.flow_state || "normal",
      reason: callConfig.flow_state_reason || "setup",
      call_mode: callConfig.call_mode || "normal",
      digit_capture_active: callConfig.digit_capture_active === true,
      flow_state_updated_at:
        callConfig.flow_state_updated_at || new Date().toISOString(),
    },
    { callConfig, skipToolRefresh: true, source: "ensureCallSetup" },
  );
  queuePersistCallRuntimeState(callSid, {
    snapshot: { source: "ensureCallSetup" },
  });
  return { callConfig, functionSystem, created: true };
}

async function ensureCallRecord(
  callSid,
  payload = {},
  source = "unknown",
  setupOptions = {},
) {
  if (!db || !callSid) return null;
  const setup = ensureCallSetup(callSid, payload, setupOptions);
  const existing = await db.getCall(callSid).catch(() => null);
  if (existing) return existing;

  const { callConfig, functionSystem } = setup;
  const inbound = callConfig?.inbound !== false;
  const direction = inbound ? "inbound" : "outbound";
  const from =
    payload.From || payload.from || payload.Caller || payload.caller || null;
  const to =
    payload.To || payload.to || payload.Called || payload.called || null;

  try {
    await db.createCall({
      call_sid: callSid,
      phone_number: from || null,
      prompt: callConfig.prompt,
      first_message: callConfig.first_message,
      user_chat_id: callConfig.user_chat_id || null,
      business_context: JSON.stringify(functionSystem?.context || {}),
      generated_functions: JSON.stringify(
        (functionSystem?.functions || [])
          .map((f) => f.function?.name || f.function?.function?.name || f.name)
          .filter(Boolean),
      ),
      direction,
    });
    await db.updateCallState(callSid, "call_created", {
      inbound,
      source,
      from: from || null,
      to: to || null,
      provider: callConfig.provider || "twilio",
      provider_metadata: callConfig.provider_metadata || null,
      business_id: callConfig.business_id || null,
      route_label: callConfig.route_label || null,
      script: callConfig.script || null,
      script_id: callConfig.script_id || null,
      script_version: callConfig.script_version || null,
      purpose: callConfig.purpose || null,
      voice_model: callConfig.voice_model || null,
      flow_state: callConfig.flow_state || "normal",
      flow_state_reason: callConfig.flow_state_reason || "call_created",
      flow_state_updated_at:
        callConfig.flow_state_updated_at || new Date().toISOString(),
      call_mode: callConfig.call_mode || "normal",
      digit_capture_active: callConfig.digit_capture_active === true,
      capabilities: callConfig.capabilities || buildCallCapabilities(callConfig),
      payment_enabled: callConfig.payment_enabled === true,
      payment_connector: callConfig.payment_connector || null,
      payment_amount: callConfig.payment_amount || null,
      payment_currency: callConfig.payment_currency || null,
      payment_description: callConfig.payment_description || null,
      payment_start_message: callConfig.payment_start_message || null,
      payment_success_message: callConfig.payment_success_message || null,
      payment_failure_message: callConfig.payment_failure_message || null,
      payment_retry_message: callConfig.payment_retry_message || null,
      payment_policy: callConfig.payment_policy || null,
      payment_state: callConfig.payment_state || (callConfig.payment_enabled === true ? "ready" : "disabled"),
      payment_state_updated_at:
        callConfig.payment_state_updated_at || new Date().toISOString(),
    });
    return await db.getCall(callSid);
  } catch (error) {
    console.error("Failed to create inbound call record:", error);
    return null;
  }
}

async function hydrateCallConfigFromDb(callSid) {
  if (!db || !callSid) return null;
  const call = await db.getCall(callSid).catch(() => null);
  if (!call) return null;
  let state = null;
  try {
    state = await db.getLatestCallState(callSid, "call_created");
  } catch (_) {
    state = null;
  }
  let parsedContext = null;
  if (call?.business_context) {
    try {
      parsedContext = JSON.parse(call.business_context);
    } catch (_) {
      parsedContext = null;
    }
  }
  const prompt = call.prompt || DEFAULT_INBOUND_PROMPT;
  const firstMessage = call.first_message || DEFAULT_INBOUND_FIRST_MESSAGE;
  const functionSystem = functionEngine.generateAdaptiveFunctionSystem(
    prompt,
    firstMessage,
  );
  const createdAt = call.created_at || new Date().toISOString();
  const callConfig = {
    prompt,
    first_message: firstMessage,
    created_at: createdAt,
    user_chat_id: call.user_chat_id || null,
    customer_name: state?.customer_name || state?.victim_name || null,
    provider: state?.provider || currentProvider,
    provider_metadata: state?.provider_metadata || null,
    business_context:
      state?.business_context || parsedContext || functionSystem.context,
    function_count: functionSystem.functions.length,
    purpose: state?.purpose || null,
    business_id: state?.business_id || null,
    script: state?.script || null,
    script_id: state?.script_id || null,
    script_version: Number.isFinite(Number(state?.script_version))
      ? Math.max(1, Math.floor(Number(state.script_version)))
      : null,
    emotion: state?.emotion || null,
    urgency: state?.urgency || null,
    technical_level: state?.technical_level || null,
    voice_model: state?.voice_model || null,
    collection_profile: state?.collection_profile || null,
    collection_expected_length: state?.collection_expected_length || null,
    collection_timeout_s: state?.collection_timeout_s || null,
    collection_max_retries: state?.collection_max_retries || null,
    collection_mask_for_gpt: state?.collection_mask_for_gpt,
    collection_speak_confirmation: state?.collection_speak_confirmation,
    payment_enabled: normalizeBooleanFlag(state?.payment_enabled, false),
    payment_connector: state?.payment_connector || null,
    payment_amount: state?.payment_amount || null,
    payment_currency: state?.payment_currency || null,
    payment_description: state?.payment_description || null,
    payment_start_message: state?.payment_start_message || null,
    payment_success_message: state?.payment_success_message || null,
    payment_failure_message: state?.payment_failure_message || null,
    payment_retry_message: state?.payment_retry_message || null,
    payment_policy: parsePaymentPolicy(state?.payment_policy),
    payment_state:
      state?.payment_state ||
      (normalizeBooleanFlag(state?.payment_enabled, false) ? "ready" : "disabled"),
    payment_state_updated_at: state?.payment_state_updated_at || createdAt,
    payment_session: null,
    payment_last_result: state?.payment_last_result || null,
    capabilities:
      state?.capabilities && typeof state.capabilities === "object"
        ? state.capabilities
        : null,
    script_policy: state?.script_policy || null,
    flow_state: state?.flow_state || "normal",
    flow_state_updated_at: state?.flow_state_updated_at || createdAt,
    call_mode: state?.call_mode || "normal",
    digit_capture_active:
      state?.digit_capture_active === true ||
      state?.digit_capture_active === 1 ||
      state?.flow_state === "capture_active" ||
      state?.flow_state === "capture_pending",
    inbound: false,
  };
  if (!callConfig.capabilities) {
    callConfig.capabilities = buildCallCapabilities(callConfig);
  }

  callConfigurations.set(callSid, callConfig);
  callFunctionSystems.set(callSid, functionSystem);
  setCallFlowState(
    callSid,
    {
      flow_state: callConfig.flow_state || "normal",
      reason: callConfig.flow_state_reason || "hydrated",
      call_mode: callConfig.call_mode || "normal",
      digit_capture_active: callConfig.digit_capture_active === true,
      flow_state_updated_at:
        callConfig.flow_state_updated_at || new Date().toISOString(),
    },
    { callConfig, skipToolRefresh: true, skipPersist: true, source: "hydrate" },
  );
  return { callConfig, functionSystem };
}

function buildStreamAuthToken(callSid, timestamp) {
  const secret = config.streamAuth?.secret;
  if (!secret) return null;
  const payload = `${callSid}.${timestamp}`;
  return crypto.createHmac("sha256", secret).update(payload).digest("hex");
}

function resolveStreamAuthParams(req, extraParams = null) {
  const result = {};
  if (req?.query && Object.keys(req.query).length) {
    Object.assign(result, req.query);
  } else {
    const url = req?.url || "";
    const queryIndex = url.indexOf("?");
    if (queryIndex !== -1) {
      const params = new URLSearchParams(url.slice(queryIndex + 1));
      for (const [key, value] of params.entries()) {
        result[key] = value;
      }
    }
  }
  if (extraParams && typeof extraParams === "object") {
    for (const [key, value] of Object.entries(extraParams)) {
      if (value === undefined || value === null || value === "") continue;
      result[key] = String(value);
    }
  }
  return result;
}

function verifyStreamAuth(callSid, req, extraParams = null) {
  const secret = config.streamAuth?.secret;
  if (!secret) return { ok: true, skipped: true, reason: "missing_secret" };
  const params = resolveStreamAuthParams(req, extraParams);
  const token = params.token || params.signature;
  const timestamp = Number(params.ts || params.timestamp);
  if (!token || !Number.isFinite(timestamp)) {
    return { ok: false, reason: "missing_token" };
  }
  const maxSkewMs = Number(config.streamAuth?.maxSkewMs || 300000);
  const now = Date.now();
  if (Math.abs(now - timestamp) > maxSkewMs) {
    return { ok: false, reason: "timestamp_out_of_range" };
  }
  const expected = buildStreamAuthToken(callSid, String(timestamp));
  if (!expected) return { ok: false, reason: "missing_secret" };
  try {
    const expectedBuf = Buffer.from(expected, "hex");
    const providedBuf = Buffer.from(String(token), "hex");
    if (expectedBuf.length !== providedBuf.length) {
      return { ok: false, reason: "invalid_signature" };
    }
    if (!crypto.timingSafeEqual(expectedBuf, providedBuf)) {
      return { ok: false, reason: "invalid_signature" };
    }
  } catch (error) {
    return { ok: false, reason: "invalid_signature" };
  }
  return { ok: true };
}

function clearCallEndLock(callSid) {
  if (callEndLocks.has(callSid)) {
    callEndLocks.delete(callSid);
  }
}

function clearSilenceTimer(callSid) {
  const timer = silenceTimers.get(callSid);
  if (timer) {
    clearTimeout(timer);
    silenceTimers.delete(callSid);
  }
}

function isCaptureActiveConfig(callConfig) {
  if (!callConfig) return false;
  const flowState = callConfig.flow_state;
  if (flowState === "capture_active" || flowState === "capture_pending") {
    return true;
  }
  if (callConfig.call_mode === "dtmf_capture") {
    return true;
  }
  return (
    callConfig?.digit_intent?.mode === "dtmf" &&
    callConfig?.digit_capture_active === true
  );
}

function isCaptureActive(callSid) {
  if (!callSid) return false;
  const callConfig = callConfigurations.get(callSid);
  return isCaptureActiveConfig(callConfig);
}

function resolveVoiceModel(callConfig) {
  const model = callConfig?.voice_model;
  if (model && typeof model === "string" && model.trim()) {
    return model.trim();
  }
  return null;
}

function resolveTwilioSayVoice(callConfig) {
  const model = resolveVoiceModel(callConfig);
  if (!model) return null;
  const normalized = model.toLowerCase();
  if (["alice", "man", "woman"].includes(normalized)) {
    return model;
  }
  if (model.startsWith("Polly.")) {
    return model;
  }
  return null;
}

function resolveDeepgramVoiceModel(callConfig) {
  if (callConfig && typeof callConfig === "object") {
    const lockedModel = String(callConfig.deepgram_voice_model_locked || "").trim();
    if (lockedModel) {
      return lockedModel;
    }
  }

  const candidateModel = callConfig?.voice_model;
  let resolvedModel = config.deepgram?.voiceModel || "aura-asteria-en";
  if (candidateModel && typeof candidateModel === "string") {
    const normalized = candidateModel.toLowerCase();
    if (
      !["alice", "man", "woman"].includes(normalized) &&
      !candidateModel.startsWith("Polly.")
    ) {
      resolvedModel = candidateModel;
    }
  }

  if (callConfig && typeof callConfig === "object") {
    callConfig.deepgram_voice_model_locked = resolvedModel;
  }
  return resolvedModel;
}

function shouldUseTwilioPlay(callConfig) {
  if (!config.deepgram?.apiKey) return false;
  if (!config.server?.hostname) return false;
  if (config.twilio?.ttsPlayEnabled === false) return false;
  return true;
}

function normalizeTwilioTtsText(text = "") {
  const cleaned = String(text || "").trim();
  if (!cleaned) return "";
  if (cleaned.length > TWILIO_TTS_MAX_CHARS) {
    return "";
  }
  return cleaned;
}

function buildTwilioTtsCacheKey(text, voiceModel) {
  return crypto
    .createHash("sha256")
    .update(`${voiceModel}::${text}`)
    .digest("hex");
}

function pruneTwilioTtsCache() {
  const now = Date.now();
  for (const [key, entry] of twilioTtsCache.entries()) {
    if (!entry || entry.expiresAt <= now) {
      twilioTtsCache.delete(key);
    }
  }
  if (twilioTtsCache.size <= TWILIO_TTS_CACHE_MAX) return;
  const entries = Array.from(twilioTtsCache.entries()).sort(
    (a, b) => (a[1]?.createdAt || 0) - (b[1]?.createdAt || 0),
  );
  const overflow = twilioTtsCache.size - TWILIO_TTS_CACHE_MAX;
  for (let i = 0; i < overflow; i += 1) {
    const entry = entries[i];
    if (entry) {
      twilioTtsCache.delete(entry[0]);
    }
  }
}

async function synthesizeTwilioTtsAudio(text, voiceModel) {
  const model = voiceModel || resolveDeepgramVoiceModel(null);
  const url = `https://api.deepgram.com/v1/speak?model=${encodeURIComponent(model)}&encoding=mulaw&sample_rate=8000&container=none`;
  const response = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Token ${config.deepgram.apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ text }),
    timeout: TWILIO_TTS_FETCH_TIMEOUT_MS,
  });
  if (!response.ok) {
    const errorText = await response.text();
    console.error(
      "Deepgram TTS error:",
      response.status,
      response.statusText,
      errorText,
    );
    return null;
  }
  const arrayBuffer = await response.arrayBuffer();
  const mulawBuffer = Buffer.from(arrayBuffer);
  const wav = new WaveFile();
  wav.fromScratch(1, 8000, "8m", mulawBuffer);
  return {
    buffer: Buffer.from(wav.toBuffer()),
    contentType: "audio/wav",
  };
}

async function getTwilioTtsAudioUrl(text, callConfig, options = {}) {
  const cleaned = normalizeTwilioTtsText(text);
  if (!cleaned) return null;
  const cacheOnly = options?.cacheOnly === true;
  const forceGenerate = options?.forceGenerate === true;
  if (!forceGenerate && !shouldUseTwilioPlay(callConfig)) return null;
  const voiceModel = resolveDeepgramVoiceModel(callConfig);
  const key = buildTwilioTtsCacheKey(cleaned, voiceModel);
  const now = Date.now();
  const cached = twilioTtsCache.get(key);
  if (cached && cached.expiresAt > now) {
    return `https://${config.server.hostname}/webhook/twilio-tts?key=${encodeURIComponent(key)}`;
  }
  const pending = twilioTtsPending.get(key);
  if (pending) {
    if (cacheOnly) {
      return null;
    }
    await pending;
    const refreshed = twilioTtsCache.get(key);
    if (refreshed && refreshed.expiresAt > Date.now()) {
      return `https://${config.server.hostname}/webhook/twilio-tts?key=${encodeURIComponent(key)}`;
    }
    return null;
  }
  if (cacheOnly) {
    const job = (async () => {
      try {
        const audio = await synthesizeTwilioTtsAudio(cleaned, voiceModel);
        if (!audio) return;
        twilioTtsCache.set(key, {
          ...audio,
          createdAt: Date.now(),
          expiresAt: Date.now() + TWILIO_TTS_CACHE_TTL_MS,
        });
        pruneTwilioTtsCache();
      } catch (err) {
        console.error("Twilio TTS synthesis error:", err);
      }
    })();
    twilioTtsPending.set(key, job);
    job.finally(() => {
      if (twilioTtsPending.get(key) === job) {
        twilioTtsPending.delete(key);
      }
    });
    return null;
  }
  const job = (async () => {
    try {
      const audio = await synthesizeTwilioTtsAudio(cleaned, voiceModel);
      if (!audio) return;
      twilioTtsCache.set(key, {
        ...audio,
        createdAt: Date.now(),
        expiresAt: Date.now() + TWILIO_TTS_CACHE_TTL_MS,
      });
      pruneTwilioTtsCache();
    } catch (err) {
      console.error("Twilio TTS synthesis error:", err);
    }
  })();
  twilioTtsPending.set(key, job);
  await job;
  twilioTtsPending.delete(key);
  const refreshed = twilioTtsCache.get(key);
  if (refreshed && refreshed.expiresAt > Date.now()) {
    return `https://${config.server.hostname}/webhook/twilio-tts?key=${encodeURIComponent(key)}`;
  }
  return null;
}

async function getTwilioTtsAudioUrlSafe(
  text,
  callConfig,
  timeoutMs = 1200,
  options = {},
) {
  const safeTimeoutMs =
    Number.isFinite(timeoutMs) && timeoutMs > 0 ? timeoutMs : 0;
  if (!safeTimeoutMs) {
    return getTwilioTtsAudioUrl(text, callConfig, options);
  }
  const timeoutPromise = new Promise((resolve) => {
    setTimeout(() => resolve(null), safeTimeoutMs);
  });
  try {
    return await Promise.race([
      getTwilioTtsAudioUrl(text, callConfig, options),
      timeoutPromise,
    ]);
  } catch (error) {
    console.error("Twilio TTS timeout fallback:", error);
    return null;
  }
}

function maskDigitsForLog(input = "") {
  const digits = String(input || "").replace(/\D/g, "");
  if (!digits) return "0 digits";
  return `${digits.length} digits`;
}

function maskPhoneForLog(input = "") {
  const digits = String(input || "").replace(/\D/g, "");
  if (!digits) return "unknown";
  const tail = digits.slice(-4);
  return `***${tail}`;
}

function maskSmsBodyForLog(body = "") {
  const text = String(body || "").replace(/\s+/g, " ").trim();
  if (!text) return "[empty len=0]";
  const digest = crypto
    .createHash("sha256")
    .update(text)
    .digest("hex")
    .slice(0, 12);
  return `[len=${text.length} sha=${digest}]`;
}

function redactSensitiveLogValue(input = "") {
  let text = String(input || "");
  if (!text) return "unknown";
  text = text.replace(/\+?\d[\d\s().-]{6,}\d/g, (match) => {
    const digits = String(match || "").replace(/\D/g, "");
    if (digits.length < 4) return "[redacted-phone]";
    return `***${digits.slice(-2)}`;
  });
  text = text.replace(
    /[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi,
    "[redacted-email]",
  );
  if (text.length > 220) {
    return `${text.slice(0, 200)}...[redacted]`;
  }
  return text;
}

function normalizeRequestId(value = "") {
  const candidate = String(value || "").trim();
  if (!candidate) return null;
  if (candidate.length > 80) return null;
  if (!/^[A-Za-z0-9._:-]+$/.test(candidate)) return null;
  return candidate;
}

function buildApiError(code, message, requestId = null, extra = {}) {
  return {
    success: false,
    error: message,
    code,
    ...(requestId ? { request_id: requestId } : {}),
    ...(extra || {}),
  };
}

function sendApiError(res, status, code, message, requestId = null, extra = {}) {
  return res.status(status).json(buildApiError(code, message, requestId, extra));
}

function parseBoundedInteger(value, options = {}) {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed)) {
    return Number.isFinite(options.defaultValue) ? options.defaultValue : null;
  }
  const min = Number.isFinite(options.min) ? options.min : null;
  const max = Number.isFinite(options.max) ? options.max : null;
  if (min !== null && parsed < min) {
    return Number.isFinite(options.defaultValue) ? options.defaultValue : min;
  }
  if (max !== null && parsed > max) {
    return max;
  }
  return parsed;
}

function parsePagination(query = {}, options = {}) {
  const defaultLimit = Number.isFinite(options.defaultLimit)
    ? options.defaultLimit
    : 10;
  const maxLimit = Number.isFinite(options.maxLimit) ? options.maxLimit : 50;
  const limit = parseBoundedInteger(query.limit, {
    defaultValue: defaultLimit,
    min: 1,
    max: maxLimit,
  });
  const offset = parseBoundedInteger(query.offset, {
    defaultValue: 0,
    min: 0,
  });
  return { limit, offset };
}

function isSafeId(value, options = {}) {
  const candidate = String(value || "").trim();
  if (!candidate) return false;
  const max = Number.isFinite(options.max) ? options.max : 128;
  if (candidate.length > max) return false;
  return /^[A-Za-z0-9._:-]+$/.test(candidate);
}

function buildErrorDetails(error) {
  return redactSensitiveLogValue(error?.message || String(error || "unknown"));
}

function getOutboundActorKey(req, explicitValue = null) {
  const explicit = String(explicitValue || "").trim();
  if (explicit) return explicit;
  const bodyUser = String(req?.body?.user_chat_id || req?.body?.userChatId || "").trim();
  if (bodyUser) return bodyUser;
  const tenant = String(req?.body?.tenant_id || req?.body?.tenantId || "").trim();
  if (tenant) return `tenant:${tenant}`;
  return String(req?.ip || req?.headers?.["x-forwarded-for"] || "anonymous");
}

async function checkOutboundRateLimit(scope, key, limit, windowMs) {
  const safeLimit = Number(limit);
  const safeWindowMs = Number(windowMs);
  if (!Number.isFinite(safeLimit) || safeLimit <= 0) {
    return { allowed: true };
  }
  if (!Number.isFinite(safeWindowMs) || safeWindowMs <= 0) {
    return { allowed: true };
  }

  if (db?.checkAndConsumeOutboundRateLimit) {
    try {
      return await db.checkAndConsumeOutboundRateLimit({
        scope,
        key,
        limit: safeLimit,
        windowMs: safeWindowMs,
        nowMs: Date.now(),
      });
    } catch (error) {
      console.error("outbound_rate_limit_store_error", {
        scope: String(scope || "unknown"),
        key: redactSensitiveLogValue(String(key || "anonymous")),
        reason: redactSensitiveLogValue(error?.message || "unknown"),
      });
    }
  }

  const now = Date.now();
  if (outboundRateBuckets.size > 5000) {
    for (const [entryKey, entry] of outboundRateBuckets.entries()) {
      if (!entry || now - entry.windowStart >= safeWindowMs * 2) {
        outboundRateBuckets.delete(entryKey);
      }
    }
  }
  const bucketKey = `${scope}:${key}`;
  const existing = outboundRateBuckets.get(bucketKey);
  if (!existing || now - existing.windowStart >= safeWindowMs) {
    outboundRateBuckets.set(bucketKey, {
      count: 1,
      windowStart: now,
    });
    return { allowed: true };
  }
  if (existing.count >= safeLimit) {
    const retryAfterMs = Math.max(0, safeWindowMs - (now - existing.windowStart));
    return { allowed: false, retryAfterMs };
  }
  existing.count += 1;
  outboundRateBuckets.set(bucketKey, existing);
  return { allowed: true };
}

async function enforceOutboundRateLimits(req, res, options = {}) {
  const requestId = req.requestId || null;
  const actorKey = getOutboundActorKey(req, options.actorKey);
  const windowMs = Number(options.windowMs) || Number(config.outboundLimits?.windowMs) || 60000;
  const perUserLimit = Number(options.perUserLimit);
  const globalLimit = Number(options.globalLimit);
  const namespace = options.namespace || "outbound";

  const perUserCheck = await checkOutboundRateLimit(
    `${namespace}:user`,
    actorKey,
    perUserLimit,
    windowMs,
  );
  if (!perUserCheck.allowed) {
    res.setHeader("retry-after", Math.ceil(perUserCheck.retryAfterMs / 1000));
    return sendApiError(
      res,
      429,
      `${namespace}_per_user_rate_limited`,
      "Too many requests for this user. Please retry shortly.",
      requestId,
      { retry_after_ms: perUserCheck.retryAfterMs },
    );
  }

  const globalCheck = await checkOutboundRateLimit(
    `${namespace}:global`,
    "all",
    globalLimit,
    windowMs,
  );
  if (!globalCheck.allowed) {
    res.setHeader("retry-after", Math.ceil(globalCheck.retryAfterMs / 1000));
    return sendApiError(
      res,
      429,
      `${namespace}_global_rate_limited`,
      "Service is temporarily rate limited. Please retry shortly.",
      requestId,
      { retry_after_ms: globalCheck.retryAfterMs },
    );
  }

  return null;
}

function setDbForTests(nextDb = null) {
  db = nextDb;
}

function queuePendingDigitAction(callSid, action = {}) {
  if (!callSid) return false;
  const callConfig = callConfigurations.get(callSid);
  if (!callConfig) return false;
  if (!Array.isArray(callConfig.pending_digit_actions)) {
    callConfig.pending_digit_actions = [];
  }
  callConfig.pending_digit_actions.push({
    type: action.type,
    text: action.text || "",
    reason: action.reason || null,
    scheduleTimeout: action.scheduleTimeout === true,
  });
  callConfigurations.set(callSid, callConfig);
  return true;
}

function popPendingDigitActions(callSid) {
  const callConfig = callConfigurations.get(callSid);
  if (
    !callConfig ||
    !Array.isArray(callConfig.pending_digit_actions) ||
    !callConfig.pending_digit_actions.length
  ) {
    return [];
  }
  const actions = callConfig.pending_digit_actions.slice(0);
  callConfig.pending_digit_actions = [];
  callConfigurations.set(callSid, callConfig);
  return actions;
}

function clearPendingDigitReprompts(callSid) {
  const callConfig = callConfigurations.get(callSid);
  if (
    !callConfig ||
    !Array.isArray(callConfig.pending_digit_actions) ||
    !callConfig.pending_digit_actions.length
  ) {
    return;
  }
  callConfig.pending_digit_actions = callConfig.pending_digit_actions.filter(
    (action) => action?.type !== "reprompt",
  );
  callConfigurations.set(callSid, callConfig);
}

async function handlePendingDigitActions(
  callSid,
  actions = [],
  gptService,
  interactionCount = 0,
) {
  if (!callSid || !actions.length) return false;
  for (const action of actions) {
    if (!action) continue;
    if (action.type === "end") {
      const reason = action.reason || "digits_collected";
      const message = action.text || CLOSING_MESSAGE;
      await speakAndEndCall(callSid, message, reason);
      return true;
    }
    if (action.type === "reprompt" && gptService && action.text) {
      const personalityInfo =
        gptService?.personalityEngine?.getCurrentPersonality?.();
      gptService.emit(
        "gptreply",
        {
          partialResponseIndex: null,
          partialResponse: action.text,
          personalityInfo,
          adaptationHistory: gptService?.personalityChanges?.slice(-3) || [],
        },
        interactionCount,
      );
      if (digitService) {
        digitService.markDigitPrompted(
          callSid,
          gptService,
          interactionCount,
          "dtmf",
          {
            allowCallEnd: true,
            prompt_text: action.text,
            reset_buffer: true,
          },
        );
        if (action.scheduleTimeout) {
          digitService.scheduleDigitTimeout(
            callSid,
            gptService,
            interactionCount + 1,
          );
        }
      }
    }
  }
  return true;
}

function scheduleSilenceTimer(callSid, timeoutMs = 30000) {
  if (!callSid) return;
  if (callEndLocks.has(callSid)) {
    return;
  }
  if (digitService?.hasExpectation(callSid) || isCaptureActive(callSid)) {
    return;
  }
  clearSilenceTimer(callSid);
  const timer = setTimeout(() => {
    if (!digitService?.hasExpectation(callSid) && !isCaptureActive(callSid)) {
      speakAndEndCall(
        callSid,
        CALL_END_MESSAGES.no_response,
        "silence_timeout",
      );
    }
  }, timeoutMs);
  silenceTimers.set(callSid, timer);
}

function clearFirstMediaWatchdog(callSid) {
  const timer = streamFirstMediaTimers.get(callSid);
  if (timer) {
    clearTimeout(timer);
    streamFirstMediaTimers.delete(callSid);
  }
}

function markStreamMediaSeen(callSid) {
  if (!callSid || streamFirstMediaSeen.has(callSid)) return;
  streamLastMediaAt.set(callSid, Date.now());
  streamFirstMediaSeen.add(callSid);
  clearFirstMediaWatchdog(callSid);
  const startedAt = streamStartTimes.get(callSid);
  if (startedAt) {
    const deltaMs = Math.max(0, Date.now() - startedAt);
    const threshold = Number(config.callSlo?.firstMediaMs);
    const thresholdMs =
      Number.isFinite(threshold) && threshold > 0 ? threshold : null;
    db?.addCallMetric?.(callSid, "first_media_ms", deltaMs, {
      threshold_ms: thresholdMs,
    }).catch(() => {});
    if (thresholdMs && deltaMs > thresholdMs) {
      db?.logServiceHealth?.("call_slo", "degraded", {
        call_sid: callSid,
        metric: "first_media_ms",
        value: deltaMs,
        threshold_ms: thresholdMs,
      }).catch(() => {});
    }
    streamStartTimes.delete(callSid);
  }
  db?.updateCallState?.(callSid, "stream_media", {
    at: new Date().toISOString(),
  }).catch(() => {});
}

function scheduleFirstMediaWatchdog(callSid, host, callConfig) {
  if (!callSid || !callConfig?.inbound) return;
  if (TWILIO_STREAM_TRACK === "inbound_track") {
    return;
  }
  const timeoutMs = Number(
    callConfig.firstMediaTimeoutMs || config.inbound?.firstMediaTimeoutMs,
  );
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) return;
  if (streamFirstMediaSeen.has(callSid)) return;
  clearFirstMediaWatchdog(callSid);
  const timer = setTimeout(async () => {
    streamFirstMediaTimers.delete(callSid);
    if (streamFirstMediaSeen.has(callSid)) return;
    webhookService.addLiveEvent(
      callSid,
      "⚠️ No audio detected. Attempting fallback.",
      { force: true },
    );
    await db
      ?.updateCallState?.(callSid, "stream_no_media", {
        at: new Date().toISOString(),
        timeout_ms: timeoutMs,
      })
      .catch(() => {});
    await handleStreamTimeout(callSid, host, {
      allowHangup: false,
      reason: "no_media",
    });
  }, timeoutMs);
  streamFirstMediaTimers.set(callSid, timer);
}

const STREAM_RETRY_SETTINGS = {
  maxAttempts: 1,
  baseDelayMs: 1500,
  maxDelayMs: 8000,
};

function shouldRetryStream(reason = "") {
  return [
    "no_media",
    "stream_not_connected",
    "stream_auth_failed",
    "watchdog_no_media",
  ].includes(reason);
}

async function scheduleStreamReconnect(callSid, host, reason = "unknown") {
  if (!callSid || !config.twilio?.accountSid || !config.twilio?.authToken)
    return false;
  const state = streamRetryState.get(callSid) || {
    attempts: 0,
    nextDelayMs: STREAM_RETRY_SETTINGS.baseDelayMs,
  };
  if (state.attempts >= STREAM_RETRY_SETTINGS.maxAttempts) {
    return false;
  }
  state.attempts += 1;
  const delayMs = Math.min(state.nextDelayMs, STREAM_RETRY_SETTINGS.maxDelayMs);
  state.nextDelayMs = Math.min(
    state.nextDelayMs * 2,
    STREAM_RETRY_SETTINGS.maxDelayMs,
  );
  streamRetryState.set(callSid, state);
  const jitterMs = Math.floor(Math.random() * 250);

  webhookService.addLiveEvent(
    callSid,
    `🔁 Retrying stream (${state.attempts}/${STREAM_RETRY_SETTINGS.maxAttempts})`,
    { force: true },
  );
  setTimeout(async () => {
    try {
      const twiml = buildTwilioStreamTwiml(host, { callSid });
      const client = twilio(config.twilio.accountSid, config.twilio.authToken);
      await client.calls(callSid).update({ twiml });
      await db
        .updateCallState(callSid, "stream_retry", {
          attempt: state.attempts,
          reason,
          at: new Date().toISOString(),
        })
        .catch(() => {});
    } catch (error) {
      console.error(
        `Stream retry failed for ${callSid}:`,
        error?.message || error,
      );
      await db
        .updateCallState(callSid, "stream_retry_failed", {
          attempt: state.attempts,
          reason,
          at: new Date().toISOString(),
          error: error?.message || String(error),
        })
        .catch(() => {});
    }
  }, delayMs + jitterMs);

  return true;
}

const STREAM_WATCHDOG_INTERVAL_MS = 5000;
const STREAM_STALL_DEFAULTS = {
  noMediaMs: 20000,
  noMediaEscalationMs: 45000,
  sttStallMs: 25000,
  sttEscalationMs: 60000,
};

function resolveStreamConnectedAt(callSid) {
  if (!callSid) return null;
  const startedAt = streamStartTimes.get(callSid);
  if (Number.isFinite(startedAt)) {
    return startedAt;
  }
  const connection = activeStreamConnections.get(callSid);
  if (connection?.connectedAt) {
    const parsed = Date.parse(connection.connectedAt);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return null;
}

function resolveStreamWatchdogThresholds(callConfig) {
  const sloFirstMedia = Number(config.callSlo?.firstMediaMs) || 4000;
  const inboundFirstMedia = Number(
    callConfig?.firstMediaTimeoutMs || config.inbound?.firstMediaTimeoutMs,
  );
  const noMediaMs =
    Number.isFinite(inboundFirstMedia) && inboundFirstMedia > 0
      ? inboundFirstMedia
      : Math.max(STREAM_STALL_DEFAULTS.noMediaMs, sloFirstMedia * 3);
  const noMediaEscalationMs = Math.max(
    STREAM_STALL_DEFAULTS.noMediaEscalationMs,
    noMediaMs * 2,
  );
  const sttStallMs = Math.max(
    STREAM_STALL_DEFAULTS.sttStallMs,
    sloFirstMedia * 6,
  );
  const sttEscalationMs = Math.max(
    STREAM_STALL_DEFAULTS.sttEscalationMs,
    sttStallMs * 2,
  );
  return { noMediaMs, noMediaEscalationMs, sttStallMs, sttEscalationMs };
}

function getStreamWatchdogState(callSid) {
  if (!callSid) return null;
  const state = streamWatchdogState.get(callSid) || {};
  streamWatchdogState.set(callSid, state);
  return state;
}

async function handleStreamStallNotice(callSid, message, stateKey, state) {
  if (!callSid || !state || state[stateKey]) return false;
  state[stateKey] = Date.now();
  webhookService.addLiveEvent(callSid, message, { force: true });
  return true;
}

async function runStreamWatchdog() {
  const host = config.server?.hostname;
  if (!host) return;
  const now = Date.now();

  for (const [callSid, callConfig] of callConfigurations.entries()) {
    if (!callSid || callEndLocks.has(callSid)) continue;
    const state = getStreamWatchdogState(callSid);
    if (!state) continue;
    const connectedAt = resolveStreamConnectedAt(callSid);
    if (!connectedAt) continue;
    const thresholds = resolveStreamWatchdogThresholds(callConfig);
    const noMediaElapsed = now - connectedAt;

    if (
      !streamFirstMediaSeen.has(callSid) &&
      noMediaElapsed > thresholds.noMediaMs
    ) {
      const notified = await handleStreamStallNotice(
        callSid,
        "⚠️ Stream stalled. Attempting recovery…",
        "noMediaNotifiedAt",
        state,
      );
      if (notified) {
        await db
          ?.updateCallState?.(callSid, "stream_stalled", {
            at: new Date().toISOString(),
            phase: "no_media",
            elapsed_ms: noMediaElapsed,
          })
          .catch(() => {});
        void handleStreamTimeout(callSid, host, {
          allowHangup: false,
          reason: "watchdog_no_media",
        });
        continue;
      }
      if (
        !state.noMediaEscalatedAt &&
        noMediaElapsed > thresholds.noMediaEscalationMs
      ) {
        state.noMediaEscalatedAt = now;
        webhookService.addLiveEvent(
          callSid,
          "⚠️ Stream still offline. Ending call.",
          { force: true },
        );
        void handleStreamTimeout(callSid, host, {
          allowHangup: true,
          reason: "watchdog_no_media",
        });
      }
      continue;
    }

    const lastMediaAt = streamLastMediaAt.get(callSid);
    if (!lastMediaAt) continue;
    const sttElapsed = now - (sttLastFrameAt.get(callSid) || lastMediaAt);
    if (sttElapsed > thresholds.sttStallMs) {
      const notified = await handleStreamStallNotice(
        callSid,
        "⚠️ Speech pipeline stalled. Switching to keypad…",
        "sttNotifiedAt",
        state,
      );
      if (notified) {
        await db
          ?.updateCallState?.(callSid, "stt_stalled", {
            at: new Date().toISOString(),
            elapsed_ms: sttElapsed,
          })
          .catch(() => {});
        const session = activeCalls.get(callSid);
        void activateDtmfFallback(
          callSid,
          callConfig,
          session?.gptService,
          session?.interactionCount || 0,
          "stt_stall",
        );
      } else if (
        !state.sttEscalatedAt &&
        sttElapsed > thresholds.sttEscalationMs
      ) {
        state.sttEscalatedAt = now;
        webhookService.addLiveEvent(
          callSid,
          "⚠️ Speech still unavailable. Ending call.",
          { force: true },
        );
        void handleStreamTimeout(callSid, host, {
          allowHangup: true,
          reason: "stt_stall",
        });
      }
    }
  }
}

async function handleStreamTimeout(callSid, host, options = {}) {
  if (!callSid || streamTimeoutCalls.has(callSid)) return;
  const allowHangup = options.allowHangup !== false;
  streamTimeoutCalls.add(callSid);
  let releaseLock = false;
  try {
    const callConfig = callConfigurations.get(callSid);
    const callDetails = await db?.getCall?.(callSid).catch(() => null);
    const statusValue = normalizeCallStatus(
      callDetails?.status || callDetails?.twilio_status,
    );
    const isAnswered =
      Boolean(callDetails?.started_at) ||
      ["answered", "in-progress", "completed"].includes(statusValue);
    if (!isAnswered) {
      console.warn(
        `Skipping stream timeout for ${callSid} (status=${statusValue || "unknown"})`,
      );
      releaseLock = true;
      return;
    }
    const expectation = digitService?.getExpectation?.(callSid);
    if (expectation && config.twilio?.gatherFallback) {
      const prompt =
        expectation.prompt ||
        (digitService?.buildDigitPrompt
          ? digitService.buildDigitPrompt(expectation)
          : "");
      const sent = await digitService.sendTwilioGather(
        callSid,
        expectation,
        { prompt },
        host,
      );
      if (sent) {
        await db
          .updateCallState(callSid, "stream_fallback_gather", {
            at: new Date().toISOString(),
          })
          .catch(() => {});
        return;
      }
    }

    if (
      shouldRetryStream(options.reason) &&
      (await scheduleStreamReconnect(callSid, host, options.reason))
    ) {
      console.warn(
        `Stream retry scheduled for ${callSid} (${options.reason || "unspecified"})`,
      );
      releaseLock = true;
      return;
    }

    if (!allowHangup) {
      console.warn(
        `Stream timeout for ${callSid} resolved without hangup (${options.reason || "unspecified"})`,
      );
      releaseLock = true;
      return;
    }

    if (config.twilio?.accountSid && config.twilio?.authToken) {
      const client = twilio(config.twilio.accountSid, config.twilio.authToken);
      const response = new VoiceResponse();
      response.say(
        "We are having trouble connecting the call. Please try again later.",
      );
      response.hangup();
      await client.calls(callSid).update({ twiml: response.toString() });
    }

    await db
      .updateCallState(callSid, "stream_timeout", {
        at: new Date().toISOString(),
        provider: callConfig?.provider || currentProvider,
      })
      .catch(() => {});
  } catch (error) {
    console.error("Stream timeout handler error:", error);
  } finally {
    if (releaseLock) {
      streamTimeoutCalls.delete(callSid);
    }
  }
}

async function activateDtmfFallback(
  callSid,
  callConfig,
  gptService,
  interactionCount = 0,
  reason = "stt_failure",
) {
  if (!callSid || sttFallbackCalls.has(callSid)) return false;
  if (!digitService) return false;
  const provider = callConfig?.provider || currentProvider;
  if (provider !== "twilio") return false;
  sttFallbackCalls.add(callSid);

  const configToUse = callConfig || callConfigurations.get(callSid);
  if (!configToUse) return false;

  configToUse.digit_intent = { mode: "dtmf", reason, confidence: 1 };
  setCallFlowState(
    callSid,
    {
      flow_state: "capture_pending",
      reason,
      call_mode: "dtmf_capture",
      digit_capture_active: true,
    },
    { callConfig: configToUse, source: "activateDtmfFallback" },
  );

  await db
    .updateCallState(callSid, "stt_fallback", {
      reason,
      at: new Date().toISOString(),
    })
    .catch(() => {});

  try {
    await applyInitialDigitIntent(
      callSid,
      configToUse,
      gptService,
      interactionCount,
    );
  } catch (error) {
    console.error("Failed to apply digit intent during STT fallback:", error);
  }

  const expectation = digitService.getExpectation(callSid);
  if (expectation && config.twilio?.gatherFallback) {
    const prompt =
      expectation.prompt || digitService.buildDigitPrompt(expectation);
    try {
      const sent = await digitService.sendTwilioGather(callSid, expectation, {
        prompt,
      });
      if (sent) {
        webhookService.addLiveEvent(callSid, "📟 Switching to keypad capture", {
          force: true,
        });
        return true;
      }
    } catch (error) {
      console.error("Twilio gather fallback error:", error);
    }
  }

  const fallbackPrompt = expectation
    ? digitService.buildDigitPrompt(expectation)
    : "Enter the digits now.";
  if (gptService) {
    const personalityInfo =
      gptService?.personalityEngine?.getCurrentPersonality?.();
    gptService.emit(
      "gptreply",
      {
        partialResponseIndex: null,
        partialResponse: fallbackPrompt,
        personalityInfo,
        adaptationHistory: gptService?.personalityChanges?.slice(-3) || [],
      },
      interactionCount,
    );
  }
  if (expectation) {
    digitService.markDigitPrompted(
      callSid,
      gptService,
      interactionCount,
      "dtmf",
      {
        allowCallEnd: true,
        prompt_text: fallbackPrompt,
        reset_buffer: true,
      },
    );
    digitService.scheduleDigitTimeout(
      callSid,
      gptService,
      interactionCount + 1,
    );
  }
  return true;
}

function estimateAudioLevelFromBase64(base64 = "") {
  if (!base64) return null;
  let buffer;
  try {
    buffer = Buffer.from(base64, "base64");
  } catch (_) {
    return null;
  }
  if (!buffer.length) return null;
  const step = Math.max(1, Math.floor(buffer.length / 800));
  let sum = 0;
  let count = 0;
  for (let i = 0; i < buffer.length; i += step) {
    sum += Math.abs(buffer[i] - 128);
    count += 1;
  }
  if (!count) return null;
  const level = sum / (count * 128);
  return Math.max(0, Math.min(1, level));
}

function estimateAudioLevelFromBuffer(buffer, options = {}) {
  if (!Buffer.isBuffer(buffer) || !buffer.length) return null;
  const encoding = String(options.encoding || "").toLowerCase();
  if (["pcm", "linear", "linear16", "l16"].includes(encoding)) {
    const minStep = 2;
    let step = Math.max(minStep, Math.floor(buffer.length / 800));
    if (step % 2 !== 0) {
      step += 1;
    }
    let sum = 0;
    let count = 0;
    for (let i = 0; i + 1 < buffer.length; i += step) {
      sum += Math.abs(buffer.readInt16LE(i));
      count += 1;
    }
    if (!count) return null;
    const level = sum / (count * 32768);
    return Math.max(0, Math.min(1, level));
  }
  const step = Math.max(1, Math.floor(buffer.length / 800));
  let sum = 0;
  let count = 0;
  for (let i = 0; i < buffer.length; i += step) {
    sum += Math.abs(buffer[i] - 128);
    count += 1;
  }
  if (!count) return null;
  const level = sum / (count * 128);
  return Math.max(0, Math.min(1, level));
}

function clampLevel(level) {
  if (!Number.isFinite(level)) return null;
  return Math.max(0, Math.min(1, level));
}

function shouldSampleUserAudioLevel(callSid, now = Date.now()) {
  const state = userAudioStates.get(callSid);
  if (!state) return true;
  return now - state.lastTickAt >= liveConsoleAudioTickMs;
}

function updateUserAudioLevel(callSid, level, now = Date.now()) {
  if (!callSid) return;
  const normalized = clampLevel(level);
  if (!Number.isFinite(normalized)) return;
  let state = userAudioStates.get(callSid);
  if (!state) {
    state = { lastTickAt: 0, lastAboveAt: 0, speaking: false };
  }
  if (now - state.lastTickAt < liveConsoleAudioTickMs) {
    return;
  }
  state.lastTickAt = now;
  const currentPhase = webhookService.getLiveConsolePhaseKey?.(callSid);
  if (normalized >= liveConsoleUserLevelThreshold) {
    state.speaking = true;
    state.lastAboveAt = now;
    userAudioStates.set(callSid, state);
    const nextPhase =
      currentPhase === "agent_speaking" || currentPhase === "agent_responding"
        ? "interrupted"
        : "user_speaking";
    webhookService
      .setLiveCallPhase(callSid, nextPhase, {
        level: normalized,
        logEvent: false,
      })
      .catch(() => {});
    return;
  }

  if (state.speaking) {
    if (now - state.lastAboveAt >= liveConsoleUserHoldMs) {
      state.speaking = false;
      userAudioStates.set(callSid, state);
      if (
        currentPhase !== "agent_speaking" &&
        currentPhase !== "agent_responding"
      ) {
        webhookService
          .setLiveCallPhase(callSid, "listening", { level: 0, logEvent: false })
          .catch(() => {});
      }
      return;
    }
    userAudioStates.set(callSid, state);
    if (currentPhase === "user_speaking" || currentPhase === "interrupted") {
      webhookService
        .setLiveCallPhase(callSid, currentPhase, {
          level: normalized,
          logEvent: false,
        })
        .catch(() => {});
    }
  } else {
    userAudioStates.set(callSid, state);
  }
}

function estimateAudioLevelsFromBase64(base64 = "", options = {}) {
  if (!base64)
    return { durationMs: 0, levels: [], intervalMs: options.intervalMs || 160 };
  let buffer;
  try {
    buffer = Buffer.from(base64, "base64");
  } catch (_) {
    return { durationMs: 0, levels: [], intervalMs: options.intervalMs || 160 };
  }
  const length = buffer.length;
  if (!length)
    return { durationMs: 0, levels: [], intervalMs: options.intervalMs || 160 };
  const durationMs = Math.round((length / 8000) * 1000);
  const intervalMs = Math.max(80, Number(options.intervalMs) || 160);
  const maxFrames = Number(options.maxFrames) || 48;
  const frames = Math.min(
    maxFrames,
    Math.max(1, Math.ceil(durationMs / intervalMs)),
  );
  const bytesPerFrame = Math.max(1, Math.floor(length / frames));
  const levels = new Array(frames).fill(0);
  for (let frame = 0; frame < frames; frame += 1) {
    const start = frame * bytesPerFrame;
    const end =
      frame === frames - 1 ? length : Math.min(length, start + bytesPerFrame);
    const span = Math.max(1, end - start);
    const step = Math.max(1, Math.floor(span / 120));
    let sum = 0;
    let count = 0;
    for (let i = start; i < end; i += step) {
      sum += Math.abs(buffer[i] - 128);
      count += 1;
    }
    const level = count ? Math.max(0, Math.min(1, sum / (count * 128))) : 0;
    levels[frame] = level;
  }
  const effectiveInterval = frames
    ? Math.max(80, Math.floor(durationMs / frames))
    : intervalMs;
  return { durationMs, levels, intervalMs: effectiveInterval };
}

function estimateAudioDurationMsFromBase64(base64 = "") {
  if (!base64) return 0;
  let buffer;
  try {
    buffer = Buffer.from(base64, "base64");
  } catch (_) {
    return 0;
  }
  return Math.round((buffer.length / 8000) * 1000);
}

function isGroupedGatherPlan(plan, callConfig = {}) {
  if (!plan) return false;
  const provider = callConfig?.provider || currentProvider;
  return (
    provider === "twilio" &&
    ["banking", "card"].includes(plan.group_id) &&
    plan.capture_mode === "ivr_gather" &&
    isCaptureActiveConfig(callConfig)
  );
}

function startGroupedGather(callSid, callConfig, options = {}) {
  if (!callSid || !digitService?.sendTwilioGather || !digitService?.getPlan)
    return false;
  const plan = digitService.getPlan(callSid);
  if (!isGroupedGatherPlan(plan, callConfig)) return false;
  const expectation = digitService.getExpectation(callSid);
  if (!expectation) return false;
  if (expectation.prompted_at && options.force !== true) return false;
  const prompt = digitService.buildPlanStepPrompt
    ? digitService.buildPlanStepPrompt(expectation)
    : expectation.prompt || digitService.buildDigitPrompt(expectation);
  if (!prompt) return false;
  const sayVoice = resolveTwilioSayVoice(callConfig);
  const sayOptions = sayVoice ? { voice: sayVoice } : null;
  const delayMs = Math.max(
    0,
    Number.isFinite(options.delayMs) ? options.delayMs : 0,
  );
  const preamble = options.preamble || "";
  const gptService = options.gptService || null;
  const interactionCount = Number.isFinite(options.interactionCount)
    ? options.interactionCount
    : 0;
  setTimeout(async () => {
    try {
      const activePlan = digitService.getPlan(callSid);
      const activeExpectation = digitService.getExpectation(callSid);
      if (!activePlan || !activeExpectation) return;
      if (!isGroupedGatherPlan(activePlan, callConfig)) return;
      if (activeExpectation.prompted_at && options.force !== true) return;
      if (
        activeExpectation.plan_id &&
        activePlan.id &&
        activeExpectation.plan_id !== activePlan.id
      )
        return;
      const usePlay = shouldUseTwilioPlay(callConfig);
      const ttsTimeoutMs = Number(config.twilio?.ttsMaxWaitMs) || 1200;
      const preambleUrl = usePlay
        ? await getTwilioTtsAudioUrlSafe(preamble, callConfig, ttsTimeoutMs)
        : null;
      const promptUrl = usePlay
        ? await getTwilioTtsAudioUrlSafe(prompt, callConfig, ttsTimeoutMs)
        : null;
      const sent = await digitService.sendTwilioGather(
        callSid,
        activeExpectation,
        {
          prompt,
          preamble,
          promptUrl,
          preambleUrl,
          sayOptions,
        },
      );
      if (!sent) {
        webhookService.addLiveEvent(
          callSid,
          "⚠️ Gather unavailable; using stream DTMF capture",
          { force: true },
        );
        digitService.markDigitPrompted(
          callSid,
          gptService,
          interactionCount,
          "dtmf",
          {
            allowCallEnd: true,
            prompt_text: [preamble, prompt].filter(Boolean).join(" "),
          },
        );
        if (gptService) {
          const personalityInfo =
            gptService?.personalityEngine?.getCurrentPersonality?.();
          gptService.emit(
            "gptreply",
            {
              partialResponseIndex: null,
              partialResponse: [preamble, prompt].filter(Boolean).join(" "),
              personalityInfo,
              adaptationHistory:
                gptService?.personalityChanges?.slice(-3) || [],
            },
            interactionCount,
          );
        }
        digitService.scheduleDigitTimeout(
          callSid,
          gptService,
          interactionCount,
        );
      }
    } catch (err) {
      console.error("Grouped gather start error:", err);
    }
  }, delayMs);
  return true;
}

function clearSpeechTicks(callSid) {
  const timer = speechTickTimers.get(callSid);
  if (timer) {
    clearInterval(timer);
    speechTickTimers.delete(callSid);
  }
}

function scheduleSpeechTicks(
  callSid,
  phaseKey,
  durationMs,
  level = null,
  options = {},
) {
  if (!callSid) return;
  clearSpeechTicks(callSid);
  const intervalMs = Math.max(80, Number(options.intervalMs) || 200);
  const levels = Array.isArray(options.levels) ? options.levels : null;
  const safeDuration = Math.max(0, Number(durationMs) || 0);
  if (!safeDuration || safeDuration <= intervalMs) {
    webhookService
      .setLiveCallPhase(callSid, phaseKey, { level, logEvent: false })
      .catch(() => {});
    return;
  }
  const start = Date.now();
  webhookService
    .setLiveCallPhase(callSid, phaseKey, { level, logEvent: false })
    .catch(() => {});
  const timer = setInterval(() => {
    const elapsed = Date.now() - start;
    if (elapsed >= safeDuration) {
      clearSpeechTicks(callSid);
      return;
    }
    let nextLevel = level;
    if (levels?.length) {
      const idx = Math.min(
        levels.length - 1,
        Math.floor((elapsed / safeDuration) * levels.length),
      );
      if (Number.isFinite(levels[idx])) {
        nextLevel = levels[idx];
      }
    }
    webhookService
      .setLiveCallPhase(callSid, phaseKey, {
        level: nextLevel,
        logEvent: false,
      })
      .catch(() => {});
  }, intervalMs);
  speechTickTimers.set(callSid, timer);
}

function scheduleSpeechTicksFromAudio(callSid, phaseKey, base64Audio = "") {
  if (!base64Audio) return;
  const { durationMs, levels, intervalMs } = estimateAudioLevelsFromBase64(
    base64Audio,
    { intervalMs: liveConsoleAudioTickMs, maxFrames: 48 },
  );
  const fallbackLevel = estimateAudioLevelFromBase64(base64Audio);
  const startLevel = Number.isFinite(levels?.[0]) ? levels[0] : fallbackLevel;
  scheduleSpeechTicks(callSid, phaseKey, durationMs, startLevel, {
    levels,
    intervalMs,
  });
}

async function applyInitialDigitIntent(
  callSid,
  callConfig,
  gptService = null,
  interactionCount = 0,
) {
  if (!digitService || !callConfig) return null;
  if (callConfig.digit_intent) {
    const existing = {
      intent: callConfig.digit_intent,
      expectation: digitService.getExpectation(callSid) || null,
    };
    if (
      existing.intent?.mode === "dtmf" &&
      callConfig.digit_capture_active !== true
    ) {
      setCallFlowState(
        callSid,
        {
          flow_state: existing.expectation
            ? "capture_active"
            : "capture_pending",
          reason: existing.intent?.reason || "digit_intent",
          call_mode: "dtmf_capture",
          digit_capture_active: true,
        },
        { callConfig, source: "applyInitialDigitIntent_existing" },
      );
    }
    if (existing.intent?.mode === "dtmf" && existing.expectation) {
      try {
        await digitService.flushBufferedDigits(
          callSid,
          gptService,
          interactionCount,
          "dtmf",
          { allowCallEnd: true },
        );
      } catch (err) {
        console.error("Flush buffered digits error:", err);
      }
    }
    return existing;
  }
  const result = digitService.prepareInitialExpectation(callSid, callConfig);
  callConfig.digit_intent = result.intent;
  if (result.intent?.mode === "dtmf") {
    setCallFlowState(
      callSid,
      {
        flow_state: result.expectation ? "capture_active" : "capture_pending",
        reason: result.intent?.reason || "digit_intent",
        call_mode: "dtmf_capture",
        digit_capture_active: true,
      },
      { callConfig, source: "applyInitialDigitIntent_prepare" },
    );
  } else {
    setCallFlowState(
      callSid,
      {
        flow_state: "normal",
        reason: result.intent?.reason || "no_signal",
        call_mode: "normal",
        digit_capture_active: false,
      },
      { callConfig, source: "applyInitialDigitIntent_prepare" },
    );
  }
  callConfigurations.set(callSid, callConfig);
  if (
    result.intent?.mode === "dtmf" &&
    Array.isArray(result.plan_steps) &&
    result.plan_steps.length
  ) {
    webhookService.addLiveEvent(
      callSid,
      formatDigitCaptureLabel(result.intent, result.expectation),
      { force: true },
    );
  } else if (result.intent?.mode === "dtmf" && result.expectation) {
    webhookService.addLiveEvent(
      callSid,
      `🔢 DTMF intent detected (${result.intent.reason})`,
      { force: true },
    );
  } else {
    webhookService.addLiveEvent(
      callSid,
      `🗣️ Normal call flow (${result.intent?.reason || "no_signal"})`,
      { force: true },
    );
  }
  if (
    result.intent?.mode === "dtmf" &&
    Array.isArray(result.plan_steps) &&
    result.plan_steps.length
  ) {
    webhookService.addLiveEvent(
      callSid,
      `🧭 Digit capture plan started (${result.intent.group_id || "group"})`,
      { force: true },
    );
    const provider = callConfig?.provider || currentProvider;
    const isGroupedPlan = ["banking", "card"].includes(result.intent.group_id);
    const deferTwiml = provider === "twilio" && isGroupedPlan;
    await digitService.requestDigitCollectionPlan(
      callSid,
      {
        steps: result.plan_steps,
        end_call_on_success: true,
        group_id: result.intent.group_id,
        capture_mode: "ivr_gather",
        defer_twiml: deferTwiml,
      },
      gptService,
    );
    return result;
  }
  if (result.intent?.mode === "dtmf" && result.expectation) {
    try {
      await digitService.flushBufferedDigits(
        callSid,
        gptService,
        interactionCount,
        "dtmf",
        { allowCallEnd: true },
      );
    } catch (err) {
      console.error("Flush buffered digits error:", err);
    }
  }
  return result;
}

async function handleExternalDtmfInput(callSid, digits, options = {}) {
  if (!callSid || !digitService) {
    return { handled: false, reason: "digit_service_unavailable" };
  }
  const normalizedDigits = normalizeDigitString(digits);
  if (!normalizedDigits) {
    return { handled: false, reason: "empty_digits" };
  }

  const source = String(options.source || "dtmf").trim() || "dtmf";
  const provider =
    String(options.provider || callConfigurations.get(callSid)?.provider || "")
      .trim()
      .toLowerCase() || null;
  const activeSession = activeCalls.get(callSid);
  const gptService = options.gptService || activeSession?.gptService || null;
  const interactionCount = Number.isFinite(options.interactionCount)
    ? Number(options.interactionCount)
    : Number.isFinite(activeSession?.interactionCount)
      ? Number(activeSession.interactionCount)
      : 0;

  clearSilenceTimer(callSid);
  markStreamMediaSeen(callSid);
  streamLastMediaAt.set(callSid, Date.now());

  const callConfig = callConfigurations.get(callSid);
  if (!callConfig) {
    return { handled: false, reason: "missing_call_config" };
  }
  markKeypadDtmfSeen(callSid, {
    source,
    digitsLength: normalizedDigits.length,
  });

  const captureActive = isCaptureActiveConfig(callConfig);
  let isDigitIntent = callConfig?.digit_intent?.mode === "dtmf" || captureActive;
  if (!isDigitIntent) {
    const hasExplicitDigitConfig = Boolean(
      callConfig.collection_profile ||
        callConfig.script_policy?.requires_otp ||
        callConfig.script_policy?.default_profile,
    );
    if (hasExplicitDigitConfig) {
      await applyInitialDigitIntent(
        callSid,
        callConfig,
        gptService,
        interactionCount,
      );
      isDigitIntent = callConfig?.digit_intent?.mode === "dtmf";
    }
  }

  const shouldBuffer =
    isDigitIntent ||
    digitService?.hasPlan?.(callSid) ||
    digitService?.hasExpectation?.(callSid);
  if (!isDigitIntent && !shouldBuffer) {
    webhookService.addLiveEvent(
      callSid,
      `🔢 Keypad: ${normalizedDigits} (ignored - normal flow)`,
      { force: true },
    );
    return { handled: false, reason: "normal_flow" };
  }

  const expectation = digitService?.getExpectation(callSid);
  const activePlan = digitService?.getPlan?.(callSid);
  const planStepIndex = Number.isFinite(activePlan?.index)
    ? activePlan.index + 1
    : null;

  if (!expectation) {
    if (digitService?.bufferDigits) {
      digitService.bufferDigits(callSid, normalizedDigits, {
        timestamp: Date.now(),
        source,
        early: true,
        plan_id: activePlan?.id || null,
        plan_step_index: planStepIndex,
        provider,
      });
    }
    webhookService.addLiveEvent(
      callSid,
      `🔢 Keypad: ${normalizedDigits} (buffered early)`,
      { force: true },
    );
    return { handled: true, buffered: true };
  }

  await digitService.flushBufferedDigits(
    callSid,
    gptService,
    interactionCount,
    "dtmf",
    { allowCallEnd: true },
  );
  if (!digitService?.hasExpectation(callSid)) {
    return { handled: true, reason: "expectation_cleared" };
  }

  const activeExpectation = digitService.getExpectation(callSid);
  const display =
    activeExpectation?.profile === "verification"
      ? digitService.formatOtpForDisplay(
          normalizedDigits,
          "progress",
          activeExpectation?.max_digits,
        )
      : `Keypad: ${normalizedDigits}`;
  webhookService.addLiveEvent(callSid, `🔢 ${display}`, {
    force: true,
  });

  const collection = digitService.recordDigits(callSid, normalizedDigits, {
    timestamp: Date.now(),
    source,
    provider,
    attempt_id: activeExpectation?.attempt_id || null,
    plan_id: activeExpectation?.plan_id || null,
    plan_step_index: activeExpectation?.plan_step_index || null,
    channel_session_id: activeExpectation?.channel_session_id || null,
  });

  await digitService.handleCollectionResult(
    callSid,
    collection,
    gptService,
    interactionCount,
    "dtmf",
    { allowCallEnd: true },
  );

  if (db?.updateCallState) {
    await db.updateCallState(callSid, "dtmf_received", {
      at: new Date().toISOString(),
      source,
      provider,
      digits_length: normalizedDigits.length,
      accepted: !!collection?.accepted,
      profile: collection?.profile || null,
      plan_id: collection?.plan_id || null,
      plan_step_index: collection?.plan_step_index || null,
    })
      .catch(() => {});
  }

  return { handled: true, collection };
}

function normalizeHostValue(value) {
  if (!value) return "";
  const first = String(value).split(",")[0].trim();
  if (!first) return "";
  try {
    if (first.includes("://")) {
      const parsed = new URL(first);
      return parsed.host || "";
    }
  } catch {
    // Fall through to plain host normalization.
  }
  return first.replace(/^https?:\/\//i, "").replace(/\/+$/, "");
}

function resolveHost(req) {
  const forwardedHost = normalizeHostValue(req?.headers?.["x-forwarded-host"]);
  if (forwardedHost) return forwardedHost;
  const hostHeader = normalizeHostValue(req?.headers?.host);
  if (hostHeader) return hostHeader;
  return normalizeHostValue(config.server?.hostname);
}

function appendQueryParamsToUrl(rawUrl, params = {}) {
  if (!rawUrl) return rawUrl;
  try {
    const parsed = new URL(String(rawUrl));
    Object.entries(params || {}).forEach(([key, value]) => {
      if (value === undefined || value === null || value === "") return;
      parsed.searchParams.set(key, String(value));
    });
    return parsed.toString();
  } catch {
    return rawUrl;
  }
}

const VONAGE_WS_DEFAULT_CONTENT_TYPE = "audio/l16;rate=16000";
let cachedVonageWebsocketAudioSpec = null;

function normalizeVonageWebsocketContentType(rawValue) {
  const fallback = {
    contentType: VONAGE_WS_DEFAULT_CONTENT_TYPE,
    sampleRate: 16000,
    sttEncoding: "linear16",
    ttsEncoding: "linear16",
  };
  const raw = String(rawValue || "")
    .trim()
    .toLowerCase();
  if (!raw) return fallback;

  const parts = raw
    .split(";")
    .map((part) => part.trim())
    .filter(Boolean);
  const mediaType = parts[0];
  const params = {};
  parts.slice(1).forEach((part) => {
    const [key, value] = part.split("=");
    if (!key || !value) return;
    params[String(key).toLowerCase()] = String(value).toLowerCase();
  });

  const rateCandidate =
    Number(params.rate) ||
    Number(params.sample_rate) ||
    Number(params.samplerate);
  const sampleRate =
    Number.isFinite(rateCandidate) && rateCandidate > 0
      ? rateCandidate
      : fallback.sampleRate;

  if (mediaType === "audio/l16") {
    return {
      contentType: `audio/l16;rate=${sampleRate}`,
      sampleRate,
      sttEncoding: "linear16",
      ttsEncoding: "linear16",
    };
  }

  // Keep backward compatibility for legacy installations that used PCM-u.
  if (mediaType === "audio/pcmu") {
    return {
      contentType: `audio/pcmu;rate=${sampleRate}`,
      sampleRate,
      sttEncoding: "mulaw",
      ttsEncoding: "mulaw",
    };
  }

  console.warn(
    `Unsupported VONAGE_WEBSOCKET_CONTENT_TYPE "${rawValue}". Falling back to ${fallback.contentType}.`,
  );
  return fallback;
}

function getVonageWebsocketAudioSpec() {
  if (!cachedVonageWebsocketAudioSpec) {
    cachedVonageWebsocketAudioSpec = normalizeVonageWebsocketContentType(
      config.vonage?.voice?.websocketContentType,
    );
  }
  return cachedVonageWebsocketAudioSpec;
}

function getVonageWebsocketContentType() {
  return getVonageWebsocketAudioSpec().contentType;
}

function buildVonageAnswerWebhookUrl(req, callSid, extraParams = {}) {
  const host = resolveHost(req) || config.server?.hostname;
  const defaultBase = host ? `https://${host}/answer` : "";
  const baseUrl = config.vonage?.voice?.answerUrl || defaultBase;
  return appendQueryParamsToUrl(baseUrl, {
    callSid: callSid || undefined,
    ...extraParams,
  });
}

function buildVonageEventWebhookUrl(req, callSid, extraParams = {}) {
  const host = resolveHost(req) || config.server?.hostname;
  const defaultBase = host ? `https://${host}/event` : "";
  const baseUrl = config.vonage?.voice?.eventUrl || defaultBase;
  return appendQueryParamsToUrl(baseUrl, {
    callSid: callSid || undefined,
    ...extraParams,
  });
}

function buildVonageWebsocketUrl(req, callSid, extraParams = {}) {
  const host = resolveHost(req) || config.server?.hostname;
  if (!host || !callSid) return "";
  const params = new URLSearchParams();
  params.set("callSid", String(callSid));
  Object.entries(extraParams || {}).forEach(([key, value]) => {
    if (value === undefined || value === null || value === "") return;
    params.set(key, String(value));
  });

  // Reuse existing stream HMAC auth for provider-neutral websocket protection.
  if (config.streamAuth?.secret) {
    const timestamp = String(Date.now());
    const token = buildStreamAuthToken(String(callSid), timestamp);
    if (token) {
      params.set("ts", timestamp);
      params.set("token", token);
    }
  }
  return `wss://${host}/vonage/stream?${params.toString()}`;
}

function reqForHost(host) {
  return {
    headers: {
      host: normalizeHostValue(host),
    },
  };
}

const warnOnInvalidTwilioSignature = (req, label = "") =>
  twilioSignature.warnOnInvalidTwilioSignature(req, label, { resolveHost });

const requireValidTwilioSignature = (req, res, label = "") =>
  twilioSignature.requireValidTwilioSignature(req, res, label, { resolveHost });

function buildTwilioStreamTwiml(hostname, options = {}) {
  const response = new VoiceResponse();
  const connect = response.connect();
  const host = hostname || config.server.hostname;
  const streamParameters = {};
  if (options.from) streamParameters.from = String(options.from);
  if (options.to) streamParameters.to = String(options.to);
  if (options.callSid && config.streamAuth?.secret) {
    const timestamp = String(Date.now());
    const token = buildStreamAuthToken(options.callSid, timestamp);
    if (token) {
      streamParameters.token = token;
      streamParameters.ts = timestamp;
    }
  }
  const streamNode = connect.stream({
    url: `wss://${host}/connection`,
    track: TWILIO_STREAM_TRACK,
  });
  for (const [name, value] of Object.entries(streamParameters)) {
    if (value === undefined || value === null || value === "") continue;
    if (typeof streamNode?.parameter === "function") {
      streamNode.parameter({ name, value: String(value) });
    }
  }
  return response.toString();
}

function buildInboundHoldTwiml(hostname) {
  const response = new VoiceResponse();
  const host = hostname || config.server.hostname;
  const pauseSeconds = 10;
  response.pause({ length: pauseSeconds });
  response.redirect({ method: "POST" }, `https://${host}/incoming?wait=1`);
  return response.toString();
}

function shouldBypassHmac(req) {
  const path = req.path || "";
  if (!path) return false;
  if (req.method === "OPTIONS") {
    return true;
  }
  if (
    req.method === "GET" &&
    (path === "/" ||
      path === "/favicon.ico" ||
      path === "/health" ||
      path === "/status")
  ) {
    return true;
  }
  if (path.startsWith("/webhook/")) return true;
  return HMAC_BYPASS_PATH_PREFIXES.some((prefix) => path.startsWith(prefix));
}

function verifyHmacSignature(req) {
  const secret = config.apiAuth?.hmacSecret;
  if (!secret) {
    return { ok: true, skipped: true, reason: "missing_secret" };
  }

  const headers = req?.headers || {};
  const timestampHeader = headers[HMAC_HEADER_TIMESTAMP];
  const signatureHeader = headers[HMAC_HEADER_SIGNATURE];

  if (!timestampHeader || !signatureHeader) {
    return { ok: false, reason: "missing_headers" };
  }

  const timestamp = Number(timestampHeader);
  if (!Number.isFinite(timestamp)) {
    return { ok: false, reason: "invalid_timestamp" };
  }

  const maxSkewMs = Number(config.apiAuth?.maxSkewMs || 300000);
  const now = Date.now();
  if (Math.abs(now - timestamp) > maxSkewMs) {
    return { ok: false, reason: "timestamp_out_of_range" };
  }

  const payload = buildHmacPayload(req, String(timestampHeader));
  const expected = crypto
    .createHmac("sha256", secret)
    .update(payload)
    .digest("hex");

  try {
    const expectedBuf = Buffer.from(expected, "hex");
    const providedBuf = Buffer.from(String(signatureHeader), "hex");
    if (expectedBuf.length !== providedBuf.length) {
      return { ok: false, reason: "invalid_signature" };
    }
    if (!crypto.timingSafeEqual(expectedBuf, providedBuf)) {
      return { ok: false, reason: "invalid_signature" };
    }
  } catch (error) {
    return { ok: false, reason: "invalid_signature" };
  }

  return { ok: true };
}

function safeCompareSecret(provided, expected) {
  if (!provided || !expected) return false;
  try {
    const expectedBuf = Buffer.from(String(expected), "utf8");
    const providedBuf = Buffer.from(String(provided), "utf8");
    if (expectedBuf.length !== providedBuf.length) return false;
    return crypto.timingSafeEqual(expectedBuf, providedBuf);
  } catch {
    return false;
  }
}

function verifyTelegramWebhookAuth(req) {
  const hmac = verifyHmacSignature(req);
  if (hmac.ok && !hmac.skipped) {
    return { ok: true, method: "hmac" };
  }
  if (hmac.skipped) {
    return { ok: false, reason: "missing_auth_config" };
  }
  return { ok: false, reason: hmac.reason || "invalid_hmac" };
}

function requireValidTelegramWebhook(req, res, label = "") {
  const mode = String(config.telegram?.webhookValidation || "warn").toLowerCase();
  if (mode === "off") return true;
  const verification = verifyTelegramWebhookAuth(req);
  if (verification.ok) return true;
  const path = label || req.originalUrl || req.path || "unknown";
  console.warn(
    `⚠️ Telegram webhook auth failed for ${path}: ${verification.reason || "unknown"}`,
  );
  if (mode === "strict") {
    res.status(401).send("Unauthorized");
    return false;
  }
  return true;
}

function verifyAwsWebhookAuth(req, options = {}) {
  const { allowQuerySecret = false } = options;
  const expectedSecret = config.aws?.webhookSecret;
  const providedHeaderSecret = req?.headers?.["x-aws-webhook-secret"];
  const providedQuerySecret = allowQuerySecret
    ? req.query?.awsWebhookSecret || req.query?.secret
    : null;
  const providedSecret = providedHeaderSecret || providedQuerySecret;
  if (expectedSecret) {
    if (!providedSecret) {
      return { ok: false, reason: "missing_aws_secret" };
    }
    if (!safeCompareSecret(providedSecret, expectedSecret)) {
      return { ok: false, reason: "invalid_aws_secret" };
    }
    return { ok: true, method: "aws_secret" };
  }

  const hmac = verifyHmacSignature(req);
  if (hmac.ok && !hmac.skipped) {
    return { ok: true, method: "hmac" };
  }
  if (hmac.skipped) {
    return { ok: false, reason: "missing_auth_config" };
  }
  return { ok: false, reason: hmac.reason || "invalid_hmac" };
}

function requireValidAwsWebhook(req, res, label = "", options = {}) {
  const mode = String(config.aws?.webhookValidation || "warn").toLowerCase();
  if (mode === "off") return true;
  const verification = verifyAwsWebhookAuth(req, options);
  if (verification.ok) return true;
  const path = label || req.originalUrl || req.path || "unknown";
  console.warn(
    `⚠️ AWS webhook auth failed for ${path}: ${verification.reason || "unknown"}`,
  );
  if (mode === "strict") {
    res.status(401).send("Unauthorized");
    return false;
  }
  return true;
}

function verifyEmailWebhookAuth(req) {
  const expectedSecret = String(config.email?.webhookSecret || "").trim();
  const providedHeaderSecret = req?.headers?.["x-email-webhook-secret"];
  const providedQuerySecret = req?.query?.secret || req?.query?.token;
  const providedSecret = providedHeaderSecret || providedQuerySecret;

  if (expectedSecret) {
    if (!providedSecret) {
      return { ok: false, reason: "missing_email_secret" };
    }
    if (!safeCompareSecret(providedSecret, expectedSecret)) {
      return { ok: false, reason: "invalid_email_secret" };
    }
    return { ok: true, method: "email_secret" };
  }

  const hmac = verifyHmacSignature(req);
  if (hmac.ok && !hmac.skipped) {
    return { ok: true, method: "hmac" };
  }
  if (hmac.skipped) {
    return { ok: false, reason: "missing_auth_config" };
  }
  return { ok: false, reason: hmac.reason || "invalid_hmac" };
}

function requireValidEmailWebhook(req, res, label = "") {
  const mode = String(config.email?.webhookValidation || "warn").toLowerCase();
  if (mode === "off") return true;
  const verification = verifyEmailWebhookAuth(req);
  if (verification.ok) return true;
  const path = label || req.originalUrl || req.path || "unknown";
  console.warn(
    `⚠️ Email webhook auth failed for ${path}: ${verification.reason || "unknown"}`,
  );
  if (mode === "strict") {
    res.status(401).send("Unauthorized");
    return false;
  }
  return true;
}

function verifyAwsStreamAuth(callSid, req) {
  const streamAuth = verifyStreamAuth(callSid, req);
  if (streamAuth.ok || streamAuth.skipped) {
    return { ok: true, method: "stream_auth" };
  }
  const awsFallback = verifyAwsWebhookAuth(req, { allowQuerySecret: true });
  if (awsFallback.ok) {
    return { ok: true, method: awsFallback.method };
  }
  return {
    ok: false,
    reason: streamAuth.reason || awsFallback.reason || "unauthorized",
  };
}

function parseBearerToken(value) {
  if (!value) return null;
  const match = String(value).match(/^Bearer\s+(.+)$/i);
  if (!match) return null;
  const token = match[1]?.trim();
  return token || null;
}

function decodeBase64UrlJson(segment) {
  try {
    const decoded = Buffer.from(String(segment || ""), "base64url").toString(
      "utf8",
    );
    return JSON.parse(decoded);
  } catch {
    return null;
  }
}

function parseVonageSignedJwt(token) {
  if (!token) return null;
  const parts = String(token).split(".");
  if (parts.length !== 3) return null;
  const [encodedHeader, encodedPayload, encodedSignature] = parts;
  const header = decodeBase64UrlJson(encodedHeader);
  const payload = decodeBase64UrlJson(encodedPayload);
  if (!header || !payload) return null;
  return {
    token: String(token),
    header,
    payload,
    encodedHeader,
    encodedPayload,
    encodedSignature,
  };
}

function pruneVonageWebhookJtiCache(nowMs = Date.now()) {
  for (const [key, expiresAt] of vonageWebhookJtiCache.entries()) {
    if (!Number.isFinite(expiresAt) || expiresAt <= nowMs) {
      vonageWebhookJtiCache.delete(key);
    }
  }
  if (vonageWebhookJtiCache.size <= VONAGE_WEBHOOK_JTI_CACHE_MAX) return;
  const ordered = [...vonageWebhookJtiCache.entries()].sort(
    (a, b) => a[1] - b[1],
  );
  const overflow = ordered.length - VONAGE_WEBHOOK_JTI_CACHE_MAX;
  for (let i = 0; i < overflow; i += 1) {
    vonageWebhookJtiCache.delete(ordered[i][0]);
  }
}

function seenVonageWebhookJti(jti, nowMs = Date.now()) {
  if (!jti) return false;
  pruneVonageWebhookJtiCache(nowMs);
  const expiresAt = vonageWebhookJtiCache.get(String(jti));
  if (!Number.isFinite(expiresAt)) return false;
  if (expiresAt <= nowMs) {
    vonageWebhookJtiCache.delete(String(jti));
    return false;
  }
  return true;
}

function storeVonageWebhookJti(jti, expiresAtMs, nowMs = Date.now()) {
  if (!jti) return;
  const fallbackTtlMs = Number(config.vonage?.webhookMaxSkewMs || 300000);
  const safeExpiry = Number.isFinite(expiresAtMs)
    ? expiresAtMs
    : nowMs + fallbackTtlMs;
  vonageWebhookJtiCache.set(String(jti), safeExpiry);
  pruneVonageWebhookJtiCache(nowMs);
}

function computeVonagePayloadHash(req) {
  const method = String(req?.method || "GET").toUpperCase();
  let body = "";
  if (method !== "GET" && method !== "HEAD") {
    if (typeof req?.rawBody === "string") {
      body = req.rawBody;
    } else if (Buffer.isBuffer(req?.rawBody)) {
      body = req.rawBody.toString("utf8");
    } else if (req?.body && Object.keys(req.body).length) {
      body = stableStringify(req.body);
    }
  }
  return crypto
    .createHash("sha256")
    .update(body || "")
    .digest("hex")
    .toLowerCase();
}

function validateVonageSignedWebhook(req) {
  const secret = config.vonage?.webhookSignatureSecret;
  if (!secret) {
    return { ok: false, reason: "missing_secret" };
  }
  const authorization = req?.headers?.authorization || req?.headers?.Authorization;
  const token = parseBearerToken(authorization);
  if (!token) {
    return { ok: false, reason: "missing_bearer_token" };
  }
  const parsed = parseVonageSignedJwt(token);
  if (!parsed) {
    return { ok: false, reason: "invalid_token_format" };
  }
  if (String(parsed.header?.alg || "").toUpperCase() !== "HS256") {
    return { ok: false, reason: "unsupported_algorithm" };
  }

  let providedSignature;
  try {
    providedSignature = Buffer.from(parsed.encodedSignature, "base64url");
  } catch {
    return { ok: false, reason: "invalid_signature_encoding" };
  }
  const expectedSignature = crypto
    .createHmac("sha256", secret)
    .update(`${parsed.encodedHeader}.${parsed.encodedPayload}`)
    .digest();
  if (
    expectedSignature.length !== providedSignature.length ||
    !crypto.timingSafeEqual(expectedSignature, providedSignature)
  ) {
    return { ok: false, reason: "invalid_signature" };
  }

  const claims = parsed.payload || {};
  const nowMs = Date.now();
  const nowSec = Math.floor(nowMs / 1000);
  const skewMs = Number(config.vonage?.webhookMaxSkewMs || 300000);
  const skewSec = Math.ceil(skewMs / 1000);
  const iat = Number(claims.iat);
  const exp = Number(claims.exp);
  const nbf = Number(claims.nbf);

  if (Number.isFinite(iat) && Math.abs(nowSec - iat) > skewSec) {
    return { ok: false, reason: "iat_out_of_range" };
  }
  if (Number.isFinite(exp) && nowSec > exp + skewSec) {
    return { ok: false, reason: "token_expired" };
  }
  if (Number.isFinite(nbf) && nowSec + skewSec < nbf) {
    return { ok: false, reason: "token_not_active" };
  }
  if (
    claims.api_key &&
    config.vonage?.apiKey &&
    String(claims.api_key) !== String(config.vonage.apiKey)
  ) {
    return { ok: false, reason: "api_key_mismatch" };
  }

  const jti = claims.jti ? String(claims.jti) : null;
  if (jti) {
    if (seenVonageWebhookJti(jti, nowMs)) {
      return { ok: false, reason: "replay_detected" };
    }
    const expiresAtMs = Number.isFinite(exp)
      ? exp * 1000 + skewMs
      : Number.isFinite(iat)
        ? iat * 1000 + skewMs
        : nowMs + skewMs;
    storeVonageWebhookJti(jti, expiresAtMs, nowMs);
  }

  const payloadHash =
    claims.payload_hash || claims.payloadHash || claims.body_hash || null;
  const requirePayloadHash = !!config.vonage?.webhookRequirePayloadHash;
  const method = String(req?.method || "GET").toUpperCase();
  const shouldCheckPayloadHash =
    method !== "GET" && method !== "HEAD" && (payloadHash || requirePayloadHash);
  if (shouldCheckPayloadHash && !payloadHash) {
    return { ok: false, reason: "missing_payload_hash" };
  }
  if (shouldCheckPayloadHash && payloadHash) {
    const expectedHash = computeVonagePayloadHash(req);
    if (String(payloadHash).toLowerCase() !== expectedHash) {
      return { ok: false, reason: "payload_hash_mismatch" };
    }
  }

  return { ok: true, claims };
}

function requireValidVonageWebhook(req, res, label = "") {
  const mode = String(config.vonage?.webhookValidation || "warn").toLowerCase();
  if (mode === "off") return true;
  const result = validateVonageSignedWebhook(req);
  if (result.ok) return true;
  const path = label || req.originalUrl || req.path || "unknown";
  console.warn(
    `⚠️ Vonage webhook signature invalid for ${path}: ${result.reason || "unknown"}`,
  );
  if (mode === "strict") {
    // Signed callbacks may be retried by Vonage on 5xx.
    res.status(503).send("Temporary validation failure");
    return false;
  }
  return true;
}

function selectWsProtocol(protocols) {
  if (!protocols) return false;
  if (Array.isArray(protocols) && protocols.length) return protocols[0];
  if (protocols instanceof Set) {
    const iter = protocols.values().next();
    return iter.done ? false : iter.value;
  }
  if (typeof protocols === "string") return protocols;
  return false;
}

const app = express();
ExpressWs(app, null, {
  wsOptions: {
    handleProtocols: (protocols) => selectWsProtocol(protocols),
  },
});
// Trust the first proxy (ngrok/load balancer) so rate limiting can read X-Forwarded-For safely
app.set("trust proxy", 1);

const allowedCorsOrigins = Array.isArray(config.server?.corsOrigins)
  ? config.server.corsOrigins.filter(Boolean)
  : [];
const allowAllCorsInDev = !isProduction && allowedCorsOrigins.length === 0;
if (isProduction && allowedCorsOrigins.length === 0) {
  console.warn(
    "CORS_ORIGINS is empty in production; browser origins will be denied by default.",
  );
}

app.use(
  helmet({
    contentSecurityPolicy: false,
    crossOriginEmbedderPolicy: false,
  }),
);
app.use(
  cors({
    origin: (origin, callback) => {
      if (!origin) {
        return callback(null, true);
      }
      if (!allowedCorsOrigins.length) {
        return callback(null, allowAllCorsInDev);
      }
      if (allowedCorsOrigins.includes(origin)) {
        return callback(null, true);
      }
      return callback(null, false);
    },
    credentials: true,
  }),
);
app.use(compression());

function captureRawBody(req, _res, buf) {
  if (!buf || !buf.length) return;
  req.rawBody = buf.toString("utf8");
}

app.use(express.json({ verify: captureRawBody }));
app.use(express.urlencoded({ extended: true, verify: captureRawBody }));

app.use((req, res, next) => {
  const incoming = normalizeRequestId(req.headers["x-request-id"]);
  const requestId = incoming || uuidv4();
  req.requestId = requestId;
  res.setHeader("x-request-id", requestId);
  next();
});

const apiLimiter = rateLimit({
  windowMs: config.server?.rateLimit?.windowMs || 60000,
  max: config.server?.rateLimit?.max || 300,
  standardHeaders: true,
  legacyHeaders: false,
});
const bypassPathLimiter = rateLimit({
  windowMs: config.server?.rateLimit?.windowMs || 60000,
  max: Math.max(
    60,
    Math.floor((config.server?.rateLimit?.max || 300) / 2),
  ),
  standardHeaders: true,
  legacyHeaders: false,
});

function shouldApplyBypassPathRateLimit(req) {
  if (!shouldBypassHmac(req)) return false;
  const path = req.path || "";
  if (req.method === "OPTIONS") return false;
  if (
    req.method === "GET" &&
    (path === "/" ||
      path === "/favicon.ico" ||
      path === "/health" ||
      path === "/status")
  ) {
    return false;
  }
  return true;
}

function sanitizeTelemetryValue(value) {
  if (value == null) return null;
  if (typeof value === "number") return value;
  if (typeof value === "string") {
    if (value.length > 120) return value.slice(0, 120);
    return value;
  }
  if (typeof value === "boolean") return value;
  return null;
}

function sanitizeTelemetryData(data = {}) {
  const filtered = {};
  const blockedKeys = [
    "phone",
    "otp",
    "token",
    "secret",
    "init",
    "authorization",
    "sid",
  ];
  Object.entries(data || {}).forEach(([key, value]) => {
    const lower = key.toLowerCase();
    if (blockedKeys.some((blocked) => lower.includes(blocked))) return;
    const sanitized = sanitizeTelemetryValue(value);
    if (sanitized !== null) {
      filtered[key] = sanitized;
    }
  });
  return filtered;
}

app.use((req, res, next) => {
  if (shouldBypassHmac(req)) {
    return next();
  }

  const verification = verifyHmacSignature(req);
  if (!verification.ok) {
    console.warn(
      `⚠️ Rejected request due to invalid HMAC (${verification.reason}) ${req.method} ${req.originalUrl}`,
    );
    return res.status(401).json({ success: false, error: "Unauthorized" });
  }
  return next();
});

app.use((req, res, next) => {
  if (shouldBypassHmac(req)) {
    return next();
  }
  return apiLimiter(req, res, next);
});
app.use((req, res, next) => {
  if (!shouldApplyBypassPathRateLimit(req)) {
    return next();
  }
  return bypassPathLimiter(req, res, next);
});

const PORT = config.server?.port || 3000;

// Enhanced call configurations with function context
const callConfigurations = new Map();
const callDirections = new Map();
const activeCalls = new Map();
const callFunctionSystems = new Map(); // Store generated functions per call
const callEndLocks = new Map();
const gatherEventDedupe = new Map();
const silenceTimers = new Map();
const twilioTtsCache = new Map();
const twilioTtsPending = new Map();
const TWILIO_TTS_CACHE_TTL_MS =
  Number(config.twilio?.ttsCacheTtlMs) || 10 * 60 * 1000;
const TWILIO_TTS_CACHE_MAX = Number(config.twilio?.ttsCacheMax) || 200;
const TWILIO_TTS_MAX_CHARS = Number(config.twilio?.ttsMaxChars) || 500;
const TWILIO_TTS_FETCH_TIMEOUT_MS =
  Number(config.twilio?.ttsFetchTimeoutMs) || 4000;
const pendingStreams = new Map(); // callSid -> timeout to detect missing websocket
const streamFirstMediaTimers = new Map();
const streamFirstMediaSeen = new Set();
const gptQueues = new Map();
const normalFlowBuffers = new Map();
const normalFlowProcessing = new Set();
const normalFlowLastInput = new Map();
const normalFlowFailureCounts = new Map();
const gptStallState = new Map();
const gptStallTimers = new Map();
const speechTickTimers = new Map();
const userAudioStates = new Map();

function enqueueGptTask(callSid, task) {
  if (!callSid || typeof task !== "function") {
    return Promise.resolve();
  }
  const current = gptQueues.get(callSid) || Promise.resolve();
  const next = current
    .then(task)
    .catch((err) => {
      console.error("GPT queue error:", err);
    })
    .finally(() => {
      if (gptQueues.get(callSid) === next) {
        gptQueues.delete(callSid);
      }
    });
  gptQueues.set(callSid, next);
  return next;
}

function clearGptQueue(callSid) {
  if (callSid) {
    gptQueues.delete(callSid);
  }
}

function clearNormalFlowState(callSid) {
  if (!callSid) return;
  normalFlowBuffers.delete(callSid);
  normalFlowProcessing.delete(callSid);
  normalFlowLastInput.delete(callSid);
  normalFlowFailureCounts.delete(callSid);
  gptStallState.delete(callSid);
  const stallTimer = gptStallTimers.get(callSid);
  if (stallTimer) {
    clearTimeout(stallTimer);
    gptStallTimers.delete(callSid);
  }
}

function markGptReplyProgress(callSid) {
  if (!callSid) return;
  const state = gptStallState.get(callSid) || {};
  state.lastReplyAt = Date.now();
  state.consecutiveStalls = 0;
  gptStallState.set(callSid, state);
  normalFlowFailureCounts.delete(callSid);
  const stallTimer = gptStallTimers.get(callSid);
  if (stallTimer) {
    clearTimeout(stallTimer);
    gptStallTimers.delete(callSid);
  }
}

function getLastGptReplyAt(callSid) {
  return Number(gptStallState.get(callSid)?.lastReplyAt || 0);
}

function scheduleGptStallGuard(callSid, stallAt) {
  if (!callSid) return;
  const existing = gptStallTimers.get(callSid);
  if (existing) {
    clearTimeout(existing);
  }
  const stallCloseMs =
    Number(config.openRouter?.stallCloseMs) > 0
      ? Number(config.openRouter.stallCloseMs)
      : 12000;
  const timer = setTimeout(() => {
    const state = gptStallState.get(callSid);
    const lastReplyAt = Number(state?.lastReplyAt || 0);
    if (lastReplyAt > stallAt) return;
    if (callEndLocks.has(callSid)) return;
    const session = activeCalls.get(callSid);
    if (session?.ending) return;
    webhookService.addLiveEvent(
      callSid,
      "⚠️ Unable to complete response. Ending call safely.",
      { force: true },
    );
    speakAndEndCall(callSid, CALL_END_MESSAGES.error, "gpt_stall_timeout").catch(
      () => {},
    );
  }, stallCloseMs);
  gptStallTimers.set(callSid, timer);
}

function handleGptStall(callSid, fillerText, emitFiller) {
  if (!callSid) return;
  const state = gptStallState.get(callSid) || {};
  state.lastStallAt = Date.now();
  state.consecutiveStalls = Number(state.consecutiveStalls || 0) + 1;
  gptStallState.set(callSid, state);

  if (state.consecutiveStalls <= 1) {
    webhookService.addLiveEvent(callSid, "⏳ One moment…", { force: true });
    if (typeof emitFiller === "function") {
      emitFiller(fillerText);
    }
  } else {
    webhookService.addLiveEvent(callSid, "⏳ Still working on that request…", {
      force: true,
    });
  }
  scheduleGptStallGuard(callSid, state.lastStallAt);
}

function shouldSkipNormalInput(callSid, text, windowMs = 2000) {
  const cleaned = String(text || "").trim();
  if (!cleaned) return true;
  const last = normalFlowLastInput.get(callSid);
  const now = Date.now();
  if (last && last.text === cleaned && now - last.at < windowMs) {
    return true;
  }
  normalFlowLastInput.set(callSid, { text: cleaned, at: now });
  return false;
}

async function processNormalFlowTranscript(
  callSid,
  text,
  gptService,
  getInteractionCount,
  setInteractionCount,
) {
  if (!callSid || !gptService) return;
  const cleaned = String(text || "").trim();
  if (!cleaned) return;
  if (shouldSkipNormalInput(callSid, cleaned)) return;

  normalFlowBuffers.set(callSid, { text: cleaned, at: Date.now() });
  if (normalFlowProcessing.has(callSid)) {
    return;
  }
  normalFlowProcessing.add(callSid);
  try {
    while (normalFlowBuffers.has(callSid)) {
      const next = normalFlowBuffers.get(callSid);
      normalFlowBuffers.delete(callSid);
      await enqueueGptTask(callSid, async () => {
        if (callEndLocks.has(callSid)) return;
        const session = activeCalls.get(callSid);
        if (session?.ending) return;
        const currentCount =
          typeof getInteractionCount === "function" ? getInteractionCount() : 0;
        const beforeReplyAt = getLastGptReplyAt(callSid);
        try {
          await gptService.completion(next.text, currentCount);
          const afterReplyAt = getLastGptReplyAt(callSid);
          if (afterReplyAt <= beforeReplyAt) {
            const failures = Number(normalFlowFailureCounts.get(callSid) || 0) + 1;
            normalFlowFailureCounts.set(callSid, failures);
            if (failures >= 2) {
              await speakAndEndCall(
                callSid,
                CALL_END_MESSAGES.error,
                "gpt_no_reply",
              );
              return;
            }
          } else {
            normalFlowFailureCounts.delete(callSid);
          }
        } catch (gptError) {
          console.error("GPT completion error:", gptError);
          const failures = Number(normalFlowFailureCounts.get(callSid) || 0) + 1;
          normalFlowFailureCounts.set(callSid, failures);
          webhookService.addLiveEvent(callSid, "⚠️ GPT error, retrying", {
            force: true,
          });
          if (failures >= 2) {
            await speakAndEndCall(callSid, CALL_END_MESSAGES.error, "gpt_error");
            return;
          }
        }
        const nextCount = currentCount + 1;
        if (typeof setInteractionCount === "function") {
          setInteractionCount(nextCount);
        }
      });
    }
  } finally {
    normalFlowProcessing.delete(callSid);
  }
}

const ALLOWED_TWILIO_STREAM_TRACKS = new Set([
  "inbound_track",
  "outbound_track",
  "both_tracks",
]);
const TWILIO_STREAM_TRACK = ALLOWED_TWILIO_STREAM_TRACKS.has(
  (process.env.TWILIO_STREAM_TRACK || "").toLowerCase(),
)
  ? process.env.TWILIO_STREAM_TRACK.toLowerCase()
  : "inbound_track";

const CALL_END_MESSAGES = {
  success: "Thanks, we have what we need. Goodbye.",
  failure:
    "We could not verify the information provided. Thank you for your time. Goodbye.",
  no_response: "We did not receive a response. Thank you and goodbye.",
  user_goodbye: "Thanks for your time. Goodbye.",
  error: "I am having trouble right now. Thank you and goodbye.",
};
const CLOSING_MESSAGE =
  "Thank you—your input has been received. Your request is complete. Goodbye.";
const DIGIT_SETTINGS = {
  otpLength: 6,
  otpMaxRetries: 3,
  otpDisplayMode: "masked",
  defaultCollectDelayMs: 1200,
  fallbackToVoiceOnFailure: true,
  showRawDigitsLive:
    String(process.env.SHOW_RAW_DIGITS_LIVE || "true").toLowerCase() === "true",
  sendRawDigitsToUser:
    String(process.env.SEND_RAW_DIGITS_TO_USER || "true").toLowerCase() ===
    "true",
  minDtmfGapMs: 200,
  riskThresholds: {
    confirm: Number(process.env.DIGIT_RISK_CONFIRM || 0.55),
    dtmf_only: Number(process.env.DIGIT_RISK_DTMF_ONLY || 0.7),
    route_agent: Number(process.env.DIGIT_RISK_ROUTE_AGENT || 0.9),
  },
  smsFallbackEnabled:
    String(process.env.DIGIT_SMS_FALLBACK_ENABLED || "true").toLowerCase() ===
    "true",
  smsFallbackMinRetries: Number(
    process.env.DIGIT_SMS_FALLBACK_MIN_RETRIES || 2,
  ),
  captureVaultTtlMs: 10 * 60 * 1000,
  captureSlo: {
    windowSize: 200,
    successRateMin: 0.78,
    medianCaptureMsMax: 45000,
    duplicateSuppressionRateMax: 0.35,
    timeoutErrorRateMax: 0.2,
  },
  healthThresholds: {
    degraded: Number(process.env.DIGIT_HEALTH_DEGRADED || 30),
    overloaded: Number(process.env.DIGIT_HEALTH_OVERLOADED || 60),
  },
  circuitBreaker: {
    windowMs: Number(process.env.DIGIT_BREAKER_WINDOW_MS || 60000),
    minSamples: Number(process.env.DIGIT_BREAKER_MIN_SAMPLES || 8),
    errorRate: Number(process.env.DIGIT_BREAKER_ERROR_RATE || 0.3),
    cooldownMs: Number(process.env.DIGIT_BREAKER_COOLDOWN_MS || 60000),
  },
};

function getDigitSystemHealth() {
  const active = callConfigurations.size;
  const thresholds = DIGIT_SETTINGS.healthThresholds || {};
  const status =
    active >= thresholds.overloaded
      ? "overloaded"
      : active >= thresholds.degraded
        ? "degraded"
        : "healthy";
  return { status, load: active };
}

// Built-in telephony function scripts to give GPT deterministic controls
const telephonyTools = [
  {
    type: "function",
    function: {
      name: "confirm_identity",
      description:
        "Log that the caller has been identity-verified (do not include the code) and proceed to the next step.",
      parameters: {
        type: "object",
        properties: {
          method: {
            type: "string",
            enum: ["otp", "pin", "knowledge", "other"],
            description: "Verification method used.",
          },
          note: {
            type: "string",
            description:
              "Brief note about what was confirmed (no sensitive values).",
          },
        },
        required: ["method"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "route_to_agent",
      description:
        "End the call politely (no transfer) when escalation is requested.",
      parameters: {
        type: "object",
        properties: {
          reason: {
            type: "string",
            description: "Short reason for the transfer.",
          },
          priority: {
            type: "string",
            enum: ["low", "normal", "high"],
            description: "Transfer priority if applicable.",
          },
        },
        required: ["reason"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "collect_digits",
      description:
        "Ask caller to enter digits on the keypad (e.g., OTP). Do not speak or repeat the digits.",
      parameters: {
        type: "object",
        properties: {
          prompt: {
            type: "string",
            description: "Short instruction to the caller.",
          },
          min_digits: {
            type: "integer",
            description: "Minimum digits expected.",
            minimum: 1,
          },
          max_digits: {
            type: "integer",
            description: "Maximum digits expected.",
            minimum: 1,
          },
          profile: {
            type: "string",
            enum: [
              "generic",
              "verification",
              "ssn",
              "dob",
              "routing_number",
              "account_number",
              "phone",
              "tax_id",
              "ein",
              "claim_number",
              "reservation_number",
              "ticket_number",
              "case_number",
              "account",
              "extension",
              "zip",
              "amount",
              "callback_confirm",
              "card_number",
              "cvv",
              "card_expiry",
            ],
            description: "Collection profile for downstream handling.",
          },
          confirmation_style: {
            type: "string",
            enum: ["none", "last4", "spoken_amount"],
            description:
              "How to confirm receipt (masked, spoken summary only).",
          },
          timeout_s: {
            type: "integer",
            description: "Timeout in seconds before reprompt.",
            minimum: 3,
          },
          max_retries: {
            type: "integer",
            description: "Number of retries before fallback.",
            minimum: 0,
          },
          end_call_on_success: {
            type: "boolean",
            description:
              "If false, keep the call active after digits are captured.",
          },
          allow_spoken_fallback: {
            type: "boolean",
            description: "If true, allow spoken fallback after keypad timeout.",
          },
          mask_for_gpt: {
            type: "boolean",
            description:
              "If true (default), mask digits before sending to GPT/transcripts.",
          },
          speak_confirmation: {
            type: "boolean",
            description:
              "If true, GPT can verbally confirm receipt (without echoing digits).",
          },
          allow_terminator: {
            type: "boolean",
            description:
              "If true, allow a terminator key (default #) to finish early.",
          },
          terminator_char: {
            type: "string",
            description:
              "Single key used to end entry when allow_terminator is true.",
          },
        },
        required: ["prompt", "min_digits", "max_digits"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "collect_multiple_digits",
      description:
        "Collect multiple digit profiles sequentially in a single call (e.g., card number, expiry, CVV, ZIP). Do not repeat digits.",
      parameters: {
        type: "object",
        properties: {
          steps: {
            type: "array",
            description: "Ordered list of digit collection steps.",
            items: {
              type: "object",
              properties: {
                prompt: {
                  type: "string",
                  description: "Short instruction to the caller.",
                },
                min_digits: {
                  type: "integer",
                  description: "Minimum digits expected.",
                  minimum: 1,
                },
                max_digits: {
                  type: "integer",
                  description: "Maximum digits expected.",
                  minimum: 1,
                },
                profile: {
                  type: "string",
                  enum: [
                    "generic",
                    "verification",
                    "ssn",
                    "dob",
                    "routing_number",
                    "account_number",
                    "phone",
                    "tax_id",
                    "ein",
                    "claim_number",
                    "reservation_number",
                    "ticket_number",
                    "case_number",
                    "account",
                    "extension",
                    "zip",
                    "amount",
                    "callback_confirm",
                    "card_number",
                    "cvv",
                    "card_expiry",
                  ],
                  description: "Collection profile for downstream handling.",
                },
                confirmation_style: {
                  type: "string",
                  enum: ["none", "last4", "spoken_amount"],
                  description:
                    "How to confirm receipt (masked, spoken summary only).",
                },
                timeout_s: {
                  type: "integer",
                  description: "Timeout in seconds before reprompt.",
                  minimum: 3,
                },
                max_retries: {
                  type: "integer",
                  description: "Number of retries before fallback.",
                  minimum: 0,
                },
                allow_spoken_fallback: {
                  type: "boolean",
                  description:
                    "If true, allow spoken fallback after keypad timeout.",
                },
                mask_for_gpt: {
                  type: "boolean",
                  description:
                    "If true (default), mask digits before sending to GPT/transcripts.",
                },
                speak_confirmation: {
                  type: "boolean",
                  description:
                    "If true, GPT can verbally confirm receipt (without echoing digits).",
                },
                allow_terminator: {
                  type: "boolean",
                  description:
                    "If true, allow a terminator key (default #) to finish early.",
                },
                terminator_char: {
                  type: "string",
                  description:
                    "Single key used to end entry when allow_terminator is true.",
                },
                end_call_on_success: {
                  type: "boolean",
                  description:
                    "If false, keep the call active after this step.",
                },
              },
              required: ["profile"],
            },
          },
          end_call_on_success: {
            type: "boolean",
            description:
              "If false, keep the call active after all steps are captured.",
          },
          completion_message: {
            type: "string",
            description:
              "Optional message to speak after the final step when not ending the call.",
          },
        },
        required: ["steps"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "play_disclosure",
      description:
        "Play or read a required disclosure to the caller. Keep it concise.",
      parameters: {
        type: "object",
        properties: {
          message: {
            type: "string",
            description: "Disclosure text to convey.",
          },
        },
        required: ["message"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "start_payment",
      description:
        "Start a secure phone payment step for this live call. Twilio provider only.",
      parameters: {
        type: "object",
        properties: {
          amount: {
            type: "number",
            description: "Charge amount in major units (for example 49.99).",
          },
          currency: {
            type: "string",
            description: "Three-letter currency code (for example USD).",
          },
          payment_connector: {
            type: "string",
            description:
              "Twilio Pay Connector name configured in your Twilio account.",
          },
          description: {
            type: "string",
            description: "Short transaction description shown to processors.",
          },
          start_message: {
            type: "string",
            description:
              "Optional spoken line before card capture begins.",
          },
          success_message: {
            type: "string",
            description:
              "Short spoken line after successful payment before resuming the call.",
          },
          failure_message: {
            type: "string",
            description:
              "Short spoken line after failed payment before resuming the call.",
          },
          retry_message: {
            type: "string",
            description:
              "Optional spoken line for recoverable payment retries/timeouts.",
          },
        },
        required: ["amount"],
      },
    },
  },
];

function buildTelephonyImplementations(callSid, gptService = null) {
  const withToolExecution = (toolName, handler) => async (args = {}) => {
    const now = Date.now();
    const existingLock = callToolInFlight.get(callSid);
    if (
      existingLock &&
      now - Number(existingLock.startedAt || 0) < TOOL_LOCK_TTL_MS
    ) {
      webhookService.addLiveEvent(
        callSid,
        `⏳ Tool in progress (${existingLock.tool || "action"})`,
        { force: false },
      );
      return {
        status: "in_progress",
        tool: existingLock.tool || toolName,
      };
    }

    const startedAtIso = new Date(now).toISOString();
    callToolInFlight.set(callSid, {
      tool: toolName,
      startedAt: now,
    });
    const callConfig = callConfigurations.get(callSid);
    if (callConfig) {
      callConfig.tool_in_progress = toolName;
      callConfig.tool_started_at = startedAtIso;
      callConfigurations.set(callSid, callConfig);
    }
    webhookService.markToolInvocation(callSid, toolName, { force: true });
    webhookService.addLiveEvent(callSid, `🛠️ Running ${toolName}`, {
      force: false,
    });
    webhookService
      .setLiveCallPhase(callSid, "agent_responding", { logEvent: false })
      .catch(() => {});
    queuePersistCallRuntimeState(callSid, {
      snapshot: {
        tool_in_progress: toolName,
        tool_started_at: startedAtIso,
      },
    });
    refreshActiveCallTools(callSid);

    try {
      const result = await handler(args);
      webhookService.addLiveEvent(callSid, `✅ ${toolName} completed`, {
        force: false,
      });
      return result;
    } catch (error) {
      webhookService.addLiveEvent(callSid, `⚠️ ${toolName} failed`, {
        force: true,
      });
      console.error(`${toolName} handler error:`, error);
      return {
        error: "tool_failed",
        tool: toolName,
        message: error?.message || "Tool failed",
      };
    } finally {
      const lock = callToolInFlight.get(callSid);
      if (!lock || lock.tool === toolName) {
        callToolInFlight.delete(callSid);
      }
      const finalConfig = callConfigurations.get(callSid);
      if (finalConfig && finalConfig.tool_in_progress === toolName) {
        delete finalConfig.tool_in_progress;
        delete finalConfig.tool_started_at;
        callConfigurations.set(callSid, finalConfig);
      }
      queuePersistCallRuntimeState(callSid, {
        snapshot: {
          tool_in_progress: null,
          tool_started_at: null,
        },
      });
      refreshActiveCallTools(callSid);
      const latestConfig = callConfigurations.get(callSid);
      if (normalizeFlowStateKey(latestConfig?.flow_state) !== "ending") {
        webhookService
          .setLiveCallPhase(callSid, "listening", { logEvent: false })
          .catch(() => {});
      }
    }
  };

  const implementations = {
    confirm_identity: async (args = {}) => {
      const payload = {
        status: "acknowledged",
        method: args.method || "unspecified",
        note: args.note || "",
      };
      try {
        await db.updateCallState(callSid, "identity_confirmed", payload);
        webhookService.addLiveEvent(
          callSid,
          `✅ Identity confirmed (${payload.method})`,
          { force: true },
        );
      } catch (err) {
        console.error("confirm_identity handler error:", err);
      }
      return payload;
    },
    route_to_agent: async (args = {}) => {
      const payload = {
        status: "queued",
        reason: args.reason || "unspecified",
        priority: args.priority || "normal",
      };
      try {
        webhookService.addLiveEvent(
          callSid,
          `📞 Transfer requested (${payload.reason}) • ending call`,
          { force: true },
        );
        await speakAndEndCall(
          callSid,
          CALL_END_MESSAGES.failure,
          "transfer_requested",
        );
      } catch (err) {
        console.error("route_to_agent handler error:", err);
      }
      return payload;
    },
    collect_digits: async (args = {}) => {
      if (!digitService) {
        return { error: "Digit service not ready" };
      }
      const callConfig = callConfigurations.get(callSid) || {};
      const flowState = normalizeFlowStateKey(callConfig.flow_state || "normal");
      if (callConfig.payment_in_progress === true || flowState === "payment_active") {
        return {
          error: "payment_in_progress",
          message: "Digit capture is temporarily unavailable while payment is in progress.",
        };
      }
      return digitService.requestDigitCollection(callSid, args, gptService);
    },
    collect_multiple_digits: async (args = {}) => {
      if (!digitService) {
        return { error: "Digit service not ready" };
      }
      const callConfig = callConfigurations.get(callSid) || {};
      const flowState = normalizeFlowStateKey(callConfig.flow_state || "normal");
      if (callConfig.payment_in_progress === true || flowState === "payment_active") {
        return {
          error: "payment_in_progress",
          message: "Digit capture is temporarily unavailable while payment is in progress.",
        };
      }
      return digitService.requestDigitCollectionPlan(callSid, args, gptService);
    },
    play_disclosure: async (args = {}) => {
      const payload = { message: args.message || "" };
      try {
        await db.updateCallState(callSid, "disclosure_played", payload);
        webhookService.addLiveEvent(callSid, "📢 Disclosure played", {
          force: true,
        });
      } catch (err) {
        console.error("play_disclosure handler error:", err);
      }
      return payload;
    },
    start_payment: async (args = {}) => {
      if (!digitService?.requestPhonePayment) {
        return {
          error: "payment_service_unavailable",
          message: "Payment service is not ready.",
        };
      }
      const callConfig = callConfigurations.get(callSid) || {};
      const flowState = normalizeFlowStateKey(callConfig.flow_state || "normal");
      if (isCaptureActiveConfig(callConfig) || flowState === "capture_pending") {
        return {
          error: "capture_active",
          message: "Cannot start payment while digit capture is active.",
        };
      }
      if (callConfig.payment_in_progress === true || flowState === "payment_active") {
        return {
          error: "payment_in_progress",
          message: "A payment session is already in progress.",
        };
      }
      const activeSession = activeCalls.get(callSid);
      const interactionCount = Number.isFinite(
        Number(activeSession?.interactionCount),
      )
        ? Math.max(0, Math.floor(Number(activeSession.interactionCount)))
        : 0;
      const result = await digitService.requestPhonePayment(callSid, {
        ...args,
        interaction_count: interactionCount,
      });
      if (result?.status === "started") {
        queuePersistCallRuntimeState(callSid, {
          snapshot: {
            payment_in_progress: true,
            payment_session: {
              payment_id: result.payment_id || null,
              amount: result.amount || null,
              currency: result.currency || null,
            },
          },
        });
      }
      return result;
    },
  };
  return Object.fromEntries(
    Object.entries(implementations).map(([toolName, handler]) => [
      toolName,
      withToolExecution(toolName, handler),
    ]),
  );
}

function applyTelephonyTools(
  gptService,
  callSid,
  baseTools = [],
  baseImpl = {},
  options = {},
) {
  const allowTransfer = options.allowTransfer !== false;
  const allowDigitCollection = options.allowDigitCollection !== false;
  const allowPayment = options.allowPayment === true;
  const normalizedName = (tool) =>
    String(tool?.function?.name || "")
      .trim()
      .toLowerCase();

  const filteredBaseTools = (Array.isArray(baseTools) ? baseTools : []).filter(
    (tool) => {
      const name = normalizedName(tool);
      if (!name) return false;
      if (
        !allowTransfer &&
        (name === "route_to_agent" || name === "transfercall")
      )
        return false;
      if (
        !allowDigitCollection &&
        (name === "collect_digits" || name === "collect_multiple_digits")
      )
        return false;
      if (!allowPayment && name === "start_payment") return false;
      return true;
    },
  );

  const filteredTelephonyTools = telephonyTools.filter((tool) => {
    const name = normalizedName(tool);
    if (!allowTransfer && name === "route_to_agent") return false;
    if (
      !allowDigitCollection &&
      (name === "collect_digits" || name === "collect_multiple_digits")
    )
      return false;
    if (!allowPayment && name === "start_payment") return false;
    return true;
  });

  const combinedTools = [...filteredBaseTools, ...filteredTelephonyTools];
  const combinedImpl = {
    ...baseImpl,
    ...buildTelephonyImplementations(callSid, gptService),
  };
  if (!allowTransfer) {
    delete combinedImpl.route_to_agent;
    delete combinedImpl.transferCall;
    delete combinedImpl.transfercall;
  }
  if (!allowDigitCollection) {
    delete combinedImpl.collect_digits;
    delete combinedImpl.collect_multiple_digits;
  }
  if (!allowPayment) {
    delete combinedImpl.start_payment;
  }
  gptService.setDynamicFunctions(combinedTools, combinedImpl);
}

function getCallToolOptions(callSid, callConfig = {}) {
  const isDigitIntent =
    callConfig?.digit_intent?.mode === "dtmf" || isCaptureActiveConfig(callConfig);
  const flowState = normalizeFlowStateKey(callConfig?.flow_state || "normal");
  const hasToolLock = Boolean(
    callConfig?.tool_in_progress || (callSid && callToolInFlight.has(callSid)),
  );
  const capabilities = buildCallCapabilities(callConfig);
  const policyAllowsTransfer = capabilities.transfer === true && isDigitIntent;
  const policyAllowsDigitCollection =
    capabilities.capture === true && isDigitIntent;
  const policyAllowsPayment = capabilities.payment === true;
  const phaseAllowsTransfer = flowState !== "ending";
  const phaseAllowsDigitCollection =
    flowState !== "ending" &&
    flowState !== "capture_active" &&
    flowState !== "payment_active";
  const phaseAllowsPayment =
    flowState !== "ending" &&
    flowState !== "capture_active" &&
    flowState !== "payment_active";
  return {
    allowTransfer: policyAllowsTransfer && phaseAllowsTransfer && !hasToolLock,
    allowDigitCollection:
      policyAllowsDigitCollection && phaseAllowsDigitCollection && !hasToolLock,
    allowPayment: policyAllowsPayment && phaseAllowsPayment && !hasToolLock,
    policyAllowsTransfer,
    policyAllowsDigitCollection,
    policyAllowsPayment,
    capabilities,
    flowState,
    hasToolLock,
  };
}

function refreshActiveCallTools(callSid) {
  if (!callSid) return;
  const session = activeCalls.get(callSid);
  if (!session?.gptService) return;
  const callConfig = callConfigurations.get(callSid) || session.callConfig || {};
  const functionSystem =
    callFunctionSystems.get(callSid) || session.functionSystem || null;
  configureCallTools(session.gptService, callSid, callConfig, functionSystem);
}

function configureCallTools(gptService, callSid, callConfig, functionSystem) {
  if (!gptService) return;
  const baseTools = functionSystem?.functions || [];
  const baseImpl = functionSystem?.implementations || {};
  const options = getCallToolOptions(callSid, callConfig);
  applyTelephonyTools(gptService, callSid, baseTools, baseImpl, options);
  if (
    !options.policyAllowsTransfer &&
    callConfig &&
    !callConfig.no_transfer_note_added
  ) {
    gptService.setCallIntent(
      "Constraint: do not transfer or escalate this call. Stay on the line and handle the customer end-to-end.",
    );
    callConfig.no_transfer_note_added = true;
    callConfigurations.set(callSid, callConfig);
  }
}

function formatDurationForSms(seconds) {
  if (!seconds || Number.isNaN(seconds)) return "";
  const mins = Math.floor(seconds / 60);
  const secs = seconds % 60;
  if (mins === 0) {
    return `${secs}s`;
  }
  return `${mins}m ${secs}s`;
}

function normalizeCallStatus(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/_/g, "-");
}

const STATUS_ORDER = [
  "queued",
  "initiated",
  "ringing",
  "answered",
  "in-progress",
  "completed",
  "voicemail",
  "busy",
  "no-answer",
  "failed",
  "canceled",
];
const TERMINAL_STATUSES = new Set([
  "completed",
  "voicemail",
  "busy",
  "no-answer",
  "failed",
  "canceled",
]);

function getStatusRank(status) {
  const normalized = normalizeCallStatus(status);
  return STATUS_ORDER.indexOf(normalized);
}

function isTerminalStatusKey(status) {
  return TERMINAL_STATUSES.has(normalizeCallStatus(status));
}

function shouldApplyStatusUpdate(previousStatus, nextStatus, options = {}) {
  const prev = normalizeCallStatus(previousStatus);
  const next = normalizeCallStatus(nextStatus);
  if (!next) return false;
  if (!prev) return true;
  if (prev === next) return true;
  if (isTerminalStatusKey(prev)) {
    if (
      options.allowTerminalUpgrade &&
      next === "completed" &&
      prev !== "completed"
    ) {
      return true;
    }
    return false;
  }
  const prevRank = getStatusRank(prev);
  const nextRank = getStatusRank(next);
  if (prevRank === -1 || nextRank === -1) return true;
  return nextRank >= prevRank;
}

function formatContactLabel(call) {
  if (call?.customer_name) return call.customer_name;
  if (call?.victim_name) return call.victim_name;
  const digits = String(call?.phone_number || call?.number || "").replace(
    /\D/g,
    "",
  );
  if (digits.length >= 4) {
    return `the contact ending ${digits.slice(-4)}`;
  }
  return "the contact";
}

function buildOutcomeSummary(call, status) {
  const label = formatContactLabel(call);
  switch (status) {
    case "no-answer":
      return `${label} didn't pick up the call.`;
    case "busy":
      return `${label}'s line was busy.`;
    case "failed":
      return `Call failed to reach ${label}.`;
    case "canceled":
      return `Call to ${label} was canceled.`;
    default:
      return "Call finished.";
  }
}

function buildRecapSmsBody(call) {
  const nameValue = call.customer_name || call.victim_name;
  const name = nameValue ? ` with ${nameValue}` : "";
  const normalizedStatus = normalizeCallStatus(
    call.status || call.twilio_status || "completed",
  );
  const status = normalizedStatus.replace(/_/g, " ");
  const duration = call.duration
    ? ` Duration: ${formatDurationForSms(call.duration)}.`
    : "";
  const rawSummary = (call.call_summary || "").replace(/\s+/g, " ").trim();
  const summary =
    normalizedStatus === "completed"
      ? rawSummary
        ? rawSummary.slice(0, 180)
        : "Call finished."
      : buildOutcomeSummary(call, normalizedStatus);
  return `VoicedNut call recap${name}: ${summary} Status: ${status}.${duration}`;
}

function buildRetrySmsBody(callRecord, callState) {
  const name =
    callState?.customer_name ||
    callState?.victim_name ||
    callRecord?.customer_name ||
    callRecord?.victim_name;
  const greeting = name ? `Hi ${name},` : "Hi,";
  return `${greeting} we tried to reach you by phone. When is a good time to call back?`;
}

function buildInboundSmsBody(callRecord, callState) {
  const name =
    callState?.customer_name ||
    callState?.victim_name ||
    callRecord?.customer_name ||
    callRecord?.victim_name;
  const greeting = name ? `Hi ${name},` : "Hi,";
  const business = callState?.business_id || callRecord?.business_id;
  const intro = business
    ? `Thanks for calling ${business}.`
    : "Thanks for calling.";
  return `${greeting} ${intro} Reply with your request and we will follow up shortly.`;
}

function buildCallbackPayload(callRecord, callState) {
  const prompt = callRecord?.prompt || DEFAULT_INBOUND_PROMPT;
  const firstMessage =
    callRecord?.first_message || DEFAULT_INBOUND_FIRST_MESSAGE;
  return {
    number: callRecord?.phone_number,
    prompt,
    first_message: firstMessage,
    user_chat_id: callRecord?.user_chat_id || null,
    customer_name:
      callState?.customer_name ||
      callState?.victim_name ||
      callRecord?.customer_name ||
      callRecord?.victim_name,
    business_id: callState?.business_id || callRecord?.business_id || null,
    script: callState?.script || callRecord?.script || null,
    script_id: callState?.script_id || callRecord?.script_id || null,
    script_version: callState?.script_version || callRecord?.script_version || null,
    purpose: callState?.purpose || callRecord?.purpose || null,
    emotion: callState?.emotion || callRecord?.emotion || null,
    urgency: callState?.urgency || callRecord?.urgency || null,
    technical_level:
      callState?.technical_level || callRecord?.technical_level || null,
    voice_model: callState?.voice_model || callRecord?.voice_model || null,
    collection_profile:
      callState?.collection_profile || callRecord?.collection_profile || null,
    collection_expected_length:
      callState?.collection_expected_length ||
      callRecord?.collection_expected_length ||
      null,
    collection_timeout_s:
      callState?.collection_timeout_s ||
      callRecord?.collection_timeout_s ||
      null,
    collection_max_retries:
      callState?.collection_max_retries ||
      callRecord?.collection_max_retries ||
      null,
    collection_mask_for_gpt:
      callState?.collection_mask_for_gpt || callRecord?.collection_mask_for_gpt,
    collection_speak_confirmation:
      callState?.collection_speak_confirmation ||
      callRecord?.collection_speak_confirmation,
    payment_enabled: normalizeBooleanFlag(callState?.payment_enabled, false),
    payment_connector: callState?.payment_connector || null,
    payment_amount: callState?.payment_amount || null,
    payment_currency: callState?.payment_currency || null,
    payment_description: callState?.payment_description || null,
    payment_start_message: callState?.payment_start_message || null,
    payment_success_message: callState?.payment_success_message || null,
    payment_failure_message: callState?.payment_failure_message || null,
    payment_retry_message: callState?.payment_retry_message || null,
    payment_policy: parsePaymentPolicy(callState?.payment_policy),
  };
}

async function logConsoleAction(callSid, action, meta = {}) {
  if (!db || !callSid || !action) return;
  try {
    await db.updateCallState(callSid, "console_action", {
      action,
      at: new Date().toISOString(),
      ...meta,
    });
  } catch (error) {
    console.error("Failed to log console action:", error);
  }
}

const DIGIT_PROFILE_LABELS = {
  verification: "OTP",
  otp: "OTP",
  ssn: "SSN",
  dob: "DOB",
  routing_number: "Routing",
  account_number: "Account #",
  phone: "Phone",
  tax_id: "Tax ID",
  ein: "EIN",
  claim_number: "Claim",
  reservation_number: "Reservation",
  ticket_number: "Ticket",
  case_number: "Case",
  account: "Account",
  zip: "ZIP",
  extension: "Ext",
  amount: "Amount",
  callback_confirm: "Callback",
  card_number: "Card",
  cvv: "CVV",
  card_expiry: "Expiry",
  generic: "Digits",
};

function maskSensitiveDigitValue(value, keepTail = 2) {
  const raw = String(value || "").replace(/\D/g, "");
  if (!raw) return "";
  if (raw.length <= keepTail) {
    return "*".repeat(raw.length);
  }
  return `${"*".repeat(Math.max(2, raw.length - keepTail))}${raw.slice(
    -keepTail,
  )}`;
}

function formatDigitSummaryValue(profile, value) {
  const raw = String(value || "").trim();
  if (!raw) return "none";

  if (profile === "amount") {
    const cents = Number(raw);
    if (!Number.isNaN(cents)) {
      return `$${(cents / 100).toFixed(2)}`;
    }
    return raw;
  }

  if (profile === "card_expiry") {
    return "**/**";
  }

  return maskSensitiveDigitValue(raw, 2) || "masked";
}

function buildDigitSummary(digitEvents = []) {
  if (!Array.isArray(digitEvents) || digitEvents.length === 0) {
    return { summary: "", count: 0 };
  }

  const grouped = new Map();
  for (const event of digitEvents) {
    const profile = String(event.profile || "generic").toLowerCase();
    if (!grouped.has(profile)) {
      grouped.set(profile, []);
    }
    grouped.get(profile).push({ ...event, profile });
  }
  const hasSpecificProfiles = [...grouped.keys()].some(
    (profile) => profile !== "generic",
  );
  if (hasSpecificProfiles) {
    grouped.delete("generic");
  }

  const parts = [];
  let acceptedCount = 0;

  for (const [profile, events] of grouped.entries()) {
    const acceptedEvents = events.filter((e) => e.accepted);
    const chosen = acceptedEvents.length
      ? acceptedEvents[acceptedEvents.length - 1]
      : events[events.length - 1];
    const label = DIGIT_PROFILE_LABELS[profile] || profile;
    const value = formatDigitSummaryValue(profile, chosen.digits);

    const suffix = chosen.accepted ? "" : " (unverified)";
    if (chosen.accepted) {
      acceptedCount += 1;
    }
    parts.push(`${label}: ${value}${suffix}`);
  }

  return {
    summary: parts.join(" • "),
    count: acceptedCount,
  };
}

function parseDigitEventMetadata(event = {}) {
  if (!event || event.metadata == null) return {};
  if (typeof event.metadata === "object") return event.metadata;
  try {
    return JSON.parse(event.metadata);
  } catch (_) {
    return {};
  }
}

function buildDigitFunnelStats(digitEvents = []) {
  if (!Array.isArray(digitEvents) || digitEvents.length === 0) {
    return null;
  }
  const steps = new Map();
  for (const event of digitEvents) {
    const meta = parseDigitEventMetadata(event);
    const stepKey = meta.plan_step_index
      ? String(meta.plan_step_index)
      : event.profile || "generic";
    const step = steps.get(stepKey) || {
      step: stepKey,
      label:
        meta.step_label ||
        DIGIT_PROFILE_LABELS[event.profile] ||
        event.profile ||
        "digits",
      plan_id: meta.plan_id || null,
      attempts: 0,
      accepted: 0,
      failed: 0,
      reasons: {},
    };
    step.attempts += 1;
    if (event.accepted) {
      step.accepted += 1;
    } else {
      step.failed += 1;
      const reason = event.reason || "invalid";
      step.reasons[reason] = (step.reasons[reason] || 0) + 1;
    }
    steps.set(stepKey, step);
  }
  const list = Array.from(steps.values());
  const topFailures = {};
  for (const step of list) {
    let topReason = null;
    let topCount = 0;
    for (const [reason, count] of Object.entries(step.reasons || {})) {
      if (count > topCount) {
        topReason = reason;
        topCount = count;
      }
    }
    if (topReason) {
      topFailures[step.step] = { reason: topReason, count: topCount };
    }
  }
  return { steps: list, topFailures };
}

function shouldCloseConversation(text = "") {
  const lower = String(text || "").toLowerCase();
  if (!lower) return false;
  return !!lower.match(
    /\b(thanks|thank you|bye|goodbye|appreciate|that.s all|that is all|have a good|bye bye)\b/,
  );
}

const ADMIN_HEADER_NAME = "x-admin-token";
const SUPPORTED_PROVIDERS = [...SUPPORTED_CALL_PROVIDERS];
let currentProvider = getActiveCallProvider();
let storedProvider = getStoredCallProvider();
let currentSmsProvider = getActiveSmsProvider();
let storedSmsProvider = getStoredSmsProvider();
let currentEmailProvider = getActiveEmailProvider();
let storedEmailProvider = getStoredEmailProvider();

function syncRuntimeProviderMirrors() {
  currentProvider = getActiveCallProvider();
  storedProvider = getStoredCallProvider();
  currentSmsProvider = getActiveSmsProvider();
  storedSmsProvider = getStoredSmsProvider();
  currentEmailProvider = getActiveEmailProvider();
  storedEmailProvider = getStoredEmailProvider();
}
const awsContactMap = new Map();
const vonageCallMap = new Map();

let awsConnectAdapter = null;
let awsTtsAdapter = null;
let vonageVoiceAdapter = null;

function rememberVonageCallMapping(callSid, vonageUuid, source = "unknown") {
  if (!callSid || !vonageUuid) return;
  vonageCallMap.set(String(vonageUuid), String(callSid));

  const callConfig = callConfigurations.get(callSid);
  if (callConfig) {
    if (!callConfig.provider_metadata) {
      callConfig.provider_metadata = {};
    }
    if (callConfig.provider_metadata.vonage_uuid !== vonageUuid) {
      callConfig.provider_metadata.vonage_uuid = String(vonageUuid);
      callConfigurations.set(callSid, callConfig);
      if (db?.updateCallState) {
        db.updateCallState(callSid, "provider_metadata_updated", {
          provider: "vonage",
          vonage_uuid: String(vonageUuid),
          source,
          at: new Date().toISOString(),
        })
          .catch(() => {});
      }
    }
  }
}

async function resolveVonageCallSidFromUuid(vonageUuid) {
  if (!vonageUuid) return null;
  const normalizedUuid = String(vonageUuid);
  const inMemory = vonageCallMap.get(normalizedUuid);
  if (inMemory) return inMemory;

  for (const [callSid, cfg] of callConfigurations.entries()) {
    const cfgUuid = cfg?.provider_metadata?.vonage_uuid;
    if (cfgUuid && String(cfgUuid) === normalizedUuid) {
      rememberVonageCallMapping(callSid, normalizedUuid, "memory_scan");
      return callSid;
    }
  }

  if (!db?.db) return null;
  const rows = await new Promise((resolve) => {
    db.db.all(
      `
        SELECT call_sid, data
        FROM call_states
        WHERE state = 'call_created'
          AND data LIKE ?
        ORDER BY id DESC
        LIMIT 50
      `,
      [`%${normalizedUuid}%`],
      (err, resultRows) => {
        if (err) {
          resolve([]);
          return;
        }
        resolve(Array.isArray(resultRows) ? resultRows : []);
      },
    );
  });

  for (const row of rows) {
    try {
      const parsed = row?.data ? JSON.parse(row.data) : null;
      const stateUuid = parsed?.provider_metadata?.vonage_uuid;
      if (stateUuid && String(stateUuid) === normalizedUuid && row?.call_sid) {
        rememberVonageCallMapping(row.call_sid, normalizedUuid, "db_scan");
        return row.call_sid;
      }
    } catch {
      // Ignore malformed JSON rows.
    }
  }

  return null;
}

async function resolveVonageCallSid(req, payload = {}) {
  const query = req?.query || {};
  const body = payload || {};

  const directCallSid =
    query.callSid ||
    query.call_sid ||
    query.callsid ||
    query.client_ref ||
    body.callSid ||
    body.call_sid ||
    body.callsid ||
    body.client_ref;
  if (directCallSid) {
    return String(directCallSid);
  }

  const uuidCandidates = [
    query.uuid,
    query.vonage_uuid,
    query.conversation_uuid,
    body.uuid,
    body.vonage_uuid,
    body.conversation_uuid,
  ].filter(Boolean);

  for (const candidate of uuidCandidates) {
    const resolved = await resolveVonageCallSidFromUuid(candidate);
    if (resolved) return String(resolved);
  }

  return null;
}

function resolveVonageHangupUuid(callSid, callConfig) {
  const direct = callConfig?.provider_metadata?.vonage_uuid;
  if (direct) return String(direct);
  for (const [uuid, mappedCallSid] of vonageCallMap.entries()) {
    if (String(mappedCallSid) === String(callSid)) {
      return String(uuid);
    }
  }
  return null;
}

function buildVonageInboundCallSid(vonageUuid) {
  const normalized = String(vonageUuid || "")
    .trim()
    .replace(/[^a-zA-Z0-9_-]/g, "");
  if (!normalized) return null;
  return `vonage-in-${normalized}`;
}

function normalizeVonageDirection(value = "") {
  return String(value || "")
    .trim()
    .toLowerCase();
}

function isOutboundVonageDirection(value = "") {
  const normalized = normalizeVonageDirection(value);
  return (
    normalized.startsWith("outbound") ||
    normalized === "outbound-api" ||
    normalized === "api_outbound"
  );
}

function getVonageCallPayload(req, payload = null) {
  const body = payload || req?.body || {};
  const query = req?.query || {};
  const from =
    body.from ||
    body.from_number ||
    body.caller ||
    body.Caller ||
    query.from ||
    query.from_number ||
    query.caller ||
    null;
  const to =
    body.to ||
    body.to_number ||
    body.called ||
    body.Called ||
    query.to ||
    query.to_number ||
    query.called ||
    null;
  const direction =
    body.direction || query.direction || body.call_direction || query.call_direction;
  return {
    from: from || null,
    to: to || null,
    direction: direction || null,
    From: from || null,
    To: to || null,
    Direction: direction || null,
  };
}

function buildVonageTalkHangupNcco(message) {
  const text = String(message || "").trim();
  if (!text) return [{ action: "hangup" }];
  return [
    { action: "talk", text },
    { action: "hangup" },
  ];
}

function normalizeDigitString(value) {
  // Keep digits plus keypad terminators so provider webhooks can signal explicit end-of-entry.
  return String(value || "").replace(/[^0-9#*]/g, "");
}

function getVonageDtmfDigits(payload = {}) {
  const dtmf = payload?.dtmf;
  const candidates = [
    typeof dtmf === "string" ? dtmf : null,
    dtmf?.digits,
    dtmf?.digit,
    payload?.digits,
    payload?.digit,
    payload?.keypad_digits,
    payload?.keypad,
    payload?.key,
    payload?.value,
    payload?.input,
  ];
  for (const candidate of candidates) {
    const normalized = normalizeDigitString(candidate);
    if (normalized) return normalized;
  }
  return "";
}

function clearVonageCallMappings(callSid) {
  if (!callSid) return;
  for (const [uuid, mappedCallSid] of vonageCallMap.entries()) {
    if (String(mappedCallSid) === String(callSid)) {
      vonageCallMap.delete(uuid);
    }
  }
}

const builtinPersonas = [
  {
    id: "general",
    label: "General",
    description: "General voice call assistant",
    purposes: [{ id: "general", label: "General" }],
    default_purpose: "general",
    default_emotion: "neutral",
    default_urgency: "normal",
    default_technical_level: "general",
  },
];

function requireAdminToken(req, res, next) {
  const token = config.admin?.apiToken;
  if (!token) {
    return res
      .status(500)
      .json({ success: false, error: "Admin token not configured" });
  }
  const provided = req.headers[ADMIN_HEADER_NAME];
  if (!provided || provided !== token) {
    return res
      .status(403)
      .json({ success: false, error: "Admin token required" });
  }
  return next();
}

function hasAdminToken(req) {
  const token = config.admin?.apiToken;
  if (!token) return false;
  const provided = req.headers[ADMIN_HEADER_NAME];
  return Boolean(provided && provided === token);
}

function requireOutboundAuthorization(req, res, next) {
  // If request HMAC is configured, global middleware already enforces it.
  if (config.apiAuth?.hmacSecret) {
    return next();
  }
  if (hasAdminToken(req)) {
    return next();
  }
  return sendApiError(
    res,
    403,
    "admin_token_required",
    "Admin token required",
    req.requestId || null,
  );
}

function supportsKeypadCaptureProvider(provider) {
  const normalized = String(provider || "")
    .trim()
    .toLowerCase();
  if (!normalized) return false;
  if (normalized === "twilio") return true;
  if (normalized === "vonage") {
    return config.vonage?.dtmfWebhookEnabled === true;
  }
  return false;
}

function normalizeDigitProfile(value) {
  return String(value || "")
    .trim()
    .toLowerCase();
}

function isKeypadRequiredFlow(collectionProfile, scriptPolicy = {}) {
  const profile = normalizeDigitProfile(
    collectionProfile || scriptPolicy?.default_profile,
  );
  if (scriptPolicy?.requires_otp) return true;
  return ["verification", "otp", "pin"].includes(profile);
}

function normalizeScriptId(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return null;
  if (parsed <= 0) return null;
  return Math.trunc(parsed);
}

function buildKeypadScopeKeys(collectionProfile, scriptPolicy = {}, scriptId = null) {
  const keys = [];
  const normalizedScriptId = normalizeScriptId(scriptId);
  if (normalizedScriptId) {
    keys.push({
      key: `script:${normalizedScriptId}`,
      scope: "script",
      value: normalizedScriptId,
    });
  }
  const profile = normalizeDigitProfile(
    collectionProfile || scriptPolicy?.default_profile,
  );
  if (profile) {
    keys.push({
      key: `profile:${profile}`,
      scope: "profile",
      value: profile,
    });
  }
  return keys;
}

function pruneExpiredKeypadProviderOverrides(nowMs = Date.now()) {
  let changed = false;
  for (const [scopeKey, override] of keypadProviderOverrides.entries()) {
    const expiresAt = Number(override?.expiresAt || 0);
    if (!Number.isFinite(expiresAt) || expiresAt <= nowMs) {
      keypadProviderOverrides.delete(scopeKey);
      changed = true;
    }
  }
  return changed;
}

function serializeKeypadProviderOverrides() {
  pruneExpiredKeypadProviderOverrides();
  return {
    version: 1,
    updated_at: new Date().toISOString(),
    overrides: [...keypadProviderOverrides.entries()].map(
      ([scopeKey, override]) => ({
        scopeKey,
        provider: override?.provider || "twilio",
        expiresAt: Number(override?.expiresAt || 0),
        createdAt: override?.createdAt || null,
        reason: override?.reason || null,
        script_id: override?.script_id || null,
        collection_profile: override?.collection_profile || null,
        source_call_sid: override?.source_call_sid || null,
      }),
    ),
  };
}

function listKeypadProviderOverrides() {
  const changed = pruneExpiredKeypadProviderOverrides();
  if (changed) {
    persistKeypadProviderOverrides().catch(() => {});
  }
  return [...keypadProviderOverrides.entries()].map(([scopeKey, override]) => ({
    scope_key: scopeKey,
    provider: override?.provider || "twilio",
    expires_at: override?.expiresAt
      ? new Date(override.expiresAt).toISOString()
      : null,
    created_at: override?.createdAt || null,
    reason: override?.reason || null,
    script_id: override?.script_id || null,
    collection_profile: override?.collection_profile || null,
    source_call_sid: override?.source_call_sid || null,
  }));
}

async function clearKeypadProviderOverrides(params = {}) {
  const {
    all = false,
    scopeKey = null,
    scope = null,
    value = null,
  } = params || {};
  const normalizedScopeKey = scopeKey ? String(scopeKey).trim() : "";
  const normalizedScope = scope ? String(scope).trim().toLowerCase() : "";
  const normalizedValue = value == null ? "" : String(value).trim().toLowerCase();

  let cleared = 0;
  if (all) {
    cleared = keypadProviderOverrides.size;
    keypadProviderOverrides.clear();
  } else if (normalizedScopeKey) {
    if (keypadProviderOverrides.delete(normalizedScopeKey)) {
      cleared = 1;
    }
  } else if (normalizedScope && normalizedValue) {
    const targetKey = `${normalizedScope}:${normalizedValue}`;
    if (keypadProviderOverrides.delete(targetKey)) {
      cleared = 1;
    }
  }

  await persistKeypadProviderOverrides();
  return {
    cleared,
    remaining: keypadProviderOverrides.size,
    overrides: listKeypadProviderOverrides(),
  };
}

async function loadStoredProviderSetting(options = {}) {
  const {
    channel,
    settingKey,
    label = "provider",
    supportedProviders = [],
    getReadiness = () => ({}),
    getActive = () => "unknown",
    setActive = () => {},
    setStored = () => {},
  } = options;
  if (!db?.getSetting) return;

  try {
    const raw = await db.getSetting(settingKey);
    if (!raw) {
      console.log(
        `☎️ Default ${label} provider from env: ${String(getActive() || "unknown").toUpperCase()}`,
      );
      return;
    }

    let normalized = null;
    try {
      normalized = normalizeProvider(channel, raw);
    } catch {
      console.warn(
        `Ignoring invalid stored ${label} provider "${raw}". Supported values: ${supportedProviders.join(", ")}`,
      );
      return;
    }

    setStored(normalized);
    const readiness = getReadiness() || {};
    if (readiness[normalized]) {
      setActive(normalized);
      console.log(
        `☎️ Loaded default ${label} provider from storage: ${normalized.toUpperCase()} (active)`,
      );
      return;
    }

    console.warn(
      `Stored ${label} provider "${normalized}" is not configured/ready in this environment. Keeping active provider "${getActive()}".`,
    );
    console.log(
      `☎️ Default ${label} provider remains: ${String(getActive() || "unknown").toUpperCase()}`,
    );
  } catch (error) {
    console.error(`Failed to load stored ${label} provider:`, error);
  } finally {
    syncRuntimeProviderMirrors();
  }
}

async function loadStoredCallProvider() {
  await loadStoredProviderSetting({
    channel: PROVIDER_CHANNELS.CALL,
    settingKey: CALL_PROVIDER_SETTING_KEY,
    label: "call",
    supportedProviders: SUPPORTED_CALL_PROVIDERS,
    getReadiness: getProviderReadiness,
    getActive: getActiveCallProvider,
    setActive: setActiveCallProvider,
    setStored: setStoredCallProvider,
  });
}

async function loadStoredSmsProvider() {
  await loadStoredProviderSetting({
    channel: PROVIDER_CHANNELS.SMS,
    settingKey: SMS_PROVIDER_SETTING_KEY,
    label: "sms",
    supportedProviders: SUPPORTED_SMS_PROVIDERS,
    getReadiness: getSmsProviderReadiness,
    getActive: getActiveSmsProvider,
    setActive: setActiveSmsProvider,
    setStored: setStoredSmsProvider,
  });
}

async function loadStoredEmailProvider() {
  await loadStoredProviderSetting({
    channel: PROVIDER_CHANNELS.EMAIL,
    settingKey: EMAIL_PROVIDER_SETTING_KEY,
    label: "email",
    supportedProviders: SUPPORTED_EMAIL_PROVIDERS,
    getReadiness: getEmailProviderReadiness,
    getActive: getActiveEmailProvider,
    setActive: setActiveEmailProvider,
    setStored: setStoredEmailProvider,
  });
}

async function persistKeypadProviderOverrides() {
  if (!db?.setSetting) return;
  try {
    await db.setSetting(
      KEYPAD_PROVIDER_OVERRIDE_SETTING_KEY,
      JSON.stringify(serializeKeypadProviderOverrides()),
    );
  } catch (error) {
    console.error("Failed to persist keypad provider overrides:", error);
  }
}

async function loadKeypadProviderOverrides() {
  keypadProviderOverrides.clear();
  if (!db?.getSetting) return;
  try {
    const raw = await db.getSetting(KEYPAD_PROVIDER_OVERRIDE_SETTING_KEY);
    if (!raw) return;
    const parsed = JSON.parse(raw);
    const overrides = Array.isArray(parsed?.overrides) ? parsed.overrides : [];
    const nowMs = Date.now();
    for (const item of overrides) {
      const scopeKey = String(item?.scopeKey || "").trim();
      const provider = String(item?.provider || "").trim().toLowerCase();
      const expiresAt = Number(item?.expiresAt || 0);
      if (!scopeKey || !provider || !Number.isFinite(expiresAt)) continue;
      if (expiresAt <= nowMs) continue;
      keypadProviderOverrides.set(scopeKey, {
        provider,
        expiresAt,
        createdAt: item?.createdAt || null,
        reason: item?.reason || null,
        script_id: item?.script_id || null,
        collection_profile: item?.collection_profile || null,
        source_call_sid: item?.source_call_sid || null,
      });
    }
  } catch (error) {
    console.error("Failed to load keypad provider overrides:", error);
  }
}

function resolveKeypadProviderOverride(
  collectionProfile,
  scriptPolicy = {},
  scriptId = null,
) {
  const changed = pruneExpiredKeypadProviderOverrides();
  if (changed) {
    persistKeypadProviderOverrides().catch(() => {});
  }
  const scopeKeys = buildKeypadScopeKeys(collectionProfile, scriptPolicy, scriptId);
  for (const scope of scopeKeys) {
    const override = keypadProviderOverrides.get(scope.key);
    if (!override) continue;
    return {
      ...override,
      scopeKey: scope.key,
      scope: scope.scope,
      scopeValue: scope.value,
    };
  }
  return null;
}

function clearKeypadDtmfWatchdog(callSid) {
  if (!callSid) return;
  const existing = keypadDtmfWatchdogs.get(callSid);
  if (existing) {
    clearTimeout(existing);
    keypadDtmfWatchdogs.delete(callSid);
  }
}

function clearKeypadCallState(callSid) {
  if (!callSid) return;
  clearKeypadDtmfWatchdog(callSid);
  keypadDtmfSeen.delete(callSid);
}

function markKeypadDtmfSeen(callSid, meta = {}) {
  if (!callSid) return;
  keypadDtmfSeen.set(callSid, {
    seenAt: new Date().toISOString(),
    source: meta?.source || null,
    digitsLength: Number(meta?.digitsLength || 0) || null,
  });
  clearKeypadDtmfWatchdog(callSid);
}

async function triggerVonageKeypadGuard(callSid, callConfig, timeoutMs) {
  if (!callSid || !callConfig) return;
  const scopeKeys = buildKeypadScopeKeys(
    callConfig.collection_profile,
    callConfig.script_policy || {},
    callConfig.script_id,
  );
  if (!scopeKeys.length) return;

  const cooldownMs = Number(config.keypadGuard?.providerOverrideCooldownMs) || 1800000;
  const nowMs = Date.now();
  const expiresAt = nowMs + cooldownMs;
  const createdAt = new Date(nowMs).toISOString();
  const profile =
    normalizeDigitProfile(
      callConfig.collection_profile || callConfig.script_policy?.default_profile,
    ) || null;
  const scriptId = normalizeScriptId(callConfig.script_id);

  for (const scope of scopeKeys) {
    keypadProviderOverrides.set(scope.key, {
      provider: "twilio",
      expiresAt,
      createdAt,
      reason: "vonage_dtmf_timeout",
      script_id: scriptId,
      collection_profile: profile,
      source_call_sid: callSid,
    });
  }

  await persistKeypadProviderOverrides();

  const remainingMinutes = Math.max(1, Math.ceil(cooldownMs / 60000));
  const alertMessage = `⚠️ Provider guard: no keypad DTMF detected on Vonage within ${Math.round(
    timeoutMs / 1000,
  )}s for call ${callSid.slice(-6)}. Future keypad flows for ${scopeKeys
    .map((s) => s.key)
    .join(", ")} will route to TWILIO for ${remainingMinutes}m.`;

  webhookService.addLiveEvent(callSid, alertMessage, { force: true });
  if (db?.updateCallState) {
    await db
      .updateCallState(callSid, "keypad_provider_override_triggered", {
        at: createdAt,
        provider: "vonage",
        fallback_provider: "twilio",
        timeout_ms: timeoutMs,
        override_scope_keys: scopeKeys.map((s) => s.key),
        override_expires_at: new Date(expiresAt).toISOString(),
      })
      .catch(() => {});
  }
  db?.addCallMetric?.(callSid, "keypad_dtmf_timeout_ms", timeoutMs, {
    provider: "vonage",
    scope_keys: scopeKeys.map((s) => s.key),
    override_provider: "twilio",
  }).catch(() => {});
  db?.logServiceHealth?.("provider_guard", "keypad_timeout", {
    call_sid: callSid,
    provider: "vonage",
    timeout_ms: timeoutMs,
    override_provider: "twilio",
    scope_keys: scopeKeys.map((s) => s.key),
    override_expires_at: new Date(expiresAt).toISOString(),
  }).catch(() => {});

  const alertChatId = callConfig.user_chat_id || config.telegram?.adminChatId || null;
  if (alertChatId && webhookService?.sendTelegramMessage) {
    webhookService
      .sendTelegramMessage(alertChatId, alertMessage)
      .catch((error) =>
        console.error("Failed to send keypad guard alert to Telegram:", error),
      );
  }
}

function scheduleVonageKeypadDtmfWatchdog(callSid, callConfig) {
  clearKeypadDtmfWatchdog(callSid);
  if (!callSid || !callConfig) return;
  if (!config.keypadGuard?.enabled) return;
  if (String(callConfig.provider || "").toLowerCase() !== "vonage") return;
  if (!isKeypadRequiredFlow(callConfig.collection_profile, callConfig.script_policy)) {
    return;
  }
  const timeoutMs = Number(config.keypadGuard?.vonageDtmfTimeoutMs) || 12000;
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) return;

  const timer = setTimeout(() => {
    keypadDtmfWatchdogs.delete(callSid);
    if (keypadDtmfSeen.has(callSid)) return;
    triggerVonageKeypadGuard(callSid, callConfig, timeoutMs).catch((error) => {
      console.error("Vonage keypad guard trigger failed:", error);
    });
  }, timeoutMs);
  keypadDtmfWatchdogs.set(callSid, timer);
}

function getProviderReadiness() {
  const vonageHasCredentials = !!(
    config.vonage.apiKey &&
    config.vonage.apiSecret &&
    config.vonage.applicationId &&
    config.vonage.privateKey &&
    config.vonage.voice?.fromNumber
  );
  const vonageHasRouting = !!(
    config.server?.hostname ||
    config.vonage.voice?.answerUrl ||
    config.vonage.voice?.eventUrl
  );
  const vonageWebhookMode = String(
    config.vonage?.webhookValidation || "warn",
  ).toLowerCase();
  const vonageWebhookReady =
    vonageWebhookMode !== "strict" ||
    Boolean(config.vonage?.webhookSignatureSecret);
  if (
    vonageHasCredentials &&
    vonageHasRouting &&
    !vonageWebhookReady &&
    !warnedVonageWebhookValidation
  ) {
    console.warn(
      "⚠️ Vonage webhook validation is strict but VONAGE_WEBHOOK_SIGNATURE_SECRET is missing. Vonage callbacks will fail until configured.",
    );
    warnedVonageWebhookValidation = true;
  }
  return {
    twilio: !!(
      config.twilio.accountSid &&
      config.twilio.authToken &&
      config.twilio.fromNumber
    ),
    aws: !!(config.aws.connect.instanceId && config.aws.connect.contactFlowId),
    vonage: vonageHasCredentials && vonageHasRouting && vonageWebhookReady,
  };
}

function getSmsProviderReadiness() {
  if (smsService?.getProviderReadiness) {
    return smsService.getProviderReadiness();
  }
  return {
    twilio: !!(
      config.twilio?.accountSid &&
      config.twilio?.authToken &&
      config.twilio?.fromNumber
    ),
    aws: !!(
      config.aws?.pinpoint?.applicationId &&
      config.aws?.pinpoint?.originationNumber &&
      config.aws?.pinpoint?.region
    ),
    vonage: !!(
      config.vonage?.apiKey &&
      config.vonage?.apiSecret &&
      config.vonage?.sms?.fromNumber
    ),
  };
}

function getEmailProviderReadiness() {
  if (emailService?.getProviderReadiness) {
    return emailService.getProviderReadiness();
  }
  return {
    sendgrid: !!config.email?.sendgrid?.apiKey,
    mailgun: !!(config.email?.mailgun?.apiKey && config.email?.mailgun?.domain),
    ses: !!(
      config.email?.ses?.region &&
      config.email?.ses?.accessKeyId &&
      config.email?.ses?.secretAccessKey
    ),
  };
}

function getProviderHealthEntry(provider) {
  if (!providerHealth.has(provider)) {
    providerHealth.set(provider, {
      errorTimestamps: [],
      degradedUntil: 0,
      lastErrorAt: null,
      lastSuccessAt: null,
    });
  }
  return providerHealth.get(provider);
}

function recordProviderError(provider, error) {
  const health = getProviderHealthEntry(provider);
  const windowMs = Number(config.providerFailover?.errorWindowMs) || 120000;
  const threshold = Number(config.providerFailover?.errorThreshold) || 3;
  const cooldownMs = Number(config.providerFailover?.cooldownMs) || 300000;
  const now = Date.now();
  health.errorTimestamps = health.errorTimestamps.filter(
    (ts) => now - ts <= windowMs,
  );
  health.errorTimestamps.push(now);
  health.lastErrorAt = new Date().toISOString();
  if (health.errorTimestamps.length >= threshold) {
    health.degradedUntil = now + cooldownMs;
    db?.logServiceHealth?.("provider_failover", "degraded", {
      provider,
      errors: health.errorTimestamps.length,
      window_ms: windowMs,
      cooldown_ms: cooldownMs,
      error: error?.message || String(error || "unknown"),
    }).catch(() => {});
  }
  providerHealth.set(provider, health);
}

function recordProviderSuccess(provider) {
  const health = getProviderHealthEntry(provider);
  health.errorTimestamps = [];
  health.lastSuccessAt = new Date().toISOString();
  if (health.degradedUntil && Date.now() > health.degradedUntil) {
    health.degradedUntil = 0;
  }
  providerHealth.set(provider, health);
}

function isProviderDegraded(provider) {
  const health = getProviderHealthEntry(provider);
  if (!health.degradedUntil) return false;
  if (Date.now() > health.degradedUntil) {
    health.degradedUntil = 0;
    providerHealth.set(provider, health);
    return false;
  }
  return true;
}

function getProviderOrder(preferred) {
  const order = [];
  if (preferred) order.push(preferred);
  for (const provider of SUPPORTED_PROVIDERS) {
    if (!order.includes(provider)) order.push(provider);
  }
  return order;
}

function selectOutboundProvider(preferred) {
  const readiness = getProviderReadiness();
  const failoverEnabled = config.providerFailover?.enabled !== false;
  const order = getProviderOrder(preferred);
  for (const provider of order) {
    if (!readiness[provider]) continue;
    if (!failoverEnabled) return provider;
    if (!isProviderDegraded(provider)) return provider;
  }
  return null;
}

let warnedMachineDetection = false;
let warnedVonageWebhookValidation = false;
function isMachineDetectionEnabled() {
  const value = String(config.twilio?.machineDetection || "").toLowerCase();
  if (!value) return false;
  if (["disable", "disabled", "off", "false", "0", "none"].includes(value))
    return false;
  return true;
}

function warnIfMachineDetectionDisabled(context = "") {
  if (warnedMachineDetection) return;
  if (currentProvider !== "twilio") return;
  if (isMachineDetectionEnabled()) return;
  const suffix = context ? ` (${context})` : "";
  console.warn(
    `⚠️ Twilio AMD is not enabled${suffix}. Voicemail detection may be unreliable. Set TWILIO_MACHINE_DETECTION=Enable.`,
  );
  warnedMachineDetection = true;
}

function getAwsConnectAdapter() {
  if (!awsConnectAdapter) {
    awsConnectAdapter = new AwsConnectAdapter(config.aws);
  }
  return awsConnectAdapter;
}

function getVonageVoiceAdapter() {
  if (!vonageVoiceAdapter) {
    vonageVoiceAdapter = new VonageVoiceAdapter(config.vonage);
  }
  return vonageVoiceAdapter;
}

function getAwsTtsAdapter() {
  if (!awsTtsAdapter) {
    awsTtsAdapter = new AwsTtsAdapter(config.aws);
  }
  return awsTtsAdapter;
}

async function endCallForProvider(callSid) {
  const callConfig = callConfigurations.get(callSid);
  const provider = callConfig?.provider || currentProvider;

  if (provider === "twilio") {
    const accountSid = config.twilio.accountSid;
    const authToken = config.twilio.authToken;
    if (!accountSid || !authToken) {
      throw new Error("Twilio credentials not configured");
    }
    const client = twilio(accountSid, authToken);
    await client.calls(callSid).update({ status: "completed" });
    return;
  }

  if (provider === "aws") {
    const contactId = callConfig?.provider_metadata?.contact_id;
    if (!contactId) {
      throw new Error("AWS contact id not available");
    }
    const awsAdapter = getAwsConnectAdapter();
    await awsAdapter.stopContact({ contactId });
    return;
  }

  if (provider === "vonage") {
    const callUuid = resolveVonageHangupUuid(callSid, callConfig);
    if (!callUuid) {
      throw new Error(
        "Vonage call UUID not available for hangup; ensure event webhook mapping is configured",
      );
    }
    const vonageAdapter = getVonageVoiceAdapter();
    await vonageAdapter.hangupCall(callUuid);
    return;
  }

  throw new Error(`Unsupported provider ${provider}`);
}

async function connectInboundCall(callSid, hostOverride = null) {
  const callConfig = callConfigurations.get(callSid);
  const provider = callConfig?.provider || currentProvider;
  if (provider !== "twilio") {
    throw new Error("Inbound answer is only supported for Twilio");
  }
  const host = hostOverride || config.server?.hostname;
  if (!host) {
    throw new Error("Server hostname not configured");
  }
  const accountSid = config.twilio.accountSid;
  const authToken = config.twilio.authToken;
  if (!accountSid || !authToken) {
    throw new Error("Twilio credentials not configured");
  }
  const client = twilio(accountSid, authToken);
  const url = `https://${host}/incoming?answer=1`;
  await client.calls(callSid).update({ url, method: "POST" });
}

function estimateSpeechDurationMs(text = "") {
  const words = String(text || "")
    .trim()
    .split(/\s+/)
    .filter(Boolean).length;
  const baseMs = 1200;
  const perWordMs = 420;
  const estimated = baseMs + words * perWordMs;
  return Math.max(1600, Math.min(12000, estimated));
}

async function runPaymentReconciliation(options = {}) {
  if (!db?.listStalePaymentSessions || paymentReconcileRunning) {
    return { ok: false, skipped: true, reason: "not_ready" };
  }
  const reconcileEnabled = config.payment?.reconcile?.enabled !== false;
  if (!reconcileEnabled && options.force !== true) {
    return { ok: true, skipped: true, reason: "disabled" };
  }
  paymentReconcileRunning = true;
  try {
    const staleSeconds = Number.isFinite(Number(options.staleSeconds))
      ? Math.max(60, Math.floor(Number(options.staleSeconds)))
      : Number(config.payment?.reconcile?.staleSeconds) || 240;
    const limit = Number.isFinite(Number(options.limit))
      ? Math.max(1, Math.min(100, Math.floor(Number(options.limit))))
      : Number(config.payment?.reconcile?.batchSize) || 20;
    const rows = await db.listStalePaymentSessions({
      olderThanSeconds: staleSeconds,
      limit,
    });
    let reconciled = 0;
    let skipped = 0;
    let failed = 0;
    for (const row of rows) {
      const callSid = String(row?.call_sid || "").trim();
      if (!callSid || !digitService?.reconcilePaymentSession) {
        skipped += 1;
        continue;
      }
      try {
        const result = await digitService.reconcilePaymentSession(callSid, {
          reason: "payment_reconcile_stale",
          source: options.source || "payment_reconcile_worker",
          staleSince: row?.active_at || null,
        });
        if (result?.reconciled === true) {
          reconciled += 1;
        } else {
          skipped += 1;
        }
      } catch (error) {
        failed += 1;
        console.error("payment_reconcile_call_error", {
          call_sid: callSid,
          error: String(error?.message || error || "unknown_error"),
        });
      }
    }
    const payload = {
      scanned: rows.length,
      reconciled,
      skipped,
      failed,
      stale_seconds: staleSeconds,
      limit,
      source: options.source || "worker",
      at: new Date().toISOString(),
    };
    db.logServiceHealth?.("payment_reconcile", "run", payload).catch(() => {});
    return { ok: true, ...payload };
  } catch (error) {
    db
      .logServiceHealth?.("payment_reconcile", "error", {
        source: options.source || "worker",
        error: String(error?.message || error || "payment_reconcile_failed"),
        at: new Date().toISOString(),
      })
      .catch(() => {});
    throw error;
  } finally {
    paymentReconcileRunning = false;
  }
}

function startBackgroundWorkers() {
  if (backgroundWorkersStarted) return;
  backgroundWorkersStarted = true;

  setInterval(() => {
    smsService.processScheduledMessages().catch((error) => {
      console.error("❌ Scheduled SMS processing error:", error);
    });
  }, 60000); // Check every minute

  if (config.sms?.reconcile?.enabled !== false) {
    setInterval(() => {
      smsService.reconcileStaleOutboundStatuses().catch((error) => {
        console.error("❌ SMS reconcile worker error:", error);
      });
    }, Number(config.sms?.reconcile?.intervalMs) || 120000);

    smsService.reconcileStaleOutboundStatuses().catch((error) => {
      console.error("❌ Initial SMS reconcile run failed:", error);
    });
  }

  if (config.payment?.reconcile?.enabled !== false) {
    setInterval(() => {
      runPaymentReconciliation({
        source: "payment_reconcile_interval",
      }).catch((error) => {
        console.error("❌ Payment reconcile worker error:", error);
      });
    }, Number(config.payment?.reconcile?.intervalMs) || 120000);

    runPaymentReconciliation({
      source: "payment_reconcile_startup",
    }).catch((error) => {
      console.error("❌ Initial payment reconcile run failed:", error);
    });
  }

  setInterval(() => {
    processCallJobs().catch((error) => {
      console.error("❌ Call job processor error:", error);
    });
  }, config.callJobs?.intervalMs || 5000);

  processCallJobs().catch((error) => {
    console.error("❌ Initial call job processor error:", error);
  });

  setInterval(() => {
    if (!db) return;
    db.pruneProviderEventIdempotency?.().catch((error) => {
      console.error("❌ Provider event dedupe prune error:", error);
    });
    db.cleanupCallRuntimeState?.(24).catch((error) => {
      console.error("❌ Call runtime cleanup error:", error);
    });
  }, 5 * 60 * 1000);

  if (db) {
    db.pruneProviderEventIdempotency?.().catch((error) => {
      console.error("❌ Initial provider event dedupe prune error:", error);
    });
    db.cleanupCallRuntimeState?.(24).catch((error) => {
      console.error("❌ Initial call runtime cleanup error:", error);
    });
  }

  setInterval(() => {
    runStreamWatchdog().catch((error) => {
      console.error("❌ Stream watchdog error:", error);
    });
  }, STREAM_WATCHDOG_INTERVAL_MS);

  setInterval(() => {
    if (!emailService) {
      return;
    }
    emailService.processQueue({ limit: 10 }).catch((error) => {
      console.error("❌ Email queue processing error:", error);
    });
  }, config.email?.queueIntervalMs || 5000);

  setInterval(
    () => {
      smsService.cleanupOldConversations(24); // Keep conversations for 24 hours
    },
    60 * 60 * 1000,
  );
}

async function speakAndEndCall(callSid, message, reason = "completed") {
  if (!callSid || callEndLocks.has(callSid)) {
    return;
  }
  callEndLocks.set(callSid, true);
  clearSilenceTimer(callSid);
  if (digitService) {
    digitService.clearCallState(callSid);
  }

  const text = message || "Thank you for your time. Goodbye.";
  const callConfig = callConfigurations.get(callSid);
  const provider = callConfig?.provider || currentProvider;
  const isDigitProfileClosing =
    isCaptureActiveConfig(callConfig) ||
    (typeof reason === "string" && /^digits?[_-]/i.test(reason));
  const session = activeCalls.get(callSid);
  if (session) {
    session.ending = true;
  }
  setCallFlowState(
    callSid,
    {
      flow_state: "ending",
      reason: reason || "call_ending",
      call_mode: "normal",
      digit_capture_active: false,
    },
    { callConfig, source: "speakAndEndCall" },
  );

  webhookService.addLiveEvent(callSid, `👋 Ending call (${reason})`, {
    force: true,
  });
  webhookService.setLiveCallPhase(callSid, "ending").catch(() => {});

  try {
    await db.addTranscript({
      call_sid: callSid,
      speaker: "ai",
      message: text,
      interaction_count: session?.interactionCount || 0,
      personality_used: "closing",
    });
    webhookService.recordTranscriptTurn(callSid, "agent", text);
  } catch (dbError) {
    console.error("Database error adding closing transcript:", dbError);
  }

  try {
    await db.updateCallState(callSid, "call_ending", {
      reason,
      message: text,
    });
  } catch (stateError) {
    console.error("Database error logging call ending:", stateError);
  }

  const delayMs = estimateSpeechDurationMs(text);

  if (provider === "aws") {
    try {
      const ttsAdapter = getAwsTtsAdapter();
      const voiceId = resolveVoiceModel(callConfig);
      const { key } = await ttsAdapter.synthesizeToS3(
        text,
        voiceId ? { voiceId } : {},
      );
      const contactId = callConfig?.provider_metadata?.contact_id;
      if (contactId) {
        const awsAdapter = getAwsConnectAdapter();
        await awsAdapter.enqueueAudioPlayback({ contactId, audioKey: key });
      }
      scheduleSpeechTicks(
        callSid,
        "agent_speaking",
        estimateSpeechDurationMs(text),
        0.5,
      );
    } catch (ttsError) {
      console.error("AWS closing TTS error:", ttsError);
    }
    setTimeout(() => {
      endCallForProvider(callSid).catch((err) =>
        console.error("End call error:", err),
      );
    }, delayMs);
    return;
  }

  if (provider === "twilio" && !session?.ttsService) {
    try {
      const accountSid = config.twilio.accountSid;
      const authToken = config.twilio.authToken;
      if (accountSid && authToken) {
        const response = new VoiceResponse();
        const shouldUseHostedTts = shouldUseTwilioPlay(callConfig);
        const strictTtsPlay = config.twilio?.strictTtsPlay === true;
        let playedHostedTts = false;
        if (shouldUseHostedTts) {
          const finalPromptTtsTimeoutMs = Number.isFinite(
            Number(config.twilio?.finalPromptTtsTimeoutMs),
          )
            ? Number(config.twilio.finalPromptTtsTimeoutMs)
            : 6000;
          let ttsUrl = await getTwilioTtsAudioUrlSafe(
            text,
            callConfig,
            Math.max(1500, finalPromptTtsTimeoutMs),
            { forceGenerate: true },
          );
          if (!ttsUrl && strictTtsPlay) {
            // Strict hosted-TTS mode: retry once before abandoning Twilio say().
            ttsUrl = await getTwilioTtsAudioUrlSafe(
              text,
              callConfig,
              Math.max(2500, finalPromptTtsTimeoutMs + 1500),
              { forceGenerate: true },
            );
          }
          if (ttsUrl) {
            response.play(ttsUrl);
            playedHostedTts = true;
          }
        }
        if (!playedHostedTts) {
          if (isDigitProfileClosing) {
            response.pause({ length: 1 });
          } else if (strictTtsPlay && shouldUseHostedTts) {
            console.warn(
              `Strict hosted TTS mode active for ${callSid}; ending call without Twilio say fallback.`,
            );
            response.pause({ length: 1 });
          } else {
            const sayVoice = resolveTwilioSayVoice(callConfig);
            if (sayVoice) {
              response.say({ voice: sayVoice }, text);
            } else {
              response.say(text);
            }
          }
        }
        response.hangup();
        const client = twilio(accountSid, authToken);
        await client.calls(callSid).update({ twiml: response.toString() });
        return;
      }
    } catch (twilioError) {
      console.error("Twilio closing update error:", twilioError);
    }
  }

  if (session?.ttsService) {
    try {
      await session.ttsService.generate(
        { partialResponseIndex: null, partialResponse: text },
        session?.interactionCount || 0,
      );
    } catch (ttsError) {
      console.error("Closing TTS error:", ttsError);
    }
  }

  setTimeout(() => {
    endCallForProvider(callSid).catch((err) =>
      console.error("End call error:", err),
    );
  }, delayMs);
}

async function recordCallStatus(callSid, status, notificationType, extra = {}) {
  if (!callSid) return;
  const call = await db.getCall(callSid).catch(() => null);
  const previousStatus = call?.status || call?.twilio_status;
  const previousNormalized = normalizeCallStatus(previousStatus);
  const normalizedStatus = normalizeCallStatus(status);
  const applyStatus = shouldApplyStatusUpdate(
    previousStatus,
    normalizedStatus,
    {
      allowTerminalUpgrade: normalizedStatus === "completed",
    },
  );
  const finalStatus = applyStatus
    ? normalizedStatus
    : normalizeCallStatus(previousStatus || normalizedStatus);
  const statusChanged = previousNormalized !== finalStatus;
  await db.updateCallStatus(callSid, finalStatus, extra);
  if (applyStatus && statusChanged) {
    recordCallLifecycle(callSid, finalStatus, {
      source: "internal",
      raw_status: status,
      duration: extra?.duration,
    });
    if (isTerminalStatusKey(finalStatus)) {
      scheduleCallLifecycleCleanup(callSid);
    }
  }
  if (call?.user_chat_id && notificationType && applyStatus && statusChanged) {
    await db.createEnhancedWebhookNotification(
      callSid,
      notificationType,
      call.user_chat_id,
    );
  }
}

async function ensureAwsSession(callSid) {
  if (activeCalls.has(callSid)) {
    return activeCalls.get(callSid);
  }

  const callConfig = callConfigurations.get(callSid);
  const functionSystem = callFunctionSystems.get(callSid);
  if (!callConfig) {
    throw new Error(`Missing call configuration for ${callSid}`);
  }
  const runtimeRestore = await restoreCallRuntimeState(callSid, callConfig);

  let gptService;
  if (functionSystem) {
    gptService = new EnhancedGptService(
      callConfig.prompt,
      callConfig.first_message,
      {
        db,
        webhookService,
        channel: "voice",
        provider: callConfig?.provider || getCurrentProvider(),
        traceId: `call:${callSid}`,
      },
    );
  } else {
    gptService = new EnhancedGptService(
      callConfig.prompt,
      callConfig.first_message,
      {
        db,
        webhookService,
        channel: "voice",
        provider: callConfig?.provider || getCurrentProvider(),
        traceId: `call:${callSid}`,
      },
    );
  }

  gptService.setCallSid(callSid);
  gptService.setExecutionContext({
    traceId: `call:${callSid}`,
    channel: "voice",
    provider: callConfig?.provider || getCurrentProvider(),
  });
  gptService.setCustomerName(
    callConfig?.customer_name || callConfig?.victim_name,
  );
  gptService.setCallProfile(
    callConfig?.purpose || callConfig?.business_context?.purpose,
  );
  gptService.setPersonaContext({
    domain: callConfig?.purpose || callConfig?.business_context?.purpose || "general",
    channel: "voice",
    urgency: callConfig?.urgency || "normal",
  });
  const intentLine = `Call intent: ${callConfig?.script || "general"} | purpose: ${callConfig?.purpose || "general"} | business: ${callConfig?.business_context?.business_id || callConfig?.business_id || "unspecified"}. Keep replies concise and on-task.`;
  gptService.setCallIntent(intentLine);
  const restoredCount = Number(runtimeRestore?.interactionCount || 0);
  await applyInitialDigitIntent(callSid, callConfig, gptService, restoredCount);
  configureCallTools(gptService, callSid, callConfig, functionSystem);

  const session = {
    startTime: new Date(),
    transcripts: [],
    gptService,
    callConfig,
    functionSystem,
    personalityChanges: [],
    interactionCount: restoredCount,
  };

  gptService.on("gptreply", async (gptReply, icount) => {
    try {
      markGptReplyProgress(callSid);
      if (session?.ending) {
        return;
      }
      const personalityInfo = gptReply.personalityInfo || {};

      webhookService.recordTranscriptTurn(
        callSid,
        "agent",
        gptReply.partialResponse,
      );
      webhookService
        .setLiveCallPhase(callSid, "agent_responding")
        .catch(() => {});

      try {
        await db.addTranscript({
          call_sid: callSid,
          speaker: "ai",
          message: gptReply.partialResponse,
          interaction_count: icount,
          personality_used: personalityInfo.name || "default",
          adaptation_data: JSON.stringify(gptReply.adaptationHistory || []),
        });

        await db.updateCallState(callSid, "ai_responded", {
          message: gptReply.partialResponse,
          interaction_count: icount,
          personality: personalityInfo.name,
        });
      } catch (dbError) {
        console.error("Database error adding AI transcript:", dbError);
      }

      try {
        const ttsAdapter = getAwsTtsAdapter();
        const voiceId = resolveVoiceModel(callConfig);
        const { key } = await ttsAdapter.synthesizeToS3(
          gptReply.partialResponse,
          voiceId ? { voiceId } : {},
        );
        const contactId = callConfig?.provider_metadata?.contact_id;
        if (contactId) {
          const awsAdapter = getAwsConnectAdapter();
          await awsAdapter.enqueueAudioPlayback({
            contactId,
            audioKey: key,
          });
          webhookService
            .setLiveCallPhase(callSid, "agent_speaking")
            .catch(() => {});
          scheduleSpeechTicks(
            callSid,
            "agent_speaking",
            estimateSpeechDurationMs(gptReply.partialResponse),
            0.55,
          );
          scheduleSilenceTimer(callSid);
        }
      } catch (ttsError) {
        console.error("AWS TTS playback error:", ttsError);
      }
    } catch (gptReplyError) {
      console.error("AWS session GPT reply handler error:", gptReplyError);
    }
  });

  gptService.on("stall", async (fillerText) => {
    handleGptStall(callSid, fillerText, async (speechText) => {
      try {
        const ttsAdapter = getAwsTtsAdapter();
        const voiceId = resolveVoiceModel(callConfig);
        const { key } = await ttsAdapter.synthesizeToS3(
          speechText,
          voiceId ? { voiceId } : {},
        );
        const contactId = callConfig?.provider_metadata?.contact_id;
        if (contactId) {
          const awsAdapter = getAwsConnectAdapter();
          await awsAdapter.enqueueAudioPlayback({
            contactId,
            audioKey: key,
          });
          webhookService
            .setLiveCallPhase(callSid, "agent_speaking")
            .catch(() => {});
        }
      } catch (err) {
        console.error("AWS filler TTS error:", err);
      }
    });
  });

  activeCalls.set(callSid, session);
  queuePersistCallRuntimeState(callSid, {
    interaction_count: session.interactionCount,
    snapshot: { source: "ensureAwsSession" },
  });

  try {
    const initialExpectation = digitService?.getExpectation(callSid);
    const firstMessage =
      callConfig.first_message ||
      (initialExpectation
        ? digitService.buildDigitPrompt(initialExpectation)
        : "Hello!");
    const ttsAdapter = getAwsTtsAdapter();
    const voiceId = resolveVoiceModel(callConfig);
    const { key } = await ttsAdapter.synthesizeToS3(
      firstMessage,
      voiceId ? { voiceId } : {},
    );
    const contactId = callConfig?.provider_metadata?.contact_id;
    if (contactId) {
      const awsAdapter = getAwsConnectAdapter();
      await awsAdapter.enqueueAudioPlayback({
        contactId,
        audioKey: key,
      });
      webhookService.recordTranscriptTurn(callSid, "agent", firstMessage);
      webhookService
        .setLiveCallPhase(callSid, "agent_speaking")
        .catch(() => {});
      scheduleSpeechTicks(
        callSid,
        "agent_speaking",
        estimateSpeechDurationMs(firstMessage),
        0.5,
      );
      if (digitService?.hasExpectation(callSid)) {
        digitService.markDigitPrompted(callSid, gptService, 0, "dtmf", {
          allowCallEnd: true,
          prompt_text: firstMessage,
        });
        digitService.scheduleDigitTimeout(callSid, gptService, 0);
      }
      scheduleSilenceTimer(callSid);
    }
  } catch (error) {
    console.error("AWS first message playback error:", error);
  }

  return session;
}

async function startServer(options = {}) {
  const { listen = true } = options;
  try {
    console.log("🚀 Initializing Adaptive AI Call System...");
    warnIfMachineDetectionDisabled("startup");

    // Initialize database first
    console.log("Initializing enhanced database...");
    db = new Database();
    await db.initialize();
    const schemaGuard = await db.ensureSchemaGuardrails({
      expectedVersion: Number(config.database?.schemaVersion) || 2,
      strict: config.database?.schemaStrict !== false,
      requiredTables: [
        "calls",
        "call_states",
        "gpt_call_memory",
        "gpt_memory_facts",
        "gpt_tool_audit",
        "gpt_tool_idempotency",
        "provider_event_idempotency",
        "call_runtime_state",
      ],
      requiredIndexes: [
        "idx_gpt_tool_audit_created",
        "idx_gpt_tool_idem_status",
        "idx_provider_event_idem_expires",
        "idx_call_runtime_state_updated",
      ],
    });
    if (!schemaGuard.ok) {
      console.warn("Database schema guardrails detected missing artifacts:", schemaGuard);
    }
    console.log("✅ Enhanced database initialized successfully");
    if (smsService?.setDb) {
      smsService.setDb(db);
    }
    emailService = new EmailService({
      db,
      config,
      providerResolver: () => getActiveEmailProvider(),
    });
    await loadStoredCallProvider();
    await loadStoredSmsProvider();
    await loadStoredEmailProvider();
    await loadPaymentFeatureConfig();
    await refreshInboundDefaultScript(true);
    await loadKeypadProviderOverrides();
    logStartupRuntimeProfile();
    console.log(
      `☎️ Default call provider: ${String(storedProvider || currentProvider || "twilio").toUpperCase()} (active: ${String(currentProvider || "twilio").toUpperCase()})`,
    );
    console.log(
      `✉️ Default SMS provider: ${String(storedSmsProvider || currentSmsProvider || "twilio").toUpperCase()} (active: ${String(currentSmsProvider || "twilio").toUpperCase()})`,
    );
    console.log(
      `📧 Default email provider: ${String(storedEmailProvider || currentEmailProvider || "sendgrid").toUpperCase()} (active: ${String(currentEmailProvider || "sendgrid").toUpperCase()})`,
    );
    console.log("🤖 Voice runtime mode: legacy STT+GPT+TTS");

    // Start webhook service after database is ready
    console.log("Starting enhanced webhook service...");
    webhookService.start(db);
    console.log("✅ Enhanced webhook service started");

    digitService = createDigitCollectionService({
      db,
      webhookService,
      callConfigurations,
      config,
      twilioClient: twilio,
      VoiceResponse,
      getCurrentProvider: () => currentProvider,
      speakAndEndCall,
      clearSilenceTimer,
      queuePendingDigitAction,
      getTwilioTtsAudioUrl: getTwilioTtsAudioUrlSafe,
      callEndMessages: CALL_END_MESSAGES,
      closingMessage: CLOSING_MESSAGE,
      settings: DIGIT_SETTINGS,
      smsService,
      healthProvider: getDigitSystemHealth,
      setCallFlowState,
      getPaymentFeatureConfig: () => getPaymentFeatureConfig(),
      buildPaymentSmsFallbackLink: (callSid, session, callConfig, opts = {}) =>
        buildPaymentSmsFallbackLink(callSid, session, callConfig, opts),
      buildPaymentSmsFallbackMessage: (context = {}) =>
        buildPaymentSmsFallbackMessage(context),
    });
    if (typeof webhookService.setDigitTokenResolver === "function") {
      webhookService.setDigitTokenResolver((callSid, tokenRef) => {
        if (!digitService?.resolveSensitiveTokenRef) return null;
        return digitService.resolveSensitiveTokenRef(callSid, tokenRef);
      });
    }

    // Initialize function engine
    console.log("✅ Dynamic Function Engine ready");

    startBackgroundWorkers();

    // Start HTTP server
    if (listen) {
      app.listen(PORT, () => {
        console.log(`✅ Enhanced Adaptive API server running on port ${PORT}`);
        console.log(
          `🎭 System ready - Personality Engine & Dynamic Functions active`,
        );
        console.log(`📡 Enhanced webhook notifications enabled`);
        console.log(
          `📞 Twilio Media Stream track mode: ${TWILIO_STREAM_TRACK}`,
        );
      });
    }
  } catch (error) {
    console.error("❌ Failed to start server:", error);
    process.exit(1);
  }
}

function logStartupRuntimeProfile() {
  const envCallProvider = String(config.platform?.provider || "twilio").toLowerCase();
  const envSmsProvider = String(
    config.sms?.provider || config.platform?.provider || "twilio",
  ).toLowerCase();
  const envEmailProvider = String(config.email?.provider || "sendgrid").toLowerCase();

  const payload = {
    type: "startup_runtime_profile",
    timestamp: new Date().toISOString(),
    provider: {
      call: {
        env_default: envCallProvider,
        stored_default: storedProvider ? String(storedProvider).toLowerCase() : null,
        effective_default:
          (storedProvider ? String(storedProvider).toLowerCase() : null) ||
          envCallProvider,
        active: String(currentProvider || "twilio").toLowerCase(),
      },
      sms: {
        env_default: envSmsProvider,
        stored_default: storedSmsProvider
          ? String(storedSmsProvider).toLowerCase()
          : null,
        effective_default:
          (storedSmsProvider ? String(storedSmsProvider).toLowerCase() : null) ||
          envSmsProvider,
        active: String(currentSmsProvider || "twilio").toLowerCase(),
      },
      email: {
        env_default: envEmailProvider,
        stored_default: storedEmailProvider
          ? String(storedEmailProvider).toLowerCase()
          : null,
        effective_default:
          (storedEmailProvider ? String(storedEmailProvider).toLowerCase() : null) ||
          envEmailProvider,
        active: String(currentEmailProvider || "sendgrid").toLowerCase(),
      },
    },
    voice_runtime: {
      mode: "legacy_stt_gpt_tts",
    },
  };

  console.log(JSON.stringify(payload));
}

// Enhanced WebSocket connection handler with dynamic functions
app.ws("/connection", (ws, req) => {
  const ua = req?.headers?.["user-agent"] || "unknown-ua";
  const host = req?.headers?.host || "unknown-host";
  console.log(`New WebSocket connection established (host=${host}, ua=${ua})`);
  console.log("Using legacy STT+GPT+TTS pipeline");

  try {
    ws.on("error", (error) => {
      console.error("WebSocket error:", error);
    });
    ws.on("close", (code, reason) => {
      console.warn(
        `WebSocket closed code=${code} reason=${reason?.toString() || ""}`,
      );
    });

    let streamSid;
    let callSid;
    let callConfig = null;
    let callStartTime = null;
    let functionSystem = null;

    let gptErrorCount = 0;
    let gptService;
    const streamService = new StreamService(ws, {
      audioTickIntervalMs: liveConsoleAudioTickMs,
    });
    streamService.on("error", (error) => {
      console.error("Stream service error:", error);
    });
    const transcriptionService = new TranscriptionService();
    const ttsService = new TextToSpeechService({});
    // Prewarm TTS to reduce first-synthesis delay (silent)
    ttsService
      .generate(
        { partialResponseIndex: null, partialResponse: "warming up" },
        -1,
        { silent: true },
      )
      .catch(() => {});

    let marks = [];
    let interactionCount = 0;
    let isInitialized = false;
    let streamAuthOk = false;

    const handleSttFailure = async (tag, error) => {
      if (!callSid) return;
      console.error(
        `STT failure (${tag}) for ${callSid}`,
        error?.message || error || "",
      );
      const nextCount = (sttFailureCounts.get(callSid) || 0) + 1;
      sttFailureCounts.set(callSid, nextCount);
      db?.addCallMetric?.(callSid, "stt_failure", nextCount, { tag }).catch(
        () => {},
      );
      const threshold = Number(config.callSlo?.sttFailureThreshold);
      if (
        Number.isFinite(threshold) &&
        threshold > 0 &&
        nextCount >= threshold
      ) {
        db?.logServiceHealth?.("call_slo", "degraded", {
          call_sid: callSid,
          metric: "stt_failure_count",
          value: nextCount,
          threshold,
        }).catch(() => {});
      }
      const activeSession = activeCalls.get(callSid);
      await activateDtmfFallback(
        callSid,
        callConfig,
        gptService,
        activeSession?.interactionCount || interactionCount,
        tag,
      );
    };

    transcriptionService.on("error", (error) => {
      void handleSttFailure("stt_error", error).catch((sttFailureError) => {
        console.error("STT fallback activation error:", sttFailureError);
      });
    });
    transcriptionService.on("close", () => {
      void handleSttFailure("stt_closed").catch((sttFailureError) => {
        console.error("STT fallback activation error:", sttFailureError);
      });
    });

    ws.on("message", async function message(data) {
      try {
        const msg = JSON.parse(data);
        const event = msg.event;

        if (event === "start") {
          streamSid = msg.start.streamSid;
          callSid = msg.start.callSid;
          callStartTime = new Date();
          streamStartTimes.set(callSid, Date.now());
          if (!callSid) {
            console.warn("WebSocket start missing CallSid");
            ws.close();
            return;
          }
          const customParams = msg.start?.customParameters || {};
          const authResult = verifyStreamAuth(callSid, req, customParams);
          if (!authResult.ok) {
            console.warn("Stream auth failed", {
              callSid,
              streamSid,
              reason: authResult.reason,
            });
            db.updateCallState(callSid, "stream_auth_failed", {
              reason: authResult.reason,
              stream_sid: streamSid || null,
              at: new Date().toISOString(),
            }).catch(() => {});
            ws.close();
            return;
          }
          streamAuthOk = authResult.ok || authResult.skipped;
          const priorStreamSid = streamStartSeen.get(callSid);
          if (priorStreamSid && priorStreamSid === streamSid) {
            console.log(
              `Duplicate stream start ignored for ${callSid} (${streamSid})`,
            );
            return;
          }
          streamStartSeen.set(callSid, streamSid || "unknown");
          const existingConnection = activeStreamConnections.get(callSid);
          if (
            existingConnection &&
            existingConnection.ws !== ws &&
            existingConnection.ws.readyState === 1
          ) {
            console.warn(`Replacing existing stream for ${callSid}`);
            try {
              existingConnection.ws.close(4000, "Replaced by new stream");
            } catch {}
            db.updateCallState(callSid, "stream_replaced", {
              at: new Date().toISOString(),
              previous_stream_sid: existingConnection.streamSid || null,
              new_stream_sid: streamSid || null,
            }).catch(() => {});
          }
          activeStreamConnections.set(callSid, {
            ws,
            streamSid: streamSid || null,
            connectedAt: new Date().toISOString(),
          });
          if (digitService?.isFallbackActive?.(callSid)) {
            digitService.clearDigitFallbackState(callSid);
          }
          if (pendingStreams.has(callSid)) {
            clearTimeout(pendingStreams.get(callSid));
            pendingStreams.delete(callSid);
          }

          console.log(`Adaptive call started - SID: ${callSid}`);

          streamService.setStreamSid(streamSid);

          const streamParams = resolveStreamAuthParams(req, customParams);
          const fromValue =
            streamParams.from ||
            streamParams.From ||
            customParams.from ||
            customParams.From;
          const toValue =
            streamParams.to ||
            streamParams.To ||
            customParams.to ||
            customParams.To;
          const directionHint =
            streamParams.direction ||
            customParams.direction ||
            callDirections.get(callSid);
          const hasDirection = Boolean(String(directionHint || "").trim());
          const isOutbound = hasDirection
            ? isOutboundTwilioDirection(directionHint)
            : false;
          const defaultInbound = callConfigurations.get(callSid)?.inbound;
          const isInbound = hasDirection
            ? !isOutbound
            : typeof defaultInbound === "boolean"
              ? defaultInbound
              : true;

          callConfig = callConfigurations.get(callSid);
          functionSystem = callFunctionSystems.get(callSid);
          if (!callConfig && isOutbound) {
            const hydrated = await hydrateCallConfigFromDb(callSid);
            callConfig = hydrated?.callConfig || callConfig;
            functionSystem = hydrated?.functionSystem || functionSystem;
          }

          if (!callConfig || !functionSystem) {
            const setup = ensureCallSetup(
              callSid,
              {
                From: fromValue,
                To: toValue,
              },
              {
                provider: "twilio",
                inbound: isInbound,
              },
            );
            callConfig = setup.callConfig || callConfig;
            functionSystem = setup.functionSystem || functionSystem;
          }

          if (callConfig && hasDirection) {
            callConfig.inbound = isInbound;
            callConfigurations.set(callSid, callConfig);
          }
          if (callSid && hasDirection) {
            callDirections.set(callSid, isInbound ? "inbound" : "outbound");
          }
          await ensureCallRecord(
            callSid,
            {
              From: fromValue,
              To: toValue,
            },
            "ws_start",
            {
              provider: "twilio",
              inbound: isInbound,
            },
          );
          const runtimeRestore = await restoreCallRuntimeState(
            callSid,
            callConfig,
          );
          if (runtimeRestore?.restored) {
            interactionCount = Math.max(
              interactionCount,
              Number(runtimeRestore.interactionCount || 0),
            );
          }
          streamFirstMediaSeen.delete(callSid);
          scheduleFirstMediaWatchdog(callSid, host, callConfig);

          // Update database with enhanced tracking
          try {
            await db.updateCallStatus(callSid, "started", {
              started_at: callStartTime.toISOString(),
            });
            await db.updateCallState(callSid, "stream_started", {
              stream_sid: streamSid,
              start_time: callStartTime.toISOString(),
            });

            // Create webhook notification for stream start (internal tracking)
            const call = await db.getCall(callSid);
            if (call && call.user_chat_id) {
              await db.createEnhancedWebhookNotification(
                callSid,
                "call_stream_started",
                call.user_chat_id,
              );
            }
            if (callConfig?.inbound) {
              const chatId =
                call?.user_chat_id ||
                callConfig?.user_chat_id ||
                config.telegram?.adminChatId;
              if (chatId) {
                webhookService
                  .sendCallStatusUpdate(callSid, "answered", chatId, {
                    status_source: "stream",
                  })
                  .catch((err) =>
                    console.error("Inbound answered update error:", err),
                  );
              }
            }
          } catch (dbError) {
            console.error("Database error on call start:", dbError);
          }
          // Get call configuration and function system
          const resolvedVoiceModel = resolveVoiceModel(callConfig);
          if (resolvedVoiceModel) {
            ttsService.voiceModel = resolvedVoiceModel;
          }

          if (callConfig && functionSystem) {
            console.log(
              `Using adaptive configuration for ${functionSystem.context.industry} industry`,
            );
            console.log(
              `Available functions: ${Object.keys(functionSystem.implementations).join(", ")}`,
            );
            gptService = new EnhancedGptService(
              callConfig.prompt,
              callConfig.first_message,
              {
                db,
                webhookService,
                channel: "voice",
                provider: callConfig?.provider || getCurrentProvider(),
                traceId: `call:${callSid}`,
              },
            );
          } else {
            console.log(`Standard call detected: ${callSid}`);
            gptService = new EnhancedGptService(
              null,
              null,
              {
                db,
                webhookService,
                channel: "voice",
                provider: callConfig?.provider || getCurrentProvider(),
                traceId: `call:${callSid}`,
              },
            );
          }

          gptService.setCallSid(callSid);
          gptService.setExecutionContext({
            traceId: `call:${callSid}`,
            channel: "voice",
            provider: callConfig?.provider || getCurrentProvider(),
          });
          gptService.setCustomerName(
            callConfig?.customer_name || callConfig?.victim_name,
          );
          gptService.setCallProfile(
            callConfig?.purpose || callConfig?.business_context?.purpose,
          );
          gptService.setPersonaContext({
            domain:
              callConfig?.purpose || callConfig?.business_context?.purpose || "general",
            channel: "voice",
            urgency: callConfig?.urgency || "normal",
          });
          const intentLine = `Call intent: ${callConfig?.script || "general"} | purpose: ${callConfig?.purpose || "general"} | business: ${callConfig?.business_context?.business_id || callConfig?.business_id || "unspecified"}. Keep replies concise and on-task.`;
          gptService.setCallIntent(intentLine);
          if (callConfig) {
            await applyInitialDigitIntent(
              callSid,
              callConfig,
              gptService,
              interactionCount,
            );
          }
          configureCallTools(gptService, callSid, callConfig, functionSystem);

          let gptErrorCount = 0;

          // Set up GPT reply handler with personality tracking
          gptService.on("gptreply", async (gptReply, icount) => {
            try {
              gptErrorCount = 0;
              markGptReplyProgress(callSid);
              const activeSession = activeCalls.get(callSid);
              if (activeSession?.ending) {
                return;
              }
              const personalityInfo = gptReply.personalityInfo || {};
              console.log(
                `${personalityInfo.name || "Default"} Personality: ${gptReply.partialResponse.substring(0, 50)}...`,
              );
              webhookService.recordTranscriptTurn(
                callSid,
                "agent",
                gptReply.partialResponse,
              );
              webhookService
                .setLiveCallPhase(callSid, "agent_responding")
                .catch(() => {});

              // Save AI response to database with personality context
              try {
                await db.addTranscript({
                  call_sid: callSid,
                  speaker: "ai",
                  message: gptReply.partialResponse,
                  interaction_count: icount,
                  personality_used: personalityInfo.name || "default",
                  adaptation_data: JSON.stringify(
                    gptReply.adaptationHistory || [],
                  ),
                });

                await db.updateCallState(callSid, "ai_responded", {
                  message: gptReply.partialResponse,
                  interaction_count: icount,
                  personality: personalityInfo.name,
                });
              } catch (dbError) {
                console.error("Database error adding AI transcript:", dbError);
              }

              ttsService.generate(gptReply, icount);
              scheduleSilenceTimer(callSid);
            } catch (gptReplyError) {
              console.error("Twilio GPT reply handler error:", gptReplyError);
            }
          });

          gptService.on("stall", (fillerText) => {
            handleGptStall(callSid, fillerText, (speechText) => {
              try {
                ttsService.generate(
                  {
                    partialResponse: speechText,
                    personalityInfo: { name: "filler" },
                    adaptationHistory: [],
                  },
                  interactionCount,
                );
              } catch (err) {
                console.error("Filler TTS error:", err);
              }
            });
          });

          gptService.on("gpterror", async (err) => {
            try {
              gptErrorCount += 1;
              const message = err?.message || "GPT error";
              webhookService.addLiveEvent(callSid, `⚠️ GPT error: ${message}`, {
                force: true,
              });
              if (gptErrorCount >= 2) {
                await speakAndEndCall(
                  callSid,
                  CALL_END_MESSAGES.error,
                  "gpt_error",
                );
              }
            } catch (gptHandlerError) {
              console.error("GPT error handler failure:", gptHandlerError);
            }
          });

          // Listen for personality changes
          gptService.on("personalityChanged", async (changeData) => {
            console.log(
              `Personality adapted: ${changeData.from} → ${changeData.to}`,
            );
            console.log(`Reason: ${JSON.stringify(changeData.reason)}`.blue);

            // Log personality change to database
            try {
              await db.updateCallState(callSid, "personality_changed", {
                from: changeData.from,
                to: changeData.to,
                reason: changeData.reason,
                interaction_count: interactionCount,
              });
            } catch (dbError) {
              console.error(
                "Database error logging personality change:",
                dbError,
              );
            }
          });

          activeCalls.set(callSid, {
            startTime: callStartTime,
            transcripts: [],
            gptService,
            callConfig,
            functionSystem,
            personalityChanges: [],
            ttsService,
            interactionCount,
          });
          queuePersistCallRuntimeState(callSid, {
            interaction_count: interactionCount,
            snapshot: { source: "twilio_stream_start" },
          });

          const pendingDigitActions = popPendingDigitActions(callSid);
          const skipGreeting =
            callConfig?.initial_prompt_played === true ||
            pendingDigitActions.length > 0;

          // Initialize call with recording
          try {
            if (skipGreeting) {
              isInitialized = true;
              console.log(
                `Stream reconnected for ${callSid} (skipping greeting)`,
              );
              if (pendingDigitActions.length) {
                await handlePendingDigitActions(
                  callSid,
                  pendingDigitActions,
                  gptService,
                  interactionCount,
                );
              }
              startGroupedGather(callSid, callConfig, {
                preamble: "",
                gptService,
                interactionCount,
              });
            } else {
              await recordingService(ttsService, callSid);

              const initialExpectation = digitService?.getExpectation(callSid);
              const activePlan = digitService?.getPlan
                ? digitService.getPlan(callSid)
                : null;
              const isGroupedGather = Boolean(
                activePlan &&
                ["banking", "card"].includes(activePlan.group_id) &&
                activePlan.capture_mode === "ivr_gather",
              );
              const fallbackPrompt = "One moment while I pull that up.";
              if (isGroupedGather) {
                const firstMessage =
                  callConfig && callConfig.first_message
                    ? callConfig.first_message
                    : fallbackPrompt;
                const preamble = callConfig?.initial_prompt_played
                  ? ""
                  : firstMessage;
                if (callConfig) {
                  callConfig.initial_prompt_played = true;
                  callConfigurations.set(callSid, callConfig);
                }
                if (preamble) {
                  try {
                    await db.addTranscript({
                      call_sid: callSid,
                      speaker: "ai",
                      message: preamble,
                      interaction_count: 0,
                      personality_used: "default",
                    });
                  } catch (dbError) {
                    console.error(
                      "Database error adding initial transcript:",
                      dbError,
                    );
                  }
                  webhookService.recordTranscriptTurn(
                    callSid,
                    "agent",
                    preamble,
                  );
                }
                startGroupedGather(callSid, callConfig, {
                  preamble,
                  gptService,
                  interactionCount,
                });
                scheduleSilenceTimer(callSid);
                isInitialized = true;
                if (pendingDigitActions.length) {
                  await handlePendingDigitActions(
                    callSid,
                    pendingDigitActions,
                    gptService,
                    interactionCount,
                  );
                }
                console.log("Adaptive call initialization complete");
                return;
              }

              const firstMessage =
                callConfig && callConfig.first_message
                  ? callConfig.first_message
                  : initialExpectation
                    ? digitService.buildDigitPrompt(initialExpectation)
                    : fallbackPrompt;

              console.log(
                `First message (${functionSystem?.context.industry || "default"}): ${firstMessage.substring(0, 50)}...`,
              );
              let promptUsed = firstMessage;
              try {
                await ttsService.generate(
                  {
                    partialResponseIndex: null,
                    partialResponse: firstMessage,
                  },
                  0,
                );
              } catch (ttsError) {
                console.error("Initial TTS error:", ttsError);
                try {
                  await ttsService.generate(
                    {
                      partialResponseIndex: null,
                      partialResponse: fallbackPrompt,
                    },
                    0,
                  );
                  promptUsed = fallbackPrompt;
                } catch (fallbackError) {
                  console.error("Initial TTS fallback error:", fallbackError);
                  await speakAndEndCall(
                    callSid,
                    CALL_END_MESSAGES.error,
                    "tts_error",
                  );
                  isInitialized = true;
                  return;
                }
              }

              try {
                await db.addTranscript({
                  call_sid: callSid,
                  speaker: "ai",
                  message: promptUsed,
                  interaction_count: 0,
                  personality_used: "default",
                });
              } catch (dbError) {
                console.error(
                  "Database error adding initial transcript:",
                  dbError,
                );
              }
              if (callConfig) {
                callConfig.initial_prompt_played = true;
                callConfigurations.set(callSid, callConfig);
              }
              if (digitService?.hasExpectation(callSid) && !isGroupedGather) {
                digitService.markDigitPrompted(
                  callSid,
                  gptService,
                  interactionCount,
                  "dtmf",
                  {
                    allowCallEnd: true,
                    prompt_text: promptUsed,
                  },
                );
                digitService.scheduleDigitTimeout(callSid, gptService, 0);
              }
              scheduleSilenceTimer(callSid);
              startGroupedGather(callSid, callConfig, {
                preamble: "",
                delayMs: estimateSpeechDurationMs(promptUsed) + 200,
                gptService,
                interactionCount,
              });

              isInitialized = true;
              if (pendingDigitActions.length) {
                await handlePendingDigitActions(
                  callSid,
                  pendingDigitActions,
                  gptService,
                  interactionCount,
                );
              }
              console.log("Adaptive call initialization complete");
            }
          } catch (recordingError) {
            console.error("Recording service error:", recordingError);
            if (skipGreeting) {
              isInitialized = true;
              console.log(
                `Stream reconnected for ${callSid} (skipping greeting)`,
              );
              if (pendingDigitActions.length) {
                await handlePendingDigitActions(
                  callSid,
                  pendingDigitActions,
                  gptService,
                  interactionCount,
                );
              }
              startGroupedGather(callSid, callConfig, {
                preamble: "",
                gptService,
                interactionCount,
              });
            } else {
              const initialExpectation = digitService?.getExpectation(callSid);
              const activePlan = digitService?.getPlan
                ? digitService.getPlan(callSid)
                : null;
              const isGroupedGather = Boolean(
                activePlan &&
                ["banking", "card"].includes(activePlan.group_id) &&
                activePlan.capture_mode === "ivr_gather",
              );
              const fallbackPrompt = "One moment while I pull that up.";
              if (isGroupedGather) {
                const firstMessage =
                  callConfig && callConfig.first_message
                    ? callConfig.first_message
                    : fallbackPrompt;
                const preamble = callConfig?.initial_prompt_played
                  ? ""
                  : firstMessage;
                if (callConfig) {
                  callConfig.initial_prompt_played = true;
                  callConfigurations.set(callSid, callConfig);
                }
                if (preamble) {
                  try {
                    await db.addTranscript({
                      call_sid: callSid,
                      speaker: "ai",
                      message: preamble,
                      interaction_count: 0,
                      personality_used: "default",
                    });
                  } catch (dbError) {
                    console.error(
                      "Database error adding initial transcript:",
                      dbError,
                    );
                  }
                  webhookService.recordTranscriptTurn(
                    callSid,
                    "agent",
                    preamble,
                  );
                }
                startGroupedGather(callSid, callConfig, {
                  preamble,
                  gptService,
                  interactionCount,
                });
                scheduleSilenceTimer(callSid);
                isInitialized = true;
                return;
              }

              const firstMessage =
                callConfig && callConfig.first_message
                  ? callConfig.first_message
                  : initialExpectation
                    ? digitService.buildDigitPrompt(initialExpectation)
                    : fallbackPrompt;

              let promptUsed = firstMessage;
              try {
                await ttsService.generate(
                  {
                    partialResponseIndex: null,
                    partialResponse: firstMessage,
                  },
                  0,
                );
              } catch (ttsError) {
                console.error("Initial TTS error:", ttsError);
                try {
                  await ttsService.generate(
                    {
                      partialResponseIndex: null,
                      partialResponse: fallbackPrompt,
                    },
                    0,
                  );
                  promptUsed = fallbackPrompt;
                } catch (fallbackError) {
                  console.error("Initial TTS fallback error:", fallbackError);
                  await speakAndEndCall(
                    callSid,
                    CALL_END_MESSAGES.error,
                    "tts_error",
                  );
                  isInitialized = true;
                  return;
                }
              }

              try {
                await db.addTranscript({
                  call_sid: callSid,
                  speaker: "ai",
                  message: promptUsed,
                  interaction_count: 0,
                  personality_used: "default",
                });
              } catch (dbError) {
                console.error("Database error adding AI transcript:", dbError);
              }
              if (callConfig) {
                callConfig.initial_prompt_played = true;
                callConfigurations.set(callSid, callConfig);
              }
              if (digitService?.hasExpectation(callSid) && !isGroupedGather) {
                digitService.markDigitPrompted(
                  callSid,
                  gptService,
                  interactionCount,
                  "dtmf",
                  {
                    allowCallEnd: true,
                    prompt_text: promptUsed,
                  },
                );
                digitService.scheduleDigitTimeout(callSid, gptService, 0);
              }
              scheduleSilenceTimer(callSid);
              startGroupedGather(callSid, callConfig, {
                preamble: "",
                delayMs: estimateSpeechDurationMs(promptUsed) + 200,
                gptService,
                interactionCount,
              });

              isInitialized = true;
            }
          }

          // Clean up old configurations
          const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000);
          for (const [sid, config] of callConfigurations.entries()) {
            if (new Date(config.created_at) < oneHourAgo) {
              callConfigurations.delete(sid);
              callFunctionSystems.delete(sid);
              callDirections.delete(sid);
              activeStreamConnections.delete(sid);
            }
          }
        } else if (event === "media") {
          if (!streamAuthOk) {
            return;
          }
          if (isInitialized && transcriptionService) {
            const now = Date.now();
            streamLastMediaAt.set(callSid, now);
            if (shouldSampleUserAudioLevel(callSid, now)) {
              const level = estimateAudioLevelFromBase64(
                msg?.media?.payload || "",
              );
              updateUserAudioLevel(callSid, level, now);
            }
            markStreamMediaSeen(callSid);
            transcriptionService.send(msg.media.payload);
          }
        } else if (event === "mark") {
          const label = msg.mark.name;
          marks = marks.filter((m) => m !== msg.mark.name);
        } else if (event === "dtmf") {
          const digits = msg?.dtmf?.digits || msg?.dtmf?.digit || "";
          if (digits) {
            clearSilenceTimer(callSid);
            markStreamMediaSeen(callSid);
            streamLastMediaAt.set(callSid, Date.now());
            const callConfig = callConfigurations.get(callSid);
            const captureActive = isCaptureActiveConfig(callConfig);
            let isDigitIntent =
              callConfig?.digit_intent?.mode === "dtmf" || captureActive;
            if (!isDigitIntent && callConfig && digitService) {
              const hasExplicitDigitConfig = !!(
                callConfig.collection_profile ||
                callConfig.script_policy?.requires_otp ||
                callConfig.script_policy?.default_profile
              );
              if (hasExplicitDigitConfig) {
                await applyInitialDigitIntent(
                  callSid,
                  callConfig,
                  gptService,
                  interactionCount,
                );
                isDigitIntent = callConfig?.digit_intent?.mode === "dtmf";
              }
            }
            const shouldBuffer =
              isDigitIntent ||
              digitService?.hasPlan?.(callSid) ||
              digitService?.hasExpectation?.(callSid);
            if (!isDigitIntent && !shouldBuffer) {
              webhookService.addLiveEvent(
                callSid,
                `🔢 Keypad: ${digits} (ignored - normal flow)`,
                { force: true },
              );
              return;
            }
            const expectation = digitService?.getExpectation(callSid);
            const activePlan = digitService?.getPlan?.(callSid);
            const planStepIndex = Number.isFinite(activePlan?.index)
              ? activePlan.index + 1
              : null;
            console.log(
              `Media DTMF for ${callSid}: ${maskDigitsForLog(digits)} (expectation ${expectation ? "present" : "missing"})`,
            );
            if (!expectation) {
              if (digitService?.bufferDigits) {
                digitService.bufferDigits(callSid, digits, {
                  timestamp: Date.now(),
                  source: "dtmf",
                  early: true,
                  plan_id: activePlan?.id || null,
                  plan_step_index: planStepIndex,
                });
              }
              webhookService.addLiveEvent(
                callSid,
                `🔢 Keypad: ${digits} (buffered early)`,
                { force: true },
              );
              return;
            }
            await digitService.flushBufferedDigits(
              callSid,
              gptService,
              interactionCount,
              "dtmf",
              { allowCallEnd: true },
            );
            if (!digitService?.hasExpectation(callSid)) {
              return;
            }
            const activeExpectation = digitService.getExpectation(callSid);
            const display =
              activeExpectation?.profile === "verification"
                ? digitService.formatOtpForDisplay(
                    digits,
                    "progress",
                    activeExpectation?.max_digits,
                  )
                : `Keypad: ${digits}`;
            webhookService.addLiveEvent(callSid, `🔢 ${display}`, {
              force: true,
            });
            const collection = digitService.recordDigits(callSid, digits, {
              timestamp: Date.now(),
              source: "dtmf",
              attempt_id: activeExpectation?.attempt_id || null,
              plan_id: activeExpectation?.plan_id || null,
              plan_step_index: activeExpectation?.plan_step_index || null,
            });
            await digitService.handleCollectionResult(
              callSid,
              collection,
              gptService,
              interactionCount,
              "dtmf",
              { allowCallEnd: true },
            );
          }
        } else if (event === "stop") {
          console.log(`Adaptive call stream ${streamSid} ended`.red);
          const stopKey = `${callSid || "unknown"}:${streamSid || "unknown"}`;
          if (streamStopSeen.has(stopKey)) {
            console.log(`Duplicate stream stop ignored for ${stopKey}`);
            return;
          }
          streamStopSeen.add(stopKey);
          clearFirstMediaWatchdog(callSid);
          streamFirstMediaSeen.delete(callSid);
          streamStartTimes.delete(callSid);
          if (pendingStreams.has(callSid)) {
            clearTimeout(pendingStreams.get(callSid));
            pendingStreams.delete(callSid);
          }
          if (
            callSid &&
            activeStreamConnections.get(callSid)?.streamSid === streamSid
          ) {
            activeStreamConnections.delete(callSid);
          }

          const activePlan = digitService?.getPlan?.(callSid);
          const isGatherPlan = activePlan?.capture_mode === "ivr_gather";
          if (digitService?.isFallbackActive?.(callSid) || isGatherPlan) {
            const reason = digitService?.isFallbackActive?.(callSid)
              ? "Gather fallback"
              : "IVR gather";
            console.log(
              `📟 Stream stopped during ${reason} for ${callSid}; preserving call state.`,
            );
            const session = activeCalls.get(callSid);
            if (session) {
              queuePersistCallRuntimeState(callSid, {
                interaction_count: session.interactionCount || interactionCount,
                snapshot: {
                  source: "twilio_stream_stop_preserve",
                  reason,
                },
              });
            }
            activeCalls.delete(callSid);
            callToolInFlight.delete(callSid);
            clearCallEndLock(callSid);
            clearSilenceTimer(callSid);
            return;
          }

          const authBypass = streamAuthBypass.get(callSid);
          if (authBypass && !streamFirstMediaSeen.has(callSid)) {
            console.warn(
              `Stream stopped before auth for ${callSid} (${authBypass.reason})`,
            );
            webhookService.addLiveEvent(
              callSid,
              "⚠️ Stream stopped before auth; attempting recovery",
              { force: true },
            );
            await db
              .updateCallState(callSid, "stream_stopped_before_auth", {
                reason: authBypass.reason,
                stream_sid: streamSid || null,
                at: new Date().toISOString(),
              })
              .catch(() => {});
            void handleStreamTimeout(callSid, host, {
              allowHangup: false,
              reason: "stream_auth_failed",
            });
            clearCallEndLock(callSid);
            clearSilenceTimer(callSid);
            return;
          }

          await handleCallEnd(callSid, callStartTime);

          // Clean up
          activeCalls.delete(callSid);
          callToolInFlight.delete(callSid);
          await clearCallRuntimeState(callSid);
          if (callSid && callConfigurations.has(callSid)) {
            callConfigurations.delete(callSid);
            callFunctionSystems.delete(callSid);
            callDirections.delete(callSid);
            console.log(
              `Cleaned up adaptive configuration for call: ${callSid}`,
            );
          }
          if (callSid) {
            streamStartSeen.delete(callSid);
            streamAuthBypass.delete(callSid);
            streamRetryState.delete(callSid);
            purgeStreamStatusDedupe(callSid);
            streamLastMediaAt.delete(callSid);
            sttLastFrameAt.delete(callSid);
            streamWatchdogState.delete(callSid);
          }
          if (digitService) {
            digitService.clearCallState(callSid);
          }
          clearCallEndLock(callSid);
          clearSilenceTimer(callSid);
        } else {
          console.log(
            `Unrecognized WS event for ${callSid || "unknown"}: ${event || "none"}`,
            msg,
          );
        }
      } catch (messageError) {
        console.error("Error processing WebSocket message:", messageError);
      }
    });

    transcriptionService.on("utterance", (text) => {
      clearSilenceTimer(callSid);
      if (callSid) {
        sttLastFrameAt.set(callSid, Date.now());
      }
      if (text && text.trim().length > 0) {
        webhookService
          .setLiveCallPhase(callSid, "user_speaking")
          .catch(() => {});
      }
      if (marks.length > 0 && text?.length > 5) {
        console.log("Interruption detected, clearing stream".red);
        try {
          ws.send(
            JSON.stringify({
              streamSid,
              event: "clear",
            }),
          );
        } catch (error) {
          console.error("WebSocket clear event send error:", error);
        }
      }
    });

    transcriptionService.on("transcription", async (text) => {
      try {
        if (!text || !gptService || !isInitialized) {
          return;
        }
        clearSilenceTimer(callSid);
        if (callSid) {
          sttLastFrameAt.set(callSid, Date.now());
        }

        const callConfig = callConfigurations.get(callSid);
        const isDigitIntent = callConfig?.digit_intent?.mode === "dtmf";
        const captureActive = isCaptureActiveConfig(callConfig);
        const otpContext = digitService.getOtpContext(text, callSid);
        console.log(`Customer: ${otpContext.maskedForLogs}`);

        // Save user transcript with enhanced context
        try {
          await db.addTranscript({
            call_sid: callSid,
            speaker: "user",
            message: otpContext.raw,
            interaction_count: interactionCount,
          });

          await db.updateCallState(callSid, "user_spoke", {
            message: otpContext.raw,
            interaction_count: interactionCount,
            otp_detected: otpContext.otpDetected,
            last_collected_code: otpContext.codes?.slice(-1)[0] || null,
            collected_codes: otpContext.codes?.join(", ") || null,
          });
        } catch (dbError) {
          console.error("Database error adding user transcript:", dbError);
        }

        webhookService.recordTranscriptTurn(callSid, "user", otpContext.raw);
        if (
          (isDigitIntent || captureActive) &&
          otpContext.codes &&
          otpContext.codes.length &&
          digitService?.hasExpectation(callSid)
        ) {
          const activeExpectation = digitService.getExpectation(callSid);
          const progress = digitService.formatOtpForDisplay(
            otpContext.codes[otpContext.codes.length - 1],
            "progress",
            activeExpectation?.max_digits,
          );
          webhookService.addLiveEvent(callSid, `🔢 ${progress}`, { force: true });
          const collection = digitService.recordDigits(
            callSid,
            otpContext.codes[otpContext.codes.length - 1],
            {
              timestamp: Date.now(),
              source: "spoken",
              full_input: true,
              attempt_id: activeExpectation?.attempt_id || null,
              plan_id: activeExpectation?.plan_id || null,
              plan_step_index: activeExpectation?.plan_step_index || null,
              channel_session_id: activeExpectation?.channel_session_id || null,
            },
          );
          await digitService.handleCollectionResult(
            callSid,
            collection,
            gptService,
            interactionCount,
            "spoken",
            { allowCallEnd: true },
          );
        }
        if (captureActive) {
          return;
        }

        if (!otpContext.maskedForGpt || !otpContext.maskedForGpt.trim()) {
          interactionCount += 1;
          const session = activeCalls.get(callSid);
          if (session) {
            session.interactionCount = interactionCount;
          }
          queuePersistCallRuntimeState(callSid, {
            interaction_count: interactionCount,
          });
          return;
        }

        if (
          shouldCloseConversation(otpContext.maskedForGpt) &&
          interactionCount >= 1
        ) {
          await speakAndEndCall(
            callSid,
            CALL_END_MESSAGES.user_goodbye,
            "user_goodbye",
          );
          interactionCount += 1;
          const session = activeCalls.get(callSid);
          if (session) {
            session.interactionCount = interactionCount;
          }
          queuePersistCallRuntimeState(callSid, {
            interaction_count: interactionCount,
          });
          return;
        }

        const getInteractionCount = () => interactionCount;
        const setInteractionCount = (nextCount) => {
          interactionCount = nextCount;
          const session = activeCalls.get(callSid);
          if (session) {
            session.interactionCount = nextCount;
          }
          queuePersistCallRuntimeState(callSid, {
            interaction_count: nextCount,
          });
        };
        if (isDigitIntent) {
          await enqueueGptTask(callSid, async () => {
            const currentCount = interactionCount;
            try {
              await gptService.completion(otpContext.maskedForGpt, currentCount);
            } catch (gptError) {
              console.error("GPT completion error:", gptError);
              webhookService.addLiveEvent(callSid, "⚠️ GPT error, retrying", {
                force: true,
              });
            }
            setInteractionCount(currentCount + 1);
          });
          return;
        }
        await processNormalFlowTranscript(
          callSid,
          otpContext.maskedForGpt,
          gptService,
          getInteractionCount,
          setInteractionCount,
        );
      } catch (transcriptionError) {
        console.error("Transcription handler error:", transcriptionError);
      }
    });

    ttsService.on("speech", (responseIndex, audio, label, icount) => {
      const level = estimateAudioLevelFromBase64(audio);
      webhookService
        .setLiveCallPhase(callSid, "agent_speaking", { level })
        .catch(() => {});
      if (digitService?.hasExpectation(callSid)) {
        digitService.updatePromptDelay(
          callSid,
          estimateAudioDurationMsFromBase64(audio),
        );
      }
      if (callSid) {
        db.updateCallState(callSid, "tts_ready", {
          response_index: responseIndex,
          interaction_count: icount,
          audio_bytes: audio?.length || null,
        }).catch(() => {});
      }
      streamService.buffer(responseIndex, audio);
    });

    streamService.on("audiosent", (markLabel) => {
      marks.push(markLabel);
    });
    streamService.on("audiotick", (tick) => {
      webhookService
        .setLiveCallPhase(callSid, "agent_speaking", {
          level: tick?.level,
          logEvent: false,
        })
        .catch(() => {});
    });

    ws.on("close", () => {
      console.log(
        `WebSocket connection closed for adaptive call: ${callSid || "unknown"}`,
      );
      transcriptionService.close();
      streamService.close();
      if (digitService) {
        digitService.clearCallState(callSid);
      }
      clearSpeechTicks(callSid);
      clearGptQueue(callSid);
      clearNormalFlowState(callSid);
      clearCallEndLock(callSid);
      clearSilenceTimer(callSid);
      sttFallbackCalls.delete(callSid);
      streamTimeoutCalls.delete(callSid);
      streamStartTimes.delete(callSid);
      sttFailureCounts.delete(callSid);
      if (callSid && activeStreamConnections.get(callSid)?.ws === ws) {
        activeStreamConnections.delete(callSid);
      }
      if (callSid) {
        if (pendingStreams.has(callSid)) {
          clearTimeout(pendingStreams.get(callSid));
          pendingStreams.delete(callSid);
        }
        streamStartSeen.delete(callSid);
        streamAuthBypass.delete(callSid);
        streamRetryState.delete(callSid);
        purgeStreamStatusDedupe(callSid);
        streamLastMediaAt.delete(callSid);
        sttLastFrameAt.delete(callSid);
        streamWatchdogState.delete(callSid);
        if (streamSid) {
          streamStopSeen.delete(`${callSid}:${streamSid}`);
        }
      }
    });
  } catch (err) {
    console.error("WebSocket handler error:", err);
  }
});

// Vonage websocket media handler (bidirectional PCM stream)
app.ws("/vonage/stream", async (ws, req) => {
  try {
    const vonageUuid =
      req.query?.uuid || req.query?.conversation_uuid || req.query?.vonage_uuid;
    let callSid = req.query?.callSid;
    if (!callSid && vonageUuid) {
      callSid = await resolveVonageCallSidFromUuid(vonageUuid);
    }
    if (!callSid) {
      console.warn("Vonage websocket missing callSid; closing connection", {
        uuid: vonageUuid || null,
      });
      ws.close();
      return;
    }

    const streamAuth = verifyStreamAuth(callSid, req);
    if (!streamAuth.ok) {
      console.warn("Vonage websocket auth failed", {
        callSid,
        reason: streamAuth.reason || "invalid",
      });
      ws.close();
      return;
    }

    if (vonageUuid) {
      rememberVonageCallMapping(callSid, vonageUuid, "stream_open");
    }

    let interactionCount = 0;
    let callConfig = callConfigurations.get(callSid);
    let functionSystem = callFunctionSystems.get(callSid);
    if (!callConfig) {
      const hydrated = await hydrateCallConfigFromDb(callSid);
      callConfig = hydrated?.callConfig || callConfig;
      functionSystem = hydrated?.functionSystem || functionSystem;
    }
    if (!callConfig && callSid) {
      const callRecord = db?.getCall
        ? await db.getCall(callSid).catch(() => null)
        : null;
      if (callRecord) {
        const setup = ensureCallSetup(callSid, {
          From: callRecord.phone_number || null,
          To: null,
        }, {
          provider: "vonage",
          inbound: callDirections.get(callSid) !== "outbound",
        });
        callConfig = setup.callConfig || callConfig;
        functionSystem = setup.functionSystem || functionSystem;
      }
    }
    if (!callConfig) {
      console.warn(`Vonage websocket missing call configuration for ${callSid}`);
      ws.close();
      return;
    }
    if (!functionSystem) {
      functionSystem = functionEngine.generateAdaptiveFunctionSystem(
        callConfig?.prompt || DEFAULT_INBOUND_PROMPT,
        callConfig?.first_message || DEFAULT_INBOUND_FIRST_MESSAGE,
      );
      callFunctionSystems.set(callSid, functionSystem);
    }
    if (!callConfig.provider_metadata) {
      callConfig.provider_metadata = {};
    }
    if (vonageUuid && callConfig.provider_metadata.vonage_uuid !== vonageUuid) {
      callConfig.provider_metadata.vonage_uuid = String(vonageUuid);
    }
    const directionHint =
      req.query?.direction ||
      req.query?.Direction ||
      callDirections.get(callSid) ||
      (callConfig.inbound ? "inbound" : "outbound");
    const isInboundCall = !isOutboundVonageDirection(directionHint);
    callConfig.provider = "vonage";
    callConfig.inbound = isInboundCall;
    callConfigurations.set(callSid, callConfig);
    callDirections.set(callSid, isInboundCall ? "inbound" : "outbound");
    const runtimeRestore = await restoreCallRuntimeState(callSid, callConfig);
    if (runtimeRestore?.restored) {
      interactionCount = Math.max(
        interactionCount,
        Number(runtimeRestore.interactionCount || 0),
      );
    }

    const vonageAudioSpec = getVonageWebsocketAudioSpec();
    const startedAt = new Date().toISOString();
    if (db?.updateCallStatus) {
      await db
        .updateCallStatus(callSid, "started", { started_at: startedAt })
        .catch(() => {});
    }
    if (db?.updateCallState) {
      await db.updateCallState(callSid, "stream_started", {
        stream_provider: "vonage",
        started_at: startedAt,
        vonage_uuid: vonageUuid || callConfig?.provider_metadata?.vonage_uuid,
        stream_audio_content_type: vonageAudioSpec.contentType,
        stream_audio_encoding: vonageAudioSpec.sttEncoding,
        stream_audio_sample_rate: vonageAudioSpec.sampleRate,
      })
        .catch(() => {});
    }
    console.log(`Vonage stream connected for ${callSid}; using legacy STT+GPT+TTS`);

    const ttsService = new TextToSpeechService({
      encoding: vonageAudioSpec.ttsEncoding,
      sampleRate: vonageAudioSpec.sampleRate,
    });
    ttsService
      .generate(
        { partialResponseIndex: null, partialResponse: "warming up" },
        -1,
        { silent: true },
      )
      .catch(() => {});
    const transcriptionService = new TranscriptionService({
      encoding: vonageAudioSpec.sttEncoding,
      sampleRate: vonageAudioSpec.sampleRate,
    });

    const handleSttFailure = async (tag, error) => {
      if (!callSid) return;
      console.error(
        `STT failure (${tag}) for ${callSid}`,
        error?.message || error || "",
      );
      const session = activeCalls.get(callSid);
      await activateDtmfFallback(
        callSid,
        callConfig,
        gptService,
        session?.interactionCount || interactionCount,
        tag,
      );
    };

    transcriptionService.on("error", (error) => {
      void handleSttFailure("stt_error", error).catch((sttFailureError) => {
        console.error("STT fallback activation error:", sttFailureError);
      });
    });
    transcriptionService.on("close", () => {
      void handleSttFailure("stt_closed").catch((sttFailureError) => {
        console.error("STT fallback activation error:", sttFailureError);
      });
    });

    let gptService;
    if (functionSystem) {
      gptService = new EnhancedGptService(
        callConfig?.prompt,
        callConfig?.first_message,
        {
          db,
          webhookService,
          channel: "voice",
          provider: callConfig?.provider || getCurrentProvider(),
          traceId: `call:${callSid}`,
        },
      );
    } else {
      gptService = new EnhancedGptService(
        callConfig?.prompt,
        callConfig?.first_message,
        {
          db,
          webhookService,
          channel: "voice",
          provider: callConfig?.provider || getCurrentProvider(),
          traceId: `call:${callSid}`,
        },
      );
    }

    gptService.setCallSid(callSid);
    gptService.setExecutionContext({
      traceId: `call:${callSid}`,
      channel: "voice",
      provider: callConfig?.provider || getCurrentProvider(),
    });
    gptService.setCustomerName(
      callConfig?.customer_name || callConfig?.victim_name,
    );
    gptService.setCallProfile(
      callConfig?.purpose || callConfig?.business_context?.purpose,
    );
    gptService.setPersonaContext({
      domain: callConfig?.purpose || callConfig?.business_context?.purpose || "general",
      channel: "voice",
      urgency: callConfig?.urgency || "normal",
    });
    const intentLine = `Call intent: ${callConfig?.script || "general"} | purpose: ${callConfig?.purpose || "general"} | business: ${callConfig?.business_context?.business_id || callConfig?.business_id || "unspecified"}. Keep replies concise and on-task.`;
    gptService.setCallIntent(intentLine);
    await applyInitialDigitIntent(
      callSid,
      callConfig,
      gptService,
      interactionCount,
    );
    configureCallTools(gptService, callSid, callConfig, functionSystem);

    activeCalls.set(callSid, {
      startTime: new Date(),
      transcripts: [],
      gptService,
      callConfig,
      functionSystem,
      personalityChanges: [],
      ws,
      ttsService,
      interactionCount,
    });
    queuePersistCallRuntimeState(callSid, {
      interaction_count: interactionCount,
      snapshot: { source: "vonage_stream_start" },
    });
    clearKeypadCallState(callSid);
    scheduleVonageKeypadDtmfWatchdog(callSid, callConfig);

    let gptErrorCount = 0;
    gptService.on("gptreply", async (gptReply, icount) => {
      try {
        gptErrorCount = 0;
        markGptReplyProgress(callSid);
        const activeSession = activeCalls.get(callSid);
        if (activeSession?.ending) {
          return;
        }
        webhookService.recordTranscriptTurn(
          callSid,
          "agent",
          gptReply.partialResponse,
        );
        webhookService
          .setLiveCallPhase(callSid, "agent_responding")
          .catch(() => {});
        try {
          await db.addTranscript({
            call_sid: callSid,
            speaker: "ai",
            message: gptReply.partialResponse,
            interaction_count: icount,
            personality_used: gptReply.personalityInfo?.name || "default",
            adaptation_data: JSON.stringify(gptReply.adaptationHistory || []),
          });
          await db.updateCallState(callSid, "ai_responded", {
            message: gptReply.partialResponse,
            interaction_count: icount,
          });
        } catch (dbError) {
          console.error("Database error adding AI transcript:", dbError);
        }

        await ttsService.generate(gptReply, icount);
        scheduleSilenceTimer(callSid);
      } catch (gptReplyError) {
        console.error("Vonage GPT reply handler error:", gptReplyError);
      }
    });

    gptService.on("stall", (fillerText) => {
      handleGptStall(callSid, fillerText, (speechText) => {
        try {
          ttsService.generate(
            {
              partialResponse: speechText,
              personalityInfo: { name: "filler" },
              adaptationHistory: [],
            },
            interactionCount,
          );
        } catch (err) {
          console.error("Filler TTS error:", err);
        }
      });
    });

    gptService.on("gpterror", async (err) => {
      try {
        gptErrorCount += 1;
        const message = err?.message || "GPT error";
        webhookService.addLiveEvent(callSid, `⚠️ GPT error: ${message}`, {
          force: true,
        });
        if (gptErrorCount >= 2) {
          await speakAndEndCall(callSid, CALL_END_MESSAGES.error, "gpt_error");
        }
      } catch (gptHandlerError) {
        console.error("GPT error handler failure:", gptHandlerError);
      }
    });

    ttsService.on("speech", (responseIndex, audio) => {
      const level = estimateAudioLevelFromBase64(audio);
      webhookService
        .setLiveCallPhase(callSid, "agent_speaking", { level })
        .catch(() => {});
      scheduleSpeechTicksFromAudio(callSid, "agent_speaking", audio);
      if (digitService?.hasExpectation(callSid)) {
        digitService.updatePromptDelay(
          callSid,
          estimateAudioDurationMsFromBase64(audio),
        );
      }
      if (callSid) {
        db.updateCallState(callSid, "tts_ready", {
          response_index: responseIndex,
          interaction_count: interactionCount,
          audio_bytes: audio?.length || null,
          provider: "vonage",
        }).catch(() => {});
      }
      try {
        const buffer = Buffer.from(audio, "base64");
        ws.send(buffer);
      } catch (error) {
        console.error("Vonage websocket send error:", error);
      }
    });

    transcriptionService.on("utterance", (text) => {
      clearSilenceTimer(callSid);
      if (text && text.trim().length > 0) {
        webhookService
          .setLiveCallPhase(callSid, "user_speaking")
          .catch(() => {});
      }
    });

    transcriptionService.on("transcription", async (text) => {
      try {
        if (!text) return;
        clearSilenceTimer(callSid);
        const callConfig = callConfigurations.get(callSid);
        const isDigitIntent = callConfig?.digit_intent?.mode === "dtmf";
        const captureActive = isCaptureActiveConfig(callConfig);
        const otpContext = digitService.getOtpContext(text, callSid);
        try {
          await db.addTranscript({
            call_sid: callSid,
            speaker: "user",
            message: otpContext.raw,
            interaction_count: interactionCount,
          });
          await db.updateCallState(callSid, "user_spoke", {
            message: otpContext.raw,
            interaction_count: interactionCount,
            otp_detected: otpContext.otpDetected,
            last_collected_code: otpContext.codes?.slice(-1)[0] || null,
            collected_codes: otpContext.codes?.join(", ") || null,
          });
        } catch (dbError) {
          console.error("Database error adding user transcript:", dbError);
        }
        webhookService.recordTranscriptTurn(callSid, "user", otpContext.raw);
        if (
          (isDigitIntent || captureActive) &&
          otpContext.codes &&
          otpContext.codes.length &&
          digitService?.hasExpectation(callSid)
        ) {
          const activeExpectation = digitService.getExpectation(callSid);
          const progress = digitService.formatOtpForDisplay(
            otpContext.codes[otpContext.codes.length - 1],
            "progress",
            activeExpectation?.max_digits,
          );
          webhookService.addLiveEvent(callSid, `🔢 ${progress}`, { force: true });
          const collection = digitService.recordDigits(
            callSid,
            otpContext.codes[otpContext.codes.length - 1],
            {
              timestamp: Date.now(),
              source: "spoken",
              full_input: true,
              attempt_id: activeExpectation?.attempt_id || null,
              plan_id: activeExpectation?.plan_id || null,
              plan_step_index: activeExpectation?.plan_step_index || null,
              channel_session_id: activeExpectation?.channel_session_id || null,
            },
          );
          await digitService.handleCollectionResult(
            callSid,
            collection,
            gptService,
            interactionCount,
            "spoken",
            { allowCallEnd: true },
          );
        }
        if (captureActive) {
          return;
        }
        if (!otpContext.maskedForGpt || !otpContext.maskedForGpt.trim()) {
          interactionCount += 1;
          const session = activeCalls.get(callSid);
          if (session) {
            session.interactionCount = interactionCount;
          }
          queuePersistCallRuntimeState(callSid, {
            interaction_count: interactionCount,
          });
          return;
        }
        if (
          shouldCloseConversation(otpContext.maskedForGpt) &&
          interactionCount >= 1
        ) {
          await speakAndEndCall(
            callSid,
            CALL_END_MESSAGES.user_goodbye,
            "user_goodbye",
          );
          interactionCount += 1;
          const session = activeCalls.get(callSid);
          if (session) {
            session.interactionCount = interactionCount;
          }
          queuePersistCallRuntimeState(callSid, {
            interaction_count: interactionCount,
          });
          return;
        }
        const getInteractionCount = () => interactionCount;
        const setInteractionCount = (nextCount) => {
          interactionCount = nextCount;
          const session = activeCalls.get(callSid);
          if (session) {
            session.interactionCount = nextCount;
          }
          queuePersistCallRuntimeState(callSid, {
            interaction_count: nextCount,
          });
        };
        if (isDigitIntent) {
          await enqueueGptTask(callSid, async () => {
            const currentCount = interactionCount;
            try {
              await gptService.completion(otpContext.maskedForGpt, currentCount);
            } catch (gptError) {
              console.error("GPT completion error:", gptError);
              webhookService.addLiveEvent(callSid, "⚠️ GPT error, retrying", {
                force: true,
              });
            }
            setInteractionCount(currentCount + 1);
          });
          return;
        }
        await processNormalFlowTranscript(
          callSid,
          otpContext.maskedForGpt,
          gptService,
          getInteractionCount,
          setInteractionCount,
        );
      } catch (transcriptionError) {
        console.error("Vonage transcription handler error:", transcriptionError);
      }
    });

    ws.on("message", (data) => {
      if (!data) return;
      if (Buffer.isBuffer(data)) {
        transcriptionService.sendBuffer(data);
        return;
      }
      const str = data.toString();
      try {
        const parsed = JSON.parse(str);
        const wsDigits = getVonageDtmfDigits(parsed || {});
        if (wsDigits) {
          handleExternalDtmfInput(callSid, wsDigits, {
            source: "vonage_ws_dtmf",
            provider: "vonage",
            gptService,
            interactionCount,
          }).catch((error) => {
            console.error("Vonage websocket DTMF handling error:", error);
          });
          return;
        }
        if (parsed?.event === "websocket:closed") {
          ws.close();
        }
      } catch {
        // ignore non-JSON
      }
    });

    ws.on("close", async () => {
      transcriptionService.close();
      try {
        const session = activeCalls.get(callSid);
        if (session?.startTime) {
          await handleCallEnd(callSid, session.startTime);
        }
      } catch (closeError) {
        console.error("Vonage websocket close handler error:", closeError);
      } finally {
        activeCalls.delete(callSid);
        callToolInFlight.delete(callSid);
        await clearCallRuntimeState(callSid);
        if (digitService) {
          digitService.clearCallState(callSid);
        }
        clearSpeechTicks(callSid);
        clearGptQueue(callSid);
        clearNormalFlowState(callSid);
        clearCallEndLock(callSid);
        clearSilenceTimer(callSid);
        sttFallbackCalls.delete(callSid);
        streamTimeoutCalls.delete(callSid);
      }
    });

    // Send first message once stream is ready
    const initialExpectation = digitService?.getExpectation(callSid);
    const firstMessage =
      callConfig?.first_message ||
      (initialExpectation
        ? digitService.buildDigitPrompt(initialExpectation)
        : "");
    if (firstMessage) {
      ttsService.generate(
        { partialResponseIndex: null, partialResponse: firstMessage },
        0,
      );
      webhookService.recordTranscriptTurn(callSid, "agent", firstMessage);
      if (digitService?.hasExpectation(callSid)) {
        digitService.markDigitPrompted(callSid, gptService, 0, "dtmf", {
          allowCallEnd: true,
          prompt_text: firstMessage,
        });
        digitService.scheduleDigitTimeout(callSid, gptService, 0);
      }
      scheduleSilenceTimer(callSid);
    }
  } catch (error) {
    console.error("Vonage websocket error:", error);
    ws.close();
  }
});

// AWS websocket media handler (external audio forwarder -> Deepgram -> GPT -> Polly)
app.ws("/aws/stream", (ws, req) => {
  try {
    const callSid = req.query?.callSid;
    const contactId = req.query?.contactId;
    if (!callSid || !contactId) {
      ws.close();
      return;
    }

    const awsWebhookMode = String(config.aws?.webhookValidation || "warn")
      .toLowerCase()
      .trim();
    if (awsWebhookMode !== "off") {
      const authResult = verifyAwsStreamAuth(callSid, req);
      if (!authResult.ok) {
        console.warn("AWS websocket auth failed", {
          callSid,
          contactId,
          reason: authResult.reason || "unknown",
        });
        if (awsWebhookMode === "strict") {
          ws.close();
          return;
        }
      }
    }

    const callConfig = callConfigurations.get(callSid);
    if (!callConfig) {
      ws.close();
      return;
    }

    if (!callConfig.provider_metadata) {
      callConfig.provider_metadata = {};
    }
    if (!callConfig.provider_metadata.contact_id) {
      callConfig.provider_metadata.contact_id = contactId;
    }
    awsContactMap.set(contactId, callSid);

    const sampleRate = Number(req.query?.sampleRate) || 16000;
    const encoding = req.query?.encoding || "pcm";

    const transcriptionService = new TranscriptionService({
      encoding: encoding,
      sampleRate: sampleRate,
    });

    const handleSttFailure = async (tag, error) => {
      if (!callSid) return;
      console.error(
        `STT failure (${tag}) for ${callSid}`,
        error?.message || error || "",
      );
      const session = activeCalls.get(callSid);
      await activateDtmfFallback(
        callSid,
        session?.callConfig || callConfig,
        session?.gptService,
        session?.interactionCount || interactionCount,
        tag,
      );
    };

    transcriptionService.on("error", (error) => {
      void handleSttFailure("stt_error", error).catch((sttFailureError) => {
        console.error("STT fallback activation error:", sttFailureError);
      });
    });
    transcriptionService.on("close", () => {
      void handleSttFailure("stt_closed").catch((sttFailureError) => {
        console.error("STT fallback activation error:", sttFailureError);
      });
    });

    const sessionPromise = ensureAwsSession(callSid).catch((sessionError) => {
      console.error("Failed to initialize AWS call session:", sessionError);
      try {
        ws.close();
      } catch {}
      return null;
    });
    let interactionCount = 0;

    transcriptionService.on("utterance", (text) => {
      clearSilenceTimer(callSid);
      if (text && text.trim().length > 0) {
        webhookService
          .setLiveCallPhase(callSid, "user_speaking")
          .catch(() => {});
      }
    });

    transcriptionService.on("transcription", async (text) => {
      try {
        if (!text) return;
        clearSilenceTimer(callSid);
        const session = await sessionPromise;
        if (!session?.gptService) {
          return;
        }
        interactionCount = Math.max(
          interactionCount,
          Number(session.interactionCount || 0),
        );
        const isDigitIntent = session?.callConfig?.digit_intent?.mode === "dtmf";
        const captureActive = isCaptureActiveConfig(session?.callConfig);
        const otpContext = digitService.getOtpContext(text, callSid);
        try {
          await db.addTranscript({
            call_sid: callSid,
            speaker: "user",
            message: otpContext.raw,
            interaction_count: interactionCount,
          });
          await db.updateCallState(callSid, "user_spoke", {
            message: otpContext.raw,
            interaction_count: interactionCount,
            otp_detected: otpContext.otpDetected,
            last_collected_code: otpContext.codes?.slice(-1)[0] || null,
            collected_codes: otpContext.codes?.join(", ") || null,
          });
        } catch (dbError) {
          console.error("Database error adding user transcript:", dbError);
        }

        webhookService.recordTranscriptTurn(callSid, "user", otpContext.raw);
        if (
          (isDigitIntent || captureActive) &&
          otpContext.codes &&
          otpContext.codes.length &&
          digitService?.hasExpectation(callSid)
        ) {
          const activeExpectation = digitService.getExpectation(callSid);
          const progress = digitService.formatOtpForDisplay(
            otpContext.codes[otpContext.codes.length - 1],
            "progress",
            activeExpectation?.max_digits,
          );
          webhookService.addLiveEvent(callSid, `🔢 ${progress}`, { force: true });
          const collection = digitService.recordDigits(
            callSid,
            otpContext.codes[otpContext.codes.length - 1],
            {
              timestamp: Date.now(),
              source: "spoken",
              full_input: true,
              attempt_id: activeExpectation?.attempt_id || null,
              plan_id: activeExpectation?.plan_id || null,
              plan_step_index: activeExpectation?.plan_step_index || null,
              channel_session_id: activeExpectation?.channel_session_id || null,
            },
          );
          await digitService.handleCollectionResult(
            callSid,
            collection,
            session.gptService,
            interactionCount,
            "spoken",
            { allowCallEnd: true },
          );
        }
        if (captureActive) {
          return;
        }

        if (
          shouldCloseConversation(otpContext.maskedForGpt) &&
          interactionCount >= 1
        ) {
          await speakAndEndCall(
            callSid,
            CALL_END_MESSAGES.user_goodbye,
            "user_goodbye",
          );
          interactionCount += 1;
          session.interactionCount = interactionCount;
          queuePersistCallRuntimeState(callSid, {
            interaction_count: interactionCount,
          });
          return;
        }

        const getInteractionCount = () => interactionCount;
        const setInteractionCount = (nextCount) => {
          interactionCount = nextCount;
          session.interactionCount = nextCount;
          queuePersistCallRuntimeState(callSid, {
            interaction_count: nextCount,
          });
        };
        if (isDigitIntent) {
          await enqueueGptTask(callSid, async () => {
            const currentCount = interactionCount;
            try {
              await session.gptService.completion(
                otpContext.maskedForGpt,
                currentCount,
              );
            } catch (gptError) {
              console.error("GPT completion error:", gptError);
              webhookService.addLiveEvent(callSid, "⚠️ GPT error, retrying", {
                force: true,
              });
            }
            setInteractionCount(currentCount + 1);
          });
          return;
        }
        await processNormalFlowTranscript(
          callSid,
          otpContext.maskedForGpt,
          session.gptService,
          getInteractionCount,
          setInteractionCount,
        );
      } catch (transcriptionError) {
        console.error("AWS transcription handler error:", transcriptionError);
      }
    });

    ws.on("message", (data) => {
      if (!data) return;
      if (Buffer.isBuffer(data)) {
        transcriptionService.sendBuffer(data);
        return;
      }
      const str = data.toString();
      try {
        const payload = JSON.parse(str);
        if (payload?.audio) {
          transcriptionService.send(payload.audio);
        }
      } catch {
        // ignore non-JSON text frames
      }
    });

    ws.on("close", async () => {
      transcriptionService.close();
      try {
        const session = activeCalls.get(callSid);
        if (session?.startTime) {
          await handleCallEnd(callSid, session.startTime);
        }
      } catch (closeError) {
        console.error("AWS websocket close handler error:", closeError);
      } finally {
        activeCalls.delete(callSid);
        callToolInFlight.delete(callSid);
        await clearCallRuntimeState(callSid);
        if (digitService) {
          digitService.clearCallState(callSid);
        }
        clearGptQueue(callSid);
        clearNormalFlowState(callSid);
        clearCallEndLock(callSid);
        clearSilenceTimer(callSid);
        sttFallbackCalls.delete(callSid);
        streamTimeoutCalls.delete(callSid);
      }
    });

    recordCallStatus(callSid, "in-progress", "call_in_progress").catch(
      () => {},
    );
  } catch (error) {
    console.error("AWS websocket error:", error);
    ws.close();
  }
});

// Enhanced call end handler with adaptation analytics
async function handleCallEnd(callSid, callStartTime) {
  try {
    const callEndTime = new Date();
    const duration = Math.round((callEndTime - callStartTime) / 1000);
    for (const key of gatherEventDedupe.keys()) {
      if (key.startsWith(`${callSid}:`)) {
        gatherEventDedupe.delete(key);
      }
    }
    clearGptQueue(callSid);
    clearNormalFlowState(callSid);
    clearSpeechTicks(callSid);
    sttFallbackCalls.delete(callSid);
    streamTimeoutCalls.delete(callSid);
    clearFirstMediaWatchdog(callSid);
    streamFirstMediaSeen.delete(callSid);
    streamLastMediaAt.delete(callSid);
    sttLastFrameAt.delete(callSid);
    streamWatchdogState.delete(callSid);
    streamStartSeen.delete(callSid);
    streamAuthBypass.delete(callSid);
    streamRetryState.delete(callSid);
    clearKeypadCallState(callSid);
    purgeStreamStatusDedupe(callSid);
    purgeCallStatusDedupe(callSid);
    callLifecycle.delete(callSid);
    const lifecycleTimer = callLifecycleCleanupTimers.get(callSid);
    if (lifecycleTimer) {
      clearTimeout(lifecycleTimer);
      callLifecycleCleanupTimers.delete(callSid);
    }
    const terminalStatuses = new Set([
      "completed",
      "no-answer",
      "no_answer",
      "busy",
      "failed",
      "canceled",
    ]);
    const normalizeStatus = (value) =>
      String(value || "")
        .toLowerCase()
        .replace(/_/g, "-");
    const initialCallDetails = await db.getCall(callSid);
    const persistedStatus = normalizeStatus(
      initialCallDetails?.status || initialCallDetails?.twilio_status,
    );
    const finalStatus = terminalStatuses.has(persistedStatus)
      ? persistedStatus
      : "completed";
    const notificationMap = {
      completed: "call_completed",
      "no-answer": "call_no_answer",
      busy: "call_busy",
      failed: "call_failed",
      canceled: "call_canceled",
    };
    const notificationType = notificationMap[finalStatus] || "call_completed";
    if (digitService) {
      digitService.clearCallState(callSid);
    }
    clearCallEndLock(callSid);
    clearSilenceTimer(callSid);

    const transcripts = (await db.getCallTranscripts(callSid)) || [];
    const summary = generateCallSummary(transcripts, duration);
    const digitEvents = await db.getCallDigits(callSid).catch(() => []);
    const digitSummary = buildDigitSummary(digitEvents);
    const digitFunnel = buildDigitFunnelStats(digitEvents);

    // Get personality adaptation data
    const callSession = activeCalls.get(callSid);
    let adaptationAnalysis = {};

    if (callSession && callSession.gptService) {
      const conversationAnalysis =
        callSession.gptService.getConversationAnalysis();
      adaptationAnalysis = {
        personalityChanges: conversationAnalysis.personalityChanges,
        finalPersonality: conversationAnalysis.currentPersonality,
        adaptationEffectiveness:
          conversationAnalysis.personalityChanges /
          Math.max(conversationAnalysis.totalInteractions / 10, 1),
        businessContext: callSession.functionSystem?.context || {},
      };
    }

    await db.updateCallStatus(callSid, finalStatus, {
      ended_at: callEndTime.toISOString(),
      duration: duration,
      call_summary: summary.summary,
      ai_analysis: JSON.stringify({
        ...summary.analysis,
        adaptation: adaptationAnalysis,
      }),
      digit_summary: digitSummary.summary,
      digit_count: digitSummary.count,
    });

    await db.updateCallState(callSid, "call_ended", {
      end_time: callEndTime.toISOString(),
      duration: duration,
      total_interactions: transcripts.length,
      personality_adaptations: adaptationAnalysis.personalityChanges || 0,
    });
    if (digitFunnel) {
      await db
        .updateCallState(callSid, "digit_funnel_summary", digitFunnel)
        .catch(() => {});
    }

    const callDetails = await db.getCall(callSid);

    // Create enhanced webhook notification for completion
    if (callDetails && callDetails.user_chat_id) {
      if (callDetails.last_otp) {
        const masked = digitService
          ? digitService.formatOtpForDisplay(callDetails.last_otp, "masked")
          : callDetails.last_otp;
        const otpMsg = `🔐 ${masked} (call ${callSid.slice(-6)})`;
        try {
          await webhookService.sendTelegramMessage(
            callDetails.user_chat_id,
            otpMsg,
          );
        } catch (err) {
          console.error("Error sending OTP to user:", err);
        }
      }

      if (digitEvents && digitEvents.length) {
        const lines = digitEvents
          .filter((d) => d.digits)
          .map((d) => {
            const label = DIGIT_PROFILE_LABELS[d.profile] || d.profile;
            const display = digitService
              ? digitService.formatDigitsGeneral(d.digits, null, "notify")
              : d.digits;
            const src = d.source || "unknown";
            return `• ${label} [${src}]: ${display}`;
          });
        // Suppressed verbose digit timeline to avoid leaking sensitive digits in notifications
      }
      await db.createEnhancedWebhookNotification(
        callSid,
        notificationType,
        callDetails.user_chat_id,
      );

      const hasTranscriptText = transcripts.some(
        (entry) => String(entry?.message || "").trim().length > 0,
      );
      let hasTranscriptAudio = Boolean(
        String(
          callDetails?.transcript_audio_url ||
            callDetails?.recording_url ||
            callDetails?.audio_url ||
            "",
        ).trim(),
      );
      if (!hasTranscriptAudio) {
        const recentStates = await db.getCallStates(callSid, { limit: 40 }).catch(() => []);
        hasTranscriptAudio = recentStates.some((state) => {
          const data =
            state?.data && typeof state.data === "object" && !Array.isArray(state.data)
              ? state.data
              : {};
          return Boolean(
            String(
              data.transcript_audio_url ||
                data.recording_url ||
                data.audio_url ||
                data.media_url ||
                data.url ||
                "",
            ).trim(),
          );
        });
      }

      // Schedule transcript notification whenever transcript text/audio is available.
      if (hasTranscriptText || hasTranscriptAudio) {
        setTimeout(async () => {
          try {
            await db.createEnhancedWebhookNotification(
              callSid,
              "call_transcript",
              callDetails.user_chat_id,
            );
          } catch (transcriptError) {
            console.error(
              "Error creating transcript notification:",
              transcriptError,
            );
          }
        }, 2000);
      }
    }

    const inboundConfig = callConfigurations.get(callSid);
    if (inboundConfig?.inbound && callDetails?.user_chat_id) {
      const normalizedStatus = normalizeCallStatus(
        callDetails.status || callDetails.twilio_status || finalStatus,
      );
      webhookService
        .sendCallStatusUpdate(
          callSid,
          normalizedStatus,
          callDetails.user_chat_id,
          {
            duration,
            ring_duration: callDetails.ring_duration,
            answered_by: callDetails.answered_by,
            status_source: "stream",
          },
        )
        .catch((err) => console.error("Inbound terminal update error:", err));
    }

    console.log(`Enhanced adaptive call ${callSid} ended (${finalStatus})`);
    console.log(
      `Duration: ${duration}s | Messages: ${transcripts.length} | Adaptations: ${adaptationAnalysis.personalityChanges || 0}`,
    );
    if (adaptationAnalysis.finalPersonality) {
      console.log(`Final personality: ${adaptationAnalysis.finalPersonality}`);
    }

    // Log service health
    await db.logServiceHealth("call_system", `call_${finalStatus}`, {
      call_sid: callSid,
      duration: duration,
      interactions: transcripts.length,
      adaptations: adaptationAnalysis.personalityChanges || 0,
    });
  } catch (error) {
    console.error("Error handling enhanced adaptive call end:", error);

    // Log error to service health
    try {
      await db.logServiceHealth("call_system", "error", {
        operation: "handle_call_end",
        call_sid: callSid,
        error: error.message,
      });
    } catch (logError) {
      console.error("Failed to log service health error:", logError);
    }
  } finally {
    callToolInFlight.delete(callSid);
    await clearCallRuntimeState(callSid);
  }
}

function generateCallSummary(transcripts, duration) {
  if (!transcripts || transcripts.length === 0) {
    return {
      summary: "No conversation recorded",
      analysis: { total_messages: 0, user_messages: 0, ai_messages: 0 },
    };
  }

  const userMessages = transcripts.filter((t) => t.speaker === "user");
  const aiMessages = transcripts.filter((t) => t.speaker === "ai");

  const analysis = {
    total_messages: transcripts.length,
    user_messages: userMessages.length,
    ai_messages: aiMessages.length,
    duration_seconds: duration,
    conversation_turns: Math.max(userMessages.length, aiMessages.length),
  };

  const summary =
    `Enhanced adaptive call completed with ${transcripts.length} messages over ${Math.round(duration / 60)} minutes. ` +
    `User spoke ${userMessages.length} times, AI responded ${aiMessages.length} times.`;

  return { summary, analysis };
}

async function handleTwilioIncoming(req, res) {
  try {
    if (!requireValidTwilioSignature(req, res, "/incoming")) {
      return;
    }
    const host = resolveHost(req);
    if (!host) {
      return res.status(500).send("Server hostname not configured");
    }
    const maskedFrom = maskPhoneForLog(req.body?.From);
    const maskedTo = maskPhoneForLog(req.body?.To);
    console.log(
      `Incoming call webhook (${req.method}) from ${maskedFrom} to ${maskedTo} host=${host}`,
    );
    const callSid = req.body?.CallSid;
    const directionRaw = req.body?.Direction || req.body?.direction;
    const isOutbound = isOutboundTwilioDirection(directionRaw);
    const directionLabel = isOutbound ? "outbound" : "inbound";
    if (callSid) {
      callDirections.set(callSid, directionLabel);
      if (!isOutbound) {
        await refreshInboundDefaultScript();
        const callRecord = await ensureCallRecord(
          callSid,
          req.body,
          "incoming_webhook",
          {
            provider: "twilio",
            inbound: true,
          },
        );
        const chatId = callRecord?.user_chat_id || config.telegram?.adminChatId;
        const callerLookup = callRecord?.phone_number
          ? normalizePhoneForFlag(callRecord.phone_number) ||
            callRecord.phone_number
          : null;
        const callerFlag = callerLookup
          ? await db.getCallerFlag(callerLookup).catch(() => null)
          : null;
        if (callerFlag?.status !== "allowed") {
          const rateLimit = shouldRateLimitInbound(req, req.body || {});
          if (rateLimit.limited) {
            await db
              .updateCallState(callSid, "inbound_rate_limited", {
                at: new Date().toISOString(),
                key: rateLimit.key,
                count: rateLimit.count,
                reset_at: rateLimit.resetAt,
              })
              .catch(() => {});
            if (chatId) {
              webhookService
                .sendCallStatusUpdate(callSid, "failed", chatId, {
                  status_source: "rate_limit",
                })
                .catch((err) =>
                  console.error("Inbound rate limit update error:", err),
                );
              webhookService.addLiveEvent(
                callSid,
                "⛔ Inbound rate limit reached",
                { force: true },
              );
            }
            if (
              config.inbound?.rateLimitSmsEnabled &&
              callRecord?.phone_number
            ) {
              try {
                const smsBody = buildInboundSmsBody(
                  callRecord,
                  await db
                    .getLatestCallState(callSid, "call_created")
                    .catch(() => null),
                );
                await smsService.sendSMS(callRecord.phone_number, smsBody);
                await db
                  .updateCallState(callSid, "rate_limit_sms_sent", {
                    at: new Date().toISOString(),
                  })
                  .catch(() => {});
              } catch (smsError) {
                console.error("Failed to send rate-limit SMS:", smsError);
              }
            }
            if (
              config.inbound?.rateLimitCallbackEnabled &&
              callRecord?.phone_number
            ) {
              try {
                const callState = await db
                  .getLatestCallState(callSid, "call_created")
                  .catch(() => null);
                const payload = buildCallbackPayload(callRecord, callState);
                const delayMin = Math.max(
                  1,
                  Number(config.inbound?.callbackDelayMinutes) || 15,
                );
                const runAt = new Date(
                  Date.now() + delayMin * 60 * 1000,
                ).toISOString();
                await scheduleCallJob("callback_call", payload, runAt);
                await db
                  .updateCallState(callSid, "callback_scheduled", {
                    at: new Date().toISOString(),
                    run_at: runAt,
                  })
                  .catch(() => {});
              } catch (callbackError) {
                console.error("Failed to schedule callback:", callbackError);
              }
            }
            const limitedResponse = new VoiceResponse();
            limitedResponse.say(
              "We are experiencing high call volume. Please try again later.",
            );
            limitedResponse.hangup();
            res.type("text/xml");
            res.end(limitedResponse.toString());
            return;
          }
        }
        if (callerFlag?.status === "blocked") {
          if (chatId) {
            webhookService
              .sendCallStatusUpdate(callSid, "failed", chatId, {
                status_source: "blocked",
              })
              .catch((err) =>
                console.error("Blocked caller update error:", err),
              );
          }
          await db
            .updateCallState(callSid, "caller_blocked", {
              at: new Date().toISOString(),
              phone_number: callerLookup || callRecord?.phone_number || null,
              status: callerFlag.status,
              note: callerFlag.note || null,
            })
            .catch(() => {});
          const blockedResponse = new VoiceResponse();
          blockedResponse.say("We cannot take your call at this time.");
          blockedResponse.hangup();
          res.type("text/xml");
          res.end(blockedResponse.toString());
          return;
        }
        if (chatId) {
          webhookService
            .sendCallStatusUpdate(callSid, "ringing", chatId, {
              status_source: "inbound",
            })
            .catch((err) =>
              console.error("Inbound ringing update error:", err),
            );
        }

        const gateStatus =
          webhookService.getInboundGate?.(callSid)?.status || "pending";
        const answerOverride = ["1", "true", "yes"].includes(
          String(req.query?.answer || "").toLowerCase(),
        );
        if (gateStatus === "declined") {
          const declinedResponse = new VoiceResponse();
          declinedResponse.hangup();
          res.type("text/xml");
          res.end(declinedResponse.toString());
          return;
        }
        if (!answerOverride && gateStatus !== "answered") {
          const holdTwiml = buildInboundHoldTwiml(host);
          res.type("text/xml");
          res.end(holdTwiml);
          return;
        }
      }
      const timeoutMs = 30000;
      const timeout = setTimeout(async () => {
        pendingStreams.delete(callSid);
        if (activeCalls.has(callSid)) {
          return;
        }
        let statusValue = "unknown";
        try {
          const callDetails = await db?.getCall?.(callSid);
          statusValue = normalizeCallStatus(
            callDetails?.status || callDetails?.twilio_status,
          );
          if (
            !callDetails?.started_at &&
            !["answered", "in-progress", "completed"].includes(statusValue)
          ) {
            console.warn(
              `Stream not established for ${callSid} yet (status=${statusValue || "unknown"}).`,
            );
            return;
          }
        } catch (err) {
          console.warn(
            `Stream status check failed for ${callSid}: ${err?.message || err}`,
          );
        }
        console.warn(
          `Stream not established for ${callSid} after ${timeoutMs}ms (status=${statusValue || "unknown"}).`,
        );
        webhookService.addLiveEvent(
          callSid,
          "⚠️ Stream not connected yet. Attempting recovery…",
          { force: true },
        );
        void handleStreamTimeout(callSid, host, {
          allowHangup: false,
          reason: "stream_not_connected",
        });
      }, timeoutMs);
      pendingStreams.set(callSid, timeout);
    }
    const response = new VoiceResponse();
    if (!isOutbound) {
      const preconnectMessage = String(
        config.inbound?.preConnectMessage || "",
      ).trim();
      const pauseSeconds = Math.max(
        0,
        Math.min(
          10,
          Math.round(Number(config.inbound?.preConnectPauseSeconds) || 0),
        ),
      );
      if (preconnectMessage) {
        response.say(preconnectMessage);
        if (pauseSeconds > 0) {
          response.pause({ length: pauseSeconds });
        }
      }
    }
    const connect = response.connect();
    const streamParameters = {};
    if (req.body?.From) streamParameters.from = String(req.body.From);
    if (req.body?.To) streamParameters.to = String(req.body.To);
    streamParameters.direction = directionLabel;
    if (callSid && config.streamAuth?.secret) {
      const timestamp = String(Date.now());
      const token = buildStreamAuthToken(callSid, timestamp);
      if (token) {
        streamParameters.token = token;
        streamParameters.ts = timestamp;
      }
    }
    // Request both audio + DTMF events from Twilio Media Streams
    const streamNode = connect.stream({
      url: `wss://${host}/connection`,
      track: TWILIO_STREAM_TRACK,
      statusCallback: `https://${host}/webhook/twilio-stream`,
      statusCallbackMethod: "POST",
      statusCallbackEvent: ["start", "end"],
    });
    for (const [name, value] of Object.entries(streamParameters)) {
      if (value === undefined || value === null || value === "") continue;
      if (typeof streamNode?.parameter === "function") {
        streamNode.parameter({ name, value: String(value) });
      }
    }

    res.type("text/xml");
    res.end(response.toString());
  } catch (err) {
    console.log(err);
    res.status(500).send("Error");
  }
}

// Incoming endpoint used by Twilio to connect the call to our websocket stream
app.post("/incoming", handleTwilioIncoming);
app.get("/incoming", handleTwilioIncoming);

function buildVonageUnavailableNcco() {
  return [
    {
      action: "talk",
      text: "We are unable to connect this call right now. Please try again shortly.",
    },
    { action: "hangup" },
  ];
}

app.post("/aws/transcripts", async (req, res) => {
  try {
    if (!requireValidAwsWebhook(req, res, "/aws/transcripts")) {
      return;
    }
    const { callSid, transcript, isPartial } = req.body || {};
    const normalizedCallSid = String(callSid || "").trim();
    const normalizedTranscript = String(transcript || "").trim();
    const partialFlag =
      isPartial === true ||
      isPartial === 1 ||
      String(isPartial || "").toLowerCase() === "true";
    if (
      !normalizedCallSid ||
      !normalizedTranscript ||
      !isSafeId(normalizedCallSid, { max: 128 })
    ) {
      return res
        .status(400)
        .json({ success: false, error: "callSid and transcript required" });
    }
    if (normalizedTranscript.length > 4000) {
      return res.status(400).json({
        success: false,
        error: "transcript too long",
      });
    }
    if (partialFlag) {
      return res.status(200).json({ success: true });
    }
    const session = await ensureAwsSession(normalizedCallSid);
    clearSilenceTimer(normalizedCallSid);
    await db.addTranscript({
      call_sid: normalizedCallSid,
      speaker: "user",
      message: normalizedTranscript,
      interaction_count: session.interactionCount,
    });
    await db.updateCallState(normalizedCallSid, "user_spoke", {
      message: normalizedTranscript,
      interaction_count: session.interactionCount,
    });
    if (
      shouldCloseConversation(normalizedTranscript) &&
      session.interactionCount >= 1
    ) {
      await speakAndEndCall(
        normalizedCallSid,
        CALL_END_MESSAGES.user_goodbye,
        "user_goodbye",
      );
      session.interactionCount += 1;
      return res.status(200).json({ success: true });
    }
    enqueueGptTask(normalizedCallSid, async () => {
      const currentCount = session.interactionCount || 0;
      try {
        await session.gptService.completion(normalizedTranscript, currentCount);
      } catch (gptError) {
        console.error("GPT completion error:", gptError);
        webhookService.addLiveEvent(normalizedCallSid, "⚠️ GPT error, retrying", {
          force: true,
        });
      }
      session.interactionCount = currentCount + 1;
    });
    res.status(200).json({ success: true });
  } catch (error) {
    console.error("AWS transcript webhook error:", error);
    res
      .status(500)
      .json({ success: false, error: "Failed to ingest transcript" });
  }
});

// Provider status/update endpoints (admin only)
function getProviderStateSnapshot() {
  const callReadiness = getProviderReadiness();
  const smsReadiness = getSmsProviderReadiness();
  const emailReadiness = getEmailProviderReadiness();
  const callState = {
    provider: currentProvider,
    stored_provider: storedProvider,
    supported_providers: SUPPORTED_PROVIDERS,
    readiness: callReadiness,
    twilio_ready: callReadiness.twilio,
    aws_ready: callReadiness.aws,
    vonage_ready: callReadiness.vonage,
  };
  const smsState = {
    provider: currentSmsProvider,
    stored_provider: storedSmsProvider,
    supported_providers: SUPPORTED_SMS_PROVIDERS,
    readiness: smsReadiness,
  };
  const emailState = {
    provider: currentEmailProvider,
    stored_provider: storedEmailProvider,
    supported_providers: SUPPORTED_EMAIL_PROVIDERS,
    readiness: emailReadiness,
  };
  return {
    callState,
    smsState,
    emailState,
  };
}

function resolveProviderChannel(value) {
  const normalized = String(value || PROVIDER_CHANNELS.CALL)
    .trim()
    .toLowerCase();
  if (
    normalized !== PROVIDER_CHANNELS.CALL &&
    normalized !== PROVIDER_CHANNELS.SMS &&
    normalized !== PROVIDER_CHANNELS.EMAIL
  ) {
    return null;
  }
  return normalized;
}

app.get("/admin/provider", requireAdminToken, async (req, res) => {
  syncRuntimeProviderMirrors();
  const { callState, smsState, emailState } = getProviderStateSnapshot();
  pruneExpiredKeypadProviderOverrides();
  const requestedChannel = resolveProviderChannel(req.query?.channel);
  if (!requestedChannel && req.query?.channel) {
    return res.status(400).json({
      success: false,
      error: "Unsupported provider channel",
    });
  }
  const channel = requestedChannel || PROVIDER_CHANNELS.CALL;
  const selectedState =
    channel === PROVIDER_CHANNELS.CALL
      ? callState
      : channel === PROVIDER_CHANNELS.SMS
        ? smsState
        : emailState;

  res.json({
    channel,
    provider: selectedState.provider,
    stored_provider: selectedState.stored_provider,
    supported_providers: selectedState.supported_providers,
    twilio_ready: callState.twilio_ready,
    aws_ready: callState.aws_ready,
    vonage_ready: callState.vonage_ready,
    vonage_dtmf_ready: config.vonage?.dtmfWebhookEnabled === true,
    keypad_guard_enabled: config.keypadGuard?.enabled === true,
    keypad_override_count: keypadProviderOverrides.size,
    sms_provider: smsState.provider,
    sms_stored_provider: smsState.stored_provider,
    sms_supported_providers: smsState.supported_providers,
    sms_readiness: smsState.readiness,
    email_provider: emailState.provider,
    email_stored_provider: emailState.stored_provider,
    email_supported_providers: emailState.supported_providers,
    email_readiness: emailState.readiness,
    providers: {
      call: callState,
      sms: smsState,
      email: emailState,
    },
  });
});

app.post("/admin/provider", requireAdminToken, async (req, res) => {
  syncRuntimeProviderMirrors();
  const body = req.body || {};
  const provider = String(body.provider || "")
    .trim()
    .toLowerCase();
  const channel = resolveProviderChannel(body.channel);
  if (!channel) {
    return res.status(400).json({
      success: false,
      error: "Unsupported provider channel",
    });
  }
  const channelStateMap = getProviderStateSnapshot();
  const stateByChannel = {
    [PROVIDER_CHANNELS.CALL]: channelStateMap.callState,
    [PROVIDER_CHANNELS.SMS]: channelStateMap.smsState,
    [PROVIDER_CHANNELS.EMAIL]: channelStateMap.emailState,
  };
  const selectedState = stateByChannel[channel];
  if (!provider || !selectedState.supported_providers.includes(provider)) {
    return res
      .status(400)
      .json({
        success: false,
        error: "Unsupported provider",
        channel,
        supported_providers: selectedState.supported_providers,
      });
  }
  const readiness = selectedState.readiness || {};
  if (!readiness[provider]) {
    return res.status(400).json({
      success: false,
      error: `Provider ${provider} is not configured for ${channel}`,
      channel,
    });
  }
  const normalized = provider;
  const changed = normalized !== selectedState.provider;
  if (channel === PROVIDER_CHANNELS.CALL) {
    setActiveCallProvider(normalized);
    setStoredCallProvider(normalized);
  } else if (channel === PROVIDER_CHANNELS.SMS) {
    setActiveSmsProvider(normalized);
    setStoredSmsProvider(normalized);
  } else if (channel === PROVIDER_CHANNELS.EMAIL) {
    setActiveEmailProvider(normalized);
    setStoredEmailProvider(normalized);
  }
  syncRuntimeProviderMirrors();

  const settingKey =
    channel === PROVIDER_CHANNELS.CALL
      ? CALL_PROVIDER_SETTING_KEY
      : channel === PROVIDER_CHANNELS.SMS
        ? SMS_PROVIDER_SETTING_KEY
        : EMAIL_PROVIDER_SETTING_KEY;
  if (db?.setSetting) {
    await db
      .setSetting(settingKey, normalized)
      .catch((error) =>
        console.error(
          `Failed to persist selected ${channel} provider:`,
          error,
        ),
      );
  }

  const label =
    channel === PROVIDER_CHANNELS.CALL
      ? "call"
      : channel === PROVIDER_CHANNELS.SMS
        ? "SMS"
        : "email";
  console.log(
    `☎️ Default ${label} provider updated: ${normalized.toUpperCase()} (changed=${changed})`,
  );
  const { callState, smsState, emailState } = getProviderStateSnapshot();
  const activeChannelProvider =
    channel === PROVIDER_CHANNELS.CALL
      ? currentProvider
      : channel === PROVIDER_CHANNELS.SMS
        ? currentSmsProvider
        : currentEmailProvider;
  return res.json({
    success: true,
    channel,
    provider: activeChannelProvider,
    changed,
    call_provider: callState.provider,
    sms_provider: smsState.provider,
    email_provider: emailState.provider,
    providers: {
      call: callState,
      sms: smsState,
      email: emailState,
    },
  });
});

app.get("/admin/provider/keypad-overrides", requireAdminToken, async (req, res) => {
  try {
    const overrides = listKeypadProviderOverrides();
    return res.json({
      success: true,
      total: overrides.length,
      overrides,
    });
  } catch (error) {
    console.error("Failed to list keypad provider overrides:", error);
    return res.status(500).json({
      success: false,
      error: "Failed to list keypad provider overrides",
    });
  }
});

app.post(
  "/admin/provider/keypad-overrides/clear",
  requireAdminToken,
  async (req, res) => {
    try {
      const body = req.body || {};
      const clearAll = body.all === true || String(body.all).toLowerCase() === "true";
      const scopeKey = body.scope_key || body.scopeKey || null;
      const scope = body.scope || null;
      const value = body.value || null;

      if (!clearAll && !scopeKey && !(scope && value)) {
        return res.status(400).json({
          success: false,
          error:
            "Provide one of: all=true, scope_key, or scope+value to clear keypad overrides",
        });
      }

      const result = await clearKeypadProviderOverrides({
        all: clearAll,
        scopeKey,
        scope,
        value,
      });

      return res.json({
        success: true,
        cleared: result.cleared,
        remaining: result.remaining,
        overrides: result.overrides,
      });
    } catch (error) {
      console.error("Failed to clear keypad provider overrides:", error);
      return res.status(500).json({
        success: false,
        error: "Failed to clear keypad provider overrides",
      });
    }
  },
);

app.get("/admin/payment/feature", requireAdminToken, async (req, res) => {
  try {
    const paymentConfig = getPaymentFeatureConfig();
    return res.json({
      success: true,
      feature: paymentConfig,
      twilio_ready: getProviderReadiness()?.twilio === true,
    });
  } catch (error) {
    console.error("Failed to fetch payment feature config:", error);
    return res.status(500).json({
      success: false,
      error: "Failed to fetch payment feature config",
    });
  }
});

app.post("/admin/payment/feature", requireAdminToken, async (req, res) => {
  try {
    const body = req.body && typeof req.body === "object" ? req.body : {};
    paymentFeatureConfig = sanitizePaymentFeatureConfig(body, paymentFeatureConfig);
    await persistPaymentFeatureConfig();

    // Refresh dynamic tool exposure for active calls immediately.
    for (const callSid of activeCalls.keys()) {
      refreshActiveCallTools(callSid);
    }

    return res.json({
      success: true,
      feature: getPaymentFeatureConfig(),
      twilio_ready: getProviderReadiness()?.twilio === true,
    });
  } catch (error) {
    console.error("Failed to update payment feature config:", error);
    return res.status(500).json({
      success: false,
      error: "Failed to update payment feature config",
    });
  }
});

app.get("/admin/call-jobs/dlq", requireAdminToken, async (req, res) => {
  try {
    if (!db) {
      return res
        .status(500)
        .json({ success: false, error: "Database not initialized" });
    }
    const limit = Math.min(Math.max(Number(req.query?.limit) || 20, 1), 100);
    const offset = Math.max(Number(req.query?.offset) || 0, 0);
    const status = req.query?.status ? String(req.query.status) : null;
    const rows = await db.listCallJobDlq({ status, limit, offset });
    return res.json({
      success: true,
      rows,
      limit,
      offset,
      status: status || "all",
    });
  } catch (error) {
    console.error("Failed to list call-job DLQ:", error);
    return res
      .status(500)
      .json({ success: false, error: "Failed to list call-job DLQ" });
  }
});

app.post(
  "/admin/call-jobs/dlq/:id/replay",
  requireAdminToken,
  async (req, res) => {
    try {
      if (!db) {
        return res
          .status(500)
          .json({ success: false, error: "Database not initialized" });
      }
      const dlqId = Number(req.params?.id);
      if (!Number.isFinite(dlqId) || dlqId <= 0) {
        return res.status(400).json({ success: false, error: "Invalid DLQ id" });
      }
      const entry = await db.getCallJobDlqEntry(dlqId);
      if (!entry) {
        return res
          .status(404)
          .json({ success: false, error: "DLQ entry not found" });
      }
      const maxReplays = Number(config.callJobs?.dlqMaxReplays) || 2;
      if (Number(entry.replay_count) >= maxReplays) {
        return res.status(409).json({
          success: false,
          error: "Replay limit reached for this DLQ entry",
          replay_count: Number(entry.replay_count) || 0,
          max_replays: maxReplays,
        });
      }
      let runAt = new Date().toISOString();
      if (req.body?.run_at) {
        const parsed = new Date(String(req.body.run_at));
        if (Number.isNaN(parsed.getTime())) {
          return res
            .status(400)
            .json({ success: false, error: "Invalid run_at timestamp" });
        }
        runAt = parsed.toISOString();
      }
      let payload = {};
      try {
        payload = entry.payload ? JSON.parse(entry.payload) : {};
      } catch {
        return res.status(400).json({
          success: false,
          error: "DLQ payload is not valid JSON; cannot replay",
        });
      }
      const replayJobId = await db.createCallJob(entry.job_type, payload, runAt);
      await db.markCallJobDlqReplayed(dlqId, replayJobId);
      db
        ?.logServiceHealth?.("call_jobs", "dlq_replayed", {
          dlq_id: dlqId,
          original_job_id: entry.job_id,
          replay_job_id: replayJobId,
          replay_count: (Number(entry.replay_count) || 0) + 1,
          at: new Date().toISOString(),
        })
        .catch(() => {});
      return res.json({
        success: true,
        dlq_id: dlqId,
        original_job_id: entry.job_id,
        replay_job_id: replayJobId,
      });
    } catch (error) {
      console.error("Failed to replay call-job DLQ entry:", error);
      return res
        .status(500)
        .json({ success: false, error: "Failed to replay call-job DLQ entry" });
    }
  },
);

app.get("/admin/email/dlq", requireAdminToken, async (req, res) => {
  try {
    if (!db) {
      return res
        .status(500)
        .json({ success: false, error: "Database not initialized" });
    }
    const limit = Math.min(Math.max(Number(req.query?.limit) || 20, 1), 100);
    const offset = Math.max(Number(req.query?.offset) || 0, 0);
    const status = req.query?.status ? String(req.query.status) : null;
    const rows = await db.listEmailDlq({ status, limit, offset });
    return res.json({
      success: true,
      rows,
      limit,
      offset,
      status: status || "all",
    });
  } catch (error) {
    console.error("Failed to list email DLQ:", error);
    return res
      .status(500)
      .json({ success: false, error: "Failed to list email DLQ" });
  }
});

app.post("/admin/email/dlq/:id/replay", requireAdminToken, async (req, res) => {
  try {
    if (!db) {
      return res
        .status(500)
        .json({ success: false, error: "Database not initialized" });
    }
    const dlqId = Number(req.params?.id);
    if (!Number.isFinite(dlqId) || dlqId <= 0) {
      return res.status(400).json({ success: false, error: "Invalid DLQ id" });
    }
    const entry = await db.getEmailDlqEntry(dlqId);
    if (!entry) {
      return res
        .status(404)
        .json({ success: false, error: "DLQ entry not found" });
    }
    const maxReplays = Number(config.email?.dlqMaxReplays) || 2;
    if (Number(entry.replay_count) >= maxReplays) {
      return res.status(409).json({
        success: false,
        error: "Replay limit reached for this DLQ entry",
        replay_count: Number(entry.replay_count) || 0,
        max_replays: maxReplays,
      });
    }
    const message = await db.getEmailMessage(entry.message_id);
    if (!message) {
      await db.markEmailDlqReplayed(dlqId, "email_message_not_found");
      return res.status(404).json({
        success: false,
        error: "Email message not found for DLQ entry",
      });
    }
    const immutableStatuses = new Set(["queued", "retry", "sending", "sent", "delivered"]);
    if (immutableStatuses.has(String(message.status || "").toLowerCase())) {
      return res.status(409).json({
        success: false,
        error: `Cannot replay email message with status "${message.status}"`,
      });
    }
    const nextAttempt = new Date().toISOString();
    await db.updateEmailMessageStatus(message.message_id, {
      status: "queued",
      failure_reason: null,
      provider_message_id: null,
      provider_response: null,
      last_attempt_at: null,
      next_attempt_at: nextAttempt,
      retry_count: 0,
      failed_at: null,
      suppressed_reason: null,
    });
    await db.addEmailEvent(message.message_id, "requeued_from_dlq", {
      dlq_id: dlqId,
      next_attempt_at: nextAttempt,
    });
    await db.markEmailDlqReplayed(dlqId);
    db
      ?.logServiceHealth?.("email_queue", "dlq_replayed", {
        dlq_id: dlqId,
        message_id: message.message_id,
        replay_count: (Number(entry.replay_count) || 0) + 1,
        at: new Date().toISOString(),
      })
      .catch(() => {});
    return res.json({
      success: true,
      dlq_id: dlqId,
      message_id: message.message_id,
      status: "queued",
      next_attempt_at: nextAttempt,
    });
  } catch (error) {
    console.error("Failed to replay email DLQ entry:", error);
    return res
      .status(500)
      .json({ success: false, error: "Failed to replay email DLQ entry" });
  }
});

// Personas list for bot selection
app.get("/api/personas", async (req, res) => {
  res.json({
    success: true,
    builtin: builtinPersonas,
    custom: [],
  });
});

const callScriptMutationIdempotency = new Map();
const CALL_SCRIPT_MUTATION_IDEMPOTENCY_TTL_MS = 10 * 60 * 1000;

function sortObjectKeys(value) {
  if (Array.isArray(value)) {
    return value.map(sortObjectKeys);
  }
  if (value && typeof value === "object") {
    return Object.keys(value)
      .sort()
      .reduce((acc, key) => {
        acc[key] = sortObjectKeys(value[key]);
        return acc;
      }, {});
  }
  return value;
}

function buildCallScriptMutationFingerprint(action, target, payload) {
  const normalizedAction = String(action || "").trim().toLowerCase();
  const normalizedTarget = String(target || "").trim().toLowerCase();
  const normalizedPayload = sortObjectKeys(payload || {});
  return `${normalizedAction}:${normalizedTarget}:${JSON.stringify(normalizedPayload)}`;
}

function pruneCallScriptMutationIdempotency(now = Date.now()) {
  for (const [key, value] of callScriptMutationIdempotency.entries()) {
    if (!value?.at || now - value.at > CALL_SCRIPT_MUTATION_IDEMPOTENCY_TTL_MS) {
      callScriptMutationIdempotency.delete(key);
    }
  }
}

function resetCallScriptMutationIdempotencyForTests() {
  callScriptMutationIdempotency.clear();
}

function beginCallScriptMutationIdempotency(req, action, target, payload) {
  const key = String(
    req.headers["idempotency-key"] || req.headers["Idempotency-Key"] || "",
  ).trim();
  if (!isSafeId(key, { max: 128 })) {
    return { enabled: false };
  }

  const fingerprint = buildCallScriptMutationFingerprint(action, target, payload);
  const now = Date.now();
  pruneCallScriptMutationIdempotency(now);
  const existing = callScriptMutationIdempotency.get(key);
  if (existing) {
    if (existing.fingerprint !== fingerprint) {
      return {
        enabled: true,
        key,
        error: {
          status: 409,
          code: "idempotency_conflict",
          message: "Idempotency key reuse with different payload",
        },
      };
    }
    if (existing.status === "pending") {
      return {
        enabled: true,
        key,
        error: {
          status: 409,
          code: "idempotency_in_progress",
          message: "Idempotency key is currently processing",
        },
      };
    }
    if (existing.status === "done" && existing.response) {
      return {
        enabled: true,
        key,
        replay: existing.response,
      };
    }
  }

  callScriptMutationIdempotency.set(key, {
    at: now,
    status: "pending",
    fingerprint,
    response: null,
  });
  return { enabled: true, key };
}

function completeCallScriptMutationIdempotency(idem, status, body) {
  if (!idem?.enabled || !idem?.key) return;
  callScriptMutationIdempotency.set(idem.key, {
    at: Date.now(),
    status: "done",
    fingerprint:
      callScriptMutationIdempotency.get(idem.key)?.fingerprint || null,
    response: {
      status: Number(status) || 200,
      body,
    },
  });
}

function failCallScriptMutationIdempotency(idem) {
  if (!idem?.enabled || !idem?.key) return;
  callScriptMutationIdempotency.delete(idem.key);
}

function applyIdempotencyResponse(res, idem) {
  if (!idem) return false;
  if (idem.error) {
    return res.status(idem.error.status).json({
      success: false,
      error: idem.error.message,
      code: idem.error.code,
    });
  }
  if (idem.replay) {
    return res.status(idem.replay.status).json(idem.replay.body);
  }
  return false;
}

function normalizeCallTemplateName(value = "") {
  const trimmed = String(value || "").trim();
  return trimmed.slice(0, 80);
}

function callScriptPaymentFieldTouched(payload = {}) {
  const fields = [
    "payment_enabled",
    "payment_connector",
    "payment_amount",
    "payment_currency",
    "payment_description",
    "payment_policy",
    "payment_start_message",
    "payment_success_message",
    "payment_failure_message",
    "payment_retry_message",
  ];
  return fields.some((field) =>
    Object.prototype.hasOwnProperty.call(payload || {}, field),
  );
}

async function findCallTemplateNameCollision(name, excludeId = null) {
  const normalized = normalizeCallTemplateName(name);
  if (!normalized) return null;
  const scripts = await db.getCallTemplates();
  const normalizedLower = normalized.toLowerCase();
  const excluded = Number(excludeId);
  return (
    (scripts || []).find((script) => {
      const scriptName = normalizeCallTemplateName(script?.name || "").toLowerCase();
      if (!scriptName || scriptName !== normalizedLower) {
        return false;
      }
      if (Number.isFinite(excluded) && Number(script?.id) === excluded) {
        return false;
      }
      return true;
    }) || null
  );
}

async function suggestCallTemplateName(baseName, excludeId = null) {
  const fallbackBase = normalizeCallTemplateName(baseName) || "Call Script";
  const scripts = await db.getCallTemplates().catch(() => []);
  const excluded = Number(excludeId);
  const existingNames = new Set(
    (scripts || [])
      .filter((script) => !(Number.isFinite(excluded) && Number(script?.id) === excluded))
      .map((script) => normalizeCallTemplateName(script?.name || "").toLowerCase())
      .filter(Boolean),
  );

  if (!existingNames.has(fallbackBase.toLowerCase())) {
    return fallbackBase;
  }

  for (let index = 2; index < 1000; index += 1) {
    const suffix = ` ${index}`;
    const maxBaseLength = Math.max(1, 80 - suffix.length);
    const candidate = `${fallbackBase.slice(0, maxBaseLength)}${suffix}`;
    if (!existingNames.has(candidate.toLowerCase())) {
      return candidate;
    }
  }
  return `${fallbackBase.slice(0, 75)} ${Date.now().toString().slice(-4)}`;
}

// Call script endpoints for bot script management
app.get("/api/call-scripts", requireAdminToken, async (req, res) => {
  try {
    const scripts = (await db.getCallTemplates()).map((item) =>
      normalizeScriptTemplateRecord(item),
    );
    res.json({ success: true, scripts });
  } catch (error) {
    res
      .status(500)
      .json({ success: false, error: "Failed to fetch call scripts" });
  }
});

app.get("/api/call-scripts/:id", requireAdminToken, async (req, res) => {
  try {
    const scriptId = Number(req.params.id);
    if (Number.isNaN(scriptId)) {
      return res
        .status(400)
        .json({ success: false, error: "Invalid script id" });
    }
    const script = normalizeScriptTemplateRecord(
      await db.getCallTemplateById(scriptId),
    );
    if (!script) {
      return res
        .status(404)
        .json({ success: false, error: "Script not found" });
    }
    res.json({ success: true, script: normalizeScriptTemplateRecord(script) });
  } catch (error) {
    res
      .status(500)
      .json({ success: false, error: "Failed to fetch call script" });
  }
});

app.post("/api/call-scripts", requireAdminToken, async (req, res) => {
  const idempotency = beginCallScriptMutationIdempotency(
    req,
    "create",
    "new",
    req.body || {},
  );
  const prior = applyIdempotencyResponse(res, idempotency);
  if (prior) return prior;
  try {
    const requestBody = isPlainObject(req.body) ? req.body : {};
    const normalizedName = normalizeCallTemplateName(requestBody.name);
    const firstMessage = String(requestBody.first_message || "").trim();
    if (!normalizedName || !firstMessage) {
      failCallScriptMutationIdempotency(idempotency);
      return res
        .status(400)
        .json({ success: false, error: "name and first_message are required" });
    }
    const duplicate = await findCallTemplateNameCollision(normalizedName);
    if (duplicate) {
      failCallScriptMutationIdempotency(idempotency);
      return res.status(409).json({
        success: false,
        error: `Script '${normalizedName}' already exists`,
        code: "SCRIPT_NAME_DUPLICATE",
        suggested_name: await suggestCallTemplateName(`${normalizedName} Copy`),
      });
    }
    const paymentSettings = normalizePaymentSettings(requestBody, {
      provider: currentProvider,
      requireConnectorWhenEnabled: true,
    });
    if (paymentSettings.errors.length) {
      failCallScriptMutationIdempotency(idempotency);
      return res.status(400).json({
        success: false,
        error: paymentSettings.errors.join(" "),
      });
    }
    let paymentPolicyWarnings = [];
    let normalizedPaymentPolicy = null;
    if (Object.prototype.hasOwnProperty.call(requestBody, "payment_policy")) {
      const rawPolicy = requestBody.payment_policy;
      if (
        typeof rawPolicy === "string" &&
        rawPolicy.trim() &&
        !parsePaymentPolicy(rawPolicy)
      ) {
        failCallScriptMutationIdempotency(idempotency);
        return res.status(400).json({
          success: false,
          error: "payment_policy must be a valid JSON object.",
        });
      }
      const paymentPolicy = normalizePaymentPolicy(
        parsePaymentPolicy(rawPolicy) || {},
      );
      if (paymentPolicy.errors.length) {
        failCallScriptMutationIdempotency(idempotency);
        return res.status(400).json({
          success: false,
          error: paymentPolicy.errors.join(" "),
        });
      }
      paymentPolicyWarnings = paymentPolicy.warnings;
      normalizedPaymentPolicy =
        Object.keys(paymentPolicy.normalized).length > 0
          ? paymentPolicy.normalized
          : null;
    }
    const id = await db.createCallTemplate({
      ...requestBody,
      name: normalizedName,
      first_message: firstMessage,
      ...paymentSettings.normalized,
      payment_policy: normalizedPaymentPolicy,
    });
    const script = normalizeScriptTemplateRecord(
      await db.getCallTemplateById(id),
    );
    const responseBody = {
      success: true,
      script,
      warnings: [...paymentSettings.warnings, ...paymentPolicyWarnings],
    };
    completeCallScriptMutationIdempotency(idempotency, 201, responseBody);
    res.status(201).json(responseBody);
  } catch (error) {
    failCallScriptMutationIdempotency(idempotency);
    res
      .status(500)
      .json({ success: false, error: "Failed to create call script" });
  }
});

app.put("/api/call-scripts/:id", requireAdminToken, async (req, res) => {
  const scriptIdForIdem = Number(req.params.id);
  const idempotency = beginCallScriptMutationIdempotency(
    req,
    "update",
    Number.isFinite(scriptIdForIdem) ? scriptIdForIdem : req.params.id,
    req.body || {},
  );
  const prior = applyIdempotencyResponse(res, idempotency);
  if (prior) return prior;
  try {
    const requestBody = isPlainObject(req.body) ? req.body : {};
    const scriptId = Number(req.params.id);
    if (Number.isNaN(scriptId)) {
      failCallScriptMutationIdempotency(idempotency);
      return res
        .status(400)
        .json({ success: false, error: "Invalid script id" });
    }
    const existing = normalizeScriptTemplateRecord(
      await db.getCallTemplateById(scriptId),
    );
    if (!existing) {
      failCallScriptMutationIdempotency(idempotency);
      return res
        .status(404)
        .json({ success: false, error: "Script not found" });
    }

    const updates = { ...requestBody };
    if (updates.name !== undefined) {
      const normalizedName = normalizeCallTemplateName(updates.name);
      if (!normalizedName) {
        failCallScriptMutationIdempotency(idempotency);
        return res
          .status(400)
          .json({ success: false, error: "name cannot be empty" });
      }
      const duplicate = await findCallTemplateNameCollision(normalizedName, scriptId);
      if (duplicate) {
        failCallScriptMutationIdempotency(idempotency);
        return res.status(409).json({
          success: false,
          error: `Script '${normalizedName}' already exists`,
          code: "SCRIPT_NAME_DUPLICATE",
          suggested_name: await suggestCallTemplateName(normalizedName, scriptId),
        });
      }
      updates.name = normalizedName;
    }
    let paymentWarnings = [];
    if (callScriptPaymentFieldTouched(requestBody)) {
      const paymentSettings = normalizePaymentSettings(
        {
          payment_enabled: updates.payment_enabled ?? existing.payment_enabled,
          payment_connector:
            updates.payment_connector ?? existing.payment_connector,
          payment_amount: updates.payment_amount ?? existing.payment_amount,
          payment_currency:
            updates.payment_currency ?? existing.payment_currency,
          payment_description:
            updates.payment_description ?? existing.payment_description,
          payment_start_message:
            updates.payment_start_message ?? existing.payment_start_message,
          payment_success_message:
            updates.payment_success_message ?? existing.payment_success_message,
          payment_failure_message:
            updates.payment_failure_message ?? existing.payment_failure_message,
          payment_retry_message:
            updates.payment_retry_message ?? existing.payment_retry_message,
        },
        {
          provider: currentProvider,
          requireConnectorWhenEnabled: true,
        },
      );
      if (paymentSettings.errors.length) {
        failCallScriptMutationIdempotency(idempotency);
        return res.status(400).json({
          success: false,
          error: paymentSettings.errors.join(" "),
        });
      }
      paymentWarnings = paymentSettings.warnings;
      Object.assign(updates, paymentSettings.normalized);
    }
    if (Object.prototype.hasOwnProperty.call(requestBody, "payment_policy")) {
      const rawPolicy = requestBody.payment_policy;
      if (
        typeof rawPolicy === "string" &&
        rawPolicy.trim() &&
        !parsePaymentPolicy(rawPolicy)
      ) {
        failCallScriptMutationIdempotency(idempotency);
        return res.status(400).json({
          success: false,
          error: "payment_policy must be a valid JSON object.",
        });
      }
      const paymentPolicy = normalizePaymentPolicy(
        parsePaymentPolicy(rawPolicy) || {},
      );
      if (paymentPolicy.errors.length) {
        failCallScriptMutationIdempotency(idempotency);
        return res.status(400).json({
          success: false,
          error: paymentPolicy.errors.join(" "),
        });
      }
      updates.payment_policy =
        Object.keys(paymentPolicy.normalized).length > 0
          ? paymentPolicy.normalized
          : null;
      paymentWarnings = [...paymentWarnings, ...paymentPolicy.warnings];
    }
    const updated = await db.updateCallTemplate(scriptId, updates);
    if (!updated) {
      failCallScriptMutationIdempotency(idempotency);
      return res
        .status(404)
        .json({ success: false, error: "Script not found" });
    }
    const script = normalizeScriptTemplateRecord(
      await db.getCallTemplateById(scriptId),
    );
    if (inboundDefaultScriptId === scriptId) {
      inboundDefaultScript = script || null;
      inboundDefaultLoadedAt = Date.now();
    }
    const responseBody = { success: true, script, warnings: paymentWarnings };
    completeCallScriptMutationIdempotency(idempotency, 200, responseBody);
    res.json(responseBody);
  } catch (error) {
    failCallScriptMutationIdempotency(idempotency);
    res
      .status(500)
      .json({ success: false, error: "Failed to update call script" });
  }
});

app.delete("/api/call-scripts/:id", requireAdminToken, async (req, res) => {
  const scriptIdForIdem = Number(req.params.id);
  const idempotency = beginCallScriptMutationIdempotency(
    req,
    "delete",
    Number.isFinite(scriptIdForIdem) ? scriptIdForIdem : req.params.id,
    {},
  );
  const prior = applyIdempotencyResponse(res, idempotency);
  if (prior) return prior;
  try {
    const scriptId = Number(req.params.id);
    if (Number.isNaN(scriptId)) {
      failCallScriptMutationIdempotency(idempotency);
      return res
        .status(400)
        .json({ success: false, error: "Invalid script id" });
    }
    const deleted = await db.deleteCallTemplate(scriptId);
    if (!deleted) {
      failCallScriptMutationIdempotency(idempotency);
      return res
        .status(404)
        .json({ success: false, error: "Script not found" });
    }
    if (inboundDefaultScriptId === scriptId) {
      await db.setSetting(INBOUND_DEFAULT_SETTING_KEY, null);
      inboundDefaultScriptId = null;
      inboundDefaultScript = null;
      inboundDefaultLoadedAt = Date.now();
    }
    const body = { success: true };
    completeCallScriptMutationIdempotency(idempotency, 200, body);
    res.json(body);
  } catch (error) {
    failCallScriptMutationIdempotency(idempotency);
    res
      .status(500)
      .json({ success: false, error: "Failed to delete call script" });
  }
});

app.post("/api/call-scripts/:id/clone", requireAdminToken, async (req, res) => {
  const scriptIdForIdem = Number(req.params.id);
  const idempotency = beginCallScriptMutationIdempotency(
    req,
    "clone",
    Number.isFinite(scriptIdForIdem) ? scriptIdForIdem : req.params.id,
    req.body || {},
  );
  const prior = applyIdempotencyResponse(res, idempotency);
  if (prior) return prior;
  try {
    const requestBody = isPlainObject(req.body) ? req.body : {};
    const scriptId = Number(req.params.id);
    if (Number.isNaN(scriptId)) {
      failCallScriptMutationIdempotency(idempotency);
      return res
        .status(400)
        .json({ success: false, error: "Invalid script id" });
    }
    const existingRaw = await db.getCallTemplateById(scriptId);
    const existing = normalizeScriptTemplateRecord(existingRaw);
    if (!existing) {
      failCallScriptMutationIdempotency(idempotency);
      return res
        .status(404)
        .json({ success: false, error: "Script not found" });
    }
    const normalizedName = normalizeCallTemplateName(
      requestBody.name || `${existing.name} Copy`,
    );
    if (!normalizedName) {
      failCallScriptMutationIdempotency(idempotency);
      return res
        .status(400)
        .json({ success: false, error: "name is required for clone" });
    }
    const duplicate = await findCallTemplateNameCollision(normalizedName);
    if (duplicate) {
      failCallScriptMutationIdempotency(idempotency);
      return res.status(409).json({
        success: false,
        error: `Script '${normalizedName}' already exists`,
        code: "SCRIPT_NAME_DUPLICATE",
        suggested_name: await suggestCallTemplateName(normalizedName),
      });
    }

    const hasDescription = Object.prototype.hasOwnProperty.call(
      requestBody,
      "description",
    );
    const payload = {
      name: normalizedName,
      description: hasDescription
        ? requestBody.description || null
        : existing.description || null,
      prompt: existing.prompt || null,
      first_message: existing.first_message,
      business_id: existing.business_id || null,
      voice_model: existing.voice_model || null,
      requires_otp: existing.requires_otp ? 1 : 0,
      default_profile: existing.default_profile || null,
      expected_length:
        existing.expected_length === undefined ? null : existing.expected_length,
      allow_terminator: existing.allow_terminator ? 1 : 0,
      terminator_char: existing.terminator_char || null,
      payment_enabled: normalizeBooleanFlag(existing.payment_enabled, false),
      payment_connector: existing.payment_connector || null,
      payment_amount: existing.payment_amount || null,
      payment_currency: existing.payment_currency || null,
      payment_description: existing.payment_description || null,
      payment_policy: existing.payment_policy || null,
      payment_start_message: existing.payment_start_message || null,
      payment_success_message: existing.payment_success_message || null,
      payment_failure_message: existing.payment_failure_message || null,
      payment_retry_message: existing.payment_retry_message || null,
    };
    const paymentSettings = normalizePaymentSettings(payload, {
      provider: currentProvider,
      requireConnectorWhenEnabled: true,
    });
    if (paymentSettings.errors.length) {
      failCallScriptMutationIdempotency(idempotency);
      return res.status(400).json({
        success: false,
        error: paymentSettings.errors.join(" "),
      });
    }
    const newId = await db.createCallTemplate({
      ...payload,
      ...paymentSettings.normalized,
    });
    const script = normalizeScriptTemplateRecord(
      await db.getCallTemplateById(newId),
    );
    const responseBody = {
      success: true,
      script,
      warnings: paymentSettings.warnings,
    };
    completeCallScriptMutationIdempotency(idempotency, 201, responseBody);
    res.status(201).json(responseBody);
  } catch (error) {
    failCallScriptMutationIdempotency(idempotency);
    res
      .status(500)
      .json({ success: false, error: "Failed to clone call script" });
  }
});

app.get("/api/inbound/default-script", requireAdminToken, async (req, res) => {
  try {
    await refreshInboundDefaultScript(true);
    if (!inboundDefaultScript) {
      return res.json({ success: true, mode: "builtin" });
    }
    return res.json({
      success: true,
      mode: "script",
      script_id: inboundDefaultScriptId,
      script: inboundDefaultScript,
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: "Failed to fetch inbound default script",
    });
  }
});

app.put("/api/inbound/default-script", requireAdminToken, async (req, res) => {
  try {
    const scriptId = Number(req.body?.script_id);
    if (!Number.isFinite(scriptId)) {
      return res
        .status(400)
        .json({ success: false, error: "script_id is required" });
    }
    const script = normalizeScriptTemplateRecord(
      await db.getCallTemplateById(scriptId),
    );
    if (!script) {
      return res
        .status(404)
        .json({ success: false, error: "Script not found" });
    }
    if (!script.prompt || !script.first_message) {
      return res.status(400).json({
        success: false,
        error: "Script must include prompt and first_message",
      });
    }
    await db.setSetting(INBOUND_DEFAULT_SETTING_KEY, String(scriptId));
    inboundDefaultScriptId = scriptId;
    inboundDefaultScript = script;
    inboundDefaultLoadedAt = Date.now();
    await db.logServiceHealth("inbound_defaults", "set", {
      script_id: scriptId,
      script_name: script.name,
      source: "api",
    });
    return res.json({
      success: true,
      mode: "script",
      script_id: scriptId,
      script,
    });
  } catch (error) {
    res
      .status(500)
      .json({ success: false, error: "Failed to set inbound default script" });
  }
});

app.delete(
  "/api/inbound/default-script",
  requireAdminToken,
  async (req, res) => {
    try {
      await db.setSetting(INBOUND_DEFAULT_SETTING_KEY, null);
      inboundDefaultScriptId = null;
      inboundDefaultScript = null;
      inboundDefaultLoadedAt = Date.now();
      await db.logServiceHealth("inbound_defaults", "cleared", {
        source: "api",
      });
      return res.json({ success: true, mode: "builtin" });
    } catch (error) {
      res.status(500).json({
        success: false,
        error: "Failed to clear inbound default script",
      });
    }
  },
);

// Caller flags (block/allow/spam)
app.get("/api/caller-flags", requireAdminToken, async (req, res) => {
  try {
    const status = req.query?.status;
    const limit = req.query?.limit;
    const flags = await db.listCallerFlags({ status, limit });
    res.json({ success: true, flags });
  } catch (error) {
    console.error("Failed to list caller flags:", error);
    res
      .status(500)
      .json({ success: false, error: "Failed to list caller flags" });
  }
});

app.post("/api/caller-flags", requireAdminToken, async (req, res) => {
  try {
    const phoneInput = req.body?.phone_number || req.body?.phone || null;
    const status = String(req.body?.status || "").toLowerCase();
    const note = req.body?.note || null;
    const phone = normalizePhoneForFlag(phoneInput) || phoneInput;
    if (!phone) {
      return res
        .status(400)
        .json({ success: false, error: "phone_number is required" });
    }
    if (!["blocked", "allowed", "spam"].includes(status)) {
      return res.status(400).json({
        success: false,
        error: "status must be blocked, allowed, or spam",
      });
    }
    const flag = await db.setCallerFlag(phone, status, {
      note,
      updated_by: req.headers?.["x-admin-user"] || null,
      source: "api",
    });
    res.json({ success: true, flag });
  } catch (error) {
    console.error("Failed to set caller flag:", error);
    res
      .status(500)
      .json({ success: false, error: "Failed to set caller flag" });
  }
});

async function buildRetryPayload(callSid) {
  const callRecord = await db.getCall(callSid);
  if (!callRecord) {
    throw new Error("Call not found");
  }
  const callState = await db
    .getLatestCallState(callSid, "call_created")
    .catch(() => null);

  return {
    number: callRecord.phone_number,
    prompt: callRecord.prompt,
    first_message: callRecord.first_message,
    user_chat_id: callRecord.user_chat_id,
    customer_name: callState?.customer_name || callState?.victim_name || null,
    business_id: callState?.business_id || null,
    script: callState?.script || null,
    script_id: callState?.script_id || null,
    script_version: callState?.script_version || null,
    purpose: callState?.purpose || null,
    emotion: callState?.emotion || null,
    urgency: callState?.urgency || null,
    technical_level: callState?.technical_level || null,
    voice_model: callState?.voice_model || null,
    collection_profile: callState?.collection_profile || null,
    collection_expected_length: callState?.collection_expected_length || null,
    collection_timeout_s: callState?.collection_timeout_s || null,
    collection_max_retries: callState?.collection_max_retries || null,
    collection_mask_for_gpt: callState?.collection_mask_for_gpt,
    collection_speak_confirmation: callState?.collection_speak_confirmation,
    payment_enabled: normalizeBooleanFlag(callState?.payment_enabled, false),
    payment_connector: callState?.payment_connector || null,
    payment_amount: callState?.payment_amount || null,
    payment_currency: callState?.payment_currency || null,
    payment_description: callState?.payment_description || null,
    payment_start_message: callState?.payment_start_message || null,
    payment_success_message: callState?.payment_success_message || null,
    payment_failure_message: callState?.payment_failure_message || null,
    payment_retry_message: callState?.payment_retry_message || null,
    payment_policy: parsePaymentPolicy(callState?.payment_policy),
  };
}

async function scheduleCallJob(jobType, payload, runAt = null) {
  if (!db) throw new Error("Database not initialized");
  return db.createCallJob(jobType, payload, runAt);
}

function computeCallJobBackoff(attempt) {
  const base = Number(config.callJobs?.retryBaseMs) || 5000;
  const max = Number(config.callJobs?.retryMaxMs) || 60000;
  const exp = Math.max(0, Number(attempt) - 1);
  const delay = Math.min(base * Math.pow(2, exp), max);
  return delay;
}

async function runWithTimeout(
  operationPromise,
  timeoutMs,
  label = "operation",
  timeoutCode = "operation_timeout",
) {
  const safeTimeoutMs = Number(timeoutMs);
  if (!Number.isFinite(safeTimeoutMs) || safeTimeoutMs <= 0) {
    return operationPromise;
  }
  let settled = false;
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      const timeoutError = new Error(
        `${label} timed out after ${safeTimeoutMs}ms`,
      );
      timeoutError.code = timeoutCode;
      reject(timeoutError);
    }, safeTimeoutMs);

    Promise.resolve(operationPromise)
      .then((result) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        resolve(result);
      })
      .catch((error) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        reject(error);
      });
  });
}

async function processCallJobs() {
  if (!db || callJobProcessing) return;
  callJobProcessing = true;
  try {
    const jobs = await db.claimDueCallJobs(10);
    const jobTimeoutMs = Number(config.callJobs?.timeoutMs) || 45000;
    for (const job of jobs) {
      let payload = {};
      try {
        payload = job.payload ? JSON.parse(job.payload) : {};
      } catch {
        payload = {};
      }
      try {
        if (
          job.job_type === "outbound_call" ||
          job.job_type === "callback_call"
        ) {
          await runWithTimeout(
            placeOutboundCall(payload),
            jobTimeoutMs,
            `call job ${job.id}`,
            "call_job_timeout",
          );
        } else if (job.job_type === "sms_scheduled_send") {
          const to = String(payload?.to || "").trim();
          const message = String(payload?.message || "").trim();
          const from = payload?.from || null;
          const userChatId = payload?.user_chat_id || null;
          if (!to || !message) {
            throw new Error("sms_scheduled_send requires to and message");
          }
          const smsOptions = {
            ...(payload?.sms_options || {}),
            allowQuietHours: false,
            minIntervalMs: 0,
          };
          if (payload?.idempotency_key && !smsOptions.idempotencyKey) {
            smsOptions.idempotencyKey = payload.idempotency_key;
          }
          if (userChatId && !smsOptions.userChatId) {
            smsOptions.userChatId = userChatId;
          }
          const smsResult = await runWithTimeout(
            smsService.sendSMS(to, message, from, smsOptions),
            jobTimeoutMs,
            `sms job ${job.id}`,
            "call_job_timeout",
          );
          if (smsResult?.message_sid && db && smsResult.idempotent !== true) {
            try {
              await db.saveSMSMessage({
                message_sid: smsResult.message_sid,
                to_number: to,
                from_number: smsResult.from || from,
                body: message,
                status: smsResult.status || "queued",
                direction: "outbound",
                provider:
                  smsResult.provider ||
                  smsOptions.provider ||
                  getActiveSmsProvider(),
                user_chat_id: userChatId || null,
              });
            } catch (saveError) {
              const saveMsg = String(saveError?.message || "");
              if (
                !saveMsg.includes("UNIQUE constraint failed") &&
                !saveMsg.includes("SQLITE_CONSTRAINT")
              ) {
                throw saveError;
              }
            }
            if (userChatId) {
              await db.createEnhancedWebhookNotification(
                smsResult.message_sid,
                "sms_sent",
                String(userChatId),
              );
            }
          }
        } else {
          throw new Error(`Unsupported job type ${job.job_type}`);
        }
        await db.completeCallJob(job.id, "completed");
      } catch (error) {
        const isTimeout = error?.code === "call_job_timeout";
        if (isTimeout) {
          db
            ?.logServiceHealth?.("call_jobs", "job_timeout", {
              job_id: job.id,
              job_type: job.job_type,
              timeout_ms: jobTimeoutMs,
              attempts: Number(job.attempts) || 1,
              at: new Date().toISOString(),
            })
            .catch(() => {});
        }
        const attempts = Number(job.attempts) || 1;
        const maxAttempts = Number(config.callJobs?.maxAttempts) || 3;
        if (attempts >= maxAttempts) {
          const failureReason = error.message || String(error);
          await db.completeCallJob(
            job.id,
            "failed",
            failureReason,
          );
          await db.moveCallJobToDlq(
            {
              ...job,
              attempts,
            },
            "max_attempts_exceeded",
            failureReason,
          );
          db
            ?.logServiceHealth?.("call_jobs", "job_dead_lettered", {
              job_id: job.id,
              job_type: job.job_type,
              attempts,
              max_attempts: maxAttempts,
              reason: failureReason,
              at: new Date().toISOString(),
            })
            .catch(() => {});
          const openDlqCount = await db.countOpenCallJobDlq().catch(() => null);
          const dlqAlertThreshold =
            Number(config.callJobs?.dlqAlertThreshold) || 20;
          if (openDlqCount !== null && openDlqCount >= dlqAlertThreshold) {
            db
              ?.logServiceHealth?.("call_jobs", "dlq_alert_threshold", {
                open_dlq: openDlqCount,
                alert_threshold: dlqAlertThreshold,
                at: new Date().toISOString(),
              })
              .catch(() => {});
          }
        } else {
          const delay = computeCallJobBackoff(attempts);
          const nextRunAt = new Date(Date.now() + delay).toISOString();
          await db.rescheduleCallJob(
            job.id,
            nextRunAt,
            error.message || String(error),
          );
        }
      }
    }
  } catch (error) {
    console.error("Call job processor error:", error);
  } finally {
    callJobProcessing = false;
  }
}

async function placeOutboundCall(payload, hostOverride = null) {
  const {
    number,
    prompt,
    first_message,
    user_chat_id,
    customer_name,
    business_id,
    script,
    script_id,
    script_version,
    purpose,
    emotion,
    urgency,
    technical_level,
    voice_model,
    collection_profile,
    collection_expected_length,
    collection_timeout_s,
    collection_max_retries,
    collection_mask_for_gpt,
    collection_speak_confirmation,
    payment_enabled,
    payment_connector,
    payment_amount,
    payment_currency,
    payment_description,
    payment_start_message,
    payment_success_message,
    payment_failure_message,
    payment_retry_message,
    payment_policy,
  } = payload || {};
  const payloadObject =
    payload && typeof payload === "object" ? payload : {};
  const hasPayloadField = (field) =>
    Object.prototype.hasOwnProperty.call(payloadObject, field);
  assertScriptBoundPayment(payloadObject, script_id);
  assertScriptBoundPaymentPolicy(payloadObject, script_id);

  if (!number || !prompt || !first_message) {
    throw new Error(
      "Missing required fields: number, prompt, and first_message are required",
    );
  }

  if (!number.match(/^\+[1-9]\d{1,14}$/)) {
    throw new Error(
      "Invalid phone number format. Use E.164 format (e.g., +1234567890)",
    );
  }

  const host = hostOverride || config.server?.hostname;
  if (!host) {
    throw new Error("Server hostname not configured");
  }

  console.log("Generating adaptive function system for call...".blue);
  const functionSystem = functionEngine.generateAdaptiveFunctionSystem(
    prompt,
    first_message,
  );
  console.log(
    `Generated ${functionSystem.functions.length} functions for ${functionSystem.context.industry} industry`,
  );

  const callWarnings = [];
  const normalizedScriptId = normalizeScriptId(script_id);
  const requestedScriptVersion = Number(script_version);
  let resolvedScriptVersion =
    Number.isFinite(requestedScriptVersion) && requestedScriptVersion > 0
      ? Math.max(1, Math.floor(requestedScriptVersion))
      : null;
  let scriptPolicy = {};
  let scriptPaymentDefaults = {};
  let scriptPaymentPolicy = null;
  if (normalizedScriptId) {
    try {
      const tpl = normalizeScriptTemplateRecord(
        await db.getCallTemplateById(Number(normalizedScriptId)),
      );
      if (tpl) {
        const currentTemplateVersion =
          Number.isFinite(Number(tpl.version)) && Number(tpl.version) > 0
            ? Math.max(1, Math.floor(Number(tpl.version)))
            : 1;
        if (!resolvedScriptVersion) {
          resolvedScriptVersion = currentTemplateVersion;
        }
        const versionMatches =
          !resolvedScriptVersion || resolvedScriptVersion === currentTemplateVersion;
        if (versionMatches) {
          scriptPolicy = {
            requires_otp: !!tpl.requires_otp,
            default_profile: tpl.default_profile || null,
            expected_length: tpl.expected_length || null,
            allow_terminator: !!tpl.allow_terminator,
            terminator_char: tpl.terminator_char || null,
          };
          scriptPaymentDefaults = {
            payment_enabled: normalizeBooleanFlag(tpl.payment_enabled, false),
            payment_connector: tpl.payment_connector || null,
            payment_amount: tpl.payment_amount || null,
            payment_currency: tpl.payment_currency || null,
            payment_description: tpl.payment_description || null,
            payment_start_message: tpl.payment_start_message || null,
            payment_success_message: tpl.payment_success_message || null,
            payment_failure_message: tpl.payment_failure_message || null,
            payment_retry_message: tpl.payment_retry_message || null,
          };
          scriptPaymentPolicy = tpl.payment_policy || null;
        } else {
          callWarnings.push(
            `Script version mismatch for script_id ${normalizedScriptId}: requested v${resolvedScriptVersion}, current v${currentTemplateVersion}. Using pinned payload settings.`,
          );
        }
      }
    } catch (err) {
      console.error("Script metadata load error:", err);
    }
  }

  let callId;
  let callStatus = "queued";
  let providerMetadata = {};
  let selectedProvider = null;
  const keypadRequired = isKeypadRequiredFlow(collection_profile, scriptPolicy);
  const keypadRequiredReason = keypadRequired
    ? `profile=${
        normalizeDigitProfile(collection_profile) ||
        normalizeDigitProfile(scriptPolicy?.default_profile) ||
        "unknown"
      }, script_requires_otp=${scriptPolicy?.requires_otp ? "true" : "false"}`
    : null;
  const keypadOverride = keypadRequired
    ? resolveKeypadProviderOverride(
        collection_profile,
        scriptPolicy,
        normalizedScriptId,
      )
    : null;

  const readiness = getProviderReadiness();
  const preferredProvider = keypadOverride?.provider || currentProvider;
  const orderedProviders = getProviderOrder(preferredProvider);
  let availableProviders = orderedProviders.filter(
    (provider) => readiness[provider],
  );
  if (keypadOverride && !availableProviders.includes(preferredProvider)) {
    console.warn(
      `Provider guard override for ${keypadOverride.scopeKey} requested ${preferredProvider}, but provider is unavailable. Falling back to available keypad-capable providers.`,
    );
  } else if (keypadOverride) {
    console.log(
      `Provider guard override active for ${keypadOverride.scopeKey}: preferring ${preferredProvider} until ${new Date(
        keypadOverride.expiresAt,
      ).toISOString()}`,
    );
  }
  if (keypadRequired) {
    const keypadProviders = availableProviders.filter((provider) =>
      supportsKeypadCaptureProvider(provider),
    );
    if (!keypadProviders.length) {
      throw new Error(
        "This call requires keypad digit capture, but no keypad-capable provider is configured. Configure Twilio or enable VONAGE_DTMF_WEBHOOK_ENABLED=true.",
      );
    }
    if (
      keypadProviders.length !== availableProviders.length &&
      !keypadProviderGuardWarnings.has(currentProvider)
    ) {
      console.warn(
        `Provider guard: restricting outbound call providers for keypad flow (${keypadRequiredReason || "digit_capture"}) to ${keypadProviders.join(", ")}`,
      );
      keypadProviderGuardWarnings.add(currentProvider);
    }
    availableProviders = keypadProviders;
  }
  if (!availableProviders.length) {
    throw new Error("No outbound provider configured");
  }
  const failoverEnabled = config.providerFailover?.enabled !== false;
  const healthyProviders = failoverEnabled
    ? availableProviders.filter((provider) => !isProviderDegraded(provider))
    : availableProviders;
  const attemptProviders = healthyProviders.length
    ? healthyProviders
    : availableProviders;
  let lastError = null;

  for (const provider of attemptProviders) {
    try {
      if (provider === "twilio") {
        warnIfMachineDetectionDisabled("outbound-call");
        const accountSid = config.twilio.accountSid;
        const authToken = config.twilio.authToken;
        const fromNumber = config.twilio.fromNumber;

        if (!accountSid || !authToken || !fromNumber) {
          throw new Error("Twilio credentials not configured");
        }

        const client = twilio(accountSid, authToken);
        const twimlUrl = `https://${host}/incoming`;
        const statusUrl = `https://${host}/webhook/call-status`;
        console.log(
          `Twilio call URLs: twiml=${twimlUrl} statusCallback=${statusUrl}`,
        );
        const callPayload = {
          url: twimlUrl,
          to: number,
          from: fromNumber,
          statusCallback: statusUrl,
          statusCallbackEvent: [
            "initiated",
            "ringing",
            "answered",
            "completed",
            "busy",
            "no-answer",
            "canceled",
            "failed",
          ],
          statusCallbackMethod: "POST",
        };
        if (config.twilio?.machineDetection) {
          callPayload.machineDetection = config.twilio.machineDetection;
        }
        if (Number.isFinite(config.twilio?.machineDetectionTimeout)) {
          callPayload.machineDetectionTimeout =
            config.twilio.machineDetectionTimeout;
        }
        const call = await client.calls.create(callPayload);
        callId = call.sid;
        callStatus = call.status || "queued";
      } else if (provider === "aws") {
        const awsAdapter = getAwsConnectAdapter();
        callId = uuidv4();
        const response = await awsAdapter.startOutboundCall({
          destinationPhoneNumber: number,
          clientToken: callId,
          attributes: {
            CALL_SID: callId,
            FIRST_MESSAGE: first_message,
          },
        });
        providerMetadata = { contact_id: response.ContactId };
        if (response.ContactId) {
          awsContactMap.set(response.ContactId, callId);
        }
        callStatus = "queued";
      } else if (provider === "vonage") {
        const vonageAdapter = getVonageVoiceAdapter();
        callId = uuidv4();
        const webhookReq = reqForHost(host);
        const answerUrl = buildVonageAnswerWebhookUrl(webhookReq, callId);
        const eventUrl = buildVonageEventWebhookUrl(webhookReq, callId);

        const wsUrl = host
          ? buildVonageWebsocketUrl(webhookReq, callId, {
              direction: "outbound",
            })
          : "";
        const ncco = wsUrl
          ? [
              {
                action: "connect",
                endpoint: [
                  {
                    type: "websocket",
                    uri: wsUrl,
                    "content-type": getVonageWebsocketContentType(),
                  },
                ],
              },
            ]
          : null;

        if (!ncco && !answerUrl) {
          throw new Error(
            "Vonage requires a public SERVER hostname or VONAGE_ANSWER_URL",
          );
        }
        const response = await vonageAdapter.createOutboundCall({
          to: number,
          callSid: callId,
          answerUrl,
          eventUrl,
          ncco: ncco || undefined,
        });
        const vonageUuid = response?.uuid;
        providerMetadata = {
          vonage_uuid: vonageUuid || null,
          answer_url: answerUrl || null,
          event_url: eventUrl || null,
        };
        if (vonageUuid) {
          rememberVonageCallMapping(callId, vonageUuid, "outbound_create");
        }
        callStatus = response?.status || "queued";
      } else {
        throw new Error(`Unsupported provider ${provider}`);
      }
      recordProviderSuccess(provider);
      selectedProvider = provider;
      break;
    } catch (error) {
      lastError = error;
      recordProviderError(provider, error);
      console.error(
        `Outbound call failed for provider ${provider}:`,
        error.message || error,
      );
    }
  }

  if (!selectedProvider) {
    throw lastError || new Error("Failed to place outbound call");
  }
  if (keypadOverride) {
    providerMetadata = {
      ...providerMetadata,
      provider_guard_override: {
        scope_key: keypadOverride.scopeKey,
        provider: keypadOverride.provider,
        expires_at: keypadOverride.expiresAt
          ? new Date(keypadOverride.expiresAt).toISOString()
          : null,
        reason: keypadOverride.reason || "vonage_dtmf_timeout",
      },
    };
  }

  const createdAt = new Date().toISOString();
  const mergedPaymentInput = {
    payment_enabled: hasPayloadField("payment_enabled")
      ? payment_enabled
      : scriptPaymentDefaults.payment_enabled,
    payment_connector: hasPayloadField("payment_connector")
      ? payment_connector
      : scriptPaymentDefaults.payment_connector,
    payment_amount: hasPayloadField("payment_amount")
      ? payment_amount
      : scriptPaymentDefaults.payment_amount,
    payment_currency: hasPayloadField("payment_currency")
      ? payment_currency
      : scriptPaymentDefaults.payment_currency,
    payment_description: hasPayloadField("payment_description")
      ? payment_description
      : scriptPaymentDefaults.payment_description,
    payment_start_message: hasPayloadField("payment_start_message")
      ? payment_start_message
      : scriptPaymentDefaults.payment_start_message,
    payment_success_message: hasPayloadField("payment_success_message")
      ? payment_success_message
      : scriptPaymentDefaults.payment_success_message,
    payment_failure_message: hasPayloadField("payment_failure_message")
      ? payment_failure_message
      : scriptPaymentDefaults.payment_failure_message,
    payment_retry_message: hasPayloadField("payment_retry_message")
      ? payment_retry_message
      : scriptPaymentDefaults.payment_retry_message,
  };
  let payloadPaymentPolicy = null;
  if (hasPayloadField("payment_policy")) {
    const rawPayloadPaymentPolicy = payment_policy;
    const clearRequested =
      rawPayloadPaymentPolicy === null ||
      rawPayloadPaymentPolicy === undefined ||
      (typeof rawPayloadPaymentPolicy === "string" &&
        rawPayloadPaymentPolicy.trim() === "");
    if (!clearRequested) {
      payloadPaymentPolicy = parsePaymentPolicy(rawPayloadPaymentPolicy);
      if (!payloadPaymentPolicy) {
        const error = new Error(
          "payment_policy must be a valid JSON object.",
        );
        error.code = "payment_policy_invalid";
        error.status = 400;
        throw error;
      }
    }
  }
  const mergedPaymentPolicyInput = hasPayloadField("payment_policy")
    ? payloadPaymentPolicy
    : scriptPaymentPolicy;
  const normalizedPaymentPolicyResult = normalizePaymentPolicy(
    mergedPaymentPolicyInput || {},
  );
  if (normalizedPaymentPolicyResult.errors.length) {
    const error = new Error(normalizedPaymentPolicyResult.errors.join(" "));
    error.code = "payment_policy_invalid";
    error.status = 400;
    throw error;
  }
  if (normalizedPaymentPolicyResult.warnings.length) {
    callWarnings.push(...normalizedPaymentPolicyResult.warnings);
  }
  const normalizedPaymentPolicy =
    Object.keys(normalizedPaymentPolicyResult.normalized).length > 0
      ? normalizedPaymentPolicyResult.normalized
      : null;
  const normalizedPayment = normalizePaymentSettings(mergedPaymentInput, {
    provider: selectedProvider || currentProvider,
    requireConnectorWhenEnabled: false,
    hasScript: Boolean(normalizedScriptId),
    enforceFeatureGate: true,
  });
  if (normalizedPayment.errors.length) {
    const error = new Error(normalizedPayment.errors.join(" "));
    error.code = "payment_validation_error";
    error.status = 400;
    throw error;
  }
  if (normalizedPayment.warnings.length) {
    callWarnings.push(...normalizedPayment.warnings);
  }
  const paymentEnabled = normalizedPayment.normalized.payment_enabled === true;
  const normalizedPaymentCurrency =
    normalizedPayment.normalized.payment_currency || null;
  const normalizedPaymentAmount =
    normalizedPayment.normalized.payment_amount || null;
  const callConfig = {
    prompt: prompt,
    first_message: first_message,
    created_at: createdAt,
    user_chat_id: user_chat_id,
    customer_name: customer_name || null,
    provider: selectedProvider || currentProvider,
    provider_metadata: providerMetadata,
    business_context: functionSystem.context,
    function_count: functionSystem.functions.length,
    purpose: purpose || null,
    business_id: business_id || null,
    script: script || null,
    script_id: normalizedScriptId || null,
    script_version: resolvedScriptVersion || null,
    emotion: emotion || null,
    urgency: urgency || null,
    technical_level: technical_level || null,
    voice_model: voice_model || null,
    collection_profile: collection_profile || null,
    collection_expected_length: collection_expected_length || null,
    collection_timeout_s: collection_timeout_s || null,
    collection_max_retries: collection_max_retries || null,
    collection_mask_for_gpt: collection_mask_for_gpt,
    collection_speak_confirmation: collection_speak_confirmation,
    payment_enabled: paymentEnabled,
    payment_connector: normalizedPayment.normalized.payment_connector || null,
    payment_amount: normalizedPaymentAmount,
    payment_currency: normalizedPaymentCurrency || "USD",
    payment_description:
      normalizedPayment.normalized.payment_description || null,
    payment_start_message:
      normalizedPayment.normalized.payment_start_message || null,
    payment_success_message:
      normalizedPayment.normalized.payment_success_message || null,
    payment_failure_message:
      normalizedPayment.normalized.payment_failure_message || null,
    payment_retry_message:
      normalizedPayment.normalized.payment_retry_message || null,
    payment_policy: normalizedPaymentPolicy,
    payment_state: paymentEnabled ? "ready" : "disabled",
    payment_state_updated_at: createdAt,
    payment_session: null,
    payment_last_result: null,
    script_policy: scriptPolicy,
    flow_state: "normal",
    flow_state_updated_at: createdAt,
    call_mode: "normal",
    digit_capture_active: false,
    inbound: false,
  };
  callConfig.capabilities = buildCallCapabilities(callConfig, {
    provider: selectedProvider || currentProvider,
  });

  callConfigurations.set(callId, callConfig);
  callFunctionSystems.set(callId, functionSystem);
  setCallFlowState(
    callId,
    {
      flow_state: callConfig.flow_state || "normal",
      reason: callConfig.flow_state_reason || "outbound_created",
      call_mode: callConfig.call_mode || "normal",
      digit_capture_active: callConfig.digit_capture_active === true,
      flow_state_updated_at: callConfig.flow_state_updated_at || createdAt,
    },
    { callConfig, skipToolRefresh: true, source: "placeOutboundCall" },
  );
  queuePersistCallRuntimeState(callId, {
    snapshot: { source: "placeOutboundCall" },
  });

  try {
    await db.createCall({
      call_sid: callId,
      phone_number: number,
      prompt: prompt,
      first_message: first_message,
      user_chat_id: user_chat_id,
      business_context: JSON.stringify(functionSystem.context),
      generated_functions: JSON.stringify(
        functionSystem.functions.map((f) => f.function.name),
      ),
      direction: "outbound",
    });
    await db.updateCallState(callId, "call_created", {
      customer_name: customer_name || null,
      business_id: business_id || null,
      script: script || null,
      script_id: normalizedScriptId || null,
      script_version: resolvedScriptVersion || null,
      purpose: purpose || null,
      emotion: emotion || null,
      urgency: urgency || null,
      technical_level: technical_level || null,
      voice_model: voice_model || null,
      provider: selectedProvider || currentProvider,
      provider_metadata: providerMetadata,
      from:
        (selectedProvider || currentProvider) === "twilio"
          ? config.twilio?.fromNumber
          : null,
      to: number || null,
      inbound: false,
      collection_profile: collection_profile || null,
      collection_expected_length: collection_expected_length || null,
      collection_timeout_s: collection_timeout_s || null,
      collection_max_retries: collection_max_retries || null,
      collection_mask_for_gpt: collection_mask_for_gpt,
      collection_speak_confirmation: collection_speak_confirmation,
      payment_enabled: paymentEnabled,
      payment_connector: normalizedPayment.normalized.payment_connector || null,
      payment_amount: normalizedPaymentAmount,
      payment_currency: normalizedPaymentCurrency || "USD",
      payment_description:
        normalizedPayment.normalized.payment_description || null,
      payment_start_message:
        normalizedPayment.normalized.payment_start_message || null,
      payment_success_message:
        normalizedPayment.normalized.payment_success_message || null,
      payment_failure_message:
        normalizedPayment.normalized.payment_failure_message || null,
      payment_retry_message:
        normalizedPayment.normalized.payment_retry_message || null,
      payment_policy: normalizedPaymentPolicy,
      payment_state: callConfig.payment_state || (paymentEnabled ? "ready" : "disabled"),
      payment_state_updated_at:
        callConfig.payment_state_updated_at || new Date().toISOString(),
      capabilities: callConfig.capabilities,
      flow_state: callConfig.flow_state || "normal",
      flow_state_reason: callConfig.flow_state_reason || "call_created",
      flow_state_updated_at:
        callConfig.flow_state_updated_at || new Date().toISOString(),
      call_mode: callConfig.call_mode || "normal",
      digit_capture_active: callConfig.digit_capture_active === true,
    });

    if (user_chat_id) {
      await db.createEnhancedWebhookNotification(
        callId,
        "call_initiated",
        user_chat_id,
      );
    }

    console.log(
      `Enhanced adaptive call created: ${callId} to ${maskPhoneForLog(number)}`,
    );
    console.log(
      `Business context: ${functionSystem.context.industry} - ${functionSystem.context.businessType}`,
    );
  } catch (dbError) {
    console.error("Database error:", dbError);
  }

  return {
    callId,
    callStatus,
    functionSystem,
    provider: selectedProvider || currentProvider,
    warnings: callWarnings,
  };
}

async function processCallStatusWebhookPayload(payload = {}, options = {}) {
  const {
    CallSid,
    CallStatus,
    Duration,
    From,
    To,
    CallDuration,
    AnsweredBy,
    ErrorCode,
    ErrorMessage,
    DialCallDuration,
  } = payload || {};

  if (!CallSid) {
    const err = new Error("Missing CallSid");
    err.code = "missing_call_sid";
    throw err;
  }

  const source = options.source || "provider";
  if (!shouldProcessCallStatusPayload(payload, options)) {
    console.log(`⏭️ Duplicate status webhook ignored for ${CallSid}`);
    return { ok: true, callSid: CallSid, deduped: true };
  }

  console.log(`Fixed Webhook: Call ${CallSid} status: ${CallStatus}`.blue);
  console.log(`Debug Info:`);
  console.log(`Duration: ${Duration || "N/A"}`);
  console.log(`CallDuration: ${CallDuration || "N/A"}`);
  console.log(`DialCallDuration: ${DialCallDuration || "N/A"}`);
  console.log(`AnsweredBy: ${AnsweredBy || "N/A"}`);

  const durationCandidates = [Duration, CallDuration, DialCallDuration]
    .map((value) => parseInt(value, 10))
    .filter((value) => Number.isFinite(value));
  const durationValue = durationCandidates.length
    ? Math.max(...durationCandidates)
    : 0;

  let call = await db.getCall(CallSid);
  if (!call) {
    console.warn(`Webhook received for unknown call: ${CallSid}`);
    call = await ensureCallRecord(CallSid, payload, "status_webhook", {
      provider: "twilio",
      inbound: !isOutboundTwilioDirection(
        payload?.Direction || payload?.direction,
      ),
    });
    if (!call) {
      return { ok: false, error: "call_not_found", callSid: CallSid };
    }
  }

  const streamMediaState = await db
    .getLatestCallState(CallSid, "stream_media")
    .catch(() => null);
  const hasStreamMedia = Boolean(
    streamMediaState?.at || streamMediaState?.timestamp,
  );
  let notificationType = null;
  const rawStatus = String(CallStatus || "").toLowerCase();
  const answeredByValue = String(AnsweredBy || "").toLowerCase();
  const isMachineAnswered = [
    "machine_start",
    "machine_end",
    "machine",
    "fax",
  ].includes(answeredByValue);
  const voicemailDetected = isMachineAnswered;
  let actualStatus = rawStatus || "unknown";
  const priorStatus = String(call.status || "").toLowerCase();
  const hasAnswerEvidence =
    !!call.started_at ||
    ["answered", "in-progress", "completed"].includes(priorStatus) ||
    durationValue > 0 ||
    !!AnsweredBy ||
    hasStreamMedia;

  if (voicemailDetected) {
    console.log(
      `AMD detected voicemail (${answeredByValue}) - classifying as no-answer`
        .yellow,
    );
    actualStatus = "no-answer";
    notificationType = "call_no_answer";
  } else if (actualStatus === "completed") {
    console.log(`Analyzing completed call: Duration = ${durationValue}s`);

    if ((durationValue === 0 || durationValue < 6) && !hasAnswerEvidence) {
      console.log(
        `Short duration detected (${durationValue}s) - treating as no-answer`
          .red,
      );
      actualStatus = "no-answer";
      notificationType = "call_no_answer";
    } else if (voicemailDetected && durationValue < 10 && !hasAnswerEvidence) {
      console.log(
        `Voicemail detected with short duration - classifying as no-answer`.red,
      );
      actualStatus = "no-answer";
      notificationType = "call_no_answer";
    } else {
      console.log(
        `Valid call duration (${durationValue}s) - confirmed answered`,
      );
      actualStatus = "completed";
      notificationType = "call_completed";
    }
  } else {
    switch (actualStatus) {
      case "queued":
      case "initiated":
        notificationType = "call_initiated";
        break;
      case "ringing":
        notificationType = "call_ringing";
        break;
      case "in-progress":
        notificationType = "call_in_progress";
        break;
      case "answered":
        notificationType = "call_answered";
        break;
      case "busy":
        notificationType = "call_busy";
        break;
      case "no-answer":
        notificationType = "call_no_answer";
        break;
      case "voicemail":
        actualStatus = "no-answer";
        notificationType = "call_no_answer";
        break;
      case "failed":
        notificationType = "call_failed";
        break;
      case "canceled":
        notificationType = "call_canceled";
        break;
      default:
        console.warn(`Unknown call status: ${CallStatus}`);
        notificationType = `call_${actualStatus}`;
    }
  }

  if (actualStatus === "no-answer" && hasAnswerEvidence && !voicemailDetected) {
    actualStatus = "completed";
    notificationType = "call_completed";
  }

  console.log(
    `Final determination: ${CallStatus} → ${actualStatus} → ${notificationType}`,
  );

  const updateData = {
    duration: durationValue,
    twilio_status: CallStatus,
    answered_by: AnsweredBy,
    error_code: ErrorCode,
    error_message: ErrorMessage,
  };

  const applyStatus = shouldApplyStatusUpdate(priorStatus, actualStatus, {
    allowTerminalUpgrade: actualStatus === "completed",
  });
  const finalStatus = applyStatus
    ? actualStatus
    : normalizeCallStatus(priorStatus || actualStatus);
  const statusChanged = normalizeCallStatus(priorStatus) !== finalStatus;
  const finalNotificationType =
    applyStatus && statusChanged ? notificationType : null;

  if (applyStatus && actualStatus === "ringing") {
    try {
      await db.updateCallState(CallSid, "ringing", {
        at: new Date().toISOString(),
      });
    } catch (stateError) {
      console.error("Failed to record ringing state:", stateError);
    }
  }

  if (applyStatus && actualStatus === "no-answer" && call.created_at) {
    let ringStart = null;
    try {
      const ringState = await db.getLatestCallState(CallSid, "ringing");
      ringStart = ringState?.at || ringState?.timestamp || null;
    } catch (stateError) {
      console.error("Failed to load ringing state:", stateError);
    }

    const now = new Date();
    const callStart = new Date(call.created_at);
    const ringStartTime = ringStart ? new Date(ringStart) : callStart;
    const ringDuration = Math.round((now - ringStartTime) / 1000);
    updateData.ring_duration = ringDuration;
    if (!updateData.duration || updateData.duration < ringDuration) {
      updateData.duration = ringDuration;
    }
    console.log(`Calculated ring duration: ${ringDuration}s`);
  }

  if (
    applyStatus &&
    ["in-progress", "answered"].includes(actualStatus) &&
    !call.started_at
  ) {
    updateData.started_at = new Date().toISOString();
  } else if (applyStatus && !call.ended_at) {
    const isTerminal = [
      "completed",
      "no-answer",
      "failed",
      "busy",
      "canceled",
    ].includes(actualStatus);
    const rawTerminal = [
      "completed",
      "no-answer",
      "failed",
      "busy",
      "canceled",
    ].includes(rawStatus);
    if (isTerminal && rawTerminal) {
      updateData.ended_at = new Date().toISOString();
    }
  }

  await db.updateCallStatus(CallSid, finalStatus, updateData);
  if (applyStatus && statusChanged) {
    recordCallLifecycle(CallSid, finalStatus, {
      source,
      raw_status: CallStatus,
      answered_by: AnsweredBy,
      duration: updateData.duration,
    });
    if (isTerminalStatusKey(finalStatus)) {
      scheduleCallLifecycleCleanup(CallSid);
    }
  }

  if (
    call.user_chat_id &&
    finalNotificationType &&
    !options.skipNotifications
  ) {
    try {
      await db.createEnhancedWebhookNotification(
        CallSid,
        finalNotificationType,
        call.user_chat_id,
      );
      console.log(
        `📨 Created corrected ${finalNotificationType} notification for call ${CallSid}`,
      );

      if (actualStatus !== CallStatus.toLowerCase()) {
        await db.logServiceHealth("webhook_system", "status_corrected", {
          call_sid: CallSid,
          original_status: CallStatus,
          corrected_status: actualStatus,
          duration: updateData.duration,
          reason: "Short duration analysis",
          source,
        });
      }
    } catch (notificationError) {
      console.error(
        "Error creating enhanced webhook notification:",
        notificationError,
      );
    }
  }

  console.log(
    `Fixed webhook processed: ${CallSid} -> ${CallStatus} (corrected to: ${actualStatus})`,
  );
  if (updateData.duration) {
    const minutes = Math.floor(updateData.duration / 60);
    const seconds = updateData.duration % 60;
    console.log(
      `Call metrics: ${minutes}:${String(seconds).padStart(2, "0")} duration`,
    );
  }

  await db.logServiceHealth("webhook_system", "status_received", {
    call_sid: CallSid,
    original_status: CallStatus,
    final_status: actualStatus,
    duration: updateData.duration,
    answered_by: AnsweredBy,
    correction_applied: actualStatus !== CallStatus.toLowerCase(),
    source,
  });

  return {
    ok: true,
    callSid: CallSid,
    rawStatus,
    actualStatus,
    notificationType,
    duration: updateData.duration,
    voicemailDetected,
  };
}

function escapeCsvValue(value) {
  if (value === null || value === undefined) return "";
  const str = String(value);
  if (/[",\n]/.test(str)) {
    return `"${str.replace(/"/g, '""')}"`;
  }
  return str;
}

function callsToCsv(calls = []) {
  const headers = [
    "call_sid",
    "status",
    "direction",
    "phone_number",
    "created_at",
    "duration",
    "answered_by",
    "error_code",
    "error_message",
  ];
  const lines = [headers.join(",")];
  for (const call of calls) {
    const row = headers.map((key) => escapeCsvValue(call?.[key]));
    lines.push(row.join(","));
  }
  return lines.join("\n");
}

function normalizeDateFilter(value, isEnd = false) {
  if (!value) return null;
  const raw = String(value).trim();
  if (!raw) return null;
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    return `${raw} ${isEnd ? "23:59:59" : "00:00:00"}`;
  }
  return raw;
}

async function handleInboundAdminDecision(callSid, action, adminId) {
  if (!callSid) {
    return { ok: false, error: "missing_call_sid" };
  }
  const callRecord = await db.getCall(callSid).catch(() => null);
  if (!callRecord) {
    return { ok: false, error: "call_not_found" };
  }
  const gate = webhookService.getInboundGate(callSid);
  if (
    gate?.status === "answered" ||
    gate?.status === "declined" ||
    gate?.status === "expired"
  ) {
    return { ok: false, error: "already_handled", status: gate?.status };
  }
  if (action === "answer") {
    webhookService.setInboundGate(callSid, "answered", { chatId: adminId });
    webhookService.setConsoleCompact(callSid, false);
    webhookService.addLiveEvent(callSid, "✅ Admin answered", { force: true });
    await db
      .updateCallState(callSid, "admin_answered", {
        at: new Date().toISOString(),
        by: adminId,
      })
      .catch(() => {});
    await connectInboundCall(callSid);
    return { ok: true, status: "answered" };
  }
  if (action === "decline") {
    webhookService.setInboundGate(callSid, "declined", { chatId: adminId });
    webhookService.addLiveEvent(callSid, "❌ Declined by admin", {
      force: true,
    });
    await db
      .updateCallState(callSid, "admin_declined", {
        at: new Date().toISOString(),
        by: adminId,
      })
      .catch(() => {});
    await endCallForProvider(callSid);
    await webhookService.setLiveCallPhase(callSid, "ended").catch(() => {});
    return { ok: true, status: "declined" };
  }
  return { ok: false, error: "invalid_action" };
}

function truncatePrompt(value, limit = 800) {
  const text = String(value || "").trim();
  if (!text) return "";
  if (text.length <= limit) return text;
  return `${text.slice(0, limit)}...`;
}

async function applyScriptInjection(callSid, scriptId, userId) {
  if (!callSid || !scriptId) return { ok: false, error: "missing_script" };
  if (!db) return { ok: false, error: "db_unavailable" };
  const script = normalizeScriptTemplateRecord(
    await db.getCallTemplateById(scriptId),
  );
  if (!script) return { ok: false, error: "script_not_found" };
  const callConfig = callConfigurations.get(callSid);
  if (!callConfig) return { ok: false, error: "call_not_active" };
  callConfig.script = script.name || callConfig.script;
  callConfig.script_id = script.id || callConfig.script_id;
  callConfig.script_version = script.version || callConfig.script_version || 1;
  callConfig.payment_policy = script.payment_policy || null;
  if (script.prompt) {
    callConfig.prompt = script.prompt;
  }
  if (script.first_message) {
    callConfig.first_message = script.first_message;
  }
  callConfigurations.set(callSid, callConfig);

  const session = activeCalls.get(callSid);
  if (session?.gptService) {
    const promptPreview = truncatePrompt(script.prompt || "");
    const intentLine = `Injected script: ${script.name || "custom"}. ${promptPreview}`;
    session.gptService.setCallIntent(intentLine);
  }

  await db
    .updateCallState(callSid, "script_injected", {
      script_id: script.id,
      script_name: script.name || null,
      script_version: script.version || null,
      payment_policy: script.payment_policy || null,
      user_id: userId || null,
      at: new Date().toISOString(),
    })
    .catch(() => {});

  return { ok: true, script };
}

function getInboundHealthContext() {
  const inboundDefaultSummary = inboundDefaultScript
    ? {
        mode: "script",
        script_id: inboundDefaultScriptId,
        name: inboundDefaultScript.name,
      }
    : { mode: "builtin" };
  const inboundEnvSummary = {
    prompt: Boolean(config.inbound?.defaultPrompt),
    first_message: Boolean(config.inbound?.defaultFirstMessage),
  };
  return { inboundDefaultSummary, inboundEnvSummary };
}

function safeJsonParse(value, fallback = null) {
  if (value == null || value === "") return fallback;
  if (typeof value === "object") return value;
  try {
    return JSON.parse(value);
  } catch (_) {
    return fallback;
  }
}

function normalizeCallRecordForApi(call) {
  if (!call || typeof call !== "object") return call;
  const normalized = { ...call };
  const rawStatus = normalized.status || normalized.twilio_status || "";
  normalized.status_normalized = normalizeCallStatus(rawStatus);
  normalized.duration = Number.isFinite(Number(normalized.duration))
    ? Number(normalized.duration)
    : 0;
  normalized.digit_summary =
    typeof normalized.digit_summary === "string" ? normalized.digit_summary : "";
  normalized.digit_count = Number.isFinite(Number(normalized.digit_count))
    ? Number(normalized.digit_count)
    : 0;
  normalized.business_context = safeJsonParse(normalized.business_context, null);
  normalized.generated_functions = safeJsonParse(
    normalized.generated_functions,
    [],
  );
  return normalized;
}

app.get("/webhook/twilio-tts", (req, res) => {
  const key = String(req.query?.key || "").trim();
  if (!key) {
    res.status(400).send("Missing key");
    return;
  }
  const entry = twilioTtsCache.get(key);
  if (!entry || entry.expiresAt <= Date.now()) {
    twilioTtsCache.delete(key);
    res.status(404).send("Not found");
    return;
  }
  res.set(
    "Cache-Control",
    `public, max-age=${Math.floor(TWILIO_TTS_CACHE_TTL_MS / 1000)}`,
  );
  res.type(entry.contentType || "audio/wav");
  res.send(entry.buffer);
});

const twilioGatherHandler = createTwilioGatherHandler({
  warnOnInvalidTwilioSignature,
  requireTwilioSignature: requireValidTwilioSignature,
  getDigitService: () => digitService,
  callConfigurations,
  config,
  VoiceResponse,
  webhookService,
  resolveHost,
  buildTwilioStreamTwiml,
  clearPendingDigitReprompts,
  callEndLocks,
  gatherEventDedupe,
  maskDigitsForLog,
  callEndMessages: CALL_END_MESSAGES,
  closingMessage: CLOSING_MESSAGE,
  queuePendingDigitAction,
  getTwilioTtsAudioUrl,
  ttsTimeoutMs: Number(config.twilio?.ttsMaxWaitMs) || 1200,
  shouldUseTwilioPlay,
  resolveTwilioSayVoice,
  isGroupedGatherPlan,
  setCallFlowState,
});

// Twilio Gather fallback handler (DTMF)
const handleTwilioGatherWebhook = twilioGatherHandler;

// Email API endpoints
app.post("/email/send", requireOutboundAuthorization, async (req, res) => {
  try {
    if (!emailService) {
      return sendApiError(
        res,
        500,
        "email_service_unavailable",
        "Email service not initialized",
        req.requestId,
      );
    }
    const limitResponse = await enforceOutboundRateLimits(req, res, {
      namespace: "email_send",
      actorKey: getOutboundActorKey(req),
      perUserLimit: Number(config.outboundLimits?.email?.perUser) || 20,
      globalLimit: Number(config.outboundLimits?.email?.global) || 120,
      windowMs: Number(config.outboundLimits?.windowMs) || 60000,
    });
    if (limitResponse) {
      return;
    }
    const emailPayload = { ...(req.body || {}) };
    if (!emailPayload.provider) {
      emailPayload.provider = getActiveEmailProvider();
    }
    const idempotencyKey =
      req.headers["idempotency-key"] || req.headers["Idempotency-Key"];
    console.log("email_send_request", {
      request_id: req.requestId || null,
      to: redactSensitiveLogValue(req.body?.to || ""),
      from: redactSensitiveLogValue(req.body?.from || ""),
      actor: getOutboundActorKey(req),
      provider: String(emailPayload.provider || "unknown").toLowerCase(),
      idempotency_key: idempotencyKey ? "present" : "absent",
    });
    const result = await runWithTimeout(
      emailService.enqueueEmail(emailPayload, {
        idempotencyKey,
      }),
      Number(config.outboundLimits?.handlerTimeoutMs) || 30000,
      "email send handler",
      "email_handler_timeout",
    );
    res.json({
      success: true,
      message_id: result.message_id,
      deduped: result.deduped || false,
      suppressed: result.suppressed || false,
      request_id: req.requestId || null,
    });
  } catch (error) {
    const status =
      error.code === "idempotency_conflict"
        ? 409
        : error.code === "idempotency_in_progress"
        ? 409
        : error.code === "email_handler_timeout"
          ? 504
          : error.code === "missing_variables" || error.code === "validation_error"
            ? 400
            : 500;
    console.error("email_send_error", {
      request_id: req.requestId || null,
      actor: getOutboundActorKey(req),
      error: redactSensitiveLogValue(error.message || "email_send_failed"),
      code: error.code || null,
    });
    sendApiError(
      res,
      status,
      error.code || "email_send_failed",
      error.message || "Email send failed",
      req.requestId,
      { missing: error.missing },
    );
  }
});

app.post("/email/bulk", requireOutboundAuthorization, async (req, res) => {
  try {
    if (!emailService) {
      return sendApiError(
        res,
        500,
        "email_service_unavailable",
        "Email service not initialized",
        req.requestId,
      );
    }
    const limitResponse = await enforceOutboundRateLimits(req, res, {
      namespace: "email_bulk",
      actorKey: getOutboundActorKey(req),
      perUserLimit: Number(config.outboundLimits?.email?.perUser) || 20,
      globalLimit: Number(config.outboundLimits?.email?.global) || 120,
      windowMs: Number(config.outboundLimits?.windowMs) || 60000,
    });
    if (limitResponse) {
      return;
    }
    const bulkPayload = { ...(req.body || {}) };
    if (!bulkPayload.provider) {
      bulkPayload.provider = getActiveEmailProvider();
    }
    const idempotencyKey =
      req.headers["idempotency-key"] || req.headers["Idempotency-Key"];
    const recipientCount = Array.isArray(req.body?.recipients)
      ? req.body.recipients.length
      : 0;
    console.log("email_bulk_request", {
      request_id: req.requestId || null,
      actor: getOutboundActorKey(req),
      recipients: recipientCount,
      provider: String(bulkPayload.provider || "unknown").toLowerCase(),
      idempotency_key: idempotencyKey ? "present" : "absent",
    });
    const result = await runWithTimeout(
      emailService.enqueueBulk(bulkPayload, {
        idempotencyKey,
      }),
      Number(config.outboundLimits?.handlerTimeoutMs) || 30000,
      "email bulk handler",
      "email_handler_timeout",
    );
    res.json({
      success: true,
      bulk_job_id: result.bulk_job_id,
      deduped: result.deduped || false,
      request_id: req.requestId || null,
    });
  } catch (error) {
    const status =
      error.code === "idempotency_conflict"
        ? 409
        : error.code === "idempotency_in_progress"
        ? 409
        : error.code === "email_handler_timeout"
          ? 504
          : error.code === "validation_error"
            ? 400
            : 500;
    console.error("email_bulk_error", {
      request_id: req.requestId || null,
      actor: getOutboundActorKey(req),
      error: redactSensitiveLogValue(error.message || "email_bulk_failed"),
      code: error.code || null,
    });
    sendApiError(
      res,
      status,
      error.code || "email_bulk_failed",
      error.message || "Bulk email enqueue failed",
      req.requestId,
    );
  }
});

app.post("/email/preview", requireOutboundAuthorization, async (req, res) => {
  try {
    if (!emailService) {
      return res
        .status(500)
        .json({ success: false, error: "Email service not initialized" });
    }
    const result = await emailService.previewScript(req.body || {});
    res.json({ success: result.ok, ...result });
  } catch (error) {
    res.status(400).json({ success: false, error: error.message });
  }
});

function extractEmailTemplateVariables(text = "") {
  if (!text) return [];
  const matches = text.match(/{{\s*([\w.-]+)\s*}}/g) || [];
  const vars = new Set();
  matches.forEach((match) => {
    const cleaned = match.replace(/{{|}}/g, "").trim();
    if (cleaned) vars.add(cleaned);
  });
  return Array.from(vars);
}

function buildRequiredVars(subject, html, text) {
  const required = new Set();
  extractEmailTemplateVariables(subject).forEach((v) => required.add(v));
  extractEmailTemplateVariables(html).forEach((v) => required.add(v));
  extractEmailTemplateVariables(text).forEach((v) => required.add(v));
  return Array.from(required);
}

app.get("/email/templates", requireOutboundAuthorization, async (req, res) => {
  try {
    const limit = Math.min(Math.max(parseInt(req.query?.limit, 10) || 50, 1), 200);
    const templates = await db.listEmailTemplates(limit);
    res.json({ success: true, templates });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

app.get("/email/templates/:id", requireOutboundAuthorization, async (req, res) => {
  try {
    const templateId = req.params.id;
    if (!isSafeId(templateId, { max: 128 })) {
      return res.status(400).json({ success: false, error: "Invalid template identifier" });
    }
    const template = await db.getEmailTemplate(templateId);
    if (!template) {
      return res
        .status(404)
        .json({ success: false, error: "Template not found" });
    }
    res.json({ success: true, template });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

app.post("/email/templates", requireOutboundAuthorization, async (req, res) => {
  try {
    const payload = req.body || {};
    const templateId = String(payload.template_id || "").trim();
    if (!templateId) {
      return res
        .status(400)
        .json({ success: false, error: "template_id is required" });
    }
    if (!isSafeId(templateId, { max: 128 })) {
      return res
        .status(400)
        .json({ success: false, error: "Invalid template identifier" });
    }
    const subject = payload.subject || "";
    const html = payload.html || "";
    const text = payload.text || "";
    const maxSubjectChars = Number(config.email?.maxSubjectChars) || 200;
    const maxBodyChars = Number(config.email?.maxBodyChars) || 200000;
    if (!subject) {
      return res
        .status(400)
        .json({ success: false, error: "subject is required" });
    }
    if (String(subject).length > maxSubjectChars) {
      return res.status(400).json({
        success: false,
        error: `subject exceeds ${maxSubjectChars} characters`,
      });
    }
    if (!html && !text) {
      return res
        .status(400)
        .json({ success: false, error: "html or text is required" });
    }
    if (String(text || "").length > maxBodyChars) {
      return res.status(400).json({
        success: false,
        error: `text exceeds ${maxBodyChars} characters`,
      });
    }
    if (String(html || "").length > maxBodyChars) {
      return res.status(400).json({
        success: false,
        error: `html exceeds ${maxBodyChars} characters`,
      });
    }
    const requiredVars = buildRequiredVars(subject, html, text);
    await db.createEmailTemplate({
      template_id: templateId,
      subject,
      html,
      text,
      required_vars: JSON.stringify(requiredVars),
    });
    const template = await db.getEmailTemplate(templateId);
    res.json({ success: true, template });
  } catch (error) {
    res.status(400).json({ success: false, error: error.message });
  }
});

app.put("/email/templates/:id", requireOutboundAuthorization, async (req, res) => {
  try {
    const templateId = req.params.id;
    if (!isSafeId(templateId, { max: 128 })) {
      return res.status(400).json({ success: false, error: "Invalid template identifier" });
    }
    const existing = await db.getEmailTemplate(templateId);
    if (!existing) {
      return res
        .status(404)
        .json({ success: false, error: "Template not found" });
    }
    const payload = req.body || {};
    const subject =
      payload.subject !== undefined ? payload.subject : existing.subject;
    const html = payload.html !== undefined ? payload.html : existing.html;
    const text = payload.text !== undefined ? payload.text : existing.text;
    const maxSubjectChars = Number(config.email?.maxSubjectChars) || 200;
    const maxBodyChars = Number(config.email?.maxBodyChars) || 200000;
    if (!subject) {
      return res.status(400).json({ success: false, error: "subject is required" });
    }
    if (String(subject).length > maxSubjectChars) {
      return res.status(400).json({
        success: false,
        error: `subject exceeds ${maxSubjectChars} characters`,
      });
    }
    if (!html && !text) {
      return res.status(400).json({ success: false, error: "html or text is required" });
    }
    if (String(text || "").length > maxBodyChars) {
      return res.status(400).json({
        success: false,
        error: `text exceeds ${maxBodyChars} characters`,
      });
    }
    if (String(html || "").length > maxBodyChars) {
      return res.status(400).json({
        success: false,
        error: `html exceeds ${maxBodyChars} characters`,
      });
    }
    const requiredVars = buildRequiredVars(
      subject || "",
      html || "",
      text || "",
    );
    await db.updateEmailTemplate(templateId, {
      subject: payload.subject,
      html: payload.html,
      text: payload.text,
      required_vars: JSON.stringify(requiredVars),
    });
    const template = await db.getEmailTemplate(templateId);
    res.json({ success: true, template });
  } catch (error) {
    res.status(400).json({ success: false, error: error.message });
  }
});

app.delete("/email/templates/:id", requireOutboundAuthorization, async (req, res) => {
  try {
    const templateId = req.params.id;
    if (!isSafeId(templateId, { max: 128 })) {
      return res.status(400).json({ success: false, error: "Invalid template identifier" });
    }
    await db.deleteEmailTemplate(templateId);
    res.json({ success: true });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

function normalizeEmailMessageForApi(message) {
  if (!message || typeof message !== "object") return message;
  const normalized = { ...message };
  if ("template_id" in normalized) {
    normalized.script_id = normalized.template_id;
    delete normalized.template_id;
  }
  return normalized;
}

function normalizeEmailJobForApi(job) {
  if (!job || typeof job !== "object") return job;
  const normalized = { ...job };
  if ("template_id" in normalized) {
    normalized.script_id = normalized.template_id;
    delete normalized.template_id;
  }
  return normalized;
}

app.get("/email/messages/:id", requireOutboundAuthorization, async (req, res) => {
  try {
    const messageId = req.params.id;
    if (!isSafeId(messageId, { max: 128 })) {
      return res.status(400).json({ success: false, error: "Invalid message identifier" });
    }
    const message = await db.getEmailMessage(messageId);
    if (!message) {
      return res
        .status(404)
        .json({ success: false, error: "Message not found" });
    }
    const events = await db.listEmailEvents(messageId);
    res.json({
      success: true,
      message: normalizeEmailMessageForApi(message),
      events,
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

app.get("/email/bulk/:jobId", requireOutboundAuthorization, async (req, res) => {
  try {
    const jobId = req.params.jobId;
    if (!isSafeId(jobId, { max: 128 })) {
      return res.status(400).json({ success: false, error: "Invalid bulk job identifier" });
    }
    const job = await db.getEmailBulkJob(jobId);
    if (!job) {
      return res
        .status(404)
        .json({ success: false, error: "Bulk job not found" });
    }
    res.json({ success: true, job: normalizeEmailJobForApi(job) });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

app.get("/email/bulk/history", requireOutboundAuthorization, async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit, 10) || 10, 50);
    const offset = Math.max(parseInt(req.query.offset, 10) || 0, 0);
    const jobs = await db.getEmailBulkJobs({ limit, offset });
    res.json({
      success: true,
      jobs: jobs.map(normalizeEmailJobForApi),
      limit,
      offset,
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

app.get("/email/bulk/stats", requireOutboundAuthorization, async (req, res) => {
  try {
    const hours = Math.min(
      Math.max(parseInt(req.query.hours, 10) || 24, 1),
      720,
    );
    const stats = await db.getEmailBulkStats({ hours });
    res.json({ success: true, stats, hours });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

function isPlainObject(value) {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function normalizeSmsProviderInput(value) {
  if (value === undefined || value === null || value === "") {
    return { value: null };
  }
  const normalized = String(value).trim().toLowerCase();
  if (!normalized) {
    return { value: null };
  }
  if (!SUPPORTED_SMS_PROVIDERS.includes(normalized)) {
    return {
      error: `Provider must be one of: ${SUPPORTED_SMS_PROVIDERS.join(", ")}`,
    };
  }
  return { value: normalized };
}

function normalizeSmsIdempotencyInput(value) {
  if (value === undefined || value === null || value === "") {
    return { value: null };
  }
  const normalized = String(value).trim();
  if (!normalized || normalized.length > 128) {
    return { error: "Idempotency key must be between 1 and 128 characters" };
  }
  return { value: normalized };
}

function normalizeSmsQuietHoursInput(value) {
  if (value === undefined || value === null) {
    return { value: null };
  }
  if (!isPlainObject(value)) {
    return { error: "quiet_hours must be an object with start/end hours" };
  }
  const start = Number(value.start);
  const end = Number(value.end);
  const isValidHour = (hour) => Number.isInteger(hour) && hour >= 0 && hour <= 23;
  if (!isValidHour(start) || !isValidHour(end) || start === end) {
    return {
      error: "quiet_hours start/end must be different integers between 0 and 23",
    };
  }
  return { value: { start, end } };
}

function normalizeSmsMediaUrlInput(value) {
  if (value === undefined || value === null || value === "") {
    return { value: null };
  }
  const mediaList = Array.isArray(value) ? value : [value];
  if (!mediaList.length || mediaList.length > 5) {
    return { error: "media_url supports between 1 and 5 URLs" };
  }
  const normalized = [];
  for (const item of mediaList) {
    const mediaUrl = String(item || "").trim();
    if (!mediaUrl) {
      return { error: "media_url contains an empty value" };
    }
    try {
      new URL(mediaUrl);
    } catch (_) {
      return { error: "media_url contains an invalid URL" };
    }
    normalized.push(mediaUrl);
  }
  return { value: normalized.length === 1 ? normalized[0] : normalized };
}

function normalizeSmsRecipientsInput(value) {
  if (!Array.isArray(value) || value.length === 0) {
    return {
      error: "Recipients array is required and must not be empty",
    };
  }
  const normalized = [];
  for (const entry of value) {
    const phone = String(entry || "").trim();
    if (!phone) {
      return { error: "Recipients must contain non-empty phone numbers" };
    }
    if (!phone.match(/^\+[1-9]\d{1,14}$/)) {
      return { error: "Recipients must use E.164 phone format (e.g., +1234567890)" };
    }
    normalized.push(phone);
  }
  return { value: normalized };
}

function normalizeSmsChatIdInput(value) {
  if (value === undefined || value === null || value === "") {
    return { value: null };
  }
  const normalized = String(value).trim();
  if (!normalized || normalized.length > 64) {
    return { error: "user_chat_id must be a non-empty string up to 64 characters" };
  }
  return { value: normalized };
}

function normalizeSmsFromInput(value) {
  if (value === undefined || value === null || value === "") {
    return { value: null };
  }
  const normalized = String(value).trim();
  if (!normalized) {
    return { error: "from must be a non-empty string when provided" };
  }
  if (normalized.length > 64) {
    return { error: "from must be 64 characters or fewer" };
  }
  return { value: normalized };
}

function normalizeSmsBooleanInput(value, fieldName) {
  if (value === undefined || value === null) {
    return { value: null };
  }
  if (typeof value !== "boolean") {
    return { error: `${fieldName} must be true or false when provided` };
  }
  return { value };
}

// Send single SMS endpoint
app.post("/api/sms/send", requireOutboundAuthorization, async (req, res) => {
  try {
    if (!isPlainObject(req.body)) {
      return sendApiError(
        res,
        400,
        "sms_validation_failed",
        "Request body must be a JSON object",
        req.requestId,
      );
    }
    const {
      to,
      message,
      from,
      user_chat_id,
      options = {},
      idempotency_key,
      allow_quiet_hours,
      quiet_hours,
      media_url,
      provider,
    } = req.body;
    const idempotencyHeader = req.headers["idempotency-key"];
    const toNumber = String(to || "").trim();
    const messageText = typeof message === "string" ? message : "";

    if (!isPlainObject(options)) {
      return sendApiError(
        res,
        400,
        "sms_validation_failed",
        "options must be an object when provided",
        req.requestId,
      );
    }

    const parsedProvider = normalizeSmsProviderInput(provider);
    if (parsedProvider.error) {
      return sendApiError(
        res,
        400,
        "sms_validation_failed",
        parsedProvider.error,
        req.requestId,
      );
    }

    const parsedIdempotency = normalizeSmsIdempotencyInput(
      idempotency_key || idempotencyHeader,
    );
    if (parsedIdempotency.error) {
      return sendApiError(
        res,
        400,
        "sms_validation_failed",
        parsedIdempotency.error,
        req.requestId,
      );
    }

    const parsedQuietHours = normalizeSmsQuietHoursInput(quiet_hours);
    if (parsedQuietHours.error) {
      return sendApiError(
        res,
        400,
        "sms_validation_failed",
        parsedQuietHours.error,
        req.requestId,
      );
    }

    const parsedMediaUrl = normalizeSmsMediaUrlInput(media_url);
    if (parsedMediaUrl.error) {
      return sendApiError(
        res,
        400,
        "sms_validation_failed",
        parsedMediaUrl.error,
        req.requestId,
      );
    }

    const parsedUserChatId = normalizeSmsChatIdInput(user_chat_id);
    if (parsedUserChatId.error) {
      return sendApiError(
        res,
        400,
        "sms_validation_failed",
        parsedUserChatId.error,
        req.requestId,
      );
    }

    const parsedFrom = normalizeSmsFromInput(from);
    if (parsedFrom.error) {
      return sendApiError(
        res,
        400,
        "sms_validation_failed",
        parsedFrom.error,
        req.requestId,
      );
    }

    const parsedAllowQuietHours = normalizeSmsBooleanInput(
      allow_quiet_hours,
      "allow_quiet_hours",
    );
    if (parsedAllowQuietHours.error) {
      return sendApiError(
        res,
        400,
        "sms_validation_failed",
        parsedAllowQuietHours.error,
        req.requestId,
      );
    }

    if (!toNumber || !messageText.trim()) {
      return sendApiError(
        res,
        400,
        "sms_validation_failed",
        "Phone number and message are required",
        req.requestId,
      );
    }

    // Validate phone number format
    if (!toNumber.match(/^\+[1-9]\d{1,14}$/)) {
      return sendApiError(
        res,
        400,
        "sms_validation_failed",
        "Invalid phone number format. Use E.164 format (e.g., +1234567890)",
        req.requestId,
      );
    }

    const maxSmsChars = Number(config.sms?.maxMessageChars) || 1600;
    if (messageText.length > maxSmsChars) {
      return sendApiError(
        res,
        400,
        "sms_validation_failed",
        `Message exceeds ${maxSmsChars} characters`,
        req.requestId,
      );
    }

    const limitResponse = await enforceOutboundRateLimits(req, res, {
      namespace: "sms_send",
      actorKey: getOutboundActorKey(req, parsedUserChatId.value),
      perUserLimit: Number(config.outboundLimits?.sms?.perUser) || 15,
      globalLimit: Number(config.outboundLimits?.sms?.global) || 120,
      windowMs: Number(config.outboundLimits?.windowMs) || 60000,
    });
    if (limitResponse) {
      return;
    }

    const smsOptions = { ...options };
    if (parsedIdempotency.value && !smsOptions.idempotencyKey) {
      smsOptions.idempotencyKey = parsedIdempotency.value;
    }
    if (parsedAllowQuietHours.value !== null) {
      smsOptions.allowQuietHours = parsedAllowQuietHours.value;
    }
    if (parsedQuietHours.value && !smsOptions.quietHours) {
      smsOptions.quietHours = parsedQuietHours.value;
    }
    if (parsedMediaUrl.value && !smsOptions.mediaUrl) {
      smsOptions.mediaUrl = parsedMediaUrl.value;
    }
    if (parsedUserChatId.value && !smsOptions.userChatId) {
      smsOptions.userChatId = parsedUserChatId.value;
    }
    if (parsedProvider.value && !smsOptions.provider) {
      smsOptions.provider = parsedProvider.value;
    }
    if (!Object.prototype.hasOwnProperty.call(smsOptions, "durable")) {
      smsOptions.durable = false;
    }
    if (typeof smsOptions.durable !== "boolean") {
      return sendApiError(
        res,
        400,
        "sms_validation_failed",
        "options.durable must be a boolean when provided",
        req.requestId,
      );
    }

    console.log("sms_send_request", {
      request_id: req.requestId || null,
      actor: getOutboundActorKey(req, parsedUserChatId.value),
      to: maskPhoneForLog(toNumber),
      body: maskSmsBodyForLog(messageText),
      provider: smsOptions.provider || getActiveSmsProvider(),
      idempotency_key: smsOptions.idempotencyKey ? "present" : "absent",
      durable: smsOptions.durable === true,
    });

    if (smsOptions.durable === true) {
      if (!db?.createCallJob) {
        return sendApiError(
          res,
          503,
          "sms_queue_unavailable",
          "Durable SMS queue is unavailable",
          req.requestId,
        );
      }
      const durableSmsOptions = { ...smsOptions };
      delete durableSmsOptions.durable;
      const queued = await runWithTimeout(
        smsService.scheduleSMS(toNumber, messageText, new Date(), {
          reason: "durable_send",
          from: parsedFrom.value,
          userChatId: parsedUserChatId.value,
          idempotencyKey: smsOptions.idempotencyKey || null,
          smsOptions: durableSmsOptions,
        }),
        Number(config.outboundLimits?.handlerTimeoutMs) || 30000,
        "sms durable send handler",
        "sms_handler_timeout",
      );
      return res.status(202).json({
        success: true,
        queued: true,
        ...queued,
        request_id: req.requestId || null,
      });
    }

    const result = await runWithTimeout(
      smsService.sendSMS(toNumber, messageText, parsedFrom.value, smsOptions),
      Number(config.outboundLimits?.handlerTimeoutMs) || 30000,
      "sms send handler",
      "sms_handler_timeout",
    );

    // Save to database
    if (db && result.message_sid && result.idempotent !== true) {
      try {
        await db.saveSMSMessage({
          message_sid: result.message_sid,
          to_number: toNumber,
          from_number: result.from,
          body: messageText,
          status: result.status,
          direction: "outbound",
          provider: result.provider || smsOptions.provider || getActiveSmsProvider(),
          user_chat_id: parsedUserChatId.value,
        });
      } catch (saveError) {
        const saveMsg = String(saveError?.message || "");
        if (
          !saveMsg.includes("UNIQUE constraint failed") &&
          !saveMsg.includes("SQLITE_CONSTRAINT")
        ) {
          throw saveError;
        }
      }

      // Create webhook notification
      if (parsedUserChatId.value) {
        await db.createEnhancedWebhookNotification(
          result.message_sid,
          "sms_sent",
          parsedUserChatId.value,
        );
      }
    }

    res.json({
      success: true,
      ...result,
      request_id: req.requestId || null,
    });
  } catch (error) {
    const providerStatus = Number(
      error?.status || error?.statusCode || error?.response?.status,
    );
    const status =
      error.code === "idempotency_conflict"
        ? 409
        : error.code === "sms_validation_failed"
          ? 400
          : error.code === "sms_handler_timeout" ||
              error.code === "sms_provider_timeout" ||
              error.code === "sms_timeout"
            ? 504
            : providerStatus === 429
              ? 429
              : providerStatus >= 400 && providerStatus < 500
                ? 400
                : providerStatus >= 500
                  ? 502
                  : error.code === "sms_config_error"
                    ? 500
                    : 500;
    console.error("sms_send_error", {
      request_id: req.requestId || null,
      actor: getOutboundActorKey(req, req.body?.user_chat_id),
      to: maskPhoneForLog(req.body?.to || ""),
      error: redactSensitiveLogValue(error.message || "sms_send_failed"),
      code: error.code || null,
    });
    sendApiError(
      res,
      status,
      error.code || "sms_send_failed",
      error.message || "Failed to send SMS",
      req.requestId,
    );
  }
});

// Send bulk SMS endpoint
app.post("/api/sms/bulk", requireOutboundAuthorization, async (req, res) => {
  try {
    if (!isPlainObject(req.body)) {
      return sendApiError(
        res,
        400,
        "sms_validation_failed",
        "Request body must be a JSON object",
        req.requestId,
      );
    }
    const {
      recipients,
      message,
      options = {},
      user_chat_id,
      from,
      sms_options,
      idempotency_key,
      provider,
    } = req.body;
    const idempotencyHeader = req.headers["idempotency-key"];
    const messageText = typeof message === "string" ? message : "";

    if (!isPlainObject(options)) {
      return sendApiError(
        res,
        400,
        "sms_validation_failed",
        "options must be an object when provided",
        req.requestId,
      );
    }

    if (sms_options !== undefined && sms_options !== null && !isPlainObject(sms_options)) {
      return sendApiError(
        res,
        400,
        "sms_validation_failed",
        "sms_options must be an object when provided",
        req.requestId,
      );
    }

    const parsedRecipients = normalizeSmsRecipientsInput(recipients);
    if (parsedRecipients.error) {
      return sendApiError(
        res,
        400,
        "sms_validation_failed",
        parsedRecipients.error,
        req.requestId,
      );
    }

    if (!messageText.trim()) {
      return sendApiError(
        res,
        400,
        "sms_validation_failed",
        "Message is required",
        req.requestId,
      );
    }

    const parsedProvider = normalizeSmsProviderInput(provider);
    if (parsedProvider.error) {
      return sendApiError(
        res,
        400,
        "sms_validation_failed",
        parsedProvider.error,
        req.requestId,
      );
    }

    const parsedIdempotency = normalizeSmsIdempotencyInput(
      idempotency_key || idempotencyHeader,
    );
    if (parsedIdempotency.error) {
      return sendApiError(
        res,
        400,
        "sms_validation_failed",
        parsedIdempotency.error,
        req.requestId,
      );
    }

    const parsedUserChatId = normalizeSmsChatIdInput(user_chat_id);
    if (parsedUserChatId.error) {
      return sendApiError(
        res,
        400,
        "sms_validation_failed",
        parsedUserChatId.error,
        req.requestId,
      );
    }

    const parsedFrom = normalizeSmsFromInput(from);
    if (parsedFrom.error) {
      return sendApiError(
        res,
        400,
        "sms_validation_failed",
        parsedFrom.error,
        req.requestId,
      );
    }

    const maxBulkRecipients = Math.min(
      250,
      Math.max(1, Number(config.sms?.maxBulkRecipients) || 100),
    );
    if (parsedRecipients.value.length > maxBulkRecipients) {
      return sendApiError(
        res,
        400,
        "sms_validation_failed",
        `Maximum ${maxBulkRecipients} recipients per bulk send`,
        req.requestId,
      );
    }

    const maxSmsChars = Number(config.sms?.maxMessageChars) || 1600;
    if (messageText.length > maxSmsChars) {
      return sendApiError(
        res,
        400,
        "sms_validation_failed",
        `Message exceeds ${maxSmsChars} characters`,
        req.requestId,
      );
    }

    const limitResponse = await enforceOutboundRateLimits(req, res, {
      namespace: "sms_bulk",
      actorKey: getOutboundActorKey(req, parsedUserChatId.value),
      perUserLimit: Math.max(
        1,
        Math.floor((Number(config.outboundLimits?.sms?.perUser) || 15) / 3),
      ),
      globalLimit: Number(config.outboundLimits?.sms?.global) || 120,
      windowMs: Number(config.outboundLimits?.windowMs) || 60000,
    });
    if (limitResponse) {
      return;
    }

    const bulkOptions = { ...options };
    if (parsedFrom.value && !bulkOptions.from) {
      bulkOptions.from = parsedFrom.value;
    }
    if (sms_options && !bulkOptions.smsOptions) {
      bulkOptions.smsOptions = { ...sms_options };
    }
    if (parsedProvider.value) {
      bulkOptions.smsOptions = {
        ...(bulkOptions.smsOptions || {}),
        provider: bulkOptions.smsOptions?.provider || parsedProvider.value,
      };
    }
    if (parsedUserChatId.value && !bulkOptions.userChatId) {
      bulkOptions.userChatId = parsedUserChatId.value;
    }
    if (parsedIdempotency.value && !bulkOptions.idempotencyKey) {
      bulkOptions.idempotencyKey = parsedIdempotency.value;
    }
    if (!Object.prototype.hasOwnProperty.call(bulkOptions, "durable")) {
      bulkOptions.durable = true;
    }
    if (typeof bulkOptions.durable !== "boolean") {
      return sendApiError(
        res,
        400,
        "sms_validation_failed",
        "options.durable must be a boolean when provided",
        req.requestId,
      );
    }
    if (bulkOptions.durable === true && !db?.createCallJob) {
      return sendApiError(
        res,
        503,
        "sms_queue_unavailable",
        "Durable SMS queue is unavailable",
        req.requestId,
      );
    }

    console.log("sms_bulk_request", {
      request_id: req.requestId || null,
      actor: getOutboundActorKey(req, parsedUserChatId.value),
      recipients: parsedRecipients.value.length,
      provider: bulkOptions.smsOptions?.provider || getActiveSmsProvider(),
      durable: bulkOptions.durable === true,
      idempotency_key: bulkOptions.idempotencyKey ? "present" : "absent",
    });

    const result = await runWithTimeout(
      smsService.sendBulkSMS(
        parsedRecipients.value,
        messageText,
        bulkOptions,
      ),
      Number(config.outboundLimits?.handlerTimeoutMs) || 30000,
      "sms bulk handler",
      "sms_handler_timeout",
    );

    // Log bulk operation
    if (db) {
      await db.logBulkSMSOperation({
        total_recipients: result.total,
        successful: result.successful,
        failed: result.failed,
        message: messageText,
        user_chat_id: parsedUserChatId.value,
        timestamp: new Date(),
      });
    }

    res.json({
      success: true,
      ...result,
      request_id: req.requestId || null,
    });
  } catch (error) {
    const providerStatus = Number(
      error?.status || error?.statusCode || error?.response?.status,
    );
    const status =
      error.code === "idempotency_conflict"
        ? 409
        : error.code === "sms_validation_failed"
          ? 400
          : error.code === "sms_handler_timeout" ||
              error.code === "sms_provider_timeout" ||
              error.code === "sms_timeout"
            ? 504
            : providerStatus === 429
              ? 429
              : providerStatus >= 400 && providerStatus < 500
                ? 400
                : providerStatus >= 500
                  ? 502
                  : 500;
    console.error("sms_bulk_error", {
      request_id: req.requestId || null,
      actor: getOutboundActorKey(req, req.body?.user_chat_id),
      recipients: Array.isArray(req.body?.recipients)
        ? req.body.recipients.length
        : 0,
      error: redactSensitiveLogValue(error.message || "sms_bulk_failed"),
      code: error.code || null,
    });
    sendApiError(
      res,
      status,
      error.code || "sms_bulk_failed",
      error.message || "Failed to send bulk SMS",
      req.requestId,
    );
  }
});

// Schedule SMS endpoint
app.post("/api/sms/schedule", requireOutboundAuthorization, async (req, res) => {
  try {
    if (!isPlainObject(req.body)) {
      return sendApiError(
        res,
        400,
        "sms_validation_failed",
        "Request body must be a JSON object",
        req.requestId,
      );
    }
    const {
      to,
      message,
      from,
      user_chat_id,
      scheduled_time,
      options = {},
      idempotency_key,
      provider,
    } = req.body;
    const idempotencyHeader = req.headers["idempotency-key"];
    const toNumber = String(to || "").trim();
    const messageText = typeof message === "string" ? message : "";

    if (!isPlainObject(options)) {
      return sendApiError(
        res,
        400,
        "sms_validation_failed",
        "options must be an object when provided",
        req.requestId,
      );
    }

    const parsedProvider = normalizeSmsProviderInput(provider);
    if (parsedProvider.error) {
      return sendApiError(
        res,
        400,
        "sms_validation_failed",
        parsedProvider.error,
        req.requestId,
      );
    }

    const parsedIdempotency = normalizeSmsIdempotencyInput(
      idempotency_key || idempotencyHeader,
    );
    if (parsedIdempotency.error) {
      return sendApiError(
        res,
        400,
        "sms_validation_failed",
        parsedIdempotency.error,
        req.requestId,
      );
    }

    const parsedUserChatId = normalizeSmsChatIdInput(user_chat_id);
    if (parsedUserChatId.error) {
      return sendApiError(
        res,
        400,
        "sms_validation_failed",
        parsedUserChatId.error,
        req.requestId,
      );
    }

    const parsedFrom = normalizeSmsFromInput(from);
    if (parsedFrom.error) {
      return sendApiError(
        res,
        400,
        "sms_validation_failed",
        parsedFrom.error,
        req.requestId,
      );
    }

    if (!toNumber || !messageText.trim() || !scheduled_time) {
      return sendApiError(
        res,
        400,
        "sms_validation_failed",
        "Phone number, message, and scheduled_time are required",
        req.requestId,
      );
    }

    if (!toNumber.match(/^\+[1-9]\d{1,14}$/)) {
      return sendApiError(
        res,
        400,
        "sms_validation_failed",
        "Invalid phone number format. Use E.164 format (e.g., +1234567890)",
        req.requestId,
      );
    }

    const scheduledDate = new Date(scheduled_time);
    if (Number.isNaN(scheduledDate.getTime()) || scheduledDate <= new Date()) {
      return sendApiError(
        res,
        400,
        "sms_validation_failed",
        "Scheduled time must be in the future",
        req.requestId,
      );
    }

    const maxSmsChars = Number(config.sms?.maxMessageChars) || 1600;
    if (messageText.length > maxSmsChars) {
      return sendApiError(
        res,
        400,
        "sms_validation_failed",
        `Message exceeds ${maxSmsChars} characters`,
        req.requestId,
      );
    }

    const limitResponse = await enforceOutboundRateLimits(req, res, {
      namespace: "sms_schedule",
      actorKey: getOutboundActorKey(req, parsedUserChatId.value),
      perUserLimit: Number(config.outboundLimits?.sms?.perUser) || 15,
      globalLimit: Number(config.outboundLimits?.sms?.global) || 120,
      windowMs: Number(config.outboundLimits?.windowMs) || 60000,
    });
    if (limitResponse) {
      return;
    }

    const scheduleOptions = { ...options };
    if (parsedFrom.value && !scheduleOptions.from) {
      scheduleOptions.from = parsedFrom.value;
    }
    if (parsedUserChatId.value && !scheduleOptions.userChatId) {
      scheduleOptions.userChatId = parsedUserChatId.value;
    }
    if (parsedIdempotency.value && !scheduleOptions.idempotencyKey) {
      scheduleOptions.idempotencyKey = parsedIdempotency.value;
    }
    if (parsedProvider.value && !scheduleOptions.provider) {
      scheduleOptions.provider = parsedProvider.value;
    }
    if (!Object.prototype.hasOwnProperty.call(scheduleOptions, "durable")) {
      scheduleOptions.durable = true;
    }
    if (typeof scheduleOptions.durable !== "boolean") {
      return sendApiError(
        res,
        400,
        "sms_validation_failed",
        "options.durable must be a boolean when provided",
        req.requestId,
      );
    }
    if (scheduleOptions.durable === true && !db?.createCallJob) {
      return sendApiError(
        res,
        503,
        "sms_queue_unavailable",
        "Durable SMS queue is unavailable",
        req.requestId,
      );
    }

    console.log("sms_schedule_request", {
      request_id: req.requestId || null,
      actor: getOutboundActorKey(req, parsedUserChatId.value),
      to: maskPhoneForLog(toNumber),
      provider: scheduleOptions.provider || getActiveSmsProvider(),
      scheduled_at: scheduledDate.toISOString(),
      idempotency_key: scheduleOptions.idempotencyKey ? "present" : "absent",
      durable: scheduleOptions.durable === true,
    });

    const serviceOptions = { ...scheduleOptions };
    delete serviceOptions.durable;

    const result = await runWithTimeout(
      smsService.scheduleSMS(
        toNumber,
        messageText,
        scheduledDate.toISOString(),
        serviceOptions,
      ),
      Number(config.outboundLimits?.handlerTimeoutMs) || 30000,
      "sms schedule handler",
      "sms_handler_timeout",
    );

    res.json({
      success: true,
      ...result,
      request_id: req.requestId || null,
    });
  } catch (error) {
    const providerStatus = Number(
      error?.status || error?.statusCode || error?.response?.status,
    );
    const status =
      error.code === "idempotency_conflict"
        ? 409
        : error.code === "sms_validation_failed"
          ? 400
          : error.code === "sms_handler_timeout" ||
              error.code === "sms_provider_timeout" ||
              error.code === "sms_timeout"
            ? 504
            : providerStatus === 429
              ? 429
              : providerStatus >= 400 && providerStatus < 500
                ? 400
                : providerStatus >= 500
                  ? 502
                  : 500;
    console.error("sms_schedule_error", {
      request_id: req.requestId || null,
      actor: getOutboundActorKey(req, req.body?.user_chat_id),
      to: maskPhoneForLog(req.body?.to || ""),
      error: redactSensitiveLogValue(error.message || "sms_schedule_failed"),
      code: error.code || null,
    });
    sendApiError(
      res,
      status,
      error.code || "sms_schedule_failed",
      error.message || "Failed to schedule SMS",
      req.requestId,
    );
  }
});

const SMS_BUILTIN_SCRIPTS = Object.freeze({
  welcome:
    "Welcome to our service! We're excited to have you aboard. Reply HELP for assistance or STOP to unsubscribe.",
  appointment_reminder:
    "Reminder: You have an appointment on {date} at {time}. Reply CONFIRM to confirm or RESCHEDULE to change.",
  verification:
    "Your verification code is: {code}. This code will expire in 10 minutes. Do not share this code with anyone.",
  order_update:
    "Order #{order_id} update: {status}. Track your order at {tracking_url}",
  payment_reminder:
    "Payment reminder: Your payment of {amount} is due on {due_date}. Pay now: {payment_url}",
  promotional:
    "Special offer just for you! {offer_text} Use code {promo_code}. Valid until {expiry_date}. Reply STOP to opt out.",
  customer_service:
    "Thanks for contacting us! We've received your message and will respond within 24 hours. For urgent matters, call {phone}.",
  survey:
    "How was your experience with us? Rate us 1-5 stars by replying with a number. Your feedback helps us improve!",
});

function escapeSmsScriptToken(value = "") {
  return String(value).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function normalizeSmsScriptName(value = "") {
  const name = String(value || "")
    .trim()
    .toLowerCase();
  if (!name) return null;
  if (!/^[a-z0-9_-]{1,64}$/.test(name)) return null;
  return name;
}

function parseSmsScriptBoolean(value, fallback = false) {
  if (value === undefined || value === null || value === "") return fallback;
  const normalized = String(value).trim().toLowerCase();
  return normalized === "1" || normalized === "true" || normalized === "yes";
}

function parseSmsScriptVariables(value) {
  if (value === undefined || value === null || value === "") {
    return { value: {} };
  }
  if (isPlainObject(value)) {
    return { value };
  }
  try {
    const parsed = JSON.parse(String(value));
    if (!isPlainObject(parsed)) {
      return { error: "variables must be a JSON object" };
    }
    return { value: parsed };
  } catch (_) {
    return { error: "variables must be valid JSON" };
  }
}

function applySmsScriptVariables(content, variables = {}) {
  let rendered = String(content || "");
  if (!isPlainObject(variables)) return rendered;
  for (const [key, value] of Object.entries(variables)) {
    if (!key) continue;
    const token = escapeSmsScriptToken(key);
    rendered = rendered.replace(new RegExp(`{${token}}`, "g"), String(value ?? ""));
  }
  return rendered;
}

function getBuiltinSmsScriptNames() {
  return Object.keys(SMS_BUILTIN_SCRIPTS);
}

function getBuiltinSmsScriptByName(name) {
  const normalized = normalizeSmsScriptName(name);
  if (!normalized) return null;
  const content = SMS_BUILTIN_SCRIPTS[normalized];
  if (!content) return null;
  return {
    name: normalized,
    description: null,
    content,
    metadata: {},
    is_builtin: true,
    created_by: null,
    updated_by: null,
    created_at: null,
    updated_at: null,
  };
}

async function suggestSmsScriptName(baseName) {
  const normalizedBase = normalizeSmsScriptName(baseName) || "sms_script";
  const existing = await db.listSmsScripts().catch(() => []);
  const existingNames = new Set(
    (existing || []).map((row) => String(row?.name || "").toLowerCase()),
  );
  for (const builtinName of getBuiltinSmsScriptNames()) {
    existingNames.add(String(builtinName).toLowerCase());
  }
  if (!existingNames.has(normalizedBase)) {
    return normalizedBase;
  }
  for (let index = 2; index < 1000; index += 1) {
    const candidate = `${normalizedBase}_${index}`;
    if (!existingNames.has(candidate)) {
      return candidate;
    }
  }
  return `${normalizedBase}_${Date.now().toString().slice(-4)}`;
}

// SMS script management endpoints
app.get("/api/sms/scripts", requireAdminToken, async (req, res) => {
  try {
    const scriptName = req.query?.script_name;
    const parsedVariables = parseSmsScriptVariables(req.query?.variables);
    if (parsedVariables.error) {
      return res.status(400).json({
        success: false,
        error: parsedVariables.error,
      });
    }
    const variables = parsedVariables.value;

    if (scriptName) {
      const normalizedName = normalizeSmsScriptName(scriptName);
      if (!normalizedName) {
        return res.status(400).json({
          success: false,
          error:
            "script_name must use lowercase letters, numbers, underscores, or dashes",
        });
      }
      const customScript = await db.getSmsScript(normalizedName);
      const builtinScript = getBuiltinSmsScriptByName(normalizedName);
      const sourceScript = customScript || builtinScript;
      if (!sourceScript) {
        return res.status(404).json({
          success: false,
          error: `Script '${normalizedName}' not found`,
        });
      }
      const rendered = applySmsScriptVariables(sourceScript.content, variables);
      return res.json({
        success: true,
        script_name: normalizedName,
        script: rendered,
        original_script: sourceScript.content,
        variables,
      });
    }

    const includeBuiltins = parseSmsScriptBoolean(req.query?.include_builtins, true);
    const detailed = parseSmsScriptBoolean(req.query?.detailed, false);
    const customScripts = (await db.listSmsScripts()).map((script) => ({
      ...script,
      content: detailed ? script.content : "",
      is_builtin: false,
    }));
    const builtinScripts = includeBuiltins
      ? getBuiltinSmsScriptNames().map((name) => ({
          ...getBuiltinSmsScriptByName(name),
          content: detailed ? SMS_BUILTIN_SCRIPTS[name] : "",
        }))
      : [];

    return res.json({
      success: true,
      scripts: customScripts,
      builtin: builtinScripts,
      available_scripts: includeBuiltins ? getBuiltinSmsScriptNames() : [],
      script_count: customScripts.length + builtinScripts.length,
    });
  } catch (error) {
    console.error("sms_scripts_list_error", {
      request_id: req.requestId || null,
      error: redactSensitiveLogValue(error.message || "sms_scripts_list_failed"),
    });
    return res.status(500).json({
      success: false,
      error: "Failed to load SMS scripts",
    });
  }
});

app.get("/api/sms/scripts/:scriptName", requireAdminToken, async (req, res) => {
  try {
    const normalizedName = normalizeSmsScriptName(req.params.scriptName);
    if (!normalizedName) {
      return res.status(400).json({
        success: false,
        error: "Invalid script name",
      });
    }
    const detailed = parseSmsScriptBoolean(req.query?.detailed, true);
    const customScript = await db.getSmsScript(normalizedName);
    const builtinScript = getBuiltinSmsScriptByName(normalizedName);
    const script = customScript || builtinScript;
    if (!script) {
      return res.status(404).json({
        success: false,
        error: `Script '${normalizedName}' not found`,
      });
    }
    const payload = {
      ...script,
      content: detailed ? script.content : "",
    };
    return res.json({
      success: true,
      script: payload,
      script_name: payload.name,
      original_script: script.content,
    });
  } catch (error) {
    console.error("sms_script_get_error", {
      request_id: req.requestId || null,
      script_name: req.params?.scriptName || null,
      error: redactSensitiveLogValue(error.message || "sms_script_get_failed"),
    });
    return res.status(500).json({
      success: false,
      error: "Failed to load SMS script",
    });
  }
});

app.post("/api/sms/scripts", requireAdminToken, async (req, res) => {
  try {
    const body = isPlainObject(req.body) ? req.body : {};
    const normalizedName = normalizeSmsScriptName(body.name);
    if (!normalizedName) {
      return res.status(400).json({
        success: false,
        error:
          "name is required and must use lowercase letters, numbers, underscores, or dashes",
      });
    }
    const content = String(body.content || "").trim();
    if (!content) {
      return res.status(400).json({
        success: false,
        error: "content is required",
      });
    }
    if (getBuiltinSmsScriptByName(normalizedName)) {
      return res.status(409).json({
        success: false,
        error: `Script name '${normalizedName}' is reserved for a built-in script`,
        code: "SCRIPT_NAME_DUPLICATE",
        suggested_name: await suggestSmsScriptName(`${normalizedName}_custom`),
      });
    }
    const existing = await db.getSmsScript(normalizedName);
    if (existing) {
      return res.status(409).json({
        success: false,
        error: `Script '${normalizedName}' already exists`,
        code: "SCRIPT_NAME_DUPLICATE",
        suggested_name: await suggestSmsScriptName(`${normalizedName}_copy`),
      });
    }

    const metadata = body.metadata;
    if (metadata !== undefined && metadata !== null && !isPlainObject(metadata)) {
      return res.status(400).json({
        success: false,
        error: "metadata must be an object when provided",
      });
    }
    await db.createSmsScript({
      name: normalizedName,
      description: body.description || null,
      content,
      metadata: metadata === undefined ? null : metadata,
      created_by: body.created_by || req.headers?.["x-admin-user"] || null,
      updated_by: body.updated_by || req.headers?.["x-admin-user"] || null,
    });
    const script = await db.getSmsScript(normalizedName);
    return res.status(201).json({
      success: true,
      script: {
        ...script,
        is_builtin: false,
      },
    });
  } catch (error) {
    const message = String(error?.message || "");
    if (
      message.includes("UNIQUE constraint failed") ||
      message.includes("SQLITE_CONSTRAINT")
    ) {
      return res.status(409).json({
        success: false,
        error: "Script name already exists",
        code: "SCRIPT_NAME_DUPLICATE",
        suggested_name: await suggestSmsScriptName(
          `${normalizeSmsScriptName(req.body?.name) || "sms_script"}_copy`,
        ),
      });
    }
    console.error("sms_script_create_error", {
      request_id: req.requestId || null,
      error: redactSensitiveLogValue(error.message || "sms_script_create_failed"),
    });
    return res.status(500).json({
      success: false,
      error: "Failed to create SMS script",
    });
  }
});

app.put("/api/sms/scripts/:scriptName", requireAdminToken, async (req, res) => {
  try {
    const normalizedName = normalizeSmsScriptName(req.params.scriptName);
    if (!normalizedName) {
      return res.status(400).json({
        success: false,
        error: "Invalid script name",
      });
    }
    const existing = await db.getSmsScript(normalizedName);
    if (!existing) {
      if (getBuiltinSmsScriptByName(normalizedName)) {
        return res.status(400).json({
          success: false,
          error: "Built-in scripts are read-only and cannot be edited",
        });
      }
      return res.status(404).json({
        success: false,
        error: `Script '${normalizedName}' not found`,
      });
    }

    const body = isPlainObject(req.body) ? req.body : {};
    const updates = {};
    if (body.description !== undefined) {
      updates.description = body.description;
    }
    if (body.content !== undefined) {
      const content = String(body.content || "").trim();
      if (!content) {
        return res.status(400).json({
          success: false,
          error: "content cannot be empty",
        });
      }
      updates.content = content;
    }
    if (body.metadata !== undefined) {
      if (body.metadata !== null && !isPlainObject(body.metadata)) {
        return res.status(400).json({
          success: false,
          error: "metadata must be an object when provided",
        });
      }
      updates.metadata = body.metadata;
    }
    updates.updated_by = body.updated_by || req.headers?.["x-admin-user"] || null;

    await db.updateSmsScript(normalizedName, updates);
    const script = await db.getSmsScript(normalizedName);
    return res.json({
      success: true,
      script: {
        ...script,
        is_builtin: false,
      },
    });
  } catch (error) {
    console.error("sms_script_update_error", {
      request_id: req.requestId || null,
      script_name: req.params?.scriptName || null,
      error: redactSensitiveLogValue(error.message || "sms_script_update_failed"),
    });
    return res.status(500).json({
      success: false,
      error: "Failed to update SMS script",
    });
  }
});

app.delete("/api/sms/scripts/:scriptName", requireAdminToken, async (req, res) => {
  try {
    const normalizedName = normalizeSmsScriptName(req.params.scriptName);
    if (!normalizedName) {
      return res.status(400).json({
        success: false,
        error: "Invalid script name",
      });
    }
    if (getBuiltinSmsScriptByName(normalizedName)) {
      return res.status(400).json({
        success: false,
        error: "Built-in scripts cannot be deleted",
      });
    }
    const changes = await db.deleteSmsScript(normalizedName);
    if (!changes) {
      return res.status(404).json({
        success: false,
        error: `Script '${normalizedName}' not found`,
      });
    }
    return res.json({ success: true });
  } catch (error) {
    console.error("sms_script_delete_error", {
      request_id: req.requestId || null,
      script_name: req.params?.scriptName || null,
      error: redactSensitiveLogValue(error.message || "sms_script_delete_failed"),
    });
    return res.status(500).json({
      success: false,
      error: "Failed to delete SMS script",
    });
  }
});

app.post(
  "/api/sms/scripts/:scriptName/preview",
  requireAdminToken,
  async (req, res) => {
    try {
      const normalizedName = normalizeSmsScriptName(req.params.scriptName);
      if (!normalizedName) {
        return res.status(400).json({
          success: false,
          error: "Invalid script name",
        });
      }
      const body = isPlainObject(req.body) ? req.body : {};
      const to = String(body.to || "").trim();
      if (!to.match(/^\+[1-9]\d{1,14}$/)) {
        return res.status(400).json({
          success: false,
          error: "to must be a valid E.164 phone number",
        });
      }

      const parsedVariables = parseSmsScriptVariables(body.variables);
      if (parsedVariables.error) {
        return res.status(400).json({
          success: false,
          error: parsedVariables.error,
        });
      }
      const variables = parsedVariables.value;
      const customScript = await db.getSmsScript(normalizedName);
      const builtinScript = getBuiltinSmsScriptByName(normalizedName);
      const script = customScript || builtinScript;
      if (!script) {
        return res.status(404).json({
          success: false,
          error: `Script '${normalizedName}' not found`,
        });
      }
      const content = applySmsScriptVariables(script.content, variables);
      if (!content.trim()) {
        return res.status(400).json({
          success: false,
          error: "Resolved preview message is empty",
        });
      }

      const options = isPlainObject(body.options) ? { ...body.options } : {};
      const parsedIdempotency = normalizeSmsIdempotencyInput(
        body.idempotency_key || req.headers["idempotency-key"],
      );
      if (parsedIdempotency.error) {
        return res.status(400).json({
          success: false,
          error: parsedIdempotency.error,
        });
      }
      if (parsedIdempotency.value && !options.idempotencyKey) {
        options.idempotencyKey = parsedIdempotency.value;
      }
      if (!Object.prototype.hasOwnProperty.call(options, "durable")) {
        options.durable = false;
      }

      const previewResult = await runWithTimeout(
        smsService.sendSMS(to, content, null, options),
        Number(config.outboundLimits?.handlerTimeoutMs) || 30000,
        "sms script preview handler",
        "sms_handler_timeout",
      );
      if (db && previewResult.message_sid && previewResult.idempotent !== true) {
        try {
          await db.saveSMSMessage({
            message_sid: previewResult.message_sid,
            to_number: to,
            from_number: previewResult.from || null,
            body: content,
            status: previewResult.status || "queued",
            direction: "outbound",
            provider: previewResult.provider || getActiveSmsProvider(),
            user_chat_id: body.user_chat_id || null,
          });
        } catch (saveError) {
          const saveMsg = String(saveError?.message || "");
          if (
            !saveMsg.includes("UNIQUE constraint failed") &&
            !saveMsg.includes("SQLITE_CONSTRAINT")
          ) {
            throw saveError;
          }
        }
      }
      return res.json({
        success: true,
        preview: {
          to,
          message_sid: previewResult.message_sid || null,
          status: previewResult.status || null,
          provider: previewResult.provider || null,
          content,
          script_name: normalizedName,
        },
      });
    } catch (error) {
      const providerStatus = Number(
        error?.status || error?.statusCode || error?.response?.status,
      );
      const status =
        error.code === "sms_validation_failed"
          ? 400
          : error.code === "sms_handler_timeout" ||
              error.code === "sms_provider_timeout" ||
              error.code === "sms_timeout"
            ? 504
            : providerStatus === 429
              ? 429
              : providerStatus >= 400 && providerStatus < 500
                ? 400
                : providerStatus >= 500
                  ? 502
                  : 500;
      console.error("sms_script_preview_error", {
        request_id: req.requestId || null,
        script_name: req.params?.scriptName || null,
        to: maskPhoneForLog(req.body?.to || ""),
        error: redactSensitiveLogValue(
          error.message || "sms_script_preview_failed",
        ),
        code: error.code || null,
      });
      return res.status(status).json({
        success: false,
        error: error.message || "Failed to send SMS script preview",
        code: error.code || "sms_script_preview_failed",
      });
    }
  },
);

// Get SMS messages from database for conversation view
app.get(
  "/api/sms/messages/conversation/:phone",
  requireOutboundAuthorization,
  async (req, res) => {
  try {
    const { phone } = req.params;
    const limit = Math.min(Math.max(parseInt(req.query.limit, 10) || 50, 1), 100);

    if (!phone || !/^\+?[1-9]\d{5,19}$/.test(String(phone).trim())) {
      return res.status(400).json({
        success: false,
        error: "Valid phone number is required",
      });
    }

    const messages = await db.getSMSConversation(phone, limit);

    res.json({
      success: true,
      phone: phone,
      messages: messages,
      message_count: messages.length,
    });
  } catch (error) {
    console.error("❌ Error fetching SMS conversation from database:", error);
    res.status(500).json({
      success: false,
      error: "Failed to fetch conversation",
      details: error.message,
    });
  }
  },
);

// Get recent SMS messages from database
app.get("/api/sms/messages/recent", requireOutboundAuthorization, async (req, res) => {
  try {
    const limit = Math.min(Math.max(parseInt(req.query.limit, 10) || 10, 1), 50);
    const offset = Math.max(parseInt(req.query.offset, 10) || 0, 0);

    const messages = await db.getSMSMessages(limit, offset);

    res.json({
      success: true,
      messages: messages,
      count: messages.length,
      limit: limit,
      offset: offset,
    });
  } catch (error) {
    console.error("❌ Error fetching recent SMS messages:", error);
    res.status(500).json({
      success: false,
      error: "Failed to fetch recent messages",
      details: error.message,
    });
  }
});

// Get SMS database statistics
app.get("/api/sms/database-stats", requireOutboundAuthorization, async (req, res) => {
  try {
    const hours = parseInt(req.query.hours) || 24;
    const dateFrom = new Date(
      Date.now() - hours * 60 * 60 * 1000,
    ).toISOString();

    // Get comprehensive SMS statistics from database
    const stats = await new Promise((resolve, reject) => {
      const queries = {
        // Total messages
        totalMessages: `SELECT COUNT(*) as count FROM sms_messages`,

        // Messages by direction
        messagesByDirection: `
                    SELECT direction, COUNT(*) as count 
                    FROM sms_messages 
                    GROUP BY direction
                `,

        // Messages by status
        messagesByStatus: `
                    SELECT status, COUNT(*) as count 
                    FROM sms_messages 
                    GROUP BY status
                    ORDER BY count DESC
                `,

        // Recent messages
        recentMessages: `
                    SELECT * FROM sms_messages 
                    WHERE created_at >= ?
                    ORDER BY created_at DESC 
                    LIMIT 5
                `,

        // Bulk operations
        bulkOperations: `SELECT COUNT(*) as count FROM bulk_sms_operations`,

        // Recent bulk operations
        recentBulkOps: `
                    SELECT * FROM bulk_sms_operations 
                    WHERE created_at >= ?
                    ORDER BY created_at DESC 
                    LIMIT 3
                `,
      };

      const results = {};
      let completed = 0;
      const total = Object.keys(queries).length;

      for (const [key, query] of Object.entries(queries)) {
        const params = ["recentMessages", "recentBulkOps"].includes(key)
          ? [dateFrom]
          : [];

        db.db.all(query, params, (err, rows) => {
          if (err) {
            console.error(`SMS stats query error for ${key}:`, err);
            results[key] = key.includes("recent") ? [] : [{ count: 0 }];
          } else {
            results[key] = rows || [];
          }

          completed++;
          if (completed === total) {
            resolve(results);
          }
        });
      }
    });

    // Process the statistics
    const processedStats = {
      total_messages: stats.totalMessages[0]?.count || 0,
      sent_messages:
        stats.messagesByDirection.find((d) => d.direction === "outbound")
          ?.count || 0,
      received_messages:
        stats.messagesByDirection.find((d) => d.direction === "inbound")
          ?.count || 0,
      delivered_count:
        stats.messagesByStatus.find((s) => s.status === "delivered")?.count ||
        0,
      failed_count:
        stats.messagesByStatus.find((s) => s.status === "failed")?.count || 0,
      pending_count:
        stats.messagesByStatus.find((s) => s.status === "pending")?.count || 0,
      bulk_operations: stats.bulkOperations[0]?.count || 0,
      recent_messages: stats.recentMessages || [],
      recent_bulk_operations: stats.recentBulkOps || [],
      status_breakdown: stats.messagesByStatus || [],
      direction_breakdown: stats.messagesByDirection || [],
      time_period_hours: hours,
    };

    // Calculate success rate
    const totalSent = processedStats.sent_messages;
    const delivered = processedStats.delivered_count;
    processedStats.success_rate =
      totalSent > 0 ? Math.round((delivered / totalSent) * 100) : 0;

    res.json({
      success: true,
      ...processedStats,
    });
  } catch (error) {
    console.error("❌ Error fetching SMS database statistics:", error);
    res.status(500).json({
      success: false,
      error: "Failed to fetch database statistics",
      details: error.message,
    });
  }
});

// Get SMS status by message SID
app.get("/api/sms/status/:messageSid", requireOutboundAuthorization, async (req, res) => {
  try {
    const { messageSid } = req.params;
    if (!isSafeId(messageSid, { max: 128 })) {
      return res.status(400).json({
        success: false,
        error: "Invalid message identifier",
      });
    }

    const message = await new Promise((resolve, reject) => {
      db.db.get(
        `SELECT * FROM sms_messages WHERE message_sid = ?`,
        [messageSid],
        (err, row) => {
          if (err) {
            reject(err);
          } else {
            resolve(row);
          }
        },
      );
    });

    if (!message) {
      return res.status(404).json({
        success: false,
        error: "Message not found",
      });
    }

    res.json({
      success: true,
      message: message,
    });
  } catch (error) {
    console.error("❌ Error fetching SMS status:", error);
    res.status(500).json({
      success: false,
      error: "Failed to fetch message status",
      details: error.message,
    });
  }
});

registerCallRoutes(app, {
  requireOutboundAuthorization,
  sendApiError,
  resolveHost,
  config,
  placeOutboundCall,
  buildErrorDetails,
  getCurrentProvider: () => currentProvider,
  getDb: () => db,
  isSafeId,
  normalizeCallRecordForApi,
  buildDigitSummary,
  parsePagination,
  normalizeCallStatus,
  normalizeDateFilter,
  parseBoundedInteger,
  getDigitService: () => digitService,
  getTranscriptAudioUrl: (text, callConfig, options = {}) => {
    const timeoutMs = Number(options?.timeoutMs);
    const effectiveTimeoutMs =
      Number.isFinite(timeoutMs) && timeoutMs > 0 ? timeoutMs : 12000;
    return getTwilioTtsAudioUrlSafe(
      text,
      callConfig,
      effectiveTimeoutMs,
      { forceGenerate: true },
    );
  },
  transcriptAudioTimeoutMs: Number(config.api?.transcriptAudioTimeoutMs) || 12000,
  transcriptAudioMaxChars: Number(config.api?.transcriptAudioMaxChars) || 2600,
});

registerStatusRoutes(app, {
  getDb: () => db,
  isSafeId,
  normalizeCallRecordForApi,
  buildDigitSummary,
  webhookService,
  getProviderReadiness,
  appVersion,
  getCurrentProvider: () => currentProvider,
  getCurrentSmsProvider: () => currentSmsProvider,
  getCurrentEmailProvider: () => currentEmailProvider,
  callConfigurations,
  config,
  verifyHmacSignature,
  hasAdminToken,
  requireOutboundAuthorization,
  refreshInboundDefaultScript,
  getInboundHealthContext,
  supportedProviders: SUPPORTED_PROVIDERS,
  providerHealth,
  isProviderDegraded,
  pruneExpiredKeypadProviderOverrides,
  keypadProviderOverrides,
  functionEngine,
  callFunctionSystems,
});

registerWebhookRoutes(app, {
  config,
  handleTwilioGatherWebhook,
  requireValidTwilioSignature,
  requireValidAwsWebhook,
  requireValidEmailWebhook,
  requireValidVonageWebhook,
  requireValidTelegramWebhook,
  processCallStatusWebhookPayload,
  getDb: () => db,
  streamStatusDedupe,
  activeStreamConnections,
  shouldProcessProviderEvent,
  shouldProcessProviderEventAsync,
  recordCallStatus,
  getVonageCallPayload,
  getVonageDtmfDigits,
  resolveVonageCallSid,
  isOutboundVonageDirection,
  buildVonageInboundCallSid,
  refreshInboundDefaultScript,
  hydrateCallConfigFromDb,
  ensureCallSetup,
  ensureCallRecord,
  normalizePhoneForFlag,
  shouldRateLimitInbound,
  rememberVonageCallMapping,
  handleExternalDtmfInput,
  clearVonageCallMappings,
  buildVonageWebsocketUrl,
  getVonageWebsocketContentType,
  buildVonageEventWebhookUrl,
  resolveHost,
  buildRetrySmsBody,
  buildRetryPayload,
  scheduleCallJob,
  formatContactLabel,
  placeOutboundCall,
  buildRecapSmsBody,
  logConsoleAction,
  buildInboundSmsBody,
  buildCallbackPayload,
  endCallForProvider,
  webhookService,
  buildVonageTalkHangupNcco,
  buildVonageUnavailableNcco,
  buildTwilioStreamTwiml,
  getCallConfigurations: () => callConfigurations,
  getCallFunctionSystems: () => callFunctionSystems,
  getCallDirections: () => callDirections,
  getAwsContactMap: () => awsContactMap,
  getActiveCalls: () => activeCalls,
  handleCallEnd,
  maskPhoneForLog,
  maskSmsBodyForLog,
  smsService,
  smsWebhookDedupeTtlMs: Number(config.sms?.webhookDedupeTtlMs) || null,
  getDigitService: () => digitService,
  getEmailService: () => emailService,
});

// Get SMS statistics
app.get("/api/sms/stats", requireOutboundAuthorization, async (req, res) => {
  try {
    const stats = smsService.getStatistics();
    const activeConversations = smsService.getActiveConversations();
    const providerCircuits = smsService.getProviderCircuitHealth
      ? smsService.getProviderCircuitHealth()
      : {};
    const reconcileConfig = smsService.getReconcileConfig
      ? smsService.getReconcileConfig()
      : {};

    res.json({
      success: true,
      statistics: stats,
      active_conversations: activeConversations.slice(0, 20), // Last 20 conversations
      provider_circuit_health: providerCircuits,
      reconcile: reconcileConfig,
      sms_service_enabled: true,
    });
  } catch (error) {
    console.error("❌ SMS stats error:", error);
    res.status(500).json({
      success: false,
      error: "Failed to get SMS statistics",
    });
  }
});

app.post("/api/sms/reconcile", requireAdminToken, async (req, res) => {
  try {
    const result = await runWithTimeout(
      smsService.reconcileStaleOutboundStatuses({
        staleMinutes: req.body?.stale_minutes,
        limit: req.body?.limit,
      }),
      Number(config.outboundLimits?.handlerTimeoutMs) || 30000,
      "sms reconcile handler",
      "sms_handler_timeout",
    );
    return res.json({
      success: true,
      ...result,
      request_id: req.requestId || null,
    });
  } catch (error) {
    const status = error.code === "sms_handler_timeout" ? 504 : 500;
    console.error("sms_reconcile_error", {
      request_id: req.requestId || null,
      error: redactSensitiveLogValue(error.message || "sms_reconcile_failed"),
      code: error.code || null,
    });
    return sendApiError(
      res,
      status,
      error.code || "sms_reconcile_failed",
      error.message || "Failed to reconcile stale SMS statuses",
      req.requestId,
    );
  }
});

app.post("/api/payment/reconcile", requireAdminToken, async (req, res) => {
  try {
    const result = await runWithTimeout(
      runPaymentReconciliation({
        force: true,
        source: "payment_reconcile_api",
        staleSeconds: req.body?.stale_seconds,
        limit: req.body?.limit,
      }),
      Number(config.outboundLimits?.handlerTimeoutMs) || 30000,
      "payment reconcile handler",
      "payment_handler_timeout",
    );
    return res.json({
      success: true,
      ...result,
      request_id: req.requestId || null,
    });
  } catch (error) {
    const status = error.code === "payment_handler_timeout" ? 504 : 500;
    console.error("payment_reconcile_error", {
      request_id: req.requestId || null,
      error: redactSensitiveLogValue(error.message || "payment_reconcile_failed"),
      code: error.code || null,
    });
    return sendApiError(
      res,
      status,
      error.code || "payment_reconcile_failed",
      error.message || "Failed to reconcile stale payment sessions",
      req.requestId,
    );
  }
});

app.get("/api/payment/analytics", requireAdminToken, async (req, res) => {
  try {
    if (!db?.getPaymentFunnelAnalytics) {
      return res.status(503).json({
        success: false,
        error: "Payment analytics is not available",
      });
    }
    const hours = Number(req.query?.hours);
    const limit = Number(req.query?.limit);
    const rows = await db.getPaymentFunnelAnalytics({ hours, limit });
    const toNumber = (value) => {
      const parsed = Number(value);
      return Number.isFinite(parsed) ? parsed : 0;
    };
    const toRate = (num, den) => {
      if (!den) return 0;
      return Number(((num / den) * 100).toFixed(2));
    };
    const items = (rows || []).map((row) => {
      const callsTotal = toNumber(row.calls_total);
      const offered = toNumber(row.offered);
      const requested = toNumber(row.requested);
      const captureStarted = toNumber(row.capture_started);
      const completed = toNumber(row.completed);
      const failed = toNumber(row.failed);
      return {
        script_id: row.script_id || null,
        script_version: Number.isFinite(Number(row.script_version))
          ? Math.max(1, Math.floor(Number(row.script_version)))
          : null,
        provider: row.provider || null,
        calls_total: callsTotal,
        offered,
        requested,
        capture_started: captureStarted,
        completed,
        failed,
        conversion_offer_to_request_pct: toRate(requested, offered),
        conversion_request_to_complete_pct: toRate(completed, requested),
        conversion_offer_to_complete_pct: toRate(completed, offered),
        failure_rate_pct: toRate(failed, requested || offered),
      };
    });
    const summary = items.reduce(
      (acc, item) => {
        acc.calls_total += item.calls_total;
        acc.offered += item.offered;
        acc.requested += item.requested;
        acc.capture_started += item.capture_started;
        acc.completed += item.completed;
        acc.failed += item.failed;
        return acc;
      },
      {
        calls_total: 0,
        offered: 0,
        requested: 0,
        capture_started: 0,
        completed: 0,
        failed: 0,
      },
    );
    summary.conversion_offer_to_request_pct = toRate(
      summary.requested,
      summary.offered,
    );
    summary.conversion_request_to_complete_pct = toRate(
      summary.completed,
      summary.requested,
    );
    summary.conversion_offer_to_complete_pct = toRate(
      summary.completed,
      summary.offered,
    );
    summary.failure_rate_pct = toRate(
      summary.failed,
      summary.requested || summary.offered,
    );

    return res.json({
      success: true,
      window_hours: Number.isFinite(hours) && hours > 0 ? hours : 24 * 7,
      items,
      summary,
      request_id: req.requestId || null,
    });
  } catch (error) {
    console.error("payment_analytics_error", {
      request_id: req.requestId || null,
      error: redactSensitiveLogValue(error.message || "payment_analytics_failed"),
      code: error.code || null,
    });
    return sendApiError(
      res,
      500,
      error.code || "payment_analytics_failed",
      error.message || "Failed to fetch payment analytics",
      req.requestId,
    );
  }
});

// Bulk SMS status endpoint
app.get("/api/sms/bulk/status", requireOutboundAuthorization, async (req, res) => {
  try {
    const limit = parseBoundedInteger(req.query.limit, {
      defaultValue: 10,
      min: 1,
      max: 50,
    });
    const hours = parseBoundedInteger(req.query.hours, {
      defaultValue: 24,
      min: 1,
      max: 24 * 30,
    });
    const dateFrom = new Date(
      Date.now() - hours * 60 * 60 * 1000,
    ).toISOString();

    const bulkOperations = await new Promise((resolve, reject) => {
      db.db.all(
        `
                SELECT * FROM bulk_sms_operations 
                WHERE created_at >= ?
                ORDER BY created_at DESC 
                LIMIT ?
            `,
        [dateFrom, limit],
        (err, rows) => {
          if (err) {
            reject(err);
          } else {
            resolve(rows || []);
          }
        },
      );
    });

    // Get summary statistics
    const summary = bulkOperations.reduce(
      (acc, op) => {
        acc.totalOperations += 1;
        acc.totalRecipients += op.total_recipients;
        acc.totalSuccessful += op.successful;
        acc.totalFailed += op.failed;
        return acc;
      },
      {
        totalOperations: 0,
        totalRecipients: 0,
        totalSuccessful: 0,
        totalFailed: 0,
      },
    );

    summary.successRate =
      summary.totalRecipients > 0
        ? Math.round((summary.totalSuccessful / summary.totalRecipients) * 100)
        : 0;

    res.json({
      success: true,
      summary: summary,
      operations: bulkOperations,
      time_period_hours: hours,
    });
  } catch (error) {
    console.error("❌ Error fetching bulk SMS status:", error);
    res.status(500).json({
      success: false,
      error: "Failed to fetch bulk SMS status",
      details: error.message,
    });
  }
});

if (require.main === module) {
  startServer();
}

module.exports = {
  app,
  startServer,
  __testables: {
    setDbForTests,
    verifyTelegramWebhookAuth,
    verifyAwsWebhookAuth,
    verifyAwsStreamAuth,
    buildStreamAuthToken,
    buildCallScriptMutationFingerprint,
    beginCallScriptMutationIdempotency,
    completeCallScriptMutationIdempotency,
    failCallScriptMutationIdempotency,
    applyIdempotencyResponse,
    resetCallScriptMutationIdempotencyForTests,
  },
};

process.on("unhandledRejection", (reason) => {
  const details = buildErrorDetails(reason);
  console.error("Unhandled promise rejection:", details);
});

process.on("uncaughtException", (error) => {
  const details = buildErrorDetails(error);
  console.error("Uncaught exception:", details);
});

// Enhanced graceful shutdown with comprehensive cleanup
process.on("SIGINT", async () => {
  console.log("\n🛑 Shutting down enhanced adaptive system gracefully...");

  try {
    // Log shutdown start
    await db.logServiceHealth("system", "shutdown_initiated", {
      active_calls: callConfigurations.size,
      tracked_calls: callFunctionSystems.size,
    });

    // Stop services
    webhookService.stop();
    callConfigurations.clear();
    callFunctionSystems.clear();
    callDirections.clear();
    for (const timer of keypadDtmfWatchdogs.values()) {
      clearTimeout(timer);
    }
    keypadDtmfWatchdogs.clear();
    keypadDtmfSeen.clear();
    keypadProviderOverrides.clear();
    keypadProviderGuardWarnings.clear();
    for (const timer of callRuntimePersistTimers.values()) {
      clearTimeout(timer);
    }
    callRuntimePersistTimers.clear();
    callRuntimePendingWrites.clear();
    callToolInFlight.clear();
    providerEventDedupe.clear();
    callStatusDedupe.clear();

    // Log successful shutdown
    await db.logServiceHealth("system", "shutdown_completed", {
      timestamp: new Date().toISOString(),
    });

    await db.close();
    console.log("✅ Enhanced adaptive system shutdown complete");
  } catch (shutdownError) {
    console.error("❌ Error during shutdown:", shutdownError);
  }

  process.exit(0);
});

process.on("SIGTERM", async () => {
  console.log("\nShutting down enhanced adaptive system gracefully...");

  try {
    // Log shutdown start
    await db.logServiceHealth("system", "shutdown_initiated", {
      active_calls: callConfigurations.size,
      tracked_calls: callFunctionSystems.size,
      reason: "SIGTERM",
    });

    // Stop services
    webhookService.stop();
    callConfigurations.clear();
    callFunctionSystems.clear();
    callDirections.clear();
    for (const timer of keypadDtmfWatchdogs.values()) {
      clearTimeout(timer);
    }
    keypadDtmfWatchdogs.clear();
    keypadDtmfSeen.clear();
    keypadProviderOverrides.clear();
    keypadProviderGuardWarnings.clear();
    for (const timer of callRuntimePersistTimers.values()) {
      clearTimeout(timer);
    }
    callRuntimePersistTimers.clear();
    callRuntimePendingWrites.clear();
    callToolInFlight.clear();
    providerEventDedupe.clear();
    callStatusDedupe.clear();

    // Log successful shutdown
    await db.logServiceHealth("system", "shutdown_completed", {
      timestamp: new Date().toISOString(),
    });

    await db.close();
    console.log("Enhanced adaptive system shutdown complete");
  } catch (shutdownError) {
    console.error("Error during shutdown:", shutdownError);
  }

  process.exit(0);
});
