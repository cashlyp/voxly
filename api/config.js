require("dotenv").config();
const fs = require("fs");
const path = require("path");

const isProduction = process.env.NODE_ENV === "production";

function readEnv(name) {
  const value = process.env[name];
  if (typeof value === "string" && value.length > 0) {
    return value;
  }
  return undefined;
}

function ensure(name, fallback) {
  const value = readEnv(name);
  if (value !== undefined) {
    return value;
  }
  if (fallback !== undefined) {
    if (!isProduction) {
      console.warn(
        `Environment variable "${name}" is missing. Using fallback value in development.`,
      );
    }
    return fallback;
  }
  const message = `Missing required environment variable "${name}".`;
  if (isProduction) {
    throw new Error(message);
  }
  console.warn(`${message} Continuing because NODE_ENV !== 'production'.`);
  return "";
}

function normalizeHostname(value) {
  if (!value) return "";
  const trimmed = String(value).trim();
  if (!trimmed) return "";
  try {
    if (trimmed.includes("://")) {
      const parsed = new URL(trimmed);
      return parsed.host;
    }
  } catch {
    // fall through to basic cleanup
  }
  return trimmed.replace(/^https?:\/\//i, "").replace(/\/+$/, "");
}

function parseList(rawValue) {
  if (!rawValue) return [];
  return String(rawValue)
    .split(",")
    .map((value) => value.trim())
    .filter(Boolean);
}

const corsOriginsRaw = ensure("CORS_ORIGINS", "");
const corsOrigins = corsOriginsRaw
  .split(",")
  .map((origin) => origin.trim())
  .filter(Boolean);

const recordingEnabled =
  String(readEnv("RECORDING_ENABLED") || "false").toLowerCase() === "true";
const transferNumber = readEnv("TRANSFER_NUMBER");
const defaultSmsBusinessId = readEnv("DEFAULT_SMS_BUSINESS_ID") || null;
const deepgramModel = readEnv("DEEPGRAM_MODEL") || "nova-2";
const twilioGatherFallback =
  String(readEnv("TWILIO_GATHER_FALLBACK") || "true").toLowerCase() === "true";
const twilioMachineDetection = readEnv("TWILIO_MACHINE_DETECTION") || "Enable";
const twilioMachineDetectionTimeoutRaw = readEnv(
  "TWILIO_MACHINE_DETECTION_TIMEOUT",
);
const twilioMachineDetectionTimeout = Number.isFinite(
  Number(twilioMachineDetectionTimeoutRaw),
)
  ? Number(twilioMachineDetectionTimeoutRaw)
  : undefined;
const twilioTtsMaxWaitMs = Number(readEnv("TWILIO_TTS_MAX_WAIT_MS") || "1200");
const twilioFinalPromptTtsTimeoutMs = Number(
  readEnv("TWILIO_FINAL_PROMPT_TTS_TIMEOUT_MS") || "6000",
);
const twilioTtsStrictPlay =
  String(readEnv("TWILIO_TTS_STRICT_PLAY") || "true").toLowerCase() ===
  "true";
const twilioTtsPrewarmEnabled =
  String(readEnv("TWILIO_TTS_PREWARM_ENABLED") || "true").toLowerCase() ===
  "true";
const twilioWebhookValidationRaw = (
  readEnv("TWILIO_WEBHOOK_VALIDATION") || (isProduction ? "strict" : "warn")
).toLowerCase();
const twilioWebhookValidationModes = new Set(["strict", "warn", "off"]);
const twilioWebhookValidation = twilioWebhookValidationModes.has(
  twilioWebhookValidationRaw,
)
  ? twilioWebhookValidationRaw
  : isProduction
    ? "strict"
    : "warn";
const vonageWebhookValidationRaw = (
  readEnv("VONAGE_WEBHOOK_VALIDATION") || (isProduction ? "strict" : "warn")
).toLowerCase();
const vonageWebhookValidationModes = new Set(["strict", "warn", "off"]);
const vonageWebhookValidation = vonageWebhookValidationModes.has(
  vonageWebhookValidationRaw,
)
  ? vonageWebhookValidationRaw
  : isProduction
    ? "strict"
    : "warn";
const vonageWebhookSignatureSecret =
  readEnv("VONAGE_WEBHOOK_SIGNATURE_SECRET") || readEnv("VONAGE_SIGNATURE_SECRET");
const vonageWebhookMaxSkewMs = Number(
  readEnv("VONAGE_WEBHOOK_MAX_SKEW_MS") || "300000",
);
const vonageWebhookRequirePayloadHash =
  String(readEnv("VONAGE_WEBHOOK_REQUIRE_PAYLOAD_HASH") || "false").toLowerCase() ===
  "true";
const vonageDtmfWebhookEnabled =
  String(readEnv("VONAGE_DTMF_WEBHOOK_ENABLED") || "false").toLowerCase() ===
  "true";
const telegramWebhookValidationRaw = (
  readEnv("TELEGRAM_WEBHOOK_VALIDATION") || (isProduction ? "strict" : "warn")
).toLowerCase();
const telegramWebhookValidationModes = new Set(["strict", "warn", "off"]);
const telegramWebhookValidation = telegramWebhookValidationModes.has(
  telegramWebhookValidationRaw,
)
  ? telegramWebhookValidationRaw
  : isProduction
    ? "strict"
    : "warn";
const awsWebhookValidationRaw = (
  readEnv("AWS_WEBHOOK_VALIDATION") || (isProduction ? "strict" : "warn")
).toLowerCase();
const awsWebhookValidationModes = new Set(["strict", "warn", "off"]);
const awsWebhookValidation = awsWebhookValidationModes.has(
  awsWebhookValidationRaw,
)
  ? awsWebhookValidationRaw
  : isProduction
    ? "strict"
    : "warn";
const awsWebhookSecret = readEnv("AWS_WEBHOOK_SECRET");

const callProvider = ensure("CALL_PROVIDER", "twilio").toLowerCase();
const awsRegion = ensure("AWS_REGION", "us-east-1");
const apiSecret = readEnv("API_SECRET");
const adminApiToken = apiSecret || readEnv("ADMIN_API_TOKEN");
const complianceModeRaw = (
  readEnv("CONFIG_COMPLIANCE_MODE") || "safe"
).toLowerCase();
const allowedComplianceModes = new Set(["safe", "dev_insecure"]);
const complianceMode = allowedComplianceModes.has(complianceModeRaw)
  ? complianceModeRaw
  : "safe";
if (!allowedComplianceModes.has(complianceModeRaw) && !isProduction) {
  console.warn(
    `Invalid CONFIG_COMPLIANCE_MODE "${complianceModeRaw}". Falling back to "safe".`,
  );
}
const dtmfEncryptionKey = readEnv("DTMF_ENCRYPTION_KEY");
const apiHmacSecret = apiSecret || readEnv("API_HMAC_SECRET");
const apiHmacMaxSkewMs = Number(readEnv("API_HMAC_MAX_SKEW_MS") || "300000");
if (!apiHmacSecret) {
  const message =
    'Missing required environment variable "API_SECRET" (or legacy API_HMAC_SECRET).';
  if (isProduction) {
    throw new Error(message);
  }
  console.warn(`${message} HMAC auth will be disabled.`);
}
const streamAuthSecret = readEnv("STREAM_AUTH_SECRET") || apiHmacSecret;
const streamAuthMaxSkewMs = Number(
  readEnv("STREAM_AUTH_MAX_SKEW_MS") || apiHmacMaxSkewMs || "300000",
);

function parseJsonObject(rawValue, label) {
  if (!rawValue) return {};
  try {
    const parsed = JSON.parse(rawValue);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      throw new Error("JSON must be an object");
    }
    return parsed;
  } catch (error) {
    const tag = label ? ` (${label})` : "";
    console.warn(`Unable to parse JSON config${tag}: ${error.message}`);
    return {};
  }
}

const inboundDefaultPrompt = readEnv("INBOUND_PROMPT");
const inboundDefaultFirstMessage = readEnv("INBOUND_FIRST_MESSAGE");
const inboundRoutes = parseJsonObject(
  readEnv("INBOUND_NUMBER_ROUTES"),
  "INBOUND_NUMBER_ROUTES",
);
const inboundPreConnectMessage = readEnv("INBOUND_PRECONNECT_MESSAGE");
const inboundPreConnectPauseSeconds = Number(
  readEnv("INBOUND_PRECONNECT_PAUSE_S") || "1",
);
const inboundFirstMediaTimeoutMs = Number(
  readEnv("INBOUND_STREAM_FIRST_MEDIA_TIMEOUT_MS") || "8000",
);
const inboundRateLimitWindowMs =
  Number(readEnv("INBOUND_RATE_LIMIT_WINDOW_S") || "60") * 1000;
const inboundRateLimitMax = Number(readEnv("INBOUND_RATE_LIMIT_MAX") || "0");
const inboundRateLimitSmsEnabled =
  String(readEnv("INBOUND_RATE_LIMIT_SMS") || "false").toLowerCase() === "true";
const inboundRateLimitCallbackEnabled =
  String(readEnv("INBOUND_RATE_LIMIT_CALLBACK") || "false").toLowerCase() ===
  "true";
const inboundCallbackDelayMinutes = Number(
  readEnv("INBOUND_CALLBACK_DELAY_MIN") || "15",
);

const providerFailoverEnabled =
  String(readEnv("PROVIDER_FAILOVER_ENABLED") || "true").toLowerCase() ===
  "true";
const providerFailoverThreshold = Number(
  readEnv("PROVIDER_ERROR_THRESHOLD") || "3",
);
const providerFailoverWindowMs =
  Number(readEnv("PROVIDER_ERROR_WINDOW_S") || "120") * 1000;
const providerFailoverCooldownMs =
  Number(readEnv("PROVIDER_COOLDOWN_S") || "300") * 1000;
const keypadGuardEnabled =
  String(readEnv("KEYPAD_GUARD_ENABLED") || "true").toLowerCase() === "true";
const keypadVonageDtmfTimeoutMs = Number(
  readEnv("KEYPAD_VONAGE_DTMF_TIMEOUT_MS") || "12000",
);
const keypadProviderOverrideCooldownMs =
  Number(readEnv("KEYPAD_PROVIDER_OVERRIDE_COOLDOWN_S") || "1800") * 1000;
const callJobIntervalMs = Number(
  readEnv("CALL_JOB_PROCESSOR_INTERVAL_MS") || "5000",
);
const callJobRetryBaseMs = Number(readEnv("CALL_JOB_RETRY_BASE_MS") || "5000");
const callJobRetryMaxMs = Number(readEnv("CALL_JOB_RETRY_MAX_MS") || "60000");
const callJobMaxAttempts = Number(readEnv("CALL_JOB_MAX_ATTEMPTS") || "3");
const callJobTimeoutMs = Number(readEnv("CALL_JOB_TIMEOUT_MS") || "45000");
const callJobDlqAlertThreshold = Number(
  readEnv("CALL_JOB_DLQ_ALERT_THRESHOLD") || "20",
);
const callJobDlqMaxReplays = Number(
  readEnv("CALL_JOB_DLQ_MAX_REPLAYS") || "2",
);
const callSloFirstMediaMs = Number(
  readEnv("CALL_SLO_FIRST_MEDIA_MS") || "4000",
);
const callSloAnswerDelayMs = Number(
  readEnv("CALL_SLO_ANSWER_DELAY_MS") || "12000",
);
const callSloSttFailures = Number(readEnv("CALL_SLO_STT_FAILURES") || "3");
const webhookRetryBaseMs = Number(readEnv("WEBHOOK_RETRY_BASE_MS") || "5000");
const webhookRetryMaxMs = Number(readEnv("WEBHOOK_RETRY_MAX_MS") || "60000");
const webhookRetryMaxAttempts = Number(
  readEnv("WEBHOOK_RETRY_MAX_ATTEMPTS") || "5",
);
const webhookTelegramTimeoutMs = Number(
  readEnv("WEBHOOK_TELEGRAM_TIMEOUT_MS") || "15000",
);
const paymentFeatureEnabled =
  String(readEnv("PAYMENT_FEATURE_ENABLED") || "true").toLowerCase() === "true";
const paymentKillSwitch =
  String(readEnv("PAYMENT_KILL_SWITCH") || "false").toLowerCase() === "true";
const paymentAllowTwilio =
  String(readEnv("PAYMENT_ALLOW_TWILIO") || "true").toLowerCase() === "true";
const paymentRequireScriptOptIn =
  String(readEnv("PAYMENT_REQUIRE_SCRIPT_OPT_IN") || "false").toLowerCase() ===
  "true";
const paymentDefaultCurrency =
  String(readEnv("PAYMENT_DEFAULT_CURRENCY") || "USD")
    .trim()
    .toUpperCase()
    .slice(0, 3) || "USD";
const paymentMinAmount = Number(readEnv("PAYMENT_MIN_AMOUNT") || "0");
const paymentMaxAmount = Number(readEnv("PAYMENT_MAX_AMOUNT") || "0");
const paymentWebhookIdempotencyTtlMs = Number(
  readEnv("PAYMENT_WEBHOOK_IDEMPOTENCY_TTL_MS") || "300000",
);
const paymentMaxAttemptsPerCall = Number(
  readEnv("PAYMENT_MAX_ATTEMPTS_PER_CALL") || "3",
);
const paymentRetryCooldownMs = Number(
  readEnv("PAYMENT_RETRY_COOLDOWN_MS") || "20000",
);
const paymentReconcileEnabled =
  String(readEnv("PAYMENT_RECONCILE_ENABLED") || "true").toLowerCase() ===
  "true";
const paymentReconcileIntervalMs = Number(
  readEnv("PAYMENT_RECONCILE_INTERVAL_MS") || "120000",
);
const paymentReconcileStaleSeconds = Number(
  readEnv("PAYMENT_RECONCILE_STALE_SECONDS") || "240",
);
const paymentReconcileBatchSize = Number(
  readEnv("PAYMENT_RECONCILE_BATCH_SIZE") || "20",
);
const paymentSmsFallbackEnabled =
  String(readEnv("PAYMENT_SMS_FALLBACK_ENABLED") || "false").toLowerCase() ===
  "true";
const paymentSmsFallbackUrlTemplate =
  readEnv("PAYMENT_SMS_FALLBACK_URL_TEMPLATE") || "";
const paymentSmsFallbackMessageTemplate =
  readEnv("PAYMENT_SMS_FALLBACK_MESSAGE_TEMPLATE") ||
  "Complete your payment securely here: {payment_url}";
const paymentSmsFallbackTtlSeconds = Number(
  readEnv("PAYMENT_SMS_FALLBACK_TTL_SECONDS") || "900",
);
const paymentSmsFallbackSecret =
  readEnv("PAYMENT_SMS_FALLBACK_SECRET") ||
  readEnv("API_SECRET") ||
  readEnv("ADMIN_API_TOKEN") ||
  "";
const paymentSmsFallbackMaxPerCall = Number(
  readEnv("PAYMENT_SMS_FALLBACK_MAX_PER_CALL") || "1",
);

function loadPrivateKey(rawValue) {
  if (!rawValue) {
    return undefined;
  }

  const normalized = rawValue.replace(/\\n/g, "\n");
  if (normalized.includes("-----BEGIN")) {
    return normalized;
  }

  try {
    const filePath = path.isAbsolute(normalized)
      ? normalized
      : path.join(process.cwd(), normalized);
    return fs.readFileSync(filePath, "utf8");
  } catch (error) {
    console.warn(
      `Unable to load Vonage private key from path "${normalized}": ${error.message}`,
    );
    return undefined;
  }
}

const vonagePrivateKey = loadPrivateKey(readEnv("VONAGE_PRIVATE_KEY"));
const vonageVoiceWebsocketContentType =
  readEnv("VONAGE_WEBSOCKET_CONTENT_TYPE") || "audio/l16;rate=16000";
const serverHostname = normalizeHostname(ensure("SERVER", ""));
const liveConsoleAudioTickMs = Number(
  readEnv("LIVE_CONSOLE_AUDIO_TICK_MS") || "160",
);
const liveConsoleEditDebounceMs = Number(
  readEnv("LIVE_CONSOLE_EDIT_DEBOUNCE_MS") || "700",
);
const liveConsoleUserLevelThreshold = Number(
  readEnv("LIVE_CONSOLE_USER_LEVEL_THRESHOLD") || "0.08",
);
const liveConsoleUserHoldMs = Number(
  readEnv("LIVE_CONSOLE_USER_HOLD_MS") || "450",
);
const liveConsoleCarrier = readEnv("LIVE_CONSOLE_CARRIER") || "VOICEDNUT";
const liveConsoleNetworkLabel = readEnv("LIVE_CONSOLE_NETWORK_LABEL") || "LTE";
const telegramAdminChatId =
  readEnv("TELEGRAM_ADMIN_CHAT_ID") || readEnv("ADMIN_TELEGRAM_ID");
const telegramAdminChatIds = parseList(readEnv("TELEGRAM_ADMIN_CHAT_IDS"));
const telegramAdminUserIds = parseList(readEnv("TELEGRAM_ADMIN_USER_IDS"));
const telegramOperatorChatIds = parseList(
  readEnv("TELEGRAM_OPERATOR_CHAT_IDS"),
);
const telegramOperatorUserIds = parseList(
  readEnv("TELEGRAM_OPERATOR_USER_IDS"),
);
const telegramViewerChatIds = parseList(readEnv("TELEGRAM_VIEWER_CHAT_IDS"));
const telegramViewerUserIds = parseList(readEnv("TELEGRAM_VIEWER_USER_IDS"));
if (!telegramAdminChatIds.length && telegramAdminChatId) {
  telegramAdminChatIds.push(telegramAdminChatId);
}
const emailProvider = (readEnv("EMAIL_PROVIDER") || "sendgrid").toLowerCase();
const emailDefaultFrom = readEnv("EMAIL_DEFAULT_FROM") || "";
const emailVerifiedDomains = (readEnv("EMAIL_VERIFIED_DOMAINS") || "")
  .split(",")
  .map((value) => value.trim().toLowerCase())
  .filter(Boolean);
const emailRateLimitProvider = Number(
  readEnv("EMAIL_RATE_LIMIT_PROVIDER_PER_MIN") || "120",
);
const emailRateLimitTenant = Number(
  readEnv("EMAIL_RATE_LIMIT_TENANT_PER_MIN") || "120",
);
const emailRateLimitDomain = Number(
  readEnv("EMAIL_RATE_LIMIT_DOMAIN_PER_MIN") || "120",
);
const emailQueueIntervalMs = Number(
  readEnv("EMAIL_QUEUE_INTERVAL_MS") || "5000",
);
const emailMaxRetries = Number(readEnv("EMAIL_MAX_RETRIES") || "5");
const emailRequestTimeoutMs = Number(
  readEnv("EMAIL_REQUEST_TIMEOUT_MS") || "15000",
);
const emailMaxSubjectChars = Number(
  readEnv("EMAIL_MAX_SUBJECT_CHARS") || "200",
);
const emailMaxBodyChars = Number(readEnv("EMAIL_MAX_BODY_CHARS") || "200000");
const emailMaxBulkRecipients = Number(
  readEnv("EMAIL_MAX_BULK_RECIPIENTS") || "500",
);
const emailDlqAlertThreshold = Number(
  readEnv("EMAIL_DLQ_ALERT_THRESHOLD") || "25",
);
const emailDlqMaxReplays = Number(readEnv("EMAIL_DLQ_MAX_REPLAYS") || "2");
const emailQueueClaimLeaseMs = Number(
  readEnv("EMAIL_QUEUE_CLAIM_LEASE_MS") || "60000",
);
const emailQueueStaleSendingMs = Number(
  readEnv("EMAIL_QUEUE_STALE_SENDING_MS") || "180000",
);
const emailProviderEventsTtlDays = Number(
  readEnv("EMAIL_PROVIDER_EVENTS_TTL_DAYS") || "14",
);
const emailCircuitBreakerEnabled =
  String(readEnv("EMAIL_CIRCUIT_BREAKER_ENABLED") || "true").toLowerCase() ===
  "true";
const emailCircuitBreakerFailureThreshold = Number(
  readEnv("EMAIL_CIRCUIT_BREAKER_FAILURE_THRESHOLD") || "5",
);
const emailCircuitBreakerWindowMs = Number(
  readEnv("EMAIL_CIRCUIT_BREAKER_WINDOW_MS") || "120000",
);
const emailCircuitBreakerCooldownMs = Number(
  readEnv("EMAIL_CIRCUIT_BREAKER_COOLDOWN_MS") || "120000",
);
const smsProviderTimeoutMs = Number(
  readEnv("SMS_PROVIDER_TIMEOUT_MS") || "15000",
);
const smsProvider = (readEnv("SMS_PROVIDER") || callProvider).toLowerCase();
const smsAiTimeoutMs = Number(readEnv("SMS_AI_TIMEOUT_MS") || "12000");
const smsMaxMessageChars = Number(readEnv("SMS_MAX_MESSAGE_CHARS") || "1600");
const smsMaxBulkRecipients = Number(readEnv("SMS_MAX_BULK_RECIPIENTS") || "100");
const smsProviderFailoverEnabled =
  String(readEnv("SMS_PROVIDER_FAILOVER_ENABLED") || "true").toLowerCase() ===
  "true";
const smsCircuitBreakerEnabled =
  String(readEnv("SMS_CIRCUIT_BREAKER_ENABLED") || "true").toLowerCase() ===
  "true";
const smsCircuitBreakerFailureThreshold = Number(
  readEnv("SMS_CIRCUIT_BREAKER_FAILURE_THRESHOLD") || "3",
);
const smsCircuitBreakerWindowMs = Number(
  readEnv("SMS_CIRCUIT_BREAKER_WINDOW_MS") || "120000",
);
const smsCircuitBreakerCooldownMs = Number(
  readEnv("SMS_CIRCUIT_BREAKER_COOLDOWN_MS") || "120000",
);
const smsWebhookDedupeTtlMs = Number(
  readEnv("SMS_WEBHOOK_DEDUPE_TTL_MS") || "300000",
);
const smsReconcileEnabled =
  String(readEnv("SMS_RECONCILE_ENABLED") || "true").toLowerCase() === "true";
const smsReconcileIntervalMs = Number(
  readEnv("SMS_RECONCILE_INTERVAL_MS") || "120000",
);
const smsReconcileStaleMinutes = Number(
  readEnv("SMS_RECONCILE_STALE_MINUTES") || "15",
);
const smsReconcileBatchSize = Number(
  readEnv("SMS_RECONCILE_BATCH_SIZE") || "50",
);
const outboundRateWindowMs = Number(
  readEnv("OUTBOUND_RATE_LIMIT_WINDOW_MS") || "60000",
);
const smsOutboundUserRateLimit = Number(
  readEnv("SMS_OUTBOUND_USER_RATE_LIMIT_PER_WINDOW") || "15",
);
const smsOutboundGlobalRateLimit = Number(
  readEnv("SMS_OUTBOUND_GLOBAL_RATE_LIMIT_PER_WINDOW") || "120",
);
const emailOutboundUserRateLimit = Number(
  readEnv("EMAIL_OUTBOUND_USER_RATE_LIMIT_PER_WINDOW") || "20",
);
const emailOutboundGlobalRateLimit = Number(
  readEnv("EMAIL_OUTBOUND_GLOBAL_RATE_LIMIT_PER_WINDOW") || "120",
);
const outboundHandlerTimeoutMs = Number(
  readEnv("OUTBOUND_HANDLER_TIMEOUT_MS") || "30000",
);
const emailUnsubscribeUrl =
  readEnv("EMAIL_UNSUBSCRIBE_URL") ||
  (serverHostname ? `https://${serverHostname}/webhook/email-unsubscribe` : "");
const emailWarmupMaxPerDay = Number(readEnv("EMAIL_WARMUP_MAX_PER_DAY") || "0");
const emailDkimEnabled =
  String(readEnv("EMAIL_DKIM_ENABLED") || "true").toLowerCase() === "true";
const emailSpfEnabled =
  String(readEnv("EMAIL_SPF_ENABLED") || "true").toLowerCase() === "true";
const emailDmarcPolicy = readEnv("EMAIL_DMARC_POLICY") || "none";
const emailWebhookSecret = readEnv("EMAIL_WEBHOOK_SECRET") || "";
const emailWebhookValidation = (
  readEnv("EMAIL_WEBHOOK_VALIDATION") || "warn"
).toLowerCase();
const emailUnsubscribeSecret = readEnv("EMAIL_UNSUBSCRIBE_SECRET") || "";
const sendgridApiKey = readEnv("SENDGRID_API_KEY");
const sendgridBaseUrl = readEnv("SENDGRID_BASE_URL");
const mailgunApiKey = readEnv("MAILGUN_API_KEY");
const mailgunDomain = readEnv("MAILGUN_DOMAIN");
const mailgunBaseUrl = readEnv("MAILGUN_BASE_URL");
const sesRegion = readEnv("SES_REGION") || awsRegion;
const sesAccessKeyId =
  readEnv("SES_ACCESS_KEY_ID") || readEnv("AWS_ACCESS_KEY_ID");
const sesSecretAccessKey =
  readEnv("SES_SECRET_ACCESS_KEY") || readEnv("AWS_SECRET_ACCESS_KEY");
const sesSessionToken =
  readEnv("SES_SESSION_TOKEN") || readEnv("AWS_SESSION_TOKEN");

module.exports = {
  platform: {
    provider: callProvider,
  },
  twilio: {
    accountSid: ensure("TWILIO_ACCOUNT_SID"),
    authToken: ensure("TWILIO_AUTH_TOKEN"),
    fromNumber: ensure("FROM_NUMBER"),
    transferNumber,
    gatherFallback: twilioGatherFallback,
    machineDetection: twilioMachineDetection,
    machineDetectionTimeout: twilioMachineDetectionTimeout,
    ttsMaxWaitMs: Number.isFinite(twilioTtsMaxWaitMs)
      ? twilioTtsMaxWaitMs
      : 1200,
    finalPromptTtsTimeoutMs: Number.isFinite(twilioFinalPromptTtsTimeoutMs)
      ? twilioFinalPromptTtsTimeoutMs
      : 6000,
    strictTtsPlay: twilioTtsStrictPlay,
    ttsPrewarmEnabled: twilioTtsPrewarmEnabled,
    webhookValidation: twilioWebhookValidation,
  },
  aws: {
    region: awsRegion,
    connect: {
      instanceId: ensure("AWS_CONNECT_INSTANCE_ID", ""),
      contactFlowId: ensure("AWS_CONNECT_CONTACT_FLOW_ID", ""),
      queueId: readEnv("AWS_CONNECT_QUEUE_ID"),
      sourcePhoneNumber: readEnv("AWS_CONNECT_SOURCE_PHONE_NUMBER"),
      transcriptsQueueUrl: readEnv("AWS_TRANSCRIPTS_QUEUE_URL"),
      eventBusName: readEnv("AWS_EVENT_BUS_NAME"),
    },
    polly: {
      voiceId: ensure("AWS_POLLY_VOICE_ID", "Joanna"),
      outputBucket: readEnv("AWS_POLLY_OUTPUT_BUCKET"),
      outputPrefix: readEnv("AWS_POLLY_OUTPUT_PREFIX") || "tts/",
    },
    s3: {
      mediaBucket:
        readEnv("AWS_MEDIA_BUCKET") || readEnv("AWS_POLLY_OUTPUT_BUCKET"),
    },
    pinpoint: {
      applicationId: readEnv("AWS_PINPOINT_APPLICATION_ID"),
      originationNumber:
        readEnv("AWS_PINPOINT_ORIGINATION_NUMBER") ||
        readEnv("AWS_CONNECT_SOURCE_PHONE_NUMBER"),
      region: readEnv("AWS_PINPOINT_REGION") || awsRegion,
    },
    transcribe: {
      languageCode: ensure("AWS_TRANSCRIBE_LANGUAGE_CODE", "en-US"),
      vocabularyFilterName: readEnv("AWS_TRANSCRIBE_VOCABULARY_FILTER_NAME"),
    },
    webhookValidation: awsWebhookValidation,
    webhookSecret: awsWebhookSecret,
  },
  vonage: {
    apiKey: readEnv("VONAGE_API_KEY"),
    apiSecret: readEnv("VONAGE_API_SECRET"),
    applicationId: readEnv("VONAGE_APPLICATION_ID"),
    privateKey: vonagePrivateKey,
    voice: {
      fromNumber: readEnv("VONAGE_VOICE_FROM_NUMBER"),
      answerUrl: readEnv("VONAGE_ANSWER_URL"),
      eventUrl: readEnv("VONAGE_EVENT_URL"),
      websocketContentType: vonageVoiceWebsocketContentType,
    },
    webhookValidation: vonageWebhookValidation,
    webhookSignatureSecret: vonageWebhookSignatureSecret,
    webhookMaxSkewMs: Number.isFinite(vonageWebhookMaxSkewMs)
      ? vonageWebhookMaxSkewMs
      : 300000,
    webhookRequirePayloadHash: vonageWebhookRequirePayloadHash,
    dtmfWebhookEnabled: vonageDtmfWebhookEnabled,
    sms: {
      fromNumber: readEnv("VONAGE_SMS_FROM_NUMBER"),
    },
  },
  telegram: {
    botToken: ensure("TELEGRAM_BOT_TOKEN", process.env.BOT_TOKEN),
    adminChatId: telegramAdminChatId,
    adminChatIds: telegramAdminChatIds,
    adminUserIds: telegramAdminUserIds,
    operatorChatIds: telegramOperatorChatIds,
    operatorUserIds: telegramOperatorUserIds,
    viewerChatIds: telegramViewerChatIds,
    viewerUserIds: telegramViewerUserIds,
    webhookValidation: telegramWebhookValidation,
  },
  openRouter: {
    apiKey: ensure("OPENROUTER_API_KEY"),
    model: ensure("OPENROUTER_MODEL", "meta-llama/llama-3.1-8b-instruct:free"),
    backupModel: readEnv("OPENROUTER_BACKUP_MODEL"),
    siteUrl: ensure("YOUR_SITE_URL", "http://localhost:3000"),
    siteName: ensure("YOUR_SITE_NAME", "Voice Call Bot"),
    maxTokens: Number(ensure("OPENROUTER_MAX_TOKENS", "160")),
    responseTimeoutMs: Number(readEnv("OPENROUTER_RESPONSE_TIMEOUT_MS") || "25000"),
    streamIdleTimeoutMs: Number(readEnv("OPENROUTER_STREAM_IDLE_TIMEOUT_MS") || "8000"),
    contextTokenBudget: Number(readEnv("OPENROUTER_CONTEXT_TOKEN_BUDGET") || "3500"),
    summaryMaxChars: Number(readEnv("OPENROUTER_SUMMARY_MAX_CHARS") || "1200"),
    recentTurns: Number(readEnv("OPENROUTER_RECENT_TURNS") || "10"),
    memoryFactLimit: Number(readEnv("OPENROUTER_MEMORY_FACT_LIMIT") || "12"),
    memoryFactMaxAgeDays: Number(readEnv("OPENROUTER_MEMORY_FACT_MAX_AGE_DAYS") || "14"),
    memorySummaryMinTurns: Number(readEnv("OPENROUTER_MEMORY_SUMMARY_MIN_TURNS") || "10"),
    memorySummaryRollupBatch: Number(readEnv("OPENROUTER_MEMORY_SUMMARY_BATCH") || "6"),
    maxToolLoops: Number(readEnv("OPENROUTER_MAX_TOOL_LOOPS") || "6"),
    toolExecutionTimeoutMs: Number(readEnv("OPENROUTER_TOOL_EXEC_TIMEOUT_MS") || "12000"),
    toolRetryLimit: Number(readEnv("OPENROUTER_TOOL_RETRY_LIMIT") || "1"),
    toolBudgetPerInteraction: Number(readEnv("OPENROUTER_TOOL_BUDGET_PER_INTERACTION") || "4"),
    toolIdempotencyTtlMs: Number(readEnv("OPENROUTER_TOOL_IDEMPOTENCY_TTL_MS") || "120000"),
    strictToolSchemas: String(readEnv("OPENROUTER_STRICT_TOOL_SCHEMAS") || "true").toLowerCase() === "true",
    toolCircuitBreaker: {
      enabled: String(readEnv("OPENROUTER_TOOL_CIRCUIT_ENABLED") || "true").toLowerCase() === "true",
      failureThreshold: Number(readEnv("OPENROUTER_TOOL_CIRCUIT_FAILURE_THRESHOLD") || "4"),
      windowMs: Number(readEnv("OPENROUTER_TOOL_CIRCUIT_WINDOW_MS") || "120000"),
      cooldownMs: Number(readEnv("OPENROUTER_TOOL_CIRCUIT_COOLDOWN_MS") || "90000"),
    },
    personaConsistencyThreshold: Number(readEnv("OPENROUTER_PERSONA_CONSISTENCY_THRESHOLD") || "0.55"),
    slo: {
      responseRttMs: Number(readEnv("OPENROUTER_SLO_RESPONSE_RTT_MS") || "7000"),
      ttfbMs: Number(readEnv("OPENROUTER_SLO_TTFB_MS") || "2000"),
      toolFailureRate: Number(readEnv("OPENROUTER_SLO_TOOL_FAILURE_RATE") || "0.3"),
    },
    alerting: {
      windowMinutes: Number(readEnv("OPENROUTER_ALERT_WINDOW_MINUTES") || "15"),
      toolFailureRate: Number(readEnv("OPENROUTER_ALERT_TOOL_FAILURE_RATE") || "0.35"),
      circuitOpenCount: Number(readEnv("OPENROUTER_ALERT_CIRCUIT_OPEN_COUNT") || "2"),
      sloDegradedCount: Number(readEnv("OPENROUTER_ALERT_SLO_DEGRADED_COUNT") || "1"),
    },
  },
  deepgram: {
    apiKey: ensure("DEEPGRAM_API_KEY"),
    voiceModel: ensure("VOICE_MODEL", "aura-asteria-en"),
    model: deepgramModel,
  },
  server: {
    port: Number(ensure("PORT", "3000")),
    hostname: serverHostname,
    corsOrigins,
    rateLimit: {
      windowMs: Number(ensure("RATE_LIMIT_WINDOW_MS", "60000")),
      max: Number(ensure("RATE_LIMIT_MAX", "300")),
    },
  },
  database: {
    schemaVersion: Number(readEnv("DB_SCHEMA_VERSION") || "2"),
    schemaStrict: String(readEnv("DB_SCHEMA_STRICT") || "true").toLowerCase() === "true",
  },
  admin: {
    apiToken: adminApiToken,
  },
  compliance: {
    mode: complianceMode,
    encryptionKey: dtmfEncryptionKey,
    isSafe: complianceMode !== "dev_insecure",
  },
  recording: {
    enabled: recordingEnabled,
  },
  liveConsole: {
    audioTickMs: Number.isFinite(liveConsoleAudioTickMs)
      ? liveConsoleAudioTickMs
      : 160,
    editDebounceMs: Number.isFinite(liveConsoleEditDebounceMs)
      ? liveConsoleEditDebounceMs
      : 700,
    userLevelThreshold: Number.isFinite(liveConsoleUserLevelThreshold)
      ? liveConsoleUserLevelThreshold
      : 0.08,
    userHoldMs: Number.isFinite(liveConsoleUserHoldMs)
      ? liveConsoleUserHoldMs
      : 450,
    carrier: liveConsoleCarrier,
    networkLabel: liveConsoleNetworkLabel,
  },
  email: {
    provider: emailProvider,
    defaultFrom: emailDefaultFrom,
    verifiedDomains: emailVerifiedDomains,
    queueIntervalMs: Number.isFinite(emailQueueIntervalMs)
      ? emailQueueIntervalMs
      : 5000,
    maxRetries: Number.isFinite(emailMaxRetries) ? emailMaxRetries : 5,
    requestTimeoutMs: Number.isFinite(emailRequestTimeoutMs)
      ? emailRequestTimeoutMs
      : 15000,
    maxSubjectChars: Number.isFinite(emailMaxSubjectChars)
      ? emailMaxSubjectChars
      : 200,
    maxBodyChars: Number.isFinite(emailMaxBodyChars) ? emailMaxBodyChars : 200000,
    maxBulkRecipients: Number.isFinite(emailMaxBulkRecipients)
      ? emailMaxBulkRecipients
      : 500,
    dlqAlertThreshold: Number.isFinite(emailDlqAlertThreshold)
      ? emailDlqAlertThreshold
      : 25,
    dlqMaxReplays: Number.isFinite(emailDlqMaxReplays)
      ? emailDlqMaxReplays
      : 2,
    queueClaimLeaseMs: Number.isFinite(emailQueueClaimLeaseMs)
      ? emailQueueClaimLeaseMs
      : 60000,
    queueStaleSendingMs: Number.isFinite(emailQueueStaleSendingMs)
      ? emailQueueStaleSendingMs
      : 180000,
    providerEventsTtlDays: Number.isFinite(emailProviderEventsTtlDays)
      ? emailProviderEventsTtlDays
      : 14,
    unsubscribeUrl: emailUnsubscribeUrl,
    circuitBreaker: {
      enabled: emailCircuitBreakerEnabled,
      failureThreshold: Number.isFinite(emailCircuitBreakerFailureThreshold)
        ? emailCircuitBreakerFailureThreshold
        : 5,
      windowMs: Number.isFinite(emailCircuitBreakerWindowMs)
        ? emailCircuitBreakerWindowMs
        : 120000,
      cooldownMs: Number.isFinite(emailCircuitBreakerCooldownMs)
        ? emailCircuitBreakerCooldownMs
        : 120000,
    },
    rateLimits: {
      perProviderPerMinute: Number.isFinite(emailRateLimitProvider)
        ? emailRateLimitProvider
        : 120,
      perTenantPerMinute: Number.isFinite(emailRateLimitTenant)
        ? emailRateLimitTenant
        : 120,
      perDomainPerMinute: Number.isFinite(emailRateLimitDomain)
        ? emailRateLimitDomain
        : 120,
    },
    warmup: {
      enabled: emailWarmupMaxPerDay > 0,
      maxPerDay: emailWarmupMaxPerDay,
    },
    deliverability: {
      dkimEnabled: emailDkimEnabled,
      spfEnabled: emailSpfEnabled,
      dmarcPolicy: emailDmarcPolicy,
    },
    webhookSecret: emailWebhookSecret,
    webhookValidation: emailWebhookValidation,
    unsubscribeSecret: emailUnsubscribeSecret,
    sendgrid: {
      apiKey: sendgridApiKey,
      baseUrl: sendgridBaseUrl,
    },
    mailgun: {
      apiKey: mailgunApiKey,
      domain: mailgunDomain,
      baseUrl: mailgunBaseUrl,
    },
    ses: {
      region: sesRegion,
      accessKeyId: sesAccessKeyId,
      secretAccessKey: sesSecretAccessKey,
      sessionToken: sesSessionToken,
    },
  },
  smsDefaults: {
    businessId: defaultSmsBusinessId,
  },
  sms: {
    provider: smsProvider,
    providerTimeoutMs: Number.isFinite(smsProviderTimeoutMs)
      ? smsProviderTimeoutMs
      : 15000,
    aiTimeoutMs: Number.isFinite(smsAiTimeoutMs) ? smsAiTimeoutMs : 12000,
    maxMessageChars: Number.isFinite(smsMaxMessageChars)
      ? smsMaxMessageChars
      : 1600,
    maxBulkRecipients: Number.isFinite(smsMaxBulkRecipients)
      ? smsMaxBulkRecipients
      : 100,
    providerFailoverEnabled: smsProviderFailoverEnabled,
    webhookDedupeTtlMs: Number.isFinite(smsWebhookDedupeTtlMs)
      ? smsWebhookDedupeTtlMs
      : 300000,
    circuitBreaker: {
      enabled: smsCircuitBreakerEnabled,
      failureThreshold: Number.isFinite(smsCircuitBreakerFailureThreshold)
        ? smsCircuitBreakerFailureThreshold
        : 3,
      windowMs: Number.isFinite(smsCircuitBreakerWindowMs)
        ? smsCircuitBreakerWindowMs
        : 120000,
      cooldownMs: Number.isFinite(smsCircuitBreakerCooldownMs)
        ? smsCircuitBreakerCooldownMs
        : 120000,
    },
    reconcile: {
      enabled: smsReconcileEnabled,
      intervalMs: Number.isFinite(smsReconcileIntervalMs)
        ? smsReconcileIntervalMs
        : 120000,
      staleMinutes: Number.isFinite(smsReconcileStaleMinutes)
        ? smsReconcileStaleMinutes
        : 15,
      batchSize: Number.isFinite(smsReconcileBatchSize)
        ? smsReconcileBatchSize
        : 50,
    },
  },
  outboundLimits: {
    windowMs: Number.isFinite(outboundRateWindowMs)
      ? outboundRateWindowMs
      : 60000,
    handlerTimeoutMs: Number.isFinite(outboundHandlerTimeoutMs)
      ? outboundHandlerTimeoutMs
      : 30000,
    sms: {
      perUser: Number.isFinite(smsOutboundUserRateLimit)
        ? smsOutboundUserRateLimit
        : 15,
      global: Number.isFinite(smsOutboundGlobalRateLimit)
        ? smsOutboundGlobalRateLimit
        : 120,
    },
    email: {
      perUser: Number.isFinite(emailOutboundUserRateLimit)
        ? emailOutboundUserRateLimit
        : 20,
      global: Number.isFinite(emailOutboundGlobalRateLimit)
        ? emailOutboundGlobalRateLimit
        : 120,
    },
  },
  apiAuth: {
    hmacSecret: apiHmacSecret,
    maxSkewMs: apiHmacMaxSkewMs,
  },
  streamAuth: {
    secret: streamAuthSecret,
    maxSkewMs: streamAuthMaxSkewMs,
  },
  inbound: {
    defaultPrompt: inboundDefaultPrompt,
    defaultFirstMessage: inboundDefaultFirstMessage,
    routes: inboundRoutes,
    preConnectMessage: inboundPreConnectMessage,
    preConnectPauseSeconds: inboundPreConnectPauseSeconds,
    firstMediaTimeoutMs: inboundFirstMediaTimeoutMs,
    rateLimitWindowMs: inboundRateLimitWindowMs,
    rateLimitMax: inboundRateLimitMax,
    rateLimitSmsEnabled: inboundRateLimitSmsEnabled,
    rateLimitCallbackEnabled: inboundRateLimitCallbackEnabled,
    callbackDelayMinutes: inboundCallbackDelayMinutes,
  },
  providerFailover: {
    enabled: providerFailoverEnabled,
    errorThreshold: providerFailoverThreshold,
    errorWindowMs: providerFailoverWindowMs,
    cooldownMs: providerFailoverCooldownMs,
  },
  keypadGuard: {
    enabled: keypadGuardEnabled,
    vonageDtmfTimeoutMs: Number.isFinite(keypadVonageDtmfTimeoutMs)
      ? keypadVonageDtmfTimeoutMs
      : 12000,
    providerOverrideCooldownMs: Number.isFinite(keypadProviderOverrideCooldownMs)
      ? keypadProviderOverrideCooldownMs
      : 1800000,
  },
  callJobs: {
    intervalMs: callJobIntervalMs,
    retryBaseMs: callJobRetryBaseMs,
    retryMaxMs: callJobRetryMaxMs,
    maxAttempts: callJobMaxAttempts,
    timeoutMs: Number.isFinite(callJobTimeoutMs) ? callJobTimeoutMs : 45000,
    dlqAlertThreshold: Number.isFinite(callJobDlqAlertThreshold)
      ? callJobDlqAlertThreshold
      : 20,
    dlqMaxReplays: Number.isFinite(callJobDlqMaxReplays)
      ? callJobDlqMaxReplays
      : 2,
  },
  callSlo: {
    firstMediaMs: callSloFirstMediaMs,
    answerDelayMs: callSloAnswerDelayMs,
    sttFailureThreshold: callSloSttFailures,
  },
  payment: {
    enabled: paymentFeatureEnabled,
    killSwitch: paymentKillSwitch,
    allowTwilio: paymentAllowTwilio,
    requireScriptOptIn: paymentRequireScriptOptIn,
    defaultCurrency: paymentDefaultCurrency,
    minAmount: Number.isFinite(paymentMinAmount) ? paymentMinAmount : 0,
    maxAmount: Number.isFinite(paymentMaxAmount) ? paymentMaxAmount : 0,
    maxAttemptsPerCall: Number.isFinite(paymentMaxAttemptsPerCall)
      ? Math.max(1, Math.floor(paymentMaxAttemptsPerCall))
      : 3,
    retryCooldownMs: Number.isFinite(paymentRetryCooldownMs)
      ? Math.max(0, Math.floor(paymentRetryCooldownMs))
      : 20000,
    webhookIdempotencyTtlMs: Number.isFinite(paymentWebhookIdempotencyTtlMs)
      ? paymentWebhookIdempotencyTtlMs
      : 300000,
    reconcile: {
      enabled: paymentReconcileEnabled,
      intervalMs: Number.isFinite(paymentReconcileIntervalMs)
        ? Math.max(15000, Math.floor(paymentReconcileIntervalMs))
        : 120000,
      staleSeconds: Number.isFinite(paymentReconcileStaleSeconds)
        ? Math.max(60, Math.floor(paymentReconcileStaleSeconds))
        : 240,
      batchSize: Number.isFinite(paymentReconcileBatchSize)
        ? Math.max(1, Math.min(100, Math.floor(paymentReconcileBatchSize)))
        : 20,
    },
    smsFallback: {
      enabled: paymentSmsFallbackEnabled,
      urlTemplate: String(paymentSmsFallbackUrlTemplate || "").trim(),
      messageTemplate: String(paymentSmsFallbackMessageTemplate || "")
        .trim()
        .slice(0, 240),
      ttlSeconds: Number.isFinite(paymentSmsFallbackTtlSeconds)
        ? Math.max(60, Math.min(86400, Math.floor(paymentSmsFallbackTtlSeconds)))
        : 900,
      secret: String(paymentSmsFallbackSecret || "").trim(),
      maxPerCall: Number.isFinite(paymentSmsFallbackMaxPerCall)
        ? Math.max(1, Math.min(5, Math.floor(paymentSmsFallbackMaxPerCall)))
        : 1,
    },
  },
  webhook: {
    retryBaseMs: webhookRetryBaseMs,
    retryMaxMs: webhookRetryMaxMs,
    retryMaxAttempts: webhookRetryMaxAttempts,
    telegramRequestTimeoutMs: Number.isFinite(webhookTelegramTimeoutMs)
      ? webhookTelegramTimeoutMs
      : 15000,
  },
};
