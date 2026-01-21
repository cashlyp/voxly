require('dotenv').config();
const fs = require('fs');
const path = require('path');

const isProduction = process.env.NODE_ENV === 'production';

function readEnv(name) {
  const value = process.env[name];
  if (typeof value === 'string' && value.length > 0) {
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
      console.warn(`Environment variable "${name}" is missing. Using fallback value in development.`);
    }
    return fallback;
  }
  const message = `Missing required environment variable "${name}".`;
  if (isProduction) {
    throw new Error(message);
  }
  console.warn(`${message} Continuing because NODE_ENV !== 'production'.`);
  return '';
}

function normalizeHostname(value) {
  if (!value) return '';
  const trimmed = String(value).trim();
  if (!trimmed) return '';
  try {
    if (trimmed.includes('://')) {
      const parsed = new URL(trimmed);
      return parsed.host;
    }
  } catch {
    // fall through to basic cleanup
  }
  return trimmed.replace(/^https?:\/\//i, '').replace(/\/+$/, '');
}

const corsOriginsRaw = ensure('CORS_ORIGINS', process.env.WEB_APP_URL || '');
const corsOrigins = corsOriginsRaw
  .split(',')
  .map((origin) => origin.trim())
  .filter(Boolean);

const recordingEnabled = String(readEnv('RECORDING_ENABLED') || 'false').toLowerCase() === 'true';
const transferNumber = readEnv('TRANSFER_NUMBER');
const defaultSmsBusinessId = readEnv('DEFAULT_SMS_BUSINESS_ID') || null;
const deepgramModel = readEnv('DEEPGRAM_MODEL') || 'nova-2';
const twilioGatherFallback = String(readEnv('TWILIO_GATHER_FALLBACK') || 'true').toLowerCase() === 'true';
const twilioMachineDetection = readEnv('TWILIO_MACHINE_DETECTION') || 'Enable';
const twilioMachineDetectionTimeoutRaw = readEnv('TWILIO_MACHINE_DETECTION_TIMEOUT');
const twilioMachineDetectionTimeout = Number.isFinite(Number(twilioMachineDetectionTimeoutRaw))
  ? Number(twilioMachineDetectionTimeoutRaw)
  : undefined;
const twilioTtsMaxWaitMs = Number(readEnv('TWILIO_TTS_MAX_WAIT_MS') || '1200');
const twilioWebhookValidationRaw = (readEnv('TWILIO_WEBHOOK_VALIDATION') || (isProduction ? 'strict' : 'warn')).toLowerCase();
const twilioWebhookValidationModes = new Set(['strict', 'warn', 'off']);
const twilioWebhookValidation = twilioWebhookValidationModes.has(twilioWebhookValidationRaw)
  ? twilioWebhookValidationRaw
  : (isProduction ? 'strict' : 'warn');

const callProvider = ensure('CALL_PROVIDER', 'twilio').toLowerCase();
const awsRegion = ensure('AWS_REGION', 'us-east-1');
const apiSecret = readEnv('API_SECRET');
const adminApiToken = apiSecret || readEnv('ADMIN_API_TOKEN');
const complianceModeRaw = (readEnv('CONFIG_COMPLIANCE_MODE') || 'safe').toLowerCase();
const allowedComplianceModes = new Set(['safe', 'dev_insecure']);
const complianceMode = allowedComplianceModes.has(complianceModeRaw) ? complianceModeRaw : 'safe';
if (!allowedComplianceModes.has(complianceModeRaw) && !isProduction) {
  console.warn(`Invalid CONFIG_COMPLIANCE_MODE "${complianceModeRaw}". Falling back to "safe".`);
}
const dtmfEncryptionKey = readEnv('DTMF_ENCRYPTION_KEY');
const apiHmacSecret = apiSecret || readEnv('API_HMAC_SECRET');
const apiHmacMaxSkewMs = Number(readEnv('API_HMAC_MAX_SKEW_MS') || '300000');
if (!apiHmacSecret) {
  const message = 'Missing required environment variable "API_SECRET" (or legacy API_HMAC_SECRET).';
  if (isProduction) {
    throw new Error(message);
  }
  console.warn(`${message} HMAC auth will be disabled.`);
}
const streamAuthSecret = readEnv('STREAM_AUTH_SECRET') || apiHmacSecret;
const streamAuthMaxSkewMs = Number(readEnv('STREAM_AUTH_MAX_SKEW_MS') || apiHmacMaxSkewMs || '300000');

function parseJsonObject(rawValue, label) {
  if (!rawValue) return {};
  try {
    const parsed = JSON.parse(rawValue);
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
      throw new Error('JSON must be an object');
    }
    return parsed;
  } catch (error) {
    const tag = label ? ` (${label})` : '';
    console.warn(`Unable to parse JSON config${tag}: ${error.message}`);
    return {};
  }
}

const inboundDefaultPrompt = readEnv('INBOUND_PROMPT');
const inboundDefaultFirstMessage = readEnv('INBOUND_FIRST_MESSAGE');
const inboundRoutes = parseJsonObject(readEnv('INBOUND_NUMBER_ROUTES'), 'INBOUND_NUMBER_ROUTES');
const inboundPreConnectMessage = readEnv('INBOUND_PRECONNECT_MESSAGE');
const inboundPreConnectPauseSeconds = Number(readEnv('INBOUND_PRECONNECT_PAUSE_S') || '1');
const inboundFirstMediaTimeoutMs = Number(readEnv('INBOUND_STREAM_FIRST_MEDIA_TIMEOUT_MS') || '8000');
const webhookRetryBaseMs = Number(readEnv('WEBHOOK_RETRY_BASE_MS') || '5000');
const webhookRetryMaxMs = Number(readEnv('WEBHOOK_RETRY_MAX_MS') || '60000');
const webhookRetryMaxAttempts = Number(readEnv('WEBHOOK_RETRY_MAX_ATTEMPTS') || '5');

function loadPrivateKey(rawValue) {
  if (!rawValue) {
    return undefined;
  }

  const normalized = rawValue.replace(/\\n/g, '\n');
  if (normalized.includes('-----BEGIN')) {
    return normalized;
  }

  try {
    const filePath = path.isAbsolute(normalized)
      ? normalized
      : path.join(process.cwd(), normalized);
    return fs.readFileSync(filePath, 'utf8');
  } catch (error) {
    console.warn(`Unable to load Vonage private key from path "${normalized}": ${error.message}`);
    return undefined;
  }
}

const vonagePrivateKey = loadPrivateKey(readEnv('VONAGE_PRIVATE_KEY'));
const serverHostname = normalizeHostname(ensure('SERVER', ''));
const liveConsoleAudioTickMs = Number(readEnv('LIVE_CONSOLE_AUDIO_TICK_MS') || '160');
const liveConsoleEditDebounceMs = Number(readEnv('LIVE_CONSOLE_EDIT_DEBOUNCE_MS') || '700');
const liveConsoleUserLevelThreshold = Number(readEnv('LIVE_CONSOLE_USER_LEVEL_THRESHOLD') || '0.08');
const liveConsoleUserHoldMs = Number(readEnv('LIVE_CONSOLE_USER_HOLD_MS') || '450');
const liveConsoleCarrier = readEnv('LIVE_CONSOLE_CARRIER') || 'VOICEDNUT';
const liveConsoleNetworkLabel = readEnv('LIVE_CONSOLE_NETWORK_LABEL') || 'LTE';
const telegramAdminChatId = readEnv('TELEGRAM_ADMIN_CHAT_ID') || readEnv('ADMIN_TELEGRAM_ID');
const emailProvider = (readEnv('EMAIL_PROVIDER') || 'sendgrid').toLowerCase();
const emailDefaultFrom = readEnv('EMAIL_DEFAULT_FROM') || '';
const emailVerifiedDomains = (readEnv('EMAIL_VERIFIED_DOMAINS') || '')
  .split(',')
  .map((value) => value.trim().toLowerCase())
  .filter(Boolean);
const emailRateLimitProvider = Number(readEnv('EMAIL_RATE_LIMIT_PROVIDER_PER_MIN') || '120');
const emailRateLimitTenant = Number(readEnv('EMAIL_RATE_LIMIT_TENANT_PER_MIN') || '120');
const emailRateLimitDomain = Number(readEnv('EMAIL_RATE_LIMIT_DOMAIN_PER_MIN') || '120');
const emailQueueIntervalMs = Number(readEnv('EMAIL_QUEUE_INTERVAL_MS') || '5000');
const emailMaxRetries = Number(readEnv('EMAIL_MAX_RETRIES') || '5');
const emailUnsubscribeUrl = readEnv('EMAIL_UNSUBSCRIBE_URL') || (serverHostname ? `https://${serverHostname}/webhook/email-unsubscribe` : '');
const emailWarmupMaxPerDay = Number(readEnv('EMAIL_WARMUP_MAX_PER_DAY') || '0');
const emailDkimEnabled = String(readEnv('EMAIL_DKIM_ENABLED') || 'true').toLowerCase() === 'true';
const emailSpfEnabled = String(readEnv('EMAIL_SPF_ENABLED') || 'true').toLowerCase() === 'true';
const emailDmarcPolicy = readEnv('EMAIL_DMARC_POLICY') || 'none';
const sendgridApiKey = readEnv('SENDGRID_API_KEY');
const sendgridBaseUrl = readEnv('SENDGRID_BASE_URL');
const mailgunApiKey = readEnv('MAILGUN_API_KEY');
const mailgunDomain = readEnv('MAILGUN_DOMAIN');
const mailgunBaseUrl = readEnv('MAILGUN_BASE_URL');
const sesRegion = readEnv('SES_REGION') || awsRegion;
const sesAccessKeyId = readEnv('SES_ACCESS_KEY_ID') || readEnv('AWS_ACCESS_KEY_ID');
const sesSecretAccessKey = readEnv('SES_SECRET_ACCESS_KEY') || readEnv('AWS_SECRET_ACCESS_KEY');
const sesSessionToken = readEnv('SES_SESSION_TOKEN') || readEnv('AWS_SESSION_TOKEN');

module.exports = {
  platform: {
    provider: callProvider,
  },
  twilio: {
    accountSid: ensure('TWILIO_ACCOUNT_SID'),
    authToken: ensure('TWILIO_AUTH_TOKEN'),
    fromNumber: ensure('FROM_NUMBER'),
    transferNumber,
    gatherFallback: twilioGatherFallback,
    machineDetection: twilioMachineDetection,
    machineDetectionTimeout: twilioMachineDetectionTimeout,
    ttsMaxWaitMs: Number.isFinite(twilioTtsMaxWaitMs) ? twilioTtsMaxWaitMs : 1200,
    webhookValidation: twilioWebhookValidation
  },
  aws: {
    region: awsRegion,
    connect: {
      instanceId: ensure('AWS_CONNECT_INSTANCE_ID', ''),
      contactFlowId: ensure('AWS_CONNECT_CONTACT_FLOW_ID', ''),
      queueId: readEnv('AWS_CONNECT_QUEUE_ID'),
      sourcePhoneNumber: readEnv('AWS_CONNECT_SOURCE_PHONE_NUMBER'),
      transcriptsQueueUrl: readEnv('AWS_TRANSCRIPTS_QUEUE_URL'),
      eventBusName: readEnv('AWS_EVENT_BUS_NAME'),
    },
    polly: {
      voiceId: ensure('AWS_POLLY_VOICE_ID', 'Joanna'),
      outputBucket: readEnv('AWS_POLLY_OUTPUT_BUCKET'),
      outputPrefix: readEnv('AWS_POLLY_OUTPUT_PREFIX') || 'tts/',
    },
    s3: {
      mediaBucket: readEnv('AWS_MEDIA_BUCKET') || readEnv('AWS_POLLY_OUTPUT_BUCKET'),
    },
    pinpoint: {
      applicationId: readEnv('AWS_PINPOINT_APPLICATION_ID'),
      originationNumber: readEnv('AWS_PINPOINT_ORIGINATION_NUMBER') || readEnv('AWS_CONNECT_SOURCE_PHONE_NUMBER'),
      region: readEnv('AWS_PINPOINT_REGION') || awsRegion,
    },
    transcribe: {
      languageCode: ensure('AWS_TRANSCRIBE_LANGUAGE_CODE', 'en-US'),
      vocabularyFilterName: readEnv('AWS_TRANSCRIBE_VOCABULARY_FILTER_NAME'),
    },
  },
  vonage: {
    apiKey: readEnv('VONAGE_API_KEY'),
    apiSecret: readEnv('VONAGE_API_SECRET'),
    applicationId: readEnv('VONAGE_APPLICATION_ID'),
    privateKey: vonagePrivateKey,
    voice: {
      fromNumber: readEnv('VONAGE_VOICE_FROM_NUMBER'),
      answerUrl: readEnv('VONAGE_ANSWER_URL'),
      eventUrl: readEnv('VONAGE_EVENT_URL'),
    },
    sms: {
      fromNumber: readEnv('VONAGE_SMS_FROM_NUMBER'),
    },
  },
  telegram: {
    botToken: ensure('TELEGRAM_BOT_TOKEN', process.env.BOT_TOKEN),
    adminChatId: telegramAdminChatId
  },
  openRouter: {
    apiKey: ensure('OPENROUTER_API_KEY'),
    model: ensure('OPENROUTER_MODEL', 'meta-llama/llama-3.1-8b-instruct:free'),
    backupModel: readEnv('OPENROUTER_BACKUP_MODEL'),
    siteUrl: ensure('YOUR_SITE_URL', 'http://localhost:3000'),
    siteName: ensure('YOUR_SITE_NAME', 'Voice Call Bot'),
    maxTokens: Number(ensure('OPENROUTER_MAX_TOKENS', '160'))
  },
  deepgram: {
    apiKey: ensure('DEEPGRAM_API_KEY'),
    voiceModel: ensure('VOICE_MODEL', 'aura-asteria-en'),
    model: deepgramModel,
  },
  server: {
    port: Number(ensure('PORT', '3000')),
    hostname: serverHostname,
    corsOrigins,
    rateLimit: {
      windowMs: Number(ensure('RATE_LIMIT_WINDOW_MS', '60000')),
      max: Number(ensure('RATE_LIMIT_MAX', '300')),
    },
  },
  admin: {
    apiToken: adminApiToken,
  },
  compliance: {
    mode: complianceMode,
    encryptionKey: dtmfEncryptionKey,
    isSafe: complianceMode !== 'dev_insecure',
  },
  recording: {
    enabled: recordingEnabled,
  },
  liveConsole: {
    audioTickMs: Number.isFinite(liveConsoleAudioTickMs) ? liveConsoleAudioTickMs : 160,
    editDebounceMs: Number.isFinite(liveConsoleEditDebounceMs) ? liveConsoleEditDebounceMs : 700,
    userLevelThreshold: Number.isFinite(liveConsoleUserLevelThreshold) ? liveConsoleUserLevelThreshold : 0.08,
    userHoldMs: Number.isFinite(liveConsoleUserHoldMs) ? liveConsoleUserHoldMs : 450,
    carrier: liveConsoleCarrier,
    networkLabel: liveConsoleNetworkLabel
  },
  email: {
    provider: emailProvider,
    defaultFrom: emailDefaultFrom,
    verifiedDomains: emailVerifiedDomains,
    queueIntervalMs: Number.isFinite(emailQueueIntervalMs) ? emailQueueIntervalMs : 5000,
    maxRetries: Number.isFinite(emailMaxRetries) ? emailMaxRetries : 5,
    unsubscribeUrl: emailUnsubscribeUrl,
    rateLimits: {
      perProviderPerMinute: Number.isFinite(emailRateLimitProvider) ? emailRateLimitProvider : 120,
      perTenantPerMinute: Number.isFinite(emailRateLimitTenant) ? emailRateLimitTenant : 120,
      perDomainPerMinute: Number.isFinite(emailRateLimitDomain) ? emailRateLimitDomain : 120,
    },
    warmup: {
      enabled: emailWarmupMaxPerDay > 0,
      maxPerDay: emailWarmupMaxPerDay
    },
    deliverability: {
      dkimEnabled: emailDkimEnabled,
      spfEnabled: emailSpfEnabled,
      dmarcPolicy: emailDmarcPolicy
    },
    sendgrid: {
      apiKey: sendgridApiKey,
      baseUrl: sendgridBaseUrl
    },
    mailgun: {
      apiKey: mailgunApiKey,
      domain: mailgunDomain,
      baseUrl: mailgunBaseUrl
    },
    ses: {
      region: sesRegion,
      accessKeyId: sesAccessKeyId,
      secretAccessKey: sesSecretAccessKey,
      sessionToken: sesSessionToken
    }
  },
  smsDefaults: {
    businessId: defaultSmsBusinessId,
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
    firstMediaTimeoutMs: inboundFirstMediaTimeoutMs
  },
  webhook: {
    retryBaseMs: webhookRetryBaseMs,
    retryMaxMs: webhookRetryMaxMs,
    retryMaxAttempts: webhookRetryMaxAttempts
  }
};
