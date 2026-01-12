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

const callProvider = ensure('CALL_PROVIDER', 'twilio').toLowerCase();
const awsRegion = ensure('AWS_REGION', 'us-east-1');
const adminApiToken = readEnv('ADMIN_API_TOKEN');
const complianceModeRaw = (readEnv('CONFIG_COMPLIANCE_MODE') || 'safe').toLowerCase();
const allowedComplianceModes = new Set(['safe', 'dev_insecure']);
const complianceMode = allowedComplianceModes.has(complianceModeRaw) ? complianceModeRaw : 'safe';
if (!allowedComplianceModes.has(complianceModeRaw) && !isProduction) {
  console.warn(`Invalid CONFIG_COMPLIANCE_MODE "${complianceModeRaw}". Falling back to "safe".`);
}
const dtmfEncryptionKey = readEnv('DTMF_ENCRYPTION_KEY');
const apiHmacSecret = readEnv('API_HMAC_SECRET');
const apiHmacMaxSkewMs = Number(readEnv('API_HMAC_MAX_SKEW_MS') || '300000');
if (!apiHmacSecret) {
  const message = 'Missing required environment variable "API_HMAC_SECRET".';
  if (isProduction) {
    throw new Error(message);
  }
  console.warn(`${message} HMAC auth will be disabled.`);
}

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
  smsDefaults: {
    businessId: defaultSmsBusinessId,
  },
  apiAuth: {
    hmacSecret: apiHmacSecret,
    maxSkewMs: apiHmacMaxSkewMs,
  },
};
