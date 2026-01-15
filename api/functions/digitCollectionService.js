'use strict';

const crypto = require('crypto');

const DIGIT_WORD_MAP = {
  zero: '0',
  oh: '0',
  o: '0',
  one: '1',
  two: '2',
  three: '3',
  four: '4',
  five: '5',
  six: '6',
  seven: '7',
  eight: '8',
  nine: '9'
};

const SPOKEN_DIGIT_PATTERN = new RegExp(
  `\\b(?:${Object.keys(DIGIT_WORD_MAP).join('|')})(?:\\s+(?:${Object.keys(DIGIT_WORD_MAP).join('|')})){3,}\\b`,
  'gi'
);

const SAFE_TIMEOUT_MIN_S = 3;
const SAFE_TIMEOUT_MAX_S = 60;
const SAFE_RETRY_MAX = 5;
const MAX_DIGITS_BUFFER = 50;  // Prevent unbounded buffer growth
const DEFAULT_RISK_THRESHOLDS = {
  confirm: 0.55,
  dtmf_only: 0.7,
  route_agent: 0.9
};
const INTENT_PREDICT_MIN_SCORE = 0.8;
const SMS_FALLBACK_MIN_RETRIES = 2;
const DEFAULT_HEALTH_THRESHOLDS = {
  degraded: 30,
  overloaded: 60
};
const DEFAULT_CIRCUIT_BREAKER = {
  windowMs: 60000,
  minSamples: 8,
  errorRate: 0.3,
  cooldownMs: 60000
};

const SUPPORTED_DIGIT_PROFILES = new Set([
  'generic',
  'verification',
  'otp',
  'pin',
  'ssn',
  'dob',
  'routing_number',
  'account_number',
  'phone',
  'tax_id',
  'ein',
  'claim_number',
  'reservation_number',
  'ticket_number',
  'case_number',
  'account',
  'extension',
  'zip',
  'amount',
  'callback_confirm',
  'card_number',
  'cvv',
  'card_expiry'
]);

function createDigitCollectionService(options = {}) {
  const {
    db,
    webhookService,
    callConfigurations,
    config,
    twilioClient,
    VoiceResponse,
    getCurrentProvider,
    speakAndEndCall,
    clearSilenceTimer,
    queuePendingDigitAction,
    callEndMessages = {},
    closingMessage = 'Thank you for your time. Goodbye.',
    settings = {},
    logger = console,
    smsService = null,
    riskEvaluator = null,
    healthProvider = null
  } = options;

  const {
    otpLength = 6,
    otpMaxRetries = 3,
    otpDisplayMode = 'masked',
    defaultCollectDelayMs = 1200,
    fallbackToVoiceOnFailure = true,
    showRawDigitsLive = true,
    sendRawDigitsToUser = true,
    minDtmfGapMs = 200,
    riskThresholds = DEFAULT_RISK_THRESHOLDS,
    smsFallbackEnabled = true,
    smsFallbackMinRetries = SMS_FALLBACK_MIN_RETRIES,
    smsFallbackMessage = 'I have sent you a text message. Please reply with the digits to continue.',
    smsFallbackConfirmationMessage = 'Thanks, your reply was received.',
    smsFallbackFailureMessage = 'I could not verify the digits via SMS. Please try again later.',
    intentPredictor = null,
    healthThresholds = DEFAULT_HEALTH_THRESHOLDS,
    circuitBreaker = DEFAULT_CIRCUIT_BREAKER
  } = settings;

  const logDigitMetric = (event, meta = {}) => {
    const payload = { event, ...meta };
    try {
      if (logger && typeof logger.info === 'function') {
        logger.info(`[digits] ${event}`, payload);
      } else if (logger && typeof logger.log === 'function') {
        logger.log(`[digits] ${event}`, payload);
      } else {
        console.log(`[digits] ${event}`, payload);
      }
    } catch (_) {}
  };

  const REMOVED_DIGIT_PROFILES = new Set([
    'menu',
    'member_id',
    'survey',
    'policy_number',
    'invoice_number',
    'confirmation_code'
  ]);

  function normalizeProfileId(profile) {
    if (!profile) return null;
    let normalized = String(profile || '').toLowerCase().trim();
    normalized = normalized.replace(/[\s-]+/g, '_');
    if (normalized === 'bank_account') normalized = 'account_number';
    if (normalized === 'routing') normalized = 'routing_number';
    if (normalized === 'account_num') normalized = 'account_number';
    if (normalized === 'routing_num') normalized = 'routing_number';
    if (REMOVED_DIGIT_PROFILES.has(normalized)) return 'generic';
    return normalized;
  }

  function isSupportedProfile(profile) {
    const normalized = normalizeProfileId(profile);
    if (!normalized) return false;
    return SUPPORTED_DIGIT_PROFILES.has(normalized);
  }

  function maskDigitsForPreview(digits = '') {
    if (showRawDigitsLive) return digits || '';
    const len = String(digits || '').length;
    if (!len) return '••';
    const masked = '•'.repeat(Math.max(2, Math.min(6, len)));
    return len > 6 ? `${masked}…` : masked;
  }

  function labelForProfile(profile = 'generic') {
    const normalizedProfile = normalizeProfileId(profile) || 'generic';
    const map = {
      verification: 'OTP',
      otp: 'OTP',
      pin: 'PIN',
      ssn: 'SSN',
      dob: 'DOB',
      routing_number: 'Routing',
      account_number: 'Account #',
      phone: 'Phone',
      tax_id: 'Tax ID',
      ein: 'EIN',
      claim_number: 'Claim',
      reservation_number: 'Reservation',
      ticket_number: 'Ticket',
      case_number: 'Case',
      account: 'Account',
      zip: 'ZIP',
      extension: 'Ext',
      amount: 'Amount',
      callback_confirm: 'Callback',
      card_number: 'Card',
      cvv: 'CVV',
      card_expiry: 'Expiry',
      generic: 'Digits'
    };
    return map[normalizedProfile] || normalizedProfile || 'Digits';
  }

  function titleCaseLabel(value = '') {
    const text = String(value || '').trim();
    if (!text) return text;
    return text
      .split(/\s+/)
      .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
      .join(' ');
  }

  function formatPlanStepLabel(expectation = {}) {
    const stepIndex = expectation?.plan_step_index;
    const totalSteps = expectation?.plan_total_steps;
    if (!Number.isFinite(stepIndex) || !Number.isFinite(totalSteps) || totalSteps <= 0) return '';
    const label = buildExpectedLabel(expectation);
    const stepLabel = titleCaseLabel(label);
    return `Step ${stepIndex}/${totalSteps}: ${stepLabel}`;
  }

  function labelForClosing(profile = 'generic') {
    const normalizedProfile = normalizeProfileId(profile) || 'generic';
    const map = {
      verification: 'one-time password',
      otp: 'one-time password',
      pin: 'PIN',
      reservation_number: 'reservation number',
      ticket_number: 'ticket number',
      case_number: 'case number',
      claim_number: 'claim number',
      extension: 'extension',
      account_number: 'account number',
      account: 'account number',
      routing_number: 'routing number',
      ssn: 'social security number',
      dob: 'date of birth',
      zip: 'ZIP code',
      phone: 'phone number',
      tax_id: 'tax ID',
      ein: 'employer ID',
      card_number: 'card number',
      cvv: 'card security code',
      card_expiry: 'card expiry',
      amount: 'amount'
    };
    return map[normalizedProfile] || null;
  }

  function buildClosingMessage(profile) {
    const label = labelForClosing(profile);
    if (!label) {
      return 'Thank you—your input has been received. Your request is complete. Goodbye.';
    }
    return `Thank you—your ${label} has been received and verified. Your request is complete. Goodbye.`;
  }

  function estimateSpeechDurationMs(text = '') {
    const words = String(text || '')
      .trim()
      .split(/\s+/)
      .filter(Boolean).length;
    if (!words) return 0;
    const wordsPerMinute = 150;
    return Math.ceil((words / wordsPerMinute) * 60000);
  }

  function buildExpectedLabel(expectation = {}) {
    const min = expectation.min_digits || 1;
    const max = expectation.max_digits || min;
    const digitLabel = min === max ? `${min}-digit` : `${min}-${max} digit`;
    const profile = normalizeProfileId(expectation.profile) || 'generic';
    switch (profile) {
      case 'extension':
        return 'extension';
      case 'zip':
        return 'ZIP code';
      case 'account':
        return 'account number';
      case 'cvv':
        return 'security code';
      case 'card_number':
        return 'card number';
      case 'card_expiry':
        return 'expiry date';
      case 'amount':
        return 'amount';
      case 'account_number':
        return 'account number';
      case 'callback_confirm':
        return 'phone number';
      case 'ssn':
        return 'social security number';
      case 'dob':
        return 'date of birth';
      case 'routing_number':
        return 'routing number';
      case 'phone':
        return 'phone number';
      case 'tax_id':
        return 'tax ID';
      case 'ein':
        return 'employer ID';
      case 'claim_number':
        return 'claim number';
      case 'reservation_number':
        return 'reservation number';
      case 'ticket_number':
        return 'ticket number';
      case 'case_number':
        return 'case number';
      case 'verification':
      case 'otp':
        return `${digitLabel} code`;
      default:
        return `${digitLabel} code`;
    }
  }

  function buildDefaultReprompts(expectation = {}) {
    const label = buildExpectedLabel(expectation);
    return {
      invalid: [
        `That did not match. Please enter the ${label} now.`,
        `Please enter the ${label} now.`,
        `Last try: enter the ${label} now.`
      ],
      timeout: [
        `I did not receive any input. Please enter the ${label} now.`,
        `Please enter the ${label} now.`,
        `Last try: enter the ${label} now.`
      ],
      failure: `We could not verify the ${label}. Thank you for your time. Goodbye.`,
      timeout_failure: `No input received for the ${label}. Thank you for your time. Goodbye.`
    };
  }

  function normalizeRepromptValue(value) {
    if (Array.isArray(value)) {
      const trimmed = value.map((item) => String(item || '').trim()).filter(Boolean);
      return trimmed.length ? trimmed : '';
    }
    if (typeof value === 'string') {
      const trimmed = value.trim();
      return trimmed || '';
    }
    return '';
  }

  function chooseReprompt(expectation = {}, kind = 'invalid', attempt = 1) {
    const key = kind === 'timeout'
      ? expectation.reprompt_timeout
      : kind === 'incomplete'
        ? expectation.reprompt_incomplete
        : expectation.reprompt_invalid;
    if (Array.isArray(key) && key.length) {
      const idx = Math.max(0, Math.min(key.length - 1, (attempt || 1) - 1));
      return key[idx];
    }
    if (typeof key === 'string' && key.trim()) return key.trim();
    return '';
  }

  const OTP_REGEX = /\b\d{4,8}\b/g;

  const digitTimeouts = new Map();
  const digitFallbackStates = new Map();
  const digitCollectionPlans = new Map();
  const lastDtmfTimestamps = new Map();
  const pendingDigits = new Map();
  const recentAccepted = new Map();
  const callerAffect = new Map();
  const sessionState = new Map();
  const intentHistory = new Map();
  const riskSignals = new Map();
  const smsSessions = new Map();
  const smsSessionsByPhone = new Map();
  const breakerState = {
    open: false,
    opened_at: 0,
    window_start: Date.now(),
    total: 0,
    errors: 0
  };

  const emitAuditEvent = async (callSid, eventType, payload = {}) => {
    if (!callSid || !eventType) return;
    if (!db?.addCallDigitEvent) return;
    const metadata = {
      event_type: eventType,
      ...payload,
      digits_stored: false,
      recorded_at: new Date().toISOString()
    };
    try {
      await db.addCallDigitEvent({
        call_sid: callSid,
        source: payload.source || 'system',
        profile: payload.profile || 'generic',
        digits: null,
        len: payload.len || null,
        accepted: payload.accepted === true,
        reason: payload.reason || null,
        metadata
      });
    } catch (err) {
      logDigitMetric('audit_log_failed', { callSid, event: eventType, error: err.message });
    }
  };

  const buildCollectionFingerprint = (collection, expectation) => {
    if (!collection?.digits) return null;
    const hash = crypto.createHash('sha256').update(String(collection.digits)).digest('hex');
    const stepKey = expectation?.plan_step_index || 'single';
    return `${collection.profile || 'generic'}|${collection.len || 0}|${stepKey}|${hash}`;
  };

  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

  const getCallerAffect = (callSid) => {
    const state = callerAffect.get(callSid) || { attempts: 0, impatience: 0, started_at: Date.now() };
    const patience = state.impatience >= 2 || state.attempts >= 2 ? 'low' : 'high';
    return { ...state, patience };
  };

  const recordCallerAffect = (callSid, reason = '') => {
    const state = callerAffect.get(callSid) || { attempts: 0, impatience: 0, started_at: Date.now() };
    state.attempts += 1;
    if (['too_fast', 'timeout', 'spam_pattern', 'low_confidence'].includes(reason)) {
      state.impatience += 1;
    }
    callerAffect.set(callSid, state);
    return getCallerAffect(callSid);
  };

  const getSystemHealth = (callSid = null) => {
    let health = null;
    if (typeof healthProvider === 'function') {
      try {
        health = healthProvider(callSid);
      } catch (err) {
        logDigitMetric('health_provider_error', { callSid, error: err.message });
      }
    }
    const load = Number(health?.load ?? callConfigurations.size ?? 0);
    const thresholds = { ...DEFAULT_HEALTH_THRESHOLDS, ...(healthThresholds || {}) };
    const status = health?.status
      || (load >= thresholds.overloaded ? 'overloaded' : load >= thresholds.degraded ? 'degraded' : 'healthy');
    return {
      status,
      load,
      meta: health?.meta || null
    };
  };

  const applyHealthPolicy = (callSid, expectation) => {
    if (!expectation) return expectation;
    const health = getSystemHealth(callSid);
    if (!health || health.status === 'healthy') return expectation;
    const next = { ...expectation };
    if (health.status === 'overloaded') {
      next.max_retries = Math.min(next.max_retries || 0, 1);
      next.timeout_s = Math.min(next.timeout_s || SAFE_TIMEOUT_MAX_S, 10);
      if (!next.confirmation_locked) {
        next.speak_confirmation = false;
        next.confirmation_style = 'none';
      }
      next.prompt = `Please enter the ${buildExpectedLabel(next)} now.`;
    } else if (health.status === 'degraded') {
      next.max_retries = Math.min(next.max_retries || 0, 2);
      next.timeout_s = Math.min(next.timeout_s || SAFE_TIMEOUT_MAX_S, 15);
      if (!next.confirmation_locked && next.speak_confirmation) {
        next.speak_confirmation = false;
      }
    }
    logDigitMetric('health_policy_applied', {
      callSid,
      status: health.status,
      max_retries: next.max_retries,
      timeout_s: next.timeout_s
    });
    return next;
  };

  const resetCircuitWindow = () => {
    breakerState.window_start = Date.now();
    breakerState.total = 0;
    breakerState.errors = 0;
  };

  const recordCircuitAttempt = () => {
    const now = Date.now();
    const windowMs = Number(circuitBreaker?.windowMs || DEFAULT_CIRCUIT_BREAKER.windowMs);
    if (now - breakerState.window_start > windowMs) {
      resetCircuitWindow();
    }
    breakerState.total += 1;
  };

  const recordCircuitError = () => {
    breakerState.errors += 1;
    const minSamples = Number(circuitBreaker?.minSamples || DEFAULT_CIRCUIT_BREAKER.minSamples);
    const errorRate = Number(circuitBreaker?.errorRate || DEFAULT_CIRCUIT_BREAKER.errorRate);
    if (!breakerState.open && breakerState.total >= minSamples) {
      const rate = breakerState.errors / Math.max(1, breakerState.total);
      if (rate >= errorRate) {
        breakerState.open = true;
        breakerState.opened_at = Date.now();
        logDigitMetric('circuit_opened', { error_rate: rate.toFixed(2), total: breakerState.total });
      }
    }
  };

  const isCircuitOpen = () => {
    if (!breakerState.open) return false;
    const cooldownMs = Number(circuitBreaker?.cooldownMs || DEFAULT_CIRCUIT_BREAKER.cooldownMs);
    if (Date.now() - breakerState.opened_at >= cooldownMs) {
      breakerState.open = false;
      breakerState.opened_at = 0;
      resetCircuitWindow();
      logDigitMetric('circuit_closed', { recovered_at: Date.now() });
      return false;
    }
    return true;
  };

  const formatDigitsForSpeech = (digits = '', maxDigits = 6) => {
    const value = String(digits || '').replace(/\D/g, '').slice(0, maxDigits);
    if (!value) return '';
    return value.split('').join('-');
  };

  const isSensitiveProfile = (profile = '') => {
    const normalized = normalizeProfileId(profile);
    return new Set([
      'verification',
      'otp',
      'pin',
      'ssn',
      'cvv',
      'card_number',
      'routing_number',
      'account_number',
      'tax_id',
      'ein',
      'dob'
    ]).has(normalized);
  };

  const updateSessionState = (callSid, updates = {}) => {
    if (!callSid) return null;
    const existing = sessionState.get(callSid) || {
      partialDigits: '',
      lastCandidate: null,
      lastUpdatedAt: Date.now()
    };
    const next = {
      ...existing,
      ...updates,
      lastUpdatedAt: Date.now()
    };
    sessionState.set(callSid, next);
    return next;
  };

  const getSessionState = (callSid) => sessionState.get(callSid) || null;

  const normalizeRiskScore = (value) => {
    const score = Number(value);
    if (!Number.isFinite(score)) return null;
    return Math.max(0, Math.min(1, score));
  };

  const resolveRiskSignal = (callSid, callConfig = {}) => {
    const cached = riskSignals.get(callSid);
    if (cached && Date.now() - cached.updated_at < 30000) {
      return cached;
    }
    let score = null;
    let reason = null;
    try {
      if (typeof riskEvaluator === 'function') {
        const result = riskEvaluator(callSid, callConfig) || {};
        score = normalizeRiskScore(result.score ?? result.riskScore ?? result.value);
        reason = result.reason || result.source || null;
      }
    } catch (err) {
      logDigitMetric('risk_evaluator_error', { callSid, error: err.message });
    }
    if (score === null) {
      score = normalizeRiskScore(callConfig.voice_biometric_risk_score ?? callConfig.risk_score);
    }
    if (score === null) {
      return null;
    }
    const signal = {
      score,
      reason: reason || callConfig.voice_biometric_risk_reason || callConfig.risk_reason || null,
      updated_at: Date.now()
    };
    riskSignals.set(callSid, signal);
    logDigitMetric('risk_signal', { callSid, score, reason: signal.reason });
    return signal;
  };

  const applyRiskPolicy = (callSid, expectation) => {
    if (!expectation) return expectation;
    const callConfig = callConfigurations.get(callSid) || {};
    const signal = resolveRiskSignal(callSid, callConfig);
    if (!signal) return expectation;
    const thresholds = { ...DEFAULT_RISK_THRESHOLDS, ...(riskThresholds || {}) };
    let applied = false;
    if (signal.score >= thresholds.confirm) {
      expectation.speak_confirmation = true;
      if (!expectation.confirmation_style || expectation.confirmation_style === 'none') {
        expectation.confirmation_style = isSensitiveProfile(expectation.profile) ? 'none' : 'last4';
      }
      applied = true;
    }
    if (signal.score >= thresholds.dtmf_only) {
      expectation.allow_spoken_fallback = false;
      applied = true;
    }
    if (signal.score >= thresholds.route_agent) {
      expectation.risk_action = 'route_to_agent';
      expectation.risk_score = signal.score;
      expectation.risk_reason = signal.reason || 'risk_threshold';
      applied = true;
    }
    if (applied) {
      logDigitMetric('risk_policy_applied', {
        callSid,
        score: signal.score,
        action: expectation.risk_action || null,
        confirmation: expectation.speak_confirmation === true,
        dtmf_only: expectation.allow_spoken_fallback === false
      });
    }
    return expectation;
  };

  const resolveCallPhone = async (callSid) => {
    const callConfig = callConfigurations.get(callSid);
    const direct = callConfig?.phone_number || callConfig?.number || callConfig?.to;
    if (direct) return String(direct).trim();
    if (db?.getCall) {
      try {
        const callRecord = await db.getCall(callSid);
        if (callRecord?.phone_number) {
          return String(callRecord.phone_number).trim();
        }
      } catch (_) {}
    }
    return null;
  };

  const buildSmsPrompt = (expectation, correlationId = '') => {
    const label = buildExpectedLabel(expectation);
    const suffix = correlationId ? ` Ref: ${correlationId}` : '';
    return `Reply with your ${label} using digits only.${suffix}`;
  };

  const buildSmsStepPrompt = (expectation) => {
    const label = buildExpectedLabel(expectation);
    const stepIndex = expectation?.plan_step_index;
    const totalSteps = expectation?.plan_total_steps;
    const stepPrefix = Number.isFinite(stepIndex) && Number.isFinite(totalSteps)
      ? `Step ${stepIndex} of ${totalSteps}. `
      : '';
    return `${stepPrefix}Reply with your ${label} using digits only.`;
  };

  const createSmsSession = async (callSid, expectation, reason = 'fallback') => {
    if (!smsService || !smsFallbackEnabled) return null;
    const phone = await resolveCallPhone(callSid);
    if (!phone) {
      logDigitMetric('sms_fallback_no_phone', { callSid });
      return null;
    }
    const correlationId = `SMS-${callSid.slice(-6)}-${Math.random().toString(36).slice(2, 6).toUpperCase()}`;
    const prompt = buildSmsPrompt(expectation, correlationId);
    try {
      await smsService.sendSMS(phone, prompt, null, { idempotencyKey: `${callSid}:${correlationId}` });
      const session = {
        callSid,
        phone,
        correlationId,
        expectation: { ...expectation },
        created_at: Date.now(),
        reason,
        attempts: 0,
        active: true
      };
      smsSessions.set(callSid, session);
      smsSessionsByPhone.set(phone, session);
      const plan = digitCollectionPlans.get(callSid);
      if (plan?.active) {
        plan.channel = 'sms';
      }
      logDigitMetric('sms_fallback_sent', { callSid, phone, correlationId, profile: expectation.profile });
      await db.updateCallState(callSid, 'digit_collection_sms_sent', {
        phone,
        correlation_id: correlationId,
        reason
      }).catch(() => {});
      return session;
    } catch (err) {
      logDigitMetric('sms_fallback_failed', { callSid, error: err.message });
      return null;
    }
  };

  const shouldUseSmsFallback = (expectation, collection) => {
    if (!smsService || !smsFallbackEnabled || !expectation) return false;
    if (expectation.sms_fallback_used) return false;
    const retries = collection?.retries || 0;
    if (retries < smsFallbackMinRetries) return false;
    const reason = collection?.reason || '';
    return ['low_confidence', 'timeout', 'spam_pattern', 'too_fast'].includes(reason);
  };

  const clearSmsSession = (callSid) => {
    const session = smsSessions.get(callSid);
    if (session) {
      smsSessions.delete(callSid);
      smsSessionsByPhone.delete(session.phone);
    }
  };

  const getSmsSessionByPhone = (phone) => smsSessionsByPhone.get(String(phone || '').trim()) || null;

  const parseDigitsFromText = (text = '') => String(text || '').replace(/\D/g, '');

  const buildSmsReplyForResult = (collection) => {
    if (!collection) return '';
    if (collection.accepted) {
      return smsFallbackConfirmationMessage;
    }
    if (collection.fallback) {
      return smsFallbackFailureMessage;
    }
    if (collection.reason === 'incomplete') {
      return 'I only received part of the digits. Please reply with the full number.';
    }
    return 'Please reply with the digits only.';
  };

  const handleCircuitFallback = async (callSid, expectation, allowCallEnd, deferCallEnd, source = 'system') => {
    const profile = expectation?.profile || 'generic';
    await emitAuditEvent(callSid, 'DigitCaptureAborted', {
      profile,
      source,
      reason: 'circuit_open'
    });
    if (expectation?.allow_sms_fallback && smsFallbackEnabled) {
      const session = await createSmsSession(callSid, expectation, 'circuit_open');
      if (session) {
        expectation.sms_fallback_used = true;
        expectation.channel = 'sms';
        digitCollectionManager.expectations.set(callSid, expectation);
        if (allowCallEnd) {
          if (!deferCallEnd) {
            await speakAndEndCall(callSid, smsFallbackMessage, 'digits_sms_fallback');
          }
          return true;
        }
        return true;
      }
    }
    if (allowCallEnd) {
      if (deferCallEnd) {
        if (queuePendingDigitAction) {
          queuePendingDigitAction(callSid, { type: 'end', text: callEndMessages.failure || 'We could not verify the digits. Goodbye.', reason: 'digit_service_unavailable' });
        }
        return true;
      }
      await speakAndEndCall(callSid, callEndMessages.failure || 'We could not verify the digits. Goodbye.', 'digit_service_unavailable');
      return true;
    }
    return false;
  };

  const routeToAgentOnRisk = async (callSid, expectation, collection, allowCallEnd, deferCallEnd) => {
    const score = expectation?.risk_score ?? null;
    const reason = expectation?.risk_reason || 'risk_threshold';
    const message = callEndMessages.risk
      || 'For security reasons, we need to route this request to an agent. Goodbye.';
    logDigitMetric('risk_route_agent', { callSid, score, reason });
    void emitAuditEvent(callSid, 'RoutedToAgent', {
      profile: expectation?.profile || collection?.profile || 'generic',
      source: collection?.source || 'system',
      reason,
      confidence: collection?.confidence || null,
      signals: collection?.confidence_signals || null
    });
    await db.updateCallState(callSid, 'digit_risk_escalation', {
      score,
      reason,
      profile: expectation?.profile || collection?.profile || null
    }).catch(() => {});
    if (allowCallEnd) {
      if (deferCallEnd) {
        if (queuePendingDigitAction) {
          queuePendingDigitAction(callSid, { type: 'end', text: message, reason: 'risk_escalation' });
        }
        return true;
      }
      await speakAndEndCall(callSid, message, 'risk_escalation');
      return true;
    }
    if (queuePendingDigitAction) {
      queuePendingDigitAction(callSid, { type: 'end', text: message, reason: 'risk_escalation' });
    }
    return true;
  };

  const recordIntentHistory = (callSid, profile) => {
    if (!callSid || !profile) return;
    const entry = intentHistory.get(callSid) || { counts: {}, lastProfile: null };
    entry.counts[profile] = (entry.counts[profile] || 0) + 1;
    entry.lastProfile = profile;
    intentHistory.set(callSid, entry);
  };

  const estimateIntentCandidates = (callSid, callConfig = {}) => {
    const candidates = new Map();
    const pushScore = (profile, score, source) => {
      if (!profile || !isSupportedProfile(profile)) return;
      const existing = candidates.get(profile) || { profile, score: 0, sources: [] };
      existing.score += score;
      existing.sources.push(source);
      candidates.set(profile, existing);
    };
    const textSources = [
      { text: callConfig.last_agent_prompt, weight: 0.6, label: 'last_agent_prompt' },
      { text: callConfig.last_bot_prompt, weight: 0.6, label: 'last_bot_prompt' },
      { text: callConfig.workflow_state, weight: 0.5, label: 'workflow_state' },
      { text: callConfig.prompt, weight: 0.4, label: 'prompt' },
      { text: callConfig.first_message, weight: 0.3, label: 'first_message' }
    ].filter((entry) => entry.text);
    const keywordRules = [
      { profile: 'otp', regex: /\b(otp|one[-\s]?time|verification code|security code|code)\b/i, base: 0.7 },
      { profile: 'pin', regex: /\b(pin|passcode)\b/i, base: 0.7 },
      { profile: 'routing_number', regex: /\brouting\b/i, base: 0.8 },
      { profile: 'account_number', regex: /\baccount number\b/i, base: 0.7 },
      { profile: 'card_number', regex: /\b(card number|credit card|debit card)\b/i, base: 0.7 },
      { profile: 'cvv', regex: /\b(cvv|cvc|security code)\b/i, base: 0.7 },
      { profile: 'card_expiry', regex: /\b(expiry|expiration|exp date|mm\/yy)\b/i, base: 0.6 },
      { profile: 'ssn', regex: /\b(ssn|social security)\b/i, base: 0.7 },
      { profile: 'dob', regex: /\b(date of birth|dob)\b/i, base: 0.6 },
      { profile: 'zip', regex: /\b(zip|postal)\b/i, base: 0.5 },
      { profile: 'phone', regex: /\b(phone number|phone)\b/i, base: 0.5 }
    ];
    for (const source of textSources) {
      const text = String(source.text || '');
      for (const rule of keywordRules) {
        if (rule.regex.test(text)) {
          pushScore(rule.profile, rule.base * source.weight, source.label);
        }
      }
    }
    if (callConfig.template_policy?.default_profile) {
      pushScore(callConfig.template_policy.default_profile, 0.9, 'template_policy');
    }
    const history = intentHistory.get(callSid);
    if (history?.lastProfile) {
      pushScore(history.lastProfile, 0.2, 'history');
    }
    let list = Array.from(candidates.values());
    list = list.map((entry) => ({
      ...entry,
      score: Math.min(1, entry.score)
    })).sort((a, b) => b.score - a.score);
    return list.slice(0, 3);
  };

  const buildDigitCandidate = (collection, expectation, source = 'dtmf') => {
    const reasonCodes = [];
    const dtmfClarity = source === 'dtmf'
      ? (collection.reason === 'too_fast' ? 0.2 : 0.9)
      : 0.6;
    const asrConfidence = source === 'spoken'
      ? (Number.isFinite(collection.asr_confidence) ? collection.asr_confidence : 0.55)
      : 1;
    const consistency = (() => {
      const exp = expectation || {};
      if (Array.isArray(exp.collected) && exp.collected.length >= 2) {
        const last = exp.collected[exp.collected.length - 1];
        const prev = exp.collected[exp.collected.length - 2];
        return last === prev ? 0.9 : 0.5;
      }
      return 0.7;
    })();
    const contextFit = (() => {
      if (collection.reason === 'spam_pattern') return 0.1;
      if (collection.reason === 'too_long') return 0.2;
      if (collection.reason === 'too_short' || collection.reason === 'incomplete') return 0.4;
      if (collection.reason && collection.reason.startsWith('invalid_')) return 0.2;
      return collection.accepted ? 0.9 : 0.6;
    })();
    const confidence = Math.max(0, Math.min(1,
      (dtmfClarity * 0.4) + (asrConfidence * 0.3) + (consistency * 0.2) + (contextFit * 0.1)
    ));
    if (dtmfClarity < 0.5) reasonCodes.push('low_dtmf_clarity');
    if (asrConfidence < 0.5) reasonCodes.push('low_asr_confidence');
    if (consistency < 0.6) reasonCodes.push('low_consistency');
    if (contextFit < 0.6) reasonCodes.push('context_mismatch');
    return {
      confidence,
      signals: {
        dtmfClarity,
        asrConfidence,
        consistency,
        contextFit
      },
      reasonCodes
    };
  };

  const buildRetryPolicy = ({ reason, attempt, source, expectation, affect, session, health }) => {
    const label = buildExpectedLabel(expectation || {});
    const patience = affect?.patience || 'high';
    const partial = session?.partialDigits || '';
    const allowPartialReplay = partial && !isSensitiveProfile(expectation?.profile);
    const partialSpoken = allowPartialReplay ? formatDigitsForSpeech(partial) : '';
    const status = health?.status || 'healthy';
    if (status === 'overloaded') {
      return {
        delayMs: 0,
        prompt: `Please enter the ${label} now.`
      };
    }
    switch (reason) {
      case 'too_fast':
        return {
          delayMs: Math.min(500, 250 + (attempt * 50)),
          prompt: patience === 'low'
            ? `Let's try once more—enter the ${label} slowly.`
            : `No rush—enter the ${label} slowly.`
        };
      case 'timeout':
        return {
          delayMs: 0,
          prompt: `I did not receive any input. Please enter the ${label} now.`
        };
      case 'spam_pattern':
        return {
          delayMs: 0,
          prompt: `That pattern does not look right. Please enter the ${label} now.`,
          forceDtmfOnly: true
        };
      case 'low_confidence':
        return {
          delayMs: 0,
          prompt: `I may have missed that. Please enter the ${label} again.`
        };
      case 'too_short':
      case 'incomplete': {
        const expectedLen = expectation?.max_digits || expectation?.min_digits || '';
        const lenText = expectedLen ? ` all ${expectedLen} digits` : ` the ${label}`;
        if (allowPartialReplay && partialSpoken) {
          const intro = patience === 'low'
            ? `I have ${partialSpoken}.`
            : `I heard ${partialSpoken}.`;
          return {
            delayMs: 0,
            prompt: `${intro} If that is correct, enter the remaining digits. Otherwise, enter${lenText} now.`
          };
        }
        return {
          delayMs: 0,
          prompt: `I only got part of it. Please enter${lenText} now.`
        };
      }
      default:
        return { delayMs: 0 };
    }
  };

  const DIGIT_PROFILE_DEFAULTS = {
    verification: { min_digits: 4, max_digits: 8, timeout_s: 20, max_retries: 2, min_collect_delay_ms: 1500, end_call_on_success: true },
    otp: { min_digits: 4, max_digits: 8, timeout_s: 20, max_retries: 2, min_collect_delay_ms: 1500, end_call_on_success: true },
    pin: { min_digits: 4, max_digits: 8, timeout_s: 15, max_retries: 2, min_collect_delay_ms: 1200, end_call_on_success: true },
    ssn: { min_digits: 9, max_digits: 9, timeout_s: 15, max_retries: 2, min_collect_delay_ms: 1200, end_call_on_success: false },
    dob: { min_digits: 6, max_digits: 8, timeout_s: 15, max_retries: 2, min_collect_delay_ms: 1200, end_call_on_success: false },
    routing_number: { min_digits: 9, max_digits: 9, timeout_s: 15, max_retries: 2, min_collect_delay_ms: 1200, end_call_on_success: false },
    account_number: { min_digits: 6, max_digits: 17, timeout_s: 18, max_retries: 2, min_collect_delay_ms: 1200, end_call_on_success: false },
    account: { min_digits: 6, max_digits: 12, timeout_s: 15, max_retries: 2, min_collect_delay_ms: 1200, confirmation_style: 'last4', end_call_on_success: false },
    phone: { min_digits: 10, max_digits: 10, timeout_s: 15, max_retries: 2, min_collect_delay_ms: 1200, end_call_on_success: false },
    tax_id: { min_digits: 9, max_digits: 9, timeout_s: 15, max_retries: 2, min_collect_delay_ms: 1200, end_call_on_success: false },
    ein: { min_digits: 9, max_digits: 9, timeout_s: 15, max_retries: 2, min_collect_delay_ms: 1200, end_call_on_success: false },
    claim_number: { min_digits: 4, max_digits: 12, timeout_s: 15, max_retries: 2, min_collect_delay_ms: 1200, end_call_on_success: false },
    reservation_number: { min_digits: 4, max_digits: 12, timeout_s: 15, max_retries: 2, min_collect_delay_ms: 1200, end_call_on_success: false },
    ticket_number: { min_digits: 4, max_digits: 12, timeout_s: 15, max_retries: 2, min_collect_delay_ms: 1200, end_call_on_success: false },
    case_number: { min_digits: 4, max_digits: 12, timeout_s: 15, max_retries: 2, min_collect_delay_ms: 1200, end_call_on_success: false },
    amount: { min_digits: 1, max_digits: 9, timeout_s: 15, max_retries: 2, min_collect_delay_ms: 1200, end_call_on_success: false },
    callback_confirm: { min_digits: 10, max_digits: 10, timeout_s: 15, max_retries: 2, min_collect_delay_ms: 1200, end_call_on_success: false },
    cvv: { min_digits: 3, max_digits: 4, timeout_s: 12, max_retries: 2, min_collect_delay_ms: 1200, end_call_on_success: false },
    card_number: { min_digits: 13, max_digits: 19, timeout_s: 25, max_retries: 2, min_collect_delay_ms: 1500, confirmation_style: 'last4', end_call_on_success: false },
    card_expiry: { min_digits: 4, max_digits: 6, timeout_s: 20, max_retries: 2, min_collect_delay_ms: 1200, end_call_on_success: false },
    zip: { min_digits: 5, max_digits: 9, timeout_s: 15, max_retries: 2, min_collect_delay_ms: 1200, end_call_on_success: false },
    extension: { min_digits: 1, max_digits: 6, timeout_s: 10, max_retries: 2, min_collect_delay_ms: 800, end_call_on_success: false }
  };

  const PROFILE_RULES = {
    generic: { validation: 'none', mask_strategy: 'masked', channel_policy: { dtmf: true, sms: true, voice: true }, confirmation: 'none' },
    verification: { validation: 'otp', mask_strategy: 'masked', channel_policy: { dtmf: true, sms: true, voice: false }, confirmation: 'none' },
    otp: { validation: 'otp', mask_strategy: 'masked', channel_policy: { dtmf: true, sms: true, voice: false }, confirmation: 'none' },
    pin: { validation: 'pin', mask_strategy: 'masked', channel_policy: { dtmf: true, sms: false, voice: false }, confirmation: 'none' },
    ssn: { validation: 'ssn', mask_strategy: 'last4', channel_policy: { dtmf: true, sms: false, voice: false }, confirmation: 'none' },
    dob: { validation: 'dob', mask_strategy: 'masked', channel_policy: { dtmf: true, sms: false, voice: false }, confirmation: 'none' },
    routing_number: { validation: 'routing', mask_strategy: 'last4', channel_policy: { dtmf: true, sms: false, voice: false }, confirmation: 'none' },
    account_number: { validation: 'account', mask_strategy: 'last4', channel_policy: { dtmf: true, sms: false, voice: false }, confirmation: 'none' },
    account: { validation: 'account', mask_strategy: 'last4', channel_policy: { dtmf: true, sms: false, voice: false }, confirmation: 'last4' },
    phone: { validation: 'phone', mask_strategy: 'last4', channel_policy: { dtmf: true, sms: true, voice: true }, confirmation: 'last4' },
    tax_id: { validation: 'tax_id', mask_strategy: 'last4', channel_policy: { dtmf: true, sms: false, voice: false }, confirmation: 'none' },
    ein: { validation: 'ein', mask_strategy: 'last4', channel_policy: { dtmf: true, sms: false, voice: false }, confirmation: 'none' },
    claim_number: { validation: 'claim', mask_strategy: 'last4', channel_policy: { dtmf: true, sms: true, voice: true }, confirmation: 'none' },
    reservation_number: { validation: 'reservation', mask_strategy: 'last4', channel_policy: { dtmf: true, sms: true, voice: true }, confirmation: 'none' },
    ticket_number: { validation: 'ticket', mask_strategy: 'last4', channel_policy: { dtmf: true, sms: true, voice: true }, confirmation: 'none' },
    case_number: { validation: 'case', mask_strategy: 'last4', channel_policy: { dtmf: true, sms: true, voice: true }, confirmation: 'none' },
    extension: { validation: 'extension', mask_strategy: 'masked', channel_policy: { dtmf: true, sms: true, voice: true }, confirmation: 'none' },
    zip: { validation: 'zip', mask_strategy: 'masked', channel_policy: { dtmf: true, sms: true, voice: true }, confirmation: 'none' },
    amount: { validation: 'amount', mask_strategy: 'masked', channel_policy: { dtmf: true, sms: true, voice: true }, confirmation: 'spoken_amount' },
    callback_confirm: { validation: 'callback', mask_strategy: 'last4', channel_policy: { dtmf: true, sms: true, voice: true }, confirmation: 'last4' },
    card_number: { validation: 'luhn', mask_strategy: 'last4', channel_policy: { dtmf: true, sms: false, voice: false }, confirmation: 'last4' },
    cvv: { validation: 'cvv', mask_strategy: 'masked', channel_policy: { dtmf: true, sms: false, voice: false }, confirmation: 'none' },
    card_expiry: { validation: 'expiry', mask_strategy: 'masked', channel_policy: { dtmf: true, sms: false, voice: false }, confirmation: 'none' }
  };

  const generatedProfileDefaults = new Map();
  const buildGeneratedProfileDefaults = (profile) => {
    const normalized = normalizeProfileId(profile) || 'generic';
    if (generatedProfileDefaults.has(normalized)) {
      return generatedProfileDefaults.get(normalized);
    }
    const base = DIGIT_PROFILE_DEFAULTS[normalized] || {};
    const rules = PROFILE_RULES[normalized] || PROFILE_RULES.generic;
    const generated = Object.freeze({
      ...base,
      profile: normalized,
      validation: rules.validation,
      mask_strategy: rules.mask_strategy,
      channel_policy: rules.channel_policy,
      confirmation_strategy: rules.confirmation
    });
    generatedProfileDefaults.set(normalized, generated);
    return generated;
  };

  const sanitizedProfileDefaults = new Map();
  const sanitizeProfileDefaults = (profile, defaults = {}) => {
    const minDigits = Math.max(1, Number.isFinite(defaults.min_digits) ? defaults.min_digits : 1);
    const maxDigits = Math.max(minDigits, Number.isFinite(defaults.max_digits) ? defaults.max_digits : minDigits);
    const timeout = Number.isFinite(defaults.timeout_s) ? defaults.timeout_s : 15;
    const maxRetries = Number.isFinite(defaults.max_retries) ? defaults.max_retries : 2;
    const minCollectDelay = Number.isFinite(defaults.min_collect_delay_ms)
      ? defaults.min_collect_delay_ms
      : defaultCollectDelayMs;
    return {
      ...defaults,
      min_digits: minDigits,
      max_digits: maxDigits,
      timeout_s: Math.min(SAFE_TIMEOUT_MAX_S, Math.max(SAFE_TIMEOUT_MIN_S, timeout)),
      max_retries: Math.min(SAFE_RETRY_MAX, Math.max(0, maxRetries)),
      min_collect_delay_ms: Math.max(800, minCollectDelay)
    };
  };

  const validateProfileDefaults = () => {
    Object.keys(DIGIT_PROFILE_DEFAULTS).forEach((profile) => {
      if (!SUPPORTED_DIGIT_PROFILES.has(profile)) {
        logDigitMetric('profile_default_unsupported', { profile });
        return;
      }
      const generated = buildGeneratedProfileDefaults(profile);
      sanitizedProfileDefaults.set(profile, sanitizeProfileDefaults(profile, generated));
    });
  };

  validateProfileDefaults();

  function setCallDigitIntent(callSid, intent) {
    const callConfig = callConfigurations.get(callSid);
    if (!callConfig) return;
    callConfig.digit_intent = intent;
    if (intent?.mode === 'dtmf') {
      callConfig.digit_capture_active = true;
    } else if (intent?.mode === 'normal') {
      if (digitCollectionManager.expectations.has(callSid) || digitCollectionPlans.has(callSid)) {
        callConfig.digit_capture_active = true;
        callConfig.digit_intent = { mode: 'dtmf', reason: 'capture_active', confidence: 1 };
        callConfigurations.set(callSid, callConfig);
        return;
      }
      callConfig.digit_capture_active = false;
    }
    callConfigurations.set(callSid, callConfig);
  }

  function clearDigitIntent(callSid, reason = 'digits_captured') {
    if (digitCollectionManager.expectations.has(callSid) || digitCollectionPlans.has(callSid)) {
      return;
    }
    setCallDigitIntent(callSid, { mode: 'normal', reason, confidence: 1 });
  }

  function getDigitProfileDefaults(profile = 'generic') {
    const key = String(profile || 'generic').toLowerCase();
    if (sanitizedProfileDefaults.has(key)) {
      return sanitizedProfileDefaults.get(key);
    }
    return DIGIT_PROFILE_DEFAULTS[key] || {};
  }

  function normalizeDigitExpectation(params = {}) {
    const promptHint = `${params.prompt || ''} ${params.prompt_hint || ''}`.toLowerCase();
    const hasExplicitProfile = params.profile !== undefined
      && params.profile !== null
      && String(params.profile).trim() !== '';
    const hasExplicitLength = typeof params.min_digits === 'number'
      || typeof params.max_digits === 'number'
      || typeof params.force_exact_length === 'number';
    const allowProfileInference = params.allow_profile_inference === true;
    let profile = normalizeProfileId(hasExplicitProfile ? params.profile : 'generic') || 'generic';
    if (allowProfileInference && !hasExplicitProfile && !hasExplicitLength && profile === 'generic' && promptHint.match(/\b(code|otp|verification|verify|passcode|pin)\b/)) {
      profile = 'verification';
    }
    const defaults = getDigitProfileDefaults(profile);
    const minDigits = typeof params.min_digits === 'number'
      ? params.min_digits
      : (typeof defaults.min_digits === 'number' ? defaults.min_digits : 1);
    const maxDigits = typeof params.max_digits === 'number'
      ? params.max_digits
      : (typeof defaults.max_digits === 'number' ? defaults.max_digits : minDigits);
    const timeout = typeof params.timeout_s === 'number'
      ? params.timeout_s
      : (typeof defaults.timeout_s === 'number' ? defaults.timeout_s : 20);
    const maxRetries = typeof params.max_retries === 'number'
      ? params.max_retries
      : (typeof defaults.max_retries === 'number' ? defaults.max_retries : 2);
    const minCollectDelayMs = typeof params.min_collect_delay_ms === 'number'
      ? params.min_collect_delay_ms
      : (typeof defaults.min_collect_delay_ms === 'number' ? defaults.min_collect_delay_ms : defaultCollectDelayMs);
    const maskForGpt = typeof params.mask_for_gpt === 'boolean'
      ? params.mask_for_gpt
      : (typeof defaults.mask_for_gpt === 'boolean' ? defaults.mask_for_gpt : true);
    const speakConfirmationProvided = typeof params.speak_confirmation === 'boolean';
    const speakConfirmation = speakConfirmationProvided ? params.speak_confirmation : false;
    const confirmationStyle = params.confirmation_style || defaults.confirmation_style || 'none';
    const allowSmsFallback = typeof params.allow_sms_fallback === 'boolean'
      ? params.allow_sms_fallback
      : smsFallbackEnabled;
    const channel = params.channel || 'dtmf';
    const endCallOnSuccess = typeof params.end_call_on_success === 'boolean'
      ? params.end_call_on_success
      : (typeof defaults.end_call_on_success === 'boolean' ? defaults.end_call_on_success : false);
    const rawPrompt = params.prompt && String(params.prompt).trim().length > 0
      ? params.prompt
      : '';
    const reprompt_message = params.reprompt_message || defaults.reprompt_message || '';
    const terminatorChar = params.terminator_char || defaults.terminator_char || '#';
    const allowTerminator = params.allow_terminator === true || defaults.allow_terminator === true;
    const terminatorSuffix = allowTerminator
      ? ` You can end with ${terminatorChar} when finished.`
      : '';
    const prompt = rawPrompt ? `${rawPrompt}${terminatorSuffix}` : '';

    let normalizedMin = minDigits;
    let normalizedMax = maxDigits < minDigits ? minDigits : maxDigits;
    if (profile === 'verification' && params.force_exact_length) {
      normalizedMin = params.force_exact_length;
      normalizedMax = params.force_exact_length;
    }
    if (allowTerminator && terminatorChar === '#') {
      normalizedMax = Math.max(normalizedMax, normalizedMin);
    }
    if (profile === 'verification' || profile === 'otp') {
      if (normalizedMin < 4) normalizedMin = 4;
      if (normalizedMax < normalizedMin) normalizedMax = normalizedMin;
      if (normalizedMax > 8) normalizedMax = 8;
    }

    const repromptDefaults = buildDefaultReprompts({
      profile,
      min_digits: normalizedMin,
      max_digits: normalizedMax,
      allow_terminator: allowTerminator,
      terminator_char: terminatorChar
    });

    const reprompt_invalid = normalizeRepromptValue(
      params.reprompt_invalid ?? defaults.reprompt_invalid ?? repromptDefaults.invalid
    );
    const reprompt_incomplete = normalizeRepromptValue(
      params.reprompt_incomplete ?? defaults.reprompt_incomplete ?? repromptDefaults.invalid
    );
    const reprompt_timeout = normalizeRepromptValue(
      params.reprompt_timeout ?? defaults.reprompt_timeout ?? repromptDefaults.timeout
    );
    const failure_message = normalizeRepromptValue(
      params.failure_message ?? defaults.failure_message ?? repromptDefaults.failure
    );
    const timeout_failure_message = normalizeRepromptValue(
      params.timeout_failure_message ?? defaults.timeout_failure_message ?? repromptDefaults.timeout_failure
    );

    const estimatedPromptMs = estimateSpeechDurationMs(params.prompt || params.prompt_hint || '');
    const adjustedDelayMs = Math.max(minCollectDelayMs, estimatedPromptMs, 3000);
    const safeTimeout = Math.min(SAFE_TIMEOUT_MAX_S, Math.max(SAFE_TIMEOUT_MIN_S, timeout));
    const safeMaxRetries = Math.min(SAFE_RETRY_MAX, Math.max(0, maxRetries));
    const safeCollectDelayMs = Math.max(800, adjustedDelayMs);

    return {
      prompt,
      reprompt_message,
      reprompt_invalid,
      reprompt_incomplete,
      reprompt_timeout,
      failure_message,
      timeout_failure_message,
      profile,
      min_digits: normalizedMin,
      max_digits: normalizedMax,
      timeout_s: safeTimeout,
      max_retries: safeMaxRetries,
      min_collect_delay_ms: safeCollectDelayMs,
      confirmation_style: confirmationStyle,
      confirmation_locked: speakConfirmationProvided,
      allow_spoken_fallback: params.allow_spoken_fallback === true || defaults.allow_spoken_fallback === true,
      allow_sms_fallback: allowSmsFallback,
      mask_for_gpt: maskForGpt,
      speak_confirmation: speakConfirmation,
      end_call_on_success: endCallOnSuccess,
      allow_terminator: allowTerminator,
      terminator_char: terminatorChar,
      channel
    };
  }

  function buildDigitPrompt(expectation) {
    const label = buildExpectedLabel(expectation);
    const terminatorSuffix = expectation?.allow_terminator
      ? ` You can end with ${expectation?.terminator_char || '#'} when finished.`
      : '';
    return `Please enter the ${label} using your keypad.${terminatorSuffix}`;
  }

  function buildConfirmationMessage(expectation = {}, collection = {}) {
    const profile = String(expectation.profile || collection.profile || 'generic').toLowerCase();
    const style = expectation.confirmation_style || 'none';
    const speak = expectation.speak_confirmation === true || style !== 'none';
    if (!speak) return '';

    if (style === 'spoken_amount' && collection.digits) {
      const amountCents = Number(collection.digits);
      if (!Number.isNaN(amountCents)) {
        const dollars = (amountCents / 100).toFixed(2);
        return `Thanks, I noted ${dollars} dollars.`;
      }
    }

    if (style === 'last4' && collection.digits) {
      const last4 = collection.digits.slice(-4);
      if (last4) {
        return `Thanks, I have the number ending in ${last4}.`;
      }
    }

    switch (profile) {
      case 'verification':
      case 'otp':
        return 'Thanks, your code is received.';
      case 'extension':
        return 'Thanks, I have the extension.';
      case 'zip':
        return 'Thanks, I have the ZIP code.';
      case 'account':
        return 'Thanks, I have the account number.';
      default:
        return 'Thanks, I have that.';
    }
  }

  function clearDigitTimeout(callSid) {
    const timer = digitTimeouts.get(callSid);
    if (timer) {
      clearTimeout(timer);
      digitTimeouts.delete(callSid);
    }
  }

  function clearDigitFallbackState(callSid) {
    if (digitFallbackStates.has(callSid)) {
      digitFallbackStates.delete(callSid);
    }
  }

  function clearDigitPlan(callSid) {
    if (digitCollectionPlans.has(callSid)) {
      digitCollectionPlans.delete(callSid);
    }
  }

  function markDigitPrompted(callSid, gptService = null, interactionCount = 0, source = 'dtmf', options = {}) {
    const expectation = digitCollectionManager.expectations.get(callSid);
    if (!expectation) return false;
    const now = Date.now();
    const promptText = options?.prompt_text || options?.prompt || '';
    const explicitDurationMs = options?.prompt_duration_ms;
    const estimatedPromptMs = Number.isFinite(explicitDurationMs)
      ? explicitDurationMs
      : estimateSpeechDurationMs(promptText);
    const baseDelayMs = Number.isFinite(expectation.min_collect_delay_ms)
      ? expectation.min_collect_delay_ms
      : 0;
    const promptDelayMs = Math.max(1000, baseDelayMs, estimatedPromptMs || 0);
    expectation.prompted_at = now;
    expectation.prompted_delay_ms = promptDelayMs;
    digitCollectionManager.expectations.set(callSid, expectation);
    if (gptService) {
      void flushBufferedDigits(callSid, gptService, interactionCount, source, options);
    }
    return true;
  }

  function updatePromptDelay(callSid, durationMs) {
    const expectation = digitCollectionManager.expectations.get(callSid);
    if (!expectation || !Number.isFinite(durationMs)) return false;
    const baseDelayMs = Number.isFinite(expectation.min_collect_delay_ms)
      ? expectation.min_collect_delay_ms
      : 0;
    const currentDelayMs = Number.isFinite(expectation.prompted_delay_ms)
      ? expectation.prompted_delay_ms
      : 0;
    const nextDelayMs = Math.max(1000, baseDelayMs, currentDelayMs, durationMs);
    expectation.prompted_delay_ms = nextDelayMs;
    digitCollectionManager.expectations.set(callSid, expectation);
    return true;
  }

  function bufferDigits(callSid, digits = '', meta = {}) {
    if (!callSid || !digits) return;
    const existing = pendingDigits.get(callSid) || [];
    existing.push({ digits: String(digits), meta });
    pendingDigits.set(callSid, existing);
  }

  async function flushBufferedDigits(callSid, gptService = null, interactionCount = 0, source = 'dtmf', options = {}) {
    const queue = pendingDigits.get(callSid);
    if (!queue || queue.length === 0) return false;

    let processed = false;
    while (queue.length > 0) {
      if (!digitCollectionManager.expectations.has(callSid)) {
        logDigitMetric('flush_stopped_no_expectation', { callSid, remaining: queue.length });
        break;
      }
      const item = queue.shift();
      const collection = digitCollectionManager.recordDigits(callSid, item.digits, item.meta || {});
      processed = true;
      try {
        await handleCollectionResult(callSid, collection, gptService, interactionCount, source, options);
      } catch (err) {
        logDigitMetric('flush_error', { callSid, error: err.message, remaining: queue.length });
        console.error(`[digits] handleCollectionResult failed for ${callSid}:`, err);
        queue.unshift(item);  // Re-queue on failure
        break;
      }
    }

    if (queue.length === 0) {
      pendingDigits.delete(callSid);
    } else {
      pendingDigits.set(callSid, queue);
    }

    return processed;
  }

  function isValidLuhn(value = '') {
    const digits = String(value || '').replace(/\D/g, '');
    if (!digits) return false;
    let sum = 0;
    let shouldDouble = false;
    for (let i = digits.length - 1; i >= 0; i -= 1) {
      let digit = Number(digits[i]);
      if (Number.isNaN(digit)) return false;
      if (shouldDouble) {
        digit *= 2;
        if (digit > 9) digit -= 9;
      }
      sum += digit;
      shouldDouble = !shouldDouble;
    }
    return sum % 10 === 0;
  }

  function isValidRoutingNumber(value = '') {
    const digits = String(value || '').replace(/\D/g, '');
    if (digits.length !== 9) return false;
    const weights = [3, 7, 1, 3, 7, 1, 3, 7, 1];
    let sum = 0;
    for (let i = 0; i < 9; i += 1) {
      const n = Number(digits[i]);
      if (Number.isNaN(n)) return false;
      sum += n * weights[i];
    }
    return sum % 10 === 0;
  }

  function validateProfileDigits(profile = 'generic', digits = '') {
    const value = String(digits || '');
    if (!value) {
      return { valid: false, reason: 'empty' };
    }

    switch (normalizeProfileId(profile) || String(profile || '').toLowerCase()) {
      case 'verification':
      case 'otp':
        return { valid: true };
      case 'ssn':
        return value.length === 9 ? { valid: true } : { valid: false, reason: 'invalid_length' };
      case 'dob': {
        if (value.length !== 6 && value.length !== 8) {
          return { valid: false, reason: 'invalid_length' };
        }
        const month = Number(value.slice(0, 2));
        const day = Number(value.slice(2, 4));
        if (!month || month < 1 || month > 12) {
          return { valid: false, reason: 'invalid_month' };
        }
        if (!day || day < 1 || day > 31) {
          return { valid: false, reason: 'invalid_day' };
        }
        return { valid: true };
      }
      case 'routing_number':
        return isValidRoutingNumber(value)
          ? { valid: true }
          : { valid: false, reason: 'invalid_routing' };
      case 'account_number':
        return value.length >= 6 && value.length <= 17
          ? { valid: true }
          : { valid: false, reason: 'invalid_length' };
      case 'phone':
        return value.length === 10 ? { valid: true } : { valid: false, reason: 'invalid_phone' };
      case 'tax_id':
      case 'ein':
        return value.length === 9 ? { valid: true } : { valid: false, reason: 'invalid_length' };
      case 'cvv':
        if (value.length === 3 || value.length === 4) {
          return { valid: true };
        }
        return { valid: false, reason: 'invalid_cvv' };
      case 'card_number':
        if (value.length < 13 || value.length > 19) {
          return { valid: false, reason: 'invalid_card_length' };
        }
        return isValidLuhn(value)
          ? { valid: true }
          : { valid: false, reason: 'invalid_card_number' };
      case 'card_expiry': {
        if (value.length !== 4 && value.length !== 6) {
          return { valid: false, reason: 'invalid_expiry_length' };
        }
        const month = Number(value.slice(0, 2));
        if (!month || month < 1 || month > 12) {
          return { valid: false, reason: 'invalid_expiry_month' };
        }
        return { valid: true };
      }
      default:
        return { valid: true };
    }
  }

  const digitCollectionManager = {
    expectations: new Map(),
    setExpectation(callSid, params = {}) {
      const normalized = applyHealthPolicy(callSid, applyRiskPolicy(callSid, normalizeDigitExpectation(params)));
      this.expectations.set(callSid, {
        ...normalized,
        plan_id: params.plan_id || null,
        plan_step_index: Number.isFinite(params.plan_step_index) ? params.plan_step_index : null,
        plan_total_steps: Number.isFinite(params.plan_total_steps) ? params.plan_total_steps : null,
        prompted_at: params.prompted_at || null,
        retries: 0,
        buffer: '',
        collected: [],
        last_masked: null
      });
      setCallDigitIntent(callSid, { mode: 'dtmf', reason: 'expectation_set', confidence: 1 });
      logDigitMetric('expectation_set', {
        callSid,
        profile: normalized.profile,
        min_digits: normalized.min_digits,
        max_digits: normalized.max_digits,
        timeout_s: normalized.timeout_s,
        max_retries: normalized.max_retries,
        plan_id: normalized.plan_id || null,
        plan_step_index: normalized.plan_step_index || null,
        plan_total_steps: normalized.plan_total_steps || null
      });
      void emitAuditEvent(callSid, 'DigitCaptureStarted', {
        profile: normalized.profile,
        len: normalized.max_digits,
        source: normalized.channel || 'dtmf',
        reason: normalized.reason || null
      });
    },
    recordDigits(callSid, digits = '', meta = {}) {
      if (!digits) return { accepted: false, reason: 'empty' };
      const exp = this.expectations.get(callSid);
      if (!exp) return { accepted: false, reason: 'no_expectation' };
      
      // Validate input size to prevent buffer overflow
      const cleanDigitsTemp = String(digits || '').replace(/[^0-9]/g, '');
      if (cleanDigitsTemp.length > MAX_DIGITS_BUFFER) {
        logDigitMetric('digit_buffer_overflow', { callSid, length: cleanDigitsTemp.length, max: MAX_DIGITS_BUFFER });
        return { accepted: false, reason: 'exceeds_max_buffer', profile: exp.profile, mask_for_gpt: exp.mask_for_gpt };
      }
      
      const result = {
        profile: exp.profile,
        mask_for_gpt: exp.mask_for_gpt,
        source: meta.source || 'dtmf'
      };
      const hasTerminator = exp.allow_terminator && digits.includes(exp.terminator_char || '#');
      const cleanDigits = cleanDigitsTemp;
      const isRepeating = (val) => val.length >= 6 && /^([0-9])\1+$/.test(val);
      const isAscending = (val) => val.length >= 6 && '0123456789'.includes(val);

      if (meta.timestamp) {
        const lastTs = lastDtmfTimestamps.get(callSid) || 0;
        const gap = lastTs ? meta.timestamp - lastTs : null;
        if (gap !== null) {
          result.dtmf_gap_ms = gap;
        }
        if (gap !== null && gap < minDtmfGapMs && cleanDigits.length === 1) {
          result.accepted = false;
          result.reason = 'too_fast';
          result.heuristic = 'inter_key_gap';
          exp.buffer = '';
          this.expectations.set(callSid, exp);
          lastDtmfTimestamps.set(callSid, meta.timestamp);
          return result;
        }
        lastDtmfTimestamps.set(callSid, meta.timestamp);
      }

      exp.buffer = `${exp.buffer || ''}${String(cleanDigits)}`;
      const currentBuffer = exp.buffer;
      const len = currentBuffer.length;
      const inRange = len >= exp.min_digits && len <= exp.max_digits;
      const tooLong = len > exp.max_digits;
      const masked = len <= 4 ? currentBuffer : `${'*'.repeat(Math.max(0, len - 4))}${currentBuffer.slice(-4)}`;

      let accepted = inRange && !tooLong;
      let reason = null;

      if (hasTerminator) {
        if (len < exp.min_digits) {
          accepted = false;
          reason = 'too_short';
        } else if (len > exp.max_digits) {
          accepted = false;
          reason = 'too_long';
        } else {
          accepted = true;
        }
      }

      if (tooLong) {
        accepted = false;
        reason = 'too_long';
        exp.buffer = '';
      } else if (!inRange) {
        accepted = false;
        reason = 'incomplete';
      } else {
        const validation = validateProfileDigits(exp.profile, currentBuffer);
        if (!validation.valid) {
          accepted = false;
          reason = validation.reason || 'invalid';
          exp.buffer = '';
        }
      }

      Object.assign(result, {
        digits: currentBuffer,
        len,
        masked,
        accepted,
        reason
      });

      exp.collected.push(result.digits);
      exp.last_masked = masked;

      if (result.accepted) {
        if (isRepeating(currentBuffer) || isAscending(currentBuffer)) {
          result.accepted = false;
          result.reason = 'spam_pattern';
          result.heuristic = isRepeating(currentBuffer) ? 'repeat_pattern' : 'ascending_pattern';
          exp.buffer = '';
          exp.retries += 1;
          result.retries = exp.retries;
          this.expectations.set(callSid, exp);
          return result;
        }
        exp.buffer = '';
        if (hasTerminator) {
          exp.terminated = true;
        }
      } else if (result.reason && result.reason !== 'incomplete') {
        exp.retries += 1;
        result.retries = exp.retries;
        if (exp.retries > exp.max_retries) {
          result.fallback = true;
        }
      }

      if (result.reason === 'incomplete' && result.digits) {
        updateSessionState(callSid, { partialDigits: result.digits });
      } else if (result.accepted || result.reason) {
        updateSessionState(callSid, { partialDigits: '' });
      }

      this.expectations.set(callSid, exp);
      return result;
    }
  };

  async function scheduleDigitTimeout(callSid, gptService = null, interactionCount = 0) {
    const exp = digitCollectionManager.expectations.get(callSid);
    if (!exp || !exp.timeout_s) return;

    clearDigitTimeout(callSid);

    const timeoutMs = Math.max(5000, (exp.timeout_s || 10) * 1000);
    const promptAt = exp.prompted_at || Date.now();
    const promptDelayMs = Number.isFinite(exp.prompted_delay_ms)
      ? exp.prompted_delay_ms
      : (exp.min_collect_delay_ms || 0);
    const normalizedPromptDelayMs = Math.max(1000, promptDelayMs);
    const elapsedSincePrompt = Date.now() - promptAt;
    const remainingPromptDelayMs = Math.max(0, normalizedPromptDelayMs - elapsedSincePrompt);
    const waitMs = remainingPromptDelayMs + timeoutMs;

    const timer = setTimeout(async () => {
      const current = digitCollectionManager.expectations.get(callSid);
      if (!current) return;

      logDigitMetric('timeout_fired', {
        callSid,
        profile: current.profile || 'generic',
        attempt: (current.retries || 0) + 1,
        max_retries: current.max_retries
      });

      try {
        await db.addCallDigitEvent({
          call_sid: callSid,
          source: 'timeout',
          profile: current.profile || 'generic',
          digits: null,
          len: 0,
          accepted: false,
          reason: 'timeout',
          metadata: {
            attempt: (current.retries || 0) + 1,
            max_retries: current.max_retries
          }
        });
      } catch (err) {
        const log = logger || console;
        if (typeof log.error === 'function') {
          log.error('Error logging digit timeout:', err);
        } else if (typeof log.log === 'function') {
          log.log('Error logging digit timeout:', err);
        }
      }

      if (!digitFallbackStates.get(callSid)?.active && typeof triggerTwilioGatherFallback === 'function') {
        try {
          const fallbackPrompt = current?.reprompt_timeout || buildDigitPrompt(current);
          const usedFallback = await triggerTwilioGatherFallback(callSid, current, {
            prompt: queuePendingDigitAction ? '' : fallbackPrompt
          });
          if (usedFallback) {
            if (queuePendingDigitAction && fallbackPrompt) {
              queuePendingDigitAction(callSid, { type: 'reprompt', text: fallbackPrompt, scheduleTimeout: true });
            }
            return;
          }
        } catch (err) {
          logger.error('Twilio gather fallback error:', err);
        }
      }

      current.retries = (current.retries || 0) + 1;
      digitCollectionManager.expectations.set(callSid, current);

      if (current.retries > current.max_retries) {
        digitCollectionManager.expectations.delete(callSid);
        clearDigitTimeout(callSid);
        clearDigitFallbackState(callSid);
        clearDigitPlan(callSid);
        const finalTimeoutMessage = current.timeout_failure_message || callEndMessages.no_response;
        await speakAndEndCall(callSid, finalTimeoutMessage, 'digit_collection_timeout');
        return;
      }

      const affect = recordCallerAffect(callSid, 'timeout');
      const policy = buildRetryPolicy({
        reason: 'timeout',
        attempt: current.retries || 1,
        source: 'dtmf',
        expectation: current,
        affect,
        session: getSessionState(callSid),
        health: getSystemHealth(callSid)
      });
      const prompt = policy.prompt
        || chooseReprompt(current, 'timeout', current.retries)
        || `I did not catch that. Please re-enter the ${buildExpectedLabel(current)} now.`;

      const personalityInfo = gptService?.personalityEngine?.getCurrentPersonality();
      const reply = {
        partialResponseIndex: null,
        partialResponse: prompt,
        personalityInfo,
        adaptationHistory: gptService?.personalityChanges?.slice(-3) || []
      };

      if (gptService) {
        gptService.emit('gptreply', reply, interactionCount);
        try {
          gptService.updateUserContext('digit_timeout', 'system', `Digit timeout retry ${current.retries}/${current.max_retries}`);
        } catch (_) {}
        markDigitPrompted(callSid, gptService, interactionCount, 'dtmf', { prompt_text: prompt });
      }

      webhookService.addLiveEvent(callSid, `⏳ Awaiting digits retry ${current.retries}/${current.max_retries}`, { force: true });

      scheduleDigitTimeout(callSid, gptService, interactionCount + 1);
    }, waitMs);

    digitTimeouts.set(callSid, timer);
  }

  function buildTwilioGatherTwiml(callSid, expectation, options = {}, hostname) {
    if (!VoiceResponse) {
      throw new Error('VoiceResponse not configured for Twilio gather');
    }
    const response = new VoiceResponse();
    const min = expectation?.min_digits || 1;
    const max = expectation?.max_digits || min;
    const host = hostname || config?.server?.hostname;
    const actionUrl = `https://${host}/webhook/twilio-gather?callSid=${encodeURIComponent(callSid)}`;
    const gather = response.gather({
      input: 'dtmf',
      numDigits: max,
      timeout: Math.max(3, expectation?.timeout_s || 10),
      action: actionUrl,
      method: 'POST'
    });
    const hasPromptOverride = Object.prototype.hasOwnProperty.call(options, 'prompt');
    const prompt = hasPromptOverride ? options.prompt : buildDigitPrompt(expectation);
    if (prompt) {
      gather.say(prompt);
    }
    if (options.followup) {
      response.say(options.followup);
    }
    return response.toString();
  }

  async function triggerTwilioGatherFallback(callSid, expectation, options = {}) {
    const provider = typeof getCurrentProvider === 'function' ? getCurrentProvider() : config?.platform?.provider;
    if (provider && provider !== 'twilio') return false;
    if (!config?.twilio?.gatherFallback) return false;
    if (!config?.server?.hostname) return false;

    const state = digitFallbackStates.get(callSid);
    if (state?.active) return false;

    const accountSid = config.twilio.accountSid;
    const authToken = config.twilio.authToken;
    if (!accountSid || !authToken || !twilioClient) {
      return false;
    }

    const client = twilioClient(accountSid, authToken);
    const twiml = buildTwilioGatherTwiml(callSid, expectation, options);
    await client.calls(callSid).update({ twiml });
    markDigitPrompted(callSid, null, 0, 'dtmf', { prompt_text: options.prompt || '' });

    digitFallbackStates.set(callSid, {
      active: true,
      attempts: (state?.attempts || 0) + 1,
      lastAt: new Date().toISOString()
    });

    webhookService.addLiveEvent(callSid, '📟 Capturing Mode', { force: true });
    return true;
  }

  function formatOtpForDisplay(digits, mode = otpDisplayMode, expectedLength = null) {
    const safeDigits = String(digits || '').replace(/\D/g, '');
    const targetLen = Number.isFinite(expectedLength) && expectedLength > 0 ? expectedLength : otpLength;
    if (mode === 'length') {
      return `OTP received (${safeDigits.length} digits)`;
    }
    if (mode === 'progress') {
      return `OTP entry: ${safeDigits.length}/${targetLen} digits received`;
    }
    if (!safeDigits) return 'OTP received';
    const maskLen = Math.max(0, safeDigits.length - 2);
    const masked = `${'*'.repeat(maskLen)}${safeDigits.slice(-2)}`;
    return `OTP received: ${masked}`;
  }

  function formatDigitsGeneral(digits, masked = null, mode = 'live') {
    const raw = String(digits || '');
    if (mode === 'live' && showRawDigitsLive) return raw;
    if (mode === 'notify' && sendRawDigitsToUser) return raw;
    if (masked) return masked;
    const safe = raw.replace(/\d{0,}/g, (m) => (m.length <= 4 ? m : `${'*'.repeat(Math.max(0, m.length - 2))}${m.slice(-2)}`));
    return safe;
  }

  function hasDigitEntryContext(text = '') {
    // Check if text contains nearby keywords indicating digit entry context
    const keywords = /\b(enter|press|key|digit|code|number|input|type|dial|read|say|provide)\b/;
    return keywords.test(String(text || '').toLowerCase());
  }

  function extractSpokenDigitSequences(text = '', callSid = null) {
    if (!text) return [];
    const lower = String(text || '').toLowerCase();
    const tokens = lower
      .replace(/[^a-z0-9\s-]/g, ' ')  // Allow hyphens for sequences like "one-two-three"
      .split(/[\s-]+/)
      .filter(Boolean);

    const sequences = [];
    let buffer = '';
    let repeat = 1;

    for (const token of tokens) {
      if (token === 'double') {
        repeat = 2;
        continue;
      }
      if (token === 'triple') {
        repeat = 3;
        continue;
      }

      const digit = DIGIT_WORD_MAP[token];
      if (digit) {
        buffer += digit.repeat(repeat);
        repeat = 1;
        continue;
      }

      if (/^\d+$/.test(token)) {
        if (buffer) {
          sequences.push(buffer);
          buffer = '';
        }
        sequences.push(token);
        repeat = 1;
        continue;
      }

      if (buffer) {
        sequences.push(buffer);
        buffer = '';
      }
      repeat = 1;
    }

    if (buffer) {
      sequences.push(buffer);
    }

    // Filter out sequences that don't have digit entry context if we have an active expectation
    if (callSid && digitCollectionManager.expectations.has(callSid) && !hasDigitEntryContext(text)) {
      const filtered = sequences.filter((seq) => {
        // Keep sequences that are part of larger numeric context
        return /\d/.test(seq) && seq.length >= 4;
      });
      return filtered.length > 0 ? filtered : [];
    }

    return sequences;
  }

  function getOtpContext(text = '', callSid = null) {
    if (!text) {
      return {
        raw: text,
        maskedForGpt: text,
        maskedForLogs: text,
        otpDetected: false,
        codes: []
      };
    }
    const expectation = callSid ? digitCollectionManager.expectations.get(callSid) : null;
    const maskForGpt = expectation ? expectation.mask_for_gpt !== false : true;
    const minExpected = typeof expectation?.min_digits === 'number' ? expectation.min_digits : 4;
    const maxExpected = typeof expectation?.max_digits === 'number' ? expectation.max_digits : 8;
    const dynamicRegex = expectation
      ? new RegExp(`\\b\\d{${minExpected},${maxExpected}}\\b`, 'g')
      : OTP_REGEX;
    const numericCodes = [...text.matchAll(dynamicRegex)].map((m) => m[0]);
    // Pass callSid to extractSpokenDigitSequences for context-aware filtering
    const spokenCodes = extractSpokenDigitSequences(text, callSid).filter((code) => code.length >= minExpected && code.length <= maxExpected);
    const codes = [...numericCodes, ...spokenCodes];
    const otpDetected = codes.length > 0;
    const masked = text.replace(dynamicRegex, '******').replace(SPOKEN_DIGIT_PATTERN, '******');
    return {
      raw: text,
      maskedForGpt: maskForGpt ? masked : text,
      maskedForLogs: masked,
      otpDetected,
      codes
    };
  }

  function maskOtpForExternal(text = '') {
    if (!text) return text;
    return text.replace(OTP_REGEX, '******').replace(SPOKEN_DIGIT_PATTERN, '******');
  }

  function buildExpectationFromConfig(callConfig = {}) {
    const rawProfile = String(
      callConfig.collection_profile
        || callConfig.digit_profile_id
        || callConfig.digitProfileId
        || callConfig.digit_profile
        || ''
    ).trim().toLowerCase();
    if (!rawProfile) return null;
    if (REMOVED_DIGIT_PROFILES.has(rawProfile)) return null;
    const profile = normalizeProfileId(rawProfile);
    if (!profile) return null;
    if (!isSupportedProfile(profile)) {
      logDigitMetric('profile_unsupported', { profile });
      return null;
    }
    const defaults = getDigitProfileDefaults(profile);
    const expectedLength = Number(callConfig.collection_expected_length);
    const explicitLength = Number.isFinite(expectedLength) ? expectedLength : null;
    const minDigits = explicitLength || defaults.min_digits || 1;
    const maxDigits = explicitLength || defaults.max_digits || minDigits;
    const timeout = Number(callConfig.collection_timeout_s);
    const timeout_s = Number.isFinite(timeout) ? timeout : defaults.timeout_s;
    const retries = Number(callConfig.collection_max_retries);
    const max_retries = Number.isFinite(retries) ? retries : defaults.max_retries;
    const mask_for_gpt = typeof callConfig.collection_mask_for_gpt === 'boolean'
      ? callConfig.collection_mask_for_gpt
      : (typeof defaults.mask_for_gpt === 'boolean' ? defaults.mask_for_gpt : true);
    const speak_confirmation = typeof callConfig.collection_speak_confirmation === 'boolean'
      ? callConfig.collection_speak_confirmation
      : false;
    const prompt = ''; // initial prompt now comes from bot payload, not profile
    const endCallOverride = typeof callConfig.collection_end_call_on_success === 'boolean'
      ? callConfig.collection_end_call_on_success
      : null;
    const end_call_on_success = endCallOverride !== null
      ? endCallOverride
      : true;
    return {
      profile,
      min_digits: minDigits,
      max_digits: maxDigits,
      timeout_s,
      max_retries,
      mask_for_gpt,
      speak_confirmation,
      prompt,
      end_call_on_success
    };
  }

  function resolveLockedExpectation(callConfig = {}) {
    if (!callConfig) return null;
    const fromConfig = buildExpectationFromConfig(callConfig);
    if (fromConfig?.profile) {
      return normalizeDigitExpectation({ ...fromConfig, prompt: '' });
    }
    const fromIntent = callConfig?.digit_intent?.expectation;
    if (fromIntent?.profile) {
      return normalizeDigitExpectation({ ...fromIntent, prompt: fromIntent.prompt || '' });
    }
    const tpl = callConfig.template_policy || {};
    if (tpl.requires_otp) {
      const len = tpl.expected_length || otpLength;
      return normalizeDigitExpectation({
        profile: tpl.default_profile || 'verification',
        min_digits: len,
        max_digits: len,
        force_exact_length: len,
        prompt: ''
      });
    }
    return null;
  }

  const MIN_INFER_CONFIDENCE = 0.65;

  function inferDigitExpectationFromText(text = '', callConfig = {}) {
    const lower = String(text || '').toLowerCase();
    const tpl = callConfig.template_policy || {};
    const contains = (re) => re.test(lower);
    const explicitProfile = normalizeProfileId(
      callConfig.collection_profile
        || callConfig.digit_profile_id
        || callConfig.digitProfileId
        || callConfig.digit_profile
        || ''
    );
    const numberHint = (re) => {
      const m = lower.match(re);
      return m ? parseInt(m[1], 10) : null;
    };
    const hasPress = contains(/\bpress\b/);
    const hasEnter = contains(/\b(enter|input|key in|type|dial)\b/);
    const explicitDigitCount = numberHint(/\b(\d{1,2})\s*[- ]?digit\b/);
    const explicitCodeCount = numberHint(/\b(\d{1,2})\s*[- ]?code\b/);
    const explicitLen = explicitDigitCount || explicitCodeCount;
    const explicitCommand = hasPress || hasEnter;
    const hasStrongOtpSignals = contains(/\b(otp|one[-\s]?time|passcode|password)\b/);
    const hasOtpDeliveryPhrase = contains(/\b(text message code|sms code|texted code)\b/);
    const hasCodeSignals = contains(/\b(code|security code|auth(?:entication)? code)\b/);
    const hasOtpDelivery = contains(/\b(text message|sms|texted)\b/);
    const hasDigitWord = contains(/\bdigit(s)?\b/);
    const hasOtpDeliveryDigits = hasOtpDelivery && (hasDigitWord || explicitLen);
    const hasActionOrCount = explicitCommand || explicitLen;

    if (explicitProfile) {
      return null;
    }

    if (tpl.requires_otp) {
      const len = tpl.expected_length || otpLength;
      return {
        profile: tpl.default_profile || 'verification',
        min_digits: len,
        max_digits: len,
        force_exact_length: len,
        prompt: '',
        end_call_on_success: true,
        max_retries: otpMaxRetries,
        confidence: 0.95,
        reason: 'template_requires_otp',
        allow_terminator: tpl.allow_terminator === true,
        terminator_char: tpl.terminator_char || '#'
      };
    }

    if (tpl.default_profile && tpl.default_profile !== 'generic') {
      const len = tpl.expected_length || otpLength;
      return {
        profile: tpl.default_profile,
        min_digits: len,
        max_digits: len,
        force_exact_length: len,
        prompt: '',
        end_call_on_success: tpl.default_profile === 'verification',
        max_retries: otpMaxRetries,
        confidence: 0.8,
        reason: 'template_default_profile',
        allow_terminator: tpl.allow_terminator === true,
        terminator_char: tpl.terminator_char || '#'
      };
    }

    const buildProfileExpectation = (profile, overrides = {}, reason = 'keyword', confidence = 0.7) => {
      const defaults = getDigitProfileDefaults(profile);
      return {
        profile,
        min_digits: overrides.min_digits || defaults.min_digits || 1,
        max_digits: overrides.max_digits || defaults.max_digits || overrides.min_digits || defaults.min_digits || 1,
        force_exact_length: overrides.force_exact_length || false,
        prompt: '',
        end_call_on_success: typeof overrides.end_call_on_success === 'boolean'
          ? overrides.end_call_on_success
          : (profile === 'verification' || profile === 'otp'),
        max_retries: overrides.max_retries || defaults.max_retries || 2,
        confidence,
        reason,
        allow_terminator: tpl.allow_terminator === true,
        terminator_char: tpl.terminator_char || '#'
      };
    };

    const exactKeywordProfiles = [
      { profile: 'verification', regex: /\b(otp|one[-\s]?time|one[-\s]?time password|verification code|passcode)\b/, reason: 'otp_exact_keyword', confidence: 0.9 },
      { profile: 'pin', regex: /\bpin\b/, min: 4, max: 8, reason: 'pin_keyword', confidence: 0.85 },
      { profile: 'routing_number', regex: /\brouting number\b/, min: 9, max: 9, exact: 9, reason: 'routing_keyword', confidence: 0.8 },
      { profile: 'account_number', regex: /\b(bank account|bank acct)\b/, min: 6, max: 17, reason: 'account_number_keyword', confidence: 0.75 },
      { profile: 'ssn', regex: /\b(ssn|social security)\b/, min: 9, max: 9, exact: 9, reason: 'ssn_keyword', confidence: 0.85 },
      { profile: 'dob', regex: /\b(date of birth|dob|birth date)\b/, min: 6, max: 8, reason: 'dob_keyword', confidence: 0.75 },
      { profile: 'phone', regex: /\b(phone number|callback number|call back number)\b/, min: 10, max: 10, exact: 10, reason: 'phone_keyword', confidence: 0.7 },
      { profile: 'tax_id', regex: /\b(tax id|tax identification|tin)\b/, min: 9, max: 9, exact: 9, reason: 'tax_id_keyword', confidence: 0.7 },
      { profile: 'ein', regex: /\b(ein|employer identification)\b/, min: 9, max: 9, exact: 9, reason: 'ein_keyword', confidence: 0.7 },
      { profile: 'claim_number', regex: /\b(claim number|claim)\b/, min: 4, max: 12, reason: 'claim_keyword', confidence: 0.7 },
      { profile: 'reservation_number', regex: /\b(reservation number|reservation)\b/, min: 4, max: 12, reason: 'reservation_keyword', confidence: 0.7 },
      { profile: 'ticket_number', regex: /\b(ticket number|ticket id|ticket)\b/, min: 4, max: 12, reason: 'ticket_keyword', confidence: 0.7 },
      { profile: 'case_number', regex: /\b(case number|case id|case)\b/, min: 4, max: 12, reason: 'case_keyword', confidence: 0.7 },
      { profile: 'extension', regex: /\b(extension|ext\.?)\b/, min: 2, max: 6, reason: 'extension_keyword', confidence: 0.7 }
    ];

    for (const entry of exactKeywordProfiles) {
      if (!contains(entry.regex)) continue;
      if (!hasActionOrCount) return null;
      const useLen = entry.profile === 'verification' ? (explicitLen || otpLength) : null;
      return buildProfileExpectation(entry.profile, {
        min_digits: useLen || entry.min,
        max_digits: useLen || entry.max,
        force_exact_length: useLen || entry.exact || false,
        end_call_on_success: true
      }, entry.reason, entry.confidence);
    }

    // OTP / verification keyword fallback (requires action verb or explicit length)
    const hasOtpSignals = (hasStrongOtpSignals || hasOtpDeliveryPhrase || hasCodeSignals || hasOtpDeliveryDigits)
      && hasActionOrCount;

    if (hasOtpSignals) {
      const len = explicitLen || otpLength;
      return {
        profile: 'verification',
        min_digits: len,
        max_digits: len,
        force_exact_length: len,
        prompt: '',
        end_call_on_success: true,
        max_retries: otpMaxRetries,
        confidence: 0.8,
        reason: 'otp_keyword',
        allow_terminator: tpl.allow_terminator === true,
        terminator_char: tpl.terminator_char || '#'
      };
    }

    const weightedProfiles = [
      { profile: 'account_number', keywords: [/account\b/, /\bnumber\b/], weight: 0.45, min: 6, max: 17, reason: 'account_weighted' },
      { profile: 'claim_number', keywords: [/claim\b/, /\bnumber\b/], weight: 0.4, min: 4, max: 12, reason: 'claim_weighted' },
      { profile: 'reservation_number', keywords: [/reservation\b/, /\bnumber\b/], weight: 0.4, min: 4, max: 12, reason: 'reservation_weighted' },
      { profile: 'ticket_number', keywords: [/ticket\b/, /\bnumber\b/], weight: 0.4, min: 4, max: 12, reason: 'ticket_weighted' },
      { profile: 'case_number', keywords: [/case\b/, /\bnumber\b/], weight: 0.4, min: 4, max: 12, reason: 'case_weighted' }
    ];

    let best = null;
    let second = null;
    for (const entry of weightedProfiles) {
      let score = 0;
      entry.keywords.forEach((kw) => {
        if (kw.test(lower)) score += entry.weight;
      });
      if (!score) continue;
      const candidate = { ...entry, score };
      if (!best || candidate.score > best.score) {
        second = best;
        best = candidate;
      } else if (!second || candidate.score > second.score) {
        second = candidate;
      }
    }

    if (best && hasActionOrCount) {
      const minScore = 0.75;
      const gap = best.score - (second?.score || 0);
      if (best.score >= minScore && gap >= 0.2) {
        return buildProfileExpectation(best.profile, {
          min_digits: best.min,
          max_digits: best.max,
          end_call_on_success: true
        }, best.reason, Math.min(0.85, best.score));
      }
    }

    return null;
  }

  function determineDigitIntent(callSid, callConfig = {}) {
    const explicitProfileRaw = callConfig.collection_profile
      || callConfig.digit_profile_id
      || callConfig.digitProfileId
      || callConfig.digit_profile;
    if (explicitProfileRaw) {
      const normalizedProfile = normalizeProfileId(explicitProfileRaw);
      if (!isSupportedProfile(normalizedProfile)) {
        logDigitMetric('profile_invalid_config', { profile: explicitProfileRaw });
        return { mode: 'normal', reason: 'invalid_profile', confidence: 0 };
      }
    }
    const explicit = buildExpectationFromConfig(callConfig);
    if (explicit) {
      return {
        mode: 'dtmf',
        reason: 'explicit_config',
        confidence: 0.95,
        expectation: explicit
      };
    }

    let candidates = [];
    if (typeof intentPredictor === 'function') {
      try {
        const predicted = intentPredictor({ callSid, callConfig });
        if (Array.isArray(predicted)) {
          candidates = predicted;
        }
      } catch (err) {
        logDigitMetric('intent_predictor_error', { callSid, error: err.message });
      }
    } else {
      candidates = estimateIntentCandidates(callSid, callConfig);
    }
    if (candidates.length) {
      logDigitMetric('intent_candidates', {
        callSid,
        candidates: candidates.map((entry) => ({
          profile: entry.profile,
          score: Number(entry.score || 0).toFixed(2),
          sources: entry.sources || []
        }))
      });
      const top = candidates[0];
      if (top?.profile && (top.score || 0) >= INTENT_PREDICT_MIN_SCORE) {
        const predicted = buildProfileExpectation(top.profile, {}, 'predictive_intent', top.score);
        if (predicted) {
          return {
            mode: 'dtmf',
            reason: predicted.reason || 'predictive_intent',
            confidence: predicted.confidence || top.score,
            expectation: predicted
          };
        }
      }
    }

    const text = `${callConfig.prompt || ''} ${callConfig.first_message || ''}`.trim();
    if (!text) {
      return { mode: 'normal', reason: 'no_prompt', confidence: 0 };
    }

    const inferred = inferDigitExpectationFromText(text, callConfig);
    if (inferred && (inferred.confidence || 0) >= MIN_INFER_CONFIDENCE) {
      return {
        mode: 'dtmf',
        reason: inferred.reason || 'prompt_signal',
        confidence: inferred.confidence || 0.6,
        expectation: inferred
      };
    }

    return { mode: 'normal', reason: 'no_signal', confidence: 0 };
  }

  function prepareInitialExpectation(callSid, callConfig = {}) {
    const intent = determineDigitIntent(callSid, callConfig);
    logDigitMetric('intent_resolved', {
      callSid,
      mode: intent?.mode,
      profile: intent?.expectation?.profile || null,
      reason: intent?.reason || null,
      confidence: intent?.confidence || 0
    });
    if (intent.mode !== 'dtmf' || !intent.expectation) {
      return { intent, expectation: null };
    }
    const payload = normalizeDigitExpectation({
      ...intent.expectation,
      prompt: '',
      prompt_hint: `${callConfig.first_message || ''} ${callConfig.prompt || ''}`
    });
    payload.reason = intent.reason || 'initial_intent';
    digitCollectionManager.setExpectation(callSid, payload);
    return { intent, expectation: payload };
  }

  async function startNextDigitPlanStep(callSid, plan, gptService = null, interactionCount = 0) {
    if (!plan || !Array.isArray(plan.steps) || plan.index >= plan.steps.length) return;
    const step = plan.steps[plan.index];
    const callConfig = callConfigurations.get(callSid);
    const promptHint = [callConfig?.first_message, callConfig?.prompt]
      .filter(Boolean)
      .join(' ');
    const payload = normalizeDigitExpectation({ ...step, prompt_hint: promptHint });
    payload.plan_id = plan.id;
    payload.plan_step_index = plan.index + 1;
    payload.plan_total_steps = plan.steps.length;

    if (isCircuitOpen()) {
      await handleCircuitFallback(callSid, payload, true, false, 'system');
      return;
    }

    digitCollectionManager.setExpectation(callSid, payload);
    if (typeof clearSilenceTimer === 'function') {
      clearSilenceTimer(callSid);
    }

    try {
      await db.updateCallState(callSid, 'digit_collection_requested', payload);
    } catch (err) {
      logger.error('digit plan step updateCallState error:', err);
    }

    const stepLabel = payload.profile || 'digits';
    const stepTitle = formatPlanStepLabel(payload);
    if (stepTitle) {
      webhookService.addLiveEvent(callSid, `🧭 ${stepTitle} — awaiting input`, { force: true });
    } else {
      webhookService.addLiveEvent(callSid, `🔢 Collect digits (${stepLabel}) step ${payload.plan_step_index}/${payload.plan_total_steps}`, { force: true });
    }

    await flushBufferedDigits(callSid, gptService, interactionCount, 'dtmf', { allowCallEnd: true });
    const currentExpectation = digitCollectionManager.expectations.get(callSid);
    if (!currentExpectation) {
      return;
    }
    if (currentExpectation.plan_id && currentExpectation.plan_id !== payload.plan_id) {
      return;
    }
    if (currentExpectation.plan_step_index && currentExpectation.plan_step_index !== payload.plan_step_index) {
      return;
    }

    const basePrompt = payload.prompt || buildDigitPrompt(payload);
    const instruction = payload.plan_total_steps
      ? `Step ${payload.plan_step_index} of ${payload.plan_total_steps}. ${basePrompt}`
      : basePrompt;
    const channel = payload.channel || plan.channel || 'dtmf';

    if (channel === 'sms' && smsService) {
      const smsPrompt = buildSmsStepPrompt(payload);
      try {
        const session = smsSessions.get(callSid) || await createSmsSession(callSid, payload, 'plan_step');
        if (session) {
          await smsService.sendSMS(session.phone, smsPrompt, null, { idempotencyKey: `${callSid}:${payload.plan_step_index}:sms-step` });
          logDigitMetric('sms_step_prompt_sent', {
            callSid,
            step: payload.plan_step_index,
            profile: payload.profile
          });
        }
      } catch (err) {
        logDigitMetric('sms_step_prompt_failed', { callSid, error: err.message });
      }
    }

    if (gptService && channel !== 'sms') {
      gptService.emit('gptreply', {
        partialResponseIndex: null,
        partialResponse: instruction,
        personalityInfo: gptService.personalityEngine.getCurrentPersonality(),
        adaptationHistory: gptService.personalityChanges?.slice(-3) || []
      }, interactionCount);
      try {
        gptService.updateUserContext('digit_collection_plan', 'system', `Digit plan step ${payload.plan_step_index}/${payload.plan_total_steps} (${payload.profile})`);
      } catch (_) {}
    }

    if (channel !== 'sms') {
      markDigitPrompted(callSid, gptService, interactionCount, 'dtmf', {
        allowCallEnd: true,
        prompt_text: instruction
      });
      scheduleDigitTimeout(callSid, gptService, interactionCount);
    }
  }

  async function requestDigitCollection(callSid, args = {}, gptService = null) {
    if (digitCollectionPlans.has(callSid)) {
      clearDigitPlan(callSid);
    }
    if (isCircuitOpen()) {
      const payload = normalizeDigitExpectation({ ...args });
      await handleCircuitFallback(callSid, payload, true, false, 'system');
      return { error: 'circuit_open' };
    }
    setCallDigitIntent(callSid, { mode: 'dtmf', reason: 'tool_request', confidence: 1 });
    if (typeof args.end_call_on_success !== 'boolean') {
      args.end_call_on_success = true;
    }
    if (args.profile) {
      const normalizedProfile = normalizeProfileId(args.profile);
      if (!isSupportedProfile(normalizedProfile)) {
        logDigitMetric('profile_invalid_request', { profile: args.profile });
        args.profile = 'generic';
      } else {
        args.profile = normalizedProfile;
      }
    }
    const callConfig = callConfigurations.get(callSid);
    const lockedExpectation = resolveLockedExpectation(callConfig);
    if (lockedExpectation?.profile) {
      const requestedProfile = args.profile ? String(args.profile).toLowerCase() : null;
      if (requestedProfile && requestedProfile !== lockedExpectation.profile) {
        logger.warn(`Digit profile override: ${requestedProfile} -> ${lockedExpectation.profile}`);
        webhookService.addLiveEvent(callSid, `🔒 Digit profile locked to ${lockedExpectation.profile}`, { force: true });
      }
      args = {
        ...args,
        profile: lockedExpectation.profile
      };
      if (typeof args.min_digits !== 'number' && typeof lockedExpectation.min_digits === 'number') {
        args.min_digits = lockedExpectation.min_digits;
      }
      if (typeof args.max_digits !== 'number' && typeof lockedExpectation.max_digits === 'number') {
        args.max_digits = lockedExpectation.max_digits;
      }
      if (lockedExpectation.force_exact_length) {
        args.min_digits = lockedExpectation.force_exact_length;
        args.max_digits = lockedExpectation.force_exact_length;
      }
      if (typeof args.end_call_on_success !== 'boolean' && typeof lockedExpectation.end_call_on_success === 'boolean') {
        args.end_call_on_success = lockedExpectation.end_call_on_success;
      }
      if (typeof args.allow_terminator !== 'boolean' && typeof lockedExpectation.allow_terminator === 'boolean') {
        args.allow_terminator = lockedExpectation.allow_terminator;
      }
      if (!args.terminator_char && lockedExpectation.terminator_char) {
        args.terminator_char = lockedExpectation.terminator_char;
      }
    }
    const promptHint = [callConfig?.first_message, callConfig?.prompt]
      .filter(Boolean)
      .join(' ');
    const payload = normalizeDigitExpectation({ ...args, prompt_hint: promptHint });
    try {
      logDigitMetric('single_collection_requested', {
        callSid,
        profile: payload.profile,
        min_digits: payload.min_digits,
        max_digits: payload.max_digits,
        timeout_s: payload.timeout_s,
        max_retries: payload.max_retries
      });
      await db.updateCallState(callSid, 'digit_collection_requested', payload);
      webhookService.addLiveEvent(callSid, `🔢 Collect digits (${payload.profile}): ${payload.min_digits}-${payload.max_digits}`, { force: true });
      digitCollectionManager.setExpectation(callSid, payload);
      if (typeof clearSilenceTimer === 'function') {
        clearSilenceTimer(callSid);
      }
      await flushBufferedDigits(callSid, gptService, 0, 'dtmf', { allowCallEnd: true });
      if (!digitCollectionManager.expectations.has(callSid)) {
        return payload;
      }
      const instruction = payload.prompt || buildDigitPrompt(payload);
      if (gptService) {
        const reply = {
          partialResponseIndex: null,
          partialResponse: instruction,
          personalityInfo: gptService.personalityEngine.getCurrentPersonality(),
          adaptationHistory: gptService.personalityChanges?.slice(-3) || []
        };
        gptService.emit('gptreply', reply, 0);
        gptService.updateUserContext('digit_collection', 'system', `Collect digits requested (${payload.profile}): expecting ${payload.min_digits}-${payload.max_digits} digits.`);
      }
      markDigitPrompted(callSid, gptService, 0, 'dtmf', { allowCallEnd: true, prompt_text: instruction });
      scheduleDigitTimeout(callSid, gptService, 0);
    } catch (err) {
      logger.error('collect_digits handler error:', err);
    }
    return payload;
  }

  async function requestDigitCollectionPlan(callSid, args = {}, gptService = null) {
    const steps = Array.isArray(args.steps) ? args.steps : [];
    if (!steps.length) {
      return { error: 'No steps provided' };
    }

    if (digitCollectionPlans.has(callSid)) {
      clearDigitPlan(callSid);
    }
    if (isCircuitOpen()) {
      const payload = normalizeDigitExpectation({ ...steps[0] });
      await handleCircuitFallback(callSid, payload, true, false, 'system');
      return { error: 'circuit_open' };
    }
    setCallDigitIntent(callSid, { mode: 'dtmf', reason: 'tool_plan', confidence: 1 });
    digitCollectionManager.expectations.delete(callSid);
    clearDigitTimeout(callSid);
    clearDigitFallbackState(callSid);

    const callConfig = callConfigurations.get(callSid);
    const normalizedSteps = steps.map((step) => {
      const normalized = { ...step };
      if (normalized.profile) {
        const normalizedProfile = normalizeProfileId(normalized.profile);
        if (!isSupportedProfile(normalizedProfile)) {
          logDigitMetric('plan_step_invalid_profile', { profile: normalized.profile });
          normalized.profile = 'generic';
        } else {
          normalized.profile = normalizedProfile;
        }
      }
      if (!normalized.profile) {
        const hint = [step.prompt, step.label, step.name].filter(Boolean).join(' ');
        if (hint) {
          const inferred = inferDigitExpectationFromText(hint, callConfig);
          if (inferred && (inferred.confidence || 0) >= MIN_INFER_CONFIDENCE) {
            normalized.profile = inferred.profile;
            if (typeof normalized.min_digits !== 'number' && typeof inferred.min_digits === 'number') {
              normalized.min_digits = inferred.min_digits;
            }
            if (typeof normalized.max_digits !== 'number' && typeof inferred.max_digits === 'number') {
              normalized.max_digits = inferred.max_digits;
            }
            if (typeof normalized.force_exact_length !== 'number' && typeof inferred.force_exact_length === 'number') {
              normalized.force_exact_length = inferred.force_exact_length;
            }
          }
        }
      }
      return normalized;
    });
    const stepsToUse = normalizedSteps;
    const lockedExpectation = resolveLockedExpectation(callConfig);
    if (lockedExpectation?.profile) {
      const mismatched = stepsToUse.some((step) => step.profile && String(step.profile).toLowerCase() !== lockedExpectation.profile);
      if (mismatched || stepsToUse.length > 1) {
        webhookService.addLiveEvent(callSid, `🔒 Digit profile locked to ${lockedExpectation.profile} (plan rejected)`, { force: true });
        return { error: 'profile_locked', expected: lockedExpectation.profile };
      }
      if (stepsToUse.length === 1 && !stepsToUse[0].profile) {
        stepsToUse[0].profile = lockedExpectation.profile;
      }
    }

    const lastStep = stepsToUse[stepsToUse.length - 1] || {};
    const planEndOnSuccess = typeof args.end_call_on_success === 'boolean'
      ? args.end_call_on_success
      : true;
    const plan = {
      id: `plan_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`,
      steps: stepsToUse,
      index: 0,
      active: true,
      end_call_on_success: planEndOnSuccess,
      completion_message: typeof args.completion_message === 'string' ? args.completion_message.trim() : '',
      created_at: new Date().toISOString(),
      last_completed_step: null,
      last_completed_fingerprint: null,
      last_completed_at: null
    };

    digitCollectionPlans.set(callSid, plan);
    logDigitMetric('plan_started', {
      callSid,
      steps: stepsToUse.length,
      profiles: stepsToUse.map((step) => step.profile || 'generic')
    });
    await db.updateCallState(callSid, 'digit_collection_plan_started', {
      steps: stepsToUse.map((step) => step.profile || 'generic'),
      total_steps: stepsToUse.length
    }).catch(() => {});

    await startNextDigitPlanStep(callSid, plan, gptService, 0);
    return { status: 'started', steps: stepsToUse.length };
  }

  async function handleCollectionResult(callSid, collection, gptService = null, interactionCount = 0, source = 'dtmf', options = {}) {
    recordCircuitAttempt();
    try {
      if (isCircuitOpen()) {
        const expectation = digitCollectionManager.expectations.get(callSid);
        await handleCircuitFallback(callSid, expectation, options?.allowCallEnd === true, options?.deferCallEnd === true, source);
        return;
      }
      if (!collection) return;
      const allowCallEnd = options.allowCallEnd === true;
      const deferCallEnd = options.deferCallEnd === true;
      const expectation = digitCollectionManager.expectations.get(callSid);
    const shouldEndCall = allowCallEnd && expectation?.end_call_on_success !== false;
    const expectedLabel = expectation ? buildExpectedLabel(expectation) : 'the code';
    const stepTitle = formatPlanStepLabel(expectation);
    const stepPrefix = stepTitle ? `${stepTitle} — ` : '';
    const payload = {
      profile: collection.profile,
      raw_digits: collection.digits,
      masked: collection.masked,
      len: collection.len,
      route: collection.route || null,
      accepted: !!collection.accepted,
      retries: collection.retries || 0,
      fallback: !!collection.fallback,
      reason: collection.reason || null,
      heuristic: collection.heuristic || null
    };

    const source = collection.source || 'dtmf';
    const candidate = buildDigitCandidate(collection, expectation, source);
    collection.confidence = candidate.confidence;
    collection.confidence_signals = candidate.signals;
    collection.confidence_reason_codes = candidate.reasonCodes;
    updateSessionState(callSid, { lastCandidate: candidate });
    void emitAuditEvent(callSid, 'DigitCandidateProduced', {
      profile: collection.profile,
      len: collection.len,
      source,
      confidence: collection.confidence,
      signals: collection.confidence_signals,
      reason: collection.reason || null,
      masked: collection.masked
    });

    if (collection.accepted && candidate.confidence < 0.45) {
      collection.accepted = false;
      collection.reason = 'low_confidence';
      const exp = digitCollectionManager.expectations.get(callSid);
      if (exp) {
        exp.retries = (exp.retries || 0) + 1;
        collection.retries = exp.retries;
        if (exp.retries > exp.max_retries) {
          collection.fallback = true;
        }
        digitCollectionManager.expectations.set(callSid, exp);
      }
    }

    if (collection.accepted) {
      const fingerprint = buildCollectionFingerprint(collection, expectation);
      const lastAccepted = recentAccepted.get(callSid);
      if (lastAccepted && lastAccepted.fingerprint === fingerprint && Date.now() - lastAccepted.at < 2500) {
        logDigitMetric('duplicate_accept_ignored', {
          callSid,
          profile: collection.profile,
          len: collection.len,
          source
        });
        return;
      }
      recentAccepted.set(callSid, { fingerprint, at: Date.now() });
    }

    try {
      await db.updateCallState(callSid, 'digits_collected', {
        ...payload,
        masked_last4: collection.masked
      });
      await db.addCallDigitEvent({
        call_sid: callSid,
        source,
        profile: collection.profile,
        digits: collection.digits,
        len: collection.len,
        accepted: collection.accepted,
        reason: collection.reason,
        metadata: {
          masked: collection.masked,
          route: collection.route || null,
          heuristic: collection.heuristic || null,
          confidence: collection.confidence,
          confidence_signals: collection.confidence_signals,
          confidence_reasons: collection.confidence_reason_codes
        }
      });
    } catch (err) {
      logger.error('Error logging digits_collected:', err);
    }

    logDigitMetric('collection_result', {
      callSid,
      profile: collection.profile,
      len: collection.len,
      accepted: collection.accepted,
      reason: collection.reason || null,
      retries: collection.retries || 0,
      source,
      confidence: collection.confidence
    });

    const liveMasked = maskDigitsForPreview(collection.digits || collection.masked || '');
    const liveLabel = labelForProfile(collection.profile);
    if (collection.reason === 'incomplete') {
      const progressMax = expectation?.max_digits || '';
      const progress = progressMax ? ` (${collection.len}/${progressMax})` : '';
      webhookService.addLiveEvent(callSid, `🔢 ${stepPrefix}${liveLabel} progress: ${liveMasked}${progress}`, { force: true });
    } else if (collection.accepted) {
      webhookService.addLiveEvent(callSid, `✅ ${stepPrefix}${liveLabel} captured: ${liveMasked}`, { force: true });
    } else {
      const hint = collection.reason ? ` (${collection.reason.replace(/_/g, ' ')})` : '';
      webhookService.addLiveEvent(callSid, `⚠️ ${stepPrefix}${liveLabel} invalid${hint}: ${liveMasked}`, { force: true });
    }

    if (!collection.accepted && collection.reason === 'incomplete') {
      void emitAuditEvent(callSid, 'DigitCaptureFailed', {
        profile: collection.profile,
        len: collection.len,
        source,
        reason: collection.reason,
        confidence: collection.confidence,
        signals: collection.confidence_signals,
        masked: collection.masked
      });
      if (collection.profile === 'verification' || collection.profile === 'otp') {
        const progress = formatOtpForDisplay(collection.digits, 'progress', expectation?.max_digits);
        webhookService.addLiveEvent(callSid, `🔢 ${progress}`, { force: true });
      }
      recordCallerAffect(callSid, 'partial_input');
      scheduleDigitTimeout(callSid, gptService, interactionCount + 1);
      return;
    }

    const personalityInfo = gptService?.personalityEngine?.getCurrentPersonality();
    const emitReply = (text) => {
      if (!gptService || !text) return;
      const reply = {
        partialResponseIndex: null,
        partialResponse: text,
        personalityInfo,
        adaptationHistory: gptService.personalityChanges?.slice(-3) || []
      };
      gptService.emit('gptreply', reply, interactionCount);
      try {
        gptService.updateUserContext('system', 'system', `Digit handling note: ${text}`);
      } catch (_) {}
    };

    if (collection.accepted) {
      void emitAuditEvent(callSid, 'DigitCaptureSucceeded', {
        profile: collection.profile,
        len: collection.len,
        source,
        masked: collection.masked,
        confidence: collection.confidence
      });
      recordIntentHistory(callSid, collection.profile);
      const riskAction = expectation?.risk_action === 'route_to_agent';
      clearDigitTimeout(callSid);
      clearDigitFallbackState(callSid);
      digitCollectionManager.expectations.delete(callSid);
      if (stepTitle) {
        webhookService.addLiveEvent(callSid, `✅ ${stepTitle} validated`, { force: true });
      }
      const profile = String(collection.profile || '').toLowerCase();
      switch (profile) {
        case 'extension':
          break;
        case 'verification':
        case 'otp':
          webhookService.addLiveEvent(callSid, `✅ ${formatOtpForDisplay(collection.digits, showRawDigitsLive ? 'length' : 'masked')}`, { force: true });
          await db.updateCallState(callSid, 'identity_confirmed', {
            method: 'digits',
            note: `${collection.profile} digits confirmed (masked)`,
            masked: collection.masked
          }).catch(() => {});
          await db.updateCallStatus(callSid, 'in-progress', {
            last_otp: collection.digits,
            last_otp_masked: collection.masked
          }).catch(() => {});
          await db.updateCallState(callSid, 'otp_captured', {
            masked: collection.masked,
            len: collection.len
          }).catch(() => {});
          break;
        case 'account':
          webhookService.addLiveEvent(callSid, `🏷️ Account number captured (ending ${collection.masked.slice(-4)})`, { force: true });
          await db.updateCallState(callSid, 'account_number_captured', {
            masked_last4: collection.masked,
            len: collection.len
          }).catch(() => {});
          break;
        case 'zip':
          webhookService.addLiveEvent(callSid, `📮 ZIP captured`, { force: true });
          await db.updateCallState(callSid, 'zip_captured', {
            masked: collection.masked,
            len: collection.len
          }).catch(() => {});
          break;
        case 'amount': {
          const amountCents = Number(collection.digits);
          const dollars = (amountCents / 100).toFixed(2);
          webhookService.addLiveEvent(callSid, `💵 Amount entered: $${dollars}`, { force: true });
          await db.updateCallState(callSid, 'amount_captured', {
            amount_cents: amountCents,
            amount_display: `$${dollars}`
          }).catch(() => {});
          break;
        }
        case 'account_number':
          webhookService.addLiveEvent(callSid, '🏦 Account number captured', { force: true });
          await db.updateCallState(callSid, 'account_number_captured', {
            masked_last4: collection.masked,
            len: collection.len
          }).catch(() => {});
          break;
        case 'callback_confirm':
          webhookService.addLiveEvent(callSid, `📞 Callback number confirmed (ending ${collection.masked.slice(-4)})`, { force: true });
          await db.updateCallState(callSid, 'callback_confirmed', {
            masked_last4: collection.masked,
            raw_digits: collection.digits
          }).catch(() => {});
          break;
        case 'card_number':
          webhookService.addLiveEvent(callSid, `💳 Card number captured (${collection.len})`, { force: true });
          await db.updateCallState(callSid, 'card_number_captured', {
            card_number: collection.digits,
            last4: collection.digits ? collection.digits.slice(-4) : null
          }).catch(() => {});
          break;
        case 'cvv':
          webhookService.addLiveEvent(callSid, `🔐 CVV captured (${collection.len})`, { force: true });
          await db.updateCallState(callSid, 'cvv_captured', {
            cvv: collection.digits
          }).catch(() => {});
          break;
        case 'card_expiry':
          webhookService.addLiveEvent(callSid, `📅 Expiry captured (${collection.digits})`, { force: true });
          await db.updateCallState(callSid, 'card_expiry_captured', {
            expiry: collection.digits
          }).catch(() => {});
          break;
        case 'ssn':
          webhookService.addLiveEvent(callSid, '🪪 SSN captured', { force: true });
          await db.updateCallState(callSid, 'ssn_captured', {
            masked_last4: collection.masked,
            len: collection.len
          }).catch(() => {});
          break;
        case 'dob':
          webhookService.addLiveEvent(callSid, '🎂 DOB captured', { force: true });
          await db.updateCallState(callSid, 'dob_captured', {
            masked: collection.masked,
            len: collection.len
          }).catch(() => {});
          break;
        case 'routing_number':
          webhookService.addLiveEvent(callSid, '🏦 Routing number captured', { force: true });
          await db.updateCallState(callSid, 'routing_number_captured', {
            masked_last4: collection.masked,
            len: collection.len
          }).catch(() => {});
          break;
        case 'phone':
          webhookService.addLiveEvent(callSid, '📱 Phone number captured', { force: true });
          await db.updateCallState(callSid, 'phone_number_captured', {
            masked_last4: collection.masked,
            len: collection.len
          }).catch(() => {});
          break;
        case 'tax_id':
          webhookService.addLiveEvent(callSid, '🧾 Tax ID captured', { force: true });
          await db.updateCallState(callSid, 'tax_id_captured', {
            masked_last4: collection.masked,
            len: collection.len
          }).catch(() => {});
          break;
        case 'ein':
          webhookService.addLiveEvent(callSid, '🏢 EIN captured', { force: true });
          await db.updateCallState(callSid, 'ein_captured', {
            masked_last4: collection.masked,
            len: collection.len
          }).catch(() => {});
          break;
        case 'claim_number':
          webhookService.addLiveEvent(callSid, '🧾 Claim number captured', { force: true });
          await db.updateCallState(callSid, 'claim_number_captured', {
            masked_last4: collection.masked,
            len: collection.len
          }).catch(() => {});
          break;
        case 'reservation_number':
          webhookService.addLiveEvent(callSid, '🧾 Reservation number captured', { force: true });
          await db.updateCallState(callSid, 'reservation_number_captured', {
            masked_last4: collection.masked,
            len: collection.len
          }).catch(() => {});
          break;
        case 'ticket_number':
          webhookService.addLiveEvent(callSid, '🧾 Ticket number captured', { force: true });
          await db.updateCallState(callSid, 'ticket_number_captured', {
            masked_last4: collection.masked,
            len: collection.len
          }).catch(() => {});
          break;
        case 'case_number':
          webhookService.addLiveEvent(callSid, '🧾 Case number captured', { force: true });
          await db.updateCallState(callSid, 'case_number_captured', {
            masked_last4: collection.masked,
            len: collection.len
          }).catch(() => {});
          break;
        default:
          webhookService.addLiveEvent(callSid, `🔢 Digits captured (${collection.len})`, { force: true });
      }
      const planId = expectation?.plan_id;
      if (planId && digitCollectionPlans.has(callSid)) {
        const plan = digitCollectionPlans.get(callSid);
        if (plan?.id === planId && plan.active) {
          const fingerprint = buildCollectionFingerprint(collection, expectation);
          if (
            plan.last_completed_step === expectation.plan_step_index
            && plan.last_completed_fingerprint === fingerprint
            && plan.last_completed_at
            && Date.now() - plan.last_completed_at < 3000
          ) {
            logDigitMetric('duplicate_step_ignored', {
              callSid,
              profile: collection.profile,
              step: expectation.plan_step_index,
              plan_id: planId
            });
            return;
          }
          plan.last_completed_step = expectation.plan_step_index;
          plan.last_completed_fingerprint = fingerprint;
          plan.last_completed_at = Date.now();
          plan.index += 1;
          if (plan.index < plan.steps.length) {
            await startNextDigitPlanStep(callSid, plan, gptService, interactionCount + 1);
            return;
          }
          plan.active = false;
          digitCollectionPlans.delete(callSid);
          webhookService.addLiveEvent(callSid, '✅ Digit collection plan completed', { force: true });
          await db.updateCallState(callSid, 'digit_collection_plan_completed', {
            steps: plan.steps.length,
            completed_at: new Date().toISOString()
          }).catch(() => {});
          if (riskAction) {
            await routeToAgentOnRisk(callSid, expectation, collection, allowCallEnd, deferCallEnd);
            return;
          }
          const planShouldEnd = allowCallEnd && plan.end_call_on_success !== false;
          if (planShouldEnd) {
            const completionMessage = plan.completion_message
              || buildClosingMessage(collection.profile || expectation?.profile)
              || closingMessage;
            if (deferCallEnd) {
              return;
            }
            await speakAndEndCall(callSid, completionMessage, 'digits_collected_plan');
            return;
          }
          clearDigitIntent(callSid, 'digit_plan_completed');
          if (gptService) {
            const completionMessage = plan.completion_message || 'Thanks, I have all the digits I need.';
            emitReply(completionMessage);
          }
          return;
        }
      }

      if (riskAction) {
        await routeToAgentOnRisk(callSid, expectation, collection, allowCallEnd, deferCallEnd);
        return;
      }
      if (shouldEndCall) {
        if (deferCallEnd) {
          return;
        }
        const completionMessage = buildClosingMessage(collection.profile || expectation?.profile) || closingMessage;
        await speakAndEndCall(
          callSid,
          completionMessage,
          (collection.profile === 'verification' || collection.profile === 'otp') ? 'otp_verified' : 'digits_collected'
        );
        return;
      }
      clearDigitIntent(callSid);
      const confirmation = buildConfirmationMessage(expectation || {}, collection);
      if (confirmation) {
        emitReply(confirmation);
        void emitAuditEvent(callSid, 'DigitCaptureConfirmed', {
          profile: collection.profile,
          len: collection.len,
          source
        });
      }
      return;
    } else {
      void emitAuditEvent(callSid, 'DigitCaptureFailed', {
        profile: collection.profile,
        len: collection.len,
        source,
        reason: collection.reason,
        confidence: collection.confidence,
        signals: collection.confidence_signals,
        masked: collection.masked
      });
      const reasonHint = collection.reason ? ` (${collection.reason.replace(/_/g, ' ')})` : '';
      webhookService.addLiveEvent(callSid, `⚠️ Invalid digits (${collection.len})${reasonHint}; retry ${collection.retries}/${digitCollectionManager.expectations.get(callSid)?.max_retries || 0}`, { force: true });
      if (collection.fallback) {
        if (expectation?.allow_sms_fallback && shouldUseSmsFallback(expectation, collection)) {
          const session = await createSmsSession(callSid, expectation, collection.reason || 'fallback');
          if (session) {
            expectation.sms_fallback_used = true;
            expectation.channel = 'sms';
            digitCollectionManager.expectations.set(callSid, expectation);
            webhookService.addLiveEvent(callSid, '📩 SMS fallback sent for digit capture', { force: true });
            void emitAuditEvent(callSid, 'DigitCaptureAborted', {
              profile: expectation.profile,
              source,
              reason: 'sms_fallback'
            });
            if (allowCallEnd) {
              if (!deferCallEnd) {
                await speakAndEndCall(callSid, smsFallbackMessage, 'digits_sms_fallback');
              }
              return;
            }
            emitReply(smsFallbackMessage);
            return;
          }
        }
        const failureMessage = expectation?.failure_message || callEndMessages.failure || 'I could not verify the digits. Thank you for your time.';
        const allowSpokenFallback = expectation?.allow_spoken_fallback !== false;
        const shouldFallbackToVoice = fallbackToVoiceOnFailure && allowSpokenFallback;
        const fallbackMsg = shouldFallbackToVoice
          ? 'I could not verify the digits. I will continue the call without keypad entry.'
          : failureMessage;
        webhookService.addLiveEvent(callSid, `⏳ No valid digits; ${shouldFallbackToVoice ? 'switching to voice' : 'ending call'}`, { force: true });
        digitCollectionManager.expectations.delete(callSid);
        clearDigitTimeout(callSid);
        clearDigitFallbackState(callSid);
        clearDigitPlan(callSid);
        void emitAuditEvent(callSid, 'DigitCaptureAborted', {
          profile: expectation?.profile || collection.profile,
          source,
          reason: shouldFallbackToVoice ? 'voice_fallback' : 'max_retries'
        });
        if (shouldFallbackToVoice) {
          clearDigitIntent(callSid, 'digit_collection_failed');
          emitReply(fallbackMsg);
          return;
        }
        if (allowCallEnd) {
          if (deferCallEnd) {
            return;
          }
          await speakAndEndCall(callSid, failureMessage, 'digit_collection_failed');
          return;
        }
        emitReply(fallbackMsg);
      } else {
        const affect = recordCallerAffect(callSid, collection.reason || 'invalid');
        const policy = buildRetryPolicy({
          reason: collection.reason || 'invalid',
          attempt: collection.retries || 1,
          source,
          expectation,
          affect,
          session: getSessionState(callSid),
          health: getSystemHealth(callSid)
        });
        let prompt = policy.prompt;
        if (!prompt) {
          if (collection.reason === 'too_short') {
            prompt = chooseReprompt(expectation || {}, 'incomplete', collection.retries || 1)
              || `Please enter the ${expectedLabel} now.`;
          } else {
            prompt = chooseReprompt(expectation || {}, 'invalid', collection.retries || 1)
              || `Please enter the ${expectedLabel} now.`;
          }
        }
        if (policy.forceDtmfOnly && expectation) {
          expectation.allow_spoken_fallback = false;
          digitCollectionManager.expectations.set(callSid, expectation);
        }
        if (policy.delayMs && gptService) {
          await sleep(policy.delayMs);
        }
        emitReply(prompt);
        if (gptService) {
          markDigitPrompted(callSid, gptService, interactionCount, 'dtmf', { prompt_text: prompt });
          scheduleDigitTimeout(callSid, gptService, interactionCount + 1);
        }
      }
    }

    const summary = collection.accepted
      ? collection.route
        ? `✅ Digits accepted • routed: ${collection.route}`
        : (collection.profile === 'verification' || collection.profile === 'otp')
          ? `✅ ${formatOtpForDisplay(collection.digits, showRawDigitsLive ? 'length' : 'masked')}`
          : `✅ Digits accepted (${collection.len})`
      : collection.fallback
        ? '⚠️ Digits failed after retries'
        : `⚠️ Invalid digits (${collection.len}); retry ${collection.retries}/${digitCollectionManager.expectations.get(callSid)?.max_retries || 0}`;
    webhookService.addLiveEvent(callSid, summary, { force: true });
    } catch (err) {
      recordCircuitError();
      logDigitMetric('digit_service_error', { callSid, error: err.message });
      throw err;
    }
  }

  function clearCallState(callSid) {
    if (!smsSessions.has(callSid)) {
      digitCollectionManager.expectations.delete(callSid);
      clearDigitPlan(callSid);
    }
    clearDigitTimeout(callSid);
    clearDigitFallbackState(callSid);
    lastDtmfTimestamps.delete(callSid);
    pendingDigits.delete(callSid);
    recentAccepted.delete(callSid);  // Add missing cleanup to prevent memory leak
    sessionState.delete(callSid);
    intentHistory.delete(callSid);
    riskSignals.delete(callSid);
    clearSmsSession(callSid);
    const callConfig = callConfigurations.get(callSid);
    if (callConfig) {
      callConfig.digit_capture_active = false;
      if (callConfig.digit_intent?.mode === 'dtmf') {
        callConfig.digit_intent = { mode: 'normal', reason: 'call_end', confidence: 1 };
      }
      callConfigurations.set(callSid, callConfig);
    }
    logDigitMetric('call_state_cleared', { callSid, timestamp: Date.now() });
  }

  async function handleIncomingSms(from, body) {
    const session = getSmsSessionByPhone(from);
    if (!session || !session.active) {
      return { handled: false };
    }
    const digits = parseDigitsFromText(body);
    if (!digits) {
      if (smsService) {
        await smsService.sendSMS(session.phone, 'Please reply with digits only.', null, {
          idempotencyKey: `${session.callSid}:sms-nodigits:${Date.now()}`
        });
      }
      return { handled: true, reason: 'no_digits' };
    }
    const callSid = session.callSid;
    if (!digitCollectionManager.expectations.has(callSid)) {
      digitCollectionManager.setExpectation(callSid, { ...session.expectation, channel: 'sms' });
    }
    const collection = digitCollectionManager.recordDigits(callSid, digits, { source: 'sms', timestamp: Date.now() });
    await handleCollectionResult(callSid, collection, null, 0, 'sms', { allowCallEnd: false, deferCallEnd: true });
    session.attempts += 1;
    smsSessions.set(callSid, session);
    const reply = buildSmsReplyForResult(collection);
    if (smsService && reply) {
      await smsService.sendSMS(session.phone, reply, null, {
        idempotencyKey: `${callSid}:sms-reply:${session.attempts}`
      });
    }
    if (collection.accepted) {
      const plan = digitCollectionPlans.get(callSid);
      if (!plan || !plan.active) {
        clearSmsSession(callSid);
      }
    } else if (collection.fallback) {
      clearSmsSession(callSid);
      digitCollectionManager.expectations.delete(callSid);
    }
    return { handled: true, collection };
  }

  return {
    expectations: digitCollectionManager.expectations,
    buildDigitPrompt,
    buildTwilioGatherTwiml,
    clearCallState,
    clearDigitFallbackState,
    clearDigitPlan,
    clearDigitTimeout,
    determineDigitIntent,
    handleIncomingSms,
    formatDigitsGeneral,
    formatOtpForDisplay,
    getExpectation: (callSid) => digitCollectionManager.expectations.get(callSid),
    getOtpContext,
    handleCollectionResult,
    hasExpectation: (callSid) => digitCollectionManager.expectations.has(callSid),
    inferDigitExpectationFromText,
    markDigitPrompted,
    updatePromptDelay,
    maskOtpForExternal,
    normalizeDigitExpectation,
    bufferDigits,
    flushBufferedDigits,
    prepareInitialExpectation,
    recordDigits: (callSid, digits, meta) => digitCollectionManager.recordDigits(callSid, digits, meta),
    requestDigitCollection,
    requestDigitCollectionPlan,
    scheduleDigitTimeout,
    setExpectation: (callSid, params) => digitCollectionManager.setExpectation(callSid, params),
    isFallbackActive: (callSid) => digitFallbackStates.get(callSid)?.active === true,
    hasPlan: (callSid) => digitCollectionPlans.has(callSid),
    buildClosingMessage
  };
}

module.exports = {
  createDigitCollectionService
};
