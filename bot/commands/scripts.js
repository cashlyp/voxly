const axios = require('axios');
const config = require('../config');
const httpClient = require('../utils/httpClient');
const { withRetry } = require('../utils/httpClient');
const {
  getUser,
  isAdmin,
  saveScriptVersion,
  listScriptVersions,
  getScriptVersion
} = require('../db/db');
const {
  getBusinessOptions,
  findBusinessOption,
  MOOD_OPTIONS,
  URGENCY_OPTIONS,
  TECH_LEVEL_OPTIONS,
  askOptionWithButtons,
  getOptionLabel,
  invalidatePersonaCache
} = require('../utils/persona');
const { extractScriptVariables } = require('../utils/scripts');
const {
  startOperation,
  ensureOperationActive,
  OperationCancelledError,
  getCurrentOpId,
  guardAgainstCommandInterrupt
} = require('../utils/sessionState');
const { emailTemplatesFlow } = require('./email');
const { section, buildLine, tipLine } = require('../utils/ui');
const { attachHmacAuth } = require('../utils/apiAuth');

const scriptsApi = axios.create({
  baseURL: config.scriptsApiUrl.replace(/\/+$/, ''),
  timeout: 15000,
  headers: {
    'Content-Type': 'application/json',
    'x-admin-token': config.admin.apiToken
  }
});

attachHmacAuth(scriptsApi, {
  secret: config.apiAuth?.hmacSecret,
  allowedOrigins: [new URL(config.scriptsApiUrl).origin],
  defaultBaseUrl: config.scriptsApiUrl
});

function styledNotice(ctx, title, lines) {
  const content = Array.isArray(lines) ? lines : [lines];
  return ctx.reply(section(title, content));
}

function nonJsonResponseError(endpoint, response) {
  const contentType = response?.headers?.['content-type'] || 'unknown';
  const snippet =
    typeof response?.data === 'string'
      ? response.data.replace(/\s+/g, ' ').trim().slice(0, 140)
      : '';
  const error = new Error(
    `Scripts API returned non-JSON response (content-type: ${contentType})`
  );
  error.isScriptsApiError = true;
  error.reason = 'non_json_response';
  error.endpoint = endpoint;
  error.contentType = contentType;
  error.snippet = snippet;
  return error;
}

async function scriptsApiRequest(options) {
  const endpoint = `${(options.method || 'GET').toUpperCase()} ${options.url}`;
  try {
    const response = await withRetry(() => scriptsApi.request(options), options.retry || {});
    const contentType = response.headers?.['content-type'] || '';
    if (!contentType.includes('application/json')) {
      throw nonJsonResponseError(endpoint, response);
    }
    if (response.data && response.data.success === false) {
      const apiError = new Error(response.data.error || 'Scripts API reported failure');
      apiError.isScriptsApiError = true;
      apiError.reason = 'api_failure';
      apiError.endpoint = endpoint;
      throw apiError;
    }
    return response.data;
  } catch (error) {
    if (error.response) {
      const contentType = error.response.headers?.['content-type'] || '';
      if (!contentType.includes('application/json')) {
        throw nonJsonResponseError(endpoint, error.response);
      }
    }
    error.scriptsApi = { endpoint };
    throw error;
  }
}

function formatScriptsApiError(error, action) {
  const baseHelp = `Ensure the scripts service is reachable at ${config.scriptsApiUrl} or update SCRIPTS_API_URL.`;

  const apiCode = error.response?.data?.code || error.code;
  if (apiCode === 'SCRIPT_NAME_DUPLICATE') {
    const suggested = error.response?.data?.suggested_name;
    const suggestionLine = suggested ? ` Suggested name: ${suggested}` : '';
    return `â ï¸ ${action}: Script name already exists.${suggestionLine}`;
  }

  if (error.isScriptsApiError && error.reason === 'non_json_response') {
    return `â ${action}: Scripts API returned unexpected content (type: ${error.contentType}). ${baseHelp}${
      error.snippet ? `\nSnippet: ${error.snippet}` : ''
    }`;
  }

  if (error.isScriptsApiError && error.reason === 'api_failure') {
    return `â ${action}: ${error.message}. ${baseHelp}`;
  }

  if (error.response) {
    const status = error.response.status;
    const statusText = error.response.statusText || '';
    const details =
      error.response.data?.error ||
      error.response.data?.message ||
      error.message;

    const contentType = error.response.headers?.['content-type'] || '';
    if (!contentType.includes('application/json')) {
      const snippet =
        typeof error.response.data === 'string'
          ? error.response.data.replace(/\s+/g, ' ').trim().slice(0, 140)
          : '';
      return `â ${action}: Scripts API responded with HTTP ${status} ${statusText}. ${baseHelp}${
        snippet ? `\nSnippet: ${snippet}` : ''
      }`;
    }

    return `â ${action}: ${details || `HTTP ${status}`}`;
  }

  if (error.request) {
    return `â ${action}: No response from Scripts API. ${baseHelp}`;
  }

  return `â ${action}: ${error.message}`;
}

const CANCEL_KEYWORDS = new Set(['cancel', 'exit', 'quit']);

function isCancelInput(text) {
  return typeof text === 'string' && CANCEL_KEYWORDS.has(text.trim().toLowerCase());
}

function escapeMarkdown(text = '') {
  return text.replace(/([_*[\]`])/g, '\\$1');
}

function normalizeScriptName(name = '') {
  return String(name || '').trim().slice(0, 80);
}

function buildDigitCaptureSummary(script = {}) {
  const requiresOtp = !!script.requires_otp;
  const defaultProfile = script.default_profile;
  const expectedLength = script.expected_length;
  const parts = [];
  if (requiresOtp) parts.push('OTP required');
  if (defaultProfile) parts.push(`Profile: ${defaultProfile}`);
  if (expectedLength) parts.push(`Len: ${expectedLength}`);
  if (!parts.length) return 'None';
  return parts.join(' â¢ ');
}

function validateCallScriptPayload(payload = {}) {
  const errors = [];
  const warnings = [];
  const name = normalizeScriptName(payload.name);
  if (!name) {
    errors.push('Script name is required.');
  } else if (name.length < 3) {
    warnings.push('Name is very short; consider a clearer label.');
  }
  if (!payload.first_message || !String(payload.first_message).trim()) {
    errors.push('First message is required.');
  }
  if (!payload.prompt || !String(payload.prompt).trim()) {
    warnings.push('Prompt is empty; normal call flow may sound generic.');
  }
  if (payload.requires_otp) {
    const len = Number(payload.expected_length);
    if (!Number.isFinite(len) || len < 4 || len > 8) {
      errors.push('OTP length must be between 4 and 8 digits.');
    }
  }
  if (payload.default_profile && payload.expected_length) {
    const len = Number(payload.expected_length);
    if (!Number.isFinite(len) || len < 1) {
      errors.push('Expected length must be a positive number.');
    }
  }
  if (payload.capture_group) {
    const promptText = `${payload.prompt || ''} ${payload.first_message || ''}`.toLowerCase();
    const keyword = payload.capture_group === 'banking' ? 'bank' : 'card';
    if (!promptText.includes(keyword)) {
      warnings.push(`Capture group "${payload.capture_group}" is set but the prompt does not mention "${keyword}".`);
    }
  }
  return { errors, warnings };
}

function replacePlaceholders(text = '', values = {}) {
  let output = text;
  for (const [token, value] of Object.entries(values)) {
    const pattern = new RegExp(`{${token}}`, 'g');
    output = output.replace(pattern, value);
  }
  return output;
}

function buildCallScriptSnapshot(script = {}) {
  return {
    name: script.name,
    description: script.description ?? null,
    business_id: script.business_id ?? null,
    persona_config: script.persona_config ?? null,
    prompt: script.prompt ?? null,
    first_message: script.first_message ?? null,
    voice_model: script.voice_model ?? null,
    requires_otp: !!script.requires_otp,
    default_profile: script.default_profile ?? null,
    expected_length: script.expected_length ?? null,
    allow_terminator: !!script.allow_terminator,
    terminator_char: script.terminator_char ?? null,
    capture_group: script.capture_group ?? null
  };
}

function buildSmsScriptSnapshot(script = {}) {
  return {
    name: script.name,
    description: script.description ?? null,
    content: script.content ?? null,
    metadata: script.metadata ?? null
  };
}

async function storeScriptVersionSnapshot(script, type, ctx) {
  try {
    const payload = type === 'sms' ? buildSmsScriptSnapshot(script) : buildCallScriptSnapshot(script);
    await saveScriptVersion(type === 'sms' ? script.name : script.id, type, payload, ctx.from?.id?.toString?.());
  } catch (error) {
    console.warn('Failed to store script version:', error?.message || error);
  }
}

function stripUndefined(payload = {}) {
  return Object.fromEntries(Object.entries(payload).filter(([, value]) => value !== undefined));
}

async function promptText(
  conversation,
  ctx,
  message,
  {
    allowEmpty = false,
    allowSkip = false,
    defaultValue = null,
    parse = (value) => value,
    ensureActive
  } = {}
) {
  const safeEnsureActive = typeof ensureActive === 'function'
    ? ensureActive
    : () => ensureOperationActive(ctx, getCurrentOpId(ctx));
  const hints = [];
  if (defaultValue !== null && defaultValue !== undefined && defaultValue !== '') {
    hints.push(`Current: ${defaultValue}`);
  }
  if (allowSkip) {
    hints.push('Type skip to keep current value');
  }
  hints.push('Type cancel to abort');

  const promptMessage = hints.length > 0 ? `${message}\n_${hints.join(' | ')}_` : message;
  await ctx.reply(promptMessage, { parse_mode: 'Markdown' });

  const response = await conversation.wait();
  safeEnsureActive();
  const text = response?.message?.text?.trim();
  if (text) {
    await guardAgainstCommandInterrupt(ctx, text);
  }

  if (!text) {
    if (allowEmpty) {
      return '';
    }
    return null;
  }

  if (isCancelInput(text)) {
    return null;
  }

  if (allowSkip && text.toLowerCase() === 'skip') {
    return undefined;
  }

  try {
    return parse(text);
  } catch (error) {
    await ctx.reply(`â ${error.message || 'Invalid value supplied.'}`);
    return null;
  }
}

async function confirm(conversation, ctx, prompt, ensureActive) {
  const safeEnsureActive = typeof ensureActive === 'function'
    ? ensureActive
    : () => ensureOperationActive(ctx, getCurrentOpId(ctx));
  const choice = await askOptionWithButtons(
    conversation,
    ctx,
    prompt,
    [
      { id: 'yes', label: 'â Yes' },
      { id: 'no', label: 'â No' }
    ],
    { prefix: 'confirm', columns: 2, ensureActive: safeEnsureActive }
  );
  return choice.id === 'yes';
}

async function collectPlaceholderValues(conversation, ctx, placeholders, ensureActive) {
  const safeEnsureActive = typeof ensureActive === 'function'
    ? ensureActive
    : () => ensureOperationActive(ctx, getCurrentOpId(ctx));
  const values = {};
  for (const placeholder of placeholders) {
    await ctx.reply(
      `âï¸ Enter value for *${escapeMarkdown(placeholder)}* (type skip to leave unchanged, cancel to abort).`,
      { parse_mode: 'Markdown' }
    );
    const response = await conversation.wait();
    safeEnsureActive();
    const text = response?.message?.text?.trim();
    if (text) {
      await guardAgainstCommandInterrupt(ctx, text);
    }
    if (!text) {
      continue;
    }
    if (isCancelInput(text)) {
      return null;
    }
    if (text.toLowerCase() === 'skip') {
      continue;
    }
    values[placeholder] = text;
  }
  return values;
}

function toPersonaOverrides(personaResult) {
  if (!personaResult) {
    return null;
  }

  const overrides = {};
  if (personaResult.business_id) {
    overrides.business_id = personaResult.business_id;
  }

  const persona = personaResult.persona_config || {};
  if (persona.purpose) {
    overrides.purpose = persona.purpose;
  }
  if (persona.emotion) {
    overrides.emotion = persona.emotion;
  }
  if (persona.urgency) {
    overrides.urgency = persona.urgency;
  }
  if (persona.technical_level) {
    overrides.technical_level = persona.technical_level;
  }

  return Object.keys(overrides).length ? overrides : null;
}

function buildPersonaSummaryFromConfig(script) {
  const summary = [];
  if (script.business_id) {
    const business = findBusinessOption(script.business_id);
    summary.push(`Persona: ${business ? business.label : script.business_id}`);
  }
  const persona = script.persona_config || {};
  if (persona.purpose) {
    summary.push(`Purpose: ${persona.purpose}`);
  }
  if (persona.emotion) {
    summary.push(`Tone: ${persona.emotion}`);
  }
  if (persona.urgency) {
    summary.push(`Urgency: ${persona.urgency}`);
  }
  if (persona.technical_level) {
    summary.push(`Technical level: ${persona.technical_level}`);
  }
  return summary;
}

function buildPersonaSummaryFromOverrides(overrides = {}) {
  if (!overrides) {
    return [];
  }

  const summary = [];
  if (overrides.business_id) {
    const business = findBusinessOption(overrides.business_id);
    summary.push(`Persona: ${business ? business.label : overrides.business_id}`);
  }
  if (overrides.purpose) {
    summary.push(`Purpose: ${overrides.purpose}`);
  }
  if (overrides.emotion) {
    summary.push(`Tone: ${overrides.emotion}`);
  }
  if (overrides.urgency) {
    summary.push(`Urgency: ${overrides.urgency}`);
  }
  if (overrides.technical_level) {
    summary.push(`Technical level: ${overrides.technical_level}`);
  }
  return summary;
}

async function collectPersonaConfig(conversation, ctx, defaults = {}, options = {}) {
  const { allowCancel = true, ensureActive } = options;
  const safeEnsureActive = typeof ensureActive === 'function'
    ? ensureActive
    : () => ensureOperationActive(ctx, getCurrentOpId(ctx));
  const businessOptions = await getBusinessOptions();
  safeEnsureActive();

  const personaSummary = [];
  let businessSelection = defaults.business_id
    ? businessOptions.find((option) => option.id === defaults.business_id)
    : null;

  const selectionOptions = businessOptions.map((option) => ({ ...option }));
  if (allowCancel) {
    selectionOptions.unshift({ id: 'cancel', label: 'â Cancel', custom: true });
  }

  const businessChoice = await askOptionWithButtons(
    conversation,
    ctx,
    `ð­ *Select persona for this script:*
Choose the primary business context.`,
    selectionOptions,
    {
      prefix: 'script-business',
      columns: 2,
      ensureActive: safeEnsureActive,
      formatLabel: (option) => (option.custom && option.id !== 'cancel' ? 'âï¸ Custom persona' : option.label)
    }
  );

  if (!businessChoice) {
    await ctx.reply('â Invalid persona selection. Please try again.');
    return null;
  }

  if (allowCancel && businessChoice.id === 'cancel') {
    return null;
  }

  businessSelection = businessChoice;

  const personaConfig = { ...(defaults.persona_config || {}) };

  if (businessSelection && !businessSelection.custom) {
    personaSummary.push(`Persona: ${businessSelection.label}`);
    const availablePurposes = businessSelection.purposes || [];

    if (availablePurposes.length > 0) {
      const currentPurposeLabel = personaConfig.purpose
        ? getOptionLabel(availablePurposes, personaConfig.purpose)
        : null;

      const purposePrompt = currentPurposeLabel
        ? `ð¯ *Choose script purpose:*
This helps align tone and follow-up actions.
_Current: ${currentPurposeLabel}_`
        : `ð¯ *Choose script purpose:*
This helps align tone and follow-up actions.`;

      const purposeSelection = await askOptionWithButtons(
        conversation,
        ctx,
        purposePrompt,
        availablePurposes,
        {
          prefix: 'script-purpose',
          columns: 1,
           ensureActive: safeEnsureActive,
          formatLabel: (option) => `${option.emoji || 'â¢'} ${option.label}`
        }
      );

      personaConfig.purpose = purposeSelection?.id || null;
      if (purposeSelection?.label) {
        personaSummary.push(`Purpose: ${purposeSelection.label}`);
      }
    }

    const tonePrompt = personaConfig.emotion
      ? `ðï¸ *Preferred tone for this script:*
_Current: ${getOptionLabel(MOOD_OPTIONS, personaConfig.emotion)}_`
      : `ðï¸ *Preferred tone for this script:*`;

    const moodSelection = await askOptionWithButtons(
      conversation,
      ctx,
      tonePrompt,
      MOOD_OPTIONS,
      { prefix: 'script-tone', columns: 2, ensureActive: safeEnsureActive }
    );
    personaConfig.emotion = moodSelection.id;
    personaSummary.push(`Tone: ${moodSelection.label}`);

    const urgencyPrompt = personaConfig.urgency
      ? `â±ï¸ *Default urgency:*
_Current: ${getOptionLabel(URGENCY_OPTIONS, personaConfig.urgency)}_`
      : `â±ï¸ *Default urgency:*`;

    const urgencySelection = await askOptionWithButtons(
      conversation,
      ctx,
      urgencyPrompt,
      URGENCY_OPTIONS,
      { prefix: 'script-urgency', columns: 2, ensureActive: safeEnsureActive }
    );
    personaConfig.urgency = urgencySelection.id;
    personaSummary.push(`Urgency: ${urgencySelection.label}`);

    const techPrompt = personaConfig.technical_level
      ? `ð§  *Recipient technical level:*
_Current: ${getOptionLabel(TECH_LEVEL_OPTIONS, personaConfig.technical_level)}_`
      : `ð§  *Recipient technical level:*`;

    const techSelection = await askOptionWithButtons(
      conversation,
      ctx,
      techPrompt,
      TECH_LEVEL_OPTIONS,
      { prefix: 'script-tech', columns: 2, ensureActive: safeEnsureActive }
    );
    personaConfig.technical_level = techSelection.id;
    personaSummary.push(`Technical level: ${techSelection.label}`);
  } else {
    personaSummary.push('Persona: Custom');
    personaConfig.purpose = personaConfig.purpose || null;
    personaConfig.emotion = personaConfig.emotion || null;
    personaConfig.urgency = personaConfig.urgency || null;
    personaConfig.technical_level = personaConfig.technical_level || null;
  }

  return {
    business_id: businessSelection && !businessSelection.custom ? businessSelection.id : null,
    persona_config: personaConfig,
    personaSummary
  };
}

async function collectPromptAndVoice(conversation, ctx, defaults = {}, ensureActive) {
  const safeEnsureActive = typeof ensureActive === 'function'
    ? ensureActive
    : () => ensureOperationActive(ctx, getCurrentOpId(ctx));
  const prompt = await promptText(
    conversation,
    ctx,
    'ð§  Provide the system prompt for this call script. This sets the AI behavior.',
    {
      allowEmpty: false,
      allowSkip: !!defaults.prompt,
      defaultValue: defaults.prompt,
      parse: (value) => value,
      ensureActive: safeEnsureActive
    }
  );

  if (prompt === null) {
    return null;
  }

  const firstMessage = await promptText(
    conversation,
    ctx,
    'ð£ï¸ Provide the first message the agent says when the call connects.',
    {
      allowEmpty: false,
      allowSkip: !!defaults.first_message,
      defaultValue: defaults.first_message,
      parse: (value) => value,
      ensureActive: safeEnsureActive
    }
  );

  if (firstMessage === null) {
    return null;
  }

  const voicePrompt = defaults.voice_model ? defaults.voice_model : 'default';
  const voiceModel = await promptText(
    conversation,
    ctx,
    'ð¤ Enter the Deepgram voice model for this script (or type skip to use the default).',
    {
      allowEmpty: true,
      allowSkip: true,
      defaultValue: voicePrompt,
      parse: (value) => value,
      ensureActive: safeEnsureActive
    }
  );

  if (voiceModel === null) {
    return null;
  }

  return {
    prompt: prompt === undefined ? defaults.prompt : prompt,
    first_message: firstMessage === undefined ? defaults.first_message : firstMessage,
    voice_model: voiceModel === undefined ? defaults.voice_model : (voiceModel || null)
  };
}

async function collectDigitCaptureConfig(conversation, ctx, defaults = {}, ensureActive) {
  const safeEnsureActive = typeof ensureActive === 'function'
    ? ensureActive
    : () => ensureOperationActive(ctx, getCurrentOpId(ctx));
  const selection = await askOptionWithButtons(
    conversation,
    ctx,
    'ð¢ Add digit capture to this script?',
    [
      { id: 'none', label: 'ð« None' },
      { id: 'otp', label: 'ð OTP (code)' },
      { id: 'pin', label: 'ð PIN' },
      { id: 'routing', label: 'ð¦ Routing number' },
      { id: 'account', label: 'ð¦ Account number' },
      { id: 'banking', label: 'ð¦ Banking group (routing + account)' },
      { id: 'card', label: 'ð³ Card group (card + expiry + zip + cvv)' },
      { id: 'custom', label: 'âï¸ Custom profile' }
    ],
    { prefix: 'call-script-capture', columns: 2, ensureActive: safeEnsureActive }
  );

  if (!selection || selection.id === 'none') {
    return {
      requires_otp: false,
      default_profile: null,
      expected_length: null,
      allow_terminator: false,
      terminator_char: null,
      capture_group: null
    };
  }

  const capture = {
    requires_otp: false,
    default_profile: null,
    expected_length: null,
    allow_terminator: false,
    terminator_char: null,
    capture_group: null
  };

  if (selection.id === 'otp') {
    capture.requires_otp = true;
    const length = await promptText(
      conversation,
      ctx,
      'ð¢ OTP length (4-8 digits).',
      { allowEmpty: false, parse: (value) => Number(value), ensureActive: safeEnsureActive }
    );
    if (!length || Number.isNaN(length)) return null;
    capture.expected_length = length;
    return capture;
  }

  if (selection.id === 'banking') {
    capture.capture_group = 'banking';
    return capture;
  }
  if (selection.id === 'card') {
    capture.capture_group = 'card';
    return capture;
  }

  if (selection.id === 'custom') {
    const profile = await promptText(
      conversation,
      ctx,
      'Enter a profile id (e.g., routing_number, account_number, card_number, cvv).',
      { allowEmpty: false, parse: (value) => value.trim(), ensureActive: safeEnsureActive }
    );
    if (!profile) return null;
    capture.default_profile = profile;
  } else {
    const profileMap = {
      pin: 'pin',
      routing: 'routing_number',
      account: 'account_number'
    };
    capture.default_profile = profileMap[selection.id] || selection.id;
  }

  const expectedLength = await promptText(
    conversation,
    ctx,
    'Optional expected length (or type skip).',
    { allowEmpty: true, allowSkip: true, parse: (value) => Number(value), ensureActive: safeEnsureActive }
  );
  if (expectedLength === null) return null;
  if (expectedLength !== undefined && !Number.isNaN(expectedLength)) {
    capture.expected_length = expectedLength;
  }

  const allowTerminator = await confirm(conversation, ctx, 'Allow terminator key (#)?', safeEnsureActive);
  capture.allow_terminator = !!allowTerminator;
  if (capture.allow_terminator) {
    const term = await promptText(
      conversation,
      ctx,
      'Terminator key (default #).',
      { allowEmpty: true, allowSkip: true, defaultValue: '#', parse: (value) => value.trim() || '#', ensureActive: safeEnsureActive }
    );
    if (term === null) return null;
    capture.terminator_char = term === undefined ? '#' : term;
  }

  return capture;
}

async function fetchCallScripts() {
  const data = await scriptsApiRequest({ method: 'get', url: '/api/call-scripts' });
  return data.scripts || [];
}

async function fetchCallScriptById(id) {
  const data = await scriptsApiRequest({ method: 'get', url: `/api/call-scripts/${id}` });
  return data.script;
}

async function fetchInboundDefaultScript() {
  const data = await scriptsApiRequest({ method: 'get', url: '/api/inbound/default-script' });
  return data || {};
}

async function setInboundDefaultScript(scriptId) {
  const data = await scriptsApiRequest({
    method: 'put',
    url: '/api/inbound/default-script',
    data: { script_id: scriptId }
  });
  return data;
}

async function clearInboundDefaultScript() {
  const data = await scriptsApiRequest({ method: 'delete', url: '/api/inbound/default-script' });
  return data;
}

async function createCallScript(payload) {
  const data = await scriptsApiRequest({ method: 'post', url: '/api/call-scripts', data: payload });
  return data.script;
}

async function updateCallScript(id, payload) {
  const data = await scriptsApiRequest({ method: 'put', url: `/api/call-scripts/${id}`, data: payload });
  return data.script;
}

async function deleteCallScript(id) {
  await scriptsApiRequest({ method: 'delete', url: `/api/call-scripts/${id}` });
}

async function cloneCallScript(id, payload) {
  const data = await scriptsApiRequest({ method: 'post', url: `/api/call-scripts/${id}/clone`, data: payload });
  return data.script;
}

function formatCallScriptSummary(script) {
  const summary = [];
  summary.push(`ð *${escapeMarkdown(script.name)}*`);
  if (script.description) {
    summary.push(`ð ${escapeMarkdown(script.description)}`);
  }
  if (script.business_id) {
    const business = findBusinessOption(script.business_id);
    summary.push(`ð¢ Persona: ${escapeMarkdown(business ? business.label : script.business_id)}`);
  }
  const personaSummary = buildPersonaSummaryFromConfig(script);
  if (personaSummary.length) {
    personaSummary.forEach((line) => summary.push(`â¢ ${escapeMarkdown(line)}`));
  }

  const captureSummary = buildDigitCaptureSummary(script);
  summary.push(`ð¢ Digit capture: ${escapeMarkdown(captureSummary)}`);

  if (script.voice_model) {
    summary.push(`ð¤ Voice model: ${escapeMarkdown(script.voice_model)}`);
  }

  const placeholders = new Set([
    ...extractScriptVariables(script.prompt || ''),
    ...extractScriptVariables(script.first_message || '')
  ]);
  if (placeholders.size > 0) {
    summary.push(`ð§© Placeholders: ${Array.from(placeholders).map(escapeMarkdown).join(', ')}`);
  }

  if (script.prompt) {
    const snippet = script.prompt.substring(0, 160);
    summary.push(`ð Prompt snippet: ${escapeMarkdown(snippet)}${script.prompt.length > 160 ? 'â¦' : ''}`);
  }
  if (script.first_message) {
    const snippet = script.first_message.substring(0, 160);
    summary.push(`ð¨ï¸ First message: ${escapeMarkdown(snippet)}${script.first_message.length > 160 ? 'â¦' : ''}`);
  }
  summary.push(
    `ð Updated: ${escapeMarkdown(new Date(script.updated_at || script.created_at).toLocaleString())}`
  );
  return summary.join('\n');
}

async function previewCallScript(conversation, ctx, script, ensureActive) {
  const safeEnsureActive = typeof ensureActive === 'function'
    ? ensureActive
    : () => ensureOperationActive(ctx, getCurrentOpId(ctx));
  const phonePrompt =
    'ð Enter the test phone number (E.164 format, e.g., +1234567890) to receive a preview call.';
  const testNumber = await promptText(conversation, ctx, phonePrompt, {
    allowEmpty: false,
    ensureActive: safeEnsureActive
  });
  if (!testNumber) {
    await ctx.reply('â Preview cancelled.');
    return;
  }

  if (!/^\+[1-9]\d{1,14}$/.test(testNumber)) {
    await ctx.reply('â Invalid phone number format. Preview cancelled.');
    return;
  }

  const placeholderSet = new Set();
  extractScriptVariables(script.prompt || '').forEach((token) => placeholderSet.add(token));
  extractScriptVariables(script.first_message || '').forEach((token) => placeholderSet.add(token));

  let prompt = script.prompt;
  let firstMessage = script.first_message;

  if (placeholderSet.size > 0) {
    await ctx.reply('ð§© This script has placeholders. Provide values where needed (type skip to leave unchanged).');
    const values = await collectPlaceholderValues(conversation, ctx, Array.from(placeholderSet), safeEnsureActive);
    if (values === null) {
      await ctx.reply('â Preview cancelled.');
      return;
    }
    if (prompt) {
      prompt = replacePlaceholders(prompt, values);
    }
    if (firstMessage) {
      firstMessage = replacePlaceholders(firstMessage, values);
    }
  }

  const payload = {
    number: testNumber,
    user_chat_id: ctx.from.id.toString()
  };

  if (script.business_id) {
    payload.business_id = script.business_id;
  }
  const persona = script.persona_config || {};
  if (prompt) {
    payload.prompt = prompt;
  }
  if (firstMessage) {
    payload.first_message = firstMessage;
  }
  if (script.voice_model) {
    payload.voice_model = script.voice_model;
  }
  if (persona.purpose) {
    payload.purpose = persona.purpose;
  }
  if (persona.emotion) {
    payload.emotion = persona.emotion;
  }
  if (persona.urgency) {
    payload.urgency = persona.urgency;
  }
  if (persona.technical_level) {
    payload.technical_level = persona.technical_level;
  }

  try {
    await httpClient.post(null, `${config.apiUrl}/outbound-call`, payload, {
      headers: { 'Content-Type': 'application/json' },
      timeout: 30000
    });

    await ctx.reply('â Preview call launched! You should receive a call shortly.');
  } catch (error) {
    console.error('Failed to launch preview call:', error?.response?.data || error.message);
    await ctx.reply(`â Preview failed: ${error?.response?.data?.error || error.message}`);
  }
}

async function createCallScriptFlow(conversation, ctx, ensureActive) {
  const safeEnsureActive = typeof ensureActive === 'function'
    ? ensureActive
    : () => ensureOperationActive(ctx, getCurrentOpId(ctx));
  const name = await promptText(
    conversation,
    ctx,
    'ð *Script name*\nEnter a unique name for this call script.',
    {
      allowEmpty: false,
      parse: (value) => value.trim(),
      ensureActive: safeEnsureActive
    }
  );

  if (!name) {
    await ctx.reply('â Script creation cancelled.');
    return;
  }

  const description = await promptText(
    conversation,
    ctx,
    'ð Provide an optional description for this script (or type skip).',
    {
      allowEmpty: true,
      allowSkip: true,
      parse: (value) => value.trim(),
      ensureActive: safeEnsureActive
    }
  );
  if (description === null) {
    await ctx.reply('â Script creation cancelled.');
    return;
  }

  const personaResult = await collectPersonaConfig(conversation, ctx, {}, { allowCancel: true, ensureActive: safeEnsureActive });
  if (!personaResult) {
    await ctx.reply('â Script creation cancelled.');
    return;
  }

  const promptAndVoice = await collectPromptAndVoice(conversation, ctx, {}, safeEnsureActive);
  if (!promptAndVoice) {
    await ctx.reply('â Script creation cancelled.');
    return;
  }

  const captureConfig = await collectDigitCaptureConfig(conversation, ctx, {}, safeEnsureActive);
  if (!captureConfig) {
    await ctx.reply('â Script creation cancelled.');
    return;
  }
  if (captureConfig.capture_group) {
    await ctx.reply('â¹ï¸ Capture groups are guidance-only; the API still infers groups from the prompt text.');
  }

  const scriptPayload = {
    name,
    description: description === undefined ? null : (description.length ? description : null),
    business_id: personaResult.business_id,
    persona_config: personaResult.persona_config,
    prompt: promptAndVoice.prompt,
    first_message: promptAndVoice.first_message,
    voice_model: promptAndVoice.voice_model || null,
    requires_otp: captureConfig.requires_otp || false,
    default_profile: captureConfig.default_profile || null,
    expected_length: captureConfig.expected_length || null,
    allow_terminator: captureConfig.allow_terminator || false,
    terminator_char: captureConfig.terminator_char || null,
    capture_group: captureConfig.capture_group || null
  };

  const validation = validateCallScriptPayload(scriptPayload);
  if (validation.errors.length) {
    await ctx.reply(`â Fix the following issues:\nâ¢ ${validation.errors.join('\nâ¢ ')}`);
    return;
  }
  if (validation.warnings.length) {
    await ctx.reply(`â ï¸ Warnings:\nâ¢ ${validation.warnings.join('\nâ¢ ')}`);
    const proceed = await confirm(conversation, ctx, 'Proceed anyway?', safeEnsureActive);
    if (!proceed) {
      await ctx.reply('â Script creation cancelled.');
      return;
    }
  }

  try {
    const apiPayload = { ...scriptPayload };
    delete apiPayload.capture_group;
    const script = await createCallScript(apiPayload);
    const needsCaptureUpdate = scriptPayload.requires_otp
      || scriptPayload.default_profile
      || scriptPayload.expected_length
      || scriptPayload.allow_terminator
      || scriptPayload.terminator_char;
    if (needsCaptureUpdate) {
      try {
        await updateCallScript(script.id, stripUndefined({
          requires_otp: scriptPayload.requires_otp,
          default_profile: scriptPayload.default_profile,
          expected_length: scriptPayload.expected_length,
          allow_terminator: scriptPayload.allow_terminator,
          terminator_char: scriptPayload.terminator_char
        }));
      } catch (updateError) {
        console.warn('Capture settings update failed:', updateError.message);
      }
    }
    await storeScriptVersionSnapshot({ ...script, ...scriptPayload }, 'call', ctx);
    await ctx.reply(`â Script *${escapeMarkdown(script.name)}* created successfully!`, { parse_mode: 'Markdown' });
  } catch (error) {
    console.error('Failed to create script:', error);
    await ctx.reply(formatScriptsApiError(error, 'Failed to create script'));
  }
}

async function editCallScriptFlow(conversation, ctx, script, ensureActive) {
  const safeEnsureActive = typeof ensureActive === 'function'
    ? ensureActive
    : () => ensureOperationActive(ctx, getCurrentOpId(ctx));
  const updates = {};

  const name = await promptText(
    conversation,
    ctx,
    'âï¸ Update script name (or type skip to keep current).',
    {
      allowEmpty: false,
      allowSkip: true,
      defaultValue: script.name,
      parse: (value) => value.trim(),
      ensureActive: safeEnsureActive
    }
  );
  if (name === null) {
    await ctx.reply('â Update cancelled.');
    return;
  }
  if (name !== undefined) {
    if (!name.length) {
      await ctx.reply('â Script name cannot be empty.');
      return;
    }
    updates.name = name;
  }

  const description = await promptText(
    conversation,
    ctx,
    'ð Update description (or type skip).',
    {
      allowEmpty: true,
      allowSkip: true,
      defaultValue: script.description || '',
      parse: (value) => value.trim(),
      ensureActive: safeEnsureActive
    }
  );
  if (description === null) {
    await ctx.reply('â Update cancelled.');
    return;
  }
  if (description !== undefined) {
    updates.description = description.length ? description : null;
  }

  const adjustPersona = await confirm(conversation, ctx, 'Would you like to update the persona settings?', safeEnsureActive);
  if (adjustPersona) {
    const personaResult = await collectPersonaConfig(conversation, ctx, script, { allowCancel: true, ensureActive: safeEnsureActive });
    if (!personaResult) {
      await ctx.reply('â Update cancelled.');
      return;
    }
    updates.business_id = personaResult.business_id;
    updates.persona_config = personaResult.persona_config;
  }

  const adjustPrompt = await confirm(conversation, ctx, 'Update prompt, first message, or voice settings?', safeEnsureActive);
  if (adjustPrompt) {
    const promptAndVoice = await collectPromptAndVoice(conversation, ctx, script, safeEnsureActive);
    if (!promptAndVoice) {
      await ctx.reply('â Update cancelled.');
      return;
    }
    updates.prompt = promptAndVoice.prompt;
    updates.first_message = promptAndVoice.first_message;
    updates.voice_model = promptAndVoice.voice_model || null;
  }

  const adjustCapture = await confirm(conversation, ctx, 'Update digit capture settings?', safeEnsureActive);
  if (adjustCapture) {
    const captureConfig = await collectDigitCaptureConfig(conversation, ctx, script, safeEnsureActive);
    if (!captureConfig) {
      await ctx.reply('â Update cancelled.');
      return;
    }
    updates.requires_otp = captureConfig.requires_otp || false;
    updates.default_profile = captureConfig.default_profile || null;
    updates.expected_length = captureConfig.expected_length || null;
    updates.allow_terminator = captureConfig.allow_terminator || false;
    updates.terminator_char = captureConfig.terminator_char || null;
    updates.capture_group = captureConfig.capture_group || null;
  }

  if (Object.keys(updates).length === 0) {
    await ctx.reply('â¹ï¸ No changes made.');
    return;
  }

  const merged = { ...script, ...updates };
  const validation = validateCallScriptPayload(merged);
  if (validation.errors.length) {
    await ctx.reply(`â Fix the following issues:\nâ¢ ${validation.errors.join('\nâ¢ ')}`);
    return;
  }
  if (validation.warnings.length) {
    await ctx.reply(`â ï¸ Warnings:\nâ¢ ${validation.warnings.join('\nâ¢ ')}`);
    const proceed = await confirm(conversation, ctx, 'Proceed anyway?', safeEnsureActive);
    if (!proceed) {
      await ctx.reply('â Update cancelled.');
      return;
    }
  }

  try {
    await storeScriptVersionSnapshot(script, 'call', ctx);
    const apiUpdates = stripUndefined({ ...updates });
    delete apiUpdates.capture_group;
    const updated = await updateCallScript(script.id, apiUpdates);
    await ctx.reply(`â Script *${escapeMarkdown(updated.name)}* updated.`, { parse_mode: 'Markdown' });
  } catch (error) {
    console.error('Failed to update script:', error);
    await ctx.reply(formatScriptsApiError(error, 'Failed to update script'));
  }
}

async function cloneCallScriptFlow(conversation, ctx, script, ensureActive) {
  const safeEnsureActive = typeof ensureActive === 'function'
    ? ensureActive
    : () => ensureOperationActive(ctx, getCurrentOpId(ctx));
  const name = await promptText(
    conversation,
    ctx,
    `ð Enter a name for the clone of *${escapeMarkdown(script.name)}*.`,
    {
      allowEmpty: false,
      parse: (value) => value.trim(),
      defaultValue: null,
      ensureActive: safeEnsureActive
    }
  );
  if (!name) {
    await ctx.reply('â Clone cancelled.');
    return;
  }

  const description = await promptText(
    conversation,
    ctx,
    'ð Optionally provide a description for the new script (or type skip).',
    {
      allowEmpty: true,
      allowSkip: true,
      defaultValue: script.description || '',
      parse: (value) => value.trim(),
      ensureActive: safeEnsureActive
    }
  );
  if (description === null) {
    await ctx.reply('â Clone cancelled.');
    return;
  }

  try {
    const cloned = await cloneCallScript(script.id, {
      name,
      description: description === undefined ? script.description : (description.length ? description : null)
    });
    await ctx.reply(`â Script cloned as *${escapeMarkdown(cloned.name)}*.`, { parse_mode: 'Markdown' });
  } catch (error) {
    console.error('Failed to clone script:', error);
    await ctx.reply(formatScriptsApiError(error, 'Failed to clone script'));
  }
}

async function deleteCallScriptFlow(conversation, ctx, script, ensureActive) {
  const safeEnsureActive = typeof ensureActive === 'function'
    ? ensureActive
    : () => ensureOperationActive(ctx, getCurrentOpId(ctx));
  const confirmed = await confirm(
    conversation,
    ctx,
    `Are you sure you want to delete *${escapeMarkdown(script.name)}*?`,
    safeEnsureActive
  );
  if (!confirmed) {
    await ctx.reply('Deletion cancelled.');
    return;
  }

  try {
    await storeScriptVersionSnapshot(script, 'call', ctx);
    await deleteCallScript(script.id);
    await ctx.reply(`ðï¸ Script *${escapeMarkdown(script.name)}* deleted.`, { parse_mode: 'Markdown' });
  } catch (error) {
    console.error('Failed to delete script:', error);
    await ctx.reply(formatScriptsApiError(error, 'Failed to delete script'));
  }
}

async function showCallScriptVersions(conversation, ctx, script, ensureActive) {
  const safeEnsureActive = typeof ensureActive === 'function'
    ? ensureActive
    : () => ensureOperationActive(ctx, getCurrentOpId(ctx));
  try {
    const versions = await listScriptVersions(script.id, 'call', 8);
    safeEnsureActive();
    if (!versions.length) {
      await ctx.reply('â¹ï¸ No saved versions yet. Versions are stored on edit/delete.');
      return;
    }
    const lines = versions.map((v) => `v${v.version_number} â¢ ${new Date(v.created_at).toLocaleString()}`);
    await ctx.reply(`ðï¸ Saved versions\n${lines.join('\n')}`);

    const options = versions.map((v) => ({
      id: String(v.version_number),
      label: `â©ï¸ Restore v${v.version_number}`
    }));
    options.push({ id: 'back', label: 'â¬ï¸ Back' });

    const selection = await askOptionWithButtons(
      conversation,
      ctx,
      'Select a version to restore.',
      options,
      { prefix: 'call-script-version', columns: 2, ensureActive: safeEnsureActive }
    );
    if (!selection || selection.id === 'back') return;
    const versionNumber = Number(selection.id);
    if (Number.isNaN(versionNumber)) {
      await ctx.reply('â Invalid version selected.');
      return;
    }
    const version = await getScriptVersion(script.id, 'call', versionNumber);
    safeEnsureActive();
    if (!version || !version.payload) {
      await ctx.reply('â Version payload not found.');
      return;
    }
    const confirmRestore = await confirm(conversation, ctx, `Restore version v${versionNumber}?`, safeEnsureActive);
    if (!confirmRestore) {
      await ctx.reply('Restore cancelled.');
      return;
    }
    await storeScriptVersionSnapshot(script, 'call', ctx);
    const payload = stripUndefined({ ...version.payload });
    delete payload.capture_group;
    const updated = await updateCallScript(script.id, payload);
    await ctx.reply(`â Script restored to v${versionNumber} (${escapeMarkdown(updated.name)}).`, { parse_mode: 'Markdown' });
  } catch (error) {
    console.error('Version restore failed:', error);
    await ctx.reply(`â Failed to restore version: ${error.message}`);
  }
}

async function showCallScriptDetail(conversation, ctx, script, ensureActive) {
  const safeEnsureActive = typeof ensureActive === 'function'
    ? ensureActive
    : () => ensureOperationActive(ctx, getCurrentOpId(ctx));
  let viewing = true;
  while (viewing) {
    const summary = formatCallScriptSummary(script);
    await ctx.reply(summary, { parse_mode: 'Markdown' });

    const action = await askOptionWithButtons(
      conversation,
      ctx,
      'Choose an action for this script.',
      [
        { id: 'preview', label: 'ð Preview' },
        { id: 'edit', label: 'âï¸ Edit' },
        { id: 'clone', label: 'ð§¬ Clone' },
        { id: 'versions', label: 'ðï¸ Versions' },
        { id: 'delete', label: 'ðï¸ Delete' },
        { id: 'back', label: 'â¬ï¸ Back' }
      ],
      { prefix: 'call-script-action', columns: 2, ensureActive: safeEnsureActive }
    );

    switch (action.id) {
      case 'preview':
        await previewCallScript(conversation, ctx, script, safeEnsureActive);
        break;
      case 'edit':
        await editCallScriptFlow(conversation, ctx, script, safeEnsureActive);
        try {
          script = await fetchCallScriptById(script.id);
        } catch (error) {
          console.error('Failed to refresh call script after edit:', error);
          await ctx.reply(formatScriptsApiError(error, 'Failed to refresh script details'));
          viewing = false;
        }
        break;
      case 'clone':
        await cloneCallScriptFlow(conversation, ctx, script, safeEnsureActive);
        break;
      case 'versions':
        await showCallScriptVersions(conversation, ctx, script, safeEnsureActive);
        break;
      case 'delete':
        await deleteCallScriptFlow(conversation, ctx, script, safeEnsureActive);
        viewing = false;
        break;
      case 'back':
        viewing = false;
        break;
      default:
        break;
    }
  }
}

async function listCallScriptsFlow(conversation, ctx, ensureActive) {
  const safeEnsureActive = typeof ensureActive === 'function'
    ? ensureActive
    : () => ensureOperationActive(ctx, getCurrentOpId(ctx));
  try {
    const scripts = await fetchCallScripts();
    safeEnsureActive();
    const list = Array.isArray(scripts) ? scripts : [];
    const validScripts = list.filter((script) => script && typeof script.id !== 'undefined' && script.id !== null);

    if (!validScripts.length) {
      if (scripts && scripts.length && scripts.some((t) => !t || typeof t.id === 'undefined')) {
        console.warn('Script list contained invalid entries, ignoring malformed records.');
      }
      await ctx.reply('â¹ï¸ No call scripts found. Use the create action to add one.');
      return;
    }

    const summaryLines = validScripts.slice(0, 15).map((script, index) => {
      const parts = [`${index + 1}. ${script.name}`];
      if (script.description) {
        parts.push(`â ${script.description}`);
      }
      return parts.join(' ');
    });

    let message = 'âï¸ Call Scripts\n\n';
    message += summaryLines.join('\n');
    if (validScripts.length > 15) {
      message += `\nâ¦ and ${validScripts.length - 15} more.`;
    }
    message += '\n\nSelect a script below to view details.';

    await ctx.reply(message);

    const options = validScripts.map((script) => ({
      id: script.id.toString(),
      label: `ð ${script.name}`
    }));
    options.push({ id: 'back', label: 'â¬ï¸ Back' });

    const selection = await askOptionWithButtons(
      conversation,
      ctx,
      'Choose a call script to manage.',
      options,
      { prefix: 'call-script-select', columns: 1, formatLabel: (option) => option.label, ensureActive: safeEnsureActive }
    );

    if (!selection || !selection.id) {
      await ctx.reply('â No selection received. Please try again.');
      return;
    }

    if (selection.id === 'back') {
      return;
    }

    const scriptId = Number(selection.id);
    if (Number.isNaN(scriptId)) {
      await ctx.reply('â Invalid script selection.');
      return;
    }

    try {
      const script = await fetchCallScriptById(scriptId);
      safeEnsureActive();
      if (!script) {
        await ctx.reply('â Script not found.');
        return;
      }

      await showCallScriptDetail(conversation, ctx, script, safeEnsureActive);
    } catch (error) {
      console.error('Failed to load call script details:', error);
      await ctx.reply(formatScriptsApiError(error, 'Failed to load script details'));
    }
  } catch (error) {
    console.error('Failed to list scripts:', error);
    await ctx.reply(formatScriptsApiError(error, 'Failed to list call scripts'));
  }
}

async function inboundDefaultScriptMenu(conversation, ctx, ensureActive) {
  const safeEnsureActive = typeof ensureActive === 'function'
    ? ensureActive
    : () => ensureOperationActive(ctx, getCurrentOpId(ctx));
  let open = true;
  while (open) {
    let current = null;
    try {
      current = await fetchInboundDefaultScript();
      safeEnsureActive();
    } catch (error) {
      console.error('Failed to fetch inbound default script:', error);
      await ctx.reply(formatScriptsApiError(error, 'Failed to load inbound default script'));
      return;
    }

    const currentLabel = current?.mode === 'script' && current?.script
      ? `ð¥ Current inbound default: ${current.script.name} (ID ${current.script_id})`
      : 'ð¥ Current inbound default: Built-in default';
    const previewLine = current?.mode === 'script' && current?.script?.first_message
      ? `ð¨ï¸ First message: ${current.script.first_message.slice(0, 140)}${current.script.first_message.length > 140 ? 'â¦' : ''}`
      : null;

    const action = await askOptionWithButtons(
      conversation,
      ctx,
      `${currentLabel}${previewLine ? `\n${previewLine}` : ''}\n\nChoose an action.`,
      [
        { id: 'set', label: 'â Set default' },
        { id: 'clear', label: 'â©ï¸ Revert to built-in' },
        { id: 'back', label: 'â¬ï¸ Back' }
      ],
      { prefix: 'inbound-default', columns: 1, ensureActive: safeEnsureActive }
    );

    switch (action.id) {
      case 'set': {
        let scripts;
        try {
          scripts = await fetchCallScripts();
          safeEnsureActive();
        } catch (error) {
          console.error('Failed to fetch call scripts:', error);
          await ctx.reply(formatScriptsApiError(error, 'Failed to load call scripts'));
          break;
        }

        if (!scripts.length) {
          await ctx.reply('â¹ï¸ No call scripts available. Create one first.');
          break;
        }

        const options = scripts.map((script) => ({
          id: script.id.toString(),
          label: `ð ${script.name}`
        }));
        options.push({ id: 'back', label: 'â¬ï¸ Back' });

        const selection = await askOptionWithButtons(
          conversation,
          ctx,
          'Select a script to use as the inbound default.',
          options,
          { prefix: 'inbound-default-select', columns: 1, ensureActive: safeEnsureActive }
        );

        if (!selection || !selection.id || selection.id === 'back') {
          break;
        }

        const scriptId = Number(selection.id);
        if (Number.isNaN(scriptId)) {
          await ctx.reply('â Invalid script selection.');
          break;
        }

        try {
          const result = await setInboundDefaultScript(scriptId);
          safeEnsureActive();
          await ctx.reply(`â Inbound default set to ${result?.script?.name || 'selected script'}.`);
        } catch (error) {
          console.error('Failed to set inbound default script:', error);
          await ctx.reply(formatScriptsApiError(error, 'Failed to set inbound default script'));
        }
        break;
      }
      case 'clear':
        try {
          await clearInboundDefaultScript();
          safeEnsureActive();
          await ctx.reply('â Inbound default reverted to built-in settings.');
        } catch (error) {
          console.error('Failed to clear inbound default script:', error);
          await ctx.reply(formatScriptsApiError(error, 'Failed to clear inbound default script'));
        }
        break;
      case 'back':
        open = false;
        break;
      default:
        break;
    }
  }
}

async function callScriptsMenu(conversation, ctx, ensureActive) {
  const safeEnsureActive = typeof ensureActive === 'function'
    ? ensureActive
    : () => ensureOperationActive(ctx, getCurrentOpId(ctx));
  let open = true;
  while (open) {
    const action = await askOptionWithButtons(
      conversation,
      ctx,
      'âï¸ *Call Script Designer*\nChoose an action.',
      [
        { id: 'list', label: 'ð List scripts' },
        { id: 'create', label: 'â Create script' },
        { id: 'incoming', label: 'ð¥ Incoming default' },
        { id: 'back', label: 'â¬ï¸ Back' }
      ],
      { prefix: 'call-script-main', columns: 1, ensureActive: safeEnsureActive }
    );

    switch (action.id) {
      case 'list':
        await listCallScriptsFlow(conversation, ctx, safeEnsureActive);
        break;
      case 'create':
        await createCallScriptFlow(conversation, ctx, safeEnsureActive);
        break;
      case 'incoming':
        await inboundDefaultScriptMenu(conversation, ctx, safeEnsureActive);
        break;
      case 'back':
        open = false;
        break;
      default:
        break;
    }
  }
}

async function fetchSmsScripts({ includeContent = false } = {}) {
  const data = await scriptsApiRequest({
    method: 'get',
    url: '/api/sms/scripts',
    params: {
      include_builtins: true,
      detailed: includeContent
    }
  });

  const custom = (data.scripts || []).map((script) => ({
    ...script,
    is_builtin: !!script.is_builtin,
    metadata: script.metadata || {}
  }));

  const builtin = (data.builtin || []).map((script) => ({
    ...script,
    is_builtin: true,
    metadata: script.metadata || {}
  }));

  return [...custom, ...builtin];
}

async function fetchSmsScriptByName(name, { detailed = true } = {}) {
  const data = await scriptsApiRequest({
    method: 'get',
    url: `/api/sms/scripts/${encodeURIComponent(name)}`,
    params: { detailed }
  });

  const script = data.script;
  if (script) {
    script.is_builtin = !!script.is_builtin;
    script.metadata = script.metadata || {};
  }
  return script;
}

async function createSmsScript(payload) {
  const data = await scriptsApiRequest({ method: 'post', url: '/api/sms/scripts', data: payload });
  return data.script;
}

async function updateSmsScript(name, payload) {
  const data = await scriptsApiRequest({ method: 'put', url: `/api/sms/scripts/${encodeURIComponent(name)}`, data: payload });
  return data.script;
}

async function deleteSmsScript(name) {
  await scriptsApiRequest({ method: 'delete', url: `/api/sms/scripts/${encodeURIComponent(name)}` });
}

async function requestSmsScriptPreview(name, payload) {
  const data = await scriptsApiRequest({
    method: 'post',
    url: `/api/sms/scripts/${encodeURIComponent(name)}/preview`,
    data: payload
  });
  return data.preview;
}

function formatSmsScriptSummary(script) {
  const summary = [];
  summary.push(`${script.is_builtin ? 'ð¦' : 'ð'} *${escapeMarkdown(script.name)}*`);
  if (script.description) {
    summary.push(`ð ${escapeMarkdown(script.description)}`);
  }
  summary.push(script.is_builtin ? 'ð·ï¸ Type: Built-in (read-only)' : 'ð·ï¸ Type: Custom script');

  const personaSummary = buildPersonaSummaryFromOverrides(script.metadata?.persona);
  if (personaSummary.length) {
    personaSummary.forEach((line) => summary.push(`â¢ ${escapeMarkdown(line)}`));
  }

  const placeholders = extractScriptVariables(script.content || '');
  if (placeholders.length) {
    summary.push(`ð§© Placeholders: ${placeholders.map(escapeMarkdown).join(', ')}`);
  }

  if (script.content) {
    const snippet = script.content.substring(0, 160);
    summary.push(`ð¬ Preview: ${escapeMarkdown(snippet)}${script.content.length > 160 ? 'â¦' : ''}`);
  }

  summary.push(
    `ð Updated: ${escapeMarkdown(new Date(script.updated_at || script.created_at).toLocaleString())}`
  );

  return summary.join('\n');
}

async function createSmsScriptFlow(conversation, ctx) {
  const name = await promptText(
    conversation,
    ctx,
    'ð *Script name*\nUse lowercase letters, numbers, dashes, or underscores.',
    {
      allowEmpty: false,
      parse: (value) => {
        const trimmed = value.trim().toLowerCase();
        if (!/^[a-z0-9_-]+$/.test(trimmed)) {
          throw new Error('Use only letters, numbers, underscores, or dashes.');
        }
        return trimmed;
      }
    }
  );
  if (!name) {
    await ctx.reply('â Script creation cancelled.');
    return;
  }

  const description = await promptText(
    conversation,
    ctx,
    'ð Optional description (or type skip).',
    { allowEmpty: true, allowSkip: true, parse: (value) => value.trim() }
  );
  if (description === null) {
    await ctx.reply('â Script creation cancelled.');
    return;
  }

  const content = await promptText(
    conversation,
    ctx,
    'ð¬ Provide the SMS content. You can include placeholders like {code}.',
    { allowEmpty: false, parse: (value) => value.trim() }
  );
  if (!content) {
    await ctx.reply('â Script creation cancelled.');
    return;
  }

  const metadata = {};
  const configurePersona = await confirm(conversation, ctx, 'Add persona guidance for this script?');
  if (configurePersona) {
    const personaResult = await collectPersonaConfig(conversation, ctx, {}, { allowCancel: true });
    if (!personaResult) {
      await ctx.reply('â Script creation cancelled.');
      return;
    }
    const overrides = toPersonaOverrides(personaResult);
    if (overrides) {
      metadata.persona = overrides;
    }
  }

  const payload = {
    name,
    description: description === undefined ? null : (description.length ? description : null),
    content,
    metadata: Object.keys(metadata).length ? metadata : undefined,
    created_by: ctx.from.id.toString()
  };

  try {
    const script = await createSmsScript(payload);
    await storeScriptVersionSnapshot(script, 'sms', ctx);
    await ctx.reply(`â SMS script *${escapeMarkdown(script.name)}* created.`, { parse_mode: 'Markdown' });
  } catch (error) {
    console.error('Failed to create SMS script:', error);
    await ctx.reply(formatScriptsApiError(error, 'Failed to create SMS script'));
  }
}

async function editSmsScriptFlow(conversation, ctx, script) {
  if (script.is_builtin) {
    await ctx.reply('â¹ï¸ Built-in scripts are read-only. Clone the script to modify it.');
    return;
  }

  const updates = { updated_by: ctx.from.id.toString() };

  const description = await promptText(
    conversation,
    ctx,
    'ð Update description (or type skip).',
    { allowEmpty: true, allowSkip: true, defaultValue: script.description || '', parse: (value) => value.trim() }
  );
  if (description === null) {
    await ctx.reply('â Update cancelled.');
    return;
  }
  if (description !== undefined) {
    updates.description = description.length ? description : null;
  }

  const updateContent = await confirm(conversation, ctx, 'Update the SMS content?');
  if (updateContent) {
    const content = await promptText(
      conversation,
      ctx,
      'ð¬ Enter the new SMS content.',
      { allowEmpty: false, defaultValue: script.content, parse: (value) => value.trim() }
    );
    if (!content) {
      await ctx.reply('â Update cancelled.');
      return;
    }
    updates.content = content;
  }

  const adjustPersona = await confirm(conversation, ctx, 'Update persona guidance for this script?');
  if (adjustPersona) {
    const personaResult = await collectPersonaConfig(conversation, ctx, {}, { allowCancel: true });
    if (!personaResult) {
      await ctx.reply('â Update cancelled.');
      return;
    }
    const overrides = toPersonaOverrides(personaResult);
    const metadata = { ...(script.metadata || {}) };
    if (overrides) {
      metadata.persona = overrides;
    } else {
      delete metadata.persona;
    }
    updates.metadata = metadata;
  } else if (script.metadata?.persona) {
    const clearPersona = await confirm(conversation, ctx, 'Remove existing persona guidance?');
    if (clearPersona) {
      const metadata = { ...(script.metadata || {}) };
      delete metadata.persona;
      updates.metadata = metadata;
    }
  }

  const updateKeys = Object.keys(updates).filter((key) => key !== 'updated_by');
  if (!updateKeys.length) {
    await ctx.reply('â¹ï¸ No changes made.');
    return;
  }

  try {
    await storeScriptVersionSnapshot(script, 'sms', ctx);
    const updated = await updateSmsScript(script.name, stripUndefined(updates));
    await ctx.reply(`â SMS script *${escapeMarkdown(updated.name)}* updated.`, { parse_mode: 'Markdown' });
  } catch (error) {
    console.error('Failed to update SMS script:', error);
    await ctx.reply(formatScriptsApiError(error, 'Failed to update SMS script'));
  }
}

async function cloneSmsScriptFlow(conversation, ctx, script) {
  const name = await promptText(
    conversation,
    ctx,
    `ð Enter a name for the clone of *${escapeMarkdown(script.name)}*.`,
    {
      allowEmpty: false,
      parse: (value) => {
        const trimmed = value.trim().toLowerCase();
        if (!/^[a-z0-9_-]+$/.test(trimmed)) {
          throw new Error('Use only letters, numbers, underscores, or dashes.');
        }
        return trimmed;
      }
    }
  );
  if (!name) {
    await ctx.reply('â Clone cancelled.');
    return;
  }

  const description = await promptText(
    conversation,
    ctx,
    'ð Optional description for the cloned script (or type skip).',
    { allowEmpty: true, allowSkip: true, defaultValue: script.description || '', parse: (value) => value.trim() }
  );
  if (description === null) {
    await ctx.reply('â Clone cancelled.');
    return;
  }

  const payload = {
    name,
    description: description === undefined ? script.description : (description.length ? description : null),
    content: script.content,
    metadata: script.metadata,
    created_by: ctx.from.id.toString()
  };

  try {
    const cloned = await createSmsScript(payload);
    await ctx.reply(`â Script cloned as *${escapeMarkdown(cloned.name)}*.`, { parse_mode: 'Markdown' });
  } catch (error) {
    console.error('Failed to clone SMS script:', error);
    await ctx.reply(formatScriptsApiError(error, 'Failed to clone SMS script'));
  }
}

async function deleteSmsScriptFlow(conversation, ctx, script) {
  if (script.is_builtin) {
    await ctx.reply('â¹ï¸ Built-in scripts cannot be deleted.');
    return;
  }

  const confirmed = await confirm(conversation, ctx, `Delete SMS script *${escapeMarkdown(script.name)}*?`);
  if (!confirmed) {
    await ctx.reply('Deletion cancelled.');
    return;
  }

  try {
    await storeScriptVersionSnapshot(script, 'sms', ctx);
    await deleteSmsScript(script.name);
    await ctx.reply(`ðï¸ Script *${escapeMarkdown(script.name)}* deleted.`, { parse_mode: 'Markdown' });
  } catch (error) {
    console.error('Failed to delete SMS script:', error);
    await ctx.reply(formatScriptsApiError(error, 'Failed to delete SMS script'));
  }
}

async function showSmsScriptVersions(conversation, ctx, script) {
  try {
    const versions = await listScriptVersions(script.name, 'sms', 8);
    if (!versions.length) {
      await ctx.reply('â¹ï¸ No saved versions yet. Versions are stored on edit/delete.');
      return;
    }
    const lines = versions.map((v) => `v${v.version_number} â¢ ${new Date(v.created_at).toLocaleString()}`);
    await ctx.reply(`ðï¸ Saved versions\n${lines.join('\n')}`);

    const options = versions.map((v) => ({
      id: String(v.version_number),
      label: `â©ï¸ Restore v${v.version_number}`
    }));
    options.push({ id: 'back', label: 'â¬ï¸ Back' });

    const selection = await askOptionWithButtons(
      conversation,
      ctx,
      'Select a version to restore.',
      options,
      { prefix: 'sms-script-version', columns: 2 }
    );
    if (!selection || selection.id === 'back') return;
    const versionNumber = Number(selection.id);
    if (Number.isNaN(versionNumber)) {
      await ctx.reply('â Invalid version selected.');
      return;
    }
    const version = await getScriptVersion(script.name, 'sms', versionNumber);
    if (!version || !version.payload) {
      await ctx.reply('â Version payload not found.');
      return;
    }
    const confirmRestore = await confirm(conversation, ctx, `Restore version v${versionNumber}?`);
    if (!confirmRestore) {
      await ctx.reply('Restore cancelled.');
      return;
    }
    await storeScriptVersionSnapshot(script, 'sms', ctx);
    const updated = await updateSmsScript(script.name, stripUndefined(version.payload));
    await ctx.reply(`â SMS script restored to v${versionNumber}.`, { parse_mode: 'Markdown' });
    try {
      script = await fetchSmsScriptByName(script.name, { detailed: true });
    } catch (_) {}
  } catch (error) {
    console.error('SMS version restore failed:', error);
    await ctx.reply(`â Failed to restore version: ${error.message}`);
  }
}

async function previewSmsScript(conversation, ctx, script) {
  const to = await promptText(
    conversation,
    ctx,
    'ð± Enter the destination number (E.164 format, e.g., +1234567890).',
    { allowEmpty: false, parse: (value) => value.trim() }
  );
  if (!to) {
    await ctx.reply('â Preview cancelled.');
    return;
  }

  if (!/^\+[1-9]\d{1,14}$/.test(to)) {
    await ctx.reply('â Invalid phone number format. Preview cancelled.');
    return;
  }

  const placeholders = extractScriptVariables(script.content || '');
  let variables = {};
  if (placeholders.length > 0) {
    await ctx.reply('ð§© This script includes placeholders. Provide values or type skip to leave unchanged.');
    const values = await collectPlaceholderValues(conversation, ctx, placeholders);
    if (values === null) {
      await ctx.reply('â Preview cancelled.');
      return;
    }
    variables = values;
  }

  const payload = {
    to,
    variables,
    persona_overrides: script.metadata?.persona
  };

  if (!Object.keys(variables).length) {
    payload.variables = {};
  }

  if (!payload.persona_overrides) {
    delete payload.persona_overrides;
  }

  try {
    const preview = await requestSmsScriptPreview(script.name, payload);
    const snippet = preview.content.substring(0, 200);
    await ctx.reply(
      `â Preview SMS sent!\n\nð± To: ${preview.to}\nð Message SID: \`${preview.message_sid}\`\nð¬ Content: ${escapeMarkdown(snippet)}${preview.content.length > 200 ? 'â¦' : ''}`,
      { parse_mode: 'Markdown' }
    );
  } catch (error) {
    console.error('Failed to send SMS preview:', error);
    await ctx.reply(formatScriptsApiError(error, 'Failed to send SMS preview'));
  }
}

async function showSmsScriptDetail(conversation, ctx, script) {
  let viewing = true;
  while (viewing) {
    const summary = formatSmsScriptSummary(script);
    await ctx.reply(summary, { parse_mode: 'Markdown' });

    const actions = [
      { id: 'preview', label: 'ð² Preview' },
      { id: 'clone', label: 'ð§¬ Clone' }
    ];

    if (!script.is_builtin) {
      actions.splice(1, 0, { id: 'edit', label: 'âï¸ Edit' });
      actions.splice(2, 0, { id: 'versions', label: 'ðï¸ Versions' });
      actions.push({ id: 'delete', label: 'ðï¸ Delete' });
    }

    actions.push({ id: 'back', label: 'â¬ï¸ Back' });

    const action = await askOptionWithButtons(
      conversation,
      ctx,
      'Choose an action for this SMS script.',
      actions,
      { prefix: 'sms-script-action', columns: 2 }
    );

    switch (action.id) {
      case 'preview':
        await previewSmsScript(conversation, ctx, script);
        break;
      case 'edit':
        await editSmsScriptFlow(conversation, ctx, script);
        try {
          script = await fetchSmsScriptByName(script.name, { detailed: true });
        } catch (error) {
          console.error('Failed to refresh SMS script after edit:', error);
          await ctx.reply(formatScriptsApiError(error, 'Failed to refresh script details'));
          viewing = false;
        }
        break;
      case 'clone':
        await cloneSmsScriptFlow(conversation, ctx, script);
        break;
      case 'versions':
        await showSmsScriptVersions(conversation, ctx, script);
        break;
      case 'delete':
        await deleteSmsScriptFlow(conversation, ctx, script);
        viewing = false;
        break;
      case 'back':
        viewing = false;
        break;
      default:
        break;
    }
  }
}

async function listSmsScriptsFlow(conversation, ctx) {
  try {
    const scripts = await fetchSmsScripts();
    if (!scripts.length) {
      await ctx.reply('â¹ï¸ No SMS scripts found. Use the create action to add one.');
      return;
    }

    const custom = scripts.filter((script) => !script.is_builtin);
    const builtin = scripts.filter((script) => script.is_builtin);

    let message = 'ð¬ SMS Scripts\n\n';
    if (custom.length) {
      message += 'Custom scripts:\n';
      message += custom
        .slice(0, 15)
        .map((script) => `â¢ ${script.name}${script.description ? ` â ${script.description}` : ''}`)
        .join('\n');
      message += '\n\n';
    } else {
      message += 'No custom scripts yet.\n\n';
    }

    if (builtin.length) {
      message += 'Built-in scripts:\n';
      message += builtin
        .map((script) => `â¢ ${script.name}${script.description ? ` â ${script.description}` : ''}`)
        .join('\n');
      message += '\n\n';
    }

    message += 'Select a script below to view details.';
    await ctx.reply(message);

    const options = scripts.map((script) => ({
      id: script.name,
      label: `${script.is_builtin ? 'ð¦' : 'ð'} ${script.name}`,
      is_builtin: script.is_builtin
    }));
    options.push({ id: 'back', label: 'â¬ï¸ Back' });

    const selection = await askOptionWithButtons(
      conversation,
      ctx,
      'Choose an SMS script to manage.',
      options,
      { prefix: 'sms-script-select', columns: 1, formatLabel: (option) => option.label }
    );

    if (selection.id === 'back') {
      return;
    }

    try {
      const script = await fetchSmsScriptByName(selection.id, { detailed: true });
      if (!script) {
        await ctx.reply('â Script not found.');
        return;
      }

      await showSmsScriptDetail(conversation, ctx, script);
    } catch (error) {
      console.error('Failed to load SMS script details:', error);
      await ctx.reply(formatScriptsApiError(error, 'Failed to load script details'));
    }
  } catch (error) {
    console.error('Failed to list SMS scripts:', error);
    await ctx.reply(formatScriptsApiError(error, 'Failed to list SMS scripts'));
  }
}

async function smsScriptsMenu(conversation, ctx) {
  let open = true;
  while (open) {
    const action = await askOptionWithButtons(
      conversation,
      ctx,
      'ð¬ *SMS Script Designer*\nChoose an action.',
      [
        { id: 'list', label: 'ð List scripts' },
        { id: 'create', label: 'â Create script' },
        { id: 'back', label: 'â¬ï¸ Back' }
      ],
      { prefix: 'sms-script-main', columns: 1 }
    );

    switch (action.id) {
      case 'list':
        await listSmsScriptsFlow(conversation, ctx);
        break;
      case 'create':
        await createSmsScriptFlow(conversation, ctx);
        break;
      case 'back':
        open = false;
        break;
      default:
        break;
    }
  }
}

async function scriptsFlow(conversation, ctx) {
  const opId = startOperation(ctx, 'scripts');
  const ensureActive = () => ensureOperationActive(ctx, opId);

  try {
    const user = await new Promise((resolve) => getUser(ctx.from.id, resolve));
    ensureActive();
    if (!user) {
      await ctx.reply('â You are not authorized to use this bot.');
      return;
    }

    const adminStatus = await new Promise((resolve) => isAdmin(ctx.from.id, resolve));
    ensureActive();
    if (!adminStatus) {
      await ctx.reply('â This command is for administrators only.');
      return;
    }

    // Warm persona cache so downstream selections have up-to-date personas.
    await getBusinessOptions();
    ensureActive();

    let active = true;
    while (active) {
      const selection = await askOptionWithButtons(
        conversation,
        ctx,
        'ð§° *Script Designer*\nChoose which scripts to manage.',
        [
          { id: 'call', label: 'âï¸ Call scripts' },
          { id: 'sms', label: 'ð¬ SMS scripts' },
          { id: 'email', label: 'ð§ Email templates' },
          { id: 'exit', label: 'ðª Exit' }
        ],
        { prefix: 'script-channel', columns: 1, ensureActive }
      );

      switch (selection.id) {
        case 'call':
          await callScriptsMenu(conversation, ctx, ensureActive);
          break;
        case 'sms':
          await smsScriptsMenu(conversation, ctx, ensureActive);
          break;
        case 'email':
          await emailTemplatesFlow(conversation, ctx, { ensureActive });
          break;
        case 'exit':
          active = false;
          break;
        default:
          break;
      }
    }

    await ctx.reply('â Script designer closed.');
  } catch (error) {
    if (error instanceof OperationCancelledError) {
      console.log('Scripts flow cancelled:', error.message);
      return;
    }
    throw error;
  } finally {
    if (ctx.session?.currentOp?.id === opId) {
      ctx.session.currentOp = null;
    }
  }
}

function registerScriptsCommand(bot) {
  bot.command('scripts', async (ctx) => {
    const user = await new Promise((resolve) => getUser(ctx.from.id, resolve));
    if (!user) {
      return ctx.reply('â You are not authorized to use this bot.');
    }

    const adminStatus = await new Promise((resolve) => isAdmin(ctx.from.id, resolve));
    if (!adminStatus) {
      return ctx.reply('â This command is for administrators only.');
    }

    await ctx.conversation.enter('scripts-conversation');
  });
}

module.exports = {
  scriptsFlow,
  registerScriptsCommand
};
