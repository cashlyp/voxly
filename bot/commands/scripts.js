const axios = require('axios');
const { InlineKeyboard } = require('grammy');
const config = require('../config');
const httpClient = require('../utils/httpClient');
const { withRetry } = require('../utils/httpClient');
const {
  getUser,
  isAdmin
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
  fetchVoiceModelCatalog,
  askVoiceModelWithPagination,
} = require('../utils/voiceModels');
const {
  startOperation,
  ensureOperationActive,
  OperationCancelledError,
  getCurrentOpId,
  guardAgainstCommandInterrupt
} = require('../utils/sessionState');
const { emailTemplatesFlow } = require('./email');
const {
  section,
  buildLine,
  tipLine,
  clearMenuMessages,
  upsertMenuMessage,
  dismissMenuMessage,
  buildMainMenuReplyMarkup,
  selectionExpiredMessage
} = require('../utils/ui');
const { buildCallbackData, matchesCallbackPrefix, parseCallbackData } = require('../utils/actions');
const { attachHmacAuth } = require('../utils/apiAuth');
const {
  RELATIONSHIP_FLOW_TYPES,
} = require('../../api/functions/Dating');
const {
  normalizeCallScriptFlowType: normalizeCallScriptFlowTypeShared,
  normalizeObjectiveTag: normalizeObjectiveTagShared,
  buildObjectiveTagsForFlow: buildObjectiveTagsForFlowShared,
  getCallScriptFlowTypes: getCallScriptFlowTypesShared,
  getPrimaryFlowType: getPrimaryFlowTypeShared,
  getEffectiveObjectiveTags: getEffectiveObjectiveTagsShared,
} = require('../../api/functions/relationshipFlowMetadata');

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
    return `⚠️ ${action}: Script name already exists.${suggestionLine}`;
  }

  if (error.isScriptsApiError && error.reason === 'non_json_response') {
    return `❌ ${action}: Scripts API returned unexpected content (type: ${error.contentType}). ${baseHelp}${
      error.snippet ? `\nSnippet: ${error.snippet}` : ''
    }`;
  }

  if (error.isScriptsApiError && error.reason === 'api_failure') {
    return `❌ ${action}: ${error.message}. ${baseHelp}`;
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
      return `❌ ${action}: Scripts API responded with HTTP ${status} ${statusText}. ${baseHelp}${
        snippet ? `\nSnippet: ${snippet}` : ''
      }`;
    }

    return `❌ ${action}: ${details || `HTTP ${status}`}`;
  }

  if (error.request) {
    return `❌ ${action}: No response from Scripts API. ${baseHelp}`;
  }

  return `❌ ${action}: ${error.message}`;
}

function stableStringify(value) {
  if (value === null || typeof value !== 'object') {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    const items = value.map((item) =>
      item === undefined ? 'null' : stableStringify(item)
    );
    return `[${items.join(',')}]`;
  }
  const keys = Object.keys(value)
    .filter((key) => value[key] !== undefined)
    .sort();
  const entries = keys.map(
    (key) => `${JSON.stringify(key)}:${stableStringify(value[key])}`
  );
  return `{${entries.join(',')}}`;
}

const CANCEL_KEYWORDS = new Set(['cancel', 'exit', 'quit']);
const RELATIONSHIP_FLOW_TYPE_SET = new Set(RELATIONSHIP_FLOW_TYPES);

const CORE_FLOW_LABELS = Object.freeze({
  payment_collection: 'Payment collection',
  identity_verification: 'Identity verification',
  appointment_confirmation: 'Appointment confirmation',
  service_recovery: 'Service recovery',
  general_outreach: 'General outreach',
  general: 'General'
});

const CORE_FLOW_BADGES = Object.freeze({
  payment_collection: '💳 Payment',
  identity_verification: '🔐 Verification',
  appointment_confirmation: '📅 Appointment',
  service_recovery: '🛠️ Recovery',
  general_outreach: '📣 Outreach',
  general: '🧩 General'
});

const RELATIONSHIP_FLOW_LABEL_OVERRIDES = Object.freeze({
  dating: 'Dating',
  celebrity: 'Celebrity fan engagement',
  fan: 'Fan engagement',
  creator: 'Creator collaboration',
  friendship: 'Friendship',
  networking: 'Networking',
  community: 'Community engagement',
  marketplace_seller: 'Marketplace seller',
  real_estate_agent: 'Real estate outreach'
});

const RELATIONSHIP_FLOW_BADGE_OVERRIDES = Object.freeze({
  dating: '💕 Dating',
  celebrity: '⭐ Celebrity',
  fan: '🌟 Fan',
  creator: '🎬 Creator',
  friendship: '🤝 Friendship',
  networking: '📇 Networking',
  community: '👥 Community',
  marketplace_seller: '🛍️ Marketplace',
  real_estate_agent: '🏡 Real estate'
});

function toTitleCase(value = '') {
  return String(value || '')
    .split(/[_\s-]+/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');
}

const RELATIONSHIP_FLOW_LABELS = Object.freeze(
  RELATIONSHIP_FLOW_TYPES.reduce((acc, flowType) => {
    acc[flowType] = RELATIONSHIP_FLOW_LABEL_OVERRIDES[flowType] || toTitleCase(flowType);
    return acc;
  }, {})
);

const RELATIONSHIP_FLOW_BADGES = Object.freeze(
  RELATIONSHIP_FLOW_TYPES.reduce((acc, flowType) => {
    acc[flowType] = RELATIONSHIP_FLOW_BADGE_OVERRIDES[flowType] || `💬 ${toTitleCase(flowType)}`;
    return acc;
  }, {})
);

const CALL_SCRIPT_FLOW_LABELS = Object.freeze({
  ...CORE_FLOW_LABELS,
  ...RELATIONSHIP_FLOW_LABELS
});

const CALL_SCRIPT_FLOW_BADGES = Object.freeze({
  ...CORE_FLOW_BADGES,
  ...RELATIONSHIP_FLOW_BADGES
});

const CALL_SCRIPT_FLOW_FILTER_OPTIONS = Object.freeze([
  { id: 'all', label: '📚 All flows' },
  { id: 'payment_collection', label: '💳 Payment collection' },
  { id: 'identity_verification', label: '🔐 Identity verification' },
  { id: 'appointment_confirmation', label: '📅 Appointment confirmation' },
  { id: 'service_recovery', label: '🛠️ Service recovery' },
  { id: 'general_outreach', label: '📣 General outreach' },
  ...RELATIONSHIP_FLOW_TYPES.map((flowType) => ({
    id: flowType,
    label: `${CALL_SCRIPT_FLOW_BADGES[flowType]}`
  })),
  { id: 'general', label: '🧩 General' }
]);

const CALL_SCRIPT_FLOW_CREATE_OPTIONS = Object.freeze([
  { id: 'auto', label: '⚙️ Auto-detect flow' },
  { id: 'payment_collection', label: '💳 Payment collection' },
  { id: 'identity_verification', label: '🔐 Identity verification' },
  { id: 'appointment_confirmation', label: '📅 Appointment confirmation' },
  { id: 'service_recovery', label: '🛠️ Service recovery' },
  { id: 'general_outreach', label: '📣 General outreach' },
  ...RELATIONSHIP_FLOW_TYPES.map((flowType) => ({
    id: flowType,
    label: `${CALL_SCRIPT_FLOW_BADGES[flowType]}`
  })),
  { id: 'general', label: '🧩 General' }
]);

function isCancelInput(text) {
  return typeof text === 'string' && CANCEL_KEYWORDS.has(text.trim().toLowerCase());
}

function escapeMarkdown(text = '') {
  return text.replace(/([_*[\]`])/g, '\\$1');
}

async function askScriptSelectionWithPagination(
  conversation,
  ctx,
  {
    prompt = 'Select a script.',
    items = [],
    prefix = 'script-select',
    pageSize = 8,
    ensureActive,
    getItemId = (item) => String(item?.id || ''),
    getItemLabel = (item) => String(item?.name || item?.id || ''),
    searchLabel = 'script name',
    includeBack = true
  } = {}
) {
  const safeEnsureActive = typeof ensureActive === 'function'
    ? ensureActive
    : () => ensureOperationActive(ctx, getCurrentOpId(ctx));
  const opToken = String(ctx.session?.currentOp?.token || '').trim();
  const safeItems = (Array.isArray(items) ? items : [])
    .map((item, index) => ({
      item,
      key: String(index),
      id: String(getItemId(item) || '').trim(),
      label: String(getItemLabel(item) || '').trim()
    }))
    .filter((entry) => entry.id && entry.label);

  if (!safeItems.length) {
    return { id: 'back', item: null };
  }

  const normalizedPageSize = Math.max(1, Math.floor(Number(pageSize) || 8));
  let page = 0;
  let activeFilter = '';
  let menuMessage = null;

  const buildKeyboard = ({
    pageEntries = [],
    hasPrev = false,
    hasNext = false,
    hasFilter = false,
    noResults = false
  }) => {
    const keyboard = new InlineKeyboard();
    pageEntries.forEach((entry) => {
      keyboard
        .text(entry.label, buildCallbackData(ctx, `${prefix}:pick:${entry.key}`))
        .row();
    });

    if (noResults) {
      keyboard
        .text('🔎 Search', buildCallbackData(ctx, `${prefix}:__search__`))
        .text(hasFilter ? '✖ Clear Filter' : '⏺', buildCallbackData(ctx, `${prefix}:${hasFilter ? '__clear_search__' : '__noop__'}`))
        .row();
    } else {
      keyboard
        .text(hasPrev ? '⬅️ Previous' : '⏺', buildCallbackData(ctx, `${prefix}:${hasPrev ? '__nav_prev__' : '__noop__'}`))
        .text('🔎 Search', buildCallbackData(ctx, `${prefix}:__search__`))
        .text(hasNext ? 'Next ➡️' : '⏺', buildCallbackData(ctx, `${prefix}:${hasNext ? '__nav_next__' : '__noop__'}`))
        .row();
    }

    if (hasFilter && !noResults) {
      keyboard
        .text('✖ Clear Filter', buildCallbackData(ctx, `${prefix}:__clear_search__`))
        .row();
    }
    if (includeBack) {
      keyboard.text('⬅️ Back', buildCallbackData(ctx, `${prefix}:__back__`)).row();
    }
    return keyboard;
  };

  while (true) {
    const needle = activeFilter.trim().toLowerCase();
    const filtered = needle
      ? safeItems.filter((entry) => `${entry.label} ${entry.id}`.toLowerCase().includes(needle))
      : safeItems;
    const totalPages = Math.max(1, Math.ceil(filtered.length / normalizedPageSize));
    page = Math.min(Math.max(0, page), totalPages - 1);
    const start = page * normalizedPageSize;
    const pageEntries = filtered.slice(start, start + normalizedPageSize);
    const noResults = filtered.length === 0;

    const pageHint = noResults
      ? '\n_No matches. Adjust search or clear filter._'
      : totalPages > 1
      ? `\n_Page ${page + 1}/${totalPages} • ${start + 1}-${start + pageEntries.length} of ${filtered.length}_`
      : `\n_${filtered.length} match${filtered.length === 1 ? '' : 'es'}_`;
    const filterHint = needle ? `\n_Filter: ${escapeMarkdown(needle)}_` : '';
    const header = `${prompt}${filterHint}${pageHint}`;
    const keyboard = buildKeyboard({
      pageEntries,
      hasPrev: totalPages > 1 && page > 0,
      hasNext: totalPages > 1 && page < totalPages - 1,
      hasFilter: Boolean(needle),
      noResults
    });
    menuMessage = await upsertMenuMessage(ctx, menuMessage, header, {
      parse_mode: 'Markdown',
      reply_markup: keyboard
    });

    const selectionCtx = await conversation.waitFor('callback_query:data', (callbackCtx) => {
      const callbackData = callbackCtx?.callbackQuery?.data;
      if (!callbackData) return false;
      if (!matchesCallbackPrefix(callbackData, prefix)) return false;
      const parsed = parseCallbackData(callbackData);
      if (parsed?.signed && opToken && parsed.token && parsed.token !== opToken) {
        return false;
      }
      return true;
    });
    safeEnsureActive();
    await selectionCtx.answerCallbackQuery();

    const selectedData = selectionCtx?.callbackQuery?.data || '';
    const selectedAction = parseCallbackData(selectedData).action || selectedData;
    const prefixSegments = String(prefix).split(':').filter(Boolean).length;
    const selectedId = selectedAction.split(':').slice(prefixSegments).join(':');

    if (selectedId === '__back__') {
      await dismissMenuMessage(ctx, menuMessage);
      return { id: 'back', item: null };
    }
    if (selectedId === '__noop__') {
      continue;
    }
    if (selectedId === '__nav_prev__') {
      if (page > 0) page -= 1;
      continue;
    }
    if (selectedId === '__nav_next__') {
      if (page < totalPages - 1) page += 1;
      continue;
    }
    if (selectedId === '__clear_search__') {
      activeFilter = '';
      page = 0;
      continue;
    }
    if (selectedId === '__search__') {
      let searchPromptMessage = null;
      searchPromptMessage = await upsertMenuMessage(
        ctx,
        searchPromptMessage,
        `🔎 Enter ${searchLabel} filter. Type \`clear\` to reset or \`cancel\` to keep current results.`,
        { parse_mode: 'Markdown' }
      );
      const update = await conversation.wait();
      safeEnsureActive();
      if (searchPromptMessage) {
        await dismissMenuMessage(ctx, searchPromptMessage);
      }
      const input = String(update?.message?.text || '').trim();
      if (!input) continue;
      const lower = input.toLowerCase();
      if (lower === 'cancel') continue;
      if (lower === 'clear') {
        activeFilter = '';
        page = 0;
        continue;
      }
      activeFilter = lower;
      page = 0;
      continue;
    }

    if (selectedId.startsWith('pick:')) {
      const key = selectedId.slice('pick:'.length);
      const selected = safeItems.find((entry) => entry.key === key);
      if (selected) {
        await dismissMenuMessage(ctx, menuMessage);
        return { id: selected.id, item: selected.item };
      }
    }

    await dismissMenuMessage(ctx, menuMessage);
    return { id: 'back', item: null };
  }
}

function normalizeScriptName(name = '') {
  return String(name || '').trim().slice(0, 80);
}

function normalizeCallScriptFlowType(rawType) {
  return normalizeCallScriptFlowTypeShared(rawType);
}

function normalizeObjectiveTag(entry) {
  return normalizeObjectiveTagShared(entry);
}

function buildObjectiveTagsForFlow(flowType = null, existingTags = []) {
  return buildObjectiveTagsForFlowShared(flowType, existingTags);
}

function getCallScriptFlowTypes(script = {}) {
  return getCallScriptFlowTypesShared(script);
}

function getCallScriptPrimaryFlowType(script = {}) {
  return getPrimaryFlowTypeShared(script);
}

function getEffectiveObjectiveTags(script = {}) {
  return getEffectiveObjectiveTagsShared(script);
}

function getCallScriptFlowLabel(flowType) {
  return CALL_SCRIPT_FLOW_LABELS[flowType] || CALL_SCRIPT_FLOW_LABELS.general;
}

function getCallScriptFlowBadge(script = {}) {
  const flowType = getCallScriptPrimaryFlowType(script);
  return CALL_SCRIPT_FLOW_BADGES[flowType] || CALL_SCRIPT_FLOW_BADGES.general;
}

function buildRelationshipFlowNotice(flowType) {
  const label = CALL_SCRIPT_FLOW_LABELS[flowType] || toTitleCase(flowType);
  if (!RELATIONSHIP_FLOW_TYPE_SET.has(flowType)) {
    return null;
  }
  return `${CALL_SCRIPT_FLOW_BADGES[flowType]} flow selected. Runtime will apply ${label.toLowerCase()} profile behavior and context tools.`;
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
  return parts.join(' • ');
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
    await ctx.reply(`❌ ${error.message || 'Invalid value supplied.'}`);
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
      { id: 'yes', label: '✅ Yes' },
      { id: 'no', label: '❌ No' }
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
      `✏️ Enter value for *${escapeMarkdown(placeholder)}* (type skip to leave unchanged, cancel to abort).`,
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
    selectionOptions.unshift({ id: 'cancel', label: '❌ Cancel', custom: true });
  }

  const businessChoice = await askOptionWithButtons(
    conversation,
    ctx,
    `🎭 *Select persona for this script:*
Choose the primary business context.`,
    selectionOptions,
    {
      prefix: 'script-business',
      columns: 2,
      ensureActive: safeEnsureActive,
      formatLabel: (option) => (option.custom && option.id !== 'cancel' ? '✍️ Custom persona' : option.label)
    }
  );

  if (!businessChoice) {
    await ctx.reply('❌ Invalid persona selection. Please try again.');
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
        ? `🎯 *Choose script purpose:*
This helps align tone and follow-up actions.
_Current: ${currentPurposeLabel}_`
        : `🎯 *Choose script purpose:*
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
          formatLabel: (option) => `${option.emoji || '•'} ${option.label}`
        }
      );

      personaConfig.purpose = purposeSelection?.id || null;
      if (purposeSelection?.label) {
        personaSummary.push(`Purpose: ${purposeSelection.label}`);
      }
    }

    const tonePrompt = personaConfig.emotion
      ? `🎙️ *Preferred tone for this script:*
_Current: ${getOptionLabel(MOOD_OPTIONS, personaConfig.emotion)}_`
      : `🎙️ *Preferred tone for this script:*`;

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
      ? `⏱️ *Default urgency:*
_Current: ${getOptionLabel(URGENCY_OPTIONS, personaConfig.urgency)}_`
      : `⏱️ *Default urgency:*`;

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
      ? `🧠 *Recipient technical level:*
_Current: ${getOptionLabel(TECH_LEVEL_OPTIONS, personaConfig.technical_level)}_`
      : `🧠 *Recipient technical level:*`;

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
    '🧠 Provide the system prompt for this call script. This sets the AI behavior.',
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
    '🗣️ Provide the first message the agent says when the call connects.',
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

  const voiceCatalog = await fetchVoiceModelCatalog(ctx);
  const availableModels = Array.isArray(voiceCatalog.models) ? voiceCatalog.models : [];
  if (availableModels.length > 8) {
    await ctx.reply('🔎 Tip: use Search in the voice picker to filter by model id quickly.');
  }

  const currentVoiceModel = defaults.voice_model ? String(defaults.voice_model).trim() : null;
  const voiceSelection = await askVoiceModelWithPagination(
    conversation,
    ctx,
    {
      prompt: '🎙️ Select the voice model for this script.',
      models: availableModels,
      topOptions: [
        { id: 'auto', label: '⚙️ Auto (best for flow)' },
        ...(currentVoiceModel ? [{ id: 'keep', label: `🔁 Keep current (${currentVoiceModel})` }] : [])
      ],
      bottomOptions: [
        { id: 'custom', label: '✍️ Enter custom model id' },
        { id: 'cancel', label: '❌ Cancel' }
      ],
      prefix: 'script-voice-model',
      pageSize: 8,
      ensureActive: safeEnsureActive
    }
  );

  if (!voiceSelection || voiceSelection.id === 'cancel') {
    return null;
  }

  let resolvedVoiceModel = currentVoiceModel || null;
  if (voiceSelection.id === 'auto') {
    resolvedVoiceModel = null;
  } else if (voiceSelection.id.startsWith('model:')) {
    resolvedVoiceModel = voiceSelection.id.slice('model:'.length).trim() || null;
  } else if (voiceSelection.id === 'custom') {
    const customVoice = await promptText(
      conversation,
      ctx,
      '🎤 Enter the Deepgram voice model id (or type skip for Auto).',
      {
        allowEmpty: true,
        allowSkip: true,
        defaultValue: currentVoiceModel || 'auto',
        parse: (value) => value.trim(),
        ensureActive: safeEnsureActive
      }
    );
    if (customVoice === null) {
      return null;
    }
    if (customVoice === undefined || customVoice === '') {
      resolvedVoiceModel = null;
    } else {
      resolvedVoiceModel = customVoice;
    }
  }

  return {
    prompt: prompt === undefined ? defaults.prompt : prompt,
    first_message: firstMessage === undefined ? defaults.first_message : firstMessage,
    voice_model: resolvedVoiceModel
  };
}

async function collectDigitCaptureConfig(conversation, ctx, defaults = {}, ensureActive) {
  const safeEnsureActive = typeof ensureActive === 'function'
    ? ensureActive
    : () => ensureOperationActive(ctx, getCurrentOpId(ctx));
  const selection = await askOptionWithButtons(
    conversation,
    ctx,
    '🔢 Add digit capture to this script?',
    [
      { id: 'none', label: '🚫 None' },
      { id: 'otp', label: '🔐 OTP (code)' },
      { id: 'pin', label: '🔑 PIN' },
      { id: 'routing', label: '🏦 Routing number' },
      { id: 'account', label: '🏦 Account number' },
      { id: 'banking', label: '🏦 Banking group (routing + account)' },
      { id: 'card', label: '💳 Card group (card + expiry + zip + cvv)' },
      { id: 'custom', label: '⚙️ Custom profile' }
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
      '🔢 OTP length (4-8 digits).',
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

async function collectCallFlowType(conversation, ctx, defaults = {}, ensureActive) {
  const safeEnsureActive = typeof ensureActive === 'function'
    ? ensureActive
    : () => ensureOperationActive(ctx, getCurrentOpId(ctx));
  const currentFlow = normalizeCallScriptFlowType(
    defaults.flow_type || getCallScriptPrimaryFlowType(defaults)
  );
  const currentLabel = currentFlow
    ? getCallScriptFlowLabel(currentFlow)
    : 'Auto-detect';

  const selection = await askOptionWithButtons(
    conversation,
    ctx,
    `🧭 Select call flow type for this script.\n_Current: ${currentLabel}_\n_This controls runtime behavior (for example, digit capture or relationship context tools)._`,
    CALL_SCRIPT_FLOW_CREATE_OPTIONS,
    { prefix: 'call-script-flow-type', columns: 1, ensureActive: safeEnsureActive }
  );

  if (!selection || !selection.id) {
    return null;
  }
  if (selection.id === 'auto') {
    return null;
  }
  return normalizeCallScriptFlowType(selection.id);
}

async function fetchCallScripts({ flowType = null } = {}) {
  const params = {};
  if (flowType && flowType !== 'all') {
    params.flow_type = flowType;
  }
  const data = await scriptsApiRequest({
    method: 'get',
    url: '/api/call-scripts',
    params: Object.keys(params).length ? params : undefined
  });
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

async function submitCallScriptForReview(id) {
  const data = await scriptsApiRequest({
    method: 'post',
    url: `/api/call-scripts/${id}/submit-review`,
    data: {},
  });
  return data.script;
}

async function reviewCallScript(id, decision, note = null) {
  const data = await scriptsApiRequest({
    method: 'post',
    url: `/api/call-scripts/${id}/review`,
    data: { decision, note },
  });
  return data.script;
}

async function promoteCallScriptLive(id) {
  const data = await scriptsApiRequest({
    method: 'post',
    url: `/api/call-scripts/${id}/promote-live`,
    data: {},
  });
  return data.script;
}

async function listCallScriptApiVersions(id) {
  const data = await scriptsApiRequest({
    method: 'get',
    url: `/api/call-scripts/${id}/versions`,
  });
  return data.versions || [];
}

async function diffCallScriptApiVersions(id, fromVersion, toVersion) {
  const data = await scriptsApiRequest({
    method: 'get',
    url: `/api/call-scripts/${id}/diff`,
    params: {
      from_version: fromVersion,
      to_version: toVersion,
    },
  });
  return data;
}

async function rollbackCallScriptApiVersion(id, version) {
  const data = await scriptsApiRequest({
    method: 'post',
    url: `/api/call-scripts/${id}/rollback`,
    data: { version },
  });
  return data.script;
}

async function simulateCallScriptApi(id, variables = {}) {
  const data = await scriptsApiRequest({
    method: 'post',
    url: `/api/call-scripts/${id}/simulate`,
    data: { variables },
  });
  return data.simulation || {};
}

function getCallScriptLifecycleState(script = {}) {
  const raw = script?.lifecycle?.lifecycle_state || script?.lifecycle_state || 'draft';
  return String(raw || 'draft').trim().toLowerCase();
}

function getCallScriptLifecycleBadge(script = {}) {
  const state = getCallScriptLifecycleState(script);
  switch (state) {
    case 'review':
      return '🟠 In Review';
    case 'approved':
      return '✅ Approved';
    case 'live':
      return '🟢 Live';
    default:
      return '📝 Draft';
  }
}

function formatCallScriptSummary(script) {
  const summary = [];
  const flowTypes = getCallScriptFlowTypes(script);
  const objectiveTags = getEffectiveObjectiveTags(script);
  summary.push(`📛 *${escapeMarkdown(script.name)}*`);
  summary.push(`🧾 Lifecycle: ${escapeMarkdown(getCallScriptLifecycleBadge(script))}`);
  if (script?.lifecycle?.review_note) {
    summary.push(`🗒️ Review note: ${escapeMarkdown(String(script.lifecycle.review_note).slice(0, 180))}`);
  }
  summary.push(`🧭 Flow type: ${escapeMarkdown(getCallScriptFlowLabel(flowTypes[0] || 'general'))}`);
  if (flowTypes.length > 1) {
    summary.push(`🗂️ Flow coverage: ${flowTypes.slice(1).map((flow) => escapeMarkdown(getCallScriptFlowLabel(flow))).join(', ')}`);
  }
  summary.push(
    `🏷️ Objective tags: ${escapeMarkdown(objectiveTags.length ? objectiveTags.join(', ') : 'none')}`
  );
  if (script.description) {
    summary.push(`📝 ${escapeMarkdown(script.description)}`);
  }
  if (script.business_id) {
    const business = findBusinessOption(script.business_id);
    summary.push(`🏢 Persona: ${escapeMarkdown(business ? business.label : script.business_id)}`);
  }
  const personaSummary = buildPersonaSummaryFromConfig(script);
  if (personaSummary.length) {
    personaSummary.forEach((line) => summary.push(`• ${escapeMarkdown(line)}`));
  }

  const captureSummary = buildDigitCaptureSummary(script);
  summary.push(`🔢 Digit capture: ${escapeMarkdown(captureSummary)}`);

  if (script.voice_model) {
    summary.push(`🎤 Voice model: ${escapeMarkdown(script.voice_model)}`);
  }

  const placeholders = new Set([
    ...extractScriptVariables(script.prompt || ''),
    ...extractScriptVariables(script.first_message || '')
  ]);
  if (placeholders.size > 0) {
    summary.push(`🧩 Placeholders: ${Array.from(placeholders).map(escapeMarkdown).join(', ')}`);
  }

  if (script.prompt) {
    const snippet = script.prompt.substring(0, 160);
    summary.push(`📜 Prompt snippet: ${escapeMarkdown(snippet)}${script.prompt.length > 160 ? '…' : ''}`);
  }
  if (script.first_message) {
    const snippet = script.first_message.substring(0, 160);
    summary.push(`🗨️ First message: ${escapeMarkdown(snippet)}${script.first_message.length > 160 ? '…' : ''}`);
  }
  summary.push(
    `📅 Updated: ${escapeMarkdown(new Date(script.updated_at || script.created_at).toLocaleString())}`
  );
  return summary.join('\n');
}

async function previewCallScript(conversation, ctx, script, ensureActive) {
  const safeEnsureActive = typeof ensureActive === 'function'
    ? ensureActive
    : () => ensureOperationActive(ctx, getCurrentOpId(ctx));
  const phonePrompt =
    '📞 Enter the test phone number (E.164 format, e.g., +1234567890) to receive a preview call.';
  const testNumber = await promptText(conversation, ctx, phonePrompt, {
    allowEmpty: false,
    ensureActive: safeEnsureActive
  });
  if (!testNumber) {
    await ctx.reply('❌ Preview cancelled.');
    return;
  }

  if (!/^\+[1-9]\d{1,14}$/.test(testNumber)) {
    await ctx.reply('❌ Invalid phone number format. Preview cancelled.');
    return;
  }

  const placeholderSet = new Set();
  extractScriptVariables(script.prompt || '').forEach((token) => placeholderSet.add(token));
  extractScriptVariables(script.first_message || '').forEach((token) => placeholderSet.add(token));

  let prompt = script.prompt;
  let firstMessage = script.first_message;

  if (placeholderSet.size > 0) {
    await ctx.reply('🧩 This script has placeholders. Provide values where needed (type skip to leave unchanged).');
    const values = await collectPlaceholderValues(conversation, ctx, Array.from(placeholderSet), safeEnsureActive);
    if (values === null) {
      await ctx.reply('❌ Preview cancelled.');
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
  const flowTypes = getCallScriptFlowTypes(script);
  const primaryFlow = flowTypes[0] || 'general';
  const objectiveTags = getEffectiveObjectiveTags(script);

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
  if (RELATIONSHIP_FLOW_TYPE_SET.has(primaryFlow)) {
    payload.call_profile = primaryFlow;
    payload.conversation_profile = primaryFlow;
    payload.conversation_profile_lock = true;
    payload.purpose = primaryFlow;
  }

  await ctx.reply(
    `🧭 Preview flow: ${getCallScriptFlowLabel(primaryFlow)}\n🏷️ Objective tags: ${objectiveTags.length ? objectiveTags.join(', ') : 'none'}`
  );

  try {
    await httpClient.post(null, `${config.apiUrl}/outbound-call`, payload, {
      headers: { 'Content-Type': 'application/json' },
      timeout: 30000
    });

    await ctx.reply('✅ Preview call launched! You should receive a call shortly.');
  } catch (error) {
    console.error('Failed to launch preview call:', error?.response?.data || error.message);
    await ctx.reply(`❌ Preview failed: ${error?.response?.data?.error || error.message}`);
  }
}

async function createCallScriptFlow(conversation, ctx, ensureActive) {
  const safeEnsureActive = typeof ensureActive === 'function'
    ? ensureActive
    : () => ensureOperationActive(ctx, getCurrentOpId(ctx));
  const name = await promptText(
    conversation,
    ctx,
    '🆕 *Script name*\nEnter a unique name for this call script.',
    {
      allowEmpty: false,
      parse: (value) => value.trim(),
      ensureActive: safeEnsureActive
    }
  );

  if (!name) {
    await ctx.reply('❌ Script creation cancelled.');
    return;
  }

  const description = await promptText(
    conversation,
    ctx,
    '📝 Provide an optional description for this script (or type skip).',
    {
      allowEmpty: true,
      allowSkip: true,
      parse: (value) => value.trim(),
      ensureActive: safeEnsureActive
    }
  );
  if (description === null) {
    await ctx.reply('❌ Script creation cancelled.');
    return;
  }

  const personaResult = await collectPersonaConfig(conversation, ctx, {}, { allowCancel: true, ensureActive: safeEnsureActive });
  if (!personaResult) {
    await ctx.reply('❌ Script creation cancelled.');
    return;
  }

  const promptAndVoice = await collectPromptAndVoice(conversation, ctx, {}, safeEnsureActive);
  if (!promptAndVoice) {
    await ctx.reply('❌ Script creation cancelled.');
    return;
  }

  const flowType = await collectCallFlowType(conversation, ctx, {}, safeEnsureActive);
  const isRelationshipFlow = RELATIONSHIP_FLOW_TYPE_SET.has(flowType);
  const relationshipNotice = buildRelationshipFlowNotice(flowType);
  if (relationshipNotice) {
    await ctx.reply(relationshipNotice);
  }

  let captureConfig = {
    requires_otp: false,
    default_profile: null,
    expected_length: null,
    allow_terminator: false,
    terminator_char: null,
    capture_group: null
  };

  if (isRelationshipFlow) {
    const addCapture = await confirm(
      conversation,
      ctx,
      'Add optional digit capture settings to this relationship profile script?',
      safeEnsureActive
    );
    if (addCapture) {
      const optionalCaptureConfig = await collectDigitCaptureConfig(conversation, ctx, {}, safeEnsureActive);
      if (!optionalCaptureConfig) {
        await ctx.reply('❌ Script creation cancelled.');
        return;
      }
      captureConfig = optionalCaptureConfig;
    }
  } else {
    captureConfig = await collectDigitCaptureConfig(conversation, ctx, {}, safeEnsureActive);
    if (!captureConfig) {
      await ctx.reply('❌ Script creation cancelled.');
      return;
    }
  }

  if (captureConfig.capture_group) {
    await ctx.reply('ℹ️ Capture groups are guidance-only; the API still infers groups from the prompt text.');
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
    capture_group: captureConfig.capture_group || null,
    flow_type: flowType || null,
    objective_tags: buildObjectiveTagsForFlow(flowType, []),
  };

  if (RELATIONSHIP_FLOW_TYPE_SET.has(flowType) && scriptPayload.persona_config) {
    scriptPayload.persona_config = {
      ...scriptPayload.persona_config,
      purpose: flowType
    };
  }

  const validation = validateCallScriptPayload(scriptPayload);
  if (validation.errors.length) {
    await ctx.reply(`❌ Fix the following issues:\n• ${validation.errors.join('\n• ')}`);
    return;
  }
  if (validation.warnings.length) {
    await ctx.reply(`⚠️ Warnings:\n• ${validation.warnings.join('\n• ')}`);
    const proceed = await confirm(conversation, ctx, 'Proceed anyway?', safeEnsureActive);
    if (!proceed) {
      await ctx.reply('❌ Script creation cancelled.');
      return;
    }
  }

  try {
    const apiPayload = { ...scriptPayload };
    delete apiPayload.capture_group;
    if (!apiPayload.flow_type) {
      delete apiPayload.flow_type;
    }
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
    await ctx.reply(`✅ Script *${escapeMarkdown(script.name)}* created successfully!`, { parse_mode: 'Markdown' });
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
    '✏️ Update script name (or type skip to keep current).',
    {
      allowEmpty: false,
      allowSkip: true,
      defaultValue: script.name,
      parse: (value) => value.trim(),
      ensureActive: safeEnsureActive
    }
  );
  if (name === null) {
    await ctx.reply('❌ Update cancelled.');
    return;
  }
  if (name !== undefined) {
    if (!name.length) {
      await ctx.reply('❌ Script name cannot be empty.');
      return;
    }
    updates.name = name;
  }

  const description = await promptText(
    conversation,
    ctx,
    '📝 Update description (or type skip).',
    {
      allowEmpty: true,
      allowSkip: true,
      defaultValue: script.description || '',
      parse: (value) => value.trim(),
      ensureActive: safeEnsureActive
    }
  );
  if (description === null) {
    await ctx.reply('❌ Update cancelled.');
    return;
  }
  if (description !== undefined) {
    updates.description = description.length ? description : null;
  }

  const adjustPersona = await confirm(conversation, ctx, 'Would you like to update the persona settings?', safeEnsureActive);
  if (adjustPersona) {
    const personaResult = await collectPersonaConfig(conversation, ctx, script, { allowCancel: true, ensureActive: safeEnsureActive });
    if (!personaResult) {
      await ctx.reply('❌ Update cancelled.');
      return;
    }
    updates.business_id = personaResult.business_id;
    updates.persona_config = personaResult.persona_config;
  }

  const adjustPrompt = await confirm(conversation, ctx, 'Update prompt, first message, or voice settings?', safeEnsureActive);
  if (adjustPrompt) {
    const promptAndVoice = await collectPromptAndVoice(conversation, ctx, script, safeEnsureActive);
    if (!promptAndVoice) {
      await ctx.reply('❌ Update cancelled.');
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
      await ctx.reply('❌ Update cancelled.');
      return;
    }
    updates.requires_otp = captureConfig.requires_otp || false;
    updates.default_profile = captureConfig.default_profile || null;
    updates.expected_length = captureConfig.expected_length || null;
    updates.allow_terminator = captureConfig.allow_terminator || false;
    updates.terminator_char = captureConfig.terminator_char || null;
    updates.capture_group = captureConfig.capture_group || null;
  }

  const adjustFlow = await confirm(conversation, ctx, 'Update flow type settings?', safeEnsureActive);
  if (adjustFlow) {
    const flowType = await collectCallFlowType(conversation, ctx, script, safeEnsureActive);
    updates.flow_type = flowType || null;
    updates.objective_tags = buildObjectiveTagsForFlow(
      flowType,
      Array.isArray(script.objective_tags) ? script.objective_tags : []
    );

    if (RELATIONSHIP_FLOW_TYPE_SET.has(flowType)) {
      const flowNotice = buildRelationshipFlowNotice(flowType);
      if (flowNotice) {
        await ctx.reply(flowNotice);
      }
      updates.persona_config = {
        ...(updates.persona_config || script.persona_config || {}),
        purpose: flowType
      };
    }
  }

  if (Object.keys(updates).length === 0) {
    await ctx.reply('ℹ️ No changes made.');
    return;
  }

  const merged = { ...script, ...updates };
  const validation = validateCallScriptPayload(merged);
  if (validation.errors.length) {
    await ctx.reply(`❌ Fix the following issues:\n• ${validation.errors.join('\n• ')}`);
    return;
  }
  if (validation.warnings.length) {
    await ctx.reply(`⚠️ Warnings:\n• ${validation.warnings.join('\n• ')}`);
    const proceed = await confirm(conversation, ctx, 'Proceed anyway?', safeEnsureActive);
    if (!proceed) {
      await ctx.reply('❌ Update cancelled.');
      return;
    }
  }

  try {
    const apiUpdates = stripUndefined({ ...updates });
    delete apiUpdates.capture_group;
    if (!apiUpdates.flow_type) {
      delete apiUpdates.flow_type;
    }
    const updated = await updateCallScript(script.id, apiUpdates);
    await ctx.reply(`✅ Script *${escapeMarkdown(updated.name)}* updated.`, { parse_mode: 'Markdown' });
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
    `🆕 Enter a name for the clone of *${escapeMarkdown(script.name)}*.`,
    {
      allowEmpty: false,
      parse: (value) => value.trim(),
      defaultValue: null,
      ensureActive: safeEnsureActive
    }
  );
  if (!name) {
    await ctx.reply('❌ Clone cancelled.');
    return;
  }

  const description = await promptText(
    conversation,
    ctx,
    '📝 Optionally provide a description for the new script (or type skip).',
    {
      allowEmpty: true,
      allowSkip: true,
      defaultValue: script.description || '',
      parse: (value) => value.trim(),
      ensureActive: safeEnsureActive
    }
  );
  if (description === null) {
    await ctx.reply('❌ Clone cancelled.');
    return;
  }

  try {
    const cloned = await cloneCallScript(script.id, {
      name,
      description: description === undefined ? script.description : (description.length ? description : null)
    });
    await ctx.reply(`✅ Script cloned as *${escapeMarkdown(cloned.name)}*.`, { parse_mode: 'Markdown' });
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
    await deleteCallScript(script.id);
    await ctx.reply(`🗑️ Script *${escapeMarkdown(script.name)}* deleted.`, { parse_mode: 'Markdown' });
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
    const versions = await listCallScriptApiVersions(script.id);
    safeEnsureActive();
    if (!versions.length) {
      await ctx.reply('ℹ️ No governance versions found yet.');
      return;
    }
    const lines = versions.map((v) => {
      const reason = v.reason ? ` • ${v.reason}` : '';
      return `v${v.version} • ${new Date(v.created_at).toLocaleString()}${reason}`;
    });
    await ctx.reply(`🗂️ API versions\n${lines.join('\n')}`);
  } catch (error) {
    console.error('Version list failed:', error);
    await ctx.reply(formatScriptsApiError(error, 'Failed to list versions'));
  }
}

async function rollbackCallScriptVersionFlow(conversation, ctx, script, ensureActive) {
  const safeEnsureActive = typeof ensureActive === 'function'
    ? ensureActive
    : () => ensureOperationActive(ctx, getCurrentOpId(ctx));
  try {
    const versions = await listCallScriptApiVersions(script.id);
    safeEnsureActive();
    if (!versions.length) {
      await ctx.reply('ℹ️ No versions available for rollback.');
      return script;
    }
    const options = versions.slice(0, 12).map((v) => ({
      id: String(v.version),
      label: `↩️ Rollback to v${v.version}`
    }));
    options.push({ id: 'back', label: '⬅️ Back' });
    const selection = await askOptionWithButtons(
      conversation,
      ctx,
      'Select a version to rollback to.',
      options,
      { prefix: 'call-script-rollback', columns: 2, ensureActive: safeEnsureActive }
    );
    if (!selection || selection.id === 'back') return script;
    const versionNumber = Number(selection.id);
    if (Number.isNaN(versionNumber)) {
      await ctx.reply('❌ Invalid version selected.');
      return script;
    }
    const ok = await confirm(
      conversation,
      ctx,
      `Rollback *${escapeMarkdown(script.name)}* to v${versionNumber}?`,
      safeEnsureActive
    );
    if (!ok) {
      await ctx.reply('Rollback cancelled.');
      return script;
    }
    const updated = await rollbackCallScriptApiVersion(script.id, versionNumber);
    await ctx.reply(
      `✅ Rolled back to v${versionNumber} (${escapeMarkdown(updated.name)}).`,
      { parse_mode: 'Markdown' }
    );
    return updated || script;
  } catch (error) {
    console.error('Rollback flow failed:', error);
    await ctx.reply(formatScriptsApiError(error, 'Failed to rollback version'));
    return script;
  }
}

async function showCallScriptVersionDiffFlow(conversation, ctx, script, ensureActive) {
  const safeEnsureActive = typeof ensureActive === 'function'
    ? ensureActive
    : () => ensureOperationActive(ctx, getCurrentOpId(ctx));
  try {
    const versions = await listCallScriptApiVersions(script.id);
    safeEnsureActive();
    if (versions.length < 2) {
      await ctx.reply('ℹ️ At least two versions are required to compare changes.');
      return;
    }
    const options = versions.slice(0, 12).map((v) => ({
      id: String(v.version),
      label: `v${v.version}`
    }));
    const fromSelection = await askOptionWithButtons(
      conversation,
      ctx,
      'Select the *from* version.',
      [...options, { id: 'back', label: '⬅️ Back' }],
      { prefix: 'call-script-diff-from', columns: 3, ensureActive: safeEnsureActive }
    );
    if (!fromSelection || fromSelection.id === 'back') return;
    const toSelection = await askOptionWithButtons(
      conversation,
      ctx,
      'Select the *to* version.',
      [...options, { id: 'back', label: '⬅️ Back' }],
      { prefix: 'call-script-diff-to', columns: 3, ensureActive: safeEnsureActive }
    );
    if (!toSelection || toSelection.id === 'back') return;

    const fromVersion = Number(fromSelection.id);
    const toVersion = Number(toSelection.id);
    if (!Number.isFinite(fromVersion) || !Number.isFinite(toVersion) || fromVersion === toVersion) {
      await ctx.reply('❌ Select two different versions to compare.');
      return;
    }

    const diff = await diffCallScriptApiVersions(script.id, fromVersion, toVersion);
    const changes = Array.isArray(diff?.changes) ? diff.changes : [];
    if (!changes.length) {
      await ctx.reply(`ℹ️ No differences between v${fromVersion} and v${toVersion}.`);
      return;
    }
    const lines = changes
      .slice(0, 20)
      .map((entry) => `• *${escapeMarkdown(entry.field)}*: \`${escapeMarkdown(stableStringify(entry.from))}\` → \`${escapeMarkdown(stableStringify(entry.to))}\``);
    await ctx.reply(
      `🧮 Version diff v${fromVersion} → v${toVersion}\n${lines.join('\n')}${changes.length > 20 ? '\n… (truncated)' : ''}`,
      { parse_mode: 'Markdown' }
    );
  } catch (error) {
    console.error('Diff flow failed:', error);
    await ctx.reply(formatScriptsApiError(error, 'Failed to diff versions'));
  }
}

async function simulateCallScriptFlow(conversation, ctx, script, ensureActive) {
  const safeEnsureActive = typeof ensureActive === 'function'
    ? ensureActive
    : () => ensureOperationActive(ctx, getCurrentOpId(ctx));
  try {
    const placeholders = Array.from(new Set([
      ...extractScriptVariables(script.prompt || ''),
      ...extractScriptVariables(script.first_message || '')
    ]));
    let values = {};
    if (placeholders.length > 0) {
      await ctx.reply('🧪 Simulation mode: provide placeholder values (or type skip).');
      const collected = await collectPlaceholderValues(conversation, ctx, placeholders, safeEnsureActive);
      safeEnsureActive();
      if (collected === null) {
        await ctx.reply('❌ Simulation cancelled.');
        return;
      }
      values = collected;
    }
    const simulation = await simulateCallScriptApi(script.id, values);
    const missing = Array.isArray(simulation.missing_variables)
      ? simulation.missing_variables
      : [];
    const renderedPrompt = String(simulation.rendered_prompt || '').slice(0, 300);
    const renderedFirstMessage = String(simulation.rendered_first_message || '').slice(0, 300);
    await ctx.reply(
      `🧪 Simulation result\n` +
      `Lifecycle: ${escapeMarkdown(getCallScriptLifecycleBadge(script))}\n` +
      `Missing variables: ${escapeMarkdown(missing.length ? missing.join(', ') : 'none')}\n` +
      `\n📜 Prompt:\n${escapeMarkdown(renderedPrompt)}${String(simulation.rendered_prompt || '').length > 300 ? '…' : ''}\n` +
      `\n🗨️ First message:\n${escapeMarkdown(renderedFirstMessage)}${String(simulation.rendered_first_message || '').length > 300 ? '…' : ''}`,
      { parse_mode: 'Markdown' }
    );
  } catch (error) {
    console.error('Simulation flow failed:', error);
    await ctx.reply(formatScriptsApiError(error, 'Failed to simulate script'));
  }
}

async function showCallScriptDetail(conversation, ctx, script, ensureActive) {
  const safeEnsureActive = typeof ensureActive === 'function'
    ? ensureActive
    : () => ensureOperationActive(ctx, getCurrentOpId(ctx));
  let summaryMessage = null;
  let viewing = true;
  try {
    while (viewing) {
      const summary = formatCallScriptSummary(script);
      summaryMessage = await upsertMenuMessage(ctx, summaryMessage, summary, { parse_mode: 'Markdown' });
      const lifecycleState = getCallScriptLifecycleState(script);
      const actions = [
        { id: 'preview', label: '📞 Preview' },
        { id: 'simulate', label: '🧪 Simulate' },
        { id: 'edit', label: '✏️ Edit' },
        { id: 'clone', label: '🧬 Clone' },
        { id: 'versions', label: '🗂️ Versions' },
        { id: 'diff', label: '🧮 Diff' },
        { id: 'rollback', label: '↩️ Rollback' }
      ];
      if (lifecycleState === 'draft') {
        actions.push({ id: 'submit_review', label: '📨 Submit Review' });
      } else if (lifecycleState === 'review') {
        actions.push({ id: 'approve', label: '✅ Approve' });
        actions.push({ id: 'reject', label: '↩️ Reject' });
      } else if (lifecycleState === 'approved') {
        actions.push({ id: 'promote_live', label: '🚀 Promote Live' });
      }
      actions.push({ id: 'delete', label: '🗑️ Delete' });
      actions.push({ id: 'back', label: '⬅️ Back' });

      const action = await askOptionWithButtons(
        conversation,
        ctx,
        'Choose an action for this script.',
        actions,
        {
          prefix: 'call-script-action',
          columns: 2,
          ensureActive: safeEnsureActive,
          keepMessage: summaryMessage
        }
      );

      if (!action?.id) {
        await ctx.reply(selectionExpiredMessage(), { parse_mode: 'Markdown' });
        continue;
      }

      switch (action.id) {
        case 'preview':
          await previewCallScript(conversation, ctx, script, safeEnsureActive);
          break;
        case 'simulate':
          await simulateCallScriptFlow(conversation, ctx, script, safeEnsureActive);
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
        case 'diff':
          await showCallScriptVersionDiffFlow(conversation, ctx, script, safeEnsureActive);
          break;
        case 'rollback':
          script = await rollbackCallScriptVersionFlow(conversation, ctx, script, safeEnsureActive);
          break;
        case 'submit_review':
          try {
            script = await submitCallScriptForReview(script.id);
            await ctx.reply('✅ Script submitted for review.');
          } catch (error) {
            await ctx.reply(formatScriptsApiError(error, 'Failed to submit review'));
          }
          break;
        case 'approve': {
          const note = await promptText(
            conversation,
            ctx,
            'Optional approval note (type skip to leave blank).',
            { allowEmpty: true, allowSkip: true, parse: (value) => value.trim(), ensureActive: safeEnsureActive }
          );
          if (note === null) {
            await ctx.reply('Approval cancelled.');
            break;
          }
          try {
            script = await reviewCallScript(
              script.id,
              'approve',
              note === undefined ? null : note
            );
            await ctx.reply('✅ Script approved.');
          } catch (error) {
            await ctx.reply(formatScriptsApiError(error, 'Failed to approve script'));
          }
          break;
        }
        case 'reject': {
          const note = await promptText(
            conversation,
            ctx,
            'Optional rejection note (type skip to use default).',
            { allowEmpty: true, allowSkip: true, parse: (value) => value.trim(), ensureActive: safeEnsureActive }
          );
          if (note === null) {
            await ctx.reply('Rejection cancelled.');
            break;
          }
          try {
            script = await reviewCallScript(
              script.id,
              'reject',
              note === undefined ? null : note
            );
            await ctx.reply('↩️ Script returned to draft.');
          } catch (error) {
            await ctx.reply(formatScriptsApiError(error, 'Failed to reject script'));
          }
          break;
        }
        case 'promote_live': {
          const ok = await confirm(
            conversation,
            ctx,
            `Promote *${escapeMarkdown(script.name)}* to live?`,
            safeEnsureActive
          );
          if (!ok) {
            await ctx.reply('Promotion cancelled.');
            break;
          }
          try {
            script = await promoteCallScriptLive(script.id);
            await ctx.reply('🚀 Script promoted to live.');
          } catch (error) {
            await ctx.reply(formatScriptsApiError(error, 'Failed to promote script'));
          }
          break;
        }
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
  } finally {
    if (summaryMessage) {
      await dismissMenuMessage(ctx, summaryMessage);
    }
  }
}

async function listCallScriptsFlow(conversation, ctx, ensureActive, options = {}) {
  const safeEnsureActive = typeof ensureActive === 'function'
    ? ensureActive
    : () => ensureOperationActive(ctx, getCurrentOpId(ctx));
  const selectedFlow = normalizeCallScriptFlowType(options.flowType);
  let headerMessage = null;
  try {
    await clearMenuMessages(ctx);
    const scripts = await fetchCallScripts({ flowType: selectedFlow });
    safeEnsureActive();
    const list = Array.isArray(scripts) ? scripts : [];
    const validScripts = list.filter((script) => script && typeof script.id !== 'undefined' && script.id !== null);

    if (!validScripts.length) {
      if (scripts && scripts.length && scripts.some((t) => !t || typeof t.id === 'undefined')) {
        console.warn('Script list contained invalid entries, ignoring malformed records.');
      }
      if (selectedFlow) {
        await ctx.reply(`ℹ️ No call scripts found for flow: ${getCallScriptFlowLabel(selectedFlow)}.`);
      } else {
        await ctx.reply('ℹ️ No call scripts found. Use the create action to add one.');
      }
      return;
    }

    const header = selectedFlow
      ? `☎️ Call Scripts (${getCallScriptFlowLabel(selectedFlow)})`
      : '☎️ Call Scripts';
    headerMessage = await upsertMenuMessage(
      ctx,
      headerMessage,
      `${header}\n\n${validScripts.length} script${validScripts.length === 1 ? '' : 's'} found. Use Search to filter quickly.`
    );

    const selection = await askScriptSelectionWithPagination(
      conversation,
      ctx,
      {
        prompt: 'Choose a call script to manage.',
        items: validScripts,
        prefix: 'call-script-select',
        pageSize: 8,
        ensureActive: safeEnsureActive,
        searchLabel: 'script name or flow',
        getItemId: (script) => String(script.id),
        getItemLabel: (script) => {
          const flowLabel = getCallScriptFlowLabel(getCallScriptPrimaryFlowType(script));
          return `${getCallScriptFlowBadge(script)} ${script.name} · ${flowLabel}`;
        }
      }
    );

    if (!selection || !selection.id) {
      await ctx.reply('❌ No selection received. Please try again.');
      return;
    }

    if (selection.id === 'back') {
      return;
    }

    const scriptId = Number(selection.id);
    if (Number.isNaN(scriptId)) {
      await ctx.reply('❌ Invalid script selection.');
      return;
    }

    try {
      const script = await fetchCallScriptById(scriptId);
      safeEnsureActive();
      if (!script) {
        await ctx.reply('❌ Script not found.');
        return;
      }

      if (headerMessage) {
        await dismissMenuMessage(ctx, headerMessage);
        headerMessage = null;
      }
      await showCallScriptDetail(conversation, ctx, script, safeEnsureActive);
    } catch (error) {
      console.error('Failed to load call script details:', error);
      await ctx.reply(formatScriptsApiError(error, 'Failed to load script details'));
    }
  } catch (error) {
    console.error('Failed to list scripts:', error);
    await ctx.reply(formatScriptsApiError(error, 'Failed to list call scripts'));
  } finally {
    if (headerMessage) {
      await dismissMenuMessage(ctx, headerMessage);
    }
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
      ? `📥 Current inbound default: ${current.script.name} (ID ${current.script_id})`
      : '📥 Current inbound default: Built-in default';
    const previewLine = current?.mode === 'script' && current?.script?.first_message
      ? `🗨️ First message: ${current.script.first_message.slice(0, 140)}${current.script.first_message.length > 140 ? '…' : ''}`
      : null;

    const action = await askOptionWithButtons(
      conversation,
      ctx,
      `${currentLabel}${previewLine ? `\n${previewLine}` : ''}\n\nChoose an action.`,
      [
        { id: 'set', label: '✅ Set default' },
        { id: 'clear', label: '↩️ Revert to built-in' },
        { id: 'back', label: '⬅️ Back' }
      ],
      { prefix: 'inbound-default', columns: 1, ensureActive: safeEnsureActive }
    );

    if (!action?.id) {
      await ctx.reply(selectionExpiredMessage(), { parse_mode: 'Markdown' });
      continue;
    }

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
          await ctx.reply('ℹ️ No call scripts available. Create one first.');
          break;
        }
        const eligibleScripts = scripts.filter((script) => {
          const state = getCallScriptLifecycleState(script);
          return state === 'approved' || state === 'live';
        });
        if (!eligibleScripts.length) {
          await ctx.reply('ℹ️ No approved/live scripts are available. Approve a script first.');
          break;
        }

        const selection = await askScriptSelectionWithPagination(
          conversation,
          ctx,
          {
            prompt: 'Select a script to use as the inbound default.',
            items: eligibleScripts,
            prefix: 'inbound-default-select',
            pageSize: 8,
            ensureActive: safeEnsureActive,
            searchLabel: 'script name or flow',
            getItemId: (script) => String(script.id),
            getItemLabel: (script) => {
              const flowType = getCallScriptPrimaryFlowType(script);
              return `${getCallScriptFlowBadge(script)} ${script.name} · ${getCallScriptFlowLabel(flowType)} · ${getCallScriptLifecycleBadge(script)}`;
            }
          }
        );

        if (!selection || !selection.id || selection.id === 'back') {
          break;
        }

        const scriptId = Number(selection.id);
        if (Number.isNaN(scriptId)) {
          await ctx.reply('❌ Invalid script selection.');
          break;
        }

        try {
          const result = await setInboundDefaultScript(scriptId);
          safeEnsureActive();
          await ctx.reply(`✅ Inbound default set to ${result?.script?.name || 'selected script'}.`);
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
          await ctx.reply('✅ Inbound default reverted to built-in settings.');
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
      '☎️ *Call Script Designer*\nChoose an action.',
      [
        { id: 'list', label: '📄 List scripts' },
        { id: 'create', label: '➕ Create script' },
        { id: 'flow', label: '🧭 List by flow type' },
        { id: 'incoming', label: '📥 Incoming default' },
        { id: 'back', label: '⬅️ Back to Script Designer' }
      ],
      { prefix: 'call-script-main', columns: 1, ensureActive: safeEnsureActive }
    );

    if (!action?.id) {
      await ctx.reply(selectionExpiredMessage(), { parse_mode: 'Markdown' });
      continue;
    }

    switch (action.id) {
      case 'list':
        await listCallScriptsFlow(conversation, ctx, safeEnsureActive);
        break;
      case 'create':
        await createCallScriptFlow(conversation, ctx, safeEnsureActive);
        break;
      case 'flow': {
        const flowSelection = await askOptionWithButtons(
          conversation,
          ctx,
          'Select the flow type to list.',
          [
            ...CALL_SCRIPT_FLOW_FILTER_OPTIONS,
            { id: 'back', label: '⬅️ Back to Call Scripts' }
          ],
          { prefix: 'call-script-flow', columns: 1, ensureActive: safeEnsureActive }
        );
        if (flowSelection?.id && flowSelection.id !== 'back') {
          await listCallScriptsFlow(conversation, ctx, safeEnsureActive, { flowType: flowSelection.id });
        }
        break;
      }
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

async function submitSmsScriptForReview(name) {
  const data = await scriptsApiRequest({
    method: 'post',
    url: `/api/sms/scripts/${encodeURIComponent(name)}/submit-review`,
    data: {},
  });
  return data.script;
}

async function reviewSmsScript(name, decision, note = null) {
  const data = await scriptsApiRequest({
    method: 'post',
    url: `/api/sms/scripts/${encodeURIComponent(name)}/review`,
    data: { decision, note },
  });
  return data.script;
}

async function promoteSmsScriptLive(name) {
  const data = await scriptsApiRequest({
    method: 'post',
    url: `/api/sms/scripts/${encodeURIComponent(name)}/promote-live`,
    data: {},
  });
  return data.script;
}

async function listSmsScriptApiVersions(name) {
  const data = await scriptsApiRequest({
    method: 'get',
    url: `/api/sms/scripts/${encodeURIComponent(name)}/versions`,
  });
  return data.versions || [];
}

async function diffSmsScriptApiVersions(name, fromVersion, toVersion) {
  const data = await scriptsApiRequest({
    method: 'get',
    url: `/api/sms/scripts/${encodeURIComponent(name)}/diff`,
    params: {
      from_version: fromVersion,
      to_version: toVersion,
    },
  });
  return data;
}

async function rollbackSmsScriptApiVersion(name, version) {
  const data = await scriptsApiRequest({
    method: 'post',
    url: `/api/sms/scripts/${encodeURIComponent(name)}/rollback`,
    data: { version },
  });
  return data.script;
}

async function simulateSmsScriptApi(name, variables = {}) {
  const data = await scriptsApiRequest({
    method: 'post',
    url: `/api/sms/scripts/${encodeURIComponent(name)}/simulate`,
    data: { variables },
  });
  return data.simulation || {};
}

function getSmsScriptLifecycleState(script = {}) {
  if (script?.is_builtin) return 'builtin';
  const raw = script?.lifecycle?.lifecycle_state || script?.lifecycle_state || 'draft';
  return String(raw || 'draft').trim().toLowerCase();
}

function getSmsScriptLifecycleBadge(script = {}) {
  if (script?.is_builtin) {
    return 'Built-in';
  }
  const state = getSmsScriptLifecycleState(script);
  switch (state) {
    case 'review':
      return 'In Review';
    case 'approved':
      return 'Approved';
    case 'live':
      return 'Live';
    default:
      return 'Draft';
  }
}

function formatSmsScriptSummary(script) {
  const summary = [];
  summary.push(`${script.is_builtin ? '📦' : '📛'} *${escapeMarkdown(script.name)}*`);
  if (script.description) {
    summary.push(`📝 ${escapeMarkdown(script.description)}`);
  }
  summary.push(script.is_builtin ? '🏷️ Type: Built-in (read-only)' : '🏷️ Type: Custom script');
  if (!script.is_builtin) {
    summary.push(`🧾 Lifecycle: ${escapeMarkdown(getSmsScriptLifecycleBadge(script))}`);
    if (script?.lifecycle?.review_note) {
      summary.push(`🗒️ Review note: ${escapeMarkdown(String(script.lifecycle.review_note).slice(0, 180))}`);
    }
  }

  const personaSummary = buildPersonaSummaryFromOverrides(script.metadata?.persona);
  if (personaSummary.length) {
    personaSummary.forEach((line) => summary.push(`• ${escapeMarkdown(line)}`));
  }

  const placeholders = extractScriptVariables(script.content || '');
  if (placeholders.length) {
    summary.push(`🧩 Placeholders: ${placeholders.map(escapeMarkdown).join(', ')}`);
  }

  if (script.content) {
    const snippet = script.content.substring(0, 160);
    summary.push(`💬 Preview: ${escapeMarkdown(snippet)}${script.content.length > 160 ? '…' : ''}`);
  }

  summary.push(
    `📅 Updated: ${escapeMarkdown(new Date(script.updated_at || script.created_at).toLocaleString())}`
  );

  return summary.join('\n');
}

async function createSmsScriptFlow(conversation, ctx) {
  const name = await promptText(
    conversation,
    ctx,
    '🆕 *Script name*\nUse lowercase letters, numbers, dashes, or underscores.',
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
    await ctx.reply('❌ Script creation cancelled.');
    return;
  }

  const description = await promptText(
    conversation,
    ctx,
    '📝 Optional description (or type skip).',
    { allowEmpty: true, allowSkip: true, parse: (value) => value.trim() }
  );
  if (description === null) {
    await ctx.reply('❌ Script creation cancelled.');
    return;
  }

  const content = await promptText(
    conversation,
    ctx,
    '💬 Provide the SMS content. You can include placeholders like {code}.',
    { allowEmpty: false, parse: (value) => value.trim() }
  );
  if (!content) {
    await ctx.reply('❌ Script creation cancelled.');
    return;
  }

  const metadata = {};
  const configurePersona = await confirm(conversation, ctx, 'Add persona guidance for this script?');
  if (configurePersona) {
    const personaResult = await collectPersonaConfig(conversation, ctx, {}, { allowCancel: true });
    if (!personaResult) {
      await ctx.reply('❌ Script creation cancelled.');
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
    await ctx.reply(`✅ SMS script *${escapeMarkdown(script.name)}* created.`, { parse_mode: 'Markdown' });
  } catch (error) {
    console.error('Failed to create SMS script:', error);
    await ctx.reply(formatScriptsApiError(error, 'Failed to create SMS script'));
  }
}

async function editSmsScriptFlow(conversation, ctx, script) {
  if (script.is_builtin) {
    await ctx.reply('ℹ️ Built-in scripts are read-only. Clone the script to modify it.');
    return;
  }

  const updates = { updated_by: ctx.from.id.toString() };

  const description = await promptText(
    conversation,
    ctx,
    '📝 Update description (or type skip).',
    { allowEmpty: true, allowSkip: true, defaultValue: script.description || '', parse: (value) => value.trim() }
  );
  if (description === null) {
    await ctx.reply('❌ Update cancelled.');
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
      '💬 Enter the new SMS content.',
      { allowEmpty: false, defaultValue: script.content, parse: (value) => value.trim() }
    );
    if (!content) {
      await ctx.reply('❌ Update cancelled.');
      return;
    }
    updates.content = content;
  }

  const adjustPersona = await confirm(conversation, ctx, 'Update persona guidance for this script?');
  if (adjustPersona) {
    const personaResult = await collectPersonaConfig(conversation, ctx, {}, { allowCancel: true });
    if (!personaResult) {
      await ctx.reply('❌ Update cancelled.');
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
    await ctx.reply('ℹ️ No changes made.');
    return;
  }

  try {
    const updated = await updateSmsScript(script.name, stripUndefined(updates));
    await ctx.reply(`✅ SMS script *${escapeMarkdown(updated.name)}* updated.`, { parse_mode: 'Markdown' });
  } catch (error) {
    console.error('Failed to update SMS script:', error);
    await ctx.reply(formatScriptsApiError(error, 'Failed to update SMS script'));
  }
}

async function cloneSmsScriptFlow(conversation, ctx, script) {
  const name = await promptText(
    conversation,
    ctx,
    `🆕 Enter a name for the clone of *${escapeMarkdown(script.name)}*.`,
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
    await ctx.reply('❌ Clone cancelled.');
    return;
  }

  const description = await promptText(
    conversation,
    ctx,
    '📝 Optional description for the cloned script (or type skip).',
    { allowEmpty: true, allowSkip: true, defaultValue: script.description || '', parse: (value) => value.trim() }
  );
  if (description === null) {
    await ctx.reply('❌ Clone cancelled.');
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
    await ctx.reply(`✅ Script cloned as *${escapeMarkdown(cloned.name)}*.`, { parse_mode: 'Markdown' });
  } catch (error) {
    console.error('Failed to clone SMS script:', error);
    await ctx.reply(formatScriptsApiError(error, 'Failed to clone SMS script'));
  }
}

async function deleteSmsScriptFlow(conversation, ctx, script) {
  if (script.is_builtin) {
    await ctx.reply('ℹ️ Built-in scripts cannot be deleted.');
    return;
  }

  const confirmed = await confirm(conversation, ctx, `Delete SMS script *${escapeMarkdown(script.name)}*?`);
  if (!confirmed) {
    await ctx.reply('Deletion cancelled.');
    return;
  }

  try {
    await deleteSmsScript(script.name);
    await ctx.reply(`🗑️ Script *${escapeMarkdown(script.name)}* deleted.`, { parse_mode: 'Markdown' });
  } catch (error) {
    console.error('Failed to delete SMS script:', error);
    await ctx.reply(formatScriptsApiError(error, 'Failed to delete SMS script'));
  }
}

async function showSmsScriptVersions(conversation, ctx, script) {
  try {
    const versions = await listSmsScriptApiVersions(script.name);
    if (!versions.length) {
      await ctx.reply('ℹ️ No governance versions found yet.');
      return;
    }
    const lines = versions.map((v) => {
      const reason = v.reason ? ` • ${v.reason}` : '';
      return `v${v.version} • ${new Date(v.created_at).toLocaleString()}${reason}`;
    });
    await ctx.reply(`🗂️ API versions\n${lines.join('\n')}`);
  } catch (error) {
    console.error('SMS version list failed:', error);
    await ctx.reply(formatScriptsApiError(error, 'Failed to list versions'));
  }
}

async function rollbackSmsScriptVersionFlow(conversation, ctx, script) {
  try {
    const versions = await listSmsScriptApiVersions(script.name);
    if (!versions.length) {
      await ctx.reply('ℹ️ No versions available for rollback.');
      return script;
    }
    const options = versions.slice(0, 12).map((v) => ({
      id: String(v.version),
      label: `↩️ Rollback to v${v.version}`
    }));
    options.push({ id: 'back', label: '⬅️ Back' });
    const selection = await askOptionWithButtons(
      conversation,
      ctx,
      'Select a version to rollback to.',
      options,
      { prefix: 'sms-script-rollback', columns: 2 }
    );
    if (!selection || selection.id === 'back') return script;
    const versionNumber = Number(selection.id);
    if (Number.isNaN(versionNumber)) {
      await ctx.reply('❌ Invalid version selected.');
      return script;
    }
    const ok = await confirm(
      conversation,
      ctx,
      `Rollback *${escapeMarkdown(script.name)}* to v${versionNumber}?`
    );
    if (!ok) {
      await ctx.reply('Rollback cancelled.');
      return script;
    }
    const updated = await rollbackSmsScriptApiVersion(script.name, versionNumber);
    await ctx.reply(
      `✅ Rolled back to v${versionNumber} (${escapeMarkdown(updated.name)}).`,
      { parse_mode: 'Markdown' }
    );
    return updated || script;
  } catch (error) {
    console.error('SMS rollback flow failed:', error);
    await ctx.reply(formatScriptsApiError(error, 'Failed to rollback version'));
    return script;
  }
}

async function showSmsScriptVersionDiffFlow(conversation, ctx, script) {
  try {
    const versions = await listSmsScriptApiVersions(script.name);
    if (versions.length < 2) {
      await ctx.reply('ℹ️ At least two versions are required to compare changes.');
      return;
    }
    const options = versions.slice(0, 12).map((v) => ({
      id: String(v.version),
      label: `v${v.version}`
    }));
    const fromSelection = await askOptionWithButtons(
      conversation,
      ctx,
      'Select the *from* version.',
      [...options, { id: 'back', label: '⬅️ Back' }],
      { prefix: 'sms-script-diff-from', columns: 3 }
    );
    if (!fromSelection || fromSelection.id === 'back') return;
    const toSelection = await askOptionWithButtons(
      conversation,
      ctx,
      'Select the *to* version.',
      [...options, { id: 'back', label: '⬅️ Back' }],
      { prefix: 'sms-script-diff-to', columns: 3 }
    );
    if (!toSelection || toSelection.id === 'back') return;
    const fromVersion = Number(fromSelection.id);
    const toVersion = Number(toSelection.id);
    if (!Number.isFinite(fromVersion) || !Number.isFinite(toVersion) || fromVersion === toVersion) {
      await ctx.reply('❌ Select two different versions to compare.');
      return;
    }
    const diff = await diffSmsScriptApiVersions(script.name, fromVersion, toVersion);
    const changes = Array.isArray(diff?.changes) ? diff.changes : [];
    if (!changes.length) {
      await ctx.reply(`ℹ️ No differences between v${fromVersion} and v${toVersion}.`);
      return;
    }
    const lines = changes
      .slice(0, 20)
      .map((entry) => `• *${escapeMarkdown(entry.field)}*: \`${escapeMarkdown(stableStringify(entry.from))}\` → \`${escapeMarkdown(stableStringify(entry.to))}\``);
    await ctx.reply(
      `🧮 Version diff v${fromVersion} → v${toVersion}\n${lines.join('\n')}${changes.length > 20 ? '\n… (truncated)' : ''}`,
      { parse_mode: 'Markdown' }
    );
  } catch (error) {
    console.error('SMS diff flow failed:', error);
    await ctx.reply(formatScriptsApiError(error, 'Failed to diff versions'));
  }
}

async function simulateSmsScriptFlow(conversation, ctx, script) {
  try {
    const placeholders = extractScriptVariables(script.content || '');
    let values = {};
    if (placeholders.length > 0) {
      await ctx.reply('🧪 Simulation mode: provide placeholder values (or type skip).');
      const collected = await collectPlaceholderValues(conversation, ctx, placeholders);
      if (collected === null) {
        await ctx.reply('❌ Simulation cancelled.');
        return;
      }
      values = collected;
    }
    const simulation = await simulateSmsScriptApi(script.name, values);
    const missing = Array.isArray(simulation.missing_variables)
      ? simulation.missing_variables
      : [];
    const renderedContent = String(simulation.rendered_content || '').slice(0, 450);
    await ctx.reply(
      `🧪 Simulation result\n` +
      `Lifecycle: ${escapeMarkdown(getSmsScriptLifecycleBadge(script))}\n` +
      `Missing variables: ${escapeMarkdown(missing.length ? missing.join(', ') : 'none')}\n` +
      `\n💬 Content:\n${escapeMarkdown(renderedContent)}${String(simulation.rendered_content || '').length > 450 ? '…' : ''}`,
      { parse_mode: 'Markdown' }
    );
  } catch (error) {
    console.error('SMS simulation flow failed:', error);
    await ctx.reply(formatScriptsApiError(error, 'Failed to simulate script'));
  }
}

async function previewSmsScript(conversation, ctx, script) {
  const to = await promptText(
    conversation,
    ctx,
    '📱 Enter the destination number (E.164 format, e.g., +1234567890).',
    { allowEmpty: false, parse: (value) => value.trim() }
  );
  if (!to) {
    await ctx.reply('❌ Preview cancelled.');
    return;
  }

  if (!/^\+[1-9]\d{1,14}$/.test(to)) {
    await ctx.reply('❌ Invalid phone number format. Preview cancelled.');
    return;
  }

  const placeholders = extractScriptVariables(script.content || '');
  let variables = {};
  if (placeholders.length > 0) {
    await ctx.reply('🧩 This script includes placeholders. Provide values or type skip to leave unchanged.');
    const values = await collectPlaceholderValues(conversation, ctx, placeholders);
    if (values === null) {
      await ctx.reply('❌ Preview cancelled.');
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
      `✅ Preview SMS sent!\n\n📱 To: ${preview.to}\n🆔 Message SID: \`${preview.message_sid}\`\n💬 Content: ${escapeMarkdown(snippet)}${preview.content.length > 200 ? '…' : ''}`,
      { parse_mode: 'Markdown' }
    );
  } catch (error) {
    console.error('Failed to send SMS preview:', error);
    await ctx.reply(formatScriptsApiError(error, 'Failed to send SMS preview'));
  }
}

async function showSmsScriptDetail(conversation, ctx, script) {
  let summaryMessage = null;
  let viewing = true;
  try {
    while (viewing) {
      const summary = formatSmsScriptSummary(script);
      summaryMessage = await upsertMenuMessage(ctx, summaryMessage, summary, { parse_mode: 'Markdown' });

      const actions = [
        { id: 'preview', label: '📲 Preview' },
        { id: 'simulate', label: '🧪 Simulate' },
        { id: 'clone', label: '🧬 Clone' }
      ];

      if (!script.is_builtin) {
        const lifecycleState = getSmsScriptLifecycleState(script);
        actions.splice(2, 0, { id: 'edit', label: '✏️ Edit' });
        actions.splice(3, 0, { id: 'versions', label: '🗂️ Versions' });
        actions.splice(4, 0, { id: 'diff', label: '🧮 Diff' });
        actions.splice(5, 0, { id: 'rollback', label: '↩️ Rollback' });
        if (lifecycleState === 'draft') {
          actions.push({ id: 'submit_review', label: '📨 Submit Review' });
        } else if (lifecycleState === 'review') {
          actions.push({ id: 'approve', label: '✅ Approve' });
          actions.push({ id: 'reject', label: '↩️ Reject' });
        } else if (lifecycleState === 'approved') {
          actions.push({ id: 'promote_live', label: '🚀 Promote Live' });
        }
        actions.push({ id: 'delete', label: '🗑️ Delete' });
      }

      actions.push({ id: 'back', label: '⬅️ Back' });

      const action = await askOptionWithButtons(
        conversation,
        ctx,
        'Choose an action for this SMS script.',
        actions,
        {
          prefix: 'sms-script-action',
          columns: 2,
          keepMessage: summaryMessage
        }
      );

      if (!action?.id) {
        await ctx.reply(selectionExpiredMessage(), { parse_mode: 'Markdown' });
        continue;
      }

      switch (action.id) {
        case 'preview':
          await previewSmsScript(conversation, ctx, script);
          break;
        case 'simulate':
          await simulateSmsScriptFlow(conversation, ctx, script);
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
        case 'diff':
          await showSmsScriptVersionDiffFlow(conversation, ctx, script);
          break;
        case 'rollback':
          script = await rollbackSmsScriptVersionFlow(conversation, ctx, script);
          break;
        case 'submit_review':
          try {
            script = await submitSmsScriptForReview(script.name);
            await ctx.reply('✅ SMS script submitted for review.');
          } catch (error) {
            await ctx.reply(formatScriptsApiError(error, 'Failed to submit review'));
          }
          break;
        case 'approve': {
          const note = await promptText(
            conversation,
            ctx,
            'Optional approval note (type skip to leave blank).',
            { allowEmpty: true, allowSkip: true, parse: (value) => value.trim() }
          );
          if (note === null) {
            await ctx.reply('Approval cancelled.');
            break;
          }
          try {
            script = await reviewSmsScript(
              script.name,
              'approve',
              note === undefined ? null : note
            );
            await ctx.reply('✅ SMS script approved.');
          } catch (error) {
            await ctx.reply(formatScriptsApiError(error, 'Failed to approve script'));
          }
          break;
        }
        case 'reject': {
          const note = await promptText(
            conversation,
            ctx,
            'Optional rejection note (type skip to use default).',
            { allowEmpty: true, allowSkip: true, parse: (value) => value.trim() }
          );
          if (note === null) {
            await ctx.reply('Rejection cancelled.');
            break;
          }
          try {
            script = await reviewSmsScript(
              script.name,
              'reject',
              note === undefined ? null : note
            );
            await ctx.reply('↩️ SMS script returned to draft.');
          } catch (error) {
            await ctx.reply(formatScriptsApiError(error, 'Failed to reject script'));
          }
          break;
        }
        case 'promote_live': {
          const ok = await confirm(
            conversation,
            ctx,
            `Promote *${escapeMarkdown(script.name)}* to live?`
          );
          if (!ok) {
            await ctx.reply('Promotion cancelled.');
            break;
          }
          try {
            script = await promoteSmsScriptLive(script.name);
            await ctx.reply('🚀 SMS script promoted to live.');
          } catch (error) {
            await ctx.reply(formatScriptsApiError(error, 'Failed to promote script'));
          }
          break;
        }
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
  } finally {
    if (summaryMessage) {
      await dismissMenuMessage(ctx, summaryMessage);
    }
  }
}

async function listSmsScriptsFlow(conversation, ctx) {
  let headerMessage = null;
  try {
    await clearMenuMessages(ctx);
    const scripts = await fetchSmsScripts();
    if (!scripts.length) {
      await ctx.reply('ℹ️ No SMS scripts found. Use the create action to add one.');
      return;
    }

    const customCount = scripts.filter((script) => !script.is_builtin).length;
    const builtinCount = scripts.length - customCount;
    headerMessage = await upsertMenuMessage(
      ctx,
      headerMessage,
      `💬 SMS Scripts\n\n${scripts.length} script${scripts.length === 1 ? '' : 's'} found (${customCount} custom, ${builtinCount} built-in). Use Search to filter quickly.`
    );

    const selection = await askScriptSelectionWithPagination(
      conversation,
      ctx,
      {
        prompt: 'Choose an SMS script to manage.',
        items: scripts,
        prefix: 'sms-script-select',
        pageSize: 8,
        searchLabel: 'script name',
        getItemId: (script) => script.name,
        getItemLabel: (script) => {
          const lifecycleLabel = script.is_builtin
            ? 'Built-in'
            : getSmsScriptLifecycleBadge(script);
          return `${script.is_builtin ? '📦' : '📝'} ${script.name}${script.description ? ` · ${script.description}` : ''} · ${lifecycleLabel}`;
        }
      }
    );

    if (!selection || selection.id === 'back') {
      return;
    }

    try {
      const script = await fetchSmsScriptByName(selection.id, { detailed: true });
      if (!script) {
        await ctx.reply('❌ Script not found.');
        return;
      }

      if (headerMessage) {
        await dismissMenuMessage(ctx, headerMessage);
        headerMessage = null;
      }
      await showSmsScriptDetail(conversation, ctx, script);
    } catch (error) {
      console.error('Failed to load SMS script details:', error);
      await ctx.reply(formatScriptsApiError(error, 'Failed to load script details'));
    }
  } catch (error) {
    console.error('Failed to list SMS scripts:', error);
    await ctx.reply(formatScriptsApiError(error, 'Failed to list SMS scripts'));
  } finally {
    if (headerMessage) {
      await dismissMenuMessage(ctx, headerMessage);
    }
  }
}

async function smsScriptsMenu(conversation, ctx) {
  let open = true;
  while (open) {
    const action = await askOptionWithButtons(
      conversation,
      ctx,
      '💬 *SMS Script Designer*\nChoose an action.',
      [
        { id: 'list', label: '📄 List scripts' },
        { id: 'create', label: '➕ Create script' },
        { id: 'back', label: '⬅️ Back to Script Designer' }
      ],
      { prefix: 'sms-script-main', columns: 1 }
    );

    if (!action?.id) {
      await ctx.reply(selectionExpiredMessage(), { parse_mode: 'Markdown' });
      continue;
    }

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
      await ctx.reply('❌ Access denied. Your account is not authorized for this action.');
      return;
    }

    const adminStatus = await new Promise((resolve) => isAdmin(ctx.from.id, resolve));
    ensureActive();
    if (!adminStatus) {
      await ctx.reply('❌ Access denied. This action is available to administrators only.');
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
        '🧰 *Script Designer*\nChoose which scripts to manage.\n\n💡 List views support inline *Search* + *Previous/Next* navigation.',
        [
          { id: 'call', label: '☎️ Call scripts' },
          { id: 'sms', label: '💬 SMS scripts' },
          { id: 'email', label: '📧 Email templates' },
          { id: 'exit', label: '⬅️ Main Menu' }
        ],
        { prefix: 'script-channel', columns: 1, ensureActive }
      );

      if (!selection?.id) {
        await ctx.reply(selectionExpiredMessage(), { parse_mode: 'Markdown' });
        continue;
      }

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

    await ctx.reply('✅ Script designer closed.', {
      reply_markup: buildMainMenuReplyMarkup(ctx)
    });
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
      return ctx.reply('❌ Access denied. Your account is not authorized for this action.');
    }

    const adminStatus = await new Promise((resolve) => isAdmin(ctx.from.id, resolve));
    if (!adminStatus) {
      return ctx.reply('❌ Access denied. This action is available to administrators only.');
    }

    await ctx.conversation.enter('scripts-conversation');
  });
}

module.exports = {
  scriptsFlow,
  registerScriptsCommand
};
