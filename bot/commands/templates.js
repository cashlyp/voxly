const axios = require('axios');
const config = require('../config');
const { getUser, isAdmin } = require('../db/db');
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
const { extractTemplateVariables } = require('../utils/templates');
const {
  startOperation,
  ensureOperationActive,
  OperationCancelledError,
  getCurrentOpId,
  guardAgainstCommandInterrupt
} = require('../utils/sessionState');
const { section, buildLine, tipLine } = require('../utils/messageStyle');
const { attachHmacAuth } = require('../utils/apiAuth');

const templatesApi = axios.create({
  baseURL: config.templatesApiUrl.replace(/\/+$/, ''),
  timeout: 15000,
  headers: {
    'Content-Type': 'application/json',
    'x-admin-token': config.admin.apiToken
  }
});

attachHmacAuth(templatesApi, {
  secret: config.apiAuth?.hmacSecret,
  allowedOrigins: [new URL(config.templatesApiUrl).origin],
  defaultBaseUrl: config.templatesApiUrl
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
    `Templates API returned non-JSON response (content-type: ${contentType})`
  );
  error.isTemplatesApiError = true;
  error.reason = 'non_json_response';
  error.endpoint = endpoint;
  error.contentType = contentType;
  error.snippet = snippet;
  return error;
}

async function templatesApiRequest(options) {
  const endpoint = `${(options.method || 'GET').toUpperCase()} ${options.url}`;
  try {
    const response = await templatesApi.request(options);
    const contentType = response.headers?.['content-type'] || '';
    if (!contentType.includes('application/json')) {
      throw nonJsonResponseError(endpoint, response);
    }
    if (response.data && response.data.success === false) {
      const apiError = new Error(response.data.error || 'Templates API reported failure');
      apiError.isTemplatesApiError = true;
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
    error.templatesApi = { endpoint };
    throw error;
  }
}

function formatTemplatesApiError(error, action) {
  const baseHelp = `Ensure the templates service is reachable at ${config.templatesApiUrl} or update TEMPLATES_API_URL.`;

  const apiCode = error.response?.data?.code || error.code;
  if (apiCode === 'TEMPLATE_NAME_DUPLICATE') {
    const suggested = error.response?.data?.suggested_name;
    const suggestionLine = suggested ? ` Suggested name: ${suggested}` : '';
    return `⚠️ ${action}: Template name already exists.${suggestionLine}`;
  }

  if (error.isTemplatesApiError && error.reason === 'non_json_response') {
    return `❌ ${action}: Templates API returned unexpected content (type: ${error.contentType}). ${baseHelp}${
      error.snippet ? `\nSnippet: ${error.snippet}` : ''
    }`;
  }

  if (error.isTemplatesApiError && error.reason === 'api_failure') {
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
      return `❌ ${action}: Templates API responded with HTTP ${status} ${statusText}. ${baseHelp}${
        snippet ? `\nSnippet: ${snippet}` : ''
      }`;
    }

    return `❌ ${action}: ${details || `HTTP ${status}`}`;
  }

  if (error.request) {
    return `❌ ${action}: No response from Templates API. ${baseHelp}`;
  }

  return `❌ ${action}: ${error.message}`;
}

const CANCEL_KEYWORDS = new Set(['cancel', 'exit', 'quit']);

function isCancelInput(text) {
  return typeof text === 'string' && CANCEL_KEYWORDS.has(text.trim().toLowerCase());
}

function escapeMarkdown(text = '') {
  return text.replace(/([_*[\]`])/g, '\\$1');
}

function replacePlaceholders(text = '', values = {}) {
  let output = text;
  for (const [token, value] of Object.entries(values)) {
    const pattern = new RegExp(`{${token}}`, 'g');
    output = output.replace(pattern, value);
  }
  return output;
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

function buildPersonaSummaryFromConfig(template) {
  const summary = [];
  if (template.business_id) {
    const business = findBusinessOption(template.business_id);
    summary.push(`Persona: ${business ? business.label : template.business_id}`);
  }
  const persona = template.persona_config || {};
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
    `🎭 *Select persona for this template:*
Choose the primary business context.`,
    selectionOptions,
    {
      prefix: 'template-business',
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
        ? `🎯 *Choose template purpose:*
This helps align tone and follow-up actions.
_Current: ${currentPurposeLabel}_`
        : `🎯 *Choose template purpose:*
This helps align tone and follow-up actions.`;

      const purposeSelection = await askOptionWithButtons(
        conversation,
        ctx,
        purposePrompt,
        availablePurposes,
        {
          prefix: 'template-purpose',
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
      ? `🎙️ *Preferred tone for this template:*
_Current: ${getOptionLabel(MOOD_OPTIONS, personaConfig.emotion)}_`
      : `🎙️ *Preferred tone for this template:*`;

    const moodSelection = await askOptionWithButtons(
      conversation,
      ctx,
      tonePrompt,
      MOOD_OPTIONS,
      { prefix: 'template-tone', columns: 2, ensureActive: safeEnsureActive }
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
      { prefix: 'template-urgency', columns: 2, ensureActive: safeEnsureActive }
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
      { prefix: 'template-tech', columns: 2, ensureActive: safeEnsureActive }
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
    '🧠 Provide the system prompt for this call template. This sets the AI behavior.',
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

  const voicePrompt = defaults.voice_model ? defaults.voice_model : 'default';
  const voiceModel = await promptText(
    conversation,
    ctx,
    '🎤 Enter the Deepgram voice model for this template (or type skip to use the default).',
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

async function fetchCallTemplates() {
  const data = await templatesApiRequest({ method: 'get', url: '/api/call-templates' });
  return data.templates || [];
}

async function fetchCallTemplateById(id) {
  const data = await templatesApiRequest({ method: 'get', url: `/api/call-templates/${id}` });
  return data.template;
}

async function createCallTemplate(payload) {
  const data = await templatesApiRequest({ method: 'post', url: '/api/call-templates', data: payload });
  return data.template;
}

async function updateCallTemplate(id, payload) {
  const data = await templatesApiRequest({ method: 'put', url: `/api/call-templates/${id}`, data: payload });
  return data.template;
}

async function deleteCallTemplate(id) {
  await templatesApiRequest({ method: 'delete', url: `/api/call-templates/${id}` });
}

async function cloneCallTemplate(id, payload) {
  const data = await templatesApiRequest({ method: 'post', url: `/api/call-templates/${id}/clone`, data: payload });
  return data.template;
}

function formatCallTemplateSummary(template) {
  const summary = [];
  summary.push(`📛 *${escapeMarkdown(template.name)}*`);
  if (template.description) {
    summary.push(`📝 ${escapeMarkdown(template.description)}`);
  }
  if (template.business_id) {
    const business = findBusinessOption(template.business_id);
    summary.push(`🏢 Persona: ${escapeMarkdown(business ? business.label : template.business_id)}`);
  }
  const personaSummary = buildPersonaSummaryFromConfig(template);
  if (personaSummary.length) {
    personaSummary.forEach((line) => summary.push(`• ${escapeMarkdown(line)}`));
  }

  if (template.voice_model) {
    summary.push(`🎤 Voice model: ${escapeMarkdown(template.voice_model)}`);
  }

  const placeholders = new Set([
    ...extractTemplateVariables(template.prompt || ''),
    ...extractTemplateVariables(template.first_message || '')
  ]);
  if (placeholders.size > 0) {
    summary.push(`🧩 Placeholders: ${Array.from(placeholders).map(escapeMarkdown).join(', ')}`);
  }

  if (template.prompt) {
    const snippet = template.prompt.substring(0, 160);
    summary.push(`📜 Prompt snippet: ${escapeMarkdown(snippet)}${template.prompt.length > 160 ? '…' : ''}`);
  }
  if (template.first_message) {
    const snippet = template.first_message.substring(0, 160);
    summary.push(`🗨️ First message: ${escapeMarkdown(snippet)}${template.first_message.length > 160 ? '…' : ''}`);
  }
  summary.push(
    `📅 Updated: ${escapeMarkdown(new Date(template.updated_at || template.created_at).toLocaleString())}`
  );
  return summary.join('\n');
}

async function previewCallTemplate(conversation, ctx, template, ensureActive) {
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
  extractTemplateVariables(template.prompt || '').forEach((token) => placeholderSet.add(token));
  extractTemplateVariables(template.first_message || '').forEach((token) => placeholderSet.add(token));

  let prompt = template.prompt;
  let firstMessage = template.first_message;

  if (placeholderSet.size > 0) {
    await ctx.reply('🧩 This template has placeholders. Provide values where needed (type skip to leave unchanged).');
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

  if (template.business_id) {
    payload.business_id = template.business_id;
  }
  const persona = template.persona_config || {};
  if (prompt) {
    payload.prompt = prompt;
  }
  if (firstMessage) {
    payload.first_message = firstMessage;
  }
  if (template.voice_model) {
    payload.voice_model = template.voice_model;
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
    await axios.post(`${config.apiUrl}/outbound-call`, payload, {
      headers: { 'Content-Type': 'application/json' },
      timeout: 30000
    });

    await ctx.reply('✅ Preview call launched! You should receive a call shortly.');
  } catch (error) {
    console.error('Failed to launch preview call:', error?.response?.data || error.message);
    await ctx.reply(`❌ Preview failed: ${error?.response?.data?.error || error.message}`);
  }
}

async function createCallTemplateFlow(conversation, ctx, ensureActive) {
  const safeEnsureActive = typeof ensureActive === 'function'
    ? ensureActive
    : () => ensureOperationActive(ctx, getCurrentOpId(ctx));
  const name = await promptText(
    conversation,
    ctx,
    '🆕 *Template name*\nEnter a unique name for this call template.',
    {
      allowEmpty: false,
      parse: (value) => value.trim(),
      ensureActive: safeEnsureActive
    }
  );

  if (!name) {
    await ctx.reply('❌ Template creation cancelled.');
    return;
  }

  const description = await promptText(
    conversation,
    ctx,
    '📝 Provide an optional description for this template (or type skip).',
    {
      allowEmpty: true,
      allowSkip: true,
      parse: (value) => value.trim(),
      ensureActive: safeEnsureActive
    }
  );
  if (description === null) {
    await ctx.reply('❌ Template creation cancelled.');
    return;
  }

  const personaResult = await collectPersonaConfig(conversation, ctx, {}, { allowCancel: true, ensureActive: safeEnsureActive });
  if (!personaResult) {
    await ctx.reply('❌ Template creation cancelled.');
    return;
  }

  const promptAndVoice = await collectPromptAndVoice(conversation, ctx, {}, safeEnsureActive);
  if (!promptAndVoice) {
    await ctx.reply('❌ Template creation cancelled.');
    return;
  }

  const templatePayload = {
    name,
    description: description === undefined ? null : (description.length ? description : null),
    business_id: personaResult.business_id,
    persona_config: personaResult.persona_config,
    prompt: promptAndVoice.prompt,
    first_message: promptAndVoice.first_message,
    voice_model: promptAndVoice.voice_model || null
  };

  try {
    const template = await createCallTemplate(templatePayload);
    await ctx.reply(`✅ Template *${escapeMarkdown(template.name)}* created successfully!`, { parse_mode: 'Markdown' });
  } catch (error) {
    console.error('Failed to create template:', error);
    await ctx.reply(formatTemplatesApiError(error, 'Failed to create template'));
  }
}

async function editCallTemplateFlow(conversation, ctx, template, ensureActive) {
  const safeEnsureActive = typeof ensureActive === 'function'
    ? ensureActive
    : () => ensureOperationActive(ctx, getCurrentOpId(ctx));
  const updates = {};

  const name = await promptText(
    conversation,
    ctx,
    '✏️ Update template name (or type skip to keep current).',
    {
      allowEmpty: false,
      allowSkip: true,
      defaultValue: template.name,
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
      await ctx.reply('❌ Template name cannot be empty.');
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
      defaultValue: template.description || '',
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
    const personaResult = await collectPersonaConfig(conversation, ctx, template, { allowCancel: true, ensureActive: safeEnsureActive });
    if (!personaResult) {
      await ctx.reply('❌ Update cancelled.');
      return;
    }
    updates.business_id = personaResult.business_id;
    updates.persona_config = personaResult.persona_config;
  }

  const adjustPrompt = await confirm(conversation, ctx, 'Update prompt, first message, or voice settings?', safeEnsureActive);
  if (adjustPrompt) {
    const promptAndVoice = await collectPromptAndVoice(conversation, ctx, template, safeEnsureActive);
    if (!promptAndVoice) {
      await ctx.reply('❌ Update cancelled.');
      return;
    }
    updates.prompt = promptAndVoice.prompt;
    updates.first_message = promptAndVoice.first_message;
    updates.voice_model = promptAndVoice.voice_model || null;
  }

  if (Object.keys(updates).length === 0) {
    await ctx.reply('ℹ️ No changes made.');
    return;
  }

  try {
    const updated = await updateCallTemplate(template.id, updates);
    await ctx.reply(`✅ Template *${escapeMarkdown(updated.name)}* updated.`, { parse_mode: 'Markdown' });
  } catch (error) {
    console.error('Failed to update template:', error);
    await ctx.reply(formatTemplatesApiError(error, 'Failed to update template'));
  }
}

async function cloneCallTemplateFlow(conversation, ctx, template, ensureActive) {
  const safeEnsureActive = typeof ensureActive === 'function'
    ? ensureActive
    : () => ensureOperationActive(ctx, getCurrentOpId(ctx));
  const name = await promptText(
    conversation,
    ctx,
    `🆕 Enter a name for the clone of *${escapeMarkdown(template.name)}*.`,
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
    '📝 Optionally provide a description for the new template (or type skip).',
    {
      allowEmpty: true,
      allowSkip: true,
      defaultValue: template.description || '',
      parse: (value) => value.trim(),
      ensureActive: safeEnsureActive
    }
  );
  if (description === null) {
    await ctx.reply('❌ Clone cancelled.');
    return;
  }

  try {
    const cloned = await cloneCallTemplate(template.id, {
      name,
      description: description === undefined ? template.description : (description.length ? description : null)
    });
    await ctx.reply(`✅ Template cloned as *${escapeMarkdown(cloned.name)}*.`, { parse_mode: 'Markdown' });
  } catch (error) {
    console.error('Failed to clone template:', error);
    await ctx.reply(formatTemplatesApiError(error, 'Failed to clone template'));
  }
}

async function deleteCallTemplateFlow(conversation, ctx, template, ensureActive) {
  const safeEnsureActive = typeof ensureActive === 'function'
    ? ensureActive
    : () => ensureOperationActive(ctx, getCurrentOpId(ctx));
  const confirmed = await confirm(
    conversation,
    ctx,
    `Are you sure you want to delete *${escapeMarkdown(template.name)}*?`,
    safeEnsureActive
  );
  if (!confirmed) {
    await ctx.reply('Deletion cancelled.');
    return;
  }

  try {
    await deleteCallTemplate(template.id);
    await ctx.reply(`🗑️ Template *${escapeMarkdown(template.name)}* deleted.`, { parse_mode: 'Markdown' });
  } catch (error) {
    console.error('Failed to delete template:', error);
    await ctx.reply(formatTemplatesApiError(error, 'Failed to delete template'));
  }
}

async function showCallTemplateDetail(conversation, ctx, template, ensureActive) {
  const safeEnsureActive = typeof ensureActive === 'function'
    ? ensureActive
    : () => ensureOperationActive(ctx, getCurrentOpId(ctx));
  let viewing = true;
  while (viewing) {
    const summary = formatCallTemplateSummary(template);
    await ctx.reply(summary, { parse_mode: 'Markdown' });

    const action = await askOptionWithButtons(
      conversation,
      ctx,
      'Choose an action for this template.',
      [
        { id: 'preview', label: '📞 Preview' },
        { id: 'edit', label: '✏️ Edit' },
        { id: 'clone', label: '🧬 Clone' },
        { id: 'delete', label: '🗑️ Delete' },
        { id: 'back', label: '⬅️ Back' }
      ],
      { prefix: 'call-template-action', columns: 2, ensureActive: safeEnsureActive }
    );

    switch (action.id) {
      case 'preview':
        await previewCallTemplate(conversation, ctx, template, safeEnsureActive);
        break;
      case 'edit':
        await editCallTemplateFlow(conversation, ctx, template, safeEnsureActive);
        try {
          template = await fetchCallTemplateById(template.id);
        } catch (error) {
          console.error('Failed to refresh call template after edit:', error);
          await ctx.reply(formatTemplatesApiError(error, 'Failed to refresh template details'));
          viewing = false;
        }
        break;
      case 'clone':
        await cloneCallTemplateFlow(conversation, ctx, template, safeEnsureActive);
        break;
      case 'delete':
        await deleteCallTemplateFlow(conversation, ctx, template, safeEnsureActive);
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

async function listCallTemplatesFlow(conversation, ctx, ensureActive) {
  const safeEnsureActive = typeof ensureActive === 'function'
    ? ensureActive
    : () => ensureOperationActive(ctx, getCurrentOpId(ctx));
  try {
    const templates = await fetchCallTemplates();
    safeEnsureActive();
    const list = Array.isArray(templates) ? templates : [];
    const validTemplates = list.filter((template) => template && typeof template.id !== 'undefined' && template.id !== null);

    if (!validTemplates.length) {
      if (templates && templates.length && templates.some((t) => !t || typeof t.id === 'undefined')) {
        console.warn('Template list contained invalid entries, ignoring malformed records.');
      }
      await ctx.reply('ℹ️ No call templates found. Use the create action to add one.');
      return;
    }

    const summaryLines = validTemplates.slice(0, 15).map((template, index) => {
      const parts = [`${index + 1}. ${template.name}`];
      if (template.description) {
        parts.push(`– ${template.description}`);
      }
      return parts.join(' ');
    });

    let message = '☎️ Call Templates\n\n';
    message += summaryLines.join('\n');
    if (validTemplates.length > 15) {
      message += `\n… and ${validTemplates.length - 15} more.`;
    }
    message += '\n\nSelect a template below to view details.';

    await ctx.reply(message);

    const options = validTemplates.map((template) => ({
      id: template.id.toString(),
      label: `📄 ${template.name}`
    }));
    options.push({ id: 'back', label: '⬅️ Back' });

    const selection = await askOptionWithButtons(
      conversation,
      ctx,
      'Choose a call template to manage.',
      options,
      { prefix: 'call-template-select', columns: 1, formatLabel: (option) => option.label, ensureActive: safeEnsureActive }
    );

    if (!selection || !selection.id) {
      await ctx.reply('❌ No selection received. Please try again.');
      return;
    }

    if (selection.id === 'back') {
      return;
    }

    const templateId = Number(selection.id);
    if (Number.isNaN(templateId)) {
      await ctx.reply('❌ Invalid template selection.');
      return;
    }

    try {
      const template = await fetchCallTemplateById(templateId);
      safeEnsureActive();
      if (!template) {
        await ctx.reply('❌ Template not found.');
        return;
      }

      await showCallTemplateDetail(conversation, ctx, template, safeEnsureActive);
    } catch (error) {
      console.error('Failed to load call template details:', error);
      await ctx.reply(formatTemplatesApiError(error, 'Failed to load template details'));
    }
  } catch (error) {
    console.error('Failed to list templates:', error);
    await ctx.reply(formatTemplatesApiError(error, 'Failed to list call templates'));
  }
}

async function callTemplatesMenu(conversation, ctx, ensureActive) {
  const safeEnsureActive = typeof ensureActive === 'function'
    ? ensureActive
    : () => ensureOperationActive(ctx, getCurrentOpId(ctx));
  let open = true;
  while (open) {
    const action = await askOptionWithButtons(
      conversation,
      ctx,
      '☎️ *Call Template Designer*\nChoose an action.',
      [
        { id: 'list', label: '📄 List templates' },
        { id: 'create', label: '➕ Create template' },
        { id: 'back', label: '⬅️ Back' }
      ],
      { prefix: 'call-template-main', columns: 1, ensureActive: safeEnsureActive }
    );

    switch (action.id) {
      case 'list':
        await listCallTemplatesFlow(conversation, ctx, safeEnsureActive);
        break;
      case 'create':
        await createCallTemplateFlow(conversation, ctx, safeEnsureActive);
        break;
      case 'back':
        open = false;
        break;
      default:
        break;
    }
  }
}

async function fetchSmsTemplates({ includeContent = false } = {}) {
  const data = await templatesApiRequest({
    method: 'get',
    url: '/api/sms/templates',
    params: {
      include_builtins: true,
      detailed: includeContent
    }
  });

  const custom = (data.templates || []).map((template) => ({
    ...template,
    is_builtin: !!template.is_builtin,
    metadata: template.metadata || {}
  }));

  const builtin = (data.builtin || []).map((template) => ({
    ...template,
    is_builtin: true,
    metadata: template.metadata || {}
  }));

  return [...custom, ...builtin];
}

async function fetchSmsTemplateByName(name, { detailed = true } = {}) {
  const data = await templatesApiRequest({
    method: 'get',
    url: `/api/sms/templates/${encodeURIComponent(name)}`,
    params: { detailed }
  });

  const template = data.template;
  if (template) {
    template.is_builtin = !!template.is_builtin;
    template.metadata = template.metadata || {};
  }
  return template;
}

async function createSmsTemplate(payload) {
  const data = await templatesApiRequest({ method: 'post', url: '/api/sms/templates', data: payload });
  return data.template;
}

async function updateSmsTemplate(name, payload) {
  const data = await templatesApiRequest({ method: 'put', url: `/api/sms/templates/${encodeURIComponent(name)}`, data: payload });
  return data.template;
}

async function deleteSmsTemplate(name) {
  await templatesApiRequest({ method: 'delete', url: `/api/sms/templates/${encodeURIComponent(name)}` });
}

async function requestSmsTemplatePreview(name, payload) {
  const data = await templatesApiRequest({
    method: 'post',
    url: `/api/sms/templates/${encodeURIComponent(name)}/preview`,
    data: payload
  });
  return data.preview;
}

function formatSmsTemplateSummary(template) {
  const summary = [];
  summary.push(`${template.is_builtin ? '📦' : '📛'} *${escapeMarkdown(template.name)}*`);
  if (template.description) {
    summary.push(`📝 ${escapeMarkdown(template.description)}`);
  }
  summary.push(template.is_builtin ? '🏷️ Type: Built-in (read-only)' : '🏷️ Type: Custom template');

  const personaSummary = buildPersonaSummaryFromOverrides(template.metadata?.persona);
  if (personaSummary.length) {
    personaSummary.forEach((line) => summary.push(`• ${escapeMarkdown(line)}`));
  }

  const placeholders = extractTemplateVariables(template.content || '');
  if (placeholders.length) {
    summary.push(`🧩 Placeholders: ${placeholders.map(escapeMarkdown).join(', ')}`);
  }

  if (template.content) {
    const snippet = template.content.substring(0, 160);
    summary.push(`💬 Preview: ${escapeMarkdown(snippet)}${template.content.length > 160 ? '…' : ''}`);
  }

  summary.push(
    `📅 Updated: ${escapeMarkdown(new Date(template.updated_at || template.created_at).toLocaleString())}`
  );

  return summary.join('\n');
}

async function createSmsTemplateFlow(conversation, ctx) {
  const name = await promptText(
    conversation,
    ctx,
    '🆕 *Template name*\nUse lowercase letters, numbers, dashes, or underscores.',
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
    await ctx.reply('❌ Template creation cancelled.');
    return;
  }

  const description = await promptText(
    conversation,
    ctx,
    '📝 Optional description (or type skip).',
    { allowEmpty: true, allowSkip: true, parse: (value) => value.trim() }
  );
  if (description === null) {
    await ctx.reply('❌ Template creation cancelled.');
    return;
  }

  const content = await promptText(
    conversation,
    ctx,
    '💬 Provide the SMS content. You can include placeholders like {code}.',
    { allowEmpty: false, parse: (value) => value.trim() }
  );
  if (!content) {
    await ctx.reply('❌ Template creation cancelled.');
    return;
  }

  const metadata = {};
  const configurePersona = await confirm(conversation, ctx, 'Add persona guidance for this template?');
  if (configurePersona) {
    const personaResult = await collectPersonaConfig(conversation, ctx, {}, { allowCancel: true });
    if (!personaResult) {
      await ctx.reply('❌ Template creation cancelled.');
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
    const template = await createSmsTemplate(payload);
    await ctx.reply(`✅ SMS template *${escapeMarkdown(template.name)}* created.`, { parse_mode: 'Markdown' });
  } catch (error) {
    console.error('Failed to create SMS template:', error);
    await ctx.reply(formatTemplatesApiError(error, 'Failed to create SMS template'));
  }
}

async function editSmsTemplateFlow(conversation, ctx, template) {
  if (template.is_builtin) {
    await ctx.reply('ℹ️ Built-in templates are read-only. Clone the template to modify it.');
    return;
  }

  const updates = { updated_by: ctx.from.id.toString() };

  const description = await promptText(
    conversation,
    ctx,
    '📝 Update description (or type skip).',
    { allowEmpty: true, allowSkip: true, defaultValue: template.description || '', parse: (value) => value.trim() }
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
      { allowEmpty: false, defaultValue: template.content, parse: (value) => value.trim() }
    );
    if (!content) {
      await ctx.reply('❌ Update cancelled.');
      return;
    }
    updates.content = content;
  }

  const adjustPersona = await confirm(conversation, ctx, 'Update persona guidance for this template?');
  if (adjustPersona) {
    const personaResult = await collectPersonaConfig(conversation, ctx, {}, { allowCancel: true });
    if (!personaResult) {
      await ctx.reply('❌ Update cancelled.');
      return;
    }
    const overrides = toPersonaOverrides(personaResult);
    const metadata = { ...(template.metadata || {}) };
    if (overrides) {
      metadata.persona = overrides;
    } else {
      delete metadata.persona;
    }
    updates.metadata = metadata;
  } else if (template.metadata?.persona) {
    const clearPersona = await confirm(conversation, ctx, 'Remove existing persona guidance?');
    if (clearPersona) {
      const metadata = { ...(template.metadata || {}) };
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
    const updated = await updateSmsTemplate(template.name, updates);
    await ctx.reply(`✅ SMS template *${escapeMarkdown(updated.name)}* updated.`, { parse_mode: 'Markdown' });
  } catch (error) {
    console.error('Failed to update SMS template:', error);
    await ctx.reply(formatTemplatesApiError(error, 'Failed to update SMS template'));
  }
}

async function cloneSmsTemplateFlow(conversation, ctx, template) {
  const name = await promptText(
    conversation,
    ctx,
    `🆕 Enter a name for the clone of *${escapeMarkdown(template.name)}*.`,
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
    '📝 Optional description for the cloned template (or type skip).',
    { allowEmpty: true, allowSkip: true, defaultValue: template.description || '', parse: (value) => value.trim() }
  );
  if (description === null) {
    await ctx.reply('❌ Clone cancelled.');
    return;
  }

  const payload = {
    name,
    description: description === undefined ? template.description : (description.length ? description : null),
    content: template.content,
    metadata: template.metadata,
    created_by: ctx.from.id.toString()
  };

  try {
    const cloned = await createSmsTemplate(payload);
    await ctx.reply(`✅ Template cloned as *${escapeMarkdown(cloned.name)}*.`, { parse_mode: 'Markdown' });
  } catch (error) {
    console.error('Failed to clone SMS template:', error);
    await ctx.reply(formatTemplatesApiError(error, 'Failed to clone SMS template'));
  }
}

async function deleteSmsTemplateFlow(conversation, ctx, template) {
  if (template.is_builtin) {
    await ctx.reply('ℹ️ Built-in templates cannot be deleted.');
    return;
  }

  const confirmed = await confirm(conversation, ctx, `Delete SMS template *${escapeMarkdown(template.name)}*?`);
  if (!confirmed) {
    await ctx.reply('Deletion cancelled.');
    return;
  }

  try {
    await deleteSmsTemplate(template.name);
    await ctx.reply(`🗑️ Template *${escapeMarkdown(template.name)}* deleted.`, { parse_mode: 'Markdown' });
  } catch (error) {
    console.error('Failed to delete SMS template:', error);
    await ctx.reply(formatTemplatesApiError(error, 'Failed to delete SMS template'));
  }
}

async function previewSmsTemplate(conversation, ctx, template) {
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

  const placeholders = extractTemplateVariables(template.content || '');
  let variables = {};
  if (placeholders.length > 0) {
    await ctx.reply('🧩 This template includes placeholders. Provide values or type skip to leave unchanged.');
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
    persona_overrides: template.metadata?.persona
  };

  if (!Object.keys(variables).length) {
    payload.variables = {};
  }

  if (!payload.persona_overrides) {
    delete payload.persona_overrides;
  }

  try {
    const preview = await requestSmsTemplatePreview(template.name, payload);
    const snippet = preview.content.substring(0, 200);
    await ctx.reply(
      `✅ Preview SMS sent!\n\n📱 To: ${preview.to}\n🆔 Message SID: \`${preview.message_sid}\`\n💬 Content: ${escapeMarkdown(snippet)}${preview.content.length > 200 ? '…' : ''}`,
      { parse_mode: 'Markdown' }
    );
  } catch (error) {
    console.error('Failed to send SMS preview:', error);
    await ctx.reply(formatTemplatesApiError(error, 'Failed to send SMS preview'));
  }
}

async function showSmsTemplateDetail(conversation, ctx, template) {
  let viewing = true;
  while (viewing) {
    const summary = formatSmsTemplateSummary(template);
    await ctx.reply(summary, { parse_mode: 'Markdown' });

    const actions = [
      { id: 'preview', label: '📲 Preview' },
      { id: 'clone', label: '🧬 Clone' }
    ];

    if (!template.is_builtin) {
      actions.splice(1, 0, { id: 'edit', label: '✏️ Edit' });
      actions.push({ id: 'delete', label: '🗑️ Delete' });
    }

    actions.push({ id: 'back', label: '⬅️ Back' });

    const action = await askOptionWithButtons(
      conversation,
      ctx,
      'Choose an action for this SMS template.',
      actions,
      { prefix: 'sms-template-action', columns: 2 }
    );

    switch (action.id) {
      case 'preview':
        await previewSmsTemplate(conversation, ctx, template);
        break;
      case 'edit':
        await editSmsTemplateFlow(conversation, ctx, template);
        try {
          template = await fetchSmsTemplateByName(template.name, { detailed: true });
        } catch (error) {
          console.error('Failed to refresh SMS template after edit:', error);
          await ctx.reply(formatTemplatesApiError(error, 'Failed to refresh template details'));
          viewing = false;
        }
        break;
      case 'clone':
        await cloneSmsTemplateFlow(conversation, ctx, template);
        break;
      case 'delete':
        await deleteSmsTemplateFlow(conversation, ctx, template);
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

async function listSmsTemplatesFlow(conversation, ctx) {
  try {
    const templates = await fetchSmsTemplates();
    if (!templates.length) {
      await ctx.reply('ℹ️ No SMS templates found. Use the create action to add one.');
      return;
    }

    const custom = templates.filter((template) => !template.is_builtin);
    const builtin = templates.filter((template) => template.is_builtin);

    let message = '💬 SMS Templates\n\n';
    if (custom.length) {
      message += 'Custom templates:\n';
      message += custom
        .slice(0, 15)
        .map((template) => `• ${template.name}${template.description ? ` – ${template.description}` : ''}`)
        .join('\n');
      message += '\n\n';
    } else {
      message += 'No custom templates yet.\n\n';
    }

    if (builtin.length) {
      message += 'Built-in templates:\n';
      message += builtin
        .map((template) => `• ${template.name}${template.description ? ` – ${template.description}` : ''}`)
        .join('\n');
      message += '\n\n';
    }

    message += 'Select a template below to view details.';
    await ctx.reply(message);

    const options = templates.map((template) => ({
      id: template.name,
      label: `${template.is_builtin ? '📦' : '📝'} ${template.name}`,
      is_builtin: template.is_builtin
    }));
    options.push({ id: 'back', label: '⬅️ Back' });

    const selection = await askOptionWithButtons(
      conversation,
      ctx,
      'Choose an SMS template to manage.',
      options,
      { prefix: 'sms-template-select', columns: 1, formatLabel: (option) => option.label }
    );

    if (selection.id === 'back') {
      return;
    }

    try {
      const template = await fetchSmsTemplateByName(selection.id, { detailed: true });
      if (!template) {
        await ctx.reply('❌ Template not found.');
        return;
      }

      await showSmsTemplateDetail(conversation, ctx, template);
    } catch (error) {
      console.error('Failed to load SMS template details:', error);
      await ctx.reply(formatTemplatesApiError(error, 'Failed to load template details'));
    }
  } catch (error) {
    console.error('Failed to list SMS templates:', error);
    await ctx.reply(formatTemplatesApiError(error, 'Failed to list SMS templates'));
  }
}

async function smsTemplatesMenu(conversation, ctx) {
  let open = true;
  while (open) {
    const action = await askOptionWithButtons(
      conversation,
      ctx,
      '💬 *SMS Template Designer*\nChoose an action.',
      [
        { id: 'list', label: '📄 List templates' },
        { id: 'create', label: '➕ Create template' },
        { id: 'back', label: '⬅️ Back' }
      ],
      { prefix: 'sms-template-main', columns: 1 }
    );

    switch (action.id) {
      case 'list':
        await listSmsTemplatesFlow(conversation, ctx);
        break;
      case 'create':
        await createSmsTemplateFlow(conversation, ctx);
        break;
      case 'back':
        open = false;
        break;
      default:
        break;
    }
  }
}

async function templatesFlow(conversation, ctx) {
  const opId = startOperation(ctx, 'templates');
  const ensureActive = () => ensureOperationActive(ctx, opId);

  try {
    const user = await new Promise((resolve) => getUser(ctx.from.id, resolve));
    ensureActive();
    if (!user) {
      await ctx.reply('❌ You are not authorized to use this bot.');
      return;
    }

    const adminStatus = await new Promise((resolve) => isAdmin(ctx.from.id, resolve));
    ensureActive();
    if (!adminStatus) {
      await ctx.reply('❌ This command is for administrators only.');
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
        '🧰 *Template Designer*\nChoose which templates to manage.',
        [
          { id: 'call', label: '☎️ Call templates' },
          { id: 'sms', label: '💬 SMS templates' },
          { id: 'exit', label: '🚪 Exit' }
        ],
        { prefix: 'template-channel', columns: 1, ensureActive }
      );

      switch (selection.id) {
        case 'call':
          await callTemplatesMenu(conversation, ctx, ensureActive);
          break;
        case 'sms':
          await smsTemplatesMenu(conversation, ctx, ensureActive);
          break;
        case 'exit':
          active = false;
          break;
        default:
          break;
      }
    }

    await ctx.reply('✅ Template designer closed.');
  } catch (error) {
    if (error instanceof OperationCancelledError) {
      console.log('Templates flow cancelled:', error.message);
      return;
    }
    throw error;
  } finally {
    if (ctx.session?.currentOp?.id === opId) {
      ctx.session.currentOp = null;
    }
  }
}

function registerTemplatesCommand(bot) {
  bot.command('templates', async (ctx) => {
    const user = await new Promise((resolve) => getUser(ctx.from.id, resolve));
    if (!user) {
      return ctx.reply('❌ You are not authorized to use this bot.');
    }

    const adminStatus = await new Promise((resolve) => isAdmin(ctx.from.id, resolve));
    if (!adminStatus) {
      return ctx.reply('❌ This command is for administrators only.');
    }

    await ctx.conversation.enter('templates-conversation');
  });
}

module.exports = {
  templatesFlow,
  registerTemplatesCommand
};
