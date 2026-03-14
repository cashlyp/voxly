const config = require('../config');
const httpClient = require('../utils/httpClient');
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
  fetchVoiceModelCatalog,
  askVoiceModelWithPagination,
} = require('../utils/voiceModels');
const {
  startOperation,
  ensureOperationActive,
  registerAbortController,
  OperationCancelledError,
  ensureFlow,
  safeReset,
  guardAgainstCommandInterrupt
} = require('../utils/sessionState');
const {
  RELATIONSHIP_FLOW_TYPES,
  deriveConversationProfile,
} = require('../../api/functions/Dating');
const {
  getCallScriptFlowTypes: getCallScriptFlowTypesShared,
  getPrimaryFlowType: getPrimaryFlowTypeShared,
  getEffectiveObjectiveTags: getEffectiveObjectiveTagsShared,
  isRelationshipFlowType,
} = require('../../api/functions/relationshipFlowMetadata');
const {
  section,
  escapeMarkdown,
  tipLine,
  buildLine,
  renderMenu,
  sendEphemeral,
  buildBackToMenuReplyMarkup,
  cancelledMessage,
  setupStepMessage
} = require('../utils/ui');

async function notifyCallError(ctx, lines = []) {
  const body = Array.isArray(lines) ? lines : [lines];
  await ctx.reply(section('❌ Call Alert', body), {
    reply_markup: buildBackToMenuReplyMarkup(ctx, {
      backAction: 'CALL',
      backLabel: '⬅️ Back to Call'
    })
  });
}
const { buildCallbackData } = require('../utils/actions');

const scriptsApiBase = config.scriptsApiUrl.replace(/\/+$/, '');
const DEFAULT_FIRST_MESSAGE = 'Hello! This is an automated call. How can I help you today?';
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
  payment_collection: '💳',
  identity_verification: '🔐',
  appointment_confirmation: '📅',
  service_recovery: '🛠️',
  general_outreach: '📣',
  general: '🧩'
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
  dating: '💕',
  celebrity: '⭐',
  fan: '🌟',
  creator: '🎬',
  friendship: '🤝',
  networking: '📇',
  community: '👥',
  marketplace_seller: '🛍️',
  real_estate_agent: '🏡'
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
    acc[flowType] = RELATIONSHIP_FLOW_BADGE_OVERRIDES[flowType] || '💬';
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

function isValidPhoneNumber(number) {
  const e164Regex = /^\+[1-9]\d{1,14}$/;
  return e164Regex.test((number || '').trim());
}

function getCallScriptFlowTypes(script = {}) {
  return getCallScriptFlowTypesShared(script);
}

function getPrimaryCallScriptFlowType(script = {}) {
  return getPrimaryFlowTypeShared(script);
}

function getEffectiveObjectiveTags(script = {}) {
  return getEffectiveObjectiveTagsShared(script);
}

function getCallScriptFlowLabel(script = {}) {
  const flowType = getPrimaryCallScriptFlowType(script);
  return CALL_SCRIPT_FLOW_LABELS[flowType] || CALL_SCRIPT_FLOW_LABELS.general;
}

function getCallScriptFlowBadge(script = {}) {
  const flowType = getPrimaryCallScriptFlowType(script);
  return CALL_SCRIPT_FLOW_BADGES[flowType] || CALL_SCRIPT_FLOW_BADGES.general;
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

async function getCallScriptById(scriptId) {
  const response = await httpClient.get(null, `${scriptsApiBase}/api/call-scripts/${scriptId}`, { timeout: 12000 });
  return response.data;
}

async function getCallScripts() {
  const response = await httpClient.get(null, `${scriptsApiBase}/api/call-scripts`, { timeout: 12000 });
  return response.data;
}

async function getActiveCallProvider() {
  const response = await httpClient.get(null, `${scriptsApiBase}/admin/provider`, {
    timeout: 10000,
    headers: {
      'x-admin-token': config.admin.apiToken
    },
    params: {
      channel: 'call'
    }
  });
  return String(response?.data?.provider || '').trim().toLowerCase() || null;
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
  const data = await getCallScripts();
  return data.scripts || [];
}

async function fetchCallScriptById(id) {
  const data = await getCallScriptById(id);
  return data.script;
}

async function selectCallScript(conversation, ctx, ensureActive) {
  let scripts;
  try {
    scripts = await fetchCallScripts();
    ensureActive();
  } catch (error) {
    await ctx.reply(httpClient.getUserMessage(error, 'Unable to load call scripts.'));
    return { status: 'error' };
  }

  if (!scripts.length) {
    await ctx.reply('ℹ️ No call scripts found. Use /scripts to create one.');
    return { status: 'empty' };
  }

  const options = scripts.map((script) => ({
    id: script.id.toString(),
    label: `${getCallScriptFlowBadge(script)} ${script.name}`
  }));
  options.push({ id: 'back', label: '⬅️ Back' });
  options.push({ id: 'cancel', label: '❌ Cancel' });

  const selection = await askOptionWithButtons(
    conversation,
    ctx,
    '📚 *Call Scripts*\nChoose a script to use for this call.',
    options,
    { prefix: 'call-script', columns: 1 }
  );
  ensureActive();
  if (!selection || !selection.id) {
    await ctx.reply('❌ No script selected.');
    return { status: 'back' };
  }

  if (selection.id === 'back') {
    return { status: 'back' };
  }
  if (selection.id === 'cancel') {
    return { status: 'cancel' };
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
    await ctx.reply(httpClient.getUserMessage(error, 'Unable to load the selected script.'));
    return { status: 'error' };
  }

  if (!script) {
    await ctx.reply('❌ Script not found.');
    return { status: 'error' };
  }

  if (!script.first_message) {
    await ctx.reply('⚠️ This script does not define a first message. Please edit it before using.');
    return { status: 'error' };
  }

  const scriptFlowTypes = getCallScriptFlowTypes(script);
  const scriptObjectiveTags = getEffectiveObjectiveTags(script);
  const relationshipFlow =
    scriptFlowTypes.find((flowType) => RELATIONSHIP_FLOW_TYPE_SET.has(flowType)) || null;
  if (scriptFlowTypes.includes('payment_collection')) {
    let activeProvider = null;
    try {
      activeProvider = await getActiveCallProvider();
      ensureActive();
    } catch (error) {
      console.warn('Failed to fetch active provider for payment flow guard:', error?.message || error);
    }

    if (activeProvider && activeProvider !== 'twilio') {
      const providerGuard = await askOptionWithButtons(
        conversation,
        ctx,
        `⚠️ *Payment flow selected*\nActive provider is *${activeProvider}*.\nPayment capture is most reliable on *twilio*.`,
        [
          { id: 'continue', label: '✅ Continue anyway' },
          { id: 'reselect', label: '↩️ Choose another script' },
          { id: 'cancel', label: '❌ Cancel' }
        ],
        { prefix: 'call-script-provider-guard', columns: 1 }
      );
      ensureActive();
      if (!providerGuard || providerGuard.id === 'cancel') {
        return { status: 'cancel' };
      }
      if (providerGuard.id === 'reselect') {
        return { status: 'back' };
      }
    }
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
    voice_model: script.voice_model || null,
    script: script.name,
    script_id: script.id
  };
  if (relationshipFlow) {
    payloadUpdates.call_profile = relationshipFlow;
    payloadUpdates.conversation_profile = relationshipFlow;
    payloadUpdates.conversation_profile_lock = true;
    payloadUpdates.purpose = relationshipFlow;
  }

  const summary = [
    `Script: ${script.name}`,
    `Flow: ${getCallScriptFlowLabel(script)}`,
    `Objective tags: ${scriptObjectiveTags.length ? scriptObjectiveTags.join(', ') : 'none'}`
  ];
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
    status: 'ok',
    payloadUpdates,
    summary,
    meta: {
      scriptName: script.name,
      scriptDescription: script.description || 'No description provided',
      personaLabel: businessOption?.label || script.business_id || 'Custom',
      scriptVoiceModel: script.voice_model || null,
      scriptFlowLabel: getCallScriptFlowLabel(script),
      scriptObjectiveTags
    }
  };
}

async function promptScriptFallback(conversation, ctx, reason = 'empty') {
  const message = reason === 'empty'
    ? '⚠️ No call scripts found. You can create one with /scripts or build a custom persona now.'
    : 'No script selected. You can build a custom persona or cancel.';
  const selection = await askOptionWithButtons(
    conversation,
    ctx,
    message,
    [
      { id: 'custom', label: '🛠️ Build custom persona' },
      { id: 'cancel', label: '❌ Cancel' }
    ],
    { prefix: 'call-script-fallback', columns: 1 }
  );
  return selection?.id || null;
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
    voice_model: null,
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
    const inferredProfile = deriveConversationProfile({
      purpose: null,
      prompt,
      firstMessage,
      fallback: 'general',
    });

    const customFlowOptions = [
      { id: 'general', label: `🧩 ${CALL_SCRIPT_FLOW_LABELS.general || 'General'}` },
      ...RELATIONSHIP_FLOW_TYPES.map((flowType) => ({
        id: flowType,
        label: CALL_SCRIPT_FLOW_BADGES[flowType]
          ? `${CALL_SCRIPT_FLOW_BADGES[flowType]} ${CALL_SCRIPT_FLOW_LABELS[flowType] || flowType}`
          : CALL_SCRIPT_FLOW_LABELS[flowType] || flowType
      }))
    ];

    const recommendedLabel = CALL_SCRIPT_FLOW_LABELS[inferredProfile] || inferredProfile || 'General';
    const selectedFlow = await askOptionWithButtons(
      conversation,
      ctx,
      `🧭 *Select call flow for this custom prompt*\nRecommended from prompt text: *${recommendedLabel}*.\nThis will be applied explicitly during call setup.`,
      customFlowOptions,
      {
        prefix: 'custom-flow',
        columns: 1
      }
    );
    ensureActive();

    if (!selectedFlow?.id) {
      await ctx.reply('❌ Flow selection is required for custom prompt mode.');
      return null;
    }

    if (isRelationshipFlowType(selectedFlow.id)) {
      payloadUpdates.call_profile = selectedFlow.id;
      payloadUpdates.conversation_profile = selectedFlow.id;
      payloadUpdates.conversation_profile_lock = true;
      payloadUpdates.purpose = selectedFlow.id;
      summary.push(`Flow: ${CALL_SCRIPT_FLOW_LABELS[selectedFlow.id] || selectedFlow.id}`);
    } else {
      payloadUpdates.purpose = 'general';
      summary.push(`Flow: ${CALL_SCRIPT_FLOW_LABELS.general || 'General'}`);
    }
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
      personaLabel: personaOptions?.label || 'Custom',
      scriptVoiceModel: null
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
    await sendEphemeral(ctx, 'Starting call process…');
    const user = await new Promise((resolve) => getUser(ctx.from.id, resolve));
    ensureActive();
    if (!user) {
      await ctx.reply('❌ Access denied. Your account is not authorized for this action.');
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
      await ctx.reply(setupStepMessage('Call setup', [
        'Enter phone number in E.164 format.',
        'Example: +16125151442'
      ]), { parse_mode: 'Markdown' });
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
      await ctx.reply(setupStepMessage('Caller details', [
        'Enter the victim name as it should be spoken on the call.',
        'Type `skip` to leave blank.'
      ]), { parse_mode: 'Markdown' });
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
        { id: 'custom', label: '🛠️ Build custom persona' },
        { id: 'cancel', label: '❌ Cancel' }
      ],
      { prefix: 'call-config', columns: 1 }
    );
    ensureActive();
    if (!configurationMode || configurationMode.id === 'cancel') {
      await ctx.reply(cancelledMessage('Call setup', 'Use /call to start again.'), {
        parse_mode: 'Markdown',
        reply_markup: buildBackToMenuReplyMarkup(ctx, {
          backAction: 'CALL',
          backLabel: '⬅️ Back to Call'
        })
      });
      return;
    }

    let configuration = null;
    if (configurationMode.id === 'script') {
      const selection = await selectCallScript(conversation, ctx, ensureActive);
      if (selection?.status === 'ok') {
        configuration = selection;
      } else if (selection?.status === 'cancel') {
        await ctx.reply(cancelledMessage('Call setup', 'Use /call to start again.'), {
          parse_mode: 'Markdown',
          reply_markup: buildBackToMenuReplyMarkup(ctx, {
            backAction: 'CALL',
            backLabel: '⬅️ Back to Call'
          })
        });
        return;
      } else if (selection?.status === 'empty' || selection?.status === 'back') {
        const fallbackChoice = await promptScriptFallback(
          conversation,
          ctx,
          selection?.status === 'empty' ? 'empty' : 'back'
        );
        ensureActive();
        if (fallbackChoice !== 'custom') {
          await ctx.reply(cancelledMessage('Call setup', 'Use /call to start again.'), {
            parse_mode: 'Markdown',
            reply_markup: buildBackToMenuReplyMarkup(ctx, {
              backAction: 'CALL',
              backLabel: '⬅️ Back to Call'
            })
          });
          return;
        }
      } else if (selection?.status === 'error') {
        await safeReset(ctx, 'call_script_error', {
          message: '⚠️ Unable to load call scripts.',
          menuHint: '📋 Check API credentials or use /call to try again.'
        });
        return;
      }
    }
    flow.touch('mode-selected');

    if (!configuration) {
      configuration = await buildCustomCallConfig(conversation, ctx, ensureActive, businessOptions);
    }

    if (!configuration) {
      await ctx.reply(cancelledMessage('Call setup', 'Use /call to start again.'), {
        parse_mode: 'Markdown',
        reply_markup: buildBackToMenuReplyMarkup(ctx, {
          backAction: 'CALL',
          backLabel: '⬅️ Back to Call'
        })
      });
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
    payload.voice_model = payload.voice_model || null;
    payload.script = payload.script || 'custom';
    payload.technical_level = payload.technical_level || 'auto';

    const scriptName =
      configuration.meta?.scriptName ||
      configuration.payloadUpdates?.script ||
      'Custom';
    const personaLabel =
      configuration.meta?.personaLabel ||
      configuration.payloadUpdates?.persona_label ||
      'Custom';
    const scriptVoiceModel = configuration.meta?.scriptVoiceModel || null;
    const scriptFlowLabel = configuration.meta?.scriptFlowLabel || null;
    const scriptObjectiveTags = Array.isArray(configuration.meta?.scriptObjectiveTags)
      ? configuration.meta.scriptObjectiveTags.filter(Boolean)
      : [];

    const defaultVoice = config.defaultVoiceModel;
    const voiceCatalog = await fetchVoiceModelCatalog(ctx);
    const availableVoiceModels = Array.isArray(voiceCatalog.models) ? voiceCatalog.models : [];
    if (availableVoiceModels.length > 8) {
      await ctx.reply('🔎 Tip: use Search in the voice picker to filter by model id quickly.');
    }

    const voiceSelection = await askVoiceModelWithPagination(
      conversation,
      ctx,
      {
        prompt: '🎙️ *Voice selection*\nChoose which voice to use for this call.',
        models: availableVoiceModels,
        topOptions: [
          { id: 'auto', label: '⚙️ Auto (best for flow)' },
          ...(scriptVoiceModel
            ? [{ id: 'script', label: `🎤 Script voice (${scriptVoiceModel})` }]
            : []),
          ...(defaultVoice && defaultVoice !== scriptVoiceModel
            ? [{ id: 'default', label: `🎧 Default voice (${defaultVoice})` }]
            : []),
        ],
        bottomOptions: [
          { id: 'custom', label: '✍️ Custom voice id' },
          { id: 'cancel', label: '❌ Cancel' },
        ],
        prefix: 'call-voice',
        pageSize: 8,
        ensureActive,
      }
    );
    ensureActive();
    if (!voiceSelection || voiceSelection.id === 'cancel') {
      await ctx.reply(cancelledMessage('Call setup', 'Use /call to start again.'), {
        parse_mode: 'Markdown',
        reply_markup: buildBackToMenuReplyMarkup(ctx, {
          backAction: 'CALL',
          backLabel: '⬅️ Back to Call'
        })
      });
      return;
    }

    if (voiceSelection?.id === 'auto') {
      payload.voice_model = null;
    } else if (voiceSelection?.id === 'script' && scriptVoiceModel) {
      payload.voice_model = scriptVoiceModel;
    } else if (voiceSelection?.id === 'default') {
      payload.voice_model = defaultVoice;
    } else if (voiceSelection?.id?.startsWith('model:')) {
      payload.voice_model = voiceSelection.id.slice('model:'.length).trim() || null;
    } else if (voiceSelection?.id === 'custom') {
      await ctx.reply(setupStepMessage('Voice override', [
        'Enter the voice model id to use.',
        'Type `skip` for Auto voice selection.'
      ]), { parse_mode: 'Markdown' });
      const voiceMsg = await waitForMessage();
      let customVoice = voiceMsg?.message?.text?.trim();
      if (customVoice && customVoice.toLowerCase() === 'skip') {
        customVoice = null;
      }
      payload.voice_model = customVoice || null;
    }

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
      scriptFlowLabel ? buildLine('🧭', 'Flow', escapeMarkdown(scriptFlowLabel)) : null,
      scriptObjectiveTags.length
        ? buildLine('🏷️', 'Objective tags', escapeMarkdown(scriptObjectiveTags.join(', ')))
        : null,
      buildLine('🎤', 'Voice', escapeMarkdown(payload.voice_model || 'Auto (best for flow)')),
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

    let callBriefKeyboard = null;
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
      callBriefKeyboard = {
        inline_keyboard: [[{ text: 'ℹ️ Details', callback_data: buildCallbackData(ctx, `CALL_DETAILS:${detailsKey}`) }]]
      };
    }

    await renderMenu(ctx, section('🔍 Call Brief', detailLines), callBriefKeyboard, {
      payload: { parse_mode: 'Markdown' }
    });
    await sendEphemeral(ctx, '⏳ Making the call…');

    const payloadForLog = { ...payload };
    if (payloadForLog.prompt) {
      payloadForLog.prompt = `${payloadForLog.prompt.substring(0, 50)}${payloadForLog.prompt.length > 50 ? '...' : ''}`;
    }

    console.log('Sending call request to API');

    const controller = new AbortController();
    const release = registerAbortController(ctx, controller);
    let data;
    try {
      const response = await httpClient.post(ctx, `${config.apiUrl}/outbound-call`, payload, {
        headers: {
          'Content-Type': 'application/json'
        },
        timeout: 30000,
        signal: controller.signal
      });
      data = response?.data;
      ensureActive();
    } finally {
      release();
    }
    if (data?.success && data.call_sid) {
      flow.touch('completed');
    } else {
      await ctx.reply('⚠️ Call was sent but response format unexpected. Check logs.', {
        reply_markup: buildBackToMenuReplyMarkup(ctx, {
          backAction: 'CALL',
          backLabel: '⬅️ Back to Call'
        })
      });
    }
  } catch (error) {
    if (error instanceof OperationCancelledError || error?.name === 'AbortError' || error?.name === 'CanceledError') {
      console.log('Call flow cancelled');
      return;
    }

    console.error('Call error:', {
      message: error.message,
      status: error.response?.status,
      statusText: error.response?.statusText
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
      } else if (status === 401 || status === 403) {
        await notifyCallError(ctx, 'Not authorized. Check the ADMIN token / API secret.');
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
      await notifyCallError(ctx, httpClient.getUserMessage(error, 'API unreachable. Please try again.'));
      handled = true;
    } else {
      await notifyCallError(ctx, httpClient.getUserMessage(error, `Unexpected error: ${escapeMarkdown(error.message)}`));
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
        return ctx.reply('❌ Access denied. Your account is not authorized for this action.');
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
