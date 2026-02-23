const config = require('../config');
const httpClient = require('../utils/httpClient');
const {
  getUser,
  listScriptLifecycle,
  getScriptLifecycle,
  getScriptVersion
} = require('../db/db');
const {
  findBusinessOption,
  askOptionWithButtons,
  getBusinessOptions,
  MOOD_OPTIONS,
  URGENCY_OPTIONS,
  TECH_LEVEL_OPTIONS,
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
  waitForConversationText
} = require('../utils/sessionState');
function buildMainMenuReplyMarkup(ctx) {
  return {
    inline_keyboard: [[{ text: '⬅️ Main Menu', callback_data: buildCallbackData(ctx, 'MENU') }]]
  };
}

async function notifyCallError(ctx, lines = []) {
  const body = Array.isArray(lines) ? lines : [lines];
  await ctx.reply(section('❌ Call Alert', body), {
    reply_markup: buildMainMenuReplyMarkup(ctx)
  });
}
const { section, escapeMarkdown, tipLine, buildLine, renderMenu } = require('../utils/ui');
const { buildCallbackData } = require('../utils/actions');

const scriptsApiBase = config.scriptsApiUrl.replace(/\/+$/, '');
const DEFAULT_FIRST_MESSAGE = 'Hello! This is an automated call. How can I help you today?';
const SCRIPT_STATUS_ACTIVE = 'active';
const DEFAULT_OBJECTIVE_ID = 'general_outreach';
const OBJECTIVE_OPTIONS = [
  {
    id: 'collect_payment',
    label: 'Collect Payment',
    emoji: '💳',
    summary: 'Collect outstanding balance with payment-capable scripts.',
    defaultPurpose: 'payment',
    recommendedBusinessId: 'finance',
    recommendedEmotion: 'neutral',
    recommendedUrgency: 'high',
    recommendedTechnicalLevel: 'general',
    requiresPayment: true,
    keywords: ['payment', 'invoice', 'billing', 'charge', 'card']
  },
  {
    id: 'verify_identity',
    label: 'Verify Identity',
    emoji: '🛡️',
    summary: 'Guide OTP/security verification flows.',
    defaultPurpose: 'security',
    recommendedBusinessId: 'finance',
    recommendedEmotion: 'urgent',
    recommendedUrgency: 'high',
    recommendedTechnicalLevel: 'general',
    requiresDigitFlow: true,
    keywords: ['verify', 'verification', 'otp', 'security', 'fraud']
  },
  {
    id: 'appointment_confirm',
    label: 'Appointment Confirm',
    emoji: '📅',
    summary: 'Confirm or remind upcoming appointments.',
    defaultPurpose: 'appointment',
    recommendedBusinessId: 'healthcare',
    recommendedEmotion: 'positive',
    recommendedUrgency: 'normal',
    recommendedTechnicalLevel: 'general',
    keywords: ['appointment', 'schedule', 'visit', 'booking', 'reservation']
  },
  {
    id: 'service_recovery',
    label: 'Service Recovery',
    emoji: '🧯',
    summary: 'Handle complaints and win-back conversations.',
    defaultPurpose: 'recovery',
    recommendedBusinessId: 'hospitality',
    recommendedEmotion: 'empathetic',
    recommendedUrgency: 'normal',
    recommendedTechnicalLevel: 'general',
    keywords: ['recovery', 'follow-up', 'support', 'issue', 'complaint']
  },
  {
    id: DEFAULT_OBJECTIVE_ID,
    label: 'General Outreach',
    emoji: '📞',
    summary: 'Use standard script selection with broad defaults.',
    defaultPurpose: config.defaultPurpose || 'general',
    recommendedBusinessId: config.defaultBusinessId || 'general',
    recommendedEmotion: 'neutral',
    recommendedUrgency: 'normal',
    recommendedTechnicalLevel: 'general',
    keywords: []
  }
];

function getObjectiveById(id) {
  return OBJECTIVE_OPTIONS.find((objective) => objective.id === id) || OBJECTIVE_OPTIONS.find((objective) => objective.id === DEFAULT_OBJECTIVE_ID);
}

function getScriptContextText(script) {
  const personaConfig = script?.persona_config && typeof script.persona_config === 'object'
    ? script.persona_config
    : {};
  return [
    script?.name,
    script?.description,
    script?.business_id,
    script?.purpose,
    personaConfig.purpose,
    personaConfig.emotion,
    personaConfig.urgency,
    script?.prompt,
    script?.first_message
  ]
    .filter(Boolean)
    .join(' ')
    .toLowerCase();
}

function normalizeObjectiveTags(value) {
  if (value === undefined || value === null) return [];
  let list = [];
  if (Array.isArray(value)) {
    list = value;
  } else if (typeof value === 'string') {
    const raw = String(value || '').trim();
    if (!raw) return [];
    if (raw.startsWith('[')) {
      try {
        const parsed = JSON.parse(raw);
        if (Array.isArray(parsed)) {
          list = parsed;
        }
      } catch (_) {
        return [];
      }
    } else {
      list = raw.split(',');
    }
  }
  const known = new Set(OBJECTIVE_OPTIONS.map((entry) => entry.id));
  return Array.from(
    new Set(
      list
        .map((entry) => String(entry || '').trim().toLowerCase())
        .filter((entry) => known.has(entry))
    )
  );
}

function normalizeSupportFlag(value) {
  if (value === undefined || value === null || value === '') return null;
  if (value === true || value === 1) return true;
  if (value === false || value === 0) return false;
  const raw = String(value).trim().toLowerCase();
  if (!raw || raw === 'auto' || raw === 'inherit') return null;
  if (['1', 'true', 'yes', 'on'].includes(raw)) return true;
  if (['0', 'false', 'no', 'off'].includes(raw)) return false;
  return null;
}

function hasPaymentCapability(script) {
  const explicit = normalizeSupportFlag(script?.supports_payment);
  if (explicit !== null) {
    return explicit;
  }
  const enabled =
    script?.payment_enabled === true ||
    script?.payment_enabled === 1 ||
    String(script?.payment_enabled || '').toLowerCase() === 'true';
  if (enabled) {
    return true;
  }
  if (script?.payment_policy && typeof script.payment_policy === 'object') {
    return true;
  }
  const text = getScriptContextText(script);
  return /(payment|billing|invoice|charge|card)/.test(text);
}

function hasDigitFlowCapability(script) {
  const explicit = normalizeSupportFlag(script?.supports_digit_capture);
  if (explicit !== null) {
    return explicit;
  }
  const requiresOtp =
    script?.requires_otp === true ||
    script?.requires_otp === 1 ||
    String(script?.requires_otp || '').toLowerCase() === 'true';
  if (requiresOtp) {
    return true;
  }
  if (script?.default_profile && String(script.default_profile).trim()) {
    return true;
  }
  if (Number.isFinite(Number(script?.expected_length)) && Number(script.expected_length) > 0) {
    return true;
  }
  const text = getScriptContextText(script);
  return /(otp|one[ -]?time|verification|verify|code|pin|digits?)/.test(text);
}

function scoreScriptForObjective(script, objective) {
  if (!script || !objective) {
    return 0;
  }
  if (objective.id === DEFAULT_OBJECTIVE_ID) {
    return 0;
  }
  const text = getScriptContextText(script);
  const personaConfig = script.persona_config && typeof script.persona_config === 'object'
    ? script.persona_config
    : {};
  const objectiveTags = normalizeObjectiveTags(script.objective_tags);
  let score = 0;

  if (objective.requiresPayment && hasPaymentCapability(script)) {
    score += 100;
  }
  if (objective.requiresDigitFlow && hasDigitFlowCapability(script)) {
    score += 100;
  }
  if (objective.defaultPurpose) {
    const scriptPurpose = String(personaConfig.purpose || script.purpose || '').toLowerCase();
    if (scriptPurpose && scriptPurpose === String(objective.defaultPurpose).toLowerCase()) {
      score += 40;
    }
  }
  if (objective.recommendedBusinessId) {
    const scriptBusiness = String(script.business_id || '').toLowerCase();
    if (scriptBusiness && scriptBusiness === String(objective.recommendedBusinessId).toLowerCase()) {
      score += 25;
    }
  }
  if (objectiveTags.length) {
    if (objectiveTags.includes(objective.id)) {
      score += 70;
    } else if (objective.id !== DEFAULT_OBJECTIVE_ID) {
      score -= 20;
    }
  }
  (objective.keywords || []).forEach((keyword) => {
    if (text.includes(String(keyword).toLowerCase())) {
      score += 10;
    }
  });

  return score;
}

async function selectCallObjective(conversation, ctx, ensureActive) {
  const options = OBJECTIVE_OPTIONS.map((objective) => ({
    id: objective.id,
    label: `${objective.emoji || '🎯'} ${objective.label}`
  }));
  options.push({ id: 'back', label: '⬅️ Back' });

  const selection = await askOptionWithButtons(
    conversation,
    ctx,
    '🎯 *Call Objective*\nChoose the outcome you want. The bot will optimize script/persona defaults automatically.',
    options,
    { prefix: 'call-objective', columns: 1 }
  );
  ensureActive();

  if (!selection || selection.id === 'back') {
    return { status: 'back' };
  }
  const objective = getObjectiveById(selection.id);
  if (!objective) {
    return { status: 'error' };
  }

  const objectiveSummary = [
    buildLine('🎯', 'Objective', escapeMarkdown(objective.label)),
    buildLine('🧭', 'Plan', escapeMarkdown(objective.summary || 'Guided configuration')),
    objective.requiresPayment ? buildLine('✅', 'Requirement', 'Payment-ready script') : null,
    objective.requiresDigitFlow ? buildLine('✅', 'Requirement', 'Digit-flow script') : null
  ].filter(Boolean);
  await ctx.reply(section('Objective Selected', objectiveSummary), { parse_mode: 'Markdown' });

  return {
    status: 'ok',
    objective
  };
}

function buildObjectivePreflightReport(payload = {}, objective = null, configuration = null) {
  const targetObjective = objective || getObjectiveById(DEFAULT_OBJECTIVE_ID);
  const blockers = [];
  const warnings = [];
  const objectiveTags = normalizeObjectiveTags(
    configuration?.meta?.objectiveTags
      || configuration?.payloadUpdates?.objective_tags
      || payload?.objective_tags
  );
  const supportsPayment = hasPaymentCapability({
    ...configuration?.payloadUpdates,
    ...payload,
    supports_payment: configuration?.meta?.supportsPayment
  });
  const supportsDigitCapture = hasDigitFlowCapability({
    ...configuration?.payloadUpdates,
    ...payload,
    supports_digit_capture: configuration?.meta?.supportsDigitCapture
  });

  if (objectiveTags.length && !objectiveTags.includes(targetObjective.id)) {
    warnings.push(`Script objective tags do not explicitly include ${targetObjective.label}.`);
  }

  if (targetObjective.requiresPayment) {
    if (!payload.script_id) {
      blockers.push('Payment objective requires selecting a saved script.');
    }
    if (!supportsPayment || payload.payment_enabled !== true) {
      blockers.push('Payment objective requires payment-enabled script settings.');
    }
    if (!String(payload.payment_connector || '').trim()) {
      blockers.push('Payment connector is missing.');
    }
    if (!String(payload.payment_amount || '').trim()) {
      warnings.push('Payment amount is not preset; amount must be captured during call.');
    }
  }

  if (targetObjective.requiresDigitFlow) {
    if (!payload.script_id) {
      blockers.push('Verification objective requires selecting a saved script.');
    }
    if (!supportsDigitCapture) {
      blockers.push('Verification objective requires digit capture capability.');
    }
    const expectedLength = Number(payload.expected_length);
    if (
      !payload.default_profile
      && !(Number.isFinite(expectedLength) && expectedLength > 0)
      && payload.requires_otp !== true
    ) {
      warnings.push('No explicit digit profile/length detected; runtime will infer from prompts.');
    }
  }

  if (!String(payload.first_message || '').trim()) {
    blockers.push('First message is missing.');
  }

  const readinessScore = Math.max(
    0,
    Math.min(100, 100 - blockers.length * 20 - warnings.length * 7)
  );

  return {
    readinessScore,
    blockers,
    warnings,
    objectiveLabel: targetObjective.label
  };
}

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

async function getCallScriptById(scriptId) {
  const response = await httpClient.get(null, `${scriptsApiBase}/api/call-scripts/${scriptId}`, { timeout: 12000 });
  return response.data;
}

async function getCallScripts() {
  const response = await httpClient.get(null, `${scriptsApiBase}/api/call-scripts`, { timeout: 12000 });
  return response.data;
}

async function collectPlaceholderValues(conversation, ctx, placeholders, ensureActive) {
  const values = {};
  for (const placeholder of placeholders) {
    await ctx.reply(`✏️ Enter value for *${placeholder}* (type skip to leave unchanged):`, { parse_mode: 'Markdown' });
    const { text } = await waitForConversationText(conversation, ctx, {
      ensureActive,
      invalidMessage: '⚠️ Please type a value or "skip" to continue.'
    });
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

function normalizeScriptStatus(value) {
  const normalized = String(value || '').trim().toLowerCase();
  if (['draft', 'review', 'active', 'archived'].includes(normalized)) {
    return normalized;
  }
  return 'draft';
}

function formatScriptStatusLabel(status) {
  const normalized = normalizeScriptStatus(status);
  if (normalized === 'active') return '🟢 active';
  if (normalized === 'review') return '🟡 review';
  if (normalized === 'archived') return '⚫ archived';
  return '🟠 draft';
}

async function listCallLifecycleMap() {
  try {
    const rows = await listScriptLifecycle('call');
    const map = new Map();
    (rows || []).forEach((row) => {
      if (!row?.script_id) return;
      map.set(String(row.script_id), row);
    });
    return map;
  } catch (error) {
    console.warn('Failed to load script lifecycle state:', error?.message || error);
    return new Map();
  }
}

async function selectCustomPersonaFallback(conversation, ctx, ensureActive, objective) {
  if (objective?.requiresPayment || objective?.requiresDigitFlow) {
    const requirement = objective?.requiresPayment ? 'payment-capable' : 'digit-flow capable';
    await ctx.reply(`❌ ${objective?.label || 'This'} objective requires a ${requirement} script. Custom setup is disabled for this objective.`);
    return { status: 'objective_blocked' };
  }

  let businessOptions = [];
  try {
    businessOptions = await getBusinessOptions();
    ensureActive();
  } catch (error) {
    await ctx.reply(httpClient.getUserMessage(error, 'Unable to load persona options.'));
    return { status: 'error' };
  }

  const recommendedBusinessId = String(objective?.recommendedBusinessId || '').toLowerCase();
  const personaChoices = (Array.isArray(businessOptions) ? businessOptions : [])
    .filter((option) => option && option.id)
    .map((option) => ({
      id: option.id,
      label: option.custom
        ? '✍️ Custom Persona'
        : `${option.emoji || '🎭'} ${option.label}${recommendedBusinessId && String(option.id).toLowerCase() === recommendedBusinessId ? ' ⭐' : ''}`
    }))
    .sort((a, b) => {
      const aRecommended = recommendedBusinessId && String(a.id).toLowerCase() === recommendedBusinessId ? 1 : 0;
      const bRecommended = recommendedBusinessId && String(b.id).toLowerCase() === recommendedBusinessId ? 1 : 0;
      return bRecommended - aRecommended;
    });

  if (!personaChoices.length) {
    personaChoices.push({ id: 'custom', label: '✍️ Custom Persona' });
  }
  personaChoices.push({ id: 'back', label: '⬅️ Back' });

  const personaSelection = await askOptionWithButtons(
    conversation,
    ctx,
    `🎭 *Custom Call Setup*\nObjective: *${escapeMarkdown(objective?.label || 'General Outreach')}*\nChoose a persona profile or use fully custom mode.`,
    personaChoices,
    { prefix: 'call-persona', columns: 1 }
  );
  ensureActive();

  if (!personaSelection || personaSelection.id === 'back') {
    return { status: 'back' };
  }

  const selectedBusiness = findBusinessOption(personaSelection.id) || {
    id: 'custom',
    label: 'Custom Persona',
    custom: true
  };
  const payloadUpdates = {
    channel: 'voice',
    script: 'custom'
  };
  if (!selectedBusiness.custom) {
    payloadUpdates.business_id = selectedBusiness.id;
  } else {
    payloadUpdates.business_id = objective?.recommendedBusinessId || config.defaultBusinessId;
  }

  const summary = ['Script: Custom setup'];
  if (objective?.label) {
    summary.push(`Objective: ${objective.label}`);
  }
  summary.push(`Persona: ${selectedBusiness.label || 'Custom Persona'}`);

  let recommendedEmotion = selectedBusiness.defaultEmotion || objective?.recommendedEmotion || 'neutral';
  let recommendedUrgency = selectedBusiness.defaultUrgency || objective?.recommendedUrgency || 'normal';
  let recommendedTech = selectedBusiness.defaultTechnicalLevel || objective?.recommendedTechnicalLevel || 'general';

  const availablePurposes = Array.isArray(selectedBusiness.purposes) ? selectedBusiness.purposes : [];
  if (!selectedBusiness.custom && availablePurposes.length > 0) {
    let selectedPurpose =
      availablePurposes.find((item) => item.id === objective?.defaultPurpose) ||
      availablePurposes.find((item) => item.id === selectedBusiness.defaultPurpose) ||
      availablePurposes[0];
    if (availablePurposes.length > 1) {
      const selectedPurposeChoice = await askOptionWithButtons(
        conversation,
        ctx,
        `🎯 *Choose call purpose:*\nRecommended: *${escapeMarkdown(selectedPurpose?.label || selectedPurpose?.id || objective?.defaultPurpose || 'General')}*`,
        availablePurposes.map((purpose) => ({
          id: purpose.id,
          label: `${purpose.emoji || '•'} ${purpose.label || purpose.id}`
        })),
        { prefix: 'call-purpose', columns: 1 }
      );
      ensureActive();
      selectedPurpose =
        availablePurposes.find((item) => item.id === selectedPurposeChoice?.id) ||
        selectedPurpose;
    }

    if (selectedPurpose?.id) {
      payloadUpdates.purpose = selectedPurpose.id;
      summary.push(`Purpose: ${selectedPurpose.label || selectedPurpose.id}`);
      recommendedEmotion = selectedPurpose.defaultEmotion || recommendedEmotion;
      recommendedUrgency = selectedPurpose.defaultUrgency || recommendedUrgency;
      recommendedTech = selectedPurpose.defaultTechnicalLevel || recommendedTech;
    }
  } else if (selectedBusiness.custom && objective?.defaultPurpose) {
    payloadUpdates.purpose = objective.defaultPurpose;
    summary.push(`Purpose: ${objective.defaultPurpose}`);
  }

  const toneSelection = await askOptionWithButtons(
    conversation,
    ctx,
    `🎙️ *Tone preference*\nRecommended: *${getOptionLabel(MOOD_OPTIONS, recommendedEmotion)}*.`,
    MOOD_OPTIONS,
    { prefix: 'call-tone', columns: 2 }
  );
  ensureActive();
  if (toneSelection?.id && toneSelection.id !== 'auto') {
    payloadUpdates.emotion = toneSelection.id;
    summary.push(`Tone: ${toneSelection.label}`);
  } else if (toneSelection?.label) {
    if (objective && objective.id !== DEFAULT_OBJECTIVE_ID && recommendedEmotion) {
      payloadUpdates.emotion = recommendedEmotion;
    }
    summary.push(`Tone: ${toneSelection.label} (${getOptionLabel(MOOD_OPTIONS, recommendedEmotion)})`);
  }

  const urgencySelection = await askOptionWithButtons(
    conversation,
    ctx,
    `⏱️ *Urgency level*\nRecommended: *${getOptionLabel(URGENCY_OPTIONS, recommendedUrgency)}*.`,
    URGENCY_OPTIONS,
    { prefix: 'call-urgency', columns: 2 }
  );
  ensureActive();
  if (urgencySelection?.id && urgencySelection.id !== 'auto') {
    payloadUpdates.urgency = urgencySelection.id;
    summary.push(`Urgency: ${urgencySelection.label}`);
  } else if (urgencySelection?.label) {
    if (objective && objective.id !== DEFAULT_OBJECTIVE_ID && recommendedUrgency) {
      payloadUpdates.urgency = recommendedUrgency;
    }
    summary.push(`Urgency: ${urgencySelection.label} (${getOptionLabel(URGENCY_OPTIONS, recommendedUrgency)})`);
  }

  const techSelection = await askOptionWithButtons(
    conversation,
    ctx,
    `🧠 *Technical level*\nRecommended: *${getOptionLabel(TECH_LEVEL_OPTIONS, recommendedTech)}*.`,
    TECH_LEVEL_OPTIONS,
    { prefix: 'call-tech', columns: 2 }
  );
  ensureActive();
  if (techSelection?.id && techSelection.id !== 'auto') {
    payloadUpdates.technical_level = techSelection.id;
    summary.push(`Technical level: ${techSelection.label}`);
  } else if (techSelection?.label) {
    if (objective && objective.id !== DEFAULT_OBJECTIVE_ID && recommendedTech) {
      payloadUpdates.technical_level = recommendedTech;
    }
    summary.push(`Technical level: ${techSelection.label} (${getOptionLabel(TECH_LEVEL_OPTIONS, recommendedTech)})`);
  }

  await ctx.reply('🧠 Enter custom prompt instructions (type skip to keep API defaults).');
  const { text: promptText } = await waitForConversationText(conversation, ctx, {
    ensureActive,
    invalidMessage: '⚠️ Please type prompt text or "skip".'
  });
  if (promptText && promptText.toLowerCase() !== 'skip') {
    payloadUpdates.prompt = promptText;
  }

  await ctx.reply('🗣️ Enter the first message for this call (type skip for default greeting).');
  const { text: firstMessageText } = await waitForConversationText(conversation, ctx, {
    ensureActive,
    invalidMessage: '⚠️ Please type first message text or "skip".'
  });
  if (firstMessageText && firstMessageText.toLowerCase() !== 'skip') {
    payloadUpdates.first_message = firstMessageText;
  }

  return {
    status: 'ok',
    payloadUpdates,
    summary,
    meta: {
      scriptName: 'Custom setup',
      scriptDescription: 'Manual persona and prompt configuration',
      personaLabel: selectedBusiness.label || 'Custom Persona',
      scriptVoiceModel: null,
      objectiveTags: objective?.id ? [objective.id] : [],
      supportsPayment: false,
      supportsDigitCapture: false
    }
  };
}

async function selectCallScript(conversation, ctx, ensureActive, objective) {
  const objectiveTarget = objective || getObjectiveById(DEFAULT_OBJECTIVE_ID);
  let scripts = [];
  let lifecycleMap = new Map();
  try {
    scripts = await fetchCallScripts();
    lifecycleMap = await listCallLifecycleMap();
    ensureActive();
  } catch (error) {
    const fallbackMessage = objectiveTarget.requiresPayment
      ? 'Unable to load call scripts. Payment objective requires script selection, so please retry.'
      : 'Unable to load call scripts. You can still continue with custom setup.';
    await ctx.reply(httpClient.getUserMessage(error, fallbackMessage));
    scripts = [];
    lifecycleMap = new Map();
  }

  const scriptsWithLifecycle = (Array.isArray(scripts) ? scripts : [])
    .filter((script) => script && Number.isFinite(Number(script.id)))
    .map((script) => {
      const lifecycle = lifecycleMap.get(String(script.id)) || null;
      const status = normalizeScriptStatus(lifecycle?.status);
      return {
        ...script,
        _lifecycle: lifecycle,
        _status: status
      };
    });
  const activeScripts = scriptsWithLifecycle.filter((script) => script._status === SCRIPT_STATUS_ACTIVE);
  const selectableScripts = activeScripts.length ? activeScripts : scriptsWithLifecycle;

  const scoredScripts = selectableScripts.map((script) => ({
    ...script,
    _objectiveScore: scoreScriptForObjective(script, objectiveTarget)
  }));

  let objectiveScripts = scoredScripts;
  if (objectiveTarget.id !== DEFAULT_OBJECTIVE_ID) {
    if (objectiveTarget.requiresPayment) {
      objectiveScripts = scoredScripts.filter((script) => hasPaymentCapability(script));
    } else if (objectiveTarget.requiresDigitFlow) {
      objectiveScripts = scoredScripts.filter((script) => hasDigitFlowCapability(script));
    } else {
      const matched = scoredScripts.filter((script) => script._objectiveScore > 0);
      if (matched.length) {
        objectiveScripts = matched;
      }
    }
  }

  if (objectiveTarget.requiresPayment && !objectiveScripts.length) {
    await ctx.reply('❌ No payment-ready scripts were found for this objective. Create/activate one in /scripts and try again.');
    return { status: 'objective_blocked' };
  }
  if (objectiveTarget.requiresDigitFlow && !objectiveScripts.length) {
    await ctx.reply('❌ No digit-flow scripts were found for this objective. Create/activate one in /scripts and try again.');
    return { status: 'objective_blocked' };
  }

  if (!objectiveScripts.length) {
    objectiveScripts = scoredScripts;
  }

  objectiveScripts.sort((a, b) => {
    const scoreDelta = Number(b._objectiveScore || 0) - Number(a._objectiveScore || 0);
    if (scoreDelta !== 0) {
      return scoreDelta;
    }
    return String(a.name || '').localeCompare(String(b.name || ''));
  });

  let selection = null;
  while (true) {
    const options = objectiveScripts.map((script) => ({
      id: script.id.toString(),
      label: `📄 ${script.name}${activeScripts.length ? '' : ` (${formatScriptStatusLabel(script._status)})`}${script._objectiveScore > 0 ? ' ⭐' : ''}`
    }));
    if (!objectiveTarget.requiresPayment && !objectiveTarget.requiresDigitFlow) {
      options.push({ id: 'custom', label: '✍️ Custom persona setup' });
    }
    options.push({ id: 'back', label: '⬅️ Back' });

    selection = await askOptionWithButtons(
      conversation,
      ctx,
      `📚 *Call Setup*\nObjective: *${escapeMarkdown(objectiveTarget.label)}*\nChoose a script${objectiveTarget.requiresPayment ? ' (payment-ready only).' : objectiveTarget.requiresDigitFlow ? ' (digit-flow capable only).' : ' or continue with custom persona setup.'}`,
      options,
      { prefix: 'call-script', columns: 1 }
    );
    ensureActive();

    if (selection.id === 'back') {
      return { status: 'back' };
    }
    if (selection.id !== 'custom') {
      break;
    }

    const customSelection = await selectCustomPersonaFallback(conversation, ctx, ensureActive, objectiveTarget);
    ensureActive();
    if (customSelection?.status === 'back') {
      continue;
    }
    return customSelection;
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

  let lifecycle = lifecycleMap.get(String(script.id)) || null;
  if (!lifecycle) {
    try {
      lifecycle = await getScriptLifecycle('call', String(script.id));
    } catch (_) {
      lifecycle = null;
    }
  }
  const lifecycleStatus = normalizeScriptStatus(lifecycle?.status);

  let runtimeScript = { ...script };
  let runtimeVersion = Number.isFinite(Number(script.version)) && Number(script.version) > 0
    ? Math.max(1, Math.floor(Number(script.version)))
    : 1;
  const pinnedVersion = Number.isFinite(Number(lifecycle?.pinned_version)) && Number(lifecycle.pinned_version) > 0
    ? Math.max(1, Math.floor(Number(lifecycle.pinned_version)))
    : null;
  if (pinnedVersion) {
    try {
      const snapshot = await getScriptVersion(script.id, 'call', pinnedVersion);
      if (snapshot?.payload && typeof snapshot.payload === 'object') {
        runtimeScript = {
          ...script,
          ...snapshot.payload,
          id: script.id,
          name: script.name,
          description: script.description
        };
        runtimeVersion = pinnedVersion;
        await ctx.reply(`📌 Using pinned runtime version v${pinnedVersion}.`);
      } else {
        runtimeVersion = pinnedVersion;
        await ctx.reply(`⚠️ Pinned version v${pinnedVersion} not found locally. Using current API script.`);
      }
    } catch (error) {
      console.warn('Pinned script lookup failed:', error?.message || error);
      runtimeVersion = pinnedVersion;
      await ctx.reply(`⚠️ Failed to load pinned version v${pinnedVersion}. Using current API script.`);
    }
  }

  if (!runtimeScript.first_message) {
    await ctx.reply('⚠️ This script does not define a first message. Please edit it before using.');
    return { status: 'error' };
  }

  const placeholderSet = new Set();
  extractScriptVariables(runtimeScript.prompt || '').forEach((token) => placeholderSet.add(token));
  extractScriptVariables(runtimeScript.first_message || '').forEach((token) => placeholderSet.add(token));

  const placeholderValues = {};
  if (placeholderSet.size > 0) {
    await ctx.reply('🧩 This script contains placeholders. Provide values where applicable (type skip to leave as-is).');
    Object.assign(placeholderValues, await collectPlaceholderValues(conversation, ctx, Array.from(placeholderSet), ensureActive));
  }

  const filledPrompt = runtimeScript.prompt ? replacePlaceholders(runtimeScript.prompt, placeholderValues) : undefined;
  const filledFirstMessage = replacePlaceholders(runtimeScript.first_message, placeholderValues);

  const payloadUpdates = {
    channel: 'voice',
    business_id: runtimeScript.business_id || config.defaultBusinessId,
    prompt: filledPrompt,
    first_message: filledFirstMessage,
    voice_model: runtimeScript.voice_model || config.defaultVoiceModel,
    script: script.name,
    script_id: script.id
  };
  if (Number.isFinite(Number(runtimeVersion)) && Number(runtimeVersion) > 0) {
    payloadUpdates.script_version = Math.max(1, Math.floor(Number(runtimeVersion)));
  }
  const summary = [`Script: ${script.name}`];
  if (objectiveTarget?.label) {
    summary.push(`Objective: ${objectiveTarget.label}`);
  }
  if (payloadUpdates.script_version) {
    summary.push(`Version: v${payloadUpdates.script_version}`);
  }
  summary.push(`Lifecycle: ${formatScriptStatusLabel(lifecycleStatus)}`);
  if (lifecycleStatus !== SCRIPT_STATUS_ACTIVE) {
    summary.push('Status note: using fallback non-active script.');
  }

  const scriptPaymentEnabled =
    runtimeScript.payment_enabled === true ||
    runtimeScript.payment_enabled === 1 ||
    String(runtimeScript.payment_enabled || '').toLowerCase() === 'true';
  const scriptHasPaymentPolicy = runtimeScript.payment_policy && typeof runtimeScript.payment_policy === 'object';
  if (objectiveTarget?.requiresPayment && !scriptPaymentEnabled && !scriptHasPaymentPolicy) {
    await ctx.reply('❌ Selected script is not payment-ready for this objective. Choose another script.');
    return { status: 'objective_blocked' };
  }
  if (objectiveTarget?.requiresDigitFlow && !hasDigitFlowCapability(runtimeScript)) {
    await ctx.reply('❌ Selected script is not digit-flow capable for this objective. Choose another script.');
    return { status: 'objective_blocked' };
  }
  if (scriptPaymentEnabled) {
    payloadUpdates.payment_enabled = true;
    if (runtimeScript.payment_connector) {
      payloadUpdates.payment_connector = String(runtimeScript.payment_connector).trim();
    }
    if (runtimeScript.payment_amount) {
      payloadUpdates.payment_amount = String(runtimeScript.payment_amount).trim();
    }
    if (runtimeScript.payment_currency) {
      payloadUpdates.payment_currency = String(runtimeScript.payment_currency).trim().toUpperCase();
    }
    if (runtimeScript.payment_description) {
      payloadUpdates.payment_description = String(runtimeScript.payment_description).trim().slice(0, 240);
    }
    if (runtimeScript.payment_start_message) {
      payloadUpdates.payment_start_message = String(runtimeScript.payment_start_message).trim().slice(0, 240);
    }
    if (runtimeScript.payment_success_message) {
      payloadUpdates.payment_success_message = String(runtimeScript.payment_success_message).trim().slice(0, 240);
    }
    if (runtimeScript.payment_failure_message) {
      payloadUpdates.payment_failure_message = String(runtimeScript.payment_failure_message).trim().slice(0, 240);
    }
    if (runtimeScript.payment_retry_message) {
      payloadUpdates.payment_retry_message = String(runtimeScript.payment_retry_message).trim().slice(0, 240);
    }
    summary.push(
      `Payment defaults: ${payloadUpdates.payment_currency || 'USD'} ${payloadUpdates.payment_amount || '(amount at runtime)'}`
    );
  }
  if (scriptHasPaymentPolicy) {
    payloadUpdates.payment_policy = runtimeScript.payment_policy;
  }

  if (script.description) {
    summary.push(`Description: ${script.description}`);
  }

  const businessOption = runtimeScript.business_id ? findBusinessOption(runtimeScript.business_id) : null;
  if (businessOption) {
    summary.push(`Persona: ${businessOption.label}`);
  } else if (runtimeScript.business_id) {
    summary.push(`Persona: ${runtimeScript.business_id}`);
  }

  if (!payloadUpdates.purpose && businessOption?.defaultPurpose) {
    payloadUpdates.purpose = businessOption.defaultPurpose;
  }

  const personaConfig = runtimeScript.persona_config || {};
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
    payloadUpdates.purpose = objectiveTarget?.defaultPurpose || config.defaultPurpose;
  }

  return {
    status: 'ok',
    payloadUpdates,
    summary,
    meta: {
      scriptName: script.name,
      scriptDescription: script.description || 'No description provided',
      personaLabel: businessOption?.label || runtimeScript.business_id || 'Custom',
      scriptVoiceModel: runtimeScript.voice_model || null,
      objectiveTags: normalizeObjectiveTags(runtimeScript.objective_tags),
      supportsPayment: hasPaymentCapability(runtimeScript),
      supportsDigitCapture: hasDigitFlowCapability(runtimeScript)
    }
  };
}

async function callFlow(conversation, ctx) {
  const opId = startOperation(ctx, 'call');
  const flow = ensureFlow(ctx, 'call', { step: 'start' });
  const ensureActive = () => ensureOperationActive(ctx, opId);

  const waitForMessage = async () => {
    const { update } = await waitForConversationText(conversation, ctx, {
      ensureActive,
      invalidMessage: '⚠️ Please send a text response to continue call setup.'
    });
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
    const objectiveSelection = await selectCallObjective(conversation, ctx, ensureActive);
    if (objectiveSelection?.status === 'back') {
      return;
    }
    if (objectiveSelection?.status !== 'ok' || !objectiveSelection.objective) {
      await safeReset(ctx, 'call_objective_error', {
        message: '⚠️ Unable to set call objective.',
        menuHint: '📋 Use /call to try again.'
      });
      return;
    }
    let selectedObjective = objectiveSelection.objective;
    flow.touch('objective-selected');

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

    let configuration = null;
    while (!configuration) {
      const selection = await selectCallScript(conversation, ctx, ensureActive, selectedObjective);
      if (selection?.status === 'ok') {
        configuration = selection;
        break;
      }
      if (selection?.status === 'empty') {
        await ctx.reply('⚠️ No call scripts found. Use /scripts to create one before calling.');
        return;
      }
      if (selection?.status === 'objective_blocked') {
        return;
      }
      if (selection?.status === 'back') {
        const retryObjectiveSelection = await selectCallObjective(conversation, ctx, ensureActive);
        if (retryObjectiveSelection?.status === 'back') {
          return;
        }
        if (retryObjectiveSelection?.status !== 'ok' || !retryObjectiveSelection.objective) {
          await safeReset(ctx, 'call_objective_error', {
            message: '⚠️ Unable to set call objective.',
            menuHint: '📋 Use /call to try again.'
          });
          return;
        }
        selectedObjective = retryObjectiveSelection.objective;
        flow.touch('objective-selected');
        continue;
      }
      if (selection?.status === 'error') {
        await safeReset(ctx, 'call_script_error', {
          message: '⚠️ Unable to load call scripts.',
          menuHint: '📋 Check API credentials or use /call to try again.'
        });
        return;
      }
      return;
    }
    flow.touch('mode-selected');

    if (!configuration) {
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
    payload.purpose = payload.purpose || selectedObjective.defaultPurpose || config.defaultPurpose;
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
    const scriptVoiceModel = configuration.meta?.scriptVoiceModel || null;

    const defaultVoice = config.defaultVoiceModel;
    const voiceOptions = [];
    if (scriptVoiceModel && scriptVoiceModel !== defaultVoice) {
      voiceOptions.push({ id: 'script', label: `🎤 Script voice (${scriptVoiceModel})` });
      voiceOptions.push({ id: 'default', label: `🎧 Default voice (${defaultVoice})` });
    } else {
      voiceOptions.push({ id: 'default', label: `🎧 Default voice (${defaultVoice})` });
    }
    voiceOptions.push({ id: 'custom', label: '✍️ Custom voice id' });

    const voiceSelection = await askOptionWithButtons(
      conversation,
      ctx,
      '🎙️ *Voice selection*\nChoose which voice to use for this call.',
      voiceOptions,
      { prefix: 'call-voice', columns: 1 }
    );
    ensureActive();

    if (voiceSelection?.id === 'script' && scriptVoiceModel) {
      payload.voice_model = scriptVoiceModel;
    } else if (voiceSelection?.id === 'default') {
      payload.voice_model = defaultVoice;
    } else if (voiceSelection?.id === 'custom') {
      await ctx.reply('🎙️ Enter the voice model id (type skip to keep current):');
      const voiceMsg = await waitForMessage();
      let customVoice = voiceMsg?.message?.text?.trim();
      if (customVoice && customVoice.toLowerCase() === 'skip') {
        customVoice = null;
      }
      if (customVoice) {
        payload.voice_model = customVoice;
      }
    }

    if (!payload.first_message) {
      payload.first_message = DEFAULT_FIRST_MESSAGE;
    }
    payload.first_message = buildPersonalizedFirstMessage(
      payload.first_message,
      victimName,
      personaLabel
    );

    const objectivePreflight = buildObjectivePreflightReport(
      payload,
      selectedObjective,
      configuration
    );
    if (objectivePreflight.blockers.length) {
      const blockerLines = [
        `🧪 Objective preflight: ${objectivePreflight.readinessScore}/100`,
        `🎯 Objective: ${objectivePreflight.objectiveLabel || (selectedObjective.label || 'General Outreach')}`,
        '❌ Blockers:',
        ...objectivePreflight.blockers.map((item) => `• ${item}`)
      ];
      await ctx.reply(blockerLines.join('\n'));
      return;
    }

    const toneValue = payload.emotion || 'auto';
    const urgencyValue = payload.urgency || 'auto';
    const techValue = payload.technical_level || 'auto';
    const hasAutoFields = [toneValue, urgencyValue, techValue].some((value) => value === 'auto');

    const detailLines = [
      buildLine('🎯', 'Objective', escapeMarkdown(selectedObjective.label || 'General Outreach')),
      buildLine('📋', 'To', number),
      victimName ? buildLine('👤', 'Victim', escapeMarkdown(victimName)) : null,
      buildLine('🧩', 'Script', escapeMarkdown(scriptName)),
      buildLine('🎤', 'Voice', escapeMarkdown(payload.voice_model || defaultVoice)),
      buildLine('🧪', 'Preflight', `${objectivePreflight.readinessScore}/100`),
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
    if (objectivePreflight.warnings.length) {
      detailLines.push(buildLine('⚠️', 'Warnings', `${objectivePreflight.warnings.length} (use Details)`));
    }
    if (payload.payment_enabled) {
      const connectorLabel = escapeMarkdown(payload.payment_connector || 'configured');
      const amountLabel = payload.payment_amount
        ? `${payload.payment_currency || 'USD'} ${payload.payment_amount}`
        : 'amount set during call';
      detailLines.push(buildLine('💳', 'Payment', `Enabled (${connectorLabel})`));
      detailLines.push(buildLine('💵', 'Charge', amountLabel));
      if (payload.payment_description) {
        detailLines.push(buildLine('🧾', 'Payment note', escapeMarkdown(payload.payment_description)));
      }
      if (payload.payment_start_message) {
        detailLines.push(buildLine('🗣️', 'Payment intro', escapeMarkdown(payload.payment_start_message)));
      }
      if (payload.payment_success_message) {
        detailLines.push(buildLine('✅', 'Payment success', escapeMarkdown(payload.payment_success_message)));
      }
      if (payload.payment_failure_message) {
        detailLines.push(buildLine('⚠️', 'Payment failure', escapeMarkdown(payload.payment_failure_message)));
      }
      if (payload.payment_retry_message) {
        detailLines.push(buildLine('🔁', 'Payment retry', escapeMarkdown(payload.payment_retry_message)));
      }
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
        `• Technical level: ${techValue}`,
        ...(objectivePreflight.warnings.length
          ? ['• Preflight warnings:', ...objectivePreflight.warnings.map((item) => `• ${item}`)]
          : [])
      ].join('\n');
      ctx.session.callDetailsKeys.push(detailsKey);
      if (ctx.session.callDetailsKeys.length > 10) {
        const oldestKey = ctx.session.callDetailsKeys.shift();
        if (oldestKey) {
          delete ctx.session.callDetailsCache[oldestKey];
        }
      }
      replyOptions.reply_markup = {
        inline_keyboard: [[{ text: 'ℹ️ Details', callback_data: buildCallbackData(ctx, `CALL_DETAILS:${detailsKey}`) }]]
      };
    }
    await renderMenu(ctx, section('🔍 Call Brief', detailLines), replyOptions.reply_markup, {
      payload: { parse_mode: 'Markdown' }
    });
    await ctx.reply('⏳ Making the call…');

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
        reply_markup: buildMainMenuReplyMarkup(ctx)
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
