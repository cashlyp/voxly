const { InlineKeyboard } = require('grammy');
const axios = require('axios');
const config = require('../config');
const { ensureOperationActive, getCurrentOpId } = require('./sessionState');

const FALLBACK_PERSONAS = [
  {
    id: 'custom',
    label: '✍️ Custom Persona',
    description: 'Manually configure prompt, first message, and tone for ad-hoc calls or SMS.',
    custom: true
  },
  {
    id: 'technical_support',
    label: 'Technical Support',
    emoji: '🛠️',
    description: 'Guides customers through troubleshooting steps and software onboarding.',
    defaultPurpose: 'general',
    defaultEmotion: 'frustrated',
    defaultUrgency: 'normal',
    defaultTechnicalLevel: 'novice',
    purposes: [
      {
        id: 'general',
        label: 'General Troubleshooting',
        emoji: '🛠️',
        defaultEmotion: 'frustrated',
        defaultUrgency: 'normal',
        defaultTechnicalLevel: 'novice'
      },
      {
        id: 'installation',
        label: 'Installation Help',
        emoji: '💿',
        defaultEmotion: 'confused',
        defaultUrgency: 'normal',
        defaultTechnicalLevel: 'general'
      },
      {
        id: 'outage',
        label: 'Service Outage',
        emoji: '🚨',
        defaultEmotion: 'urgent',
        defaultUrgency: 'high',
        defaultTechnicalLevel: 'advanced'
      }
    ]
  },
  {
    id: 'healthcare',
    label: 'Healthcare Services',
    emoji: '🩺',
    description: 'Coordinates patient reminders, follow-ups, and care outreach.',
    defaultPurpose: 'appointment',
    defaultEmotion: 'positive',
    defaultUrgency: 'normal',
    defaultTechnicalLevel: 'general',
    purposes: [
      {
        id: 'appointment',
        label: 'Appointment Reminder',
        emoji: '📅',
        defaultEmotion: 'positive',
        defaultUrgency: 'normal',
        defaultTechnicalLevel: 'general'
      },
      {
        id: 'follow_up',
        label: 'Post-Visit Follow-up',
        emoji: '📋',
        defaultEmotion: 'empathetic',
        defaultUrgency: 'normal',
        defaultTechnicalLevel: 'general'
      },
      {
        id: 'wellness_check',
        label: 'Wellness Check',
        emoji: '💙',
        defaultEmotion: 'empathetic',
        defaultUrgency: 'low',
        defaultTechnicalLevel: 'general'
      }
    ]
  },
  {
    id: 'finance',
    label: 'Financial Services',
    emoji: '💳',
    description: 'Delivers account alerts, security notices, and payment reminders.',
    defaultPurpose: 'security',
    defaultEmotion: 'urgent',
    defaultUrgency: 'high',
    defaultTechnicalLevel: 'advanced',
    purposes: [
      {
        id: 'security',
        label: 'Security Alert',
        emoji: '🔐',
        defaultEmotion: 'urgent',
        defaultUrgency: 'high',
        defaultTechnicalLevel: 'general'
      },
      {
        id: 'payment',
        label: 'Payment Reminder',
        emoji: '🧾',
        defaultEmotion: 'neutral',
        defaultUrgency: 'normal',
        defaultTechnicalLevel: 'general'
      },
      {
        id: 'fraud',
        label: 'Fraud Investigation',
        emoji: '🚔',
        defaultEmotion: 'urgent',
        defaultUrgency: 'critical',
        defaultTechnicalLevel: 'advanced'
      }
    ]
  },
  {
    id: 'hospitality',
    label: 'Hospitality & Guest Services',
    emoji: '🏨',
    description: 'Handles reservations, guest recovery, and VIP outreach with warm tone.',
    defaultPurpose: 'recovery',
    defaultEmotion: 'empathetic',
    defaultUrgency: 'normal',
    defaultTechnicalLevel: 'general',
    purposes: [
      {
        id: 'reservation',
        label: 'Reservation Follow-up',
        emoji: '📞',
        defaultEmotion: 'positive',
        defaultUrgency: 'normal',
        defaultTechnicalLevel: 'general'
      },
      {
        id: 'recovery',
        label: 'Service Recovery',
        emoji: '💡',
        defaultEmotion: 'empathetic',
        defaultUrgency: 'high',
        defaultTechnicalLevel: 'general'
      },
      {
        id: 'vip_outreach',
        label: 'VIP Outreach',
        emoji: '⭐',
        defaultEmotion: 'positive',
        defaultUrgency: 'low',
        defaultTechnicalLevel: 'general'
      }
    ]
  },
  {
    id: 'emergency_response',
    label: 'Emergency Response',
    emoji: '🚑',
    description: 'Coordinates critical incident response and escalation workflows.',
    defaultPurpose: 'incident',
    defaultEmotion: 'urgent',
    defaultUrgency: 'critical',
    defaultTechnicalLevel: 'advanced',
    purposes: [
      {
        id: 'incident',
        label: 'Incident Response',
        emoji: '⚠️',
        defaultEmotion: 'urgent',
        defaultUrgency: 'critical',
        defaultTechnicalLevel: 'advanced'
      },
      {
        id: 'safety_check',
        label: 'Safety Check',
        emoji: '🆘',
        defaultEmotion: 'urgent',
        defaultUrgency: 'high',
        defaultTechnicalLevel: 'general'
      },
      {
        id: 'drill',
        label: 'Emergency Drill',
        emoji: '🛡️',
        defaultEmotion: 'neutral',
        defaultUrgency: 'normal',
        defaultTechnicalLevel: 'general'
      }
    ]
  }
];

function getCachedBusinessOptions() {
  return Array.isArray(personaCache.options) && personaCache.options.length
    ? personaCache.options
    : NORMALIZED_FALLBACK_PERSONAS;
}

function findBusinessOption(id) {
  if (!id) return null;
  return getCachedBusinessOptions().find((option) => option.id === id) || null;
}

function normalizePurpose(purpose) {
  if (!purpose) {
    return null;
  }
  if (typeof purpose === 'string') {
    return {
      id: purpose,
      label: purpose,
      emoji: undefined,
      defaultEmotion: null,
      defaultUrgency: null,
      defaultTechnicalLevel: null
    };
  }
  const id = purpose.id || purpose.slug || purpose.name;
  if (!id) return null;
  return {
    id,
    label: purpose.label || purpose.name || id,
    emoji: purpose.emoji,
    defaultEmotion: purpose.defaultEmotion || purpose.default_emotion || null,
    defaultUrgency: purpose.defaultUrgency || purpose.default_urgency || null,
    defaultTechnicalLevel: purpose.defaultTechnicalLevel || purpose.default_technical_level || null
  };
}

function normalizePersonaProfile(profile) {
  if (!profile) {
    return null;
  }

  const id = profile.slug || profile.id;
  if (!id) {
    return null;
  }

  const purposesRaw = Array.isArray(profile.purposes) ? profile.purposes : [];
  const purposes = purposesRaw.map(normalizePurpose).filter(Boolean);

  const defaultPurpose =
    profile.defaultPurpose ||
    profile.default_purpose ||
    purposes[0]?.id ||
    'general';

  return {
    id,
    label: profile.label || profile.name || id,
    description: profile.description || '',
    purposes,
    defaultPurpose,
    defaultEmotion: profile.defaultEmotion || profile.default_emotion || null,
    defaultUrgency: profile.defaultUrgency || profile.default_urgency || null,
    defaultTechnicalLevel: profile.defaultTechnicalLevel || profile.default_technical_level || null,
    call_template_id: profile.call_template_id || profile.callTemplateId || null,
    sms_template_name: profile.sms_template_name || profile.smsTemplateName || null,
    custom: Boolean(profile.custom || id === 'custom'),
    dynamic: Boolean(profile.slug && profile.slug !== 'custom')
  };
}

const NORMALIZED_FALLBACK_PERSONAS = FALLBACK_PERSONAS.map(normalizePersonaProfile).filter(Boolean);

let personaCache = {
  expiresAt: 0,
  options: NORMALIZED_FALLBACK_PERSONAS
};

async function fetchRemotePersonas() {
  try {
    const response = await axios.get(`${config.apiUrl}/api/personas`, { timeout: 10000 });
    const data = response.data || {};
    const builtin = Array.isArray(data.builtin) ? data.builtin : [];
    const custom = Array.isArray(data.custom) ? data.custom : [];
    const normalized = [...builtin, ...custom].map(normalizePersonaProfile).filter(Boolean);
    if (!normalized.length) {
      return NORMALIZED_FALLBACK_PERSONAS;
    }
    return normalized;
  } catch (error) {
    console.error('Failed to fetch personas:', error.message);
    return NORMALIZED_FALLBACK_PERSONAS;
  }
}

async function getBusinessOptions(forceRefresh = false) {
  const now = Date.now();
  if (!forceRefresh && personaCache.options && personaCache.expiresAt > now) {
    return personaCache.options;
  }

  const options = await fetchRemotePersonas();
  const seen = new Set();
  const merged = [];

  const addOption = (option) => {
    if (!option || !option.id || seen.has(option.id)) return;
    seen.add(option.id);
    merged.push(option);
  };

  options.forEach(addOption);

  // Ensure custom fallback is always present for manual prompts.
  NORMALIZED_FALLBACK_PERSONAS.forEach((option) => {
    if (option.id === 'custom') {
      addOption(option);
    }
  });

  personaCache = {
    options: merged,
    expiresAt: now + 60 * 1000
  };

  return merged;
}

function invalidatePersonaCache() {
  personaCache.expiresAt = 0;
}

const MOOD_OPTIONS = [
  { id: 'auto', label: 'Auto (use recommended)' },
  { id: 'neutral', label: 'Neutral / professional' },
  { id: 'frustrated', label: 'Empathetic troubleshooter' },
  { id: 'urgent', label: 'Urgent / high-priority' },
  { id: 'confused', label: 'Patient explainer' },
  { id: 'positive', label: 'Upbeat / encouraging' },
  { id: 'stressed', label: 'Reassuring & calming' },
];

const URGENCY_OPTIONS = [
  { id: 'auto', label: 'Auto (use recommended)' },
  { id: 'low', label: 'Low – casual follow-up' },
  { id: 'normal', label: 'Normal – timely assistance' },
  { id: 'high', label: 'High – priority handling' },
  { id: 'critical', label: 'Critical – emergency protocol' },
];

const TECH_LEVEL_OPTIONS = [
  { id: 'auto', label: 'Auto (general audience)' },
  { id: 'general', label: 'General audience' },
  { id: 'novice', label: 'Beginner-friendly' },
  { id: 'advanced', label: 'Advanced / technical specialist' },
];

function formatOptionLabel(option) {
  if (option.emoji) {
    return `${option.emoji} ${option.label}`;
  }
  return option.label;
}

async function askOptionWithButtons(
  conversation,
  ctx,
  prompt,
  options,
  { prefix, columns = 2, formatLabel, ensureActive } = {}
) {
  const keyboard = new InlineKeyboard();
  options.forEach((option, index) => {
    const label = formatLabel ? formatLabel(option) : formatOptionLabel(option);
    keyboard.text(label, `${prefix}:${option.id}`);
    if ((index + 1) % columns === 0) {
      keyboard.row();
    }
  });

  const message = await ctx.reply(prompt, { parse_mode: 'Markdown', reply_markup: keyboard });
  const selectionCtx = await conversation.waitFor('callback_query:data', (callbackCtx) => {
    return callbackCtx.callbackQuery.data.startsWith(`${prefix}:`);
  });
  const activeChecker = typeof ensureActive === 'function'
    ? ensureActive
    : () => ensureOperationActive(ctx, getCurrentOpId(ctx));
  activeChecker();

  await selectionCtx.answerCallbackQuery();
  await ctx.api.editMessageReplyMarkup(message.chat.id, message.message_id).catch(() => {});

  const selectedId = selectionCtx.callbackQuery.data.split(':')[1];
  return options.find((option) => option.id === selectedId);
}

function getOptionLabel(options, id) {
  const match = options.find((option) => option.id === id);
  return match ? match.label : id;
}

module.exports = {
  MOOD_OPTIONS,
  URGENCY_OPTIONS,
  TECH_LEVEL_OPTIONS,
  formatOptionLabel,
  askOptionWithButtons,
  getOptionLabel,
  getBusinessOptions,
  invalidatePersonaCache,
  getCachedBusinessOptions,
  findBusinessOption
};
