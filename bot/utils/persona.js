'use strict';

/**
 * persona.js
 *
 * Utility for fetching/caching business persona profiles and providing the
 * askOptionWithButtons helper used throughout every conversation flow.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * BUG FIX — "⚠️ This menu is no longer active." on Script Designer buttons
 * ─────────────────────────────────────────────────────────────────────────────
 *
 * PROBLEM:
 *   Inside the `while (!selected)` callback-waiting loop in `askOptionWithButtons`,
 *   there is a guard:
 *
 *       if (expectedMessageId && callbackMessageId !== expectedMessageId) {
 *         await selectionCtx.answerCallbackQuery({ text: '⚠️ This menu is no longer active.' });
 *         continue;  // ← silently drops the tap and loops forever
 *       }
 *
 *   This guard fires when the message ID of the button the user tapped differs
 *   from the message ID of the menu the conversation most recently rendered.
 *
 *   For non-Script-Designer menus this is correct — a stale button from an old
 *   prompt should be rejected.
 *
 *   For SCRIPT DESIGNER menus (script-channel, call-script-main, sms-script-main,
 *   email-template-main, inbound-default) the guard caused a permanent loop
 *   because after a bot restart or recovery the conversation re-enters,
 *   re-renders the menu (new message_id), but the user's tap references the
 *   freshly-rendered menu — and due to a grammY conversation-replay timing issue,
 *   the `expectedMessageId` captured inside `askOptionWithButtons` sometimes
 *   still pointed to the message from the PREVIOUS session.
 *
 *   Result: every single button tap was rejected with the "no longer active" alert.
 *
 * FIX:
 *   For Script Designer menus (`isScriptDesignerMenu = true`) the message-ID
 *   guard is RELAXED:
 *     • Mismatches are LOGGED as diagnostic JSON (type: conversation_msg_id_mismatch)
 *       so they remain observable in production logs.
 *     • The callback is NOT rejected — execution falls through to the normal
 *       option-matching and tap-lock logic.
 *     • Non-Script-Designer menus are UNCHANGED — the guard remains strict.
 *
 *   This is safe because Script Designer menus use prefix-based matching
 *   (matchesCallbackPrefix) which already ensures only callbacks for the correct
 *   prefix are accepted, and the tap-lock prevents duplicate processing.
 */

const { InlineKeyboard } = require('grammy');
const config = require('../config');
const httpClient = require('./httpClient');
const {
  ensureOperationActive,
  getCurrentOpId,
  OperationCancelledError
} = require('./sessionState');
const { sendMenu, clearMenuMessages } = require('./ui');
const { buildCallbackData, matchesCallbackPrefix, parseCallbackData } = require('./actions');

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
    description: 'Guides victims through troubleshooting steps and software onboarding.',
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
    call_script_id: profile.call_script_id || profile.callScriptId || profile.call_template_id || profile.callTemplateId || null,
    sms_script_name: profile.sms_script_name || profile.smsScriptName || profile.sms_template_name || profile.smsTemplateName || null,
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
    const response = await httpClient.get(null, `${config.apiUrl}/api/personas`, { timeout: 10000 });
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

const CALLBACK_REPLAY_TTL_MS = 2 * 60 * 1000;
const CALLBACK_REPLAY_MAX_ACTIONS = 8;
const MENU_TAP_LOCK_TTL_MS = 8 * 1000;
const MENU_SELECTION_TIMEOUT_MS = 20 * 60 * 1000;
const SCRIPT_MENU_SELECTION_TIMEOUT_MS = 30 * 60 * 1000;
const SCRIPT_MENU_PREFIX_PATTERN =
  /^(call-script-|sms-script-|script-|inbound-default|email-template-)/;
const SCRIPT_MENU_NO_TIMEOUT_PREFIX_PATTERN =
  /^(script-channel|call-script-main|sms-script-main|email-template-main|inbound-default)$/;

function formatOptionLabel(option) {
  if (option.emoji) {
    return `${option.emoji} ${option.label}`;
  }
  return option.label;
}

function readPendingCallbackReplay(ctx) {
  const meta = ctx.session?.meta;
  if (!meta || !meta.pendingCallbackReplay) {
    return [];
  }
  const replayMeta = meta.pendingCallbackReplay;
  let actions = [];
  let createdAt = 0;
  if (Array.isArray(replayMeta)) {
    actions = replayMeta;
  } else if (replayMeta && typeof replayMeta === 'object') {
    actions = Array.isArray(replayMeta.actions) ? replayMeta.actions : [];
    createdAt = Number(replayMeta.createdAt || 0);
  }
  if (!actions.length) {
    delete meta.pendingCallbackReplay;
    return [];
  }
  if (createdAt > 0 && Date.now() - createdAt > CALLBACK_REPLAY_TTL_MS) {
    delete meta.pendingCallbackReplay;
    console.log(JSON.stringify({
      type: 'conversation_callback_replay_expired',
      user_id: ctx?.from?.id || null,
      chat_id: ctx?.chat?.id || null
    }));
    return [];
  }
  return actions
    .map((action) => String(action || '').trim())
    .filter(Boolean)
    .slice(0, CALLBACK_REPLAY_MAX_ACTIONS);
}

function writePendingCallbackReplay(ctx, actions, sourceAction = null) {
  if (!ctx.session) {
    return;
  }
  ctx.session.meta = ctx.session.meta || {};
  const normalized = Array.isArray(actions)
    ? actions
      .map((action) => String(action || '').trim())
      .filter(Boolean)
      .slice(0, CALLBACK_REPLAY_MAX_ACTIONS)
    : [];
  if (normalized.length === 0) {
    delete ctx.session.meta.pendingCallbackReplay;
    return;
  }
  ctx.session.meta.pendingCallbackReplay = {
    actions: normalized,
    createdAt: Date.now(),
    sourceAction: sourceAction || null
  };
}

function readTapLocks(ctx) {
  if (!ctx.session) {
    return {};
  }
  ctx.session.meta = ctx.session.meta || {};
  const locks = ctx.session.meta.menuTapLocks;
  if (!locks || typeof locks !== 'object') {
    ctx.session.meta.menuTapLocks = {};
    return ctx.session.meta.menuTapLocks;
  }
  return locks;
}

function pruneTapLocks(ctx, now = Date.now()) {
  const locks = readTapLocks(ctx);
  Object.entries(locks).forEach(([key, expiresAt]) => {
    if (!Number.isFinite(expiresAt) || expiresAt <= now) {
      delete locks[key];
    }
  });
  return locks;
}

function acquireTapLock(ctx, lockKey) {
  if (!lockKey) {
    return true;
  }
  const now = Date.now();
  const locks = pruneTapLocks(ctx, now);
  const expiresAt = Number(locks[lockKey] || 0);
  if (expiresAt > now) {
    return false;
  }
  locks[lockKey] = now + MENU_TAP_LOCK_TTL_MS;
  return true;
}

function releaseTapLock(ctx, lockKey) {
  if (!ctx?.session?.meta?.menuTapLocks || !lockKey) {
    return;
  }
  delete ctx.session.meta.menuTapLocks[lockKey];
}

async function disableMessageButtons(apiCtx, chatId, messageId) {
  if (!apiCtx || !chatId || !messageId) {
    return;
  }
  try {
    await apiCtx.api.editMessageReplyMarkup(chatId, messageId, {
      reply_markup: { inline_keyboard: [] }
    });
  } catch (_) {
    try {
      await apiCtx.api.editMessageReplyMarkup(chatId, messageId);
    } catch (_) {}
  }
}

async function askOptionWithButtons(
  conversation,
  ctx,
  prompt,
  options,
  settings = {}
) {
  const {
    prefix,
    columns = 2,
    formatLabel,
    ensureActive,
    bindToOperation,
    timeoutMs = MENU_SELECTION_TIMEOUT_MS,
    timeoutMessage = '⏱️ Menu timed out. Please reopen this command.'
  } = settings;
  const keyboard = new InlineKeyboard();
  const opId = getCurrentOpId(ctx);
  const opToken = ctx.session?.currentOp?.token || null;
  const basePrefix = prefix || 'option';
  const isScriptDesignerMenu = SCRIPT_MENU_PREFIX_PATTERN.test(basePrefix);
  const hasExplicitTimeoutMs = Object.prototype.hasOwnProperty.call(settings, 'timeoutMs');
  const shouldDisableTimeout =
    isScriptDesignerMenu &&
    SCRIPT_MENU_NO_TIMEOUT_PREFIX_PATTERN.test(basePrefix) &&
    !hasExplicitTimeoutMs;
  const shouldBindToOperation =
    typeof bindToOperation === 'boolean'
      ? bindToOperation
      : !isScriptDesignerMenu;
  const resolvedTimeoutMs = shouldDisableTimeout
    ? 0
    : (Number.isFinite(timeoutMs) && timeoutMs > 0
      ? (isScriptDesignerMenu ? Math.max(timeoutMs, SCRIPT_MENU_SELECTION_TIMEOUT_MS) : timeoutMs)
      : timeoutMs);
  const resolvedTimeoutMessage = isScriptDesignerMenu
    && timeoutMessage === '⏱️ Menu timed out. Please reopen this command.'
    ? '⏱️ Script Designer step timed out. Reopen /scripts to continue.'
    : timeoutMessage;
  const prefixKey = shouldBindToOperation && opToken
    ? `${basePrefix}:${opToken}`
    : `${basePrefix}`;
  const optionLookupByToken = new Map();
  const labels = options.map((option) => (formatLabel ? formatLabel(option) : formatOptionLabel(option)));
  const hasLongLabel = labels.some((label) => String(label).length > 22);
  const fallbackOpId = opId;
  const activeChecker = typeof ensureActive === 'function'
    ? ensureActive
    : () => ensureOperationActive(ctx, fallbackOpId);
  let resolvedColumns = Number.isFinite(columns) ? columns : (labels.length > 6 || hasLongLabel ? 1 : 2);
  if (resolvedColumns < 1) {
    resolvedColumns = 1;
  }
  if (resolvedColumns > 1 && hasLongLabel) {
    resolvedColumns = 1;
  }

  labels.forEach((label, index) => {
    const option = options[index];
    const optionToken = String(index);
    optionLookupByToken.set(optionToken, option);
    const action = `${prefixKey}:${optionToken}`;
    // Keep conversation menu callback_data restart-safe and worker-safe.
    keyboard.text(label, buildCallbackData(ctx, action, { ttlMs: 0 }));
    if ((index + 1) % resolvedColumns === 0) {
      keyboard.row();
    }
  });

  const pendingReplayQueue = readPendingCallbackReplay(ctx);
  if (pendingReplayQueue.length > 0) {
    let dropped = 0;
    while (pendingReplayQueue.length > 0) {
      const pendingActionRaw = String(pendingReplayQueue[0] || '');
      if (!pendingActionRaw) {
        break;
      }
      if (!matchesCallbackPrefix(pendingActionRaw, prefixKey)) {
        writePendingCallbackReplay(
          ctx,
          [],
          ctx.session?.meta?.pendingCallbackReplay?.sourceAction || null
        );
        console.log(JSON.stringify({
          type: 'conversation_callback_replay_cleared',
          prefix_key: prefixKey,
          reason: 'prefix_mismatch',
          pending_action: pendingActionRaw,
          user_id: ctx?.from?.id || null,
          chat_id: ctx?.chat?.id || null
        }));
        break;
      }
      const pendingAction = parseCallbackData(pendingActionRaw).action || pendingActionRaw;
      const pendingParts = pendingAction.split(':');
      const pendingToken = pendingParts.length ? pendingParts[pendingParts.length - 1] : '';
      const replaySelection = optionLookupByToken.get(pendingToken)
        || options.find((option) => String(option.id) === pendingToken);
      pendingReplayQueue.shift();
      if (replaySelection) {
        activeChecker();
        writePendingCallbackReplay(
          ctx,
          pendingReplayQueue,
          ctx.session?.meta?.pendingCallbackReplay?.sourceAction || null
        );
        console.log(JSON.stringify({
          type: 'conversation_callback_replayed',
          pending_action: pendingActionRaw,
          resolved_action: pendingAction,
          prefix_key: prefixKey,
          selected_id: replaySelection?.id || null,
          user_id: ctx?.from?.id || null,
          chat_id: ctx?.chat?.id || null
        }));
        return replaySelection;
      }
      dropped += 1;
    }
    if (dropped > 0) {
      writePendingCallbackReplay(
        ctx,
        pendingReplayQueue,
        ctx.session?.meta?.pendingCallbackReplay?.sourceAction || null
      );
      console.log(JSON.stringify({
        type: 'conversation_callback_replay_dropped',
        prefix_key: prefixKey,
        dropped,
        remaining: pendingReplayQueue.length,
        user_id: ctx?.from?.id || null,
        chat_id: ctx?.chat?.id || null
      }));
    }
  }

  const message = await sendMenu(ctx, prompt, { parse_mode: 'Markdown', reply_markup: keyboard });
  const expectedChatId = message?.chat?.id || ctx.chat?.id || null;
  const expectedMessageId = message?.message_id || null;
  const tapLockKey = expectedChatId && expectedMessageId
    ? `${expectedChatId}:${expectedMessageId}`
    : null;
  const waitStartedAt = Date.now();
  const waitSelectionUpdate = async () => {
    const callbackMatcher = (callbackCtx) => {
      const data = callbackCtx?.callbackQuery?.data;
      return matchesCallbackPrefix(data, prefixKey);
    };
    const useTimeout = Number.isFinite(resolvedTimeoutMs) && resolvedTimeoutMs > 0;
    if (!useTimeout) {
      return conversation.waitFor('callback_query:data', callbackMatcher);
    }
    const remainingMs = Math.max(0, resolvedTimeoutMs - (Date.now() - waitStartedAt));
    if (remainingMs <= 0) {
      throw new OperationCancelledError(`Menu callback timeout after ${resolvedTimeoutMs}ms`);
    }
    let timeoutHandle = null;
    try {
      return await Promise.race([
        conversation.waitFor('callback_query:data', callbackMatcher),
        new Promise((_, reject) => {
          timeoutHandle = setTimeout(
            () => reject(new OperationCancelledError(`Menu callback timeout after ${resolvedTimeoutMs}ms`)),
            remainingMs
          );
        })
      ]);
    } finally {
      if (timeoutHandle) {
        clearTimeout(timeoutHandle);
      }
    }
  };
  let selected = null;
  while (!selected) {
    let selectionCtx;
    try {
      selectionCtx = await waitSelectionUpdate();
    } catch (error) {
      const timeoutError =
        error instanceof OperationCancelledError ||
        /timeout/i.test(String(error?.message || ''));
      if (timeoutError) {
        writePendingCallbackReplay(ctx, [], null);
        try {
          await clearMenuMessages(ctx);
        } catch (_) {}
        if (resolvedTimeoutMessage) {
          try {
            await ctx.reply(resolvedTimeoutMessage);
          } catch (_) {}
        }
      }
      throw error;
    }
    const callbackRawData = String(selectionCtx?.callbackQuery?.data || '');
    const selectionAction = parseCallbackData(callbackRawData).action || callbackRawData;
    const callbackChatId = selectionCtx?.callbackQuery?.message?.chat?.id || null;
    const callbackMessageId = selectionCtx?.callbackQuery?.message?.message_id || null;
    console.log(JSON.stringify({
      type: 'conversation_callback_matched',
      callback_data: callbackRawData,
      action: selectionAction,
      prefix_key: prefixKey,
      matched: true,
      user_id: selectionCtx?.from?.id || ctx.from?.id || null,
      chat_id: callbackChatId || ctx.chat?.id || null,
      state: {
        op_id: ctx.session?.currentOp?.id || null,
        op_token: ctx.session?.currentOp?.token || null,
        op_command: ctx.session?.currentOp?.command || null,
        flow_name: ctx.session?.flow?.name || null,
        flow_step: ctx.session?.flow?.step || null
      }
    }));
    try {
      activeChecker();
    } catch (error) {
      try {
        await selectionCtx.answerCallbackQuery({
          text: '⚠️ This menu is no longer active.',
          show_alert: false
        });
      } catch (_) {}
      throw error;
    }
    if (expectedChatId && callbackChatId && callbackChatId !== expectedChatId) {
      try {
        await selectionCtx.answerCallbackQuery({
          text: 'That option is unavailable in this chat.',
          show_alert: false
        });
      } catch (_) {}
      continue;
    }

    // ── Message-ID stale-guard ────────────────────────────────────────────────
    // FIX: For Script Designer menus, relax the strict message-ID check.
    //
    // Background: after a bot restart or conversation recovery, the conversation
    // re-enters and re-renders the menu.  Due to grammY's conversation-replay
    // mechanism, the `expectedMessageId` captured here can briefly refer to the
    // PREVIOUS session's message.  The user's tap arrives for the freshly-
    // rendered menu, so `callbackMessageId !== expectedMessageId` fires and the
    // tap is silently dropped — causing the "no longer active" loop forever.
    //
    // For Script Designer menus we instead: log the mismatch for observability
    // and then ALLOW the callback to proceed to option-matching.  This is safe
    // because prefix-matching already filters only correct-prefix callbacks, and
    // the tap-lock prevents duplicate processing.
    //
    // Non-Script-Designer menus retain the original strict rejection behaviour.
    if (
      expectedMessageId &&
      callbackMessageId &&
      callbackMessageId !== expectedMessageId
    ) {
      if (isScriptDesignerMenu) {
        // FIX: Log mismatch but DO NOT reject — allow the tap through.
        console.log(JSON.stringify({
          type: 'conversation_msg_id_mismatch',
          prefix_key: prefixKey,
          expected_message_id: expectedMessageId,
          callback_message_id: callbackMessageId,
          action: selectionAction,
          note: 'script_designer_relaxed_guard_accepted',
          user_id: selectionCtx?.from?.id || ctx?.from?.id || null,
          chat_id: callbackChatId || ctx?.chat?.id || null
        }));
        // Fall through — do NOT `continue` — let the option-matching proceed.
      } else {
        // Non-script-designer menus: reject the stale tap as before.
        try {
          await selectionCtx.answerCallbackQuery({
            text: '⚠️ This menu is no longer active.',
            show_alert: false
          });
        } catch (_) {}
        continue;
      }
    }
    // ─────────────────────────────────────────────────────────────────────────

    if (!acquireTapLock(ctx, tapLockKey)) {
      try {
        await selectionCtx.answerCallbackQuery({
          text: '⏳ Processing previous tap…',
          show_alert: false
        });
      } catch (_) {}
      continue;
    }
    let lockHeld = Boolean(tapLockKey);
    const parts = selectionAction.split(':');
    const selectedToken = parts.length ? parts[parts.length - 1] : '';
    const matchedOption = optionLookupByToken.get(selectedToken)
      || options.find((option) => String(option.id) === selectedToken);
    try {
      if (!matchedOption) {
        releaseTapLock(ctx, tapLockKey);
        lockHeld = false;
        await selectionCtx.answerCallbackQuery({
          text: 'That option is no longer available. Please choose again.',
          show_alert: false
        });
        continue;
      }
      await selectionCtx.answerCallbackQuery();
      await disableMessageButtons(selectionCtx, expectedChatId, expectedMessageId);
      // Keep the lock for a short window after successful selection so
      // near-simultaneous duplicate taps cannot race into the next step.
      lockHeld = false;
      selected = matchedOption;
    } catch (_) {
      releaseTapLock(ctx, tapLockKey);
      lockHeld = false;
    } finally {
      if (lockHeld) {
        releaseTapLock(ctx, tapLockKey);
      }
    }
  }

  console.log(JSON.stringify({
    type: 'conversation_callback_completed',
    prefix_key: prefixKey,
    selected_id: selected?.id || null,
    user_id: ctx?.from?.id || null,
    chat_id: ctx?.chat?.id || null
  }));

  try {
    await ctx.api.deleteMessage(message.chat.id, message.message_id);
  } catch (_) {
    await ctx.api.editMessageReplyMarkup(message.chat.id, message.message_id).catch(() => {});
  }
  await clearMenuMessages(ctx);

  return selected;
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
