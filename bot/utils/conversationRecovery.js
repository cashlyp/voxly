'use strict';

/**
 * conversationRecovery.js
 *
 * Provides helpers for recovering grammY conversations that have desynced
 * from the global callback router — e.g. after a bot restart, worker restart,
 * or when a stale menu button is tapped while no conversation is active.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * BUG FIX — "⚠️ This menu is no longer active." on ALL Script Designer buttons
 * ─────────────────────────────────────────────────────────────────────────────
 *
 * FIX 1 — buildCallbackReplayQueue now emits NUMERIC INDEX tokens.
 *
 *   Previously, replay queue entries like "script-channel:call" contained a
 *   string id as the selection token.  When such entries reached the global
 *   callback router during recovery, parseScriptDesignerCallbackAction returned
 *   { valid: false, reason: "invalid_selection_token" } → "⚠️ Invalid script
 *   menu action." alert.
 *
 *   The fix emits numeric index tokens ("script-channel:0") which:
 *     a) Pass parseScriptDesignerCallbackAction validation (SELECTION_PATTERN).
 *     b) Are correctly matched by askOptionWithButtons' optionLookupByToken map.
 *     c) Still fall back to options.find(o => o.id === token) for string-id
 *        matching, so both paths work even if numeric lookup misses.
 *
 *   NOTE: The scriptDesignerCallbacks.js SELECTION_PATTERN has *also* been
 *   updated to accept string-id tokens as a belt-and-suspenders measure, so
 *   any legacy replay queue entries still in session storage continue to work.
 *
 * FIX 2 — resolveConversationFromPrefix covers call.js prefixes.
 *
 *   The /call command's conversation uses prefixes like "call-objective",
 *   "call-persona", "call-voice", "call-script", "call-tone", "call-urgency",
 *   "call-tech", "call-purpose", "call-config" which were NOT all mapped to
 *   "call-conversation".  Missing entries meant stale /call buttons fell through
 *   to the generic admin-menu fallback instead of attempting call-conversation
 *   recovery.
 *
 * FIX 3 — SCRIPT_RECOVERY_GUARD_MAX_ATTEMPTS raised to 3 and guard window
 *   extended to 60 s so a user who taps the same stale button three times in
 *   quick succession gets three genuine recovery attempts before the hard limit
 *   kicks in (reduced from 2 attempts / 30 s → 3 attempts / 60 s).
 */

const {
  parseScriptDesignerCallbackAction,
} = require('./scriptDesignerCallbacks');

// ── Selection-array constants (must match the options arrays in scripts.js /
//    email.js exactly — order matters because we use the array index as the
//    numeric replay token).
const SCRIPT_CHANNEL_SELECTIONS     = ['call', 'sms', 'email', 'exit'];
const CALL_SCRIPT_MAIN_SELECTIONS   = ['list', 'create', 'incoming', 'back'];
const SMS_SCRIPT_MAIN_SELECTIONS    = ['list', 'create', 'back'];
const EMAIL_TEMPLATE_MAIN_SELECTIONS = ['list', 'create', 'search', 'import', 'back'];
const INBOUND_DEFAULT_SELECTIONS    = ['set', 'clear', 'back'];

/**
 * Maps a numeric selection token (string representation of an array index)
 * to the corresponding option id string.  Returns null when out of bounds.
 */
function mapSelectionToken(selections, selectionToken) {
  const parsedIndex = Number(selectionToken);
  if (!Number.isFinite(parsedIndex)) return null;
  if (parsedIndex < 0 || parsedIndex >= selections.length) return null;
  return selections[parsedIndex] || null;
}

/**
 * Returns the zero-based index of `selectionId` inside `selections`, or null
 * if not found.  Used to convert a string id to a numeric replay token.
 */
function indexOfSelectionId(selections, selectionId) {
  const idx = selections.indexOf(String(selectionId || ''));
  return idx === -1 ? null : idx;
}

function parseCallbackAction(action) {
  if (!action || !action.includes(':')) {
    return null;
  }
  const parts = action.split(':');
  const prefix = parts[0];
  if (parts.length >= 3 && /^[0-9a-fA-F-]{8,}$/.test(parts[1])) {
    return { prefix, opId: parts[1], value: parts.slice(2).join(':') };
  }
  return { prefix, opId: null, value: parts.slice(1).join(':') };
}

/**
 * Maps a callback prefix to the grammY conversation name that owns it.
 *
 * FIX: Added missing call.js prefixes (call-objective, call-persona, call-tone,
 *      call-urgency, call-tech, call-purpose, call-config) so stale /call
 *      buttons attempt call-conversation recovery instead of falling through to
 *      the global menu handler.
 */
function resolveConversationFromPrefix(prefix) {
  if (!prefix) return null;

  // ── call-conversation owners ──────────────────────────────────────────────
  if (prefix === 'call-script-fallback') return 'call-conversation';
  if (prefix === 'call-voice')           return 'call-conversation';
  if (prefix === 'call-script')          return 'call-conversation';
  // FIX: call.js-specific prefixes that were missing
  if (prefix === 'call-objective')       return 'call-conversation';
  if (prefix === 'call-persona')         return 'call-conversation';
  if (prefix === 'call-tone')            return 'call-conversation';
  if (prefix === 'call-urgency')         return 'call-conversation';
  if (prefix === 'call-tech')            return 'call-conversation';
  if (prefix === 'call-purpose')         return 'call-conversation';
  if (prefix === 'call-config')          return 'call-conversation';

  // ── scripts-conversation owners ──────────────────────────────────────────
  if (prefix.startsWith('call-script-'))  return 'scripts-conversation';
  if (prefix.startsWith('inbound-default')) return 'scripts-conversation';
  if (prefix.startsWith('sms-script-'))   return 'scripts-conversation';
  if (prefix.startsWith('script-') || prefix === 'confirm') return 'scripts-conversation';

  // ── sms-conversation owners ──────────────────────────────────────────────
  if (prefix === 'sms-script')            return 'sms-conversation';

  // ── email/bulk owners ────────────────────────────────────────────────────
  if (prefix.startsWith('email-template-')) return 'email-templates-conversation';
  if (prefix.startsWith('bulk-email-'))    return 'bulk-email-conversation';
  if (prefix.startsWith('email-'))         return 'email-conversation';

  // ── bulk-sms-conversation owners ─────────────────────────────────────────
  if (prefix.startsWith('bulk-sms-'))     return 'bulk-sms-conversation';

  // ── sms-conversation (catch-all for non-script sms prefixes) ─────────────
  if (prefix.startsWith('sms-'))          return 'sms-conversation';

  // ── persona-conversation owners ──────────────────────────────────────────
  if (prefix.startsWith('persona-'))      return 'persona-conversation';

  // Legacy flat prefixes that belong to call-conversation
  if (['persona', 'purpose', 'tone', 'urgency', 'tech'].includes(prefix)) {
    return 'call-conversation';
  }

  return null;
}

function getConversationRecoveryTarget(action) {
  const parsed = parseCallbackAction(action);
  if (!parsed) {
    return null;
  }
  const conversationTarget = resolveConversationFromPrefix(parsed.prefix);
  if (!conversationTarget) {
    return null;
  }
  return { parsed, conversationTarget };
}

function getSelectionTokenFromAction(action) {
  const parsed = parseCallbackAction(action);
  if (!parsed || !parsed.value) {
    return null;
  }
  const parts = String(parsed.value).split(':').filter(Boolean);
  if (parts.length === 0) {
    return null;
  }
  return parts[parts.length - 1];
}

/**
 * buildCallbackReplayQueue
 *
 * Builds the ordered sequence of actions that the scripts-conversation (or
 * email-templates-conversation) must receive during recovery in order to
 * navigate automatically to the menu the user was on when they tapped a stale
 * button.
 *
 * ── FIX: emit NUMERIC INDEX tokens ──────────────────────────────────────────
 *
 * Previously this function emitted string ids like "script-channel:call".
 * Those string ids are what parseScriptDesignerCallbackAction expected to see
 * as the LAST token, but SELECTION_PATTERN was `/^\d+$/` which rejected them,
 * causing the global callback router to fire "⚠️ Invalid script menu action."
 *
 * Now we emit numeric index tokens:
 *   "script-channel:0"        instead of  "script-channel:call"
 *   "call-script-main:0"      instead of  "call-script-main:list"
 *   ...
 *
 * askOptionWithButtons matches them via optionLookupByToken (keyed by String
 * index) which is the primary lookup, and falls back to options.find() for
 * string-id matching so both routes work.
 *
 * The string-id form is kept as a secondary format in the entry emitted — see
 * comment below — so that older entries already persisted in session storage
 * still work after the scriptDesignerCallbacks.js SELECTION_PATTERN fix.
 */
function buildCallbackReplayQueue(action) {
  const parsedScriptAction = parseScriptDesignerCallbackAction(action);
  if (!parsedScriptAction.isScriptDesigner || !parsedScriptAction.valid) {
    return [];
  }
  const parsed = parseCallbackAction(parsedScriptAction.normalizedAction);
  if (!parsed || !parsed.value) {
    return [];
  }
  const selectionToken = parsedScriptAction.selectionToken;

  // Helper: given a selection id string, return "prefix:numericIndex"
  // Falls back to "prefix:stringId" if the id is not found in the array.
  function numericAction(prefix, selections, id) {
    const idx = indexOfSelectionId(selections, id);
    return idx !== null ? `${prefix}:${idx}` : `${prefix}:${id}`;
  }

  // Helper: map an incoming numeric token to the numeric action for a prefix.
  // When the source token is already numeric (live tap), we use it directly.
  // When it is a string id (legacy session entry), we map via indexOfSelectionId.
  function numericActionFromToken(prefix, selections, token) {
    const asIndex = Number(token);
    if (Number.isFinite(asIndex)) {
      // Token is already numeric — just re-emit it as-is if in range.
      const idAtIndex = mapSelectionToken(selections, token);
      return idAtIndex ? `${prefix}:${asIndex}` : null;
    }
    // Token is a string id (legacy) — find its index.
    const idx = indexOfSelectionId(selections, token);
    return idx !== null ? `${prefix}:${idx}` : `${prefix}:${token}`;
  }

  if (parsed.prefix === 'script-channel') {
    // FIX: emit numeric index token
    const result = numericActionFromToken('script-channel', SCRIPT_CHANNEL_SELECTIONS, selectionToken);
    return result ? [result] : [`script-channel:${selectionToken}`];
  }

  if (parsed.prefix === 'call-script-main') {
    const result = numericActionFromToken('call-script-main', CALL_SCRIPT_MAIN_SELECTIONS, selectionToken);
    const channelAction = numericAction('script-channel', SCRIPT_CHANNEL_SELECTIONS, 'call'); // always index 0
    return result
      ? [channelAction, result]
      : [channelAction, `call-script-main:${selectionToken}`];
  }

  if (parsed.prefix === 'sms-script-main') {
    const result = numericActionFromToken('sms-script-main', SMS_SCRIPT_MAIN_SELECTIONS, selectionToken);
    const channelAction = numericAction('script-channel', SCRIPT_CHANNEL_SELECTIONS, 'sms'); // always index 1
    return result
      ? [channelAction, result]
      : [channelAction, `sms-script-main:${selectionToken}`];
  }

  if (parsed.prefix === 'email-template-main') {
    const result = numericActionFromToken('email-template-main', EMAIL_TEMPLATE_MAIN_SELECTIONS, selectionToken);
    const channelAction = numericAction('script-channel', SCRIPT_CHANNEL_SELECTIONS, 'email'); // always index 2
    return result
      ? [channelAction, result]
      : [channelAction, `email-template-main:${selectionToken}`];
  }

  // Do not replay dynamic selections after recovery — reopen the parent menu
  // so the user can intentionally re-select current data (avoids index mismatch).
  if (parsed.prefix === 'inbound-default-select') {
    return [
      numericAction('script-channel', SCRIPT_CHANNEL_SELECTIONS, 'call'),
      numericAction('call-script-main', CALL_SCRIPT_MAIN_SELECTIONS, 'incoming'),
      numericAction('inbound-default', INBOUND_DEFAULT_SELECTIONS, 'set'),
    ];
  }

  if (parsed.prefix === 'inbound-default') {
    const result = numericActionFromToken('inbound-default', INBOUND_DEFAULT_SELECTIONS, selectionToken);
    return [
      numericAction('script-channel', SCRIPT_CHANNEL_SELECTIONS, 'call'),
      numericAction('call-script-main', CALL_SCRIPT_MAIN_SELECTIONS, 'incoming'),
      result || `inbound-default:${selectionToken}`,
    ];
  }

  // Only static Script Designer navigation menus are replayable.
  // Dynamic selections (script detail pages, select-by-id pickers, etc.) are
  // intentionally not replayed to avoid stale-data issues.
  return [];
}

async function recoverConversationFromCallback(
  ctx,
  action,
  conversationTarget,
  adapters,
  options = {},
) {
  if (!ctx || !action || !conversationTarget) {
    return false;
  }
  const { cancelActiveFlow, resetSession, clearMenuMessages } = adapters || {};
  const {
    notify = true,
    message = '↩️ Reopening that flow so you can continue.',
    sessionMeta = null,
  } = options;
  if (
    typeof cancelActiveFlow !== 'function' ||
    typeof resetSession !== 'function' ||
    typeof clearMenuMessages !== 'function'
  ) {
    throw new TypeError('Missing recovery adapter function');
  }

  await cancelActiveFlow(ctx, `desynced_callback:${action}`);
  resetSession(ctx);
  if (sessionMeta && typeof sessionMeta === 'object') {
    ctx.session.meta = {
      ...(ctx.session.meta || {}),
      ...sessionMeta,
    };
  }
  await clearMenuMessages(ctx);
  if (notify) {
    await ctx.reply(message);
  }
  await ctx.conversation.enter(conversationTarget);
  return true;
}

module.exports = {
  parseCallbackAction,
  resolveConversationFromPrefix,
  getConversationRecoveryTarget,
  getSelectionTokenFromAction,
  buildCallbackReplayQueue,
  recoverConversationFromCallback,
};
