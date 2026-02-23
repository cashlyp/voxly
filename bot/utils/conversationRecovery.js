'use strict';

const {
  parseScriptDesignerCallbackAction,
} = require('./scriptDesignerCallbacks');

const SCRIPT_CHANNEL_SELECTIONS = ['call', 'sms', 'email', 'exit'];
const CALL_SCRIPT_MAIN_SELECTIONS = ['list', 'create', 'incoming', 'back'];
const SMS_SCRIPT_MAIN_SELECTIONS = ['list', 'create', 'back'];
const EMAIL_TEMPLATE_MAIN_SELECTIONS = ['list', 'create', 'search', 'import', 'back'];
const INBOUND_DEFAULT_SELECTIONS = ['set', 'clear', 'back'];

function mapSelectionToken(selections, selectionToken) {
  const parsedIndex = Number(selectionToken);
  if (!Number.isFinite(parsedIndex)) return null;
  if (parsedIndex < 0 || parsedIndex >= selections.length) return null;
  return selections[parsedIndex] || null;
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

function resolveConversationFromPrefix(prefix) {
  if (!prefix) return null;
  if (prefix === 'call-script-fallback') return 'call-conversation';
  if (prefix === 'call-voice') return 'call-conversation';
  if (prefix === 'call-script') return 'call-conversation';
  if (prefix.startsWith('call-script-')) return 'scripts-conversation';
  if (prefix.startsWith('inbound-default')) return 'scripts-conversation';
  if (prefix.startsWith('sms-script-')) return 'scripts-conversation';
  if (prefix === 'sms-script') return 'sms-conversation';
  if (prefix.startsWith('script-') || prefix === 'confirm') return 'scripts-conversation';
  if (prefix.startsWith('email-template-')) return 'email-templates-conversation';
  if (prefix.startsWith('bulk-email-')) return 'bulk-email-conversation';
  if (prefix.startsWith('email-')) return 'email-conversation';
  if (prefix.startsWith('bulk-sms-')) return 'bulk-sms-conversation';
  if (prefix.startsWith('sms-')) return 'sms-conversation';
  if (prefix.startsWith('persona-')) return 'persona-conversation';
  if (['persona', 'purpose', 'tone', 'urgency', 'tech', 'call-config'].includes(prefix)) {
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

  const normalizedSelection = String(selectionToken);
  const normalizedAction = `${parsed.prefix}:${normalizedSelection}`;

  if (parsed.prefix === 'script-channel') {
    const selectionId = mapSelectionToken(SCRIPT_CHANNEL_SELECTIONS, selectionToken);
    return selectionId ? [`script-channel:${selectionId}`] : [normalizedAction];
  }
  if (parsed.prefix === 'call-script-main') {
    const selectionId = mapSelectionToken(CALL_SCRIPT_MAIN_SELECTIONS, selectionToken);
    return selectionId
      ? ['script-channel:call', `call-script-main:${selectionId}`]
      : ['script-channel:call', normalizedAction];
  }
  if (parsed.prefix === 'sms-script-main') {
    const selectionId = mapSelectionToken(SMS_SCRIPT_MAIN_SELECTIONS, selectionToken);
    return selectionId
      ? ['script-channel:sms', `sms-script-main:${selectionId}`]
      : ['script-channel:sms', normalizedAction];
  }
  if (parsed.prefix === 'email-template-main') {
    const selectionId = mapSelectionToken(
      EMAIL_TEMPLATE_MAIN_SELECTIONS,
      selectionToken,
    );
    return selectionId
      ? ['script-channel:email', `email-template-main:${selectionId}`]
      : ['script-channel:email', normalizedAction];
  }
  // Do not replay dynamic selections after recovery. Reopen the parent menu so
  // users can intentionally reselect current data without index mismatch risk.
  if (parsed.prefix === 'inbound-default-select') {
    return ['script-channel:call', 'call-script-main:incoming', 'inbound-default:set'];
  }
  if (parsed.prefix === 'inbound-default') {
    const selectionId = mapSelectionToken(INBOUND_DEFAULT_SELECTIONS, selectionToken);
    return selectionId
      ? ['script-channel:call', 'call-script-main:incoming', `inbound-default:${selectionId}`]
      : ['script-channel:call', 'call-script-main:incoming', normalizedAction];
  }

  // Only static Script Designer navigation menus are replayable.
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
  const { notify = true, message = '↩️ Reopening that flow so you can continue.', sessionMeta = null } = options;
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
      ...sessionMeta
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
  recoverConversationFromCallback
};
