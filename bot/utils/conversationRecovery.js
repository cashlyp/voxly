'use strict';

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
  if (prefix.startsWith('call-script-')) return 'scripts-conversation';
  if (prefix === 'call-script') return 'call-conversation';
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

async function recoverConversationFromCallback(ctx, action, conversationTarget, adapters) {
  if (!ctx || !action || !conversationTarget) {
    return false;
  }
  const { cancelActiveFlow, resetSession, clearMenuMessages } = adapters || {};
  if (
    typeof cancelActiveFlow !== 'function' ||
    typeof resetSession !== 'function' ||
    typeof clearMenuMessages !== 'function'
  ) {
    throw new TypeError('Missing recovery adapter function');
  }

  await cancelActiveFlow(ctx, `desynced_callback:${action}`);
  resetSession(ctx);
  await clearMenuMessages(ctx);
  await ctx.reply('↩️ Reopening that flow so you can continue.');
  await ctx.conversation.enter(conversationTarget);
  return true;
}

module.exports = {
  parseCallbackAction,
  resolveConversationFromPrefix,
  getConversationRecoveryTarget,
  recoverConversationFromCallback
};
