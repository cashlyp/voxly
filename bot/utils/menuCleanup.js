const { ensureSession } = require('./sessionState');

function getMenuEntries(ctx) {
  ensureSession(ctx);
  if (!Array.isArray(ctx.session.menuMessages)) {
    ctx.session.menuMessages = [];
  }
  return ctx.session.menuMessages;
}

function setMenuEntries(ctx, entries) {
  ensureSession(ctx);
  ctx.session.menuMessages = Array.isArray(entries) ? entries : [];
}

function registerMenuMessage(ctx, message) {
  const messageId = message?.message_id;
  const chatId = message?.chat?.id || ctx.chat?.id;
  if (!messageId || !chatId) {
    return;
  }
  const entries = getMenuEntries(ctx).filter(
    (entry) => !(entry.chatId === chatId && entry.messageId === messageId)
  );
  entries.push({ chatId, messageId, createdAt: Date.now() });
  setMenuEntries(ctx, entries);
}

async function clearMenuMessages(ctx, { keepMessageId = null } = {}) {
  const entries = getMenuEntries(ctx);
  if (entries.length === 0) {
    return;
  }

  const nextEntries = [];
  for (const entry of entries) {
    if (keepMessageId && entry.messageId === keepMessageId) {
      nextEntries.push(entry);
      continue;
    }
    if (!entry.chatId || !entry.messageId) {
      continue;
    }
    try {
      await ctx.api.deleteMessage(entry.chatId, entry.messageId);
      continue;
    } catch (_) {
      // Fallback: remove buttons if deletion is not allowed (e.g., older messages).
    }
    try {
      await ctx.api.editMessageReplyMarkup(entry.chatId, entry.messageId);
    } catch (_) {
      // Ignore if we can't edit or delete.
    }
  }

  setMenuEntries(ctx, nextEntries);
}

async function sendMenu(ctx, text, options = {}) {
  await clearMenuMessages(ctx);
  const message = await ctx.reply(text, options);
  registerMenuMessage(ctx, message);
  return message;
}

async function activateMenuMessage(ctx, messageId, chatId = null) {
  const resolvedChatId = chatId || ctx.chat?.id;
  if (!messageId || !resolvedChatId) {
    return;
  }
  await clearMenuMessages(ctx, { keepMessageId: messageId });
  registerMenuMessage(ctx, { chat: { id: resolvedChatId }, message_id: messageId });
}

module.exports = {
  sendMenu,
  clearMenuMessages,
  registerMenuMessage,
  activateMenuMessage
};
