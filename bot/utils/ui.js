const { ensureSession } = require('./sessionState');

const DEFAULT_MENU_TTL_MS = 15 * 60 * 1000;

function normalizeReply(text, options = {}) {
    const normalizedText = text === undefined || text === null ? '' : String(text);
    const normalizedOptions = { ...options };

    if (!normalizedOptions.parse_mode) {
        if (/<[^>]+>/.test(normalizedText)) {
            normalizedOptions.parse_mode = 'HTML';
        } else if (/[`*_]/.test(normalizedText)) {
            normalizedOptions.parse_mode = 'Markdown';
        }
    }

    return { text: normalizedText, options: normalizedOptions };
}

function logCommandError(ctx, error) {
    const command = ctx.session?.lastCommand || ctx.message?.text || ctx.callbackQuery?.data || 'unknown';
    const userId = ctx.from?.id || 'unknown';
    const username = ctx.from?.username || 'unknown';
    const message = error?.message || error;
    console.error(`Command error (${command}) for user ${username} (${userId}):`, message);
}

function escapeHtml(text = '') {
    if (text === null || text === undefined) return '';
    return String(text)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;');
}

function escapeMarkdown(text = '') {
    return String(text).replace(/([_*[\]()`])/g, '\\$1');
}

function emphasize(text = '') {
    return `*${text}*`;
}

function buildLine(icon, label, value) {
    const safeLabel = label ? escapeMarkdown(label) : '';
    const safeValue = value === undefined || value === null ? '' : String(value);
    return `${icon} ${safeLabel ? `*${safeLabel}:* ` : ''}${safeValue}`;
}

function tipLine(icon, text) {
    return `${icon} ${text}`;
}

function section(title, lines = []) {
    const body = Array.isArray(lines) ? lines : [lines];
    const cleaned = body.filter(Boolean);
    const header = emphasize(title);
    if (!cleaned.length) {
        return header;
    }
    return `${header}\n${cleaned.join('\n')}`;
}

async function styledAlert(ctx, message, options = {}) {
    return ctx.reply(section('â ï¸ Notice', [message]), { parse_mode: 'Markdown', ...options });
}

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

function getLatestMenuEntry(ctx, chatId = null) {
    const entries = getMenuEntries(ctx).filter((entry) => {
        if (!chatId) return true;
        return entry.chatId === chatId;
    });
    if (entries.length === 0) {
        return null;
    }
    return entries.reduce((latest, entry) => {
        if (!latest || (entry.createdAt || 0) > (latest.createdAt || 0)) {
            return entry;
        }
        return latest;
    }, null);
}

function getLatestMenuMessageId(ctx, chatId = null) {
    const entry = getLatestMenuEntry(ctx, chatId);
    return entry ? entry.messageId : null;
}

function isMenuEntryExpired(entry, ttlMs = DEFAULT_MENU_TTL_MS) {
    if (!entry || typeof entry.createdAt !== 'number') {
        return false;
    }
    return Date.now() - entry.createdAt > ttlMs;
}

function isLatestMenuExpired(ctx, chatId = null, ttlMs = DEFAULT_MENU_TTL_MS) {
    const entry = getLatestMenuEntry(ctx, chatId);
    return isMenuEntryExpired(entry, ttlMs);
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

async function renderMenu(ctx, text, keyboard, options = {}) {
    let replyMarkup = keyboard;
    const payloadOptions = options.payload || {};
    if (!replyMarkup && payloadOptions.reply_markup) {
        replyMarkup = payloadOptions.reply_markup;
    }
    const payload = {
        ...payloadOptions,
        parse_mode: options.parseMode || payloadOptions.parse_mode,
        reply_markup: replyMarkup
    };
    return sendMenu(ctx, text, payload);
}

module.exports = {
    normalizeReply,
    logCommandError,
    escapeHtml,
    escapeMarkdown,
    emphasize,
    buildLine,
    tipLine,
    section,
    styledAlert,
    sendMenu,
    clearMenuMessages,
    registerMenuMessage,
    activateMenuMessage,
    getLatestMenuMessageId,
    isLatestMenuExpired,
    renderMenu
};
