const { InlineKeyboard } = require('grammy');
const { buildCallbackData } = require('./actions');
const { ensureSession } = require('./sessionState');
const config = require('../config');

const DEFAULT_MENU_TTL_MS = 15 * 60 * 1000;
const DEFAULT_EPHEMERAL_TTL_MS = Number.isFinite(config.ui?.ephemeralTtlMs)
    ? config.ui.ephemeralTtlMs
    : 8 * 1000;
const MIN_EPHEMERAL_TTL_MS = 1000;
const MAX_EPHEMERAL_TTL_MS = 60 * 1000;

function clampEphemeralTtl(ttlMs) {
    const parsed = Number(ttlMs);
    if (!Number.isFinite(parsed)) {
        return DEFAULT_EPHEMERAL_TTL_MS;
    }
    if (parsed < MIN_EPHEMERAL_TTL_MS) {
        return MIN_EPHEMERAL_TTL_MS;
    }
    if (parsed > MAX_EPHEMERAL_TTL_MS) {
        return MAX_EPHEMERAL_TTL_MS;
    }
    return parsed;
}

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

function selectionExpiredMessage() {
    return section('⌛ Selection expired', [
        'That menu button is no longer active.',
        'Please choose an option again.'
    ]);
}

function cancelledMessage(flowLabel = 'Action', nextHint = 'Use /menu to continue.') {
    return section(`🛑 ${flowLabel} cancelled`, [
        'No changes were applied.',
        nextHint
    ]);
}

function setupStepMessage(flowLabel, lines = []) {
    return section(`🧭 ${flowLabel}`, lines);
}

function buildMainMenuReplyMarkup(ctx, label = '⬅️ Main Menu') {
    return {
        inline_keyboard: [[{ text: label, callback_data: buildCallbackData(ctx, 'MENU') }]]
    };
}

function buildMainMenuKeyboard(ctx, label = '⬅️ Main Menu') {
    return new InlineKeyboard().text(label, buildCallbackData(ctx, 'MENU'));
}

function appendBackToMenuRows(
    keyboard,
    ctx,
    { backAction = null, backLabel = '⬅️ Back', mainLabel = '⬅️ Main Menu' } = {}
) {
    const target = keyboard || new InlineKeyboard();
    let hasRows = Array.isArray(target.inline_keyboard)
        && target.inline_keyboard.some((row) => Array.isArray(row) && row.length > 0);

    if (backAction) {
        if (hasRows) {
            target.row();
        }
        target.text(backLabel, buildCallbackData(ctx, backAction));
        hasRows = true;
    }

    if (hasRows) {
        target.row();
    }
    target.text(mainLabel, buildCallbackData(ctx, 'MENU'));
    return target;
}

function buildBackToMenuKeyboard(ctx, { backAction = null, backLabel = '⬅️ Back' } = {}) {
    return appendBackToMenuRows(new InlineKeyboard(), ctx, {
        backAction,
        backLabel
    });
}

function buildBackToMenuReplyMarkup(ctx, options = {}) {
    const keyboard = buildBackToMenuKeyboard(ctx, options);
    return { inline_keyboard: keyboard.inline_keyboard };
}

async function styledAlert(ctx, message, options = {}) {
    return ctx.reply(section('⚠️ Notice', [message]), { parse_mode: 'Markdown', ...options });
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

function removeMenuEntry(ctx, chatId, messageId) {
    if (!chatId || !messageId) return;
    const entries = getMenuEntries(ctx).filter((entry) => !(
        entry?.chatId === chatId && entry?.messageId === messageId
    ));
    setMenuEntries(ctx, entries);
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

function toMessageRef(message, ctx) {
    const chatId = message?.chat?.id || message?.chatId || ctx.chat?.id || null;
    const messageId = message?.message_id || message?.messageId || null;
    if (!chatId || !messageId) {
        return null;
    }
    return {
        chat: { id: chatId },
        message_id: messageId,
        chatId,
        messageId
    };
}

function isMessageNotModifiedError(error) {
    const message = String(error?.message || "").toLowerCase();
    return message.includes("message is not modified");
}

async function upsertMenuMessage(ctx, currentMessage, text, options = {}) {
    const normalized = normalizeReply(text, options);
    const current = toMessageRef(currentMessage, ctx);
    if (current?.chatId && current?.messageId) {
        try {
            const edited = await ctx.api.editMessageText(
                current.chatId,
                current.messageId,
                normalized.text,
                normalized.options
            );
            const nextRef = toMessageRef(edited || current, ctx) || current;
            registerMenuMessage(ctx, nextRef);
            return nextRef;
        } catch (error) {
            if (isMessageNotModifiedError(error)) {
                registerMenuMessage(ctx, current);
                return current;
            }
        }
    }
    const sent = await sendMenu(ctx, normalized.text, normalized.options);
    return toMessageRef(sent, ctx) || sent;
}

async function dismissMenuMessage(ctx, message, { clearOnly = false } = {}) {
    const ref = toMessageRef(message, ctx);
    if (!ref?.chatId || !ref?.messageId) {
        return false;
    }
    let handled = false;
    if (!clearOnly) {
        try {
            await ctx.api.deleteMessage(ref.chatId, ref.messageId);
            handled = true;
        } catch (_) {
            // Fallback to clearing markup below.
        }
    }
    if (!handled) {
        try {
            await ctx.api.editMessageReplyMarkup(ref.chatId, ref.messageId);
            handled = true;
        } catch (_) {
            handled = false;
        }
    }
    removeMenuEntry(ctx, ref.chatId, ref.messageId);
    return handled;
}

async function sendEphemeral(ctx, text, options = {}) {
    const {
        ttlMs = DEFAULT_EPHEMERAL_TTL_MS,
        parseMode = null,
        replyMarkup = null,
        payload = {}
    } = options;

    const normalized = normalizeReply(text, {
        ...payload,
        ...(parseMode ? { parse_mode: parseMode } : {}),
        ...(replyMarkup ? { reply_markup: replyMarkup } : {})
    });
    const message = await ctx.reply(normalized.text, normalized.options);

    const chatId = message?.chat?.id || ctx.chat?.id;
    const messageId = message?.message_id;
    if (!chatId || !messageId) {
        return message;
    }

    const delay = clampEphemeralTtl(ttlMs);
    const timer = setTimeout(async () => {
        try {
            await ctx.api.deleteMessage(chatId, messageId);
            return;
        } catch (_) {
            // Ignore and try fallback below.
        }
        try {
            await ctx.api.editMessageReplyMarkup(chatId, messageId);
        } catch (_) {
            // Ignore if neither delete nor edit is possible.
        }
    }, delay);

    if (typeof timer.unref === 'function') {
        timer.unref();
    }
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
    selectionExpiredMessage,
    cancelledMessage,
    setupStepMessage,
    buildMainMenuReplyMarkup,
    buildMainMenuKeyboard,
    appendBackToMenuRows,
    buildBackToMenuKeyboard,
    buildBackToMenuReplyMarkup,
    styledAlert,
    sendMenu,
    sendEphemeral,
    clearMenuMessages,
    registerMenuMessage,
    activateMenuMessage,
    upsertMenuMessage,
    dismissMenuMessage,
    getLatestMenuMessageId,
    isLatestMenuExpired,
    renderMenu
};
