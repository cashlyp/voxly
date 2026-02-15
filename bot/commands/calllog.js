const config = require('../config');
const httpClient = require('../utils/httpClient');
const { InlineKeyboard } = require('grammy');
const { getUser } = require('../db/db');
const { startOperation, ensureOperationActive, OperationCancelledError, waitForConversationText } = require('../utils/sessionState');
const { renderMenu, escapeMarkdown, buildLine, section } = require('../utils/ui');
const { buildCallbackData } = require('../utils/actions');
const { getAccessProfile } = require('../utils/capabilities');

function maskPhoneForDisplay(value = '') {
    const text = String(value || '').trim();
    if (!text) return 'N/A';
    const digits = text.replace(/\D/g, '');
    if (digits.length < 4) return '***';
    const prefix = text.startsWith('+') ? '+' : '';
    return `${prefix}***${digits.slice(-4)}`;
}

function summarizeSensitiveText(value = '') {
    const text = String(value || '').trim();
    if (!text) return '';
    return `🔒 redacted (${text.length} chars)`;
}

function parseRecentFilter(input = '') {
    const trimmed = String(input || '').trim();
    if (!trimmed) return null;
    const looksLikePhone = /^[+\d\s().-]+$/.test(trimmed);
    if (looksLikePhone) {
        return { phone: trimmed };
    }
    return { status: trimmed };
}

function isSessionCancellationError(error) {
    if (!error) return false;
    if (error instanceof OperationCancelledError) return true;
    const name = String(error.name || '');
    return name === 'AbortError' || name === 'CanceledError';
}

async function fetchRecentCalls({ limit = 10, filter } = {}) {
    const filterParams = parseRecentFilter(filter);
    const candidates = [
        {
            url: `${config.apiUrl}/api/calls/list`,
            params: { limit, ...(filterParams || {}) },
            filtered: Boolean(filterParams)
        },
        {
            url: `${config.apiUrl}/api/calls`,
            params: { limit },
            filtered: false
        }
    ];

    let lastError;
    for (const candidate of candidates) {
        try {
            const res = await httpClient.get(null, candidate.url, {
                params: candidate.params,
                timeout: 12000
            });
            return {
                calls: res.data?.calls || res.data || [],
                filtered: candidate.filtered
            };
        } catch (error) {
            lastError = error;
            if (error.response?.status === 404) {
                continue;
            }
            throw error;
        }
    }
    throw lastError || new Error('Failed to fetch calls');
}

function buildCalllogMenuKeyboard(ctx) {
    return new InlineKeyboard()
        .text('🕒 Recent Calls', buildCallbackData(ctx, 'CALLLOG_RECENT'))
        .text('🔍 Search', buildCallbackData(ctx, 'CALLLOG_SEARCH'))
        .row()
        .text('📄 Call Details', buildCallbackData(ctx, 'CALLLOG_DETAILS'))
        .text('🧾 Recent Events', buildCallbackData(ctx, 'CALLLOG_EVENTS'))
        .row()
        .text('⬅️ Back', buildCallbackData(ctx, 'MENU'))
        .text('🚪 Exit', buildCallbackData(ctx, 'MENU_EXIT'));
}

function buildMainMenuKeyboard(ctx) {
    return new InlineKeyboard().text('⬅️ Main Menu', buildCallbackData(ctx, 'MENU'));
}

async function renderCalllogMenu(ctx) {
    const access = await getAccessProfile(ctx);
    startOperation(ctx, 'calllog-menu');
    const keyboard = buildCalllogMenuKeyboard(ctx);
    const title = access.user ? '📜 *Call Log*' : '🔒 *Call Log (Access limited)*';
    const lines = [
        'Choose an action to explore call history.',
        'Search by phone, call ID, status, or date.',
        access.user ? 'Authorized access enabled.' : 'Limited access: request approval to view details.',
        access.user ? '' : '🔒 Actions are locked without approval.'
    ].filter(Boolean);
    await renderMenu(ctx, `${title}\n${lines.join('\n')}`, keyboard, { parseMode: 'Markdown' });
}

async function calllogRecentFlow(conversation, ctx) {
    const opId = startOperation(ctx, 'calllog-recent');
    const ensureActive = () => ensureOperationActive(ctx, opId);
    try {
        const user = await new Promise((resolve) => getUser(ctx.from.id, resolve));
        ensureActive();
        if (!user) {
            await ctx.reply('❌ You are not authorized to use this bot.');
            return;
        }

        await ctx.reply('🕒 Enter limit (max 30) and optional filter (status or phone).\nExample: `15 completed` or `20 +1234567890`', {
            parse_mode: 'Markdown'
        });
        const { text: rawText } = await waitForConversationText(conversation, ctx, {
            ensureActive,
            allowEmpty: true,
            invalidMessage: '⚠️ Please send the limit/filter as text.'
        });
        const raw = rawText || '';
        const parts = raw.split(/\s+/).filter(Boolean);
        const limit = Math.min(parseInt(parts[0], 10) || 10, 30);
        const filter = parts.slice(1).join(' ');

        const { calls, filtered } = await fetchRecentCalls({ limit, filter });
        if (!calls.length) {
            await ctx.reply('ℹ️ No recent calls found.', {
                reply_markup: buildMainMenuKeyboard(ctx)
            });
            return;
        }

        const lines = calls.map((call) => {
            const status = call.status || 'unknown';
            const when = new Date(call.created_at).toLocaleString();
            const duration = call.duration ? `${Math.floor(call.duration / 60)}:${String(call.duration % 60).padStart(2, '0')}` : 'N/A';
            return [
                `• ${escapeMarkdown(call.call_sid || 'unknown')} (${escapeMarkdown(status)})`,
                `📞 ${escapeMarkdown(maskPhoneForDisplay(call.phone_number))}`,
                `⏱️ ${duration} | 🕒 ${escapeMarkdown(when)}`
            ].join('\n');
        });

        const header = filter && !filtered
            ? 'ℹ️ Filter unavailable on this API; showing latest calls.\n\n'
            : '';
        await ctx.reply(`${header}${lines.join('\n\n')}`, {
            parse_mode: 'Markdown',
            reply_markup: buildMainMenuKeyboard(ctx)
        });
    } catch (error) {
        if (isSessionCancellationError(error)) {
            return;
        }
        await ctx.reply(httpClient.getUserMessage(error, 'Failed to fetch recent calls.'), {
            reply_markup: buildMainMenuKeyboard(ctx)
        });
    }
}

async function calllogSearchFlow(conversation, ctx) {
    const opId = startOperation(ctx, 'calllog-search');
    const ensureActive = () => ensureOperationActive(ctx, opId);
    try {
        const user = await new Promise((resolve) => getUser(ctx.from.id, resolve));
        ensureActive();
        if (!user) {
            await ctx.reply('❌ You are not authorized to use this bot.');
            return;
        }
        await ctx.reply('🔍 Enter a search term (phone, call ID, or status).');
        const { text: query } = await waitForConversationText(conversation, ctx, {
            ensureActive,
            invalidMessage: '⚠️ Please send the search term as text.'
        });
        if (!query || query.length < 2) {
            await ctx.reply('❌ Please provide at least 2 characters.');
            return;
        }

        await ctx.reply('🔍 Searching call log…');
        const res = await httpClient.get(null, `${config.apiUrl}/api/calls/search`, {
            params: { q: query, limit: 10 },
            timeout: 12000
        });
        const results = res.data?.results || [];
        if (!results.length) {
            await ctx.reply('ℹ️ No matches found.', {
                reply_markup: buildMainMenuKeyboard(ctx)
            });
            return;
        }

        const lines = results.slice(0, 5).map((c) => {
            const status = c.status || 'unknown';
            const when = new Date(c.created_at).toLocaleString();
            const summary = c.call_summary ? `\n📝 ${escapeMarkdown(summarizeSensitiveText(c.call_summary))}` : '';
            return `• ${escapeMarkdown(c.call_sid || 'unknown')} (${escapeMarkdown(status)})\n📞 ${escapeMarkdown(maskPhoneForDisplay(c.phone_number))}\n🕒 ${escapeMarkdown(when)}${summary}`;
        });
        await ctx.reply(lines.join('\n\n'), {
            parse_mode: 'Markdown',
            reply_markup: buildMainMenuKeyboard(ctx)
        });
    } catch (error) {
        if (isSessionCancellationError(error)) {
            return;
        }
        await ctx.reply(httpClient.getUserMessage(error, 'Search failed. Please try again later.'), {
            reply_markup: buildMainMenuKeyboard(ctx)
        });
    }
}

async function calllogDetailsFlow(conversation, ctx) {
    const opId = startOperation(ctx, 'calllog-details');
    const ensureActive = () => ensureOperationActive(ctx, opId);
    try {
        const user = await new Promise((resolve) => getUser(ctx.from.id, resolve));
        ensureActive();
        if (!user) {
            await ctx.reply('❌ You are not authorized to use this bot.');
            return;
        }
        await ctx.reply('📄 Enter the call SID to view details.');
        const { text: callSid } = await waitForConversationText(conversation, ctx, {
            ensureActive,
            invalidMessage: '⚠️ Please send the call SID as text.'
        });
        if (!callSid) {
            await ctx.reply('❌ Call SID is required.');
            return;
        }

        const res = await httpClient.get(null, `${config.apiUrl}/api/calls/${encodeURIComponent(callSid)}`, {
            timeout: 12000
        });
        const call = res.data?.call || res.data;
        if (!call) {
            await ctx.reply('❌ Call not found.', {
                reply_markup: buildMainMenuKeyboard(ctx)
            });
            return;
        }

        const duration = call.duration ? `${Math.floor(call.duration / 60)}:${String(call.duration % 60).padStart(2, '0')}` : 'N/A';
        const lines = [
            buildLine('🆔', 'Call', escapeMarkdown(call.call_sid || callSid)),
            buildLine('📞', 'Phone', escapeMarkdown(maskPhoneForDisplay(call.phone_number))),
            buildLine('📊', 'Status', escapeMarkdown(call.status || 'unknown')),
            buildLine('⏱️', 'Duration', escapeMarkdown(duration)),
            buildLine('🕒', 'Started', escapeMarkdown(call.created_at ? new Date(call.created_at).toLocaleString() : 'N/A'))
        ];
        if (call.call_summary) {
            lines.push(`📝 ${escapeMarkdown(summarizeSensitiveText(call.call_summary))}`);
        }
        await ctx.reply(section('📄 Call Details', lines), {
            parse_mode: 'Markdown',
            reply_markup: buildMainMenuKeyboard(ctx)
        });
    } catch (error) {
        if (isSessionCancellationError(error)) {
            return;
        }
        await ctx.reply(httpClient.getUserMessage(error, 'Failed to fetch call details.'), {
            reply_markup: buildMainMenuKeyboard(ctx)
        });
    }
}

async function calllogEventsFlow(conversation, ctx) {
    const opId = startOperation(ctx, 'calllog-events');
    const ensureActive = () => ensureOperationActive(ctx, opId);
    try {
        const user = await new Promise((resolve) => getUser(ctx.from.id, resolve));
        ensureActive();
        if (!user) {
            await ctx.reply('❌ You are not authorized to use this bot.');
            return;
        }
        await ctx.reply('🧾 Enter the call SID to view recent events.');
        const { text: callSid } = await waitForConversationText(conversation, ctx, {
            ensureActive,
            invalidMessage: '⚠️ Please send the call SID as text.'
        });
        if (!callSid) {
            await ctx.reply('❌ Call SID is required.');
            return;
        }

        const res = await httpClient.get(null, `${config.apiUrl}/api/calls/${encodeURIComponent(callSid)}/status`, {
            timeout: 12000
        });
        const states = res.data?.recent_states || [];
        if (!states.length) {
            await ctx.reply('ℹ️ No recent events found.', {
                reply_markup: buildMainMenuKeyboard(ctx)
            });
            return;
        }

        const lines = states.slice(0, 8).map((state) => {
            const when = state.timestamp ? new Date(state.timestamp).toLocaleString() : 'unknown time';
            return `• ${escapeMarkdown(state.state || 'event')} — ${escapeMarkdown(when)}`;
        });
        await ctx.reply(section('🧾 Recent Events', lines), {
            parse_mode: 'Markdown',
            reply_markup: buildMainMenuKeyboard(ctx)
        });
    } catch (error) {
        if (isSessionCancellationError(error)) {
            return;
        }
        await ctx.reply(httpClient.getUserMessage(error, 'Failed to fetch recent events.'), {
            reply_markup: buildMainMenuKeyboard(ctx)
        });
    }
}

function registerCalllogCommand(bot) {
    bot.command('calllog', async (ctx) => {
        try {
            await renderCalllogMenu(ctx);
        } catch (error) {
            await ctx.reply(httpClient.getUserMessage(error, 'Could not open call log.'));
        }
    });
}

module.exports = {
    renderCalllogMenu,
    calllogRecentFlow,
    calllogSearchFlow,
    calllogDetailsFlow,
    calllogEventsFlow,
    registerCalllogCommand
};
