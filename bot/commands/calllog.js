const config = require('../config');
const httpClient = require('../utils/httpClient');
const { InlineKeyboard } = require('grammy');
const { getUser } = require('../db/db');
const { startOperation, ensureOperationActive, registerAbortController } = require('../utils/sessionState');
const {
    renderMenu,
    escapeMarkdown,
    buildLine,
    section,
    sendEphemeral,
    buildBackToMenuKeyboard,
    cancelledMessage,
    setupStepMessage
} = require('../utils/ui');
const { buildCallbackData } = require('../utils/actions');
const { getAccessProfile } = require('../utils/capabilities');

const CANCEL_KEYWORDS = new Set(['cancel', 'exit', 'quit']);

function isCancelInput(value) {
    return CANCEL_KEYWORDS.has(String(value || '').trim().toLowerCase());
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

async function fetchRecentCalls(ctx, ensureActive, { limit = 10, filter } = {}) {
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
            const res = await guardedGet(ctx, ensureActive, candidate.url, {
                params: candidate.params
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

async function guardedGet(ctx, ensureActive, url, options = {}) {
    const controller = new AbortController();
    const release = registerAbortController(ctx, controller);
    try {
        const response = await httpClient.get(null, url, {
            timeout: 12000,
            signal: controller.signal,
            ...options
        });
        if (typeof ensureActive === 'function') {
            ensureActive();
        }
        return response;
    } finally {
        release();
    }
}

function buildCalllogMenuKeyboard(ctx) {
    return new InlineKeyboard()
        .text('🕒 Recent Calls', buildCallbackData(ctx, 'CALLLOG_RECENT'))
        .text('🔍 Search', buildCallbackData(ctx, 'CALLLOG_SEARCH'))
        .row()
        .text('📄 Call Details', buildCallbackData(ctx, 'CALLLOG_DETAILS'))
        .text('🧾 Recent Events', buildCallbackData(ctx, 'CALLLOG_EVENTS'))
        .row()
        .text('⬅️ Main Menu', buildCallbackData(ctx, 'MENU'));
}

function buildCalllogLimitedKeyboard(ctx) {
    const adminUsername = (config.admin.username || '').replace(/^@/, '');
    const keyboard = new InlineKeyboard()
        .text('ℹ️ Help', buildCallbackData(ctx, 'HELP'))
        .text('⬅️ Main Menu', buildCallbackData(ctx, 'MENU'));
    if (adminUsername) {
        keyboard.row().url('📱 Request Access', `https://t.me/${adminUsername}`);
    }
    return keyboard;
}

function buildCalllogResultKeyboard(ctx) {
    return buildBackToMenuKeyboard(ctx, {
        backAction: 'CALLLOG',
        backLabel: '⬅️ Back to Call Log'
    });
}

async function replyCalllogUnauthorized(ctx) {
    await ctx.reply('❌ Access denied. Your account is not authorized for this action.', {
        reply_markup: buildCalllogResultKeyboard(ctx)
    });
}

async function renderCalllogMenu(ctx) {
    const access = await getAccessProfile(ctx);
    startOperation(ctx, 'calllog-menu');
    const keyboard = access.user
        ? buildCalllogMenuKeyboard(ctx)
        : buildCalllogLimitedKeyboard(ctx);
    const title = access.user ? '📜 *Call Log*' : '🔒 *Call Log (Access limited)*';
    const lines = [
        access.user ? 'Choose an action to explore call history.' : 'Call log actions require approved account access.',
        access.user ? 'Search by phone, call ID, status, or date.' : 'Use the Request Access button to unlock call history tools.',
        access.user ? 'Authorized access enabled.' : 'Limited access mode is active.'
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
            await replyCalllogUnauthorized(ctx);
            return;
        }

        await ctx.reply(setupStepMessage('Call Log - Recent Calls', [
            'Enter limit (max 30) with an optional filter (status or phone).',
            'Example: `15 completed` or `20 +1234567890`.',
            'Type `cancel` to stop.'
        ]), {
            parse_mode: 'Markdown'
        });
        const update = await conversation.wait();
        ensureActive();
        const raw = update?.message?.text?.trim() || '';
        if (isCancelInput(raw)) {
            await ctx.reply(cancelledMessage('Call log lookup', 'Use /calllog to run another query.'), {
                parse_mode: 'Markdown',
                reply_markup: buildCalllogResultKeyboard(ctx)
            });
            return;
        }
        const parts = raw.split(/\s+/).filter(Boolean);
        const parsedLimit = parseInt(parts[0], 10);
        const limit = Number.isFinite(parsedLimit)
            ? Math.max(1, Math.min(parsedLimit, 30))
            : 10;
        const filter = parts.slice(1).join(' ');

        const { calls, filtered } = await fetchRecentCalls(ctx, ensureActive, { limit, filter });
        if (!calls.length) {
            await ctx.reply('ℹ️ No recent calls found.', {
                reply_markup: buildCalllogResultKeyboard(ctx)
            });
            return;
        }

        const lines = calls.map((call) => {
            const status = call.status || 'unknown';
            const when = new Date(call.created_at).toLocaleString();
            const duration = call.duration ? `${Math.floor(call.duration / 60)}:${String(call.duration % 60).padStart(2, '0')}` : 'N/A';
            return [
                `• ${escapeMarkdown(call.call_sid || 'unknown')} (${escapeMarkdown(status)})`,
                `📞 ${escapeMarkdown(call.phone_number || 'N/A')}`,
                `⏱️ ${duration} | 🕒 ${escapeMarkdown(when)}`
            ].join('\n');
        });

        const header = filter && !filtered
            ? 'ℹ️ Filter unavailable on this API; showing latest calls.\n\n'
            : '';
        await ctx.reply(`${header}${lines.join('\n\n')}`, {
            parse_mode: 'Markdown',
            reply_markup: buildCalllogResultKeyboard(ctx)
        });
    } catch (error) {
        await ctx.reply(httpClient.getUserMessage(error, 'Failed to fetch recent calls.'), {
            reply_markup: buildCalllogResultKeyboard(ctx)
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
            await replyCalllogUnauthorized(ctx);
            return;
        }
        await ctx.reply(setupStepMessage('Call Log - Search', [
            'Enter a search term (phone number, call SID, or status).',
            'Type `cancel` to stop.'
        ]), {
            parse_mode: 'Markdown'
        });
        const update = await conversation.wait();
        ensureActive();
        const query = update?.message?.text?.trim();
        if (isCancelInput(query)) {
            await ctx.reply(cancelledMessage('Call log search', 'Use /calllog to run another search.'), {
                parse_mode: 'Markdown',
                reply_markup: buildCalllogResultKeyboard(ctx)
            });
            return;
        }
        if (!query || query.length < 2) {
            await ctx.reply('❌ Please provide at least 2 characters.');
            return;
        }

        await sendEphemeral(ctx, '🔍 Searching call log…');
        const res = await guardedGet(ctx, ensureActive, `${config.apiUrl}/api/calls/search`, {
            params: { q: query, limit: 10 },
        });
        const results = res.data?.results || [];
        if (!results.length) {
            await ctx.reply('ℹ️ No matches found.', {
                reply_markup: buildCalllogResultKeyboard(ctx)
            });
            return;
        }

        const lines = results.slice(0, 5).map((c) => {
            const status = c.status || 'unknown';
            const when = new Date(c.created_at).toLocaleString();
            const summary = c.call_summary ? `\n📝 ${escapeMarkdown(c.call_summary.slice(0, 120))}${c.call_summary.length > 120 ? '…' : ''}` : '';
            return `• ${escapeMarkdown(c.call_sid || 'unknown')} (${escapeMarkdown(status)})\n📞 ${escapeMarkdown(c.phone_number || 'N/A')}\n🕒 ${escapeMarkdown(when)}${summary}`;
        });
        await ctx.reply(lines.join('\n\n'), {
            parse_mode: 'Markdown',
            reply_markup: buildCalllogResultKeyboard(ctx)
        });
    } catch (error) {
        await ctx.reply(httpClient.getUserMessage(error, 'Search failed. Please try again later.'), {
            reply_markup: buildCalllogResultKeyboard(ctx)
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
            await replyCalllogUnauthorized(ctx);
            return;
        }
        await ctx.reply(setupStepMessage('Call Log - Call Details', [
            'Enter the call SID to view detailed status and summary.',
            'Type `cancel` to stop.'
        ]), {
            parse_mode: 'Markdown'
        });
        const update = await conversation.wait();
        ensureActive();
        const callSid = update?.message?.text?.trim();
        if (isCancelInput(callSid)) {
            await ctx.reply(cancelledMessage('Call details lookup', 'Use /calllog to view another call.'), {
                parse_mode: 'Markdown',
                reply_markup: buildCalllogResultKeyboard(ctx)
            });
            return;
        }
        if (!callSid) {
            await ctx.reply('❌ Call SID is required.');
            return;
        }

        const res = await guardedGet(ctx, ensureActive, `${config.apiUrl}/api/calls/${encodeURIComponent(callSid)}`);
        const call = res.data?.call || res.data;
        if (!call) {
            await ctx.reply('❌ Call not found.', {
                reply_markup: buildCalllogResultKeyboard(ctx)
            });
            return;
        }

        const duration = call.duration ? `${Math.floor(call.duration / 60)}:${String(call.duration % 60).padStart(2, '0')}` : 'N/A';
        const lines = [
            buildLine('🆔', 'Call', escapeMarkdown(call.call_sid || callSid)),
            buildLine('📞', 'Phone', escapeMarkdown(call.phone_number || 'N/A')),
            buildLine('📊', 'Status', escapeMarkdown(call.status || 'unknown')),
            buildLine('⏱️', 'Duration', escapeMarkdown(duration)),
            buildLine('🕒', 'Started', escapeMarkdown(call.created_at ? new Date(call.created_at).toLocaleString() : 'N/A'))
        ];
        if (call.call_summary) {
            lines.push(`📝 ${escapeMarkdown(call.call_summary.slice(0, 300))}${call.call_summary.length > 300 ? '…' : ''}`);
        }
        await ctx.reply(section('📄 Call Details', lines), {
            parse_mode: 'Markdown',
            reply_markup: buildCalllogResultKeyboard(ctx)
        });
    } catch (error) {
        await ctx.reply(httpClient.getUserMessage(error, 'Failed to fetch call details.'), {
            reply_markup: buildCalllogResultKeyboard(ctx)
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
            await replyCalllogUnauthorized(ctx);
            return;
        }
        await ctx.reply(setupStepMessage('Call Log - Recent Events', [
            'Enter the call SID to view the most recent call events.',
            'Type `cancel` to stop.'
        ]), {
            parse_mode: 'Markdown'
        });
        const update = await conversation.wait();
        ensureActive();
        const callSid = update?.message?.text?.trim();
        if (isCancelInput(callSid)) {
            await ctx.reply(cancelledMessage('Call events lookup', 'Use /calllog to review another call event stream.'), {
                parse_mode: 'Markdown',
                reply_markup: buildCalllogResultKeyboard(ctx)
            });
            return;
        }
        if (!callSid) {
            await ctx.reply('❌ Call SID is required.');
            return;
        }

        const res = await guardedGet(ctx, ensureActive, `${config.apiUrl}/api/calls/${encodeURIComponent(callSid)}/status`);
        const states = res.data?.recent_states || [];
        if (!states.length) {
            await ctx.reply('ℹ️ No recent events found.', {
                reply_markup: buildCalllogResultKeyboard(ctx)
            });
            return;
        }

        const lines = states.slice(0, 8).map((state) => {
            const when = state.timestamp ? new Date(state.timestamp).toLocaleString() : 'unknown time';
            return `• ${escapeMarkdown(state.state || 'event')} — ${escapeMarkdown(when)}`;
        });
        await ctx.reply(section('🧾 Recent Events', lines), {
            parse_mode: 'Markdown',
            reply_markup: buildCalllogResultKeyboard(ctx)
        });
    } catch (error) {
        await ctx.reply(httpClient.getUserMessage(error, 'Failed to fetch recent events.'), {
            reply_markup: buildCalllogResultKeyboard(ctx)
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
