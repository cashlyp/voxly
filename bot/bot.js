let grammyPkg;
try {
    grammyPkg = require('grammy');
} catch (error) {
    console.error('❌ Missing dependency "grammy". Run `npm ci --omit=dev` in /bot before starting PM2.');
    throw error;
}
const { Bot, session, InlineKeyboard } = grammyPkg;

let conversationsPkg;
try {
    conversationsPkg = require('@grammyjs/conversations');
} catch (error) {
    console.error('❌ Missing dependency "@grammyjs/conversations". Run `npm ci --omit=dev` in /bot before starting PM2.');
    throw error;
}
const { conversations, createConversation } = conversationsPkg;
const axios = require('axios');
const config = require('./config');
const { attachHmacAuth } = require('./utils/apiAuth');

const apiOrigins = new Set();
try {
    apiOrigins.add(new URL(config.apiUrl).origin);
} catch (_) {}
try {
    apiOrigins.add(new URL(config.templatesApiUrl).origin);
} catch (_) {}

attachHmacAuth(axios, {
    secret: config.apiAuth?.hmacSecret,
    allowedOrigins: apiOrigins,
    defaultBaseUrl: config.apiUrl
});
const {
    initialSessionState,
    ensureSession,
    cancelActiveFlow,
    startOperation,
    resetSession,
    OperationCancelledError
} = require('./utils/sessionState');

// Bot initialization
const token = config.botToken;
const bot = new Bot(token);

// Initialize conversations with error handling wrapper
function wrapConversation(handler, name) {
    return createConversation(async (conversation, ctx) => {
        try {
            await handler(conversation, ctx);
        } catch (error) {
            if (error instanceof OperationCancelledError) {
                console.log(`Conversation ${name} cancelled: ${error.message}`);
                return;
            }
            console.error(`Conversation error in ${name}:`, error);
            await ctx.reply('❌ An error occurred during the conversation. Please try again.');
        }
    }, name);
}

// IMPORTANT: Add session middleware BEFORE conversations
bot.use(session({ initial: initialSessionState }));

// Ensure every update touches a session object
bot.use(async (ctx, next) => {
    ensureSession(ctx);
    return next();
});

// When a new slash command arrives, cancel any active flow first
bot.use(async (ctx, next) => {
    const text = ctx.message?.text || ctx.callbackQuery?.data;
    if (text && text.startsWith('/')) {
        const command = text.split(' ')[0].toLowerCase();
        if (command !== '/cancel') {
            await cancelActiveFlow(ctx, `command:${command}`);
        }
        ctx.session.lastCommand = command;
        ctx.session.currentOp = null;
    }
    return next();
});

// Operator/alert inline actions
bot.callbackQuery(/^alert:/, async (ctx) => {
    const data = ctx.callbackQuery.data || '';
    const parts = data.split(':');
    if (parts.length < 3) return;
    const action = parts[1];
    const callSid = parts[2];

    try {
        switch (action) {
            case 'mute':
                await axios.post(`${API_BASE}/api/calls/${callSid}/operator`, { action: 'mute_alerts' }, { timeout: 8000 });
                await ctx.answerCallbackQuery({ text: '🔕 Alerts muted for this call', show_alert: false });
                break;
            case 'retry':
                await axios.post(`${API_BASE}/api/calls/${callSid}/operator`, { action: 'clarify', text: 'Let me retry that step.' }, { timeout: 8000 });
                await ctx.answerCallbackQuery({ text: '🔄 Retry requested', show_alert: false });
                break;
            case 'transfer':
                await axios.post(`${API_BASE}/api/calls/${callSid}/operator`, { action: 'transfer' }, { timeout: 8000 });
                await ctx.answerCallbackQuery({ text: '📞 Transfer request noted', show_alert: false });
                break;
            default:
                await ctx.answerCallbackQuery({ text: 'Action not supported yet', show_alert: false });
                break;
        }
    } catch (error) {
        console.error('Operator action error:', error?.message || error);
        await ctx.answerCallbackQuery({ text: '⚠️ Failed to execute action', show_alert: false });
    }
});

// Live call console actions (proxy to API webhook handler)
bot.callbackQuery(/^lc:/, async (ctx) => {
    try {
        await ctx.answerCallbackQuery();
        await axios.post(`${config.apiUrl}/webhook/telegram`, ctx.update, { timeout: 8000 });
        return;
    } catch (error) {
        console.error('Live call action proxy error:', error?.message || error);
        await ctx.answerCallbackQuery({ text: '⚠️ Failed to process action', show_alert: false });
    }
});

// Initialize conversations middleware AFTER session
bot.use(conversations());

// Global error handler
bot.catch((err) => {
    const errorMessage = `Error while handling update ${err.ctx.update.update_id}:
    ${err.error.message}
    Stack: ${err.error.stack}`;
    console.error(errorMessage);
    
    try {
        err.ctx.reply('❌ An error occurred. Please try again or contact support.');
    } catch (replyError) {
        console.error('Failed to send error message:', replyError);
    }
});

async function validateTemplatesApiConnectivity() {
    const healthUrl = new URL('/health', config.templatesApiUrl).toString();
    try {
        const response = await axios.get(healthUrl, { timeout: 5000 });
        const contentType = response.headers?.['content-type'] || '';
        if (!contentType.includes('application/json')) {
            throw new Error(`healthcheck returned ${contentType || 'unknown'} content`);
        }
        if (response.data?.status && response.data.status !== 'healthy') {
            throw new Error(`service reported status "${response.data.status}"`);
        }
        console.log(`✅ Templates API reachable (${healthUrl})`);
    } catch (error) {
        let reason;
        if (error.response) {
            const status = error.response.status;
            const statusText = error.response.statusText || '';
            reason = `HTTP ${status} ${statusText}`;
        } else if (error.request) {
            reason = 'no response received';
        } else {
            reason = error.message;
        }
        throw new Error(`Unable to reach Templates API at ${healthUrl}: ${reason}`);
    }
}

// Import dependencies
const { getUser, isAdmin, expireInactiveUsers } = require('./db/db');
const { callFlow, registerCallCommand } = require('./commands/call');
const { smsFlow, bulkSmsFlow, scheduleSmsFlow, registerSmsCommands, getSmsStats } = require('./commands/sms');
const { templatesFlow, registerTemplatesCommand } = require('./commands/templates');
const { personaFlow, registerPersonaCommand } = require('./commands/persona');
const {
    registerProviderCommand,
    fetchProviderStatus,
    formatProviderStatus,
    updateProvider,
    SUPPORTED_PROVIDERS,
} = require('./commands/provider');
const {
    addUserFlow,
    registerAddUserCommand,
    promoteFlow,
    registerPromoteCommand,
    removeUserFlow,
    registerRemoveUserCommand,
    registerUserListCommand
} = require('./commands/users');

// Register conversations with error handling
bot.use(wrapConversation(callFlow, "call-conversation"));
bot.use(wrapConversation(addUserFlow, "adduser-conversation"));
bot.use(wrapConversation(promoteFlow, "promote-conversation"));
bot.use(wrapConversation(removeUserFlow, "remove-conversation"));
bot.use(wrapConversation(scheduleSmsFlow, "schedule-sms-conversation"));
bot.use(wrapConversation(smsFlow, "sms-conversation"));
bot.use(wrapConversation(bulkSmsFlow, "bulk-sms-conversation"));
bot.use(wrapConversation(templatesFlow, "templates-conversation"));
bot.use(wrapConversation(personaFlow, "persona-conversation"));

// Register command handlers
registerCallCommand(bot);
registerAddUserCommand(bot);
registerPromoteCommand(bot);
registerRemoveUserCommand(bot);
registerSmsCommands(bot);
registerTemplatesCommand(bot);
registerUserListCommand(bot);
registerPersonaCommand(bot);


// Register non-conversation commands
require('./commands/help')(bot);
require('./commands/menu')(bot);
require('./commands/guide')(bot);
require('./commands/api')(bot);
registerProviderCommand(bot);
const API_BASE = config.apiUrl;

function escapeMarkdown(text = '') {
    return text.replace(/([_*[\]`])/g, '\\$1');
}

function escapeHtml(text = '') {
    return String(text)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;');
}

function formatDuration(seconds = 0) {
    if (!seconds || seconds < 1) return 'N/A';
    const minutes = Math.floor(seconds / 60);
    const remaining = seconds % 60;
    return `${minutes}:${String(remaining).padStart(2, '0')}`;
}

function normalizeCallStatus(value) {
    return String(value || '').toLowerCase().replace(/_/g, '-');
}

function formatContactLabel(callData) {
    if (callData?.customer_name) return callData.customer_name;
    const digits = String(callData?.phone_number || '').replace(/\D/g, '');
    if (digits.length >= 4) {
        return `the contact ending ${digits.slice(-4)}`;
    }
    return 'the contact';
}

function buildOutcomeSummary(callData, status) {
    const label = formatContactLabel(callData);
    switch (status) {
        case 'no-answer':
            return `${label} didn't pick up the call.`;
        case 'busy':
            return `${label}'s line was busy.`;
        case 'failed':
            return `Call failed to reach ${label}.`;
        case 'canceled':
            return `Call to ${label} was canceled.`;
        default:
            return 'Call finished.';
    }
}

function splitMessageIntoChunks(message = '', limit = 3500) {
    const lines = String(message || '').split('\n');
    const chunks = [];
    let buffer = '';
    for (const line of lines) {
        const next = buffer ? `${buffer}\n${line}` : line;
        if (next.length > limit && buffer) {
            chunks.push(buffer);
            buffer = line;
        } else {
            buffer = next;
        }
    }
    if (buffer) {
        chunks.push(buffer);
    }
    return chunks;
}

async function sendFullTranscriptFromApi(ctx, callSid) {
    if (!callSid) {
        await ctx.reply('❌ Missing call identifier for transcript.');
        return;
    }

    let callData;
    let transcripts = [];

    try {
        const response = await axios.get(`${config.apiUrl}/api/calls/${callSid}`, { timeout: 15000 });
        callData = response.data?.call || response.data;
        transcripts = response.data?.transcripts || [];
    } catch (error) {
        console.error('Full transcript fetch error:', error?.message || error);
        await ctx.reply('❌ Unable to retrieve full transcript. Please try again later.');
        return;
    }

    if (!callData || !transcripts || transcripts.length === 0) {
        await ctx.reply('📋 No transcript available for this call yet.');
        return;
    }

    const duration = callData.duration ? formatDuration(callData.duration) : 'N/A';
    const startTime = callData.started_at ? new Date(callData.started_at).toLocaleString() : 'Unknown';
    const digitSummary = callData.digit_summary ? escapeHtml(callData.digit_summary) : '';

    let message = `📄 <b>Full Transcript</b>\n\n`;
    message += `📞 <b>Phone:</b> ${escapeHtml(callData.phone_number || 'Unknown')}\n`;
    message += `⏱️ <b>Duration:</b> ${escapeHtml(duration)}\n`;
    message += `🕐 <b>Time:</b> ${escapeHtml(startTime)}\n`;
    message += `💬 <b>Messages:</b> ${escapeHtml(String(transcripts.length))}\n`;
    if (digitSummary) {
        message += `🔢 <b>Digits:</b> ${digitSummary}\n`;
    }
    message += `\n<b>Conversation:</b>\n`;
    message += `${'─'.repeat(25)}\n`;

    for (const entry of transcripts) {
        const speaker = entry.speaker === 'user' ? '🧑 User' : '🤖 AI';
        const cleanMessage = escapeHtml(entry.message || '');
        message += `<b>${speaker}:</b> ${cleanMessage}\n\n`;
    }

    const chunks = splitMessageIntoChunks(message, 3500);
    for (let i = 0; i < chunks.length; i += 1) {
        await ctx.reply(chunks[i], { parse_mode: 'HTML' });
    }
}

async function handleCallFollowUp(ctx, callSid, followAction) {
    if (!callSid) {
        await ctx.reply('❌ Missing call identifier for follow-up.');
        return;
    }

    if (ctx.callbackQuery?.message) {
        try {
            await ctx.editMessageReplyMarkup();
        } catch (error) {
            console.warn('Unable to clear follow-up keyboard:', error.message);
        }
    }

    const action = followAction || 'recap';

    let callData;
    let transcripts = [];

    try {
        const response = await axios.get(`${config.apiUrl}/api/calls/${callSid}`, {
            timeout: 15000
        });

        callData = response.data?.call || response.data;
        transcripts = response.data?.transcripts || [];
    } catch (error) {
        console.error('Follow-up call fetch error:', error?.message || error);
        await ctx.reply('❌ Unable to retrieve call details. Please try again later.');
        return;
    }

    if (!callData) {
        await ctx.reply('❌ Call not found. It may have been archived.');
        return;
    }

    switch (action) {
        case 'recap': {
            const rawSummary = callData.call_summary || 'No recap is available yet for this call.';
            const summary = rawSummary.length > 1200 ? `${rawSummary.slice(0, 1200)}…` : rawSummary;
            const status = callData.status || 'unknown';
            const duration = formatDuration(callData.duration);

            const message =
                `📝 *Call Recap*\n\n` +
                `📞 ${escapeMarkdown(callData.phone_number || 'Unknown')}\n` +
                `📊 Status: ${escapeMarkdown(status)}\n` +
                `⏱️ Duration: ${escapeMarkdown(duration)}\n\n` +
                `${escapeMarkdown(summary)}`;

            await ctx.reply(message, { parse_mode: 'Markdown' });
            break;
        }

        case 'schedule': {
            if (!callData.phone_number) {
                await ctx.reply('❌ Cannot schedule follow-up: original phone number missing.');
                return;
            }
            ctx.session.meta = ctx.session.meta || {};
            ctx.session.meta.prefill = {
                phoneNumber: callData.phone_number,
                customerName: callData.customer_name || callData.client_name || callData.metadata?.customer_name || null,
                followUp: 'sms',
                callSid
            };
            await ctx.reply('⏰ Starting follow-up SMS scheduling flow...');
            try {
                await ctx.conversation.enter('schedule-sms-conversation');
            } catch (error) {
                console.error('Follow-up schedule flow error:', error);
                await ctx.reply('❌ Unable to start scheduling flow. You can use /schedulesms manually.');
            }
            break;
        }

        case 'reassign': {
            if (!callData.phone_number) {
                await ctx.reply('❌ Cannot reassign: original phone number missing.');
                return;
            }
            ctx.session.meta = ctx.session.meta || {};
            ctx.session.meta.prefill = {
                phoneNumber: callData.phone_number,
                customerName: callData.customer_name || callData.client_name || callData.metadata?.customer_name || null,
                followUp: 'call',
                callSid
            };
            await ctx.reply('👤 Reassigning to a new agent. Starting call setup...');
            try {
                await ctx.conversation.enter('call-conversation');
            } catch (error) {
                console.error('Follow-up call flow error:', error);
                await ctx.reply('❌ Unable to start call flow. You can use /call to retry manually.');
            }
            break;
        }

        case 'transcript': {
            await sendFullTranscriptFromApi(ctx, callSid);
            break;
        }
        case 'callagain': {
            if (!callData.phone_number) {
                await ctx.reply('❌ Cannot place the follow-up call because the phone number is missing.');
                return;
            }
            ctx.session.meta = ctx.session.meta || {};
            ctx.session.meta.prefill = {
                phoneNumber: callData.phone_number,
                customerName: callData.customer_name || callData.client_name || callData.metadata?.customer_name || null,
                followUp: 'call',
                callSid,
                quickAction: 'callagain'
            };
            await ctx.reply('☎️ Calling the customer again with the same configuration...');
            try {
                await ctx.conversation.enter('call-conversation');
            } catch (error) {
                console.error('Follow-up call-again flow error:', error);
                await ctx.reply('❌ Unable to start the call flow. You can use /call to retry manually.');
            }
            break;
        }
        case 'skip': {
            await ctx.reply('👍 Noted. Skipping the follow-up for now—you can revisit this call anytime from /calls.');
            break;
        }
        case 'resend': {
            if (!callData.phone_number) {
                await ctx.reply('❌ Cannot resend the code: original phone number missing.');
                return;
            }
            ctx.session.meta = ctx.session.meta || {};
            ctx.session.meta.prefill = {
                phoneNumber: callData.phone_number,
                followUp: 'sms',
                callSid,
                quickAction: 'resend_code'
            };
            await ctx.reply('🔁 Sending a fresh verification code via SMS...');
            try {
                await ctx.conversation.enter('sms-conversation');
            } catch (error) {
                console.error('Resend code flow error:', error);
                await ctx.reply('❌ Unable to start SMS flow. You can use /sms to send the code manually.');
            }
            break;
        }

        default:
            await ctx.reply('ℹ️ Quick action not recognised or not yet implemented.');
            break;
    }
}

async function handleSmsFollowUp(ctx, phone, followAction) {
    if (!phone) {
        await ctx.reply('❌ Missing phone number for follow-up.');
        return;
    }

    if (ctx.callbackQuery?.message) {
        try {
            await ctx.editMessageReplyMarkup();
        } catch (error) {
            console.warn('Unable to clear SMS follow-up keyboard:', error.message);
        }
    }

    const normalizedPhone = phone.startsWith('+') ? phone : `+${phone}`;
    const action = followAction || 'new';

    ctx.session.meta = ctx.session.meta || {};

    switch (action) {
        case 'new': {
            ctx.session.meta.prefill = {
                phoneNumber: normalizedPhone,
                followUp: 'sms'
            };
            await ctx.reply('💬 Continuing the conversation via SMS...');
            try {
                await ctx.conversation.enter('sms-conversation');
            } catch (error) {
                console.error('Follow-up SMS flow error:', error);
                await ctx.reply('❌ Unable to start SMS flow. You can use /sms to continue manually.');
            }
            break;
        }

        case 'schedule': {
            ctx.session.meta.prefill = {
                phoneNumber: normalizedPhone,
                followUp: 'sms'
            };
            await ctx.reply('⏰ Scheduling a follow-up SMS...');
            try {
                await ctx.conversation.enter('schedule-sms-conversation');
            } catch (error) {
                console.error('Follow-up schedule SMS flow error:', error);
                await ctx.reply('❌ Unable to start schedule flow. You can use /schedulesms manually.');
            }
            break;
        }

        case 'call': {
            ctx.session.meta.prefill = {
                phoneNumber: normalizedPhone,
                followUp: 'call'
            };
            await ctx.reply('📞 Initiating a follow-up call setup...');
            try {
                await ctx.conversation.enter('call-conversation');
            } catch (error) {
                console.error('Follow-up call via SMS action error:', error);
                await ctx.reply('❌ Unable to start call flow. You can use /call to retry manually.');
            }
            break;
        }

        default:
            await ctx.reply('ℹ️ SMS quick action not recognised.');
            break;
    }
}


// Start command handler
bot.command('start', async (ctx) => {
    try {
        expireInactiveUsers();
        
        let user = await new Promise(r => getUser(ctx.from.id, r));
        if (!user) {
            const adminUsername = (config.admin.username || '').replace(/^@/, '');
            const kb = new InlineKeyboard()
                .url('📱 Contact Admin', `https://t.me/${adminUsername}`);
            
            return ctx.reply('*Access Restricted* ⚠️\n\n' +
                'This bot requires authorization.\n' +
                'Please contact an administrator to get access.\n\n' +
                'You can still use /help to learn how the bot works.', {
                parse_mode: 'Markdown',
                reply_markup: kb
            });
        }

        const isOwner = await new Promise(r => isAdmin(ctx.from.id, r));
        
        // Prepare user information
        const userStats = `👤 *User Information*
• ID: \`${ctx.from.id}\`
• Username: @${ctx.from.username || 'none'}
• Role: ${user.role}
• Joined: ${new Date(user.timestamp).toLocaleDateString()}`;

        const welcomeText = isOwner ? 
            '🛡️ *Welcome, Administrator!*\n\nYou have full access to all bot features.' :
            '👋 *Welcome to Voicednut Bot!*\n\nYou can make voice calls using AI agents.';

        const kb = new InlineKeyboard()
          .text('📞 New Call', 'CALL')
          .text('📚 Guide', 'GUIDE')
            .row()
            .text('💬 New Sms', 'SMS')
            .text('⏰ Schedule SMS', 'SCHEDULE_SMS')
            .row()
            .text('📜 SMS Status', 'SMS_STATUS_HELP')
            .text('🧾 SMS Threads', 'SMS_CONVO_HELP')
            .row()
            .text('🏥 Health', 'HEALTH')            
            .text('📋 Menu', 'MENU')
            .row()
            .text('❔ Help', 'HELP');

        if (isOwner) {
            kb.row()
                .text('➕ Add User', 'ADDUSER')
                .text('⬆️ Promote', 'PROMOTE')
            .row()
                .text('👥 Users', 'USERS')
                .text('❌ Remove', 'REMOVE')
            .row()
                .text('📤 Bulk SMS', 'BULK_SMS')
                .text('📊 SMS Stats', 'SMS_STATS')
            .row()
                .text('☎️ Provider', 'PROVIDER_STATUS')
                .text('🔍 Status', 'STATUS');
        }

        const message = `${welcomeText}\n\n${userStats}\n\nUse the buttons below or type /help for available commands.`;
        
        await ctx.reply(message, {
            parse_mode: 'Markdown',
            reply_markup: kb
        });
    } catch (error) {
        console.error('Start command error:', error);
        await ctx.reply('❌ An error occurred. Please try again or contact support.');
    }
});

// Enhanced callback query handler
bot.on('callback_query:data', async (ctx) => {
    try {
        const action = ctx.callbackQuery.data;
        if (action && action.startsWith('lc:')) {
            return;
        }
        // Answer callback query immediately to prevent timeout
        await ctx.answerCallbackQuery();
        console.log(`Callback query received: ${action} from user ${ctx.from.id}`);

        // Verify user authorization
        const user = await new Promise(r => getUser(ctx.from.id, r));
        if (!user) {
            await ctx.reply("❌ You are not authorized to use this bot.");
            return;
        }

        // Check admin permissions
        const isAdminUser = user.role === 'ADMIN';
        const adminActions = ['ADDUSER', 'PROMOTE', 'REMOVE', 'USERS', 'STATUS', 'TEST_API', 'TEMPLATES', 'SMS_STATS', 'RECENT_SMS', 'PROVIDER_STATUS', 'BULK_SMS'];
        const adminActionPrefixes = ['PROVIDER_SET:'];

        const requiresAdmin = adminActions.includes(action) || adminActionPrefixes.some((prefix) => action.startsWith(prefix));

        if (requiresAdmin && !isAdminUser) {
            await ctx.reply("❌ This action is for administrators only.");
            return;
        }

        if (action.startsWith('CALL_DETAILS:')) {
            const detailsKey = action.split(':')[1];
            const detailsMessage = ctx.session?.callDetailsCache?.[detailsKey];
            if (!detailsMessage) {
                await ctx.reply('ℹ️ Details are no longer available for this call.');
                return;
            }
            await ctx.reply(detailsMessage);
            return;
        }

        if (action.startsWith('tr:')) {
            const callSid = action.split(':')[1];
            await cancelActiveFlow(ctx, `callback:${action}`);
            resetSession(ctx);
            await sendFullTranscriptFromApi(ctx, callSid);
            return;
        }

        if (action.startsWith('rca:')) {
            const callSid = action.split(':')[1];
            await cancelActiveFlow(ctx, `callback:${action}`);
            resetSession(ctx);
            await ctx.reply('🎧 Recording link is not available yet. Sending full transcript instead...');
            await sendFullTranscriptFromApi(ctx, callSid);
            return;
        }

        if (action.startsWith('recap:')) {
            const [, recapAction, callSid] = action.split(':');
            await cancelActiveFlow(ctx, `callback:${action}`);
            resetSession(ctx);
            if (recapAction === 'skip') {
                await ctx.reply('👍 Skipping recap for now.');
                return;
            }
            if (recapAction === 'sms') {
                await ctx.reply('📩 Sending recap via SMS...');
                try {
                    const response = await axios.get(`${config.apiUrl}/api/calls/${callSid}`, { timeout: 15000 });
                    const callData = response.data?.call || response.data;
                    if (!callData?.phone_number) {
                        await ctx.reply('❌ Unable to send recap: phone number missing.');
                        return;
                    }
                    const normalizedStatus = normalizeCallStatus(callData.status || callData.twilio_status || 'completed');
                    const status = normalizedStatus.replace(/_/g, ' ');
                    const duration = callData.duration ? ` Duration: ${formatDuration(callData.duration)}.` : '';
                    const summaryRaw = (callData.call_summary || '').replace(/\s+/g, ' ').trim();
                    const summary = normalizedStatus === 'completed'
                        ? (summaryRaw ? summaryRaw.slice(0, 180) : 'Call finished.')
                        : buildOutcomeSummary(callData, normalizedStatus);
                    const name = callData.customer_name ? ` with ${callData.customer_name}` : '';
                    const message = `VoicedNut call recap${name}: ${summary} Status: ${status}.${duration}`;
                    await axios.post(`${config.apiUrl}/api/sms/send`, {
                        to: callData.phone_number,
                        message,
                        user_chat_id: ctx.from.id
                    }, { timeout: 15000 });
                    await ctx.reply('✅ Recap SMS sent.');
                } catch (error) {
                    console.error('Recap SMS error:', error?.message || error);
                    await ctx.reply('❌ Failed to send recap SMS. Please try again later.');
                }
                return;
            }
        }

        if (action.startsWith('FOLLOWUP_CALL:')) {
            await cancelActiveFlow(ctx, `callback:${action}`);
            resetSession(ctx);
            const [, callSid, followAction] = action.split(':');
            await handleCallFollowUp(ctx, callSid, followAction || 'recap');
            return;
        }

        if (action.startsWith('FOLLOWUP_SMS:')) {
            await cancelActiveFlow(ctx, `callback:${action}`);
            resetSession(ctx);
            const [, phone, followAction] = action.split(':');
            await handleSmsFollowUp(ctx, phone, followAction || 'new');
            return;
        }

        if (action.startsWith('PROVIDER_SET:')) {
            const [, provider] = action.split(':');
            await cancelActiveFlow(ctx, `callback:${action}`);
            resetSession(ctx);
            await executeProviderSwitchCommand(ctx, provider?.toLowerCase());
            return;
        }

        // Handle conversation actions
        const conversations = {
            'CALL': 'call-conversation',
            'ADDUSER': 'adduser-conversation',
            'PROMOTE': 'promote-conversation',
            'REMOVE': 'remove-conversation',
            'SMS': 'sms-conversation',
            'BULK_SMS': 'bulk-sms-conversation',
            'SCHEDULE_SMS': 'schedule-sms-conversation',
            'TEMPLATES': 'templates-conversation'
        };

        if (conversations[action]) {
            console.log(`Starting conversation: ${conversations[action]}`);
            await cancelActiveFlow(ctx, `callback:${action}`);
            startOperation(ctx, action.toLowerCase());
            await ctx.reply(`Starting ${action.toLowerCase()} process...`);
            await ctx.conversation.enter(conversations[action]);
            return;
        }

        // Handle direct command actions
        await cancelActiveFlow(ctx, `callback:${action}`);
        resetSession(ctx);

        switch (action) {
            case 'HELP':
                await executeHelpCommand(ctx);
                break;
                
            case 'USERS':
                if (isAdminUser) {
                    try {
                        await executeUsersCommand(ctx);
                    } catch (usersError) {
                        console.error('Users callback error:', usersError);
                        await ctx.reply('❌ Error displaying users list. Please try again.');
                    }
                }
                break;
                
            case 'GUIDE':
                await executeGuideCommand(ctx);
                break;
                
            case 'MENU':
                await cancelActiveFlow(ctx, 'callback:MENU');
                resetSession(ctx);
                await executeMenuCommand(ctx, isAdminUser);
                break;
                
            case 'HEALTH':
                await executeHealthCommand(ctx);
                break;
                
            case 'STATUS':
                if (isAdminUser) {
                    await executeStatusCommand(ctx);
                }
                break;

            case 'TEST_API':
                if (isAdminUser) {
                    await executeTestApiCommand(ctx);
                }
                break;

            case 'PROVIDER_STATUS':
                if (isAdminUser) {
                    await executeProviderStatusCommand(ctx);
                }
                break;

            case 'CALLS':
                await executeCallsCommand(ctx);
                break;

            case 'SMS':
                await ctx.reply(`Starting SMS process...`);
                await ctx.conversation.enter('sms-conversation');
                break;
                
            case 'BULK_SMS':
                if (isAdminUser) {
                    await ctx.reply(`Starting bulk SMS process...`);
                    await ctx.conversation.enter('bulk-sms-conversation');
                }
                break;
            
            case 'SCHEDULE_SMS':
                await ctx.reply(`Starting SMS scheduling...`);
                await ctx.conversation.enter('schedule-sms-conversation');
                break;
            
            case 'SMS_STATS':
                if (isAdminUser) {
                    await executeSmsStatsCommand(ctx);
                }
                break;

            case 'RECENT_SMS':
                if (isAdminUser) {
                    await executeRecentSmsCommand(ctx);
                }
                break;

            case 'SMS_STATUS_HELP':
                await ctx.reply('Use /smsstatus <message_sid> to check delivery status.\nExample: /smsstatus SM1234567890abcdef');
                break;

            case 'SMS_CONVO_HELP':
                if (isAdminUser) {
                    await ctx.reply('Use /smsconversation <phone_number> to view a thread.\nExample: /smsconversation +1234567890');
                }
                break;
                
            default:
                console.log(`Unknown callback action: ${action}`);
                await ctx.reply("❌ Unknown action. Please try again.");
        }

    } catch (error) {
        console.error('Callback query error:', error);
        await ctx.reply("❌ An error occurred processing your request. Please try again.");
    }
});

// Command execution functions for inline buttons
async function executeHelpCommand(ctx) {
    try {
        // Check if user is authorized
        const user = await new Promise(r => getUser(ctx.from.id, r));
        if (!user) {
            return ctx.reply('❌ You are not authorized to use this bot.');
        }
        const isOwner = await new Promise(r => isAdmin(ctx.from.id, r));
        
        // Build help text using HTML formatting (more reliable)
        let helpText = `📱 <b>Basic Commands</b>
• /start - Restart bot &amp; show main menu
• /call - Start a new voice call
• /sms - Send an SMS message
• /smsconversation &lt;phone&gt; - View SMS conversation
• /search &lt;term&gt; - Find calls by ID/phone/intent
• /recent [limit] - List recent calls (max 50)
• /health or /ping - Check bot &amp; API health
• /guide - Show detailed usage guide
• /menu - Show quick action buttons
• /help - Show this help message`;
        
        if (isOwner) {
            helpText += `
            
👑 <b>Admin Commands</b>
• /adduser - Add new authorized user
• /promote - Promote user to admin
• /removeuser - Remove user access
• /users - List all authorized users
• /bulksms - Send bulk SMS messages
• /schedulesms - Schedule SMS for later
• /provider - View or switch call provider
• /smsstats - View SMS statistics
• /templates - Manage call &amp; SMS templates
• /status - Full system status check
• /testapi - Test API connection`;
        }
        
        helpText += `
        
📖 <b>Quick Usage</b>
1. Use /call or click 📞 Call button
2. Enter phone number (E.164 format: +1234567890)
3. Define agent behavior/prompt
4. Set initial message to be spoken
5. Monitor call progress and receive notifications

💡 <b>Examples</b>
• Phone format: +1234567890 (not 123-456-7890)
• Search calls: /search refund
• List calls: /recent 20
• Check health: /health
        
🆘 <b>Support &amp; Info</b>
• Contact admin: @${config.admin.username}
• Bot version: 2.0.0
• For issues or questions, contact support`;
        
        const kb = new InlineKeyboard()
        .text('📞 New Call', 'CALL')
        .text('📋 Menu', 'MENU')
        .row()
        .text('📱 New SMS', 'SMS')
        .text('📚 Full Guide', 'GUIDE');
        
        if (isOwner) {
            kb.row()
            .text('👥 Users', 'USERS')
            .text('➕ Add User', 'ADDUSER')
            .row()
            .text('☎️ Provider', 'PROVIDER_STATUS');
        }
        
        await ctx.reply(helpText, {
            parse_mode: 'HTML',
            reply_markup: kb
        });
    } catch (error) {
        console.error('Help command error:', error);
        await ctx.reply('❌ Error displaying help. Please try again.');
    }
}

async function executeUsersCommand(ctx) {
    try {
        const { getUserList } = require('./db/db');
        
        const users = await new Promise((resolve, reject) => {
            getUserList((err, result) => {
                if (err) {
                    console.error('Database error in getUserList:', err);
                    reject(err);
                } else {
                    resolve(result);
                }
            });
        });

        if (!users || users.length === 0) {
            await ctx.reply('📋 No users found in the system.');
            return;
        }

        // Create user list without problematic markdown - use plain text
        let message = `📋 USERS LIST (${users.length}):\n\n`;
        
        users.forEach((user, index) => {
            const roleIcon = user.role === 'ADMIN' ? '🛡️' : '👤';
            const username = user.username || 'no_username';
            const joinDate = new Date(user.timestamp).toLocaleDateString();
            message += `${index + 1}. ${roleIcon} @${username}\n`;
            message += `   ID: ${user.telegram_id}\n`;
            message += `   Role: ${user.role}\n`;
            message += `   Joined: ${joinDate}\n\n`;
        });

        // Send without parse_mode to avoid markdown parsing errors
        await ctx.reply(message);

    } catch (error) {
        console.error('executeUsersCommand error:', error);
        await ctx.reply('❌ Error fetching users list. Please try again.');
    }
}

async function executeGuideCommand(ctx) {
    const mainGuide = `📚 *Voice Call Bot Guide*

*Making Calls:*
1️⃣ Start a call using /call or the Call button
2️⃣ Enter phone number in E.164 format (+1234567890)
3️⃣ Define the AI agent's behavior/personality
4️⃣ Set the first message to be spoken
5️⃣ Monitor the call progress

*Phone Number Format:*
• Must start with + symbol
• Include country code
• No spaces or special characters
• Example: +1234567890

*Best Practices:*
• Keep agent prompts clear and specific
• Test with short calls first
• Monitor initial responses
• End calls if needed

*Troubleshooting:*
• If call fails, check number format
• Ensure proper authorization
• Contact admin for persistent issues
• Use /status to check bot health

*Need Help?*
Contact: @${config.admin.username} for support.
Version: 2.0.0`;

    const kb = new InlineKeyboard()
        .text('📞 New Call', 'CALL')
        .text('📋 Commands', 'HELP')
        .row()
        .text('🔄 Main Menu', 'MENU')
        .text('New SMS', 'SMS');

    await ctx.reply(mainGuide, {
        parse_mode: 'Markdown',
        reply_markup: kb
    });
}

async function executeMenuCommand(ctx, isAdminUser) {
    const kb = new InlineKeyboard()
        .text('📞 New Call', 'CALL')
        .text('📱 Send SMS', 'SMS')
        .row()
        .text('⏰ Schedule SMS', 'SCHEDULE_SMS')
        .text('📜 SMS Status', 'SMS_STATUS_HELP')
        .row()
        .text('📋 Recent Calls', 'CALLS')
        .text('📚 Guide', 'GUIDE')
        .row()
        .text('🧾 SMS Threads', 'SMS_CONVO_HELP')
        .text('🏥 Health Check', 'HEALTH')
        .row()
        .text('ℹ️ Help', 'HELP');

    if (isAdminUser) {
        kb.row()
            .text('📤 Bulk SMS', 'BULK_SMS')
            .row()
            .text('➕ Add User', 'ADDUSER')
            .text('⬆️ Promote', 'PROMOTE')
            .row()
            .text('👥 Users', 'USERS')
            .text('❌ Remove', 'REMOVE')
            .row()
            .text('🧰 Templates', 'TEMPLATES')
            .text('📊 SMS Stats', 'SMS_STATS')
            .row()
            .text('📥 Recent SMS', 'RECENT_SMS')
            .text('☎️ Provider', 'PROVIDER_STATUS')
            .row()
            .text('🔍 Status', 'STATUS')
            .text('🧪 Test API', 'TEST_API');
    }

    const menuText = isAdminUser ? 
        '🛡️ *Administrator Menu*\n\nSelect an action below:' :
        '📋 *Quick Actions Menu*\n\nSelect an action below:';

    await ctx.reply(menuText, {
        parse_mode: 'Markdown',
        reply_markup: kb
    });
}


async function executeHealthCommand(ctx) {
    const axios = require('axios');
    
    try {
        const startTime = Date.now();
        const response = await axios.get(`${config.apiUrl}/health`, {
            timeout: 5000
        });
        const responseTime = Date.now() - startTime;
        
        const health = response.data;
        
        let message = `🏥 *Health Check*\n\n`;
        message += `🤖 Bot: ✅ Responsive\n`;
        message += `🌐 API: ${health.status === 'healthy' ? '✅' : '❌'} ${health.status}\n`;
        message += `⚡ Response Time: ${responseTime}ms\n`;
        message += `📊 Active Calls: ${health.active_calls || 0}\n`;
        message += `⏰ Checked: ${new Date().toLocaleTimeString()}`;
        
        await ctx.reply(message, { parse_mode: 'Markdown' });
    } catch (error) {
        console.error('Health command error:', error);
        await ctx.reply(`❌ *Health Check Failed*\n\nBot is online but API connection failed.\nError: ${error.message}`, { parse_mode: 'Markdown' });
    }
}

async function executeStatusCommand(ctx) {
    const axios = require('axios');
    
    try {
        const response = await axios.get(`${config.apiUrl}/health`, {
            timeout: 10000
        });
        
        const health = response.data;
        
        let message = `🔍 *System Status*\n\n`;
        message += `🤖 Bot: ✅ Online\n`;
        message += `🌐 API: ${health.status === 'healthy' ? '✅' : '❌'} ${health.status}\n`;
        message += `🗄️ Database: ${health.services?.database?.connected ? '✅ Connected' : '❌ Disconnected'}\n`;
        message += `📊 Active Calls: ${health.active_calls || 0}\n`;
        message += `📋 Recent Calls: ${health.services?.database?.recent_calls || 0}\n`;
        message += `📡 Webhook Service: ${health.services?.webhook_service?.status || 'Unknown'}\n`;
        message += `⏰ Last Check: ${new Date(health.timestamp).toLocaleString()}\n\n`;
        message += `📡 API Endpoint: ${config.apiUrl}`;
        
        await ctx.reply(message, { parse_mode: 'Markdown' });
    } catch (error) {
        console.error('Status command error:', error);
        await ctx.reply(`❌ *System Status Check Failed*\n\nError: ${error.message}`, { parse_mode: 'Markdown' });
    }
}

async function executeSmsStatsCommand(ctx) {
    try {
        await getSmsStats(ctx);
    } catch (error) {
        console.error('SMS stats callback error:', error);
        await ctx.reply('❌ Error fetching SMS statistics.');
    }
}

async function executeRecentSmsCommand(ctx) {
    try {
        const limit = 10;
        const response = await axios.get(`${config.apiUrl}/api/sms/messages/recent`, {
            params: { limit },
            timeout: 10000
        });

        if (response.data.success && response.data.messages.length > 0) {
            const messages = response.data.messages;
            let messagesText = `📱 Recent SMS (${messages.length})\n\n`;

            messages.forEach((msg, index) => {
                const time = new Date(msg.created_at).toLocaleString();
                const direction = msg.direction === 'inbound' ? '📨' : '📤';
                const phone = msg.to_number || msg.from_number || 'Unknown';
                const statusIcon = msg.status === 'delivered' ? '✅' :
                    msg.status === 'failed' ? '❌' :
                    msg.status === 'pending' ? '⏳' : '❓';
                
                messagesText +=
                    `${index + 1}. ${direction} ${phone} ${statusIcon}\n` +
                    `   Status: ${msg.status} | ${time}\n` +
                    `   Message: ${msg.body.substring(0, 60)}${msg.body.length > 60 ? '...' : ''}\n`;
                
                if (msg.error_message) {
                    messagesText += `   Error: ${msg.error_message}\n`;
                }
                
                messagesText += '\n';
            });

            messagesText += 'Use /smsstatus <message_sid> for detailed status info';
            await ctx.reply(messagesText, { parse_mode: 'Markdown' });
        } else {
            await ctx.reply('📱 No recent SMS messages found.');
        }
    } catch (error) {
        console.error('Recent SMS callback error:', error);
        await ctx.reply('❌ Error fetching recent SMS messages.');
    }
}

async function executeTestApiCommand(ctx) {
    const axios = require('axios');
    
    try {
        console.log('Testing API connection to:', config.apiUrl);
        const response = await axios.get(`${config.apiUrl}/health`, {
            timeout: 10000
        });
        
        const health = response.data;
        
        let message = `✅ *API Status: ${health.status}*\n\n`;
        message += `🔗 URL: ${config.apiUrl}\n`;
        message += `📊 Active Calls: ${health.active_calls || 0}\n`;
        message += `🗄️ Database: ${health.services?.database?.connected ? '✅ Connected' : '❌ Disconnected'}\n`;
        message += `⏰ Timestamp: ${new Date(health.timestamp).toLocaleString()}`;
        
        // Add enhanced features info if available
        if (health.enhanced_features) {
            message += `\n🚀 Enhanced Features: ✅ Active`;
        }
        
        await ctx.reply(message, { parse_mode: 'Markdown' });
    } catch (error) {
        console.error('API test failed:', error.message);
        await ctx.reply(`❌ *API Test Failed*\n\nURL: ${config.apiUrl}\nError: ${error.message}`, { parse_mode: 'Markdown' });
    }
}

async function executeCallsCommand(ctx) {
    const axios = require('axios');

    try {
        console.log('Executing calls command via callback...');
        
        let response;
        let calls = [];
        
        // Try multiple API endpoints in order of preference
        const endpoints = [
            `${config.apiUrl}/api/calls/list?limit=10`,  // Enhanced endpoint
            `${config.apiUrl}/api/calls?limit=10`,       // Basic endpoint
        ];
        
        let lastError = null;
        let successfulEndpoint = null;
        
        for (const endpoint of endpoints) {
            try {
                console.log(`Trying endpoint: ${endpoint}`);
                
                response = await axios.get(endpoint, {
                    timeout: 15000,
                    headers: {
                        'Accept': 'application/json',
                        'Content-Type': 'application/json'
                    }
                });

                console.log(`Success! API Response status: ${response.status}`);
                successfulEndpoint = endpoint;
                
                // Handle different response structures
                if (response.data.calls) {
                    calls = response.data.calls;
                } else if (Array.isArray(response.data)) {
                    calls = response.data;
                } else {
                    console.log('Unexpected response structure:', Object.keys(response.data));
                    continue; // Try next endpoint
                }
                
                break; // Success, exit loop
                
            } catch (endpointError) {
                console.log(`Endpoint ${endpoint} failed:`, endpointError.message);
                lastError = endpointError;
                continue; // Try next endpoint
            }
        }
        
        // If all endpoints failed
        if (!calls || calls.length === 0) {
            if (lastError) {
                throw lastError; // Re-throw the last error for proper handling
            } else {
                return ctx.reply('📋 No calls found');
            }
        }

        console.log(`Successfully fetched ${calls.length} calls from: ${successfulEndpoint}`);

        let message = `<b>Recent Calls (${calls.length})</b>\n\n`;

        calls.forEach((call, index) => {
            const dateLabel = escapeHtml(new Date(call.created_at).toLocaleDateString());
            const durationLabel = escapeHtml(
                call.duration
                    ? `${Math.floor(call.duration / 60)}:${String(call.duration % 60).padStart(2, '0')}`
                    : 'N/A'
            );
            const statusLabel = escapeHtml(call.status || 'Unknown');
            const phoneLabel = escapeHtml(call.phone_number || 'Unknown');
            const callId = escapeHtml(call.call_sid || 'N/A');
            const transcriptCount = call.transcript_count || 0;
            const dtmfCount = call.dtmf_input_count || 0;

            message += `${index + 1}. 📞 <b>${phoneLabel}</b>\n`;
            message += `&nbsp;&nbsp;🆔 <code>${callId}</code>\n`;
            message += `&nbsp;&nbsp;📅 ${dateLabel} | ⏱️ ${durationLabel} | 📊 ${statusLabel}\n`;
            if (dtmfCount > 0) {
                message += `&nbsp;&nbsp;🔢 Keypad entries: ${dtmfCount}\n`;
            }
            message += `&nbsp;&nbsp;💬 ${transcriptCount} message${transcriptCount === 1 ? '' : 's'}\n\n`;
        });

        message += 'Use /search &lt;call_id&gt; to view details';

        await ctx.reply(message, { parse_mode: 'HTML' });

    } catch (error) {
        console.error('Error fetching calls list via callback:', error);
        
        // Provide specific error messages based on error type
        if (error.response?.status === 404) {
            await ctx.reply(
                '❌ *API Endpoints Missing*\n\n' +
                'The calls list endpoints are not available on the server\\.\n\n' +
                '*Missing endpoints:*\n' +
                '• `/api/calls` \\- Basic calls listing\n' +
                '• `/api/calls/list` \\- Enhanced calls listing\n\n' +
                'Please contact your system administrator to add these endpoints to the Express application\\.',
                { parse_mode: 'Markdown' }
            );
        } else if (error.code === 'ECONNREFUSED' || error.code === 'ENOTFOUND') {
            await ctx.reply(
                `❌ *Server Connection Failed*\n\n` +
                `Cannot connect to API server at:\n\`${config.apiUrl}\`\n\n` +
                `Please check if the server is running\\.`,
                { parse_mode: 'Markdown' }
            );
        } else if (error.response?.status === 500) {
            await ctx.reply('❌ Server error while fetching calls. Please try again later.');
        } else if (error.response) {
            await ctx.reply(`❌ API error (${error.response.status}): ${error.response.data?.error || 'Unknown error'}`);
        } else {
            await ctx.reply('❌ Error fetching calls list. Please try again later.');
        }
    }
}

function buildProviderKeyboard(activeProvider = '') {
    const keyboard = new InlineKeyboard();
    SUPPORTED_PROVIDERS.forEach((provider, index) => {
        const normalized = provider.toLowerCase();
        const isActive = normalized === activeProvider;
        const label = isActive ? `✅ ${normalized.toUpperCase()}` : normalized.toUpperCase();
        keyboard.text(label, `PROVIDER_SET:${normalized}`);

        const shouldInsertRow = index % 2 === 1 && index < SUPPORTED_PROVIDERS.length - 1;
        if (shouldInsertRow) {
            keyboard.row();
        }
    });

    keyboard.row().text('🔄 Refresh', 'PROVIDER_STATUS');
    return keyboard;
}

async function executeProviderStatusCommand(ctx) {
    try {
        const status = await fetchProviderStatus();
        const active = (status.provider || '').toLowerCase();
        const keyboard = buildProviderKeyboard(active);

        let message = formatProviderStatus(status);
        message += '\n\nTap a provider below to switch.';

        await ctx.reply(message, {
            parse_mode: 'Markdown',
            reply_markup: keyboard,
        });
    } catch (error) {
        console.error('Provider status command error:', error);
        if (error.response) {
            const details = error.response.data?.details || error.response.data?.error || error.response.statusText;
            await ctx.reply(`❌ Failed to fetch provider status: ${details || 'Unknown error'}`);
        } else if (error.request) {
            await ctx.reply('❌ No response from provider API. Please check the server.');
        } else {
            await ctx.reply(`❌ Error fetching provider status: ${error.message}`);
        }
    }
}

async function executeProviderSwitchCommand(ctx, provider) {
    const normalized = (provider || '').trim().toLowerCase();
    if (!normalized || !SUPPORTED_PROVIDERS.includes(normalized)) {
        await ctx.reply('❌ Unsupported provider selection.');
        return;
    }

    try {
        const result = await updateProvider(normalized);
        const status = await fetchProviderStatus();
        const active = (status.provider || '').toLowerCase();
        const keyboard = buildProviderKeyboard(active);

        const targetLabel = active ? active.toUpperCase() : normalized.toUpperCase();
        let message = result.changed === false
            ? `ℹ️ Provider already set to *${targetLabel}*.`
            : `✅ Call provider set to *${targetLabel}*.`;

        message += '\n\n';
        message += formatProviderStatus(status);
        message += '\n\nTap a provider below to switch again.';

        await ctx.reply(message, {
            parse_mode: 'Markdown',
            reply_markup: keyboard,
        });
    } catch (error) {
        console.error('Provider switch command error:', error);
        if (error.response) {
            const details = error.response.data?.details || error.response.data?.error || error.response.statusText;
            await ctx.reply(`❌ Failed to update provider: ${details || 'Unknown error'}`);
        } else if (error.request) {
            await ctx.reply('❌ No response from provider API. Please check the server.');
        } else {
            await ctx.reply(`❌ Error switching provider: ${error.message}`);
        }
    }
}

const TELEGRAM_COMMANDS = [
    { command: 'start', description: 'Start or restart the bot' },
    { command: 'call', description: 'Start outbound voice call' },
    { command: 'sms', description: 'Send SMS message' },
    { command: 'search', description: 'Search calls' },
    { command: 'recent', description: 'List recent calls' },
    { command: 'smsconversation', description: 'View SMS conversation' },
    { command: 'guide', description: 'Show detailed usage guide' },
    { command: 'help', description: 'Show available commands' },
    { command: 'cancel', description: 'Cancel the current action' },
    { command: 'menu', description: 'Show quick action menu' },
    { command: 'health', description: 'Check bot and API health' },
    { command: 'bulksms', description: 'Send bulk SMS (admin only)' },
    { command: 'schedulesms', description: 'Schedule SMS message' },
    { command: 'provider', description: 'Manage call provider (admin only)' },
    { command: 'smsstats', description: 'SMS statistics (admin only)' },
    { command: 'templates', description: 'Manage call & SMS templates (admin only)' },
    { command: 'adduser', description: 'Add user (admin only)' },
    { command: 'promote', description: 'Promote to ADMIN (admin only)' },
    { command: 'removeuser', description: 'Remove a USER (admin only)' },
    { command: 'users', description: 'List authorized users (admin only)' },
    { command: 'status', description: 'System status (admin only)' }
];

// Handle unknown commands and text messages
bot.on('message:text', async (ctx) => {
    const text = ctx.message.text;
    
    // Skip if it's a command that's handled elsewhere
    if (text.startsWith('/')) {
        return;
    }
    
    // For non-command messages outside conversations
    if (!ctx.conversation) {
        await ctx.reply('👋 Use /help to see available commands or /menu for quick actions.');
    }
});

async function bootstrap() {
    try {
        await validateTemplatesApiConnectivity();
    } catch (error) {
        console.error(`❌ ${error.message}`);
        process.exit(1);
    }

    console.log('🚀 Starting Voice Call Bot...');
    try {
        await bot.api.setMyCommands(TELEGRAM_COMMANDS);
        console.log('✅ Telegram commands registered');
        await bot.start();
        console.log('✅ Voice Call Bot is running!');
        console.log('🔄 Polling for updates...');
    } catch (error) {
        console.error('❌ Failed to start bot:', error);
        process.exit(1);
    }
}

bootstrap();
