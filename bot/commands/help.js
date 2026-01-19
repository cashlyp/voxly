const { InlineKeyboard } = require('grammy');
const { isAdmin, getUser } = require('../db/db');
const config = require('../config');
const { escapeHtml } = require('../utils/commandFormat');
const { sendMenu } = require('../utils/menuCleanup');

async function handleHelp(ctx) {
    try {
        const user = await new Promise(r => getUser(ctx.from.id, r));
        const isAuthorized = Boolean(user);
        const isOwner = isAuthorized ? await new Promise(r => isAdmin(ctx.from.id, r)) : false;

        const formatLines = (items) => items.map((item) => `• ${escapeHtml(item)}`).join('\n');

        const callList = [
            '📞 /call — launch a fresh voice session (requires access)',
            '🔍 /search <term> — locate calls by number, intent, or ID',
            '🕒 /recent [limit] — list recent calls (max 50)',
            '⏱️ /latency <callSid> — see STT/GPT/TTS timing',
            '🧭 /version — view API/service version info'
        ];

        const smsList = [
            '💬 /sms — send a quick AI-powered SMS (requires access)',
            '📅 /schedulesms — schedule an SMS in the future (requires access)',
            '🧾 /smsconversation <phone> — view recent SMS threads (admin)',
            '🔎 /smsstatus <message_sid> — delivery status for a message (requires access)'
        ];

        const emailList = [
            '📧 /email — send an email message (requires access)',
            '📬 /emailstatus <message_id> — check email delivery (requires access)'
        ];

        const infoList = [
            '🩺 /health or /ping — check bot & API health',
            '📰 /digest — 24h notifications + recent calls digest',
            '📚 /guide — view the master user guide (access required)',
            '📋 /menu — reopen quick actions (access required)',
            '❓ /help — show this message again'
        ];

        const quickUsage = [
            'Use /call or the 📞 button to get started',
            'Enter phone numbers in E.164 format (+1234567890)',
            'Describe the AI agent personality and first message',
            'Monitor live updates and ask for transcripts',
            'End the call with the ✋ Interrupt or ⏹️ End button if needed'
        ];

        const exampleUsage = [
            '+1234567890 (not 123-456-7890)',
            '/search refund',
            '/recent 20',
            '/health'
        ];

        const supportBlock = [
            `🆘 Contact admin: @${escapeHtml(config.admin.username || '')}`,
            '🧭 Bot edition: v2.0.0 — secrets aged to perfection'
        ];

        const helpSections = [
            `<b>${escapeHtml('Ready to guide your AI calls with sparkling clarity.')}</b>`,
            `<b>Call Tools</b>\n${formatLines(callList)}`,
            `<b>SMS Tools</b>\n${formatLines(smsList)}`,
            `<b>Email Tools</b>\n${formatLines(emailList)}`,
            `<b>Navigation & Info</b>\n${formatLines(infoList)}`,
            `<b>Quick Usage Flow</b>\n${formatLines(quickUsage)}`
        ];

        if (isOwner) {
            const adminList = [
                '🛡️ /adduser — add a trusted operator',
                '⭐ /promote — elevate a teammate to admin',
                '❌ /removeuser — cut access cleanly',
                '👥 /users — list all authorized personnel',
                '📣 /bulksms — broadcast smart SMS',
                '📥 /recentsms [limit] — list recent SMS messages',
                '📊 /smsstats — view SMS health & delivery',
                '📦 /bulkemail — send bulk email',
                '📬 /emailbulk <job_id> — bulk email job status',
                '🧪 /status — deep system status',
                '🧪 /testapi — hit the API health endpoint',
                '🧰 /scripts — manage reusable prompts',
                '🍃 /persona — sculpt adaptive agents',
                '🔀 /provider — view or switch voice providers'
            ];
            helpSections.push(`<b>Admin Toolkit</b>\n${formatLines(adminList)}`);
        }

        helpSections.push(
            `<b>Examples</b>\n${formatLines(exampleUsage)}`,
            `<b>Support & Info</b>\n${formatLines(supportBlock)}`
        );

        const unauthSections = [
            `<b>${escapeHtml('Welcome! Access is required to use most commands.')}</b>`,
            `<b>What this bot can do</b>\n${formatLines([
                '🤖 Run AI-powered voice calls and SMS outreach',
                '🧾 Track conversations and delivery status',
                '🛡️ Admins manage users, scripts, and providers'
            ])}`,
            `<b>Get access</b>\n${formatLines([
                `🆘 Contact admin: @${escapeHtml(config.admin.username || '')}`,
                'Share your Telegram @ and reason to be approved.',
                'Once approved, use /start to see your menu.'
            ])}`
        ];

        const helpText = isAuthorized ? helpSections.join('\n\n') : unauthSections.join('\n\n');

        const adminUsername = (config.admin.username || '').replace(/^@/, '');

        const kb = isAuthorized
            ? (() => {
                const keyboard = new InlineKeyboard()
                    .text('📞 Call', 'CALL')
                    .text('📋 Menu', 'MENU')
                    .row()
                    .text('💬 SMS', 'SMS')
                    .text('📧 Email', 'EMAIL')
                    .row()
                    .text('📚 Guide', 'GUIDE');

                if (isOwner) {
                    keyboard.row()
                        .text('👥 Users', 'USERS')
                        .text('➕ Add', 'ADDUSER')
                        .row()
                        .text('☎️ Provider', 'PROVIDER_STATUS');
                }
                return keyboard;
            })()
            : new InlineKeyboard().url('📱 Contact Admin', `https://t.me/${adminUsername}`);

        await sendMenu(ctx, helpText, {
            parse_mode: 'HTML',
            reply_markup: kb
        });

    } catch (error) {
        console.error('Help command error:', error);
        await ctx.reply('❌ Error displaying help. Please try again.');
    }
}

function registerHelpCommand(bot) {
    bot.command('help', handleHelp);
}

module.exports = {
    registerHelpCommand,
    handleHelp
};
