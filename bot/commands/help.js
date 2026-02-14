const { InlineKeyboard } = require('grammy');
const { isAdmin, getUser } = require('../db/db');
const config = require('../config');
const { escapeHtml, renderMenu } = require('../utils/ui');
const { buildCallbackData } = require('../utils/actions');

async function handleHelp(ctx) {
    try {
        const user = await new Promise(r => getUser(ctx.from.id, r));
        const isAuthorized = Boolean(user);
        const isOwner = isAuthorized ? await new Promise(r => isAdmin(ctx.from.id, r)) : false;

        const formatLines = (items) => items.map((item) => `• ${escapeHtml(item)}`).join('\n');

        const callList = [
            '📞 /call — launch a fresh voice session (requires access)',
            '📜 /calllog — browse recent calls, search, and events'
        ];

        const smsList = [
            '💬 /sms — open the SMS center (send, schedule, status, threads, stats)'
        ];

        const emailList = [
            '📧 /email — open the Email center (send, status, templates)'
        ];

        const infoList = [
            '🩺 /health or /ping — check bot & API health',
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
            '/calllog',
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
                '👥 /users — manage authorized personnel',
                '📵 /callerflags — manage inbound caller flags',
                '📣 /smssender — bulk SMS center',
                '📦 /mailer — bulk email center',
                '🧪 /status — deep system status',
                '🧰 /scripts — manage reusable prompts',
                '🍃 /persona — sculpt adaptive agents',
                '🔀 /provider — view/switch provider and manage keypad overrides'
            ];
            helpSections.push(`<b>Admin Toolkit</b>\n${formatLines(adminList)}`);
        }

        helpSections.push(
            `<b>Examples</b>\n${formatLines(exampleUsage)}`,
            `<b>Support & Info</b>\n${formatLines(supportBlock)}`
        );

        const unauthSections = [
            `<b>${escapeHtml('⚠️ Access limited')}</b>\n${formatLines([
                'You can browse menus, but actions require approval.'
            ])}`,
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
            ])}`,
            `<b>${escapeHtml('🔒 Limited mode')}</b>\n${formatLines([
                'Menus are visible, but execution is locked.'
            ])}`
        ];

        const helpText = isAuthorized ? helpSections.join('\n\n') : unauthSections.join('\n\n');

        const kb = isAuthorized
            ? (() => {
                const keyboard = new InlineKeyboard()
                    .text('📞 Call', buildCallbackData(ctx, 'CALL'))
                    .text('💬 SMS', buildCallbackData(ctx, 'SMS'))
                    .row()
                    .text('📧 Email', buildCallbackData(ctx, 'EMAIL'))
                    .text('📋 Menu', buildCallbackData(ctx, 'MENU'));

                keyboard.row()
                    .text('📚 Guide', buildCallbackData(ctx, 'GUIDE'))
                    .text('ℹ️ Help', buildCallbackData(ctx, 'HELP'));

                if (isOwner) {
                    keyboard.row()
                        .text('👥 Users', buildCallbackData(ctx, 'USERS'))
                        .text('📵 Caller Flags', buildCallbackData(ctx, 'CALLER_FLAGS'))
                        .row()
                        .text('☎️ Provider', buildCallbackData(ctx, 'PROVIDER_STATUS'));
                }
                return keyboard;
            })()
            : (() => {
                const keyboard = new InlineKeyboard()
                    .text('📚 Guide', buildCallbackData(ctx, 'GUIDE'))
                    .text('📋 Menu', buildCallbackData(ctx, 'MENU'));
                keyboard.row().text('📩 Request Access', buildCallbackData(ctx, 'REQUEST_ACCESS'));
                return keyboard;
            })();

        await renderMenu(ctx, helpText, kb, { parseMode: 'HTML' });

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
