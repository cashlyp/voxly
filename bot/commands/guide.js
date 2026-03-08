const { InlineKeyboard } = require('grammy');
const config = require('../config');
const { escapeHtml, renderMenu } = require('../utils/ui');
const { buildCallbackData } = require('../utils/actions');
const { getAccessProfile } = require('../utils/capabilities');

async function handleGuide(ctx) {
    const access = await getAccessProfile(ctx);
    const callSteps = [
        '1️⃣ Start a call via /call or the 📞 button',
        '2️⃣ Provide the number in E.164 format (+1234567890)',
        '3️⃣ Describe the personality and first prompt',
        '4️⃣ Confirm the initial message to speak',
        '5️⃣ Watch the live console and use controls as needed'
    ];

    const formatRules = [
        '• Must include the + symbol',
        '• Keep the country code first',
        '• No spaces or punctuation besides digits',
        '• Example: +18005551234'
    ];

    const bestPractices = [
        '🧹 Keep prompts precise so the AI stays on track',
        '🧪 Test with a short call before scaling',
        '👂 Monitor the console for user tone shifts',
        '✋ End or interrupt if you need to steer the call'
    ];

    const adminControls = [
        '📍 /provider status — see the active provider',
        '🔁 /provider <name> — switch providers on the fly',
        '👥 /users — manage seats'
    ];

    const troubleshooting = [
        'Check number format if a call fails',
        'Ensure your profile is authorized',
        'Ask the admin for persistent issues',
        'Use /status to validate system health'
    ];

    const formatLines = (items) => items.map((item) => `• ${escapeHtml(item)}`).join('\n');

    const guideSections = [
        `<b>${escapeHtml('Voice Call Bot Guide - Reliable operating steps.')}</b>`,
        `<b>Making Calls</b>\n${formatLines(callSteps)}`,
        `<b>Phone Number Rules</b>\n${formatLines(formatRules)}`,
        `<b>Best Practices</b>\n${formatLines(bestPractices)}`,
        `<b>Admin Controls</b>\n${formatLines(adminControls)}`,
        `<b>Troubleshooting</b>\n${formatLines(troubleshooting)}`,
        `<b>Need Help?</b>\n${formatLines([
            `🆘 Contact: @${escapeHtml(config.admin.username || '')}`,
            'Use /help for the full command list.'
        ])}`
    ];

    if (!access.user) {
        guideSections.unshift(
            `<b>${escapeHtml('Limited Access')}</b>\n${formatLines([
                'You can explore menus, but actions require approval.',
                'Use the contact above to request access.'
            ])}`
        );
    }

    const guideText = guideSections.join('\n\n');

    const kb = new InlineKeyboard()
        .text('📋 Commands', buildCallbackData(ctx, 'HELP'))
        .text('🔄 Menu', buildCallbackData(ctx, 'MENU'));

    if (access.user) {
        kb.row()
            .text('📞 Call', buildCallbackData(ctx, 'CALL'))
            .text('💬 SMS', buildCallbackData(ctx, 'SMS'))
            .row()
            .text('📧 Email', buildCallbackData(ctx, 'EMAIL'));
    } else {
        const adminUsername = (config.admin.username || '').replace(/^@/, '');
        if (adminUsername) {
            kb.row().url('🔓 Request Access', `https://t.me/${adminUsername}`);
        }
    }

    await renderMenu(ctx, guideText, kb, { parseMode: 'HTML' });
}

function registerGuideCommand(bot) {
    bot.command('guide', handleGuide);
}

module.exports = {
    registerGuideCommand,
    handleGuide
};
