const { InlineKeyboard } = require('grammy');
const config = require('../config');
const { getUser } = require('../db/db');
const { escapeHtml } = require('../utils/commandFormat');
const { sendMenu } = require('../utils/menuCleanup');

async function handleGuide(ctx) {
    const user = await new Promise(r => getUser(ctx.from.id, r));
    if (!user) {
        return ctx.reply('❌ You are not authorized to use this bot.');
    }
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
        '🔁 /provider twilio|aws|vonage — switch on the fly',
        '👥 /users, /adduser, /removeuser — manage seats'
    ];

    const troubleshooting = [
        'Check number format if a call fails',
        'Ensure your profile is authorized',
        'Ask the admin for persistent issues',
        'Use /status to validate system health'
    ];

    const formatLines = (items) => items.map((item) => `• ${escapeHtml(item)}`).join('\n');

    const guideSections = [
        `<b>${escapeHtml('Voice Call Bot Guide — stylized steps for smooth operations.')}</b>`,
        `<b>Making Calls</b>\n${formatLines(callSteps)}`,
        `<b>Phone Number Rules</b>\n${formatLines(formatRules)}`,
        `<b>Best Practices</b>\n${formatLines(bestPractices)}`,
        `<b>Admin Controls</b>\n${formatLines(adminControls)}`,
        `<b>Troubleshooting</b>\n${formatLines(troubleshooting)}`,
        `<b>Need Help?</b>\n${formatLines([
            `🆘 Contact: @${escapeHtml(config.admin.username || '')}`,
            '🧭 Version: 1.0.0'
        ])}`
    ];

    const guideText = guideSections.join('\n\n');

    const kb = new InlineKeyboard()
        .text('📞 Call', 'CALL')
        .text('📋 Commands', 'HELP')
        .row()
        .text('💬 SMS', 'SMS')
        .text('📧 Email', 'EMAIL')
        .row()
        .text('🔄 Menu', 'MENU');

    await sendMenu(ctx, guideText, {
        parse_mode: 'HTML',
        reply_markup: kb
    });
}

function registerGuideCommand(bot) {
    bot.command('guide', handleGuide);
}

module.exports = {
    registerGuideCommand,
    handleGuide
};
