const { InlineKeyboard } = require('grammy');
const config = require('../config');
const { getUser } = require('../db/db');
const { section, emphasize, buildLine, escapeMarkdown } = require('../utils/messageStyle');

module.exports = (bot) => {
    bot.command('guide', async (ctx) => {
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

        const guideSections = [
            emphasize('Voice Call Bot Guide — stylized steps for smooth operations.'),
            section('Making Calls', callSteps),
            section('Phone Number Rules', formatRules),
            section('Best Practices', bestPractices),
            section('Admin Controls', adminControls),
            section('Troubleshooting', troubleshooting),
            section('Need Help?', [
                tipLine('🆘', `Contact: @${escapeMarkdown(config.admin.username)}`),
                buildLine('🧭', 'Version', '1.0.0')
            ])
        ];

        const guideText = guideSections.join('\n\n');

        const kb = new InlineKeyboard()
            .text('📞 Call', 'CALL')
            .text('📋 Commands', 'HELP')
            .row()
            .text('💬 SMS', 'SMS')
            .text('🔄 Menu', 'MENU');

        await ctx.reply(guideText, {
            parse_mode: 'Markdown',
            reply_markup: kb
        });
    });
};
