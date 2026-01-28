const { InlineKeyboard } = require('grammy');
const config = require('../config');
const { getAccessProfile } = require('../utils/capabilities');

function buildMiniappKeyboard() {
    const kb = new InlineKeyboard();
    const buttonLabel = '🖥️ VOICEDNUT ✅ mini app';
    if (typeof kb.webApp === 'function') {
        kb.webApp(buttonLabel, config.miniappUrl);
    } else {
        kb.url(buttonLabel, config.miniappUrl);
    }
    return kb;
}

async function handleMiniapp(ctx) {
    try {
        const access = await getAccessProfile(ctx);
        if (!config.miniappUrl) {
            await ctx.reply('❌ VOICEDNUT ✅ mini app URL is not configured. Set MINIAPP_URL in bot/.env.');
            return;
        }
        if (!access.user) {
            await ctx.reply('🔒 Access required to use the VOICEDNUT ✅ mini app. Contact the admin to get approved.');
            return;
        }
        const brandedName = 'VOICEDNUT ✅ mini app';
        const message = access.isAdmin
            ? `🖥️ Open the ${brandedName} admin console.`
            : `🖥️ Open the ${brandedName} (read-only access).`;
        await ctx.reply(message, {
            reply_markup: buildMiniappKeyboard()
        });
    } catch (error) {
        console.error('Miniapp command error:', error);
        await ctx.reply('❌ Unable to open the VOICEDNUT ✅ mini app right now.');
    }
}

function registerMiniappCommand(bot) {
    bot.command('miniapp', handleMiniapp);
}

module.exports = {
    registerMiniappCommand,
    handleMiniapp
};
