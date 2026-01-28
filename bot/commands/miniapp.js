const { InlineKeyboard } = require('grammy');
const config = require('../config');
const { getAccessProfile } = require('../utils/capabilities');

function buildMiniappKeyboard() {
    const kb = new InlineKeyboard();
    if (typeof kb.webApp === 'function') {
        kb.webApp('🖥️ Open Mini App', config.miniappUrl);
    } else {
        kb.url('🖥️ Open Mini App', config.miniappUrl);
    }
    return kb;
}

async function handleMiniapp(ctx) {
    try {
        const access = await getAccessProfile(ctx);
        if (!config.miniappUrl) {
            await ctx.reply('❌ Mini App URL is not configured. Set MINIAPP_URL in bot/.env.');
            return;
        }
        if (!access.user) {
            await ctx.reply('🔒 Access required to use the Mini App. Contact the admin to get approved.');
            return;
        }
        const message = access.isAdmin
            ? '🖥️ Open the VOICEDNUT Mini App admin console.'
            : '🖥️ Open the VOICEDNUT Mini App (read-only access).';
        await ctx.reply(message, {
            reply_markup: buildMiniappKeyboard()
        });
    } catch (error) {
        console.error('Miniapp command error:', error);
        await ctx.reply('❌ Unable to open the Mini App right now.');
    }
}

function registerMiniappCommand(bot) {
    bot.command('miniapp', handleMiniapp);
}

module.exports = {
    registerMiniappCommand,
    handleMiniapp
};
