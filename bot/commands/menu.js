const { InlineKeyboard } = require('grammy');
const { getUser, isAdmin } = require('../db/db');
const { cancelActiveFlow, resetSession } = require('../utils/sessionState');
const { escapeHtml } = require('../utils/commandFormat');
const { sendMenu } = require('../utils/menuCleanup');

async function handleMenu(ctx) {
    try {
        await cancelActiveFlow(ctx, 'command:/menu');
        resetSession(ctx);

        const user = await new Promise(r => getUser(ctx.from.id, r));
        if (!user) {
            return ctx.reply('❌ You are not authorized to use this bot.');
        }

        const isOwner = await new Promise(r => isAdmin(ctx.from.id, r));

        const kb = new InlineKeyboard()
            .text('📞 Call', 'CALL')
            .text('💬 SMS', 'SMS')
            .row()
            .text('📧 Email', 'EMAIL')
            .text('⏰ Schedule', 'SCHEDULE_SMS')
            .row()
            .text('📋 Calls', 'CALLS');

        if (isOwner) {
            kb.text('🧾 Threads', 'SMS_CONVO_HELP');
        }

        kb.row()
            .text('📜 SMS Status', 'SMS_STATUS_HELP')
            .text('📨 Email Status', 'EMAIL_STATUS_HELP')
            .row()
            .text('📚 Guide', 'GUIDE')
            .text('🏥 Health', 'HEALTH')
            .row()
            .text('ℹ️ Help', 'HELP');

        if (isOwner) {
            kb.row()
                .text('📤 Bulk SMS', 'BULK_SMS')
                .text('📧 Bulk Email', 'BULK_EMAIL')
                .row()
                .text('📊 SMS Stats', 'SMS_STATS')
                .text('📥 Recent', 'RECENT_SMS')
                .row()
                .text('👥 Users', 'USERS')
                .text('➕ Add', 'ADDUSER')
                .row()
                .text('⬆️ Promote', 'PROMOTE')
                .text('❌ Remove', 'REMOVE')
                .row()
                .text('🧰 Scripts', 'SCRIPTS')
                .text('☎️ Provider', 'PROVIDER_STATUS')
                .row()
                .text('🔍 Status', 'STATUS')
                .text('🧪 Test API', 'TEST_API');
        }

        const menuText = isOwner
            ? `<b>${escapeHtml('Administrator Menu')}</b>\n${escapeHtml('Choose an action')}\n• ${escapeHtml('Access advanced tools below')}`
            : `<b>${escapeHtml('Quick Actions Menu')}</b>\n${escapeHtml('Tap a shortcut')}\n• ${escapeHtml('Get calling, texting and status tools fast')}`;

        await sendMenu(ctx, menuText, {
            parse_mode: 'HTML',
            reply_markup: kb
        });
    } catch (error) {
        console.error('Menu command error:', error);
        await ctx.reply('❌ Error displaying menu. Please try again.');
    }
}

function registerMenuCommand(bot) {
    bot.command('menu', handleMenu);
}

module.exports = {
    registerMenuCommand,
    handleMenu
};
