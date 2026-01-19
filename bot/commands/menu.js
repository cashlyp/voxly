const { InlineKeyboard } = require('grammy');
const { getUser, isAdmin } = require('../db/db');
const { cancelActiveFlow, resetSession } = require('../utils/sessionState');
const { escapeHtml, renderMenu } = require('../utils/ui');
const { buildCallbackData } = require('../utils/actions');

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
            .text('📞 Call', buildCallbackData(ctx, 'CALL'))
            .text('💬 SMS', buildCallbackData(ctx, 'SMS'))
            .row()
            .text('📧 Email', buildCallbackData(ctx, 'EMAIL'))
            .text('⏰ Schedule', buildCallbackData(ctx, 'SCHEDULE_SMS'))
            .row()
            .text('📋 Calls', buildCallbackData(ctx, 'CALLS'));

        if (isOwner) {
            kb.text('🧾 Threads', buildCallbackData(ctx, 'SMS_CONVO_HELP'));
        }

        kb.row()
            .text('📜 SMS Status', buildCallbackData(ctx, 'SMS_STATUS_HELP'))
            .text('📨 Email Status', buildCallbackData(ctx, 'EMAIL_STATUS_HELP'))
            .row()
            .text('📚 Guide', buildCallbackData(ctx, 'GUIDE'))
            .text('🏥 Health', buildCallbackData(ctx, 'HEALTH'))
            .row()
            .text('ℹ️ Help', buildCallbackData(ctx, 'HELP'));

        if (isOwner) {
            kb.row()
                .text('📤 Bulk SMS', buildCallbackData(ctx, 'BULK_SMS'))
                .text('📧 Bulk Email', buildCallbackData(ctx, 'BULK_EMAIL'))
                .row()
                .text('📊 SMS Stats', buildCallbackData(ctx, 'SMS_STATS'))
                .text('📥 Recent', buildCallbackData(ctx, 'RECENT_SMS'))
                .row()
                .text('👥 Users', buildCallbackData(ctx, 'USERS'))
                .text('➕ Add', buildCallbackData(ctx, 'ADDUSER'))
                .row()
                .text('⬆️ Promote', buildCallbackData(ctx, 'PROMOTE'))
                .text('❌ Remove', buildCallbackData(ctx, 'REMOVE'))
                .row()
                .text('🧰 Scripts', buildCallbackData(ctx, 'SCRIPTS'))
                .text('☎️ Provider', buildCallbackData(ctx, 'PROVIDER_STATUS'))
                .row()
                .text('🔍 Status', buildCallbackData(ctx, 'STATUS'))
                .text('🧪 Test API', buildCallbackData(ctx, 'TEST_API'));
        }

        const menuText = isOwner
            ? `<b>${escapeHtml('Administrator Menu')}</b>\n${escapeHtml('Choose an action')}\n• ${escapeHtml('Access advanced tools below')}`
            : `<b>${escapeHtml('Quick Actions Menu')}</b>\n${escapeHtml('Tap a shortcut')}\n• ${escapeHtml('Get calling, texting and status tools fast')}`;

        await renderMenu(ctx, menuText, kb, { parseMode: 'HTML' });
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
