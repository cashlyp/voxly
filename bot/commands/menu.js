const { InlineKeyboard } = require('grammy');
const { getAccessProfile } = require('../utils/capabilities');
const { cancelActiveFlow, resetSession } = require('../utils/sessionState');
const { escapeHtml, renderMenu } = require('../utils/ui');
const { buildCallbackData } = require('../utils/actions');

async function handleMenu(ctx) {
    try {
        await cancelActiveFlow(ctx, 'command:/menu');
        resetSession(ctx);
        if (ctx.session && typeof ctx.session === 'object') {
            ctx.session.timeoutNotified = false;
        }

        const access = await getAccessProfile(ctx);
        const isOwner = access.isAdmin;

        const kb = new InlineKeyboard()
            .text(access.user ? '📞 Call' : '🔒 Call', buildCallbackData(ctx, 'CALL'))
            .text(access.user ? '💬 SMS' : '🔒 SMS', buildCallbackData(ctx, 'SMS'))
            .row()
            .text(access.user ? '📧 Email' : '🔒 Email', buildCallbackData(ctx, 'EMAIL'))
            .text(access.user ? '📜 Call Log' : '🔒 Call Log', buildCallbackData(ctx, 'CALLLOG'));

        if (access.user) {
            kb.row()
                .text('📚 Guide', buildCallbackData(ctx, 'GUIDE'))
                .text('ℹ️ Help', buildCallbackData(ctx, 'HELP'));
            if (isOwner) {
                kb.row()
                    .text('🏥 Health', buildCallbackData(ctx, 'HEALTH'))
                    .text('🔍 Status', buildCallbackData(ctx, 'STATUS'));
            } else {
                kb.row().text('🏥 Health', buildCallbackData(ctx, 'HEALTH'));
            }
        } else {
            kb.row()
                .text('📚 Guide', buildCallbackData(ctx, 'GUIDE'))
                .text('ℹ️ Help', buildCallbackData(ctx, 'HELP'));
        }

        if (isOwner) {
            kb.row()
                .text('👥 Users', buildCallbackData(ctx, 'USERS'))
                .text('🧰 Scripts', buildCallbackData(ctx, 'SCRIPTS'))
                .row()
                .text('📵 Caller Flags', buildCallbackData(ctx, 'CALLER_FLAGS'))
                .text('☎️ Provider', buildCallbackData(ctx, 'PROVIDER:HOME'))
                .row()
                .text('📤 SMS Sender', buildCallbackData(ctx, 'BULK_SMS'))
                .text('📧 Mailer', buildCallbackData(ctx, 'BULK_EMAIL'));
        } else if (!access.user) {
            kb.row().text('📩 Request Access', buildCallbackData(ctx, 'REQUEST_ACCESS'));
        }

        const commonHint = 'SMS and Email actions are grouped under /sms and /email.';
        const accessHint = access.user
            ? 'Authorized access enabled.'
            : 'Limited access: request approval to run actions.';
        const menuText = isOwner
            ? `<b>${escapeHtml('Administrator Menu')}</b>\n${escapeHtml('Choose an action')}\n• ${escapeHtml('Admin tools enabled')}\n• ${escapeHtml(commonHint)}`
            : `<b>${escapeHtml('Quick Actions Menu')}</b>\n${escapeHtml('Tap a shortcut')}\n• ${escapeHtml(commonHint)}\n• ${escapeHtml(accessHint)}`;

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
