const { InlineKeyboard } = require('grammy');
const { getUser, isAdmin } = require('../db/db');
const { cancelActiveFlow, resetSession } = require('../utils/sessionState');
const { section, emphasize } = require('../utils/messageStyle');

module.exports = (bot) => {
    // Menu command
    bot.command('menu', async (ctx) => {
        try {
            // Check user authorization
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
            .text('⏰ Schedule', 'SCHEDULE_SMS')
            .text('📜 SMS Status', 'SMS_STATUS_HELP')
            .row()
            .text('📋 Calls', 'CALLS')
            .text('🧾 Threads', 'SMS_CONVO_HELP')
            .row()
            .text('📚 Guide', 'GUIDE')
            .text('🏥 Health', 'HEALTH')
            .row()
            .text('ℹ️ Help', 'HELP');

            if (isOwner) {
                kb.row()
                    .text('📤 Bulk', 'BULK_SMS')
                    .text('📊 Stats', 'SMS_STATS')
                    .row()
                    .text('👥 Users', 'USERS')
                    .text('➕ Add', 'ADDUSER')
                    .row()
                    .text('⬆️ Promote', 'PROMOTE')
                    .text('❌ Remove', 'REMOVE')
                    .row()
                    .text('🧰 Templates', 'TEMPLATES')
                    .text('📥 Recent', 'RECENT_SMS')
                    .row()
                    .text('☎️ Provider', 'PROVIDER_STATUS')
                    .text('🔍 Status', 'STATUS')
                    .row()
                    .text('🧪 Test API', 'TEST_API');
            }

            const menuText = isOwner ? 
                `${emphasize('Administrator Menu')} \n${section('Choose an action', ['• Access advanced tools below'])}` :
                `${emphasize('Quick Actions Menu')} \n${section('Tap a shortcut', ['• Get calling, texting and status tools fast'])}`;

            await ctx.reply(menuText, {
                parse_mode: 'Markdown',
                reply_markup: kb
            });
        } catch (error) {
            console.error('Menu command error:', error);
            await ctx.reply('❌ Error displaying menu. Please try again.');
        }
    });
};
