const { InlineKeyboard } = require('grammy');
const { getUser, getUserList, addUser, promoteUser, removeUser, isAdmin } = require('../db/db');
const { buildCallbackData } = require('../utils/actions');
const { guardAgainstCommandInterrupt, OperationCancelledError } = require('../utils/sessionState');
const { renderMenu } = require('../utils/ui');

async function ensureAdminAccess(ctx) {
  const user = await new Promise((resolve) => getUser(ctx.from.id, resolve));
  if (!user) {
    await ctx.reply('❌ You are not authorized to use this bot.');
    return false;
  }

  const adminStatus = await new Promise((resolve) => isAdmin(ctx.from.id, resolve));
  if (!adminStatus) {
    await ctx.reply('❌ This command is for administrators only.');
    return false;
  }

  return true;
}

function buildUsersKeyboard(ctx) {
  return new InlineKeyboard()
    .text('📋 List Users', buildCallbackData(ctx, 'USERS_LIST'))
    .row()
    .text('➕ Add User', buildCallbackData(ctx, 'ADDUSER'))
    .text('⬆️ Promote User', buildCallbackData(ctx, 'PROMOTE'))
    .row()
    .text('❌ Remove User', buildCallbackData(ctx, 'REMOVE'));
}

async function renderUsersMenu(ctx, note = '') {
  const message = note
    ? `👥 User Management\n${note}`
    : '👥 User Management\nChoose an action below.';
  await renderMenu(ctx, message, buildUsersKeyboard(ctx));
}

async function sendUsersList(ctx) {
  try {
    const users = await new Promise((resolve) => {
      getUserList((err, result) => {
        if (err) {
          console.error('Database error in getUserList:', err);
          resolve([]);
        } else {
          resolve(result || []);
        }
      });
    });

    if (!users || users.length === 0) {
      await ctx.reply('📋 No users found in the system.');
      return;
    }

    let message = `📋 USERS LIST (${users.length}):\n\n`;

    users.forEach((item, index) => {
      const roleIcon = item.role === 'ADMIN' ? '🛡️' : '👤';
      const username = item.username || 'no_username';
      const joinDate = new Date(item.timestamp).toLocaleDateString();
      message += `${index + 1}. ${roleIcon} @${username}\n`;
      message += `   ID: ${item.telegram_id}\n`;
      message += `   Role: ${item.role}\n`;
      message += `   Joined: ${joinDate}\n\n`;
    });

    await ctx.reply(message);
  } catch (error) {
    console.error('Users list error:', error);
    await ctx.reply('❌ Error fetching users list. Please try again.');
  }
}

// ------------------------- Add User Flow -------------------------
async function addUserFlow(conversation, ctx) {
  try {
    await ctx.reply('🆔 Enter Telegram ID:');
    const idMsg = await conversation.wait();
    const idText = idMsg?.message?.text?.trim();
    if (idText) {
      await guardAgainstCommandInterrupt(ctx, idText);
    }
    if (!idText) {
      await ctx.reply('❌ Please send a valid text message.');
      return;
    }

    const id = parseInt(idText, 10);
    if (Number.isNaN(id)) {
      await ctx.reply('❌ Invalid Telegram ID. Please send a number.');
      return;
    }

    await ctx.reply('🔠 Enter username:');
    const usernameMsg = await conversation.wait();
    const usernameText = usernameMsg?.message?.text?.trim();
    if (usernameText) {
      await guardAgainstCommandInterrupt(ctx, usernameText);
    }
    if (!usernameText) {
      await ctx.reply('❌ Please send a valid username.');
      return;
    }

    const username = usernameText;
    if (!username) {
      await ctx.reply('❌ Username cannot be empty.');
      return;
    }

    await new Promise((resolve, reject) => {
      addUser(id, username, 'USER', (err) => {
        if (err) {
          console.error('Database error in addUser:', err);
          reject(err);
          return;
        }
        resolve();
      });
    });

    await ctx.reply(`✅ @${username} (${id}) added as USER.`);
  } catch (error) {
    if (error instanceof OperationCancelledError) {
      console.log('Add user flow cancelled');
      return;
    }
    console.error('Add user flow error:', error);
    await ctx.reply('❌ An error occurred while adding user. Please try again.');
  }
}


// ------------------------- Promote User Flow -------------------------
async function promoteFlow(conversation, ctx) {
  try {
    await ctx.reply('🆔 Enter Telegram ID to promote:');
    const idMsg = await conversation.wait();
    const idText = idMsg?.message?.text?.trim();
    if (idText) {
      await guardAgainstCommandInterrupt(ctx, idText);
    }
    if (!idText) {
      await ctx.reply('❌ Please send a valid Telegram ID.');
      return;
    }

    const id = parseInt(idText, 10);
    if (Number.isNaN(id)) {
      await ctx.reply('❌ Invalid Telegram ID. Please send a number.');
      return;
    }

    await new Promise((resolve, reject) => {
      promoteUser(id, (err) => {
        if (err) {
          console.error('Database error in promoteUser:', err);
          reject(err);
          return;
        }
        resolve();
      });
    });

    await ctx.reply(`✅ User ${id} promoted to ADMIN.`);
  } catch (error) {
    if (error instanceof OperationCancelledError) {
      console.log('Promote flow cancelled');
      return;
    }
    console.error('Promote flow error:', error);
    await ctx.reply('❌ An error occurred while promoting user. Please try again.');
  }
}


// ------------------------- Remove User Flow -------------------------
async function removeUserFlow(conversation, ctx) {
  try {
    await ctx.reply('🆔 Enter Telegram ID to remove:');
    const idMsg = await conversation.wait();
    const idText = idMsg?.message?.text?.trim();
    if (idText) {
      await guardAgainstCommandInterrupt(ctx, idText);
    }
    if (!idText) {
      await ctx.reply('❌ Please send a valid Telegram ID.');
      return;
    }

    const id = parseInt(idText, 10);
    if (Number.isNaN(id)) {
      await ctx.reply('❌ Invalid Telegram ID. Please send a number.');
      return;
    }

    await new Promise((resolve, reject) => {
      removeUser(id, (err) => {
        if (err) {
          console.error('Database error in removeUser:', err);
          reject(err);
          return;
        }
        resolve();
      });
    });

    await ctx.reply(`✅ User ${id} removed.`);
  } catch (error) {
    if (error instanceof OperationCancelledError) {
      console.log('Remove user flow cancelled');
      return;
    }
    console.error('Remove user flow error:', error);
    await ctx.reply('❌ An error occurred while removing user. Please try again.');
  }
}


// ------------------------- Users List Command -------------------------
function registerUserListCommand(bot) {
  bot.command('users', async (ctx) => {
    try {
      const allowed = await ensureAdminAccess(ctx);
      if (!allowed) return;
      await renderUsersMenu(ctx);
    } catch (error) {
      console.error('Users command error:', error);
      await ctx.reply('❌ Error opening user management. Please try again.');
    }
  });
}

module.exports = {
  addUserFlow,
  promoteFlow,
  removeUserFlow,
  registerUserListCommand,
  renderUsersMenu,
  sendUsersList,
};
