const { InlineKeyboard } = require('grammy');
const { getUser, getUserList, addUser, promoteUser, removeUser, isAdmin } = require('../db/db');
const { buildCallbackData } = require('../utils/actions');
const { guardAgainstCommandInterrupt, OperationCancelledError } = require('../utils/sessionState');
const { renderMenu, buildBackToMenuKeyboard, cancelledMessage, setupStepMessage } = require('../utils/ui');

const CANCEL_KEYWORDS = new Set(['cancel', 'exit', 'quit']);

function isCancelInput(value) {
  return CANCEL_KEYWORDS.has(String(value || '').trim().toLowerCase());
}

async function ensureAdminAccess(ctx) {
  const user = await new Promise((resolve) => getUser(ctx.from.id, resolve));
  if (!user) {
    await ctx.reply('❌ Access denied. Your account is not authorized for this action.');
    return false;
  }

  const adminStatus = await new Promise((resolve) => isAdmin(ctx.from.id, resolve));
  if (!adminStatus) {
    await ctx.reply('❌ Access denied. This action is available to administrators only.');
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
    .text('❌ Remove User', buildCallbackData(ctx, 'REMOVE'))
    .row()
    .text('⬅️ Main Menu', buildCallbackData(ctx, 'MENU'));
}

function buildUsersResultKeyboard(ctx) {
  return buildBackToMenuKeyboard(ctx, {
    backAction: 'USERS',
    backLabel: '⬅️ Back to User Management'
  });
}

async function renderUsersMenu(ctx, note = '') {
  const message = note
    ? setupStepMessage('User Management', [note])
    : setupStepMessage('User Management', ['Choose an action below.']);
  await renderMenu(ctx, message, buildUsersKeyboard(ctx), { parseMode: 'Markdown' });
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
      await ctx.reply('📋 No users found in the system.', {
        reply_markup: buildUsersResultKeyboard(ctx)
      });
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

    await ctx.reply(message, {
      reply_markup: buildUsersResultKeyboard(ctx)
    });
  } catch (error) {
    console.error('Users list error:', error);
    await ctx.reply('❌ Error fetching users list. Please try again.', {
      reply_markup: buildUsersResultKeyboard(ctx)
    });
  }
}

// ------------------------- Add User Flow -------------------------
async function addUserFlow(conversation, ctx) {
  try {
    await ctx.reply(setupStepMessage('Add User', [
      'Enter the Telegram numeric ID.',
      'Type `cancel` to stop.'
    ]), {
      parse_mode: 'Markdown'
    });
    const idMsg = await conversation.wait();
    const idText = idMsg?.message?.text?.trim();
    if (idText) {
      await guardAgainstCommandInterrupt(ctx, idText);
    }
    if (isCancelInput(idText)) {
      await ctx.reply(cancelledMessage('Add user', 'Use /users to continue user management.'), {
        parse_mode: 'Markdown',
        reply_markup: buildUsersResultKeyboard(ctx)
      });
      return;
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

    await ctx.reply(setupStepMessage('Add User', [
      'Enter the username (without @).',
      'Type `cancel` to stop.'
    ]), {
      parse_mode: 'Markdown'
    });
    const usernameMsg = await conversation.wait();
    const usernameText = usernameMsg?.message?.text?.trim();
    if (usernameText) {
      await guardAgainstCommandInterrupt(ctx, usernameText);
    }
    if (isCancelInput(usernameText)) {
      await ctx.reply(cancelledMessage('Add user', 'Use /users to continue user management.'), {
        parse_mode: 'Markdown',
        reply_markup: buildUsersResultKeyboard(ctx)
      });
      return;
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

    await ctx.reply(`✅ @${username} (${id}) added as USER.`, {
      reply_markup: buildUsersResultKeyboard(ctx)
    });
  } catch (error) {
    if (error instanceof OperationCancelledError) {
      console.log('Add user flow cancelled');
      return;
    }
    console.error('Add user flow error:', error);
    await ctx.reply('❌ An error occurred while adding user. Please try again.', {
      reply_markup: buildUsersResultKeyboard(ctx)
    });
  }
}


// ------------------------- Promote User Flow -------------------------
async function promoteFlow(conversation, ctx) {
  try {
    await ctx.reply(setupStepMessage('Promote User', [
      'Enter the Telegram numeric ID to promote.',
      'Type `cancel` to stop.'
    ]), {
      parse_mode: 'Markdown'
    });
    const idMsg = await conversation.wait();
    const idText = idMsg?.message?.text?.trim();
    if (idText) {
      await guardAgainstCommandInterrupt(ctx, idText);
    }
    if (isCancelInput(idText)) {
      await ctx.reply(cancelledMessage('Promote user', 'Use /users to continue user management.'), {
        parse_mode: 'Markdown',
        reply_markup: buildUsersResultKeyboard(ctx)
      });
      return;
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

    await ctx.reply(`✅ User ${id} promoted to ADMIN.`, {
      reply_markup: buildUsersResultKeyboard(ctx)
    });
  } catch (error) {
    if (error instanceof OperationCancelledError) {
      console.log('Promote flow cancelled');
      return;
    }
    console.error('Promote flow error:', error);
    await ctx.reply('❌ An error occurred while promoting user. Please try again.', {
      reply_markup: buildUsersResultKeyboard(ctx)
    });
  }
}


// ------------------------- Remove User Flow -------------------------
async function removeUserFlow(conversation, ctx) {
  try {
    await ctx.reply(setupStepMessage('Remove User', [
      'Enter the Telegram numeric ID to remove.',
      'Type `cancel` to stop.'
    ]), {
      parse_mode: 'Markdown'
    });
    const idMsg = await conversation.wait();
    const idText = idMsg?.message?.text?.trim();
    if (idText) {
      await guardAgainstCommandInterrupt(ctx, idText);
    }
    if (isCancelInput(idText)) {
      await ctx.reply(cancelledMessage('Remove user', 'Use /users to continue user management.'), {
        parse_mode: 'Markdown',
        reply_markup: buildUsersResultKeyboard(ctx)
      });
      return;
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

    await ctx.reply(`✅ User ${id} removed.`, {
      reply_markup: buildUsersResultKeyboard(ctx)
    });
  } catch (error) {
    if (error instanceof OperationCancelledError) {
      console.log('Remove user flow cancelled');
      return;
    }
    console.error('Remove user flow error:', error);
    await ctx.reply('❌ An error occurred while removing user. Please try again.', {
      reply_markup: buildUsersResultKeyboard(ctx)
    });
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
