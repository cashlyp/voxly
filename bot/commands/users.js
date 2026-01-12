const { getUser, getUserList, addUser, promoteUser, removeUser, isAdmin } = require('../db/db');
const { guardAgainstCommandInterrupt, OperationCancelledError } = require('../utils/sessionState');

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

function registerAddUserCommand(bot) {
  bot.command(['adduser', 'authorize'], async (ctx) => {
    try {
      const user = await new Promise((resolve) => getUser(ctx.from.id, resolve));
      if (!user) {
        return ctx.reply('❌ You are not authorized to use this bot.');
      }

      const adminStatus = await new Promise((resolve) => isAdmin(ctx.from.id, resolve));
      if (!adminStatus) {
        return ctx.reply('❌ This command is for administrators only.');
      }

      await ctx.conversation.enter('adduser-conversation');
    } catch (error) {
      console.error('Add user command error:', error);
      await ctx.reply('❌ An error occurred. Please try again.');
    }
  });
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

function registerPromoteCommand(bot) {
  bot.command('promote', async (ctx) => {
    try {
      const user = await new Promise((resolve) => getUser(ctx.from.id, resolve));
      if (!user) {
        return ctx.reply('❌ You are not authorized to use this bot.');
      }

      const adminStatus = await new Promise((resolve) => isAdmin(ctx.from.id, resolve));
      if (!adminStatus) {
        return ctx.reply('❌ This command is for administrators only.');
      }

      await ctx.conversation.enter('promote-conversation');
    } catch (error) {
      console.error('Promote command error:', error);
      await ctx.reply('❌ An error occurred. Please try again.');
    }
  });
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

function registerRemoveUserCommand(bot) {
  bot.command('removeuser', async (ctx) => {
    try {
      const user = await new Promise((resolve) => getUser(ctx.from.id, resolve));
      if (!user) {
        return ctx.reply('❌ You are not authorized to use this bot.');
      }

      const adminStatus = await new Promise((resolve) => isAdmin(ctx.from.id, resolve));
      if (!adminStatus) {
        return ctx.reply('❌ This command is for administrators only.');
      }

      await ctx.conversation.enter('remove-conversation');
    } catch (error) {
      console.error('Remove user command error:', error);
      await ctx.reply('❌ An error occurred. Please try again.');
    }
  });
}

// ------------------------- Users List Command -------------------------
function registerUserListCommand(bot) {
  bot.command('users', async (ctx) => {
    try {
      const user = await new Promise((resolve) => getUser(ctx.from.id, resolve));
      if (!user) {
        return ctx.reply('❌ You are not authorized to use this bot.');
      }

      const adminStatus = await new Promise((resolve) => isAdmin(ctx.from.id, resolve));
      if (!adminStatus) {
        return ctx.reply('❌ This command is for administrators only.');
      }

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
      console.error('Users command error:', error);
      await ctx.reply('❌ Error fetching users list. Please try again.');
    }
  });
}

module.exports = {
  addUserFlow,
  registerAddUserCommand,
  promoteFlow,
  registerPromoteCommand,
  removeUserFlow,
  registerRemoveUserCommand,
  registerUserListCommand,
};
