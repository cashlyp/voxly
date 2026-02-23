const { InlineKeyboard } = require('grammy');
const config = require('../config');
const httpClient = require('../utils/httpClient');
const { getUser, isAdmin } = require('../db/db');
const { buildCallbackData } = require('../utils/actions');
const { guardAgainstCommandInterrupt, OperationCancelledError, startOperation } = require('../utils/sessionState');
const { escapeMarkdown, renderMenu } = require('../utils/ui');

const ADMIN_HEADER_NAME = 'x-admin-token';
const DEFAULT_LIMIT = 20;
const STATUS_OPTIONS = ['blocked', 'allowed', 'spam'];

function normalizePhoneInput(raw) {
  const digits = String(raw || '').replace(/\D/g, '');
  if (!digits) return null;
  return `+${digits}`;
}

function formatStatusLabel(status) {
  const normalized = String(status || '').toLowerCase();
  if (normalized === 'blocked') return '🚫 blocked';
  if (normalized === 'allowed') return '✅ allowed';
  if (normalized === 'spam') return '⚠️ spam';
  return normalized || 'unknown';
}

function normalizeStatusInput(input) {
  const normalized = String(input || '').toLowerCase();
  if (normalized === 'allow') return 'allowed';
  if (normalized === 'block') return 'blocked';
  if (normalized === 'spam') return 'spam';
  if (STATUS_OPTIONS.includes(normalized)) return normalized;
  return null;
}

async function ensureAuthorizedAdmin(ctx) {
  const user = await new Promise((resolve) => getUser(ctx.from?.id, resolve));
  if (!user) {
    await ctx.reply('❌ You are not authorized to use this bot.');
    return { isAdminUser: false };
  }
  const adminStatus = await new Promise((resolve) => isAdmin(ctx.from?.id, resolve));
  if (!adminStatus) {
    await ctx.reply('❌ This command is for administrators only.');
    return { isAdminUser: false };
  }
  return { isAdminUser: true };
}

function buildCallerFlagsKeyboard(ctx) {
  return new InlineKeyboard()
    .text('📋 List', buildCallbackData(ctx, 'CALLER_FLAGS_LIST'))
    .row()
    .text('✅ Allow', buildCallbackData(ctx, 'CALLER_FLAGS_ALLOW'))
    .text('🚫 Block', buildCallbackData(ctx, 'CALLER_FLAGS_BLOCK'))
    .row()
    .text('⚠️ Spam', buildCallbackData(ctx, 'CALLER_FLAGS_SPAM'));
}

async function renderCallerFlagsMenu(ctx, note = '') {
  const heading = '📵 Caller Flags';
  const message = note
    ? `${heading}\n${note}`
    : `${heading}\nManage inbound allow/block/spam decisions.`;
  await renderMenu(ctx, message, buildCallerFlagsKeyboard(ctx));
}

async function fetchCallerFlags(params = {}) {
  const response = await httpClient.get(null, `${config.apiUrl}/api/caller-flags`, {
    params,
    timeout: 15000,
    headers: {
      [ADMIN_HEADER_NAME]: config.admin.apiToken,
      'Content-Type': 'application/json'
    }
  });
  return response.data?.flags || [];
}

function formatFlagsList(flags = [], status = null) {
  if (!flags.length) {
    return status
      ? `📋 No ${status} callers found.`
      : '📋 No caller flags found yet.';
  }

  const lines = flags.map((flag, index) => {
    const phone = escapeMarkdown(flag.phone_number || 'unknown');
    const statusLabel = formatStatusLabel(flag.status);
    const updatedAt = flag.updated_at ? new Date(flag.updated_at).toLocaleString() : 'unknown';
    const note = flag.note ? ` — ${escapeMarkdown(String(flag.note))}` : '';
    return `${index + 1}. ${phone} • ${statusLabel} • ${escapeMarkdown(updatedAt)}${note}`;
  });

  const header = status
    ? `📋 Caller Flags (${status})`
    : '📋 Caller Flags';
  return `${header}\n\n${lines.join('\n')}`;
}

async function sendCallerFlagsList(ctx, { status = null, limit = DEFAULT_LIMIT } = {}) {
  const safeStatus = status ? normalizeStatusInput(status) : null;
  const safeLimit = Number.isFinite(Number(limit)) ? Math.max(1, Math.min(100, Number(limit))) : DEFAULT_LIMIT;
  const flags = await fetchCallerFlags({
    ...(safeStatus ? { status: safeStatus } : {}),
    limit: safeLimit
  });
  const message = formatFlagsList(flags, safeStatus);
  await ctx.reply(message, { parse_mode: 'Markdown' });
}

async function upsertCallerFlag(ctx, { phone, status, note } = {}) {
  const normalizedStatus = String(status || '').toLowerCase();
  if (!STATUS_OPTIONS.includes(normalizedStatus)) {
    throw new Error('Status must be allowed, blocked, or spam');
  }
  const normalizedPhone = normalizePhoneInput(phone);
  if (!normalizedPhone) {
    throw new Error('Phone number is required');
  }

  const response = await httpClient.post(
    null,
    `${config.apiUrl}/api/caller-flags`,
    {
      phone_number: normalizedPhone,
      status: normalizedStatus,
      ...(note ? { note } : {})
    },
    {
      timeout: 15000,
      headers: {
        [ADMIN_HEADER_NAME]: config.admin.apiToken,
        'Content-Type': 'application/json'
      }
    }
  );

  return response.data?.flag || {
    phone_number: normalizedPhone,
    status: normalizedStatus,
    note: note || null
  };
}

function parseCallerFlagsArgs(args = []) {
  if (!args.length) return { action: null };
  const action = String(args[0] || '').toLowerCase();
  const rest = args.slice(1);
  return { action, rest };
}

async function handleCallerFlagsCommand(ctx) {
  try {
    const { isAdminUser } = await ensureAuthorizedAdmin(ctx);
    if (!isAdminUser) return;

    const text = ctx.message?.text || '';
    const args = text.split(/\s+/).slice(1);
    const { action, rest } = parseCallerFlagsArgs(args);

    if (!action) {
      await renderCallerFlagsMenu(ctx);
      return;
    }

    if (action === 'list') {
      let statusCandidate = rest[0];
      let limitCandidate = rest[1];
      if (statusCandidate && /^\d+$/.test(statusCandidate)) {
        limitCandidate = statusCandidate;
        statusCandidate = null;
      }
      if (statusCandidate && !normalizeStatusInput(statusCandidate)) {
        await ctx.reply('❌ Status must be blocked, allowed, or spam.');
        return;
      }
      await sendCallerFlagsList(ctx, {
        status: statusCandidate,
        limit: limitCandidate
      });
      return;
    }

    if (['allow', 'block', 'spam'].includes(action)) {
      const phone = rest[0];
      const note = rest.slice(1).join(' ').trim();
      if (phone) {
        const status = normalizeStatusInput(action);
        const flag = await upsertCallerFlag(ctx, { phone, status, note });
        await ctx.reply(
          `✅ Updated ${escapeMarkdown(flag.phone_number)} as ${formatStatusLabel(flag.status)}.`,
          { parse_mode: 'Markdown' }
        );
        return;
      }

      const flowName = action === 'allow' ? 'callerflag-allow-conversation'
        : action === 'block' ? 'callerflag-block-conversation'
        : 'callerflag-spam-conversation';
      startOperation(ctx, `callerflags_${action}`);
      await ctx.reply(`Starting ${action} flow...`);
      await ctx.conversation.enter(flowName);
      return;
    }

    await ctx.reply(
      'Usage:\n' +
      '• /callerflags list [blocked|allowed|spam] [limit]\n' +
      '• /callerflags allow <phone> [note]\n' +
      '• /callerflags block <phone> [note]\n' +
      '• /callerflags spam <phone> [note]',
      { parse_mode: 'Markdown' }
    );
  } catch (error) {
    console.error('Caller flags command error:', error);
    await ctx.reply(httpClient.getUserMessage(error, 'Unable to manage caller flags. Please try again.'));
  }
}

function createCallerFlagFlow(status) {
  return async function callerFlagFlow(conversation, ctx) {
    try {
      const { isAdminUser } = await ensureAuthorizedAdmin(ctx);
      if (!isAdminUser) return;

      await ctx.reply('📞 Enter the caller phone number:');
      const phoneMsg = await conversation.wait();
      const phoneText = phoneMsg?.message?.text?.trim();
      if (phoneText) {
        await guardAgainstCommandInterrupt(ctx, phoneText);
      }
      const normalizedPhone = normalizePhoneInput(phoneText);
      if (!normalizedPhone) {
        await ctx.reply('❌ Please provide a valid phone number.');
        return;
      }

      await ctx.reply('📝 Optional note (or type skip):');
      const noteMsg = await conversation.wait();
      const noteText = noteMsg?.message?.text?.trim();
      if (noteText) {
        await guardAgainstCommandInterrupt(ctx, noteText);
      }
      const note = noteText && noteText.toLowerCase() !== 'skip'
        ? noteText
        : null;

      const flag = await upsertCallerFlag(ctx, { phone: normalizedPhone, status, note });
      await ctx.reply(
        `✅ Updated ${escapeMarkdown(flag.phone_number)} as ${formatStatusLabel(flag.status)}.`,
        { parse_mode: 'Markdown' }
      );
    } catch (error) {
      if (error instanceof OperationCancelledError) {
        console.log('Caller flag flow cancelled');
        return;
      }
      console.error('Caller flag flow error:', error);
      await ctx.reply('❌ Failed to update caller flag. Please try again.');
    }
  };
}

const callerFlagAllowFlow = createCallerFlagFlow('allowed');
const callerFlagBlockFlow = createCallerFlagFlow('blocked');
const callerFlagSpamFlow = createCallerFlagFlow('spam');

function registerCallerFlagsCommand(bot) {
  bot.command('callerflags', handleCallerFlagsCommand);
}

module.exports = {
  registerCallerFlagsCommand,
  renderCallerFlagsMenu,
  sendCallerFlagsList,
  callerFlagAllowFlow,
  callerFlagBlockFlow,
  callerFlagSpamFlow
};
