const { InlineKeyboard } = require('grammy');
const fs = require('fs');
const path = require('path');
const config = require('../config');
const { getUser, isAdmin } = require('../db/db');
const { buildCallbackData } = require('./actions');
const { ensureSession } = require('./sessionState');

const DENY_WINDOW_MS = 60 * 1000;
const DENY_MAX_ATTEMPTS = 5;
const COOLDOWN_MS = 30 * 1000;
const denyBuckets = new Map();
const deniedEvents = [];
const lastCooldownNotice = new Map();
const LOG_DIR = path.resolve(__dirname, '../logs');
const DENIED_LOG_PATH = path.join(LOG_DIR, 'access-denied.log');
let logDirReady = false;
const ACCESS_CACHE_TTL_MS = 30 * 1000;

const CAPABILITY_RULES = {
  view_menu: ['guest', 'user', 'admin'],
  view_help: ['guest', 'user', 'admin'],
  view_guide: ['guest', 'user', 'admin'],
  view_sms_menu: ['guest', 'user', 'admin'],
  view_email_menu: ['guest', 'user', 'admin'],
  view_calllog_menu: ['guest', 'user', 'admin'],
  call: ['user', 'admin'],
  calllog_view: ['user', 'admin'],
  sms_send: ['user', 'admin'],
  sms_status: ['user', 'admin'],
  sms_schedule: ['user', 'admin'],
  email_send: ['user', 'admin'],
  email_status: ['user', 'admin'],
  health: ['user', 'admin'],
  bulk_sms: ['admin'],
  bulk_email: ['admin'],
  sms_admin: ['admin'],
  email_templates: ['admin'],
  email_history: ['admin'],
  scripts_manage: ['admin'],
  persona_manage: ['admin'],
  provider_manage: ['admin'],
  users_manage: ['admin'],
  status_admin: ['admin'],
  caller_flags_manage: ['admin']
};

const COMMAND_CAPABILITIES = {
  help: 'view_help',
  guide: 'view_guide',
  menu: 'view_menu',
  call: 'call',
  calllog: 'view_calllog_menu',
  sms: 'view_sms_menu',
  email: 'view_email_menu',
  smssender: 'bulk_sms',
  mailer: 'bulk_email',
  scripts: 'scripts_manage',
  persona: 'persona_manage',
  provider: 'provider_manage',
  users: 'users_manage',
  callerflags: 'caller_flags_manage',
  status: 'status_admin',
  health: 'health',
  ping: 'health'
};

const ACTION_CAPABILITIES = [
  { match: (action) => action === 'HELP', cap: 'view_help' },
  { match: (action) => action === 'GUIDE', cap: 'view_guide' },
  { match: (action) => action === 'MENU', cap: 'view_menu' },
  { match: (action) => action === 'HEALTH', cap: 'health' },
  { match: (action) => action === 'STATUS', cap: 'status_admin' },
  { match: (action) => action === 'CALL', cap: 'call' },
  { match: (action) => action === 'CALLLOG', cap: 'view_calllog_menu' },
  { match: (action) => ['CALLLOG_RECENT', 'CALLLOG_SEARCH', 'CALLLOG_DETAILS', 'CALLLOG_EVENTS'].includes(action), cap: 'calllog_view' },
  { match: (action) => action === 'SMS', cap: 'view_sms_menu' },
  { match: (action) => action === 'EMAIL', cap: 'view_email_menu' },
  { match: (action) => ['SMS_SEND', 'SMS_SCHEDULE'].includes(action), cap: 'sms_send' },
  { match: (action) => action === 'SMS_STATUS', cap: 'sms_status' },
  { match: (action) => ['SMS_CONVO', 'SMS_RECENT', 'SMS_STATS', 'RECENT_SMS'].includes(action), cap: 'sms_admin' },
  { match: (action) => action === 'EMAIL_SEND', cap: 'email_send' },
  { match: (action) => action === 'EMAIL_STATUS', cap: 'email_status' },
  { match: (action) => action === 'EMAIL_TEMPLATES', cap: 'email_templates' },
  { match: (action) => action === 'EMAIL_HISTORY', cap: 'email_history' },
  { match: (action) => action === 'BULK_SMS', cap: 'bulk_sms' },
  { match: (action) => action === 'BULK_EMAIL', cap: 'bulk_email' },
  { match: (action) => action.startsWith('BULK_SMS_'), cap: 'bulk_sms' },
  { match: (action) => action.startsWith('BULK_EMAIL_'), cap: 'bulk_email' },
  { match: (action) => action === 'SCRIPTS', cap: 'scripts_manage' },
  { match: (action) => action === 'PERSONA', cap: 'persona_manage' },
  { match: (action) => action === 'PROVIDER_STATUS', cap: 'provider_manage' },
  { match: (action) => action.startsWith('PROVIDER_SET:'), cap: 'provider_manage' },
  { match: (action) => ['USERS', 'USERS_LIST', 'ADDUSER', 'PROMOTE', 'REMOVE'].includes(action), cap: 'users_manage' },
  { match: (action) => ['CALLER_FLAGS', 'CALLER_FLAGS_LIST', 'CALLER_FLAGS_ALLOW', 'CALLER_FLAGS_BLOCK', 'CALLER_FLAGS_SPAM'].includes(action), cap: 'caller_flags_manage' },
  { match: (action) => action.startsWith('CALL_DETAILS:'), cap: 'calllog_view' },
  { match: (action) => action.startsWith('EMAIL_STATUS:'), cap: 'email_status' },
  { match: (action) => action.startsWith('EMAIL_TIMELINE:'), cap: 'email_status' },
  { match: (action) => action.startsWith('EMAIL_BULK:'), cap: 'bulk_email' },
  { match: (action) => action.startsWith('lc:'), cap: 'calllog_view' }
];

function resolveRole(user) {
  if (!user) return 'guest';
  return user.role === 'ADMIN' ? 'admin' : 'user';
}

function isRoleAllowed(role, capability) {
  if (!capability) return true;
  const allowed = CAPABILITY_RULES[capability];
  if (!Array.isArray(allowed)) {
    return true;
  }
  return allowed.includes(role);
}

async function getAccessProfile(ctx) {
  ensureSession(ctx);
  const cached = ctx.session.accessProfile;
  if (cached && Date.now() - cached.checkedAt < ACCESS_CACHE_TTL_MS) {
    return cached;
  }
  const user = await new Promise((resolve) => getUser(ctx.from?.id, resolve));
  let role = resolveRole(user);
  if (user && role !== 'admin') {
    const adminStatus = await new Promise((resolve) => isAdmin(ctx.from?.id, resolve));
    role = adminStatus ? 'admin' : 'user';
  }
  const profile = {
    role,
    user,
    isAuthorized: Boolean(user),
    isAdmin: role === 'admin',
    checkedAt: Date.now()
  };
  ctx.session.accessProfile = profile;
  return profile;
}

function getCapabilityForCommand(command = '') {
  return COMMAND_CAPABILITIES[String(command || '').toLowerCase()] || null;
}

function getCapabilityForAction(action = '') {
  const raw = String(action || '');
  for (const entry of ACTION_CAPABILITIES) {
    if (entry.match(raw)) {
      return entry.cap;
    }
  }
  return null;
}

function escapeHtml(text = '') {
  return String(text)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

function formatActionLabel(actionLabel = '') {
  const raw = String(actionLabel || '').trim();
  if (!raw) return '';
  return raw.replace(/_/g, ' ');
}

function buildAccessKeyboard(ctx) {
  const adminUsername = (config.admin.username || '').replace(/^@/, '');
  const keyboard = new InlineKeyboard()
    .text('⬅️ Main Menu', buildCallbackData(ctx, 'MENU'));
  if (adminUsername) {
    keyboard.row().url('📱 Request Access', `https://t.me/${adminUsername}`);
  }
  return keyboard;
}

function recordDeniedAttempt(userId, meta = {}) {
  const now = Date.now();
  const bucket = denyBuckets.get(userId) || { count: 0, resetAt: now + DENY_WINDOW_MS };
  if (now > bucket.resetAt) {
    bucket.count = 0;
    bucket.resetAt = now + DENY_WINDOW_MS;
  }
  bucket.count += 1;
  denyBuckets.set(userId, bucket);

  deniedEvents.push({
    userId,
    role: meta.role || 'unknown',
    capability: meta.capability || 'unknown',
    actionLabel: meta.actionLabel || meta.action || 'unknown',
    timestamp: now
  });
  if (deniedEvents.length > 50) {
    deniedEvents.splice(0, deniedEvents.length - 50);
  }

  appendDeniedLog({
    timestamp: new Date(now).toISOString(),
    user: maskUserId(userId),
    role: meta.role || 'unknown',
    capability: meta.capability || 'unknown',
    action: meta.actionLabel || meta.action || 'unknown'
  });

  return bucket;
}

function isRateLimited(userId) {
  if (!userId) return false;
  const bucket = denyBuckets.get(userId);
  if (!bucket) return false;
  if (Date.now() > bucket.resetAt) {
    denyBuckets.delete(userId);
    return false;
  }
  return bucket.count > DENY_MAX_ATTEMPTS;
}

function getDeniedAuditSummary() {
  const now = Date.now();
  let total = 0;
  let users = 0;
  let rateLimited = 0;

  for (const [userId, bucket] of denyBuckets.entries()) {
    if (!bucket || !bucket.resetAt || now > bucket.resetAt) {
      denyBuckets.delete(userId);
      continue;
    }
    users += 1;
    total += bucket.count || 0;
    if (bucket.count > DENY_MAX_ATTEMPTS) {
      rateLimited += 1;
    }
  }

  return {
    total,
    users,
    rateLimited,
    windowSeconds: Math.round(DENY_WINDOW_MS / 1000),
    recent: deniedEvents.slice(-5).reverse()
  };
}

async function sendAccessDenied(ctx, capability, options = {}) {
  const actionText = formatActionLabel(options.actionLabel);
  const lines = [
    '<b>🔒 Access Required</b>',
    'This action is not available for your current role.',
    'You can still use <b>Menu</b>, <b>Guide</b>, and <b>Help</b> while waiting for approval.'
  ];
  if (actionText) {
    lines.push(`<b>Action:</b> <code>${escapeHtml(actionText)}</code>`);
  }
  await ctx.reply(lines.join('\n\n'), {
    parse_mode: 'HTML',
    reply_markup: buildAccessKeyboard(ctx)
  });
}

async function requireCapability(ctx, capability, options = {}) {
  if (!capability) return true;
  const profile = options.profile || await getAccessProfile(ctx);
  if (isRoleAllowed(profile.role, capability)) {
    return true;
  }
  const userId = ctx.from?.id || 'unknown';
  const bucket = recordDeniedAttempt(userId, {
    role: profile.role,
    capability,
    actionLabel: options.actionLabel,
    action: options.action
  });
  console.warn(`Access denied for user ${userId} (${profile.role}) on capability ${capability}${options.action ? ` via ${options.action}` : ''}`);
  if (isRateLimited(userId)) {
    await ctx.reply('⏳ Too many access attempts. Please wait a moment and try again.', {
      parse_mode: 'HTML',
      reply_markup: buildAccessKeyboard(ctx)
    });
    return false;
  }
  if (bucket?.count >= 2) {
    const lastNotice = lastCooldownNotice.get(userId) || 0;
    if (Date.now() - lastNotice > COOLDOWN_MS) {
      lastCooldownNotice.set(userId, Date.now());
      await ctx.reply('⏳ Please wait 30 seconds before retrying locked actions.', {
        parse_mode: 'HTML',
        reply_markup: buildAccessKeyboard(ctx)
      });
      return false;
    }
  }
  await sendAccessDenied(ctx, capability, { actionLabel: options.actionLabel });
  return false;
}

module.exports = {
  getAccessProfile,
  getCapabilityForCommand,
  getCapabilityForAction,
  requireCapability,
  getDeniedAuditSummary
};
function maskUserId(userId) {
  if (userId === undefined || userId === null) return 'unknown';
  const text = String(userId);
  if (text.length <= 4) return text;
  return `***${text.slice(-4)}`;
}

function appendDeniedLog(entry) {
  try {
    if (!logDirReady) {
      fs.mkdirSync(LOG_DIR, { recursive: true });
      logDirReady = true;
    }
    fs.appendFile(DENIED_LOG_PATH, `${JSON.stringify(entry)}\n`, () => {});
  } catch (error) {
    console.warn('Failed to append access-denied log:', error?.message || error);
  }
}
