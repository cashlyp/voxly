const crypto = require('crypto');
const config = require('../config');
const { ensureSession } = require('./sessionState');

const DEFAULT_CALLBACK_TTL_MS = 15 * 60 * 1000;
const DEFAULT_DEDUPE_TTL_MS = 8000;
const SIGN_PREFIX = 'cb';
const LEGACY_OP_TOKEN_PATTERN = /^[0-9a-zA-Z-]{8,}$/;
const MENU_ACTION_LOG_INTERVAL = 25;
const MENU_ACTIONS = new Set([
  'CALL',
  'CALLLOG',
  'CALLLOG_RECENT',
  'CALLLOG_SEARCH',
  'CALLLOG_DETAILS',
  'CALLLOG_EVENTS',
  'SMS',
  'SMS_SEND',
  'SMS_SCHEDULE',
  'SMS_STATUS',
  'SMS_CONVO',
  'SMS_RECENT',
  'SMS_STATS',
  'EMAIL',
  'EMAIL_SEND',
  'EMAIL_STATUS',
  'EMAIL_TEMPLATES',
  'EMAIL_HISTORY',
  'BULK_SMS',
  'BULK_SMS_SEND',
  'BULK_SMS_LIST',
  'BULK_SMS_STATUS',
  'BULK_SMS_STATS',
  'BULK_EMAIL',
  'BULK_EMAIL_SEND',
  'BULK_EMAIL_STATUS',
  'BULK_EMAIL_LIST',
  'BULK_EMAIL_STATS',
  'SCRIPTS',
  'PROVIDER:HOME',
  'PROVIDER:CALL',
  'PROVIDER:SMS',
  'PROVIDER:EMAIL',
  'PROVIDER:BACK:HOME',
  'PROVIDER_STATUS:CALL',
  'PROVIDER_STATUS:SMS',
  'PROVIDER_STATUS:EMAIL',
  'PROVIDER_STATUS',
  'PROVIDER_OVERRIDES',
  'PROVIDER_CLEAR_OVERRIDES',
  'REQUEST_ACCESS',
  'STATUS',
  'USERS',
  'USERS_LIST',
  'CALLER_FLAGS',
  'CALLER_FLAGS_LIST',
  'CALLER_FLAGS_ALLOW',
  'CALLER_FLAGS_BLOCK',
  'CALLER_FLAGS_SPAM'
]);
const menuActionStats = {};
let menuActionCount = 0;

function getCallbackSecret() {
  return config.botToken || config.apiAuth?.hmacSecret || 'callback-secret';
}

function signPayload(payload) {
  const secret = getCallbackSecret();
  return crypto.createHmac('sha256', secret).update(payload).digest('hex').slice(0, 8);
}

function buildCallbackData(ctx, action, options = {}) {
  const safeAction = String(action || '');
  if (!safeAction) {
    return '';
  }
  const ttlMs = Number.isFinite(options.ttlMs) ? options.ttlMs : DEFAULT_CALLBACK_TTL_MS;
  ensureSession(ctx);
  const token = options.token || ctx.session?.currentOp?.token || '';
  const ts = options.timestamp || Date.now();
  const payload = `${safeAction}|${token}|${ts}`;
  const sig = signPayload(payload);
  const data = `${SIGN_PREFIX}|${safeAction}|${token}|${ts}|${sig}`;
  if (data.length > 64) {
    return safeAction;
  }
  if (ttlMs <= 0) {
    return safeAction;
  }
  return data;
}

function parseCallbackData(rawAction) {
  const text = String(rawAction || '');
  if (!text.startsWith(`${SIGN_PREFIX}|`)) {
    return { action: text, signed: false };
  }
  const parts = text.split('|');
  if (parts.length < 5) {
    return { action: text, signed: true, valid: false, reason: 'format' };
  }
  const [, action, token, ts, sig] = parts;
  const payload = `${action}|${token}|${ts}`;
  const expected = signPayload(payload);
  const timestamp = Number(ts);
  return {
    action,
    signed: true,
    token,
    timestamp,
    valid: sig === expected,
    reason: sig === expected ? null : 'signature'
  };
}

function extractLegacyOpToken(rawAction) {
  const parts = String(rawAction || '').split(':');
  if (parts.length < 2) {
    return null;
  }
  const candidate = parts[1];
  if (LEGACY_OP_TOKEN_PATTERN.test(candidate)) {
    return candidate.replace(/-/g, '').slice(0, 8);
  }
  return null;
}

function validateCallback(ctx, rawAction, options = {}) {
  ensureSession(ctx);
  void options;
  const parsed = parseCallbackData(rawAction);

  if (parsed.signed) {
    if (!parsed.valid) {
      return { status: 'invalid', reason: parsed.reason, action: parsed.action };
    }
    return { status: 'ok', action: parsed.action };
  }

  void extractLegacyOpToken(rawAction);
  return { status: 'ok', action: parsed.action };
}

function matchesCallbackPrefix(rawAction, prefix) {
  const parsed = parseCallbackData(rawAction);
  const action = parsed.action || '';
  return action === prefix || action.startsWith(`${prefix}:`);
}

function cleanupHistory(history, ttlMs) {
  const now = Date.now();
  Object.entries(history).forEach(([key, timestamp]) => {
    if (!timestamp || now - timestamp > ttlMs) {
      delete history[key];
    }
  });
}

function isDuplicateAction(ctx, key, ttlMs = DEFAULT_DEDUPE_TTL_MS) {
  ensureSession(ctx);
  if (!key) return false;
  const history = ctx.session.actionHistory || {};
  cleanupHistory(history, ttlMs);
  const now = Date.now();
  const lastSeen = history[key];
  if (lastSeen && now - lastSeen < ttlMs) {
    return true;
  }
  history[key] = now;
  ctx.session.actionHistory = history;
  return false;
}

function startActionMetric(ctx, name, meta = {}) {
  ensureSession(ctx);
  return {
    name,
    start: Date.now(),
    userId: ctx.from?.id || null,
    chatId: ctx.chat?.id || ctx.callbackQuery?.message?.chat?.id || null,
    opId: ctx.session?.currentOp?.id || null,
    ...meta
  };
}

function finishActionMetric(metric, status = 'ok', extra = {}) {
  if (!metric) return;
  const durationMs = Date.now() - metric.start;
  const payload = {
    type: 'metric',
    action: metric.name,
    status,
    duration_ms: durationMs,
    user_id: metric.userId,
    chat_id: metric.chatId,
    op_id: metric.opId,
    ...extra
  };
  console.log(JSON.stringify(payload));
  if (metric?.name === 'callback' && metric?.raw_action) {
    const parsed = parseCallbackData(metric.raw_action);
    const action = parsed.action || metric.raw_action;
    if (MENU_ACTIONS.has(action)) {
      const entry = menuActionStats[action] || { total: 0, error: 0 };
      entry.total += 1;
      if (status !== 'ok') {
        entry.error += 1;
      }
      menuActionStats[action] = entry;
      menuActionCount += 1;
      if (menuActionCount % MENU_ACTION_LOG_INTERVAL === 0) {
        const summary = Object.entries(menuActionStats)
          .map(([key, value]) => ({
            action: key,
            total: value.total,
            errorRate: value.total ? Math.round((value.error / value.total) * 100) : 0
          }))
          .sort((a, b) => b.errorRate - a.errorRate)
          .slice(0, 5)
          .map((row) => `${row.action}: ${row.errorRate}% (${row.total})`)
          .join(' | ');
        console.log(`📊 Menu action health: ${summary}`);
      }
    }
  }
}

module.exports = {
  buildCallbackData,
  parseCallbackData,
  validateCallback,
  matchesCallbackPrefix,
  isDuplicateAction,
  startActionMetric,
  finishActionMetric
};
