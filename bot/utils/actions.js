const { ensureSession } = require('./sessionState');

const DEFAULT_DEDUPE_TTL_MS = 8000;
const SIGN_PREFIX = 'cb';
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

function buildCallbackData(_ctx, action, _options = {}) {
  const safeAction = String(action || '');
  if (!safeAction) {
    return '';
  }
  return safeAction;
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
  const timestamp = Number(ts);
  return {
    action,
    signed: true,
    token,
    timestamp,
    valid: Boolean(sig),
    reason: null
  };
}

function validateCallback(ctx, rawAction, _options = {}) {
  ensureSession(ctx);
  const parsed = parseCallbackData(rawAction);
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
