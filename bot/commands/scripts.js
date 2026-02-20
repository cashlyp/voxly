const axios = require('axios');
const config = require('../config');
const httpClient = require('../utils/httpClient');
const { withRetry } = require('../utils/httpClient');
const {
  getUser,
  isAdmin,
  saveScriptVersion,
  listScriptVersions,
  getScriptVersion
} = require('../db/db');
const {
  getBusinessOptions,
  findBusinessOption,
  MOOD_OPTIONS,
  URGENCY_OPTIONS,
  TECH_LEVEL_OPTIONS,
  askOptionWithButtons,
  getOptionLabel,
  invalidatePersonaCache
} = require('../utils/persona');
const { extractScriptVariables } = require('../utils/scripts');
const {
  startOperation,
  ensureOperationActive,
  OperationCancelledError,
  getCurrentOpId,
  waitForConversationText
} = require('../utils/sessionState');
const { emailTemplatesFlow } = require('./email');
const { section, buildLine, tipLine } = require('../utils/ui');
const { attachHmacAuth } = require('../utils/apiAuth');

const CLONE_REQUEST_TTL_MS = 30000;
const cloneRequestState = new Map();
const MUTATION_REQUEST_TTL_MS = 15000;
const mutationRequestState = new Map();
const SCRIPT_ACTION_PROGRESS_MS = 5000;
const SCRIPT_ACTION_TIMEOUT_MS = 20000;
const SCRIPTS_CIRCUIT_FAILURE_THRESHOLD = 3;
const SCRIPTS_CIRCUIT_OPEN_MS = 30000;
const SCRIPTS_METRIC_WINDOW_MS = 5 * 60 * 1000;
const SCRIPTS_ALERT_COOLDOWN_MS = 60 * 1000;
const scriptsCircuitState = {
  state: 'closed',
  consecutiveFailures: 0,
  openedAt: 0,
  halfOpenInFlight: false,
  lastError: null
};
const scriptsMetricsState = {
  windowStart: Date.now(),
  counts: {
    total: 0,
    ok: 0,
    error: 0,
    timeout: 0,
    duplicate_block: 0,
    circuit_open: 0
  },
  actions: {},
  lastAlertAt: 0
};

const scriptsApi = axios.create({
  baseURL: config.scriptsApiUrl.replace(/\/+$/, ''),
  timeout: 15000,
  headers: {
    'Content-Type': 'application/json',
    'x-admin-token': config.admin.apiToken
  }
});
const scriptsApiExecutorState = {
  request: (requestOptions) => scriptsApi.request(requestOptions)
};

attachHmacAuth(scriptsApi, {
  secret: config.apiAuth?.hmacSecret,
  allowedOrigins: [new URL(config.scriptsApiUrl).origin],
  defaultBaseUrl: config.scriptsApiUrl
});

function toPositiveInt(value, fallback) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback;
  }
  return Math.round(parsed);
}

function resetScriptsMetricsWindow(now = Date.now()) {
  scriptsMetricsState.windowStart = now;
  scriptsMetricsState.counts = {
    total: 0,
    ok: 0,
    error: 0,
    timeout: 0,
    duplicate_block: 0,
    circuit_open: 0
  };
  scriptsMetricsState.actions = {};
}

function maybeEmitScriptsAlert(now = Date.now()) {
  const counts = scriptsMetricsState.counts;
  if (now - scriptsMetricsState.lastAlertAt < SCRIPTS_ALERT_COOLDOWN_MS) {
    return;
  }
  if (counts.total < 5) return;
  const errorRate = counts.total > 0 ? counts.error / counts.total : 0;
  const shouldAlert =
    errorRate >= 0.4 ||
    counts.timeout >= 3 ||
    counts.circuit_open >= 2 ||
    counts.duplicate_block >= 10;
  if (!shouldAlert) {
    return;
  }
  scriptsMetricsState.lastAlertAt = now;
  console.warn('[scripts][alert] degraded command health', {
    window_ms: SCRIPTS_METRIC_WINDOW_MS,
    counts: { ...counts }
  });
}

function recordScriptsMetric(action, status, durationMs = 0, meta = {}) {
  const now = Date.now();
  if (now - scriptsMetricsState.windowStart > SCRIPTS_METRIC_WINDOW_MS) {
    resetScriptsMetricsWindow(now);
  }
  const safeAction = String(action || 'unknown').trim();
  const safeStatus = String(status || 'unknown').trim();
  scriptsMetricsState.counts.total += 1;
  if (safeStatus === 'ok') scriptsMetricsState.counts.ok += 1;
  if (safeStatus === 'error') scriptsMetricsState.counts.error += 1;
  if (safeStatus === 'timeout') scriptsMetricsState.counts.timeout += 1;
  if (safeStatus === 'duplicate_block') scriptsMetricsState.counts.duplicate_block += 1;
  if (safeStatus === 'circuit_open') scriptsMetricsState.counts.circuit_open += 1;

  const actionState = scriptsMetricsState.actions[safeAction] || {
    count: 0,
    ok: 0,
    error: 0,
    timeout: 0
  };
  actionState.count += 1;
  if (safeStatus === 'ok') actionState.ok += 1;
  if (safeStatus === 'error') actionState.error += 1;
  if (safeStatus === 'timeout') actionState.timeout += 1;
  scriptsMetricsState.actions[safeAction] = actionState;

  if (meta.log) {
    console.log('[scripts][metric]', {
      action: safeAction,
      status: safeStatus,
      duration_ms: Number.isFinite(durationMs) ? Math.round(durationMs) : null
    });
  }
  maybeEmitScriptsAlert(now);
}

function shouldTripScriptsCircuit(error) {
  const code = String(error?.code || '').toUpperCase();
  if (['ECONNRESET', 'ECONNABORTED', 'ETIMEDOUT', 'ENOTFOUND', 'ECONNREFUSED'].includes(code)) {
    return true;
  }
  if (error?.isScriptsApiError && error?.reason === 'non_json_response') {
    return true;
  }
  const status = Number(error?.response?.status || 0);
  return status >= 500;
}

function assertScriptsCircuitHealthy() {
  if (scriptsCircuitState.state !== 'open') return;
  const now = Date.now();
  const openMs = toPositiveInt(config?.scripts?.circuitOpenMs, SCRIPTS_CIRCUIT_OPEN_MS);
  if (now - scriptsCircuitState.openedAt >= openMs) {
    scriptsCircuitState.state = 'half_open';
    scriptsCircuitState.halfOpenInFlight = false;
    return;
  }
  const err = new Error('Scripts API circuit is open. Please retry shortly.');
  err.code = 'scripts_circuit_open';
  err.isScriptsApiError = true;
  throw err;
}

function markScriptsCircuitSuccess() {
  scriptsCircuitState.state = 'closed';
  scriptsCircuitState.consecutiveFailures = 0;
  scriptsCircuitState.openedAt = 0;
  scriptsCircuitState.halfOpenInFlight = false;
  scriptsCircuitState.lastError = null;
}

function markScriptsCircuitFailure(error) {
  if (!shouldTripScriptsCircuit(error)) return;
  scriptsCircuitState.consecutiveFailures += 1;
  scriptsCircuitState.lastError = String(error?.message || 'scripts_api_error');
  const threshold = toPositiveInt(
    config?.scripts?.circuitFailureThreshold,
    SCRIPTS_CIRCUIT_FAILURE_THRESHOLD
  );
  if (scriptsCircuitState.state === 'half_open' || scriptsCircuitState.consecutiveFailures >= threshold) {
    scriptsCircuitState.state = 'open';
    scriptsCircuitState.openedAt = Date.now();
    scriptsCircuitState.halfOpenInFlight = false;
    console.warn('[scripts] circuit opened', {
      failures: scriptsCircuitState.consecutiveFailures,
      last_error: scriptsCircuitState.lastError
    });
  }
}

async function runWithActionWatchdog(ctx, actionLabel, actionName, operation) {
  const progressMs = toPositiveInt(config?.scripts?.actionProgressMs, SCRIPT_ACTION_PROGRESS_MS);
  const timeoutMs = toPositiveInt(config?.scripts?.actionTimeoutMs, SCRIPT_ACTION_TIMEOUT_MS);
  const startedAt = Date.now();
  let progressTimer = null;
  let timeoutTimer = null;
  let completed = false;

  const timeoutPromise = new Promise((_, reject) => {
    timeoutTimer = setTimeout(() => {
      if (completed) return;
      const err = new Error(`${actionLabel} timed out`);
      err.code = 'script_action_timeout';
      reject(err);
    }, timeoutMs);
  });

  progressTimer = setTimeout(() => {
    if (completed) return;
    ctx.reply(`⏳ ${actionLabel} is taking longer than expected...`).catch(() => {});
  }, progressMs);

  try {
    const result = await Promise.race([operation(), timeoutPromise]);
    completed = true;
    recordScriptsMetric(actionName, 'ok', Date.now() - startedAt);
    return result;
  } catch (error) {
    completed = true;
    if (error?.code === 'script_action_timeout') {
      recordScriptsMetric(actionName, 'timeout', Date.now() - startedAt);
    } else {
      recordScriptsMetric(actionName, 'error', Date.now() - startedAt);
    }
    throw error;
  } finally {
    if (progressTimer) clearTimeout(progressTimer);
    if (timeoutTimer) clearTimeout(timeoutTimer);
  }
}

function styledNotice(ctx, title, lines) {
  const content = Array.isArray(lines) ? lines : [lines];
  return ctx.reply(section(title, content));
}

function nonJsonResponseError(endpoint, response) {
  const contentType = response?.headers?.['content-type'] || 'unknown';
  const snippet =
    typeof response?.data === 'string'
      ? response.data.replace(/\s+/g, ' ').trim().slice(0, 140)
      : '';
  const error = new Error(
    `Scripts API returned non-JSON response (content-type: ${contentType})`
  );
  error.isScriptsApiError = true;
  error.reason = 'non_json_response';
  error.endpoint = endpoint;
  error.contentType = contentType;
  error.snippet = snippet;
  return error;
}

async function scriptsApiRequest(options) {
  const method = `${(options.method || 'GET').toUpperCase()}`;
  const endpoint = `${method} ${options.url}`;
  const { retry, ...requestOptions } = options || {};
  const retryOptions = retry !== undefined ? retry : (method === 'GET' ? {} : { retries: 0 });
  const startedAt = Date.now();
  try {
    assertScriptsCircuitHealthy();
  } catch (circuitError) {
    recordScriptsMetric(endpoint, 'circuit_open', 0);
    throw circuitError;
  }
  const halfOpenRequest = scriptsCircuitState.state === 'half_open';
  if (halfOpenRequest && scriptsCircuitState.halfOpenInFlight) {
    const err = new Error('Scripts API circuit is probing recovery. Please retry shortly.');
    err.code = 'scripts_circuit_open';
    err.isScriptsApiError = true;
    recordScriptsMetric(endpoint, 'circuit_open', 0);
    throw err;
  }
  if (halfOpenRequest) {
    scriptsCircuitState.halfOpenInFlight = true;
  }
  try {
    const response = await withRetry(() => scriptsApiExecutorState.request(requestOptions), retryOptions || {});
    const status = Number(response?.status || 0);
    if (status === 204 || status === 205) {
      markScriptsCircuitSuccess();
      recordScriptsMetric(endpoint, 'ok', Date.now() - startedAt);
      return { success: true };
    }
    const contentType = response.headers?.['content-type'] || '';
    const isEmptyBody =
      response.data === undefined ||
      response.data === null ||
      response.data === '';
    if (!contentType.includes('application/json') && isEmptyBody) {
      markScriptsCircuitSuccess();
      recordScriptsMetric(endpoint, 'ok', Date.now() - startedAt);
      return { success: true };
    }
    if (!contentType.includes('application/json')) {
      throw nonJsonResponseError(endpoint, response);
    }
    if (!response.data || typeof response.data !== 'object') {
      return { success: true };
    }
    if (response.data && response.data.success === false) {
      const apiError = new Error(response.data.error || 'Scripts API reported failure');
      apiError.isScriptsApiError = true;
      apiError.reason = 'api_failure';
      apiError.endpoint = endpoint;
      throw apiError;
    }
    markScriptsCircuitSuccess();
    recordScriptsMetric(endpoint, 'ok', Date.now() - startedAt);
    return response.data;
  } catch (error) {
    if (error?.code === 'scripts_circuit_open') {
      throw error;
    }
    markScriptsCircuitFailure(error);
    const status = Number(error?.response?.status || 0);
    const isTimeout = error?.code === 'ECONNABORTED' || /timeout/i.test(String(error?.message || ''));
    const metricStatus = isTimeout ? 'timeout' : (status >= 500 || shouldTripScriptsCircuit(error) ? 'error' : 'error');
    recordScriptsMetric(endpoint, metricStatus, Date.now() - startedAt);
    if (error.response) {
      const contentType = error.response.headers?.['content-type'] || '';
      if (!contentType.includes('application/json')) {
        throw nonJsonResponseError(endpoint, error.response);
      }
    }
    error.scriptsApi = { endpoint };
    throw error;
  } finally {
    if (halfOpenRequest) {
      scriptsCircuitState.halfOpenInFlight = false;
    }
  }
}

function formatScriptsApiError(error, action) {
  const baseHelp = `Ensure the scripts service is reachable at ${config.scriptsApiUrl} or update SCRIPTS_API_URL.`;
  if (error?.code === 'scripts_circuit_open') {
    return `⚠️ ${action}: Scripts API is temporarily paused after repeated failures. Please retry in 30 seconds.`;
  }
  if (error?.code === 'script_action_timeout') {
    return `❌ ${action}: Operation timed out. Please retry.`;
  }
  if (error?.code === 'ECONNABORTED' || /timeout/i.test(String(error?.message || ''))) {
    return `❌ ${action}: Scripts API timed out. ${baseHelp}`;
  }

  const apiCode = error.response?.data?.code || error.code;
  if (apiCode === 'SCRIPT_NAME_DUPLICATE') {
    const suggested = error.response?.data?.suggested_name;
    const suggestionLine = suggested ? ` Suggested name: ${suggested}` : '';
    return `⚠️ ${action}: Script name already exists.${suggestionLine}`;
  }

  if (error.isScriptsApiError && error.reason === 'non_json_response') {
    return `❌ ${action}: Scripts API returned unexpected content (type: ${error.contentType}). ${baseHelp}${
      error.snippet ? `\nSnippet: ${error.snippet}` : ''
    }`;
  }

  if (error.isScriptsApiError && error.reason === 'api_failure') {
    return `❌ ${action}: ${error.message}. ${baseHelp}`;
  }

  if (error.response) {
    const status = error.response.status;
    const statusText = error.response.statusText || '';
    const details =
      error.response.data?.error ||
      error.response.data?.message ||
      error.message;

    const contentType = error.response.headers?.['content-type'] || '';
    if (!contentType.includes('application/json')) {
      const snippet =
        typeof error.response.data === 'string'
          ? error.response.data.replace(/\s+/g, ' ').trim().slice(0, 140)
          : '';
      return `❌ ${action}: Scripts API responded with HTTP ${status} ${statusText}. ${baseHelp}${
        snippet ? `\nSnippet: ${snippet}` : ''
      }`;
    }

    return `❌ ${action}: ${details || `HTTP ${status}`}`;
  }

  if (error.request) {
    return `❌ ${action}: No response from Scripts API. ${baseHelp}`;
  }

  return `❌ ${action}: ${error.message}`;
}

const CANCEL_KEYWORDS = new Set(['cancel', 'exit', 'quit']);

function isCancelInput(text) {
  return typeof text === 'string' && CANCEL_KEYWORDS.has(text.trim().toLowerCase());
}

function escapeMarkdown(text = '') {
  return text.replace(/([_*[\]`])/g, '\\$1');
}

function normalizeScriptName(name = '') {
  return String(name || '').trim().slice(0, 80);
}

function buildDigitCaptureSummary(script = {}) {
  const requiresOtp = !!script.requires_otp;
  const defaultProfile = script.default_profile;
  const expectedLength = script.expected_length;
  const parts = [];
  if (requiresOtp) parts.push('OTP required');
  if (defaultProfile) parts.push(`Profile: ${defaultProfile}`);
  if (expectedLength) parts.push(`Len: ${expectedLength}`);
  if (!parts.length) return 'None';
  return parts.join(' • ');
}

function isPaymentEnabled(value) {
  return value === true || value === 1 || String(value || '').trim().toLowerCase() === 'true';
}

function parsePositiveAmountInput(rawValue = '') {
  const cleaned = String(rawValue || '').trim().replace(/[$,\s]/g, '');
  if (!cleaned) return null;
  if (!/^\d+(\.\d{1,2})?$/.test(cleaned)) return null;
  const amount = Number(cleaned);
  if (!Number.isFinite(amount) || amount <= 0) return null;
  return amount.toFixed(2);
}

function normalizeCurrencyInput(rawValue = '') {
  const normalized = String(rawValue || '').trim().toUpperCase();
  if (!normalized) return null;
  if (!/^[A-Z]{3}$/.test(normalized)) return null;
  return normalized;
}

function normalizePaymentMessageInput(rawValue = '') {
  const normalized = String(rawValue || '').trim();
  if (!normalized) return null;
  return normalized.slice(0, 240);
}

function buildPaymentDefaultsSummary(script = {}) {
  if (!isPaymentEnabled(script.payment_enabled)) return 'Disabled';
  const connector = String(script.payment_connector || '').trim();
  const amount = String(script.payment_amount || '').trim();
  const currency = String(script.payment_currency || 'USD').trim().toUpperCase();
  const summary = [`Enabled${connector ? ` (${connector})` : ''}`];
  if (amount) {
    summary.push(`${currency || 'USD'} ${amount}`);
  } else {
    summary.push('Amount set during call');
  }
  const customMessages = [
    script.payment_start_message,
    script.payment_success_message,
    script.payment_failure_message,
    script.payment_retry_message
  ].filter((value) => String(value || '').trim()).length;
  if (customMessages > 0) {
    summary.push(`${customMessages} custom prompt${customMessages > 1 ? 's' : ''}`);
  }
  return summary.join(' • ');
}

function validateCallScriptPayload(payload = {}) {
  const errors = [];
  const warnings = [];
  const name = normalizeScriptName(payload.name);
  if (!name) {
    errors.push('Script name is required.');
  } else if (name.length < 3) {
    warnings.push('Name is very short; consider a clearer label.');
  }
  if (!payload.first_message || !String(payload.first_message).trim()) {
    errors.push('First message is required.');
  }
  if (!payload.prompt || !String(payload.prompt).trim()) {
    warnings.push('Prompt is empty; normal call flow may sound generic.');
  }
  if (payload.requires_otp) {
    const len = Number(payload.expected_length);
    if (!Number.isFinite(len) || len < 4 || len > 8) {
      errors.push('OTP length must be between 4 and 8 digits.');
    }
  }
  if (payload.default_profile && payload.expected_length) {
    const len = Number(payload.expected_length);
    if (!Number.isFinite(len) || len < 1) {
      errors.push('Expected length must be a positive number.');
    }
  }
  const paymentEnabled = isPaymentEnabled(payload.payment_enabled);
  if (paymentEnabled && !String(payload.payment_connector || '').trim()) {
    errors.push('Payment connector is required when payment defaults are enabled.');
  }
  if (payload.payment_amount !== null && payload.payment_amount !== undefined && payload.payment_amount !== '') {
    if (!parsePositiveAmountInput(payload.payment_amount)) {
      errors.push('Payment amount must be a positive number with up to 2 decimals.');
    }
  }
  if (payload.payment_currency) {
    if (!normalizeCurrencyInput(payload.payment_currency)) {
      errors.push('Payment currency must be a 3-letter code (for example USD).');
    }
  }
  ['payment_start_message', 'payment_success_message', 'payment_failure_message', 'payment_retry_message']
    .forEach((field) => {
      if (payload[field] !== undefined && payload[field] !== null) {
        const text = String(payload[field]).trim();
        if (text.length > 240) {
          errors.push(`${field} cannot exceed 240 characters.`);
        }
      }
    });
  if (paymentEnabled && !payload.payment_currency) {
    warnings.push('Payment currency not set; API will default to USD.');
  }
  if (payload.capture_group) {
    const promptText = `${payload.prompt || ''} ${payload.first_message || ''}`.toLowerCase();
    const keyword = payload.capture_group === 'banking' ? 'bank' : 'card';
    if (!promptText.includes(keyword)) {
      warnings.push(`Capture group "${payload.capture_group}" is set but the prompt does not mention "${keyword}".`);
    }
  }
  return { errors, warnings };
}

function replacePlaceholders(text = '', values = {}) {
  let output = text;
  for (const [token, value] of Object.entries(values)) {
    const pattern = new RegExp(`{${token}}`, 'g');
    output = output.replace(pattern, value);
  }
  return output;
}

function buildCallScriptSnapshot(script = {}) {
  return {
    name: script.name,
    description: script.description ?? null,
    business_id: script.business_id ?? null,
    persona_config: script.persona_config ?? null,
    prompt: script.prompt ?? null,
    first_message: script.first_message ?? null,
    voice_model: script.voice_model ?? null,
    requires_otp: !!script.requires_otp,
    default_profile: script.default_profile ?? null,
    expected_length: script.expected_length ?? null,
    allow_terminator: !!script.allow_terminator,
    terminator_char: script.terminator_char ?? null,
    payment_enabled: isPaymentEnabled(script.payment_enabled),
    payment_connector: script.payment_connector ?? null,
    payment_amount: script.payment_amount ?? null,
    payment_currency: script.payment_currency ?? null,
    payment_description: script.payment_description ?? null,
    payment_start_message: script.payment_start_message ?? null,
    payment_success_message: script.payment_success_message ?? null,
    payment_failure_message: script.payment_failure_message ?? null,
    payment_retry_message: script.payment_retry_message ?? null,
    capture_group: script.capture_group ?? null
  };
}

function buildSmsScriptSnapshot(script = {}) {
  return {
    name: script.name,
    description: script.description ?? null,
    content: script.content ?? null,
    metadata: script.metadata ?? null
  };
}

async function storeScriptVersionSnapshot(script, type, ctx) {
  try {
    const payload = type === 'sms' ? buildSmsScriptSnapshot(script) : buildCallScriptSnapshot(script);
    await saveScriptVersion(type === 'sms' ? script.name : script.id, type, payload, ctx.from?.id?.toString?.());
  } catch (error) {
    console.warn('Failed to store script version:', error?.message || error);
  }
}

function stripUndefined(payload = {}) {
  return Object.fromEntries(Object.entries(payload).filter(([, value]) => value !== undefined));
}

function resolveEnsureActive(ensureActive, ctx) {
  if (typeof ensureActive === 'function') {
    return ensureActive;
  }
  const opIdSnapshot = getCurrentOpId(ctx);
  return () => ensureOperationActive(ctx, opIdSnapshot);
}

function pruneCloneRequestState() {
  const now = Date.now();
  for (const [key, entry] of cloneRequestState.entries()) {
    const age = now - Number(entry?.timestamp || 0);
    if (!entry || age > CLONE_REQUEST_TTL_MS) {
      cloneRequestState.delete(key);
    }
  }
}

function buildCloneRequestKey(ctx, scriptId, name) {
  const userId = String(ctx?.from?.id || 'unknown');
  const safeScriptId = String(scriptId || 'unknown');
  const safeName = String(name || '').trim().toLowerCase();
  return `clone:${userId}:${safeScriptId}:${safeName}`;
}

function beginCloneRequest(ctx, scriptId, name) {
  pruneCloneRequestState();
  const key = buildCloneRequestKey(ctx, scriptId, name);
  const now = Date.now();
  const existing = cloneRequestState.get(key);
  if (existing && existing.status === 'pending') {
    recordScriptsMetric('clone_gate', 'duplicate_block', 0);
    return { allowed: false, key, reason: 'pending' };
  }
  if (existing && existing.status === 'done' && now - existing.timestamp < CLONE_REQUEST_TTL_MS) {
    recordScriptsMetric('clone_gate', 'duplicate_block', 0);
    return { allowed: false, key, reason: 'recent' };
  }
  cloneRequestState.set(key, { status: 'pending', timestamp: now });
  return { allowed: true, key };
}

function finishCloneRequest(key, status = 'done') {
  if (!key) return;
  cloneRequestState.set(key, { status, timestamp: Date.now() });
}

function pruneMutationRequestState() {
  const now = Date.now();
  for (const [key, entry] of mutationRequestState.entries()) {
    const age = now - Number(entry?.timestamp || 0);
    if (!entry || age > MUTATION_REQUEST_TTL_MS) {
      mutationRequestState.delete(key);
    }
  }
}

function buildMutationRequestKey(ctx, action, target = '') {
  const userId = String(ctx?.from?.id || 'unknown');
  const safeAction = String(action || 'unknown').trim().toLowerCase();
  const safeTarget = String(target || '').trim().toLowerCase();
  return `mut:${userId}:${safeAction}:${safeTarget}`;
}

function beginMutationRequest(ctx, action, target) {
  pruneMutationRequestState();
  const key = buildMutationRequestKey(ctx, action, target);
  const now = Date.now();
  const existing = mutationRequestState.get(key);
  if (existing && existing.status === 'pending') {
    recordScriptsMetric(action, 'duplicate_block', 0);
    return { allowed: false, key, reason: 'pending' };
  }
  if (existing && existing.status === 'done' && now - existing.timestamp < MUTATION_REQUEST_TTL_MS) {
    recordScriptsMetric(action, 'duplicate_block', 0);
    return { allowed: false, key, reason: 'recent' };
  }
  mutationRequestState.set(key, { status: 'pending', timestamp: now });
  return { allowed: true, key };
}

function finishMutationRequest(key, status = 'done') {
  if (!key) return;
  mutationRequestState.set(key, { status, timestamp: Date.now() });
}

function logScriptsError(context, error) {
  const endpoint = error?.scriptsApi?.endpoint || `${error?.config?.method || 'GET'} ${error?.config?.url || ''}`.trim();
  const status = error?.response?.status || null;
  const code = error?.response?.data?.code || error?.code || null;
  const message =
    error?.response?.data?.error ||
    error?.response?.data?.message ||
    error?.message ||
    'Unknown error';
  console.error(`[scripts] ${context}`, {
    endpoint,
    status,
    code,
    message
  });
}

async function promptText(
  conversation,
  ctx,
  message,
  {
    allowEmpty = false,
    allowSkip = false,
    defaultValue = null,
    parse = (value) => value,
    ensureActive
  } = {}
) {
  const safeEnsureActive = resolveEnsureActive(ensureActive, ctx);
  const hints = [];
  if (defaultValue !== null && defaultValue !== undefined && defaultValue !== '') {
    hints.push(`Current: ${defaultValue}`);
  }
  if (allowSkip) {
    hints.push('Type skip to keep current value');
  }
  hints.push('Type cancel to abort');

  const promptMessage = hints.length > 0 ? `${message}\n_${hints.join(' | ')}_` : message;
  await ctx.reply(promptMessage, { parse_mode: 'Markdown' });
  while (true) {
    const { text } = await waitForConversationText(conversation, ctx, {
      ensureActive: safeEnsureActive,
      allowEmpty,
      invalidMessage: '⚠️ Please send a text response to continue.'
    });

    if (!text) {
      if (allowEmpty) {
        return '';
      }
      await ctx.reply('⚠️ Please enter a value or type cancel.');
      continue;
    }

    if (isCancelInput(text)) {
      return null;
    }

    if (allowSkip && text.toLowerCase() === 'skip') {
      return undefined;
    }

    try {
      return parse(text);
    } catch (error) {
      await ctx.reply(`❌ ${error.message || 'Invalid value supplied.'}`);
    }
  }
}

async function confirm(conversation, ctx, prompt, ensureActive) {
  const safeEnsureActive = resolveEnsureActive(ensureActive, ctx);
  const choice = await askOptionWithButtons(
    conversation,
    ctx,
    prompt,
    [
      { id: 'yes', label: '✅ Yes' },
      { id: 'no', label: '❌ No' }
    ],
    { prefix: 'confirm', columns: 2, ensureActive: safeEnsureActive }
  );
  return choice.id === 'yes';
}

async function collectPlaceholderValues(conversation, ctx, placeholders, ensureActive) {
  const safeEnsureActive = resolveEnsureActive(ensureActive, ctx);
  const values = {};
  for (const placeholder of placeholders) {
    await ctx.reply(
      `✏️ Enter value for *${escapeMarkdown(placeholder)}* (type skip to leave unchanged, cancel to abort).`,
      { parse_mode: 'Markdown' }
    );
    const { text } = await waitForConversationText(conversation, ctx, {
      ensureActive: safeEnsureActive,
      invalidMessage: '⚠️ Please send a text response for this placeholder.'
    });
    if (!text) {
      continue;
    }
    if (isCancelInput(text)) {
      return null;
    }
    if (text.toLowerCase() === 'skip') {
      continue;
    }
    values[placeholder] = text;
  }
  return values;
}

function toPersonaOverrides(personaResult) {
  if (!personaResult) {
    return null;
  }

  const overrides = {};
  if (personaResult.business_id) {
    overrides.business_id = personaResult.business_id;
  }

  const persona = personaResult.persona_config || {};
  if (persona.purpose) {
    overrides.purpose = persona.purpose;
  }
  if (persona.emotion) {
    overrides.emotion = persona.emotion;
  }
  if (persona.urgency) {
    overrides.urgency = persona.urgency;
  }
  if (persona.technical_level) {
    overrides.technical_level = persona.technical_level;
  }

  return Object.keys(overrides).length ? overrides : null;
}

function buildPersonaSummaryFromConfig(script) {
  const summary = [];
  if (script.business_id) {
    const business = findBusinessOption(script.business_id);
    summary.push(`Persona: ${business ? business.label : script.business_id}`);
  }
  const persona = script.persona_config || {};
  if (persona.purpose) {
    summary.push(`Purpose: ${persona.purpose}`);
  }
  if (persona.emotion) {
    summary.push(`Tone: ${persona.emotion}`);
  }
  if (persona.urgency) {
    summary.push(`Urgency: ${persona.urgency}`);
  }
  if (persona.technical_level) {
    summary.push(`Technical level: ${persona.technical_level}`);
  }
  return summary;
}

function buildPersonaSummaryFromOverrides(overrides = {}) {
  if (!overrides) {
    return [];
  }

  const summary = [];
  if (overrides.business_id) {
    const business = findBusinessOption(overrides.business_id);
    summary.push(`Persona: ${business ? business.label : overrides.business_id}`);
  }
  if (overrides.purpose) {
    summary.push(`Purpose: ${overrides.purpose}`);
  }
  if (overrides.emotion) {
    summary.push(`Tone: ${overrides.emotion}`);
  }
  if (overrides.urgency) {
    summary.push(`Urgency: ${overrides.urgency}`);
  }
  if (overrides.technical_level) {
    summary.push(`Technical level: ${overrides.technical_level}`);
  }
  return summary;
}

async function collectPersonaConfig(conversation, ctx, defaults = {}, options = {}) {
  const { allowCancel = true, ensureActive } = options;
  const safeEnsureActive = resolveEnsureActive(ensureActive, ctx);
  const businessOptions = await getBusinessOptions();
  safeEnsureActive();

  const personaSummary = [];
  let businessSelection = defaults.business_id
    ? businessOptions.find((option) => option.id === defaults.business_id)
    : null;

  const selectionOptions = businessOptions.map((option) => ({ ...option }));
  if (allowCancel) {
    selectionOptions.unshift({ id: 'cancel', label: '❌ Cancel', custom: true });
  }

  const businessChoice = await askOptionWithButtons(
    conversation,
    ctx,
    `🎭 *Select persona for this script:*
Choose the primary business context.`,
    selectionOptions,
    {
      prefix: 'script-business',
      columns: 2,
      ensureActive: safeEnsureActive,
      formatLabel: (option) => (option.custom && option.id !== 'cancel' ? '✍️ Custom persona' : option.label)
    }
  );

  if (!businessChoice) {
    await ctx.reply('❌ Invalid persona selection. Please try again.');
    return null;
  }

  if (allowCancel && businessChoice.id === 'cancel') {
    return null;
  }

  businessSelection = businessChoice;

  const personaConfig = { ...(defaults.persona_config || {}) };

  if (businessSelection && !businessSelection.custom) {
    personaSummary.push(`Persona: ${businessSelection.label}`);
    const availablePurposes = businessSelection.purposes || [];

    if (availablePurposes.length > 0) {
      const currentPurposeLabel = personaConfig.purpose
        ? getOptionLabel(availablePurposes, personaConfig.purpose)
        : null;

      const purposePrompt = currentPurposeLabel
        ? `🎯 *Choose script purpose:*
This helps align tone and follow-up actions.
_Current: ${currentPurposeLabel}_`
        : `🎯 *Choose script purpose:*
This helps align tone and follow-up actions.`;

      const purposeSelection = await askOptionWithButtons(
        conversation,
        ctx,
        purposePrompt,
        availablePurposes,
        {
          prefix: 'script-purpose',
          columns: 1,
           ensureActive: safeEnsureActive,
          formatLabel: (option) => `${option.emoji || '•'} ${option.label}`
        }
      );

      personaConfig.purpose = purposeSelection?.id || null;
      if (purposeSelection?.label) {
        personaSummary.push(`Purpose: ${purposeSelection.label}`);
      }
    }

    const tonePrompt = personaConfig.emotion
      ? `🎙️ *Preferred tone for this script:*
_Current: ${getOptionLabel(MOOD_OPTIONS, personaConfig.emotion)}_`
      : `🎙️ *Preferred tone for this script:*`;

    const moodSelection = await askOptionWithButtons(
      conversation,
      ctx,
      tonePrompt,
      MOOD_OPTIONS,
      { prefix: 'script-tone', columns: 2, ensureActive: safeEnsureActive }
    );
    personaConfig.emotion = moodSelection.id;
    personaSummary.push(`Tone: ${moodSelection.label}`);

    const urgencyPrompt = personaConfig.urgency
      ? `⏱️ *Default urgency:*
_Current: ${getOptionLabel(URGENCY_OPTIONS, personaConfig.urgency)}_`
      : `⏱️ *Default urgency:*`;

    const urgencySelection = await askOptionWithButtons(
      conversation,
      ctx,
      urgencyPrompt,
      URGENCY_OPTIONS,
      { prefix: 'script-urgency', columns: 2, ensureActive: safeEnsureActive }
    );
    personaConfig.urgency = urgencySelection.id;
    personaSummary.push(`Urgency: ${urgencySelection.label}`);

    const techPrompt = personaConfig.technical_level
      ? `🧠 *Recipient technical level:*
_Current: ${getOptionLabel(TECH_LEVEL_OPTIONS, personaConfig.technical_level)}_`
      : `🧠 *Recipient technical level:*`;

    const techSelection = await askOptionWithButtons(
      conversation,
      ctx,
      techPrompt,
      TECH_LEVEL_OPTIONS,
      { prefix: 'script-tech', columns: 2, ensureActive: safeEnsureActive }
    );
    personaConfig.technical_level = techSelection.id;
    personaSummary.push(`Technical level: ${techSelection.label}`);
  } else {
    personaSummary.push('Persona: Custom');
    personaConfig.purpose = personaConfig.purpose || null;
    personaConfig.emotion = personaConfig.emotion || null;
    personaConfig.urgency = personaConfig.urgency || null;
    personaConfig.technical_level = personaConfig.technical_level || null;
  }

  return {
    business_id: businessSelection && !businessSelection.custom ? businessSelection.id : null,
    persona_config: personaConfig,
    personaSummary
  };
}

async function collectPromptAndVoice(conversation, ctx, defaults = {}, ensureActive) {
  const safeEnsureActive = resolveEnsureActive(ensureActive, ctx);
  const prompt = await promptText(
    conversation,
    ctx,
    '🧠 Provide the system prompt for this call script. This sets the AI behavior.',
    {
      allowEmpty: false,
      allowSkip: !!defaults.prompt,
      defaultValue: defaults.prompt,
      parse: (value) => value,
      ensureActive: safeEnsureActive
    }
  );

  if (prompt === null) {
    return null;
  }

  const firstMessage = await promptText(
    conversation,
    ctx,
    '🗣️ Provide the first message the agent says when the call connects.',
    {
      allowEmpty: false,
      allowSkip: !!defaults.first_message,
      defaultValue: defaults.first_message,
      parse: (value) => value,
      ensureActive: safeEnsureActive
    }
  );

  if (firstMessage === null) {
    return null;
  }

  const voicePrompt = defaults.voice_model ? defaults.voice_model : 'default';
  const voiceModel = await promptText(
    conversation,
    ctx,
    '🎤 Enter the Deepgram voice model for this script (or type skip to use the default).',
    {
      allowEmpty: true,
      allowSkip: true,
      defaultValue: voicePrompt,
      parse: (value) => value,
      ensureActive: safeEnsureActive
    }
  );

  if (voiceModel === null) {
    return null;
  }

  return {
    prompt: prompt === undefined ? defaults.prompt : prompt,
    first_message: firstMessage === undefined ? defaults.first_message : firstMessage,
    voice_model: voiceModel === undefined ? defaults.voice_model : (voiceModel || null)
  };
}

async function collectDigitCaptureConfig(conversation, ctx, defaults = {}, ensureActive) {
  const safeEnsureActive = resolveEnsureActive(ensureActive, ctx);
  const selection = await askOptionWithButtons(
    conversation,
    ctx,
    '🔢 Add digit capture to this script?',
    [
      { id: 'none', label: '🚫 None' },
      { id: 'otp', label: '🔐 OTP (code)' },
      { id: 'pin', label: '🔑 PIN' },
      { id: 'routing', label: '🏦 Routing number' },
      { id: 'account', label: '🏦 Account number' },
      { id: 'banking', label: '🏦 Banking group (routing + account)' },
      { id: 'card', label: '💳 Card group (card + expiry + zip + cvv)' },
      { id: 'custom', label: '⚙️ Custom profile' }
    ],
    { prefix: 'call-script-capture', columns: 2, ensureActive: safeEnsureActive }
  );

  if (!selection || selection.id === 'none') {
    return {
      requires_otp: false,
      default_profile: null,
      expected_length: null,
      allow_terminator: false,
      terminator_char: null,
      capture_group: null
    };
  }

  const capture = {
    requires_otp: false,
    default_profile: null,
    expected_length: null,
    allow_terminator: false,
    terminator_char: null,
    capture_group: null
  };

  if (selection.id === 'otp') {
    capture.requires_otp = true;
    const length = await promptText(
      conversation,
      ctx,
      '🔢 OTP length (4-8 digits).',
      {
        allowEmpty: false,
        parse: (value) => {
          const parsed = Number(value);
          if (!Number.isInteger(parsed) || parsed < 4 || parsed > 8) {
            throw new Error('OTP length must be an integer between 4 and 8.');
          }
          return parsed;
        },
        ensureActive: safeEnsureActive
      }
    );
    if (length === null) return null;
    capture.expected_length = length;
    return capture;
  }

  if (selection.id === 'banking') {
    capture.capture_group = 'banking';
    return capture;
  }
  if (selection.id === 'card') {
    capture.capture_group = 'card';
    return capture;
  }

  if (selection.id === 'custom') {
    const profile = await promptText(
      conversation,
      ctx,
      'Enter a profile id (e.g., routing_number, account_number, card_number, cvv).',
      { allowEmpty: false, parse: (value) => value.trim(), ensureActive: safeEnsureActive }
    );
    if (!profile) return null;
    capture.default_profile = profile;
  } else {
    const profileMap = {
      pin: 'pin',
      routing: 'routing_number',
      account: 'account_number'
    };
    capture.default_profile = profileMap[selection.id] || selection.id;
  }

  const expectedLength = await promptText(
    conversation,
    ctx,
    'Optional expected length (or type skip).',
    {
      allowEmpty: true,
      allowSkip: true,
      parse: (value) => {
        const parsed = Number(value);
        if (!Number.isInteger(parsed) || parsed < 1 || parsed > 64) {
          throw new Error('Expected length must be an integer between 1 and 64.');
        }
        return parsed;
      },
      ensureActive: safeEnsureActive
    }
  );
  if (expectedLength === null) return null;
  if (expectedLength !== undefined && expectedLength !== '') {
    capture.expected_length = expectedLength;
  }

  const allowTerminator = await confirm(conversation, ctx, 'Allow terminator key (#)?', safeEnsureActive);
  capture.allow_terminator = !!allowTerminator;
  if (capture.allow_terminator) {
    const term = await promptText(
      conversation,
      ctx,
      'Terminator key (default #).',
      { allowEmpty: true, allowSkip: true, defaultValue: '#', parse: (value) => value.trim() || '#', ensureActive: safeEnsureActive }
    );
    if (term === null) return null;
    capture.terminator_char = term === undefined ? '#' : term;
  }

  return capture;
}

async function collectPaymentDefaultsConfig(conversation, ctx, defaults = {}, ensureActive) {
  const safeEnsureActive = resolveEnsureActive(ensureActive, ctx);
  const currentlyEnabled = isPaymentEnabled(defaults.payment_enabled);
  const choice = await askOptionWithButtons(
    conversation,
    ctx,
    currentlyEnabled
      ? '💳 Payment defaults are currently enabled for this script. Update them?'
      : '💳 Add payment defaults to this script?',
    [
      { id: 'no', label: '🚫 Disabled' },
      { id: 'yes', label: '✅ Enabled' }
    ],
    { prefix: 'call-script-payment-defaults', columns: 2, ensureActive: safeEnsureActive }
  );

  if (!choice || choice.id === 'no') {
    return {
      payment_enabled: false,
      payment_connector: null,
      payment_amount: null,
      payment_currency: null,
      payment_description: null,
      payment_start_message: null,
      payment_success_message: null,
      payment_failure_message: null,
      payment_retry_message: null
    };
  }

  const connector = await promptText(
    conversation,
    ctx,
    '💳 Enter Twilio payment connector name.',
    {
      allowEmpty: false,
      allowSkip: !!defaults.payment_connector,
      defaultValue: defaults.payment_connector || undefined,
      parse: (value) => value.trim(),
      ensureActive: safeEnsureActive
    }
  );
  if (connector === null) return null;
  const resolvedConnector = connector === undefined
    ? String(defaults.payment_connector || '').trim()
    : String(connector || '').trim();
  if (!resolvedConnector) {
    await ctx.reply('❌ Payment connector is required when payment defaults are enabled.');
    return null;
  }

  const amount = await promptText(
    conversation,
    ctx,
    '💵 Optional default amount (example 49.99) or type skip.',
    {
      allowEmpty: true,
      allowSkip: true,
      defaultValue: defaults.payment_amount || '',
      parse: (value) => {
        const parsed = parsePositiveAmountInput(value);
        if (!parsed) {
          throw new Error('Amount must be a positive value with up to 2 decimals.');
        }
        return parsed;
      },
      ensureActive: safeEnsureActive
    }
  );
  if (amount === null) return null;

  const currency = await promptText(
    conversation,
    ctx,
    '💱 Optional default currency (3-letter code, example USD) or type skip.',
    {
      allowEmpty: true,
      allowSkip: true,
      defaultValue: defaults.payment_currency || 'USD',
      parse: (value) => {
        const normalized = normalizeCurrencyInput(value);
        if (!normalized) {
          throw new Error('Currency must be a 3-letter code.');
        }
        return normalized;
      },
      ensureActive: safeEnsureActive
    }
  );
  if (currency === null) return null;

  const description = await promptText(
    conversation,
    ctx,
    '🧾 Optional payment description (or type skip).',
    {
      allowEmpty: true,
      allowSkip: true,
      defaultValue: defaults.payment_description || '',
      parse: (value) => value.trim().slice(0, 240),
      ensureActive: safeEnsureActive
    }
  );
  if (description === null) return null;

  const startMessage = await promptText(
    conversation,
    ctx,
    '🗣️ Optional message before payment capture starts (or type skip).',
    {
      allowEmpty: true,
      allowSkip: true,
      defaultValue: defaults.payment_start_message || '',
      parse: (value) => normalizePaymentMessageInput(value),
      ensureActive: safeEnsureActive
    }
  );
  if (startMessage === null) return null;

  const successMessage = await promptText(
    conversation,
    ctx,
    '✅ Optional success message after payment completes (or type skip).',
    {
      allowEmpty: true,
      allowSkip: true,
      defaultValue: defaults.payment_success_message || '',
      parse: (value) => normalizePaymentMessageInput(value),
      ensureActive: safeEnsureActive
    }
  );
  if (successMessage === null) return null;

  const failureMessage = await promptText(
    conversation,
    ctx,
    '⚠️ Optional failure message after payment fails (or type skip).',
    {
      allowEmpty: true,
      allowSkip: true,
      defaultValue: defaults.payment_failure_message || '',
      parse: (value) => normalizePaymentMessageInput(value),
      ensureActive: safeEnsureActive
    }
  );
  if (failureMessage === null) return null;

  const retryMessage = await promptText(
    conversation,
    ctx,
    '🔁 Optional retry/timeout guidance message (or type skip).',
    {
      allowEmpty: true,
      allowSkip: true,
      defaultValue: defaults.payment_retry_message || '',
      parse: (value) => normalizePaymentMessageInput(value),
      ensureActive: safeEnsureActive
    }
  );
  if (retryMessage === null) return null;

  return {
    payment_enabled: true,
    payment_connector: resolvedConnector,
    payment_amount:
      amount === undefined ? defaults.payment_amount || null : amount || null,
    payment_currency:
      currency === undefined
        ? (defaults.payment_currency || 'USD')
        : (currency || 'USD'),
    payment_description:
      description === undefined
        ? (defaults.payment_description || null)
        : (description || null),
    payment_start_message:
      startMessage === undefined
        ? (defaults.payment_start_message || null)
        : (startMessage || null),
    payment_success_message:
      successMessage === undefined
        ? (defaults.payment_success_message || null)
        : (successMessage || null),
    payment_failure_message:
      failureMessage === undefined
        ? (defaults.payment_failure_message || null)
        : (failureMessage || null),
    payment_retry_message:
      retryMessage === undefined
        ? (defaults.payment_retry_message || null)
        : (retryMessage || null)
  };
}

async function fetchCallScripts() {
  const data = await scriptsApiRequest({ method: 'get', url: '/api/call-scripts' });
  const payload = Array.isArray(data)
    ? { scripts: data }
    : (data?.data && typeof data.data === 'object' ? data.data : data) || {};
  const sourceList = Array.isArray(payload?.scripts)
    ? payload.scripts
    : Array.isArray(payload?.templates)
      ? payload.templates
      : Array.isArray(payload?.call_scripts)
        ? payload.call_scripts
        : Array.isArray(payload?.callTemplates)
          ? payload.callTemplates
          : Array.isArray(payload?.items)
            ? payload.items
            : [];

  return sourceList
    .map((item, index) => {
      if (!item || typeof item !== 'object') return null;
      const idCandidate = item.id ?? item.script_id ?? item.template_id ?? item.call_template_id ?? null;
      const parsedId = Number(idCandidate);
      const normalizedId = Number.isFinite(parsedId) ? parsedId : null;
      const normalizedName = String(
        item.name ?? item.script_name ?? item.template_name ?? `Script ${index + 1}`
      ).trim();
      if (!normalizedName) return null;
      return {
        ...item,
        id: normalizedId,
        name: normalizedName,
        description: item.description ?? item.summary ?? item.notes ?? null,
        prompt: item.prompt ?? item.script_prompt ?? null,
        first_message: item.first_message ?? item.firstMessage ?? null,
        payment_enabled:
          item.payment_enabled ?? item.paymentEnabled ?? false,
        payment_connector:
          item.payment_connector ?? item.paymentConnector ?? null,
        payment_amount:
          item.payment_amount ?? item.paymentAmount ?? null,
        payment_currency:
          item.payment_currency ?? item.paymentCurrency ?? null,
        payment_description:
          item.payment_description ?? item.paymentDescription ?? null,
        payment_start_message:
          item.payment_start_message ?? item.paymentStartMessage ?? null,
        payment_success_message:
          item.payment_success_message ?? item.paymentSuccessMessage ?? null,
        payment_failure_message:
          item.payment_failure_message ?? item.paymentFailureMessage ?? null,
        payment_retry_message:
          item.payment_retry_message ?? item.paymentRetryMessage ?? null
      };
    })
    .filter(Boolean);
}

async function fetchCallScriptById(id) {
  const data = await scriptsApiRequest({ method: 'get', url: `/api/call-scripts/${id}` });
  return data.script;
}

async function fetchInboundDefaultScript() {
  const data = await scriptsApiRequest({ method: 'get', url: '/api/inbound/default-script' });
  return data || {};
}

async function setInboundDefaultScript(scriptId) {
  const data = await scriptsApiRequest({
    method: 'put',
    url: '/api/inbound/default-script',
    data: { script_id: scriptId }
  });
  return data;
}

async function clearInboundDefaultScript() {
  const data = await scriptsApiRequest({ method: 'delete', url: '/api/inbound/default-script' });
  return data;
}

async function createCallScript(payload, options = {}) {
  const headers = {};
  const idempotencyKey = String(options.idempotencyKey || '').trim();
  if (idempotencyKey) {
    headers['Idempotency-Key'] = idempotencyKey;
  }
  const data = await scriptsApiRequest({
    method: 'post',
    url: '/api/call-scripts',
    data: payload,
    headers: Object.keys(headers).length ? headers : undefined
  });
  return data.script;
}

async function updateCallScript(id, payload, options = {}) {
  const headers = {};
  const idempotencyKey = String(options.idempotencyKey || '').trim();
  if (idempotencyKey) {
    headers['Idempotency-Key'] = idempotencyKey;
  }
  const data = await scriptsApiRequest({
    method: 'put',
    url: `/api/call-scripts/${id}`,
    data: payload,
    headers: Object.keys(headers).length ? headers : undefined
  });
  return data.script;
}

async function deleteCallScript(id, options = {}) {
  const headers = {};
  const idempotencyKey = String(options.idempotencyKey || '').trim();
  if (idempotencyKey) {
    headers['Idempotency-Key'] = idempotencyKey;
  }
  await scriptsApiRequest({
    method: 'delete',
    url: `/api/call-scripts/${id}`,
    headers: Object.keys(headers).length ? headers : undefined
  });
}

async function cloneCallScript(id, payload, options = {}) {
  const explicitKey = String(options.idempotencyKey || '').trim();
  const fallbackKey = `clone_call_script_${id}`;
  const idempotencyKey = explicitKey || fallbackKey;
  const data = await scriptsApiRequest({
    method: 'post',
    url: `/api/call-scripts/${id}/clone`,
    data: payload,
    retry: { retries: 0 },
    headers: {
      'Idempotency-Key': idempotencyKey
    }
  });
  return data.script;
}

function formatCallScriptSummary(script) {
  const summary = [];
  summary.push(`📛 *${escapeMarkdown(script.name)}*`);
  if (script.description) {
    summary.push(`📝 ${escapeMarkdown(script.description)}`);
  }
  if (script.business_id) {
    const business = findBusinessOption(script.business_id);
    summary.push(`🏢 Persona: ${escapeMarkdown(business ? business.label : script.business_id)}`);
  }
  const personaSummary = buildPersonaSummaryFromConfig(script);
  if (personaSummary.length) {
    personaSummary.forEach((line) => summary.push(`• ${escapeMarkdown(line)}`));
  }

  const captureSummary = buildDigitCaptureSummary(script);
  summary.push(`🔢 Digit capture: ${escapeMarkdown(captureSummary)}`);
  summary.push(`💳 Payment defaults: ${escapeMarkdown(buildPaymentDefaultsSummary(script))}`);
  if (isPaymentEnabled(script.payment_enabled) && script.payment_description) {
    summary.push(`🧾 Payment note: ${escapeMarkdown(String(script.payment_description).slice(0, 160))}`);
  }
  if (isPaymentEnabled(script.payment_enabled) && script.payment_start_message) {
    summary.push(`🗣️ Payment intro: ${escapeMarkdown(String(script.payment_start_message).slice(0, 160))}`);
  }
  if (isPaymentEnabled(script.payment_enabled) && script.payment_success_message) {
    summary.push(`✅ Payment success line: ${escapeMarkdown(String(script.payment_success_message).slice(0, 160))}`);
  }
  if (isPaymentEnabled(script.payment_enabled) && script.payment_failure_message) {
    summary.push(`⚠️ Payment failure line: ${escapeMarkdown(String(script.payment_failure_message).slice(0, 160))}`);
  }
  if (isPaymentEnabled(script.payment_enabled) && script.payment_retry_message) {
    summary.push(`🔁 Payment retry line: ${escapeMarkdown(String(script.payment_retry_message).slice(0, 160))}`);
  }

  if (script.voice_model) {
    summary.push(`🎤 Voice model: ${escapeMarkdown(script.voice_model)}`);
  }

  const placeholders = new Set([
    ...extractScriptVariables(script.prompt || ''),
    ...extractScriptVariables(script.first_message || '')
  ]);
  if (placeholders.size > 0) {
    summary.push(`🧩 Placeholders: ${Array.from(placeholders).map(escapeMarkdown).join(', ')}`);
  }

  if (script.prompt) {
    const snippet = script.prompt.substring(0, 160);
    summary.push(`📜 Prompt snippet: ${escapeMarkdown(snippet)}${script.prompt.length > 160 ? '…' : ''}`);
  }
  if (script.first_message) {
    const snippet = script.first_message.substring(0, 160);
    summary.push(`🗨️ First message: ${escapeMarkdown(snippet)}${script.first_message.length > 160 ? '…' : ''}`);
  }
  summary.push(
    `📅 Updated: ${escapeMarkdown(new Date(script.updated_at || script.created_at).toLocaleString())}`
  );
  return summary.join('\n');
}

async function previewCallScript(conversation, ctx, script, ensureActive) {
  const safeEnsureActive = resolveEnsureActive(ensureActive, ctx);
  const phonePrompt =
    '📞 Enter the test phone number (E.164 format, e.g., +1234567890) to receive a preview call.';
  const testNumber = await promptText(conversation, ctx, phonePrompt, {
    allowEmpty: false,
    ensureActive: safeEnsureActive
  });
  if (!testNumber) {
    await ctx.reply('❌ Preview cancelled.');
    return;
  }

  if (!/^\+[1-9]\d{1,14}$/.test(testNumber)) {
    await ctx.reply('❌ Invalid phone number format. Preview cancelled.');
    return;
  }

  const placeholderSet = new Set();
  extractScriptVariables(script.prompt || '').forEach((token) => placeholderSet.add(token));
  extractScriptVariables(script.first_message || '').forEach((token) => placeholderSet.add(token));

  let prompt = script.prompt;
  let firstMessage = script.first_message;

  if (placeholderSet.size > 0) {
    await ctx.reply('🧩 This script has placeholders. Provide values where needed (type skip to leave unchanged).');
    const values = await collectPlaceholderValues(conversation, ctx, Array.from(placeholderSet), safeEnsureActive);
    if (values === null) {
      await ctx.reply('❌ Preview cancelled.');
      return;
    }
    if (prompt) {
      prompt = replacePlaceholders(prompt, values);
    }
    if (firstMessage) {
      firstMessage = replacePlaceholders(firstMessage, values);
    }
  }

  const payload = {
    number: testNumber,
    user_chat_id: ctx.from.id.toString()
  };

  if (script.business_id) {
    payload.business_id = script.business_id;
  }
  const persona = script.persona_config || {};
  if (prompt) {
    payload.prompt = prompt;
  }
  if (firstMessage) {
    payload.first_message = firstMessage;
  }
  if (script.voice_model) {
    payload.voice_model = script.voice_model;
  }
  if (persona.purpose) {
    payload.purpose = persona.purpose;
  }
  if (persona.emotion) {
    payload.emotion = persona.emotion;
  }
  if (persona.urgency) {
    payload.urgency = persona.urgency;
  }
  if (persona.technical_level) {
    payload.technical_level = persona.technical_level;
  }
  if (isPaymentEnabled(script.payment_enabled)) {
    payload.payment_enabled = true;
    if (script.payment_connector) {
      payload.payment_connector = String(script.payment_connector).trim();
    }
    if (script.payment_amount) {
      payload.payment_amount = String(script.payment_amount).trim();
    }
    if (script.payment_currency) {
      payload.payment_currency = String(script.payment_currency).trim().toUpperCase();
    }
    if (script.payment_description) {
      payload.payment_description = String(script.payment_description).trim().slice(0, 240);
    }
    if (script.payment_start_message) {
      payload.payment_start_message = String(script.payment_start_message).trim().slice(0, 240);
    }
    if (script.payment_success_message) {
      payload.payment_success_message = String(script.payment_success_message).trim().slice(0, 240);
    }
    if (script.payment_failure_message) {
      payload.payment_failure_message = String(script.payment_failure_message).trim().slice(0, 240);
    }
    if (script.payment_retry_message) {
      payload.payment_retry_message = String(script.payment_retry_message).trim().slice(0, 240);
    }
  }

  try {
    await runWithActionWatchdog(
      ctx,
      'Launching preview call',
      'call_preview',
      () => httpClient.post(null, `${config.apiUrl}/outbound-call`, payload, {
        headers: { 'Content-Type': 'application/json' },
        timeout: 30000
      })
    );

    await ctx.reply('✅ Preview call launched! You should receive a call shortly.');
  } catch (error) {
    console.error('Failed to launch preview call:', error?.response?.data || error.message);
    await ctx.reply(`❌ Preview failed: ${error?.response?.data?.error || error.message}`);
  }
}

async function createCallScriptFlow(conversation, ctx, ensureActive) {
  const safeEnsureActive = resolveEnsureActive(ensureActive, ctx);
  const name = await promptText(
    conversation,
    ctx,
    '🆕 *Script name*\nEnter a unique name for this call script.',
    {
      allowEmpty: false,
      parse: (value) => value.trim(),
      ensureActive: safeEnsureActive
    }
  );

  if (!name) {
    await ctx.reply('❌ Script creation cancelled.');
    return;
  }

  const description = await promptText(
    conversation,
    ctx,
    '📝 Provide an optional description for this script (or type skip).',
    {
      allowEmpty: true,
      allowSkip: true,
      parse: (value) => value.trim(),
      ensureActive: safeEnsureActive
    }
  );
  if (description === null) {
    await ctx.reply('❌ Script creation cancelled.');
    return;
  }

  const personaResult = await collectPersonaConfig(conversation, ctx, {}, { allowCancel: true, ensureActive: safeEnsureActive });
  if (!personaResult) {
    await ctx.reply('❌ Script creation cancelled.');
    return;
  }

  const promptAndVoice = await collectPromptAndVoice(conversation, ctx, {}, safeEnsureActive);
  if (!promptAndVoice) {
    await ctx.reply('❌ Script creation cancelled.');
    return;
  }

  const captureConfig = await collectDigitCaptureConfig(conversation, ctx, {}, safeEnsureActive);
  if (!captureConfig) {
    await ctx.reply('❌ Script creation cancelled.');
    return;
  }
  if (captureConfig.capture_group) {
    await ctx.reply('ℹ️ Capture groups are guidance-only; the API still infers groups from the prompt text.');
  }

  const paymentConfig = await collectPaymentDefaultsConfig(conversation, ctx, {}, safeEnsureActive);
  if (!paymentConfig) {
    await ctx.reply('❌ Script creation cancelled.');
    return;
  }

  const scriptPayload = {
    name,
    description: description === undefined ? null : (description.length ? description : null),
    business_id: personaResult.business_id,
    persona_config: personaResult.persona_config,
    prompt: promptAndVoice.prompt,
    first_message: promptAndVoice.first_message,
    voice_model: promptAndVoice.voice_model || null,
    requires_otp: captureConfig.requires_otp || false,
    default_profile: captureConfig.default_profile || null,
    expected_length: captureConfig.expected_length || null,
    allow_terminator: captureConfig.allow_terminator || false,
    terminator_char: captureConfig.terminator_char || null,
    payment_enabled: paymentConfig.payment_enabled === true,
    payment_connector: paymentConfig.payment_connector || null,
    payment_amount: paymentConfig.payment_amount || null,
    payment_currency: paymentConfig.payment_currency || null,
    payment_description: paymentConfig.payment_description || null,
    payment_start_message: paymentConfig.payment_start_message || null,
    payment_success_message: paymentConfig.payment_success_message || null,
    payment_failure_message: paymentConfig.payment_failure_message || null,
    payment_retry_message: paymentConfig.payment_retry_message || null,
    capture_group: captureConfig.capture_group || null
  };

  const validation = validateCallScriptPayload(scriptPayload);
  if (validation.errors.length) {
    await ctx.reply(`❌ Fix the following issues:\n• ${validation.errors.join('\n• ')}`);
    return;
  }
  if (validation.warnings.length) {
    await ctx.reply(`⚠️ Warnings:\n• ${validation.warnings.join('\n• ')}`);
    const proceed = await confirm(conversation, ctx, 'Proceed anyway?', safeEnsureActive);
    if (!proceed) {
      await ctx.reply('❌ Script creation cancelled.');
      return;
    }
  }

  try {
    const mutationGate = beginMutationRequest(ctx, 'call:create', name);
    if (!mutationGate.allowed) {
      await ctx.reply('⚠️ Script creation is already processing or just completed. Please wait a moment.');
      return;
    }

    const apiPayload = { ...scriptPayload };
    delete apiPayload.capture_group;
    try {
      const opId = getCurrentOpId(ctx) || `user_${ctx?.from?.id || 'unknown'}`;
      const createIdempotencyKey = `create_call_script_${name}_${opId}`.slice(0, 96);
      const script = await runWithActionWatchdog(
        ctx,
        'Creating call script',
        'call_create',
        () => createCallScript(apiPayload, { idempotencyKey: createIdempotencyKey })
      );
      const needsCaptureUpdate = scriptPayload.requires_otp
        || scriptPayload.default_profile
        || scriptPayload.expected_length
        || scriptPayload.allow_terminator
        || scriptPayload.terminator_char;
      if (needsCaptureUpdate) {
        try {
          await runWithActionWatchdog(
            ctx,
            'Applying digit capture settings',
            'call_update_capture',
            () => updateCallScript(script.id, stripUndefined({
              requires_otp: scriptPayload.requires_otp,
              default_profile: scriptPayload.default_profile,
              expected_length: scriptPayload.expected_length,
              allow_terminator: scriptPayload.allow_terminator,
              terminator_char: scriptPayload.terminator_char
            }), {
              idempotencyKey: `update_call_script_capture_${script.id}_${opId}`.slice(0, 96)
            })
          );
        } catch (updateError) {
          console.warn('Capture settings update failed:', updateError.message);
        }
      }
      finishMutationRequest(mutationGate.key, 'done');
      await storeScriptVersionSnapshot({ ...script, ...scriptPayload }, 'call', ctx);
      await ctx.reply(`✅ Script *${escapeMarkdown(script.name)}* created successfully!`, { parse_mode: 'Markdown' });
    } catch (error) {
      finishMutationRequest(mutationGate.key, 'failed');
      throw error;
    }
  } catch (error) {
    logScriptsError('Failed to create script', error);
    await ctx.reply(formatScriptsApiError(error, 'Failed to create script'));
  }
}

async function editCallScriptFlow(conversation, ctx, script, ensureActive) {
  const safeEnsureActive = resolveEnsureActive(ensureActive, ctx);
  const updates = {};

  const name = await promptText(
    conversation,
    ctx,
    '✏️ Update script name (or type skip to keep current).',
    {
      allowEmpty: false,
      allowSkip: true,
      defaultValue: script.name,
      parse: (value) => value.trim(),
      ensureActive: safeEnsureActive
    }
  );
  if (name === null) {
    await ctx.reply('❌ Update cancelled.');
    return;
  }
  if (name !== undefined) {
    if (!name.length) {
      await ctx.reply('❌ Script name cannot be empty.');
      return;
    }
    updates.name = name;
  }

  const description = await promptText(
    conversation,
    ctx,
    '📝 Update description (or type skip).',
    {
      allowEmpty: true,
      allowSkip: true,
      defaultValue: script.description || '',
      parse: (value) => value.trim(),
      ensureActive: safeEnsureActive
    }
  );
  if (description === null) {
    await ctx.reply('❌ Update cancelled.');
    return;
  }
  if (description !== undefined) {
    updates.description = description.length ? description : null;
  }

  const adjustPersona = await confirm(conversation, ctx, 'Would you like to update the persona settings?', safeEnsureActive);
  if (adjustPersona) {
    const personaResult = await collectPersonaConfig(conversation, ctx, script, { allowCancel: true, ensureActive: safeEnsureActive });
    if (!personaResult) {
      await ctx.reply('❌ Update cancelled.');
      return;
    }
    updates.business_id = personaResult.business_id;
    updates.persona_config = personaResult.persona_config;
  }

  const adjustPrompt = await confirm(conversation, ctx, 'Update prompt, first message, or voice settings?', safeEnsureActive);
  if (adjustPrompt) {
    const promptAndVoice = await collectPromptAndVoice(conversation, ctx, script, safeEnsureActive);
    if (!promptAndVoice) {
      await ctx.reply('❌ Update cancelled.');
      return;
    }
    updates.prompt = promptAndVoice.prompt;
    updates.first_message = promptAndVoice.first_message;
    updates.voice_model = promptAndVoice.voice_model || null;
  }

  const adjustCapture = await confirm(conversation, ctx, 'Update digit capture settings?', safeEnsureActive);
  if (adjustCapture) {
    const captureConfig = await collectDigitCaptureConfig(conversation, ctx, script, safeEnsureActive);
    if (!captureConfig) {
      await ctx.reply('❌ Update cancelled.');
      return;
    }
    updates.requires_otp = captureConfig.requires_otp || false;
    updates.default_profile = captureConfig.default_profile || null;
    updates.expected_length = captureConfig.expected_length || null;
    updates.allow_terminator = captureConfig.allow_terminator || false;
    updates.terminator_char = captureConfig.terminator_char || null;
    updates.capture_group = captureConfig.capture_group || null;
  }

  const adjustPayment = await confirm(conversation, ctx, 'Update payment defaults?', safeEnsureActive);
  if (adjustPayment) {
    const paymentConfig = await collectPaymentDefaultsConfig(conversation, ctx, script, safeEnsureActive);
    if (!paymentConfig) {
      await ctx.reply('❌ Update cancelled.');
      return;
    }
    updates.payment_enabled = paymentConfig.payment_enabled === true;
    updates.payment_connector = paymentConfig.payment_connector || null;
    updates.payment_amount = paymentConfig.payment_amount || null;
    updates.payment_currency = paymentConfig.payment_currency || null;
    updates.payment_description = paymentConfig.payment_description || null;
    updates.payment_start_message = paymentConfig.payment_start_message || null;
    updates.payment_success_message = paymentConfig.payment_success_message || null;
    updates.payment_failure_message = paymentConfig.payment_failure_message || null;
    updates.payment_retry_message = paymentConfig.payment_retry_message || null;
  }

  if (Object.keys(updates).length === 0) {
    await ctx.reply('ℹ️ No changes made.');
    return;
  }

  const merged = { ...script, ...updates };
  const validation = validateCallScriptPayload(merged);
  if (validation.errors.length) {
    await ctx.reply(`❌ Fix the following issues:\n• ${validation.errors.join('\n• ')}`);
    return;
  }
  if (validation.warnings.length) {
    await ctx.reply(`⚠️ Warnings:\n• ${validation.warnings.join('\n• ')}`);
    const proceed = await confirm(conversation, ctx, 'Proceed anyway?', safeEnsureActive);
    if (!proceed) {
      await ctx.reply('❌ Update cancelled.');
      return;
    }
  }

  try {
    const mutationGate = beginMutationRequest(ctx, 'call:update', script.id);
    if (!mutationGate.allowed) {
      await ctx.reply('⚠️ Script update is already processing or just completed. Please wait a moment.');
      return;
    }

    await storeScriptVersionSnapshot(script, 'call', ctx);
    try {
      const opId = getCurrentOpId(ctx) || `user_${ctx?.from?.id || 'unknown'}`;
      const idempotencyKey = `update_call_script_${script.id}_${opId}`.slice(0, 96);
      const apiUpdates = stripUndefined({ ...updates });
      delete apiUpdates.capture_group;
      const updated = await runWithActionWatchdog(
        ctx,
        'Updating call script',
        'call_update',
        () => updateCallScript(script.id, apiUpdates, { idempotencyKey })
      );
      finishMutationRequest(mutationGate.key, 'done');
      await ctx.reply(`✅ Script *${escapeMarkdown(updated.name)}* updated.`, { parse_mode: 'Markdown' });
    } catch (error) {
      finishMutationRequest(mutationGate.key, 'failed');
      throw error;
    }
  } catch (error) {
    logScriptsError('Failed to update script', error);
    await ctx.reply(formatScriptsApiError(error, 'Failed to update script'));
  }
}

async function cloneCallScriptFlow(conversation, ctx, script, ensureActive) {
  const safeEnsureActive = resolveEnsureActive(ensureActive, ctx);
  const name = await promptText(
    conversation,
    ctx,
    `🆕 Enter a name for the clone of *${escapeMarkdown(script.name)}*.`,
    {
      allowEmpty: false,
      parse: (value) => value.trim(),
      defaultValue: null,
      ensureActive: safeEnsureActive
    }
  );
  if (!name) {
    await ctx.reply('❌ Clone cancelled.');
    return;
  }

  const description = await promptText(
    conversation,
    ctx,
    '📝 Optionally provide a description for the new script (or type skip).',
    {
      allowEmpty: true,
      allowSkip: true,
      defaultValue: script.description || '',
      parse: (value) => value.trim(),
      ensureActive: safeEnsureActive
    }
  );
  if (description === null) {
    await ctx.reply('❌ Clone cancelled.');
    return;
  }

  const cloneGate = beginCloneRequest(ctx, script.id, name);
  if (!cloneGate.allowed) {
    await ctx.reply('⚠️ Clone already in progress or just completed. Please wait a moment before trying again.');
    return;
  }

  try {
    const normalizedName = name
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9_-]+/g, '_')
      .replace(/^_+|_+$/g, '')
      .slice(0, 42) || 'script';
    const stableActor = String(ctx?.from?.id || 'unknown').replace(/[^0-9a-z_-]/gi, '').slice(0, 20) || 'user';
    const idempotencyKey = `clone_call_script_${stableActor}_${script.id}_${normalizedName}`.slice(0, 96);
    const cloned = await runWithActionWatchdog(
      ctx,
      'Cloning call script',
      'call_clone',
      () => cloneCallScript(script.id, {
        name,
        description: description === undefined ? script.description : (description.length ? description : null)
      }, {
        idempotencyKey
      })
    );
    finishCloneRequest(cloneGate.key, 'done');
    await ctx.reply(`✅ Script cloned as *${escapeMarkdown(cloned.name)}*.`, { parse_mode: 'Markdown' });
  } catch (error) {
    finishCloneRequest(cloneGate.key, 'failed');
    logScriptsError('Failed to clone script', error);
    await ctx.reply(formatScriptsApiError(error, 'Failed to clone script'));
  }
}

async function deleteCallScriptFlow(conversation, ctx, script, ensureActive) {
  const safeEnsureActive = resolveEnsureActive(ensureActive, ctx);
  const confirmed = await confirm(
    conversation,
    ctx,
    `Are you sure you want to delete *${escapeMarkdown(script.name)}*?`,
    safeEnsureActive
  );
  if (!confirmed) {
    await ctx.reply('Deletion cancelled.');
    return;
  }

  try {
    const mutationGate = beginMutationRequest(ctx, 'call:delete', script.id);
    if (!mutationGate.allowed) {
      await ctx.reply('⚠️ Script deletion is already processing or just completed. Please wait a moment.');
      return;
    }
    await storeScriptVersionSnapshot(script, 'call', ctx);
    try {
      const opId = getCurrentOpId(ctx) || `user_${ctx?.from?.id || 'unknown'}`;
      const idempotencyKey = `delete_call_script_${script.id}_${opId}`.slice(0, 96);
      await runWithActionWatchdog(
        ctx,
        'Deleting call script',
        'call_delete',
        () => deleteCallScript(script.id, { idempotencyKey })
      );
      finishMutationRequest(mutationGate.key, 'done');
      await ctx.reply(`🗑️ Script *${escapeMarkdown(script.name)}* deleted.`, { parse_mode: 'Markdown' });
    } catch (error) {
      finishMutationRequest(mutationGate.key, 'failed');
      throw error;
    }
  } catch (error) {
    logScriptsError('Failed to delete script', error);
    await ctx.reply(formatScriptsApiError(error, 'Failed to delete script'));
  }
}

async function showCallScriptVersions(conversation, ctx, script, ensureActive) {
  const safeEnsureActive = resolveEnsureActive(ensureActive, ctx);
  try {
    const versions = await listScriptVersions(script.id, 'call', 8);
    safeEnsureActive();
    if (!versions.length) {
      await ctx.reply('ℹ️ No saved versions yet. Versions are stored on edit/delete.');
      return;
    }
    const lines = versions.map((v) => `v${v.version_number} • ${new Date(v.created_at).toLocaleString()}`);
    await ctx.reply(`🗂️ Saved versions\n${lines.join('\n')}`);

    const options = versions.map((v) => ({
      id: String(v.version_number),
      label: `↩️ Restore v${v.version_number}`
    }));
    options.push({ id: 'back', label: '⬅️ Back' });

    const selection = await askOptionWithButtons(
      conversation,
      ctx,
      'Select a version to restore.',
      options,
      { prefix: 'call-script-version', columns: 2, ensureActive: safeEnsureActive }
    );
    if (!selection || selection.id === 'back') return;
    const versionNumber = Number(selection.id);
    if (Number.isNaN(versionNumber)) {
      await ctx.reply('❌ Invalid version selected.');
      return;
    }
    const version = await getScriptVersion(script.id, 'call', versionNumber);
    safeEnsureActive();
    if (!version || !version.payload) {
      await ctx.reply('❌ Version payload not found.');
      return;
    }
    const confirmRestore = await confirm(conversation, ctx, `Restore version v${versionNumber}?`, safeEnsureActive);
    if (!confirmRestore) {
      await ctx.reply('Restore cancelled.');
      return;
    }
    await storeScriptVersionSnapshot(script, 'call', ctx);
    const payload = stripUndefined({ ...version.payload });
    delete payload.capture_group;
    const opId = getCurrentOpId(ctx) || `user_${ctx?.from?.id || 'unknown'}`;
    const idempotencyKey = `restore_call_script_${script.id}_v${versionNumber}_${opId}`.slice(0, 96);
    const updated = await runWithActionWatchdog(
      ctx,
      'Restoring call script version',
      'call_restore_version',
      () => updateCallScript(script.id, payload, { idempotencyKey })
    );
    await ctx.reply(`✅ Script restored to v${versionNumber} (${escapeMarkdown(updated.name)}).`, { parse_mode: 'Markdown' });
  } catch (error) {
    logScriptsError('Version restore failed', error);
    await ctx.reply(`❌ Failed to restore version: ${error.message}`);
  }
}

async function showCallScriptDetail(conversation, ctx, script, ensureActive) {
  const safeEnsureActive = resolveEnsureActive(ensureActive, ctx);
  let viewing = true;
  while (viewing) {
    const summary = formatCallScriptSummary(script);
    await ctx.reply(summary, { parse_mode: 'Markdown' });

    const action = await askOptionWithButtons(
      conversation,
      ctx,
      'Choose an action for this script.',
      [
        { id: 'preview', label: '📞 Preview' },
        { id: 'edit', label: '✏️ Edit' },
        { id: 'clone', label: '🧬 Clone' },
        { id: 'versions', label: '🗂️ Versions' },
        { id: 'delete', label: '🗑️ Delete' },
        { id: 'back', label: '⬅️ Back' }
      ],
      { prefix: 'call-script-action', columns: 2, ensureActive: safeEnsureActive }
    );

    switch (action.id) {
      case 'preview':
        await previewCallScript(conversation, ctx, script, safeEnsureActive);
        break;
      case 'edit':
        await editCallScriptFlow(conversation, ctx, script, safeEnsureActive);
        try {
          script = await fetchCallScriptById(script.id);
        } catch (error) {
          logScriptsError('Failed to refresh call script after edit', error);
          await ctx.reply(formatScriptsApiError(error, 'Failed to refresh script details'));
          viewing = false;
        }
        break;
      case 'clone':
        await cloneCallScriptFlow(conversation, ctx, script, safeEnsureActive);
        break;
      case 'versions':
        await showCallScriptVersions(conversation, ctx, script, safeEnsureActive);
        break;
      case 'delete':
        await deleteCallScriptFlow(conversation, ctx, script, safeEnsureActive);
        viewing = false;
        break;
      case 'back':
        viewing = false;
        break;
      default:
        break;
    }
  }
}

async function listCallScriptsFlow(conversation, ctx, ensureActive) {
  const safeEnsureActive = resolveEnsureActive(ensureActive, ctx);
  try {
    const scripts = await fetchCallScripts();
    safeEnsureActive();
    const list = Array.isArray(scripts) ? scripts : [];
    const manageableScripts = list.filter((script) => script && Number.isFinite(Number(script.id)));
    const readOnlyScripts = list.filter((script) => script && !Number.isFinite(Number(script.id)));

    if (!manageableScripts.length && !readOnlyScripts.length) {
      await ctx.reply('ℹ️ No call scripts found. Use the create action to add one.');
      return;
    }

    if (!manageableScripts.length && readOnlyScripts.length) {
      const previewNames = readOnlyScripts.slice(0, 10).map((script, index) => `${index + 1}. ${script.name}`);
      await ctx.reply(
        `⚠️ Call scripts were returned by the API, but they are missing numeric IDs, so they cannot be managed from this menu.\n\n${previewNames.join('\n')}`
      );
      return;
    }

    if (readOnlyScripts.length) {
      console.warn('[scripts] call script entries missing numeric id; ignoring unmanageable records', {
        count: readOnlyScripts.length
      });
    }

    const summaryLines = manageableScripts.slice(0, 15).map((script, index) => {
      const parts = [`${index + 1}. ${script.name}`];
      if (script.description) {
        parts.push(`– ${script.description}`);
      }
      return parts.join(' ');
    });

    let message = '☎️ Call Scripts\n\n';
    message += summaryLines.join('\n');
    if (manageableScripts.length > 15) {
      message += `\n… and ${manageableScripts.length - 15} more.`;
    }
    if (readOnlyScripts.length > 0) {
      message += `\n\n⚠️ ${readOnlyScripts.length} script record(s) were skipped because they are missing IDs.`;
    }
    message += '\n\nSelect a script below to view details.';

    await ctx.reply(message);

    const options = manageableScripts.map((script) => ({
      id: script.id.toString(),
      label: `📄 ${script.name}`
    }));
    options.push({ id: 'back', label: '⬅️ Back' });

    const selection = await askOptionWithButtons(
      conversation,
      ctx,
      'Choose a call script to manage.',
      options,
      { prefix: 'call-script-select', columns: 1, formatLabel: (option) => option.label, ensureActive: safeEnsureActive }
    );

    if (!selection || !selection.id) {
      await ctx.reply('❌ No selection received. Please try again.');
      return;
    }

    if (selection.id === 'back') {
      return;
    }

    const scriptId = Number(selection.id);
    if (Number.isNaN(scriptId)) {
      await ctx.reply('❌ Invalid script selection.');
      return;
    }

    try {
      const script = await fetchCallScriptById(scriptId);
      safeEnsureActive();
      if (!script) {
        await ctx.reply('❌ Script not found.');
        return;
      }

      await showCallScriptDetail(conversation, ctx, script, safeEnsureActive);
    } catch (error) {
      logScriptsError('Failed to load call script details', error);
      await ctx.reply(formatScriptsApiError(error, 'Failed to load script details'));
    }
  } catch (error) {
    logScriptsError('Failed to list scripts', error);
    await ctx.reply(formatScriptsApiError(error, 'Failed to list call scripts'));
  }
}

async function inboundDefaultScriptMenu(conversation, ctx, ensureActive) {
  const safeEnsureActive = resolveEnsureActive(ensureActive, ctx);
  let open = true;
  while (open) {
    let current = null;
    try {
      current = await fetchInboundDefaultScript();
      safeEnsureActive();
    } catch (error) {
      logScriptsError('Failed to fetch inbound default script', error);
      await ctx.reply(formatScriptsApiError(error, 'Failed to load inbound default script'));
      return;
    }

    const currentLabel = current?.mode === 'script' && current?.script
      ? `📥 Current inbound default: ${current.script.name} (ID ${current.script_id})`
      : '📥 Current inbound default: Built-in default';
    const previewLine = current?.mode === 'script' && current?.script?.first_message
      ? `🗨️ First message: ${current.script.first_message.slice(0, 140)}${current.script.first_message.length > 140 ? '…' : ''}`
      : null;

    const action = await askOptionWithButtons(
      conversation,
      ctx,
      `${currentLabel}${previewLine ? `\n${previewLine}` : ''}\n\nChoose an action.`,
      [
        { id: 'set', label: '✅ Set default' },
        { id: 'clear', label: '↩️ Revert to built-in' },
        { id: 'back', label: '⬅️ Back' }
      ],
      { prefix: 'inbound-default', columns: 1, ensureActive: safeEnsureActive }
    );

    switch (action.id) {
      case 'set': {
        let scripts;
        try {
          scripts = await fetchCallScripts();
          safeEnsureActive();
        } catch (error) {
          logScriptsError('Failed to fetch call scripts', error);
          await ctx.reply(formatScriptsApiError(error, 'Failed to load call scripts'));
          break;
        }

        const manageableScripts = scripts.filter((script) => script && Number.isFinite(Number(script.id)));
        if (!manageableScripts.length) {
          if (scripts.length) {
            await ctx.reply('⚠️ No selectable call scripts were returned (missing numeric IDs).');
            break;
          }
          await ctx.reply('ℹ️ No call scripts available. Create one first.');
          break;
        }

        const options = manageableScripts.map((script) => ({
          id: String(Number(script.id)),
          label: `📄 ${script.name}`
        }));
        options.push({ id: 'back', label: '⬅️ Back' });

        const selection = await askOptionWithButtons(
          conversation,
          ctx,
          'Select a script to use as the inbound default.',
          options,
          { prefix: 'inbound-default-select', columns: 1, ensureActive: safeEnsureActive }
        );

        if (!selection || !selection.id || selection.id === 'back') {
          break;
        }

        const scriptId = Number(selection.id);
        if (Number.isNaN(scriptId)) {
          await ctx.reply('❌ Invalid script selection.');
          break;
        }

        try {
          const result = await runWithActionWatchdog(
            ctx,
            'Updating inbound default script',
            'call_inbound_default_set',
            () => setInboundDefaultScript(scriptId)
          );
          safeEnsureActive();
          await ctx.reply(`✅ Inbound default set to ${result?.script?.name || 'selected script'}.`);
        } catch (error) {
          logScriptsError('Failed to set inbound default script', error);
          await ctx.reply(formatScriptsApiError(error, 'Failed to set inbound default script'));
        }
        break;
      }
      case 'clear':
        try {
          await runWithActionWatchdog(
            ctx,
            'Clearing inbound default script',
            'call_inbound_default_clear',
            () => clearInboundDefaultScript()
          );
          safeEnsureActive();
          await ctx.reply('✅ Inbound default reverted to built-in settings.');
        } catch (error) {
          logScriptsError('Failed to clear inbound default script', error);
          await ctx.reply(formatScriptsApiError(error, 'Failed to clear inbound default script'));
        }
        break;
      case 'back':
        open = false;
        break;
      default:
        break;
    }
  }
}

async function callScriptsMenu(conversation, ctx, ensureActive) {
  const safeEnsureActive = resolveEnsureActive(ensureActive, ctx);
  let open = true;
  while (open) {
    try {
      const action = await askOptionWithButtons(
        conversation,
        ctx,
        '☎️ *Call Script Designer*\nChoose an action.',
        [
          { id: 'list', label: '📄 List scripts' },
          { id: 'create', label: '➕ Create script' },
          { id: 'incoming', label: '📥 Incoming default' },
          { id: 'back', label: '⬅️ Back' }
        ],
        { prefix: 'call-script-main', columns: 1, ensureActive: safeEnsureActive }
      );

      switch (action.id) {
        case 'list':
          await listCallScriptsFlow(conversation, ctx, safeEnsureActive);
          break;
        case 'create':
          await createCallScriptFlow(conversation, ctx, safeEnsureActive);
          break;
        case 'incoming':
          await inboundDefaultScriptMenu(conversation, ctx, safeEnsureActive);
          break;
        case 'back':
          open = false;
          break;
        default:
          break;
      }
    } catch (error) {
      if (error instanceof OperationCancelledError) {
        throw error;
      }
      logScriptsError('Call scripts menu step failed', error);
      await ctx.reply(formatScriptsApiError(error, 'Call script menu failed'));
    }
  }
}

async function fetchSmsScripts({ includeContent = false } = {}) {
  const data = await scriptsApiRequest({
    method: 'get',
    url: '/api/sms/scripts',
    params: {
      include_builtins: true,
      detailed: includeContent
    }
  });
  const payload = Array.isArray(data)
    ? { scripts: data }
    : (data?.data && typeof data.data === 'object' ? data.data : data) || {};

  const scriptsMap = new Map();
  const customSource = Array.isArray(payload.scripts)
    ? payload.scripts
    : Array.isArray(payload.custom)
      ? payload.custom
      : [];
  const builtinSource = Array.isArray(payload.builtin)
    ? payload.builtin
    : Array.isArray(payload.builtin_scripts)
      ? payload.builtin_scripts
      : [];

  customSource.forEach((script) => {
    const scriptName = script?.name || script?.script_name;
    if (!scriptName) return;
    scriptsMap.set(scriptName, {
      ...script,
      name: scriptName,
      is_builtin: !!script.is_builtin,
      metadata: script.metadata || {}
    });
  });

  builtinSource.forEach((script) => {
    const scriptName = script?.name || script?.script_name;
    if (!scriptName || scriptsMap.has(scriptName)) return;
    scriptsMap.set(scriptName, {
      ...script,
      name: scriptName,
      is_builtin: true,
      metadata: script.metadata || {}
    });
  });

  if (!scriptsMap.size && Array.isArray(payload.available_scripts)) {
    payload.available_scripts.forEach((scriptName) => {
      if (!scriptName || scriptsMap.has(scriptName)) return;
      scriptsMap.set(scriptName, {
        name: scriptName,
        description: null,
        content: '',
        metadata: {},
        is_builtin: true
      });
    });
  }

  return Array.from(scriptsMap.values());
}

async function fetchSmsScriptByName(name, { detailed = true } = {}) {
  const data = await scriptsApiRequest({
    method: 'get',
    url: `/api/sms/scripts/${encodeURIComponent(name)}`,
    params: { detailed }
  });

  const script = data.script && typeof data.script === 'object'
    ? data.script
    : (data.script_name
      ? {
        name: data.script_name,
        content: data.script || data.original_script || '',
        description: null,
        metadata: {},
        is_builtin: true
      }
      : null);
  if (script) {
    script.name = script.name || script.script_name || name;
    script.is_builtin = !!script.is_builtin;
    script.metadata = script.metadata || {};
  }
  return script;
}

async function createSmsScript(payload, options = {}) {
  const headers = {};
  const idempotencyKey = String(options.idempotencyKey || '').trim();
  if (idempotencyKey) {
    headers['Idempotency-Key'] = idempotencyKey;
  }
  const data = await scriptsApiRequest({
    method: 'post',
    url: '/api/sms/scripts',
    data: payload,
    headers: Object.keys(headers).length ? headers : undefined
  });
  return data.script;
}

async function updateSmsScript(name, payload, options = {}) {
  const headers = {};
  const idempotencyKey = String(options.idempotencyKey || '').trim();
  if (idempotencyKey) {
    headers['Idempotency-Key'] = idempotencyKey;
  }
  const data = await scriptsApiRequest({
    method: 'put',
    url: `/api/sms/scripts/${encodeURIComponent(name)}`,
    data: payload,
    headers: Object.keys(headers).length ? headers : undefined
  });
  return data.script;
}

async function deleteSmsScript(name, options = {}) {
  const headers = {};
  const idempotencyKey = String(options.idempotencyKey || '').trim();
  if (idempotencyKey) {
    headers['Idempotency-Key'] = idempotencyKey;
  }
  await scriptsApiRequest({
    method: 'delete',
    url: `/api/sms/scripts/${encodeURIComponent(name)}`,
    headers: Object.keys(headers).length ? headers : undefined
  });
}

async function requestSmsScriptPreview(name, payload) {
  const data = await scriptsApiRequest({
    method: 'post',
    url: `/api/sms/scripts/${encodeURIComponent(name)}/preview`,
    data: payload
  });
  if (data?.preview && typeof data.preview === 'object') {
    return data.preview;
  }
  if (typeof data?.content === 'string' || typeof data?.script === 'string') {
    return {
      to: data.to || payload.to,
      message_sid: data.message_sid || data.messageSid || 'preview',
      content: data.content || data.script || ''
    };
  }
  const error = new Error('Scripts API returned an invalid preview response.');
  error.isScriptsApiError = true;
  error.reason = 'api_failure';
  throw error;
}

function formatSmsScriptSummary(script) {
  const summary = [];
  summary.push(`${script.is_builtin ? '📦' : '📛'} *${escapeMarkdown(script.name)}*`);
  if (script.description) {
    summary.push(`📝 ${escapeMarkdown(script.description)}`);
  }
  summary.push(script.is_builtin ? '🏷️ Type: Built-in (read-only)' : '🏷️ Type: Custom script');

  const personaSummary = buildPersonaSummaryFromOverrides(script.metadata?.persona);
  if (personaSummary.length) {
    personaSummary.forEach((line) => summary.push(`• ${escapeMarkdown(line)}`));
  }

  const placeholders = extractScriptVariables(script.content || '');
  if (placeholders.length) {
    summary.push(`🧩 Placeholders: ${placeholders.map(escapeMarkdown).join(', ')}`);
  }

  if (script.content) {
    const snippet = script.content.substring(0, 160);
    summary.push(`💬 Preview: ${escapeMarkdown(snippet)}${script.content.length > 160 ? '…' : ''}`);
  }

  summary.push(
    `📅 Updated: ${escapeMarkdown(new Date(script.updated_at || script.created_at).toLocaleString())}`
  );

  return summary.join('\n');
}

async function createSmsScriptFlow(conversation, ctx) {
  const name = await promptText(
    conversation,
    ctx,
    '🆕 *Script name*\nUse lowercase letters, numbers, dashes, or underscores.',
    {
      allowEmpty: false,
      parse: (value) => {
        const trimmed = value.trim().toLowerCase();
        if (!/^[a-z0-9_-]+$/.test(trimmed)) {
          throw new Error('Use only letters, numbers, underscores, or dashes.');
        }
        return trimmed;
      }
    }
  );
  if (!name) {
    await ctx.reply('❌ Script creation cancelled.');
    return;
  }

  const description = await promptText(
    conversation,
    ctx,
    '📝 Optional description (or type skip).',
    { allowEmpty: true, allowSkip: true, parse: (value) => value.trim() }
  );
  if (description === null) {
    await ctx.reply('❌ Script creation cancelled.');
    return;
  }

  const content = await promptText(
    conversation,
    ctx,
    '💬 Provide the SMS content. You can include placeholders like {code}.',
    { allowEmpty: false, parse: (value) => value.trim() }
  );
  if (!content) {
    await ctx.reply('❌ Script creation cancelled.');
    return;
  }

  const metadata = {};
  const configurePersona = await confirm(conversation, ctx, 'Add persona guidance for this script?');
  if (configurePersona) {
    const personaResult = await collectPersonaConfig(conversation, ctx, {}, { allowCancel: true });
    if (!personaResult) {
      await ctx.reply('❌ Script creation cancelled.');
      return;
    }
    const overrides = toPersonaOverrides(personaResult);
    if (overrides) {
      metadata.persona = overrides;
    }
  }

  const payload = {
    name,
    description: description === undefined ? null : (description.length ? description : null),
    content,
    metadata: Object.keys(metadata).length ? metadata : undefined,
    created_by: ctx.from.id.toString()
  };

  try {
    const mutationGate = beginMutationRequest(ctx, 'sms:create', name);
    if (!mutationGate.allowed) {
      await ctx.reply('⚠️ SMS script creation is already processing or just completed. Please wait a moment.');
      return;
    }
    try {
      const opId = getCurrentOpId(ctx) || `user_${ctx?.from?.id || 'unknown'}`;
      const idempotencyKey = `create_sms_script_${name}_${opId}`.slice(0, 96);
      const script = await runWithActionWatchdog(
        ctx,
        'Creating SMS script',
        'sms_create',
        () => createSmsScript(payload, { idempotencyKey })
      );
      finishMutationRequest(mutationGate.key, 'done');
      await storeScriptVersionSnapshot(script, 'sms', ctx);
      await ctx.reply(`✅ SMS script *${escapeMarkdown(script.name)}* created.`, { parse_mode: 'Markdown' });
    } catch (error) {
      finishMutationRequest(mutationGate.key, 'failed');
      throw error;
    }
  } catch (error) {
    logScriptsError('Failed to create SMS script', error);
    await ctx.reply(formatScriptsApiError(error, 'Failed to create SMS script'));
  }
}

async function editSmsScriptFlow(conversation, ctx, script) {
  if (script.is_builtin) {
    await ctx.reply('ℹ️ Built-in scripts are read-only. Clone the script to modify it.');
    return;
  }

  const updates = { updated_by: ctx.from.id.toString() };

  const description = await promptText(
    conversation,
    ctx,
    '📝 Update description (or type skip).',
    { allowEmpty: true, allowSkip: true, defaultValue: script.description || '', parse: (value) => value.trim() }
  );
  if (description === null) {
    await ctx.reply('❌ Update cancelled.');
    return;
  }
  if (description !== undefined) {
    updates.description = description.length ? description : null;
  }

  const updateContent = await confirm(conversation, ctx, 'Update the SMS content?');
  if (updateContent) {
    const content = await promptText(
      conversation,
      ctx,
      '💬 Enter the new SMS content.',
      { allowEmpty: false, defaultValue: script.content, parse: (value) => value.trim() }
    );
    if (!content) {
      await ctx.reply('❌ Update cancelled.');
      return;
    }
    updates.content = content;
  }

  const adjustPersona = await confirm(conversation, ctx, 'Update persona guidance for this script?');
  if (adjustPersona) {
    const personaResult = await collectPersonaConfig(conversation, ctx, {}, { allowCancel: true });
    if (!personaResult) {
      await ctx.reply('❌ Update cancelled.');
      return;
    }
    const overrides = toPersonaOverrides(personaResult);
    const metadata = { ...(script.metadata || {}) };
    if (overrides) {
      metadata.persona = overrides;
    } else {
      delete metadata.persona;
    }
    updates.metadata = metadata;
  } else if (script.metadata?.persona) {
    const clearPersona = await confirm(conversation, ctx, 'Remove existing persona guidance?');
    if (clearPersona) {
      const metadata = { ...(script.metadata || {}) };
      delete metadata.persona;
      updates.metadata = metadata;
    }
  }

  const updateKeys = Object.keys(updates).filter((key) => key !== 'updated_by');
  if (!updateKeys.length) {
    await ctx.reply('ℹ️ No changes made.');
    return;
  }

  try {
    const mutationGate = beginMutationRequest(ctx, 'sms:update', script.name);
    if (!mutationGate.allowed) {
      await ctx.reply('⚠️ SMS script update is already processing or just completed. Please wait a moment.');
      return;
    }
    await storeScriptVersionSnapshot(script, 'sms', ctx);
    try {
      const opId = getCurrentOpId(ctx) || `user_${ctx?.from?.id || 'unknown'}`;
      const idempotencyKey = `update_sms_script_${script.name}_${opId}`.slice(0, 96);
      const updated = await runWithActionWatchdog(
        ctx,
        'Updating SMS script',
        'sms_update',
        () => updateSmsScript(script.name, stripUndefined(updates), { idempotencyKey })
      );
      finishMutationRequest(mutationGate.key, 'done');
      await ctx.reply(`✅ SMS script *${escapeMarkdown(updated.name)}* updated.`, { parse_mode: 'Markdown' });
    } catch (error) {
      finishMutationRequest(mutationGate.key, 'failed');
      throw error;
    }
  } catch (error) {
    logScriptsError('Failed to update SMS script', error);
    await ctx.reply(formatScriptsApiError(error, 'Failed to update SMS script'));
  }
}

async function cloneSmsScriptFlow(conversation, ctx, script) {
  const name = await promptText(
    conversation,
    ctx,
    `🆕 Enter a name for the clone of *${escapeMarkdown(script.name)}*.`,
    {
      allowEmpty: false,
      parse: (value) => {
        const trimmed = value.trim().toLowerCase();
        if (!/^[a-z0-9_-]+$/.test(trimmed)) {
          throw new Error('Use only letters, numbers, underscores, or dashes.');
        }
        return trimmed;
      }
    }
  );
  if (!name) {
    await ctx.reply('❌ Clone cancelled.');
    return;
  }

  const description = await promptText(
    conversation,
    ctx,
    '📝 Optional description for the cloned script (or type skip).',
    { allowEmpty: true, allowSkip: true, defaultValue: script.description || '', parse: (value) => value.trim() }
  );
  if (description === null) {
    await ctx.reply('❌ Clone cancelled.');
    return;
  }

  const payload = {
    name,
    description: description === undefined ? script.description : (description.length ? description : null),
    content: script.content,
    metadata: script.metadata,
    created_by: ctx.from.id.toString()
  };

  const cloneGate = beginCloneRequest(ctx, script.name, name);
  if (!cloneGate.allowed) {
    await ctx.reply('⚠️ Clone already in progress or just completed. Please wait a moment before trying again.');
    return;
  }

  try {
    const opId = getCurrentOpId(ctx) || `user_${ctx?.from?.id || 'unknown'}`;
    const idempotencyKey = `clone_sms_script_${script.name}_${name}_${opId}`.slice(0, 96);
    const cloned = await runWithActionWatchdog(
      ctx,
      'Cloning SMS script',
      'sms_clone',
      () => createSmsScript(payload, { idempotencyKey })
    );
    finishCloneRequest(cloneGate.key, 'done');
    await ctx.reply(`✅ Script cloned as *${escapeMarkdown(cloned.name)}*.`, { parse_mode: 'Markdown' });
  } catch (error) {
    finishCloneRequest(cloneGate.key, 'failed');
    logScriptsError('Failed to clone SMS script', error);
    await ctx.reply(formatScriptsApiError(error, 'Failed to clone SMS script'));
  }
}

async function deleteSmsScriptFlow(conversation, ctx, script) {
  if (script.is_builtin) {
    await ctx.reply('ℹ️ Built-in scripts cannot be deleted.');
    return;
  }

  const confirmed = await confirm(conversation, ctx, `Delete SMS script *${escapeMarkdown(script.name)}*?`);
  if (!confirmed) {
    await ctx.reply('Deletion cancelled.');
    return;
  }

  try {
    const mutationGate = beginMutationRequest(ctx, 'sms:delete', script.name);
    if (!mutationGate.allowed) {
      await ctx.reply('⚠️ SMS script deletion is already processing or just completed. Please wait a moment.');
      return;
    }
    await storeScriptVersionSnapshot(script, 'sms', ctx);
    try {
      const opId = getCurrentOpId(ctx) || `user_${ctx?.from?.id || 'unknown'}`;
      const idempotencyKey = `delete_sms_script_${script.name}_${opId}`.slice(0, 96);
      await runWithActionWatchdog(
        ctx,
        'Deleting SMS script',
        'sms_delete',
        () => deleteSmsScript(script.name, { idempotencyKey })
      );
      finishMutationRequest(mutationGate.key, 'done');
      await ctx.reply(`🗑️ Script *${escapeMarkdown(script.name)}* deleted.`, { parse_mode: 'Markdown' });
    } catch (error) {
      finishMutationRequest(mutationGate.key, 'failed');
      throw error;
    }
  } catch (error) {
    logScriptsError('Failed to delete SMS script', error);
    await ctx.reply(formatScriptsApiError(error, 'Failed to delete SMS script'));
  }
}

async function showSmsScriptVersions(conversation, ctx, script) {
  try {
    const versions = await listScriptVersions(script.name, 'sms', 8);
    if (!versions.length) {
      await ctx.reply('ℹ️ No saved versions yet. Versions are stored on edit/delete.');
      return;
    }
    const lines = versions.map((v) => `v${v.version_number} • ${new Date(v.created_at).toLocaleString()}`);
    await ctx.reply(`🗂️ Saved versions\n${lines.join('\n')}`);

    const options = versions.map((v) => ({
      id: String(v.version_number),
      label: `↩️ Restore v${v.version_number}`
    }));
    options.push({ id: 'back', label: '⬅️ Back' });

    const selection = await askOptionWithButtons(
      conversation,
      ctx,
      'Select a version to restore.',
      options,
      { prefix: 'sms-script-version', columns: 2 }
    );
    if (!selection || selection.id === 'back') return;
    const versionNumber = Number(selection.id);
    if (Number.isNaN(versionNumber)) {
      await ctx.reply('❌ Invalid version selected.');
      return;
    }
    const version = await getScriptVersion(script.name, 'sms', versionNumber);
    if (!version || !version.payload) {
      await ctx.reply('❌ Version payload not found.');
      return;
    }
    const confirmRestore = await confirm(conversation, ctx, `Restore version v${versionNumber}?`);
    if (!confirmRestore) {
      await ctx.reply('Restore cancelled.');
      return;
    }
    await storeScriptVersionSnapshot(script, 'sms', ctx);
    const updated = await runWithActionWatchdog(
      ctx,
      'Restoring SMS script version',
      'sms_restore_version',
      () => updateSmsScript(script.name, stripUndefined(version.payload))
    );
    await ctx.reply(`✅ SMS script restored to v${versionNumber}.`, { parse_mode: 'Markdown' });
    try {
      script = await fetchSmsScriptByName(script.name, { detailed: true });
    } catch (_) {}
  } catch (error) {
    logScriptsError('SMS version restore failed', error);
    await ctx.reply(`❌ Failed to restore version: ${error.message}`);
  }
}

async function previewSmsScript(conversation, ctx, script) {
  const to = await promptText(
    conversation,
    ctx,
    '📱 Enter the destination number (E.164 format, e.g., +1234567890).',
    { allowEmpty: false, parse: (value) => value.trim() }
  );
  if (!to) {
    await ctx.reply('❌ Preview cancelled.');
    return;
  }

  if (!/^\+[1-9]\d{1,14}$/.test(to)) {
    await ctx.reply('❌ Invalid phone number format. Preview cancelled.');
    return;
  }

  const placeholders = extractScriptVariables(script.content || '');
  let variables = {};
  if (placeholders.length > 0) {
    await ctx.reply('🧩 This script includes placeholders. Provide values or type skip to leave unchanged.');
    const values = await collectPlaceholderValues(conversation, ctx, placeholders);
    if (values === null) {
      await ctx.reply('❌ Preview cancelled.');
      return;
    }
    variables = values;
  }

  const payload = {
    to,
    variables,
    persona_overrides: script.metadata?.persona
  };

  if (!Object.keys(variables).length) {
    payload.variables = {};
  }

  if (!payload.persona_overrides) {
    delete payload.persona_overrides;
  }

  try {
    const preview = await runWithActionWatchdog(
      ctx,
      'Sending SMS preview',
      'sms_preview',
      () => requestSmsScriptPreview(script.name, payload)
    );
    const previewContent = String(preview?.content || '');
    const snippet = previewContent.substring(0, 200);
    const messageSid = preview?.message_sid || preview?.messageSid || 'n/a';
    const toValue = preview?.to || to;
    await ctx.reply(
      `✅ Preview SMS sent!\n\n📱 To: ${toValue}\n🆔 Message SID: \`${messageSid}\`\n💬 Content: ${escapeMarkdown(snippet)}${previewContent.length > 200 ? '…' : ''}`,
      { parse_mode: 'Markdown' }
    );
  } catch (error) {
    logScriptsError('Failed to send SMS preview', error);
    await ctx.reply(formatScriptsApiError(error, 'Failed to send SMS preview'));
  }
}

async function showSmsScriptDetail(conversation, ctx, script) {
  let viewing = true;
  while (viewing) {
    const summary = formatSmsScriptSummary(script);
    await ctx.reply(summary, { parse_mode: 'Markdown' });

    const actions = [
      { id: 'preview', label: '📲 Preview' },
      { id: 'clone', label: '🧬 Clone' }
    ];

    if (!script.is_builtin) {
      actions.splice(1, 0, { id: 'edit', label: '✏️ Edit' });
      actions.splice(2, 0, { id: 'versions', label: '🗂️ Versions' });
      actions.push({ id: 'delete', label: '🗑️ Delete' });
    }

    actions.push({ id: 'back', label: '⬅️ Back' });

    const action = await askOptionWithButtons(
      conversation,
      ctx,
      'Choose an action for this SMS script.',
      actions,
      { prefix: 'sms-script-action', columns: 2 }
    );

    switch (action.id) {
      case 'preview':
        await previewSmsScript(conversation, ctx, script);
        break;
      case 'edit':
        await editSmsScriptFlow(conversation, ctx, script);
        try {
          script = await fetchSmsScriptByName(script.name, { detailed: true });
        } catch (error) {
          logScriptsError('Failed to refresh SMS script after edit', error);
          await ctx.reply(formatScriptsApiError(error, 'Failed to refresh script details'));
          viewing = false;
        }
        break;
      case 'clone':
        await cloneSmsScriptFlow(conversation, ctx, script);
        break;
      case 'versions':
        await showSmsScriptVersions(conversation, ctx, script);
        break;
      case 'delete':
        await deleteSmsScriptFlow(conversation, ctx, script);
        viewing = false;
        break;
      case 'back':
        viewing = false;
        break;
      default:
        break;
    }
  }
}

async function listSmsScriptsFlow(conversation, ctx) {
  try {
    const scripts = await fetchSmsScripts();
    if (!scripts.length) {
      await ctx.reply('ℹ️ No SMS scripts found. Use the create action to add one.');
      return;
    }

    const custom = scripts.filter((script) => !script.is_builtin);
    const builtin = scripts.filter((script) => script.is_builtin);

    let message = '💬 SMS Scripts\n\n';
    if (custom.length) {
      message += 'Custom scripts:\n';
      message += custom
        .slice(0, 15)
        .map((script) => `• ${script.name}${script.description ? ` – ${script.description}` : ''}`)
        .join('\n');
      message += '\n\n';
    } else {
      message += 'No custom scripts yet.\n\n';
    }

    if (builtin.length) {
      message += 'Built-in scripts:\n';
      message += builtin
        .map((script) => `• ${script.name}${script.description ? ` – ${script.description}` : ''}`)
        .join('\n');
      message += '\n\n';
    }

    message += 'Select a script below to view details.';
    await ctx.reply(message);

    const options = scripts.map((script) => ({
      id: script.name,
      label: `${script.is_builtin ? '📦' : '📝'} ${script.name}`,
      is_builtin: script.is_builtin
    }));
    options.push({ id: 'back', label: '⬅️ Back' });

    const selection = await askOptionWithButtons(
      conversation,
      ctx,
      'Choose an SMS script to manage.',
      options,
      { prefix: 'sms-script-select', columns: 1, formatLabel: (option) => option.label }
    );

    if (!selection?.id || selection.id === 'back') {
      return;
    }

    try {
      const script = await fetchSmsScriptByName(selection.id, { detailed: true });
      if (!script) {
        await ctx.reply('❌ Script not found.');
        return;
      }

      await showSmsScriptDetail(conversation, ctx, script);
    } catch (error) {
      logScriptsError('Failed to load SMS script details', error);
      await ctx.reply(formatScriptsApiError(error, 'Failed to load script details'));
    }
  } catch (error) {
    logScriptsError('Failed to list SMS scripts', error);
    await ctx.reply(formatScriptsApiError(error, 'Failed to list SMS scripts'));
  }
}

async function smsScriptsMenu(conversation, ctx) {
  let open = true;
  while (open) {
    try {
      const action = await askOptionWithButtons(
        conversation,
        ctx,
        '💬 *SMS Script Designer*\nChoose an action.',
        [
          { id: 'list', label: '📄 List scripts' },
          { id: 'create', label: '➕ Create script' },
          { id: 'back', label: '⬅️ Back' }
        ],
        { prefix: 'sms-script-main', columns: 1 }
      );

      switch (action.id) {
        case 'list':
          await listSmsScriptsFlow(conversation, ctx);
          break;
        case 'create':
          await createSmsScriptFlow(conversation, ctx);
          break;
        case 'back':
          open = false;
          break;
        default:
          break;
      }
    } catch (error) {
      if (error instanceof OperationCancelledError) {
        throw error;
      }
      logScriptsError('SMS scripts menu step failed', error);
      await ctx.reply(formatScriptsApiError(error, 'SMS script menu failed'));
    }
  }
}

async function scriptsFlow(conversation, ctx) {
  const opId = startOperation(ctx, 'scripts');
  const ensureActive = () => ensureOperationActive(ctx, opId);

  try {
    const user = await new Promise((resolve) => getUser(ctx.from.id, resolve));
    ensureActive();
    if (!user) {
      await ctx.reply('❌ You are not authorized to use this bot.');
      return;
    }

    const adminStatus = await new Promise((resolve) => isAdmin(ctx.from.id, resolve));
    ensureActive();
    if (!adminStatus) {
      await ctx.reply('❌ This command is for administrators only.');
      return;
    }

    try {
      // Warm persona cache so downstream selections have up-to-date personas.
      await getBusinessOptions();
      ensureActive();
    } catch (error) {
      console.error('Failed to load persona options for scripts command:', error?.message || error);
      await ctx.reply(formatScriptsApiError(error, 'Failed to initialize script designer'));
      return;
    }

    let active = true;
    while (active) {
      const selection = await askOptionWithButtons(
        conversation,
        ctx,
        '🧰 *Script Designer*\nChoose which scripts to manage.',
        [
          { id: 'call', label: '☎️ Call scripts' },
          { id: 'sms', label: '💬 SMS scripts' },
          { id: 'email', label: '📧 Email templates' },
          { id: 'exit', label: '🚪 Exit' }
        ],
        { prefix: 'script-channel', columns: 1, ensureActive }
      );

      switch (selection.id) {
        case 'call':
          await callScriptsMenu(conversation, ctx, ensureActive);
          break;
        case 'sms':
          await smsScriptsMenu(conversation, ctx, ensureActive);
          break;
        case 'email':
          await emailTemplatesFlow(conversation, ctx, { ensureActive });
          break;
        case 'exit':
          active = false;
          break;
        default:
          break;
      }
    }

    await ctx.reply('✅ Script designer closed.');
  } catch (error) {
    if (error instanceof OperationCancelledError) {
      console.warn('Scripts flow cancelled:', error.message);
      await ctx.reply('⚠️ Script designer session was interrupted. Run /scripts to continue.');
      return;
    }
    console.error('Scripts flow unexpected error:', error?.message || error);
    await ctx.reply(formatScriptsApiError(error, 'Script command failed'));
    return;
  } finally {
    if (ctx.session?.currentOp?.id === opId) {
      ctx.session.currentOp = null;
    }
  }
}

function resetScriptsRuntimeStateForTests() {
  cloneRequestState.clear();
  mutationRequestState.clear();
  scriptsCircuitState.state = 'closed';
  scriptsCircuitState.consecutiveFailures = 0;
  scriptsCircuitState.openedAt = 0;
  scriptsCircuitState.halfOpenInFlight = false;
  scriptsCircuitState.lastError = null;
  resetScriptsMetricsWindow(Date.now());
  scriptsMetricsState.lastAlertAt = 0;
  scriptsApiExecutorState.request = (requestOptions) => scriptsApi.request(requestOptions);
}

function setScriptsApiExecutorForTests(executor) {
  if (typeof executor === 'function') {
    scriptsApiExecutorState.request = executor;
    return;
  }
  scriptsApiExecutorState.request = (requestOptions) => scriptsApi.request(requestOptions);
}

function getScriptsCircuitSnapshot() {
  return {
    ...scriptsCircuitState
  };
}

function getScriptsMetricsSnapshot() {
  return {
    windowStart: scriptsMetricsState.windowStart,
    counts: { ...scriptsMetricsState.counts },
    actions: { ...scriptsMetricsState.actions },
    lastAlertAt: scriptsMetricsState.lastAlertAt
  };
}

function registerScriptsCommand(bot) {
  bot.command('scripts', async (ctx) => {
    const user = await new Promise((resolve) => getUser(ctx.from.id, resolve));
    if (!user) {
      return ctx.reply('❌ You are not authorized to use this bot.');
    }

    const adminStatus = await new Promise((resolve) => isAdmin(ctx.from.id, resolve));
    if (!adminStatus) {
      return ctx.reply('❌ This command is for administrators only.');
    }

    try {
      await ctx.conversation.enter('scripts-conversation');
    } catch (error) {
      console.error('Failed to enter scripts conversation:', error?.message || error);
      await ctx.reply(formatScriptsApiError(error, 'Unable to open script designer'));
    }
  });
}

module.exports = {
  scriptsFlow,
  registerScriptsCommand,
  __testables: {
    scriptsApiRequest,
    formatScriptsApiError,
    nonJsonResponseError,
    beginCloneRequest,
    finishCloneRequest,
    beginMutationRequest,
    finishMutationRequest,
    runWithActionWatchdog,
    resetScriptsRuntimeStateForTests,
    setScriptsApiExecutorForTests,
    getScriptsCircuitSnapshot,
    getScriptsMetricsSnapshot
  }
};
