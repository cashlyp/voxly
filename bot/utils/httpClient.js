const axios = require('axios');
const config = require('../config');

const DEFAULT_TIMEOUT_MS = 12000;
const ADMIN_HEADER = 'x-admin-token';
const ADMIN_ORIGINS = new Set();
try {
  if (config.apiUrl) {
    ADMIN_ORIGINS.add(new URL(config.apiUrl).origin);
  }
} catch (_) {}
try {
  if (config.scriptsApiUrl) {
    ADMIN_ORIGINS.add(new URL(config.scriptsApiUrl).origin);
  }
} catch (_) {}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function withRetry(fn, options = {}) {
  const {
    retries = 2,
    baseDelayMs = 300,
    maxDelayMs = 3000,
    jitterRatio = 0.2,
    retryOn = (error) => {
      if (!error) return false;
      if (error.code && ['ECONNRESET', 'ECONNREFUSED', 'ETIMEDOUT', 'ENOTFOUND', 'ECONNABORTED'].includes(error.code)) {
        return true;
      }
      const status = error.response?.status;
      if (status === 429) return true;
      return status >= 500 && status < 600;
    }
  } = options;

  let attempt = 0;
  while (true) {
    try {
      return await fn();
    } catch (error) {
      attempt += 1;
      if (attempt > retries || !retryOn(error)) {
        throw error;
      }
      const retryAfter = Number(error?.response?.headers?.['retry-after']);
      const expDelay = Math.min(maxDelayMs, baseDelayMs * Math.pow(2, attempt - 1));
      const baseDelay = Number.isFinite(retryAfter) && retryAfter > 0
        ? retryAfter * 1000
        : expDelay;
      const jitter = baseDelay * jitterRatio * (Math.random() * 2 - 1);
      const delay = Math.max(0, Math.round(baseDelay + jitter));
      await sleep(delay);
    }
  }
}

function sanitizeUrl(url = '') {
  try {
    const parsed = new URL(url);
    return `${parsed.origin}${parsed.pathname}`;
  } catch (_) {
    return String(url || '');
  }
}

function shouldAttachAdmin(url) {
  if (!url) return false;
  try {
    const origin = new URL(url).origin;
    return ADMIN_ORIGINS.has(origin);
  } catch (_) {
    return false;
  }
}

function withAdminHeader(headers = {}, enableAdmin = true) {
  const merged = { ...headers };
  if (enableAdmin && config.admin?.apiToken && !merged[ADMIN_HEADER]) {
    merged[ADMIN_HEADER] = config.admin.apiToken;
  }
  return merged;
}

function normalizeOptions(options = {}, url) {
  const enableAdmin = options.admin !== false && shouldAttachAdmin(url);
  const headers = withAdminHeader(options.headers || {}, enableAdmin);
  const timeout = Number.isFinite(options.timeout) ? options.timeout : DEFAULT_TIMEOUT_MS;
  return {
    ...options,
    headers,
    timeout
  };
}

function extractRequestId(error) {
  return (
    error?.response?.headers?.['x-request-id'] ||
    error?.response?.headers?.['x-requestid'] ||
    error?.response?.data?.request_id ||
    error?.response?.data?.requestId ||
    null
  );
}

function mapErrorToUserMessage(error) {
  const status = error?.response?.status;
  if (status === 401 || status === 403) {
    return 'Not authorized. Check the ADMIN token / API secret.';
  }
  if (status === 404) {
    return 'API endpoint not found.';
  }
  if (status === 422) {
    return 'Invalid request. Please review the inputs.';
  }
  if (status === 429) {
    return 'Rate limited by the API. Please try again shortly.';
  }
  if (status >= 500) {
    return 'API error. Please try again.';
  }
  if (error?.code === 'ECONNABORTED') {
    return 'API timeout. Please try again.';
  }
  if (error?.request && !error?.response) {
    return 'API unreachable. Please check the server and network.';
  }
  return 'API request failed. Please try again.';
}

function enrichError(error, context = {}) {
  if (!error || typeof error !== 'object') {
    return error;
  }
  const requestId = extractRequestId(error);
  const status = error?.response?.status;
  const method = context.method || error?.config?.method?.toUpperCase() || 'GET';
  const url = sanitizeUrl(context.url || error?.config?.url || '');
  error.userMessage = error.userMessage || mapErrorToUserMessage(error);
  error.apiContext = {
    method,
    url,
    status,
    requestId
  };
  return error;
}

function logError(error, context = {}) {
  const enriched = enrichError(error, context);
  const apiContext = enriched?.apiContext || {};
  console.error('API request failed', apiContext);
}

function getUserMessage(error, fallback = 'API request failed.') {
  if (!error) return fallback;
  return error.userMessage || mapErrorToUserMessage(error) || fallback;
}

async function request(method, ctx, url, data, options = {}) {
  const normalized = normalizeOptions(options, url);
  const { retry, admin, ...axiosOptions } = normalized;
  try {
    if (method === 'get') {
      return await withRetry(() => axios.get(url, axiosOptions), retry);
    }
    if (method === 'delete') {
      return await withRetry(() => axios.delete(url, axiosOptions), retry);
    }
    if (method === 'put') {
      return await withRetry(() => axios.put(url, data, axiosOptions), retry);
    }
    return await withRetry(() => axios.post(url, data, axiosOptions), retry);
  } catch (error) {
    logError(error, { method: method.toUpperCase(), url });
    const enriched = enrichError(error, { method: method.toUpperCase(), url });
    if (enriched?.userMessage && typeof enriched.message === 'string' && /request failed/i.test(enriched.message)) {
      enriched.message = enriched.userMessage;
    }
    throw enriched;
  }
}

async function get(ctx, url, options = {}) {
  return request('get', ctx, url, null, options);
}

async function post(ctx, url, data, options = {}) {
  return request('post', ctx, url, data, options);
}

async function put(ctx, url, data, options = {}) {
  return request('put', ctx, url, data, options);
}

async function del(ctx, url, options = {}) {
  return request('delete', ctx, url, null, options);
}

module.exports = {
  withRetry,
  getUserMessage,
  mapErrorToUserMessage,
  sanitizeUrl,
  get,
  post,
  put,
  del
};
