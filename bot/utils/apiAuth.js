'use strict';

const crypto = require('crypto');

const SIGNATURE_HEADER = 'x-api-signature';
const TIMESTAMP_HEADER = 'x-api-timestamp';

function stableStringify(value) {
  if (value === null || typeof value !== 'object') {
    return JSON.stringify(value);
  }

  if (Array.isArray(value)) {
    const items = value.map((item) => (item === undefined ? 'null' : stableStringify(item)));
    return `[${items.join(',')}]`;
  }

  const keys = Object.keys(value).filter((key) => value[key] !== undefined).sort();
  const entries = keys.map((key) => `${JSON.stringify(key)}:${stableStringify(value[key])}`);
  return `{${entries.join(',')}}`;
}

function normalizeBody(data, method = 'GET') {
  const normalizedMethod = String(method || 'GET').toUpperCase();
  if (['GET', 'HEAD'].includes(normalizedMethod)) {
    return '';
  }
  if (data === undefined || data === null) {
    return '';
  }
  if (typeof data === 'string') {
    const trimmed = data.trim();
    if ((trimmed.startsWith('{') && trimmed.endsWith('}')) || (trimmed.startsWith('[') && trimmed.endsWith(']'))) {
      try {
        const parsed = JSON.parse(trimmed);
        return stableStringify(parsed);
      } catch (_) {
        return data;
      }
    }
    return data;
  }
  if (typeof data === 'object') {
    return stableStringify(data);
  }
  return JSON.stringify(data);
}

function buildRequestUrl(request, fallbackBaseUrl) {
  const baseUrl = request.baseURL || fallbackBaseUrl;
  if (!request.url) {
    return null;
  }

  let resolved;
  try {
    resolved = new URL(request.url, baseUrl);
  } catch (_) {
    return null;
  }

  if (request.params) {
    const searchParams = new URLSearchParams(resolved.search);
    Object.entries(request.params).forEach(([key, value]) => {
      if (value === undefined || value === null) return;
      if (Array.isArray(value)) {
        value.forEach((entry) => {
          if (entry === undefined || entry === null) return;
          searchParams.append(key, String(entry));
        });
      } else {
        searchParams.append(key, String(value));
      }
    });
    const query = searchParams.toString();
    resolved.search = query ? `?${query}` : '';
  }

  return resolved;
}

function buildSignature(secret, timestamp, method, path, body) {
  const payload = `${timestamp}.${method}.${path}.${body}`;
  return crypto.createHmac('sha256', secret).update(payload).digest('hex');
}

function attachHmacAuth(axiosInstance, options = {}) {
  const secret = options.secret;
  if (!secret) {
    return;
  }

  const allowedOrigins = options.allowedOrigins instanceof Set
    ? options.allowedOrigins
    : new Set(options.allowedOrigins || []);
  const fallbackBaseUrl = options.defaultBaseUrl;

  axiosInstance.interceptors.request.use((request) => {
    const resolved = buildRequestUrl(request, fallbackBaseUrl);
    if (!resolved || (allowedOrigins.size > 0 && !allowedOrigins.has(resolved.origin))) {
      return request;
    }

    const timestamp = Date.now().toString();
    const method = String(request.method || 'GET').toUpperCase();
    const body = normalizeBody(request.data, method);
    const signature = buildSignature(secret, timestamp, method, `${resolved.pathname}${resolved.search}`, body);

    request.headers = request.headers || {};
    request.headers[TIMESTAMP_HEADER] = timestamp;
    request.headers[SIGNATURE_HEADER] = signature;

    return request;
  });
}

module.exports = {
  attachHmacAuth,
};
