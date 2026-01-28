export type ApiErrorPayload = {
  status: number;
  code: string;
  message: string;
  details?: unknown;
  retryable?: boolean;
};

export class ApiError extends Error {
  status: number;
  code: string;
  details?: unknown;
  retryable?: boolean;

  constructor(payload: ApiErrorPayload) {
    super(payload.message);
    this.name = 'ApiError';
    this.status = payload.status;
    this.code = payload.code;
    this.details = payload.details;
    this.retryable = payload.retryable;
  }
}

export function createIdempotencyKey() {
  if (typeof crypto !== 'undefined' && 'randomUUID' in crypto) {
    return crypto.randomUUID();
  }
  return `idem_${Date.now()}_${Math.random().toString(36).slice(2)}`;
}

type AuthTokenProvider = () => string | null;

let authTokenProvider: AuthTokenProvider = () => null;
let authRefreshProvider: (() => Promise<void>) | null = null;

export function setAuthTokenProvider(provider: AuthTokenProvider) {
  authTokenProvider = provider;
}

export function setAuthRefreshProvider(provider: () => Promise<void>) {
  authRefreshProvider = provider;
}

const API_BASE = import.meta.env.VITE_API_BASE || '';
const DEFAULT_TIMEOUT_MS = 8000;
const DEFAULT_RETRIES = 2;

export function getApiBase() {
  return API_BASE;
}

function buildUrl(path: string) {
  if (path.startsWith('http://') || path.startsWith('https://')) return path;
  return `${API_BASE}${path}`;
}

function shouldRetry(status: number) {
  return [408, 429, 502, 503, 504].includes(status);
}

async function readJsonSafely(response: Response) {
  const contentType = response.headers.get('content-type') || '';
  if (!contentType.includes('application/json')) {
    return null;
  }
  try {
    return await response.json();
  } catch {
    return null;
  }
}

export async function apiFetch<T>(
  path: string,
  options: {
    method?: string;
    body?: unknown;
    headers?: Record<string, string>;
    timeoutMs?: number;
    retries?: number;
    auth?: boolean;
    idempotencyKey?: string;
  } = {},
): Promise<T> {
  const {
    method = 'GET',
    body,
    headers = {},
    timeoutMs = DEFAULT_TIMEOUT_MS,
    retries = DEFAULT_RETRIES,
    auth = true,
    idempotencyKey,
  } = options;

  const token = auth ? authTokenProvider() : null;
  const requestHeaders: Record<string, string> = {
    Accept: 'application/json',
    ...headers,
  };
  if (idempotencyKey) {
    requestHeaders['Idempotency-Key'] = idempotencyKey;
  }
  if (body !== undefined) {
    requestHeaders['Content-Type'] = 'application/json';
  }
  if (token) {
    requestHeaders.Authorization = `Bearer ${token}`;
  }

  let attempt = 0;
  let refreshed = false;
  while (true) {
    attempt += 1;
    const controller = new AbortController();
    const timeout = window.setTimeout(() => controller.abort(), timeoutMs);
    try {
      const response = await fetch(buildUrl(path), {
        method,
        headers: requestHeaders,
        body: body === undefined ? undefined : JSON.stringify(body),
        signal: controller.signal,
        credentials: 'include',
      });
      window.clearTimeout(timeout);

      if (!response.ok) {
        if (response.status === 401 && auth && authRefreshProvider && !refreshed) {
          refreshed = true;
          try {
            await authRefreshProvider();
          } catch {
            // ignore refresh failures, fall through to error handling
          }
          continue;
        }
        const payload = await readJsonSafely(response);
        const error = new ApiError({
          status: response.status,
          code: payload?.error || payload?.code || `http_${response.status}`,
          message: payload?.message || payload?.error || response.statusText || `Request failed (${response.status})`,
          details: payload,
          retryable: shouldRetry(response.status),
        });
        if (error.retryable && attempt <= retries) {
          await new Promise((resolve) => window.setTimeout(resolve, 300 * attempt));
          continue;
        }
        throw error;
      }

      const data = await readJsonSafely(response);
      return data as T;
    } catch (error) {
      window.clearTimeout(timeout);
      const isAbort = error instanceof DOMException && error.name === 'AbortError';
      if ((isAbort || error instanceof TypeError) && attempt <= retries) {
        await new Promise((resolve) => window.setTimeout(resolve, 300 * attempt));
        continue;
      }
      if (error instanceof ApiError) {
        throw error;
      }
      if (isAbort) {
        throw new ApiError({
          status: 0,
          code: 'timeout',
          message: 'Request timed out',
          details: error,
          retryable: true,
        });
      }
      if (error instanceof TypeError) {
        throw new ApiError({
          status: 0,
          code: 'network_error',
          message: 'Cannot reach API (network/CORS/DNS)',
          details: error,
          retryable: true,
        });
      }
      throw new ApiError({
        status: 0,
        code: 'unknown_error',
        message: 'Unexpected network failure',
        details: error,
        retryable: true,
      });
    }
  }
}
