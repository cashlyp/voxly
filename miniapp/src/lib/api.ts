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

type AuthTokenProvider = () => string | null;

let authTokenProvider: AuthTokenProvider = () => null;

export function setAuthTokenProvider(provider: AuthTokenProvider) {
  authTokenProvider = provider;
}

const API_BASE = import.meta.env.VITE_API_BASE || '';
const DEFAULT_TIMEOUT_MS = 8000;
const DEFAULT_RETRIES = 2;

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
  } = {},
): Promise<T> {
  const {
    method = 'GET',
    body,
    headers = {},
    timeoutMs = DEFAULT_TIMEOUT_MS,
    retries = DEFAULT_RETRIES,
    auth = true,
  } = options;

  const token = auth ? authTokenProvider() : null;
  const requestHeaders: Record<string, string> = {
    Accept: 'application/json',
    ...headers,
  };
  if (body !== undefined) {
    requestHeaders['Content-Type'] = 'application/json';
  }
  if (token) {
    requestHeaders.Authorization = `Bearer ${token}`;
  }

  let attempt = 0;
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
        const payload = await readJsonSafely(response);
        const error = new ApiError({
          status: response.status,
          code: payload?.error || payload?.code || 'request_failed',
          message: payload?.message || payload?.error || 'Request failed',
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
      throw new ApiError({
        status: 0,
        code: 'network_error',
        message: 'Network error',
        details: error,
        retryable: true,
      });
    }
  }
}
