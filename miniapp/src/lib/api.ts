export type ApiErrorPayload = {
  status: number;
  code: string;
  message: string;
  details?: unknown;
  retryable?: boolean;
};

export class ApiError extends Error {
  public status: number;
  public code: string;
  public details?: unknown;
  public retryable?: boolean;

  public constructor(payload: ApiErrorPayload) {
    super(payload.message);
    this.name = "ApiError";
    this.status = payload.status;
    this.code = payload.code;
    this.details = payload.details;
    this.retryable = payload.retryable;
  }
}

export function createIdempotencyKey(): string {
  if (typeof crypto !== "undefined" && "randomUUID" in crypto) {
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

const API_BASE = String(import.meta.env.VITE_API_BASE ?? "").trim();
const SOCKET_BASE = String(import.meta.env.VITE_SOCKET_URL ?? "").trim();
const DEFAULT_TIMEOUT_MS = 8000;
const DEFAULT_RETRIES = 2;

// Validate API base is configured in production
if (API_BASE === "" && typeof window !== "undefined" && import.meta.env.PROD) {
  console.error(
    "❌ CRITICAL: VITE_API_BASE environment variable is not set. API communication will fail.",
  );
}

if (API_BASE === "" && typeof window !== "undefined" && import.meta.env.DEV) {
  console.warn(
    "⚠️  VITE_API_BASE environment variable is not set. API communication may fail.",
  );
}

export function getApiBase() {
  return API_BASE;
}

export function getSocketBase() {
  return SOCKET_BASE;
}

export type ApiPingResponse = {
  ok: boolean;
  ts: string;
  version?: string | null;
};

export async function pingApi(options: { timeoutMs?: number } = {}) {
  const start = Date.now();
  const payload = await apiFetch<ApiPingResponse>("/webapp/ping", {
    auth: false,
    retries: 0,
    timeoutMs: options.timeoutMs ?? 5000,
  });
  return {
    payload,
    latencyMs: Date.now() - start,
  };
}

function buildUrl(path: string): string {
  if (path.startsWith("http://") || path.startsWith("https://")) {
    return path;
  }
  if (!API_BASE) {
    throw new ApiError({
      status: 0,
      code: "no_api_base",
      message:
        "API URL is not configured. Set VITE_API_BASE environment variable.",
      retryable: false,
    });
  }
  // Ensure no double slashes except in protocol
  const normalized = `${API_BASE}${path}`.replace(/([^:]\/)\/+/g, "$1");
  if (import.meta.env.DEV) {
    console.warn(`[API URL] ${normalized}`);
  }
  return normalized;
}

function shouldRetry(status: number): boolean {
  // Retry on specific server/network errors
  return [408, 429, 502, 503, 504].includes(status);
}

async function readJsonSafely(response: Response): Promise<unknown> {
  const contentType = response.headers.get("content-type") ?? "";
  if (!contentType.includes("application/json")) {
    return null;
  }
  try {
    return await response.json();
  } catch (error) {
    if (import.meta.env.DEV) {
      console.warn("Failed to parse response JSON:", error);
    }
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
    method = "GET",
    body,
    headers = {},
    timeoutMs = DEFAULT_TIMEOUT_MS,
    retries = DEFAULT_RETRIES,
    auth = true,
    idempotencyKey,
  } = options;

  const token = auth ? authTokenProvider() : null;
  const requestHeaders: Record<string, string> = {
    Accept: "application/json",
    ...headers,
  };
  if (idempotencyKey !== undefined && idempotencyKey !== "") {
    requestHeaders["Idempotency-Key"] = idempotencyKey;
  }
  if (body !== undefined) {
    requestHeaders["Content-Type"] = "application/json";
  }
  if (token !== null && token !== "") {
    requestHeaders.Authorization = `Bearer ${token}`;
  }

  const methodUpper = method.toUpperCase();
  const safeMethod = ["GET", "HEAD"].includes(methodUpper);
  const maxRetries = safeMethod ? retries : 0;

  let attempt = 0;
  let refreshed = false;
  const fullUrl = buildUrl(path);

  while (true) {
    attempt += 1;
    const controller = new AbortController();
    const timeout = window.setTimeout(() => controller.abort(), timeoutMs);
    try {
      const response = await fetch(fullUrl, {
        method,
        headers: requestHeaders,
        body: body === undefined ? undefined : JSON.stringify(body),
        signal: controller.signal,
        credentials: "include",
      });
      window.clearTimeout(timeout);

      if (!response.ok) {
        if (
          (response.status === 401 || response.status === 403) &&
          auth &&
          authRefreshProvider &&
          !refreshed
        ) {
          refreshed = true;
          try {
            await authRefreshProvider();
          } catch {
            // ignore refresh failures, fall through to error handling
          }
          continue;
        }
        const payload = (await readJsonSafely(response)) as Record<
          string,
          unknown
        > | null;
        const error = new ApiError({
          status: response.status,
          code:
            (payload?.error as string) ||
            (payload?.code as string) ||
            `http_${response.status}`,
          message:
            (payload?.message as string) ||
            (payload?.error as string) ||
            response.statusText ||
            `Request failed (${response.status})`,
          details: {
            ...payload,
            url: import.meta.env.DEV ? fullUrl : undefined,
          },
          retryable: shouldRetry(response.status),
        });

        // Log API errors for debugging
        if (import.meta.env.DEV) {
          console.error(`[API Error] ${method} ${fullUrl}:`, {
            status: response.status,
            statusText: response.statusText,
            code: error.code,
            message: error.message,
            payload,
          });
        }

        if (error.retryable === true && attempt <= maxRetries) {
          await new Promise((resolve) =>
            window.setTimeout(resolve, 300 * attempt),
          );
          continue;
        }
        throw error;
      }

      const data = await readJsonSafely(response);

      // Log successful API calls in development
      if (import.meta.env.DEV) {
        console.warn(`[API Success] ${method} ${fullUrl}`, data);
      }

      return data as T;
    } catch (error) {
      window.clearTimeout(timeout);
      const isAbort =
        error instanceof DOMException && error.name === "AbortError";
      if ((isAbort || error instanceof TypeError) && attempt <= maxRetries) {
        await new Promise((resolve) =>
          window.setTimeout(resolve, 300 * attempt),
        );
        continue;
      }
      if (error instanceof ApiError) {
        throw error;
      }
      if (isAbort) {
        throw new ApiError({
          status: 0,
          code: "timeout",
          message: "Request timed out",
          details: { url: import.meta.env.DEV ? fullUrl : undefined, error },
          retryable: true,
        });
      }
      if (error instanceof TypeError) {
        throw new ApiError({
          status: 0,
          code: "network_error",
          message: "Cannot reach API (network/CORS/DNS)",
          details: { url: import.meta.env.DEV ? fullUrl : undefined, error },
          retryable: true,
        });
      }
      throw new ApiError({
        status: 0,
        code: "unknown_error",
        message: "Unexpected network failure",
        details: { url: import.meta.env.DEV ? fullUrl : undefined, error },
        retryable: true,
      });
    }
  }
}

/**
 * Validation helpers for common field types
 */
export const validate = {
  /** Validate phone number (basic E.164 check) */
  phoneNumber: (phone: string): boolean => {
    return /^\+?[1-9]\d{1,14}$/.test(phone.replace(/\D/g, ""));
  },

  /** Validate email address */
  email: (email: string): boolean => {
    return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
  },

  /** Validate URL */
  url: (url: string): boolean => {
    try {
      new URL(url);
      return true;
    } catch {
      return false;
    }
  },

  /** Validate string length */
  stringLength: (str: string, min: number, max: number): boolean => {
    const len = str.trim().length;
    return len >= min && len <= max;
  },

  /** Validate not empty string */
  required: (str: string): boolean => {
    return str.trim().length > 0;
  },
};
