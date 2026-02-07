import { retrieveRawInitData } from "@tma.js/sdk-react";

import {
  ApiError,
  apiFetch,
  getApiBase,
  setAuthRefreshProvider,
  setAuthTokenProvider,
} from "./api";

export type WebappUser = {
  id: string | number;
  username?: string | null;
  first_name?: string | null;
  last_name?: string | null;
};

export type AuthSession = {
  token: string;
  expiresAt: string;
  user: WebappUser;
  roles: string[];
  environment?: string | null;
  tenant_id?: string | null;
};

const STORAGE_KEY = "voicednut.webapp.jwt";
const STORAGE_EXP_KEY = "voicednut.webapp.jwt.exp";
const STORAGE_USER_KEY = "voicednut.webapp.user";
const STORAGE_ROLES_KEY = "voicednut.webapp.roles";

let cachedToken: string | null = null;

type InitDataContext = {
  initData: string;
  user: WebappUser | null;
};

export type AuthErrorKind =
  | "offline"
  | "unauthorized"
  | "initdata"
  | "server"
  | "unknown";

export class AuthError extends Error {
  kind: AuthErrorKind;
  status?: number;
  code?: string;

  constructor(
    message: string,
    kind: AuthErrorKind,
    status?: number,
    code?: string,
  ) {
    super(message);
    this.name = "AuthError";
    this.kind = kind;
    this.status = status;
    this.code = code;
  }
}

function parseJwtPayload(token: string) {
  try {
    const payload = token.split(".")[1];
    const normalized = payload.replace(/-/g, "+").replace(/_/g, "/");
    const padded = normalized.padEnd(
      normalized.length + (4 - (normalized.length % 4 || 4)),
      "=",
    );
    const json = atob(padded);
    return JSON.parse(json);
  } catch {
    return null;
  }
}

export function getStoredToken() {
  if (cachedToken) return cachedToken;
  const token = window.localStorage.getItem(STORAGE_KEY);
  cachedToken = token;
  return token;
}

export function setStoredToken(
  token: string,
  expiresAt: string,
  user: WebappUser,
  roles: string[],
) {
  cachedToken = token;
  window.localStorage.setItem(STORAGE_KEY, token);
  window.localStorage.setItem(STORAGE_EXP_KEY, expiresAt);
  window.localStorage.setItem(STORAGE_USER_KEY, JSON.stringify(user));
  window.localStorage.setItem(STORAGE_ROLES_KEY, JSON.stringify(roles || []));
}

export function clearStoredToken() {
  cachedToken = null;
  window.localStorage.removeItem(STORAGE_KEY);
  window.localStorage.removeItem(STORAGE_EXP_KEY);
  window.localStorage.removeItem(STORAGE_USER_KEY);
  window.localStorage.removeItem(STORAGE_ROLES_KEY);
}

export function getStoredUser(): WebappUser | null {
  const raw = window.localStorage.getItem(STORAGE_USER_KEY);
  if (!raw) return null;
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

export function getStoredRoles(): string[] {
  const raw = window.localStorage.getItem(STORAGE_ROLES_KEY);
  if (!raw) return [];
  try {
    const roles = JSON.parse(raw);
    return Array.isArray(roles) ? roles : [];
  } catch {
    return [];
  }
}

export function getTokenExpiry() {
  const expIso = window.localStorage.getItem(STORAGE_EXP_KEY);
  if (expIso) return expIso;
  const token = getStoredToken();
  if (!token) return null;
  const payload = parseJwtPayload(token);
  if (!payload?.exp) return null;
  return new Date(payload.exp * 1000).toISOString();
}

export function isTokenValid(bufferSeconds = 30): boolean {
  const token = getStoredToken();
  if (!token) {
    return false;
  }
  const expIso = getTokenExpiry();
  if (!expIso) {
    return false;
  }
  const exp = Date.parse(expIso);
  if (!Number.isFinite(exp)) {
    return false;
  }
  return Date.now() + bufferSeconds * 1000 < exp;
}

export function getInitData() {
  const fromEnv = import.meta.env.VITE_TELEGRAM_INITDATA;
  if (fromEnv) return String(fromEnv);
  try {
    const raw = retrieveRawInitData();
    if (raw) return raw;
  } catch {
    // fall through to legacy WebApp initData lookup
  }
  const webapp = (
    window as Window & { Telegram?: { WebApp?: { initData?: string } } }
  ).Telegram?.WebApp;
  return webapp?.initData || "";
}

function getInitDataContext(): InitDataContext {
  const webapp = (
    window as Window & {
      Telegram?: {
        WebApp?: { initData?: string; initDataUnsafe?: { user?: WebappUser } };
      };
    }
  ).Telegram?.WebApp;
  return {
    initData: getInitData(),
    user: webapp?.initDataUnsafe?.user || null,
  };
}

function describeAuthError(error: unknown) {
  if (error instanceof ApiError) {
    const apiBase = getApiBase() || window.location.origin;
    if (error.status === 0) {
      if (error.code === "timeout") {
        return new AuthError(
          `Cannot reach API (timeout). Check ${apiBase}.`,
          "offline",
          0,
          error.code,
        );
      }
      // Add URL details in dev mode
      const urlInfo =
        import.meta.env.DEV && (error.details as any)?.url
          ? ` (${(error.details as any).url})`
          : "";
      return new AuthError(
        `Cannot reach API (network/CORS/DNS). Check ${apiBase}${urlInfo}.`,
        "offline",
        0,
        error.code,
      );
    }
    const initDataErrors: Record<string, string> = {
      missing_initdata:
        "Telegram init data is missing. Open the Mini App from Telegram.",
      missing_hash: "Telegram init data hash is missing.",
      invalid_hash: "Telegram init data signature is invalid.",
      expired_init_data:
        "Telegram init data expired. Close and reopen the Mini App.",
      invalid_auth_date: "Telegram init data has an invalid timestamp.",
      missing_bot_token:
        "Server is missing the bot token for initData verification.",
      invalid_initdata: "Telegram init data is invalid.",
      not_authorized: "You are not authorized for this Mini App.",
      origin_not_allowed:
        "Origin not allowed. Add MINI_APP_URL to the API allowlist.",
    };
    const mapped = initDataErrors[error.code];
    const reason = mapped || error.message || error.code || "Auth failed";
    const kind = [
      "missing_initdata",
      "missing_hash",
      "invalid_hash",
      "expired_init_data",
      "invalid_auth_date",
      "invalid_initdata",
    ].includes(error.code)
      ? "initdata"
      : error.status === 401 || error.status === 403
        ? "unauthorized"
        : error.status >= 500
          ? "server"
          : "unknown";
    return new AuthError(
      `Auth failed (${error.status}): ${reason}`,
      kind,
      error.status,
      error.code,
    );
  }
  if (error instanceof AuthError) {
    return error;
  }
  return new AuthError(
    error instanceof Error ? error.message : "Auth failed",
    "unknown",
  );
}

async function fetchSessionDetails(): Promise<
  Pick<AuthSession, "user" | "roles" | "environment" | "tenant_id">
> {
  const response = await apiFetch<{
    ok: boolean;
    user: WebappUser;
    roles: string[];
    environment?: string | null;
    tenant_id?: string | null;
  }>("/webapp/me");
  return {
    user: response.user,
    roles: response.roles || [],
    environment: response.environment ?? null,
    tenant_id: response.tenant_id ?? null,
  };
}

export async function authenticate(initData?: string): Promise<AuthSession> {
  const context = getInitDataContext();
  const rawInitData = initData || context.initData;
  if (!rawInitData) {
    throw new AuthError(
      "Telegram init data is missing. Open the Mini App from Telegram.",
      "initdata",
      401,
      "missing_initdata",
    );
  }
  try {
    const response = await apiFetch<{
      ok: boolean;
      token: string;
      expires_at: string;
      user: WebappUser;
      roles: string[];
      environment?: string | null;
      tenant_id?: string | null;
    }>("/webapp/auth", {
      method: "POST",
      headers: {
        Authorization: `tma ${rawInitData}`,
        "X-Telegram-Init-Data": rawInitData,
      },
      body: { initData: rawInitData },
      auth: false,
    });
    const session: AuthSession = {
      token: response.token,
      expiresAt: response.expires_at,
      user: response.user,
      roles: response.roles || [],
      environment: response.environment ?? null,
      tenant_id: response.tenant_id ?? null,
    };
    setStoredToken(
      session.token,
      session.expiresAt,
      session.user,
      session.roles,
    );

    const details = await fetchSessionDetails();
    const merged = {
      ...session,
      user: details.user || session.user || context.user || { id: "" },
      roles: details.roles || [],
      environment: details.environment ?? session.environment ?? null,
      tenant_id: details.tenant_id ?? session.tenant_id ?? null,
    };
    setStoredToken(merged.token, merged.expiresAt, merged.user, merged.roles);
    return merged;
  } catch (error) {
    const described = describeAuthError(error);
    if (described instanceof AuthError) {
      throw described;
    }
    throw new AuthError(String(described || "Auth failed"), "unknown");
  }
}

export async function ensureAuth(
  initData?: string,
  options: {
    minValiditySeconds?: number;
    forceRefresh?: boolean;
    verify?: boolean;
  } = {},
): Promise<AuthSession> {
  const minValiditySeconds = options.minValiditySeconds ?? 60;
  const verify = options.verify ?? true;
  if (!options.forceRefresh && isTokenValid(minValiditySeconds)) {
    if (verify) {
      try {
        const details = await fetchSessionDetails();
        const session = {
          token: getStoredToken() || "",
          expiresAt: getTokenExpiry() || "",
          user: details.user || getStoredUser() || { id: "" },
          roles: details.roles || [],
          environment: details.environment ?? null,
          tenant_id: details.tenant_id ?? null,
        };
        setStoredToken(
          session.token,
          session.expiresAt,
          session.user,
          session.roles,
        );
        return session;
      } catch (error) {
        const described = describeAuthError(error);
        if (described instanceof AuthError) throw described;
        throw new AuthError("Auth failed", "unknown");
      }
    }
    return {
      token: getStoredToken() || "",
      expiresAt: getTokenExpiry() || "",
      user: getStoredUser() || { id: "" },
      roles: getStoredRoles(),
      environment: null,
      tenant_id: null,
    };
  }
  return authenticate(initData);
}

setAuthTokenProvider(() => getStoredToken());
setAuthRefreshProvider(async () => {
  await ensureAuth(undefined, { forceRefresh: true, verify: false });
});
