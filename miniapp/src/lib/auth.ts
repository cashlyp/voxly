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
  role?: string | null;
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
const SESSION_STORAGE_PREFIX = "voicednut.webapp.session";
const SESSION_TOKEN_KEY = `${SESSION_STORAGE_PREFIX}.token`;
const SESSION_EXP_KEY = `${SESSION_STORAGE_PREFIX}.exp`;
const SESSION_USER_KEY = `${SESSION_STORAGE_PREFIX}.user`;
const SESSION_ROLES_KEY = `${SESSION_STORAGE_PREFIX}.roles`;

let cachedToken: string | null = null;
let cachedExpiry: string | null = null;
let cachedUser: WebappUser | null = null;
let cachedRoles: string[] = [];

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
  public kind: AuthErrorKind;
  public status?: number;
  public code?: string;

  public constructor(
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

type JwtPayload = {
  exp?: number;
  [key: string]: unknown;
};

function parseJwtPayload(token: string): JwtPayload | null {
  try {
    const payload = token.split(".")[1];
    const normalized = payload.replace(/-/g, "+").replace(/_/g, "/");
    const padded = normalized.padEnd(
      normalized.length + (4 - (normalized.length % 4 || 4)),
      "=",
    );
    const json = atob(padded);
    return JSON.parse(json) as JwtPayload;
  } catch {
    return null;
  }
}

function clearLegacyStorage() {
  if (typeof window === "undefined") return;
  try {
    window.localStorage.removeItem(STORAGE_KEY);
    window.localStorage.removeItem(STORAGE_EXP_KEY);
    window.localStorage.removeItem(STORAGE_USER_KEY);
    window.localStorage.removeItem(STORAGE_ROLES_KEY);
  } catch {
    // ignore storage errors
  }
}

clearLegacyStorage();

function loadSessionStorage() {
  if (typeof window === "undefined") return;
  try {
    const storedToken = window.sessionStorage.getItem(SESSION_TOKEN_KEY);
    const storedExp = window.sessionStorage.getItem(SESSION_EXP_KEY);
    const storedUser = window.sessionStorage.getItem(SESSION_USER_KEY);
    const storedRoles = window.sessionStorage.getItem(SESSION_ROLES_KEY);
    if (storedToken !== null && storedToken !== "") cachedToken = storedToken;
    if (storedExp !== null && storedExp !== "") cachedExpiry = storedExp;
    if (storedUser !== null && storedUser !== "") {
      const parsed = JSON.parse(storedUser) as unknown;
      cachedUser =
        parsed !== null && typeof parsed === "object"
          ? (parsed as WebappUser)
          : null;
    }
    if (storedRoles !== null && storedRoles !== "") {
      const parsed = JSON.parse(storedRoles) as unknown;
      if (
        Array.isArray(parsed) &&
        parsed.every((item) => typeof item === "string")
      ) {
        cachedRoles = parsed;
      }
    }
  } catch {
    // ignore storage errors
  }
}

function persistSessionStorage(
  token: string,
  expiresAt: string,
  user: WebappUser,
  roles: string[],
) {
  if (typeof window === "undefined") return;
  try {
    window.sessionStorage.setItem(SESSION_TOKEN_KEY, token);
    window.sessionStorage.setItem(SESSION_EXP_KEY, expiresAt);
    window.sessionStorage.setItem(SESSION_USER_KEY, JSON.stringify(user));
    window.sessionStorage.setItem(SESSION_ROLES_KEY, JSON.stringify(roles));
  } catch {
    // ignore storage errors
  }
}

function clearSessionStorage() {
  if (typeof window === "undefined") return;
  try {
    window.sessionStorage.removeItem(SESSION_TOKEN_KEY);
    window.sessionStorage.removeItem(SESSION_EXP_KEY);
    window.sessionStorage.removeItem(SESSION_USER_KEY);
    window.sessionStorage.removeItem(SESSION_ROLES_KEY);
  } catch {
    // ignore storage errors
  }
}

loadSessionStorage();

export function getStoredToken() {
  return cachedToken;
}

export function setStoredToken(
  token: string,
  expiresAt: string,
  user: WebappUser,
  roles: string[],
) {
  cachedToken = token;
  cachedExpiry = expiresAt;
  cachedUser = user;
  cachedRoles = roles;
  persistSessionStorage(token, expiresAt, user, roles);
}

export function clearStoredToken() {
  cachedToken = null;
  cachedExpiry = null;
  cachedUser = null;
  cachedRoles = [];
  clearLegacyStorage();
  clearSessionStorage();
}

export function getStoredUser(): WebappUser | null {
  return cachedUser;
}

export function getStoredRoles(): string[] {
  return cachedRoles;
}

export function getTokenExpiry() {
  if (cachedExpiry !== null) return cachedExpiry;
  const token = getStoredToken();
  if (token === null || token === "") return null;
  const payload = parseJwtPayload(token);
  if (payload === null || typeof payload.exp !== "number") return null;
  const expIso = new Date(payload.exp * 1000).toISOString();
  cachedExpiry = expIso;
  return expIso;
}

export function isTokenValid(bufferSeconds = 30): boolean {
  const token = getStoredToken();
  if (token === null || token === "") {
    return false;
  }
  const expIso = getTokenExpiry();
  if (expIso === null || expIso === "") {
    return false;
  }
  const exp = Date.parse(expIso);
  if (!Number.isFinite(exp)) {
    return false;
  }
  return Date.now() + bufferSeconds * 1000 < exp;
}

export function getInitData() {
  const fromEnv =
    typeof import.meta.env.VITE_TELEGRAM_INITDATA === "string"
      ? import.meta.env.VITE_TELEGRAM_INITDATA
      : "";
  if (typeof fromEnv === "string" && fromEnv !== "") return fromEnv;
  try {
    const raw = retrieveRawInitData();
    if (typeof raw === "string" && raw !== "") return raw;
  } catch {
    // fall through to legacy WebApp initData lookup
  }
  const webapp = (
    window as Window & { Telegram?: { WebApp?: { initData?: string } } }
  ).Telegram?.WebApp;
  return webapp?.initData ?? "";
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
    user: webapp?.initDataUnsafe?.user ?? null,
  };
}

function getErrorDetailUrl(details: unknown): string | null {
  if (details === null || typeof details !== "object") return null;
  if (!("url" in details)) return null;
  const url = (details as { url?: unknown }).url;
  return typeof url === "string" && url !== "" ? url : null;
}

function describeAuthError(error: unknown) {
  if (error instanceof ApiError) {
    if (error.code === "no_api_base") {
      return new AuthError(
        "API URL not configured. Set VITE_API_BASE in the Mini App environment.",
        "offline",
        0,
        error.code,
      );
    }
    const apiBase = getApiBase();
    const resolvedBase =
      apiBase !== "" ? apiBase : window.location.origin;
    if (error.status === 0) {
      if (error.code === "timeout") {
        return new AuthError(
          `Cannot reach API (timeout). Check ${resolvedBase}.`,
          "offline",
          0,
          error.code,
        );
      }
      // Add URL details in dev mode
      const detailUrl =
        import.meta.env.DEV ? getErrorDetailUrl(error.details) : null;
      const urlInfo =
        detailUrl !== null && detailUrl !== "" ? ` (${detailUrl})` : "";
      return new AuthError(
        `Cannot reach API (network/CORS/DNS). Check ${resolvedBase}${urlInfo}.`,
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
    const reason = mapped ?? error.message ?? error.code ?? "Auth failed";
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
    roles: response.roles,
    environment: response.environment ?? null,
    tenant_id: response.tenant_id ?? null,
  };
}

export async function authenticate(initData?: string): Promise<AuthSession> {
  const context = getInitDataContext();
  const rawInitData = initData ?? context.initData;
  if (rawInitData === "") {
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
      token?: string;
      accessToken?: string;
      expires_at?: string;
      expiresIn?: number;
      user: WebappUser;
      roles: string[];
      environment?: string | null;
      tenant_id?: string | null;
    }>("/webapp/auth", {
      method: "POST",
      headers: { Authorization: `tma ${rawInitData}` },
      auth: false,
    });
    const resolvedToken = response.accessToken ?? response.token ?? "";
    if (!resolvedToken) {
      throw new AuthError(
        "Auth token missing from response.",
        "server",
        500,
        "missing_token",
      );
    }
    const fallbackExpirySeconds =
      typeof response.expiresIn === "number" ? response.expiresIn : 900;
    const resolvedExpiry =
      response.expires_at ??
      new Date(Date.now() + fallbackExpirySeconds * 1000).toISOString();
    const session: AuthSession = {
      token: resolvedToken,
      expiresAt: resolvedExpiry,
      user: response.user,
      roles: response.roles,
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
      user: details.user ?? session.user ?? context.user ?? { id: "" },
      roles: details.roles ?? [],
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
    throw new AuthError(String(described ?? "Auth failed"), "unknown");
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
  const forceRefresh = options.forceRefresh === true;
  if (!forceRefresh && isTokenValid(minValiditySeconds)) {
    if (verify) {
      try {
        const details = await fetchSessionDetails();
        const session = {
          token: getStoredToken() ?? "",
          expiresAt: getTokenExpiry() ?? "",
          user: details.user ?? getStoredUser() ?? { id: "" },
          roles: details.roles ?? [],
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
      token: getStoredToken() ?? "",
      expiresAt: getTokenExpiry() ?? "",
      user: getStoredUser() ?? { id: "" },
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
