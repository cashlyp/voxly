import { apiFetch, setAuthTokenProvider } from './api';

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
};

const STORAGE_KEY = 'voicednut.webapp.jwt';
const STORAGE_EXP_KEY = 'voicednut.webapp.jwt.exp';
const STORAGE_USER_KEY = 'voicednut.webapp.user';
const STORAGE_ROLES_KEY = 'voicednut.webapp.roles';

let cachedToken: string | null = null;

function parseJwtPayload(token: string) {
  try {
    const payload = token.split('.')[1];
    const normalized = payload.replace(/-/g, '+').replace(/_/g, '/');
    const padded = normalized.padEnd(normalized.length + (4 - (normalized.length % 4 || 4)), '=');
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

export function setStoredToken(token: string, expiresAt: string, user: WebappUser, roles: string[]) {
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

export function isTokenValid(bufferSeconds = 30) {
  const token = getStoredToken();
  if (!token) return false;
  const expIso = getTokenExpiry();
  if (!expIso) return false;
  const exp = Date.parse(expIso);
  if (!Number.isFinite(exp)) return false;
  return Date.now() + bufferSeconds * 1000 < exp;
}

export function getInitData() {
  const fromEnv = import.meta.env.VITE_TELEGRAM_INITDATA;
  if (fromEnv) return String(fromEnv);
  const webapp = (window as Window & { Telegram?: { WebApp?: { initData?: string } } }).Telegram?.WebApp;
  return webapp?.initData || '';
}

export async function authenticate(initData?: string): Promise<AuthSession> {
  const payload = {
    initData: initData || getInitData(),
  };
  const response = await apiFetch<{
    ok: boolean;
    token: string;
    expires_at: string;
    user: WebappUser;
    roles: string[];
  }>('/webapp/auth', {
    method: 'POST',
    body: payload,
    auth: false,
  });
  const session: AuthSession = {
    token: response.token,
    expiresAt: response.expires_at,
    user: response.user,
    roles: response.roles || [],
  };
  setStoredToken(session.token, session.expiresAt, session.user, session.roles);
  return session;
}

export async function ensureAuth(initData?: string): Promise<AuthSession> {
  if (isTokenValid()) {
    return {
      token: getStoredToken() || '',
      expiresAt: getTokenExpiry() || '',
      user: getStoredUser() || { id: '' },
      roles: getStoredRoles(),
    };
  }
  return authenticate(initData);
}

setAuthTokenProvider(() => getStoredToken());
