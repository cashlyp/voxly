export interface SessionCacheEntry {
  token: string;
  exp: number | null;
}

export function isSessionBootstrapBlockingCode(code: string): boolean {
  return [
    'miniapp_invalid_signature',
    'miniapp_missing_init_data',
    'miniapp_init_data_expired',
    'miniapp_auth_date_future',
    'miniapp_admin_required',
  ].includes(String(code || '').trim());
}

export function readSessionCache(storageKey: string): SessionCacheEntry | null {
  try {
    const raw = sessionStorage.getItem(storageKey);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as Partial<SessionCacheEntry>;
    if (!parsed || !parsed.token) return null;
    const exp = Number(parsed.exp);
    if (Number.isFinite(exp) && exp <= Math.floor(Date.now() / 1000) + 15) {
      sessionStorage.removeItem(storageKey);
      return null;
    }
    return {
      token: String(parsed.token),
      exp: Number.isFinite(exp) ? exp : null,
    };
  } catch {
    return null;
  }
}

export function writeSessionCache(storageKey: string, entry: SessionCacheEntry | null): void {
  if (!entry) {
    sessionStorage.removeItem(storageKey);
    return;
  }
  sessionStorage.setItem(storageKey, JSON.stringify(entry));
}

export function buildApiUrl(path: string, apiBaseUrl: string): string {
  const normalizedPath = path.startsWith('/') ? path : `/${path}`;
  return apiBaseUrl ? `${apiBaseUrl}${normalizedPath}` : normalizedPath;
}

export function buildEventStreamUrl(path: string, token: string, apiBaseUrl: string): string {
  const base = buildApiUrl(path, apiBaseUrl);
  const separator = base.includes('?') ? '&' : '?';
  const encodedToken = encodeURIComponent(token);
  return `${base}${separator}token=${encodedToken}&session_token=${encodedToken}&transport=sse`;
}
