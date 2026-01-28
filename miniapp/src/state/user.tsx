import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type PropsWithChildren,
} from 'react';
import {
  authenticate,
  clearStoredToken,
  ensureAuth,
  getStoredRoles,
  getStoredUser,
  getTokenExpiry,
  AuthError,
  type AuthErrorKind,
  type WebappUser,
} from '../lib/auth';
import { trackEvent } from '../lib/telemetry';

type UserState = {
  status: 'idle' | 'loading' | 'ready' | 'error';
  user: WebappUser | null;
  roles: string[];
  error?: string | null;
  errorKind?: AuthErrorKind | null;
  environment?: string | null;
  tenantId?: string | null;
  refresh: () => Promise<void>;
  login: () => Promise<void>;
  logout: () => void;
};

const UserContext = createContext<UserState | null>(null);

export function UserProvider({ children }: PropsWithChildren) {
  const [status, setStatus] = useState<UserState['status']>('idle');
  const [user, setUser] = useState<WebappUser | null>(getStoredUser());
  const [roles, setRoles] = useState<string[]>(getStoredRoles());
  const [error, setError] = useState<string | null>(null);
  const [errorKind, setErrorKind] = useState<AuthErrorKind | null>(null);
  const [environment, setEnvironment] = useState<string | null>(null);
  const [tenantId, setTenantId] = useState<string | null>(null);
  const authStartRef = useRef<number | null>(Date.now());

  const refresh = useCallback(async () => {
    setStatus('loading');
    setError(null);
    setErrorKind(null);
    authStartRef.current = Date.now();
    try {
      const session = await ensureAuth();
      setUser(session.user);
      setRoles(session.roles || []);
      setEnvironment(session.environment ?? null);
      setTenantId(session.tenant_id ?? null);
      setStatus('ready');
      if (authStartRef.current !== null) {
        trackEvent('slo_auth_ready', { duration_ms: Math.round(Date.now() - authStartRef.current) });
      }
    } catch (err) {
      setStatus('error');
      if (err instanceof AuthError) {
        setError(err.message);
        setErrorKind(err.kind);
      } else {
        setError(err instanceof Error ? err.message : 'Auth failed');
        setErrorKind('unknown');
      }
    }
  }, []);

  const login = useCallback(async () => {
    setStatus('loading');
    setError(null);
    setErrorKind(null);
    authStartRef.current = Date.now();
    try {
      const session = await authenticate();
      setUser(session.user);
      setRoles(session.roles || []);
      setEnvironment(session.environment ?? null);
      setTenantId(session.tenant_id ?? null);
      setStatus('ready');
      if (authStartRef.current !== null) {
        trackEvent('slo_auth_ready', { duration_ms: Math.round(Date.now() - authStartRef.current) });
      }
    } catch (err) {
      setStatus('error');
      if (err instanceof AuthError) {
        setError(err.message);
        setErrorKind(err.kind);
      } else {
        setError(err instanceof Error ? err.message : 'Login failed');
        setErrorKind('unknown');
      }
    }
  }, []);

  const logout = useCallback(() => {
    clearStoredToken();
    setUser(null);
    setRoles([]);
    setStatus('idle');
    setError(null);
    setErrorKind(null);
    setEnvironment(null);
    setTenantId(null);
  }, []);

  useEffect(() => {
    refresh();
  }, [refresh]);

  useEffect(() => {
    if (status !== 'ready') return;
    const expIso = getTokenExpiry();
    if (!expIso) return;
    const exp = Date.parse(expIso);
    if (!Number.isFinite(exp)) return;
    const bufferMs = 60 * 1000;
    const delay = Math.max(exp - Date.now() - bufferMs, 5000);
    const timer = window.setTimeout(() => {
      refresh().catch(() => {});
    }, delay);
    return () => window.clearTimeout(timer);
  }, [status, refresh]);

  const value = useMemo<UserState>(() => ({
    status,
    user,
    roles,
    error,
    errorKind,
    environment,
    tenantId,
    refresh,
    login,
    logout,
  }), [status, user, roles, error, errorKind, environment, tenantId, refresh, login, logout]);

  return (
    <UserContext.Provider value={value}>
      {children}
    </UserContext.Provider>
  );
}

export function useUser() {
  const context = useContext(UserContext);
  if (!context) {
    throw new Error('useUser must be used within UserProvider');
  }
  return context;
}
