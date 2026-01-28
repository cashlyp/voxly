import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type PropsWithChildren,
} from 'react';
import { authenticate, clearStoredToken, ensureAuth, getStoredRoles, getStoredUser, type WebappUser } from '../lib/auth';

type UserState = {
  status: 'idle' | 'loading' | 'ready' | 'error';
  user: WebappUser | null;
  roles: string[];
  error?: string | null;
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

  const refresh = useCallback(async () => {
    setStatus('loading');
    setError(null);
    try {
      const session = await ensureAuth();
      setUser(session.user);
      setRoles(session.roles || []);
      setStatus('ready');
    } catch (err) {
      setStatus('error');
      setError(err instanceof Error ? err.message : 'Auth failed');
    }
  }, []);

  const login = useCallback(async () => {
    setStatus('loading');
    setError(null);
    try {
      const session = await authenticate();
      setUser(session.user);
      setRoles(session.roles || []);
      setStatus('ready');
    } catch (err) {
      setStatus('error');
      setError(err instanceof Error ? err.message : 'Login failed');
    }
  }, []);

  const logout = useCallback(() => {
    clearStoredToken();
    setUser(null);
    setRoles([]);
    setStatus('idle');
  }, []);

  useEffect(() => {
    refresh();
  }, [refresh]);

  const value = useMemo<UserState>(() => ({
    status,
    user,
    roles,
    error,
    refresh,
    login,
    logout,
  }), [status, user, roles, error, refresh, login, logout]);

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
