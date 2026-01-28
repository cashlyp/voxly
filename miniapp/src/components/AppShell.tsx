import { useEffect, useMemo, useState } from 'react';
import { backButton } from '@tma.js/sdk-react';
import { matchRoute, navigate, getHashPath } from '../lib/router';
import { useUser } from '../state/user';
import { CallsProvider } from '../state/calls';
import { Dashboard } from '../routes/Dashboard';
import { Inbox } from '../routes/Inbox';
import { Calls } from '../routes/Calls';
import { CallConsole } from '../routes/CallConsole';
import { Scripts } from '../routes/Scripts';
import { Users } from '../routes/Users';
import { Settings } from '../routes/Settings';

type NavItem = {
  label: string;
  path: string;
  adminOnly?: boolean;
};

const navItems: NavItem[] = [
  { label: 'Dashboard', path: '/' },
  { label: 'Inbox', path: '/inbox' },
  { label: 'Calls', path: '/calls' },
  { label: 'Scripts', path: '/scripts' },
  { label: 'Users', path: '/users', adminOnly: true },
  { label: 'Settings', path: '/settings', adminOnly: true },
];

function Nav() {
  const { roles } = useUser();
  const isAdmin = roles.includes('admin');
  const [path, setPath] = useState(getHashPath());

  useEffect(() => {
    const handler = () => setPath(getHashPath());
    window.addEventListener('hashchange', handler);
    return () => window.removeEventListener('hashchange', handler);
  }, []);

  return (
    <nav className="nav">
      {navItems.filter((item) => (item.adminOnly ? isAdmin : true)).map((item) => (
        <button
          type="button"
          key={item.path}
          className={`nav-link${path === item.path ? ' active' : ''}`}
          onClick={() => navigate(item.path)}
        >
          {item.label}
        </button>
      ))}
    </nav>
  );
}

function RouteRenderer() {
  const route = matchRoute(getHashPath());
  const { roles } = useUser();
  const isAdmin = roles.includes('admin');

  switch (route.name) {
    case 'dashboard':
      return <Dashboard />;
    case 'inbox':
      return <Inbox />;
    case 'calls':
      return <Calls />;
    case 'callConsole':
      return <CallConsole callSid={route.params.callSid} />;
    case 'scripts':
      return <Scripts />;
    case 'users':
      return isAdmin ? <Users /> : <div className="panel">Admin access required.</div>;
    case 'settings':
      return isAdmin ? <Settings /> : <div className="panel">Admin access required.</div>;
    default:
      return <div className="panel">Route not found.</div>;
  }
}

export function AppShell() {
  const { status, user, roles, error, refresh } = useUser();
  const isAdmin = roles.includes('admin');

  useEffect(() => {
    const handleBack = () => {
      if (window.history.length > 1) {
        window.history.back();
      } else {
        navigate('/');
      }
    };
    backButton.show();
    const off = backButton.onClick(handleBack);
    return () => {
      off();
      backButton.hide();
    };
  }, []);

  const headerSubtitle = useMemo(() => {
    if (status === 'loading') return 'Authorizing...';
    if (status === 'error') return error || 'Auth failed';
    if (!user) return 'Not connected';
    const roleLabel = isAdmin ? 'Admin' : 'Viewer';
    return `${roleLabel} - ${user.username || user.first_name || user.id}`;
  }, [status, error, user, isAdmin]);

  return (
    <CallsProvider>
      <div className="app">
        <header className="app-header">
          <div>
            <h1>VOICEDNUT</h1>
            <p>{headerSubtitle}</p>
          </div>
          <div className="header-actions">
            <button type="button" className="btn ghost" onClick={refresh}>
              Refresh
            </button>
          </div>
        </header>
        <Nav />
        <main className="content">
          <RouteRenderer />
        </main>
      </div>
    </CallsProvider>
  );
}
