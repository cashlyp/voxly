import { useEffect, useMemo, useState, type ReactNode } from 'react';
import { addToHomeScreen, backButton, miniApp, openLink, openTelegramLink } from '@tma.js/sdk-react';
import {
  Banner,
  Button,
  Cell,
  List,
  Modal,
  Section,
  Tabbar,
} from '@telegram-apps/telegram-ui';
import { matchRoute, navigate, getHashPath, type RouteMatch } from '../lib/router';
import { useUser } from '../state/user';
import { CallsProvider } from '../state/calls';
import { Dashboard } from '../routes/Dashboard';
import { Inbox } from '../routes/Inbox';
import { Calls } from '../routes/Calls';
import { CallConsole } from '../routes/CallConsole';
import { Scripts } from '../routes/Scripts';
import { Users } from '../routes/Users';
import { Settings } from '../routes/Settings';

type TabItem = {
  label: string;
  path: string;
  icon: JSX.Element;
};

const tabItems: TabItem[] = [
  {
    label: 'Dashboard',
    path: '/',
    icon: (
      <svg className="tab-icon" viewBox="0 0 24 24" aria-hidden="true">
        <path d="M4 4h7v7H4zM13 4h7v4h-7zM13 10h7v10h-7zM4 13h7v7H4z" fill="currentColor" />
      </svg>
    ),
  },
  {
    label: 'Inbox',
    path: '/inbox',
    icon: (
      <svg className="tab-icon" viewBox="0 0 24 24" aria-hidden="true">
        <path d="M4 6h16l-2.5 10a2 2 0 0 1-2 1.5H8.5a2 2 0 0 1-2-1.5L4 6z" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinejoin="round" />
        <path d="M8 12h8" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" />
      </svg>
    ),
  },
  {
    label: 'Calls',
    path: '/calls',
    icon: (
      <svg className="tab-icon" viewBox="0 0 24 24" aria-hidden="true">
        <path d="M6 4h12v16H6z" fill="none" stroke="currentColor" strokeWidth="1.6" />
        <path d="M9 8h6M9 12h6M9 16h4" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" />
      </svg>
    ),
  },
  {
    label: 'Settings',
    path: '/settings',
    icon: (
      <svg className="tab-icon" viewBox="0 0 24 24" aria-hidden="true">
        <path d="M12 8a4 4 0 1 1 0 8 4 4 0 0 1 0-8z" fill="none" stroke="currentColor" strokeWidth="1.6" />
        <path d="M4 12h2m12 0h2M6.5 6.5l1.4 1.4m8.2 8.2 1.4 1.4M17.5 6.5l-1.4 1.4M8.1 16.1l-1.6 1.6" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" />
      </svg>
    ),
  },
];

const MenuIcon = ({ children }: { children: ReactNode }) => (
  <span className="menu-icon" aria-hidden="true">
    <svg viewBox="0 0 24 24">{children}</svg>
  </span>
);

function RouteRenderer({ route }: { route: RouteMatch }) {
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
  const [isOnline, setIsOnline] = useState(() => (typeof navigator !== 'undefined' ? navigator.onLine : true));
  const [path, setPath] = useState(getHashPath());
  const [menuOpen, setMenuOpen] = useState(false);

  useEffect(() => {
    const handler = () => setPath(getHashPath());
    window.addEventListener('hashchange', handler);
    return () => window.removeEventListener('hashchange', handler);
  }, []);

  const route = matchRoute(path);
  const tabPaths = tabItems.map((item) => item.path);
  const showBack = !tabPaths.includes(route.path);

  useEffect(() => {
    const handleBack = () => {
      if (window.history.length > 1) {
        window.history.back();
      } else {
        navigate('/');
      }
    };

    if (showBack) {
      backButton.show();
      const off = backButton.onClick(handleBack);
      return () => {
        off();
        backButton.hide();
      };
    }

    backButton.hide();
    return undefined;
  }, [showBack]);

  useEffect(() => {
    const handleOnline = () => setIsOnline(true);
    const handleOffline = () => setIsOnline(false);
    window.addEventListener('online', handleOnline);
    window.addEventListener('offline', handleOffline);
    return () => {
      window.removeEventListener('online', handleOnline);
      window.removeEventListener('offline', handleOffline);
    };
  }, []);

  const headerSubtitle = useMemo(() => {
    if (status === 'loading') return 'Authorizing...';
    if (status === 'error') return error || 'Auth failed';
    if (!user) return 'Not connected';
    const roleLabel = isAdmin ? 'Admin' : 'Viewer';
    return `${roleLabel} • ${user.username || user.first_name || user.id}`;
  }, [status, error, user, isAdmin]);

  const botUsername = import.meta.env.VITE_BOT_USERNAME || '';
  const botUrl = botUsername ? `https://t.me/${botUsername}` : '';
  const termsUrl = import.meta.env.VITE_TERMS_URL || '';
  const privacyUrl = import.meta.env.VITE_PRIVACY_URL || '';
  const addToHomeSupported = addToHomeScreen?.isAvailable?.() ?? false;

  const handleClose = () => {
    if (miniApp.close?.ifAvailable) {
      miniApp.close.ifAvailable();
    } else {
      miniApp.close();
    }
  };

  const handleOpenBot = () => {
    if (!botUrl) return;
    if (openTelegramLink.ifAvailable) {
      openTelegramLink.ifAvailable(botUrl);
    } else {
      openLink(botUrl);
    }
    setMenuOpen(false);
  };

  const handleOpenSettings = () => {
    navigate('/settings');
    setMenuOpen(false);
  };

  const handleReload = () => {
    setMenuOpen(false);
    window.location.reload();
  };

  const handleOpenUrl = (url: string) => {
    if (!url) return;
    openLink(url);
    setMenuOpen(false);
  };

  const handleAddToHome = () => {
    if (addToHomeSupported) {
      addToHomeScreen();
      setMenuOpen(false);
    }
  };

  return (
    <CallsProvider>
      <div className="app-shell">
        <header className="wallet-topbar">
          <button type="button" className="close-button" onClick={handleClose} aria-label="Close mini app">
            Close
          </button>
          <div className="brand">
            <div className="brand-title">
              VOICEDNUT
              <span className="brand-badge" aria-hidden="true">
                <span className="brand-check" />
              </span>
            </div>
            <div className="brand-sub">mini app</div>
            <div className="brand-meta">{headerSubtitle}</div>
          </div>
          <button type="button" className="menu-button" onClick={() => setMenuOpen(true)} aria-label="Open menu">
            <span className="menu-dots" aria-hidden="true">...</span>
          </button>
        </header>

        {!isOnline && (
          <Banner
            type="inline"
            header="You're offline"
            description="Some data may be outdated. Reconnect to refresh."
            className="wallet-banner"
          />
        )}

        <main key={route.path} className="content" data-route={route.name}>
          <RouteRenderer route={route} />
        </main>

        <Tabbar className="vn-tabbar">
          {tabItems.map((item) => {
            const isActive = route.path === item.path;
            return (
              <Tabbar.Item
                key={item.path}
                text={item.label}
                selected={isActive}
                className={`vn-tab ${isActive ? 'is-active' : ''}`}
                onClick={() => navigate(item.path)}
              >
                {item.icon}
              </Tabbar.Item>
            );
          })}
        </Tabbar>

        <Modal
          open={menuOpen}
          onOpenChange={setMenuOpen}
          className="menu-sheet"
          snapPoints={['62%']}
        >
          <Modal.Header after={<Modal.Close />}>
            Menu
          </Modal.Header>
          <List className="wallet-list">
            <Section className="wallet-section" header="Quick actions">
              <Cell
                before={(
                  <MenuIcon>
                    <path d="M12 8a4 4 0 1 1 0 8 4 4 0 0 1 0-8z" fill="none" stroke="currentColor" strokeWidth="1.6" />
                    <path d="M4 12h2m12 0h2M6.5 6.5l1.4 1.4m8.2 8.2 1.4 1.4M17.5 6.5l-1.4 1.4M8.1 16.1l-1.6 1.6" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" />
                  </MenuIcon>
                )}
                onClick={handleOpenSettings}
              >
                Settings
              </Cell>
              {botUrl && (
                <Cell
                  before={(
                    <MenuIcon>
                      <path d="M6 8h12a3 3 0 0 1 3 3v4H3v-4a3 3 0 0 1 3-3z" fill="none" stroke="currentColor" strokeWidth="1.6" />
                      <path d="M8.5 6.5h7a1.5 1.5 0 0 1 0 3h-7a1.5 1.5 0 0 1 0-3z" fill="none" stroke="currentColor" strokeWidth="1.6" />
                      <path d="M9 12.5h.01M15 12.5h.01" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" />
                    </MenuIcon>
                  )}
                  onClick={handleOpenBot}
                >
                  Open Bot
                </Cell>
              )}
              <Cell
                before={(
                  <MenuIcon>
                    <path d="M20 12a8 8 0 1 1-2.3-5.7" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" />
                    <path d="M20 4v6h-6" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" />
                  </MenuIcon>
                )}
                onClick={handleReload}
              >
                Reload
              </Cell>
              {addToHomeSupported && (
                <Cell
                  before={(
                    <MenuIcon>
                      <path d="M7 4h10a2 2 0 0 1 2 2v10a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2z" fill="none" stroke="currentColor" strokeWidth="1.6" />
                      <path d="M12 8v8M8 12h8" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" />
                    </MenuIcon>
                  )}
                  onClick={handleAddToHome}
                >
                  Add to Home Screen
                </Cell>
              )}
            </Section>
            <Section className="wallet-section" header="Legal">
              <Cell
                className={!termsUrl ? 'cell-disabled' : undefined}
                before={(
                  <MenuIcon>
                    <path d="M12 7.5h.01M11 11h1v5" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" />
                    <path d="M12 3.5a8.5 8.5 0 1 1 0 17 8.5 8.5 0 0 1 0-17z" fill="none" stroke="currentColor" strokeWidth="1.6" />
                  </MenuIcon>
                )}
                onClick={() => handleOpenUrl(termsUrl)}
              >
                Terms of Use
              </Cell>
              <Cell
                className={!privacyUrl ? 'cell-disabled' : undefined}
                before={(
                  <MenuIcon>
                    <path d="M12 4l6 3v5c0 4-2.6 6.8-6 8-3.4-1.2-6-4-6-8V7l6-3z" fill="none" stroke="currentColor" strokeWidth="1.6" />
                    <path d="M9.5 12.5l2 2 3-3" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" />
                  </MenuIcon>
                )}
                onClick={() => handleOpenUrl(privacyUrl)}
              >
                Privacy Policy
              </Cell>
            </Section>
            <Section className="wallet-section">
              <Button size="s" mode="bezeled" onClick={refresh}>
                Refresh data
              </Button>
            </Section>
          </List>
        </Modal>
      </div>
    </CallsProvider>
  );
}
