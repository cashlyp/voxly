import { useLaunchParams, useSignal, miniApp } from '@tma.js/sdk-react';
import { AppRoot } from '@telegram-apps/telegram-ui';
import { Suspense, lazy } from 'react';

const AdminDashboard = lazy(async () => {
  const module = await import('@/components/AdminDashboard.tsx');
  return { default: module.AdminDashboard };
});

export function App() {
  const lp = useLaunchParams();
  const isDark = useSignal(miniApp.isDark);

  return (
    <AppRoot
      appearance={isDark ? 'dark' : 'light'}
      platform={['macos', 'ios'].includes(lp.tgWebAppPlatform) ? 'ios' : 'base'}
    >
      <Suspense fallback={<div style={{ padding: 16 }}>Loading dashboard...</div>}>
        <AdminDashboard />
      </Suspense>
    </AppRoot>
  );
}
