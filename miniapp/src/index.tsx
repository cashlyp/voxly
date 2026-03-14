// Include Telegram UI styles first to allow our code override the package CSS.
import '@telegram-apps/telegram-ui/dist/styles.css';

import ReactDOM from 'react-dom/client';
import { StrictMode } from 'react';
import { retrieveLaunchParams } from '@tma.js/sdk-react';

import { EnvUnsupported } from '@/components/EnvUnsupported.tsx';
import { init } from '@/init.ts';

import './index.css';

if (import.meta.env.DEV) {
  // Mock the environment only in local development.
  await import('./mockEnv.ts');
}

const root = ReactDOM.createRoot(document.getElementById('root')!);

try {
  const launchParams = retrieveLaunchParams();
  const { tgWebAppPlatform: platform } = launchParams;
  const debug = (launchParams.tgWebAppStartParam || '').includes('debug')
    || import.meta.env.DEV;

  // Configure all application dependencies.
  await init({
    debug,
    eruda: debug && ['ios', 'android'].includes(platform),
    mockForMacOS: platform === 'macos',
  });
  const { Root } = await import('@/components/Root.tsx');

  root.render(
    <StrictMode>
      <Root/>
    </StrictMode>,
  );
} catch (error) {
  console.error('Mini App initialization failed:', error);
  root.render(<EnvUnsupported/>);
}
