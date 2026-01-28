// Include Telegram UI styles first to allow our code override the package CSS.
import '@telegram-apps/telegram-ui/dist/styles.css';

import ReactDOM from 'react-dom/client';
import { StrictMode } from 'react';
import { retrieveLaunchParams } from '@tma.js/sdk-react';

import { Root } from '@/components/Root.tsx';
import { EnvUnsupported } from '@/components/EnvUnsupported.tsx';
import { init } from '@/init.ts';

import './index.css';

// Mock the environment in case, we are outside Telegram.
import './mockEnv.ts';

const root = ReactDOM.createRoot(document.getElementById('root')!);

async function start() {
  try {
    const launchParams = retrieveLaunchParams();
    const { tgWebAppPlatform: platform } = launchParams;
    const debug = (launchParams.tgWebAppStartParam || '').includes('debug')
      || import.meta.env.DEV;
    const enableEruda = import.meta.env.DEV
      && debug
      && ['ios', 'android'].includes(platform);

    await init({
      debug,
      eruda: enableEruda,
      mockForMacOS: platform === 'macos',
    });

    root.render(
      <StrictMode>
        <Root/>
      </StrictMode>,
    );
  } catch {
    root.render(<EnvUnsupported/>);
  }
}

void start();
