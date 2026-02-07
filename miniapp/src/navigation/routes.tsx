import type { ComponentType, JSX } from 'react';

import { Dashboard } from '@/routes/Dashboard';
import { Inbox } from '@/routes/Inbox';
import { Calls } from '@/routes/Calls';
import { CallConsole } from '@/routes/CallConsole';
import { Settings } from '@/routes/Settings';
import { Transcripts } from '@/routes/Transcripts';
import { TranscriptDetail } from '@/routes/TranscriptDetail';
import { Provider } from '@/routes/Provider';
import { Users } from '@/routes/Users';
import { Logs } from '@/routes/Logs';

export interface Route {
  path: string;
  name: string;
  Component: ComponentType<Record<string, unknown>>;
  title?: string;
  icon?: JSX.Element;
}

export const routes: Route[] = [
  { path: '/', name: 'dashboard', Component: Dashboard, title: 'Dashboard' },
  { path: '/inbox', name: 'inbox', Component: Inbox, title: 'Inbox' },
  { path: '/calls', name: 'calls', Component: Calls, title: 'Calls' },
  { path: '/calls/:callSid', name: 'callConsole', Component: CallConsole, title: 'Call Console' },
  { path: '/transcripts', name: 'transcripts', Component: Transcripts, title: 'Transcripts' },
  { path: '/transcripts/:callSid', name: 'transcriptDetail', Component: TranscriptDetail, title: 'Transcript Detail' },
  { path: '/settings', name: 'settings', Component: Settings, title: 'Settings' },
  { path: '/provider', name: 'provider', Component: Provider, title: 'Provider Status' },
  { path: '/users', name: 'users', Component: Users, title: 'Users' },
  { path: '/logs', name: 'logs', Component: Logs, title: 'Logs' },
];
