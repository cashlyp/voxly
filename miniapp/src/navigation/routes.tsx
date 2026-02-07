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

type RouteParams = Record<string, string | undefined>;

export interface Route<Params extends RouteParams = RouteParams> {
  path: string;
  name: string;
  Component: ComponentType<Params>;
  title?: string;
  icon?: JSX.Element;
}

const defineRoute = <Params extends RouteParams>(route: Route<Params>) => route;

export const routes = [
  defineRoute({ path: '/', name: 'dashboard', Component: Dashboard, title: 'Dashboard' }),
  defineRoute({ path: '/inbox', name: 'inbox', Component: Inbox, title: 'Inbox' }),
  defineRoute({ path: '/calls', name: 'calls', Component: Calls, title: 'Calls' }),
  defineRoute({ path: '/calls/:callSid', name: 'callConsole', Component: CallConsole, title: 'Call Console' }),
  defineRoute({ path: '/transcripts', name: 'transcripts', Component: Transcripts, title: 'Transcripts' }),
  defineRoute({ path: '/transcripts/:callSid', name: 'transcriptDetail', Component: TranscriptDetail, title: 'Transcript Detail' }),
  defineRoute({ path: '/settings', name: 'settings', Component: Settings, title: 'Settings' }),
  defineRoute({ path: '/provider', name: 'provider', Component: Provider, title: 'Provider Status' }),
  defineRoute({ path: '/users', name: 'users', Component: Users, title: 'Users' }),
  defineRoute({ path: '/logs', name: 'logs', Component: Logs, title: 'Logs' }),
];
