export type RouteMatch = {
  name: string;
  params: Record<string, string>;
  path: string;
};

const routes: { name: string; pattern: RegExp; keys: string[] }[] = [
  { name: 'dashboard', pattern: /^\/$/, keys: [] },
  { name: 'inbox', pattern: /^\/inbox$/, keys: [] },
  { name: 'calls', pattern: /^\/calls$/, keys: [] },
  { name: 'callConsole', pattern: /^\/calls\/([^/]+)$/, keys: ['callSid'] },
  { name: 'transcripts', pattern: /^\/transcripts$/, keys: [] },
  {
    name: 'transcriptDetail',
    pattern: /^\/transcripts\/([^/]+)$/,
    keys: ['callSid'],
  },
  { name: 'sms', pattern: /^\/sms$/, keys: [] },
  { name: 'provider', pattern: /^\/provider$/, keys: [] },
  { name: 'users', pattern: /^\/users$/, keys: [] },
  { name: 'logs', pattern: /^\/logs$/, keys: [] },
  { name: 'settings', pattern: /^\/settings$/, keys: [] },
];

function normalizePath(rawPath: string) {
  let path = (rawPath || '').trim();
  if (!path) return '/';
  if (path.startsWith('#')) path = path.slice(1);
  if (!path.startsWith('/')) path = `/${path}`;
  const queryIndex = path.indexOf('?');
  if (queryIndex >= 0) path = path.slice(0, queryIndex);
  const hashIndex = path.indexOf('#');
  if (hashIndex >= 0) path = path.slice(0, hashIndex);
  if (path.length > 1 && path.endsWith('/')) path = path.slice(0, -1);
  return path || '/';
}

export function getHashPath() {
  const raw = window.location.hash || '#/';
  return normalizePath(raw);
}

export function matchRoute(path: string): RouteMatch {
  const normalized = normalizePath(path);
  for (const route of routes) {
    const match = normalized.match(route.pattern);
    if (!match) continue;
    const params: Record<string, string> = {};
    route.keys.forEach((key, index) => {
      params[key] = match[index + 1];
    });
    return { name: route.name, params, path: normalized };
  }
  return { name: 'notFound', params: {}, path: normalized };
}

export function navigate(path: string) {
  window.location.hash = path.startsWith('/') ? path : `/${path}`;
}
