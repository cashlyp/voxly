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
  { name: 'scripts', pattern: /^\/scripts$/, keys: [] },
  { name: 'users', pattern: /^\/users$/, keys: [] },
  { name: 'settings', pattern: /^\/settings$/, keys: [] },
];

export function getHashPath() {
  const raw = window.location.hash || '#/';
  const path = raw.startsWith('#') ? raw.slice(1) : raw;
  return path || '/';
}

export function matchRoute(path: string): RouteMatch {
  for (const route of routes) {
    const match = path.match(route.pattern);
    if (!match) continue;
    const params: Record<string, string> = {};
    route.keys.forEach((key, index) => {
      params[key] = match[index + 1];
    });
    return { name: route.name, params, path };
  }
  return { name: 'notFound', params: {}, path };
}

export function navigate(path: string) {
  window.location.hash = path.startsWith('/') ? path : `/${path}`;
}
