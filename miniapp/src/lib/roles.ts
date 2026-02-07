export type RoleTier = 'admin' | 'operator' | 'viewer' | 'unknown';

export function resolveRoleTier(roles: string[] = []): RoleTier {
  if (roles.includes('admin')) return 'admin';
  if (roles.includes('operator')) return 'operator';
  if (roles.includes('viewer')) return 'viewer';
  return 'unknown';
}

export function canAccessRoute(role: RoleTier, routeName: string) {
  if (role === 'admin') return true;
  const baseRoutes = [
    'dashboard',
    'inbox',
    'calls',
    'callConsole',
    'sms',
    'settings',
    'transcripts',
    'transcriptDetail',
  ];
  if (role === 'operator') {
    return baseRoutes.includes(routeName);
  }
  if (role === 'viewer') {
    return baseRoutes.includes(routeName);
  }
  return routeName === 'dashboard';
}
