import assert from 'node:assert/strict';
import { existsSync, readFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const root = path.resolve(__dirname, '..');

function resolvePath(relativePath) {
  return path.join(root, relativePath);
}

function expectFile(relativePath) {
  const target = resolvePath(relativePath);
  assert.ok(existsSync(target), `Expected file to exist: ${relativePath}`);
  return target;
}

function read(relativePath) {
  const target = expectFile(relativePath);
  return readFileSync(target, 'utf8');
}

function expectContains(relativePath, needle) {
  const content = read(relativePath);
  assert.ok(
    content.includes(needle),
    `Expected ${relativePath} to include: ${needle}`,
  );
}

function parseActionGuardKeys(content) {
  const actionMapMatch = content.match(/const ACTION_GUARDS:[\s\S]*?=\s*\{([\s\S]*?)\};/);
  assert.ok(actionMapMatch, 'Could not parse ACTION_GUARDS map');
  const body = actionMapMatch[1];
  const matches = [...body.matchAll(/'([^']+)'\s*:\s*[a-zA-Z0-9_]+/g)];
  return new Set(matches.map((entry) => entry[1]));
}

function main() {
  const requiredFiles = [
    'src/pages/AdminDashboard/AdminDashboardPage.tsx',
    'src/hooks/admin-dashboard/useDashboardActions.ts',
    'src/services/admin-dashboard/dashboardApiContracts.ts',
    'src/services/admin-dashboard/dashboardActionGuards.ts',
    'src/services/admin-dashboard/dashboardVm/buildDashboardVm.ts',
    'src/services/admin-dashboard/dashboardVm/buildOpsVmSection.ts',
    'src/services/admin-dashboard/dashboardVm/buildSmsVmSection.ts',
    'src/services/admin-dashboard/dashboardVm/buildMailerVmSection.ts',
    'src/services/admin-dashboard/dashboardVm/buildProviderVmSection.ts',
    'src/services/admin-dashboard/dashboardVm/buildGovernanceVmSection.ts',
    'src/pages/AdminDashboard/vmSelectors.ts',
  ];
  requiredFiles.forEach(expectFile);

  const requiredGuardedActions = [
    'provider.set',
    'provider.rollback',
    'provider.preflight',
    'sms.bulk.send',
    'sms.schedule.send',
    'email.bulk.send',
    'users.role.set',
    'callscript.update',
    'callscript.submit_review',
    'callscript.review',
    'callscript.promote_live',
    'callscript.simulate',
  ];
  const actionGuardContent = read('src/services/admin-dashboard/dashboardActionGuards.ts');
  const guardKeys = parseActionGuardKeys(actionGuardContent);
  requiredGuardedActions.forEach((action) => {
    assert.ok(guardKeys.has(action), `Missing action guard mapping for: ${action}`);
  });

  expectContains('src/hooks/admin-dashboard/useDashboardActions.ts', 'validateDashboardActionPayload');
  expectContains('src/hooks/admin-dashboard/useDashboardActions.ts', 'validateActionEnvelope');
  expectContains('src/pages/AdminDashboard/AdminDashboardPage.tsx', 'validateBootstrapPayload');
  expectContains('src/pages/AdminDashboard/AdminDashboardPage.tsx', 'validatePollPayload');
  expectContains('src/pages/AdminDashboard/AdminDashboardPage.tsx', 'validateStreamPayload');

  expectContains('src/pages/AdminDashboard/OpsDashboardPage.tsx', 'selectOpsPageVm(vm)');
  expectContains('src/pages/AdminDashboard/SmsSenderPage.tsx', 'selectSmsPageVm(vm)');
  expectContains('src/pages/AdminDashboard/MailerPage.tsx', 'selectMailerPageVm(vm)');
  expectContains('src/pages/AdminDashboard/ProviderControlPage.tsx', 'selectProviderPageVm(vm)');
  expectContains('src/pages/AdminDashboard/ScriptStudioPage.tsx', 'selectScriptStudioPageVm(vm)');
  expectContains('src/pages/AdminDashboard/UsersRolePage.tsx', 'selectUsersRolePageVm(vm)');
  expectContains('src/pages/AdminDashboard/AuditIncidentsPage.tsx', 'selectAuditIncidentsPageVm(vm)');

  process.stdout.write('miniapp smoke checks passed\n');
}

try {
  main();
} catch (error) {
  const message = error instanceof Error ? error.message : String(error);
  process.stderr.write(`miniapp smoke checks failed: ${message}\n`);
  process.exit(1);
}
