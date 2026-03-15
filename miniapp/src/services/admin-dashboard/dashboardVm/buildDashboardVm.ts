import type { DashboardVm } from '@/pages/AdminDashboard/types';

import type { DashboardVmSection } from '@/services/admin-dashboard/dashboardVm/types';

export function buildDashboardVm(...sections: DashboardVmSection[]): DashboardVm {
  return Object.assign({}, ...sections) as DashboardVm;
}
