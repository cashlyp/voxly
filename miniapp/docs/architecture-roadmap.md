# Voicednut Mini App Architecture and Roadmap

This document defines the target architecture and implementation sequence for the Telegram Mini App admin console.

It is designed to keep integration safe, incremental, and production-ready without changing backend contracts.

## Source References

Telegram Mini Apps platform references used for integration constraints:
- About: https://docs.telegram-mini-apps.com/platform/about
- Init Data: https://docs.telegram-mini-apps.com/platform/init-data
- Settings Button: https://docs.telegram-mini-apps.com/platform/settings-button
- Back Button: https://docs.telegram-mini-apps.com/platform/back-button
- Haptic Feedback: https://docs.telegram-mini-apps.com/platform/haptic-feedback
- Theme Params: https://docs.telegram-mini-apps.com/platform/theme-params
- Viewport: https://docs.telegram-mini-apps.com/platform/viewport

Package references:
- Create Mini App: https://docs.telegram-mini-apps.com/packages/tma-js-create-mini-app
- SDK React: https://docs.telegram-mini-apps.com/packages/tma-js-sdk-react
- Init Data Node: https://docs.telegram-mini-apps.com/packages/tma-js-init-data-node

## Current Runtime Architecture

### 1) Platform Shell Layer
Files:
- `src/index.tsx`
- `src/components/Root.tsx`
- `src/components/App.tsx`

Responsibilities:
- Mini App SDK setup and bootstrap.
- Platform control wiring (settings button, back button, viewport/theme integration).
- Top-level routing handoff.

### 2) Navigation Layer
Files:
- `src/navigation/routes.tsx`

Responsibilities:
- Route definitions and page-level boundaries.
- Route-driven lifecycle and rendering flow.

### 3) Admin Domain Layer
Files:
- `src/pages/AdminDashboard/AdminDashboardPage.tsx`
- `src/pages/AdminDashboard/*.tsx` (module pages)
- `src/hooks/admin-dashboard/*`
- `src/services/admin-dashboard/*`

Responsibilities:
- Dashboard state composition across bootstrap, polling, and stream updates.
- Capability-aware module visibility.
- Read and write orchestration to API endpoints.

Current direction:
- Continue reducing monolithic page logic by moving pure primitives and side-effect workflows into dedicated service/hook modules.

### 4) Presentation Layer
Files:
- `src/pages/AdminDashboard/AdminDashboardPage.css`
- module-level page components
- `src/components/admin-dashboard/*` (shared dashboard UI components)

Responsibilities:
- Shared visual tokens and reusable UI primitives.
- Telegram-native, mobile-first dark interface.
- Stable component patterns (cards, chips, buttons, tiles, empty/error states).

## API Contract Boundaries

No backend behavior changes are required for this roadmap. The frontend keeps using:
- `POST /miniapp/session`
- `GET /miniapp/bootstrap`
- `GET /miniapp/jobs/poll`
- `POST /miniapp/action`

Reliability posture:
- Session bootstrap is required before privileged data/actions.
- Polling remains fallback-safe when streaming is degraded.
- Action calls remain capability-gated by backend.

## Implementation Sequence (Practical)

### Phase A: Foundation Hardening (in progress)
- Extract pure utilities from page-level components to `services/*`.
- Normalize parsing and coercion helpers for consistent API payload handling.
- Keep types explicit at page boundaries.

Done:
- `dashboardPrimitives.ts` created and wired into `AdminDashboardPage.tsx`.
- Polling lifecycle extracted to `hooks/useDashboardPollingLoop.ts`.
- Event stream lifecycle extracted to `hooks/useDashboardEventStream.ts`.
- Action orchestration extracted to `src/hooks/admin-dashboard/useDashboardActions.ts`.
- Feature flag resolution extracted to `src/hooks/admin-dashboard/useDashboardFeatureFlags.ts`.
- Module visibility/layout resolution extracted to `src/hooks/admin-dashboard/useDashboardModuleLayout.ts`.
- Ops and sync metric derivation extracted to `src/hooks/admin-dashboard/useDashboardOpsMetrics.ts`.
- Provider matrix/readiness/current-provider derivation extracted to `src/hooks/admin-dashboard/useDashboardProviderMetrics.ts`.
- Messaging metrics/validation/preview derivation extracted to `src/hooks/admin-dashboard/useDashboardMessagingMetrics.ts`.
- Users/audit refresh orchestration extracted to `src/hooks/admin-dashboard/useDashboardGovernanceData.ts`.
- Governance write actions extracted to `src/hooks/admin-dashboard/useDashboardGovernanceActions.ts`.
- Runtime control actions extracted to `src/hooks/admin-dashboard/useDashboardRuntimeControls.ts`.
- Call script lifecycle actions extracted to `src/hooks/admin-dashboard/useDashboardCallScriptActions.ts`.
- Messaging send/schedule orchestration extracted to `src/hooks/admin-dashboard/useDashboardMessagingActions.ts`.
- Provider preflight/switch orchestration extracted to `src/hooks/admin-dashboard/useDashboardProviderActions.ts`.
- Provider section rendering extracted to `src/components/admin-dashboard/ProviderChannelCard.tsx`.
- Dashboard blocked/empty/skeleton/fallback state cards extracted to `src/components/admin-dashboard/DashboardStateCards.tsx`.
- Header/module-navigation chrome extracted to `src/components/admin-dashboard/DashboardChrome.tsx`.
- Unified request-state and observability rail extracted to `src/components/admin-dashboard/DashboardStatusRail.tsx`.
- Action payload guard layer added in `src/services/admin-dashboard/dashboardActionGuards.ts`.
- Bootstrap/poll/stream response contract validation added in `src/services/admin-dashboard/dashboardApiContracts.ts`.
- Dashboard VM composition entrypoint added in `src/services/admin-dashboard/dashboardVm/buildDashboardVm.ts`.
- Dashboard VM per-module builders added in `src/services/admin-dashboard/dashboardVm/build{Ops,Sms,Mailer,Provider,Governance}VmSection.ts`.
- Per-module builder typing tightened with explicit `Pick<DashboardVm, ...>` section contracts in `src/services/admin-dashboard/dashboardVm/types.ts`.
- Per-page VM selectors extracted to `src/pages/AdminDashboard/vmSelectors.ts` and wired across module pages.
- Production smoke gate added in `scripts/smoke-admin-dashboard.mjs` and wired to `npm run validate:prod`.
- Module layout parsing extracted to `src/services/admin-dashboard/dashboardLayout.ts`.
- Session/cache/url transport helpers extracted to `src/services/admin-dashboard/dashboardTransport.ts`.

Next:
- Add module-level request-state primitives inside page modules (`sms`, `mailer`, `provider`).
- Add lightweight memoized selector helpers for expensive list/table derivations to reduce re-renders.

### Phase B: Page Composition Cleanup
- Ensure each feature module has a dedicated page component under `src/pages/AdminDashboard/`.
- Keep `AdminDashboardPage.tsx` as orchestration shell only.
- Push repeated UI sections into reusable components (stat tiles, panels, empty states).

### Phase C: Resilience and Observability
- Standardize request states: idle, loading, success, empty, error.
- Add trace-friendly activity metadata around write actions.
- Maintain deterministic fallback behavior if stream endpoint fails.

### Phase D: UX and Governance Maturity
- Role-aware navigation and module gating polish.
- Better admin copy for blocked/empty states.
- Audit and incident surfaces with filterable, production-grade views.

## Verification Gate Per Change

Run minimum checks after each meaningful refactor:

```bash
npm --prefix miniapp run lint
npm --prefix miniapp run build
npm --prefix miniapp run smoke:admin-dashboard
```

For auth/session-critical changes, also verify manually:
- Launch from Telegram bot menu only.
- Confirm init-data validation and session bootstrap succeeds.
- Confirm settings and back button lifecycle work on supported clients.

## Deployment Guardrails (Vercel)

- Root: `miniapp`
- Build: `npm run build`
- Output: `dist`
- Required env:
  - `VITE_API_BASE_URL` (preferred)
  - or `VITE_API_BASE`

Rollout checklist:
- Confirm API URL targets correct environment.
- Confirm bot `MINI_APP_URL` points to current Vercel deployment.
- Smoke test with real Telegram launch, not standalone browser only.

## Non-Goals

To keep upgrades safe, this roadmap intentionally avoids:
- backend endpoint contract changes
- new external dependencies unless explicitly approved
- large rewrites that reduce release confidence
