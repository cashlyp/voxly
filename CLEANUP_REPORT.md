# API Cleanup Report

Date: 2026-03-04
Scope: `/api` runtime and direct imports

## Skills and Evidence
- Skills used:
  - `integration-docs-kit`: provider surface detection and docs-first guardrails.
  - `intent-codegen`: minimal-diff implementation strategy.
  - `bug-risk-review`: deadlock/blocking and concurrency hazard audit.
  - `workflow-automation`: repeatable checks (`workflow-run-fast`, integration audit scripts).
- Providers detected (repo scan): `aws`, `deepgram`, `twilio`, `vonage` (and `grammy` in `/bot`).
- Docs checked:
  - Local references:
    - `/home/codespace/.codex/skills/integration-docs-kit/references/provider-docs-index.md`
    - `/home/codespace/.codex/skills/integration-docs-kit/references/provider-routing.md`
    - `/home/codespace/.codex/skills/integration-docs-kit/references/provider-playbooks.md`
    - `/home/codespace/.codex/skills/integration-docs-kit/references/integration-cookbook.md`
    - `/home/codespace/.codex/skills/integration-docs-kit/references/integration-checklists.md`
    - `/home/codespace/.codex/skills/integration-docs-kit/references/production-troubleshooting-checklist.md`
    - `/home/codespace/.codex/skills/integration-docs-kit/references/provider-version-watch.md`
  - Context7 package docs:
    - `/twilio/twilio-node` (voice call create/update + HTTP timeout options)
    - `/vonage/vonage-node-sdk` (voice API promise behavior and 3.x method mapping)
    - `/aws/aws-sdk-js-v3` (`client.send(command)` promise behavior)
- Version assumptions (from integration audit scripts):
  - `twilio`: declared `^4.19.3`, resolved `4.23.0`, latest watch `5.12.2`.
  - `@vonage/server-sdk`: declared/resolved `^3.25.1`/`3.25.1`, latest watch `3.26.4`.
  - `@aws-sdk/client-connect`: declared `^3.374.0`, resolved `3.964.0`, latest watch `3.1000.0`.
  - `@deepgram/sdk`: declared `^3.3.4`, resolved `3.13.0`, latest watch `4.11.3`.

## A) API Surface Map

### Entrypoint and registration style
- Primary API entrypoint: [`api/app.js`](/workspaces/voxly/api/app.js)
- Route registration pattern:
  - Large inline route table in `app.js`
  - Plus modular route registration via:
    - [`api/controllers/callRoutes.js`](/workspaces/voxly/api/controllers/callRoutes.js)
    - [`api/controllers/statusRoutes.js`](/workspaces/voxly/api/controllers/statusRoutes.js)
    - [`api/controllers/webhookRoutes.js`](/workspaces/voxly/api/controllers/webhookRoutes.js)

### Route inventory (registered in code)
- `app.js` directly registers 57 HTTP/WS routes.
- `controllers/callRoutes.js` registers 6 routes.
- `controllers/statusRoutes.js` registers 6 routes.
- `controllers/webhookRoutes.js` registers 23 routes.
- Full extracted list (method/path with source line) is from static scan and includes, for example:
  - `POST /outbound-call` (`callRoutes.js:882`)
  - `POST /incoming` (`app.js:10631`)
  - `POST /webhook/call-status` (`webhookRoutes.js:2053`)
  - `POST /webhook/sms` (`webhookRoutes.js:2058`)
  - `GET /status` (`statusRoutes.js:485`)
  - `WS /connection` (`app.js:7949`)

### Dependency graph for `/api`
- Import graph is static (no dynamic `require(...)` with non-literal path found).
- Core edges:
  - `app.js -> controllers/*`, `routes/*`, `adapters/*`, `functions/*`, `db/db.js`, `middleware/twilioSignature.js`
  - `controllers/webhookRoutes.js -> adapters/providerFlowPolicy.js`, `functions/transferCall.js`
  - `routes/sms.js -> adapters/providerFlowPolicy.js`, `config.js`
  - New shared utility edge:
    - `adapters/*`, `app.js`, `routes/sms.js`, `functions/transferCall.js`, `adapters/providerPreflight.js` -> `utils/asyncControl.js`

## B) Deadlock / Blocking Risks Found and Fixed

### 1) Outbound Twilio call could stall indefinitely in provider failover path
- Risk:
  - `client.calls.create(...)` in `placeOutboundCall` had no explicit timeout guard.
  - A hanging provider request could block failover progression and tie up request processing.
- Fix:
  - Wrapped call creation with timeout guard and structured logging.
  - File: [`api/app.js:12699`](/workspaces/voxly/api/app.js:12699)

### 2) Live transfer operation had no timeout
- Risk:
  - `transferCall` awaited Twilio update without timeout.
  - Could hang long-running admin action flow.
- Fix:
  - Added bounded timeout wrapper with timeout code `transfer_call_timeout`.
  - File: [`api/functions/transferCall.js:10`](/workspaces/voxly/api/functions/transferCall.js:10)

### 3) Inconsistent timeout wrappers across provider adapters and preflight probes
- Risk:
  - Multiple local `withTimeout` implementations increased drift risk and inconsistent behavior.
  - No unified long-operation warning or timeout telemetry shape.
- Fix:
  - Added shared timeout utility with:
    - bounded timeout rejection
    - `long_running_operation` warning event
    - `operation_timeout` structured error event
    - `timer.unref()` to avoid timers keeping the event loop alive
  - New file: [`api/utils/asyncControl.js`](/workspaces/voxly/api/utils/asyncControl.js)
  - Replaced duplicated wrappers in:
    - [`api/adapters/AwsConnectAdapter.js`](/workspaces/voxly/api/adapters/AwsConnectAdapter.js)
    - [`api/adapters/AwsTtsAdapter.js`](/workspaces/voxly/api/adapters/AwsTtsAdapter.js)
    - [`api/adapters/VonageVoiceAdapter.js`](/workspaces/voxly/api/adapters/VonageVoiceAdapter.js)
    - [`api/adapters/providerPreflight.js`](/workspaces/voxly/api/adapters/providerPreflight.js)
    - [`api/routes/sms.js`](/workspaces/voxly/api/routes/sms.js)
    - [`api/app.js:12243`](/workspaces/voxly/api/app.js:12243)

### 4) Unbounded in-memory idempotency cache in SMS service
- Risk:
  - `idempotencyCache` could grow indefinitely with sustained traffic (shared mutable state without bounds).
  - Potential memory-pressure and latency degradation under load.
- Fix:
  - Added TTL + max-size pruning and centralized cache accessors.
  - File: [`api/routes/sms.js:492`](/workspaces/voxly/api/routes/sms.js:492)

## C) Duplicate Code Removal

### Timeout wrapper dedupe
- Before:
  - Separate timeout implementations in 5 runtime files.
- After:
  - Shared helper: [`api/utils/asyncControl.js`](/workspaces/voxly/api/utils/asyncControl.js)
  - Consumers updated to shared implementation with per-call metadata.

### Shutdown handler dedupe
- Before:
  - Near-identical `SIGINT` and `SIGTERM` cleanup blocks.
- After:
  - Single `gracefulShutdown` function with signal-specific messages.
  - File: [`api/app.js:16246`](/workspaces/voxly/api/app.js:16246)

## D) Unused / Dead Code and Dependency Removal

### Removed dependencies (verified unused)
- Removed from [`api/package.json`](/workspaces/voxly/api/package.json):
  - `@aws-sdk/client-sqs`
  - `@aws-sdk/client-transcribe-streaming`
  - `express-validator`
- Lockfile updated: [`api/package-lock.json`](/workspaces/voxly/api/package-lock.json)

### Proof of non-usage
- Full-repo grep (excluding `node_modules`) showed no runtime imports/usage for these packages outside `package.json`/`package-lock.json`.
- Command evidence used:
  - `grep -RIn "@aws-sdk/client-sqs|client-sqs" /workspaces/voxly --exclude-dir=node_modules`
  - `grep -RIn "@aws-sdk/client-transcribe-streaming|TranscribeStreamingClient|StartStreamTranscription" /workspaces/voxly --exclude-dir=node_modules`
  - `grep -RIn "express-validator" /workspaces/voxly --exclude-dir=node_modules`

### Unused files/exports/routes
- No safely-removable runtime files or exported API symbols were deleted in this pass.
- Entry script files (`setup-env.js`, provider scripts, `ecosystem.config.js`) are intentionally retained as operational entrypoints.

### Config/env audit
- `.env.example` keys scanned against `/api` JS corpus.
- Result: `192/192` keys referenced at least once in code. No unreferenced env keys removed.

## E) Suspected-Unused but Not Removed
- None identified that met safe-removal confidence thresholds.
- No dynamic module-loading patterns were detected (`NO_DYNAMIC_REQUIRE_CALLS_FOUND`), but provider/runtime-sensitive paths were still treated conservatively.

## F) Quality Gates and Validation

### Commands run
- `workflow-run-integration-audit.sh /workspaces/voxly --check-latest`
- `integration-version-report.sh /workspaces/voxly --check-latest`
- `detect-integration-surface.sh /workspaces/voxly`
- `/home/codespace/.codex/skills/workflow-automation/scripts/workflow-run-fast.sh /workspaces/voxly`
- `npm test -- --runInBand` (in `/api`)
- `npm run parity:providers` (in `/api`)
- `npm run lint` (in `/api`) -> script missing
- `npm run build` (in `/api`) -> script missing
- `npm run preflight:provider` (in `/api`) -> blocked: missing admin token

### Results summary
- Tests: PASS (`tests/route-registration.smoke.test.js`)
- Provider parity smoke: PASS (offline checks)
- Lint/build: not configured in package scripts
- Provider preflight script: not executed end-to-end due missing token-based auth input

## Regression Checklist (Reviewer Sanity Test)
1. Voice outbound path:
- `POST /outbound-call` (Twilio primary + failover behavior) and verify no contract change in response shape.
2. Transfer action path:
- Trigger transfer via Telegram/admin action and verify timeout fallback message behavior.
3. Webhook paths:
- `POST /webhook/call-status`
- `POST /webhook/twilio-stream`
- `POST /webhook/sms`
- `POST /webhook/sms-status`
- `POST /webhook/sms-delivery`
4. Vonage parity:
- `GET /va`, `POST /ve`, `GET/POST /vs`, `GET/POST /vd`
5. Admin/status endpoints:
- `GET /status`, `GET /health`, `GET /status/provider-compat`, `GET /api/observability/gpt`
6. SMS idempotency behavior:
- Repeat same idempotency key payload and verify dedupe response unchanged.
- Reuse same key with different payload and verify conflict behavior remains.
7. Provider scripts:
- `npm run parity:providers` remains passing in offline mode.
