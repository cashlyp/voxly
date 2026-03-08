# Skill Routing Playbook

This playbook makes skill selection repeatable and safe.

Use it together with [AGENTS.md](/workspaces/voxly/AGENTS.md).

## 1. Deterministic Selection Algorithm

Run this sequence for every task:

1. Check explicit user skill requests.
2. Apply mandatory safety triggers.
3. Detect provider surface (query + files).
4. Detect repo area.
5. Detect primary intent.
6. Detect runtime constraints.
7. Build minimal covering skill set.
8. Run preflight guardrails.

## 2. Intent -> Skill Mapping

| Intent signal | Primary skill | Secondary skills |
|---|---|---|
| implement, add, modify, patch | `intent-codegen` | `bug-risk-review` |
| explain, understand, walk through, map flow | `legacy-code-explainer` | none |
| review, audit, production readiness, robustness | `bug-risk-review` | `integration-docs-kit` when provider-related |
| debug, reproduce, root cause, fix failure | `debug-fix-playbook` | `bug-risk-review` |
| automate repetitive commands, migrations, refactor loops | `workflow-automation` | `intent-codegen` |
| create/update/install skills | `skill-creator` or `skill-installer` | none |

## 3. Provider -> Skill Mapping

Always include `integration-docs-kit` if any signal contains:
- `twilio`
- `vonage`
- `aws` (Connect/Pinpoint/Polly/Transcribe)
- `openrouter`
- `deepgram`
- `grammy` or `telegram bot`

## 4. Repo Area -> Skill Mapping

| Path pattern | Required skills | Why |
|---|---|---|
| `api/controllers/*webhook*` | `integration-docs-kit`, `bug-risk-review` | auth/signature, callback shape, idempotency |
| `api/routes/sms.js` | `integration-docs-kit`, `bug-risk-review` | provider failover, statuses, retries |
| `api/app.js` provider sections | `integration-docs-kit`, `intent-codegen` | central routing and policy behavior |
| `api/functions/*` | `intent-codegen` | feature behavior implementation |
| `api/adapters/*` | `intent-codegen`, `bug-risk-review` | contract mapping and error semantics |
| `bot/*` provider integrations | `integration-docs-kit`, `intent-codegen` | provider-specific API/runtime contracts |

## 5. Runtime Constraint Rules

If one or more conditions are true, add `workflow-automation`:
- repeated command loops are needed
- scripts should be reused for determinism
- test harness is missing and a smoke harness must be created

## 6. Guardrails

### 6.1 Missing Context Guard

Before edits, confirm:
- provider/auth assumptions are known
- webhook/callback direction is known
- terminal state behavior is known

If unknown:
- ask one targeted question, or
- implement safe default that blocks unsafe actions.

### 6.2 Wrong Provider Docs Guard

Required for provider work:
1. Run provider detection script.
2. Load provider docs index first.
3. Load only provider-relevant sections.
4. Cite mismatches between local code and docs.

### 6.3 Outdated Sample Guard

Required for provider work:
1. Collect declared + resolved versions.
2. Attempt latest check.
3. If latest unavailable, mark drift as unknown.
4. Prefer version-matched behavior over generic snippets.

### 6.4 Wrong-Skill Abort Conditions

Abort and reroute skills when:
- callbacks/auth logic is being changed without `integration-docs-kit`.
- risk/audit output is requested without `bug-risk-review`.
- bug-fix work starts without a reproducible failure path.
- large implementation starts without `intent-codegen`.

## 7. Evidence Checklist for Final Output

Include:
- selected skills + rationale
- provider detection result
- docs and references used
- version posture
- checks run and checks skipped
- residual risks

## 8. Quick Commands

```bash
# Detect provider surface
/home/codespace/.codex/skills/integration-docs-kit/scripts/detect-integration-surface.sh /workspaces/voxly

# Version posture
/home/codespace/.codex/skills/workflow-automation/scripts/integration-version-report.sh /workspaces/voxly --check-latest

# Local routing helper
/workspaces/voxly/docs/agent/route-skills.sh --from-git --query "your task text"

# Local profile selector (print decision)
/workspaces/voxly/docs/agent/select-codex-profile.sh --from-git --query "your task text"

# Local profile selector (launch codex)
/workspaces/voxly/docs/agent/select-codex-profile.sh --from-git --query "your task text" --exec
```
