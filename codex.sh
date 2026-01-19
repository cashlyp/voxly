[200~#!/usr/bin/env bash
set -euo pipefail

# Codex stores config/state under CODEX_HOME (defaults to ~/.codex)
CODEX_HOME="${CODEX_HOME:-$HOME/.codex}"
mkdir -p "$CODEX_HOME"

timestamp="$(date +"%Y%m%d_%H%M%S")"

backup_if_exists () {
  local path="$1"
  if [[ -f "$path" ]]; then
    mv "$path" "${path}.bak_${timestamp}"
    echo "Backed up: $path -> ${path}.bak_${timestamp}"
  fi
}

# Back up existing files (Codex reads these)
backup_if_exists "$CODEX_HOME/config.toml"
backup_if_exists "$CODEX_HOME/AGENTS.md"
backup_if_exists "$CODEX_HOME/AGENTS.override.md"

# Write config.toml atomically (ensure same filesystem for atomic mv)
tmp_config="$(mktemp -p "$CODEX_HOME" config.toml.XXXXXX)"
cat > "$tmp_config" <<'EOF'
################################################################################
# Codex user configuration: ~/.codex/config.toml
# Docs:
# - Sample config + defaults: https://developers.openai.com/codex/config-sample
# - Full key reference:       https://developers.openai.com/codex/config-reference
################################################################################

################################################################################
# Core model selection
################################################################################

# Recommended default coding model for Codex.
model = "gpt-5.2-codex"

# Model used by /review (code review). Keep aligned unless you want cheaper reviews.
review_model = "gpt-5.2-codex"

# Provider id from [model_providers]. Built-in default is "openai".
model_provider = "openai"

################################################################################
# Reasoning & verbosity (supported by GPT-5 Codex family)
################################################################################

# reasoning effort: minimal | low | medium | high | xhigh
# Practical default for high-quality code/refactors without always going max.
model_reasoning_effort = "high"

# reasoning summary: auto | concise | detailed | none
model_reasoning_summary = "auto"

# output verbosity: low | medium | high
model_verbosity = "medium"

################################################################################
# Safety + execution posture
################################################################################

# When to ask before running commands:
# untrusted | on-failure | on-request | never
approval_policy = "on-request"

# Sandbox posture for tool calls:
# read-only | workspace-write | danger-full-access
# Use workspace-write if you want Codex to actually apply edits during refactors.
sandbox_mode = "workspace-write"

# Keep network off in workspace-write mode
[sandbox_workspace_write]
network_access = false

################################################################################
# Project instructions discovery (AGENTS.md layering)
################################################################################
project_doc_max_bytes = 32768
project_doc_fallback_filenames = []

################################################################################
# Quality-of-life
################################################################################

# Clickable citation opener: vscode | vscode-insiders | windsurf | cursor | none
file_opener = "vscode"

# Show/hide reasoning events in CLI output (does not change model quality)
hide_agent_reasoning = false
show_raw_agent_reasoning = false

check_for_update_on_startup = true

################################################################################
# History
################################################################################
[history]
persistence = "save-all"
max_bytes = 5242880

################################################################################
# Shell environment policy (optional hardening)
################################################################################
[shell_environment_policy]
inherit = "all"
ignore_default_excludes = true
exclude = []
set = {}
include_only = []

################################################################################
# Feature flags
################################################################################
[features]
shell_tool = true

# Off by default; enable only if you explicitly want web lookups inside Codex
web_search_request = false

# Helpful performance toggle (beta): snapshots shell env to speed repeat commands.
shell_snapshot = true

# Experimental: freeform apply_patch can be great for large refactors; enable if desired.
apply_patch_freeform = true

# Beta/experimental toggles (leave conservative)
unified_exec = false
remote_compaction = true
remote_models = false
EOF

mv "$tmp_config" "$CODEX_HOME/config.toml"
chmod 600 "$CODEX_HOME/config.toml"
echo "Wrote: $CODEX_HOME/config.toml"

# Write AGENTS.md atomically (ensure same filesystem for atomic mv)
tmp_agents="$(mktemp -p "$CODEX_HOME" AGENTS.md.XXXXXX)"
cat > "$tmp_agents" <<'EOF'
# ~/.codex/AGENTS.md
# Global guidance loaded by Codex before any work (layered with project AGENTS.md).
# Keep this short, crisp, and actionable.

## Working agreements (always)
- Behave like a senior engineer: correctness first, then clarity, then speed.
- Prefer small, reviewable diffs; avoid drive-by formatting changes.
- Preserve public APIs and behavior unless explicitly asked to change them.
- Don’t introduce new dependencies without asking (especially runtime deps).
- When uncertain, ask one targeted question or present 2 options with tradeoffs.

## Refactoring
- Refactor in safe steps: mechanical rename -> extraction -> simplification -> optimization.
- Keep functions small and named for intent; reduce nested branching where possible.
- Remove dead code only when you can prove it’s unused (or with user approval).

## Code generation
- Match the project’s existing conventions (lint rules, architecture, patterns).
- Use strict typing where the language supports it.
- Prefer standard-library solutions unless there’s a clear benefit.

## Testing & verification
- Add/adjust tests for any behavior change.
- If the repo has tests/linters, run the fastest relevant checks and report results.
- When you can’t run commands, explain what you would run and why.

## Output format (fast + useful)
- Provide a concise plan (3-6 bullets max).
- Then provide a short "What changed" summary:
  - Files touched
  - Key behavior changes
  - Any follow-ups / risks
- Do NOT paste unified diffs (e.g., "diff --git ...") by default.
- Only provide a diff/patch if I explicitly ask for it.
- Include exact commands to run (tests, lint, format) when relevant.
EOF

mv "$tmp_agents" "$CODEX_HOME/AGENTS.md"
chmod 600 "$CODEX_HOME/AGENTS.md"
echo "Wrote: $CODEX_HOME/AGENTS.md"

echo
echo "Done. Tip: run 'codex -c model=\"gpt-5.2-codex\" --help' to confirm it loads config."
