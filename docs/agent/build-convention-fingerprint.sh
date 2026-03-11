#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REPO_ROOT="$ROOT_DIR"
REPO_ID="$(basename "$ROOT_DIR")"
OUTPUT_PATH=""
FROM_GIT=0
DRY_RUN=0
EXPLICIT_FILES=0

declare -a INPUT_FILES=()

usage() {
  cat <<'USAGE_EOF'
Usage:
  build-convention-fingerprint.sh [options]

Options:
  --repo-root <path>      Repository root (default: auto-detected)
  --repo-id <id>          Fingerprint repo id (default: basename(repo-root))
  --output <path>         Output file path (default: ~/.codex/conventions/<repo-id>.md)
  --file <path>           Include one canonical file (repeatable, path relative to repo root)
  --from-git              Include changed/untracked files from git status
  --dry-run               Print generated markdown to stdout only
  -h, --help              Show this help

Examples:
  build-convention-fingerprint.sh
  build-convention-fingerprint.sh --from-git
  build-convention-fingerprint.sh --file api/app.js --file api/config.js
  build-convention-fingerprint.sh --repo-id voxly --output ~/.codex/conventions/voxly.md
USAGE_EOF
}

append_unique() {
  local value="$1"
  shift
  local -n arr_ref="$1"
  local existing
  for existing in "${arr_ref[@]:-}"; do
    if [[ "$existing" == "$value" ]]; then
      return 0
    fi
  done
  arr_ref+=("$value")
}

relpath() {
  local input="$1"
  local cleaned
  cleaned="${input#./}"
  cleaned="${cleaned#${REPO_ROOT}/}"
  printf '%s\n' "$cleaned"
}

quote_join() {
  local first=1
  local item
  for item in "$@"; do
    if [[ $first -eq 1 ]]; then
      printf '%s' "$item"
      first=0
    else
      printf ', %s' "$item"
    fi
  done
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo-root)
      [[ $# -lt 2 ]] && { echo "Missing value for $1" >&2; exit 1; }
      REPO_ROOT="$2"
      shift 2
      ;;
    --repo-id)
      [[ $# -lt 2 ]] && { echo "Missing value for $1" >&2; exit 1; }
      REPO_ID="$2"
      shift 2
      ;;
    --output)
      [[ $# -lt 2 ]] && { echo "Missing value for $1" >&2; exit 1; }
      OUTPUT_PATH="$2"
      shift 2
      ;;
    --file)
      [[ $# -lt 2 ]] && { echo "Missing value for $1" >&2; exit 1; }
      INPUT_FILES+=("$2")
      EXPLICIT_FILES=1
      shift 2
      ;;
    --from-git)
      FROM_GIT=1
      shift
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ ! -d "$REPO_ROOT" ]]; then
  echo "Repository root does not exist: $REPO_ROOT" >&2
  exit 1
fi

if [[ -z "$REPO_ID" ]]; then
  REPO_ID="$(basename "$REPO_ROOT")"
fi

if [[ -z "$OUTPUT_PATH" ]]; then
  OUTPUT_PATH="$HOME/.codex/conventions/$REPO_ID.md"
fi

if [[ $FROM_GIT -eq 1 ]]; then
  if git -C "$REPO_ROOT" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    while IFS= read -r changed; do
      [[ -z "$changed" ]] && continue
      normalized_changed="$(relpath "$changed")"
      case "$normalized_changed" in
        api/*|bot/*|README.md)
          append_unique "$normalized_changed" INPUT_FILES
          ;;
        *)
          # Ignore docs/meta-only edits for runtime convention baseline generation.
          ;;
      esac
    done < <(
      git -C "$REPO_ROOT" diff --name-only
      git -C "$REPO_ROOT" ls-files --others --exclude-standard
    )
  fi
fi

if [[ $EXPLICIT_FILES -eq 0 ]]; then
  default_candidates=(
    "README.md"
    "api/app.js"
    "api/config.js"
    "api/routes/sms.js"
    "api/services/webhookRoutes.vonageEvent.test.js"
    "api/services/voiceRuntimeControl.test.js"
    "api/package.json"
    "bot/bot.js"
    "bot/config.js"
    "bot/package.json"
  )
  for candidate in "${default_candidates[@]}"; do
    if [[ -f "$REPO_ROOT/$candidate" ]]; then
      append_unique "$candidate" INPUT_FILES
    fi
  done
fi

declare -a CANONICAL_FILES=()
for item in "${INPUT_FILES[@]}"; do
  normalized="$(relpath "$item")"
  if [[ -f "$REPO_ROOT/$normalized" ]]; then
    append_unique "$normalized" CANONICAL_FILES
  fi
done

if [[ ${#CANONICAL_FILES[@]} -eq 0 ]]; then
  echo "No existing files selected. Use --file with valid repo-relative paths." >&2
  exit 1
fi

utc_date="$(date -u +%F)"
declare -a provider_scan_paths=()
if [[ -f "$REPO_ROOT/README.md" ]]; then
  provider_scan_paths+=("$REPO_ROOT/README.md")
fi
if [[ -f "$REPO_ROOT/api/package.json" ]]; then
  provider_scan_paths+=("$REPO_ROOT/api/package.json")
fi
if [[ -f "$REPO_ROOT/bot/package.json" ]]; then
  provider_scan_paths+=("$REPO_ROOT/bot/package.json")
fi
for file in "${CANONICAL_FILES[@]}"; do
  append_unique "$REPO_ROOT/$file" provider_scan_paths
done

declare -a detected_providers=()
if rg -qi "twilio" "${provider_scan_paths[@]}" 2>/dev/null; then append_unique "twilio" detected_providers; fi
if rg -qi "vonage" "${provider_scan_paths[@]}" 2>/dev/null; then append_unique "vonage" detected_providers; fi
if rg -qi "deepgram" "${provider_scan_paths[@]}" 2>/dev/null; then append_unique "deepgram" detected_providers; fi
if rg -qi "openrouter" "${provider_scan_paths[@]}" 2>/dev/null; then append_unique "openrouter" detected_providers; fi
if rg -qi "grammy|telegram" "${provider_scan_paths[@]}" 2>/dev/null; then append_unique "grammy" detected_providers; fi
if rg -qi "aws|connect|pinpoint|polly|transcribe" "${provider_scan_paths[@]}" 2>/dev/null; then append_unique "aws" detected_providers; fi

provider_summary="none detected"
if [[ ${#detected_providers[@]} -gt 0 ]]; then
  provider_summary="$(quote_join "${detected_providers[@]}")"
fi

api_node=""
bot_node=""
if [[ -f "$REPO_ROOT/api/package.json" ]]; then
  api_node="$(sed -n 's/.*"node"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$REPO_ROOT/api/package.json" | head -n 1)"
fi
if [[ -f "$REPO_ROOT/bot/package.json" ]]; then
  bot_node="$(sed -n 's/.*"node"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$REPO_ROOT/bot/package.json" | head -n 1)"
fi
node_summary="unspecified"
if [[ -n "$api_node" && -n "$bot_node" ]]; then
  node_summary="api: $api_node, bot: $bot_node"
elif [[ -n "$api_node" ]]; then
  node_summary="api: $api_node"
elif [[ -n "$bot_node" ]]; then
  node_summary="bot: $bot_node"
fi

module_summary="mixed/unknown"
if rg -q "module\.exports|require\(" "$REPO_ROOT/api" "$REPO_ROOT/bot" 2>/dev/null; then
  module_summary="CommonJS"
fi

declare -a validation_commands=()
if [[ -f "$REPO_ROOT/api/package.json" ]]; then
  if rg -q '"validate:profiles"' "$REPO_ROOT/api/package.json"; then
    append_unique "npm --prefix api run validate:profiles" validation_commands
  fi
  if rg -q '"preflight:provider"' "$REPO_ROOT/api/package.json"; then
    append_unique "npm --prefix api run preflight:provider" validation_commands
  fi
  if rg -q '"parity:providers"' "$REPO_ROOT/api/package.json"; then
    append_unique "npm --prefix api run parity:providers" validation_commands
  fi
  if rg -q '"test"' "$REPO_ROOT/api/package.json"; then
    append_unique "npm --prefix api test -- <targeted-test-file>" validation_commands
  fi
fi
if [[ -f "$REPO_ROOT/bot/package.json" ]] && rg -q '"test"' "$REPO_ROOT/bot/package.json"; then
  append_unique "npm --prefix bot run test" validation_commands
fi

render_file_reason() {
  local path="$1"
  case "$path" in
    README.md) echo "high-level system constraints and provider matrix" ;;
    api/app.js) echo "API composition and cross-service orchestration" ;;
    api/config.js) echo "environment parsing and runtime defaults" ;;
    api/routes/sms.js) echo "provider abstraction, validation, and error mapping" ;;
    api/services/webhookRoutes.vonageEvent.test.js) echo "webhook callback ordering and dedupe behavior" ;;
    api/services/voiceRuntimeControl.test.js) echo "runtime decision logic and regression expectations" ;;
    api/package.json) echo "API scripts and verification contract" ;;
    bot/bot.js) echo "bot middleware order and command/callback flow" ;;
    bot/config.js) echo "bot configuration and fail-fast rules" ;;
    bot/package.json) echo "bot runtime scripts and engine posture" ;;
    *) echo "selected as representative local convention source" ;;
  esac
}

generate_markdown() {
  cat <<DOC_BLOCK
# Convention Fingerprint: $REPO_ID

## Metadata
- Repo ID: \`$REPO_ID\`
- Last Updated (UTC): \`$utc_date\`
- Maintainer: \`codex + project owners\`
- Confidence: \`medium\`

## 1) Architecture Boundaries
- Entry points:
  - \`api/app.js\` for API runtime when present.
  - \`bot/bot.js\` for Telegram bot runtime when present.
- Layer ownership:
  - Provider-specific adapters stay under \`api/adapters/*\`.
  - Request/channel handlers stay under \`api/routes/*\`.
  - Domain orchestration stays under \`api/services/*\`.
  - Bot shared helpers stay under \`bot/utils/*\`.
- Forbidden crossings:
  - Do not add direct provider SDK usage in bot flows when API adapters/services already own provider logic.
  - Do not bypass existing adapters for provider normalization and policy.
- Public contracts that must remain stable:
  - Existing webhook paths and callback semantics.
  - Provider mode behavior and status mapping continuity.

## 2) Naming and File Layout
- Naming conventions:
  - Constants: \`UPPER_SNAKE_CASE\`.
  - Functions/variables: \`camelCase\`.
  - Factory helpers commonly use \`create*\` naming.
- File placement rules:
  - Provider policy/state logic in \`api/adapters\` or \`api/services\`.
  - Shared helpers in \`api/utils\` or \`bot/utils\`.
  - Tests near touched domain modules.
- Export/import style:
  - \`$module_summary\`.

## 3) Error and Response Semantics
- Error object shape:
  - Prefer typed/mapped errors carrying code + status + retryability when crossing provider boundaries.
- Status mapping rules:
  - Keep canonical provider status normalization stable.
- Fail-fast vs recover behavior:
  - Fail fast for missing critical configuration.
  - Use controlled retries and mapped errors for transient provider/network failure.

## 4) Logging and Observability
- Required log fields:
  - Include route/action/provider and stable entity ids where available.
- Redaction rules:
  - Redact phone numbers and payload bodies in logs.
- Health/metric emission rules:
  - Preserve existing service health and action metric patterns.

## 5) Testing Conventions
- Test framework and style:
  - Jest with \`describe/test/expect\` style.
- Required test locality for new logic:
  - Add targeted tests close to the changed module.
- Priority regression areas:
  - Webhook dedupe/idempotency.
  - Provider status mapping and fallback behavior.

## 6) Dependency and Runtime Rules
- Allowed dependency changes:
  - No new dependencies without explicit approval.
- Restricted or forbidden dependencies:
  - Reuse existing utilities/SDK abstractions before adding packages.
- Runtime assumptions (node version, module system, env):
  - Node engines: \`$node_summary\`.
  - Module system: \`$module_summary\`.
  - Provider surface detected: \`$provider_summary\`.

## 7) Patch Contract Defaults
- Required contract fields before edits:
  - Inputs
  - Outputs
  - Invariants
  - Non-goals
  - Allowed files
  - Forbidden files
  - One normal case and one edge case
- Default blast-radius limits:
  - Prefer target module + nearest tests first.
  - Avoid cross-layer refactors unless requested.

## 8) Conformance Gate Checklist
- [ ] Structure conformance passed.
- [ ] Behavior matches declared contract examples.
- [ ] Naming/style/logging align with local conventions.
- [ ] No unapproved dependency changes.
- [ ] Targeted tests exist or explicit gap note is documented.

## 9) Validation Commands
DOC_BLOCK

  if [[ ${#validation_commands[@]} -gt 0 ]]; then
    local cmd
    for cmd in "${validation_commands[@]}"; do
      printf -- '- `%s`\n' "$cmd"
    done
  else
    echo "- No standard validation commands detected."
  fi

  cat <<'DOC_CANONICAL'

## 10) Canonical Source Files (auto-selected)
DOC_CANONICAL

  local canonical
  for canonical in "${CANONICAL_FILES[@]}"; do
    printf -- '- `%s`: %s\n' "$canonical" "$(render_file_reason "$canonical")"
  done

  cat <<'DOC_END'

## 11) Known Exceptions
- Exception: quote style may be mixed across legacy files.
- Rationale: repository evolved over time and style is not globally normalized.
- Expiration/review date: `2026-06-30`
DOC_END
}

if [[ $DRY_RUN -eq 1 ]]; then
  generate_markdown
  exit 0
fi

mkdir -p "$(dirname "$OUTPUT_PATH")"
tmp_output="$(mktemp)"
generate_markdown > "$tmp_output"
mv "$tmp_output" "$OUTPUT_PATH"

printf 'Generated convention fingerprint: %s\n' "$OUTPUT_PATH"
printf 'Canonical files included: %s\n' "${#CANONICAL_FILES[@]}"
