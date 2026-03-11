#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REPO_ID="$(basename "$ROOT_DIR")"
FINGERPRINT_PATH="$HOME/.codex/conventions/$REPO_ID.md"

QUERY=""
USE_GIT=0
declare -a FILES=()

usage() {
  cat <<'EOF'
Usage:
  route-skills.sh [--query "text"] [--from-git] [file1 file2 ...]

Examples:
  route-skills.sh --query "audit vonage webhook security"
  route-skills.sh --from-git --query "implement twilio to vonage parity"
  route-skills.sh api/controllers/webhookRoutes.js api/routes/sms.js
EOF
}

contains() {
  local haystack="$1"
  local needle="$2"
  [[ "$haystack" == *"$needle"* ]]
}

append_unique() {
  local value="$1"
  shift
  local -n arr_ref="$1"
  for existing in "${arr_ref[@]:-}"; do
    [[ "$existing" == "$value" ]] && return 0
  done
  arr_ref+=("$value")
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -q|--query)
      [[ $# -lt 2 ]] && { echo "Missing value for $1" >&2; exit 1; }
      QUERY="$2"
      shift 2
      ;;
    --from-git)
      USE_GIT=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      FILES+=("$1")
      shift
      ;;
  esac
done

if [[ $USE_GIT -eq 1 ]]; then
  if git -C "$ROOT_DIR" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    while IFS= read -r path; do
      [[ -n "$path" ]] && FILES+=("$path")
    done < <(git -C "$ROOT_DIR" diff --name-only)
  fi
fi

query_lc="$(echo "$QUERY" | tr '[:upper:]' '[:lower:]')"

declare -a explicit_skills=()
declare -a providers=()
declare -a skills=()
declare -a reasons=()

declare -a all_known_skills=(
  "integration-docs-kit"
  "intent-codegen"
  "bug-risk-review"
  "debug-fix-playbook"
  "legacy-code-explainer"
  "workflow-automation"
  "skill-creator"
  "skill-installer"
)

for skill in "${all_known_skills[@]}"; do
  if contains "$query_lc" "$skill"; then
    append_unique "$skill" explicit_skills
  fi
done

provider_keywords=(
  "twilio"
  "vonage"
  "deepgram"
  "openrouter"
  "grammy"
  "telegram"
  "pinpoint"
  "transcribe"
  "polly"
  "connect"
  "aws"
)

for key in "${provider_keywords[@]}"; do
  if contains "$query_lc" "$key"; then
    case "$key" in
      telegram|grammy) append_unique "grammy" providers ;;
      pinpoint|transcribe|polly|connect|aws) append_unique "aws" providers ;;
      *) append_unique "$key" providers ;;
    esac
  fi
done

for file in "${FILES[@]:-}"; do
  file_lc="$(echo "$file" | tr '[:upper:]' '[:lower:]')"

  if contains "$file_lc" "twilio"; then append_unique "twilio" providers; fi
  if contains "$file_lc" "vonage"; then append_unique "vonage" providers; fi
  if contains "$file_lc" "deepgram"; then append_unique "deepgram" providers; fi
  if contains "$file_lc" "openrouter"; then append_unique "openrouter" providers; fi
  if contains "$file_lc" "grammy" || contains "$file_lc" "telegram"; then append_unique "grammy" providers; fi
  if contains "$file_lc" "aws"; then append_unique "aws" providers; fi

  if contains "$file_lc" "webhook"; then
    append_unique "integration-docs-kit" skills
    append_unique "bug-risk-review" skills
  fi
  if contains "$file_lc" "api/functions/" || contains "$file_lc" "api/adapters/" || contains "$file_lc" "bot/"; then
    append_unique "intent-codegen" skills
  fi
done

if [[ ${#providers[@]} -gt 0 ]]; then
  append_unique "integration-docs-kit" skills
  reasons+=("Provider surface detected (${providers[*]}).")
fi

if contains "$query_lc" "review" || contains "$query_lc" "audit" || contains "$query_lc" "production readiness" || contains "$query_lc" "risk"; then
  append_unique "bug-risk-review" skills
fi

if contains "$query_lc" "debug" || contains "$query_lc" "reproduce" || contains "$query_lc" "root cause" || contains "$query_lc" "fix failure"; then
  append_unique "debug-fix-playbook" skills
fi

if contains "$query_lc" "explain" || contains "$query_lc" "understand" || contains "$query_lc" "legacy"; then
  append_unique "legacy-code-explainer" skills
fi

if contains "$query_lc" "implement" || contains "$query_lc" "add " || contains "$query_lc" "build " || contains "$query_lc" "proceed"; then
  append_unique "intent-codegen" skills
fi

if contains "$query_lc" "automation" || contains "$query_lc" "migrate" || contains "$query_lc" "refactor" || contains "$query_lc" "loop" || contains "$query_lc" "workflow"; then
  append_unique "workflow-automation" skills
fi

if contains "$query_lc" "create skill" || contains "$query_lc" "update skill"; then
  append_unique "skill-creator" skills
fi
if contains "$query_lc" "install skill"; then
  append_unique "skill-installer" skills
fi

declare -a final=()

# 1) explicit user named skills
for skill in "${explicit_skills[@]:-}"; do
  append_unique "$skill" final
done

# 2) mandatory provider safety
if [[ ${#providers[@]} -gt 0 ]]; then
  append_unique "integration-docs-kit" final
fi

# 3) primary intent defaults
for skill in "${skills[@]:-}"; do
  append_unique "$skill" final
done

if [[ ${#final[@]} -eq 0 ]]; then
  final=("intent-codegen" "bug-risk-review")
  reasons+=("No strong signal found; using safe fallback bundle.")
fi

echo "Skill Router Result"
echo "repo: $ROOT_DIR"
if [[ -n "$QUERY" ]]; then
  echo "query: $QUERY"
fi
if [[ ${#FILES[@]} -gt 0 ]]; then
  echo "files: ${FILES[*]}"
fi
if [[ ${#providers[@]} -gt 0 ]]; then
  echo "providers: ${providers[*]}"
fi

echo "recommended_skills:"
idx=1
for skill in "${final[@]}"; do
  echo "  $idx. $skill"
  idx=$((idx + 1))
done

echo "guardrails:"
if [[ -f "$FINGERPRINT_PATH" ]]; then
  echo "  - Load convention fingerprint first: $FINGERPRINT_PATH"
else
  echo "  - Generate convention fingerprint first: $ROOT_DIR/docs/agent/build-convention-fingerprint.sh --output $FINGERPRINT_PATH"
fi
if [[ ${#providers[@]} -gt 0 ]]; then
  echo "  - Run provider detection script before edits."
  echo "  - Load provider-docs-index.md before provider-specific references."
  echo "  - Run integration-version-report.sh with --check-latest."
fi
echo "  - Validate with fastest relevant checks before final response."
echo "  - Call out unresolved assumptions and mismatches explicitly."

if [[ ${#reasons[@]} -gt 0 ]]; then
  echo "notes:"
  for note in "${reasons[@]}"; do
    echo "  - $note"
  done
fi
