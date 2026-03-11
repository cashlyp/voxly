#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REPO_ID="$(basename "$ROOT_DIR")"
FINGERPRINT_PATH="$HOME/.codex/conventions/$REPO_ID.md"

QUERY=""
USE_GIT=0
EXEC_MODE=0
PRINT_MODE=0
declare -a FILES=()
declare -a PASS_ARGS=()
declare -a QUERY_WORDS=()

usage() {
  cat <<'EOF'
Usage:
  select-codex-profile.sh [--query "text"] [--from-git] [--print] [--exec] [file1 file2 ...]
  select-codex-profile.sh [--query "text"] [--from-git] [--exec] -- [extra codex args]

Modes:
  --print    Print selected profile and reasoning (default mode).
  --exec     Launch codex with the selected profile.

Examples:
  select-codex-profile.sh --query "review webhook auth flow"
  select-codex-profile.sh --from-git --query "fix deepgram regression" --exec
  select-codex-profile.sh --query "implement vonage callback retry policy" --exec -- --approval on-request
EOF
}

to_lc() {
  echo "$1" | tr '[:upper:]' '[:lower:]'
}

contains_any() {
  local haystack="$1"
  shift
  local needle
  for needle in "$@"; do
    if [[ "$haystack" == *"$needle"* ]]; then
      return 0
    fi
  done
  return 1
}

add_unique() {
  local value="$1"
  shift
  local -n arr_ref="$1"
  local item
  for item in "${arr_ref[@]:-}"; do
    [[ "$item" == "$value" ]] && return 0
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
    --print)
      PRINT_MODE=1
      shift
      ;;
    --exec)
      EXEC_MODE=1
      shift
      ;;
    --)
      shift
      PASS_ARGS=("$@")
      break
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      if [[ -f "$1" || "$1" == *"/"* ]]; then
        FILES+=("$1")
      else
        QUERY_WORDS+=("$1")
      fi
      shift
      ;;
  esac
done

if [[ -z "$QUERY" && ${#QUERY_WORDS[@]} -gt 0 ]]; then
  QUERY="${QUERY_WORDS[*]}"
fi

if [[ $EXEC_MODE -eq 0 && $PRINT_MODE -eq 0 ]]; then
  PRINT_MODE=1
fi

if [[ $USE_GIT -eq 1 ]]; then
  if git -C "$ROOT_DIR" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    while IFS= read -r path; do
      [[ -n "$path" ]] && add_unique "$path" FILES
    done < <(git -C "$ROOT_DIR" diff --name-only; git -C "$ROOT_DIR" ls-files --others --exclude-standard)
  fi
fi

query_lc="$(to_lc "$QUERY")"
files_blob_lc="$(to_lc "${FILES[*]:-}")"
signal_blob="$query_lc $files_blob_lc"

provider_keys=("twilio" "vonage" "deepgram" "openrouter" "grammy" "telegram" "aws" "connect" "pinpoint" "polly" "transcribe")
review_keys=("review" "audit" "risk" "security" "hardening" "production readiness")
debug_keys=("debug" "fix" "bug" "failure" "failing" "error" "regression" "flaky" "root cause" "reproduce")
explain_keys=("explain" "understand" "walk through" "trace" "legacy")
automation_keys=("automate" "automation" "workflow" "loop" "migration" "migrate" "batch")
implement_keys=("implement" "add" "build" "create" "change" "modify" "patch")
research_keys=("research" "investigate" "compare" "latest" "best practice")
maintenance_keys=("maintenance" "chore" "cleanup" "bump" "upgrade" "deprecation")

provider_signal=0
review_signal=0
debug_signal=0
explain_signal=0
automation_signal=0
implement_signal=0
research_signal=0
maintenance_signal=0

contains_any "$signal_blob" "${provider_keys[@]}" && provider_signal=1
contains_any "$signal_blob" "${review_keys[@]}" && review_signal=1
contains_any "$signal_blob" "${debug_keys[@]}" && debug_signal=1
contains_any "$signal_blob" "${explain_keys[@]}" && explain_signal=1
contains_any "$signal_blob" "${automation_keys[@]}" && automation_signal=1
contains_any "$signal_blob" "${implement_keys[@]}" && implement_signal=1
contains_any "$signal_blob" "${research_keys[@]}" && research_signal=1
contains_any "$signal_blob" "${maintenance_keys[@]}" && maintenance_signal=1

selected_profile=""
declare -a reasons=()

if [[ $provider_signal -eq 1 ]]; then
  if [[ $review_signal -eq 1 ]]; then
    selected_profile="integration-audit"
    reasons+=("Provider signals + review/audit intent.")
  elif [[ $maintenance_signal -eq 1 && $debug_signal -eq 0 ]]; then
    selected_profile="integration-maintenance"
    reasons+=("Provider signals + maintenance/chore intent.")
  else
    selected_profile="integration-implement"
    if [[ $debug_signal -eq 1 ]]; then
      reasons+=("Provider signals + debug/fix intent (implementation-depth profile selected).")
    else
      reasons+=("Provider signals detected; defaulting to provider implementation profile.")
    fi
  fi
else
  if [[ $review_signal -eq 1 ]]; then
    selected_profile="review"
    reasons+=("Review/audit/risk intent detected.")
  elif [[ $debug_signal -eq 1 ]]; then
    selected_profile="debug"
    reasons+=("Debug/fix/regression intent detected.")
  elif [[ $explain_signal -eq 1 ]]; then
    selected_profile="legacy"
    reasons+=("Explain/trace/legacy understanding intent detected.")
  elif [[ $automation_signal -eq 1 ]]; then
    selected_profile="automate"
    reasons+=("Automation/migration/repetitive workflow intent detected.")
  elif [[ $implement_signal -eq 1 ]]; then
    selected_profile="intent"
    reasons+=("Feature implementation/change intent detected.")
  elif [[ $research_signal -eq 1 ]]; then
    selected_profile="research"
    reasons+=("Research/comparison/latest-intent detected.")
  else
    selected_profile="fast"
    reasons+=("No strong intent signal; defaulting to fast profile.")
  fi
fi

if [[ $PRINT_MODE -eq 1 ]]; then
  echo "Codex Profile Selector"
  echo "repo: $ROOT_DIR"
  if [[ -f "$FINGERPRINT_PATH" ]]; then
    echo "convention_fingerprint: $FINGERPRINT_PATH"
  else
    echo "convention_fingerprint: missing"
    echo "fingerprint_generate: $ROOT_DIR/docs/agent/build-convention-fingerprint.sh --output $FINGERPRINT_PATH"
  fi
  [[ -n "$QUERY" ]] && echo "query: $QUERY"
  if [[ ${#FILES[@]} -gt 0 ]]; then
    echo "files: ${FILES[*]}"
  fi
  echo "selected_profile: $selected_profile"
  echo "reasons:"
  for reason in "${reasons[@]}"; do
    echo "  - $reason"
  done
  echo "run_command: codex -p $selected_profile${QUERY:+ \"$QUERY\"}"
fi

if [[ $EXEC_MODE -eq 1 ]]; then
  cmd=(codex -p "$selected_profile")
  if [[ ${#PASS_ARGS[@]} -gt 0 ]]; then
    cmd+=("${PASS_ARGS[@]}")
  fi
  if [[ -n "$QUERY" ]]; then
    cmd+=("$QUERY")
  fi
  exec "${cmd[@]}"
fi
