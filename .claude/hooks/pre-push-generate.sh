#!/usr/bin/env bash
# .claude/hooks/pre-push-generate.sh
#
# PreToolUse hook: intercepts 'git push' Bash calls.
#
# Blocks the push if there are uncommitted dirty files that look like
# generated output (BUILD.bazel, generated_test.go, docs/generated/).
# Does NOT run './dev generate' — that's too slow for a hook and causes
# timeouts. Instead, the generate step must be part of the commit workflow.
#
# Watched paths (triggers the check):
#   pkg/clusterversion/
#   pkg/upgrade/upgrades/
#   pkg/sql/catalog/systemschema/
#   docs/generated/settings/
#   pkg/sql/logictest/logictestbase/

set -uo pipefail

# Read the hook input JSON from stdin.
INPUT=$(cat)

# Extract the bash command being run.
COMMAND=$(printf '%s' "$INPUT" | jq -r '.tool_input.command // ""')

# Only intercept git push commands.
if ! printf '%s' "$COMMAND" | grep -qE '\bgit push\b'; then
  exit 0
fi

# Resolve the repo root from the current working directory.
REPO_ROOT=$(git rev-parse --show-toplevel 2>/dev/null) || {
  echo "pre-push-generate: could not determine repo root, skipping" >&2
  exit 0
}
cd "$REPO_ROOT"

# Paths that trigger code generation when changed.
RELATED_PATTERNS=(
  "pkg/clusterversion/"
  "pkg/upgrade/upgrades/"
  "pkg/sql/catalog/systemschema/"
  "docs/generated/settings/"
  "pkg/sql/logictest/logictestbase/"
)

# Determine the upstream ref: use tracking branch if set, fall back to origin/master.
UPSTREAM=$(git rev-parse --abbrev-ref --symbolic-full-name @{u} 2>/dev/null || echo "origin/master")

# Collect changed files: staged and commits not yet pushed.
CHANGED_FILES=$(
  git diff --name-only --cached 2>/dev/null
  git log --name-only --format='' "$UPSTREAM..HEAD" 2>/dev/null || true
)

NEEDS_CHECK=false
for PATTERN in "${RELATED_PATTERNS[@]}"; do
  if printf '%s' "$CHANGED_FILES" | grep -q "$PATTERN"; then
    NEEDS_CHECK=true
    break
  fi
done

if ! $NEEDS_CHECK; then
  exit 0
fi

# Fast check: are there dirty files that look like generated output?
# These patterns match files that './dev generate bazel' or './dev generate docs' would update.
DIRTY_GENERATED=$(git diff --name-only 2>/dev/null | grep -E \
  '(^|/)BUILD\.bazel$|(^|/)generated_test\.go$|^docs/generated/|^pkg/BUILD\.bazel$' \
  || true)

if [ -n "$DIRTY_GENERATED" ]; then
  DIRTY_LIST=$(printf '%s' "$DIRTY_GENERATED" | head -20)
  MSG=$(printf "PUSH BLOCKED: uncommitted generated files detected.\n\nThese look like output from './dev generate bazel' or './dev generate docs':\n%s\n\nRun:\n  git add <files>\n  git commit --amend --no-edit\n  git push ..." "$DIRTY_LIST")
  jq -n --arg msg "$MSG" '{"continue": false, "stopReason": $msg}'
  exit 0
fi

echo "==> pre-push-generate: no dirty generated files detected; proceeding with push" >&2
exit 0
