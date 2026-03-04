#!/usr/bin/env bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.
# compare-with-reference-pr.sh: Compare current branch changes against a reference PR.
#
# Helps catch two classes of problems before pushing:
#   1. Files changed that shouldn't be (unexpected scope creep)
#   2. Files not changed that should be (missed steps)
#
# Usage: ./pkg/clusterversion/runbooks/scripts/compare-with-reference-pr.sh <pr_number> [base_branch]
#
# Examples:
#   ./pkg/clusterversion/runbooks/scripts/compare-with-reference-pr.sh 149494
#   ./pkg/clusterversion/runbooks/scripts/compare-with-reference-pr.sh 157767 master
#
# Reference PRs by task:
#   M.1: 149494 (25.3→25.4), 139387 (25.2→25.3 equivalent)
#   M.4: 157767 (25.2→25.3), 158225 (25.3→25.4)

set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <pr_number> [base_branch]"
  echo ""
  echo "Reference PRs by task:"
  echo "  M.1: 149494 (most recent)"
  echo "  M.4: 158225 (most recent)"
  exit 1
fi

PR="$1"
BASE="${2:-master}"

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

echo "=== Comparing against PR #$PR (base: $BASE) ==="
echo ""

git diff --name-only "$BASE" | sort > /tmp/current_files.txt
gh pr view "$PR" --repo cockroachdb/cockroach --json files \
  --jq '.files[].path' | sort > /tmp/ref_files.txt

CURRENT_COUNT=$(wc -l < /tmp/current_files.txt | tr -d ' ')
REF_COUNT=$(wc -l < /tmp/ref_files.txt | tr -d ' ')

echo "File counts: current=$CURRENT_COUNT  reference PR #$PR=$REF_COUNT"
echo ""

ONLY_CURRENT=$(comm -13 /tmp/ref_files.txt /tmp/current_files.txt)
ONLY_REF=$(comm -23 /tmp/ref_files.txt /tmp/current_files.txt)
COMMON=$(comm -12 /tmp/ref_files.txt /tmp/current_files.txt)

if [[ -n "$ONLY_CURRENT" ]]; then
  echo "=== Files ONLY in current branch (investigate — may be unintended): ==="
  echo "$ONLY_CURRENT"
  echo ""
else
  echo "=== No extra files in current branch (good) ==="
  echo ""
fi

if [[ -n "$ONLY_REF" ]]; then
  echo "=== Files ONLY in reference PR (possible missed steps): ==="
  echo "$ONLY_REF"
  echo ""
else
  echo "=== No missing files vs reference PR (good) ==="
  echo ""
fi

COMMON_COUNT=$(echo "$COMMON" | grep -c . || true)
echo "=== $COMMON_COUNT files in common with reference PR ==="
