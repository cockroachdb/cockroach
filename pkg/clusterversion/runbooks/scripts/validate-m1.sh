#!/usr/bin/env bash
# validate-m1.sh: Pre-push validation for M.1 (Bump Current Version).
#
# Run this before creating or re-pushing the M.1 PR to catch the most common
# CI failures locally. The script rewrites test data where appropriate and then
# runs the relevant unit tests.
#
# Usage: ./pkg/clusterversion/runbooks/scripts/validate-m1.sh
#
# Run from the repo root.

set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

echo "=== M.1 Pre-Push Validation ==="
echo ""

echo "[1/4] Rewriting scplan rule test outputs..."
./dev test pkg/sql/schemachanger/scplan/internal/rules/... --rewrite
echo ""

echo "[2/4] Rewriting DeclarativeRules test output..."
./dev test pkg/cli -f DeclarativeRules --rewrite
echo ""

echo "[3/4] Rewriting bootstrap test data..."
bazel test //pkg/sql/catalog/bootstrap:bootstrap_test \
  --test_arg=-rewrite \
  --sandbox_writable_path="$REPO_ROOT"/pkg/sql/catalog/bootstrap
echo ""

echo "[4/4] Running clusterversion and roachpb unit tests..."
./dev test pkg/clusterversion pkg/roachpb
echo ""

echo "=== Validation complete. Review unexpected diffs before pushing: ==="
git diff --stat
