#!/usr/bin/env bash
# validate-m4.sh: Pre-push validation for M.4 (Bump MinSupported Version).
#
# Run this before creating or re-pushing the M.4 PR to catch the most common
# CI failures locally. The script runs the tests known to fail after MinSupported
# is bumped and rewrites test data where appropriate.
#
# Usage: ./pkg/clusterversion/runbooks/scripts/validate-m4.sh
#
# Run from the repo root.

set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

echo "=== M.4 Pre-Push Validation ==="
echo ""

echo "[1/4] Testing MinimumSupportedFormatVersion (storage)..."
./dev test pkg/storage -f TestMinimumSupportedFormatVersion -v
echo ""

echo "[2/4] Running clusterversion unit tests..."
./dev test pkg/clusterversion
echo ""

echo "[3/4] Rewriting DeclarativeRules test output..."
./dev test pkg/cli -f DeclarativeRules --rewrite
echo ""

echo "[4/4] Build check..."
./dev build short
echo ""

echo "=== Validation complete. Review unexpected diffs before pushing: ==="
git diff --stat
