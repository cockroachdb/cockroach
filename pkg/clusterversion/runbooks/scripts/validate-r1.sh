#!/usr/bin/env bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.
# validate-r1.sh: Pre-push validation for R.1 (Prepare for Beta).
#
# Run this before creating or re-pushing the R.1 PR to catch the most common
# CI failures locally. Regenerates documentation and rewrites test data, then
# runs the bootstrap verification test.
#
# Usage: ./pkg/clusterversion/runbooks/scripts/validate-r1.sh
#
# Run from the repo root.

set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

echo "=== R.1 Pre-Push Validation ==="
echo ""

echo "[1/4] Regenerating documentation..."
./dev gen docs
echo ""

echo "[2/4] Rewriting systemschema test data..."
./dev test pkg/sql/catalog/systemschema_test --rewrite
echo ""

echo "[3/4] Rewriting bootstrap hash test data..."
# --rewrite works for most cases; if it fails, the test output will show the
# expected hash values to update manually in bootstrap/testdata/testdata.
./dev test pkg/sql/catalog/bootstrap --rewrite -f TestInitialValuesToString
echo ""

echo "[4/4] Verifying bootstrap test passes..."
./dev test pkg/sql/catalog/bootstrap -f TestInitialValuesToString
echo ""

echo "=== Validation complete. Review unexpected diffs before pushing: ==="
git diff --stat
