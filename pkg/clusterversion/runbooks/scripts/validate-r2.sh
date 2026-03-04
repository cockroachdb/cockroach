#!/usr/bin/env bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.
# validate-r2.sh: Pre-push validation for R.2 (Mint Release).
#
# Run this before creating or re-pushing the R.2 PR to catch the most common
# CI failures locally. Regenerates documentation, rewrites systemschema test
# data, and verifies the bootstrap and logic tests pass.
#
# Note: The bootstrap hash in bootstrap/testdata/testdata sometimes requires a
# manual update — the test output will show the expected values if it fails.
#
# Usage: ./pkg/clusterversion/runbooks/scripts/validate-r2.sh
#
# Run from the repo root.

set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

echo "=== R.2 Pre-Push Validation ==="
echo ""

echo "[1/4] Regenerating documentation..."
./dev gen docs
echo ""

echo "[2/4] Rewriting systemschema test data..."
./dev test pkg/sql/catalog/systemschema_test --rewrite
echo ""

echo "[3/4] Verifying bootstrap hash test..."
# This test may fail if SystemDatabaseSchemaBootstrapVersion was updated.
# If it fails, copy the expected hash values from the test output into
# pkg/sql/catalog/bootstrap/testdata/testdata manually, then re-run.
./dev test pkg/sql/catalog/bootstrap -f TestInitialValuesToString
echo ""

echo "[4/4] Verifying crdb_internal_catalog logic test..."
./dev test pkg/sql/logictest -f TestLogic/local/crdb_internal_catalog
echo ""

echo "=== Validation complete. Review unexpected diffs before pushing: ==="
git diff --stat
