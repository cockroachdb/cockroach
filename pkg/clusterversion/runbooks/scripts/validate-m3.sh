#!/usr/bin/env bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.
# validate-m3.sh: Pre-push validation for M.3 PR 2 (Enable Upgrade Tests, code changes).
#
# Run this before creating or re-pushing the M.3 code PR. Rewrites the
# declarative rules corpus (which changes when PreviousRelease is bumped) and
# runs the unit tests most likely to fail.
#
# This script covers PR 2 (code changes) only. PR 1 (fixtures) is generated on
# gceworker and has no local pre-push validation — verify fixture file sizes
# manually (each checkpoint-vX.Y.tgz should be ~3-5 MB).
#
# Usage: ./pkg/clusterversion/runbooks/scripts/validate-m3.sh
#
# Run from the repo root.

set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

echo "=== M.3 Pre-Push Validation (PR 2: code changes) ==="
echo ""

echo "[1/3] Rewriting DeclarativeRules test corpus..."
./dev test pkg/cli -f=TestDeclarativeRules --rewrite
echo ""

echo "[2/3] Running clusterversion unit tests..."
./dev test pkg/clusterversion -v
echo ""

echo "[3/3] Running logictestbase unit tests..."
./dev test pkg/sql/logictest/logictestbase -v
echo ""

echo "=== Validation complete. Review unexpected diffs before pushing: ==="
git diff --stat
