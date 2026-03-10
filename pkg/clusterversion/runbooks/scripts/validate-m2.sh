#!/usr/bin/env bash
# validate-m2.sh: Pre-push validation for M.2 (Enable Mixed-Cluster Logic Tests).
#
# Run this before creating or re-pushing the M.2 PR. Verifies bootstrap data
# integrity and runs a quick smoke test of the new mixed-cluster logic test config.
#
# Note: The skipif directives for logic tests cannot be pre-determined locally —
# those are discovered reactively when tests fail. This script catches bootstrap
# and config wiring issues before CI.
#
# Usage: ./pkg/clusterversion/runbooks/scripts/validate-m2.sh <version>
#
# Example: ./pkg/clusterversion/runbooks/scripts/validate-m2.sh 25.4
#
# Run from the repo root.

set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <version>"
  echo "Example: $0 25.4"
  exit 1
fi

VERSION="$1"
VERSION_UNDERSCORED="${VERSION//./_}"

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

BOOTSTRAP_DATA_DIR="pkg/sql/catalog/bootstrap/data"
CONFIG="local-mixed-${VERSION}"

echo "=== M.2 Pre-Push Validation (version: ${VERSION}) ==="
echo ""

echo "[1/4] Verifying bootstrap data files exist..."
for suffix in system.keys system.sha256 tenant.keys tenant.sha256; do
  f="${BOOTSTRAP_DATA_DIR}/${VERSION_UNDERSCORED}_${suffix}"
  if [[ ! -f "$f" ]]; then
    echo "ERROR: Missing bootstrap file: $f"
    echo "Generate it by running on the release-${VERSION} branch:"
    echo "  ./dev build sql-bootstrap-data && bin/sql-bootstrap-data"
    exit 1
  fi
done
echo "  All 4 bootstrap files present."
echo ""

echo "[2/4] Verifying bootstrap SHA256 hashes..."
sha256sum "${BOOTSTRAP_DATA_DIR}/${VERSION_UNDERSCORED}_system.keys" \
  | awk '{print $1}' \
  | diff - "${BOOTSTRAP_DATA_DIR}/${VERSION_UNDERSCORED}_system.sha256" \
  || { echo "ERROR: system.keys sha256 mismatch — regenerate bootstrap data"; exit 1; }
sha256sum "${BOOTSTRAP_DATA_DIR}/${VERSION_UNDERSCORED}_tenant.keys" \
  | awk '{print $1}' \
  | diff - "${BOOTSTRAP_DATA_DIR}/${VERSION_UNDERSCORED}_tenant.sha256" \
  || { echo "ERROR: tenant.keys sha256 mismatch — regenerate bootstrap data"; exit 1; }
echo "  SHA256 hashes match."
echo ""

echo "[3/4] Running bootstrap initial keys test..."
./dev test pkg/sql/catalog/bootstrap -f TestInitialKeys -v
echo ""

echo "[4/4] Running quick logictest smoke test with ${CONFIG} config..."
./dev testlogic base --config="${CONFIG}" --files=crdb_internal_catalog -v
echo ""

echo "=== Validation complete. Review unexpected diffs before pushing: ==="
git diff --stat
