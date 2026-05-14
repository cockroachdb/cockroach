#!/usr/bin/env bash

# Copyright 2026 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# This script bumps the DRPC dependency in go.mod to a given commit.
#
# Usage:
#
#   ./scripts/bump-drpc.sh <commit-url>
#
# Where <commit-url> is a GitHub commit link, e.g.:
#   https://github.com/cockroachdb/drpc/commit/d57f8922c21431976751f40127e59cda71768431
#   https://github.com/shubhamdhama/drpc/commit/d57f8922c21431976751f40127e59cda71768431

set -euo pipefail

if [ -z "${1-}" ]; then
  echo "Usage: $0 <github-commit-url>" >&2
  echo "  e.g. $0 https://github.com/cockroachdb/drpc/commit/abc123" >&2
  exit 1
fi

COMMIT_URL="$1"

# Parse the org and SHA from the commit URL.
# Expected format: https://github.com/<org>/drpc/commit/<sha>
if [[ "$COMMIT_URL" =~ github\.com/([^/]+)/drpc/commit/([0-9a-f]+) ]]; then
  ORG="${BASH_REMATCH[1]}"
  SHA="${BASH_REMATCH[2]}"
else
  echo "Error: could not parse commit URL: $COMMIT_URL" >&2
  echo "Expected format: https://github.com/<org>/drpc/commit/<sha>" >&2
  exit 1
fi

MODULE="github.com/${ORG}/drpc"
echo "Organization: $ORG"
echo "Commit SHA:   $SHA"
echo "Module:       $MODULE"
echo

# Resolve the pseudo-version for this commit.
echo "Resolving pseudo-version..."
VERSION=$(go list -m -json "${MODULE}@${SHA}" | jq -r '.Version')
if [ -z "$VERSION" ] || [ "$VERSION" == "null" ]; then
  echo "Error: could not resolve version for ${MODULE}@${SHA}" >&2
  exit 1
fi
echo "Resolved version: $VERSION"
echo

# Update the replace directive in go.mod.
OLD_REPLACE=$(grep '^replace storj.io/drpc =>' go.mod)
NEW_REPLACE="replace storj.io/drpc => ${MODULE} ${VERSION}"

if [ -z "$OLD_REPLACE" ]; then
  echo "Error: could not find 'replace storj.io/drpc =>' in go.mod" >&2
  exit 1
fi

echo "Old: $OLD_REPLACE"
echo "New: $NEW_REPLACE"
echo

sed -i "s|^replace storj.io/drpc =>.*|${NEW_REPLACE}|" go.mod

# Tidy up.
go mod tidy

# Regenerate Bazel files.
echo "Running dev generate bazel --mirror..."
./dev generate bazel --mirror

echo
echo "Done. DRPC bumped to ${SHA:0:12} (${VERSION})."
echo "Modified files:"
git diff --name-only
