#!/usr/bin/env bash

# Copyright 2026 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# Computes the Bazel test targets affected by changes between a base SHA and
# HEAD. Outputs one target label per line. If the change is too broad (non-Go
# file changes, bulk edits) or no testable files changed, the script prints a
# fallback marker or nothing so callers can decide what to do.
#
# Usage: affected-targets.sh BASE_SHA
#
# Exit behaviour:
#   - Prints affected test target labels (one per line) on stdout.
#   - Prints "FULL" if a full test run is required (non-Go changes, bulk edits).
#   - Prints nothing if no test-relevant files changed.

set -euo pipefail

if [ -z "${1:-}" ]; then
  echo "Usage: affected-targets.sh BASE_SHA" >&2
  exit 1
fi

BASE_SHA="$1"

MERGE_BASE=$(git merge-base "$BASE_SHA" HEAD)

# Check for non-Go/non-testdata changes that require a full run.
NON_GO_CHANGES=$(git diff --no-ext-diff --name-only "$MERGE_BASE" -- \
  '*.proto' '*.bzl' 'BUILD.bazel' '*/BUILD.bazel' 'WORKSPACE' 'DEPS.bzl' \
  '.bazelrc' '.bazelrc.user' 'go.mod' 'go.sum' \
  '.github/*' 'build/github/*' | head -1)

if [ -n "$NON_GO_CHANGES" ]; then
  echo "FULL"
  exit 0
fi

# If only inert files changed (docs, markdown, config that doesn't affect
# build/test), skip tests entirely rather than running the full suite.
ALL_CHANGES=$(git diff --no-ext-diff --name-only "$MERGE_BASE")
if [ -n "$ALL_CHANGES" ]; then
  NON_INERT=$(echo "$ALL_CHANGES" | grep -v \
    -e '\.md$' \
    -e '^docs/' \
    -e '^\.claude/' \
    -e '^LICENSE$' \
    -e '^\.gitignore$' \
    -e '^\.gitattributes$' \
    -e '^\.editorconfig$' \
    || true)
  if [ -z "$NON_INERT" ]; then
    # Only inert files changed, no tests needed.
    exit 0
  fi
fi

# Generate all possible Bazel labels for each changed file by splitting the
# path at every directory boundary. Bazel itself validates which are real
# source file targets, so no shell heuristics about package structure are needed.
CANDIDATES=""
while IFS= read -r file; do
  [ -z "$file" ] && continue
  dir=$(dirname "$file")
  rest=$(basename "$file")
  while [ "$dir" != "." ]; do
    CANDIDATES="${CANDIDATES} //${dir}:${rest}"
    rest="$(basename "$dir")/${rest}"
    dir=$(dirname "$dir")
  done
done <<< "$ALL_CHANGES"

if [ -z "$CANDIDATES" ]; then
  exit 0
fi

# Let Bazel resolve which candidates are real source file targets, find the
# build rules that own them, then find all go_test targets that transitively
# depend on those rules. Exclude integration-tagged tests, which require
# bespoke setup (e.g. lint_test needs GO_SDK) and have their own CI jobs.
QUERY="kind('go_test', rdeps(//pkg/..., same_pkg_direct_rdeps(set(${CANDIDATES})))) except attr('tags', 'integration', //pkg/...)"

# Temporarily disable errexit so we can inspect the query exit code.
set +e
AFFECTED=$(bazel query --keep_going "$QUERY" --output=label 2>/dev/null)
QUERY_EXIT=$?
set -e
# Exit 0 = success, exit 3 = partial (some invalid labels skipped by --keep_going).
# Any other exit code means the query itself failed; fall back to full suite.
if [ $QUERY_EXIT -ne 0 ] && [ $QUERY_EXIT -ne 3 ]; then
  echo "FULL"
  exit 0
fi

if [ -z "$AFFECTED" ]; then
  exit 0
fi

# If too many targets are affected, fall back to full suite.
TARGET_COUNT=$(echo "$AFFECTED" | wc -l | tr -d ' ')
if [ "$TARGET_COUNT" -gt 1000 ]; then
  echo "FULL"
  exit 0
fi

echo "$AFFECTED"
