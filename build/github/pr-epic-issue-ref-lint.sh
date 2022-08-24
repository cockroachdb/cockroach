#!/usr/bin/env bash

set -euo pipefail

PR_NUMBER="$(echo "$GITHUB_REF" | awk -F '/' '{print $3}')"

GITHUB_TOKEN="${GITHUB_TOKEN}" bin/bazel run //pkg/cmd/lint-epic-issue-refs:lint-epic-issue-refs "$PR_NUMBER"
