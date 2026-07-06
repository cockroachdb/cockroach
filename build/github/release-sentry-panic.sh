#!/usr/bin/env bash

# Copyright 2026 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# GitHub Actions wrapper for triggering a Sentry panic on a new release.
# Expects SENTRY_AUTH_TOKEN to be set by the calling workflow step (fetched
# via google-github-actions/get-secretmanager-secrets, which auto-masks the
# value with ::add-mask::).
#
# NOTE: This script intentionally does NOT use set -x. It handles secrets
# that must never appear in build logs.

set -euo pipefail

if [[ -n "${DRY_RUN:-}" ]] ; then
  echo "Skipping this step in dry-run mode"
  exit
fi

: "${SENTRY_AUTH_TOKEN:?must be set by the workflow}"

# Assign then export separately so a failed version.txt read isn't masked by
# export's own exit status (SC2155) and set -e can catch it.
VERSION=$(grep -v "^#" "pkg/build/version.txt" | head -n1)
export VERSION

# Skip the panic when this release series is filtered out by Sentry's inbound
# "Releases" data filter. As a release series reaches end-of-life we add it to
# that filter (e.g. "v23.*") so its events are dropped at ingest; triggering a
# panic for such a version would just produce an event Sentry immediately
# discards. We read the filter from Sentry instead of hardcoding the EOL list
# here so there is a single source of truth.
#
# The filter lives in the project option "filters:releases": a newline-separated
# list of case-insensitive globs. Reading it requires the project:read scope on
# SENTRY_AUTH_TOKEN. The token is passed via a curl config on stdin (not -H) so
# it never appears in the process argv. We match VERSION with bash globbing
# (unquoted right-hand side of [[ == ]]), which covers the simple "vMAJOR.*"
# patterns actually in use (not Sentry's full glob grammar). On any API or parse
# error we fail closed and skip, logging loudly, so a transient Sentry hiccup
# never fails the release build.
if ! releases_filter=$(printf 'url = "%s"\nheader = "Authorization: Bearer %s"\n' \
  "https://sentry.io/api/0/projects/cockroach-labs/cockroachdb/" "$SENTRY_AUTH_TOKEN" \
  | curl -fsS --max-time 30 -K - | jq -r '(.options // {})["filters:releases"] // ""'); then
  echo "WARNING: could not read Sentry releases filter; skipping Sentry panic for $VERSION." >&2
  exit 0
fi

shopt -s nocasematch
while IFS= read -r pattern; do
  [[ -z $pattern ]] && continue
  # shellcheck disable=SC2053 # unquoted RHS is intentional: glob-match the version.
  if [[ $VERSION == $pattern ]]; then
    echo "$VERSION is filtered out by Sentry's releases data filter (matched '$pattern'); skipping Sentry panic."
    exit 0
  fi
done <<< "$releases_filter"
shopt -u nocasematch

echo "Triggering Sentry panic for $VERSION..."

# Call the existing script which runs the sentry tool via Bazel in Docker.
exec build/teamcity/internal/release/process/trigger-sentry-panic.sh
