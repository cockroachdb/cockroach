#!/usr/bin/env bash

# Copyright 2026 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# Fetch the GitHub App credentials used by the lint job to authenticate
# Go module fetches from private cockroachlabs/* repos, and expose them
# as env vars for subsequent workflow steps. The companion step
# (actions/create-github-app-token) reads $GO_DEPS_APP_ID and
# $GO_DEPS_APP_PRIVATE_KEY to mint an installation token.

# Note: deliberately no `set -x`. The PEM and App ID would otherwise be
# echoed to the log before `::add-mask::` registered them as secrets.
set -euo pipefail

APP_ID="$(gcloud secrets versions access 1 \
  --secret=go-deps-github-app-id \
  --project=crl-github-actions)"
PRIVATE_KEY="$(gcloud secrets versions access 1 \
  --secret=go-deps-github-app-private-key \
  --project=crl-github-actions)"

# Register masks before writing the values anywhere. `::add-mask::`
# matches by exact substring per line, so mask the PEM line-by-line in
# addition to the whole blob; otherwise individual lines printed via
# `set -x` in a later step would slip through.
echo "::add-mask::${APP_ID}"
echo "::add-mask::${PRIVATE_KEY}"
while IFS= read -r line; do
  [[ -n "${line}" ]] && echo "::add-mask::${line}"
done <<< "${PRIVATE_KEY}"

# Multiline values use the heredoc form of $GITHUB_ENV.
{
  echo "GO_DEPS_APP_ID=${APP_ID}"
  echo "GO_DEPS_APP_PRIVATE_KEY<<__GHA_PEM_EOF__"
  echo "${PRIVATE_KEY}"
  echo "__GHA_PEM_EOF__"
} >> "${GITHUB_ENV}"
