#!/usr/bin/env bash

# Copyright 2026 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# Configure git and Go to authenticate against private repositories
# when fetching modules. Used by the lint job, where staticcheck shells
# out to `go list`, which in turn invokes git to fetch any module not
# served from the public proxy.
#
# We hold one installation token per GitHub org the App is installed
# on, because `actions/create-github-app-token` mints tokens scoped to
# a single installation. Token TTL is ~1h, which comfortably covers
# the initial `go list` resolution; subsequent lookups hit the cached
# $GOMODCACHE on the self-hosted runner.
#
# Expects the workflow to have minted both tokens and exported them as
# $GH_APP_TOKEN_COCKROACHLABS and $GH_APP_TOKEN_COCKROACHDB.

# Note: deliberately no `set -x`. The tokens would be echoed before the
# `::add-mask::` workflow command registered them as secrets.
set -euo pipefail

: "${GH_APP_TOKEN_COCKROACHLABS:?must be set by the workflow step}"
: "${GH_APP_TOKEN_COCKROACHDB:?must be set by the workflow step}"

echo "::add-mask::${GH_APP_TOKEN_COCKROACHLABS}"
echo "::add-mask::${GH_APP_TOKEN_COCKROACHDB}"

# Each rewrite is scoped to the exact private repo it authenticates,
# so we don't attach a token when cloning unrelated public siblings
# (e.g. cockroachdb/errors, and cockroachlabs hosts both public and
# private repos).
git config --global \
  "url.https://x-access-token:${GH_APP_TOKEN_COCKROACHLABS}@github.com/cockroachlabs/roachmgr.insteadOf" \
  "https://github.com/cockroachlabs/roachmgr"
git config --global \
  "url.https://x-access-token:${GH_APP_TOKEN_COCKROACHDB}@github.com/cockroachdb/pebble-private.insteadOf" \
  "https://github.com/cockroachdb/pebble-private"

# GOPRIVATE bypasses proxy.golang.org and sum.golang.org for these
# modules, forcing `go list` to hit the VCS directly where the git
# rewrites above kick in.
echo "GOPRIVATE=github.com/cockroachlabs/roachmgr,github.com/cockroachdb/pebble-private" >> "${GITHUB_ENV}"
