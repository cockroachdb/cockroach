#!/usr/bin/env bash

# Copyright 2026 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# Fetch a Google Cloud application-default access token and expose it
# to subsequent workflow steps as BAZEL_STORAGE_ACCESS_TOKEN. Bazel's
# credential helper (build/bazelutil/credential-helper) reads this
# variable in CI to authenticate downloads from private GCS buckets.
#
# This script must be run from inside a GitHub Actions job that has
# access to gcloud and to the GITHUB_ENV file.

# Note: deliberately no `-x`. `set -x` would echo the variable
# expansions below to the workflow log, leaking the token before the
# `::add-mask::` workflow command registers it as a secret.
set -euo pipefail

token="$(gcloud auth application-default print-access-token)"

# Register the token as a secret so any downstream step that
# accidentally prints it (e.g. via `set -x` or `env`) is masked in the
# log. add-mask must come before the token is written anywhere else.
echo "::add-mask::${token}"

echo "BAZEL_STORAGE_ACCESS_TOKEN=${token}" >> "${GITHUB_ENV}"
