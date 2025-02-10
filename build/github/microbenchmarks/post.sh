#!/bin/bash

# Copyright 2025 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -euxo pipefail

working_dir=$(mktemp -d)
storage_bucket="$BUCKET"

# Get the microbenchmark CI utility (BASE version). It is important that we only
# use the base version of the utility here, as the GitHub token has elevated
# permissions.
gcloud storage cp "gs://${storage_bucket}/builds/${BASE_SHA}/bin/microbench-ci" "$working_dir/microbench-ci"
chmod +x "$working_dir/microbench-ci"

# Grab the summary from the previous step
gcloud storage cp "gs://${storage_bucket}/results/${HEAD_SHA}/${BUILD_ID}/summary.md" "$working_dir/summary.md"

# Post summary to GitHub on the PR
"$working_dir/microbench-ci" post --github-summary="$working_dir/summary.md" \
  --pr-number="$PR_NUMBER" \
  --repo="$REPO"
