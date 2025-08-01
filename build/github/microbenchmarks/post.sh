#!/bin/bash

# Copyright 2025 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -euxo pipefail

working_dir=$(mktemp -d)
storage_bucket="$BUCKET"

# Grab the summary from the previous step
gcloud storage cp "gs://${storage_bucket}/results/${HEAD_SHA}/${BUILD_ID}/summary.md" "$working_dir/summary.md"

# Post summary to GitHub on the PR
./build/github/microbenchmarks/util.sh post \
  --github-summary="$working_dir/summary.md" \
  --pr-number="$PR_NUMBER" \
  --repo="$REPO"
