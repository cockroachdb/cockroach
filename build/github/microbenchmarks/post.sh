#!/bin/bash

# Copyright 2025 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -euxo pipefail

working_dir=$(mktemp -d)
storage_bucket="$BUCKET"

# Build microbenchmark CI utility (base version)
bazel build --config=crosslinux $(./build/github/engflow-args.sh) \
  --jobs 100 \
  --bes_keywords integration-test-artifact-build \
  //pkg/cmd/microbench-ci

bazel_bin=$(bazel info bazel-bin --config=crosslinux)

# Grab the summary from the previous step
gcloud storage cp "gs://${storage_bucket}/results/${HEAD_SHA}/${BUILD_ID}/summary.md" "$working_dir/summary.md"

# Post summary to GitHub on the PR
"$bazel_bin/pkg/cmd/microbench-ci/microbench-ci_/microbench-ci" post \
  --github-summary="$working_dir/summary.md" \
  --pr-number="$PR_NUMBER" \
  --repo="$REPO"
