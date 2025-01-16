#!/bin/bash

# Copyright 2025 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -euxo pipefail

working_dir=$(mktemp -d)
output_dir=$(mktemp -d)
storage_bucket="$BUCKET"
shas=("$BASE_SHA" "$HEAD_SHA")

# Disable parallel uploads, as it requires specific permissions
gcloud config set storage/parallel_composite_upload_enabled False

# Retrieve outputs from the runs
for sha in "${shas[@]}"; do
  mkdir -p "${working_dir}/${sha}/artifacts"
  gcloud storage cp -r "gs://${storage_bucket}/artifacts/${sha}/${BUILD_ID}/*" "${working_dir}/${sha}/artifacts/"
done

# Build microbenchmark CI utility
# This will build the repository's base version
bazel build --config=crosslinux $(./build/github/engflow-args.sh) \
      --jobs 100 \
      --bes_keywords integration-test-artifact-build \
      //pkg/cmd/microbench-ci \

bazel_bin=$(bazel info bazel-bin --config=crosslinux)
microbench_ci_bin=$bazel_bin/pkg/cmd/microbench-ci/microbench-ci_

# Compare the microbenchmarks
$microbench_ci_bin/microbench-ci compare \
  --working-dir="$working_dir" \
  --summary="$output_dir/summary.json" \
  --github-summary="$output_dir/summary.md" \
  --build-id="$BUILD_ID" \
  --old="$BASE_SHA" \
  --new="$HEAD_SHA"

cat "$output_dir/summary.md" > "$GITHUB_STEP_SUMMARY"

# Copy comparison results to GCS
gcloud storage cp -r "$output_dir/*" "gs://${storage_bucket}/results/${HEAD_SHA}/${BUILD_ID}"
