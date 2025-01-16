#!/bin/bash

# Copyright 2025 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -euxo pipefail

# Build microbenchmark CI utility
# This will build the repository's base version
bazel build --config=crosslinux $(./build/github/engflow-args.sh) \
      --jobs 100 \
      --bes_keywords integration-test-artifact-build \
      //pkg/cmd/microbench-ci \

bazel_bin=$(bazel info bazel-bin --config=crosslinux)
microbench_ci_bin=$bazel_bin/pkg/cmd/microbench-ci/microbench-ci_

# Grab the summary from the previous step
working_dir=$(mktemp -d)
gcloud storage cp "gs://${storage_bucket}/results/${HEAD_SHA}/${BUILD_ID}/summary.md" "$working_dir/summary.md"

cat "$working_dir/summary.md"

# Compare the microbenchmarks
#$microbench_ci_bin/microbench-ci post


