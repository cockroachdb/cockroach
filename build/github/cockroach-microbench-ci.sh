#!/bin/bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euxo pipefail

# Directories and file names
output_dir="./artifacts/microbench"
cleaned_current_dir="$output_dir/current"
cleaned_base_dir="$output_dir/base"

# hardcoded the file name as of now since `compare` requires a particular format for the file name.
benchmark_file_name="pkg_sql-report.log"
log_output_file_path="$output_dir/microbench.log"
storage_bucket_url="gs://cockroach-microbench-ci"

# Threshold for comparison
threshold=${COMPARE_THRESHOLD:-0}
success_exit_status=0
error_exit_status=1

# Exit early if SKIP_COMPARISON is true and not a push step
if $SKIP_COMPARISON && ! $PUSH_STEP; then
  echo "Exiting since skip comparison is enabled and this is not a push step."
  exit $success_exit_status
fi

# Build binary with Bazel
bazel build --config crosslinux $(./build/github/engflow-args.sh) --jobs 100 //pkg/cmd/roachprod-microbench

roachprod_microbench_dir="_bazel/bin/pkg/cmd/roachprod-microbench/roachprod-microbench_"

mkdir -p "$output_dir"

# Run benchmarks and clean output
# running count=4 here because that's the minimum required for a comparison
bazel test //pkg/sql/tests:tests_test \
  --test_timeout=1800 \
  --strategy=TestRunner=sandboxed \
  --jobs 100 \
  --config=crosslinux \
  --remote_download_minimal \
  $(./build/github/engflow-args.sh) \
  --test_arg=-test.run=- \
  --test_arg='-test.bench=^BenchmarkKV$' \
  --test_sharding_strategy=disabled \
  --test_arg=-test.cpu --test_arg=1 \
  --test_arg=-test.v \
  --test_arg=-test.count=10 \
  --test_arg=-test.benchmem \
  --crdb_test_off \
  --test_output=all > "$log_output_file_path"

$roachprod_microbench_dir/roachprod-microbench clean "$log_output_file_path" "$cleaned_current_dir/$benchmark_file_name"

# Push artifact if this is a base merge and skip comparison
if $PUSH_STEP; then
  gcloud storage cp "$cleaned_current_dir/$benchmark_file_name" "$storage_bucket_url/$GITHUB_REF_NAME/$GITHUB_SHA.log"
  echo "Skipping comparison since this is a push step into the target branch"
  exit $success_exit_status
fi

# Compare benchmarks
if ! gcloud storage cp "$storage_bucket_url/$GITHUB_BASE_REF/$BASE_SHA.log" "$cleaned_base_dir/$benchmark_file_name"; then
  echo "Couldn't download base bench file, exiting."
  exit $success_exit_status
fi

if ! $roachprod_microbench_dir/roachprod-microbench compare "$cleaned_current_dir" "$cleaned_base_dir" --threshold="$threshold"; then
  echo "There is an error during comparison. If it's a perf regression, please try to fix it. This won't block your change for merging currently."
  exit $error_exit_status
fi

rm -rf "$output_dir"
exit $success_exit_status
